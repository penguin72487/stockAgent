#!/usr/bin/env python3
"""Read-only A/B for feature-page filtering on one trusted frozen snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import time
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.benchmark_data_monitor_feature_shards import _trusted_body  # noqa: E402
from stockagent.live.dashboard_updates import metadata_signature  # noqa: E402
from stockagent.live.data_monitor_feature_receipt import trusted_feature_source_pages  # noqa: E402
from stockagent.live.data_monitor_feature_pages import (  # noqa: E402
    FeaturePageIndex,
    feature_page_revision,
    page_from_source_rows,
)


def _reference_page(
    index: FeaturePageIndex, *, offset: int, limit: int, search: str = "",
    category: str = "all", source: str = "all",
) -> dict[str, Any]:
    """The prior full-scan algorithm, retained only as a benchmark oracle."""

    query = search.strip().lower()
    if not query and category == "all" and source == "all":
        total = len(index.rows)
        page_rows = list(index.rows[offset:offset + limit])
    else:
        total = 0
        page_rows = []
        for row, searchable in zip(index.rows, index.search_text, strict=True):
            if category != "all" and row["market_category"] != category:
                continue
            if source != "all" and row["dataset_id"] != source:
                continue
            if query and query not in searchable:
                continue
            if offset <= total < offset + limit:
                page_rows.append(row)
            total += 1
    return {
        "schema_version": 1,
        "read_only": True,
        "production_control_possible": False,
        "generated_at_utc": index.generated_at_utc,
        "revision": index.revision,
        "reset_required": False,
        "offset": offset,
        "limit": limit,
        "matching_total": total,
        "has_more": offset + len(page_rows) < total,
        "summary": index.summary,
        "filters": {"categories": index.categories, "sources": index.sources},
        "rows": page_rows,
    }


def _uncached_source_page(
    path: Path, *, source: str, variant: str,
) -> dict[str, Any]:
    source_stat = path.stat()
    signature = metadata_signature(source_stat)
    if variant == "full":
        with path.open("rb") as stream:
            if metadata_signature(os.fstat(stream.fileno())) != signature:
                raise ValueError("source changed before full read")
            body = stream.read()
            if metadata_signature(os.fstat(stream.fileno())) != signature:
                raise ValueError("source changed during full read")
        if metadata_signature(path.stat()) != signature:
            raise ValueError("source changed after full read")

        def reject_nonfinite(value: str) -> None:
            raise ValueError(f"non-finite feature JSON: {value}")

        payload = json.loads(body, parse_constant=reject_nonfinite)
        index = FeaturePageIndex.from_payload(payload, source_signature=signature)
        return index.page(offset=0, limit=80, source=source)
    trusted = trusted_feature_source_pages(path, source_stat=source_stat)
    if trusted is None:
        raise ValueError("source-bound small projection unavailable")
    pages, preview = trusted
    if source not in pages:
        raise ValueError("selected source is not in small projection")
    return page_from_source_rows(
        rows=pages[source], preview=preview,
        revision=feature_page_revision(signature), requested_revision=None,
        offset=0, limit=80, search="", category="all",
    )


def _uncached_source_trials(
    path: Path, *, source: str, initial_signature: tuple[int, ...],
) -> dict[str, Any]:
    trials = []
    reference: dict[str, Any] | None = None
    equal = True
    try:
        for variant in ("full", "small", "small", "full"):
            started = time.perf_counter()
            page = _uncached_source_page(path, source=source, variant=variant)
            elapsed_ms = round((time.perf_counter() - started) * 1_000, 3)
            normalized = json.loads(json.dumps(page, ensure_ascii=False))
            if reference is None:
                reference = normalized
            equal &= normalized == reference
            trials.append({"variant": variant, "elapsed_ms": elapsed_ms})
    except (OSError, ValueError) as exc:
        return {"state": "inconclusive", "error_type": type(exc).__name__, "trials": trials}
    if metadata_signature(path.stat()) != initial_signature:
        return {"state": "inconclusive_source_changed", "trials": trials}
    return {
        "state": "verified" if equal else "mismatch",
        "source": source,
        "matching_total": reference["matching_total"] if reference else None,
        "trials": trials,
        "claim_boundary": (
            "Each call reads the current local files and does not reuse a gateway index. "
            "OS page cache and competing load are uncontrolled; this is not public HTTP or p95."
        ),
    }


def benchmark(source: Path) -> dict[str, Any]:
    body, stat = _trusted_body(source)
    payload = json.loads(body)
    started = time.perf_counter()
    index = FeaturePageIndex.from_payload(payload, source_signature=(
        stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns,
    ))
    build_ms = round((time.perf_counter() - started) * 1_000, 3)
    source_sizes: dict[str, int] = {
        key: len(positions) for key, positions in index.source_positions.items()
    }
    smallest = min(source_sizes, key=source_sizes.get) if source_sizes else "missing"
    largest = max(source_sizes, key=source_sizes.get) if source_sizes else "missing"
    category = min(
        index.category_positions, key=lambda name: len(index.category_positions[name]),
    ) if index.category_positions else "missing"
    cases = {
        "unfiltered_search": {"search": "close"},
        "small_source": {"source": smallest},
        "largest_source": {"source": largest},
        "category": {"category": category},
        "source_and_search": {"source": smallest, "search": "close"},
        "missing_source": {"source": "__missing__"},
    }
    results = []
    all_equal = True
    for name, filters in cases.items():
        kwargs = {"offset": 0, "limit": 80, **filters}
        reference = _reference_page(index, **kwargs)
        improved = index.page(**kwargs)
        equal = reference == improved
        all_equal &= equal
        trials = []
        for variant in ("reference", "indexed", "indexed", "reference"):
            method = _reference_page if variant == "reference" else index.page
            for _ in range(3):
                method(index, **kwargs) if variant == "reference" else method(**kwargs)
            samples = []
            for _ in range(20):
                start = time.perf_counter()
                method(index, **kwargs) if variant == "reference" else method(**kwargs)
                samples.append((time.perf_counter() - start) * 1_000)
            trials.append({
                "variant": variant,
                "median_ms": round(statistics.median(samples), 3),
            })
        results.append({
            "case": name, "filters": filters, "matching_total": improved["matching_total"],
            "equal": equal, "trials": trials,
        })
    uncached = _uncached_source_trials(
        source, source=smallest, initial_signature=metadata_signature(stat),
    )
    return {
        "state": "verified" if all_equal else "mismatch",
        "source_sha256": hashlib.sha256(body).hexdigest(),
        "source_bytes": len(body),
        "rows": len(index.rows),
        "sources": len(index.source_positions),
        "index_build_ms": build_ms,
        "cases": results,
        "uncached_source": uncached,
        "claim_boundary": (
            "Frozen trusted snapshot, one process and local in-memory page method only. "
            "Not public HTTP, browser rendering, producer build, or live p95."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", type=Path,
        default=REPO_ROOT / "artifacts/live/data_monitor/feature_inventory.json",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = benchmark(args.source.resolve())
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
    return 0 if result["state"] == "verified" else 2


if __name__ == "__main__":
    raise SystemExit(main())
