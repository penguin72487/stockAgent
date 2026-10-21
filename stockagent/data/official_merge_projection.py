"""Lossless suffix reuse for the canonical official OHLCV merge owner.

This derived cache neither publishes sources nor changes their precedence.
Every original byte receipt remains required. Unsupported clocks are full
rebuilds; changed lifecycle semantics invalidate the entire projection.
"""
from __future__ import annotations

from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import date
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import time
from uuid import uuid4

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.partitioned_parquet import ParquetSource, write_partitioned_table
from stockagent.data.tw_public_incremental import session_fingerprints
from stockagent.data_sync.cache_capacity import GenerationWriteBudget, reserve_cache_write

CONTRACT = "official_ohlcv_exact_session_merge_v1"
MAX_CONTROL_BYTES = 16 * 1024**2


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


class OfficialMergeProjection:
    """One serialized build; only a fully committed manifest can be reused."""

    def __init__(self, root, implementation, *, capacity_policy=None):
        self.root = Path(root).absolute()
        if self.root.resolve() != self.root:
            raise ValueError("redirected official merge cache")
        self.implementation = implementation
        self.capacity_policy = capacity_policy
        self.leases = ExitStack()
        self.previous = None
        self.start = None
        self.identity = None
        self.metrics = {}
        self.work = {"contract": CONTRACT, "full_source_receipts_required": True,
                     "reused_rows": 0, "rebuilt_rows": 0, "old_generations_removed": 0}

    def __enter__(self):
        self.root.mkdir(parents=True, exist_ok=True)
        lock = self.leases.enter_context((self.root / "merge.lock").open("a+b"))
        fcntl.flock(lock, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exception):
        return self.leases.__exit__(*exception)

    def _previous(self):
        try:
            with (self.root / "current.json").open("rb") as stream:
                raw = stream.read(1025)
            if len(raw) > 1024:
                return None
            pointer = json.loads(raw)
            name = pointer["manifest"]
            if not re.fullmatch(r"generation_[a-f0-9]{32}/merged\.parquet\.manifest\.json", name):
                return None
            path = self.root / name
            if sha256_file(path) != pointer["sha256"]:
                return None
            source = ParquetSource(path)
            identity = source.manifest["source_identity"]
            if (identity["contract"] != CONTRACT or identity["implementation"] != self.implementation
                    or not isinstance(identity["sources"], dict)
                    or not isinstance(identity["metrics"], dict)):
                return None
            return source
        except (OSError, ValueError, KeyError, TypeError, pa.ArrowException):
            return None

    def prepare_sources(self, receipts):
        """Certify clocks before the owner selects/normalizes source columns."""
        scope = tuple((str(Path(row["path"]).absolute()), row["sha256"]) for row in receipts)
        if getattr(self, "_prepared_scope", None) == scope:
            return
        started = time.perf_counter()
        previous = self._previous()
        old = previous.manifest["source_identity"] if previous is not None else {}
        sources = {}
        changed = []
        fallback_reason = None
        pending = []
        proof_work = {}
        for receipt in receipts:
            path = Path(receipt["path"])
            name = str(path.absolute())
            prior = old.get("sources", {}).get(name, {})
            if prior.get("sha256") == receipt["sha256"]:
                sources[name] = prior
                proof_work[name] = {"unchanged_original_byte_sha256": True, "decoded_rows": 0}
                continue
            pending.append((name, path, receipt, prior))

        def certify(item):
            name, path, receipt, prior = item
            try:
                proof = session_fingerprints(path, previous=prior.get("proof"), capture_physical=True)
            except (ValueError, OSError, pa.ArrowException):
                # Archives with another ordering/date ABI still use the original
                # owner. An unchanged unsupported archive is reusable next time.
                proof = None
            return name, receipt, prior, proof

        # Independent exact source scans overlap without weakening a single
        # byte/clock check. Bound the local decoder fan-out; do not create one
        # executor per session or compete with every upstream producer thread.
        with ThreadPoolExecutor(max_workers=min(2, max(1, len(pending)))) as executor:
            certified = list(executor.map(certify, pending)) if pending else []
        for name, receipt, prior, proof in certified:
            sources[name] = {"sha256": receipt["sha256"], "proof": proof}
            proof_work[name] = proof.get("row_group_work") if proof else {"unsupported_clock": True}
            before = prior.get("proof")
            if not before or not proof or any(before[key] != proof[key] for key in
                    ("schema_sha256", "session_clock")):
                fallback_reason = "source_clock_schema_or_initial_generation"
                continue
            dates = set(before["sessions"]) | set(proof["sessions"])
            changed.extend(day for day in dates if before["sessions"].get(day) != proof["sessions"].get(day))
        self._prepared_scope = scope
        self._prepared_sources = sources
        self._prepared_previous = previous
        self._prepared_changed = changed
        self._prepared_fallback = fallback_reason
        self.work["source_proof_seconds"] = time.perf_counter() - started
        self.work["source_proof_work"] = proof_work

    def input_scan(self, path, *, lower=None, upper=None):
        """Use only decoded/certified original clocks, not footer date stats.

        Whole-history metadata projections remain lazy. A bounded price read
        selects the exact original row groups using previously decoded clock
        bounds. Unknown clocks retain the original generic parser/full scan.
        """
        path = Path(path)
        proof = self._prepared_sources.get(str(path.absolute()), {}).get("proof")
        clock = {"column": "date", "format": "iso", "ignore_null": False}
        if not proof or proof["session_clock"] != clock:
            return pl.scan_parquet(path, glob=False, use_statistics=False), None
        parquet = pq.ParquetFile(path)
        raw_type = parquet.schema_arrow.field("date").type
        physical = proof.get("physical_prefix")
        if lower is not None and physical is not None:
            selected = [index for index, group in enumerate(physical["groups"])
                if group["last_day"] >= lower.isoformat()
                and (upper is None or group["first_day"] <= upper.isoformat())]
            frame = pl.from_arrow(parquet.read_row_groups(selected, use_threads=False)).lazy()
            self.work.setdefault("price_source_scans", {})[str(path.absolute())] = {
                "bounds": "exact_decoded_chronological_values_not_footer_statistics",
                "total_groups": parquet.num_row_groups, "decoded_groups": len(selected),
                "decoded_rows": sum(parquet.metadata.row_group(index).num_rows for index in selected)}
        else:
            frame = pl.scan_parquet(path, glob=False, use_statistics=False)
        text = pa.types.is_string(raw_type) or pa.types.is_large_string(raw_type)
        if lower is not None:
            frame = frame.filter(pl.col("date") >= pl.lit(lower.isoformat() if text else lower))
        if upper is not None:
            frame = frame.filter(pl.col("date") <= pl.lit(upper.isoformat() if text else upper))
        # All source date values were proved canonical ISO or native Date32;
        # do not execute every alternative ROC/text parser on them again.
        return frame, pl.col("date").cast(pl.Date)

    def plan(self, receipts, *, semantics, calendar, end_date):
        """Certify a complete unchanged prefix, never infer it from max date."""
        self.prepare_sources(receipts)
        previous, sources = self._prepared_previous, self._prepared_sources
        old = previous.manifest["source_identity"] if previous is not None else {}
        changed = list(self._prepared_changed)
        fallback_reason = self._prepared_fallback
        days = [day.isoformat() for day in calendar.get_column("date").sort().to_list()]
        if (previous is None or old.get("semantics") != semantics
                or set(old.get("sources", {})) != set(sources)):
            fallback_reason = "initial_source_membership_or_lifecycle_semantics"
        old_days = set(old.get("calendar", []))
        changed.extend(old_days.symmetric_difference(days))
        previous_end = old.get("end_date")
        selected_end = end_date.isoformat() if end_date else None
        if previous_end != selected_end:
            # None means unbounded. Re-evaluate the first session that crosses
            # the earlier cutoff, including removal of a previously kept tail.
            boundary = min(value for value in (previous_end, selected_end) if value is not None)
            changed.extend(day for day in set(days) | old_days if day > boundary)
        self.identity = {"contract": CONTRACT, "implementation": self.implementation,
                         "semantics": semantics, "sources": sources, "calendar": days,
                         "end_date": selected_end}
        if fallback_reason is None and previous is not None:
            self.previous = previous
            self.start = date.fromisoformat(min(changed)) if changed else None
            self.metrics = {key: {day: row for day, row in values.items()
                                 if self.start is None or day < self.start.isoformat()}
                            for key, values in old["metrics"].items()}
            self.work["mode"] = "unchanged" if self.start is None else "suffix"
        else:
            self.work.update(mode="full", fallback_reason=fallback_reason)
        self.work["replace_from"] = self.start.isoformat() if self.start else None
        if self.work["mode"] != "unchanged":
            rows = 0
            archive_bytes = 0
            for receipt in receipts:
                try:
                    rows += pq.ParquetFile(receipt["path"]).metadata.num_rows
                except pa.ArrowException:
                    archive_bytes += Path(receipt["path"]).stat().st_size
            # Reserve before merging; no eviction or deletion to make room.
            growth = max(4 * MAX_CONTROL_BYTES, rows * 1024 + archive_bytes * 128 + 4 * MAX_CONTROL_BYTES)
            self.leases.enter_context(reserve_cache_write(self.root,
                key=_digest(self.identity), maximum_growth_bytes=growth,
                family="panel", policy=self.capacity_policy))
            self.budget = self.leases.enter_context(GenerationWriteBudget(growth))
        return self.start

    @property
    def unchanged(self):
        return self.work.get("mode") == "unchanged"

    def prefix(self):
        if self.previous is None:
            return None
        started = time.perf_counter()
        previous_end = self.previous.manifest["source_identity"].get("end_date")
        complete_prefix = (self.start is None or previous_end is not None
                           and self.start > date.fromisoformat(previous_end))
        # Reuse the canonical snapshot reader, including its complete member
        # proof, rather than a separate sequential Arrow decoder per year.
        query = self.previous.scan()
        if not complete_prefix:
            query = query.filter(pl.col("date") < pl.lit(self.start))
        frame = query.sort(["symbol", "date"]).collect()
        self.previous.verify()
        self.work["prefix_reader"] = "canonical_polars_snapshot_all_members_verified"
        self.work["reused_rows"] = frame.height
        self.work["prefix_read_and_sort_seconds"] = time.perf_counter() - started
        return frame

    def metric(self, name, frame, *, example=None):
        """Retain per-day counts and enough ordered witnesses for global top20."""
        values = dict(self.metrics.get(name, {}))
        groups = frame.group_by("date").len()
        examples = {}
        if example is not None:
            samples = frame.select("date", "symbol", example.alias("_example")).sort(["date", "symbol"])
            examples = {row["date"].isoformat(): row["_example"] for row in
                samples.group_by("date", maintain_order=True).agg(pl.col("_example").head(20)).to_dicts()}
        for day, count in groups.iter_rows():
            values[day.isoformat()] = {"count": count, "examples": examples.get(day.isoformat(), [])}
        self.metrics[name] = values

    def apply_metrics(self, statistics):
        for name, values in self.metrics.items():
            setattr(statistics, name, sum(value["count"] for value in values.values()))
            examples_name = name.removesuffix("_rows") + "_examples"
            if hasattr(statistics, examples_name):
                samples = [sample for row in values.values() for sample in row["examples"]]
                if name == "previous_session_next_reference_candidate_rows":
                    samples.sort(key=lambda item: (item.split(":", 2)[1], item.split(":", 1)[0]))
                else:
                    samples.sort()
                setattr(statistics, examples_name, samples[:20])
        return statistics

    def commit(self, suffix, statistics, *, prefix=None):
        self.apply_metrics(statistics)
        if self.unchanged:
            self.previous.verify()
            return prefix, statistics
        self.work["rebuilt_rows"] = suffix.height
        started = time.perf_counter()
        identity = {**self.identity, "metrics": self.metrics,
                    "statistics": asdict(statistics)}
        target = (self.previous.path if self.previous is not None else
                  self.root / f"generation_{uuid4().hex}" / "merged.parquet.manifest.json")
        self.work["partition_write"] = write_partitioned_table(
            suffix.sort(["date", "symbol"]).to_arrow(), target, period="year",
            source_identity=identity, replace_from=self.start if self.previous else None,
            write_budget=self.budget, previous_source=self.previous)
        pointer = {"manifest": str(target.relative_to(self.root)), "sha256": sha256_file(target)}
        self.budget.charge_json(pointer)
        atomic_write_json(self.root / "current.json", pointer)
        self.work["partition_commit_seconds"] = time.perf_counter() - started
        started = time.perf_counter()
        frame = suffix.sort(["symbol", "date"])
        if prefix is not None and not prefix.is_empty():
            # Both sides have unique symbol/date keys and disjoint date ranges.
            # Merge their proved order instead of re-sorting the entire history.
            key = "__official_merge_order"
            frame = prefix.with_columns(pl.struct("symbol", "date").alias(key)).merge_sorted(
                frame.with_columns(pl.struct("symbol", "date").alias(key)), key=key).drop(key)
        self.work["full_output_assembly_seconds"] = time.perf_counter() - started
        return frame, statistics

    def stats(self):
        return dict(self.work)
