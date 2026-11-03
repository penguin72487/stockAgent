#!/usr/bin/env python3
"""Compare today’s retained pre-open pointers without touching live owners.

This measures pointer admission only, not inference, fills, browser paint or
network/market latency. Artifacts are copied into an ephemeral private fixture;
the real signal pointers and paper journals are never modified.
"""

from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from datetime import datetime
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import run_tw_day_trade_simulation as candidate  # noqa: E402
from stockagent.live.market_config import SIGNAL_UPDATE_PROGRESSIVE  # noqa: E402
from stockagent.live.opening_signal_revision import revision_admission  # noqa: E402


def _frozen_baseline(path: Path):
    module_spec = importlib.util.spec_from_file_location("tw_preopen_baseline", path)
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("cannot load baseline")
    module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_spec.name] = module
    module_spec.loader.exec_module(module)
    return module


def _copy_checked(src: Path, dst: Path) -> str:
    before = src.stat()
    content = src.read_bytes()
    after = src.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns
    ):
        raise RuntimeError(f"source changed: {src}")
    dst.write_bytes(content)
    return hashlib.sha256(content).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-script", type=Path, required=True)
    parser.add_argument("--summary", type=Path, action="append", required=True)
    parser.add_argument("--session", required=True, help="YYYY-MM-DD")
    parser.add_argument("--tmp-dir", type=Path, default=Path("/dev/shm"))
    args = parser.parse_args()
    baseline = _frozen_baseline(args.baseline_script.resolve(strict=True))
    now = datetime.fromisoformat(f"{args.session}T09:00:03").replace(tzinfo=ZoneInfo("Asia/Taipei"))
    if shutil.disk_usage(args.tmp_dir).free < 128 * 1024 * 1024:
        raise RuntimeError("fixture filesystem has less than 128 MiB free")

    with tempfile.TemporaryDirectory(prefix="tw-preopen-admission-", dir=args.tmp_dir) as temp:
        fixture_root = Path(temp)
        specs, inputs = [], []
        for source in args.summary:
            source = source.resolve(strict=True)
            raw_bytes = source.read_bytes()
            raw = json.loads(raw_bytes)
            market = str(raw.get("market") or "")
            signal_id = str(raw.get("signal_id") or "")
            if (not market or Path(market).name != market or not signal_id
                    or Path(signal_id).name != signal_id or str(raw.get("asof_date") or "")[:10] != args.session):
                raise ValueError(f"invalid source identity: {source}")
            published = datetime.fromisoformat(str(raw.get("artifact_published_at") or ""))
            if (published.utcoffset() is None or published.astimezone(ZoneInfo("Asia/Taipei")).date() != now.date()
                    or published.astimezone(ZoneInfo("Asia/Taipei")).time() >= now.replace(hour=9, minute=0, second=0).time()):
                raise ValueError(f"source is not a pre-open artifact: {source}")
            if market in {item.market for item in specs}:
                raise ValueError(f"duplicate market: {market}")
            root = fixture_root / market
            result_dir = root / args.session / signal_id
            result_dir.mkdir(parents=True)
            summary_path = result_dir / "summary.json"
            weights_path = result_dir / "execution_weights.json"
            summary_sha = _copy_checked(source, summary_path)
            if summary_sha != hashlib.sha256(raw_bytes).hexdigest():
                raise RuntimeError(f"source changed during inspection: {source}")
            weights_sha = _copy_checked(source.parent / "execution_weights.json", weights_path)
            copied = json.loads(summary_path.read_text(encoding="utf-8"))
            if copied.get("signal_id") != signal_id or copied.get("market") != market:
                raise RuntimeError(f"source identity changed: {source}")
            pointer = {
                "schema_version": 2, "market": market, "signal_id": signal_id,
                "signal_ready_at": copied["signal_ready_at"],
                "artifact_published_at": copied["artifact_published_at"],
                "summary_path": str(summary_path),
                "weights_path": str(result_dir / "target_weights.parquet"),
                "execution_weights_path": str(weights_path),
            }
            (root / "latest_signal.json").write_text(json.dumps(pointer), encoding="utf-8")
            specs.append(SimpleNamespace(market=market, signal_market=None, live_output_dir=root))
            inputs.append({"market": market, "signal_id": signal_id,
                           "summary_sha256": summary_sha, "execution_weights_sha256": weights_sha})

        samples = []
        for name, module in (("baseline", baseline), ("candidate", candidate),
                             ("candidate", candidate), ("baseline", baseline)):
            module._LATEST_SIGNAL_CACHE.clear()
            if name == "candidate":
                module._PREOPEN_POINTER_REJECTIONS.clear()
            results = []
            start = time.perf_counter_ns()
            with redirect_stdout(io.StringIO()):
                for spec in specs:
                    results.append(module._latest_signal(spec, now))
            elapsed_ms = (time.perf_counter_ns() - start) / 1e6
            reasons = []
            if name == "baseline":
                for result in results:
                    if result is None:
                        raise RuntimeError("baseline did not consume retained pointer")
                    _, reason = revision_admission(policy=SIGNAL_UPDATE_PROGRESSIVE,
                        summary=result[0], mode={}, session_date=args.session)
                    reasons.append(reason)
                if any(reason != "signal_update_policy_mismatch" for reason in reasons):
                    raise RuntimeError(f"baseline admission differs: {reasons}")
            elif any(result is not None for result in results):
                raise RuntimeError("candidate admitted a pre-open pointer")
            samples.append({"implementation": name, "elapsed_ms": round(elapsed_ms, 3),
                            "pointers_decoded_as_signals": sum(result is not None for result in results),
                            "baseline_revision_reasons": reasons})
        print(json.dumps({"contract": "retained_preopen_pointer_admission_abba_v1",
            "measurement_boundary": "fixture_pointer_reads_only_excludes_eligibility_quotes_ledger_inference_and_browser",
            "session_date": args.session, "fixture_mode_count": len(specs),
            "baseline_script": str(args.baseline_script.resolve()),
            "fixtures": inputs, "samples": samples, "network_requests": False,
            "live_artifacts_or_ledgers_modified": False}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
