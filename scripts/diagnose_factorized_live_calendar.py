#!/usr/bin/env python3
"""Read-only canonical BASE/projection date comparison, no model or source update."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--hint-only", action="store_true", help="Inspect bounded source receipts only; never build the BASE panel.")
    args = parser.parse_args()
    if args.hint_only:
        from scripts import run_discord_artifact_maintenance as runner
        from downloader.artifact_io import atomic_write_json
        started = time.perf_counter()
        hint = runner._stale_factorized_projection_request()
        result = {"observed_at_utc": datetime.now(timezone.utc).isoformat(),
                  "status": "projection_refresh_advisory" if hint else "no_projection_refresh_hint",
                  "hint": hint, "elapsed_seconds": time.perf_counter() - started,
                  "model_runtime_imported": "torch" in sys.modules, "source_writes": 0,
                  "scope": "Receipt-only scheduling hint, not BASE coverage or completed-cache acceptance"}
        atomic_write_json(args.output, result)
        print(json.dumps(result, ensure_ascii=False))
        return
    if args.config is None:
        parser.error("--config is required without --hint-only")
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    pools = configure_cpu_source_runtime()
    import pyarrow as pa
    pa.set_cpu_count(pools["effective_threads"])
    import torch
    import requests
    from stockagent.config import load_config
    from stockagent.live import signal_engine as engine
    from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard
    from downloader.artifact_io import atomic_write_json

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Calendar diagnosis reached model/provider/CUDA")

    requests.Session.request = forbidden
    engine.generate_live_signal = forbidden
    engine._cached_live_model = forbidden
    for name in ("is_available", "init", "_lazy_init", "synchronize", "current_device"):
        setattr(torch.cuda, name, forbidden)
    if torch.cuda.is_initialized():
        raise RuntimeError("Calendar diagnosis must be CUDA cold")
    config_bytes = args.config.read_bytes()
    config = load_config(args.config)
    path = Path(config.data.factorized_feature_manifest).resolve(strict=True)
    manifest_bytes = path.read_bytes()
    manifest = json.loads(manifest_bytes)
    started = time.perf_counter()
    with live_tw_public_source_read_guard(config.data.parquet_root):
        panel, hit, tier = engine._build_panel_from_stable_source(config, live_tail=True)
        projection = manifest["dates"]
        requested = [str(day)[:10] for day in panel.dates if str(day)[:10] >= projection[0]]
        missing = sorted(set(requested) - set(projection))
        if path.read_bytes() != manifest_bytes or args.config.read_bytes() != config_bytes:
            raise RuntimeError("Calendar/config generation changed during diagnosis")
    if torch.cuda.is_initialized():
        raise RuntimeError("Calendar diagnosis initialized CUDA")
    receipt = {
        "status": "calendar_coverage_missing" if missing else "calendar_coverage_present",
        "observed_wall_seconds": time.perf_counter() - started,
        "config": str(args.config), "config_sha256": hashlib.sha256(config_bytes).hexdigest(),
        "projection_manifest": str(path), "projection_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "projection_range": [projection[0], projection[-1]], "projection_date_count": len(projection),
        "base_requested_range": [requested[0], requested[-1]], "base_date_count": len(requested),
        "base_symbols": panel.num_symbols, "base_features": list(panel.feature_names),
        "missing_dates": missing, "missing_date_count": len(missing),
        "missing_tail_only": bool(missing) and all(day > projection[-1] for day in missing),
        "paper_receipt": {key: (manifest.get("paper_inference") or {}).get(key)
                          for key in ("contract", "status", "completed_session", "decision_session")},
        "base_cache_hit": hit, "base_cache_tier": tier, "processing_pools": pools,
        "model_executed": False, "cuda_initialized": False, "source_writes": 0,
        "scope": "Canonical read-guarded BASE calendar only; not feature-value/completeness/model-output acceptance",
    }
    atomic_write_json(args.output, receipt)
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
