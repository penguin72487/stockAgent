"""Summarize per-symbol feature timings without confusing worker time with wall time."""

from __future__ import annotations

import json
import math
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def feature_run_summary_path(output_dir: Path, *, features_enabled: bool) -> Path:
    """Keep the latest feature-run evidence separate from candle-only refreshes."""

    name = (
        "download_summary.historical_features.json"
        if features_enabled
        else "download_summary.candles_only.json"
    )
    return output_dir / name


def stage_latency_summary(results: Iterable[Any]) -> dict[str, dict[str, float | int]]:
    """Use nearest-rank percentiles over completed worker samples."""

    samples: dict[str, list[float]] = {}
    for result in results:
        try:
            elapsed_by_stage = json.loads(result.stage_elapsed_seconds_json)
        except (AttributeError, TypeError, ValueError):
            continue
        if not isinstance(elapsed_by_stage, dict):
            continue
        for stage, raw_value in elapsed_by_stage.items():
            if not isinstance(stage, str) or isinstance(raw_value, bool):
                continue
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            if math.isfinite(value) and value >= 0:
                samples.setdefault(stage, []).append(value)
    summary: dict[str, dict[str, float | int]] = {}
    for stage, values in sorted(samples.items()):
        values.sort()
        summary[stage] = {
            "samples": len(values),
            "p50_seconds": round(values[(len(values) - 1) // 2], 3),
            "p95_seconds": round(values[math.ceil(0.95 * len(values)) - 1], 3),
            "aggregate_worker_seconds": round(sum(values), 3),
        }
    return summary
