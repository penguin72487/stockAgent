"""Preopen model previews rank acquisition, never prices, signals or fills.

The existing warmup owns preparation; the shared opening fan-in owns consumption.
Only a small, dated private hint crosses that boundary. No network or model runs
here, and every requested symbol survives ordering, including unseen listings.
"""
from __future__ import annotations

from datetime import date, datetime, time as daytime
from functools import lru_cache
import fcntl
import hashlib
import json
import math
from pathlib import Path
import threading
import time
from zoneinfo import ZoneInfo

import numpy as np

from downloader.artifact_io import atomic_write_bytes

CONTRACT = "preopen_signal_recent_trade_volume_priority_v1"
TAIPEI = ZoneInfo("Asia/Taipei")
_WRITE_LOCK = threading.Lock()


def build_priority_rows(panel, panel_idx: int, weights_rows, last_trade_dates) -> list[dict]:
    """Use only completed rows; daily_volumes is canonical share capacity.

    Missing volume stays unknown, not zero. Replicated panels are not summed.
    The last-real-trade dates come from the signal owner's existing projection.
    """
    size = panel.num_symbols
    dates = np.asarray(last_trade_dates, dtype="datetime64[D]")
    if dates.shape != (size,) or not 0 <= panel_idx < len(panel.dates):
        raise ValueError("priority requires aligned completed history")
    cutoff = np.datetime64(panel.dates[panel_idx], "D")
    if np.any((~np.isnat(dates)) & (dates > cutoff)):
        raise ValueError("priority last trade exceeds the feature cutoff")
    mean = np.full(size, np.nan, dtype=np.float64)
    counts = np.zeros(size, dtype=np.int64)
    volumes = getattr(panel, "daily_volumes", None)
    if volumes is not None:
        closes, alive = panel.close_prices, panel.alive_mask
        if any(np.shape(values) != (len(panel.dates), size) for values in (volumes, closes, alive)):
            raise ValueError("priority requires aligned share-volume history")
        start = max(0, panel_idx - 19)
        v = np.asarray(volumes[start:panel_idx + 1], dtype=np.float64)
        p = np.asarray(closes[start:panel_idx + 1], dtype=np.float64)
        known = (np.asarray(alive[start:panel_idx + 1], dtype=bool)
                 & np.isfinite(v) & (v >= 0) & np.isfinite(p) & (p > 0))
        counts = known.sum(axis=0)
        np.divide(np.where(known, v, 0).sum(axis=0), counts, out=mean, where=counts > 0)
    by_symbol = {str(row["symbol"]): row for row in weights_rows}
    return [{"symbol": str(symbol),
             "abs_model_weight": abs(float(by_symbol[str(symbol)].get("model_weight") or 0.0)),
             "last_trade_date": None if np.isnat(dates[i]) else str(dates[i]),
             "mean_volume_shares_20d": float(mean[i]) if np.isfinite(mean[i]) else None,
             "volume_observed_days": int(counts[i])}
            for i, symbol in enumerate(panel.symbols)]


def _path(parquet_root, session_date: str) -> Path:
    from stockagent.live.quote_provider import _tw_mis_opening_receipt_path
    root_hash = hashlib.sha256(str(Path(parquet_root).resolve()).encode()).hexdigest()[:20]
    return _tw_mis_opening_receipt_path(session_date).with_name(f"{session_date}.{root_hash}.priority.json")


def _digest(payload) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False,
        allow_nan=False, separators=(",", ":")).encode()).hexdigest()


def model_identity_from_summary(summary) -> dict:
    result = {key: summary.get(key) for key in ("config_path", "config_fingerprint",
        "checkpoint_path", "checkpoint_fingerprint", "completed_panel_source_fingerprint")}
    result["dependencies"] = list(summary.get("preopen_priority_config_dependencies") or []) + [
        {"path": str(Path(summary[key]).absolute()),
        "identity": list(_dependency_identity(Path(summary[key])))}
        for key in ("config_path", "checkpoint_path") if summary.get(key)]
    return result


def _dependency_identity(path):
    stat = path.stat()
    return stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _dependencies_current(preview):
    try:
        return all(list(_dependency_identity(Path(item["path"]))) == item["identity"]
            for item in preview["model_identity"].get("dependencies", ()))
    except (OSError, KeyError, TypeError, ValueError):
        return False


def _valid_market(preview: dict, session_date: str) -> bool:
    try:
        cutoff = date.fromisoformat(preview["feature_date"])
        prepared = datetime.fromisoformat(preview["prepared_at"])
        rows = preview["rows"]
        model = preview["model_identity"]
        local = prepared.astimezone(TAIPEI)
        if (prepared.tzinfo is None or local.date().isoformat() != session_date
                or local.time() >= daytime(9) or cutoff >= date.fromisoformat(session_date)
                or not isinstance(rows, list) or not 0 < len(rows) <= 10_000
                or not isinstance(model, dict) or not model):
            return False
        dependencies = model.get("dependencies", [])
        # Annual deployments legitimately inherit more than 16 source files.
        # Keep a bounded advisory input without dropping a real active mode.
        if not isinstance(dependencies, list) or len(dependencies) > 256:
            return False
        for dependency in dependencies:
            if (not isinstance(dependency, dict) or not isinstance(dependency.get("path"), str)
                    or not Path(dependency["path"]).is_absolute()
                    or not isinstance(dependency.get("identity"), list) or len(dependency["identity"]) != 4
                    or any(type(value) is not int or value < 0 for value in dependency["identity"])):
                return False
        seen = set()
        for row in rows:
            symbol = row["symbol"]
            if not isinstance(symbol, str) or not symbol or symbol in seen:
                return False
            seen.add(symbol)
            weight = row["abs_model_weight"]
            if type(weight) not in (int, float) or not math.isfinite(weight) or weight < 0:
                return False
            traded = row.get("last_trade_date")
            if traded is not None and date.fromisoformat(traded) > cutoff:
                return False
            volume = row.get("mean_volume_shares_20d")
            count = row.get("volume_observed_days")
            if type(count) is not int or not 0 <= count <= 20:
                return False
            if volume is not None and (type(volume) not in (int, float)
                    or not math.isfinite(volume) or volume < 0 or count == 0):
                return False
        return True
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def _read(path, root, session_date):
    payload = json.loads(path.read_bytes())
    if not isinstance(payload, dict):
        raise ValueError("opening-priority hint must be an object")
    signature = payload.pop("sha256")
    if (payload.get("contract") != CONTRACT or payload.get("parquet_root") != root
            or payload.get("session_date") != session_date or signature != _digest(payload)
            or not isinstance(payload.get("markets"), dict) or not 0 < len(payload["markets"]) <= 64
            or not all(_valid_market(preview, session_date) for preview in payload["markets"].values())):
        raise ValueError("invalid opening-priority hint")
    return payload, signature


def publish_preopen_priority(*, parquet_root, session_date: str, market: str,
                             feature_date: str, rows, model_identity: dict,
                             prepared_at: datetime) -> dict:
    """Merge six already-computed previews; publication is private and atomic.

    A late/recovery warmup cannot backdate a preopen hint. Hint failure is not a
    reason to delay source acquisition; its consumer retains official order.
    """
    started = time.perf_counter()
    session_date = date.fromisoformat(session_date).isoformat()
    preview = {"feature_date": feature_date[:10], "prepared_at": prepared_at.astimezone(TAIPEI).isoformat()
               if prepared_at.tzinfo is not None else prepared_at.isoformat(),
               "model_identity": model_identity, "rows": rows}
    if not market or not _valid_market(preview, session_date):
        raise ValueError("priority requires a same-session, pre-09:00 completed-close preview")
    root = str(Path(parquet_root).resolve())
    path = _path(root, session_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _WRITE_LOCK, path.with_suffix(".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            payload, _ = _read(path, root, session_date)
        except (OSError, ValueError, KeyError, TypeError):
            # A bad advisory hint cannot block warmup or overwrite source data.
            payload = {"contract": CONTRACT, "parquet_root": root,
                       "session_date": session_date, "markets": {}}
        old = payload["markets"].get(market)
        if old is not None and old["prepared_at"] > preview["prepared_at"]:
            return {"status": "superseded", "market": market}
        payload["markets"][market] = preview
        signature = _digest(payload)
        atomic_write_bytes(path, json.dumps({**payload, "sha256": signature},
            ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode(), durable=True, mode=0o600)
    # Populate this source owner's small decoded/ranked cache before 09:00.
    # A restarted owner still validates the durable hint once; cold cost is
    # separately measured rather than claimed to have disappeared.
    all_symbols = list(dict.fromkeys(row["symbol"] for item in payload["markets"].values()
                                    for row in item["rows"]))
    _indices, warmed = priority_indices(all_symbols, parquet_root=root, session_date=session_date)
    return {"status": "prepared", "contract": CONTRACT, "sha256": signature,
            "path": str(path), "market": market, "market_count": len(payload["markets"]),
            "candidate_count": sum(row["abs_model_weight"] > 0 for row in rows),
            "measurement_boundary": "acquisition_hint_not_signal_or_fill",
            "rank_warmup_ms": warmed.get("sort_read_ms"),
            "prepare_publish_ms": round((time.perf_counter() - started) * 1000, 3)}


@lru_cache(maxsize=8)
def _loaded_hint(path: str, identity: tuple, root: str, session_date: str):
    file_path = Path(path)
    payload, signature = _read(file_path, root, session_date)
    if _file_identity(file_path) != identity:
        raise ValueError("priority publication changed during read")
    return payload, signature


@lru_cache(maxsize=8)
def _ranked_hint(path: str, identity: tuple, root: str, session_date: str, active_markets: tuple):
    payload, signature = _loaded_hint(path, identity, root, session_date)
    combined = {}
    for market, preview in sorted(payload["markets"].items()):
        if market not in active_markets:
            continue
        for row in preview["rows"]:
            item = combined.setdefault(row["symbol"], {"abs_model_weight": 0., "modes": 0})
            weight = row["abs_model_weight"]
            item["abs_model_weight"] = max(item["abs_model_weight"], weight)
            item["modes"] += int(weight > 0)
            # Latest completed source wins; never sum six copies of volume.
            generation = (preview["feature_date"], preview["prepared_at"], market)
            if generation > item.get("generation", ("", "", "")):
                item.update(generation=generation, last_trade_date=row.get("last_trade_date"),
                            mean_volume_shares_20d=row.get("mean_volume_shares_20d"))
    def key(symbol):
        item = combined[symbol]
        traded, volume = item["last_trade_date"], item["mean_volume_shares_20d"]
        return (not bool(item["modes"]), -(date.fromisoformat(traded).toordinal() if traded else 0),
                traded is None, volume is None, -(volume or 0.),
                -item["abs_model_weight"], -item["modes"], symbol)
    ranked = {symbol: index for index, symbol in enumerate(sorted(combined, key=key))}
    candidates = frozenset(s for s, item in combined.items() if item["modes"])
    return ranked, candidates, {"contract": CONTRACT, "sha256": signature,
        "markets": list(active_markets), "prepared_market_count": len(active_markets),
        "stale_market_count": len(payload["markets"]) - len(active_markets)}


def _file_identity(path):
    stat = path.stat()
    if stat.st_size > 8 * 1024**2:
        raise ValueError("opening-priority hint exceeds its bounded scope")
    return stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


@lru_cache(maxsize=32)
def _permutation(symbols: tuple[str, ...], path, identity, root, session_date, active):
    """Memoize indices, NOT prices, keyed by every hint/model generation."""
    ranks, candidates, evidence = _ranked_hint(path, identity, root, session_date, active)
    result = tuple(sorted(range(len(symbols)), key=lambda i: ranks.get(symbols[i], len(ranks) + i)))
    return result, {**evidence, "status": "prepared", "requested_count": len(symbols),
        "candidate_count": sum(symbol in candidates for symbol in symbols),
        "first_80_candidate_count": sum(symbols[i] in candidates for i in result[:80]),
        "first_500_candidate_count": sum(symbols[i] in candidates for i in result[:500]),
        "unprepared_symbol_count": sum(symbol not in ranks for symbol in symbols),
        "order_sha256": hashlib.sha256("\n".join(symbols[i] for i in result).encode()).hexdigest()}


def priority_indices(symbols, *, parquet_root, session_date: str):
    """Return a permutation only; neither universe nor caller arrays change."""
    started = time.perf_counter()
    original = list(range(len(symbols)))
    try:
        root = str(Path(parquet_root).resolve())
        path = _path(root, session_date)
        identity = _file_identity(path)
        payload, _ = _loaded_hint(str(path), identity, root, session_date)
        active = tuple(sorted(market for market, preview in payload["markets"].items()
                              if _dependencies_current(preview)))
        if not active:
            raise ValueError("all priority model dependencies changed")
        result, evidence = _permutation(tuple(symbols), str(path), identity, root, session_date, active)
    except (OSError, ValueError, KeyError, TypeError):
        return original, {"status": "unavailable", "contract": CONTRACT,
                          "fallback": "complete_caller_order", "requested_count": len(symbols)}
    return list(result), {**evidence, "markets": list(evidence["markets"]),
        "sort_read_ms": round((time.perf_counter() - started) * 1000, 3)}
