"""Receipt-backed 13:25 information for the canonical overnight account."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from stockagent.data.panel import DAY_TRADE_OPEN_GAP_FEATURE, PanelData
from stockagent.data.tw_minute import _sha256

OVERNIGHT_1325_FEATURE = "next_session_1325_gap_logret"
OVERNIGHT_1325_CONTRACT_VERSION = 1
OVERNIGHT_CLOSE_FALLBACK_CONTRACT_VERSION = 2
OVERNIGHT_CLOSE_FALLBACK_CAVEAT = (
    "Missing 13:25 inputs use the same session's final close. "
    "These samples contain look-ahead relative to a 13:25 decision and "
    "are a user-selected research approximation, not executable 13:25 evidence."
)


def _validate_missing_policy(policy: str) -> bool:
    if policy not in {"reject", "same_session_close"}:
        raise ValueError("13:25 missing_price_policy must be reject or same_session_close")
    return policy == "same_session_close"


def overnight_minute_manifest(root: str | Path) -> dict:
    path = Path(root) / "manifest.json"
    if not path.is_file():
        raise RuntimeError(f"13:25 source manifest missing: {path}")
    manifest = json.loads(path.read_text())
    # v4/v5 share raw ts/Close. Their differing engineered features are unused.
    if (manifest.get("schema_version") not in {4, 5}
            or manifest.get("source") != "shioaji_kbars_1m"
            or manifest.get("research_ready") is not True
            or manifest.get("status") != "research_ready"
            or manifest.get("decision_clock") != "completed_right_labelled_1m_bar"):
        raise RuntimeError("13:25 requires receipt-verified right-labelled Shioaji Kbars")
    dates = manifest.get("dates", [])
    keys = [p.get("trade_date") for p in manifest.get("partitions", [])]
    if (not dates or dates != sorted(set(dates)) or len(keys) != len(set(keys))
            or set(keys) != set(dates)):
        raise RuntimeError("13:25 manifest calendar/partition identity mismatch")
    return manifest


def require_overnight_dates(manifest: dict, dates: np.ndarray) -> None:
    present = set(manifest["dates"])
    missing = [str(day) for day in np.asarray(dates, dtype="datetime64[D]")
               if str(day) not in present]
    if missing:
        raise RuntimeError(
            f"13:25 source lacks {len(missing)} requested sessions; "
            f"first={missing[:5]}, last={missing[-1]}; source spans "
            f"{manifest['dates'][0]}..{manifest['dates'][-1]}. "
            "No daily-price fallback or automatic horizon truncation is allowed."
        )


def attach_overnight_1325(panel: PanelData, root: str | Path, *,
                          missing_price_policy: str = "reject") -> PanelData:
    allow_fallback = _validate_missing_policy(missing_price_policy)
    if DAY_TRADE_OPEN_GAP_FEATURE in panel.feature_names:
        raise ValueError("13:25 must replace, not relabel, the opening-gap channel")
    if OVERNIGHT_1325_FEATURE in panel.feature_names:
        raise ValueError("13:25 context is already attached")
    root = Path(root).resolve()
    manifest_path = root / "manifest.json"
    manifest_sha256 = _sha256(manifest_path) if manifest_path.is_file() else None
    # Absence may use the authorized price approximation. Present-but-invalid
    # receipts, hashes and clocks remain integrity failures, never fallbacks.
    manifest = (overnight_minute_manifest(root) if manifest_sha256 is not None or not allow_fallback
                else {"dates": [], "partitions": []})
    if manifest_sha256 is not None and _sha256(manifest_path) != manifest_sha256:
        raise RuntimeError("13:25 manifest changed while selecting source")
    if not allow_fallback:
        require_overnight_dates(manifest, panel.dates[1:])
    summaries = {p["trade_date"]: p for p in manifest["partitions"]}
    symbols = {str(s): i for i, s in enumerate(panel.symbols)}
    prices = np.full(panel.close_prices.shape, np.nan, dtype=np.float64)
    missing_partitions = []
    verified_partitions = 0
    for index in range(1, panel.num_dates):
        day = str(np.datetime64(panel.dates[index], "D"))
        path = root / f"trade_date={day}" / "data.parquet"
        if not path.resolve().is_relative_to(root):
            raise RuntimeError(f"13:25 partition escapes source root: {path}")
        if allow_fallback and (day not in summaries or not path.exists()):
            missing_partitions.append(day)
            continue
        expected = summaries[day].get("output_sha256")
        if not path.is_file() or not expected or _sha256(path) != expected:
            raise RuntimeError(f"13:25 partition SHA256 mismatch or missing: {path}")
        table = pq.read_table(path, columns=["ts", "symbol", "Close", "minutes_from_open"],
                              filters=[("minutes_from_open", "=", 265)])
        seen = set()
        for row in table.to_pylist():
            ts = row["ts"]
            if (str(ts.date()) != day or (ts.hour, ts.minute, ts.second, ts.microsecond)
                    != (13, 25, 0, 0)):
                raise RuntimeError(f"13:25 timestamp disagrees with partition: {path}")
            if ts.tzinfo is not None and ts.utcoffset().total_seconds() != 8 * 3600:
                raise RuntimeError("13:25 timestamps must use Asia/Taipei exchange time")
            symbol = str(row["symbol"])
            if symbol in seen:
                raise RuntimeError(f"duplicate 13:25 observation: {day}/{symbol}")
            seen.add(symbol)
            if symbol in symbols and row["Close"] is not None:
                value = float(row["Close"])
                if np.isfinite(value) and value > 0:
                    prices[index, symbols[symbol]] = value
        if _sha256(path) != expected:
            raise RuntimeError(f"13:25 source changed while reading: {path}")
        verified_partitions += 1
        if verified_partitions == 1 or index % 100 == 0 or index == panel.num_dates - 1:
            print(f"[overnight] verified 13:25 partitions={verified_partitions} "
                  f"requested_session={index}/{panel.num_dates - 1} date={day}", flush=True)
    final_manifest_sha256 = _sha256(manifest_path) if manifest_path.is_file() else None
    if final_manifest_sha256 != manifest_sha256:
        raise RuntimeError("13:25 manifest changed while preparing panel")
    panel = attach_overnight_prices(panel, prices, missing_price_policy=missing_price_policy)
    panel.overnight_1325_source = {
        "source_root": str(root), "manifest_sha256": manifest_sha256,
        "first_source_date": manifest["dates"][0] if manifest["dates"] else None,
        "last_source_date": manifest["dates"][-1] if manifest["dates"] else None,
        "verified_session_partitions": verified_partitions,
    }
    if allow_fallback:
        fallback = panel.overnight_1325_close_fallback_mask
        panel.overnight_1325_source.update(
            contract_version=OVERNIGHT_CLOSE_FALLBACK_CONTRACT_VERSION,
            missing_price_policy=missing_price_policy,
            missing_partition_dates=missing_partitions,
            close_fallback_symbol_sessions=int(np.count_nonzero(fallback)),
            close_fallback_sessions=int(np.count_nonzero(np.any(fallback, axis=1))),
            caveat=OVERNIGHT_CLOSE_FALLBACK_CAVEAT,
        )
        print(f"[overnight] CLOSE FALLBACK research approximation: "
              f"missing_partitions={len(missing_partitions)} "
              f"symbol_sessions={np.count_nonzero(fallback)}", flush=True)
    return panel


def attach_overnight_prices(panel: PanelData, prices: np.ndarray, *,
                            missing_price_policy: str = "reject") -> PanelData:
    """Align verified observations; kept separate for causal-boundary tests."""
    allow_fallback = _validate_missing_policy(missing_price_policy)
    prices = np.array(prices, dtype=np.float64, copy=True)
    if prices.shape != panel.close_prices.shape:
        raise ValueError("13:25 prices must match the complete panel [T,S]")
    prior_close = np.full_like(prices, np.nan)
    prior_close[1:] = panel.close_prices[:-1]
    fallback = None
    if allow_fallback:
        close = np.asarray(panel.close_prices, dtype=np.float64)
        fallback = (~(np.isfinite(prices) & (prices > 0))
                    & np.isfinite(close) & (close > 0)
                    & np.isfinite(prior_close) & (prior_close > 0)
                    & np.asarray(panel.alive_mask, dtype=bool))
        prices[fallback] = close[fallback]
    available = np.isfinite(prices) & (prices > 0) & np.isfinite(prior_close) & (prior_close > 0)
    gap = np.zeros_like(prices, dtype=np.float32)
    gap[available] = np.log(prices[available] / prior_close[available])
    context = np.zeros_like(gap)
    context[:-1] = gap[1:]
    panel.features = np.concatenate((panel.features, context[..., None]), axis=-1)
    panel.feature_names = [*panel.feature_names, OVERNIGHT_1325_FEATURE]
    panel.overnight_1325_available = available
    panel.overnight_1325_close_fallback_mask = fallback
    # Recompute the logical array hashes after adding a new information source.
    panel.content_fingerprints = None
    return panel
