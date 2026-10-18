#!/usr/bin/env python3
"""Daily official membership refresh/audit; no inference, ledger or order.

The scheduler is independent of Discord/model warmup, but calls the same
canonical universe owner and its versioned current-product membership policy.
"""
from __future__ import annotations

import argparse
from datetime import datetime, time
import hashlib
import json
from pathlib import Path
import sys
from time import perf_counter
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from stockagent.data import tw_current_trading_universe as universe

TAIPEI = ZoneInfo("Asia/Taipei")
TPEX_ETF_CATALOGS = universe.TPEX_ETF_CATALOGS
parse_tpex_etf_catalog = universe.parse_tpex_etf_catalog


def _fetch_official(url: str) -> bytes:
    return universe._fetch(url)


def audit_tpex_etf_catalog(records: list[dict], session_date: str, *, fetch=None) -> dict:
    """Observe an official source conflict without changing the live scope.

    These are current observations, not a historical delisting reconstruction.
    Overlapping product categories are unioned, not added as independent issues.
    """
    fetch = fetch or _fetch_official
    root = universe.receipt_path(session_date).parent
    catalogs = {}
    started = perf_counter()
    raw, source_rows, errors = universe.collect_current_sources(
        tuple("tpex_etf_" + name for name in TPEX_ETF_CATALOGS), root, session_date, fetch=fetch)
    sources = {name.removeprefix("tpex_etf_"): row for name, row in source_rows.items()}
    failures = {name.removeprefix("tpex_etf_"): error for name, error in errors.items()}
    for key, content in raw.items():
        name = key.removeprefix("tpex_etf_")
        try:
            catalogs[name] = parse_tpex_etf_catalog(content, session_date)
        except (ValueError, KeyError, TypeError) as exc:
            failures[name] = type(exc).__name__
            sources[name]["status"] = "invalid_source"
            atomic_write_json(root / "source_state" / session_date / (key + ".json"), sources[name])
    isin = {r["symbol"] for r in records if r["market"] == "tpex" and r["security_type"] == "etf"}
    official = set().union(*catalogs.values())
    complete = len(catalogs) == len(TPEX_ETF_CATALOGS) and not failures and bool(official)
    mismatch = complete and isin != official
    return {"contract": "tw_current_tpex_etf_catalog_crosscheck_v1", "session_date": session_date,
            "status": "waiting_source" if not complete else "conflict" if mismatch else "matched",
            "observed_at_taipei": datetime.now(TAIPEI).isoformat(), "sources": sources,
            "failures": failures, "isin_count": len(isin), "catalog_count": len(official) if complete else None,
            "only_isin": sorted(isin - official) if complete else None,
            "only_catalog": sorted(official - isin) if complete else None,
            "runtime_membership_changed": False, "historical_pit_claim": False,
            "elapsed_ms": round((perf_counter() - started) * 1000, 3)}


def observe_current_tpex_catalog(session_date: str) -> dict:
    """Current source-only audit, also available on a non-trading day."""
    if session_date != datetime.now(TAIPEI).date().isoformat():
        raise ValueError("current catalogue observation cannot be backdated")
    url = universe.SOURCES["tpex_registry"]
    content = _fetch_official(url)
    observed = datetime.now(TAIPEI)
    if observed.date().isoformat() != session_date:
        raise ValueError("official registry observation crossed the day")
    records = universe.parse_registry(content, "otc", observed)
    digest = hashlib.sha256(content).hexdigest()
    root = universe.receipt_path(session_date).parent
    path = root / "raw" / session_date / ("tpex_registry." + digest + ".html")
    atomic_write_bytes(path, content, durable=True)
    result = audit_tpex_etf_catalog(records, session_date)
    result["registry_source"] = {"url": url, "observed_at_taipei": observed.isoformat(),
        "raw_path": str(path.resolve()), "sha256": digest, "bytes": len(content)}
    # This pointer cannot replace either a verified trading universe or the
    # calendar-aware scheduled result. A Sunday audit is not a Sunday session.
    audit_digest = hashlib.sha256(json.dumps(result, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    atomic_write_json(root / "catalog_audits" / session_date / (audit_digest + ".json"), result)
    atomic_write_json(root / "catalog_audit_status.json", result)
    return result


def accepted_catalog_audit(payload: dict) -> dict:
    """Read the owner's resolved policy, without fetching/parsing it twice.

    Keep the raw ISIN discrepancy visible. A verified exclusion is resolved,
    not a claim that the original ISIN rows suddenly match the active table.
    """
    source = payload["sources"][universe.MEMBERSHIP_SOURCE]
    content = Path(source["raw_path"]).read_bytes()
    if hashlib.sha256(content).hexdigest() != source["sha256"] or len(content) != source["bytes"]:
        raise ValueError("accepted membership policy raw evidence mismatch")
    policy = json.loads(content)
    comparison = policy["etf_catalog_comparison"]["tpex"]
    excluded = comparison["excluded_symbols"]
    return {"contract": "tw_current_tpex_etf_catalog_crosscheck_v1", "session_date": payload["session_date"],
            "status": "resolved" if excluded else "matched",
            "comparison_scope": "raw_isin_with_accepted_current_membership_resolution",
            "observed_at_taipei": payload["observed_at_taipei"],
            "sources": {name: payload["sources"]["tpex_etf_" + name] for name in TPEX_ETF_CATALOGS},
            "failures": {}, "isin_count": comparison["registry_count"],
            "catalog_count": comparison["active_catalog_count"],
            "only_isin": excluded, "only_catalog": comparison["catalog_only_symbols"],
            "resolved_excluded_symbols": excluded, "membership_contract": policy["contract"],
            "universe_fingerprint": payload["fingerprint"],
            "runtime_membership_changed": False, "historical_pit_claim": False,
            "additional_provider_requests": 0}


def scheduled_refresh(*, session_date: str, parquet_root: str | Path, now=None) -> dict:
    """Calendar-aware daily retry with no traffic in the protected open."""
    now = now or datetime.now(TAIPEI)
    if now.tzinfo is None or now.astimezone(TAIPEI).date().isoformat() != session_date:
        raise ValueError("daily universe update cannot be backdated")
    now = now.astimezone(TAIPEI)
    result = {"contract": "tw_daily_official_universe_update_v1", "session_date": session_date,
              "observed_at_taipei": now.isoformat(), "broker_orders": 0,
              "extra_shioaji_login": False, "ledger_modified": False, "inference_executed": False}
    if time(8, 57) <= now.time() < time(9, 10):
        # A boot/Persistent catch-up may otherwise compete with the very
        # opening it is meant to prepare. The next checkpoint owns recovery.
        return result | {"status": "deferred_protected_opening"}
    from stockagent.live.market_status import tw_stock_day_decision
    day = tw_stock_day_decision(now.date(), parquet_root=Path(parquet_root).resolve().parent, observed=now)
    result.update(calendar_status=day.status, calendar_reason=day.reason)
    if not day.is_session:
        return result | {"status": "closed" if day.status == "closed" else "waiting_calendar"}
    try:
        # Refresh short-lived halts preopen, but never relabel an existing
        # opening scope with a later-day observation after 09:00.
        payload = universe.refresh_current_universe(session_date,
            max_age_seconds=900 if now.time() < time(9) else None,
            parquet_root=parquet_root, fetch=_fetch_official)
    except universe.TWTradingUniversePending:
        return result | {"status": "waiting_source", "source_status_path": str((universe.receipt_path(session_date).parent / "refresh_status.json").resolve())}
    audit = accepted_catalog_audit(payload)
    result.update({k: payload[k] for k in ("fingerprint", "listed_count", "halted_count", "trading_count",
        "membership_contract", "registry_count", "excluded_count")})
    result.update(universe_contract=payload["contract"], universe_observed_at_taipei=payload["observed_at_taipei"],
                  catalog_audit=audit, status="ready" if audit["status"] in {"matched", "resolved"} else "degraded_catalog_" + audit["status"])
    return result


def publish_update(result: dict) -> Path:
    root = universe.receipt_path(result["session_date"]).parent
    digest = hashlib.sha256(json.dumps(result, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    path = root / "daily_updates" / result["session_date"] / (digest + ".json")
    atomic_write_json(path, result)
    atomic_write_json(root / "daily_update_status.json", result)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", default=datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat())
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--parquet-root", default="data_tw_public/stocks")
    parser.add_argument("--scheduled", action="store_true", help="Independent calendar-aware daily refresh and catalogue cross-check")
    parser.add_argument("--catalog-audit", action="store_true", help="Observe current TPEx ETF catalogue/ISIN disagreement; never change live membership")
    args = parser.parse_args()
    if args.catalog_audit:
        if args.force or args.scheduled:
            parser.error("catalogue audit is separate from scheduled/live membership promotion")
        result = observe_current_tpex_catalog(args.session_date)
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result["status"] == "matched" else 65 if result["status"] == "conflict" else 75
    if args.scheduled:
        if args.force:
            parser.error("scheduled updates own their source TTLs; no force refetch")
        result = scheduled_refresh(session_date=args.session_date, parquet_root=args.parquet_root)
        path = publish_update(result)
        print(json.dumps({k: v for k, v in result.items() if k != "catalog_audit"} | {"receipt_path": str(path)}, ensure_ascii=False))
        if result["status"] == "degraded_catalog_conflict":
            return 65  # Repeating a valid catalogue disagreement cannot fix it.
        return 0 if result["status"] in {"ready", "closed", "deferred_protected_opening"} else 75
    result = universe.refresh_current_universe(args.session_date, force=args.force, parquet_root=args.parquet_root)
    print(json.dumps({k: result[k] for k in ("contract", "session_date", "observed_at_taipei", "listed_count", "halted_count", "trading_count", "fingerprint", "elapsed_ms")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
