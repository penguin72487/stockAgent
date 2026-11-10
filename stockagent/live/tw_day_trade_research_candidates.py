"""Source-pinned research results, explicitly separate from paper execution.

Only an opt-in registry is public. A research backtest is neither an active
paper account nor proof that its historical decisions were published live.
"""
from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import stat
import zlib
from zoneinfo import ZoneInfo

import polars as pl
import yaml

from stockagent.live.market_config import load_market_config, load_market_configs
from stockagent.live.performance_contract import compound_simple_returns, simple_return_frame
from stockagent.live.shioaji_schedule import HISTORICAL_QUERY_RESUME


_TAIPEI = ZoneInfo("Asia/Taipei")
_REGISTRY = Path("configs/deployments/public_research_candidates.yaml")
_FILES = frozenset({"checkpoint_best.pt", "daily_portfolio_returns.parquet", "metrics.json",
                    "fold_complete.json", "mode_artifact_contract.json"})
_PROVISIONAL_CONTRACT = "public_audited_provisional_daily_replay_v1"
_BRACKET_CONTRACT = "historical_bracket_full_on_strict_observed_cross_paper_assumption_v1"
_MINUTE_CONTRACT = "public_audited_provisional_minute_equity_v1"
_COUNTERFACTUAL_SIGNAL_CONTRACT = "public_source_checked_counterfactual_open_signal_v1"
# Match the replay owner's completed-session maintenance boundary. A closing
# print or a wall clock alone is never the source/ledger acceptance proof.
_RESEARCH_SESSION_RECEIPT_DEADLINE = HISTORICAL_QUERY_RESUME


def research_session_is_completed(day: date, observed_at: datetime) -> bool:
    """Apply the same Taipei session boundary to research writers and readers.

    This clock check is necessary, not proof of source coverage or a fill.
    Those must still pass the independent replay and publication validations.
    """
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observation time must be timezone-aware")
    local = observed_at.astimezone(_TAIPEI)
    return day.year == local.year and (
        day < local.date()
        or (day == local.date() and local.time() >= _RESEARCH_SESSION_RECEIPT_DEADLINE)
    )


def _pinned_bytes(path: Path, expected: str) -> bytes:
    if len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected):
        raise ValueError("invalid artifact digest")
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != expected:
        raise ValueError("research artifact identity changed")
    return payload


@lru_cache(maxsize=8)
def _read_minute_archive(path: Path, digest: str, generation: tuple[int, ...]) -> dict:
    """Load one bounded, content-pinned research projection, not a live ledger."""
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError("invalid research minute digest")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    with os.fdopen(fd, "rb") as source:
        before = os.fstat(source.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_size > 4 * 1024 * 1024
                or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != generation):
            raise ValueError("compressed research minutes changed or exceed bound")
        compressed = source.read(4 * 1024 * 1024 + 1)
        after = os.fstat(source.fileno())
        if (len(compressed) > 4 * 1024 * 1024
                or (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) != generation
                or hashlib.sha256(compressed).hexdigest() != digest):
            raise ValueError("research minute archive changed during read")
    with gzip.GzipFile(fileobj=io.BytesIO(compressed), mode="rb") as stream:
        raw = stream.read(16 * 1024 * 1024 + 1)
    if len(raw) > 16 * 1024 * 1024:
        raise ValueError("research minutes exceed decoded bound")
    info = path.stat()
    if (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns) != generation:
        raise ValueError("research minute archive changed during read")
    result = json.loads(raw)
    if not isinstance(result, dict):
        raise ValueError("research minute archive root is invalid")
    return result


def _validated_minute_archive(repo: Path, item: dict, *, market: str,
                              year: int, daily: dict) -> dict:
    entry = item.get("provisional_minutes") or {}
    path = (repo / str(entry.get("path") or "")).resolve(strict=True)
    if not path.is_relative_to((repo / "artifacts/operations/public_research").resolve()):
        raise ValueError("research minute archive is outside its public namespace")
    stat = path.stat()
    generation = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
    result = _read_minute_archive(path, str(entry.get("sha256") or ""), generation)
    sessions, expected = result.get("sessions"), daily.get("daily")
    if (result.get("contract") != _MINUTE_CONTRACT or result.get("market") != market
            or result.get("year") != year or result.get("summary_sha256") != (item.get("provisional_replay") or {}).get("sha256")
            or result.get("simulation_only") is not True or result.get("provisional") is not True
            or result.get("broker_fill") is not False or result.get("exchange_fill") is not False
            or result.get("source_completeness_verified") is not False
            or not isinstance(sessions, list) or not isinstance(expected, list)
            or len(sessions) != len(expected) or len(sessions) > 366
            or result.get("session_count") != len(sessions) or result.get("minute_count") != len(sessions) * 270):
        raise ValueError("research minute identity differs from daily summary")
    for session, reference in zip(sessions, expected, strict=True):
        if not isinstance(session, dict) or session.get("date") != reference.get("date"):
            raise ValueError("research minute dates differ from daily summary")
        values, stale = session.get("equity_twd"), session.get("valuation_stale")
        if (not isinstance(values, list) or not isinstance(stale, list)
                or len(values) != 270 or len(stale) != 270
                or any(type(x) not in (int, float) or not math.isfinite(x) or x <= 0 for x in values)
                or any(type(x) is not bool for x in stale)
                or sum(stale) != reference.get("carried_mark_minutes")
                or not math.isclose(values[-1], reference["end_equity_twd"], rel_tol=0.0, abs_tol=1e-5)):
            raise ValueError("research minute values differ from audited daily accounting")
    return result


def _confirmed_active_market(repo: Path, market: str) -> bool:
    """A YAML enablement is intent, not a durably acknowledged account."""
    from stockagent.live.tw_day_trade_live_snapshot import _read_once

    try:
        _receipt, files = _read_once(repo / "artifacts/live/tw_day_trade_simulation")
        status = files["status.json"].get("modes") or {}
        positions = files["positions.json"].get("modes") or {}
        return (isinstance(status, dict) and isinstance(positions, dict)
                and isinstance(status.get(market), dict) and status[market].get("market") == market
                and isinstance(positions.get(market), dict) and positions[market].get("market") == market)
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return False


def _public_counterfactual_signal(repo: Path, item: dict, *, market: str,
                                  checkpoint_sha256: str, observed_at: datetime) -> dict:
    path = (repo / str(item.get("path") or "")).resolve(strict=True)
    if (not path.is_relative_to((repo / "artifacts/operations/public_research").resolve())
            or path.stat().st_size > 1024 * 1024):
        raise ValueError("counterfactual signal is outside its bounded public namespace")
    result = json.loads(_pinned_bytes(path, str(item.get("sha256") or "")))
    if (not isinstance(result, dict) or result.get("contract") != _COUNTERFACTUAL_SIGNAL_CONTRACT
            or result.get("market") != market or result.get("checkpoint_sha256") != checkpoint_sha256
            or result.get("simulation_only") is not True or result.get("counterfactual") is not True
            or result.get("live_signal") is not False or result.get("paper_execution_enabled") is not False
            or result.get("broker_fill") is not False or result.get("exchange_fill") is not False
            or result.get("execution_and_minute_coverage_verified") is not False):
        raise ValueError("counterfactual signal identity or disclosure differs")
    day_text = str(result.get("session_date") or "")
    day = date.fromisoformat(day_text)
    generated = datetime.fromisoformat(str(result.get("generated_at") or ""))
    published = datetime.fromisoformat(str(result.get("published_at") or ""))
    if (day.isoformat() != day_text or generated.tzinfo is None or published.tzinfo is None
            or generated.utcoffset() is None or published.utcoffset() is None
            or not research_session_is_completed(day, generated)
            or not research_session_is_completed(day, observed_at)
            or not generated <= published <= observed_at):
        raise ValueError("counterfactual signal session or clock differs")
    rows = result.get("signals")
    if (not isinstance(rows, list) or len(rows) > 10_000
            or type(result.get("signal_count")) is not int or result["signal_count"] != len(rows)):
        raise ValueError("counterfactual signal list is incomplete")
    safe, seen = [], set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("invalid counterfactual signal")
        symbol, name, weight = row.get("symbol"), row.get("name"), row.get("target_weight_pct")
        if (not isinstance(symbol, str) or not re.fullmatch(r"[A-Z0-9]{1,12}", symbol)
                or symbol in seen or not isinstance(name, str) or len(name) > 80
                or type(weight) not in (int, float) or not math.isfinite(weight) or not 0 < abs(weight) <= 100
                or any(type(row.get(key)) is not bool for key in ("tradable", "can_buy", "can_sell"))):
            raise ValueError("invalid counterfactual signal row")
        seen.add(symbol)
        safe.append({key: row[key] for key in ("symbol", "name", "target_weight_pct", "tradable", "can_buy", "can_sell")})
    if safe != sorted(safe, key=lambda row: (-abs(row["target_weight_pct"]), row["symbol"])):
        raise ValueError("counterfactual signals are not sorted by absolute target")
    return {"status": "available", "session_date": day_text, "generated_at": generated.isoformat(),
            "signal_count": len(safe), "signals": safe, "counterfactual": True,
            "live_signal": False, "broker_fill": False, "execution_and_minute_coverage_verified": False}


def _public_provisional_replay(repo: Path, item: dict, *, market: str, capital: float,
                               year: int, observed_at: datetime) -> dict:
    path = (repo / str(item.get("path") or "")).resolve(strict=True)
    if not path.is_relative_to((repo / "artifacts/operations/public_research").resolve()):
        raise ValueError("provisional summary is outside its public research namespace")
    if path.stat().st_size > 1024 * 1024:
        raise ValueError("provisional summary exceeds its bounded transport")
    result = json.loads(_pinned_bytes(path, str(item.get("sha256") or "")))
    if (not isinstance(result, dict) or result.get("contract") != _PROVISIONAL_CONTRACT
            or result.get("market") != market or result.get("year") != year
            or result.get("simulation_only") is not True or result.get("broker_fill") is not False
            or result.get("exchange_fill") is not False or result.get("paper_execution_enabled") is not False
            or result.get("provisional") is not True or result.get("source_completeness_verified") is not False
            or result.get("all_rejected_entry_prices_independently_verified") is not False
            or result.get("historical_bracket_cross_assumption") != _BRACKET_CONTRACT
            or result.get("historical_decision_policy") != "single_counterfactual_open_not_published_revision_replay_v1"):
        raise ValueError("provisional summary is not a disclosed research replay")
    rows = result.get("daily")
    if not isinstance(rows, list) or not rows or len(rows) > 366 or result.get("session_count") != len(rows):
        raise ValueError("provisional daily summary is incomplete")
    generated_at = datetime.fromisoformat(str(result.get("generated_at") or ""))
    if (generated_at.tzinfo is None or generated_at.utcoffset() is None
            or generated_at > observed_at):
        raise ValueError("provisional summary has an invalid publication clock")
    prior, previous, stale_total, safe = capital, "", 0, []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("invalid provisional daily row")
        day = str(row.get("date") or "")
        if (date.fromisoformat(day).isoformat() != day or not day.startswith(f"{year:04d}-")
                or day <= previous or not research_session_is_completed(date.fromisoformat(day), observed_at)
                or not research_session_is_completed(date.fromisoformat(day), generated_at)):
            raise ValueError("provisional date is not a completed current-year session")
        equity, daily, cumulative = (float(row[name]) for name in
                                     ("end_equity_twd", "daily_return_pct", "cumulative_return_pct"))
        stale = row.get("carried_mark_minutes")
        if (not all(math.isfinite(value) for value in (equity, daily, cumulative)) or equity <= 0
                or type(stale) is not int or not 0 <= stale <= 270
                or not math.isclose(daily, (equity / prior - 1) * 100, rel_tol=1e-8, abs_tol=1e-7)
                or not math.isclose(cumulative, (equity / capital - 1) * 100, rel_tol=1e-8, abs_tol=1e-7)):
            raise ValueError("provisional return does not reconcile with equity")
        safe.append({"date": day, "end_equity_twd": equity, "daily_return_pct": daily,
                     "cumulative_return_pct": cumulative, "carried_mark_minutes": stale})
        prior, previous, stale_total = equity, day, stale_total + stale
    if (result.get("start_date") != safe[0]["date"] or result.get("end_date") != previous
            or result.get("minute_count") != len(safe) * 270 or result.get("carried_mark_minutes") != stale_total
            or not math.isclose(float(result.get("end_equity_twd")), prior, rel_tol=0.0, abs_tol=1e-5)
            or not math.isclose(float(result.get("cumulative_return_pct")), safe[-1]["cumulative_return_pct"],
                                rel_tol=0.0, abs_tol=1e-7)):
        raise ValueError("provisional summary totals do not reconcile")
    counts = [result.get(key) for key in ("full_cross_assumptions", "nontrade_assumptions")]
    if any(type(value) is not int or value < 0 for value in counts):
        raise ValueError("invalid provisional assumption counts")
    return {"status": "available", "start_date": safe[0]["date"], "end_date": previous,
            "session_count": len(safe), "minute_count": len(safe) * 270,
            "carried_mark_minutes": stale_total, "full_cross_assumptions": counts[0],
            "nontrade_assumptions": counts[1], "end_equity_twd": prior,
            "cumulative_return_pct": safe[-1]["cumulative_return_pct"], "daily": safe}


def _candidate(repo: Path, item: dict, *, observed_at: datetime, active: set[str]) -> dict | None:
    year, today = observed_at.year, observed_at.date().isoformat()
    relative = item.get("market_config")
    if not isinstance(relative, str) or not relative:
        raise ValueError("missing research market config")
    path = (repo / relative).resolve(strict=True)
    if not path.is_relative_to((repo / "configs/deployments/candidates").resolve()):
        raise ValueError("research config outside candidate directory")
    cfg = load_market_config(path)
    if cfg.market != item.get("expected_market"):
        raise ValueError("candidate market identity changed")
    configured = cfg.market in active
    if configured and _confirmed_active_market(repo, cfg.market):
        # A durably acknowledged account appears through the paper APIs, not here.
        return None
    if cfg.enabled or not cfg.day_trade_simulation_enabled or not cfg.checkpoint_path:
        raise ValueError("candidate is not a disabled paper configuration")
    capital = float(cfg.initial_capital or 0)
    if (not math.isfinite(capital) or capital <= 0 or cfg.fold_id is None
            or capital != float(item.get("expected_initial_capital_twd") or 0)):
        raise ValueError("candidate capital or fold is invalid")
    checkpoint = Path(cfg.checkpoint_path).resolve(strict=True)
    if checkpoint.name != "checkpoint_best.pt" or checkpoint.parent.name != f"fold_{cfg.fold_id:02d}":
        raise ValueError("candidate checkpoint identity does not match fold")
    pins = item.get("artifact_sha256")
    if not isinstance(pins, dict) or set(pins) != _FILES:
        raise ValueError("research artifact pins are incomplete")
    payloads = {name: _pinned_bytes(checkpoint.parent / name, str(pins[name])) for name in _FILES}
    complete = json.loads(payloads["fold_complete.json"])
    contract = json.loads(payloads["mode_artifact_contract.json"])
    metrics = json.loads(payloads["metrics.json"])
    details = contract.get("mode_details") or {}
    if (complete.get("status") != "complete" or complete.get("fold_id") != cfg.fold_id
            or complete.get("test_scope") != "full_horizon"
            or contract.get("execution_mode") != "tw_day_trade"
            or contract.get("frequency") != "daily_policy_exact_minute_execution"
            or details.get("daily_policy_decisions_per_session") != 1):
        raise ValueError("research contract is incompatible")
    rows = pl.read_parquet(io.BytesIO(payloads["daily_portfolio_returns.parquet"]))
    if not {"date", "portfolio_return"}.issubset(rows.columns):
        raise ValueError("research return columns missing")
    dates = [str(day)[:10] for day in rows["date"].to_list()]
    if (not dates or len(dates) != int(complete.get("test_rows") or -1)
            or dates != sorted(set(dates)) or dates[0] != complete.get("test_date_start")
            or dates[-1] != complete.get("test_date_end")):
        raise ValueError("research return dates disagree with completion receipt")
    # Training stores log returns. Use the shared idempotent decoder and
    # compound only simple DTO returns, never multiply the raw log values.
    decoded = simple_return_frame(rows)
    simple = decoded["portfolio_return"].to_list()
    if any(value is None for value in simple):
        raise ValueError("research return has an unknown observation")
    full = compound_simple_returns(simple)
    expected = float(metrics["test_metrics"]["cumulative_return"])
    if full is None or not math.isfinite(expected) or not math.isclose(full, expected, rel_tol=1e-9, abs_tol=1e-9):
        raise ValueError("research return does not match metrics")
    selected = [(day, value) for day, value in zip(dates, simple, strict=True)
                if day.startswith(f"{year:04d}-") and day <= today]
    result = {"market": cfg.market, "label": cfg.label, "initial_capital_twd": capital,
              "fold": cfg.fold_id,
              "status": "research_only_pending_paper" if configured else "research_only_not_enabled",
              "paper_execution_enabled": None if configured else False, "live_signal_available": False,
              "simulation_only": True, "broker_fill": False,
              "research_policy": "one_decision_per_session", "year": year,
              "source": "source_pinned_completed_model_backtest"}
    if "counterfactual_signal" in item:
        try:
            result["counterfactual_signal"] = _public_counterfactual_signal(
                repo, item["counterfactual_signal"], market=cfg.market,
                checkpoint_sha256=str(pins["checkpoint_best.pt"]), observed_at=observed_at)
        except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
            result["counterfactual_signal"] = {"status": "unavailable"}
    if "provisional_replay" in item:
        try:
            result["provisional_replay"] = _public_provisional_replay(
                repo, item["provisional_replay"], market=cfg.market, capital=capital, year=year,
                observed_at=observed_at)
            if item.get("provisional_minutes"):
                try:
                    _validated_minute_archive(repo, item, market=cfg.market, year=year,
                                              daily=result["provisional_replay"])
                    result["provisional_replay"]["minute_series_available"] = True
                except (OSError, TypeError, ValueError, KeyError, EOFError, UnicodeError, zlib.error):
                    result["provisional_replay"]["minute_series_available"] = False
        except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
            result["provisional_replay"] = {"status": "unavailable"}
    if not selected:
        return result | {"research_status": "no_current_year_data", "daily": []}
    values = []
    wealth = 1.0
    for day, daily_return in selected:
        wealth *= 1.0 + daily_return
        if not math.isfinite(wealth):
            raise ValueError("research equity is not finite")
        values.append({"date": day, "daily_return_pct": daily_return * 100.0,
                       "cumulative_return_pct": (wealth - 1.0) * 100.0})
    return result | {"research_status": "available", "start_date": selected[0][0],
                     "end_date": selected[-1][0], "session_count": len(selected),
                     "cumulative_return_pct": values[-1]["cumulative_return_pct"], "daily": values}


def build_research_candidates(*, repo_root: Path, observed_at: datetime | None = None) -> dict:
    repo = Path(repo_root).resolve(strict=True)
    now = observed_at or datetime.now(_TAIPEI)
    if now.tzinfo is None:
        raise ValueError("observation time must be timezone-aware")
    now = now.astimezone(_TAIPEI)
    registry = yaml.safe_load((repo / _REGISTRY).read_text(encoding="utf-8")) or {}
    if registry.get("schema_version") != 1 or not isinstance(registry.get("candidates"), list):
        raise ValueError("research registry invalid")
    active = {market for market, cfg in load_market_configs(repo / "services/discord_bot/markets").items()
              if cfg.enabled and cfg.day_trade_simulation_enabled}
    candidates = []
    for item in registry["candidates"]:
        try:
            candidate = _candidate(repo, item, observed_at=now, active=active)
        except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError, pl.exceptions.PolarsError):
            # Fail closed without exposing filesystem paths or weakening the
            # independently served six-mode current signal endpoint.
            candidate = {"status": "research_source_unavailable", "research_status": "unavailable"}
        if candidate is not None:
            candidates.append(candidate)
    return {"schema_version": 1, "observed_at": now.isoformat(), "candidates": candidates}


def build_research_minute_replay(*, repo_root: Path, market: str, session_date: str,
                                 observed_at: datetime | None = None) -> dict:
    """Return one audited research session; never expose prices or private paths."""
    unavailable = {"schema_version": 1, "status": "unavailable"}
    try:
        repo = Path(repo_root).resolve(strict=True)
        now = observed_at or datetime.now(_TAIPEI)
        if now.tzinfo is None:
            raise ValueError("observation time must be timezone-aware")
        now = now.astimezone(_TAIPEI)
        day = date.fromisoformat(session_date)
        if day.isoformat() != session_date or not research_session_is_completed(day, now):
            return unavailable
        registry = yaml.safe_load((repo / _REGISTRY).read_text(encoding="utf-8")) or {}
        if registry.get("schema_version") != 1 or not isinstance(registry.get("candidates"), list):
            return unavailable
        matches = [item for item in registry["candidates"] if isinstance(item, dict)
                   and item.get("expected_market") == market]
        if len(matches) != 1:
            return unavailable
        active = {key for key, cfg in load_market_configs(repo / "services/discord_bot/markets").items()
                  if cfg.enabled and cfg.day_trade_simulation_enabled}
        item = matches[0]
        candidate = _candidate(repo, item, observed_at=now, active=active)
        daily = (candidate or {}).get("provisional_replay") or {}
        if daily.get("status") != "available" or daily.get("minute_series_available") is not True:
            return unavailable
        archive = _validated_minute_archive(repo, item, market=market, year=now.year, daily=daily)
        selected = [row for row in archive["sessions"] if row["date"] == session_date]
        if len(selected) != 1:
            return unavailable
        values, stale = selected[0]["equity_twd"], selected[0]["valuation_stale"]
        return {"schema_version": 1, "status": "available", "market": market,
                "session_date": session_date, "first_minute": f"{session_date}T09:01:00+08:00",
                "interval_seconds": 60, "equity_twd": list(values), "valuation_stale": list(stale),
                "simulation_only": True, "provisional": True, "broker_fill": False,
                "exchange_fill": False, "source_completeness_verified": False}
    except (OSError, TypeError, ValueError, KeyError, EOFError, UnicodeError, zlib.error, pl.exceptions.PolarsError):
        # A public reader never receives a private source path or an unverified
        # fallback if the independent research archive cannot be checked.
        return unavailable
