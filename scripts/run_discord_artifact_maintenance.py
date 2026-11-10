#!/usr/bin/env python3
"""Run one isolated pass of Discord-owned artifact maintenance.

The Discord Gateway must stay responsive even when full-universe history
inference consumes substantial CPU or memory.  This entry point deliberately
runs outside the bot service cgroup and communicates only through the existing
durable artifact-maintenance receipt.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import replace
from datetime import date, datetime, time as wall_time, timedelta, timezone
from pathlib import Path
from types import ModuleType
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending
from stockagent.data_sync.completed_forward_capacity import CapacityDeferred

discord_bot: ModuleType | None = None
ARTIFACT_OWNER_UNIT = "stockagent-discord-artifact-maintenance.service"
PRIORITY_SOURCE_UNIT = "stockagent-tw-day-trade-priority-source.service"
PRIORITY_SOURCE_START = wall_time(0, 0)
# Input recovery can continue after the open when a mode is still missing its
# validated input. Preserve the separate 13:20 exit/flattening window.
PRIORITY_SOURCE_END = wall_time(13, 20)
# Existing owner: 120min start budget + 90s stop grace; reserve controller/queue
# overhead too. Do not dispatch up to the opening fence using only start time.
ARTIFACT_RETRY_RUNWAY_MINUTES = 123
RETRY_RECEIPT_MAX_BYTES = 16 * 1024 * 1024
UNCHANGED_RETRY_WAKEUP_SECONDS = 300
# A missing live-session input gets a new chance on the next minute timer;
# systemd and the producer's existing lease prevent duplicate concurrent work.
PRIORITY_SOURCE_RETRY_WAKEUP_SECONDS = 60
# Measured complete 2014--2026 cold source: 3812 s plus source-owner wait.
# Keep the isolated recovery within its 120-min unit, reserving 30 min for
# formal inference and 10 min for cache/control overhead. Interactive callers
# retain their configured ordinary completed-session timeout.
FACTORIZED_SOURCE_RECOVERY_SECONDS = 80 * 60
CAPACITY_WAIT_CONTRACT = "artifact_history_capacity_backoff_v1"
CAPACITY_WAIT_MAX_SECONDS = 15 * 60
CAPACITY_HINT_TIMEOUT_SECONDS = 8
CURRENT_YEAR_HISTORY_CONTRACT = "tw_current_year_formal_history_v1"
_CAPACITY_HINT_CHILD = r'''
import json, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from stockagent.data_sync.cache_capacity import _admit_backing_volume
from stockagent.data_sync.completed_forward_capacity import CapacityDeferred
try:
    result = _admit_backing_volume(Path(sys.argv[1]), int(sys.argv[2]))
except CapacityDeferred as error:
    state = "blocked" if error.details.get("reason") == "physical_backing_reserve" else "unknown"
    print(json.dumps({"state": state, "free": None}))
except Exception:
    print(json.dumps({"state": "unknown", "free": None}))
else:
    free = result.get("physical_free_bytes") if isinstance(result, dict) else None
    state = "available" if type(free) is int and free >= int(sys.argv[2]) else "unknown"
    print(json.dumps({"state": state, "free": free if state == "available" else None}))
'''


def _preopen_source_priority_window(observed: datetime) -> bool:
    """Admit today's input recovery until the intraday exit window."""
    if observed.tzinfo is None:
        raise ValueError("preopen source admission requires an aware time")
    local = observed.astimezone(ZoneInfo("Asia/Taipei"))
    if not PRIORITY_SOURCE_START <= local.time().replace(tzinfo=None) < PRIORITY_SOURCE_END:
        return False
    from stockagent.live.market_status import tw_stock_day_decision

    decision = tw_stock_day_decision(local.date(), parquet_root=ROOT / "data_tw_public",
                                     observed=local)
    return decision.is_session


def _priority_source_requested(probe: dict, observed: datetime) -> bool:
    if os.environ.get("STOCKAGENT_PREOPEN_PRIORITY_SOURCE") != "1":
        return False
    keys = probe.get("due_keys") or ()
    return (any(str(key).startswith(("source_projection_refresh:", "source_cache_warming:"))
                for key in keys)
            and _preopen_source_priority_window(observed))


def _load_discord_bot() -> ModuleType:
    """Do not import Torch/model code for a receipt-only retry probe."""
    global discord_bot
    if discord_bot is None:
        from services.discord_bot import bot

        discord_bot = bot
    return discord_bot


def _current_history_year(*, now: datetime | None = None) -> int | None:
    """Read the bounded result-scope policy shared by the worker and launcher."""
    configured = os.environ.get("STOCKAGENT_FORMAL_HISTORY_SCOPE_POLICY")
    path = Path(configured) if configured else ROOT / "configs/deployments/tw_day_trade_formal_history_scope.json"
    if not path.is_absolute():
        path = ROOT / path
    try:
        with path.open("rb") as stream:
            raw = stream.read(8193)
    except FileNotFoundError:
        return None
    if len(raw) > 8192:
        raise ValueError("formal-history scope policy exceeds its bounded reader")
    policy = json.loads(raw)
    if (not isinstance(policy, dict)
            or set(policy) != {"contract", "enabled", "calendar_year"}
            or policy.get("contract") != CURRENT_YEAR_HISTORY_CONTRACT
            or type(policy.get("enabled")) is not bool):
        raise ValueError("invalid formal-history year scope policy")
    if not policy["enabled"]:
        return None
    observed = now or datetime.now(ZoneInfo("Asia/Taipei"))
    if observed.tzinfo is None:
        raise ValueError("formal-history scope requires an aware observation clock")
    current = observed.astimezone(ZoneInfo("Asia/Taipei")).year
    selected = policy["calendar_year"]
    if selected == "current":
        return current
    if type(selected) is not int or selected != current:
        raise ValueError("formal-history policy must select the current calendar year")
    return selected



def _year_scope_requires_new_owner(markets: list[str], bot: ModuleType) -> int | None:
    year = _current_history_year()
    if year is None:
        return None
    if not any(bool(getattr(bot._resolve_market(market), "day_trade_simulation_enabled", False))
               for market in markets):
        return None
    if getattr(bot, "FORMAL_HISTORY_SCOPE_CONTRACT", None) == CURRENT_YEAR_HISTORY_CONTRACT:
        return None
    return year


def _read_retry_json(path: Path) -> dict:
    try:
        with path.open("rb") as handle:
            raw = handle.read(RETRY_RECEIPT_MAX_BYTES + 1)
    except FileNotFoundError:
        return {}
    if len(raw) > RETRY_RECEIPT_MAX_BYTES:
        raise ValueError("artifact retry receipt exceeds the bounded reader size")
    result = json.loads(raw)
    if not isinstance(result, dict):
        raise ValueError("artifact retry receipt must be a JSON object")
    return result


def _backing_capacity_hint(required: int) -> tuple[str, int | None]:
    """Observe workspace backing capacity; this is never a write reservation.

    The actual writer must still recheck its own filesystem, root, leases and
    byte budget. Unknown probes cannot trigger an early retry.
    """
    if type(required) is not int or not 0 < required < 2**63:
        return "unknown", None
    # WSL interop discovery can otherwise exceed the controller's 30-second
    # service timeout. Isolate this advisory probe and bound the entire process
    # group; never leave a blocked helper holding up the trading guardian.
    try:
        process = subprocess.Popen(
            [sys.executable, "-c", _CAPACITY_HINT_CHILD, str(ROOT), str(required)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, start_new_session=True,
        )
    except OSError:
        return "unknown", None
    try:
        output, _ = process.communicate(timeout=CAPACITY_HINT_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except OSError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        try:
            process.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            # A descendant that escaped the new group is not ours to signal;
            # close our pipe and leave the sample unknown.
            if process.stdout is not None:
                process.stdout.close()
        return "unknown", None
    if process.returncode != 0 or len(output) > 1024:
        return "unknown", None
    try:
        result = json.loads(output)
    except (TypeError, ValueError):
        return "unknown", None
    if not isinstance(result, dict):
        return "unknown", None
    state, free = result.get("state"), result.get("free")
    if state == "blocked" and free is None:
        return "blocked", None
    if state == "available" and type(free) is int and free >= required:
        return "available", free
    return "unknown", None


def _capacity_wait_signature(job: dict) -> tuple:
    details = job.get("error_details")
    return (job.get("key"), job.get("status"), job.get("error_type"),
            job.get("run_id"), job.get("attempt"), job.get("completed_at"), job.get("next_retry_at"),
            job.get("capacity_wait_contract"), job.get("capacity_workspace_was_blocked"),
            job.get("capacity_source_hint"),
            details.get("reason") if isinstance(details, dict) else None,
            details.get("required_bytes_including_reserve") if isinstance(details, dict) else None)


def _capacity_source_hint() -> str | None:
    markets_dir = Path(os.environ.get("STOCKAGENT_MARKETS_DIR", str(ROOT / "services/discord_bot/markets")))
    if not markets_dir.is_absolute():
        markets_dir = ROOT / markets_dir
    try:
        return _retry_wakeup_fingerprint({}, markets_dir)
    except (OSError, ValueError):
        # A failed hint must not turn a recoverable capacity wait into a
        # maintenance failure. The normal bounded deadline remains active.
        return None


def _capacity_recovered_jobs(jobs: dict, *, observed: datetime) -> dict[str, tuple]:
    """Return advisory early-retry hints, with one physical probe per pass."""
    candidates = []
    for key, job in jobs.items():
        if not isinstance(job, dict) or job.get("status") != "waiting_source":
            continue
        details = job.get("error_details")
        required = details.get("required_bytes_including_reserve") if isinstance(details, dict) else None
        if (not isinstance(details, dict)
                or job.get("capacity_wait_contract") != CAPACITY_WAIT_CONTRACT
                or job.get("error_type") != "CapacityDeferred"
                or details.get("reason") != "physical_backing_reserve"):
            continue
        try:
            deadline = datetime.fromisoformat(job["next_retry_at"])
        except (KeyError, TypeError, ValueError):
            continue
        if deadline.tzinfo is None or deadline <= observed:
            continue
        candidates.append((key, job, required))
    if not candidates:
        return {}
    # A changed source/configuration can make an existing cache usable without
    # allocation. Wake the canonical owner, which still performs its checks.
    source_hint = _capacity_source_hint()
    recovered = {key: _capacity_wait_signature(job) for key, job, _ in candidates
                 if source_hint is not None and isinstance(job.get("capacity_source_hint"), str)
                 and job["capacity_source_hint"] != source_hint}
    candidates = [(key, job, need) for key, job, need in candidates
                  if key not in recovered and job.get("capacity_workspace_was_blocked") is True
                  and type(need) is int and 0 < need < 2**63]
    if not candidates:
        return recovered
    # One fresh physical observation is only a wakeup hint. It is not a
    # reservation, and may concern a different filesystem from the child.
    state, free = _backing_capacity_hint(min(item[2] for item in candidates))
    if state != "available" or free is None:
        return recovered
    recovered.update((key, _capacity_wait_signature(job)) for key, job, need in candidates if need <= free)
    return recovered


def _previous_capacity_wait(key: str) -> dict:
    with discord_bot._artifact_backfill_status_guard():
        job = (discord_bot._load_artifact_backfill_status().get("jobs") or {}).get(key)
    return dict(job) if isinstance(job, dict) else {}


def _schedule_capacity_wait(key: str, waiting: dict, previous: dict) -> dict:
    """Bound repeated full preparations without modifying the writer's gate."""
    details = waiting.get("error_details")
    if (not isinstance(details, dict) or details.get("reason") != "physical_backing_reserve"
            or waiting.get("status") != "waiting_source"):
        return waiting
    required = details.get("required_bytes_including_reserve")
    source_hint = _capacity_source_hint()
    old_details = previous.get("error_details")
    consecutive = (previous.get("status") == "waiting_source"
                   and isinstance(old_details, dict)
                   and old_details.get("reason") == "physical_backing_reserve"
                   and previous.get("capacity_wait_contract") == CAPACITY_WAIT_CONTRACT
                   and previous.get("capacity_source_hint") == source_hint)
    old_count = previous.get("capacity_wait_attempt", 0)
    count = min(16, old_count + 1) if consecutive and type(old_count) is int and old_count >= 0 else 1
    delay = min(CAPACITY_WAIT_MAX_SECONDS, 60 * (2 ** min(count - 1, 4)))
    # If this particular workspace was observed below the recorded threshold,
    # the cheap controller may later request an early retry when it rises above
    # it. The child might have used another filesystem: this is never admission.
    workspace_blocked = _backing_capacity_hint(required)[0] == "blocked"
    with discord_bot._artifact_backfill_status_guard():
        payload = discord_bot._load_artifact_backfill_status()
        job = (payload.get("jobs") or {}).get(key)
        if not isinstance(job, dict) or _capacity_wait_signature(job) != _capacity_wait_signature(waiting):
            return waiting
        completed = datetime.fromisoformat(job["completed_at"])
        job.update(capacity_wait_contract=CAPACITY_WAIT_CONTRACT,
                   capacity_wait_attempt=count, capacity_workspace_was_blocked=workspace_blocked,
                   capacity_source_hint=source_hint,
                   retry_delay_seconds=float(delay),
                   next_retry_at=(completed + timedelta(seconds=delay)).isoformat(timespec="seconds"))
        payload["updated_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
        discord_bot._write_artifact_backfill_status(payload)
        return dict(job)


def _capacity_hint_is_current(key: str, signature: tuple) -> bool:
    job = _previous_capacity_wait(key)
    return (job.get("status") == "waiting_source"
            and job.get("capacity_wait_contract") == CAPACITY_WAIT_CONTRACT
            and _capacity_wait_signature(job) == signature)


def _retry_wakeup_fingerprint(jobs: dict, markets_dir: Path) -> str:
    """Coalesce identical wakeups, never certify source freshness from stat.

    These are the same bounded close-publication event paths used by manual
    signals. A changed hint bypasses coalescing, but only the canonical worker
    may validate the contents and produce a ready artifact receipt.
    """
    receipt = Path(
        os.environ.get(
            "STOCKAGENT_TW_COMPLETED_SESSION_RECEIPT",
            "artifacts/data_refresh/tw_public/completed_session/latest.json",
        )
    )
    publication = Path(
        os.environ.get(
            "STOCKAGENT_TW_PUBLICATION_RECEIPT_ROOT",
            "artifacts/data_refresh/tw_public/publications",
        )
    )
    paths = [receipt, ROOT / "artifacts/discord_bot/state.json"]
    paths.extend(
        publication / phase / "latest.json"
        for phase in ("close_final", "close_revision", "close_initial", "close_event")
    )
    paths.extend(sorted(markets_dir.glob("*.yaml")))
    paths.extend(sorted(markets_dir.glob("*.yml")))
    hints = []
    for path in paths:
        if not path.is_absolute():
            path = ROOT / path
        try:
            stat = path.stat()
        except FileNotFoundError:
            hints.append((str(path), None))
        else:
            hints.append((str(path), stat.st_ino, stat.st_mtime_ns, stat.st_size))
    content = json.dumps(
        {"jobs": jobs, "source_event_hints": hints},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def _retry_dispatch_state_path() -> Path:
    path = Path(
        os.environ.get(
            "STOCKAGENT_ARTIFACT_RETRY_STATE_PATH",
            str(ROOT / "artifacts/discord_bot/artifact_retry_dispatch_latest.json"),
        )
    )
    return path if path.is_absolute() else ROOT / path


def _factorized_source_updater_receipt() -> Path:
    return ROOT / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1/updater_status.json"


def _deferred_factorized_source_request() -> dict | None:
    """A bounded wakeup hint, never acceptance of source/data/model readiness."""
    state = _read_retry_json(_factorized_source_updater_receipt())
    if state.get("status") == "projection_refresh_required":
        if (state.get("error_type") != "FactorizedPreopenProjectionPending"
                or state.get("phase") != "preopen"
                or state.get("last_good_generation_retained") is not True
                or state.get("model_executed") is not False):
            raise ValueError("unverified factorized projection request cannot invoke the source owner")
        return state
    if state.get("status") != "cache_warming_required":
        return None
    from stockagent.data.annual_cache_admission import CONTRACT

    admission = state.get("admission")
    if (state.get("error_type") != "AnnualCacheBuildDeferred"
            or not isinstance(admission, dict) or admission.get("contract") != CONTRACT
            or state.get("last_good_generation_retained") is not True):
        raise ValueError("unverified factorized warming request cannot invoke the source owner")
    return state


def _stale_factorized_projection_request(*, config_path: str | Path | None = None) -> dict | None:
    """A no-model wakeup hint for a preopen projection after an accepted close.

    A decision-session label is not proof its calendar includes that session's
    close. Only the canonical owner may extend the receipt-bound projection;
    this bounded reader cannot accept features, fabricate dates, or run a model.
    """
    completed_path = Path(os.environ.get(
        "STOCKAGENT_TW_COMPLETED_SESSION_RECEIPT",
        "artifacts/data_refresh/tw_public/completed_session/latest.json"))
    completed = _read_retry_json(completed_path if completed_path.is_absolute() else ROOT / completed_path)
    if completed.get("status") != "ok" or (completed.get("after") or {}).get("current") is not True:
        return None
    expected = completed.get("expected_date")
    if not isinstance(expected, str) or date.fromisoformat(expected).isoformat() != expected:
        raise ValueError("completed projection refresh date must be a canonical date")
    if date.fromisoformat(expected) > datetime.now(ZoneInfo("Asia/Taipei")).date():
        raise ValueError("future completed projection refresh is not observable")
    root = ROOT / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1"
    pointer = root / "current/factorized_manifest.json"
    if not pointer.is_file():
        return {"status": "projection_refresh_required", "expected_completed_session": expected,
                "projection_end": None, "manifest": str(pointer), "reason": "published_projection_missing",
                "advisory_only": True, "model_executed": False,
                "contract": "factorized_paper_projection_lag1_v1"}
    path = pointer.resolve(strict=True)
    if not path.is_relative_to(root.resolve()):
        raise ValueError("factorized projection pointer escapes the canonical source root")
    manifest = _read_retry_json(path)
    paper = manifest.get("paper_inference") or {}
    if (manifest.get("status") != "complete" or manifest.get("research_only") is not True
            or manifest.get("historical_point_in_time") is not False or manifest.get("feature_lag") != 1
            or paper.get("contract") != "factorized_paper_projection_lag1_v1"
            or paper.get("status") != "verified" or paper.get("raw_source_publication") is not False):
        raise ValueError("unverified factorized projection cannot request source recovery")
    dates = manifest.get("dates")
    if (not isinstance(dates, list) or not dates
            or any(not isinstance(day, str) or date.fromisoformat(day).isoformat() != day for day in dates)
            or len(dates) != len(set(dates)) or dates != sorted(dates)):
        raise ValueError("factorized projection dates must be unique ordered canonical dates")
    # A current tail does not satisfy the same deployment's formal-history
    # consumer. Use the canonical inherited config, not another hard-coded
    # cutoff. This path is used only by the selected source owner's worker;
    # the receipt-only retry controller still imports no model/config runtime.
    required_start = None
    source_start = (paper.get("source_identity") or {}).get("start_date")
    if config_path is not None:
        from stockagent.config import load_config

        selected = Path(config_path)
        selected = selected if selected.is_absolute() else ROOT / selected
        configured_start = load_config(selected).data.panel_start_date
        if configured_start:
            required_start = date.fromisoformat(configured_start).isoformat()
    if source_start is not None:
        source_start = date.fromisoformat(source_start).isoformat()
    # Missing legacy producer identity is not proof of an earlier coverage
    # boundary. The exact BASE/calendar preflight remains the acceptance gate.
    scope_gap = bool(required_start and (source_start or dates[0]) > required_start)
    calendar_change = None
    if dates[-1] >= expected and not scope_gap:
        # A cancelled session can advance the next decision day without
        # advancing the last completed close. Compare the producer's exact
        # calendar inputs too; a current tail alone must not suppress repair.
        # This is a wakeup hint only: the canonical producer still verifies
        # the official receipts and rebuilds publication-dependent inputs.
        from stockagent.live.market_status import TW_HOLIDAY_SCHEDULE_NAME

        names = (TW_HOLIDAY_SCHEDULE_NAME, "twse_taiex_ohlc.parquet",
                 "twse_taiex_ohlc.summary.json")
        identity = paper.get("source_identity") or {}
        proof = (identity.get("decision_calendar") or {}).get("input_sha256")
        if proof is None:
            calendar_change = {"reason": "decision_calendar_proof_missing"}
        else:
            if (not isinstance(proof, dict) or set(proof) != set(names)
                    or any(not isinstance(value, str) or len(value) != 64
                           or any(char not in "0123456789abcdef" for char in value)
                           for value in proof.values())):
                raise ValueError("invalid factorized decision-calendar input proof")
            current = {}
            for name in names:
                with (ROOT / "data_tw_public" / name).open("rb") as handle:
                    current[name] = hashlib.file_digest(handle, "sha256").hexdigest()
            if current == proof:
                return None
            calendar_change = {"reason": "decision_calendar_inputs_changed",
                               "current_calendar_sha256": current}
    return {"status": "projection_refresh_required", "expected_completed_session": expected,
            "projection_end": dates[-1], "manifest": str(path),
            "reason": "formal_history_scope_missing" if scope_gap else "completed_session_missing",
            "required_start_date": required_start,
            "projection_source_start": source_start or dates[0],
            **(calendar_change or {}),
            "advisory_only": True, "model_executed": False,
            "contract": "factorized_paper_projection_lag1_v1"}


def _owns_factorized_source_updater(cfg) -> bool:
    target = ROOT / "scripts/update_tw_day_trade_factorized_live.py"
    for item in (getattr(cfg, "completed_session_command", ()) or ()):
        path = Path(str(item))
        if (path if path.is_absolute() else ROOT / path).absolute() == target:
            return True
    return False


def retry_probe(*, observed: datetime | None = None) -> dict:
    """Advisory wakeup only; the canonical worker still owns every readiness gate.

    A ready receipt is not re-certified here. Only the newest job per currently
    scheduled market can wake the worker. A kernel-held worker lock, not a PID
    or timestamp, suppresses duplicate requests and proves a running owner.
    """
    started = time.perf_counter()
    observed = observed or datetime.now(timezone.utc)
    if observed.tzinfo is None:
        raise ValueError("retry probe requires a timezone-aware observation")
    status_path = Path(
        os.environ.get(
            "STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH",
            str(ROOT / "artifacts/discord_bot/artifact_backfill_status.json"),
        )
    )
    if not status_path.is_absolute():
        status_path = ROOT / status_path
    result = {
        "schema_version": 1,
        "observed_at": observed.isoformat(),
        "advisory_only": True,
        "due": False,
        "due_keys": [],
        "status_path": str(status_path),
        "reason": "no_due_retry",
        "job_count": 0,
        "ignored_invalid_jobs": 0,
    }

    def finish() -> dict:
        result["probe_seconds"] = time.perf_counter() - started
        result["model_runtime_imported"] = "torch" in sys.modules
        return result

    lock_path = status_path.with_name(f"{status_path.name}.worker.lock")
    try:
        with lock_path.open("rb") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                result["reason"] = "canonical_worker_running"
                return finish()
            fcntl.flock(handle, fcntl.LOCK_UN)
    except FileNotFoundError:
        pass
    payload = _read_retry_json(status_path)
    jobs = payload.get("jobs", {})
    if not isinstance(jobs, dict):
        raise ValueError("artifact retry jobs must be a JSON object")
    result["job_count"] = len(jobs)
    latest: dict[str, tuple[str, dict]] = {}
    for key, job in jobs.items():
        if not isinstance(job, dict):
            result["ignored_invalid_jobs"] += 1
            continue
        market = job.get("market")
        if (
            not isinstance(market, str)
            or not market
            or not key.endswith(f":{market}:artifact_backfill")
            or job.get("key", key) != key
        ):
            result["ignored_invalid_jobs"] += 1
            continue
        try:
            datetime.strptime(key.split(":", 1)[0], "%Y-%m-%d")
        except ValueError:
            result["ignored_invalid_jobs"] += 1
            continue
        if market not in latest or key > latest[market][0]:
            latest[market] = key, job
    capacity_recovered = _capacity_recovered_jobs(
        {key: job for key, job in latest.values()}, observed=observed,
    )
    candidates = []
    for market, (key, job) in latest.items():
        if job.get("status") == "running":
            # No canonical lock owner exists. The full worker will verify its
            # current artifacts/clock before recovering this interrupted job.
            candidates.append((key, market))
        elif job.get("status") in {"failed", "waiting_source"}:
            retry_at = job.get("next_retry_at")
            if not isinstance(retry_at, str):
                raise ValueError("retryable artifact job has no retry deadline")
            deadline = datetime.fromisoformat(retry_at)
            if deadline.tzinfo is None:
                raise ValueError("artifact retry deadline must be timezone-aware")
            if deadline <= observed or key in capacity_recovered:
                candidates.append((key, market))
    warming = _deferred_factorized_source_request() or _stale_factorized_projection_request()
    if not candidates and warming is None:
        return finish()

    # Reuse the lightweight canonical YAML loader, never load the trading/model
    # runtime merely to resolve the current scheduled/enabled market scope.
    from stockagent.live.market_config import load_market_configs

    markets_dir = Path(
        os.environ.get(
            "STOCKAGENT_MARKETS_DIR", str(ROOT / "services/discord_bot/markets")
        )
    )
    if not markets_dir.is_absolute():
        markets_dir = ROOT / markets_dir
    configs = load_market_configs(markets_dir)
    overrides = _read_retry_json(ROOT / "artifacts/discord_bot/state.json").get(
        "markets", {}
    )
    if not isinstance(overrides, dict):
        raise ValueError("market-state overrides must be a JSON object")
    scheduled = os.environ.get("STOCKAGENT_SCHEDULED_MARKETS", "").strip()
    selection = {item.strip() for item in scheduled.split(",") if item.strip()}
    all_markets = not selection or any(
        item.lower() in {"all", "*"} for item in selection
    )

    def enabled_override(name: str, default: bool) -> bool:
        entry = overrides.get(name, {})
        return (
            bool(entry.get("enabled", default)) if isinstance(entry, dict) else default
        )

    enabled = {
        name
        for name, cfg in configs.items()
        if (all_markets or name in selection) and enabled_override(name, cfg.enabled)
    }
    if not configs:
        fallback = os.environ.get("STOCKAGENT_DEFAULT_MARKET", "default")
        if (all_markets or fallback in selection) and enabled_override(fallback, True):
            enabled.add(fallback)
    due_jobs = {key: jobs[key] for key, market in candidates if market in enabled}
    if warming is not None:
        # Headroom is a resource snapshot, not a source generation. Coalesce
        # an unchanged warming request; committed source hints and the full
        # worker retain the publication/resource gates and last-good proof.
        if warming.get("status") == "projection_refresh_required":
            hint = {key: warming.get(key) for key in
                    ("status", "contract", "expected_completed_session", "projection_end", "manifest",
                     "reason", "current_calendar_sha256")}
            prefix = "source_projection_refresh"
        else:
            admission = warming["admission"]
            hint = {"phase": warming.get("phase"), "admission": {key: admission.get(key) for key in
                    ("contract", "source", "immutable_seed", "required_headroom_bytes", "reason")}}
            prefix = "source_cache_warming"
        for name, cfg in configs.items():
            if name in enabled and _owns_factorized_source_updater(cfg):
                due_jobs[f"{prefix}:{name}"] = hint
    result["due_keys"] = sorted(due_jobs)
    result["due"] = bool(result["due_keys"])
    result["reason"] = "retry_due" if result["due"] else "no_due_scheduled_retry"
    if result["due"]:
        result["wakeup_fingerprint"] = _retry_wakeup_fingerprint(due_jobs, markets_dir)
    return finish()


def _retry_owner_invocation(unit: str) -> dict[str, str]:
    """Read systemd's real unit/job identity; a queued start is not a run."""

    result = subprocess.run(
        ["systemctl", "show", "--no-pager", "--property=ActiveState",
         "--property=InvocationID", "--property=Job", unit],
        check=True, capture_output=True, text=True, timeout=10,
    )
    fields = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    if not {"ActiveState", "InvocationID", "Job"}.issubset(fields) or not fields["ActiveState"]:
        raise ValueError("systemd retry owner state is incomplete")
    return {"active_state": fields["ActiveState"], "invocation_id": fields["InvocationID"],
            "job": fields["Job"]}


def request_due_retry() -> int:
    probe = retry_probe()
    if not probe["due"]:
        _emit(**probe, requested=False)
        return 0
    state_path = _retry_dispatch_state_path()
    previous = _read_retry_json(state_path)
    now = datetime.now(timezone.utc)
    priority_source = _priority_source_requested(probe, now)
    owner_unit = PRIORITY_SOURCE_UNIT if priority_source else ARTIFACT_OWNER_UNIT
    wakeup_seconds = (PRIORITY_SOURCE_RETRY_WAKEUP_SECONDS if priority_source
                      else UNCHANGED_RETRY_WAKEUP_SECONDS)
    owner = _retry_owner_invocation(owner_unit)
    # The worker's OS lock can be released just before systemd observes its
    # process exiting. Starting an already-active unit is a successful no-op,
    # not an invocation; wait for a later timer rather than recording success.
    if owner["active_state"] not in {"inactive", "failed"} or owner["job"]:
        _emit(**probe, requested=False, owner_unit=owner_unit,
              dispatch_reason="owner_unit_busy_or_job_pending",
              owner_active_state=owner["active_state"])
        return 0
    if (previous.get("wakeup_fingerprint") == probe["wakeup_fingerprint"]
            and previous.get("owner_unit", ARTIFACT_OWNER_UNIT) == owner_unit):
        accepted_at = datetime.fromisoformat(previous["dispatch_accepted_at"])
        if accepted_at.tzinfo is None:
            raise ValueError("retry dispatch time must be timezone-aware")
        # A backwards wall-clock jump must not stall recovery indefinitely.
        # The canonical worker lock still protects a conservative fresh wakeup.
        elapsed = (now - accepted_at).total_seconds()
        previous_invocation = previous.get("owner_invocation_id_before")
        invocation_started = (
            isinstance(previous_invocation, str)
            and bool(owner["invocation_id"])
            and owner["invocation_id"] != previous_invocation
        )
        if 0 <= elapsed < wakeup_seconds and invocation_started:
            _emit(
                **probe,
                requested=False,
                dispatch_reason="unchanged_retry_wakeup_coalesced",
                unchanged_wakeup_seconds=wakeup_seconds,
            )
            return 0
    from scripts.check_outside_tw_opening_resource_window import evaluate

    window = evaluate(
        datetime.now(timezone.utc),
        minimum_runway_minutes=ARTIFACT_RETRY_RUNWAY_MINUTES,
        protected_until=wall_time(13, 40),
    )
    if not window["allowed"] and not priority_source:
        _emit(**probe, requested=False, reason_window="protected_market_runway")
        return 0
    subprocess.run(
        ["systemctl", "start", "--no-block", owner_unit],
        check=True,
        timeout=10,
    )
    from stockagent.data_sync.desync_snapshots import atomic_write_json

    dispatch = {
        "schema_version": 1,
        "advisory_only": True,
        "dispatch_accepted_at": datetime.now(timezone.utc).isoformat(),
        "owner_unit": owner_unit,
        # A subsequent probe coalesces only if systemd actually changed the
        # invocation after this request. Older receipts without this evidence
        # receive one conservative wakeup, not a false success claim.
        "owner_invocation_id_before": owner["invocation_id"],
        "due_keys": probe["due_keys"],
        "wakeup_fingerprint": probe["wakeup_fingerprint"],
    }
    try:
        atomic_write_json(state_path, dispatch)
    except Exception:
        # A start request is not completion. If persistence fails, retain that
        # distinction and fail the controller instead of claiming a durable retry.
        _emit(
            **probe,
            requested=True,
            owner_unit=owner_unit,
            dispatch_state_persisted=False,
        )
        raise
    _emit(
        **probe,
        requested=True,
        owner_unit=owner_unit,
        dispatch_state_persisted=True,
    )
    return 0


def request_due_postclose_cache() -> int:
    """Fallback when inotify is exhausted; this controller never imports Torch."""
    from scripts import discord_postclose_wakeup as wakeup
    from scripts.check_outside_tw_opening_resource_window import evaluate

    started = time.perf_counter()
    window = evaluate(datetime.now(timezone.utc), minimum_runway_minutes=wakeup.RUNWAY_MINUTES,
                      protected_until=wall_time(13, 40))
    if not window["allowed"]:
        _emit(status="postclose_wakeup_idle", reason="protected_market_runway",
              requested=False, model_runtime_imported="torch" in sys.modules)
        return 0
    status_path = wakeup.absolute(ROOT, os.environ.get("STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH") or
                                  "artifacts/discord_bot/artifact_backfill_status.json")
    source_lock = wakeup.absolute(ROOT, os.environ.get("STOCKAGENT_TW_PUBLIC_REFRESH_LOCK") or
                                  "/srv/stockagent-live/.locks/tw-public-refresh.lock")
    locks = (status_path.with_name("postclose_signal_cache.worker.lock"),
             status_path.with_name(status_path.name + ".worker.lock"))
    if (any(wakeup.lock_held(path) for path in locks)
            or wakeup.lock_held(source_lock, producer_only=True)):
        _emit(status="postclose_wakeup_idle", reason="canonical_owner_or_source_lock_held",
              requested=False, model_runtime_imported="torch" in sys.modules)
        return 0
    accepted = wakeup.read_json(wakeup.accepted_path(ROOT))
    dispatch_path = _retry_dispatch_state_path().with_name("postclose_dispatch_latest.json")
    previous = _read_retry_json(dispatch_path)
    snapshot = wakeup.observe(ROOT, previous=[accepted, previous])
    fingerprint = snapshot.get("fingerprint")
    reason = snapshot["reason"]
    if (fingerprint and accepted.get("contract") == wakeup.CONTRACT
            and accepted.get("status") == "canonical_artifacts_reusable"
            and accepted.get("fingerprint") == fingerprint and wakeup.artifacts_unchanged(accepted)):
        reason = "unchanged_verified_wakeup_generation"
        fingerprint = None
    now = datetime.now(timezone.utc)
    if fingerprint and previous.get("fingerprint") == fingerprint:
        accepted_at = datetime.fromisoformat(previous["dispatch_accepted_at"])
        if accepted_at.tzinfo is None:
            raise ValueError("postclose dispatch time must be timezone-aware")
        if 0 <= (now - accepted_at).total_seconds() < UNCHANGED_RETRY_WAKEUP_SECONDS:
            reason, fingerprint = "unchanged_postclose_wakeup_coalesced", None
    if not fingerprint:
        _emit(status="postclose_wakeup_idle", reason=reason, requested=False,
              member_count=snapshot["member_count"], probe_seconds=time.perf_counter() - started,
              model_runtime_imported="torch" in sys.modules)
        return 0
    subprocess.run(["systemctl", "start", "--no-block", wakeup.OWNER_UNIT], check=True, timeout=10)
    from downloader.artifact_io import atomic_write_json

    # Dispatch acceptance is deliberately separate from worker/data acceptance.
    atomic_write_json(dispatch_path, {**snapshot, "dispatch_accepted_at": now.isoformat(),
                                     "owner_unit": wakeup.OWNER_UNIT})
    _emit(status="postclose_wakeup_requested", requested=True,
          **{key: value for key, value in snapshot.items() if key != "observation_plan"},
          probe_seconds=time.perf_counter() - started, model_runtime_imported="torch" in sys.modules)
    return 0


def request_due_work() -> int:
    # The bounded latest-close worker and two-hour history owner have different
    # runway budgets. Never let history's restriction silence the cheap bridge.
    request_due_postclose_cache()
    return request_due_retry()


def _emit(**payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)


def _recover_deferred_factorized_source(
    cfg, *, allow_warming: bool = True, allow_market_open: bool = False,
) -> bool:
    """Let the existing isolated owner recover a gateway-denied cold cache.

    Run only the configured canonical updater, after an accepted official close.
    Do not require the derived panel to already be fresh: that is the missing
    artifact this operation repairs. Ordinary cache passes incur no new source
    command or memory probe. Opening and producer-lock gates remain with the
    surrounding worker and source owner.
    """
    expected = ROOT / "scripts/update_tw_day_trade_factorized_live.py"
    if not _owns_factorized_source_updater(cfg):
        return True
    source_config = getattr(cfg, "config_path", None) or None
    state = _deferred_factorized_source_request() or _stale_factorized_projection_request(config_path=source_config)
    if state is None:
        return True
    projection_refresh = state.get("status") == "projection_refresh_required"
    action = "factorized_projection_refresh" if projection_refresh else "factorized_cache_warming"
    if not allow_warming:
        _emit(status=f"{action}_deferred", market=cfg.market,
              reason="isolated_source_owner_required", owner_unit=ARTIFACT_OWNER_UNIT,
              model_executed=False)
        return False
    if _factorized_source_preparation_busy(cfg):
        # A cold full-history producer may legitimately outlive this command's
        # short activation budget. Do not queue another child on prepare.lock
        # or hold every other close-cache consumer behind it. This is a wait
        # hint only; the next pass still validates the actual committed source.
        _emit(status=f"{action}_deferred", market=cfg.market,
              reason="factorized_projection_in_progress", model_executed=False)
        return False
    bot = _load_discord_bot()
    status = bot._ensure_signal_ready(cfg)
    if ((status.market_open and not allow_market_open)
            or not bot._completed_session_publication_ready(status)):
        _emit(status=f"{action}_deferred", market=cfg.market,
              reason="accepted_official_close_required", model_executed=False)
        return False
    _emit(status=f"{action}_started", market=cfg.market,
          owner=ARTIFACT_OWNER_UNIT, source_owner=str(expected), model_executed=False,
          timeout_seconds=FACTORIZED_SOURCE_RECOVERY_SECONDS)
    recovery_cfg = replace(cfg, completed_session_timeout_seconds=FACTORIZED_SOURCE_RECOVERY_SECONDS)
    bot._run_completed_session_command(recovery_cfg, bypass_cache=True)
    if _read_retry_json(_factorized_source_updater_receipt()).get("status") not in {"verified", "already_current"}:
        raise RuntimeError("canonical factorized source recovery has no accepted receipt")
    if projection_refresh and (not (ROOT / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1/current/factorized_manifest.json").is_file()
                               or _stale_factorized_projection_request(config_path=source_config) is not None):
        raise RuntimeError("canonical factorized source recovery still lacks the completed calendar")
    _emit(status=f"{action}_verified", market=cfg.market,
          source_owner=str(expected), model_executed=False)
    return True


def _factorized_source_preparation_busy(cfg) -> bool:
    """Observe the canonical kernel lease, not an old lock file or process name."""
    if not _owns_factorized_source_updater(cfg):
        return False
    path = _factorized_source_updater_receipt().with_name("prepare.lock")
    try:
        handle = path.open("rb")
    except FileNotFoundError:
        return False
    with handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
    return False


def run_preopen_source_child() -> int:
    """Run only the shared canonical input producer, never history or inference."""
    if not _preopen_source_priority_window(datetime.now(timezone.utc)):
        _emit(status="preopen_source_deferred", reason="outside_admitted_session", model_executed=False)
        return 75
    bot = _load_discord_bot()
    status_path = bot._artifact_backfill_status_path()
    status_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = status_path.with_name(f"{status_path.name}.worker.lock")
    with lock_path.open("a+", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            _emit(status="preopen_source_deferred", reason="canonical_worker_running", model_executed=False)
            return 75
        # An interactive signal may itself be waiting for this exact source.
        # The shared updater and source locks arbitrate real concurrent writers;
        # treating the pending consumer as a blocker would deadlock recovery.
        if not _wait_for_tw_public_refresh(record_maintenance_run=False):
            _emit(status="preopen_source_deferred", reason="tw_public_refresh_wait_timeout", model_executed=False)
            return 75
        if not _preopen_source_priority_window(datetime.now(timezone.utc)):
            _emit(status="preopen_source_deferred", reason="preopen_deadline", model_executed=False)
            return 75
        for market in bot._artifact_maintenance_markets():
            cfg = bot._resolve_market(market)
            if not _owns_factorized_source_updater(cfg):
                continue
            if _recover_deferred_factorized_source(cfg, allow_market_open=True):
                # The normal producer validates receipts and generation; this
                # operation does not certify native warmup or all six modes.
                _emit(status="preopen_source_pass_finished", market=market, model_executed=False)
                return 0
            _emit(status="preopen_source_deferred", market=market, reason="source_not_ready", model_executed=False)
            return 75
    _emit(status="preopen_source_deferred", reason="no_enabled_source_consumer", model_executed=False)
    return 75


def _stop_preopen_source_group(proc: subprocess.Popen) -> None:
    """Terminate the whole owned producer tree, not only its Python parent."""
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


def run_preopen_source_supervised() -> int:
    """Bound today's input recovery before the separate exit/flattening phase."""
    now = datetime.now(timezone.utc)
    if not _preopen_source_priority_window(now):
        _emit(status="preopen_source_deferred", reason="outside_admitted_session", model_executed=False)
        return 75
    local = now.astimezone(ZoneInfo("Asia/Taipei"))
    deadline = datetime.combine(local.date(), PRIORITY_SOURCE_END, tzinfo=local.tzinfo)
    remaining = (deadline - local).total_seconds()
    proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--preopen-source-child"],
                            cwd=ROOT, start_new_session=True)
    try:
        return proc.wait(timeout=remaining)
    except subprocess.TimeoutExpired:
        _stop_preopen_source_group(proc)
        _emit(status="preopen_source_deferred", reason="preopen_deadline", model_executed=False)
        return 75
    except BaseException:
        _stop_preopen_source_group(proc)
        raise


def _verify_maintenance_gateway_generation() -> None:
    """Never acknowledge cache artifacts after their Gateway owner changes."""
    pid = os.environ.get("STOCKAGENT_MAINTENANCE_OWNER_PID")
    receipt = os.environ.get("STOCKAGENT_MAINTENANCE_RELEASE")
    if pid is None and receipt is None:
        return  # Direct focused callers do not claim namespace acceptance.
    if not pid or not pid.isdecimal() or not receipt:
        raise RuntimeError("incomplete maintenance Gateway source identity")
    from stockagent.live.service_code_release import configured_release, native_owner_pid

    if (configured_release(ROOT) != Path(receipt)
            or native_owner_pid(ROOT, role="discord") != int(pid)):
        raise RuntimeError("Discord Gateway owner or source release changed during maintenance")


def _populate_postclose_signal_caches(markets: list[str], *, allow_source_warming: bool = True) -> tuple[int, int, int]:
    from scripts import discord_postclose_wakeup as wakeup
    from stockagent.live.completed_inference_cache import LoadedImplementationChanged

    wakeup_before = wakeup.observe(ROOT)
    discord_bot = _load_discord_bot()
    attempted = 0
    failures = 0
    deferred = 0
    # A full worker may spend its entire source-recovery budget preparing the
    # shared Factorized/MGDA projection. Publish independent close caches first
    # instead of making them wait behind that cold source. This is a stable
    # partition, not another worker or a change to inference/report semantics.
    ordered_markets = (
        sorted(markets, key=lambda market: _owns_factorized_source_updater(
            discord_bot._resolve_market(market)))
        if allow_source_warming else markets
    )
    for market in ordered_markets:
        cfg = discord_bot._resolve_market(market)
        if not discord_bot._is_tw_daily_signal_market(cfg):
            continue
        attempted += 1
        try:
            _verify_maintenance_gateway_generation()
            if not _recover_deferred_factorized_source(cfg, allow_warming=allow_source_warming):
                deferred += 1
                continue
            result = discord_bot._run_completed_session_signal_cache_sync(cfg)
            _verify_maintenance_gateway_generation()
        except TWPublicSourceUpdatePending:
            # A writer may acquire the lock just after the outer busy check.
            # Stop this source-dependent pass, not the Gateway or source owner.
            raise
        except LoadedImplementationChanged:
            # This is process-wide, not one market's bad data. No later model
            # can be admitted in this process. Let main retain a worker failure
            # and the existing timer start fresh, without spending every
            # market's artifact retry/backoff or relabelling loaded code.
            raise
        except Exception as exc:
            failures += 1
            discord_bot._log_exception(
                f"postclose_signal_cache:{market}",
                exc,
            )
            _emit(
                status="postclose_cache_failed",
                market=market,
                error_type=type(exc).__name__,
                error_message=str(exc)[:500],
            )
            continue
        if result is not None:
            _emit(
                status="postclose_cache_ready",
                market=market,
                signal_id=result.summary.get("signal_id"),
                panel_date=result.summary.get("panel_date"),
                output_dir=str(result.output_dir),
            )
    if attempted and not failures and not deferred:
        _verify_maintenance_gateway_generation()
        acknowledged = wakeup.acknowledge(ROOT, wakeup_before, discord_bot)
        _emit(status="postclose_wakeup_readback", acknowledged=acknowledged,
              advisory_only=True)
        if not acknowledged:
            # Every call may have succeeded against a different source epoch.
            # A false readback cannot attribute a stale dependency to one mode,
            # but it does prove this whole pass is not yet coherent/complete.
            # Preserve successful artifacts and let the existing owner retry;
            # do not convert a source transition into a failed model attempt.
            deferred += 1
            _emit(status="postclose_cache_deferred",
                  reason="canonical_readback_not_accepted", owner_unit=ARTIFACT_OWNER_UNIT)
    return attempted, failures, deferred


def _record_signal_cache_maintenance_run(status: str, *, reason: str | None = None,
                                         attempted: int = 0, failures: int = 0,
                                         deferred: int = 0) -> dict:
    """Keep the independently locked cache worker out of history's receipt."""
    bot = _load_discord_bot()
    observed = datetime.now().astimezone().isoformat(timespec="seconds")
    with bot._artifact_backfill_status_guard():
        payload = bot._load_artifact_backfill_status()
        previous = payload.get("signal_cache_maintenance_run")
        previous = previous if isinstance(previous, dict) else {}
        row = {"status": status, "reason": reason, "attempted": int(attempted),
               "failures": int(failures), "deferred": int(deferred),
               "run_id": bot._BOT_RUN_ID, "updated_at": observed}
        if status == "running":
            row["started_at"] = (previous.get("started_at")
                if previous.get("run_id") == bot._BOT_RUN_ID else None) or observed
        else:
            row["started_at"] = (previous.get("started_at")
                if previous.get("run_id") == bot._BOT_RUN_ID else None) or observed
            row["completed_at"] = observed
        payload["signal_cache_maintenance_run"] = row
        payload["schema_version"] = max(2, int(payload.get("schema_version") or 1))
        payload["updated_at"] = observed
        bot._write_artifact_backfill_status(payload)
    return dict(row)


def _populate_postclose_signal_caches_or_defer(
        markets: list[str], *, allow_source_warming: bool = True,
        record_worker=None) -> tuple[int, int, int] | None:
    try:
        return _populate_postclose_signal_caches(markets, allow_source_warming=allow_source_warming)
    except TWPublicSourceUpdatePending as exc:
        discord_bot = _load_discord_bot()
        (record_worker or discord_bot._record_artifact_maintenance_run)(
            "waiting_source", reason="tw_public_refresh_in_progress", deferred=1,
        )
        _emit(status="waiting_source", **exc.details)
        return None


def _wait_for_tw_public_refresh(*, record_maintenance_run: bool = True,
                                record_worker=None) -> bool:
    """Wait through a transient writer collision instead of losing the run."""

    discord_bot = _load_discord_bot()
    timeout = max(
        0.0,
        discord_bot._env_float("STOCKAGENT_ARTIFACT_SOURCE_WAIT_SECONDS", 900.0),
    )
    poll = max(
        0.1,
        discord_bot._env_float("STOCKAGENT_ARTIFACT_SOURCE_POLL_SECONDS", 5.0),
    )
    started = time.monotonic()
    first = True
    while discord_bot._tw_public_refresh_in_progress():
        elapsed = time.monotonic() - started
        if first:
            recorder = record_worker or (discord_bot._record_artifact_maintenance_run
                                         if record_maintenance_run else None)
            if recorder is not None:
                recorder(
                    "waiting_source",
                    reason="tw_public_refresh_in_progress",
                )
            _emit(
                status="waiting_source",
                reason="tw_public_refresh_in_progress",
                timeout_seconds=timeout,
            )
            first = False
        if elapsed >= timeout:
            return False
        time.sleep(min(poll, max(0.0, timeout - elapsed)))
    if not first and (record_maintenance_run or record_worker is not None):
        # The source became available; make the durable worker state match the
        # work that is about to resume rather than leaving it at waiting_source.
        (record_worker or discord_bot._record_artifact_maintenance_run)("running")
    return True


def _completed_session_retry_key(cfg, status, *, now: datetime) -> str | None:
    """Resume an existing close job, never schedule a new non-session job.

    The daily Gateway scheduler deliberately creates no weekend/holiday key.
    The isolated retry owner may still finish the accepted latest close's
    durable failed/interrupted job. Call only after the canonical fresh-data
    and completed-session receipt gates; ordinary retry backoff still applies.
    """
    if (not bool(getattr(cfg, "day_trade_simulation_enabled", False))
            or not bool(getattr(getattr(status, "data", None), "fresh", False))):
        return None
    bot = _load_discord_bot()
    target = bot._completed_session_target_date(status)
    if not target:
        return None
    parsed = date.fromisoformat(target)
    if parsed.isoformat() != target or parsed > now.date():
        return None
    key = f"{target}:{cfg.market}:artifact_backfill"
    with bot._artifact_backfill_status_guard():
        job = (bot._load_artifact_backfill_status().get("jobs") or {}).get(key)
    if (not isinstance(job, dict) or job.get("market") != cfg.market
            or job.get("key", key) != key
            or job.get("status") not in {"failed", "running", "waiting_source"}):
        return None
    return key


def run_once(*, signal_cache_only: bool = False) -> int:
    discord_bot = _load_discord_bot()
    discord_bot._rotate_error_log_if_needed()
    status_path = discord_bot._artifact_backfill_status_path()
    status_path.parent.mkdir(parents=True, exist_ok=True)
    full_worker_lock_path = status_path.with_name(f"{status_path.name}.worker.lock")
    cache_worker_lock_path = status_path.with_name("postclose_signal_cache.worker.lock")
    worker_lock_path = (
        cache_worker_lock_path if signal_cache_only else full_worker_lock_path
    )

    with worker_lock_path.open("a+", encoding="utf-8") as worker_lock:
        try:
            fcntl.flock(worker_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            _emit(status="already_running", lock_path=str(worker_lock_path))
            return 0

        record_worker = (_record_signal_cache_maintenance_run if signal_cache_only
                         else discord_bot._record_artifact_maintenance_run)
        record_worker("running")
        if not signal_cache_only:
            # An older immutable Gateway must not silently ignore a new year
            # boundary. Keep caches in their independent owner and preserve all
            # prior job/failure receipts while the compatible owner is admitted.
            year = _year_scope_requires_new_owner(
                discord_bot._artifact_maintenance_markets(), discord_bot)
            if year is not None:
                reason = "current_year_inference_owner_required"
                record_worker("waiting_source", reason=reason, attempted=0, deferred=1)
                _emit(status="waiting_source", reason=reason, calendar_year=year,
                      result_start=f"{year}-01-01", result_end=f"{year}-12-31",
                      inference_started=False, raw_data_deleted=False,
                      signal_cache_owner="stockagent-discord-postclose-cache.service")
                return 0
        if discord_bot._opening_critical_work_pending():
            record_worker(
                "deferred", reason="opening_critical_work_pending"
            )
            _emit(status="deferred", reason="opening_critical_work_pending")
            return 0
        if discord_bot._interactive_signal_work_pending():
            record_worker(
                "deferred", reason="interactive_signal_work_pending"
            )
            _emit(status="deferred", reason="interactive_signal_work_pending")
            return 0
        source_available = (_wait_for_tw_public_refresh(record_maintenance_run=False, record_worker=record_worker)
                            if signal_cache_only else _wait_for_tw_public_refresh())
        if not source_available:
            record_worker(
                "deferred", reason="tw_public_refresh_wait_timeout"
            )
            _emit(status="deferred", reason="tw_public_refresh_wait_timeout")
            return 0

        markets = discord_bot._artifact_maintenance_markets()
        if signal_cache_only:
            population = _populate_postclose_signal_caches_or_defer(
                markets, allow_source_warming=False, record_worker=record_worker,
            )
            if population is None:
                return 0
            cache_attempted, cache_failures, cache_deferred = population
            cache_status = ("degraded" if cache_failures else
                            "waiting_source" if cache_deferred else
                            "complete" if cache_attempted else "not_applicable")
            _emit(
                status=f"signal_cache_{cache_status}",
                attempted=cache_attempted,
                failures=cache_failures,
                deferred=cache_deferred,
            )
            record_worker(
                cache_status,
                reason="postclose_cache_failed" if cache_failures else
                       "postclose_source_deferred" if cache_deferred else None,
                attempted=cache_attempted,
                failures=cache_failures,
                deferred=cache_deferred,
            )
            return 0 if cache_failures == 0 else 1

        with cache_worker_lock_path.open("a+", encoding="utf-8") as cache_lock:
            try:
                fcntl.flock(cache_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                discord_bot._record_artifact_maintenance_run(
                    "deferred", reason="postclose_signal_cache_in_progress"
                )
                _emit(
                    status="deferred",
                    reason="postclose_signal_cache_in_progress",
                )
                return 0
            _record_signal_cache_maintenance_run("running")

            def record_both_cache_and_history(status: str, **kwargs) -> None:
                _record_signal_cache_maintenance_run(status, **kwargs)
                record_worker(status, **kwargs)

            try:
                population = _populate_postclose_signal_caches_or_defer(
                    markets, record_worker=record_both_cache_and_history,
                )
            except Exception as exc:
                _record_signal_cache_maintenance_run(
                    "degraded", reason=f"unhandled_{type(exc).__name__}", failures=1,
                )
                raise
            if population is None:
                return 0
            cache_attempted, cache_failures, cache_deferred = population
            cache_status = ("degraded" if cache_failures else
                            "waiting_source" if cache_deferred else
                            "complete" if cache_attempted else "not_applicable")
            _record_signal_cache_maintenance_run(
                cache_status,
                reason="postclose_cache_failed" if cache_failures else
                       "postclose_source_deferred" if cache_deferred else None,
                attempted=cache_attempted, failures=cache_failures, deferred=cache_deferred,
            )

        with discord_bot._artifact_backfill_status_guard():
            prior_jobs = dict(discord_bot._load_artifact_backfill_status().get("jobs") or {})
        capacity_recovered = _capacity_recovered_jobs(prior_jobs, observed=datetime.now(timezone.utc))
        attempted = 0
        failures = 0
        deferred = 0
        retry_deferred = 0
        source_deferred = 0
        lock_deferred = 0
        for market in markets:
            cfg = discord_bot._resolve_market(market)
            if _factorized_source_preparation_busy(cfg):
                # Source work is not a failed inference attempt. Preserve the
                # existing retry receipt and let independent markets proceed.
                deferred += 1
                source_deferred += 1
                _emit(status="deferred", reason="factorized_projection_in_progress",
                      market=market, model_executed=False)
                continue
            if bool(getattr(cfg, "day_trade_simulation_enabled", False)):
                runtime_status = discord_bot._ensure_signal_ready_cached(cfg)
                if (
                    not runtime_status.data.fresh
                    or not discord_bot._completed_session_receipt_ready(runtime_status)
                ):
                    deferred += 1
                    _emit(
                        status="deferred",
                        reason="completed_session_not_ready",
                        market=market,
                        latest=runtime_status.data.last_data_date,
                        target=runtime_status.data.expected_latest_date,
                    )
                    continue
            now = datetime.now(ZoneInfo(cfg.timezone or "Asia/Taipei"))
            key = discord_bot._artifact_backfill_key(cfg, now)
            if key is None and bool(getattr(cfg, "day_trade_simulation_enabled", False)):
                key = _completed_session_retry_key(cfg, runtime_status, now=now)
                if key is not None:
                    _emit(status="resuming_completed_session", market=market, key=key,
                          new_session_job=False)
            if key is None or not discord_bot._market_has_model(cfg):
                continue
            if not discord_bot._artifact_backfill_retry_allowed(key):
                recovered = discord_bot._reconcile_artifact_backfill_if_current(
                    cfg,
                    key=key,
                    market=market,
                )
                if recovered:
                    continue
                if (not discord_bot._artifact_backfill_retry_allowed(key)
                        and not (key in capacity_recovered
                                 and _capacity_hint_is_current(key, capacity_recovered[key]))):
                    # Skipping a failed job during backoff does not prove its
                    # artifacts are current. Preserve its per-market receipt
                    # and make the worker's unfinished work visible as well.
                    deferred += 1
                    retry_deferred += 1
                    _emit(
                        status="deferred",
                        reason="artifact_retry_deferred",
                        market=market,
                        key=key,
                    )
                    continue
                # A current readback can CAS an obsolete ready hint to failed
                # with an immediate retry. Keep this same canonical serial
                # owner: do not defer repairs behind a later hour-long job or
                # spawn another inference worker. Recheck priority gates below.

            # A multi-market pass can run for hours. Recheck the independent
            # priority gates between expensive inferences, not only once at
            # worker startup; a close refresh or an interactive request may
            # begin while the previous market is still running.
            if discord_bot._opening_critical_work_pending():
                deferred += 1
                _emit(
                    status="deferred",
                    reason="opening_critical_work_pending",
                    market=market,
                )
                continue
            if discord_bot._interactive_signal_work_pending():
                deferred += 1
                _emit(
                    status="deferred",
                    reason="interactive_signal_work_pending",
                    market=market,
                )
                continue
            if not _wait_for_tw_public_refresh():
                deferred += 1
                _emit(
                    status="deferred",
                    reason="tw_public_refresh_wait_timeout",
                    market=market,
                )
                continue

            _verify_maintenance_gateway_generation()
            attempted += 1
            previous_wait = _previous_capacity_wait(key)
            discord_bot._begin_artifact_backfill(key, market)
            try:
                result = discord_bot._run_artifact_backfill_sync(cfg)
                _verify_maintenance_gateway_generation()
            except (TWPublicSourceUpdatePending, CapacityDeferred) as exc:
                deferred += 1
                lock_deferred += int(isinstance(exc, TWPublicSourceUpdatePending))
                waiting = discord_bot._finish_artifact_backfill(
                    key, market, status="waiting_source", exc=exc,
                )
                if isinstance(exc, CapacityDeferred):
                    waiting = _schedule_capacity_wait(key, waiting, previous_wait)
                _emit(
                    status="waiting_source", market=market, key=key,
                    attempt=waiting.get("attempt"),
                    next_retry_at=waiting.get("next_retry_at"),
                    **exc.details,
                )
                continue
            except Exception as exc:
                failures += 1
                failed = discord_bot._finish_artifact_backfill(
                    key,
                    market,
                    status="failed",
                    exc=exc,
                )
                discord_bot._log_exception(f"artifact_maintenance:{market}", exc)
                _emit(
                    status="failed",
                    market=market,
                    key=key,
                    attempt=failed.get("attempt"),
                    next_retry_at=failed.get("next_retry_at"),
                    error_type=type(exc).__name__,
                    error_message=str(exc)[:500],
                )
                continue

            ready = discord_bot._finish_artifact_backfill(
                key,
                market,
                status="ready",
            )
            summary = result.summary if result is not None else {}
            _emit(
                status="ready",
                market=market,
                key=key,
                attempt=ready.get("attempt"),
                signal_id=summary.get("signal_id"),
                panel_date=summary.get("panel_date"),
                output_dir=str(result.output_dir) if result is not None else None,
            )

        # Close caches and full-history artifacts have separate work/receipts.
        # A successful history pass cannot erase a real cache failure; waiting
        # for a source is neither an inference failure nor cache completion.
        attempted += cache_attempted
        failures += cache_failures
        deferred += cache_deferred
        final_status = (
            "degraded" if failures else "waiting_source" if deferred else "complete"
        )
        _emit(
            status=final_status,
            attempted=attempted,
            failures=failures,
            deferred=deferred,
            retry_deferred=retry_deferred,
            source_deferred=source_deferred,
            lock_deferred=lock_deferred,
            cache_attempted=cache_attempted,
            cache_failures=cache_failures,
            cache_deferred=cache_deferred,
        )
        discord_bot._record_artifact_maintenance_run(
            final_status,
            reason=("postclose_cache_failed" if cache_failures else
                    "artifact_retry_deferred" if retry_deferred else
                    "factorized_projection_in_progress" if source_deferred else
                    "tw_public_refresh_observed_in_child" if lock_deferred else
                    "postclose_source_deferred" if cache_deferred else None),
            attempted=attempted,
            failures=failures,
            deferred=deferred,
        )
        return 0 if failures == 0 else 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--signal-cache-only",
        action="store_true",
        help="Generate bounded latest-close caches without formal-history inference.",
    )
    retry_mode = parser.add_mutually_exclusive_group()
    retry_mode.add_argument("--retry-check-only", action="store_true")
    retry_mode.add_argument("--request-due-retry", action="store_true")
    retry_mode.add_argument("--request-due-work", action="store_true")
    retry_mode.add_argument("--preopen-source-only", action="store_true")
    retry_mode.add_argument("--preopen-source-child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.signal_cache_only and (args.retry_check_only or args.request_due_retry or args.request_due_work
                                   or args.preopen_source_only or args.preopen_source_child):
        parser.error("retry probe modes cannot generate signal caches")
    if args.preopen_source_only or args.preopen_source_child:
        raise SystemExit(run_preopen_source_child() if args.preopen_source_child
                         else run_preopen_source_supervised())
    if args.retry_check_only or args.request_due_retry or args.request_due_work:
        try:
            if args.request_due_work:
                return_code = request_due_work()
            elif args.request_due_retry:
                return_code = request_due_retry()
            else:
                probe = retry_probe()
                _emit(**probe)
                return_code = 0 if probe["due"] else 1
        except Exception as exc:
            _emit(
                status="retry_probe_failed",
                error_type=type(exc).__name__,
                error_message=str(exc)[:500],
            )
            # Unlike ExecCondition's normal skip range, 255 records a real
            # invalid-receipt/controller failure rather than silent success.
            return_code = 255
        raise SystemExit(return_code)
    try:
        return_code = run_once(signal_cache_only=bool(args.signal_cache_only))
    except Exception as exc:
        # A worker-level crash must not leave the durable state stuck at
        # `running`; per-market receipts remain untouched for diagnosis/retry.
        if discord_bot is not None:
            record_worker = (_record_signal_cache_maintenance_run if args.signal_cache_only
                             else discord_bot._record_artifact_maintenance_run)
            record_worker(
                "degraded",
                reason=f"unhandled_{type(exc).__name__}",
                failures=1,
            )
            discord_bot._log_exception("artifact_maintenance_worker", exc)
        _emit(
            status="degraded",
            reason=f"unhandled_{type(exc).__name__}",
            error_message=str(exc)[:500],
        )
        raise
    raise SystemExit(return_code)


if __name__ == "__main__":
    main()
