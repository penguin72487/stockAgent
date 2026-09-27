"""Read-only projection of the existing TAIFEX wrapper's stage-end events.

No producer, source-health or execution semantics live here. Evidence must bind
to one terminal process and boot; Bash SECONDS timings retain second resolution.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import math
from pathlib import Path
import re
import subprocess


TAIFEX_AUXILIARY_UNIT = "stockagent-taifex-auxiliary-daily.service"
_STAGES = ("options_daily", "recent_ticks", "final_settlement")
_IDENTITY = re.compile(r"[0-9a-f]{32}")
_EVENT = re.compile(
    r"\[taifex-auxiliary\] stage=(options_daily|recent_ticks|final_settlement) "
    r"exit_status=([0-9]{1,3}) elapsed_seconds=([0-9]{1,8})"
)
_LOCK_SKIP = "[taifex-auxiliary] another refresh owns the lock; leaving it running"


def _utc(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("unbound wall clock")
    return parsed.astimezone(UTC)


def _micros(value: object) -> int:
    if type(value) is int and value > 0:
        return value
    if isinstance(value, str) and re.fullmatch(r"[0-9]{1,20}", value) and int(value) > 0:
        return int(value)
    raise ValueError("invalid monotonic clock")


def taifex_auxiliary_stage_receipt(
    props: dict[str, object], snapshot: dict[str, object],
    journal_attempt: dict[str, object] | None,
) -> dict[str, object]:
    """Project only complete, exact-run step evidence; unknown stays unknown.

Current process identity takes precedence over historical journal recovery.
A skipped lock has no step timings. All validation is bounded and failure-safe;
provider text and arbitrary journal fields never enter the public projection.
"""

    result: dict[str, object] = {
        "receipt_source": "systemd_stage_journal", "matches_last_attempt": False,
        "state": "unknown", "completion_claim": "recorded_wrapper_steps_only",
        "source_completeness": "not_checked",
    }
    if props.get("ActiveState") not in ("inactive", "failed"):
        return result
    try:
        observed = _utc(snapshot.get("observed_at"))
        if props.get("ExecMainStartTimestampMonotonic"):
            start_us = _micros(props["ExecMainStartTimestampMonotonic"])
            end_us = _micros(props.get("ExecMainExitTimestampMonotonic"))
            sampled = snapshot.get("monotonic")
            if type(sampled) not in (float, int) or not math.isfinite(sampled):
                return result
            if not 0 < start_us <= end_us <= sampled * 1e6:
                return result
            started = observed - timedelta(seconds=sampled - start_us / 1e6)
            finished = observed - timedelta(seconds=sampled - end_us / 1e6)
            identity = props.get("InvocationID")
            boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip().replace("-", "")
            outcome = props.get("Result")
            exit_status = props.get("ExecMainStatus")
            if (outcome, exit_status) not in (("success", 0), ("exit-code", 1)):
                return result
        else:
            attempt = journal_attempt or {}
            identity = attempt.get("last_attempt_invocation_id")
            boot_id = attempt.get("last_attempt_boot_id")
            if props.get("InvocationID") not in (None, "", identity):
                return result
            start_us = _micros(attempt.get("last_attempt_started_monotonic_us"))
            end_us = _micros(attempt.get("last_attempt_completed_monotonic_us"))
            started = _utc(attempt.get("last_attempt_started_at_utc"))
            finished = _utc(attempt.get("last_attempt_completed_at_utc"))
            outcome = attempt.get("last_attempt_outcome")
            exit_status = attempt.get("last_attempt_exit_status")
            if (outcome == "process_exited_zero"
                    and (exit_status is None or (type(exit_status) is int and exit_status == 0))
                    and attempt.get("last_attempt_exit_code") in (None, "exited")):
                exit_status = 0  # PID 1's explicit success event is the proof.
            elif not (outcome == "failed" and exit_status == 1
                      and attempt.get("last_attempt_exit_code") == "exited"):
                return result
        if type(exit_status) is not int:
            return result
        if any(not isinstance(value, str) or _IDENTITY.fullmatch(value) is None
               for value in (identity, boot_id)):
            return result
        if (not start_us <= end_us or not started <= finished <= observed
                or abs((finished - started).total_seconds() - (end_us - start_us) / 1e6) > 1):
            return result
        process = subprocess.run(
            ["journalctl", f"_SYSTEMD_UNIT={TAIFEX_AUXILIARY_UNIT}",
             f"_SYSTEMD_INVOCATION_ID={identity}", f"_BOOT_ID={boot_id}",
             "--since", started.isoformat(), "--until", observed.isoformat(),
             "--grep=^\\[taifex-auxiliary\\]", "-n", "16", "-o", "json", "--no-pager",
             "--output-fields=MESSAGE,_SYSTEMD_UNIT,_SYSTEMD_INVOCATION_ID,_BOOT_ID,"
             "__REALTIME_TIMESTAMP,__MONOTONIC_TIMESTAMP"],
            check=False, capture_output=True, text=True, timeout=2,
        )
        if process.returncode != 0 or len(process.stdout) > 65536:
            return result
        events = []
        lines = process.stdout.splitlines()
        if not 1 <= len(lines) <= 16:
            return result
        for line in lines:
            event = json.loads(line)
            if (not isinstance(event, dict) or event.get("_SYSTEMD_UNIT") != TAIFEX_AUXILIARY_UNIT
                    or event.get("_SYSTEMD_INVOCATION_ID") != identity
                    or event.get("_BOOT_ID") != boot_id):
                return result
            mono = _micros(event.get("__MONOTONIC_TIMESTAMP"))
            realtime = datetime.fromtimestamp(_micros(event.get("__REALTIME_TIMESTAMP")) / 1e6, UTC)
            if not start_us <= mono <= end_us or not started <= realtime <= finished:
                return result
            if abs((realtime - started).total_seconds() - (mono - start_us) / 1e6) > 1:
                return result
            message = event.get("MESSAGE")
            if not isinstance(message, str):
                return result
            events.append((mono, realtime, message))
        events.sort(key=lambda event: event[0])
        if len(events) == 1 and events[0][2] == _LOCK_SKIP and exit_status == 0:
            result.update(matches_last_attempt=True, state="deferred", reason="lock_busy",
                          systemd_invocation_id=identity, boot_id=boot_id)
            return result
        if len(events) != len(_STAGES):
            return result
        steps = []
        previous_mono, previous_real = start_us, started
        for expected, (mono, realtime, message) in zip(_STAGES, events):
            match = _EVENT.fullmatch(message)
            if match is None or match[1] != expected or mono <= previous_mono or realtime < previous_real:
                return result
            status, elapsed = int(match[2]), int(match[3])
            # The wrapper samples Bash SECONDS before/after each serial command.
            # Integer quantization and wrapper/logging gaps are not millisecond precision.
            if status > 255 or abs((mono - previous_mono) / 1e6 - elapsed) > 2:
                return result
            steps.append({"step": expected, "elapsed_seconds": elapsed,
                          "exit_status": status, "finished_at_utc": realtime.isoformat()})
            previous_mono, previous_real = mono, realtime
        failed = [step["step"] for step in steps if step["exit_status"] != 0]
        if exit_status != int(bool(failed)):
            return result
        result.update(
            matches_last_attempt=True, state="completed_with_failures" if failed else "completed",
            systemd_invocation_id=identity, boot_id=boot_id,
            started_at_utc=started.isoformat(), finished_at_utc=finished.isoformat(),
            elapsed_seconds=round((end_us - start_us) / 1e6, 6),
            step_receipts_scope="exact_matched_run", step_receipts=steps,
            failed_steps=failed, timing_resolution_seconds=1,
        )
        return result
    except (OSError, subprocess.TimeoutExpired, ValueError, TypeError, KeyError,
            OverflowError, RecursionError):
        return result
