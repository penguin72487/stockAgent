"""Pure, auditable repair scope planning for FinMind correction notices.

This module never fetches data, changes queues or claims a repaired partition.
Consumers must verify receipts and source hashes against each required watermark.
The baseline notice-date boundary is only a redownload filter, never PIT proof.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, date, datetime, time, timedelta
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from downloader.finmind_scheduling import PRODUCT_HISTORY_STARTS, SPECS


TAIPEI = ZoneInfo("Asia/Taipei")
DEFAULT_SCOPES = Path(__file__).resolve().parents[1] / "configs/finmind_correction_scopes.json"
LONG_INSTITUTIONAL = "TaiwanStockInstitutionalInvestorsBuySell"
WIDE_INSTITUTIONAL = "TaiwanStockInstitutionalInvestorsBuySellWide"
DATE_PATTERN = re.compile(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)")
INTERVAL_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})\s*(?:~|～|至|到|—|–)\s*(\d{4}-\d{2}-\d{2})")


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def _aware(value: datetime | str) -> datetime:
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if not isinstance(stamp, datetime) or stamp.tzinfo is None:
        raise ValueError("timezone-aware correction watermark required")
    return stamp.astimezone(UTC)


@lru_cache(maxsize=1)
def _contracts() -> dict[str, dict[str, Any]]:
    # Worker constants remain authoritative; imports are delayed so the worker
    # can import consumer/planner helpers without a module-initialization cycle.
    from downloader.download_finmind_complement import ALL_DATASETS, GLOBAL_START_YEAR, SNAPSHOTS
    from downloader.download_finmind_free import CALENDAR_DATASET, HISTORY_START, MASTER_DATASET, SESSION_DATASETS

    result = {dataset: {"owner": "complement", "first_date": (
        date(GLOBAL_START_YEAR[dataset], 1, 1) if dataset in GLOBAL_START_YEAR else None),
        "snapshot": dataset in SNAPSHOTS} for dataset in ALL_DATASETS}
    result.update({dataset: {"owner": "sponsor", "first_date": spec.first_date,
                            "snapshot": spec.grain == "snapshot"} for dataset, spec in SPECS.items()})
    result.update({dataset: {"owner": "complement", "first_date": first, "snapshot": False}
                   for dataset, first in PRODUCT_HISTORY_STARTS.items()})
    result.update({dataset: {"owner": "free", "first_date": HISTORY_START, "snapshot": False}
                   for dataset in SESSION_DATASETS})
    result.update({dataset: {"owner": "free", "first_date": None, "snapshot": True}
                   for dataset in (CALENDAR_DATASET, MASTER_DATASET)})
    return result


def resolve_owner(dataset: str) -> str | None:
    contract = _contracts().get(LONG_INSTITUTIONAL if dataset == WIDE_INSTITUTIONAL else dataset)
    return contract["owner"] if contract else None


def load_scope_registry(path: Path = DEFAULT_SCOPES) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != 1 or not isinstance(payload.get("scopes"), dict):
        raise ValueError("invalid FinMind correction scope registry")
    return payload


def _auto_scope(entry: Mapping[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    """Accept only a single unambiguous instruction, never nested examples."""
    text = str(entry.get("leading_text") or entry.get("text") or "")
    datasets = list(dict.fromkeys(entry.get("datasets") or []))
    if not datasets:
        return [], "no_explicit_dataset"
    if entry.get("nested_items"):
        return [], "nested_scope_requires_review"
    if entry.get("known_unavailable") or re.search(
        r"無法|未提供|不受影響|例如|範例|比方|原記|原為|以前|以後|之前|之後|截至|部分日期|以來|開始|起|except|unaffected|example",
        text, flags=re.IGNORECASE,
    ):
        return [], "mixed_or_exception_scope_requires_review"
    matches = DATE_PATTERN.findall(text)
    full_history = re.search(r"全歷史|全期間|全部歷史|entire history|full history", text, re.IGNORECASE)
    if matches and full_history:
        return [], "mixed_full_history_and_dated_scope_requires_review"
    try:
        dates = [date.fromisoformat(value) for value in matches]
    except ValueError:
        return [], "invalid_notice_scope_date"
    common: dict[str, Any]
    if len(dates) == 1 and not re.search(r"[~～至到—–]\s*\d|\d{4}年", text):
        common = {"start_date": dates[0].isoformat(), "end_date": dates[0].isoformat()}
    elif len(dates) == 2 and INTERVAL_PATTERN.search(text):
        matched = INTERVAL_PATTERN.search(text)
        assert matched is not None
        if list(matched.groups()) != matches or dates[0] > dates[1]:
            return [], "ambiguous_notice_scope_interval"
        common = {"start_date": dates[0].isoformat(), "end_date": dates[1].isoformat()}
    elif not dates and full_history:
        common = {"scope_kind": "full_history"}
    else:
        return [], "no_single_exact_scope"
    return [{"dataset": dataset, **common, "allow_empty": False,
             "reason": "unambiguous_notice_scope"} for dataset in datasets], None


def _request(entry: Mapping[str, Any], scope: Mapping[str, Any], state: Mapping[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    original_dataset = str(scope.get("dataset") or "")
    dataset = LONG_INSTITUTIONAL if original_dataset == WIDE_INSTITUTIONAL else original_dataset
    contract = _contracts().get(dataset)
    if not contract:
        return None, {"status": "not_scheduled", "dataset": original_dataset,
                      "reason": "dataset_has_no_enabled_acquisition_owner"}
    kind = str(scope.get("scope_kind") or ("snapshot" if contract["snapshot"] else "date_range"))
    if kind not in {"snapshot", "date_range", "full_history"}:
        return None, {"status": "needs_review", "dataset": dataset, "reason": "unknown_scope_kind"}
    try:
        if kind == "full_history":
            first = contract["first_date"]
            if first is None:
                return None, {"status": "needs_review", "dataset": dataset, "reason": "unverified_history_lower_bound"}
            start = first
            # A published correction covers history through its announcement,
            # not future dates added by each subsequent daily monitor run.
            end = date.fromisoformat(entry["notice_date"])
        elif kind == "snapshot":
            start = date.fromisoformat(str(scope.get("start_date") or entry["notice_date"]))
            end = date.fromisoformat(str(scope.get("end_date") or scope.get("start_date") or entry["notice_date"]))
        else:
            start = date.fromisoformat(str(scope["start_date"]))
            end = date.fromisoformat(str(scope.get("end_date") or scope["start_date"]))
        if start > end:
            raise ValueError("reversed scope")
    except (ValueError, KeyError, TypeError):
        return None, {"status": "needs_review", "dataset": dataset, "reason": "invalid_curated_scope_dates"}
    data_ids = scope.get("data_ids")
    if data_ids is not None and (not isinstance(data_ids, list) or not data_ids or
                                 any(not isinstance(value, str) or not value.strip() for value in data_ids)):
        return None, {"status": "needs_review", "dataset": dataset, "reason": "invalid_scope_data_ids"}
    derived = sorted(set(scope.get("derived_datasets") or []) | (
        {WIDE_INSTITUTIONAL} if dataset == LONG_INSTITUTIONAL else set()))
    request = {
        "entry_id": entry["entry_id"], "owner": contract["owner"], "dataset": dataset,
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "data_ids": sorted(set(data_ids)) if data_ids is not None else None,
        "scope_kind": kind, "required_after_utc": state["required_after_utc"],
        "allow_empty": scope.get("allow_empty") is True,
        "reason": str(scope.get("reason") or "provider_correction_notice")[:4096],
        "source_url": entry["source_url"], "derived_datasets": derived,
        "watermark_basis": state["watermark_basis"],
    }
    if scope.get("market_scope"):
        request["market_scope"] = scope["market_scope"]
    request["correction_id"] = _hash({key: request[key] for key in (
        "entry_id", "owner", "dataset", "start_date", "end_date", "data_ids", "scope_kind", "allow_empty",
    )})
    request["request_id"] = _hash(request)
    return request, None


def build_repair_plan(entries: Iterable[Mapping[str, Any]], previous_state: Mapping[str, Any] | None = None,
                      now: datetime | None = None, *, scopes: Mapping[str, Any] | None = None,
                      scopes_path: Path = DEFAULT_SCOPES) -> dict[str, Any]:
    """Create stable owner-specific intents and the next monitor state.

    Persist ``result['state']`` only after the monitor atomically stores its
    source snapshot/plan. Unchanged entries retain watermarks indefinitely;
    new or edited entries after baseline require a post-detection fetch.
    """
    now = _aware(now or datetime.now(UTC))
    registry = dict(scopes) if scopes is not None else load_scope_registry(scopes_path)
    if registry.get("schema_version") != 1 or not isinstance(registry.get("scopes"), dict):
        raise ValueError("invalid FinMind correction scope registry")
    previous = dict(previous_state or {})
    baseline = not previous.get("initialized_at_utc")
    prior_entries = previous.get("entries", {})
    if not isinstance(prior_entries, dict):
        raise ValueError("invalid prior notice state")
    next_entries = deepcopy(prior_entries)
    auto_since = date.fromisoformat(registry.get("baseline_auto_since", "2026-09-01"))
    pinned_families = {value.get("revision_family_id") for value in registry["scopes"].values()
                       if isinstance(value, dict) and value.get("revision_family_id")}
    pinned_date_datasets = {(value.get("notice_date"), tuple(sorted(value.get("datasets") or [])))
                            for value in registry["scopes"].values() if isinstance(value, dict)}
    notices: list[dict[str, Any]] = []
    requests: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    for raw in entries:
        entry = dict(raw)
        entry_id = entry.get("entry_id")
        if not isinstance(entry_id, str) or not re.fullmatch(r"[0-9a-f]{64}", entry_id):
            raise ValueError("full semantic entry SHA-256 required")
        if entry_id in seen:
            raise ValueError("duplicate entry identity")
        seen.add(entry_id)
        notice = {"correction_id": entry_id, "notice_date": entry.get("notice_date"),
                  "datasets": list(entry.get("datasets") or []), "source_url": entry.get("source_url"),
                  "known_unavailable": bool(entry.get("known_unavailable")),
                  "requests": 0, "issues": []}
        try:
            notice_day = date.fromisoformat(str(entry["notice_date"]))
            if notice_day > now.astimezone(TAIPEI).date():
                raise ValueError("future notice")
        except (ValueError, KeyError):
            notice.update(status="needs_review", reason="invalid_or_future_notice_date")
            notices.append(notice)
            continue
        old = prior_entries.get(entry_id)
        if old is not None:
            if not isinstance(old, dict):
                raise ValueError("invalid existing notice state")
            state = dict(old)
            _aware(state["required_after_utc"])
            _aware(state["first_detected_at_utc"])
        else:
            boundary = datetime.combine(notice_day + timedelta(days=1), time(), TAIPEI).astimezone(UTC)
            required = min(boundary, now) if baseline else now
            state = {"first_detected_at_utc": now.isoformat(), "required_after_utc": required.isoformat(),
                     "watermark_basis": ("baseline_next_local_midnight_not_pit" if boundary <= now else
                                         "baseline_same_day_detection_not_pit") if baseline else "first_detection_of_new_or_edited_notice",
                     "origin": "baseline" if baseline else "new_or_edited", "notice_date": str(notice_day),
                     "revision_family_id": entry.get("revision_family_id")}
        next_entries[entry_id] = state
        notice.update(required_after_utc=state["required_after_utc"], watermark_basis=state["watermark_basis"])
        if not entry.get("is_correction"):
            notice.update(status="not_correction", reason="informational_notice")
            notices.append(notice)
            continue
        curated = registry["scopes"].get(entry_id)
        if curated is not None:
            if curated.get("notice_date") != entry["notice_date"] or curated.get("evidence_text") != entry.get("text"):
                notice.update(status="needs_review", reason="curated_identity_or_text_mismatch")
                notices.append(notice)
                continue
            todo = curated.get("requests", [])
            notice["issues"] = deepcopy(curated.get("issues", []))
            notice["scope_basis"] = "reviewed_full_entry_hash"
        elif (entry.get("revision_family_id") in pinned_families or
              (entry.get("notice_date"), tuple(sorted(entry.get("datasets") or []))) in pinned_date_datasets):
            notice.update(status="needs_review", reason="edited_curated_notice_requires_new_scope_review")
            notices.append(notice)
            continue
        elif state.get("origin") == "baseline" and notice_day < auto_since:
            notice.update(status="not_scheduled", reason="historical_notice_not_scope_reviewed")
            notices.append(notice)
            continue
        else:
            todo, reason = _auto_scope(entry)
            notice["scope_basis"] = "unambiguous_notice_text"
            if reason:
                status = "not_scheduled" if notice["datasets"] and all(resolve_owner(item) is None for item in notice["datasets"]) else "needs_review"
                notice.update(status=status, reason=reason)
                notices.append(notice)
                continue
        for scope in todo:
            request, issue = _request(entry, scope, state)
            if issue:
                notice["issues"].append(issue)
                continue
            assert request is not None
            # Long/wide declarations of the same scope cause one raw fetch.
            signature = _hash({key: value for key, value in request.items() if key not in {"request_id", "reason"}})
            if signature not in requests:
                requests[signature] = request
                notice["requests"] += 1
        notice["status"] = ("planned_with_limitations" if notice["requests"] and notice["issues"] else
                            "planned" if notice["requests"] else "needs_review" if any(
                                issue.get("status") == "needs_review" for issue in notice["issues"]) else "not_scheduled")
        notices.append(notice)
    ordered = sorted(requests.values(), key=lambda item: (item["owner"], item["dataset"], item["start_date"], item["request_id"]))
    plan_id = _hash({"schema_version": 1, "requests": ordered,
                     "notices": sorted(notices, key=lambda item: item["correction_id"])})
    return {"schema_version": 1, "plan_id": plan_id, "generated_at_utc": now.isoformat(),
            "requests": ordered, "notices": notices,
            "state": {"schema_version": 1, "initialized_at_utc": previous.get("initialized_at_utc") or now.isoformat(),
                      "entries": next_entries},
            "completion_claim": "repair_intents_only_receipts_must_be_verified",
            "baseline_boundary_is_point_in_time_proof": False}
