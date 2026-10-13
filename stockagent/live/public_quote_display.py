"""Public quote disclosure, independent of private inference/accounting.

An API entitlement is not a redistribution grant. Unknown quotes are withheld.
Only a server-owned grant and a separately sourced, role-specific observation
can supply a public value. Payload flags cannot authorize their own publication.
Policy changes require a public-reader restart, which also discards old encoded
responses; they never require a broker login or a trading-engine restart.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
import json
import math
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


CONTRACT = "public_quote_display_v1"
PROTECTED_OBSERVATIONS_KEY = "public_price_observations"
TAIPEI = ZoneInfo("Asia/Taipei")
_POLICY_PATH = Path(__file__).resolve().parents[2] / "configs/public_quote_display.json"
_QUOTE_FIELDS = frozenset({
    "price", "prices", "open", "high", "low", "close", "bid", "ask", "mid",
    "last", "upper_limit", "lower_limit", "daily_return_previous_close",
})
# These are immutable paper/accounting prices, not replaceable live quotations.
_ACCOUNTING_FIELDS = frozenset({
    "price", "entry_price", "exit_price", "execution_price", "last_exit_price",
    "inventory_basis_price", "sizing_open_price", "sizing_price_at_decision",
    "sizing_price_at_13_25", "order_limit_price", "eod_limit_price",
    "closing_auction_limit_price", "opening_exit_limit_price", "take_profit_price",
    "stop_trigger_price",
})


def _is_quote_price_field(key: str) -> bool:
    normalized = key.casefold()
    # Strike is contract metadata, not a current quote. Counts, source names,
    # timing, offsets, method names, P&L and return ratios are not price fields.
    if normalized in {"strike_price", "exercise_price"}:
        return False
    return normalized in _QUOTE_FIELDS or normalized.endswith(
        ("_price", "_prices", "_bid", "_ask", "_mid")
    )


@lru_cache(maxsize=512)
def _cached_quote_price_field(key: str) -> bool:
    return _is_quote_price_field(key)


def is_quote_price_field(key: str) -> bool:
    return (_cached_quote_price_field(key) if len(key) <= 128
            else _is_quote_price_field(key))


def _session(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(TAIPEI).date().isoformat() if parsed.tzinfo else None


def _expected_session(record: Mapping[str, Any], field: str) -> str | None:
    keys = ("last_quote_at", "last_mark_at")
    if field in {"entry_price", "execution_price", "inventory_basis_price"}:
        keys = ("entry_quote_at", "entry_exchange_at", "entry_at", "signal_at")
    elif field in {"exit_price", "last_exit_price"}:
        keys = ("exit_quote_at", "exit_exchange_at", "exit_at", "last_exit_at")
    elif field.startswith("sizing_") or field in {"bid", "ask", "upper_limit", "lower_limit"}:
        keys = ("exchange_quote_at", "quote_at", "signal_at")
    for key in keys:
        session = _session(record.get(key))
        if session:
            return session
    session = record.get("session_date")
    try:
        return datetime.strptime(str(session), "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


@dataclass(frozen=True)
class PublicQuoteDisplayPolicy:
    grants: tuple[Mapping[str, Any], ...] = ()

    def observation(
        self, record: Mapping[str, Any], field: str, private_value: Any,
        *, now: datetime | None = None,
    ) -> tuple[float, dict[str, str]] | None:
        observations = record.get(PROTECTED_OBSERVATIONS_KEY)
        candidate = observations.get(field) if isinstance(observations, Mapping) else None
        if not isinstance(candidate, Mapping):
            return None
        observed_at = candidate.get("observed_at")
        session = _session(observed_at)
        expected = _expected_session(record, field)
        symbol = str(record.get("symbol") or "")
        if (
            not expected or session != expected or not symbol
            or candidate.get("symbol") != symbol
            or candidate.get("price_role") != field
        ):
            return None
        value = candidate.get("value")
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            return None
        # A public last quote may be sourced separately; a recorded fill may
        # only be disclosed when the permitted observation verifies its value.
        if field in _ACCOUNTING_FIELDS and (
            type(private_value) not in (int, float) or value != private_value
        ):
            return None
        source_id = candidate.get("source_id")
        grant_id = candidate.get("grant_id")
        venue = candidate.get("venue")
        if not isinstance(source_id, str) or not source_id.strip() or not isinstance(grant_id, str) or not grant_id.strip():
            return None
        if venue not in {"TWSE", "TPEX", "TAIFEX"}:
            return None
        record_venue = record.get("exchange") or record.get("venue")
        if record_venue and record_venue != venue:
            return None
        current_clock = now or datetime.now(TAIPEI)
        if current_clock.tzinfo is None:
            return None
        today = current_clock.astimezone(TAIPEI).date()
        observed_clock = datetime.fromisoformat(str(observed_at).replace("Z", "+00:00"))
        if observed_clock > current_clock:
            return None
        for grant in self.grants:
            try:
                valid_from = datetime.strptime(str(grant.get("valid_from")), "%Y-%m-%d").date()
                valid_until = datetime.strptime(str(grant.get("valid_until")), "%Y-%m-%d").date()
            except ValueError:
                continue
            if (
                grant.get("source_id") != source_id
                or grant.get("grant_id") != grant_id
                or grant.get("public_display") is not True
                or not str(grant.get("license_reference") or "").strip()
                or not str(grant.get("public_label") or "").strip()
                or not isinstance(grant.get("venues"), list)
                or venue not in grant["venues"]
                or not (valid_from <= today <= valid_until)
            ):
                continue
            return float(value), {
                "source": str(grant["public_label"]),
                "observed_at": str(observed_at),
                "price_role": field,
            }
        return None


@lru_cache(maxsize=1)
def public_quote_display_policy() -> PublicQuoteDisplayPolicy:
    """Read trusted local configuration once; malformed policies fail closed."""
    try:
        payload = json.loads(_POLICY_PATH.read_text(encoding="utf-8"))
        if (
            not isinstance(payload, dict) or payload.get("schema_version") != 1
            or payload.get("contract") != CONTRACT
            or payload.get("default_policy") != "withhold_without_verified_public_display_grant"
            or not isinstance(payload.get("authorized_sources"), list)
        ):
            return PublicQuoteDisplayPolicy()
        return PublicQuoteDisplayPolicy(tuple(
            dict(grant) for grant in payload["authorized_sources"]
            if isinstance(grant, Mapping)
        ))
    except (OSError, ValueError, TypeError):
        return PublicQuoteDisplayPolicy()


def public_quote_display_notice() -> dict[str, str]:
    return {
        "contract": CONTRACT,
        "policy": "verified_public_source_only",
        "pending_label": "等待可公開來源",
        "message": "行情價格僅由已確認可公開展示的來源提供；受限或未確認授權的價格不對外傳送。模型、模擬成交與損益仍使用內部原始資料。",
    }
