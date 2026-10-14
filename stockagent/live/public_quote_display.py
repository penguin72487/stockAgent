"""Public quote disclosure, independent of private inference/accounting.

An API entitlement is not a redistribution grant. Unknown quotes are withheld.
Raw quotations need a server-owned grant and a role-specific observation.
Separately, the owner may publish recorded paper transactions as a transaction
report, not as a licensed feed or a broker fill. Payload flags cannot authorize
either publication path.
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
PAPER_REPORT_CONTRACT = "owner_paper_transaction_report_v1"
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


def _record_clock(value: Any, current_clock: datetime) -> datetime | None:
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        clock = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return clock if clock.tzinfo and clock <= current_clock else None


def _positive_quantity(value: Any) -> bool:
    return (type(value) in (int, float) and math.isfinite(value)
            and value > 0 and int(value) == value)


def _expected_session(record: Mapping[str, Any], field: str) -> str | None:
    if field in {"daily_return_previous_close", "daily_return_reference_price"}:
        value = record.get("daily_return_previous_close_date")
        try:
            return datetime.strptime(str(value), "%Y-%m-%d").date().isoformat()
        except ValueError:
            return None
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
    paper_transaction_reports: bool = False

    def transaction_report_price(
        self, record: Mapping[str, Any], field: str, private_value: Any,
        *, record_kind: str | None, now: datetime | None = None,
    ) -> tuple[float, dict[str, Any]] | None:
        """Publish only a recorded paper fill in a trusted endpoint context.

        The operator's choice to disclose a transaction report is NOT an
        assertion of market-feed redistribution rights. Never inherit this
        context into nested quotes, accept payload permission flags, replace
        accounting values, or call paper results broker/deal evidence.
        """
        if (
            not self.paper_transaction_reports
            or type(private_value) not in (int, float)
            or not math.isfinite(private_value) or private_value <= 0
            or not isinstance(record.get("symbol"), str) or not record["symbol"]
        ):
            return None
        current_clock = now or datetime.now(TAIPEI)
        if current_clock.tzinfo is None:
            return None
        clock_value = None
        if record_kind == "paper_position":
            if field not in {"entry_price", "inventory_basis_price", "exit_price", "last_exit_price"}:
                return None
            if not _positive_quantity(record.get("filled_shares")):
                return None
            entry_clock = _record_clock(record.get("entry_at"), current_clock)
            if entry_clock is None or entry_clock.astimezone(TAIPEI).date().isoformat() != record.get("session_date"):
                return None
            clock_value = record.get("entry_at")
            if field in {"exit_price", "last_exit_price"}:
                clock_value = record.get("exit_at" if field == "exit_price" else "last_exit_at")
                exit_clock = _record_clock(clock_value, current_clock)
                if exit_clock is None or exit_clock < entry_clock:
                    return None
                signed = record.get("signed_shares")
                closed = record.get("status") == "closed" or (
                    type(signed) in (int, float) and math.isfinite(signed) and signed == 0
                )
                partial = (type(signed) in (int, float) and math.isfinite(signed)
                           and abs(signed) < record["filled_shares"])
                if not closed and (field == "exit_price" or not partial):
                    return None
        elif record_kind == "paper_signal":
            if field != "execution_price" or not _positive_quantity(record.get("filled_shares")):
                return None
            position = record.get("position")
            if not isinstance(position, Mapping) or any(
                position.get(key) != record.get(key) for key in ("symbol", "market", "session_date")
            ) or position.get("entry_price") != private_value:
                return None
            verified = self.transaction_report_price(
                position, "entry_price", private_value, record_kind="paper_position", now=current_clock,
            )
            if verified is None or record["filled_shares"] > position["filled_shares"]:
                return None
            clock_value = verified[1]["recorded_at"]
        elif record_kind == "paper_event":
            if (field != "price" or record.get("event_kind") != "fill"
                or record.get("status") != "filled" or record.get("simulation_only") is not True
                or not _positive_quantity(record.get("quantity"))):
                return None
            clock_value = record.get("fill_at") or record.get("exchange_match_at") or record.get("recorded_at")
        else:
            return None
        if _record_clock(clock_value, current_clock) is None:
            return None
        return float(private_value), {
            "source": "使用者公開的模擬帳本", "price_role": field,
            "publication_basis": PAPER_REPORT_CONTRACT, "evidence_kind": "paper_fill",
            "recorded_at": str(clock_value), "broker_execution_verified": False,
        }

    @staticmethod
    def withheld_reason(record: Mapping[str, Any], field: str) -> str:
        """Describe the disclosure gap, not a missing-price/provider failure.

        These labels never authorize publication or turn unknown contractual
        permission into a proven prohibition.
        """
        observations = record.get(PROTECTED_OBSERVATIONS_KEY)
        if isinstance(observations, Mapping) and isinstance(observations.get(field), Mapping):
            return "public_observation_not_verified"
        for key in ("entry_price_source", "last_mark_quote_source", "source", "quote_source"):
            source = record.get(key)
            if isinstance(source, str) and "shioaji" in source.casefold():
                return "public_display_permission_unverified"
        return "public_source_not_connected"

    def observation(
        self, record: Mapping[str, Any], field: str, private_value: Any,
        *, now: datetime | None = None,
    ) -> tuple[float, dict[str, str]] | None:
        observations = record.get(PROTECTED_OBSERVATIONS_KEY)
        candidate = observations.get(field) if isinstance(observations, Mapping) else None
        if not isinstance(candidate, Mapping):
            return None
        observed_at = candidate.get("observed_at")
        daily = candidate.get("grain") == "daily"
        session = candidate.get("session_date") if daily else _session(observed_at)
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
        if _session(observed_at) is None:
            return None
        observed_clock = datetime.fromisoformat(str(observed_at).replace("Z", "+00:00"))
        if observed_clock > current_clock:
            return None
        for grant in self.grants:
            try:
                valid_from = datetime.strptime(str(grant.get("valid_from")), "%Y-%m-%d").date()
                valid_until = (None if grant.get("perpetual") is True and grant.get("valid_until") is None
                               else datetime.strptime(str(grant.get("valid_until")), "%Y-%m-%d").date())
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
                or not (valid_from <= today and (valid_until is None or today <= valid_until))
                or (grant.get("grain") is not None and grant.get("grain") != candidate.get("grain"))
                or (grant.get("dataset_id") is not None and grant.get("dataset_id") != candidate.get("dataset_id"))
            ):
                continue
            if daily:
                roles = grant.get("daily_price_roles")
                if not isinstance(roles, Mapping) or candidate.get("source_field") not in roles.get(field, []):
                    continue
                # This adapter is the post-close daily report, not an opening
                # tick. Its acquisition receipt cannot precede that close.
                closed_at = datetime.fromisoformat(f"{session}T13:30:00+08:00")
                if session > today.isoformat() or observed_clock < closed_at:
                    continue
                return float(value), {
                    "source": str(grant["public_label"]), "observed_at": str(observed_at),
                    "price_role": field, "grain": "daily", "session_date": str(session),
                    "source_field": str(candidate["source_field"]),
                    "license_reference": str(grant["license_reference"]),
                }
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
        report = payload.get("owner_transaction_reports")
        enabled = (isinstance(report, Mapping) and report.get("enabled") is True
                   and report.get("contract") == PAPER_REPORT_CONTRACT
                   and report.get("record_scope") == "tw_day_trade_and_overnight_paper_ledger")
        return PublicQuoteDisplayPolicy(tuple(
            dict(grant) for grant in payload["authorized_sources"]
            if isinstance(grant, Mapping)
        ), paper_transaction_reports=enabled)
    except (OSError, ValueError, TypeError):
        return PublicQuoteDisplayPolicy()


def public_quote_display_notice(*, include_paper_reports: bool = False) -> dict[str, Any]:
    notice = {
        "contract": CONTRACT,
        "policy": "verified_public_source_only",
        "pending_label": "等待可公開來源",
        "message": "官方日行情已接通；行情日期與取得時間分開標示。永豐分鐘／即時價格的公開展示授權尚未確認，不代表沒有取得價格；不以日價替代分鐘成交。模型、帳本與損益原值不變。",
        "broker_public_display_permission": "unverified",
        "usage_rules_reviewed_on": "2026-10-13",
        "usage_rules_references": [
            "https://www.sinotrade.com.tw/newweb/signCenter/S_openAPI/",
            "https://www.twse.com.tw/zh/products/information/qa.html",
            "https://www.sinotrade.com.tw/newweb/signCenter/F_openApi/",
            "https://data.gov.tw/dataset/11549",
            "https://data.gov.tw/dataset/11370",
        ],
    }
    if include_paper_reports and public_quote_display_policy().paper_transaction_reports:
        notice["policy"] = "verified_public_quotes_and_owner_paper_transaction_reports"
        notice["paper_transaction_reports"] = True
        notice["message"] = (
            "已記錄的進出場價按使用者意願公開，標示為模擬帳本成交，不是券商成交。"
            "官方日行情另列日期與來源；永豐原始分鐘／即時行情公開展示授權尚未確認。"
            "交易紀錄公開不等於行情轉載授權；模型、帳本與損益原值不變。"
        )
    return notice
