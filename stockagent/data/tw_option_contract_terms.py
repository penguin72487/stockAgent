"""Source-bound candidate terms for dated TAIFEX option contract changes.

This module extracts a *change event*, not a complete specification history or
an account tape. Historical source availability and product admission remain
separate obligations for any eventual training consumer.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
import re
import unicodedata


def _compact(value: str) -> str:
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", value))


def _one(pattern: str, text: str, what: str) -> re.Match[str]:
    matches = list(re.finditer(pattern, text))
    if len(matches) != 1:
        raise ValueError(f"expected one unambiguous {what}, found {len(matches)}")
    return matches[0]


def gold_option_2026_resize(spec_pages: list[str], notice_pages: list[str]) -> dict:
    """Extract the TGO old/new sizes and open-position split from two notices.

    Both old and new values are explicitly identified by the source's column
    order. The result is retrospective and must not be used as a PIT rule,
    inferred historical interval, quote, or trading permission by itself.
    """
    spec = [_compact(p) for p in spec_pages]
    notice = _compact("".join(notice_pages))
    size_pages = [p for p in spec if "「黃金選擇權契約」規格修正對照表" in p]
    rule_pages = [p for p in spec if "「黃金選擇權契約」交易規則修正條文對照表" in p]
    if len(size_pages) != 1 or len(rule_pages) != 1:
        raise ValueError("one gold-option specification and rule page required")
    size, rules = size_pages[0], rule_pages[0]
    if "修正條文現行條文說明" not in size or "修正條文現行條文說明" not in rules:
        raise ValueError("cannot determine old and new source column order")
    amounts = _one(
        r"契約規模●(\d+)台兩\((\d+)台錢、([\d.]+)公克\)"
        r"契約規模●(\d+)台兩\((\d+)台錢、([\d.]+)公克\)",
        size, "gold-option contract sizes")
    quote = _one(
        r"權利金報價單位●([\d.]+)點\(新臺幣([\d.]+)元\)"
        r"權利金報價單位●([\d.]+)點\(新臺幣([\d.]+)元\)",
        size, "gold-option premium increments")
    new_size, new_unit, new_grams, old_size, old_unit, old_grams = amounts.groups()
    new_step, new_tick, old_step, old_tick = map(Decimal, quote.groups())
    if new_step <= 0 or old_step <= 0:
        raise ValueError("premium quote increment must be positive")
    new_multiplier, old_multiplier = new_tick / new_step, old_tick / old_step
    # Article texts corroborate the premium and exercise multipliers; never
    # borrow a futures contract's size or premium rule by product name alone.
    exercise_new = _one(r"前項履約價格乘數為十。", rules, "new exercise multiplier")
    exercise_old = _one(r"前項履約價格乘數為五十。", rules, "old exercise multiplier")
    premium_new = _one(r"權利金乘數為新臺幣十元", rules, "new premium multiplier")
    premium_old = _one(r"權利金乘數為新臺幣五十元", rules, "old premium multiplier")
    if (exercise_new.start() >= exercise_old.start() or premium_new.start() >= premium_old.start()):
        raise ValueError("option premium or exercise source column order differs")
    issue = _one(r"發文字號:台期交字第(1150201214)號", notice, "effective notice identity")
    if "台期交字第1150200958號函相關規章修正條文" not in notice:
        raise ValueError("effective notice does not bind the specification notice")
    effective = _one(r"台期交字第1150200958號函相關規章修正條文自(\d+)年(\d+)月(\d+)日起實施", notice,
                     "effective date binding")
    year, month, day = map(int, effective.groups())
    effective_day = date(year + 1911, month, day)
    split = _one(r"(\d+)月(\d+)日開盤前之各契約未沖銷部位.*?"
                 r"黃金選擇權之未沖銷部位,(\d+)口調整為(\d+)口",
                 notice, "pre-open option position split")
    split_month, split_day, old_lots, new_lots = map(int, split.groups())
    if (split_month, split_day) != (month, day):
        raise ValueError("position split date differs from the effective date")
    if (old_lots <= 0 or new_lots <= 0
            or int(new_unit) * new_lots != int(old_unit) * old_lots
            or new_multiplier * new_lots != old_multiplier * old_lots
            or (new_multiplier, old_multiplier) != (Decimal(10), Decimal(50))
            or (new_size, old_size, new_grams, old_grams) != ('1', '5', '37.5', '187.5')):
        raise ValueError("source sizes, multipliers and position split disagree")
    return {
        "product": "TGO", "product_name": "黃金選擇權", "currency": "TWD",
        "effective_date": effective_day.isoformat(), "effective_phase": "before_open",
        "notice_number": issue.group(1),
        "old": {"contract_tael": int(old_size), "contract_taicandareen": int(old_unit),
                "contract_grams": old_grams, "premium_tick_points": str(old_step),
                "premium_multiplier_twd_per_point": format(old_multiplier, 'f'),
                "exercise_multiplier_twd_per_point": format(old_multiplier, 'f')},
        "new": {"contract_tael": int(new_size), "contract_taicandareen": int(new_unit),
                "contract_grams": new_grams, "premium_tick_points": str(new_step),
                "premium_multiplier_twd_per_point": format(new_multiplier, 'f'),
                "exercise_multiplier_twd_per_point": format(new_multiplier, 'f')},
        "open_position_split": {"old_contracts": old_lots, "new_contracts": new_lots},
        "point_in_time_verified": False, "training_ready": False,
    }
