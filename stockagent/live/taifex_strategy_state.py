"""Small, read-only helpers for the persisted TAIFEX strategy state."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


def held_option_codes(state: Mapping[str, Any]) -> tuple[str, ...]:
    """Return every option code with a non-zero persisted strategy position."""

    strategies = state.get("strategies") or {}
    if not isinstance(strategies, Mapping):
        raise ValueError("TAIFEX strategy state strategies must be an object")
    codes: set[str] = set()
    for strategy_id, ledger in strategies.items():
        if not isinstance(ledger, Mapping):
            raise ValueError(
                f"TAIFEX strategy ledger must be an object: {strategy_id!r}"
            )
        positions = ledger.get("option_positions") or {}
        if not isinstance(positions, Mapping):
            raise ValueError(
                f"TAIFEX option_positions must be an object: {strategy_id!r}"
            )
        for raw_code, raw_quantity in positions.items():
            try:
                quantity = int(raw_quantity)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "TAIFEX option position quantity must be an integer: "
                    f"{strategy_id!r}/{raw_code!r}"
                ) from exc
            if quantity == 0:
                continue
            code = str(raw_code).strip()
            if not code:
                raise ValueError(
                    f"TAIFEX non-zero option position has no code: {strategy_id!r}"
                )
            codes.add(code)
    return tuple(sorted(codes))


def required_option_codes(state: Mapping[str, Any]) -> tuple[str, ...]:
    """Return held plus pending-roll option codes required after a restart."""

    codes = set(held_option_codes(state))
    strategies = state.get("strategies") or {}
    if not isinstance(strategies, Mapping):
        raise ValueError("TAIFEX strategy state strategies must be an object")
    for strategy_id, ledger in strategies.items():
        if not isinstance(ledger, Mapping):
            raise ValueError(
                f"TAIFEX strategy ledger must be an object: {strategy_id!r}"
            )
        pending = ledger.get("pending_option_roll")
        if pending is None:
            continue
        if not isinstance(pending, Mapping):
            raise ValueError(
                f"TAIFEX pending_option_roll must be an object: {strategy_id!r}"
            )
        legs = pending.get("legs") or ()
        if not isinstance(legs, (list, tuple)):
            raise ValueError(
                f"TAIFEX pending option roll legs must be an array: {strategy_id!r}"
            )
        for leg in legs:
            if not isinstance(leg, Mapping):
                raise ValueError(
                    f"TAIFEX pending option roll leg must be an object: {strategy_id!r}"
                )
            for key in ("old_code", "new_code"):
                code = str(leg.get(key) or "").strip()
                if not code:
                    raise ValueError(
                        f"TAIFEX pending option roll leg has no {key}: {strategy_id!r}"
                    )
                codes.add(code)
    return tuple(sorted(codes))


def load_held_option_codes(state_dir: Path) -> tuple[str, ...]:
    """Load held option codes from one atomically committed state snapshot."""

    state_path = Path(state_dir) / "state.json"
    if not state_path.is_file():
        return ()
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"TAIFEX strategy state root is not an object: {state_path}")
    return held_option_codes(payload)


def load_required_option_codes(state_dir: Path) -> tuple[str, ...]:
    """Load every option code required to resume a persisted strategy state."""

    state_path = Path(state_dir) / "state.json"
    if not state_path.is_file():
        return ()
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"TAIFEX strategy state root is not an object: {state_path}")
    return required_option_codes(payload)


__all__ = [
    "held_option_codes",
    "load_held_option_codes",
    "load_required_option_codes",
    "required_option_codes",
]
