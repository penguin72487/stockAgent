"""Live signal helpers for stockAgent."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stockagent.live.signal_engine import LiveSignalResult, generate_live_signal

__all__ = ["LiveSignalResult", "generate_live_signal"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from stockagent.live.signal_engine import LiveSignalResult, generate_live_signal

        return {
            "LiveSignalResult": LiveSignalResult,
            "generate_live_signal": generate_live_signal,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted((*globals(), *__all__))
