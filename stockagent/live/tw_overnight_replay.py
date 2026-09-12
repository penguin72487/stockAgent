"""Historical price authority around the existing overnight paper account.

The live executor still requires exchange-timestamped prints. This offline
subclass consumes explicitly selected official daily prices, leaves exchange
timestamps absent, and reuses the same orders, lot sizing, costs and PnL ledger.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from stockagent.live.tw_day_trade_simulation import TAIPEI, _atomic_json, _parse_timestamp
from stockagent.live.tw_overnight_simulation import TwOvernightSimulationEngine, _finite


class TwOvernightHistoricalReplayEngine(TwOvernightSimulationEngine):
    def __init__(self, state_dir: Path) -> None:
        if state_dir.exists() or "live" in state_dir.resolve().parts:
            raise ValueError("Historical replay requires a new isolated state directory")
        self.replay_generated_at = datetime.now(TAIPEI).isoformat(timespec="seconds")
        self.replay_session_date = ""
        self.replay_rows: dict[str, dict[str, Any]] = {}
        self.replay_source: dict[str, Any] = {}
        super().__init__(state_dir)
        self.state["historical_counterfactual_replay"] = True

    def select_session(
        self, day: str, rows: Mapping[str, Mapping[str, Any]],
        source: Mapping[str, Any],
    ) -> None:
        if self.replay_session_date and day <= self.replay_session_date:
            raise ValueError("Replay sessions must advance strictly chronologically")
        if source.get("session_date") != day or not source.get("source_hashes"):
            raise ValueError("Replay prices require a dated source receipt")
        for digest in source["source_hashes"].values():
            if len(str(digest)) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise ValueError("Invalid historical source hash")
        if any(str(row.get("date")) != day for row in rows.values()):
            raise ValueError("Official price rows disagree with replay session")
        self.replay_session_date = day
        self.replay_rows = {str(symbol): dict(row) for symbol, row in rows.items()}
        self.replay_source = dict(source)
        self.replay_source_sha256 = hashlib.sha256(
            json.dumps(
                self.replay_source,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    def _signal_timestamp(self, summary: Mapping[str, Any]) -> datetime | None:
        if summary.get("counterfactual_signal_regeneration") is not True:
            raise ValueError("Replay requires an explicitly counterfactual signal")
        effective = _parse_timestamp(summary.get("replay_effective_signal_at"))
        if effective is None or effective.date().isoformat() != self.replay_session_date:
            raise ValueError("Replay signal date differs from selected official source")
        return effective

    def _match_auction_print(
        self, quote: Mapping[str, Any], *, symbol: str, **kwargs: Any,
    ) -> tuple[float, None] | None:
        if kwargs["session_date"] != self.replay_session_date:
            raise ValueError("Replay auction date differs from selected source")
        # Read the immutable selected source, never a caller-supplied quote or
        # an invented exchange timestamp. Daily OPEN/CLOSE are price proxies.
        column = "open" if kwargs["price_field"] == "open" else "close"
        price = _finite(self.replay_rows.get(symbol, {}).get(column))
        return (price, None) if price is not None else None

    def _auction_fill_contract(self, phase: str) -> str:
        return f"historical_official_{phase}_counterfactual_no_auction_timestamp_or_queue_proof"

    def _auction_price_within_limits(
        self,
        price: float,
        lower: float | None,
        upper: float | None,
        *,
        phase: str,
    ) -> bool:
        """Accept an official daily auction proxy independently of bad bands.

        The selected TWSE/TPEx OPEN/CLOSE is the price authority for this
        counterfactual replay.  Reconstructed price bands are retained and
        audited separately; they cannot turn an observed official price into a
        delayed fill merely because an ETF was classified with stock rules.
        """

        _ = lower, upper, phase
        return _finite(price) is not None

    def _opening_limits_current(
        self, quote: Mapping[str, Any], *, session_date: str,
    ) -> bool:
        return bool(
            session_date == self.replay_session_date
            and quote.get("historical_limits_session_date") == session_date
            and quote.get("historical_limits_sha256")
            == self.replay_source["source_hashes"].get("price_limits")
        )

    def _append_ledger(self, path: Path, payload: Mapping[str, Any]) -> None:
        source = self.replay_source
        if path == self.signals_path:
            # A complete receipt is stored once per session under inputs/.  A
            # 2,700-symbol signal ledger must not repeat that object on every
            # line; keep a hash-pinned reference in the large row stream.
            source = {
                "session_date": self.replay_source.get("session_date"),
                "receipt_sha256": self.replay_source_sha256,
                "prices_sha256": self.replay_source.get("prices_sha256"),
            }
        super()._append_ledger(path, {
            **payload,
            "counterfactual": True,
            "counterfactual_generated_at": self.replay_generated_at,
            "recorded_at_basis": "historical_simulation_effective_time",
            "production_order_possible": False,
            "historical_source": source,
        })

    def _mark_mode(
        self, market: str, now: datetime, quotes: Mapping[str, Mapping[str, Any]],
        *, append_history: bool = True,
    ) -> None:
        super()._mark_mode(market, now, quotes, append_history=False)
        mode = self.state["modes"][market]
        if not append_history:
            return
        self._append_ledger(self.marks_path, {
            "market": market, "session_date": now.date().isoformat(),
            "minute": now.isoformat(timespec="minutes"),
            "recorded_at": now.isoformat(timespec="seconds"),
            **{key: mode.get(key) for key in (
                "initial_capital_twd", "cumulative_realized_net_pnl_twd",
                "open_net_liquidation_pnl_twd", "total_equity_twd",
                "open_position_count", "stale_position_count", "valuation_stale",
            )},
            "valuation_basis": "historical_official_price" if mode.get("open_position_count") else "flat_cash_balance",
        })

    def _persist(self, now: datetime | None = None) -> None:
        for mode in self.state.get('modes', {}).values():
            if mode.get('entry_fill_contract'):
                mode['entry_fill_contract'] = self._auction_fill_contract('close')
            if mode.get('entry_fill_outcome') == 'filled_at_actual_close':
                mode['entry_fill_outcome'] = 'filled_at_historical_official_close'
        super()._persist(now)
        import json
        status = json.loads(self.status_path.read_text())
        status["historical_counterfactual_replay"] = True
        status["historical_source"] = self.replay_source
        status["schedule"]["close_fill"] = self._auction_fill_contract("close")
        status["schedule"]["opening_fill"] = self._auction_fill_contract("open")
        _atomic_json(self.status_path, status)
