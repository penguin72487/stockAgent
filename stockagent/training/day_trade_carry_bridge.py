"""Physical-account adapter for the existing chronological windowed trainer.

No model, optimizer, loss formula or source downloader lives here. A prepared
session set is NOT a claim that public source receipts were accepted. The
production config gate stays closed until its source builder and fold/report
callers bind this same contract.
"""
from __future__ import annotations

from dataclasses import dataclass, fields, replace
from typing import Any, Callable

import torch

from stockagent.backtest.tw_day_trade_carry import DayTradeCarrySession, DayTradeCarryState
from stockagent.backtest.tw_day_trade_inventory import _validate_checkpoint_identity


@dataclass(frozen=True)
class PreparedDayTradeCarrySource:
    sessions: tuple[DayTradeCarrySession, ...]
    universe: tuple[str, ...]
    release_id: str
    session_days: tuple[int, ...] = ()
    session_loader: Callable[[int], DayTradeCarrySession] | None = None
    audit_receipt: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        _validate_checkpoint_identity(self.universe, self.release_id, len(self.universe))
        if self.audit_receipt is not None and not isinstance(self.audit_receipt, dict):
            raise ValueError("physical source audit receipt must be structured")
        if self.sessions and self.session_loader is not None:
            raise ValueError("physical source cannot mix eager sessions and a lazy loader")
        days = (
            tuple(session.day for session in self.sessions)
            if self.sessions
            else tuple(self.session_days)
        )
        if not days or any(a >= b for a, b in zip(days, days[1:])):
            raise ValueError("physical source requires ordered unique sessions")
        if self.sessions:
            if self.session_days and tuple(self.session_days) != days:
                raise ValueError("physical source eager dates differ from declared dates")
            for session in self.sessions:
                session.validate_shape(len(self.universe), session.official_open.device)
        elif self.session_loader is None:
            raise ValueError("lazy physical source requires a session loader")
        object.__setattr__(self, "session_days", days)

    def __len__(self) -> int:
        return len(self.session_days)

    def day_at(self, row: int) -> int:
        return self.session_days[int(row)]

    def session_at(self, row: int) -> DayTradeCarrySession:
        index = int(row)
        if index < 0 or index >= len(self):
            raise IndexError("physical source row is outside its pinned calendar")
        session = (
            self.sessions[index]
            if self.sessions
            else self.session_loader(index)  # type: ignore[misc]
        )
        if session.day != self.session_days[index]:
            raise ValueError("lazy physical session differs from its pinned calendar")
        session.validate_shape(len(self.universe), session.official_open.device)
        return session

    def rows(
        self,
        row_indices: list[int],
        symbol_indices: list[int],
        device: torch.device,
    ) -> tuple[DayTradeCarrySession, ...]:
        if (
            not row_indices
            or any(a + 1 != b for a, b in zip(row_indices, row_indices[1:]))
            or min(row_indices) < 0
            or max(row_indices) >= len(self)
        ):
            raise ValueError("physical source rows must be a nonempty contiguous interval")
        if (
            not symbol_indices
            or len(set(symbol_indices)) != len(symbol_indices)
            or min(symbol_indices) < 0
            or max(symbol_indices) >= len(self.universe)
        ):
            raise ValueError("physical source requires a unique in-range symbol mapping")
        identity = symbol_indices == list(range(len(self.universe)))
        result = []
        for row in row_indices:
            session = self.session_at(row)
            values = {}
            index_by_device = {}
            for field in fields(session):
                value = getattr(session, field.name)
                if isinstance(value, torch.Tensor):
                    if identity:
                        selected = value
                    else:
                        index = index_by_device.get(value.device)
                        if index is None:
                            index = index_by_device[value.device] = torch.tensor(
                                symbol_indices, device=value.device
                            )
                        selected = value.index_select(0, index)
                    values[field.name] = selected.to(device=device, dtype=torch.float64)
            result.append(replace(session, **values))
        return tuple(result)

    def batch(self, split, start: int, end: int, device: torch.device):
        if split.execution_mode != "tw_day_trade":
            raise ValueError("physical source attached to a different execution mode")
        if start < 0 or end <= start or start >= len(split):
            raise ValueError("physical batch requires a nonempty in-range row interval")
        if len(self) != int(split.features.shape[0]):
            raise ValueError("physical source and base feature calendars differ")
        rows = split._valid_indices_cpu[start:end]
        # Padding is a trainer representation, never a second exchange session.
        sample = (torch.ones(len(rows), dtype=torch.bool) if split.sample_mask is None
                  else split.sample_mask[start:end].detach().cpu().bool())
        count = int(sample.sum())
        if count == 0 or not torch.equal(sample, torch.arange(len(rows)) < count):
            raise ValueError("physical batch allows only a nonempty valid prefix and trailing padding")
        rows = rows[:count].tolist()
        if min(rows) < 0 or max(rows) >= len(self):
            raise ValueError("physical batch indexes outside its pinned source calendar")
        if any(a + 1 != b for a, b in zip(rows, rows[1:])):
            raise ValueError("physical batch skips, repeats or reorders an exchange session")
        indices = (list(range(len(self.universe))) if split.symbol_indices is None
                   else split.symbol_indices.detach().cpu().tolist())
        if (not indices or len(set(indices)) != len(indices)
                or min(indices) < 0 or max(indices) >= len(self.universe)
                or len(indices) != int(split.features.shape[1])):
            raise ValueError("physical batch differs from the pinned symbol universe")
        return self.rows(rows, indices, device), count


def bind_physical_carry_loss(
    loss_fn: Callable, *, source: PreparedDayTradeCarrySource, split,
    start: int, end: int, device: torch.device, previous: DayTradeCarryState | None,
) -> Callable:
    """Bind an exact batch; model padding never advances inventory or interest."""
    sessions, count = source.batch(split, start, end, device)
    if previous is not None:
        first_row = int(split._valid_indices_cpu[start])
        if first_row <= 0 or previous.last_session_day != source.day_at(first_row - 1):
            raise ValueError("physical batch skipped or repeated a source session")

    def call(weights, returns, mask, **kwargs):
        if not bool(torch.isfinite(weights[:count]).all()):
            raise FloatingPointError("physical training model actions are nonfinite")
        supplied = dict(kwargs)
        aux = supplied.get("aux_outputs")
        if not isinstance(aux, dict):
            raise ValueError("physical trainer requires its recurrent aux channel")
        # Translate causal panel obligations into policy constraints before
        # discarding the legacy daily ledger fields.  Avoidance/terminal rows
        # target zero; a statutory short-cover row targets zero only for an
        # existing/requested short.  The physical 09:01 reduction still needs
        # an observed price and capacity, so this never guarantees a fill.
        force_exit = supplied.pop("force_exit_mask", None)
        unresolved = supplied.pop("unresolved_corporate_action_mask", None)
        force_cover = supplied.pop("force_short_cover_mask", None)
        must_flat = torch.zeros_like(weights[:count], dtype=torch.bool)
        for name, value in (
            ("force_exit_mask", force_exit),
            ("unresolved_corporate_action_mask", unresolved),
        ):
            if value is not None:
                current = value[:count]
                if current.shape != must_flat.shape:
                    raise ValueError(f"physical {name} differs from model actions")
                must_flat |= current.bool()
        physical_weights = torch.where(must_flat, 0, weights[:count])
        if force_cover is not None:
            cover = force_cover[:count].bool()
            if cover.shape != must_flat.shape:
                raise ValueError("physical force_short_cover_mask differs from model actions")
            physical_weights = torch.where(
                cover & (physical_weights < 0), 0, physical_weights
            )
            for key in ("can_short_open_mask", "can_short_open_open_mask"):
                value = supplied.get(key)
                if isinstance(value, torch.Tensor):
                    supplied[key] = value[:count].bool() & ~cover
        # The accepted source owns all price/volume paths.  The old compressed
        # tape and close-gap proxy remain attached only for legacy coverage and
        # checkpoint migration; they must not be executed a second time.
        supplied.pop("overnight_log_returns", None)
        for key in ("cash_dividend_yield", "cash_dividend_payment_delay_sessions"):
            value = supplied.pop(key, None)
            if value is not None and not bool((value[:count] == 0).all()):
                raise ValueError(
                    f"physical avoid-mode source cannot consume exact field {key}"
                )
        advance = supplied.pop("state_advance_mask", None)
        if advance is not None and not bool(advance[:count].bool().all()):
            raise ValueError("physical source cannot skip an owned exchange session")
        # Capacity is explicitly owned by the prepared 50%-minute session;
        # the daily-volume and short-availability proxies do not govern it.
        supplied.pop("volume_limit_weights", None)
        supplied.pop("short_capacity_weights", None)
        supplied.pop("sample_mask", None)
        for key in ("benchmark_returns", "can_buy_mask", "can_sell_mask", "can_short_open_mask",
                    "can_short_open_open_mask", "day_trade_eligible_mask",
                    "day_trade_can_buy_open_mask", "day_trade_can_sell_open_mask",
                    "session_month_ids", "commission_rebate_payment_eligible_mask"):
            value = supplied.get(key)
            if isinstance(value, torch.Tensor):
                supplied[key] = value[:count]
        # Legacy initial_* entries are the trainer's old account buffers, not
        # a physical FIFO state. The caller retains only the explicit state.
        physical_aux = {"initial_day_trade_carry_state": previous}
        supplied["aux_outputs"] = physical_aux
        result = loss_fn(physical_weights, returns[:count], mask[:count],
                         day_trade_carry_sessions=sessions, **supplied)
        final = physical_aux.get("_final_day_trade_carry_state")
        if not isinstance(final, DayTradeCarryState) or final.last_session_day != sessions[-1].day:
            raise RuntimeError("physical loss did not commit its final session state")
        # Never let the generic nonfinite-batch skip path discard a failed
        # chronological account and continue with the next day's policy.
        if not bool(torch.isfinite(result).all()) or bool(final.inventory.failed != 0):
            raise FloatingPointError("physical training trajectory failed; cannot skip a batch")
        aux.update(physical_aux)
        return result

    return call


def bind_physical_carry_backtest(
    runner: Callable, *, source: PreparedDayTradeCarrySource, split,
    start: int, end: int, device: torch.device, previous: DayTradeCarryState | None,
) -> Callable:
    """Adapt the canonical eval chunk, retaining its actual physical endpoint."""
    # Share padding, legacy-field rejection and chronology with the loss
    # adapter. This closure translates names only, never recalculates returns.
    def invoke(weights, returns, mask, *, aux_outputs, **kwargs):
        kwargs["overnight_returns"] = kwargs.pop("overnight_log_returns", None)
        benchmark = kwargs.pop("benchmark_returns")
        result = runner(weights, returns, mask, benchmark,
                        initial_day_trade_carry_state=previous, **kwargs)
        aux_outputs["_final_day_trade_carry_state"] = result.day_trade_carry_state
        invoke.result = result
        return result.strategy_returns

    invoke.result = None
    bound = bind_physical_carry_loss(invoke, source=source, split=split,
                                     start=start, end=end, device=device, previous=previous)

    def call(weights, returns, mask, benchmark, buy_fee_rate, sell_fee_rate, **kwargs):
        supplied = {key: value for key, value in kwargs.items() if not key.startswith("initial_")}
        supplied["overnight_log_returns"] = supplied.pop("overnight_returns", None)
        bound(weights, returns, mask, benchmark_returns=benchmark,
              buy_fee_rate=buy_fee_rate, sell_fee_rate=sell_fee_rate, aux_outputs={}, **supplied)
        return invoke.result

    return call
