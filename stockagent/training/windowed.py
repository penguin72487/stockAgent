from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any

import torch

from stockagent.backtest.tw_execution import normalize_execution_mode
from stockagent.training.dataset import CrossSectionalDataset, execution_feature_lag


@dataclass(slots=True)
class WindowedSplitTensors:
    features: torch.Tensor
    valid_indices: torch.Tensor
    future_log_returns: torch.Tensor
    tradable_mask: torch.Tensor
    can_buy_mask: torch.Tensor
    can_sell_mask: torch.Tensor
    benchmark: torch.Tensor
    lookback: int
    volume_notional: torch.Tensor | None = None
    can_short_open_mask: torch.Tensor | None = None
    force_short_cover_mask: torch.Tensor | None = None
    force_exit_mask: torch.Tensor | None = None
    sample_mask: torch.Tensor | None = None
    symbol_indices: torch.Tensor | None = None
    execution_mode: str = "naive"
    session_advance_mask: torch.Tensor | None = None
    day_trade_eligible_mask: torch.Tensor | None = None
    day_trade_can_buy_open_mask: torch.Tensor | None = None
    day_trade_can_sell_open_mask: torch.Tensor | None = None
    unresolved_corporate_action_mask: torch.Tensor | None = None
    cash_dividend_yield: torch.Tensor | None = None
    cash_dividend_payment_delay_sessions: torch.Tensor | None = None
    short_capacity_shares: torch.Tensor | None = None
    short_margin_rate: torch.Tensor | None = None
    short_capacity_notional: torch.Tensor | None = None
    _window_offsets: torch.Tensor = field(init=False, repr=False)
    _feature_lag: int = field(init=False, repr=False)
    _valid_indices_are_contiguous: bool = field(init=False, repr=False)
    _valid_indices_cpu: torch.Tensor = field(init=False, repr=False)
    _first_valid_index: int = field(init=False, repr=False)
    _contiguous_prefix_len: int = field(init=False, repr=False)
    _default_sample_mask: torch.Tensor = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.lookback = int(self.lookback)
        self.execution_mode = normalize_execution_mode(self.execution_mode)
        self._feature_lag = execution_feature_lag(self.execution_mode)
        if self.lookback < 1:
            raise ValueError("lookback must be >= 1")
        if self.features.dim() != 3:
            raise ValueError(f"features must have shape [T,S,F], got {tuple(self.features.shape)}")
        if self.valid_indices.dim() != 1:
            raise ValueError("valid_indices must be 1D")
        self._valid_indices_cpu = self.valid_indices.detach().to(device=torch.device("cpu"), dtype=torch.long)
        if self.volume_notional is not None and self.volume_notional.shape != self.future_log_returns.shape:
            raise ValueError(
                "volume_notional must have the same shape as future_log_returns: "
                f"{tuple(self.volume_notional.shape)} != {tuple(self.future_log_returns.shape)}"
            )
        expected_symbol_shape = tuple(self.future_log_returns.shape)
        if self.short_capacity_shares is None:
            self.short_capacity_shares = torch.zeros(
                (),
                dtype=torch.int64,
                device=self.future_log_returns.device,
            ).expand(expected_symbol_shape)
        else:
            if tuple(self.short_capacity_shares.shape) != expected_symbol_shape:
                raise ValueError(
                    "short_capacity_shares must have shape [T,S] matching "
                    f"future_log_returns: {tuple(self.short_capacity_shares.shape)} "
                    f"!= {expected_symbol_shape}"
                )
            capacity = self.short_capacity_shares
            if capacity.is_floating_point():
                finite_capacity = torch.isfinite(capacity)
                invalid_capacity = torch.isinf(capacity) | (
                    finite_capacity
                    & (
                        (capacity < 0)
                        | (capacity != torch.floor(capacity))
                        | (capacity > torch.iinfo(torch.int64).max)
                    )
                )
                if bool(invalid_capacity.any().item()):
                    raise ValueError(
                        "short_capacity_shares must contain non-negative integer "
                        "shares or NaN for missing evidence"
                    )
                capacity = torch.where(
                    finite_capacity,
                    capacity,
                    torch.zeros_like(capacity),
                )
            elif bool((capacity < 0).any().item()):
                raise ValueError("short_capacity_shares must be non-negative")
            self.short_capacity_shares = capacity.to(dtype=torch.int64)
        if self.short_margin_rate is None:
            self.short_margin_rate = torch.full(
                (),
                float("nan"),
                dtype=torch.float32,
                device=self.future_log_returns.device,
            ).expand(expected_symbol_shape)
        else:
            if tuple(self.short_margin_rate.shape) != expected_symbol_shape:
                raise ValueError(
                    "short_margin_rate must have shape [T,S] matching "
                    f"future_log_returns: {tuple(self.short_margin_rate.shape)} "
                    f"!= {expected_symbol_shape}"
                )
            margin_rate = self.short_margin_rate.to(dtype=torch.float32)
            invalid_margin_rate = torch.isinf(margin_rate) | (
                torch.isfinite(margin_rate) & (margin_rate < 0)
            )
            if bool(invalid_margin_rate.any().item()):
                raise ValueError("short_margin_rate must be non-negative or NaN")
            self.short_margin_rate = margin_rate
        if self.short_capacity_notional is None:
            self.short_capacity_notional = torch.zeros(
                (),
                dtype=torch.float32,
                device=self.future_log_returns.device,
            ).expand(expected_symbol_shape)
        else:
            if tuple(self.short_capacity_notional.shape) != expected_symbol_shape:
                raise ValueError(
                    "short_capacity_notional must have shape [T,S] matching "
                    f"future_log_returns: {tuple(self.short_capacity_notional.shape)} "
                    f"!= {expected_symbol_shape}"
                )
            capacity_notional = self.short_capacity_notional.to(
                dtype=torch.float32
            )
            if bool(
                (
                    ~torch.isfinite(capacity_notional)
                    | (capacity_notional < 0)
                ).any().item()
            ):
                raise ValueError(
                    "short_capacity_notional must contain finite non-negative values"
                )
            self.short_capacity_notional = capacity_notional
        if self.session_advance_mask is None:
            self.session_advance_mask = torch.ones(
                int(self.features.size(0)),
                dtype=torch.bool,
                device=self.features.device,
            )
        else:
            self.session_advance_mask = self.session_advance_mask.to(dtype=torch.bool)
        if self.session_advance_mask.dim() != 1 or int(self.session_advance_mask.numel()) != int(
            self.features.size(0)
        ):
            raise ValueError(
                "session_advance_mask must have shape [T] matching features: "
                f"{tuple(self.session_advance_mask.shape)} != ({int(self.features.size(0))},)"
            )
        if self.day_trade_eligible_mask is not None:
            self.day_trade_eligible_mask = self.day_trade_eligible_mask.to(dtype=torch.bool)
            expected_eligibility_shape = (
                int(self.features.size(0)),
                int(self.features.size(1)),
            )
            if tuple(self.day_trade_eligible_mask.shape) != expected_eligibility_shape:
                raise ValueError(
                    "day_trade_eligible_mask must have shape [T,S] matching features: "
                    f"{tuple(self.day_trade_eligible_mask.shape)} != {expected_eligibility_shape}"
                )
        if self.execution_mode == "tw_day_trade" and self.day_trade_eligible_mask is None:
            raise ValueError(
                "tw_day_trade requires a point-in-time day_trade_eligible_mask; "
                "current eligibility must not be projected backward"
            )
        for name in (
            "day_trade_can_buy_open_mask",
            "day_trade_can_sell_open_mask",
        ):
            value = getattr(self, name)
            if value is not None:
                value = value.to(dtype=torch.bool)
                setattr(self, name, value)
                expected_shape = (
                    int(self.features.size(0)),
                    int(self.features.size(1)),
                )
                if tuple(value.shape) != expected_shape:
                    raise ValueError(
                        f"{name} must have shape [T,S] matching features: "
                        f"{tuple(value.shape)} != {expected_shape}"
                    )
        if self.execution_mode == "tw_day_trade" and (
            self.day_trade_can_buy_open_mask is None
            or self.day_trade_can_sell_open_mask is None
        ):
            raise ValueError(
                "tw_day_trade requires point-in-time open-session buy/sell masks; "
                "close-session masks must not be reused for the open"
            )
        if self.unresolved_corporate_action_mask is not None:
            self.unresolved_corporate_action_mask = (
                self.unresolved_corporate_action_mask.to(dtype=torch.bool)
            )
            expected_shape = (
                int(self.features.size(0)),
                int(self.features.size(1)),
            )
            if tuple(self.unresolved_corporate_action_mask.shape) != expected_shape:
                raise ValueError(
                    "unresolved_corporate_action_mask must have shape [T,S] "
                    f"matching features: {tuple(self.unresolved_corporate_action_mask.shape)} "
                    f"!= {expected_shape}"
                )
        if (
            self.execution_mode == "tw_cash"
            and self.unresolved_corporate_action_mask is None
        ):
            raise ValueError(
                "tw_cash requires unresolved_corporate_action_mask; raw-price "
                "holdings cannot safely consume adjusted total returns without a "
                "verified corporate-action ledger"
            )
        if (self.cash_dividend_yield is None) != (
            self.cash_dividend_payment_delay_sessions is None
        ):
            raise ValueError(
                "cash-dividend amount and payment-delay tensors must be paired"
            )
        if self.cash_dividend_yield is not None:
            expected_shape = (
                int(self.features.size(0)),
                int(self.features.size(1)),
            )
            self.cash_dividend_yield = self.cash_dividend_yield.to(
                dtype=torch.float32
            )
            self.cash_dividend_payment_delay_sessions = (
                self.cash_dividend_payment_delay_sessions.to(dtype=torch.int64)
            )
            if (
                tuple(self.cash_dividend_yield.shape) != expected_shape
                or tuple(self.cash_dividend_payment_delay_sessions.shape)
                != expected_shape
            ):
                raise ValueError(
                    "cash-dividend tensors must have shape [T,S] matching features"
                )
        if self.can_short_open_mask is None:
            self.can_short_open_mask = (
                torch.zeros_like(self.can_sell_mask, dtype=torch.bool)
                if self.execution_mode == "tw_cash"
                else self.can_sell_mask.clone()
            )
        else:
            self.can_short_open_mask = self.can_short_open_mask.to(dtype=torch.bool)
        if tuple(self.can_short_open_mask.shape) != expected_symbol_shape:
            raise ValueError(
                "can_short_open_mask must have shape [T,S] matching "
                f"future_log_returns: {tuple(self.can_short_open_mask.shape)} "
                f"!= {expected_symbol_shape}"
            )
        if self.execution_mode == "tw_cash":
            self.can_short_open_mask = (
                self.can_short_open_mask
                & self.can_sell_mask.to(dtype=torch.bool)
                & (self.short_capacity_shares > 0)
            )
            self.short_capacity_shares = torch.where(
                self.can_short_open_mask,
                self.short_capacity_shares,
                torch.zeros_like(self.short_capacity_shares),
            )
            self.short_capacity_notional = torch.where(
                self.can_short_open_mask,
                self.short_capacity_notional,
                torch.zeros_like(self.short_capacity_notional),
            )
        if self.force_short_cover_mask is None:
            self.force_short_cover_mask = torch.zeros_like(self.tradable_mask, dtype=torch.bool)
        if self.force_exit_mask is None:
            self.force_exit_mask = torch.zeros_like(self.tradable_mask, dtype=torch.bool)
        if self.symbol_indices is not None:
            self.symbol_indices = self.symbol_indices.detach().to(device=self.features.device, dtype=torch.long)
            if self.symbol_indices.dim() != 1:
                raise ValueError("symbol_indices must be 1D")
            if int(self.symbol_indices.numel()) != int(self.features.size(1)):
                raise ValueError(
                    "symbol_indices length must match features symbol dimension: "
                    f"{int(self.symbol_indices.numel())} != {int(self.features.size(1))}"
                )
        self._window_offsets = torch.arange(
            self.lookback - 1,
            -1,
            -1,
            device=self.valid_indices.device,
            dtype=torch.long,
        )
        if int(self.valid_indices.numel()) == 0:
            self._valid_indices_are_contiguous = True
            self._first_valid_index = 0
            self._contiguous_prefix_len = 0
        else:
            self._first_valid_index = int(self.valid_indices[0].detach().cpu().item())
            if int(self.valid_indices.numel()) == 1:
                self._valid_indices_are_contiguous = True
                self._contiguous_prefix_len = 1
            else:
                expected_last = self._first_valid_index + int(self.valid_indices.numel()) - 1
                actual_last = int(self.valid_indices[-1].detach().cpu().item())
                diffs = self.valid_indices[1:] - self.valid_indices[:-1]
                contiguous_diffs = (diffs == 1).detach().cpu()
                if bool(torch.all(contiguous_diffs).item()):
                    self._valid_indices_are_contiguous = actual_last == expected_last
                    self._contiguous_prefix_len = int(self.valid_indices.numel())
                else:
                    first_break = int((~contiguous_diffs).nonzero(as_tuple=False)[0].item())
                    self._valid_indices_are_contiguous = False
                    self._contiguous_prefix_len = first_break + 1
        self._default_sample_mask = torch.ones(
            int(self.valid_indices.numel()),
            dtype=torch.bool,
            device=self.valid_indices.device,
        )

    @staticmethod
    def _prepare_timer_start() -> float:
        return time.perf_counter()

    @staticmethod
    def _prepare_timer_stop(timing: Any | None, key: str, start: float) -> None:
        if timing is None:
            return
        attr = f"prepare_{key}_s"
        try:
            setattr(timing, attr, float(getattr(timing, attr, 0.0)) + (time.perf_counter() - start))
        except Exception:
            return

    def _sample_mask_slice(self, start: int, rows: int) -> torch.Tensor:
        if self.sample_mask is None:
            return self._default_sample_mask.narrow(0, int(start), int(rows))
        return self.sample_mask.narrow(0, int(start), int(rows))

    def _execution_masks_for_date_indices(
        self,
        date_indices: torch.Tensor,
        sample_mask: torch.Tensor,
    ) -> tuple[
        torch.Tensor,
        torch.Tensor | None,
        torch.Tensor | None,
        torch.Tensor | None,
        torch.Tensor | None,
        torch.Tensor | None,
        torch.Tensor | None,
    ]:
        session_advance = self.session_advance_mask[date_indices]
        sample_gate = sample_mask.to(device=session_advance.device, dtype=torch.bool)
        session_advance = session_advance & sample_gate
        eligibility = (
            None
            if self.day_trade_eligible_mask is None
            else self.day_trade_eligible_mask[date_indices] & sample_gate[:, None]
        )
        can_buy_open = (
            None
            if self.day_trade_can_buy_open_mask is None
            else self.day_trade_can_buy_open_mask[date_indices] & sample_gate[:, None]
        )
        can_sell_open = (
            None
            if self.day_trade_can_sell_open_mask is None
            else self.day_trade_can_sell_open_mask[date_indices] & sample_gate[:, None]
        )
        unresolved_action = (
            None
            if self.unresolved_corporate_action_mask is None
            else self.unresolved_corporate_action_mask[date_indices]
            & sample_gate[:, None]
        )
        cash_dividend_yield = (
            None
            if self.cash_dividend_yield is None
            else self.cash_dividend_yield[date_indices]
            * sample_gate[:, None].to(dtype=self.cash_dividend_yield.dtype)
        )
        cash_dividend_delay = (
            None
            if self.cash_dividend_payment_delay_sessions is None
            else torch.where(
                sample_gate[:, None],
                self.cash_dividend_payment_delay_sessions[date_indices],
                torch.zeros_like(
                    self.cash_dividend_payment_delay_sessions[date_indices]
                ),
            )
        )
        return (
            session_advance,
            eligibility,
            can_buy_open,
            can_sell_open,
            unresolved_action,
            cash_dividend_yield,
            cash_dividend_delay,
        )

    def _execution_masks_for_date_range(
        self,
        date_start: int,
        rows: int,
        sample_mask: torch.Tensor,
    ) -> tuple[
        torch.Tensor,
        torch.Tensor | None,
        torch.Tensor | None,
        torch.Tensor | None,
        torch.Tensor | None,
        torch.Tensor | None,
        torch.Tensor | None,
    ]:
        session_advance = self.session_advance_mask.narrow(0, int(date_start), int(rows))
        sample_gate = sample_mask.to(device=session_advance.device, dtype=torch.bool)
        session_advance = session_advance & sample_gate
        eligibility = (
            None
            if self.day_trade_eligible_mask is None
            else self.day_trade_eligible_mask.narrow(0, int(date_start), int(rows))
            & sample_gate[:, None]
        )
        can_buy_open = (
            None
            if self.day_trade_can_buy_open_mask is None
            else self.day_trade_can_buy_open_mask.narrow(0, int(date_start), int(rows))
            & sample_gate[:, None]
        )
        can_sell_open = (
            None
            if self.day_trade_can_sell_open_mask is None
            else self.day_trade_can_sell_open_mask.narrow(0, int(date_start), int(rows))
            & sample_gate[:, None]
        )
        unresolved_action = (
            None
            if self.unresolved_corporate_action_mask is None
            else self.unresolved_corporate_action_mask.narrow(
                0, int(date_start), int(rows)
            )
            & sample_gate[:, None]
        )
        cash_dividend_yield = (
            None
            if self.cash_dividend_yield is None
            else self.cash_dividend_yield.narrow(0, int(date_start), int(rows))
            * sample_gate[:, None].to(dtype=self.cash_dividend_yield.dtype)
        )
        cash_dividend_delay = (
            None
            if self.cash_dividend_payment_delay_sessions is None
            else torch.where(
                sample_gate[:, None],
                self.cash_dividend_payment_delay_sessions.narrow(
                    0, int(date_start), int(rows)
                ),
                torch.zeros_like(
                    self.cash_dividend_payment_delay_sessions.narrow(
                        0, int(date_start), int(rows)
                    )
                ),
            )
        )
        return (
            session_advance,
            eligibility,
            can_buy_open,
            can_sell_open,
            unresolved_action,
            cash_dividend_yield,
            cash_dividend_delay,
        )

    @staticmethod
    def _to_device(
        tensor: torch.Tensor,
        device: torch.device,
        non_blocking: bool,
        timing: Any | None,
    ) -> torch.Tensor:
        timer = WindowedSplitTensors._prepare_timer_start()
        out = tensor.to(device=device, non_blocking=non_blocking)
        WindowedSplitTensors._prepare_timer_stop(timing, "device_move", timer)
        return out

    def _day_trade_open_mask_items(
        self,
        can_buy_open: torch.Tensor | None,
        can_sell_open: torch.Tensor | None,
        device: torch.device,
        non_blocking: bool,
        timing: Any | None,
    ) -> dict[str, torch.Tensor]:
        if can_buy_open is None and can_sell_open is None:
            return {}
        if can_buy_open is None or can_sell_open is None:
            raise RuntimeError("day-trade open-session masks must be present as a pair")
        return {
            "day_trade_can_buy_open_mask": self._to_device(
                can_buy_open, device, non_blocking, timing
            ),
            "day_trade_can_sell_open_mask": self._to_device(
                can_sell_open, device, non_blocking, timing
            ),
        }

    def _corporate_action_mask_items(
        self,
        unresolved_action: torch.Tensor | None,
        cash_dividend_yield: torch.Tensor | None,
        cash_dividend_delay: torch.Tensor | None,
        device: torch.device,
        non_blocking: bool,
        timing: Any | None,
    ) -> dict[str, torch.Tensor]:
        items: dict[str, torch.Tensor] = {}
        if unresolved_action is not None:
            items["unresolved_corporate_action_mask"] = self._to_device(
                unresolved_action, device, non_blocking, timing
            )
        if cash_dividend_yield is not None:
            if cash_dividend_delay is None:
                raise RuntimeError("cash-dividend tensors must be paired")
            items["cash_dividend_yield"] = self._to_device(
                cash_dividend_yield, device, non_blocking, timing
            )
            items["cash_dividend_payment_delay_sessions"] = self._to_device(
                cash_dividend_delay, device, non_blocking, timing
            )
        return items

    def _short_margin_for_date_indices(
        self,
        date_indices: torch.Tensor,
        sample_mask: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        capacity = self.short_capacity_shares[date_indices]
        margin_rate = self.short_margin_rate[date_indices]
        capacity_notional = self.short_capacity_notional[date_indices]
        sample_gate = sample_mask.to(device=capacity.device, dtype=torch.bool)[
            :, None
        ]
        return (
            torch.where(sample_gate, capacity, torch.zeros_like(capacity)),
            torch.where(
                sample_gate,
                margin_rate,
                torch.full_like(margin_rate, float("nan")),
            ),
            torch.where(
                sample_gate,
                capacity_notional,
                torch.zeros_like(capacity_notional),
            ),
        )

    def _short_margin_for_date_range(
        self,
        date_start: int,
        rows: int,
        sample_mask: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        capacity = self.short_capacity_shares.narrow(0, int(date_start), int(rows))
        margin_rate = self.short_margin_rate.narrow(0, int(date_start), int(rows))
        capacity_notional = self.short_capacity_notional.narrow(
            0,
            int(date_start),
            int(rows),
        )
        sample_gate = sample_mask.to(device=capacity.device, dtype=torch.bool)[
            :, None
        ]
        return (
            torch.where(sample_gate, capacity, torch.zeros_like(capacity)),
            torch.where(
                sample_gate,
                margin_rate,
                torch.full_like(margin_rate, float("nan")),
            ),
            torch.where(
                sample_gate,
                capacity_notional,
                torch.zeros_like(capacity_notional),
            ),
        )

    def __len__(self) -> int:
        return int(self.valid_indices.numel())

    @property
    def num_symbols(self) -> int:
        return int(self.features.size(1))

    def to_device_cache(self, device: torch.device, non_blocking: bool = True) -> "WindowedSplitTensors":
        return WindowedSplitTensors(
            features=self.features.to(device=device, non_blocking=non_blocking),
            valid_indices=self.valid_indices.to(device=device, non_blocking=non_blocking),
            future_log_returns=self.future_log_returns.to(device=device, non_blocking=non_blocking),
            tradable_mask=self.tradable_mask.to(device=device, non_blocking=non_blocking),
            can_buy_mask=self.can_buy_mask.to(device=device, non_blocking=non_blocking),
            can_sell_mask=self.can_sell_mask.to(device=device, non_blocking=non_blocking),
            benchmark=self.benchmark.to(device=device, non_blocking=non_blocking),
            lookback=self.lookback,
            volume_notional=(
                None
                if self.volume_notional is None
                else self.volume_notional.to(device=device, non_blocking=non_blocking)
            ),
            can_short_open_mask=self.can_short_open_mask.to(device=device, non_blocking=non_blocking),
            short_capacity_shares=self.short_capacity_shares.to(
                device=device, non_blocking=non_blocking
            ),
            short_margin_rate=self.short_margin_rate.to(
                device=device, non_blocking=non_blocking
            ),
            short_capacity_notional=self.short_capacity_notional.to(
                device=device, non_blocking=non_blocking
            ),
            force_short_cover_mask=self.force_short_cover_mask.to(device=device, non_blocking=non_blocking),
            force_exit_mask=self.force_exit_mask.to(device=device, non_blocking=non_blocking),
            sample_mask=(
                None if self.sample_mask is None else self.sample_mask.to(device=device, non_blocking=non_blocking)
            ),
            symbol_indices=(
                None if self.symbol_indices is None else self.symbol_indices.to(device=device, non_blocking=non_blocking)
            ),
            execution_mode=self.execution_mode,
            session_advance_mask=self.session_advance_mask.to(device=device, non_blocking=non_blocking),
            day_trade_eligible_mask=(
                None
                if self.day_trade_eligible_mask is None
                else self.day_trade_eligible_mask.to(device=device, non_blocking=non_blocking)
            ),
            day_trade_can_buy_open_mask=(
                None
                if self.day_trade_can_buy_open_mask is None
                else self.day_trade_can_buy_open_mask.to(device=device, non_blocking=non_blocking)
            ),
            day_trade_can_sell_open_mask=(
                None
                if self.day_trade_can_sell_open_mask is None
                else self.day_trade_can_sell_open_mask.to(device=device, non_blocking=non_blocking)
            ),
            unresolved_corporate_action_mask=(
                None
                if self.unresolved_corporate_action_mask is None
                else self.unresolved_corporate_action_mask.to(
                    device=device, non_blocking=non_blocking
                )
            ),
            cash_dividend_yield=(
                None
                if self.cash_dividend_yield is None
                else self.cash_dividend_yield.to(
                    device=device, non_blocking=non_blocking
                )
            ),
            cash_dividend_payment_delay_sessions=(
                None
                if self.cash_dividend_payment_delay_sessions is None
                else self.cash_dividend_payment_delay_sessions.to(
                    device=device, non_blocking=non_blocking
                )
            ),
        )

    def pin_memory(self) -> "WindowedSplitTensors":
        def _pin(tensor: torch.Tensor) -> torch.Tensor:
            if tensor.device.type != "cpu" or tensor.is_pinned():
                return tensor
            try:
                return tensor.pin_memory()
            except torch.AcceleratorError as exc:
                if "out of memory" not in str(exc).lower():
                    raise
                return tensor

        return WindowedSplitTensors(
            features=_pin(self.features),
            valid_indices=_pin(self.valid_indices),
            future_log_returns=_pin(self.future_log_returns),
            tradable_mask=_pin(self.tradable_mask),
            can_buy_mask=_pin(self.can_buy_mask),
            can_sell_mask=_pin(self.can_sell_mask),
            benchmark=_pin(self.benchmark),
            lookback=self.lookback,
            volume_notional=None if self.volume_notional is None else _pin(self.volume_notional),
            can_short_open_mask=_pin(self.can_short_open_mask),
            short_capacity_shares=_pin(self.short_capacity_shares),
            short_margin_rate=_pin(self.short_margin_rate),
            short_capacity_notional=_pin(self.short_capacity_notional),
            force_short_cover_mask=_pin(self.force_short_cover_mask),
            force_exit_mask=_pin(self.force_exit_mask),
            sample_mask=None if self.sample_mask is None else _pin(self.sample_mask),
            symbol_indices=None if self.symbol_indices is None else _pin(self.symbol_indices),
            execution_mode=self.execution_mode,
            session_advance_mask=_pin(self.session_advance_mask),
            day_trade_eligible_mask=(
                None if self.day_trade_eligible_mask is None else _pin(self.day_trade_eligible_mask)
            ),
            day_trade_can_buy_open_mask=(
                None
                if self.day_trade_can_buy_open_mask is None
                else _pin(self.day_trade_can_buy_open_mask)
            ),
            day_trade_can_sell_open_mask=(
                None
                if self.day_trade_can_sell_open_mask is None
                else _pin(self.day_trade_can_sell_open_mask)
            ),
            unresolved_corporate_action_mask=(
                None
                if self.unresolved_corporate_action_mask is None
                else _pin(self.unresolved_corporate_action_mask)
            ),
            cash_dividend_yield=(
                None
                if self.cash_dividend_yield is None
                else _pin(self.cash_dividend_yield)
            ),
            cash_dividend_payment_delay_sessions=(
                None
                if self.cash_dividend_payment_delay_sessions is None
                else _pin(self.cash_dividend_payment_delay_sessions)
            ),
        )

    def subset_symbols(self, symbol_indices: torch.Tensor) -> "WindowedSplitTensors":
        source_device = self.features.device
        local_indices = symbol_indices.to(device=source_device, dtype=torch.long)
        if local_indices.dim() != 1:
            raise ValueError("symbol_indices must be 1D")
        if int(local_indices.numel()) == 0:
            raise ValueError("symbol_indices must be non-empty")
        if self.symbol_indices is None:
            original_indices = local_indices.detach().clone()
        else:
            original_indices = self.symbol_indices.index_select(0, local_indices).detach().clone()
        return WindowedSplitTensors(
            features=self.features.index_select(1, local_indices),
            valid_indices=self.valid_indices,
            future_log_returns=self.future_log_returns.index_select(1, local_indices),
            tradable_mask=self.tradable_mask.index_select(1, local_indices),
            can_buy_mask=self.can_buy_mask.index_select(1, local_indices),
            can_sell_mask=self.can_sell_mask.index_select(1, local_indices),
            benchmark=self.benchmark,
            lookback=self.lookback,
            volume_notional=(
                None
                if self.volume_notional is None
                else self.volume_notional.index_select(1, local_indices)
            ),
            can_short_open_mask=self.can_short_open_mask.index_select(1, local_indices),
            short_capacity_shares=self.short_capacity_shares.index_select(
                1, local_indices
            ),
            short_margin_rate=self.short_margin_rate.index_select(1, local_indices),
            short_capacity_notional=self.short_capacity_notional.index_select(
                1, local_indices
            ),
            force_short_cover_mask=self.force_short_cover_mask.index_select(1, local_indices),
            force_exit_mask=self.force_exit_mask.index_select(1, local_indices),
            sample_mask=self.sample_mask,
            symbol_indices=original_indices,
            execution_mode=self.execution_mode,
            session_advance_mask=self.session_advance_mask,
            day_trade_eligible_mask=(
                None
                if self.day_trade_eligible_mask is None
                else self.day_trade_eligible_mask.index_select(1, local_indices)
            ),
            day_trade_can_buy_open_mask=(
                None
                if self.day_trade_can_buy_open_mask is None
                else self.day_trade_can_buy_open_mask.index_select(1, local_indices)
            ),
            day_trade_can_sell_open_mask=(
                None
                if self.day_trade_can_sell_open_mask is None
                else self.day_trade_can_sell_open_mask.index_select(1, local_indices)
            ),
            unresolved_corporate_action_mask=(
                None
                if self.unresolved_corporate_action_mask is None
                else self.unresolved_corporate_action_mask.index_select(
                    1, local_indices
                )
            ),
            cash_dividend_yield=(
                None
                if self.cash_dividend_yield is None
                else self.cash_dividend_yield.index_select(1, local_indices)
            ),
            cash_dividend_payment_delay_sessions=(
                None
                if self.cash_dividend_payment_delay_sessions is None
                else self.cash_dividend_payment_delay_sessions.index_select(
                    1, local_indices
                )
            ),
        )

    def pad_symbols(self, target_symbols: int, *, pad_symbol_index: int | None = None) -> "WindowedSplitTensors":
        target_symbols = int(target_symbols)
        current_symbols = int(self.features.size(1))
        if target_symbols < current_symbols:
            raise ValueError(
                f"target_symbols must be >= current symbol count: {target_symbols} < {current_symbols}"
            )
        if target_symbols == current_symbols:
            return self
        if current_symbols <= 0:
            raise ValueError("cannot pad an empty symbol dimension")

        pad_count = target_symbols - current_symbols

        def _pad_symbol_dim(tensor: torch.Tensor, fill_value: int | float | bool = 0) -> torch.Tensor:
            if tensor.dim() < 2 or int(tensor.size(1)) != current_symbols:
                raise ValueError(
                    "symbol-wise tensor must have symbol dimension at axis 1: "
                    f"shape={tuple(tensor.shape)}, expected_symbols={current_symbols}"
                )
            pad_shape = (int(tensor.size(0)), pad_count, *tuple(tensor.shape[2:]))
            pad = tensor.new_full(pad_shape, fill_value)
            return torch.cat((tensor, pad), dim=1)

        padded_symbol_indices = self.symbol_indices
        if self.symbol_indices is not None:
            if pad_symbol_index is None:
                pad_symbol_index = int(self.symbol_indices[0].detach().cpu().item())
            index_pad = self.symbol_indices.new_full((pad_count,), int(pad_symbol_index))
            padded_symbol_indices = torch.cat((self.symbol_indices, index_pad), dim=0)

        return WindowedSplitTensors(
            features=_pad_symbol_dim(self.features, 0.0),
            valid_indices=self.valid_indices,
            future_log_returns=_pad_symbol_dim(self.future_log_returns, 0.0),
            tradable_mask=_pad_symbol_dim(self.tradable_mask, False),
            can_buy_mask=_pad_symbol_dim(self.can_buy_mask, False),
            can_sell_mask=_pad_symbol_dim(self.can_sell_mask, False),
            benchmark=self.benchmark,
            lookback=self.lookback,
            volume_notional=None if self.volume_notional is None else _pad_symbol_dim(self.volume_notional, 0.0),
            can_short_open_mask=_pad_symbol_dim(self.can_short_open_mask, False),
            short_capacity_shares=_pad_symbol_dim(
                self.short_capacity_shares, 0
            ),
            short_margin_rate=_pad_symbol_dim(
                self.short_margin_rate, float("nan")
            ),
            short_capacity_notional=_pad_symbol_dim(
                self.short_capacity_notional, 0.0
            ),
            force_short_cover_mask=_pad_symbol_dim(self.force_short_cover_mask, False),
            force_exit_mask=_pad_symbol_dim(self.force_exit_mask, False),
            sample_mask=self.sample_mask,
            symbol_indices=padded_symbol_indices,
            execution_mode=self.execution_mode,
            session_advance_mask=self.session_advance_mask,
            day_trade_eligible_mask=(
                None
                if self.day_trade_eligible_mask is None
                else _pad_symbol_dim(self.day_trade_eligible_mask, False)
            ),
            day_trade_can_buy_open_mask=(
                None
                if self.day_trade_can_buy_open_mask is None
                else _pad_symbol_dim(self.day_trade_can_buy_open_mask, False)
            ),
            day_trade_can_sell_open_mask=(
                None
                if self.day_trade_can_sell_open_mask is None
                else _pad_symbol_dim(self.day_trade_can_sell_open_mask, False)
            ),
            unresolved_corporate_action_mask=(
                None
                if self.unresolved_corporate_action_mask is None
                else _pad_symbol_dim(self.unresolved_corporate_action_mask, False)
            ),
            cash_dividend_yield=(
                None
                if self.cash_dividend_yield is None
                else _pad_symbol_dim(self.cash_dividend_yield, 0.0)
            ),
            cash_dividend_payment_delay_sessions=(
                None
                if self.cash_dividend_payment_delay_sessions is None
                else _pad_symbol_dim(
                    self.cash_dividend_payment_delay_sessions, 0
                )
            ),
        )

    def clamp_symbol_indices(self, max_symbols: int) -> "WindowedSplitTensors":
        if self.symbol_indices is None:
            return self
        max_symbols = int(max_symbols)
        if max_symbols <= 0:
            raise ValueError("max_symbols must be positive")
        clamped_indices = self.symbol_indices.clamp(0, max_symbols - 1)
        if bool(torch.equal(clamped_indices, self.symbol_indices)):
            return self
        return WindowedSplitTensors(
            features=self.features,
            valid_indices=self.valid_indices,
            future_log_returns=self.future_log_returns,
            tradable_mask=self.tradable_mask,
            can_buy_mask=self.can_buy_mask,
            can_sell_mask=self.can_sell_mask,
            benchmark=self.benchmark,
            lookback=self.lookback,
            volume_notional=self.volume_notional,
            can_short_open_mask=self.can_short_open_mask,
            short_capacity_shares=self.short_capacity_shares,
            short_margin_rate=self.short_margin_rate,
            short_capacity_notional=self.short_capacity_notional,
            force_short_cover_mask=self.force_short_cover_mask,
            force_exit_mask=self.force_exit_mask,
            sample_mask=self.sample_mask,
            symbol_indices=clamped_indices,
            execution_mode=self.execution_mode,
            session_advance_mask=self.session_advance_mask,
            day_trade_eligible_mask=self.day_trade_eligible_mask,
            day_trade_can_buy_open_mask=self.day_trade_can_buy_open_mask,
            day_trade_can_sell_open_mask=self.day_trade_can_sell_open_mask,
            unresolved_corporate_action_mask=self.unresolved_corporate_action_mask,
            cash_dividend_yield=self.cash_dividend_yield,
            cash_dividend_payment_delay_sessions=(
                self.cash_dividend_payment_delay_sessions
            ),
        )

    def _window_indices_for_rows(self, row_indices: torch.Tensor) -> torch.Tensor:
        row_indices = row_indices.to(device=self.valid_indices.device, dtype=torch.long)
        date_idx = self.valid_indices[row_indices]
        return date_idx[:, None] - self._feature_lag - self._window_offsets[None, :]

    def _window_view_for_contiguous_rows(
        self,
        start: int,
        end: int,
        *,
        contiguous_x: bool,
        prepare_timing: Any | None = None,
    ) -> tuple[torch.Tensor, int, torch.Tensor]:
        batch_rows = int(end) - int(start)
        source_device = self.features.device
        date_start = self._first_valid_index + int(start)
        feature_start = date_start - self._feature_lag - self.lookback + 1
        if batch_rows <= 0 or feature_start < 0:
            raise ValueError("invalid contiguous window slice")
        timer = self._prepare_timer_start()
        source = self.features.narrow(0, feature_start, batch_rows + self.lookback - 1)
        x = source.unfold(0, self.lookback, 1).permute(0, 3, 1, 2)
        self._prepare_timer_stop(prepare_timing, "window_slice", timer)
        if contiguous_x:
            timer = self._prepare_timer_start()
            x = x.contiguous()
            self._prepare_timer_stop(prepare_timing, "contiguous", timer)
        timer = self._prepare_timer_start()
        sample_mask = self._sample_mask_slice(int(start), batch_rows).to(device=source_device, non_blocking=False)
        self._prepare_timer_stop(prepare_timing, "mask_build", timer)
        return x, date_start, sample_mask

    def _batch_from_row_indices(
        self,
        row_indices: torch.Tensor,
        device: torch.device,
        non_blocking: bool,
        *,
        contiguous_x: bool = True,
        prepare_timing: Any | None = None,
    ) -> dict[str, torch.Tensor]:
        source_device = self.features.device
        timer = self._prepare_timer_start()
        row_indices = row_indices.to(device=source_device, dtype=torch.long)
        self._prepare_timer_stop(prepare_timing, "dtype_cast", timer)
        timer = self._prepare_timer_start()
        window_idx = self._window_indices_for_rows(row_indices)
        date_idx = self.valid_indices[row_indices]
        self._prepare_timer_stop(prepare_timing, "index_build", timer)

        timer = self._prepare_timer_start()
        x = self.features[window_idx]
        self._prepare_timer_stop(prepare_timing, "feature_gather", timer)
        if contiguous_x:
            timer = self._prepare_timer_start()
            x = x.contiguous()
            self._prepare_timer_stop(prepare_timing, "contiguous", timer)
        timer = self._prepare_timer_start()
        sample_mask = self._default_sample_mask[row_indices] if self.sample_mask is None else self.sample_mask[row_indices]
        (
            session_advance_mask,
            day_trade_eligible_mask,
            day_trade_can_buy_open_mask,
            day_trade_can_sell_open_mask,
            unresolved_corporate_action_mask,
            cash_dividend_yield,
            cash_dividend_payment_delay_sessions,
        ) = self._execution_masks_for_date_indices(date_idx, sample_mask)
        self._prepare_timer_stop(prepare_timing, "mask_build", timer)
        timer = self._prepare_timer_start()
        future_log_returns = self.future_log_returns[date_idx]
        benchmark = self.benchmark[date_idx]
        volume_notional = None if self.volume_notional is None else self.volume_notional[date_idx]
        self._prepare_timer_stop(prepare_timing, "target_gather", timer)
        timer = self._prepare_timer_start()
        tradable_mask = self.tradable_mask[date_idx]
        can_buy_mask = self.can_buy_mask[date_idx]
        can_sell_mask = self.can_sell_mask[date_idx]
        can_short_open_mask = self.can_short_open_mask[date_idx]
        (
            short_capacity_shares,
            short_margin_rate,
            short_capacity_notional,
        ) = (
            self._short_margin_for_date_indices(date_idx, sample_mask)
        )
        force_short_cover_mask = self.force_short_cover_mask[date_idx]
        force_exit_mask = self.force_exit_mask[date_idx]
        self._prepare_timer_stop(prepare_timing, "mask_build", timer)
        return {
            "x": self._to_device(x, device, non_blocking, prepare_timing),
            **(
                {}
                if self.symbol_indices is None
                else {"symbol_indices": self._to_device(self.symbol_indices, device, non_blocking, prepare_timing)}
            ),
            "future_log_returns": self._to_device(future_log_returns, device, non_blocking, prepare_timing),
            **(
                {}
                if volume_notional is None
                else {"volume_notional": self._to_device(volume_notional, device, non_blocking, prepare_timing)}
            ),
            "tradable_mask": self._to_device(tradable_mask, device, non_blocking, prepare_timing),
            "can_buy_mask": self._to_device(can_buy_mask, device, non_blocking, prepare_timing),
            "can_sell_mask": self._to_device(can_sell_mask, device, non_blocking, prepare_timing),
            "can_short_open_mask": self._to_device(can_short_open_mask, device, non_blocking, prepare_timing),
            "short_capacity_shares": self._to_device(
                short_capacity_shares, device, non_blocking, prepare_timing
            ),
            "short_margin_rate": self._to_device(
                short_margin_rate, device, non_blocking, prepare_timing
            ),
            "short_capacity_notional": self._to_device(
                short_capacity_notional,
                device,
                non_blocking,
                prepare_timing,
            ),
            "force_short_cover_mask": self._to_device(force_short_cover_mask, device, non_blocking, prepare_timing),
            "force_exit_mask": self._to_device(force_exit_mask, device, non_blocking, prepare_timing),
            "benchmark": self._to_device(benchmark, device, non_blocking, prepare_timing),
            "session_advance_mask": self._to_device(
                session_advance_mask, device, non_blocking, prepare_timing
            ),
            **(
                {}
                if day_trade_eligible_mask is None
                else {
                    "day_trade_eligible_mask": self._to_device(
                        day_trade_eligible_mask,
                        device,
                        non_blocking,
                        prepare_timing,
                    )
                }
            ),
            **self._day_trade_open_mask_items(
                day_trade_can_buy_open_mask,
                day_trade_can_sell_open_mask,
                device,
                non_blocking,
                prepare_timing,
            ),
            **self._corporate_action_mask_items(
                unresolved_corporate_action_mask,
                cash_dividend_yield,
                cash_dividend_payment_delay_sessions,
                device,
                non_blocking,
                prepare_timing,
            ),
            "sample_mask": self._to_device(sample_mask, device, non_blocking, prepare_timing),
        }

    def _batch_metadata_from_row_indices(
        self,
        row_indices: torch.Tensor,
        device: torch.device,
        non_blocking: bool,
        prepare_timing: Any | None = None,
    ) -> dict[str, torch.Tensor]:
        source_device = self.valid_indices.device
        timer = self._prepare_timer_start()
        if row_indices.device.type == "cpu":
            row_indices_cpu = row_indices.to(dtype=torch.long)
        else:
            row_indices_cpu = row_indices.detach().to(device=torch.device("cpu"), dtype=torch.long)
        row_indices = row_indices_cpu.to(device=source_device, dtype=torch.long)
        self._prepare_timer_stop(prepare_timing, "dtype_cast", timer)
        timer = self._prepare_timer_start()
        date_idx_control = self._valid_indices_cpu[row_indices_cpu]
        date_idx = self.valid_indices[row_indices]
        if int(date_idx_control.numel()) <= 1:
            rows_are_contiguous = torch.ones((), dtype=torch.bool)
        else:
            rows_are_contiguous = torch.all((date_idx_control[1:] - date_idx_control[:-1]) == 1)
        self._prepare_timer_stop(prepare_timing, "index_build", timer)
        timer = self._prepare_timer_start()
        sample_mask = self._default_sample_mask[row_indices] if self.sample_mask is None else self.sample_mask[row_indices]
        (
            session_advance_mask,
            day_trade_eligible_mask,
            day_trade_can_buy_open_mask,
            day_trade_can_sell_open_mask,
            unresolved_corporate_action_mask,
            cash_dividend_yield,
            cash_dividend_payment_delay_sessions,
        ) = self._execution_masks_for_date_indices(date_idx, sample_mask)
        tradable_mask = self.tradable_mask[date_idx]
        can_buy_mask = self.can_buy_mask[date_idx]
        can_sell_mask = self.can_sell_mask[date_idx]
        can_short_open_mask = self.can_short_open_mask[date_idx]
        (
            short_capacity_shares,
            short_margin_rate,
            short_capacity_notional,
        ) = (
            self._short_margin_for_date_indices(date_idx, sample_mask)
        )
        force_short_cover_mask = self.force_short_cover_mask[date_idx]
        force_exit_mask = self.force_exit_mask[date_idx]
        self._prepare_timer_stop(prepare_timing, "mask_build", timer)
        timer = self._prepare_timer_start()
        future_log_returns = self.future_log_returns[date_idx]
        benchmark = self.benchmark[date_idx]
        volume_notional = None if self.volume_notional is None else self.volume_notional[date_idx]
        self._prepare_timer_stop(prepare_timing, "target_gather", timer)
        return {
            "date_indices": self._to_device(date_idx, device, non_blocking, prepare_timing),
            "date_start": date_idx_control[:1].detach().clone(),
            "rows_are_contiguous": rows_are_contiguous.detach().clone(),
            **(
                {}
                if self.symbol_indices is None
                else {"symbol_indices": self._to_device(self.symbol_indices, device, non_blocking, prepare_timing)}
            ),
            "future_log_returns": self._to_device(future_log_returns, device, non_blocking, prepare_timing),
            **(
                {}
                if volume_notional is None
                else {"volume_notional": self._to_device(volume_notional, device, non_blocking, prepare_timing)}
            ),
            "tradable_mask": self._to_device(tradable_mask, device, non_blocking, prepare_timing),
            "can_buy_mask": self._to_device(can_buy_mask, device, non_blocking, prepare_timing),
            "can_sell_mask": self._to_device(can_sell_mask, device, non_blocking, prepare_timing),
            "can_short_open_mask": self._to_device(can_short_open_mask, device, non_blocking, prepare_timing),
            "short_capacity_shares": self._to_device(
                short_capacity_shares, device, non_blocking, prepare_timing
            ),
            "short_margin_rate": self._to_device(
                short_margin_rate, device, non_blocking, prepare_timing
            ),
            "short_capacity_notional": self._to_device(
                short_capacity_notional,
                device,
                non_blocking,
                prepare_timing,
            ),
            "force_short_cover_mask": self._to_device(force_short_cover_mask, device, non_blocking, prepare_timing),
            "force_exit_mask": self._to_device(force_exit_mask, device, non_blocking, prepare_timing),
            "benchmark": self._to_device(benchmark, device, non_blocking, prepare_timing),
            "session_advance_mask": self._to_device(
                session_advance_mask, device, non_blocking, prepare_timing
            ),
            **(
                {}
                if day_trade_eligible_mask is None
                else {
                    "day_trade_eligible_mask": self._to_device(
                        day_trade_eligible_mask,
                        device,
                        non_blocking,
                        prepare_timing,
                    )
                }
            ),
            **self._day_trade_open_mask_items(
                day_trade_can_buy_open_mask,
                day_trade_can_sell_open_mask,
                device,
                non_blocking,
                prepare_timing,
            ),
            **self._corporate_action_mask_items(
                unresolved_corporate_action_mask,
                cash_dividend_yield,
                cash_dividend_payment_delay_sessions,
                device,
                non_blocking,
                prepare_timing,
            ),
            "sample_mask": self._to_device(sample_mask, device, non_blocking, prepare_timing),
        }

    def _batch_metadata_from_row_range(
        self,
        start: int,
        end: int,
        device: torch.device,
        non_blocking: bool,
        prepare_timing: Any | None = None,
    ) -> dict[str, torch.Tensor]:
        rows = int(end) - int(start)
        if rows < 0:
            raise ValueError("end must be >= start")
        timer = self._prepare_timer_start()
        date_idx_control = self._valid_indices_cpu.narrow(0, int(start), rows)
        date_idx = self.valid_indices.narrow(0, int(start), rows)
        if rows <= 1:
            rows_are_contiguous = torch.ones((), dtype=torch.bool)
        else:
            rows_are_contiguous = torch.all((date_idx_control[1:] - date_idx_control[:-1]) == 1)
        self._prepare_timer_stop(prepare_timing, "index_build", timer)
        timer = self._prepare_timer_start()
        sample_mask = self._sample_mask_slice(int(start), rows)
        (
            session_advance_mask,
            day_trade_eligible_mask,
            day_trade_can_buy_open_mask,
            day_trade_can_sell_open_mask,
            unresolved_corporate_action_mask,
            cash_dividend_yield,
            cash_dividend_payment_delay_sessions,
        ) = self._execution_masks_for_date_indices(date_idx, sample_mask)
        tradable_mask = self.tradable_mask[date_idx]
        can_buy_mask = self.can_buy_mask[date_idx]
        can_sell_mask = self.can_sell_mask[date_idx]
        can_short_open_mask = self.can_short_open_mask[date_idx]
        (
            short_capacity_shares,
            short_margin_rate,
            short_capacity_notional,
        ) = (
            self._short_margin_for_date_indices(date_idx, sample_mask)
        )
        force_short_cover_mask = self.force_short_cover_mask[date_idx]
        force_exit_mask = self.force_exit_mask[date_idx]
        self._prepare_timer_stop(prepare_timing, "mask_build", timer)
        timer = self._prepare_timer_start()
        future_log_returns = self.future_log_returns[date_idx]
        benchmark = self.benchmark[date_idx]
        volume_notional = None if self.volume_notional is None else self.volume_notional[date_idx]
        self._prepare_timer_stop(prepare_timing, "target_gather", timer)
        return {
            "date_indices": self._to_device(date_idx, device, non_blocking, prepare_timing),
            "date_start": date_idx_control[:1].detach().clone(),
            "rows_are_contiguous": rows_are_contiguous.detach().clone(),
            **(
                {}
                if self.symbol_indices is None
                else {"symbol_indices": self._to_device(self.symbol_indices, device, non_blocking, prepare_timing)}
            ),
            "future_log_returns": self._to_device(future_log_returns, device, non_blocking, prepare_timing),
            **(
                {}
                if volume_notional is None
                else {"volume_notional": self._to_device(volume_notional, device, non_blocking, prepare_timing)}
            ),
            "tradable_mask": self._to_device(tradable_mask, device, non_blocking, prepare_timing),
            "can_buy_mask": self._to_device(can_buy_mask, device, non_blocking, prepare_timing),
            "can_sell_mask": self._to_device(can_sell_mask, device, non_blocking, prepare_timing),
            "can_short_open_mask": self._to_device(can_short_open_mask, device, non_blocking, prepare_timing),
            "short_capacity_shares": self._to_device(
                short_capacity_shares, device, non_blocking, prepare_timing
            ),
            "short_margin_rate": self._to_device(
                short_margin_rate, device, non_blocking, prepare_timing
            ),
            "short_capacity_notional": self._to_device(
                short_capacity_notional,
                device,
                non_blocking,
                prepare_timing,
            ),
            "force_short_cover_mask": self._to_device(force_short_cover_mask, device, non_blocking, prepare_timing),
            "force_exit_mask": self._to_device(force_exit_mask, device, non_blocking, prepare_timing),
            "benchmark": self._to_device(benchmark, device, non_blocking, prepare_timing),
            "session_advance_mask": self._to_device(
                session_advance_mask, device, non_blocking, prepare_timing
            ),
            **(
                {}
                if day_trade_eligible_mask is None
                else {
                    "day_trade_eligible_mask": self._to_device(
                        day_trade_eligible_mask,
                        device,
                        non_blocking,
                        prepare_timing,
                    )
                }
            ),
            **self._day_trade_open_mask_items(
                day_trade_can_buy_open_mask,
                day_trade_can_sell_open_mask,
                device,
                non_blocking,
                prepare_timing,
            ),
            **self._corporate_action_mask_items(
                unresolved_corporate_action_mask,
                cash_dividend_yield,
                cash_dividend_payment_delay_sessions,
                device,
                non_blocking,
                prepare_timing,
            ),
            "sample_mask": self._to_device(sample_mask, device, non_blocking, prepare_timing),
        }

    def _batch_from_contiguous_rows(
        self,
        start: int,
        end: int,
        device: torch.device,
        non_blocking: bool,
        *,
        contiguous_x: bool = True,
        prepare_timing: Any | None = None,
    ) -> dict[str, torch.Tensor]:
        x, date_start, sample_mask = self._window_view_for_contiguous_rows(
            start,
            end,
            contiguous_x=contiguous_x,
            prepare_timing=prepare_timing,
        )
        rows = int(end) - int(start)
        timer = self._prepare_timer_start()
        (
            session_advance_mask,
            day_trade_eligible_mask,
            day_trade_can_buy_open_mask,
            day_trade_can_sell_open_mask,
            unresolved_corporate_action_mask,
            cash_dividend_yield,
            cash_dividend_payment_delay_sessions,
        ) = self._execution_masks_for_date_range(date_start, rows, sample_mask)
        future_log_returns = self.future_log_returns.narrow(0, date_start, rows)
        benchmark = self.benchmark.narrow(0, date_start, rows)
        volume_notional = None if self.volume_notional is None else self.volume_notional.narrow(0, date_start, rows)
        self._prepare_timer_stop(prepare_timing, "target_gather", timer)
        timer = self._prepare_timer_start()
        tradable_mask = self.tradable_mask.narrow(0, date_start, rows)
        can_buy_mask = self.can_buy_mask.narrow(0, date_start, rows)
        can_sell_mask = self.can_sell_mask.narrow(0, date_start, rows)
        can_short_open_mask = self.can_short_open_mask.narrow(0, date_start, rows)
        (
            short_capacity_shares,
            short_margin_rate,
            short_capacity_notional,
        ) = (
            self._short_margin_for_date_range(date_start, rows, sample_mask)
        )
        force_short_cover_mask = self.force_short_cover_mask.narrow(0, date_start, rows)
        force_exit_mask = self.force_exit_mask.narrow(0, date_start, rows)
        self._prepare_timer_stop(prepare_timing, "mask_build", timer)
        return {
            "x": self._to_device(x, device, non_blocking, prepare_timing),
            **(
                {}
                if self.symbol_indices is None
                else {"symbol_indices": self._to_device(self.symbol_indices, device, non_blocking, prepare_timing)}
            ),
            "future_log_returns": self._to_device(future_log_returns, device, non_blocking, prepare_timing),
            **(
                {}
                if volume_notional is None
                else {"volume_notional": self._to_device(volume_notional, device, non_blocking, prepare_timing)}
            ),
            "tradable_mask": self._to_device(tradable_mask, device, non_blocking, prepare_timing),
            "can_buy_mask": self._to_device(can_buy_mask, device, non_blocking, prepare_timing),
            "can_sell_mask": self._to_device(can_sell_mask, device, non_blocking, prepare_timing),
            "can_short_open_mask": self._to_device(can_short_open_mask, device, non_blocking, prepare_timing),
            "short_capacity_shares": self._to_device(
                short_capacity_shares, device, non_blocking, prepare_timing
            ),
            "short_margin_rate": self._to_device(
                short_margin_rate, device, non_blocking, prepare_timing
            ),
            "short_capacity_notional": self._to_device(
                short_capacity_notional,
                device,
                non_blocking,
                prepare_timing,
            ),
            "force_short_cover_mask": self._to_device(force_short_cover_mask, device, non_blocking, prepare_timing),
            "force_exit_mask": self._to_device(force_exit_mask, device, non_blocking, prepare_timing),
            "benchmark": self._to_device(benchmark, device, non_blocking, prepare_timing),
            "session_advance_mask": self._to_device(
                session_advance_mask, device, non_blocking, prepare_timing
            ),
            **(
                {}
                if day_trade_eligible_mask is None
                else {
                    "day_trade_eligible_mask": self._to_device(
                        day_trade_eligible_mask,
                        device,
                        non_blocking,
                        prepare_timing,
                    )
                }
            ),
            **self._day_trade_open_mask_items(
                day_trade_can_buy_open_mask,
                day_trade_can_sell_open_mask,
                device,
                non_blocking,
                prepare_timing,
            ),
            **self._corporate_action_mask_items(
                unresolved_corporate_action_mask,
                cash_dividend_yield,
                cash_dividend_payment_delay_sessions,
                device,
                non_blocking,
                prepare_timing,
            ),
            "sample_mask": self._to_device(sample_mask, device, non_blocking, prepare_timing),
        }

    def _batch_metadata_from_contiguous_rows(
        self,
        start: int,
        end: int,
        device: torch.device,
        non_blocking: bool,
        prepare_timing: Any | None = None,
    ) -> dict[str, torch.Tensor]:
        rows = int(end) - int(start)
        date_start = self._first_valid_index + int(start)
        if rows < 0:
            raise ValueError("end must be >= start")
        timer = self._prepare_timer_start()
        sample_mask = self._sample_mask_slice(int(start), rows)
        self._prepare_timer_stop(prepare_timing, "mask_build", timer)
        timer = self._prepare_timer_start()
        date_indices = self.valid_indices.narrow(0, int(start), rows)
        date_start_control = (
            torch.empty((0,), dtype=torch.long)
            if rows <= 0
            else torch.tensor([date_start], dtype=torch.long)
        )
        self._prepare_timer_stop(prepare_timing, "index_build", timer)
        timer = self._prepare_timer_start()
        (
            session_advance_mask,
            day_trade_eligible_mask,
            day_trade_can_buy_open_mask,
            day_trade_can_sell_open_mask,
            unresolved_corporate_action_mask,
            cash_dividend_yield,
            cash_dividend_payment_delay_sessions,
        ) = self._execution_masks_for_date_range(date_start, rows, sample_mask)
        future_log_returns = self.future_log_returns.narrow(0, date_start, rows)
        benchmark = self.benchmark.narrow(0, date_start, rows)
        volume_notional = None if self.volume_notional is None else self.volume_notional.narrow(0, date_start, rows)
        self._prepare_timer_stop(prepare_timing, "target_gather", timer)
        timer = self._prepare_timer_start()
        tradable_mask = self.tradable_mask.narrow(0, date_start, rows)
        can_buy_mask = self.can_buy_mask.narrow(0, date_start, rows)
        can_sell_mask = self.can_sell_mask.narrow(0, date_start, rows)
        can_short_open_mask = self.can_short_open_mask.narrow(0, date_start, rows)
        (
            short_capacity_shares,
            short_margin_rate,
            short_capacity_notional,
        ) = (
            self._short_margin_for_date_range(date_start, rows, sample_mask)
        )
        force_short_cover_mask = self.force_short_cover_mask.narrow(0, date_start, rows)
        force_exit_mask = self.force_exit_mask.narrow(0, date_start, rows)
        self._prepare_timer_stop(prepare_timing, "mask_build", timer)
        return {
            "date_indices": self._to_device(date_indices, device, non_blocking, prepare_timing),
            "date_start": date_start_control,
            "rows_are_contiguous": torch.ones((), dtype=torch.bool),
            **(
                {}
                if self.symbol_indices is None
                else {"symbol_indices": self._to_device(self.symbol_indices, device, non_blocking, prepare_timing)}
            ),
            "future_log_returns": self._to_device(future_log_returns, device, non_blocking, prepare_timing),
            **(
                {}
                if volume_notional is None
                else {"volume_notional": self._to_device(volume_notional, device, non_blocking, prepare_timing)}
            ),
            "tradable_mask": self._to_device(tradable_mask, device, non_blocking, prepare_timing),
            "can_buy_mask": self._to_device(can_buy_mask, device, non_blocking, prepare_timing),
            "can_sell_mask": self._to_device(can_sell_mask, device, non_blocking, prepare_timing),
            "can_short_open_mask": self._to_device(can_short_open_mask, device, non_blocking, prepare_timing),
            "short_capacity_shares": self._to_device(
                short_capacity_shares, device, non_blocking, prepare_timing
            ),
            "short_margin_rate": self._to_device(
                short_margin_rate, device, non_blocking, prepare_timing
            ),
            "short_capacity_notional": self._to_device(
                short_capacity_notional,
                device,
                non_blocking,
                prepare_timing,
            ),
            "force_short_cover_mask": self._to_device(force_short_cover_mask, device, non_blocking, prepare_timing),
            "force_exit_mask": self._to_device(force_exit_mask, device, non_blocking, prepare_timing),
            "benchmark": self._to_device(benchmark, device, non_blocking, prepare_timing),
            "session_advance_mask": self._to_device(
                session_advance_mask, device, non_blocking, prepare_timing
            ),
            **(
                {}
                if day_trade_eligible_mask is None
                else {
                    "day_trade_eligible_mask": self._to_device(
                        day_trade_eligible_mask,
                        device,
                        non_blocking,
                        prepare_timing,
                    )
                }
            ),
            **self._day_trade_open_mask_items(
                day_trade_can_buy_open_mask,
                day_trade_can_sell_open_mask,
                device,
                non_blocking,
                prepare_timing,
            ),
            **self._corporate_action_mask_items(
                unresolved_corporate_action_mask,
                cash_dividend_yield,
                cash_dividend_payment_delay_sessions,
                device,
                non_blocking,
                prepare_timing,
            ),
            "sample_mask": self._to_device(sample_mask, device, non_blocking, prepare_timing),
        }

    def _panel_slab_batch_from_contiguous_rows(
        self,
        start: int,
        end: int,
        device: torch.device,
        non_blocking: bool,
        prepare_timing: Any | None = None,
    ) -> dict[str, torch.Tensor] | None:
        rows = int(end) - int(start)
        if rows < 0:
            raise ValueError("end must be >= start")
        if rows <= 0:
            return None
        date_start = self._first_valid_index + int(start)
        feature_start = date_start - self._feature_lag - self.lookback + 1
        slab_rows = rows + self.lookback - 1
        if feature_start < 0 or feature_start + slab_rows > int(self.features.size(0)):
            return None
        timer = self._prepare_timer_start()
        sample_mask = self._sample_mask_slice(int(start), rows)
        self._prepare_timer_stop(prepare_timing, "mask_build", timer)
        timer = self._prepare_timer_start()
        feature_slab = self.features.narrow(0, feature_start, slab_rows)
        self._prepare_timer_stop(prepare_timing, "window_slice", timer)
        timer = self._prepare_timer_start()
        (
            session_advance_mask,
            day_trade_eligible_mask,
            day_trade_can_buy_open_mask,
            day_trade_can_sell_open_mask,
            unresolved_corporate_action_mask,
            cash_dividend_yield,
            cash_dividend_payment_delay_sessions,
        ) = self._execution_masks_for_date_range(date_start, rows, sample_mask)
        future_log_returns = self.future_log_returns.narrow(0, date_start, rows)
        benchmark = self.benchmark.narrow(0, date_start, rows)
        volume_notional = None if self.volume_notional is None else self.volume_notional.narrow(0, date_start, rows)
        self._prepare_timer_stop(prepare_timing, "target_gather", timer)
        timer = self._prepare_timer_start()
        tradable_mask = self.tradable_mask.narrow(0, date_start, rows)
        can_buy_mask = self.can_buy_mask.narrow(0, date_start, rows)
        can_sell_mask = self.can_sell_mask.narrow(0, date_start, rows)
        can_short_open_mask = self.can_short_open_mask.narrow(0, date_start, rows)
        (
            short_capacity_shares,
            short_margin_rate,
            short_capacity_notional,
        ) = (
            self._short_margin_for_date_range(date_start, rows, sample_mask)
        )
        force_short_cover_mask = self.force_short_cover_mask.narrow(0, date_start, rows)
        force_exit_mask = self.force_exit_mask.narrow(0, date_start, rows)
        self._prepare_timer_stop(prepare_timing, "mask_build", timer)
        return {
            "feature_slab": self._to_device(feature_slab, device, non_blocking, prepare_timing),
            **(
                {}
                if self.symbol_indices is None
                else {"symbol_indices": self._to_device(self.symbol_indices, device, non_blocking, prepare_timing)}
            ),
            "future_log_returns": self._to_device(future_log_returns, device, non_blocking, prepare_timing),
            **(
                {}
                if volume_notional is None
                else {"volume_notional": self._to_device(volume_notional, device, non_blocking, prepare_timing)}
            ),
            "tradable_mask": self._to_device(tradable_mask, device, non_blocking, prepare_timing),
            "can_buy_mask": self._to_device(can_buy_mask, device, non_blocking, prepare_timing),
            "can_sell_mask": self._to_device(can_sell_mask, device, non_blocking, prepare_timing),
            "can_short_open_mask": self._to_device(can_short_open_mask, device, non_blocking, prepare_timing),
            "short_capacity_shares": self._to_device(
                short_capacity_shares, device, non_blocking, prepare_timing
            ),
            "short_margin_rate": self._to_device(
                short_margin_rate, device, non_blocking, prepare_timing
            ),
            "short_capacity_notional": self._to_device(
                short_capacity_notional,
                device,
                non_blocking,
                prepare_timing,
            ),
            "force_short_cover_mask": self._to_device(force_short_cover_mask, device, non_blocking, prepare_timing),
            "force_exit_mask": self._to_device(force_exit_mask, device, non_blocking, prepare_timing),
            "benchmark": self._to_device(benchmark, device, non_blocking, prepare_timing),
            "session_advance_mask": self._to_device(
                session_advance_mask, device, non_blocking, prepare_timing
            ),
            **(
                {}
                if day_trade_eligible_mask is None
                else {
                    "day_trade_eligible_mask": self._to_device(
                        day_trade_eligible_mask,
                        device,
                        non_blocking,
                        prepare_timing,
                    )
                }
            ),
            **self._day_trade_open_mask_items(
                day_trade_can_buy_open_mask,
                day_trade_can_sell_open_mask,
                device,
                non_blocking,
                prepare_timing,
            ),
            **self._corporate_action_mask_items(
                unresolved_corporate_action_mask,
                cash_dividend_yield,
                cash_dividend_payment_delay_sessions,
                device,
                non_blocking,
                prepare_timing,
            ),
            "sample_mask": self._to_device(sample_mask, device, non_blocking, prepare_timing),
        }

    def _panel_slab_batch_from_padded_tail(
        self,
        start: int,
        end: int,
        device: torch.device,
        non_blocking: bool,
        prepare_timing: Any | None = None,
    ) -> dict[str, torch.Tensor] | None:
        """Build one fixed-shape slab for the final sample-masked train batch.

        The leading rows are the original contiguous observations.  Synthetic
        feature rows only produce outputs whose sample mask is false, so they
        cannot affect the valid loss or recurrent state consumed by another
        batch.  Keeping the slab shape fixed avoids a second generic gather
        graph solely for the padded epoch tail.
        """
        rows = int(end) - int(start)
        real_rows = self._contiguous_prefix_len - int(start)
        if (
            rows <= 0
            or real_rows <= 0
            or real_rows >= rows
            or self.sample_mask is None
            or int(end) > len(self)
        ):
            return None
        sample_mask_cpu = self.sample_mask[int(start) : int(end)].detach().to(device="cpu", dtype=torch.bool)
        if not bool(sample_mask_cpu[:real_rows].all()) or bool(sample_mask_cpu[real_rows:].any()):
            return None

        date_start = self._first_valid_index + int(start)
        feature_start = date_start - self._feature_lag - self.lookback + 1
        source_rows = real_rows + self.lookback - 1
        if feature_start < 0 or feature_start + source_rows > int(self.features.size(0)):
            return None

        timer = self._prepare_timer_start()
        real_slab = self.features.narrow(0, feature_start, source_rows)
        pad_rows = rows - real_rows
        feature_slab = torch.cat(
            (real_slab, real_slab[-1:].expand(pad_rows, *real_slab.shape[1:])),
            dim=0,
        ).contiguous()
        self._prepare_timer_stop(prepare_timing, "window_slice", timer)

        metadata = self._batch_metadata_from_row_range(
            start,
            end,
            device,
            non_blocking,
            prepare_timing=prepare_timing,
        )
        metadata.pop("date_indices", None)
        metadata.pop("date_start", None)
        metadata.pop("rows_are_contiguous", None)
        return {
            "feature_slab": self._to_device(feature_slab, device, non_blocking, prepare_timing),
            **metadata,
        }

    def _panel_slab_batch_from_padded_rows(
        self,
        start: int,
        end: int,
        device: torch.device,
        non_blocking: bool,
        prepare_timing: Any | None = None,
    ) -> dict[str, torch.Tensor] | None:
        """Build a fixed-shape harmless slab for a DDP shard containing only padding."""
        rows = int(end) - int(start)
        if rows <= 0 or self.sample_mask is None or int(end) > len(self):
            return None
        sample_mask_cpu = self.sample_mask[int(start) : int(end)].detach().to(device="cpu", dtype=torch.bool)
        if bool(sample_mask_cpu.any()):
            return None

        last_date = int(self._valid_indices_cpu[self._contiguous_prefix_len - 1].item())
        last_feature_date = last_date - self._feature_lag
        if last_feature_date < 0 or last_feature_date >= int(self.features.size(0)):
            return None
        timer = self._prepare_timer_start()
        feature_slab = self.features[last_feature_date : last_feature_date + 1].expand(
            rows + self.lookback - 1,
            *self.features.shape[1:],
        ).contiguous()
        self._prepare_timer_stop(prepare_timing, "window_slice", timer)

        metadata = self._batch_metadata_from_row_range(
            start,
            end,
            device,
            non_blocking,
            prepare_timing=prepare_timing,
        )
        metadata.pop("date_indices", None)
        metadata.pop("date_start", None)
        metadata.pop("rows_are_contiguous", None)
        return {
            "feature_slab": self._to_device(feature_slab, device, non_blocking, prepare_timing),
            **metadata,
        }

    def batch_by_rows(
        self,
        start: int,
        end: int,
        device: torch.device,
        non_blocking: bool,
        prepare_timing: Any | None = None,
    ) -> dict[str, torch.Tensor]:
        if end < start:
            raise ValueError("end must be >= start")
        if int(start) >= 0 and int(end) <= self._contiguous_prefix_len:
            return self._batch_from_contiguous_rows(start, end, device, non_blocking, prepare_timing=prepare_timing)
        timer = self._prepare_timer_start()
        rows = torch.arange(int(start), int(end), dtype=torch.long, device=self.valid_indices.device)
        self._prepare_timer_stop(prepare_timing, "index_build", timer)
        return self._batch_from_row_indices(rows, device, non_blocking, prepare_timing=prepare_timing)

    def batch_metadata_by_rows(
        self,
        start: int,
        end: int,
        device: torch.device,
        non_blocking: bool,
        prepare_timing: Any | None = None,
    ) -> dict[str, torch.Tensor]:
        if end < start:
            raise ValueError("end must be >= start")
        if int(start) >= 0 and int(end) <= self._contiguous_prefix_len:
            return self._batch_metadata_from_contiguous_rows(start, end, device, non_blocking, prepare_timing=prepare_timing)
        return self._batch_metadata_from_row_range(start, end, device, non_blocking, prepare_timing=prepare_timing)

    def panel_slab_batch_by_rows(
        self,
        start: int,
        end: int,
        device: torch.device,
        non_blocking: bool,
        prepare_timing: Any | None = None,
    ) -> dict[str, torch.Tensor] | None:
        if end < start:
            raise ValueError("end must be >= start")
        if int(start) >= 0 and int(end) <= self._contiguous_prefix_len:
            return self._panel_slab_batch_from_contiguous_rows(start, end, device, non_blocking, prepare_timing=prepare_timing)
        if int(start) < self._contiguous_prefix_len < int(end):
            return self._panel_slab_batch_from_padded_tail(
                start,
                end,
                device,
                non_blocking,
                prepare_timing=prepare_timing,
            )
        if int(start) >= self._contiguous_prefix_len:
            return self._panel_slab_batch_from_padded_rows(
                start,
                end,
                device,
                non_blocking,
                prepare_timing=prepare_timing,
            )
        return None

    def batch_by_batch_indices(
        self,
        batch_indices: torch.Tensor,
        device: torch.device,
        non_blocking: bool,
        prepare_timing: Any | None = None,
    ) -> dict[str, torch.Tensor]:
        return self._batch_from_row_indices(batch_indices.reshape(-1), device, non_blocking, prepare_timing=prepare_timing)

    def batch_metadata_by_batch_indices(
        self,
        batch_indices: torch.Tensor,
        device: torch.device,
        non_blocking: bool,
        prepare_timing: Any | None = None,
    ) -> dict[str, torch.Tensor]:
        return self._batch_metadata_from_row_indices(batch_indices.reshape(-1), device, non_blocking, prepare_timing=prepare_timing)

    def materialize_windows(
        self,
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
        if len(self) == 0:
            empty_x = self.features.new_empty((0, self.lookback, self.features.size(1), self.features.size(2)))
            empty_2d = self.future_log_returns.new_empty((0, self.features.size(1)))
            empty_mask = self.tradable_mask.new_empty((0, self.features.size(1)))
            empty_bench = self.benchmark.new_empty((0,))
            return empty_x, empty_2d, empty_mask, empty_mask.clone(), empty_mask.clone(), empty_bench

        row_indices = torch.arange(len(self), dtype=torch.long, device=self.valid_indices.device)
        date_idx = self.valid_indices[row_indices]
        if self._valid_indices_are_contiguous:
            x, _, _ = self._window_view_for_contiguous_rows(0, len(self), contiguous_x=True)
        else:
            window_idx = self._window_indices_for_rows(row_indices)
            x = self.features[window_idx].contiguous()
        return (
            x,
            self.future_log_returns[date_idx],
            self.tradable_mask[date_idx],
            self.can_buy_mask[date_idx],
            self.can_sell_mask[date_idx],
            self.benchmark[date_idx],
        )


def dataset_to_windowed_tensors(dataset: CrossSectionalDataset) -> WindowedSplitTensors:
    return WindowedSplitTensors(
        features=dataset.features_t,
        valid_indices=torch.as_tensor(dataset.valid_indices, dtype=torch.long),
        future_log_returns=dataset.future_log_returns_t,
        tradable_mask=dataset.tradable_mask_t,
        can_buy_mask=dataset.can_buy_mask_t,
        can_sell_mask=dataset.can_sell_mask_t,
        benchmark=dataset.benchmark_t,
        lookback=dataset.lookback,
        volume_notional=dataset.volume_notional_t,
        can_short_open_mask=dataset.can_short_open_mask_t,
        short_capacity_shares=dataset.short_capacity_shares_t,
        short_margin_rate=dataset.short_margin_rate_t,
        short_capacity_notional=dataset.short_capacity_notional_t,
        force_short_cover_mask=dataset.force_short_cover_mask_t,
        force_exit_mask=dataset.force_exit_mask_t,
        execution_mode=dataset.execution_mode,
        session_advance_mask=dataset.session_advance_mask_t,
        day_trade_eligible_mask=dataset.day_trade_eligible_mask_t,
        day_trade_can_buy_open_mask=dataset.day_trade_can_buy_open_mask_t,
        day_trade_can_sell_open_mask=dataset.day_trade_can_sell_open_mask_t,
        unresolved_corporate_action_mask=dataset.unresolved_corporate_action_mask_t,
        cash_dividend_yield=dataset.cash_dividend_yield_t,
        cash_dividend_payment_delay_sessions=(
            dataset.cash_dividend_payment_delay_sessions_t
        ),
    )
