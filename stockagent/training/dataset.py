from __future__ import annotations

import numpy as np
import torch
from torch.utils.data import Dataset

from stockagent.backtest.tw_execution import (
    normalize_execution_mode,
    official_tw_short_initial_margin_rates,
)
from stockagent.data.panel import PanelData


def execution_feature_lag(execution_mode: str) -> int:
    """Return the number of complete sessions available before execution.

    ``naive`` preserves the historical same-row tensor contract.  Both Taiwan
    execution modes submit a signal for session ``t`` using information through
    ``t-1``: cash orders execute in the session-t closing auction and day trades
    enter at the session-t open.
    """

    return 0 if normalize_execution_mode(execution_mode) == "naive" else 1


class CrossSectionalDataset(Dataset[dict[str, torch.Tensor]]):
    def __init__(
        self,
        panel: PanelData,
        date_indices: np.ndarray,
        lookback: int,
        *,
        allow_empty: bool = False,
        include_volume_notional: bool = True,
        execution_mode: str = "naive",
        short_capacity_limit_enabled: bool = True,
        tw_corporate_action_mode: str = "avoid",
    ) -> None:
        self.lookback = int(lookback)
        self.execution_mode = normalize_execution_mode(execution_mode)
        self.short_capacity_limit_enabled = bool(short_capacity_limit_enabled)
        self.tw_corporate_action_mode = str(tw_corporate_action_mode).strip().lower()
        if self.tw_corporate_action_mode not in {"avoid", "exact"}:
            raise ValueError("tw_corporate_action_mode must be 'avoid' or 'exact'")
        self.date_indices = np.array(sorted(np.asarray(date_indices, dtype=np.int64).tolist()), dtype=np.int64)
        day_trade_eligible: np.ndarray | None = None
        day_trade_short_open: np.ndarray | None = None
        day_trade_can_buy_open: np.ndarray | None = None
        day_trade_can_sell_open: np.ndarray | None = None
        unresolved_corporate_action = (
            None
            if panel.unresolved_corporate_action_mask is None
            else np.asarray(panel.unresolved_corporate_action_mask, dtype=bool)
        )
        all_corporate_action_avoidance = (
            None
            if getattr(panel, "corporate_action_avoidance_mask", None) is None
            else np.asarray(panel.corporate_action_avoidance_mask, dtype=bool)
        )
        cash_dividend_yield = getattr(panel, "cash_dividend_yield", None)
        cash_dividend_delay = getattr(
            panel, "cash_dividend_payment_delay_sessions", None
        )
        if (cash_dividend_yield is None) != (cash_dividend_delay is None):
            raise ValueError(
                "PanelData exact cash-dividend amount and payment tensors must "
                "be supplied together"
            )
        if cash_dividend_yield is not None:
            cash_dividend_yield = np.asarray(cash_dividend_yield, dtype=np.float32)
            cash_dividend_delay = np.asarray(cash_dividend_delay, dtype=np.int64)
            if (
                cash_dividend_yield.shape != panel.tradable_mask.shape
                or cash_dividend_delay.shape != panel.tradable_mask.shape
            ):
                raise ValueError(
                    "PanelData exact cash-dividend tensors must match tradable_mask"
                )
        if self.execution_mode == "tw_cash" and self.tw_corporate_action_mode == "exact":
            if cash_dividend_yield is None:
                raise ValueError(
                    "exact tw_cash requires a receipt-verified MOPS cash-dividend "
                    "amount/payment archive"
                )
        elif self.execution_mode == "tw_cash":
            # Avoid mode needs the full interval beginning at the last close
            # where both a long and short can be flattened. A single exact
            # yield cell at the last cum-right close is insufficient when that
            # close is limit-blocked or halted.
            if all_corporate_action_avoidance is not None:
                unresolved_corporate_action = all_corporate_action_avoidance
            elif cash_dividend_yield is not None and bool(
                (cash_dividend_yield > 0.0).any()
            ):
                raise ValueError(
                    "avoid tw_cash requires PanelData.corporate_action_avoidance_mask "
                    "when exact entitlement events are present"
                )
            cash_dividend_yield = None
            cash_dividend_delay = None
        if self.execution_mode == "tw_cash" and unresolved_corporate_action is None:
            raise ValueError(
                "tw_cash requires PanelData.unresolved_corporate_action_mask; "
                "cash positions require receipt-verified official ex-date "
                "avoidance transitions"
            )
        if (
            unresolved_corporate_action is not None
            and unresolved_corporate_action.shape != panel.tradable_mask.shape
        ):
            raise ValueError(
                "PanelData.unresolved_corporate_action_mask must match "
                "tradable_mask shape"
            )
        if (
            all_corporate_action_avoidance is not None
            and all_corporate_action_avoidance.shape != panel.tradable_mask.shape
        ):
            raise ValueError(
                "PanelData.corporate_action_avoidance_mask must match "
                "tradable_mask shape"
            )
        if self.execution_mode == "tw_day_trade":
            if panel.intraday_returns is None:
                raise ValueError(
                    "tw_day_trade requires PanelData.intraday_returns; refusing to "
                    "substitute close-to-close returns"
                )
            if panel.day_trade_eligible_mask is None:
                raise ValueError(
                    "tw_day_trade requires a point-in-time "
                    "PanelData.day_trade_eligible_mask; current eligibility must "
                    "not be projected backward"
                )
            if (
                panel.day_trade_can_buy_open_mask is None
                or panel.day_trade_can_sell_open_mask is None
            ):
                raise ValueError(
                    "tw_day_trade requires point-in-time open-session buy/sell "
                    "masks; close-session masks must not be reused for the open"
                )
            target_returns = np.asarray(panel.intraday_returns)
            day_trade_eligible = np.asarray(
                panel.day_trade_eligible_mask, dtype=bool
            )
            day_trade_short_open = (
                np.asarray(panel.day_trade_can_short_open_mask, dtype=bool)
                if panel.day_trade_can_short_open_mask is not None
                else np.zeros_like(day_trade_eligible, dtype=bool)
            )
            day_trade_can_buy_open = np.asarray(
                panel.day_trade_can_buy_open_mask, dtype=bool
            )
            day_trade_can_sell_open = np.asarray(
                panel.day_trade_can_sell_open_mask, dtype=bool
            )
            if target_returns.shape != panel.tradable_mask.shape:
                raise ValueError(
                    "PanelData.intraday_returns must match tradable_mask shape"
                )
            if day_trade_eligible.shape != panel.tradable_mask.shape:
                raise ValueError(
                    "PanelData.day_trade_eligible_mask must match tradable_mask shape"
                )
            if day_trade_short_open.shape != panel.tradable_mask.shape:
                raise ValueError(
                    "PanelData.day_trade_can_short_open_mask must match "
                    "tradable_mask shape"
                )
            if day_trade_can_buy_open.shape != panel.tradable_mask.shape:
                raise ValueError(
                    "PanelData.day_trade_can_buy_open_mask must match "
                    "tradable_mask shape"
                )
            if day_trade_can_sell_open.shape != panel.tradable_mask.shape:
                raise ValueError(
                    "PanelData.day_trade_can_sell_open_mask must match "
                    "tradable_mask shape"
                )
        elif self.execution_mode == "tw_cash":
            if panel.raw_close_returns_1d is None:
                raise ValueError(
                    "tw_cash requires PanelData.raw_close_returns_1d; adjusted "
                    "total returns cannot value an exact-share cash ledger"
                )
            target_returns = np.asarray(panel.raw_close_returns_1d)
            if target_returns.shape != panel.tradable_mask.shape:
                raise ValueError(
                    "PanelData.raw_close_returns_1d must match tradable_mask shape"
                )
        else:
            target_returns = panel.returns_1d

        finite_target = np.isfinite(target_returns)
        close_tradable = panel.tradable_mask & finite_target
        if day_trade_eligible is not None:
            close_tradable = close_tradable & day_trade_eligible
            # The model target is committed before the opening auction.  Even
            # today's realized open price/limit state is therefore execution
            # information, not a portfolio-selection mask.  Only the official
            # point-in-time eligibility gate is visible to the model; the
            # signed open-side fill masks are applied later by the executor.
            tradable = day_trade_eligible.copy()
        elif self.execution_mode == "tw_cash":
            # The cash signal is committed before session-t close.  Its outer
            # cross-sectional mask may therefore use only the last completed
            # session's known universe, never whether close[t] traded or the
            # close[t]->close[t+1] label exists.  Current-close side masks below
            # remain executor-only, and an active non-finite valuation is
            # rejected by the recurrent ledger rather than pre-filtered with
            # future information.
            prior_alive = np.zeros_like(panel.alive_mask, dtype=bool)
            prior_alive[1:] = np.asarray(panel.alive_mask[:-1], dtype=bool)
            tradable = prior_alive
            # At close[t], quote/limit availability is observable, while the
            # close[t]->next-session valuation label is not.  Side execution
            # masks must therefore use the raw current-session trading mask,
            # never ``finite_target`` (which is future information).
            close_tradable = np.asarray(panel.tradable_mask, dtype=bool)
        else:
            tradable = close_tradable
        if self.execution_mode == "tw_day_trade":
            # A session-t day trade is held from open[t] to close[t].  Its
            # buy-and-hold comparator must cover the same wall-clock session:
            # adjusted close[t-1] to adjusted close[t].  Panel benchmark labels
            # are forward returns close[t] -> close[t+1], so shift them by one
            # row instead of comparing with a future session or an unrelated
            # cross-sectional average of intraday returns.
            benchmark_values = np.zeros(panel.num_dates, dtype=np.float32)
            if panel.num_dates > 1:
                benchmark_values[1:] = np.nan_to_num(
                    np.asarray(panel.benchmark_returns[:-1], dtype=np.float32),
                    nan=0.0,
                    posinf=0.0,
                    neginf=0.0,
                )
        else:
            benchmark_values = panel.benchmark_returns.astype(
                np.float32, copy=False
            )
        force_exit = (
            panel.force_exit_mask
            if panel.force_exit_mask is not None
            else np.zeros_like(tradable, dtype=bool)
        )
        if self.date_indices.size == 0:
            valid_indices = self.date_indices
            if not allow_empty:
                raise ValueError("Fold has no dates after split filtering.")
        else:
            # Keep only indices that have a full lookback window inside this fold.
            fold_start_idx = int(self.date_indices[0])
            # Real execution for session t may observe only through t-1. Naive
            # preserves the historical same-row feature contract.
            feature_lag = execution_feature_lag(self.execution_mode)
            min_valid_idx = fold_start_idx + self.lookback - 1 + feature_lag
            valid_indices = self.date_indices[self.date_indices >= min_valid_idx]
            if valid_indices.size > 0 and self.execution_mode == "naive":
                executable_or_terminal = (
                    tradable[valid_indices].any(axis=1)
                    | force_exit[valid_indices].any(axis=1)
                )
                valid_indices = valid_indices[executable_or_terminal]
        self.valid_indices = valid_indices

        if len(self.valid_indices) == 0 and not allow_empty:
            raise ValueError(
                f"Fold has insufficient data for lookback={self.lookback}. Need at least {self.lookback} dates."
            )

        if self.execution_mode == "naive":
            returns = np.nan_to_num(
                target_returns,
                nan=0.0,
                posinf=0.0,
                neginf=0.0,
                copy=True,
            ).astype(np.float32, copy=False)
        else:
            # The Taiwan ledgers sanitize only inactive cells internally and
            # fail closed when a held/executed asset has no finite valuation.
            # Replacing every missing label with zero here would fabricate a
            # flat return for an existing cash holding and defeat that check.
            returns = np.asarray(target_returns, dtype=np.float32)
        if panel.can_buy_mask is None or panel.can_sell_mask is None:
            raise ValueError(
                "PanelData must provide can_buy_mask and can_sell_mask; no-fallback dataset path "
                "does not infer side masks from tradable_mask"
            )
        can_buy = np.asarray(panel.can_buy_mask, dtype=bool) & close_tradable
        can_sell = np.asarray(panel.can_sell_mask, dtype=bool) & close_tradable
        raw_short_capacity = getattr(panel, "short_capacity_shares", None)
        if raw_short_capacity is None:
            short_capacity_shares: np.ndarray | None = None
        else:
            raw_capacity_values = np.asarray(raw_short_capacity)
            if raw_capacity_values.shape != panel.tradable_mask.shape:
                raise ValueError(
                    "PanelData.short_capacity_shares must match tradable_mask shape"
                )
            if raw_capacity_values.dtype.kind in {"i", "u"}:
                if (
                    raw_capacity_values.dtype.kind == "i"
                    and bool((raw_capacity_values < 0).any())
                ) or (
                    raw_capacity_values.dtype.kind == "u"
                    and raw_capacity_values.size > 0
                    and int(raw_capacity_values.max()) > np.iinfo(np.int64).max
                ):
                    raise ValueError(
                        "PanelData.short_capacity_shares must contain non-negative "
                        "integer shares or NaN for missing evidence"
                    )
                # Keep the official integer oracle exact.  Converting int64
                # shares through float64 first would silently round values
                # above 2**53 before the notional multiplication.
                short_capacity_shares = raw_capacity_values.astype(
                    np.int64,
                    copy=False,
                )
            else:
                try:
                    capacity_values = np.asarray(
                        raw_short_capacity,
                        dtype=np.float64,
                    )
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "PanelData.short_capacity_shares must be numeric"
                    ) from exc
                finite_capacity = np.isfinite(capacity_values)
                invalid_capacity = (
                    np.isinf(capacity_values)
                    | (
                        finite_capacity
                        & (
                            (capacity_values < 0.0)
                            | (capacity_values != np.floor(capacity_values))
                            | (capacity_values >= float(2**63))
                        )
                    )
                )
                if bool(invalid_capacity.any()):
                    raise ValueError(
                        "PanelData.short_capacity_shares must contain non-negative "
                        "integer shares or NaN for missing evidence"
                    )
                short_capacity_shares = np.zeros(
                    panel.tradable_mask.shape,
                    dtype=np.int64,
                )
                short_capacity_shares[finite_capacity] = capacity_values[
                    finite_capacity
                ].astype(np.int64, copy=False)

        official_short_margin_floor = official_tw_short_initial_margin_rates(
            panel.dates
        )
        if official_short_margin_floor.shape != (panel.num_dates,):
            raise ValueError(
                "official short-margin schedule must match PanelData.dates"
            )
        official_short_margin_floor = official_short_margin_floor.astype(
            np.float32,
            copy=False,
        )
        raw_short_margin_rate = getattr(panel, "short_margin_rate", None)
        if raw_short_margin_rate is None:
            short_margin_rate: np.ndarray | None = None
        else:
            try:
                short_margin_rate_values = np.asarray(
                    raw_short_margin_rate,
                    dtype=np.float64,
                )
            except (TypeError, ValueError) as exc:
                raise ValueError("PanelData.short_margin_rate must be numeric") from exc
            if short_margin_rate_values.shape != panel.tradable_mask.shape:
                raise ValueError(
                    "PanelData.short_margin_rate must match tradable_mask shape"
                )
            if bool(
                (
                    np.isinf(short_margin_rate_values)
                    | (
                        np.isfinite(short_margin_rate_values)
                        & (short_margin_rate_values < 0.0)
                    )
                ).any()
            ):
                raise ValueError(
                    "PanelData.short_margin_rate must be non-negative or NaN"
                )
            floor_by_date = official_short_margin_floor[:, None]
            short_margin_rate = np.maximum(
                np.where(
                    np.isnan(short_margin_rate_values),
                    floor_by_date,
                    short_margin_rate_values,
                ),
                floor_by_date,
            ).astype(
                np.float32,
                copy=False,
            )
        can_short_open = (
            np.asarray(panel.can_short_open_mask, dtype=bool)
            if panel.can_short_open_mask is not None
            else np.zeros_like(can_sell, dtype=bool)
            if self.execution_mode == "tw_cash"
            else can_sell.copy()
        )
        if self.execution_mode == "tw_cash":
            # Eligibility and demonstrated broker inventory are distinct.
            # The default contract requires both; an explicit no-capacity-limit
            # counterfactual keeps the receipt-backed eligibility mask while a
            # one-share sentinel preserves tensor shape until the trainer
            # replaces capacity with its non-binding execution value.
            if not self.short_capacity_limit_enabled:
                can_short_open = can_short_open & can_sell
                short_capacity_shares = np.where(
                    can_short_open,
                    np.ones_like(can_sell, dtype=np.int64),
                    np.zeros_like(can_sell, dtype=np.int64),
                )
            elif short_capacity_shares is None:
                can_short_open = np.zeros_like(can_sell, dtype=bool)
            else:
                can_short_open = (
                    can_short_open
                    & can_sell
                    & (short_capacity_shares > 0)
                )
                short_capacity_shares = np.where(
                    can_short_open,
                    short_capacity_shares,
                    0,
                ).astype(np.int64, copy=False)
        short_capacity_notional: np.ndarray | None = None
        if short_capacity_shares is not None:
            cash_execution_close = np.asarray(panel.close_prices, dtype=np.float64)
            if cash_execution_close.shape != panel.tradable_mask.shape:
                raise ValueError(
                    "PanelData.close_prices must match tradable_mask shape"
                )
            valid_capacity_price = (
                (short_capacity_shares > 0)
                & np.isfinite(cash_execution_close)
                & (cash_execution_close > 0.0)
            )
            capacity_notional_f64 = np.zeros(
                panel.tradable_mask.shape,
                dtype=np.float64,
            )
            np.multiply(
                short_capacity_shares,
                cash_execution_close,
                out=capacity_notional_f64,
                where=valid_capacity_price,
            )
            valid_capacity_notional = (
                valid_capacity_price
                & np.isfinite(capacity_notional_f64)
                & (capacity_notional_f64 <= np.finfo(np.float32).max)
            )
            short_capacity_notional = np.where(
                valid_capacity_notional,
                capacity_notional_f64,
                0.0,
            ).astype(np.float32, copy=False)
        if day_trade_eligible is not None:
            # Sell-first permission is a dedicated point-in-time rule tensor;
            # the generic short mask contains close-session tradability and
            # would leak the label into the opening decision.
            assert day_trade_can_sell_open is not None
            can_short_open = (
                np.asarray(day_trade_short_open, dtype=bool)
                & day_trade_eligible
                & day_trade_can_sell_open
            )
        force_short_cover = (
            panel.force_short_cover_mask
            if panel.force_short_cover_mask is not None
            else np.zeros_like(tradable, dtype=bool)
        )
        # build_panel sanitizes feature NaN/inf values before caching.  Re-running
        # torch.nan_to_num here would duplicate the full panel for every split.
        features = panel.features.astype(np.float32, copy=False)
        if not features.flags.c_contiguous:
            features = np.ascontiguousarray(features)
        self.features_t = torch.from_numpy(features)
        self.future_log_returns_t = torch.from_numpy(returns)
        self.volume_notional_t: torch.Tensor | None = None
        if bool(include_volume_notional):
            daily_volumes = getattr(panel, "daily_volumes", None)
            if daily_volumes is None:
                fill = 0.0 if self.execution_mode != "naive" else np.inf
                volume_notional = np.full_like(
                    panel.close_prices,
                    fill,
                    dtype=np.float32,
                )
            else:
                daily_volumes_arr = np.asarray(daily_volumes, dtype=np.float32)
                if self.execution_mode != "naive":
                    # A fill at today's open or close cannot be sized from the
                    # eventual full-session volume (which includes the closing
                    # auction). Use the last completed session's shares as the
                    # causal reference and value them at today's execution
                    # price only to convert the cap into portfolio-weight units.
                    causal_volumes = np.zeros_like(daily_volumes_arr)
                    causal_volumes[1:] = daily_volumes_arr[:-1]
                    daily_volumes_arr = np.where(
                        np.isfinite(causal_volumes) & (causal_volumes >= 0.0),
                        causal_volumes,
                        0.0,
                    )
                price_source = (
                    panel.open_prices
                    if self.execution_mode == "tw_day_trade"
                    and panel.open_prices is not None
                    else panel.close_prices
                )
                prices_arr = np.asarray(price_source, dtype=np.float32)
                volume_notional = (daily_volumes_arr * prices_arr).astype(np.float32, copy=False)
            self.volume_notional_t = torch.from_numpy(volume_notional)
        self.tradable_mask_t = torch.from_numpy(tradable)
        self.can_buy_mask_t = torch.from_numpy(can_buy)
        self.can_sell_mask_t = torch.from_numpy(can_sell)
        self.can_short_open_mask_t = torch.from_numpy(can_short_open)
        self.short_capacity_shares_t = (
            torch.zeros((), dtype=torch.int64).expand(panel.tradable_mask.shape)
            if short_capacity_shares is None
            else torch.from_numpy(short_capacity_shares)
        )
        self.short_capacity_notional_t = (
            torch.zeros((), dtype=torch.float32).expand(panel.tradable_mask.shape)
            if short_capacity_notional is None
            else torch.from_numpy(short_capacity_notional)
        )
        self.short_margin_rate_t = (
            torch.from_numpy(official_short_margin_floor)
            .view(panel.num_dates, 1)
            .expand(panel.tradable_mask.shape)
            if short_margin_rate is None
            else torch.from_numpy(short_margin_rate)
        )
        self.force_short_cover_mask_t = torch.from_numpy(force_short_cover)
        self.force_exit_mask_t = torch.from_numpy(force_exit)
        self.benchmark_t = torch.from_numpy(benchmark_values)
        self.session_advance_mask_t = torch.ones(panel.num_dates, dtype=torch.bool)
        self.day_trade_eligible_mask_t = (
            None
            if day_trade_eligible is None
            else torch.from_numpy(day_trade_eligible)
        )
        self.day_trade_can_buy_open_mask_t = (
            None
            if day_trade_can_buy_open is None
            else torch.from_numpy(day_trade_can_buy_open)
        )
        self.day_trade_can_sell_open_mask_t = (
            None
            if day_trade_can_sell_open is None
            else torch.from_numpy(day_trade_can_sell_open)
        )
        self.unresolved_corporate_action_mask_t = (
            None
            if unresolved_corporate_action is None
            else torch.from_numpy(unresolved_corporate_action)
        )
        self.cash_dividend_yield_t = (
            None
            if cash_dividend_yield is None
            else torch.from_numpy(cash_dividend_yield)
        )
        self.cash_dividend_payment_delay_sessions_t = (
            None
            if cash_dividend_delay is None
            else torch.from_numpy(cash_dividend_delay)
        )

    def __len__(self) -> int:
        return int(self.valid_indices.size)

    def __getitem__(self, index: int) -> dict[str, torch.Tensor]:
        date_idx = int(self.valid_indices[index])
        feature_end = date_idx + 1 - execution_feature_lag(self.execution_mode)
        start_idx = feature_end - self.lookback
        sample = {
            "x": self.features_t[start_idx:feature_end],
            "future_log_returns": self.future_log_returns_t[date_idx],
            "tradable_mask": self.tradable_mask_t[date_idx],
            "can_buy_mask": self.can_buy_mask_t[date_idx],
            "can_sell_mask": self.can_sell_mask_t[date_idx],
            "can_short_open_mask": self.can_short_open_mask_t[date_idx],
            "short_capacity_shares": self.short_capacity_shares_t[date_idx],
            "short_capacity_notional": self.short_capacity_notional_t[date_idx],
            "short_margin_rate": self.short_margin_rate_t[date_idx],
            "force_short_cover_mask": self.force_short_cover_mask_t[date_idx],
            "force_exit_mask": self.force_exit_mask_t[date_idx],
            "benchmark": self.benchmark_t[date_idx],
            "session_advance_mask": self.session_advance_mask_t[date_idx],
        }
        if self.volume_notional_t is not None:
            sample["volume_notional"] = self.volume_notional_t[date_idx]
        if self.day_trade_eligible_mask_t is not None:
            sample["day_trade_eligible_mask"] = self.day_trade_eligible_mask_t[date_idx]
        if self.day_trade_can_buy_open_mask_t is not None:
            sample["day_trade_can_buy_open_mask"] = self.day_trade_can_buy_open_mask_t[date_idx]
        if self.day_trade_can_sell_open_mask_t is not None:
            sample["day_trade_can_sell_open_mask"] = self.day_trade_can_sell_open_mask_t[date_idx]
        if self.unresolved_corporate_action_mask_t is not None:
            sample["unresolved_corporate_action_mask"] = (
                self.unresolved_corporate_action_mask_t[date_idx]
            )
        if self.cash_dividend_yield_t is not None:
            sample["cash_dividend_yield"] = self.cash_dividend_yield_t[date_idx]
            sample["cash_dividend_payment_delay_sessions"] = (
                self.cash_dividend_payment_delay_sessions_t[date_idx]
            )
        return sample


def collate_batch(
    samples: list[dict[str, torch.Tensor]],
    batch_size: int | None = None,
) -> dict[str, torch.Tensor]:
    if batch_size is None or len(samples) >= batch_size:
        batch = {
            "x": torch.stack([s["x"] for s in samples]),
            "future_log_returns": torch.stack([s["future_log_returns"] for s in samples]),
            "tradable_mask": torch.stack([s["tradable_mask"] for s in samples]),
            "can_buy_mask": torch.stack([s["can_buy_mask"] for s in samples]),
            "can_sell_mask": torch.stack([s["can_sell_mask"] for s in samples]),
            "can_short_open_mask": torch.stack([s["can_short_open_mask"] for s in samples]),
            "short_capacity_shares": torch.stack(
                [s["short_capacity_shares"] for s in samples]
            ),
            "short_capacity_notional": torch.stack(
                [s["short_capacity_notional"] for s in samples]
            ),
            "short_margin_rate": torch.stack(
                [s["short_margin_rate"] for s in samples]
            ),
            "force_short_cover_mask": torch.stack([s["force_short_cover_mask"] for s in samples]),
            "force_exit_mask": torch.stack([s["force_exit_mask"] for s in samples]),
            "benchmark": torch.stack([s["benchmark"] for s in samples]),
            "session_advance_mask": torch.stack(
                [s["session_advance_mask"] for s in samples]
            ),
            "sample_mask": torch.ones(len(samples), dtype=torch.bool),
        }
        if "volume_notional" in samples[0]:
            batch["volume_notional"] = torch.stack([s["volume_notional"] for s in samples])
        if "day_trade_eligible_mask" in samples[0]:
            batch["day_trade_eligible_mask"] = torch.stack(
                [s["day_trade_eligible_mask"] for s in samples]
            )
        if "day_trade_can_buy_open_mask" in samples[0]:
            batch["day_trade_can_buy_open_mask"] = torch.stack(
                [s["day_trade_can_buy_open_mask"] for s in samples]
            )
        if "day_trade_can_sell_open_mask" in samples[0]:
            batch["day_trade_can_sell_open_mask"] = torch.stack(
                [s["day_trade_can_sell_open_mask"] for s in samples]
            )
        if "unresolved_corporate_action_mask" in samples[0]:
            batch["unresolved_corporate_action_mask"] = torch.stack(
                [s["unresolved_corporate_action_mask"] for s in samples]
            )
        if "cash_dividend_yield" in samples[0]:
            batch["cash_dividend_yield"] = torch.stack(
                [s["cash_dividend_yield"] for s in samples]
            )
            batch["cash_dividend_payment_delay_sessions"] = torch.stack(
                [s["cash_dividend_payment_delay_sessions"] for s in samples]
            )
        return batch

    pad_count = batch_size - len(samples)
    template = samples[0]

    def _pad_tensor_list(name: str) -> torch.Tensor:
        values = [s[name] for s in samples]
        padding = [torch.zeros_like(template[name]) for _ in range(pad_count)]
        return torch.stack(values + padding)

    batch = {
        "x": _pad_tensor_list("x"),
        "future_log_returns": _pad_tensor_list("future_log_returns"),
        "tradable_mask": _pad_tensor_list("tradable_mask"),
        "can_buy_mask": _pad_tensor_list("can_buy_mask"),
        "can_sell_mask": _pad_tensor_list("can_sell_mask"),
        "can_short_open_mask": _pad_tensor_list("can_short_open_mask"),
        "short_capacity_shares": _pad_tensor_list("short_capacity_shares"),
        "short_capacity_notional": _pad_tensor_list("short_capacity_notional"),
        "short_margin_rate": torch.stack(
            [s["short_margin_rate"] for s in samples]
            + [
                torch.full_like(template["short_margin_rate"], float("nan"))
                for _ in range(pad_count)
            ]
        ),
        "force_short_cover_mask": _pad_tensor_list("force_short_cover_mask"),
        "force_exit_mask": _pad_tensor_list("force_exit_mask"),
        "benchmark": _pad_tensor_list("benchmark"),
        "session_advance_mask": _pad_tensor_list("session_advance_mask"),
        "sample_mask": torch.tensor([True] * len(samples) + [False] * pad_count, dtype=torch.bool),
    }
    if "volume_notional" in template:
        batch["volume_notional"] = _pad_tensor_list("volume_notional")
    if "day_trade_eligible_mask" in template:
        batch["day_trade_eligible_mask"] = _pad_tensor_list(
            "day_trade_eligible_mask"
        )
    if "day_trade_can_buy_open_mask" in template:
        batch["day_trade_can_buy_open_mask"] = _pad_tensor_list(
            "day_trade_can_buy_open_mask"
        )
    if "day_trade_can_sell_open_mask" in template:
        batch["day_trade_can_sell_open_mask"] = _pad_tensor_list(
            "day_trade_can_sell_open_mask"
        )
    if "unresolved_corporate_action_mask" in template:
        batch["unresolved_corporate_action_mask"] = _pad_tensor_list(
            "unresolved_corporate_action_mask"
        )
    if "cash_dividend_yield" in template:
        batch["cash_dividend_yield"] = _pad_tensor_list("cash_dividend_yield")
        batch["cash_dividend_payment_delay_sessions"] = _pad_tensor_list(
            "cash_dividend_payment_delay_sessions"
        )
    return batch
