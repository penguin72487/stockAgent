"""Differential tests against the production paper accounting implementation.

These are ledger tests, not a claim that train.py is already using this state.
Keep the candidate config's rejection test until end-to-end wiring is done.
"""

from dataclasses import replace
from datetime import timedelta

import pytest
import torch

from stockagent.backtest.tw_day_trade_inventory import (
    CohortField as F,
    DayTradeInventoryState,
    accrue_inventory_interest,
    append_inventory_fill,
    apply_inventory_action,
    convert_inventory_to_margin,
    inventory_nav,
    inventory_path_nav,
    inventory_intraday_nav_lower_bound,
    inventory_target_delta,
    rebalance_inventory_at_open,
    reduce_inventory_fifo,
    reduce_inventory_fifo_liquidity,
    reduce_inventory_fifo_sparse_liquidity,
    reduce_inventory_fifo_path,
    settle_inventory_claims,
    validate_inventory_state,
)
from stockagent.backtest.tw_day_trade_contract import ODD_LOT_BOARD_PRICE
from stockagent.live.tw_day_trade_simulation import position_net_liquidation_pnl
from test_day_trade_margin_carry import (
    cash_entitlement,
    fills,
    register,
    replacement_reference,
    setup_account,
)
from test_tw_day_trade_simulation import _now, _quote


DAY = _now(9, 0).date().toordinal()


def v(x, *, device="cpu"):
    return torch.tensor(
        x if isinstance(x, list) else [x], dtype=torch.float64, device=device
    )


def add(
    state,
    q=2000.0,
    p=1000.0,
    *,
    day=DAY,
    fee=0.001425,
    tax=0.0015,
    normal_tax=0.003,
    rebate=0.00114,
):
    return append_inventory_fill(
        state,
        signed_shares=v(q, device=state.cohorts.device),
        price=v(p, device=state.cohorts.device),
        buy_fee_rate=v(fee, device=state.cohorts.device),
        day_sell_fee_rate=v(fee + tax, device=state.cohorts.device),
        normal_sell_fee_rate=v(fee + normal_tax, device=state.cohorts.device),
        rebate_rate=v(rebate, device=state.cohorts.device),
        day=day,
    )


def paper_initial_state(mode, *, device="cpu"):
    """Feed the same initial acquisition facts, not a reconstructed final NAV."""
    state = DayTradeInventoryState.empty(1, device=device)
    p = next(iter(mode["positions"].values()))
    state = append_inventory_fill(
        state,
        signed_shares=v(p["signed_shares"], device=device),
        price=v(p["entry_price"], device=device),
        buy_fee_rate=v(p["buy_fee_rate"], device=device),
        day_sell_fee_rate=v(p["sell_fee_rate"], device=device),
        normal_sell_fee_rate=v(p["cash_sell_fee_rate"], device=device),
        rebate_rate=v(p["commission_rebate_rate"], device=device),
        day=DAY,
    )
    return convert_inventory_to_margin(state, day=DAY)


def assert_paper(state, mode, mark):
    positions = list(mode["positions"].values())
    expected_q = sum(p["signed_shares"] for p in positions)
    assert state.shares.item() == expected_q
    assert state.realized_net_pnl.item() == pytest.approx(
        mode.get("cumulative_realized_net_pnl_twd", 0), abs=1e-7
    )
    assert state.carry_cost.item() == pytest.approx(
        mode.get("cumulative_carry_cost_twd", 0), abs=1e-7
    )
    assert state.cohorts[..., F.ENTRY_COST].sum().item() == pytest.approx(
        sum(p.get("remaining_entry_fee_twd", 0) for p in positions), abs=1e-7
    )
    expected = (
        10_000_000
        + mode.get("cumulative_realized_net_pnl_twd", 0)
        + mode.get("cumulative_corporate_action_net_twd", 0)
        - mode.get("cumulative_carry_cost_twd", 0)
        + sum(
            position_net_liquidation_pnl(p, mark)
            for p in positions
            if p["signed_shares"]
        )
    )
    assert inventory_nav(
        state, initial_capital=10_000_000, marks=v(mark, device=state.cohorts.device)
    ).item() == pytest.approx(expected, abs=1e-7)
    validate_inventory_state(state, symbols=1)


@pytest.mark.parametrize("direction", [1, -1])
def test_liquidity_curve_endpoint_matches_full_minute_fifo(direction):
    state = add(DayTradeInventoryState.empty(1), q=2000 * direction, p=1000)
    # 4,500 shares forces a partial fill inside the fifth price bucket.
    state = add(state, q=2500 * direction, p=1010, day=DAY + 1)
    prices = torch.full((1, 270), float("nan"), dtype=torch.float64)
    capacity = torch.zeros((1, 270), dtype=torch.float64)
    prices[0, [1, 20, 260, 264, 269]] = v([980, 990, 1005, 995, 1015])
    capacity[0, [1, 20, 260, 264, 269]] = v([1000, 1000, 1000, 1000, 1000])

    minute = reduce_inventory_fifo_path(
        state, prices=prices, capacity_shares=capacity
    )
    compact = reduce_inventory_fifo_liquidity(
        state, prices=prices, capacity_shares=capacity
    )
    for name in state.__dataclass_fields__:
        torch.testing.assert_close(
            getattr(compact.reduction.state, name),
            getattr(minute.reduction.state, name),
            rtol=0,
            atol=1e-10,
        )
    torch.testing.assert_close(
        compact.reduction.filled_shares,
        minute.reduction.filled_shares,
        rtol=0,
        atol=0,
    )
    torch.testing.assert_close(
        compact.executed_notional.sum(),
        (minute.minute_filled_shares * torch.nan_to_num(prices, nan=0)).sum(),
        rtol=0,
        atol=1e-10,
    )


@pytest.mark.parametrize("seed", range(16))
def test_sparse_liquidity_matches_dense_multisymbol_value_and_gradient(seed):
    """The sparse CSR ABI follows dense FIFO within FP64 summation tolerance."""
    generator = torch.Generator().manual_seed(seed)
    symbols, events, cohorts = 7, 19, 4
    directions = torch.where(
        torch.arange(symbols).remainder(2) == 0,
        torch.ones(symbols, dtype=torch.float64),
        -torch.ones(symbols, dtype=torch.float64),
    )
    state = DayTradeInventoryState.empty(symbols)
    for cohort in range(cohorts):
        shares = (
            torch.randint(0, 5, (symbols,), generator=generator).double()
            * 1000
            * directions
        )
        state = append_inventory_fill(
            state,
            signed_shares=shares,
            price=800 + 400 * torch.rand(symbols, generator=generator),
            buy_fee_rate=torch.full((symbols,), 0.001425),
            day_sell_fee_rate=torch.full((symbols,), 0.002925),
            normal_sell_fee_rate=torch.full((symbols,), 0.004425),
            rebate_rate=torch.full((symbols,), 0.00114),
            day=DAY + cohort,
        )

    dense_prices = 700 + 600 * torch.rand(
        (symbols, events, 2), generator=generator
    )
    dense_capacity = (
        torch.randint(0, 4, (symbols, events, 2), generator=generator).double()
        * 1000
    )
    # Explicit zero-liquidity plateaus and absent scheduler cells exercise
    # searchsorted ties and CSR gaps at symbol boundaries.
    dense_capacity[:, ::3] = 0
    dense_prices[:, ::5] = float("nan")
    held_short = state.shares < 0
    side = held_short.long().view(symbols, 1, 1).expand(-1, events, 1)
    selected_prices = dense_prices.gather(2, side).squeeze(2)
    selected_capacity = dense_capacity.gather(2, side).squeeze(2)

    flat_prices = dense_prices.reshape(-1)
    flat_capacity = dense_capacity.reshape(-1)
    event_symbols = torch.arange(symbols, dtype=torch.int64).repeat_interleave(
        events * 2
    )
    event_sides = torch.arange(2, dtype=torch.int64).repeat(symbols * events)
    starts = torch.arange(symbols, dtype=torch.int64) * events * 2
    ends = starts + events * 2

    dense_cohorts = state.cohorts.detach().clone().requires_grad_()
    sparse_cohorts = state.cohorts.detach().clone().requires_grad_()
    dense_state = replace(state, cohorts=dense_cohorts)
    sparse_state = replace(state, cohorts=sparse_cohorts)
    dense = reduce_inventory_fifo_liquidity(
        dense_state,
        prices=selected_prices,
        capacity_shares=selected_capacity,
    )
    sparse = reduce_inventory_fifo_sparse_liquidity(
        sparse_state,
        prices=flat_prices,
        capacity_shares=flat_capacity,
        event_symbol_indices=event_symbols,
        event_sides=event_sides,
        symbol_event_starts=starts,
        symbol_event_ends=ends,
    )
    for name in dense.reduction.state.__dataclass_fields__:
        torch.testing.assert_close(
            getattr(sparse.reduction.state, name),
            getattr(dense.reduction.state, name),
            rtol=0,
            atol=1e-8,
        )
    for name in (
        "filled_shares",
        "gross_pnl",
        "entry_fee_allocated",
        "exit_fee",
        "net_pnl",
    ):
        torch.testing.assert_close(
            getattr(sparse.reduction, name),
            getattr(dense.reduction, name),
            rtol=0,
            atol=1e-8,
        )
    torch.testing.assert_close(
        sparse.executed_notional, dense.executed_notional, rtol=0, atol=1e-8
    )

    dense_objective = (
        dense.reduction.state.realized_net_pnl
        + dense.reduction.state.cohorts.sum() * 1e-6
        + dense.executed_notional.sum() * 1e-7
    )
    sparse_objective = (
        sparse.reduction.state.realized_net_pnl
        + sparse.reduction.state.cohorts.sum() * 1e-6
        + sparse.executed_notional.sum() * 1e-7
    )
    dense_objective.backward()
    sparse_objective.backward()
    torch.testing.assert_close(
        sparse_cohorts.grad, dense_cohorts.grad, rtol=1e-10, atol=1e-10
    )


def test_sparse_liquidity_large_universe_preserves_physical_fills():
    """Global sparse prefixes must not change physical shares at panel scale."""
    symbols = 2755
    axis = torch.arange(symbols, dtype=torch.int64)
    direction = torch.where(axis.remainder(2) == 0, 1.0, -1.0)
    state = DayTradeInventoryState.empty(symbols)
    fee = torch.full((symbols,), 0.001425, dtype=torch.float64)
    for cohort in range(3):
        state = append_inventory_fill(
            state,
            signed_shares=direction * (cohort + 1) * 1000,
            price=(20 + axis.remainder(100)).to(torch.float64),
            buy_fee_rate=fee,
            day_sell_fee_rate=fee,
            normal_sell_fee_rate=fee,
            rebate_rate=fee * 0,
            day=DAY + cohort,
        )
    prices = torch.full((symbols, 270, 2), float("nan"), dtype=torch.float64)
    capacity = torch.zeros_like(prices)
    for minute, shares in ((3, 1000), (101, 2000), (269, 1000)):
        prices[:, minute, :] = (30 + axis.remainder(100)).to(torch.float64)[:, None]
        capacity[:, minute, :] = shares
    side = (state.shares < 0).long().view(symbols, 1, 1).expand(-1, 270, 1)
    dense = reduce_inventory_fifo_path(
        state,
        prices=prices.gather(2, side).squeeze(2),
        capacity_shares=capacity.gather(2, side).squeeze(2),
    )
    retained = torch.isfinite(prices).reshape(-1)
    flat = torch.nonzero(retained, as_tuple=False).flatten()
    event_symbols = flat // 540
    starts = torch.searchsorted(event_symbols, axis)
    ends = torch.searchsorted(event_symbols, axis, right=True)
    sparse = reduce_inventory_fifo_sparse_liquidity(
        state,
        prices=prices.reshape(-1)[flat],
        capacity_shares=capacity.reshape(-1)[flat],
        event_symbol_indices=event_symbols,
        event_sides=flat.remainder(2),
        symbol_event_starts=starts,
        symbol_event_ends=ends,
    )
    torch.testing.assert_close(
        sparse.reduction.filled_shares,
        dense.reduction.filled_shares,
        rtol=0,
        atol=0,
    )
    # Global versus per-symbol FP64 prefixes have different summation order.
    # This bound is monetary, not a claim of bitwise checkpoint parity.
    torch.testing.assert_close(
        sparse.reduction.net_pnl, dense.reduction.net_pnl, rtol=0, atol=2e-5
    )


def test_positive_intraday_lower_bound_is_a_valid_solvency_certificate():
    state = add(DayTradeInventoryState.empty(1), q=2000, p=1000)
    prices = torch.full((1, 270), float("nan"), dtype=torch.float64)
    capacity = torch.zeros((1, 270), dtype=torch.float64)
    prices[0, [20, 264, 269]] = v([900, 950, 1000])
    capacity[0, [20, 264, 269]] = v([1000, 1000, 1000])
    marks = torch.linspace(900, 1100, 270, dtype=torch.float64).reshape(1, -1)
    path = reduce_inventory_fifo_path(
        state, prices=prices, capacity_shares=capacity
    )
    exact = inventory_path_nav(
        state,
        prices=prices,
        minute_filled_shares=path.minute_filled_shares,
        marks=marks,
        initial_capital=10_000_000,
    )
    bound = inventory_intraday_nav_lower_bound(
        state, prices=prices, marks=marks, initial_capital=10_000_000
    )
    assert bound > 0
    assert bound <= exact.amin()


@pytest.mark.parametrize("direction", [1, -1])
def test_gap_and_calendar_interest_match_paper_not_flat_stock_proxy(
    tmp_path, direction
):
    engine, spec = setup_account(tmp_path, 0.2 * direction)
    mode = engine.state["modes"][spec.market]
    state = paper_initial_state(mode)
    assert_paper(state, mode, 1000)
    cost = state.carry_cost.clone()
    assert torch.equal(convert_inventory_to_margin(state, day=DAY).carry_cost, cost)
    # Four calendar days, even though there is only one next decision.
    assert register(engine, spec, 4, 0, price=900) == "registered"
    state = accrue_inventory_interest(state, day=DAY + 4)
    reduced = reduce_inventory_fifo(
        state, requested_shares=v(2000), price=v(900), capacity_shares=v(50000)
    )
    state = reduced.state
    assert reduced.filled_shares.item() == 2000
    assert reduced.gross_pnl.item() == direction * -200000
    assert_paper(state, mode, 900)
    assert len(fills(engine)) == 2  # conversion/accrual did not manufacture fills
    assert torch.equal(
        accrue_inventory_interest(state, day=DAY + 4).carry_cost, state.carry_cost
    )


@pytest.mark.parametrize(
    "capacity, expected_remaining, expected_addition",
    [
        (0, 2000, 0),
        (1000, 1000, 0),
        (2000, 0, 0),
        (3000, 0, -1000),
        (8000, 0, -4000),
    ],
)
def test_reversal_shares_one_capacity_budget(
    capacity, expected_remaining, expected_addition
):
    delta = inventory_target_delta(v(2000), v(-4000), v(capacity))
    assert 2000 - delta.reduction.item() == expected_remaining
    assert delta.addition.item() == expected_addition
    assert delta.reduction.item() + abs(delta.addition.item()) <= capacity


@pytest.mark.parametrize(
    "held,target,cap,reduce,add_qty",
    [
        (2000, 2000, 5000, 0, 0),
        (2000, 1000, 5000, 1000, 0),
        (2000, 4000, 5000, 0, 2000),
        (1500, 2000, 1000, 0, 500),
        (-1500, -1000, 1000, 500, 0),
        (-1500, 1000, 2000, 1500, 500),
    ],
)
def test_target_delta_retains_same_inventory_and_real_odd_residuals(
    held, target, cap, reduce, add_qty
):
    result = inventory_target_delta(v(held), v(target), v(cap))
    assert result.reduction.item() == reduce
    assert result.addition.item() == add_qty


@pytest.mark.parametrize("direction", [1, -1])
def test_fifo_partial_close_keeps_old_and_new_fee_basis(tmp_path, direction):
    engine, spec = setup_account(tmp_path, 0.2 * direction)
    mode = engine.state["modes"][spec.market]
    state = paper_initial_state(mode)
    assert register(engine, spec, 1, 0.401 * direction, price=995) == "registered"
    state = accrue_inventory_interest(state, day=DAY + 1)
    # Entry facts supplied to both ledgers; selection/funding is not under test.
    positions = sorted(mode["positions"].values(), key=lambda p: p["entry_at"])
    new = positions[-1]
    state = append_inventory_fill(
        state,
        signed_shares=v(new["signed_shares"]),
        price=v(995),
        buy_fee_rate=v(new["buy_fee_rate"]),
        day_sell_fee_rate=v(new["sell_fee_rate"]),
        normal_sell_fee_rate=v(new["cash_sell_fee_rate"]),
        rebate_rate=v(new["commission_rebate_rate"]),
        day=DAY + 1,
    )
    assert len(positions) == 2
    at = _now(13, 25) + timedelta(days=1)
    quote = _quote(bid=1010, ask=1010, minute_volume_lots=5) | {
        "quote_at": at.isoformat()
    }
    # 50% of 5 lots floors to 2 board lots total, not 2 lots per cohort.
    for p in positions:
        engine._close_position(
            p,
            mode,
            price=1010,
            quote=quote,
            now=at,
            reason="13_24_market_force_exit",
            order_type="MKT",
            quantity=3000,
        )
    reduced = reduce_inventory_fifo(
        state, requested_shares=v(3000), price=v(1010), capacity_shares=v(2000)
    )
    state = reduced.state
    assert reduced.filled_shares.item() == 2000
    assert state.cohorts[0, 0, F.SHARES] == 0
    assert state.cohorts[1, 0, F.BASIS] == 995
    assert_paper(state, mode, 1010)


@pytest.mark.parametrize("direction", [1, -1])
def test_physical_replacement_signed_claim_odd_exit_and_payment_match_paper(
    tmp_path, direction
):
    engine, spec = setup_account(tmp_path, 0.2 * direction)
    mode = engine.state["modes"][spec.market]
    state = paper_initial_state(mode)
    spec = replace(spec, odd_lot_execution_policy=ODD_LOT_BOARD_PRICE)
    mode["odd_lot_execution_policy"] = ODD_LOT_BOARD_PRICE
    replacement_reference(tmp_path)
    assert (
        register(
            engine,
            spec,
            4,
            0.0,
            price=1330.0,
            quote_updates={"upper_limit": 1500.0, "lower_limit": 1200.0},
        )
        == "registered"
    )
    state = apply_inventory_action(
        state,
        event_day=DAY + 4,
        as_of_day=DAY + 4,
        event_mask=v(1),
        share_ratio=v(0.75),
        cash_per_old_share=v(2.5),
        payment_day=v(DAY + 7),
    )
    assert state.shares.item() == 1500 * direction
    assert state.cohorts[0, 0, F.ENTRY_PRICE] == 1000  # historical fill stays unchanged
    assert state.cohorts[0, 0, F.BASIS].item() == pytest.approx(1000 / 0.75)
    state = accrue_inventory_interest(state, day=DAY + 4)
    state = reduce_inventory_fifo(
        state, requested_shares=v(1500), price=v(1330), capacity_shares=v(50000)
    ).state
    assert_paper(state, mode, 1330)
    assert (
        state.corporate_action_receivable.item()
        == mode["corporate_action_receivable_twd"]
    )
    assert state.corporate_action_payable.item() == mode["corporate_action_payable_twd"]
    before = inventory_nav(state, initial_capital=10_000_000, marks=v(1330))
    state = settle_inventory_claims(state, day=DAY + 7)
    state = settle_inventory_claims(state, day=DAY + 7)
    engine._settle_corporate_action_claims(mode, _now(9, 1) + timedelta(days=7))
    assert (
        state.corporate_action_cash.item()
        == mode["corporate_action_cash_net_twd"]
        == 5000 * direction
    )
    assert inventory_nav(state, initial_capital=10_000_000, marks=v(1330)) == before


def test_action_entitlement_excludes_new_shares_and_rejects_duplicate_or_missing_terms():
    state = add(DayTradeInventoryState.empty(1))
    state = add(state, q=1000, day=DAY + 1)
    kwargs = dict(
        event_day=DAY + 1,
        as_of_day=DAY + 1,
        event_mask=v(1),
        share_ratio=v(1),
        cash_per_old_share=v(10),
        payment_day=v(DAY + 5),
    )
    result = apply_inventory_action(state, **kwargs)
    assert result.corporate_action_net == 20000  # only pre-ex-date shares
    with pytest.raises(RuntimeError, match="duplicate"):
        apply_inventory_action(result, **kwargs)
    with pytest.raises(RuntimeError, match="exact payment"):
        apply_inventory_action(state, **(kwargs | {"payment_day": v(float("nan"))}))
    with pytest.raises(RuntimeError, match="fractional shares"):
        apply_inventory_action(state, **(kwargs | {"share_ratio": v(0.750123)}))
    assert state.shares == 3000 and state.claims.numel() == 0  # atomic failure


def test_missing_price_no_execution_and_no_invented_inventory_mark():
    state = add(DayTradeInventoryState.empty(1))
    result = reduce_inventory_fifo(
        state, requested_shares=v(2000), price=v(float("nan")), capacity_shares=v(5000)
    )
    assert result.filled_shares == 0 and result.state.shares == 2000
    assert torch.isfinite(result.state.realized_net_pnl)
    with pytest.raises(RuntimeError, match="missing a source-backed mark"):
        inventory_nav(state, initial_capital=10_000_000, marks=v(float("nan")))


def test_checkpoint_uses_canonical_safe_writer_and_preserves_next_day_result(tmp_path):
    from stockagent.training.trainer import _atomic_torch_save, _load_checkpoint

    state = convert_inventory_to_margin(add(DayTradeInventoryState.empty(1)), day=DAY)
    state = apply_inventory_action(
        state,
        event_day=DAY + 1,
        as_of_day=DAY + 1,
        event_mask=v(1),
        share_ratio=v(1),
        cash_per_old_share=v(10),
        payment_day=v(DAY + 5),
    )
    path = tmp_path / "checkpoint_last.pt"
    identity = dict(universe=("2330",), release_id="fixture-release-1")
    _atomic_torch_save(
        {"tw_day_trade_inventory": state.checkpoint_state(**identity)}, path
    )
    payload = _load_checkpoint(path)["tw_day_trade_inventory"]
    restored = DayTradeInventoryState.from_checkpoint_state(payload, **identity)

    def finish(s):
        s = accrue_inventory_interest(s, day=DAY + 4)
        s = reduce_inventory_fifo(
            s, requested_shares=v(1000), price=v(900), capacity_shares=v(5000)
        ).state
        return settle_inventory_claims(s, day=DAY + 5)

    whole, split = finish(state), finish(restored)
    for name in state.__dataclass_fields__:
        torch.testing.assert_close(
            getattr(whole, name), getattr(split, name), rtol=0, atol=0
        )
    with pytest.raises(ValueError, match="incompatible"):
        DayTradeInventoryState.from_checkpoint_state(
            payload | {"abi": "old-flat-stock"}, **identity
        )
    with pytest.raises(ValueError, match="pinned universe"):
        DayTradeInventoryState.from_checkpoint_state(
            payload, **(identity | {"universe": ("0050",)})
        )
    with pytest.raises(ValueError, match="pinned universe/release"):
        DayTradeInventoryState.from_checkpoint_state(
            payload, **(identity | {"release_id": "fixture-release-2"})
        )


@pytest.mark.parametrize("direction", [1, -1])
def test_repeated_partial_fifo_exit_preserves_nonnegative_entry_cost(direction):
    """Long-lived cohorts must remain checkpointable after many partial exits."""
    state = add(
        DayTradeInventoryState.empty(1),
        q=direction * 10007,
        p=123.45,
    )
    original_entry_cost = state.cohorts[..., F.ENTRY_COST].sum().clone()
    allocated = original_entry_cost.new_zeros(())
    prices = v([124.01, 124.02, 124.03]).reshape(1, 3)
    capacity = v([19, 23, 31]).reshape(1, 3)

    # This mirrors a residual position being reduced across many trading days.
    for _ in range(137):
        result = reduce_inventory_fifo_path(
            state, prices=prices, capacity_shares=capacity
        ).reduction
        state = result.state
        allocated = allocated + result.entry_fee_allocated.sum()
        assert torch.all(state.cohorts[..., F.ENTRY_COST] >= 0)
        validate_inventory_state(state, symbols=1)

    result = reduce_inventory_fifo(
        state,
        requested_shares=state.shares.abs(),
        price=v(124.04),
        capacity_shares=state.shares.abs(),
    )
    state = result.state
    allocated = allocated + result.entry_fee_allocated.sum()
    assert state.shares.item() == 0
    assert state.cohorts[..., F.ENTRY_COST].sum().item() == 0
    torch.testing.assert_close(allocated, original_entry_cost, rtol=0, atol=1e-10)
    state.checkpoint_state(universe=("2330",), release_id="fixture-release-1")


@pytest.mark.parametrize(
    "direction,weight,volume,price",
    [
        (1, 0.0, 100, 900),
        (-1, 0.0, 100, 1010),
        (1, 0.2002, 100, 1000),
        (-1, -0.2008, 100, 1000),
        (1, -0.4, 6, 1000),
        (1, -0.4, 2, 1000),
        (-1, 0.4, 6, 1000),
        (-1, 0.4, 2, 1000),
        (1, 0.401, 100, 995),
        (-1, -0.401, 100, 995),
        (1, 1.0, 1000, 1000),
        (-1, -1.0, 1000, 1000),
    ],
)
def test_full_opening_target_sizing_reduction_funding_matches_paper(
    tmp_path, direction, weight, volume, price
):
    engine, spec = setup_account(tmp_path, 0.2 * direction)
    mode = engine.state["modes"][spec.market]
    state = paper_initial_state(mode)
    p = next(iter(mode["positions"].values())).copy()
    assert register(engine, spec, 1, weight, volume=volume, price=price) == "registered"
    result = rebalance_inventory_at_open(
        state,
        weights=v(weight),
        official_open=v(price),
        entry_price=v(price),
        entry_volume_shares=v(volume * 1000),
        lower_limit=v(900),
        upper_limit=v(1100),
        can_enter=v(1),
        buy_fee_rate=v(p["buy_fee_rate"]),
        day_sell_fee_rate=v(p["sell_fee_rate"]),
        normal_sell_fee_rate=v(p["cash_sell_fee_rate"]),
        rebate_rate=v(p["commission_rebate_rate"]),
        initial_capital=10_000_000,
        day=DAY + 1,
    )
    assert result.sizing_nav.item() == pytest.approx(
        mode["session_sizing_nav_twd"], abs=1e-7
    )
    assert_paper(result.state, mode, price)
    actual = fills(engine)[1:]
    assert result.reduction.filled_shares.item() == sum(
        r["quantity"] for r in actual if r["purpose"] == "next_signal_inventory_delta"
    )
    assert result.addition_shares.abs().item() == sum(
        r["quantity"] for r in actual if r["purpose"] == "entry"
    )


def opening_kwargs(*, symbols=1, day=DAY):
    return dict(
        weights=v([0.2] * symbols),
        official_open=v([1000] * symbols),
        entry_price=v([1000] * symbols),
        entry_volume_shares=v([100_000] * symbols),
        lower_limit=v([900] * symbols),
        upper_limit=v([1100] * symbols),
        can_enter=v([1] * symbols),
        buy_fee_rate=v([0.001425] * symbols),
        day_sell_fee_rate=v([0.002925] * symbols),
        normal_sell_fee_rate=v([0.004425] * symbols),
        rebate_rate=v([0.00114] * symbols),
        initial_capital=10_000_000,
        day=day,
    )


def test_open_price_sizes_but_vwap_executes_and_does_not_get_tick_rounded():
    kwargs = opening_kwargs() | {"entry_price": v(1003.123456789)}
    result = rebalance_inventory_at_open(DayTradeInventoryState.empty(1), **kwargs)
    assert result.target_shares.item() == result.addition_shares.item() == 2000
    assert result.state.cohorts[0, 0, F.ENTRY_PRICE].item() == 1003.123456789
    assert result.sizing_nav.item() == 10_000_000
    with pytest.raises(RuntimeError, match="duplicate or out-of-order daily decision"):
        rebalance_inventory_at_open(result.state, **kwargs)


def test_gross_nav_funding_matches_existing_integer_allocator_full_universe():
    import numpy as np
    from stockagent.backtest.tw_integer_execution import (
        _scale_lot_buys_to_budget_with_fixed_fees,
    )

    n = 2746
    rng = np.random.default_rng(72)
    raw = rng.normal(size=n)
    weights = raw / np.abs(raw).sum() * 2  # explicitly stress gross funding
    opening = rng.uniform(5, 1200, n)
    execution = opening * rng.uniform(0.91, 1.09, n)
    kwargs = opening_kwargs(symbols=n) | dict(
        weights=torch.as_tensor(weights),
        official_open=torch.as_tensor(opening),
        entry_price=torch.as_tensor(execution),
        lower_limit=torch.as_tensor(opening * 0.9),
        upper_limit=torch.as_tensor(opening * 1.1),
    )
    result = rebalance_inventory_at_open(DayTradeInventoryState.empty(n), **kwargs)
    desired = (
        np.floor(np.abs(weights) * 10_000_000 / opening / 1000).astype(np.int64) * 1000
    )
    desired = np.minimum(desired, 50_000)
    fees = np.where(weights >= 0, 0.001425, 0.002925)
    expected = _scale_lot_buys_to_budget_with_fixed_fees(
        desired,
        execution,
        np.full(n, 1000),
        fees,
        budget=10_000_000,
        minimum_commission=0,
        commission_rounding="none",
    )
    np.testing.assert_array_equal(
        result.addition_shares.detach().abs().numpy(), expected
    )
    assert result.gross_addition_cost <= 10_000_000


def test_entry_eligibility_cannot_block_reducing_already_held_shares():
    state = convert_inventory_to_margin(add(DayTradeInventoryState.empty(1)), day=DAY)
    result = rebalance_inventory_at_open(
        state, **(opening_kwargs(day=DAY + 1) | {"can_enter": v(0)})
    )
    assert result.reduction.filled_shares == 2000 and result.addition_shares == 0
    halted = rebalance_inventory_at_open(
        state, **(opening_kwargs(day=DAY + 1) | {"can_enter": v(0), "halted": v(1)})
    )
    assert halted.reduction.filled_shares == 0 and halted.state.shares == 2000


def test_negative_nav_is_blocked_not_recapitalized():
    state = replace(
        DayTradeInventoryState.empty(1),
        realized_net_pnl=torch.tensor(-10_000_001.0, dtype=torch.float64),
    )
    with pytest.raises(RuntimeError, match="nonpositive account NAV"):
        rebalance_inventory_at_open(state, **opening_kwargs())
    assert state.realized_net_pnl == -10_000_001


def test_multi_day_opening_checkpoint_chunks_match_whole_trajectory():
    state = DayTradeInventoryState.empty(1)
    split = state
    identity = dict(universe=("2330",), release_id="fixture-release-1")
    for i, (weight, volume) in enumerate(
        [
            (0.4, 100000),
            (0.2, 2000),
            (-0.5, 4000),
            (-0.7, 50000),
            (0.4, 2000),
            (0.2, 100000),
            (0, 50000),
        ]
    ):
        kwargs = opening_kwargs(day=DAY + i) | {
            "weights": v(weight),
            "entry_volume_shares": v(volume),
        }
        whole = rebalance_inventory_at_open(state, **kwargs)
        divided = rebalance_inventory_at_open(split, **kwargs)
        torch.testing.assert_close(whole.sizing_nav, divided.sizing_nav, atol=0, rtol=0)
        state = convert_inventory_to_margin(whole.state, day=DAY + i)
        split = convert_inventory_to_margin(divided.state, day=DAY + i)
        if i in (1, 3, 5):
            split = DayTradeInventoryState.from_checkpoint_state(
                split.checkpoint_state(**identity), **identity
            )
        for name in state.__dataclass_fields__:
            torch.testing.assert_close(
                getattr(state, name), getattr(split, name), atol=0, rtol=0
            )


@pytest.mark.parametrize(
    "device",
    [
        "cpu",
        pytest.param(
            "cuda",
            marks=pytest.mark.skipif(
                not torch.cuda.is_available(), reason="CUDA unavailable"
            ),
        ),
    ],
)
def test_cross_day_gradient_is_finite_and_account_state_stays_on_device(device):
    from stockagent.backtest.tw_day_trade_minute import _ste_floor_lots

    requested = torch.tensor(
        [2100.0], dtype=torch.float64, device=device, requires_grad=True
    )
    q = _ste_floor_lots(requested)
    state = DayTradeInventoryState.empty(1, device=device)
    state = append_inventory_fill(
        state,
        signed_shares=q,
        price=v(1000, device=device),
        buy_fee_rate=v(0, device=device),
        day_sell_fee_rate=v(0, device=device),
        normal_sell_fee_rate=v(0, device=device),
        rebate_rate=v(0, device=device),
        day=DAY,
    )
    state = convert_inventory_to_margin(state, day=DAY)
    state = accrue_inventory_interest(state, day=DAY + 4)
    nav = inventory_nav(state, initial_capital=10_000_000, marks=v(900, device=device))
    (-nav.log()).backward()
    assert requested.grad is not None and torch.isfinite(requested.grad).all()
    assert requested.grad.abs().sum() > 0
    assert state.shares.item() == 2000
    assert state.cohorts.device.type == device
    private = state.detached()
    assert private.cohorts.data_ptr() != state.cohorts.data_ptr()
    assert private.cohorts.grad_fn is None


def test_fifo_kernel_fullgraph_matches_eager():
    state = add(DayTradeInventoryState.empty(1))
    fn = torch.compile(reduce_inventory_fifo, fullgraph=True, backend="eager")
    args = dict(requested_shares=v(1000), price=v(1010), capacity_shares=v(1000))
    eager, compiled = reduce_inventory_fifo(state, **args), fn(state, **args)
    torch.testing.assert_close(
        eager.state.cohorts, compiled.state.cohorts, rtol=0, atol=0
    )
    torch.testing.assert_close(eager.net_pnl, compiled.net_pnl, rtol=0, atol=0)


def test_opening_fullgraph_and_gradient_match_eager():
    fn = torch.compile(rebalance_inventory_at_open, fullgraph=True, backend="eager")
    state = convert_inventory_to_margin(add(DayTradeInventoryState.empty(1)), day=DAY)
    w1 = v(0.45).requires_grad_()
    w2 = w1.detach().clone().requires_grad_()
    kwargs = opening_kwargs(day=DAY + 1) | {"entry_price": v(1001.123456)}
    eager = rebalance_inventory_at_open(state, **(kwargs | {"weights": w1}))
    compiled = fn(state, **(kwargs | {"weights": w2}))
    for name in state.__dataclass_fields__:
        torch.testing.assert_close(
            getattr(eager.state, name), getattr(compiled.state, name), rtol=0, atol=0
        )

    def loss(result):
        return -inventory_nav(
            result.state, initial_capital=10_000_000, marks=v(990)
        ).log()

    loss(eager).backward()
    loss(compiled).backward()
    assert w1.grad is not None and w1.grad.abs().item() > 0
    torch.testing.assert_close(w1.grad, w2.grad, rtol=0, atol=0)


@pytest.mark.parametrize("device", ["cpu", "cuda"])
def test_compiled_invalid_event_is_atomic_and_does_not_poison_device(device):
    if device == "cuda" and not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    state = add(DayTradeInventoryState.empty(1, device=device))
    fn = torch.compile(reduce_inventory_fifo, fullgraph=True, backend="eager")
    bad = fn(
        state,
        requested_shares=v(1000, device=device),
        price=v(1010, device=device),
        capacity_shares=v(-1000, device=device),
    )
    assert bad.state.failed.item() == 1
    assert bad.filled_shares.item() == 0
    torch.testing.assert_close(bad.state.cohorts, state.cohorts, rtol=0, atol=0)
    torch.testing.assert_close(bad.state.realized_net_pnl, state.realized_net_pnl)
    with pytest.raises(RuntimeError, match="trajectory failed"):
        validate_inventory_state(bad.state, symbols=1)
    with pytest.raises(RuntimeError, match="trajectory failed"):
        bad.state.checkpoint_state(universe=("2330",), release_id="fixture-1")
    # Both another independent trajectory and ordinary GPU work still succeed.
    good = fn(
        state,
        requested_shares=v(1000, device=device),
        price=v(1010, device=device),
        capacity_shares=v(1000, device=device),
    )
    assert good.state.failed.item() == 0
    assert good.filled_shares.item() == 1000
    assert (v(2, device=device) * 3).item() == 6
    absorbing = fn(
        bad.state,
        requested_shares=v(1000, device=device),
        price=v(1010, device=device),
        capacity_shares=v(1000, device=device),
    )
    assert absorbing.filled_shares.item() == 0
    assert absorbing.state.failed.item() == 1
    assert torch.isnan(
        inventory_nav(
            absorbing.state, initial_capital=10_000_000, marks=v(1010, device=device)
        )
    )


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA unavailable")
def test_cuda_invalid_opening_rejects_whole_event_and_persists_failure():
    state = convert_inventory_to_margin(
        add(DayTradeInventoryState.empty(1, device="cuda")), day=DAY
    )
    good = rebalance_inventory_at_open(state, **opening_kwargs(day=DAY + 1))
    rejected = rebalance_inventory_at_open(good.state, **opening_kwargs(day=DAY + 1))
    assert rejected.state.failed.item() == 1
    assert rejected.addition_shares.item() == 0
    torch.testing.assert_close(rejected.state.shares, good.state.shares)
    torch.testing.assert_close(
        rejected.state.realized_net_pnl, good.state.realized_net_pnl
    )


def test_inventory_hotpath_contains_no_device_assertions():
    import inspect
    import stockagent.backtest.tw_day_trade_inventory as module

    assert "torch._assert" not in inspect.getsource(module)


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("device", ["cpu", "cuda"])
def test_path_integral_matches_all_270_fifo_events(direction, device):
    if device == "cuda" and not torch.cuda.is_available():
        pytest.skip("CUDA unavailable")
    state = DayTradeInventoryState.empty(1, device=device)
    for day in range(11):
        state = add(
            state,
            q=direction * (1751 + 1000 * day),
            p=990 + day * 5,
            fee=0.001425 + day * 0.00001,
            day=DAY + day,
        )
        if day < 10:
            state = convert_inventory_to_margin(state, day=DAY + day)
    g = torch.Generator().manual_seed(82)
    prices = (950 + torch.rand((1, 270), generator=g, dtype=torch.float64) * 100).to(
        device
    )
    capacity = (
        torch.randint(0, 3, (1, 270), generator=g).to(
            device=device, dtype=torch.float64
        )
        * 1000
    )
    capacity[:, 20:200] = 0
    prices[:, 10:16] = float("nan")
    fast = reduce_inventory_fifo_path(state, prices=prices, capacity_shares=capacity)
    marks = torch.full_like(prices, 1000)
    fast_nav = inventory_path_nav(state, prices=prices, minute_filled_shares=fast.minute_filled_shares,
                                  marks=marks, initial_capital=10_000_000)
    reference = state
    expected = []
    expected_nav = []
    for minute in range(270):
        result = reduce_inventory_fifo(
            reference,
            requested_shares=reference.shares.abs(),
            price=prices[:, minute],
            capacity_shares=capacity[:, minute],
        )
        reference = result.state
        expected.append(result.filled_shares)
        expected_nav.append(inventory_nav(reference, initial_capital=10_000_000, marks=marks[:, minute]))
    torch.testing.assert_close(
        fast.minute_filled_shares, torch.stack(expected, -1), rtol=0, atol=0
    )
    torch.testing.assert_close(
        fast.reduction.state.cohorts, reference.cohorts, rtol=1e-12, atol=1e-9
    )
    torch.testing.assert_close(
        fast.reduction.state.realized_net_pnl,
        reference.realized_net_pnl,
        rtol=1e-12,
        atol=1e-7,
    )
    torch.testing.assert_close(fast_nav, torch.stack(expected_nav), rtol=1e-12, atol=1e-7)


def test_path_integral_gradient_matches_sequential_fifo_away_from_kinks():
    def run(raw, fast):
        q = raw + (raw.round() - raw).detach()
        state = append_inventory_fill(
            DayTradeInventoryState.empty(1),
            signed_shares=q,
            price=v(100),
            buy_fee_rate=v(0.001425),
            day_sell_fee_rate=v(0.002925),
            normal_sell_fee_rate=v(0.004425),
            rebate_rate=v(0.00114),
            day=DAY,
        )
        p, c = v([101, 99, 102]).reshape(1, 3), v([1000, 1000, 1000]).reshape(1, 3)
        if fast:
            state = reduce_inventory_fifo_path(
                state, prices=p, capacity_shares=c
            ).reduction.state
        else:
            for m in range(3):
                state = reduce_inventory_fifo(
                    state,
                    requested_shares=state.shares.abs(),
                    price=p[:, m],
                    capacity_shares=c[:, m],
                ).state
        return inventory_nav(state, initial_capital=10_000_000, marks=v(103))

    a, b = v(2751).requires_grad_(), v(2751).requires_grad_()
    va, vb = run(a, True), run(b, False)
    va.backward()
    vb.backward()
    torch.testing.assert_close(va, vb, rtol=0, atol=1e-8)
    torch.testing.assert_close(a.grad, b.grad, rtol=1e-12, atol=1e-10)


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA unavailable")
def test_full_opening_cuda_matches_cpu_without_precision_demotion():
    cpu = convert_inventory_to_margin(
        add(DayTradeInventoryState.empty(1), q=-2000), day=DAY
    )
    identity = dict(universe=("2330",), release_id="fixture-release-1")
    gpu = DayTradeInventoryState.from_checkpoint_state(
        cpu.checkpoint_state(**identity), **identity, device="cuda"
    )
    kwargs = opening_kwargs(day=DAY + 4) | {
        "weights": v(0.5),
        "entry_volume_shares": v(6000),
    }
    reference = rebalance_inventory_at_open(cpu, **kwargs)
    actual = rebalance_inventory_at_open(gpu, **kwargs)
    torch.cuda.synchronize()
    for name in cpu.__dataclass_fields__:
        torch.testing.assert_close(
            getattr(reference.state, name),
            getattr(actual.state, name).cpu(),
            rtol=0,
            atol=1e-8,
        )
        assert getattr(actual.state, name).dtype == torch.float64
    assert actual.state.shares == 1000


def test_checkpoint_rejects_precision_loss_and_bad_identity():
    state = add(DayTradeInventoryState.empty(1))
    identity = dict(universe=("2330",), release_id="fixture-release-1")
    payload = state.checkpoint_state(**identity)
    with pytest.raises(ValueError, match="float64"):
        DayTradeInventoryState.from_checkpoint_state(
            payload | {"cohorts": payload["cohorts"].float()}, **identity
        )
    with pytest.raises(ValueError, match="exact data release"):
        state.checkpoint_state(**(identity | {"release_id": "latest"}))
    with pytest.raises(ValueError, match="unique ordered"):
        state.checkpoint_state(**(identity | {"universe": ("2330", "2330")}))


def test_future_claim_cash_cannot_fund_past_decisions():
    state = convert_inventory_to_margin(add(DayTradeInventoryState.empty(1)), day=DAY)
    state = apply_inventory_action(
        state,
        event_day=DAY + 1,
        as_of_day=DAY + 1,
        event_mask=v(1),
        share_ratio=v(1),
        cash_per_old_share=v(10),
        payment_day=v(DAY + 5),
    )
    future = settle_inventory_claims(state, day=DAY + 5)
    with pytest.raises(RuntimeError, match="clock moved backwards"):
        rebalance_inventory_at_open(future, **opening_kwargs(day=DAY + 2))
    with pytest.raises(RuntimeError, match="missing acquisition-day margin conversion"):
        convert_inventory_to_margin(add(DayTradeInventoryState.empty(1)), day=DAY + 1)


def test_unpaid_dividends_are_equity_not_funding_matches_paper(tmp_path):
    engine, spec = setup_account(tmp_path)
    mode = engine.state["modes"][spec.market]
    state = paper_initial_state(mode)
    p = next(iter(mode["positions"].values())).copy()
    cash_entitlement(tmp_path, amount=500)
    assert (
        register(
            engine,
            spec,
            1,
            1.0,
            price=500.0,
            quote_updates={"upper_limit": 550.0, "lower_limit": 450.0},
        )
        == "registered"
    )
    state = apply_inventory_action(
        state,
        event_day=DAY + 1,
        as_of_day=DAY + 1,
        event_mask=v(1),
        share_ratio=v(1),
        cash_per_old_share=v(500),
        payment_day=v(DAY + 5),
    )
    result = rebalance_inventory_at_open(
        state,
        weights=v(1),
        official_open=v(500),
        entry_price=v(500),
        entry_volume_shares=v(100000),
        lower_limit=v(450),
        upper_limit=v(550),
        can_enter=v(1),
        buy_fee_rate=v(p["buy_fee_rate"]),
        day_sell_fee_rate=v(p["sell_fee_rate"]),
        normal_sell_fee_rate=v(p["cash_sell_fee_rate"]),
        rebate_rate=v(p["commission_rebate_rate"]),
        initial_capital=10_000_000,
        day=DAY + 1,
    )
    assert result.state.corporate_action_receivable == 1_000_000
    assert result.state.corporate_action_cash == 0
    assert_paper(result.state, mode, 500)
