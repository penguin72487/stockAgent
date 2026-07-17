"""Differentiable Taiwan cash and day-trade settlement ledgers.

The two executors in this module are the continuous-weight counterparts of the
integer-share audit executor.  They deliberately keep the time recurrence
sequential while vectorising every per-security operation.  This is O(T * S),
which is the asymptotic lower bound when every requested order and return must
be inspected.

Fees here are proportional rates only.  Broker-specific minimum commission and
whole-currency rounding are discontinuous in both notional and order count, so
they intentionally remain outside this differentiable training surrogate.  The
final Taiwan report/artifact is produced by the exact integer executor, which
applies those configured broker rules once per nonzero symbol-side aggregate
order.

All state is expressed as a ratio of current net asset value (NAV).  The core
accounting identity is therefore, while the portfolio is alive::

    settled_cash + risky_market_value + restricted_short_collateral
        + receivables - payables == 1

Settlement transfers do not create returns.  Fees and price changes do, because
they change the right-hand-side NAV before the state is re-normalised.  A
liquidity default at the pre-receipt T+2 payment phase is absorbing.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Callable

import torch


_MIN_WEALTH_FACTOR = 1.0e-6

_TW_CASH_COMPILED_CHUNK_CACHE: dict[tuple[object, ...], Callable[..., tuple[torch.Tensor, ...]]] = {}
_TW_CASH_FAILED_COMPILE_KEYS: set[tuple[object, ...]] = set()
_TW_CASH_COMPILE_STATS: dict[str, int] = {
    "compile_constructors": 0,
    "compiled_chunk_calls": 0,
    "eager_fallback_calls": 0,
}


def _settlement_gradient_horizon_rows() -> int:
    try:
        return max(
            0,
            int(
                os.environ.get(
                    "STOCKAGENT_TW_CONTINUOUS_GRADIENT_HORIZON_ROWS",
                    "0",
                )
            ),
        )
    except ValueError:
        return 0


def _restart_recurrent_gradient(
    value: torch.Tensor,
    *,
    requires_grad: bool,
) -> torch.Tensor:
    """Detach carried state while preserving the compiled-call tensor ABI."""

    restarted = value.detach().clone(memory_format=torch.contiguous_format)
    if requires_grad and restarted.is_floating_point():
        restarted.requires_grad_(True)
    return restarted


def _validate_finite_nonnegative_eager(value: torch.Tensor, name: str) -> None:
    """Validate external state without introducing data guards into fullgraph compile."""

    if torch.compiler.is_compiling():
        return
    if not bool(torch.isfinite(value).all().item()):
        raise ValueError(f"{name} must be finite")
    if not bool((value >= 0.0).all().item()):
        raise ValueError(f"{name} must be non-negative")


def _resolve_short_maintenance_ratio(value: float) -> float:
    """Validate the scalar whole-account maintenance-collateral floor."""

    if isinstance(value, bool):
        raise ValueError("short_maintenance_ratio must be a finite scalar >= 1.0")
    try:
        ratio = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "short_maintenance_ratio must be a finite scalar >= 1.0"
        ) from exc
    if not math.isfinite(ratio) or ratio < 1.0:
        raise ValueError("short_maintenance_ratio must be a finite scalar >= 1.0")
    return ratio


def _validate_normalized_state_eager(
    *,
    risky: torch.Tensor,
    cash: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
    alive: torch.Tensor,
    short_sale_collateral: torch.Tensor | None = None,
    short_margin_collateral: torch.Tensor | None = None,
) -> None:
    """Reject external recurrent state that violates capital conservation.

    Every live recurrent state is expressed as a fraction of its own NAV, so
    its signed ledger must sum to one.  A dead account is absorbing and must
    carry no residual assets or claims.  This validation intentionally stays at
    the eager API boundary: all compiled training/evaluation recurrence starts
    from a validated seed and then consumes only state produced by this ledger.
    A data-dependent assertion inside the fullgraph kernel would either graph
    break or turn a bad CUDA input into a device-side assertion.
    """

    if torch.compiler.is_compiling():
        return
    # Reduce on-device and synchronize only the three scalar diagnostics; never
    # copy an O(S) portfolio vector to the host merely to validate the boundary.
    risky64 = risky.detach().to(dtype=torch.float64)
    cash64 = cash.detach().to(dtype=torch.float64)
    payables64 = payables.detach().to(dtype=torch.float64)
    receivables64 = receivables.detach().to(dtype=torch.float64)
    short_sale64 = (
        torch.zeros((), device=risky.device, dtype=torch.float64)
        if short_sale_collateral is None
        else short_sale_collateral.detach().to(dtype=torch.float64).sum()
    )
    short_margin64 = (
        torch.zeros((), device=risky.device, dtype=torch.float64)
        if short_margin_collateral is None
        else short_margin_collateral.detach().to(dtype=torch.float64).sum()
    )
    alive_value = bool(alive.detach().item())
    ledger = (
        cash64
        + risky64.sum()
        + short_sale64
        + short_margin64
        + receivables64.sum()
        - payables64.sum()
    )
    magnitude = (
        cash64.abs()
        + risky64.abs().sum()
        + short_sale64.abs()
        + short_margin64.abs()
        + receivables64.abs().sum()
        + payables64.abs().sum()
    )
    source_eps = float(torch.finfo(cash.dtype).eps)
    # FP32 is the canonical finance dtype.  The cap still catches material
    # corruption if a lower-precision caller invokes this low-level API.
    relative_tolerance = min(max(source_eps * 64.0, 1.0e-10), 5.0e-2)
    tolerance = relative_tolerance * max(1.0, float(magnitude.item()))
    if alive_value:
        if abs(float(ledger.item()) - 1.0) > tolerance:
            raise ValueError(
                "initial Taiwan settlement state violates the normalized "
                "accounting identity: cash + risky + short-sale collateral + "
                "short-margin collateral + receivables - payables must equal 1"
            )
    elif float(magnitude.item()) > tolerance:
        raise ValueError(
            "an absorbing dead Taiwan settlement state must contain only zeros"
        )


@dataclass(slots=True)
class TaiwanContinuousResult:
    strategy_returns: torch.Tensor
    turnovers: torch.Tensor
    weights_history: torch.Tensor
    cash_history: torch.Tensor
    payables_history: torch.Tensor
    receivables_history: torch.Tensor
    settlement_default: torch.Tensor
    equity_scale_history: torch.Tensor
    final_weights: torch.Tensor
    final_cash: torch.Tensor
    final_payables: torch.Tensor
    final_receivables: torch.Tensor
    final_alive: torch.Tensor
    final_equity_scale: torch.Tensor
    short_sale_collateral_history: torch.Tensor | None = None
    short_margin_collateral_history: torch.Tensor | None = None
    final_short_sale_collateral: torch.Tensor | None = None
    final_short_margin_collateral: torch.Tensor | None = None


def _split_signed(value: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
    """Return ``(negative, positive)`` while preserving ``negative + positive``.

    Calling ``clamp_max(0)`` and ``clamp_min(0)`` independently is numerically
    correct in the forward pass, but PyTorch assigns a derivative to *both*
    branches at zero.  Recombining those independent branches then doubles the
    derivative exactly when a target already equals the current position.  A
    shared negative part and a residual positive part preserve the algebraic
    identity and its unit derivative, including at the kink.
    """

    negative = torch.minimum(value, torch.zeros_like(value))
    positive = value - negative
    return negative, positive


def _as_fee_vector(
    value: torch.Tensor,
    *,
    reference: torch.Tensor,
    name: str,
) -> torch.Tensor:
    result = value.to(device=reference.device, dtype=reference.dtype)
    if result.dim() != 1 or int(result.numel()) != int(reference.size(1)):
        raise ValueError(
            f"{name} must be one-dimensional with one value per symbol: "
            f"expected ({int(reference.size(1))},), got {tuple(result.shape)}"
        )
    _validate_finite_nonnegative_eager(result, name)
    return result


def _initial_scalar(
    value: torch.Tensor | None,
    *,
    reference: torch.Tensor,
    default: float,
    name: str,
    dtype: torch.dtype | None = None,
    detach: bool = True,
) -> torch.Tensor:
    if value is None:
        return torch.full(
            (),
            float(default),
            device=reference.device,
            dtype=dtype if dtype is not None else reference.dtype,
        )
    source = value.detach() if detach else value
    if source.dim() != 0:
        raise ValueError(f"{name} must be a scalar tensor, got {tuple(source.shape)}")
    return (
        source
        .clone(memory_format=torch.contiguous_format)
        .to(
            device=reference.device,
            dtype=dtype if dtype is not None else reference.dtype,
        )
        .reshape(())
    )


def _initial_vector(
    value: torch.Tensor | None,
    *,
    reference: torch.Tensor,
    size: int,
    detach: bool = True,
) -> torch.Tensor:
    if value is None:
        return torch.zeros((size,), device=reference.device, dtype=reference.dtype)
    result = (
        (value.detach() if detach else value)
        .clone(memory_format=torch.contiguous_format)
        .to(device=reference.device, dtype=reference.dtype)
    )
    if result.dim() != 1 or int(result.numel()) != size:
        raise ValueError(f"settlement state must have shape ({size},), got {tuple(result.shape)}")
    _validate_finite_nonnegative_eager(result, "settlement state")
    return result


def _prepare_common_state(
    target_weights: torch.Tensor,
    *,
    settlement_lag_sessions: int,
    initial_weights: torch.Tensor | None,
    initial_cash: torch.Tensor | None,
    initial_payables: torch.Tensor | None,
    initial_receivables: torch.Tensor | None,
    initial_alive: torch.Tensor | None,
    state_advance_mask: torch.Tensor | None,
    detach_initial_state: bool = True,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    if target_weights.dim() != 2 or int(target_weights.size(0)) <= 0:
        raise ValueError("target_weights must be non-empty with shape [T, S]")
    if isinstance(settlement_lag_sessions, bool) or int(settlement_lag_sessions) != settlement_lag_sessions:
        raise ValueError("settlement_lag_sessions must be a positive integer")
    lag = int(settlement_lag_sessions)
    if lag <= 0:
        raise ValueError("settlement_lag_sessions must be a positive integer")

    if initial_weights is None:
        risky = torch.zeros_like(target_weights[0])
    else:
        risky = (
            (initial_weights.detach() if detach_initial_state else initial_weights)
            .clone(memory_format=torch.contiguous_format)
            .to(device=target_weights.device, dtype=target_weights.dtype)
        )
        if tuple(risky.shape) != (int(target_weights.size(1)),):
            raise ValueError(
                "initial_weights must contain one value per symbol: "
                f"expected ({int(target_weights.size(1))},), got {tuple(risky.shape)}"
            )
    payables = _initial_vector(
        initial_payables,
        reference=target_weights,
        size=lag,
        detach=detach_initial_state,
    )
    receivables = _initial_vector(
        initial_receivables,
        reference=target_weights,
        size=lag,
        detach=detach_initial_state,
    )
    alive = _initial_scalar(
        initial_alive,
        reference=target_weights,
        default=1.0,
        name="initial_alive",
        dtype=torch.bool,
        detach=detach_initial_state,
    )
    if initial_cash is None:
        # A caller that supplies only carried risky weights must not receive a
        # second unit of cash.  Infer the residual of the normalized accounting
        # identity; a brand-new all-zero state still starts with cash == 1.
        cash = 1.0 - risky.sum() - receivables.sum() + payables.sum()
        cash = torch.where(alive, cash, torch.zeros_like(cash))
    else:
        cash = _initial_scalar(
            initial_cash,
            reference=target_weights,
            default=1.0,
            name="initial_cash",
            detach=detach_initial_state,
        )
    _validate_finite_nonnegative_eager(cash, "initial_cash")
    if not torch.compiler.is_compiling():
        if not bool(torch.isfinite(risky).all().item()):
            raise ValueError("initial_weights must be finite")
        if not bool((risky >= 0.0).all().item()):
            raise ValueError("initial_weights must be non-negative for Taiwan cash state")
    _validate_normalized_state_eager(
        risky=risky,
        cash=cash,
        payables=payables,
        receivables=receivables,
        alive=alive,
    )
    if state_advance_mask is None:
        advance = torch.ones(
            (target_weights.size(0),),
            device=target_weights.device,
            dtype=torch.bool,
        )
    else:
        advance = state_advance_mask.to(device=target_weights.device, dtype=torch.bool)
        if tuple(advance.shape) != (int(target_weights.size(0)),):
            raise ValueError(
                "state_advance_mask must have shape [T]: "
                f"expected ({int(target_weights.size(0))},), got {tuple(advance.shape)}"
            )
    return risky, cash, payables, receivables, alive, advance


def _prepare_cash_short_state(
    target_weights: torch.Tensor,
    *,
    settlement_lag_sessions: int,
    claim_queue_sessions: int,
    initial_weights: torch.Tensor | None,
    initial_cash: torch.Tensor | None,
    initial_payables: torch.Tensor | None,
    initial_receivables: torch.Tensor | None,
    initial_alive: torch.Tensor | None,
    initial_short_sale_collateral: torch.Tensor | None,
    initial_short_margin_collateral: torch.Tensor | None,
    state_advance_mask: torch.Tensor | None,
    detach_initial_state: bool,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    """Prepare a signed cash/margin account without fabricating naked shorts."""

    if (initial_short_sale_collateral is None) != (
        initial_short_margin_collateral is None
    ):
        raise ValueError(
            "initial_short_sale_collateral and "
            "initial_short_margin_collateral must be supplied together"
        )

    if target_weights.dim() != 2 or int(target_weights.size(0)) <= 0:
        raise ValueError("target_weights must be non-empty with shape [T, S]")
    if (
        isinstance(settlement_lag_sessions, bool)
        or int(settlement_lag_sessions) != settlement_lag_sessions
        or int(settlement_lag_sessions) <= 0
    ):
        raise ValueError("settlement_lag_sessions must be a positive integer")
    lag = int(settlement_lag_sessions)
    if (
        isinstance(claim_queue_sessions, bool)
        or int(claim_queue_sessions) != claim_queue_sessions
        or int(claim_queue_sessions) < lag
    ):
        raise ValueError(
            "claim_queue_sessions must be an integer at least as large as "
            "settlement_lag_sessions"
        )
    claim_queue = int(claim_queue_sessions)
    symbols = int(target_weights.size(1))

    if initial_weights is None:
        risky = torch.zeros_like(target_weights[0])
    else:
        risky = (
            (initial_weights.detach() if detach_initial_state else initial_weights)
            .clone(memory_format=torch.contiguous_format)
            .to(device=target_weights.device, dtype=target_weights.dtype)
        )
        if tuple(risky.shape) != (symbols,):
            raise ValueError(
                "initial_weights must contain one value per symbol: "
                f"expected ({symbols},), got {tuple(risky.shape)}"
            )
    if not torch.compiler.is_compiling() and not bool(torch.isfinite(risky).all().item()):
        raise ValueError("initial_weights must be finite")

    payables = _initial_vector(
        initial_payables,
        reference=target_weights,
        size=lag,
        detach=detach_initial_state,
    )
    receivables = _initial_vector(
        initial_receivables,
        reference=target_weights,
        size=claim_queue,
        detach=detach_initial_state,
    )
    short_sale = _initial_vector(
        initial_short_sale_collateral,
        reference=target_weights,
        size=symbols,
        detach=detach_initial_state,
    )
    short_margin = _initial_vector(
        initial_short_margin_collateral,
        reference=target_weights,
        size=symbols,
        detach=detach_initial_state,
    )
    alive = _initial_scalar(
        initial_alive,
        reference=target_weights,
        default=1.0,
        name="initial_alive",
        dtype=torch.bool,
        detach=detach_initial_state,
    )

    if not torch.compiler.is_compiling():
        short_positions = risky < -1.0e-12
        complete_collateral = (short_sale > 1.0e-12) & (short_margin > 1.0e-12)
        supplied_collateral = (short_sale > 1.0e-12) | (short_margin > 1.0e-12)
        if bool((short_positions & ~complete_collateral).any().item()):
            raise ValueError(
                "negative initial_weights require nonzero per-symbol "
                "short-sale and short-margin collateral; naked or "
                "single-pool carried shorts are invalid"
            )
        if bool((~short_positions.any() & supplied_collateral.any()).item()):
            raise ValueError(
                "short collateral requires at least one carried short position"
            )

    if initial_cash is None:
        cash = (
            1.0
            - risky.sum()
            - short_sale.sum()
            - short_margin.sum()
            - receivables.sum()
            + payables.sum()
        )
        cash = torch.where(alive, cash, torch.zeros_like(cash))
    else:
        cash = _initial_scalar(
            initial_cash,
            reference=target_weights,
            default=1.0,
            name="initial_cash",
            detach=detach_initial_state,
        )
    _validate_finite_nonnegative_eager(cash, "initial_cash")
    _validate_normalized_state_eager(
        risky=risky,
        cash=cash,
        payables=payables,
        receivables=receivables,
        alive=alive,
        short_sale_collateral=short_sale,
        short_margin_collateral=short_margin,
    )

    if state_advance_mask is None:
        advance = torch.ones(
            (target_weights.size(0),),
            device=target_weights.device,
            dtype=torch.bool,
        )
    else:
        advance = state_advance_mask.to(device=target_weights.device, dtype=torch.bool)
        if tuple(advance.shape) != (int(target_weights.size(0)),):
            raise ValueError(
                "state_advance_mask must have shape [T]: "
                f"expected ({int(target_weights.size(0))},), got {tuple(advance.shape)}"
            )
    return risky, cash, payables, receivables, short_sale, short_margin, alive, advance


def _as_time_symbol_rate(
    value: torch.Tensor | float | None,
    *,
    reference: torch.Tensor,
    default: float,
    name: str,
) -> torch.Tensor:
    """Broadcast a scalar/[S]/[T,S] contract parameter without material copies."""

    if value is None:
        result = reference.new_full((), float(default)).expand_as(reference)
    else:
        result = torch.as_tensor(value, device=reference.device, dtype=reference.dtype)
        if result.dim() == 0:
            result = result.expand_as(reference)
        elif result.dim() == 1 and int(result.numel()) == int(reference.size(1)):
            result = result.unsqueeze(0).expand_as(reference)
        elif tuple(result.shape) != tuple(reference.shape):
            raise ValueError(
                f"{name} must be scalar, [S], or [T,S]; got {tuple(result.shape)}"
            )
    _validate_finite_nonnegative_eager(result, name)
    return result


def _prepare_equity_scale(
    target_weights: torch.Tensor,
    *,
    initial_equity_scale: torch.Tensor | None,
    alive: torch.Tensor,
    detach_initial_state: bool = True,
) -> torch.Tensor:
    """Return absolute NAV as a ratio of the configured reference equity.

    ``volume_limit_weights`` is built from a causal share cap valued at the
    execution price and divided by a fixed reference equity.  Portfolio weights,
    however, are fractions of *current* NAV.  Carrying this one scalar therefore
    is necessary to convert that fixed-reference cap into the correct current-NAV
    weight on every row and across compiled chunks.
    """

    equity_scale = _initial_scalar(
        initial_equity_scale,
        reference=target_weights,
        default=1.0,
        name="initial_equity_scale",
        detach=detach_initial_state,
    )
    if initial_equity_scale is None:
        equity_scale = torch.where(alive, equity_scale, torch.zeros_like(equity_scale))
    _validate_finite_nonnegative_eager(equity_scale, "initial_equity_scale")
    if not torch.compiler.is_compiling():
        alive_value = bool(alive.detach().item())
        scale_value = float(equity_scale.detach().item())
        if alive_value and scale_value <= 0.0:
            raise ValueError("a live Taiwan settlement state requires positive equity scale")
        if not alive_value and scale_value != 0.0:
            raise ValueError("an absorbing dead Taiwan settlement state requires zero equity scale")
    return equity_scale


def _advance_equity_scale(
    equity_scale: torch.Tensor,
    simple_return: torch.Tensor,
    *,
    advance: torch.Tensor,
    alive: torch.Tensor,
) -> torch.Tensor:
    """Compound absolute equity while preserving padding and absorbing ruin."""

    compounded = equity_scale * (1.0 + simple_return)
    next_scale = torch.where(advance, compounded, equity_scale)
    return torch.where(alive, next_scale, torch.zeros_like(next_scale))


def _cap_current_nav_request_from_reference_notional(
    requested: torch.Tensor,
    reference_cap: torch.Tensor,
    equity_scale: torch.Tensor,
) -> torch.Tensor:
    """Cap a current-NAV request using a fixed-reference notional limit.

    The direct expression ``reference_cap / equity_scale`` is algebraically
    simple but unsafe for reverse-mode AD: a dead account has equity scale zero,
    so even an ultimately unselected branch can overflow and produce
    ``0 * inf == nan`` in ``DivBackward``.  Compare in fixed-reference units
    first and divide only on cells where the cap genuinely binds.  On those
    cells ``equity_scale * requested > reference_cap >= 0``, which proves the
    selected denominator is positive and the quotient cannot exceed the finite
    request.
    """

    valid_cap = torch.isfinite(reference_cap) & (reference_cap >= 0.0)
    requested_reference_notional = requested * equity_scale
    cap_binds = valid_cap & (reference_cap < requested_reference_notional)
    safe_cap = torch.where(
        cap_binds,
        reference_cap,
        torch.zeros_like(reference_cap),
    )
    safe_equity_scale = torch.where(
        cap_binds,
        equity_scale,
        torch.ones_like(requested_reference_notional),
    )
    capped_when_binding = safe_cap / safe_equity_scale
    capped = torch.where(cap_binds, capped_when_binding, requested)
    return torch.where(valid_cap, capped, torch.zeros_like(requested))


def _settle_open_phase(
    cash: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
    alive: torch.Tensor,
    advance: torch.Tensor,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    """Pay before receipts, then advance the fixed-length session queues."""

    due_payable = payables[0]
    due_receivable = receivables[0]
    tolerance = cash.new_tensor(torch.finfo(cash.dtype).eps * 8.0) * torch.maximum(
        torch.ones_like(cash),
        torch.maximum(cash.abs(), due_payable.abs()),
    )
    can_pay = cash + tolerance >= due_payable
    default_now = advance & alive & ~can_pay
    survived = alive & ~default_now

    # Admission control guarantees funding.  Project a within-roundoff
    # residual to zero so an accepted payment cannot manufacture a tiny
    # negative cash balance and a false default on the next zero-due session.
    settled_cash = (cash - due_payable).clamp_min(0.0) + due_receivable
    zero = torch.zeros_like(due_payable).unsqueeze(0)
    shifted_payables = torch.cat((payables[1:], zero), dim=0)
    shifted_receivables = torch.cat((receivables[1:], zero), dim=0)

    cash = torch.where(advance & survived, settled_cash, cash)
    payables = torch.where(advance & survived, shifted_payables, payables)
    receivables = torch.where(advance & survived, shifted_receivables, receivables)
    cash = torch.where(survived, cash, torch.zeros_like(cash))
    payables = torch.where(survived, payables, torch.zeros_like(payables))
    receivables = torch.where(survived, receivables, torch.zeros_like(receivables))
    return cash, payables, receivables, survived, default_now


def _enqueue_net_claim(
    payables: torch.Tensor,
    receivables: torch.Tensor,
    net_receivable: torch.Tensor,
    *,
    settlement_lag_sessions: int | None = None,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Append exactly one account-level signed claim for a trade session."""

    negative, positive = _split_signed(net_receivable)
    new_receivable = positive
    new_payable = -negative
    payables = torch.cat((payables[:-1], new_payable.unsqueeze(0)), dim=0)
    lag = int(payables.numel()) if settlement_lag_sessions is None else int(
        settlement_lag_sessions
    )
    if lag <= 0 or lag > int(receivables.numel()):
        raise ValueError(
            "settlement_lag_sessions must address the receivable claim queue"
        )
    insertion = torch.nn.functional.one_hot(
        torch.tensor(lag - 1, device=receivables.device),
        num_classes=int(receivables.numel()),
    ).to(dtype=receivables.dtype)
    receivables = receivables + insertion * new_receivable
    return payables, receivables


def _capacity_for_new_payable(
    cash: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
) -> torch.Tensor:
    """Project settled cash session-by-session until a new claim becomes due.

    A simple ``cash + receivables.sum() - payables.sum()`` is valid only when
    every intermediate payable is already known to be fundable.  Explicitly
    checking each pre-receipt phase keeps arbitrary configured lags and injected
    recurrent states correct while retaining a fixed, compile-visible loop.
    """

    projected = cash
    solvent = torch.ones_like(cash, dtype=torch.bool)
    one = torch.ones_like(cash)
    for index in range(max(int(payables.numel()) - 1, 0)):
        due = payables[index]
        tolerance = cash.new_tensor(torch.finfo(cash.dtype).eps * 8.0) * torch.maximum(
            one,
            torch.maximum(projected.abs(), due.abs()),
        )
        can_pay = solvent & (projected + tolerance >= due)
        projected = torch.where(
            can_pay,
            (projected - due).clamp_min(0.0) + receivables[index],
            projected,
        )
        solvent = can_pay
    capacity = (projected - payables[-1]).clamp_min(0.0)
    return torch.where(solvent, capacity, torch.zeros_like(capacity))


def _finalize_day(
    *,
    nav_start: torch.Tensor,
    nav_end: torch.Tensor,
    risky_end: torch.Tensor,
    cash: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
    alive: torch.Tensor,
    advance: torch.Tensor,
    default_now: torch.Tensor,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    finite_positive = torch.isfinite(nav_end) & (nav_end > _MIN_WEALTH_FACTOR)
    nav_ruin_now = advance & alive & ~finite_positive
    newly_dead = default_now | nav_ruin_now
    survived = alive & (~advance | finite_positive)
    live_advance = advance & survived
    safe_nav_start = torch.where(
        live_advance,
        nav_start.clamp_min(_MIN_WEALTH_FACTOR),
        torch.ones_like(nav_start),
    )
    safe_nav_end = torch.where(live_advance, nav_end, safe_nav_start)
    simple_return = safe_nav_end / safe_nav_start - 1.0
    ruin_return = simple_return.new_tensor(-1.0 + _MIN_WEALTH_FACTOR)
    # An absorbing dead account produces the ruin return exactly once.  Later
    # real sessions (and synthetic padding) are recurrent no-ops.
    simple_return = torch.where(newly_dead, ruin_return, simple_return)

    safe_denominator = torch.where(
        live_advance,
        nav_end,
        torch.ones_like(nav_end),
    )

    def normalize(value: torch.Tensor) -> torch.Tensor:
        safe_value = torch.where(live_advance, value, torch.zeros_like(value))
        normalized = safe_value / safe_denominator
        normalized = torch.where(survived, normalized, torch.zeros_like(normalized))
        return torch.where(advance, normalized, value)

    risky_normalized = normalize(risky_end)
    cash_normalized = normalize(cash)
    payables_normalized = normalize(payables)
    receivables_normalized = normalize(receivables)
    survived = torch.where(advance, survived, alive)
    return (
        simple_return,
        risky_normalized,
        cash_normalized,
        payables_normalized,
        receivables_normalized,
        survived,
        newly_dead,
    )


def _finalize_cash_day(
    *,
    nav_start: torch.Tensor,
    nav_end: torch.Tensor,
    risky_end: torch.Tensor,
    cash: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
    short_sale_collateral: torch.Tensor,
    short_margin_collateral: torch.Tensor,
    alive: torch.Tensor,
    advance: torch.Tensor,
    default_now: torch.Tensor,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    """Normalize the complete cash-plus-margin ledger after one price move."""

    finite_positive = torch.isfinite(nav_end) & (nav_end > _MIN_WEALTH_FACTOR)
    nav_ruin_now = advance & alive & ~finite_positive
    newly_dead = default_now | nav_ruin_now
    survived = alive & (~advance | finite_positive)
    live_advance = advance & survived
    safe_nav_start = torch.where(
        live_advance,
        nav_start.clamp_min(_MIN_WEALTH_FACTOR),
        torch.ones_like(nav_start),
    )
    safe_nav_end = torch.where(live_advance, nav_end, safe_nav_start)
    simple_return = safe_nav_end / safe_nav_start - 1.0
    simple_return = torch.where(
        newly_dead,
        simple_return.new_tensor(-1.0 + _MIN_WEALTH_FACTOR),
        simple_return,
    )

    def normalize(value: torch.Tensor) -> torch.Tensor:
        safe_value = torch.where(live_advance, value, torch.zeros_like(value))
        safe_denominator = torch.where(
            live_advance,
            nav_end,
            torch.ones_like(nav_end),
        )
        normalized = safe_value / safe_denominator
        normalized = torch.where(survived, normalized, torch.zeros_like(normalized))
        return torch.where(advance, normalized, value)

    risky_normalized = normalize(risky_end)
    cash_normalized = normalize(cash)
    payables_normalized = normalize(payables)
    receivables_normalized = normalize(receivables)
    short_sale_normalized = normalize(short_sale_collateral)
    short_margin_normalized = normalize(short_margin_collateral)
    survived = torch.where(advance, survived, alive)
    return (
        simple_return,
        risky_normalized,
        cash_normalized,
        payables_normalized,
        receivables_normalized,
        short_sale_normalized,
        short_margin_normalized,
        survived,
        newly_dead,
    )


def _run_tw_cash_continuous_impl(
    target_weights: torch.Tensor,
    asset_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    *,
    can_short_open_mask: torch.Tensor | None = None,
    force_short_cover_mask: torch.Tensor | None = None,
    short_margin_rate: torch.Tensor | float | None = None,
    short_capacity_weights: torch.Tensor | None = None,
    short_handling_fee_rate: torch.Tensor | float = 0.0,
    short_maintenance_ratio: float = 1.30,
    unresolved_corporate_action_mask: torch.Tensor | None = None,
    cash_dividend_yield: torch.Tensor | None = None,
    cash_dividend_payment_delay_sessions: torch.Tensor | None = None,
    claim_queue_sessions: int | None = None,
    settlement_lag_sessions: int = 2,
    gross_budget: float = 1.0,
    max_turnover_ratio: float = 0.0,
    volume_limit_weights: torch.Tensor | None = None,
    force_exit_mask: torch.Tensor | None = None,
    state_advance_mask: torch.Tensor | None = None,
    return_weights_history: bool = True,
    initial_weights: torch.Tensor | None = None,
    initial_cash: torch.Tensor | None = None,
    initial_payables: torch.Tensor | None = None,
    initial_receivables: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_short_sale_collateral: torch.Tensor | None = None,
    initial_short_margin_collateral: torch.Tensor | None = None,
    _detach_initial_state: bool = True,
) -> TaiwanContinuousResult:
    """Taiwan cash plus credit-short ledger with T+2 net settlement.

    Positive risky values are fully paid cash holdings. Negative risky values
    are credit-short liabilities. Their net sale proceeds and client margin are
    restricted per-symbol collateral, never free buying power. Every row costs
    O(S + Q), where Q is the fixed claim-queue horizon; with bounded Q this
    matches the unavoidable O(T*S) per-symbol execution lower bound.
    """

    if can_short_open_mask is not None:
        if short_margin_rate is None:
            raise ValueError(
                "can_short_open_mask requires an explicit point-in-time "
                "short_margin_rate"
            )
        if short_capacity_weights is None:
            raise ValueError(
                "can_short_open_mask requires explicit point-in-time "
                "short_capacity_weights"
            )
    maintenance_ratio_value = _resolve_short_maintenance_ratio(
        short_maintenance_ratio
    )

    if unresolved_corporate_action_mask is None:
        raise ValueError(
            "tw_cash requires the official corporate-action avoidance mask; "
            "real-share execution cannot silently treat total returns as "
            "reinvested cash/shares"
        )
    has_cash_dividends = cash_dividend_yield is not None
    if has_cash_dividends != (cash_dividend_payment_delay_sessions is not None):
        raise ValueError(
            "cash_dividend_yield and cash_dividend_payment_delay_sessions "
            "must be supplied together"
        )
    if claim_queue_sessions is None:
        resolved_claim_queue_sessions = int(settlement_lag_sessions)
    else:
        if isinstance(claim_queue_sessions, bool) or not isinstance(
            claim_queue_sessions, int
        ):
            raise ValueError(
                "claim_queue_sessions must be an integer at least as large as "
                "settlement_lag_sessions"
            )
        resolved_claim_queue_sessions = claim_queue_sessions

    if tuple(asset_log_returns.shape) != tuple(target_weights.shape):
        raise ValueError("asset_log_returns shape must match target_weights")
    if tuple(tradable_mask.shape) != tuple(target_weights.shape):
        raise ValueError("tradable_mask shape must match target_weights")
    for name, value in (
        ("can_buy_mask", can_buy_mask),
        ("can_sell_mask", can_sell_mask),
    ):
        if tuple(value.shape) != tuple(target_weights.shape):
            raise ValueError(f"{name} shape must match target_weights")
    buy_fees = _as_fee_vector(buy_fee_rates, reference=target_weights, name="buy_fee_rates")
    sell_fees = _as_fee_vector(sell_fee_rates, reference=target_weights, name="sell_fee_rates")
    # ``tradable_mask`` is the causal model-selection universe (prior-alive for
    # TW cash), while these are current-close broker execution facts.  A held
    # name that resumes trading must remain reducible even if it was not
    # selectable from yesterday's information.
    buy_mask = can_buy_mask.to(
        device=target_weights.device,
        dtype=torch.bool,
    )
    sell_mask = can_sell_mask.to(
        device=target_weights.device,
        dtype=torch.bool,
    )
    if can_short_open_mask is None:
        short_open_mask = torch.zeros_like(sell_mask)
    else:
        if tuple(can_short_open_mask.shape) != tuple(target_weights.shape):
            raise ValueError("can_short_open_mask shape must match target_weights")
        short_open_mask = (
            can_short_open_mask.to(device=target_weights.device, dtype=torch.bool)
            & sell_mask
        )
    if force_short_cover_mask is None:
        forced_cover = torch.zeros_like(buy_mask)
    else:
        if tuple(force_short_cover_mask.shape) != tuple(target_weights.shape):
            raise ValueError("force_short_cover_mask shape must match target_weights")
        forced_cover = force_short_cover_mask.to(
            device=target_weights.device,
            dtype=torch.bool,
        )
    if force_exit_mask is not None and tuple(force_exit_mask.shape) != tuple(
        target_weights.shape
    ):
        raise ValueError("force_exit_mask shape must match target_weights")
    terminal = (
        torch.zeros_like(buy_mask)
        if force_exit_mask is None
        else force_exit_mask.to(device=target_weights.device, dtype=torch.bool)
    )
    if volume_limit_weights is not None and tuple(volume_limit_weights.shape) != tuple(target_weights.shape):
        raise ValueError("volume_limit_weights shape must match target_weights")
    if short_capacity_weights is not None and tuple(short_capacity_weights.shape) != tuple(target_weights.shape):
        raise ValueError("short_capacity_weights shape must match target_weights")
    if tuple(unresolved_corporate_action_mask.shape) != tuple(target_weights.shape):
        raise ValueError(
            "unresolved_corporate_action_mask shape must match target_weights"
        )
    unresolved_actions = unresolved_corporate_action_mask.to(
        device=target_weights.device,
        dtype=torch.bool,
    )
    if has_cash_dividends:
        if tuple(cash_dividend_yield.shape) != tuple(target_weights.shape):
            raise ValueError("cash_dividend_yield shape must match target_weights")
        if tuple(cash_dividend_payment_delay_sessions.shape) != tuple(
            target_weights.shape
        ):
            raise ValueError(
                "cash_dividend_payment_delay_sessions shape must match "
                "target_weights"
            )
        dividend_yields = cash_dividend_yield.to(
            device=target_weights.device,
            dtype=target_weights.dtype,
        )
        dividend_delays = cash_dividend_payment_delay_sessions.to(
            device=target_weights.device,
            dtype=torch.int64,
        )
        if not torch.compiler.is_compiling():
            if not bool(torch.isfinite(dividend_yields).all().item()) or bool(
                (dividend_yields < 0.0).any().item()
            ):
                raise ValueError("cash_dividend_yield must be finite and non-negative")
            event_cells = dividend_yields > 0.0
            invalid_delay = event_cells & (
                (dividend_delays < 1)
                | (dividend_delays > resolved_claim_queue_sessions)
            )
            if bool(invalid_delay.any().item()):
                raise ValueError(
                    "cash-dividend payment delay exceeds the configured claim queue"
                )
            if bool(((~event_cells) & (dividend_delays != 0)).any().item()):
                raise ValueError(
                    "cash-dividend payment delay must be zero when yield is zero"
                )
    else:
        dividend_yields = torch.zeros_like(target_weights)
        dividend_delays = torch.zeros_like(target_weights, dtype=torch.int64)

    margin_rates = _as_time_symbol_rate(
        short_margin_rate,
        reference=target_weights,
        default=0.90,
        name="short_margin_rate",
    )
    short_handling_rates = _as_time_symbol_rate(
        short_handling_fee_rate,
        reference=target_weights,
        default=0.0,
        name="short_handling_fee_rate",
    )
    if not torch.compiler.is_compiling() and bool(short_open_mask.any().item()):
        combined_short_sale_cost = sell_fees + short_handling_rates.amax(dim=0)
        if bool((combined_short_sale_cost >= 1.0).any().item()):
            raise ValueError("short sell fees must leave positive locked sale proceeds")

    (
        risky,
        cash,
        payables,
        receivables,
        short_sale_collateral,
        short_margin_collateral,
        alive,
        advance_mask,
    ) = _prepare_cash_short_state(
        target_weights,
        settlement_lag_sessions=settlement_lag_sessions,
        claim_queue_sessions=resolved_claim_queue_sessions,
        initial_weights=initial_weights,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_alive=initial_alive,
        initial_short_sale_collateral=initial_short_sale_collateral,
        initial_short_margin_collateral=initial_short_margin_collateral,
        state_advance_mask=state_advance_mask,
        detach_initial_state=_detach_initial_state,
    )
    equity_scale = _prepare_equity_scale(
        target_weights,
        initial_equity_scale=initial_equity_scale,
        alive=alive,
        detach_initial_state=_detach_initial_state,
    )
    raw_asset_log_returns = asset_log_returns.to(
        device=target_weights.device,
        dtype=target_weights.dtype,
    )
    finite_log_return = torch.isfinite(raw_asset_log_returns)
    simple_asset_returns_unchecked = torch.expm1(
        torch.where(
            finite_log_return,
            raw_asset_log_returns,
            torch.zeros_like(raw_asset_log_returns),
        )
    )
    valid_asset_return = finite_log_return & torch.isfinite(
        simple_asset_returns_unchecked
    )
    simple_asset_returns = torch.where(
        valid_asset_return,
        simple_asset_returns_unchecked,
        torch.zeros_like(simple_asset_returns_unchecked),
    )
    t_len, n_symbols = target_weights.shape
    weight_rows: list[torch.Tensor] = []
    strategy_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    cash_rows: list[torch.Tensor] = []
    payable_rows: list[torch.Tensor] = []
    receivable_rows: list[torch.Tensor] = []
    short_sale_rows: list[torch.Tensor] = []
    short_margin_rows: list[torch.Tensor] = []
    default_rows: list[torch.Tensor] = []
    equity_scale_rows: list[torch.Tensor] = []
    turnover_cap = target_weights.new_tensor(float(max_turnover_ratio))
    gross_cap = target_weights.new_tensor(float(gross_budget)).clamp(min=0.0, max=1.0)
    maintenance_ratio = target_weights.new_tensor(maintenance_ratio_value)
    gradient_horizon = _settlement_gradient_horizon_rows()

    for idx in range(int(t_len)):
        if gradient_horizon > 0 and idx > 0 and idx % gradient_horizon == 0:
            risky = _restart_recurrent_gradient(risky, requires_grad=False)
            cash = _restart_recurrent_gradient(cash, requires_grad=False)
            payables = _restart_recurrent_gradient(payables, requires_grad=False)
            receivables = _restart_recurrent_gradient(
                receivables,
                requires_grad=False,
            )
            short_sale_collateral = _restart_recurrent_gradient(
                short_sale_collateral,
                requires_grad=False,
            )
            short_margin_collateral = _restart_recurrent_gradient(
                short_margin_collateral,
                requires_grad=False,
            )
            alive = _restart_recurrent_gradient(alive, requires_grad=False)
            equity_scale = _restart_recurrent_gradient(
                equity_scale,
                requires_grad=False,
            )
        advance = advance_mask[idx]
        risky_before = risky
        cash, payables, receivables, alive_after_settlement, default_now = _settle_open_phase(
            cash,
            payables,
            receivables,
            alive,
            advance,
        )
        risky_before = torch.where(alive_after_settlement, risky_before, torch.zeros_like(risky_before))
        short_sale_collateral = torch.where(
            alive_after_settlement,
            short_sale_collateral,
            torch.zeros_like(short_sale_collateral),
        )
        short_margin_collateral = torch.where(
            alive_after_settlement,
            short_margin_collateral,
            torch.zeros_like(short_margin_collateral),
        )
        nav_start = (
            cash
            + risky_before.sum()
            + short_sale_collateral.sum()
            + short_margin_collateral.sum()
            + receivables.sum()
            - payables.sum()
        )

        # The source mask is aligned to the exchange session immediately before
        # an official ex-date.  Because the source does not include entitlement
        # payment dates, the only exact no-fabrication policy is to leave the
        # position at this close and forbid a new entry.  Unlike a terminal
        # lifecycle exit, this sale may not bypass an unavailable sell side.
        action_avoidance = unresolved_actions[idx] & advance & alive_after_settlement
        action_exit = action_avoidance & ~terminal[idx] & (risky_before.abs() > 1.0e-12)
        blocked_action_exit = action_exit & torch.where(
            risky_before >= 0.0,
            ~sell_mask[idx],
            ~buy_mask[idx],
        )
        torch._assert_async(
            ~blocked_action_exit.any(),
            "tw_cash cannot liquidate a held position before an official "
            "corporate action because the sell side is unavailable (or the "
            "buy side is unavailable for a short position)",
        )
        mandatory_exit = terminal[idx] | action_avoidance
        current_long = risky_before.clamp_min(0.0)
        current_short = (-risky_before).clamp_min(0.0)
        mandatory_long_sell = torch.where(
            mandatory_exit,
            current_long,
            torch.zeros_like(current_long),
        )
        mandatory_short_cover = torch.where(
            mandatory_exit | forced_cover[idx],
            current_short,
            torch.zeros_like(current_short),
        )
        blocked_mandatory_short_cover = (
            mandatory_short_cover > 1.0e-12
        ) & ~buy_mask[idx]
        torch._assert_async(
            ~blocked_mandatory_short_cover.any(),
            "tw_cash mandatory short cover cannot execute because the buy "
            "side is unavailable",
        )
        base = risky_before - mandatory_long_sell + mandatory_short_cover
        desired = torch.where(
            advance
            & alive_after_settlement
            & tradable_mask[idx].to(dtype=torch.bool)
            & ~mandatory_exit,
            target_weights[idx],
            torch.zeros_like(base),
        )
        # A mandatory cover forbids re-opening the short, but does not suppress
        # a valid same-day discretionary long request.
        desired = torch.where(forced_cover[idx], desired.clamp_min(0.0), desired)

        base_long = base.clamp_min(0.0)
        base_short = (-base).clamp_min(0.0)
        desired_long = desired.clamp_min(0.0)
        desired_short = (-desired).clamp_min(0.0)
        voluntary_long_sell = (base_long - desired_long).clamp_min(0.0)
        voluntary_long_buy = (desired_long - base_long).clamp_min(0.0)
        voluntary_short_cover = (base_short - desired_short).clamp_min(0.0)
        voluntary_short_open = (desired_short - base_short).clamp_min(0.0)

        voluntary_long_sell = voluntary_long_sell * sell_mask[idx].to(target_weights.dtype)
        voluntary_long_buy = voluntary_long_buy * buy_mask[idx].to(target_weights.dtype)
        voluntary_short_cover = voluntary_short_cover * buy_mask[idx].to(target_weights.dtype)
        voluntary_short_open = voluntary_short_open * short_open_mask[idx].to(target_weights.dtype)

        if short_capacity_weights is not None:
            raw_short_cap = short_capacity_weights[idx].to(
                device=target_weights.device,
                dtype=target_weights.dtype,
            )
            voluntary_short_open = _cap_current_nav_request_from_reference_notional(
                voluntary_short_open,
                raw_short_cap,
                equity_scale,
            )

        if volume_limit_weights is not None:
            volume_cap = volume_limit_weights[idx].to(
                device=target_weights.device,
                dtype=target_weights.dtype,
            )
            voluntary_notional = (
                voluntary_long_sell
                + voluntary_long_buy
                + voluntary_short_cover
                + voluntary_short_open
            )
            capped_notional = _cap_current_nav_request_from_reference_notional(
                voluntary_notional,
                volume_cap,
                equity_scale,
            )
            cap_is_valid = torch.isfinite(volume_cap) & (volume_cap >= 0.0)
            cap_binds = cap_is_valid & (
                volume_cap < voluntary_notional * equity_scale
            )
            safe_requested = torch.where(
                cap_binds,
                voluntary_notional,
                torch.ones_like(voluntary_notional),
            )
            participation_scale = torch.where(
                cap_is_valid,
                torch.where(
                    cap_binds,
                    capped_notional / safe_requested,
                    torch.ones_like(voluntary_notional),
                ),
                torch.zeros_like(voluntary_notional),
            )
            voluntary_long_sell = voluntary_long_sell * participation_scale
            voluntary_long_buy = voluntary_long_buy * participation_scale
            voluntary_short_cover = voluntary_short_cover * participation_scale
            voluntary_short_open = voluntary_short_open * participation_scale
        if max_turnover_ratio > 0.0:
            voluntary_turnover = (
                voluntary_long_sell
                + voluntary_long_buy
                + voluntary_short_cover
                + voluntary_short_open
            ).sum()
            scale = torch.minimum(
                torch.ones_like(voluntary_turnover),
                turnover_cap / voluntary_turnover.clamp_min(1.0e-12),
            )
            voluntary_long_sell = voluntary_long_sell * scale
            voluntary_long_buy = voluntary_long_buy * scale
            voluntary_short_cover = voluntary_short_cover * scale
            voluntary_short_open = voluntary_short_open * scale

        # Risk reductions execute before risk additions. This prevents a
        # blocked liquidation from being hidden by long/short netting.
        exposure_after_reductions = (
            (base_long - voluntary_long_sell).clamp_min(0.0)
            + (base_short - voluntary_short_cover).clamp_min(0.0)
        ).sum()
        requested_increases = (voluntary_long_buy + voluntary_short_open).sum()
        gross_scale = torch.minimum(
            torch.ones_like(requested_increases),
            (gross_cap - exposure_after_reductions).clamp_min(0.0)
            / requested_increases.clamp_min(1.0e-12),
        )
        voluntary_long_buy = voluntary_long_buy * gross_scale
        voluntary_short_open = voluntary_short_open * gross_scale

        release_sale_per_cover = torch.where(
            current_short > 1.0e-12,
            short_sale_collateral / current_short.clamp_min(1.0e-12),
            torch.zeros_like(current_short),
        )
        release_margin_per_cover = torch.where(
            current_short > 1.0e-12,
            short_margin_collateral / current_short.clamp_min(1.0e-12),
            torch.zeros_like(current_short),
        )
        cover_net_per_unit = (
            release_sale_per_cover
            + release_margin_per_cover
            - (1.0 + buy_fees)
        )
        mandatory_net_claim = (
            mandatory_long_sell * (1.0 - sell_fees)
            + mandatory_short_cover * cover_net_per_unit
        ).sum()
        positive_cover = torch.where(
            cover_net_per_unit >= 0.0,
            voluntary_short_cover,
            torch.zeros_like(voluntary_short_cover),
        )
        consuming_cover = voluntary_short_cover - positive_cover
        producer_claim = (
            voluntary_long_sell * (1.0 - sell_fees)
            + positive_cover * cover_net_per_unit
        ).sum()

        free_cash_at_new_due = _capacity_for_new_payable(
            cash,
            payables,
            receivables,
        )
        consumer_cost = (
            (voluntary_long_buy * (1.0 + buy_fees)).sum()
            + (voluntary_short_open * margin_rates[idx]).sum()
            + (consuming_cover * (-cover_net_per_unit)).sum()
        )
        consumer_capacity = (
            free_cash_at_new_due + mandatory_net_claim + producer_claim
        ).clamp_min(0.0)
        funding_scale = torch.minimum(
            torch.ones_like(consumer_cost),
            consumer_capacity / consumer_cost.clamp_min(1.0e-12),
        )
        voluntary_long_buy = voluntary_long_buy * funding_scale
        voluntary_short_open = voluntary_short_open * funding_scale
        voluntary_short_cover = positive_cover + consuming_cover * funding_scale

        long_sell = mandatory_long_sell + voluntary_long_sell
        long_buy = voluntary_long_buy
        short_cover = mandatory_short_cover + voluntary_short_cover
        short_open = voluntary_short_open
        executed = risky_before - long_sell + long_buy + short_cover - short_open

        cover_fraction = torch.where(
            current_short > 1.0e-12,
            (short_cover / current_short.clamp_min(1.0e-12)).clamp(0.0, 1.0),
            torch.zeros_like(current_short),
        )
        candidate_released_sale = short_sale_collateral * cover_fraction
        candidate_released_margin = short_margin_collateral * cover_fraction
        opened_sale_collateral = short_open * (
            1.0 - sell_fees - short_handling_rates[idx]
        )
        opened_margin_collateral = short_open * margin_rates[idx]
        remaining_short = (-executed).clamp_min(0.0)
        remaining_short_total = remaining_short.sum()
        candidate_release_total = (
            candidate_released_sale.sum() + candidate_released_margin.sum()
        )
        collateral_before_release = (
            short_sale_collateral.sum()
            + short_margin_collateral.sum()
            + opened_sale_collateral.sum()
            + opened_margin_collateral.sum()
        )
        required_remaining_collateral = (
            maintenance_ratio * remaining_short_total
        )
        max_maintenance_release = (
            collateral_before_release - required_remaining_collateral
        ).clamp_min(0.0)
        release_scale = torch.minimum(
            torch.ones_like(candidate_release_total),
            max_maintenance_release
            / candidate_release_total.clamp_min(1.0e-12),
        )
        all_shorts_covered = remaining_short_total <= 1.0e-12
        release_scale = torch.where(
            all_shorts_covered,
            torch.ones_like(release_scale),
            release_scale,
        )
        released_sale = torch.where(
            all_shorts_covered,
            short_sale_collateral,
            candidate_released_sale * release_scale,
        )
        released_margin = torch.where(
            all_shorts_covered,
            short_margin_collateral,
            candidate_released_margin * release_scale,
        )
        raw_next_short_sale_collateral = (
            short_sale_collateral - released_sale + opened_sale_collateral
        )
        raw_next_short_margin_collateral = (
            short_margin_collateral - released_margin + opened_margin_collateral
        )
        remaining_allocation = remaining_short / remaining_short_total.clamp_min(
            1.0e-12
        )
        allocated_sale = (
            remaining_allocation * raw_next_short_sale_collateral.sum()
        )
        allocated_margin = (
            remaining_allocation * raw_next_short_margin_collateral.sum()
        )
        orphaned_collateral = (
            (remaining_short <= 1.0e-12)
            & (
                raw_next_short_sale_collateral
                + raw_next_short_margin_collateral
                > 1.0e-12
            )
        ).any()
        next_short_sale_collateral = torch.where(
            orphaned_collateral,
            allocated_sale,
            raw_next_short_sale_collateral,
        )
        next_short_margin_collateral = torch.where(
            orphaned_collateral,
            allocated_margin,
            raw_next_short_margin_collateral,
        )
        partial_cover = (short_cover.sum() > 1.0e-12) & ~all_shorts_covered
        retained_collateral = (
            next_short_sale_collateral.sum()
            + next_short_margin_collateral.sum()
        )
        maintenance_tolerance = target_weights.new_tensor(
            torch.finfo(target_weights.dtype).eps * 16.0
        ) * torch.maximum(
            torch.ones_like(required_remaining_collateral),
            required_remaining_collateral.abs(),
        )
        torch._assert_async(
            ~partial_cover
            | (
                retained_collateral + maintenance_tolerance
                >= required_remaining_collateral
            ),
            "tw_cash partial short cover cannot leave whole-account "
            "restricted collateral below short_maintenance_ratio",
        )
        net_claim = (
            (long_sell * (1.0 - sell_fees)).sum()
            - (long_buy * (1.0 + buy_fees)).sum()
            + released_sale.sum()
            + released_margin.sum()
            - (short_cover * (1.0 + buy_fees)).sum()
            - opened_margin_collateral.sum()
        )
        next_payables, next_receivables = _enqueue_net_claim(
            payables,
            receivables,
            net_claim,
            settlement_lag_sessions=settlement_lag_sessions,
        )
        payables = torch.where(advance & alive_after_settlement, next_payables, payables)
        receivables = torch.where(advance & alive_after_settlement, next_receivables, receivables)
        executed = torch.where(advance & alive_after_settlement, executed, risky_before)
        short_sale_collateral = torch.where(
            advance & alive_after_settlement,
            next_short_sale_collateral,
            short_sale_collateral,
        )
        short_margin_collateral = torch.where(
            advance & alive_after_settlement,
            next_short_margin_collateral,
            short_margin_collateral,
        )
        nav_at_execution = (
            cash
            + executed.sum()
            + short_sale_collateral.sum()
            + short_margin_collateral.sum()
            + receivables.sum()
            - payables.sum()
        )
        execution_weights = torch.where(
            advance & alive_after_settlement,
            executed / nav_at_execution.clamp_min(_MIN_WEALTH_FACTOR),
            risky_before,
        )
        execution_weights = torch.where(
            alive_after_settlement,
            execution_weights,
            torch.zeros_like(execution_weights),
        )
        turnover = torch.where(
            advance & alive_after_settlement,
            (long_sell + long_buy + short_cover + short_open).sum(),
            torch.zeros_like(nav_start),
        )

        residual_action_position = (executed.abs() > 1.0e-12) & action_avoidance
        torch._assert_async(
            ~residual_action_position.any(),
            "tw_cash official corporate-action avoidance liquidation did not "
            "finish at the preceding close",
        )

        active_return = (
            advance
            & alive_after_settlement
            & (executed.abs() > 1.0e-12)
        )
        invalid_active_return = active_return & ~valid_asset_return[idx]
        torch._assert_async(
            ~invalid_active_return.any(),
            "tw_cash has a non-finite close-to-next-close return for an "
            "executed holding",
        )

        effective_asset_return = torch.where(
            advance,
            simple_asset_returns[idx],
            torch.zeros_like(simple_asset_returns[idx]),
        )
        risky_end = executed * (1.0 + effective_asset_return)
        dividend_claims = (
            executed.clamp_min(0.0)
            * dividend_yields[idx]
            * (advance & alive_after_settlement).to(dtype=target_weights.dtype)
        )
        claim_indices = (dividend_delays[idx] - 1).clamp(
            min=0,
            max=resolved_claim_queue_sessions - 1,
        )
        scheduled_dividends = torch.zeros_like(receivables).scatter_add(
            0,
            claim_indices,
            dividend_claims,
        )
        receivables = receivables + scheduled_dividends
        nav_end = (
            cash
            + risky_end.sum()
            + short_sale_collateral.sum()
            + short_margin_collateral.sum()
            + receivables.sum()
            - payables.sum()
        )
        (
            simple_return,
            risky,
            cash,
            payables,
            receivables,
            short_sale_collateral,
            short_margin_collateral,
            alive,
            default_event,
        ) = _finalize_cash_day(
            nav_start=nav_start,
            nav_end=nav_end,
            risky_end=risky_end,
            cash=cash,
            payables=payables,
            receivables=receivables,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            alive=alive_after_settlement,
            advance=advance,
            default_now=default_now,
        )
        equity_scale = _advance_equity_scale(
            equity_scale,
            simple_return,
            advance=advance,
            alive=alive,
        )
        execution_weights = torch.where(
            alive,
            execution_weights,
            torch.zeros_like(execution_weights),
        )
        strategy_rows.append(torch.log1p(simple_return.clamp_min(-1.0 + _MIN_WEALTH_FACTOR)))
        turnover_rows.append(turnover)
        if return_weights_history:
            weight_rows.append(execution_weights)
        cash_rows.append(cash)
        payable_rows.append(payables)
        receivable_rows.append(receivables)
        short_sale_rows.append(short_sale_collateral)
        short_margin_rows.append(short_margin_collateral)
        default_rows.append(default_event)
        equity_scale_rows.append(equity_scale)

    weights_history = (
        torch.stack(weight_rows, dim=0)
        if return_weights_history
        else target_weights.new_empty((0, n_symbols))
    )
    return TaiwanContinuousResult(
        strategy_returns=torch.stack(strategy_rows),
        turnovers=torch.stack(turnover_rows),
        weights_history=weights_history,
        cash_history=torch.stack(cash_rows),
        payables_history=torch.stack(payable_rows),
        receivables_history=torch.stack(receivable_rows),
        settlement_default=torch.stack(default_rows),
        equity_scale_history=torch.stack(equity_scale_rows),
        final_weights=risky,
        final_cash=cash,
        final_payables=payables,
        final_receivables=receivables,
        final_alive=alive,
        final_equity_scale=equity_scale,
        short_sale_collateral_history=torch.stack(short_sale_rows),
        short_margin_collateral_history=torch.stack(short_margin_rows),
        final_short_sale_collateral=short_sale_collateral,
        final_short_margin_collateral=short_margin_collateral,
    )


def get_tw_continuous_compile_stats(*, reset: bool = False) -> dict[str, int]:
    """Return process-local compiled Taiwan chunk usage counters."""

    snapshot = dict(_TW_CASH_COMPILE_STATS)
    if reset:
        for name in _TW_CASH_COMPILE_STATS:
            _TW_CASH_COMPILE_STATS[name] = 0
    return snapshot


def _tw_env_truthy(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
    }


def _tw_cash_compile_symbol_bounds(symbols: int) -> tuple[int, int]:
    # Dynamo deliberately specializes dimensions of size zero and one. Keep a
    # singleton universe on its own static cache key; a graph first traced at
    # S=1 cannot later become a valid bounded-dynamic S graph.
    if int(symbols) == 1:
        return 1, 1
    lower = max(
        2,
        int(os.environ.get("STOCKAGENT_TW_COMPILE_SYMBOL_MIN", "1")),
    )
    upper = max(
        int(symbols),
        int(os.environ.get("STOCKAGENT_TW_COMPILE_SYMBOL_MAX", str(symbols))),
    )
    if not lower <= int(symbols) <= upper:
        raise ValueError(
            "TW compiled chunk observed S outside configured bounds "
            f"[{lower}, {upper}]: {symbols}"
        )
    return lower, upper


def _mark_tw_cash_chunk_symbol_axes(
    values_2d: tuple[torch.Tensor, ...],
    values_1d: tuple[torch.Tensor, ...],
    *,
    symbols: int,
    min_symbols: int,
    max_symbols: int,
) -> None:
    if min_symbols == max_symbols:
        return
    mark_dynamic = getattr(getattr(torch, "_dynamo", None), "mark_dynamic", None)
    if mark_dynamic is None:
        raise RuntimeError(
            "compiled Taiwan chunks require torch._dynamo.mark_dynamic"
        )
    seen: set[int] = set()
    for value in values_2d:
        if id(value) in seen:
            continue
        seen.add(id(value))
        if value.dim() != 2 or int(value.size(1)) != symbols:
            raise ValueError("TW compiled chunk expected a [T,S] tensor")
        mark_dynamic(value, 1, min=min_symbols, max=max_symbols)
    for value in values_1d:
        if id(value) in seen:
            continue
        seen.add(id(value))
        if value.dim() != 1 or int(value.numel()) != symbols:
            raise ValueError("TW compiled chunk expected a [S] tensor")
        mark_dynamic(value, 0, min=min_symbols, max=max_symbols)


def _tw_cash_result_tuple(result: TaiwanContinuousResult) -> tuple[torch.Tensor, ...]:
    return (
        result.strategy_returns,
        result.turnovers,
        result.weights_history,
        result.cash_history,
        result.payables_history,
        result.receivables_history,
        result.settlement_default,
        result.equity_scale_history,
        result.final_weights,
        result.final_cash,
        result.final_payables,
        result.final_receivables,
        result.final_alive,
        result.final_equity_scale,
        result.short_sale_collateral_history,
        result.short_margin_collateral_history,
        result.final_short_sale_collateral,
        result.final_short_margin_collateral,
    )


def _tw_cash_result_from_tuple(
    values: tuple[torch.Tensor, ...],
) -> TaiwanContinuousResult:
    return TaiwanContinuousResult(
        strategy_returns=values[0],
        turnovers=values[1],
        weights_history=values[2],
        cash_history=values[3],
        payables_history=values[4],
        receivables_history=values[5],
        settlement_default=values[6],
        equity_scale_history=values[7],
        final_weights=values[8],
        final_cash=values[9],
        final_payables=values[10],
        final_receivables=values[11],
        final_alive=values[12],
        final_equity_scale=values[13],
        short_sale_collateral_history=values[14],
        short_margin_collateral_history=values[15],
        final_short_sale_collateral=values[16],
        final_short_margin_collateral=values[17],
    )


def _tw_cash_compiled_chunk_runner(
    *,
    target_weights: torch.Tensor,
    chunk_rows: int,
    settlement_lag_sessions: int,
    gross_budget: float,
    max_turnover_ratio: float,
    short_maintenance_ratio: float,
    has_volume_limit: bool,
    has_short_contract: bool,
    has_short_open_mask: bool,
    has_force_short_cover_mask: bool,
    has_short_capacity: bool,
    return_weights_history: bool,
    requires_state_grad: bool,
    min_symbols: int,
    max_symbols: int,
    has_cash_dividends: bool = False,
    claim_queue_sessions: int | None = None,
) -> tuple[tuple[object, ...], Callable[..., tuple[torch.Tensor, ...]]]:
    if target_weights.device.type == "cuda":
        device_index = (
            target_weights.device.index
            if target_weights.device.index is not None
            else torch.cuda.current_device()
        )
    else:
        # The public fast path is CUDA-only.  A stable CPU sentinel keeps the
        # recurrent ABI and cache contract unit-testable without pretending
        # that CPU execution is a production compile benchmark.
        device_index = -1
    key: tuple[object, ...] = (
        "tw_cash",
        int(device_index),
        str(target_weights.dtype),
        int(chunk_rows),
        int(settlement_lag_sessions),
        float(gross_budget),
        float(max_turnover_ratio),
        float(short_maintenance_ratio),
        bool(has_volume_limit),
        bool(has_short_contract),
        bool(has_short_open_mask),
        bool(has_force_short_cover_mask),
        bool(has_short_capacity),
        bool(has_cash_dividends),
        int(
            settlement_lag_sessions
            if claim_queue_sessions is None
            else claim_queue_sessions
        ),
        bool(return_weights_history),
        bool(torch.is_grad_enabled()),
        bool(target_weights.requires_grad),
        bool(requires_state_grad),
        bool(torch.is_inference_mode_enabled()),
        int(min_symbols),
        int(max_symbols),
    )
    cached = _TW_CASH_COMPILED_CHUNK_CACHE.get(key)
    if cached is not None:
        return key, cached

    def run_chunk(
        chunk_target_weights: torch.Tensor,
        chunk_asset_log_returns: torch.Tensor,
        chunk_tradable_mask: torch.Tensor,
        chunk_can_buy_mask: torch.Tensor,
        chunk_can_sell_mask: torch.Tensor,
        chunk_can_short_open_mask: torch.Tensor,
        chunk_force_short_cover_mask: torch.Tensor,
        chunk_short_margin_rate: torch.Tensor,
        chunk_short_capacity_weights: torch.Tensor,
        chunk_short_handling_fee_rate: torch.Tensor,
        chunk_cash_dividend_yield: torch.Tensor,
        chunk_cash_dividend_payment_delay_sessions: torch.Tensor,
        buy_fee_rates: torch.Tensor,
        sell_fee_rates: torch.Tensor,
        chunk_volume_limit_weights: torch.Tensor,
        chunk_force_exit_mask: torch.Tensor,
        chunk_unresolved_corporate_action_mask: torch.Tensor,
        chunk_state_advance_mask: torch.Tensor,
        initial_weights: torch.Tensor,
        initial_cash: torch.Tensor,
        initial_payables: torch.Tensor,
        initial_receivables: torch.Tensor,
        initial_short_sale_collateral: torch.Tensor,
        initial_short_margin_collateral: torch.Tensor,
        initial_alive: torch.Tensor,
        initial_equity_scale: torch.Tensor,
    ) -> tuple[torch.Tensor, ...]:
        result = _run_tw_cash_continuous_impl(
            chunk_target_weights,
            chunk_asset_log_returns,
            chunk_tradable_mask,
            chunk_can_buy_mask,
            chunk_can_sell_mask,
            buy_fee_rates,
            sell_fee_rates,
            can_short_open_mask=(
                chunk_can_short_open_mask if has_short_open_mask else None
            ),
            force_short_cover_mask=(
                chunk_force_short_cover_mask
                if has_force_short_cover_mask
                else None
            ),
            short_margin_rate=(
                chunk_short_margin_rate if has_short_contract else None
            ),
            short_capacity_weights=(
                chunk_short_capacity_weights if has_short_capacity else None
            ),
            short_handling_fee_rate=(
                chunk_short_handling_fee_rate if has_short_contract else 0.0
            ),
            short_maintenance_ratio=short_maintenance_ratio,
            unresolved_corporate_action_mask=(
                chunk_unresolved_corporate_action_mask
            ),
            cash_dividend_yield=(
                chunk_cash_dividend_yield if has_cash_dividends else None
            ),
            cash_dividend_payment_delay_sessions=(
                chunk_cash_dividend_payment_delay_sessions
                if has_cash_dividends
                else None
            ),
            claim_queue_sessions=(
                settlement_lag_sessions
                if claim_queue_sessions is None
                else claim_queue_sessions
            ),
            settlement_lag_sessions=settlement_lag_sessions,
            gross_budget=gross_budget,
            max_turnover_ratio=max_turnover_ratio,
            volume_limit_weights=(
                chunk_volume_limit_weights if has_volume_limit else None
            ),
            force_exit_mask=chunk_force_exit_mask,
            state_advance_mask=chunk_state_advance_mask,
            return_weights_history=return_weights_history,
            initial_weights=initial_weights,
            initial_cash=initial_cash,
            initial_payables=initial_payables,
            initial_receivables=initial_receivables,
            initial_short_sale_collateral=initial_short_sale_collateral,
            initial_short_margin_collateral=initial_short_margin_collateral,
            initial_alive=initial_alive,
            initial_equity_scale=initial_equity_scale,
            _detach_initial_state=False,
        )
        return _tw_cash_result_tuple(result)

    compiled = torch.compile(
        run_chunk,
        fullgraph=True,
        dynamic=None,
        options={"triton.cudagraphs": False},
    )
    _TW_CASH_COMPILED_CHUNK_CACHE[key] = compiled
    _TW_CASH_COMPILE_STATS["compile_constructors"] += 1
    return key, compiled


def run_tw_cash_continuous(
    target_weights: torch.Tensor,
    asset_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    *,
    can_short_open_mask: torch.Tensor | None = None,
    force_short_cover_mask: torch.Tensor | None = None,
    short_margin_rate: torch.Tensor | float | None = None,
    short_capacity_weights: torch.Tensor | None = None,
    short_handling_fee_rate: torch.Tensor | float = 0.0,
    short_maintenance_ratio: float = 1.30,
    unresolved_corporate_action_mask: torch.Tensor | None = None,
    cash_dividend_yield: torch.Tensor | None = None,
    cash_dividend_payment_delay_sessions: torch.Tensor | None = None,
    claim_queue_sessions: int | None = None,
    settlement_lag_sessions: int = 2,
    gross_budget: float = 1.0,
    max_turnover_ratio: float = 0.0,
    volume_limit_weights: torch.Tensor | None = None,
    force_exit_mask: torch.Tensor | None = None,
    state_advance_mask: torch.Tensor | None = None,
    return_weights_history: bool = True,
    initial_weights: torch.Tensor | None = None,
    initial_cash: torch.Tensor | None = None,
    initial_payables: torch.Tensor | None = None,
    initial_receivables: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_short_sale_collateral: torch.Tensor | None = None,
    initial_short_margin_collateral: torch.Tensor | None = None,
) -> TaiwanContinuousResult:
    """Run the T+2 ledger, compiling a small recurrent CUDA chunk when useful.

    Compiling the full training horizon unrolls the Python recurrence into a
    graph proportional to ``T``. Eight-session chunks keep compile memory and
    latency bounded while the eager outer loop preserves the exact recurrence.
    Carried state remains attached inside one loss call; only caller-provided
    cross-batch state is detached at this public boundary.
    """

    try:
        chunk_rows = int(
            os.environ.get("STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS", "8")
        )
    except ValueError:
        chunk_rows = 0
    if can_short_open_mask is not None:
        if short_margin_rate is None:
            raise ValueError(
                "can_short_open_mask requires an explicit point-in-time "
                "short_margin_rate"
            )
        if short_capacity_weights is None:
            raise ValueError(
                "can_short_open_mask requires explicit point-in-time "
                "short_capacity_weights"
            )
    maintenance_ratio_value = _resolve_short_maintenance_ratio(
        short_maintenance_ratio
    )
    has_cash_dividends = cash_dividend_yield is not None
    if has_cash_dividends != (cash_dividend_payment_delay_sessions is not None):
        raise ValueError(
            "cash_dividend_yield and cash_dividend_payment_delay_sessions "
            "must be supplied together"
        )
    if claim_queue_sessions is None:
        resolved_claim_queue_sessions = int(settlement_lag_sessions)
    else:
        if isinstance(claim_queue_sessions, bool) or not isinstance(
            claim_queue_sessions, int
        ):
            raise ValueError(
                "claim_queue_sessions must be an integer at least as large as "
                "settlement_lag_sessions"
            )
        resolved_claim_queue_sessions = int(claim_queue_sessions)
    if resolved_claim_queue_sessions < int(settlement_lag_sessions):
        raise ValueError(
            "claim_queue_sessions must be an integer at least as large as "
            "settlement_lag_sessions"
        )
    has_explicit_short_handling = (
        True
        if isinstance(short_handling_fee_rate, torch.Tensor)
        else (
            False
            if short_handling_fee_rate is None
            else float(short_handling_fee_rate) != 0.0
        )
    )
    has_short_contract = (
        short_margin_rate is not None
        or has_explicit_short_handling
        or any(
            value is not None
            for value in (
                can_short_open_mask,
                force_short_cover_mask,
                short_capacity_weights,
                initial_short_sale_collateral,
                initial_short_margin_collateral,
            )
        )
    )
    has_short_open_mask = can_short_open_mask is not None
    has_force_short_cover_mask = force_short_cover_mask is not None
    has_short_capacity = short_capacity_weights is not None
    compile_chunks = bool(
        chunk_rows > 0
        and target_weights.device.type == "cuda"
        and hasattr(torch, "compile")
        and _tw_env_truthy("STOCKAGENT_BACKTEST_COMPILE", "1")
        and not torch.compiler.is_compiling()
        and int(target_weights.size(0)) >= chunk_rows
    )
    if not compile_chunks:
        return _run_tw_cash_continuous_impl(
            target_weights,
            asset_log_returns,
            tradable_mask,
            can_buy_mask,
            can_sell_mask,
            buy_fee_rates,
            sell_fee_rates,
            can_short_open_mask=can_short_open_mask,
            force_short_cover_mask=force_short_cover_mask,
            short_margin_rate=short_margin_rate,
            short_capacity_weights=short_capacity_weights,
            short_handling_fee_rate=short_handling_fee_rate,
            short_maintenance_ratio=maintenance_ratio_value,
            unresolved_corporate_action_mask=unresolved_corporate_action_mask,
            cash_dividend_yield=cash_dividend_yield,
            cash_dividend_payment_delay_sessions=(
                cash_dividend_payment_delay_sessions
            ),
            claim_queue_sessions=claim_queue_sessions,
            settlement_lag_sessions=settlement_lag_sessions,
            gross_budget=gross_budget,
            max_turnover_ratio=max_turnover_ratio,
            volume_limit_weights=volume_limit_weights,
            force_exit_mask=force_exit_mask,
            state_advance_mask=state_advance_mask,
            return_weights_history=return_weights_history,
            initial_weights=initial_weights,
            initial_cash=initial_cash,
            initial_payables=initial_payables,
            initial_receivables=initial_receivables,
            initial_alive=initial_alive,
            initial_equity_scale=initial_equity_scale,
            initial_short_sale_collateral=initial_short_sale_collateral,
            initial_short_margin_collateral=initial_short_margin_collateral,
        )

    if unresolved_corporate_action_mask is None:
        raise ValueError(
            "tw_cash requires the official corporate-action avoidance mask"
        )
    expected_shape = tuple(target_weights.shape)
    for name, value in (
        ("asset_log_returns", asset_log_returns),
        ("tradable_mask", tradable_mask),
        ("can_buy_mask", can_buy_mask),
        ("can_sell_mask", can_sell_mask),
        ("unresolved_corporate_action_mask", unresolved_corporate_action_mask),
    ):
        if tuple(value.shape) != expected_shape:
            raise ValueError(f"{name} shape must match target_weights")
    if force_exit_mask is not None and tuple(force_exit_mask.shape) != expected_shape:
        raise ValueError("force_exit_mask shape must match target_weights")
    if volume_limit_weights is not None and tuple(volume_limit_weights.shape) != expected_shape:
        raise ValueError("volume_limit_weights shape must match target_weights")
    if can_short_open_mask is not None and tuple(can_short_open_mask.shape) != expected_shape:
        raise ValueError("can_short_open_mask shape must match target_weights")
    if force_short_cover_mask is not None and tuple(force_short_cover_mask.shape) != expected_shape:
        raise ValueError("force_short_cover_mask shape must match target_weights")
    if short_capacity_weights is not None and tuple(short_capacity_weights.shape) != expected_shape:
        raise ValueError("short_capacity_weights shape must match target_weights")
    if has_cash_dividends:
        if tuple(cash_dividend_yield.shape) != expected_shape:
            raise ValueError("cash_dividend_yield shape must match target_weights")
        if tuple(cash_dividend_payment_delay_sessions.shape) != expected_shape:
            raise ValueError(
                "cash_dividend_payment_delay_sessions shape must match "
                "target_weights"
            )
        dividend_yields = cash_dividend_yield.to(
            device=target_weights.device,
            dtype=target_weights.dtype,
        ).contiguous()
        dividend_delays = cash_dividend_payment_delay_sessions.to(
            device=target_weights.device,
            dtype=torch.int64,
        ).contiguous()
        if not bool(torch.isfinite(dividend_yields).all().item()) or bool(
            (dividend_yields < 0.0).any().item()
        ):
            raise ValueError("cash_dividend_yield must be finite and non-negative")
        event_cells = dividend_yields > 0.0
        invalid_delay = event_cells & (
            (dividend_delays < 1)
            | (dividend_delays > resolved_claim_queue_sessions)
        )
        if bool(invalid_delay.any().item()):
            raise ValueError(
                "cash-dividend payment delay exceeds the configured claim queue"
            )
        if bool(((~event_cells) & (dividend_delays != 0)).any().item()):
            raise ValueError(
                "cash-dividend payment delay must be zero when yield is zero"
            )
    else:
        # Unused aliases preserve a fixed compiled-call ABI without allocating
        # two full [T,S] placeholders on the established avoidance path.
        dividend_yields = target_weights
        dividend_delays = tradable_mask

    buy_fees = _as_fee_vector(
        buy_fee_rates,
        reference=target_weights,
        name="buy_fee_rates",
    )
    sell_fees = _as_fee_vector(
        sell_fee_rates,
        reference=target_weights,
        name="sell_fee_rates",
    )
    (
        risky,
        cash,
        payables,
        receivables,
        short_sale_collateral,
        short_margin_collateral,
        alive,
        advance,
    ) = _prepare_cash_short_state(
        target_weights,
        settlement_lag_sessions=settlement_lag_sessions,
        claim_queue_sessions=resolved_claim_queue_sessions,
        initial_weights=initial_weights,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_alive=initial_alive,
        initial_short_sale_collateral=initial_short_sale_collateral,
        initial_short_margin_collateral=initial_short_margin_collateral,
        state_advance_mask=state_advance_mask,
        detach_initial_state=True,
    )
    equity_scale = _prepare_equity_scale(
        target_weights,
        initial_equity_scale=initial_equity_scale,
        alive=alive,
        detach_initial_state=True,
    )
    tradable = tradable_mask.to(device=target_weights.device, dtype=torch.bool)
    buy_mask = can_buy_mask.to(device=target_weights.device, dtype=torch.bool)
    sell_mask = can_sell_mask.to(device=target_weights.device, dtype=torch.bool)
    short_open_mask = (
        tradable
        if can_short_open_mask is None
        else can_short_open_mask.to(
            device=target_weights.device,
            dtype=torch.bool,
        )
    )
    forced_cover = (
        tradable
        if force_short_cover_mask is None
        else force_short_cover_mask.to(
            device=target_weights.device,
            dtype=torch.bool,
        )
    )
    # A long-only compiled contract keeps these as aliases of an existing
    # [T,S] input.  The closure erases them to None/scalar defaults, avoiding
    # three full-panel placeholder allocations on the established fast path.
    validated_margin_rates = _as_time_symbol_rate(
        short_margin_rate,
        reference=target_weights,
        default=0.90,
        name="short_margin_rate",
    )
    validated_short_handling_rates = _as_time_symbol_rate(
        short_handling_fee_rate,
        reference=target_weights,
        default=0.0,
        name="short_handling_fee_rate",
    )
    margin_rates = (
        validated_margin_rates
        if has_short_contract
        else target_weights
    )
    short_handling_rates = (
        validated_short_handling_rates
        if has_short_contract
        else target_weights
    )
    short_capacities = (
        target_weights
        if short_capacity_weights is None
        else short_capacity_weights.to(
            device=target_weights.device,
            dtype=target_weights.dtype,
        ).contiguous()
    )
    if has_short_open_mask and bool(short_open_mask.any().item()):
        combined_short_sale_cost = sell_fees + short_handling_rates.amax(dim=0)
        if bool((combined_short_sale_cost >= 1.0).any().item()):
            raise ValueError("short sell fees must leave positive locked sale proceeds")
    unresolved_actions = unresolved_corporate_action_mask.to(
        device=target_weights.device,
        dtype=torch.bool,
    )
    terminal = (
        torch.zeros_like(tradable)
        if force_exit_mask is None
        else force_exit_mask.to(device=target_weights.device, dtype=torch.bool)
    )
    has_volume_limit = volume_limit_weights is not None
    volume_limits = (
        torch.zeros_like(target_weights)
        if volume_limit_weights is None
        else volume_limit_weights.to(
            device=target_weights.device,
            dtype=target_weights.dtype,
        )
    )
    # Caller-provided recurrent state is intentionally detached.  Within one
    # loss call, however, every later chunk receives differentiable state from
    # its predecessor.  Make the private first-chunk seed follow that same
    # requires-grad contract so Dynamo/AOTAutograd can reuse one graph for all
    # chunks.  These leaf gradients are discarded; external state remains
    # detached exactly as required by the cross-batch contract.
    requires_state_grad = bool(
        torch.is_grad_enabled()
        and any(
            value.requires_grad
            for value in (
                target_weights,
                asset_log_returns,
                buy_fees,
                sell_fees,
                margin_rates,
                short_capacities,
                short_handling_rates,
                volume_limits,
            )
        )
    )
    if requires_state_grad:
        risky = (
            risky.detach()
            .clone(memory_format=torch.contiguous_format)
            .requires_grad_(True)
        )
        cash = (
            cash.detach()
            .clone(memory_format=torch.contiguous_format)
            .requires_grad_(True)
        )
        payables = (
            payables.detach()
            .clone(memory_format=torch.contiguous_format)
            .requires_grad_(True)
        )
        receivables = (
            receivables.detach()
            .clone(memory_format=torch.contiguous_format)
            .requires_grad_(True)
        )
        short_sale_collateral = (
            short_sale_collateral.detach()
            .clone(memory_format=torch.contiguous_format)
            .requires_grad_(True)
        )
        short_margin_collateral = (
            short_margin_collateral.detach()
            .clone(memory_format=torch.contiguous_format)
            .requires_grad_(True)
        )
        equity_scale = (
            equity_scale.detach()
            .clone(memory_format=torch.contiguous_format)
            .requires_grad_(True)
        )
    symbols = int(target_weights.size(1))
    min_symbols, max_symbols = _tw_cash_compile_symbol_bounds(symbols)
    try:
        key, compiled_chunk = _tw_cash_compiled_chunk_runner(
            target_weights=target_weights,
            chunk_rows=chunk_rows,
            settlement_lag_sessions=settlement_lag_sessions,
            gross_budget=gross_budget,
            max_turnover_ratio=max_turnover_ratio,
            short_maintenance_ratio=maintenance_ratio_value,
            has_volume_limit=has_volume_limit,
            has_short_contract=has_short_contract,
            has_short_open_mask=has_short_open_mask,
            has_force_short_cover_mask=has_force_short_cover_mask,
            has_short_capacity=has_short_capacity,
            return_weights_history=return_weights_history,
            requires_state_grad=requires_state_grad,
            min_symbols=min_symbols,
            max_symbols=max_symbols,
            has_cash_dividends=has_cash_dividends,
            claim_queue_sessions=resolved_claim_queue_sessions,
        )
    except Exception as exc:
        if _tw_env_truthy("STOCKAGENT_STRICT_NO_FALLBACK", "0"):
            raise RuntimeError(
                "failed to construct compiled Taiwan cash chunk runner"
            ) from exc
        _TW_CASH_COMPILE_STATS["eager_fallback_calls"] += 1
        print(
            "[tw cash compile] chunk constructor failed; using the exact eager "
            f"ledger: {type(exc).__name__}: {exc}",
            flush=True,
        )
        return _run_tw_cash_continuous_impl(
            target_weights,
            asset_log_returns,
            tradable_mask,
            can_buy_mask,
            can_sell_mask,
            buy_fee_rates,
            sell_fee_rates,
            can_short_open_mask=can_short_open_mask,
            force_short_cover_mask=force_short_cover_mask,
            short_margin_rate=short_margin_rate,
            short_capacity_weights=short_capacity_weights,
            short_handling_fee_rate=short_handling_fee_rate,
            short_maintenance_ratio=maintenance_ratio_value,
            unresolved_corporate_action_mask=unresolved_corporate_action_mask,
            cash_dividend_yield=cash_dividend_yield,
            cash_dividend_payment_delay_sessions=(
                cash_dividend_payment_delay_sessions
            ),
            claim_queue_sessions=claim_queue_sessions,
            settlement_lag_sessions=settlement_lag_sessions,
            gross_budget=gross_budget,
            max_turnover_ratio=max_turnover_ratio,
            volume_limit_weights=volume_limit_weights,
            force_exit_mask=force_exit_mask,
            state_advance_mask=state_advance_mask,
            return_weights_history=return_weights_history,
            initial_weights=initial_weights,
            initial_cash=initial_cash,
            initial_payables=initial_payables,
            initial_receivables=initial_receivables,
            initial_alive=initial_alive,
            initial_equity_scale=initial_equity_scale,
            initial_short_sale_collateral=initial_short_sale_collateral,
            initial_short_margin_collateral=initial_short_margin_collateral,
        )
    if key in _TW_CASH_FAILED_COMPILE_KEYS:
        if _tw_env_truthy("STOCKAGENT_STRICT_NO_FALLBACK", "0"):
            raise RuntimeError(
                "compiled Taiwan cash chunk previously failed for this contract; "
                "strict fallback is enabled"
            )
        _TW_CASH_COMPILE_STATS["eager_fallback_calls"] += 1
        return _run_tw_cash_continuous_impl(
            target_weights,
            asset_log_returns,
            tradable_mask,
            can_buy_mask,
            can_sell_mask,
            buy_fee_rates,
            sell_fee_rates,
            can_short_open_mask=can_short_open_mask,
            force_short_cover_mask=force_short_cover_mask,
            short_margin_rate=short_margin_rate,
            short_capacity_weights=short_capacity_weights,
            short_handling_fee_rate=short_handling_fee_rate,
            short_maintenance_ratio=maintenance_ratio_value,
            unresolved_corporate_action_mask=unresolved_corporate_action_mask,
            cash_dividend_yield=cash_dividend_yield,
            cash_dividend_payment_delay_sessions=(
                cash_dividend_payment_delay_sessions
            ),
            claim_queue_sessions=claim_queue_sessions,
            settlement_lag_sessions=settlement_lag_sessions,
            gross_budget=gross_budget,
            max_turnover_ratio=max_turnover_ratio,
            volume_limit_weights=volume_limit_weights,
            force_exit_mask=force_exit_mask,
            state_advance_mask=state_advance_mask,
            return_weights_history=return_weights_history,
            initial_weights=initial_weights,
            initial_cash=initial_cash,
            initial_payables=initial_payables,
            initial_receivables=initial_receivables,
            initial_alive=initial_alive,
            initial_equity_scale=initial_equity_scale,
            initial_short_sale_collateral=initial_short_sale_collateral,
            initial_short_margin_collateral=initial_short_margin_collateral,
        )

    chunks: list[TaiwanContinuousResult] = []
    try:
        total_rows = int(target_weights.size(0))
        full_stop = total_rows - total_rows % chunk_rows
        gradient_horizon = _settlement_gradient_horizon_rows()
        for start in range(0, full_stop, chunk_rows):
            if (
                gradient_horizon > 0
                and start > 0
                and start % gradient_horizon == 0
            ):
                risky = _restart_recurrent_gradient(
                    risky,
                    requires_grad=requires_state_grad,
                )
                cash = _restart_recurrent_gradient(
                    cash,
                    requires_grad=requires_state_grad,
                )
                payables = _restart_recurrent_gradient(
                    payables,
                    requires_grad=requires_state_grad,
                )
                receivables = _restart_recurrent_gradient(
                    receivables,
                    requires_grad=requires_state_grad,
                )
                short_sale_collateral = _restart_recurrent_gradient(
                    short_sale_collateral,
                    requires_grad=requires_state_grad,
                )
                short_margin_collateral = _restart_recurrent_gradient(
                    short_margin_collateral,
                    requires_grad=requires_state_grad,
                )
                alive = _restart_recurrent_gradient(
                    alive,
                    requires_grad=False,
                )
                equity_scale = _restart_recurrent_gradient(
                    equity_scale,
                    requires_grad=requires_state_grad,
                )
            stop = start + chunk_rows
            chunk_tensors_2d = (
                target_weights[start:stop],
                asset_log_returns[start:stop],
                tradable[start:stop],
                buy_mask[start:stop],
                sell_mask[start:stop],
                short_open_mask[start:stop],
                forced_cover[start:stop],
                margin_rates[start:stop],
                short_capacities[start:stop],
                short_handling_rates[start:stop],
                dividend_yields[start:stop],
                dividend_delays[start:stop],
                volume_limits[start:stop],
                terminal[start:stop],
                unresolved_actions[start:stop],
            )
            _mark_tw_cash_chunk_symbol_axes(
                chunk_tensors_2d,
                (
                    buy_fees,
                    sell_fees,
                    risky,
                    short_sale_collateral,
                    short_margin_collateral,
                ),
                symbols=symbols,
                min_symbols=min_symbols,
                max_symbols=max_symbols,
            )
            values = compiled_chunk(
                *chunk_tensors_2d[:12],
                buy_fees,
                sell_fees,
                *chunk_tensors_2d[12:],
                advance[start:stop],
                risky,
                cash,
                payables,
                receivables,
                short_sale_collateral,
                short_margin_collateral,
                alive,
                equity_scale,
            )
            result = _tw_cash_result_from_tuple(values)
            chunks.append(result)
            risky = result.final_weights
            cash = result.final_cash
            payables = result.final_payables
            receivables = result.final_receivables
            short_sale_collateral = result.final_short_sale_collateral
            short_margin_collateral = result.final_short_margin_collateral
            alive = result.final_alive
            equity_scale = result.final_equity_scale
            _TW_CASH_COMPILE_STATS["compiled_chunk_calls"] += 1

        if full_stop < total_rows:
            if (
                gradient_horizon > 0
                and full_stop > 0
                and full_stop % gradient_horizon == 0
            ):
                risky = _restart_recurrent_gradient(risky, requires_grad=False)
                cash = _restart_recurrent_gradient(cash, requires_grad=False)
                payables = _restart_recurrent_gradient(payables, requires_grad=False)
                receivables = _restart_recurrent_gradient(
                    receivables,
                    requires_grad=False,
                )
                short_sale_collateral = _restart_recurrent_gradient(
                    short_sale_collateral,
                    requires_grad=False,
                )
                short_margin_collateral = _restart_recurrent_gradient(
                    short_margin_collateral,
                    requires_grad=False,
                )
                alive = _restart_recurrent_gradient(alive, requires_grad=False)
                equity_scale = _restart_recurrent_gradient(
                    equity_scale,
                    requires_grad=False,
                )
            tail = _run_tw_cash_continuous_impl(
                target_weights[full_stop:],
                asset_log_returns[full_stop:],
                tradable[full_stop:],
                buy_mask[full_stop:],
                sell_mask[full_stop:],
                buy_fees,
                sell_fees,
                can_short_open_mask=(
                    short_open_mask[full_stop:] if has_short_open_mask else None
                ),
                force_short_cover_mask=(
                    forced_cover[full_stop:]
                    if has_force_short_cover_mask
                    else None
                ),
                short_margin_rate=(
                    margin_rates[full_stop:] if has_short_contract else None
                ),
                short_capacity_weights=(
                    short_capacities[full_stop:] if has_short_capacity else None
                ),
                short_handling_fee_rate=(
                    short_handling_rates[full_stop:]
                    if has_short_contract
                    else 0.0
                ),
                short_maintenance_ratio=maintenance_ratio_value,
                unresolved_corporate_action_mask=unresolved_actions[full_stop:],
                cash_dividend_yield=(
                    dividend_yields[full_stop:] if has_cash_dividends else None
                ),
                cash_dividend_payment_delay_sessions=(
                    dividend_delays[full_stop:] if has_cash_dividends else None
                ),
                claim_queue_sessions=resolved_claim_queue_sessions,
                settlement_lag_sessions=settlement_lag_sessions,
                gross_budget=gross_budget,
                max_turnover_ratio=max_turnover_ratio,
                volume_limit_weights=(
                    volume_limits[full_stop:] if has_volume_limit else None
                ),
                force_exit_mask=terminal[full_stop:],
                state_advance_mask=advance[full_stop:],
                return_weights_history=return_weights_history,
                initial_weights=risky,
                initial_cash=cash,
                initial_payables=payables,
                initial_receivables=receivables,
                initial_short_sale_collateral=short_sale_collateral,
                initial_short_margin_collateral=short_margin_collateral,
                initial_alive=alive,
                initial_equity_scale=equity_scale,
                _detach_initial_state=False,
            )
            chunks.append(tail)
    except Exception as exc:
        _TW_CASH_FAILED_COMPILE_KEYS.add(key)
        if _tw_env_truthy("STOCKAGENT_STRICT_NO_FALLBACK", "0"):
            raise RuntimeError(
                "compiled Taiwan cash chunk failed and strict fallback is enabled"
            ) from exc
        _TW_CASH_COMPILE_STATS["eager_fallback_calls"] += 1
        print(
            "[tw cash compile] chunk runner failed; using the exact eager ledger "
            f"for this shape: {type(exc).__name__}: {exc}",
            flush=True,
        )
        return _run_tw_cash_continuous_impl(
            target_weights,
            asset_log_returns,
            tradable_mask,
            can_buy_mask,
            can_sell_mask,
            buy_fee_rates,
            sell_fee_rates,
            can_short_open_mask=can_short_open_mask,
            force_short_cover_mask=force_short_cover_mask,
            short_margin_rate=short_margin_rate,
            short_capacity_weights=short_capacity_weights,
            short_handling_fee_rate=short_handling_fee_rate,
            short_maintenance_ratio=maintenance_ratio_value,
            unresolved_corporate_action_mask=unresolved_corporate_action_mask,
            cash_dividend_yield=cash_dividend_yield,
            cash_dividend_payment_delay_sessions=(
                cash_dividend_payment_delay_sessions
            ),
            claim_queue_sessions=claim_queue_sessions,
            settlement_lag_sessions=settlement_lag_sessions,
            gross_budget=gross_budget,
            max_turnover_ratio=max_turnover_ratio,
            volume_limit_weights=volume_limit_weights,
            force_exit_mask=force_exit_mask,
            state_advance_mask=state_advance_mask,
            return_weights_history=return_weights_history,
            initial_weights=initial_weights,
            initial_cash=initial_cash,
            initial_payables=initial_payables,
            initial_receivables=initial_receivables,
            initial_alive=initial_alive,
            initial_equity_scale=initial_equity_scale,
            initial_short_sale_collateral=initial_short_sale_collateral,
            initial_short_margin_collateral=initial_short_margin_collateral,
        )

    final = chunks[-1]
    return TaiwanContinuousResult(
        strategy_returns=torch.cat([chunk.strategy_returns for chunk in chunks]),
        turnovers=torch.cat([chunk.turnovers for chunk in chunks]),
        weights_history=torch.cat([chunk.weights_history for chunk in chunks]),
        cash_history=torch.cat([chunk.cash_history for chunk in chunks]),
        payables_history=torch.cat([chunk.payables_history for chunk in chunks]),
        receivables_history=torch.cat(
            [chunk.receivables_history for chunk in chunks]
        ),
        settlement_default=torch.cat(
            [chunk.settlement_default for chunk in chunks]
        ),
        equity_scale_history=torch.cat(
            [chunk.equity_scale_history for chunk in chunks]
        ),
        short_sale_collateral_history=torch.cat(
            [chunk.short_sale_collateral_history for chunk in chunks]
        ),
        short_margin_collateral_history=torch.cat(
            [chunk.short_margin_collateral_history for chunk in chunks]
        ),
        final_weights=final.final_weights,
        final_cash=final.final_cash,
        final_payables=final.final_payables,
        final_receivables=final.final_receivables,
        final_alive=final.final_alive,
        final_equity_scale=final.final_equity_scale,
        final_short_sale_collateral=final.final_short_sale_collateral,
        final_short_margin_collateral=final.final_short_margin_collateral,
    )


def run_tw_day_trade_continuous(
    target_weights: torch.Tensor,
    intraday_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    can_short_open_mask: torch.Tensor,
    day_trade_eligible_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    *,
    day_trade_can_buy_open_mask: torch.Tensor | None = None,
    day_trade_can_sell_open_mask: torch.Tensor | None = None,
    settlement_lag_sessions: int = 2,
    max_turnover_ratio: float = 0.0,
    volume_limit_weights: torch.Tensor | None = None,
    force_short_cover_mask: torch.Tensor | None = None,
    force_exit_mask: torch.Tensor | None = None,
    state_advance_mask: torch.Tensor | None = None,
    return_weights_history: bool = True,
    initial_cash: torch.Tensor | None = None,
    initial_payables: torch.Tensor | None = None,
    initial_receivables: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
) -> TaiwanContinuousResult:
    """Open-entry/close-exit execution with T+2 net-difference settlement.

    Entry is decided exclusively from open-side masks.  Close-side masks are
    evaluated only after that entry has occurred; an unavailable mandatory
    closing leg fails closed instead of retroactively erasing the open trade.
    ``volume_limit_weights`` and ``initial_equity_scale`` use the same fixed-
    reference/current-NAV contract as the cash executor.
    """

    if day_trade_can_buy_open_mask is None or day_trade_can_sell_open_mask is None:
        raise ValueError(
            "tw_day_trade requires explicit open-side buy and sell masks; "
            "close masks cannot be reused without look-ahead"
        )

    shape = tuple(target_weights.shape)
    for name, value in (
        ("intraday_log_returns", intraday_log_returns),
        ("tradable_mask", tradable_mask),
        ("can_buy_mask", can_buy_mask),
        ("can_sell_mask", can_sell_mask),
        ("can_short_open_mask", can_short_open_mask),
        ("day_trade_eligible_mask", day_trade_eligible_mask),
        ("day_trade_can_buy_open_mask", day_trade_can_buy_open_mask),
        ("day_trade_can_sell_open_mask", day_trade_can_sell_open_mask),
    ):
        if tuple(value.shape) != shape:
            raise ValueError(f"{name} shape must match target_weights")
    if volume_limit_weights is not None and tuple(volume_limit_weights.shape) != shape:
        raise ValueError("volume_limit_weights shape must match target_weights")
    buy_fees = _as_fee_vector(buy_fee_rates, reference=target_weights, name="buy_fee_rates")
    sell_fees = _as_fee_vector(sell_fee_rates, reference=target_weights, name="sell_fee_rates")
    zero_initial_weights = torch.zeros_like(target_weights[0])
    risky, cash, payables, receivables, alive, advance_mask = _prepare_common_state(
        target_weights,
        settlement_lag_sessions=settlement_lag_sessions,
        initial_weights=zero_initial_weights,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_alive=initial_alive,
        state_advance_mask=state_advance_mask,
    )
    equity_scale = _prepare_equity_scale(
        target_weights,
        initial_equity_scale=initial_equity_scale,
        alive=alive,
    )
    raw_intraday_log_returns = intraday_log_returns.to(
        device=target_weights.device,
        dtype=target_weights.dtype,
    )
    finite_intraday_log_return = torch.isfinite(raw_intraday_log_returns)
    intraday_simple_unchecked = torch.expm1(
        torch.where(
            finite_intraday_log_return,
            raw_intraday_log_returns,
            torch.zeros_like(raw_intraday_log_returns),
        )
    )
    valid_intraday_return = finite_intraday_log_return & torch.isfinite(
        intraday_simple_unchecked
    )
    intraday_simple = torch.where(
        valid_intraday_return,
        intraday_simple_unchecked,
        torch.zeros_like(intraday_simple_unchecked),
    )
    tradable = tradable_mask.to(device=target_weights.device, dtype=torch.bool)
    buy = can_buy_mask.to(device=target_weights.device, dtype=torch.bool) & tradable
    sell = can_sell_mask.to(device=target_weights.device, dtype=torch.bool) & tradable
    buy_open = (
        day_trade_can_buy_open_mask.to(
            device=target_weights.device, dtype=torch.bool
        )
    )
    sell_open = (
        day_trade_can_sell_open_mask.to(
            device=target_weights.device, dtype=torch.bool
        )
    )
    short_open = (
        can_short_open_mask.to(device=target_weights.device, dtype=torch.bool)
        & sell_open
    )
    eligible = day_trade_eligible_mask.to(
        device=target_weights.device, dtype=torch.bool
    )
    force_cover = (
        torch.zeros_like(tradable)
        if force_short_cover_mask is None
        else force_short_cover_mask.to(device=target_weights.device, dtype=torch.bool)
    )
    force_exit = (
        torch.zeros_like(tradable)
        if force_exit_mask is None
        else force_exit_mask.to(device=target_weights.device, dtype=torch.bool)
    )
    if tuple(force_cover.shape) != shape or tuple(force_exit.shape) != shape:
        raise ValueError("day-trade lifecycle masks must match target_weights")
    t_len, n_symbols = target_weights.shape
    weight_rows: list[torch.Tensor] = []
    strategy_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    cash_rows: list[torch.Tensor] = []
    payable_rows: list[torch.Tensor] = []
    receivable_rows: list[torch.Tensor] = []
    default_rows: list[torch.Tensor] = []
    equity_scale_rows: list[torch.Tensor] = []
    turnover_cap = target_weights.new_tensor(float(max_turnover_ratio))
    gradient_horizon = _settlement_gradient_horizon_rows()

    for idx in range(int(t_len)):
        if gradient_horizon > 0 and idx > 0 and idx % gradient_horizon == 0:
            risky = _restart_recurrent_gradient(risky, requires_grad=False)
            cash = _restart_recurrent_gradient(cash, requires_grad=False)
            payables = _restart_recurrent_gradient(payables, requires_grad=False)
            receivables = _restart_recurrent_gradient(
                receivables,
                requires_grad=False,
            )
            alive = _restart_recurrent_gradient(alive, requires_grad=False)
            equity_scale = _restart_recurrent_gradient(
                equity_scale,
                requires_grad=False,
            )
        advance = advance_mask[idx]
        cash, payables, receivables, alive_after_settlement, default_now = _settle_open_phase(
            cash,
            payables,
            receivables,
            alive,
            advance,
        )
        nav_start = cash + receivables.sum() - payables.sum()
        # A day-trade account begins and ends flat.  A terminal exit therefore
        # means no new round trip may be opened, while a mandatory short-cover
        # event blocks only a new sell-first trade and still permits buy-first.
        entry_eligible = eligible[idx] & ~force_exit[idx]
        direction_open = torch.where(
            target_weights[idx] < 0.0,
            sell_open[idx],
            buy_open[idx],
        )
        signed_target = torch.where(
            advance & alive_after_settlement & entry_eligible & direction_open,
            target_weights[idx],
            torch.zeros_like(target_weights[idx]),
        )
        signed_target = torch.where(
            (signed_target < 0.0) & (~short_open[idx] | force_cover[idx]),
            torch.zeros_like(signed_target),
            signed_target,
        )
        ret = intraday_simple[idx]
        if volume_limit_weights is not None:
            volume_cap = volume_limit_weights[idx].to(
                device=target_weights.device,
                dtype=target_weights.dtype,
            )
            # ``volume_cap`` is one-leg open notional (open * reported shares
            # * participation / equity).  Every day trade consumes the same
            # number of shares twice, once at entry and once at exit, so the
            # admissible opening position is one half of that aggregate daily
            # share budget.  Scaling by buy+sell dollars would incorrectly make
            # the share cap depend on the intraday return/closing price.
            position_cap = volume_cap * 0.5
            capped_position = _cap_current_nav_request_from_reference_notional(
                signed_target.abs(),
                position_cap,
                equity_scale,
            )
            signed_target = torch.sign(signed_target) * capped_position
        if max_turnover_ratio > 0.0:
            # Entry sizing is decided at the open.  Actual close notional is
            # future information, so use a two-leg open-notional budget here.
            # Reported realized turnover below still uses both actual legs.
            raw_turnover = signed_target.abs().sum() * 2.0
            scale = torch.minimum(
                torch.ones_like(raw_turnover),
                turnover_cap / raw_turnover.clamp_min(1.0e-12),
            )
            signed_target = signed_target * scale
        close_available = torch.where(
            signed_target < 0.0,
            buy[idx],
            sell[idx],
        )
        blocked_close = (signed_target != 0.0) & ~close_available
        torch._assert_async(
            ~blocked_close.any(),
            "tw_day_trade mandatory close leg is unavailable after open entry; "
            "daily data cannot carry or price the unresolved position exactly",
        )
        invalid_active_return = (signed_target != 0.0) & ~valid_intraday_return[idx]
        torch._assert_async(
            ~invalid_active_return.any(),
            "tw_day_trade has a non-finite open-to-close return for an "
            "executed round trip",
        )

        short_signed, long_entry = _split_signed(signed_target)
        short_entry = -short_signed
        buy_notional = long_entry + short_entry * (1.0 + ret)
        sell_notional = long_entry * (1.0 + ret) + short_entry

        buy_cost = (buy_notional * (1.0 + buy_fees)).sum()
        sell_proceeds = (sell_notional * (1.0 - sell_fees)).sum()
        next_payables, next_receivables = _enqueue_net_claim(
            payables,
            receivables,
            sell_proceeds - buy_cost,
        )
        payables = torch.where(advance & alive_after_settlement, next_payables, payables)
        receivables = torch.where(advance & alive_after_settlement, next_receivables, receivables)
        turnover = torch.where(
            advance & alive_after_settlement,
            (buy_notional + sell_notional).sum(),
            torch.zeros_like(nav_start),
        )
        nav_end = cash + receivables.sum() - payables.sum()
        zero_risky = torch.zeros_like(risky)
        (
            simple_return,
            risky,
            cash,
            payables,
            receivables,
            alive,
            default_event,
        ) = _finalize_day(
            nav_start=nav_start,
            nav_end=nav_end,
            risky_end=zero_risky,
            cash=cash,
            payables=payables,
            receivables=receivables,
            alive=alive_after_settlement,
            advance=advance,
            default_now=default_now,
        )
        equity_scale = _advance_equity_scale(
            equity_scale,
            simple_return,
            advance=advance,
            alive=alive,
        )
        strategy_rows.append(torch.log1p(simple_return.clamp_min(-1.0 + _MIN_WEALTH_FACTOR)))
        turnover_rows.append(turnover)
        if return_weights_history:
            weight_rows.append(signed_target)
        cash_rows.append(cash)
        payable_rows.append(payables)
        receivable_rows.append(receivables)
        default_rows.append(default_event)
        equity_scale_rows.append(equity_scale)

    weights_history = (
        torch.stack(weight_rows, dim=0)
        if return_weights_history
        else target_weights.new_empty((0, n_symbols))
    )
    return TaiwanContinuousResult(
        strategy_returns=torch.stack(strategy_rows),
        turnovers=torch.stack(turnover_rows),
        weights_history=weights_history,
        cash_history=torch.stack(cash_rows),
        payables_history=torch.stack(payable_rows),
        receivables_history=torch.stack(receivable_rows),
        settlement_default=torch.stack(default_rows),
        equity_scale_history=torch.stack(equity_scale_rows),
        final_weights=torch.zeros_like(risky),
        final_cash=cash,
        final_payables=payables,
        final_receivables=receivables,
        final_alive=alive,
        final_equity_scale=equity_scale,
    )


__all__ = [
    "TaiwanContinuousResult",
    "run_tw_cash_continuous",
    "run_tw_day_trade_continuous",
]
