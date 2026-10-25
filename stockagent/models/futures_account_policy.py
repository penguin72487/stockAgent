"""Causal, market-conditioned feedback for the canonical integer margin ledger.

The batch model emits coefficients, not an executed portfolio. Only the ledger
can resolve them, using its actual carried contracts and *previous* settled NAV.
The packet deliberately has a different shape from legacy [T,S] allocations.
No current execution price, current PnL or future account state enters this map.
"""

from __future__ import annotations

import math
import torch

from stockagent.models.normalization import masked_score_entmax_log_cash_weights

FUTURES_ACCOUNT_POLICY_VERSION = 1
FUTURES_ACCOUNT_OBSERVATION_COLUMNS = (
    "prior_observed_settlement_return_rms",
    "prior_observed_settlement_return_fraction",
)
FUTURES_BETA_OBSERVATION_COLUMNS = ("prior_observed_tx_beta_shrunk8",)
FUTURES_ACCOUNT_STATE_COLUMNS = (
    "signed_notional_nav_asinh", "signed_margin_nav_asinh",
    "held_margin_nav_asinh", "one_contract_margin_nav_asinh",
    "held_roundtrip_cost_nav_bps_asinh", "roundtrip_cost_notional_bps_asinh",
    "notional_to_margin_log1p", "held_rms_notional_risk_nav_asinh",
    "log_nav_to_initial_capital", "gross_notional_nav_asinh",
    "net_notional_nav_asinh", "initial_margin_nav_asinh",
    "maintenance_margin_nav_asinh", "free_collateral_nav_asinh",
    "prior_observed_settlement_rms_percent", "prior_observation_fraction",
)
ACCOUNT_COEFFICIENT_START = 2
ACCOUNT_COEFFICIENT_END = ACCOUNT_COEFFICIENT_START + len(FUTURES_ACCOUNT_STATE_COLUMNS)
ACCOUNT_MASK = ACCOUNT_COEFFICIENT_END
ACCOUNT_NOTIONAL = ACCOUNT_MASK + 1
ACCOUNT_INITIAL_MARGIN = ACCOUNT_MASK + 2
ACCOUNT_MAINTENANCE_MARGIN = ACCOUNT_MASK + 3
ACCOUNT_SIDE_COST = ACCOUNT_MASK + 4
ACCOUNT_RMS = ACCOUNT_MASK + 5
ACCOUNT_OBSERVATION_FRACTION = ACCOUNT_MASK + 6
FUTURES_ACCOUNT_PACKET_WIDTH = ACCOUNT_MASK + 7
ACCOUNT_NET_NOTIONAL_CAP = FUTURES_ACCOUNT_PACKET_WIDTH
FUTURES_NET_EXPOSURE_PACKET_WIDTH = FUTURES_ACCOUNT_PACKET_WIDTH + 1
# Version 3 has a factor loading and its cap instead of version 2's net cap.
ACCOUNT_TX_BETA = FUTURES_ACCOUNT_PACKET_WIDTH
ACCOUNT_BETA_NOTIONAL_CAP = FUTURES_ACCOUNT_PACKET_WIDTH + 1
FUTURES_BETA_EXPOSURE_PACKET_WIDTH = FUTURES_ACCOUNT_PACKET_WIDTH + 2


def futures_account_policy_version(packet_width: int) -> int:
    if packet_width == FUTURES_ACCOUNT_PACKET_WIDTH:
        return FUTURES_ACCOUNT_POLICY_VERSION
    if packet_width == FUTURES_NET_EXPOSURE_PACKET_WIDTH:
        return 2
    if packet_width == FUTURES_BETA_EXPOSURE_PACKET_WIDTH:
        return 3
    raise ValueError('unknown futures account policy packet width')


def futures_account_policy_width(net_notional_cap: float | None, beta_notional_cap: float | None = None) -> int:
    if beta_notional_cap is not None:
        if net_notional_cap is not None:
            raise ValueError('net and beta notional caps are separate experiments')
        return FUTURES_BETA_EXPOSURE_PACKET_WIDTH
    return (FUTURES_ACCOUNT_PACKET_WIDTH if net_notional_cap is None
            else FUTURES_NET_EXPOSURE_PACKET_WIDTH)


def is_futures_account_policy_packet(value: torch.Tensor) -> bool:
    return value.ndim == 3 and value.size(-1) in (
        FUTURES_ACCOUNT_PACKET_WIDTH, FUTURES_NET_EXPOSURE_PACKET_WIDTH,
        FUTURES_BETA_EXPOSURE_PACKET_WIDTH)


def project_prior_net_notional(
    weights: torch.Tensor, mask: torch.Tensor, known_notional: torch.Tensor,
    known_initial: torch.Tensor, cap: torch.Tensor,
    *, factor_beta: torch.Tensor | None = None,
) -> torch.Tensor:
    """Bound requested prior-mark net notional, preserving the cash budget.

    This is an O(S) orthogonal projection in prior-notional coordinates followed
    by a common downscale if required to retain the learned L1 margin budget.
    Optional factor_beta replaces the unit loading by a prior factor estimate.
    It cannot guarantee realized beta neutrality: current prices, dated margin
    rules, whole contracts, fills and carried inventory still own execution.
    """
    valid = mask & (known_notional > 0) & (known_initial > 0)
    notional_per_margin = torch.where(valid, known_notional / known_initial.clamp_min(1e-12), 1.)
    original = torch.where(valid, weights.float(), 0.)
    notional = original * notional_per_margin
    if factor_beta is None:
        net = notional.sum(-1, keepdim=True)
    else:
        loading = torch.where(valid, factor_beta.detach().float(), 0.)
        net = (notional * loading).sum(-1, keepdim=True)
    # clamp(x, 0, 0) chooses an input subgradient of one at x=0 in PyTorch.
    # Exact neutrality instead needs a constant bound with zero derivative.
    bounded = torch.where(cap > 0, net.clamp(-cap, cap), torch.zeros_like(net))
    excess = net - bounded
    if factor_beta is None:
        correction = excess / valid.sum(-1, keepdim=True).clamp_min(1)
    else:
        squared_loading = loading.square().sum(-1, keepdim=True).clamp_min(1e-12)
        correction = loading * excess / squared_loading
    projected = torch.where(valid, notional - correction, 0.) / notional_per_margin
    budget = original.abs().sum(-1, keepdim=True)
    scale = (budget / projected.abs().sum(-1, keepdim=True).clamp_min(1e-12)).clamp(max=1.)
    return projected * scale


def pack_futures_account_policy(
    base_weights: torch.Tensor, base_logits: torch.Tensor,
    coefficients: torch.Tensor, candidate_mask: torch.Tensor,
    known_notional: torch.Tensor, known_initial: torch.Tensor,
    known_maintenance: torch.Tensor, known_side_cost: torch.Tensor,
    prior_rms: torch.Tensor, prior_observation_fraction: torch.Tensor,
    *, net_notional_cap: float | None = None,
    beta_notional_cap: float | None = None, prior_beta: torch.Tensor | None = None,
) -> torch.Tensor:
    """Keep FP32 financial observations even when the backbone uses AMP."""
    if coefficients.shape != (*base_weights.shape, len(FUTURES_ACCOUNT_STATE_COLUMNS)):
        raise ValueError("account response coefficients must have shape [T,S,K]")
    scalar_fields = (base_weights, base_logits, candidate_mask, known_notional,
                     known_initial, known_maintenance, known_side_cost,
                     prior_rms, prior_observation_fraction)
    if any(field.shape != base_weights.shape for field in scalar_fields):
        raise ValueError("account policy observations must align with [T,S]")
    if beta_notional_cap is not None:
        if (net_notional_cap is not None or isinstance(beta_notional_cap, bool)
                or not math.isfinite(beta_notional_cap) or beta_notional_cap < 0
                or prior_beta is None or prior_beta.shape != base_weights.shape):
            raise ValueError('beta cap requires aligned prior beta and no net cap')
        base_weights = project_prior_net_notional(base_weights, candidate_mask,
            known_notional.detach(), known_initial.detach(), base_weights.new_tensor(beta_notional_cap),
            factor_beta=prior_beta)
    if net_notional_cap is not None:
        if isinstance(net_notional_cap, bool) or not math.isfinite(net_notional_cap) or net_notional_cap < 0:
            raise ValueError('prior net notional cap must be finite and nonnegative')
        base_weights = project_prior_net_notional(base_weights, candidate_mask,
            known_notional.detach(), known_initial.detach(), base_weights.new_tensor(net_notional_cap))
    packet = torch.cat((base_weights.float().unsqueeze(-1), base_logits.float().unsqueeze(-1),
                      coefficients.float(), candidate_mask.float().unsqueeze(-1),
                      *(field.detach().float().unsqueeze(-1) for field in scalar_fields[3:])), dim=-1)
    if net_notional_cap is not None:
        packet = torch.cat((packet, torch.full_like(base_weights.float().unsqueeze(-1), net_notional_cap)), -1)
    if beta_notional_cap is not None:
        packet = torch.cat((packet, prior_beta.detach().float().unsqueeze(-1),
            torch.full_like(base_weights.float().unsqueeze(-1), beta_notional_cap)), -1)
    return packet


def futures_account_state_features(
    packet_row: torch.Tensor, quantities: torch.Tensor,
    previous_equity: torch.Tensor, *, initial_capital: float, detach_state: bool = True,
    previous_inventory_marks: torch.Tensor | None = None,
) -> torch.Tensor:
    """Dimensionless observations of the exact pre-decision account.

    Integer inventory and settled NAV are observations, not differentiable
    teacher targets. asinh/log transforms stabilize inputs without imposing an
    exposure cap or a strategy. Percent and basis-point units are fixed units.
    Masked contracts still contribute to account-wide exposure when held.
    """
    row = packet_row.detach().float()
    q = (quantities.detach() if detach_state else quantities).float()
    nav = (previous_equity.detach() if detach_state else previous_equity).float().clamp_min(torch.finfo(torch.float32).tiny)
    notional = row[:, ACCOUNT_NOTIONAL].clamp_min(0)
    initial = row[:, ACCOUNT_INITIAL_MARGIN].clamp_min(0)
    maintenance = row[:, ACCOUNT_MAINTENANCE_MARGIN].clamp_min(0)
    if previous_inventory_marks is not None:
        # A frozen/unadmitted retained slot can have no current candidate
        # context. Its last settled physical mark still owns real account risk.
        previous = previous_inventory_marks.detach().float().nan_to_num()
        held = q != 0
        notional = torch.where(held & (notional <= 0), previous[:, 0].clamp_min(0), notional)
        initial = torch.where(held & (initial <= 0), previous[:, 1].clamp_min(0), initial)
        maintenance = torch.where(held & (maintenance <= 0), previous[:, 2].clamp_min(0), maintenance)
    cost = row[:, ACCOUNT_SIDE_COST].clamp_min(0)
    sigma = row[:, ACCOUNT_RMS].clamp_min(0)
    signed_notional = q * notional / nav
    signed_margin = q * initial / nav
    gross = signed_notional.abs().sum()
    net = signed_notional.sum()
    im = signed_margin.abs().sum()
    mm = (q.abs() * maintenance / nav).sum()
    broadcast = lambda value: value.expand_as(q)
    features = torch.stack((
        signed_notional.asinh(), signed_margin.asinh(), signed_margin.abs().asinh(),
        (initial / nav).asinh(), (q.abs() * 2 * cost / nav * 10_000).asinh(),
        (2 * cost / notional.clamp_min(1) * 10_000).asinh(),
        (notional / initial.clamp_min(1)).log1p(),
        (signed_notional.abs() * sigma).asinh(),
        broadcast((nav / float(initial_capital)).log()), broadcast(gross.asinh()),
        broadcast(net.asinh()), broadcast(im.asinh()), broadcast(mm.asinh()),
        broadcast((1 - im).asinh()), sigma * 100,
        row[:, ACCOUNT_OBSERVATION_FRACTION].clamp(0, 1),
    ), dim=-1)
    return torch.nan_to_num(features, nan=0.0, posinf=0.0, neginf=0.0)


def resolve_futures_account_policy(
    packet_row: torch.Tensor, quantities: torch.Tensor,
    previous_equity: torch.Tensor, *, initial_capital: float,
    alive: torch.Tensor, advance: torch.Tensor, detach_state: bool = True,
    previous_inventory_marks: torch.Tensor | None = None,
) -> torch.Tensor:
    """Resolve one action using the same output map as the market-only policy."""
    state = futures_account_state_features(packet_row, quantities, previous_equity,
                                          initial_capital=initial_capital, detach_state=detach_state,
                                          previous_inventory_marks=previous_inventory_marks)
    logits = packet_row[:, 1] + (packet_row[:, ACCOUNT_COEFFICIENT_START:ACCOUNT_COEFFICIENT_END] * state).sum(-1)
    mask = (packet_row[:, ACCOUNT_MASK] > 0.5) & alive & advance
    weights = masked_score_entmax_log_cash_weights(logits.unsqueeze(0), mask.unsqueeze(0)).squeeze(0)
    if packet_row.size(-1) == FUTURES_NET_EXPOSURE_PACKET_WIDTH:
        weights = project_prior_net_notional(weights, mask,
            packet_row[:, ACCOUNT_NOTIONAL].detach(), packet_row[:, ACCOUNT_INITIAL_MARGIN].detach(),
            packet_row[0, ACCOUNT_NET_NOTIONAL_CAP].detach())
    if packet_row.size(-1) == FUTURES_BETA_EXPOSURE_PACKET_WIDTH:
        weights = project_prior_net_notional(weights, mask,
            packet_row[:, ACCOUNT_NOTIONAL].detach(), packet_row[:, ACCOUNT_INITIAL_MARGIN].detach(),
            packet_row[0, ACCOUNT_BETA_NOTIONAL_CAP].detach(),
            factor_beta=packet_row[:, ACCOUNT_TX_BETA].detach())
    return weights
