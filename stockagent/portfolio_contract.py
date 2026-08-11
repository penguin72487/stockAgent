from __future__ import annotations


DEFAULT_PORTFOLIO_ACTIVATION = "identity"


def normalize_portfolio_mode(portfolio_mode: str | None) -> str:
    """Normalize the shared model portfolio-direction name."""
    normalized = str(portfolio_mode or "").strip().lower().replace("-", "_")
    if normalized in {"long", "long_only", "longonly"}:
        return "long_only"
    if normalized in {"long_short", "longshort", "short", "dual_branch", "long_and_short"}:
        return "long_short"
    raise ValueError("portfolio_mode must be 'long_only' or 'long_short'")


def normalize_portfolio_output_mode(mode: str | None) -> str:
    """Normalize the shared trainable portfolio-output representation name."""
    normalized = str(mode or "activation_l1").strip().lower().replace("-", "_")
    if normalized in {
        "activation_l1",
        "activated_l1",
        "activation",
        "bounded_l1",
        "bounded_activation_l1",
        "default",
    }:
        return "activation_l1"
    if normalized in {"l1", "raw_l1", "score_l1", "linear_l1", "identity_l1"}:
        return "l1"
    if normalized in {
        "cash_l1",
        "explicit_cash_l1",
        "learned_cash_l1",
        "cash_asset_l1",
    }:
        return "cash_l1"
    if normalized in {"logits", "raw_logits", "scores", "raw_scores", "score_logits"}:
        return "logits"
    if normalized in {"signed_softmax", "signed_action_softmax", "action_softmax"}:
        return "signed_softmax"
    if normalized in {"signed_sparsemax", "signed_action_sparsemax", "action_sparsemax", "sparsemax"}:
        return "signed_sparsemax"
    if normalized in {
        "signed_entmax",
        "signed_entmax15",
        "signed_entmax_15",
        "signed_action_entmax",
        "signed_action_entmax15",
        "action_entmax",
        "action_entmax15",
        "entmax",
        "entmax15",
        "entmax_15",
    }:
        return "signed_entmax15"
    if normalized in {
        "projection",
        "projection_l1",
        "l1_projection",
        "project_l1",
        "differentiable_projection",
        "differentiable_l1_projection",
    }:
        return "projection_l1"
    raise ValueError(
        "portfolio_output_mode must be 'activation_l1', 'l1', 'cash_l1', 'logits', "
        "'signed_softmax', 'signed_sparsemax', 'signed_entmax15', or 'projection_l1'"
    )


def normalize_portfolio_activation(activation: str | None) -> str:
    """Normalize the shared config/model/backtest portfolio activation name."""
    normalized = str(activation or DEFAULT_PORTFOLIO_ACTIVATION).strip().lower().replace("-", "_")
    aliases = {
        "arc_tan": "atan",
        "arctan": "atan",
        "erf_scaled": "erf",
        "gd": "gudermannian",
        "already_normalized": "pre_normalized",
        "inverse_square_root_unit": "isru",
        "inverse_sqrt": "isru",
        "inverse_sqrt_unit": "isru",
        "isr": "isru",
        "isru1": "isru",
        "pre_normalized_weights": "pre_normalized",
        "preserve": "pre_normalized",
        "preserve_weights": "pre_normalized",
        "soft_sign": "softsign",
        "weights": "pre_normalized",
        "x_over_1_abs_x": "softsign",
        "x_over_sqrt_1_x2": "isru",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized in {"identity", "linear", "none", "raw"}:
        return "identity"
    valid = {"tanh", "softsign", "isru", "erf", "atan", "gudermannian", "pre_normalized"}
    if normalized not in valid:
        raise ValueError(
            "portfolio activation must be one of "
            "'identity', 'tanh', 'softsign', 'isru', 'erf', 'atan', 'gd', or 'pre_normalized'"
        )
    return normalized
