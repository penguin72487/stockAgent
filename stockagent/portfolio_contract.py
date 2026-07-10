from __future__ import annotations


DEFAULT_PORTFOLIO_ACTIVATION = "identity"


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
