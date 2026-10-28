"""Lightweight container names shared by dated-futures producers and lifecycle."""

FUTURES_ACCOUNT_ARTIFACT_ABI = "stock_decision_futures_account_ledger_v3"
FUTURES_ACCOUNT_METRICS_ABI = "account_daily_returns_benchmark_nonmarket_carry_v1"
FUTURES_ACCOUNT_ARCHIVE_SCHEMA = 9
FUTURES_ACCOUNT_PREFIX = "futures_account_ledger_"
FUTURES_ACCOUNT_LEDGER_FIELDS = (
    "dates", "decision_dates", "account_only_mask", "strategy_returns", "decision_returns",
    "turnovers", "opening_quantities", "residual_quantities", "equity_scale", "default",
    "default_reason", "margin_audit", "final_quantities", "final_equity_scale", "final_alive",
    "calendar_fingerprint",
)
FUTURES_ACCOUNT_CONTEXT_FIELDS = (
    "universe", "event_source_fingerprint", "execution_release_fingerprint",
    "expected_calendar_fingerprint", "initial_capital", "initial_equity_scale", "initial_alive",
    "initial_quantities",
)
FUTURES_ACCOUNT_PAYLOAD_FIELDS = frozenset(
    f"{FUTURES_ACCOUNT_PREFIX}{name}"
    for name in (*FUTURES_ACCOUNT_LEDGER_FIELDS, *FUTURES_ACCOUNT_CONTEXT_FIELDS,
                 "abi", "metrics_abi", "margin_audit_columns")
)
