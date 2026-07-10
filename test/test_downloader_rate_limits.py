from downloader.common import (
    DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
    provider_rate_limit,
    resolve_request_interval,
)


def test_documented_provider_defaults_use_exact_average_limit() -> None:
    okx = provider_rate_limit("okx_history_candles")
    bybit = provider_rate_limit("bybit_public_rest")
    alpaca = provider_rate_limit("alpaca_market_data_basic")

    assert round(okx.requests_per_second, 2) == 10.00
    assert round(bybit.requests_per_second, 2) == 120.00
    assert alpaca.requests_per_second == 200 / 60
    assert resolve_request_interval("okx_history_candles", None) == okx.interval_seconds
    assert resolve_request_interval("bybit_public_rest", None) == bybit.interval_seconds
    assert resolve_request_interval("alpaca_market_data_basic", None) == alpaca.interval_seconds


def test_requested_interval_is_clamped_to_configured_limit() -> None:
    okx = provider_rate_limit("okx_history_candles")
    assert resolve_request_interval("okx_history_candles", 0.001) == okx.interval_seconds
    assert resolve_request_interval("okx_history_candles", 1.0) == 1.0


def test_unpublished_and_unknown_providers_default_to_ten_rps() -> None:
    providers = (
        "frankfurter_public",
        "tw_public",
        "future_provider_without_profile",
    )

    for provider in providers:
        profile = provider_rate_limit(provider)
        assert profile.requests_per_second == DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND
        assert resolve_request_interval(provider, None) == 0.1
