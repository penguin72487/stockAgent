from datetime import datetime
from types import SimpleNamespace

import numpy as np
import pytest

from stockagent.live import signal_engine as signals
from stockagent.live.quote_provider import PriceSnapshot


def panel():
    return SimpleNamespace(
        num_symbols=4, symbols=["OPEN", "NO_PRINT", "UNKNOWN", "MISSING_DAY"],
        dates=np.array(["2026-10-19", "2026-10-20", "2026-10-21"], dtype="datetime64[D]"),
        close_prices=np.array([[100., 49., 60., 70.], [101., 50., 61., np.nan], [102., 51., 62., 72.]]),
        daily_volumes=np.array([[10., 10., 10., 10.], [10., 0., 0., 0.], [10., 0., 0., 0.]]),
        alive_mask=np.array([[True]*4, [True, True, True, False], [True]*4]),
        feature_names=[signals.DAY_TRADE_OPEN_GAP_FEATURE], features=np.zeros((3, 4, 1), dtype=np.float32))


def snapshot():
    stamp = int(datetime.fromisoformat("2026-10-22T09:00:00.100+08:00").timestamp()*1000)
    return PriceSnapshot(source="opening:shared_opening_snapshot", timestamp="2026-10-22T09:00:00.100+08:00",
        prices=np.array([104., 500., 600., 700.]),  # Deliberately not trade/open references.
        reference_prices=np.full(4, 999.), open_prices=np.array([103., np.nan, np.nan, np.nan]),
        available_mask=np.array([True, True, False, True]), timestamps_ms=np.full(4, stamp),
        exchange_timestamps_ms=np.array([stamp, stamp, 0, stamp]),
        no_open_print_timestamps_ms=np.array([0, stamp, 0, stamp]))


def test_last_trade_skips_zero_volume_adjusted_references_and_missing_observations():
    data = panel()
    prices, dates = signals._last_completed_trade_reference(data, 2)
    np.testing.assert_allclose(prices, [102., 49., 60., 70.])
    assert dates.astype(str).tolist() == ["2026-10-21", "2026-10-19", "2026-10-19", "2026-10-19"]
    assert not prices.flags.writeable and not dates.flags.writeable
    with pytest.raises(ValueError):
        prices[1] = 999.


def test_no_first_print_uses_last_real_trade_not_midpoint_auction_reference_or_unknown():
    data, quotes = panel(), snapshot()
    before = quotes.open_prices.copy()
    window, _, _, live, observed = signals._day_trade_live_model_window(data,
        panel_idx=2, lookback=3, price_snapshot=quotes,
        resolved_asof=quotes.timestamp, source_timezone="Asia/Taipei")
    assert live
    np.testing.assert_array_equal(observed, [True, False, False, False])
    assert window[-1, 0, 0] == pytest.approx(np.log(103./102.))
    assert window[-1, 1, 0] == pytest.approx(np.log(49./51.))
    assert window[-1, 2, 0] == 0.  # Source unknown is not no-first-print proof.
    assert window[-1, 3, 0] == pytest.approx(np.log(70./72.))
    np.testing.assert_array_equal(quotes.open_prices, before)
    assert (data.features == 0.).all()


def test_actual_first_print_supersedes_carried_model_reference():
    data, quotes = panel(), snapshot()
    quotes.open_prices[1] = 52.
    window, _, _, _, observed = signals._day_trade_live_model_window(data,
        panel_idx=2, lookback=3, price_snapshot=quotes,
        resolved_asof=quotes.timestamp, source_timezone="Asia/Taipei")
    assert observed[1] and window[-1, 1, 0] == pytest.approx(np.log(52./51.))


@pytest.mark.parametrize("volume", [None, 0., np.nan, -1., np.inf])
def test_missing_or_invalid_volume_cannot_prove_last_trade(volume):
    data = panel()
    data.daily_volumes = None if volume is None else np.full((3, 4), volume)
    prices, dates = signals._last_completed_trade_reference(data, 2)
    assert np.isnan(prices).all() and np.isnat(dates).all()


def test_last_trade_reference_is_causal_and_cache_invalidates_on_panel_replacement():
    data = panel()
    prices, dates = signals._last_completed_trade_reference(data, 0)
    np.testing.assert_array_equal(prices, [100., 49., 60., 70.])
    assert (dates == np.datetime64("2026-10-19")).all()
    cached = signals._last_completed_trade_reference(data, 0)
    assert cached[0] is prices
    data.close_prices = data.close_prices.copy()
    data.close_prices[0, 1] = 48.
    assert signals._last_completed_trade_reference(data, 0)[0][1] == 48.
    signals.clear_live_panel_memory_cache()
    assert not signals._LIVE_LAST_TRADE_REFERENCE_CACHE


def test_reference_requires_aligned_rows():
    data = panel()
    data.daily_volumes = np.ones((1, 4))
    with pytest.raises(ValueError, match="aligned completed panel rows"):
        signals._last_completed_trade_reference(data, 2)


def test_missing_panel_close_uses_real_trade_as_carry_gap_denominator():
    data, quotes = panel(), snapshot()
    data.close_prices[2, 1] = np.nan
    window, _, _, _, observed = signals._day_trade_live_model_window(data,
        panel_idx=2, lookback=3, price_snapshot=quotes,
        resolved_asof=quotes.timestamp, source_timezone="Asia/Taipei")
    assert not observed[1] and window[-1, 1, 0] == 0.


def test_missing_latest_day_is_not_evidence_of_delisting_or_loss_of_last_trade():
    data = panel()
    data.alive_mask[2, 1] = False
    data.close_prices[2, 1] = np.nan
    prices, dates = signals._last_completed_trade_reference(data, 2)
    assert prices[1] == 49. and dates[1] == np.datetime64("2026-10-19")


@pytest.mark.parametrize("missing_open_column", [False, True])
def test_dense_and_factorized_overlays_share_carried_reference_without_source_mutation(missing_open_column):
    from stockagent.data.factorized_panel import FactorizedLiveWindow

    data, quotes = panel(), snapshot()
    if missing_open_column:
        quotes.open_prices = None
    before = data.features.tobytes()
    kwargs = dict(panel_idx=2, lookback=3, price_snapshot=quotes,
        resolved_asof=quotes.timestamp, source_timezone="Asia/Taipei")
    dense = signals._day_trade_live_model_window(data, **kwargs)
    lazy = signals._day_trade_live_model_window(data,
        base_model_window=FactorizedLiveWindow(data.features, 0, 3), **kwargs)
    np.testing.assert_array_equal(lazy[0][-1], dense[0][-1])
    assert dense[0][-1, 1, 0] == pytest.approx(np.log(49./51.))
    assert data.features.tobytes() == before
