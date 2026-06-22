from __future__ import annotations

import sys

from downloader import download_bybit_perp_daily as bybit
from downloader import download_okx_perp_daily as okx
from downloader import download_yahoo_ohlcv as yahoo
from stockagent.config import load_config
from stockagent.live.market_config import load_market_config


def test_crypto_market_config_is_15m() -> None:
    config = load_config("configs/markets/crypto.yaml")

    assert config.trading.frequency == "15m"
    assert config.training.target == "next_15m_rank"
    assert config.data.universe_mode == "all_15m_symbols"


def test_discord_crypto_market_uses_15m_incremental_updater() -> None:
    cfg = load_market_config("services/discord_bot/markets/crypto.yaml")

    assert cfg.schedule_interval_minutes == 15
    assert cfg.history_frequency == "bar"
    assert "downloader/download_okx_perp_15m.py" in cfg.pre_signal_command
    assert "incremental" in cfg.pre_signal_command


def test_discord_daily_markets_use_downloader_without_audit() -> None:
    for market, asset in (("tw", "tw_stocks"), ("us", "us_stocks"), ("forex", "forex")):
        cfg = load_market_config(f"services/discord_bot/markets/{market}.yaml")
        command = " ".join(cfg.pre_signal_command)
        assert "downloader/download_yahoo_ohlcv.py" in command
        assert "--mode daily-update" in command
        assert f"--asset {asset}" in command
        assert "audit_ohlcv_data.py" not in command


def test_crypto_downloaders_accept_incremental_15m_mode(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["download_yahoo_ohlcv.py", "--asset", "crypto", "--mode", "incremental"])
    yahoo_args = yahoo.parse_args()
    assert yahoo_args.asset == "crypto"
    assert yahoo_args.mode == "incremental"
    assert yahoo._is_incremental_mode(yahoo_args)

    monkeypatch.setattr(sys, "argv", ["download_okx_perp_daily.py", "--mode", "incremental"])
    okx_args = okx.parse_args()
    assert okx_args.mode == "incremental"

    monkeypatch.setattr(sys, "argv", ["download_bybit_perp_daily.py", "--mode", "incremental"])
    bybit_args = bybit.parse_args()
    assert bybit_args.mode == "incremental"


def test_crypto_downloader_overlap_replaces_existing_tail() -> None:
    existing = okx.pl.DataFrame(
        {
            "date": ["2026-06-22 00:00:00", "2026-06-22 00:15:00"],
            "open": [100.0, 110.0],
            "max": [101.0, 111.0],
            "min": [99.0, 109.0],
            "close": [100.5, 110.5],
            "adjclose": [100.5, 110.5],
            "Trading_Volume": [10.0, 1.0],
        }
    )
    fresh = okx.pl.DataFrame(
        {
            "date": ["2026-06-22 00:00:00", "2026-06-22 00:15:00"],
            "open": [100.0, 110.0],
            "max": [102.0, 112.0],
            "min": [98.0, 108.0],
            "close": [101.0, 111.0],
            "adjclose": [101.0, 111.0],
            "Trading_Volume": [12.0, 20.0],
        }
    )
    effective_start_ms = okx._date_to_ms("2026-06-22", end_of_day=False)

    merged, changed = okx._merge_existing_with_fresh(existing, fresh, effective_start_ms)

    assert changed
    assert merged.height == 2
    assert merged.filter(okx.pl.col("date") == "2026-06-22 00:15:00").select("Trading_Volume").item() == 20.0
