from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

from downloader import download_bybit_perp_daily as bybit
from downloader import download_okx_perp_daily as okx
from downloader import download_yahoo_ohlcv as yahoo
from stockagent.config import load_config
from stockagent.live.market_config import load_market_config


def test_crypto_market_config_is_15m() -> None:
    config = load_config("configs/markets/crypto.yaml")

    assert config.trading.frequency == "15m"
    assert config.data.parquet_root == "data_okx"


def test_discord_crypto_market_uses_15m_incremental_updater() -> None:
    cfg = load_market_config("services/discord_bot/markets/crypto.yaml")

    assert cfg.pre_signal_command[0] == "{python}"
    assert cfg.schedule_interval_minutes == 15
    assert cfg.history_frequency == "bar"
    assert "downloader/download_okx_perp_15m.py" in cfg.pre_signal_command
    assert "incremental" in cfg.pre_signal_command


def test_discord_tw_market_uses_canonical_official_data_layer() -> None:
    cfg = load_market_config("services/discord_bot/markets/tw.yaml")
    assert cfg.pre_signal_command == (
        "{python}",
        "scripts/refresh_tw_public_live_snapshot.py",
        "--config",
        "configs/markets/tw_day_trade_10m.yaml",
    )


def test_discord_tw_day_trade_uses_its_point_in_time_data_contract() -> None:
    cfg = load_market_config("services/discord_bot/markets/tw_day_trade.yaml")
    assert cfg.pre_signal_command == load_market_config(
        "services/discord_bot/markets/tw.yaml"
    ).pre_signal_command


def test_discord_tw_day_trade_1m_uses_its_point_in_time_data_contract() -> None:
    cfg = load_market_config("services/discord_bot/markets/tw_day_trade_1m.yaml")
    assert cfg.pre_signal_command == load_market_config(
        "services/discord_bot/markets/tw.yaml"
    ).pre_signal_command


def test_discord_tw_day_trade_100m_uses_its_point_in_time_data_contract() -> None:
    cfg = load_market_config("services/discord_bot/markets/tw_day_trade_100m.yaml")
    assert cfg.pre_signal_command == load_market_config(
        "services/discord_bot/markets/tw.yaml"
    ).pre_signal_command


def test_discord_tw_day_trade_multi_basis_uses_its_point_in_time_data_contract() -> None:
    cfg = load_market_config("services/discord_bot/markets/tw_day_trade_multi_basis.yaml")
    assert cfg.pre_signal_command == load_market_config(
        "services/discord_bot/markets/tw.yaml"
    ).pre_signal_command


def test_daily_downloader_keeps_tw_out_of_legacy_yahoo_tree() -> None:
    script = Path("downloader/run_daily_all_markets.sh").read_text(encoding="utf-8")

    assert 'TW_PUBLIC_OUTPUT_DIR="${TW_PUBLIC_OUTPUT_DIR:-data_tw_public}"' in script
    assert 'TW_PUBLIC_STOCKS_ROOT="${TW_PUBLIC_STOCKS_ROOT:-data_tw_public/stocks}"' in script
    assert 'if [[ " $YAHOO_ASSETS " == *" tw_stocks "* ]]; then' in script
    assert "--no-include-tw-delisted" in script


def test_discord_daily_markets_use_canonical_downloaders_without_audit() -> None:
    expected_downloaders = {
        "us": "downloader/download_alpaca_us_ohlcv.py",
        "forex": "downloader/download_forex_frankfurter.py",
    }
    for market, downloader in expected_downloaders.items():
        cfg = load_market_config(f"services/discord_bot/markets/{market}.yaml")
        assert cfg.pre_signal_command[0] == "{python}"
        command = " ".join(cfg.pre_signal_command)
        assert downloader in command
        assert "--mode daily-update" in command
        assert "audit_ohlcv_data.py" not in command


def test_live_market_config_rejects_unknown_typo_key(tmp_path: Path) -> None:
    path = tmp_path / "tw.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "market": "tw",
                "config_path": "configs/markets/tw.yaml",
                "freshness_max_lag_day": 3,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"Unknown market config key.*freshness_max_lag_day"):
        load_market_config(path)


def test_live_market_config_rejects_unsupported_nested_keys(tmp_path: Path) -> None:
    path = tmp_path / "tw.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "market": "tw",
                "config_path": "configs/markets/tw.yaml",
                "pre_signal_command": {"command": ["{python}", "downloader/example.py"]},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"Nested market config key.*pre_signal_command.command"):
        load_market_config(path)


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


def test_crypto_downloader_overlap_preserves_historical_feature_columns() -> None:
    existing = okx.pl.DataFrame(
        {
            "date": ["2026-06-22 00:00:00", "2026-06-22 00:15:00"],
            "open": [100.0, 110.0],
            "max": [101.0, 111.0],
            "min": [99.0, 109.0],
            "close": [100.5, 110.5],
            "adjclose": [100.5, 110.5],
            "Trading_Volume": [10.0, 1.0],
            "okx_open_interest_usd": [1000.0, 1100.0],
        }
    )
    fresh = existing.select(
        [
            "date",
            "open",
            "max",
            "min",
            "close",
            "adjclose",
            "Trading_Volume",
        ]
    ).with_columns(okx.pl.lit(20.0).alias("Trading_Volume"))

    merged, changed = okx._merge_existing_with_fresh(
        existing,
        fresh,
        okx._date_to_ms("2026-06-22", end_of_day=False),
    )

    assert changed
    assert merged["okx_open_interest_usd"].to_list() == [1000.0, 1100.0]
    assert merged["Trading_Volume"].to_list() == [20.0, 20.0]
