from __future__ import annotations

from types import SimpleNamespace

import numpy as np

from stockagent.live import quote_provider


class _Contracts:
    def __init__(self, values):
        self.values = values

    def get(self, code):
        return self.values.get(str(code))


class _FakeApi:
    def __init__(self, contracts):
        self.contracts = _Contracts(contracts)
        self.batch_sizes: list[int] = []
        self.logged_out = False

    def snapshots(self, contracts):
        self.batch_sizes.append(len(contracts))
        return [
            SimpleNamespace(
                code=contract.code,
                open=100.0,
                high=102.0,
                low=99.0,
                close=101.0,
                total_volume=500.0,
                buy_price=100.5,
                sell_price=101.0,
                buy_volume=8.0,
                sell_volume=7.0,
            )
            for contract in contracts
        ]

    def logout(self):
        self.logged_out = True


def test_shioaji_stock_snapshots_batch_and_reuse_cache(monkeypatch, tmp_path):
    monkeypatch.setenv("STOCKAGENT_TW_PRICE_LIMIT_ROOT", str(tmp_path))
    monkeypatch.setattr(quote_provider, "_TW_LIMIT_CACHE_KEY", None)
    monkeypatch.setattr(quote_provider, "_TW_LIMIT_CACHE", {})
    symbols = [f"{idx:04d}" for idx in range(501)]
    contracts = {
        code: SimpleNamespace(
            code=code,
            reference=98.0,
            limit_up=107.5,
            limit_down=88.5,
        )
        for code in symbols
    }
    api = _FakeApi(contracts)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", api)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CONTRACTS", {})
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CACHE", {})

    first = quote_provider.fetch_shioaji_stock_snapshots(
        symbols,
        np.full((len(symbols),), 90.0, dtype=np.float64),
        cache_ttl_seconds=15.0,
    )
    second = quote_provider.fetch_shioaji_stock_snapshots(
        symbols,
        np.full((len(symbols),), 90.0, dtype=np.float64),
        cache_ttl_seconds=15.0,
    )

    assert api.batch_sizes == [500, 1]
    assert first.source.startswith("shioaji:stock_snapshot")
    assert first.available_count == 501
    assert np.all(first.open_prices == 100.0)
    assert np.all(first.bid_prices == 100.5)
    assert np.all(first.ask_prices == 101.0)
    assert np.all(first.upper_limit_prices == 107.5)
    assert np.all(first.lower_limit_prices == 88.5)
    assert np.all(first.timestamps_ms > 0)
    assert np.array_equal(second.prices, first.prices)


def test_shioaji_stock_snapshots_fail_closed_without_usable_rows(monkeypatch):
    api = _FakeApi({})
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", api)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CONTRACTS", {})
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CACHE", {})

    try:
        quote_provider.fetch_shioaji_stock_snapshots(
            ["missing"],
            np.asarray([1.0], dtype=np.float64),
        )
    except RuntimeError as exc:
        assert "no usable stock snapshots" in str(exc)
    else:
        raise AssertionError("missing Shioaji quotes must fail closed")


def test_shioaji_futures_snapshot_resolves_target_and_fetches_old_roll_contract(
    monkeypatch,
):
    logical = SimpleNamespace(code="TXFR1", target_code="TXFH6")
    current = SimpleNamespace(code="TXFH6")
    previous = SimpleNamespace(code="TXFG6")
    api = _FakeApi(
        {
            "TXFR1": logical,
            "TXFH6": current,
            "TXFG6": previous,
        }
    )
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", api)

    payload = quote_provider.fetch_shioaji_futures_snapshot(
        "TXFR1",
        additional_contract_codes=("TXFG6",),
    )

    assert payload["current_contract_code"] == "TXFH6"
    assert set(payload["quotes"]) == {"TXFH6", "TXFG6"}
    assert payload["quotes"]["TXFH6"]["bid"] == 100.5
    assert payload["quotes"]["TXFH6"]["ask"] == 101.0
    assert payload["source"].endswith("contract_v2_target")


def test_shioaji_stock_snapshots_restore_only_executable_locked_limit_side(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("STOCKAGENT_TW_PRICE_LIMIT_ROOT", str(tmp_path))
    monkeypatch.setattr(quote_provider, "_TW_LIMIT_CACHE_KEY", None)
    monkeypatch.setattr(quote_provider, "_TW_LIMIT_CACHE", {})
    contracts = {
        code: SimpleNamespace(
            code=code,
            reference=100.0,
            limit_up=110.0,
            limit_down=90.0,
        )
        for code in ("LIMIT_UP", "LIMIT_DOWN", "NOT_LOCKED")
    }

    class LockedLimitApi(_FakeApi):
        def snapshots(self, requested_contracts):
            self.batch_sizes.append(len(requested_contracts))
            rows = {
                "LIMIT_UP": SimpleNamespace(
                    code="LIMIT_UP",
                    open=100.0,
                    high=110.0,
                    low=100.0,
                    close=110.0,
                    total_volume=500.0,
                    buy_price=0.0,
                    sell_price=0.0,
                    buy_volume=1234.0,
                    sell_volume=0.0,
                ),
                "LIMIT_DOWN": SimpleNamespace(
                    code="LIMIT_DOWN",
                    open=100.0,
                    high=100.0,
                    low=90.0,
                    close=90.0,
                    total_volume=600.0,
                    buy_price=0.0,
                    sell_price=0.0,
                    buy_volume=0.0,
                    sell_volume=4321.0,
                ),
                "NOT_LOCKED": SimpleNamespace(
                    code="NOT_LOCKED",
                    open=100.0,
                    high=110.0,
                    low=99.0,
                    close=109.5,
                    total_volume=700.0,
                    buy_price=0.0,
                    sell_price=0.0,
                    buy_volume=999.0,
                    sell_volume=0.0,
                ),
            }
            return [rows[contract.code] for contract in requested_contracts]

    api = LockedLimitApi(contracts)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", api)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CONTRACTS", {})
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CACHE", {})

    snapshot = quote_provider.fetch_shioaji_stock_snapshots(
        list(contracts),
        np.full((len(contracts),), 100.0, dtype=np.float64),
    )

    np.testing.assert_allclose(snapshot.bid_prices[0], 110.0)
    assert np.isnan(snapshot.ask_prices[0])
    assert np.isnan(snapshot.bid_prices[1])
    np.testing.assert_allclose(snapshot.ask_prices[1], 90.0)
    assert np.isnan(snapshot.bid_prices[2])
    assert np.isnan(snapshot.ask_prices[2])
    assert snapshot.source.endswith("+locked_limit_book_repair")


def test_prepare_tw_price_limits_persists_only_static_metadata(monkeypatch, tmp_path):
    monkeypatch.setenv("STOCKAGENT_TW_PRICE_LIMIT_ROOT", str(tmp_path))
    monkeypatch.setattr(quote_provider, "_TW_LIMIT_CACHE_KEY", None)
    monkeypatch.setattr(quote_provider, "_TW_LIMIT_CACHE", {})
    calls: list[list[str]] = []

    def fake_mis(symbols, fallback_prices, **_kwargs):
        calls.append(list(symbols))
        count = len(symbols)
        return quote_provider.PriceSnapshot(
            prices=np.asarray(fallback_prices, dtype=np.float64),
            source="twse_tpex:mis",
            available_count=count,
            reference_prices=np.full((count,), 100.0),
            upper_limit_prices=np.full((count,), 110.0),
            lower_limit_prices=np.full((count,), 90.0),
        )

    monkeypatch.setattr(quote_provider, "fetch_tw_mis_last_prices", fake_mis)
    first = quote_provider.prepare_tw_price_limit_snapshot(
        ["2330", "2317"],
        np.asarray([100.0, 100.0]),
        parquet_root=tmp_path,
        trading_date="2026-08-12",
    )
    second = quote_provider.prepare_tw_price_limit_snapshot(
        ["2330", "2317"],
        np.asarray([100.0, 100.0]),
        parquet_root=tmp_path,
        trading_date="2026-08-12",
    )

    assert calls == [["2330", "2317"]]
    assert first["prepared_count"] == 2
    assert second["missing_count"] == 0
    assert (tmp_path / "2026-08-12.parquet").is_file()
