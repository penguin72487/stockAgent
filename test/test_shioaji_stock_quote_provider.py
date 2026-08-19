from __future__ import annotations

import sys
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


def _reset_shioaji_connection_state(monkeypatch):
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", None)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CONTRACTS", {})
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CACHE", {})
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_LOGIN_RETRY_AFTER", 0.0)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_LAST_LOGIN_ERROR", None)


def test_shioaji_warm_client_validates_and_replaces_expired_session(monkeypatch):
    class ExpiredApi:
        def __init__(self):
            self.logged_out = False

        def usage(self):
            raise RuntimeError("401 Token is expired")

        def logout(self):
            self.logged_out = True

    class FreshApi:
        def __init__(self, *, simulation):
            assert simulation is True
            self.login_kwargs = None
            self.usage_calls = 0

        def set_event_callback(self, _callback):
            return None

        def login(self, **kwargs):
            self.login_kwargs = kwargs

        def usage(self):
            self.usage_calls += 1
            return 0

        def logout(self):
            return None

    expired = ExpiredApi()
    fresh_instances: list[FreshApi] = []

    def create_fresh(*, simulation):
        api = FreshApi(simulation=simulation)
        fresh_instances.append(api)
        return api

    _reset_shioaji_connection_state(monkeypatch)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", expired)
    monkeypatch.setitem(sys.modules, "shioaji", SimpleNamespace(Shioaji=create_fresh))
    monkeypatch.setenv("SHIOAJI_API_KEY", "test-key")
    monkeypatch.setenv("SHIOAJI_SECRET_KEY", "test-secret")

    quote_provider.warm_shioaji_stock_quote_client()

    assert expired.logged_out is True
    assert len(fresh_instances) == 1
    assert fresh_instances[0].login_kwargs == {
        "api_key": "test-key",
        "secret_key": "test-secret",
        "subscribe_trade": False,
        "force_refresh": True,
    }
    assert fresh_instances[0].usage_calls == 1
    assert quote_provider._SHIOAJI_STOCK_API is fresh_instances[0]


def test_shioaji_stock_snapshot_reconnects_once_after_session_failure(
    monkeypatch, tmp_path
):
    contract = SimpleNamespace(
        code="2330",
        reference=100.0,
        limit_up=110.0,
        limit_down=90.0,
    )

    class ExpiredApi(_FakeApi):
        def snapshots(self, contracts):
            raise RuntimeError("SessionNotEstablished")

    class FreshApi(_FakeApi):
        def __init__(self, *, simulation):
            assert simulation is True
            super().__init__({"2330": contract})
            self.login_kwargs = None

        def set_event_callback(self, _callback):
            return None

        def login(self, **kwargs):
            self.login_kwargs = kwargs

    expired = ExpiredApi({"2330": contract})
    fresh_instances: list[FreshApi] = []

    def create_fresh(*, simulation):
        api = FreshApi(simulation=simulation)
        fresh_instances.append(api)
        return api

    monkeypatch.setenv("STOCKAGENT_TW_PRICE_LIMIT_ROOT", str(tmp_path))
    monkeypatch.setattr(quote_provider, "_TW_LIMIT_CACHE_KEY", None)
    monkeypatch.setattr(quote_provider, "_TW_LIMIT_CACHE", {})
    _reset_shioaji_connection_state(monkeypatch)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", expired)
    monkeypatch.setitem(sys.modules, "shioaji", SimpleNamespace(Shioaji=create_fresh))
    monkeypatch.setenv("SHIOAJI_API_KEY", "test-key")
    monkeypatch.setenv("SHIOAJI_SECRET_KEY", "test-secret")

    snapshot = quote_provider.fetch_shioaji_stock_snapshots(
        ["2330"],
        np.asarray([99.0], dtype=np.float64),
    )

    assert expired.logged_out is True
    assert len(fresh_instances) == 1
    assert fresh_instances[0].login_kwargs["force_refresh"] is True
    assert snapshot.available_count == 1
    np.testing.assert_allclose(snapshot.bid_prices, [100.5])


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


def test_shioaji_limits_never_derive_from_panel_close_without_official_reference(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("STOCKAGENT_TW_PRICE_LIMIT_ROOT", str(tmp_path))
    monkeypatch.setattr(quote_provider, "_TW_LIMIT_CACHE_KEY", None)
    monkeypatch.setattr(quote_provider, "_TW_LIMIT_CACHE", {})
    contract = SimpleNamespace(code="2330")
    api = _FakeApi({"2330": contract})
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", api)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CONTRACTS", {})
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CACHE", {})

    snapshot = quote_provider.fetch_shioaji_stock_snapshots(
        ["2330"],
        np.asarray([100.0], dtype=np.float64),
    )

    assert np.isnan(snapshot.reference_prices[0])
    assert np.isnan(snapshot.upper_limit_prices[0])
    assert np.isnan(snapshot.lower_limit_prices[0])


def test_shioaji_futures_snapshot_resolves_target_and_fetches_old_roll_contract(
    monkeypatch,
):
    logical = SimpleNamespace(code="TXFR1", target_code="TXFH6")
    current = SimpleNamespace(
        code="TXFH6",
        delivery_month="202608",
        delivery_date=None,
        last_trading_date=None,
    )
    previous = SimpleNamespace(
        code="TXFG6",
        delivery_month="202607",
        delivery_date=None,
        last_trading_date=None,
    )
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
    assert payload["quotes"]["TXFH6"]["delivery_month"] == "202608"
    assert payload["quotes"]["TXFG6"]["delivery_month"] == "202607"
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
