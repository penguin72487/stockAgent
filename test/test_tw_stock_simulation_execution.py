from dataclasses import replace
from datetime import datetime, timedelta
from types import SimpleNamespace
import sys
import threading

import pytest

from stockagent.live import tw_stock_simulation_execution as execution
from stockagent.live.tw_stock_simulation_execution import (
    BROKER_FILL, LOCAL_ODD_FILL, TAIPEI, ExecutionEvidenceError,
    ShioajiStockSimulationSession, StockSimulationIntent, StockSimulationJournal,
    account_fingerprint, odd_lot_residual_fill,
    legacy_inventory_cutover_gate,
)


NOW = datetime(2026, 9, 10, 9, 1, tzinfo=TAIPEI)
ACCOUNT = account_fingerprint("simulation-broker", "simulation-account")


def intent(**changes):
    return replace(StockSimulationIntent("signal:entry:2330", "test-mode", "2330",
                   NOW - timedelta(seconds=1), 2000, "Buy"), **changes)


@pytest.fixture
def journal(tmp_path, monkeypatch):
    monkeypatch.setenv("STOCKAGENT_SHIOAJI_SIMULATION_OWNER_DIR", str(tmp_path / "owner"))
    store = StockSimulationJournal(tmp_path / "sim.sqlite", account_key=ACCOUNT)
    store._test_sessions = []
    yield store
    for session in store._test_sessions:
        session.logout()
    store.close()


def claim(journal, order=None):
    order = order or intent()
    row = journal.prepare(order)
    journal.claim(order.key)
    return row


def deal(tag="S00001", **changes):
    return {"custom_field": tag, "trade_id": "broker-trade-1", "exchange_seq": "1",
            "broker_id": "simulation-broker", "account_id": "simulation-account",
            "code": "2330", "action": "Buy", "order_cond": "Cash", "order_lot": "Common",
            "price": 1000, "quantity": 1, "ts": NOW.timestamp()} | changes


def odd(**changes):
    return odd_lot_residual_fill(**(dict(position_id="carried-position-1", symbol="2330",
        signed_inventory=1500, shares=500, action="Sell", decision_at=NOW - timedelta(seconds=2),
        observed_at=NOW, quote_at=NOW - timedelta(seconds=1), bid=999, ask=1000,
        source="shioaji_bidask", regular_trade=True) | changes))


@pytest.mark.parametrize("quantity", [1, 500, 1500, True, 1.5, float("nan"), -1000, 0])
def test_no_non_board_lot_broker_order(quantity):
    with pytest.raises(ValueError):
        intent(shares=quantity).payload()


@pytest.mark.parametrize("changes", [dict(price_type="MKT", price="1000"),
    dict(price_type="MKT", order_type="ROD"), dict(action="Sell", order_cond="ShortSelling", daytrade_short=True),
    dict(price_type="LMT", price="1001"), dict(signal_at=NOW.replace(tzinfo=None))])
def test_invalid_broker_intent_cannot_be_prepared(journal, changes):
    with pytest.raises(ValueError):
        journal.prepare(intent(**changes))
    assert journal.status()["orders"] == []


def test_source_price_is_product_and_date_specific():
    assert intent(price_type="LMT", price="50.05", security_type="etf").payload()["price"] == "50.05"
    with pytest.raises(ValueError, match="tick"):
        intent(price_type="LMT", price="50.05", security_type="stock").payload()
    # Before 2005-03-01 a stock at 10.01 was not on the old 0.05 grid.
    with pytest.raises(ValueError, match="tick"):
        intent(price_type="LMT", price="10.01", signal_at=NOW.replace(year=2004)).payload()
    assert intent(price_type="LMT", price="9.99").payload()["price"] == "9.99"


def test_odd_residual_keeps_board_lots_and_exact_source_identity():
    receipt = odd()
    assert receipt["shares"] == 500
    assert receipt["price"] == "999"
    assert receipt["inventory_after"] == 1000
    assert receipt["fill_contract"] == LOCAL_ODD_FILL
    assert receipt["broker_confirmed"] is False
    short = odd(signed_inventory=-1500, action="Buy")
    assert short["price"] == "1000"
    assert short["inventory_after"] == -1000


@pytest.mark.parametrize("changes", [dict(signed_inventory=1000), dict(signed_inventory=0),
    dict(shares=501), dict(shares=1000), dict(shares=1500), dict(action="Buy"),
    dict(signed_inventory=1500.5), dict(regular_trade=False), dict(bid=1001),
    dict(quote_at=NOW + timedelta(seconds=1)), dict(quote_at=NOW - timedelta(minutes=1)),
    dict(quote_at=NOW - timedelta(seconds=2)), dict(bid=1005, ask=1000),
    dict(observed_at=NOW.replace(hour=13, minute=25)), dict(source=""),
    dict(security_type="warrant"), dict(position_id="")])
def test_local_exception_never_becomes_general_fallback(changes):
    with pytest.raises((ValueError, ExecutionEvidenceError)):
        odd(**changes)


def test_acknowledgement_cannot_create_any_fill(journal):
    row = claim(journal)
    assert journal.order_ack(tag=row["tag"], trade_id="broker-trade-1", account_key=ACCOUNT,
                             symbol="2330", action="Buy")
    assert journal.receipts() == []
    assert journal.status()["orders"][0]["state"] == "submitted"
    with pytest.raises(ExecutionEvidenceError, match="already claimed"):
        journal.claim(intent().key)


def test_deal_before_ack_duplicates_and_restart_are_idempotent(tmp_path):
    path = tmp_path / "sim.sqlite"
    journal = StockSimulationJournal(path, account_key=ACCOUNT)
    row = claim(journal)
    assert journal.stock_deal(deal(row["tag"]), received_at=NOW)
    assert not journal.stock_deal(deal(row["tag"]), received_at=NOW + timedelta(seconds=1))
    assert journal.status()["orders"][0]["state"] == "partially_filled"
    journal.close()
    journal = StockSimulationJournal(path, account_key=ACCOUNT)
    assert not journal.stock_deal(deal(row["tag"]), received_at=NOW)
    assert journal.stock_deal(deal(row["tag"], exchange_seq="2"), received_at=NOW)
    assert journal.order_ack(tag=row["tag"], trade_id="broker-trade-1", account_key=ACCOUNT,
                             symbol="2330", action="Buy")
    receipts = journal.receipts()
    assert len(receipts) == 2
    assert sum(r["shares"] for r in receipts) == 2000
    assert all(r["fill_contract"] == BROKER_FILL and r["broker_confirmed"] for r in receipts)
    assert journal.receipts(after_sequence=receipts[0]["sequence"]) == receipts[1:]
    assert journal.status()["orders"][0]["state"] == "filled"
    journal.close()


def test_broker_simulation_uses_reported_deals_without_local_capacity_limit(journal):
    # Shioaji simulation owns fill decisions. Local L1 depth and completed-minute
    # volume are deliberately absent from the broker order/receipt contract.
    large = intent(shares=200_000)
    row = claim(journal, large)
    assert journal.stock_deal(deal(row["tag"], quantity=1), received_at=NOW)
    assert journal.status()["orders"][0]["filled_shares"] == 1_000
    assert journal.status()["orders"][0]["state"] == "partially_filled"
    assert journal.stock_deal(
        deal(row["tag"], quantity=199, exchange_seq="2"), received_at=NOW
    )
    assert journal.status()["orders"][0]["filled_shares"] == 200_000
    assert journal.status()["orders"][0]["state"] == "filled"
    assert sum(receipt["shares"] for receipt in journal.receipts()) == 200_000
    assert all(receipt["broker_confirmed"] for receipt in journal.receipts())


def test_unknown_submission_blocks_same_key_and_disguised_retry_across_restart(tmp_path):
    path = tmp_path / "sim.sqlite"
    journal = StockSimulationJournal(path, account_key=ACCOUNT)
    row = claim(journal)
    journal.unknown(intent().key)
    journal.close()
    journal = StockSimulationJournal(path, account_key=ACCOUNT)
    with pytest.raises(ExecutionEvidenceError, match="already claimed"):
        journal.claim(intent().key)
    new = intent(key="a-new-key", symbol="2317")
    journal.prepare(new)
    with pytest.raises(ExecutionEvidenceError, match="unresolved"):
        journal.claim(new.key)
    # A genuine broker deal resolves the submission uncertainty; not a quote.
    assert journal.stock_deal(deal(row["tag"], quantity=2), received_at=NOW)
    journal.claim(new.key)
    journal.close()


@pytest.mark.parametrize("changes", [dict(code="2317"), dict(action="Sell"),
    dict(order_lot="IntradayOdd"), dict(order_cond="ShortSelling"), dict(exchange_seq=""),
    dict(price=1001), dict(quantity=3), dict(quantity=0.5), dict(ts=NOW.timestamp() + 1),
    dict(ts=NOW.timestamp() - 86400), dict(ts=float("nan")), dict(ts=None)])
def test_invalid_or_misattributed_deal_never_changes_quantity(journal, changes):
    row = claim(journal)
    with pytest.raises((ValueError, ExecutionEvidenceError)):
        journal.stock_deal(deal(row["tag"], **changes), received_at=NOW)
    assert journal.receipts() == []
    assert journal.status()["orders"][0]["filled_shares"] == 0


def test_conflicting_duplicate_does_not_silently_change_price(journal):
    row = claim(journal)
    journal.stock_deal(deal(row["tag"]), received_at=NOW)
    with pytest.raises(ExecutionEvidenceError, match="conflicting duplicate"):
        journal.stock_deal(deal(row["tag"], price=1005), received_at=NOW)
    assert len(journal.receipts()) == 1
    assert journal.receipts()[0]["price"] == "1000"


def test_account_and_intent_identity_are_not_exchangeable(journal, tmp_path):
    row = claim(journal)
    assert not journal.stock_deal(deal(row["tag"], account_id="another-account"), received_at=NOW)
    assert not journal.stock_deal(deal("OTHER1"), received_at=NOW)
    with pytest.raises(ExecutionEvidenceError, match="different intent"):
        journal.prepare(intent(shares=1000))
    with pytest.raises(ExecutionEvidenceError, match="account/schema"):
        StockSimulationJournal(journal.path, account_key="other")
    assert journal.receipts() == []


@pytest.fixture
def fake_sdk(monkeypatch):
    monkeypatch.setattr(execution, "_now", lambda: NOW)
    account = SimpleNamespace(account_type="S", broker_id="simulation-broker",
                              account_id="simulation-account", signed=True)
    contract = SimpleNamespace(code="2330", exchange="TSE", security_type="STK")
    info = SimpleNamespace(unit=1000, currency="TWD", update_date="2026-09-10",
                           trading_suspended=False, limit_up=1100, limit_down=900)

    class FakeAPI:
        def __init__(self, **kwargs):
            assert kwargs == {"simulation": True}
            self.submissions = []
            self.accounts = [account]
            self.on_place = lambda order: None
            self.contracts = SimpleNamespace(get=lambda symbol: contract, info=lambda base: info)
            self.logins = 0
            self.logouts = 0
            self.subscriptions = 0
            self.status_updates = 0
            self.health = SimpleNamespace(state="Healthy", reasons=[])
            self.authenticated = False
            self.steps = []
        def set_order_callback(self, callback):
            if not self.authenticated:
                raise RuntimeError("Not authenticated")
            self.steps.append("callback")
            self.callback = callback
        def clear_order_callback(self):
            self.callback = None
        def login(self, **kwargs):
            assert kwargs["subscribe_trade"] is False
            assert kwargs["force_refresh"] is True
            self.logins += 1
            self.authenticated = True
            self.steps.append("login")
            return self.accounts
        def logout(self):
            self.logouts += 1
            self.authenticated = False
        def list_accounts(self):
            return self.accounts
        def subscribe_trade(self, account, timeout):
            assert account is self.accounts[0]
            assert timeout == 5000
            self.subscriptions += 1
            self.steps.append("subscribe")
            return True
        def trade_cache_health(self, account):
            assert account is self.accounts[0]
            return self.health
        def update_status(self, account, timeout):
            assert account is self.accounts[0] and timeout == 5000
            self.status_updates += 1
        def place_order(self, contract, order, **kwargs):
            assert kwargs["timeout"] == 0
            self.submissions.append(order)
            self.on_place(order)
            # A Filled return by itself is deliberately insufficient.
            return SimpleNamespace(status=SimpleNamespace(status="Filled", deal_quantity=2))

    sdk = SimpleNamespace(Shioaji=FakeAPI, StockOrder=lambda **kwargs: SimpleNamespace(**kwargs),
        Action=SimpleNamespace(Buy="Buy", Sell="Sell"),
        StockPriceType=SimpleNamespace(MKT="MKT", LMT="LMT"),
        OrderType=SimpleNamespace(IOC="IOC", ROD="ROD", FOK="FOK"),
        StockOrderLot=SimpleNamespace(Common="Common"),
        StockOrderCond=SimpleNamespace(Cash="Cash", MarginTrading="MarginTrading", ShortSelling="ShortSelling"))
    monkeypatch.setitem(sys.modules, "shioaji", sdk)
    return sdk, info


def logged_session(journal):
    session = ShioajiStockSimulationSession(journal)
    journal._test_sessions.append(session)
    session.login(api_key="fixture-key", secret_key="fixture-secret")
    return session


@pytest.fixture
def shared_quote(fake_sdk, monkeypatch):
    from stockagent.live import quote_provider
    api = fake_sdk[0].Shioaji(simulation=True)
    api.authenticated = True  # A previously logged-in quote owner.
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", api)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_SIMULATION_API", api)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_ORDER_OWNER", None)
    return api


def shared_session(journal):
    session = ShioajiStockSimulationSession.from_existing_quote_owner(journal)
    journal._test_sessions.append(session)
    return session


def test_enumerate_hashed_signed_accounts_without_login_or_callback(shared_quote):
    assert execution.existing_quote_simulation_stock_accounts() == (ACCOUNT,)
    assert shared_quote.logins == shared_quote.logouts == shared_quote.subscriptions == 0
    assert not hasattr(shared_quote, "callback")


def test_shared_quote_owner_reuses_client_and_never_logs_it_out(journal, shared_quote):
    session = shared_session(journal)
    assert shared_quote.logins == 0
    assert shared_quote.subscriptions == 1
    with pytest.raises(ExecutionEvidenceError, match="never log in"):
        session.login(api_key="no", secret_key="no")
    session.logout()
    assert shared_quote.logouts == 0
    assert shared_quote.callback is None


@pytest.mark.parametrize("present,proven", [(False, False), (True, False)])
def test_shared_quote_requires_proven_existing_simulation_no_new_binding(
        journal, shared_quote, monkeypatch, tmp_path, present, proven):
    from stockagent.live import quote_provider
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", shared_quote if present else None)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_SIMULATION_API", shared_quote if proven else None)
    with pytest.raises(RuntimeError, match="not proven"):
        shared_session(journal)
    assert not (tmp_path / "owner" / "account-journals.sqlite").exists()
    assert shared_quote.logins == shared_quote.subscriptions == 0


def test_shared_callback_lease_is_exclusive(journal, shared_quote, tmp_path):
    first = shared_session(journal)
    second_journal = StockSimulationJournal(tmp_path / "other.sqlite", account_key=ACCOUNT)
    try:
        with pytest.raises(RuntimeError, match="callback already has an owner"):
            ShioajiStockSimulationSession.from_existing_quote_owner(second_journal)
        assert shared_quote.subscriptions == 1
    finally:
        first.logout()
        second_journal.close()


def test_account_writer_is_exclusive_and_binding_survives_restart(journal, fake_sdk, tmp_path):
    first = logged_session(journal)
    other = StockSimulationJournal(tmp_path / "other.sqlite", account_key=ACCOUNT)
    try:
        with pytest.raises(ExecutionEvidenceError, match="account already has a writer"):
            ShioajiStockSimulationSession(other)
        first.logout()
        with pytest.raises(ExecutionEvidenceError, match="bound to a different journal"):
            ShioajiStockSimulationSession(other)
        again = logged_session(journal)
        again.logout()
    finally:
        other.close()


def test_account_registry_rejects_non_private_directory(journal, fake_sdk, tmp_path):
    root = tmp_path / "owner"
    root.mkdir(mode=0o755)
    root.chmod(0o755)
    with pytest.raises(ExecutionEvidenceError, match="private owned directory"):
        ShioajiStockSimulationSession(journal)


def test_replaced_journal_at_same_path_cannot_reuse_account_tags(tmp_path, fake_sdk, monkeypatch):
    monkeypatch.setenv("STOCKAGENT_SHIOAJI_SIMULATION_OWNER_DIR", str(tmp_path / "owner"))
    path = tmp_path / "canonical.sqlite"
    original = StockSimulationJournal(path, account_key=ACCOUNT)
    original_id = original.journal_id
    owner = ShioajiStockSimulationSession(original)
    owner.logout()
    original.close()
    path.rename(tmp_path / "preserved-original.sqlite")
    replacement = StockSimulationJournal(path, account_key=ACCOUNT)
    try:
        assert replacement.journal_id != original_id
        with pytest.raises(ExecutionEvidenceError, match="bound to a different journal"):
            ShioajiStockSimulationSession(replacement)
    finally:
        replacement.close()


def test_shared_subscription_failure_does_not_logout_quote_owner(journal, shared_quote, monkeypatch):
    from stockagent.live import quote_provider
    monkeypatch.setattr(shared_quote, "subscribe_trade", lambda *_args, **_kwargs: False)
    with pytest.raises(ExecutionEvidenceError, match="subscription was not accepted"):
        shared_session(journal)
    assert shared_quote.logouts == 0
    assert shared_quote.callback is None
    assert quote_provider._SHIOAJI_STOCK_ORDER_OWNER is None


def test_shared_subscription_is_not_started_in_protected_opening(journal, shared_quote, monkeypatch, tmp_path):
    monkeypatch.setattr(execution, "_now", lambda: NOW.replace(minute=0))
    with pytest.raises(ExecutionEvidenceError, match="protected opening"):
        shared_session(journal)
    assert shared_quote.subscriptions == 0
    assert not (tmp_path / "owner" / "account-journals.sqlite").exists()


def test_shared_baseline_query_is_not_started_in_protected_opening(journal, shared_quote, monkeypatch):
    session = shared_session(journal)
    monkeypatch.setattr(execution, "_now", lambda: NOW.replace(minute=0))
    with pytest.raises(ExecutionEvidenceError, match="protected opening"):
        session.refresh_shared_report_baseline()
    assert shared_quote.status_updates == 0
    session.logout()


def test_shared_cache_health_must_not_have_contradictory_reasons(journal, shared_quote):
    session = shared_session(journal)
    shared_quote.health = SimpleNamespace(state="Healthy",
        reasons=[SimpleNamespace(event_type="StockOrder", reason="SequenceGap")])
    assert not session.shared_readiness()["preflight_ready"]
    with pytest.raises(ExecutionEvidenceError, match="preflight is not ready"):
        session.submit(intent())
    assert shared_quote.submissions == []
    session.logout()


def test_failed_callback_install_must_clear_or_retain_exclusive_lease(journal, shared_quote, monkeypatch):
    from stockagent.live import quote_provider
    def uncertain_install(callback):
        shared_quote.callback = callback
        raise RuntimeError("setter outcome uncertain")
    monkeypatch.setattr(shared_quote, "set_order_callback", uncertain_install)
    monkeypatch.setattr(shared_quote, "clear_order_callback", lambda: (_ for _ in ()).throw(RuntimeError("clear failed")))
    with pytest.raises(RuntimeError, match="setter outcome uncertain"):
        shared_session(journal)
    assert quote_provider._SHIOAJI_STOCK_ORDER_OWNER is not None
    with pytest.raises(RuntimeError, match="callback already has an owner"):
        shared_session(journal)
    assert shared_quote.logouts == 0


@pytest.mark.parametrize("state", ["Unknown", "Degraded"])
def test_shared_report_cache_must_be_healthy_before_order(journal, shared_quote, state):
    session = shared_session(journal)
    shared_quote.health = SimpleNamespace(state=state,
        reasons=[SimpleNamespace(event_type="StockDeal", reason="NoBaseline")])
    readiness = session.shared_readiness()
    assert readiness["report_cache_state"] == state
    assert readiness["report_cache_reasons"] == [{"event_type": "StockDeal", "reason": "NoBaseline"}]
    assert not readiness["preflight_ready"] and not readiness["actual_callback_or_fill_verified"]
    with pytest.raises(ExecutionEvidenceError, match="preflight is not ready"):
        session.submit(intent())
    assert journal.status()["orders"] == [] and shared_quote.submissions == []
    session.logout()


def test_shared_baseline_refresh_preserves_connection_and_rechecks_health(journal, shared_quote, monkeypatch):
    session = shared_session(journal)
    shared_quote.health = SimpleNamespace(state="Unknown", reasons=[])
    def refresh(account, timeout):
        assert account is shared_quote.accounts[0] and timeout == 5000
        shared_quote.health = SimpleNamespace(state="Healthy", reasons=[])
    monkeypatch.setattr(shared_quote, "update_status", refresh)
    result = session.refresh_shared_report_baseline()
    assert result["preflight_ready"]
    assert not result["actual_callback_or_fill_verified"]
    assert shared_quote.logins == shared_quote.logouts == 0
    session.logout()


def test_shared_baseline_from_replaced_generation_cannot_authorize_order(journal, shared_quote, monkeypatch):
    from stockagent.live import quote_provider
    session = shared_session(journal)
    def refresh(_account, timeout):
        monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", object())
    monkeypatch.setattr(shared_quote, "update_status", refresh)
    with pytest.raises(ExecutionEvidenceError, match="session changed"):
        session.refresh_shared_report_baseline()
    assert shared_quote.submissions == []
    session.logout()


def test_shared_reconnect_before_intent_blocks_without_order(journal, shared_quote, monkeypatch):
    from stockagent.live import quote_provider
    session = shared_session(journal)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", object())
    with pytest.raises(ExecutionEvidenceError, match="session changed"):
        session.submit(intent())
    assert journal.status()["orders"] == []
    assert shared_quote.submissions == []
    session.logout()
    assert shared_quote.logouts == 0


def test_shared_reconnect_after_claim_proves_nothing_was_sent(journal, shared_quote, fake_sdk, monkeypatch):
    from stockagent.live import quote_provider
    session = shared_session(journal)
    def change_generation(**kwargs):
        monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", object())
        return SimpleNamespace(**kwargs)
    monkeypatch.setattr(fake_sdk[0], "StockOrder", change_generation)
    with pytest.raises(ExecutionEvidenceError, match="nothing sent"):
        session.submit(intent())
    assert journal.status()["orders"][0]["state"] == "rejected"
    assert shared_quote.submissions == []
    session.logout()


def test_shared_report_health_rechecked_after_claim_before_send(journal, shared_quote, fake_sdk, monkeypatch):
    session = shared_session(journal)
    def degrade(**kwargs):
        shared_quote.health = SimpleNamespace(state="Degraded", reasons=[])
        return SimpleNamespace(**kwargs)
    monkeypatch.setattr(fake_sdk[0], "StockOrder", degrade)
    with pytest.raises(ExecutionEvidenceError, match="nothing sent"):
        session.submit(intent())
    assert journal.status()["orders"][0]["state"] == "rejected"
    assert shared_quote.submissions == []
    session.logout()


def test_shared_callback_cannot_detach_while_order_unresolved(journal, shared_quote):
    session = shared_session(journal)
    session.submit(intent())
    with pytest.raises(ExecutionEvidenceError, match="reconciliation before detach"):
        session.logout()
    shared_quote.callback("SDEAL", deal(shared_quote.submissions[0].custom_field, quantity=2))
    assert session.drain() == 1
    session.logout()
    assert shared_quote.logouts == 0


def test_shared_unknown_submit_keeps_callback_and_never_resends(journal, shared_quote):
    session = shared_session(journal)
    def timeout(_order):
        raise TimeoutError("unknown transport outcome")
    shared_quote.on_place = timeout
    with pytest.raises(ExecutionEvidenceError, match="outcome unknown"):
        session.submit(intent())
    assert journal.status()["orders"][0]["state"] == "unknown"
    with pytest.raises(ExecutionEvidenceError, match="reconciliation before detach"):
        session.logout()
    with pytest.raises(ExecutionEvidenceError, match="preflight is not ready"):
        session.submit(intent(key="different-key"))
    assert len(shared_quote.submissions) == 1
    shared_quote.callback("SDEAL", deal(shared_quote.submissions[0].custom_field, quantity=2))
    assert session.drain() == 1
    assert journal.status()["orders"][0]["state"] == "filled"
    session.logout()


def test_shared_reconnect_fences_old_owner_until_explicit_detach(journal, shared_quote, monkeypatch):
    from stockagent.live import quote_provider
    session = shared_session(journal)
    replacement = object()
    monkeypatch.setattr(quote_provider, "_shioaji_stock_api", lambda: replacement)
    assert quote_provider._reconnect_shioaji_stock_quote_client(shared_quote) is replacement
    assert shared_quote.logouts == 1  # The quote owner, not the shared session, reconnected.
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", replacement)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_SIMULATION_API", replacement)
    with pytest.raises(RuntimeError, match="callback already has an owner"):
        quote_provider._claim_existing_shioaji_simulation_order_owner()
    with pytest.raises(ExecutionEvidenceError, match="session changed"):
        session.submit(intent())
    session.logout()
    api, token = quote_provider._claim_existing_shioaji_simulation_order_owner()
    quote_provider._release_existing_shioaji_simulation_order_owner(api, token, clear_callback=False)


def test_shared_send_fences_quote_generation_change(journal, shared_quote, monkeypatch):
    from stockagent.live import quote_provider
    session = shared_session(journal)
    entered, allow_return, changed = threading.Event(), threading.Event(), threading.Event()
    def place(order):
        entered.set()
        assert allow_return.wait(2)
        shared_quote.callback("SDEAL", deal(order.custom_field, quantity=2))
    shared_quote.on_place = place
    errors = []
    def submit():
        try:
            session.submit(intent())
        except Exception as exc:
            errors.append(exc)
    def replace_quote():
        with quote_provider._SHIOAJI_STOCK_LIFECYCLE_LOCK:
            with quote_provider._SHIOAJI_STOCK_LOCK:
                monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_API", object())
                changed.set()
    worker = threading.Thread(target=submit)
    worker.start()
    assert entered.wait(2)
    replacer = threading.Thread(target=replace_quote)
    replacer.start()
    assert not changed.wait(0.05)
    assert quote_provider._SHIOAJI_STOCK_LOCK.acquire(timeout=0.2)
    quote_provider._SHIOAJI_STOCK_LOCK.release()
    allow_return.set()
    worker.join(2)
    replacer.join(2)
    assert changed.is_set() and not worker.is_alive() and not errors
    assert journal.status()["orders"][0]["state"] == "filled"
    session.logout()


def test_sdk_session_literal_simulation_and_no_inferred_fill(journal, fake_sdk):
    session = logged_session(journal)
    assert session._api.steps == ["login", "callback", "subscribe"]
    session.submit(intent())
    assert session._api.submissions[0].quantity == 2
    assert session._api.submissions[0].order_lot == "Common"
    assert journal.receipts() == []
    with pytest.raises(ExecutionEvidenceError):
        session.submit(intent())
    assert len(session._api.submissions) == 1


def test_native_sdk_session_constructor_never_registers_before_login(journal):
    pytest.importorskip("shioaji")
    session = ShioajiStockSimulationSession(journal)
    journal._test_sessions.append(session)
    assert session._account is None
    session.logout()  # No SDK logout is attempted on an unlogged client.


def test_session_early_deal_and_network_timeout_never_double_send(journal, fake_sdk):
    session = logged_session(journal)
    def fill_then_disconnect(order):
        session._api.callback("SDEAL", deal(order.custom_field, quantity=2))
        raise TimeoutError("private transport detail")
    session._api.on_place = fill_then_disconnect
    with pytest.raises(ExecutionEvidenceError, match="outcome unknown"):
        session.submit(intent())
    assert journal.status()["orders"][0]["state"] == "filled"
    assert journal.receipts()[0]["shares"] == 2000
    with pytest.raises(ExecutionEvidenceError):
        session.submit(intent())
    assert len(session._api.submissions) == 1


@pytest.mark.parametrize("field,value", [("unit", 100), ("currency", "USD"),
    ("update_date", "2026-09-09"), ("trading_suspended", True)])
def test_contract_rules_verified_before_network_write(journal, fake_sdk, field, value):
    setattr(fake_sdk[1], field, value)
    session = logged_session(journal)
    with pytest.raises(ExecutionEvidenceError, match="contract rules"):
        session.submit(intent())
    assert session._api.submissions == []
    assert journal.status()["orders"] == []


def test_callback_corruption_is_visible_and_blocks_network_write(journal, fake_sdk):
    session = logged_session(journal)
    session._api.callback("SDEAL", {"price": float("nan")})
    for _ in range(3):
        with pytest.raises(ExecutionEvidenceError, match="callback failure"):
            session.submit(intent())
    assert len(journal.status()["problems"]) == 1
    assert session._api.submissions == []


def test_replay_clock_is_not_accepted_on_network_writer(journal, fake_sdk):
    session = logged_session(journal)
    with pytest.raises(ExecutionEvidenceError, match="current trading session"):
        session.submit(intent(signal_at=NOW - timedelta(days=1)))
    with pytest.raises(TypeError):
        session.submit(intent(), now=NOW)
    assert session._api.submissions == []


def test_single_session_owner_per_account_journal(journal, fake_sdk):
    session = logged_session(journal)
    with pytest.raises(ExecutionEvidenceError, match="already has a session owner"):
        ShioajiStockSimulationSession(journal)
    session.logout()
    with pytest.raises(ExecutionEvidenceError, match="closed"):
        session.login(api_key="fixture", secret_key="fixture")
    logged_session(journal)


def test_recorded_callback_is_replayed_idempotently_after_consumer_crash(journal, fake_sdk):
    row = claim(journal)
    journal.record_event("SDEAL", deal(row["tag"]), NOW)
    # Crash after committing a fill but before marking its event processed.
    assert journal.stock_deal(deal(row["tag"]), received_at=NOW)
    session = logged_session(journal)
    assert session.drain() == 0
    assert len(journal.receipts()) == 1
    assert journal.status()["pending_event_count"] == 0


def test_pending_order_prevents_duplicate_target_but_not_another_symbol(journal):
    row = claim(journal)
    journal.order_ack(tag=row["tag"], trade_id="trade-1", account_key=ACCOUNT,
                      symbol="2330", action="Buy")
    duplicate = intent(key="new-signal-same-symbol")
    journal.prepare(duplicate)
    with pytest.raises(ExecutionEvidenceError, match="earlier order"):
        journal.claim(duplicate.key)
    other = intent(key="new-signal-another-symbol", symbol="2317")
    journal.prepare(other)
    journal.claim(other.key)


def test_partial_fill_and_cancel_only_account_for_proven_shares(journal):
    row = claim(journal)
    journal.stock_deal(deal(row["tag"]), received_at=NOW)
    journal.order_cancelled(tag=row["tag"], cancelled_lots=1)
    status = journal.status()["orders"][0]
    assert status["state"] == "cancelled"
    assert status["filled_shares"] == status["cancelled_shares"] == 1000
    assert len(journal.receipts()) == 1
    with pytest.raises(ExecutionEvidenceError, match="exceeds"):
        journal.stock_deal(deal(row["tag"], exchange_seq="2"), received_at=NOW)


def test_cancel_callback_can_precede_last_deal(journal):
    row = claim(journal)
    journal.order_cancelled(tag=row["tag"], cancelled_lots=1)
    assert journal.stock_deal(deal(row["tag"]), received_at=NOW)
    assert journal.status()["orders"][0]["state"] == "cancelled"
    assert len(journal.receipts()) == 1


def test_malformed_raw_callback_persists_diagnostic_without_creating_fill(journal, fake_sdk):
    session = logged_session(journal)
    row = claim(journal)
    session._api.callback("SDEAL", deal(row["tag"], price=1001))
    assert session.drain() == 0
    assert journal.status()["problems"][0]["blocks"] == 1
    assert journal.db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    assert journal.receipts() == []


def test_legacy_inventory_never_becomes_broker_evidence():
    state = {"simulation_only": True, "modes": {"a": {"positions": {
        "whole": {"signed_shares": 2000}, "odd": {"signed_shares": -1500}}}}}
    report = legacy_inventory_cutover_gate(state, ["a"])
    assert report["status"] == "blocked_legacy_inventory"
    assert not report["broker_execution_ready"]
    assert report["markets"][0]["open_positions"] == 2
    assert report["markets"][0]["odd_lot_positions"] == 1
    assert state["modes"]["a"]["positions"]["odd"]["signed_shares"] == -1500
    assert not report["existing_paper_fills_can_be_relabelled"]


def test_flat_paper_book_is_not_sufficient_broker_acceptance():
    state = {"simulation_only": True, "modes": {"a": {"positions": {}}}}
    report = legacy_inventory_cutover_gate(state, ["a"])
    assert report["legacy_inventory_clear"]
    assert not report["broker_execution_ready"]
    for patch in ({"modes": {}}, {"simulation_only": False},
                  {"modes": {"a": {"positions": {"p": {"signed_shares": .5}}}}}):
        assert not legacy_inventory_cutover_gate(state | patch, ["a"])["legacy_inventory_clear"]


def test_pending_local_order_must_not_cross_cutover():
    state = {"simulation_only": True, "modes": {"a": {"positions": {},
        "pending_entry_orders": {"2330": {"status": "working"}}}}}
    report = legacy_inventory_cutover_gate(state, ["a"])
    assert not report["legacy_inventory_clear"]
    assert report["markets"][0]["pending_local_entry_orders"] == 1


def test_nonblocking_submission_does_not_wait_for_unrelated_model_ack(journal, fake_sdk):
    session = logged_session(journal)
    session.submit(intent())
    session.submit(intent(key="another-mode:2330", market="another-mode"))
    assert len(session._api.submissions) == 2
    assert journal.receipts() == []


def test_missing_async_ack_expires_to_durable_unknown(journal, fake_sdk, monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(execution.monotonic_time, "monotonic", lambda: clock[0])
    session = logged_session(journal)
    session.submit(intent())
    clock[0] += 5.1
    session.drain()
    assert journal.status()["orders"][0]["state"] == "unknown"
    with pytest.raises(ExecutionEvidenceError, match="unresolved"):
        session.submit(intent(key="different-key", market="different-mode"))
    assert len(session._api.submissions) == 1


def test_late_new_ack_does_not_resurrect_cancelled_order(journal):
    row = claim(journal)
    journal.order_cancelled(tag=row["tag"], cancelled_lots=2)
    journal.order_ack(tag=row["tag"], trade_id="trade-1", account_key=ACCOUNT,
                      symbol="2330", action="Buy")
    assert journal.status()["orders"][0]["state"] == "cancelled"


def test_atomic_prepare_claim_is_one_commit_and_rolls_back_on_block(journal):
    trace = []
    journal.db.set_trace_callback(trace.append)
    journal.prepare_and_claim(intent())
    assert sum(line == "COMMIT" for line in trace) == 1
    with pytest.raises(ExecutionEvidenceError, match="unresolved"):
        journal.prepare_and_claim(intent(key="blocked-new-intent"))
    assert len(journal.status()["orders"]) == 1
    assert journal.status()["orders"][0]["state"] == "submitting"


def test_sdk_construction_failure_is_proven_not_sent(journal, fake_sdk):
    def invalid_order(**kwargs):
        raise ValueError("fixture schema mismatch")
    fake_sdk[0].StockOrder = invalid_order
    session = logged_session(journal)
    with pytest.raises(ExecutionEvidenceError, match="before any submission"):
        session.submit(intent())
    assert session._api.submissions == []
    assert journal.status()["orders"][0]["state"] == "rejected"
    assert journal.receipts() == []


def test_broker_rejection_is_not_local_fill_or_whole_account_failure(journal, fake_sdk):
    session = logged_session(journal)
    def reject(order):
        session._api.callback("SORDER", {
            "order": {"custom_field": order.custom_field, "action": "Buy",
                      "account": {"broker_id": "simulation-broker", "account_id": "simulation-account"}},
            "contract": {"code": "2330"},
            "operation": {"op_type": "New", "op_code": "88", "op_msg": "fixture rejection"}})
    session._api.on_place = reject
    session.submit(intent())
    assert journal.status()["orders"][0]["state"] == "rejected"
    assert journal.receipts() == []
    session.submit(intent(key="different-mode", market="different-mode"))
    assert len(session._api.submissions) == 2
    assert journal.receipts() == []


def test_closing_auction_cannot_receive_market_order(journal, fake_sdk, monkeypatch):
    monkeypatch.setattr(execution, "_now", lambda: NOW.replace(hour=13, minute=25))
    session = logged_session(journal)
    with pytest.raises(ExecutionEvidenceError, match="auction"):
        session.submit(intent())
    assert session._api.submissions == []
    session.submit(intent(price_type="LMT", order_type="ROD", price="1000"))
    assert len(session._api.submissions) == 1
    assert journal.receipts() == []
