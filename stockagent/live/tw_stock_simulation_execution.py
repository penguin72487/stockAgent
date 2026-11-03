"""Durable stock-simulation order/fill boundary, separate from quote matching.

This is a staged adapter, not an implicit migration of existing paper books.
Only StockDeal callbacks produce broker fill receipts. A submission timeout,
acknowledgement, position snapshot or local price never constitutes a deal.
The single owner drains callbacks outside Shioaji's callback thread. Uncertain
submissions survive restart and cannot be sent again, even under a new key.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
import fcntl
import hashlib
import json
import os
from pathlib import Path
import queue
import sqlite3
import stat
import threading
import time as monotonic_time
from typing import Any, Iterator, Mapping
import uuid
from zoneinfo import ZoneInfo

from stockagent.backtest.tw_day_trade_contract import BOARD_LOT_SHARES
from stockagent.data.tw_price_rules import price_on_tick_grid_numpy


TAIPEI = ZoneInfo("Asia/Taipei")
BROKER_FILL = "shioaji_simulation_stock_deal_v1"
LOCAL_ODD_FILL = "local_odd_lot_assumption"


class ExecutionEvidenceError(RuntimeError):
    """An unresolved evidence boundary; never authorizes a synthetic fill."""


def _enum(value: Any) -> str:
    return str(getattr(value, "value", value))


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _positive_integer(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("quantity must be a positive integer")
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number != number.to_integral_value() or number <= 0:
            raise ValueError("quantity must be a positive integer")
        return int(number)
    except InvalidOperation as exc:
        raise ValueError("quantity must be a positive integer") from exc


def _dated_timestamp(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("execution timestamps must be timezone-aware")
    return value.astimezone(TAIPEI)


def _now() -> datetime:
    return datetime.now(TAIPEI)


def _outside_protected_opening() -> None:
    clock = _now().astimezone(TAIPEI).time().replace(tzinfo=None)
    # Leave room for registry I/O and the five-second SDK timeout before the
    # engine's 08:59:30 protected window. It is not a provider latency SLA.
    if time(8, 59) <= clock < time(9, 1):
        raise ExecutionEvidenceError("subscription and baseline queries are disabled during protected opening")


def _source_price(value: Any, session: date, security_type: str) -> str:
    # Do not round invalid source evidence onto the price grid. Decimal keeps
    # money exact in the durable receipt; VWAPs are not passed to this function.
    try:
        price = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid source price") from exc
    if not price.is_finite() or price <= 0 or not bool(price_on_tick_grid_numpy(
        float(price), session.isoformat(), security_types=security_type
    )):
        raise ValueError("source price is not on the dated product tick grid")
    return format(price.normalize(), "f")


def account_fingerprint(broker_id: str, account_id: str) -> str:
    if not broker_id or not account_id:
        raise ValueError("broker and account identity are required")
    return hashlib.sha256(_json([str(broker_id), str(account_id)]).encode()).hexdigest()


def existing_quote_simulation_stock_accounts() -> tuple[str, ...]:
    """List only hashed signed stock identities on the existing simulation owner.

    No caller-provided SDK client, login, order callback or source request is
    accepted. A subsequent attach independently fences the current generation.
    """
    from stockagent.live import quote_provider

    api, token = quote_provider._claim_existing_shioaji_simulation_order_owner()
    try:
        with quote_provider._guard_existing_shioaji_simulation_order_owner(api, token):
            accounts = api.list_accounts()
        identities = tuple(account_fingerprint(str(getattr(account, "broker_id", "")),
                                               str(getattr(account, "account_id", "")))
                           for account in accounts
                           if _enum(getattr(account, "account_type", "")) == "S"
                           and getattr(account, "signed", None) is True)
        if len(set(identities)) != len(identities):
            raise ExecutionEvidenceError("duplicate simulation stock account identity")
        return identities
    finally:
        quote_provider._release_existing_shioaji_simulation_order_owner(api, token, clear_callback=False)


def _acquire_account_writer(journal: StockSimulationJournal):
    """Cooperating writers share one durable account-to-journal binding.

    A file lock alone cannot stop a later restart from picking another journal
    and reusing its order tags. The small durable index preserves that identity;
    it does not prove that unrelated external clients obey this boundary.
    """
    key = journal.account_key
    if len(key) != 64 or any(ch not in "0123456789abcdef" for ch in key):
        raise ExecutionEvidenceError("simulation account fingerprint is invalid")
    # Keep the 0700 owner registry below the journal tree: opening a journal
    # first may create its parent using the process's ordinary umask.
    default = Path(__file__).resolve().parents[2] / "artifacts/live/shioaji_stock_simulation/owner_registry"
    root = Path(os.environ.get("STOCKAGENT_SHIOAJI_SIMULATION_OWNER_DIR", str(default))).absolute()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = root.lstat()
    if (root.resolve() != root or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid() or info.st_mode & 0o077):
        raise ExecutionEvidenceError("simulation account registry must be a private owned directory")
    lock_path = root / (key + ".writer.lock")
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    lock = os.fdopen(fd, "a+")
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        lock.close()
        raise ExecutionEvidenceError("simulation account already has a writer") from exc
    try:
        index_path = root / "account-journals.sqlite"
        if index_path.is_symlink() or (index_path.exists() and not index_path.is_file()):
            raise ExecutionEvidenceError("simulation account registry is not a regular file")
        db = sqlite3.connect(index_path, timeout=10, isolation_level=None)
        try:
            index_path.chmod(0o600)
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS bindings "
                       "(account_key TEXT PRIMARY KEY, journal_path TEXT NOT NULL, journal_id TEXT)")
            if "journal_id" not in {row[1] for row in db.execute("PRAGMA table_info(bindings)")}:
                # An older index cannot prove that a path still contains the
                # original journal. Existing rows stay NULL and fail closed.
                db.execute("ALTER TABLE bindings ADD COLUMN journal_id TEXT")
            path = str(journal.path.resolve())
            db.execute("INSERT OR IGNORE INTO bindings(account_key,journal_path,journal_id) VALUES (?,?,?)",
                       (key, path, journal.journal_id))
            bound = db.execute("SELECT journal_path,journal_id FROM bindings WHERE account_key=?", (key,)).fetchone()
            if bound is None or bound[0] != path or bound[1] != journal.journal_id:
                raise ExecutionEvidenceError("simulation account is bound to a different journal")
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise
        finally:
            db.close()
    except BaseException:
        lock.close()
        raise
    return lock


def legacy_inventory_cutover_gate(state: Mapping[str, Any], markets: list[str]) -> dict[str, Any]:
    """Read-only gate: paper inventory must not be relabelled as broker stock.

    Flat legacy inventory is only one prerequisite, not broker readiness. In
    particular, this function cannot establish account balance or live fills.
    """
    modes = state.get("modes") or {}
    rows, errors = [], []
    if not markets or state.get("simulation_only") is not True:
        errors.append("missing_selected_modes_or_simulation_ledger_proof")
    for market in markets:
        mode = modes.get(market)
        if not isinstance(mode, dict) or not isinstance(mode.get("positions"), dict):
            errors.append(f"missing_inventory:{market}")
            continue
        positions, odd_positions = 0, 0
        for position in mode["positions"].values():
            try:
                signed = position["signed_shares"]
                if isinstance(signed, bool) or not isinstance(signed, int):
                    raise ValueError("noninteger inventory")
                if signed:
                    positions += 1
                    odd_positions += int(bool(abs(signed) % BOARD_LOT_SHARES))
            except (KeyError, TypeError, ValueError):
                errors.append(f"invalid_inventory:{market}")
        pending = sum(order.get("status") == "working"
                      for order in (mode.get("pending_entry_orders") or {}).values())
        rows.append({"market": market, "session_date": mode.get("session_date"),
                     "open_positions": positions, "odd_lot_positions": odd_positions,
                     "pending_local_entry_orders": pending})
    clear = not errors and not any(r["open_positions"] or r["pending_local_entry_orders"] for r in rows)
    return {"status": "legacy_inventory_clear_only" if clear else "blocked_legacy_inventory",
            "legacy_inventory_clear": clear, "broker_execution_ready": False,
            "existing_paper_fills_can_be_relabelled": False,
            "scope": "inventory_handoff_prerequisite_not_live_account_acceptance",
            "state_updated_at": state.get("updated_at"), "markets": rows, "errors": errors}


@dataclass(frozen=True)
class StockSimulationIntent:
    key: str
    market: str
    symbol: str
    signal_at: datetime
    shares: int
    action: str
    security_type: str = "stock"
    price_type: str = "MKT"
    price: str = "0"
    order_type: str = "IOC"
    order_cond: str = "Cash"
    daytrade_short: bool = False

    def payload(self) -> dict[str, Any]:
        signal_at = _dated_timestamp(self.signal_at)
        shares = _positive_integer(self.shares)
        if shares % BOARD_LOT_SHARES:
            raise ValueError("broker simulation accepts board lots only; no odd-lot fallback")
        if not self.key or not self.market or not self.symbol.isalnum():
            raise ValueError("intent identity is required")
        if self.action not in {"Buy", "Sell"} or self.security_type not in {"stock", "etf"}:
            raise ValueError("unsupported stock action or security type")
        if self.order_type not in {"ROD", "IOC", "FOK"}:
            raise ValueError("unsupported order type")
        if self.order_cond not in {"Cash", "MarginTrading", "ShortSelling"}:
            raise ValueError("unsupported order condition")
        if not isinstance(self.daytrade_short, bool) or (
            self.daytrade_short and (self.action != "Sell" or self.order_cond != "Cash")
        ):
            raise ValueError("daytrade_short requires a cash sell")
        if self.price_type == "MKT":
            if Decimal(str(self.price)) != 0 or self.order_type == "ROD":
                raise ValueError("market orders require price zero and IOC/FOK")
            price = "0"
        elif self.price_type == "LMT":
            price = _source_price(self.price, signal_at.date(), self.security_type)
        else:
            raise ValueError("unsupported stock price type")
        return asdict(self) | {"signal_at": signal_at.isoformat(), "shares": shares,
                               "price": price, "session_date": signal_at.date().isoformat()}


def odd_lot_residual_fill(*, position_id: str, symbol: str, signed_inventory: int,
                         shares: int, action: str, decision_at: datetime,
                         observed_at: datetime, quote_at: datetime,
                         bid: Any, ask: Any, security_type: str = "stock",
                         source: str, regular_trade: bool,
                         max_quote_age_seconds: float = 3.0) -> dict[str, Any]:
    """Plan ONLY a reduction of an observed odd-lot inventory remainder.

    The caller must atomically commit this receipt and reduce that inventory;
    this pure function is not a durable fill consumer. No whole-lot rejection
    or pending order can be supplied as inventory. Historical replay uses its
    separate source-minute/50% participation contract, not this live exception.
    """
    quantity = _positive_integer(shares)
    inventory = _positive_integer(abs(signed_inventory))
    if isinstance(signed_inventory, bool) or int(signed_inventory) != signed_inventory:
        raise ValueError("inventory must be integer shares")
    if quantity > inventory % BOARD_LOT_SHARES:
        raise ExecutionEvidenceError("local fallback is restricted to the odd-lot remainder")
    if action != ("Sell" if signed_inventory > 0 else "Buy"):
        raise ExecutionEvidenceError("local odd-lot execution must reduce existing inventory")
    decision, observed, quoted = map(_dated_timestamp, (decision_at, observed_at, quote_at))
    if not (0 < max_quote_age_seconds <= 60):
        raise ValueError("quote age budget must be positive and bounded")
    if (decision.date() != observed.date() or quoted.date() != observed.date()
            or not decision < quoted <= observed
            or (observed - quoted).total_seconds() > max_quote_age_seconds
            or not time(9) <= observed.time() < time(13, 25)):
        raise ExecutionEvidenceError("a fresh causal continuous-session quote is required")
    if not position_id or not symbol or not source or regular_trade is not True:
        raise ExecutionEvidenceError("real regular-board quote and inventory identity required")
    bid_price = _source_price(bid, quoted.date(), security_type)
    ask_price = _source_price(ask, quoted.date(), security_type)
    if Decimal(bid_price) > Decimal(ask_price):
        raise ExecutionEvidenceError("crossed order book")
    price = bid_price if action == "Sell" else ask_price
    return {"fill_contract": LOCAL_ODD_FILL, "broker_confirmed": False,
            "simulation_only": True, "position_id": position_id, "symbol": symbol,
            "shares": quantity, "action": action, "price": price,
            "quote_source": source, "quote_at": quoted.isoformat(),
            "decision_at": decision.isoformat(), "fill_at": observed.isoformat(),
            "inventory_before": signed_inventory,
            "inventory_after": signed_inventory + (quantity if action == "Buy" else -quantity)}


class StockSimulationJournal:
    """SQLite transactional outbox + deduplicated broker receipt inbox.

    A short transaction claims an intent before any network write. Receipt
    sequence numbers support durable consumer cursors; consumers must apply
    receipt and cursor together (or idempotently by receipt_id).
    """

    def __init__(self, path: Path, *, account_key: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.account_key = account_key
        self._lock = threading.RLock()
        self._savepoint_sequence = 0
        self.db = sqlite3.connect(self.path, timeout=10, check_same_thread=False,
                                  isolation_level=None)
        self.path.chmod(0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS intents (
            key TEXT PRIMARY KEY, tag TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
            state TEXT NOT NULL, trade_id TEXT, filled_shares INTEGER NOT NULL DEFAULT 0,
            cancelled_shares INTEGER NOT NULL DEFAULT 0);
          CREATE UNIQUE INDEX IF NOT EXISTS trade_identity ON intents(trade_id)
            WHERE trade_id IS NOT NULL;
          CREATE INDEX IF NOT EXISTS intents_state ON intents(state);
          CREATE TABLE IF NOT EXISTS fills (
            sequence INTEGER PRIMARY KEY, receipt_id TEXT NOT NULL UNIQUE,
            intent_key TEXT NOT NULL, payload TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS problems (
            id INTEGER PRIMARY KEY, code TEXT NOT NULL, detail TEXT NOT NULL,
            blocks INTEGER NOT NULL, UNIQUE(code, detail));
          CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY, state TEXT NOT NULL, payload TEXT NOT NULL,
            received_at TEXT NOT NULL, processed INTEGER NOT NULL DEFAULT 0);
          CREATE INDEX IF NOT EXISTS events_unprocessed ON events(processed);
        """)
        with self._transaction():
            self.db.execute("INSERT OR IGNORE INTO meta VALUES ('schema', '1')")
            self.db.execute("INSERT OR IGNORE INTO meta VALUES ('account', ?)", (account_key,))
            self.db.execute("INSERT OR IGNORE INTO meta VALUES ('journal_id', ?)", (uuid.uuid4().hex,))
            metadata = dict(self.db.execute("SELECT key,value FROM meta"))
            if metadata["account"] != account_key or metadata["schema"] != "1":
                raise ExecutionEvidenceError("journal account/schema mismatch")
            self.journal_id = metadata["journal_id"]
            if (len(self.journal_id) != 32
                    or any(ch not in "0123456789abcdef" for ch in self.journal_id)):
                raise ExecutionEvidenceError("journal identity is invalid")

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        with self._lock:
            nested = self.db.in_transaction
            self._savepoint_sequence += 1
            savepoint = f"execution_{self._savepoint_sequence}"
            self.db.execute(f"SAVEPOINT {savepoint}" if nested else "BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                if nested:
                    self.db.execute(f"ROLLBACK TO {savepoint}")
                    self.db.execute(f"RELEASE {savepoint}")
                else:
                    self.db.execute("ROLLBACK")
                raise
            else:
                self.db.execute(f"RELEASE {savepoint}" if nested else "COMMIT")

    def close(self) -> None:
        with self._lock:
            self.db.close()

    def prepare(self, intent: StockSimulationIntent) -> dict[str, Any]:
        payload = _json(intent.payload())
        with self._transaction():
            existing = self.db.execute("SELECT * FROM intents WHERE key=?", (intent.key,)).fetchone()
            if existing is not None:
                if existing["payload"] != payload:
                    raise ExecutionEvidenceError("idempotency key reused with different intent")
                return dict(existing)
            # Permanent local monotonic identity; never recycle a six-character
            # custom_field after restart. Exhaustion blocks instead of wrapping.
            count = self.db.execute("SELECT COUNT(*) FROM intents").fetchone()[0] + 1
            if count >= 36**5:
                raise ExecutionEvidenceError("simulation custom_field namespace exhausted")
            number, digits = count, ""
            while number:
                number, remainder = divmod(number, 36)
                digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"[remainder] + digits
            tag = "S" + digits.zfill(5)
            self.db.execute("INSERT INTO intents(key,tag,payload,state) VALUES (?,?,?,'prepared')",
                            (intent.key, tag, payload))
            return dict(self.db.execute("SELECT * FROM intents WHERE key=?", (intent.key,)).fetchone())

    def claim(self, key: str, *, in_flight: frozenset[str] = frozenset()) -> dict[str, Any]:
        with self._transaction():
            row = self.db.execute("SELECT * FROM intents WHERE key=?", (key,)).fetchone()
            if row is None or row["state"] != "prepared":
                raise ExecutionEvidenceError("intent was already claimed; reconcile, never resend")
            pending = self.db.execute(
                "SELECT key,state FROM intents WHERE state IN ('submitting','unknown')"
            ).fetchall()
            if self.db.execute("SELECT 1 FROM problems WHERE blocks=1 LIMIT 1").fetchone() or any(
                p["state"] == "unknown" or p["key"] not in in_flight for p in pending
            ):
                raise ExecutionEvidenceError("unresolved broker evidence blocks new submissions")
            target = json.loads(row["payload"])
            for other in self.db.execute("SELECT payload FROM intents WHERE key<>? AND state "
                                         "NOT IN ('prepared','filled','cancelled','rejected')", (key,)):
                existing = json.loads(other[0])
                if (existing["market"], existing["symbol"]) == (target["market"], target["symbol"]):
                    raise ExecutionEvidenceError("an earlier order still owns this model-symbol target")
            self.db.execute("UPDATE intents SET state='submitting' WHERE key=?", (key,))
            return dict(row)

    def prepare_and_claim(self, intent: StockSimulationIntent, *,
                          in_flight: frozenset[str] = frozenset()) -> dict[str, Any]:
        """One FULL-sync commit before sending, not two serial fsyncs."""
        with self._transaction():
            self.prepare(intent)
            return self.claim(intent.key, in_flight=in_flight)

    def reject_before_send(self, key: str) -> None:
        """Only for an SDK order-construction error BEFORE place_order is called."""
        with self._transaction():
            self.db.execute("UPDATE intents SET state='rejected' WHERE key=? AND state='submitting' "
                            "AND trade_id IS NULL AND filled_shares=0", (key,))

    def order_rejected(self, *, tag: str, symbol: str, action: str) -> None:
        with self._transaction():
            row = self.db.execute("SELECT * FROM intents WHERE tag=?", (tag,)).fetchone()
            if row is None:
                return
            payload = json.loads(row["payload"])
            if (payload["symbol"] != symbol or payload["action"] != action
                    or row["state"] == "prepared" or row["filled_shares"]):
                raise ExecutionEvidenceError("new-order rejection conflicts with order/fill evidence")
            self.db.execute("UPDATE intents SET state='rejected' WHERE tag=?", (tag,))

    def unknown(self, key: str) -> None:
        with self._transaction():
            # An already received deal/ack outranks the caller's network timeout.
            self.db.execute("UPDATE intents SET state='unknown' WHERE key=? AND state='submitting'", (key,))

    def problem(self, code: str, detail: str, *, blocks: bool = True) -> None:
        with self._transaction():
            self.db.execute("INSERT OR IGNORE INTO problems(code,detail,blocks) VALUES (?,?,?)",
                            (code, detail, int(blocks)))

    def owns(self, tag: str, trade_id: str = "") -> bool:
        with self._lock:
            return self.db.execute("SELECT 1 FROM intents WHERE tag=? OR trade_id=?",
                                   (tag, trade_id)).fetchone() is not None

    def require_restart_reconciliation(self) -> None:
        with self._transaction():
            self.db.execute("UPDATE intents SET state='unknown' WHERE state "
                            "IN ('submitting','submitted','partially_filled')")

    def record_event(self, state: str, payload: Mapping[str, Any], received_at: datetime) -> None:
        with self._transaction():
            self.db.execute("INSERT INTO events(state,payload,received_at) VALUES (?,?,?)",
                            (state, _json(payload), _dated_timestamp(received_at).isoformat()))

    def pending_events(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(row) for row in self.db.execute("SELECT * FROM events WHERE processed=0 ORDER BY id")]

    def finish_event(self, event_id: int) -> None:
        with self._transaction():
            self.db.execute("UPDATE events SET processed=1 WHERE id=?", (event_id,))

    def order_cancelled(self, *, tag: str, cancelled_lots: Any) -> None:
        if cancelled_lots == 0 and not isinstance(cancelled_lots, bool):
            quantity = 0
        else:
            quantity = _positive_integer(cancelled_lots) * BOARD_LOT_SHARES
        with self._transaction():
            row = self.db.execute("SELECT * FROM intents WHERE tag=?", (tag,)).fetchone()
            if row is None:
                return
            total = json.loads(row["payload"])["shares"]
            if quantity + row["filled_shares"] > total or quantity < row["cancelled_shares"]:
                raise ExecutionEvidenceError("cancel quantity conflicts with durable fills")
            self.db.execute("UPDATE intents SET cancelled_shares=?,state=CASE "
                            "WHEN filled_shares+?=? AND filled_shares<? THEN 'cancelled' "
                            "ELSE state END WHERE tag=?", (quantity, quantity, total, total, tag))

    def order_ack(self, *, tag: str, trade_id: str, account_key: str,
                  symbol: str, action: str) -> bool:
        """Bind an acknowledged identity, never a quantity or price-based fill."""
        with self._transaction():
            row = self.db.execute("SELECT * FROM intents WHERE tag=?", (tag,)).fetchone()
            if row is None or account_key != self.account_key:
                return False  # Another strategy/account's callback is not ours.
            payload = json.loads(row["payload"])
            if (row["state"] == "prepared" or not trade_id
                    or payload["symbol"] != symbol or payload["action"] != action
                    or row["trade_id"] not in (None, trade_id)):
                raise ExecutionEvidenceError("order acknowledgement identity mismatch")
            self.db.execute("UPDATE intents SET trade_id=?,state=CASE WHEN filled_shares>0 "
                            "OR state IN ('filled','cancelled','rejected') THEN state "
                            "ELSE 'submitted' END WHERE key=?", (trade_id, row["key"]))
            return True

    def stock_deal(self, message: Mapping[str, Any], *, received_at: datetime) -> bool:
        received = _dated_timestamp(received_at)
        account_key = account_fingerprint(str(message.get("broker_id") or ""),
                                          str(message.get("account_id") or ""))
        if account_key != self.account_key:
            return False
        with self._transaction():
            tag, trade_id = str(message.get("custom_field") or ""), str(message.get("trade_id") or "")
            row = self.db.execute("SELECT * FROM intents WHERE tag=?", (tag,)).fetchone()
            if row is None and trade_id:
                row = self.db.execute("SELECT * FROM intents WHERE trade_id=?", (trade_id,)).fetchone()
            if row is None:
                return False
            payload = json.loads(row["payload"])
            if (row["state"] == "prepared" or not trade_id
                    or (tag and tag != row["tag"]) or row["trade_id"] not in (None, trade_id)
                    or str(message.get("code")) != payload["symbol"]
                    or _enum(message.get("action")) != payload["action"]
                    or _enum(message.get("order_lot")) != "Common"
                    or _enum(message.get("order_cond")) != payload["order_cond"]):
                raise ExecutionEvidenceError("stock deal identity/lot/condition mismatch")
            exchange_seq = str(message.get("exchange_seq") or "")
            if not exchange_seq:
                raise ExecutionEvidenceError("deal needs an exchange execution identity for deduplication")
            try:
                timestamp = Decimal(str(message["ts"]))
                if not timestamp.is_finite() or timestamp <= 0:
                    raise ValueError("invalid deal time")
                filled_at = datetime.fromtimestamp(float(timestamp), TAIPEI)
            except (KeyError, InvalidOperation, ValueError, OverflowError) as exc:
                raise ExecutionEvidenceError("deal timestamp is missing or invalid") from exc
            signal_at = datetime.fromisoformat(payload["signal_at"])
            if (filled_at.date().isoformat() != payload["session_date"]
                    or filled_at < signal_at or filled_at > received):
                raise ExecutionEvidenceError("deal is not causally attributable to this session")
            shares = _positive_integer(message.get("quantity")) * BOARD_LOT_SHARES
            price = _source_price(message.get("price"), filled_at.date(), payload["security_type"])
            receipt_id = hashlib.sha256(_json([account_key, payload["session_date"],
                                               trade_id, exchange_seq]).encode()).hexdigest()
            receipt = {"receipt_id": receipt_id, "intent_key": row["key"], "market": payload["market"],
                       "symbol": payload["symbol"], "action": payload["action"], "shares": shares,
                       "price": price, "fill_at": filled_at.isoformat(), "trade_id": trade_id,
                       "exchange_seq": exchange_seq, "fill_contract": BROKER_FILL,
                       "simulation_only": True, "broker_confirmed": True}
            encoded = _json(receipt)
            duplicate = self.db.execute("SELECT payload FROM fills WHERE receipt_id=?", (receipt_id,)).fetchone()
            if duplicate:
                if duplicate[0] != encoded:
                    raise ExecutionEvidenceError("conflicting duplicate deal receipt")
                return False
            total = row["filled_shares"] + shares
            if total + row["cancelled_shares"] > payload["shares"]:
                raise ExecutionEvidenceError("deal quantity exceeds submitted intent")
            self.db.execute("INSERT INTO fills(receipt_id,intent_key,payload) VALUES (?,?,?)",
                            (receipt_id, row["key"], encoded))
            state = ("filled" if total == payload["shares"] else "cancelled"
                     if total + row["cancelled_shares"] == payload["shares"] else "partially_filled")
            self.db.execute("UPDATE intents SET trade_id=?,filled_shares=?,state=? WHERE key=?",
                            (trade_id, total, state, row["key"]))
            return True

    def receipts(self, *, after_sequence: int = 0) -> list[dict[str, Any]]:
        with self._lock:
            return [json.loads(row["payload"]) | {"sequence": row["sequence"]}
                    for row in self.db.execute("SELECT * FROM fills WHERE sequence>? ORDER BY sequence",
                                               (after_sequence,))]

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {"simulation_only": True, "production_order_possible": False,
                    "orders": [dict(row) for row in self.db.execute(
                        "SELECT key,tag,state,filled_shares,cancelled_shares FROM intents ORDER BY rowid")],
                    "receipt_count": self.db.execute("SELECT COUNT(*) FROM fills").fetchone()[0],
                    "pending_event_count": self.db.execute("SELECT COUNT(*) FROM events WHERE processed=0").fetchone()[0],
                    "problems": [dict(row) for row in self.db.execute("SELECT code,detail,blocks FROM problems")]}

    def preflight_summary(self) -> dict[str, int]:
        """One consistent indexed read, without materializing account history."""
        with self._lock:
            row = self.db.execute(
                "SELECT (SELECT COUNT(*) FROM intents WHERE state IN "
                "('submitting','submitted','partially_filled','unknown')), "
                "(SELECT COUNT(*) FROM problems WHERE blocks=1), "
                "(SELECT COUNT(*) FROM events WHERE processed=0)"
            ).fetchone()
            return {
                "unresolved_order_count": row[0],
                "blocking_problem_count": row[1],
                "pending_durable_event_count": row[2],
            }


class ShioajiStockSimulationSession:
    """One simulation-only client, never supplied by callers.

    The ordinary constructor creates a private client and requires login.
    ``from_existing_quote_owner`` attaches to the proven process-local quote
    client without a new login. Neither accepts an arbitrary client, activates
    a CA, reconnects/resends automatically, or submits odd-lot orders.
    """

    def _initialize(self, journal: StockSimulationJournal) -> None:
        import shioaji as sj
        self._owner_lock = journal.path.with_suffix(".writer.lock").open("a")
        try:
            fcntl.flock(self._owner_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._owner_lock.close()
            raise ExecutionEvidenceError("simulation journal already has a session owner") from exc
        self._sj = sj
        try:
            self._account_lock = _acquire_account_writer(journal)
        except BaseException:
            self._owner_lock.close()
            raise
        self._closed = False
        self._shared_token: object | None = None
        self._api: Any = None
        self._private_logged_in = False
        self.journal = journal
        self._account: Any = None
        self._events: queue.SimpleQueue[tuple[str, dict[str, Any], datetime]] = queue.SimpleQueue()
        self._callback_failed = threading.Event()
        self._submit_deadlines: dict[str, float] = {}
        try:
            journal.require_restart_reconciliation()
        except BaseException:
            self._account_lock.close()
            self._owner_lock.close()
            raise

    def __init__(self, journal: StockSimulationJournal):
        self._initialize(journal)
        try:
            self._api = self._sj.Shioaji(simulation=True)
        except BaseException:
            self._account_lock.close()
            self._owner_lock.close()
            raise

    @classmethod
    def from_existing_quote_owner(cls, journal: StockSimulationJournal) -> ShioajiStockSimulationSession:
        """Attach to the existing simulation quote owner, with no login/logout.

        Subscribe outside the quote mutex: a slow subscription must not stall
        stock snapshots. Check the generation again afterwards; an old client
        can never acquire permission to submit on the new one.
        """
        from stockagent.live import quote_provider

        _outside_protected_opening()
        self = cls.__new__(cls)
        # Claim the proven existing client before creating any durable account
        # binding. A missing/replaced owner must leave the index untouched.
        api, token = quote_provider._claim_existing_shioaji_simulation_order_owner()
        callback_installed = False
        initialized = False
        try:
            self._initialize(journal)
            initialized = True
            self._api, self._shared_token = api, token
            with quote_provider._guard_existing_shioaji_simulation_order_owner(self._api, self._shared_token):
                # Treat a setter exception as potentially installed. Cleanup
                # must clear it or retain the exclusive lease on uncertainty.
                callback_installed = True
                self._api.set_order_callback(self._callback)
            accounts = self._api.list_accounts()
            candidates = [a for a in accounts if _enum(getattr(a, "account_type", "")) == "S"
                          and account_fingerprint(str(getattr(a, "broker_id", "")),
                                                  str(getattr(a, "account_id", ""))) == journal.account_key]
            if len(candidates) != 1 or getattr(candidates[0], "signed", None) is not True:
                raise ExecutionEvidenceError("exact signed stock simulation account not found")
            account = candidates[0]
            if self._api.subscribe_trade(account, timeout=5000) is not True:
                raise ExecutionEvidenceError("simulation trade report subscription was not accepted")
            with quote_provider._guard_existing_shioaji_simulation_order_owner(self._api, self._shared_token):
                self._account = account
            return self
        except BaseException:
            try:
                quote_provider._release_existing_shioaji_simulation_order_owner(
                    api, token, clear_callback=callback_installed,
                )
            except Exception:
                # If callback release fails, its process lease stays held.
                pass
            if initialized:
                self._account_lock.close()
                self._owner_lock.close()
            raise

    @contextmanager
    def _submission_generation(self) -> Iterator[None]:
        if self._shared_token is None:
            yield
        else:
            from stockagent.live import quote_provider
            try:
                with quote_provider._guard_existing_shioaji_simulation_order_owner(self._api, self._shared_token):
                    yield
            except RuntimeError as exc:
                raise ExecutionEvidenceError("shared simulation session changed; reconcile before use") from exc

    def shared_readiness(self) -> dict[str, Any]:
        """Observe the same client's report-cache and durable journal gates.

        A healthy cache is a prerequisite, not a real callback or fill proof.
        This does not query, submit, log in, or modify the quote connection.
        """
        if self._closed or self._shared_token is None or self._account is None:
            raise ExecutionEvidenceError("open shared simulation session required")
        with self._submission_generation():
            health = self._api.trade_cache_health(self._account)
        cache_state = _enum(getattr(health, "state", "Unknown"))
        reasons = [{"event_type": _enum(getattr(item, "event_type", "")),
                    "reason": _enum(getattr(item, "reason", ""))}
                   for item in (getattr(health, "reasons", None) or [])]
        summary = self.journal.preflight_summary()
        unresolved = summary["unresolved_order_count"]
        blocking = summary["blocking_problem_count"]
        callbacks_pending = not self._events.empty()
        ready = (cache_state == "Healthy" and not reasons and not unresolved and not blocking
                 and not summary["pending_durable_event_count"] and not callbacks_pending
                 and not self._callback_failed.is_set())
        return {"simulation_only": True, "shared_existing_quote_client": True,
                "extra_login": False, "report_cache_state": cache_state,
                "report_cache_reasons": reasons, "unresolved_order_count": unresolved,
                "blocking_problem_count": blocking,
                "pending_durable_event_count": summary["pending_durable_event_count"],
                "callbacks_awaiting_drain": callbacks_pending,
                "report_callback_failed": self._callback_failed.is_set(),
                "preflight_ready": ready, "actual_callback_or_fill_verified": False}

    def refresh_shared_report_baseline(self) -> dict[str, Any]:
        """Reconcile the subscribed cache off the quote mutex, then recheck.

        A caller must run this outside the protected opening window. This is a
        status query, not an order or evidence that an unknown order was absent.
        """
        if self._closed or self._shared_token is None or self._account is None:
            raise ExecutionEvidenceError("open shared simulation session required")
        _outside_protected_opening()
        with self._submission_generation():
            pass
        self._api.update_status(self._account, timeout=5000)
        # A disconnected/replaced generation must never inherit this result.
        return self.shared_readiness()

    def _callback(self, state: Any, message: Any) -> None:
        try:
            state_name = _enum(state)
            if state_name not in {"StockDeal", "SDEAL", "StockOrder", "SORDER"}:
                return
            # SDK order payloads are plain nested dictionaries. JSON freezes
            # them before the native callback returns; no I/O/network here.
            frozen = json.loads(json.dumps(dict(message), default=_enum, allow_nan=False))
            self._events.put((state_name, frozen, _now()))
        except Exception:
            self._callback_failed.set()

    def login(self, *, api_key: str, secret_key: str) -> None:
        if self._closed:
            raise ExecutionEvidenceError("closed simulation session cannot be reused")
        if self._shared_token is not None:
            raise ExecutionEvidenceError("shared simulation session must never log in again")
        if self._account is not None:
            raise ExecutionEvidenceError("reuse the existing logged-in simulation session")
        if self._private_logged_in:
            raise ExecutionEvidenceError("previous simulation login cleanup is unresolved")
        try:
            # SDK 1.7.9 rejects set_order_callback before login. Start with a
            # fresh, unsubscribed token so no trade report can race the setter.
            accounts = self._api.login(api_key=api_key, secret_key=secret_key,
                                       subscribe_trade=False, force_refresh=True)
            self._private_logged_in = True
            candidates = [a for a in accounts if _enum(getattr(a, "account_type", "")) == "S"
                          and account_fingerprint(str(getattr(a, "broker_id", "")),
                                                  str(getattr(a, "account_id", ""))) == self.journal.account_key]
            if len(candidates) != 1 or getattr(candidates[0], "signed", None) is not True:
                raise ExecutionEvidenceError("exact signed stock simulation account not found")
            self._api.set_order_callback(self._callback)
            if self._api.subscribe_trade(candidates[0], timeout=5000) is not True:
                raise ExecutionEvidenceError("simulation trade report subscription was not accepted")
            self._account = candidates[0]
        except Exception as exc:
            if self._private_logged_in:
                try:
                    self._api.logout()
                except Exception:
                    self._callback_failed.set()
                    raise ExecutionEvidenceError("simulation login cleanup is unresolved") from exc
                else:
                    self._private_logged_in = False
            raise

    def submit(self, intent: StockSimulationIntent) -> None:
        # Do not accept a caller-supplied historical clock on a network writer.
        observed = _now()
        payload = intent.payload()
        signal_at = datetime.fromisoformat(payload["signal_at"])
        if self._account is None:
            raise ExecutionEvidenceError("simulation login required")
        # Fail before making an intent durable when the quote owner has already
        # been replaced. A second fence immediately before send handles races.
        with self._submission_generation():
            pass
        if (signal_at.date() != observed.date() or signal_at > observed
                or not time(9) <= observed.time() < time(13, 30)):
            raise ExecutionEvidenceError("live submission must belong to the current trading session")
        if observed.time() >= time(13, 25) and (intent.price_type != "LMT" or intent.order_type != "ROD"):
            raise ExecutionEvidenceError("closing auction requires a limit ROD order")
        self.drain()
        if self._callback_failed.is_set():
            raise ExecutionEvidenceError("callback failure requires reconciliation")
        if self._shared_token is not None and not self.shared_readiness()["preflight_ready"]:
            raise ExecutionEvidenceError("shared simulation report/journal preflight is not ready")
        contract = self._api.contracts.get(intent.symbol)
        if (contract is None or str(getattr(contract, "code", "")) != intent.symbol
                or _enum(getattr(contract, "security_type", "")) != "STK"
                or _enum(getattr(contract, "exchange", "")) not in {"TSE", "OTC"}):
            raise ExecutionEvidenceError("unsupported or mismatched stock contract")
        info = self._api.contracts.info(contract)
        if (info is None or _enum(getattr(info, "currency", "")) != "TWD"
                or getattr(info, "unit", None) != BOARD_LOT_SHARES
                or str(getattr(info, "update_date", "")) != observed.date().isoformat()
                or getattr(info, "trading_suspended", None) is not False):
            raise ExecutionEvidenceError("fresh regular-board TWD contract rules required")
        if intent.price_type == "LMT":
            for field, is_upper in (("limit_up", True), ("limit_down", False)):
                limit = getattr(info, field, None)
                if intent.security_type == "stock" and (limit is None or Decimal(str(limit)) <= 0):
                    raise ExecutionEvidenceError("ordinary stock price limits are missing")
                if limit is not None and Decimal(str(limit)) > 0:
                    if (Decimal(payload["price"]) > Decimal(str(limit))) if is_upper else (
                        Decimal(payload["price"]) < Decimal(str(limit))
                    ):
                        raise ExecutionEvidenceError("limit order outside current contract bounds")
        row = self.journal.prepare_and_claim(intent, in_flight=frozenset(self._submit_deadlines))
        try:
            order = self._sj.StockOrder(
                action=getattr(self._sj.Action, intent.action), price=float(payload["price"]),
                quantity=int(payload["shares"]) // BOARD_LOT_SHARES,
                price_type=getattr(self._sj.StockPriceType, intent.price_type),
                order_type=getattr(self._sj.OrderType, intent.order_type),
                order_lot=self._sj.StockOrderLot.Common,
                order_cond=getattr(self._sj.StockOrderCond, intent.order_cond),
                daytrade_short=intent.daytrade_short, custom_field=row["tag"], account=self._account)
        except Exception:
            self.journal.reject_before_send(intent.key)
            raise ExecutionEvidenceError("SDK order validation failed before any submission") from None
        invoked = False
        try:
            # A returned Trade (even Filled) is not consumed as a StockDeal.
            # Native callbacks, including ones racing this return, own fills.
            with self._submission_generation():
                if self._shared_token is not None:
                    health = self._api.trade_cache_health(self._account)
                    if (_enum(getattr(health, "state", "Unknown")) != "Healthy"
                            or bool(getattr(health, "reasons", None))
                            or self._callback_failed.is_set() or not self._events.empty()):
                        raise ExecutionEvidenceError("shared report health changed before submission")
                self._submit_deadlines[intent.key] = monotonic_time.monotonic() + 5.0
                invoked = True
                self._api.place_order(contract, order, timeout=0)
        except Exception as exc:
            if not invoked:
                self.journal.reject_before_send(intent.key)
                raise ExecutionEvidenceError("shared preflight or session changed before submission; nothing sent") from exc
            self.journal.unknown(intent.key)
            raise ExecutionEvidenceError("simulation submit outcome unknown; do not resend") from None
        finally:
            self.drain()

    def drain(self) -> int:
        count = 0
        if self._callback_failed.is_set():
            self.journal.problem("callback_decode_failed", "broker callback requires reconciliation")
        while True:
            try:
                state, message, received_at = self._events.get_nowait()
            except queue.Empty:
                break
            # Crash after this commit is recoverable; crash after a fill commit
            # but before finish_event replays the same idempotent receipt.
            self.journal.record_event(state, message, received_at)
        for event in self.journal.pending_events():
            state, message = event["state"], json.loads(event["payload"])
            received_at = datetime.fromisoformat(event["received_at"])
            try:
                if state in {"StockDeal", "SDEAL"}:
                    count += int(self.journal.stock_deal(message, received_at=received_at))
                elif state in {"StockOrder", "SORDER"}:
                    order = message.get("order") or {}
                    account = order.get("account") or {}
                    identity = account_fingerprint(str(account.get("broker_id") or ""),
                                                   str(account.get("account_id") or ""))
                    if identity != self.journal.account_key:
                        self.journal.finish_event(event["id"])
                        continue
                    tag, trade_id = str(order.get("custom_field") or ""), str(order.get("id") or "")
                    if not self.journal.owns(tag, trade_id):
                        self.journal.finish_event(event["id"])
                        continue
                    operation = message.get("operation") or {}
                    if operation.get("op_code") != "00":
                        # No automatic local match on any rejection. Surface the
                        # code, without serializing a possibly sensitive op_msg.
                        self.journal.problem("order_operation_failed", str(operation.get("op_code")), blocks=False)
                        if operation.get("op_type") == "New":
                            self.journal.order_rejected(tag=tag,
                                symbol=str((message.get("contract") or {}).get("code") or ""),
                                action=_enum(order.get("action")))
                        # Cancel/update failure preserves the prior order. An
                        # explicit new-order rejection cannot create a fill.
                        self.journal.finish_event(event["id"])
                        continue
                    self.journal.order_ack(tag=tag, trade_id=trade_id, account_key=identity,
                        symbol=str((message.get("contract") or {}).get("code") or ""),
                        action=_enum(order.get("action")))
                    status = message.get("status") or {}
                    if operation.get("op_type") in {"Cancel", "UpdateQty"}:
                        self.journal.order_cancelled(tag=tag, cancelled_lots=status.get("cancel_quantity"))
            except (ValueError, ExecutionEvidenceError, sqlite3.IntegrityError) as exc:
                self.journal.problem(type(exc).__name__, str(exc))
            self.journal.finish_event(event["id"])
        for key, deadline in list(self._submit_deadlines.items()):
            if monotonic_time.monotonic() >= deadline:
                self.journal.unknown(key)
                del self._submit_deadlines[key]
        return count

    def logout(self) -> None:
        if self._closed:
            return
        if self._shared_token is not None:
            # Detaching an unresolved order would drop its later report. Keep
            # the lease and locks until the owner explicitly reconciles it.
            self.drain()
            if self.journal.preflight_summary()["unresolved_order_count"]:
                raise ExecutionEvidenceError("unresolved shared orders require reconciliation before detach")
            from stockagent.live import quote_provider
            quote_provider._release_existing_shioaji_simulation_order_owner(self._api, self._shared_token)
            self._account = None
            self._closed = True
            self._account_lock.close()
            self._owner_lock.close()
            return
        try:
            try:
                self.drain()
            finally:
                if self._private_logged_in:
                    self._api.logout()
        finally:
            self._account = None
            self._private_logged_in = False
            self._closed = True
            self._account_lock.close()
            self._owner_lock.close()

    def __enter__(self) -> ShioajiStockSimulationSession:
        return self

    def __exit__(self, *_args: Any) -> None:
        self.logout()
