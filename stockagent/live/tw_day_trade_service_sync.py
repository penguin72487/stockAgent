"""Compact receipts for the Discord -> paper engine -> dashboard pipeline.

The large simulation state is intentionally not a synchronization primitive.
The paper engine publishes this small receipt only after every related state
file has been atomically replaced.  Readers can therefore acknowledge one
committed revision without repeatedly parsing multi-megabyte ledgers.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any, Final, Mapping


SERVICE_SYNC_FILENAME: Final[str] = "service_sync.json"
SERVICE_SYNC_DURABLE_ACK_FILENAME: Final[str] = "service_sync_durable_ack.json"
SERVICE_SYNC_DURABLE_ACK_CONTRACT: Final[str] = "post_commit_directory_fsync_v1"
DISCORD_SERVICE_STATUS_FILENAME: Final[str] = "service_status.json"
SERVICE_SYNC_SCHEMA_VERSION: Final[int] = 1
PAPER_JOURNAL_FILENAMES: Final[tuple[str, ...]] = (
    "signals.jsonl", "orders.jsonl", "fills.jsonl", "events.jsonl",
    "marks.jsonl", "benchmark_marks.jsonl",
)


def paper_journal_generations(root: Path) -> dict[str, list[int] | None]:
    """Bind the commit to exact local journal metadata, not size alone.

    This is a read-consistency fence, NOT a prefix-integrity proof. It cannot
    authorize skipping the canonical reader's rewrite detection or hashing.
    Missing files are explicit, and permission/I/O errors remain errors.
    """
    return _paper_file_generations(root, PAPER_JOURNAL_FILENAMES)


def paper_projection_generations(root: Path) -> dict[str, list[int] | None]:
    return _paper_file_generations(root, ("state.json", "positions.json", "status.json"))


def _paper_file_generations(root: Path, names) -> dict[str, list[int] | None]:
    result = {}
    for name in names:
        try:
            stat = (Path(root) / name).stat()
        except FileNotFoundError:
            result[name] = None
        else:
            result[name] = [stat.st_dev, stat.st_ino, stat.st_size,
                            stat.st_mtime_ns, stat.st_ctime_ns]
    return result


class PaperReadChanged(OSError):
    """The requested projection is not one stable committed paper view."""


@contextmanager
def committed_paper_read_guard(root: Path):
    """Bracket financial readers without locking or parsing large state.

    New receipts bind BOTH journals and replaced projections. A matching
    journal size alone cannot validate a state rename before the commit marker.
    Legacy receipts retain their prior diagnostic semantics, not this proof.
    """
    receipt = load_service_sync(root)
    if receipt is None and (Path(root) / SERVICE_SYNC_FILENAME).exists():
        # An absent historical marker is legacy diagnostic input; a present
        # corrupt/unsupported marker is not. Never downgrade known commit
        # corruption into permission to publish an unfenced financial view.
        raise PaperReadChanged("paper commit marker is invalid or unreadable")
    if not isinstance(receipt, Mapping) or "projection_generations" not in receipt:
        yield
        return
    identity = tuple(receipt.get(key) for key in ("engine_run_id", "state_revision", "content_revision"))
    if (not isinstance(identity[0], str) or not identity[0]
            or type(identity[1]) is not int or identity[1] <= 0
            or type(identity[2]) is not int or identity[2] < 0):
        raise PaperReadChanged("invalid committed paper identity")

    def matches():
        return (paper_projection_generations(root) == receipt.get("projection_generations")
                and paper_journal_generations(root) == receipt.get("journal_generations"))

    if not matches():
        raise PaperReadChanged("paper files are ahead of their commit marker")
    yield
    after = load_service_sync(root) or {}
    if (tuple(after.get(key) for key in ("engine_run_id", "state_revision", "content_revision"))
            != identity or not matches()):
        raise PaperReadChanged("paper commit changed during view construction")


def read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root is not an object: {path}")
    return payload


def read_service_ack_pair(engine_path: Path, discord_path: Path, *,
                          timeout_seconds: float = 2., poll_seconds: float = .05,
                          reader=None):
    """Observe one stable committed engine revision and its exact Discord ACK.

    Whole-second retries can phase-lock with the independent one-second owners
    and report a perpetual one-revision lag. Short bounded observation is not a
    looser ACK: require exact run/revision equality and bracket the bot read
    with the same engine revision. Freshness/source/market checks stay with the
    caller. This reads compact receipts only, never state, quotes or orders.
    """
    if not 0 < poll_seconds <= timeout_seconds <= 10:
        raise ValueError("ACK observation requires 0 < poll <= timeout <= 10 seconds")
    def committed_read(path):
        if Path(path).name == SERVICE_SYNC_FILENAME:
            receipt = load_service_sync(Path(path).parent)
            if receipt is None:
                raise ValueError("engine receipt is not durably acknowledged")
            return receipt
        return read_json_object(path)

    read = reader or committed_read
    started = time.monotonic()
    deadline = started + timeout_seconds
    attempts = 0
    while True:
        attempts += 1
        try:
            before, bot, engine = read(engine_path), read(discord_path), read(engine_path)
            if not all(isinstance(value, dict) for value in (before, bot, engine)):
                raise ValueError("object service receipts required")
        except (OSError, ValueError, TypeError):
            before, bot, engine = {}, {}, {}
        revision, acknowledged = engine.get("state_revision"), bot.get("engine_state_revision")
        valid = type(revision) is int and revision > 0 and type(acknowledged) is int and acknowledged > 0
        lag = revision - acknowledged if valid else None
        run = engine.get("engine_run_id")
        stable = bool(isinstance(run, str) and run and before.get("engine_run_id") == run
                      and before.get("state_revision") == revision and type(before.get("state_revision")) is int)
        matched = bool(valid and stable and lag == 0 and bot.get("engine_run_id") == run)
        remaining = deadline - time.monotonic()
        if matched or remaining <= 0:
            return engine, bot, {"matched": matched, "stable_engine_pair": stable,
                                 "revision_lag": lag, "attempts": attempts,
                                 "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
                                 "policy": "bounded_stable_exact_run_revision_ack_v1"}
        time.sleep(min(poll_seconds, remaining))


def load_service_sync(state_dir: Path) -> dict[str, Any] | None:
    """Load the paper engine's compact commit receipt, if one exists."""

    path = Path(state_dir) / SERVICE_SYNC_FILENAME
    try:
        payload = read_json_object(path)
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    try:
        supported = int(payload.get("schema_version") or 0) == SERVICE_SYNC_SCHEMA_VERSION
    except (TypeError, ValueError, OverflowError):
        return None
    if not supported:
        return None
    if "durability_ack" in payload:
        # A marker rename may be visible before its directory fsync returns.
        # Only the canonical writer, AFTER that barrier succeeds, may publish
        # this separate admission hint. It is not another financial ledger.
        # A missing/lost hint after a crash fails closed until the engine's
        # ordinary next commit; legacy receipts retain diagnostic semantics.
        expected = payload["durability_ack"]
        if (not isinstance(expected, dict)
                or expected.get("contract") != SERVICE_SYNC_DURABLE_ACK_CONTRACT
                or not isinstance(expected.get("commit_id"), str)
                or not expected["commit_id"]):
            return None
        try:
            ack = read_json_object(Path(state_dir) / SERVICE_SYNC_DURABLE_ACK_FILENAME)
            generation = _paper_file_generations(state_dir, (SERVICE_SYNC_FILENAME,))
        except (OSError, TypeError, ValueError):
            return None
        if (ack.get("schema_version") != 1 or ack.get("durability_ack") != expected
                or ack.get("marker_generation") != generation[SERVICE_SYNC_FILENAME]
                or any(ack.get(key) != payload.get(key) for key in
                       ("engine_run_id", "state_revision", "content_revision"))):
            return None
    return payload


def mode_from_service_sync(
    receipt: Mapping[str, Any] | None,
    market: str,
) -> dict[str, Any] | None:
    if not isinstance(receipt, Mapping):
        return None
    row = (receipt.get("modes") or {}).get(str(market))
    return dict(row) if isinstance(row, Mapping) else None


def parse_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def age_seconds(value: object, *, now: datetime | None = None) -> float | None:
    parsed = parse_timestamp(value)
    if parsed is None:
        return None
    observed = now or datetime.now(timezone.utc)
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    return max(0.0, (observed.astimezone(timezone.utc) - parsed).total_seconds())


__all__ = [
    "DISCORD_SERVICE_STATUS_FILENAME",
    "SERVICE_SYNC_FILENAME",
    "SERVICE_SYNC_DURABLE_ACK_FILENAME",
    "SERVICE_SYNC_DURABLE_ACK_CONTRACT",
    "SERVICE_SYNC_SCHEMA_VERSION",
    "age_seconds",
    "load_service_sync",
    "PaperReadChanged",
    "committed_paper_read_guard",
    "paper_journal_generations",
    "paper_projection_generations",
    "mode_from_service_sync",
    "parse_timestamp",
    "read_json_object",
    "read_service_ack_pair",
]
