"""Bounded, pure request planning for verified FinMind range endpoints.

The queue and per-partition receipts remain owned by the existing worker.  A
batch changes only HTTP request shape; it cannot make a hole or a foreign row
disappear.  Most whole-market FinMind endpoints are *single-date* queries and
therefore are deliberately absent from this allowlist.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Generic, Iterable, Mapping, Protocol, TypeVar


class PartitionTask(Protocol):
    dataset: str
    data_id: str
    partition: str
    kind: str
    priority: int
    state: str


TaskT = TypeVar("TaskT", bound=PartitionTask)


class BatchContractError(ValueError):
    """A batch cannot be safely dispatched or split into source partitions."""


@dataclass(frozen=True)
class RangeContract:
    grain: str
    first_date: date
    max_partitions: int | None
    max_response_rows: int
    documentation_url: str
    max_response_bytes: int = 64 * 1024 * 1024


# These three endpoints have documented range queries AND locally verified
# inclusive end_date behavior; see finmind_partition_semantics_2026-09-27.json.
# No artificial calendar-length ceiling. Protect memory with a bounded decoded
# HTTP body and a row safety check; learn smaller batches only after actual
# resource/transport failures. These are local safeguards, not provider limits.
RANGE_CONTRACTS = {
    "TaiwanBusinessIndicator": RangeContract(
        "year", date(1982, 1, 1), None, 1_000_000,
        "https://finmind.github.io/tutor/TaiwanMarket/Others/",
    ),
    "CnnFearGreedIndex": RangeContract(
        "year", date(2011, 1, 3), None, 1_000_000,
        "https://finmind.github.io/llms-full.txt",
    ),
    "TaiwanOptionVix": RangeContract(
        "month", date(2026, 3, 1), None, 1_000_000,
        "https://finmind.github.io/tutor/TaiwanMarket/Derivative/",
    ),
}
BATCH_CONTRACT_VERSION = 2


def _period_start(day: date, contract: RangeContract) -> date:
    nominal = date(day.year, 1 if contract.grain == "year" else day.month, 1)
    return max(nominal, contract.first_date)


def _period_end(start: date, grain: str) -> date:
    if grain == "year":
        return date(start.year + 1, 1, 1)
    return date(start.year + (start.month == 12), start.month % 12 + 1, 1)


def _task_start(task: PartitionTask, contract: RangeContract) -> date:
    try:
        start = date.fromisoformat(task.partition)
    except (TypeError, ValueError) as exc:
        raise BatchContractError("invalid_partition_date") from exc
    if task.partition != start.isoformat() or start != _period_start(start, contract):
        raise BatchContractError("invalid_partition_boundary")
    return start


@dataclass(frozen=True)
class RangeBatch(Generic[TaskT]):
    """Oldest-first constituent tasks with a closed provider request window."""

    dataset: str
    kind: str
    tasks: tuple[TaskT, ...]
    start_date: date
    end_date: date
    observed_through: date | None = None

    def params(self) -> dict[str, str]:
        return {
            "dataset": self.dataset,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
        }

    def metadata(self) -> dict[str, Any]:
        return {
            "batch_contract_version": BATCH_CONTRACT_VERSION,
            "query_shape": "whole_market_inclusive_date_range",
            "request_start_date": self.start_date.isoformat(),
            "request_end_date": self.end_date.isoformat(),
            "request_end_inclusive": True,
            "request_count": 1,
            "partition_count": len(self.tasks),
            "partitions": [task.partition for task in self.tasks],
            "documentation_url": RANGE_CONTRACTS[self.dataset].documentation_url,
            "calendar_length_limit": None,
            "decoded_response_byte_limit": RANGE_CONTRACTS[self.dataset].max_response_bytes,
            "observed_through": self.observed_through.isoformat() if self.observed_through else None,
        }


def coalesce_pending_tasks(
    seed: TaskT,
    candidates: Iterable[TaskT],
    *,
    today: date,
    max_years: int | None = None,
    max_months: int | None = None,
) -> RangeBatch[TaskT] | None:
    """Join older, contiguous pending tasks without crossing ownership or gaps.

    The caller must select candidates whose retry/release time is already due
    and claim every returned task atomically before dispatch.  This pure helper
    performs no database writes or requests.  The seed may be a due retry, but
    only pending neighbors can be added. All required background priorities may
    share a request; the arbitrary 2014 priority boundary is not an API limit.
    A new current-period seed may include history when the caller has admitted
    background quota. Existing completed refreshes are never re-downloaded just
    to join batches. Optional limits are learned resource bounds, not year caps.
    """

    for value in (max_years, max_months):
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 1):
            raise ValueError("batch limits must be positive integers")
    contract = RANGE_CONTRACTS.get(seed.dataset)
    if (contract is None or seed.kind != contract.grain or seed.data_id
            or not 0 <= seed.priority < 8 or seed.state not in {"pending", "failed"}):
        return None
    seed_start = _task_start(seed, contract)
    if seed_start > today:
        return None
    supplied = max_years if contract.grain == "year" else max_months
    # The catalog's first date bounds the finite possible partition universe.
    available = ((seed_start.year - contract.first_date.year + 1) if seed.kind == 'year' else
                 (seed_start.year - contract.first_date.year) * 12 + seed_start.month - contract.first_date.month + 1)
    limit = min(available, supplied or available, contract.max_partitions or available)
    if limit < 2:
        return None
    eligible: dict[date, TaskT] = {}
    for task in candidates:
        if (task.dataset != seed.dataset or task.data_id or task.kind != seed.kind
                or not 0 <= task.priority < 8 or task.state != "pending"):
            continue
        start = _task_start(task, contract)
        if start >= seed_start:
            continue
        if start in eligible:
            raise BatchContractError("duplicate_partition")
        eligible[start] = task
    selected = [seed]
    cursor = seed_start
    while len(selected) < limit and cursor > contract.first_date:
        older = _period_start(cursor - timedelta(days=1), contract)
        neighbor = eligible.get(older)
        if neighbor is None:
            break
        selected.append(neighbor)
        cursor = older
    if len(selected) < 2:
        return None
    return RangeBatch(
        seed.dataset, seed.kind, tuple(reversed(selected)), cursor,
        min(_period_end(seed_start, seed.kind) - timedelta(days=1), today), today,
    )


def split_batch_rows(
    batch: RangeBatch[TaskT], rows: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Validate the entire response before exposing any partition for storage.

    Preserve every row, original value and within-partition ordering, including
    valid duplicates.  An empty partition is returned explicitly; its existing
    worker-owned receipt policy decides whether that means observed_empty.
    This is a query-boundary check, not a claim of upstream completeness.
    """

    contract = RANGE_CONTRACTS.get(batch.dataset)
    if contract is None or batch.kind != contract.grain:
        raise BatchContractError("unsupported_batch_contract")
    if len(batch.tasks) < 2 or (contract.max_partitions is not None and len(batch.tasks) > contract.max_partitions):
        raise BatchContractError("invalid_batch_size")
    partitions: dict[str, list[dict[str, Any]]] = {}
    expected_start = batch.start_date
    for task in batch.tasks:
        if (task.dataset != batch.dataset or task.data_id or task.kind != batch.kind
                or not 0 <= task.priority < 8):
            raise BatchContractError("foreign_batch_task")
        start = _task_start(task, contract)
        if start != expected_start:
            raise BatchContractError("noncontiguous_batch")
        partitions[task.partition] = []
        expected_start = _period_end(start, batch.kind)
    expected_end = expected_start - timedelta(days=1)
    if batch.observed_through is not None:
        expected_end = min(expected_end, batch.observed_through)
    if expected_end != batch.end_date or batch.end_date < date.fromisoformat(batch.tasks[-1].partition):
        raise BatchContractError("invalid_batch_end")
    if not isinstance(rows, list) or len(rows) > contract.max_response_rows:
        raise BatchContractError("batch_response_row_limit")
    for row in rows:
        if not isinstance(row, Mapping):
            raise BatchContractError("invalid_batch_row")
        if "dataset" in row and row["dataset"] != batch.dataset:
            raise BatchContractError("foreign_response_dataset")
        stamp = row.get("date")
        try:
            if not isinstance(stamp, str) or len(stamp) < 10:
                raise ValueError("missing date")
            day = date.fromisoformat(stamp[:10])
            if stamp[:10] != day.isoformat():
                raise ValueError("noncanonical date")
            if len(stamp) > 10:
                if stamp[10] not in {"T", " "}:
                    raise ValueError("invalid timestamp delimiter")
                datetime.fromisoformat(stamp)
        except (TypeError, ValueError) as exc:
            raise BatchContractError("invalid_response_date") from exc
        if not batch.start_date <= day <= batch.end_date:
            raise BatchContractError("response_outside_batch")
        partition = _period_start(day, contract).isoformat()
        if partition not in partitions:
            raise BatchContractError("response_outside_partition")
        partitions[partition].append(row)
    return partitions
