from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date

import pytest

from downloader.finmind_batching import (
    BatchContractError, coalesce_pending_tasks, split_batch_rows,
)


@dataclass(frozen=True)
class Task:
    dataset: str = "TaiwanBusinessIndicator"
    data_id: str = ""
    partition: str = "2025-01-01"
    kind: str = "year"
    priority: int = 2
    state: str = "pending"


TODAY = date(2026, 9, 27)


def years(*values: int) -> list[Task]:
    return [Task(partition=f"{year}-01-01") for year in values]


def test_merge_all_contiguous_years_and_inclusive_request_end() -> None:
    seed = Task()
    batch = coalesce_pending_tasks(seed, years(2019, 2022, 2020, 2024, 2023, 2021), today=TODAY)
    assert batch is not None
    assert [task.partition for task in batch.tasks] == [f"{year}-01-01" for year in range(2019, 2026)]
    assert batch.tasks[-1] is seed
    assert batch.params() == {"dataset": seed.dataset, "start_date": "2019-01-01", "end_date": "2025-12-31"}
    assert batch.metadata()["partition_count"] == 7
    assert batch.metadata()["request_count"] == 1
    assert batch.metadata()["request_end_inclusive"] is True


def test_hole_and_complete_partition_cannot_be_silently_bridged() -> None:
    seed = Task()
    batch = coalesce_pending_tasks(seed, [*years(2024, 2022), replace(seed, partition="2023-01-01", state="complete")], today=TODAY)
    assert batch is not None
    assert [task.partition for task in batch.tasks] == ["2024-01-01", "2025-01-01"]
    assert coalesce_pending_tasks(seed, years(2023, 2022), today=TODAY) is None


def test_future_retry_and_inflight_neighbors_are_not_eligible_by_state() -> None:
    seed = Task()
    for state in ("failed", "inflight", "observed_empty", "blocked"):
        assert coalesce_pending_tasks(seed, [replace(seed, partition="2024-01-01", state=state)], today=TODAY) is None
    # A seed chosen by the existing due-retry queue may still coalesce.
    assert coalesce_pending_tasks(replace(seed, state="failed"), years(2024), today=TODAY) is not None


@pytest.mark.parametrize("seed", [
    Task(dataset="TaiwanStockFinancialStatements"),
    Task(data_id="2330"), Task(kind="day"), Task(priority=8),
    Task(state="complete"), Task(state="inflight"),
    Task(partition="2027-01-01"),
])
def test_unknown_single_day_current_and_incremental_requests_stay_unbatched(seed: Task) -> None:
    assert coalesce_pending_tasks(seed, years(2024, 2023), today=TODAY) is None


@pytest.mark.parametrize("neighbor", [
    Task(dataset="CnnFearGreedIndex", partition="2024-01-01"),
    Task(data_id="2330", partition="2024-01-01"),
    Task(kind="month", partition="2024-01-01"),
    Task(priority=8, partition="2024-01-01"),
])
def test_cannot_cross_dataset_identity_grain_or_priority(neighbor: Task) -> None:
    assert coalesce_pending_tasks(Task(), [neighbor], today=TODAY) is None


def test_month_batch_is_resource_bounded_and_crosses_year_boundary() -> None:
    seed = Task(dataset="TaiwanOptionVix", kind="month", partition="2027-02-01")
    candidates = [replace(seed, partition=p) for p in ("2027-01-01", "2026-12-01", "2026-11-01")]
    batch = coalesce_pending_tasks(seed, candidates, today=date(2027, 3, 1), max_months=12)
    assert batch is not None
    assert len(batch.tasks) == 4  # no artificial three-month calendar bound
    assert batch.params()["start_date"] == "2026-11-01"
    assert batch.params()["end_date"] == "2027-02-28"
    leap_seed = replace(seed, partition="2028-02-01")
    leap = coalesce_pending_tasks(leap_seed, [replace(leap_seed, partition="2028-01-01")], today=date(2028, 3, 1))
    assert leap is not None and leap.end_date == date(2028, 2, 29)


def test_partial_first_year_is_not_replaced_with_earlier_unsupported_date() -> None:
    seed = Task(dataset="CnnFearGreedIndex", partition="2012-01-01")
    batch = coalesce_pending_tasks(seed, [replace(seed, partition="2011-01-03")], today=TODAY)
    assert batch is not None
    assert batch.start_date == date(2011, 1, 3)
    assert batch.end_date == date(2012, 12, 31)
    rows = [{"date": "2011-01-03", "value": 1}, {"date": "2012-01-01", "value": 2}]
    assert split_batch_rows(batch, rows) == {"2011-01-03": rows[:1], "2012-01-01": rows[1:]}
    with pytest.raises(BatchContractError, match="invalid_partition_boundary"):
        coalesce_pending_tasks(seed, [replace(seed, partition="2011-01-01")], today=TODAY)


def test_split_preserves_all_rows_values_duplicates_and_order() -> None:
    batch = coalesce_pending_tasks(Task(), years(2024), today=TODAY)
    assert batch is not None
    rows = [
        {"date": "2025-12-31", "value": -1.23456789},
        {"date": "2024-02-29T09:01:00", "value": None},
        {"date": "2024-12-31 23:59:59+08:00", "value": 0},
        {"date": "2025-12-31", "value": -1.23456789},
    ]
    parts = split_batch_rows(batch, rows)
    assert parts["2024-01-01"] == [rows[1], rows[2]]
    assert parts["2025-01-01"] == [rows[0], rows[3]]
    assert parts["2025-01-01"][0] is rows[0]
    assert sum(map(len, parts.values())) == len(rows)


def test_empty_response_keeps_explicit_partitions_without_manufacturing_rows() -> None:
    batch = coalesce_pending_tasks(Task(), years(2024), today=TODAY)
    assert batch is not None
    assert split_batch_rows(batch, []) == {"2024-01-01": [], "2025-01-01": []}
    assert split_batch_rows(batch, [{"date": "2025-03-01"}]) == {"2024-01-01": [], "2025-01-01": [{"date": "2025-03-01"}]}


@pytest.mark.parametrize("bad", [
    {}, {"date": None}, {"date": 20250101}, {"date": "2025-02-29"},
    {"date": "2025-1-01"}, {"date": "20250101"}, {"date": "2025-01-01garbage"},
    {"date": "2025-01-01T99:99:99"}, {"date": "2025-01-01 "},
    {"date": "2023-12-31"}, {"date": "2026-01-01"},
    {"date": "2025-01-01", "dataset": "Foreign"}, None,
])
def test_invalid_or_foreign_response_fails_before_returning_any_partition(bad: dict) -> None:
    batch = coalesce_pending_tasks(Task(), years(2024), today=TODAY)
    assert batch is not None
    with pytest.raises(BatchContractError):
        split_batch_rows(batch, [{"date": "2024-01-01"}, bad])


def test_row_limit_is_fail_closed_not_truncation() -> None:
    batch = coalesce_pending_tasks(Task(), years(2024), today=TODAY)
    assert batch is not None
    with pytest.raises(BatchContractError, match="batch_response_row_limit"):
        split_batch_rows(batch, [{"date": "2024-01-01"}] * 1_000_001)


def test_forged_or_gapped_batch_cannot_reassign_rows() -> None:
    batch = coalesce_pending_tasks(Task(), years(2024), today=TODAY)
    assert batch is not None
    with pytest.raises(BatchContractError, match="noncontiguous_batch"):
        split_batch_rows(replace(batch, tasks=(Task(partition="2023-01-01"), Task())), [])
    with pytest.raises(BatchContractError, match="invalid_batch_end"):
        split_batch_rows(replace(batch, end_date=date(2026, 1, 1)), [])
    with pytest.raises(BatchContractError, match="foreign_batch_task"):
        split_batch_rows(replace(batch, tasks=(replace(batch.tasks[0], data_id="2330"), batch.tasks[1])), [])


def test_limits_lower_not_enlarge_resource_bound() -> None:
    assert coalesce_pending_tasks(Task(), years(2024), today=TODAY, max_years=1) is None
    batch = coalesce_pending_tasks(Task(), years(2024, 2023, 2022), today=TODAY, max_years=2)
    assert batch is not None and len(batch.tasks) == 2
    for value in (0, -1, True, 1.5):
        with pytest.raises(ValueError, match="positive integers"):
            coalesce_pending_tasks(Task(), years(2024), today=TODAY, max_years=value)


def test_duplicate_pending_task_and_malformed_partition_are_rejected() -> None:
    with pytest.raises(BatchContractError, match="duplicate_partition"):
        coalesce_pending_tasks(Task(), years(2024, 2024), today=TODAY)
    for partition in ("not-a-date", "2024-04-01", "1981-01-01"):
        with pytest.raises(BatchContractError):
            coalesce_pending_tasks(Task(), [Task(partition=partition)], today=TODAY)


def test_seed_in_candidate_snapshot_is_not_duplicated() -> None:
    batch = coalesce_pending_tasks(Task(), years(2025, 2024), today=TODAY)
    assert batch is not None and len(batch.tasks) == 2


def test_44_year_full_history_in_one_request_including_fresh_current_partition():
    seed = Task(partition='2026-01-01', priority=0)
    old = [Task(partition=f'{year}-01-01', priority=2 if year >= 2014 else 4)
           for year in range(1982, 2026)]
    batch = coalesce_pending_tasks(seed, old, today=TODAY)
    assert batch and len(batch.tasks) == 45
    assert batch.params()['start_date'] == '1982-01-01'
    assert batch.params()['end_date'] == TODAY.isoformat()
    assert batch.metadata()['calendar_length_limit'] is None
    assert split_batch_rows(batch, [{'date': TODAY.isoformat()}])['2026-01-01']


def test_current_refresh_does_not_drag_already_completed_history_again():
    seed = Task(partition='2026-01-01', priority=0, state='complete')
    assert coalesce_pending_tasks(seed, years(2024, 2025), today=TODAY) is None
