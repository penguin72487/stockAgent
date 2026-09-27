import csv
from datetime import UTC, date, datetime
import json
from pathlib import Path
import sqlite3

import pytest
import requests

from downloader import download_finmind_complement as complement
from downloader import download_finmind_free as free
from downloader import download_finmind_sponsor as sponsor
from downloader import finmind_batching as batching
from downloader.finmind_scheduling import PRODUCT_HISTORY_STARTS
from scripts import audit_finmind_query_ranges as audit


NOW = datetime(2026, 9, 27, 3, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def prohibit_network_and_production_queue_helpers(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("Inventory must not request data or initialize a queue")

    monkeypatch.setattr(requests.Session, "request", forbidden)
    monkeypatch.setattr(sponsor, "_db", forbidden)
    monkeypatch.setattr(complement, "_db", forbidden)


def expected_datasets():
    return (
        {source.dataset for source in sponsor.SOURCES}
        | set(complement.ALL_DATASETS)
        | set(free.SESSION_DATASETS)
        | {free.CALENDAR_DATASET, free.MASTER_DATASET}
        | set(sponsor.UNSCHEDULED)
        | {"TaiwanStockNews"}
    )


def dataset_row(report, dataset):
    matches = [row for row in report["datasets"] if row["dataset"] == dataset]
    assert len(matches) == 1
    return matches[0]


def create_queue(root, owner, rows):
    path = root / owner / "queue.sqlite3"
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE tasks (dataset TEXT, data_id TEXT, partition TEXT, "
            "kind TEXT, priority INTEGER, state TEXT, next_attempt_at_utc TEXT, "
            "first_data_date TEXT, last_data_date TEXT, rows INTEGER)"
        )
        connection.executemany(
            "INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
        )
    return path


def task(dataset, partition, state, *, data_id="", kind="year", priority=2, retry=None, rows=0):
    return (
        dataset, data_id, partition, kind, priority, state, retry,
        partition if rows else None, partition if rows else None, rows,
    )


def test_registry_covers_live_source_union_once_and_retains_owner_contracts():
    registry = audit.registry()
    assert set(registry) == expected_datasets()
    shared = registry["TaiwanStockCapitalReductionReferencePrice"]
    assert {"sponsor", "complement"} <= set(shared["owners"])
    assert {"sponsor", "complement"} <= set(shared["owner_contracts"])
    assert shared["primary_owner"] == "sponsor"
    assert registry["TaiwanStockNews"]["disabled"] is True
    assert registry["TaiwanStockNews"]["owners"] == ["disabled_policy"]


def test_missing_queues_are_unknown_and_inventory_has_no_side_effects(tmp_path):
    report = audit.build_inventory(tmp_path, now=NOW)

    assert report["coverage"]["unique_dataset_count"] == len(expected_datasets())
    current_alias_count = (
        len(sponsor.SOURCES) + len(complement.ALL_DATASETS)
        + len(free.SESSION_DATASETS) + 2 + len(sponsor.UNSCHEDULED)
    )
    migrated_alias_count = len(set(PRODUCT_HISTORY_STARTS) & set(complement.ALL_DATASETS))
    assert report["coverage"]["current_catalog_alias_count"] == current_alias_count
    assert report["coverage"]["product_history_migration_alias_count"] == migrated_alias_count
    assert report["coverage"]["legacy_catalog_alias_count"] == current_alias_count - migrated_alias_count
    assert len(report["datasets"]) == len(expected_datasets())
    assert report["api_requests"] == 0
    assert report["production_queue_writes"] == 0
    assert report["global_minimum_requests"] is None
    row = dataset_row(report, "TaiwanBusinessIndicator")
    assert row["owner_observations"]["sponsor"]["state_counts"] is None
    assert all(row[state] is None for state in (
        "pending", "complete", "observed_empty", "failed"
    ))
    assert not list(tmp_path.iterdir())


def test_queue_counts_are_partition_observations_not_http_request_claims(tmp_path):
    dataset = "TaiwanBusinessIndicator"
    queue = create_queue(tmp_path, "sponsor", [
        task(dataset, "1982-01-01", "pending"),
        task(dataset, "1983-01-01", "pending", retry="2027-01-01T00:00:00+00:00"),
        task(dataset, "1984-01-01", "complete", rows=12),
        task(dataset, "1985-01-01", "observed_empty"),
        task(dataset, "1986-01-01", "failed"),
        task(dataset, "1987-01-01", "running"),
        task(dataset, "2026-01-01", "complete", priority=0, rows=8,
             retry="2026-09-26T00:00:00+00:00"),
    ])
    before = queue.read_bytes()
    report = audit.build_inventory(tmp_path, now=NOW)

    assert queue.read_bytes() == before
    row = dataset_row(report, dataset)
    assert {state: row[state] for state in (
        "pending", "complete", "observed_empty", "failed"
    )} == {"pending": 2, "complete": 2, "observed_empty": 1, "failed": 1}
    assert row["owner_observations"]["sponsor"]["state_counts"]["running"] == 1
    assert row["owner_observations"]["sponsor"]["pending_due"] == 1
    unseeded = dataset_row(report, "CnnFearGreedIndex")
    assert unseeded["owner_observations"]["sponsor"]["state_counts"] is None
    assert unseeded["pending"] is None
    assert report["api_requests"] == 0
    assert report["production_queue_writes"] == 0
    assert report["global_minimum_requests"] is None


def test_duplicate_owners_preserve_separate_counts_without_summing(tmp_path):
    dataset = "TaiwanStockCapitalReductionReferencePrice"
    sponsor_queue = create_queue(tmp_path, "sponsor", [
        task(dataset, "2011-01-01", "pending"),
        task(dataset, "2012-01-01", "complete", rows=3),
    ])
    complement_queue = create_queue(tmp_path, "complement", [
        task(dataset, "2011-01-01", "pending", data_id="2330"),
        task(dataset, "2011-01-01", "pending", data_id="2317"),
    ])
    before = {path: path.read_bytes() for path in (sponsor_queue, complement_queue)}
    report = audit.build_inventory(tmp_path, now=NOW)
    row = dataset_row(report, dataset)

    assert row["primary_owner"] == "sponsor"
    assert row["pending"] == 1
    assert row["complete"] == 1
    assert row["owner_observations"]["sponsor"]["state_counts"]["pending"] == 1
    assert row["owner_observations"]["complement"]["state_counts"]["pending"] == 2
    assert all(path.read_bytes() == content for path, content in before.items())


@pytest.mark.parametrize("dataset", sorted(PRODUCT_HISTORY_STARTS))
def test_settlement_registry_uses_shared_product_history_contract(dataset):
    row = audit.registry()[dataset]

    assert row["primary_owner"] == "complement"
    assert row["query_shape"] == "per_product_history"
    assert row["configured_first_date"] == PRODUCT_HISTORY_STARTS[dataset].isoformat()
    current = row["owner_contracts"]["complement"]
    assert current["query_shape"] == "per_product_history"
    assert current["configured_first_date"] == PRODUCT_HISTORY_STARTS[dataset].isoformat()
    assert current["historical_universe_verified_complete"] is False
    legacy = row["owner_contracts"]["sponsor"]
    assert legacy["legacy_alias"] is True
    assert legacy["delegated_to_owner"] == "complement"
    assert legacy["query_shape"] == "deprecated_no_id_query_shape"
    assert legacy["excluded_queue_state"] == "deprecated_query_shape"


@pytest.mark.parametrize("dataset", sorted(PRODUCT_HISTORY_STARTS))
def test_settlement_counts_follow_complement_preserving_deprecated_sponsor_evidence(tmp_path, dataset):
    sponsor_queue = create_queue(tmp_path, "sponsor", [
        task(dataset, "2014-01-01", "deprecated_query_shape"),
        task(dataset, "2015-01-01", "deprecated_query_shape"),
    ])
    product_ids = ("TX", "MTX") if dataset.startswith("TaiwanFutures") else ("TXO", "TEO")
    complement_queue = create_queue(tmp_path, "complement", [
        task(dataset, "history", "pending", data_id=data_id, kind="id_history")
        for data_id in product_ids
    ])
    before = {path: path.read_bytes() for path in (sponsor_queue, complement_queue)}
    report = audit.build_inventory(tmp_path, now=NOW)
    row = dataset_row(report, dataset)

    assert row["primary_owner"] == "complement"
    assert row["query_shape"] == "per_product_history"
    assert row["pending"] == len(product_ids)
    assert row["complete"] == row["observed_empty"] == row["failed"] == 0
    legacy = row["owner_observations"]["sponsor"]
    assert legacy["state_counts"] == {"deprecated_query_shape": 2}
    assert legacy["pending"] == legacy["pending_due"] == 0
    assert legacy["delegated_to_complement_product_history"] is True
    assert row["owner_observations"]["complement"]["state_counts"] == {"pending": len(product_ids)}
    assert row["remaining_lower_bound_requests"] is None
    assert row["lowerbound_basis"] == "historical_identifier_universe_not_proven_complete"
    assert report["api_requests"] == report["production_queue_writes"] == 0
    assert all(path.read_bytes() == content for path, content in before.items())


def test_inventory_rechecks_current_range_contracts(monkeypatch, tmp_path):
    existing = "TaiwanBusinessIndicator"
    candidate = "TaiwanStockDayTradingSuspension"
    monkeypatch.delitem(batching.RANGE_CONTRACTS, existing)
    monkeypatch.setitem(batching.RANGE_CONTRACTS, candidate, batching.RangeContract(
        "year", date(2014, 6, 1), None, 1_000_000,
        "https://finmind.github.io/tutor/TaiwanMarket/Technical/",
    ))

    registry = audit.registry()
    assert registry[existing]["owner_contracts"]["sponsor"]["query_shape"] == (
        "unverified_whole_market_date_range"
    )
    contract = registry[candidate]["owner_contracts"]["sponsor"]
    assert contract["query_shape"] == "whole_market_date_range"
    assert contract["max_span"] == "all_due_contiguous_pending_periods"
    report = audit.build_inventory(tmp_path, now=NOW)
    assert dataset_row(report, candidate)["owner_contracts"]["sponsor"] == contract


def test_written_json_and_csv_preserve_unknown_counts(tmp_path):
    root = tmp_path / "empty_finmind"
    root.mkdir()
    report = audit.build_inventory(root, now=NOW)
    paths = audit.write_inventory(report, tmp_path / "inventory")

    saved = json.loads(Path(paths["json"]).read_text(encoding="utf-8"))
    assert saved == report
    with Path(paths["csv"]).open(newline="", encoding="utf-8-sig") as stream:
        rows = list(csv.DictReader(stream))
    assert {row["dataset"] for row in rows} == expected_datasets()
    assert len(rows) == len(expected_datasets())
    row = next(row for row in rows if row["dataset"] == "TaiwanBusinessIndicator")
    assert all(row[state] == "" for state in (
        "pending", "complete", "observed_empty", "failed"
    ))
    assert saved["api_requests"] == 0
    assert saved["production_queue_writes"] == 0
    assert not list(root.iterdir())


def test_complement_year_ranges_are_bounded_by_due_interval_not_open_ended():
    for dataset in complement.BULK_GLOBAL_HISTORY:
        contract = audit.registry()[dataset]['owner_contracts']['complement']
        assert contract['max_span'] == 'all_due_contiguous_periods'
        assert contract['open_ended_history_request'] is False
        assert contract['calendar_partition_limit'] is None
        assert contract['decoded_response_byte_limit'] == complement.BULK_MAX_RESPONSE_BYTES


def test_inventory_keeps_primary_rows_and_learned_resource_limits(tmp_path):
    dataset = 'TaiwanStockParValueChange'
    path = create_queue(tmp_path, 'complement', [task(dataset, '2020', 'complete', rows=4)])
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE complement_year_batch_policy(dataset,max_partitions)')
        conn.execute('INSERT INTO complement_year_batch_policy VALUES (?,?)', (dataset, 12))
    report = audit.build_inventory(tmp_path, now=NOW)
    assert dataset_row(report, dataset)['observed_rows'] == 4
    assert report['queue_observations']['complement']['learned_year_batch_limits'] == {dataset: 12}
