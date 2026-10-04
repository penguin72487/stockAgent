from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
import socket

import pytest

from downloader.artifact_io import sha256_bytes
from scripts import public_catalog_inventory as inventory


def _write_catalog(root, provider, payload, rows, /, **changes):
    storage = root / "data_keyed_public_catalogs"
    encoded = json.dumps(payload).encode()
    digest = sha256_bytes(encoded)
    relative = f"objects/{provider}/{digest}.json"
    object_path = storage / relative
    object_path.parent.mkdir(parents=True, exist_ok=True)
    object_path.write_bytes(encoded)
    receipt = {
        "provider": provider, "dataset": inventory.SPEC_BY_PROVIDER[provider].dataset,
        "status": "acquired", "kind": "metadata_catalog", "history_complete": False,
        "retained_payload_format": "sanitized_json", "object_path": relative,
        "object_sha256": digest, "object_bytes": len(encoded), "rows": rows,
        "observed_at_utc": "2020-01-02T03:04:05+00:00", "catalog_complete": True,
    }
    receipt.update(changes)
    receipt_path = storage / "receipts" / f"{provider}.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    return receipt_path, object_path


def _noaa(**changes):
    return {"id": "GHCND", "name": "Daily summaries", "mindate": "1763-01-01",
            "maxdate": "2020-01-01", **changes}


def _census(**changes):
    return {"c_dataset": ["acs", "acs1"], "title": "2014 ACS one-year dataset",
            "c_vintage": 2014, "c_documentationLink": "https://www.census.gov/developer/",
            "api_key": "PRIVATE_NOT_A_DATASET_FIELD", "contactPoint": {"email": "private@example.org"},
            **changes}


def _issues_for(issues, provider):
    return [row for row in issues if row["provider"] == provider]


def test_expands_only_dataset_catalogs_without_network_or_fabricated_dates(tmp_path, monkeypatch):
    def forbid_network(*args, **kwargs):
        raise AssertionError("unexpected network request")
    monkeypatch.setattr(socket.socket, "connect", forbid_network)
    _write_catalog(tmp_path, "noaa_cdo", {"results": [_noaa()]}, 1)
    _write_catalog(tmp_path, "census", {"dataset": [_census(), _census(c_vintage=2015)]}, 2)
    _write_catalog(tmp_path, "nasa_firms", {"records": [{
        "data_id": "MODIS_NRT", "min_date": "2000-01-01", "max_date": "2020-01-01",
    }]}, 1)
    _write_catalog(tmp_path, "bea", {"BEAAPI": {"Results": {"Dataset": [{
        "DatasetName": "NIPA", "DatasetDescription": "National income and product accounts",
    }]}}}, 1)
    # Neither individual symbols nor weather-station observations are datasets.
    _write_catalog(tmp_path, "finnhub", [{"symbol": "AAPL"}], 1)
    _write_catalog(tmp_path, "cwa", {"records": {"Station": [{"StationId": "0001"}]}}, 1)
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert issues == []
    assert Counter(row["provider"] for row in rows) == {
        "noaa_cdo": 1, "census": 2, "nasa_firms": 1, "bea": 1,
    }
    census = [row for row in rows if row["provider"] == "census"]
    assert {row["year_or_vintage"] for row in census} == {"2014", "2015"}
    assert all(row["advertised_first"] is row["advertised_last"] is None for row in census)
    assert all(row["history_downloaded"] is False for row in rows)
    assert all(row["upstream_cadence"] is None for row in rows)
    assert all(row["capture_at"] == "2020-01-02T03:04:05+00:00" for row in rows)
    assert all("no_observation_history" in row["evidence"] for row in rows)
    assert "PRIVATE_NOT_A_DATASET_FIELD" not in json.dumps(rows)
    assert "private@example.org" not in json.dumps(rows)
    noaa = next(row for row in rows if row["provider"] == "noaa_cdo")
    assert noaa["advertised_first"] == "1763-01-01"
    assert set(noaa) == {
        "provider", "dataset_id", "title", "advertised_first", "advertised_last",
        "year_or_vintage", "upstream_cadence", "documentation", "capture_at",
        "history_downloaded", "evidence",
    }


def test_missing_receipts_are_issues_not_empty_success(tmp_path):
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert rows == []
    assert len(issues) == 4
    assert all(row["code"] == "receipt_missing" for row in issues)


@pytest.mark.parametrize("changes", [
    {"provider": "private-provider-name"}, {"dataset": "private-dataset"},
    {"status": "http_429"}, {"kind": "prospective_snapshot"}, {"history_complete": True},
    {"retained_payload_format": "raw_json"}, {"object_sha256": "private-digest"},
    {"object_path": "../../private-secret-file"}, {"object_path": "/private-secret-file"},
    {"object_bytes": True}, {"object_bytes": 0},
    {"object_bytes": inventory.MAX_OBJECT_BYTES + 1},
    {"observed_at_utc": "2999-01-01T00:00:00+00:00"},
    {"observed_at_utc": "2020-01-01"}, {"rows": True}, {"rows": 2},
])
def test_rejects_receipt_contract_and_paths_without_echoing_inputs(tmp_path, changes):
    _write_catalog(tmp_path, "noaa_cdo", {"results": [_noaa()]}, 1, **changes)
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert rows == []
    assert len(_issues_for(issues, "noaa_cdo")) == 1
    assert "private" not in json.dumps(issues)


def test_object_actual_hash_is_verified_even_at_same_size(tmp_path):
    _, object_path = _write_catalog(tmp_path, "noaa_cdo", {"results": [_noaa()]}, 1)
    encoded = object_path.read_bytes().replace(b"Daily summaries", b"Other summaries")
    assert len(encoded) == object_path.stat().st_size
    object_path.write_bytes(encoded)
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert rows == []
    assert _issues_for(issues, "noaa_cdo")[0]["code"] == "object_digest_mismatch"


def test_object_size_mismatch_is_not_a_hash_proof(tmp_path):
    receipt_path, _ = _write_catalog(tmp_path, "noaa_cdo", {"results": [_noaa()]}, 1)
    receipt = json.loads(receipt_path.read_text())
    receipt["object_bytes"] += 1
    receipt_path.write_text(json.dumps(receipt))
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert rows == []
    assert _issues_for(issues, "noaa_cdo")[0]["code"] == "object_size_mismatch"


@pytest.mark.parametrize("target", ["receipt", "object"])
def test_symlink_cannot_read_outside_the_dataset_root(tmp_path, target):
    receipt_path, object_path = _write_catalog(tmp_path, "noaa_cdo", {"results": [_noaa()]}, 1)
    source = receipt_path if target == "receipt" else object_path
    private = tmp_path / "private-secret-file"
    private.write_bytes(source.read_bytes())
    source.unlink()
    source.symlink_to(private)
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert rows == []
    assert _issues_for(issues, "noaa_cdo")[0]["code"] == "path_outside_storage"
    assert "private" not in json.dumps(issues)


def test_object_missing_keeps_an_issue(tmp_path):
    _, object_path = _write_catalog(tmp_path, "noaa_cdo", {"results": [_noaa()]}, 1)
    object_path.unlink()
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert rows == []
    assert _issues_for(issues, "noaa_cdo")[0]["code"] == "object_missing"


def test_all_reads_are_bounded(tmp_path, monkeypatch):
    _write_catalog(tmp_path, "noaa_cdo", {"results": [_noaa()]}, 1)
    monkeypatch.setattr(inventory, "MAX_RECEIPT_BYTES", 10)
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert rows == []
    assert _issues_for(issues, "noaa_cdo")[0]["code"] == "invalid_file_size_or_type"


def test_receipt_symlink_loop_becomes_an_issue_without_raw_path(tmp_path):
    directory = tmp_path / "data_keyed_public_catalogs/receipts"
    directory.mkdir(parents=True)
    path = directory / "noaa_cdo.json"
    path.symlink_to(path.name)
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert rows == []
    assert _issues_for(issues, "noaa_cdo")[0]["code"] == "local_evidence_unreadable"


def test_invalid_receipt_json_is_not_echoed(tmp_path):
    directory = tmp_path / "data_keyed_public_catalogs/receipts"
    directory.mkdir(parents=True)
    (directory / "noaa_cdo.json").write_bytes(b'{"PRIVATE_CREDENTIAL": "invalid')
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert rows == []
    assert _issues_for(issues, "noaa_cdo")[0]["code"] == "invalid_json"
    assert "PRIVATE_CREDENTIAL" not in json.dumps(issues)


@pytest.mark.parametrize("payload", [None, [], "private-corrupt-json", {"results": None}, {"results": []}])
def test_malformed_payloads_never_escape_or_count_complete(tmp_path, payload):
    _write_catalog(tmp_path, "noaa_cdo", payload, 0)
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert rows == []
    assert len(_issues_for(issues, "noaa_cdo")) == 1
    assert "private" not in json.dumps(issues)


@pytest.mark.parametrize("bad", [
    None, _noaa(id=None), _noaa(name=""), _noaa(mindate="private-bad-date"),
    _noaa(mindate="2020-02-30"), _noaa(maxdate="1700-01-01"),
])
def test_bad_records_report_index_and_do_not_hide_usable_records(tmp_path, bad):
    _write_catalog(tmp_path, "noaa_cdo", {"results": [_noaa(), bad]}, 2)
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert len(rows) == 1
    assert _issues_for(issues, "noaa_cdo")[0]["row_index"] == 1
    assert "private" not in json.dumps(issues)


def test_duplicate_dataset_identities_are_not_silently_overwritten(tmp_path):
    _write_catalog(tmp_path, "noaa_cdo", {"results": [_noaa(), _noaa(name="Conflicting title")]}, 2)
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert rows == []
    duplicate_issues = _issues_for(issues, "noaa_cdo")
    assert [row["row_index"] for row in duplicate_issues] == [0, 1]
    assert all(row["code"] == "duplicate_dataset_identity" for row in duplicate_issues)


@pytest.mark.parametrize("changes", [
    {"c_vintage": True}, {"c_vintage": "2014-01-01"}, {"c_vintage": "private-vintage"},
    {"c_dataset": "private-dataset"}, {"c_dataset": ["../secret"]},
    {"c_documentationLink": "https://www.census.gov/data?key=PRIVATE_TOKEN"},
    {"c_documentationLink": "https://user:PRIVATE_TOKEN@www.census.gov/data"},
    {"c_documentationLink": "https://census.gov.invalid/PRIVATE_TOKEN"},
])
def test_census_rejects_bad_vintages_and_unsafe_documentation(tmp_path, changes):
    _write_catalog(tmp_path, "census", {"dataset": [_census(**changes)]}, 1)
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert rows == []
    assert len(_issues_for(issues, "census")) == 1
    assert "PRIVATE_TOKEN" not in json.dumps(issues)
    assert "private" not in json.dumps(issues)


def test_census_timeseries_without_vintage_does_not_invent_a_year(tmp_path):
    _write_catalog(tmp_path, "census", {"dataset": [_census(c_vintage=None)]}, 1)
    rows, _ = inventory.catalog_inventory(tmp_path)
    assert rows[0]["year_or_vintage"] is None
    assert rows[0]["advertised_first"] is rows[0]["advertised_last"] is None


def test_partial_listing_keeps_valid_rows_but_reports_incompleteness(tmp_path):
    _write_catalog(tmp_path, "noaa_cdo", {"results": [_noaa()]}, 1, catalog_complete=False)
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert len(rows) == 1
    assert rows[0]["history_downloaded"] is False
    assert _issues_for(issues, "noaa_cdo") == [{"provider": "noaa_cdo", "code": "catalog_listing_incomplete"}]


def test_fresh_failure_summary_does_not_erase_successful_catalog_inventory(tmp_path):
    _write_catalog(tmp_path, "noaa_cdo", {"results": [_noaa()]}, 1)
    (tmp_path / "data_keyed_public_catalogs/download_summary.json").write_text(json.dumps({
        "providers": [{"provider": "noaa_cdo", "status": "http_429"}],
    }))
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert len(rows) == 1
    assert _issues_for(issues, "noaa_cdo") == []


def test_receipt_replacement_during_read_invalidates_the_join(tmp_path, monkeypatch):
    receipt_path, object_path = _write_catalog(tmp_path, "noaa_cdo", {"results": [_noaa()]}, 1)
    original = inventory._read_bounded
    def replace_after_object(path, storage, maximum):
        data = original(path, storage, maximum)
        if path == object_path:
            receipt_path.write_text(json.dumps({"private": "do-not-echo"}))
        return data
    monkeypatch.setattr(inventory, "_read_bounded", replace_after_object)
    rows, issues = inventory.catalog_inventory(tmp_path)
    assert rows == []
    assert _issues_for(issues, "noaa_cdo")[0]["code"] == "receipt_changed"
    assert "do-not-echo" not in json.dumps(issues)
