from __future__ import annotations

from argparse import Namespace
import json
from pathlib import Path
from uuid import UUID

from downloader import download_tw_public_data as twpub
from scripts.classify_tw_public_2014_features import research_acceptance


class Response:
    def __init__(self, content: bytes, payload: object | None = None) -> None:
        self.content = content
        self._payload = payload

    def json(self) -> object:
        return self._payload


def _args() -> Namespace:
    return Namespace(mode="daily", refresh=False, timeout=5, verify_ssl=True,
                     retries=0, retry_backoff=0.0)


def test_gcis_catalog_skips_complete_resources_and_keeps_corrections(
    tmp_path: Path, monkeypatch,
) -> None:
    dataset_ids = [str(UUID(int=i + 1)).upper() for i in range(100)]
    resource_id = str(UUID(int=1001)).upper()
    catalog = "<ul>" + "".join(
        f'<li><a href="/od/detail?oid={oid}">資料{i}</a>'
        '<span class="date">2026/09/18</span></li>'
        for i, oid in enumerate(dataset_ids)
    ) + "</ul>"
    detail = (
        f'<table><tr><td>2010年01月</td><td><a onclick="showDialog(\'/od/file?oid={resource_id}\')">'
        "CSV</a></td></tr></table>"
    )
    version = [b"month,value\n201001,1\n"]
    calls: list[str] = []

    def get(url: str, _args: Namespace) -> Response:
        calls.append(url)
        if url == twpub.GCIS_CATALOG_URL:
            return Response(catalog.encode())
        if url == twpub.GCIS_DETAIL_URL.format(oid=dataset_ids[0]):
            return Response(detail.encode())
        if url.startswith("https://data.gcis.nat.gov.tw/od/detail?"):
            return Response(b"<html><body>No CSV</body></html>")
        if url == twpub.GCIS_FILE_URL.format(oid=resource_id):
            return Response(version[0])
        raise AssertionError(url)

    monkeypatch.setattr(twpub, "_catalog_get", get)
    spec = next(s for s in twpub.ADDITIONAL_OFFICIAL_CATALOGS if s.kind == "gcis_catalog")
    first = twpub._download_gcis_catalog(spec, _args(), tmp_path)
    assert first.status == "ok"
    assert first.fetched_dates == 1
    assert len(calls) == 102
    raw_files = list((tmp_path / "raw" / spec.name).rglob("*.csv"))
    assert len(raw_files) == 1 and raw_files[0].read_bytes() == version[0]

    state_path = tmp_path / "state" / f"{spec.name}.json"
    prior_state = state_path.read_bytes()
    prior_inventory = (tmp_path / f"{spec.name}.parquet").read_bytes()
    calls.clear()
    second = twpub._download_gcis_catalog(spec, _args(), tmp_path)
    assert second.status == "up_to_date"
    assert calls == [twpub.GCIS_CATALOG_URL]
    assert state_path.read_bytes() == prior_state
    assert (tmp_path / f"{spec.name}.parquet").read_bytes() == prior_inventory

    state = json.loads(state_path.read_text())
    state["datasets"][dataset_ids[0]]["checked_on"] = "2020-01-01"
    state_path.write_text(json.dumps(state))
    calls.clear()
    aged = twpub._download_gcis_catalog(spec, _args(), tmp_path)
    assert aged.status == "up_to_date"
    assert calls == [twpub.GCIS_CATALOG_URL]

    state = json.loads(state_path.read_text())
    state["datasets"][dataset_ids[0]]["complete"] = False
    state_path.write_text(json.dumps(state))
    calls.clear()
    resumed = twpub._download_gcis_catalog(spec, _args(), tmp_path)
    assert resumed.status == "up_to_date"
    assert calls == [
        twpub.GCIS_CATALOG_URL,
        twpub.GCIS_DETAIL_URL.format(oid=dataset_ids[0]),
    ]

    state = json.loads(state_path.read_text())
    state["datasets"][dataset_ids[0]]["update_marker"] = "2026/09/01"
    state_path.write_text(json.dumps(state))
    version[0] = b"month,value\n201001,2\n"
    third = twpub._download_gcis_catalog(spec, _args(), tmp_path)
    assert third.status == "ok" and third.fetched_dates == 1
    assert {p.read_bytes() for p in (tmp_path / "raw" / spec.name).rglob("*.csv")} == {
        b"month,value\n201001,1\n", b"month,value\n201001,2\n",
    }


def test_fsc_catalog_rechecks_changed_row_counts_without_refetching_unchanged(
    tmp_path: Path, monkeypatch,
) -> None:
    dataset_ids = [str(UUID(int=i + 2001)) for i in range(50)]
    entries = [
        {"id": oid, "identifier": str(i), "name": f"表{i}",
         "row_count": 1 if i == 0 else 0}
        for i, oid in enumerate(dataset_ids)
    ]
    csv_body = [b"year,value\n2025,1\n"]
    calls: list[str] = []

    def get(url: str, _args: Namespace) -> Response:
        calls.append(url)
        if url == twpub.FSC_CATALOG_URL:
            return Response(b"[]", entries)
        if url == twpub.FSC_EXPORT_URL.format(identifier="0"):
            return Response(csv_body[0])
        raise AssertionError(url)

    monkeypatch.setattr(twpub, "_catalog_get", get)
    spec = next(s for s in twpub.ADDITIONAL_OFFICIAL_CATALOGS if s.kind == "fsc_catalog")
    first = twpub._download_fsc_catalog(spec, _args(), tmp_path)
    assert first.status == "ok" and first.fetched_dates == 1
    state_path = tmp_path / "state" / f"{spec.name}.json"
    prior_state = state_path.read_bytes()
    prior_inventory = (tmp_path / f"{spec.name}.parquet").read_bytes()
    calls.clear()
    second = twpub._download_fsc_catalog(spec, _args(), tmp_path)
    assert second.status == "up_to_date"
    assert calls == [twpub.FSC_CATALOG_URL]
    assert state_path.read_bytes() == prior_state
    assert (tmp_path / f"{spec.name}.parquet").read_bytes() == prior_inventory

    entries[0]["row_count"] = 2
    csv_body[0] = b"year,value\n2025,1\n2026,2\n"
    third = twpub._download_fsc_catalog(spec, _args(), tmp_path)
    assert third.status == "ok" and third.fetched_dates == 1
    assert len(list((tmp_path / "raw" / spec.name).rglob("*.csv"))) == 2
    state = json.loads((tmp_path / "state" / f"{spec.name}.json").read_text())
    assert state["datasets"][dataset_ids[0]]["row_count"] == 2


def test_extended_catalogs_do_not_change_opening_source_contract() -> None:
    assert "gcis_open_data_catalog" not in twpub.DEFAULT_DATASETS
    assert "fsc_open_data_catalog" not in twpub.DEFAULT_DATASETS
    assert len(twpub._select_specs(["all-extended"])) == len(twpub.DEFAULT_DATASETS) + 2
    assert {s.kind for s in twpub._select_specs(["gcis", "fsc"])} == {
        "gcis_catalog", "fsc_catalog",
    }


def test_research_policy_accepts_current_values_with_labeled_time_basis() -> None:
    assert research_acceptance("twpub_cbc_m1b_raw", 100, "bulk_no_original_values") == (
        "current_revised_version", "official_announcement_else_inferred", "yes_after_date_mapping",
    )
    assert research_acceptance("twpub_tdcc_retail_holder_ratio", 10, "snapshot_no_old_vintage") == (
        "current_captured_version", "first_observed_at_only", "yes_from_first_observation",
    )
    assert research_acceptance("x", 0, "all_null") == ("unavailable", "unavailable", "no")
