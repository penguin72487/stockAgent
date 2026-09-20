from __future__ import annotations

from datetime import UTC, date, datetime
import hashlib
import io
import json
from pathlib import Path
import zipfile

import pyarrow.parquet as pq
import pytest

from downloader.download_tw_mops_xbrl import (
    _authorization, _download_one, _ingest, _selected_work, _work_queue,
    discover_archives, parse_document, summarize, validate_zip,
)
from stockagent.live.data_monitor_dashboard import _enrich_and_sort_rows, _tw_public_sources
from stockagent.live.data_monitor_inventory import build_record_inventory


def _link(name: str, folder: str) -> str:
    return ("window.open('/server-java/FileDownLoad?step=9&functionName=show_file2"
            f"&fileName={name}&filePath=/{folder}/')")


def _xbrl() -> bytes:
    return (b'<xbrl xmlns="http://www.xbrl.org/2003/instance" xmlns:tw="urn:tw">'
            b'<context id="c1"><entity><identifier>2330</identifier></entity>'
            b'<period><instant>2024-12-31</instant></period></context>'
            b'<unit id="u1"><measure>iso4217:TWD</measure></unit>'
            b'<tw:Assets contextRef="c1" unitRef="u1" decimals="0">1,234</tw:Assets>'
            b'</xbrl>')


def test_saved_html4_inline_xbrl_retains_context_and_numeric_value() -> None:
    payload = (
        b'\xef\xbb\xbf<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN">'
        b'<HTML xmlns:ix="http://www.xbrl.org/2013/inlineXBRL" '
        b'xmlns:xbrli="http://www.xbrl.org/2003/instance"><HEAD><META charset="utf-8">'
        b'</HEAD><BODY>'
        b'<xbrli:context id="c1"><xbrli:entity><xbrli:identifier>2330'
        b'</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:instant>'
        b'2021-06-30</xbrli:instant></xbrli:period></xbrli:context>'
        b'<ix:nonFraction contextRef="c1" name="ifrs-full:Cash" unitRef="TWD" '
        b'scale="3" format="ixt:numdotdecimal">305,449</ix:nonFraction>'
        b'</BODY></HTML>'
    )
    rows = parse_document(payload, archive_name="tifrs-2021Q2.zip",
                          archive_sha256="archive", member_name="report.html",
                          observed_at="2026-09-17T00:00:00Z")
    assert len(rows) == 1
    assert rows[0]["document_sha256"] == hashlib.sha256(payload).hexdigest()
    assert rows[0]["entity_identifier"] == "2330"
    assert rows[0]["period_instant"] == "2021-06-30"
    assert rows[0]["decimal_value"] == "305449000"


def test_discovery_uses_exact_advertised_links_and_excludes_future() -> None:
    page = "案例文件整批下載" + _link("tw-gaap-2009Q4.zip", "xbrl/2009") + _link(
        "tifrs-2024Q4.zip", "ifrs/2024") + _link("tifrs-2026Q4.zip", "ifrs/2026")
    links = discover_archives(page, today=date(2026, 9, 17))
    assert [(link.standard, link.period) for link in links] == [
        ("tw_gaap", "2009Q4"), ("ifrs", "2024Q4")]
    with pytest.raises(ValueError, match="unexpected parameters"):
        discover_archives("案例文件整批下載" + _link("tifrs-2024Q4.zip", "ifrs/2024")
                          .replace("&filePath", "&evil=1&filePath"), today=date(2026, 9, 17))


def test_queue_prioritizes_recent_and_rotates_unavailable_old_gap() -> None:
    page = "案例文件整批下載" + "".join(
        _link(f"tifrs-{year}Q4.zip", f"ifrs/{year}") for year in (2021, 2022, 2023, 2024)
    )
    links = discover_archives(page, today=date(2026, 9, 17))
    missing = {f"ifrs:{year}Q4" for year in (2021, 2022, 2023, 2024)}
    attempts = {"ifrs:2021Q4": {"at_utc": "2026-09-17T01:00:00+00:00",
                                 "outcome": "source_unavailable"}}
    queue = _work_queue(links, missing, attempts, 1)
    assert [link.period for link in queue] == ["2024Q4", "2022Q4", "2023Q4", "2021Q4"]


def test_one_shot_backfill_selects_all_missing_once() -> None:
    page = "案例文件整批下載" + "".join(
        _link(f"tifrs-{year}Q4.zip", f"ifrs/{year}") for year in (2021, 2022, 2023, 2024)
    )
    links = discover_archives(page, today=date(2026, 9, 17))
    missing = {f"ifrs:{year}Q4" for year in (2021, 2022, 2023)}
    bounded = _selected_work(
        links, missing, {}, recheck_recent_quarters=1,
        max_archives=2, backfill_all=False,
    )
    all_missing = _selected_work(
        links, missing, {}, recheck_recent_quarters=1,
        max_archives=2, backfill_all=True,
    )
    assert [link.period for link in bounded] == ["2024Q4", "2021Q4"]
    assert [link.period for link in all_missing] == [
        "2024Q4", "2021Q4", "2022Q4", "2023Q4",
    ]
    assert len({(link.standard, link.period) for link in all_missing}) == len(all_missing)


def test_nested_archive_ingest_preserves_raw_and_never_claims_pit(tmp_path: Path) -> None:
    nested = io.BytesIO()
    with zipfile.ZipFile(nested, "w") as archive:
        archive.writestr("2330.xml", _xbrl())
    path = tmp_path / "tifrs-2024Q4.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("company-2330.zip", nested.getvalue())
    output = tmp_path / "output"
    first = _ingest(path, root=output, source_url=None)
    second = _ingest(path, root=output, source_url=None)
    assert first["archive_sha256"] == second["archive_sha256"]
    assert first["first_observed_at_utc"] == second["first_observed_at_utc"]
    assert first["fact_count"] == 1
    assert first["historical_point_in_time"] is False
    assert first["source_authenticity_verified"] is False
    row = pq.read_table(first["facts_path"]).to_pylist()[0]
    assert row["concept"] == "{urn:tw}Assets"
    assert row["decimal_value"] is None  # XML non-inline commas are not a valid xbrl numeric lexical value.
    assert row["value_parse_status"] == "not_plain_decimal"
    assert row["period_instant"] == "2024-12-31"
    assert row["report_period_end"] == "2024-12-31"
    assert row["published_at_utc"] is None
    links = discover_archives(
        "案例文件整批下載" + _link("tifrs-2024Q4.zip", "ifrs/2024"),
        today=date(2026, 9, 17),
    )
    state = summarize(links, output, authorized=False)
    assert state["local_imported_periods"] == 1
    assert state["local_fact_rows"] == 1
    assert state["local_import_complete"] is True
    assert state["completed_periods"] == 0
    assert state["fact_rows"] == 0


def test_verified_http_zip_counts_as_covered_without_moving_user_file(tmp_path: Path) -> None:
    link = discover_archives(
        "案例文件整批下載" + _link("tifrs-2024Q4.zip", "ifrs/2024"),
        today=date(2026, 9, 17),
    )[0]
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("2330.xml", _xbrl())

    class Response:
        status_code = 200
        headers = {"Content-Type": "application/octet-stream",
                   "Content-Disposition": 'attachment; filename="tifrs-2024Q4.zip"'}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size):
            yield payload.getvalue()

    class Session:
        def get(self, url, **_kwargs):
            assert url == link.url
            return Response()

    class Limiter:
        def wait(self):
            return None

    root = tmp_path / "out"
    root.mkdir()
    receipt = _download_one(Session(), Limiter(), link, root)
    assert receipt["source_authenticity_verified"] is True
    assert Path(receipt["raw_path"]).is_file()
    state = summarize([link], root, authorized=True)
    assert state["completed_periods"] == 1
    assert state["fact_rows"] == 1


def test_zip_traversal_and_xml_dtd_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "tifrs-2024Q4.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("../escape.xml", _xbrl())
    with pytest.raises(ValueError, match="unsafe"):
        validate_zip(path)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("company.xml", b'<!DOCTYPE x [<!ENTITY e "bad">]><x>&e;</x>')
    with pytest.raises(Exception):
        _ingest(path, root=tmp_path / "out", source_url=None)
    assert not list((tmp_path / "out").glob("normalized/**/facts.parquet"))


def test_declared_big5_xbrl_decodes_without_changing_raw_hash() -> None:
    payload = (b'<?xml version="1.0" encoding="BIG5"?>' +
               _xbrl().replace(b'</xbrl>',
                               '<tw:Note contextRef="c1">中文</tw:Note></xbrl>'.encode('big5')))
    rows = parse_document(payload, archive_name="tw-gaap-2009Q4.zip",
                          archive_sha256="0" * 64, member_name="2330.xml",
                          observed_at="2026-09-17T00:00:00+00:00")
    note = next(row for row in rows if row["concept"] == "{urn:tw}Note")
    assert note["raw_value"] == "中文"
    assert note["document_sha256"] == hashlib.sha256(payload).hexdigest()
    malicious = (b'<?xml version="1.0" encoding="BIG5"?>'
                 b'<!DOCTYPE x [<!ENTITY e "bad">]><x>&e;</x>')
    with pytest.raises(Exception):
        parse_document(malicious, archive_name="tw-gaap-2009Q4.zip",
                       archive_sha256="0" * 64, member_name="bad.xml",
                       observed_at="2026-09-17T00:00:00+00:00")


def test_undeclared_xml_prefix_keeps_lexical_name_and_records_recovery() -> None:
    payload = (b'<?xml version="1.0" encoding="BIG5"?>' +
               _xbrl().replace(
                   b'</xbrl>',
                   b'<tw-gaap-ci:NetAssets contextRef="c1" unitRef="u1">12'
                   b'</tw-gaap-ci:NetAssets></xbrl>',
               ))
    recovery: list[dict[str, object]] = []
    rows = parse_document(payload, archive_name="tw-gaap-2009Q4.zip",
                          archive_sha256="archive", member_name="report.xml",
                          observed_at="2026-09-17T00:00:00Z",
                          recovery_events=recovery)
    fact = next(row for row in rows if row["concept"] == "tw-gaap-ci:NetAssets")
    assert fact["raw_value"] == "12"
    assert fact["document_sha256"] == hashlib.sha256(payload).hexdigest()
    assert recovery == [{"member": "report.xml", "type": "undeclared_xml_prefix",
                         "prefixes": ["tw-gaap-ci"]}]


def test_authorization_requires_evidence_and_coverage_reports_missing(tmp_path: Path) -> None:
    with pytest.raises(PermissionError):
        _authorization(None)
    attestation = tmp_path / "authorization.json"
    attestation.write_text(json.dumps({"provider": "TWSE", "authorized": True,
                                       "scope": "automated_mops_xbrl_bulk_download",
                                       "evidence_reference": "", "expires_on": "2099-01-01"}))
    with pytest.raises(PermissionError):
        _authorization(attestation)
    links = discover_archives("案例文件整批下載" + _link("tifrs-2024Q4.zip", "ifrs/2024"),
                              today=date(2026, 9, 17))
    state = summarize(links, tmp_path / "out", authorized=False)
    assert state["discovered_periods"] == 1
    assert state["completed_periods"] == 0
    assert state["missing_periods"] == ["ifrs:2024Q4"]
    assert state["training_eligible"] is False


def test_xbrl_is_separate_monitor_row_without_claiming_training_readiness(tmp_path: Path) -> None:
    rows = _tw_public_sources(tmp_path, now=datetime.now(UTC))
    row = next(item for item in rows if item["id"] == "tw-public:mops_xbrl_quarterly")
    assert row["status"] == "blocked"
    assert row["publishable"] is False
    assert row["coverage"] is None
    stored = build_record_inventory(tmp_path, refresh=True)["datasets"]
    assert "tw-public:mops_xbrl_quarterly" in stored
    assert stored["tw-public:mops_xbrl_quarterly"]["count"] is None


def test_xbrl_monitor_shows_local_import_without_claiming_verified_source(tmp_path: Path) -> None:
    state_path = tmp_path / "data_tw_public/mops_xbrl/state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(json.dumps({
        "status": "blocked_authorization", "automated_download_authorized": False,
        "discovered_periods": 71, "completed_periods": 0, "fact_rows": 0,
        "local_imported_periods": 71, "local_fact_rows": 123456,
    }))
    row = next(item for item in _tw_public_sources(tmp_path, now=datetime.now(UTC))
               if item["id"] == "tw-public:mops_xbrl_quarterly")
    assert row["status"] == "blocked"
    assert row["coverage"]["current"] == 71
    assert row["coverage"]["label"] == "本機季度 ZIP 正規化匯入"
    assert row["rows"] == 123456
    assert "來源與申報時刻待驗證" in row["status_label"]
    assert row["publishable"] is False


def test_xbrl_timer_requires_current_authorization_and_active_service(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    state_path = tmp_path / "data_tw_public/mops_xbrl/state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(json.dumps({
        "generated_at_utc": now.isoformat(), "status": "incomplete",
        "automated_download_authorized": True,
        "authorization_expires_on": "2099-01-01",
        "discovered_periods": 71, "completed_periods": 3, "fact_rows": 10,
    }))
    row = next(item for item in _tw_public_sources(tmp_path, now=now)
               if item["id"] == "tw-public:mops_xbrl_quarterly")
    disabled = _enrich_and_sort_rows([row], now=now, refresh_services={})[0]
    enabled = _enrich_and_sort_rows([row], now=now, refresh_services={
        "tw_mops_xbrl": {"timer_active": True, "active": False}
    })[0]
    assert disabled["automation"]["automatic_update"] is False
    assert enabled["automation"]["automatic_update"] is True
    assert enabled["operation_state"] == "catching_up"
