from pathlib import Path

import pytest

from scripts.audit_shioaji_tw_minute_dataset import _partition_path


@pytest.mark.parametrize('allow,ready,symbols,accepted', [
    (False, False, ['2330'], False), (True, False, ['2330'], True),
    (True, True, ['2330'], False), (True, False, [], False),
    (True, False, ['2330', '2330'], False),
])
def test_repair_subset_requires_explicit_scope_and_never_becomes_full_ready(allow, ready, symbols, accepted):
    from scripts.audit_shioaji_tw_minute_dataset import _audit_manifest_status, SCHEMA_VERSION
    manifest = {'schema_version': SCHEMA_VERSION, 'status': 'research_subset',
                'research_ready': ready, 'symbols': symbols}
    if accepted:
        assert _audit_manifest_status(manifest, allow_subset=allow) == 'research_subset'
    else:
        with pytest.raises(RuntimeError, match='explicitly permitted'):
            _audit_manifest_status(manifest, allow_subset=allow)


def test_partition_is_owned_by_selected_release_not_working_directory(
    tmp_path, monkeypatch
):
    release = tmp_path / "release"
    partition = release / "trade_date=2026-09-10" / "data.parquet"
    partition.parent.mkdir(parents=True)
    partition.write_bytes(b"release content")
    producer = tmp_path / "producer"
    other = producer / "trade_date=2026-09-10" / "data.parquet"
    other.parent.mkdir(parents=True)
    other.write_bytes(b"different mutable content")
    monkeypatch.chdir(producer)
    assert _partition_path(release, "2026-09-10") == partition
    with pytest.raises(RuntimeError, match="missing from selected release"):
        _partition_path(release, "2026-09-09")


def test_partition_cannot_escape_selected_release_by_symlink(tmp_path):
    release = tmp_path / "release"
    release.mkdir()
    external = tmp_path / "outside.parquet"
    external.write_bytes(b"outside")
    partition = release / "trade_date=2026-09-10" / "data.parquet"
    partition.parent.mkdir()
    partition.symlink_to(external)
    with pytest.raises(RuntimeError, match="escapes selected release"):
        _partition_path(release, "2026-09-10")


@pytest.mark.parametrize(
    "date_text", ["../outside", "20260910", "2026-W37-4", "2026-02-30"]
)
def test_partition_requires_canonical_date(tmp_path: Path, date_text):
    with pytest.raises((ValueError, RuntimeError)):
        _partition_path(tmp_path, date_text)


@pytest.mark.parametrize(
    "defect",
    [
        None,
        "missing_session",
        "manifest_dates",
        "extra_non_session",
        "undeclared_partition",
    ],
)
def test_full_audit_records_scope_calendar_and_exact_release(tmp_path, monkeypatch, defect):
    from datetime import date
    import hashlib
    import json
    from types import SimpleNamespace
    from scripts import audit_shioaji_tw_minute_dataset as audit
    from downloader import download_tw_public_data as public

    dates = ["2026-09-10"]
    if defect == "extra_non_session":
        dates.insert(0, "2026-09-05")
    partitions = []
    for day in dates:
        path = tmp_path / f"trade_date={day}" / "data.parquet"
        path.parent.mkdir()
        path.write_bytes(b"test fixture")
        partitions.append({"trade_date": day, "rows": 1, "output": "/untrusted/producer.parquet",
                           "output_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    manifest = {"schema_version": audit.SCHEMA_VERSION, "status": "research_ready",
                "research_ready": True, "partitions": partitions,
                "dates": dates if defect != "manifest_dates" else ["2026-09-01", "2026-09-10"]}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    if defect == "undeclared_partition":
        (tmp_path / "trade_date=2026-09-08").mkdir()
    output = tmp_path / "result.json"
    monkeypatch.setattr(audit, "parse_args", lambda: SimpleNamespace(all_partitions=True,
        dataset_root=tmp_path, calendar_root=tmp_path / "calendar", output=output))
    monkeypatch.setattr(audit.pl, "read_parquet", lambda _: None)
    monkeypatch.setattr(audit, "audit_frame", lambda *args, **kwargs: {
        "rows": 1, "symbols": 1, "feature_valid_rows": 0, "label_valid_rows": 0,
        "positive_volume_rows": 0, "volume_multiplier_1_rows": 0,
        "volume_multiplier_10_rows": 0, "volume_multiplier_100_rows": 0,
        "volume_multiplier_1000_rows": 0, "volume_rows_using_contract_unit": 0,
        "volume_rows_using_non_contract_unit": 0})
    sessions = {date(2026, 9, 10)}
    if defect == "missing_session":
        sessions.add(date(2026, 9, 9))
    monkeypatch.setattr(public, "_validated_taiex_session_dates", lambda *args: (sessions, "calendar-sha"))
    if defect:
        with pytest.raises(
            RuntimeError,
            match=(
                "accounting is incomplete|dates differ|sessions lack|non-session|"
                "stored minute partitions differ"
            ),
        ):
            audit.main()
        assert not output.exists()
    else:
        audit.main()
        result = json.loads(output.read_text())
        assert result["dataset_root"] == str(tmp_path)
        assert result["manifest_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
        assert result["calendar"]["extra_non_session_partitions"] == []
        assert result["calendar"]["missing_session_partitions"] == []
        assert not result["corporate_action_completeness_checked"]
        assert not result["calendar"]["per_symbol_session_completeness_checked"]


def test_full_ready_audit_requires_receipt_backed_calendar(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace
    from scripts import audit_shioaji_tw_minute_dataset as audit

    (tmp_path / "manifest.json").write_text(json.dumps({
        "schema_version": audit.SCHEMA_VERSION,
        "status": "research_ready",
        "research_ready": True,
    }))
    monkeypatch.setattr(audit, "parse_args", lambda: SimpleNamespace(
        all_partitions=True,
        dataset_root=tmp_path,
        calendar_root=None,
        output=tmp_path / "audit.json",
    ))
    with pytest.raises(RuntimeError, match="requires --calendar-root"):
        audit.main()
