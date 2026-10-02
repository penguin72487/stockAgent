from __future__ import annotations

from pathlib import Path
from dataclasses import replace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.download_openbb_archive import (
    CoverageDecision,
    Manifest,
    PlannerContext,
    TaskResult,
    _command_providers,
    _atomic_write_parquet,
    make_task,
    repair_catalog_followups_once,
)


def _context(root: Path) -> PlannerContext:
    return PlannerContext(
        schemas={},
        commands={".regulators.sec.symbol_map": ["sec"]},
        output_dir=root,
        start_date="2000-01-01",
        end_date="2026-07-18",
        assets=[],
        etfs=[],
        currencies=[],
        indices=[],
        countries=[],
        allowed_providers=None,
        disabled_providers=set(),
        endpoint_filters=(),
        categories=None,
    )


def _coverage(*endpoints: str) -> list[CoverageDecision]:
    return [
        CoverageDecision(endpoint, "regulators", "sec", "sec", "included", "test")
        for endpoint in endpoints
    ]


def _accept_parent(context: PlannerContext, manifest: Manifest, records: list[dict]):
    parent = make_task(context, "regulators.sec.cik_map", "A", {"symbol": "A"}, ["sec"])
    manifest.upsert_tasks([parent], plan_token="archive")
    rows = _atomic_write_parquet(records, parent, "sec")
    manifest.claim([parent])
    manifest.complete(TaskResult(parent, "success", "sec", rows, parent.output_path, 1))
    return parent


@pytest.mark.parametrize("registry_prefix", ["", "."])
@pytest.mark.parametrize("child_decision", ["included", "deferred"])
def test_restart_restores_only_missing_children_without_changing_accepted_data(
    tmp_path: Path,
    registry_prefix: str,
    child_decision: str,
) -> None:
    context = _context(tmp_path)
    context.commands = {registry_prefix + "regulators.sec.symbol_map": ["sec"]}
    coverage = _coverage("regulators.sec.cik_map", "regulators.sec.symbol_map")
    coverage[-1] = replace(coverage[-1], decision=child_decision)
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        parent = _accept_parent(
            context, manifest, [{"cik": "0000002230"}, {"cik": "0001837240"}]
        )
        accepted = make_task(
            context,
            "regulators.sec.symbol_map",
            "0001837240",
            {"query": "0001837240", "use_cache": True},
            ["sec"],
        )
        manifest.upsert_tasks([accepted], plan_token="archive", task_source="followup")
        rows = _atomic_write_parquet([{"symbol": "accepted"}], accepted, "sec")
        manifest.claim([accepted])
        manifest.complete(
            TaskResult(accepted, "success", "sec", rows, accepted.output_path, 1)
        )
        before = tuple(
            manifest.connection.execute(
                "SELECT * FROM tasks WHERE task_id=?", (accepted.task_id,)
            ).fetchone()
        )
        source = Path(parent.output_path).read_bytes()
        accepted_bytes = Path(accepted.output_path).read_bytes()
        result = repair_catalog_followups_once(
            context, manifest, plan_token="archive", coverage=coverage
        )
        assert result == {
            "checked_parents": 1,
            "expected_children": 2,
            "restored_children": 1,
        }
        missing = make_task(
            context,
            "regulators.sec.symbol_map",
            "0000002230",
            {"query": "0000002230", "use_cache": True},
            ["sec"],
        )
        row = manifest.connection.execute(
            "SELECT * FROM tasks WHERE task_id=?", (missing.task_id,)
        ).fetchone()
        assert row["active"] == 1
        assert row["plan_token"] == "archive"
        assert row["task_source"] == "followup"
        assert row["status"] == "pending"
        assert row["attempts"] == 0
        assert (
            tuple(
                manifest.connection.execute(
                    "SELECT * FROM tasks WHERE task_id=?", (accepted.task_id,)
                ).fetchone()
            )
            == before
        )
        assert Path(parent.output_path).read_bytes() == source
        assert Path(accepted.output_path).read_bytes() == accepted_bytes
        assert not Path(missing.output_path).exists()
        assert (
            repair_catalog_followups_once(
                context, manifest, plan_token="archive", coverage=coverage
            )
            is None
        )
        assert context.end_date == "2026-07-18"
    finally:
        manifest.close()


def test_registry_lookup_preserves_explicit_empty_provider_route(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    context.commands = {
        ".regulators.sec.symbol_map": [],
        "regulators.sec.symbol_map": ["sec"],
    }
    assert _command_providers(context, "regulators.sec.symbol_map") == []


def test_membership_batches_use_primary_key_not_active_schedule_scan(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        _accept_parent(
            context,
            manifest,
            [{"cik": f"{number:010}"} for number in range(526)],
        )
        lookups = []

        def capture(statement: str) -> None:
            if statement.startswith("SELECT task_id,active,plan_token FROM tasks"):
                lookups.append(statement)

        manifest.connection.set_trace_callback(capture)
        result = repair_catalog_followups_once(
            context,
            manifest,
            plan_token="archive",
            coverage=_coverage("regulators.sec.cik_map", "regulators.sec.symbol_map"),
        )
        manifest.connection.set_trace_callback(None)
        assert result == {
            "checked_parents": 1,
            "expected_children": 526,
            "restored_children": 526,
        }
        assert len(lookups) == 2
        for statement in lookups:
            assert "WHERE task_id IN (" in statement
            plan = manifest.connection.execute(
                "EXPLAIN QUERY PLAN " + statement
            ).fetchall()
            assert any("(task_id=?)" in row["detail"] for row in plan)
            assert all("(active=?)" not in row["detail"] for row in plan)
    finally:
        manifest.close()


def test_inactive_and_foreign_children_rejoin_plan_without_resetting_success(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        _accept_parent(context, manifest, [{"cik": "inactive"}, {"cik": "foreign"}])
        children = []
        before = {}
        for scope in ("inactive", "foreign"):
            child = make_task(
                context,
                "regulators.sec.symbol_map",
                scope,
                {"query": scope, "use_cache": True},
                ["sec"],
            )
            manifest.upsert_tasks([child], plan_token="other", task_source="followup")
            rows = _atomic_write_parquet([{"symbol": scope}], child, "sec")
            manifest.claim([child])
            manifest.complete(
                TaskResult(child, "success", "sec", rows, child.output_path, 1)
            )
            if scope == "inactive":
                manifest.connection.execute(
                    "UPDATE tasks SET active=0 WHERE task_id=?", (child.task_id,)
                )
                manifest.connection.commit()
            before[child.task_id] = Path(child.output_path).read_bytes()
            children.append(child)
        result = repair_catalog_followups_once(
            context,
            manifest,
            plan_token="archive",
            coverage=_coverage("regulators.sec.cik_map", "regulators.sec.symbol_map"),
        )
        assert result["restored_children"] == 2
        for child in children:
            row = manifest.connection.execute(
                "SELECT * FROM tasks WHERE task_id=?", (child.task_id,)
            ).fetchone()
            assert row["active"] == 1
            assert row["plan_token"] == "archive"
            assert row["status"] == "success"
            assert row["attempts"] == 1
            assert row["rows"] == 1
            assert Path(child.output_path).read_bytes() == before[child.task_id]
    finally:
        manifest.close()


@pytest.mark.parametrize("decision", ["excluded", "unavailable", "not_enumerable"])
def test_repair_never_admits_disallowed_child_scope(
    tmp_path: Path, decision: str
) -> None:
    context = _context(tmp_path)
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        _accept_parent(context, manifest, [{"cik": "0000002230"}])
        coverage = _coverage("regulators.sec.cik_map", "regulators.sec.symbol_map")
        coverage[-1] = replace(coverage[-1], decision=decision)
        result = repair_catalog_followups_once(
            context, manifest, plan_token="archive", coverage=coverage
        )
        assert result["restored_children"] == 0
        assert (
            manifest.connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 1
        )
    finally:
        manifest.close()


@pytest.mark.parametrize("defect", ["wrong_rows", "wrong_metadata", "missing_file"])
def test_bad_parent_never_records_repair_complete(tmp_path: Path, defect: str) -> None:
    context = _context(tmp_path)
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        parent = _accept_parent(context, manifest, [{"cik": "0000002230"}])
        path = Path(parent.output_path)
        if defect == "wrong_rows":
            manifest.connection.execute(
                "UPDATE tasks SET rows=2 WHERE task_id=?", (parent.task_id,)
            )
            manifest.connection.commit()
        elif defect == "wrong_metadata":
            table = pq.read_table(path)
            table = table.set_column(
                table.schema.get_field_index("_scope_key"),
                "_scope_key",
                pa.array(["other-parent"]),
            )
            pq.write_table(table, path)
        else:
            path.unlink()
        with pytest.raises((ValueError, FileNotFoundError)):
            repair_catalog_followups_once(
                context,
                manifest,
                plan_token="archive",
                coverage=_coverage(
                    "regulators.sec.cik_map", "regulators.sec.symbol_map"
                ),
            )
        assert manifest.meta_value("archive:catalog_followup_repair_version") is None
        assert (
            manifest.connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 1
        )
    finally:
        manifest.close()


def test_no_discovery_and_excluded_children_are_respected(tmp_path: Path) -> None:
    context = _context(tmp_path)
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        _accept_parent(context, manifest, [{"cik": "0000002230"}])
        coverage = _coverage("regulators.sec.cik_map", "regulators.sec.symbol_map")
        assert (
            repair_catalog_followups_once(
                context,
                manifest,
                plan_token="archive",
                coverage=coverage,
                no_discovery=True,
            )
            is None
        )
        assert manifest.meta_value("archive:catalog_followup_repair_version") is None
        result = repair_catalog_followups_once(
            context,
            manifest,
            plan_token="archive",
            coverage=_coverage("regulators.sec.cik_map"),
        )
        assert result["restored_children"] == 0
        assert (
            manifest.connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 1
        )
    finally:
        manifest.close()


def test_interrupted_repair_resumes_committed_batches_without_false_completion(
    tmp_path: Path, monkeypatch
) -> None:
    context = _context(tmp_path)
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        parent = _accept_parent(
            context, manifest, [{"cik": f"{number:010}"} for number in range(513)]
        )
        source = Path(parent.output_path).read_bytes()
        original_upsert = manifest.upsert_tasks

        def interrupted_upsert(*args, **kwargs):
            original_upsert(*args, **kwargs)
            raise RuntimeError("interrupted after first durable batch")

        monkeypatch.setattr(manifest, "upsert_tasks", interrupted_upsert)
        coverage = _coverage("regulators.sec.cik_map", "regulators.sec.symbol_map")
        with pytest.raises(RuntimeError, match="first durable batch"):
            repair_catalog_followups_once(
                context, manifest, plan_token="archive", coverage=coverage
            )
        assert manifest.meta_value("archive:catalog_followup_repair_version") is None
        before = [
            tuple(row)
            for row in manifest.connection.execute(
                "SELECT * FROM tasks WHERE endpoint='regulators.sec.symbol_map' "
                "ORDER BY task_id"
            )
        ]
        assert len(before) == 512
        monkeypatch.setattr(manifest, "upsert_tasks", original_upsert)
        result = repair_catalog_followups_once(
            context, manifest, plan_token="archive", coverage=coverage
        )
        assert result["restored_children"] == 1
        after = {
            row["task_id"]: tuple(row)
            for row in manifest.connection.execute(
                "SELECT * FROM tasks WHERE endpoint='regulators.sec.symbol_map'"
            )
        }
        assert len(after) == 513
        assert all(after[row[0]] == row for row in before)
        assert Path(parent.output_path).read_bytes() == source
    finally:
        manifest.close()
