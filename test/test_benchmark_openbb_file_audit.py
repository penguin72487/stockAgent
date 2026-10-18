from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.download_openbb_archive import (
    Manifest,
    PlannerContext,
    TaskResult,
    make_task,
)
from scripts.benchmark_openbb_file_audit import _identity, _process_tree_rss, run


def test_tree_rss_includes_children_of_non_main_threads_without_double_visits(tmp_path: Path) -> None:
    for pid, rss, children in ((100, 20, {100: "101", 104: "102 101"}),
                               (101, 30, {101: "103"}), (102, 40, {102: "999"}),
                               (103, 50, {103: ""})):
        directory = tmp_path / str(pid)
        directory.mkdir()
        (directory / "status").write_text(f"Name:\ttest\nVmRSS:\t{rss} kB\n")
        for tid, values in children.items():
            task = directory / "task" / str(tid)
            task.mkdir(parents=True)
            (task / "children").write_text(values)
    assert _process_tree_rss(100, tmp_path) == 140 * 1024
    assert _process_tree_rss(999, tmp_path) == 0


@pytest.mark.parametrize("mode", ["ownership", "file-audit", "reconcile"])
def test_real_file_ownership_benchmark_is_bounded_and_preserves_source(
    tmp_path: Path, mode: str,
) -> None:
    context = PlannerContext(
        schemas={},
        commands={},
        output_dir=tmp_path / "source",
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
    manifest = Manifest(context.output_dir / "_state" / "openbb_archive.sqlite3")
    paths = []
    try:
        for symbol in ("A", "B"):
            task = make_task(
                context, "regulators.sec.filing_headers", symbol, {}, ["sec"]
            )
            path = Path(task.output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist([{
                "value": 1, "_openbb_endpoint": task.endpoint, "_provider": "sec",
                "_scope_key": task.scope_key, "_query_json": "{}",
                "_retrieved_at": "2026-10-18T04:00:00+00:00",
            }]), path)
            paths.append(path)
            manifest.upsert_tasks([task], plan_token="archive")
            manifest.claim([task])
            manifest.complete(TaskResult(task, "success", "sec", 1, str(path), 1))
        manifest.set_meta_value("active_plan_token", "archive")
    finally:
        manifest.close()
    before = [(path.read_bytes(), _identity(path)) for path in paths]
    receipt = run(
        [
            "--source-root",
            str(context.output_dir),
            "--scratch-parent",
            str(tmp_path / "scratch"),
            "--sample-files",
            "2",
            "--mode", mode,
        ]
    )
    assert receipt["sample_files"] == 2
    assert receipt["source_plan_token"] == "archive"
    assert [trial["variant"] for trial in receipt["trials"]] == [
        "baseline",
        "candidate",
        "candidate",
        "baseline",
    ]
    assert receipt["mode"] == mode
    assert receipt["schema_version"] == 5
    assert receipt["source_filenames_preserved"] is True
    assert receipt["candidate_contract_index_build_included_each_trial"] is (mode == "reconcile")
    for trial in receipt["trials"]:
        assert trial["workflow_tree_cpu_seconds"] >= trial["cpu_seconds"] > 0
        assert trial["sampled_tree_peak_rss_bytes"] > 0
        assert trial["process_tree_samples"] >= 2
        assert trial["semantic_counters"] == receipt["trials"][0]["semantic_counters"]
    assert receipt["scope"].endswith("not_full_archive_or_service")
    assert all(trial["non_success_parquet_files"] == 0 for trial in receipt["trials"])
    assert before == [(path.read_bytes(), _identity(path)) for path in paths]
    assert not list((tmp_path / "scratch").iterdir())
