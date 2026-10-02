from __future__ import annotations

from pathlib import Path

from downloader.download_openbb_archive import (
    Manifest,
    PlannerContext,
    TaskResult,
    make_task,
)
from scripts.benchmark_openbb_file_audit import _identity, run


def test_real_file_ownership_benchmark_is_bounded_and_preserves_source(
    tmp_path: Path,
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
            path.write_bytes(b"ownership-only fixture; not a content audit")
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
    assert receipt["scope"] == "path_ownership_stage_only_not_full_archive_or_service"
    assert all(trial["non_success_parquet_files"] == 0 for trial in receipt["trials"])
    assert before == [(path.read_bytes(), _identity(path)) for path in paths]
    assert not list((tmp_path / "scratch").iterdir())
