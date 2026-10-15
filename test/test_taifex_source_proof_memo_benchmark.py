import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json
from scripts import benchmark_taifex_source_proof_memo as benchmark
from test_taifex_volatility_dashboard import _fixture


@pytest.fixture
def real_fixture(tmp_path):
    state, _ = _fixture(tmp_path)
    root = tmp_path / "benchmark"
    fixture = root / "fixture"
    benchmark.columns.prepare(fixture, state)
    proof = benchmark.columns.copy_prefix(
        Path(benchmark.workflow.cache.__file__), root / "original_column_cache.py"
    )
    atomic_write_json(root / "original-code-proof.json", proof)
    with (fixture / "state/marks.jsonl").open("ab") as stream:
        stream.write((fixture / "captured-tail.jsonl").read_bytes())
    return root


def test_full_seven_curve_worker_durable_readback_and_same_process_warm(
    real_fixture, monkeypatch
):
    monkeypatch.setattr(benchmark.workflow.cache, "MIN_SOURCE_BYTES", 1)
    original = benchmark.worker(real_fixture, "baseline", 0, "update")
    candidate = benchmark.worker(real_fixture, "candidate", 1, "update")
    assert original["output_sha256"] == candidate["output_sha256"]
    assert set(candidate["output_sha256"]) == set(
        benchmark.workflow.dashboard.HISTORY_RANGE_SECONDS
    )
    assert candidate["warm_proof_bytes"] == 0
    assert candidate["durable_complete_readback"] is True
    assert candidate["warm_exact_output_parity"] is True
    for path in (real_fixture / "comparison").rglob("update-*.json"):
        assert path.stat().st_mode & 0o777 == 0o600
        assert len(json.loads(path.read_bytes())["history"]) > 0


def test_corrupt_original_code_is_rejected_before_cache_work(real_fixture):
    (real_fixture / "original_column_cache.py").write_text("# replaced\n")
    with pytest.raises(ValueError, match="original implementation"):
        benchmark.worker(real_fixture, "baseline", 0, "update")
    assert not (real_fixture / "comparison").exists()


@pytest.mark.parametrize(
    "identifier", ["../escaped", "/tmp/escaped", "bad/name", "", "a" * 65]
)
def test_measurement_name_cannot_escape_private_root(real_fixture, identifier):
    with pytest.raises(ValueError, match="measurement identifier"):
        benchmark.worker(real_fixture, "candidate", 0, "update", identifier)


def test_worker_failure_keeps_actionable_error_without_claiming_output(
    real_fixture, monkeypatch
):
    monkeypatch.setattr(
        benchmark.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1, stderr="original failure", stdout=""
        ),
    )
    with pytest.raises(RuntimeError, match="original failure"):
        benchmark.launch(real_fixture, "candidate", 0, "update", "comparison")
