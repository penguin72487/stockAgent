"""Resource and provenance failure cases, without importing optional GPU wheels."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from stockagent.ocr.rapidocr import (
    ExecutionBudget, RapidOCRRunner, rapidocr_runtime, require_provider, summarize_profile,
)


@pytest.mark.parametrize("values", [
    {"device": "auto"}, {"cpu_threads": 0}, {"cpu_threads": True},
    {"cpu_cores": 0}, {"nice": -5}, {"gpu_memory_mb": 0}, {"device_id": -1},
])
def test_invalid_budget_does_not_reach_optional_runtime(values):
    with pytest.raises(ValueError):
        ExecutionBudget(**values)


def test_cuda_advertised_but_cpu_session_is_rejected():
    calls = []
    session = SimpleNamespace(get_providers=lambda: ["CPUExecutionProvider"],
                              disable_fallback=lambda: calls.append(True))
    with pytest.raises(RuntimeError, match="actual providers"):
        require_provider(session, "cuda")
    assert calls == []
    session.get_providers = lambda: ["CUDAExecutionProvider", "CPUExecutionProvider"]
    assert require_provider(session, "cuda")[0] == "CUDAExecutionProvider"
    assert calls == [True]


def test_profile_distinguishes_cpu_shape_ops_from_cpu_neural_compute(tmp_path):
    profile = tmp_path / "profile.json"
    events = [
        {"cat": "Node", "dur": 2, "args": {"provider": "CPUExecutionProvider", "op_name": "Shape"}},
        {"cat": "Node", "dur": 50, "args": {"provider": "CUDAExecutionProvider", "op_name": "Conv"}},
        {"cat": "Node", "dur": 30, "args": {"provider": "CPUExecutionProvider", "op_name": "FusedMatMul"}},
        {"cat": "Session", "dur": 900, "args": {}},
    ]
    profile.write_text(json.dumps(events))
    result = summarize_profile(profile)
    assert result["node_events"] == {"CPUExecutionProvider": 2, "CUDAExecutionProvider": 1}
    assert result["cpu_neural_compute"] == {"FusedMatMul": 1}
    assert result["node_duration_us"]["CUDAExecutionProvider"] == 50
    assert len(result["sha256"]) == 64


def test_gpu_owner_cannot_be_used_after_fork_or_close():
    engine = RapidOCRRunner.__new__(RapidOCRRunner)
    engine.closed = False
    engine.owner_pid = -1
    with pytest.raises(RuntimeError, match="owning process"):
        engine("image.png")
    engine.closed = True
    with pytest.raises(RuntimeError, match="owning process"):
        engine("image.png")


def test_gpu_trace_rejects_cpu_matrix_execution(tmp_path):
    path = tmp_path / "trace.json"
    path.write_text(json.dumps([
        {"cat": "Node", "args": {"provider": "CUDAExecutionProvider", "op_name": "Conv"}},
        {"cat": "Node", "args": {"provider": "CPUExecutionProvider", "op_name": "MatMul"}},
    ]))
    engine = RapidOCRRunner.__new__(RapidOCRRunner)
    engine.profile_dir = tmp_path
    engine.sessions = {"Rec": SimpleNamespace(end_profiling=lambda: str(path))}
    engine.budget = ExecutionBudget()
    engine.calls = 1
    with pytest.raises(RuntimeError, match="GPU neural execution not established"):
        engine.finish_profiling()


def test_execution_change_cannot_reuse_legacy_extraction_profile(monkeypatch, tmp_path):
    from scripts import extract_taifex_rule_review_candidates as extractor
    from downloader.artifact_io import sha256_file
    config = tmp_path / "config.json"
    config.write_text("{}")
    monkeypatch.setattr(extractor, "rapidocr_runtime", lambda _: ({}, {}))
    legacy = extractor.rapidocr_review_profile(config, 300, True)
    assert legacy == f"pdf_review_rapidocr_{sha256_file(config)}_300dpi_force1_v1"
    monkeypatch.setattr(extractor, "rapidocr_runtime", lambda _: (
        {"execution": {"device": "cuda"}}, {"implementation_sha256": "a" * 64}))
    first = extractor.rapidocr_review_profile(config, 300, True)
    monkeypatch.setattr(extractor, "rapidocr_runtime", lambda _: (
        {"execution": {"device": "cuda"}}, {"implementation_sha256": "b" * 64}))
    second = extractor.rapidocr_review_profile(config, 300, True)
    assert len({legacy, first, second}) == 3


def test_corrupt_model_is_rejected_before_dependency_import(tmp_path):
    model = tmp_path / "model.onnx"
    model.write_bytes(b"wrong retained source")
    cfg = tmp_path / "ocr.json"
    cfg.write_text(json.dumps({"libraries": str(tmp_path),
        "models": {name: {"path": str(model), "sha256": "0" * 64} for name in ("Det", "Rec", "Cls")},
        "packages": {}, "params": {}}))
    with pytest.raises(ValueError, match="model hash mismatch"):
        rapidocr_runtime(cfg)


def test_cli_rejects_multiple_gpu_owners_before_loading_data(monkeypatch, tmp_path):
    from scripts.extract_taifex_rule_review_candidates import main
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({"execution": {"device": "cuda"}}))
    monkeypatch.setattr("sys.argv", ["extract", "--output-dir", str(tmp_path / "out"),
        "--rapidocr-config", str(cfg), "--workers", "12"])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
    assert not (tmp_path / "out").exists()
