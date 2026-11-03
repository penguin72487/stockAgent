"""Resource and provenance failure cases, without importing optional GPU wheels."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from stockagent.ocr.rapidocr import (
    ExecutionBudget, RapidOCRRunner, rapidocr_runtime, require_provider, summarize_profile,
    _configure_native_thread_environment, _cuda_provider_options,
)
from stockagent.ocr.fastpath import Optimizations


@pytest.mark.parametrize("values", [
    {"device": "auto"}, {"cpu_threads": 0}, {"cpu_threads": True},
    {"cpu_cores": 0}, {"nice": -5}, {"gpu_memory_mb": 0}, {"device_id": -1},
    {"model_memory_mb": {'Wrong':2048}}, {"model_memory_mb": {'Det':True}},
    {"shared_thread_pool": True}, {"shared_thread_pool": 'yes'},
])
def test_invalid_budget_does_not_reach_optional_runtime(values):
    with pytest.raises(ValueError):
        ExecutionBudget(**values)


def test_unrestricted_profile_removes_inherited_thread_limits_and_gpu_arena_cap(monkeypatch):
    for name in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'OMP_THREAD_LIMIT'):
        monkeypatch.setenv(name, '1')
    budget = ExecutionBudget(cpu_threads=None, cpu_cores=None, nice=None,
                             gpu_memory_mb=None, min_free_gpu_mb=None,
                             disable_thp=False, exclusive_gpu_owner=False)
    _configure_native_thread_environment(budget)
    import os
    assert all(name not in os.environ for name in
               ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'OMP_THREAD_LIMIT'))
    assert 'gpu_mem_limit' not in _cuda_provider_options(budget, Optimizations(), 'Rec')
    bounded = _cuda_provider_options(ExecutionBudget(model_memory_mb={'Rec': 512}),
                                     Optimizations(), 'Rec')
    assert bounded['gpu_mem_limit'] == 512 * 1024**2


def test_unrestricted_runner_does_not_apply_process_or_ort_caps(monkeypatch):
    import sys
    import stockagent.ocr.rapidocr as module

    def forbidden(*args, **kwargs):
        raise AssertionError('unrestricted profile attempted to impose a resource limit')

    monkeypatch.setattr(module.os, 'sched_setaffinity', forbidden)
    monkeypatch.setattr(module.os, 'setpriority', forbidden)
    sessions = []

    class Options:
        def __init__(self):
            self.intra_op_num_threads = 0
            self.inter_op_num_threads = 0
            self.use_per_session_threads = True
            self.config = {}
        def add_session_config_entry(self, key, value):
            self.config[key] = value

    class Session:
        def __init__(self, path, sess_options, providers):
            self.options, self.providers, self.fallback_disabled = sess_options, providers, False
            sessions.append(self)
        def get_providers(self):
            return ['CUDAExecutionProvider', 'CPUExecutionProvider']
        def disable_fallback(self):
            self.fallback_disabled = True
        def get_modelmeta(self):
            return SimpleNamespace(custom_metadata_map={'character': 'abc'})
        def get_inputs(self):
            return [SimpleNamespace(name='input')]
        def get_outputs(self):
            return [SimpleNamespace(name='output')]

    shared_pools = []
    fake_ort = SimpleNamespace(SessionOptions=Options, InferenceSession=Session,
        set_global_thread_pool_sizes=lambda: shared_pools.append(True),
        get_available_providers=lambda: ['CUDAExecutionProvider'])
    fake_cv2 = SimpleNamespace(setNumThreads=forbidden, getNumThreads=lambda: 8)
    fake_torch = SimpleNamespace(set_num_threads=forbidden, __version__='test',
        version=SimpleNamespace(cuda='test'), backends=SimpleNamespace(cudnn=SimpleNamespace(version=lambda: 1)),
        cuda=SimpleNamespace(is_available=lambda: True, device_count=lambda: 1,
                             mem_get_info=lambda device: (0, 1), get_device_name=lambda device: 'test'))
    monkeypatch.setitem(sys.modules, 'torch', fake_torch)
    monkeypatch.setattr(module, 'import_module', lambda name: {'onnxruntime': fake_ort, 'cv2': fake_cv2}[name])
    monkeypatch.setattr(module, '_parameters', lambda config: {})

    class FakeOCR:
        def __init__(self, params):
            for attr in ('text_det', 'text_cls', 'text_rec'):
                setattr(self, attr, SimpleNamespace(get_preprocess=lambda: None))
            for attr in ('load_img', 'preprocess_img', 'crop_text_regions', 'build_final_output'):
                setattr(self, attr, lambda *args: None)

    monkeypatch.setitem(sys.modules, 'rapidocr', SimpleNamespace(RapidOCR=FakeOCR))
    monkeypatch.setitem(sys.modules, 'omegaconf', SimpleNamespace(flag_override=lambda *a: None))
    runner = RapidOCRRunner.__new__(RapidOCRRunner)
    runner.budget = ExecutionBudget(cpu_threads=None, cpu_cores=None, nice=None,
        gpu_memory_mb=None, min_free_gpu_mb=None, disable_thp=False, exclusive_gpu_owner=False,
        shared_thread_pool=True)
    runner.optimizations = Optimizations()
    runner.lock, runner.profile_dir, runner.sessions, runner.events, runner.runtime = None, None, {}, [], {}
    runner._initialize({'models': {name: {'path': name} for name in ('Det', 'Rec', 'Cls')}})
    assert runner.lock is None
    assert len(sessions) == 3 and shared_pools == [True]
    assert all(s.fallback_disabled and s.options.intra_op_num_threads == 0 and
               s.options.inter_op_num_threads == 0 and not s.options.config and
               not s.options.use_per_session_threads and
               'gpu_mem_limit' not in s.providers[0][1] for s in sessions)


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
