"""CPU-only observation must not accept or create a model-forward cache."""

from pathlib import Path
import json

import pytest

from scripts import benchmark_live_inference as benchmark


@pytest.fixture
def observer(monkeypatch, tmp_path):
    identity = {"contract": "fixture", "input": {"sha256": "values"}, "mask": [True]}
    calls = {"validation": 0, "compute": 0}
    root = tmp_path / "must-not-be-created"

    def validate():
        calls["validation"] += 1

    def compute():
        calls["compute"] += 1
        raise AssertionError("model callback was executed")

    def forward(**kwargs):
        return benchmark.forward_cache.cached_forward(
            root, identity, compute, validate_before_commit=validate)

    def generate(**kwargs):
        return benchmark.signal_engine._live_forward_outputs(source_fingerprint="source-proof")

    monkeypatch.setattr(benchmark.signal_engine, "_live_forward_outputs", forward)
    monkeypatch.setattr(benchmark.signal_engine, "generate_live_signal", generate)
    monkeypatch.setattr(benchmark.torch.cuda, "is_initialized", lambda: False)
    return {
        "identity": identity, "calls": calls, "root": root,
        "kwargs": {"use_completed_session_cache": True, "write": False,
                   "publish_latest": False, "ensure_previous_signal": False,
                   "price_source": "panel"},
    }


def test_capture_uses_canonical_identity_fence_and_never_reads_writes_or_computes(observer):
    result = benchmark.capture_forward_identity(observer["kwargs"])
    assert result["contract"] == "completed_forward_identity_observer_v1"
    assert result["key"] == benchmark.forward_cache.identity_digest(observer["identity"])
    assert result["source_generation"] == "source-proof"
    assert observer["calls"] == {"validation": 1, "compute": 0}
    assert not observer["root"].exists()
    assert result["model_calls"] == result["network_requests"] == 0
    assert result["forward_cache_reads"] == result["forward_cache_writes"] == 0
    assert not result["signal_publication"] and not result["cuda_initialized"]
    observer["identity"]["mask"][0] = False
    assert result["identity"]["mask"] == [True]


@pytest.mark.parametrize("field,value", [
    ("use_completed_session_cache", False), ("write", True), ("publish_latest", True),
    ("ensure_previous_signal", True), ("price_source", "shioaji"),
])
def test_capture_rejects_online_or_publishing_options(observer, field, value):
    kwargs = {**observer["kwargs"], field: value}
    with pytest.raises(ValueError):
        benchmark.capture_forward_identity(kwargs)
    assert observer["calls"] == {"validation": 0, "compute": 0}


def test_capture_requires_fresh_cuda_cold_process(observer, monkeypatch):
    monkeypatch.setattr(benchmark.torch.cuda, "is_initialized", lambda: True)
    with pytest.raises(RuntimeError, match="CUDA-cold"):
        benchmark.capture_forward_identity(observer["kwargs"])


@pytest.mark.parametrize("work", ["model", "resolve", "cuda", "http"])
def test_capture_forbids_model_cuda_and_http(observer, monkeypatch, work):
    def generate(**kwargs):
        if work == "model":
            return benchmark.signal_engine.call_model(None)
        if work == "resolve":
            return benchmark.signal_engine.resolve_device(None)
        if work == "cuda":
            return benchmark.torch.cuda.is_available()
        import requests
        return requests.Session().request("GET", "https://invalid.example")

    monkeypatch.setattr(benchmark.signal_engine, "generate_live_signal", generate)
    with pytest.raises(AssertionError, match="model, CUDA or HTTP"):
        benchmark.capture_forward_identity(observer["kwargs"])


def test_capture_refuses_missing_identity_fence(observer, monkeypatch):
    def forward(**kwargs):
        return benchmark.forward_cache.cached_forward(Path("unused"), {}, lambda: None)

    monkeypatch.setattr(benchmark.signal_engine, "_live_forward_outputs", forward)
    with pytest.raises(RuntimeError, match="identity fence is missing"):
        benchmark.capture_forward_identity(observer["kwargs"])


def test_capture_preserves_failed_generation_validation(observer, monkeypatch):
    def forward(**kwargs):
        def invalid():
            raise RuntimeError("config/checkpoint/code generation changed")
        return benchmark.forward_cache.cached_forward(
            observer["root"], observer["identity"], lambda: None,
            validate_before_commit=invalid)

    monkeypatch.setattr(benchmark.signal_engine, "_live_forward_outputs", forward)
    with pytest.raises(RuntimeError, match="generation changed"):
        benchmark.capture_forward_identity(observer["kwargs"])
    assert not observer["root"].exists()


def test_capture_refuses_absent_source_fence(observer, monkeypatch):
    def generate(**kwargs):
        return benchmark.signal_engine._live_forward_outputs(source_fingerprint=None)

    monkeypatch.setattr(benchmark.signal_engine, "generate_live_signal", generate)
    with pytest.raises(RuntimeError, match="source fence"):
        benchmark.capture_forward_identity(observer["kwargs"])


def test_capture_refuses_a_path_that_never_reaches_forward(observer, monkeypatch):
    monkeypatch.setattr(benchmark.signal_engine, "generate_live_signal", lambda **kwargs: None)
    with pytest.raises(RuntimeError, match="never reached"):
        benchmark.capture_forward_identity(observer["kwargs"])


def test_identity_cli_cannot_be_mislabeled_as_forward_acceptance(monkeypatch, tmp_path):
    monkeypatch.setattr(benchmark.sys, "argv", ["benchmark_live_inference",
        "--market-config", "unused.yaml", "--panel-date", "2026-10-14",
        "--output", str(tmp_path / "unused.json"), "--completed-session-cache",
        "--cache-root", str(tmp_path / "unused-cache"), "--capture-forward-identity",
        "--compare-to", "old-forward.json"])
    with pytest.raises(SystemExit) as error:
        benchmark.main()
    assert error.value.code == 2
    assert not (tmp_path / "unused.json").exists()


@pytest.fixture
def forward_control(tmp_path):
    identity = {"code": ["old"], "input": "complete-window", "mask": [True, False]}
    value = {"weights": benchmark.torch.tensor([0.25, -0.0]),
             "aux": (benchmark.torch.tensor([float("nan")]), {"note": "keep", "count": 2})}
    old = tmp_path / "old"
    benchmark.forward_cache.cached_forward(old, identity, lambda: value,
                                          validate_before_commit=lambda: None)
    key = benchmark.forward_cache.identity_digest(identity)
    return old / f"{key}.receipt.json", value, {**identity, "code": ["new"]}


def test_forward_control_runs_new_model_once_then_accepts_full_cpu_cache(forward_control, tmp_path):
    receipt, value, identity = forward_control
    evidence = []
    wrapper = benchmark.forward_control_wrapper(receipt, evidence)
    calls = []

    def compute():
        calls.append("new-model-forward")
        return value

    for _ in range(2):
        wrapper(tmp_path / "new", identity, compute, validate_before_commit=lambda: None)
    assert calls == ["new-model-forward"]
    assert [row["model_calls"] for row in evidence] == [1, 0]
    assert all(row["whole_output_bitwise_equal"] for row in evidence)
    assert all(row["different_components"] == ["code"] for row in evidence)
    assert json.loads(receipt.read_text())["identity"]["code"] == ["old"]


def test_forward_control_refuses_different_inputs_before_any_model_work(forward_control, tmp_path):
    receipt, value, identity = forward_control
    wrapper = benchmark.forward_control_wrapper(receipt, [])
    with pytest.raises(RuntimeError, match="different canonical inputs: input"):
        wrapper(tmp_path / "new", {**identity, "input": "changed-window"},
                lambda: pytest.fail("model must not run"), validate_before_commit=lambda: None)
    assert not (tmp_path / "new").exists()


def test_forward_control_refuses_writing_into_old_root(forward_control):
    receipt, value, identity = forward_control
    wrapper = benchmark.forward_control_wrapper(receipt, [])
    with pytest.raises(ValueError, match="private forward cache"):
        wrapper(receipt.parent, identity, lambda: value, validate_before_commit=lambda: None)


def test_forward_control_refuses_changed_auxiliary_without_committing_candidate(forward_control, tmp_path):
    receipt, value, identity = forward_control
    wrapper = benchmark.forward_control_wrapper(receipt, [])
    changed = {**value, "aux": (value["aux"][0], {"note": "changed", "count": 2})}
    with pytest.raises(RuntimeError, match=r"output.aux\[1\].note"):
        wrapper(tmp_path / "new", identity, lambda: changed, validate_before_commit=lambda: None)
    assert not list((tmp_path / "new").glob("*.forward"))


def test_forward_control_corrupt_payload_never_falls_back_to_compute(forward_control, tmp_path):
    receipt, value, identity = forward_control
    path = receipt.parent / f"{json.loads(receipt.read_text())['key']}.forward"
    path.write_bytes(b"invalid")
    wrapper = benchmark.forward_control_wrapper(receipt, [])
    with pytest.raises(ValueError, match="size mismatch"):
        wrapper(tmp_path / "new", identity, lambda: pytest.fail("no unqualified model work"),
                validate_before_commit=lambda: None)


@pytest.mark.parametrize("candidate", [
    {"weights": benchmark.torch.tensor([1.0])},
    {"weights": benchmark.torch.tensor([0.25, 0.0]), "aux": None},
])
def test_whole_forward_oracle_refuses_missing_or_changed_tensors(forward_control, candidate):
    with pytest.raises(RuntimeError):
        benchmark.require_equal_forward_outputs(forward_control[1], candidate)


def test_whole_forward_oracle_retains_nan_bits_signed_zero_and_types():
    benchmark.require_equal_forward_outputs(benchmark.torch.tensor([float("nan"), -0.0]),
                                            benchmark.torch.tensor([float("nan"), -0.0]))
    with pytest.raises(RuntimeError, match="tensor bytes"):
        benchmark.require_equal_forward_outputs(benchmark.torch.tensor([-0.0]),
                                                benchmark.torch.tensor([0.0]))
    with pytest.raises(RuntimeError, match="shape/dtype"):
        benchmark.require_equal_forward_outputs(benchmark.torch.tensor([1.0]),
                                                benchmark.torch.tensor([1.0], dtype=benchmark.torch.float64))
    with pytest.raises(RuntimeError, match="output type"):
        benchmark.require_equal_forward_outputs([1], (1,))
