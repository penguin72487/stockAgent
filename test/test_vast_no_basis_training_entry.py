"""The fixed Vast entry delegates restart semantics to canonical train.py."""

import ast
import json
from pathlib import Path
import re
import subprocess
import sys
from types import SimpleNamespace

import pytest

import train
from scripts import manage_gpu_jobs
from stockagent import config as config_module
from stockagent import runtime_identity
from stockagent.data import factorized_panel


ENTRY = (
    Path(__file__).resolve().parents[1]
    / "artifacts/data_quality/tw_feature_expected_gaps_20261004"
    / "train_vast_no_basis_fold11_v3.sh"
)
FORMAL_OUTPUT = Path(
    "/root/stockAgent/artifacts/markets/"
    "tw_day_trade_factorized_values_20261005_no_basis_v3/training-bf16"
)


def _entry_python_blocks():
    blocks = re.findall(
        r"run_fintech_python - <<'PY'\n(.*?)\nPY", ENTRY.read_text(), re.S
    )
    assert len(blocks) == 2
    return blocks


def test_entry_uses_canonical_resume_without_profiling(monkeypatch):
    calls = []
    monkeypatch.setattr(manage_gpu_jobs, "_busy_gpu_indices", lambda: set())

    def lease(command, gpus, *, sharing):
        calls.append((command, gpus, sharing))
        return command

    monkeypatch.setattr(manage_gpu_jobs, "_gpu_lease_command", lease)
    monkeypatch.setattr(
        subprocess, "run", lambda command: subprocess.CompletedProcess(command, 0)
    )
    with pytest.raises(SystemExit) as exited:
        exec(compile(_entry_python_blocks()[1], str(ENTRY), "exec"), {})
    assert exited.value.code == 0
    command, gpus, sharing = calls[0]
    assert gpus == [0, 1] and sharing is False
    monkeypatch.setattr(sys, "argv", command[1:])
    args = train.parse_args()
    assert args.resume is True
    assert args.retrain_completed_folds is False
    assert args.profile_timing is False
    assert args.debug_timing_sync is False
    assert args.start_fold == 11 and args.max_folds == 1
    assert args.torch_compile_threads == 16
    assert args.output_dir is None  # Keep the existing formal output/checkpoints.
    assert args.isolate_train_folds is False
    assert Path(args.config).name == (
        "tw_day_trade_factorized_values_20261006_no_basis_runtime_optimized_v1.yaml"
    )


def test_entry_rejects_busy_gpu_without_starting_training(monkeypatch):
    monkeypatch.setattr(manage_gpu_jobs, "_busy_gpu_indices", lambda: {1})

    def unexpected_launch(*args, **kwargs):
        pytest.fail("must not launch another owner on an occupied GPU")

    monkeypatch.setattr(subprocess, "run", unexpected_launch)
    with pytest.raises(RuntimeError, match="GPU 0/1"):
        exec(compile(_entry_python_blocks()[1], str(ENTRY), "exec"), {})


@pytest.mark.parametrize("raw_resume", [False, True])
@pytest.mark.parametrize("failed_gate", [None, "lifecycle", "source", "gradient", "finance"])
def test_preflight_accepts_existing_formal_output(
    monkeypatch, tmp_path, raw_resume, failed_gate
):
    output = tmp_path / "training-bf16"
    output.mkdir()
    (output / "checkpoint_last.pt").write_bytes(b"existing formal checkpoint")
    config = SimpleNamespace(
        environment=SimpleNamespace(amp_dtype="bf16", cpu_threads=8),
        runner=SimpleNamespace(
            require_cuda=True, resume=raw_resume, output_dir=FORMAL_OUTPUT,
            isolate_train_folds=False,
        ),
        training=SimpleNamespace(
            epochs=1000,
            batch_size_train=32,
            multi_gpu_strategy="distributed_data_parallel",
            day_trade_sparse_events=False,
            financial_transformer=SimpleNamespace(
                temporal_basis_families=[],
                feature_svd_components=0,
                candle_projection_fp32=True,
            ),
            pretrained_initialization_root=None,
            factorized_encoder_checkpoint=False,
            eval_backtest_chunk_rows=32,
        ),
    )
    proof = {
        "state": "accepted_actual_fold11_dual_gpu_runtime_optimization",
        "training_ready": True,
        "formal_training_started": False,  # Historical engineering receipt only.
        "formal_optimizer_reused": False,
        "model_decomposition": "none",
        "code_source_sha256": "verified-source",
        "config_sha256": "verified-config",
        "complete_fold_lifecycle_verified": failed_gate != "lifecycle",
        "source_raw_bits_exact": failed_gate != "source",
        "existing_bf16_gradient_oracle_passed": failed_gate != "gradient",
        "artifact_agreement": {"financial_fields_bit_exact": failed_gate != "finance"},
    }
    original_is_file = Path.is_file
    original_read_text = Path.read_text
    monkeypatch.setattr(
        Path, "is_file",
        lambda path: True if path.name == "fold11-runtime-optimization-acceptance.json"
        else original_is_file(path),
    )
    monkeypatch.setattr(
        Path, "read_text",
        lambda path, *args, **kwargs: json.dumps(proof)
        if path.name == "fold11-runtime-optimization-acceptance.json"
        else original_read_text(path, *args, **kwargs),
    )
    monkeypatch.setattr(config_module, "load_config", lambda path: config)
    monkeypatch.setattr(factorized_panel, "file_sha256", lambda path: "verified-config")
    monkeypatch.setattr(
        runtime_identity, "verify_source_release",
        lambda *args: {"source_sha256": "verified-source"},
    )
    tree = ast.parse(_entry_python_blocks()[0])
    # Execute the actual entry's admission stage, without renewing source leases.
    boundary = next(
        index for index, node in enumerate(tree.body)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "manifest"
                for target in node.targets)
    )
    code = compile(ast.Module(body=tree.body[:boundary], type_ignores=[]), str(ENTRY), "exec")
    if failed_gate is None:
        exec(code, {})
    else:
        with pytest.raises(AssertionError):
            exec(code, {})
    assert (output / "checkpoint_last.pt").read_bytes() == b"existing formal checkpoint"
