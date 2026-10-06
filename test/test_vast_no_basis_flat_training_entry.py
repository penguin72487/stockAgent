"""A new numerical/batch experiment still delegates resume to canonical train.py."""
import ast
import hashlib
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


ENTRY = Path(__file__).resolve().parents[1] / "scripts/run_tw_no_basis_flat_b128_vastai1t.sh"
ROOT = Path("/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_v1")


def _blocks():
    blocks = re.findall(r"run_fintech_python - <<'PY'\n(.*?)\nPY", ENTRY.read_text(), re.S)
    assert len(blocks) == 2
    return blocks


def test_foreground_entry_keeps_accepted_node_and_allocator_budget():
    entry = ENTRY.read_text()
    for setting in (
        "STOCKAGENT_CPU_THREADS=8", "STOCKAGENT_TORCH_COMPILE_THREADS=16",
        "STOCKAGENT_POLARS_THREADS=1", "POLARS_MAX_THREADS=1", "RAYON_NUM_THREADS=1",
        "STOCKAGENT_BACKTEST_COMPILE_PREP=1", "STOCKAGENT_STRICT_NO_FALLBACK=1",
        "PYTORCH_ALLOC_CONF=expandable_segments:True",
    ):
        assert setting in entry
    assert "unset PYTORCH_CUDA_ALLOC_CONF" in entry


def test_foreground_entry_resume_fold11_dual_card_no_profile(monkeypatch):
    calls = []
    monkeypatch.setattr(manage_gpu_jobs, "_busy_gpu_indices", lambda: set())

    def leased(command, gpus, *, sharing):
        calls.append((command, gpus, sharing))
        return command

    monkeypatch.setattr(manage_gpu_jobs, "_gpu_lease_command", leased)
    monkeypatch.setattr(subprocess, "run", lambda command: subprocess.CompletedProcess(command, 0))
    with pytest.raises(SystemExit) as result:
        exec(compile(_blocks()[1], str(ENTRY), "exec"), {})
    assert result.value.code == 0
    command, cards, sharing = calls[0]
    assert cards == [0, 1] and not sharing
    monkeypatch.setattr(sys, "argv", command[1:])
    args = train.parse_args()
    assert args.resume and not args.retrain_completed_folds
    assert not args.profile_timing and not args.debug_timing_sync
    assert not args.isolate_train_folds
    assert args.start_fold == 11 and args.max_folds == 1
    assert args.torch_compile_threads == 16 and args.output_dir is None
    assert Path(args.config) == ROOT / "config.yaml"


def test_new_entry_does_not_contend_with_an_existing_gpu_owner(monkeypatch):
    monkeypatch.setattr(manage_gpu_jobs, "_busy_gpu_indices", lambda: {0})

    def forbidden(*args, **kwargs):
        pytest.fail("must not launch another GPU owner")

    monkeypatch.setattr(subprocess, "run", forbidden)
    with pytest.raises(RuntimeError, match="GPU 0/1"):
        exec(compile(_blocks()[1], str(ENTRY), "exec"), {})


@pytest.mark.parametrize("failed", [None, "lifecycle", "world_size", "optimizer_donor", "source", "config", "precision", "batch", "resume"])
def test_preflight_admits_only_matching_new_experiment(monkeypatch, tmp_path, failed):
    stored = tmp_path / "checkpoint_last.pt"
    stored.write_bytes(b"the existing formal optimizer must be preserved")
    config_bytes = b"pinned-config"
    proof = {
        "state": "accepted_complete_fold11_dual_gpu_flat_bf16_tf32_b128",
        "complete_fold_lifecycle_verified": failed != "lifecycle",
        "world_size": 1 if failed == "world_size" else 2,
        "formal_optimizer_reused": failed == "optimizer_donor",
        "source_sha256": "different" if failed == "source" else "pinned-source",
        "config_sha256": "different" if failed == "config" else hashlib.sha256(config_bytes).hexdigest(),
    }
    config = SimpleNamespace(
        environment=SimpleNamespace(amp_dtype="bf16", use_tensor_cores=failed != "precision", cpu_threads=8),
        training=SimpleNamespace(batch_size_train=64 if failed == "batch" else 128,
            batch_size_eval=16, factorized_encoder_checkpoint=True, epochs=1000,
            multi_gpu_strategy="distributed_data_parallel", day_trade_sparse_events=False,
            financial_transformer=SimpleNamespace(temporal_basis_families=[], feature_svd_components=0),
            pretrained_initialization_root=None),
        runner=SimpleNamespace(require_cuda=True, resume=failed != "resume", isolate_train_folds=False,
                               output_dir=str(ROOT / "training-bf16")),
    )
    original_read_text, original_read_bytes = Path.read_text, Path.read_bytes
    monkeypatch.setattr(Path, "read_text", lambda path, *a, **kw: json.dumps(proof)
        if path.name == "training-ready.json" else original_read_text(path, *a, **kw))
    monkeypatch.setattr(Path, "read_bytes", lambda path: config_bytes
        if path == ROOT / "config.yaml" else original_read_bytes(path))
    monkeypatch.setattr(config_module, "load_config", lambda path: config)
    monkeypatch.setattr(runtime_identity, "verify_source_release", lambda *a: {"source_sha256": "pinned-source"})
    monkeypatch.setattr(runtime_identity, "verify_release_bundles", lambda *a: None)
    tree = ast.parse(_blocks()[0])
    boundary = next(i for i, node in enumerate(tree.body) if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "feature_path" for target in node.targets))
    code = compile(ast.Module(body=tree.body[:boundary], type_ignores=[]), str(ENTRY), "exec")
    if failed is None:
        exec(code, {})
    else:
        with pytest.raises(AssertionError):
            exec(code, {})
    assert stored.read_bytes() == b"the existing formal optimizer must be preserved"
