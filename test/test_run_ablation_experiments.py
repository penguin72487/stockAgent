from pathlib import Path

import pytest
import yaml

import scripts.run_ablation_experiments as ablation_module
from scripts.run_ablation_experiments import (
    _build_configs,
    _deep_merge,
    _effective_parallel_jobs,
    _experiment_rows,
    _failure_kind,
    _fold_status,
    _format_fold_status,
    _per_job_thread_budget,
    _resolve_pinned_panel_cache_env,
)


def test_deep_merge_preserves_unmodified_nested_values() -> None:
    base = {"training": {"epochs": 10, "model": {"dropout": 0.1, "layers": 2}}}
    result = _deep_merge(base, {"training": {"model": {"dropout": 0.0}}})

    assert result == {
        "training": {"epochs": 10, "model": {"dropout": 0.0, "layers": 2}}
    }
    assert base["training"]["model"]["dropout"] == 0.1


def test_ablation_spec_inheritance_reuses_matrix_and_overrides_contract(
    tmp_path: Path,
) -> None:
    base = tmp_path / "base.yaml"
    base.write_text(
        """
base_config: configs/markets/tw_day_trade_daily_no_default.yaml
output_root: artifacts/ablations/base
expected_fold_count: 12
runtime:
  parallel_jobs: 1
matrix:
  include_baseline: true
  dimensions:
    - name: learning_rate
      enabled: true
      path: training.learning_rate
      values:
        - name: variant
          experiment_name: variant
          value: 0.0002
""",
        encoding="utf-8",
    )
    child = tmp_path / "child.yaml"
    child.write_text(
        """
base_spec: base.yaml
base_config: configs/markets/tw_day_trade_daily_multi_basis_tplus2_close_capital10m.yaml
output_root: artifacts/ablations/multi_basis_capital10m
""",
        encoding="utf-8",
    )

    spec, rows = _experiment_rows(child)

    assert spec["base_config"].endswith("multi_basis_tplus2_close_capital10m.yaml")
    assert spec["output_root"].endswith("multi_basis_capital10m")
    assert spec["runtime"]["parallel_jobs"] == 1
    assert spec["expected_fold_count"] == 12
    assert [row["name"] for row in rows] == ["baseline", "variant"]


def test_projection_l1_multi_basis_ablation_runs_baseline_then_every_variant(
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    spec_path = (
        repo_root
        / "configs/ablations/tw_day_trade_daily_multi_basis_projection_l1_tplus2_close_commission20_v1.yaml"
    )
    spec, experiments = _experiment_rows(spec_path)

    assert len(experiments) == 20
    experiment_names = [row["name"] for row in experiments]
    assert experiment_names[0] == "baseline"
    assert "lookback128_batch128" in experiment_names
    assert "lookback128_batch256" not in experiment_names
    assert "require_complete_baseline_artifact" not in spec
    assert "baseline_artifact_root" not in spec
    assert "panel_history_v3" in spec["output_root"]
    assert spec["runtime"]["parallel_jobs"] == 1
    assert spec["pinned_panel_cache"] == {
        "snapshot_id": "tw-public-20260818T004438951872136Z-l0-penguin-03232cac51756cb4",
        "variant_id": "dffa1e873a46390f68a60efa5e30e8f2a5d0c3546d1a6aeed5d52215e3de2448",
        "version": 51,
        "generation": "a49777df44f241b29c87d36f89504406",
        "source_hash": "7aa93ac5d97fa96bdfec042750edc28d68ab5129afa748dbf83a7df81f119464",
    }

    runs = _build_configs(spec_path, spec, experiments, tmp_path)
    effective = {
        run["name"]: yaml.safe_load(run["config_path"].read_text(encoding="utf-8"))
        for run in runs
    }
    output_mode_variants = {
        "output_activation_l1",
        "output_logits",
        "output_signed_softmax",
        "output_signed_entmax15",
        "output_signed_sparsemax",
    }
    for name, raw in effective.items():
        assert raw["walk_forward"]["lookback_context"] == "panel_history"
        assert raw["training"]["epochs"] == 1000
        assert raw["training"]["record_epoch_curve"] is True
        assert raw["training"]["curve_plot_interval"] == 1
        assert raw["training"]["defer_epoch_curve_plot_until_end"] is False
        expected_capital = {
            "initial_capital_1m": 1_000_000.0,
            "initial_capital_100m": 100_000_000.0,
        }.get(name, 10_000_000.0)
        assert raw["trading"]["volume_participation_equity"] == expected_capital
        assert raw["training"]["batch_size_train"] == 128
        if name not in output_mode_variants:
            assert (
                raw["training"]["financial_transformer"]["portfolio_output_mode"]
                == "projection_l1"
            )
    assert effective["lookback128_batch128"]["training"]["lookback"] == 128


def test_inherited_experiment_override_renames_and_patches_one_row(
    tmp_path: Path,
) -> None:
    base = tmp_path / "base.yaml"
    base.write_text(
        """
base_config: configs/markets/tw_day_trade_daily_no_default.yaml
matrix:
  include_baseline: false
  dimensions:
    - name: learning_rate
      enabled: true
      path: training.learning_rate
      values:
        - name: original
          experiment_name: original
          value: 0.0002
""",
        encoding="utf-8",
    )
    child = tmp_path / "child.yaml"
    child.write_text(
        """
base_spec: base.yaml
matrix:
  experiment_overrides:
    original:
      experiment_name: repaired
      description: repaired without copying the inherited matrix
      overrides:
        training:
          learning_rate: 0.0003
          batch_size_train: 128
""",
        encoding="utf-8",
    )

    _, rows = _experiment_rows(child)

    assert rows == [
        {
            "name": "repaired",
            "dimension": "learning_rate",
            "description": "repaired without copying the inherited matrix",
            "overrides": {
                "training": {
                    "learning_rate": 0.0003,
                    "batch_size_train": 128,
                }
            },
        }
    ]


def test_parallel_jobs_split_host_wide_thread_budgets() -> None:
    assert _per_job_thread_budget(112, 2) == 56
    assert _per_job_thread_budget(16, 2) == 8
    assert _per_job_thread_budget(1, 2) == 1
    assert _per_job_thread_budget(None, 2) is None

    with pytest.raises(ValueError, match="must be positive"):
        _per_job_thread_budget(0, 2)


def test_ddp_experiments_own_all_visible_gpus_and_run_sequentially() -> None:
    assert _effective_parallel_jobs(4, "distributed_data_parallel") == 1
    assert _effective_parallel_jobs(2, "ddp") == 1
    assert _effective_parallel_jobs(4, "none") == 4


def test_parallel_scheduler_launches_two_independent_runs_and_splits_threads(
    tmp_path: Path,
    monkeypatch,
) -> None:
    spec = tmp_path / "parallel.yaml"
    spec.write_text(
        """
base_config: configs/markets/tw_day_trade_daily_no_default.yaml
expected_fold_count: 1
runtime:
  cpu_threads: 112
  torch_compile_threads: 16
matrix:
  include_baseline: true
  dimensions:
    - name: learning_rate
      enabled: true
      path: training.learning_rate
      values:
        - name: variant
          experiment_name: variant
          value: 0.0002
""",
        encoding="utf-8",
    )
    output_root = tmp_path / "output"

    class FakeProcess:
        next_pid = 90_000
        live = 0
        peak_live = 0
        commands: list[list[str]] = []

        def __init__(self, command, **_kwargs):
            type(self).next_pid += 1
            self.pid = type(self).next_pid
            self.returncode = None
            type(self).live += 1
            type(self).peak_live = max(type(self).peak_live, type(self).live)
            type(self).commands.append(list(command))

        def poll(self):
            if self.returncode is None:
                self.returncode = 0
                type(self).live -= 1
            return self.returncode

    monkeypatch.setattr(ablation_module.subprocess, "Popen", FakeProcess)
    monkeypatch.setattr(
        ablation_module.sys,
        "argv",
        [
            "run_ablation_experiments.py",
            "--spec",
            str(spec),
            "--output-root",
            str(output_root),
            "--runner",
            "/bin/true",
            "--parallel-jobs",
            "2",
            "--max-folds",
            "1",
        ],
    )

    ablation_module.main()

    assert FakeProcess.peak_live == 2
    assert len(FakeProcess.commands) == 2
    assert all("--cpu-threads" in command for command in FakeProcess.commands)
    assert all(
        command[command.index("--cpu-threads") + 1] == "56"
        for command in FakeProcess.commands
    )
    assert all(
        command[command.index("--torch-compile-threads") + 1] == "8"
        for command in FakeProcess.commands
    )
    summary = yaml.safe_load(
        (output_root / "summary.json").read_text(encoding="utf-8")
    )
    assert [row["status"] for row in summary] == ["succeeded", "succeeded"]


def test_scheduler_auto_resumes_failed_worker_until_success(
    tmp_path: Path,
    monkeypatch,
) -> None:
    spec = tmp_path / "retry.yaml"
    spec.write_text(
        """
base_config: configs/markets/tw_day_trade_daily_no_default.yaml
expected_fold_count: 1
matrix:
  include_baseline: false
  dimensions:
    - name: learning_rate
      enabled: true
      path: training.learning_rate
      values:
        - name: variant
          experiment_name: variant
          value: 0.0002
""",
        encoding="utf-8",
    )
    output_root = tmp_path / "output"

    class FakeProcess:
        next_pid = 91_000
        attempts = 0

        def __init__(self, _command, **_kwargs):
            type(self).next_pid += 1
            type(self).attempts += 1
            self.pid = type(self).next_pid
            self.returncode = None
            self.attempt = type(self).attempts

        def poll(self):
            if self.returncode is None:
                self.returncode = 1 if self.attempt == 1 else 0
            return self.returncode

    monkeypatch.setattr(ablation_module.subprocess, "Popen", FakeProcess)
    monkeypatch.setattr(
        ablation_module.sys,
        "argv",
        [
            "run_ablation_experiments.py",
            "--spec",
            str(spec),
            "--output-root",
            str(output_root),
            "--runner",
            "/bin/true",
            "--max-folds",
            "1",
            "--retry-backoff-seconds",
            "0",
        ],
    )

    ablation_module.main()

    assert FakeProcess.attempts == 2
    summary = yaml.safe_load(
        (output_root / "summary.json").read_text(encoding="utf-8")
    )
    assert summary[0]["status"] == "succeeded"
    assert summary[0]["attempts"] == 2


def test_sequential_scheduler_retries_current_experiment_before_next(
    tmp_path: Path,
    monkeypatch,
) -> None:
    spec = tmp_path / "retry_order.yaml"
    spec.write_text(
        """
base_config: configs/markets/tw_day_trade_daily_no_default.yaml
expected_fold_count: 1
matrix:
  include_baseline: false
  dimensions:
    - name: learning_rate
      enabled: true
      path: training.learning_rate
      values:
        - name: first
          experiment_name: first
          value: 0.0002
        - name: second
          experiment_name: second
          value: 0.0003
""",
        encoding="utf-8",
    )
    output_root = tmp_path / "output"

    class FailFirstAttemptProcess:
        next_pid = 91_500
        attempts: dict[str, int] = {}
        launch_order: list[str] = []

        def __init__(self, command, **_kwargs):
            type(self).next_pid += 1
            self.pid = type(self).next_pid
            self.returncode = None
            config_path = Path(command[command.index("-c") + 1])
            self.name = config_path.stem
            type(self).launch_order.append(self.name)
            type(self).attempts[self.name] = (
                type(self).attempts.get(self.name, 0) + 1
            )
            self.attempt = type(self).attempts[self.name]

        def poll(self):
            if self.returncode is None:
                self.returncode = (
                    1 if self.name == "first" and self.attempt == 1 else 0
                )
            return self.returncode

    monkeypatch.setattr(
        ablation_module.subprocess,
        "Popen",
        FailFirstAttemptProcess,
    )
    monkeypatch.setattr(
        ablation_module.sys,
        "argv",
        [
            "run_ablation_experiments.py",
            "--spec",
            str(spec),
            "--output-root",
            str(output_root),
            "--runner",
            "/bin/true",
            "--max-folds",
            "1",
            "--retry-backoff-seconds",
            "0",
        ],
    )

    ablation_module.main()

    assert FailFirstAttemptProcess.launch_order == ["first", "first", "second"]


def test_scheduler_fails_closed_after_consecutive_no_progress_limit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    spec = tmp_path / "retry_limit.yaml"
    spec.write_text(
        """
base_config: configs/markets/tw_day_trade_daily_no_default.yaml
expected_fold_count: 1
matrix:
  include_baseline: false
  dimensions:
    - name: learning_rate
      enabled: true
      path: training.learning_rate
      values:
        - name: variant
          experiment_name: variant
          value: 0.0002
""",
        encoding="utf-8",
    )
    output_root = tmp_path / "output"

    class AlwaysFailProcess:
        next_pid = 92_000
        attempts = 0

        def __init__(self, _command, **_kwargs):
            type(self).next_pid += 1
            type(self).attempts += 1
            self.pid = type(self).next_pid
            self.returncode = None

        def poll(self):
            self.returncode = 1
            return self.returncode

    monkeypatch.setattr(ablation_module.subprocess, "Popen", AlwaysFailProcess)
    monkeypatch.setattr(
        ablation_module.sys,
        "argv",
        [
            "run_ablation_experiments.py",
            "--spec",
            str(spec),
            "--output-root",
            str(output_root),
            "--runner",
            "/bin/true",
            "--max-folds",
            "1",
            "--max-no-progress-retries",
            "1",
            "--retry-backoff-seconds",
            "0",
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        ablation_module.main()

    assert exc_info.value.code == 1
    assert AlwaysFailProcess.attempts == 2
    summary = yaml.safe_load(
        (output_root / "summary.json").read_text(encoding="utf-8")
    )
    assert summary[0]["status"] == "failed"
    assert summary[0]["attempts"] == 2
    assert summary[0]["consecutive_no_progress_failures"] == 2


def test_failure_kind_distinguishes_cuda_infrastructure_from_oom() -> None:
    assert (
        _failure_kind(1, "CUDA initialization: CUDA unknown error")
        == "cuda_infrastructure_unavailable"
    )
    assert (
        _failure_kind(1, "open /dev/nvidia-uvm: Input/output error")
        == "cuda_infrastructure_unavailable"
    )
    assert _failure_kind(1, "CUDA out of memory") == "cuda_oom"
    assert (
        _failure_kind(1, "Checkpoint semantic fingerprint mismatch (data: saved=x)")
        == "checkpoint_contract_mismatch"
    )


def test_pinned_panel_cache_resolves_snapshot_receipt_and_checks_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = tmp_path / "snapshots"
    snapshot_id = "tw-public-test"
    variant_id = "a" * 64
    manifest_path = (
        store
        / snapshot_id
        / "stocks"
        / "panel_cache_v2"
        / "variants"
        / f"{variant_id}.json"
    )
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        '{"version": 51, "generation": "generation-v1", '
        '"source_hash": "source-v1"}',
        encoding="utf-8",
    )
    monkeypatch.setenv("STOCKAGENT_TW_PUBLIC_SNAPSHOT_STORE", str(store))
    spec = {
        "pinned_panel_cache": {
            "snapshot_id": snapshot_id,
            "variant_id": variant_id,
            "version": 51,
            "generation": "generation-v1",
            "source_hash": "source-v1",
        }
    }

    env = _resolve_pinned_panel_cache_env(spec)

    assert env["STOCKAGENT_PINNED_PANEL_CACHE_MANIFEST"] == str(
        manifest_path.resolve()
    )
    assert env["STOCKAGENT_PINNED_PANEL_CACHE_GENERATION"] == "generation-v1"
    spec["pinned_panel_cache"]["generation"] = "wrong-generation"
    with pytest.raises(ValueError, match="identity mismatch"):
        _resolve_pinned_panel_cache_env(spec)


def test_cuda_infrastructure_wait_does_not_consume_retry_budget(
    tmp_path: Path,
    monkeypatch,
) -> None:
    spec = tmp_path / "cuda_wait.yaml"
    spec.write_text(
        """
base_config: configs/markets/tw_day_trade_daily_no_default.yaml
expected_fold_count: 1
matrix:
  include_baseline: false
  dimensions:
    - name: learning_rate
      enabled: true
      path: training.learning_rate
      values:
        - name: variant
          experiment_name: variant
          value: 0.0002
""",
        encoding="utf-8",
    )
    output_root = tmp_path / "output"

    class CudaFailThenSucceedProcess:
        next_pid = 93_000
        attempts = 0

        def __init__(self, _command, **kwargs):
            type(self).next_pid += 1
            type(self).attempts += 1
            self.pid = type(self).next_pid
            self.returncode = None
            self.attempt = type(self).attempts
            if self.attempt == 1:
                kwargs["stdout"].write(
                    "CUDA initialization: CUDA unknown error\n"
                )
                kwargs["stdout"].flush()

        def poll(self):
            if self.returncode is None:
                self.returncode = 1 if self.attempt == 1 else 0
            return self.returncode

    health_probes = 0

    def _healthy_after_failure():
        nonlocal health_probes
        health_probes += 1
        return True, "healthy CUDA devices=2"

    monkeypatch.setattr(
        ablation_module.subprocess, "Popen", CudaFailThenSucceedProcess
    )
    monkeypatch.setattr(ablation_module, "_cuda_runtime_health", _healthy_after_failure)
    monkeypatch.setattr(
        ablation_module.sys,
        "argv",
        [
            "run_ablation_experiments.py",
            "--spec",
            str(spec),
            "--output-root",
            str(output_root),
            "--runner",
            "/bin/true",
            "--max-folds",
            "1",
            "--max-no-progress-retries",
            "0",
        ],
    )

    ablation_module.main()

    assert CudaFailThenSucceedProcess.attempts == 2
    assert health_probes == 1
    summary = yaml.safe_load(
        (output_root / "summary.json").read_text(encoding="utf-8")
    )
    assert summary[0]["status"] == "succeeded"
    assert summary[0]["attempts"] == 2
    assert summary[0]["consecutive_no_progress_failures"] == 0


def test_experiment_rows_filter_and_validate_names(tmp_path: Path) -> None:
    spec = tmp_path / "ablations.yaml"
    spec.write_text(
        """
base_config: configs/markets/tw_public.yaml
matrix:
  mode: one_factor_at_a_time
  include_baseline: true
  dimensions:
    - name: dropout
      enabled: true
      path: training.dropout
      values:
        - name: disabled
          experiment_name: no_dropout
          value: 0
""",
        encoding="utf-8",
    )

    _, rows = _experiment_rows(spec, {"no_dropout"})
    assert [row["name"] for row in rows] == ["no_dropout"]

    with pytest.raises(ValueError, match="unknown experiments"):
        _experiment_rows(spec, {"missing"})


def test_experiment_rows_expand_coupled_variant(tmp_path: Path) -> None:
    spec = tmp_path / "ablations.yaml"
    spec.write_text(
        """
base_config: configs/markets/tw_public.yaml
matrix:
  dimensions:
    - name: bottleneck
      enabled: true
      variants:
        - name: temporal
          experiment_name: temporal_only
          overrides:
            training:
              model:
                latent: false
                market: false
""",
        encoding="utf-8",
    )

    _, rows = _experiment_rows(spec)
    assert [row["name"] for row in rows] == ["baseline", "temporal_only"]
    assert rows[1]["overrides"]["training"]["model"] == {
        "latent": False,
        "market": False,
    }


def test_experiment_rows_expand_paired_discrete_values(tmp_path: Path) -> None:
    spec = tmp_path / "ablations.yaml"
    spec.write_text(
        """
base_config: configs/markets/tw_public.yaml
matrix:
  dimensions:
    - name: lookback_batch
      enabled: true
      paths: [training.lookback, training.batch_size_train]
      values:
        - name: lb256_bs32
          value: [256, 32]
          overrides:
            training:
              batch_size_eval: 16
""",
        encoding="utf-8",
    )

    _, rows = _experiment_rows(spec)
    assert rows[1]["overrides"]["training"] == {
        "lookback": 256,
        "batch_size_train": 32,
        "batch_size_eval": 16,
    }


def test_fold_status_uses_requested_fold_range(tmp_path: Path) -> None:
    (tmp_path / "fold_02").mkdir()
    (tmp_path / "fold_02" / "fold_complete.json").write_text("{}", encoding="utf-8")
    (tmp_path / "fold_04").mkdir()
    (tmp_path / "fold_04" / "fold_complete.json").write_text("{}", encoding="utf-8")

    assert _fold_status(tmp_path, 2, 3) == (2, 3)


def test_fold_status_does_not_assume_unbounded_partial_run_is_complete(
    tmp_path: Path,
) -> None:
    (tmp_path / "fold_01").mkdir()
    (tmp_path / "fold_01" / "fold_complete.json").write_text("{}", encoding="utf-8")
    (tmp_path / "fold_02").mkdir()
    (tmp_path / "fold_02" / "fold_complete.json").write_text("{}", encoding="utf-8")

    assert _fold_status(tmp_path, None, None) == (2, None)
    assert _format_fold_status(2, None) == "2/?"


def test_fold_status_uses_declared_matrix_fold_count_for_fast_resume(
    tmp_path: Path,
) -> None:
    for fold in range(8, 21):
        fold_dir = tmp_path / f"fold_{fold:02d}"
        fold_dir.mkdir()
        (fold_dir / "fold_complete.json").write_text("{}", encoding="utf-8")

    assert _fold_status(tmp_path, 8, None, 20) == (13, 13)
    assert _fold_status(tmp_path, None, None, 20) == (13, 20)


def test_active_ablation_matrix_uses_bounded_dynamic_stock_axis(
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    spec_path = repo_root / "configs/ablations/transformer_base_portfolio.yaml"
    spec, experiments = _experiment_rows(
        spec_path,
        {"baseline", "lookback256_batch32", "mean_pooling", "attention_pooling"},
    )

    runs = _build_configs(spec_path, spec, experiments, tmp_path)
    effective = {
        run["name"]: yaml.safe_load(run["config_path"].read_text(encoding="utf-8"))
        for run in runs
    }

    for name in ("baseline", "lookback256_batch32"):
        raw = effective[name]
        assert raw["runner"]["post_train_infer"] is False
        assert raw["training"]["compile_model_dynamic_symbols"] is True
        assert raw["training"]["compile_loss_dynamic_symbols"] is True
        assert raw["training"]["train_symbol_compaction"] == "train_union"
        assert raw["training"]["train_symbol_compaction_bucket_size"] == 0
        assert raw["walk_forward"]["split_start_year"] == 2006
        assert raw["walk_forward"]["lookback_context"] == "panel_history"

    assert effective["baseline"]["training"]["lookback"] == 32
    assert effective["lookback256_batch32"]["training"]["lookback"] == 256
    mean_training = effective["mean_pooling"]["training"]
    assert mean_training["compile_model_dynamic_symbols"] is False
    assert mean_training["compile_loss_dynamic_symbols"] is False
    assert mean_training["train_symbol_compaction_bucket_size"] == 512
    assert mean_training["transformer_base_portfolio"]["temporal_pooling"] == "mean"
    attention_training = effective["attention_pooling"]["training"]
    assert attention_training["compile_model_dynamic_symbols"] is False
    assert attention_training["compile_loss_dynamic_symbols"] is False
    assert attention_training["train_symbol_compaction_bucket_size"] == 512
    assert attention_training["transformer_base_portfolio"]["temporal_pooling"] == "attention"


def test_tw_day_trade_unified_matrix_keeps_projection_control_except_output_modes(
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    spec_path = (
        repo_root / "configs/ablations/financial_transformer_tw_day_trade.yaml"
    )
    spec, experiments = _experiment_rows(spec_path)
    assert spec["output_root"].endswith("capital10m_panel_history_v4")
    expected_names = [
        "baseline",
        "lookback256_batch32",
        "latent_only",
        "market_only",
        "temporal_only",
        "no_rope_temporal",
        "no_time_position",
        "with_symbol_position",
        "no_qk_norm",
        "mean_pooling",
        "attention_pooling",
        "layernorm",
        "gelu_ffn",
        "initial_capital_1m",
        "initial_capital_100m",
        "output_activation_l1",
        "output_l1",
        "output_logits",
        "output_signed_softmax",
        "output_signed_entmax15",
        "output_signed_sparsemax",
    ]
    assert [row["name"] for row in experiments] == expected_names

    runs = _build_configs(spec_path, spec, experiments, tmp_path)
    effective = {
        run["name"]: yaml.safe_load(run["config_path"].read_text(encoding="utf-8"))
        for run in runs
    }
    for raw in effective.values():
        assert raw["trading"]["execution_mode"] == "tw_day_trade"
        assert raw["trading"]["long_only"] is False
        assert raw["training"]["loss_type"] == "log_utility"
        assert raw["walk_forward"]["lookback_context"] == "panel_history"

    output_modes = {
        "output_activation_l1": "activation_l1",
        "output_l1": "l1",
        "output_logits": "logits",
        "output_signed_softmax": "signed_softmax",
        "output_signed_entmax15": "signed_entmax15",
        "output_signed_sparsemax": "signed_sparsemax",
    }
    architecture_names = set(expected_names) - set(output_modes)
    for name in architecture_names:
        raw = effective[name]
        assert (
            raw["training"]["financial_transformer"]["portfolio_output_mode"]
            == "projection_l1"
        )
        assert raw["trading"]["portfolio_activation"] == "pre_normalized"
        assert raw["training"]["loss_portfolio_activation"] == "pre_normalized"

    for name, output_mode in output_modes.items():
        assert (
            effective[name]["training"]["financial_transformer"][
                "portfolio_output_mode"
            ]
            == output_mode
        )

    assert effective["baseline"]["training"]["lookback"] == 32
    assert effective["baseline"]["training"]["batch_size_train"] == 128
    assert effective["lookback256_batch32"]["training"]["lookback"] == 256
    assert (
        effective["lookback256_batch32"]["training"]["batch_size_train"] == 32
    )
    assert effective["baseline"]["trading"]["volume_participation_equity"] == 10_000_000.0
    assert (
        effective["initial_capital_1m"]["trading"]["volume_participation_equity"]
        == 1_000_000.0
    )
    assert (
        effective["initial_capital_100m"]["trading"]["volume_participation_equity"]
        == 100_000_000.0
    )
    assert {
        raw["trading"]["volume_participation_equity"]
        for name, raw in effective.items()
        if name not in {"initial_capital_1m", "initial_capital_100m"}
    } == {10_000_000.0}
    assert effective["initial_capital_1m"]["trading"]["tw_short_initial_margin_rate"] == 0.9
    assert effective["initial_capital_100m"]["trading"]["tw_short_initial_margin_rate"] == 0.9


def test_tw_day_trade_output_mode_matrix_uses_ten_million_for_every_run(
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    spec_path = (
        repo_root
        / "configs/ablations/financial_transformer_tw_day_trade_output_modes.yaml"
    )
    spec, experiments = _experiment_rows(spec_path)
    runs = _build_configs(spec_path, spec, experiments, tmp_path)

    assert spec["output_root"].endswith("projection_l1_capital10m_panel_history_v4")
    for run in runs:
        raw = yaml.safe_load(run["config_path"].read_text(encoding="utf-8"))
        assert raw["trading"]["execution_mode"] == "tw_day_trade"
        assert raw["trading"]["volume_participation_equity"] == 10_000_000.0
        assert raw["walk_forward"]["lookback_context"] == "panel_history"


def test_tw_day_trade_mixed_batch_matrix_resolves_only_measured_oom_variants(
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    spec_path = (
        repo_root
        / "configs/ablations/tw_day_trade_daily_tplus2_close_commission20_v3_mixed_batch.yaml"
    )
    spec, experiments = _experiment_rows(spec_path)
    assert len(experiments) == 20
    assert spec["pinned_panel_cache"]["generation"] == (
        "0287b07e62da4030967877bd9b3e3bac"
    )

    runs = _build_configs(spec_path, spec, experiments, tmp_path)
    effective = {
        run["name"]: yaml.safe_load(run["config_path"].read_text(encoding="utf-8"))
        for run in runs
    }
    batches = {
        name: int(raw["training"]["batch_size_train"])
        for name, raw in effective.items()
    }
    assert batches["lookback128_batch256"] == 256
    assert batches["mean_pooling"] == 128
    assert batches["attention_pooling"] == 128
    assert {
        batch
        for name, batch in batches.items()
        if name not in {"lookback128_batch256", "mean_pooling", "attention_pooling"}
    } == {512}

    for name, raw in effective.items():
        assert raw["trading"]["execution_mode"] == "tw_day_trade"
        assert raw["trading"]["frequency"] == "daily"
        assert raw["walk_forward"]["lookback_context"] == "panel_history"
        assert raw["training"]["auto_batch_size"] is False
        assert raw["training"]["epochs"] == 1000
        assert raw["training"]["record_epoch_curve"] is True
        assert raw["training"]["curve_plot_interval"] == 1
        assert raw["training"]["curve_plot_async"] is True
        assert raw["training"]["defer_epoch_curve_plot_until_end"] is False
    assert "baseline_artifact_root" not in spec
    assert spec["output_root"].endswith(
        "v4_ofat_mixed_batch_panel_history_capital10m"
    )
    assert effective["initial_capital_1m"]["trading"]["volume_participation_equity"] == 1_000_000.0
    assert effective["initial_capital_100m"]["trading"]["volume_participation_equity"] == 100_000_000.0
    assert {
        raw["trading"]["volume_participation_equity"]
        for name, raw in effective.items()
        if name not in {"initial_capital_1m", "initial_capital_100m"}
    } == {10_000_000.0}
    assert (
        effective["mean_pooling"]["training"]["financial_transformer"][
            "temporal_query_mode"
        ]
        == "full_then_last"
    )
    assert (
        effective["attention_pooling"]["training"]["financial_transformer"][
            "temporal_query_mode"
        ]
        == "full_then_last"
    )
