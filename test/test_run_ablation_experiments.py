from pathlib import Path

import pytest
import yaml

import scripts.run_ablation_experiments as ablation_module
from scripts.run_ablation_experiments import (
    _build_configs,
    _deep_merge,
    _effective_parallel_jobs,
    _experiment_rows,
    _fold_status,
    _format_fold_status,
    _per_job_thread_budget,
)


def test_deep_merge_preserves_unmodified_nested_values() -> None:
    base = {"training": {"epochs": 10, "model": {"dropout": 0.1, "layers": 2}}}
    result = _deep_merge(base, {"training": {"model": {"dropout": 0.0}}})

    assert result == {
        "training": {"epochs": 10, "model": {"dropout": 0.0, "layers": 2}}
    }
    assert base["training"]["model"]["dropout"] == 0.1


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
        "initial_capital_10m",
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
    assert effective["baseline"]["trading"]["volume_participation_equity"] == 1_000_000.0
    assert (
        effective["initial_capital_10m"]["trading"]["volume_participation_equity"]
        == 10_000_000.0
    )
    assert (
        effective["initial_capital_100m"]["trading"]["volume_participation_equity"]
        == 100_000_000.0
    )
    assert effective["initial_capital_10m"]["trading"]["tw_short_initial_margin_rate"] == 0.9
    assert effective["initial_capital_100m"]["trading"]["tw_short_initial_margin_rate"] == 0.9


def test_tw_day_trade_mixed_batch_matrix_resolves_only_measured_oom_variants(
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    spec_path = (
        repo_root
        / "configs/ablations/tw_day_trade_daily_tplus2_close_commission20_v3_mixed_batch.yaml"
    )
    spec, experiments = _experiment_rows(spec_path)
    assert len(experiments) == 17

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
        assert raw["training"]["auto_batch_size"] is False
        assert raw["training"]["epochs"] == 1000
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
