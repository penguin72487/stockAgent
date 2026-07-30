from pathlib import Path

import pytest

from scripts.run_ablation_experiments import (
    _deep_merge,
    _experiment_rows,
    _fold_status,
)


def test_deep_merge_preserves_unmodified_nested_values() -> None:
    base = {"training": {"epochs": 10, "model": {"dropout": 0.1, "layers": 2}}}
    result = _deep_merge(base, {"training": {"model": {"dropout": 0.0}}})

    assert result == {
        "training": {"epochs": 10, "model": {"dropout": 0.0, "layers": 2}}
    }
    assert base["training"]["model"]["dropout"] == 0.1


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
        - {name: lb256_bs32, value: [256, 32]}
""",
        encoding="utf-8",
    )

    _, rows = _experiment_rows(spec)
    assert rows[1]["overrides"]["training"] == {
        "lookback": 256,
        "batch_size_train": 32,
    }


def test_fold_status_uses_requested_fold_range(tmp_path: Path) -> None:
    (tmp_path / "fold_02").mkdir()
    (tmp_path / "fold_02" / "fold_complete.json").write_text("{}", encoding="utf-8")
    (tmp_path / "fold_04").mkdir()
    (tmp_path / "fold_04" / "fold_complete.json").write_text("{}", encoding="utf-8")

    assert _fold_status(tmp_path, 2, 3) == (2, 3)
