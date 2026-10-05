from __future__ import annotations

import json
from types import SimpleNamespace

import numpy as np
import pytest
import torch

import stockagent.training.trainer as trainer_module
from stockagent.config import load_config
from stockagent.models.factory import build_model
from stockagent.models.temporal_basis_fit import (
    build_temporal_basis_metadata,
    fit_training_only_pca_klt,
    temporal_basis_effective_rank_profile,
    temporal_basis_overrides_from_state_dict,
    training_temporal_covariance,
)
from stockagent.models.transformer_base_portfolio import (
    FIXED_GRID_TEMPORAL_BASIS_FAMILIES,
    ONLINE_SAFE_TEMPORAL_BASIS_FAMILIES,
    TemporalBasisFeatureEncoder,
    _fixed_temporal_basis_candidates,
    _temporal_basis_matrix,
)
from stockagent.training.trainer import _fit_group_temporal_basis


@pytest.mark.parametrize(
    ("family", "expected_count"),
    [
        ("kautz", 40),
        ("discrete_hermite", 24),
        ("chirplet", 48),
    ],
)
def test_new_fixed_candidate_grids_remove_dc_and_l2_normalize(
    family: str,
    expected_count: int,
) -> None:
    candidates = _fixed_temporal_basis_candidates(
        family,
        steps=32,
        components_hint=4,
    )

    assert len(candidates) == expected_count
    matrix = torch.stack(candidates)
    torch.testing.assert_close(
        matrix.sum(dim=1),
        torch.zeros(expected_count, dtype=torch.float64),
        rtol=0.0,
        atol=1e-10,
    )
    torch.testing.assert_close(
        matrix.norm(dim=1),
        torch.ones(expected_count, dtype=torch.float64),
        rtol=1e-10,
        atol=1e-10,
    )


@pytest.mark.parametrize(
    "family",
    tuple(
        family
        for family in (
            *ONLINE_SAFE_TEMPORAL_BASIS_FAMILIES,
            *FIXED_GRID_TEMPORAL_BASIS_FAMILIES,
        )
        if family != "learned"
    ),
)
def test_every_fixed_candidate_is_non_dc_and_l2_normalized(family: str) -> None:
    candidates = _fixed_temporal_basis_candidates(
        family,
        steps=32,
        components_hint=4,
    )

    assert candidates
    matrix = torch.stack(candidates)
    torch.testing.assert_close(
        matrix.sum(dim=1),
        torch.zeros(len(candidates), dtype=torch.float64),
        rtol=0.0,
        atol=2e-10,
    )
    torch.testing.assert_close(
        matrix.norm(dim=1),
        torch.ones(len(candidates), dtype=torch.float64),
        rtol=2e-10,
        atol=2e-10,
    )


@pytest.mark.parametrize("lookback", [16, 32])
@pytest.mark.parametrize(
    "family",
    ["kautz", "discrete_hermite", "chirplet"],
)
def test_new_fixed_banks_follow_configured_lookback(
    family: str,
    lookback: int,
) -> None:
    matrix = _temporal_basis_matrix(family, steps=lookback, components=4).double()

    assert matrix.shape == (4, lookback)
    torch.testing.assert_close(
        matrix @ matrix.transpose(0, 1),
        torch.eye(4, dtype=torch.float64),
        rtol=2e-6,
        atol=2e-6,
    )
    torch.testing.assert_close(
        matrix.sum(dim=1),
        torch.zeros(4, dtype=torch.float64),
        rtol=0.0,
        atol=2e-6,
    )


def test_pca_klt_uses_only_training_windows_and_has_lookback_minus_one_candidates() -> None:
    generator = np.random.default_rng(713)
    features = generator.normal(size=(64, 3, 2)).astype(np.float32)
    training_targets = np.arange(12, 31, dtype=np.int64)

    fitted = fit_training_only_pca_klt(
        features,
        training_targets,
        lookback=8,
        feature_lag=1,
        components=4,
    )
    changed_outside_training = features.copy()
    changed_outside_training[31:] += 10_000.0
    refitted = fit_training_only_pca_klt(
        changed_outside_training,
        training_targets,
        lookback=8,
        feature_lag=1,
        components=4,
    )

    torch.testing.assert_close(fitted.covariance, refitted.covariance)
    torch.testing.assert_close(fitted.basis, refitted.basis)
    assert fitted.metadata["scope"] == "fold_training_windows_only"
    assert fitted.metadata["validation_rows_used"] == 0
    assert fitted.metadata["test_rows_used"] == 0
    assert fitted.metadata["candidate_count"] == 7
    assert fitted.metadata["selected_count"] == 4
    assert fitted.basis.shape == (4, 8)
    torch.testing.assert_close(
        fitted.basis.double() @ fitted.basis.double().transpose(0, 1),
        torch.eye(4, dtype=torch.float64),
        rtol=2e-6,
        atol=2e-6,
    )
    torch.testing.assert_close(
        fitted.basis.double().sum(dim=1),
        torch.zeros(4, dtype=torch.float64),
        rtol=0.0,
        atol=2e-6,
    )
    assert fitted.eigenvalues.tolist() == sorted(
        fitted.eigenvalues.tolist(), reverse=True
    )


def test_training_temporal_covariance_matches_direct_training_windows() -> None:
    generator = np.random.default_rng(777)
    features = generator.normal(size=(18, 3, 2)).astype(np.float32)
    features[3, 1, 1] = np.nan
    targets = np.asarray([7, 9, 12, 14], dtype=np.int64)
    lookback = 5
    feature_lag = 1

    actual, metadata = training_temporal_covariance(
        features,
        targets,
        lookback=lookback,
        feature_lag=feature_lag,
        feature_chunk_columns=2,
    )
    observations = []
    clean = np.nan_to_num(features, nan=0.0, posinf=0.0, neginf=0.0)
    for target in targets:
        end = int(target) + 1 - feature_lag
        observations.append(
            clean[end - lookback : end].transpose(1, 2, 0).reshape(-1, lookback)
        )
    direct = torch.from_numpy(np.concatenate(observations, axis=0)).double()
    expected = torch.cov(direct.transpose(0, 1), correction=1)
    dc = torch.ones(lookback, dtype=torch.float64) / np.sqrt(float(lookback))
    projector = torch.eye(lookback, dtype=torch.float64) - torch.outer(dc, dc)
    expected = projector @ expected @ projector

    torch.testing.assert_close(actual, expected, rtol=2e-6, atol=2e-6)
    assert metadata["observation_count"] == len(targets) * 3 * 2


@pytest.mark.parametrize("dtype", [np.float32, np.float64])
@pytest.mark.parametrize("lookback", [2, 8, 32])
@pytest.mark.parametrize("lag_batch_size", [2, 8, 32])
def test_batched_temporal_lags_preserve_covariance_and_training_scope(
    dtype, lookback, lag_batch_size,
) -> None:
    features = np.random.default_rng(482).normal(size=(85, 3, 13)).astype(dtype)
    features[20, 1, 3] = np.nan
    features[25, 2, 4] = np.inf
    features[29, 0, 5] = -np.inf
    targets = np.array([lookback, lookback + 3, 57, 69, 69], dtype=np.int64)
    kwargs = dict(lookback=lookback, feature_lag=1, feature_chunk_columns=7)
    expected, scope = training_temporal_covariance(features, targets, **kwargs)
    actual, metadata = training_temporal_covariance(
        features, targets, **kwargs, lag_batch_size=lag_batch_size,
    )
    torch.testing.assert_close(actual, expected, rtol=1e-12, atol=1e-12)
    assert metadata == scope
    features[69:] += 50_000  # Validation/test data cannot affect the fit.
    outside_changed, changed_metadata = training_temporal_covariance(
        features, targets, **kwargs, lag_batch_size=lag_batch_size,
    )
    torch.testing.assert_close(outside_changed, actual, rtol=0, atol=0)
    assert changed_metadata == scope


def test_batched_temporal_lags_reject_zero_batch_and_preserve_pca_bank() -> None:
    features = np.random.default_rng(519).normal(size=(65, 3, 5)).astype(np.float32)
    targets = np.arange(32, 60)
    kwargs = dict(lookback=32, feature_lag=1, components=31)
    with pytest.raises(ValueError, match="lag_batch_size must be positive"):
        fit_training_only_pca_klt(features, targets, **kwargs, lag_batch_size=0)
    expected = fit_training_only_pca_klt(features, targets, **kwargs)
    actual = fit_training_only_pca_klt(features, targets, **kwargs, lag_batch_size=32)
    torch.testing.assert_close(actual.covariance, expected.covariance, rtol=1e-12, atol=1e-12)
    torch.testing.assert_close(actual.eigenvalues, expected.eigenvalues, rtol=1e-12, atol=1e-12)
    torch.testing.assert_close(actual.basis, expected.basis, rtol=1e-6, atol=1e-6)
    assert actual.metadata == expected.metadata


def test_pca_klt_requires_training_override_and_round_trips_in_state_dict() -> None:
    assert temporal_basis_overrides_from_state_dict(None) == {}
    with pytest.raises(ValueError, match="fold-training covariance override"):
        _temporal_basis_matrix("pca_klt", steps=8, components=3)

    generator = np.random.default_rng(817)
    features = generator.normal(size=(40, 2, 3)).astype(np.float32)
    fitted = fit_training_only_pca_klt(
        features,
        np.arange(10, 28),
        lookback=8,
        feature_lag=0,
        components=3,
    )
    encoder = TemporalBasisFeatureEncoder(
        lookback=8,
        dim=4,
        families=("pca_klt",),
        components=3,
        basis_overrides={"pca_klt": fitted.basis},
    )
    state = encoder.state_dict()
    overrides = temporal_basis_overrides_from_state_dict(state)
    restored = TemporalBasisFeatureEncoder(
        lookback=8,
        dim=4,
        families=("pca_klt",),
        components=3,
        basis_overrides=overrides,
    )
    restored.load_state_dict(state, strict=True)

    assert "pca_klt_basis" in state
    torch.testing.assert_close(restored.pca_klt_basis, encoder.pca_klt_basis)


def test_metadata_reports_rank_novelty_variance_and_candidate_counts() -> None:
    generator = np.random.default_rng(919)
    features = generator.normal(size=(72, 2, 3)).astype(np.float32)
    fitted = fit_training_only_pca_klt(
        features,
        np.arange(16, 48),
        lookback=12,
        feature_lag=1,
        components=4,
    )
    metadata = build_temporal_basis_metadata(
        ("kautz", "discrete_hermite", "chirplet", "pca_klt"),
        lookback=12,
        components=4,
        covariance=fitted.covariance,
        basis_overrides={"pca_klt": fitted.basis},
        pca_metadata=fitted.metadata,
    )

    assert metadata["selection_policy"] == "legacy_per_family_component_count"
    assert metadata["candidate_counts"] == {
        "kautz": 40,
        "discrete_hermite": 24,
        "chirplet": 48,
        "pca_klt": 11,
    }
    assert metadata["selected_total"] == 16
    assert 1 <= metadata["actual_rank"] <= 11
    assert metadata["near_duplicate_count"] == (
        metadata["selected_total"] - metadata["actual_rank"]
    )
    assert all("parameters" in row for row in metadata["selected"])
    assert all("rank_after" in row for row in metadata["selected"])
    assert all("novelty_ratio" in row for row in metadata["selected"])
    assert all(
        row["variance_contribution"] is not None
        for row in metadata["selected"]
    )


def test_fold_training_fit_writes_metadata_and_builds_checkpointable_model(
    tmp_path,
    monkeypatch,
) -> None:
    config = load_config("configs/markets/tw_public_multi_basis.yaml")
    config.training.financial_transformer.temporal_basis_families = [
        "kautz",
        "discrete_hermite",
        "chirplet",
        "pca_klt",
    ]
    config.training.financial_transformer.temporal_basis_components = 4
    config.training.temporal_basis_covariance_cpu_threads = 1
    config.training.temporal_basis_covariance_lag_batch_size = 8
    generator = np.random.default_rng(1021)
    train_dataset = SimpleNamespace(
        features_t=torch.from_numpy(
            generator.normal(size=(72, 3, 2)).astype(np.float32)
        ),
        valid_indices=np.arange(32, 56, dtype=np.int64),
        execution_mode="naive",
    )
    synchronized_phases: list[str] = []
    observed_fit = []
    original_fit = trainer_module.fit_training_only_pca_klt
    ambient_threads = torch.get_num_threads()
    def record_fit(*args, **kwargs):
        observed_fit.append((torch.get_num_threads(),kwargs["lag_batch_size"]))
        return original_fit(*args, **kwargs)
    monkeypatch.setattr(trainer_module,"fit_training_only_pca_klt",record_fit)
    original_phase_runner = trainer_module._run_rank0_store_synchronized_phase

    def _record_phase(phase, operation, **kwargs):
        synchronized_phases.append(phase)
        return original_phase_runner(phase, operation, **kwargs)

    monkeypatch.setattr(
        trainer_module,
        "_run_rank0_store_synchronized_phase",
        _record_phase,
    )

    overrides, metadata = _fit_group_temporal_basis(
        config=config,
        train_ds=train_dataset,
        train_years=[2014],
        group_folds=[
            SimpleNamespace(
                fold_id=3,
                train_indices=np.arange(32, 56, dtype=np.int64),
            )
        ],
        output_path=tmp_path,
    )
    assert observed_fit == [(1,8)]
    assert torch.get_num_threads() == ambient_threads

    assert metadata is not None
    assert synchronized_phases == ["temporal_basis_fit"]
    assert metadata["selection_policy"] == "legacy_per_family_component_count"
    assert metadata["pca_klt_training_only"] is True
    assert metadata["training_covariance"]["validation_rows_used"] == 0
    assert metadata["training_covariance"]["test_rows_used"] == 0
    assert overrides["pca_klt"].shape == (4, 32)
    fold_metadata_path = tmp_path / "fold_03" / "temporal_basis_selection.json"
    saved = json.loads(fold_metadata_path.read_text(encoding="utf-8"))
    assert saved["fold_id"] == 3
    assert saved["actual_rank"] == metadata["actual_rank"]

    model = build_model(
        config=config,
        lookback=32,
        num_features=2,
        num_symbols=3,
        feature_names=("feature_a", "feature_b"),
        temporal_basis_overrides=overrides,
    )
    state = model.state_dict()
    restored_overrides = temporal_basis_overrides_from_state_dict(state)
    torch.testing.assert_close(restored_overrides["pca_klt"], overrides["pca_klt"])


def test_all_22_unlimited_pools_have_expected_effective_ranks() -> None:
    families = (
        *ONLINE_SAFE_TEMPORAL_BASIS_FAMILIES,
        *FIXED_GRID_TEMPORAL_BASIS_FAMILIES,
        "pca_klt",
    )
    profile = temporal_basis_effective_rank_profile(
        families,
        lookback=32,
        novelty_threshold=1e-4,
    )
    expected = {
        "haar": (31, 31),
        "swt_db2": (14, 13),
        "swt_sym4": (10, 10),
        "wavelet_packet": (14, 14),
        "walsh": (31, 31),
        "fourier": (31, 31),
        "dct": (31, 31),
        "dpss": (32, 31),
        "local_cosine": (309, 31),
        "morlet": (30, 27),
        "exponential": (32, 8),
        "laguerre": (32, 13),
        "difference": (46, 31),
        "ar_innovation": (31, 8),
        "bspline": (32, 30),
        "legendre": (31, 31),
        "chebyshev": (31, 31),
        "learned": (31, 31),
        "kautz": (40, 23),
        "discrete_hermite": (24, 16),
        "chirplet": (48, 21),
        "pca_klt": (31, 31),
    }

    assert len(profile["families"]) == 22
    assert profile["candidate_total"] == 942
    assert profile["sum_family_effective_ranks"] == 524
    assert profile["combined_effective_rank"] == 31
    assert profile["combined_near_duplicate_count"] == 911
    assert {
        name: (values["candidate_count"], values["effective_rank"])
        for name, values in profile["families"].items()
    } == expected


def test_22_family_training_config_uses_each_effective_rank() -> None:
    config = load_config(
        "configs/markets/"
        "tw_day_trade_daily_multi_basis_22_effective_rank_projection_l1_"
        "tplus2_close_capital10m.yaml"
    )
    model_config = config.training.financial_transformer

    assert len(model_config.temporal_basis_families) == 22
    assert sum(model_config.temporal_basis_components_by_family.values()) == 524
    assert model_config.temporal_basis_novelty_threshold == pytest.approx(1e-4)
    assert config.training.batch_size_train == 16
    assert config.training.batch_size_eval == 16

    generator = np.random.default_rng(1223)
    pca = fit_training_only_pca_klt(
        generator.normal(size=(72, 3, 2)).astype(np.float32),
        np.arange(32, 56, dtype=np.int64),
        lookback=32,
        feature_lag=0,
        components=31,
    )
    model = build_model(
        config=config,
        lookback=32,
        num_features=2,
        num_symbols=3,
        feature_names=("feature_a", "feature_b"),
        temporal_basis_overrides={"pca_klt": pca.basis},
    )
    builder = model.temporal_basis_input_feature_builder
    assert builder is not None
    assert builder.total_basis_components == 524
    assert builder.family_component_counts == (
        model_config.temporal_basis_components_by_family
    )
