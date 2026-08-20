from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import math
from typing import Any

import numpy as np
import torch

from stockagent.models.transformer_base_portfolio import (
    _fixed_temporal_basis_candidates,
    _normalize_temporal_basis_families,
    _orthonormalize_non_dc_rows,
    _select_novel_temporal_basis_rows,
    _temporal_basis_matrix,
    _validated_temporal_basis_override,
    temporal_basis_candidate_parameters,
)


TEMPORAL_BASIS_METADATA_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class TemporalPCAFit:
    basis: torch.Tensor
    covariance: torch.Tensor
    eigenvalues: torch.Tensor
    metadata: dict[str, Any]


def training_temporal_covariance(
    features: np.ndarray | torch.Tensor,
    training_target_indices: np.ndarray | torch.Tensor | Sequence[int],
    *,
    lookback: int,
    feature_lag: int,
    feature_chunk_columns: int = 4_096,
) -> tuple[torch.Tensor, dict[str, Any]]:
    """Estimate ``[L,L]`` covariance from training windows and nothing else."""

    lookback = int(lookback)
    feature_lag = int(feature_lag)
    if lookback < 2:
        raise ValueError("PCA/KLT temporal basis requires lookback >= 2")
    if feature_lag < 0:
        raise ValueError("feature_lag must be non-negative")
    values = torch.as_tensor(features).detach().to(device="cpu")
    if values.ndim != 3:
        raise ValueError("features must have shape [T,S,F]")
    indices = torch.as_tensor(training_target_indices, dtype=torch.long).reshape(-1)
    indices = torch.unique(indices, sorted=True)
    if int(indices.numel()) == 0:
        raise ValueError("PCA/KLT requires at least one training target index")
    feature_ends = indices + 1 - feature_lag
    starts = feature_ends - lookback
    if int(starts.min().item()) < 0 or int(feature_ends.max().item()) > int(
        values.size(0)
    ):
        raise ValueError("training target indices cannot form the configured lookback")

    # The direct construction has N_train * S * F observations, each with L
    # columns, and would repeat the same overlapping daily dot products L times.
    # Compute every temporal lag once, then gather only the training-owned
    # window starts. This is exactly the same covariance but O(L), not O(L^2),
    # passes over the large symbol-feature axis.
    source_min = int(starts.min().item())
    source_max = int(feature_ends.max().item())
    source = values[source_min:source_max].reshape(source_max - source_min, -1)
    source_rows = int(source.size(0))
    flattened_features = int(source.size(1))
    if flattened_features <= 0:
        raise ValueError("PCA/KLT temporal covariance has no feature observations")
    accumulation_dtype = (
        torch.float64 if source.dtype == torch.float64 else torch.float32
    )
    daily_sum = torch.zeros(source_rows, dtype=torch.float64)
    lag_products = torch.zeros((lookback, source_rows), dtype=torch.float64)
    column_chunk = max(1, int(feature_chunk_columns))
    for column_start in range(0, flattened_features, column_chunk):
        block = source[:, column_start : column_start + column_chunk].to(
            dtype=accumulation_dtype
        )
        block = torch.nan_to_num(block, nan=0.0, posinf=0.0, neginf=0.0)
        daily_sum += block.sum(dim=1, dtype=torch.float64)
        for lag in range(lookback):
            width = source_rows - lag
            if width <= 0:
                break
            left = block[:width]
            right = block[lag : lag + width]
            lag_products[lag, :width] += (left * right).sum(
                dim=1,
                dtype=torch.float64,
            )

    relative_starts = starts - source_min
    sum_vector = torch.empty(lookback, dtype=torch.float64)
    cross_product = torch.empty((lookback, lookback), dtype=torch.float64)
    for left_offset in range(lookback):
        left_indices = relative_starts + left_offset
        sum_vector[left_offset] = daily_sum[left_indices].sum()
        for right_offset in range(left_offset, lookback):
            lag = right_offset - left_offset
            value = lag_products[lag, left_indices].sum()
            cross_product[left_offset, right_offset] = value
            cross_product[right_offset, left_offset] = value
    observation_count = int(indices.numel()) * flattened_features

    if observation_count < 2:
        raise ValueError("PCA/KLT temporal covariance needs at least two observations")
    mean = sum_vector / float(observation_count)
    covariance = (
        cross_product - float(observation_count) * torch.outer(mean, mean)
    ) / float(observation_count - 1)
    covariance = 0.5 * (covariance + covariance.transpose(0, 1))

    dc = torch.ones(lookback, dtype=torch.float64) / math.sqrt(float(lookback))
    projector = torch.eye(lookback, dtype=torch.float64) - torch.outer(dc, dc)
    covariance = projector @ covariance @ projector
    covariance = 0.5 * (covariance + covariance.transpose(0, 1))

    index_bytes = indices.numpy().astype("<i8", copy=False).tobytes()
    metadata = {
        "scope": "fold_training_windows_only",
        "validation_rows_used": 0,
        "test_rows_used": 0,
        "lookback": lookback,
        "feature_lag": feature_lag,
        "training_target_count": int(indices.numel()),
        "training_target_min": int(indices.min().item()),
        "training_target_max": int(indices.max().item()),
        "training_target_indices_sha256": hashlib.sha256(index_bytes).hexdigest(),
        "training_window_source_min": int(starts.min().item()),
        "training_window_source_max": int(feature_ends.max().item() - 1),
        "observation_count": int(observation_count),
    }
    return covariance, metadata


def fit_training_only_pca_klt(
    features: np.ndarray | torch.Tensor,
    training_target_indices: np.ndarray | torch.Tensor | Sequence[int],
    *,
    lookback: int,
    feature_lag: int,
    components: int,
) -> TemporalPCAFit:
    """Fit a non-DC KLT bank, ordered by fold-training eigenvalue."""

    lookback = int(lookback)
    components = int(components)
    if components < 1:
        raise ValueError("temporal_basis_components must be positive")
    covariance, scope_metadata = training_temporal_covariance(
        features,
        training_target_indices,
        lookback=lookback,
        feature_lag=feature_lag,
    )
    _eigenvalues, eigenvectors = torch.linalg.eigh(covariance)
    ordered_vectors = [
        row
        for row in eigenvectors.transpose(0, 1).flip(0)
    ]
    rows = _orthonormalize_non_dc_rows(ordered_vectors, steps=lookback)
    if len(rows) != lookback - 1:
        raise RuntimeError(
            "PCA/KLT failed to produce the complete non-DC temporal subspace "
            f"({len(rows)} != {lookback - 1})"
        )
    complete = torch.stack(rows, dim=0)
    rayleigh = torch.einsum("ki,ij,kj->k", complete, covariance, complete)
    order = torch.argsort(rayleigh, descending=True, stable=True)
    complete = complete[order]
    rayleigh = rayleigh[order].clamp_min(0.0)
    keep = min(components, lookback - 1)
    trace = float(torch.trace(covariance).clamp_min(0.0).item())
    metadata = {
        **scope_metadata,
        "family": "pca_klt",
        "candidate_count": int(lookback - 1),
        "selected_count": int(keep),
        "covariance_trace": trace,
        "eigenvalues_descending": [float(value) for value in rayleigh.tolist()],
    }
    return TemporalPCAFit(
        basis=complete[:keep].to(dtype=torch.float32),
        covariance=covariance,
        eigenvalues=rayleigh,
        metadata=metadata,
    )


def temporal_basis_overrides_from_state_dict(
    state_dict: Mapping[str, Any] | None,
) -> dict[str, torch.Tensor]:
    """Recover fold-fitted banks before constructing the checkpoint model."""

    if state_dict is None:
        return {}
    matches: list[torch.Tensor] = []
    for raw_key, raw_value in state_dict.items():
        key = str(raw_key)
        if key.endswith("pca_klt_basis") and torch.is_tensor(raw_value):
            matches.append(raw_value.detach().to(device="cpu"))
    if not matches:
        return {}
    reference = matches[0]
    for value in matches[1:]:
        if value.shape != reference.shape or not torch.equal(value, reference):
            raise ValueError("Checkpoint contains conflicting PCA/KLT basis banks")
    return {"pca_klt": reference}


def build_temporal_basis_metadata(
    families: Sequence[str] | str,
    *,
    lookback: int,
    components: int,
    components_by_family: Mapping[str, int] | None = None,
    covariance: torch.Tensor | None = None,
    basis_overrides: Mapping[str, torch.Tensor] | None = None,
    pca_metadata: Mapping[str, Any] | None = None,
    novelty_threshold: float = 1e-4,
) -> dict[str, Any]:
    """Describe the unchanged per-family selection and its realized span."""

    names = _normalize_temporal_basis_families(families)
    overrides = dict(basis_overrides or {})
    component_limits = {
        _normalize_temporal_basis_families((name,))[0]: int(value)
        for name, value in dict(components_by_family or {}).items()
    }
    unexpected_limits = set(component_limits).difference(names)
    if unexpected_limits:
        raise ValueError(
            "Temporal basis metadata component limits are not enabled: "
            f"{sorted(unexpected_limits)}"
        )
    selected_rows: list[dict[str, Any]] = []
    q_rows: list[torch.Tensor] = []
    trace = None
    if covariance is not None:
        covariance = torch.as_tensor(covariance, dtype=torch.float64)
        trace = float(torch.trace(covariance).clamp_min(0.0).item())

    candidate_counts: dict[str, int] = {}
    selected_counts: dict[str, int] = {}
    duplicate_count = 0
    for family in names:
        family_components = component_limits.get(family, int(components))
        selected_candidate_indices: list[int]
        if family == "pca_klt":
            candidate_counts[family] = max(0, int(lookback) - 1)
            if family not in overrides:
                raise ValueError("PCA/KLT metadata requires its training-only override")
            matrix = _validated_temporal_basis_override(
                overrides[family],
                family=family,
                steps=int(lookback),
                components=family_components,
            ).to(dtype=torch.float64)
            selected_candidate_indices = list(range(int(matrix.size(0))))
        else:
            candidates = _fixed_temporal_basis_candidates(
                family,
                steps=int(lookback),
                components_hint=(
                    int(lookback) - 1
                    if family in component_limits
                    else int(components)
                ),
            )
            candidate_counts[family] = len(candidates)
            if family in component_limits:
                effective_rows, effective_indices, _candidate_novelty = (
                    _select_novel_temporal_basis_rows(
                        candidates,
                        steps=int(lookback),
                        novelty_threshold=float(novelty_threshold),
                    )
                )
                keep = min(family_components, len(effective_rows))
                matrix = torch.stack(effective_rows[:keep], dim=0)
                selected_candidate_indices = effective_indices[:keep]
            else:
                matrix = _temporal_basis_matrix(
                    family,
                    steps=int(lookback),
                    components=family_components,
                ).to(dtype=torch.float64)
                selected_candidate_indices = list(range(int(matrix.size(0))))
        selected_counts[family] = int(matrix.size(0))
        parameter_grid = temporal_basis_candidate_parameters(
            family,
            steps=int(lookback),
        )
        for component_index, row in enumerate(matrix):
            row = row - row.mean()
            row = row / row.norm().clamp_min(1e-12)
            residual = row.clone()
            for _ in range(2):
                for previous in q_rows:
                    residual = residual - torch.dot(residual, previous) * previous
            novelty_ratio = float(residual.square().sum().item())
            duplicate = novelty_ratio < float(novelty_threshold)
            if duplicate:
                duplicate_count += 1
            else:
                q_rows.append(residual / residual.norm().clamp_min(1e-12))
            variance_contribution = None
            novelty_variance_contribution = None
            if covariance is not None and trace is not None and trace > 0.0:
                variance_contribution = float(
                    (row @ covariance @ row).clamp_min(0.0).item() / trace
                )
                novelty_variance_contribution = float(
                    (residual @ covariance @ residual).clamp_min(0.0).item()
                    / trace
                )
            candidate_index = selected_candidate_indices[component_index]
            parameters = (
                dict(parameter_grid[candidate_index])
                if candidate_index < len(parameter_grid)
                else {"component_index": int(component_index)}
            )
            parameters["candidate_index"] = int(candidate_index)
            if family == "pca_klt" and pca_metadata is not None:
                eigenvalues = list(pca_metadata.get("eigenvalues_descending", []))
                if component_index < len(eigenvalues):
                    parameters["eigenvalue"] = float(eigenvalues[component_index])
            selected_rows.append(
                {
                    "family": family,
                    "basis_name": f"{family}_{component_index + 1:02d}",
                    "parameters": parameters,
                    "family_component_index": int(component_index),
                    "rank_after": int(len(q_rows)),
                    "novelty_ratio": novelty_ratio,
                    "near_duplicate": duplicate,
                    "variance_contribution": variance_contribution,
                    "novelty_variance_contribution": (
                        novelty_variance_contribution
                    ),
                }
            )

    return {
        "schema_version": TEMPORAL_BASIS_METADATA_SCHEMA_VERSION,
        "selection_policy": (
            "per_family_effective_rank"
            if component_limits
            else "legacy_per_family_component_count"
        ),
        "lookback": int(lookback),
        "components_per_family": int(components),
        "components_by_family": component_limits,
        "families": list(names),
        "candidate_counts": candidate_counts,
        "selected_counts": selected_counts,
        "selected": selected_rows,
        "actual_rank": int(len(q_rows)),
        "selected_total": int(len(selected_rows)),
        "near_duplicate_threshold": float(novelty_threshold),
        "near_duplicate_count": int(duplicate_count),
        "training_covariance": dict(pca_metadata or {}),
    }


def temporal_basis_effective_rank_profile(
    families: Sequence[str] | str,
    *,
    lookback: int,
    novelty_threshold: float = 1e-4,
) -> dict[str, Any]:
    """Profile unlimited per-family and combined non-DC candidate spans."""

    names = _normalize_temporal_basis_families(families)
    lookback = int(lookback)
    if lookback < 2:
        raise ValueError("Temporal basis effective rank requires lookback >= 2")
    family_profiles: dict[str, dict[str, int]] = {}
    combined_candidates: list[torch.Tensor] = []
    for family in names:
        if family == "pca_klt":
            candidates = [
                row.double()
                for row in _temporal_basis_matrix(
                    "dct",
                    steps=lookback,
                    components=lookback - 1,
                )
            ]
        else:
            candidates = _fixed_temporal_basis_candidates(
                family,
                steps=lookback,
                components_hint=lookback - 1,
            )
        effective_rows, _indices, _novelty = _select_novel_temporal_basis_rows(
            candidates,
            steps=lookback,
            novelty_threshold=float(novelty_threshold),
        )
        candidate_count = len(candidates)
        effective_rank = len(effective_rows)
        family_profiles[family] = {
            "candidate_count": int(candidate_count),
            "effective_rank": int(effective_rank),
            "near_duplicate_count": int(candidate_count - effective_rank),
        }
        combined_candidates.extend(candidates)

    combined_rows, _combined_indices, _combined_novelty = (
        _select_novel_temporal_basis_rows(
            combined_candidates,
            steps=lookback,
            novelty_threshold=float(novelty_threshold),
        )
    )
    candidate_total = sum(
        values["candidate_count"] for values in family_profiles.values()
    )
    rank_sum = sum(
        values["effective_rank"] for values in family_profiles.values()
    )
    return {
        "lookback": lookback,
        "novelty_threshold": float(novelty_threshold),
        "families": family_profiles,
        "candidate_total": int(candidate_total),
        "sum_family_effective_ranks": int(rank_sum),
        "combined_effective_rank": int(len(combined_rows)),
        "combined_near_duplicate_count": int(
            candidate_total - len(combined_rows)
        ),
    }
