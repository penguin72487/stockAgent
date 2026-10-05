"""Fold-owned, zero-preserving feature-axis truncated SVD.

The covariance operator is streamed over every alive training input cell.
Verified positive-zero storage columns may be omitted from multiplication,
but never from the schema or the energy denominator. No temporal basis bank,
centering, label, validation date or test date is involved.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import time

import numpy as np
import torch

CONTRACT = "training_only_rms_uncentered_feature_svd_v1"


@dataclass(frozen=True)
class FeatureSVDFit:
    directions: torch.Tensor
    squared_singular_values: torch.Tensor
    metadata: dict


@dataclass(frozen=True)
class FullFeatureSpectrum:
    """Analysis only: no projection directions or model/optimizer mutation."""

    squared_singular_values: torch.Tensor
    metadata: dict


def training_feature_rows(targets, *, lookback: int, feature_lag: int, rows: int):
    targets = np.asarray(targets, dtype=np.int64).reshape(-1)
    if (not targets.size or lookback < 1 or feature_lag < 0
            or np.any(np.diff(targets) <= 0)):
        raise ValueError("SVD needs ordered unique training targets and a valid causal window")
    ends = targets - feature_lag
    starts = ends - lookback + 1
    if starts.min() < 0 or ends.max() >= rows:
        raise ValueError("SVD training feature window is outside the panel")
    return np.unique((starts[:, None] + np.arange(lookback)[None, :]).reshape(-1))


def full_training_feature_spectrum(
    features, alive_mask, training_target_indices, *, lookback: int,
    feature_lag: int, scale, active_mask, device="cpu", distributed=False,
    progress=None,
) -> FullFeatureSpectrum:
    """Full FP64 Gram spectrum over every unique alive causal training cell.

    The input normalization follows the model's FP32 zero-preserving RMS.
    Dot products, accumulation and eigensolution are FP64. Verified storage
    zeros and training-inactive coordinates save computation, not observations.
    Unlike a randomized truncated spectrum, this computes every direction and
    can answer thresholds beyond the leading 128. It never fits a model.
    """
    started = time.perf_counter()
    width = int(features.shape[-1])
    device = torch.device(device)
    if device.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA feature spectrum must not silently fall back to CPU")
    scale_cpu = torch.as_tensor(scale, dtype=torch.float32).reshape(-1).cpu()
    active_cpu = torch.as_tensor(active_mask, dtype=torch.bool).reshape(-1).cpu()
    if (scale_cpu.numel() != width or active_cpu.numel() != width
            or not torch.isfinite(scale_cpu).all() or (scale_cpu <= 0).any()):
        raise ValueError("feature spectrum requires aligned finite positive RMS scales")
    if tuple(alive_mask.shape) != tuple(features.shape[:2]):
        raise ValueError("feature spectrum alive mask does not match panel")
    source_rows = training_feature_rows(training_target_indices, lookback=lookback,
                                       feature_lag=feature_lag, rows=features.shape[0])
    if distributed:
        if not torch.distributed.is_initialized():
            raise RuntimeError("distributed feature spectrum needs the canonical process group")
        rank, world = torch.distributed.get_rank(), torch.distributed.get_world_size()
    else:
        rank, world = 0, 1
    owned_rows = np.array_split(source_rows, world)[rank]
    active_ids = np.flatnonzero(active_cpu.numpy())
    active_width = len(active_ids)
    if not active_width:
        raise ValueError("feature spectrum has no positive training energy")
    positions = np.full(width, -1, dtype=np.int64)
    positions[active_ids] = np.arange(active_width)
    scale_device = scale_cpu.to(device)
    gram = torch.zeros((active_width, active_width), dtype=torch.float64, device=device)
    energy = torch.zeros((), dtype=torch.float64, device=device)
    count = torch.zeros((), dtype=torch.int64, device=device)

    def slabs():
        if getattr(features, "_stockagent_factorized_features", False):
            yield from features.iter_reduction_column_slabs(owned_rows)
        else:
            for start in range(0, len(owned_rows), 32):
                selected = owned_rows[start:start + 32]
                for stock in range(0, features.shape[1], 128):
                    stocks = slice(stock, min(features.shape[1], stock + 128))
                    yield selected, stocks, np.arange(width), features[selected, stocks]

    last_message = time.perf_counter()
    blocks = 0
    with torch.no_grad(), torch.autocast(device.type, enabled=False):
        for selected, stocks, columns, values in slabs():
            alive = np.asarray(alive_mask[selected, stocks], dtype=bool).reshape(-1)
            count += int(alive.sum())
            if not np.any(alive):
                continue
            columns = np.asarray(columns, dtype=np.int64)
            keep = positions[columns] >= 0
            if not np.any(keep):
                continue
            compact = np.nan_to_num(np.asarray(values, dtype=np.float32),
                                    nan=0., posinf=0., neginf=0.)
            compact = np.ascontiguousarray(compact.reshape(-1, len(columns))[alive][:, keep])
            ids = torch.as_tensor(columns[keep], device=device)
            x = (torch.from_numpy(compact).to(device) / scale_device[ids]).to(torch.float64)
            product = x.T @ x
            coords = torch.as_tensor(positions[columns[keep]], device=device)
            # Each rectangle has unique columns; no repeated-index race.
            gram.index_put_((coords[:, None], coords[None, :]), product, accumulate=True)
            energy += x.square().sum()
            blocks += 1
            if progress is not None and time.perf_counter() - last_message >= 10:
                progress({"phase": "full_fp64_gram", "rank": rank, "rectangles": blocks,
                          "elapsed_s": time.perf_counter() - started})
                last_message = time.perf_counter()
        if distributed:
            for value in (gram, energy, count):
                torch.distributed.all_reduce(value)
        if device.type == "cuda":
            torch.cuda.synchronize(device)
        total_energy, alive_count = float(energy), int(count)
        if alive_count <= 0 or not (0 < total_energy < float("inf")):
            raise ValueError("feature spectrum has no finite positive training energy")
        gram_wall = time.perf_counter() - started
        trace_error = abs(float(gram.diag().sum()) - total_energy) / total_energy
        if trace_error > 1e-11:
            raise RuntimeError("full feature Gram trace disagrees with independent energy")
        if progress is not None:
            progress({"phase": "full_fp64_eigvalsh", "rank": rank,
                      "active_features": active_width, "gram_wall_s": gram_wall})
        eigen_started = time.perf_counter()
        # Both ranks independently solve the same all-reduced full matrix;
        # no peer enters a long NCCL-only wait while the other solves it.
        eigenvalues = torch.linalg.eigvalsh(gram, UPLO="L").flip(0).cpu()
        if device.type == "cuda":
            torch.cuda.synchronize(device)
        eigen_wall = time.perf_counter() - eigen_started
        tolerance = active_width * torch.finfo(torch.float64).eps * float(eigenvalues[0])
        if float(eigenvalues[-1]) < -64 * tolerance:
            raise RuntimeError("full feature Gram has a materially negative eigenvalue")
        negative_energy = float((-eigenvalues.clamp_max(0.)).sum())
        eigen_sum_error = abs(float(eigenvalues.sum()) - total_energy) / total_energy
        if eigen_sum_error > 1e-10:
            raise RuntimeError("full feature spectrum disagrees with independent energy")
        full_values = torch.cat((eigenvalues.clamp_min(0.),
                                 torch.zeros(width - active_width, dtype=torch.float64)))
        ratios = (full_values.cumsum(0) / total_energy).clamp(0., 1.)
        thresholds = []
        for percent in [float(k) for k in range(101)] + [99.9, 99.99, 99.999]:
            if percent == 0:
                dimensions, meaning = 0, "minimum_dimensions"
            elif percent == 100:
                dimensions, meaning = active_width, "untruncated_active_coordinate_guarantee"
            else:
                dimensions = int(torch.searchsorted(ratios, percent / 100.)) + 1
                meaning = "minimum_dimensions"
            thresholds.append({"target_percent": percent, "dimensions": dimensions,
                               "retained_percent": 0. if not dimensions else float(ratios[dimensions - 1]) * 100,
                               "meaning": meaning})
        metadata = {
            "contract": "training_only_rms_uncentered_full_feature_spectrum_v1",
            "axis": "feature", "centered": False, "analysis_only": True,
            "model_projection_fitted": False, "optimizer_started": False,
            "normalization": "fold_training_only_zero_preserving_rms_fp32",
            "gram_dtype": "float64", "eigensolver_dtype": "float64",
            "scope": "unique_alive_causal_training_input_cells",
            "validation_rows_used": 0, "test_rows_used": 0,
            "features": width, "active_features": active_width,
            "training_inactive_zero_coordinates": width - active_width,
            "spectrum_is_truncated": False, "all_training_cells_used": True,
            "distributed_world_size": world, "training_source_rows": len(source_rows),
            "training_source_min": int(source_rows.min()), "training_source_max": int(source_rows.max()),
            "training_source_rows_sha256": hashlib.sha256(source_rows.astype("<i8").tobytes()).hexdigest(),
            "alive_training_cells": alive_count, "total_squared_energy": total_energy,
            "trace_relative_error": trace_error, "eigen_sum_relative_error": eigen_sum_error,
            "negative_eigenvalue_energy_ratio": negative_energy / total_energy,
            "smallest_raw_eigenvalue": float(eigenvalues[-1]),
            "numerical_rank_tolerance": tolerance,
            "numerical_rank": int((eigenvalues > tolerance).sum()),
            "numerical_rank_is_exact_algebraic_rank": False,
            "squared_singular_values": full_values.tolist(),
            "cumulative_squared_energy_ratio": ratios.tolist(),
            "percent_to_dimensions": sorted(thresholds, key=lambda row: row["target_percent"]),
            "local_rectangles": blocks, "gram_wall_s": gram_wall,
            "eigensolver_wall_s": eigen_wall, "fit_wall_s": time.perf_counter() - started,
            "metric_is_predictive_information": False,
        }
    return FullFeatureSpectrum(full_values, metadata)


def fit_training_feature_svd(
    features, alive_mask, training_target_indices, *, lookback: int,
    feature_lag: int, scale, active_mask, components: int,
    analysis_components: int = 128, oversampling: int = 32,
    power_iterations: int = 2, seed: int = 7, device="cpu", distributed=False,
    progress=None,
) -> FeatureSVDFit:
    """Randomized Rayleigh-Ritz SVD with exact full-data squared-energy trace.

    Small problems retain the entire feature subspace and are exact up to
    numerical arithmetic. Wide problems use a reproducible oversampled range;
    their retained energy is measured against ALL data, not renormalized to
    the computed leading spectrum. Residuals expose approximation quality.
    """
    width = int(features.shape[-1])
    if (components < 1 or components > width or analysis_components < components
            or oversampling < 0 or power_iterations < 0):
        raise ValueError("invalid feature SVD dimensions or iteration count")
    device = torch.device(device)
    if device.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA feature SVD must not silently fall back to CPU")
    scale_cpu = torch.as_tensor(scale, dtype=torch.float32).reshape(-1).cpu()
    active_cpu = torch.as_tensor(active_mask, dtype=torch.bool).reshape(-1).cpu()
    if (scale_cpu.numel() != width or active_cpu.numel() != width
            or not torch.isfinite(scale_cpu).all() or (scale_cpu <= 0).any()):
        raise ValueError("feature SVD requires aligned finite positive RMS scales")
    if tuple(alive_mask.shape) != tuple(features.shape[:2]):
        raise ValueError("feature SVD alive mask does not match panel")
    source_rows = training_feature_rows(training_target_indices, lookback=lookback,
                                       feature_lag=feature_lag, rows=features.shape[0])
    if distributed:
        if not torch.distributed.is_initialized():
            raise RuntimeError("distributed feature SVD needs the canonical process group")
        rank = torch.distributed.get_rank()
        world = torch.distributed.get_world_size()
    else:
        rank, world = 0, 1
    owned_rows = np.array_split(source_rows, world)[rank]
    analysis = min(width, int(analysis_components))
    sketch_width = min(width, analysis + int(oversampling))
    generator = torch.Generator(device="cpu").manual_seed(int(seed))
    sketch = torch.randn(width, sketch_width, generator=generator, dtype=torch.float64)
    sketch = torch.linalg.qr(sketch, mode="reduced").Q.to(device=device, dtype=torch.float32)
    scale_device = scale_cpu.to(device)
    active_device = active_cpu.to(device=device, dtype=torch.float32)
    started = time.perf_counter()
    passes = []
    total_energy = None
    alive_count = None

    def slabs():
        if getattr(features, "_stockagent_factorized_features", False):
            yield from features.iter_reduction_column_slabs(owned_rows)
        else:
            for start in range(0, len(owned_rows), 32):
                selected = owned_rows[start:start + 32]
                for stock in range(0, features.shape[1], 128):
                    stocks = slice(stock, min(features.shape[1], stock + 128))
                    yield selected, stocks, np.arange(width), features[selected, stocks]

    # Covariance products stay FP32; accumulation/QR/Ritz/energy stay FP64.
    # BF16 is intentionally not used to determine retained directions.
    with torch.no_grad(), torch.autocast(device.type, enabled=False):
        for iteration in range(power_iterations + 1):
            accumulated = torch.zeros(width, sketch_width, device=device, dtype=torch.float64)
            energy = torch.zeros((), device=device, dtype=torch.float64)
            count = torch.zeros((), device=device, dtype=torch.int64)
            pass_started = last_message = time.perf_counter()
            blocks = 0
            for selected, stocks, columns, values in slabs():
                alive = np.asarray(alive_mask[selected, stocks], dtype=bool).reshape(-1)
                if not np.any(alive):
                    continue
                ids = torch.as_tensor(np.asarray(columns, dtype=np.int64), device=device)
                compact = np.nan_to_num(np.asarray(values, dtype=np.float32), nan=0., posinf=0., neginf=0.)
                compact = np.ascontiguousarray(compact.reshape(-1, len(columns))[alive])
                x = torch.from_numpy(compact).to(device)
                x = (x / scale_device[ids]) * active_device[ids]
                projected = x @ sketch.index_select(0, ids)
                product = x.T @ projected
                accumulated.index_add_(0, ids, product.to(torch.float64))
                if iteration == power_iterations:
                    energy += x.to(torch.float64).square().sum()
                    count += len(x)
                blocks += 1
                if progress is not None and time.perf_counter() - last_message >= 10:
                    progress({"rank": rank, "pass": iteration + 1, "passes": power_iterations + 1,
                              "rectangles": blocks, "elapsed_s": time.perf_counter() - pass_started})
                    last_message = time.perf_counter()
            if distributed:
                for value in (accumulated, energy, count):
                    torch.distributed.all_reduce(value)
            if device.type == "cuda":
                torch.cuda.synchronize(device)
            passes.append({"pass": iteration + 1, "local_rectangles": blocks,
                           "wall_s": time.perf_counter() - pass_started})
            covariance_product = accumulated.cpu()
            if iteration != power_iterations:
                sketch = torch.linalg.qr(covariance_product, mode="reduced").Q.to(
                    device=device, dtype=torch.float32)
            else:
                total_energy, alive_count = float(energy), int(count)
        if alive_count <= 0 or not (0 < total_energy < float("inf")):
            raise ValueError("feature SVD has no finite positive training energy")
        q = sketch.cpu().to(torch.float64)
        gram = q.T @ covariance_product
        eigenvalues, vectors = torch.linalg.eigh((gram + gram.T) * 0.5)
        order = torch.argsort(eigenvalues, descending=True, stable=True)[:analysis]
        eigenvalues = eigenvalues[order].clamp_min(0.)
        vectors = vectors[:, order]
        directions = q @ vectors
        residual = covariance_product @ vectors - directions * eigenvalues[None, :]
        relative_residual = torch.linalg.vector_norm(residual, dim=0) / eigenvalues.clamp_min(1e-30)
        # Deterministic signs; arbitrary singular-vector sign is not an ABI.
        pivots = directions.abs().argmax(dim=0)
        signs = torch.sign(directions[pivots, torch.arange(analysis)])
        directions *= torch.where(signs == 0, 1., signs)[None, :]
        cumulative = eigenvalues.cumsum(0) / total_energy
        if float(cumulative[-1]) > 1.0001:
            raise RuntimeError("feature SVD retained energy exceeds full-data denominator")
        metadata = {
            "contract": CONTRACT, "axis": "feature", "centered": False,
            "normalization": "fold_training_only_zero_preserving_rms",
            "scope": "unique_alive_causal_training_input_cells", "validation_rows_used": 0,
            "test_rows_used": 0, "features": width, "active_features": int(active_cpu.sum()),
            "selected_components": components, "analysis_components": analysis,
            "sketch_width": sketch_width, "power_iterations": power_iterations, "seed": seed,
            "distributed_world_size": world, "training_source_rows": len(source_rows),
            "training_source_min": int(source_rows.min()), "training_source_max": int(source_rows.max()),
            "training_source_rows_sha256": hashlib.sha256(source_rows.astype("<i8").tobytes()).hexdigest(),
            "alive_training_cells": alive_count, "total_squared_energy": total_energy,
            "squared_singular_values": eigenvalues.tolist(),
            "cumulative_squared_energy_ratio": cumulative.tolist(),
            "relative_ritz_residual": relative_residual.tolist(),
            "selected_energy_ratio": float(cumulative[components - 1]),
            "spectrum_is_truncated": sketch_width < width, "all_training_cells_used": True,
            "passes": passes, "fit_wall_s": time.perf_counter() - started,
            "metric_is_predictive_information": False,
        }
    return FeatureSVDFit(directions[:, :components].T.contiguous().to(torch.float32),
                         eigenvalues, metadata)
