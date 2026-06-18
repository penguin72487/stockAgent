from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Sequence

import numpy as np
import torch

from stockagent.runtime_env import normalize_cuda_env


Color = str
LineSeries = tuple[str, object, object, Color]
ScatterSeries = tuple[str, object, object, Color]
HeatmapLabel = tuple[int, str]


def _ensure_conda_cuda_path() -> None:
    if normalize_cuda_env() is not None:
        return

    prefix = Path(sys.prefix)
    env_bin = prefix / "bin"
    if env_bin.exists():
        parts = os.environ.get("PATH", "").split(os.pathsep)
        if str(env_bin) not in parts:
            os.environ["PATH"] = str(env_bin) + os.pathsep + os.environ.get("PATH", "")

    current = os.environ.get("CUDA_PATH")
    if current and (Path(current) / "include" / "cuda_runtime.h").exists():
        os.environ["CUDA_HOME"] = current
        return

    for candidate in (prefix / "targets" / "x86_64-linux", prefix):
        if (candidate / "include" / "cuda_runtime.h").exists():
            os.environ["CUDA_PATH"] = str(candidate)
            os.environ["CUDA_HOME"] = str(candidate)
            return


_ensure_conda_cuda_path()


def _import_rapids_stack():
    _ensure_conda_cuda_path()
    import cupy as cp
    import cudf
    import datashader as ds
    from datashader import transfer_functions as tf

    return cp, cudf, ds, tf


def _import_cuml_umap():
    _ensure_conda_cuda_path()
    from cuml.manifold import UMAP

    return UMAP


def _new_cuml_umap(**kwargs):
    UMAP = _import_cuml_umap()
    try:
        return UMAP(init="random", **kwargs)
    except TypeError:
        return UMAP(**kwargs)


def rapids_datashader_available(require_cuda: bool = True) -> bool:
    try:
        cp, _cudf, _ds, _tf = _import_rapids_stack()
        if require_cuda and int(cp.cuda.runtime.getDeviceCount()) <= 0:
            return False
        probe = cp.asarray([0.0, 1.0], dtype=cp.float32)
        _ = int(cp.isfinite(probe).sum().get())
        return True
    except Exception:
        return False


def cuml_umap_available(require_cuda: bool = True) -> bool:
    try:
        cp, _cudf, _ds, _tf = _import_rapids_stack()
        if require_cuda and int(cp.cuda.runtime.getDeviceCount()) <= 0:
            return False
        probe = cp.random.random((8, 3), dtype=cp.float32)
        _ = _new_cuml_umap(n_components=2, n_neighbors=3, min_dist=0.1, random_state=42).fit_transform(probe)
        return True
    except Exception:
        return False


def to_cupy_2d(values: object, *, dtype: str = "float32"):
    cp, _cudf, _ds, _tf = _import_rapids_stack()
    if isinstance(values, torch.Tensor):
        tensor = values.detach()
        if tensor.device.type == "cuda":
            tensor = tensor.contiguous()
            return cp.from_dlpack(torch.utils.dlpack.to_dlpack(tensor)).astype(dtype, copy=False).reshape(
                tensor.shape[0], -1
            )
        return cp.asarray(tensor.cpu().numpy(), dtype=dtype).reshape(tensor.shape[0], -1)
    arr = cp.asarray(values, dtype=dtype)
    return arr.reshape(arr.shape[0], -1)


def _to_cupy_1d(values: object, *, dtype: str = "float64"):
    cp, _cudf, _ds, _tf = _import_rapids_stack()
    if isinstance(values, torch.Tensor):
        tensor = values.detach()
        if tensor.device.type == "cuda":
            tensor = tensor.contiguous()
            return cp.from_dlpack(torch.utils.dlpack.to_dlpack(tensor)).astype(dtype, copy=False).reshape(-1)
        return cp.asarray(tensor.cpu().numpy(), dtype=dtype).reshape(-1)
    return cp.asarray(values, dtype=dtype).reshape(-1)


def _finite_xy(x, y):
    cp, _cudf, _ds, _tf = _import_rapids_stack()
    finite = cp.isfinite(x) & cp.isfinite(y)
    return x[finite], y[finite]


def _safe_range(values, *, pad: float = 0.05) -> tuple[float, float]:
    cp, _cudf, _ds, _tf = _import_rapids_stack()
    values = values[cp.isfinite(values)]
    if int(values.size) == 0:
        return 0.0, 1.0
    v_min = float(cp.min(values).get())
    v_max = float(cp.max(values).get())
    if not np.isfinite(v_min) or not np.isfinite(v_max):
        return 0.0, 1.0
    if v_min == v_max:
        delta = 1.0 if v_min == 0.0 else abs(v_min) * 0.1
        return v_min - delta, v_max + delta
    delta = (v_max - v_min) * float(pad)
    return v_min - delta, v_max + delta


def _draw_overlay(pil_image, *, title: str, legend: Sequence[tuple[str, Color]] = (), y_label: str = ""):
    from PIL import Image, ImageDraw

    top = 54
    right = 260 if legend else 24
    width, height = pil_image.size
    canvas = Image.new("RGB", (width + right, height + top), "white")
    canvas.paste(pil_image.convert("RGB"), (0, top))
    draw = ImageDraw.Draw(canvas)
    draw.text((12, 12), title, fill=(20, 20, 20))
    if y_label:
        draw.text((12, 32), y_label, fill=(70, 70, 70))
    legend_x = width + 16
    legend_y = top + 12
    for label, color in legend:
        draw.rectangle((legend_x, legend_y, legend_x + 18, legend_y + 10), fill=color)
        draw.text((legend_x + 26, legend_y - 3), label, fill=(30, 30, 30))
        legend_y += 22
    return canvas


def _draw_heatmap_overlay(
    pil_image,
    *,
    title: str,
    x_label: str = "",
    y_label: str = "",
    y_labels: Sequence[HeatmapLabel] | None = None,
):
    from PIL import Image, ImageDraw

    top = 58
    left = 220 if y_labels else 24
    bottom = 30 if x_label else 12
    width, height = pil_image.size
    canvas = Image.new("RGB", (width + left, height + top + bottom), "white")
    canvas.paste(pil_image.convert("RGB"), (left, top))
    draw = ImageDraw.Draw(canvas)
    draw.text((12, 12), title, fill=(20, 20, 20))
    subtitle = " / ".join(part for part in (x_label, y_label) if part)
    if subtitle:
        draw.text((12, 34), subtitle, fill=(70, 70, 70))
    if x_label:
        draw.text((left + 8, top + height + 8), x_label, fill=(70, 70, 70))
    if y_labels:
        max_labels = max(1, min(int(height // 18), len(y_labels)))
        stride = max(1, int(np.ceil(len(y_labels) / max_labels)))
        for ordinal, label in y_labels[::stride]:
            y = top + height - int((float(ordinal) + 0.5) / max(1.0, float(len(y_labels))) * height)
            draw.text((12, max(top, y - 8)), str(label)[:28], fill=(55, 55, 55))
    return canvas


def save_line_series_datashader(
    series: Sequence[LineSeries],
    *,
    output_path: str | Path,
    title: str,
    y_label: str = "",
    width: int = 1500,
    height: int = 650,
) -> None:
    cp, cudf, ds, tf = _import_rapids_stack()
    converted = []
    for label, x_values, y_values, color in series:
        x = _to_cupy_1d(x_values)
        y = _to_cupy_1d(y_values)
        x, y = _finite_xy(x, y)
        if int(x.size) >= 2:
            converted.append((label, x, y, color))
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    if not converted:
        from PIL import Image

        _draw_overlay(Image.new("RGB", (width, height), "white"), title=title, y_label=y_label).save(output)
        return
    x_all = cp.concatenate([item[1] for item in converted])
    y_all = cp.concatenate([item[2] for item in converted])
    canvas = ds.Canvas(
        plot_width=max(32, int(width)),
        plot_height=max(32, int(height)),
        x_range=_safe_range(x_all, pad=0.01),
        y_range=_safe_range(y_all),
    )
    images = []
    legend = []
    for label, x, y, color in converted:
        frame = cudf.DataFrame({"x": x.astype(cp.float64, copy=False), "y": y.astype(cp.float64, copy=False)})
        images.append(tf.shade(canvas.line(frame, "x", "y"), cmap=[color], how="linear"))
        legend.append((label, color))
    image = tf.set_background(tf.stack(*images), "white")
    _draw_overlay(image.to_pil(), title=title, legend=legend, y_label=y_label).save(output)


def save_scatter_datashader(
    series: Sequence[ScatterSeries],
    *,
    output_path: str | Path,
    title: str,
    width: int = 1100,
    height: int = 760,
) -> None:
    cp, cudf, ds, tf = _import_rapids_stack()
    converted = []
    for label, x_values, y_values, color in series:
        x = _to_cupy_1d(x_values)
        y = _to_cupy_1d(y_values)
        x, y = _finite_xy(x, y)
        if int(x.size) > 0:
            converted.append((label, x, y, color))
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    if not converted:
        from PIL import Image

        _draw_overlay(Image.new("RGB", (width, height), "white"), title=title).save(output)
        return
    x_all = cp.concatenate([item[1] for item in converted])
    y_all = cp.concatenate([item[2] for item in converted])
    canvas = ds.Canvas(
        plot_width=max(32, int(width)),
        plot_height=max(32, int(height)),
        x_range=_safe_range(x_all, pad=0.08),
        y_range=_safe_range(y_all, pad=0.08),
    )
    images = []
    legend = []
    for label, x, y, color in converted:
        frame = cudf.DataFrame({"x": x.astype(cp.float64, copy=False), "y": y.astype(cp.float64, copy=False)})
        images.append(tf.shade(canvas.points(frame, "x", "y"), cmap=[color], how="eq_hist"))
        legend.append((label, color))
    image = tf.set_background(tf.stack(*images), "white")
    _draw_overlay(image.to_pil(), title=title, legend=legend).save(output)


def save_heatmap_points_datashader(
    x_values: object,
    y_values: object,
    values: object,
    *,
    output_path: str | Path,
    title: str,
    x_label: str = "",
    y_label: str = "",
    y_labels: Sequence[HeatmapLabel] | None = None,
    width: int = 1100,
    height: int = 720,
) -> None:
    cp, cudf, ds, tf = _import_rapids_stack()
    x = _to_cupy_1d(x_values)
    y = _to_cupy_1d(y_values)
    value = _to_cupy_1d(values)
    finite = cp.isfinite(x) & cp.isfinite(y) & cp.isfinite(value)
    x = x[finite]
    y = y[finite]
    value = cp.nan_to_num(value[finite], nan=0.0, posinf=0.0, neginf=0.0)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    if int(x.size) == 0:
        from PIL import Image

        _draw_heatmap_overlay(
            Image.new("RGB", (width, height), "white"),
            title=title,
            x_label=x_label,
            y_label=y_label,
            y_labels=y_labels,
        ).save(output)
        return
    x_range = _safe_range(x, pad=0.01)
    y_range = (-0.5, float(len(y_labels)) - 0.5) if y_labels else _safe_range(y, pad=0.01)
    frame = cudf.DataFrame(
        {
            "x": x.astype(cp.float64, copy=False),
            "y": y.astype(cp.float64, copy=False),
            "value": value.astype(cp.float64, copy=False),
        }
    )
    canvas = ds.Canvas(
        plot_width=max(32, int(width)),
        plot_height=max(32, int(height)),
        x_range=x_range,
        y_range=y_range,
    )
    aggregate = canvas.points(frame, "x", "y", agg=ds.sum("value"))
    image = tf.shade(aggregate, cmap=["#f7fbff", "#c6dbef", "#6baed6", "#2171b5", "#08306b"], how="linear")
    image = tf.set_background(image, "white")
    _draw_heatmap_overlay(
        image.to_pil(),
        title=title,
        x_label=x_label,
        y_label=y_label,
        y_labels=y_labels,
    ).save(output)


def run_cuml_umap(
    values: torch.Tensor,
    *,
    n_neighbors: int = 15,
    min_dist: float = 0.1,
    random_state: int = 42,
):
    cp, _cudf, _ds, _tf = _import_rapids_stack()
    matrix = to_cupy_2d(values, dtype="float32")
    matrix = cp.nan_to_num(matrix, nan=0.0, posinf=0.0, neginf=0.0)
    n_samples = int(matrix.shape[0])
    if n_samples < 4:
        raise ValueError("cuML UMAP needs at least 4 samples for a useful projection")
    n_neighbors = max(2, min(int(n_neighbors), n_samples - 1))
    reducer = _new_cuml_umap(
        n_components=2,
        n_neighbors=n_neighbors,
        min_dist=float(min_dist),
        random_state=int(random_state),
        output_type="cupy",
    )
    return reducer.fit_transform(matrix)
