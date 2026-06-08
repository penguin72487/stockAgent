from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Sequence

import numpy as np
import torch

from stockagent.backtest.simulator import BacktestResultTensor


Color = str
LineSeries = tuple[str, object, object, Color]
HeatmapLabel = tuple[int, str]


def _ensure_conda_cuda_path() -> None:
    current = os.environ.get("CUDA_PATH")
    if current and (Path(current) / "include" / "cuda_runtime.h").exists():
        cuda_root = Path(current)
    else:
        cuda_root = None
    prefix = Path(sys.prefix)
    bin_dir = prefix / "bin"
    if bin_dir.exists():
        path_parts = os.environ.get("PATH", "").split(os.pathsep)
        if str(bin_dir) not in path_parts:
            os.environ["PATH"] = str(bin_dir) + os.pathsep + os.environ.get("PATH", "")
    if cuda_root is not None:
        os.environ.setdefault("CUDA_HOME", str(cuda_root))
        return
    candidates = [
        prefix / "targets" / "x86_64-linux",
        prefix,
    ]
    for candidate in candidates:
        if (candidate / "include" / "cuda_runtime.h").exists():
            os.environ["CUDA_PATH"] = str(candidate)
            os.environ.setdefault("CUDA_HOME", str(candidate))
            return


_ensure_conda_cuda_path()


def _import_rapids_stack():
    _ensure_conda_cuda_path()
    import cupy as cp
    import cudf
    import datashader as ds
    from datashader import transfer_functions as tf

    return cp, cudf, ds, tf


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


def _gpu_nan_to_num(values, *, nan: float = 0.0, posinf: float = 0.0, neginf: float = 0.0):
    cp, _cudf, _ds, _tf = _import_rapids_stack()
    return cp.nan_to_num(values, nan=nan, posinf=posinf, neginf=neginf)


def _safe_y_range(series: Sequence[LineSeries], *, y_pad: float = 0.05) -> tuple[float, float]:
    cp, _cudf, _ds, _tf = _import_rapids_stack()
    mins = []
    maxs = []
    for _label, x_values, y_values, _color in series:
        x = _to_cupy_1d(x_values)
        y = _to_cupy_1d(y_values)
        _x, y = _finite_xy(x, y)
        if int(y.size) == 0:
            continue
        mins.append(cp.nanmin(y))
        maxs.append(cp.nanmax(y))
    if not mins:
        return 0.0, 1.0
    y_min = float(cp.min(cp.stack(mins)).get())
    y_max = float(cp.max(cp.stack(maxs)).get())
    if not np.isfinite(y_min) or not np.isfinite(y_max):
        return 0.0, 1.0
    if y_min == y_max:
        delta = 1.0 if y_min == 0.0 else abs(y_min) * 0.1
        return y_min - delta, y_max + delta
    pad = (y_max - y_min) * float(y_pad)
    return y_min - pad, y_max + pad


def _safe_x_range(series: Sequence[LineSeries]) -> tuple[float, float]:
    cp, _cudf, _ds, _tf = _import_rapids_stack()
    mins = []
    maxs = []
    for _label, x_values, y_values, _color in series:
        x = _to_cupy_1d(x_values)
        y = _to_cupy_1d(y_values)
        x, _y = _finite_xy(x, y)
        if int(x.size) == 0:
            continue
        mins.append(cp.nanmin(x))
        maxs.append(cp.nanmax(x))
    if not mins:
        return 0.0, 1.0
    x_min = float(cp.min(cp.stack(mins)).get())
    x_max = float(cp.max(cp.stack(maxs)).get())
    if x_min == x_max:
        return x_min - 1.0, x_max + 1.0
    return x_min, x_max


def _draw_overlay(pil_image, *, title: str, legend: Sequence[tuple[str, Color]], y_label: str = ""):
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
    subtitle_parts = [part for part in (x_label, y_label) if part]
    if subtitle_parts:
        draw.text((12, 34), " / ".join(subtitle_parts), fill=(70, 70, 70))
    if x_label:
        draw.text((left + 8, top + height + 8), x_label, fill=(70, 70, 70))
    if y_labels:
        max_labels = max(1, min(int(height // 18), len(y_labels)))
        stride = max(1, int(np.ceil(len(y_labels) / max_labels)))
        for ordinal, label in y_labels[::stride]:
            y = top + height - int((float(ordinal) + 0.5) / max(1.0, float(len(y_labels))) * height)
            draw.text((12, max(top, y - 8)), str(label)[:28], fill=(55, 55, 55))
    return canvas


def render_line_series_datashader(
    series: Sequence[LineSeries],
    *,
    title: str,
    y_label: str = "",
    width: int = 1500,
    height: int = 650,
):
    cp, cudf, ds, tf = _import_rapids_stack()
    x_range = _safe_x_range(series)
    y_range = _safe_y_range(series)
    canvas = ds.Canvas(
        plot_width=max(32, int(width)),
        plot_height=max(32, int(height)),
        x_range=x_range,
        y_range=y_range,
    )
    images = []
    legend: list[tuple[str, Color]] = []
    for label, x_values, y_values, color in series:
        x = _to_cupy_1d(x_values)
        y = _to_cupy_1d(y_values)
        x, y = _finite_xy(x, y)
        if int(x.size) < 2:
            continue
        frame = cudf.DataFrame({"x": x.astype(cp.float64, copy=False), "y": y.astype(cp.float64, copy=False)})
        aggregate = canvas.line(frame, "x", "y")
        images.append(tf.shade(aggregate, cmap=[color], how="linear"))
        legend.append((label, color))
    if not images:
        from PIL import Image

        return Image.new("RGB", (max(32, int(width)), max(32, int(height))), "white")
    image = tf.stack(*images)
    image = tf.set_background(image, "white")
    return _draw_overlay(image.to_pil(), title=title, legend=legend, y_label=y_label)


def save_line_series_datashader(
    series: Sequence[LineSeries],
    *,
    output_path: str | Path,
    title: str,
    y_label: str = "",
    width: int = 1500,
    height: int = 650,
) -> None:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    image = render_line_series_datashader(
        series,
        title=title,
        y_label=y_label,
        width=width,
        height=height,
    )
    image.save(output)


def render_heatmap_points_datashader(
    x_values: object,
    y_values: object,
    values: object,
    *,
    title: str,
    x_label: str = "",
    y_label: str = "",
    y_labels: Sequence[HeatmapLabel] | None = None,
    width: int = 1100,
    height: int = 720,
    cmap: Sequence[str] | None = None,
):
    cp, cudf, ds, tf = _import_rapids_stack()
    x = _to_cupy_1d(x_values)
    y = _to_cupy_1d(y_values)
    value = _to_cupy_1d(values)
    finite = cp.isfinite(x) & cp.isfinite(y) & cp.isfinite(value)
    x = x[finite]
    y = y[finite]
    value = value[finite]
    if int(x.size) == 0:
        from PIL import Image

        blank = Image.new("RGB", (max(32, int(width)), max(32, int(height))), "white")
        return _draw_heatmap_overlay(blank, title=title, x_label=x_label, y_label=y_label, y_labels=y_labels)

    value = cp.nan_to_num(value, nan=0.0, posinf=0.0, neginf=0.0)
    if int(value.size) and float(cp.max(cp.abs(value)).get()) <= 0.0:
        value = value + 1e-12

    x_min = float(cp.min(x).get())
    x_max = float(cp.max(x).get())
    y_min = float(cp.min(y).get())
    y_max = float(cp.max(y).get())
    if x_min == x_max:
        x_min -= 0.5
        x_max += 0.5
    if y_labels:
        y_min = -0.5
        y_max = float(len(y_labels)) - 0.5
    elif y_min == y_max:
        y_min -= 0.5
        y_max += 0.5

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
        x_range=(x_min, x_max),
        y_range=(y_min, y_max),
    )
    aggregate = canvas.points(frame, "x", "y", agg=ds.sum("value"))
    palette = list(cmap or ["#f7fbff", "#c6dbef", "#6baed6", "#2171b5", "#08306b"])
    image = tf.shade(aggregate, cmap=palette, how="linear")
    image = tf.set_background(image, "white")
    return _draw_heatmap_overlay(
        image.to_pil(),
        title=title,
        x_label=x_label,
        y_label=y_label,
        y_labels=y_labels,
    )


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
    cmap: Sequence[str] | None = None,
) -> None:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    image = render_heatmap_points_datashader(
        x_values,
        y_values,
        values,
        title=title,
        x_label=x_label,
        y_label=y_label,
        y_labels=y_labels,
        width=width,
        height=height,
        cmap=cmap,
    )
    image.save(output)


def save_two_panel_line_series_datashader(
    top_series: Sequence[LineSeries],
    bottom_series: Sequence[LineSeries],
    *,
    output_path: str | Path,
    title_top: str,
    title_bottom: str,
    y_label_top: str = "",
    y_label_bottom: str = "",
    width: int = 1500,
    panel_height: int = 520,
) -> None:
    from PIL import Image

    top = render_line_series_datashader(
        top_series,
        title=title_top,
        y_label=y_label_top,
        width=width,
        height=panel_height,
    )
    bottom = render_line_series_datashader(
        bottom_series,
        title=title_bottom,
        y_label=y_label_bottom,
        width=width,
        height=panel_height,
    )
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    combined = Image.new("RGB", (max(top.width, bottom.width), top.height + bottom.height), "white")
    combined.paste(top, (0, 0))
    combined.paste(bottom, (0, top.height))
    combined.save(output)


def plot_equity_curve_tensor_datashader(
    result: BacktestResultTensor,
    dates: np.ndarray,
    output_path: str | Path,
    *,
    log_y: bool = False,
    title: str = "Strategy vs Benchmark Equity Curve",
) -> bool:
    del dates  # Datashader path uses row index for fast GPU raster; dates are preserved in CSV/report artifacts.
    if result.strategy_returns.device.type != "cuda":
        return False
    cp, _cudf, _ds, _tf = _import_rapids_stack()
    strategy_log = cp.cumsum(_gpu_nan_to_num(_to_cupy_1d(result.strategy_returns), nan=0.0))
    benchmark_log = cp.cumsum(_gpu_nan_to_num(_to_cupy_1d(result.benchmark_returns), nan=0.0))
    if log_y:
        strategy_y = strategy_log
        benchmark_y = benchmark_log
        y_label = "Cumulative log return"
    else:
        strategy_y = cp.exp(cp.clip(strategy_log, -745.0, 600.0))
        benchmark_y = cp.exp(cp.clip(benchmark_log, -745.0, 600.0))
        y_label = "Cumulative value"
    x = cp.arange(int(strategy_y.size), dtype=cp.float64)
    save_line_series_datashader(
        [
            ("Strategy", x, strategy_y, "#1f77b4"),
            ("Benchmark", x, benchmark_y, "#ff7f0e"),
        ],
        output_path=output_path,
        title=title,
        y_label=y_label,
        width=1500,
        height=720,
    )
    return True
