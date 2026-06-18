from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np


PANEL_CACHE_V2_DIRNAME = "panel_cache_v2"
ARRAY_NAMES = (
    "dates",
    "features",
    "returns_1d",
    "tradable_mask",
    "can_buy_mask",
    "can_sell_mask",
    "alive_mask",
    "benchmark_returns",
    "close_prices",
)


def panel_cache_v2_dir(parquet_root: str | Path) -> Path:
    return Path(parquet_root) / PANEL_CACHE_V2_DIRNAME


def legacy_panel_cache_path(parquet_root: str | Path) -> Path:
    return Path(parquet_root) / "panel_cache.npz"


def legacy_panel_meta_path(parquet_root: str | Path) -> Path:
    return Path(parquet_root) / ".panel_meta.pkl"


def panel_cache_v2_meta_path(parquet_root: str | Path) -> Path:
    return panel_cache_v2_dir(parquet_root) / "meta.json"


def _array_file(cache_dir: Path, name: str) -> Path:
    return cache_dir / f"{name}.npy"


def _json_file(cache_dir: Path, name: str) -> Path:
    return cache_dir / f"{name}.json"


def _write_json(path: Path, payload: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _save_array(cache_dir: Path, name: str, array: np.ndarray) -> dict[str, Any]:
    path = _array_file(cache_dir, name)
    arr = np.asarray(array)
    mmap = np.lib.format.open_memmap(path, mode="w+", dtype=arr.dtype, shape=arr.shape)
    mmap[...] = arr
    mmap.flush()
    del mmap
    return {
        "file": path.name,
        "shape": list(arr.shape),
        "dtype": str(arr.dtype),
    }


def save_panel_cache_v2(
    parquet_root: str | Path,
    panel_like: Any,
    *,
    source_hash: str,
    backend_key: str,
    version: int,
) -> Path:
    cache_dir = panel_cache_v2_dir(parquet_root)
    cache_dir.mkdir(parents=True, exist_ok=True)

    arrays = {
        "dates": np.asarray(panel_like.dates),
        "features": np.asarray(panel_like.features),
        "returns_1d": np.asarray(panel_like.returns_1d),
        "tradable_mask": np.asarray(panel_like.tradable_mask),
        "can_buy_mask": np.asarray(
            panel_like.can_buy_mask if panel_like.can_buy_mask is not None else panel_like.tradable_mask
        ),
        "can_sell_mask": np.asarray(
            panel_like.can_sell_mask if panel_like.can_sell_mask is not None else panel_like.tradable_mask
        ),
        "alive_mask": np.asarray(panel_like.alive_mask),
        "benchmark_returns": np.asarray(panel_like.benchmark_returns),
        "close_prices": np.asarray(panel_like.close_prices),
    }
    array_meta = {name: _save_array(cache_dir, name, array) for name, array in arrays.items()}
    _write_json(_json_file(cache_dir, "symbols"), list(panel_like.symbols))
    _write_json(_json_file(cache_dir, "feature_names"), list(panel_like.feature_names))
    meta = {
        "version": int(version),
        "source_hash": str(source_hash),
        "backend_key": str(backend_key),
        "num_dates": int(panel_like.num_dates),
        "num_symbols": int(panel_like.num_symbols),
        "arrays": array_meta,
    }
    _write_json(panel_cache_v2_meta_path(parquet_root), meta)
    return cache_dir


def read_panel_cache_v2_meta(parquet_root: str | Path) -> dict[str, Any] | None:
    meta_path = panel_cache_v2_meta_path(parquet_root)
    if not meta_path.exists():
        return None
    try:
        meta = _read_json(meta_path)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(meta, dict):
        return None
    return meta


def load_panel_cache_v2(parquet_root: str | Path, *, mmap_mode: str | None = "r") -> dict[str, Any]:
    cache_dir = panel_cache_v2_dir(parquet_root)
    meta = read_panel_cache_v2_meta(parquet_root)
    if meta is None:
        raise FileNotFoundError(f"missing panel cache v2 metadata under {cache_dir}")
    payload: dict[str, Any] = {
        "symbols": list(_read_json(_json_file(cache_dir, "symbols"))),
        "feature_names": list(_read_json(_json_file(cache_dir, "feature_names"))),
    }
    arrays_meta = meta.get("arrays", {})
    for name in ARRAY_NAMES:
        file_name = arrays_meta.get(name, {}).get("file", f"{name}.npy")
        payload[name] = np.load(cache_dir / file_name, mmap_mode=mmap_mode, allow_pickle=False)
    return payload


def panel_cache_v2_is_valid(
    parquet_root: str | Path,
    *,
    source_hash: str,
    backend_key: str,
    version: int,
    source_paths: list[Path],
) -> bool:
    meta = read_panel_cache_v2_meta(parquet_root)
    if meta is None:
        return False
    if (
        meta.get("source_hash") != source_hash
        or int(meta.get("version", -1)) != int(version)
        or meta.get("backend_key") != backend_key
    ):
        return False
    cache_dir = panel_cache_v2_dir(parquet_root)
    required_paths = [panel_cache_v2_meta_path(parquet_root), _json_file(cache_dir, "symbols"), _json_file(cache_dir, "feature_names")]
    required_paths.extend(_array_file(cache_dir, name) for name in ARRAY_NAMES)
    if not all(path.exists() for path in required_paths):
        return False
    if not source_paths:
        return True
    newest_source_mtime = max(path.stat().st_mtime for path in source_paths)
    oldest_cache_mtime = min(path.stat().st_mtime for path in required_paths)
    return oldest_cache_mtime >= newest_source_mtime
