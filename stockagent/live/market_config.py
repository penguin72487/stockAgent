from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class LiveMarketConfig:
    market: str
    label: str
    config_path: str
    output_dir: str | None = None
    live_output_dir: str | None = None
    fold_id: int | None = None
    checkpoint_path: str | None = None
    weights_path: str | None = None
    panel_date: str = "latest"
    price_source: str = "panel"
    prices_csv: str | None = None
    yahoo_chunk_size: int = 80
    device: str | None = None
    top_n: int = 20
    min_abs_delta: float = 0.001
    unsupported_message: str | None = None

    def signal_kwargs(self, **overrides: Any) -> dict[str, Any]:
        values: dict[str, Any] = {
            "market": self.market,
            "market_label": self.label,
            "config_path": self.config_path,
            "output_dir": self.output_dir,
            "live_output_dir": self.live_output_dir,
            "fold_id": self.fold_id,
            "checkpoint_path": self.checkpoint_path,
            "weights_path": self.weights_path,
            "panel_date": self.panel_date,
            "price_source": self.price_source,
            "prices_csv": self.prices_csv,
            "yahoo_chunk_size": self.yahoo_chunk_size,
            "device": self.device,
            "top_n": self.top_n,
            "min_abs_delta": self.min_abs_delta,
            "write": True,
        }
        for key, value in overrides.items():
            if value is not None:
                values[key] = value
        return values


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() in {"auto", "latest", "none", "null"}:
        return None
    return int(text)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() in {"none", "null"}:
        return None
    return text


def load_market_config(path: str | Path) -> LiveMarketConfig:
    path = Path(path)
    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Market config must be a YAML mapping: {path}")

    market = str(raw.get("market") or raw.get("id") or path.stem).strip()
    if not market:
        raise ValueError(f"Market config missing market id: {path}")

    return LiveMarketConfig(
        market=market,
        label=str(raw.get("label") or market),
        config_path=str(raw.get("config_path") or raw.get("config") or "configs/markets/tw.yaml"),
        output_dir=_optional_str(raw.get("output_dir")),
        live_output_dir=_optional_str(raw.get("live_output_dir")),
        fold_id=_optional_int(raw.get("fold_id")),
        checkpoint_path=_optional_str(raw.get("checkpoint_path") or raw.get("checkpoint")),
        weights_path=_optional_str(raw.get("weights_path")),
        panel_date=str(raw.get("panel_date") or "latest"),
        price_source=str(raw.get("price_source") or "panel"),
        prices_csv=_optional_str(raw.get("prices_csv")),
        yahoo_chunk_size=int(raw.get("yahoo_chunk_size") or 80),
        device=_optional_str(raw.get("device")),
        top_n=int(raw.get("top_n") or 20),
        min_abs_delta=float(raw.get("min_abs_delta") if raw.get("min_abs_delta") is not None else 0.001),
        unsupported_message=_optional_str(raw.get("unsupported_message")),
    )


def load_market_configs(directory: str | Path) -> dict[str, LiveMarketConfig]:
    root = Path(directory)
    configs: dict[str, LiveMarketConfig] = {}
    if not root.exists():
        return configs
    for path in sorted(root.glob("*.yaml")) + sorted(root.glob("*.yml")):
        cfg = load_market_config(path)
        configs[cfg.market] = cfg
    return configs
