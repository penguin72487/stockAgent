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
    enabled: bool = True
    market_type: str | None = None
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
    timezone: str = "Asia/Taipei"
    open_time: str | None = None
    close_time: str | None = None
    schedule_time: str | None = None
    summary_time: str | None = None
    data_ready_time: str | None = None
    freshness_max_lag_days: int = 1
    freshness_scan_limit: int = 0
    holidays: tuple[str, ...] = ()
    benchmark_window_days: int = 20
    max_turnover_warning: float = 1.5
    max_top_weight_warning: float = 0.1
    max_gross_warning: float | None = None
    trader_role_ids: tuple[int, ...] = ()
    trader_role_names: tuple[str, ...] = ()

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
            "benchmark_window_days": self.benchmark_window_days,
            "max_turnover_warning": self.max_turnover_warning,
            "max_top_weight_warning": self.max_top_weight_warning,
            "max_gross_warning": self.max_gross_warning,
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


def _bool_value(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(default)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() in {"auto", "none", "null"}:
        return None
    return float(text)


def _str_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return tuple(item.strip() for item in str(value).split(",") if item.strip())


def _int_tuple(value: Any) -> tuple[int, ...]:
    out: list[int] = []
    for item in _str_tuple(value):
        try:
            out.append(int(item))
        except ValueError:
            continue
    return tuple(out)


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
        enabled=_bool_value(raw.get("enabled"), True),
        market_type=_optional_str(raw.get("market_type")),
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
        timezone=str(raw.get("timezone") or "Asia/Taipei"),
        open_time=_optional_str(raw.get("open_time")),
        close_time=_optional_str(raw.get("close_time")),
        schedule_time=_optional_str(raw.get("schedule_time")),
        summary_time=_optional_str(raw.get("summary_time")),
        data_ready_time=_optional_str(raw.get("data_ready_time")),
        freshness_max_lag_days=int(raw.get("freshness_max_lag_days") or 1),
        freshness_scan_limit=int(raw.get("freshness_scan_limit") or 0),
        holidays=_str_tuple(raw.get("holidays")),
        benchmark_window_days=int(raw.get("benchmark_window_days") or 20),
        max_turnover_warning=float(raw.get("max_turnover_warning") or 1.5),
        max_top_weight_warning=float(raw.get("max_top_weight_warning") or 0.1),
        max_gross_warning=_optional_float(raw.get("max_gross_warning")),
        trader_role_ids=_int_tuple(raw.get("trader_role_ids")),
        trader_role_names=_str_tuple(raw.get("trader_role_names")),
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
