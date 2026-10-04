"""OpenBB archive values; scheduling observations remain outside task identity."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

import pyarrow as pa


@dataclass(frozen=True, slots=True)
class AssetRecord:
    symbol: str
    name: str
    market: str
    security_type: str



@dataclass(frozen=True, slots=True)
class DownloadTask:
    task_id: str
    endpoint: str
    category: str
    scope_key: str
    kwargs: dict[str, Any]
    providers: tuple[str, ...]
    output_path: str
    # Durable per-provider terminal outcomes let a fallback chain make forward
    # progress even when another provider is cooling down.  Without this, a
    # task such as SEC -> FMP either has to wait for FMP before trying SEC, or
    # repeat an authoritative SEC empty response after every FMP cooldown.
    provider_outcomes: dict[str, str] = field(default_factory=dict, compare=False)
    # Terminal capability claims need their positive evidence to survive
    # fallback deferrals and process restarts.  A categorical ``unavailable``
    # outcome alone cannot distinguish a stable subscription restriction from
    # a transient request failure.
    provider_evidence: dict[str, str] = field(default_factory=dict, compare=False)
    # These scheduling fields are observations, not part of task identity.
    # ``attempts`` remains the lifetime request audit counter, while
    # ``transient_failures`` is the consecutive task-local failure streak used
    # exclusively for durable exponential backoff.
    attempts: int = field(default=0, compare=False)
    transient_failures: int = field(default=0, compare=False)



@dataclass(slots=True)
class TaskResult:
    task: DownloadTask
    status: str
    provider: str | None
    rows: int
    output_path: str | None
    attempts: int
    error: str | None = None
    records: list[dict[str, Any]] = field(default_factory=list, repr=False)
    followups: list[DownloadTask] = field(default_factory=list, repr=False)
    provider_outcomes: dict[str, str] = field(default_factory=dict, repr=False)
    provider_evidence: dict[str, str] = field(default_factory=dict, repr=False)
    retry_not_before: str | None = None
    transient_failures: int = 0



@dataclass(frozen=True, slots=True)
class ColumnarTaskPayload:
    """A provider result already normalized as an Arrow table.

    Keeping this marker distinct from ordinary OpenBB results prevents the
    worker from materializing a large Python ``list[dict]`` only to convert it
    back to Arrow during Parquet publication.
    """

    table: pa.Table



@dataclass(slots=True)
class CoverageDecision:
    endpoint: str
    category: str
    available_providers: str
    selected_providers: str
    decision: str
    reason: str
    initial_task_count: int = 0



@dataclass(slots=True)
class PlannerContext:
    schemas: Mapping[str, Mapping[str, Any]]
    commands: Mapping[str, Sequence[str]]
    output_dir: Path
    start_date: str
    end_date: str
    assets: list[AssetRecord]
    etfs: list[AssetRecord]
    currencies: list[str]
    indices: list[str]
    countries: list[str]
    allowed_providers: set[str] | None
    disabled_providers: set[str]
    endpoint_filters: tuple[str, ...]
    categories: set[str] | None
    metadata_only: bool = True
    show_progress: bool = False
