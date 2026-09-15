"""Physical-account adapter for the existing chronological windowed trainer.

No model, optimizer, loss formula or source downloader lives here. A prepared
session set is NOT a claim that public source receipts were accepted. The
production config gate stays closed until its source builder and fold/report
callers bind this same contract.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, fields, replace
import os
from typing import Any, Callable
import warnings

import torch

from stockagent.backtest.tw_day_trade_carry import (
    DayTradeCarryEventCompressionFallback,
    DayTradeCarrySession,
    DayTradeCarryState,
    compact_day_trade_carry_session,
    invalidate_day_trade_carry_compiled_caches,
)
from stockagent.backtest.tw_day_trade_inventory import _validate_checkpoint_identity


def _diagnostic_failure_value(value: Any, device: torch.device) -> Any:
    """Detach diagnostic inputs; never mutate the live training trajectory."""
    if isinstance(value, torch.Tensor):
        return value.detach().to(device=device)
    if isinstance(value, DayTradeCarryState):
        return value.detached(device=device)
    return value


@contextmanager
def _force_eager_physical_audit():
    """Temporarily bypass process-local compiled wrappers for an independent oracle."""
    names = (
        "STOCKAGENT_BACKTEST_COMPILE",
        "STOCKAGENT_DAY_TRADE_CARRY_COMPILE",
        "STOCKAGENT_DAY_TRADE_FULL_SESSION_COMPILE",
    )
    previous = {name: os.environ.get(name) for name in names}
    try:
        for name in names:
            os.environ[name] = "0"
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _diagnose_physical_trajectory_failure(
    loss_fn: Callable,
    *,
    source: "PreparedDayTradeCarrySource",
    split,
    start: int,
    end: int,
    weights: torch.Tensor,
    returns: torch.Tensor,
    mask: torch.Tensor,
    supplied: dict[str, Any],
    previous: DayTradeCarryState | None,
    count: int,
) -> tuple[bool, str]:
    """Replay one failed batch through the canonical eager GPU audit boundary.

    GPU kernels deliberately collapse data-dependent validity checks into the
    inventory ``failed`` bit so one bad rank cannot poison the CUDA context.
    That is correct for the hot path but insufficient for repair. This
    failure-only replay uses the same actions, recurrent state, fees and source
    sessions with event compression disabled. The eager outer loop then stops
    at the first bad contract-day and transfers only its small evidence vectors
    to CPU. This avoids an unbounded multi-minute CPU replay on a wide universe.
    """
    eager_loss_fn = getattr(loss_fn, "_eager_fn", loss_fn)
    if not callable(eager_loss_fn):
        return False, "gpu_audit_unavailable: loss callable has no eager implementation"
    diagnostic_device = weights.device
    dense = source.prepare_batch(
        split, start, end, event_compression=False
    ).sessions(diagnostic_device)
    diagnostic_aux: dict[str, Any] = {
        "initial_day_trade_carry_state": (
            None if previous is None else previous.detached(device=diagnostic_device)
        )
    }
    diagnostic_supplied = {
        key: _diagnostic_failure_value(value, diagnostic_device)
        for key, value in supplied.items()
        if key != "aux_outputs"
    }
    diagnostic_supplied["aux_outputs"] = diagnostic_aux
    diagnostic_supplied["day_trade_carry_event_compression"] = False
    first_day = dense[0].day
    last_day = dense[-1].day
    try:
        with _force_eager_physical_audit(), torch.no_grad():
            diagnostic_result = eager_loss_fn(
                weights[:count].detach(),
                returns[:count].detach(),
                mask[:count].detach(),
                day_trade_carry_sessions=dense,
                **diagnostic_supplied,
            )
    except Exception as exc:
        return False, (
            f"gpu_audit={type(exc).__name__}: {exc}; "
            f"batch_days=[{first_day},{last_day}] rows=[{start},{start + count})"
        )
    final = diagnostic_aux.get("_final_day_trade_carry_state")
    final_failed = (
        None
        if not isinstance(final, DayTradeCarryState)
        else float(final.inventory.failed.detach().cpu())
    )
    final_nav = (
        None
        if not isinstance(final, DayTradeCarryState)
        else float(final.last_nav.detach().cpu())
    )
    loss_finite = bool(torch.isfinite(diagnostic_result).all())
    replay_valid = bool(
        loss_finite
        and isinstance(final, DayTradeCarryState)
        and final_failed == 0.0
        and final.last_session_day == last_day
    )
    return replay_valid, (
        "gpu_eager_audit_completed: "
        f"loss_finite={loss_finite} "
        f"final_failed={final_failed} final_nav={final_nav}; "
        f"batch_days=[{first_day},{last_day}] rows=[{start},{start + count})"
    )


@dataclass(frozen=True)
class PackedDayTradeCarrySession:
    """Lossless CPU transport: sparse exits plus the authoritative dense marks.

    The immutable source is already stored this way.  Keeping this transport
    through host staging avoids manufacturing and copying NaN/zero exit cells;
    :class:`PreparedDayTradeCarryBatch` reconstructs the exact dense tensor on
    CUDA before the unchanged session kernel observes it.
    """

    day: int
    official_open: torch.Tensor
    opening_marks: torch.Tensor
    entry_price: torch.Tensor
    entry_volume: torch.Tensor
    lower_limit: torch.Tensor
    upper_limit: torch.Tensor
    halted: torch.Tensor
    exit_flat: torch.Tensor
    exit_price: torch.Tensor
    exit_capacity: torch.Tensor
    marks: torch.Tensor
    action_mask: torch.Tensor
    share_ratio: torch.Tensor
    cash_per_old_share: torch.Tensor
    payment_day: torch.Tensor
    stock_delivery_day: torch.Tensor
    source_gap_mask: torch.Tensor
    unresolved_action_gap_mask: torch.Tensor
    daily_proxy_mask: torch.Tensor

    def validate(self) -> None:
        symbols = int(self.official_open.numel())
        vector_fields = (
            self.official_open,
            self.opening_marks,
            self.entry_price,
            self.entry_volume,
            self.lower_limit,
            self.upper_limit,
            self.halted,
            self.action_mask,
            self.share_ratio,
            self.cash_per_old_share,
            self.payment_day,
            self.stock_delivery_day,
            self.source_gap_mask,
            self.unresolved_action_gap_mask,
            self.daily_proxy_mask,
        )
        if (
            symbols <= 0
            or any(value.shape != (symbols,) for value in vector_fields)
            or any(value.dtype != torch.float64 for value in vector_fields)
            or self.marks.shape != (symbols, 270)
            or self.marks.dtype != torch.float64
            or self.exit_flat.dtype != torch.int64
            or self.exit_flat.ndim != 1
            or self.exit_price.shape != self.exit_flat.shape
            or self.exit_capacity.shape != self.exit_flat.shape
            or self.exit_price.dtype != torch.float64
            or self.exit_capacity.dtype != torch.float64
            or (
                self.exit_flat.numel()
                and not bool(
                    (
                        (self.exit_flat >= 0)
                        & (self.exit_flat < symbols * 270 * 2)
                    ).all()
                )
            )
        ):
            raise ValueError("invalid lossless packed day-trade transport")


@dataclass(frozen=True)
class PreparedDayTradeCarryBatch:
    """Field-major CPU staging so one batch needs O(fields), not O(days*fields), copies."""

    days: tuple[int, ...]
    tensors: dict[str, torch.Tensor]
    count: int
    cpu_sessions: tuple[DayTradeCarrySession, ...] = ()
    event_compression: bool = False
    packed_transport: bool = False
    transport_symbols: int = 0

    @classmethod
    def from_sessions(
        cls,
        sessions: tuple[DayTradeCarrySession, ...],
        count: int,
        *,
        preserve_cpu_sessions: bool = False,
        event_compression: bool = False,
        sparse_events: bool = False,
    ) -> PreparedDayTradeCarryBatch:
        if not sessions or count != len(sessions):
            raise ValueError("physical staged batch requires every real session")
        if sparse_events and not event_compression:
            raise ValueError("sparse carry events require endpoint event compression")
        if sparse_events:
            sessions = tuple(compact_day_trade_carry_session(x) for x in sessions)
        sparse = tuple(session.uses_sparse_events for session in sessions)
        if any(sparse) and not all(sparse):
            raise ValueError("physical staged batch cannot mix dense and sparse sessions")
        packed: dict[str, torch.Tensor] = {}
        event_slots = 0
        if all(sparse):
            raw_slots = os.environ.get(
                "STOCKAGENT_DAY_TRADE_SPARSE_EVENT_SLOTS", "262144"
            )
            try:
                event_slots = int(raw_slots)
            except ValueError as exc:
                raise ValueError(
                    "STOCKAGENT_DAY_TRADE_SPARSE_EVENT_SLOTS must be a power of two"
                ) from exc
            if (
                event_slots <= 0
                or event_slots & (event_slots - 1)
            ):
                raise ValueError(
                    "STOCKAGENT_DAY_TRADE_SPARSE_EVENT_SLOTS must be a power of two"
                )
            required_slots = max(int(x.exit_prices.shape[0]) for x in sessions)
            if required_slots > event_slots:
                raise ValueError(
                    "sparse event session exceeds the fixed compiled ABI: "
                    f"required={required_slots} configured={event_slots}; increase "
                    "STOCKAGENT_DAY_TRADE_SPARSE_EVENT_SLOTS to the next power of two"
                )
        for field in fields(DayTradeCarrySession):
            if field.name == "day":
                continue
            values = tuple(getattr(session, field.name) for session in sessions)
            if all(value is None for value in values):
                continue
            if any(not isinstance(value, torch.Tensor) for value in values):
                raise ValueError(
                    f"physical staged field {field.name} is present on only part of the batch"
                )
            if all(sparse) and field.name in {
                "exit_prices", "exit_capacity", "exit_symbol_indices", "exit_sides"
            }:
                padded = []
                for session, value in zip(sessions, values, strict=True):
                    assert isinstance(value, torch.Tensor)
                    missing = event_slots - int(value.shape[0])
                    if missing < 0:
                        raise RuntimeError("sparse event power-of-two padding underflow")
                    if field.name == "exit_prices":
                        fill = float("nan")
                    elif field.name == "exit_symbol_indices":
                        fill = int(session.official_open.shape[0] - 1)
                    else:
                        fill = 0
                    padded.append(
                        torch.cat((value, value.new_full((missing,), fill)), dim=0)
                        if missing
                        else value
                    )
                packed[field.name] = torch.stack(padded)
            else:
                packed[field.name] = torch.stack(values)  # type: ignore[arg-type]
        return cls(
            tuple(session.day for session in sessions),
            packed,
            int(count),
            sessions if preserve_cpu_sessions else (),
            bool(event_compression),
        )

    @classmethod
    def from_packed_sessions(
        cls,
        sessions: tuple[PackedDayTradeCarrySession, ...],
        count: int,
        *,
        event_compression: bool,
    ) -> PreparedDayTradeCarryBatch:
        if not sessions or count != len(sessions):
            raise ValueError("packed physical batch requires every real session")
        for session in sessions:
            session.validate()
        symbols = int(sessions[0].official_open.numel())
        if any(int(session.official_open.numel()) != symbols for session in sessions):
            raise ValueError("packed physical batch changed its symbol universe")
        required = max(1, max(int(session.exit_flat.numel()) for session in sessions))
        event_slots = 1 << int(required - 1).bit_length()
        exit_size = symbols * 270 * 2
        packed: dict[str, torch.Tensor] = {}
        for name in (
            "official_open",
            "opening_marks",
            "entry_price",
            "entry_volume",
            "lower_limit",
            "upper_limit",
            "halted",
            "marks",
            "action_mask",
            "share_ratio",
            "cash_per_old_share",
            "payment_day",
            "stock_delivery_day",
            "source_gap_mask",
            "unresolved_action_gap_mask",
            "daily_proxy_mask",
        ):
            packed[name] = torch.stack(
                tuple(getattr(session, name) for session in sessions)
            )
        for name, fill in (
            ("exit_flat", exit_size),
            ("exit_price", float("nan")),
            ("exit_capacity", 0.0),
        ):
            rows = []
            for session in sessions:
                value = getattr(session, name)
                missing = event_slots - int(value.numel())
                rows.append(
                    torch.cat((value, value.new_full((missing,), fill)))
                    if missing
                    else value
                )
            packed[f"_transport_{name}"] = torch.stack(rows)
        return cls(
            days=tuple(session.day for session in sessions),
            tensors=packed,
            count=int(count),
            event_compression=bool(event_compression),
            packed_transport=True,
            transport_symbols=symbols,
        )

    def sessions(self, device: torch.device) -> tuple[DayTradeCarrySession, ...]:
        if device.type == "cpu" and self.cpu_sessions:
            return self.cpu_sessions
        moved = {name: tensor.to(device=device) for name, tensor in self.tensors.items()}
        if self.packed_transport:
            symbols = int(self.transport_symbols)
            exit_size = symbols * 270 * 2
            flat = moved.pop("_transport_exit_flat").to(dtype=torch.int64)
            exit_values = moved.pop("_transport_exit_price")
            capacity_values = moved.pop("_transport_exit_capacity")
            rows = int(flat.shape[0])
            dense_prices = torch.full(
                (rows, exit_size + 1),
                float("nan"),
                device=device,
                dtype=torch.float64,
            )
            dense_capacity = torch.zeros_like(dense_prices)
            dense_prices.scatter_(1, flat, exit_values)
            dense_capacity.scatter_(1, flat, capacity_values)
            moved["exit_prices"] = dense_prices[:, :exit_size].reshape(
                rows, symbols, 270, 2
            )
            moved["exit_capacity"] = dense_capacity[:, :exit_size].reshape(
                rows, symbols, 270, 2
            )
        result = []
        for index, day in enumerate(self.days):
            values = {name: tensor[index] for name, tensor in moved.items()}
            result.append(DayTradeCarrySession(day=day, **values))
        return tuple(result)


@dataclass(frozen=True)
class PreparedDayTradeCarrySource:
    sessions: tuple[DayTradeCarrySession, ...]
    universe: tuple[str, ...]
    release_id: str
    session_days: tuple[int, ...] = ()
    session_loader: Callable[[int], DayTradeCarrySession] | None = None
    compact_session_loader: Callable[[int], DayTradeCarrySession] | None = None
    packed_session_loader: Callable[[int], PackedDayTradeCarrySession] | None = None
    audit_receipt: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        _validate_checkpoint_identity(self.universe, self.release_id, len(self.universe))
        if self.audit_receipt is not None and not isinstance(self.audit_receipt, dict):
            raise ValueError("physical source audit receipt must be structured")
        if self.sessions and self.session_loader is not None:
            raise ValueError("physical source cannot mix eager sessions and a lazy loader")
        days = (
            tuple(session.day for session in self.sessions)
            if self.sessions
            else tuple(self.session_days)
        )
        if not days or any(a >= b for a, b in zip(days, days[1:])):
            raise ValueError("physical source requires ordered unique sessions")
        if self.sessions:
            if self.session_days and tuple(self.session_days) != days:
                raise ValueError("physical source eager dates differ from declared dates")
            for session in self.sessions:
                session.validate_shape(len(self.universe), session.official_open.device)
        elif self.session_loader is None:
            raise ValueError("lazy physical source requires a session loader")
        object.__setattr__(self, "session_days", days)

    def __len__(self) -> int:
        return len(self.session_days)

    def day_at(self, row: int) -> int:
        return self.session_days[int(row)]

    def session_at(self, row: int) -> DayTradeCarrySession:
        index = int(row)
        if index < 0 or index >= len(self):
            raise IndexError("physical source row is outside its pinned calendar")
        session = (
            self.sessions[index]
            if self.sessions
            else self.session_loader(index)  # type: ignore[misc]
        )
        if session.day != self.session_days[index]:
            raise ValueError("lazy physical session differs from its pinned calendar")
        session.validate_shape(len(self.universe), session.official_open.device)
        return session

    def compact_session_at(self, row: int) -> DayTradeCarrySession:
        index = int(row)
        if index < 0 or index >= len(self):
            raise IndexError("physical source row is outside its pinned calendar")
        if self.sessions:
            session = compact_day_trade_carry_session(self.sessions[index])
        elif self.compact_session_loader is not None:
            session = self.compact_session_loader(index)
        else:
            session = compact_day_trade_carry_session(self.session_at(index))
        if session.day != self.session_days[index]:
            raise ValueError("compact physical session differs from its pinned calendar")
        session.validate_shape(len(self.universe), session.official_open.device)
        if not session.uses_sparse_events:
            raise ValueError("compact physical loader returned a dense minute session")
        return session

    def rows(
        self,
        row_indices: list[int],
        symbol_indices: list[int],
        device: torch.device,
    ) -> tuple[DayTradeCarrySession, ...]:
        if (
            not row_indices
            or any(a + 1 != b for a, b in zip(row_indices, row_indices[1:]))
            or min(row_indices) < 0
            or max(row_indices) >= len(self)
        ):
            raise ValueError("physical source rows must be a nonempty contiguous interval")
        if (
            not symbol_indices
            or len(set(symbol_indices)) != len(symbol_indices)
            or min(symbol_indices) < 0
            or max(symbol_indices) >= len(self.universe)
        ):
            raise ValueError("physical source requires a unique in-range symbol mapping")
        identity = symbol_indices == list(range(len(self.universe)))
        result = []
        for row in row_indices:
            session = self.session_at(row)
            values = {}
            index_by_device = {}
            for field in fields(session):
                value = getattr(session, field.name)
                if isinstance(value, torch.Tensor):
                    if identity:
                        selected = value
                    else:
                        index = index_by_device.get(value.device)
                        if index is None:
                            index = index_by_device[value.device] = torch.tensor(
                                symbol_indices, device=value.device
                            )
                        selected = value.index_select(0, index)
                    values[field.name] = selected.to(device=device, dtype=torch.float64)
            result.append(replace(session, **values))
        return tuple(result)

    def batch(self, split, start: int, end: int, device: torch.device):
        staged = self.prepare_batch(split, start, end)
        return staged.sessions(device), staged.count

    def prepare_batch(
        self, split, start: int, end: int, *, event_compression: bool = False
    ) -> PreparedDayTradeCarryBatch:
        """Decode and pack the next chronological batch without touching CUDA."""
        if split.execution_mode != "tw_day_trade":
            raise ValueError("physical source attached to a different execution mode")
        if start < 0 or end <= start or start >= len(split):
            raise ValueError("physical batch requires a nonempty in-range row interval")
        if len(self) != int(split.features.shape[0]):
            raise ValueError("physical source and base feature calendars differ")
        rows = split._valid_indices_cpu[start:end]
        # Padding is a trainer representation, never a second exchange session.
        sample = (torch.ones(len(rows), dtype=torch.bool) if split.sample_mask is None
                  else split.sample_mask[start:end].detach().cpu().bool())
        count = int(sample.sum())
        if count == 0 or not torch.equal(sample, torch.arange(len(rows)) < count):
            raise ValueError("physical batch allows only a nonempty valid prefix and trailing padding")
        rows = rows[:count].tolist()
        if min(rows) < 0 or max(rows) >= len(self):
            raise ValueError("physical batch indexes outside its pinned source calendar")
        if any(a + 1 != b for a, b in zip(rows, rows[1:])):
            raise ValueError("physical batch skips, repeats or reorders an exchange session")
        indices = (list(range(len(self.universe))) if split.symbol_indices is None
                   else split.symbol_indices.detach().cpu().tolist())
        if (not indices or len(set(indices)) != len(indices)
                or min(indices) < 0 or max(indices) >= len(self.universe)
                or len(indices) != int(split.features.shape[1])):
            raise ValueError("physical batch differs from the pinned symbol universe")
        identity = indices == list(range(len(self.universe)))
        raw_workers = os.environ.get(
            "STOCKAGENT_DAY_TRADE_SOURCE_DECODE_WORKERS", "4"
        )
        try:
            decode_workers = int(raw_workers)
        except ValueError as exc:
            raise ValueError(
                "STOCKAGENT_DAY_TRADE_SOURCE_DECODE_WORKERS must be positive"
            ) from exc
        if decode_workers <= 0:
            raise ValueError(
                "STOCKAGENT_DAY_TRADE_SOURCE_DECODE_WORKERS must be positive"
            )
        raw_sparse = str(
            os.environ.get("STOCKAGENT_DAY_TRADE_SPARSE_EVENTS", "0")
        ).strip().lower()
        sparse_events = bool(event_compression) and raw_sparse not in {
            "0", "false", "off", "no"
        }
        packed_transport = (
            not sparse_events
            and identity
            and self.packed_session_loader is not None
        )
        if sparse_events:
            row_loader = self.compact_session_at
        elif packed_transport:
            row_loader = self.packed_session_loader
        else:
            row_loader = self.session_at
        if self.session_loader is not None and identity and len(rows) > 1:
            # Each day owns an independent immutable NPZ. Parallel decoding is
            # safe and preserves order because executor.map is ordered.
            with ThreadPoolExecutor(
                max_workers=min(decode_workers, len(rows)),
                thread_name_prefix="physical-session-decode",
            ) as pool:
                sessions = tuple(pool.map(row_loader, rows))
        else:
            if event_compression and identity:
                sessions = tuple(row_loader(row) for row in rows)
            else:
                sessions = self.rows(rows, indices, torch.device("cpu"))
        if packed_transport:
            return PreparedDayTradeCarryBatch.from_packed_sessions(
                sessions,  # type: ignore[arg-type]
                count,
                event_compression=event_compression,
            )
        return PreparedDayTradeCarryBatch.from_sessions(
            sessions,  # type: ignore[arg-type]
            count,
            preserve_cpu_sessions=bool(self.sessions),
            event_compression=event_compression,
            sparse_events=sparse_events,
        )


def prefetch_physical_carry_batches(
    source: PreparedDayTradeCarrySource,
    split,
    batch_order: list[int],
    batch_size: int,
    *,
    event_compression: bool | None = None,
):
    """Overlap next-batch NPZ decode/CPU packing with current CUDA training."""
    if batch_size <= 0 or not batch_order:
        raise ValueError("physical prefetch requires a nonempty positive batch plan")
    if batch_order != list(range(len(batch_order))):
        raise ValueError("physical carry prefetch requires chronological batch order")

    if event_compression is None:
        raw_compression = str(
            os.environ.get("STOCKAGENT_DAY_TRADE_EVENT_COMPRESSION", "1")
        ).strip().lower()
        event_compression = raw_compression not in {"0", "false", "off", "no"}

    def prepare(batch_index: int):
        start = batch_index * batch_size
        end = min(start + batch_size, len(split))
        return source.prepare_batch(
            split, start, end, event_compression=bool(event_compression)
        )

    raw_depth = os.environ.get("STOCKAGENT_DAY_TRADE_PREFETCH_DEPTH", "2")
    try:
        depth = int(raw_depth)
    except ValueError as exc:
        raise ValueError("STOCKAGENT_DAY_TRADE_PREFETCH_DEPTH must be positive") from exc
    if depth <= 0:
        raise ValueError("STOCKAGENT_DAY_TRADE_PREFETCH_DEPTH must be positive")
    depth = min(depth, len(batch_order))
    with ThreadPoolExecutor(
        max_workers=depth, thread_name_prefix="physical-carry-prefetch"
    ) as pool:
        futures = {
            offset: pool.submit(prepare, batch_order[offset])
            for offset in range(depth)
        }
        for offset, batch_index in enumerate(batch_order):
            staged = futures.pop(offset).result()
            next_offset = offset + depth
            if next_offset < len(batch_order):
                futures[next_offset] = pool.submit(
                    prepare, batch_order[next_offset]
                )
            yield batch_index, staged


def bind_physical_carry_loss(
    loss_fn: Callable, *, source: PreparedDayTradeCarrySource, split,
    start: int, end: int, device: torch.device, previous: DayTradeCarryState | None,
    prepared_batch: PreparedDayTradeCarryBatch | None = None,
    event_compression: bool = True,
) -> Callable:
    """Bind an exact batch; model padding never advances inventory or interest."""
    staged = (
        source.prepare_batch(split, start, end, event_compression=event_compression)
        if prepared_batch is None
        else prepared_batch
    )
    compression_env = str(
        os.environ.get("STOCKAGENT_DAY_TRADE_EVENT_COMPRESSION", "1")
    ).strip().lower()
    compression_enabled = bool(event_compression) and (
        compression_env not in {"0", "false", "off", "no"}
    )
    if staged.event_compression != compression_enabled:
        staged = source.prepare_batch(
            split, start, end, event_compression=compression_enabled
        )
    sessions, count = staged.sessions(device), staged.count
    expected_rows = split._valid_indices_cpu[start : start + count].tolist()
    if (
        not expected_rows
        or tuple(source.day_at(row) for row in expected_rows) != staged.days
    ):
        raise ValueError("prefetched physical batch differs from the requested interval")
    if previous is not None:
        first_row = int(split._valid_indices_cpu[start])
        if first_row <= 0 or previous.last_session_day != source.day_at(first_row - 1):
            raise ValueError("physical batch skipped or repeated a source session")

    def call(weights, returns, mask, **kwargs):
        if not bool(torch.isfinite(weights[:count]).all()):
            raise FloatingPointError("physical training model actions are nonfinite")
        supplied = dict(kwargs)
        aux = supplied.get("aux_outputs")
        if not isinstance(aux, dict):
            raise ValueError("physical trainer requires its recurrent aux channel")
        # Translate causal panel obligations into policy constraints before
        # discarding the legacy daily ledger fields.  Avoidance/terminal rows
        # target zero; a statutory short-cover row targets zero only for an
        # existing/requested short.  The physical 09:01 reduction still needs
        # an observed price and capacity, so this never guarantees a fill.
        force_exit = supplied.pop("force_exit_mask", None)
        unresolved = supplied.pop("unresolved_corporate_action_mask", None)
        force_cover = supplied.pop("force_short_cover_mask", None)
        must_flat = torch.zeros_like(weights[:count], dtype=torch.bool)
        for name, value in (
            ("force_exit_mask", force_exit),
            ("unresolved_corporate_action_mask", unresolved),
        ):
            if value is not None:
                current = value[:count]
                if current.shape != must_flat.shape:
                    raise ValueError(f"physical {name} differs from model actions")
                must_flat |= current.bool()
        physical_weights = torch.where(must_flat, 0, weights[:count])
        if force_cover is not None:
            cover = force_cover[:count].bool()
            if cover.shape != must_flat.shape:
                raise ValueError("physical force_short_cover_mask differs from model actions")
            physical_weights = torch.where(
                cover & (physical_weights < 0), 0, physical_weights
            )
            for key in ("can_short_open_mask", "can_short_open_open_mask"):
                value = supplied.get(key)
                if isinstance(value, torch.Tensor):
                    supplied[key] = value[:count].bool() & ~cover
        # The accepted source owns all price/volume paths.  The old compressed
        # tape and close-gap proxy remain attached only for legacy coverage and
        # checkpoint migration; they must not be executed a second time.
        supplied.pop("overnight_log_returns", None)
        for key in ("cash_dividend_yield", "cash_dividend_payment_delay_sessions"):
            value = supplied.pop(key, None)
            if value is not None and not bool((value[:count] == 0).all()):
                raise ValueError(
                    f"physical avoid-mode source cannot consume exact field {key}"
                )
        advance = supplied.pop("state_advance_mask", None)
        if advance is not None and not bool(advance[:count].bool().all()):
            raise ValueError("physical source cannot skip an owned exchange session")
        # Capacity is explicitly owned by the prepared 50%-minute session;
        # the daily-volume and short-availability proxies do not govern it.
        supplied.pop("volume_limit_weights", None)
        supplied.pop("short_capacity_weights", None)
        supplied.pop("sample_mask", None)
        for key in ("benchmark_returns", "can_buy_mask", "can_sell_mask", "can_short_open_mask",
                    "can_short_open_open_mask", "day_trade_eligible_mask",
                    "day_trade_can_buy_open_mask", "day_trade_can_sell_open_mask",
                    "session_month_ids", "commission_rebate_payment_eligible_mask"):
            value = supplied.get(key)
            if isinstance(value, torch.Tensor):
                supplied[key] = value[:count]
        # Legacy initial_* entries are the trainer's old account buffers, not
        # a physical FIFO state. The caller retains only the explicit state.
        physical_aux: dict[str, Any] = {}

        def run_bound_loss() -> tuple[torch.Tensor, DayTradeCarryState]:
            physical_aux.clear()
            physical_aux["initial_day_trade_carry_state"] = previous
            supplied["aux_outputs"] = physical_aux
            supplied["day_trade_carry_event_compression"] = compression_enabled
            try:
                current_result = loss_fn(
                    physical_weights,
                    returns[:count],
                    mask[:count],
                    day_trade_carry_sessions=sessions,
                    **supplied,
                )
            except DayTradeCarryEventCompressionFallback:
                # The sparse path is accepted only after its conservative
                # solvency proof. An inconclusive proof pays the rare cost of
                # replaying all 270 authoritative minutes for this batch.
                dense = source.prepare_batch(
                    split, start, end, event_compression=False
                ).sessions(device)
                physical_aux.clear()
                physical_aux["initial_day_trade_carry_state"] = previous
                supplied["aux_outputs"] = physical_aux
                supplied["day_trade_carry_event_compression"] = False
                current_result = loss_fn(
                    physical_weights,
                    returns[:count],
                    mask[:count],
                    day_trade_carry_sessions=dense,
                    **supplied,
                )
            current_final = physical_aux.get("_final_day_trade_carry_state")
            if not isinstance(current_final, DayTradeCarryState):
                raise RuntimeError("physical loss did not return its account state")
            return current_result, current_final

        result, final = run_bound_loss()
        if not isinstance(final, DayTradeCarryState) or final.last_session_day != sessions[-1].day:
            raise RuntimeError("physical loss did not commit its final session state")
        # Never let the generic nonfinite-batch skip path discard a failed
        # chronological account and continue with the next day's policy.
        local_failed = (
            not bool(torch.isfinite(result).all())
            or bool(final.inventory.failed != 0)
        )
        if local_failed:
            audit_valid, diagnosis = _diagnose_physical_trajectory_failure(
                loss_fn,
                source=source,
                split=split,
                start=start,
                end=end,
                weights=physical_weights,
                returns=returns,
                mask=mask,
                supplied=supplied,
                previous=previous,
                count=count,
            )
            if audit_valid:
                evicted_paths, evicted_sessions = (
                    invalidate_day_trade_carry_compiled_caches()
                )
                try:
                    retry_result, retry_final = run_bound_loss()
                    retry_valid = (
                        bool(torch.isfinite(retry_result).all())
                        and bool(retry_final.inventory.failed == 0)
                        and retry_final.last_session_day == sessions[-1].day
                    )
                except Exception as retry_error:
                    retry_valid = False
                    diagnosis += (
                        "; compiled_retry="
                        f"{type(retry_error).__name__}: {retry_error}"
                    )
                if retry_valid:
                    warnings.warn(
                        "physical compiled trajectory disagreed with the eager "
                        "authoritative audit; invalidated process-local caches "
                        f"(paths={evicted_paths}, sessions={evicted_sessions}) "
                        "and recovered the unchanged batch before backward",
                        RuntimeWarning,
                        stacklevel=2,
                    )
                    aux.update(physical_aux)
                    return retry_result
                diagnosis += "; compiled_retry_failed_after_cache_invalidation"
            raise FloatingPointError(
                "physical training trajectory failed; cannot skip a batch; "
                f"{diagnosis}"
            )
        aux.update(physical_aux)
        return result

    return call


def bind_physical_carry_backtest(
    runner: Callable, *, source: PreparedDayTradeCarrySource, split,
    start: int, end: int, device: torch.device, previous: DayTradeCarryState | None,
    event_compression: bool = False,
) -> Callable:
    """Adapt the canonical eval chunk, retaining its actual physical endpoint."""
    # Share padding, legacy-field rejection and chronology with the loss
    # adapter. This closure translates names only, never recalculates returns.
    def invoke(weights, returns, mask, *, aux_outputs, **kwargs):
        kwargs["overnight_returns"] = kwargs.pop("overnight_log_returns", None)
        benchmark = kwargs.pop("benchmark_returns")
        result = runner(weights, returns, mask, benchmark,
                        initial_day_trade_carry_state=previous, **kwargs)
        aux_outputs["_final_day_trade_carry_state"] = result.day_trade_carry_state
        invoke.result = result
        return result.strategy_returns

    invoke.result = None
    bound = bind_physical_carry_loss(invoke, source=source, split=split,
                                     start=start, end=end, device=device, previous=previous,
                                     event_compression=event_compression)

    def call(weights, returns, mask, benchmark, buy_fee_rate, sell_fee_rate, **kwargs):
        supplied = {key: value for key, value in kwargs.items() if not key.startswith("initial_")}
        supplied["overnight_log_returns"] = supplied.pop("overnight_returns", None)
        bound(weights, returns, mask, benchmark_returns=benchmark,
              buy_fee_rate=buy_fee_rate, sell_fee_rate=sell_fee_rate, aux_outputs={}, **supplied)
        if (
            event_compression
            and invoke.result is not None
            and invoke.result.minute_nav.shape[-1] != 2
        ):
            minute_nav = invoke.result.minute_nav
            invoke.result = replace(
                invoke.result,
                minute_nav=torch.stack(
                    (minute_nav.amin(dim=-1), minute_nav[:, -1]), dim=-1
                ),
            )
        return invoke.result

    return call
