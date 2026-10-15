"""Whole-account ownership for the canonical annual physical trainer.

Dates within an account remain ordered. Model parameters stay fixed until all
owned accounts have contributed to the single global trajectory update.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping

import numpy as np
import torch

from stockagent.data.walkforward import period_labels_from_contract


EPISODE_PARALLEL_ABI = "annual_whole_account_local_actions_weighted_gradient_mean_v2"


@dataclass(frozen=True)
class AnnualEpisodePlan:
    # Source-split row offsets, excluding inert padding. Each account has one
    # owner. Local blocks never mix dates from different accounts.
    spans_by_rank: tuple[tuple[tuple[int, int], ...], ...]
    periods_by_rank: tuple[tuple[int, ...], ...]
    rows_by_rank: tuple[int, ...]
    total_valid_rows: int
    block_rows: int
    accounts_by_rank: tuple[tuple[tuple[int, int, int], ...], ...]

    def receipt(self) -> dict[str, Any]:
        return {"abi": EPISODE_PARALLEL_ABI, "spans_by_rank": self.spans_by_rank,
                "periods_by_rank": self.periods_by_rank,
                "rows_by_rank": self.rows_by_rank,
                "total_valid_rows": self.total_valid_rows, "block_rows": self.block_rows,
                "accounts_by_rank": self.accounts_by_rank,
                "reporting_rank": 0,
                "gradient_weighting": "world_size * owned_valid_rows / global_valid_rows",
                "update_cadence": "one_clip_optimizer_and_scheduler_step_per_epoch",
                "bptt": "detach_at_owned_block_and_annual_account_boundaries"}


def plan_annual_episode_ownership(split, source, *,
                                 period_contract: Mapping[str, Any] | None,
                                 world_size: int, global_batch_rows: int,
                                 block_rows: int = 0) -> AnnualEpisodePlan:
    """Balance complete configured periods; never infer ownership from Jan 1."""
    if world_size < 2 or global_batch_rows < world_size:
        raise ValueError("annual episode sharding requires at least two ranks and a valid global batch")
    if global_batch_rows % world_size:
        raise ValueError("global model batch must be divisible by world size")
    rows_per_block = int(block_rows) or global_batch_rows // world_size
    if rows_per_block < 1 or rows_per_block > global_batch_rows:
        raise ValueError("episode block rows must be within the global batch")
    sample_mask = split.sample_mask
    if sample_mask is None:
        sample_mask = torch.ones(len(split), dtype=torch.bool)
    sample = sample_mask.detach().to(device="cpu", dtype=torch.bool).numpy()
    n = int(sample.sum())
    if n <= 0 or not bool(sample[:n].all()) or bool(sample[n:].any()):
        raise ValueError("annual ownership requires valid chronological rows followed by inert padding")
    indices = split._valid_indices_cpu[:n].numpy()
    if indices.size > 1 and not bool((np.diff(indices) == 1).all()):
        raise ValueError("annual ownership cannot skip or repeat physical sessions")

    def labels(days):
        # Physical day_at uses Gregorian ordinals, not days since 1970.
        dates = np.asarray([date.fromordinal(int(day)) for day in days], dtype="datetime64[D]")
        return (dates.astype("datetime64[Y]").astype(np.int64) + 1970
                if period_contract is None
                else period_labels_from_contract(dates, period_contract))

    periods = labels([source.day_at(int(index)) for index in indices])
    cuts = [0, *(int(index) + 1 for index in np.flatnonzero(periods[1:] != periods[:-1])), n]
    episodes = [(int(periods[start]), start, stop)
                for start, stop in zip(cuts[:-1], cuts[1:], strict=True)]
    if len({period for period, _, _ in episodes}) != len(episodes):
        raise ValueError("an annual account appears in disjoint source intervals")
    if len(episodes) < world_size:
        raise ValueError("annual episode sharding needs at least one complete account per rank")
    for period, start, _ in episodes:
        source_row = int(indices[start])
        if source_row > 0 and int(labels([source.day_at(source_row - 1)])[0]) == period:
            raise ValueError("an owned account omits its first configured physical session")

    loads = [0] * world_size
    owned: list[list[tuple[int, int, int]]] = [[] for _ in range(world_size)]
    for period, start, stop in sorted(episodes, key=lambda item: (-(item[2] - item[1]), item[0])):
        rank = min(range(world_size), key=lambda rank: (loads[rank], rank))
        owned[rank].append((period, start, stop))
        loads[rank] += stop - start
    all_spans, all_periods = [], []
    for accounts in owned:
        spans = []
        accounts.sort(key=lambda item: item[1])
        for _, start, stop in accounts:
            cursor = start
            while cursor < stop:
                end = min(stop, cursor + rows_per_block)
                spans.append((cursor, end))
                cursor = end
        all_spans.append(tuple(spans))
        all_periods.append(tuple(period for period, _, _ in accounts))
    return AnnualEpisodePlan(tuple(all_spans), tuple(all_periods), tuple(loads), n, rows_per_block,
                             tuple(tuple(accounts) for accounts in owned))


def validate_annual_episode_receipt(receipt: Mapping[str, Any], *, train_rows: int,
                                   global_batch_rows: int, world_size: int) -> dict[str, Any]:
    """Independently verify complete accounts, exact date coverage and NN work."""
    if receipt.get("abi") != EPISODE_PARALLEL_ABI or world_size < 2:
        raise ValueError("incompatible annual ownership ABI or world size")
    if (receipt.get("total_valid_rows") != train_rows
            or receipt.get("block_rows") != global_batch_rows // world_size
            or receipt.get("reporting_rank") != 0
            or receipt.get("update_cadence") != "one_clip_optimizer_and_scheduler_step_per_epoch"
            or receipt.get("gradient_weighting") != "world_size * owned_valid_rows / global_valid_rows"):
        raise ValueError("annual ownership row/batch/gradient contract mismatch")
    keys = ("spans_by_rank", "accounts_by_rank", "periods_by_rank", "rows_by_rank")
    if any(len(receipt.get(key, ())) != world_size for key in keys):
        raise ValueError("annual ownership rank count mismatch")
    accounts = []
    counts, batches = [], []
    for rank in range(world_size):
        owned = receipt["accounts_by_rank"][rank]
        if not owned or list(receipt["periods_by_rank"][rank]) != [a[0] for a in owned]:
            raise ValueError("annual ownership account labels mismatch")
        spans = [tuple(s) for s in receipt["spans_by_rank"][rank]]
        expected = []
        rows = 0
        for period, start, stop in owned:
            if not (0 <= start < stop <= train_rows):
                raise ValueError("annual ownership has an invalid account interval")
            accounts.append((period, start, stop))
            rows += stop - start
            expected.extend((cursor, min(stop, cursor + receipt["block_rows"]))
                            for cursor in range(start, stop, receipt["block_rows"]))
        if spans != expected or rows != receipt["rows_by_rank"][rank]:
            raise ValueError("annual ownership spans or date weights mismatch")
        counts.append(rows)
        batches.append(len(spans))
    accounts.sort(key=lambda a: a[1])
    if (accounts[0][1] != 0 or accounts[-1][2] != train_rows
            or any(a[2] != b[1] for a, b in zip(accounts, accounts[1:]))
            or len({a[0] for a in accounts}) != len(accounts)):
        raise ValueError("annual ownership duplicates or omits an account or physical date")
    slots = sum(batches) * receipt["block_rows"]
    return {"ok": True, "rows_by_rank": counts, "batches_by_rank": batches,
            "reporting_rank_batches": batches[0], "padded_slots": slots,
            "padding_fraction": (slots - train_rows) / slots,
            "ownership": dict(receipt)}
