"""Full-schema affine projection from resident immutable CSR observations."""
from __future__ import annotations

import torch
from torch.nn import functional as F

from stockagent.data.factorized_sparse_rows import FactorizedSparseRowStore


def project_factorized_sparse_rows(model, slab):
    candle = model.candle_encoder
    store = model.__dict__.get("_factorized_sparse_row_store")
    if store is None:
        store = FactorizedSparseRowStore(slab.source, candle, slab.device,
            budget_bytes=model.factorized_sparse_cache_bytes)
        object.__setattr__(model, "_factorized_sparse_row_store", store)
    store.validate(slab.source, candle)
    before = store.host_payload_bytes
    misses_before = store.misses
    norm, projection = candle.input_norm, candle.joint_projection.proj
    with torch.autocast(slab.device.type, enabled=False):
        # Every logical parameter participates. Observed centered values use
        # CSR; omitted positive zeros retain their centered affine effect.
        weight = projection.weight.float()
        # CSR CUDA kernels require consecutive output channels for coalesced
        # weight reads; a transposed view caused the measured strided-read bottleneck.
        effective = (weight * norm.weight.float()).t().contiguous()
        total = effective.sum(dim=0)
        offset = (None if getattr(norm, "bias", None) is None
                  else F.linear(norm.bias.float(), weight))
        outputs = []
        for values, zero_value in store.slices(slab.rows):
            projected = torch.sparse.mm(values, effective)
            projected = projected + zero_value[:, None] * total
            if offset is not None:
                projected = projected + offset
            if projection.bias is not None:
                projected = projected + projection.bias.float()
            outputs.append(projected)
        projected = torch.cat(outputs, dim=0).reshape(len(slab.rows), slab.size(1), -1)
        result = candle._finish_embedding(candle._activate_projected(projected))
    if slab.device.type == "cuda" and store.misses != misses_before:
        # Cold construction and CSR backward use differently sized transient
        # workspaces. Keep live source/autograd tensors, but return a large
        # inactive reservation before growing the persistent source cache.
        # Warm epochs never perform this allocator synchronization.
        inactive = (torch.cuda.memory_reserved(slab.device)
                    - torch.cuda.memory_allocated(slab.device))
        if inactive > 2 * 1024**3:
            torch.cuda.empty_cache()
            store.__dict__["cold_allocator_releases"] = (
                store.__dict__.get("cold_allocator_releases", 0) + 1)
    stats = slab.source.transfer_stats
    stats["host_payload_bytes"] += store.host_payload_bytes - before
    stats["sparse_resident_bytes"] = store.bytes
    stats["sparse_cache_hits"] = store.hits
    stats["sparse_cache_misses"] = store.misses
    if store.disk is not None:
        for key, value in store.disk.stats.items():
            stats[f"sparse_disk_{key}"] = value
    stats["sparse_cold_allocator_releases"] = store.__dict__.get("cold_allocator_releases", 0)
    return result

