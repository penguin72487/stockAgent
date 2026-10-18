"""Bounded CPU snapshots of checkpoint storages without per-tensor CUDA waits."""
from __future__ import annotations

from collections.abc import Mapping
from copy import copy
import hashlib
import json
from typing import Any

import torch


_MANIFEST_ENCODING = 'stockagent_checkpoint_manifest_json_v1'


def encode_checkpoint_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Encode only JSON metadata; tensor state and the original mapping stay intact."""
    result = dict(payload)
    manifest = payload.get('experiment_manifest')
    if not isinstance(manifest, dict):
        return result
    if manifest.get('encoding') == _MANIFEST_ENCODING:
        return result
    try:
        body = json.dumps(manifest, ensure_ascii=True, allow_nan=False,
                          separators=(',', ':')).encode('utf-8')
        # JSON is admissible only when it preserves the exact metadata types
        # and values. Older/custom metadata remains on native torch.save.
        if json.loads(body) != manifest:
            return result
    except (TypeError, ValueError):
        return result
    result['experiment_manifest'] = dict(encoding=_MANIFEST_ENCODING,
        payload=body, sha256=hashlib.sha256(body).hexdigest())
    return result


def decode_checkpoint_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    """Restore the canonical manifest before any compatibility/resume check."""
    manifest = payload.get('experiment_manifest')
    if not isinstance(manifest, dict) or manifest.get('encoding') != _MANIFEST_ENCODING:
        return payload
    if set(manifest) != {'encoding', 'payload', 'sha256'}:
        raise ValueError('invalid encoded checkpoint manifest envelope')
    body = manifest['payload']
    if not isinstance(body, bytes) or hashlib.sha256(body).hexdigest() != manifest['sha256']:
        raise ValueError('checkpoint manifest SHA256 mismatch')
    def reject_constant(value):
        raise ValueError(f'non-finite JSON checkpoint manifest constant: {value}')
    decoded = json.loads(body, parse_constant=reject_constant)
    if not isinstance(decoded, dict):
        raise TypeError('decoded checkpoint manifest must be a mapping')
    result = dict(payload)
    result['experiment_manifest'] = decoded
    return result


def cpu_checkpoint_snapshot(payload: Any, *, max_pack_bytes: int = 64 * 1024**2) -> Any:
    """Copy CUDA storages in bounded groups, preserving values, views and metadata.

    The caller still owns checkpoint validation, fsync and atomic publication.
    CPU tensors are already synchronous and stay unchanged. Each CUDA group is
    copied synchronously once before its temporary GPU allocation is released.
    This snapshot must be taken while the training owner is not updating state.
    """
    if max_pack_bytes <= 0:
        raise ValueError('max_pack_bytes must be positive')
    tensors: dict[int, torch.Tensor] = {}

    def collect(value):
        if torch.is_tensor(value):
            if value.device.type == 'cuda':
                tensors[id(value)] = value
        elif isinstance(value, Mapping):
            for item in value.values():
                collect(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                collect(item)

    collect(payload)
    if not tensors:
        return payload
    snapshots: dict[int, torch.Tensor] = {}
    groups: dict[tuple[torch.device, torch.dtype], dict[int, tuple[Any, list[torch.Tensor]]]] = {}
    for tensor in tensors.values():
        if tensor.layout != torch.strided or tensor.is_quantized or tensor.is_conj() or tensor.is_neg():
            snapshots[id(tensor)] = tensor.detach().cpu()
            continue
        storage = tensor.untyped_storage()
        groups.setdefault((tensor.device, tensor.dtype), {}).setdefault(
            storage._cdata, (storage, []))[1].append(tensor)

    def transfer(device, dtype, storages):
        element_size = torch.empty((), dtype=dtype).element_size()
        views, offsets, offset = [], [], 0
        for storage, owners in storages:
            size = storage.nbytes()
            if size % element_size:
                raise ValueError('checkpoint storage size is not aligned to its dtype')
            view = torch.empty(0, dtype=dtype, device=device).set_(
                storage, 0, (size // element_size,), (1,))
            offsets.append(offset)
            views.append(view)
            offset += view.numel()
        # One D2H synchronization for all the storages of this bounded group.
        packed = (views[0] if len(views) == 1 else torch.cat(views)).cpu()
        for (storage, owners), base in zip(storages, offsets, strict=True):
            for tensor in owners:
                snapshot = torch.empty(0, dtype=dtype).set_(
                    packed.untyped_storage(), base + tensor.storage_offset(),
                    tensor.size(), tensor.stride())
                if tensor.requires_grad:
                    snapshot.requires_grad_(True)
                snapshots[id(tensor)] = snapshot

    with torch.no_grad():
        for (device, dtype), storages in groups.items():
            chunk, size = [], 0
            for entry in storages.values():
                nbytes = entry[0].nbytes()
                if chunk and size + nbytes > max_pack_bytes:
                    transfer(device, dtype, chunk)
                    chunk, size = [], 0
                chunk.append(entry)
                size += nbytes
            if chunk:
                transfer(device, dtype, chunk)

    def replace(value):
        if torch.is_tensor(value):
            return snapshots.get(id(value), value)
        if isinstance(value, dict):
            result = copy(value)  # Retains OrderedDict state_dict._metadata.
            for key, item in value.items():
                result[key] = replace(item)
            return result
        if isinstance(value, list):
            return [replace(item) for item in value]
        if isinstance(value, tuple):
            items = [replace(item) for item in value]
            return type(value)(*items) if hasattr(value, '_fields') else tuple(items)
        if isinstance(value, Mapping):
            raise TypeError(f'unsupported checkpoint mapping {type(value).__name__}')
        return value

    return replace(payload)
