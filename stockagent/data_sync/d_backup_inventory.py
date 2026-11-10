"""Bounded, read-only classification of the existing published D catalog.

This describes catalogued encoded objects, not allocated disk usage, backup
acceptance, private data, or permission to retire anything. It never opens a
payload or contacts the owner of the catalog.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import stat
import math

import ijson

CONTRACT = "stockagent.d_backup_inventory.v1"
CLASSIFIER_REVISION = 2
GROUPS = (
    ("taiwan", "台股與台灣公開資料"),
    ("derivatives", "台灣期貨、選擇權與衍生資料"),
    ("crypto", "加密貨幣與鏈上資料"),
    ("global", "全球市場、外匯與總經"),
    ("recipes", "訓練資料配方與版本"),
    ("research", "研究與訓練產物"),
    ("vast", "Vast 歷史研究回傳"),
    ("legacy", "舊 WSL 歷史歸檔"),
    ("other", "其他已登錄資料"),
)
LABELS = dict(GROUPS)
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
DIGEST = re.compile(r"[0-9a-f]{64}\Z")
MAX_REFS = 1_000_000
MAX_DATASETS = 20_000
MAX_RELEASE_REFS = 500_000


def group_for(name: str) -> str:
    s = name.lower()
    words = set(re.split(r"[-_.]+", s))
    if s.startswith("legacy-wsl-"):
        return "legacy"
    if s.startswith("training-dataset-recipes"):
        return "recipes"
    if s.startswith("legacy-vast-"):
        return "vast"
    if s.startswith(("artifact-", "training-", "research-", "completed-tw-forward-")):
        return "research"
    if words & {"future", "futures", "option", "options", "taifex", "derivative", "derivatives"}:
        return "derivatives"
    if words & {"crypto", "binance", "okx", "bybit", "bitcoin", "blockchain", "dune"}:
        return "crypto"
    if s.startswith(("tw-", "tw_", "twpublic", "taiwan-", "tpex-", "twse-", "legacy-markets-tw")) or words & {"finlab", "finmind"}:
        return "taiwan"
    if words & {"openbb", "yahoo", "forex", "fx", "macro", "economic", "fred", "oecd", "imf", "worldbank", "cftc"}:
        return "global"
    return "other"


def _generation(st: os.stat_result) -> tuple[int, int, int, int, int]:
    return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns


class _MeteredReader:
    def __init__(self, stream, size, progress):
        self.stream, self.size, self.progress = stream, size, progress

    def read(self, size=-1):
        data = self.stream.read(size)
        if self.progress is not None:
            self.progress(self.stream, self.size)
        return data


def classify(catalog: Path, *, expected_identity: str, expected_files: int | None = None,
             expected_bytes: int | None = None, progress=None) -> dict:
    """Stream one pinned catalog generation and verify its observed identity.

    The identity is the owner's catalog identity; it is not re-hashed here or
    evidence that any referenced file was restored. Shared objects appear in
    every relevant category, but are counted once in the overall total.
    """
    if not DIGEST.fullmatch(expected_identity or ""):
        raise ValueError("invalid_catalog_identity")
    fd = os.open(catalog, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("catalog_not_regular")
        references: dict[str, int] = {}
        datasets: dict[str, dict[str, int]] = defaultdict(dict)
        counts = {key: {"retained_versions": 0, "current_versions": 0,
                        "referenced_files": 0, "referenced_bytes": 0} for key, _ in GROUPS}
        roles: dict[str, dict[str, int]] = defaultdict(lambda: {"files": 0, "bytes": 0})
        release: dict = {}
        release_refs: set[str] = set()
        file: dict = {}
        seen_files: set[str] = set()
        file_count = total_bytes = 0
        identity = contract = None
        for prefix, event, value in ijson.parse(_MeteredReader(stream, before.st_size, progress)):
            if prefix == "releases.item" and event == "start_map":
                release, release_refs = {}, set()
            elif prefix in {"releases.item.dataset", "releases.item.current_head"} and event in {"string", "boolean"}:
                release[prefix.rsplit(".", 1)[-1]] = value
            elif prefix == "releases.item.required_file_keys.item" and event == "string":
                if len(value) > 8192 or len(release_refs) >= MAX_RELEASE_REFS:
                    raise ValueError("release_reference_limit")
                release_refs.add(value)
            elif prefix == "releases.item" and event == "end_map":
                name, current = release.get("dataset"), release.get("current_head")
                if not isinstance(name, str) or not NAME.fullmatch(name) or type(current) is not bool:
                    raise ValueError("invalid_release")
                group = group_for(name)
                counts[group]["retained_versions"] += 1
                counts[group]["current_versions"] += int(current)
                datasets[group][name] = datasets[group].get(name, 0) + 1
                if sum(len(values) for values in datasets.values()) > MAX_DATASETS:
                    raise ValueError("dataset_limit")
                mask = 1 << tuple(LABELS).index(group)
                for key in release_refs:
                    references[key] = references.get(key, 0) | mask
                if len(references) > MAX_REFS:
                    raise ValueError("reference_limit")
            elif prefix == "files.item" and event == "start_map":
                file = {}
            elif prefix in {"files.item.relative", "files.item.sha256", "files.item.role", "files.item.bytes"} and event in {"string", "number"}:
                file[prefix.rsplit(".", 1)[-1]] = value
            elif prefix == "files.item" and event == "end_map":
                relative, digest, size, role = (file.get(k) for k in ("relative", "sha256", "bytes", "role"))
                if (not isinstance(relative, str) or len(relative) > 8192 or not isinstance(digest, str)
                        or not DIGEST.fullmatch(digest) or type(size) is not int or size < 0):
                    raise ValueError("invalid_file_descriptor")
                key = relative + "@" + digest
                if key in seen_files or len(seen_files) >= MAX_REFS:
                    raise ValueError("duplicate_or_excess_file_descriptor")
                seen_files.add(key)
                role = role if role in {"packed_object", "cold_metadata", "unreferenced_cold_object"} else "other"
                roles[role]["files"] += 1
                roles[role]["bytes"] += size
                file_count += 1
                total_bytes += size
                mask = references.pop(key, 0)
                for index, (group, _) in enumerate(GROUPS):
                    if mask & (1 << index):
                        counts[group]["referenced_files"] += 1
                        counts[group]["referenced_bytes"] += size
            elif prefix == "identity_sha256" and event == "string":
                identity = value
            elif prefix == "contract" and event == "string":
                contract = value
        after = os.fstat(stream.fileno())
    if (_generation(before) != _generation(after) or _generation(before) != _generation(os.stat(catalog, follow_symlinks=False))
            or identity != expected_identity or contract != "retained_packed_backup_catalog_v1"):
        raise ValueError("catalog_generation_changed_or_mismatched")
    if expected_files is not None and file_count != expected_files:
        raise ValueError("catalog_file_count_mismatch")
    if expected_bytes is not None and total_bytes != expected_bytes:
        raise ValueError("catalog_byte_count_mismatch")
    return {"contract": CONTRACT, "classifier_revision": CLASSIFIER_REVISION,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "catalog_identity_sha256": identity, "catalog_size_bytes": before.st_size,
            "total_files": file_count, "total_bytes": total_bytes, "unresolved_references": len(references),
            "roles": dict(roles), "groups": [{"id": key, "label": label, **counts[key],
                "datasets": [{"name": name, "versions": versions} for name, versions in sorted(datasets[key].items())]}
                for key, label in GROUPS]}


def _time(value, now: float) -> str | None:
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.isoformat() if parsed.tzinfo is not None and parsed.timestamp() <= now + 300 else None
    except (TypeError, ValueError, OverflowError):
        return None


def public_status(value: object, *, now: float) -> dict | None:
    """Whitelist public aggregates; never expose paths or object identities."""
    if not isinstance(value, dict) or value.get("contract") != CONTRACT:
        return None
    try:
        observed = datetime.fromisoformat(value["observed_at_utc"])
        if observed.tzinfo is None or observed.timestamp() > now + 300:
            return None
    except (KeyError, TypeError, ValueError, OverflowError):
        return None
    def number(value):
        return value if type(value) is int and 0 <= value <= 2**53 - 1 else None
    groups = []
    rows = value.get("groups")
    if not isinstance(rows, list) or len(rows) > len(GROUPS):
        return None
    for row in rows:
        if not isinstance(row, dict) or row.get("id") not in LABELS:
            return None
        datasets = row.get("datasets")
        if not isinstance(datasets, list) or len(datasets) > MAX_DATASETS:
            return None
        clean = [{"name": item["name"], "versions": number(item.get("versions"))}
                 for item in datasets if isinstance(item, dict) and isinstance(item.get("name"), str)
                 and NAME.fullmatch(item["name"])]
        groups.append({"id": row["id"], "label": LABELS[row["id"]],
                       **{k: number(row.get(k)) for k in ("retained_versions", "current_versions", "referenced_files", "referenced_bytes")},
                       "datasets": clean})
    roles = value.get("roles") if isinstance(value.get("roles"), dict) else {}
    source_time = _time(value.get("catalog_observed_at_utc"), now)
    return {"observed_at_utc": observed.isoformat(), "catalog_observed_at_utc": source_time,
            "stale": source_time is None or now - datetime.fromisoformat(source_time).timestamp() > 12 * 3600,
            "total_files": number(value.get("total_files")), "total_bytes": number(value.get("total_bytes")),
            "unresolved_references": number(value.get("unresolved_references")), "groups": groups,
            "roles": {role: {k: number(row.get(k)) for k in ("files", "bytes")}
                      for role, row in roles.items() if role in {"packed_object", "cold_metadata", "unreferenced_cold_object", "other"}
                      and isinstance(row, dict)}}


def public_progress(value: object, *, now: float) -> dict | None:
    if not isinstance(value, dict) or value.get("contract") != CONTRACT:
        return None
    state = value.get("state")
    stamp = _time(value.get("observed_at_utc"), now)
    if state not in {"current", "scanning", "failed"} or stamp is None:
        return None
    stale = now - datetime.fromisoformat(stamp).timestamp() > 180 and state == "scanning"
    done, total = value.get("processed_bytes"), value.get("total_bytes")
    valid = type(done) is int and type(total) is int and 0 <= done <= total <= 2**53 - 1 and total > 0
    eta = _time(value.get("completion_at_utc"), now + 7 * 86400) if state == "scanning" and not stale and valid else None
    if eta and datetime.fromisoformat(eta).timestamp() < now:
        eta = None
    return {"state": "stale" if stale else state, "observed_at_utc": stamp,
            "catalog_observed_at_utc": _time(value.get("catalog_observed_at_utc"), now),
            "processed_bytes": done if valid and state == "scanning" else None,
            "total_bytes": total if valid and state == "scanning" else None,
            "completion_at_utc": eta, "scope": "catalog_classification_only"}


def public_pack_trial(value: object) -> dict | None:
    """Expose a bounded E codec trial, without presenting it as D acceptance."""
    if (not isinstance(value, dict) or value.get("state") != "completed"
            or value.get("source_files_modified") != 0 or value.get("production_codec_changed") is not False
            or not isinstance(value.get("files"), list) or len(value["files"]) > 8):
        return None
    results = []
    for row in value["files"]:
        if (not isinstance(row, dict) or row.get("state") != "roundtrip_verified_source_unchanged"
                or not isinstance(row.get("source"), str) or not NAME.fullmatch(row["source"])
                or type(row.get("source_bytes")) is not int or row["source_bytes"] < 0
                or not isinstance(row.get("candidates"), list) or len(row["candidates"]) > 8):
            return None
        candidates = []
        for item in row["candidates"]:
            if (not isinstance(item, dict) or item.get("codec") != "zstd"
                    or item.get("level") not in {3, 6, 12, 19} or item.get("full_value_roundtrip_verified") is not True
                    or type(item.get("bytes")) is not int or item["bytes"] < 0):
                return None
            times = [item.get("write_fsync_seconds"), item.get("warm_read_decode_seconds")]
            if any(type(t) not in (int, float) or not math.isfinite(t) or not 0 <= t < 86400 for t in times):
                return None
            candidates.append({key: item[key] for key in ("level", "bytes", "write_fsync_seconds", "warm_read_decode_seconds")})
        results.append({"name": row["source"], "source_bytes": row["source_bytes"], "candidates": candidates})
    return {"state": "completed", "files": results, "scope": "e_small_sample_semantic_roundtrip_only",
            "production_changed": False}
