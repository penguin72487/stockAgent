"""All data_* roots share one versioning contract, independent of transport rights.

This is a policy/readback adapter for the existing packed publisher and news
builder. It is not another collector, object pool, scheduler or migration job.
Current source workspaces and fixed legacy releases remain readable as before.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_snapshots import (
    CHUNKED_SNAPSHOT_SCHEMA_VERSION, MAX_FILE_CHUNK_BYTES, resolve_latest_packed,
)


def version_policy(repo_root: Path) -> dict[str, Any]:
    path = repo_root / "configs/columnar_storage.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    policy = document.get("source_versioning", {})
    if (policy.get("contract_version") != 2 or not isinstance(policy.get("roots"), dict)
            or policy.get("reader_contract") != "packed-v2-chunks"
            or policy.get("logical_version") != "complete_inventory_not_delta_chain"
            or policy.get("physical_version") != "reuse_immutable_parts_and_sha256_chunks"
            or policy.get("legacy_payload_deletion") is not False
            or policy.get("local_versioning_grants_replication") is not False):
        raise SnapshotError("invalid common dataset versioning policy")
    count = policy.get("file_chunk_bytes")
    if type(count) is not int or not 1 <= count <= MAX_FILE_CHUNK_BYTES:
        raise SnapshotError("invalid common dataset chunk bound")
    for name, spec in policy["roots"].items():
        if (not name.startswith("data_") or Path(name).name != name or not isinstance(spec, dict)
                or not spec.get("native_layout") or not spec.get("target_grain")):
            raise SnapshotError(f"invalid dataset versioning root: {name!r}")
    return policy


def dataset_roots(repo_root: Path, source: Path, policy: Mapping[str, Any]) -> list[str]:
    source = source.resolve()
    return [name for name in policy["roots"]
            if source == (repo_root / name).resolve() or source.is_relative_to((repo_root / name).resolve())]


def publication_version_options(
    repo_root: Path, entry: Mapping[str, Any], *, reader_contract: str | None,
) -> dict[str, int]:
    """Reader-first opt in for ALL catalogued source roots, never cache families.

    A flag is an explicit operator rollout assertion, not a measured claim that
    remote peers are upgraded. The default legacy mode cannot silently advance
    a shared head to an unreadable protocol. Existing per-entry v2 enrollment
    is validated by the canonical catalog loader, not inferred from file names.
    """
    if "file_chunk_bytes" in entry:
        return {"file_chunk_bytes": entry["file_chunk_bytes"]}
    if reader_contract != "packed-v2-chunks" or "publication_family" in entry:
        return {}
    policy = version_policy(repo_root)
    source = Path(str(entry["source"]))
    source = source if source.is_absolute() else repo_root / source
    if not dataset_roots(repo_root, source, policy):
        return {}
    return {"file_chunk_bytes": policy["file_chunk_bytes"]}


def source_root_entries(repo_root: Path, catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Scope a rollout to the registered data roots, without granting publish rights."""
    policy = version_policy(repo_root)
    selected = []
    for entry in catalog:
        if "publication_family" in entry:
            continue
        source = Path(str(entry["source"]))
        source = source if source.is_absolute() else repo_root / source
        if dataset_roots(repo_root, source, policy):
            selected.append(entry)
    return selected


def inventory_version_layout(
    repo_root: Path, catalog: list[dict[str, Any]], *, sync_root: Path | None = None,
) -> dict[str, Any]:
    """Bounded catalog/head readback; no source recursion or market-data reads."""
    policy = version_policy(repo_root)
    discovered = {path.name for path in repo_root.glob("data_*") if path.is_dir()}
    configured = set(policy["roots"])
    errors = [f"unregistered data root: {name}" for name in sorted(discovered - configured)]
    errors += [f"configured root is absent: {name}" for name in sorted(configured - discovered)]
    descriptions = {}
    description_path = repo_root / "configs/data_readmes.yaml"
    if description_path.is_file():
        import yaml
        document = yaml.safe_load(description_path.read_text(encoding="utf-8"))
        if not isinstance(document, dict) or not isinstance(document.get("datasets"), dict):
            raise SnapshotError("invalid dataset description catalog")
        descriptions = document["datasets"]
    selected: dict[str, list[dict[str, Any]]] = {name: [] for name in configured}
    heads: dict[str, dict[str, Any]] = {}
    for entry in catalog:
        path = Path(str(entry["source"]))
        path = path if path.is_absolute() else repo_root / path
        for root in dataset_roots(repo_root, path, policy):
            selected[root].append(entry)
        if sync_root is None or not (sync_root / "heads" / entry["dataset"]).is_dir():
            continue
        try:
            resolved = resolve_latest_packed(sync_root, str(entry["dataset"]), require_objects=False)
            heads[entry["dataset"]] = {"snapshot_id": resolved.manifest["snapshot_id"],
                "manifest_sha256": resolved.manifest_sha256, "schema_version": resolved.manifest["schema_version"],
                "file_chunk_bytes": resolved.manifest["archive"].get("file_chunk_bytes"),
                "objects_verified": False}
        except (SnapshotError, OSError, ValueError) as error:
            heads[entry["dataset"]] = {"state": "head_unreadable", "error_type": type(error).__name__}
    rows = []
    for name, spec in policy["roots"].items():
        bindings = selected[name]
        root = repo_root / name
        local_layout = None
        if name == "data_news" and (root / "manifest.json").is_file():
            current = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            local_layout = current.get("storage_layout", "legacy_full_generation_v1")
        native_acceptance = None
        if local_layout == "news_incremental_parts_v2":
            proof_path = root / "verification.json"
            proof = json.loads(proof_path.read_text(encoding="utf-8")) if proof_path.is_file() else {}
            accepted = (
                current.get("state") == "ready" and current.get("contract_version") == 2
                and proof.get("state") == "verified" and proof.get("generation") == current.get("generation")
                and proof.get("manifest_content_sha256") == hashlib.sha256(
                    json.dumps(current, sort_keys=True).encode()).hexdigest()
                and proof.get("row_counts") == current.get("row_counts")
                and proof.get("parts_verified") == len(current.get("parts", []))
                and proof.get("bytes_verified") == sum(p["bytes"] for p in current.get("parts", []))
                and proof.get("document_hashes_verified") is True
                and proof.get("quality", {}).get("state") == "verified"
            )
            native_acceptance = {"state": "verified_receipt" if accepted else "acceptance_pending",
                "generation": current.get("generation"),
                "verified_at_utc": proof.get("verified_at_utc") if accepted else None,
                "payload_reverified_this_inventory": False}
        current_heads = [heads[item["dataset"]] for item in bindings if item["dataset"] in heads]
        shared_v2 = any(item.get("schema_version") == CHUNKED_SNAPSHOT_SCHEMA_VERSION
                        for item in current_heads)
        enrolled = [item["dataset"] for item in bindings
                    if item.get("reader_contract") == "packed-v2-chunks"
                    and item.get("file_chunk_bytes") == policy["file_chunk_bytes"]]
        rows.append({"root": name, "exists": root.is_dir(), "path_is_symlink": root.is_symlink(),
            **spec, "writer_implementation": ("news_copy_on_write_parts" if name == "data_news"
                                            else "shared_packed_file_and_chunk_cas"),
            "source_and_consumer_entrypoints": descriptions.get(name, {}).get("sources", []),
            "local_training_layout": local_layout,
            "publication_bindings": [{"dataset": item["dataset"], "source": item["source"],
                "publish": bool(item.get("publish")), "excluded_subtrees": item.get("excluded_subtrees", []),
                "head": heads.get(item["dataset"])} for item in bindings],
            "shared_v2_head_observed": shared_v2,
            "configured_v2_publication_branches": enrolled,
            "native_workspace_migrated": native_acceptance is not None and native_acceptance["state"] == "verified_receipt",
            **({"native_acceptance": native_acceptance} if native_acceptance is not None else {}),
            "rollout_state": ("local_news_v2_verified_receipt" if native_acceptance and native_acceptance["state"] == "verified_receipt"
                              else "local_news_v2_acceptance_pending" if native_acceptance
                              else "shared_v2_head_observed" if shared_v2
                              else "policy_enrolled_head_pending" if enrolled
                              else "reader_first_rollout_required"),
            "coverage_claim": "layout_inventory_only_not_download_or_recovery_acceptance"})
    return {"contract_version": 2, "roots": rows, "root_count": len(rows), "errors": errors,
            "network_calls": 0, "source_payload_reads": 0, "source_payload_writes": 0,
            "legacy_deletions": 0, "state": "accepted" if not errors else "failed"}
