"""Inspect model-input readiness without building prices or submitting orders.

Never silently feed six executable base columns to a wide checkpoint.
Research completion and source-publication rights are separate evidence.
"""
from __future__ import annotations

import hashlib
import json
import copy
from functools import lru_cache
from pathlib import Path
from typing import Any

class _ManifestMetadataInvalid(ValueError):
    def __init__(self, message: str, sha256: str):
        super().__init__(message)
        self.sha256 = sha256


@lru_cache(maxsize=8)
def _manifest_metadata(path: Path, generation: tuple) -> dict[str, Any]:
    """Cache only bounded metadata, not feature blocks or readiness decisions."""
    from stockagent.live.completed_inference_cache import file_generation

    raw = path.read_bytes()
    if file_generation(path) != generation:
        raise OSError("factorized manifest changed during metadata read")
    digest = hashlib.sha256(raw).hexdigest()
    try:
        manifest = json.loads(raw)
        if not isinstance(manifest, dict):
            raise ValueError("manifest must be an object")
    except (ValueError, UnicodeError) as exc:
        raise _ManifestMetadataInvalid(str(exc), digest) from exc
    result = {key: manifest.get(key) for key in (
        "contract", "status", "live_eligible", "research_only", "historical_point_in_time",
        "use_restriction", "logical_model_channels", "paper_inference")}
    dates = manifest.get("dates") or [None]
    result.update(manifest_sha256=digest,
                  first_date=dates[0], last_date=dates[-1])
    return result


class LiveFeatureContractError(RuntimeError):
    def __init__(self, evidence: dict[str, Any]) -> None:
        self.evidence = evidence
        codes = ", ".join(row["code"] for row in evidence["blockers"])
        super().__init__(f"live feature contract is not ready: {codes}; {evidence['manifest_path']}")


def inspect_live_feature_source(data: Any) -> dict[str, Any]:
    """Return bounded, read-only evidence. Do not decode any feature blocks.

    A successful metadata check here is not member-hash or execution acceptance.
    The canonical attachment verifies the complete published projection before
    any model receives its values.
    """
    configured = str(getattr(data, "factorized_feature_manifest", "") or "")
    if not configured:
        return {"required": False, "ready": True, "blockers": []}
    from stockagent.live.completed_inference_cache import file_generation

    path = Path(configured).resolve()
    blockers: list[dict[str, str]] = []
    evidence: dict[str, Any] = {
        "required": True, "ready": False, "manifest_path": str(path),
        "base_features": len(getattr(data, "feature_include", []) or []),
        "blockers": blockers,
    }
    try:
        generation = file_generation(path)
        manifest = copy.deepcopy(_manifest_metadata(path, generation))
        if file_generation(path) != generation:
            raise OSError("factorized manifest changed during metadata validation")
    except FileNotFoundError:
        blockers.append({"code": "factorized_manifest_missing", "detail": str(path)})
        return evidence
    except OSError as exc:
        blockers.append({"code": "factorized_manifest_unreadable", "detail": str(exc)})
        return evidence
    except (ValueError, UnicodeError) as exc:
        if isinstance(exc, _ManifestMetadataInvalid):
            evidence["manifest_sha256"] = exc.sha256
        blockers.append({"code": "factorized_manifest_invalid", "detail": str(exc)})
        return evidence
    evidence["manifest_sha256"] = manifest["manifest_sha256"]
    from stockagent.data.factorized_panel import SUPPORTED_CONTRACTS

    evidence.update({
        "contract": manifest.get("contract"), "status": manifest.get("status"),
        "live_eligible": manifest.get("live_eligible"),
        "research_only": manifest.get("research_only"),
        "historical_point_in_time": manifest.get("historical_point_in_time"),
        "use_restriction": manifest.get("use_restriction"),
        "logical_model_channels": manifest.get("logical_model_channels"),
        "first_date": manifest["first_date"],
        "last_date": manifest["last_date"],
        "raw_source_publication_requested": False,
        "notices": [],
    })
    if manifest.get("contract") not in SUPPORTED_CONTRACTS or manifest.get("status") != "complete":
        blockers.append({"code": "factorized_contract_incomplete", "detail": "Unsupported or incomplete factorized manifest."})
    paper = manifest.get("paper_inference") or {}
    paper_ready = (isinstance(paper, dict)
                   and paper.get("contract") == "factorized_paper_projection_lag1_v1"
                   and paper.get("status") == "verified"
                   and paper.get("raw_source_publication") is False
                   and bool(paper.get("parent_manifest_sha256"))
                   and bool(paper.get("decision_session")))
    if not paper_ready:
        blockers.append({"code": "factorized_paper_projection_unverified", "detail": "A sourced, causal research-paper inference projection is required; changing source admission flags is insufficient."})
    if manifest.get("research_only") is not False or manifest.get("historical_point_in_time") is not True:
        evidence["notices"].append({"code": "factorized_research_not_live_pit", "detail": "Retain the selected research ABI caveat; historical PIT is not proven."})
    if "no_public_redistribution" in str(manifest.get("use_restriction") or ""):
        evidence["notices"].append({"code": "factorized_private_raw_source", "detail": "Retain the original raw-source restriction. Model-only signals are a separate publication scope, not a raw data export."})
    evidence["member_hashes_verified"] = False
    evidence["checkpoint_input_parity_proven"] = False
    evidence["paper_inference"] = paper
    evidence["ready"] = not blockers
    return evidence


def require_live_feature_source(data: Any) -> None:
    evidence = inspect_live_feature_source(data)
    if not evidence["ready"]:
        raise LiveFeatureContractError(evidence)
