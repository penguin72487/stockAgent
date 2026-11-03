"""Identify an owner's claim without confusing retained earlier attempts."""
from __future__ import annotations

import json
from pathlib import Path
import re

from downloader.artifact_io import sha256_file


def find_capture_claim(claim_dir: Path, task_id: str, intent_sha256: str,
                       armed_attempt: str | None = None) -> Path | None:
    """Return the unique claim for this exact intent, retaining old evidence.

    Claim filenames include the native attempt ID. The owner may retain claims
    from earlier attempts of the same task; those are not evidence that the
    current intent was consumed. Identical duplicate claims remain an error.
    """
    if not re.fullmatch(r"[0-9a-f]{24}", task_id) or not re.fullmatch(r"[0-9a-f]{64}", intent_sha256):
        raise ValueError("Invalid task or intent identity")
    if armed_attempt is not None and not re.fullmatch(re.escape(task_id) + r"-[0-9a-f]{32}", armed_attempt):
        raise ValueError("Invalid armed attempt identity")
    matches = []
    for path in claim_dir.glob(task_id + "-*.json"):
        if (not re.fullmatch(re.escape(task_id) + r"-[0-9a-f]{32}\.json", path.name)
                or path.is_symlink() or not path.is_file()):
            raise ValueError("Invalid retained capture claim")
        digest = sha256_file(path)
        if path.stem == armed_attempt and digest != intent_sha256:
            raise ValueError("The armed attempt claimed a different intent")
        if digest == intent_sha256:
            matches.append(path)
    if len(matches) > 1:
        raise ValueError("The same capture intent has multiple claims")
    return matches[0] if matches else None


def verified_unsent_failure(root: Path, claim: Path, family: str) -> Path | None:
    """Verify a prior exact metadata failure before allowing another attempt.

    Unknown outcomes and already-captured menus are not retried. An absent
    outcome means the owner has not yet attested the pre-Preview boundary.
    """
    from downloader.tej_history import CONTRACT_VERSION, DesktopBridge

    types = {
        "finite": ("finite_axis_capture_intents", "tej_finite_company_date_axis_capture_once_v1",
                   ".finite-axis.json"),
        "large": ("large_axis_capture_intents", "tej_large_company_axis_capture_once_v1",
                  ".company-axis.snapshot.json"),
    }
    if family not in types:
        raise ValueError("Unknown capture family")
    folder, contract, success_suffix = types[family]
    root = root.resolve(strict=True)
    claim = claim.absolute()
    if (claim.is_symlink() or not claim.is_file() or claim.stat().st_size > 256 * 1024
            or claim.parent.resolve(strict=True) != (root / folder / "claimed").resolve(strict=True)
            or not re.fullmatch(r"[0-9a-f]{24}-[0-9a-f]{32}\.json", claim.name)):
        raise ValueError("Invalid prior capture claim")
    attempt, task_id = claim.stem, claim.stem[:24]
    success = root / "raw" / (attempt + ".json" + success_suffix)
    if success.exists() or success.is_symlink():
        raise ValueError("Prior successful capture requires readback and reuse")
    outcome = root / "raw" / (attempt + ".json.outcome.json")
    if not outcome.exists():
        return None
    if outcome.is_symlink() or not outcome.is_file() or outcome.stat().st_size > 4096:
        raise ValueError("Invalid prior metadata outcome")
    request_path = root / "requests" / (attempt + ".json")
    _, release_sha = DesktopBridge.bound_script_release(root, attempt, request_path)
    request = json.loads(request_path.read_text(encoding="utf-8-sig"))
    intent = json.loads(claim.read_text(encoding="utf-8-sig"))
    result = json.loads(outcome.read_text(encoding="utf-8-sig"))
    if (request.get("task_id") != task_id or request.get("action") != "plan"
            or intent.get("contract") != contract or intent.get("task_id") != task_id
            or intent.get("prepared_request_sha256") != sha256_file(request_path)
            or intent.get("bridge_script_sha256") != release_sha
            or intent.get("market_data_query_authorized") is not False
            or intent.get("provider_queries_by_registration") != 0
            or result.get("contract_version") != CONTRACT_VERSION
            or result.get("provider") != "tej_smart_wizard" or result.get("task_id") != task_id
            or result.get("action") != "plan"
            or result.get("market_data_query_submission_possible") is not False
            or result.get("error_code") != "metadata_preparation_failed_before_preview"
            or any(intent.get(key) != request.get(key) or result.get(key) != request.get(key)
                   for key in ("type", "smart_id", "table"))):
        raise ValueError("Prior claim lacks exact pre-Preview failure evidence")
    return outcome


def require_reconciled_failures(root: Path, claim_dir: Path, task_id: str, family: str) -> None:
    """Keep every prior claim in place; admit only independently proved failures."""
    for claim in claim_dir.glob(task_id + "-*.json"):
        if verified_unsent_failure(root, claim, family) is None:
            raise ValueError("A prior capture outcome is unknown and requires reconciliation")
