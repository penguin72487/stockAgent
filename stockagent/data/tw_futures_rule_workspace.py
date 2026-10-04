"""One maintained rule-fact workspace around the existing candidate builder."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import shutil

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.artifact_consumers import artifact_service_references

CURRENT_NAME = "all_products_rule_facts"
DEFAULT_CURRENT = Path("artifacts/markets/tw_futures_v8_margin_preparation") / CURRENT_NAME


def check_rule_output_path(path: Path) -> None:
    if re.fullmatch(r"all_products_rule_facts_native_v\d+_.+", path.name):
        raise ValueError("Use the maintained all_products_rule_facts directory; numbered rule copies are retired")


def _payload_identity(root: Path) -> dict:
    receipt = json.loads((root / "manifest.json").read_text())
    outputs = {name: row["sha256"] for name, row in receipt["outputs"].items()}
    sources = sorted((row["path"], row["sha256"], row.get("url", ""), row["kind"])
                     for row in receipt["sources"] if row["kind"] != "parent_candidate_manifest")
    for name, digest in outputs.items():
        if not (root / name).resolve().is_relative_to(root.resolve()) or sha256_file(root / name) != digest:
            raise ValueError(f"Rule output hash mismatch: {name}")
    for name, digest, _, _ in sources:
        if not (root / name).resolve().is_relative_to(root.resolve()) or sha256_file(root / name) != digest:
            raise ValueError(f"Rule source hash mismatch: {name}")
    contracts = {key: receipt.get(key) for key in (
        "schema_version", "clock_parser_version", "builder_source_sha256",
        "clock_parser_source_sha256", "preparation_source_sha256",
        "position_research_policy_sha256", "point_in_time_verified",
        "all_products_training_ready",
    )}
    return {"outputs": outputs, "sources": sources, "contracts": contracts}


@contextmanager
def maintained_rule_build(arguments, *, repo_root: Path, rebuild_interval_kinds=(),
                          resume_pending_parent_sha256=None):
    """Stage and verify a current build; retain its exact prior input receipt.

    Temporary staging is reused under the same path. Completed old bytes remain
    available for pinned consumers until the separate cold retirement gate.
    No numbered parser bundle is created.
    """
    if not set(rebuild_interval_kinds) <= {"margin", "position", "corporate"}:
        raise ValueError("Unknown interval repair kind")
    target = arguments.output_dir.absolute()
    check_rule_output_path(target)
    if not arguments.update_current:
        yield arguments
        return
    if target.name != CURRENT_NAME or target.resolve(strict=False) != target:
        raise ValueError("--update-current requires a real all_products_rule_facts directory")
    target.parent.mkdir(parents=True, exist_ok=True)
    with (target.parent / ".rule-facts-build.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        original_base, original_output = arguments.base_candidates, arguments.output_dir
        if target.exists():
            if artifact_process_references(target, repo_root / "artifacts/markets"):
                raise ValueError("Current rules are in use; build cannot replace them")
            if artifact_service_references((target,), repo_root)[str(target)]:
                raise ValueError("An active service uses current rules")
            if original_base is not None and original_base.resolve() != target:
                raise ValueError("Current updates must extend the current bundle")
            arguments.base_candidates = target
        pending = target.parent / ".rule-facts-pending"
        if pending.exists():
            if (resume_pending_parent_sha256 is None
                    or sha256_file(target / "manifest.json") != resume_pending_parent_sha256):
                raise ValueError("Unfinished .rule-facts-pending exists; inspect it before continuing")
            _payload_identity(pending)
        else:
            pending.mkdir()
        arguments.output_dir = pending
        try:
            yield arguments
            receipt = json.loads((pending / "manifest.json").read_text())
            if target.exists():
                previous_receipt = json.loads((target / "manifest.json").read_text())
                # Keep already accepted interval repairs when their underlying
                # source facts did not change. Unchanged margin facts must not
                # silently lose a separately verified margin-layout correction.
                companions = {
                    "margin_event_candidates.parquet": ("margin_level_intervals.parquet", "margin_interval_issues.json"),
                    "position_event_candidates.parquet": ("position_level_intervals.parquet", "position_interval_issues.json"),
                    "corporate_event_candidates.parquet": ("corporate_terms_intervals.parquet", "corporate_unit_intervals.parquet",
                                                              "corporate_term_issues.json", "corporate_unit_issues.json"),
                }
                retained = []
                for facts, names in companions.items():
                    if facts.split("_", 1)[0] in rebuild_interval_kinds:
                        continue
                    old = previous_receipt["outputs"].get(facts)
                    new = receipt["outputs"].get(facts)
                    if not old or old != new:
                        continue
                    for name in names:
                        proof = previous_receipt["outputs"].get(name)
                        if proof is None:
                            continue
                        if sha256_file(target / name) != proof["sha256"]:
                            raise ValueError("Previous accepted interval changed")
                        if not (pending / name).exists() or not os.path.samefile(target / name, pending / name):
                            shutil.copy2(target / name, pending / name)
                        receipt["outputs"][name] = proof
                        retained.append(name)
                receipt["retained_unchanged_fact_intervals"] = retained
                for prefix, filename in (
                    ("margin", "margin_level_intervals.parquet"),
                    ("position", "position_level_intervals.parquet"),
                    ("corporate", "corporate_terms_intervals.parquet"),
                ):
                    if filename in retained:
                        for key, value in previous_receipt.items():
                            if key.startswith(prefix + "_interval") or key in {
                                prefix + "_level_intervals", "corporate_term_intervals", "corporate_term_products",
                                "corporate_term_issues", "corporate_unit_intervals", "corporate_unit_issues",
                            } and key.startswith(prefix + "_"):
                                receipt[key] = value
                policy_proof = previous_receipt["outputs"].get("position_research_policy.json")
                if policy_proof is not None:
                    if sha256_file(target / "position_research_policy.json") != policy_proof["sha256"]:
                        raise ValueError("Research policy changed without a new receipt")
                    if not (pending / "position_research_policy.json").exists() or not os.path.samefile(
                            target / "position_research_policy.json", pending / "position_research_policy.json"):
                        shutil.copy2(target / "position_research_policy.json", pending / "position_research_policy.json")
                    receipt["outputs"]["position_research_policy.json"] = policy_proof
                    receipt["position_research_policy_sha256"] = policy_proof["sha256"]
            parent_sources = [row for row in receipt["sources"]
                              if row["kind"] == "parent_candidate_manifest"]
            receipt["sources"] = [row for row in receipt["sources"]
                                  if row["kind"] != "parent_candidate_manifest"]
            receipt["maintenance_contract"] = "single_current_rule_facts_v1"
            for row in parent_sources:
                path = pending / row["path"]
                if not path.resolve().is_relative_to(pending.resolve()) or sha256_file(path) != row["sha256"]:
                    raise ValueError("Invalid retained parent receipt")
                path.unlink()
            atomic_write_json(pending / "manifest.json", receipt)
            identity = _payload_identity(pending)
            if target.exists() and _payload_identity(target) == identity:
                shutil.rmtree(pending)
                return
            # Preserve exact input bytes before atomic exchange, rather than
            # rewriting hash-pinned inputs or growing a chain of version dirs.
            if target.exists():
                from scripts.promote_tw_day_trade_replay import _exchange_directories
                previous = target.parent / ".rule-facts-previous"
                if previous.exists():
                    raise ValueError("Previous rules await cold retirement; refuse another retained copy")
                _exchange_directories(target, pending)
                os.rename(pending, previous)
            else:
                os.rename(pending, target)
        finally:
            arguments.base_candidates = original_base
            arguments.output_dir = original_output
