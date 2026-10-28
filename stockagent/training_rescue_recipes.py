"""Immutable input-recovery revisions, separate from original source enrollment.

These declarations add missing recovery dependencies. They never change the
training core or prove raw recovery, CPU reconstruction, or a GPU resume.
"""
from copy import deepcopy

from stockagent.training_rescue_worker import (
    PREPARED_BUILD_POLICY, digest, preparation_references, validate_spec,
)

INPUT_RECIPE = "stockagent.training_input_recipe.v1"
INPUT_FIELDS = frozenset({
    "data_sources", "data_capsules", "input_controls", "rebuild_steps",
    "rebuild_source_roots", "rebuild_working_directories", "prepared_links", "prepared_reconciliation", "prepared_build_policy",
})
REVISION_FIELD = "input_recipe_revision_sha256"


def input_recipe(base: dict, candidate: dict) -> dict:
    """Only explicit fixed-input fields may differ from the enrolled source."""
    validate_spec(base)
    validate_spec(candidate)
    if REVISION_FIELD in base or REVISION_FIELD in candidate:
        raise ValueError("recipe revisions must start from the original enrolled spec")
    core = lambda spec: {k: v for k, v in spec.items() if k not in INPUT_FIELDS}
    if digest(core(base)) != digest(core(candidate)):
        raise ValueError("input recipe cannot change the enrolled training/source/runtime core")
    if not (candidate.get("data_sources") or candidate.get("data_capsules")):
        raise ValueError("input recipe requires non-empty pinned source bindings")
    if candidate.get("prepared_build_policy") != PREPARED_BUILD_POLICY:
        raise ValueError("input recipe requires shared atomic CPU build ownership")
    preparation_references(candidate, require_complete=True)
    return {"contract": INPUT_RECIPE, "run_id": base["run_id"], "base_spec_sha256": digest(base),
            "inputs": {k: deepcopy(v) for k, v in candidate.items() if k in INPUT_FIELDS}}


def apply_input_recipe(base: dict, revision: dict, revision_sha: str) -> dict:
    """Reconstruct a pinned effective target spec without rewriting the source."""
    if (set(revision) != {"contract", "run_id", "base_spec_sha256", "inputs"}
            or revision.get("contract") != INPUT_RECIPE or digest(revision) != revision_sha
            or revision.get("run_id") != base["run_id"] or revision.get("base_spec_sha256") != digest(base)
            or not isinstance(revision.get("inputs"), dict) or not set(revision["inputs"]) <= INPUT_FIELDS):
        raise ValueError("input recipe revision no longer matches its original enrollment")
    candidate = {k: deepcopy(v) for k, v in base.items() if k not in INPUT_FIELDS}
    candidate.update(deepcopy(revision["inputs"]))
    if input_recipe(base, candidate) != revision:
        raise ValueError("input recipe differs from its canonical input declaration")
    candidate[REVISION_FIELD] = revision_sha
    return validate_spec(candidate)
