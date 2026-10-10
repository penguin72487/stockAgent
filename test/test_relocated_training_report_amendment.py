"""A reporting release cannot silently amend a measured optimizer contract."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts import run_relocated_training as launcher


@pytest.mark.parametrize("change", ["report_only", "training_function", "other_module"])
def test_report_amendment_rejects_non_reporting_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    receipts = {}
    for name in ("parent", "new"):
        root = tmp_path / name
        trainer = root / "build-source/stockagent/training/trainer.py"
        trainer.parent.mkdir(parents=True)
        text = "def train():\n    return 1\n\ndef _refresh_walkforward_artifacts():\n    return 1\n"
        if name == "new":
            text = text.replace("return 1\n", "return 2\n", 1 if change == "training_function" else 0)
            if change != "training_function":
                text += "\ndef _stitched_deployment_prefix_results():\n    return []\n"
        trainer.write_text(text)
        files = {"stockagent/training/trainer.py": hashlib.sha256(text.encode()).hexdigest(),
                 "stockagent/backtest/simulator.py": "changed" if name == "new" and change == "other_module" else "same"}
        receipt = root / "release.json"
        receipt.write_text(json.dumps({"code": {"files": files}}))
        receipts[name] = receipt
    monkeypatch.setattr(launcher, "verify_source_release",
                        lambda receipt, root: {"source_sha256": receipt.parent.name})
    monkeypatch.setattr(launcher, "verify_release_bundles", lambda receipt: None)
    amendment = tmp_path / "amendment.json"
    amendment.write_text(json.dumps({
        "contract": "report_only_stitched_source_amendment_v1", "state": "verified",
        "optimizer_contract_changed": False, "parent_source_sha256": "parent",
        "source_sha256": "new", "source_receipt": str(receipts["new"]),
        "parent_source_receipt": str(receipts["parent"]), "config_sha256": "config",
    }))
    launch = {
        "reporting_source_amendment": str(amendment),
        "reporting_source_amendment_sha256": hashlib.sha256(amendment.read_bytes()).hexdigest(),
        "source_sha256": "new", "source_receipt": str(receipts["new"]),
        "config_sha256": "config",
    }
    if change == "report_only":
        launcher._verify_reporting_source_amendment(launch, "parent")
    else:
        with pytest.raises(AssertionError, match="report fix changes"):
            launcher._verify_reporting_source_amendment(launch, "parent")


def test_unreceipted_source_change_is_rejected() -> None:
    with pytest.raises(AssertionError, match="source_sha256"):
        launcher._verify_reporting_source_amendment({"source_sha256": "new"}, "old")
