from argparse import Namespace
from pathlib import Path
import json
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_rule_workspace import (
    check_rule_output_path, maintained_rule_build,
)


def bundle(root, value=b"verified candidate"):
    root.mkdir(parents=True, exist_ok=True)
    (root / "facts.parquet").write_bytes(value)
    atomic_write_json(root / "manifest.json", {
        "outputs": {"facts.parquet": {"sha256": sha256_file(root / "facts.parquet")}},
        "sources": [],
    })


def args(root):
    return Namespace(output_dir=root, base_candidates=None, update_current=True)


def usage_gates(monkeypatch):
    monkeypatch.setattr("stockagent.data.tw_futures_rule_workspace.artifact_process_references", lambda *a: [])
    monkeypatch.setattr("stockagent.data.tw_futures_rule_workspace.artifact_service_references",
                        lambda roots, repo: {str(r): [] for r in roots})


def test_numbered_full_parser_outputs_are_retired():
    with pytest.raises(ValueError, match="numbered"):
        check_rule_output_path(Path("all_products_rule_facts_native_v123_20261003"))
    check_rule_output_path(Path("all_products_rule_facts"))


def test_failed_build_keeps_current_untouched(tmp_path, monkeypatch):
    usage_gates(monkeypatch)
    root = tmp_path / "all_products_rule_facts"; bundle(root)
    before = (root / "manifest.json").read_bytes()
    with pytest.raises(RuntimeError, match="parser failed"):
        with maintained_rule_build(args(root), repo_root=tmp_path) as selected:
            (selected.output_dir / "partial").write_text("incomplete")
            raise RuntimeError("parser failed")
    assert (root / "manifest.json").read_bytes() == before
    assert (tmp_path / ".rule-facts-pending/partial").exists()


def test_semantic_noop_does_not_advance_current(tmp_path, monkeypatch):
    usage_gates(monkeypatch)
    root = tmp_path / "all_products_rule_facts"; bundle(root)
    before = (root / "manifest.json").stat()
    with maintained_rule_build(args(root), repo_root=tmp_path) as selected:
        assert selected.base_candidates == root
        bundle(selected.output_dir)
    after = (root / "manifest.json").stat()
    assert (before.st_ino, before.st_mtime_ns) == (after.st_ino, after.st_mtime_ns)
    assert not (tmp_path / ".rule-facts-pending").exists()
    assert not (tmp_path / ".rule-facts-previous").exists()


def test_atomic_change_keeps_exact_prior_for_pinned_consumers(tmp_path, monkeypatch):
    usage_gates(monkeypatch)
    root = tmp_path / "all_products_rule_facts"; bundle(root, b"old")
    before = (root / "manifest.json").read_bytes()
    with maintained_rule_build(args(root), repo_root=tmp_path) as selected:
        bundle(selected.output_dir, b"new")
    assert (root / "facts.parquet").read_bytes() == b"new"
    assert (tmp_path / ".rule-facts-previous/manifest.json").read_bytes() == before
    with pytest.raises(ValueError, match="Previous rules"):
        with maintained_rule_build(args(root), repo_root=tmp_path) as selected:
            bundle(selected.output_dir, b"third")
    assert (root / "facts.parquet").read_bytes() == b"new"


def test_tampered_output_never_replaces_current(tmp_path, monkeypatch):
    usage_gates(monkeypatch)
    root = tmp_path / "all_products_rule_facts"; bundle(root)
    before = (root / "manifest.json").read_bytes()
    with pytest.raises(ValueError, match="hash mismatch"):
        with maintained_rule_build(args(root), repo_root=tmp_path) as selected:
            bundle(selected.output_dir)
            (selected.output_dir / "facts.parquet").write_bytes(b"changed after receipt")
    assert (root / "manifest.json").read_bytes() == before


def test_active_process_rejects_replacement(tmp_path, monkeypatch):
    usage_gates(monkeypatch)
    root = tmp_path / "all_products_rule_facts"; bundle(root)
    monkeypatch.setattr("stockagent.data.tw_futures_rule_workspace.artifact_process_references",
                        lambda *a: ["pid=1"])
    with pytest.raises(ValueError, match="in use"):
        with maintained_rule_build(args(root), repo_root=tmp_path):
            pytest.fail("must reject before compilation")


def test_parent_receipt_chain_is_not_copied_into_current(tmp_path, monkeypatch):
    usage_gates(monkeypatch)
    root = tmp_path / "all_products_rule_facts"; bundle(root)
    with maintained_rule_build(args(root), repo_root=tmp_path) as selected:
        bundle(selected.output_dir, b"new")
        source = selected.output_dir / "sources/parent.json"
        source.parent.mkdir(); source.write_bytes((root / "manifest.json").read_bytes())
        receipt = json.loads((selected.output_dir / "manifest.json").read_text())
        receipt["sources"] = [{"path": "sources/parent.json", "sha256": sha256_file(source),
                               "url": "", "kind": "parent_candidate_manifest"}]
        atomic_write_json(selected.output_dir / "manifest.json", receipt)
    assert not (root / "sources/parent.json").exists()
    assert json.loads((root / "manifest.json").read_text())["sources"] == []


def test_unchanged_facts_keep_separately_accepted_interval_repair(tmp_path, monkeypatch):
    usage_gates(monkeypatch)
    root = tmp_path / "all_products_rule_facts"; bundle(root)
    (root / "margin_event_candidates.parquet").write_bytes(b"same facts")
    (root / "margin_level_intervals.parquet").write_bytes(b"verified interval repair")
    receipt = json.loads((root / "manifest.json").read_text())
    for name in ("margin_event_candidates.parquet", "margin_level_intervals.parquet"):
        receipt["outputs"][name] = {"sha256": sha256_file(root / name)}
    atomic_write_json(root / "manifest.json", receipt)
    with maintained_rule_build(args(root), repo_root=tmp_path) as selected:
        bundle(selected.output_dir, b"new other fact")
        (selected.output_dir / "margin_event_candidates.parquet").write_bytes(b"same facts")
        (selected.output_dir / "margin_level_intervals.parquet").write_bytes(b"unrepaired recomputation")
        result = json.loads((selected.output_dir / "manifest.json").read_text())
        for name in ("margin_event_candidates.parquet", "margin_level_intervals.parquet"):
            result["outputs"][name] = {"sha256": sha256_file(selected.output_dir / name)}
        atomic_write_json(selected.output_dir / "manifest.json", result)
    assert (root / "margin_level_intervals.parquet").read_bytes() == b"verified interval repair"
