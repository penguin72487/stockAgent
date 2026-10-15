from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest

from scripts import audit_git_content as content


@pytest.mark.parametrize("path", [
    "stockAgent.7z", "data_new_provider/prices.parquet", "data_bybit",
    "artifacts_new/model.pt", "artifacts/operations/run.json", "reports/snapshot/report.html",
    ".agents/skills/.skill-sync-backups/old/example/SKILL.md", ".codex/session.json",
    ".env", ".env.production", "test/fixtures/.env.local", "test/fixtures/private.key",
    "keys/certificate.p12", "nested/__pycache__/test.pyc", ".stignore",
    "model.pt", "state.sqlite", "timing.log", "scratch.tmp", ".DS_Store",
])
def test_excludes_roles_not_needed_by_git(path):
    assert content.decision(path)["decision"] == "exclude"


@pytest.mark.parametrize("path", [
    "scripts/audit_training_quality.py", "stockagent/data/panel.py", "test/test_feature.py",
    "configs/data_sync/catalog.json", "configs/fallback_symbols.csv", "uv.lock",
    "package-lock.json", ".env.example", "services/discord_bot/.env copy.example",
    "AGENTS.md", "docs/evidence_2026-10-15.md",
    "services/dashboard/index.html", "deploy/syncthing/example.stignore",
    ".agents/skills/example/SKILL.md", ".vscode/settings.json", "data_processing.py",
    "test/fixtures/prices.parquet", "test/fixtures/archive/input.zip",
])
def test_keeps_source_templates_locks_and_small_fixtures(path):
    assert content.decision(path)["decision"] == "keep"


def test_unknown_and_large_source_need_review_not_deletion():
    assert content.decision("unknown/output.json")["decision"] == "review"
    assert content.decision("scripts/large.py", 6 * 1024**2)["decision"] == "review"
    assert content.decision("test/fixtures/a.parquet", 6 * 1024**2)["decision"] == "review"


@pytest.fixture
def repo(tmp_path):
    path = tmp_path / "repo"
    path.mkdir()
    content.git(path, "init", "--initial-branch=main")
    content.git(path, "config", "user.email", "fixture@example.invalid")
    content.git(path, "config", "user.name", "Fixture")
    (path / "README.md").write_text("source\n")
    content.git(path, "add", "README.md")
    content.git(path, "commit", "-m", "initial")
    return path


def test_staged_audit_uses_index_bytes_not_working_copy(repo):
    payload = repo / "model.pt"
    payload.write_bytes(b"staged model")
    content.git(repo, "add", "model.pt")
    payload.write_bytes(b"different working bytes")
    report = content.audit(repo, staged=True)
    assert report["tracked"][0]["bytes"] == len(b"staged model")
    assert report["tracked"][0]["decision"] == "exclude"
    assert content.main(["--repo", str(repo), "--staged", "--check"]) == 1
    assert payload.read_bytes() == b"different working bytes"


def test_staged_removal_is_allowed_and_local_payload_not_deleted(repo):
    payload = repo / "model.pt"
    payload.write_bytes(b"retained local bytes")
    content.git(repo, "add", "model.pt")
    content.git(repo, "commit", "-m", "legacy")
    content.git(repo, "rm", "--cached", "--", "model.pt")
    assert content.main(["--repo", str(repo), "--staged", "--check"]) == 0
    assert payload.read_bytes() == b"retained local bytes"


def test_history_sees_deleted_tagged_and_renamed_paths_with_spaces_and_newlines(repo):
    data = repo / "data_old"
    data.mkdir()
    name = "data_old/a space\nand newline.parquet"
    (repo / name).write_bytes(b"historical input")
    content.git(repo, "add", "--", name)
    content.git(repo, "commit", "-m", "legacy payload")
    content.git(repo, "tag", "legacy")
    (repo / "configs").mkdir()
    content.git(repo, "mv", "--", name, "configs/kept.csv")
    content.git(repo, "commit", "-m", "renamed as source")
    report = content.history_inventory(repo)
    rows = {row["path"]: row for row in report["paths"]}
    assert rows[name]["decision"] == "exclude"
    assert rows["configs/kept.csv"]["decision"] == "keep"
    assert report["excluded_unique_blob_bytes"] == len(b"historical input")
    # Shared blob bytes are logical inventory, not an assertion of recovered disk.
    assert report["commits"] == 3


def test_untracked_source_visible_but_ignored_payload_not_traversed(repo):
    (repo / ".gitignore").write_text("/data_*/\n/artifacts/\n")
    (repo / "data_new").mkdir()
    (repo / "data_new/raw.parquet").write_bytes(b"private")
    (repo / "scripts").mkdir()
    (repo / "scripts/new.py").write_text("print(1)\n")
    result = content.audit(repo)
    paths = {row["path"] for row in result["untracked"]}
    assert "scripts/new.py" in paths
    assert "data_new/raw.parquet" not in paths


def test_history_includes_standalone_ide_tree_and_blob_refs(repo):
    (repo / "model.pt").write_bytes(b"only in an IDE tree")
    content.git(repo, "add", "model.pt")
    tree = content.git(repo, "write-tree").decode().strip()
    content.git(repo, "update-ref", "refs/ide/checkpoint", tree)
    content.git(repo, "tag", "-a", "tree-snapshot", "-m", "snapshot", tree)
    content.git(repo, "rm", "--cached", "--", "model.pt")
    blob = content.git(repo, "hash-object", "-w", "--stdin", data=b"pathless blob").decode().strip()
    content.git(repo, "update-ref", "refs/ide/blob", blob)
    report = content.history_inventory(repo)
    assert report["commits"] == 1
    assert report["excluded_unique_blob_bytes"] == len(b"only in an IDE tree")
    assert any(r["path"] == "model.pt" for r in report["paths"])
    refs = {r["ref"]: r for r in report["non_commit_refs"]}
    assert refs["refs/ide/checkpoint"]["decision"] == "inspect_paths"
    assert refs["refs/tags/tree-snapshot"]["oid"] == tree
    assert refs["refs/ide/blob"]["decision"] == "review"


def test_foreign_submodule_commit_is_not_a_missing_local_blob(repo):
    content.git(repo, "update-index", "--add", "--cacheinfo",
                "160000,1234567890123456789012345678901234567890,vendor/external")
    content.git(repo, "commit", "-m", "submodule reference")
    assert content.audit(repo)["tracked"]
    history = content.history_inventory(repo)
    row = next(r for r in history["paths"] if r["path"] == "vendor/external")
    assert row["versions"] == 0


def test_missing_local_object_fails_closed(repo):
    with pytest.raises(ValueError, match="missing or invalid Git object"):
        content.object_sizes(repo, {"1234567890123456789012345678901234567890"})


def test_report_is_private_fresh_and_ignored(repo):
    (repo / ".gitignore").write_text("/artifacts/\n")
    output = repo / "artifacts/audit.json"
    args = ["--repo", str(repo), "--output", str(output)]
    assert content.main(args) == 0
    assert output.stat().st_mode & 0o777 == 0o600
    assert json.loads(output.read_text())["contract"] == "git_content_role_audit_v1"
    with pytest.raises(SystemExit):
        content.main(args)


@pytest.mark.parametrize("target", ["README.md", ".git/audit.json", "new-public.json"])
def test_output_cannot_overwrite_source_metadata_or_add_public_dump(repo, target):
    before = (repo / "README.md").read_bytes()
    with pytest.raises(SystemExit):
        content.main(["--repo", str(repo), "--output", str(repo / target)])
    assert (repo / "README.md").read_bytes() == before


def test_output_cannot_follow_symlink_parent(repo, tmp_path):
    (repo / ".gitignore").write_text("/artifacts/\n")
    outside = tmp_path / "outside"
    outside.mkdir()
    (repo / "artifacts").symlink_to(outside, target_is_directory=True)
    with pytest.raises(SystemExit):
        content.main(["--repo", str(repo), "--output", str(repo / "artifacts/audit.json")])
    assert not (outside / "audit.json").exists()


def test_repo_ignore_rules_do_not_hide_source_or_required_fixtures(repo):
    (repo / ".gitignore").write_bytes((Path(__file__).resolve().parents[1] / ".gitignore").read_bytes())
    ignored = ["data_new/x.parquet", "artifacts_trial/model.pt", "reports/one/report.html",
               ".env.local", ".agents/skills/.skill-sync-backups/old/x.md", "model.pt"]
    kept = ["configs/data_sync/catalog.json", "stockagent/data/panel.py", "test/test_new.py",
            "test/fixtures/a.parquet", "test/fixtures/nested/a.zip", ".env.example", "uv.lock",
            "services/discord_bot/.env copy.example", "services/dashboard/index.html",
            "deploy/syncthing/example.stignore", "data_processing.py"]
    for name in ignored + kept:
        # Explicit file paths allow check-ignore to work without building test payload trees.
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture\n")
        result = subprocess.run(["git", "-C", str(repo), "check-ignore", "--quiet", "--", name])
        assert (result.returncode == 0) == (name in ignored), name
