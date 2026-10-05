from pathlib import Path

import pytest

from stockagent.skill_sync import SkillSyncError, content, install, inventory, key_parts, merge_files, public_member, record
from scripts.sync_codex_skills import validate_entrypoints


KEY = "user/example-skill/SKILL.md"


def test_union_and_single_peer_edit():
    local = {KEY: record(b"local\n")}
    remote = {"repo/remote-skill/SKILL.md": record(b"remote\n")}
    result, conflicts = merge_files(local, remote, {})
    assert result == local | remote and conflicts == []
    updated = {KEY: record(b"updated\n")}
    result, conflicts = merge_files(local, updated, local)
    assert result == updated and conflicts == []


def test_three_way_merges_independent_edits():
    base = {KEY: record(b"first\nunchanged\nlast\n")}
    local = {KEY: record(b"FIRST\nunchanged\nlast\n")}
    remote = {KEY: record(b"first\nunchanged\nLAST\n")}
    merged, conflicts = merge_files(local, remote, base)
    assert not conflicts
    assert content(merged[KEY]) == b"FIRST\nunchanged\nLAST\n"


@pytest.mark.parametrize("base", [{}, {KEY: record(b"base\n")}])
def test_overlapping_edits_keep_both_inputs(base):
    local, remote = {KEY: record(b"local\n")}, {KEY: record(b"remote\n")}
    merged, conflicts = merge_files(local, remote, base)
    assert KEY not in merged and conflicts[0]["key"] == KEY
    assert content(local[KEY]) == b"local\n" and content(remote[KEY]) == b"remote\n"


def test_deletion_not_propagated():
    base = {KEY: record(b"preserved\n")}
    assert merge_files({}, {}, base) == (base, [])
    assert merge_files(base, {}, base) == (base, [])


@pytest.mark.parametrize("key", ["user/x/../../outside", "user/x//SKILL.md", "user/x/.env", "plugin/x/SKILL.md"])
def test_rejects_escaping_or_noncustom_members(key):
    with pytest.raises(SkillSyncError):
        key_parts(key)


def test_install_preserves_previous_version_and_executable_resource(tmp_path):
    root = tmp_path / "skills"
    (root / "example-skill").mkdir(parents=True)
    (root / "example-skill/SKILL.md").write_bytes(b"original\n")
    roots = {"user": str(root)}
    before = inventory(roots)
    desired = {KEY: record(b"updated\n"), "user/example-skill/scripts/helper.py": record(b"print(1)\n", 0o755)}
    result = install(roots, before, desired, "test_run")
    assert inventory(roots) == desired
    assert (Path(result["backups"][0]) / "SKILL.md").read_bytes() == b"original\n"
    assert (root / "example-skill/scripts/helper.py").stat().st_mode & 0o111


def test_concurrent_edit_rejected_before_writes(tmp_path):
    root = tmp_path / "skills"
    (root / "example-skill").mkdir(parents=True)
    p = root / "example-skill/SKILL.md"
    p.write_bytes(b"original\n")
    roots = {"user": str(root)}
    before = inventory(roots)
    p.write_bytes(b"another agent changed it\n")
    with pytest.raises(SkillSyncError, match="changed since planning"):
        install(roots, before, {KEY: record(b"sync\n")}, "test_run")
    assert p.read_bytes() == b"another agent changed it\n"


def test_symlink_and_system_skills_excluded(tmp_path):
    root = tmp_path / "skills"
    (root / ".system/hidden").mkdir(parents=True)
    (root / ".system/hidden/SKILL.md").write_bytes(b"system\n")
    assert inventory({"user": str(root)}) == {}
    (root / "example-skill").symlink_to(root / ".system/hidden", target_is_directory=True)
    with pytest.raises(SkillSyncError, match="symlink"):
        inventory({"user": str(root)})


@pytest.mark.parametrize("name,data", [("credentials.json", b"{}"), ("certificate.pem", b"certificate"),
                                       ("innocent.txt", b"-----BEGIN OPENSSH PRIVATE KEY-----")])
def test_private_resources_rejected(name, data):
    with pytest.raises(SkillSyncError, match="outside"):
        public_member(name, data)


def test_invalid_or_duplicate_skill_identity_rejected():
    with pytest.raises(SkillSyncError, match="frontmatter"):
        validate_entrypoints({KEY: record(b"no header\n")})
    header = record(b"---\nname: example-skill\ndescription: Example task\n---\n")
    with pytest.raises(SkillSyncError, match="duplicate"):
        validate_entrypoints({KEY: header, "repo/example-skill/SKILL.md": header})
