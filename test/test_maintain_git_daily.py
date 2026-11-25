from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from scripts.audit_git_content import git


ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = "stockagent-rate-limits-0/limiter.state"


@pytest.fixture
def checkout(tmp_path):
    repo, origin = tmp_path / "work", tmp_path / "origin.git"
    repo.mkdir()
    git(repo, "init", "--initial-branch=twRule")
    git(repo, "init", "--bare", "--initial-branch=twRule", str(origin))
    git(repo, "config", "user.email", "fixture@example.invalid")
    git(repo, "config", "user.name", "Fixture")
    git(repo, "remote", "add", "origin", str(origin))
    (repo / ".gitignore").write_bytes((ROOT / ".gitignore").read_bytes())
    (repo / "README.md").write_text("tracked source\n")
    (repo / STATE_PATH).parent.mkdir()
    (repo / STATE_PATH).write_text("runtime\n")
    git(repo, "add", "-f", "--", ".gitignore", "README.md", STATE_PATH)
    git(repo, "commit", "-m", "fixture baseline")
    git(repo, "push", "-u", "origin", "twRule")
    return repo, origin, tmp_path / "receipts"


def run_daily(checkout, *args):
    repo, origin, state = checkout
    completed = subprocess.run([sys.executable, str(ROOT / "scripts/maintain_git_daily.py"),
                                "--repo", str(repo), "--state-dir", str(state),
                                "--expected-url", str(origin), *args], capture_output=True, check=False)
    return completed.returncode, json.loads((state / "latest.json").read_text())


def test_plan_and_apply_only_remove_tracking_and_push_exact_revision(checkout):
    repo, origin, _ = checkout
    base = git(repo, "rev-parse", "HEAD")
    (repo / "README.md").write_text("private work in progress\n")
    (repo / "not_yet_reviewed.py").write_text("draft data\n")
    (repo / STATE_PATH).write_text("live runtime updates\n")
    code, plan = run_daily(checkout)
    assert code == 0 and plan["state"] == "planned" and plan["cleanup_paths"] == [STATE_PATH]
    assert git(repo, "rev-parse", "HEAD") == base
    assert git(repo, "diff", "--cached", "--name-only") == b""

    code, result = run_daily(checkout, "--apply")
    assert code == 0 and result["state"] == "pushed", result
    assert git(origin, "rev-parse", "twRule") == git(repo, "rev-parse", "HEAD")
    assert git(repo, "rev-parse", "origin/twRule") == git(repo, "rev-parse", "HEAD")
    assert git(repo, "diff", "--name-only", base.decode().strip(), "HEAD").decode().splitlines() == [STATE_PATH]
    assert git(repo, "diff", "--cached", "--name-only") == b""
    assert (repo / STATE_PATH).read_text() == "live runtime updates\n"
    assert (repo / "README.md").read_text() == "private work in progress\n"
    assert (repo / "not_yet_reviewed.py").is_file()
    assert run_daily(checkout, "--apply")[1]["state"] == "up_to_date"


def test_unrelated_staging_is_preserved_and_cleanup_can_retry(checkout):
    repo, origin, _ = checkout
    base = git(origin, "rev-parse", "twRule")
    (repo / "README.md").write_text("partial staged draft\n")
    git(repo, "add", "--", "README.md")
    staged = git(repo, "diff", "--cached", "--binary")
    code, result = run_daily(checkout, "--apply")
    assert code == 0 and result["cleanup_deferred"] == "unrelated_staged_changes"
    assert git(repo, "diff", "--cached", "--binary") == staged
    assert git(origin, "rev-parse", "twRule") == base


def test_retry_accepts_only_already_staged_runtime_removal(checkout):
    repo, _, _ = checkout
    git(repo, "rm", "--cached", "--", STATE_PATH)
    code, result = run_daily(checkout, "--apply")
    assert code == 0 and result["state"] == "pushed", result
    assert (repo / STATE_PATH).read_text() == "runtime\n"


def test_outgoing_exclusion_survives_a_later_deletion(checkout):
    repo, origin, _ = checkout
    base = git(origin, "rev-parse", "twRule")
    (repo / "model.pt").write_bytes(b"synthetic artifact")
    git(repo, "add", "-f", "--", "model.pt")
    git(repo, "commit", "-m", "accidentally committed an artifact")
    git(repo, "rm", "--cached", "--", "model.pt")
    git(repo, "commit", "-m", "drop artifact from tip")
    code, result = run_daily(checkout, "--apply")
    assert code == 75 and result["reason"] == "outgoing_paths_need_review"
    assert [row["path"] for row in result["outgoing_exclusions_or_review"]] == ["model.pt"]
    assert git(origin, "rev-parse", "twRule") == base


@pytest.mark.skipif(shutil.which("gitleaks") is None, reason="system Gitleaks unavailable")
@pytest.mark.parametrize("secret", [False, True])
def test_secret_scan_on_committed_content(checkout, secret):
    repo, origin, state = checkout
    base = git(origin, "rev-parse", "twRule")
    dummy = "ghp_" + "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    source = f'token = "{dummy}"\n' if secret else "reviewed_feature = True\n"
    (repo / "new_feature.py").write_text(source)
    git(repo, "add", "--", "new_feature.py")
    git(repo, "commit", "-m", "feature commit")
    code, result = run_daily(checkout, "--apply")
    if secret:
        assert code == 75 and result["reason"] == "secret_scan_findings", result
        assert git(origin, "rev-parse", "twRule") == base
        assert dummy not in (state / "latest.json").read_text()
    else:
        assert code == 0 and result["secret_scan_passed"] and result["state"] == "pushed", result
        assert git(origin, "rev-parse", "twRule") == git(repo, "rev-parse", "HEAD")


def test_remote_ahead_defers_and_does_not_untrack(checkout, tmp_path):
    repo, origin, _ = checkout
    peer = tmp_path / "peer"
    git(repo, "clone", "--branch", "twRule", str(origin), str(peer))
    git(peer, "-c", "user.email=fixture@example.invalid", "-c", "user.name=Fixture",
        "commit", "--allow-empty", "-m", "new peer commit")
    git(peer, "push", "origin", "twRule")
    ahead = git(peer, "rev-parse", "HEAD")
    code, result = run_daily(checkout, "--apply")
    assert code == 75 and result["reason"] == "remote_not_local_ancestor"
    assert git(origin, "rev-parse", "twRule") == ahead
    assert git(repo, "ls-files", "--", STATE_PATH)
