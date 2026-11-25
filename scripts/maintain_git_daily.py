#!/usr/bin/env python3
"""Inventory a checkout, untrack committed local outputs, and publish audited commits."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from tempfile import TemporaryDirectory

import audit_git_content as content


REF = "refs/heads/twRule"
TRACKING_REF = "refs/remotes/origin/twRule"
GITHUB_URL = "https://github.com/penguin72487/stockAgent.git"
GIT_OPERATION_MARKERS = ("MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD", "rebase-apply",
                         "rebase-merge", "sequencer", "index.lock")


class Deferred(RuntimeError):
    """A run that needs a fresh checkout, review, or remote read before publication."""


def command(repo: Path, *args: str, env: dict | None = None, data: bytes | None = None,
            check: bool = True) -> subprocess.CompletedProcess:
    environment = {**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0",
                   "GIT_NO_REPLACE_OBJECTS": "1"}
    if env:
        environment.update(env)
    return subprocess.run(args, cwd=repo, env=environment, input=data, capture_output=True,
                          timeout=180, check=check)


def git(repo: Path, *args: str, **kwargs) -> bytes:
    return command(repo, "git", "--literal-pathspecs", *args, **kwargs).stdout


def paths(output: bytes) -> set[str]:
    return {os.fsdecode(value) for value in output.split(b"\0") if value}


def check_checkout(repo: Path) -> str:
    branch = command(repo, "git", "symbolic-ref", "-q", "HEAD", check=False)
    if branch.returncode or branch.stdout.decode().strip() != REF:
        raise Deferred("wrong_branch")
    git_dir = Path(git(repo, "rev-parse", "--absolute-git-dir").decode().strip())
    if any((git_dir / path).exists() for path in GIT_OPERATION_MARKERS) or git(repo, "ls-files", "-u"):
        raise Deferred("git_operation_in_progress")
    return git(repo, "rev-parse", REF).decode().strip()


def verify_origin(repo: Path, target: str) -> None:
    for args in [("--all",), ("--all", "--push")]:
        urls = git(repo, "remote", "get-url", *args, "origin").decode().splitlines()
        if urls != [target]:
            raise Deferred("origin_url_changed")


def transport(repo: Path, target: str, *args: str) -> bytes:
    settings = ()
    if target == GITHUB_URL:
        settings = ("-c", "credential.helper=", "-c", "credential.helper=!gh auth git-credential")
    return git(repo, *settings, *args)


def remote_head(repo: Path, target: str) -> str:
    rows = transport(repo, target, "ls-remote", "--refs", target, REF).decode().splitlines()
    if len(rows) != 1:
        raise Deferred("remote_branch_not_found")
    oid, name = rows[0].split("\t", 1)
    if name != REF or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", oid):
        raise Deferred("remote_branch_unverified")
    return oid


def refresh_tracking(repo: Path, target: str, observed: str) -> None:
    current = command(repo, "git", "rev-parse", "--verify", "-q", TRACKING_REF, check=False)
    if current.stdout.decode().strip() == observed:
        return
    # Explicit URL pins the destination and this fetch refreshes the normal
    # tracking ref without updating the checkout or fetching other branches.
    verify_origin(repo, target)
    transport(repo, target, "fetch", "--no-tags", "--no-recurse-submodules", "--no-write-fetch-head",
              "--no-auto-maintenance", target, f"{REF}:{TRACKING_REF}")


def check_secrets(repo: Path, base: str, head: str) -> None:
    # Use the Git directory as the source so an uncommitted worktree config
    # cannot change the scanner's default rules or ignore findings.
    git_dir = git(repo, "rev-parse", "--absolute-git-dir").decode().strip()
    log_opts = f"--full-history -m --no-ext-diff --no-textconv --no-renames {base}..{head}"
    scanner = command(repo, "env", "-u", "GITLEAKS_CONFIG", "gitleaks", "detect",
                      "--source", git_dir, "--log-opts", log_opts,
                      "--redact", "--no-banner", "--exit-code", "3", check=False)
    if scanner.returncode == 3:
        raise Deferred("secret_scan_findings")
    scanner.check_returncode()


def is_cleanup_stage(repo: Path, head: str, cleanup: set[str]) -> bool:
    staged = paths(git(repo, "diff", "--cached", "--name-only", "-z", "--no-renames", head))
    deletions = paths(git(repo, "diff", "--cached", "--name-only", "-z", "--no-renames",
                          "--diff-filter=D", head))
    return staged == deletions and staged <= cleanup


def remove_tracking(repo: Path, cleanup: set[str], **kwargs) -> None:
    encoded = b"\0".join(os.fsencode(path) for path in sorted(cleanup)) + b"\0"
    git(repo, "rm", "--cached", "--ignore-unmatch", "--pathspec-from-file=-",
        "--pathspec-file-nul", data=encoded, **kwargs)


def advance_cleanup(repo: Path, head: str, candidate: str, cleanup: set[str]) -> None:
    # Prepare Git's ref transaction before touching the real index. It locks
    # twRule and its HEAD reflog while only the selected paths are untracked.
    # If interrupted after rm --cached, the next run accepts those same staged
    # deletions and builds the exact cleanup commit again from the branch tip.
    with subprocess.Popen(["git", "update-ref", "-m", "daily Git hygiene", "--stdin"],
                          cwd=repo, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          stderr=subprocess.DEVNULL, text=True) as transaction:
        try:
            transaction.stdin.write(f"start\nupdate {REF} {candidate} {head}\nprepare\n")
            transaction.stdin.flush()
            if (transaction.stdout.readline() != "start: ok\n"
                    or transaction.stdout.readline() != "prepare: ok\n"):
                raise Deferred("local_ref_changed")
            if check_checkout(repo) != head or not is_cleanup_stage(repo, head, cleanup):
                raise Deferred("local_index_or_head_changed")
            remove_tracking(repo, cleanup)
            transaction.stdin.write("commit\n")
            transaction.stdin.flush()
            if transaction.stdout.readline() != "commit: ok\n":
                raise Deferred("cleanup_ref_update_incomplete")
        finally:
            transaction.stdin.close()
    if transaction.returncode != 0:
        raise Deferred("cleanup_ref_update_incomplete")


def cleanup_commit(repo: Path, head: str, work: Path, result: dict, *, apply: bool) -> str:
    # Read ignore rules and tracked entries from the committed snapshot. Local
    # editor changes and personal .git/info/exclude rules cannot select removal.
    ignore = work / "committed-ignore"
    ignore.write_bytes(git(repo, "show", f"{head}:.gitignore"))
    env = {"GIT_INDEX_FILE": str(work / "index")}
    git(repo, "read-tree", head, env=env)
    ignored = paths(git(repo, "ls-files", "-ci", "-z", f"--exclude-from={ignore}", env=env))
    cleanup = {path for path in ignored if content.decision(path)["decision"] == "exclude"}
    result["cleanup_paths"] = sorted(cleanup)
    if not cleanup:
        return head
    if not is_cleanup_stage(repo, head, cleanup):
        result["cleanup_deferred"] = "unrelated_staged_changes"
        return head
    if not apply:
        return head

    remove_tracking(repo, cleanup, env=env)
    command(repo, sys.executable, str(Path(content.__file__).resolve()), "--repo", str(repo),
            "--staged", "--check", env=env)
    git(repo, "diff", "--cached", "--check", head, env=env)
    candidate_tree = git(repo, "write-tree", env=env).decode().strip()
    candidate = git(repo, "commit-tree", candidate_tree, "-p", head,
                    "-m", "Stop tracking ignored local runtime and data outputs").decode().strip()
    advance_cleanup(repo, head, candidate, cleanup)
    result["cleanup_commit"] = candidate
    return candidate


def maintain(repo: Path, target: str, work: Path, result: dict, *, apply: bool) -> None:
    head = check_checkout(repo)
    verify_origin(repo, target)
    audit = content.audit(repo)
    if audit["head"] != head:
        raise Deferred("local_ref_changed")
    result.update(local_before=head, indexed_paths=len(audit["tracked"]),
                  working_tree_changed=len(paths(git(repo, "diff", "--name-only", "-z", head))),
                  visible_untracked=len(audit["untracked"]),
                  untracked_exclusions_or_review=[row for row in audit["untracked"]
                                                   if row["decision"] != "keep"],
                  tracked_exclusions=[row["path"] for row in audit["tracked"]
                                      if row["decision"] == "exclude"])
    upstream = remote_head(repo, target)
    result["remote_before"] = upstream
    ancestor = command(repo, "git", "merge-base", "--is-ancestor", upstream, head, check=False)
    if ancestor.returncode:
        raise Deferred("remote_not_local_ancestor")

    if head != upstream:
        result["outgoing_exclusions_or_review"] = content.outgoing_exclusions(repo, upstream, head)
        if result["outgoing_exclusions_or_review"]:
            raise Deferred("outgoing_paths_need_review")
        check_secrets(repo, upstream, head)
        result["secret_scan_passed"] = True

    candidate = cleanup_commit(repo, head, work, result, apply=apply)
    result["local_after"] = candidate
    if not apply:
        result["state"] = "planned"
        return
    if candidate == upstream:
        result.update(state="up_to_date", remote_after=upstream)
        refresh_tracking(repo, target, upstream)
        return
    verify_origin(repo, target)
    if check_checkout(repo) != candidate or remote_head(repo, target) != upstream:
        raise Deferred("ref_changed_before_push")
    transport(repo, target, "push", "--porcelain", "--no-follow-tags", "--no-recurse-submodules",
              target, f"{candidate}:{REF}")
    result["push_accepted"] = True
    observed = remote_head(repo, target)
    result["remote_after"] = observed
    if observed != candidate:
        raise Deferred("push_readback_changed")
    result["state"] = "pushed"
    refresh_tracking(repo, target, observed)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--expected-url", default=GITHUB_URL)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true", help="Commit eligible cleanup and push")
    args = parser.parse_args()
    os.umask(0o077)
    os.environ["GIT_OPTIONAL_LOCKS"] = "0"
    os.environ["GIT_NO_REPLACE_OBJECTS"] = "1"
    repo = args.repo.resolve(strict=True)
    state = args.state_dir.resolve()
    if Path(git(repo, "rev-parse", "--show-toplevel").decode().strip()) != repo or state.is_relative_to(repo):
        parser.error("specify the checkout root and a state directory outside it")
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (state / "owner.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({"state": "deferred", "reason": "run_already_active"}))
            return 75
        result = {"contract": "stockagent_daily_git_v1", "started_at": datetime.now(timezone.utc).isoformat(),
                  "branch": REF, "apply": args.apply}
        exit_code = 0
        with TemporaryDirectory(prefix="run-", dir=state) as temp:
            work = Path(temp)
            try:
                maintain(repo, args.expected_url, work, result, apply=args.apply)
            except Deferred as exc:
                result.update(state="deferred", reason=str(exc))
                exit_code = 75
            except (OSError, ValueError, subprocess.SubprocessError) as exc:
                # Git and scanner diagnostics can contain private inputs.
                result.update(state="failed", reason=type(exc).__name__)
                exit_code = 1
            result["finished_at"] = datetime.now(timezone.utc).isoformat()
            temporary = work / "receipt.json"
            temporary.write_text(json.dumps(result, ensure_ascii=True, indent=2) + "\n")
            os.replace(temporary, state / "latest.json")
        print(json.dumps({key: result[key] for key in ("state", "reason", "local_after", "remote_after")
                          if key in result}))
        return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
