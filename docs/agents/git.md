# Git content and history cleanup

Git owns the reproducible software and its reviewable decisions, not a copy of
every file used by a running node. This contract covers new-file review,
`.gitignore`, index removal and history rewriting; it does not authorize local
dataset/checkpoint retirement or changes to production services.

## Decide by role

| Role | Git disposition |
| --- | --- |
| Maintained source, focused tests, schemas, declared dependencies and lockfiles | Keep |
| Portable config/deployment templates and minimal, licensed non-private fixtures | Keep |
| Concise research conclusions, correctness contracts, exact source identities and recovery instructions | Keep |
| Downloaded data, prepared panels, model weights, training outputs, caches, logs and machine state | Exclude; preserve through the owning data/artifact lifecycle |
| Generated HTML/JSON report exports, environment secrets and duplicate skill backup trees | Exclude; retain needed evidence outside Git |
| Large source, unknown files, unreferenced scripts and ambiguous evidence | Review purpose, consumers and recovery; never delete from size/name alone |

For each new file ask what behavior or reproducibility obligation needs it,
whether an existing file already owns that responsibility, and whether it is
source or reproducible output. A new reusable audit script is source; its latest
JSON dump is not. A shared dashboard HTML asset is source; an exported dashboard
snapshot is output. `configs/data_sync` is source, not a root `data_*` payload.
Keep non-reconstructible evidence even when its hosting location needs changing.

## Before staging and committing

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_git_content.py
run_fintech_python scripts/audit_git_content.py --staged --check
git diff --cached --check
git diff --cached --stat
```

The tool reports index bytes and visible untracked files. `--history` adds all
local refs, including files no longer in HEAD; `--reflogs` extends that inventory
to locally retained reflog commits. It reports path-role candidates, not a
secret-content audit or proof every script is necessary. Review source purpose
and private values separately; a filename ending in `.example` is not evidence
that its contents are safe. Do not install hooks by overwriting existing hooks.
Standalone tree refs (for example IDE checkpoints) are inventoried too; pathless
blob refs require explicit review. A commit-only history walk is insufficient.

Put new node data under root `data_*`, outputs under `artifacts`, and transient
report exports under `reports`; shared code/config/fixtures remain visible.
Small reviewed binary fixtures belong under `test/fixtures`, not an exception
for all production datasets. Avoid blanket `*.json`, `*.csv`, `*.html`, `docs/`
or `.agents/skills/` ignores: those also hide necessary software and evidence.

## Historical removal is a separate, recoverable operation

`.gitignore` only controls untracked files; it does not remove an index entry or
old commits. See the [Git documentation](https://git-scm.com/docs/gitignore).
For approved exclusions, remove exact index entries with `git rm --cached --`
while retaining local files. Do not use `git clean`, `reset --hard` or delete a
data directory to make status quiet.

Before rewriting, inventory all branches/tags/other refs, reflogs, detached and
attached worktrees, remotes, dirty index/working files and renamed historical
paths. Freeze an explicit path manifest with reasons. Never infer deletion of
all large blobs: code can be large, and the same blob can have a retained path.

Keep a private recovery copy outside the repository being rewritten. A verified
`git bundle --all` covers reachable refs, **not** dirty work, per-worktree state
or arbitrary reflog-only objects; preserve those independently. Verify an
independent restore and checksums before mutation. See
[Git bundle](https://git-scm.com/docs/git-bundle).

Use a disposable, independent mirror (no shared object hardlinks/alternates) and
[git-filter-repo](https://github.com/newren/git-filter-repo) with an exact reviewed
path list. Verify all refs and commit mappings, every retained mapped tree,
path absence throughout history, object integrity and actual packed size.
Keep the old/new SHA map outside the cleaned repository; restoring an old branch
or pushing from an old clone can reintroduce removed history.

Applying rewritten refs to a shared active checkout requires coordination with
every worktree writer and preservation/reapplication of its index and dirty
work. Do not expire live reflogs or prune shared objects while other worktrees
still require their old commits. A cleaned mirror is a verified candidate, not
proof that the active checkout or GitHub history has been replaced.

Remote force-push is a distinct external mutation. Obtain explicit target and
approval, capture current remote ref SHAs, and use narrowly scoped leases; do
not infer authorization for `push --mirror`, branch/tag deletion or stopping
other work. Remote PR/cache/clone retention and credential revocation are
separate from local rewriting. Report exactly what was removed, what remains,
where the recovery copy lives and whether remote history was changed.
