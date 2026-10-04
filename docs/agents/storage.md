# Data storage and synchronization contracts

Read before publication, synchronization, materialization, migration, or cleanup. The destructive-operation gates below remain mandatory; reorganizing instructions does not authorize deletion.

## Data Storage And Syncthing Contract

This section is a correctness contract for every agent and machine, not a
description of one deployment snapshot.  Its purpose is to preserve data while
keeping synchronization incremental, cold storage compact, and local training
fast.  Daily commands live in `README.md`; the detailed runbook is
`docs/packed_dataset_storage.md`.  Historical sizes, snapshot IDs, IP addresses,
connection counts, and service states are evidence from one observation and must
be measured again.  If an example conflicts with the current catalog or tool
output, do not copy the example blindly.  Changing any boundary below requires a
coordinated code, config, test, and documentation change.

### First-principles invariants

- Preservation, synchronization, and usability are different claims.  A source
  is preserved only after its release passes source/freshness audit and every
  referenced packed object can reconstruct it.  A peer is synchronized only
  after Syncthing convergence and cold verification.  A dataset is locally
  usable only after the selected release is materialized and its `READY` proof
  verifies.
- Mutable work and immutable distribution must be separate.  Downloaders write
  resumable local workspaces; only an audited atomic release enters the shared
  cold store.  Receiving cold objects must never directly overwrite a running
  dataset or training directory.
- Deduplication is content-addressed.  File names, sizes, mtimes, and apparent
  similarity are not deletion proof.  Preserve one verified object per digest
  and let manifests retain logical paths.  An identical inventory is a semantic
  no-op: reuse the current release and do not advance a timestamp-only head.
  Never delete a source merely because another path looks duplicated.
- Small-file query layout and transport layout solve different problems.  Keep
  canonical Parquet/receipt grains needed by readers, but publish them through
  deterministic fixed hash buckets plus large-file blobs.  Do not replace this
  with raw small-file Syncthing or a single giant archive whose smallest change
  retransmits the entire dataset.
- Immutable TAIFEX rule evidence may share SHA-256-addressed, read-only local
  objects through `artifact_dedup.link_immutable_source`. Copy the producer once
  into `.rule-source-objects` beside the preparation bundles; never hard-link
  a mutable official source. Keep each bundle's logical `sources/` paths and
  manifest hashes unchanged. These local objects are not cold publications or
  training/readiness proofs, and must never be modified in place.
- "Real-time synchronization" means that the Syncthing watcher starts copying
  an atomic release immediately after all publication gates pass.  It never
  means syncing half-written downloader output.  Scheduled and continuous
  downloaders must use the canonical publish wrapper after each successful,
  auditable batch.

### Penguin source host and remote training ownership

The user's current operating standard (2026-10-01) makes penguin a **data and
service host, not a training/preparation host**. This supersedes the assumption
that every trainable view should be routinely generated and kept on penguin.

- Retain cleaned, deduplicated authoritative source observations, their original
  values/units, observation and availability clocks, historical revisions,
  licenses, hashes, download state and recovery receipts. Normalization may
  compact paths and encode typed Parquet; it must not silently discard unique
  observations or provenance. A current provider response cannot recover a
  historical revision or a short-retention Tick/BidAsk capture.
- Generate experiment-specific features, labels, panels, tensors, folds,
  execution tapes, training splits and compiler caches **on the remote training
  node on demand**. Reuse the existing builders and training lifecycle. Record
  exact source release IDs/hashes, Git/config/feature ABI and environment; never
  follow a moving `latest` during a run. Remote derived caches are expendable
  only after a verified rebuild path exists. Do not automatically hydrate or
  build them merely because a cold release arrives.
- A source table and a training cache can share a file. In particular, current
  OKX/Binance `*_features.parquet` files contain original market fields as well
  as calculated fields. Neither the name `features` nor `cache` licenses removal
  of the whole file. A lean source projection needs explicit column/clock/unit
  coverage, a versioned compatible consumer migration and exact cold recovery.
- Keep bounded projections/caches and deployed model assets that current
  collection, website, Discord or paper-execution services actually require.
  Treat them as named service dependencies, not permission for every training
  version to remain hot. `tw_public_stock_daily.parquet` currently has this
  service-compatibility exception. Preserve its existing publication gates.
- FinLab/FinMind and any `publish: false` or licensed source remain restricted
  by the catalog; a remote-training preference does not grant redistribution
  rights. Do not replace an unavailable authorized input with fabricated data.
- Existing derived views, unique training results and D cold history remain
  protected until inventory, exact recovery/rebuild, consumer/process, pin/lease
  and applicable synchronization gates pass. This role policy is **not** proof
  that today's remote rebuild works, that cold heads are current, or that an
  unmanaged local cache has a seven-day automatic GC.

Inventory and operational implications are recorded in
`docs/penguin_source_only_storage_2026-10-01.md`; the reproducible read-only entry
point is `scripts/audit_training_source_inventory.py`.

### Physical layer ownership

| Layer | Current authority | Mutable | Syncthing |
|---|---|---:|---:|
| Code, configs, contracts | Git working tree | yes, through Git | no data folder |
| Canonical producer workspace | catalog-resolved `source`, including `/srv/stockagent-live/data_tw_public` for `tw-public` | yes | no |
| Fleet current cold store | `/srv/stockagent-packed`; on penguin this is a guarded bind mount of `D:\stockagent-cold-primary\packed` | immutable release objects and metadata | yes, Folder ID `stockagent-packed` |
| Penguin C cold store | retired after exact C-to-D audit; never a fallback writer | no | no |
| Replaceable local hot cache | `/srv/stockagent-packed-materialized` | only lifecycle metadata; materialized data is immutable | no |
| In-progress training artifacts | node-local `artifacts` workspace | yes | no |
| Retired operational artifact transport | `/srv/stockagent-artifacts-hot` | yes, pending local audit | no; Syncthing folder retired |

- `configs/data_sync/packed_datasets.json` is the dataset publication catalog.
  Its `source`, `publish`, `excluded_subtrees`, writer-process, freshness, role,
  and redistribution restrictions are authoritative.  A dataset with
  `publish: false` stays local.  Do not create an unregistered alias or bypass a
  catalog restriction to make synchronization convenient.
- Packed and materialized trees are never downloader destinations.  A consumer
  that writes panel caches, metadata, checkpoints, logs, or temporary files must
  use a separate writable live/cache/artifact root.  In particular,
  `stocks/panel_cache_v2` is reproducible node-local state and is excluded from
  the `tw-public` release.  `artifacts/cache` is also node-local derived state:
  neither the hot artifact bridge nor its Syncthing folder may publish or
  rehydrate it. Only exact byte-identical, stable cache files may share an
  inode; a cache directory name is not proof that unique source data can go.
- Runtime `data_*` paths may be local directories or atomic symlinks, but the
  target's role must remain explicit.  A writable service must target its
  catalog-resolved live workspace, not a packed materialization.  Never infer
  authority from the convenient `data_*` link name.

### Syncthing topology and identity

- The canonical data namespace is `stockagent-packed` at
  `/srv/stockagent-packed`, configured Send & Receive and not paused. On
  penguin the D: DrvFs mount has no reliable inotify: an atomic publication
  requests an explicit Syncthing scan (objects before manifest/head), with a
  300-second periodic rescan as fallback. Never infer delivery solely from a
  successful scan request. Full-replica nodes retain the rolling current/protected
  manifests, per-node heads, inventories, packs, blobs, and their proofs.  An
  explicitly enrolled ephemeral compute node
  may use index-only edge mode: it still synchronizes heads/manifests/inventories
  in real time, ignores local blob/pack payload copies, and hydrates the exact
  objects for a selected release before use.  Repositories, mutable `data_*`
  trees, materialized caches, downloader shards, and active training directories
  do not belong in this folder.
- `stockagent`, `stockagent-desync`, `stockagent-artifacts-live`, and
  `stockagent-artifacts-hot` are retired Folder IDs. Never recreate or accept
  them. Preserve any unique data still present in the old local hot transport
  until an exact content and process-reference audit proves it redundant.
- The explicitly authorized `stockagent-artifact-ingress-vastai1t` folder is a
  separate, bounded **quarantine transport**, not another cold authority or hot
  artifact mirror. `configs/data_sync/artifact_ingress.json` pins its sole model
  root, checkpoint digest, resource bounds, origin and receiver. Vast sends only
  completed deterministic bucket packs / blobs and a hash-pinned envelope;
  penguin is receive-only. Reuse canonical packing primitives, reject source
  changes/process references and unsafe paths, and verify every received file
  plus training lifecycle before accepting into node-local quarantine. Only
  penguin may subsequently publish through the registered cold-artifact path.
  Do not change Vast's index-only role, sync raw artifacts, create release heads
  in the ingress folder, auto-activate models, or delete either source copy.
- Each enrolled producer owns one permanent release node ID and one unique
  Syncthing identity, currently `penguin` and `vastai1T`. `lab203` remains a
  retired historical **producer**: retain its existing cold heads, manifests,
  and objects. The user's 2026-10-03 instruction permits re-enrollment only as
  a backup receiver/relay in the bounded `stockagent-backup-ingress-lab203`
  transport after checking its local identity and configuration. The bounded
  `stockagent-backup-receipts-lab203` return namespace is lab203 Send Only and
  penguin Receive Only; it carries only allowlisted NAS acceptance/readiness,
  never keys or private config. Reuse the current lab203 backup owner; the
  return channel grants no release publishing authority. Fixed file backup,
  canonical source reconstruction and database restore remain separate proofs.
  See [continuous NAS backup](../continuous_nas_backup_2026-10-04.md).
  For unattended semantic recovery, source-pinned data-only requests may be
  consumed by fixed locally installed code in the existing backup service's
  post hook, under that same owner lock. Never interpret synced commands or
  execute moving synced code. Preserve separate file ACK and semantic proofs,
  bounded scratch/physical WSL capacity, retry status and exact scratch cleanup
  evidence; see [automatic backup](../lab203_automatic_backup_2026-10-04.md).
  Source-owned rolling transport retirement is limited to machine-ACKed,
  reconstructible `incremental_cold_objects` batches: unchanged canonical
  source/full-SHA file signatures, fixed NAS independent-restore ACK, exact
  inventory, no links/unknown files/process references, paired idle/error-free
  Syncthing convergence, retained dry-run/owner intent and explicit deletion
  scan. Preserve code/SQL, relayed pilots, failed staging, source objects and
  NAS snapshots; lab203's worker is not a second deletion owner. Recovery
  indexes retain the fixed NAS snapshots after transport removal.
  It does not
  restore publishing/training, the retired hot folders, auto-acceptance, or
  later lab203-produced releases. Its Windows-side deployment is handled
  locally by the user's Codex; SSH access is not a prerequisite. The user's
  latest instruction explicitly authorizes discarding and permanently deleting
  lab203's old StockAgent data and retiring its unused project services, without
  backup, migration, waiting periods, or exact recovery proof. This exception
  applies only to those local project files, not penguin, Vast, NAS, Windows,
  or other users' data. Identify the exact local roots and detach their old
  Syncthing shares before deletion so it cannot propagate to authoritative
  peers. Preserve the local Syncthing identity and use fresh backup directories.
  See [backup handoff](../lab203_backup_handoff_2026-10-03.md).
  Never copy Syncthing certificates, keys, device IDs, databases, or
  `.local-state/node-id` between machines.  `.local-state` remains ignored by
  Syncthing.
- QUIC and multiple connections are preferred throughput mechanisms, not data
  correctness proofs.  Syncthing's authenticated encrypted transport remains
  mandatory; TCP is a valid fallback.  Report QUIC, relay, TLS suite, or channel
  count as active only when the current connection record actually observes it.
- Platform supervision may differ: penguin uses systemd, while a
  Vast container may use a user supervisor and cron.  This does not change the
  storage contract.  Before calling a Vast cold store durable, verify that its
  path is on persistent storage that survives instance recreation.
- Index-only edge conversion is allowed only for a non-persistent compute node
  after a durable peer passes full object checksums and current Syncthing
  convergence, the payload tree has no process references, and local ignore
  rules are active before unlink.  It may delete only its ignored local
  `objects/blobs` and `objects/packs` copies; heads, manifests, inventories,
  materialized data, sources, and the durable peer remain untouched.  Direct
  cold publication is disabled in this mode.  On-demand use must temporarily
  re-include, receive, hash, and materialize the exact release, then may prune
  only the redundant local payload copy.
- On an index-only edge, cache GC may substitute a freshly observed, fully
  converged durable-peer proof for local payload presence.  Manifest hash,
  `READY`, pin, lease age, and live-process checks remain mandatory; a
  disconnected, incomplete, or invalid peer makes eviction fail closed.  Edge
  leases are capped at seven days; live references renew that seven-day window,
  while intentional longer retention must use a pin.

### Penguin D: primary and single-volume risk

- Current deployment authority is **penguin**. Its only local physical cold
  copy is `D:\stockagent-cold-primary\packed`, exposed at the stable
  `/srv/stockagent-packed` path. A producer node ID in an immutable manifest is
  provenance, not a competing authority. Retain existing per-node heads and
  never copy another node's identity. The user explicitly accepted losing the
  old independent C/D local backup; do not call Syncthing or a second path on
  the same D: volume an independent disaster-recovery backup.
- `stockagent-d-cold-mount.service` must validate the enrolled D: volume marker,
  guarded canonical bind mount, primary marker and node identity before
  Syncthing or a local publisher accesses the cold store. A missing D: mount
  exposes only the C-side fail-closed marker; it must never become a new cold
  store. The Syncthing unit depends on the mount service. Check the mount with
  `scripts/mount_packed_d_cold.sh --check` before cold publication or cleanup.
- The old `stockagent-packed-backup.service` and C-only
  `stockagent-packed-retention.timer` are retired/disabled. Their historical
  config and receipts may remain for audit, but must not be restarted or used
  as proof of an independent copy. `packed_backup.py status` reports
  `retired_single_d_primary` when the D primary is active. Do not run the old
  retention `apply` against the D bind mount. A future D-specific retention
  policy needs separate reachability, historical-recovery and peer proofs.
- Preserve existing D historical manifests, objects and immutable head history.
  A `snapshot_id` is an atomic release identifier, not a full copied tree.
  Preserve all referenced objects, pins, leases, current heads, and receipts;
  do not prune history merely from mtime or apparent duplication. Current-head
  verification never asserts that every old historical release is reconstructible.
  Missing D, disk pressure, conflict files, checksum mismatch or incomplete
  current release is degraded/blocked. WSL must be running for publication and
  synchronization; Windows cold-boot recovery remains a separate acceptance.
  The user's 2026-10-04 instruction abandons recovery only for the 25 pinned,
  non-current historical releases and 114 already-unavailable objects in
  `configs/data_sync/backup_history_disposition_20261004.json`. Recheck head,
  manifest and unavailable-object identity each capture; preserve existing
  bytes, metadata, pins and quarantine. This does not waive future/current
  damage or claim full-history recovery. Existing unreferenced immutable cold
  objects may be backed as unique evidence with no release/deletion claim;
  restricted-manifest objects must not be reclassified to bypass publication.
  See [full backup rollout](../lab203_complete_backup_rollout_2026-10-04.md).
  See `docs/d_cold_store_migration_2026-09-25.md` for migration receipts.

### Multi-writer publication and conflict resolution

- The format supports multiple producers, distinct from penguin's deployment
  authority. Full-replica publishing nodes publish
  immutable releases under their own permanent node IDs.  An index-only edge is
  a consumer and may not publish until it is explicitly returned to full-replica
  mode.  Per-node heads plus the deterministic HLC/LWW resolver choose the newest
  *valid* release; catalog freshness and non-regression checks outrank wall-clock
  recency.
- "Newest wins" applies only to validated release heads.  It does not authorize
  agents to compare mtimes, hand-edit a shared head, copy another node's head,
  impersonate its node ID, or accept a Syncthing conflict file.  A conflict file
  is a failed publication/synchronization condition that must be audited.
- Publish only through the catalog-backed entry points such as
  `stockagent-data publish`, `scripts/run_data_release.sh`, or
  `scripts/run_downloader_with_release.sh`.  Publication must fail closed while
  a declared writer is active or when build, strict audit, inventory hash,
  freshness receipt, coverage, rights, or atomic-head update is incomplete.
- For `tw-public` cold publication, a stale derived receipt is a durable
  publication veto, not a worker crash: retain the failed receipt and do not
  restart the same exact-byte audit every five minutes. Retry after the
  official-symbol or feature receipt changes, after successful feature
  reconciliation releases the source lock, or at the nightly timer. The
  publisher must still verify both receipts under the canonical source lock;
  no retry trigger may convert stale data into a release or enter the
  protected Taiwan opening window.
- Partial downloads, task shards, locks, PIDs, quarantine data, reproducible
  caches, and incomplete training runs remain node-local.  Completed artifacts
  may enter cold storage only after their lifecycle/completion contract and
  final hashes pass; an active service or named file is not completion evidence.
- A separately allowlisted `legacy-quarantine-archive` is a byte-preservation
  exception, **not** a completed-artifact release. Only penguin may archive
  stable, inactive legacy `artifacts/markets` roots listed in
  `configs/data_sync/legacy_artifact_archives.json`. Record every original path,
  size, signature and SHA-256, verify the encoded payload and exact decode, and
  mark `deployable=false` / `completion_claim=not_checked`. Never use such an
  archive as a model selection, promotion, or service-readiness proof. Hot-source
  use leases may be enrolled before a slow cold publication with the explicit
  `enroll` command: this records observation time only, never backdates a lease,
  asserts cold validity, or deletes bytes. All retirement gates remain mandatory.
  Retirement still requires an exact source and old hard-link mirror audit,
  direct D-primary cold verification, local Syncthing health and the peers in
  `artifact_retirement.json`, no pins or active service/process references,
  and an observed seven-day lease. Enabled US
  Discord service output is protected; do not retire that root merely because
  its bytes have been archived. Recovery is an explicit restore into a new path.
  Both full-run and legacy retirement resolve configured and selected models,
  candidate roots/configs, experiment input/initialization paths, runtime Discord
  enablement and independently configured overnight consumers through the shared
  artifact-consumer gate. Missing referenced configuration fails closed. Other
  collector/preparation dependencies still require a separate dependency and
  process audit; a clean Discord gate alone does not prove inactivity.
- An explicit user request to retire hot artifacts **without waiting seven
  days** authorizes a one-shot `--manual-immediate` age bypass in the canonical
  full-run/legacy retirement tools. Bind this mode into the dry-run fingerprint
  and record it in the retirement receipt; never backdate a lease or lower the
  automatic retention policy. All exact D cold/source/mirror recovery, source
  stability, pin, service/process, stopped-bridge, quarantine and current local/
  applicable-peer transport gates remain mandatory. The hourly timer never
  passes this flag. Batch manual retirement is restricted to the existing
  enrolled allowlist and must persist each plan and result independently.
- A user-authorized, one-shot legacy preservation may additionally select
  `--manual-capture`, but only for a source with an explicit
  `manual_capture_min_stable_hours` entry in the legacy catalog (at least 12
  hours). This is a distinct exact-capture contract, not an implicit extension
  of `--manual-immediate`. Automatic publication and retirement retain their
  seven-day source-stability and use-lease policies. Manual capture requires
  inactive process/service consumers, complete source signatures before and
  after encoding and verification, original and encoded SHA-256, independent
  decode, and a manifest-bound capture-plan digest. Reverify the source after
  cold commit. Retirement requires both flags, binds both into the dry-run
  fingerprint, and repeats all ordinary D/cold/mirror/pin/usage/transport and
  quarantine gates. The archive remains non-deployable; restore into a new
  path preserves original bytes, mode and mtime. No timer selects this mode.
- Archive encoding scratch is not a cold authority. The four manual legacy
  roots enrolled on 2026-10-03 may use the bounded
  `/var/lib/stockagent-legacy-archive-stage` C-side work area to avoid DrvFs
  per-small-file I/O. Require the D guard even before preparing this scratch,
  budget the full source size plus a 32 GiB C reserve, and publish only to the
  canonical D store. Preserve all interrupted D staging as audit evidence until
  separately verified redundant. After exact hot retirement, the explicit
  `prune-stage-plan` / `prune-stage-apply` transaction may remove only that
  dataset's C scratch: require its matching cold-only state, all original and
  encoded hashes, every cold member's independent reconstruction, exact control
  receipts, no unknown/symlink files, pin/process gates and unchanged dry-run
  signatures; rename to private quarantine and repeat the D/recovery/signature
  gates immediately before unlink. Never use this tool for D staging or objects.
  `verify --cold-only` without a retained scratch uses a bounded, private
  canonical fetch for verification only; remove it after successful original
  decode plus cold verification, and retain any failure for diagnosis. It must
  not activate/hydrate an artifact path or create a persistent C cold replica.
- Legacy recovery compares original bytes, paths, sizes, mtime and permissions
  with the archived content. Historical inode/device/ctime are observation
  provenance, not recoverable file content: unlinking a different verified hard
  link legitimately changes ctime. Record that drift and require the **current**
  full source signature to remain unchanged before/after hashing and between
  dry run/apply. Do not remove the SHA-256, exact decode or mutation-race gates,
  rewrite an immutable archive manifest, or count blocks still held by another
  hard link as recovered space.
- Automatic completed-artifact maintenance must keep discovery, publication,
  peer convergence, and source eviction as separate gates.  Publish at most one
  new wave at a time; deletion requires a later exact cold/source verification,
  an empty process-reference check, an unchanged source activity fingerprint,
  expired retention, and full intended-peer convergence.  Resolve peer identity
  from current Syncthing configuration rather than hard-coding a Device ID.
- On penguin the retired hot bridge left hard links between the repository
  artifact tree and `/srv/stockagent-artifacts-hot`. Source-only eviction is
  forbidden: it frees no shared payload, and some transport paths may be unique.
  The bridge must stay disabled and the retired folder must not be recreated. A complete
  run may become cold-only only through an exact, D-primary-backed retirement plan
  that checks both hot names, a seven-day use lease, pins, process references,
  and the local Syncthing health plus peers named by
  `configs/data_sync/artifact_retirement.json`. The penguin-only hot
  retirement policy currently names no remote peer: lab203 does not block
  local hot eviction; the retired C cold-object retention policy must not run.
  Install a node-local directory tombstone
  before unlinking either hot name. Partial cold releases cannot retire a
  whole run. A failed/interrupted retirement remains in quarantine for audit.
  The explicitly allowlisted enrolled-artifact timer checks hourly; an active
  seven-day lease is a cheap negative gate, not a cold recovery verification.
  On expiry, it must repeat all exact-content, pin, usage and transport gates.
- On-demand artifact use must resolve one exact retired release through the
  managed materialized cache, expose only an immutable symlink at its original
  artifact path, and renew the same seven-day lease. A deployed or intermittently
  read strategy requires a pin or an explicit `use` before a short job; an
  open-file monitor alone cannot guarantee future availability.
- Training, backtest, resume, and audit jobs must resolve and record one exact
  release ID before starting.  They may not re-resolve `latest` during a run or
  silently resume against a different release.

### Cold receive, materialization, and leases

- A receiving node's default steady state is `COLD_ONLY`.  Syncthing services,
  timers, cron jobs, and downloader hooks must not automatically run `fetch`,
  `use`, materialize a release, or create a repository `data_*` symlink.
- `stockagent-data use DATASET` is the explicit local transition from cold to
  hot.  It must verify the complete selected release before atomically changing
  the managed current link.  Treat the resulting materialized tree as immutable;
  writable consumers use another root.
- The default hot-cache lease is seven days.  The cache monitor runs every five
  minutes and inspects `/proc` file descriptors, maps, cwd, root, and executable
  references.  An observed reference renews `last_used_at` and extends the same
  lease TTL; recursive `atime` scanning is not a substitute.  A short job that
  can open and close between monitor scans must call `stockagent-data use` first.
- `stockagent-data gc --dry-run` must expose would-renew and would-evict actions.
  Automatic or manual eviction must refuse an active reference, pin, incomplete
  cold release, missing object, or mismatched `READY` proof.  `evict` may bypass
  lease age only; it may not bypass those safety proofs.
- Cache GC may delete only managed materialized versions.  It must never delete
  `/srv/stockagent-packed`, a canonical producer source, an active artifact, or
  an unmanaged directory. It must never act as cold-object GC. Former C cold
  deletion was a one-time, separately audited migration; there is no approved
  automatic D cold-object GC. Other nodes and D remain report-only until the
  user separately approves a new retention policy.
- Compiler caches are a separate rebuildable layer.  Under disk pressure, use
  `scripts/maintain_storage_pressure.py`: it may prune only allowlisted old
  TorchInductor/Triton/CUDA cache files after fd/mmap and signature rechecks.  It
  must never broaden its target to packed, materialized, source, artifact, Git,
  checkpoint, or receipt trees.  Automatic cleanup must defer the whole run
  while training, torchrun, distributed launch, or compiler worker processes
  are active; only an explicit manual force may bypass that process-level gate.
  Keep its audit receipt and low-I/O scheduling.
- An abandoned packed fetch staging tree is not an ordinary leased hot version.
  Audit one explicit canonical `.<release-id>.partial.<uuid>` with
  `stockagent-data prune-partial`; it is not automatic GC and cannot target a
  complete version or a producer source. Require the fetch/cache writer locks,
  seven-day age, no pin/process reference, complete cold SHA-256/ZIP checks and
  independent decoded SHA-256 for every selected file. Unlink only unchanged
  files that exactly match that release inventory; retain unknown, mismatched,
  young and shared-inode evidence. A missing cold object blocks cleanup. Keep
  the dry-run and apply receipts; never remove cold objects or clear a pin.
- Windows `%LOCALAPPDATA%\Temp` is not a disposable tree.  Never blanket-delete
  it from WSL or Windows.  Orphaned WSL swap recovery must use
  `scripts/cleanup_windows_wsl_temp_swap.ps1`: only a top-level GUID directory
  containing exactly one aged, inactive, exclusively unlockable `swap.vhdx`
  may be removed, with an audit/apply receipt and an immediate pre-delete
  recheck.  The swap belonging to any current `wslhost.exe --vm-id` is protected.

### Acceptance and reporting

- Do not call a node synchronized merely because the service is active or a
  device is connected.  For every intended peer require the canonical folder to
  be idle/up to date, `needBytes == 0`, all needed item/delete counts zero,
  folder/system errors zero, `pullErrors == 0`, empty `watchError`, peer
  completion 100%, and `remoteState == valid`.
- Syncthing convergence proves byte delivery, not release validity.  Afterwards
  run packed manifest/object verification for the selected release.  If the
  requested outcome is local usability, also verify materialization, `READY`,
  the exact release ID, and the resolved runtime link.
- Keep evidence boundaries explicit.  If an agent has only local or Syncthing
  API access, it may report observed transport and cold convergence but must not
  claim an inaccessible peer's materialized cache, service, disk persistence,
  or training readiness was verified.

### Destructive migration and retirement

- Never delete a legacy store, source tree, release, or duplicate-looking file
  until a current packed release independently reconstructs and verifies, every
  intended peer has converged, the replacement path is in use, and the exact
  deletion target and filesystem scope have been resolved.
- Start every cleanup with a read-only inventory/reachability report and dry run.
  Preserve receipts, pins, active process references, quarantine evidence, and
  any object reachable from a retained manifest.  Use explicit paths; never a
  broad root, unresolved variable, or unchecked glob.
- After an approved cleanup, report exactly what was removed, bytes recovered,
  what remains authoritative, whether recovery is possible, and fresh
  post-cleanup Syncthing plus release-verification evidence.
