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

### Futures preparation source placement and accidental-deletion recovery

- The user's 2026-10-04 correction places retained futures preparation **raw
  originals, observations and source receipts** under
  `data_tw_index_futures/preparation_sources/`, not `artifacts/markets`.
  `margin_sources`, `margin_repair_pending`, `margin_repair_inputs` and the
  retained official final-settlement source belong to this data role. Keep
  experiment compilation/checkpoints under node-local artifacts; do not move
  panels, tensors or historical parser output versions into the source tree.
- Same-filesystem migration uses a current inventory, no process references,
  atomic no-overwrite renames and preserved inode/file fingerprints. Old
  absolute receipt paths may remain explicit data-only compatibility symlinks;
  never rewrite immutable provenance just to make an old path look current.
  Such aliases are not extra payload copies or permission to evict sources.
  The current compiled release may use node-local `current_release_source_refs`
  containing verified hardlink references to immutable canonical input bundles;
  its artifact raw attachment directories are explicit symlink interfaces only.
  Retire a generated duplicate only after full original/duplicate SHA, exact
  declared name sets, source stability, inode/link and process gates pass, then
  prove the alias still reconstructs every original receipt. Preserve derived
  admission/valuation proof tables and both original core release hashes.
  These reference views are excluded from cold publication and are not enrolled
  in seven-day automatic GC; they do not replace authoritative input bundles.
  The publication catalog excludes the dedicated `margin_sources` bundle from
  its parent dataset to avoid duplicate transport. New staging/refresh writes
  target the canonical source location, not a new legacy artifact directory.
- An accidentally deleted root may be held through the root-owned private
  `/var/lib/stockagent-legacy-return/recovery-holds.json`; both bulk and legacy
  remote retirement must fail closed for the named root and its descendants.
  Invalid/redirected hold policies deny retirement. A hold is protection, not a
  publication or recoverability proof; unrelated verified cleanup may continue.
- Receipt-matching C originals or independently decoded D carrier members may
  rescue exact observations without deleting the carrier. Prefer the latest
  needed sources and a pinned canonical rebuild over hydrating all old parser
  versions. Publish/expose derived current data only after the original
  release hashes match. A received, source-changing tar archive is preservation
  evidence, not proof every latest original was captured or formal cold ACK.
  See [the scoped recovery runbook](../vast_futures_preparation_recovery_2026-10-04.md).

### Physical layer ownership

| Layer | Current authority | Mutable | Syncthing |
|---|---|---:|---:|
| Code, configs, contracts | Git working tree | yes, through Git | no data folder |
| Canonical producer workspace | catalog-resolved `source`, including `/srv/stockagent-live/data_tw_public` for `tw-public` | yes | no |
| Fleet current cold store | `/srv/stockagent-packed`; on penguin this is a guarded bind mount of `D:\stockagent-cold-primary\packed` | immutable release objects and metadata | yes, Folder ID `stockagent-packed` |
| Versioned lake | guarded D `/srv/stockagent-d-volume/stockagent-immutable-lake` | immutable Parquet/ZSTD and closed catalog exports; live catalog stays in PostgreSQL on SSD | closed deliveries only, never live PG files |
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
  3600-second periodic rescan as fallback on penguin (2026-10-04 measured
  repair), plus the existing durable pending-scan retry timer. Never infer delivery solely from a
  successful scan request. Full-replica nodes retain the rolling current/protected
  manifests, per-node heads, inventories, packs, blobs, and their proofs.  An
  explicitly enrolled ephemeral compute node
  may use index-only edge mode: it still synchronizes heads/manifests/inventories
  in real time, ignores local blob/pack payload copies, and hydrates the exact
  objects for a selected release before use.  Repositories, mutable `data_*`
  trees, materialized caches, downloader shards, and active training directories
  do not belong in this folder.
- With penguin's only paired packed consumer being the verified index-only
  Vast node, penguin retains all authoritative blobs/packs on D but excludes
  `/objects/blobs/*/*` and `/objects/packs/*/*` from this Syncthing transport.
  Heads, manifests, inventories and shard directories still synchronize;
  `stockagent-packed-transport.timer` polls the existing Vast edge demand every
  15 seconds and resolves fixed releases against penguin's manifests. With
  private `/etc/stockagent/packed-rclone-transport.json` enrolled, payloads use
  its bounded rclone SFTP adapter; source Syncthing has no blob/pack exceptions,
  ensuring exactly one payload writer. It verifies complete SHA, fences demand
  and promotes atomically without overwriting. Without that enrollment, the
  older exact Syncthing exceptions remain the fallback. The existing
  cold fetch still owns hydration, full verification, leases and local eviction;
  NAS receives complete payloads through the separate verified backup ingress.
  State/ignore transitions and disconnections preserve the existing source
  policy; a durable scan intent survives POST/scan timeouts. Completion removes
  the request exceptions without deleting authoritative objects. This transport
  filter does not enroll penguin as an evictable edge. Adding another packed
  peer invalidates the enrolled sole-peer SFTP contract and requires explicit
  revalidation; Syncthing-only transport can restore full replication. See
  [the selected deployment](../ducklake_temporal_replication_2026-10-05.md) and
  [the measured repair](../vast_sync_cleanup_acceleration_2026-10-04.md).

### Selected DuckLake / Temporal / NAS archive boundary

The 2026-10-05 selected deployment extends the canonical packed source contract:

- DuckLake's PostgreSQL catalog registers exact dataset releases, CAS members,
  captures and original UTF-8 manifest/head bytes in four Parquet/ZSTD registry
  tables. Native snapshots identify registry state. Registration does not
  convert every financial table, prove missing history or transfer authority.
- Temporal uses dedicated PostgreSQL persistence/visibility databases and real
  durable catalog/source-replication workflows. Activities have heartbeats,
  durable publication/copy intents and canonical locks; no arbitrary received
  code, provider queries, GPU jobs or broker orders are dispatched.
- lab203's fixed `lab203-lake-relay` rclone-copies closed deliveries to a
  separate immutable NAS archive. Accept only exact paired ACKs after full
  file/set/byte checks, independent NAS readback, mount/runtime/owner guards
  and, for catalogs, isolated PG plus native DuckLake restore. Syncthing is
  the private ingress/receipt channel; live catalog/repository files never sync.
- Restic continues code/config/SQL backup and retains original raw history.
  `cold_object_replication_enabled: false` delegates new cold payload waves to
  Temporal; the original stream still consumes old ACKs and auxiliary/recovery
  work. Overlapping coverage counters must not be added.
- Source ACKs persist before cleanup. `stockagent-lake-transport-gc.timer`
  handles one exact accepted wave per invocation through the shared journal
  and per-delivery lock. Inventory, primary recovery, process, signatures and
  exact NAS gates remain mandatory. Only temporary ingress is retired; D/NAS
  primary objects are retained. Cleanup cannot block the next increment.
- No snapshot expiration, DuckLake cleanup, NAS deletion, Restic prune or
  formatting is enabled. Direct NAS recovery, reconstruction of NAS-attested
  identical copies, process recovery and physical reboot are separate claims.

Use `bash scripts/run_lakehouse_control.sh status` and the dated runbook. This
wrapper selects the private Mamba role and PID 1 mount namespace; do not print
catalog credentials or hard-code an interpreter path.
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
  in the ingress folder or auto-activate models. Quarantine acceptance alone
  never authorizes removal of a source copy.
- The user's 2026-10-04 four-node policy permits the existing penguin remote
  cold-artifact ingress owner to enroll canonical lifecycle-complete Vast runs
  in `markets`/`ablations` under `configs/data_sync/training_return.json`.
  A returned run may be retired on Vast only after exact penguin cold authority,
  every decoded file SHA/mode and complete-set verification, fresh paired cold
  convergence, and an authenticated `durable_completed_training_return_v2` ACK.
  Recheck the full shared convergence gate after independent reconstruction;
  an earlier idle observation cannot authorize a later unlink. ACKs expire
  after 30 minutes, tolerate at most 60 seconds of future clock skew, and must
  remain fresh through plan/apply and the final pre-unlink check. Unknown,
  expired or malformed timestamps preserve the source/quarantine for a new
  verification attempt; never replay an old ACK as current deletion authority.
  Require real local filesystem/capacity admission for mutable penguin staging,
  fixed run budgets, a fresh dry-run fingerprint, source stability/full SHA,
  process/service/link/mount gates and private quarantine journal before unlink.
  This role-specific completed-output handoff does not bypass ordinary input
  cache leases or authorize deleting sources, incomplete runs, unknown derived
  caches, cold objects or NAS snapshots. Reuse the existing ingress timer/lock;
  do not put mutable training output trees into Syncthing. See
  [four-node architecture](../four_node_storage_architecture_2026-10-04.md).
  Policy v2 may unlink only names inside the exact returned run when regular
  files share an inode. Preserve all external names, block external FD/mmap
  consumers by inode identity, bind link counts into the fresh fingerprint,
  account for only controller-owned unlink changes, and reuse canonical final
  inode block accounting. Unknown links/mounts and original source inputs stay
  protected. Keep one fixed unconfirmed return wave before another publication;
  a missing/busy origin may confirm its cold wave without authorizing deletion.
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
  The user's later parallel-pipeline request allows bounded worker pools under
  that same coordinator/owner. Independent backup and fixed-snapshot restore
  jobs may overlap; Restic's exclusive repository check remains a shared
  post-write barrier. Requeue failed jobs without blocking valid batches and
  preserve exact recovery/ACK gates. Measure worker choices on the actual NAS;
  see [parallel backup](../lab203_parallel_backup_2026-10-04.md). A frozen
  upgrade delivered by Syncthing is not proof the remote service was upgraded.
  A bounded immutable control package may be published under its own metadata
  lock without waiting for a long source data cycle: stage outside watched
  ingress, pin the enrolled physical transport alias, verify all hashes, and
  rename the complete package atomically. This grants no source-ledger,
  recovery-request, release, or retirement authority; the data owner remains
  unique. Preserve frozen previous packages and the disk reserve.
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
- Before edge `gc`, `evict`, `prune --apply`, or post-hydration payload cleanup,
  install the current payload allowlist in `.stignore-edge`, scan and observe
  fresh convergence before unlink. Keep shard directories synchronized; ignore
  their payload children, not the directories themselves. A zero-byte directory
  debt is still a nonzero `needItems`, even at 100% completion. Serialize the
  entire operation against hydration with the existing owner lock and release
  it on every return or exception. See the measured repair and focused tests in
  [Vast storage cleanup](../vastai_storage_cleanup_2026-10-04.md).

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
  stable, inactive legacy `artifacts/markets` or `artifacts/ablations` roots listed in
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
- The user's 2026-10-04 instruction also authorizes a one-shot return of Vast's
  `artifacts/markets` and `artifacts/ablations` to penguin's D primary. Use
  `scripts/return_remote_legacy_archives.py`, its explicit preservation policy
  `configs/data_sync/vastai_legacy_archive_return.json`, and retained full
  inventory/generated allowlist. Reuse canonical legacy encoding, packing,
  independent D decode, service/process/pin checks and the existing ingress
  owner lock. This is not raw Syncthing, completed-training certification,
  model activation, another publisher or a second cold store. Current remote
  consumer configurations must parse before source retirement: unresolved Git
  merges fail closed, never resolve them by choosing a side for cleanup.
  Keep per-item acknowledgements and dry-run/apply receipts; preserve active,
  recent, oversized, shared-inode, redirected and unregistered entries. Twelve
  hours of source stability and an explicit immediate retirement bypass do not
  weaken any exact-content/transport/reference gate. New legacy archives retain
  empty directories, permissions and mtimes; old immutable archives remain
  unchanged. Private transfer/encoding scratch may be pruned only after exact
  independent D recovery, no unknown bytes/links/references and unchanged
  quarantine signatures. See [Vast artifact return](../vast_all_artifacts_cold_return_2026-10-04.md).
  Its measured optional `gzip-1-adaptive-over-8m` cold profile selects gzip only
  when complete-file encoding saves over 10%; retain raw otherwise and reuse
  existing verified stage receipts without rewriting historical objects. Original
  SHA/mode/mtime, independent decoded recovery and every retirement gate remain
  mandatory. Network compression is a separate policy. Preserve immutable
  per-attempt evidence; upgrade known cohort workers only at an unowned,
  childless transaction boundary. The experimental batched directory durability
  barrier remains opt-in/disabled after this workload's comparison; it must
  fsync payloads and every renamed parent before exposing any manifest/head.
- The later 2026-10-04 explicit full `markets`/`ablations` compressed return and
  unused-cache review permit the one-shot `receive_vast_bulk_archives.py` /
  `organize_vast_bulk_archives.py` preservation format and strict local
  `vastai_bulk_preservation.json` policy. It is non-deployable and does not
  replace normal incremental publication or seven-day leases. Use SSH tar/zstd
  streams, D-only incoming, canonical content-addressed blobs and the SAME
  ingress owner. Require full original SHA/metadata/empty-directory decoding,
  independent canonical D decoding, fresh manifest-bound ACK and all current
  convergence/source/consumer/pin/link/quarantine gates before remote unlink.
  Cache names do not prove regenerability. Known stable immutable generation
  `.npy` duplicates may be exact-SHA hardlinked under panel writer locks with
  logical paths retained; unsupported/original/active/service caches stay.
  WSL 64 KiB 9p bulk I/O failed ENOMEM here. Do not promote it from a small
  benchmark or remount an in-flight authority. The validated Windows native
  FileStream adapter changes byte I/O only, retaining D enrollment and inode
  checks; keep the production 8 KiB mount. Prefix resume requires exact
  compressed SHA before append. Retain failed fragments; never count transport
  as cold acceptance. See [bulk return](../vast_bulk_compressed_return_2026-10-04.md).
- The 2026-10-05 bulk acknowledgement v2 may unlink only the exact fully
  preserved names, leaving every external hardlink intact. Bind
  `shared_file_policy: unlink_preserved_names_only` into the acknowledgement
  identity and plan fingerprint; v1 still rejects unknown shared names. Reuse
  the completed-return inode FD/mmap check before comparison and after
  quarantine. Check size/mode/mtime and one-link decrement after each unlink;
  only a final link release counts as reclaimed allocated bytes. This does not
  authorize deleting aliases or establish training lifecycle completion.
- Publication and retirement reuse the common cold-ingress owner. Private
  transfer/encoding and independent immutable D recovery run outside it under
  a cohort/journal owner. A publication receipt is explicitly unverified until
  complete canonical object and original-member recovery passes. Recheck the
  exact release, D guard, current source/consumer/convergence and original proof
  expiry when reacquiring the retirement owner; never refresh a proof from stat
  alone. Worker upgrades remain limited to an unowned childless lock wait.
- A training-only role can exclude foreign penguin service templates only
  through a root-owned local enrollment, matching node identity, index-only
  edge state and current Supervisor/independent-process observation. It cannot
  waive active training/config/FD/mmap, recovery, pin or convergence checks.
  The periodic bulk coordinator may retry the privately enrolled old cache
  cohort after twelve-hour stability; bind measured hardware/compression and
  exact portable source fingerprints. New namespaces or changed generations
  are never enrolled automatically. This one-shot preservation is separate
  from the policy to rebuild future training views on Vast and discard them
  only after their reproducibility and current-use gates pass.
- On guarded canonical D, measured legacy recovery may read one immutable pack
  sequentially through the Windows native byte adapter into anonymous ext4
  scratch. Require the full object SHA before ZIP access, complete CRC/member
  hashes, before/after source signatures, and physical backing-drive admission
  plus a 32 GiB reserve. Keep the 8 KiB mount, logical cold path and source
  metadata unchanged. See [hardware-bound workflow measurements](../four_node_storage_optimization_2026-10-05.md).
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
  The opt-in manual `--tmp-torchinductor-only` scope names only the root-owned,
  non-redirected local `/tmp/torchinductor_root` compiler cache; it rejects
  custom roots and never authorizes pruning `/tmp` or itself enrolls a new automatic
  scope. The user's subsequent explicit automation request permits only the
  fixed `compiler-home-and-root-tmp` scope through a locally enrolled,
  root-controlled `/etc/stockagent/storage-pressure.json`. Reuse the existing
  maintenance lock and timer/cron; never load a deletion policy or commands
  from Syncthing. Validate both narrow root groups before any unlink, prohibit
  force/custom-process/custom-root overrides, keep at least 14 days, and reject
  redirect/mount/schema/ownership/duplicate-key/nonfinite-value ambiguity.
  Existing different policies must be preserved. The default 14-day atime/mtime cutoff, process deferral and per-file
  identity/ctime/mode/link-count checks remain in effect. Keep locks, partials,
  young/open/mapped/shared files and all source/model/cold data. Cleared compiled
  entries may need recompilation on the next matching training run.
  The self-service `stockagent-data automation-status [--human] [--live]` is
  read-only scheduler/receipt observation, not a new coordinator or permission
  to hydrate/delete. Scheduler liveness, live index convergence and independent
  exact cold recovery remain distinct. See [automation runbook](../automatic_cold_storage.md).
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
