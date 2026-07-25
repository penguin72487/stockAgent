# Multi-writer data synchronization with Git, desync, and Syncthing

This repository uses three separate consistency planes. They must not be aimed
at the same writable tree:

- Git owns code, configuration, schemas, and the snapshot tooling.
- desync owns immutable directory archives (`.caidx`) and content-addressed
  chunks (`.castr`).
- Syncthing replicates immutable indexes, manifests, chunks, and one mutable
  head per node.

Training reads a verified, materialized snapshot outside the Syncthing folder.
It never reads a directory while another node is modifying it.

## First-principles consistency contract

There is no central publisher and no consensus service. Therefore an absolute
"last event in real time" cannot be known when clocks or networks partition.
This implementation provides the strongest useful deterministic contract
without introducing consensus:

1. A publish creates a new immutable index and manifest. It never changes an
   existing snapshot.
2. Every machine has a unique, persistent node ID and writes only
   `heads/<dataset>/<node-id>.json`.
3. Hybrid Logical Clock order is `(physical_ns, logical, node_id)`. Every node
   resolves the same winner from the same set of heads.
4. A remote clock more than five minutes in the future is rejected by default.
   Keep NTP/chrony healthy on every machine.
5. A head is eligible only after its manifest exists and both manifest and
   index SHA-256 values match. A fetch also requires all unique chunks to be
   present locally.
6. Extraction happens in a new directory. The target becomes visible by an
   atomic rename only after its portable tree fingerprint matches the manifest.
7. A training run pins the exact manifest SHA and snapshot ID. Resume must use
   that pin, not resolve `latest` again.

The LWW unit is one dataset key. If two machines publish the same key, the
newest complete snapshot wins as a whole. Independent writers that must not
overwrite one another should use independent keys, for example
`openbb-equity-prices`, `openbb-fundamentals`, and `tw-public`. A generic
path-by-path merge of two directory archives is intentionally not fabricated:
it would require domain-specific merge rules or a consensus/catalog service.

Syncthing conflict copies under `heads/` are invalid. They indicate that two
machines reused the same node ID. The resolver reports and ignores them rather
than guessing.

## Directory layout

Use separate filesystems or directories for live inputs, replicated CAS, and
materialized snapshots:

```text
/srv/stockagent-live/                 # downloader/build output; not Syncthing
/srv/stockagent-sync/                 # one Syncthing send-receive folder
├── .stignore
├── heads/<dataset>/<node-id>.json
├── indices/<dataset>/<snapshot>.caidx
├── manifests/<dataset>/<snapshot>.json
└── stores/<dataset>.castr/
/srv/stockagent-snapshots/            # verified local training inputs; not Syncthing
└── <dataset>/<snapshot>/
```

Never place `/srv/stockagent-sync` inside a Git worktree. The CLI rejects that
layout. It also rejects a source or materialized root that overlaps the sync
root, preventing recursive archives and accidental replication of live data.

## Install and initialize each machine

The installer pins desync `v1.0.3` and verifies the release archive against the
upstream checksum file before installation:

```bash
./scripts/install_desync.sh
export STOCKAGENT_SYNC_ROOT=/srv/stockagent-sync
export STOCKAGENT_MATERIALIZED_ROOT=/srv/stockagent-snapshots
./scripts/run_desync_snapshot.sh init \
  --sync-root "$STOCKAGENT_SYNC_ROOT" \
  --node-id trainer-a
```

Use a different stable `--node-id` on every machine. Do not use a cloud image
with a pre-populated `.local-state/node-id`. Changing an initialized identity
requires the explicit `init --replace-node-id` flag.

Initialize the directory before adding it to Syncthing so `.stignore` exists
before the first scan. The tracked reference is
`deploy/syncthing/stockagent-desync.stignore`.

Configure `/srv/stockagent-sync` as a separate **Send & Receive** Syncthing
folder on every publisher. This is safe because:

- chunks, indexes, and manifests are immutable;
- different publishers normally create different content-addressed paths;
- the only mutable files are per-node heads, and a node owns exactly one head.

Do not reuse the current `/root/stockAgent` Syncthing folder for this layout,
and never synchronize `.git/`, `.local-state/`, materialized snapshots,
temporary downloads, lock files, SQLite WAL/SHM files, credentials, Conda
environments, or compiler caches.

## Publish

Publish only from a frozen directory or while holding the downloader's existing
dataset lock. The CLI scans metadata before and after desync and refuses to
publish if it observes a mutation, but a filesystem snapshot is stronger than
any userspace check.

```bash
./scripts/run_desync_snapshot.sh publish tw-public \
  /srv/stockagent-live/data_tw_public \
  --sync-root "$STOCKAGENT_SYNC_ROOT" \
  --metadata audit=strict \
  --metadata storage_frequency=daily
```

The default chunk range is `256:1024:4096` KiB. This intentionally produces
fewer Syncthing-visible chunk files than desync's small-file default. Benchmark
it on `data_openBB` before changing the fleet-wide value; all values are stored
in each manifest.

Publishing is locally serialized per `(dataset, node-id)`. Concurrent
publishers on different machines are allowed. desync writes content-addressed
chunks, then the CLI verifies `in-store == unique`, promotes the immutable
index, writes the immutable manifest, and updates the node's head last.

## Resolve, fetch, and pin

Inspect the current deterministic winner:

```bash
./scripts/run_desync_snapshot.sh status tw-public \
  --sync-root "$STOCKAGENT_SYNC_ROOT"
```

Materialize the latest complete snapshot and save the exact pin beside the run:

```bash
./scripts/run_desync_snapshot.sh fetch tw-public \
  --sync-root "$STOCKAGENT_SYNC_ROOT" \
  --materialized-root "$STOCKAGENT_MATERIALIZED_ROOT" \
  --pin artifacts/my-run/data_snapshot_pin.json
```

For resume or reproduction, pass the pinned `snapshot_id` explicitly:

```bash
./scripts/run_desync_snapshot.sh fetch tw-public \
  --snapshot-id tw-public-YYYYMMDDTHHMMSSnnnnnnnnnZ-l0-trainer-a-0123456789abcdef \
  --sync-root "$STOCKAGENT_SYNC_ROOT" \
  --materialized-root "$STOCKAGENT_MATERIALIZED_ROOT"
```

Verify CAS and a materialized tree independently:

```bash
./scripts/run_desync_snapshot.sh verify tw-public \
  --sync-root "$STOCKAGENT_SYNC_ROOT" \
  --materialized /srv/stockagent-snapshots/tw-public/SNAPSHOT_ID
```

Do not silently fall back to an older snapshot when the newest head has not
finished syncing. `fetch` fails with the missing chunk count. Waiting for the
latest data preserves reproducibility; selecting an older snapshot must be an
explicit `--snapshot-id` decision.

## Syncthing readiness

A running service is not sufficient. Before publishing or starting a fleet
training job, confirm the dedicated folder reports:

- `state=idle`
- `errors=0`
- `pullErrors=0`
- `needTotalItems=0`
- an empty `watchError`

The snapshot CLI adds a second, content-level gate, so a Syncthing ordering race
(head arriving before index or chunks) becomes a visible failure rather than a
partial training dataset.

## Retention and failure recovery

- Syncthing replication is not a backup. Deletion propagates.
- Do not run `desync prune` automatically. Pruning must collect every retained
  `.caidx`, acquire a fleet maintenance lock, and keep all snapshots referenced
  by active training-run pins.
- Interrupted publishes may leave unreferenced chunks. They are safe and should
  be reclaimed only by a separately reviewed retention operation.
- Interrupted extraction leaves a `.partial.<uuid>` directory outside the
  Syncthing tree for diagnosis. It never replaces a verified target.
- Back up the CAS and manifests outside Syncthing before the first prune.
- Never put secrets in `--metadata`; manifests replicate to every peer.

## Suggested migration

1. Keep the current `data_tw_public` share unchanged during the pilot.
2. Install desync and initialize the new, separate sync root on two machines.
3. Publish and fetch `data_tw_microstructure` first.
4. Compare manifest/index SHA-256, materialized fingerprint, transfer bytes,
   Syncthing scan time, database size, memory, and restore time.
5. Test one concurrent publish and prove every node resolves the same snapshot.
6. Stress-test the 1.8-million-file `data_openBB` tree.
7. Move training configs to pinned materialized paths.
8. Only then retire direct synchronization of live dataset directories.
