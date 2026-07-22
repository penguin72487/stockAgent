# StockAgent multi-writer dataset synchronization

Git transports code; Syncthing must not transport a Git working tree. Dataset
snapshots use desync for immutable content-addressed chunks and Syncthing only
for transport of `/srv/stockagent-sync`.

Each host initializes the same layout with a permanent unique publisher ID:

```bash
./scripts/run_desync_snapshot.sh init --sync-root /srv/stockagent-sync --node-id trainer-a
```

The `.local-state/node-id` path is ignored by Syncthing. Each publisher owns
`heads/<dataset>/<node-id>.json`, eliminating shared-file head conflicts. The
latest complete snapshot is selected by `(physical_ms, logical, publisher)`.
Indexes, manifests, and chunks are immutable and checksum-verified before use.

Publish and restore a complete dataset snapshot:

```bash
./scripts/run_desync_snapshot.sh publish tw-public /root/stockAgent/data_tw_public \
  --sync-root /srv/stockagent-sync --metadata storage_frequency=daily
./scripts/run_desync_snapshot.sh status tw-public --sync-root /srv/stockagent-sync
./scripts/run_desync_snapshot.sh fetch tw-public --sync-root /srv/stockagent-sync \
  --materialized-root /srv/stockagent-snapshots \
  --pin /srv/stockagent-snapshots/tw-public.pin.json
./scripts/run_desync_snapshot.sh verify tw-public --sync-root /srv/stockagent-sync
```

Publishing compares a complete filesystem inventory before and after archive
creation and refuses to advance the head if the source changed. Stop download,
repair, and other writer processes or publish from a filesystem snapshot.
Training reads only the materialized snapshot named in the pin, never `latest`.

Configure both Syncthing nodes with Folder ID `stockagent-desync`, Folder Type
`Send & Receive`, and path `/srv/stockagent-sync`. Never copy Syncthing keys,
configuration, or `.local-state` between machines.
