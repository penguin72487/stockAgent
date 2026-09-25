#!/usr/bin/env bash
# Non-destructive, resumable initial copy from the verified D archive to the
# D-backed ext4 staging volume. This script does not cut over any service.
set -euo pipefail

cold_image=/mnt/d/stockagent-cold-primary/packed.ext4.img
cold_source_root=/mnt/d/stockagent-backup/packed
cold_stage_root=/srv/stockagent-packed-d-stage
expected_uuid=7371835a-d0d1-442a-a50f-9292f79c3c3c
expected_volume_id=9ba6ab87-3889-40e7-90e4-9597dc88abaf

if [[ $# -ne 1 || ( "$1" != "--plan" && "$1" != "--copy" ) ]]; then
  printf 'usage: %s --plan|--copy\n' "$0" >&2
  exit 2
fi

mount_source="$(findmnt -n -o SOURCE -T /mnt/d)"
mount_type="$(findmnt -n -o FSTYPE -T /mnt/d)"
if [[ "$mount_source" != 'D:\' || ( "$mount_type" != 9p && "$mount_type" != drvfs ) ]]; then
  printf 'D: is not the expected Windows drive mount\n' >&2
  exit 2
fi
if [[ ! -f "$cold_image" || -L "$cold_image" ]]; then
  printf 'cold image is missing or is a symlink\n' >&2
  exit 2
fi
if [[ "$(findmnt -n -o TARGET -T "$cold_stage_root")" != "$cold_stage_root" ||
      "$(findmnt -n -o FSTYPE -T "$cold_stage_root")" != ext4 ]]; then
  printf 'D cold staging root is not a distinct ext4 mount\n' >&2
  exit 2
fi
loop_device="$(findmnt -n -o SOURCE -T "$cold_stage_root")"
if [[ "$(blkid -s UUID -o value "$loop_device")" != "$expected_uuid" ]]; then
  printf 'D cold staging filesystem UUID does not match\n' >&2
  exit 2
fi
if ! losetup -j "$cold_image" | rg -q "^${loop_device}:"; then
  printf 'staging loop device is not backed by the expected D image\n' >&2
  exit 2
fi

marker=/mnt/d/stockagent-backup/backup-volume.json
if ! rg -q "\"volume_id\":\"${expected_volume_id}\"" "$marker"; then
  printf 'D backup volume marker does not match\n' >&2
  exit 2
fi
if [[ -L "$cold_source_root" || -L "$cold_source_root/objects" ||
      ! -d "$cold_source_root/objects" || ! -d "$cold_stage_root/manifests" ]]; then
  printf 'source or staging namespace is incomplete\n' >&2
  exit 2
fi
if find "$cold_source_root/objects" -type l -print -quit | rg -q .; then
  printf 'source object namespace contains a symlink\n' >&2
  exit 2
fi

mkdir -p /var/lib/stockagent-d-cold-migration
exec 9>/var/lib/stockagent-d-cold-migration/stage.lock
flock -n 9 || { printf 'D cold staging is already running\n' >&2; exit 2; }

rsync_args=(-a --numeric-ids --ignore-existing --stats)
if [[ "$1" == "--plan" ]]; then
  rsync_args+=(-n)
else
  rsync_args+=(--bwlimit=120M)
fi
rsync "${rsync_args[@]}" \
  "$cold_source_root/objects/" "$cold_stage_root/objects/"
if [[ "$1" == "--copy" ]]; then
  # Metadata is tiny. Refresh it after payloads so a newly received head is
  # never staged before the object pass that can satisfy it. A later repeated
  # --copy handles any head that advances during this pass; verification must
  # still prove the exact selected head and all of its object digests.
  rsync -a --numeric-ids --ignore-existing \
    "$cold_source_root/manifests/" "$cold_stage_root/manifests/"
  rsync -a --numeric-ids \
    "$cold_source_root/heads/" "$cold_stage_root/heads/"
  if [[ -d "$cold_source_root/head-history" ]]; then
    mkdir -p "$cold_stage_root/head-history"
    rsync -a --numeric-ids --ignore-existing \
      "$cold_source_root/head-history/" "$cold_stage_root/head-history/"
  fi
  sync -f "$cold_stage_root"
fi
