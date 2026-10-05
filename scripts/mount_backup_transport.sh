#!/usr/bin/env bash
# Source-side transport staging on the already enrolled D: volume.
set -euo pipefail
backup_mount_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
backup_backing=/srv/stockagent-d-volume/stockagent-backup-ingress-lab203
backup_transport=/srv/stockagent-backup-ingress-lab203
[[ $# -eq 1 ]] || exit 2
[[ "$1" == --check || "$1" == --mount || "$1" == --unmount ]] || exit 2
if [[ "$1" == --unmount ]]; then
  if [[ "$(findmnt -n -o TARGET -T "$backup_transport")" == "$backup_transport" ]]; then
    [[ "$(findmnt -n -o SOURCE -T "$backup_transport")" == 'D:[/stockagent-backup-ingress-lab203]' &&
       "$(findmnt -n -o FSTYPE -T "$backup_transport")" == 9p &&
       ! -L "$backup_transport" &&
       -f "$backup_transport/.transport-owner.json" && ! -L "$backup_transport/.transport-owner.json" ]] || exit 2
    cmp -s "$backup_transport/.transport-owner.json" "$backup_mount_repo/configs/data_sync/backup_transport_volume.json" || exit 2
    umount "$backup_transport"
  fi
  exit 0
fi
bash "$backup_mount_repo/scripts/mount_packed_d_cold.sh" --check
[[ -d "$backup_backing" && ! -L "$backup_backing" &&
   -d "$backup_transport" && ! -L "$backup_transport" &&
   -d "$backup_backing/.stfolder" && -f "$backup_backing/.stignore" &&
   -f "$backup_backing/.transport-owner.json" && ! -L "$backup_backing/.transport-owner.json" ]] || exit 2
cmp -s "$backup_backing/.transport-owner.json" "$backup_mount_repo/configs/data_sync/backup_transport_volume.json" || exit 2
if [[ "$1" == --mount && "$(findmnt -n -o TARGET -T "$backup_transport")" != "$backup_transport" ]]; then
  # The installer retains the small verified pre-migration C: batches underneath
  # this mount; they are never removed or used as an automatic fallback.
  mount --bind "$backup_backing" "$backup_transport"
fi
[[ "$(findmnt -n -o TARGET -T "$backup_transport")" == "$backup_transport" &&
   "$(findmnt -n -o FSTYPE -T "$backup_transport")" == 9p &&
   "$(stat -c '%d:%i' "$backup_transport")" == "$(stat -c '%d:%i' "$backup_backing")" ]] || exit 2
