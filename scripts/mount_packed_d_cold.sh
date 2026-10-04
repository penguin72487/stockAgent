#!/usr/bin/env bash
# Fail-closed penguin D: DrvFs mount with bounded 8 KiB 9p requests.
set -euo pipefail

volume=/srv/stockagent-d-volume
primary="$volume/stockagent-cold-primary/packed"
canonical=/srv/stockagent-packed
fallback_marker=.stockagent-d-mount-required
volume_marker="$volume/stockagent-cold-primary/volume.json"
expected_volume_id=9ba6ab87-3889-40e7-90e4-9597dc88abaf

usage() { printf 'usage: %s --check-volume|--check|--mount|--unmount\n' "$0" >&2; exit 2; }
[[ $# -eq 1 ]] || usage
case "$1" in
  --check-volume|--check|--mount|--unmount) ;;
  *) usage ;;
esac

check_volume() {
  [[ "$(findmnt -n -o TARGET -T "$volume")" == "$volume" &&
     "$(findmnt -n -o SOURCE -T "$volume")" == 'D:' &&
     "$(findmnt -n -o FSTYPE -T "$volume")" == 9p ]] || {
    printf 'enrolled D: DrvFs volume is not mounted\n' >&2; exit 2;
  }
  findmnt -n -o OPTIONS -T "$volume" | tr ',' '\n' | rg -qx 'msize=8192' || {
    printf 'D: DrvFs request size is not 8192 bytes\n' >&2; exit 2;
  }
  [[ -f "$volume_marker" && ! -L "$volume_marker" ]] || {
    printf 'D primary volume marker is missing or redirected\n' >&2; exit 2;
  }
  /usr/bin/python3 - "$volume_marker" "$expected_volume_id" <<'PY' || {
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    marker = json.load(source)
assert marker == {
    "schema_version": 1,
    "volume_id": sys.argv[2],
    "authority_node_id": "penguin",
    "purpose": "stockagent-d-cold-primary",
}
PY
    printf 'D primary volume identity mismatch\n' >&2; exit 2;
  }
}

check_primary() {
  [[ -d "$primary" && ! -L "$primary" &&
     -f "$primary/.stockagent-d-primary" &&
     -d "$primary/.stfolder" && -f "$primary/.stignore" &&
     -f "$primary/.local-state/node-id" ]] || {
    printf 'D primary packed namespace or identity is missing\n' >&2; exit 2;
  }
  /usr/bin/python3 - "$primary/.stockagent-d-primary" "$expected_volume_id" <<'PY' || {
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    marker = json.load(source)
assert marker == {
    "schema_version": 1,
    "volume_id": sys.argv[2],
    "backing": "drvfs_msize8192",
    "authority_node_id": "penguin",
    "resilience": "single_d_volume",
}
PY
    printf 'D primary packed marker identity mismatch\n' >&2; exit 2;
  }
  [[ "$(< "$primary/.local-state/node-id")" == penguin ]] || {
    printf 'D primary release node identity mismatch\n' >&2; exit 2;
  }
}

check_canonical() {
  [[ "$(findmnt -n -o TARGET -T "$canonical")" == "$canonical" &&
     "$(findmnt -n -o FSTYPE -T "$canonical")" == 9p &&
     "$(stat -c '%d:%i' "$canonical")" == "$(stat -c '%d:%i' "$primary")" ]] || {
    printf 'canonical cold store is not bound to the D primary namespace\n' >&2; exit 2;
  }
}

if [[ "$1" == --mount && "$(findmnt -n -o TARGET -T "$volume")" != "$volume" ]]; then
  [[ -d "$volume" && ! -L "$volume" ]] || {
    printf 'D primary mountpoint missing or redirected\n' >&2; exit 2;
  }
  mount -t drvfs 'D:' "$volume" -o metadata,msize=8192,uid=0,gid=0,noatime
fi
check_volume
if [[ "$1" == --check-volume ]]; then exit 0; fi
check_primary

if [[ "$1" == --mount && "$(findmnt -n -o TARGET -T "$canonical")" != "$canonical" ]]; then
  [[ -d "$canonical" && ! -L "$canonical" &&
     -f "$canonical/$fallback_marker" ]] || {
    printf 'canonical fallback marker missing; refusing to hide C data\n' >&2; exit 2;
  }
  if find "$canonical" -mindepth 1 -maxdepth 1 ! -name "$fallback_marker" -print -quit | rg -q .; then
    printf 'canonical fallback is not empty except its mount marker\n' >&2; exit 2;
  fi
  mount --bind "$primary" "$canonical"
fi
check_canonical

if [[ "$1" == --unmount ]]; then
  umount "$canonical"
  umount "$volume"
fi
