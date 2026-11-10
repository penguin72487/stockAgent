#!/usr/bin/env bash
# Keep the WSL service filesystem's free blocks available to WSL; do not trim.
set -euo pipefail

if [[ "$(systemd-detect-virt)" != wsl ]]; then
  echo 'This setting is restricted to WSL.' >&2
  exit 1
fi
if [[ "$(findmnt -n -o FSTYPE --mountpoint /)" != ext4 ]]; then
  echo 'The WSL root is not the expected ext4 filesystem.' >&2
  exit 1
fi

options=",$(findmnt -n -o OPTIONS --mountpoint /),"
if [[ "$options" == *,discard,* ]]; then
  mount -o remount,nodiscard /
fi
options=",$(findmnt -n -o OPTIONS --mountpoint /),"
if [[ "$options" == *,discard,* ]]; then
  echo 'WSL root still has online discard enabled.' >&2
  exit 1
fi
echo 'WSL root online discard is disabled; no trim was issued.'
