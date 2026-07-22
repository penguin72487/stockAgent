#!/usr/bin/env bash
set -euo pipefail

version="1.0.3"
case "$(uname -s):$(uname -m)" in
  Linux:x86_64) platform="linux_amd64"; expected="ad4dd9e91b57eef8627d2038df09281d7f38dca02eeca0e66592b54087619953" ;;
  Linux:aarch64|Linux:arm64) platform="linux_arm64"; expected="9008e297f527634efe94688f67c7a49a534c561bf43d223e50f64bec899c15ca" ;;
  Darwin:x86_64) platform="darwin_amd64"; expected="ab029448074428dc757d2235109dd557e9f34e4865052432a6ea7c431f0a5a19" ;;
  Darwin:arm64) platform="darwin_arm64"; expected="d3082017b9f12d8716aa1fb4b33f80a4e781305971508db45bf777fc110a657d" ;;
  *) echo "unsupported platform: $(uname -s) $(uname -m)" >&2; exit 2 ;;
esac

asset="desync_${version}_${platform}.tar.gz"
tmp_dir="$(mktemp -d)"
archive="$tmp_dir/$asset"
curl -fsSL "https://github.com/folbricht/desync/releases/download/v${version}/${asset}" -o "$archive"
printf '%s  %s\n' "$expected" "$archive" | sha256sum -c -
tar -xzf "$archive" -C "$tmp_dir" desync
install -d -m 0755 "${HOME}/.local/bin"
install -m 0755 "$tmp_dir/desync" "${HOME}/.local/bin/desync"
"${HOME}/.local/bin/desync" help >/dev/null
echo "installed desync v${version} at ${HOME}/.local/bin/desync"
