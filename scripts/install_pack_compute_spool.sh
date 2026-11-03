#!/usr/bin/env bash
# Install the disposable compute consumer; never replace a storage authority.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ "$EUID" -ne 0 || $# -ne 0 ]]; then
    echo 'Usage: sudo bash scripts/install_pack_compute_spool.sh' >&2
    exit 2
fi
if [[ "$(readlink /proc/self/ns/mnt)" != "$(readlink /proc/1/ns/mnt)" ]]; then
    exec nsenter --mount=/proc/1/ns/mnt -- bash "$repo_root/scripts/install_pack_compute_spool.sh"
fi
for policy in /etc/stockagent/pack-compute-spool.json /etc/stockagent/pack-verification.json; do
    if [[ ! -f "$policy" || -L "$policy" || "$(stat -c '%u:%a' "$policy")" != '0:600' ]]; then
        echo "Install the explicitly enrolled private policy first: $policy" >&2
        exit 1
    fi
done
temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
units=(stockagent-pack-compute-spool.service stockagent-pack-compute-spool.timer)
paths=()
for unit in "${units[@]}"; do
    sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary_dir/$unit"
    if [[ -e "/etc/systemd/system/$unit" ]] && ! cmp -s "$temporary_dir/$unit" "/etc/systemd/system/$unit"; then
        echo "Preserve the changed existing unit; review it first: $unit" >&2
        exit 1
    fi
    paths+=("$temporary_dir/$unit")
done
systemd-analyze verify "${paths[@]}"
runtime_rule="$repo_root/deploy/tmpfiles/stockagent-agent-ssh.conf"
runtime_target=/etc/tmpfiles.d/stockagent-agent-ssh.conf
runtime_dropin=/etc/systemd/system/stockagent-pack-compute-spool.service.d/20-ssh-runtime.conf
for pair in "$runtime_rule:$runtime_target" "$repo_root/deploy/systemd/stockagent-pack-compute-spool-ssh-runtime.conf:$runtime_dropin"; do
    src="${pair%%:*}"
    dest="${pair#*:}"
    if [[ -L "$dest" ]] || { [[ -e "$dest" ]] && ! cmp -s "$src" "$dest"; }; then
        echo "Preserve the changed existing SSH runtime rule; review it first: $dest" >&2
        exit 1
    fi
done
install -d -m 0755 "$(dirname "$runtime_dropin")"
install -m 0644 "$runtime_rule" "$runtime_target"
install -m 0644 "$repo_root/deploy/systemd/stockagent-pack-compute-spool-ssh-runtime.conf" "$runtime_dropin"
systemd-tmpfiles --create "$runtime_target"
install -m 0644 "${paths[@]}" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now stockagent-pack-compute-spool.timer
