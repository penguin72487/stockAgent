#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ssh_target="${1:-}"
ssh_port="${2:-}"
identity_file="${3:-}"
include_root="${4:-}"

if (( EUID != 0 )); then
  echo "Root privileges are required." >&2
  exit 2
fi
if [[ ! "$ssh_target" =~ ^[A-Za-z0-9_.@:-]+$ || "$ssh_target" == -* ]]; then
  echo "Usage: $0 SSH_TARGET SSH_PORT IDENTITY_FILE ARTIFACT_RELATIVE_ROOT" >&2
  exit 2
fi
if [[ ! "$ssh_port" =~ ^[0-9]+$ ]] || (( ssh_port < 1 || ssh_port > 65535 )); then
  echo "SSH_PORT must be between 1 and 65535." >&2
  exit 2
fi
if [[ ! -f "$identity_file" || -L "$identity_file" ]]; then
  echo "IDENTITY_FILE must be a regular file." >&2
  exit 2
fi
if [[ -z "$include_root" || "$include_root" == /* || "$include_root" == *..* ]]; then
  echo "ARTIFACT_RELATIVE_ROOT must be a safe relative path." >&2
  exit 2
fi

chmod 0600 "$identity_file"
chmod 0755 \
  "$repo_root/scripts/ingest_remote_cold_artifacts.py" \
  "$repo_root/scripts/run_remote_cold_artifact_ingress.sh" \
  "$repo_root/scripts/install_remote_cold_artifact_ingress.sh"

temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT
install -d -m 0755 /etc/stockagent
{
  printf 'COLD_ARTIFACT_INGRESS_SSH_TARGET=%q\n' "$ssh_target"
  printf 'COLD_ARTIFACT_INGRESS_SSH_PORT=%q\n' "$ssh_port"
  printf 'COLD_ARTIFACT_INGRESS_IDENTITY_FILE=%q\n' "$identity_file"
  printf 'COLD_ARTIFACT_INGRESS_ORIGIN_NODE_ID=%q\n' "vastai1T"
  printf 'COLD_ARTIFACT_INGRESS_REMOTE_REPO_ROOT=%q\n' "/root/stockAgent"
  printf 'COLD_ARTIFACT_INGRESS_REMOTE_ARTIFACT_ROOT=%q\n' "/root/stockAgent/artifacts"
  printf 'COLD_ARTIFACT_INGRESS_INCLUDE_ROOTS=%q\n' "$include_root"
  printf 'COLD_ARTIFACT_INGRESS_SCOPE=%q\n' "ablations"
  printf 'COLD_ARTIFACT_INGRESS_STABLE_HOURS=%q\n' "0"
  printf 'COLD_ARTIFACT_INGRESS_MAX_PUBLISH=%q\n' "1"
} >"$temporary_dir/remote-cold-artifact-ingress.env"
install -m 0600 \
  "$temporary_dir/remote-cold-artifact-ingress.env" \
  /etc/stockagent/remote-cold-artifact-ingress.env

escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in \
  stockagent-remote-cold-artifact-ingress.service \
  stockagent-remote-cold-artifact-ingress.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" \
    "$repo_root/deploy/systemd/${unit}.in" >"$temporary_dir/$unit"
done
systemd-analyze verify \
  "$temporary_dir/stockagent-remote-cold-artifact-ingress.service" \
  "$temporary_dir/stockagent-remote-cold-artifact-ingress.timer"
install -m 0644 "$temporary_dir"/*.service "$temporary_dir"/*.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now stockagent-remote-cold-artifact-ingress.timer
systemctl start stockagent-remote-cold-artifact-ingress.service

echo "Installed allowlisted remote cold-artifact ingress for $include_root"
