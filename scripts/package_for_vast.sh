#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ -f "$PROJECT_DIR/scripts/runtime_env.sh" ]]; then
  # shellcheck disable=SC1091
  source "$PROJECT_DIR/scripts/runtime_env.sh"
fi

if [[ -z "${LOCAL_ENV_DIR:-}" ]]; then
  LOCAL_ENV_DIR="${FINTECH_ENV_PATH:-$(detect_fintech_env_path 2>/dev/null || true)}"
fi
REMOTE_USER="${REMOTE_USER:-root}"
REMOTE_HOST="${REMOTE_HOST:-}"
REMOTE_PORT="${REMOTE_PORT:-22}"
REMOTE_WORKSPACE="${REMOTE_WORKSPACE:-/workspace}"
REMOTE_PROJECT_DIR="${REMOTE_PROJECT_DIR:-}"
REMOTE_ENV_ARCHIVE="${REMOTE_ENV_ARCHIVE:-}"
REMOTE_ENV_DIR="${REMOTE_ENV_DIR:-}"
ARCHIVE_PATH="${ARCHIVE_PATH:-$PROJECT_DIR/fintech-env.tar.gz}"
RSYNC_DELETE="${RSYNC_DELETE:-0}"
RUN_REMOTE_SETUP="${RUN_REMOTE_SETUP:-1}"
ENV_TRANSFER_MODE="${ENV_TRANSFER_MODE:-pack}"

usage() {
  cat <<'EOF'
Usage: bash scripts/package_for_vast.sh --host <host> [options]

Packages the local fintech conda env with conda-pack, rsyncs the full stockAgent
directory to Vast, uploads the env archive, and optionally runs setup_vast.sh.

Options:
  --host <host>               Vast SSH host or IP
  --port <port>               Vast SSH port (default: 22)
  --user <user>               SSH user (default: root)
  --local-env-dir <path>      Local fintech env path
  --archive-path <path>       Local env archive path (default: ./fintech-env.tar.gz)
  --remote-workspace <path>   Vast workspace (default: /workspace)
  --remote-project-dir <path> Vast project dir (default: /workspace/stockAgent)
  --remote-env-archive <path> Vast env archive path (default: /workspace/fintech-env.tar.gz)
  --remote-env-dir <path>     Vast env dir (default: <remote-workspace>/fintech-env)
  --env-transfer <pack|rsync> Transfer env as conda-pack archive or direct rsync (default: pack)
  --no-remote-setup           Upload only; do not run setup_vast.sh
  --delete                    Mirror deletes to remote project dir during rsync
  -h, --help                  Show this help

Environment overrides:
  LOCAL_ENV_DIR, REMOTE_USER, REMOTE_HOST, REMOTE_PORT, REMOTE_WORKSPACE,
  REMOTE_PROJECT_DIR, REMOTE_ENV_ARCHIVE, REMOTE_ENV_DIR, ARCHIVE_PATH,
  RSYNC_DELETE, RUN_REMOTE_SETUP, ENV_TRANSFER_MODE

Example:
  bash scripts/package_for_vast.sh --host ssh5.vast.ai --port 12345
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      REMOTE_HOST="$2"
      shift 2
      ;;
    --port)
      REMOTE_PORT="$2"
      shift 2
      ;;
    --user)
      REMOTE_USER="$2"
      shift 2
      ;;
    --local-env-dir)
      LOCAL_ENV_DIR="$2"
      shift 2
      ;;
    --archive-path)
      ARCHIVE_PATH="$2"
      shift 2
      ;;
    --remote-workspace)
      REMOTE_WORKSPACE="$2"
      shift 2
      ;;
    --remote-project-dir)
      REMOTE_PROJECT_DIR="$2"
      shift 2
      ;;
    --remote-env-archive)
      REMOTE_ENV_ARCHIVE="$2"
      shift 2
      ;;
    --remote-env-dir)
      REMOTE_ENV_DIR="$2"
      shift 2
      ;;
    --env-transfer)
      ENV_TRANSFER_MODE="$2"
      shift 2
      ;;
    --no-remote-setup)
      RUN_REMOTE_SETUP="0"
      shift
      ;;
    --delete)
      RSYNC_DELETE="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$REMOTE_HOST" ]]; then
  echo "--host is required." >&2
  usage >&2
  exit 2
fi
if [[ -z "$LOCAL_ENV_DIR" ]]; then
  echo "Local fintech env was not discovered; set FINTECH_ENV_PATH or --local-env-dir." >&2
  exit 2
fi
if [[ ! -x "$LOCAL_ENV_DIR/bin/python" ]]; then
  echo "Local fintech env not found: $LOCAL_ENV_DIR" >&2
  exit 2
fi
if [[ "$ENV_TRANSFER_MODE" != "pack" && "$ENV_TRANSFER_MODE" != "rsync" ]]; then
  echo "--env-transfer must be 'pack' or 'rsync'." >&2
  exit 2
fi
REMOTE_PROJECT_DIR="${REMOTE_PROJECT_DIR:-$REMOTE_WORKSPACE/stockAgent}"
REMOTE_ENV_ARCHIVE="${REMOTE_ENV_ARCHIVE:-$REMOTE_WORKSPACE/fintech-env.tar.gz}"
REMOTE_ENV_DIR="${REMOTE_ENV_DIR:-$REMOTE_WORKSPACE/fintech-env}"

find_conda_pack() {
  if command -v conda-pack >/dev/null 2>&1; then
    command -v conda-pack
    return 0
  fi
  if [[ -x "$LOCAL_ENV_DIR/bin/conda-pack" ]]; then
    printf "%s\n" "$LOCAL_ENV_DIR/bin/conda-pack"
    return 0
  fi
  local candidate
  for candidate in \
    "$HOME/miniforge3/bin/conda-pack" \
    "$HOME/mambaforge/bin/conda-pack" \
    "$HOME/miniconda3/bin/conda-pack" \
    "$HOME/anaconda3/bin/conda-pack" \
    "/root/miniforge3/bin/conda-pack" \
    "/home/user/miniforge3/bin/conda-pack"; do
    if [[ -x "$candidate" ]]; then
      printf "%s\n" "$candidate"
      return 0
    fi
  done
  return 1
}

CONDA_PACK_BIN=""
if [[ "$ENV_TRANSFER_MODE" == "pack" ]]; then
  CONDA_PACK_BIN="$(find_conda_pack || true)"
fi
if [[ "$ENV_TRANSFER_MODE" == "pack" && -z "$CONDA_PACK_BIN" ]]; then
  echo "conda-pack is required. Install it first, for example:" >&2
  echo "  conda install -n base -c conda-forge conda-pack -y" >&2
  exit 2
fi

SSH_TARGET="${REMOTE_USER}@${REMOTE_HOST}"
SSH_CMD=(ssh -p "$REMOTE_PORT")
RSYNC_RSH="ssh -p $REMOTE_PORT"
RSYNC_ARGS=(-az --info=progress2)
if [[ "$RSYNC_DELETE" == "1" ]]; then
  RSYNC_ARGS+=(--delete)
fi

mkdir -p "$(dirname "$ARCHIVE_PATH")"

echo "[package-vast] project: $PROJECT_DIR"
echo "[package-vast] env: $LOCAL_ENV_DIR"

echo "[package-vast] creating remote dirs"
"${SSH_CMD[@]}" "$SSH_TARGET" "mkdir -p '$REMOTE_WORKSPACE' '$REMOTE_PROJECT_DIR' '$REMOTE_ENV_DIR'"

if [[ "$ENV_TRANSFER_MODE" == "pack" ]]; then
  echo "[package-vast] packing env -> $ARCHIVE_PATH"
  "$CONDA_PACK_BIN" -p "$LOCAL_ENV_DIR" -o "$ARCHIVE_PATH" --force --compress-level 6

  echo "[package-vast] uploading env archive -> $SSH_TARGET:$REMOTE_ENV_ARCHIVE"
  rsync "${RSYNC_ARGS[@]}" -e "$RSYNC_RSH" "$ARCHIVE_PATH" "$SSH_TARGET:$REMOTE_ENV_ARCHIVE"
else
  echo "[package-vast] syncing env directory -> $SSH_TARGET:$REMOTE_ENV_DIR/"
  rsync "${RSYNC_ARGS[@]}" -e "$RSYNC_RSH" "$LOCAL_ENV_DIR/" "$SSH_TARGET:$REMOTE_ENV_DIR/"
fi

echo "[package-vast] syncing full project -> $SSH_TARGET:$REMOTE_PROJECT_DIR/"
rsync "${RSYNC_ARGS[@]}" -e "$RSYNC_RSH" "$PROJECT_DIR/" "$SSH_TARGET:$REMOTE_PROJECT_DIR/"

if [[ "$RUN_REMOTE_SETUP" == "1" ]]; then
  echo "[package-vast] running remote setup"
  "${SSH_CMD[@]}" "$SSH_TARGET" \
    "cd '$REMOTE_PROJECT_DIR' && bash scripts/setup_vast.sh --project-dir '$REMOTE_PROJECT_DIR' --env-dir '$REMOTE_ENV_DIR' --env-archive '$REMOTE_ENV_ARCHIVE'"
fi

echo "[package-vast] done"
echo "[package-vast] remote project: $SSH_TARGET:$REMOTE_PROJECT_DIR"
echo "[package-vast] remote python: $REMOTE_ENV_DIR/bin/python"
