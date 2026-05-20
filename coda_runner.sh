#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

CONFIG_PATH="configs/experiment_baseline.yaml"
OUTPUT_DIR=""
REQUIRE_CUDA=""
PYTHON_BIN="$(command -v python3 || command -v python || true)"

usage() {
  cat <<'EOF'
Usage: ./coda_runner.sh [options] [-- <extra train.py args>]

Options:
  -c, --config <path>      Experiment config yaml (default: configs/experiment_baseline.yaml)
  --allow-cpu              Do not enforce CUDA check in runner
  -h, --help               Show this help

Examples:
  ./coda_runner.sh
  ./coda_runner.sh -c configs/experiment_baseline.yaml
  ./coda_runner.sh -- --help
  ./coda_runner.sh -- --some-extra-flag value
EOF
}

EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--config)
      CONFIG_PATH="$2"
      shift 2
      ;;
    --allow-cpu)
      REQUIRE_CUDA="0"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      EXTRA_ARGS=("$@")
      break
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config file not found: $CONFIG_PATH" >&2
  exit 2
fi

if [[ -z "$PYTHON_BIN" ]]; then
  echo "Neither python3 nor python found in PATH." >&2
  exit 2
fi

read -r ENV_NAME OUTPUT_DIR REQUIRE_CUDA < <("$PYTHON_BIN" - "$CONFIG_PATH" <<'PY'
import sys
import yaml

config_path = sys.argv[1]
with open(config_path, "r", encoding="utf-8") as f:
    raw = yaml.safe_load(f)
runner = raw.get("runner", {})
environment = raw.get("environment", {})
print(
    environment.get("conda_env", ""),
    runner.get("output_dir", "artifacts"),
    "1" if runner.get("require_cuda", True) else "0",
)
PY
)

if [[ -z "$ENV_NAME" ]]; then
  echo "Unable to resolve conda env name from $CONFIG_PATH." >&2
  exit 2
fi

mkdir -p "$OUTPUT_DIR"

MERGED_CONFIG_PATH="$OUTPUT_DIR/generated_config_$(date +%Y%m%d_%H%M%S).yaml"

cp "$CONFIG_PATH" "$MERGED_CONFIG_PATH"

if command -v mamba >/dev/null 2>&1; then
  RUNNER=(mamba run -n "$ENV_NAME")
elif command -v conda >/dev/null 2>&1; then
  RUNNER=(conda run -n "$ENV_NAME")
else
  echo "Neither mamba nor conda found in PATH." >&2
  exit 2
fi

echo "[runner] env=$ENV_NAME base_config=$CONFIG_PATH merged_config=$MERGED_CONFIG_PATH output=$OUTPUT_DIR require_cuda=$REQUIRE_CUDA"
"${RUNNER[@]}" python - "$MERGED_CONFIG_PATH" <<'PY'
import sys
import yaml

p = sys.argv[1]
with open(p, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

t = cfg.get("training", {})
print("[runner] effective training config: "
      f"epochs={t.get('epochs')} batch_size={t.get('batch_size')} "
      f"lr={t.get('learning_rate')} hidden_dim={t.get('hidden_dim')}")
PY

"${RUNNER[@]}" python - "$REQUIRE_CUDA" <<'PY'
import sys
import torch

print(f"[runner] torch={torch.__version__} torch_cuda={torch.version.cuda}")
print(f"[runner] cuda_available={torch.cuda.is_available()} device_count={torch.cuda.device_count()}")
require_cuda = sys.argv[1] == "1"
if require_cuda and not torch.cuda.is_available():
    sys.exit("CUDA is not available in this environment.")
PY

LOG_PATH="$OUTPUT_DIR/train_$(date +%Y%m%d_%H%M%S).log"
echo "[runner] log: $LOG_PATH"

set -o pipefail
"${RUNNER[@]}" python -u train.py \
  --config "$MERGED_CONFIG_PATH" \
  --output-dir "$OUTPUT_DIR" \
  "${EXTRA_ARGS[@]}" 2>&1 | tee "$LOG_PATH"

echo "[runner] done"
