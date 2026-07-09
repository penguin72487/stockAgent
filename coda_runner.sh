#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [[ -f "$ROOT_DIR/scripts/runtime_env.sh" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/scripts/runtime_env.sh"
  prepend_fintech_path
fi

CONFIG_PATH="configs/experiment_baseline.yaml"
OUTPUT_DIR=""
REQUIRE_CUDA=""
PYTHON_BIN="${PYTHON_BIN:-$(resolve_fintech_python 2>/dev/null || true)}"

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
from stockagent.config import load_config

config = load_config(sys.argv[1])
print(
    config.environment.conda_env,
    config.runner.output_dir,
    "1" if config.runner.require_cuda else "0",
)
PY
)

if [[ -z "$ENV_NAME" ]]; then
  echo "Unable to resolve conda env name from $CONFIG_PATH." >&2
  exit 2
fi

mkdir -p "$OUTPUT_DIR"

MERGED_CONFIG_PATH="$OUTPUT_DIR/generated_config_$(date +%Y%m%d_%H%M%S).yaml"

"$PYTHON_BIN" - "$CONFIG_PATH" "$MERGED_CONFIG_PATH" <<'PY'
import sys
import yaml
from stockagent.config import _load_raw_config

raw = _load_raw_config(sys.argv[1])
with open(sys.argv[2], "w", encoding="utf-8") as handle:
    yaml.safe_dump(raw, handle, sort_keys=False, allow_unicode=True)
PY

if [[ "$ENV_NAME" == "fintech" && -x "$FINTECH_ENV_PATH/bin/python" ]]; then
  PY_RUNNER=("$FINTECH_ENV_PATH/bin/python")
elif [[ -x "$FINTECH_MAMBA_BIN" ]]; then
  PY_RUNNER=("$FINTECH_MAMBA_BIN" run -n "$ENV_NAME" python)
elif command -v mamba >/dev/null 2>&1; then
  PY_RUNNER=(mamba run -n "$ENV_NAME" python)
elif command -v conda >/dev/null 2>&1; then
  PY_RUNNER=(conda run -n "$ENV_NAME" python)
else
  echo "Neither mamba nor conda found in PATH." >&2
  exit 2
fi

echo "[runner] env=$ENV_NAME base_config=$CONFIG_PATH merged_config=$MERGED_CONFIG_PATH output=$OUTPUT_DIR require_cuda=$REQUIRE_CUDA"
"${PY_RUNNER[@]}" - "$MERGED_CONFIG_PATH" <<'PY'
import sys
from stockagent.config import load_config

cfg = load_config(sys.argv[1])

print("[runner] effective training config: "
      f"epochs={cfg.training.epochs} batch_size={cfg.training.batch_size} "
      f"lr={cfg.training.learning_rate} model={cfg.training.model_name}")
PY

"${PY_RUNNER[@]}" - "$REQUIRE_CUDA" <<'PY'
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
"${PY_RUNNER[@]}" -u train.py \
  --config "$MERGED_CONFIG_PATH" \
  --output-dir "$OUTPUT_DIR" \
  "${EXTRA_ARGS[@]}" 2>&1 | tee "$LOG_PATH"

echo "[runner] done"
