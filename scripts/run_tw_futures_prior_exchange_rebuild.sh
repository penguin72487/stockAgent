#!/usr/bin/env bash
# Rebuild one pinned all-TWD research candidate with corrected prior-session
# liquidity. This prepares source-bound data only; it does not publish a
# release or launch a model.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  printf 'Usage: %s FROZEN_CODE_RECEIPT NEW_OUTPUT_DIRECTORY\n' "$0" >&2
  exit 2
fi
receipt="$1"
output="$2"
workspace="${STOCKAGENT_WORKSPACE:-$(git rev-parse --show-toplevel)}"
cd "$workspace"
if [[ -z "${FINTECH_ENV_PATH:-}" && -f /etc/supervisor/conf.d/stockagent-futures-halt345-v9-complete.conf ]]; then
  FINTECH_ENV_PATH="$(sed -n 's/^environment=FINTECH_ENV_PATH="\([^"]*\)".*/\1/p' /etc/supervisor/conf.d/stockagent-futures-halt345-v9-complete.conf)"
  [[ -n "$FINTECH_ENV_PATH" ]] || { printf '%s\n' 'Existing futures runtime selection is missing' >&2; exit 1; }
  export FINTECH_ENV_PATH
fi
code="$(dirname "$receipt")/build-source"
source "$code/scripts/runtime_env.sh"
export PYTHONPATH="$code" PYTHONDONTWRITEBYTECODE=1 POLARS_MAX_THREADS=4 OMP_NUM_THREADS=4
run_fintech_python - "$receipt" "$output" <<'PY'
import json, shutil, sys
from pathlib import Path
receipt, output = map(Path, sys.argv[1:])
code = receipt.parent / 'build-source'
sys.path.insert(0, str(code.resolve()))
from downloader.artifact_io import sha256_file
from stockagent.runtime_identity import verify_release_bundles, verify_source_release

root = Path.cwd()
identity = verify_source_release(receipt, code)
verify_release_bundles(receipt)
release = json.loads(receipt.read_text())
overlay = release.get('source_overlay', {})
expected = {'scripts/build_project_release.py', 'scripts/prepare_tw_futures_margin_training.py',
            'stockagent/data/tw_futures_margin_release.py'}
if (overlay.get('base_source_sha256') != '3b31101d252c96a513972fbf6f0afbb9fd5db5d9bdd0538384056b4cd8132261'
        or overlay.get('unselected_workspace_changes_promoted') is not False
        or set(overlay.get('overrides', {})) != expected):
    raise ValueError('candidate code is not the expected explicitly scoped overlay')
proof_path = root / 'artifacts/markets/tw_futures_universe765_recovery_20261027_v1/dl_early2011_v12_halt_scoped_replay_v3.json'
proof = json.loads(proof_path.read_bytes())
policy = root / 'data_tw_index_futures/preparation_sources/universe765_repair_20261027_v1/valuation/sources/valuation_research_policy_dl_early2011_halt_v12_rebind_v1.json'
if (proof['contract'] != 'bounded_dl_early2011_v12_suspension_carry_scoped_replay_v3'
        or proof['source_release']['source_sha256'] != overlay['base_source_sha256']
        or proof['policy_identity']['valuation_research_policy_sha256'] != sha256_file(policy)
        or len(proof['target_status']) != 3
        or any(x['has_blocker'] or x['lost_inventory_continuation'] for x in proof['target_status'])
        or proof['research_valuation_rows'] != 36 or proof['lost_selected_rows'] != 0
        or proof['native_observations_and_execution_masks_unchanged'] is not True
        or proof['terminal_cash_conversions_unchanged'] is not True
        or proof['actual_official_halt_settlements_recovered'] is not False
        or proof['financial_or_training_adopted'] is not False):
    raise ValueError('bounded suspension candidate proof failed')
for name, digest in proof['inputs'].items():
    if sha256_file(root / name) != digest:
        raise ValueError('candidate source changed: ' + name)
if output.exists():
    raise FileExistsError('a financial generation may not overwrite an existing output')
if shutil.disk_usage(output.parent).free < 25 * 1024**3:
    raise RuntimeError('less than 25 GiB remains for financial preparation')
print(json.dumps(dict(stage='candidate_preflight_passed', code=identity,
    proof_sha256=sha256_file(proof_path), policy_sha256=sha256_file(policy),
    output=str(output), publication_or_training_started=False)), flush=True)
PY
exec_fintech_python "$code/scripts/prepare_tw_futures_margin_training.py" \
  --sources "$workspace/data_tw_index_futures/preparation_sources/margin_sources" \
  --output "$output" --start 2011-01-03 --end 2026-09-24 --slots 2816 \
  --universe-scope all_twd --publication-asset-scope all_twd \
  --integrate-rule-delta "$workspace/data_tw_index_futures/preparation_sources/universe765_repair_20261027_v1/ordinary_rule_margin_delta_v12_ewf_reference_rebind" \
  --valuation-research-policy "$workspace/data_tw_index_futures/preparation_sources/universe765_repair_20261027_v1/valuation/sources/valuation_research_policy_dl_early2011_halt_v12_rebind_v1.json" \
  --reuse-official-evidence-cache "$workspace/data_tw_index_futures/training_prepared/tw_futures_universe765_20261027_v1/financial_all_twd_v8/official_daily" \
  --official-spread-research-policy "$workspace/data_tw_index_futures/preparation_sources/universe765_repair_20261027_v1/mtx_spread_research_policy_20131225_v1.json" \
  --omit-empty-account-prefixes --allow-component-history-before-start --prepare-only
