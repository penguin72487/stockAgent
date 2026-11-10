"""Explicitly reviewed offline replay scopes; this module has no heavy imports."""
import re

CONTRACT = 'stockagent.offline_replay_pair_migration.v1'
ENROLLED = {
    'offline-replay-multibasis22': 'tw_day_trade_multi_basis_22_replay.h0NCLb',
    'offline-replay-multibasis22-final': 'tw_day_trade_multi_basis_22_replay_final.ffvbbw',
    'offline-replay-multibasis22-postclose': 'tw_day_trade_multi_basis_22_postclose.493dcrv5',
}


def completed(value: dict, dataset: str) -> bool:
    """A no-op hint, never an authorization to delete a source or a D object."""
    retirement = value.get('C_retirement') if isinstance(value, dict) else None
    cleanup = value.get('E_archive_cleanup') if isinstance(value, dict) else None
    return (isinstance(value, dict) and value.get('contract') == CONTRACT and value.get('dataset') == dataset
            and value.get('state') == 'completed' and value.get('D_archive_committed') is True
            and isinstance(value.get('D_manifest_sha256'), str)
            and re.fullmatch('[0-9a-f]{64}', value['D_manifest_sha256']) is not None
            and value.get('C_payload_retired') is True and isinstance(retirement, dict)
            and retirement.get('state') == 'paired_payload_retired' and isinstance(cleanup, dict)
            and cleanup.get('state') == 'completed')
