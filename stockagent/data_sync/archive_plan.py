"""Public, bounded policy for the existing archive owners; never a work queue."""
from __future__ import annotations

from stockagent.data_sync import archive_schedule

CONTRACT = 'stockagent.storage_archive_plan.v1'
LOCAL_STAGES = frozenset({'publish', 'd_old_to_e', 'repack', 'd_new', 'old_gc', 'daily_merge', 'retire'})
REPLICATION_STAGES = frozenset({'send', 'nas', 'transport_gc'})


def from_config(columnar: dict, placement: dict, *, pack_target_bytes: int) -> dict | None:
    """Read declared behavior without treating configuration as completed work."""
    lifecycle = columnar.get('dataset_version_lifecycle', {})
    source = columnar.get('source_versioning', {})
    if not isinstance(lifecycle, dict) or not isinstance(source, dict) or not isinstance(placement, dict):
        return None
    if lifecycle.get('enabled') is not True:
        return None
    window = archive_schedule.validate_window(lifecycle.get('daily_merge_window'))
    if window is None:
        return None
    policy = {'contract': CONTRACT, 'daily_merge_window': window,
        'ordinary_history_retention': lifecycle.get('ordinary_history_retention'),
        'migration_catchup': lifecycle.get('ordinary_history_migration_catchup'),
        'logical_version': source.get('logical_version'),
        'physical_version': source.get('physical_version'),
        'file_chunk_bytes': source.get('file_chunk_bytes'),
        'dataset_pack_target_bytes': pack_target_bytes,
        'general_raw_retirement_enabled': (source.get('private_history', {}).get('source_retirement')
            if isinstance(source.get('private_history', {}), dict) else None),
        'maximum_transfer_wave_bytes': placement.get('maximum_wave_bytes'),
        'placement_enabled': placement.get('enabled') is True}
    return public_policy(policy)


def public_policy(value: object) -> dict | None:
    """Discard every path, source identity and arbitrary string at the gateway."""
    if not isinstance(value, dict) or value.get('contract') != CONTRACT:
        return None
    try:
        window = archive_schedule.validate_window(value.get('daily_merge_window'))
    except (TypeError, ValueError, KeyError):
        return None
    if window is None or window['timezone'] != 'Asia/Taipei':
        return None
    def size(key):
        number = value.get(key)
        return number if type(number) is int and 0 < number <= 16*1024**3 else None
    def boolean(key):
        number = value.get(key)
        return number if type(number) is bool else None
    return {'contract': CONTRACT, 'daily_merge_window': window,
        'ordinary_history_retention': 'current-and-research-pins'
            if value.get('ordinary_history_retention') == 'current-and-research-pins' else None,
        'migration_catchup': boolean('migration_catchup'),
        'logical_version': 'complete_inventory_not_delta_chain'
            if value.get('logical_version') == 'complete_inventory_not_delta_chain' else None,
        'physical_version': 'reuse_immutable_parts_and_sha256_chunks'
            if value.get('physical_version') == 'reuse_immutable_parts_and_sha256_chunks' else None,
        'file_chunk_bytes': size('file_chunk_bytes'),
        'dataset_pack_target_bytes': size('dataset_pack_target_bytes'),
        'maximum_transfer_wave_bytes': size('maximum_transfer_wave_bytes'),
        'general_raw_retirement_enabled': boolean('general_raw_retirement_enabled'),
        'placement_enabled': boolean('placement_enabled')}


def branch(identity: str) -> str:
    if identity in LOCAL_STAGES:
        return 'local_migration'
    if identity in REPLICATION_STAGES:
        return 'nas_backup'
    return 'independent_verification' if identity == 'recover' else 'source_workspace'
