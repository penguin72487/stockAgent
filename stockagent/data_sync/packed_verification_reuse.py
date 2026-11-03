"""Resume closed pack computations after fresh full encoded-byte verification.

The original activity owns these receipts. A complete cohort, the exact worker,
manifest, ordered original commitments and every stored CRC must agree. A new
full cold SHA and generation pass still precedes reuse. No partial cohort,
source retirement, NAS acknowledgement or moving head is admitted here.
"""
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time


def _read_control(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
                or before.st_mode & 0o022 or before.st_nlink != 1
                or not 0 < before.st_size <= 64 * 1024):
            raise ValueError('completed pack evidence is not bounded owned metadata')
        raw = os.read(descriptor, before.st_size + 1)
        after = os.fstat(descriptor)
        def signature(info):
            return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
                    info.st_ctime_ns, info.st_mode, info.st_uid, info.st_gid, info.st_nlink)
        if (signature(before) != signature(after) or signature(path.lstat()) != signature(before)
                or len(raw) != before.st_size):
            raise ValueError('completed pack evidence changed during inspection')
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError('completed pack evidence is not an object')
        return value, hashlib.sha256(raw).hexdigest()
    finally:
        os.close(descriptor)


def _check_directory(path):
    info = path.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022):
        raise ValueError('completed pack evidence directory is redirected or unowned')
    return info.st_dev, info.st_ino


def find_completed_pack_evidence(parent, resolved, options):
    """Discover only closed computations in this original recovery journal."""
    from stockagent.data_sync.packed_verification import SOURCE_SHA256
    packs = [item for item in resolved.manifest.get('archive', {}).get('objects', ())
             if item['kind'] == 'pack']
    if not packs or not parent.exists():
        return None
    _check_directory(parent)
    directories = [path for path in parent.iterdir()
                   if re.fullmatch('[0-9a-f]{32}', path.name) and not path.is_symlink()]
    for directory in sorted(directories, key=lambda path: path.stat().st_mtime, reverse=True):
        _check_directory(directory)
        if not (directory / 'summary.json').exists():
            continue  # Interrupted jobs are evidence, not a completed cohort.
        summary, _ = _read_control(directory / 'summary.json')
        if (summary.get('jobs') != len(packs) or summary.get('worker_sha256') != SOURCE_SHA256
                or summary.get('policy_sha256') != options.get('policy_sha256')
                or not isinstance(summary.get('source_read_execution'), dict)):
            continue
        first, _ = _read_control(directory / (packs[0]['sha256'] + '.json'))
        if first.get('manifest_sha256') == resolved.manifest_sha256:
            return directory
    return None


def reuse_pack_jobs(root, resolved, entries, options, *, native, progress=None):
    from downloader.artifact_io import atomic_write_json
    from stockagent.data_sync import packed_snapshots as packed
    from stockagent.data_sync import packed_verification as worker
    from stockagent.data_sync import packed_verification_sources as adapter

    source, destination = Path(options['completed_evidence']), Path(options['evidence_directory'])
    if source.parent != destination.parent or source == destination:
        raise ValueError('completed pack reuse must remain in the original recovery journal')
    identity = _check_directory(source)
    summary, summary_sha = _read_control(source / 'summary.json')
    packs = [item for item in resolved.manifest['archive']['objects'] if item['kind'] == 'pack']
    if not packs:
        raise ValueError('completed pack reuse requires original pack objects')
    names = {item['sha256'] + '.json' for item in packs}
    if (summary.get('contract') != worker.CONTRACT or summary.get('jobs') != len(packs)
            or summary.get('worker_sha256') != worker.SOURCE_SHA256
            or summary.get('policy_sha256') != options.get('policy_sha256')
            or summary.get('source_read_execution', {}).get('adapter_sha256') != adapter.SOURCE_SHA256
            or {path.name for path in source.iterdir()} - {'summary.json', 'remote-session.json'} != names):
        raise ValueError('completed pack cohort or implementation differs from original verification')
    proofs, proof_hashes = {}, {}
    started = time.monotonic()
    for item in packs:
        proof, proof_sha = _read_control(source / (item['sha256'] + '.json'))
        rows = [[row['path'], row['size'], row['sha256']] for row in entries.object_rows(item['sha256'])]
        expected = worker.expected_result(item, rows)
        if (any(proof.get(key) != value for key, value in expected.items())
                or proof.get('manifest_sha256') != resolved.manifest_sha256
                or proof.get('worker_sha256') != worker.SOURCE_SHA256
                or proof.get('stored_members_crc_verified') != int(item.get('stored_file_count', item['file_count']))
                or proof.get('acceptance') is not False or proof.get('source_retirement') is not False
                or proof.get('execution_node') not in {'local', 'vast'}):
            raise ValueError('completed pack proof differs from exact original-byte expectations')
        proofs[item['sha256']], proof_hashes[item['sha256']] = proof, proof_sha
    if dict(Counter(proof['execution_node'] for proof in proofs.values())) != {
            key: value for key, value in summary.get('nodes', {}).items() if value}:
        raise ValueError('completed pack node totals differ from original receipts')

    members = [(root / item['relpath'], item['bytes']) for item in packs]
    expected_sizes = dict(members)
    def generations(stage):
        observed = packed._collect_object_signatures(expected_sizes, progress=progress, stage=stage)
        return {path: generation(path, info) for path, info in observed.items()}
    def generation(path, info):
        if not stat.S_ISREG(info.st_mode):
            raise ValueError('retained pack is no longer an exact regular object')
        if info.st_size != expected_sizes[path]:
            raise ValueError('retained pack size differs from the original inventory')
        return packed._source_stat(info), info.st_nlink
    before = generations('capture-retained-pack-generations')
    expected_hashes = {root / item['relpath']: item['sha256'] for item in packs}
    checked, checked_bytes, batches = 0, 0, 0
    batches_to_hash = list(packed._native_blob_hash_batches(members))
    batches_to_hash.extend([path] for path, size in members
                          if size > packed.MAX_NATIVE_HASH_MEMBER_BYTES)
    total_bytes = sum(item['bytes'] for item in packs)
    for batch in batches_to_hash:
        if native and all(expected_sizes[path] <= packed.MAX_NATIVE_HASH_MEMBER_BYTES for path in batch):
            from stockagent.data_sync.windows_cold_io import hash_many
            hashes = hash_many(batch)
        else:
            hashes = {path: packed.sha256_file(path) for path in batch}
        if hashes != {path: expected_hashes[path] for path in batch}:
            raise ValueError('fresh complete pack SHA differs from retained original proof')
        checked += len(batch)
        checked_bytes += sum(expected_sizes[path] for path in batch)
        batches += 1
        if progress is not None:
            progress({'stage': 'revalidate-completed-pack-bytes', 'completed_items': checked,
                'total_items': len(packs), 'completed_bytes': checked_bytes,
                'total_bytes': total_bytes, 'acceptance': False})
    if (checked != len(packs) or before != generations('recheck-retained-pack-generations')
            or _check_directory(source) != identity
            or _read_control(source / 'summary.json')[1] != summary_sha
            or any(_read_control(source / (key + '.json'))[1] != digest
                   for key, digest in proof_hashes.items())):
        raise ValueError('completed pack generation or evidence changed during fresh verification')
    for module in (worker, adapter):
        if hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() != module.SOURCE_SHA256:
            raise ValueError('pack verification implementation changed during evidence reuse')
    if options.get('policy_path') and hashlib.sha256(Path(options['policy_path']).read_bytes()).hexdigest() != options['policy_sha256']:
        raise ValueError('pack verification policy changed during evidence reuse')
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    resumed = {'contract': worker.CONTRACT, 'jobs': len(packs), 'nodes': {'retained': len(packs)},
        'worker_sha256': worker.SOURCE_SHA256, 'policy_sha256': options.get('policy_sha256'),
        'manifest_sha256': resolved.manifest_sha256, 'complete_seconds': time.monotonic() - started,
        'completed_evidence': str(source), 'completed_summary_sha256': summary_sha,
        'original_nodes': summary['nodes'], 'fresh_full_encoded_sha_objects': checked,
        'fresh_full_encoded_sha_bytes': checked_bytes, 'native_hash_batches': batches,
        'original_ordered_sha_and_all_crc_reused': True, 'remote_payload_files_created': 0,
        'acceptance': False, 'source_retirement': False, 'evidence_directory': str(destination)}
    atomic_write_json(destination / 'summary.json', resumed, durable=True)
    return proofs, resumed
