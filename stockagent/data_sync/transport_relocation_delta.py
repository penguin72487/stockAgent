"""Preserve differing fixed D transport members as one verified E archive.

This never changes the live sender, original D names, the cold authority or NAS.
The immutable manifest describes exactly which historical names are preserved.
"""
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import sqlite3
import stat
import subprocess
import tarfile
import time
import uuid

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync.workspace_relocation import digest, signature

CONTRACT = 'stockagent.transport_relocation_delta.v1'


def _validate_native_source(root, intent):
    """Bind the exact renamed root to the enrolled physical D volume."""
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    from stockagent.data_sync.transport_placement import D_BACKING
    expected_root = Path('/mnt/d') / (D_BACKING.name + '.relocated-to-E-v1')
    if root != expected_root or root.is_symlink() or root.resolve() != root:
        raise ValueError('unexpected or redirected D relocation root')
    _check_d_primary_mount(Path('/srv/stockagent-packed'))
    records = json.loads(subprocess.check_output(
        ['findmnt', '-J', '-T', '/mnt/d', '-o', 'TARGET,SOURCE,FSTYPE'], text=True))['filesystems']
    if len(records) != 1 or records[0].get('source') != 'D:\\' or records[0].get('fstype') != '9p':
        raise ValueError('native source is not the enrolled D volume')
    if ((Path('/mnt/d/stockagent-cold-primary/packed/.stockagent-d-primary').read_bytes()
         != Path('/srv/stockagent-packed/.stockagent-d-primary').read_bytes())):
        raise ValueError('D volume markers differ')
    observed, expected = signature(root), intent['source_root_signature']
    if (observed[1], stat.S_IFMT(observed[2]), observed[6], observed[7]) != (
            expected[1], stat.S_IFMT(expected[2]), expected[6], expected[7]):
        raise ValueError('fixed D source root identity differs')


class _HashingReader:
    def __init__(self, stream):
        self.stream, self.sha, self.count = stream, hashlib.sha256(), 0

    def read(self, length=-1):
        block = self.stream.read(length)
        self.sha.update(block)
        self.count += len(block)
        return block


def _same_native_file_generation(observed, expected):
    """Two 9p mounts of enrolled NTFS may project different mode bits."""
    if len(observed) != 9 or len(expected) != 9:
        return False
    return (stat.S_ISREG(observed[2]) and stat.S_ISREG(expected[2])
            and all(observed[index] == expected[index] for index in (1, 3, 4, 5, 6, 7, 8)))


def _verify_archive(path, manifest_bytes, records):
    expected = {record['archive_name']: record for record in records}
    seen = set()
    with tarfile.open(path, mode='r|', bufsize=4 * 1024**2) as archive:
        for member in archive:
            if member.name in seen or not member.isfile():
                raise ValueError('duplicate or non-regular delta archive member')
            seen.add(member.name)
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError('delta archive member unavailable')
            if member.name == 'manifest.json':
                if stream.read() != manifest_bytes:
                    raise ValueError('delta archive manifest differs')
                continue
            record = expected.get(member.name)
            if not record or member.size != record['bytes']:
                raise ValueError('unexpected delta archive member')
            h, count = hashlib.sha256(), 0
            for block in iter(lambda: stream.read(4 * 1024**2), b''):
                h.update(block)
                count += len(block)
            if count != record['bytes'] or h.hexdigest() != record['sha256']:
                raise ValueError('delta archive independent readback differs')
    if seen != set(expected) | {'manifest.json'}:
        raise ValueError('delta archive member set differs')


def preserve_differences(evidence: Path, source: Path, output_dir: Path, *, admit):
    intent = json.loads((evidence/'intent.json').read_bytes())
    comparison = json.loads((evidence/'comparison-status.json').read_bytes())
    if (comparison.get('contract') != 'stockagent.transport_relocation_comparison.v1'
            or comparison.get('state') != 'completed'
            or comparison.get('files_completed') != intent['files']
            or comparison.get('logical_bytes_completed') != intent['logical_bytes']):
        raise ValueError('fixed comparison is not complete')
    identity = comparison.get('identity_sha256')
    if not isinstance(identity, str) or len(identity) != 64 or any(c not in '0123456789abcdef' for c in identity):
        raise ValueError('invalid comparison identity')
    _validate_native_source(source, intent)
    original = sqlite3.connect(f'file:{evidence / "files.sqlite"}?mode=ro', uri=True)
    compared = sqlite3.connect(f'file:{evidence / "comparison.sqlite"}?mode=ro', uri=True)
    try:
        bound = compared.execute('SELECT identity FROM metadata').fetchone()
        if not bound or bound[0] != identity:
            raise ValueError('comparison index identity differs')
        if compared.execute('SELECT count(*) FROM members WHERE outcome IS NULL').fetchone()[0]:
            raise ValueError('comparison has unfinished members')
        delta = compared.execute("SELECT path,sha,bytes,outcome FROM members WHERE outcome!='same' ORDER BY path").fetchall()
        records = []
        for relative, sha, size, outcome in delta:
            original_row = original.execute("SELECT sha,bytes,sig,state FROM files WHERE path=? AND kind='file'", (relative,)).fetchone()
            parts = PurePosixPath(relative)
            if (not original_row or original_row[0] != sha or original_row[1] != size
                    or original_row[3] != 'verified' or parts.is_absolute() or not parts.parts
                    or '..' in parts.parts):
                raise ValueError('delta differs from original verified inventory')
            records.append({'original_path': relative, 'archive_name': 'members/' + hashlib.sha256(
                relative.encode('utf-8', 'surrogateescape')).hexdigest(), 'sha256': sha,
                'bytes': size, 'source_signature': json.loads(original_row[2]), 'comparison': outcome})
    finally:
        original.close()
        compared.close()
    if len({r['archive_name'] for r in records}) != len(records):
        raise ValueError('delta archive path identity collision')
    manifest = {'contract': CONTRACT, 'comparison_identity_sha256': identity, 'members': records,
                'source_names_deleted': 0, 'retirement_authorized': False, 'NAS_acceptance_claimed': False}
    manifest_bytes = (json.dumps(manifest, ensure_ascii=True, sort_keys=True, separators=(',', ':'))+'\n').encode()
    total = sum(r['bytes'] for r in records)
    admit(total + max(64 * 1024**2, len(records) * 8192))
    output_dir.mkdir(parents=True, exist_ok=True)
    if output_dir.is_symlink() or output_dir.resolve() != output_dir.absolute():
        raise ValueError('redirected E delta destination')
    final = output_dir / (identity + '.tar')
    receipt_path = evidence / 'delta-receipt.json'
    if receipt_path.exists():
        saved = json.loads(receipt_path.read_bytes())
        if (saved.get('contract') == CONTRACT and saved.get('comparison_identity_sha256') == identity
                and saved.get('state') == 'verified' and saved.get('files') == len(records)
                and saved.get('logical_bytes') == total
                and saved.get('archive_signature') == signature(final)):
            return saved
        raise ValueError('existing delta receipt or archive changed')
    start = time.monotonic()
    def progress(state, completed, completed_bytes):
        atomic_write_json(evidence/'delta-status.json', {'contract': CONTRACT, 'state': state,
            'comparison_identity_sha256': identity, 'files_total': len(records), 'files_completed': completed,
            'logical_bytes_total': total, 'logical_bytes_completed': completed_bytes,
            'observed_at_epoch': time.time(), 'source_names_deleted': 0,
            'retirement_authorized': False, 'NAS_acceptance_claimed': False})
    progress('copying', 0, 0)
    if not final.exists():
        temporary = output_dir / ('.' + identity + '.' + uuid.uuid4().hex + '.partial')
        count = 0
        with temporary.open('xb') as output:
            os.chmod(temporary, 0o600)
            with tarfile.open(fileobj=output, mode='w|', bufsize=4 * 1024**2,
                              copybufsize=4 * 1024**2) as archive:
                header = tarfile.TarInfo('manifest.json')
                header.size = len(manifest_bytes)
                header.mode = 0o600
                archive.addfile(header, io.BytesIO(manifest_bytes))
                for number, record in enumerate(records, 1):
                    path = source.joinpath(*PurePosixPath(record['original_path']).parts)
                    if path.resolve() != path or path.is_symlink():
                        raise ValueError('redirected D delta member')
                    expected = record['source_signature']
                    if not _same_native_file_generation(signature(path), expected):
                        raise ValueError('D delta source generation changed')
                    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                    with os.fdopen(fd, 'rb') as stream:
                        before = os.fstat(stream.fileno())
                        reader = _HashingReader(stream)
                        header = tarfile.TarInfo(record['archive_name'])
                        header.size, header.mode = record['bytes'], 0o600
                        archive.addfile(header, reader)
                        after = os.fstat(stream.fileno())
                    before_fields = (before.st_dev, before.st_ino, before.st_mode, before.st_size,
                                     before.st_mtime_ns, before.st_ctime_ns, before.st_uid, before.st_gid, before.st_nlink)
                    after_fields = (after.st_dev, after.st_ino, after.st_mode, after.st_size,
                                    after.st_mtime_ns, after.st_ctime_ns, after.st_uid, after.st_gid, after.st_nlink)
                    if (reader.count != record['bytes'] or reader.sha.hexdigest() != record['sha256']
                            or before_fields != after_fields
                            or not _same_native_file_generation(signature(path), expected)):
                        raise ValueError('D delta source changed or original SHA differs')
                    count += record['bytes']
                    if number % 16 == 0:
                        progress('copying', number, count)
            output.flush()
            os.fsync(output.fileno())
        progress('readback', 0, 0)
        _verify_archive(temporary, manifest_bytes, records)
        archive_sha = digest(temporary)
        os.link(temporary, final, follow_symlinks=False)
        temporary.unlink()
        descriptor = os.open(output_dir, os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    else:
        # A crash after atomic installation is recoverable only by a full
        # independent member readback bound to the same fixed manifest.
        _verify_archive(final, manifest_bytes, records)
        archive_sha = digest(final)
    saved = {'contract': CONTRACT, 'state': 'verified', 'comparison_identity_sha256': identity,
             'archive': str(final), 'archive_sha256': archive_sha, 'archive_signature': signature(final),
             'manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(), 'files': len(records),
             'logical_bytes': total, 'observed_at_epoch': time.time(),
             'elapsed_seconds': time.monotonic()-start, 'source_names_deleted': 0,
             'retirement_authorized': False, 'NAS_acceptance_claimed': False}
    atomic_write_json(receipt_path, saved)
    progress('verified', len(records), total)
    return saved
