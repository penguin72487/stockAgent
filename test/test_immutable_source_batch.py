"""Batch durability never changes payloads, ownership or publication gates."""
from pathlib import Path
import os
import stat
import subprocess
import sys

import pytest

from downloader.artifact_io import sha256_file
from stockagent.data_sync.artifact_dedup import ImmutableSourceBatch, link_immutable_source


def source_case(tmp_path):
    source = tmp_path/'producer.bin'
    source.write_bytes(b'original literal values: 99.50, (null), -0.0')
    return source, tmp_path/'objects', tmp_path/'output'/'nested', sha256_file(source)


def test_batch_keeps_producer_independent_and_all_aliases_byte_exact(tmp_path):
    source, objects, output, digest = source_case(tmp_path)
    with ImmutableSourceBatch() as batch:
        for name in ['one.raw','two.raw']:
            link_immutable_source(source,output/name,objects,digest,batch=batch)
    first, second = output/'one.raw', output/'two.raw'
    assert first.read_bytes() == second.read_bytes() == source.read_bytes()
    assert first.stat().st_ino == second.stat().st_ino
    assert first.stat().st_ino != source.stat().st_ino
    assert stat.S_IMODE(first.stat().st_mode) == 0o444
    source.write_bytes(b'new mutable producer values')
    assert sha256_file(first) == sha256_file(second) == digest


def test_directory_sync_is_coalesced_and_payload_sync_precedes_success(tmp_path,monkeypatch):
    source, objects, output, digest = source_case(tmp_path)
    original = os.fsync
    directories, payloads = [], []

    def observed(fd):
        info = os.fstat(fd)
        (directories if stat.S_ISDIR(info.st_mode) else payloads).append((info.st_dev,info.st_ino))
        original(fd)

    monkeypatch.setattr(os,'fsync',observed)
    with ImmutableSourceBatch() as batch:
        for index in range(20):
            link_immutable_source(source,output/str(index),objects,digest,batch=batch)
        assert not directories and len(payloads) == 1
    assert directories and len(directories) == len(set(directories))
    assert (output.stat().st_dev,output.stat().st_ino) in directories
    assert (tmp_path.stat().st_dev,tmp_path.stat().st_ino) in directories


def test_body_failure_preserves_partial_aliases_and_does_not_publish_completion(tmp_path):
    source, objects, output, digest = source_case(tmp_path)
    marker = tmp_path/'complete.json'
    with pytest.raises(RuntimeError,match='source failure'):
        with ImmutableSourceBatch() as batch:
            link_immutable_source(source,output/'part',objects,digest,batch=batch)
            raise RuntimeError('source failure')
        marker.write_text('complete')
    assert not marker.exists()
    assert sha256_file(source) == sha256_file(output/'part') == digest


def test_directory_sync_failure_rejects_publication_and_existing_alias_can_retry(tmp_path,monkeypatch):
    source, objects, output, digest = source_case(tmp_path)
    original = os.fsync

    def fail_directory(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError('directory sync rejected')
        original(fd)

    monkeypatch.setattr(os,'fsync',fail_directory)
    with pytest.raises(OSError,match='sync rejected'):
        with ImmutableSourceBatch() as batch:
            link_immutable_source(source,output/'part',objects,digest,batch=batch)
    assert sha256_file(source) == digest
    monkeypatch.setattr(os,'fsync',original)
    with ImmutableSourceBatch() as batch:
        link_immutable_source(source,output/'part',objects,digest,batch=batch)
    assert sha256_file(output/'part') == digest


def test_replaced_directory_cannot_be_published_under_old_identity(tmp_path):
    source, objects, output, digest = source_case(tmp_path)
    with pytest.raises(ValueError,match='identity changed'):
        with ImmutableSourceBatch() as batch:
            link_immutable_source(source,output/'part',objects,digest,batch=batch)
            output.rename(output.with_name('preserved-original'))
            output.mkdir()
    assert sha256_file(source) == digest


@pytest.mark.parametrize('phase',['new','closed'])
def test_inactive_batch_is_rejected_before_creating_any_output(tmp_path,phase):
    source, objects, output, digest = source_case(tmp_path)
    batch = ImmutableSourceBatch()
    if phase == 'closed':
        with batch:
            pass
    with pytest.raises(ValueError,match='not active'):
        link_immutable_source(source,output/'part',objects,digest,batch=batch)
    assert not output.exists() and not objects.exists()


def test_corrupt_object_does_not_adopt_mutable_producer_or_publish_alias(tmp_path):
    source, objects, output, digest = source_case(tmp_path)
    bad = objects/digest[:2]/digest
    bad.parent.mkdir(parents=True)
    bad.write_bytes(b'corrupt')
    bad.chmod(0o444)
    with pytest.raises(ValueError,match='hash/type mismatch'):
        with ImmutableSourceBatch() as batch:
            link_immutable_source(source,output/'part',objects,digest,batch=batch)
    assert sha256_file(source) == digest and not (output/'part').exists()


def test_process_loss_keeps_original_and_fresh_process_can_verify_and_finish(tmp_path):
    source, objects, output, digest = source_case(tmp_path)
    code = '''
import os,sys
from pathlib import Path
from stockagent.data_sync.artifact_dedup import ImmutableSourceBatch,link_immutable_source
source,objects,output,digest=sys.argv[1:]
with ImmutableSourceBatch() as batch:
    link_immutable_source(Path(source),Path(output)/'part',Path(objects),digest,batch=batch)
    os._exit(19)
'''
    result = subprocess.run([sys.executable,'-c',code,str(source),str(objects),str(output),digest],check=False)
    assert result.returncode == 19 and sha256_file(source) == digest
    with ImmutableSourceBatch() as batch:
        link_immutable_source(source,output/'part',objects,digest,batch=batch)
    assert sha256_file(output/'part') == digest
