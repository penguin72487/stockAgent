import gzip
import hashlib
import os
from pathlib import Path
import pytest

from stockagent.workflow_snapshot_streams import PinnedSnapshotStreams


def fixture(tmp_path):
    output = tmp_path / 'observation'
    folder = output / 'evidence'
    folder.mkdir(parents=True)
    raw = b'full source bytes\x00\xff' * 200000
    name = hashlib.sha256(raw).hexdigest() + '.gz'
    path = folder / name
    path.write_bytes(gzip.compress(raw, mtime=0))

    def local(value):
        candidate = output / value
        if not candidate.resolve().is_relative_to(output):
            raise ValueError('evidence path escapes observation')
        return candidate

    return output, path, raw, local


def test_streaming_keeps_complete_bytes_and_bounded_read_requests(tmp_path):
    output, path, raw, local = fixture(tmp_path)
    actual = hashlib.sha256()
    with PinnedSnapshotStreams(output, local) as reader:
        for _ in range(2):
            count = 0
            actual = hashlib.sha256()
            with reader.open('evidence/' + path.name) as stream:
                while block := stream.read(4096):
                    assert len(block) <= 4096
                    count += len(block)
                    actual.update(block)
            assert count == len(raw) and actual.hexdigest() == hashlib.sha256(raw).hexdigest()
    assert reader.output_fd is reader.evidence_fd is None


@pytest.mark.parametrize('layout', ['file_link', 'directory_link', 'noncanonical_name'])
def test_original_inside_symlink_and_noncanonical_path_semantics(tmp_path, layout):
    output, path, raw, local = fixture(tmp_path)
    if layout == 'file_link':
        target = output / 'retained.gz'
        path.rename(target)
        path.symlink_to(target)
    elif layout == 'directory_link':
        folder = output / 'retained'
        path.parent.rename(folder)
        (output / 'evidence').symlink_to(folder, target_is_directory=True)
    else:
        target = path.with_name('original-name.gz')
        path.rename(target)
        path = target
    with PinnedSnapshotStreams(output, local) as reader:
        with reader.open('evidence/' + path.name) as stream:
            assert stream.read() == raw


@pytest.mark.parametrize('layout', ['file_link', 'directory_link', 'absolute', 'traversal'])
def test_escaping_paths_fail(tmp_path, layout):
    output, path, raw, local = fixture(tmp_path)
    outside = tmp_path / 'outside.gz'
    outside.write_bytes(gzip.compress(raw))
    name = 'evidence/' + path.name
    if layout == 'file_link':
        path.unlink()
        path.symlink_to(outside)
    elif layout == 'directory_link':
        path.parent.rename(tmp_path / 'outside-folder')
        (output / 'evidence').symlink_to(tmp_path / 'outside-folder', target_is_directory=True)
    elif layout == 'absolute':
        name = str(outside)
    else:
        name = '../outside.gz'
    with pytest.raises(ValueError, match='escapes'):
        with PinnedSnapshotStreams(output, local) as reader:
            with reader.open(name) as stream:
                stream.read()


@pytest.mark.parametrize('mutation', ['replace_file', 'replace_directory', 'rename_output', 'inplace'])
def test_generation_changes_fail_and_close_descriptors(tmp_path, mutation):
    output, path, raw, local = fixture(tmp_path)
    before = len(list(Path('/proc/self/fd').iterdir()))
    with pytest.raises((ValueError, FileNotFoundError)):
        with PinnedSnapshotStreams(output, local) as reader:
            with reader.open('evidence/' + path.name) as stream:
                assert stream.read() == raw
                if mutation == 'replace_file':
                    replacement = output / 'replacement.gz'
                    replacement.write_bytes(path.read_bytes())
                    replacement.replace(path)
                elif mutation == 'replace_directory':
                    path.parent.rename(output / 'old-evidence')
                    (output / 'evidence').mkdir()
                elif mutation == 'rename_output':
                    output.rename(tmp_path / 'renamed')
                    output.mkdir()
                else:
                    with path.open('ab') as handle:
                        handle.write(b'\0')
    assert reader.output_fd is reader.evidence_fd is None
    assert len(list(Path('/proc/self/fd').iterdir())) == before


@pytest.mark.parametrize('payload', [b'not gzip', gzip.compress(b'payload')[:-4]])
def test_corruption_is_not_hidden(tmp_path, payload):
    output, path, raw, local = fixture(tmp_path)
    path.write_bytes(payload)
    with pytest.raises((gzip.BadGzipFile, EOFError)):
        with PinnedSnapshotStreams(output, local) as reader:
            with reader.open('evidence/' + path.name) as stream:
                stream.read()
    assert reader.output_fd is reader.evidence_fd is None


def test_fifo_is_rejected_without_waiting_for_a_writer(tmp_path):
    output, path, raw, local = fixture(tmp_path)
    path.unlink()
    os.mkfifo(path)
    with pytest.raises(ValueError, match='regular file'):
        with PinnedSnapshotStreams(output, local) as reader:
            with reader.open('evidence/' + path.name):
                pytest.fail('FIFO must not enter decompression')


def test_original_exception_is_preserved_and_fds_close(tmp_path):
    output, path, raw, local = fixture(tmp_path)
    failure = KeyboardInterrupt('original')
    with pytest.raises(KeyboardInterrupt) as caught:
        with PinnedSnapshotStreams(output, local) as reader:
            with reader.open('evidence/' + path.name):
                raise failure
    assert caught.value is failure
    assert reader.output_fd is reader.evidence_fd is None
