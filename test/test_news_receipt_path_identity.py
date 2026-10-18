"""Lexical allocation reduction is not a source/path-validation waiver."""
from pathlib import Path, PosixPath, PurePosixPath, PureWindowsPath

import pytest

from downloader import news_fulltext_queue as queue_module
from downloader import news_receipt_io as io_module
from scripts.benchmark_news_acceptance_recovery import benchmark
from scripts.audit_news_runtime import runtime_source_paths, RUNTIME_ACCEPTANCE_CONTRACT_VERSION
from test_news_acceptance_recovery import accepted_queue, config


def test_runtime_code_gate_includes_both_recovery_and_out_of_prefix_reader():
    repo = Path(__file__).resolve().parents[1]
    paths = runtime_source_paths(repo)
    assert repo / 'downloader/news_receipt_io.py' in paths
    assert repo / 'downloader/news_fulltext_queue.py' in paths
    assert set(repo.joinpath('downloader').glob('news_fulltext*.py')).issubset(paths)
    assert len(paths) == len(set(paths)) and all(path.is_file() for path in paths)
    assert RUNTIME_ACCEPTANCE_CONTRACT_VERSION == 10


@pytest.mark.parametrize('flavour', [PosixPath, PurePosixPath, PureWindowsPath])
@pytest.mark.parametrize('path', [
    '.', '..', '/', '//', '///', '/queue', '/queue/receipts',
    '/queue/receipts/a.json', '/queue/receipts/.hidden.json',
    '/queue/receipts/../a.json', '/queue/receipts/nested/a.json',
    '/queue/receipts-other/a.json', '/Queue/Receipts/a.json',
    'queue/receipts/a.json', '/queue//receipts/./a.json',
    '//queue/receipts/a.json', 'C:/Queue/Receipts/a.json',
])
@pytest.mark.parametrize('directory', [
    '.', '/', '//', '/queue/receipts', 'queue/receipts', 'c:/queue/receipts',
])
def test_lexical_parent_matches_original_for_all_path_shapes(flavour, path, directory):
    path, directory = flavour(path), flavour(directory)
    assert io_module.receipt_parent_matches(path, directory) == (path.parent == directory)


def test_custom_subclass_parent_semantics_remain_authoritative():
    class CustomPath(PosixPath):
        @property
        def parent(self):
            return PosixPath('/custom-parent')
    assert io_module.receipt_parent_matches(CustomPath('/other/a.json'), PosixPath('/custom-parent'))


def test_symlink_is_not_resolved_or_considered_a_valid_receipt(tmp_path):
    directory = tmp_path / 'receipts'
    directory.mkdir()
    link = directory / 'not-a-valid-receipt.json'
    link.symlink_to(tmp_path / 'missing.json')
    assert io_module.receipt_parent_matches(link, directory)
    rows = io_module._read_chunk([link], tmp_path)
    assert isinstance(rows[0][2], FileNotFoundError)


def test_benchmark_uses_same_canonical_full_recovery_and_restores_predicates(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    predicates = queue_module.receipt_parent_matches, io_module.receipt_parent_matches
    try:
        report = benchmark(tmp_path, maximum_receipts=1, cycles=1, path_identity_only=True)
        assert report['accepted'] and report['contract_version'] == 7
        assert report['decode_parity']['all_fields_equal'] and report['path_identity_parity']
        assert report['source_receipts_unchanged'] and report['publisher_requests'] == 0
        assert {row['mode'] for row in report['results']} == {'parent_path_objects', 'lexical_parent_parts'}
        assert all(row['sqlite_changes'] == 0 and row['receipt_io']['completed'] for row in report['results'])
        assert predicates == (queue_module.receipt_parent_matches, io_module.receipt_parent_matches)
    finally:
        queue.close()
        http.close()


def test_failed_benchmark_restores_both_original_predicates(config, tmp_path, monkeypatch):
    queue, http = accepted_queue(config, tmp_path)
    predicates = queue_module.receipt_parent_matches, io_module.receipt_parent_matches
    def fail_recovery(*_args, **_kwargs):
        raise RuntimeError('bounded_benchmark_failure')
    try:
        monkeypatch.setattr(queue_module.NewsQueue, 'recover_acceptance', fail_recovery)
        with pytest.raises(RuntimeError, match='bounded_benchmark_failure'):
            benchmark(tmp_path, maximum_receipts=1, cycles=1, path_identity_only=True)
        assert predicates == (queue_module.receipt_parent_matches, io_module.receipt_parent_matches)
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('kwargs', [
    {'adaptive_read_ahead': True}, {'json_decode_only': True}, {'binary_read_only': True},
    {'read_ahead_workers': 1}, {'batched_read_ahead_workers': 1}, {'path_identity_only': 1},
])
def test_benchmark_rejects_mixed_axes_or_nonboolean(tmp_path, kwargs):
    with pytest.raises(ValueError):
        benchmark(tmp_path, path_identity_only=kwargs.pop('path_identity_only', True), **kwargs)
