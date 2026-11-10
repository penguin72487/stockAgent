import hashlib
import os
import sqlite3

import pytest

from scripts.audit_e_staging_against_cold import compare_index


def make_index(path, rows):
    with sqlite3.connect(path) as connection:
        connection.execute('CREATE TABLE files (path TEXT, kind TEXT, sha TEXT, bytes INTEGER, state TEXT)')
        connection.executemany('INSERT INTO files VALUES (?, ?, ?, ?, ?)', rows)


def test_inventory_comparison_conserves_staged_scope_and_keeps_D_only(tmp_path):
    path = tmp_path / 'files.sqlite'
    a, b, c = 'a' * 64, 'b' * 64, 'c' * 64
    make_index(path, [('same', 'file', a, 10, 'verified'), ('changed', 'file', b, 20, 'verified'),
                      ('new', 'file', c, 30, 'verified'), ('dir', 'directory', None, 0, 'pending')])
    cold = [{'kind': 'file', 'path': 'changed', 'sha256': a, 'size': 20},
            {'kind': 'directory', 'path': 'dir'},
            {'kind': 'file', 'path': 'only-d', 'sha256': b, 'size': 4},
            {'kind': 'file', 'path': 'same', 'sha256': a, 'size': 10}]
    assert compare_index(path, 3, 60, cold) == {
        'exact_files': 1, 'exact_bytes': 10, 'changed_files': 1, 'changed_bytes': 20,
        'e_only_files': 1, 'e_only_bytes': 30, 'd_only_files': 1, 'd_non_file_entries': 1}


def test_inventory_comparison_includes_exact_changed_and_missing_symlink_text(tmp_path):
    path = tmp_path/'files.sqlite'
    digest = lambda text: hashlib.sha256(os.fsencode(text)).hexdigest()
    make_index(path, [('a-link', 'symlink', digest('../a'), 0, 'verified'),
                      ('b-link', 'symlink', digest('../b'), 0, 'verified'),
                      ('c-link', 'symlink', digest('../c'), 0, 'verified'),
                      ('file', 'file', 'a'*64, 3, 'verified')])
    cold = [{'kind':'symlink', 'path':'prepared/a-link', 'target':'../a'},
            {'kind':'symlink', 'path':'prepared/b-link', 'target':'../other'},
            {'kind':'symlink', 'path':'prepared/d-link', 'target':'../d'},
            {'kind':'file', 'path':'prepared/file', 'sha256':'a'*64, 'size':3}]
    result = compare_index(path, 1, 3, cold, source_prefix='prepared', expected_symlinks=3)
    assert {key:result[key] for key in ('exact_symlinks', 'changed_symlinks', 'e_only_symlinks',
                                        'd_only_symlinks')} == {'exact_symlinks':1, 'changed_symlinks':1,
                                                                  'e_only_symlinks':1, 'd_only_symlinks':1}
    assert result['exact_files'] == 1 and result['d_non_file_entries'] == 3
    with pytest.raises(ValueError, match='symlink count'):
        compare_index(path, 1, 3, cold, source_prefix='prepared', expected_symlinks=2)


@pytest.mark.parametrize('path_name,digest', [('../escape', 'a' * 64), ('bad//name', 'a' * 64),
    ('a/./b', 'a' * 64), ('a/../b', 'a' * 64), ('a/', 'a' * 64), ('name', 'not-a-sha')])
def test_comparison_rejects_invalid_staging_identity(tmp_path, path_name, digest):
    path = tmp_path / 'files.sqlite'
    make_index(path, [(path_name, 'file', digest, 1, 'verified')])
    with pytest.raises(ValueError, match='invalid member'):
        compare_index(path, 1, 1, [])


def test_comparison_cannot_claim_an_unfinished_or_wrong_sized_copy(tmp_path):
    path = tmp_path / 'files.sqlite'
    make_index(path, [('same', 'file', 'a' * 64, 10, 'verified'),
                      ('pending', 'file', 'b' * 64, 20, 'pending')])
    with pytest.raises(ValueError, match='invalid member'):
        compare_index(path, 2, 30, [])
    with pytest.raises(ValueError, match='invalid member'):
        compare_index(path, 1, 11, [])


def test_comparison_refuses_uncheckpointed_wal(tmp_path):
    path = tmp_path / 'files.sqlite'
    make_index(path, [('same', 'file', 'a' * 64, 10, 'verified')])
    (tmp_path / 'files.sqlite-wal').write_bytes(b'uncheckpointed')
    with pytest.raises(ValueError, match='WAL'):
        compare_index(path, 1, 10, [])


def test_subtree_comparison_matches_relative_paths_without_counting_siblings(tmp_path):
    path = tmp_path / 'files.sqlite'
    digest = 'a' * 64
    make_index(path, [('child/file', 'file', digest, 10, 'verified')])
    cold = iter([{'kind': 'file', 'path': 'other', 'sha256': digest, 'size': 5},
                 {'kind': 'directory', 'path': 'prepared'},
                 {'kind': 'directory', 'path': 'prepared/child'},
                 {'kind': 'file', 'path': 'prepared/child/file', 'sha256': digest, 'size': 10},
                 {'kind': 'file', 'path': 'prepared/old', 'sha256': digest, 'size': 4}])
    result = compare_index(path, 1, 10, cold, source_prefix='prepared')
    assert result['exact_files'] == 1 and result['exact_bytes'] == 10
    assert result['d_only_files'] == 1 and result['d_non_file_entries'] == 1


def test_inventory_rejects_unsorted_cold_namespace(tmp_path):
    path = tmp_path / 'files.sqlite'
    digest = 'a' * 64
    make_index(path, [('file', 'file', digest, 10, 'verified')])
    cold = [{'kind': 'file', 'path': 'z', 'sha256': digest, 'size': 1},
            {'kind': 'file', 'path': 'a', 'sha256': digest, 'size': 1}]
    with pytest.raises(ValueError, match='sorted'):
        compare_index(path, 1, 10, cold)


def test_comparison_reports_source_progress_and_obeys_the_work_guard(tmp_path):
    path = tmp_path/'files.sqlite'
    digest = 'a'*64
    rows = [(f'{index:05}', 'file', digest, 1, 'verified') for index in range(8200)]
    make_index(path, rows)
    cold = ({'kind':'file', 'path':row[0], 'sha256':digest, 'size':1} for row in rows)
    progress, checks = [], []
    result = compare_index(path, len(rows), len(rows), cold, progress=progress.append,
                           guard=lambda: checks.append(True))
    assert result['exact_files'] == len(rows) and checks
    assert progress[0]['staged_files_scanned'] == 0
    assert progress[-1]['staged_files_scanned'] == len(rows)
    assert progress[-1]['cold_entries_scanned'] == len(rows)
