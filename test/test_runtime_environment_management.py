from pathlib import Path
import sys

import pytest

from scripts.manage_runtime_environments import explicit_lock, validate_fresh_role


def test_explicit_lock_has_header_and_content_hash_without_display_banner():
    rows = [{'name': 'python', 'url': 'https://conda.anaconda.org/conda-forge/linux-64/python.conda',
             'sha256': 'a' * 64, 'md5': 'b' * 32}]
    lock = explicit_lock(rows)
    assert '\n@EXPLICIT\n' in lock
    assert lock.endswith('python.conda#' + 'a' * 64 + '\n')


@pytest.mark.parametrize('rows', [[], [{'url': 'https://private.invalid/pkg', 'sha256': 'a'*64}],
                                  [{'url': 'https://conda.anaconda.org/pkg', 'sha256': ''}]])
def test_explicit_lock_rejects_missing_or_unbounded_installed_identity(rows):
    with pytest.raises(ValueError):
        explicit_lock(rows)


def test_creation_cannot_replace_native_or_existing_role(tmp_path):
    with pytest.raises(ValueError, match='native runtime'):
        validate_fresh_role(Path(sys.prefix))
    sentinel = tmp_path/'existing'
    sentinel.mkdir()
    (sentinel/'owner').write_text('retained')
    with pytest.raises(ValueError, match='existing environments'):
        validate_fresh_role(sentinel)
    assert (sentinel/'owner').read_text() == 'retained'


def test_creation_rejects_redirected_parent(tmp_path):
    actual = tmp_path/'actual'
    actual.mkdir()
    (tmp_path/'redirect').symlink_to(actual, target_is_directory=True)
    with pytest.raises(ValueError, match='redirected'):
        validate_fresh_role(tmp_path/'redirect'/'new')
