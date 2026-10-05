"""Installation must refuse native/unknown environments before any mutation."""
from pathlib import Path
import sys

import pytest

from scripts.install_control_plane import (activate_control_role, control_environment_body,
    control_role_specification, install_control_role, install_verification_units)


def test_role_install_refuses_the_selected_native_interpreter_prefix(tmp_path):
    with pytest.raises(ValueError, match='selected native runtime'):
        install_control_role(Path(sys.prefix), tmp_path/'evidence')
    assert not (tmp_path/'evidence').exists()


def test_verification_install_refuses_untrusted_role_before_any_deployment(tmp_path):
    env = tmp_path / 'private.env'
    env.write_text('CONTROL_PLANE_DSN=private\nCONTROL_PLANE_ENV_PATH=/unknown\n')
    env.chmod(0o644)
    evidence = tmp_path / 'evidence'
    with pytest.raises(ValueError, match='private owned'):
        install_verification_units(env, tmp_path / 'accepted-lock.json', evidence)
    assert not evidence.exists()
    env.chmod(0o600)
    alias = tmp_path / 'redirect.env'
    alias.symlink_to(env)
    with pytest.raises(ValueError, match='private owned'):
        install_verification_units(alias, tmp_path / 'accepted-lock.json', evidence)
    assert not evidence.exists()


def test_control_pointer_change_preserves_credentials_comments_and_other_roles(tmp_path):
    body = "# private environment\nCONTROL_PLANE_DSN='postgresql://secret@localhost/db'\nCONTROL_PLANE_ENV_PATH=/old\nOTHER_ROLE=/untouched\n"
    changed, dsn = control_environment_body(body, tmp_path/'new role')
    assert dsn == 'postgresql://secret@localhost/db'
    assert changed.replace("CONTROL_PLANE_ENV_PATH='" + str(tmp_path/'new role') + "'", 'CONTROL_PLANE_ENV_PATH=/old') == body


@pytest.mark.parametrize('body', [
    'CONTROL_PLANE_ENV_PATH=/old\n',
    'CONTROL_PLANE_DSN=secret\nCONTROL_PLANE_ENV_PATH=/a\nCONTROL_PLANE_ENV_PATH=/b\n',
    'CONTROL_PLANE_DSN=one two\nCONTROL_PLANE_ENV_PATH=/old\n',
])
def test_ambiguous_control_environment_is_rejected(body, tmp_path):
    with pytest.raises(ValueError):
        control_environment_body(body, tmp_path/'new')


def test_activation_rejects_public_or_redirected_private_environment(tmp_path):
    target = tmp_path/'private.env'
    target.write_text('CONTROL_PLANE_DSN=secret\nCONTROL_PLANE_ENV_PATH=/old\n')
    target.chmod(0o644)
    with pytest.raises(ValueError, match='private owned'):
        activate_control_role(tmp_path/'role', target, tmp_path/'evidence')
    target.chmod(0o600)
    link = tmp_path/'redirect.env'
    link.symlink_to(target)
    with pytest.raises(ValueError, match='private owned'):
        activate_control_role(tmp_path/'role', link, tmp_path/'evidence')
    assert 'ENV_PATH=/old' in target.read_text()


def test_other_platform_resolves_its_own_declaration(monkeypatch):
    monkeypatch.setattr('scripts.install_control_plane.platform.machine', lambda: 'aarch64')
    specification, explicit = control_role_specification()
    assert specification.name == 'control.yml'
    assert not explicit


def test_role_install_refuses_existing_environment_without_modifying_it(tmp_path):
    target = tmp_path/'existing-role'
    target.mkdir()
    sentinel = target/'unknown-owner.txt'
    sentinel.write_text('existing environment remains its owner\n')
    with pytest.raises(ValueError, match='fresh mamba prefix'):
        install_control_role(target, tmp_path/'evidence')
    assert sentinel.read_text() == 'existing environment remains its owner\n'
    assert list(target.iterdir()) == [sentinel]
    assert not (tmp_path/'evidence').exists()
