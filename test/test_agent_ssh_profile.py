"""Optional SSH setup must preserve unrelated configuration and trust files."""
import json
from pathlib import Path
import tempfile

import pytest

from scripts.prepare_agent_ssh_profile import prepare


@pytest.fixture
def endpoint(tmp_path):
    identity = tmp_path / "identity with spaces"
    hosts = tmp_path / "known_hosts"
    identity.write_text("test identity path only")
    hosts.write_text("test host-verification path only")
    source = tmp_path / "private.ini"
    source.write_text(f"[node]\ntype=sftp\nhost=example.test\nuser=root\nport=2222\n"
                      f"key_file={identity}\nknown_hosts_file={hosts}\n")
    source.chmod(0o600)
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"rclone_config": str(source), "remote_name": "node"}))
    # SSH appends a 40-byte connection hash. pytest's descriptive directory
    # names can exceed the kernel's Unix socket path limit even for valid inputs.
    with tempfile.TemporaryDirectory(prefix="sa-ssh-test-") as short_root:
        yield policy, source, tmp_path / "agent.conf", Path(short_root) / "control"


def test_prepared_profile_preserves_trust_and_changes_socket_for_a_new_endpoint(endpoint):
    policy, source, output, control = endpoint
    first = prepare(policy, output, control)
    text = output.read_text()
    assert "StrictHostKeyChecking yes" in text and "BatchMode yes" in text
    assert "identity with spaces" in text
    assert output.stat().st_mode & 0o777 == 0o600
    assert control.stat().st_mode & 0o777 == 0o700
    assert prepare(policy, output, control)["profile_sha256"] == first["profile_sha256"]
    source.write_text(source.read_text().replace("port=2222", "port=2223"))
    assert prepare(policy, output, control)["profile_sha256"] != first["profile_sha256"]
    assert next(line for line in text.splitlines() if "ControlPath" in line) not in output.read_text()


def test_unmanaged_or_redirected_output_is_preserved(endpoint):
    policy, _, output, control = endpoint
    output.write_text("Host user-owned\n")
    with pytest.raises(ValueError, match="unrelated"):
        prepare(policy, output, control)
    assert output.read_text() == "Host user-owned\n"
    alias = output.with_name("alias")
    alias.symlink_to(output)
    with pytest.raises(ValueError, match="redirected"):
        prepare(policy, alias, control)
    assert output.read_text() == "Host user-owned\n"


def test_unsafe_endpoint_or_public_credential_file_never_writes_profile(endpoint):
    policy, source, output, control = endpoint
    source.chmod(0o644)
    with pytest.raises(ValueError, match="private"):
        prepare(policy, output, control)
    assert not output.exists()
    source.chmod(0o600)
    source.write_text(source.read_text().replace("example.test", "-oProxyCommand=bad"))
    with pytest.raises(ValueError, match="invalid"):
        prepare(policy, output, control)
    assert not output.exists()


def test_world_accessible_socket_directory_and_long_paths_are_rejected(endpoint):
    policy, _, output, control = endpoint
    control.mkdir(mode=0o755)
    with pytest.raises(ValueError, match="privately"):
        prepare(policy, output, control)
    assert not output.exists()
    with pytest.raises(ValueError, match="too long"):
        prepare(policy, output, control.parent / ("a" * 100))
    assert not output.exists()
