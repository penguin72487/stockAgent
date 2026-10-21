"""The hardened reader must reject stale boots without guessing UTC epochs."""
from datetime import UTC,datetime,timedelta
import os
from pathlib import Path
import subprocess

import pytest

from stockagent.data import finlab_acquisition_contract as contract


@pytest.fixture
def hidden_kernel_with_bind(tmp_path,monkeypatch):
    owner=contract.process_owner()
    assert owner and owner['boot_id']
    bound=tmp_path/'kernel-boot-id'
    bound.write_text(owner['boot_id']+'\n')
    original_read=Path.read_text

    def read(path,*args,**kwargs):
        if str(path)=='/proc/sys/kernel/random/boot_id':
            raise FileNotFoundError('ProcSubset=pid')
        return original_read(path,*args,**kwargs)

    monkeypatch.setattr(Path,'read_text',read)
    monkeypatch.setattr(contract,'PROCESS_BOOT_ID_BIND',bound)
    monkeypatch.setattr(Path,'is_mount',lambda path:path==bound)
    return owner,bound


def test_exact_boot_and_pid_survive_wall_clock_resynchronization(hidden_kernel_with_bind):
    owner,_=hidden_kernel_with_bind
    now=datetime.now(UTC)
    # A reconstructed boot UTC can move past the recorded run start. Exact
    # current kernel identity still proves this process without a tolerance.
    assert contract.process_owner_alive(owner,started_at_utc=(now-timedelta(days=1)).isoformat(),now=now)
    assert contract.process_owner(owner['pid'])==owner
    assert not contract.process_owner_alive({**owner,'boot_id':'00000000-0000-0000-0000-000000000000'},now=now)
    assert not contract.process_owner_alive({**owner,'start_ticks':'wrong-generation'},now=now)


def test_an_ordinary_persisted_file_is_not_boot_evidence(hidden_kernel_with_bind,monkeypatch):
    owner,_=hidden_kernel_with_bind
    monkeypatch.setattr(Path,'is_mount',lambda _path:False)
    assert contract.process_owner(owner['pid'])['boot_id'] is None
    assert not contract.process_owner_alive(owner,now=datetime.now(UTC))


def test_a_symlink_is_not_a_kernel_bind(hidden_kernel_with_bind,monkeypatch):
    owner,bound=hidden_kernel_with_bind
    link=bound.with_name('symlink')
    link.symlink_to(bound)
    monkeypatch.setattr(contract,'PROCESS_BOOT_ID_BIND',link)
    monkeypatch.setattr(Path,'is_mount',lambda _path:True)
    assert contract.process_owner(owner['pid'])['boot_id'] is None


@pytest.mark.parametrize('content',[
    b'',b'not-a-boot-uuid',b'x'*1048576,b'\xff'*37,
    b'00000000-0000-0000-0000-000000000000\n\n',
    b'00000000-0000-0000-0000-000000000000x',
])
def test_malformed_or_oversized_bound_data_is_not_trusted(hidden_kernel_with_bind,content):
    owner,bound=hidden_kernel_with_bind
    bound.write_bytes(content)
    assert contract.process_owner(owner['pid'])['boot_id'] is None


def test_gateway_exposes_only_the_readonly_kernel_identity():
    path=Path(__file__).resolve().parents[1]/'deploy/systemd/stockagent-public-dashboards.service.in'
    unit=path.read_text()
    assert 'ProcSubset=pid' in unit and 'ProtectProc=invisible' in unit
    assert 'ProtectKernelTunables=true' in unit and 'ProtectClock=true' in unit
    assert 'BindReadOnlyPaths=/proc/sys/kernel/random/boot_id:/run/stockagent-process-boot-id' in unit


@pytest.mark.parametrize('arguments',[
    ['--gateway-only'],['--gateway-only','--no-restart'],['--no-restart','--gateway-only'],[],
])
def test_scoped_installer_preserves_other_units_and_passes_exact_paths(tmp_path,arguments):
    binaries=tmp_path/'bin'
    binaries.mkdir()
    log=tmp_path/'install.log'
    stub=binaries/'mock-command'
    stub.write_text('''#!/usr/bin/env bash
set -eu
printf '%s ' "${0##*/}" "$@" >> "$news_install_log"
printf '\\n' >> "$news_install_log"
if [[ "${0##*/}" == systemd-analyze ]]; then
    [[ "$1" == verify ]]
    shift
    for path in "$@"; do [[ -f "$path" ]]; done
fi
''')
    stub.chmod(0o755)
    for name in ('systemd-analyze','install','systemctl','curl','chmod','mkdir'):
        (binaries/name).symlink_to(stub)
    script=Path(__file__).resolve().parents[1]/'scripts/install_public_dashboards_service.sh'
    env={**os.environ,'PATH':str(binaries)+os.pathsep+os.environ['PATH'],'news_install_log':str(log)}
    subprocess.run(['bash',str(script),*arguments],env=env,check=True,capture_output=True,text=True,timeout=10)
    calls=log.read_text()
    if '--gateway-only' in arguments:
        assert 'stockagent-data-refresh-status-snapshot' not in calls
    else:
        assert 'systemctl enable --now stockagent-data-refresh-status-snapshot.timer' in calls
    assert ('systemctl restart stockagent-public-dashboards.service' in calls)==('--no-restart' not in arguments)
    assert 'systemctl enable stockagent-public-dashboards.service' in calls
