"""CUDA discovery is not executable CUDA; preflight must fail closed."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from scripts import check_environment as check


def _torch(*, available=True, count=2):
    cuda = SimpleNamespace(
        is_available=Mock(return_value=available),
        device_count=Mock(return_value=count),
        init=Mock(),
        get_device_name=Mock(side_effect=lambda index: f'GPU {index}'),
        synchronize=Mock(),
    )
    value = SimpleNamespace(item=Mock(return_value=1.0))
    return SimpleNamespace(version=SimpleNamespace(cuda='12.8'), cuda=cuda,
                           ones=Mock(return_value=value))


def test_nvml_count_does_not_trigger_property_query_after_failed_cuda(monkeypatch):
    monkeypatch.setenv('CUDA_VISIBLE_DEVICES', '0,1')
    torch = _torch(available=False)
    info, failures = check._torch_cuda_report(torch, require_cuda=True, minimum_devices=2)
    assert failures and info['device_count'] == 2
    assert info['cuda_available'] is False and info['cuda_compute_verified'] is False
    assert info['devices'] == [] and info['verified_device_indices'] == []
    torch.cuda.init.assert_not_called()
    torch.cuda.get_device_name.assert_not_called()
    torch.ones.assert_not_called()
    assert info['cuda_visible_devices'] == '0,1'
    assert check.os.environ['CUDA_VISIBLE_DEVICES'] == '0,1'


def test_nvml_based_available_true_still_requires_driver_initialization():
    torch = _torch()
    torch.cuda.init.side_effect = RuntimeError('CUDA unknown error')
    info, failures = check._torch_cuda_report(torch, require_cuda=True)
    assert any('initialization' in message and 'CUDA unknown error' in message for message in failures)
    assert info['cuda_available'] is False and info['cuda_runtime_initialized'] is False
    torch.cuda.get_device_name.assert_not_called()
    torch.ones.assert_not_called()


def test_healthy_dual_gpu_preflight_checks_all_actual_allocations():
    torch = _torch()
    info, failures = check._torch_cuda_report(torch, require_cuda=True, minimum_devices=2)
    assert failures == []
    assert info['cuda_available'] and info['cuda_runtime_initialized'] and info['cuda_compute_verified']
    assert info['verified_device_indices'] == [0, 1]
    assert [call.kwargs['device'] for call in torch.ones.call_args_list] == ['cuda:0', 'cuda:1']
    assert [call.args[0] for call in torch.cuda.synchronize.call_args_list] == [0, 1]


def test_partial_second_device_failure_does_not_pass_ddp_gate():
    torch = _torch()
    torch.ones.side_effect = [SimpleNamespace(item=lambda: 1), RuntimeError('second device failed')]
    info, failures = check._torch_cuda_report(torch, require_cuda=True, minimum_devices=2)
    assert info['cuda_available'] is False and info['cuda_compute_verified'] is False
    assert info['verified_device_indices'] == [0]
    assert info['probe_errors'][0]['stage'] == 'device:1'
    assert any('second device failed' in message for message in failures)


@pytest.mark.parametrize('stage', ['is_available', 'device_count', 'get_device_name', 'synchronize'])
def test_cuda_errors_are_reported_without_escaping(stage):
    torch = _torch()
    getattr(torch.cuda, stage).side_effect = RuntimeError('injected CUDA failure')
    info, failures = check._torch_cuda_report(torch, require_cuda=True)
    assert info['cuda_available'] is False and failures
    assert any('injected CUDA failure' in message for message in failures)


def test_metadata_success_cannot_hide_bad_cuda_kernel_result():
    torch = _torch()
    torch.ones.return_value.item.return_value = 0
    info, failures = check._torch_cuda_report(torch, require_cuda=True)
    assert info['cuda_compute_verified'] is False and info['cuda_available'] is False
    assert any('verification failed' in message for message in failures)


def test_minimum_devices_blocks_one_card_ddp_before_allocation():
    torch = _torch(count=1)
    info, failures = check._torch_cuda_report(torch, require_cuda=True, minimum_devices=2)
    assert failures == ['CUDA requires at least 2 devices; discovered 1']
    assert info['cuda_available'] is False
    torch.cuda.init.assert_not_called()
    torch.ones.assert_not_called()


def test_cpu_only_inspection_remains_supported_without_requiring_cuda():
    torch = _torch(available=False, count=0)
    torch.version.cuda = None
    info, failures = check._torch_cuda_report(torch, require_cuda=False)
    assert failures == [] and info['cuda_available'] is False
    torch.cuda.init.assert_not_called()


def test_optional_metadata_inspection_does_not_run_gpu_kernel():
    torch = _torch()
    info, failures = check._torch_cuda_report(torch, require_cuda=False)
    assert failures == [] and info['cuda_available'] is True
    assert info['cuda_compute_verified'] is False
    torch.ones.assert_not_called()


def test_recovery_query_reports_host_reboot_without_claiming_compute_health(monkeypatch):
    query = Mock(return_value=SimpleNamespace(
        returncode=0, stderr='', stdout='0, GPU-first, Reboot\n1, GPU-second, Reboot\n',
    ))
    monkeypatch.setattr(check.subprocess, 'run', query)
    info = check._gpu_recovery_diagnostics()
    assert info['status'] == 'observed' and info['host_os_reboot_required'] is True
    assert info['gpus'][1]['recovery_action'] == 'Reboot'
    assert 'cuda_available' not in info
    assert query.call_args.kwargs['timeout'] == 5
    assert query.call_args.args[0] == [
        'nvidia-smi', '--query-gpu=index,uuid,gpu_recovery_action', '--format=csv,noheader',
    ]


def test_recovery_query_none_does_not_imply_cuda_readiness(monkeypatch):
    monkeypatch.setattr(check.subprocess, 'run', Mock(return_value=SimpleNamespace(
        returncode=0, stderr='', stdout='0, GPU-first, None\n',
    )))
    info = check._gpu_recovery_diagnostics()
    assert info['status'] == 'observed' and info['host_os_reboot_required'] is False
    assert 'cuda_compute_verified' not in info


@pytest.mark.parametrize('fault', [
    FileNotFoundError('nvidia-smi missing'),
    check.subprocess.TimeoutExpired('nvidia-smi', 5),
    SimpleNamespace(returncode=1, stderr='unsupported query', stdout=''),
    SimpleNamespace(returncode=0, stderr='', stdout='not,a,gpu\n'),
    SimpleNamespace(returncode=0, stderr='', stdout=''),
])
def test_recovery_query_failure_preserves_unknown_and_does_not_escape(monkeypatch, fault):
    query = Mock(side_effect=fault) if isinstance(fault, Exception) else Mock(return_value=fault)
    monkeypatch.setattr(check.subprocess, 'run', query)
    info = check._gpu_recovery_diagnostics()
    assert info['status'] == 'unavailable'
    assert info['host_os_reboot_required'] is None and info['gpus'] == []
    assert info['error']['message']


def test_device_diagnostics_explains_host_reboot_without_mutating_devices(monkeypatch):
    monkeypatch.setattr(check.Path, 'stat', Mock(side_effect=FileNotFoundError('no device')))
    monkeypatch.setattr(check, '_gpu_recovery_diagnostics', lambda: {
        'status': 'observed', 'gpus': [{'recovery_action': 'Reboot'}], 'host_os_reboot_required': True,
    })
    open_device = Mock()
    monkeypatch.setattr(check.os, 'open', open_device)
    info = check._cuda_device_diagnostics()
    assert 'physical host OS' in info['recovery_hint']
    assert 'Docker instance' in info['recovery_hint']
    assert all('error' in node for node in info['device_nodes'])
    open_device.assert_not_called()


@pytest.mark.parametrize('args', [
    ['--minimum-cuda-devices', '2'],
    ['--require-cuda', '--minimum-cuda-devices', '0'],
])
def test_cuda_minimum_arguments_are_not_silently_ignored(monkeypatch, args):
    monkeypatch.setattr(check.sys, 'argv', ['check_environment.py', *args])
    with pytest.raises(SystemExit) as result:
        check._parse_args()
    assert result.value.code == 2


def test_failed_main_emits_json_and_does_not_write_a_runtime_lock(monkeypatch, capsys, tmp_path):
    import json

    target = tmp_path / 'must-not-exist.json'
    monkeypatch.setattr(check, '_parse_args', lambda: SimpleNamespace(
        require_cuda=True, strict=False, minimum_cuda_devices=2,
        expected_runtime_lock=None, runtime_lock_output=target,
    ))
    monkeypatch.setattr(check, 'normalize_runtime_env', lambda: (tmp_path / 'fintech', None))
    original = check._torch_cuda_report
    monkeypatch.setattr(check, '_torch_cuda_report', lambda *args, **kwargs:
                        original(_torch(available=False), **kwargs))
    monkeypatch.setattr(check, '_cuda_device_diagnostics', lambda: {'uvm': 'EIO'})
    monkeypatch.setattr(check, 'runtime_identity', lambda: {'metadata_errors': []})
    monkeypatch.delenv('CONDA_PREFIX', raising=False)
    assert check.main() == 1
    output = capsys.readouterr()
    report = json.loads(output.out)
    assert report['torch']['device_count'] == 2
    assert report['torch']['devices'] == [] and report['failures']
    assert 'Environment check failed' in output.err and 'Traceback' not in output.err
    assert not target.exists()
