from copy import deepcopy
from types import SimpleNamespace
from pathlib import Path

import pytest
import torch
from torch import nn

from stockagent.training.optimizer_config import normalized_optimizer_options, optimizer_checkpoint_fields
from stockagent.training.optimizers import CombinedOptimizer, create_frontier_optimizer, partition_hidden_linear_parameters


class Policy(nn.Module):
    def __init__(self):
        super().__init__()
        self.feature_proj = nn.Linear(5, 8)
        self.temporal_blocks = nn.ModuleList([nn.Sequential(nn.Linear(8, 8), nn.LayerNorm(8))])
        self.score_head = nn.Linear(8, 1)
        self.latent_queries = nn.Parameter(torch.randn(1, 8))

    def forward(self, x):
        return self.score_head(self.temporal_blocks[0](self.feature_proj(x)) + self.latent_queries)


def settings(name, options=None):
    return SimpleNamespace(optimizer_name=name, optimizer_options=options or {}, learning_rate=1e-4, weight_decay=.01)


def update(model, optimizer, x):
    optimizer.zero_grad(set_to_none=True)
    model(x).square().mean().backward()
    torch.nn.utils.clip_grad_norm_(model.parameters(), 1.)
    optimizer.step()


def test_partition_selects_hidden_module_roles_and_covers_each_parameter_once():
    model = Policy()
    matrix, aux, manifest = partition_hidden_linear_parameters(model)
    assert matrix == [model.temporal_blocks[0][0].weight]
    assert {id(p) for p in matrix}.isdisjoint({id(p) for p in aux})
    assert {id(p) for p in matrix + aux} == {id(p) for p in model.parameters()}
    assert manifest['matrix'][0]['name'] == 'temporal_blocks.0.0.weight'
    assert id(model.feature_proj.weight) in {id(p) for p in aux}
    assert id(model.latent_queries) in {id(p) for p in aux}


@pytest.mark.parametrize('name', ['sgd', 'muon', 'soap', 'ademamix', 'normuon', 'dion3'])
def test_optimizer_resume_and_scheduler_continue_the_same_trajectory(name, tmp_path):
    if name in {'normuon', 'dion3'}:
        pytest.importorskip('dion')
    torch.manual_seed(42)
    model = Policy()
    cfg = settings(name)
    optimizer = create_frontier_optimizer(model, cfg, torch.device('cpu'))
    scheduler = torch.optim.lr_scheduler.ExponentialLR(optimizer, gamma=.9)
    x = torch.randn(4, 5)
    for _ in range(3):
        update(model, optimizer, x)
        scheduler.step()
    checkpoint = tmp_path / 'optimizer.pt'
    torch.save({'model': model.state_dict(), 'optimizer': optimizer.state_dict(), 'scheduler': scheduler.state_dict()}, checkpoint)
    resumed = Policy()
    resumed_optimizer = create_frontier_optimizer(resumed, cfg, torch.device('cpu'))
    resumed_scheduler = torch.optim.lr_scheduler.ExponentialLR(resumed_optimizer, gamma=.9)
    state = torch.load(checkpoint, weights_only=True)
    resumed.load_state_dict(state['model'])
    resumed_optimizer.load_state_dict(state['optimizer'])
    resumed_scheduler.load_state_dict(state['scheduler'])
    for _ in range(3):
        update(model, optimizer, x)
        update(resumed, resumed_optimizer, x)
        scheduler.step()
        resumed_scheduler.step()
    for p, q in zip(model.parameters(), resumed.parameters()):
        torch.testing.assert_close(p, q, rtol=0, atol=0)
    assert [g['lr'] for g in optimizer.param_groups] == [g['lr'] for g in resumed_optimizer.param_groups]
    assert all(bool(torch.isfinite(p).all()) for p in model.parameters())


def test_muon_combination_matches_independent_native_optimizers():
    torch.manual_seed(2)
    candidate = Policy()
    oracle = deepcopy(candidate)
    combined = create_frontier_optimizer(candidate, settings('muon'), torch.device('cpu'))
    matrix, aux, _ = partition_hidden_linear_parameters(oracle)
    muon = torch.optim.Muon(matrix, lr=1e-4, weight_decay=.01, momentum=.95, nesterov=True, ns_steps=5, adjust_lr_fn='match_rms_adamw')
    adamw = torch.optim.AdamW(aux, lr=1e-4, weight_decay=.01)
    x = torch.randn(4, 5)
    for _ in range(3):
        update(candidate, combined, x)
        oracle.zero_grad(set_to_none=True)
        oracle(x).square().mean().backward()
        torch.nn.utils.clip_grad_norm_(oracle.parameters(), 1.)
        muon.step()
        adamw.step()
    for p, q in zip(candidate.parameters(), oracle.parameters()):
        torch.testing.assert_close(p, q, rtol=0, atol=0)


def test_legacy_adamw_fingerprint_and_new_optimizer_identities():
    assert optimizer_checkpoint_fields(settings('adamw')) == {'name': 'AdamW'}
    assert optimizer_checkpoint_fields(settings('muon')) != optimizer_checkpoint_fields(settings('adamw'))
    assert optimizer_checkpoint_fields(settings('muon', {'matrix_lr_scale': 2.})) != optimizer_checkpoint_fields(settings('muon'))
    mix = optimizer_checkpoint_fields(settings('muon', {'aux_optimizer': 'ademamix'}))
    assert 'apple-aiml-research/ml-ademamix' in mix['implementation_revisions']
    sgd = optimizer_checkpoint_fields(settings('sgd'))
    assert sgd['parameter_partition'] == 'all_trainable'
    assert sgd != optimizer_checkpoint_fields(settings('adamw'))
    assert sgd != optimizer_checkpoint_fields(settings('sgd', {'momentum': .9}))


@pytest.mark.parametrize('device_name', ['cpu', pytest.param('cuda', marks=pytest.mark.skipif(
    not torch.cuda.is_available(), reason='requires CUDA'))])
def test_sgd_covers_models_without_hidden_partition_and_matches_native_updates(device_name):
    device = torch.device(device_name)
    model = nn.Linear(3, 2).to(device)
    oracle = deepcopy(model)
    config = settings('sgd', {'momentum': .9})
    actual = create_frontier_optimizer(model, config, device)
    expected = torch.optim.SGD(oracle.parameters(), lr=config.learning_rate,
                              weight_decay=config.weight_decay, momentum=.9, foreach=True)
    x = torch.tensor([[.2, .3, -.1]], device=device)
    for _ in range(3):
        update(model, actual, x)
        update(oracle, expected, x)
    for p, q in zip(model.parameters(), oracle.parameters()):
        torch.testing.assert_close(p, q, rtol=1e-6, atol=1e-8)


@pytest.mark.parametrize('name,options', [('fake', {}), ('muon', {'ns_steps': 0}), ('soap', {'precondition_frequency': False}), ('muon', {'matrix_lr_scale': float('nan')}), ('adamw', {'ignored_key': 3}), ('dion3', {'fraction': 0.}), ('normuon', {'aux_optimizer': 'ademamix'}), ('sgd', {'nesterov': True}), ('sgd', {'dampening': -1.})])
def test_invalid_optimizer_configuration_fails_closed(name, options):
    with pytest.raises(ValueError):
        normalized_optimizer_options(name, options)


def test_combined_optimizer_rejects_overlapping_groups():
    parameter = nn.Parameter(torch.ones(2))
    with pytest.raises(ValueError, match='overlap'):
        CombinedOptimizer([torch.optim.AdamW([parameter]), torch.optim.SGD([parameter], lr=.1)])


def test_short_screen_preserves_formal_scheduler_and_original_config():
    from scripts.run_frontier_training_techniques import pin_screen_scheduler
    original = {'training': {'epochs': 1000, 'lr_scheduler_t_max': 0,
                            'lr_scheduler_warmup_steps': 32,
                            'day_trade_optimizer_step_per_trajectory': True}}
    short = pin_screen_scheduler(original)
    assert short['training']['lr_scheduler_t_max'] == 1000
    assert short['training']['lr_scheduler_warmup_steps'] == 32
    assert original['training']['lr_scheduler_t_max'] == 0


def test_screen_binds_paths_before_child_cwd_and_continues_failed_trials(tmp_path, monkeypatch):
    import json
    import sys
    import yaml
    import scripts.run_frontier_training_techniques as screen
    import scripts.run_relocated_training as relocated
    workspace = tmp_path / 'workspace'
    code = tmp_path / 'frozen-source'
    workspace.mkdir()
    code.mkdir()
    monkeypatch.chdir(workspace)
    baseline = {'training': {'epochs': 1000, 'lr_scheduler_t_max': 0,
                            'lr_scheduler_warmup_steps': 32, 'batch_size_train': 256,
                            'day_trade_optimizer_step_per_trajectory': True}}
    Path('baseline.yaml').write_text(yaml.safe_dump(baseline))
    Path('launch.json').write_text('{}')
    Path('recipe.yaml').write_text(yaml.safe_dump({'full_fold_candidates': [{'runtime_overrides': {}}]}))
    Path('plan.yaml').write_text(yaml.safe_dump({'contract': 'frontier_optimizer_screen_v1',
        'epochs': 12, 'fold': 11, 'baseline_config': str(workspace / 'baseline.yaml'),
        'trials': [{'name': 'first', 'overrides': {}}, {'name': 'second', 'overrides': {}}]}))
    monkeypatch.setattr(screen, 'ROOT', code)
    monkeypatch.setattr(screen, 'verify_source_release', lambda *args: {'source_sha256': 'test'})
    monkeypatch.setattr(screen, 'verify_release_bundles', lambda *args: None)
    monkeypatch.setattr(relocated, 'renew_original_source_leases', lambda *args: None)
    calls = []
    def run(command, **kwargs):
        for key in ('--baseline-config', '--overrides', '--runtime-overrides', '--source-receipt', '--output'):
            value = Path(command[command.index(key) + 1])
            assert value.is_absolute()
            if key in ('--baseline-config', '--overrides', '--runtime-overrides'):
                assert value.is_file()
        assert kwargs['cwd'] == code
        calls.append(command)
        return SimpleNamespace(returncode=7)
    monkeypatch.setattr(screen.subprocess, 'run', run)
    monkeypatch.setattr(sys, 'argv', ['screen', '--source-receipt', 'receipt.json', '--plan', 'plan.yaml',
        '--output-root', 'result', '--accepted-launch', 'launch.json', '--dependency-root', 'deps',
        '--optimization-recipe', 'recipe.yaml'])
    assert screen.main() == 1
    receipt = json.loads(Path('result/screen.json').read_text())
    assert len(calls) == 2 and receipt['complete']
    assert [r['name'] for r in receipt['trials']] == ['first', 'second']
    assert all(r['returncode'] == 7 and not r['accepted'] for r in receipt['trials'])


def test_muon_ademamix_combination_resumes_without_missing_or_double_updates(tmp_path):
    torch.manual_seed(9)
    model = Policy()
    cfg = settings('muon', {'aux_optimizer': 'ademamix', 'aux_betas': [.9, .999, .99]})
    optimizer = create_frontier_optimizer(model, cfg, torch.device('cpu'))
    x = torch.randn(3, 5)
    update(model, optimizer, x)
    clone = deepcopy(model)
    restored = create_frontier_optimizer(clone, cfg, torch.device('cpu'))
    restored.load_state_dict(deepcopy(optimizer.state_dict()))
    update(model, optimizer, x)
    update(clone, restored, x)
    for p, q in zip(model.parameters(), clone.parameters()):
        torch.testing.assert_close(p, q, rtol=0, atol=0)


def _distributed_optimizer_worker(rank, rendezvous, output):
    import torch.distributed as dist
    torch.set_num_threads(1)
    dist.init_process_group('gloo', init_method='file://' + rendezvous, rank=rank, world_size=2)
    try:
        for name in ('muon', 'normuon', 'dion3', 'soap', 'ademamix'):
            torch.manual_seed(42)
            model = Policy()
            oracle = deepcopy(model)
            optimizer = create_frontier_optimizer(model, settings(name), torch.device('cpu'))
            reference = create_frontier_optimizer(oracle, settings(name), torch.device('cpu'))
            for step in range(3):
                for p, q in zip(model.parameters(), oracle.parameters()):
                    axis = torch.arange(p.numel(), dtype=p.dtype).reshape_as(p)
                    first = torch.sin(axis + step) + .2
                    second = torch.cos(axis * .7 + step) - .1
                    p.grad = (first if rank == 0 else second).clone()
                    dist.all_reduce(p.grad)
                    p.grad.div_(2)
                    q.grad = (first + second) / 2
                torch.nn.utils.clip_grad_norm_(model.parameters(), 1.)
                torch.nn.utils.clip_grad_norm_(oracle.parameters(), 1.)
                optimizer.step()
                reference.step()
                for p, q in zip(model.parameters(), oracle.parameters()):
                    torch.testing.assert_close(p, q, rtol=0, atol=0)
            flattened = torch.cat([p.detach().flatten() for p in model.parameters()])
            gathered = [torch.empty_like(flattened) for _ in range(2)]
            dist.all_gather(gathered, flattened)
            torch.testing.assert_close(gathered[0], gathered[1], rtol=0, atol=0)
        if rank == 0:
            Path(output).write_text('five optimizers match the globally averaged single-device oracle')
    finally:
        dist.destroy_process_group()


def test_two_rank_global_gradient_sync_precedes_nonlinear_optimizer_update(tmp_path):
    pytest.importorskip('dion')
    import torch.multiprocessing as mp
    output = tmp_path / 'accepted.txt'
    mp.spawn(_distributed_optimizer_worker, args=(str(tmp_path / 'rendezvous'), str(output)), nprocs=2, join=True)
    assert output.is_file()
