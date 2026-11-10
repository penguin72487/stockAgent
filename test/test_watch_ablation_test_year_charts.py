import json

from scripts import watch_ablation_test_year_charts as watch


def test_snapshot_tracks_completed_fold_markers_and_summary_without_reading_npz(tmp_path):
    case = tmp_path / 'case'
    fold = case / 'fold_10'
    fold.mkdir(parents=True)
    (case / 'summary.json').write_text('[]')
    before = watch.report_input_signature(tmp_path)
    (fold / 'test_backtest.npz').write_bytes(b'large payload not part of polling')
    assert watch.report_input_signature(tmp_path) == before
    (fold / 'fold_complete.json').write_text('{"status":"complete"}')
    assert watch.report_input_signature(tmp_path) != before


def test_watch_once_records_plot_failure_without_touching_training(tmp_path, monkeypatch):
    root, output = tmp_path / 'runs', tmp_path / 'reports'
    root.mkdir()
    queue = tmp_path / 'queue.json'
    queue.write_text(json.dumps({'phase': 'formal_priority_matrix'}))
    calls = []
    class Failed:
        returncode = 7
    monkeypatch.setattr(watch.subprocess, 'run', lambda argv, **kwargs: calls.append(argv) or Failed())
    monkeypatch.setattr(watch.sys, 'argv', ['watch', '--root', str(root), '--output-dir', str(output),
        '--calendar-receipt', str(tmp_path / 'calendar.json'), '--queue-status', str(queue), '--once'])
    assert watch.main() == 7
    status = json.loads((output / 'live_report_watch_status.json').read_text())
    assert status['state'] == 'retry_pending' and status['returncode'] == 7
    assert calls[0][calls[0].index('--output-dir') + 1] == str(output)
    assert not list(root.iterdir())


def test_watch_refreshes_on_each_new_completed_fold_and_persists_readback(tmp_path, monkeypatch):
    root, output = tmp_path / 'runs', tmp_path / 'reports'
    root.mkdir()
    output.mkdir()
    queue = tmp_path / 'queue.json'
    queue.write_text(json.dumps({'phase': 'formal_priority_matrix'}))
    signatures = iter(['first-fold', 'first-fold', 'second-fold', 'second-fold'])
    monkeypatch.setattr(watch, 'report_input_signature', lambda root: next(signatures))
    monkeypatch.setattr(watch.time, 'sleep', lambda value: None)
    calls = []
    class Success:
        returncode = 0

    def render(argv, **kwargs):
        calls.append(argv)
        report = output / 'reverse_test_by_test_year/alignment_status.json'
        report.parent.mkdir(exist_ok=True)
        rows = [{'variant': 'baseline', 'fold_id': 10, 'test_year': 2026}]
        if len(calls) == 2:
            rows.append({'variant': 'baseline', 'fold_id': 9, 'test_year': 2025})
            queue.write_text(json.dumps({'phase': 'queue_exited'}))
        report.write_text(json.dumps({'rendering_contract': 'canonical_ablation_risk_return_v1',
                                     'completed_observations': len(rows), 'records': rows}))
        return Success()

    monkeypatch.setattr(watch.subprocess, 'run', render)
    monkeypatch.setattr(watch.sys, 'argv', ['watch', '--root', str(root), '--output-dir', str(output),
        '--calendar-receipt', str(tmp_path / 'calendar.json'), '--queue-status', str(queue)])
    assert watch.main() == 0
    assert len(calls) == 2
    history = [json.loads(line) for line in (output / 'live_report_refreshes.jsonl').read_text().splitlines()]
    assert [row['included_observations'] for row in history] == [1, 2]
    assert history[-1]['state'] == 'current'
    assert history[-1]['included_folds'][-1] == {'variant': 'baseline', 'fold_id': 9, 'test_year': 2025}


def test_manifest_refreshes_legacy_families_and_mirrors_the_year_report(tmp_path, monkeypatch):
    from pathlib import Path
    root = tmp_path / 'runs'
    root.mkdir()
    queue = tmp_path / 'queue.json'
    queue.write_text('{"phase":"formal_priority_matrix"}')
    profile = tmp_path / 'views.json'
    profile.write_text(json.dumps({'contract':'ablation_report_views_v1','views':[
        dict(name='live',mode='test_year',split='test',prefix='reverse_test',output_subdir='live_reports'),
        dict(name='legacy_years',mode='mirror',source_view='live',prefix='reverse_test',output_subdir=''),
        dict(name='validation',mode='completed',split='val',prefix='ofat_val',output_subdir=''),
        dict(name='diagnostic',mode='completed',split='test',prefix='ofat_test_diagnostic',output_subdir='')]}))
    calls = []
    class Success:
        returncode = 0
    def render(argv, **kwargs):
        calls.append(argv)
        output = Path(argv[argv.index('--output-dir')+1])
        prefix = argv[argv.index('--prefix')+1]
        mode = argv[argv.index('--comparison-mode')+1]
        directory = output / (prefix + ('_risk_return_by_fold' if mode=='completed' else '_by_test_year'))
        directory.mkdir(parents=True)
        (directory/'all_folds_connected.png').write_bytes(b'\x89PNG\r\n\x1a\n')
        (directory/'test_year_fold_metrics.csv').write_text('retained values\n')
        status = dict(rendering_contract='canonical_ablation_risk_return_v1',completed_observations=2,
                      records=[dict(variant='baseline',fold_id=10,test_year=2026)],
                      png_files=['all_folds_connected.png'],input_sha256='same-input')
        (directory/('plot_status.json' if mode=='completed' else 'alignment_status.json')).write_text(json.dumps(status))
        return Success()
    monkeypatch.setattr(watch.subprocess,'run',render)
    monkeypatch.setattr(watch.sys,'argv',['watch','--root',str(root),'--output-dir',str(root/'live_reports'),
        '--calendar-receipt',str(tmp_path/'calendar.json'),'--queue-status',str(queue),
        '--report-spec',str(profile),'--once'])
    assert watch.main()==0
    assert len(calls)==3
    assert {(argv[argv.index('--split')+1],argv[argv.index('--prefix')+1]) for argv in calls} == {
        ('test','reverse_test'),('val','ofat_val'),('test','ofat_test_diagnostic')}
    assert (root/'reverse_test_by_test_year/all_folds_connected.png').read_bytes() == (
        root/'live_reports/reverse_test_by_test_year/all_folds_connected.png').read_bytes()
    receipt=json.loads((root/'live_reports/live_report_watch_status.json').read_text())
    assert set(receipt['views'])=={'live','legacy_years','validation','diagnostic'}
    assert all(view['state']=='current' for view in receipt['views'].values())
