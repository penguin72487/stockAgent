"""Offline audit chains resume from durable reports without duplicating proof work."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import audit_news_cnyes_retained_missing as audit_module
from scripts import audit_news_cnyes_retained_missing_chain as chain
from test_cnyes_news_bulk import cfg  # noqa: F401
from test_news_cached_replay import _request_scoped_history_fixture


def test_chain_cli_runs_outside_repo_without_pythonpath(tmp_path):
    script = Path(__file__).resolve().parents[1] / 'scripts/audit_news_cnyes_retained_missing_chain.py'
    env = dict(os.environ)
    env.pop('PYTHONPATH', None)
    result = subprocess.run([sys.executable, str(script), '--help'], cwd=tmp_path, env=env,
                            capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    assert '--check-only' in result.stdout


def _fixture(cfg, tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    registry, _, _, _, artifacts, _ = _request_scoped_history_fixture(cfg, source, monkeypatch)
    monkeypatch.setattr(audit_module, 'load_config', lambda _: ({'root': str(source)}, registry, Path.cwd()))
    prior = tmp_path / 'prior.json'
    prior.write_text('{}')
    return source, artifacts, prior, tmp_path / 'reports'


def _run(prior, reports, **kwargs):
    return chain.run_chain(Path('unused.json'), [prior], reports, observation_chunk_size=2,
                           max_responses=1, max_seconds=300, **kwargs)


def test_chain_restart_retains_receipts_and_does_not_reprocess_previous_candidate(cfg, tmp_path, monkeypatch):
    source, artifacts, prior, reports = _fixture(cfg, tmp_path, monkeypatch)
    before = audit_module._stamp(source / 'queue.sqlite3')
    first = _run(prior, reports, max_reports=1)
    assert first['state'] == 'continuation_available'
    assert first['observation_keys_scanned'] == 2 and first['reports'] == 1
    first_path = reports / 'chunk-000001.json'
    first_digest = hashlib.sha256(first_path.read_bytes()).hexdigest()
    status_stamp = audit_module._stamp(reports / 'status.json')
    assert _run(prior, reports, check_only=True) == first
    assert audit_module._stamp(reports / 'status.json') == status_stamp
    assert not (reports / 'chunk-000002.json').exists()
    second = _run(prior, reports, max_reports=1)
    assert second['state'] == 'scan_finished_no_reported_issues'
    assert second['observation_keys_scanned'] == 2 and second['reports'] == 2
    second_path = reports / 'chunk-000002.json'
    r1, r2 = (json.loads(p.read_bytes()) for p in (first_path, second_path))
    assert r2['resumed_scan_audit_sha256'] == first_digest
    assert {o['source_artifact_path'] for r in (r1, r2) for o in r['target_outcomes']} == {
        a['parquet_path'] for a in artifacts}
    assert len(r1['target_outcomes']) == len(r2['target_outcomes']) == 1
    again = _run(prior, reports, max_reports=1)
    assert again == second
    assert hashlib.sha256(first_path.read_bytes()).hexdigest() == first_digest
    assert audit_module._stamp(source / 'queue.sqlite3') == before
    assert second['publisher_requests'] == second['production_queue_writes'] == 0
    assert second['all_history_complete'] is False


def test_chain_accepts_resizing_only_at_a_completed_chunk_boundary(cfg, tmp_path, monkeypatch):
    _, _, prior, reports = _fixture(cfg, tmp_path, monkeypatch)
    result = chain.run_chain(Path('unused.json'), [prior], reports, observation_chunk_size=1,
                             max_responses=1, max_seconds=300, max_reports=1)
    assert result['state'] == 'continuation_available'
    result = chain.run_chain(Path('unused.json'), [prior], reports, observation_chunk_size=2,
                             max_responses=1, max_seconds=300)
    assert result['state'] == 'scan_finished_no_reported_issues' and result['reports'] == 2
    partial_reports = tmp_path / 'partial-reports'
    _run(prior, partial_reports, max_reports=1)
    with pytest.raises(ValueError, match='scan_scope_changed'):
        chain.run_chain(Path('unused.json'), [prior], partial_reports, observation_chunk_size=1,
                        max_responses=1, max_seconds=300)


def test_chain_detects_report_tampering_and_changed_prior(cfg, tmp_path, monkeypatch):
    _, _, prior, reports = _fixture(cfg, tmp_path, monkeypatch)
    _run(prior, reports, max_reports=1)
    prior.write_text('{"changed":true}')
    with pytest.raises(ValueError, match='scan_input_prior_changed'):
        _run(prior, reports, max_reports=1)
    prior.write_text('{}')
    path = reports / 'chunk-000001.json'
    report = json.loads(path.read_bytes())
    report['observation_scan']['candidate_offset'] += 1
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError, match='scan_checkpoint_invalid'):
        _run(prior, reports, max_reports=1)


def test_reviewed_historical_chain_is_read_only_and_preserves_report_integrity(cfg, tmp_path, monkeypatch):
    source, _, prior, reports = _fixture(cfg, tmp_path, monkeypatch)
    real_sha = audit_module.sha256_file
    historical = chain._REVIEWED_HISTORICAL_PARSER

    def prior_parser_hash(path):
        relative = str(Path(path).resolve().relative_to(Path.cwd())) if Path(path).resolve().is_relative_to(Path.cwd()) else ''
        return historical[relative] if relative in historical else real_sha(path)

    # Build a synthetic old-identity report through the ordinary audit path.
    with monkeypatch.context() as patch:
        patch.setattr(audit_module, 'sha256_file', prior_parser_hash)
        patch.setattr(chain, 'sha256_file', prior_parser_hash)
        old = _run(prior, reports)
    old_snapshot = json.loads((reports / 'chunk-000001.json').read_bytes())['observation_scan']['snapshot']
    # A new empty WAL identity alone does not change the immutable database.
    (source / 'queue.sqlite3-wal').write_bytes(b'')
    assert chain._snapshot(source) != old_snapshot
    stamps = {p.name: audit_module._stamp(p) for p in reports.iterdir()}
    readback = _run(prior, reports, check_only=True)
    assert readback['state'] == old['state'] and readback['reports'] == old['reports']
    assert readback['historical_reader_readback'] is True and readback['resumable_with_current_reader'] is False
    assert readback['empty_sidecar_identity_changed'] is True
    assert {p.name: audit_module._stamp(p) for p in reports.iterdir()} == stamps
    with (source / 'owner.lock').open('r') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match='scan_source_owner_busy'):
            _run(prior, reports, check_only=True)
    reads = 0
    def changing_reader(path):
        nonlocal reads
        if Path(path).resolve() == (Path.cwd() / 'downloader/news_fulltext_cnyes_history.py').resolve():
            reads += 1
            if reads > 1:
                return '0' * 64
        return real_sha(path)
    with monkeypatch.context() as patch:
        patch.setattr(chain, 'sha256_file', changing_reader)
        with pytest.raises(ValueError, match='scan_reader_changed'):
            _run(prior, reports, check_only=True)
    with pytest.raises(ValueError):
        _run(prior, reports, max_reports=1)
    plan = _run(prior, reports, check_only=True, plan_mismatches=True)
    assert plan['historical_reader_readback'] is True and plan['resumable_with_current_reader'] is False
    assert plan['production_queue_writes'] == plan['publisher_requests'] == 0
    path = reports / 'chunk-000001.json'
    report = json.loads(path.read_bytes())
    report['observation_scan']['candidate_offset'] += 1
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError, match='scan_checkpoint_invalid'):
        _run(prior, reports, check_only=True)


def test_historical_partial_chain_cannot_claim_current_reader_resume(cfg, tmp_path, monkeypatch):
    _, _, prior, reports = _fixture(cfg, tmp_path, monkeypatch)
    real_sha = audit_module.sha256_file
    historical = chain._REVIEWED_HISTORICAL_PARSER
    def prior_parser_hash(path):
        resolved = Path(path).resolve()
        relative = str(resolved.relative_to(Path.cwd())) if resolved.is_relative_to(Path.cwd()) else ''
        return historical[relative] if relative in historical else real_sha(path)
    with monkeypatch.context() as patch:
        patch.setattr(audit_module, 'sha256_file', prior_parser_hash)
        patch.setattr(chain, 'sha256_file', prior_parser_hash)
        assert _run(prior, reports, max_reports=1)['state'] == 'continuation_available'
    readback = _run(prior, reports, check_only=True)
    assert readback['state'] == 'historical_partial_read_only'
    assert readback['historical_reader_readback'] and not readback['resumable_with_current_reader']
    with pytest.raises(ValueError, match='scan_not_finished'):
        _run(prior, reports, check_only=True, plan_mismatches=True)
    with pytest.raises(ValueError):
        _run(prior, reports, max_reports=1)


def test_historical_readback_rejects_changed_database_or_nonempty_wal(cfg, tmp_path, monkeypatch):
    source, _, prior, reports = _fixture(cfg, tmp_path, monkeypatch)
    real_sha = audit_module.sha256_file
    historical = chain._REVIEWED_HISTORICAL_PARSER
    def prior_parser_hash(path):
        resolved = Path(path).resolve()
        relative = str(resolved.relative_to(Path.cwd())) if resolved.is_relative_to(Path.cwd()) else ''
        return historical[relative] if relative in historical else real_sha(path)
    with monkeypatch.context() as patch:
        patch.setattr(audit_module, 'sha256_file', prior_parser_hash)
        patch.setattr(chain, 'sha256_file', prior_parser_hash)
        _run(prior, reports)
    wal = source / 'queue.sqlite3-wal'
    wal.write_bytes(b'uncommitted')
    with pytest.raises(ValueError, match='immutable_snapshot_requires_empty_wal_and_journal'):
        _run(prior, reports, check_only=True)
    wal.write_bytes(b'')
    (source / 'queue.sqlite3').touch()
    with pytest.raises(ValueError, match='scan_snapshot_changed'):
        _run(prior, reports, check_only=True)


def test_chain_retains_failures_and_continues_other_originals(cfg, tmp_path, monkeypatch):
    source, artifacts, prior, reports = _fixture(cfg, tmp_path, monkeypatch)
    for artifact in artifacts:
        (source / artifact['receipt_path']).write_text('invalid receipt')
    result = _run(prior, reports)
    assert result['state'] == 'scan_finished_with_issues'
    assert result['reports'] == 2 and result['source_envelope_failures'] == 2
    assert result['all_history_complete'] is False


def test_chain_recovers_a_report_published_before_status_write_failed(cfg, tmp_path, monkeypatch):
    _, _, prior, reports = _fixture(cfg, tmp_path, monkeypatch)
    first = _run(prior, reports, max_reports=1)
    assert first['reports'] == 1
    with monkeypatch.context() as patch:
        def interrupted(*args, **kwargs):
            raise OSError('simulated_status_write_failure')
        patch.setattr(chain, 'atomic_write_bytes', interrupted)
        with pytest.raises(OSError, match='simulated_status_write_failure'):
            _run(prior, reports, max_reports=1)
    assert (reports / 'chunk-000002.json').exists()
    status_path = reports / 'status.json'
    original_status = json.loads(status_path.read_text())
    assert original_status['reports'] == 1
    status_path.write_text(json.dumps({**original_status, 'last_report_sha256': '0' * 64}))
    with pytest.raises(ValueError, match='scan_status_digest_mismatch'):
        _run(prior, reports, max_reports=1)
    status_path.write_text(json.dumps(original_status))
    recovered = _run(prior, reports, max_reports=1)
    assert recovered['state'] == 'scan_finished_no_reported_issues'
    assert recovered['reports'] == 2
    assert not (reports / 'chunk-000003.json').exists()


def test_chain_requires_exclusive_output_and_cannot_write_under_source(cfg, tmp_path, monkeypatch):
    source, _, prior, reports = _fixture(cfg, tmp_path, monkeypatch)
    with pytest.raises(ValueError, match='scan_output_not_found'):
        _run(prior, reports, check_only=True)
    assert not reports.exists()
    with pytest.raises(ValueError, match='scan_output_inside_source'):
        _run(prior, source / 'reports')
    reports.mkdir()
    lock = (reports / '.scan.lock').open('w')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match='scan_already_running'):
            _run(prior, reports)
    finally:
        lock.close()


def test_chain_rejects_gaps_in_existing_reports(cfg, tmp_path, monkeypatch):
    _, _, prior, reports = _fixture(cfg, tmp_path, monkeypatch)
    _run(prior, reports)
    (reports / 'chunk-000001.json').unlink()
    with pytest.raises(ValueError, match='scan_report_gap'):
        _run(prior, reports)


def _mismatch_fixture(cfg, tmp_path, monkeypatch, *, exact=True, chunk_size=2, unrelated_observations=0):
    from downloader.news_fulltext_bulk import cnyes_url
    from downloader.news_fulltext_cnyes_history import initialize
    from downloader.news_fulltext_queue import NewsQueue
    from downloader.news_fulltext_replay import ensure_replay_schema
    from test_cnyes_news_bulk import BASE, payload, registry_http, response
    from test_news_cached_replay import _insert_crosscheck_observation, retained

    source = tmp_path / 'source'
    source.mkdir()
    registry, _ = registry_http(cfg, source)
    queue = NewsQueue(source, registry)
    data = payload(ids=(1,))
    try:
        path = retained(queue, data)
        initialize(queue)
        ensure_replay_schema(queue)
        artifact = {'kind': 'canonical_quarantine', 'parquet_path': 'quarantine/' + path.name,
                    'receipt_path': 'quarantine/' + path.name + '.receipt.json'}
        with queue.db:
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
            queue.db.execute("UPDATE articles SET state='source_article_empty_template'")
            if exact:
                _insert_crosscheck_observation(queue, data, response(data).url, artifact)
            wrong = _insert_crosscheck_observation(queue, data,
                cnyes_url(BASE, start=1388534400, end=1388534500), artifact)
            queue.db.executemany('INSERT INTO cnyes_history_page_observations VALUES (?,?,?,?,?,?,?,?)',
                ((f'noise-{number:06}', 1, '0' * 64, 1, 'a' * 64, 'b' * 64, f'{number:064x}',
                  '{"identities":[]}') for number in range(unrelated_observations)))
    finally:
        queue.close()
    monkeypatch.setattr(audit_module, 'load_config', lambda _: ({'root': str(source)}, registry, Path.cwd()))
    prior, reports = tmp_path / 'prior.json', tmp_path / 'reports'
    prior.write_text('{}')
    result = chain.run_chain(Path('unused.json'), [prior], reports, observation_chunk_size=chunk_size)
    assert result['state'] == 'scan_finished_with_issues'
    return source, prior, reports, artifact, wrong


def _mismatch_plan(prior, reports):
    return chain.run_chain(Path('unused.json'), [prior], reports, check_only=True, plan_mismatches=True)


def test_historical_mismatch_plan_rechecks_witnesses_without_redownloading(cfg, tmp_path, monkeypatch):
    real_sha = audit_module.sha256_file
    historical = chain._REVIEWED_HISTORICAL_PARSER
    def prior_parser_hash(path):
        resolved = Path(path).resolve()
        relative = str(resolved.relative_to(Path.cwd())) if resolved.is_relative_to(Path.cwd()) else ''
        return historical[relative] if relative in historical else real_sha(path)
    with monkeypatch.context() as patch:
        patch.setattr(audit_module, 'sha256_file', prior_parser_hash)
        patch.setattr(chain, 'sha256_file', prior_parser_hash)
        source, prior, reports, artifact, wrong = _mismatch_fixture(cfg, tmp_path, monkeypatch)
    (source / 'queue.sqlite3-wal').write_bytes(b'')
    def forbidden(*args, **kwargs):
        raise AssertionError('historical plan must not decode source originals')
    monkeypatch.setattr(audit_module, '_source_proof', forbidden)
    plan = _mismatch_plan(prior, reports)
    assert plan['historical_reader_readback'] and not plan['resumable_with_current_reader']
    assert plan['unchanged_mismatch_envelopes'] == 1 and plan['needs_evidence_envelopes'] == 0
    assert plan['unchanged_mismatches'][0]['observation_sha256'] == [wrong]
    assert plan['unchanged_mismatches'][0]['source_artifact_path'] == artifact['parquet_path']
    assert plan['publisher_requests'] == plan['production_queue_writes'] == plan['original_contents_redecoded'] == 0


def test_mismatch_plan_reuses_same_report_witness_without_resolving_issue(cfg, tmp_path, monkeypatch):
    source, prior, reports, artifact, wrong = _mismatch_fixture(cfg, tmp_path, monkeypatch)
    before = {p: audit_module._stamp(p) for p in (source / 'queue.sqlite3', reports / 'status.json')}
    def forbidden(*args, **kwargs):
        raise AssertionError('plan must not decode source originals')
    monkeypatch.setattr(audit_module, '_source_proof', forbidden)
    plan = _mismatch_plan(prior, reports)
    assert plan['state'] == 'unresolved_evidence_plan' and not plan['all_history_complete']
    assert plan['prior_exact_pairs'] == plan['candidate_envelopes'] == plan['unchanged_mismatch_envelopes'] == 1
    assert plan['needs_evidence_envelopes'] == 0 and plan['pending_response_sha256'] == []
    entry = plan['unchanged_mismatches'][0]
    assert entry['source_artifact_path'] == artifact['parquet_path'] and entry['observation_sha256'] == [wrong]
    assert entry['proof_reports'] == [{'path': str((reports / 'chunk-000001.json').resolve()),
        'sha256': hashlib.sha256((reports / 'chunk-000001.json').read_bytes()).hexdigest()}]
    assert plan['publisher_requests'] == plan['production_queue_writes'] == plan['original_contents_redecoded'] == 0
    assert {p: audit_module._stamp(p) for p in before} == before


@pytest.mark.parametrize('exact,chunk_size', [(False, 2), (True, 1)])
def test_mismatch_plan_reuses_same_report_source_only_without_decoding(cfg, tmp_path, monkeypatch, exact, chunk_size):
    source, prior, reports, artifact, wrong = _mismatch_fixture(
        cfg, tmp_path, monkeypatch, exact=exact, chunk_size=chunk_size)
    def forbidden(*args, **kwargs):
        raise AssertionError('plan must not decode source originals')
    monkeypatch.setattr(audit_module, '_source_proof', forbidden)
    plan = _mismatch_plan(prior, reports)
    assert plan['contract'] == 'cnyes_unchanged_mismatch_plan_v2'
    assert plan['unchanged_mismatch_envelopes'] == 1 and plan['needs_evidence_envelopes'] == 0
    assert plan['unchanged_mismatches'][0]['observation_sha256'] == [wrong]
    assert plan['unchanged_mismatches'][0]['witness_types'] == ['same_report_source_only']
    assert plan['all_history_complete'] is False and plan['original_contents_redecoded'] == 0
    assert (source / artifact['parquet_path']).is_file()


@pytest.mark.parametrize('exact,chunk_size', [(False, 2), (True, 1)])
def test_mismatch_plan_does_not_borrow_witness_from_other_report(cfg, tmp_path, monkeypatch, exact, chunk_size):
    original = audit_module._source_proof
    def without_source_only(*args, **kwargs):
        proof = original(*args, **kwargs)
        proof['source_only_target_outcomes'] = []
        return proof
    monkeypatch.setattr(audit_module, '_source_proof', without_source_only)
    _, prior, reports, _, wrong = _mismatch_fixture(cfg, tmp_path, monkeypatch, exact=exact, chunk_size=chunk_size)
    plan = _mismatch_plan(prior, reports)
    assert plan['unchanged_mismatch_envelopes'] == 0 and plan['needs_evidence_envelopes'] == 1
    assert plan['needs_evidence'][0]['observation_sha256'] == [wrong]
    assert len(plan['pending_response_sha256']) == 1


@pytest.mark.parametrize('field', ['original_bytes', 'receipt_response'])
def test_source_only_witness_requires_matching_current_original_and_receipt(cfg, tmp_path, monkeypatch, field):
    source, prior, reports, artifact, wrong = _mismatch_fixture(cfg, tmp_path, monkeypatch, exact=False)
    if field == 'original_bytes':
        path = source / artifact['parquet_path']
        path.write_bytes(path.read_bytes() + b' ')
    else:
        path = source / artifact['receipt_path']
        receipt = json.loads(path.read_text())
        receipt['response_sha256'] = '0' * 64
        path.write_text(json.dumps(receipt))
    plan = _mismatch_plan(prior, reports)
    assert plan['unchanged_mismatch_envelopes'] == 0 and plan['needs_evidence_envelopes'] == 1
    assert plan['needs_evidence'][0]['observation_sha256'] == [wrong]


def test_source_only_witness_rejects_original_change_during_scan(cfg, tmp_path, monkeypatch):
    source, prior, reports, artifact, _ = _mismatch_fixture(cfg, tmp_path, monkeypatch, exact=False)
    original = audit_module._candidate_index
    def change_during_scan(*args, **kwargs):
        result = original(*args, **kwargs)
        receipt = source / artifact['receipt_path']
        receipt.write_bytes(receipt.read_bytes() + b' ')
        return result
    monkeypatch.setattr(audit_module, '_candidate_index', change_during_scan)
    with pytest.raises(ValueError, match='scan_original_changed_during_plan'):
        _mismatch_plan(prior, reports)


@pytest.mark.parametrize('mutation', ['wrong_target', 'same_request', 'wrong_parquet_hash', 'missing_target'])
def test_source_only_witness_does_not_promote_incomplete_or_conflicting_proof(cfg, tmp_path, monkeypatch, mutation):
    from downloader.news_fulltext_bulk import cnyes_url
    from test_cnyes_news_bulk import BASE

    _, prior, reports, _, wrong = _mismatch_fixture(cfg, tmp_path, monkeypatch, exact=False)
    original = chain._read_json
    def altered(path):
        report, digest = original(path)
        if path.name.startswith('chunk-') and report.get('source_only_target_outcomes'):
            item = report['source_only_target_outcomes'][0]
            if mutation == 'wrong_target':
                item['news_id'] += 1
            elif mutation == 'same_request':
                request = cnyes_url(BASE, start=1388534400, end=1388534500)
                item['source_request_sha256'] = hashlib.sha256(request.encode()).hexdigest()
            elif mutation == 'wrong_parquet_hash':
                item['parquet_sha256'] = '0' * 64
            else:
                report['source_only_target_outcomes'] = []
        return report, digest
    monkeypatch.setattr(chain, '_read_json', altered)
    plan = _mismatch_plan(prior, reports)
    assert plan['unchanged_mismatch_envelopes'] == 0 and plan['needs_evidence_envelopes'] == 1
    assert plan['needs_evidence'][0]['observation_sha256'] == [wrong]


@pytest.mark.parametrize('field', ['parquet_path', 'receipt_path'])
def test_mismatch_plan_does_not_reuse_when_original_or_receipt_changes(cfg, tmp_path, monkeypatch, field):
    source, prior, reports, artifact, _ = _mismatch_fixture(cfg, tmp_path, monkeypatch)
    path = source / artifact[field]
    path.write_bytes(path.read_bytes() + b' ')
    plan = _mismatch_plan(prior, reports)
    assert plan['prior_exact_pairs'] == 0
    assert plan['unchanged_mismatch_envelopes'] == 0 and plan['needs_evidence_envelopes'] == 1


def test_mismatch_plan_rejects_proof_changed_during_scan(cfg, tmp_path, monkeypatch):
    source, prior, reports, artifact, _ = _mismatch_fixture(cfg, tmp_path, monkeypatch)
    original = audit_module._candidate_index
    def change_during_scan(*args, **kwargs):
        result = original(*args, **kwargs)
        receipt = source / artifact['receipt_path']
        receipt.write_bytes(receipt.read_bytes() + b' ')
        return result
    monkeypatch.setattr(audit_module, '_candidate_index', change_during_scan)
    with pytest.raises(ValueError, match='scan_prior_proofs_changed_during_plan'):
        _mismatch_plan(prior, reports)


def test_mismatch_plan_requires_complete_chain_and_read_only_mode(cfg, tmp_path, monkeypatch):
    _, _, prior, reports = _fixture(cfg, tmp_path, monkeypatch)
    _run(prior, reports, max_reports=1)
    with pytest.raises(ValueError, match='scan_not_finished'):
        _mismatch_plan(prior, reports)
    with pytest.raises(ValueError, match='invalid_scan_chain_bounds'):
        chain.run_chain(Path('unused.json'), [prior], reports, plan_mismatches=True)


def test_mismatch_plan_scans_past_the_100000_observation_page_boundary(cfg, tmp_path, monkeypatch):
    _, prior, reports, _, wrong = _mismatch_fixture(cfg, tmp_path, monkeypatch,
        chunk_size=100_000, unrelated_observations=100_001)
    plan = _mismatch_plan(prior, reports)
    assert plan['candidate_envelopes'] == plan['unchanged_mismatch_envelopes'] == 1
    assert plan['unchanged_mismatches'][0]['observation_sha256'] == [wrong]


def test_mismatch_plan_reports_shared_response_selection_collision(cfg, tmp_path, monkeypatch):
    from downloader.news_fulltext_bulk import cnyes_url
    from downloader.news_fulltext_queue import NewsQueue
    from test_cnyes_news_bulk import BASE
    from test_news_cached_replay import _insert_crosscheck_observation

    source = tmp_path / 'source'
    source.mkdir()
    registry, data, _, _, artifacts, _ = _request_scoped_history_fixture(cfg, source, monkeypatch)
    queue = NewsQueue(source, registry)
    try:
        with queue.db:
            for index, artifact in enumerate(artifacts):
                _insert_crosscheck_observation(queue, data,
                    cnyes_url(BASE, start=1388534600 + index * 200, end=1388534700 + index * 200), artifact)
    finally:
        queue.close()
    monkeypatch.setattr(audit_module, 'load_config', lambda _: ({'root': str(source)}, registry, Path.cwd()))
    prior, reports = tmp_path / 'prior.json', tmp_path / 'reports'
    prior.write_text('{}')
    result = chain.run_chain(Path('unused.json'), [prior], reports, observation_chunk_size=4, max_responses=2)
    assert result['state'] == 'scan_finished_with_issues'
    receipt = source / artifacts[1]['receipt_path']
    receipt.write_bytes(receipt.read_bytes() + b' ')
    plan = _mismatch_plan(prior, reports)
    assert plan['candidate_envelopes'] == 2
    assert plan['unchanged_mismatch_envelopes'] == plan['needs_evidence_envelopes'] == 1
    assert len(plan['pending_response_sha256']) == 1
    assert plan['unchanged_sharing_pending_response_sha'] == 1
