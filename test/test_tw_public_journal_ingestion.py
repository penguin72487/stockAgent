from collections import Counter
import copy
from dataclasses import asdict
import gzip
import hashlib
import json
import os

import pytest

from downloader.download_tw_public_data import EXTENDED_DATASETS
from scripts.profile_workflow_speeds import (
    CONTRACT, Evidence, Ledger, bind_source_stages, collect_source_operation,
    collect_tw_public_operations, digest, main, verify_collection, write_csv,
)

DATASET = 'twse_api_holidayschedule_holidayschedule'
SCOPE = 'native_dataset_dispatch_excludes_executor_queue_timing_log_parent_reports_and_later_audits'


def event():
    spec = EXTENDED_DATASETS[DATASET]
    return {'contract': 'tw_public_dataset_operation_timing_v1', 'dataset': DATASET,
        'dataset_kind': spec.kind, 'dataset_spec_sha256': digest(asdict(spec)),
        'request_policy_sha256': '3' * 64, 'source_endpoint_id': 'tw-public:' + DATASET,
        'source_binding_eligible': True, 'producer_pid': 123, 'producer_thread_id': 456,
        'status': 'ok', 'outcome': 'returned', 'returned_dataset': DATASET,
        'result_dataset_matches_request': True, 'observed_at_utc': '2026-10-24T01:00:00+08:00',
        'operation_timing': {'contract': 'source_operation_monotonic_v1', 'clock': 'perf_counter_ns',
            'scope': SCOPE, 'seconds': 0.5, 'invalid_clock': False, 'stages': [],
            'started_monotonic_ns': 10000000000},
        'reported_work': {'rows': 810}, 'work_units_denominator': None,
        'clock_is_source_publication_time': False, 'configuration_is_resolved_request_range': False,
        'acquisition_completeness_verified': False, 'live_readiness_verified': False}


def envelope(value=None, *, byte_array=False):
    value = event() if value is None else value
    message = '\rdownload:tw_public: 0%| 0/1 [tw-public:operation-timing] ' + json.dumps(value)
    return {'MESSAGE': list(message.encode()) if byte_array else message,
        '_PID': '123', '_SYSTEMD_UNIT': 'stockagent-tw-public-calendar-refresh.service',
        '_BOOT_ID': '1' * 32, '_SYSTEMD_INVOCATION_ID': '2' * 32}


def setup(tmp_path, entries):
    output = tmp_path / 'observation'
    output.mkdir()
    evidence = Evidence(output, max_file_bytes=1 << 20, max_total_bytes=4 << 20)
    ledger = Ledger()
    ledger.inventory.extend([{'kind': 'source_endpoint', 'id': 'tw-public:' + DATASET},
                             {'kind': 'source_endpoint', 'id': 'tw-public:feature_proxy'}])
    source = tmp_path / 'journal.jsonl'
    source.write_text(''.join(json.dumps(row) + '\n' for row in entries))
    return output, evidence, ledger, source


@pytest.mark.parametrize('byte_array', [False, True])
def test_exact_native_clock_binds_without_inventing_completed_work(tmp_path, byte_array):
    output, evidence, ledger, source = setup(tmp_path, [envelope(byte_array=byte_array)])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 1
    sample, = ledger.samples
    assert sample['seconds'] == 0.5 and sample['outcome'] == 'ok'
    assert sample['origin'] == 'recorded_tw_public_operation'
    assert sample['context']['timing_scope'] == SCOPE
    assert sample['context']['input_identity'] == 'native_dataset_dispatch'
    assert sample['work'] == {} and sample['units_per_second'] is None
    assert sample['context']['work_units_denominator'] is None
    assert sample['context']['whole_loaded_source_identity_verified'] is False
    assert bind_source_stages(ledger) == 1
    assert sample['source_endpoint_id'] == 'tw-public:' + DATASET
    assert 'timed_stages' not in ledger.inventory[1]
    captured = evidence.sources[str(source)]
    assert sample['source_sha256'] == captured['sha256']
    assert gzip.decompress((output / captured['snapshot']).read_bytes()) == source.read_bytes()


def test_overlapping_exports_and_repeated_paths_do_not_multiply_calls(tmp_path):
    output, evidence, ledger, source = setup(tmp_path, [envelope(), envelope()])
    alias = tmp_path / 'overlap.jsonl'
    alias.write_text(json.dumps(envelope()) + '\n')
    assert collect_tw_public_operations(ledger, evidence, [source, source, alias]) == 1
    assert len(ledger.samples) == 1
    journals = [row for row in ledger.inventory if row['kind'] == 'source_clock_journal']
    assert len(journals) == 2 and sum(row['duplicate_events'] for row in journals) == 2
    assert sum(row['native_events'] for row in journals) == 3
    assert bind_source_stages(ledger) == 1
    assert len(evidence.sources) == 2


def test_distinct_calls_keep_their_original_durations(tmp_path):
    second = event()
    second['operation_timing']['started_monotonic_ns'] += 1
    second['operation_timing']['seconds'] = 0.75
    output, evidence, ledger, source = setup(tmp_path, [envelope(), envelope(second)])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 2
    assert [row['seconds'] for row in ledger.samples] == [0.5, 0.75]
    assert bind_source_stages(ledger) == 1


def test_conflicting_same_call_keeps_both_clocks_without_source_coverage(tmp_path):
    second = event()
    second['operation_timing']['seconds'] = 0.75
    output, evidence, ledger, source = setup(tmp_path, [envelope(), envelope(second)])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 2
    assert [row['seconds'] for row in ledger.samples] == [0.5, 0.75]
    assert all(row['context']['native_operation_conflict'] for row in ledger.samples)
    assert bind_source_stages(ledger) == 0
    assert len(evidence.issues) == 2


@pytest.mark.parametrize('status', ['ok', 'failed', 'incomplete', 'empty', 'up_to_date', 'unsupported'])
def test_original_status_is_retained_and_is_not_acquisition_readiness(tmp_path, status):
    value = event()
    value['status'] = status
    output, evidence, ledger, source = setup(tmp_path, [envelope(value)])
    collect_tw_public_operations(ledger, evidence, [source])
    sample, = ledger.samples
    assert sample['outcome'] == status and sample['seconds'] == 0.5
    assert sample['context']['acquisition_completeness_verified'] is False
    assert sample['context']['live_readiness_verified'] is False
    assert bind_source_stages(ledger) == (0 if status == 'unsupported' else 1)


@pytest.mark.parametrize('field,value', [
    ('dataset_spec_sha256', 'a' * 64), ('dataset_kind', 'unknown'),
    ('source_endpoint_id', 'tw-public:feature_proxy'), ('returned_dataset', 'other'),
    ('result_dataset_matches_request', False), ('source_binding_eligible', False),
    ('source_binding_eligible', 1), ('outcome', 'raised')])
def test_mismatched_identity_clock_remains_unbound(tmp_path, field, value):
    record = event()
    record[field] = value
    output, evidence, ledger, source = setup(tmp_path, [envelope(record)])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 1
    assert ledger.samples[0]['seconds'] == 0.5
    assert bind_source_stages(ledger) == 0


@pytest.mark.parametrize('field,value', [
    ('seconds', -1), ('seconds', float('nan')), ('seconds', float('inf')), ('seconds', True),
    ('invalid_clock', True), ('scope', 'whole_source_e2e'), ('clock', 'UTC'),
    ('contract', 'unknown'), ('started_monotonic_ns', 1.5), ('started_monotonic_ns', -1),
    ('stages', [{'stage': 'HTTP', 'seconds': 0.1}])])
def test_invalid_native_clock_is_visible_as_gap(tmp_path, field, value):
    record = event()
    record['operation_timing'][field] = value
    output, evidence, ledger, source = setup(tmp_path, [envelope(record)])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 0
    assert ledger.samples == [] and len(evidence.issues) == 1
    assert ledger.inventory[-1]['invalid_entries'] == 1
    assert len(evidence.sources) == 1


@pytest.mark.parametrize('field,value', [
    ('_PID', '999'), ('_BOOT_ID', ''), ('_SYSTEMD_INVOCATION_ID', None),
    ('_SYSTEMD_UNIT', 'unknown.service')])
def test_invalid_journal_provenance_cannot_supply_source_time(tmp_path, field, value):
    row = envelope()
    row[field] = value
    output, evidence, ledger, source = setup(tmp_path, [row])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 0
    assert bind_source_stages(ledger) == 0 and len(evidence.issues) == 1


@pytest.mark.parametrize('field', ['clock_is_source_publication_time',
    'configuration_is_resolved_request_range', 'acquisition_completeness_verified',
    'live_readiness_verified'])
def test_wrong_observation_boundary_is_not_silently_relabelled(tmp_path, field):
    record = event()
    record[field] = True
    output, evidence, ledger, source = setup(tmp_path, [envelope(record)])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 0
    assert len(evidence.issues) == 1


def test_invalid_bytes_and_bad_json_are_frozen_not_zero_duration_samples(tmp_path):
    output, evidence, ledger, source = setup(tmp_path, [{'MESSAGE': [True]}, {'MESSAGE': 'ordinary log'}])
    with source.open('a') as stream:
        stream.write('not json\n')
    assert collect_tw_public_operations(ledger, evidence, [source]) == 0
    assert ledger.samples == [] and len(evidence.issues) == 2
    assert ledger.inventory[-1]['invalid_entries'] == 2


def test_existing_source_owner_cannot_accept_tw_native_scope():
    ledger = Ledger()
    collect_source_operation(ledger, {'provider': 'census', 'dataset': 'marts',
        'operation_timing': event()['operation_timing']}, 'source', locator='fixture', owner='economic')
    assert ledger.samples == []


def test_cli_exposes_explicit_frozen_journal_input(capsys):
    with pytest.raises(SystemExit) as caught:
        main(['--help'])
    assert caught.value.code == 0
    assert '--tw-public-operation-journal' in capsys.readouterr().out


def test_complete_reader_retains_native_raw_sha_and_summary(tmp_path):
    output, evidence, ledger, source = setup(tmp_path, [envelope(byte_array=True)])
    collect_tw_public_operations(ledger, evidence, [source])
    assert bind_source_stages(ledger) == 1
    (output / 'speed_samples.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in ledger.samples))
    write_csv(output / 'speed_summary.csv', ledger.summaries())
    (output / 'inventory.json').write_text(json.dumps({'contract': CONTRACT, 'rows': ledger.inventory}))
    (output / 'evidence_manifest.json').write_text(json.dumps({'files': list(evidence.sources.values())}))
    report = {'speed_samples': len(ledger.samples), 'summary_groups': len(ledger.summaries()),
        'sample_counts': dict(Counter(row['origin'] for row in ledger.samples)),
        'inventory_counts': dict(Counter(row['kind'] for row in ledger.inventory)),
        'tw_public_operation_ingestion_contract': 'tw_public_operation_journal_ingestion_v1'}
    report['outputs'] = {path.name: {'bytes': path.stat().st_size,
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        for path in output.iterdir() if path.is_file()}
    (output / 'collection.json').write_text(json.dumps(report))
    result = verify_collection(output)
    assert result['state'] == 'verified' and result['speed_samples_verified'] == 1
    assert result['unique_originals_verified'] == 1


def release_envelope(capsys, dataset='dgbas_release_vintages', *, byte_array=False):
    from downloader.release_archive_timing import PREFIX, SCOPE, SOURCES, _emit
    namespace = {'__name__': 'release_clock_fixture'}
    exec(compile('def collect(): pass', SOURCES[dataset], 'exec'), namespace)
    timing = {'contract': 'source_operation_monotonic_v1', 'clock': 'perf_counter_ns',
              'scope': SCOPE, 'seconds': 0.25, 'invalid_clock': False, 'stages': [],
              'started_monotonic_ns': 10000000000}
    _emit(dataset, namespace['collect'], {'dataset': dataset, 'status': 'ok'}, True, timing)
    message = capsys.readouterr().err.rstrip('\n')
    assert message.startswith(PREFIX)
    return {'MESSAGE': list(message.encode()) if byte_array else message,
            '_PID': str(os.getpid()), '_SYSTEMD_UNIT': 'stockagent-tw-public-release-archives.service',
            '_BOOT_ID': '1' * 32, '_SYSTEMD_INVOCATION_ID': '2' * 32}


def change_release(row, field, value, *, clock=False):
    from downloader.release_archive_timing import PREFIX
    result = copy.deepcopy(row)
    message = result['MESSAGE']
    payload = json.loads(message[len(PREFIX):])
    (payload['operation_timing'] if clock else payload)[field] = value
    result['MESSAGE'] = PREFIX + json.dumps(payload)
    return result


def release_setup(tmp_path, entries):
    from downloader.release_archive_timing import SOURCES
    output, evidence, ledger, source = setup(tmp_path, entries)
    ledger.inventory.extend({'kind': 'source_endpoint', 'id': 'tw-public:' + dataset} for dataset in SOURCES)
    return output, evidence, ledger, source


@pytest.mark.parametrize('dataset', [
    'cbc_overnight_official_pages', 'cbc_usdtwd_annual_pages', 'mof_macro_release_dates',
    'mof_trade_release_values', 'mof_original_release_archive', 'dgbas_release_vintages'])
@pytest.mark.parametrize('byte_array', [False, True])
def test_emitted_release_clock_uses_distinct_collect_boundary(tmp_path, capsys, dataset, byte_array):
    from downloader.release_archive_timing import CONTRACT as release_contract, SCOPE as release_scope
    entry = release_envelope(capsys, dataset, byte_array=byte_array)
    output, evidence, ledger, source = release_setup(tmp_path, [entry])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 1
    sample, = ledger.samples
    assert sample['seconds'] == 0.25 and sample['stage'] == 'producer_collect_total'
    assert sample['context']['timing_scope'] == release_scope
    assert sample['context']['input_identity'] == 'native_release_archive_collect'
    assert sample['context']['native_timing_contract'] == release_contract
    assert sample['work'] == {} and sample['units_per_second'] is None
    assert sample['context']['whole_loaded_source_identity_verified'] is False
    assert sample['context']['acquisition_completeness_verified'] is False
    assert sample['context']['live_readiness_verified'] is False
    assert bind_source_stages(ledger) == 1
    assert sample['source_endpoint_id'] == 'tw-public:' + dataset
    assert ledger.inventory[-1]['contract'] == 'tw_public_operation_journal_ingestion_v2'
    assert gzip.decompress((output / evidence.sources[str(source)]['snapshot']).read_bytes()) == source.read_bytes()


@pytest.mark.parametrize('field,value', [
    ('definition_sha256', 'a' * 64), ('producer_file', 'other.py'), ('producer_function', 'other'),
    ('source_endpoint_id', 'tw-public:feature_proxy'), ('returned_dataset', 'other'),
    ('result_dataset_matches_request', False), ('source_binding_eligible', False),
    ('source_binding_eligible', 1), ('outcome', 'raised'), ('status', 'unsupported')])
def test_release_identity_mismatch_retains_clock_without_binding(tmp_path, capsys, field, value):
    entry = change_release(release_envelope(capsys), field, value)
    output, evidence, ledger, source = release_setup(tmp_path, [entry])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 1
    assert ledger.samples[0]['seconds'] == 0.25
    assert bind_source_stages(ledger) == 0


@pytest.mark.parametrize('status', ['ok', 'failed', 'incomplete', 'empty', 'up_to_date'])
def test_release_original_status_is_preserved_without_completeness_claim(tmp_path, capsys, status):
    entry = change_release(release_envelope(capsys), 'status', status)
    output, evidence, ledger, source = release_setup(tmp_path, [entry])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 1
    assert ledger.samples[0]['outcome'] == status
    assert ledger.samples[0]['context']['acquisition_completeness_verified'] is False
    assert bind_source_stages(ledger) == 1


@pytest.mark.parametrize('field,value', [
    ('seconds', -1), ('seconds', float('nan')), ('seconds', float('inf')), ('seconds', True),
    ('invalid_clock', True), ('scope', SCOPE), ('clock', 'UTC'), ('contract', 'unknown'),
    ('started_monotonic_ns', True), ('started_monotonic_ns', -1), ('stages', [{'seconds': 0.1}])])
def test_invalid_release_clock_is_a_retained_evidence_gap(tmp_path, capsys, field, value):
    entry = change_release(release_envelope(capsys), field, value, clock=True)
    output, evidence, ledger, source = release_setup(tmp_path, [entry])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 0
    assert ledger.samples == [] and len(evidence.issues) == 1
    assert len(evidence.sources) == 1


@pytest.mark.parametrize('field', ['clock_is_source_publication_time',
    'configuration_is_resolved_request_range', 'whole_loaded_source_identity_verified',
    'acquisition_completeness_verified', 'live_readiness_verified'])
def test_release_observation_claims_cannot_be_promoted(tmp_path, capsys, field):
    entry = change_release(release_envelope(capsys), field, True)
    output, evidence, ledger, source = release_setup(tmp_path, [entry])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 0
    assert len(evidence.issues) == 1


@pytest.mark.parametrize('field,value', [('_PID', '0'), ('_BOOT_ID', ''),
    ('_SYSTEMD_INVOCATION_ID', None), ('_SYSTEMD_UNIT', 'unknown.service')])
def test_release_journal_provenance_is_required(tmp_path, capsys, field, value):
    entry = release_envelope(capsys)
    entry[field] = value
    output, evidence, ledger, source = release_setup(tmp_path, [entry])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 0
    assert bind_source_stages(ledger) == 0 and len(evidence.issues) == 1


def test_release_duplicates_and_conflicts_use_existing_journal_owner(tmp_path, capsys):
    entry = release_envelope(capsys)
    conflict = change_release(entry, 'seconds', 0.5, clock=True)
    output, evidence, ledger, source = release_setup(tmp_path, [entry, entry, conflict])
    assert collect_tw_public_operations(ledger, evidence, [source, source]) == 2
    assert [sample['seconds'] for sample in ledger.samples] == [0.25, 0.5]
    assert all(sample['context']['native_operation_conflict'] for sample in ledger.samples)
    assert bind_source_stages(ledger) == 0
    journal = ledger.inventory[-1]
    assert (journal['native_events'], journal['duplicate_events'], journal['conflicting_events']) == (3, 1, 2)


def test_release_and_dispatch_clock_definitions_remain_distinct(tmp_path, capsys):
    released = release_envelope(capsys)
    dispatched = event()
    dispatched.update(dataset='dgbas_release_vintages', producer_pid=os.getpid())
    legacy = envelope(dispatched)
    legacy['_PID'] = str(os.getpid())
    output, evidence, ledger, source = release_setup(tmp_path, [legacy, released])
    assert collect_tw_public_operations(ledger, evidence, [source]) == 2
    assert [row['stage'] for row in ledger.samples] == ['producer_call_total', 'producer_collect_total']
    assert not any(row['context']['native_operation_conflict'] for row in ledger.samples)
    assert bind_source_stages(ledger) == 1
