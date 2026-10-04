"""Acceptance must retain exact source, dependency and failed-attempt evidence."""
from copy import deepcopy

import pytest

from scripts.verify_control_plane_pilot import accepted_snapshot


def pilot():
    source = {'receipt_sha256': 'a' * 64, 'source_sha256': 'b' * 64,
              'verified_file_count': 915, 'source_files_verified': True}
    states = [('local-release', 1, 'penguin-control-pilot', 'succeeded'),
              ('remote-release', 1, 'vast-control-pilot', 'succeeded'),
              ('recovered-release', 1, 'vast-control-pilot', 'expired'),
              ('recovered-release', 2, 'penguin-control-pilot', 'succeeded')]
    snapshot = {'nodes': [{'node_id': n} for n in ('penguin-control-pilot', 'vast-control-pilot')],
                'jobs': [{'key': n, 'state': 'succeeded', 'result': deepcopy(source)} for n in
                         ('local-release', 'remote-release', 'recovered-release')],
                'dependencies': [{'job_key': 'remote-release', 'dependency_key': 'local-release'}],
                'attempts': [dict(zip(('job_key', 'attempt', 'node_id', 'state'), row)) for row in states]}
    return snapshot, source


def test_exact_cross_node_crash_evidence():
    snapshot, source = pilot()
    assert accepted_snapshot(snapshot, source)['remote_crash_recovered'] is True


@pytest.mark.parametrize('field,value', [('receipt_sha256', 'c' * 64), ('source_sha256', 'd' * 64),
                                         ('verified_file_count', 914), ('source_files_verified', False),
                                         ('data_publication', True), ('loaded_service_revision_verified', True)])
def test_inexact_or_overstated_source_completion_rejected(field, value):
    snapshot, source = pilot()
    snapshot['jobs'][1]['result'][field] = value
    with pytest.raises(ValueError):
        accepted_snapshot(snapshot, source)


@pytest.mark.parametrize('table', ['nodes', 'jobs', 'dependencies', 'attempts'])
def test_missing_history_rejected(table):
    snapshot, source = pilot()
    snapshot[table].pop()
    with pytest.raises(ValueError):
        accepted_snapshot(snapshot, source)


def test_expired_crash_cannot_be_relabelled_success():
    snapshot, source = pilot()
    snapshot['attempts'][2]['state'] = 'succeeded'
    with pytest.raises(ValueError):
        accepted_snapshot(snapshot, source)
