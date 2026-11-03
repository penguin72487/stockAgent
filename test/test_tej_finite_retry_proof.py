"""The finite exception cannot convert an unknown Preview into a retry."""
from contextlib import closing
import json

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_finite_retry import _retry_proof
from downloader.tej_history import connect
from downloader.tej_metadata_recovery import metadata_recovery_policy, recover_failed_metadata
from test_tej_history import registry
from test_tej_metadata_recovery import CONFIG, FreshBridge, failed_metadata


@pytest.mark.parametrize('fault', [None, 'retry_attempt', 'outcome', 'isolation', 'audit'])
def test_current_metadata_retry_requires_original_unsent_and_isolation_evidence(failed_metadata, fault):
    _, root, task, _, outcome, _, _ = failed_metadata
    result = recover_failed_metadata(root, FreshBridge(), policy=metadata_recovery_policy(CONFIG))
    assert result['state'] == 'metadata_retry_scheduled'
    audit_path = root / result['audit_path']
    audit = json.loads(audit_path.read_text())
    isolation_path = root / audit['isolation_audit_path']
    if fault == 'retry_attempt':
        with closing(connect(root)) as con, con:
            con.execute("UPDATE desktop_retry_windows SET attempt_id='foreign'")
    elif fault == 'outcome':
        body = json.loads(outcome.read_text())
        body['market_data_query_submission_possible'] = True
        atomic_write_json(outcome, body)
    elif fault == 'isolation':
        body = json.loads(isolation_path.read_text())
        body['source_axes_adopted'] = True
        atomic_write_json(isolation_path, body)
    elif fault == 'audit':
        audit['data_query_repeated'] = True
        atomic_write_json(audit_path, audit)
    with closing(connect(root)) as con:
        if fault is None:
            assert _retry_proof(con, root, task['table_id'])['attempt_id'] == result['attempt_id']
        else:
            with pytest.raises(ValueError):
                _retry_proof(con, root, task['table_id'])
