import pytest
import sys

from scripts.benchmark_tej_eta import benchmark
from test_tej_planning import registry as registry  # Explicitly shared pytest fixture.


def test_full_projection_benchmark_is_metadata_only_and_preserves_database(registry,tmp_path):
    root,_=registry
    original=(root/'queue.sqlite3').read_bytes()
    result=benchmark(root.parent,tmp_path/'benchmark.json',3)
    assert result['tables']==1 and result['fields']==59
    assert result['provider_requests_sent']==result['plan_blobs_read']==result['raw_value_files_read']==0
    assert not result['forbidden_actions'] and result['stable_workload_inputs']
    assert result['warm_p50_seconds']>0
    assert (root/'queue.sqlite3').read_bytes()==original
    sys.audit('socket.connect',None)  # The run-scoped guard is no longer active.


@pytest.mark.parametrize('repeats',[0,2,51])
def test_benchmark_refuses_unbounded_repetitions(tmp_path,repeats):
    with pytest.raises(ValueError):benchmark(tmp_path,tmp_path/'report.json',repeats)


def test_benchmark_refuses_to_replace_existing_evidence(tmp_path):
    output=tmp_path/'existing.json';output.touch()
    with pytest.raises(FileExistsError):benchmark(tmp_path,output,3)
