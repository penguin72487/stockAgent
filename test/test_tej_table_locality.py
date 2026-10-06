from contextlib import closing

import pytest

from downloader.tej_history import _ready_task, configure_runtime_policy, connect


@pytest.fixture
def queue(tmp_path):
    root=tmp_path/'data'
    with closing(connect(root)) as con,con:
        for task,table,priority,kind in [('a1','a',100,'download'),('b1','b',100,'download')]:
            con.execute('INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json) VALUES(?,?,?,?,?,?)',
                        (task,table,kind,'P1',priority,'{}'))
        for key,value in [('scheduler_last_download_table','a'),('scheduler_table_consecutive_downloads','1')]:
            con.execute('INSERT INTO meta VALUES (?,?)',(key,value))
    return root


def test_default_rotation_unchanged_but_finite_locality_prefers_exact_priority_tie(queue):
    with closing(connect(queue)) as con:
        assert _ready_task(con,'2099',None,{})['task_id']=='b1'
        assert _ready_task(con,'2099',None,{'download_table_locality_burst':4})['task_id']=='a1'
        con.execute("UPDATE meta SET value='4' WHERE key='scheduler_table_consecutive_downloads'")
        assert _ready_task(con,'2099',None,{'download_table_locality_burst':4})['task_id']=='b1'


def test_higher_priority_always_preempts_locality(queue):
    with closing(connect(queue)) as con:
        con.execute("UPDATE tasks SET priority=99 WHERE task_id='b1'")
        assert _ready_task(con,'2099',None,{'download_table_locality_burst':4})['task_id']=='b1'


def test_due_discovery_still_wins_before_table_locality(queue):
    with closing(connect(queue)) as con:
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json) VALUES ('discovery','c','discover','P1',100,'{}')")
        con.execute("INSERT INTO meta VALUES ('scheduler_consecutive_downloads','4')")
        assert _ready_task(con,'2099',None,{'download_table_locality_burst':4,'download_burst':4})['task_id']=='discovery'


@pytest.mark.parametrize('barrier',['cooldown','source_isolation','retry_window'])
def test_table_locality_never_bypasses_table_barriers(queue,barrier):
    with closing(connect(queue)) as con:
        if barrier=='cooldown':con.execute("UPDATE tasks SET next_attempt_at_utc='9999' WHERE task_id='a1'")
        elif barrier=='source_isolation':
            con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,last_error_code) VALUES ('blocked','a','download','P1',100,'{}','blocked','source_validation_failed_deferred')")
        else:
            con.execute("INSERT INTO desktop_retry_windows VALUES ('a','a1','attempt',1,'9999','local','audit')")
        assert _ready_task(con,'2099',None,{'download_table_locality_burst':4})['task_id']=='b1'


@pytest.mark.parametrize('invalid',[0,9,False,1.5,'4'])
def test_locality_policy_is_finite_integer_and_default_preserved(queue,invalid):
    with pytest.raises(ValueError):configure_runtime_policy(queue,{'download_table_locality_burst':invalid})
    assert configure_runtime_policy(queue,{})['download_table_locality_burst']==1


def test_exact_kind_and_table_selector_do_not_change_to_another_table(queue):
    with closing(connect(queue)) as con:
        assert _ready_task(con,'2099','download',{'download_table_locality_burst':4},'b')['task_id']=='b1'
