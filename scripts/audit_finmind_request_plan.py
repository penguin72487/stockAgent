"""Replay FinMind scheduling on a private SQLite backup, with zero API calls."""

from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
import tempfile

from downloader.artifact_io import atomic_write_json
from downloader import download_finmind_sponsor as sponsor


def counts(conn):
    return {state: count for state, count in conn.execute('SELECT state,count(*) FROM tasks GROUP BY state')}


def main():
    repo = Path(__file__).resolve().parents[1]
    source = repo / 'data_finmind/sponsor/queue.sqlite3'
    now = datetime.now(UTC)
    with tempfile.TemporaryDirectory(prefix='finmind-request-plan-') as temporary:
        path = Path(temporary) / 'queue.sqlite3'
        with sqlite3.connect(f'file:{source}?mode=ro', uri=True) as live, sqlite3.connect(path) as clone:
            if live.execute('PRAGMA user_version').fetchone()[0] != sponsor.QUERY_SHAPE_VERSION:
                raise RuntimeError('audit only supports already migrated queues')
            live.backup(clone)
        with sponsor._db(path) as conn:
            before = counts(conn)
            conn.execute("CREATE TEMP TABLE before_plan AS SELECT dataset,partition,state,priority,rows FROM tasks")
            policy = sponsor._seed(conn, now, root=Path(temporary),
                                   official_sessions=sponsor._official_session_calendar(),
                                   official_price_coverage=sponsor._official_price_coverage())
            transitions = [dict(zip(('dataset', 'old_state', 'new_state', 'partitions'), row)) for row in conn.execute(
                "SELECT t.dataset,b.state,t.state,count(*) FROM tasks t JOIN before_plan b "
                "ON t.dataset=b.dataset AND t.partition=b.partition WHERE t.state!=b.state "
                "GROUP BY t.dataset,b.state,t.state ORDER BY t.dataset,b.state,t.state")]
            report = {'observed_at_utc': now.isoformat(), 'api_requests': 0, 'live_queue_modified': False,
                      'before': before, 'after': counts(conn), 'policy': policy, 'transitions': transitions,
                      'pending_requests_avoided': sum(item['partitions'] for item in transitions
                          if item['old_state'] == 'pending' and item['new_state'] in {'non_session','not_observation_date'}),
                      'nonempty_rows_reclassified': conn.execute(
                          "SELECT count(*) FROM tasks t JOIN before_plan b ON t.dataset=b.dataset "
                          "AND t.partition=b.partition WHERE b.rows>0 AND t.state!=b.state").fetchone()[0]}
    target = repo / 'artifacts/data_quality' / f"finmind_request_plan_{now.strftime('%Y%m%dT%H%M%S%fZ')}.json"
    atomic_write_json(target, report)
    print(json.dumps({'report': str(target), **report}, ensure_ascii=False))


if __name__ == '__main__':
    main()
