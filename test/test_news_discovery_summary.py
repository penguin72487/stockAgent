"""Exact clocked counts share source transactions, including rollback/recovery."""
from itertools import product
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from downloader.news_fulltext_status import discovery_summary, initialize_discovery_summary
from scripts.benchmark_news_status_projection import legacy_discovery_status


def database():
    db = sqlite3.connect(':memory:')
    db.executescript('''CREATE TABLE discovery(id INTEGER PRIMARY KEY,kind TEXT NOT NULL,due REAL,
        error_code TEXT,superseded_by_windows_json TEXT,other TEXT);
        CREATE TABLE queue_settings(key TEXT PRIMARY KEY,value TEXT);''')
    return db


def cases():
    return [(k,d,e,s) for k,d,e,s in product(
        ('feed','index','cnyes_news','wordpress_posts','rss_fulltext','unknown'),
        (None,-1,0,99.999,100,100.001,1e20),
        (None,'','source_access_denied'),(None,'','[]','["child"]'))]


def assert_exact(db):
    changes = db.total_changes
    for now in (-1,0,99.999,100,100.001,1e20):
        assert discovery_summary(db,now)==legacy_discovery_status(db,now)
    assert db.total_changes==changes


@pytest.mark.parametrize('seed_first',[True,False])
def test_seed_or_live_insert_all_due_null_error_archive_kind_combinations(seed_first):
    with database() as db:
        if seed_first:
            initialize_discovery_summary(db)
            assert discovery_summary(db,100)==(0,0,0,0,0)
        db.executemany('INSERT INTO discovery(kind,due,error_code,superseded_by_windows_json) VALUES (?,?,?,?)',cases())
        db.commit()
        if not seed_first:
            original=list(db.execute('SELECT * FROM discovery'))
            initialize_discovery_summary(db)
            assert original==list(db.execute('SELECT * FROM discovery'))
        assert_exact(db)
        changes=db.total_changes
        initialize_discovery_summary(db)
        assert db.total_changes==changes
        assert_exact(db)


def test_update_delete_upsert_and_rollback_are_atomic_and_prune_zero_groups():
    with database() as db:
        initialize_discovery_summary(db)
        db.executemany('INSERT INTO discovery(kind,due,error_code,superseded_by_windows_json) VALUES (?,?,?,?)',cases())
        db.commit()
        assert_exact(db)
        db.execute("UPDATE discovery SET due=NULL,error_code='',superseded_by_windows_json=NULL WHERE kind='index'")
        assert_exact(db)
        db.execute("UPDATE discovery SET due=100,kind='rss_fulltext',error_code=NULL WHERE kind='unknown'")
        assert_exact(db)
        db.execute("UPDATE discovery SET superseded_by_windows_json='[]' WHERE kind='feed'")
        assert_exact(db)
        db.execute("DELETE FROM discovery WHERE kind='cnyes_news'")
        assert_exact(db)
        db.rollback()
        assert_exact(db)
        # The source uses UPSERT rather than REPLACE; demonstrate its exact
        # existing-row update path under the same trigger transaction.
        db.execute("INSERT INTO discovery(id,kind,due,error_code) VALUES (1,'rss_fulltext',10,'changed') "
                   'ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,due=excluded.due,error_code=excluded.error_code')
        assert_exact(db)
        db.execute('DELETE FROM discovery')
        assert discovery_summary(db,100)==(0,0,0,0,0)
        assert db.execute('SELECT count(*) FROM discovery_deadline_counts').fetchone()[0]==0
        db.rollback()
        assert_exact(db)


def test_unrelated_or_same_value_updates_do_not_write_derived_counts():
    with database() as db:
        initialize_discovery_summary(db)
        db.execute("INSERT INTO discovery(kind,due) VALUES ('cnyes_news',100)")
        db.commit()
        for sql in ('UPDATE discovery SET other=\'body not here\'', 'UPDATE discovery SET due=due,kind=kind,error_code=error_code,superseded_by_windows_json=superseded_by_windows_json'):
            changes=db.total_changes
            db.execute(sql)
            assert db.total_changes-changes==1
            assert_exact(db)


@pytest.mark.parametrize('lost',['static','deadlines','marker'])
def test_missing_derived_generation_reseeds_only_from_originals(lost):
    with database() as db:
        db.executemany('INSERT INTO discovery(kind,due,error_code,superseded_by_windows_json) VALUES (?,?,?,?)',cases())
        db.commit()
        initialize_discovery_summary(db)
        original=list(db.execute('SELECT * FROM discovery'))
        if lost=='static': db.execute('DROP TABLE discovery_static_counts')
        elif lost=='deadlines': db.execute('DROP TABLE discovery_deadline_counts')
        else: db.execute("DELETE FROM queue_settings WHERE key='discovery_summary_version'")
        db.commit()
        initialize_discovery_summary(db)
        assert original==list(db.execute('SELECT * FROM discovery'))
        assert_exact(db)


def test_readonly_legacy_has_no_writes_and_invalid_current_summary_fails():
    with database() as db:
        db.executemany('INSERT INTO discovery(kind,due,error_code,superseded_by_windows_json) VALUES (?,?,?,?)',cases())
        db.commit()
        db.execute('PRAGMA query_only=ON')
        assert_exact(db)
        assert db.execute("SELECT count(*) FROM sqlite_master WHERE name LIKE 'discovery_%counts'").fetchone()[0]==0
        db.execute('PRAGMA query_only=OFF')
        initialize_discovery_summary(db)
        db.execute('DELETE FROM discovery_static_counts')
        with pytest.raises(ValueError,match='invalid_discovery_summary_generation'):
            discovery_summary(db,100)


def test_benchmark_direct_cli_imports_canonical_modules_without_pythonpath(tmp_path):
    script=Path(__file__).resolve().parents[1]/'scripts/benchmark_news_status_projection.py'
    # --help does not read credentials/config, start an owner or call a source.
    result=subprocess.run([sys.executable,str(script),'--help'],cwd=tmp_path,
                          capture_output=True,text=True,timeout=20)
    assert result.returncode==0,result.stderr
    assert '--compare-discovery-transactional' in result.stdout
