"""Private staging cannot submit to the production repair inbox."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_provider_storage_readback import snapshot
from scripts.prepare_news_cmnews_retained_reviews import prepare
from test_news_cmnews_scope import URL, VIP_MASK, TEXT, page


def test_private_stage_revalidates_real_original_and_excludes_identical_previous_request(tmp_path, monkeypatch):
    root = tmp_path / 'news'
    root.mkdir()
    _config, registry, _repo = load_config(Path('configs/news_fulltext.json'))
    with closing(NewsQueue(root, registry)) as queue:
        queue.add_articles([{'url': URL}])
        head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        fake_http = SimpleNamespace(registry=registry,
            get=lambda *_a, **_kw: Response(URL, 200, {'content-type': 'text/html'}, page()))
        record = fetch_article(fake_http, head)
        raw = page(body=TEXT + VIP_MASK)
        record.update(raw_response_body=raw, response_sha256=hashlib.sha256(raw).hexdigest(),
                      http_response_bytes=len(raw), captured_at_utc='2026-01-01T00:00:00+00:00')
        queue.accept([record])
        row = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    before = snapshot(root)
    checkpoint = tmp_path / 'checkpoint.sqlite3'
    with sqlite3.connect(checkpoint) as db:
        db.execute('CREATE TABLE candidates(article_id TEXT,path TEXT,state TEXT,body_sha256 TEXT,'
                   'captured_at_utc TEXT,response_sha256 TEXT,native_outcome TEXT)')
        db.execute('INSERT INTO candidates VALUES(?,?,?,?,?,?,?)', (bytes(row['id']).hex(), row['parquet_path'],
            row['state'], row['body_sha256'], row['captured_at_utc'], record['response_sha256'], 'paywall_or_login'))
    (tmp_path / 'checkpoint.sqlite3.lock').touch(mode=0o600)
    report = tmp_path / 'readback.json'
    report.write_text(json.dumps({'contract': 'cmnews_retained_login_mask_readback_v1', 'accepted': True,
        'checkpoint_sha256': hashlib.sha256(checkpoint.read_bytes()).hexdigest(), 'source_snapshot': before,
        'outcomes': {'confirmed_login_mask': 1}}))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: (_ for _ in ()).throw(
        AssertionError('no publisher requests')))
    first = prepare(root, registry, checkpoint, report, tmp_path / 'stage')
    assert first['new_staged'] == 1 and first['failed_preflight'] == 0 and not first['submitted']
    assert first['publisher_requests'] == first['production_queue_writes'] == first['production_inbox_requests'] == 0
    assert not (root / 'reviewed_repairs').exists() and snapshot(root) == before
    second = prepare(root, registry, checkpoint, report, tmp_path / 'stage2', prior=tmp_path / 'stage')
    assert second['already_staged'] == 1 and second['new_staged'] == 0 and not second['submitted']
    with sqlite3.connect(checkpoint) as db:
        db.execute('UPDATE candidates SET response_sha256=?', ('0' * 64,))
    changed = json.loads(report.read_text())
    changed['checkpoint_sha256'] = hashlib.sha256(checkpoint.read_bytes()).hexdigest()
    report.write_text(json.dumps(changed))
    mismatch = prepare(root, registry, checkpoint, report, tmp_path / 'stage3')
    assert mismatch['failed_preflight'] == 1 and mismatch['new_staged'] == 0
    assert mismatch['failures'][0]['error'] == 'candidate_binding_changed'
    assert not (root / 'reviewed_repairs').exists() and snapshot(root) == before
