from __future__ import annotations

from dataclasses import replace
import gzip
import hashlib
import time

import pytest

from downloader.news_fulltext import (accept_discovery, discovery_fingerprint, discovery_links,
    fetch_discovery, seed_discovery, sitemap_discovery_fingerprints)
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader import news_fulltext_sitemaps as sitemaps
from test_news_fulltext import FakeResponse, config as config, http_fixture


def urlset(*entries):
    return ('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' + ''.join(entries) + '</urlset>').encode()


def entry(url='https://publisher.test/story/1', date='2026-10-24'):
    return f'<url><loc>{url}</loc><lastmod>{date}</lastmod></url>'


def test_gzip_urlset_preserves_all_fields_order_duplicates_and_existing_filter(config, tmp_path):
    source = PolicyRegistry(config, base=tmp_path).sources['publisher']
    source = replace(source, article_path_pattern=r'^/story/')
    xml = urlset(entry(date=' 2026-10-24T12:34:56+08:00 '), entry(), entry(),
        entry('https://elsewhere.test/story/1'), entry('https://publisher.test/category/1'),
        '<url><loc>https://publisher.test/story/2?a=1&amp;b=2</loc></url>')
    assert discovery_links(gzip.compress(xml), 'sitemap', source) == discovery_links(xml, 'sitemap', source)
    links = discovery_links(gzip.compress(xml), 'sitemap', source)
    assert len(links)==4 and links[1]==links[2]
    assert links[0][2]==' 2026-10-24T12:34:56+08:00 '
    assert links[-1]==('https://publisher.test/story/2?a=1&b=2','','')


@pytest.mark.parametrize('namespace', ['', ' xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"'])
def test_gzip_index_and_extension_metadata_do_not_become_articles_or_publication(config, tmp_path, namespace):
    source = PolicyRegistry(config, base=tmp_path).sources['publisher']
    xml = ('<sitemapindex' + namespace + '><sitemap><loc>https://publisher.test/news.xml.gz</loc>'
        '<lastmod>2026-10-24</lastmod></sitemap></sitemapindex>').encode()
    assert discovery_links(gzip.compress(xml), 'sitemap', source)==[('https://publisher.test/news.xml.gz','sitemap','')]
    xml = urlset('<url xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">'
        '<loc>https://publisher.test/story/1</loc><image:image><image:loc>https://elsewhere.test/a.jpg</image:loc>'
        '</image:image></url>')
    assert discovery_links(gzip.compress(xml), 'sitemap', source)==[('https://publisher.test/story/1','','')]


def test_max_sitemap_filter_remains_shared_with_gzip(config, tmp_path):
    source = replace(PolicyRegistry(config, base=tmp_path).sources['publisher'],
        domains=('maxfinanciallife.com',), embedded_article_layout='max_visible_post_v4')
    xml = b'<sitemapindex><sitemap><loc>https://maxfinanciallife.com/post-sitemap1.xml</loc></sitemap><sitemap><loc>https://maxfinanciallife.com/page-sitemap.xml</loc></sitemap></sitemapindex>'
    assert discovery_links(gzip.compress(xml), 'sitemap', source)==discovery_links(xml, 'sitemap', source)


@pytest.mark.parametrize('damage', ['crc','truncated','trailing','second_xml'])
def test_corruption_after_valid_entries_never_returns_partial_metadata(damage):
    raw = gzip.compress(urlset(entry()))
    if damage=='crc': raw = raw[:-8] + bytes([raw[-8]^1]) + raw[-7:]
    elif damage=='truncated': raw = raw[:-4]
    elif damage=='trailing': raw += b'not-a-gzip-member'
    else: raw += gzip.compress(urlset(entry('https://publisher.test/story/2')))
    with pytest.raises(NewsError) as error:
        sitemaps.gzip_sitemap_links(raw)
    assert error.value.code in {'invalid_sitemap_gzip','invalid_discovery_xml'}


def test_byte_limit_includes_all_gzip_members_and_all_xml_whitespace(monkeypatch):
    xml = urlset(entry())
    monkeypatch.setattr(sitemaps, 'MAX_SITEMAP_DECODED_BYTES', len(xml))
    assert sitemaps.gzip_sitemap_links(gzip.compress(xml))
    split = len(xml)//2
    assert sitemaps.gzip_sitemap_links(gzip.compress(xml[:split])+gzip.compress(xml[split:]))
    with pytest.raises(NewsError, match='sitemap_decoded_limit_exceeded'):
        sitemaps.gzip_sitemap_links(gzip.compress(xml)+gzip.compress(b' '))


def test_entry_limit_includes_entries_without_locations(monkeypatch):
    monkeypatch.setattr(sitemaps, 'MAX_SITEMAP_ENTRIES', 2)
    assert len(sitemaps.gzip_sitemap_links(gzip.compress(urlset(entry(),entry()))))==2
    with pytest.raises(NewsError, match='sitemap_entry_limit_exceeded'):
        sitemaps.gzip_sitemap_links(gzip.compress(urlset('<url/>',entry(),entry())))


@pytest.mark.parametrize('xml', [
    b'<!DOCTYPE urlset [<!ENTITY local SYSTEM "file:///etc/passwd">]><urlset><url><loc>&local;</loc></url></urlset>',
    b'<!DOCTYPE urlset SYSTEM "https://publisher.test/entity.dtd"><urlset/>',
    b'<!DOCTYPE urlset [<!ENTITY x "secret">]><urlset/>',
])
def test_doctype_cannot_fetch_network_files_or_expand_entities(xml):
    with pytest.raises(NewsError, match='sitemap_doctype_not_allowed'):
        sitemaps.gzip_sitemap_links(gzip.compress(xml))


@pytest.mark.parametrize('xml,code', [(b'<rss/>','invalid_sitemap'),
    (b'<urlset xmlns="https://unrelated.test/"/>','invalid_sitemap'),
    (b'<urlset><url></urlset>','invalid_discovery_xml'), (b'','invalid_discovery_xml')])
def test_invalid_compressed_documents_are_explicit_failures(xml, code):
    with pytest.raises(NewsError, match=code):
        sitemaps.gzip_sitemap_links(gzip.compress(xml))


def sitemap_fixture(config, tmp_path, raw, status=200):
    url = 'https://publisher.test/sitemap.xml.gz'
    config['providers'][0]['sitemaps']=[url]
    http, session = http_fixture(config,tmp_path,{
        'https://publisher.test/robots.txt':FakeResponse(b'User-agent: *\nAllow: /'),
        url:FakeResponse(raw,status,{'Content-Type':'application/gzip','ETag':'"map-v1"'})})
    queue = NewsQueue(tmp_path,http.registry)
    seed_discovery(queue)
    return url, http, session, queue


def test_gzip_fetch_accept_and_reopen_keep_original_response_identity(config, tmp_path):
    raw = gzip.compress(urlset(entry(),entry('https://publisher.test/story/2')))
    url, http, session, queue = sitemap_fixture(config,tmp_path,raw)
    try:
        source = http.registry.sources['publisher']
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(url,)).fetchone())
        response, links, records, errors = fetch_discovery(http,task)
        assert response.body==raw and len(links)==2 and records==errors==[]
        assert accept_discovery(queue,task,response,links,records,errors)==0
        stored = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(url,)).fetchone())
        assert stored['response_sha256']==hashlib.sha256(raw).hexdigest()
        assert stored['parser_fingerprint']==discovery_fingerprint(source,kind='sitemap')
        assert queue.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0]==0
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0]==2
        # Exact same compressed response uses validators and avoids re-parsing.
        assert fetch_discovery(http,stored)[1:]==([],[],[])
        assert session.calls[-1][1]['headers']['If-None-Match']=='"map-v1"'
    finally:
        queue.close();http.close()
    reopened = NewsQueue(tmp_path,http.registry)
    try:
        assert reopened.db.execute('SELECT response_sha256 FROM discovery WHERE url=?',(url,)).fetchone()[0]==hashlib.sha256(raw).hexdigest()
        assert reopened.db.execute('SELECT count(*) FROM articles').fetchone()[0]==2
    finally:
        reopened.close()


@pytest.mark.parametrize('status', [200,304])
def test_old_successful_plain_sitemap_keeps_validators_and_original_deadline(config, tmp_path, status):
    raw = urlset(entry())
    url, http, session, queue = sitemap_fixture(config,tmp_path,raw if status==200 else b'',status)
    try:
        source=http.registry.sources['publisher']
        legacy=discovery_fingerprint(source)
        assert sitemap_discovery_fingerprints(source)==(discovery_fingerprint(source,kind='sitemap'),legacy)
        future=time.time()+86400
        with queue.db:
            queue.db.execute('UPDATE discovery SET due=?,parser_fingerprint=?,etag=?,response_sha256=? WHERE url=?',
                (future,legacy,'"map-v1"',hashlib.sha256(raw).hexdigest(),url))
        seed_discovery(queue)
        task=dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(url,)).fetchone())
        assert task['due']==future
        response,links,records,errors=fetch_discovery(http,task)
        assert links==records==errors==[]
        assert session.calls[-1][1]['headers']['If-None-Match']=='"map-v1"'
        accept_discovery(queue,task,response,links,records,errors)
        current=queue.db.execute('SELECT parser_fingerprint FROM discovery WHERE url=?',(url,)).fetchone()[0]
        assert current==(discovery_fingerprint(source,kind='sitemap') if status==200 else legacy)
        assert discovery_fingerprint(source,kind='feed')==legacy
        assert discovery_fingerprint(source,kind='index')==legacy
    finally:
        queue.close();http.close()


def test_failed_gzip_fetch_cannot_register_early_urls(config, tmp_path):
    url,http,session,queue=sitemap_fixture(config,tmp_path,gzip.compress(urlset(entry()))[:-4])
    try:
        task=dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(url,)).fetchone())
        with pytest.raises(NewsError,match='invalid_sitemap_gzip'):
            fetch_discovery(http,task)
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0]==0
        assert queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?',(url,)).fetchone()[0] is None
    finally:
        queue.close();http.close()


def test_gzip_endpoint_family_changes_no_authorization(config, tmp_path):
    source=PolicyRegistry(config,base=tmp_path).sources['publisher']
    assert NewsHTTP.endpoint_kind(source,'https://publisher.test/unlisted.xml.gz')=='xml_discovery'
    denied={**config,'providers':[{**config['providers'][0],'enabled':False}]}
    http,session=http_fixture(denied,tmp_path,{})
    try:
        with pytest.raises(NewsError):
            fetch_discovery(http,{'kind':'sitemap','url':'https://publisher.test/sitemap.xml.gz'})
        assert session.calls==[]
    finally:
        http.close()
