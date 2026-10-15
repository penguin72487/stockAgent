from dataclasses import replace
import time

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import PolicyRegistry, _native_exclusions, _plain_text, _plain_text_node, normalize_url
from scripts.benchmark_news_cpu import reference_plain_text
from test_news_fulltext import config, FakeResponse, http_fixture
from downloader.news_fulltext_policy import NewsError


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('selector', [
    'p', '#target', '.example', 'script[type="application/ld+json"]', '[itemprop=articleBody]',
    'a[rel=author]', 'link[rel=canonical]', 'meta[property="og:title"]',
    'a[href*="creativecommons.org/"]', 'input[type="submit"]', '[data-empty]',
    '[ITEMPROP=articleBody]', '[type="SUBMIT" s]', 'article > p.example', 'p, a',
    '.example:nth-child(2)', '#missing', '.missing', 'script', ':scope > p',
])
def test_selector_candidate_narrowing_exactly_matches_original_css(parser, selector):
    from downloader.news_fulltext_policy import _select_nodes, _select_first
    soup = BeautifulSoup('''<html><head><meta property="og:title" content="Title">
    <link rel="canonical alternate" href="/wrong"><link rel="canonical" href="/right">
    <script type="Application/LD+JSON">{}</script></head><body><article id="target" class="example other">
    <p class="example" data-empty="" itemprop="articleBody other">A</p>
    <p id="target" itemprop="articleBody">B</p><a rel="author external">not exact</a><a rel="author">exact</a>
    <a href="https://creativecommons.org/licenses/by/4.0/">CC</a><input type="SUBMIT"></article></body></html>''', parser)
    assert _select_nodes(soup, selector) == soup.select(selector)
    assert _select_nodes(soup, selector, limit=1) == soup.select(selector, limit=1)
    assert _select_first(soup, selector) == soup.select_one(selector)


@pytest.mark.parametrize('marker', [
    '<div class="paywall other"></div>', '<div data-paywall=""></div>', '<form action="/login"></form>',
    '<form action="/LOGIN"></form>', '<p class="notpaywall">paywall</p>', '<p>login data-paywall</p>',
    '<div class="paywall-other"></div>', '<form action="/api/user-login-now"></form>',
])
def test_single_walk_paywall_detection_preserves_exact_original_denial(marker):
    from downloader.news_fulltext_policy import _has_paywall
    soup = BeautifulSoup(marker, 'lxml')
    assert _has_paywall(soup) == (soup.select_one('.paywall, [data-paywall], form[action*=login]') is not None)


@pytest.mark.parametrize("fragment", [
    "原始負值 -12.50\n\n比率 2.3%", "A &amp; B &nbsp; C &#x4e2d;",
    "before<p>A<script>secret</script>B</p>after", "A<!--comment-->B",
    "<p>A<!--comment-->B<?not-an-article pi?>C</p>",
    "<p>原文<i>斜體</i>尾巴<br>下一行</p>",
    "<div><nav>選單<span>子選單</span></nav>原始正文<style>red</style></div>",
    "<p>A<figure><nav>secret</nav>caption</figure>B</p>",
    "<tr><td>價格</td><td>-10.1</td></tr><p>資料</p>",
    "<p>bad<div>nest</div>tail</p>",
    "<p>A<span class='omit'><b>hidden</b></span>B<span id='hide'>hidden</span>C</p>",
    "<html><head><title>真實標題</title></head><body>完整內文</body></html>",
    "<p><ruby>字<rt>annotation</rt><rp>(</rp></ruby>後文</p>",
    "<?xml version='1.0' encoding='utf-8'?><p>entity &amp; body</p>",
    "<!doctype html><html><body><p>text</p></body></html>",
    "<p> A\r\n B\t\n C </p>", "<template>hidden or visible?</template>tail",
])
@pytest.mark.parametrize("selectors", [(), (".omit", "#hide"), ("p > .omit",),
                                        ('[id^="hide"]',), ('[id^="hi"]', '.omit')])
@pytest.mark.filterwarnings("ignore::bs4.XMLParsedAsHTMLWarning")
def test_native_text_is_exact_oracle(fragment, selectors):
    assert _plain_text(fragment, selectors, parser="lxml") == reference_plain_text(fragment, selectors, parser="lxml")


@pytest.mark.parametrize("parser", ["lxml", "html.parser"])
@pytest.mark.parametrize("selectors", [(), (".omit", "#hide"), ("p > .omit",)])
def test_existing_node_is_nonmutating_and_preserves_attribution(parser, selectors):
    payload = "<article><h1>原標題</h1><p>A<!-- hidden -->B<span class='omit'>廣告</span>C</p><p id='hide'>略過</p><a rel='author'>作者</a></article>"
    soup = BeautifulSoup(payload, parser)
    node = soup.article
    before = str(node)
    assert _plain_text_node(node, selectors, parser=parser) == reference_plain_text(before, selectors, parser=parser)
    assert str(node) == before


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('selectors', [
    ('[id^="div-gpt-ad-cnyes_news_article_"]',),
    ("[id^='div-gpt-ad-cnyes_news_article_']", '.omit', '#hide'),
    ('[id^="ad_"]', '[id^="other_"]'),
])
@pytest.mark.parametrize('fragment', [
    '<article><p>價格 -12.50 <b>百分比 2.3%</b></p>'
    '<div id="div-gpt-ad-cnyes_news_article_1"><p>廣告<b>nested</b></p></div>尾巴'
    '<p id="DIV-gpt-ad-cnyes_news_article_2">大小寫不同的正文</p>'
    '<p id="xdiv-gpt-ad-cnyes_news_article_3">非前綴正文</p>'
    '<span class="omit">略</span>之後<p id="hide">略</p>'
    '<p id="ad_first">廣告一</p>甲<p id="other_second">廣告二</p>乙'
    '<a rel="author">原作者</a></article>',
    '<article id="div-gpt-ad-cnyes_news_article_root"><p>root excluded</p></article>',
    '<article><p>A<!--comment-->B<figure><img src="image">caption</figure>C</p>'
    '<template>template</template><ruby>字<rt>annotation</rt></ruby>尾文</article>',
    '<article><p>A<div id="ad_bad"><p>malformed nesting</div>tail</article>',
])
def test_quoted_id_prefix_matches_exact_css_and_never_mutates(parser, selectors, fragment):
    assert _native_exclusions(selectors) is not None
    soup = BeautifulSoup(fragment, parser)
    node = soup.article
    before = str(node)
    expected = reference_plain_text(before, selectors, parser=parser)
    assert _plain_text_node(node, selectors, parser=parser) == expected
    assert _plain_text(before, selectors, parser=parser) == expected
    assert str(node) == before


@pytest.mark.parametrize('selector', [
    '[id^=""]', '[id^="ad_" i]', '[id^="ad_" s]', '[id^=ad_]',
    '[ID^="ad_"]', '[id^="ad_"] p', 'p[id^="ad_"]',
    '[id^="ad_"], .other', '[id^="ad\\31 _"]', '[id^="廣告"]',
])
def test_unknown_id_prefix_semantics_keep_original_css(selector):
    assert _native_exclusions((selector,)) is None
    fragment = '<article><p id="AD_first">original</p><p id="ad_second">body</p></article>'
    assert _plain_text(fragment, (selector,), parser='lxml') == reference_plain_text(fragment, (selector,), parser='lxml')


def test_existing_id_prefix_walk_does_not_serialize_or_parse(monkeypatch):
    import downloader.news_fulltext_policy as policy
    soup = BeautifulSoup('<article><p>原始正文</p><div id="ad_1">廣告</div>尾文</article>', 'lxml')
    def forbidden(*args, **kwargs):
        raise AssertionError('existing article tree must not serialize/reparse')
    monkeypatch.setattr(policy, '_plain_text', forbidden)
    assert policy._plain_text_node(soup.article, ('[id^="ad_"]',), parser='lxml') == '原始正文\n尾文'


def test_prefix_only_benchmark_restores_every_function_on_error():
    import downloader.news_fulltext_policy as policy
    from scripts.benchmark_news_cpu import implementation
    original = policy._native_exclusions
    try:
        with implementation(True, id_prefix_only=True):
            assert policy._native_exclusions(('[id^="ad_"]',)) is None
            assert policy._native_exclusions(('.omit',)) == original(('.omit',))
            raise RuntimeError('test failure')
    except RuntimeError:
        pass
    assert policy._native_exclusions is original
    assert original(('[id^="ad_"]',)) is not None


@pytest.mark.parametrize('fail', [False, True])
def test_status_benchmark_is_read_only_preserves_all_data_and_restores_clocks(config, tmp_path, monkeypatch, fail):
    import time
    from downloader import news_fulltext_cohorts as cohorts, news_fulltext_queue as queue_module
    from scripts.benchmark_news_cpu import status_projection_replay
    config['providers'][0]['acquisition_mode'] = 'finite'
    registry = PolicyRegistry(config, base=tmp_path)
    q = queue_module.NewsQueue(tmp_path / 'private', registry)
    try:
        with q.db:
            q.add_articles([{'url': 'https://publisher.test/a'}])
        q.sync_finite_cohorts()
        q.status()
        original_status = (q.root / 'status.json').read_bytes()
        settings = list(q.db.execute('SELECT key,value FROM queue_settings ORDER BY key'))
        changes = q.db.total_changes
        original = (time.time, time.monotonic, queue_module.utc_now, cohorts._NONBODY_OUTCOME_PROGRESS_SQL)
        if fail:
            def reject(*args, **kwargs):
                raise RuntimeError('synthetic replay failure')
            monkeypatch.setattr(queue_module.NewsQueue, 'status', reject)
            with pytest.raises(RuntimeError, match='synthetic replay failure'):
                status_projection_replay(q.root, registry, tmp_path, 2)
        else:
            result = status_projection_replay(q.root, registry, tmp_path, 2)
            assert result['accepted'] and result['mismatches'] == []
            assert result['known_urls'] == 1 and result['stored_heads'] == 0
            assert result['queue_writes'] == result['publisher_http_requests'] == 0
            assert not result['production_status_written'] and not result['all_history_complete']
            assert len(result['measurements']['reference']) == len(result['measurements']['optimized']) == 2
            assert result['reviewed_nonbody_by_provider']['publisher']['urls'] == 0
        assert (time.time, time.monotonic, queue_module.utc_now, cohorts._NONBODY_OUTCOME_PROGRESS_SQL) == original
        assert (q.root / 'status.json').read_bytes() == original_status
        assert list(q.db.execute('SELECT key,value FROM queue_settings ORDER BY key')) == settings
        assert q.db.total_changes == changes
    finally:
        q.close()


@pytest.mark.parametrize('limit', [1, 64])
def test_html_benchmark_includes_canonical_batched_durability_and_rejections(config, tmp_path, limit):
    import hashlib
    from scripts.benchmark_news_cpu import pipeline, extract
    from test_news_fulltext import article_html
    config['batch_articles'] = limit
    registry = PolicyRegistry(config, base=tmp_path)
    payload = article_html()
    samples = [{'provider': 'publisher', 'url': f'https://publisher.test/{suffix}',
                'body': body, 'response_sha256': hashlib.sha256(body).hexdigest()}
               for suffix, body in [('a', payload), ('bad', b'<html><title>Not a body</title></html>'), ('b', payload)]]
    before = extract(samples, registry)
    result = pipeline(samples, registry, config, tmp_path)
    assert result['accepted'] and result['errors'] == []
    assert result['new_heads'] == result['body_versions_verified'] == 2
    assert result['replayed_responses'] == 3
    assert result['source_rejections'] == {'body_not_found': 1}
    assert result['html_batch_sizes'] == ([1, 1] if limit == 1 else [2])
    assert extract(samples, registry) == before
    assert not (tmp_path / 'queue.sqlite3').exists()


def test_diagnostic_benchmark_loads_exact_bounded_receipt_verified_responses(config, tmp_path):
    from downloader.artifact_io import atomic_write_json
    from scripts.benchmark_news_cpu import diagnostic_corpus
    from scripts.probe_news_sources import probe_provider
    from test_news_fulltext import FakeResponse, article_html, http_fixture
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              'https://publisher.test/a': FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path / 'transport', routes)
    try:
        probes = probe_provider(http, ['https://publisher.test/a'], retain=True, output_root=tmp_path)
        report = tmp_path / 'probes.json'
        atomic_write_json(report, {'observed_at_utc': '2026-10-10T00:00:00+00:00', 'probes': probes})
        network_before = list(session.calls)
        samples, files, counts, inputs = diagnostic_corpus(http.registry, [report])
        assert counts == {'publisher': 1} and samples[0]['url'] == 'https://publisher.test/a'
        assert samples[0]['response_sha256'] == probes[0]['response_sha256']
        assert files[0]['receipt_sha256'] and inputs[str(report)]
        assert session.calls == network_before
        from pathlib import Path
        receipt = Path(probes[0]['private_diagnostic_path']).with_suffix('.receipt.json')
        import json
        broken = json.loads(receipt.read_text())
        broken['accepted_as_article'] = True
        atomic_write_json(receipt, broken)
        with pytest.raises(ValueError, match='retained_diagnostic_proof_not_accepted'):
            diagnostic_corpus(http.registry, [report])
    finally:
        http.close()


def test_diagnostic_benchmark_rejects_overwide_or_finite_scope(config, tmp_path):
    from downloader.artifact_io import atomic_write_json
    from scripts.benchmark_news_cpu import diagnostic_corpus
    registry = PolicyRegistry(config, base=tmp_path)
    with pytest.raises(ValueError, match='one_to_four'):
        diagnostic_corpus(registry, [])
    report = tmp_path / 'probes.json'
    atomic_write_json(report, {'probes': [{'url': f'https://publisher.test/{n}',
                                        'private_diagnostic_path': 'not_read.parquet'} for n in range(17)]})
    with pytest.raises(ValueError, match='bounded_retained'):
        diagnostic_corpus(registry, [report])
    config['providers'][0]['acquisition_mode'] = 'finite'
    registry = PolicyRegistry(config, base=tmp_path)
    atomic_write_json(report, {'probes': [{'url': 'https://publisher.test/a', 'provider': 'publisher',
                                        'status': 200, 'private_diagnostic_path': 'not_read.parquet'}]})
    with pytest.raises(ValueError, match='private_continuous'):
        diagnostic_corpus(registry, [report])


def test_domain_lookup_longest_suffix_does_not_cache_old_policy(config, tmp_path):
    peer = {**config["providers"][0], "id": "peer", "domains": ["news.publisher.test"]}
    config["providers"].append(peer)
    registry = PolicyRegistry(config, base=tmp_path)
    assert registry.match("https://sub.news.publisher.test/a").provider == "peer"
    assert registry.match("https://publisher.test/a").provider == "publisher"
    assert registry.match("https://notpublisher.test/a") is None
    registry.sources["peer"] = replace(registry.sources["peer"], allowed=False)
    assert not registry.match("https://news.publisher.test/a").allowed


def test_heartbeat_groups_every_bulk_scope_without_losing_leaf_totals(config, tmp_path):
    import json
    from downloader.news_fulltext_queue import NewsQueue

    endpoint = "https://publisher.test/wp-json/wp/v2/posts"
    config["providers"][0]["wordpress_apis"] = [endpoint]
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        parent = endpoint + "?after=2020-01-01&before=2021-01-01"
        leaves = [endpoint + f"?after=2020-{number:02d}-01" for number in range(1, 13)]
        with queue.db:
            queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'publisher',1200,120,'2026-10-09')", (parent,))
            queue.db.execute("INSERT INTO bulk_window_splits VALUES (?,'publisher',?,'2026-10-09')", (parent, json.dumps(leaves)))
            queue.db.executemany("INSERT INTO bulk_api_coverage VALUES (?,'publisher',100,10,'2026-10-09')", [(url,) for url in leaves])
            queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'publisher',0,0,'2026-10-08')", (endpoint,))
            # An empty split did not supersede a scope in the previous oracle.
            queue.db.execute("INSERT INTO bulk_window_splits VALUES (?,'publisher','[]','2026-10-08')", (endpoint,))
            queue.db.execute("INSERT INTO bulk_api_coverage VALUES ('https://unreviewed.test/api?after=2020','unknown',7,1,'2026-10-09')")
        before = [tuple(row) for row in queue.db.execute("SELECT * FROM bulk_api_coverage ORDER BY base_url")]
        result = queue.status()
        assert result["bulk_coverage_projection"]["contract_version"] == 2
        assert result["bulk_coverage_projection"]["coverage_scopes"] == 15
        assert result["bulk_coverage_projection"]["leaf_scopes"] == 14
        assert result["bulk_coverage_projection"]["superseded_scopes"] == 1
        assert result["bulk_api_observed_articles"] == 1207
        assert len(result["bulk_api_endpoints"]) == 2
        source = next(item for item in result["bulk_api_endpoints"] if item["provider"] == "publisher")
        assert source["enabled"] and source["base_url"] == endpoint
        assert (source["coverage_scopes"], source["leaf_scopes"], source["superseded_scopes"]) == (14, 13, 1)
        assert (source["total_articles"], source["total_pages"]) == (1200, 120)
        assert not next(item for item in result["bulk_api_endpoints"] if item["provider"] == "unknown")["enabled"]
        assert [tuple(row) for row in queue.db.execute("SELECT * FROM bulk_api_coverage ORDER BY base_url")] == before
        assert queue.db.execute("SELECT child_urls_json FROM bulk_window_splits WHERE scope_url=?", (parent,)).fetchone()[0] == json.dumps(leaves)
        assert queue.db.execute("SELECT count(*) FROM discovery").fetchone()[0] == 0
        assert len(json.dumps(result["bulk_api_endpoints"])) < 1024
        from scripts.benchmark_news_status_projection import benchmark
        replay = benchmark(tmp_path, queue.registry, repetitions=3)
        assert replay["accepted"] and replay["all_scope_totals_preserved"]
        assert (replay["coverage_scopes"], replay["source_endpoints"]) == (15, 2)
        assert replay["serialized_projection_bytes"]["summary"] < replay["serialized_projection_bytes"]["legacy"]
    finally:
        queue.close()


def test_ascii_and_unicode_host_identity_unchanged():
    assert normalize_url("https://EXAMPLE.com./a?utm_source=x&id=1") == "https://example.com/a?id=1"
    assert normalize_url("https://台灣.tw/a") == "https://xn--kpry57d.tw/a"


def test_same_provider_robots_redirect_keeps_real_disallow(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(status=301, headers={"Location": "/policy/robots"}),
        "https://publisher.test/policy/robots": FakeResponse(b"User-agent: *\nDisallow: /article\n", headers={"Content-Type": "text/plain"}),
    })
    try:
        with pytest.raises(NewsError, match="robots_disallowed"):
            http.get("https://publisher.test/article")
        assert len(session.calls) == 2
    finally:
        http.close()


def test_invalid_robots_body_pauses_origin_not_every_article(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"<html><title>Homepage</title></html>"),
    })
    try:
        with pytest.raises(NewsError) as caught:
            http.get("https://publisher.test/a")
        assert caught.value.code == "robots_not_plain_rules" and caught.value.retry
        with pytest.raises(NewsError):
            http.get("https://publisher.test/b")
        assert len(session.calls) == 1
        assert http.blocked_robots_origins('publisher')['https://publisher.test'] > time.time()
        assert not http.provider_not_before
    finally:
        http.close()


def test_dispatch_replay_keeps_finite_sources_without_forcing_partial_index(config, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    from scripts.benchmark_news_cpu import dispatch_replay
    config['providers'][0]['acquisition_mode'] = 'finite'
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES ('https://publisher.test/feed','feed','publisher')")
        result = dispatch_replay(tmp_path, registry, config, repetitions=2)
        assert result['accepted']
        assert result['providers_queried'] == 1
        assert result['selected_job_counts'] == {'publisher': 0}
    finally:
        queue.close()


def test_stored_head_counter_preserves_rechecks_versions_provider_moves_and_rollback(config, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    from scripts.benchmark_news_queue_stats import benchmark
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/a'}, {'url': 'https://publisher.test/b'}])
            queue.db.execute("UPDATE articles SET body_sha256='old',body_bytes=0 WHERE url LIKE '%/a'")
        assert queue.status()['articles_with_stored_body'] == 1  # SHA presence, not positive bytes or complete state.
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',body_sha256='new' WHERE url LIKE '%/a'")
            queue.db.execute("UPDATE articles SET provider='other' WHERE url LIKE '%/a'")
            queue.db.execute("UPDATE articles SET body_sha256='second' WHERE url LIKE '%/b'")
        assert queue.status()['articles_with_stored_body'] == 2
        with pytest.raises(RuntimeError):
            with queue.db:
                queue.db.execute('UPDATE articles SET body_sha256=NULL')
                raise RuntimeError('rollback')
        assert queue.status()['articles_with_stored_body'] == 2
        with queue.db:
            queue.db.execute("UPDATE articles SET body_sha256=NULL WHERE provider='other'")
            queue.db.execute("DELETE FROM articles WHERE url LIKE '%/b'")
        assert queue.status()['articles_with_stored_body'] == 0
        assert benchmark(tmp_path, 3)['accepted']
    finally:
        queue.close()


def test_legacy_stored_head_counter_migrates_once_and_restart_never_scans(config, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    with queue.db:
        queue.add_articles([{'url': 'https://publisher.test/a'}])
        queue.db.execute("UPDATE articles SET body_sha256='body' WHERE url LIKE '%/a'")
        queue.db.execute("DELETE FROM queue_settings WHERE key='stored_body_metrics_version'")
        queue.db.execute('DELETE FROM stored_body_metrics')
    queue.close()
    queue = NewsQueue(tmp_path, registry)
    statements = []
    try:
        assert queue.status()['articles_with_stored_body'] == 1
        queue.db.set_trace_callback(statements.append)
        queue.initialize_stored_body_metrics()
        queue.status()
        assert not any('count(*) FROM articles' in statement for statement in statements)
        assert not any('INSERT INTO stored_body_metrics SELECT provider,count' in statement for statement in statements)
    finally:
        queue.close()


def test_other_provider_permission_change_never_reconciles_unchanged_frontier(config, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    import copy
    peer = {**copy.deepcopy(config['providers'][0]), 'id': 'peer', 'domains': ['peer.test'], 'enabled': False}
    config['providers'].append(peer)
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/a'}, {'url': 'https://peer.test/a'}])
            queue.db.execute("UPDATE articles SET state='retry',due=9999999999,error_code='http_429' WHERE host='publisher.test'")
        statements = []
        queue.db.set_trace_callback(statements.append)
        peer['enabled'] = True
        queue.registry = PolicyRegistry(config, base=tmp_path)
        queue.reconcile_policy()
        assert not any("UPDATE articles" in sql and "host='publisher.test'" in sql for sql in statements)
        assert queue.db.execute("SELECT state,due,error_code FROM articles WHERE host='publisher.test'").fetchone()[:] == ('retry', 9999999999, 'http_429')
        assert queue.db.execute("SELECT state FROM articles WHERE host='peer.test'").fetchone()[0] == 'pending'
    finally:
        queue.close()


def test_identical_diagnostic_bodies_never_overwrite_distinct_request_identity(config, tmp_path):
    from scripts.probe_news_sources import probe_provider
    from scripts.verify_news_source_diagnostics import verify
    from test_news_fulltext import FakeResponse, article_html, http_fixture
    payload = article_html()
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://publisher.test/a': FakeResponse(payload), 'https://publisher.test/b': FakeResponse(payload),
    })
    try:
        results = probe_provider(http, ['https://publisher.test/a', 'https://publisher.test/b'], retain=True, output_root=tmp_path)
        assert results[0]['response_sha256'] == results[1]['response_sha256']
        assert results[0]['private_diagnostic_path'] != results[1]['private_diagnostic_path']
        assert verify(results, http.registry)['accepted']
    finally:
        http.close()
