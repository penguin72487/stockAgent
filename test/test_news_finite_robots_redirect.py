"""A real REP redirect is a policy route, never another article grant."""
from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json

import pytest

from downloader.news_fulltext_policy import NewsError
from test_news_fulltext import FakeResponse, config, http_fixture
from test_news_fulltext_performance import UnreadErrorResponse
from test_news_metadata_feed_scope import config as metadata_config, setup as metadata_setup
from downloader.news_fulltext_metadata_feeds import LTN_FEEDS


ORIGIN = 'https://publisher.test'
START = ORIGIN + '/robots.txt'
POLICY = ORIGIN + '/zh/robots.txt'
ONE, TWO = ORIGIN + '/article/1', ORIGIN + '/article/2'
ALLOW = b'User-agent: *\nAllow: /\n'


@pytest.fixture
def finite(config):
    source = config['providers'][0]
    source.update(acquisition_mode='finite', finite_article_urls=[ONE, TWO],
        article_path_pattern=r'^/article/[1-9][0-9]*$')
    return config


def cache(root):
    return root/'robots'/(hashlib.sha256(ORIGIN.encode()).hexdigest()+'.json')


@pytest.mark.parametrize('status', [301, 302, 303, 307, 308])
def test_finite_policy_redirect_uses_original_authority_and_preserves_article_scope(finite, tmp_path, status):
    http, session = http_fixture(finite, tmp_path, {
        START: FakeResponse(status=status, headers={'Location': '/zh/robots.txt'}),
        POLICY: FakeResponse(ALLOW, headers={'Content-Type':'text/plain'}),
        ONE: FakeResponse(b'original one'), TWO: FakeResponse(b'original two')})
    with closing(http):
        assert http.get(ONE).body == b'original one'
        assert http.get(TWO).body == b'original two'
        assert [url for url,_ in session.calls] == [START, POLICY, ONE, TWO]
        checkpoint = json.loads(cache(tmp_path).read_bytes())
        assert checkpoint['origin'] == ORIGIN and checkpoint['fetched_url'] == POLICY
        assert checkpoint['body'] == ALLOW.decode()
        for url in (POLICY, ORIGIN+'/', ORIGIN+'/article/3'):
            with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
                http.get(url)
        assert len(session.calls) == 4
        counts=http.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['endpoint_robots_requests'] == 2
        assert counts['endpoint_article_html_requests'] == 2


def test_redirected_disallow_never_sends_a_finite_article_and_survives_restart(finite, tmp_path):
    routes={START:FakeResponse(status=301, headers={'Location':POLICY}),
        POLICY:FakeResponse(b'User-agent: *\nDisallow: /article/\n')}
    http,session=http_fixture(finite,tmp_path,routes)
    with closing(http),pytest.raises(NewsError,match='^robots_disallowed$'):
        http.get(ONE)
    assert [url for url,_ in session.calls] == [START,POLICY]
    original=cache(tmp_path).read_bytes()
    http,session=http_fixture(finite,tmp_path,{})
    with closing(http),pytest.raises(NewsError,match='^robots_disallowed$'):
        http.get(TWO)
    assert not session.calls and cache(tmp_path).read_bytes() == original


@pytest.mark.parametrize('status,code', [(403,'source_access_denied'),(429,'http_429'),(502,'http_502')])
def test_redirected_policy_refusal_keeps_its_actual_error_and_unread_body(finite,tmp_path,status,code):
    http,session=http_fixture(finite,tmp_path,{
        START:FakeResponse(status=301,headers={'Location':POLICY}),
        POLICY:UnreadErrorResponse(status=status)})
    with closing(http):
        with pytest.raises(NewsError,match='^'+code+'$'):
            http.get(ONE)
        events=[json.loads(p.read_bytes()) for p in (tmp_path/'access_problems'/'publisher').glob('*.json')]
        assert len(events)==1 and events[0]['requested_url']==POLICY
        assert events[0]['error_code']==code and events[0]['body_read'] is False
        assert [url for url,_ in session.calls]==[START,POLICY]
        with pytest.raises(NewsError):
            http.get(TWO)
        assert len(session.calls)==2


def test_finite_redirect_dns_rejection_is_one_initial_origin_failure(finite,tmp_path):
    lookups=[]
    def address(url):
        lookups.append(url)
        if url==POLICY:
            raise NewsError('non_public_address')
    http,session=http_fixture(finite,tmp_path,{START:FakeResponse(status=301,headers={'Location':POLICY})})
    http.address_check=address
    with closing(http):
        with pytest.raises(NewsError,match='^robots_non_public_address$'):
            http.get(ONE)
        saved=cache(tmp_path).read_bytes()
        assert json.loads(saved)['policy_retry']['error_code']=='robots_non_public_address'
        for _ in range(5):
            with pytest.raises(NewsError,match='^robots_origin_policy_cooldown$'):
                http.get(TWO)
        assert lookups==[START,POLICY]
        assert [url for url,_ in session.calls]==[START]
        assert cache(tmp_path).read_bytes()==saved


@pytest.mark.parametrize('target',[ORIGIN+'/article/3','https://other.publisher.test/robots.txt','https://unknown.test/robots.txt'])
def test_article_redirect_never_inherits_the_policy_route(finite,tmp_path,target):
    http,session=http_fixture(finite,tmp_path,{
        START:FakeResponse(status=301,headers={'Location':POLICY}),POLICY:FakeResponse(ALLOW),
        ONE:FakeResponse(status=302,headers={'Location':target})})
    with closing(http),pytest.raises(NewsError):
        http.get(ONE)
    assert [url for url,_ in session.calls]==[START,POLICY,ONE]


@pytest.mark.parametrize('target',['https://other.publisher.test/zh/robots.txt','https://unknown.test/robots.txt'])
def test_finite_policy_does_not_expand_to_another_authority(finite,tmp_path,target):
    http,session=http_fixture(finite,tmp_path,{START:FakeResponse(status=301,headers={'Location':target})})
    with closing(http):
        with pytest.raises(NewsError,match='^robots_redirect_refused$'):
            http.get(ONE)
        assert list(http.blocked_robots_origins('publisher'))==[ORIGIN]
        with pytest.raises(NewsError,match='^robots_origin_policy_cooldown$'):
            http.get(TWO)
        assert [url for url,_ in session.calls]==[START]


def test_five_actual_policy_redirects_allowed_without_widening_finite_scope(finite,tmp_path):
    routes={}; chain=[START]+[ORIGIN+'/zh/policy/'+str(i) for i in range(1,6)]
    for origin,target in zip(chain,chain[1:]):
        routes[origin]=FakeResponse(status=301,headers={'Location':target})
    routes[chain[-1]]=FakeResponse(ALLOW);routes[ONE]=FakeResponse(b'original')
    http,session=http_fixture(finite,tmp_path,routes)
    with closing(http):
        assert http.get(ONE).body==b'original'
        assert [url for url,_ in session.calls]==chain+[ONE]
        for url in chain[1:]:
            with pytest.raises(NewsError,match='^article_rights_or_scope_excluded$'):
                http.get(url)


def test_policy_redirect_loop_is_bounded_and_negatively_cached(finite,tmp_path):
    http,session=http_fixture(finite,tmp_path,{
        START:FakeResponse(status=301,headers={'Location':POLICY}),
        POLICY:FakeResponse(status=301,headers={'Location':START})})
    with closing(http):
        with pytest.raises(NewsError,match='^robots_redirect_loop$'):
            http.get(ONE)
        assert len(session.calls)==6
        with pytest.raises(NewsError,match='^robots_origin_policy_cooldown$'):
            http.get(TWO)
        assert len(session.calls)==6


def test_expiry_is_rechecked_after_wait_before_redirected_policy_send(finite,tmp_path):
    finite['providers'][0]['authorization']['valid_until']=(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()
    http,session=http_fixture(finite,tmp_path,{START:FakeResponse(status=301,headers={'Location':POLICY})})
    slots=[]
    def wait(**kwargs):
        slots.append(True)
        if len(slots)==2:
            http.registry.sources['publisher'].authorization['valid_until']='2000-01-01T00:00:00+00:00'
    http.limiters['publisher'].wait=wait
    with closing(http),pytest.raises(NewsError,match='^authorization_expired$'):
        http.get(ONE)
    assert len(slots)==2 and [url for url,_ in session.calls]==[START]


def test_conditional_rep_validators_do_not_follow_policy_redirect(finite,tmp_path):
    cache(tmp_path).parent.mkdir(parents=True,exist_ok=True)
    cache(tmp_path).write_text(json.dumps({'origin':ORIGIN,'checked_at':0,'status':200,
        'body':ALLOW.decode(),'etag':'old-root','last_modified':'old-date'}))
    http,session=http_fixture(finite,tmp_path,{
        START:FakeResponse(status=301,headers={'Location':POLICY}),POLICY:FakeResponse(ALLOW),ONE:FakeResponse(b'original')})
    with closing(http):
        assert http.get(ONE).body==b'original'
    first,second=session.calls[0][1]['headers'],session.calls[1][1]['headers']
    assert first['If-None-Match']=='old-root' and first['If-Modified-Since']=='old-date'
    assert 'If-None-Match' not in second and 'If-Modified-Since' not in second


def test_policy_html_without_rules_does_not_grant_finite_article_access(finite,tmp_path):
    http,session=http_fixture(finite,tmp_path,{
        START:FakeResponse(status=301,headers={'Location':POLICY}),
        POLICY:FakeResponse(b'<html><title>Publisher home</title><main>Unrelated content</main></html>')})
    with closing(http):
        with pytest.raises(NewsError,match='^robots_not_plain_rules$'):
            http.get(ONE)
        with pytest.raises(NewsError,match='^robots_origin_policy_cooldown$'):
            http.get(TWO)
    assert [url for url,_ in session.calls]==[START,POLICY]


def test_metadata_only_feed_keeps_its_existing_exact_robots_route(metadata_config,tmp_path):
    start='https://news.ltn.com.tw/robots.txt'
    queue,http,session=metadata_setup(metadata_config,tmp_path,{
        start:FakeResponse(status=301,headers={'Location':'/zh/robots.txt'})})
    try:
        with pytest.raises(NewsError,match='^request_outside_metadata_feed_scope$'):
            http.get(LTN_FEEDS[0],metadata_feed=LTN_FEEDS[0])
        assert [url for url,_ in session.calls]==[start]
        assert not http.registry.sources['ltn'].allowed
    finally:
        http.close();queue.close()
