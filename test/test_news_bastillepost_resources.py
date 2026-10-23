"""Finite resource questions and nonexecuting native boundaries; no live requests."""
from contextlib import closing
from dataclasses import replace
import hashlib
import json
from unittest.mock import patch

from bs4 import BeautifulSoup
import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader.news_fulltext_bastillepost import (
    BASTILLE_ACTIVE_RESOURCE_QUESTION, bastillepost_active_resource_metadata,
)
from downloader.news_fulltext_candidates import candidate_response_metadata
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_bastillepost_native import (
    ID_URL, URL, TITLE, settings, boundary_settings, boundary_failure, boundary_http,
    page, parse, versioned, hop,
)


def submit(queue, config, *, evidence=10):
    return jobs.submit_candidate(queue.root, config, queue.registry, URL,
        queue.registry.sources['bastillepost'].embedded_article_layout,
        reason='New src/type and native sibling relationship question only', evidence_contract_version=evidence)


def pump(queue):
    with patch.object(jobs.time, 'monotonic', return_value=0.0):
        jobs.pump_candidates(queue)


def current_head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())


@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_active_resources_resolve_literal_URI_shapes_without_code_or_queries(engine):
    raw = page(body='<p>正文不得輸出 -12.50。<script async src="//widget.example.test/widgets.js?token=PRIVATE_TOKEN&PRIVATE_KEY=value" type="text/javascript"></script></p>'
        '<script src="/assets/public.js?v=42"></script><script src="../assets/other.js"></script>'
        '<iframe src="https://www.bastillepost.com/player.html?id=22"></iframe>'
        '<script type="application/json">PRIVATE_CODE_AND_NAME</script>')
    soup = BeautifulSoup(raw,engine); before = str(soup)
    facts = bastillepost_active_resource_metadata(soup,page_url=URL)
    nodes = facts['scopes'][0]['bodies'][0]['active_nodes']
    assert [n['src']['src_form'] for n in nodes] == ['protocol_relative','root_relative','relative','absolute','missing']
    first = nodes[0]
    assert first['type_raw'] == 'text/javascript' and first['inline_characters'] == 0
    assert first['src']['resolved_origin'] == 'https://widget.example.test'
    assert first['src']['path_raw'] == '/widgets.js' and first['src']['query_keys'] == ['token']
    assert first['src']['unrecognized_query_key_count_sampled'] == 1
    assert not first['src']['query_values_retained'] and not first['src']['resource_requested']
    assert nodes[-1]['inline_characters'] == len('PRIVATE_CODE_AND_NAME')
    assert len(nodes[-1]['inline_sha256']) == 64
    exported = json.dumps(facts,ensure_ascii=False)
    assert all(text not in exported for text in ('正文不得輸出','PRIVATE_TOKEN','PRIVATE_KEY','PRIVATE_CODE_AND_NAME',TITLE))
    assert not facts['identity_or_scope_granted'] and not facts['raw_or_names_or_body_retained']
    assert facts['question'] == BASTILLE_ACTIVE_RESOURCE_QUESTION and facts['additional_requests'] == 0
    assert str(soup) == before


@pytest.mark.parametrize('src,form',[
    ('','empty'),('javascript:alert(1)','unsupported_or_invalid'),
    ('data:text/html,private','unsupported_or_invalid'),('//user:secret@host.test/x.js','unsupported_or_invalid'),
    ('https://host.test:invalid/x.js','unsupported_or_invalid'),
    ('https://host.test/a b.js','invalid_or_over_bound'),('a'*4097,'invalid_or_over_bound'),
])
def test_bad_resource_URI_is_classified_without_becoming_an_endpoint(src,form):
    soup = BeautifulSoup(page(body=f'<script src="{src}"></script>'),'lxml')
    node = bastillepost_active_resource_metadata(soup,page_url=URL)['scopes'][0]['bodies'][0]['active_nodes'][0]
    assert node['src']['src_form'] == form and node['src']['resolved_origin'] is None
    assert node['src']['path_raw'] is None and not node['src']['resource_requested']


def test_body_relationships_and_budget_are_observed_without_selecting_the_first():
    soup = BeautifulSoup(page(body='<script></script>'*30),'lxml')
    scope = soup.select_one('.article-page__column.__main')
    label = soup.new_tag('h3'); label.string='往下看更多文章'; scope.append(label)
    for _ in range(7):
        body = soup.new_tag('div',attrs={'class':'article-body'})
        body.append(BeautifulSoup('<p>不得輸出次要正文</p><script src="//asset.example.test/e.js"></script>','html.parser'))
        scope.append(body)
    facts = bastillepost_active_resource_metadata(soup,page_url=URL)
    bodies = facts['scopes'][0]['bodies']
    assert len(bodies) == 4 and facts['scopes'][0]['body_output_truncated']
    assert bodies[0]['active_nodes_truncated'] and len(bodies[0]['active_nodes']) == 8
    assert sum(len(b['active_nodes']) for b in bodies) <= 16
    assert bodies[1]['previous_siblings'][0]['known_relationship_label'] == '往下看更多文章'
    assert all(b['direct_native_child'] for b in bodies)
    assert '不得輸出' not in json.dumps(facts,ensure_ascii=False)
    assert len(json.dumps(facts)) < 65536


@pytest.mark.parametrize('version',[2,3,4,5,6,7])
@pytest.mark.parametrize('evidence',[8,9,10])
def test_every_supported_layout_passes_the_real_mailbox_loader(boundary_settings,tmp_path,version,evidence):
    config = versioned(boundary_settings,version)
    with closing(NewsQueue(tmp_path,PolicyRegistry(config,base=tmp_path))) as queue:
        before = boundary_failure(queue)
        request = submit(queue,config,evidence=evidence)
        assert request['submitted']
        path = jobs._inbox(tmp_path)/(request['request_id']+'.json')
        loaded = jobs._load(path)
        assert loaded['candidate_layout'] == f'bastillepost_visible_article_v{version}'
        assert loaded['evidence_contract_version'] == evidence
        pump(queue)
        job = jobs.claim_candidate(queue,boundary_http(queue),config,'bastillepost')
        assert job['request_id'] == request['request_id']
        assert current_head(queue) == before


@pytest.mark.parametrize('layout',['bastillepost_visible_article_v1','bastillepost_visible_article_v8',
    'bastillepost_visible_article_v99','bastillepost_visible_article_v04'])
def test_mailbox_loader_uses_closed_registered_layouts_not_a_numeric_wildcard(boundary_settings,tmp_path,layout):
    config = versioned(boundary_settings,4)
    with closing(NewsQueue(tmp_path,PolicyRegistry(config,base=tmp_path))) as queue:
        boundary_failure(queue); request = submit(queue,config)
        path = jobs._inbox(tmp_path)/(request['request_id']+'.json')
        value = json.loads(path.read_text())
        value['candidate_layout'] = layout
        payload = jobs._bytes(value)
        path = path.with_name(hashlib.sha256(payload).hexdigest()+'.json')
        jobs._write_once(path,payload)
        with pytest.raises(ValueError,match='invalid_native_candidate_request'):
            jobs._load(path)


def test_v4_resource_question_uses_actual_literal_transport_and_does_not_replay_parser(boundary_settings,tmp_path,monkeypatch):
    import downloader.news_fulltext_candidates as candidates
    config = versioned(boundary_settings,4)
    source = PolicyRegistry(config,base=tmp_path).sources['bastillepost']
    monkeypatch.setattr(candidates,'extract_article',lambda *a,**k:pytest.fail('not a parser candidate'))
    request = URL+'?current_cat=138496'
    response = Response(URL,200,{'content-type':'text/html'},page().encode(),(hop(request,URL),))
    facts = candidate_response_metadata(response,source,source,evidence_contract_version=10,requested_url=request)
    assert facts['requested_url'] == request and facts['redirect_count'] == 1
    assert not facts['accepted_as_article'] and not facts['current_parser_replayed']
    for invalid in (replace(response,redirects=()),replace(response,status=403),
            replace(response,body=b'x'*((2 << 20)+1)),replace(response,url=URL.replace('10123210','99999'))):
        with pytest.raises(NewsError):
            candidate_response_metadata(invalid,source,source,evidence_contract_version=10,requested_url=request)


def test_resource_question_is_new_once_only_and_never_rearms_the_completed_old_question(boundary_settings,tmp_path):
    config = versioned(boundary_settings,4)
    with closing(NewsQueue(tmp_path,PolicyRegistry(config,base=tmp_path))) as queue:
        before = boundary_failure(queue)
        client = boundary_http(queue,page(body='<script src="//asset.example.test/e.js"></script>'))
        old = submit(queue,config,evidence=8); pump(queue)
        old_job = jobs.claim_candidate(queue,client,config,'bastillepost')
        jobs.finish_candidate(queue,old_job,result=jobs.fetch_candidate(client,old_job))
        old_row = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(old['request_id'],)).fetchone())
        new = submit(queue,config); pump(queue)
        assert new['request_id'] != old['request_id']
        job = jobs.claim_candidate(queue,client,config,'bastillepost')
        result = jobs.fetch_candidate(client,job)
        jobs.finish_candidate(queue,job,result=result)
        assert client.calls == [URL,URL]  # Two distinct, fixed questions; no resource GET.
        assert current_head(queue) == before and queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(old['request_id'],)).fetchone()) == old_row
        assert not submit(queue,config)['submitted']
        target = jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
        assert jobs._key(target,'a'*64,evidence_contract_version=10) == jobs._key(target,'b'*64,evidence_contract_version=10)
        assert jobs._key(target,'a'*64,evidence_contract_version=10) != jobs._key(target,'a'*64,evidence_contract_version=8)
    with closing(NewsQueue(tmp_path,PolicyRegistry(config,base=tmp_path))) as queue:
        pump(queue)
        assert not submit(queue,config)['submitted']
        assert jobs.claim_candidate(queue,boundary_http(queue),config,'bastillepost') is None


@pytest.mark.parametrize('override',[
    {'due':4102444800.0},{'lease_owner':'other'},{'body_sha256':'a'*64},{'parquet_path':'saved.parquet'},
    {'state':'complete','error_code':None},{'state':'http_429','error_code':'http_429'},
    {'state':'article_rights_or_scope_excluded','error_code':'article_rights_or_scope_excluded'},
])
def test_resource_question_never_changes_saved_refused_leased_or_future_heads(boundary_settings,tmp_path,override):
    config = versioned(boundary_settings,4)
    with closing(NewsQueue(tmp_path,PolicyRegistry(config,base=tmp_path))) as queue:
        before = boundary_failure(queue,**override)
        with pytest.raises(ValueError):
            submit(queue,config)
        assert current_head(queue) == before


@pytest.mark.parametrize('pause',['provider','robots','gateway'])
def test_resource_question_keeps_source_refusal_and_uncertain_outcome_gates(boundary_settings,tmp_path,pause):
    config = versioned(boundary_settings,4)
    with closing(NewsQueue(tmp_path,PolicyRegistry(config,base=tmp_path))) as queue:
        before = boundary_failure(queue); request = submit(queue,config); pump(queue)
        client = boundary_http(queue)
        if pause == 'provider':
            client.provider_not_before['bastillepost'] = 4102444800.0
        elif pause == 'robots':
            client.blocked_robots_origins = lambda p:('https://www.bastillepost.com',)
        else:
            client.blocked_gateway_hosts = lambda p:('www.bastillepost.com',)
        assert jobs.claim_candidate(queue,client,config,'bastillepost') is None
        assert current_head(queue) == before and not client.calls
        client.provider_not_before.clear()
        client.blocked_robots_origins = client.blocked_gateway_hosts = lambda p:()
        assert jobs.claim_candidate(queue,client,config,'bastillepost')
    with closing(NewsQueue(tmp_path,PolicyRegistry(config,base=tmp_path))) as queue:
        pump(queue)
        assert jobs.candidate_status(tmp_path,request['request_id'])['state'] == 'uncertain_outcome'
        assert not submit(queue,config)['submitted']
        assert jobs.claim_candidate(queue,boundary_http(queue),config,'bastillepost') is None


def test_resource_CLI_only_writes_the_existing_mailbox(boundary_settings,tmp_path,monkeypatch):
    from scripts import probe_news_sources as cli
    config = versioned(boundary_settings,4)
    with closing(NewsQueue(tmp_path,PolicyRegistry(config,base=tmp_path))) as queue:
        before = boundary_failure(queue)
        monkeypatch.setattr(cli,'load_config',lambda path:(config,queue.registry,tmp_path))
        monkeypatch.setattr(cli,'NewsHTTP',lambda *a,**k:pytest.fail('no submitter HTTP owner'))
        output = tmp_path/'resource-question.json'
        assert cli.main(['--probe',URL,'--candidate-embedded-layout','bastillepost_visible_article_v4',
            '--require-current-missing-body-failure','--failure-code','body_not_found','--enqueue-candidate',
            '--candidate-evidence-contract','10','--output',str(output)]) == 0
        assert json.loads(output.read_text())['submitted'] and current_head(queue) == before


@pytest.mark.parametrize('evidence',[True,0,12,999])
def test_unknown_question_or_cross_provider_cannot_buy_an_observation(boundary_settings,tmp_path,evidence):
    config = versioned(boundary_settings,4)
    with closing(NewsQueue(tmp_path,PolicyRegistry(config,base=tmp_path))) as queue:
        boundary_failure(queue)
        with pytest.raises(ValueError):
            submit(queue,config,evidence=evidence)
        target = jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
        with pytest.raises(ValueError):
            jobs._key(dict(target,provider='pocket'),'a'*64,evidence_contract_version=10)


LOADER = '<script async src="//www.instagram.com/embed.js"></script>'


def embedded_body(*, loader=LOADER, copies=1):
    # Synthetic prose only. Fallback/caption/credit text remains in source order;
    # a loader is not permission to discard its p, blockquote or whole widget.
    widget = ('<div class="article-content-item type-html"><blockquote>'
        '<p>靜態備援引文 -12.50</p><a href="https://www.instagram.com/p/synthetic/">原站嵌入連結</a>'
        '<p class="caption">原生說明與署名不得刪</p></blockquote><p>'+loader+'</p>'
        '<p>載入器後的文字 0%</p></div>')
    return '<p class="font-size-scaler-control">'+'保留主要原生正文與原始值 1000股。'*8+'</p>'+widget*copies


@pytest.mark.parametrize('engine',['lxml','html.parser'])
@pytest.mark.parametrize('copies',[1,2,8])
def test_v5_empty_native_loader_never_requests_resources_or_discards_static_text(settings,engine,copies,monkeypatch):
    from downloader.news_fulltext import fetch_article
    from types import SimpleNamespace
    import requests
    config = versioned(settings,5); config['providers'][0]['html_parser']=engine
    source = PolicyRegistry(config,base=__import__('pathlib').Path.cwd())
    response = Response(URL,200,{'content-type':'text/html'},page(body=embedded_body(copies=copies)).encode())
    calls=[]
    def get(url,**kwargs):
        calls.append(url)
        assert url == URL
        return response
    monkeypatch.setattr(requests.sessions.Session,'request',lambda *a,**kw:pytest.fail('no external HTTP'))
    result = fetch_article(SimpleNamespace(registry=source,get=get),{'url':URL,'id':bytes.fromhex('12'*16)})
    assert calls == [URL]
    assert result['body'].count('保留主要原生正文')==8
    for raw in ('靜態備援引文 -12.50','原站嵌入連結','原生說明與署名不得刪','載入器後的文字 0%'):
        assert result['body'].count(raw)==copies
    notices = [n for n in json.loads(result['verified_article_notices_json']) if n['kind']=='unfetched_external_social_loader']
    assert len(notices)==copies
    assert all(n['src_raw']=='//www.instagram.com/embed.js' and n['inline_characters']==0 for n in notices)
    assert all(n['static_native_fallback_text_retained'] and not n['resource_requested'] and not n['script_executed']
        and not n['external_rendered_content_acquired'] and n['external_embedding_not_author_or_ownership_proof'] for n in notices)
    assert 'static_native_fallback_text_not_external_rendered_fulltext' in result['body_completeness']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert result['attribution_status']=='source_declared_corporate_author_not_verified_person'


@pytest.mark.parametrize('version',[1,2,3,4])
def test_old_native_versions_do_not_silently_gain_the_new_loader_boundary(settings,version):
    with pytest.raises(NewsError,match='^article_identity_not_verified$'):
        parse(versioned(settings,version),page(body=embedded_body()))


@pytest.mark.parametrize('edit',[
    lambda s:s.replace('//www.instagram.com/embed.js','https://www.instagram.com/embed.js'),
    lambda s:s.replace('//www.instagram.com/embed.js','//instagram.com/embed.js'),
    lambda s:s.replace('//www.instagram.com/embed.js','//www.instagram.com.evil.test/embed.js'),
    lambda s:s.replace('/embed.js','/embed.js?next=other'),
    lambda s:s.replace('/embed.js','/embed.js#fragment'),
    lambda s:s.replace('/embed.js','/%65mbed.js'),
    lambda s:s.replace('async src','src'),
    lambda s:s.replace('async src','async defer src'),
    lambda s:s.replace('async src','async type="text/javascript" src'),
    lambda s:s.replace('async src','async onclick="alert(1)" src'),
    lambda s:s.replace('</script>','alert(1)</script>'),
    lambda s:s.replace('</script>',' </script>'),
    lambda s:s.replace('</script>','<!-- other code --></script>'),
    lambda s:s.replace('<p><script','<p class="unknown"><script'),
    lambda s:s.replace('type-html','type-html unknown'),
    lambda s:s.replace('type-html','type-video'),
    lambda s:s.replace('<p><script','<span><script').replace('</script></p>','</script></span>'),
    lambda s:'<div>'+s+'</div>',
    lambda s:s.replace('<blockquote>','<blockquote hidden>'),
    lambda s:s.replace('type-html','type-html d-none'),
    lambda s:s.replace('<p><script','<p data-paywall="1"><script'),
    lambda s:s.replace('</blockquote>','</blockquote><iframe src="https://www.youtube.com/embed/synthetic"></iframe>'),
    lambda s:s.replace('</blockquote>','</blockquote><svg><path d="x"/></svg>'),
])
@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_v5_unknown_active_src_hidden_or_placement_still_fails_without_discarding_it(settings,engine,edit):
    config=versioned(settings,5);config['providers'][0]['html_parser']=engine
    html=page(body=edit(embedded_body()))
    # The shared document access gate precedes native identity validation.
    code='paywall_or_login' if 'data-paywall="1"' in html else 'article_identity_not_verified'
    with pytest.raises(NewsError,match='^'+code+'$'):
        parse(config,html)


def test_v5_loader_budget_and_unresolved_native_bodies_and_credit_stay_separate(settings):
    config=versioned(settings,5)
    with pytest.raises(NewsError,match='^article_identity_not_verified$'):
        parse(config,page(body=embedded_body(copies=9)))
    with pytest.raises(NewsError,match='^body_not_found$'):
        parse(config,page(body=embedded_body()).replace('<div class="fb-comments">',
            '<div class="article-body"><p>另一正文不得捨棄。</p></div><div class="fb-comments">'))
    with pytest.raises(NewsError,match='^article_rights_or_scope_excluded$'):
        parse(config,page(body=embedded_body()+'<p>（星島日報報道）明列署名必須保留界線。</p>'))
    with pytest.raises(NewsError,match='^body_not_found$'):
        parse(config,page(body='<div class="article-content-item type-html"><p>'+LOADER+'</p></div>'))


def test_v5_preserves_previously_accepted_text_clock_author_notices_and_http_identity(settings):
    from pathlib import Path
    for html in (page(),page(body='<p class="font-size-scaler-control">原生完整短文 0%。</p>')):
        assert parse(versioned(settings,5),html)==parse(versioned(settings,4),html)
    old=PolicyRegistry(versioned(settings,4),base=Path.cwd()).sources['bastillepost']
    new=PolicyRegistry(versioned(settings,5),base=Path.cwd()).sources['bastillepost']
    assert new.data_fingerprint!=old.data_fingerprint and new.parser_repair_prior_capture_fingerprint==old.data_fingerprint
    assert new.authorization_fingerprint==old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint==old.http_capacity_source_fingerprint
    assert new.http_capacity_authorization_fingerprint==old.http_capacity_authorization_fingerprint


@pytest.mark.parametrize('state',['body_not_found','article_identity_not_verified','article_rights_or_scope_excluded',
    'invalid_bulk_publication_clock','unsupported_article_path','robots_disallow','http_403','http_429'])
def test_v5_loader_revision_never_rearms_old_failures_or_resets_attempts(settings,tmp_path,state):
    from test_news_reviewed_repairs import failure,head
    with closing(NewsQueue(tmp_path,PolicyRegistry(versioned(settings,4),base=tmp_path))) as queue:
        before=failure(queue,URL,state=state,error_code=state,attempts=19)
    with closing(NewsQueue(tmp_path,PolicyRegistry(versioned(settings,5),base=tmp_path))) as queue:
        assert head(queue,URL)==before
        queue.reconcile_policy()
        assert head(queue,URL)==before


PLAYER = '<iframe allow="autoplay; encrypted-media" allowfullscreen frameborder="0" src="https://www.youtube.com/embed/abcdefghijk?autoplay=0&amp;opaque_source_key=value"></iframe>'


def video_body(player=PLAYER):
    return ('<p class="font-size-scaler-control">'+('未處理原始正文 -12.50、0%、股數 0。'*12)+'</p>'
        '<div class="youtube-video-container"><p>影片前的圖說與署名不得刪除。</p>'+player+
        '<p>影片後原生說明與連結 <a href="https://example.test/original">原站連結</a>。</p></div>'
        '<p>原文最後段落不得因播放器被截斷。</p>')


@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_v6_inert_player_preserves_all_native_text_and_does_not_fetch_video(settings,engine,monkeypatch):
    from downloader.news_fulltext import fetch_article
    from types import SimpleNamespace
    import requests
    config=versioned(settings,6);config['providers'][0]['html_parser']=engine
    registry=PolicyRegistry(config,base=__import__('pathlib').Path.cwd())
    raw=page(body=video_body()+embedded_body()).encode();calls=[]
    def get(url,**kwargs):
        calls.append(url)
        assert url==URL
        return Response(URL,200,{'content-type':'text/html'},raw)
    monkeypatch.setattr(requests.sessions.Session,'request',lambda *a,**k:pytest.fail('no player/resource request'))
    result=fetch_article(SimpleNamespace(registry=registry,get=get),{'url':URL,'id':bytes.fromhex('12'*16)})
    assert calls==[URL]
    for text in ('影片前的圖說與署名不得刪除','影片後原生說明與連結','原站連結',
            '原文最後段落不得因播放器被截斷','靜態備援引文 -12.50'):
        assert result['body'].count(text)==1
    notices=json.loads(result['verified_article_notices_json'])
    video,=[n for n in notices if n['kind']=='unfetched_external_video_player']
    assert video['provider']=='youtube' and video['src_raw'].endswith('opaque_source_key=value')
    assert video['static_native_fallback_text_retained'] and not video['external_rendered_content_acquired']
    assert not video['resource_requested'] and not video['script_executed']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert any(n['provider']=='instagram' for n in notices if n['kind']=='unfetched_external_social_loader')


@pytest.mark.parametrize('version',[1,2,3,4,5])
def test_only_v6_adds_the_new_player_boundary(settings,version):
    with pytest.raises(NewsError,match='^article_identity_not_verified$'):
        parse(versioned(settings,version),page(body=video_body()))


@pytest.mark.parametrize('edit',[
    lambda s:s.replace('https://www.youtube.com','http://www.youtube.com'),
    lambda s:s.replace('www.youtube.com','www.youtube.com.evil.test'),
    lambda s:s.replace('www.youtube.com','user@www.youtube.com'),
    lambda s:s.replace('www.youtube.com','www.youtube.com:443'),
    lambda s:s.replace('/embed/abcdefghijk','/watch/abcdefghijk'),
    lambda s:s.replace('abcdefghijk','bad'),
    lambda s:s.replace('?autoplay=0','?broken=%ZZ'),
    lambda s:s.replace('?autoplay=0','#fragment'),
    lambda s:s.replace('frameborder="0"','frameborder="1"'),
    lambda s:s.replace('allowfullscreen','allowfullscreen="false"'),
    lambda s:s.replace('allowfullscreen','onload="run()" allowfullscreen'),
    lambda s:s.replace('</iframe>','fallback must not vanish</iframe>'),
    lambda s:s.replace('</iframe>','<!-- must not vanish --></iframe>'),
    lambda s:s.replace('src="','srcdoc="not allowed" src="'),
    lambda s:s.replace('allow="','hidden allow="'),
    lambda s:s.replace('allow="','data-paywall="1" allow="'),
])
@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_v6_unknown_or_nonempty_player_is_not_silently_stripped(settings,engine,edit):
    config=versioned(settings,6);config['providers'][0]['html_parser']=engine
    html=page(body=video_body(edit(PLAYER)))
    code='paywall_or_login' if 'data-paywall="1"' in html else 'article_identity_not_verified'
    with pytest.raises(NewsError,match='^'+code+'$'):
        parse(config,html)


def test_v6_requires_direct_native_player_placement_and_preserves_old_results(settings,tmp_path):
    for body in (video_body().replace('youtube-video-container','other-player'),
            '<section>'+video_body()+'</section>'):
        with pytest.raises(NewsError,match='^article_identity_not_verified$'):
            parse(versioned(settings,6),page(body=body))
    for body in ('',embedded_body(),'<p class="font-size-scaler-control">原生短文 0%。</p>'):
        raw=page() if not body else page(body=body)
        assert parse(versioned(settings,6),raw)==parse(versioned(settings,5),raw)
    old=PolicyRegistry(versioned(settings,5),base=tmp_path).sources['bastillepost']
    new=PolicyRegistry(versioned(settings,6),base=tmp_path).sources['bastillepost']
    assert new.parser_repair_prior_capture_fingerprint==old.data_fingerprint!=new.data_fingerprint
    assert (new.authorization_fingerprint,new.http_capacity_source_fingerprint,new.http_capacity_authorization_fingerprint)==(
        old.authorization_fingerprint,old.http_capacity_source_fingerprint,old.http_capacity_authorization_fingerprint)


@pytest.mark.parametrize('state',['body_not_found','article_identity_not_verified','article_rights_or_scope_excluded',
    'invalid_bulk_publication_clock','unsupported_article_path','robots_disallow','http_403','http_429'])
def test_v6_does_not_rearm_prior_heads(settings,tmp_path,state):
    from test_news_reviewed_repairs import failure,head
    with closing(NewsQueue(tmp_path,PolicyRegistry(versioned(settings,5),base=tmp_path))) as queue:
        before=failure(queue,URL,state=state,error_code=state,attempts=19)
    with closing(NewsQueue(tmp_path,PolicyRegistry(versioned(settings,6),base=tmp_path))) as queue:
        queue.reconcile_policy()
        assert head(queue,URL)==before
