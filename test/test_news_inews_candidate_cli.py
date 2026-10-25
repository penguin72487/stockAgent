"""The CLI and resident owner expose the same finite iNEWS question versions."""
import json

import pytest

from downloader.news_fulltext_policy import PolicyRegistry
from scripts import probe_news_sources as cli
from test_news_setn_inews import URL, v5_entry, v8_entry


@pytest.fixture
def current_cli(tmp_path,monkeypatch):
    config={'contract_version':1,'root':'news','collection_purpose':'personal_research',
        'publish':False,'remote_training_export':False,'providers':[v5_entry()]}
    registry=PolicyRegistry(config,base=tmp_path)
    monkeypatch.setattr(cli,'load_config',lambda _: (config,registry,tmp_path))
    monkeypatch.setattr(cli,'require_current_missing_body_failures',lambda *a,**k: {})
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a,**k: pytest.fail('the CLI cannot create another HTTP owner'))


@pytest.mark.parametrize('version', [24,25,26])
def test_reviewed_inews_questions_reach_the_existing_submitter(tmp_path,monkeypatch,current_cli,version):
    called=[]
    def submit(*args,**kwargs):
        called.append((args,kwargs))
        return {'contract_version':1,'submitted':True,'request_id':'a'*64}
    monkeypatch.setattr('downloader.news_fulltext_candidate_diagnostics.submit_candidate',submit)
    output=tmp_path/'result.json'
    assert cli.main(['--probe',URL,'--enqueue-candidate','--candidate-evidence-contract',str(version),
        '--require-current-missing-body-failure','--failure-code','article_identity_not_verified',
        '--candidate-embedded-layout','setn_inews_native_article_v5','--output',str(output)])==0
    assert len(called)==1 and called[0][1]['evidence_contract_version']==version
    assert called[0][0][3]==URL and json.loads(output.read_text())['submitted']


def test_named_header_question_cannot_be_run_as_a_direct_http_probe(tmp_path,current_cli):
    with pytest.raises(SystemExit) as error:
        cli.main(['--probe',URL,'--candidate-evidence-contract','26',
            '--candidate-embedded-layout','setn_inews_native_article_v5','--output',str(tmp_path/'result.json')])
    assert error.value.code==2


@pytest.mark.parametrize('version', [38,39])
def test_body_failure_question_uses_resident_v8_submitter(tmp_path,monkeypatch,current_cli,version):
    config, _, repo = cli.load_config(None)
    config['providers'] = [v8_entry()]
    registry = PolicyRegistry(config, base=repo)
    monkeypatch.setattr(cli, 'load_config', lambda _: (config, registry, repo))
    calls = []
    def submit(*args, **kwargs):
        calls.append((args, kwargs))
        return {'submitted': True, 'request_id': 'a'*64}
    monkeypatch.setattr('downloader.news_fulltext_candidate_diagnostics.submit_candidate', submit)
    assert cli.main(['--probe', URL, '--enqueue-candidate', '--candidate-evidence-contract', str(version),
        '--require-current-missing-body-failure', '--failure-code', 'body_not_found',
        '--candidate-embedded-layout', 'setn_inews_native_article_v8',
        '--output', str(tmp_path/'body-question.json')]) == 0
    assert len(calls) == 1 and calls[0][1]['evidence_contract_version'] == version


@pytest.mark.parametrize('layout,failure,enqueue', [
    ('setn_inews_native_article_v8', 'body_not_found', False),
    ('setn_inews_native_article_v5', 'body_not_found', True),
    ('setn_inews_native_article_v8', 'article_identity_not_verified', True),
])
def test_body_failure_question_cannot_expand_to_direct_http_other_layouts_or_failures(
        tmp_path, current_cli, layout, failure, enqueue):
    args = ['--probe', URL, '--candidate-evidence-contract', '38',
        '--require-current-missing-body-failure', '--failure-code', failure,
        '--candidate-embedded-layout', layout, '--output', str(tmp_path/'rejected.json')]
    if enqueue:
        args.append('--enqueue-candidate')
    with pytest.raises(SystemExit) as error:
        cli.main(args)
    assert error.value.code == 2
