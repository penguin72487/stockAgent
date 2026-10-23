"""CLI startup must not depend on the shell's PYTHONPATH or current folder."""
import os
import json
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError
from scripts import probe_news_sources as probes
from test_news_fulltext import config, FakeResponse, article_html, http_fixture


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/probe_news_sources.py'


@pytest.mark.parametrize('launch', ('isolated_external_script', 'repo_script', 'repo_module'))
@pytest.mark.parametrize('script_name', ['probe_news_sources', 'diagnose_news_access'])
def test_news_probe_help_resolves_current_checkout_without_network(tmp_path, launch, script_name):
    script = ROOT / 'scripts' / (script_name + '.py')
    env = {key: value for key, value in os.environ.items() if key != 'PYTHONPATH'}
    if launch == 'isolated_external_script':
        args, cwd = [sys.executable, '-I', str(script), '--help'], tmp_path
    elif launch == 'repo_script':
        args, cwd = [sys.executable, str(script), '--help'], ROOT
    else:
        args, cwd = [sys.executable, '-m', 'scripts.' + script_name, '--help'], ROOT
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert ('--current-failure-provider' if script_name == 'probe_news_sources' else '--resolve-redirect-only') in result.stdout
    assert '--require-current-missing-body-failure' in result.stdout
    if script_name == 'probe_news_sources':
        assert '--retain-reviewed-response' in result.stdout
        assert '--metadata-only' in result.stdout
    assert 'Traceback' not in result.stderr


URL = 'https://publisher.test/article/1'
ROBOTS = 'https://publisher.test/robots.txt'


@pytest.mark.parametrize('metadata_only', [False, True])
def test_representation_question_skips_extraction_without_inventing_body_failure(
        config, tmp_path, monkeypatch, capsys, metadata_only):
    payload = b'<html><title>Known title</title><article class="story"><p>Native structure</p></article></html>'
    http, session = http_fixture(config, tmp_path, {
        ROBOTS: FakeResponse(b'User-agent: *\nAllow: /'), URL: FakeResponse(payload)})
    attempts = []
    def rejected_extraction(*args, **kwargs):
        attempts.append((args, kwargs))
        raise NewsError('body_not_found')
    monkeypatch.setattr(probes, 'extract_article', rejected_extraction)
    try:
        item = probes.probe_provider(http, [URL], metadata_only=metadata_only)[0]
        assert [call[0] for call in session.calls] == [ROBOTS, URL]
        assert item['status'] == 200 and item['bytes'] == len(payload)
        assert item['containers']['.story'][0]['characters'] == len('Native structure')
        assert not item['article_durably_accepted']
        assert 'private_diagnostic_path' not in item
        assert 'extracted' not in item and 'body_characters' not in item
        assert len(attempts) == (0 if metadata_only else 1)
        if metadata_only:
            assert item['source_probe_contract_version'] == 3
            assert item['diagnostic_mode'] == 'representation_metadata_only'
            assert item['article_extraction'] == 'not_run'
            assert 'extraction_error' not in item and 'extraction_failure_location' not in item
        else:
            assert item['source_probe_contract_version'] == 2
            assert item['extraction_error'] == 'body_not_found'
            assert 'diagnostic_mode' not in item and 'article_extraction' not in item
        assert not list(tmp_path.rglob('*.parquet'))
        assert not (tmp_path/'queue.sqlite3').exists()
    finally:
        http.close()
    capsys.readouterr()


def test_default_probe_still_validates_article_and_preserves_legacy_contract(config, tmp_path, capsys):
    http, _ = http_fixture(config, tmp_path, {
        ROBOTS: FakeResponse(b'User-agent: *\nAllow: /'), URL: FakeResponse(article_html())})
    try:
        item = probes.probe_provider(http, [URL])[0]
        assert item['source_probe_contract_version'] == 2 and item['body_characters'] > 100
        assert item['extracted']['body_sha256'] and 'extraction_error' not in item
        assert not item['article_durably_accepted']
    finally:
        http.close()
    capsys.readouterr()


@pytest.mark.parametrize('kind', ['article', 'bulk'])
def test_retained_original_and_receipt_are_private_without_another_request(config, tmp_path, capsys, kind):
    import hashlib
    from downloader.artifact_io import sha256_file
    output = tmp_path/'private-source-diagnostics'
    if kind == 'article':
        payload = article_html()
        http, session = http_fixture(config, tmp_path, {
            ROBOTS: FakeResponse(b'User-agent: *\nAllow: /'), URL: FakeResponse(payload)})
        url, expected_requests = URL, 2
    else:
        payload = b'{"items":{"data":[{"newsId":1,"payment":0,"content":"synthetic public diagnostic"}]}}'
        url, expected_requests = 'https://publisher.test/api', 1
        source = SimpleNamespace(provider='cnyes', cnyes_apis=(url,))
        session = SimpleNamespace(calls=[])
        def get(target):
            session.calls.append(target)
            return Response(url, 200, {'content-type':'application/json'}, payload)
        http = SimpleNamespace(registry=SimpleNamespace(require=lambda _:source),
                               provider_not_before={}, robot_delays={}, get=get, close=lambda:None)
    try:
        item = probes.probe_provider(http, [url], retain=True, output_root=output)[0]
        raw_path = Path(item['private_diagnostic_path'])
        receipt_path = raw_path.with_suffix('.receipt.json')
        assert len(session.calls) == expected_requests
        assert all(not p.stat().st_mode & 0o077 for p in (output, raw_path, receipt_path))
        receipt = json.loads(receipt_path.read_text())
        assert receipt['sha256'] == sha256_file(raw_path)
        assert receipt['response_sha256'] == hashlib.sha256(payload).hexdigest()
        assert not receipt['accepted_as_article'] and not receipt['publish']
        assert not item['article_durably_accepted']
    finally:
        http.close()
    capsys.readouterr()


@pytest.mark.parametrize('root_kind', ['missing', 'public', 'symlink'])
def test_retention_rejects_nonprivate_destination_before_http(tmp_path, root_kind):
    output = None if root_kind == 'missing' else tmp_path/'destination'
    if root_kind == 'public':
        output.mkdir(mode=0o755)
        output.chmod(0o755)
    elif root_kind == 'symlink':
        target = tmp_path/'private'
        target.mkdir(mode=0o700)
        output.symlink_to(target, target_is_directory=True)
    http = SimpleNamespace(registry=SimpleNamespace(require=lambda *_:pytest.fail('no HTTP before private scope')))
    with pytest.raises(ValueError, match='private_diagnostic_directory_required'):
        probes.probe_provider(http, [URL], retain=True, output_root=output)


@pytest.mark.parametrize('incompatible', [{'retain': True}, {'candidate_source': object()}])
def test_metadata_only_direct_call_rejects_retention_or_candidate_before_http(monkeypatch, incompatible):
    http = SimpleNamespace(registry=SimpleNamespace(require=lambda *_: pytest.fail('scope used before mode guard')))
    with pytest.raises(ValueError, match='metadata_only_probe_cannot_retain_or_validate_candidates'):
        probes.probe_provider(http, [URL], metadata_only=True, **incompatible)


@pytest.mark.parametrize('provided_directory', [False, True])
def test_candidate_retention_conflict_keeps_legacy_reason_before_directory_side_effect(tmp_path, provided_directory):
    output = tmp_path/'must-not-create' if provided_directory else None
    http = SimpleNamespace(registry=SimpleNamespace(require=lambda *_:pytest.fail('candidate conflict before source lookup')))
    with pytest.raises(ValueError, match='^metadata_only_same_source_candidate_required$'):
        probes.probe_provider(http, [URL], retain=True, output_root=output,candidate_source=object())
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('refusal,expected,requests', [
    ('robots', 'robots_disallowed', 1), ('403', 'source_access_denied', 2), ('429', 'http_429', 2),
])
def test_metadata_only_keeps_canonical_refusals_without_extraction_or_raw(
        config, tmp_path, monkeypatch, capsys, refusal, expected, requests):
    robots = b'User-agent: *\nDisallow: /' if refusal == 'robots' else b'User-agent: *\nAllow: /'
    http, session = http_fixture(config, tmp_path, {
        ROBOTS: FakeResponse(robots), URL: FakeResponse(b'NEVER RETAIN REFUSED BODY', status=int(refusal) if refusal != 'robots' else 200)})
    monkeypatch.setattr(probes, 'extract_article', lambda *_a, **_k: pytest.fail('refused extraction'))
    try:
        item = probes.probe_provider(http, [URL], metadata_only=True)[0]
        assert item['error'] == expected and len(session.calls) == requests
        assert item['article_extraction'] == 'not_run' and not item['article_durably_accepted']
        assert 'extraction_error' not in item and 'private_diagnostic_path' not in item
        assert not list(tmp_path.rglob('*.parquet'))
        if refusal in {'403', '429'}:
            assert http.provider_not_before['publisher'] > time.time()
    finally:
        http.close()
    assert 'NEVER RETAIN REFUSED BODY' not in capsys.readouterr().out


def test_metadata_only_preserves_existing_provider_cooldown_without_requests(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {})
    deadline = time.time()+600
    http.provider_not_before['publisher'] = deadline
    try:
        item = probes.probe_provider(http, [URL], metadata_only=True)[0]
        assert item['error'] == 'persisted_provider_cooldown' and not item['sent']
        assert item['article_extraction'] == 'not_run' and item['source_probe_contract_version'] == 3
        assert http.provider_not_before['publisher'] == deadline and session.calls == []
    finally:
        http.close()


@pytest.mark.parametrize('metadata_only', [False, True])
@pytest.mark.parametrize('kind', ['feed', 'bulk'])
def test_representation_mode_skips_feed_and_bulk_body_validation(monkeypatch, capsys, metadata_only, kind):
    from downloader import news_fulltext_bulk, news_fulltext_feed
    calls = []
    url = 'https://publisher.test/feed' if kind == 'feed' else 'https://publisher.test/api'
    def validate(*_args, **_kwargs):
        calls.append('body_validation')
        return [{}], []
    if kind == 'feed':
        source = SimpleNamespace(provider='publisher', fulltext_feeds=(url,))
        payload, content_type = b'<rss><channel><item><title>Metadata</title><link>https://publisher.test/a</link></item></channel></rss>', 'application/xml'
        monkeypatch.setattr(news_fulltext_feed, 'feed_records', validate)
        field = 'feed_valid_body_rows'
    else:
        source = SimpleNamespace(provider='cnyes', cnyes_apis=(url,))
        payload, content_type = b'{"items":{"data":[{"newsId":1,"payment":0,"content":"not exported"}]}}', 'application/json'
        monkeypatch.setattr(news_fulltext_bulk, 'cnyes_records', validate)
        monkeypatch.setattr(news_fulltext_bulk, 'cnyes_pagination', lambda *_: calls.append('pagination'))
        field = 'bulk_validated_body_rows'
    requests = []
    def get(target):
        requests.append(target)
        return Response(url, 200, {'content-type': content_type}, payload)
    http = SimpleNamespace(registry=SimpleNamespace(require=lambda _: source),
                           provider_not_before={}, robot_delays={}, get=get)
    item = probes.probe_provider(http, [url], metadata_only=metadata_only)[0]
    assert requests == [url] and not item['article_durably_accepted']
    assert ('body_validation' in calls) is (not metadata_only)
    if metadata_only:
        assert calls == [] and field not in item and item['article_extraction'] == 'not_run'
    else:
        assert item[field] == 1
    assert 'not exported' not in capsys.readouterr().out


@pytest.mark.parametrize('selection,flags', [
    (['--probe', URL], ['--retain-reviewed-response']),
    (['--probe', URL], ['--require-current-missing-body-failure']),
    (['--probe', URL], ['--candidate-embedded-layout', 'videoland_visible_article_v4']),
    (['--probe', URL], ['--enqueue-candidate']),
    (['--probe', URL], ['--candidate-evidence-contract', '2']),
    (['--probe', URL], ['--failure-code', 'unsupported_article_path']),
    (['--current-failure-provider', 'publisher'], []),
    (['--candidate-request-status', 'known-request'], []),
])
def test_metadata_only_cli_rejects_unrelated_grants_before_config_or_http(tmp_path, monkeypatch, selection, flags):
    monkeypatch.setattr(probes, 'load_config', lambda *_: pytest.fail('invalid mode loaded source configuration'))
    with pytest.raises(SystemExit) as error:
        probes.main([*selection, '--metadata-only', '--output', str(tmp_path/'result.json'), *flags])
    assert error.value.code == 2 and not list(tmp_path.iterdir())


@pytest.mark.parametrize('metadata_only', [False, True])
def test_metadata_only_cli_routes_opt_in_and_output_contract(config, tmp_path, monkeypatch, metadata_only, capsys):
    http, session = http_fixture(config, tmp_path/'http', {
        ROBOTS: FakeResponse(b'User-agent: *\nAllow: /'), URL: FakeResponse(article_html())})
    config['root'] = str(tmp_path/'http')
    monkeypatch.setattr(probes, 'load_config', lambda *_: (config, http.registry, tmp_path))
    monkeypatch.setattr(probes, 'NewsHTTP', lambda *_a, **_k: http)
    output = tmp_path/'result.json'
    args = ['--probe', URL, '--output', str(output)] + (['--metadata-only'] if metadata_only else [])
    assert probes.main(args) == 0
    result = json.loads(output.read_text())
    assert result['source_probe_contract_version'] == (3 if metadata_only else 2)
    assert result['probes'][0]['source_probe_contract_version'] == result['source_probe_contract_version']
    assert [call[0] for call in session.calls] == [ROBOTS, URL]
    assert ('diagnostic_mode' in result) is metadata_only
    assert ('extracted' in result['probes'][0]) is (not metadata_only)
    assert not (tmp_path/'private-source-diagnostics').exists()
    assert not list(tmp_path.rglob('*.parquet'))
    capsys.readouterr()


@pytest.mark.parametrize('metadata_only', [False, True])
def test_xml_diagnostics_do_not_attribute_an_earlier_parser_error_to_this_response(metadata_only, capsys):
    from lxml import etree
    # Keep ambient error state deliberately dirty, as when another provider
    # previously ran on the same ThreadPoolExecutor worker. Never clear a
    # process/thread error log as a production workaround.
    try:
        etree.fromstring(b'<unrelated>&undefined;</unrelated>',
                         etree.XMLParser(resolve_entities=False, no_network=True))
    except etree.XMLSyntaxError:
        pass
    payload = b'<?xml version="1.0"?><rss><channel><item></rss>'
    source = SimpleNamespace(provider='publisher')
    http = SimpleNamespace(registry=SimpleNamespace(require=lambda _: source),
        provider_not_before={}, robot_delays={},
        get=lambda target: Response(target, 200, {'content-type':'application/xml'}, payload))
    item = probes.probe_provider(http, [URL], metadata_only=metadata_only)[0]
    assert item['error'] == 'invalid_feed'
    assert item['xml_error_log_contract_version'] == 2
    assert item['xml_parse_error_scope'] == 'current_response_parser'
    assert item['xml_parse_errors'] and len(item['xml_parse_errors']) <= 8
    assert all(e['type'] == 'ERR_TAG_NAME_MISMATCH' for e in item['xml_parse_errors'])
    assert 'ERR_UNDECLARED_ENTITY' not in json.dumps(item)
    assert not item['article_durably_accepted'] and 'private_diagnostic_path' not in item
    assert 'undefined' not in capsys.readouterr().out
