"""Review observations never become source permissions or collector revisions."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path

import pytest

from downloader.artifact_io import atomic_write_json
from stockagent.data.news_source_catalog import (
    configuration_revision, load_news_configuration, load_source_review_ledger,
    planning_review_path,
)
from stockagent.live.news_dashboard import build_news_public_status, news_snapshot_revision
from test_news_dashboard import NOW, _seed


def _ledger():
    return {"contract_version": 1, "providers": {
        "moneydj": {"capture_decision": "permission_required",
                    "evidence_urls": ["https://example.org/terms"],
                    "observation": "PRIVATE_REVIEW_BODY_AND_KEY",
                    "reviewed_at_utc": NOW.isoformat()}},
        "provider_aliases": {"unregistered:example.org": "moneydj"}}


def test_videoland_review_alias_and_ithome_incomplete_scope_do_not_grant_training():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['videoland']
    assert reviews['unregistered:news.videoland.com.tw'] == review
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert 'https://www.videoland.com.tw/Privacy.htm' in review['evidence_urls']
    for boundary in ('9 old newspage.aspx', 'not independently verified',
                     'not open-license', 'all937-source acceptance remain separate'):
        assert boundary in review['observation']
    other = reviews['unregistered:www.ithome.com.tw']
    assert other['capture_decision'] == 'review_incomplete'
    assert 'member-service database' in other['observation']
    assert 'public anonymous-news versus member-database' in other['observation']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    assert config['publish'] is config['remote_training_export'] is False
    source = next(source for source in config['providers'] if source['id']=='videoland')
    assert source['private_known_url_scope']=='personal_noncommercial_originals_v1'
    assert not any(source.get(key) for key in ('feeds','sitemaps','wordpress_apis','fulltext_feeds'))
    assert not any('www.ithome.com.tw' in source['domains'] and source.get('enabled')
                   for source in config['providers'])


def test_idn_completed_body_cohort_and_unresolved_native_cases_stay_separate():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['idn']
    assert reviews['unregistered:www.idn.com.tw'] == review
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert 'https://www.idn.com.tw/' in review['evidence_urls']
    text = review['observation']
    for current in ('195 current bodies', 'Exactly2 remain missing', '此文章已刪除',
                    'publication clocks NULL', 'without refetch/reparse',
                    'every IDN parser version', 'full937-source goal remain active'):
        assert current in text
    prior = review['prior_reviews'][0]
    assert prior['reviewed_at_utc'] < review['reviewed_at_utc']
    text = prior['observation']
    for previous in ('193 current bodies', 'Exactly4 remain unresolved',
                     'Earlier v1 milestone retained with its original clock'):
        assert previous in text
    for evidence in ('197-URL cohort', '187 current bodies', 'Exactly10 remain unresolved',
                     'not an inferred count of unique articles or all IDN history',
                     'current header calendar and URL filename are not release-time witnesses',
                     'a zero-HTTP import is not falsely claimed', 'not an official/theoretical',
                     'All78 previous source', 'full937-source goal remain active'):
        assert evidence in text


def _configure(root, *, ledger=None):
    _seed(root)
    config_path = root / "configs/news_fulltext.json"
    config = json.loads(config_path.read_text())
    atomic_write_json(config_path, {**config, "provider_catalog": "news/catalog.json"})
    atomic_write_json(root / "configs/news/catalog.json", {
        "contract_version": 1, "providers": [], "planning_review_ledger": "reviews.json"})
    path = root / "configs/news/reviews.json"
    atomic_write_json(path, ledger or _ledger())
    return config_path, path


def test_private_ledger_preserves_evidence_aliases_and_exact_review_precedence(tmp_path):
    value = _ledger()
    exact = {**value["providers"]["moneydj"], "capture_decision": "review_incomplete"}
    value["providers"]["unregistered:example.org"] = exact
    path = tmp_path / "reviews.json"
    atomic_write_json(path, value)
    reviews = load_source_review_ledger(path)
    assert reviews["moneydj"]["observation"] == "PRIVATE_REVIEW_BODY_AND_KEY"
    assert reviews["unregistered:example.org"] == exact


@pytest.mark.parametrize('provider,decision,reviewed_at', [
    ('unregistered:blog.fugle.tw', 'permission_required', '2026-10-11T15:16:52.543736+00:00'),
    ('unregistered:school.gugu.fund', 'review_incomplete', '2026-10-20T14:20:46+00:00'),
    ('unregistered:turnnewsapp.com', 'review_incomplete', '2026-10-20T15:19:58.796031+00:00'),
    ('unregistered:www.top1markets.com', 'permission_required', '2026-10-20T15:24:16+00:00'),
    ('unregistered:pinview.com.tw', 'review_incomplete', '2026-10-20T19:40:27+00:00'),
])
def test_indexed_source_reviews_do_not_create_collectors_or_rewrite_review_clocks(provider, decision, reviewed_at):
    """A completed policy diagnosis must remain discoverable without new GETs."""
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[provider]
    assert review['capture_decision'] == decision
    assert review['reviewed_at_utc'] == reviewed_at
    host = provider.removeprefix('unregistered:')
    assert not any(host in source['domains'] for source in before[1]['providers'])
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_fugle_handoff_preserves_prior_review_not_new_body_or_entitlement():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['unregistered:blog.fugle.tw']
    assert 'https://www.fugle.tw/service' in review['evidence_urls']
    assert 'already completed2026-10-11' in review['observation']
    assert 'not an audit of every later known URL' in review['observation']
    assert 'no new article GET' in review['observation']


def test_mitrade_completed_admission_preserves_the_prior_offline_stage_and_remaining_scope():
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['mitrade']
    assert reviews['unregistered:www.mitrade.com'] == review
    assert review['capture_decision'] == 'reviewed_existing_scope'
    v4_review, = [r for r in [review, *review.get('prior_reviews', [])]
                  if r['reviewed_at_utc'] == '2026-10-20T22:51:16.270879+00:00']
    previous = review['previous_review']
    assert previous['capture_decision'] == 'private_public_candidate'
    assert previous['reviewed_at_utc'] == '2026-10-20T21:56:37+00:00'
    for phrase in ('three literal article200', 'zero additional source requests', 'NUL controls',
                   'not historical PIT', '140 local heads', '19 other category/locale paths',
                   'production source enablefalse', 'guarded producer admission',
                   'do not redownload the three originals', 'not independently verified people'):
        assert phrase in previous['observation']
    for phrase in ('zero additional publisher requests', 'original observation clocks',
                   'all77', '937 source keys', 'regional policy review', 'not historical PIT',
                   'do not redownload the three accepted originals', '103 durable bodies',
                   'preserving all102 accepted complete heads', 'exhausted diagnostic budgets',
                   'do not assume all56 share the newly reviewed credit'):
        assert phrase.lower() in v4_review['observation'].lower()
    source = next(row for row in before[1]['providers'] if row['id'] == 'mitrade')
    assert source['enabled'] and source['private_known_url_scope'] == 'personal_noncommercial_originals_v1'
    assert not source['feeds'] and not source['sitemaps'] and not source['wordpress_apis']
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_mitrade_v5_missing_only_scope_keeps_measurement_permission_and_eta_boundaries_separate():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    latest = reviews['mitrade']
    review, = [r for r in [latest, *latest.get('prior_reviews', [])]
               if r['reviewed_at_utc'] == '2026-10-20T23:09:03.688794+00:00']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    for name in ('alison-ho', 'tony'):
        assert 'https://www.mitrade.com/zh/insights/profile/'+name in review['evidence_urls']
    for phrase in ('preserved120 complete heads byte-identically', 'only7 new bodies',
                   '127 accumulated durable bodies', 'Remaining31 native exclusions',
                   'one zh-au', 'not an independently verified person', 'all77 other',
                   'Current body authorization/provenance still uses the new policy',
                   'Persisted429 floors and Retry-After remain authoritative',
                   'old-owner ETA windows are not new-owner samples',
                   'not an official or theoretical maximum', 'no healthy-body refetch',
                   'CNYES priority and CNA deferral'):
        assert phrase in review['observation']


def test_mitrade_v6_selective_retry_reuses_completed_evidence_without_repeating_all_exclusions():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    latest = reviews['mitrade']
    review, = [r for r in [latest, *latest.get('prior_reviews', [])]
               if r['reviewed_at_utc'] == '2026-10-20T23:21:42.235814+00:00']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert 'https://www.mitrade.com/zh/insights/profile/Invest' in review['evidence_urls']
    for phrase in ('requeued only the two inspected missing originals', 'not all31',
                   '127 healthy heads', '157 total unselected heads', 'all77 other',
                   '129 accumulated durable bodies', 'only2 new native pen-name bodies',
                   'without reparsing', 'Remaining29 native exclusions', 'one zh-au',
                   'not an official or theoretical maximum', 'actual new authorization',
                   'diagnostic budgets', 'empty new-owner ETA windows', 'AI assistance',
                   'not silent human attribution', 'CNYES priority and CNA deferral'):
        assert phrase in review['observation']


def test_mitrade_v7_separates_company_reviewer_AI_and_reuses_immutable_completed_work():
    latest = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['mitrade']
    review, = [r for r in [latest, *latest.get('prior_reviews', [])]
               if r['reviewed_at_utc'] == '2026-10-20T23:47:21.459880+00:00']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert 'https://www.mitrade.com/zh/insights/profile/mitrade-team' in review['evidence_urls']
    assert len({r['reviewed_at_utc'] for r in latest['prior_reviews']}) == len(latest['prior_reviews'])
    for phrase in ('requeued only one inspected missing company article', 'not all29',
                   '129 healthy heads', '158 total unselected heads', 'all77 other',
                   '130 accumulated durable bodies', 'only1 new company body', 'without reparsing',
                   'literal schema Person', 'not verified people', 'AI-assisted/editorially proofread',
                   'saved separately from financial risk and prose', 'before any publisher request',
                   'Remaining28 native exclusions', 'one zh-au', 'diagnostic budgets',
                   'not an official or theoretical maximum', 'no all-history ETA',
                   'CNYES priority and CNA deferral'):
        assert phrase in review['observation']


def test_mitrade_legacy_missing_only_delta_preserves_v7_scope_and_completed_evidence():
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['mitrade']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert review['reviewed_at_utc'] == '2026-10-21T00:07:31.588240+00:00'
    assert len(review['prior_reviews']) == 4
    assert len({r['reviewed_at_utc'] for r in [review, *review['prior_reviews']]}) == 5
    for phrase in ('full-v7-fingerprint-pinned', 'pre-v6-clock', 'ABI1 semantic requests remain compatible',
                   'once-per-parser admission', 'without assuming their authors', 'preserved all78 source identities',
                   'Exactly28 missing candidates', '28 actual article requests', '28 new durable bodies',
                   '158 accumulated durable bodies', 'reusing130 immutable body/Parquet proofs without reparsing',
                   'only28 new bodies', 'All131 unselected full queue rows remained byte-identical',
                   '937 source keys,159 known URLs and158 stored bodies', 'one zh-au regional route',
                   'reload retained prior measured2-second pacing', 'ramped to a1-second candidate',
                   'This remains ramping', 'no healthy article was refetched for rate testing',
                   'not full-site history', 'not an official or theoretical maximum', 'No healthy-body refetch',
                   'CNYES priority and CNA deferral'):
        assert phrase in review['observation']
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_digitimes_review_separates_personal_reading_member_database_and_training_scope():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['unregistered:www.digitimes.com.tw']
    assert review['capture_decision'] == 'review_incomplete'
    assert review['evidence_urls'] == ['https://www.digitimes.com.tw/svc/cprgt.asp',
        'https://www.digitimes.com.tw/svc/svc_abs.asp?showtype=106']
    for phrase in ('130 known main-host', 'member/API', 'database storage', 'AI or program-training',
                   'Public native articles still need', 'no blanket claim that all130 are paywalled',
                   'HTML-encoded query separators', 'not a guessed HTTPS/decoded alias',
                   'canonical article/REP reachability', 'source activationfalse', 'CNYES priority and CNA deferral'):
        assert phrase in review['observation']


def test_pocket_review_alias_is_not_a_capture_grant_or_body_completion():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    assert reviews['unregistered:www.pocket.tw'] == reviews['pocket']
    review = reviews['pocket']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert 'no historical PIT is inferred' in review['observation']
    assert 'not production durable acceptance' in review['observation']


def test_pinview_native_review_preserves_prior_clock_and_empty_bare_host_scope():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['pinview']
    assert reviews['unregistered:www.pinview.com.tw'] == review
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert review['previous_review']['capture_decision'] == 'review_incomplete'
    assert review['previous_review']['reviewed_at_utc'] == '2026-10-20T15:39:49+00:00'
    for phrase in ('219 literal', 'All Rights Reserved', 'no timezone', 'no verified alias',
                   'does not prove actual canonical downloading', 'No new discovery'):
        assert phrase in review['observation']
    bare = reviews['unregistered:pinview.com.tw']
    assert bare != review and bare['capture_decision'] == 'review_incomplete'
    for phrase in ('zero decoded bytes', 'no redirect', 'No alternate-host rewrite'):
        assert phrase in bare['observation']


def test_pinview_current_index_observation_is_not_body_or_alias_acceptance():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['pinview']
    assert review['reviewed_at_utc'] == '2026-10-20T19:40:27+00:00'
    index = review['current_index_review']
    assert index['observed_at_utc'] == '2026-10-20T20:32:52.264536+00:00'
    assert index['evidence_urls'] == ['https://www.pinview.com.tw/Tags/20345.html']
    for phrase in ('9531 decoded bytes', 'zero qualifying', 'narrower than',
                   'No19 old URL was requested again', 'all76 source identities',
                   'Original source review clocks', 'rather than repeat'):
        assert phrase in index['observation']


def test_peoplemedia_review_is_native_scope_not_all_history_or_editorial_person():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['peoplemedia']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert 'https://www.peoplemedia.tw/copyright' in review['evidence_urls']
    for phrase in ('nine already queued', 'not all local metadata', 'editorial-center labels',
                   'raw invalid strings', 'not prove production durable acceptance', 'not official or measured maxima'):
        assert phrase in review['observation']
    native = review['native_v2_repair_review']
    for phrase in ('metadata-only pass', 'outer ASCII spaces', 'other four heads are not requeued',
                   'all28 already accepted', 'exact v1 HTTP', 'not new production bodies'):
        assert phrase in native['observation']
    embedded = review['native_v3_embedded_review']
    for phrase in ('Four still-missing', 'all37 accepted heads', 'multimedia-not-complete',
                   'failed negative lookup', 'no obfuscated address is decoded', 'Only the four evidenced'):
        assert phrase in embedded['observation']


@pytest.mark.parametrize('provider,evidence,scope', [
    ('nextapple', 'https://news.nextapple.com/terms', 'automated bulk content grabbing'),
    ('ctinews', 'https://ctinews.com/terms', 'database form'),
    ('housefun', 'https://member.housefun.com.tw/Private/propertystatement.aspx', 'prior written consent'),
])
def test_primary_terms_reviews_preserve_disabled_collectors_and_known_work(provider, evidence, scope):
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[provider]
    assert review['capture_decision'] == 'permission_required'
    assert evidence in review['evidence_urls'] and scope in review['observation']
    assert 'legal fair-use verdict' in review['observation']
    assert 'No article' in review['observation']
    assert not next(source for source in before[1]['providers'] if source['id'] == provider)['enabled']
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_watchmedia_review_preserves_prior_clock_and_native_nonbody_boundaries():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    assert reviews['unregistered:www.watchmedia01.com'] == reviews['watchmedia']
    review = reviews['watchmedia']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert review['previous_review']['reviewed_at_utc'] == '2026-10-20T15:48:10+00:00'
    assert review['previous_review']['capture_decision'] == 'private_public_candidate'
    for phrase in ['all244', 'partner警政時報', 'no visible native author', 'not production durable acceptance',
                   'not an official or measured ceiling']:
        assert phrase in review['observation']


def test_gugu_path_inventory_is_not_story_or_content_classification():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['unregistered:school.gugu.fund']
    assert 'https://school.gugu.fund/policy_sc' in review['evidence_urls']
    assert 'Path families are discovery hints' in review['observation']
    assert 'not automatically a ban on every public news article' in review['observation']
    assert 'does not enable a collector' in review['observation']


@pytest.mark.parametrize('provider,known_heads,evidence,scope_boundary', [
    ('unregistered:www.4gtv.tv', 183,
     'https://www.4gtv.tv/explanation-legal-agreement.html',
     'including editorial content'),
    ('unregistered:www.hwa-guan.com.tw', 165,
     'https://www.hwa-guan.com.tw/Finance/Home/ServicePolicy',
     'one-third limit applies to online publication/forwarding'),
    ('unregistered:geneonline.news', 136,
     'https://geneonline.news/terms-conditions/',
     'not the separately governed international affiliate'),
])
def test_new_policy_reviews_preserve_known_work_without_collectors_or_false_absence(
        provider, known_heads, evidence, scope_boundary):
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[provider]
    assert review['capture_decision'] == 'permission_required'
    assert evidence in review['evidence_urls']
    assert f'Preserve{known_heads} frozen inventoried literal URL heads' in review['observation']
    assert scope_boundary in review['observation']
    assert 'not a legal fair-use verdict' in review['observation']
    assert 'permanent irrecoverability' in review['observation']
    assert review['reviewed_at_utc'] == '2026-10-20T21:35:02+00:00'
    host = provider.removeprefix('unregistered:')
    assert not any(host in source['domains'] for source in before[1]['providers'])
    assert before == (configuration_revision(path), load_news_configuration(path))
    assert before[1]['remote_training_export'] is False


def test_hwa_guan_policy_http_evidence_does_not_become_article_permission_or_not_found():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[
        'unregistered:www.hwa-guan.com.tw']
    assert 'returned200,87406 decoded bytes' in review['observation']
    assert 'robots request returned404' in review['observation']
    assert 'neither article absence nor a content license' in review['observation']
    assert 'not an inferred private-download limit' in review['observation']
    assert 'Article requests0, production queue writes0, source activationfalse' in review['observation']
    assert 'https://www.hwa-guan.com.tw/Finance/Home/DecCopyright' in review['evidence_urls']


@pytest.mark.parametrize('provider,decision,evidence,scope_boundary', [
    ('unregistered:more-news.tw', 'review_incomplete', 'https://more-news.tw/robots.txt',
     'article HTTP0, production queue writes0, private article raw0'),
    ('unregistered:news.ustv.com.tw', 'permission_required', 'https://www.ustv.com.tw/service',
     'expressly include the USTV news site'),
    ('unregistered:inews.hket.com', 'review_incomplete', 'https://inews.hket.com/',
     'not a canonical NewsHTTP article or REP observation'),
    ('unregistered:www.epochtimes.com', 'review_incomplete',
     'https://www.epochtimes.com/b5/UserAgreement.htm',
     'incomplete live-document verification'),
])
def test_unfinished_policy_or_access_review_never_accepts_articles_or_erases_evidence(
        provider, decision, evidence, scope_boundary):
    path = Path('configs/news_fulltext.json')
    before = configuration_revision(path), load_news_configuration(path)
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[provider]
    assert review['capture_decision'] == decision
    assert evidence in review['evidence_urls']
    assert scope_boundary in review['observation']
    assert 'CNYES priority and CNA deferral remain unchanged' in review['observation']
    host = provider.removeprefix('unregistered:')
    assert not any(host in source['domains'] for source in before[1]['providers'])
    assert before == (configuration_revision(path), load_news_configuration(path))


def test_more_news_real_robots_refusal_is_not_a_reader_error_or_official_rate():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[
        'unregistered:more-news.tw']
    assert 'returned403 at2026-10-20T21:37:53.913794+00:00' in review['observation']
    assert 'denial body was not read or retained' in review['observation']
    assert '900-second local cooldown' in review['observation']
    assert 'not an official quota or measured rate' in review['observation']
    assert 'complete-row before/after SHA agrees' in review['observation']
    assert 'do not reset or repeatedly probe the refusal' in review['observation']


@pytest.mark.parametrize("field,bad", [
    ("contract_version", True), ("contract_version", 2), ("providers", []),
    ("providers", {"bad\nname": {}}), ("providers", {"moneydj": {"capture_decision": []}}),
    ("provider_aliases", []), ("provider_aliases", {"x": "missing"}),
])
def test_bad_review_ledger_is_rejected_without_raw_error_text(tmp_path, field, bad):
    path = tmp_path / "reviews.json"
    atomic_write_json(path, {**_ledger(), field: bad})
    with pytest.raises(ValueError, match="invalid source review"):
        load_source_review_ledger(path)


@pytest.mark.parametrize("relative", ["/absolute.json", "../../outside.json", ".env", 1, ""])
def test_review_path_cannot_escape_catalog_or_read_env(tmp_path, relative):
    config_path, _ = _configure(tmp_path)
    catalog = tmp_path / "configs/news/catalog.json"
    value = json.loads(catalog.read_text())
    atomic_write_json(catalog, {**value, "planning_review_ledger": relative})
    with pytest.raises(ValueError):
        planning_review_path(config_path)


def test_review_symlink_escape_is_rejected(tmp_path):
    config_path, path = _configure(tmp_path)
    outside = tmp_path / "outside.json"
    atomic_write_json(outside, _ledger())
    link = path.parent / "symlink.json"
    link.symlink_to(outside)
    catalog = tmp_path / "configs/news/catalog.json"
    value = json.loads(catalog.read_text())
    atomic_write_json(catalog, {**value, "planning_review_ledger": "symlink.json"})
    with pytest.raises(ValueError, match="catalog-local"):
        planning_review_path(config_path)


def test_malformed_catalog_does_not_create_attribute_error(tmp_path):
    config_path, _ = _configure(tmp_path)
    atomic_write_json(tmp_path / "configs/news/catalog.json", [])
    with pytest.raises(ValueError, match="invalid news source catalog"):
        planning_review_path(config_path)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["source_reviews_state"] == "unavailable"


def test_oversized_review_is_not_read_or_projected(tmp_path):
    config_path, path = _configure(tmp_path)
    atomic_write_json(path, {**_ledger(), "large_private_note": "x" * (2 << 20)})
    with pytest.raises(ValueError, match="oversized"):
        load_source_review_ledger(path)
    with pytest.raises(ValueError, match="bounded"):
        planning_review_path(config_path)
    assert build_news_public_status(tmp_path, now=NOW)["source_reviews_state"] == "unavailable"


def test_review_edit_refreshes_only_dashboard_not_owner_or_capture_configuration(tmp_path):
    config_path, path = _configure(tmp_path)
    before = configuration_revision(config_path), news_snapshot_revision(tmp_path), load_news_configuration(config_path)
    result = build_news_public_status(tmp_path, now=NOW)
    rows = {s["id"]: s for s in result["providers"]}
    assert rows["unregistered:example.org"]["source_policy_review"] == "permission_required"
    assert rows["unregistered:example.org"]["source_reviewed_at_utc"] == NOW.isoformat()
    assert not rows["unregistered:example.org"]["enabled"]
    assert rows["unregistered:example.org"]["conditional_eta_seconds"] is None
    assert rows["cnyes"]["source_policy_review"] == "active_existing_policy"
    assert rows["cnyes"]["enabled"]
    assert result["source_policy_review_counts"] == {"permission_required": 1, "active_existing_policy": 1}
    assert sum(result["source_policy_review_counts"].values()) == len(result["providers"])
    assert not result["scope"]["source_reviews_grant_capture_permission"]
    assert "PRIVATE_REVIEW" not in json.dumps(result)
    assert "https://example.org/terms" not in json.dumps(result)
    value = _ledger()
    value["providers"]["moneydj"]["capture_decision"] = "review_incomplete"
    atomic_write_json(path, value)
    assert before[0] == configuration_revision(config_path)
    assert before[1] != news_snapshot_revision(tmp_path)
    assert before[2] == load_news_configuration(config_path)
    rows = {s["id"]: s for s in build_news_public_status(tmp_path, now=NOW)["providers"]}
    assert rows["unregistered:example.org"]["source_policy_review"] == "review_incomplete"
    assert not rows["unregistered:example.org"]["enabled"]


def test_review_does_not_revoke_an_active_runtime_license(tmp_path):
    value = _ledger()
    value["providers"]["cnyes"] = deepcopy(value["providers"]["moneydj"])
    _configure(tmp_path, ledger=value)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(s for s in result["providers"] if s["id"] == "cnyes")
    assert source["enabled"] and source["source_policy_review"] == "permission_required"
    # Pending work without an inflight request is backfilling, not running.
    assert source["state"] == "backfilling" and source["known_urls"] == 6


@pytest.mark.parametrize("clock", [True, "not-a-clock", "2026-10-08T15:00:00", (NOW + timedelta(seconds=6)).isoformat()])
def test_review_with_unverified_clock_is_explicit_not_falsely_reviewed(tmp_path, clock):
    value = _ledger()
    value["providers"]["moneydj"]["reviewed_at_utc"] = clock
    _configure(tmp_path, ledger=value)
    result = build_news_public_status(tmp_path, now=NOW)
    row = next(s for s in result["providers"] if s["id"] == "unregistered:example.org")
    assert row["source_policy_review"] == "review_unverified"
    assert row["source_reviewed_at_utc"] is None and not row["enabled"]


def test_bad_review_file_preserves_successful_capture_counts_and_state(tmp_path):
    _, path = _configure(tmp_path)
    atomic_write_json(path, {"contract_version": True})
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["health"] == "running" and result["snapshot_consistent"]
    assert result["acquisition"]["stored_body_articles"] == 2
    assert result["source_reviews_state"] == "unavailable"
    assert result["source_policy_review_counts"] == {"active_existing_policy": 1, "not_reviewed": 1}


def test_missing_review_configuration_is_explicit_and_does_not_call_publishers(tmp_path):
    _seed(tmp_path)
    assert planning_review_path(tmp_path / "configs/news_fulltext.json") is None
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["source_reviews_state"] == "not_configured"
    assert not result["scope"]["publisher_calls_on_page_view"]


def test_wantmedia_primary_review_is_permission_work_not_an_implicit_enable():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['wantmedia']
    assert review['capture_decision'] == 'permission_required'
    assert review['evidence_urls'] == ['https://member.want-media.com/member-term-of-service.htm']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'wantmedia')
    assert source['enabled'] is False
    assert source['authorization']['kind'] == 'awaiting_authorization'


@pytest.mark.parametrize('provider,decision,evidence', [
    ('vocus', 'permission_required', 'https://vocus.cc/terms/member'),
    ('mirrormedia', 'permission_required', 'https://www.mirrormedia.mg/story/service-rule'),
    ('storm', 'permission_required', 'https://www.storm.mg/terms'),
    ('setn', 'review_incomplete', 'https://www.setn.com/'),
])
def test_creator_and_publisher_policy_reviews_do_not_enable_downloads(provider, decision, evidence):
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews[provider]
    assert review['capture_decision'] == decision
    assert evidence in review['evidence_urls']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == provider)
    assert source['enabled'] is False
    assert source['authorization']['kind'] == 'awaiting_authorization'
    assert not any(source.get(key) for key in (
        'cnyes_apis', 'wordpress_apis', 'fulltext_feeds', 'feeds', 'sitemaps', 'index_pages'))
    assert config['remote_training_export'] is False


def test_cmoney_official_metadata_review_never_registers_body_API_or_credentials():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['cmoney']
    assert review['capture_decision'] == 'review_incomplete'
    assert '30 distinct operations' in review['observation']
    prefix = 'https://www.cmoney.tw/cm-service/CMService.asmx?op='
    operations = [url.removeprefix(prefix) for url in review['evidence_urls'] if url.startswith(prefix)]
    assert len(operations) == 5 and set(operations) == {
        'GetStockRssNews', 'GetMoreStockRssNews', 'GetDailyHeadlineArticles',
        'GetStockRssArticles', 'GetStockRssArticlesWithFilterType'}
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'cmoney')
    assert not any(source.get(key) for key in ('wordpress_apis', 'cnyes_apis', 'fulltext_feeds'))


def test_cmoney_owl_RSS_contract_is_metadata_not_forum403_or_original_text_entitlement():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['cmoney']
    assert any(url.endswith('API%E8%A6%8F%E6%A0%BC.pdf') for url in review['evidence_urls'])
    for declaration in ('Microsoft-IIS/8.5', 'kong/3.9.0', '/OwlApi/auth',
                        '/OwlApi/api/rss/{product}/{count}',
                        '/OwlApi/api/rss/{product}/{stock}/{count}',
                        'creation-time', 'not full text', 'No Owl auth/product request sent'):
        assert declaration in review['observation']
    source = next(row for row in load_news_configuration(Path('configs/news_fulltext.json'))['providers']
                  if row['id'] == 'cmoney')
    assert not any(source.get(key) for key in ('wordpress_apis', 'cnyes_apis', 'fulltext_feeds'))


def test_udn_licensed_export_and_archive_clock_review_does_not_grant_capture_or_backdate():
    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['udn']
    assert review['capture_decision'] == 'permission_required'
    assert 'https://udndata.com/faq/marketing.html' in review['evidence_urls']
    assert 'https://www.udndata.com/faq/fulltext.html' in review['evidence_urls']
    for declaration in ('original database licensed export', 'data-analysis application',
                        'not each article\'s exact original publication', 'Before1999-09-01',
                        'not calls per hour', 'licensed-export schema', 'candidate adapter disabled'):
        assert declaration in review['observation']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'udn')
    assert source['enabled'] is False and source['authorization']['kind'] == 'awaiting_authorization'
    assert config['remote_training_export'] is False


def test_global_voices_review_preserves_missing_byline_gate_and_original_pacing():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    assert reviews['globalvoices']['capture_decision'] == 'reviewed_existing_scope'
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'globalvoices')
    assert source['enabled'] and source['interval_seconds'] == 10
    assert source['require_author_attribution'] and not source['wordpress_apis']
    assert len(source['fulltext_feeds']) == 2


def test_global_voices_actual_embed_refusal_is_not_guessed_byline_or_API_activation():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    latest = reviews['globalvoices']
    # This is the dated API/network witness, not a requirement to overwrite
    # the latest offline article diagnosis with an old observation.
    review, = [r for r in [latest, *latest.get('prior_reviews', [])]
               if r['reviewed_at_utc'] == '2026-10-13T07:10:13+00:00']
    assert 'rest_cannot_access' in review['observation']
    assert 'All100 author embeds' in review['observation']
    assert 'zero embedded names' in review['observation']
    assert 'no raw mixed-response retention' in review['observation']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'globalvoices')
    assert source['wordpress_apis'] == [] and source['require_author_attribution'] is True


def test_global_voices_editorial_AI_and_native_network_evidence_have_distinct_scopes():
    latest = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))['globalvoices']
    review, = [r for r in [latest, *latest.get('prior_reviews', [])]
               if r['reviewed_at_utc'] == '2026-10-13T07:10:13+00:00']
    assert 'https://globalvoices.org/about/global-voices-policy-on-ai/' in review['evidence_urls']
    assert 'EHOSTUNREACH' in review['observation']
    assert 'not a new capture grant' in review['observation']
    assert 'network recovery is inferred' in review['observation']
    assert 'strictly post-failure complete captures' in review['observation']
    assert 'WP embedded-author permission remains refused' in review['observation']
    assert 'not all-history or all-provider recovery' in review['observation']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'globalvoices')
    assert source['enabled'] is True and source['authorization']['kind'] == 'open_license'
    assert config['remote_training_export'] is False


def test_ltn_official_rss_permission_does_not_enable_html_or_mislabel_abstract_as_body():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['ltn']
    assert review['capture_decision'] == 'permission_required'
    assert review['evidence_urls'] == [
        'https://service.ltn.com.tw/copyright', 'https://service.ltn.com.tw/RSS',
        'https://news.ltn.com.tw/rss/business.xml', 'https://news.ltn.com.tw/rss/all.xml']
    assert 'RSS metadata/abstracts, not full articles' in review['observation']
    assert 'not blanket HTML/full-history/training permission' in review['observation']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'ltn')
    assert source['enabled'] is False
    assert source['authorization']['kind'] == 'awaiting_authorization'
    assert not any(source.get(key) for key in ('fulltext_feeds', 'wordpress_apis', 'cnyes_apis'))


def test_money_link_public_footer_review_keeps_news_and_financial_products_separate():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['money_link']
    assert review['capture_decision'] == 'permission_required'
    assert 'https://tweb.money-link.com.tw/policy/declaration.aspx' in review['evidence_urls']
    assert 'https://ww2.money-link.com.tw/MemberCenter/MemberTerms.aspx?term=4' in review['evidence_urls']
    assert 'outside any login' in review['observation']
    assert 'editorial-content rights' in review['observation']
    assert 'not automatically treat that financial-data product paragraph as the entire news contract' in review['observation']
    assert 'does not prove that no licensed feed exists' in review['observation']
    assert 'planning review grants no source permission' in review['observation']
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    source = next(row for row in config['providers'] if row['id'] == 'money_link')
    assert source['enabled'] is False and source['authorization']['kind'] == 'awaiting_authorization'


@pytest.mark.parametrize('provider,evidence', [
    ('unregistered:hd.stheadline.com', 'https://www.stheadline.com/terms/'),
    ('unregistered:sunmedia.tw', 'https://sunmedia.tw/copyright'),
    ('unregistered:www.peoplenews.tw', 'https://www.peoplenews.tw/%E7%89%88%E6%AC%8A%E8%81%B2%E6%98%8E'),
])
def test_new_restrictive_reviews_preserve_known_urls_without_granting_capture(provider, evidence):
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews[provider]
    assert review['capture_decision'] == 'permission_required'
    assert evidence in review['evidence_urls']
    assert review['reviewed_at_utc'].startswith('2026-10-14T')
    config = load_news_configuration(Path('configs/news_fulltext.json'))
    assert not any(source['id'] == provider and source.get('enabled') for source in config['providers'])
    assert config['remote_training_export'] is False
