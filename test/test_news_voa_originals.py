"""Source-backed layout/clock cases, synthetic text rather than an archive copy."""
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from scripts.probe_news_sources import probe_provider


CHINESE = "https://www.voachinese.com/a/china-standards-2035-20200428/5395187.html"
CANTONESE = "https://www.voacantonese.com/a/TC-former-us-official-calls-for-including-taiwan-in-a-new-pandemic-response-initiative-with-asia-20200403-ry/5358735.html"
ORIGINAL = "https://www.voachinese.com/a/former-us-official-calls-for-including-taiwan-in-a-new-pandemic-response-initiative-with-asia-20200402/5357662.html"


def page(provider="voa_chinese", **changes):
    cantonese = provider == "voa_cantonese"
    values = {"url": CANTONESE if cantonese else CHINESE,
              "canonical": ORIGINAL if cantonese else CHINESE,
              "author": "鍾辰芳" if cantonese else "林枫", "schema_author": None,
              "visible": "2020年4月3日" if cantonese else "2020年4月28日 12:18",
              "machine": "2020-04-03T21:32:04+08:00" if cantonese else "2020-04-28T12:18:33+08:00",
              "published": "2020-04-03 13:32:04Z" if cantonese else "2020-04-28 04:18:33Z",
              "modified": "2020-04-03 13:33:04Z" if cantonese else "2020-04-28 10:41:33Z",
              "og_url": None, "main_entity": None, "free": True, "extra": "", "body": "正常原始資料 -12.50 1000股 5% " * 16}
    values.update(changes)
    schema = {"@type": "NewsArticle", "headline": "測試原創", "url": values["url"],
              "mainEntityOfPage": values["main_entity"] or values["url"],
              "author": {"@type": "Person", "name": values["schema_author"] or values["author"]},
              "datePublished": values["published"], "dateModified": values["modified"],
              "isAccessibleForFree": values["free"]}
    return (f'''<html><head><link rel="canonical" href="{values['canonical']}">
      <meta property="og:url" content="{values['og_url'] or values['url']}">
      <meta property="og:title" content="測試原創">
      <script type="application/ld+json">{json.dumps(schema, ensure_ascii=False)}</script></head><body>
      <nav>中國時間 2026年10月11日</nav><main class="container">
      <h1 class="title pg-title">測試原創</h1><div class="publishing-details">
      <div class="published"><span class="date"><time datetime="{values['machine']}">{values['visible']}</time></span></div>
      <div class="links"><ul class="links__list"><li class="links__item">{values['author']}</li></ul></div></div>
      <div id="article-content"><div class="wsw"><span class="dateline">華盛頓 —</span>
      <p>{values['body']}</p><div class="wsw__embed"><p>第三方圖片與播放器控制</p></div></div></div>
      <div class="wsw c-author__wsw">不得混入的作者簡介</div>{values['extra']}</main></body></html>''').encode()


def extract(provider="voa_chinese", **changes):
    _, registry, _ = load_config(Path("configs/news_fulltext.json"))
    return extract_article(page(provider, **changes), registry.sources[provider],
                           page_url=CANTONESE if provider == "voa_cantonese" else CHINESE)


@pytest.mark.parametrize("provider", ["voa_chinese", "voa_cantonese"])
def test_exact_original_text_and_declared_clock(provider):
    result = extract(provider)
    assert "-12.50 1000股 5%" in result["body"]
    assert not any(text in result["body"] for text in ["作者簡介", "播放器", "中國時間"])
    assert result["body_completeness"] == (
        "verified_source_text_container; media_embeds_not_captured" if provider == "voa_cantonese"
        else "verified_original_text_container; media_embeds_not_captured")
    detail = json.loads(result["author_attribution_detail_json"])
    assert not detail["canonical_followed"]
    assert result["published_at_raw"] == detail["source_schema_publication_raw"]
    if provider == "voa_cantonese":
        assert detail["source_visible_publication_text"] == "2020年4月3日"
        assert detail["source_time_datetime_raw"] == "2020-04-03T21:32:04+08:00"
        assert result["declared_canonical_url"] == ORIGINAL


@pytest.mark.parametrize("changes,error", [
    ({"canonical": "https://other.example/a"}, "article_identity_not_verified"),
    ({"og_url": CHINESE.replace("5395187", "5395188")}, "article_identity_not_verified"),
    ({"main_entity": CHINESE.replace("5395187", "5395188")}, "article_identity_not_verified"),
    ({"author": "美聯社"}, "article_rights_or_scope_excluded"),
    ({"schema_author": "其他作者"}, "article_attribution_not_verified"),
    ({"visible": "2020年4月29日 12:18"}, "invalid_bulk_publication_clock"),
    ({"visible": "2020年4月28日 12:19"}, "invalid_bulk_publication_clock"),
    ({"machine": "2020-04-28T12:18:34+08:00"}, "invalid_bulk_publication_clock"),
    ({"published": "2020-04-28 04:18:33"}, "invalid_bulk_publication_clock"),
    ({"modified": "2020-04-27 10:41:33Z"}, "invalid_bulk_publication_clock"),
    ({"extra": '<div id="article-content"><div class="wsw"><p>另一文章</p></div></div>'}, "body_not_found"),
])
def test_conflicting_or_unverified_source_is_not_accepted(changes, error):
    with pytest.raises(NewsError) as failure:
        extract(**changes)
    assert failure.value.code == error


def test_only_actual_reviewed_translation_relation_is_admitted():
    with pytest.raises(NewsError) as failure:
        extract("voa_cantonese", canonical=ORIGINAL.replace("5357662", "5357663"))
    assert failure.value.code == "article_identity_not_verified"


@pytest.mark.parametrize("field,value", [
    ("authorization", {"kind": "personal_research"}), ("permitted_author_names", []),
    ("acquisition_mode", "continuous"), ("article_selectors", [".wsw"]),
    ("author_selectors", ["li"]), ("exclude_selectors", []),
])
def test_native_layout_requires_reviewed_bounded_original_scope(field, value):
    config, _, _ = load_config(Path("configs/news_fulltext.json"))
    config = deepcopy(config)
    entry = next(p for p in config["providers"] if p["id"] == "voa_chinese")
    entry[field] = value
    with pytest.raises(ValueError, match="unreviewed embedded article parser"):
        PolicyRegistry(config, base=Path.cwd())


def test_voa_addition_does_not_change_cnyes_capture_fingerprint():
    config, registry, repo = load_config(Path("configs/news_fulltext.json"))
    without_voa = deepcopy(config)
    without_voa["providers"] = [p for p in without_voa["providers"] if not p["id"].startswith("voa_")]
    assert registry.sources["cnyes"].data_fingerprint == PolicyRegistry(without_voa, base=repo).sources["cnyes"].data_fingerprint


@pytest.mark.parametrize("canonical", [CHINESE, "https://other.example/a"])
def test_probe_reports_declared_credits_without_retaining_excluded_body(tmp_path, capsys, canonical):
    _, registry, _ = load_config(Path("configs/news_fulltext.json"))
    raw = page(author="美聯社", canonical=canonical)
    http = SimpleNamespace(registry=registry, provider_not_before={}, robot_delays={},
                           get=lambda url: Response(url, 200, {"content-type": "text/html"}, raw))
    result = probe_provider(http, [CHINESE], retain=True, output_root=tmp_path)[0]
    assert result["extraction_error"] == (
        "article_rights_or_scope_excluded" if canonical == CHINESE else "article_identity_not_verified")
    declared, = result["publisher_declared_articles"]
    assert declared["author"] == {"@type": "Person", "name": "美聯社"}
    assert declared["url"] == CHINESE
    assert declared["datePublished"] == "2020-04-28 04:18:33Z"
    assert "private_diagnostic_path" not in result
    assert not list(tmp_path.iterdir())
    assert "正常原始資料" not in capsys.readouterr().out


@pytest.mark.parametrize("author", ["鍾辰芳", "美國之音", "松仁"])
def test_reviewed_private_scope_keeps_declared_name_without_public_domain_inference(author):
    result = extract("voa_cantonese", author=author)
    assert json.loads(result["authors_json"]) == [author]
    detail = json.loads(result["author_attribution_detail_json"])
    assert detail["capture_scope"] == "personal_noncommercial_storage_only"
    assert not any(detail[key] for key in (
        "public_domain_status_verified", "downstream_reuse_cleared", "training_export_cleared"))
    assert not detail["canonical_followed"]


@pytest.mark.parametrize("field,value", [
    ("authorization", {"kind": "public_domain"}), ("permitted_author_names", []),
    ("acquisition_mode", "continuous"), ("article_selectors", [".wsw"]),
    ("author_selectors", ["li"]), ("exclude_selectors", []),
])
def test_private_voa_adapter_requires_exact_source_and_private_scope(field, value):
    config, _, repo = load_config(Path("configs/news_fulltext.json"))
    config = deepcopy(config)
    entry = next(p for p in config["providers"] if p["id"] == "voa_cantonese")
    entry[field] = value
    with pytest.raises(ValueError, match="unreviewed embedded article parser"):
        PolicyRegistry(config, base=repo)


@pytest.mark.parametrize("index,original_id", list(enumerate((6851803, 6942832, 6998231, 6868920, 6962936, 7019997))))
def test_only_individually_observed_supplement_canonical_pairs(index, original_id):
    config, registry, _ = load_config(Path("configs/news_fulltext.json"))
    entry = next(p for p in config["providers"] if p["id"] == "voa_cantonese")
    url = entry["authorization"]["reviewed_byline_evidence_urls"][index]
    canonical = url.rsplit("/", 1)[0].replace("www.voacantonese.com", "www.voachinese.com") + f"/{original_id}.html"
    source = registry.sources["voa_cantonese"]
    result = extract_article(page("voa_cantonese", url=url, canonical=canonical, author="美國之音"), source, page_url=url)
    assert result["declared_canonical_url"] == canonical
    assert "v2; reviewed_translation_canonical_not_followed" in result["article_identity_basis"]
    assert result["published_at_raw"] == "2020-04-03 13:32:04Z"  # Not a guessed clock from the URL.
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(page("voa_cantonese", url=url, canonical=canonical.replace(f"/{original_id}.", f"/{original_id + 1}.")), source, page_url=url)


@pytest.mark.parametrize("free,error", [(False, "paywall_or_login"), (None, "article_rights_not_verified"), (1, "article_rights_not_verified")])
def test_private_scope_does_not_assume_public_article_access(free, error):
    with pytest.raises(NewsError) as failure:
        extract("voa_cantonese", free=free)
    assert failure.value.code == error


@pytest.mark.parametrize("element", ['<div class="wsw">', '<li class="links__item">',
                                      '<h1 class="title pg-title">', '<span class="date">'])
def test_private_scope_rejects_hidden_body_or_credit(element):
    _, registry, _ = load_config(Path("configs/news_fulltext.json"))
    raw = page("voa_cantonese").replace(element.encode(), element.replace('>', ' hidden>').encode())
    with pytest.raises(NewsError, match="article_identity_not_verified"):
        extract_article(raw, registry.sources["voa_cantonese"], page_url=CANTONESE)


@pytest.mark.parametrize("changes", [{"schema_author": "其他作者"}, {"free": False}, {"free": None}])
def test_probe_does_not_retain_identity_failure_outside_verified_credit_access(tmp_path, changes):
    _, registry, _ = load_config(Path("configs/news_fulltext.json"))
    raw = page("voa_cantonese", canonical="https://other.example/a", **changes)
    http = SimpleNamespace(registry=registry, provider_not_before={}, robot_delays={},
                           get=lambda url: Response(url, 200, {"content-type": "text/html"}, raw))
    result = probe_provider(http, [CANTONESE], retain=True, output_root=tmp_path)[0]
    assert result["extraction_error"] == "article_identity_not_verified"
    assert result["reviewed_current_byline_scope"] is False
    assert "private_diagnostic_path" not in result
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("element", ['<div class="wsw">', '<li class="links__item">'])
def test_probe_does_not_retain_unverified_identity_with_hidden_body_or_credit(tmp_path, element):
    _, registry, _ = load_config(Path("configs/news_fulltext.json"))
    raw = page("voa_cantonese", canonical="https://other.example/a").replace(
        element.encode(), element.replace('>', ' hidden>').encode())
    http = SimpleNamespace(registry=registry, provider_not_before={}, robot_delays={},
                           get=lambda url: Response(url, 200, {"content-type": "text/html"}, raw))
    result = probe_provider(http, [CANTONESE], retain=True, output_root=tmp_path)[0]
    assert result["reviewed_current_byline_scope"] is False
    assert "private_diagnostic_path" not in result
    assert not list(tmp_path.iterdir())
