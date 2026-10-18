"""Issuer lookup may avoid redundant discovery, never invent media coverage."""
from datetime import datetime
import hashlib
import json
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts import audit_news_issuer_overlap as audit


def head(number=101, *, title="測試公司(1234) 公告董事會決議", day="2013-03-28", **extra):
    return {"url": f"https://finance.ettoday.net/news/{number}", "title": title,
            "metadata_order_day": day, "state": "article_attribution_not_verified",
            "error_code": "article_attribution_not_verified", "last_checked_at_utc": "2026-10-18T00:00:00+00:00",
            "provider": "ettoday", "body_sha256": None, "parquet_path": None, **extra}


def inputs(tmp_path, heads=None, records=None):
    news, finlab = tmp_path / "news", tmp_path / "finlab"
    news.mkdir(); (finlab / "datasets").mkdir(parents=True); (finlab / "receipts").mkdir()
    database = news / "queue.sqlite3"
    with sqlite3.connect(database) as db:
        db.execute("CREATE TABLE articles(url TEXT PRIMARY KEY,title TEXT,metadata_order_day TEXT,state TEXT,"
                   "error_code TEXT,last_checked_at_utc TEXT,provider TEXT,body_sha256 TEXT,parquet_path TEXT)")
        for row in heads if heads is not None else [head()]:
            columns = list(row)
            db.execute("INSERT INTO articles(" + ",".join(columns) + ") VALUES (" + ",".join("?" * len(columns)) + ")",
                       [row[key] for key in columns])
    records = records if records is not None else [{"source_index": "17", "symbol": "1234",
        "date": datetime(2013, 3, 28, 18, 3, 23), "title": "公告董事會決議", "info": "測試公告說明，並非新聞正文。"}]
    table = pa.Table.from_pylist(records, schema=pa.schema([
        ("source_index", pa.string()), ("symbol", pa.string()), ("date", pa.timestamp("ns")),
        ("title", pa.string()), ("info", pa.string())]))
    part = finlab / "datasets/disclosure.parquet"; pq.write_table(table, part, row_group_size=1)
    receipt = {"dataset": audit.DATASET, "status": "downloaded_unverified_for_pit", "rows": table.num_rows,
               "rows_with_values": table.num_rows, "parquet_path": "datasets/disclosure.parquet",
               "parquet_size_bytes": part.stat().st_size, "sha256": sha256_file(part),
               "publication_time_status": "not_verified", "index_semantics": "provider_index_unverified",
               "redistribution": "local_only_not_published"}
    receipt_path = finlab / "receipts" / (audit.safe_stem(audit.DATASET) + ".json")
    atomic_write_json(receipt_path, receipt)
    return news, finlab, database, part, receipt_path


@pytest.mark.parametrize("title", ["測試公司(1234) 公告董事會決議", "測試公司（1234） 公告董事會決議"])
def test_literal_key_and_no_name_fuzzy_match(title):
    assert audit.issuer_metadata_key(head(title=title)) == ("1234", "2013-03-28", "公告董事會決議")


@pytest.mark.parametrize("extra", [
    {"title": "測試公司(1234)公告董事會決議"}, {"title": "測試公司(1234） 公告董事會決議"},
    {"title": "測試公司(ABCD) 公告董事會決議"}, {"title": "記者(12) 公告董事會決議"},
    {"title": "測試公司(1234) 公告\n董事會決議"}, {"day": "2013-02-30"}, {"day": "20130328"},
    {"day": None}, {"url": "https://www.ettoday.net/news/not-an-actual-native-path"},
    {"url": "https://finance.ettoday.net/news/101?other=1234"}, {"url": "https://other.test/news/101"},
])
def test_invalid_keys_never_guessed(extra):
    assert audit.issuer_metadata_key(head(**extra)) is None


@pytest.mark.parametrize("suffix", audit._METADATA_BRAND_SUFFIXES)
def test_only_closed_literal_brand_suffixes_removed(suffix):
    row = head(title="測試公司(1234) 公告董事會決議" + suffix)
    assert audit.issuer_metadata_key(row) == ("1234", "2013-03-28", "公告董事會決議")
    assert audit.metadata_brand_suffix(row["title"]) == suffix
    assert row["title"].endswith(suffix)  # Input never rewritten.


def test_literal_www_is_not_an_alias_rewrite_or_a_release_clock():
    row = head(url="http://www.ettoday.net/news/20130329/101.htm", title="測試公司(1234) 公告董事會決議 - ETtoday新聞雲")
    assert audit.issuer_metadata_key(row) == ("1234", "2013-03-28", "公告董事會決議")
    assert row["url"] == "http://www.ettoday.net/news/20130329/101.htm"


def test_unknown_brand_and_punctuation_stay_literal():
    row = head(title="測試公司(1234) 公告董事會決議。 - etTODAY")
    assert audit.issuer_metadata_key(row)[2] == "公告董事會決議。 - etTODAY"


def test_zero_download_readonly_exact_overlap(tmp_path):
    news, finlab, db, part, receipt = inputs(tmp_path, heads=[
        head(), head(102, title="另個名稱(1234) 公告董事會決議"),
        head(103, title="測試公司(1234) 公告董事會決議。"),
        head(104, day="2013-03-29"), head(105, title="沒有股票代碼的新聞"),
        head(106, body_sha256="held", parquet_path="held.parquet"),
        head(107, body_sha256="partial"), head(108, parquet_path="partial.parquet"),
        head(109, state="complete"), head(110, provider="other"),
    ])
    before = {p: sha256_file(p) for p in (db, part, receipt)}
    result = audit.audit(news, finlab, batch_rows=1)
    assert result["counts"] == {"missing_attribution_heads": 5, "issuer_title_candidates": 4,
        "other_missing_attribution_heads": 1, "distinct_candidate_keys": 3,
        "match_states": {"single_local_record_nonempty_info": 2, "no_local_match": 2},
        "candidate_match_states_by_metadata_year": {"2013": {"single_local_record_nonempty_info": 2, "no_local_match": 2}}}
    assert {p: sha256_file(p) for p in before} == before
    row = result["rows"][0]; source = row["local_record_candidates"][0]
    assert source["source_index"] == "17"
    assert source["provider_event_time_literal"] == "2013-03-28T18:03:23"
    assert source["provider_event_timezone"] is None  # Never invent +08 or a publication clock.
    assert source["info_sha256"] == hashlib.sha256("測試公告說明，並非新聞正文。".encode()).hexdigest()
    assert "測試公告說明，並非新聞正文。" not in json.dumps(result, ensure_ascii=False)
    assert result["finlab_source_receipt"]["redistribution"] == "local_only_not_published"
    assert result["publisher_requests"] == result["queue_writes"] == 0
    for key in ("body_equivalence_verified", "publisher_identity_or_rights_granted", "all_history_complete",
                "publication_clock_verified_by_match", "news_training_injection", "source_body_text_exported"):
        assert result[key] is False
    assert result["finlab_receipt_sha256"] == sha256_file(receipt)


def test_duplicate_and_empty_records_never_promoted(tmp_path):
    base = {"source_index": "0", "symbol": "1234", "date": datetime(2013, 3, 28, 9),
            "title": "公告董事會決議", "info": "甲"}
    records = [base, {**base, "source_index": "1", "info": "乙"},
               {**base, "source_index": "2", "title": "空值", "info": None},
               {**base, "source_index": "3", "title": "空白", "info": "  \n"}]
    news, finlab, *_ = inputs(tmp_path, heads=[head(), head(102, title="公司(1234) 空值"),
                                           head(103, title="公司(1234) 空白")], records=records)
    r = audit.audit(news, finlab, batch_rows=1)
    assert r["counts"]["match_states"] == {"ambiguous_local_records": 1, "single_local_record_empty_info": 2}
    assert len(r["rows"][0]["local_record_candidates"]) == 2


@pytest.mark.parametrize("change", [
    {"dataset": "other"}, {"status": "metadata_only"}, {"rows_with_values": True},
    {"rows_with_values": 0}, {"sha256": "0" * 64}, {"rows": 99},
    {"parquet_path": "../escape.parquet"}, {"parquet_path": "/tmp/escape.parquet"},
])
def test_wrong_or_damaged_source_rejected(tmp_path, change):
    news, finlab, _, _, receipt_path = inputs(tmp_path)
    receipt = json.loads(receipt_path.read_bytes()); receipt.update(change)
    atomic_write_json(receipt_path, receipt)
    with pytest.raises(ValueError, match="finlab"):
        audit.audit(news, finlab)


def test_source_mutation_during_scan_rejected(tmp_path, monkeypatch):
    news, finlab, _, _, receipt = inputs(tmp_path)
    original = audit.match_local_records
    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        value = json.loads(receipt.read_bytes()); value["extra"] = "changed"
        atomic_write_json(receipt, value)
        return result
    monkeypatch.setattr(audit, "match_local_records", changed)
    with pytest.raises(ValueError, match="changed_during_audit"):
        audit.audit(news, finlab)


@pytest.mark.parametrize("kind", ["receipt", "parquet"])
def test_source_symlinks_rejected(tmp_path, kind):
    news, finlab, _, part, receipt = inputs(tmp_path)
    target = receipt if kind == "receipt" else part
    donor = tmp_path / ("donor-" + target.name)
    target.rename(donor); target.symlink_to(donor)
    with pytest.raises(ValueError, match="unsafe"):
        audit.audit(news, finlab)


def test_unknown_policy_not_inferred_from_overlap(tmp_path):
    news, finlab, _, _, receipt = inputs(tmp_path)
    value = json.loads(receipt.read_bytes()); value.pop("redistribution"); value.pop("publication_time_status")
    atomic_write_json(receipt, value)
    result = audit.audit(news, finlab)
    assert result["finlab_source_receipt"]["redistribution"] is None
    assert result["finlab_source_receipt"]["publication_time_status"] is None
    assert result["publication_clock_verified_by_match"] is result["publisher_identity_or_rights_granted"] is False


@pytest.mark.parametrize("batch_rows", [0, -1, 65537, True])
def test_invalid_batch_size_rejected(tmp_path, batch_rows):
    _, _, _, part, _ = inputs(tmp_path)
    with pytest.raises(ValueError, match="batch_rows"):
        audit.match_local_records(part, set(), batch_rows=batch_rows)


def test_cli_private_new_evidence_only(tmp_path, capsys):
    news, finlab, database, _, _ = inputs(tmp_path)
    output = tmp_path / "evidence.json"
    args = ["--news-root", str(news), "--finlab-root", str(finlab), "--output", str(output)]
    assert audit.main(args) == 0
    assert output.stat().st_mode & 0o777 == 0o600
    assert json.loads(capsys.readouterr().out)["accepted"]
    with pytest.raises(SystemExit):
        audit.main(args)  # Preserve prior accepted evidence.
    for target in (database, news / "new.json", finlab / "new.json"):
        with pytest.raises(SystemExit):
            audit.main(["--news-root", str(news), "--finlab-root", str(finlab), "--output", str(target)])
