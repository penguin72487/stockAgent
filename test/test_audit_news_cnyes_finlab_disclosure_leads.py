"""Candidate leads cannot silently become verified news or filing records."""
from copy import deepcopy
from datetime import datetime
import json

import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts import audit_news_cnyes_finlab_disclosure_leads as audit
from test_audit_news_issuer_overlap import inputs


SUBJECT = "依證券交易法第43條之1公告"


def test_observed_self_before_article_is_a_candidate_only():
    original = "依證券交易法自第43條之1第1項規定辦理公告"
    assert audit._explicit_43_1(original)
    assert not audit._explicit_43_1("依證券交易法自第43條之10辦理公告")
    assert original == "依證券交易法自第43條之1第1項規定辦理公告"


def _candidate(number, *, symbol="1234", title=SUBJECT, day="2024-06-01", incremental=False):
    result = {"cnyes_news_id": number, "cnyes_url": f"https://news.cnyes.com/news/id/{number}",
              "issuer_symbol_candidate": symbol, "cnyes_subject_literal": title,
              "official_record_identity_verified": False, "official_release_clock_verified": False}
    if incremental:
        dt = datetime.fromisoformat(day + "T12:00:00+08:00")
        result["cnyes_published_epoch"] = int(dt.timestamp())
    return result


def _worklists(tmp_path):
    old = {"contract_version": 1, "candidate_days": 1, "candidate_heads": 1,
           "groups": [{"cnyes_publication_day": "2024-06-01", "candidates": [_candidate(1)]}],
           "source_requests": 0, "production_queue_writes": 0, "historical_gaps_retired": False}
    old_path = tmp_path / "old.json"; atomic_write_json(old_path, old)
    new = {"contract": "cnyes_43_1_incremental_candidates_v1", "candidate_days": 1, "candidate_heads": 1,
           "groups": [{"cnyes_publication_day": "2024-06-02", "candidates":
                       [_candidate(2, day="2024-06-02", incremental=True)]}],
           "input_sha256": ["a" * 64, "b" * 64, sha256_file(old_path)],
           "source_requests": 0, "production_queue_writes": 0, "historical_gaps_retired": False}
    new_path = tmp_path / "new.json"; atomic_write_json(new_path, new)
    return old, new, old_path, new_path


def _record(symbol, day, title, index):
    return {"source_index": str(index), "symbol": symbol, "date": datetime.fromisoformat(day + "T08:30:00"),
            "title": title, "info": "私人公告內容不應讀取或匯出"}


def test_all_history_leads_preserve_clock_ambiguity_and_do_not_read_body(tmp_path):
    old, new, old_path, new_path = _worklists(tmp_path)
    records = [_record("1234", "2024-06-01", "同日但無關公告", 0),
               _record("1234", "2024-06-03", "依證券交易法 第４３條之１ 公告", 1),
               _record("1234", "2010-01-01", SUBJECT, 2),
               _record("5678", "2024-06-02", SUBJECT, 3)]
    news, finlab, db, part, receipt = inputs(tmp_path, records=records)
    before = {path: sha256_file(path) for path in (old_path, new_path, db, part, receipt)}
    result = audit.audit(old_path, new_path, finlab, batch_rows=1)
    assert result["counts"] == {"candidate_heads": 2, "candidate_symbols": 1, "finlab_rows_scanned": 4,
        "finlab_same_symbol_rows": 3, "finlab_same_symbol_explicit_43_1_title_rows": 2,
        "candidate_heads_with_any_same_symbol_same_day_row": 1,
        "candidate_heads_with_explicit_43_1_lead_any_day": 2,
        "candidate_heads_with_explicit_43_1_lead_same_day": 0,
        "candidate_heads_with_explicit_43_1_lead_within_7_days": 2,
        "lead_pairs": 4, "literal_subject_equal_pairs": 2, "normalized_subject_equal_pairs": 4}
    assert {path: sha256_file(path) for path in before} == before
    near = result["lead_pairs"][0]
    assert near["finlab_day_minus_cnyes_day"] == 2 and near["finlab_row_ordinal"] == 1
    assert near["finlab_event_timezone"] is None and near["finlab_event_time_literal"] == "2024-06-03T08:30:00"
    assert not near["literal_subject_equal"] and near["nfkc_whitespace_subject_equal"]
    assert result["comparison_is_only_a_lead"] and not result["filing_identity_verified"]
    assert not result["publication_clock_verified"] and not result["article_body_equivalence_verified"]
    assert result["publisher_requests"] == result["production_queue_writes"] == 0
    assert not result["body_text_read"] and "私人公告內容" not in json.dumps(result, ensure_ascii=False)


def test_request_scoped_incremental_worklist_validates_selected_original(tmp_path):
    old, new, old_path, _ = _worklists(tmp_path)
    sha = 'b' * 64
    path = 'quarantine/' + sha + '.parquet'
    new['contract'] = 'cnyes_43_1_incremental_candidates_v2'
    row = new['groups'][0]['candidates'][0]
    row.update(selected_response_sha256=sha, selected_source_artifact_path=path,
        source_observations=[{'source_response_sha256': sha, 'source_artifact_path': path,
            'source_artifact_kind': 'canonical_quarantine', 'source_observed_at_utc': '2026-10-01T00:00:00+00:00'}])
    assert len(audit.candidates(old, new, sha256_file(old_path))) == 2
    row['selected_source_artifact_path'] = 'quarantine/' + 'c' * 64 + '.parquet'
    with pytest.raises(ValueError, match='invalid_candidate_provenance'):
        audit.candidates(old, new, sha256_file(old_path))


def test_accepted_batch_incremental_worklist_requires_selected_request_and_receipt(tmp_path):
    old, new, old_path, _ = _worklists(tmp_path)
    sha, request = 'b' * 64, 'c' * 64
    path, receipt = 'bodies/fixture.parquet', 'receipts/fixture.json'
    new['contract'] = 'cnyes_43_1_incremental_candidates_v3'
    row = new['groups'][0]['candidates'][0]
    row.update(selected_response_sha256=sha, selected_source_artifact_path=path,
        selected_source_receipt_path=receipt, selected_source_request_sha256=request,
        source_observations=[{'source_response_sha256': sha, 'source_artifact_path': path,
            'source_artifact_kind': 'accepted_body_batch', 'source_receipt_path': receipt,
            'source_request_sha256': request, 'source_observed_at_utc': '2026-10-01T00:00:00+00:00'}])
    assert len(audit.candidates(old, new, sha256_file(old_path))) == 2
    row['selected_source_request_sha256'] = 'd' * 64
    with pytest.raises(ValueError, match='invalid_candidate_provenance'):
        audit.candidates(old, new, sha256_file(old_path))


@pytest.mark.parametrize("change", ["chain", "duplicate", "day", "count", "identity", "symbol", "scope"])
def test_worklist_inconsistencies_fail_closed(tmp_path, change):
    old, new, old_path, _ = _worklists(tmp_path)
    new = deepcopy(new)
    if change == "chain": new["input_sha256"][2] = "0" * 64
    if change == "duplicate":
        new["groups"][0]["candidates"][0]["cnyes_news_id"] = 1
        new["groups"][0]["candidates"][0]["cnyes_url"] = "https://news.cnyes.com/news/id/1"
    if change == "day": new["groups"][0]["cnyes_publication_day"] = "2024-06-03"
    if change == "count": new["candidate_heads"] = 2
    if change == "identity": new["groups"][0]["candidates"][0]["official_record_identity_verified"] = True
    if change == "symbol": new["groups"][0]["candidates"][0]["issuer_symbol_candidate"] = "1234X"
    if change == "scope": new["production_queue_writes"] = 1
    with pytest.raises(ValueError):
        audit.candidates(old, new, sha256_file(old_path))


def test_source_corruption_or_mutation_cannot_be_accepted(tmp_path, monkeypatch):
    _, _, old_path, new_path = _worklists(tmp_path)
    news, finlab, _, _, receipt_path = inputs(tmp_path, records=[_record("1234", "2024-06-01", SUBJECT, 0)])
    original = audit.scan
    def mutate(*args, **kwargs):
        result = original(*args, **kwargs)
        payload = json.loads(receipt_path.read_bytes()); payload["mutated"] = True
        atomic_write_json(receipt_path, payload)
        return result
    monkeypatch.setattr(audit, "scan", mutate)
    with pytest.raises(ValueError, match="changed_during_audit"):
        audit.audit(old_path, new_path, finlab)


def test_pair_limit_fails_instead_of_truncating(tmp_path, monkeypatch):
    _, _, old_path, new_path = _worklists(tmp_path)
    _, finlab, _, _, _ = inputs(tmp_path, records=[_record("1234", "2024-06-01", SUBJECT, 0)])
    monkeypatch.setattr(audit, "MAX_PAIRS", 1)
    with pytest.raises(ValueError, match="pair_limit"):
        audit.audit(old_path, new_path, finlab)
