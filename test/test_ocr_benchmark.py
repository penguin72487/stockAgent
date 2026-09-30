"""Acceptance must reject changed digits, evidence and incomplete document sets."""
import json

import pytest

from downloader.artifact_io import sha256_file
from scripts.benchmark_taifex_ocr import compare_outputs


def document(root, *, text='保證金 105,000 元', score=.99):
    import hashlib
    source=b'%PDF-source-fixture'
    key=hashlib.sha256(source).hexdigest()
    dest=root/'documents'/key
    dest.mkdir(parents=True)
    (dest/'source.pdf').write_bytes(source)
    (dest/'page_001.png').write_bytes(b'retained-rendered-pixels')
    (dest/'page_001_native.json').write_text('{}')
    (dest/'page_001_ocr.json').write_text(json.dumps([
        dict(txt=text,box=[[0,0],[10,0],[10,10],[0,10]],score=score)]))
    (dest/'candidate.txt').write_text(text)
    receipt=dict(status='complete',candidate_only=True,point_in_time_verified=False,
        content_sha256=key,document_pages=1,files=[dict(path=p.name,sha256=sha256_file(p)) for p in dest.iterdir()])
    (dest/'receipt.json').write_text(json.dumps(receipt))
    return dest


def test_digit_change_is_not_hidden_by_similar_confidence(tmp_path):
    a,b=tmp_path/'a',tmp_path/'b'
    document(a);document(b,text='保證金 106,000 元')
    result=compare_outputs(a,b)
    assert not result['exact_text_geometry_sources']
    assert {x['kind'] for x in result['mismatches']}=={'text_order','numeric_tokens','candidate_text'}


def test_confidence_roundoff_is_reported_separately(tmp_path):
    a,b=tmp_path/'a',tmp_path/'b'
    document(a);document(b,score=.99001)
    result=compare_outputs(a,b)
    assert result['exact_text_geometry_sources']
    assert result['max_score_delta']==pytest.approx(.00001)


def test_corrupt_evidence_is_rejected(tmp_path):
    a,b=tmp_path/'a',tmp_path/'b'
    document(a);dest=document(b)
    (dest/'page_001.png').write_bytes(b'changed')
    with pytest.raises(ValueError,match='evidence hash'):
        compare_outputs(a,b)


def test_missing_and_empty_corpus_are_not_success(tmp_path):
    a,b=tmp_path/'a',tmp_path/'b'
    document(a)
    assert not compare_outputs(a,b)['exact_text_geometry_sources']
    assert not compare_outputs(tmp_path/'empty1',tmp_path/'empty2')['exact_text_geometry_sources']
