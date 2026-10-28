"""Local OCR reuses sealed inputs and completed derivatives, never source calls."""
from copy import deepcopy
from dataclasses import replace
import gzip
import hashlib
import json

import pytest

from downloader.artifact_io import sha256_file
from scripts import extract_taifex_rule_review_candidates as review
from scripts.extract_news_pdf_review_candidates import read_original
from test_news_fed_private_originals import settings, queue, claim, pump, response
from test_news_candidate_diagnostics import http_stub
from downloader import news_fulltext_candidate_diagnostics as jobs


def original(queue, settings):
    client = http_stub(queue, response=response())
    submitted, job = claim(queue, settings, client)
    jobs.finish_candidate(queue, job, result=jobs.fetch_candidate(client, job)); pump(queue)
    return submitted['request_id'], client


def test_retained_input_verifies_without_http_or_queue_writes(queue, settings):
    identifier, client = original(queue, settings)
    before = [tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')]
    payload, provenance = read_original(queue.root, identifier, queue.registry.sources['fed_board'])
    assert payload == response().body and len(client.calls) == 1
    assert provenance['original']['response_sha256'] == hashlib.sha256(payload).hexdigest()
    assert read_original(queue.root, identifier, queue.registry.sources['fed_board']) == (payload, provenance)
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')] == before
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()


@pytest.mark.parametrize('case', ['source', 'raw', 'receipt', 'permission', 'reference', 'closed', 'head'])
def test_changed_original_cannot_produce_new_candidate(queue, settings, case):
    identifier, _ = original(queue, settings)
    source = queue.registry.sources['fed_board']
    reference = json.loads(queue.db.execute('SELECT result_json FROM native_candidate_jobs').fetchone()[0])['private_original']
    if case == 'source': source = replace(source, data_fingerprint='0'*64)
    elif case == 'raw': (queue.root/reference['path']).write_bytes(b'corrupt')
    elif case == 'receipt': (queue.root/reference['receipt_path']).write_text('{}')
    elif case == 'permission': (queue.root/reference['path']).chmod(0o644)
    else:
        with queue.db:
            if case == 'closed': queue.db.execute("UPDATE native_candidate_jobs SET state='reserved'")
            elif case == 'head': queue.db.execute('DELETE FROM articles')
            else:
                result = json.loads(queue.db.execute('SELECT result_json FROM native_candidate_jobs').fetchone()[0])
                result['private_original']['path'] = '../other.parquet'
                queue.db.execute('UPDATE native_candidate_jobs SET result_json=?', (json.dumps(result),))
    with pytest.raises(ValueError): read_original(queue.root, identifier, source)
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()


def test_complete_article_is_skipped_before_original_decode(queue, settings, monkeypatch):
    identifier, _ = original(queue, settings)
    with queue.db: queue.db.execute('UPDATE articles SET body_sha256=?', ('a'*64,))
    monkeypatch.setattr('pyarrow.parquet.ParquetFile', lambda *a, **k: pytest.fail('completed work must be reused'))
    assert read_original(queue.root, identifier, queue.registry.sources['fed_board']) is None


def native_pdf():
    import pymupdf
    with pymupdf.open() as document:
        page = document.new_page()
        page.insert_text((50, 50), 'Synthetic Board policy original. Value -12.50, rate 3.90 percent.\n'*3)
        return document.tobytes()


def test_verified_payload_uses_existing_extractor_and_exact_reusable_receipt(tmp_path, monkeypatch):
    payload = native_pdf()
    monkeypatch.setattr(review, 'runtime_profile', lambda *a, **k: {'synthetic_runtime':True})
    archive=tmp_path/'original.gz'; archive.write_bytes(gzip.compress(payload,mtime=0))
    item = dict(document=dict(url='https://example.invalid/source.pdf',
        content_sha256=hashlib.sha256(payload).hexdigest(), raw_sha256=sha256_file(archive),raw_path=archive.name))
    legacy = review.extract(tmp_path,tmp_path/'legacy',item,2,languages='eng')
    monkeypatch.setattr(review, 'read_verified_raw', lambda *a, **k: pytest.fail('no extra archive'))
    first = review.extract(tmp_path, tmp_path/'out', item, 2, languages='eng', verified_payload=payload)
    assert first == legacy
    assert first['candidate_only'] and not first['point_in_time_verified']
    assert first['document_pages'] == first['extracted_pages'] == 1
    assert first['pages'][0]['extraction'] == 'native'
    assert (tmp_path/'out/documents'/item['document']['content_sha256']/'source.pdf').read_bytes() == payload
    monkeypatch.setattr('pymupdf.open', lambda *a, **k: pytest.fail('reuse finished pages without decoding or OCR'))
    assert review.extract(tmp_path,tmp_path/'out',item,2,languages='eng',verified_payload=payload) == first
    for key in ('raw_sha256','url'):
        changed = deepcopy(item); changed['document'][key] = 'changed'
        with pytest.raises(ValueError, match='another extraction profile'):
            review.extract(tmp_path,tmp_path/'out',changed,2,languages='eng',verified_payload=payload)
    path = tmp_path/'out/documents'/item['document']['content_sha256']/'candidate.txt'
    path.write_text('changed')
    with pytest.raises(ValueError, match='output hash mismatch'):
        review.extract(tmp_path,tmp_path/'out',item,2,languages='eng',verified_payload=payload)


@pytest.mark.parametrize('payload', [b'', b'wrong hash', 'not bytes'])
def test_invalid_supplied_payload_never_reaches_pdf_decoder(tmp_path, monkeypatch, payload):
    monkeypatch.setattr(review, 'runtime_profile', lambda *a, **k: {})
    item = dict(document=dict(url='https://example.invalid/source.pdf', content_sha256='a'*64, raw_sha256='b'*64))
    with pytest.raises(ValueError, match='size or content hash mismatch'):
        review.extract(tmp_path,tmp_path/'out',item,2,verified_payload=payload)
    assert not (tmp_path/'out').exists()


@pytest.mark.parametrize('case', ['deferred', 'symlink', 'inside_queue'])
def test_cli_refuses_capacity_and_unsafe_outputs_before_ocr(tmp_path, monkeypatch, case):
    from types import SimpleNamespace
    from scripts import extract_news_pdf_review_candidates as news
    root = tmp_path/'queue'; root.mkdir()
    monkeypatch.setattr(news,'load_config',lambda _:({'root':'queue'},SimpleNamespace(sources={}),tmp_path))
    monkeypatch.setattr('stockagent.data_sync.cache_capacity.acquisition_dispatch_status',lambda _:dict(state='deferred'))
    monkeypatch.setattr(news,'extract',lambda *a,**k:pytest.fail('no OCR without output/capacity admission'))
    output = tmp_path/'output'
    if case == 'symlink': output.symlink_to(root, target_is_directory=True)
    elif case == 'inside_queue': output = root/'derived'
    args = ['--request-id','a'*64,'--output',str(output)]
    if case == 'deferred': assert news.main(args) == 75
    else:
        with pytest.raises(SystemExit): news.main(args)


@pytest.mark.parametrize('raises', [False, True])
def test_capacity_deferral_explains_reason_without_publishing_private_fields(tmp_path, monkeypatch, capsys, raises):
    from types import SimpleNamespace
    from scripts import extract_news_pdf_review_candidates as news
    from stockagent.data_sync.completed_forward_capacity import CapacityDeferred
    root=tmp_path/'queue';root.mkdir()
    monkeypatch.setattr(news,'load_config',lambda _:({'root':'queue'},SimpleNamespace(sources={}),tmp_path))
    def unavailable(_):
        if raises:
            raise CapacityDeferred('backing_unavailable', path='private')
        return dict(state='deferred',reason='physical_backing_headroom_unconfirmed',retryable=True,
            available_bytes=300,required_bytes=25,minimum_free_bytes=25,path='private')
    monkeypatch.setattr('stockagent.data_sync.cache_capacity.acquisition_dispatch_status',unavailable)
    monkeypatch.setattr(news,'extract',lambda *a,**k:pytest.fail('no OCR without capacity'))
    assert news.main(['--request-id','a'*64,'--output',str(tmp_path/'out')])==75
    result=json.loads(capsys.readouterr().out)
    assert result['state']=='capacity_deferred' and result['retryable']
    assert result['publisher_requests']==result['queue_writes']==0
    assert result['reason']==('backing_unavailable' if raises else 'physical_backing_headroom_unconfirmed')
    assert 'path' not in result and 'private' not in json.dumps(result)
    if not raises:
        assert result['filesystem_available_bytes']==300 and 'available_bytes' not in result


def test_cli_all_completed_writes_current_skips_and_never_invokes_ocr(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import extract_news_pdf_review_candidates as news
    monkeypatch.setattr(news,'load_config',lambda _:({'root':str(tmp_path/'queue')},
        SimpleNamespace(sources={'fed_board':object()}),news.REPO))
    monkeypatch.setattr('stockagent.data_sync.cache_capacity.acquisition_dispatch_status',lambda _:dict(state='admitted'))
    monkeypatch.setattr(news,'read_original',lambda *a,**k:None)
    monkeypatch.setattr(news,'extract',lambda *a,**k:pytest.fail('completed work cannot invoke OCR'))
    output=tmp_path/'out'
    assert news.main(['--request-id','a'*64,'--output',str(output)])==0
    progress=json.loads((output/'progress.json').read_bytes())
    assert progress['candidates_processed']==0 and progress['documents']==[]
    assert progress['skipped_complete']==['a'*64] and progress['selected_originals']==1


def three_page_pdf():
    import pymupdf
    with pymupdf.open() as document:
        for number in range(3):
            page=document.new_page(width=180,height=120)
            page.insert_text((10,30),f'Physical source page {number+1}.')
        return document.tobytes()


def test_selected_rotated_repair_reuses_other_pages_and_preserves_original(tmp_path, monkeypatch):
    from pathlib import Path
    from types import SimpleNamespace
    from PIL import Image
    payload=three_page_pdf()
    item=dict(document=dict(url='https://example.invalid/retained.pdf',
        content_sha256=hashlib.sha256(payload).hexdigest(),raw_sha256='a'*64))
    monkeypatch.setattr(review,'runtime_profile',lambda *a,**k:{'synthetic_runtime':True})
    calls=[]

    def run(args,**kwargs):
        assert args[0]=='tesseract' and kwargs['env']['OMP_THREAD_LIMIT']=='1'
        prefix=Path(args[2])
        with Image.open(args[1]) as image:
            calls.append((prefix.stem,image.size))
        prefix.with_suffix('.txt').write_text('synthetic recognition; not an observed source value')
        prefix.with_suffix('.tsv').write_text('retained TSV fixture')
        return SimpleNamespace(returncode=0,stderr=b'')

    monkeypatch.setattr(review.subprocess,'run',run)
    result=review.extract(tmp_path,tmp_path/'selected',item,2,languages='eng',force_ocr=True,
        verified_payload=payload,selected_pages=(3,1),page_rotations={3:90})
    assert [p['page'] for p in result['pages']]==[1,3]
    assert result['document_pages']==3 and result['extracted_pages']==2 and result['status']=='partial'
    assert result['candidate_only'] and not result['point_in_time_verified']
    assert result['page_selection']==dict(physical_pdf_pages=[1,3],clockwise_rotations={'3':90})
    assert calls[0][0]=='page_001_ocr' and calls[1][0]=='page_003_ocr'
    assert calls[0][1][0]>calls[0][1][1] and calls[1][1][0]<calls[1][1][1]
    assert result['pages'][1]['source_page_rotation']==0
    assert result['pages'][1]['review_rotation_clockwise']==90
    root=tmp_path/'selected/documents'/item['document']['content_sha256']
    assert (root/'source.pdf').read_bytes()==payload
    assert not (root/'page_002.png').exists()
    assert '[PAGE 2]' not in (root/'candidate.txt').read_text()
    assert 'selected_pages_v1_' in result['profile']
    monkeypatch.setattr('pymupdf.open',lambda *a,**k:pytest.fail('reuse completed repair without PDF decode'))
    again=review.extract(tmp_path,tmp_path/'selected',item,2,languages='eng',force_ocr=True,
        verified_payload=payload,selected_pages=(1,3),page_rotations={3:90})
    assert again==result and len(calls)==2
    for pages,rotations in [((1,2),{}),((1,3),{3:180})]:
        with pytest.raises(ValueError,match='another extraction profile'):
            review.extract(tmp_path,tmp_path/'selected',item,2,languages='eng',force_ocr=True,
                verified_payload=payload,selected_pages=pages,page_rotations=rotations)


@pytest.mark.parametrize('pages,rotations', [([],{}),([1,1],{}),([True],{}),([0],{}),
    (['1'],{}),([1,2,3],{}),([1],{2:90}),([1],{1:0}),([1],{1:45}),([1],{1:True}),
    ([1],{'1':90}),(None,{1:90})])
def test_selected_repair_rejects_ambiguous_scope_before_decode(tmp_path,monkeypatch,pages,rotations):
    monkeypatch.setattr('pymupdf.open',lambda *a,**k:pytest.fail('reject invalid scope before decode'))
    with pytest.raises(ValueError):
        review.extract(tmp_path,tmp_path/'out',{'document':{}},2,selected_pages=pages,page_rotations=rotations)
    assert not (tmp_path/'out').exists()


def test_selected_out_of_range_never_creates_output(tmp_path,monkeypatch):
    monkeypatch.setattr(review,'runtime_profile',lambda *a,**k:{})
    payload=three_page_pdf()
    item=dict(document=dict(url='https://example.invalid/source.pdf',raw_sha256='a'*64,
        content_sha256=hashlib.sha256(payload).hexdigest()))
    with pytest.raises(ValueError,match='outside source PDF'):
        review.extract(tmp_path,tmp_path/'out',item,2,languages='eng',verified_payload=payload,selected_pages=[4])
    assert not (tmp_path/'out').exists()


@pytest.mark.parametrize('extra', [
    ['--page','0'],['--page','1','--page','1'],['--rotate-page','1:90'],
    ['--page','1','--rotate-page','2:90'],['--page','1','--rotate-page','1:45'],
    ['--page','1','--rotate-page','1:90','--rotate-page','1:180'],
    ['--page','1','--rotate-page','garbage'],['--page','1','--request-id','b'*64],
])
def test_page_repair_cli_rejects_invalid_scope_before_source_access(tmp_path,monkeypatch,extra):
    from scripts import extract_news_pdf_review_candidates as news
    monkeypatch.setattr(news,'load_config',lambda *a,**k:pytest.fail('no source access for invalid repair scope'))
    with pytest.raises(SystemExit):
        news.main(['--request-id','a'*64,'--output',str(tmp_path/'out'),*extra])
