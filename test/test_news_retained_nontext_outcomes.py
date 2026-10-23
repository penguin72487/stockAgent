"""Retained native evidence changes an outcome, never HTTP or body clocks."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import time
from unittest.mock import patch

import pytest

from downloader import news_fulltext_candidate_diagnostics as candidates
from downloader import news_fulltext_policy as policy
from downloader import news_fulltext_repairs as repairs
from downloader import news_fulltext_replay as replay
from downloader.artifact_io import sha256_file
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_candidate_diagnostics import http_stub
from test_news_failure_diagnostics import fail
from test_news_globalvoices_private_originals import RAW, URL, response, settings


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())


def pump(queue):
    with patch.object(candidates.time,'monotonic',return_value=0.0):
        candidates.pump_candidates(queue)


@contextmanager
def retained(settings, tmp_path, monkeypatch, kind='native_candidate_original', raw=RAW):
    """Use the canonical old retention path, then reopen under the new ABI."""
    with monkeypatch.context() as old:
        old.setattr(policy,'GLOBALVOICES_PHOTO_CREDIT_OUTCOME_CONTRACT_VERSION',0)
        registry=PolicyRegistry(settings,base=tmp_path)
    previous=NewsQueue(tmp_path,registry)
    try:
        if kind=='native_candidate_original':
            previous.add_articles([{'url':URL}])
            with previous.db:
                previous.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',"
                    "attempts=1,due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?",(URL,))
            client=http_stub(previous,response=response(body=raw))
            request=candidates.submit_candidate(tmp_path,settings,registry,URL,'globalvoices_private_native_original_v1',
                reason='Synthetic original for a missing-only local outcome test',evidence_contract_version=13)
            pump(previous)
            claim=candidates.claim_candidate(previous,client,settings,'globalvoices')
            result=candidates.fetch_candidate(client,claim)
            candidates.finish_candidate(previous,claim,result=result);pump(previous)
            original=candidates.candidate_status(tmp_path,request['request_id'])['result_json']['private_original']
            ref={key:original[key] for key in ('path','sha256','receipt_path','response_sha256')}
            assert client.calls==[URL]
        else:
            fail(previous,response(body=raw))
            with previous.db:
                previous.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',due=0 WHERE url=?",(URL,))
            path,=tuple((tmp_path/'diagnostics').rglob('*.parquet'))
            receipt=path.with_suffix('.receipt.json');payload=json.loads(receipt.read_bytes())
            ref={'path':str(path.relative_to(tmp_path)),'sha256':sha256_file(path),
                'receipt_path':str(receipt.relative_to(tmp_path)),'response_sha256':payload['response_sha256']}
        ref.update(kind=kind,receipt_sha256=sha256_file(tmp_path/ref['receipt_path']))
        before=head(previous)
    finally:
        previous.close()
    current=NewsQueue(tmp_path,PolicyRegistry(settings,base=tmp_path))
    try:
        assert head(current)==before
        assert current.registry.sources['globalvoices'].data_fingerprint!=registry.sources['globalvoices'].data_fingerprint
        monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
            lambda *a,**k:pytest.fail('retained reclassification must not create HTTP'))
        yield current,ref,before
    finally:
        current.close()


def submit(queue, ref):
    return repairs.submit_repairs(queue.root,queue.registry,[URL],reason='Verified retained single photo, not body acceptance',
        retained_originals={URL:ref})


def consume(queue):
    # Deterministic tests of the transaction, not a weakened production slice.
    with patch.object(repairs.time,'monotonic',return_value=0.0):
        return repairs.consume_repairs(queue)


def receipt(queue, submitted):
    return json.loads((queue.root/'reviewed_repairs/processed'/(submitted['request_id']+'.outcome.json')).read_bytes())


@pytest.mark.parametrize('kind',['native_candidate_original','failure_diagnostic_original'])
def test_new_outcome_uses_immutable_originals_without_new_check_or_body(settings,tmp_path,monkeypatch,kind):
    with retained(settings,tmp_path,monkeypatch,kind) as (queue,ref,before):
        immutable={key:sha256_file(queue.root/ref[key]) for key in ('path','receipt_path')}
        native_before=[tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')] if kind.startswith('native') else []
        result=submit(queue,ref)
        assert head(queue)==before and result['publisher_requests']==0 and not result['queue_mutations']
        request=repairs._load(Path(result['request_path']))
        assert request['contract_version']==3 and request['kind']==replay.RETAINED_NONTEXT_OUTCOME_KIND
        assert consume(queue)['outcomes']=={'classified_source_body_image_only':1}
        assert head(queue)==dict(before,state='source_body_image_only',error_code='source_body_image_only')
        outcome=receipt(queue,result)
        assert outcome['reclassified_targets']==1 and outcome['admitted_targets']==0
        assert outcome['publisher_requests']==outcome['body_acceptances']==0
        assert not outcome['check_and_publication_clocks_changed'] and not outcome['accepted_as_bodies']
        assert outcome['originals']==request['originals']
        assert immutable=={key:sha256_file(queue.root/ref[key]) for key in immutable}
        assert not (queue.root/'bodies').exists()
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0]==0
        if native_before:
            assert native_before==[tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')]
        assert consume(queue)['processed']==0 and not submit(queue,ref)['submitted']


@pytest.mark.parametrize('kind',['native_candidate_original','failure_diagnostic_original'])
@pytest.mark.parametrize('field',['path','receipt_path'])
def test_corrupt_pair_is_not_repaired_by_another_request(settings,tmp_path,monkeypatch,kind,field):
    with retained(settings,tmp_path,monkeypatch,kind) as (queue,ref,before):
        # A wrong sealed digest is sufficient; immutable source files stay intact.
        ref['sha256' if field=='path' else 'receipt_sha256']='0'*64
        result=submit(queue,ref)
        assert consume(queue)['outcomes']=={'retained_outcome_evidence_invalid':1}
        assert head(queue)==before and receipt(queue,result)['reclassified_targets']==0
        assert not (queue.root/'bodies').exists()


@pytest.mark.parametrize('changes,expected',[
    ({'attempts':2},'head_changed_since_review'),
    ({'lease_owner':'another-owner'},'leased_head_protected'),
    ({'body_sha256':'a'*64},'stored_head_protected'),
    ({'due':4102444800.0},'future_or_invalid_deadline'),
])
def test_newer_head_lease_body_and_deadline_always_win(settings,tmp_path,monkeypatch,changes,expected):
    with retained(settings,tmp_path,monkeypatch) as (queue,ref,before):
        result=submit(queue,ref)
        with queue.db:
            queue.db.execute('UPDATE articles SET '+','.join(key+'=?' for key in changes)+' WHERE url=?',
                (*changes.values(),URL))
        changed=head(queue)
        assert consume(queue)['outcomes']=={expected:1}
        assert head(queue)==changed and receipt(queue,result)['reclassified_targets']==0


@pytest.mark.parametrize('kind',['native_candidate_original','failure_diagnostic_original'])
def test_later_check_cannot_use_old_original_as_a_new_observation(settings,tmp_path,monkeypatch,kind):
    with retained(settings,tmp_path,monkeypatch,kind) as (queue,ref,before):
        stamp=datetime.fromisoformat(before['last_checked_at_utc'])+timedelta(seconds=2)
        with queue.db:
            queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?',(stamp.isoformat(),URL))
        changed=head(queue)
        clock=max(time.time()+1,stamp.timestamp()+1)
        with patch.object(repairs.time,'time',return_value=clock), patch.object(repairs,'utc_now',
                return_value=datetime.fromtimestamp(clock-0.1,timezone.utc).isoformat()):
            result=submit(queue,ref)
            assert consume(queue)['outcomes']=={'retained_outcome_evidence_invalid':1}
        assert head(queue)==changed and receipt(queue,result)['reclassified_targets']==0


@pytest.mark.parametrize('kind',['native_candidate_original','failure_diagnostic_original'])
def test_unknown_layout_remains_missing_without_any_fallback_http(settings,tmp_path,monkeypatch,kind):
    raw=RAW.replace(b'Photo by',b'A real synthetic narrative written by')
    with retained(settings,tmp_path,monkeypatch,kind,raw=raw) as (queue,ref,before):
        result=submit(queue,ref)
        assert consume(queue)['outcomes']=={'retained_nontext_outcome_unresolved':1}
        assert head(queue)==before and receipt(queue,result)['reclassified_targets']==0


def test_transient_local_io_preserves_cursor_then_retries_only_local_evidence(settings,tmp_path,monkeypatch):
    with retained(settings,tmp_path,monkeypatch) as (queue,ref,before):
        result=submit(queue,ref)
        with monkeypatch.context() as failed:
            failed.setattr(replay,'sha256_file',lambda *a,**k:(_ for _ in ()).throw(OSError('disk unavailable')))
            assert consume(queue)['outcomes']=={'retained_outcome_io_deferred':1}
        assert head(queue)==before and Path(result['request_path']).exists()
        assert queue.db.execute('SELECT count(*) FROM reviewed_repair_items').fetchone()[0]==0
        assert queue.db.execute('SELECT next_index,completed_at_utc FROM reviewed_repair_jobs').fetchone()[:]==(0,None)
        assert consume(queue)['outcomes']=={'classified_source_body_image_only':1}
        assert receipt(queue,result)['reclassified_targets']==1


def test_unavailable_original_does_not_starve_other_mail(settings,tmp_path,monkeypatch):
    with retained(settings,tmp_path,monkeypatch) as (queue,ref,before):
        submit(queue,ref)
        other=URL.replace('synthetic-photo-post','another-synthetic-post')
        queue.add_articles([{'url':other}])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',"
                "attempts=1,due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?",(other,))
        repairs.submit_repairs(queue.root,queue.registry,[other],reason='Independent synthetic parser repair')
        monkeypatch.setattr(replay,'sha256_file',lambda *a,**k:(_ for _ in ()).throw(OSError('disk unavailable')))
        assert consume(queue)['outcomes']=={'retained_outcome_io_deferred':1,'enqueued':1}
        assert head(queue)==before
        assert queue.db.execute('SELECT state FROM articles WHERE url=?',(other,)).fetchone()[0]=='retry'


def test_head_rechecked_after_retained_validation(settings,tmp_path,monkeypatch):
    from downloader import news_fulltext_publishers as publishers
    with retained(settings,tmp_path,monkeypatch) as (queue,ref,before):
        submit(queue,ref);original=publishers.globalvoices_verified_nontext_body
        def changed(*args,**kwargs):
            result=original(*args,**kwargs)
            queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?',(URL,))
            return result
        monkeypatch.setattr(publishers,'globalvoices_verified_nontext_body',changed)
        assert consume(queue)['outcomes']=={'head_changed_during_retained_validation':1}
        assert head(queue)==dict(before,attempts=before['attempts']+1)


@pytest.mark.parametrize('field',['num_rows','num_row_groups'])
def test_unbounded_parquet_is_rejected_before_decode(settings,tmp_path,monkeypatch,field):
    from types import SimpleNamespace
    with retained(settings,tmp_path,monkeypatch) as (queue,ref,before):
        submit(queue,ref)
        metadata=SimpleNamespace(num_rows=1,num_row_groups=1)
        setattr(metadata,field,2)
        monkeypatch.setattr(replay.pq,'ParquetFile',lambda *a,**k:SimpleNamespace(metadata=metadata))
        assert consume(queue)['outcomes']=={'retained_outcome_evidence_invalid':1}
        assert head(queue)==before


def test_archive_recovery_does_not_reparse_or_reclassify(settings,tmp_path,monkeypatch):
    with retained(settings,tmp_path,monkeypatch) as (queue,ref,before):
        result=submit(queue,ref)
        with monkeypatch.context() as failure:
            failure.setattr(repairs,'_archive',lambda *a,**k:(_ for _ in ()).throw(OSError('archive unavailable')))
            assert consume(queue)['outcomes']=={'classified_source_body_image_only':1}
        classified=head(queue)
        assert Path(result['request_path']).exists()
        monkeypatch.setattr(replay,'classify_retained_nontext_outcome',lambda *a,**k:pytest.fail('already committed'))
        assert consume(queue)['processed']==0 and head(queue)==classified
        assert receipt(queue,result)['reclassified_targets']==1
        assert queue.db.execute('SELECT count(*) FROM reviewed_repair_items').fetchone()[0]==1


@pytest.mark.parametrize('change',[
    {'kind':'unknown'}, {'path':'../original.parquet'}, {'path':'diagnostics/20261020/other/20261020T084120853348Z-2a07e757.parquet'},
    {'receipt_path':'different.json'}, {'response_sha256':'not-a-hash'}, {'extra':'unexpected'},
])
def test_original_reference_abi_is_closed(settings,tmp_path,monkeypatch,change):
    with retained(settings,tmp_path,monkeypatch) as (queue,ref,before):
        with pytest.raises(ValueError,match='invalid_retained_outcome_reference'):
            submit(queue,dict(ref,**change))
        assert head(queue)==before and not (queue.root/'reviewed_repairs/pending').exists()


def test_single_parse_is_reused_for_old_scope_and_new_outcome(settings,tmp_path,monkeypatch):
    with retained(settings,tmp_path,monkeypatch) as (queue,ref,before):
        original=policy.public_article_document;calls=[]
        def parsed(*args,**kwargs):
            calls.append(1)
            return original(*args,**kwargs)
        monkeypatch.setattr(policy,'public_article_document',parsed)
        submit(queue,ref)
        assert consume(queue)['outcomes']=={'classified_source_body_image_only':1}
        assert len(calls)==1
