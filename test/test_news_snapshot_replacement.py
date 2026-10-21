"""Atomic status replacement is recoverable, never a stale-data or size-limit exception."""
from copy import deepcopy
import json

import pytest

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from stockagent.live import dashboard_updates, news_dashboard
from test_news_dashboard import _seed, NOW


def replacing_decoder(monkeypatch, path, replacements):
    original=json.loads
    calls=[]

    def decode(raw):
        value=original(raw)
        # Keep observations independent of the caller's deliberate mutation;
        # the decoder returns the original tree, its cache owns a byte packet.
        calls.append(deepcopy(value))
        if len(calls)<=len(replacements):
            next_value=replacements[len(calls)-1]
            if isinstance(next_value,bytes):atomic_write_bytes(path,next_value)
            else:atomic_write_json(path,next_value)
        return value

    monkeypatch.setattr(dashboard_updates.json,'loads',decode)
    return calls


def test_proven_atomic_replacement_retries_once_and_returns_only_the_new_generation(tmp_path,monkeypatch):
    path=tmp_path/'status.json';old={'generation':1,'stored':181};new={'generation':2,'stored':186}
    atomic_write_json(path,old)
    calls=replacing_decoder(monkeypatch,path,[new])
    value,attempts=news_dashboard._read_news_status(path)
    assert (value,attempts)==(new,2) and calls==[old,new]
    value['stored']=-1
    # The cached packet retains the real new generation, not caller mutation.
    assert news_dashboard._read_news_status(path)==(new,1)
    assert calls==[old,new]


def test_two_racing_generations_fail_closed_without_unbounded_retry(tmp_path,monkeypatch):
    path=tmp_path/'status.json';old={'generation':1};second={'generation':2};third={'generation':3}
    atomic_write_json(path,old)
    calls=replacing_decoder(monkeypatch,path,[second,third])
    assert news_dashboard._read_news_status(path)==({},2)
    assert calls==[old,second]
    # A subsequent independent call may now read the stable latest file.
    assert news_dashboard._read_news_status(path)==(third,1)


@pytest.mark.parametrize('replacement',[b'{"partial":',b'not JSON',b'\xffinvalid UTF-8'])
def test_replacement_with_broken_bytes_never_falls_back_to_old_current_data(tmp_path,monkeypatch,replacement):
    path=tmp_path/'status.json';old={'generation':1}
    atomic_write_json(path,old)
    calls=replacing_decoder(monkeypatch,path,[replacement])
    assert news_dashboard._read_news_status(path)==({},2)
    assert calls==[old]
    assert news_dashboard._read_news_status(path)==({},1)


@pytest.mark.parametrize('raw',[b'not JSON',b'{"partial":',b'null',b'[]',b'{}'])
def test_stable_invalid_or_empty_source_is_not_retried(tmp_path,monkeypatch,raw):
    path=tmp_path/'status.json';atomic_write_bytes(path,raw)
    original=news_dashboard._read;calls=[]

    def read(*args,**kwargs):
        calls.append(1)
        return original(*args,**kwargs)

    monkeypatch.setattr(news_dashboard,'_read',read)
    assert news_dashboard._read_news_status(path)==({},1)
    assert len(calls)==1


def test_retry_preserves_size_limit_even_when_previous_generation_was_allowed(tmp_path,monkeypatch):
    path=tmp_path/'status.json';old={'generation':1};new={'generation':2,'payload':'x'*1024}
    atomic_write_json(path,old)
    monkeypatch.setattr(news_dashboard,'MAX_STATUS_SNAPSHOT_BYTES',128)
    calls=replacing_decoder(monkeypatch,path,[new])
    assert news_dashboard._read_news_status(path)==({},2)
    assert calls==[old]


def test_missing_snapshot_is_still_unavailable_not_an_exception(tmp_path):
    assert news_dashboard._read_news_status(tmp_path/'missing.json')==({},1)


def test_public_projection_reports_attempts_and_uses_latest_counts_after_replacement(tmp_path,monkeypatch):
    old=_seed(tmp_path);new=json.loads(json.dumps(old))
    new['providers']['cnyes']['states']['complete']+=1
    new['providers']['cnyes']['states']['pending']-=1
    new['states']['complete']+=1;new['states']['pending']-=1
    new['articles_with_stored_body']+=1
    path=tmp_path/'data_news_fulltext/status.json'
    calls=replacing_decoder(monkeypatch,path,[new])
    result=news_dashboard.build_news_public_status(tmp_path,now=NOW)
    assert result['snapshot_reader_contract_version']==3
    assert result['snapshot_read_attempts']==2 and result['snapshot_read_state']=='loaded'
    assert result['health']=='running' and result['snapshot_consistent']
    row=next(r for r in result['providers']if r['id']=='cnyes')
    assert row['complete_urls']==new['providers']['cnyes']['states']['complete']
    assert calls[0]==old and calls[1]==new
