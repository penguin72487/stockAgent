from hashlib import sha256
import json
from types import SimpleNamespace

import pytest

from scripts import search_futures_bulletin_holiday_ocr as search
from scripts.search_futures_bulletin_holiday_ocr import matching_lines


def test_matching_lines_preserve_positive_source_clues_only():
    text = '甲\n 春節期貨市場休市 \n乙\n交割事宜\n'
    assert matching_lines(text) == ['春節期貨市場休市', '交割事宜']


def test_empty_matches_do_not_mean_a_verified_absence():
    assert matching_lines('辨識不清的掃描') == []


class Response:
    url = 'https://www.futures.org.tw/static/uploadFile/monthly/monthly_201102_100.pdf'
    headers = {'Content-Type': 'application/pdf', 'ETag': 'observed-etag'}

    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, **kwargs):
        yield self.body


def _run_search(tmp_path, monkeypatch, *, expected=None):
    import fitz
    from downloader.artifact_io import atomic_write_bytes, atomic_write_json

    doc = fitz.open()
    doc.new_page().insert_text((72, 72), 'Synthetic bulletin fixture')
    body = doc.tobytes()
    doc.close()
    filename = 'monthly_201102_100.pdf'
    if expected == 'correct':
        expected = sha256(body).hexdigest()
    monkeypatch.setattr(search, 'SOURCES', ((filename, expected),))
    monkeypatch.setattr(search.subprocess, 'run', lambda *a, **k: SimpleNamespace(
        stdout='春節帳務線索\n'.encode()))
    pages, sources = [], []
    search._search_sources(SimpleNamespace(bulletin=[filename], engine='tesseract'),
                           None, tmp_path, pages, fitz,
                           SimpleNamespace(get=lambda *a, **k: Response(body)),
                           atomic_write_bytes, atomic_write_json, sources=sources)
    return body, sources, pages


@pytest.mark.parametrize('expected,pinned', [(None, False), ('correct', True)])
def test_retained_capture_keeps_original_and_distinguishes_prior_pin(tmp_path, monkeypatch, expected, pinned):
    body, sources, pages = _run_search(tmp_path, monkeypatch, expected=expected)
    source = sources[0]
    assert (tmp_path / source['file']).read_bytes() == body
    assert source['source_digest_previously_pinned'] is pinned
    assert source['training_ready'] is False
    receipt = json.loads((tmp_path / f"{source['file']}.acquisition.json").read_text())
    assert receipt['sha256'] == sha256(body).hexdigest()
    assert receipt['http_headers']['ETag'] == 'observed-etag'
    page = pages[0]
    assert (tmp_path / page['text']).read_text() == '春節帳務線索\n'
    assert page['matching_lines'] == ['春節帳務線索']
    assert page['training_ready'] is False


def test_pinned_source_change_is_rejected_before_ocr_or_retention(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match='source content changed'):
        _run_search(tmp_path, monkeypatch, expected='0' * 64)
    assert list(tmp_path.iterdir()) == []


def test_default_january_source_pins_remain_unchanged():
    assert search.DEFAULT_BULLETINS == ('monthly_201101_99.pdf', 'monthly_201201_111.pdf')
    assert dict(search.SOURCES)['monthly_201101_99.pdf'] == '8c43d0f64b3fd19a4b9231fa79d1bae20dfd43f1c70a56e92f2cfc9305ce4017'
    assert dict(search.SOURCES)['monthly_201201_111.pdf'] == '27b895b3c2311bd048ff386a2e798eb5423a506402259786c38485f5f7cd8832'


def test_oversized_source_is_rejected(monkeypatch):
    monkeypatch.setattr(search, 'MAX_SOURCE_BYTES', 4)
    with pytest.raises(ValueError, match='bounded capture'):
        search._read_source(Response(b'%PDF-oversized'))


def test_output_generation_must_be_new(tmp_path):
    with pytest.raises(FileExistsError):
        search.main(['--engine', 'tesseract', '--output-dir', str(tmp_path)])


def test_arbitrary_download_targets_are_not_accepted(tmp_path):
    with pytest.raises(SystemExit):
        search.main(['--engine', 'tesseract', '--bulletin', '../elsewhere.pdf',
                     '--output-dir', str(tmp_path / 'new')])
    assert not (tmp_path / 'new').exists()
