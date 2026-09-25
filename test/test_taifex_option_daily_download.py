"""Fail-closed reuse of the expensive TAIFEX daily option projection."""

from datetime import date
import json
import os

import pytest

from scripts import taifex_daily_download_common as common
from scripts.download_taifex_option_daily_history import (
    _can_reuse_normalized,
    _builder_fingerprint,
    _download,
    _hashed_source_receipt,
    _require_unchanged_inputs,
)
from scripts.taifex_daily_download_common import sha256_path
from stockagent.data.tw_index_options_daily import (
    TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
)


def test_reuse_requires_identical_sources_code_and_output_bytes(tmp_path):
    futures = tmp_path / "futures.parquet"
    normalized = tmp_path / "atm.parquet"
    full_chain = tmp_path / "chain.parquet"
    receipt = tmp_path / "official.csv"
    for path in (futures, normalized, full_chain, receipt):
        path.write_bytes(path.name.encode())
    source_rows = [{
        "path": str(receipt),
        "bytes": receipt.stat().st_size,
        "sha256": sha256_path(receipt),
    }]
    fingerprint = _builder_fingerprint()
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "dataset": "taifex_monthly_opening_atm_straddles",
        "contract_version": TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
        "status": "complete",
        "start_year": 2001,
        "end_date": "2026-09-22",
        "futures_path": str(futures),
        "futures_sha256": sha256_path(futures),
        "normalized_path": str(normalized),
        "normalized_sha256": sha256_path(normalized),
        "full_chain_path": str(full_chain),
        "full_chain_sha256": sha256_path(full_chain),
        "receipts": source_rows,
        "builder_fingerprint": fingerprint,
        "series_scope": "nearest_unexpired_monthly_only",
        "quality": {"rows": 1},
        "full_chain_quality": {"rows": 2},
    }), encoding="utf-8")
    kwargs = dict(
        dataset="taifex_monthly_opening_atm_straddles",
        scope="monthly",
        start_year=2001,
        end_date=date(2026, 9, 22),
        futures_path=futures,
        futures_sha256=sha256_path(futures),
        normalized=normalized,
        full_chain=full_chain,
        receipts=source_rows,
        builder_fingerprint=fingerprint,
    )
    assert _can_reuse_normalized(manifest, **kwargs)
    assert not _can_reuse_normalized(manifest, **{**kwargs, "end_date": date(2026, 9, 23)})
    assert not _can_reuse_normalized(manifest, **{
        **kwargs, "builder_fingerprint": {**fingerprint, "test": "new-code"},
    })
    assert not _can_reuse_normalized(manifest, **{
        **kwargs, "receipts": [{**source_rows[0], "sha256": "changed-source"}],
    })
    full_chain.write_bytes(b"corrupted")
    assert not _can_reuse_normalized(manifest, **kwargs)


def test_reuse_requires_receipt_and_complete_manifest(tmp_path):
    output = tmp_path / "missing.parquet"
    kwargs = dict(
        dataset="taifex_nearest_expiry_weekly_opening_atm_straddles",
        scope="weekly",
        start_year=2001,
        end_date=date(2026, 9, 22),
        futures_path=output,
        futures_sha256="unused",
        normalized=output,
        full_chain=output,
        receipts=[],
        builder_fingerprint={},
    )
    assert not _can_reuse_normalized(tmp_path / "missing.json", **kwargs)
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"status":"complete"}', encoding="utf-8")
    assert not _can_reuse_normalized(manifest, **kwargs)


def test_official_attachment_cooldown_only_after_real_download(tmp_path, monkeypatch):
    class Response:
        headers = {"Content-Disposition": 'attachment; filename="data.csv"'}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            return b"date,value\n" + b"2026-09-23,1\n" * 10

    requests = []
    sleeps = []
    monkeypatch.setattr(common.request, "urlopen", lambda req, timeout: (
        requests.append((req, timeout)) or Response()
    ))
    monkeypatch.setattr(common.time, "sleep", sleeps.append)
    target = tmp_path / "official.csv"
    args = ({"down_type": "1"}, target)

    assert _download(*args, attempts=1, request_interval=1.0) == target
    first_bytes = target.read_bytes()
    assert _download(*args, attempts=1, request_interval=1.0) == target
    assert target.read_bytes() == first_bytes
    assert len(requests) == 1
    assert sleeps == [1.0]


def test_source_signature_rejects_same_size_rewrite_even_with_restored_mtime(tmp_path):
    path = tmp_path / "official.csv"
    path.write_bytes(b"original")
    receipt, signature = _hashed_source_receipt(path)
    assert receipt["bytes"] == 8
    assert receipt["sha256"] == sha256_path(path)

    original = path.stat()
    path.write_bytes(b"replaced")
    os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
    with pytest.raises(ValueError, match="changed during normalization"):
        _require_unchanged_inputs({path: signature}, _builder_fingerprint())

    link = tmp_path / "redirected.csv"
    link.symlink_to(path)
    with pytest.raises(ValueError, match="not a regular file"):
        _hashed_source_receipt(link)
