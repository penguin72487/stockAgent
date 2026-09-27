from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import UTC, datetime
import hashlib
import io
import json
from pathlib import Path
from statistics import median
import sys
from time import perf_counter
from types import SimpleNamespace
from unittest.mock import Mock

import polars as pl
import pytest

from downloader import download_tw_cbc_fx_release_archive as fx
from downloader import download_tw_cbc_money_release_archive as money
from downloader import release_archive_io as receipts
from scripts import refresh_tw_public_release_archives as refresh


DATASET = money.OUTPUT_NAME


def _state_path(root: Path) -> Path:
    return root / "state" / f"{DATASET}.json"


def _read_state(root: Path) -> dict:
    return json.loads(_state_path(root).read_text())


def _write_fixture_state(root: Path, state: dict) -> None:
    _state_path(root).write_text(json.dumps(state), encoding="utf-8")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class _Provider:
    """Synthetic official HTML only; no provider/session/network implementation."""

    def __init__(self, *, raw_only: bool = False):
        self.requests = []
        self.periods = {1: 6, 2: 5, 3: 3}
        self.page_ids = {1: [1], 2: [2], 3: [3]}
        self.raw_only = raw_only

    def url(self, identifier: int) -> str:
        return fx.BASE + f"/tw/cp-302-{identifier}-ABC-1.html"

    def title(self, identifier: int) -> str:
        return f"115年{self.periods[identifier]}月金融情況"

    def published(self, identifier: int) -> str:
        return f"2026-{self.periods[identifier] + 1:02d}-01"

    def listing(self, page: int) -> bytes:
        items = [
            f'<li><time>{self.published(identifier)}</time>'
            f'<a href="/tw/cp-302-{identifier}-ABC-1.html">'
            f'{self.title(identifier)}</a></li>'
            for identifier in self.page_ids[page]
        ]
        page_rows = 60 if page < 3 else 1
        items.extend(
            f'<li><time>2026-01-01</time><a href="/tw/cp-302-filler-{page}-{index}.html">'
            '一般新聞</a></li>'
            for index in range(page_rows - len(items))
        )
        return (
            "".join(items)
            + f'<div class="total">共121筆資料，第{page}/3頁</div>'
            + '<select id="PageSize"><option value="60" selected>60</option></select>'
        ).encode()

    def article(self, identifier: int) -> bytes:
        paragraph = (
            "數值請見附件。" if self.raw_only and identifier == 2
            else "貨幣總計數 M1B及M2年增率分別為1.00%及2.00%。"
        )
        return (
            f'<h2 class="title">{self.title(identifier)}</h2>'
            f'<div class="publish_time"><time>{self.published(identifier)}</time></div>'
            f'<section class="cp">{paragraph}</section>'
        ).encode()

    def fetch(self, url, _limiter):
        self.requests.append(url)
        for page in self.page_ids:
            if url == money.LIST_URL.format(page=page):
                return self.listing(page)
        for identifier in self.periods:
            if url == self.url(identifier):
                return self.article(identifier)
        raise AssertionError(f"unexpected synthetic request: {url}")


@pytest.fixture
def archive_factory(tmp_path, monkeypatch):
    def create(*, raw_only=False):
        root = tmp_path / "data_tw_public"
        provider = _Provider(raw_only=raw_only)
        monkeypatch.setattr(money, "_fetch", provider.fetch)
        with money._writer_lock(root):
            summary = money.collect(root, workers=1, refresh_recent=0)
        assert summary["complete"] is True
        assert summary["scan_scope"] == "full_index"
        assert summary["saved_releases"] == 3
        return SimpleNamespace(
            root=root, provider=provider, summary=summary,
            parquet=root / f"{DATASET}.parquet",
        )

    return create


def _retain_failure(root: Path) -> None:
    money._write_state(root, {
        "dataset": DATASET, "status": "degraded", "complete": False,
        "error": "SourceAccessBlocked: fixture", "generated_at_utc": "2026-09-01T00:00:00Z",
    })


def test_full_failure_restart_uses_verified_checkpoint_and_only_recent_pages(
    archive_factory, monkeypatch,
):
    fixture = archive_factory()
    root, provider = fixture.root, fixture.provider
    full = deepcopy(_read_state(root))
    original_parquet = fixture.parquet.read_bytes()
    assert receipts.read_release_resume_state(root, DATASET) == full
    assert refresh._money_recent_pages(root, full_index=False) == 2
    assert refresh._money_recent_pages(root, full_index=True) == 0

    blocked = Mock(side_effect=money.SourceAccessBlocked("fixture upstream error page"))
    monkeypatch.setattr(money, "_fetch", blocked)
    monkeypatch.setattr(sys, "argv", [
        "money", "--output-dir", str(root), "--workers", "1", "--recent-pages", "2",
    ])
    for _ in range(2):
        with pytest.raises(money.SourceAccessBlocked, match="fixture upstream"):
            money.main()
        failed = _read_state(root)
        assert failed["status"] == "degraded"
        assert failed["complete"] is False
        assert "SourceAccessBlocked" in failed["error"]
        assert failed["resume_checkpoint"] == {"schema_version": 1, "completed_state": full}
        assert "resume_checkpoint" not in failed["resume_checkpoint"]["completed_state"]
        assert fixture.parquet.read_bytes() == original_parquet
        assert receipts.read_release_resume_state(root, DATASET) == full
        assert refresh._money_recent_pages(root, full_index=False) == 2
    assert blocked.call_count == 2

    provider.periods[4] = 7
    provider.page_ids[1] = [4, 1]
    provider.requests.clear()
    monkeypatch.setattr(money, "_fetch", provider.fetch)
    with money._writer_lock(root):
        resumed = money.collect(root, workers=1, recent_pages=2, refresh_recent=1)

    assert resumed["complete"] is True
    assert resumed["scan_scope"] == "recent_pages"
    assert resumed["saved_releases"] == resumed["registered_releases"] == 4
    assert resumed["last_full_index_scan_at_utc"] == full["last_full_index_scan_at_utc"]
    assert provider.requests == [
        money.LIST_URL.format(page=1), money.LIST_URL.format(page=2), provider.url(4),
    ]
    assert resumed["listing_receipts"][2] == full["listing_receipts"][2]
    assert resumed["parquet_sha256"] == _sha(fixture.parquet)
    assert _read_state(root)["complete"] is True
    assert "resume_checkpoint" not in _read_state(root)
    assert receipts.read_release_resume_state(root, DATASET) == resumed


@pytest.mark.parametrize("raw_only", [False, True])
def test_original_archive_can_resume_with_missing_periods_or_raw_only_releases(
    archive_factory, raw_only,
):
    fixture = archive_factory(raw_only=raw_only)
    assert fixture.summary["value_history_complete"] is False
    assert "2026-04" in fixture.summary["missing_periods"]
    _retain_failure(fixture.root)
    prior = receipts.read_release_resume_state(fixture.root, DATASET)
    rows = money._verified_resume_rows(fixture.root, prior)
    assert len({row["release_url"] for row in rows}) == 3
    assert any(row["metric"] is None for row in rows) is raw_only


@pytest.mark.parametrize("dataset", [DATASET, fx.OUTPUT_NAME])
def test_running_and_failed_writes_preserve_one_level_success_proof(
    archive_factory, dataset,
):
    fixture = archive_factory()
    success = {**fixture.summary, "dataset": dataset}
    receipts.write_release_state(fixture.root, dataset, success)
    state_path = fixture.root / "state" / f"{dataset}.json"
    for status in ("running", "degraded", "running", "degraded"):
        receipts.write_release_state(fixture.root, dataset, {
            "dataset": dataset, "status": status, "complete": True,
            "resume_checkpoint": {"schema_version": 1, "completed_state": {"forged": True}},
        })
        current = json.loads(state_path.read_text())
        assert current["complete"] is False
        assert current["status"] == status
        assert current["resume_checkpoint"]["completed_state"] == success
        assert "resume_checkpoint" not in current["resume_checkpoint"]["completed_state"]
        assert receipts.read_release_resume_state(fixture.root, dataset) == success


@pytest.mark.parametrize("change", [
    "cached_scope", "cached_flag", "offline", "wrong_dataset", "missing_hash",
    "missing_listing", "empty", "bool_count", "future_clock", "naive_clock", "bad_clock",
    "missing_generated", "future_generated", "naive_generated", "full_after_generated",
    "list_scope", "dict_scope",
])
def test_ineligible_claimed_success_cannot_replace_last_success(archive_factory, change):
    fixture = archive_factory()
    before = _state_path(fixture.root).read_bytes()
    candidate = deepcopy(fixture.summary)
    changes = {
        "cached_scope": {"scan_scope": "cached_full_index"},
        "cached_flag": {"cached_list_pages": True},
        "offline": {"offline_cache_only": True},
        "wrong_dataset": {"dataset": fx.OUTPUT_NAME},
        "missing_hash": {"parquet_sha256": None},
        "missing_listing": {"listing_receipts": []},
        "empty": {"saved_releases": 0, "registered_releases": 0},
        "bool_count": {"saved_releases": True, "registered_releases": True},
        "future_clock": {"last_full_index_scan_at_utc": "2999-01-01T00:00:00Z"},
        "naive_clock": {"last_full_index_scan_at_utc": "2026-01-01T00:00:00"},
        "bad_clock": {"last_full_index_scan_at_utc": "not a clock"},
        "missing_generated": {"generated_at_utc": None},
        "future_generated": {"generated_at_utc": "2999-01-01T00:00:00Z"},
        "naive_generated": {"generated_at_utc": "2026-01-01T00:00:00"},
        "full_after_generated": {"generated_at_utc": "2025-01-01T00:00:00Z"},
        "list_scope": {"scan_scope": []},
        "dict_scope": {"scan_scope": {}},
    }
    candidate.update(changes[change])
    with pytest.raises(ValueError):
        receipts.write_release_state(fixture.root, DATASET, candidate)
    assert _state_path(fixture.root).read_bytes() == before
    assert receipts.read_release_resume_state(fixture.root, DATASET) == fixture.summary


@pytest.mark.parametrize("change", ["version", "bool_version", "dataset", "nested_dataset", "recursive_only"])
def test_malformed_checkpoint_is_not_a_resume_hint(archive_factory, change):
    fixture = archive_factory()
    _retain_failure(fixture.root)
    state = _read_state(fixture.root)
    if change in {"version", "bool_version"}:
        state["resume_checkpoint"]["schema_version"] = 2 if change == "version" else True
    elif change == "dataset":
        state["dataset"] = fx.OUTPUT_NAME
    elif change == "nested_dataset":
        state["resume_checkpoint"]["completed_state"]["dataset"] = fx.OUTPUT_NAME
    else:
        state["resume_checkpoint"]["completed_state"] = {
            "dataset": DATASET, "status": "degraded", "complete": False,
            "resume_checkpoint": deepcopy(state["resume_checkpoint"]),
        }
    _write_fixture_state(fixture.root, state)
    assert receipts.read_release_resume_state(fixture.root, DATASET) is None
    assert refresh._money_recent_pages(fixture.root, full_index=False) == 0


@pytest.mark.parametrize("body", [
    None, b"{invalid JSON", b"[]", b"[" * 2000 + b"]" * 2000,
    b'{"padding":"' + b"x" * (4 * 1024 * 1024) + b'"}',
], ids=["absent", "invalid_json", "not_mapping", "deep_json", "oversize"])
def test_unreadable_receipt_is_not_a_resume_hint(tmp_path, body):
    if body is not None:
        path = _state_path(tmp_path)
        path.parent.mkdir()
        path.write_bytes(body)
    assert receipts.read_release_resume_state(tmp_path, DATASET) is None
    assert refresh._money_recent_pages(tmp_path, full_index=False) == 0


def test_resume_receipt_read_is_bounded(monkeypatch, tmp_path):
    reads = []

    class Receipt(io.BytesIO):
        def read(self, size=-1):
            reads.append(size)
            return super().read(size)

    monkeypatch.setattr(Path, "open", lambda *args, **kwargs: Receipt(b"{}"))
    assert receipts.read_release_resume_state(tmp_path, DATASET) is None
    assert reads == [4 * 1024 * 1024 + 1]


@pytest.mark.parametrize("change", [
    "parquet_corrupt", "parquet_absent", "listing_corrupt", "listing_absent",
    "detail_corrupt", "detail_absent", "listing_escape", "listing_symlink_escape",
    "listing_parent_path", "detail_escape", "duplicate_page", "wrong_page_url",
    "wrong_release_count", "cached_scope", "future_clock",
])
def test_invalid_resume_proof_fails_before_network_or_promotion(
    archive_factory, monkeypatch, tmp_path, change,
):
    fixture = archive_factory()
    prior = deepcopy(fixture.summary)
    listing = Path(prior["listing_receipts"][0]["path"])
    rows = pl.read_parquet(fixture.parquet).to_dicts()
    detail = Path(rows[0]["html_path"])
    if change.endswith("_corrupt") or change.endswith("_absent"):
        path = {"parquet": fixture.parquet, "listing": listing, "detail": detail}[change.split("_")[0]]
        if change.endswith("_absent"):
            path.unlink()
        else:
            path.write_bytes(b"corrupted fixture")
    elif change in {"listing_escape", "listing_symlink_escape", "detail_escape"}:
        original = detail if change == "detail_escape" else listing
        outside = tmp_path / "outside.html"
        outside.write_bytes(original.read_bytes())
        if change == "listing_symlink_escape":
            listing.unlink()
            listing.symlink_to(outside)
        elif change == "listing_escape":
            prior["listing_receipts"][0]["path"] = str(outside)
        else:
            for row in rows:
                if row["html_path"] == str(detail):
                    row["html_path"] = str(outside)
            pl.DataFrame(rows).write_parquet(fixture.parquet)
            prior["parquet_sha256"] = _sha(fixture.parquet)
    elif change == "listing_parent_path":
        prior["listing_receipts"][0]["path"] = str(listing.parent / ".." / "list" / listing.name)
    elif change == "duplicate_page":
        prior["listing_receipts"].append(deepcopy(prior["listing_receipts"][0]))
    elif change == "wrong_page_url":
        prior["listing_receipts"][0]["url"] = money.LIST_URL.format(page=2)
    elif change == "wrong_release_count":
        prior["saved_releases"] = prior["registered_releases"] = 4
    elif change == "cached_scope":
        prior["scan_scope"] = "cached_full_index"
    else:
        prior["last_full_index_scan_at_utc"] = "2999-01-01T00:00:00Z"
    _write_fixture_state(fixture.root, prior)
    candidate = receipts.read_release_resume_state(fixture.root, DATASET)
    fetch = Mock(side_effect=AssertionError("invalid resume proof must not fetch"))
    promote = Mock(side_effect=AssertionError("invalid resume proof must not promote"))
    monkeypatch.setattr(money, "_fetch", fetch)
    monkeypatch.setattr(money, "write_release_rows_if_changed", promote)
    monkeypatch.setattr(sys, "argv", [
        "money", "--output-dir", str(fixture.root), "--workers", "1", "--recent-pages", "2",
    ])

    with pytest.raises((ValueError, FileNotFoundError)):
        money.main()

    fetch.assert_not_called()
    promote.assert_not_called()
    failed = _read_state(fixture.root)
    assert failed["status"] == "degraded"
    assert failed["complete"] is False
    assert failed["error"]
    if candidate is None:
        assert "resume_checkpoint" not in failed
    else:
        assert failed["resume_checkpoint"] == {"schema_version": 1, "completed_state": candidate}


def test_cli_rejected_success_summary_remains_failed_and_retains_previous_proof(
    archive_factory, monkeypatch,
):
    fixture = archive_factory()
    previous_bytes = fixture.parquet.read_bytes()
    monkeypatch.setattr(money, "write_release_rows_if_changed", Mock(return_value=("bad hash", False)))
    monkeypatch.setattr(sys, "argv", [
        "money", "--output-dir", str(fixture.root), "--workers", "1",
        "--recent-pages", "2", "--refresh-recent", "0",
    ])
    with pytest.raises(ValueError, match="lacks an online resume proof"):
        money.main()
    failed = _read_state(fixture.root)
    assert failed["status"] == "degraded"
    assert failed["complete"] is False
    assert "ValueError" in failed["error"]
    assert failed["resume_checkpoint"]["completed_state"] == fixture.summary
    assert fixture.parquet.read_bytes() == previous_bytes


def test_replaced_parquet_and_failed_success_receipt_cannot_reuse_old_checkpoint(
    archive_factory, monkeypatch,
):
    fixture = archive_factory()
    _retain_failure(fixture.root)
    original_state = _state_path(fixture.root).read_bytes()
    frame = pl.read_parquet(fixture.parquet).with_columns((pl.col("value_pct") + 1).alias("value_pct"))
    frame.write_parquet(fixture.parquet)
    newer = {**fixture.summary, "parquet_sha256": _sha(fixture.parquet)}
    with monkeypatch.context() as context:
        context.setattr(receipts, "atomic_write_bytes", Mock(side_effect=OSError("receipt write failed")))
        with pytest.raises(OSError, match="receipt write failed"):
            receipts.write_release_state(fixture.root, DATASET, newer)
    assert _state_path(fixture.root).read_bytes() == original_state
    assert receipts.read_release_resume_state(fixture.root, DATASET) == fixture.summary
    fetch = Mock(side_effect=AssertionError("stale checkpoint must not fetch"))
    monkeypatch.setattr(money, "_fetch", fetch)
    with pytest.raises(ValueError, match="SHA-256"):
        money.collect(fixture.root, workers=1, recent_pages=2)
    fetch.assert_not_called()


def test_verified_rows_decode_pinned_bytes_and_hash_shared_details_once(archive_factory, monkeypatch):
    fixture = archive_factory()
    opened = []
    original_open = Path.open
    original_read = money.pl.read_parquet

    def open_file(path, *args, **kwargs):
        opened.append(path)
        return original_open(path, *args, **kwargs)

    def decode(source, *args, **kwargs):
        assert isinstance(source, io.BytesIO)
        return original_read(source, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_file)
    monkeypatch.setattr(money.pl, "read_parquet", decode)
    rows = money._verified_resume_rows(fixture.root, fixture.summary)
    counts = Counter(opened)
    assert len(rows) == 6
    # The Parquet plus three listing pages and three unique detail originals
    # are each opened exactly once, regardless of streamed/captured hash API.
    assert len(counts) == 7
    assert counts[fixture.parquet] == 1
    assert set(counts.values()) == {1}


@pytest.mark.parametrize("subtree", ["listing", "detail"])
@pytest.mark.parametrize("change", ["modify", "modify_restore_mtime", "replace", "symlink"])
def test_raw_change_while_hashing_rejects_resume_before_network(
    archive_factory, monkeypatch, subtree, change,
):
    import os

    fixture = archive_factory()
    path = (
        Path(fixture.summary["listing_receipts"][0]["path"]) if subtree == "listing"
        else Path(pl.read_parquet(fixture.parquet)["html_path"][0])
    )
    original_digest = money.hashlib.file_digest
    original_sha256 = money.hashlib.sha256
    captured_body = path.read_bytes()
    original_stat = path.stat()
    changed = []

    def mutate():
        body = path.read_bytes()
        if change == "modify":
            path.write_bytes(body + b" altered while verifying")
        elif change == "modify_restore_mtime":
            path.write_bytes(body[:-1] + b"!")
            os.utime(path, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
        else:
            replacement = path.with_suffix(".replacement")
            replacement.write_bytes(body)
            if change == "replace":
                replacement.replace(path)
            else:
                path.unlink()
                path.symlink_to(replacement)
        changed.append(path)

    def digest(handle, *args, **kwargs):
        result = original_digest(handle, *args, **kwargs)
        if Path(handle.name) == path:
            mutate()
        return result

    def captured_digest(body=b"", *args, **kwargs):
        result = original_sha256(body, *args, **kwargs)
        if subtree == "listing" and body == captured_body and not changed:
            mutate()
        return result

    fetch = Mock(side_effect=AssertionError("changed raw proof must not fetch"))
    promote = Mock(side_effect=AssertionError("changed raw proof must not promote"))
    monkeypatch.setattr(money.hashlib, "file_digest", digest)
    monkeypatch.setattr(money.hashlib, "sha256", captured_digest)
    monkeypatch.setattr(money, "_fetch", fetch)
    monkeypatch.setattr(money, "write_release_rows_if_changed", promote)
    monkeypatch.setattr(sys, "argv", [
        "money", "--output-dir", str(fixture.root), "--recent-pages", "2",
    ])
    with pytest.raises(ValueError, match="source changed or SHA-256"):
        money.main()
    assert changed == [path]
    fetch.assert_not_called()
    promote.assert_not_called()
    current = _read_state(fixture.root)
    assert current["status"] == "degraded"
    assert current["complete"] is False
    assert current["resume_checkpoint"]["completed_state"] == fixture.summary


def test_cached_reparse_does_not_advance_online_checkpoint(archive_factory, monkeypatch):
    fixture = archive_factory()
    fetch = Mock(side_effect=AssertionError("cached reparse must not fetch"))
    monkeypatch.setattr(money, "_fetch", fetch)
    with money._writer_lock(fixture.root):
        summary = money.collect(fixture.root, workers=1, cached_list_pages=True, refresh_recent=0)
    assert summary["status"] == "degraded"
    assert summary["complete"] is False
    assert summary["last_full_index_scan_at_utc"] is None
    assert receipts.read_release_resume_state(fixture.root, DATASET) == fixture.summary
    assert _read_state(fixture.root)["resume_checkpoint"]["completed_state"] == fixture.summary
    fetch.assert_not_called()


def test_failed_current_with_nested_success_is_not_dashboard_or_audit_completion(archive_factory):
    from scripts.audit_tw_official_release_archives import audit_one
    from stockagent.live import data_monitor_dashboard as dashboard

    fixture = archive_factory()
    _retain_failure(fixture.root)
    row = next(
        row for row in dashboard._tw_public_sources(fixture.root.parent, now=datetime.now(UTC))
        if row["id"] == f"tw-public:{DATASET}"
    )
    assert row["status"] == "degraded"
    assert row["publishable"] is False
    assert audit_one(fixture.root, DATASET)["coverage_complete"] is False


def test_synthetic_resume_verification_local_timing(archive_factory):
    fixture = archive_factory()
    provider = fixture.provider
    full_requests = Counter(provider.requests)
    assert full_requests == Counter(
        [money.LIST_URL.format(page=page) for page in (1, 2, 3)]
        + [provider.url(identifier) for identifier in (1, 2, 3)]
    )
    expected = pl.read_parquet(fixture.parquet).to_dicts()
    elapsed = []
    for _ in range(9):
        started = perf_counter()
        rows = money._verified_resume_rows(fixture.root, fixture.summary)
        elapsed.append(perf_counter() - started)
        assert rows == expected

    provider.periods[4] = 7
    provider.page_ids[1] = [4, 1]
    provider.requests.clear()
    with money._writer_lock(fixture.root):
        resumed = money.collect(fixture.root, workers=1, recent_pages=2, refresh_recent=1)
    assert resumed["complete"] is True
    assert Counter(provider.requests) == Counter([
        money.LIST_URL.format(page=1), money.LIST_URL.format(page=2), provider.url(4),
    ])
    unique_raw_files = {
        receipt["path"] for receipt in fixture.summary["listing_receipts"]
    } | {row["html_path"] for row in expected}
    print(json.dumps({
        "scope": "synthetic_local_only_no_network_or_full_history_performance_claim",
        "rows": len(expected), "pages": len(fixture.summary["listing_receipts"]),
        "unique_raw_files": len(unique_raw_files), "unique_files": len(unique_raw_files) + 1,
        "repeats": len(elapsed),
        "verification_seconds": {
            "median": median(elapsed), "min": min(elapsed), "max": max(elapsed),
        },
        "mock_lifecycle_requests": {
            "full": {"listing": 3, "detail": 3}, "resume": {"listing": 2, "detail": 1},
        },
        "full_collection_elapsed_seconds": fixture.summary["elapsed_seconds"],
        "resume_collection_elapsed_seconds": resumed["elapsed_seconds"],
    }, sort_keys=True))
