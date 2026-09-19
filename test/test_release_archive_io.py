from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from downloader.release_archive_io import write_release_rows_if_changed


def test_release_archive_is_byte_stable_when_only_poll_clock_changes(tmp_path: Path) -> None:
    path = tmp_path / "releases.parquet"
    original = [{"source": "cpi", "release_id": "one", "html_sha256": "a", "observed_at_utc": "first"}]
    digest, changed = write_release_rows_if_changed(path, original, identity_columns=("source", "release_id"))
    assert changed is True
    original_stat = path.stat().st_mtime_ns
    later_poll = [{**original[0], "observed_at_utc": "later"}]
    digest_again, changed = write_release_rows_if_changed(path, later_poll, identity_columns=("source", "release_id"))
    assert changed is False
    assert digest_again == digest
    assert path.stat().st_mtime_ns == original_stat
    assert pl.read_parquet(path)["observed_at_utc"].to_list() == ["first"]

    revised = [{**later_poll[0], "html_sha256": "b"}]
    revised_digest, changed = write_release_rows_if_changed(path, revised, identity_columns=("source", "release_id"))
    assert changed is True
    assert revised_digest != digest
    assert pl.read_parquet(path)["observed_at_utc"].to_list() == ["later"]


def test_release_archive_refuses_duplicate_release_identity(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="duplicate release identity"):
        write_release_rows_if_changed(
            tmp_path / "releases.parquet",
            [{"release_id": "one"}, {"release_id": "one"}],
            identity_columns=("release_id",),
        )


def test_failed_scan_cannot_delete_previously_archived_release(tmp_path: Path) -> None:
    path = tmp_path / "releases.parquet"
    rows = [{"release_id": "one", "observed_at_utc": "first"},
            {"release_id": "two", "observed_at_utc": "first"}]
    original_digest, _ = write_release_rows_if_changed(
        path, rows, identity_columns=("release_id",)
    )
    with pytest.raises(ValueError, match="release identity regression"):
        write_release_rows_if_changed(path, rows[:1], identity_columns=("release_id",))
    assert pl.read_parquet(path).height == 2
    assert write_release_rows_if_changed(
        path, rows, identity_columns=("release_id",)
    ) == (original_digest, False)


def test_same_html_placeholder_can_upgrade_to_two_verified_values(tmp_path: Path) -> None:
    path = tmp_path / "money.parquet"
    placeholder = {"release_url": "https://www.cbc.gov.tw/example", "metric": None,
                   "html_sha256": "a" * 64, "observed_at_utc": "first"}
    write_release_rows_if_changed(path, [placeholder], identity_columns=("release_url", "metric"))
    values = [{**placeholder, "metric": metric, "observed_at_utc": "later"}
              for metric in ("m1b_yoy_pct", "m2_yoy_pct")]
    _, changed = write_release_rows_if_changed(
        path, values, identity_columns=("release_url", "metric"),
        allow_placeholder_upgrade=True,
    )
    assert changed
    assert pl.read_parquet(path)["observed_at_utc"].to_list() == ["first", "first"]
    with pytest.raises(ValueError, match="release identity regression"):
        write_release_rows_if_changed(
            path, values[:1], identity_columns=("release_url", "metric"),
            allow_placeholder_upgrade=True,
        )


def test_changed_html_cannot_upgrade_placeholder(tmp_path: Path) -> None:
    path = tmp_path / "money.parquet"
    placeholder = {"release_url": "https://www.cbc.gov.tw/example", "metric": None,
                   "html_sha256": "a" * 64, "observed_at_utc": "first"}
    write_release_rows_if_changed(path, [placeholder], identity_columns=("release_url", "metric"))
    values = [{**placeholder, "metric": metric, "html_sha256": "b" * 64}
              for metric in ("m1b_yoy_pct", "m2_yoy_pct")]
    with pytest.raises(ValueError, match="release identity regression"):
        write_release_rows_if_changed(
            path, values, identity_columns=("release_url", "metric"),
            allow_placeholder_upgrade=True,
        )


def test_late_nullable_parse_warning_is_not_lost_to_schema_sample(tmp_path: Path) -> None:
    rows = [{"release_id": str(index), "parse_warning": None}
            for index in range(101)]
    rows.append({"release_id": "101", "parse_warning": "headline_yoy_not_found"})
    path = tmp_path / "releases.parquet"
    _, changed = write_release_rows_if_changed(path, rows, identity_columns=("release_id",))
    assert changed
    assert pl.read_parquet(path)["parse_warning"].to_list()[-1] == "headline_yoy_not_found"
