from copy import deepcopy
from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from stockagent.data.factorized_normalization_reuse import NormalizedSourceReuse, calendar_is_closed
from stockagent.data.factorized_panel import verify_factorized_members
from scripts.build_tw_release_schedule_dataset import normalize_source


def events():
    return pl.DataFrame({"date": [date(2026, 10, 19)] * 3,
        "symbol": ["2330", "0050", "2303"], "feature": ["source"] * 3,
        "value": [-0.0, None, 100000001.123456], "period": ["2026-10-16"] * 3})


@pytest.mark.parametrize("closed", [False, True])
@pytest.mark.parametrize("change", ["none", "append_calendar", "old_calendar", "source", "context", "symbols", "corrupt"])
def test_normalized_source_reuse_preserves_values_and_requires_exact_clock(tmp_path, closed, change):
    old, new = tmp_path / "old", tmp_path / "new"
    old.mkdir()
    new.mkdir()
    sessions = [date(2026, 10, 16), date(2026, 10, 19)]
    identity = {"raw_sha256": "a" * 64, "context": "b", "symbols": ["2330"]}
    first = NormalizedSourceReuse(old)
    first.store("source", identity, sessions, closed, events())
    prior = deepcopy(first.members)
    if change == "append_calendar":
        sessions.append(date(2026, 10, 20))
    elif change == "old_calendar":
        sessions[0] = date(2026, 10, 15)
    elif change in {"source", "context", "symbols"}:
        identity = {**identity, {"source": "raw_sha256"}.get(change, change): "changed"}
    elif change == "corrupt":
        path = old / prior["source"]["path"]
        path.chmod(0o644)
        path.write_bytes(b"bad source")
    second = NormalizedSourceReuse(new, old, prior)
    table = second.get("source", identity, sessions)
    expect_hit = change == "none" or (closed and change == "append_calendar")
    if expect_hit:
        assert_frame_equal(table, events(), check_exact=True)
        name = prior["source"]["path"]
        assert (new / name).stat().st_ino == (old / name).stat().st_ino
    else:
        assert table is None
        second.store("source", identity, sessions, False, events())
        assert second.stats()["builds"] == 1


def test_unresolved_publication_never_survives_calendar_extension():
    sessions = [date(2026, 10, 19), date(2026, 10, 20)]
    lookup = pl.DataFrame({"date": [sessions[0], None]}, schema={"date": pl.Date})
    assert not calendar_is_closed(lookup, sessions)
    lookup = lookup.drop_nulls()
    uploads = pl.DataFrame({"known_upload_on": [sessions[-1]]})
    assert not calendar_is_closed(lookup, sessions, uploads=uploads)
    bounds = pl.DataFrame({"known_published_on": [sessions[-1]]})
    assert not calendar_is_closed(lookup, sessions, bounds=bounds)
    assert calendar_is_closed(lookup, sessions)


def test_normalized_payloads_are_in_full_generation_verification(tmp_path):
    first = NormalizedSourceReuse(tmp_path)
    first.store("source", {}, [date(2026, 10, 19)], True, events())
    manifest = {"common": next(iter(first.members.values())), "blocks": [],
                "normalized_sources": first.members}
    verify_factorized_members(tmp_path, manifest)
    path = tmp_path / first.members["source"]["path"]
    path.chmod(0o644)
    path.write_bytes(b"bad")
    with pytest.raises(ValueError, match="identity mismatch"):
        verify_factorized_members(tmp_path, manifest)


@pytest.mark.parametrize("key", ["security_lending:借券成交", "etl:inventory:大於四百張佔比"])
def test_early_wide_pruning_equals_full_normalization_tail(tmp_path, key):
    sessions = [date(2026, 10, day) for day in (8, 9, 12, 13, 14, 15, 16, 19, 20)]
    path = tmp_path / "raw.parquet"
    pl.DataFrame({"source_index": ["2026-10-08", "2026-10-09", "2026-10-12", "2026-10-16"],
        "2330": [-0.0, 10.0, 15.0, 120.0], "2317": [None, 8.0, 5.0, 12.0]}).write_parquet(path)
    full, lookup = normalize_source(path, key, sessions, ["2330", "2317"])
    cutoff = date(2026, 10, 13)
    tail, actual_lookup = normalize_source(path, key, sessions, ["2330", "2317"], available_after=cutoff)
    assert_frame_equal(tail, full.filter(pl.col("date") > cutoff), check_exact=True)
    assert_frame_equal(actual_lookup, lookup, check_exact=True)


def test_delayed_issuer_publication_keeps_pre_cutoff_context(tmp_path):
    sessions = [date(2026, 10, day) for day in (8, 9, 12, 13, 14, 15, 16, 19, 20)]
    path = tmp_path / "raw.parquet"
    key = "security_lending:借券成交"
    pl.DataFrame({"source_index": ["2026-10-08", "2026-10-12"], "2330": [8.0, 12.0]}).write_parquet(path)
    bounds = pl.DataFrame({"dataset": [key], "source_index": ["2026-10-08"],
        "symbol": ["2330"], "known_published_on": [date(2026, 10, 16)]})
    full, _ = normalize_source(path, key, sessions, ["2330"], publication_bounds=bounds)
    tail, _ = normalize_source(path, key, sessions, ["2330"], publication_bounds=bounds, available_after=date(2026, 10, 13))
    assert_frame_equal(tail, full, check_exact=True)
    assert date(2026, 10, 19) in tail["date"].to_list()
