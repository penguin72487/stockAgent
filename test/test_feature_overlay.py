from __future__ import annotations

from itertools import product

import polars as pl
import pytest

from downloader.feature_overlay import feature_coverage, latest_non_null_ms, overlay_feature_frame


def wide_oracle(base, fresh, columns):
    selected = [name for name in columns if name in fresh.columns]
    if fresh.is_empty() or not selected:
        return base
    renamed = fresh.select(["date", *selected]).rename({name: f"__fresh_{name}" for name in selected})
    joined = base.join(renamed, on="date", how="left", maintain_order="left")
    expressions = [(pl.coalesce([pl.col(f"__fresh_{name}"), pl.col(name)])
                    if name in base.columns else pl.col(f"__fresh_{name}")).alias(name)
                   for name in selected]
    return joined.with_columns(expressions).drop([f"__fresh_{name}" for name in selected])


@pytest.mark.parametrize("duplicate_base,duplicate_fresh,new_column", tuple(product((False, True), repeat=3)))
def test_narrow_overlay_matches_original_values_schema_nulls_and_order(
    duplicate_base, duplicate_fresh, new_column,
):
    dates = ["c", None, "a", "b", "b"] if duplicate_base else ["c", None, "a", "b", "d"]
    base = pl.DataFrame({"date": dates, "value": [1.0, -2.0, None, 0.0, float("nan")],
                         "untouched": [11, 12, 13, 14, 15]})
    fresh = pl.DataFrame({"date": ["b", "a", None, "b" if duplicate_fresh else "d"],
                          "value": [float("nan"), None, 7.0, -5.0], "new": [1, 2, None, 4]})
    columns = ("value", "new", "missing") if new_column else ("value", "missing")
    result = overlay_feature_frame(base, fresh, columns)
    expected = wide_oracle(base, fresh, columns)
    assert result.schema == expected.schema
    assert result.equals(expected)


def test_untouched_buffers_are_shared_and_empty_input_is_a_noop():
    base = pl.DataFrame({"date": ["a", "b"], "raw": [-3.5, None], "value": [1.0, 2.0]})
    fresh = pl.DataFrame({"date": ["b"], "value": [7.0]})
    result = overlay_feature_frame(base, fresh, ("value",))
    assert result["value"].to_list() == [1.0, 7.0]
    assert result["raw"].to_arrow().buffers()[1].address == base["raw"].to_arrow().buffers()[1].address
    assert overlay_feature_frame(base, fresh.head(0), ("value",)) is base
    assert overlay_feature_frame(base, fresh, ("not_present",)) is base


@pytest.mark.parametrize("empty", [True, False])
def test_narrow_summary_and_latest_match_full_width_oracle(empty):
    from downloader.binance_historical_features import _datetime_ms_expr
    frame = pl.DataFrame({"date": ["2026-10-17 02:00:00", "2026-10-17 01:00:00", None],
                          "value": [float("nan"), -5.0, None], "null": [None, None, None],
                          "large_unrelated": ["original", "values", None]})
    if empty:
        frame = frame.head(0)
    fields = {"one": "value", "two": "null", "three": "absent"}
    expected = {}
    for name, column in fields.items():
        selected = frame.filter(pl.col(column).is_not_null()) if column in frame.columns else frame.head(0)
        expected[name] = {"rows": selected.height,
                          "start": selected["date"].min() if selected.height else None,
                          "end": selected["date"].max() if selected.height else None}
        latest = selected.select(_datetime_ms_expr("date").max()).item()
        assert latest_non_null_ms(frame, column, 0) == (int(latest) - 60000 if latest else 0)
    assert feature_coverage(frame, fields) == expected
