from datetime import date

import numpy as np
import polars as pl
from polars.testing import assert_frame_equal
import pytest

from scripts.benchmark_tw_public_feature_assembly import _candidate_merge
from stockagent.data import tw_public_features as features


@pytest.mark.parametrize("engine", ["eager", "streaming"])
def test_private_large_merge_preserves_all_keys_first_values_and_raw_bits(engine):
    frames = [
        pl.DataFrame({
            "date": [date(2024, 1, 2)] * 100_000,
            "symbol": [f"{index:06d}" for index in range(100_000)],
            "twpub_pe_raw": [-0.0, None, float("nan"), -1.25] + [float(index) for index in range(99_996)],
        }),
        pl.DataFrame({
            "date": [date(2024, 1, 2)] * 5,
            "symbol": ["000000", "000001", "000002", "000002", "900000"],
            "twpub_pe_raw": [9.0, 8.0, 7.0, 6.0, 5.0],
            "twpub_pb_raw": [None, -0.0, None, -2.0, 3.0],
        }),
        # This frame has no new value columns, but its outlying key must survive.
        pl.DataFrame({"date": [date(2024, 1, 3)], "symbol": ["999999"], "twpub_pb_raw": [777.0]}),
        pl.DataFrame({"date": [date(2024, 1, 4)], "symbol": ["888888"], "twpub_dividend_yield": [0.04]}),
    ]
    expected = features._merge_feature_frames(frames).sort(list(features.KEY_COLUMNS))
    actual = _candidate_merge(features, engine)(frames).sort(list(features.KEY_COLUMNS))
    assert_frame_equal(actual, expected, check_exact=True)
    for name in actual.columns[2:]:
        valid = actual[name].is_not_null().to_numpy()
        assert np.array_equal(
            actual[name].to_numpy()[valid].view(np.uint64),
            expected[name].to_numpy()[valid].view(np.uint64),
        )
    assert actual.filter(pl.col("symbol") == "900000")["twpub_pe_raw"].item() is None
    assert actual.filter(pl.col("symbol") == "999999")["twpub_pe_raw"].item() is None
    assert actual.filter(pl.col("symbol") == "999999")["twpub_pb_raw"].item() is None
    assert actual.filter(pl.col("symbol") == "000001")["twpub_pe_raw"].item() is None


@pytest.mark.parametrize("engine", ["eager", "streaming"])
def test_private_small_merge_uses_unchanged_full_join(engine):
    frames = [
        pl.DataFrame({"date": [date(2024, 1, 2)], "symbol": ["2330"], "twpub_pe_raw": [1.0]}),
        pl.DataFrame({"date": [date(2024, 1, 3)], "symbol": ["0050"], "twpub_pb_raw": [2.0]}),
        pl.DataFrame({"date": [date(2024, 1, 4)], "symbol": ["0051"], "twpub_dividend_yield": [None]}, schema_overrides={"twpub_dividend_yield": pl.Float64}),
    ]
    assert_frame_equal(
        _candidate_merge(features, engine)(frames).sort(list(features.KEY_COLUMNS)),
        features._merge_feature_frames(frames).sort(list(features.KEY_COLUMNS)),
        check_exact=True,
    )


@pytest.mark.parametrize("engine", ["eager", "streaming"])
def test_normalization_can_make_large_frames_empty_without_changing_selection(engine):
    invalid = pl.DataFrame({"date": [None] * 100_000, "symbol": ["2330"] * 100_000,
                            "twpub_pe_raw": [1.0] * 100_000}, schema_overrides={"date": pl.Date})
    valid = pl.DataFrame({"date": [date(2024, 1, 2)], "symbol": ["2330"], "twpub_pb_raw": [2.0]})
    frames = [invalid, valid, valid]
    assert_frame_equal(_candidate_merge(features, engine)(frames), features._merge_feature_frames(frames))


@pytest.mark.parametrize("engine", ["eager", "streaming"])
def test_original_duplicate_suffix_schema_rejection_is_preserved(engine):
    frame = pl.DataFrame({"date": [date(2024, 1, 2)] * 100_000,
                          "symbol": [f"{index:06d}" for index in range(100_000)],
                          "twpub_pe_raw": [1.0] * 100_000})
    with pytest.raises(pl.exceptions.DuplicateError):
        features._merge_feature_frames([frame, frame, frame])
    with pytest.raises(pl.exceptions.DuplicateError):
        _candidate_merge(features, engine)([frame, frame, frame])
