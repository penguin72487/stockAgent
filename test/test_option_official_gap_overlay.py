import polars as pl
import pytest

from scripts.build_option_official_gap_overlay import check_strike_identity


def rows(*strikes):
    return pl.DataFrame([{'product_raw': 'TXO', 'series_raw': '202610',
                          'right_raw': 'C', 'strike_raw': strike} for strike in strikes])


def test_official_vendor_identity_accepts_decimal_spelling_only():
    result = check_strike_identity(rows('100', '200'), rows('100.0', '300.0'))
    assert result['matching_float32_key_pairs_with_exact_decimal'] == 1
    assert result['official_float32_collision_groups'] == 0


def test_official_vendor_identity_rejects_float32_collision_and_lossy_match():
    with pytest.raises(ValueError, match='ambiguous'):
        check_strike_identity(rows('100.000001', '100.000002'), rows('100'))
    with pytest.raises(ValueError, match='ambiguous'):
        check_strike_identity(rows('100.000001'), rows('100'))
