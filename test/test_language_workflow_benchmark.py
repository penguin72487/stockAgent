"""Guard against plausible but misleading benchmark acceptance."""
import numpy as np
import pytest

from scripts.benchmark_language_workflows import balanced_order, moments_prefix_once, verify_arrays
from stockagent.data.tw_all_futures import _rolling_moments


def test_verifier_rejects_roundoff_signed_zero_and_missingness_changes():
    expected = (np.array([0., 1., np.nan, np.inf]),)
    for array in [np.array([-0., 1., np.nan, np.inf]),
                  np.array([0., np.nextafter(1., 2.), np.nan, np.inf]),
                  np.array([0., 1., 0., np.inf])]:
        with pytest.raises(AssertionError):
            verify_arrays((array,), expected)
    altered_nan = expected[0].copy()
    altered_nan.view(np.uint64)[2] = 0xFFF8000000000001
    assert verify_arrays((altered_nan,), expected)['nan_masks_equal']


def test_prefix_reuse_keeps_canonical_missing_value_and_window_semantics():
    rng = np.random.default_rng(21)
    values = rng.normal(0., .01, size=129)
    values[::3] = np.nan
    values[7] = np.inf
    for window in [1, 2, 32, 128, 129, 130]:
        expected = tuple(v.ravel() for v in _rolling_moments(values[:, None], np.isfinite(values[:, None]), window))
        verify_arrays(moments_prefix_once(values, window), expected)


def test_every_balanced_pair_reverses_the_same_complete_candidate_set():
    names = ['python', 'numpy', 'numba', 'cpp', 'rust']
    for round_index in [0, 2, 4]:
        assert balanced_order(names, round_index) == list(reversed(balanced_order(names, round_index + 1)))
        assert set(balanced_order(names, round_index)) == set(names)
