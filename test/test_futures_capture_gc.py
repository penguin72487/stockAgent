import gc

import pytest

from stockagent.backtest.futures_cuda_graph import _capture_gc_guard


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("raises", [False, True])
def test_capture_gc_guard_restores_caller_on_success_and_error(enabled, raises):
    previous = gc.isenabled()
    try:
        (gc.enable if enabled else gc.disable)()
        try:
            with _capture_gc_guard():
                assert not gc.isenabled()
                if raises:
                    raise RuntimeError("capture failed")
        except RuntimeError:
            assert raises
        assert gc.isenabled() is enabled
    finally:
        (gc.enable if previous else gc.disable)()
