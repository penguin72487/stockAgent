from scripts.reclaim_wsl_backfill_memory import GIB, reclaim_budget_bytes


def test_no_reclaim_when_windows_has_headroom():
    assert reclaim_budget_bytes(
        windows_free_bytes=36 * GIB,
        lazyfree_bytes=20 * GIB,
        min_free_bytes=32 * GIB,
        max_reclaim_bytes=4 * GIB,
        lazyfree_reserve_bytes=2 * GIB,
    ) == 0


def test_reclaim_is_bounded_by_lazyfree_and_chunk():
    common = dict(
        windows_free_bytes=12 * GIB,
        min_free_bytes=32 * GIB,
        max_reclaim_bytes=4 * GIB,
        lazyfree_reserve_bytes=2 * GIB,
    )
    assert reclaim_budget_bytes(lazyfree_bytes=5 * GIB, **common) == 3 * GIB
    assert reclaim_budget_bytes(lazyfree_bytes=20 * GIB, **common) == 4 * GIB
    assert reclaim_budget_bytes(lazyfree_bytes=1 * GIB, **common) == 0
