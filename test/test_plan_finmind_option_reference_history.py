from scripts.plan_finmind_option_reference_history import partition_days
import pytest


def test_groups_only_consecutive_days_and_resets_at_size_boundaries():
    rows = [{'date': day} for day in ['2011-01-03', '2011-01-04', '2011-01-05',
                                      '2011-01-07', '2011-01-08']]
    assert [[row['date'] for row in group] for group in partition_days(rows, maximum_days=2)] == [
        ['2011-01-03', '2011-01-04'], ['2011-01-05'], ['2011-01-07', '2011-01-08']]
    with pytest.raises(ValueError, match='ordered'):
        partition_days([rows[1], rows[0]])
    with pytest.raises(ValueError, match='ordered'):
        partition_days([rows[0], rows[0]])
