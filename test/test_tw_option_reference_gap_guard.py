from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from stockagent.data.tw_option_reference_gap_guard import mark_known_missing_days, require_no_known_missing_days


def references(*timestamps):
    events = [datetime.fromisoformat(value).replace(tzinfo=ZoneInfo('Asia/Taipei'))
              for value in timestamps]
    return pl.DataFrame({'trade_ts': events,
                         'available_by': [value + timedelta(seconds=1) for value in events]})


def test_marks_gap_between_observation_and_decision_but_not_future_gap():
    frame = references('2011-02-15T13:30:00', '2011-02-16T08:45:01', '2011-02-17T08:45:01')
    marked = mark_known_missing_days(frame, missing_days=[date(2011, 2, 16), date(2011, 2, 18)],
                                    trading_date=date(2011, 2, 17), decision_time='09:00')
    assert marked['known_missing_day_since_reference'].to_list() == [True, True, False]
    assert marked['latest_known_missing_source_day'].to_list() == [date(2011, 2, 16)] * 3
    with pytest.raises(ValueError, match='missing source day'):
        require_no_known_missing_days(marked)
    require_no_known_missing_days(marked.filter(~pl.col('known_missing_day_since_reference')))


def test_missing_days_must_be_ordered_and_observation_available_before_cutoff():
    frame = references('2011-02-17T08:45:00')
    with pytest.raises(ValueError, match='unavailable'):
        mark_known_missing_days(frame, missing_days=[], trading_date=date(2011, 2, 17), decision_time='08:45')
    with pytest.raises(ValueError, match='ordered'):
        mark_known_missing_days(frame, missing_days=[date(2011, 2, 16), date(2011, 2, 16)],
                                trading_date=date(2011, 2, 17), decision_time='09:00')
    result = mark_known_missing_days(frame, missing_days=[date(2011, 2, 18)],
                                     trading_date=date(2011, 2, 17), decision_time='09:00')
    assert result['known_missing_day_since_reference'].to_list() == [False]
    assert result['latest_known_missing_source_day'].to_list() == [None]
    with pytest.raises(ValueError, match='missing source day'):
        require_no_known_missing_days(frame)
