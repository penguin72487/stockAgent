"""Calendar scheduling for ordinary archive merges, separate from research pins."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def validate_window(value):
    if value is None:
        return None
    if (not isinstance(value, dict) or set(value) != {'timezone', 'start_hour', 'end_hour'}
            or not isinstance(value['timezone'], str)
            or type(value['start_hour']) is not int or type(value['end_hour']) is not int
            or not 0 <= value['start_hour'] < value['end_hour'] <= 24):
        raise ValueError('daily archive window needs a bounded timezone/start/end')
    ZoneInfo(value['timezone'])
    return dict(value)


def bounds(now, window):
    window = validate_window(window)
    local = datetime.fromtimestamp(now, ZoneInfo(window['timezone']))
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight+timedelta(hours=window['start_hour']), midnight+timedelta(hours=window['end_hour'])


def allows_start(now, window):
    if window is None:
        return True
    start, end = bounds(now, window)
    return start.timestamp() <= now < end.timestamp()


def next_due(last_completed, now, window):
    """Merge once per local day; a late finish never shifts tomorrow's slot."""
    start, end = bounds(now, window)
    if now < start.timestamp():
        return start.timestamp()
    if now >= end.timestamp():
        return (start+timedelta(days=1)).timestamp()
    return ((start+timedelta(days=1)).timestamp()
            if last_completed is not None and last_completed >= start.timestamp() else start.timestamp())
