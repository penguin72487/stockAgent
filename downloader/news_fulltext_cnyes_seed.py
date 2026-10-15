"""Reuse planned Cnyes ranges without reparsing every pagination URL.

These are scheduled ranges, not accepted history. Refused and superseded jobs
remain covered by their existing retry/child owners. Only an exact generated
pagination prefix is reduced; all other query spelling retains the old parser.
No discovery URL, outcome, source observation or publication clock is changed.
"""
from __future__ import annotations

from urllib.parse import parse_qs, urlsplit


CONTRACT_VERSION = 1


PLANNED_QUERY = """
    SELECT DISTINCT CASE WHEN substr(url,1,:prefix_length)=:prefix
        AND instr(url,'&startAt=')>:prefix_length
        AND substr(url,:prefix_length+1,instr(url,'&startAt=')-:prefix_length-1)
            =CAST(CAST(substr(url,:prefix_length+1,instr(url,'&startAt=')-:prefix_length-1)
                       AS INTEGER) AS TEXT)
        THEN substr(url,instr(url,'&startAt=')) ELSE url END
    FROM discovery WHERE url>=:lower AND url<:upper AND kind='cnyes_news'
"""


def planned_intervals(db, base, *, statistics=None):
    prefix = base + '?limit=30&page='
    rows = db.execute(PLANNED_QUERY, {'prefix': prefix, 'prefix_length': len(prefix),
                                    'lower': base + '?', 'upper': base + '@'})
    covered, parsed = set(), 0
    for row in rows:
        value = row[0]
        query = parse_qs(value if value.startswith('&') else urlsplit(value).query)
        parsed += 1
        if 'startAt' in query and 'endAt' in query:
            covered.add((int(query['startAt'][0]), int(query['endAt'][0])))
    if statistics is not None:
        statistics['queries_parsed'] = statistics.get('queries_parsed', 0) + parsed
    return sorted(covered)
