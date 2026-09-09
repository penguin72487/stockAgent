"""Explicit retrospective source exclusions, never inferred market eligibility."""
from datetime import date
import re

CONTRACT_DAY_QUARANTINE_VERSION = 1
CONTRACT_DAY_QUARANTINE_POLICY = (
    'exclude_exact_physical_contract_day; preserve_other_candidates_and_decision_calendar; '
    'retrospective_source_quality_scope_not_historical_market_eligibility; never_impute_return'
)


def normalize_contract_days(entries) -> list[dict[str, str]]:
    if not isinstance(entries, (list, tuple)):
        raise ValueError('futures contract-day quarantine must be an explicit list')
    normalized = []
    for item in entries:
        if not isinstance(item, dict) or set(item) != {'date', 'physical_contract'}:
            raise ValueError('contract-day quarantine requires exactly date and physical_contract')
        day, physical = item['date'], item['physical_contract']
        if (not isinstance(day, str) or date.fromisoformat(day).isoformat() != day
                or not isinstance(physical, str)
                or not re.fullmatch(r'[A-Z0-9]+:[0-9]{4}(0[1-9]|1[0-2])', physical)):
            raise ValueError('contract-day quarantine requires ISO date and exact PRODUCT:YYYYMM')
        normalized.append({'date': day, 'physical_contract': physical})
    if len({(x['date'], x['physical_contract']) for x in normalized}) != len(normalized):
        raise ValueError('duplicate contract-day quarantine')
    return sorted(normalized, key=lambda x: (x['date'], x['physical_contract']))


def exclude_contract_days(frame, entries):
    """Keep candidate slots and row order; never rerank after an exclusion."""
    import polars as pl
    entries = normalize_contract_days(entries)
    if not entries:
        return frame
    keys = pl.DataFrame(entries).with_columns(pl.col('date').str.to_date())
    return frame.join(keys, on=['date', 'physical_contract'], how='anti', maintain_order='left')


def validate_contract_day_quarantine(coverage, entries, *, accepted):
    """Unknown or already verified keys cannot silently shrink the experiment."""
    import polars as pl
    entries = normalize_contract_days(entries)
    unresolved = coverage.filter(~pl.col('status').is_in(accepted))
    known = {(str(d), c) for d, c in unresolved.select('date', 'physical_contract').iter_rows()}
    if any((x['date'], x['physical_contract']) not in known for x in entries):
        raise ValueError('contract-day quarantine must identify an unresolved coverage key')
    return exclude_contract_days(unresolved, entries)
