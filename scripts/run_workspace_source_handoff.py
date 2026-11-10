#!/usr/bin/env python3
"""Resume reviewed source handoffs using their original locks and journals."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import workspace_source_handoff


def large_binance_handoff_allowed() -> bool:
    """Leave an eight-hour runway and the full TW session to day trading."""
    return workspace_source_handoff.binance_handoff_window_allowed(for_start=True)


def record_outcome(row):
    """Persist this owner's latest per-source check, without payload paths."""
    with workspace_source_handoff.selected_source(row['source']):
        scope = workspace_source_handoff._scope()
        path = scope.journal.with_suffix('.observation.json')
        if path.is_symlink():
            raise ValueError('source handoff observation is redirected')
        reason = str(row.get('reason', ''))
        reason_code = ('namespace_readback_mismatch' if 'independent source namespace stat differs' in reason
                       else 'source_access_during_handoff' if 'source or E namespace changed during handoff' in reason
                       else 'tw_day_trade_priority_window' if reason == 'tw_day_trade_priority_window'
                       else 'source_or_copy_recheck_required' if row['state'] == 'blocked'
                       else 'original_owner' if row['state'] == 'waiting' else None)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        atomic_write_json(path, {'contract': 'stockagent.workspace_source_handoff_observation.v1',
            'source': row['source'], 'state': row['state'], 'reason_code': reason_code,
            'observed_at_epoch': time.time()}, durable=True)
    return row


def ordered_sources(names, *, boot: bool):
    """Recover incomplete aliases, then initiate work before routine rescans.

    An active journal whose PID-1 mount is missing is recovery work, not a
    completed alias. Boot restores retain their original ordering.
    """
    if boot:
        return tuple(names)
    ranked = []
    for index, name in enumerate(names):
        try:
            with workspace_source_handoff.selected_source(name):
                journal = workspace_source_handoff.read_journal()
                state = journal.get('state') if journal else None
                if state == 'active':
                    scope = workspace_source_handoff._scope()
                    mounted = workspace_source_handoff.provider_workspace.exact_mount(
                        scope.source, Path(journal['destination']))
                    rank = 2 if mounted else 0
                else:
                    rank = 0 if state in {'prepared', 'exchanged'} else 1
        except (OSError, ValueError, TypeError, KeyError, RuntimeError):
            # The regular per-source handler reports this failure; it must not
            # abort ordering or hide independent work.
            rank = 1
        ranked.append((rank, index, name))
    return tuple(name for _rank, _index, name in sorted(ranked))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--boot', action='store_true', help='restore an existing handoff without initiating one')
    parser.add_argument('--priority-minute-boot', action='store_true',
                        help='restore only the enrolled TW minute alias before the trading service')
    args = parser.parse_args()
    if args.priority_minute_boot and not args.boot:
        parser.error('--priority-minute-boot requires --boot')
    outcomes = []
    registered = workspace_source_handoff.registered_names()
    if args.priority_minute_boot and workspace_source_handoff.MINUTE_NAME not in registered:
        print(json.dumps({'sources': [{'source': workspace_source_handoff.MINUTE_NAME,
                                       'state': 'not_enrolled'}]}, ensure_ascii=False))
        return 0
    names = ((workspace_source_handoff.MINUTE_NAME,) if args.priority_minute_boot
             else ordered_sources(registered, boot=args.boot))
    for name in names:
        try:
            with workspace_source_handoff.selected_source(name):
                if name in workspace_source_handoff.HEAVY_SOURCES and not args.boot:
                    existing = workspace_source_handoff.read_journal()
                    # A half-exchanged or active alias needs prompt recovery,
                    # including during a protected window. Only new large SHA
                    # scans or pre-exchange work wait for the safe window.
                    if ((existing is None or existing.get('state') in {'preparing', 'prepared'})
                            and (not large_binance_handoff_allowed()
                                 or name == workspace_source_handoff.OPTIONS_RAW_NAME
                                 and not workspace_source_handoff.options_raw_handoff_window_allowed(for_start=True))):
                        outcomes.append(record_outcome({'source': name, 'state': 'waiting',
                                         'reason': 'tw_day_trade_priority_window'}))
                        continue
                result = workspace_source_handoff.handoff(boot=args.boot)
            outcomes.append(record_outcome({'source': name, **result}))
        except BlockingIOError as error:
            outcomes.append(record_outcome({'source': name, 'state': 'waiting', 'reason': str(error)}))
        except RuntimeError as error:
            if str(error) == 'the source has a live owner; wait without changing it':
                outcomes.append(record_outcome({'source': name, 'state': 'waiting', 'reason': str(error)}))
            else:
                outcomes.append(record_outcome({'source': name, 'state': 'blocked', 'reason': str(error)}))
        except Exception as error:
            outcomes.append(record_outcome({'source': name, 'state': 'blocked', 'reason': str(error)}))
    print(json.dumps({'sources': outcomes}, ensure_ascii=False))
    if any(row['state'] == 'blocked' for row in outcomes):
        return 1
    if any(row['state'] == 'waiting' for row in outcomes):
        return 1 if args.boot else 75
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
