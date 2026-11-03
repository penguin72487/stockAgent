"""Read-only union of a verified carry index and disjoint observed source days.

This composes source observations for the existing two-clock lookup.  It does
not admit contracts, establish complete market coverage, or model executions.
The caller must verify source manifests and identify all uncovered days.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
import fcntl
import hashlib
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.data.tw_option_full_carry_index import canonical_rows, lookup
from stockagent.data.tw_option_reference_gap_guard import mark_known_missing_days


ZONE = ZoneInfo('Asia/Taipei')
OVERLAY_CONTRACTS = {
    'taifex_option_calendar_event_index_v1',
    'tw_option_two_clock_event_index_v1',
}


def verified_overlay(manifest: Path, day: date, *, policy_sha256: str) -> tuple[pl.DataFrame, dict]:
    """Read a pinned source-only index and return its selected calendar day.

    The source and its receipts are re-hashed; this verifies stored provenance,
    not the completeness or historic delivery time of the underlying market.
    """
    manifest = Path(manifest)
    if manifest.is_symlink() or not manifest.is_file():
        raise ValueError('a regular source manifest is required')
    manifest = manifest.resolve()
    manifest_sha = sha256_file(manifest)
    doc = json.loads(manifest.read_bytes())
    if (doc.get('contract') not in OVERLAY_CONTRACTS or doc.get('training_ready') is not False
            or doc.get('policy_sha256') != policy_sha256):
        raise ValueError('a matching research-only source manifest and policy are required')
    if (doc['contract'] == 'taifex_option_calendar_event_index_v1'
            and doc.get('event_calendar_date') != day.isoformat()):
        raise ValueError('official index calendar day differs')
    pins = {}

    def pin(name: str, digest: str) -> None:
        if name in pins and pins[name] != digest:
            raise ValueError('conflicting source provenance pins: ' + name)
        pins[name] = digest

    pin(doc['policy_path'], doc['policy_sha256'])
    for group in ('policy_linked_pins', 'implementation_sha256'):
        for name, digest in doc.get(group, {}).items():
            pin(name, digest)
    if 'plan_path' in doc:
        pin(doc['plan_path'], doc['plan_sha256'])
    if 'source_manifest_path' in doc:
        pin(doc['source_manifest_path'], doc['source_manifest_sha256'])
    source_pins = doc.get('source_pins')
    if not isinstance(source_pins, list) or not source_pins:
        raise ValueError('source files and receipts must be pinned')
    source_hashes = set()
    receipt_paths = set()
    for source in source_pins:
        pin(source['path'], source['sha256'])
        pin(source['receipt_path'], source['receipt_sha256'])
        source_hashes.add(source['sha256'])
        receipt_paths.add(source['receipt_path'])
    data = Path(doc['data_path'])
    if data.is_symlink() or data.resolve().parent != manifest.parent:
        raise ValueError('source payload must belong to its manifest directory')
    pin(str(data), doc['data_sha256'])
    physical_pins = {}
    for name, digest in pins.items():
        path = Path(name)
        choices = [path]
        # The official collector updates its live manifest.  Prefer its
        # content-addressed receipt snapshot when that exact snapshot exists.
        if (doc['contract'] == 'taifex_option_calendar_event_index_v1'
                and name in receipt_paths and path.name == 'manifest.json'):
            choices = [path.parent / 'manifest_versions' / f'{digest}.json',
                       path.parent / f'{digest}.json', path]
        chosen = next((p for p in choices if not p.is_symlink() and p.is_file()
                       and sha256_file(p) == digest), None)
        if chosen is None:
            raise ValueError('source, receipt, policy or implementation pin differs: ' + name)
        physical_pins[name] = chosen
    all_events = pl.read_parquet(data)
    if all_events.height != doc.get('rows'):
        raise ValueError('source event row count differs from manifest')
    selected = all_events.filter(pl.col('source_date') == day)
    if (selected.is_empty() or selected.filter(pl.col('event_ts').dt.date() != day).height
            or not set(selected['source_sha256'].to_list()) <= source_hashes):
        raise ValueError('selected source events do not match the pinned day and files')
    if sha256_file(manifest) != manifest_sha or any(sha256_file(physical_pins[p]) != h for p, h in pins.items()):
        raise ValueError('source or provenance changed while reading')
    return selected, {'manifest_path': str(manifest), 'manifest_sha256': manifest_sha,
                      'data_path': str(data.resolve()), 'data_sha256': doc['data_sha256'],
                      'selected_day': day.isoformat(), 'selected_events': selected.height,
                      'source_hashes': sorted(set(selected['source_sha256'].to_list())),
                      'receipt_snapshots': {p: str(physical_pins[p].resolve()) for p in sorted(receipt_paths)}}


def connect_union(database: Path, overlays: list[tuple[date, pl.DataFrame]]) -> sqlite3.Connection:
    """Attach nonoverlapping calendar days to a read-only index snapshot.

    The temporary overlay and view live only on the connection.  The parent
    database is never written, and same-second prices remain separate events.
    Inputs must have already passed their source and contract-identity checks.
    """
    database = Path(database)
    if database.is_symlink() or not database.is_file():
        raise ValueError('a regular existing source index is required')
    dates = [day for day, _ in overlays]
    if len(dates) != len(set(dates)):
        raise ValueError('overlaid calendar days must be unique')
    conn = sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True)
    try:
        conn.execute('PRAGMA temp_store=MEMORY')
        conn.execute('BEGIN')
        conn.execute('CREATE TEMP TABLE overlay_events ('
                     'product TEXT NOT NULL,series TEXT NOT NULL,strike TEXT NOT NULL,right TEXT NOT NULL,'
                     'event_us INTEGER NOT NULL,price TEXT NOT NULL,source_sha256 TEXT NOT NULL,'
                     'source_window INTEGER NOT NULL,'
                     'PRIMARY KEY(product,series,strike,right,event_us,price,source_sha256)) WITHOUT ROWID')
        for number, (day, frame) in enumerate(overlays):
            if not isinstance(day, date) or isinstance(day, datetime) or 'source_date' not in frame.columns:
                raise ValueError('a calendar day and dated source events are required')
            if (frame.is_empty() or frame.filter(pl.col('source_date').is_null()
                    | pl.col('event_ts').is_null() | (pl.col('source_date') != day)
                    | (pl.col('event_ts').dt.date() != day)).height):
                raise ValueError('overlay event dates differ from the selected calendar day')
            start = int(datetime.combine(day, time.min, ZONE).timestamp()) * 1_000_000
            end = int(datetime.combine(day + timedelta(days=1), time.min, ZONE).timestamp()) * 1_000_000
            if conn.execute('SELECT 1 FROM main.events WHERE event_us>=? AND event_us<? LIMIT 1',
                            (start, end)).fetchone():
                raise ValueError('parent index already contains the selected overlay day')
            conn.executemany('INSERT INTO temp.overlay_events VALUES (?,?,?,?,?,?,?,?)',
                             [(*row, number) for row in canonical_rows(frame)])
        conn.execute('CREATE TEMP VIEW events AS SELECT * FROM main.events '
                     'UNION ALL SELECT * FROM temp.overlay_events')
        return conn
    except Exception:
        conn.close()
        raise


@contextmanager
def open_pinned_union(manifest: Path):
    """Open a completed union only while its frozen parents remain unchanged."""
    manifest = Path(manifest)
    if manifest.is_symlink() or not manifest.is_file():
        raise ValueError('a regular union manifest is required')
    manifest = manifest.resolve()
    manifest_sha = sha256_file(manifest)
    doc = json.loads(manifest.read_bytes())
    if doc.get('contract') != 'tw_option_source_union_manifest_v1' or doc.get('training_ready') is not False:
        raise ValueError('a research-only union manifest is required')
    ident = doc['parent_identity']
    fingerprint_input = {'contract': doc['contract'], 'parent_sha256': doc['parent_sha256'],
                         'plan_sha256': ident['plan_sha256'], 'policy_sha256': ident['policy_sha256'],
                         'overlays': doc['overlay_sources'], 'implementations': doc['implementation_sha256'],
                         'session_windows': doc['session_windows']}
    expected = hashlib.sha256(json.dumps(fingerprint_input, ensure_ascii=False, sort_keys=True,
                                        separators=(',', ':')).encode()).hexdigest()
    if expected != doc['data_fingerprint']:
        raise ValueError('source union fingerprint differs')
    plan = Path(ident['plan_path'])
    policy = Path(ident['policy_path'])
    pins = {str(plan): ident['plan_sha256'], str(policy): ident['policy_sha256'],
            **ident['policy_linked_pins'], **doc['implementation_sha256']}
    if any(Path(p).is_symlink() or not Path(p).is_file() or sha256_file(Path(p)) != h for p, h in pins.items()):
        raise ValueError('union policy, plan or implementation changed')
    if json.loads(plan.read_bytes()).get('not_planned_days', []) != doc['known_provider_gap_days']:
        raise ValueError('provider gap list differs from the frozen plan')
    overlays = []
    for pin in doc['overlay_sources']:
        day = date.fromisoformat(pin['selected_day'])
        frame, checked = verified_overlay(Path(pin['manifest_path']), day,
                                          policy_sha256=ident['policy_sha256'])
        if checked != pin:
            raise ValueError('union source overlay differs from its pin')
        overlays.append((day, frame))
    if len(overlays) != 2 or len({day for day, _ in overlays}) != 2:
        raise ValueError('two distinct pinned source days are required')
    database = Path(doc['parent_database'])
    wal = Path(str(database) + '-wal')
    lock_path = database.parent / '.writer.lock'
    with lock_path.open('r') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        if ((wal.exists() and wal.stat().st_size) or database.is_symlink()
                or sha256_file(database) != doc['parent_sha256']):
            raise ValueError('frozen parent index changed or has an uncheckpointed WAL')
        conn = connect_union(database, overlays)
        try:
            stored = conn.execute("SELECT value FROM main.metadata WHERE key='identity'").fetchone()
            bounds = conn.execute('SELECT count(*),min(start),max(end) FROM main.windows').fetchone()
            checked = doc['parent_checkpoints']
            if (not stored or json.loads(stored[0]) != ident
                    or bounds != (checked['windows'], checked['first_day'], checked['last_day'])):
                raise ValueError('frozen parent identity or coverage differs')
            yield conn, doc
            if (sha256_file(manifest) != manifest_sha or sha256_file(database) != doc['parent_sha256']
                    or any(sha256_file(Path(p)) != h for p, h in pins.items())):
                raise ValueError('source union inputs changed while in use')
            for pin in doc['overlay_sources']:
                _, checked = verified_overlay(Path(pin['manifest_path']), date.fromisoformat(pin['selected_day']),
                                              policy_sha256=ident['policy_sha256'])
                if checked != pin:
                    raise ValueError('union overlay changed while in use')
        finally:
            conn.close()


def lookup_pinned_union(conn: sqlite3.Connection, doc: dict, keys: list[tuple[str, str, str, str]],
                        *, day: date, clock: str) -> pl.DataFrame:
    """Use the shared lookup and conservatively keep the original gap flags."""
    if not doc['parent_checkpoints']['first_day'] <= day.isoformat() <= max(
            pin['selected_day'] for pin in doc['overlay_sources']):
        raise ValueError('requested day lies outside the pinned source horizon')
    windows = {product: tuple(bounds) for product, bounds in doc['session_windows'].items()}
    result = lookup(conn, keys, day=day, clock=clock, session_windows=windows)
    missing = sorted({date.fromisoformat(item['date']) for item in doc['known_provider_gap_days']})
    return mark_known_missing_days(result, missing_days=missing, trading_date=day, decision_time=clock)
