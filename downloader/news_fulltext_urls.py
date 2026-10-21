"""Bounded original-URL metadata routing inside the existing news queue owner.

Redirect metadata and source_links stay intact. Literal pointers are not proof
that metadata headlines/stock associations match the publisher article; this
module neither copies associations nor admits features into training.
"""

from __future__ import annotations

from collections import Counter
from contextlib import closing, nullcontext
import hashlib
import json
import math
import re
import time
from urllib.parse import urlsplit

from stockagent.data.news_source_urls import GOOGLE_LITERAL_URL_CONTRACT, resolve_google_literal_url


_VERSION = GOOGLE_LITERAL_URL_CONTRACT
ORIGINAL_SOURCE_INVENTORY_VERSION = 1
ORIGINAL_URL_ROUTING_CONTRACT_VERSION = 2
_SOURCE_INVENTORY = f'original_source_inventory_v{ORIGINAL_SOURCE_INVENTORY_VERSION}'
_CURSOR = f'google_literal_url_cursor_v{_VERSION}'
_ROUTING = f'google_literal_url_routing_v{_VERSION}'
_AFTER = re.compile(r'[0-9a-f]{64}\Z')


def _valid_cursors(value):
    return isinstance(value, dict) and all(
        isinstance(key, str) and key and type(rowid) is int and 0 <= rowid <= 2**63 - 1
        for key, rowid in value.items())


def _valid_routing(value):
    if not isinstance(value, dict) or not isinstance(value.get('after'), str):
        return False
    if value['after'] not in {'', 'done'} and _AFTER.fullmatch(value['after']) is None:
        return False
    if set(value) == {'policy_fingerprint', 'after'}:
        return isinstance(value['policy_fingerprint'], str) and bool(value['policy_fingerprint'])
    return (set(value) == {'contract_version', 'routing_fingerprint', 'after'}
            and type(value['contract_version']) is int
            and value['contract_version'] == ORIGINAL_URL_ROUTING_CONTRACT_VERSION
            and isinstance(value['routing_fingerprint'], str)
            and _AFTER.fullmatch(value['routing_fingerprint']) is not None)


def routing_fingerprint(registry):
    """Only inputs used by literal routing, not publisher body/parser ABIs.

    Catalog, current permission evidence, namespace and finite membership mode
    can change target eligibility. Body selectors, pacing and API parsers cannot.
    New metadata revisions are still routed by their existing upsert path.
    """
    from downloader.news_fulltext_routing import input_fingerprint, source_inputs
    return input_fingerprint(source_inputs(registry), _VERSION)


def _routing_checkpoint(queue):
    fingerprint = routing_fingerprint(queue.registry)
    default = {'contract_version': ORIGINAL_URL_ROUTING_CONTRACT_VERSION,
               'routing_fingerprint': fingerprint, 'after': ''}
    progress = _read_checkpoint(queue, _ROUTING, default, _valid_routing)
    legacy = 'policy_fingerprint' in progress
    if legacy:
        # Carry a legacy keyset position only if its entire original policy
        # is proven current. No guessed old catalog, fast-forward or "done".
        current = queue.db.execute("SELECT value FROM queue_settings WHERE key='policy_fingerprint'").fetchone()
        retained = current is not None and progress['policy_fingerprint'] == current[0]
        upgraded = {**default, 'after': progress['after'] if retained else ''}
    else:
        upgraded = progress if progress['routing_fingerprint'] == fingerprint else default
    from downloader.news_fulltext_routing import initialize_route_plan
    with queue.db:
        # Begin the transaction before installing/reconciling the host plan;
        # its source inputs, cursors, migration evidence and marker are atomic.
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
                         (_ROUTING, json.dumps(upgraded, sort_keys=True)))
        replacement = initialize_route_plan(queue, upgraded if legacy else progress, upgraded,
                                           literal_version=_VERSION, read_checkpoint=_read_checkpoint)
        if legacy:
            digest = hashlib.sha256(json.dumps(progress, sort_keys=True).encode()).hexdigest()
            queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)', (
                'original_url_routing_upgrade_v2:' + digest, json.dumps({
                    'previous_checkpoint': progress, 'replacement_checkpoint': replacement,
                    'legacy_current_policy_matches': retained, 'publisher_requests': 0,
                    'article_states_deadlines_leases_or_bodies_changed': False}, sort_keys=True)))
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
                         (_ROUTING, json.dumps(replacement, sort_keys=True)))
    return replacement


def _read_checkpoint(queue, key, default, validate, *, manage_transaction=True):
    """Repair only a malformed local cursor, with exact private evidence.

    Restarting a metadata seek reuses immutable id/version proofs. It does not
    reset article leases, bodies, failure deadlines, permissions or HTTP state.
    Archive and replacement are one transaction; a storage error propagates.
    """
    row = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()
    if row is None:
        return default
    raw = row[0]
    try:
        value = json.loads(raw)
    except (ValueError, TypeError, UnicodeError):
        reason = 'invalid_json'
    else:
        if validate(value):
            return value
        reason = 'invalid_shape'
    from downloader.news_fulltext_queue import utc_now
    encoded = raw.encode('utf-8') if isinstance(raw, str) else raw if isinstance(raw, bytes) else repr(raw).encode('utf-8')
    digest = hashlib.sha256(type(raw).__name__.encode() + b'\0' + encoded).hexdigest()
    with queue.db if manage_transaction else nullcontext():
        queue.db.execute('INSERT INTO original_url_checkpoint_recoveries VALUES (?,?,?,?,?,?,1) '
                         'ON CONFLICT(setting_key,original_value_sha256) DO UPDATE SET '
                         'last_recovered_at_utc=excluded.last_recovered_at_utc,recovery_events=recovery_events+1',
                         (key, digest, raw, reason, utc_now(), utc_now()))
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
                         (key, json.dumps(default, sort_keys=True)))
    return default


def _bounded_slice(limit, max_seconds):
    if (type(limit) is not int or not 1 <= limit <= 2048 or type(max_seconds) not in (int, float)
            or not math.isfinite(max_seconds) or not 0.001 <= max_seconds <= 0.1):
        raise ValueError('invalid_original_url_resolution_slice')


def initialize_original_urls(queue):
    queue.db.executescript("""
        CREATE TABLE IF NOT EXISTS original_url_resolutions(
            contract_version INTEGER NOT NULL,alias_id BLOB NOT NULL,
            outcome TEXT NOT NULL,error_code TEXT NOT NULL,
            original_url TEXT,original_id BLOB,original_host TEXT,alternate_url TEXT,
            envelope_sha256 TEXT,envelope_bytes INTEGER NOT NULL,checked_at_utc TEXT NOT NULL,
            target_queued INTEGER NOT NULL DEFAULT 0,
            metadata_identity_verified INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(contract_version,alias_id)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS original_url_target ON original_url_resolutions(original_id,contract_version)
            WHERE original_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS original_url_host ON original_url_resolutions(original_host,contract_version,alias_id)
            WHERE original_host IS NOT NULL;
        CREATE TABLE IF NOT EXISTS original_url_metrics(
            contract_version INTEGER NOT NULL,outcome TEXT NOT NULL,error_code TEXT NOT NULL,
            target_queued INTEGER NOT NULL,urls INTEGER NOT NULL,
            PRIMARY KEY(contract_version,outcome,error_code,target_queued)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS original_url_checkpoint_recoveries(
            setting_key TEXT NOT NULL,original_value_sha256 TEXT NOT NULL,original_value,
            reason TEXT NOT NULL,first_recovered_at_utc TEXT NOT NULL,last_recovered_at_utc TEXT NOT NULL,
            recovery_events INTEGER NOT NULL,
            PRIMARY KEY(setting_key,original_value_sha256)
        ) WITHOUT ROWID;
        CREATE TRIGGER IF NOT EXISTS original_url_insert AFTER INSERT ON original_url_resolutions BEGIN
            INSERT INTO original_url_metrics VALUES(NEW.contract_version,NEW.outcome,NEW.error_code,NEW.target_queued,1)
            ON CONFLICT(contract_version,outcome,error_code,target_queued) DO UPDATE SET urls=urls+1;
        END;
        CREATE TRIGGER IF NOT EXISTS original_url_route_update AFTER UPDATE OF target_queued ON original_url_resolutions
        WHEN OLD.target_queued!=NEW.target_queued BEGIN
            UPDATE original_url_metrics SET urls=urls-1 WHERE contract_version=OLD.contract_version
                AND outcome=OLD.outcome AND error_code=OLD.error_code AND target_queued=OLD.target_queued;
            INSERT INTO original_url_metrics VALUES(NEW.contract_version,NEW.outcome,NEW.error_code,NEW.target_queued,1)
            ON CONFLICT(contract_version,outcome,error_code,target_queued) DO UPDATE SET urls=urls+1;
        END;
        CREATE TRIGGER IF NOT EXISTS original_url_delete AFTER DELETE ON original_url_resolutions BEGIN
            UPDATE original_url_metrics SET urls=urls-1 WHERE contract_version=OLD.contract_version
                AND outcome=OLD.outcome AND error_code=OLD.error_code AND target_queued=OLD.target_queued;
        END;
    """)
    initialize_original_source_inventory(queue.db)
    queue._original_url_started = time.monotonic()
    queue._original_url_start_count = queue.db.execute(
        'SELECT coalesce(sum(urls),0) FROM original_url_metrics WHERE contract_version=?', (_VERSION,)).fetchone()[0]
    # A changed source permission/catalog gets its own resumable routing scan.
    # It does not invalidate literal URL proofs or replay publisher failures.
    routing = _routing_checkpoint(queue)
    queue._original_url_routing_fingerprint = routing['routing_fingerprint']


def initialize_original_source_inventory(db):
    """Materialize host incidence once, then maintain it with proof writes.

    Only derived counts are rebuilt. The marker and backfill commit together;
    interruption cannot publish a partial inventory or touch URL/body/leases.
    No per-heartbeat scan of the redirect proofs and no publisher requests.
    Counts are metadata alias links, NOT distinct destinations or articles.
    """
    with db:
        db.execute('CREATE TABLE IF NOT EXISTS original_url_host_metrics('
                   'contract_version INTEGER NOT NULL,original_host TEXT NOT NULL,'
                   'metadata_links INTEGER NOT NULL CHECK(metadata_links>=0),'
                   'PRIMARY KEY(contract_version,original_host)) WITHOUT ROWID')
        db.execute('CREATE TRIGGER IF NOT EXISTS original_host_insert AFTER INSERT ON original_url_resolutions '
                   'WHEN NEW.original_host IS NOT NULL BEGIN '
                   'INSERT INTO original_url_host_metrics VALUES(NEW.contract_version,NEW.original_host,1) '
                   'ON CONFLICT(contract_version,original_host) DO UPDATE SET metadata_links=metadata_links+1; END')
        db.execute('CREATE TRIGGER IF NOT EXISTS original_host_delete AFTER DELETE ON original_url_resolutions '
                   'WHEN OLD.original_host IS NOT NULL BEGIN '
                   'UPDATE original_url_host_metrics SET metadata_links=metadata_links-1 '
                   'WHERE contract_version=OLD.contract_version AND original_host=OLD.original_host; END')
        db.execute('CREATE TRIGGER IF NOT EXISTS original_host_update AFTER UPDATE OF contract_version,original_host '
                   'ON original_url_resolutions WHEN OLD.contract_version!=NEW.contract_version '
                   'OR OLD.original_host IS NOT NEW.original_host BEGIN '
                   'UPDATE original_url_host_metrics SET metadata_links=metadata_links-1 '
                   'WHERE contract_version=OLD.contract_version AND original_host=OLD.original_host; '
                   'INSERT INTO original_url_host_metrics '
                   'SELECT NEW.contract_version,NEW.original_host,1 WHERE NEW.original_host IS NOT NULL '
                   'ON CONFLICT(contract_version,original_host) DO UPDATE SET metadata_links=metadata_links+1; END')
        marker = db.execute('SELECT value FROM queue_settings WHERE key=?', (_SOURCE_INVENTORY,)).fetchone()
        expected = db.execute('SELECT coalesce(sum(urls),0) FROM original_url_metrics '
                              'WHERE contract_version=? AND outcome=\'literal_original\'', (_VERSION,)).fetchone()[0]
        observed = db.execute('SELECT coalesce(sum(metadata_links),0) FROM original_url_host_metrics '
                              'WHERE contract_version=?', (_VERSION,)).fetchone()[0]
        if marker is None or marker[0] != '1' or expected != observed:
            db.execute('DELETE FROM original_url_host_metrics')
            db.execute('INSERT INTO original_url_host_metrics '
                       'SELECT contract_version,original_host,count(*) FROM original_url_resolutions '
                       'INDEXED BY original_url_host WHERE original_host IS NOT NULL '
                       'GROUP BY original_host,contract_version')
            db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (_SOURCE_INVENTORY, '1'))


def original_source_inventory(db, registry):
    """Read the bounded host summary, including originals never queued.

    Source matching classifies metadata only; it grants no permission and
    creates no article, association, body, query or request. Legacy/inconsistent
    databases expose unavailable evidence, never a silently empty inventory.
    """
    result = {'contract_version': ORIGINAL_SOURCE_INVENTORY_VERSION,
              'resolution_contract_version': _VERSION, 'available': False,
              'sources': {}, 'source_keys': None, 'literal_metadata_links': None, 'original_hosts': None,
              'grain': 'decoded metadata alias links; not distinct articles or bodies',
              'publisher_requests': 0, 'body_acceptance_proven': False,
              'metadata_identity_verified': False, 'training_injection_enabled': False}
    table = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='original_url_host_metrics'").fetchone()
    marker = db.execute('SELECT value FROM queue_settings WHERE key=?', (_SOURCE_INVENTORY,)).fetchone()
    if table is None or marker is None or marker[0] != '1':
        result['unavailable_reason'] = 'original_source_summary_not_initialized'
        return result
    rows = db.execute('SELECT original_host,metadata_links FROM original_url_host_metrics '
                      'WHERE contract_version=? AND metadata_links>0 ORDER BY original_host', (_VERSION,)).fetchall()
    expected = db.execute('SELECT coalesce(sum(urls),0) FROM original_url_metrics '
                          'WHERE contract_version=? AND outcome=\'literal_original\'', (_VERSION,)).fetchone()[0]
    total = sum(row[1] for row in rows)
    if total != expected:
        result['unavailable_reason'] = 'original_source_summary_not_reconciled'
        return result
    for host, links in rows:
        source = registry.match('https://' + host + '/')
        provider = source.provider if source else 'unregistered:' + host
        item = result['sources'].setdefault(provider, {'literal_metadata_links': 0, 'hosts': []})
        item['literal_metadata_links'] += links
        item['hosts'].append({'host': host, 'metadata_links': links})
    result.update(available=True, source_keys=len(result['sources']), literal_metadata_links=total, original_hosts=len(rows))
    return result


def _route_literal(queue, proof, metadata):
    source = queue.registry.match(proof['original_url'])
    if source and source.allowed:
        # Canonical URL dedupe/update never resets complete, refusal, due,
        # lease or finite-cohort gates. No origin_id: do not fabricate/copy
        # the redirect metadata's stock associations onto the publisher URL.
        queue.add_articles([{'url': proof['original_url'], 'title': metadata['title'],
                             'date': metadata['metadata_date_raw']}])
    exists = queue.db.execute('SELECT 1 FROM articles WHERE id=?', (proof['original_id'],)).fetchone() is not None
    if exists and not proof['target_queued']:
        queue.db.execute('UPDATE original_url_resolutions SET target_queued=1 WHERE contract_version=? AND alias_id=?',
                         (_VERSION, proof['alias_id']))
    return exists


def seed_unfrozen_finite_originals(queue, source, *, maximum=2048):
    """Admit a small, complete known-host snapshot before its first freeze.

    A new finite publisher must not wait for a global million-alias keyset
    pass, nor freeze after only the first alias arrives. The materialized host
    summary selects at most ``maximum`` existing proofs using the host index;
    native dedupe/namespace/rights/deadline rules still own every target upsert.
    Call inside the same transaction that writes finite membership. Existing
    immutable cohorts never call this function. No HTTP or global cursor move.

    Large scopes wait for the ordinary coherent routing pass instead of
    silently truncating membership. Missing summary tables retain fresh-queue
    behavior; later metadata still uses its normal original-URL ingest path.
    """
    if type(maximum) is not int or not 1 <= maximum <= 2048:
        raise ValueError('bounded_finite_original_seed_required')
    if not source.allowed or source.acquisition_mode != 'finite':
        return False
    if queue.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='original_url_host_metrics'").fetchone() is None:
        return True
    hosts = []
    for host, links in queue.db.execute('SELECT original_host,metadata_links FROM original_url_host_metrics '
                                       'WHERE contract_version=? AND metadata_links>0', (_VERSION,)):
        matched = queue.registry.match('https://' + host + '/')
        if matched and matched.provider == source.provider:
            hosts.append((host, links))
    expected = sum(links for _, links in hosts)
    if expected > maximum:
        saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (_ROUTING,)).fetchone()
        try:
            progress = json.loads(saved[0]) if saved else None
        except (ValueError, TypeError):
            progress = None
        return bool(_valid_routing(progress) and progress.get('routing_fingerprint') == routing_fingerprint(queue.registry)
                    and progress['after'] == 'done')
    proofs = []
    for host, _ in hosts:
        rows = queue.db.execute('SELECT r.*,a.title,a.metadata_date_raw FROM original_url_resolutions r '
            'INDEXED BY original_url_host JOIN articles a ON a.id=r.alias_id '
            'WHERE r.original_host=? AND r.original_host IS NOT NULL AND r.contract_version=? '
            'AND r.outcome=\'literal_original\' ORDER BY r.alias_id LIMIT ?',
            (host, _VERSION, maximum + 1)).fetchall()
        proofs.extend(rows)
        if len(proofs) > maximum:
            raise ValueError('finite_original_seed_summary_mismatch')
    if len(proofs) != expected:
        raise ValueError('finite_original_seed_summary_mismatch')
    for proof in sorted(proofs, key=lambda row: row['alias_id']):
        _route_literal(queue, proof, {'title': proof['title'], 'metadata_date_raw': proof['metadata_date_raw']})
    if proofs:
        from downloader.news_fulltext_queue import utc_now
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (
            'finite_original_seed_v1:' + source.provider, json.dumps({
                'contract_version': 1, 'observed_at_utc': utc_now(), 'provider': source.provider,
                'source_data_fingerprint': source.data_fingerprint, 'literal_aliases': len(proofs),
                'distinct_target_urls': len({p['original_id'] for p in proofs}),
                'basis': 'complete_current_host_summary_and_indexed_literal_proofs_before_first_cohort_freeze',
                'publisher_requests': 0, 'global_routing_cursor_changed': False,
                'all_publisher_history_complete': False,
            }, sort_keys=True)))
    return True


def resolve_original_metadata(queue, row):
    """One immutable metadata URL proof and routing, in the caller's transaction."""
    proof = queue.db.execute('SELECT * FROM original_url_resolutions WHERE contract_version=? AND alias_id=?',
                             (_VERSION, row['id'])).fetchone()
    if proof is None:
        from downloader.news_fulltext_queue import utc_now
        result = resolve_google_literal_url(row['url'])
        target = hashlib.sha256(result.original_url.encode()).digest() if result.original_url else None
        queue.db.execute('INSERT INTO original_url_resolutions(contract_version,alias_id,outcome,error_code,original_url,original_id,original_host,alternate_url,envelope_sha256,envelope_bytes,checked_at_utc) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                         (_VERSION, row['id'], result.outcome, result.error_code, result.original_url, target,
                          urlsplit(result.original_url).hostname if result.original_url else None,
                          result.alternate_url, result.envelope_sha256, result.envelope_bytes, utc_now()))
        proof = queue.db.execute('SELECT * FROM original_url_resolutions WHERE contract_version=? AND alias_id=?',
                                 (_VERSION, row['id'])).fetchone()
    if proof['outcome'] == 'literal_original':
        _route_literal(queue, proof, row)
    return proof['outcome']


def refresh_known_original_metadata(queue, rows):
    """New metadata revisions can update an already resolved target's sort data.

    New aliases are decoded by the bounded background cursor, not during an
    unbounded metadata-partition transaction. URL proofs remain versioned.
    """
    aliases = {r['key']: r for r in rows if r['host'] == 'news.google.com'}
    keys = list(aliases)
    for offset in range(0, len(keys), 400):
        batch = keys[offset:offset + 400]
        marks = ','.join('?' for _ in batch)
        proofs = queue.db.execute(f'SELECT * FROM original_url_resolutions WHERE contract_version=? AND alias_id IN ({marks}) AND outcome=\'literal_original\'', (_VERSION, *batch)).fetchall()
        for proof in proofs:
            row = aliases[proof['alias_id']]
            _route_literal(queue, proof, {'title': row['title'], 'metadata_date_raw': row['date']})


def backfill_original_urls(queue, *, limit=256, max_seconds=0.04, stop=None):
    """Host/state/rowid keyset seek: O(batch), not a repeated full-table scan.

    Proof, target routing and cursor commit together. A failed transaction or
    restart retries exactly that slice; id/version uniqueness prevents double
    counts. Every metadata state participates, including permission-gated ones.
    """
    _bounded_slice(limit, max_seconds)
    started = time.monotonic()
    cursors = _read_checkpoint(queue, _CURSOR, {}, _valid_cursors)
    states = [r[0] for r in queue.db.execute("SELECT state FROM state_metrics WHERE provider='unregistered:news.google.com' AND articles>0 ORDER BY state")]
    if not states:
        return {'examined': 0, 'literal_links': 0}
    position = getattr(queue, '_original_url_state_position', 0) % len(states)
    states = states[position:] + states[:position]
    queue._original_url_state_position = position + 1
    examined, literals = 0, 0
    with queue.metrics.measure('original_url_resolution'), queue.db:
        for state in states:
            rows = queue.db.execute('SELECT rowid,id,url,title,metadata_date_raw FROM articles INDEXED BY host_state '
                                    'WHERE host=? AND state=? AND rowid>? ORDER BY rowid LIMIT ?',
                                    ('news.google.com', state, cursors.get(state, 0), limit)).fetchall()
            for row in rows:
                if stop is not None and stop.is_set() or examined and time.monotonic() - started >= max_seconds:
                    break
                outcome = resolve_original_metadata(queue, row)
                examined += 1
                literals += outcome == 'literal_original'
                cursors[state] = row['rowid']
            if examined or stop is not None and stop.is_set():
                break
        if examined:
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (_CURSOR, json.dumps(cursors, sort_keys=True)))
    return {'examined': examined, 'literal_links': literals}


def sync_original_url_routes(queue, *, limit=256, max_seconds=0.04, stop=None):
    """Replay only host scopes whose exact routing inputs need examination."""
    _bounded_slice(limit, max_seconds)
    if stop is not None and stop.is_set():
        return 0
    from downloader.news_fulltext_routing import (
        DIRTY, FAIR_CURSOR, GEOMETRY, TABLE, advance_host, next_host, valid_host_turn,
    )
    fingerprint = queue._original_url_routing_fingerprint
    default = {'contract_version': ORIGINAL_URL_ROUTING_CONTRACT_VERSION,
               'routing_fingerprint': fingerprint, 'after': ''}
    progress = _read_checkpoint(queue, _ROUTING, default, _valid_routing)
    settings = dict(queue.db.execute('SELECT key,value FROM queue_settings WHERE key IN (?,?)', (DIRTY, GEOMETRY)))
    if (progress.get('routing_fingerprint') != fingerprint or settings.get(DIRTY) != '0'
            or settings.get(GEOMETRY) not in {'global', 'hosts'}):
        progress = _routing_checkpoint(queue)
        queue._original_url_routing_fingerprint = progress['routing_fingerprint']
        settings[GEOMETRY] = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (GEOMETRY,)).fetchone()[0]
    if progress['after'] == 'done':
        return 0
    global_scan = settings[GEOMETRY] == 'global'
    started, examined = time.monotonic(), 0
    with queue.metrics.measure('original_url_routing'), queue.db:
        turn = (_read_checkpoint(queue, FAIR_CURSOR, {'contract_version': 1, 'after_host': ''},
                                 valid_host_turn, manage_transaction=False) if not global_scan else None)
        turn_changed = False
        selected = []
        while examined < limit:
            if stop is not None and stop.is_set() or time.monotonic() - started >= max_seconds:
                break
            count = limit - examined
            if global_scan:
                after = bytes.fromhex(progress['after'])
                cursor = queue.db.execute('SELECT r.*,a.title,a.metadata_date_raw FROM original_url_resolutions r '
                    'JOIN articles a ON a.id=r.alias_id WHERE r.contract_version=? AND r.alias_id>? '
                    'AND r.outcome=\'literal_original\' ORDER BY r.alias_id LIMIT ?', (_VERSION, after, count))
            else:
                host = next_host(queue.db, _VERSION, after_host=turn['after_host'])
                if host is None:
                    progress['after'] = 'done'
                    break
                after = host['after']
                cursor = queue.db.execute('SELECT r.*,a.title,a.metadata_date_raw FROM original_url_resolutions r '
                    'INDEXED BY original_url_host JOIN articles a ON a.id=r.alias_id '
                    'WHERE r.original_host=? AND r.original_host IS NOT NULL AND r.contract_version=? '
                    'AND r.alias_id>? AND r.outcome=\'literal_original\' ORDER BY r.alias_id LIMIT ?',
                    (host['original_host'], _VERSION, after, count))
            consumed, exhausted = 0, False
            # Do not materialize a whole cold join before checking the time
            # budget. Otherwise an expensive fetchall repeatedly reads the
            # same unconsumed suffix. Close before target updates or rollback.
            with closing(cursor):
                while consumed < count:
                    if stop is not None and stop.is_set() or examined and time.monotonic() - started >= max_seconds:
                        break
                    proof = cursor.fetchone()
                    if proof is None:
                        exhausted = True
                        break
                    if stop is not None and stop.is_set():
                        break
                    source = queue.registry.match(proof['original_url'])
                    if source and source.allowed:
                        # Publisher targets cannot be redirector aliases. Keep
                        # admission in alias order: last nonempty titles and
                        # maximum metadata dates must not change with batching.
                        queue.add_articles([{'url': proof['original_url'], 'title': proof['title'],
                                             'date': proof['metadata_date_raw']}])
                    selected.append(proof)
                    after = proof['alias_id']
                    examined += 1
                    consumed += 1
            if global_scan:
                progress['after'] = 'done' if exhausted and not consumed else after.hex()
                if exhausted and not consumed:
                    queue.db.execute(f'UPDATE {TABLE} SET after=?,complete=1 WHERE contract_version=?', (b'', _VERSION))
                    queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (GEOMETRY, 'hosts'))
                break
            elif consumed or exhausted:
                advance_host(queue.db, _VERSION, host['original_host'], after,
                             complete=exhausted)
                turn['after_host'] = host['original_host']
                turn_changed = True
        # At most 2,048 targets; stay below SQLite's conservative bind bound.
        # Missing/nonpermitted targets remain missing. This does not create
        # targets, accept bodies or copy redirect associations.
        targets = list({proof['original_id'] for proof in selected if not proof['target_queued']})
        exists = set()
        for offset in range(0, len(targets), 400):
            batch = targets[offset:offset + 400]
            exists.update(row[0] for row in queue.db.execute(
                'SELECT id FROM articles WHERE id IN (' + ','.join('?' for _ in batch) + ')', batch))
        queue.db.executemany('UPDATE original_url_resolutions SET target_queued=1 '
                             'WHERE contract_version=? AND alias_id=? AND target_queued=0',
                             ((_VERSION, proof['alias_id']) for proof in selected
                              if not proof['target_queued'] and proof['original_id'] in exists))
        if not global_scan and next_host(queue.db, _VERSION) is None:
            progress['after'] = 'done'
        if turn_changed:
            # Commit turn, coverage, targets and literal associations together.
            # A stale/corrupt turn only changes the next start position, never
            # fast-forwards coverage or resets article/refusal/source state.
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
                             (FAIR_CURSOR, json.dumps(turn, sort_keys=True)))
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (_ROUTING, json.dumps(progress)))
    return examined


def has_original_url_work(queue):
    """Owner-only scheduling hint, not a source/body completion proof.

    Decoding envelopes and applying their literal routing proofs are separate
    passes. Do not enter a publisher/service sleep while either has work.
    Read only small counters/markers here: the dashboard's full host-coverage
    validation is unnecessary on every bounded metadata scheduling slice.
    Unknown markers keep work runnable through the existing repair path.
    """
    total, examined = queue.db.execute(
        "SELECT (SELECT coalesce(sum(articles),0) FROM state_metrics "
        "WHERE provider='unregistered:news.google.com'),"
        "(SELECT coalesce(sum(urls),0) FROM original_url_metrics WHERE contract_version=? AND urls>0)",
        (_VERSION,)).fetchone()
    if total > examined:
        return True
    from downloader.news_fulltext_routing import DIRTY, GEOMETRY

    settings = dict(queue.db.execute('SELECT key,value FROM queue_settings WHERE key IN (?,?,?)',
                                     (_ROUTING, DIRTY, GEOMETRY)))
    try:
        checkpoint = json.loads(settings.get(_ROUTING, 'null'))
    except (ValueError, TypeError, UnicodeError):
        return True
    return not (_valid_routing(checkpoint)
                and checkpoint.get('contract_version') == ORIGINAL_URL_ROUTING_CONTRACT_VERSION
                and checkpoint.get('routing_fingerprint') == queue._original_url_routing_fingerprint
                and checkpoint['after'] == 'done' and settings.get(DIRTY) == '0'
                and settings.get(GEOMETRY) in {'global', 'hosts'})


def original_url_progress(queue):
    counts, errors, queued = Counter(), Counter(), 0
    for row in queue.db.execute('SELECT outcome,error_code,target_queued,urls FROM original_url_metrics WHERE contract_version=? AND urls>0', (_VERSION,)):
        counts[row[0]] += row[3]
        if row[1]:
            errors[row[1]] += row[3]
        queued += row[3] if row[2] else 0
    total = queue.db.execute("SELECT coalesce(sum(articles),0) FROM state_metrics WHERE provider='unregistered:news.google.com'").fetchone()[0]
    examined = sum(counts.values())
    remaining = max(0, total - examined)
    elapsed = time.monotonic() - queue._original_url_started
    owner_count = examined - queue._original_url_start_count
    rate = owner_count / elapsed if elapsed >= 60 and owner_count >= 256 else None
    # A read-only projection has no acquisition-owner caches. Report the
    # actual durable marker, never repair a cursor or infer a migration just
    # because this reader runs newer Python code.
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (_ROUTING,)).fetchone()
    try:
        checkpoint = json.loads(saved[0]) if saved else None
    except (ValueError, TypeError, UnicodeError):
        checkpoint = None
    valid = _valid_routing(checkpoint)
    route_version = (checkpoint.get('contract_version', 1) if valid else None)
    from downloader.news_fulltext_routing import route_plan_progress

    return {'contract_version': _VERSION, 'routing_contract_version': route_version,
            'routing_fingerprint': checkpoint.get('routing_fingerprint') if valid else None,
            'routing_checkpoint_valid': valid,
            'routing_work': route_plan_progress(queue.db, checkpoint if valid else None, literal_version=_VERSION),
            'known_redirect_urls': total, 'examined_urls': examined,
            'remaining_urls': remaining, 'counts': dict(counts), 'errors': dict(errors),
            'literal_links_to_queued_targets': queued, 'progress_ratio': examined / total if total and examined <= total else None,
            'owner_examined_urls': owner_count, 'owner_elapsed_seconds': elapsed,
            'owner_urls_per_second': rate, 'conditional_eta_seconds': remaining / rate if rate and examined <= total else None,
            'progress_basis': 'metadata URL proof examination only; outcomes include unresolved/opaque, not body completion',
            'queued_target_count_basis': 'redirect URLs linked to a queue target, NOT distinct originals, successful bodies or permission grants',
            'original_metadata_and_source_links_retained': True, 'metadata_identity_verified': False,
            'publisher_requests': 0, 'body_acceptance_proven': False, 'training_injection_enabled': False,
            'checkpoint_recovery_events': queue.db.execute(
                'SELECT coalesce(sum(recovery_events),0) FROM original_url_checkpoint_recoveries').fetchone()[0]}
