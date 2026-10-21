"""Durable host-scoped eligibility work for retained literal URL proofs.

The existing routing fingerprint/cursor remains the public compatibility
marker. A separate, versioned plan records exact source inputs and per-host
coverage. Only proven unchanged coverage is reused; unknown/corrupt history
gets a bounded replay through the ordinary queue, never a permission grant.
"""
from __future__ import annotations

import hashlib
import json
import re


PLAN_VERSION = 1
INPUTS = 'original_url_routing_inputs_v1'
DIRTY = 'original_url_routing_new_hosts_v1'
GEOMETRY = 'original_url_routing_geometry_v1'
FAIR_CURSOR = 'original_url_routing_host_turn_v1'
TABLE = 'original_url_routing_hosts'
_HEX = re.compile(r'[0-9a-f]{64}\Z')


def source_inputs(registry):
    return [[s.provider, sorted(s.domains), s.allowed, s.authorization_fingerprint,
             s.article_path_pattern, s.acquisition_mode]
            for s in sorted(registry.sources.values(), key=lambda s: s.provider)]


def input_fingerprint(inputs, literal_version):
    # Keep the v2 eligibility fingerprint: changing physical seek geometry
    # does not change source permissions or invalidate a proven v2 prefix.
    return hashlib.sha256(json.dumps([2, literal_version, inputs], separators=(',', ':')).encode()).hexdigest()


def _valid_manifest(value, literal_version):
    if (not isinstance(value, dict) or set(value) != {'contract_version', 'routing_fingerprint', 'sources'}
            or type(value['contract_version']) is not int or value['contract_version'] != PLAN_VERSION
            or not isinstance(value['routing_fingerprint'], str) or not isinstance(value['sources'], list)):
        return False
    providers, domains = set(), set()
    for row in value['sources']:
        if (not isinstance(row, list) or len(row) != 6 or not isinstance(row[0], str) or not row[0]
                or row[0] in providers or not isinstance(row[1], list)
                or any(not isinstance(d, str) or not d or d in domains for d in row[1])
                or row[1] != sorted(set(row[1]))
                or type(row[2]) is not bool or not isinstance(row[3], str) or _HEX.fullmatch(row[3]) is None
                or not isinstance(row[4], str) or not isinstance(row[5], str)
                or row[5] not in {'continuous', 'finite'}):
            return False
        providers.add(row[0])
        domains.update(row[1])
    return (value['sources'] == sorted(value['sources'], key=lambda row: row[0])
            and value['routing_fingerprint'] == input_fingerprint(value['sources'], literal_version))


def _host_signatures(inputs):
    # Domain declarations choose the effective source; adding another domain
    # to that same source does not change this host's actual routing behavior.
    by_domain = {domain: hashlib.sha256(json.dumps([row[0], *row[2:]], separators=(',', ':')).encode()).hexdigest()
                 for row in inputs for domain in row[1]}
    missing = hashlib.sha256(b'null').hexdigest()

    def signature(host):
        labels = host.split('.')
        for offset in range(len(labels)):
            found = by_domain.get('.'.join(labels[offset:]))
            if found is not None:
                return found
        return missing

    return signature


def initialize_route_plan(queue, previous, current, *, literal_version, read_checkpoint):
    """Migrate a proven global prefix, or carry exact unchanged host inputs.

    Called in the same transaction as the compatibility marker replacement.
    A malformed/missing existing plan cannot fast-forward from a host cursor
    pretending to be a global cursor. New host incidence dirties the plan only
    once per host, not once per article or idle polling interval.
    """
    db = queue.db
    db.execute(f'CREATE TABLE IF NOT EXISTS {TABLE}('
               'contract_version INTEGER NOT NULL,original_host TEXT NOT NULL,'
               'source_fingerprint TEXT NOT NULL,after BLOB NOT NULL,complete INTEGER NOT NULL,'
               'PRIMARY KEY(contract_version,original_host)) WITHOUT ROWID')
    db.execute(f'CREATE INDEX IF NOT EXISTS original_route_pending ON {TABLE}(contract_version,original_host) '
               'WHERE complete=0')
    db.execute('CREATE TRIGGER IF NOT EXISTS original_route_new_host AFTER INSERT ON original_url_host_metrics '
               f"BEGIN INSERT OR REPLACE INTO queue_settings VALUES('{DIRTY}','1'); END")
    raw = db.execute('SELECT value FROM queue_settings WHERE key=?', (INPUTS,)).fetchone()
    saved = read_checkpoint(queue, INPUTS, {}, lambda v: _valid_manifest(v, literal_version), manage_transaction=False)
    geometry = db.execute('SELECT value FROM queue_settings WHERE key=?', (GEOMETRY,)).fetchone()
    rows = {r['original_host']: dict(r) for r in db.execute(f'SELECT * FROM {TABLE} WHERE contract_version=?',
                                                           (literal_version,))}
    inputs = source_inputs(queue.registry)
    manifest = {'contract_version': PLAN_VERSION, 'routing_fingerprint': current['routing_fingerprint'],
                'sources': inputs}
    signature = _host_signatures(inputs)
    previous_signature = _host_signatures(saved['sources']) if saved else None
    global_prefix = (geometry is not None and geometry[0] == 'global' and saved
                     and previous.get('routing_fingerprint') == saved['routing_fingerprint'])
    # Only an unpartitioned, matching old marker proves a global keyset prefix.
    inherit = (raw is None and not rows and previous.get('routing_fingerprint') == current['routing_fingerprint'])
    prefix = previous['after'] if inherit else ''
    resets, retained, migrated = [], [], []
    hosts = [r[0] for r in db.execute('SELECT original_host FROM original_url_host_metrics '
                                    'WHERE contract_version=? AND metadata_links>0 ORDER BY original_host',
                                    (literal_version,))]
    for host in hosts:
        prior, expected = rows.get(host), signature(host)
        valid = (prior is not None and isinstance(prior['after'], bytes) and len(prior['after']) in (0, 32)
                 and type(prior['complete']) is int and prior['complete'] in (0, 1)
                 and previous_signature is not None
                 and prior['source_fingerprint'] == previous_signature(host) == expected)
        if valid:
            if global_prefix:
                after, complete = ((b'', 1) if previous['after'] == 'done'
                                   else (bytes.fromhex(previous['after']), 0))
                db.execute(f'UPDATE {TABLE} SET after=?,complete=? WHERE contract_version=? AND original_host=?',
                           (after, complete, literal_version, host))
            retained.append(host)
            continue
        after, complete = (bytes.fromhex(prefix), 0) if prefix != 'done' else (b'', 1)
        db.execute(f'INSERT OR REPLACE INTO {TABLE} VALUES (?,?,?,?,?)',
                   (literal_version, host, expected, after, complete))
        (migrated if inherit else resets).append(host)
    # Preserve the legacy partial prefix in the immutable transition evidence,
    # not in the compatibility marker once its work is partitioned by host.
    if raw is None or saved != manifest or resets:
        evidence = {'contract_version': PLAN_VERSION, 'previous_marker': previous,
                    'previous_inputs': saved, 'replacement_inputs': manifest,
                    'legacy_global_prefix_inherited': inherit, 'retained_hosts': retained,
                    'reset_hosts': resets, 'migrated_hosts': migrated, 'publisher_requests': 0,
                    'article_states_deadlines_leases_or_bodies_changed': False}
        encoded = json.dumps(evidence, sort_keys=True)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)',
                   ('original_url_route_transition_v1:' + digest, encoded))
    db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (INPUTS, json.dumps(manifest, sort_keys=True)))
    db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (DIRTY, '0'))
    # A shared unproven prefix needs one efficient global keyset pass, not
    # hundreds of small host seeks. Partition only when coverage actually
    # differs: the normal case after a source-specific eligibility change.
    scopes = db.execute(f'SELECT h.after,h.complete FROM {TABLE} h JOIN original_url_host_metrics m '
                        'ON m.contract_version=h.contract_version AND m.original_host=h.original_host '
                        'WHERE h.contract_version=? AND m.metadata_links>0', (literal_version,)).fetchall()
    shared = bool(scopes) and all(not r['complete'] and r['after'] == scopes[0]['after'] for r in scopes)
    db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (GEOMETRY, 'global' if shared else 'hosts'))
    if shared:
        return {**current, 'after': scopes[0]['after'].hex()}
    pending = any(not r['complete'] for r in scopes)
    return {**current, 'after': '' if pending else 'done'} if hosts else current


def valid_host_turn(value):
    # This is an ordering position, never proof that a host/prefix was covered.
    return bool(isinstance(value, dict) and set(value) == {'contract_version', 'after_host'}
                and type(value['contract_version']) is int and value['contract_version'] == 1
                and isinstance(value['after_host'], str) and len(value['after_host']) <= 253)


def next_host(db, literal_version, *, after_host=None):
    """An indexed host turn; a large alphabetic neighbour cannot starve new work."""
    query = (f'SELECT h.original_host,h.after FROM {TABLE} h INDEXED BY original_route_pending '
             'JOIN original_url_host_metrics m ON m.contract_version=h.contract_version '
             'AND m.original_host=h.original_host WHERE h.contract_version=? AND h.complete=0 '
             'AND m.metadata_links>0 ')
    if after_host is not None:
        selected = db.execute(query + 'AND h.original_host>? ORDER BY h.original_host LIMIT 1',
                              (literal_version, after_host)).fetchone()
        if selected is not None:
            return selected
    return db.execute(query + 'ORDER BY h.original_host LIMIT 1', (literal_version,)).fetchone()


def advance_host(db, literal_version, host, after, *, complete):
    db.execute(f'UPDATE {TABLE} SET after=?,complete=? WHERE contract_version=? AND original_host=?',
               (after, int(complete), literal_version, host))


def route_plan_progress(db, checkpoint, *, literal_version):
    """Read only the small durable host plan, never the article/proof frontier.

    URL decoding and source eligibility are separate passes. A valid decoding
    checkpoint says nothing about this plan's completion. Legacy, dirty or
    inconsistent plans remain unknown; this reader never repairs their state.
    Host scopes have unequal sizes, so their ratio cannot supply a link ETA.
    """
    result = {'contract_version': 1, 'snapshot_consistent': False, 'state': 'unavailable',
              'known_hosts': None, 'completed_hosts': None, 'pending_hosts': None,
              'host_progress_ratio': None, 'completion_proven': False,
              'conditional_eta_seconds': None,
              'progress_basis': 'completed host scopes / materialized original hosts; not link or body progress',
              'eta_basis': 'unknown: incomplete hosts have unequal, unmeasured remaining link counts',
              'publisher_requests': 0, 'body_completion_claimed': False}
    if not (isinstance(checkpoint, dict) and checkpoint.get('contract_version') == 2
            and isinstance(checkpoint.get('routing_fingerprint'), str)):
        return result
    settings = dict(db.execute('SELECT key,value FROM queue_settings WHERE key IN (?,?,?)',
                               (INPUTS, DIRTY, GEOMETRY)))
    if settings.get(DIRTY) != '0' or settings.get(GEOMETRY) not in {'global', 'hosts'}:
        return result
    try:
        manifest = json.loads(settings.get(INPUTS, 'null'))
    except (ValueError, TypeError, UnicodeError):
        return result
    if (not _valid_manifest(manifest, literal_version)
            or manifest['routing_fingerprint'] != checkpoint['routing_fingerprint']
            or db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone() is None):
        return result
    signature = _host_signatures(manifest['sources'])
    rows = db.execute(f'SELECT m.original_host,h.source_fingerprint,h.complete,'
                      f'typeof(h.after) AS cursor_type,length(h.after) AS cursor_bytes '
                      f'FROM original_url_host_metrics m LEFT JOIN {TABLE} h '
                      'ON h.contract_version=m.contract_version AND h.original_host=m.original_host '
                      'WHERE m.contract_version=? AND m.metadata_links>0', (literal_version,)).fetchall()
    if any(r['source_fingerprint'] != signature(r['original_host'])
           or type(r['complete']) is not int or r['complete'] not in (0, 1)
           or r['cursor_type'] != 'blob' or r['cursor_bytes'] not in (0, 32) for r in rows):
        return result
    total, completed = len(rows), sum(r['complete'] for r in rows)
    done = checkpoint.get('after') == 'done'
    if done != (completed == total):
        return result
    result.update(snapshot_consistent=True, state=('empty' if not total else 'complete') if done else 'pending',
                  known_hosts=total, completed_hosts=completed, pending_hosts=total - completed,
                  host_progress_ratio=completed / total if total else None, completion_proven=done)
    return result
