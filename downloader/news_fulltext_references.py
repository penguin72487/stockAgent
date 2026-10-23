"""Durable metadata links to reviewed bodies; never another body capture.

The canonical NewsQueue owner installs explicit reviewed references and recovers
its immutable commit journal. Aliases retain their original heads, errors and
HTTP clocks. Heartbeats query only the sparse reference ledger, not the corpus.
"""
from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re

from downloader.artifact_io import atomic_write_bytes, sha256_file
from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS


CONTRACT_VERSION = 1
_KIND = 'reviewed_publisher_body_references_v1'
_BASE = 'publisher_url_references'
_SHA = re.compile(r'[0-9a-f]{64}')


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _json(path, limit):
    path = Path(path)
    _require(path.stat().st_size <= limit, 'reference evidence exceeds bound')
    with path.open('rb') as stream:
        payload = stream.read(limit + 1)
    _require(len(payload) <= limit, 'reference evidence grew beyond bound')
    value = json.loads(payload)
    _require(isinstance(value, dict), 'reference evidence object required')
    return value, payload, hashlib.sha256(payload).hexdigest()


def _clock(value):
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.utcoffset() is None or parsed > datetime.now(UTC):
            raise ValueError
        return parsed
    except (TypeError, ValueError):
        raise ValueError('invalid reference evidence clock') from None


def _inside(root, relative):
    _require(isinstance(relative, str) and not Path(relative).is_absolute(), 'relative reference path required')
    path = (root / relative).resolve()
    _require(path.is_relative_to(root.resolve()), 'reference path outside canonical root')
    return path


def _immutable(root, relative, payload):
    path = _inside(root, relative)
    if path.exists():
        _require(sha256_file(path) == hashlib.sha256(payload).hexdigest(), 'immutable reference evidence changed')
    else:
        atomic_write_bytes(path, payload, durable=True, mode=0o600)


def _guard(head):
    return {field: head[field] for field in RETAINED_SUPPLEMENT_HEAD_FIELDS}


def _source(registry, receipt):
    source = registry.sources.get(receipt.get('provider'))
    return source if (source and source.provider == 'pocket' and source.allowed and source.known_url_only
        and source.authorization.get('kind') == 'personal_research'
        and receipt.get('source_data_fingerprint') == source.data_fingerprint
        and receipt.get('authorization_fingerprint') == source.authorization_fingerprint) else None


def _live_pair(db, item, provider):
    alias = db.execute('SELECT * FROM articles WHERE id=?', (bytes.fromhex(item['alias_id']),)).fetchone()
    target = db.execute('SELECT * FROM articles WHERE id=?', (bytes.fromhex(item['target_id']),)).fetchone()
    guard = item['alias_head_guard']
    return bool(alias and target and alias['provider'] == target['provider'] == provider
        and _guard(alias) == guard and alias['state'] == alias['error_code'] == 'unsupported_article_path'
        and not any(alias[key] for key in ('body_sha256', 'parquet_path', 'captured_at_utc', 'lease_owner', 'lease_until'))
        and alias['due'] <= datetime.now(UTC).timestamp()
        and _clock(alias['last_checked_at_utc']) <= _clock(item['reference']['redirect_observed_at_utc'])
        and target['url'] == item['reference']['existing_requested_url']
        and target['body_sha256'] == item['reference']['existing_body_sha256']
        and target['parquet_path'] == item['reference']['existing_parquet_path']
        and target['captured_at_utc'] == item['reference']['existing_body_captured_at_utc']
        and target['authorization_fingerprint'] == item['target_authorization_fingerprint'])


def initialize(queue):
    queue.db.executescript('''
        CREATE TABLE IF NOT EXISTS publisher_reference_commits(
            approval_sha256 TEXT PRIMARY KEY,receipt_path TEXT NOT NULL,receipt_sha256 TEXT NOT NULL,
            outcome TEXT NOT NULL,evidence_valid INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS publisher_body_references(
            alias_id BLOB PRIMARY KEY,provider TEXT NOT NULL,source_data_fingerprint TEXT NOT NULL,
            authorization_fingerprint TEXT NOT NULL,approval_sha256 TEXT NOT NULL,item_json TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS publisher_reference_recovery_errors(
            receipt_path TEXT PRIMARY KEY,error_code TEXT NOT NULL,observed_at_utc TEXT NOT NULL
        ) WITHOUT ROWID;
    ''')
    queue.db.commit()
    directory = queue.root / _BASE / 'commits'
    for path in sorted(directory.glob('*.json')):
        relative = path.relative_to(queue.root).as_posix()
        try:
            receipt, _, digest = _json(path, 1 << 20)
            _require(path.stem == digest, 'reference commit filename differs')
            _apply(queue, receipt, relative, digest, recovery=True)
        except (OSError, ValueError, KeyError, TypeError):
            # One unavailable/corrupt metadata proof must not halt independent
            # publishers. Retain every original, deactivate only this proof and
            # expose its inactive references until the exact evidence returns.
            with queue.db:
                queue.db.execute('UPDATE publisher_reference_commits SET evidence_valid=0 WHERE receipt_path=?', (relative,))
                queue.db.execute('INSERT OR REPLACE INTO publisher_reference_recovery_errors VALUES (?,?,?)',
                    (relative, 'reference_evidence_unavailable_or_changed', datetime.now(UTC).isoformat()))
            queue.metrics.count('system', 'publisher_reference_recovery_errors')
        else:
            with queue.db:
                queue.db.execute('DELETE FROM publisher_reference_recovery_errors WHERE receipt_path=?', (relative,))
    # A missing journal is not an empty/complete reference collection either.
    for row in queue.db.execute('SELECT receipt_path FROM publisher_reference_commits').fetchall():
        if not _inside(queue.root, row['receipt_path']).is_file():
            with queue.db:
                queue.db.execute('UPDATE publisher_reference_commits SET evidence_valid=0 WHERE receipt_path=?', (row['receipt_path'],))


def _receipt_evidence(queue, receipt):
    _require(type(receipt.get('contract_version')) is int and receipt['contract_version'] == CONTRACT_VERSION
             and receipt.get('scope_kind') == _KIND and receipt.get('publish') is False
             and receipt.get('new_article_bodies') == 0 and receipt.get('remote_training_export') is False
             and isinstance(receipt.get('entries'), list) and 1 <= len(receipt['entries']) <= 16,
             'invalid reference commit scope')
    _clock(receipt['recorded_at_utc'])
    _require(isinstance(receipt.get('approval_sha256'), str) and _SHA.fullmatch(receipt['approval_sha256']),
             'invalid reference approval identity')
    proofs = {}
    for proof in receipt['immutable_evidence']:
        path = _inside(queue.root, proof['path'])
        _require(path.stat().st_size <= 8 << 20 and sha256_file(path) == proof['sha256'],
                 'reference commit evidence unavailable or changed')
        _require(proof.get('kind') not in proofs, 'repeated reference proof kind')
        proofs[proof['kind']] = _json(path, 8 << 20)[0]
    _require(set(proofs) == {'approval', 'audit', 'acceptance', 'observations'}, 'reference proof bundle incomplete')
    approval, audit, acceptance = (proofs[key] for key in ('approval', 'audit', 'acceptance'))
    evidence = {proof['kind']: proof['sha256'] for proof in receipt['immutable_evidence']}
    _require(evidence['approval'] == receipt['approval_sha256']
             and all(approval.get(key) == receipt.get(key) for key in (
                 'contract_version', 'scope_kind', 'provider', 'source_data_fingerprint',
                 'authorization_fingerprint', 'entries'))
             and approval.get('audit_sha256') == evidence['audit']
             and approval.get('acceptance_sha256') == evidence['acceptance']
             and acceptance.get('reference_report_sha256') == evidence['audit']
             and audit.get('observation_report_sha256') == evidence['observations']
             and audit.get('physical_parts') == receipt['physical_parts'], 'reference commit does not match its approved proofs')
    # Integrity is separate from the mutable target head. A stale/deleted target
    # must not activate a reference, and a corrupt source cannot silently pass.
    for part in receipt['physical_parts']:
        path, rp = _inside(queue.root, part['parquet_path']), _inside(queue.root, part['receipt_path'])
        _require(path.stat().st_size <= 64 << 20 and sha256_file(path) == part['parquet_sha256']
                 and sha256_file(rp) == part['receipt_sha256'], 'reference retained body evidence changed')


def _apply(queue, receipt, relative, digest, *, recovery=False):
    _receipt_evidence(queue, receipt)
    prior = queue.db.execute('SELECT * FROM publisher_reference_commits WHERE approval_sha256=?',
                             (receipt['approval_sha256'],)).fetchone()
    if prior:
        _require(prior['receipt_path'] == relative and prior['receipt_sha256'] == digest,
                 'reference approval has a different commit')
        with queue.db:
            queue.db.execute('UPDATE publisher_reference_commits SET evidence_valid=1 WHERE approval_sha256=?',
                             (receipt['approval_sha256'],))
            if prior['outcome'] == 'installed':
                _index_references(queue, receipt)
        return {'installed_references': 0, 'already_recorded': True, 'outcome': prior['outcome']}
    valid = bool(_source(queue.registry, receipt) and all(
        _live_pair(queue.db, item, receipt['provider']) and not queue.db.execute(
            'SELECT 1 FROM publisher_body_references WHERE alias_id=? AND approval_sha256!=?',
            (bytes.fromhex(item['alias_id']), receipt['approval_sha256'])).fetchone()
        for item in receipt['entries']))
    _require(recovery or valid, 'reference source or frontier changed before commit')
    outcome = 'installed' if valid else 'superseded_without_head_mutation'
    with queue.db:
        if valid:
            _index_references(queue, receipt)
        queue.db.execute('INSERT INTO publisher_reference_commits VALUES (?,?,?,?,1)',
            (receipt['approval_sha256'], relative, digest, outcome))
    return {'installed_references': len(receipt['entries']) if valid else 0,
            'already_recorded': False, 'outcome': outcome}


def _index_references(queue, receipt):
    """Reconstruct only this approval's derived index, never a newer approval."""
    for item in receipt['entries']:
        queue.db.execute('INSERT INTO publisher_body_references VALUES (?,?,?,?,?,?) '
            'ON CONFLICT(alias_id) DO UPDATE SET provider=excluded.provider,'
            'source_data_fingerprint=excluded.source_data_fingerprint,'
            'authorization_fingerprint=excluded.authorization_fingerprint,item_json=excluded.item_json '
            'WHERE publisher_body_references.approval_sha256=excluded.approval_sha256',
            (bytes.fromhex(item['alias_id']), receipt['provider'], receipt['source_data_fingerprint'],
             receipt['authorization_fingerprint'], receipt['approval_sha256'],
             json.dumps(item, ensure_ascii=False, sort_keys=True)))


def install_reviewed_references(queue, urls, approval_path):
    """Use a pinned completed review; no repeated body decode or source GET.

    The explicit approval binds the prior audit and its independent acceptance,
    current source identity, literal aliases and exact current head guards. All
    immutable evidence precedes the commit journal; SQL is a recoverable index.
    """
    _require(isinstance(urls, (list, tuple)) and 1 <= len(urls) <= 16 and len(set(urls)) == len(urls),
             'one to sixteen distinct reviewed references required')
    approval_path = Path(approval_path).resolve()
    approval, approval_bytes, approval_sha = _json(approval_path, 65536)
    _require(type(approval.get('contract_version')) is int and approval['contract_version'] == CONTRACT_VERSION
             and approval.get('scope_kind') == _KIND and approval.get('publish') is False
             and approval.get('remote_training_export') is False and _source(queue.registry, approval)
             and isinstance(approval.get('entries'), list)
             and [entry['alias_url'] for entry in approval['entries']] == sorted(urls), 'unreviewed reference approval')
    approved = _clock(approval['approved_at_utc'])
    audit, audit_bytes, audit_sha = _json(approval_path.parent / approval['audit_path'], 1 << 20)
    acceptance, acceptance_bytes, acceptance_sha = _json(approval_path.parent / approval['acceptance_path'], 1 << 20)
    _require(audit_sha == approval['audit_sha256'] and acceptance_sha == approval['acceptance_sha256']
             and audit.get('contract_version') == 1
             and audit.get('scope_kind') == 'observed_redirect_existing_body_reference_audit'
             and audit.get('accepted') is True and audit.get('formal_queue_alias_admission') is False
             and audit.get('raw_alias_response_retained') is False
             and audit.get('source_requests') == audit.get('queue_writes') == audit.get('new_article_bodies') == 0
             and audit.get('provider') == approval['provider']
             and audit.get('source_data_fingerprint') == approval['source_data_fingerprint']
             and audit.get('authorization_fingerprint') == approval['authorization_fingerprint']
             and _clock(audit['verified_at_utc']) <= approved
             and acceptance.get('accepted') is True and acceptance.get('reference_report_sha256') == audit_sha
             and acceptance.get('head_writes') == acceptance.get('new_body_records') == 0
             and acceptance.get('formal_queue_alias_admission') is False
             and _clock(acceptance['at_utc']) <= approved, 'reference completed review differs')
    links = {link['alias_url']: link for link in audit['verified_alias_references']}
    _require(len(links) == len(audit['verified_alias_references']) == len(urls)
             and set(links) == set(urls)
             and acceptance.get('verified_alias_references') == len(urls), 'reference selection differs from completed review')
    observation, observation_bytes, observation_sha = _json(Path(audit['observation_report']), 8 << 20)
    _require(observation_sha == audit['observation_report_sha256'] and observation.get('accepted') is True,
             'reference original observations changed')
    for entry in approval['entries']:
        item = entry['reference']
        _require(item == links[entry['alias_url']] and item['alias_body_admitted'] is False
                 and item['native_body_replayed_both_engines'] is True
                 and entry['alias_head_guard']['url'] == entry['alias_url']
                 and entry['target_authorization_fingerprint'] == approval['authorization_fingerprint']
                 and audit['completed_body_heads'].get(item['existing_requested_url']) == item['existing_body_sha256']
                 and _clock(item['redirect_observed_at_utc']) <= _clock(audit['verified_at_utc']), 'unreviewed reference entry')
    receipt = {key: approval[key] for key in ('provider', 'source_data_fingerprint', 'authorization_fingerprint')}
    receipt.update(contract_version=CONTRACT_VERSION, scope_kind=_KIND, entries=approval['entries'],
        approval_sha256=approval_sha, recorded_at_utc=datetime.now(UTC).isoformat(),
        physical_parts=audit['physical_parts'], publish=False, remote_training_export=False,
        new_article_bodies=0, immutable_evidence=[])
    for tag, payload, digest in (('approval', approval_bytes, approval_sha), ('audit', audit_bytes, audit_sha),
            ('acceptance', acceptance_bytes, acceptance_sha), ('observations', observation_bytes, observation_sha)):
        relative = f'{_BASE}/evidence/{digest}.{tag}.json'
        _immutable(queue.root, relative, payload)
        receipt['immutable_evidence'].append({'kind': tag, 'path': relative, 'sha256': digest})
    # An identical approved command is idempotent, even after its original
    # source artifact directory disappears. Recovery keeps the canonical copy.
    previous = queue.db.execute('SELECT * FROM publisher_reference_commits WHERE approval_sha256=?', (approval_sha,)).fetchone()
    if previous:
        saved, _, digest = _json(_inside(queue.root, previous['receipt_path']), 1 << 20)
        _require(digest == previous['receipt_sha256'], 'recorded reference commit changed')
        return _apply(queue, saved, previous['receipt_path'], digest)
    _receipt_evidence(queue, receipt)
    _require(_source(queue.registry, receipt) and all(_live_pair(queue.db, entry, receipt['provider'])
             and not queue.db.execute('SELECT 1 FROM publisher_body_references WHERE alias_id=? AND approval_sha256!=?',
                 (bytes.fromhex(entry['alias_id']), approval_sha)).fetchone()
             for entry in receipt['entries']), 'reference source or frontier changed before journal')
    payload = (json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()
    digest = hashlib.sha256(payload).hexdigest()
    relative = f'{_BASE}/commits/{digest}.json'
    _immutable(queue.root, relative, payload)
    return _apply(queue, receipt, relative, digest)


def progress(db, registry):
    """Sparse, current-head projection only; no payload hashes per heartbeat."""
    result = {}
    for row in db.execute('SELECT r.*,c.evidence_valid,c.outcome FROM publisher_body_references r '
                         'LEFT JOIN publisher_reference_commits c ON c.approval_sha256=r.approval_sha256'):
        source = _source(registry, dict(row))
        value = result.setdefault(row['provider'], {'contract_version': CONTRACT_VERSION,
            'registered_urls': 0, 'verified_reference_urls': 0, 'inactive_reference_urls': 0,
            'new_article_bodies': 0, 'raw_alias_response_retained': False, 'all_history_complete': False})
        value['registered_urls'] += 1
        try:
            item = json.loads(row['item_json'])
            active = bool(row['evidence_valid'] == 1 and row['outcome'] == 'installed'
                          and source and _live_pair(db, item, row['provider']))
        except (ValueError, KeyError, TypeError):
            active = False  # The immutable journal can rebuild this local index.
        value['verified_reference_urls' if active else 'inactive_reference_urls'] += 1
    return result
