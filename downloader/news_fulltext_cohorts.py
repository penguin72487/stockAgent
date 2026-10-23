"""Frozen finite capture scopes, owned by the existing NewsQueue transaction.

Membership is durable SQLite data; immutable private receipts bind its exact
URL set. No second HTTP owner, per-site process or periodic publisher polling.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import time
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file


READY_STATES = ("pending", "retry", "retry_cooldown", "inflight")
NONBODY_SCOPE_OUTCOME_CONTRACT_VERSION = 1
NONBODY_SCOPE_PROGRESS_QUERY_VERSION = 2
RETAINED_SUPPLEMENT_REPLAY_VERSION = 1
RETAINED_SUPPLEMENT_HEAD_FIELDS = (
    "url", "provider", "state", "due", "attempts", "error_code", "lease_owner", "lease_until",
    "body_sha256", "parquet_path", "captured_at_utc", "last_checked_at_utc", "authorization_fingerprint",
    "title", "metadata_date_raw",
)

# The outcome ledger is sparse; starting at the articles/provider index can
# visit millions of unrelated URLs on every heartbeat. CROSS JOIN fixes only
# the join order, not the source/body/clock/membership acceptance predicates.
_NONBODY_OUTCOME_PROGRESS_SQL = (
    "SELECT o.outcome,count(*) FROM finite_source_outcomes o CROSS JOIN articles a "
    "WHERE a.id=o.article_id AND o.provider=? AND a.provider=o.provider AND a.body_sha256 IS NULL "
    "AND a.state=o.outcome AND a.error_code=o.outcome AND a.last_checked_at_utc=o.observed_at_utc "
    "AND o.source_data_fingerprint=(SELECT value FROM queue_settings WHERE key=?) "
    "AND NOT EXISTS (SELECT 1 FROM finite_cohort_members WHERE article_id=a.id) "
    "AND NOT EXISTS (SELECT 1 FROM finite_supplement_members WHERE article_id=a.id) GROUP BY o.outcome"
)


def reviewed_nonbody_progress(db, provider):
    return Counter(dict(db.execute(_NONBODY_OUTCOME_PROGRESS_SQL,
                                  (provider, 'source_capture_fingerprint:' + provider))))


def review_nonbody_probes(registry, report, urls, *, diagnostic_root=None):
    """Verify explicit absent/non-article observations, never grant body scope.

    A direct 404/410 is transport evidence, not retained body integrity. A
    public out-of-namespace 200 also needs its original private response proof.
    Refusals, redirects, unreviewed authors and parser errors are not absences.
    """
    from downloader.news_fulltext_policy import normalize_url
    selected = sorted(set(normalize_url(url) for url in urls))
    probes = {p['url']: p for p in report['probes']}
    if not 1 <= len(selected) <= 16 or len(probes) != len(report['probes']):
        raise ValueError('bounded unique source outcome review required')
    reviewed = []
    for url in selected:
        source, probe = registry.require(url), probes.get(url, {})
        if (not source.allowed or source.acquisition_mode != 'finite' or probe.get('provider') != source.provider
                or probe.get('error') or probe.get('sent') is False or probe.get('final_url') != url
                or type(probe.get('status')) is not int or type(probe.get('bytes')) is not int
                or not 0 <= probe['bytes'] <= 10 << 20
                or not re.fullmatch(r'[0-9a-f]{64}', probe.get('response_sha256', ''))):
            raise ValueError('unreviewed or indirect source outcome')
        traces = [r for r in report.get('http_path_by_provider', {}).get(source.provider, [])
                  if r.get('requested_url') == url]
        if (len(traces) != 1 or traces[0].get('status') != probe['status']
                or traces[0].get('body_read') is not True
                or traces[0].get('response_sha256') != probe['response_sha256']
                or traces[0].get('response_bytes') != probe.get('bytes')):
            raise ValueError('source outcome lacks matching canonical transport trace')
        if probe['status'] in {404, 410}:
            outcome, basis, raw_path = 'source_not_found', 'direct_canonical_http_absence_observation', None
        elif (probe['status'] == 200 and source.article_path_pattern
                and not re.search(source.article_path_pattern, urlsplit(url).path)
                and probe.get('extraction_error') not in {
                    'paywall_or_login', 'access_challenge', 'article_rights_or_scope_excluded',
                    'article_license_not_verified', 'article_rights_not_verified'}):
            raw_path = Path(probe.get('private_diagnostic_path', '')).resolve()
            if (diagnostic_root is None or not raw_path.is_relative_to(Path(diagnostic_root).resolve())
                    or raw_path.stat().st_size > 10 << 20):
                raise ValueError('non-article original outside bounded private diagnostic scope')
            if raw_path.with_suffix('.receipt.json').stat().st_size > 65536:
                raise ValueError('unbounded non-article original receipt')
            raw_payload, receipt_payload = raw_path.read_bytes(), raw_path.with_suffix('.receipt.json').read_bytes()
            if len(raw_payload) > 10 << 20 or len(receipt_payload) > 65536:
                raise ValueError('unbounded non-article original proof')
            receipt = json.loads(receipt_payload)
            if (hashlib.sha256(raw_payload).hexdigest() != receipt.get('sha256') or receipt.get('diagnostic_only') is not True
                    or receipt.get('accepted_as_article') is not False or receipt.get('publish') is not False):
                raise ValueError('non-article original diagnostic receipt invalid')
            metadata = pq.ParquetFile(pa.BufferReader(raw_payload)).metadata
            if (metadata.num_rows != 1 or metadata.num_row_groups > 1
                    or sum(metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups)) > 16 << 20):
                raise ValueError('unbounded non-article original logical grain')
            rows = pq.read_table(pa.BufferReader(raw_payload)).to_pylist()
            if (len(rows) != 1 or rows[0].get('request_url') != url or rows[0].get('final_url') != url
                    or rows[0].get('provider') != source.provider
                    or hashlib.sha256(rows[0]['raw_response_body']).hexdigest() != probe['response_sha256']
                    or rows[0].get('response_sha256') != probe['response_sha256']
                    or len(rows[0]['raw_response_body']) != probe['bytes']
                    or rows[0].get('diagnostic_only') is not True
                    or rows[0].get('accepted_as_article') is not False or rows[0].get('publish') is not False):
                raise ValueError('non-article original source identity invalid')
            outcome, basis = 'unsupported_article_path', 'registered_article_namespace_exclusion_with_public_original'
        else:
            raise ValueError('source observation does not prove an absent or non-article outcome')
        item = {'url': url, 'provider': source.provider, 'outcome': outcome, 'basis': basis,
                         'status': probe['status'], 'response_sha256': probe['response_sha256'],
                         'source_data_fingerprint': source.data_fingerprint,
                         'private_diagnostic_path': str(raw_path) if raw_path else None}
        if raw_path:
            item.update(private_source_sha256=hashlib.sha256(raw_payload).hexdigest(),
                        private_source_receipt_sha256=hashlib.sha256(receipt_payload).hexdigest())
        reviewed.append(item)
    return reviewed


def _persist_immutable_proof(root, relative, payload):
    target = root / relative
    if target.exists():
        if sha256_file(target) != hashlib.sha256(payload).hexdigest():
            raise ValueError('immutable source outcome proof conflicts with existing evidence')
    else:
        atomic_write_bytes(target, payload, durable=True)


def record_reviewed_source_outcomes(queue, urls, diagnostic_report):
    """Explicit, atomic correction of unresolved finite metadata only; zero HTTP.

    Retain immutable source/approval receipts before the SQLite transaction.
    Saved heads, leases, future deadlines, existing failures and both original
    and supplementary membership are protected. Repeating an approval cannot
    turn a source outcome into a body or alter metadata publication dates.
    """
    from downloader.news_fulltext_queue import utc_now
    diagnostic_report = Path(diagnostic_report)
    if diagnostic_report.stat().st_size > 8 << 20:
        raise ValueError('unbounded source outcome report')
    payload = diagnostic_report.read_bytes()
    if len(payload) > 8 << 20:
        raise ValueError('unbounded source outcome report')
    report = json.loads(payload)
    observed = datetime.fromisoformat(report['observed_at_utc'])
    if observed.utcoffset() is None or not -5 <= (datetime.now(timezone.utc) - observed).total_seconds() <= 3600:
        raise ValueError('source outcome mutation needs a recent aware probe snapshot')
    selected = review_nonbody_probes(queue.registry, report, urls,
                                   diagnostic_root=diagnostic_report.resolve().parent / 'private-source-diagnostics')
    for item in selected:
        row = queue.db.execute('SELECT * FROM articles WHERE url=?', (item['url'],)).fetchone()
        if (row is None or row['provider'] != item['provider'] or row['state'] != 'waiting_cohort'
                or row['body_sha256'] is not None or row['lease_owner'] is not None or row['lease_until'] is not None
                or row['due'] > datetime.now(timezone.utc).timestamp()
                or queue.db.execute('SELECT 1 FROM finite_cohort_members WHERE article_id=? UNION ALL '
                                    'SELECT 1 FROM finite_supplement_members WHERE article_id=?', (row['id'], row['id'])).fetchone()):
            raise ValueError('source outcome selection is not unresolved unleased finite metadata')
        item['article_id'] = row['id'].hex()
        if item['private_diagnostic_path']:
            raw = Path(item.pop('private_diagnostic_path')).resolve()
            expected = diagnostic_report.resolve().parent / 'private-source-diagnostics'
            if not raw.is_relative_to(expected.resolve()) or raw.stat().st_size > 10 << 20:
                raise ValueError('non-article original outside bounded private diagnostic scope')
            raw_payload, raw_receipt = raw.read_bytes(), raw.with_suffix('.receipt.json').read_bytes()
            if (hashlib.sha256(raw_payload).hexdigest() != item['private_source_sha256']
                    or hashlib.sha256(raw_receipt).hexdigest() != item['private_source_receipt_sha256']):
                raise ValueError('non-article original proof changed during review')
            relative = 'capture_cohorts/supplement_sources/' + item['private_source_sha256'] + '.parquet'
            _persist_immutable_proof(queue.root, relative, raw_payload)
            _persist_immutable_proof(queue.root, str(Path(relative).with_suffix('.receipt.json')), raw_receipt)
            item.update(private_source_path=relative, private_source_sha256=hashlib.sha256(raw_payload).hexdigest(),
                        private_source_receipt_sha256=hashlib.sha256(raw_receipt).hexdigest())
    report_sha = hashlib.sha256(payload).hexdigest()
    if sha256_file(diagnostic_report) != report_sha:
        raise ValueError('source outcome report changed during review')
    report_relative = 'capture_cohorts/source_outcomes/' + report_sha + '.probe.json'
    _persist_immutable_proof(queue.root, report_relative, payload)
    receipt = {'contract_version': NONBODY_SCOPE_OUTCOME_CONTRACT_VERSION,
               'scope_kind': 'explicit_verified_nonbody_source_outcomes', 'outcomes': selected,
               'probe_path': report_relative, 'probe_sha256': report_sha,
               'observed_at_utc': report['observed_at_utc'], 'recorded_at_utc': utc_now(),
               'original_and_supplement_membership_unchanged': True, 'accepted_article_bodies': 0,
               'all_publisher_history_complete': False, 'publish': False}
    receipt_payload = (json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()
    receipt_sha = hashlib.sha256(receipt_payload).hexdigest()
    relative = 'capture_cohorts/source_outcomes/' + receipt_sha + '.json'
    _persist_immutable_proof(queue.root, relative, receipt_payload)
    with queue.db:
        for item in selected:
            key = bytes.fromhex(item['article_id'])
            changed = queue.db.execute("UPDATE articles SET state=?,error_code=?,last_checked_at_utc=? "
                                       "WHERE id=? AND state='waiting_cohort' AND body_sha256 IS NULL "
                                       "AND lease_owner IS NULL AND lease_until IS NULL AND due<=? "
                                       "AND NOT EXISTS (SELECT 1 FROM finite_cohort_members WHERE article_id=articles.id) "
                                       "AND NOT EXISTS (SELECT 1 FROM finite_supplement_members WHERE article_id=articles.id)",
                                       (item['outcome'], item['outcome'], report['observed_at_utc'], key,
                                        datetime.now(timezone.utc).timestamp())).rowcount
            if changed != 1:
                raise ValueError('source outcome frontier changed before commit')
            queue.db.execute('INSERT INTO finite_source_outcomes VALUES (?,?,?,?,?,?,?,?)',
                             (key, item['provider'], item['outcome'], report['observed_at_utc'], receipt['recorded_at_utc'],
                              item['source_data_fingerprint'], relative, receipt_sha))
    return {'contract_version': NONBODY_SCOPE_OUTCOME_CONTRACT_VERSION, 'outcomes': selected,
            'receipt_path': relative, 'receipt_sha256': receipt_sha, 'http_requests': 0,
            'accepted_article_bodies': 0, 'scope_extended': False}


def membership_digest(db, provider: str) -> tuple[int, str]:
    digest, count = hashlib.sha256(), 0
    for (url,) in db.execute("SELECT url FROM finite_cohort_members WHERE provider=? ORDER BY url", (provider,)):
        digest.update(url.encode("utf-8") + b"\n")
        count += 1
    return count, digest.hexdigest()


def sync_cohorts(queue, *, freeze_new=True):
    """Freeze once after metadata arrives; restarts preserve the exact cohort.

    A pre-commit crash leaves an unreferenced content-addressed receipt, not
    accepted membership. A post-commit crash resumes the same transaction's set.
    Validation is once per owner, with a streamed hash rather than an URL cache.
    """
    if type(freeze_new) is not bool:
        raise ValueError('finite_cohort_freeze_switch_must_be_boolean')
    from downloader.news_fulltext_queue import source_path, utc_now
    if not getattr(queue, "_validated_finite_supplements", False):
        validate_supplements(queue.root, queue.db)
        queue._validated_finite_supplements = True
    validated = getattr(queue, "_validated_finite_cohorts", set())
    for source in queue.registry.sources.values():
        if not source.allowed:
            continue
        if source.acquisition_mode != "finite":
            # Explicit opt-in to existing continuous acquisition releases only
            # growth waiting on scope; no source refusal or retry is reset.
            if source.provider not in validated:
                with queue.db:
                    queue.db.execute("UPDATE articles SET state='pending' WHERE provider=? AND state='waiting_cohort'",
                                     (source.provider,))
                validated.add(source.provider)
            continue
        if source.provider in validated:
            continue
        header = queue.db.execute("SELECT * FROM finite_cohorts WHERE provider=?", (source.provider,)).fetchone()
        if header:
            receipt_path = source_path(queue.root, header["receipt_path"])
            receipt = json.loads(receipt_path.read_text())
            count, digest = membership_digest(queue.db, source.provider)
            if (sha256_file(receipt_path) != header["receipt_sha256"]
                    or receipt.get("contract_version") != 1 or receipt.get("provider") != source.provider
                    or count != header["urls"] or count != receipt.get("urls")
                    or digest != header["urls_sha256"] or digest != receipt.get("urls_sha256")):
                raise ValueError("finite news cohort receipt/membership mismatch: " + source.provider)
            if source.finite_article_urls:
                expected = hashlib.sha256(''.join(url+'\n' for url in source.finite_article_urls).encode()).hexdigest()
                if count != len(source.finite_article_urls) or digest != expected:
                    raise ValueError('explicit finite article scope differs from immutable cohort')
        else:
            if not freeze_new:
                # An explicit enqueue/fetch still has its complete selected
                # URL batch to admit. Validate existing immutable cohorts now,
                # but do not freeze the few old rows before that batch arrives.
                continue
            with queue.db:
                if source.finite_article_urls:
                    placeholders = ','.join('?' for _ in source.finite_article_urls)
                    selection = 'provider=? AND url IN ('+placeholders+')'
                    binds = (source.provider, *source.finite_article_urls)
                    found = queue.db.execute('SELECT count(*) FROM articles WHERE '+selection, binds).fetchone()[0]
                    if found != len(source.finite_article_urls):
                        continue  # Never freeze an incomplete explicit selection.
                    queue.db.execute('INSERT INTO finite_cohort_members SELECT provider,id,url FROM articles WHERE '+selection, binds)
                else:
                    from downloader.news_fulltext_urls import seed_unfrozen_finite_originals
                    if not seed_unfrozen_finite_originals(queue, source):
                        continue  # Never freeze a partial large-scope routing prefix.
                    queue.db.execute("INSERT INTO finite_cohort_members SELECT provider,id,url FROM articles WHERE provider=?",
                                     (source.provider,))
                count, digest = membership_digest(queue.db, source.provider)
                if not count:
                    continue  # Do not freeze an empty pre-ingest universe.
                created_at = utc_now()
                relative = "capture_cohorts/" + source.provider + "-" + digest + ".json"
                receipt = {"contract_version": 1, "provider": source.provider, "urls": count,
                           "urls_sha256": digest, "created_at_utc": created_at,
                           "digest_basis": "UTF-8 normalized URLs sorted by SQLite BINARY, one LF each",
                           "membership_table": "finite_cohort_members", "queue_owner_id": queue.owner,
                           "source_data_fingerprint_at_freeze": source.data_fingerprint,
                           "all_publisher_history_complete": False, "publish": False}
                target = queue.root / relative
                target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                atomic_write_json(target, receipt)
                queue.db.execute("INSERT INTO finite_cohorts VALUES (?,?,?,?,?,?)",
                                 (source.provider, count, digest, created_at, relative, sha256_file(target)))
        with queue.db:
            queue.db.execute("UPDATE articles SET state='waiting_cohort' WHERE provider=? AND state IN "
                             "('pending','retry','retry_cooldown') AND NOT EXISTS "
                             "(SELECT 1 FROM finite_cohort_members m WHERE m.provider=? AND m.article_id=articles.id) "
                             "AND NOT (? AND EXISTS (SELECT 1 FROM finite_supplement_members m WHERE m.provider=? AND m.article_id=articles.id))",
                             (source.provider, source.provider, source.allow_reviewed_supplements, source.provider))
            if source.allow_reviewed_supplements:
                queue.db.execute("UPDATE articles SET state='pending' WHERE provider=? AND state='waiting_cohort' "
                                 "AND EXISTS (SELECT 1 FROM finite_supplement_members m WHERE m.provider=? AND m.article_id=articles.id)",
                                 (source.provider, source.provider))
        validated.add(source.provider)
    queue._validated_finite_cohorts = validated


def _validated_supplement_receipt(root, db, header):
    """Validate one immutable scope, not the entire supplement lake per URL."""
    from downloader.news_fulltext_queue import source_path
    path = source_path(root, header["receipt_path"])
    if path.stat().st_size > 8 << 20:
        raise ValueError("unbounded finite supplement receipt")
    payload = path.read_bytes()
    receipt = json.loads(payload)
    members = [r[0] for r in db.execute("SELECT url FROM finite_supplement_members WHERE provider=? AND scope_sha256=? ORDER BY url",
                                       (header["provider"], header["scope_sha256"]))]
    digest = hashlib.sha256("".join(url + "\n" for url in members).encode()).hexdigest()
    if (hashlib.sha256(payload).hexdigest() != header["receipt_sha256"] or receipt.get("contract_version") != 1
            or receipt.get("scope_kind") != "explicit_diagnostic_verified_supplement"
            or receipt.get("provider") != header["provider"] or receipt.get("urls") != members
            or digest != header["scope_sha256"] or len(members) != header["urls"]
            or receipt.get("urls_sha256") != digest or receipt.get("original_cohort_unchanged") is not True
            or receipt.get("publish") is not False
            or [e.get("url") for e in receipt.get("source_evidence", [])] != members):
        raise ValueError("finite supplement receipt or membership mismatch")
    for evidence in receipt["source_evidence"]:
        if sha256_file(source_path(root, evidence["private_source_path"])) != evidence["private_source_sha256"]:
            raise ValueError("finite supplement immutable source digest mismatch")
    return receipt


def validate_supplements(root, db):
    """Immutable additional scopes; original frozen membership stays unchanged."""
    scopes = []
    for header in db.execute("SELECT * FROM finite_capture_supplements ORDER BY provider,scope_sha256"):
        receipt = _validated_supplement_receipt(root, db, header)
        scopes.append({"provider": header["provider"], "urls": len(receipt["urls"]), "scope_sha256": header["scope_sha256"],
                       "receipt_path": header["receipt_path"], "integrity_verified": True})
    return scopes


def add_reviewed_supplement(queue, urls, diagnostic_report):
    """Explicit bounded CLI selection + retained original replay, not metadata growth.

    Validate every candidate before releasing any waiting work. Refusals,
    source cooldowns and original membership/receipts are never reset. The
    source response is copied by content hash to an immutable private proof,
    since ordinary diagnostic files may be refreshed later.
    """
    from downloader.news_fulltext_policy import extract_article, normalize_url
    from downloader.news_fulltext_queue import utc_now
    urls = sorted(set(normalize_url(url) for url in urls))
    diagnostic_report = Path(diagnostic_report)
    if not 1 <= len(urls) <= 16 or diagnostic_report.stat().st_size > 8 << 20:
        raise ValueError("bounded explicit supplement review required")
    report_sha = sha256_file(diagnostic_report)
    report = json.loads(diagnostic_report.read_text())
    probes = {normalize_url(p["url"]): p for p in report["probes"]}
    reviewed = {}
    for url in urls:
        source = queue.registry.require(url)
        if source.acquisition_mode != "finite" or not source.allow_reviewed_supplements:
            raise ValueError("source has not enabled reviewed supplemental capture")
        if queue.db.execute("SELECT 1 FROM finite_cohort_members WHERE provider=? AND url=? UNION ALL "
                            "SELECT 1 FROM finite_supplement_members WHERE provider=? AND url=?",
                            (source.provider, url, source.provider, url)).fetchone():
            continue
        probe = probes.get(url, {})
        if probe.get("status") != 200 or not probe.get("private_diagnostic_path"):
            raise ValueError("supplement lacks retained successful public source response")
        path = Path(probe["private_diagnostic_path"]).resolve()
        # The canonical probe writes private diagnostics beside its output
        # report, not into the collector's data root. Bind to that exact scope.
        diagnostic_root = diagnostic_report.resolve().parent / "private-source-diagnostics"
        if not path.is_relative_to(diagnostic_root.resolve()):
            raise ValueError("supplement source outside private diagnostic root")
        if path.stat().st_size > 10 << 20:
            raise ValueError("unbounded supplement diagnostic file")
        payload = path.read_bytes()
        receipt = json.loads(path.with_suffix(".receipt.json").read_text())
        if (hashlib.sha256(payload).hexdigest() != receipt["sha256"] or receipt.get("diagnostic_only") is not True
                or receipt.get("accepted_as_article") is not False or receipt.get("publish") is not False):
            raise ValueError("supplement diagnostic receipt invalid")
        table = pq.read_table(pa.BufferReader(payload))
        if table.num_rows != 1:
            raise ValueError("supplement source grain invalid")
        raw = table.to_pylist()[0]
        if (raw["provider"] != source.provider or raw["request_url"] != url
                or queue.registry.require(raw["final_url"]).provider != source.provider
                or hashlib.sha256(raw["raw_response_body"]).hexdigest() != probe["response_sha256"]
                or raw["response_sha256"] != probe["response_sha256"]
                or raw.get("diagnostic_only") is not True or raw.get("accepted_as_article") is not False or raw.get("publish") is not False):
            raise ValueError("supplement original response identity mismatch")
        if source.article_path_pattern and not re.search(source.article_path_pattern, urlsplit(raw["final_url"]).path):
            raise ValueError("supplement source path outside reviewed article scope")
        article = extract_article(raw["raw_response_body"], source, page_url=raw["final_url"])
        reviewed[url] = (source, payload, {"url": url, "final_url": raw["final_url"],
            "response_sha256": raw["response_sha256"], "reviewed_body_sha256": article["body_sha256"],
            "reviewed_authors_json": article["authors_json"], "reviewed_publication_raw": article["published_at_raw"],
            "source_data_fingerprint_at_approval": source.data_fingerprint})
    if sha256_file(diagnostic_report) != report_sha:
        raise ValueError("supplement diagnostic report changed during review")
    grouped = {}
    for url, (source, payload, evidence) in reviewed.items():
        grouped.setdefault(source.provider, []).append((url, payload, evidence))
    scopes = []
    with queue.db:
        for provider, selected in grouped.items():
            members = [url for url, _, _ in selected]
            digest = hashlib.sha256("".join(url + "\n" for url in members).encode()).hexdigest()
            evidence = []
            for _, payload, item in selected:
                source_sha = hashlib.sha256(payload).hexdigest()
                private = "capture_cohorts/supplement_sources/" + source_sha + ".parquet"
                target = queue.root / private
                if target.exists():
                    if sha256_file(target) != source_sha:
                        raise ValueError("supplement immutable source file corrupted")
                else:
                    atomic_write_bytes(target, payload, durable=True)
                evidence.append({**item, "private_source_path": private, "private_source_sha256": source_sha})
            scope = {"contract_version": 1, "scope_kind": "explicit_diagnostic_verified_supplement",
                     "provider": provider, "urls": members, "urls_sha256": digest, "source_evidence": evidence,
                     "diagnostic_report_sha256": report_sha, "approved_at_utc": utc_now(),
                     "original_cohort_unchanged": True, "all_publisher_history_complete": False, "publish": False}
            scope_payload = (json.dumps(scope, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
            scope_hash = hashlib.sha256(scope_payload).hexdigest()
            relative = f"capture_cohorts/supplements/{provider}-{digest}-{scope_hash}.json"
            atomic_write_bytes(queue.root / relative, scope_payload, durable=True)
            queue.db.execute("INSERT INTO finite_capture_supplements VALUES (?,?,?,?,?)",
                             (provider, digest, len(members), relative, sha256_file(queue.root / relative)))
            for url in members:
                row = queue.db.execute("SELECT id FROM articles WHERE url=? AND provider=?", (url, provider)).fetchone()
                if row is None:
                    raise ValueError("supplement URL absent from explicit frontier")
                queue.db.execute("INSERT INTO finite_supplement_members VALUES (?,?,?,?)", (provider, row[0], url, digest))
                queue.db.execute("UPDATE articles SET state='pending' WHERE id=? AND state='waiting_cohort'", (row[0],))
            scopes.append({"provider": provider, "urls": len(members), "scope_sha256": digest, "receipt_path": relative})
    return scopes


def _declared_calendar_day(value):
    """Compare literal source days, not inferred UTC or decision availability."""
    if not isinstance(value, str):
        return None
    match = re.match(r"(?:日期\s*[:：]\s*)?(\d{4})[/-](\d{1,2})[/-](\d{1,2})(?=$|[ T])", value.strip())
    if not match:
        return None
    try:
        return datetime(*map(int, match.groups())).date().isoformat()
    except ValueError:
        return None


def materialize_reviewed_supplements(queue, urls, *, lease_seconds):
    """Revalidate approved retained originals and use the normal durable owner.

    No HTTP object, new observation, refusal reset or fallback request. Validate
    the whole explicit batch before claiming work; legacy diagnostics without
    real headers/authorization/observation clocks remain review-only evidence.
    """
    from downloader.news_fulltext import article_response_record, extract_article_response
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_policy import normalize_url
    from downloader.news_fulltext_queue import source_path, utc_now
    urls = sorted(set(normalize_url(url) for url in urls))
    if not 1 <= len(urls) <= 16:
        raise ValueError("bounded explicit retained supplement selection required")
    prepared, scopes = [], {}
    started = time.monotonic()
    for url in urls:
        source = queue.registry.require(url)
        if source.acquisition_mode != "finite" or not source.allow_reviewed_supplements:
            raise ValueError("source has not enabled reviewed supplemental capture")
        header = queue.db.execute("SELECT s.* FROM finite_capture_supplements s JOIN finite_supplement_members m "
            "ON m.provider=s.provider AND m.scope_sha256=s.scope_sha256 WHERE m.provider=? AND m.url=?",
            (source.provider, url)).fetchone()
        if header is None:
            raise ValueError("retained materialization requires an immutable approved supplement")
        key = (source.provider, header["scope_sha256"])
        if key not in scopes:
            scopes[key] = _validated_supplement_receipt(queue.root, queue.db, header)
        scope = scopes[key]
        evidence = next(e for e in scope["source_evidence"] if e["url"] == url)
        if evidence["source_data_fingerprint_at_approval"] != source.data_fingerprint:
            raise ValueError("retained supplement source contract changed")
        path = source_path(queue.root, evidence["private_source_path"])
        if path.stat().st_size > 10 << 20:
            raise ValueError("unbounded retained supplement source")
        payload = path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != evidence["private_source_sha256"]:
            raise ValueError("retained supplement immutable source digest mismatch")
        table = pq.read_table(pa.BufferReader(payload))
        if table.num_rows != 1:
            raise ValueError("retained supplement source grain invalid")
        raw = table.to_pylist()[0]
        if (raw.get("provider") != source.provider or raw.get("request_url") != url
                or raw.get("final_url") != evidence["final_url"]
                or queue.registry.require(raw["final_url"]).provider != source.provider
                or raw.get("response_sha256") != evidence["response_sha256"]
                or not isinstance(raw.get("raw_response_body"), bytes)
                or hashlib.sha256(raw["raw_response_body"]).hexdigest() != evidence["response_sha256"]
                or raw.get("diagnostic_only") is not True or raw.get("accepted_as_article") is not False
                or raw.get("publish") is not False):
            raise ValueError("retained supplement original identity invalid")
        try:
            headers = json.loads(raw["headers_json"])
            authorization = json.loads(raw["authorization_json"])
            observed = datetime.fromisoformat(raw["captured_at_utc"])
            approved = datetime.fromisoformat(scope["approved_at_utc"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("retained supplement real HTTP evidence missing") from error
        if (not isinstance(headers, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in headers.items())
                or authorization != source.authorization or observed.utcoffset() is None or approved.utcoffset() is None
                or observed > approved or approved > datetime.now(timezone.utc)):
            raise ValueError("retained supplement authorization or observation clock invalid")
        response = Response(raw["final_url"], 200, {k.lower(): v for k, v in headers.items()}, raw["raw_response_body"])
        if (source.embedded_article_layout in {"ftv_visible_article_v1", "ftv_visible_article_v2", "ftv_visible_article_v3", "ftv_visible_article_v4", "ftv_visible_article_v5", "ftv_visible_article_v6", "ftv_visible_article_v7"} and urlsplit(response.url).hostname != "www.ftvnews.com.tw"
                or source.embedded_article_layout == "macau_visible_news_v1" and response.url != url):
            raise ValueError("retained supplement needs independent original resolution evidence")
        parse_started = time.monotonic()
        with queue.metrics.measure("article_extraction", source.provider):
            article = extract_article_response(response, source)
        if (article["body_sha256"] != evidence["reviewed_body_sha256"]
                or article["authors_json"] != evidence["reviewed_authors_json"]
                or article["published_at_raw"] != evidence["reviewed_publication_raw"]):
            raise ValueError("retained supplement re-extraction differs from approved source")
        row = queue.db.execute("SELECT * FROM articles WHERE url=? AND provider=?", (url, source.provider)).fetchone()
        if row is None:
            raise ValueError("retained supplement absent from canonical frontier")
        row = dict(row)
        record = article_response_record(source, row, response, article, captured_at_utc=raw["captured_at_utc"],
            http_duration_seconds=None, parse_duration_seconds=time.monotonic() - parse_started,
            duration_seconds=time.monotonic() - parse_started,
            resolution_basis="immutable_reviewed_diagnostic_original; original_request_and_final_url_preserved")
        source_day = _declared_calendar_day(article.get("published_at_raw"))
        metadata_day = _declared_calendar_day(row.get("metadata_date_raw"))
        record.update(source_observed_at_utc=raw["captured_at_utc"], reparsed_at_utc=utc_now(),
            acquisition_transport="retained_raw_reparse", replay_source_parquet=evidence["private_source_path"],
            reviewed_supplement_replay_contract_version=RETAINED_SUPPLEMENT_REPLAY_VERSION,
            reviewed_supplement_scope_sha256=header["scope_sha256"], metadata_title_raw=row.get("title"),
            metadata_source_publication_day_conflict=(source_day != metadata_day if source_day and metadata_day else None),
            historical_point_in_time=False)
        prepared.append((url, row, record))
    records, skipped = [], []
    for url, head, record in prepared:
        if head["body_sha256"] or head["state"] not in {"pending", "retry", "retry_cooldown"}:
            skipped.append(url)
            continue
        row = queue.claim_url(url, lease_seconds)
        if row is None:
            skipped.append(url)
            continue  # Persisted due/lease/refusal rules remain authoritative.
        witness = queue.db.execute("SELECT * FROM articles WHERE url=?", (url,)).fetchone()
        record["retained_supplement_head_json"] = json.dumps(
            {field: witness[field] for field in RETAINED_SUPPLEMENT_HEAD_FIELDS}, sort_keys=True)
        records.append(record)
    queue.accept(records)
    return {"contract_version": RETAINED_SUPPLEMENT_REPLAY_VERSION, "materialized_bodies": len(records),
        "selected_urls": len(urls), "skipped_urls": skipped, "publisher_requests_this_command": 0,
        "retained_original_response_bytes": sum(r["http_response_bytes"] for r in records),
        "metadata_source_publication_day_conflicts": sum(r["metadata_source_publication_day_conflict"] is True for r in records),
        "wall_seconds": time.monotonic() - started, "original_observation_clocks_preserved": True,
        "original_cohort_unchanged": True, "all_publisher_history_complete": False, "publish": False}


def supplement_progress(db, provider):
    scopes = db.execute("SELECT count(*) FROM finite_capture_supplements WHERE provider=?", (provider,)).fetchone()[0]
    if not scopes:
        return None
    states, bodies = Counter(), 0
    for state, count, stored in db.execute("SELECT a.state,count(*),sum(a.body_sha256 IS NOT NULL) FROM finite_supplement_members m "
                                            "JOIN articles a ON a.id=m.article_id WHERE m.provider=? GROUP BY a.state", (provider,)):
        states[state] = count
        bodies += stored or 0
    return {"scopes": scopes, "urls": sum(states.values()), "stored_bodies": bodies,
            "states": dict(states), "original_cohort_unchanged": True, "all_publisher_history_complete": False}


def cohort_progress(db, provider: str) -> dict | None:
    """Bounded cohort join, not another full frontier scan per status update."""
    header = db.execute("SELECT * FROM finite_cohorts WHERE provider=?", (provider,)).fetchone()
    if not header:
        return None
    states, bodies, bytes_saved = Counter(), 0, 0
    for state, count, stored, saved in db.execute(
            "SELECT a.state,count(*),sum(a.body_sha256 IS NOT NULL),sum(a.body_bytes) "
            "FROM finite_cohort_members m JOIN articles a ON a.id=m.article_id WHERE m.provider=? GROUP BY a.state",
            (provider,)):
        states[state] = count
        bodies += stored or 0
        bytes_saved += saved or 0
    ready = sum(states[s] for s in READY_STATES)
    known = db.execute("SELECT coalesce(sum(articles),0) FROM state_metrics WHERE provider=?", (provider,)).fetchone()[0]
    outside_outcomes = Counter()
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='finite_source_outcomes'").fetchone():
        outside_outcomes = reviewed_nonbody_progress(db, provider)
    return {"urls": header["urls"], "urls_sha256": header["urls_sha256"],
            "created_at_utc": header["created_at_utc"], "states": dict(states),
            "stored_bodies": bodies, "stored_text_bytes": bytes_saved,
            "body_progress_pct": 100 * bodies / header["urls"] if header["urls"] else None,
            "remaining_missing_bodies": header["urls"] - bodies,
            "eligible_remaining_urls": ready, "new_urls_outside_cohort": max(0, known - header["urls"]),
            "reviewed_nonbody_outside_cohort": {"contract_version": NONBODY_SCOPE_OUTCOME_CONTRACT_VERSION,
                "urls": sum(outside_outcomes.values()), "states": dict(outside_outcomes), "accepted_article_bodies": 0},
            "all_cohort_bodies_stored": bodies == header["urls"],
            "cohort_has_pending_work": bool(ready), "all_publisher_history_complete": False}
