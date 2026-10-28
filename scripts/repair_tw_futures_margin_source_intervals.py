#!/usr/bin/env python3
"""Repair missing margin intervals and corporate views from bound originals.

No raw download, OCR, execution ledger or full rule parsing. The maintained
workspace publishes one atomic repaired current bundle.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import polars as pl
from downloader.artifact_io import atomic_write_json,atomic_write_parquet,sha256_file
from stockagent.data.tw_futures_rule_workspace import maintained_rule_build
from stockagent.data.tw_futures_margin_preparation import margin_candidate_intervals,compose_margin_ratio_intervals
from stockagent.data.tw_futures_margin_release import read_bound_output


def bind_retained_margin_sources(archive, bindings, *, products):
    """Bind omitted, already captured originals to their dated announcements."""
    import re
    from stockagent.data.tw_futures_margin_preparation import check_publication

    admitted = []
    seen = set()
    for item in bindings:
        required = {'source_url', 'content_sha256', 'announcement_url', 'published_date', 'products'}
        if (set(item) != required or not isinstance(item['products'], list)
                or not item['products'] or len(set(item['products'])) != len(item['products'])
                or not set(item['products']) <= set(products)
                or not re.fullmatch('[a-f0-9]{64}', item['content_sha256'])):
            raise ValueError('retained margin original exceeds its explicit gap scope or lacks a SHA')
        url, announcement = item['source_url'], item['announcement_url']
        if url in seen:
            raise ValueError('duplicate retained margin original')
        seen.add(url)
        notice = archive.conn.execute('SELECT published_date,category FROM announcements WHERE url=?',
                                      (announcement,)).fetchone()
        if (notice is None or notice['category'] != 'margins'
                or notice['published_date'] != item['published_date']
                or (url != announcement and url not in archive.children(announcement))):
            raise ValueError('retained margin original is not bound to its dated margin announcement')
        doc = archive.document(url)
        if doc['content_sha256'] != item['content_sha256']:
            raise ValueError('retained margin original SHA mismatch')
        check_publication(doc['text'], item['published_date'])
        if url != announcement:
            archive.document(announcement)
        admitted.append(dict(item))
    return admitted


def bind_canonical_seasonal_sources(archive, bindings, *, continuations):
    """Retain an omitted seasonal original without supplying financial values.

    A parent bundle need not contain every original already captured by the
    canonical collector. Admit only explicitly pinned, completed originals for
    the requested continuation dates. The existing continuation parser still
    proves the unchanged-level clause and its join to the earlier amount.
    """
    import re
    from datetime import date
    from scripts.build_tw_futures_margin_event_candidates import candidate_notice_clock

    requested = set(continuations)
    admitted, seen = [], set()
    for item in bindings:
        required = {'source_url', 'content_sha256', 'published_date'}
        if (not isinstance(item, dict) or set(item) != required
                or not isinstance(item['content_sha256'], str)
                or not re.fullmatch('[a-f0-9]{64}', item['content_sha256'])
                or (item['source_url'], item['published_date']) not in requested):
            raise ValueError('canonical seasonal original lacks its requested date and SHA')
        date.fromisoformat(item['published_date'])
        url = item['source_url']
        if url in seen:
            raise ValueError('duplicate canonical seasonal original')
        seen.add(url)
        notice = archive.conn.execute(
            'SELECT category FROM announcements WHERE url=? AND published_date=?',
            (url, item['published_date'])).fetchone()
        source = archive.conn.execute(
            'SELECT state FROM documents WHERE url=?', (url,)).fetchone()
        if (notice is None or notice['category'] != 'margins'
                or source is None or source['state'] != 'complete'):
            raise ValueError('canonical seasonal original lacks a completed dated margin source')
        doc = archive.document(url)
        if doc['content_sha256'] != item['content_sha256']:
            raise ValueError('canonical seasonal original SHA mismatch')
        if not candidate_notice_clock(doc['text'], item['published_date'])['issue_date_bound']:
            raise ValueError('canonical seasonal original lacks its issuing date')
        admitted.append(dict(item))
    return admitted


def exclude_reused_margin_attachment_bindings(archive, facts, *, products):
    """Quarantine a backdated attachment view supported by a dated original.

    A reused URL is not evidence that its current bytes existed at every old
    index date. Only a separately published, byte-bound original with the same
    complete financial event can supply that event. Preserve the rejected row
    and both source identities; never repair its claimed publication date.
    """
    from datetime import date
    from scripts.build_tw_futures_margin_event_candidates import (
        candidate_notice_clock, reextract_retained_margin_text,
    )

    def signature(row):
        return (row.get('product'), row.get('margin_kind'),
                tuple(row.get('after') or ()), tuple(row.get('before') or ()),
                row.get('effective_date'), row.get('effective_phase'))

    def permanent(row):
        return (row.get('margin_kind') == 'fixed_twd'
                and row.get('effective_phase') in
                    ('product_regular_open', 'after_product_regular_close')
                and len(row.get('after') or ()) == 3
                and len(row.get('before') or ()) == 3
                and not row.get('requires_reversion_review')
                and (row.get('temporary_end_evidence') or '[]') == '[]'
                and not row.get('restoration_evidence'))

    anchors = {}
    for row in facts:
        if (row['product'] in products and permanent(row)
                and row.get('issue_date_bound') and not row.get('clock_error')):
            anchors.setdefault(signature(row), []).append(row)
    possible = []
    for index, row in enumerate(facts):
        if (row['product'] not in products or not permanent(row)
                or row.get('issue_date_bound')
                or row.get('clock_error') !=
                    'historical attachment issuing date disagrees with index'):
            continue
        peers = anchors.get(signature(row), [])
        # Existing same-byte verified views already resolve their own bad
        # associations in the interval compiler. Do not rewrite them.
        if any(p['source_content_sha256'] == row['source_content_sha256'] for p in peers):
            continue
        peers = [p for p in peers if p['source_url'] != row['source_url']
                 and row.get('published_date', '') < p.get('published_date', '')]
        if peers:
            possible.append((index, row, peers))
    if not possible:
        return list(facts), []
    urls = {r['source_url'] for _, row, peers in possible for r in [row, *peers]}
    native = reextract_retained_margin_text(archive, source_urls=urls)

    def exact_native(row):
        return any(signature(n) == signature(row)
                   and all(n.get(k) == row.get(k) for k in
                       ('source_url', 'source_content_sha256', 'published_date',
                        'known_at', 'issue_date_bound')) for n in native)

    rejected = {}
    for index, row, peers in possible:
        if not exact_native(row):
            continue
        doc = archive.document(row['source_url'])
        if doc['content_sha256'] != row['source_content_sha256']:
            raise ValueError('reused margin attachment original SHA mismatch')
        for peer in peers:
            if not exact_native(peer):
                continue
            announcement = peer.get('announcement_url') or peer['source_url']
            notice = archive.conn.execute('SELECT published_date,category FROM announcements WHERE url=? '
                'AND published_date=?', (announcement, peer['published_date'])).fetchone()
            if (notice is None or notice['category'] != 'margins'
                    or (announcement != peer['source_url']
                        and peer['source_url'] not in archive.children(announcement))):
                continue
            original = archive.document(peer['source_url'])
            if original['content_sha256'] != peer['source_content_sha256']:
                raise ValueError('corroborating margin original SHA mismatch')
            if announcement != peer['source_url']:
                archive.document(announcement)
            if peer.get('known_at') != peer['published_date'] + 'T23:59:59+08:00':
                continue
            if date.fromisoformat(peer['published_date']) >= date.fromisoformat(peer['effective_date']):
                continue
            clocks = [candidate_notice_clock(d['text'], peer['published_date'])
                      for d in (doc, original)]
            if any(not c.get('issue_date_bound') or c.get('clock_error')
                   or c.get('effective_date') != peer['effective_date']
                   or c.get('effective_phase') != peer['effective_phase']
                   or c.get('requires_reversion_review')
                   or (c.get('temporary_end_evidence') or '[]') != '[]' for c in clocks):
                continue
            rejected[index] = dict(
                contract='independently_bound_reused_margin_attachment_rejection_v1',
                reason='captured_attachment_bytes_postdate_the_claimed_index_publication',
                rejected_fact=dict(row), corroborating_fact=dict(peer),
                rejected_fact_republication_or_numeric_inference=False,
                source_values_supplied_only_by_the_independent_original=True)
            break
    return [r for i, r in enumerate(facts) if i not in rejected], list(rejected.values())


def prepare_margin_repairs(current: Path, worklist: Path, output: Path, receipt: Path,
                           *, archive_root: Path, restoration_evidence=(), source_reviews=(),
                           reextract_sources=False, selected_products=(), continue_delta: Path | None = None,
                           code_reviews=(), seasonal_continuations=(), inherited_rules: Path | None = None,
                           retained_source_bindings=(), reconcile_reused_attachments=False,
                           canonical_seasonal_source_bindings=()):
    """Re-prove only gap-owning restorations from retained cash evidence."""
    import time
    begun = time.monotonic()
    if retained_source_bindings and not reextract_sources:
        raise ValueError('retained margin originals require canonical re-extraction')
    from stockagent.data.tw_futures_margin_preparation import RuleArchive
    from scripts.build_tw_futures_margin_event_candidates import (
        rebuild_disposal_restorations, source_review_candidates, replace_reviewed_margin_facts,
        repair_missing_margin_source_context, apply_margin_code_reviews,
        reextract_retained_margin_text, retain_margin_code_corrections,
        bind_retained_single_session_margin_clocks, bind_native_margin_period_extension,
        compose_bound_margin_extension_views,
        repair_native_restoration_period_revisions,
        compose_same_notice_margin_clocks,
        declared_seasonal_margin_continuations,
    )

    facts, manifest = read_bound_output(current / 'margin_event_candidates.parquet')
    old_levels, _ = read_bound_output(current / 'margin_level_intervals.parquet')
    parent_levels = old_levels
    # Integrated releases also retain legal/accounting inputs without a URL;
    # they remain bound by their SHA and are not a margin notice candidate.
    rows = [dict(s, url=s.get('url', '')) for s in manifest['sources']]
    source_roots = {(s['sha256'], s['path'], s['url'], s['kind']): current for s in rows}
    continued = None
    if continue_delta is not None:
        path = continue_delta / 'manifest.json'
        continued_sha = sha256_file(path)
        continued = json.loads(path.read_text())
        if (continued.get('status') != 'bounded_margin_restoration_sources_prepared'
                or continued.get('parent_manifest_sha256') != sha256_file(current / 'manifest.json')
                or continued.get('worklist_sha256') != sha256_file(worklist)):
            raise ValueError('continued margin delta changes its source/worklist parent')
        for name in ('margin_event_candidates.parquet', 'margin_level_intervals.parquet', 'margin_interval_issues.json'):
            if sha256_file(continue_delta / name) != continued['outputs'][name]['sha256']:
                raise ValueError('continued margin delta output SHA mismatch')
        facts = pl.read_parquet(continue_delta / 'margin_event_candidates.parquet')
        old_levels = pl.read_parquet(continue_delta / 'margin_level_intervals.parquet')
        for source in continued['sources']:
            if not (continue_delta / source['path']).resolve().is_relative_to(continue_delta.resolve()):
                raise ValueError('continued margin delta source escapes its bundle')
            key = (source['sha256'], source['path'], source['url'], source['kind'])
            if key not in source_roots:
                rows.append(source)
                source_roots[key] = continue_delta
    gaps = pl.read_csv(worklist, try_parse_dates=True, schema_overrides={'contract': pl.String})
    products = set(gaps.filter(pl.col('missing_opening_margin') | pl.col('missing_settlement_margin'))['product'])
    products.update(p[:2] + 'F' for p in list(products) if p[-1:].isdigit())
    if selected_products:
        if not set(selected_products) <= products:
            raise ValueError('selected margin products exceed the retained gap scope')
        products = set(selected_products)
    # Both sides must be rebuilt: correcting an OCR label removes the false
    # boundary from its old product and adds the evidenced cell to its owner.
    # The old label must belong to the original gap scope, never an arbitrary
    # destination introduced by an earlier correction in this same request.
    gap_products = set(products)
    code_urls = set()
    for path in code_reviews:
        review = json.loads(path.read_text())
        if (review.get('review_kind') != 'source_bound_margin_product_code_cells'
                or not review.get('cells')
                or any(c['ocr_product'] not in gap_products for c in review['cells'])):
            raise ValueError('margin code review exceeds the retained gap product scope')
        products.update(c['product'] for c in review['cells'])
        code_urls.update(c['source_url'] for c in review['cells'])
    urls = set(facts.filter(pl.col('product').is_in(sorted(products))
        & pl.col('requires_reversion_review').fill_null(False))['source_url'])
    urls.update(code_urls)
    retained_urls={r['url'] for r in rows}
    explicit_seasonal_urls = {b.get('source_url') for b in canonical_seasonal_source_bindings
                             if isinstance(b, dict)}
    for url,published in seasonal_continuations:
        if url not in retained_urls and url not in explicit_seasonal_urls:
            raise ValueError('seasonal continuation requires an already retained original')
        urls.add(url)
    for path in source_reviews:
        payload = json.loads(path.read_text())
        legacy = payload.get('schema_version') == 1 and isinstance(payload.get('legacy'), dict)
        reviews = ([r for r in payload['legacy']['notices'] if r['kind']=='margin']
                   if legacy else payload.get('reviews', [payload]))
        for review in reviews:
            own = (set(review['values']) if legacy else
                   {r['product'] for r in review.get('rows', [])} | set(review.get('products', [])))
            if not own or not own <= products:
                raise ValueError('margin source review exceeds the financial gap product scope')
            urls.add(review['url'] if legacy else review['source_url'])
    if reconcile_reused_attachments:
        # Preserve all existing native text/table views used to verify an
        # independently dated replacement. Product scope comes from gaps.
        urls.update(facts.filter(pl.col('product').is_in(sorted(products)))['source_url'])
    review_hashes = set(facts.filter(pl.col('source_url').is_in(sorted(urls)))[
        'visual_review_sha256'].drop_nulls())
    output.mkdir(parents=True, exist_ok=True)
    archive = RuleArchive(archive_root, output)
    admitted_sources = bind_retained_margin_sources(archive, retained_source_bindings, products=products)
    admitted_seasonal_sources = bind_canonical_seasonal_sources(
        archive, canonical_seasonal_source_bindings, continuations=seasonal_continuations)
    urls.update(r['source_url'] for r in admitted_sources)
    if continued is not None:
        # Keep exact prior evidence before mutating the same pending delta.
        archive.copy(continue_delta / 'manifest.json', continued_sha, url='', kind='continued_margin_delta_manifest')
        for name, info in continued['outputs'].items():
            archive.copy(continue_delta / name, info['sha256'], url='', kind='continued_margin_delta_output')
    session_hashes = set()
    if reextract_sources:
        from datetime import timedelta
        retained_urls = {s['url'] for s in rows}
        margin_gaps = gaps.filter(pl.col('missing_opening_margin') | pl.col('missing_settlement_margin'))
        # Metadata lookup only; open exact retained originals around a gap.
        # The already diagnosed early CPF chain is not reparsed as stock data.
        for product in sorted(products - {'CPF'}):
            own = margin_gaps.filter(pl.col('product') == product)
            if not own.height:
                continue
            first, last = own['date'].min() - timedelta(days=30), own['date'].max()
            notices = archive.conn.execute('SELECT url FROM announcements WHERE category=? '
                'AND published_date BETWEEN ? AND ? AND title LIKE ?',
                ('margins', str(first), str(last), '%' + product + '%')).fetchall()
            for notice in notices:
                for url in (notice['url'], *archive.children(notice['url'])):
                    if url in retained_urls:
                        urls.add(url)
        for source in rows:
            if source['kind'] in ('single_session_rule_review', 'single_session_product_scope'):
                session_hashes.add(source['sha256'])
                if source['kind'] == 'single_session_rule_review':
                    review = json.loads((current / source['path']).read_text())
                    urls.update(s['source_url'] for s in review['sources'])

    def verify(source):
        root = source_roots[(source['sha256'], source['path'], source['url'], source['kind'])]
        path = root / source['path']
        if not path.resolve().is_relative_to(root.resolve()) or sha256_file(path) != source['sha256']:
            raise ValueError('margin repair retained input SHA/path mismatch')
        return path

    if continued is not None:
        for source in continued['sources']:
            archive.copy(verify(source), source['sha256'], url=source['url'], kind=source['kind'])

    # Retain every bound view for each original; no latest-view substitution.
    for source in rows:
        if source['url'] not in urls and source['sha256'] not in session_hashes and not (source['kind'] == 'visual_source_review'
                                              and source['sha256'] in review_hashes):
            continue
        path = verify(source)
        archive.copy(path, source['sha256'], url=source['url'], kind=source['kind'])
        if source['kind'] == 'parsed_v2':
            archive.cache[source['url']] = json.loads(path.read_text())
    by_hash = {r['sha256']: r for r in rows}
    combined = facts.to_dicts()
    for path in code_reviews:
        combined = apply_margin_code_reviews(archive, combined, path)
    fresh = []
    if reextract_sources:
        fresh = reextract_retained_margin_text(archive, source_urls=urls - {''})
        fresh = retain_margin_code_corrections(combined, [r for r in fresh
            if r['product'] in products - {'CPF'}])
        for original in admitted_sources:
            extracted = {r['product'] for r in fresh if r['source_url'] == original['source_url']
                         and r['source_content_sha256'] == original['content_sha256']}
            if not set(original['products']) <= extracted:
                raise ValueError('retained margin original does not supply every declared product')
        keys = {(r['source_content_sha256'], r['product']) for r in fresh}
        combined = [r for r in combined if (r['source_content_sha256'], r['product']) not in keys
            or r.get('extraction') == 'observed_disposition_conditional_restoration'] + fresh
    for path in source_reviews:
        reviewed, _ = source_review_candidates(archive, path)
        combined = replace_reviewed_margin_facts(combined, reviewed)
    clock_repairs = repair_missing_margin_source_context(archive, [r for r in combined
        if r.get('product') in products and r.get('source_url') in urls])
    if reextract_sources:
        bind_retained_single_session_margin_clocks(archive, combined)
    sibling_clock_repairs = compose_same_notice_margin_clocks(archive, [r for r in combined
        if r.get('product') in products and r.get('source_url') in urls])
    if reextract_sources:
        for index, row in enumerate(combined):
            if not row.get('extension_prior_notice_join_required') and not row.get('prior_notice_revocation_evidence'):
                continue
            doc = archive.document(row['source_url'])
            if doc['content_sha256'] != row['source_content_sha256']:
                raise ValueError('bounded extension source SHA mismatch')
            joined = bind_native_margin_period_extension(archive, doc['text'], row, combined)
            if joined is not None:
                combined[index] = joined
        compose_bound_margin_extension_views(archive, combined)
    input_proofs = []
    contexts = []
    fixed_attachment_only = (reconcile_reused_attachments and not seasonal_continuations
        and products <= {r['product'] for r in combined}
        and all(r.get('margin_kind') in ('fixed_twd', 'fixed_unresolved_currency')
                for r in combined if r['product'] in products))
    retained_dispositions = (bool(seasonal_continuations) or fixed_attachment_only) and not (
        restoration_evidence or source_reviews or reextract_sources or code_reviews
        or continue_delta is not None) and not any(
            r['product'] in products and r.get('extraction') == 'observed_disposition_conditional_restoration'
            for r in combined)
    for source in rows:
        if source['kind'] == 'restoration_evidence_manifest' and not retained_dispositions:
            contexts.append((source, json.loads(verify(source).read_text())))
    # The complete disposition context precedes bounded correction contexts.
    # Every input remains source-bound and partial contexts cannot erase a
    # different product's accepted restoration.
    contexts.sort(key=lambda pair: pl.read_parquet(verify(by_hash[pair[1]['dispositions']['sha256']])).height,
                  reverse=True)
    for source, evidence in contexts:
        portable = dict(evidence, sources=[])
        for key in ('universe', 'dispositions', 'observations'):
            original = by_hash[evidence[key]['sha256']]
            portable[key] = dict(path=str(verify(original).resolve()), sha256=original['sha256'])
        for original in evidence['sources']:
            retained = by_hash.get(original['sha256'])
            if retained is None:
                raise ValueError('margin restoration context lacks its original source')
            portable['sources'].append(dict(original, path=str(verify(retained).resolve())))
        path = output / 'restoration_inputs.json'
        atomic_write_json(path, portable)
        previous = [r for r in combined if r.get('extraction') == 'observed_disposition_conditional_restoration']
        rebuilt = rebuild_disposal_restorations(archive, combined, path, source_urls=urls)
        # A bounded input must not delete restorations established by the
        # complete source context. Retain them, and let conflicting numbers
        # remain a margin interval barrier.
        combined = [r for r in rebuilt if r.get('extraction') != 'observed_disposition_conditional_restoration']
        restorations = previous + [r for r in rebuilt if r.get('extraction') == 'observed_disposition_conditional_restoration']
        seen = set()
        for row in restorations:
            identity = json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
            if identity not in seen:
                combined.append(row)
                seen.add(identity)
        input_proofs.append(dict(source_manifest_sha256=source['sha256'], input_sha256=sha256_file(path)))
    for path in restoration_evidence:
        evidence = json.loads(path.read_text())
        if (evidence.get('parent_rule_manifest_sha256') != sha256_file(current / 'manifest.json')
                or evidence.get('worklist_sha256') != sha256_file(worklist)):
            raise ValueError('additional restoration evidence changes its retained rules/worklist parent')
        previous = [r for r in combined if r.get('extraction') == 'observed_disposition_conditional_restoration']
        rebuilt = rebuild_disposal_restorations(archive, combined, path, source_urls=urls)
        combined = [r for r in rebuilt if r.get('extraction') != 'observed_disposition_conditional_restoration']
        restored = previous + [r for r in rebuilt if r.get('extraction') == 'observed_disposition_conditional_restoration']
        unique = {json.dumps(r, sort_keys=True, ensure_ascii=False, default=str): r for r in restored}
        combined.extend(unique.values())
        input_proofs.append(dict(source_manifest_sha256=None, input_sha256=sha256_file(path)))
    period_repairs = []
    for path in restoration_evidence:
        combined, repaired = repair_native_restoration_period_revisions(archive, combined, path)
        period_repairs.extend(repaired)
    seasonal_repairs=[]
    product_classes=None
    if seasonal_continuations and inherited_rules is not None:
        source_root=inherited_rules.parent
        source_manifest=source_root/'source_manifest.json'
        universe_path=source_root/'universe/products.csv'
        source_identity=json.loads(source_manifest.read_text())
        if (sha256_file(source_manifest)!=manifest.get('parent_source_manifest_sha256')
                or source_identity.get('files',{}).get('universe/products.csv',{}).get('sha256')
                    !=sha256_file(universe_path)):
            raise ValueError('seasonal product classes differ from the exact source parent')
        source_universe=pl.read_csv(universe_path,infer_schema=False)
        if source_universe['product'].is_duplicated().any():
            raise ValueError('ambiguous seasonal product class identity')
        product_classes=dict(source_universe.select('product','asset_class').iter_rows())
        archive.copy(source_manifest,sha256_file(source_manifest),url='',kind='seasonal_product_class_parent')
        archive.copy(universe_path,sha256_file(universe_path),url='',kind='seasonal_product_classes')
    for url,published in seasonal_continuations:
        new=declared_seasonal_margin_continuations(archive,combined,url,published,products=products,
                                                 product_classes=product_classes)
        if not new:raise ValueError('declared seasonal continuation has no uniquely source-bound level')
        combined.extend(new)
        seasonal_repairs.extend(dict(product=r['product'],source_url=url,published_date=published,
            effective_date=r['effective_date'],after=r['after'],evidence=json.loads(r['extension_join_evidence'])) for r in new)
    ratio_path=current/'dated_margin_ratio_laws.json'
    inherited_ratio_proof=None
    if not ratio_path.exists():
        if inherited_rules is None:
            raise ValueError('integrated margin delta requires its explicit inherited rules parent')
        parent_path=inherited_rules/'manifest.json';parent_sha=sha256_file(parent_path)
        parent_manifest=json.loads(parent_path.read_text())
        if parent_sha!=manifest.get('parent_manifest_sha256'):
            raise ValueError('inherited margin ratio laws differ from the exact rules parent')
        ratio_path=inherited_rules/'dated_margin_ratio_laws.json'
        ratio_sha=sha256_file(ratio_path)
        if parent_manifest.get('outputs',{}).get(ratio_path.name,{}).get('sha256')!=ratio_sha:
            raise ValueError('inherited margin ratio laws lack their parent output SHA')
        inherited_ratio_proof=dict(parent_manifest_sha256=parent_sha,ratio_laws_sha256=ratio_sha)
        archive.copy(parent_path,parent_sha,url='',kind='inherited_margin_rules_manifest')
        archive.copy(ratio_path,ratio_sha,url='',kind='inherited_dated_margin_ratio_laws')
    elif inherited_rules is not None:
        raise ValueError('inherited rules input is only valid for an integrated delta without its own ratio laws')
    attachment_rejections = []
    if reconcile_reused_attachments:
        combined, attachment_rejections = exclude_reused_margin_attachment_bindings(
            archive, combined, products=products)
        rejection_path = output / 'reused_attachment_rejections.json'
        atomic_write_json(rejection_path, attachment_rejections)
        archive.copy(rejection_path, sha256_file(rejection_path), url='',
                     kind='reused_margin_attachment_rejections')
    levels, issues = margin_candidate_intervals(combined)
    levels, ratio_issues = compose_margin_ratio_intervals(levels,json.loads(ratio_path.read_text()))
    rebuilt = pl.DataFrame(levels, schema=old_levels.schema)
    newly_changed = sorted(p for p in products if not old_levels.filter(pl.col('product') == p).sort(
        'effective_date', 'effective_phase').equals(rebuilt.filter(pl.col('product') == p).sort(
        'effective_date', 'effective_phase')))
    rebuilt = pl.concat([old_levels.filter(~pl.col('product').is_in(newly_changed)),
        rebuilt.filter(pl.col('product').is_in(newly_changed))], how='vertical').sort('product', 'effective_date', 'effective_phase')
    all_changed = products | set(continued.get('changed_products', []) if continued is not None else [])
    affected = sorted(p for p in all_changed if not parent_levels.filter(pl.col('product') == p).sort(
        'effective_date', 'effective_phase').equals(rebuilt.filter(pl.col('product') == p).sort(
        'effective_date', 'effective_phase')))
    atomic_write_parquet(output / 'margin_event_candidates.parquet', pl.from_dicts(combined, infer_schema_length=None))
    atomic_write_parquet(output / 'margin_level_intervals.parquet', rebuilt)
    atomic_write_json(output / 'margin_interval_issues.json', issues + ratio_issues)
    outputs = {name: dict(sha256=sha256_file(output / name)) for name in
               ('margin_event_candidates.parquet', 'margin_level_intervals.parquet', 'margin_interval_issues.json')}
    if reconcile_reused_attachments:
        outputs['reused_attachment_rejections.json'] = dict(sha256=sha256_file(rejection_path))
    proof = dict(status='bounded_margin_restoration_sources_prepared',
        parent_manifest_sha256=sha256_file(current / 'manifest.json'), worklist_sha256=sha256_file(worklist),
        targeted_products=sorted(products), targeted_original_urls=sorted(urls), changed_products=affected,
        newly_changed_products=newly_changed,
        continued_delta_manifest_sha256=continued_sha if continued is not None else None,
        restoration_inputs=input_proofs, outputs=outputs, sources=list(archive.sources.values()),
        margin_source_reviews=[dict(path=str(p.resolve()), sha256=sha256_file(p)) for p in source_reviews],
        margin_code_reviews=[dict(path=str(p.resolve()), sha256=sha256_file(p)) for p in code_reviews],
        margin_clock_context_repairs=clock_repairs,
        margin_same_notice_clock_repairs=sibling_clock_repairs,
        margin_restoration_period_revisions=period_repairs,
        seasonal_continuations=seasonal_repairs,
        retained_disposition_restorations_reused=retained_dispositions,
        retained_margin_facts_sha256=sha256_file(current/'margin_event_candidates.parquet')
            if retained_dispositions else None,
        inherited_ratio_laws=inherited_ratio_proof,
        admitted_retained_margin_sources=admitted_sources,
        admitted_canonical_seasonal_sources=admitted_seasonal_sources,
        reused_attachment_bindings_rejected=len(attachment_rejections),
        reused_attachment_reconciliation_selected=bool(reconcile_reused_attachments),
        restoration_cash_scope=json.loads((output/'restoration_cash_scope.json').read_text())
            if (output/'restoration_cash_scope.json').exists() else None,
        elapsed_s=time.monotonic()-begun,
        reextracted_margin_facts=len(fresh), reextracted_margin_source_urls=sorted({r['source_url'] for r in fresh}),
        financial_values_inferred=False, new_provider_requests=0, new_ocr_jobs=0, new_accounting_builds=0,
        builder_sha256=sha256_file(Path(__file__)))
    atomic_write_json(output / 'manifest.json', proof)
    atomic_write_json(receipt, proof)
    archive.conn.close()
    print(json.dumps(dict(status=proof['status'], changed_products=affected)), flush=True)


def prepare_corporate_repairs(current: Path, worklist: Path, output: Path, receipt: Path,
                             *, product_universe: Path | None = None,
                             announcements: Path | None = None, numeric_repairs: bool = False,
                             review_overrides=(), archive_root: Path | None = None):
    """Re-read only source events implicated by the retained gap coordinates.

    This delta is evidence for the single maintained workspace, not another
    complete parser version. Publication and every consumed byte are checked.
    No downloading, OCR, account compilation, or training takes place.
    """
    import gzip
    import hashlib
    from datetime import date
    from collections import defaultdict
    from scripts.build_tw_futures_margin_event_candidates import (
        candidate_notice_clock, retained_document_text_views,
        corporate_numeric_anomalies, reconcile_corporate_numeric_views, retain_review_override,
    )
    from stockagent.data.tw_futures_margin_preparation import (
        corporate_grid_candidates, corporate_retained_view_candidates,
        corporate_text_candidates, corporate_terms_intervals,
        CORPORATE_TERMS_DERIVATION_VERSION,
    )

    facts, manifest = read_bound_output(current / 'corporate_event_candidates.parquet')
    gaps = pl.read_csv(worklist, try_parse_dates=True, schema_overrides={'contract': pl.String})
    by_product = defaultdict(list)
    for row in gaps.filter(pl.col('missing_adjusted_terms') | pl.col('unresolved_lifetime')
                           | pl.col('lost_inventory_continuation')).iter_rows(named=True):
        by_product[row['product']].append(row)
    targeted = set()
    numeric_before=corporate_numeric_anomalies(facts.to_dicts()) if numeric_repairs else []
    for row in numeric_before:
        targeted.add((row['source_url'],row['source_content_sha256'],row['product']))
    import re
    for row in facts.iter_rows(named=True):
        anchors = [*by_product.get(row['product'], []), *by_product.get(row.get('from_product'), [])]
        filename = re.search(r'_(\d{8})(?:[.(]|$)', row['source_url'])
        file_day = date.fromisoformat(filename[1][:4] + '-' + filename[1][4:6] + '-' + filename[1][6:]) if filename else None
        effective = date.fromisoformat(row['effective_date']) if row.get('effective_date') else None
        for gap in anchors:
            nearby = file_day is not None and abs((file_day - gap['date']).days) <= 35
            own_month = (gap['contract'] in (row.get('contract_months') or [])
                         and effective is not None and 0 <= (gap['date'] - effective).days <= 365)
            # An unresolved month list must not make its own dated original
            # disappear from repair discovery. This locates source bytes only;
            # parsing must still establish the actual physical month scope.
            own_unresolved_month = not gap['contract'] and effective == gap['date']
            if nearby or own_month or own_unresolved_month:
                targeted.add((row['source_url'], row['source_content_sha256'], row['product']))
                break
    sources = defaultdict(list)
    sources_by_hash = defaultdict(list)
    for row in manifest['sources']:
        # Integrated bundles also retain local legal-input receipts with no
        # document URL. They remain in the SHA inventory and cannot be used
        # as an announcement/text view for a different source document.
        if row.get('url'):
            sources[row['url']].append(row)
        sources_by_hash[row['sha256']].append(row)
    extra_input_hashes = {}
    notice_dates = {}
    if announcements is not None:
        extra_input_hashes['announcements'] = sha256_file(announcements)
        for row in pl.read_parquet(announcements, columns=['url', 'published_date']).iter_rows(named=True):
            notice_dates.setdefault(row['url'], set()).add(row['published_date'])
    if product_universe is not None:
        parent = json.loads(product_universe.with_name('manifest.json').read_text())
        digest = sha256_file(product_universe)
        if parent.get('products_sha256') != digest:
            raise ValueError('corporate source lookup universe SHA mismatch')
        extra_input_hashes['product_universe'] = digest
        universe = pl.read_csv(product_universe, infer_schema=False)
        symbols = defaultdict(set)
        for row in universe.select('product', 'underlying_symbol').iter_rows(named=True):
            if row['product'] in by_product and row['underlying_symbol']:
                symbols[row['underlying_symbol']].add(row['product'])
        # A missed/wrong OCR code cannot make an original undiscoverable.
        # The dated filename locates candidates only; the parsed code, month,
        # date, publication and every financial field still have to bind.
        for url, original_sources in sources.items():
            locator = re.search(r'/([0-9]{4,6})_(\d{8})(?:[.(]|$)', url)
            if locator is None or locator[1] not in symbols:
                continue
            located_day = date.fromisoformat(locator[2][:4] + '-' + locator[2][4:6] + '-' + locator[2][6:])
            products = {p for p in symbols[locator[1]]
                if any(abs((located_day - row['date']).days) <= 35 for row in by_product[p])}
            if not products:
                continue
            for source in original_sources:
                if source['kind'] != 'parsed_v2':
                    continue
                path = current / source['path']
                if not path.resolve().is_relative_to(current.resolve()) or sha256_file(path) != source['sha256']:
                    raise ValueError('source lookup parse SHA/path mismatch')
                document = json.loads(path.read_text())
                targeted.update((url, document['content_sha256'], p) for p in products)
    checked = {}
    consumed = {}
    override_archive = None
    override_admissions = []
    if review_overrides:
        if archive_root is None:
            raise ValueError('corporate extraction overrides require their original archive')
        from stockagent.data.tw_futures_margin_preparation import RuleArchive
        override_archive = RuleArchive(archive_root,output)
        override_admissions = [retain_review_override(override_archive,root) for root in review_overrides]

    def verified(source):
        path = current / source['path']
        if not path.resolve().is_relative_to(current.resolve()):
            raise ValueError('source path escapes maintained workspace')
        if source['path'] not in checked:
            if sha256_file(path) != source['sha256']:
                raise ValueError('retained corporate source SHA mismatch')
            checked[source['path']] = source['sha256']
        consumed[source['path']] = source
        return path

    fresh = []
    failures = []
    def corporate_publication_clock(text,published):
        clock=candidate_notice_clock(text,published)
        if clock['issue_date_bound']:
            return clock
        # A later official index can legitimately lag the issuing page by
        # more than the generic seven-day discovery heuristic. Retain the
        # later index as knowledge time, require one literal issuing date
        # and a still-future explicit adjustment, and never backdate values.
        from downloader.taifex_rule_parsing import temporal_mentions
        issued={r['date_iso'] for r in temporal_mentions(text) if r['role']=='publication'}
        if len(issued)!=1:
            return clock
        issue=next(iter(issued))
        original=candidate_notice_clock(text,issue)
        effective=original.get('effective_date')
        if (original['issue_date_bound'] and original.get('chronological')
                and effective and issue<=published<effective):
            return dict(original,publication_clock_contract='later_index_before_adjustment_v1',
                source_issued_date=issue,index_publication_date=published)
        return clock
    for url, digest in sorted({(u, d) for u, d, p in targeted}):
        documents = [json.loads(verified(s).read_text()) for s in sources[url] if s['kind'] == 'parsed_v2']
        documents = [d for d in documents if d.get('content_sha256') == digest]
        if len(documents) != 1:
            raise ValueError('corporate source needs one exact canonical parse')
        # Two official attachment URLs can retain the very same original.
        # The parse binds the compressed bytes as well as the content digest;
        # an alias never supplies this URL's publication clock or text review.
        raw_hash = documents[0].get('raw_sha256')
        originals = [s for s in sources_by_hash.get(raw_hash, []) if s['kind'] == 'raw_gzip'
            and hashlib.sha256(gzip.decompress(verified(s).read_bytes())).hexdigest() == digest]
        if not originals:
            failures.append(dict(url=url, source_content_sha256=digest,
                reason='exact_original_not_retained_in_current_bundle'))
            continue
        bound = [s for s in sources[url] if s['kind'] in
                 ('review_native', 'review_pages_including_ocr', 'page_extraction_receipt')]
        for s in bound:
            verified(s)
        archive = SimpleNamespace(bundle=current)
        views = retained_document_text_views(archive, url, documents[0], sources=bound)
        if override_archive is not None:
            views = list(dict.fromkeys([*views,*retained_document_text_views(
                override_archive,url,documents[0])]))
        old = facts.filter((pl.col('source_url') == url) & (pl.col('source_content_sha256') == digest))
        publications = set(old['published_date'].drop_nulls()) | notice_dates.get(url, set())
        bound_publications = {p for p in publications if any(
            corporate_publication_clock(text,p)['issue_date_bound'] for text,_ in views)}
        if len(bound_publications) != 1:
            failures.append(dict(url=url, reason='conflicting_retained_publication'
                if bound_publications else 'no_receipt_bound_publication_text'))
            continue
        # A reused URL can contain the amended PDF. Only the unique retained
        # index date compatible with these same original bytes may bind it;
        # an older index entry cannot backdate the amended financial values.
        published = next(iter(bound_publications))
        tables = []
        by_hash = {s['sha256']: s for s in sources[url]}
        for s in sources[url]:
            if s['kind'] not in ('native_table_receipt.json', 'ocr_table_receipt.json'):
                continue
            proof = json.loads(verified(s).read_text())
            if proof.get('content_sha256') != digest or proof.get('status') != 'complete':
                continue
            entries = [f for f in proof['files'] if f['path'] == 'tables.json']
            if len(entries) == 1 and entries[0]['sha256'] in by_hash:
                table_source=by_hash[entries[0]['sha256']]
                tables.append((table_source,json.loads(verified(table_source).read_text())))
        accepted_views = 0
        checked_views = []
        for text, view_identity in views:
            clock = corporate_publication_clock(text, published)
            if not clock['issue_date_bound']:
                continue
            accepted_views += 1
            checked_views.append((text, view_identity))
            candidates = corporate_text_candidates(text)
            for table_source, table in tables:
                for page in table['pages']:
                    for grid in page.get('tables', []):
                        for candidate in corporate_grid_candidates(grid['cells'], text, grid.get('caption', ''),
                                page_text=page.get('native_text', '')):
                            candidate.update(page=page['page'], table_index=grid.get('table_index'),
                                extraction_method='native_cell_grid' if table_source['kind'].startswith('native') else 'source_ruled_ocr_cell_grid',
                                numeric_source_cells_evidence=json.dumps(dict(
                                    source_content_sha256=digest,path=table_source['path'],
                                    sha256=table_source['sha256'],page=page['page'],
                                    cells=grid['cells']),sort_keys=True,ensure_ascii=False))
                            candidates.append(candidate)
            for candidate in candidates:
                if (url, digest, candidate['product']) not in targeted:
                    continue
                fresh.append(dict(candidate, source_url=url, announcement_url=url,
                    source_content_sha256=digest, published_date=published,
                    known_at=published + 'T23:59:59+08:00', issue_date_bound=True,
                    point_in_time_verified=False, retained_publication_views=[view_identity],
                    publication_clock_evidence=json.dumps({k:clock[k] for k in
                        ('publication_clock_contract','source_issued_date','index_publication_date') if k in clock},
                        sort_keys=True)))
        for candidate in corporate_retained_view_candidates(checked_views):
            if (url, digest, candidate['product']) not in targeted:
                continue
            fresh.append(dict(candidate, source_url=url, announcement_url=url,
                source_content_sha256=digest, published_date=published,
                known_at=published + 'T23:59:59+08:00', issue_date_bound=True,
                point_in_time_verified=False,
                retained_publication_views=sorted({v for t, v in checked_views})))
        if not accepted_views:
            failures.append(dict(url=url, reason='no_receipt_bound_publication_text'))
    output.mkdir(parents=True, exist_ok=True)
    # Retain originals and candidate views. Rejected extractions are not erased.
    additional = pl.from_dicts(fresh, infer_schema_length=None) if fresh else facts.head(0)
    combined = pl.concat([facts, additional], how='diagonal_relaxed').unique(maintain_order=True)
    repairs=[]
    if numeric_repairs:
        repaired,repairs=reconcile_corporate_numeric_views(combined.to_dicts())
        combined=pl.from_dicts(repaired,infer_schema_length=None).unique(maintain_order=True)
        atomic_write_json(output/'numeric_source_repairs.json',dict(
            diagnostic_before=numeric_before,repairs=repairs,
            diagnostic_after=corporate_numeric_anomalies(combined.to_dicts()),
            threshold_is_diagnostic_only=True,financial_values_inferred=False))
    _, issues = corporate_terms_intervals(combined.to_dicts())
    atomic_write_parquet(output / 'corporate_event_candidates.parquet', combined)
    atomic_write_json(output / 'corporate_term_issues.json', issues)
    proof = dict(status='bounded_corporate_source_views_prepared',
        derivation_version=CORPORATE_TERMS_DERIVATION_VERSION,
        parent_manifest_sha256=sha256_file(current / 'manifest.json'),
        worklist_sha256=sha256_file(worklist), targeted_source_products=len(targeted),
        targeted_originals=len({d for u, d, p in targeted}), added_views=additional.height,
        source_failures=failures, preserved_original_candidate_rows=facts.height,
        financial_values_inferred=False, new_provider_requests=0, new_ocr_jobs=0,
        new_accounting_builds=0, source_sha256s=checked,
        numeric_source_repairs=len(repairs),numeric_repair_contract='same_original_exact_event_coherent_numeric_view_v1',
        review_override_admissions=override_admissions,
        sources=list(override_archive.sources.values()) if override_archive is not None else [],
        outputs={'corporate_event_candidates.parquet': {'sha256': sha256_file(output / 'corporate_event_candidates.parquet')},
                 'corporate_term_issues.json': {'sha256': sha256_file(output / 'corporate_term_issues.json')},
                 **({'numeric_source_repairs.json':dict(sha256=sha256_file(output/'numeric_source_repairs.json'))}
                    if numeric_repairs else {})},
        lookup_input_sha256s=extra_input_hashes,
        preparation_sha256=sha256_file(ROOT / 'stockagent/data/tw_futures_margin_preparation.py'),
        builder_sha256=sha256_file(Path(__file__)))
    atomic_write_json(output / 'manifest.json', proof)
    atomic_write_json(receipt, proof)
    if override_archive is not None:override_archive.conn.close()
    print(json.dumps({k: proof[k] for k in ('status','targeted_originals','added_views','new_ocr_jobs')}), flush=True)


def stage_terminal_operand_repair(raw_source: Path, pending: Path, review_path: Path, receipt: Path):
    """Add an inspected deadline fixing, preserving the immutable base source."""
    import gzip
    import re
    from datetime import date
    from stockagent.data.tw_futures_margin_preparation import (
        bind_dated_corporate_terms, load_preparation_final_settlements,
        subscription_right_value_twd, terminal_contract_value_twd,
    )
    review = json.loads(review_path.read_text())
    if (review.get('contract') != 'reviewed_original_deadline_floor_twd_v1'
            or review.get('rounding') != 'floor_twd' or review.get('fixing_date') != review.get('deadline')):
        raise ValueError('a complete original deadline and rounding review is required')
    parent_path = pending / 'manifest.json'
    parent_sha = sha256_file(parent_path)
    parent = json.loads(parent_path.read_text())
    source_sha = sha256_file(raw_source / 'source_manifest.json')
    if (parent.get('status') != 'pending_source_bound_rule_delta'
            or parent.get('parent_source_manifest_sha256') != source_sha):
        raise ValueError('terminal operands require the same unpromoted source parent')
    if parent.get('terminal_source_delta') is not None or (pending / 'terminal').exists() or receipt.exists():
        raise ValueError('preserve an existing terminal operand delta')
    product, contract = review['product'], review['contract_month']
    for prefix in ('page', 'subscription', 'listing', 'cash'):
        if sha256_file(Path(review[prefix + '_path'])) != review[prefix + '_sha256']:
            raise ValueError('terminal operand original SHA mismatch')
    original_rules = json.loads((raw_source / 'rules/manifest.json').read_text())
    raw = [s for s in original_rules['sources'] if s['url'] == review['source_url'] and s['kind'] == 'raw_gzip']
    if len(raw) != 1:
        raise ValueError('exact original corporate notice is required')
    original_path = raw_source / 'rules' / raw[0]['path']
    if (sha256_file(original_path) != raw[0]['sha256']
            or __import__('hashlib').sha256(gzip.decompress(original_path.read_bytes())).hexdigest() != review['notice_content_sha256']):
        raise ValueError('corporate notice content differs from inspected page')
    detail = json.loads(Path(review['subscription_path']).read_text())
    listing = json.loads(Path(review['listing_path']).read_text())
    if str(detail.get('stat', '')).lower() != 'ok' or len(detail.get('data', [])) != 1:
        raise ValueError('own dated subscription detail is missing or ambiguous')
    row = dict(zip(detail['fields'], detail['data'][0], strict=True))
    match = re.fullmatch(r'([0-9.,]+) 元／股', row['每股認購金額'].strip())
    effective = date.fromisoformat(review['term_effective_date'])
    own_listing = [dict(zip(listing['fields'], r, strict=True)) for r in listing.get('data', [])
                   if str(r[1]).strip() == review['stock_id']]
    if (row['股票代號'].strip() != review['stock_id'] or match is None
            or str(listing.get('stat', '')).lower() != 'ok' or len(own_listing) != 1
            or own_listing[0]['詳細資料'] != review['stock_id'] + ',' + effective.strftime('%Y%m%d')
            or own_listing[0]['資料日期'] != f'{effective.year - 1911}年{effective.month:02d}月{effective.day:02d}日'):
        raise ValueError('subscription security/effective date/listing mismatch')
    strike = match[1].replace(',', '')
    fixing = date.fromisoformat(review['fixing_date'])
    cash = pl.scan_parquet(review['cash_path']).filter((pl.col('date') == str(fixing))
        & (pl.col('證券代號') == review['stock_id'])).collect()
    if (cash.height != 1 or str(cash['_source'][0]).lower() != 'twse' or cash['_dataset'][0] != 'twse_daily_ohlcv'
            or fixing.strftime('%Y%m%d') not in cash['_url'][0]):
        raise ValueError('exact unadjusted official cash fixing is required')
    close = cash['收盤價'][0].replace(',', '')
    rights = subscription_right_value_twd(close, strike, review['entitlement_units'])
    final = load_preparation_final_settlements(raw_source / 'final/futures_final_settlement_history.parquet')
    own = final.filter((pl.col('product') == product) & (pl.col('contract') == contract)).rename({'settlement_date': 'date'})
    terms, _ = read_bound_output(pending / 'corporate_terms_intervals.parquet')
    bound = bind_dated_corporate_terms(own.select('date', 'product', 'contract'), terms)
    if (own.height != 1 or own['settlement_method'][0] != 'cash_settlement'
            or own['date'][0] < fixing or bound.height != 1
            or bound['terms_binding_status'][0] != 'bound_prior_publication'
            or review['notice_content_sha256'] not in bound['source_content_sha256s'][0]
            or not bound['subscription_rights_at_final_settlement'][0]):
        raise ValueError('deadline fixing does not belong to the own terminal corporate terms')
    from decimal import Decimal, ROUND_HALF_UP
    quantity = re.fullmatch(r'([0-9.,]+) 股', row['按股東持股比例每千股認購'].strip())
    if (quantity is None or review.get('entitlement_decimal_places') != 4
            or (Decimal(quantity[1].replace(',', '')) * Decimal(str(bound['contract_multiplier'][0])) / 1000)
                .quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP) != Decimal(review['entitlement_units'])):
        raise ValueError('inspected entitlement differs from own dated subscription share ratio')
    expected = terminal_contract_value_twd(own['final_settlement_price'][0], bound['contract_multiplier'][0],
        bound['deliverable_cash_twd'][0], rights + bound['fixed_subscription_rights_twd'][0])
    old, old_manifest = read_bound_output(raw_source / 'terminal/terminal_subscription_values.parquet')
    added = pl.DataFrame([dict(date=own['date'][0], product=product, contract=contract,
        corporate_term_id=bound['corporate_term_id'][0], rights_twd=rights, fixing_date=fixing,
        stock_close=float(close), subscription_price=float(strike), notice_content_sha256=review['notice_content_sha256'],
        subscription_source_sha256=review['subscription_sha256'], cash_source_sha256=review['cash_sha256'],
        derived_terminal_value_twd=expected)], schema=old.schema)
    if old.join(added.select('date', 'product', 'contract'), on=['date', 'product', 'contract'], how='semi').height:
        raise ValueError('terminal fixing already exists; do not overwrite it')
    target = pending / 'terminal'
    target.mkdir(parents=True)
    sources = []
    def retain(path, digest, kind, url=''):
        path = Path(path)
        if sha256_file(path) != digest:
            raise ValueError('terminal source changed during staging')
        relative = Path('sources') / (digest + ''.join(path.suffixes))
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            try: os.link(path, destination)
            except OSError: shutil.copy2(path, destination)
        if sha256_file(destination) != digest:
            raise ValueError('terminal retained original SHA mismatch')
        entry = dict(path=str(relative), sha256=digest, kind=kind, url=url)
        if entry not in sources: sources.append(entry)
    for item in old_manifest['sources']:
        retain(raw_source / 'terminal' / item['path'], item['sha256'], item['kind'], item.get('url', ''))
    retain(raw_source / 'terminal/manifest.json', sha256_file(raw_source / 'terminal/manifest.json'), 'original_terminal_manifest')
    retain(parent_path, parent_sha, 'prior_pending_rule_manifest')
    retain(review_path, sha256_file(review_path), 'full_original_terminal_operand_review')
    retain(original_path, raw[0]['sha256'], 'raw_gzip', review['source_url'])
    for prefix in ('page', 'subscription', 'listing', 'cash'):
        retain(review[prefix + '_path'], review[prefix + '_sha256'], 'terminal_' + prefix + '_original')
    atomic_write_parquet(target / 'terminal_subscription_values.parquet', pl.concat([old, added], how='vertical'))
    atomic_write_parquet(target / 'terminal_overlay_rows.parquet', added)
    proof = dict(status='source_bound_terminal_components', parent_source_manifest_sha256=source_sha,
        parent_terminal_manifest_sha256=sha256_file(raw_source / 'terminal/manifest.json'),
        parent_pending_manifest_sha256=parent_sha, review_sha256=sha256_file(review_path),
        sources=sources, added_rows=added.height, rights_twd=rights, terminal_value_twd=expected,
        point_in_time_verified=False, no_model_features=True, no_daily_cash=True, financial_values_inferred=False,
        outputs={p.name: dict(sha256=sha256_file(p)) for p in target.glob('*.parquet')})
    atomic_write_json(target / 'manifest.json', proof)
    pointer = dict(contract='bound_terminal_operand_source_delta_v1', path='terminal',
                   sha256=sha256_file(target / 'manifest.json'), products=[product])
    parent['terminal_source_delta'] = pointer
    parent['outputs']['terminal/manifest.json'] = dict(sha256=pointer['sha256'])
    parent['changed_products'] = sorted(set(parent['changed_products']) | {product})
    atomic_write_json(parent_path, parent)
    atomic_write_json(receipt, dict(terminal_source_delta=pointer, pending_manifest_sha256=sha256_file(parent_path), **proof))
    print(json.dumps(dict(product=product, contract=contract, rights_twd=rights, terminal_value_twd=expected)), flush=True)


def stage_information_halt_repair(raw_source: Path, pending: Path, bundle: Path, receipt: Path):
    """Attach a bounded native legal-value review to the same pending delta."""
    from stockagent.data.tw_futures_information_halt import (
        load_information_halt_review, validate_information_halt_corporate_scope,
    )
    parent_path = pending / 'manifest.json'
    parent_sha = sha256_file(parent_path)
    parent = json.loads(parent_path.read_text())
    manifest = json.loads((bundle / 'manifest.json').read_text())
    if (parent.get('status') != 'pending_source_bound_rule_delta'
            or parent['parent_source_manifest_sha256'] != sha256_file(raw_source / 'source_manifest.json')
            or manifest.get('status') != 'source_bound_information_halt_extension'
            or manifest['parent_source_manifest_sha256'] != parent['parent_source_manifest_sha256']
            or manifest['corporate_input_sha256'] != sha256_file(pending / 'corporate_event_candidates.parquet')
            or manifest['implementation_sha256'] != sha256_file(ROOT / 'stockagent/data/tw_futures_information_halt.py')):
        raise ValueError('information-halt inputs require the exact unpromoted source/code parent')
    if parent.get('information_halt_source_delta') is not None or (pending / 'information_halt').exists() or receipt.exists():
        raise ValueError('preserve the existing information-halt delta and receipt')
    items = [*manifest['sources'], *manifest['reviews'], manifest['accounting_builder_baseline']]
    for item in items:
        file = (bundle / item['path']).resolve()
        if not file.is_relative_to(bundle.resolve()) or sha256_file(file) != item['sha256']:
            raise ValueError('information-halt input original SHA/path mismatch')
    episodes = [load_information_halt_review(bundle / r['path']) for r in manifest['reviews']]
    if (not episodes or any(e['corporate_input_sha256'] != manifest['corporate_input_sha256'] for e in episodes)):
        raise ValueError('information-halt review changes its corporate input scope')
    corporate, _ = read_bound_output(pending / 'corporate_event_candidates.parquet')
    for episode in episodes:
        validate_information_halt_corporate_scope(corporate, episode)
    return _stage_halt_source(pending, bundle, receipt, parent, parent_sha, manifest, episodes,
                             name='information_halt', baseline_key='accounting_builder_baseline')


def stage_loss_reduction_halt_repair(raw_source, pending, bundle, receipt, *, refresh_pending=False):
    """Attach own reviewed events while keeping the same unpromoted delta."""
    from stockagent.data.tw_futures_margin_halt_sources import load_loss_reduction_bundle
    parent_path = pending / 'manifest.json'
    parent_sha = sha256_file(parent_path)
    parent = json.loads(parent_path.read_text())
    if (parent.get('status') != 'pending_source_bound_rule_delta'
            or parent['parent_source_manifest_sha256'] != sha256_file(raw_source / 'source_manifest.json')):
        raise ValueError('loss-reduction staging requires the retained pending source parent')
    retained = parent.get('loss_reduction_halt_source_delta')
    if receipt.exists() or ((retained is not None or (pending / 'loss_reduction_halt').exists()) and not refresh_pending):
        raise ValueError('preserve the existing loss-reduction delta and receipt')
    _, episodes, _, _ = load_loss_reduction_bundle(
        raw_source, pending / 'corporate_event_candidates.parquet', bundle)
    manifest = json.loads((bundle / 'manifest.json').read_text())
    if refresh_pending:
        old_path = pending / 'loss_reduction_halt/manifest.json'
        if (retained is None or retained.get('path') != 'loss_reduction_halt'
                or sha256_file(old_path) != retained['sha256']):
            raise ValueError('only the same bound pending halt input may be refreshed')
        old = json.loads(old_path.read_text())
        if (not {(r['path'], r['sha256']) for r in old['reviews']}
                <= {(r['path'], r['sha256']) for r in manifest['reviews']}
                or old['accounting_library_baseline'] != manifest['accounting_library_baseline']):
            raise ValueError('halt input refresh must preserve every previous review and baseline')
        previous_items = [*old['sources'], *old['reviews'], old['accounting_library_baseline']]
        new_items = {i['path']: i['sha256'] for i in
                     [*manifest['sources'], *manifest['reviews'], manifest['accounting_library_baseline']]}
        if any(new_items.get(i['path']) != i['sha256'] or sha256_file(old_path.parent / i['path']) != i['sha256']
               for i in previous_items):
            raise ValueError('halt input refresh must preserve every previous original and review')
        backup = receipt.with_name(receipt.stem + '_halt_parent_manifest.json')
        if backup.exists(): raise FileExistsError('preserve the previous halt source receipt')
        shutil.copyfile(old_path, backup)
    return _stage_halt_source(pending, bundle, receipt, parent, parent_sha, manifest, episodes,
                             name='loss_reduction_halt', baseline_key='accounting_library_baseline',
                             refresh_pending=refresh_pending)


def _stage_halt_source(pending, bundle, receipt, parent, parent_sha, manifest, episodes, *, name, baseline_key,
                       refresh_pending=False):
    """Shared immutable source composition for the two dated halt policies."""
    parent_path = pending / 'manifest.json'
    items = [*manifest['sources'], *manifest['reviews'], manifest[baseline_key]]
    before = receipt.with_name(receipt.stem + '_parent_manifest.json')
    if before.exists():
        raise FileExistsError('preserve the pre-extension manifest receipt')
    target = pending / name
    target.mkdir(exist_ok=refresh_pending)
    for relative in sorted({i['path'] for i in items} | {'manifest.json'}):
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(bundle / relative, destination)
    for item in items:
        if sha256_file(target / item['path']) != item['sha256']:
            raise ValueError('copied halt source changed during staging')
    pointer = dict(contract='bound_' + name + '_source_delta_v1', path=name,
        sha256=sha256_file(target / 'manifest.json'), products=sorted({e['product'] for e in episodes}))
    parent[name + '_source_delta'] = pointer
    parent['changed_products'] = sorted(set(parent['changed_products']) | set(pointer['products']))
    parent['sources'] = [s for s in parent['sources'] if s.get('kind') != name + '_input']
    for path in sorted(p for p in target.rglob('*') if p.is_file()):
        parent['sources'].append(dict(path=str(path.relative_to(pending)), sha256=sha256_file(path),
                                      kind=name + '_input'))
    shutil.copyfile(parent_path, before)
    if sha256_file(parent_path) != parent_sha:
        raise ValueError('pending financial source changed during halt staging')
    atomic_write_json(parent_path, parent)
    proof = dict(status='source_bound_' + name + '_delta_staged', **{name + '_source_delta': pointer},
        parent_pending_manifest_sha256=parent_sha, pending_manifest_sha256=sha256_file(parent_path),
        parent_manifest_receipt_sha256=sha256_file(before), source_reviews=len(episodes),
        accounting_rows_recompiled=0, current_training_source_overwritten=False, financial_values_inferred=False)
    atomic_write_json(receipt, proof)
    print(json.dumps(proof), flush=True)
    return proof


def before_margin_opening_boundaries(facts, levels, reviews, *, archive):
    """Admit a literal before amount only at its own preannounced opening.

    A notice effective after close proves its before column for that session,
    not the unknown years preceding it. Keep existing intervals and require
    the literal after amount to own the corresponding closing boundary.
    """
    from datetime import date, datetime
    import math
    from zoneinfo import ZoneInfo
    from scripts.build_tw_futures_margin_event_candidates import reextract_retained_margin_text

    added, proofs = [], []
    for review in reviews:
        if review.get('review_kind') != 'source_bound_before_margin_opening_session_v1':
            raise ValueError('unsupported before-margin opening review')
        cases = review.get('cases', [])
        if not cases or len({(r['product'], r['effective_date']) for r in cases}) != len(cases):
            raise ValueError('opening margin requires distinct own-session cases')
        urls = {r['source_url'] for r in cases}
        native = reextract_retained_margin_text(archive, source_urls=urls)
        for case in cases:
            product, day = case['product'], str(date.fromisoformat(case['effective_date']))
            doc = archive.document(case['source_url'])
            if doc['content_sha256'] != case['content_sha256']:
                raise ValueError('opening margin original SHA mismatch')
            own = [r for r in native if r['product'] == product
                   and r['source_url'] == case['source_url']]
            retained = [r for r in facts.iter_rows(named=True) if r['product'] == product
                        and r['source_content_sha256'] == case['content_sha256']
                        and r['source_url'] == case['source_url']]
            if not own or not retained:
                raise ValueError('opening margin requires native and retained own-product facts')
            if not any((r.get('extraction') or '').startswith(
                    'canonical_retained_margin_grid:native_table_tables.json:') for r in own):
                raise ValueError('opening margin requires an original-bound native grid')
            for row in [*own, *retained]:
                before, after = row.get('before') or [], row.get('after') or []
                known = datetime.fromisoformat(row.get('known_at') or '')
                if (row.get('margin_kind') != 'fixed_twd' or row.get('effective_date') != day
                        or row.get('effective_phase') != 'after_product_regular_close'
                        or not row.get('issue_date_bound') or row.get('requires_reversion_review')
                        or row.get('before_column_semantics') is not None
                        or row.get('published_date') != case['published_date']
                        or row['published_date'] >= day or known.tzinfo is None
                        or known.astimezone(ZoneInfo('Asia/Taipei')).date().isoformat() >= day
                        or row['source_content_sha256'] != case['content_sha256']
                        or any(len(v) != 3 or not all(isinstance(n, (int, float))
                            and not isinstance(n, bool) and math.isfinite(n) and n > 0 for n in v)
                            or not v[0] >= v[1] >= v[2] for v in (before, after))):
                    raise ValueError('opening margin requires literal preannounced fixed before/after levels')
            signatures = {(tuple(r['before']), tuple(r['after']), r['known_at']) for r in [*own, *retained]}
            if len(signatures) != 1:
                raise ValueError('opening margin source views disagree')
            before, after, known_at = next(iter(signatures))
            closing = levels.filter((pl.col('product') == product) & (pl.col('effective_date') == day)
                                    & (pl.col('effective_phase') == 1)).to_dicts()
            if (len(closing) != 1 or closing[0]['margin_kind'] != 'fixed_twd'
                    or tuple(closing[0][f] for f in ('initial', 'maintenance', 'clearing')) != after
                    or case['content_sha256'] not in closing[0]['source_content_sha256s']
                    or closing[0]['known_at'] != known_at):
                raise ValueError('opening margin requires the same literal closing successor')
            coverage = levels.filter(pl.col('product') == product).to_dicts() + added
            covering = [r for r in coverage if r['product'] == product
                and (r['effective_date'], r['effective_phase']) <= (day, 0)
                and (not r['valid_until_date_exclusive'] or (day, 0) <
                    (r['valid_until_date_exclusive'], r['valid_until_phase_exclusive']))]
            if covering:
                if (len(covering) != 1 or covering[0]['margin_kind'] != 'fixed_twd'
                        or tuple(covering[0][f] for f in ('initial', 'maintenance', 'clearing')) != before):
                    raise ValueError('opening margin conflicts with an existing dated level')
            else:
                added.append(dict(closing[0], initial=before[0], maintenance=before[1], clearing=before[2],
                    effective_phase=0, valid_until_date_exclusive=day, valid_until_phase_exclusive=1,
                    known_at=known_at, requires_delayed_admission=False,
                    source_content_sha256s=[case['content_sha256']], source_urls=[case['source_url']],
                    temporary_level=False, restoration_applied=False))
            proofs.append(dict(product=product, effective_date=day, effective_phase=0,
                valid_until_date_exclusive=day, valid_until_phase_exclusive=1,
                before=list(before), after=list(after), known_at=known_at,
                source_content_sha256=case['content_sha256'], source_url=case['source_url'],
                older_dates_admitted=False, already_covered=bool(covering)))
    extra = pl.DataFrame(added, schema=levels.schema)
    return pl.concat([levels, extra], how='vertical').sort('product', 'effective_date', 'effective_phase'), proofs


def stage_opening_margin_boundary_repair(raw_source, pending, review_path, receipt, *, archive_root):
    """Preserve one pending source and add only previously uncovered openings."""
    from stockagent.data.tw_futures_margin_preparation import RuleArchive
    parent_path = pending / 'manifest.json'
    parent_sha = sha256_file(parent_path)
    parent = json.loads(parent_path.read_text())
    review = json.loads(review_path.read_text())
    facts, _ = read_bound_output(pending / 'margin_event_candidates.parquet')
    levels, _ = read_bound_output(pending / 'margin_level_intervals.parquet')
    if (parent.get('status') != 'pending_source_bound_rule_delta'
            or parent['parent_source_manifest_sha256'] != sha256_file(raw_source / 'source_manifest.json')
            or review['parent_pending_manifest_sha256'] != parent_sha
            or review['parent_source_manifest_sha256'] != parent['parent_source_manifest_sha256']
            or review['margin_facts_sha256'] != sha256_file(pending / 'margin_event_candidates.parquet')
            or review['margin_levels_sha256'] != sha256_file(pending / 'margin_level_intervals.parquet')):
        raise ValueError('opening margin requires the exact pending source parent')
    destination = pending / 'margin_opening_boundary' / review_path.name
    before = receipt.with_name(receipt.stem + '_parent_manifest.json')
    if destination.exists() or receipt.exists() or before.exists():
        raise FileExistsError('preserve the existing opening margin review and receipt')
    archive = RuleArchive(archive_root, pending)
    base = raw_source / 'rules'
    base_manifest = json.loads((base / 'manifest.json').read_text())
    urls = {r['source_url'] for r in review['cases']}
    for source in base_manifest['sources']:
        if source['url'] in urls:
            archive.copy(base / source['path'], source['sha256'], url=source['url'], kind=source['kind'])
    rebuilt, boundaries = before_margin_opening_boundaries(facts, levels, [review], archive=archive)
    if not any(not row['already_covered'] for row in boundaries):
        raise ValueError('opening margin review has no new bounded input')
    if sha256_file(parent_path) != parent_sha:
        raise ValueError('pending source changed during opening margin review')
    shutil.copyfile(parent_path, before)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(review_path, destination)
    parent.setdefault('opening_margin_boundary_reviews', []).append(dict(
        path=str(destination.relative_to(pending)), sha256=sha256_file(destination)))
    parent['sources'].append(dict(path=str(destination.relative_to(pending)), sha256=sha256_file(destination),
                                  url='', kind='opening_margin_boundary_input'))
    paths = {s['path'] for s in parent['sources']}
    parent['sources'].extend(s for s in archive.sources.values() if s['path'] not in paths)
    atomic_write_parquet(pending / 'margin_level_intervals.parquet', rebuilt)
    parent['outputs']['margin_level_intervals.parquet']['sha256'] = sha256_file(pending / 'margin_level_intervals.parquet')
    parent['changed_products'] = sorted(set(parent['changed_products']) | {r['product'] for r in boundaries})
    atomic_write_json(parent_path, parent)
    proof = dict(status='source_bound_opening_margin_boundary_staged', boundaries=boundaries,
        parent_pending_manifest_sha256=parent_sha, pending_manifest_sha256=sha256_file(parent_path),
        review_sha256=sha256_file(destination), parent_manifest_receipt_sha256=sha256_file(before),
        builder_sha256=sha256_file(Path(__file__)),
        financial_values_inferred=False, accounting_rows_recompiled=0, current_training_source_overwritten=False)
    atomic_write_json(receipt, proof)
    archive.conn.close()
    print(json.dumps(proof), flush=True)
    return proof


def compose_bounded_margin_amendments(current, facts, levels, amendments, *, archive):
    """Join separately diagnosed product gaps without discarding accepted repairs.

    Each addition keeps its own source-bound blocker worklist. Its changed
    products must still equal the original parent's state in the existing
    delta: overlapping accepted changes require an explicit combined repair.
    """
    if not amendments:
        return facts, levels, []
    base, parent = read_bound_output(current/'margin_level_intervals.parquet')
    parent_sha = sha256_file(current/'manifest.json')
    evidence = []
    consumed = set()
    def states(frame, product):
        return {json.dumps(row, sort_keys=True, default=str, ensure_ascii=False)
                for row in frame.filter(pl.col('product')==product).select(base.columns).to_dicts()}
    for delta, worklist in amendments:
        incoming, proof = read_bound_output(delta/'margin_level_intervals.parquet')
        incoming_facts, _ = read_bound_output(delta/'margin_event_candidates.parquet')
        if (proof.get('status')!='bounded_margin_restoration_sources_prepared'
                or proof.get('parent_manifest_sha256')!=parent_sha
                or proof.get('worklist_sha256')!=sha256_file(worklist)
                or proof.get('financial_values_inferred') is not False):
            raise ValueError('bounded margin amendment changes its verified source/worklist parent')
        gaps = pl.read_csv(worklist, try_parse_dates=True, schema_overrides={'contract':pl.String})
        gap_products = set(gaps.filter(pl.col('missing_opening_margin')
            | pl.col('missing_settlement_margin'))['product'])
        # A source-bound OCR correction can name a destination outside the old
        # blocker list. Re-prove that exact original cell and its corrected
        # numeric owner; a bare changed_products entry cannot enlarge scope.
        review_hashes={r['sha256'] for r in proof.get('margin_code_reviews',[])}
        if review_hashes:
            original_facts,_=read_bound_output(current/'margin_event_candidates.parquet')
            old_rows=original_facts.to_dicts();new_rows=incoming_facts.to_dicts()
            bound_reviews={s['sha256']:s for s in proof['sources']
                if s['kind']=='visual_margin_code_review'}
            if not review_hashes<=bound_reviews.keys():
                raise ValueError('margin code scope lacks its retained review')
            additions=set()
            for digest in sorted(review_hashes):
                item=bound_reviews[digest];path=delta/item['path']
                if not path.resolve().is_relative_to(delta.resolve()) or sha256_file(path)!=digest:
                    raise ValueError('margin code scope review SHA/path mismatch')
                review=json.loads(path.read_text())
                if review.get('review_kind')!='source_bound_margin_product_code_cells' or not review.get('cells'):
                    raise ValueError('margin code scope requires exact reviewed cells')
                for cell in review['cells']:
                    if cell['ocr_product'] not in gap_products or cell['product']==cell['ocr_product']:
                        raise ValueError('margin code scope exceeds the original gap products')
                    fields=('margin_kind','after','before')
                    def matches(row,product):
                        return (row['product']==product and row['source_url']==cell['source_url']
                            and row['source_content_sha256']==cell['content_sha256']
                            and all(row.get(f)==cell[f] for f in fields))
                    if (not any(matches(r,cell['ocr_product']) for r in old_rows)
                            or not any(matches(r,cell['product']) for r in new_rows)
                            or archive.document(cell['source_url'])['content_sha256']!=cell['content_sha256']):
                        raise ValueError('margin code scope original/corrected amount cells disagree')
                    images=[s for s in proof['sources'] if s['sha256']==cell['page_image_sha256']
                        and s['url']==cell['source_url'] and s['kind'].startswith('visual_margin_')]
                    if not images or not isinstance(cell['page'],int) or cell['page']<=0:
                        raise ValueError('margin code scope lacks its exact inspected image')
                    for image in images:
                        p=delta/image['path']
                        if not p.resolve().is_relative_to(delta.resolve()) or sha256_file(p)!=image['sha256']:
                            raise ValueError('margin code scope image SHA/path mismatch')
                    additions.add(cell['product'])
            gap_products.update(additions)
        products = set(proof.get('changed_products', []))
        actual = {p for p in set(base['product'])|set(incoming['product'])
                  if states(base,p)!=states(incoming,p)}
        if not products or actual!=products or not products<=gap_products or products&consumed:
            raise ValueError('bounded margin amendment exceeds its exact changed/gap scope')
        if any(states(levels,p)!=states(base,p) for p in products):
            raise ValueError('bounded margin amendment overlaps an already accepted product repair')
        for relative, item in proof['outputs'].items():
            path = delta/relative
            if not path.resolve().is_relative_to(delta.resolve()) or sha256_file(path)!=item['sha256']:
                raise ValueError('bounded margin amendment output SHA/path mismatch')
            archive.copy(path,item['sha256'],url='',kind='bounded_margin_amendment_output')
        for item in proof['sources']:
            path = delta/item['path']
            if not path.resolve().is_relative_to(delta.resolve()):
                raise ValueError('bounded margin amendment source escapes its bundle')
            archive.copy(path,item['sha256'],url=item['url'],kind=item['kind'])
        manifest_path = archive.copy(delta/'manifest.json',sha256_file(delta/'manifest.json'),
                                     url='',kind='bounded_margin_amendment_manifest')
        worklist_path = archive.copy(worklist,sha256_file(worklist),url='',kind='bounded_margin_amendment_worklist')
        keep = ~pl.col('product').is_in(sorted(products))
        take = pl.col('product').is_in(sorted(products))
        facts = pl.concat([facts.filter(keep),incoming_facts.filter(take)],how='diagonal_relaxed')
        levels = pl.concat([levels.filter(keep),incoming.filter(take).select(base.columns)],how='vertical')
        evidence.append(dict(manifest_path=manifest_path,manifest_sha256=sha256_file(delta/'manifest.json'),
            worklist_path=worklist_path,worklist_sha256=sha256_file(worklist),products=sorted(products)))
        consumed.update(products)
    return facts, levels, evidence


def compose_bounded_corporate_amendments(current, facts, amendments, *, archive):
    """Add re-read original views with their own exact parent and worklist.

    A corporate diagnosis need not share a margin diagnosis's worklist. It
    must preserve every original candidate, bind consumed source bytes, and
    add views only for the explicitly requested corporate products. Inspected
    cell replacement and financial conflict resolution stay with the caller.
    """
    if not amendments:
        return facts, []
    original, parent = read_bound_output(current/'corporate_event_candidates.parquet')
    parent_sha = sha256_file(current/'manifest.json')
    sources = {s['path']: s for s in parent['sources']}
    def identity(value):
        # Diagonal concatenation can promote an exact integer to float. Python
        # numeric equality preserves that value; serialized text does not.
        if isinstance(value,dict):
            return tuple(sorted((k,identity(v)) for k,v in value.items() if v is not None))
        if isinstance(value,list):
            return tuple(identity(v) for v in value)
        return value
    retained = {identity(r) for r in original.to_dicts()}
    evidence = []
    for delta, worklist in amendments:
        incoming, proof = read_bound_output(delta/'corporate_event_candidates.parquet')
        if (proof.get('status') != 'bounded_corporate_source_views_prepared'
                or proof.get('parent_manifest_sha256') != parent_sha
                or proof.get('worklist_sha256') != sha256_file(worklist)
                or proof.get('financial_values_inferred') is not False):
            raise ValueError('bounded corporate amendment changes its verified source/worklist parent')
        rows = incoming.to_dicts()
        if not retained <= {identity(r) for r in rows}:
            raise ValueError('corporate amendment discarded original candidate evidence')
        added = [r for r in rows if identity(r) not in retained]
        gaps = pl.read_csv(worklist,try_parse_dates=True,schema_overrides={'contract':pl.String})
        scope = set(gaps.filter(pl.col('missing_adjusted_terms') | pl.col('unresolved_lifetime')
                              | pl.col('lost_inventory_continuation'))['product'])
        if any(r.get('product') not in scope and r.get('from_product') not in scope for r in added):
            raise ValueError('corporate amendment enlarged its diagnosed product scope')
        for name, digest in proof['source_sha256s'].items():
            source = sources.get(name)
            path = current/name
            if (source is None or source['sha256'] != digest
                    or not path.resolve().is_relative_to(current.resolve())
                    or sha256_file(path) != digest):
                raise ValueError('corporate amendment source SHA/path mismatch')
            archive.copy(path,digest,url=source.get('url',''),kind=source['kind'])
        for source in proof.get('sources',[]):
            path=delta/source['path']
            if (not path.resolve().is_relative_to(delta.resolve())
                    or sha256_file(path)!=source['sha256']):
                raise ValueError('corporate amendment extraction source SHA/path mismatch')
            archive.copy(path,source['sha256'],url=source.get('url',''),kind=source['kind'])
        for path,kind in [(delta/'manifest.json','bounded_corporate_amendment_manifest'),
                          (worklist,'bounded_corporate_amendment_worklist')]:
            archive.copy(path,sha256_file(path),url='',kind=kind)
        if added:
            facts = pl.concat([facts,pl.from_dicts(added,infer_schema_length=None)],
                              how='diagonal_relaxed').unique(maintain_order=True)
        evidence.append(dict(manifest_sha256=sha256_file(delta/'manifest.json'),
            worklist_sha256=sha256_file(worklist),added_views=len(added),
            source_failures=proof.get('source_failures',[]),products=sorted(scope)))
    return facts,evidence


def stage_rule_repairs(current: Path, raw_source: Path, corporate_delta: Path, margin_delta: Path,
                       corporate_review: Path, output: Path, receipt: Path, *, archive_root: Path,
                       refresh_pending: bool = False, additional_corporate_reviews=(), margin_amendments=(),
                       corporate_amendments=()):
    """One pending source delta; the current training release stays immutable."""
    from stockagent.data.tw_futures_margin_preparation import (
        RuleArchive, corporate_terms_intervals, corporate_identity_boundaries,
        stock_futures_cash_reform_intervals,
    )
    from scripts.build_tw_futures_margin_event_candidates import (
        corporate_source_review_candidates, replace_reviewed_corporate_facts,
        replace_reviewed_product_facts, require_reviewed_small_unit_intervals,
    )

    parent_sha = sha256_file(current / 'manifest.json')
    retained = None
    code_paths = (Path(__file__), ROOT / 'stockagent/data/tw_futures_margin_preparation.py',
                  ROOT / 'scripts/build_tw_futures_margin_event_candidates.py')
    code_hashes = {str(p): sha256_file(p) for p in code_paths}
    if sha256_file(raw_source / 'rules/manifest.json') != parent_sha:
        raise ValueError('rule workspace and retained raw release disagree')
    if output.exists() and any(output.iterdir()):
        retained = json.loads((output / 'manifest.json').read_text())
        if (not refresh_pending or retained.get('status') != 'pending_source_bound_rule_delta'
                or retained.get('parent_manifest_sha256') != parent_sha
                or retained.get('parent_source_manifest_sha256') != sha256_file(raw_source / 'source_manifest.json')):
            raise FileExistsError('only the same unpromoted source delta may be explicitly refreshed')
    output.mkdir(parents=True, exist_ok=True)
    old_cf, parent = read_bound_output(current / 'corporate_event_candidates.parquet')
    from stockagent.data.tw_futures_margin_preparation import corporate_identity_views
    old_selected, _ = corporate_identity_views(old_cf.to_dicts())
    cf, corporate_proof = read_bound_output(corporate_delta / 'corporate_event_candidates.parquet')
    margins, margin_proof = read_bound_output(margin_delta / 'margin_level_intervals.parquet')
    margin_facts, _ = read_bound_output(margin_delta / 'margin_event_candidates.parquet')
    for delta in (corporate_proof, margin_proof):
        if delta.get('parent_manifest_sha256') != parent_sha:
            raise ValueError('source delta belongs to a different maintained rules parent')
    if corporate_proof['worklist_sha256'] != margin_proof['worklist_sha256']:
        raise ValueError('corporate and margin deltas target different blocker worklists')
    for path, digest in corporate_proof['source_sha256s'].items():
        if sha256_file(current / path) != digest:
            raise ValueError('corporate retained source changed after preparation')
    archive = RuleArchive(archive_root, output)
    cf, corporate_additions = compose_bounded_corporate_amendments(
        current,cf,corporate_amendments,archive=archive)
    margin_facts, margins, margin_additions = compose_bounded_margin_amendments(
        current, margin_facts, margins, margin_amendments, archive=archive)
    opening_reviews = retained.get('opening_margin_boundary_reviews', []) if retained is not None else []
    opening_payloads = []
    for review in opening_reviews:
        path = output / review['path']
        if not path.resolve().is_relative_to(output.resolve()) or sha256_file(path) != review['sha256']:
            raise ValueError('retained opening margin review changed')
        opening_payloads.append(json.loads(path.read_text()))
    if opening_payloads:
        for source in retained['sources']:
            if source.get('url') in {r['source_url'] for p in opening_payloads for r in p['cases']}:
                archive.copy(output / source['path'], source['sha256'], url=source['url'], kind=source['kind'])
        margins, _ = before_margin_opening_boundaries(margin_facts, margins, opening_payloads, archive=archive)
    reviewed_cf, reviewed_positions, _ = corporate_source_review_candidates(archive, corporate_review)
    # Re-reading retained OCR/text views must not revive an extraction that
    # the parent's inspected cells already replaced. The bound parent facts
    # retain their exact review, event and financial values; new inspected
    # reviews are applied afterwards and can explicitly replace them.
    inherited_reviews = [r for r in old_cf.to_dicts()
        if r.get('extraction') == 'source_bound_visual_corporate_cells'
        or (r.get('extraction_method') or '').startswith('source_bound_visual')]
    by_sha = {s['sha256']: s for s in parent['sources']}
    for digest in {r.get('visual_review_sha256') for r in inherited_reviews}:
        retained_review = by_sha.get(digest)
        if not retained_review or sha256_file(current / retained_review['path']) != digest:
            raise ValueError('inherited corporate cells lack their bound inspected review')
    corporate = replace_reviewed_corporate_facts(cf.to_dicts(), inherited_reviews)
    corporate = replace_reviewed_corporate_facts(corporate, reviewed_cf)
    for review in additional_corporate_reviews:
        extra_cf,extra_positions,_=corporate_source_review_candidates(archive,review)
        corporate=replace_reviewed_corporate_facts(corporate,extra_cf)
        reviewed_positions.extend(extra_positions)
    terms, term_issues = corporate_terms_intervals(corporate)
    units, unit_issues = corporate_terms_intervals(corporate, unit_only=True)
    require_reviewed_small_unit_intervals(corporate,[*terms,*units])
    reform = json.loads((current / 'historical_cash_reform.json').read_text())
    terms, reform_issues = stock_futures_cash_reform_intervals(terms, reform, reform['cash_origins'])
    positions, _ = read_bound_output(current / 'position_event_candidates.parquet')
    old_position_levels, _ = read_bound_output(current / 'position_level_intervals.parquet')
    from stockagent.data.tw_futures_margin_preparation import position_candidate_intervals
    position_facts = replace_reviewed_product_facts(positions.to_dicts(), reviewed_positions)
    position_levels, position_issues = position_candidate_intervals(position_facts)
    frames = dict(corporate_event_candidates=pl.from_dicts(corporate, infer_schema_length=None),
        corporate_terms_intervals=pl.from_dicts(terms, infer_schema_length=None),
        corporate_unit_intervals=pl.from_dicts(units, infer_schema_length=None),
        margin_event_candidates=margin_facts, margin_level_intervals=margins,
        position_event_candidates=pl.from_dicts(position_facts, infer_schema_length=None),
        position_level_intervals=pl.from_dicts(position_levels, infer_schema_length=None))
    changed = set()
    retained_identity_keys = {json.dumps(r, sort_keys=True, default=str) for r in old_selected}
    for row in old_cf.iter_rows(named=True):
        if json.dumps(row, sort_keys=True, default=str) not in retained_identity_keys:
            changed.update(p for p in (row.get('product'), row.get('from_product')) if p)
    changed_outputs = {}
    for name in ('corporate_terms_intervals', 'corporate_unit_intervals', 'margin_level_intervals',
                 'position_level_intervals'):
        old, _ = read_bound_output(current / (name + '.parquet'))
        new = frames[name]
        # New optional provenance fields do not change an unrelated product's
        # accepted financial state. Compare every retained state column.
        columns = old.columns
        if not set(columns) <= set(new.columns):
            raise ValueError('repaired intervals dropped a retained state field')
        products = set(old['product']) | set(new['product'])
        def states(frame, product):
            return {json.dumps(r, sort_keys=True, default=str, ensure_ascii=False)
                    for r in frame.filter(pl.col('product') == product).select(columns).to_dicts()}
        affected = sorted(p for p in products if states(old, p) != states(new, p))
        changed.update(affected)
        changed_outputs[name] = affected
        # Exact retained states for unaffected products are kept byte-for-value.
        frames[name] = pl.concat([old.filter(~pl.col('product').is_in(affected)),
            new.filter(pl.col('product').is_in(affected)).select(columns)], how='vertical')
    _, before_edges = corporate_identity_boundaries(old_cf)
    _, after_edges = corporate_identity_boundaries(frames['corporate_event_candidates'])
    edge_columns = before_edges.columns
    old_edges = {json.dumps(r, sort_keys=True, default=str) for r in before_edges.to_dicts()}
    new_edges = {json.dumps(r, sort_keys=True, default=str) for r in after_edges.select(edge_columns).to_dicts()}
    for encoded in old_edges ^ new_edges:
        row = json.loads(encoded)
        changed.update(row[k] for k in ('product', 'corporate_transfer_target') if row.get(k))
    parent_hashes = {s['sha256'] for s in parent['sources']}
    for source in margin_proof['sources']:
        path = margin_delta / source['path']
        if sha256_file(path) != source['sha256']:
            raise ValueError('margin delta source changed after preparation')
        if source['sha256'] not in parent_hashes:
            archive.copy(path, source['sha256'], url=source['url'], kind=source['kind'])
    # Do not silently discard a previously accepted legal-value input during
    # a later rule refresh. Check just its own corporate episode before all
    # financial output writes; unrelated repairs need no repeated review.
    information = retained.get('information_halt_source_delta') if retained is not None else None
    if information is not None:
        path = output / information['path'] / 'manifest.json'
        if (not path.resolve().is_relative_to(output.resolve()) or sha256_file(path) != information['sha256']):
            raise ValueError('retained information-halt input composition changed')
        from stockagent.data.tw_futures_information_halt import (
            load_information_halt_review, validate_information_halt_corporate_scope,
        )
        information_manifest = json.loads(path.read_text())
        for item in information_manifest['reviews']:
            review_path = path.parent / item['path']
            if sha256_file(review_path) != item['sha256']:
                raise ValueError('retained information-halt review changed')
            validate_information_halt_corporate_scope(frames['corporate_event_candidates'],
                load_information_halt_review(review_path))
    loss = retained.get('loss_reduction_halt_source_delta') if retained is not None else None
    if loss is not None:
        path = output / loss['path'] / 'manifest.json'
        if not path.resolve().is_relative_to(output.resolve()) or sha256_file(path) != loss['sha256']:
            raise ValueError('retained loss-reduction input composition changed')
        from stockagent.data.tw_futures_margin_halt_sources import load_loss_reduction_bundle
        load_loss_reduction_bundle(raw_source, output / 'corporate_event_candidates.parquet', path.parent,
                                  corporate_frame=frames['corporate_event_candidates'])
    for name, frame in frames.items():
        atomic_write_parquet(output / (name + '.parquet'), frame)
    for name, value in dict(corporate_term_issues=term_issues + reform_issues,
                            corporate_unit_issues=unit_issues, position_interval_issues=position_issues).items():
        atomic_write_json(output / (name + '.json'), value)
    if any(sha256_file(p) != digest for p, digest in code_hashes.items()):
        raise ValueError('rule preparation code changed during staging; no acceptance receipt')
    proof = dict(status='pending_source_bound_rule_delta',
        parent_manifest_sha256=parent_sha,
        parent_source_manifest_sha256=sha256_file(raw_source / 'source_manifest.json'),
        corporate_delta_manifest_sha256=sha256_file(corporate_delta / 'manifest.json'),
        margin_delta_manifest_sha256=sha256_file(margin_delta / 'manifest.json'),
        corporate_review_sha256=sha256_file(corporate_review),
        additional_corporate_review_sha256s=[sha256_file(p) for p in additional_corporate_reviews],
        preparation_code_sha256s=code_hashes,
        inherited_visual_review_sha256s=sorted({r['visual_review_sha256'] for r in inherited_reviews}),
        terminal_formula_revision_source_sha256s=sorted({r['source_content_sha256'] for r in reviewed_cf
            if r.get('event_role') == 'terminal_formula_revision_only'}),
        worklist_sha256=corporate_proof['worklist_sha256'],
        bounded_margin_amendments=margin_additions,
        bounded_corporate_amendments=corporate_additions,
        changed_products=sorted(changed), changed_interval_products=changed_outputs,
        sources=list(archive.sources.values()),
        outputs={p.name: dict(sha256=sha256_file(p)) for p in output.iterdir()
                 if p.is_file() and p.suffix in ('.parquet', '.json') and p.name != 'manifest.json'},
        financial_values_inferred=False, accounting_rows_recompiled=0,
        current_training_source_overwritten=False, builder_sha256=sha256_file(Path(__file__)))
    if retained is not None and retained.get('terminal_source_delta') is not None:
        terminal = retained['terminal_source_delta']
        path = output / terminal['path'] / 'manifest.json'
        if (not path.resolve().is_relative_to(output.resolve()) or sha256_file(path) != terminal['sha256']):
            raise ValueError('retained terminal input composition changed during rule staging')
        proof['terminal_source_delta'] = terminal
        proof['outputs'][str(path.relative_to(output))] = dict(sha256=terminal['sha256'])
        proof['changed_products'] = sorted(set(proof['changed_products']) | set(terminal['products']))
    if information is not None:
        proof['information_halt_source_delta'] = information
        proof['changed_products'] = sorted(set(proof['changed_products']) | set(information['products']))
        proof['sources'].extend(s for s in retained['sources']
                               if s.get('kind') == 'information_halt_input')
    if loss is not None:
        proof['loss_reduction_halt_source_delta'] = loss
        proof['changed_products'] = sorted(set(proof['changed_products']) | set(loss['products']))
        proof['sources'].extend(s for s in retained['sources'] if s.get('kind') == 'loss_reduction_halt_input')
    if opening_reviews:
        proof['opening_margin_boundary_reviews'] = opening_reviews
        proof['sources'].extend(s for s in retained['sources']
                               if s.get('kind') == 'opening_margin_boundary_input')
    atomic_write_json(output / 'manifest.json', proof)
    atomic_write_json(receipt, proof)
    archive.conn.close()
    print(json.dumps(dict(status=proof['status'], changed_products=proof['changed_products'])), flush=True)


def repair(current: Path, amendments: Path, receipt: Path):
    before=json.loads((current/'manifest.json').read_text())
    facts,_=read_bound_output(current/'margin_event_candidates.parquet')
    old,_=read_bound_output(current/'margin_level_intervals.parquet')
    patch,proof=read_bound_output(amendments/'margin_event_candidates.parquet')
    # A reviewed notice/product replaces that notice's rejected extraction;
    # unrelated notices and products remain intact.
    keys=['product','source_content_sha256']
    combined=pl.concat([facts.join(patch.select(keys).unique(),on=keys,how='anti'),patch],
        how='diagonal_relaxed').unique(maintain_order=True)
    rows,issues=margin_candidate_intervals(combined.to_dicts())
    rows,ratio_issues=compose_margin_ratio_intervals(rows,
        json.loads((current/'dated_margin_ratio_laws.json').read_text()))
    rebuilt=pl.DataFrame(rows,schema=old.schema)
    changed=sorted(set(old['product'])|set(rebuilt['product']))
    order=['product','effective_date','effective_phase']
    changed=[p for p in changed if not old.filter(pl.col('product')==p).sort(order).equals(
        rebuilt.filter(pl.col('product')==p).sort(order))]
    report=dict(status='source_fact_margin_intervals_repaired',changed_products=changed,
        before_manifest_sha256=sha256_file(current/'manifest.json'),
        amendment_manifest_sha256=sha256_file(amendments/'manifest.json'),
        raw_market_observations_changed=False,financial_values_inferred=False,
        new_provider_requests=0,new_ocr_jobs=0,new_accounting_builds=0,
        prior_interval_rows=old.height,repaired_interval_rows=rebuilt.height)
    args=SimpleNamespace(output_dir=current,base_candidates=None,update_current=True)
    pending=current.parent/'.rule-facts-pending'
    resume=None
    if pending.exists():
        retained=json.loads((pending/'margin_interval_repair.json').read_text())
        assert retained['before_manifest_sha256']==report['before_manifest_sha256']
        resume=report['before_manifest_sha256']
    with maintained_rule_build(args,repo_root=ROOT,rebuild_interval_kinds=('margin',),
                              resume_pending_parent_sha256=resume) as a:
        if resume is None:
            shutil.copytree(current,a.output_dir,dirs_exist_ok=True,copy_function=os.link)
        sources={r['path']:r for r in before['sources']}
        for row in proof['sources']:
            src=amendments/row['path'];assert sha256_file(src)==row['sha256']
            relative=Path('sources/margin-repair')/Path(row['path'])
            target=a.output_dir/relative;target.parent.mkdir(parents=True,exist_ok=True)
            if not target.exists():os.link(src,target)
            assert sha256_file(target)==row['sha256']
            sources[str(relative)]=dict(row,path=str(relative))
        for filename,frame in [('margin_event_candidates.parquet',combined),('margin_level_intervals.parquet',rebuilt)]:
            atomic_write_parquet(a.output_dir/filename,frame)
        atomic_write_json(a.output_dir/'margin_interval_issues.json',issues+ratio_issues)
        atomic_write_json(a.output_dir/'margin_interval_repair.json',report)
        names=['margin_event_candidates.parquet','margin_level_intervals.parquet',
               'margin_interval_issues.json','margin_interval_repair.json']
        result=dict(before,sources=list(sources.values()),margin_level_intervals=rebuilt.height,
                    margin_interval_issues=len(issues+ratio_issues),margin_interval_repair=report)
        result['outputs']=dict(before['outputs'],**{n:dict(sha256=sha256_file(a.output_dir/n)) for n in names})
        atomic_write_json(a.output_dir/'manifest.json',result)
    report['after_manifest_sha256']=sha256_file(current/'manifest.json')
    atomic_write_json(receipt,report);print(json.dumps(report),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--current',type=Path,required=True)
    p.add_argument('--amendments',type=Path)
    p.add_argument('--receipt',type=Path,required=True)
    p.add_argument('--corporate-worklist', type=Path)
    p.add_argument('--corporate-output', type=Path)
    p.add_argument('--product-universe', type=Path)
    p.add_argument('--announcements', type=Path)
    p.add_argument('--numeric-repairs', action='store_true',
                   help='Include source-bound numeric anomalies in the bounded corporate repair scope')
    p.add_argument('--margin-worklist', type=Path)
    p.add_argument('--margin-output', type=Path)
    p.add_argument('--margin-product', action='append', default=[],
                   help='Restrict event repairs to these retained gap products; keep all other rules')
    p.add_argument('--continue-margin-delta', type=Path,
                   help='Continue a verified unpromoted delta without discarding earlier affected-product repairs')
    p.add_argument('--restoration-evidence', type=Path, action='append', default=[],
                   help='Additional byte-bound cash/disposition evidence for the exact parent worklist')
    p.add_argument('--margin-source-review', type=Path, action='append', default=[],
                   help='Inspected original margin cells/clocks for only the gap-owning products')
    p.add_argument('--margin-code-review', type=Path, action='append', default=[],
                   help='Exact source/page/amount-bound OCR product corrections for retained gaps')
    p.add_argument('--seasonal-continuation', nargs=2, action='append', default=[],
                   metavar=('RETAINED_URL','PUBLICATION_DATE'),
                   help='An explicit unchanged seasonal-margin notice and its retained publication date')
    p.add_argument('--inherited-margin-rules', type=Path,
                   help='Exact original rules parent supplying ratio laws omitted by an integrated delta')
    p.add_argument('--reextract-margin-sources', action='store_true',
                   help='Re-read only retained gap-owning source tables with the maintained parser')
    p.add_argument('--reconcile-reused-margin-attachments', action='store_true',
                   help='Quarantine backdated URL views only with an independently bound identical event')
    p.add_argument('--stage-output', type=Path)
    p.add_argument('--terminal-review', type=Path, help='One complete inspected deadline fixing for the same pending source delta')
    p.add_argument('--information-halt-bundle', type=Path,
                   help='Native official information-halt inputs for the same pending source delta')
    p.add_argument('--loss-reduction-halt-bundle', type=Path,
                   help='Additional bound loss-reduction episodes for the same pending source delta')
    p.add_argument('--opening-margin-review', type=Path,
                   help='Literal before amount for only its own preannounced opening session')
    p.add_argument('--raw-source', type=Path)
    p.add_argument('--corporate-delta', type=Path)
    p.add_argument('--margin-delta', type=Path)
    p.add_argument('--margin-amendment-delta',type=Path,action='append',default=[],
                   help='Additional disjoint source-bound margin repair in the same staged rule bundle')
    p.add_argument('--margin-amendment-worklist',type=Path,action='append',default=[],
                   help='Exact blocker worklist for each additional margin delta, in the same order')
    p.add_argument('--corporate-source-review', type=Path)
    p.add_argument('--additional-corporate-source-review',type=Path,action='append',default=[],
                   help='Additional inspected numeric cells for the same unpromoted source delta')
    p.add_argument('--refresh-pending', action='store_true')
    p.add_argument('--archive-root', type=Path, default=Path('data_taifex_public_history/rules'))
    a=p.parse_args()
    if (len(a.margin_amendment_delta)!=len(a.margin_amendment_worklist)
            or a.margin_amendment_delta and (not a.stage_output or not a.corporate_delta or not a.margin_delta)):
        p.error('bounded margin amendments require paired deltas/worklists in the existing rule staging mode')
    if a.numeric_repairs and not a.corporate_worklist:
        p.error('--numeric-repairs requires the bounded --corporate-worklist mode')
    if a.additional_corporate_source_review and not a.stage_output:
        p.error('additional inspected corporate cells require --stage-output')
    if (a.restoration_evidence or a.margin_source_review or a.margin_code_review or a.reextract_margin_sources
            or a.seasonal_continuation or a.inherited_margin_rules or a.reconcile_reused_margin_attachments
            or a.margin_product or a.continue_margin_delta) and not a.margin_worklist:
        p.error('additional margin sources require the bounded --margin-worklist preparation mode')
    if a.opening_margin_review:
        if (not all((a.raw_source, a.stage_output)) or a.information_halt_bundle or a.loss_reduction_halt_bundle or a.terminal_review
                or a.amendments or a.margin_worklist or a.corporate_worklist or a.corporate_delta
                or a.margin_delta or a.corporate_source_review):
            p.error('opening margin staging requires --raw-source, --stage-output and --opening-margin-review only')
        stage_opening_margin_boundary_repair(a.raw_source, a.stage_output, a.opening_margin_review,
            a.receipt, archive_root=a.archive_root)
    elif a.information_halt_bundle:
        if (not all((a.raw_source, a.stage_output)) or a.loss_reduction_halt_bundle or a.terminal_review or a.amendments
                or a.margin_worklist or a.corporate_worklist or a.corporate_delta
                or a.margin_delta or a.corporate_source_review):
            p.error('information-halt staging requires --raw-source, --stage-output and --information-halt-bundle only')
        stage_information_halt_repair(a.raw_source, a.stage_output, a.information_halt_bundle, a.receipt)
    elif a.loss_reduction_halt_bundle:
        if (not all((a.raw_source, a.stage_output)) or a.terminal_review or a.amendments
                or a.margin_worklist or a.corporate_worklist or a.corporate_delta
                or a.margin_delta or a.corporate_source_review):
            p.error('loss-reduction staging requires --raw-source, --stage-output and --loss-reduction-halt-bundle only')
        stage_loss_reduction_halt_repair(a.raw_source, a.stage_output, a.loss_reduction_halt_bundle, a.receipt,
                                        refresh_pending=a.refresh_pending)
    elif a.terminal_review:
        if (not all((a.raw_source, a.stage_output)) or a.amendments or a.margin_worklist or a.corporate_worklist
                or a.corporate_delta or a.margin_delta or a.corporate_source_review):
            p.error('terminal operand staging requires --raw-source, --stage-output and --terminal-review only')
        stage_terminal_operand_repair(a.raw_source, a.stage_output, a.terminal_review, a.receipt)
    elif a.stage_output:
        if not all((a.raw_source, a.corporate_delta, a.margin_delta, a.corporate_source_review)):
            p.error('staging requires the retained raw source and both deltas plus reviewed corporate cells')
        if a.amendments or a.margin_worklist or a.corporate_worklist:
            p.error('stage an already prepared delta without another preparation/promotion mode')
        stage_rule_repairs(a.current, a.raw_source, a.corporate_delta, a.margin_delta,
            a.corporate_source_review, a.stage_output, a.receipt, archive_root=a.archive_root,
            refresh_pending=a.refresh_pending,additional_corporate_reviews=a.additional_corporate_source_review,
            margin_amendments=list(zip(a.margin_amendment_delta,a.margin_amendment_worklist)))
    elif a.margin_worklist:
        if not a.margin_output or a.corporate_worklist or a.amendments:
            p.error('bounded margin preparation requires --margin-output only')
        prepare_margin_repairs(a.current, a.margin_worklist, a.margin_output, a.receipt,
                              archive_root=a.archive_root, restoration_evidence=a.restoration_evidence,
                              source_reviews=a.margin_source_review, reextract_sources=a.reextract_margin_sources,
                              selected_products=a.margin_product, continue_delta=a.continue_margin_delta,
                              code_reviews=a.margin_code_review, seasonal_continuations=a.seasonal_continuation,
                              inherited_rules=a.inherited_margin_rules,
                              reconcile_reused_attachments=a.reconcile_reused_margin_attachments)
    elif a.corporate_worklist:
        if not a.corporate_output or a.amendments:
            p.error('corporate repair requires --corporate-output and no --amendments')
        prepare_corporate_repairs(a.current,a.corporate_worklist,a.corporate_output,a.receipt,
            product_universe=a.product_universe, announcements=a.announcements,numeric_repairs=a.numeric_repairs)
    else:
        if not a.amendments:p.error('margin repair requires --amendments')
        repair(a.current,a.amendments,a.receipt)
