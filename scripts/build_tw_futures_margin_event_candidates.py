#!/usr/bin/env python3
"""Compile hash-bound before/after facts without claiming complete rule history."""
from __future__ import annotations
import argparse
from datetime import date, datetime, timedelta, timezone
import json
import math
from pathlib import Path
import re
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import polars as pl
from downloader.artifact_io import atomic_write_json,atomic_write_parquet,sha256_file
from downloader.taifex_rule_parsing import PARSER_VERSION,temporal_mentions
from stockagent.data.tw_futures_margin_preparation import (
    RuleArchive,check_publication,margin_effective,margin_table_candidates,margin_grid_candidates,timestamp,
    compact,roc_date,ROC_DATE,TAIPEI,position_grid_candidates,corporate_grid_candidates,
    margin_legacy_word_levels,position_legacy_word_candidates,corporate_text_candidates,
    corporate_native_table_candidates,corporate_legacy_word_candidates,
    corporate_position_legacy_word_candidates,
    corporate_terms_intervals, margin_candidate_intervals, corporate_position_table_candidates,
    compose_margin_ratio_intervals,
    corporate_position_text_candidates,
    stock_futures_cash_reform_intervals,
    position_prose_candidates,position_candidate_intervals,
    disposal_margin_restorations,
)


def operative_notice_year_context(text: str, *, publication_bound: bool) -> str:
    """Exclude only numbered, quoted legal citations from a short-date year."""
    dense=compact(text)
    return re.sub(r'(?:依(?:本公司)?|依據(?:本公司)?)'+ROC_DATE+
        r'[臺台]期(?:監|結|交)字第?\d{9,12}號函[「『][^」』]{1,500}[」』]',
        '',dense) if publication_bound else dense


def candidate_notice_clock(text: str, published: str) -> dict:
    """Keep product-relative phases and conditional restoration as evidence."""
    issue_bound=False;errors=[]
    press_masthead=(r'[臺台]灣期貨交易所新聞稿(?:部門[:：](?:結算部|交易部|企劃部))?'
                    r'(?:中華民國)?'+ROC_DATE)
    try:
        check_publication(text,published);issue_bound=True
    except ValueError as exc:
        # Official press releases use a dated masthead, not 發文日期.
        dense=compact(text)
        mastheads=list(re.finditer(press_masthead,dense))
        mastheads.extend(re.finditer(r'[臺台]灣期貨交易所於'+ROC_DATE+r'公告',dense))
        issued={roc_date(m) for m in mastheads}
        explicit_publications={m['date_iso'] for m in temporal_mentions(text) if m['role']=='publication'}
        if (not explicit_publications and len(issued)==1
                and 0 <= (date.fromisoformat(published)-next(iter(issued))).days <= 7):
            issue_bound=True
        else: errors.append(str(exc))
    # A verified press masthead is a publication date even if the following
    # prose mentions implementation within the parser's look-ahead window.
    # Mark only that exact masthead, not other dates in the notice's body.
    clock_text=re.sub(press_masthead,lambda m:f'發文日期:{m[1]}年{m[2]}月{m[3]}日',
                      compact(text)) if issue_bound else text
    mentions=temporal_mentions(clock_text)
    ends=[m for m in mentions if m['role']=='effective_end']
    # A replacement notice may revoke a specifically dated older notice at
    # its own start. That clause terminates the older notice, not this one.
    # Require the prior publication, literal notice number and issuing date;
    # keep every other revocation as an unresolved/current endpoint.
    revocations=[]
    issued={m['date_iso'] for m in mentions if m['role']=='publication'}
    dense=compact(text)
    current_numbers=set(re.findall(r'發文字號:[臺台]期(?:結|交)字第(\d{9,12})號',dense))
    if issue_bound and len(issued)==1:
        for match in re.finditer(r'(?:原(?:本公司(?:於)?)?|本公司(?:於)?)'+ROC_DATE+
                r'(?:公告之)?[臺台]期(?:結|交)字第(\d{9,12})號函'
                r'(?:(?:有關|公告)[^。;；,，]{1,60}之規定)?[,，]自'+ROC_DATE+
                r'[^。;；]{0,50}(?:停止適用|廢止)',dense):
            prior_day=str(roc_date(match))
            if prior_day>=next(iter(issued)) or match[4] in current_numbers:
                continue
            for end in temporal_mentions(match[0]):
                if end['role']=='effective_end':
                    revocations.append(dict(end,revoked_notice_number=match[4],
                                            prior_publication_date=prior_day,clause=match[0]))
        revoked_evidence={compact(r['evidence']) for r in revocations}
        ends=[end for end in ends if compact(end['evidence']) not in revoked_evidence]
    # Match margin_effective's existing single-session boundary before the
    # 2017-05-15 after-hours launch. This bounds the temporary level only;
    # it does not prove that a scheduled restoration actually took place.
    ends=[dict(m,boundary='after_regular_session')
          if m['boundary']=='after_trading_session' and m['date_iso']<'2017-05-15' else m
          for m in ends]
    effective=None;phase='unresolved'
    try:
        effective=margin_effective(text)[:10];phase='after_product_regular_close'
    except ValueError as exc:
        starts={(m['date_iso'],m['boundary']) for m in mentions if m['role']=='effective_start'}
        # Older stock-futures notices put the named contract between the date
        # and close, with implementation language after a comma. A relative
        # next-business-day clause is usable here only when it ALSO prints the
        # exact date in parentheses; no calendar date is inferred from prose.
        named_closes=re.finditer(r'(?:自|生效日次一營業日\()'+ROC_DATE+
            r'\)?該股票期貨契約(一般)?交易時段結束後[,，]?(?:開始實施|起實施)',compact(text))
        starts.update((str(roc_date(m)), 'after_regular_session' if m[4]
                       else 'after_trading_session') for m in named_closes)
        listings={str(roc_date(m)) for m in re.finditer(
            r'(?:訂於|自)'+ROC_DATE+r'(?:起)?上市',compact(text))}
        listings.update(str(roc_date(m)) for m in re.finditer(
            r'上市日期(?:為|訂為|定為|[:：])'+ROC_DATE,compact(text)))
        listings.update(str(roc_date(m)) for m in re.finditer(
            ROC_DATE+r'新上市\d+檔[「\"]股票期貨(?:契約)?[」\"]',compact(text)))
        for cell in re.finditer(r'\|上市日期\|([^|]+)\|',compact(text)):
            dates=temporal_mentions(cell[1])
            if len(dates)==1:listings.add(dates[0]['date_iso'])
        if len(listings)==1 and all(day in listings and boundary=='date_only' for day,boundary in starts):
            effective=next(iter(listings));phase='new_contract_listing'
            starts=set()
        if len(starts)==1:
            effective,boundary=next(iter(starts))
            phase={'after_regular_session':'after_product_regular_close',
                   'after_trading_session':'after_product_trading_session_unspecified',
                   'regular_session_open':'product_regular_open'}.get(boundary,'date_only_requires_phase_review')
        elif effective is None:errors.append(str(exc))
    # A quoted, numbered legal reference has its own publication year. It is
    # not a year for this notice's operative month/day. Remove only that exact
    # reference syntax; other mixed-year dates remain ambiguity barriers.
    dense = compact(text)
    year_context = operative_notice_year_context(text,publication_bound=issue_bound)
    years = {int(m[1])+1911 for m in re.finditer(ROC_DATE, year_context)}
    if effective and len(years)==1:
        year = next(iter(years))
        for m in re.finditer(r'(?<![年月\d])(\d{1,2})月(\d{1,2})日(?:該(?:股票)?契約)?'
                             r'(一般)?交易時段結束後恢復為', dense):
            day = date(year,int(m[1]),int(m[2]))
            if day <= date.fromisoformat(effective):
                continue
            end = dict(date_iso=str(day), role='effective_end',
                boundary='after_regular_session' if m[3] else 'after_trading_session',
                evidence=m[0], year_binding=('unique_explicit_roc_year_excluding_numbered_legal_reference'
                    if year_context != dense else 'unique_explicit_roc_year_in_notice'))
            if not any(e['date_iso']==end['date_iso'] and e['boundary']==end['boundary'] for e in ends):
                ends.append(end)
    ends=[dict(m,boundary='after_regular_session')
          if m['boundary']=='after_trading_session' and m['date_iso']<'2017-05-15' else m
          for m in ends]
    return dict(issue_date_bound=issue_bound,effective_date=effective,effective_phase=phase,
        clock_error='; '.join(errors) or None,chronological=bool(effective and effective>published),
        temporary_end_evidence=json.dumps(ends,ensure_ascii=False),
        prior_notice_revocation_evidence=json.dumps(revocations,ensure_ascii=False),
        requires_reversion_review=bool(ends or '順延' in text or '恢復' in text))


def source_review_candidates(archive, path):
    """Read explicit visual transcriptions, including dated amendments.

    A reviewed cell is still a candidate fact, never a complete rule chain.
    Per-row ratios and position limits cannot be inherited from adjacent rows.
    """
    payload=json.loads(path.read_text())
    reviews=payload.get('reviews',[payload])
    facts=[];positions=[]
    for review in reviews:
        if (review.get('review_kind')!='source_bound_visual_transcription'
                or review.get('margin_kind') not in ('notional_rate','fixed_twd')):
            raise ValueError('unsupported source-bound margin review')
        dependencies=[dict(source_url=review['source_url'],content_sha256=review['content_sha256'],
                           announcement_url=review.get('announcement_url',review['source_url']),
                           published_date=review['published_date']),*review.get('amendments',[])]
        for dependency in dependencies:
            doc=archive.document(dependency['source_url'])
            if doc['content_sha256']!=dependency['content_sha256']:
                raise ValueError('reviewed source SHA mismatch')
            announcement=dependency.get('announcement_url',dependency['source_url'])
            notice=archive.conn.execute('SELECT * FROM announcements WHERE url=?',
                                        (announcement,)).fetchone()
            if notice is None or notice['published_date']!=dependency['published_date']:
                raise ValueError('reviewed source publication mismatch')
            if announcement!=dependency['source_url']:
                if dependency['source_url'] not in archive.children(announcement):
                    raise ValueError('reviewed source is not the announced attachment')
                archive.document(announcement)
                check_publication(doc['text'],dependency['published_date'])
        latest=max(d['published_date'] for d in dependencies)
        if review['known_at']!=timestamp(date.fromisoformat(latest),'23:59:59') or review['effective_date']<=latest:
            raise ValueError('reviewed source clock mismatch')
        if review.get('original_scheduled_date') and not review.get('requires_effective_date_review', False):
            resolution = review.get('postponement_resolution', {})
            if (resolution.get('rule') != 'first_market_session_after_original_scheduled_date'
                    or '順延至次一營業日' not in compact(resolution.get('reviewed_amendment_clause', ''))
                    or resolution.get('amendment_content_sha256') not in
                       {s['content_sha256'] for s in review.get('amendments', [])}):
                raise ValueError('resolved postponement requires a bound amendment clause')
            calendar_path = Path(resolution['calendar_source_path'])
            calendar_manifest = calendar_path.with_name('manifest.json')
            calendar_receipt = json.loads(calendar_manifest.read_text())
            calendar_sha = sha256_file(calendar_path)
            if (calendar_sha != resolution['calendar_source_sha256']
                    or calendar_receipt.get('outputs', {}).get(calendar_path.name, {}).get('sha256') != calendar_sha):
                raise ValueError('postponement calendar source SHA mismatch')
            next_day = (pl.scan_parquet(calendar_path)
                .filter((pl.col('session') == '一般')
                        & (pl.col('date') > date.fromisoformat(review['original_scheduled_date'])))
                .select(pl.col('date').min()).collect().item())
            if str(next_day) != review['effective_date']:
                raise ValueError('postponed date differs from the next official market session')
            archive.copy(calendar_manifest, sha256_file(calendar_manifest), url='', kind='postponement_calendar_manifest')
        archive.copy(path,sha256_file(path),url=review['source_url'],kind='visual_source_review')
        for page in review.get('pages',[]):
            image_path=path.parent/page['path']
            if not image_path.resolve().is_relative_to(path.parent.resolve()):
                raise ValueError('unsafe margin image path')
            archive.copy(image_path,page['sha256'],url=review['source_url'],kind='visual_margin_review_page')
        rows=review.get('rows')
        if rows is None:
            rows=[dict(product=p,after=review['after']) for p in review['products']]
        if len({r['product'] for r in rows})!=len(rows):
            raise ValueError('duplicate reviewed product')
        provenance=dict(source_url=review['source_url'],announcement_url=review.get('announcement_url',review['source_url']),
            published_date=review['published_date'],source_content_sha256=review['content_sha256'],
            known_at=review['known_at'],effective_date=review['effective_date'],
            effective_phase=review['effective_phase'],extraction='source_bound_visual_transcription',
            issue_date_bound=True,chronological=True,candidate_only=True,point_in_time_verified=False,
            amendment_evidence=json.dumps(review.get('amendments',[]),ensure_ascii=False))
        if review.get('transcribed_text'):
            provenance.update(source_review_clock_text=review['transcribed_text'],
                              visual_review_sha256=sha256_file(path))
        ends = []
        if review.get('temporary_end_evidence'):
            if not review.get('pages') or not review.get('transcribed_text'):
                raise ValueError('reviewed temporary boundary requires inspected issuing pages')
            clock = candidate_notice_clock(review['transcribed_text'], review['published_date'])
            if (not clock['issue_date_bound'] or clock['effective_date'] != review['effective_date']
                    or clock['effective_phase'] != review['effective_phase']):
                raise ValueError('reviewed temporary clock differs from the inspected clause')
            # Explicit named clauses can distinguish a restoration clock
            # from dates inside its earlier-rate reference. Require all
            # reviewed products' own clocks to match the declared review.
            ends=[end for row in rows for end in json.loads(product_margin_restoration_clock(
                review['transcribed_text'],row['product'],clock)['temporary_end_evidence'])]
            expected = {(e['date_iso'], e['boundary']) for e in review['temporary_end_evidence']}
            if (not ends or {(e['date_iso'], e['boundary']) for e in ends} != expected
                    or any(e['date_iso'] <= review['effective_date'] for e in ends)):
                raise ValueError('reviewed temporary end differs from the inspected clause')
        for row in rows:
            if review.get('pages') and row.get('page') not in {p['page'] for p in review['pages']}:
                raise ValueError('margin cell has no inspected page')
            values=[float(row['after'][k]) for k in ('initial','maintenance','clearing')]
            if (not all(math.isfinite(v) and v>0 for v in values) or not values[0]>=values[1]>=values[2]
                    or (review['margin_kind']=='notional_rate' and values[0]>1)):
                raise ValueError('invalid reviewed margin hierarchy or unit')
            before = None
            if row.get('before') is not None:
                before = [float(row['before'][k]) for k in ('initial','maintenance','clearing')]
                if (not all(math.isfinite(v) and v>0 for v in before)
                        or not before[0]>=before[1]>=before[2]
                        or (review['margin_kind']=='notional_rate' and before[0]>1)):
                    raise ValueError('invalid reviewed prior margin hierarchy or unit')
            reference=None
            if row.get('non_disposed_reference') is not None:
                reference=[float(row['non_disposed_reference'][k]) for k in ('initial','maintenance','clearing')]
                if (review['margin_kind']!='notional_rate' or not all(math.isfinite(v) and v>0 for v in reference)
                        or not 1>=reference[0]>=reference[1]>=reference[2]):
                    raise ValueError('invalid reviewed non-disposed margin reference')
            row_ends=ends
            if ends:
                scoped=product_margin_restoration_clock(review['transcribed_text'],row['product'],clock)
                row_ends=json.loads(scoped['temporary_end_evidence'])
            facts.append(dict(provenance,product=row['product'],margin_kind=review['margin_kind'],
                after=values,before=before,declared_non_disposed_margin=reference,requires_dated_session_rule=True,
                requires_reversion_review=bool(row_ends) or review.get('requires_effective_date_review',False),
                temporary_end_evidence=json.dumps(row_ends,ensure_ascii=False),
                event_type='before_after' if before is not None else 'absolute_level'))
            if 'natural_person_limit' in row:
                amount=row['natural_person_limit']
                if isinstance(amount,bool) or not isinstance(amount,int) or amount<=0:
                    raise ValueError('invalid reviewed natural-person contract count')
                positions.append(dict(provenance,product=row['product'],natural_person_limit=amount,
                    unit='contracts',event_type='absolute_level',direction='absolute',
                    underlying_symbol=row.get('underlying_symbol'),
                    requires_adjusted_contract_and_mini_group_review=True))
    return facts,positions


def apply_product_code_reviews(archive, facts, path, *, kind):
    """Correct exact OCR cells using reviewed originals, never fuzzy aliases.

    GIF and GTF, or ISF and I5F, are distinct listed products. A global OCR
    substitution would corrupt legitimate stock-futures facts. Bind the
    correction to source bytes, page image and all six original amount cells.
    """
    review = json.loads(path.read_text())
    if kind not in ('margin','position') or review.get('review_kind') != f'source_bound_{kind}_product_code_cells':
        raise ValueError('unsupported product-code review')
    fields=(('margin_kind','after','before') if kind=='margin'
            else ('natural_person_limit','underlying_symbol','effective_date','page'))
    digest = sha256_file(path)
    replacements = {}
    for cell in review['cells']:
        doc = archive.document(cell['source_url'])
        if doc['content_sha256'] != cell['content_sha256']:
            raise ValueError('margin code review source SHA mismatch')
        image = path.parent / cell['page_image']
        if (not image.resolve().is_relative_to(path.parent.resolve())
                or sha256_file(image) != cell['page_image_sha256']):
            raise ValueError('margin code review image SHA/path mismatch')
        if (not isinstance(cell['page'], int) or cell['page'] <= 0
                or not re.fullmatch(r'[A-Z][A-Z0-9]{1,3}', cell['product'])
                or cell['product'] == cell['ocr_product']):
            raise ValueError('invalid reviewed product-code cell')
        matches = [i for i, fact in enumerate(facts)
            if fact['source_content_sha256'] == cell['content_sha256']
            and fact['source_url'] == cell['source_url']
            and fact['product'] == cell['ocr_product']
            and all(fact.get(field)==cell[field] for field in fields)]
        if not matches:
            raise ValueError('reviewed margin code/amount cells do not match a candidate')
        for i in matches:
            if i in replacements:
                raise ValueError('duplicate margin code review target')
            replacements[i] = dict(facts[i], product=cell['product'],
                source_product_code=cell['ocr_product'],
                product_code_policy='source_bound_visual_cell_correction',
                product_code_review_sha256=digest, product_code_review_page=cell['page'])
        archive.copy(image, cell['page_image_sha256'], url=cell['source_url'], kind=f'visual_{kind}_code_page')
    archive.copy(path, digest, url='', kind=f'visual_{kind}_code_review')
    return [replacements.get(i, fact) for i, fact in enumerate(facts)]


def apply_margin_code_reviews(archive, facts, path):
    return apply_product_code_reviews(archive,facts,path,kind='margin')


def reviewed_fixed_subscription_rights(facts, review):
    """Evaluate already vested rights from inspected operands, never future prices."""
    from decimal import Decimal, ROUND_FLOOR
    for component in review.get('fixed_subscription_rights', []):
        if (component.get('rounding') != 'floor_twd'
                or component.get('page') not in review['visually_reviewed_pages']
                or component.get('observation_date', '9999') >= review['published_date']):
            raise ValueError('fixed rights need an inspected, prior-observable formula')
        fields = ('closing_price', 'subscription_price', 'subscription_quantity')
        operands = [Decimal(component[key]) for key in fields]
        if any(not x.is_finite() or x < 0 for x in operands):
            raise ValueError('invalid fixed rights operands')
        text = compact(component['transcribed_formula'])
        if any(str(x) not in text for x in operands):
            raise ValueError('fixed rights operands differ from inspected formula')
        matches = [f for f in facts if f['product'] == component['product']]
        if len(matches) != 1 or not matches[0]['subscription_rights_at_final_settlement']:
            raise ValueError('fixed rights do not identify one adjusted contract')
        fact = matches[0]
        if fact.get('deliverable_cash_twd') != 0:
            raise ValueError('fixed rights cannot overwrite another cash component')
        value = (max(operands[0] - operands[1], Decimal(0)) * operands[2]).quantize(
            Decimal(1), rounding=ROUND_FLOOR)
        # TAIFEX's 2010 and 2011 value rules exclude subscription rights from
        # non-expiring contract values, even after their amount is known.
        fact['fixed_subscription_rights_twd'] = float(value)
        fact['fixed_subscription_rights_evidence'] = dict(component, derived_value_twd=str(value))
    return facts


def retain_margin_code_corrections(existing, extracted):
    """Re-extraction cannot resurrect exact OCR cells already source-reviewed."""
    def key(row,product):
        return (row['source_content_sha256'],row['source_url'],product,row['margin_kind'],
                tuple(row['after']),None if row.get('before') is None else tuple(row['before']))
    corrected={key(r,r['source_product_code']) for r in existing
               if r.get('product_code_policy')=='source_bound_visual_cell_correction'
               and r.get('product_code_review_sha256')}
    reviewed={(r['source_content_sha256'],r['product']) for r in existing
              if r.get('extraction')=='source_bound_visual_transcription'}
    return [r for r in extracted if key(r,r['product']) not in corrected
            and (r['source_content_sha256'],r['product']) not in reviewed]


def replace_reviewed_product_facts(existing, reviewed):
    """A partial table review only replaces that source's reviewed products."""
    keys = {(r['source_content_sha256'], r['product']) for r in reviewed}
    return [r for r in existing if (r['source_content_sha256'], r['product']) not in keys] + reviewed


def prioritize_reviewed_position_facts(facts, inherited=()):
    """Keep inspected cells and their separately dated source compositions.

    A restored cap can share a notice/product with the temporary share cap
    whose cells were reviewed. Giving those cells priority over OCR must not
    erase the later restoration or a verified grade-boundary composition.
    Receipt-verified parent facts are retained too: a context repair may have
    added valid siblings after the parent's priority pass. Explicit new
    reviews still replace their source/product earlier in the build, so
    superseded facts are not resurrected here.
    """
    composed = {'source_composed_original_position_restoration',
                'source_composed_position_grade_boundary'}
    parent_ids = {id(r) for r in inherited}
    reviewed = [r for r in facts
        if id(r) in parent_ids
        or (r.get('extraction_method') or '').startswith('source_bound_visual')
        or r.get('extraction') == 'source_bound_visual_corporate_cells'
        or r.get('extraction') in composed]
    return replace_reviewed_product_facts(facts, reviewed)


def replace_reviewed_corporate_facts(existing, reviewed):
    """An inspected code cell owns its exact source, origin, date and months.

    OCR can turn IA1 into TA1. Matching only the decoded target would retain
    that incorrect sibling. Do not extend a correction to other source rows,
    origins, dates or contract months, even if the codes look similar.
    """
    scopes = {}
    for row in reviewed:
        key = tuple(row.get(c) for c in ('source_content_sha256', 'from_product', 'effective_date'))
        if all(key) and row.get('contract_months'):
            scopes.setdefault(key, set()).update(row['contract_months'])
    kept = []
    for row in replace_reviewed_product_facts(existing, reviewed):
        if row in reviewed:
            kept.append(row)
            continue
        key = tuple(row.get(c) for c in ('source_content_sha256', 'from_product', 'effective_date'))
        months = set(row.get('contract_months') or [])
        if months and months <= scopes.get(key, set()):
            continue
        kept.append(row)
    return kept


def bind_position_generation_constraints(archive, review, parsed, corporate_facts, position_facts, path):
    """Prove that a new combined constraint implies a transferred prior one.

    A ticker reused by a new generation cannot own its predecessor's scheduled
    units forever. This is a source-bound constraint implication, not a legal
    revocation or permission to choose whichever cap happens to be smaller.
    """
    for proof in review.get('prior_generation_constraints', []):
        old_doc = archive.document(proof['source_url'])
        new_doc = archive.document(review['source_url'])
        notice = archive.conn.execute('SELECT published_date FROM announcements WHERE url=?',
                                      (proof['source_url'],)).fetchone()
        number = proof.get('notice_number') or ''
        literal = r'[台臺]期交字第' + re.escape(number) + '號'
        kind = proof.get('proof_kind', 'referenced_originals')
        if kind not in ('referenced_originals', 'verified_originals_unchanged_transfer'):
            raise ValueError('unsupported position generation proof kind')
        if (not re.fullmatch(r'\d{9,12}', number) or notice is None
                or notice['published_date'] != proof['published_date']
                or old_doc['content_sha256'] != proof['content_sha256']
                or new_doc['content_sha256'] != review['content_sha256']):
            raise ValueError('position generation update lacks both bound originals')
        if (kind == 'referenced_originals' and (
                not re.search(literal, compact(old_doc.get('text', '')))
                or not re.search(literal, compact(new_doc.get('text', ''))))):
            raise ValueError('position generation update lacks both referenced originals')
        day = str(date.fromisoformat(proof['effective_date']))
        if not proof['published_date'] < review['published_date'] < review['effective_date'] <= day:
            raise ValueError('position generation update publication order is unproven')
        prior = [r for r in (position_facts or [])
                 if r.get('source_content_sha256') == proof['content_sha256']
                 and r.get('source_url') == proof['source_url'] and r.get('effective_date') == day]
        current = [r for r in parsed if r.get('effective_date') == day]
        if (not prior or not current or any(not r.get('issue_date_bound')
                or r.get('known_at') != timestamp(date.fromisoformat(proof['published_date']), '23:59:59')
                for r in prior)):
            raise ValueError('position generation update lacks dated prior constraints')

        def constraint(rows):
            groups = {tuple(sorted(set(r.get('combined_products') or []))) for r in rows}
            caps = {r.get('natural_person_limit') for r in rows}
            monthly = {r.get('natural_person_monthly_limit') for r in rows}
            periods = {(r.get('valid_until_date_inclusive'), r.get('end_rule'),
                        bool(r.get('requires_delisting_clock'))) for r in rows}
            units = {}
            for row in rows:
                value = row.get('position_unit')
                if (row.get('event_type') != 'corporate_securities_unit_limit' or row.get('unit') != 'shares'
                        or not isinstance(value, (int, float)) or isinstance(value, bool)
                        or not math.isfinite(value) or value <= 0):
                    raise ValueError('position generation constraint unit is unproven')
                units.setdefault(row['product'], set()).add(value)
            if (len(groups) != 1 or set(next(iter(groups))) != set(units)
                    or len(caps) != 1 or len(monthly) != 1 or len(periods) != 1
                    or any(len(values) != 1 for values in units.values())):
                raise ValueError('position generation constraint group or amounts are ambiguous')
            cap = next(iter(caps))
            if not isinstance(cap, (int, float)) or isinstance(cap, bool) or not math.isfinite(cap) or cap <= 0:
                raise ValueError('position generation constraint cap is unproven')
            return dict(units={k: next(iter(v)) for k, v in units.items()},
                        cap=cap, monthly=next(iter(monthly)), period=next(iter(periods)))

        old, new = constraint(prior), constraint(current)
        transfers = proof.get('transfers') or {}
        if (set(transfers) != set(old['units']) or len(set(transfers.values())) != len(transfers)
                or not set(transfers.values()).issubset(new['units'])
                or new['cap'] > old['cap'] or new['period'] != old['period']
                or (old['monthly'] is not None and (new['monthly'] is None or new['monthly'] > old['monthly']))):
            raise ValueError('position generation update does not imply the complete prior constraint')
        identities = [r for r in (corporate_facts or [])
                      if r.get('source_url') == review['source_url']
                      and r.get('source_content_sha256') == review['content_sha256']
                      and r.get('effective_date') == review['effective_date']]
        expected_known = timestamp(date.fromisoformat(review['published_date']), '23:59:59')
        if not identities or any(not r.get('issue_date_bound') or r.get('known_at') != expected_known
                                 for r in identities):
            raise ValueError('position generation transfer clock is unproven')
        inspected = None
        if kind == 'verified_originals_unchanged_transfer':
            # Complete own tables can prove the implication even when a new
            # notice does not cite the prior notice number. Reparse both
            # hash-bound originals; a ticker, equal cap or caller assertion
            # alone cannot establish a physical contract transfer.
            if (len({r.get('effective_phase') for r in prior + current}) != 1
                    or len({r.get('direction') for r in prior + current}) != 1
                    or {r.get('limit_follows_applicable_grade') for r in prior + current} != {False}):
                raise ValueError('generation implication requires identical fixed printed constraint semantics')
            inspected = {}
            for role, url in [('prior', proof['source_url']), ('current', review['source_url'])]:
                bound = proof.get(role + '_source_review') or {}
                source_review = Path(bound.get('path') or '')
                if not source_review.is_file() or sha256_file(source_review) != bound.get('sha256'):
                    raise ValueError('generation implication lacks hash-bound complete source reviews')
                financial, positions, _ = corporate_source_review_candidates(
                    archive, source_review, source_urls={url})
                expected_hash = proof['content_sha256'] if role == 'prior' else review['content_sha256']
                if (not financial or not positions or any(
                        r.get('source_content_sha256') != expected_hash
                        or not r.get('deliverable_components_resolved')
                        or r.get('deliverable_security_quantity') != r.get('contract_multiplier')
                        for r in financial)):
                    raise ValueError('generation implication lacks complete independent financial quantities')
                owned = [r for r in positions if r.get('effective_date') == day]
                evidence_fields = ('product', 'effective_date', 'effective_phase', 'position_unit',
                    'natural_person_limit', 'natural_person_monthly_limit', 'combined_products',
                    'valid_until_date_inclusive', 'end_rule', 'requires_delisting_clock',
                    'limit_follows_applicable_grade')
                signature = lambda row: json.dumps({k: row.get(k) for k in evidence_fields}, sort_keys=True)
                claimed = prior if role == 'prior' else current
                if {signature(r) for r in owned} != {signature(r) for r in claimed}:
                    raise ValueError('generation implication differs from inspected complete position group')
                inspected[role] = dict(financial=financial, review_sha256=bound['sha256'])
            def owned_by_key(rows):
                out = {}
                for row in rows:
                    key = (row['from_product'], row['product'], row['effective_date'], tuple(row['contract_months']))
                    out.setdefault(key, set()).add((row['contract_multiplier'], row['deliverable_security_quantity']))
                return out
            if owned_by_key(inspected['current']['financial']) != owned_by_key(identities):
                raise ValueError('generation transfer identity differs from inspected own financial tables')
            prior_months = {month for r in inspected['prior']['financial'] for month in r['contract_months']}
            if (not prior_months or any(not set(r['contract_months']) <= prior_months for r in identities)):
                raise ValueError('generation transfer extends beyond inspected prior contract months')
        resolved, _ = corporate_terms_intervals(identities, unit_only=True)
        for origin, destination in transfers.items():
            own = [r for r in resolved if r['from_product'] == origin and r['product'] == destination]
            values = {r['contract_multiplier'] for r in own}
            if (not re.fullmatch(r'[A-Z]{2}[F1-9]', origin)
                    or not re.fullmatch(re.escape(origin[:2]) + r'[1-9]', destination)
                    or (kind == 'referenced_originals'
                        and origin + '調整為' + destination not in compact(new_doc.get('text', '')))
                    or values != {old['units'][origin]} or new['units'][destination] != old['units'][origin]):
                raise ValueError('position generation transfer lacks unchanged physical units')
        pages = proof.get('pages') or []
        if not pages:
            raise ValueError('position generation update lacks inspected prior pages')
        for page in pages:
            image_path = path.parent / page['path']
            if not image_path.resolve().is_relative_to(path.parent.resolve()):
                raise ValueError('unsafe prior generation image path')
            if sha256_file(image_path) != page['sha256']:
                raise ValueError('position generation page SHA mismatch')
            archive.copy(image_path, page['sha256'], url=proof['source_url'], kind='visual_prior_position_generation')
        evidence = json.dumps(dict(proof_kind=kind, prior_source_sha256=proof['content_sha256'],
            current_source_sha256=review['content_sha256'], prior_notice_number=number,
            transfer_date=review['effective_date'], effective_date=day, transfers=transfers,
            transferred_prior_members=sorted(transfers.values()), current_combined_members=sorted(new['units']),
            prior_cap_shares=old['cap'], current_cap_shares=new['cap'],
            prior_known_at=prior[0]['known_at'], current_known_at=expected_known,
            complete_original_review_sha256s=(
                {role: item['review_sha256'] for role, item in inspected.items()} if inspected else None),
            prior_constraint_implied_after_physical_transfer=True,
            legal_notice_revocation_inferred=False), ensure_ascii=False)
        for row in current:
            row.update(supersedes_source_sha256s=sorted(set(row.get('supersedes_source_sha256s') or [])
                                                     | {proof['content_sha256']}),
                       supersession_review_sha256=sha256_file(path),
                       position_generation_constraint_evidence=evidence)


def position_source_review_candidates(archive, path, corporate_facts=None, position_facts=None, *, source_urls=None):
    """Read inspected position cells without inventing a companion margin row."""
    from fractions import Fraction
    rows=[]
    for review in json.loads(path.read_text())['reviews']:
        if source_urls is not None and review['source_url'] not in source_urls:
            continue
        is_group=review.get('review_kind')=='source_bound_visual_corporate_position_group'
        is_corporate=is_group or review.get('review_kind')=='source_bound_visual_corporate_position_cells'
        is_roster=review.get('review_kind')=='source_bound_visual_position_roster_cells'
        is_grid=review.get('review_kind')=='source_bound_visual_position_grid_cells'
        if review.get('review_kind')!='source_bound_visual_position_cells' and not (is_corporate or is_roster or is_grid):
            raise ValueError('unsupported position review')
        doc=archive.document(review['source_url'])
        notice=archive.conn.execute('SELECT * FROM announcements WHERE url=?',(review['source_url'],)).fetchone()
        if (doc['content_sha256']!=review['content_sha256'] or notice is None
                or notice['published_date']!=review['published_date']):
            raise ValueError('position review source identity/publication mismatch')
        if not is_corporate:
            check_publication(review['transcribed_text'],review['published_date'])
        if not (is_corporate or is_grid):
            clock=candidate_notice_clock(review['transcribed_text'],review['published_date'])
            required_phase='date_only_requires_phase_review' if is_roster else 'new_contract_listing'
            if clock['effective_date']!=review['effective_date'] or clock['effective_phase']!=required_phase:
                raise ValueError('position effective date/phase differs from inspected notice')
        if (not review['pages'] or (not is_grid and (
                review['effective_date']<review['published_date'] if is_roster
                else review['effective_date']<=review['published_date']))):
            raise ValueError('position review requires prior publication and inspected pages')
        for page in review['pages']:
            image_path=path.parent/page['path']
            if not image_path.resolve().is_relative_to(path.parent.resolve()):
                raise ValueError('unsafe position image path')
            if not image_path.exists():
                # Retained review JSON keeps its original relative location.
                # A copied archive may resolve only the exact previously bound
                # page bytes for this URL. The same page can serve both
                # financial and position reviews; its last copied role does
                # not change the bound original or the page bytes.
                matches=[s for s in archive.sources.values()
                    if s['kind'] in ('visual_position_review_page','visual_corporate_review_page')
                    and s['url']==review['source_url'] and s['sha256']==page['sha256']]
                if len(matches)!=1:
                    raise ValueError('retained position review page identity is unavailable')
                image_path=archive.bundle/matches[0]['path']
            archive.copy(image_path,page['sha256'],url=review['source_url'],kind='visual_position_review_page')
        if is_grid:
            parsed=[]
            for page in review['pages']:
                for table in page.get('tables',[]):
                    parsed.extend(dict(r,page=page['page']) for r in position_grid_candidates(
                        table['cells'],review['transcribed_text'],caption=table.get('caption',''),
                        published_date=review['published_date']))
            expected=review.get('expected_products',[])
            if (not parsed or len(parsed)!=len(expected) or len(set(expected))!=len(expected)
                    or {r['product'] for r in parsed}!=set(expected)
                    or any(not r.get('effective_date') or r['effective_date']<review['published_date']
                           for r in parsed)):
                raise ValueError('reviewed position grid lacks exact products and dated cells')
            rows.extend(dict(r,source_url=review['source_url'],announcement_url=review['source_url'],
                published_date=review['published_date'],source_content_sha256=review['content_sha256'],
                known_at=timestamp(date.fromisoformat(review['published_date']),'23:59:59'),
                issue_date_bound=True,point_in_time_verified=False,candidate_only=True,
                extraction_method=review['review_kind'],visual_review_sha256=sha256_file(path)) for r in parsed)
            continue
        if is_corporate:
            # Publication and identities have already been bound to these
            # exact source bytes. A position-only review cannot alter them.
            # Position units do not depend on the separate cash/rights value.
            # Bind only the identity and multiplier facts needed by this
            # table; a missing subscription value must not hide a clear cap.
            identities=[r for r in (corporate_facts or [])
                if r.get('source_content_sha256')==review['content_sha256']
                and r.get('source_url')==review['source_url']
                and r.get('effective_date')==review['effective_date']
                and r.get('issue_date_bound')]
            expected=timestamp(date.fromisoformat(review['published_date']),'23:59:59')
            if not identities or any(r['known_at']!=expected for r in identities):
                raise ValueError('position review lacks dated same-source corporate identities')
            # A damaged text view can truncate a multiplier even when the
            # independent quantity and monthly unit resolver agree. Reuse
            # that resolver only for ambiguous identities; a complete second
            # quantity/multiplier pair remains a real conflict. This review
            # still cannot replace any financial deliverable facts.
            def unit_key(row):
                return tuple(row.get(k) for k in
                    ('source_content_sha256','from_product','product','effective_date'))
            groups={}
            for row in identities:
                groups.setdefault(unit_key(row),[]).append(row)
            ambiguous={key for key,views in groups.items()
                       if len({r.get('contract_multiplier') for r in views})>1}
            if ambiguous:
                resolved,_=corporate_terms_intervals(identities,unit_only=True)
                units={}
                for row in resolved:
                    for sha in row['source_content_sha256s']:
                        key=(sha,row['from_product'],row['product'],row['effective_date'])
                        units.setdefault(key,set()).add(row['contract_multiplier'])
                accepted={}
                for key in ambiguous:
                    values=units.get(key,set())
                    corroborated={r.get('contract_multiplier') for r in groups[key]
                        if isinstance(r.get('contract_multiplier'),(int,float))
                        and not isinstance(r['contract_multiplier'],bool)
                        and math.isfinite(r['contract_multiplier']) and r['contract_multiplier']>0
                        and r.get('deliverable_security_quantity')==r['contract_multiplier']}
                    if len(values)==1 and not corroborated-values:
                        accepted[key]=values
                identities=[r for r in identities if unit_key(r) not in accepted
                            or r.get('contract_multiplier') in accepted[unit_key(r)]]
            if is_group:
                parsed=corporate_position_text_candidates(
                    '\n'.join(p['native_text'] for p in review['pages']), identities)
                if any(r.get('event_type')!='combined_position_formula'
                       or r.get('combined_position_ratio')!='1/1'
                       or r.get('natural_person_limit') is not None for r in parsed):
                    raise ValueError('reviewed group must only bind unchanged-unit members')
            else:
                parsed=corporate_position_table_candidates(review['pages'],corporate=identities)
            if not parsed or any(r['effective_date']<review['effective_date'] for r in parsed):
                raise ValueError('reviewed position cells disagree with corporate terms/clock')
            bind_position_generation_constraints(archive, review, parsed, corporate_facts, position_facts, path)
            rows.extend(dict(r,source_url=review['source_url'],announcement_url=review['source_url'],
                published_date=review['published_date'],source_content_sha256=review['content_sha256'],
                known_at=expected,issue_date_bound=True,point_in_time_verified=False,candidate_only=True,
                position_formula_parser=r.get('extraction_method') if is_group else None,
                extraction_method=review['review_kind'],visual_review_sha256=sha256_file(path))
                for r in parsed)
            continue
        for row in review['rows']:
            if row.get('page') not in {p['page'] for p in review['pages']}:
                raise ValueError('position cell has no inspected page')
            product=row['product'];amount=row.get('natural_person_limit')
            if not re.fullmatch(r'[A-Z][A-Z0-9]{1,2}',product):raise ValueError('invalid position product')
            ratio=row.get('combined_position_ratio');base=row.get('combined_position_base_product')
            if ratio:
                if not base or not 0<Fraction(ratio)<1 or amount is not None:
                    raise ValueError('invalid source-bound combined position formula')
            elif not isinstance(amount,int) or isinstance(amount,bool) or amount<=0:
                raise ValueError('invalid source-bound natural person limit')
            day=review['effective_date'];superseded=[]
            phase='date_only_requires_phase_review' if is_roster else 'new_contract_listing'
            if is_roster:
                # A roster contains both options-only rows and future listings.
                # Its same-day legal date never becomes prior-day knowledge.
                if row.get('futures_eligible') is not True:
                    raise ValueError('reviewed roster requires an explicit futures indicator')
                if row.get('effective_date',day)!=day:
                    day=row['effective_date'];clause=compact(row.get('effective_date_clause',''))
                    d=date.fromisoformat(day)
                    literal=f'{d.year-1911}年{d.month}月{d.day}日'
                    if (day<review['effective_date'] or literal not in clause
                            or clause not in compact(review['transcribed_text'])
                            or not row.get('product_name') or row['product_name'] not in clause
                            or '上市後適用' not in clause):
                        raise ValueError('roster listing exception lacks its inspected named clause')
                    phase='new_contract_listing'
                # A dated full roster can explicitly replace the cap of a
                # named forthcoming listing. Bind both inspected originals;
                # publication recency alone never selects the winning cap.
                for proof in row.get('supersedes_listing_caps',[]):
                    if phase!='new_contract_listing':
                        raise ValueError('roster supersession requires a named future listing')
                    old=archive.document(proof['source_url'])
                    notice=archive.conn.execute('SELECT published_date FROM announcements WHERE url=?',
                        (proof['source_url'],)).fetchone()
                    if (old['content_sha256']!=proof['content_sha256'] or notice is None
                            or notice['published_date']!=proof['published_date']
                            or not proof['published_date']<review['published_date']<day):
                        raise ValueError('roster supersession source/publication mismatch')
                    peers=[f for f in (position_facts or []) if f['product']==product
                        and f['source_content_sha256']==proof['content_sha256']
                        and f.get('effective_date')==day]
                    if (not peers or not row.get('underlying_symbol')
                            or proof.get('underlying_symbol')!=row['underlying_symbol']
                            or any(not f.get('issue_date_bound') or f.get('unit')!='contracts'
                            or f.get('natural_person_limit')!=proof['natural_person_limit']
                            or (f.get('underlying_symbol') is not None
                                and f['underlying_symbol']!=row['underlying_symbol']) for f in peers)):
                        raise ValueError('roster supersession differs from retained listing cap')
                    image_path=path.parent/proof['page_image']
                    if not image_path.resolve().is_relative_to(path.parent.resolve()):
                        raise ValueError('unsafe superseded position image path')
                    archive.copy(image_path,proof['page_sha256'],url=proof['source_url'],
                        kind='visual_superseded_listing_cap')
                    superseded.append(proof['content_sha256'])
            rows.append(dict(row,unit='contracts',direction='absolute',
                event_type='combined_position_formula' if ratio else 'absolute_level',
                effective_date=day,effective_phase=phase,
                source_url=review['source_url'],announcement_url=review['source_url'],
                published_date=review['published_date'],source_content_sha256=review['content_sha256'],
                known_at=timestamp(date.fromisoformat(review['published_date']),'23:59:59'),
                issue_date_bound=True,point_in_time_verified=False,candidate_only=True,
                extraction_method=review['review_kind'],visual_review_sha256=sha256_file(path),
                supersedes_source_sha256s=sorted(set(superseded)),
                supersession_review_sha256=sha256_file(path) if superseded else None))
    archive.copy(path,sha256_file(path),url='',kind='visual_position_cell_review')
    return rows


def apply_position_notice_amendments(archive, facts, path):
    """Retain a revoked notice, but never activate its cancelled future cap."""
    result=[dict(row) for row in facts]
    for review in json.loads(path.read_text())['reviews']:
        if review.get('review_kind')!='source_bound_position_notice_revocation':
            raise ValueError('unsupported position notice amendment')
        for key in ('original','amendment'):
            proof=review[key];doc=archive.document(proof['source_url'])
            notice=archive.conn.execute('SELECT published_date FROM announcements WHERE url=?',
                (proof['source_url'],)).fetchone()
            if (doc['content_sha256']!=proof['content_sha256'] or notice is None
                    or notice['published_date']!=proof['published_date']):
                raise ValueError('position amendment source/publication mismatch')
            check_publication(proof['transcribed_text'],proof['published_date'])
            page=path.parent/proof['page_image']
            if not page.resolve().is_relative_to(path.parent.resolve()):
                raise ValueError('unsafe position amendment page path')
            archive.copy(page,proof['page_sha256'],url=proof['source_url'],kind='visual_position_amendment_page')
            for extra in proof.get('extra_pages',[]):
                image_path=path.parent/extra['path']
                if not image_path.resolve().is_relative_to(path.parent.resolve()):
                    raise ValueError('unsafe position amendment table path')
                archive.copy(image_path,extra['sha256'],url=proof['source_url'],kind='visual_position_amendment_table')
        old,new=review['original'],review['amendment']
        number=old['notice_number'];old_text=compact(old['transcribed_text']);new_text=compact(new['transcribed_text'])
        if (not re.fullmatch(r'\d{10,11}',number)
                or '台期交字第'+number+'號' not in old_text.replace('臺期交','台期交')
                or '台期交字第'+number+'號公告停止適用' not in new_text.replace('臺期交','台期交')
                or old['published_date']>=new['published_date']):
            raise ValueError('position revocation does not identify the old notice')
        boundary=date.fromisoformat(review['effective_date'])
        literal=f'並自{boundary.year-1911}年{boundary.month}月{boundary.day}日起實施'
        if literal not in new_text or new['published_date']>str(boundary):
            raise ValueError('position revocation effective date mismatch')
        selected=[r for r in result if r['source_content_sha256']==old['content_sha256']]
        if not selected:raise ValueError('revoked position notice has no retained facts')
        for correction in review.get('scheduled_exceptions',[]):
            scheduled=date.fromisoformat(correction['effective_date'])
            phrase=compact(correction['source_clause'])
            if (phrase not in old_text or compact(correction['product_name']) not in phrase
                    or f'{scheduled.year-1911}年{scheduled.month}月{scheduled.day}日' not in phrase):
                raise ValueError('position exception lacks an explicit product-local date')
            matches=[r for r in selected if r['product']==correction['product']
                     and str(r.get('underlying_symbol'))==correction['underlying_symbol']
                     and r.get('natural_person_limit')==correction['natural_person_limit']]
            if not matches:raise ValueError('position exception product identity mismatch')
            for row in matches:row['effective_date']=str(scheduled)
        for row in selected:
            row.update(notice_revoked_effective_date=str(boundary),
                notice_revocation_source_sha256=new['content_sha256'],
                notice_revocation_known_at=timestamp(date.fromisoformat(new['published_date']),'23:59:59'),
                notice_revocation_review_sha256=sha256_file(path))
    archive.copy(path,sha256_file(path),url='',kind='source_bound_position_notice_amendment')
    return result


def corporate_source_review_candidates(archive, path, *, source_urls=None):
    """Bind a visual cell transcription to the exact original PDF and notice."""
    payload=json.loads(path.read_text());reviews=payload.get('reviews',[payload])
    corporate=[];positions=[];replaced=set()
    for review in reviews:
        if source_urls is not None and review.get('source_url') not in source_urls:
            continue
        if review.get('review_kind')!='source_bound_visual_corporate_cells':
            raise ValueError('unsupported corporate visual review')
        doc=archive.document(review['source_url'])
        if doc['content_sha256']!=review['content_sha256']:
            raise ValueError('corporate visual review source SHA mismatch')
        notice=archive.conn.execute('SELECT * FROM announcements WHERE url=?',
                                    (review['source_url'],)).fetchone()
        if notice is None or notice['published_date']!=review['published_date']:
            raise ValueError('corporate visual review publication mismatch')
        pages=review['pages'];text='\n'.join(p['native_text'] for p in pages)
        if sorted(p['page'] for p in pages)!=review['visually_reviewed_pages']:
            raise ValueError('visual corporate page scope mismatch')
        for page in pages:
            if 'image_path' in page:
                image_path=path.parent/page['image_path']
                if not image_path.exists():
                    # A retained review may have been copied out of its
                    # original bundle. Resolve only its already-copied exact
                    # page receipt. Financial and position reviews may share
                    # that exact page from the same original document.
                    retained={archive.bundle/s['path'] for s in archive.sources.values()
                        if s['kind'] in ('visual_corporate_review_page','visual_position_review_page')
                        and s['url']==review['source_url'] and s['sha256']==page['image_sha256']}
                    if len(retained)!=1:
                        raise ValueError('relocated corporate review lacks one bound page receipt')
                    image_path=retained.pop()
                if sha256_file(image_path)!=page['image_sha256']:
                    raise ValueError('corporate review page image SHA mismatch')
                archive.copy(image_path,page['image_sha256'],url=review['source_url'],
                             kind='visual_corporate_review_page')
        publication_text=text
        if proof:=review.get('publication_evidence'):
            # The issue-date page can already have a hash-bound extraction.
            # Keep it distinct from the cells actually inspected visually.
            manifest_path=Path(proof['manifest'])
            if sha256_file(manifest_path)!=proof['manifest_sha256']:
                raise ValueError('corporate publication evidence manifest SHA mismatch')
            manifest=json.loads(manifest_path.read_text())
            matches=[s for s in manifest['sources'] if s['path']==proof['path']
                     and s['sha256']==proof['sha256'] and s['url']==review['source_url']
                     and s['kind'] in ('review_pages_including_ocr','review_native')]
            if len(matches)!=1:
                raise ValueError('corporate publication extraction lacks source binding')
            evidence_path=manifest_path.parent/proof['path']
            if sha256_file(evidence_path)!=proof['sha256']:
                raise ValueError('corporate publication extraction SHA mismatch')
            raw_sources=[s for s in manifest['sources'] if s['kind']=='raw_gzip'
                         and (s['url']==review['source_url']
                              or s['sha256']==proof.get('raw_sha256'))]
            import gzip, hashlib
            if not any(hashlib.sha256(gzip.decompress((manifest_path.parent/s['path']).read_bytes())).hexdigest()
                       ==review['content_sha256'] for s in raw_sources):
                raise ValueError('corporate publication extraction belongs to different source bytes')
            publication_text=evidence_path.read_text()
            archive.copy(evidence_path,proof['sha256'],url=review['source_url'],
                         kind='corporate_publication_text')
            archive.copy(manifest_path,proof['manifest_sha256'],url='',
                         kind='corporate_publication_parent_manifest')
        check_publication(publication_text,review['published_date'])
        issued = review.get('issued_date')
        if issued is not None:
            literal_issued={v['date_iso'] for v in temporal_mentions(publication_text)
                if v['role']=='publication'}
            if literal_issued!={issued}:
                raise ValueError('corporate reviewed issuing date differs from its literal page')
        superseded=[]
        for old in review.get('supersedes',[]):
            original=archive.document(old['source_url'])
            original_notice=archive.conn.execute('SELECT * FROM announcements WHERE url=?',
                (old['source_url'],)).fetchone()
            number=old['notice_number'];old_text=compact(old['first_page_text'])
            if (original['content_sha256']!=old['content_sha256'] or original_notice is None
                    or original_notice['published_date']!=old['published_date']
                    or old['published_date']>=review['published_date']
                    or not re.fullmatch(r'\d{10,11}',number)
                    or not re.search(r'發文字號[:：]?[臺台]期交字第'+number+'號',old_text)
                    or not re.search(r'修正本公司'+ROC_DATE+r'[臺台]期交字第'+number+'號公告',compact(text))):
                raise ValueError('visual notice amendment lacks exact original/reference binding')
            check_publication(old['first_page_text'],old['published_date'])
            superseded.append(old['content_sha256'])
        facts=reviewed_fixed_subscription_rights(corporate_native_table_candidates(pages),review)
        if not facts or any(not r['deliverable_components_resolved'] or not r['contract_months']
                or not r['effective_date'] or r['effective_date']<=review['published_date']
                or (r['has_equity_credit_fields'] and not r['cash_equity_pair_agrees']) for r in facts):
            raise ValueError('visual corporate review has unresolved required fields')
        provenance=dict(source_url=review['source_url'],announcement_url=review['source_url'],
            published_date=review['published_date'],source_content_sha256=review['content_sha256'],
            known_at=timestamp(date.fromisoformat(review['published_date']),'23:59:59'),
            issue_date_bound=True,point_in_time_verified=False,
            extraction='source_bound_visual_corporate_cells',visual_review_sha256=sha256_file(path),
            supersedes_source_sha256s=sorted(set(superseded)),
            supersession_review_sha256=sha256_file(path) if superseded else None)
        if issued is not None:
            provenance['issued_date']=issued
        bound_facts=[dict(fact,**provenance) for fact in facts]
        corporate.extend(bound_facts)
        # A complete inspected position grid can explicitly name another
        # already adjusted contract without restating its financial event.
        # Its own printed share unit suffices for this position constraint;
        # it must never create or alter a financial corporate fact.
        table_only=review.get('position_unit_scope')=='all_named_members_in_inspected_grid'
        if review.get('position_unit_scope') and not table_only:
            raise ValueError('unsupported reviewed position unit scope')
        positions.extend(dict(fact,**provenance,
                              position_unit_review_scope=review.get('position_unit_scope')) for fact in
                         corporate_position_table_candidates(pages,corporate=bound_facts,
                             allow_table_only_members=table_only,
                             group_text=review.get('position_group_text')))
        replaced.add(review['content_sha256'])
    archive.copy(path,sha256_file(path),url='',kind='visual_corporate_cell_review')
    return corporate,positions,replaced


def reuse_candidate_bundle(archive, root):
    """Append reviewed cells without re-extracting unchanged source documents.

    Reuse is limited to unapproved candidate evidence. Every output and retained
    source is verified, and the parent manifest is included in the new bundle.
    """
    manifest=root/'manifest.json'
    receipt=json.loads(manifest.read_text())
    if (receipt.get('schema_version')!=1 or receipt.get('point_in_time_verified') is not False
            or receipt.get('all_products_training_ready') is not False):
        raise ValueError('parent must be an unapproved candidate bundle')
    required={'margin_event_candidates.parquet','position_event_candidates.parquet',
              'corporate_event_candidates.parquet','announcement_audit.json','product_event_coverage.csv'}
    if not required.issubset(receipt['outputs']):
        raise ValueError('incomplete candidate parent outputs')
    for relative,proof in receipt['outputs'].items():
        path=root/relative
        if not path.resolve().is_relative_to(root.resolve()) or sha256_file(path)!=proof['sha256']:
            raise ValueError('candidate parent output SHA mismatch')
    for proof in receipt['sources']:
        path=root/proof['path']
        if not path.resolve().is_relative_to(root.resolve()):
            raise ValueError('candidate parent source escapes bundle')
        archive.copy(path,proof['sha256'],url=proof['url'],kind=proof['kind'])
    archive.copy(manifest,sha256_file(manifest),url='',kind='parent_candidate_manifest')
    rows=[pl.read_parquet(root/(name+'_event_candidates.parquet')).to_dicts()
          for name in ('margin','position','corporate')]
    if any(row.get('point_in_time_verified') is not False for group in rows for row in group):
        raise ValueError('candidate parent contains approved rows')
    audit=json.loads((root/'announcement_audit.json').read_text())
    return (*rows,audit)


def legacy_archive_candidates(archive):
    """Re-extract native Word facts without revisiting unchanged PDF OCR."""
    margins=[];positions=[];audit=[]
    notices=archive.conn.execute("SELECT a.* FROM announcements a JOIN documents d ON a.url=d.url "
                                 "WHERE d.state='complete' AND lower(a.url) LIKE '%.doc' "
                                 "ORDER BY a.published_date,a.url").fetchall()
    for notice in notices:
        doc=archive.document(notice['url'])
        if doc.get('format')!='legacy_word':
            continue
        mf=margin_legacy_word_levels(doc['text'])
        pf=position_legacy_word_candidates(doc['text'])
        if not mf and not pf:
            continue
        clock=candidate_notice_clock(doc['text'],notice['published_date'])
        provenance=dict(source_url=notice['url'],announcement_url=notice['url'],
            published_date=notice['published_date'],source_content_sha256=doc['content_sha256'],
            known_at=timestamp(date.fromisoformat(notice['published_date']),'23:59:59'),
            extraction='canonical_native_legacy_reextraction',point_in_time_verified=False,
            requires_dated_session_rule=True,**clock)
        margins.extend(dict(row,**provenance) for row in mf)
        positions.extend(dict(row,**{k:v for k,v in provenance.items() if k not in row}) for row in pf)
        audit.append(dict(announcement_url=notice['url'],published_date=notice['published_date'],
            title=notice['title'],candidate_tables=len(mf),errors=[],reextraction='legacy_word'))
    return margins,positions,audit


def native_position_candidates(archive, root, *, corporate=False, margin=False, market_dates=None):
    """Re-read product-scoped facts from retained, hash-verified native grids."""
    positions=[]
    records=archive.conn.execute("SELECT a.* ,d.content_sha256 FROM announcements a JOIN documents d "
        "ON a.url=d.url WHERE d.state='complete' ORDER BY a.published_date,a.url").fetchall()
    seen=set()
    for notice in records:
        key=(notice['content_sha256'],notice['published_date'])
        if key in seen: continue
        seen.add(key)
        folder=root/'documents'/notice['content_sha256']
        if not (folder/'receipt.json').exists(): continue
        proof=json.loads((folder/'receipt.json').read_text())
        if proof['content_sha256']!=notice['content_sha256']:
            raise ValueError('native position source SHA mismatch')
        if proof['status']!='complete': continue
        for f in proof['files']:
            if sha256_file(folder/f['path'])!=f['sha256']:
                raise ValueError('native position table SHA mismatch')
        tables=json.loads((folder/'tables.json').read_text())
        source_text='\n'.join(p.get('ocr_text') or p['native_text'] for p in tables['pages'])
        found=[];clock=None
        for page in ([] if corporate else tables['pages']):
            for ti,table in enumerate(page['tables']):
                rows = (margin_grid_candidates(table['cells'],table.get('caption',''),source_text=source_text) if margin else
                        corporate_grid_candidates(table['cells'], source_text, table.get('caption',''),
                                                 page_text=page['native_text']) if corporate
                        else position_grid_candidates(table['cells'],source_text,caption=table.get('caption',''),
                            published_date=notice['published_date'],market_dates=market_dates))
                found.extend(dict(row,page=page['page'],table_index=ti,
                    extraction_method=table.get('extraction_method','native_cell_grid'))
                    for row in rows)
        if corporate:
            found=corporate_native_table_candidates(tables['pages'])
        elif not margin:
            clock=candidate_notice_clock(source_text,notice['published_date'])
            identities=[dict(row,issue_date_bound=clock['issue_date_bound'])
                for row in corporate_native_table_candidates(tables['pages'])]
            found.extend(corporate_position_table_candidates(tables['pages'],corporate=identities))
        if not found: continue
        doc=archive.document(notice['url'])
        table_kind='ocr_table_' if proof['profile'] in (
            'source_ruled_ocr_cell_grid_v1', 'source_ruled_ocr_cell_grid_v2',
            'source_ruled_ocr_cell_grid_v3') else 'native_table_'
        for name in ('tables.json','receipt.json'):
            archive.copy(folder/name,sha256_file(folder/name),url=notice['url'],kind=table_kind+name)
        clock=clock or candidate_notice_clock(source_text,notice['published_date'])
        for row in found:
            row.update(source_url=notice['url'],announcement_url=notice['url'],
                published_date=notice['published_date'],source_content_sha256=doc['content_sha256'],
                known_at=timestamp(date.fromisoformat(notice['published_date']),'23:59:59'),
                point_in_time_verified=False,issue_date_bound=clock['issue_date_bound'])
            if margin:
                row.update(product_margin_restoration_clock(source_text,row['product'],clock))
                row.update(extraction='canonical_native_margin_reextraction',requires_dated_session_rule=True)
            positions.append(row)
    return positions


def named_margin_restoration_clauses(text, *, publication_bound=False):
    """Return complete product/date/reference assignments in a shared notice.

    Names must come from that original's own code/name cells. Longest-name
    matching keeps a mini's name from also assigning its standard future.
    A partly parsed paragraph is not sufficient evidence for a local clock.
    """
    dense=compact(text);code=r'(?:[A-Z]{2}F|TX|MTX|TE|TF)'
    reference=r'(?:\d{2,3}年)?\d{1,2}月\d{1,2}日'
    prior=r'(?:調整前|標的證券未經處置前)(?:之)?保證金'
    targets=list(re.finditer(r'恢復(?:為(?:'+reference+r')?'+prior+r'|原保證金)',dense))
    clocks=list(re.finditer(ROC_DATE+r'一般交易時段結束後',dense))
    if not targets:return None
    aliases={}
    for m in re.finditer(r'(?<![A-Z0-9])('+code+r')\(([^()]{1,24}期貨)\)',dense):
        aliases.setdefault(m[2],set()).add(m[1])
    # The cover also declares names as 商品為甲期貨契約(AAF)及乙期貨契約(BBF).
    # Overlapping lookahead retains each connected name's own code instead of
    # consuming the whole list into its first alias. Only literal source names
    # are accepted; the later restoration subjects must repeat them exactly.
    for m in re.finditer(r'(?=(?:商品為|本公司|及|與|暨|、|[,，])'
            r'([\u4e00-\u9fff]{1,24}期貨)(?:契約)?\(('+code+r')\))',dense):
        aliases.setdefault(m[1],set()).add(m[2])
    aliases={name:next(iter(codes)) for name,codes in aliases.items() if len(codes)==1}
    names=sorted(aliases,key=len,reverse=True)
    named='|'.join(re.escape(name)+r'(?:契約)?(?:\('+re.escape(aliases[name])+r'\))?' for name in names)
    literal=r'[\u4e00-\u9fff]{1,24}期貨(?:契約)?\('+code+r'\)'
    option=r'[\u4e00-\u9fff]{1,24}選擇權(?:契約)?(?:\([A-Z]{2}O\))?'
    # A literal coded name also matches its alias. Inside a repeated subject
    # list, reconsidering both identical readings makes failed matches grow
    # exponentially. Own each longest token once, preserving its source code.
    entry='(?>'+literal+('|' +named if named else '')+'|'+option+')'
    subject=entry+r'(?:(?:、|及|與|暨|,|，)'+entry+')*'
    tail=r'一般交易時段結束後[,，]?恢復為(?P<reference>'+reference+r')?'+prior
    pattern='(?P<subjects>'+subject+')於(?P<day>'+ROC_DATE+')'+tail
    name_pattern='|'.join(re.escape(name) for name in names)
    rows=[]
    def assigned_subjects(subjects):
        products=set(re.findall(r'\(('+code+r')\)',subjects))
        if name_pattern:
            for n in re.finditer(name_pattern,subjects):
                explicit=re.match(r'(?:契約)?\(('+code+r')\)',subjects[n.end():])
                if explicit and explicit[1]!=aliases[n[0]]:return None
                products.add(aliases[n[0]])
        return products or None
    covered=set(); shared_spans=[]; expected_rows=len(targets)
    # A press release can distribute dates over named subjects and print the
    # regular-close predicate only once at the end of the complete list.
    # Its short dates require the same source's bound, unique operative year.
    day_text=r'(?:\d{2,3}年)?\d{1,2}月\d{1,2}日'
    dated_subject=subject+'於'+day_text
    shared_pattern=(r'(?<![\u4e00-\u9fffA-Z0-9])(?P<entries>'+dated_subject+
        r'(?:(?:、|[,，])'+dated_subject+r')+)'+tail)
    year_context=operative_notice_year_context(dense,publication_bound=publication_bound)
    years={int(m[1])+1911 for m in re.finditer(ROC_DATE,year_context)} if publication_bound else set()
    for shared in re.finditer(shared_pattern,dense):
        prefix=re.split('[。;；]',dense[:shared.start()])[-1]
        if re.search('於'+day_text,prefix):return None  # An earlier unowned item is a barrier.
        entries=list(re.finditer('(?P<subjects>'+subject+')於(?P<day>'+day_text+')',shared['entries']))
        predicates={t.span() for t in targets if shared.start()<=t.start() and t.end()<=shared.end()}
        if len(entries)<2 or len(predicates)!=1:return None
        for item in entries:
            products=assigned_subjects(item['subjects'])
            parsed=re.fullmatch(r'(?:(\d{2,3})年)?(\d{1,2})月(\d{1,2})日',item['day'])
            if not products or parsed is None:return None
            if parsed[1]:year=int(parsed[1])+1911
            elif len(years)==1:year=next(iter(years))
            else:return None
            try:day=str(date(year,int(parsed[2]),int(parsed[3])))
            except ValueError:return None
            proof={} if parsed[1] else dict(year_binding=(
                'unique_explicit_roc_year_excluding_numbered_legal_reference'
                if year_context!=dense else 'unique_explicit_roc_year_in_notice'))
            rows.append(dict(products=products,date_iso=day,reference=shared['reference'],
                evidence=shared[0],shared_phase_evidence='一般交易時段結束後',**proof))
        covered.update(predicates); shared_spans.append(shared.span())
        expected_rows+=len(entries)-1
    matches=[m for m in re.finditer(pattern,dense)
             if not any(start<=m.start() and m.end()<=end for start,end in shared_spans)]
    for m in matches:
        products=assigned_subjects(m['subjects'])
        if not products:return None
        covered.update(t.span() for t in targets if m.start()<=t.start() and t.end()<=m.end())
        rows.append(dict(products=products,date_iso=str(roc_date(re.search(ROC_DATE,m['day']))),
            reference=m['reference'],evidence=m[0]))
    # A shared date can precede multiple independently named before references.
    # Own the entire comma-connected predicate, and reject an omitted/unknown
    # subject rather than accepting just the recognizable half of the notice.
    item='(?P<subjects>'+subject+')恢復為(?P<reference>'+reference+r')?'+prior
    for clock in clocks:
        if not re.match('[,，]',dense[clock.end():]):continue
        stop=re.search('[。;；]',dense[clock.end():])
        end=clock.end()+stop.start() if stop else len(dense)
        body=dense[clock.end()+1:end]
        plain=subject+r'恢復為(?:'+reference+r')?'+prior
        if not re.fullmatch(plain+r'(?:(?:[,，、]|及|與|暨)'+plain+r')*',body):continue
        day=str(roc_date(re.search(ROC_DATE,clock[0])))
        for m in re.finditer(item,body):
            products=assigned_subjects(m['subjects'])
            if not products:return None
            offset=clock.end()+1
            covered.update(t.span() for t in targets
                if offset+m.start()<=t.start() and t.end()<=offset+m.end())
            rows.append(dict(products=products,date_iso=day,reference=m['reference'],
                evidence=clock[0]+','+m[0]))
    # Dates inside a before reference or a revocation are not restoration
    # clocks. Every restoration predicate must have exactly one owner.
    if len(rows)!=expected_rows or covered!={t.span() for t in targets}:return None
    owners={}
    for row in rows:
        for product in row['products']:
            owners.setdefault(product,set()).add((row['date_iso'],row['reference']))
    if any(len(assignments)!=1 for assignments in owners.values()):return None
    return rows


def product_margin_restoration_clock(text, product, clock):
    """Bind explicitly named restoration clauses to their own products.

    The common start clock is unchanged. A revocation of a previous notice is
    not the end of this notice. Unnamed or ambiguous clauses keep the old guard.
    This identifies the scheduled boundary, not completion of a disposition.
    """
    dense = compact(text)
    extensions = set(re.findall(r'本次延長保證金調整期間之商品為'
        r'[^。;；()]{1,48}\(([A-Z][A-Z0-9]{1,2})\)', dense))
    if product in extensions:
        # A different product may have a new start in this same notice. This
        # product continues its numbered prior notice; only a source-bound
        # join to that prior start/expiry can establish the added interval.
        return dict(clock,effective_date=None,effective_phase='unresolved',chronological=False,
            clock_error='period_extension_requires_prior_notice_join',
            extension_prior_notice_join_required=True)
    assigned=named_margin_restoration_clauses(dense,publication_bound=bool(clock.get('issue_date_bound')))
    if assigned is not None:
        # A shared predicate owns the complete list. One impossible date makes
        # that joint assignment ambiguous even for its otherwise valid peers.
        if any(r.get('shared_phase_evidence')
               and r['date_iso'] <= (clock.get('effective_date') or '9999') for r in assigned):
            return clock
        own=[r for r in assigned if product in r['products']]
        if (own and len({r['date_iso'] for r in own})==1
                and all(r['date_iso']>(clock.get('effective_date') or '9999') for r in own)):
            ends=[dict(date_iso=r['date_iso'],boundary='after_regular_session',role='effective_end',
                evidence=r['evidence'],product_scope=product,
                **{k:r[k] for k in ('year_binding','shared_phase_evidence') if k in r}) for r in own]
            return dict(clock,requires_reversion_review=True,
                temporary_end_evidence=json.dumps(ends,ensure_ascii=False),
                restoration_clock_scope='explicit_product_clause')
        return clock
    names = {}
    for m in re.finditer(r'(?:^|單位[:：][^A-Z]{0,24})([A-Z]{2}F)\(([^()]{1,24}期貨)\)', dense):
        names[m[1]] = m[2]
    ends = []; ambiguous = False
    code = r'[A-Z]{2}F|TX|MTX|TE|TF'
    entry = r'[\u4e00-\u9fff]{1,24}期貨(?:契約)?\((?:'+code+r')\)於'+ROC_DATE
    for clause in re.split(r'[。;；]', dense):
        if '恢復' not in clause:
            continue
        shared = list(re.finditer('(?P<entries>'+entry+'(?:[,，、]'+entry+
            r')+)[,，]該契約一般交易時段結束後恢復為', clause))
        if shared:
            assignments = {}
            if len(shared) != 1:
                ambiguous = True
                continue
            for item in re.finditer(r'\(('+code+r')\)於'+ROC_DATE, shared[0]['entries']):
                day = str(roc_date(re.search(ROC_DATE, item[0])))
                if item[1] in assignments or day <= (clock.get('effective_date') or '9999'):
                    ambiguous = True
                assignments[item[1]] = day
            if product in assignments:
                ends.append(dict(date_iso=assignments[product], boundary='after_regular_session',
                    role='effective_end', evidence=shared[0][0],
                    product_scope=product, shared_phase_evidence='該契約一般交易時段結束後恢復為'))
            continue
        dates = [m for m in re.finditer(ROC_DATE + r'一般交易時段結束後', clause)
                 if not re.match(r'(?:起|實施|生效)', clause[m.end():])]
        if not dates:
            continue
        codes = set(re.findall(r'\(([A-Z]{2}F|TX|MTX|TE|TF)\)', clause))
        codes.update(code for code, name in names.items() if name in clause)
        if not codes or len({str(roc_date(m)) for m in dates}) != 1:
            ambiguous = True
            continue
        if product in codes:
            ends.append(dict(date_iso=str(roc_date(dates[0])), boundary='after_regular_session',
                role='effective_end', evidence=clause))
    if not ends or ambiguous:
        return clock
    return dict(clock, requires_reversion_review=True,
        temporary_end_evidence=json.dumps(ends, ensure_ascii=False),
        restoration_clock_scope='explicit_product_clause')


def bind_native_margin_period_extension(archive, text, amendment, facts):
    """Join one native numbered period amendment without changing older clocks.

    The additional interval starts at the prior scheduled expiry. The original
    raised interval stays intact, and conditional restoration still requires
    the canonical completed-session evidence. Unmatched amounts, references,
    starts, dates or document numbers remain unresolved.
    """
    if (not amendment.get('extension_prior_notice_join_required')
            or amendment.get('before_column_semantics')!='explicit_non_disposed_base'
            or amendment.get('margin_kind')!='notional_rate'
            or not amendment.get('issue_date_bound')):
        return None
    try:
        check_publication(text,amendment['published_date'])
        known=timestamp(date.fromisoformat(amendment['published_date']),'23:59:59')
        if amendment['known_at']!=known:return None
        if not re.fullmatch('[a-f0-9]{64}',amendment['source_content_sha256']):return None
    except (ValueError,KeyError,TypeError):
        return None
    clause_pattern=(r'本次延長保證金調整期間之商品為[^。;；()]{1,48}'
        r'\((?P<product>[A-Z][A-Z0-9]{1,2})\)[^。;；]{0,80}?本公司'
        r'(?P<prior_day>'+ROC_DATE+r')[臺台]期結字第(?P<number>\d{9,12})號函'
        r'[^。;；]{0,120}?延長至(?P<end>'+ROC_DATE+r')一般交易時段結束後[,，]'
        r'恢復為(?P<reference>'+ROC_DATE+r')調整前之保證金')
    matches=[m for m in re.finditer(clause_pattern,compact(text))
             if m['product']==amendment['product']]
    if len(matches)!=1:return None
    match=matches[0]
    day=lambda name:str(roc_date(re.search(ROC_DATE,match[name])))
    prior_day,reference,end=day('prior_day'),day('reference'),day('end')
    bound=[]
    for prior in facts:
        if (prior['product']!=amendment['product'] or prior.get('published_date')!=prior_day
                or prior.get('effective_date')!=reference
                or prior.get('effective_phase')!='after_product_regular_close'
                or prior.get('margin_kind')!='notional_rate'
                or not prior.get('issue_date_bound')
                or prior.get('after')!=amendment.get('after')
                or prior.get('before')!=amendment.get('declared_non_disposed_margin')):
            continue
        doc=archive.document(prior['source_url'])
        if doc['content_sha256']!=prior.get('source_content_sha256'):continue
        prior_text=compact(doc['text'])
        if not re.search(r'發文字號:[臺台]期結字第'+re.escape(match['number'])+r'號',prior_text):
            continue
        check_publication(doc['text'],prior_day)
        ends=json.loads(prior.get('temporary_end_evidence') or '[]')
        ends={(e['date_iso'],e['boundary']) for e in ends}
        if len(ends)!=1:continue
        start,boundary=next(iter(ends))
        if (boundary!='after_regular_session' or not reference<start<end
                or not prior_day<amendment['published_date']<start
                or prior.get('known_at')!=timestamp(date.fromisoformat(prior_day),'23:59:59')):
            continue
        bound.append((prior,start))
    signatures={(p['source_content_sha256'],s) for p,s in bound}
    if len(signatures)!=1:return None
    prior,start=bound[0]
    evidence=dict(rule='native_numbered_extension_from_prior_scheduled_expiry',
        prior_notice_number=match['number'],prior_publication_date=prior_day,
        prior_source_content_sha256=prior['source_content_sha256'],prior_known_at=prior['known_at'],
        prior_effective_date=reference,prior_scheduled_expiry=start,
        amendment_source_content_sha256=amendment['source_content_sha256'],
        amendment_known_at=known,scheduled_restoration=end,clause=match[0])
    return dict(amendment,effective_date=start,effective_phase='after_product_regular_close',
        before=list(prior['after']),event_type='before_after',clock_error=None,chronological=True,
        extension_prior_notice_join_required=False,requires_reversion_review=True,
        temporary_end_evidence=json.dumps([dict(date_iso=end,boundary='after_regular_session',
            role='effective_end',evidence=match[0],product_scope=amendment['product'])],ensure_ascii=False),
        extension_join_evidence=json.dumps(evidence,ensure_ascii=False),
        extraction='native_numbered_margin_period_extension',point_in_time_verified=False,
        candidate_only=True)


def bind_single_session_margin_clocks(archive, facts, path):
    """Resolve a generic trading close only inside a reviewed single-session era."""
    review = json.loads(path.read_text())
    if review.get('review_kind') != 'source_bound_single_regular_session':
        raise ValueError('unsupported margin session review')
    start, end = review['effective_date'], review['valid_until_exclusive']
    if date.fromisoformat(start) >= date.fromisoformat(end):
        raise ValueError('invalid single-session review interval')
    known = datetime.fromisoformat(review['known_at'])
    if known.tzinfo is None or known.date() >= date.fromisoformat(start):
        raise ValueError('single-session rule requires prior publication')
    for source in review['sources']:
        doc = archive.document(source['source_url'])
        if doc['content_sha256'] != source['content_sha256']:
            raise ValueError('single-session source SHA mismatch')
        dense = compact(doc['text'])
        if not all(compact(clause) in dense for clause in source['required_clauses']):
            raise ValueError('single-session source clause mismatch')
    universe_path = Path(review['universe']['path'])
    if not universe_path.exists():
        retained=[archive.bundle/s['path'] for s in getattr(archive,'sources',{}).values()
                  if s['kind']=='single_session_product_scope'
                  and s['sha256']==review['universe']['sha256']]
        if retained:
            universe_path=retained[0]
    if sha256_file(universe_path) != review['universe']['sha256']:
        raise ValueError('single-session product scope SHA mismatch')
    universe = pl.read_csv(universe_path, infer_schema=False)
    products = set(universe.filter(pl.col('underlying_security_type') == 'stock')['product'])
    if not products or set(review['products']) != products:
        raise ValueError('single-session review must exactly bind stock product scope')
    archive.copy(universe_path, review['universe']['sha256'], url='', kind='single_session_product_scope')
    digest = sha256_file(path)
    archive.copy(path, digest, url='', kind='single_session_rule_review')
    changed = []
    for fact in facts:
        day = fact.get('effective_date')
        if fact['product'] not in products or not day or not start <= day < end:
            continue
        if fact.get('effective_phase') != 'after_product_trading_session_unspecified':
            continue
        ends = json.loads(fact.get('temporary_end_evidence') or '[]')
        if any(not start <= e['date_iso'] < end for e in ends):
            continue
        fact['effective_phase'] = 'after_product_regular_close'
        for e in ends:
            if e['boundary'] == 'after_trading_session':
                e['boundary'] = 'after_regular_session'
        fact['temporary_end_evidence'] = json.dumps(ends, ensure_ascii=False)
        fact['single_session_rule_review_sha256'] = digest
        changed.append(dict(product=fact['product'], effective_date=day,
            source_content_sha256=fact['source_content_sha256']))
    atomic_write_json(archive.bundle/'single_session_clock_repairs.json', changed)
    return changed


def bind_retained_single_session_margin_clocks(archive, facts, explicit=None):
    """Reapply the original dated proof after any amount/clock re-extraction."""
    reviews=[]
    for source in list(archive.sources.values()):
        if source['kind']!='single_session_rule_review':
            continue
        path=archive.bundle/source['path']
        if sha256_file(path)!=source['sha256']:
            raise ValueError('retained single-session review SHA mismatch')
        reviews.append(path)
    if explicit is not None:
        reviews.append(explicit)
    reviews=list({sha256_file(path):path for path in reviews}.values())
    changed=[]
    for path in reviews:
        changed.extend(bind_single_session_margin_clocks(archive,facts,path))
    if reviews:
        atomic_write_json(archive.bundle/'single_session_clock_repairs.json',changed)
    return changed


def retained_document_table_views(archive, url, document, sources):
    """Read retained cell grids only with their matching original-source receipt."""
    for table in sources:
        if table['kind'] not in ('native_table_tables.json', 'ocr_table_tables.json'):
            continue
        path = archive.bundle / table['path']
        if sha256_file(path) != table['sha256']:
            raise ValueError('retained table SHA mismatch')
        receipt_kind = table['kind'].replace('tables.json', 'receipt.json')
        bound = False
        for receipt in sources:
            if receipt['kind'] != receipt_kind:
                continue
            proof_path = archive.bundle / receipt['path']
            if sha256_file(proof_path) != receipt['sha256']:
                raise ValueError('retained table receipt SHA mismatch')
            proof = json.loads(proof_path.read_text())
            if (proof.get('status') == 'complete'
                    and proof.get('content_sha256') == document['content_sha256']
                    and any(f['path'] == 'tables.json' and f['sha256'] == table['sha256']
                            for f in proof.get('files', []))):
                bound = True
        if not bound:
            raise ValueError('retained table lacks matching original-source receipt')
        yield json.loads(path.read_text())['pages'], table['kind'] + ':' + table['sha256']


def reextract_retained_margin_text(archive):
    """Read all retained amount text again after a parser fix, preserving sources."""
    retained={}
    for source in archive.sources.values():
        retained.setdefault(source['url'], []).append(source)
    rows=[]
    query='''SELECT DISTINCT d.url,a.published_date,a.url announcement_url
        FROM announcements a JOIN (SELECT url parent,url child FROM announcements
            UNION SELECT parent,child FROM links) l ON l.parent=a.url
        JOIN documents d ON d.url=l.child WHERE d.state='complete'
        ORDER BY a.published_date,d.url'''
    for notice in archive.conn.execute(query):
        doc=archive.document(notice['url'])
        source_views = retained.get(notice['url'], [])
        grid_facts = []
        for pages, extraction in retained_document_table_views(archive, notice['url'], doc, source_views):
            source_text = '\n'.join(p.get('ocr_text') or p['native_text'] for p in pages)
            clock = candidate_notice_clock(source_text, notice['published_date'])
            for page in pages:
                for index, table in enumerate(page['tables']):
                    for fact in margin_grid_candidates(table['cells'], table.get('caption', ''),
                                                       source_text=source_text):
                        scoped_clock = product_margin_restoration_clock(source_text, fact['product'], clock)
                        grid_facts.append(dict(fact, **scoped_clock, page=page['page'], table_index=index,
                            extraction_method='retained_source_cell_grid', source_url=notice['url'],
                            announcement_url=notice['announcement_url'], published_date=notice['published_date'],
                            source_content_sha256=doc['content_sha256'],
                            known_at=timestamp(date.fromisoformat(notice['published_date']), '23:59:59'),
                            extraction='canonical_retained_margin_grid:' + extraction, point_in_time_verified=False,
                            requires_dated_session_rule=True))
        rows.extend(grid_facts)
        owned = {(f['product'], f['margin_kind']) for f in grid_facts}
        for text, _ in retained_document_text_views(archive, notice['url'], doc,
                                                   sources=source_views):
            facts=margin_table_candidates(text)
            if doc.get('format')=='legacy_word':facts.extend(margin_legacy_word_levels(text))
            if not facts:continue
            clock=candidate_notice_clock(text,notice['published_date'])
            for fact in facts:
                if (fact['product'], fact['margin_kind']) in owned:
                    continue
                scoped_clock=product_margin_restoration_clock(text,fact['product'],clock)
                rows.append(dict(fact,**scoped_clock,source_url=notice['url'],announcement_url=notice['announcement_url'],
                    published_date=notice['published_date'],source_content_sha256=doc['content_sha256'],
                    known_at=timestamp(date.fromisoformat(notice['published_date']),'23:59:59'),
                    extraction='canonical_retained_margin_reextraction',point_in_time_verified=False,
                    requires_dated_session_rule=True))
    return rows


def has_local_before_restoration(views, effective_date):
    """An earlier dated reference cannot mean the current table's before level."""
    for text in views:
        if any(str(roc_date(m)) != effective_date
               for m in re.finditer(r'恢復為'+ROC_DATE+r'調整前', text)):
            return False
        if re.search(r'恢復為\d{1,2}月\d{1,2}日調整前', text):
            return False
    return any(re.search(r'恢復為(?:'+ROC_DATE+r')?調整前(?:之)?保證金', text) for text in views)


def preceding_restoration_products(prefix):
    """The latest named subject owns the clause; retain a connected code list."""
    mentions = list(re.finditer(r'\(([A-Z]{2}F)\)', prefix))
    if not mentions:
        return set()
    products = {mentions[-1][1]}
    right = mentions[-1]
    for left in reversed(mentions[:-1]):
        connection = prefix[left.end():right.start()]
        if not re.fullmatch(r'(?:[、,，]+(?:及|與|和)?|及|與|和)[^()。;；]{0,24}期貨(?:契約)?', connection):
            break
        products.add(left[1]); right = left
    return products


def referenced_before_restoration(views, fact, facts):
    """Resolve an explicitly named product's reference to an older before column."""
    if not fact.get('effective_date') or not fact.get('known_at'):
        return None
    references=set(); clauses=[]
    pattern=r'恢復為(?P<ref>(?:\d{2,3}年)?\d{1,2}月\d{1,2}日)?(?:調整前|標的證券未經處置前)(?:之)?保證金'
    for text in views:
        assigned=named_margin_restoration_clauses(text,publication_bound=bool(fact.get('issue_date_bound')))
        if assigned is not None:
            for clause in assigned:
                if fact['product'] not in clause['products']:continue
                ref=clause['reference']
                if (not ref and fact.get('declared_non_disposed_margin')
                        and '標的證券未經處置前' in clause['evidence']):
                    # An undated non-disposed base is not this event's before
                    # column. A dated reference elsewhere in the same source
                    # must still prove the base and agree with its table.
                    clauses.append(clause['evidence']);continue
                if not ref:day=fact['effective_date']
                elif full:=re.fullmatch(ROC_DATE,ref):day=str(roc_date(full))
                else:
                    context=operative_notice_year_context(text,publication_bound=bool(fact.get('issue_date_bound')))
                    years={int(x[1])+1911 for x in re.finditer(ROC_DATE,context)}
                    if len(years)!=1:continue
                    short=re.fullmatch(r'(\d{1,2})月(\d{1,2})日',ref)
                    day=str(date(next(iter(years)),int(short[1]),int(short[2])))
                if day>fact['effective_date']:return None
                references.add(day);clauses.append(clause['evidence'])
            continue
        count=len(list(re.finditer(pattern,text)))
        for m in re.finditer(pattern,text):
            prefix=text[:m.start()]
            products=preceding_restoration_products(prefix)
            if fact['product'] not in products:
                if products or count!=1:
                    continue
            ref=m['ref']
            if (not ref and fact.get('declared_non_disposed_margin')
                    and '標的證券未經處置前' in m[0]):
                clauses.append(prefix[-1200:]+m[0]);continue
            if not ref:
                day=fact['effective_date']
            elif full:=re.fullmatch(ROC_DATE,ref):
                day=str(roc_date(full))
            else:
                context=operative_notice_year_context(text,publication_bound=bool(fact.get('issue_date_bound')))
                years={int(x[1])+1911 for x in re.finditer(ROC_DATE,context)}
                if len(years)!=1:continue
                short=re.fullmatch(r'(\d{1,2})月(\d{1,2})日',ref)
                day=str(date(next(iter(years)),int(short[1]),int(short[2])))
            if day>fact['effective_date']:return None
            references.add(day);clauses.append(prefix[-1200:]+m[0])
    if len(references)!=1:return None
    day=references.pop()
    if day==fact['effective_date']:
        if not fact.get('before'):return None
        return dict(restoration_target=list(fact['before']),restoration_rule='return_to_declared_before')
    prior=[r for r in facts if r['product']==fact['product'] and r.get('effective_date')==day
        and r.get('margin_kind')==fact['margin_kind'] and r.get('issue_date_bound')
        and r.get('before') and r.get('known_at') and r['known_at']<=fact['known_at']]
    values={tuple(r['before']) for r in prior}
    if len(values)!=1:return None
    target=next(iter(values))
    if fact.get('declared_non_disposed_margin') and tuple(fact['declared_non_disposed_margin'])!=target:
        return None
    if (len(target)!=3 or not all(isinstance(v,(int,float)) and math.isfinite(v) and v>0 for v in target)
            or not target[0]>=target[1]>=target[2]):return None
    evidence=dict(referenced_effective_date=day,reference_clauses=clauses,
        source_content_sha256s=sorted({r['source_content_sha256'] for r in prior}),
        known_at=max(r['known_at'] for r in prior))
    return dict(restoration_rule='return_to_referenced_before',restoration_target=list(target),
        restoration_reference_evidence=json.dumps(evidence,ensure_ascii=False))


def has_closed_cash_postponement_clause(views):
    clause=compact('調整期間如遇休市、有價證券停止買賣、全日暫停交易，則恢復日順延執行')
    return any(clause in compact(text) for text in views)


def verified_disposal_restorations(archive, facts, path, *, source_urls=None):
    """Attach observed conditional events without treating provider rows as law."""
    evidence=json.loads(path.read_text())
    loaded={}
    for key in ('universe','dispositions','observations'):
        source=evidence[key];src=Path(source['path'])
        if sha256_file(src)!=source['sha256']:raise ValueError('restoration input SHA mismatch')
        loaded[key]=pl.read_csv(src,infer_schema=False) if key=='universe' else pl.read_parquet(src)
        archive.copy(src,source['sha256'],url='',kind='restoration_'+key)
    for source in evidence['sources']:
        archive.copy(Path(source['path']),source['sha256'],url=source.get('url',''),kind=source['kind'])
    texts={};verified=[]
    for row in facts:
        row=dict(row)
        if source_urls is not None and row.get('source_url') not in source_urls:
            # Retain intervening-rule context without re-proving a different
            # original against this possibly partial disposition input.
            row['requires_reversion_review']=False
            verified.append(row)
            continue
        if row.get('requires_reversion_review'):
            url=row['source_url']
            if url not in texts:
                doc=archive.document(url)
                texts[url]=(doc['content_sha256'],[compact(t) for t,k in retained_document_text_views(archive,url,doc)])
            digest,views=texts[url]
            if digest!=row['source_content_sha256']:raise ValueError('restoration notice SHA mismatch')
            if row.get('source_review_clock_text'):
                if (row.get('extraction')!='source_bound_visual_transcription'
                        or not re.fullmatch('[a-f0-9]{64}',row.get('visual_review_sha256') or '')):
                    raise ValueError('restoration review context lacks its visual source binding')
                reviews=[s for s in archive.sources.values() if s['kind']=='visual_source_review'
                         and s['sha256']==row['visual_review_sha256']]
                bound=False
                for proof in reviews:
                    file=archive.bundle/proof['path']
                    if sha256_file(file)!=proof['sha256']:raise ValueError('restoration review SHA mismatch')
                    review_payload=json.loads(file.read_text())
                    bound=bound or any(r.get('source_url')==url
                        and r.get('content_sha256')==digest
                        and r.get('published_date')==row['published_date']
                        and r.get('transcribed_text')==row['source_review_clock_text']
                        for r in review_payload.get('reviews',[review_payload]))
                if not bound:raise ValueError('restoration review context differs from its retained review')
                views=[compact(row['source_review_clock_text'])]
            if has_closed_cash_postponement_clause(views):
                row['restoration_delay_rule']='postpone_for_closed_or_full_day_halted_cash_sessions'
            # A notice can restore a level from an EARLIER adjustment, not its
            # own before column. Do not turn any matching sibling clause into
            # permission to restore this product's immediately preceding level.
            # Explicit earlier references require a composed source-bound chain.
            target=referenced_before_restoration(views,row,facts)
            if target:
                row.update(target)
            elif row.get('before') and has_local_before_restoration(views, row['effective_date']):
                row['restoration_rule']='return_to_declared_before'
            else:
                row.pop('restoration_rule', None)
                row.pop('restoration_target', None)
            if row.get('temporary_end_postponement_evidence'):
                proof=json.loads(row['temporary_end_postponement_evidence'])
                delay=proof.get('delayed_for_official_security_halt') or proof.get('delayed_for_official_closure') or {}
                prior=json.loads(row.get('original_temporary_end_evidence') or '[]')
                ends=json.loads(row.get('temporary_end_evidence') or '[]')
                if (not delay.get('nominal_end') or not delay.get('actual_end')
                        or not prior or not ends
                        or any(e['date_iso']!=delay['nominal_end'] for e in prior)
                        or any(e['date_iso']!=delay['actual_end'] for e in ends)):
                    raise ValueError('retained closure postponement lacks its nominal boundary')
                # Re-prove the disposition from its announced boundary. The
                # original facts keep their already verified actual boundary;
                # only this local validation copy uses the nominal date.
                row['temporary_end_evidence']=json.dumps(prior,ensure_ascii=False)
        verified.append(row)
    # An abbreviated news view can omit a condition stated in the formal
    # original for the very same event. Compose only exact product, amount,
    # clock and nominal-restoration identities; never borrow a nearby notice.
    def instruction_key(row):
        if (not row.get('before') or not row.get('after')
                or row.get('restoration_rule')!='return_to_declared_before'
                or not row.get('requires_reversion_review') or not row.get('issue_date_bound')
                or any(not row.get(k) for k in ('product','margin_kind','effective_date',
                    'effective_phase','published_date','known_at'))):
            return None
        declared=json.loads(row.get('temporary_end_evidence') or '[]')
        if any(not e.get('date_iso') or not e.get('boundary') for e in declared):return None
        ends={(e['date_iso'],e['boundary']) for e in declared}
        if len(ends)!=1:return None
        return (row['product'],row['margin_kind'],tuple(row['before']),tuple(row['after']),
            row.get('effective_date'),row.get('effective_phase'),row.get('published_date'),row.get('known_at'),
            tuple(sorted(ends)))
    instructions={}
    for row in verified:
        key=instruction_key(row)
        if key and row.get('restoration_delay_rule')=='postpone_for_closed_or_full_day_halted_cash_sessions':
            instructions.setdefault(key,[]).append(row)
    for row in verified:
        if source_urls is not None and row.get('source_url') not in source_urls:continue
        key=instruction_key(row)
        matching=instructions.get(key,[]) if key else []
        if matching and not row.get('restoration_delay_rule'):
            row['restoration_delay_rule']='postpone_for_closed_or_full_day_halted_cash_sessions'
            row['restoration_instruction_evidence']=json.dumps(dict(
                composition='exact_same_product_amount_phase_publication_and_nominal_end',
                source_content_sha256s=sorted({r['source_content_sha256'] for r in matching}),
                source_urls=sorted({r['source_url'] for r in matching}),known_at=row['known_at']),ensure_ascii=False)
    rows,issues=disposal_margin_restorations(verified,loaded['universe'],loaded['dispositions'],loaded['observations'],
        market_closures=evidence.get('market_closures',[]),
        security_full_day_halts=evidence.get('security_full_day_halts',[]))
    changes=[];halt_changes=[]
    for restored in rows:
        proof=json.loads(restored['restoration_evidence'])
        delay=proof.get('delayed_for_official_security_halt') or proof.get('delayed_for_official_closure')
        if not delay:continue
        original=[r for r in facts if r['product']==restored['product']
                  and r['source_content_sha256']==restored['source_content_sha256']
                  and r.get('known_at')==restored['original_rule_known_at']
                  and r.get('requires_reversion_review')]
        if not original:raise ValueError('delayed restoration has no matching original margin boundary')
        for row in original:
            ends=json.loads(row.get('temporary_end_evidence') or '[]')
            if ends and all(e['date_iso']==delay['actual_end'] for e in ends):
                # A retained parent can already carry this exact observed
                # postponement while a fresh source view still has the
                # nominal date. Verify its original boundary before reusing
                # it; applying the same closure twice must not move it again.
                prior=json.loads(row.get('original_temporary_end_evidence') or '[]')
                if not prior or any(e['date_iso']!=delay['nominal_end'] for e in prior):
                    raise ValueError('retained closure postponement lacks its nominal boundary')
                row['temporary_end_postponement_evidence']=restored['restoration_evidence']
                continue
            if not ends or any(e['date_iso']!=delay['nominal_end'] for e in ends):
                raise ValueError('closure postponement cannot revise an unrelated boundary')
            row['original_temporary_end_evidence']=row['temporary_end_evidence']
            row['temporary_end_evidence']=json.dumps([dict(e,date_iso=delay['actual_end']) for e in ends],ensure_ascii=False)
            row['temporary_end_postponement_evidence']=restored['restoration_evidence']
        destination=halt_changes if delay.get('official_security_halts') else changes
        destination.append(dict(product=restored['product'],source_content_sha256=restored['source_content_sha256'],**delay))
    archive.copy(path,sha256_file(path),url='',kind='restoration_evidence_manifest')
    atomic_write_json(archive.bundle/'restoration_condition_issues.json',issues)
    atomic_write_json(archive.bundle/'restoration_closure_postponements.json',changes)
    atomic_write_json(archive.bundle/'restoration_security_halt_postponements.json',halt_changes)
    return rows


def rebuild_disposal_restorations(archive,facts,path,*,source_urls=None):
    """Replace selected originals' restorations; retain verified parents."""
    retained=[r for r in facts if r.get('extraction')!='observed_disposition_conditional_restoration'
              or (source_urls is not None and r.get('source_url') not in source_urls)]
    return retained+verified_disposal_restorations(archive,retained,path,source_urls=source_urls)


def retain_review_override(archive, root):
    """Replace extraction, not source history; verify every declared page file."""
    manifest=json.loads((root/'manifest.json').read_text())
    if manifest['status'] not in ('complete','partial') or manifest['point_in_time_verified'] is not False:
        raise ValueError('review override must be an unapproved extraction with explicit status')
    # A failed document must remain a gap, but cannot invalidate independent
    # complete source documents. Retain the original partial manifest unchanged.
    # Never admit a partially extracted document or a digest marked as failed.
    failed={f['content_sha256'] for f in manifest.get('failures',[])}
    admitted=[]; skipped=[]
    for record in manifest['documents']:
        if record['status']!='complete' or record['content_sha256'] in failed:
            skipped.append(record['content_sha256'])
            continue
        folder=root/'documents'/record['content_sha256']
        proof=json.loads((folder/'receipt.json').read_text())
        doc=archive.document(proof['url'])
        if proof['content_sha256']!=doc['content_sha256'] or proof['status']!='complete':
            raise ValueError('review override source identity mismatch')
        for entry in proof['files']:
            source=folder/entry['path']
            if not source.resolve().is_relative_to(folder.resolve()) or sha256_file(source)!=entry['sha256']:
                raise ValueError('review override page SHA mismatch')
        archive.copy(folder/'candidate.txt',sha256_file(folder/'candidate.txt'),url=proof['url'],
                     kind='review_pages_including_ocr' if any(p['extraction']=='ocr_candidate' for p in proof['pages']) else 'review_native')
        archive.copy(folder/'receipt.json',sha256_file(folder/'receipt.json'),url=proof['url'],kind='page_extraction_receipt')
        admitted.append(record['content_sha256'])
    archive.copy(root/'manifest.json',sha256_file(root/'manifest.json'),url='',kind='review_override_manifest')
    return dict(manifest=str(root/'manifest.json'),manifest_sha256=sha256_file(root/'manifest.json'),
        source_status=manifest['status'],admitted_complete_documents=len(admitted),
        skipped_partial_documents=skipped,failed_documents=sorted(failed),
        unprocessed_documents=manifest.get('unprocessed_documents',0))


def refresh_corporate_candidates(archive, rows):
    """Re-read the exact retained table, without borrowing another row's scope."""
    by_sha={s['sha256']:s for s in archive.sources.values()}
    native={}
    for source in archive.sources.values():
        if source['kind']!='native_table_receipt.json': continue
        proof=json.loads((archive.bundle/source['path']).read_text())
        table=[f for f in proof['files'] if f['path']=='tables.json']
        if proof['status']=='complete' and len(table)==1:
            native[proof['content_sha256']]=by_sha[table[0]['sha256']]
    documents={}
    for row in rows:
        if row.get('extraction_method') not in ('native_cell_grid',None):
            # Text candidates have no native cell coordinates. Their retained
            # source and page receipt were verified by reuse_candidate_bundle;
            # --extract-corporate-text re-extracts them separately below.
            continue
        digest=row['source_content_sha256']
        if digest not in native:
            raise ValueError('corporate source receipt differs from extracted table')
        if digest not in documents:
            tables=json.loads((archive.bundle/native[digest]['path']).read_text())
            documents[digest]=(tables,'\n'.join(p['native_text'] for p in tables['pages']))
        tables,text=documents[digest]
        refreshed=[fact for fact in corporate_native_table_candidates(tables['pages'])
                   if fact['page']==row['page'] and fact['table_index']==row['table_index']
                   and all(fact[k]==row[k] for k in
                           ('product','from_product','to_product','contract_multiplier'))]
        if len(refreshed)!=1:
            raise ValueError('corporate table identity or deliverable changed')
        row.update(refreshed[0])
    return len(documents)


def refresh_candidate_clocks(archive, facts, positions, corporate=()):
    """Rebind clocks to the exact retained text, preserving manual amendments."""
    by_url={}
    for source in archive.sources.values():
        by_url.setdefault(source['url'],{})[source['kind']]=source
    clocks={};clock_texts={}
    def clock(row):
        key=(row['source_url'],row['source_content_sha256'],row['published_date'])
        if key in clocks:
            return (product_margin_restoration_clock(clock_texts[key],row['product'],clocks[key])
                    if row.get('product') else clocks[key])
        doc=archive.document(row['source_url'])
        if doc['content_sha256']!=row['source_content_sha256']:
            raise ValueError('clock source bytes differ from candidate source')
        retained=by_url.get(row['source_url'],{})
        texts=[text for text,_ in retained_document_text_views(archive,row['source_url'],doc)]
        native=retained.get('native_table_tables.json')
        if native:
            proof=json.loads((archive.bundle/retained['native_table_receipt.json']['path']).read_text())
            if proof['content_sha256']!=doc['content_sha256'] or proof['status']!='complete':
                raise ValueError('native clock text has different source or incomplete receipt')
            tables=json.loads((archive.bundle/native['path']).read_text())
            candidate='\n'.join(page['native_text'] for page in tables['pages'])
            if candidate.strip(): texts.append(candidate)
        # A scanned issuing page can accompany machine-readable amount tables.
        # Partial native text must not hide the same document's reviewed page.
        # Conflicting dates remain ambiguous through the canonical set checks.
        clock_texts[key]='\n'.join(texts)
        clocks[key]=candidate_notice_clock(clock_texts[key],row['published_date'])
        return (product_margin_restoration_clock(clock_texts[key],row['product'],clocks[key])
                if row.get('product') else clocks[key])
    for row in facts:
        if row.get('extraction')=='source_bound_visual_transcription': continue
        row.update(clock(row))
    for row in positions:
        if row.get('extraction') in ('source_bound_visual_transcription','source_bound_visual_corporate_cells'):continue
        result=clock(row)
        row['issue_date_bound']=result['issue_date_bound']
        if row.get('effective_date') is None and result['effective_phase']=='new_contract_listing':
            row['effective_date']=result['effective_date'];row['effective_phase']='new_contract_listing'
    for row in corporate:
        if row.get('extraction')=='source_bound_visual_corporate_cells':continue
        row['issue_date_bound']=clock(row)['issue_date_bound']
    return len(clocks)


def repair_missing_margin_source_context(archive, facts):
    """Bind incomplete table clocks to the other verified views of that original.

    Existing product-local clocks and observed restorations retain their
    meaning. An omitted end may be restored only at the identical legal start.
    A conflicting or still incomplete clock remains a gap;
    neither amounts nor publication metadata are changed by this repair.
    """
    phases = {'product_regular_open', 'new_contract_listing', 'after_product_regular_close'}
    candidate_phases = phases | {'after_product_trading_session_unspecified'}
    missing_end=lambda row:(row.get('temporary_end_evidence') or '').strip() in ('','[]','null')
    def multiple_ends(row):
        try:
            ends=json.loads(row.get('temporary_end_evidence') or '[]')
        except (ValueError,TypeError):
            return False
        return isinstance(ends,list) and len({(e.get('date_iso'),e.get('boundary'))
            for e in ends if isinstance(e,dict)})>1
    def possible_prior_revocation(row):
        try:
            ends=json.loads(row.get('temporary_end_evidence') or '[]')
        except (ValueError,TypeError):
            return False
        return isinstance(ends,list) and any(isinstance(end,dict) and re.search(
            r'[臺台]期(?:結|交)字第\d{9,12}號函[^。;；]{0,180}停止適用',
            compact(end.get('evidence',''))) for end in ends)
    selected = [row for row in facts
        if row.get('extraction') not in ('source_bound_visual_transcription',
                                         'observed_disposition_conditional_restoration')
        and (not row.get('issue_date_bound') or not row.get('effective_date')
             or row.get('effective_phase') not in phases
             or missing_end(row) or multiple_ends(row) or possible_prior_revocation(row))]
    originals = [dict(row) for row in selected]
    refresh_candidate_clocks(archive, selected, [])
    if any(s['kind']=='single_session_rule_review' for s in getattr(archive,'sources',{}).values()):
        # Fresh text can restate an already proven regular close in generic
        # wording. Reapply the SAME retained dated proof before comparing the
        # old and new boundaries; it cannot authorize another product or era.
        bind_retained_single_session_margin_clocks(archive, selected)
    repaired = []
    for row, original in zip(selected, originals, strict=True):
        complete=(original.get('issue_date_bound') and original.get('effective_date')
                  and original.get('effective_phase') in phases)
        revoked={compact(r['evidence']) for r in json.loads(
            row.get('prior_notice_revocation_evidence') or '[]')}
        old_ends=json.loads(original.get('temporary_end_evidence') or '[]') if (
            revoked and possible_prior_revocation(original)) else []
        kept=[e for e in old_ends if compact(e.get('evidence','')) not in revoked]
        removed_prior=(complete and len(kept)<len(old_ends) and row.get('issue_date_bound')
                       and row.get('effective_date')==original['effective_date'])
        if removed_prior:
            proof=row['prior_notice_revocation_evidence']
            reversion=row['requires_reversion_review']
            scoped=row.get('restoration_clock_scope')=='explicit_product_clause'
            endpoints=json.loads(row['temporary_end_evidence']) if scoped else kept
            row.clear();row.update(original)
            row.update(temporary_end_evidence=json.dumps(endpoints,ensure_ascii=False),
                       prior_notice_revocation_evidence=proof,requires_reversion_review=reversion)
            if scoped:row['restoration_clock_scope']='explicit_product_clause'
        elif complete:
            if (not row.get('issue_date_bound') or row.get('effective_date')!=original['effective_date']
                    or row.get('effective_phase')!=original['effective_phase']
                    or not json.loads(row.get('temporary_end_evidence') or '[]')
                    or (multiple_ends(original) and
                        (row.get('restoration_clock_scope')!='explicit_product_clause' or multiple_ends(row)))):
                row.clear();row.update(original)
                continue
            endpoints=row['temporary_end_evidence']
            scoped=row.get('restoration_clock_scope')
            row.clear();row.update(original)
            row.update(temporary_end_evidence=endpoints,requires_reversion_review=True)
            if scoped:row['restoration_clock_scope']=scoped
        # Keep an exact source date even when its generic close still needs
        # the separately bound single-session proof. Interval admission rejects
        # this phase; the later canonical session binder can resolve it.
        if (not row.get('issue_date_bound') or not row.get('effective_date')
                or row.get('effective_phase') not in candidate_phases):
            row.clear(); row.update(original)
            continue
        for key in ('after', 'before', 'margin_kind', 'published_date', 'known_at',
                    'source_url', 'source_content_sha256'):
            if row.get(key) != original.get(key):
                raise ValueError('margin clock repair changed source identity or amounts')
        if row == original:
            continue
        row['margin_clock_repair_original'] = json.dumps({key: original.get(key) for key in
            ('issue_date_bound', 'effective_date', 'effective_phase', 'clock_error',
             'temporary_end_evidence','requires_reversion_review')}, ensure_ascii=False)
        repaired.append(dict(product=row['product'], source_content_sha256=row['source_content_sha256'],
            original=json.loads(row['margin_clock_repair_original']),
            effective_date=row['effective_date'], effective_phase=row['effective_phase']))
    atomic_write_json(archive.bundle/'margin_clock_context_repairs.json', repaired)
    return repaired


def retained_document_text_views(archive, url, document, *, sources=None):
    """Read every receipt-bound view of one source, without last-view priority."""
    if sources is None:
        sources=[s for s in archive.sources.values() if s['url']==url]
    receipts=[]
    for source in sources:
        if source['kind']!='page_extraction_receipt':continue
        path=archive.bundle/source['path']
        if sha256_file(path)!=source['sha256']:
            raise ValueError('retained extraction receipt SHA mismatch')
        proof=json.loads(path.read_text())
        if proof['content_sha256']==document['content_sha256'] and proof['status']=='complete':
            receipts.append(proof)
    views=[(document['text'],'canonical_native')]
    for source in sorted(sources,key=lambda s:s['sha256']):
        if source['kind'] not in ('review_native','review_pages_including_ocr','corporate_publication_text'):continue
        if not any(any(f['path']=='candidate.txt' and f['sha256']==source['sha256']
                       for f in proof['files']) for proof in receipts):
            raise ValueError('retained source text lacks a complete matching extraction receipt')
        path=archive.bundle/source['path']
        if sha256_file(path)!=source['sha256']:
            raise ValueError('retained source text SHA mismatch')
        views.append((path.read_text(),source['kind']+':'+source['sha256']))
    return list(dict.fromkeys(views))


def next_nearby_position_date(product, published, lifetimes, market_dates):
    """Resolve a literal next-nearby expiry clause from observed contract lives.

    Notices are known at publication-day close. Weekly, expired and later-listed
    contracts cannot displace the next-nearby monthly contract. Missing near
    expiries remain unresolved rather than selecting a later priced contract.
    """
    day = date.fromisoformat(published)
    active = lifetimes.filter((pl.col('product') == product)
        & pl.col('contract').str.contains(r'^\d{6}$')
        & (pl.col('first_observed_date') <= day)
        & (pl.col('last_observed_date') >= day)
        & (pl.col('calendar_end') > day))
    months = sorted(active['contract'].unique().to_list())
    if len(months) < 2:
        return None
    near = active.filter(pl.col('contract').is_in(months[:2]))
    if near.height != 2 or near['official_expiry'].null_count():
        return None
    expiry = near.filter(pl.col('contract') == months[1])['official_expiry'].item()
    later = [d for d in market_dates if d > expiry]
    if not later:
        return None
    return dict(effective_date=str(min(later)), next_nearby_contract=months[1],
                next_nearby_expiry=str(expiry), publication_cutoff='end_of_day')


def repair_position_source_context(archive, facts, lifetimes, market_dates):
    """Join a dated cover and its annex; resolve explicit relative clocks.

    A cover must share the exact dated announcement and independently state
    the annex row's direction and effective date. No numeric cap is changed.
    Relative reductions require the literal clause and verified contract lives.
    """
    texts = {}; source_views = {}; changes = []
    def source_text(url):
        if url not in texts:
            document = archive.document(url)
            source_views[url] = [t for t, _ in retained_document_text_views(archive, url, document)]
            texts[url] = (document, '\n'.join(source_views[url]))
        return texts[url]
    for fact in facts:
        if (fact.get('direction')=='absolute' and fact.get('unit')=='contracts'
                and not fact.get('effective_date')):
            doc,text=source_text(fact['source_url'])
            if doc['content_sha256']!=fact['source_content_sha256']:
                raise ValueError('position context source differs from candidate bytes')
            native=position_legacy_word_candidates(text)
            own=[r for r in native if r['product']==fact['product']
                 and r.get('natural_person_limit')==fact.get('natural_person_limit')
                 and r.get('natural_person_monthly_limit')==fact.get('natural_person_monthly_limit')
                 and r.get('effective_phase')=='new_contract_listing'
                 and r.get('effective_date')
                 and r['effective_date']>fact['published_date']]
            listing_days={r['effective_date'] for r in own}
            native_bound=False
            if native:
                # Mixed notices can list futures in a dated table while
                # changing options immediately in another clause. Use the
                # exact futures row, never that notice's general clock.
                try:check_publication(text,fact['published_date'])
                except ValueError:continue
                if len(listing_days)!=1:continue
                clock=dict(issue_date_bound=True,effective_phase='new_contract_listing',
                           effective_date=next(iter(listing_days)))
                native_bound=True
            else:
                clock=candidate_notice_clock(text,fact['published_date'])
            literal_code=re.search(r'(?<![A-Z0-9])'+re.escape(fact['product'])+r'(?![A-Z0-9])',text)
            if (literal_code and clock['issue_date_bound']
                    and clock['effective_phase']=='new_contract_listing'):
                fact.update(issue_date_bound=True,effective_date=clock['effective_date'],
                    effective_phase='new_contract_listing')
                if native_bound:
                    fact['publication_context_evidence']=json.dumps(dict(
                        kind='exact_native_futures_listing_row',product=fact['product'],
                        natural_person_limit=fact['natural_person_limit'],
                        source_content_sha256=doc['content_sha256'],
                        effective_date=clock['effective_date']),sort_keys=True)
                changes.append(dict(product=fact['product'],source_url=fact['source_url'],
                    kind=('exact_native_futures_listing_row' if native_bound
                          else 'new_contract_listing_source_context'),effective_date=clock['effective_date'],
                    source_content_sha256=doc['content_sha256']))
            continue
        if fact.get('direction') not in ('raise', 'lower'):
            continue
        if fact.get('issue_date_bound') and fact.get('effective_date'):
            continue
        url, published = fact['source_url'], fact['published_date']
        doc, text = source_text(url)
        if doc['content_sha256'] != fact['source_content_sha256']:
            raise ValueError('position context source differs from candidate bytes')
        if not fact.get('issue_date_bound') and fact.get('effective_date'):
            matches = []; conflicting_cover = False
            parents = archive.conn.execute('SELECT a.url FROM links l JOIN announcements a '
                'ON a.url=l.parent WHERE l.child=? AND a.published_date=?', (url, published)).fetchall()
            for parent in parents:
                parent_url = parent[0]
                parent_doc = archive.document(parent_url)
                for cover_url in archive.children(parent_url):
                    if cover_url == url:
                        continue
                    state = archive.conn.execute('SELECT state FROM documents WHERE url=?', (cover_url,)).fetchone()
                    if state is None or state[0] != 'complete':
                        continue
                    cover, cover_text = source_text(cover_url)
                    dense = compact(cover_text)
                    if not all(term in dense for term in ('部位限制', '股票期貨', '詳如附件')):
                        continue
                    try:
                        check_publication(cover_text, published)
                    except ValueError:
                        continue
                    words = '(?:提高|調高)' if fact['direction'] == 'raise' else '(?:降低|調降)'
                    dates = {str(roc_date(m)) for m in re.finditer(
                        words + r'(?:者)?[,，:：]?自' + ROC_DATE + r'起生效', dense)}
                    if dates != {fact['effective_date']}:
                        conflicting_cover |= bool(dates)
                        continue
                    matches.append(dict(announcement_url=parent_url,
                        announcement_content_sha256=parent_doc['content_sha256'],
                        cover_url=cover_url, cover_content_sha256=cover['content_sha256'],
                        direction=fact['direction'], effective_date=fact['effective_date']))
            if len(matches) == 1 and not conflicting_cover:
                fact['issue_date_bound'] = True
                fact['publication_context_evidence'] = json.dumps(matches[0], ensure_ascii=False, sort_keys=True)
                changes.append(dict(product=fact['product'], source_url=url, kind='same_notice_cover', **matches[0]))
        if (fact.get('issue_date_bound') and not fact.get('effective_date')
                and fact['direction'] == 'lower'):
            prefix = '調降者,自公告日該期貨或該選擇權已上市之次近月份契約到期後次一營業日'
            clauses = (prefix+'一般交易時段生效',prefix+'起生效')
            sections = [compact(t).rsplit('調整本公司', 1)[-1] for t in source_views[url]]
            matching = [t for t in sections if any(clause in t for clause in clauses)]
            # An incomplete later OCR view cannot hide an exact retained one.
            if not matching or any('除' in t.split('正本:', 1)[0] for t in matching):
                continue
            resolved = next_nearby_position_date(fact['product'], published, lifetimes, market_dates)
            if resolved:
                fact['effective_date'] = resolved['effective_date']
                fact['effective_phase'] = 'product_regular_open'
                fact['relative_position_clock_evidence'] = json.dumps(resolved, sort_keys=True)
                changes.append(dict(product=fact['product'], source_url=url,
                    kind='next_nearby_expiry_after_publication', **resolved))
    return changes


def repair_same_day_position_grade(archive, facts):
    """Compose a latest-grade clause with a same-day or later cap notice.

    This bounded case requires the corporate table to have returned to the
    standard share basis on the exact grade-change date. Temporary enlarged
    caps, ambiguous prior grades and unrelated publication orders stay gaps.
    """
    from fractions import Fraction
    absolute = [r for r in facts if r.get('event_type') in (None, 'absolute_level')
                and r.get('unit') == 'contracts']
    levels, _ = position_candidate_intervals(absolute)
    grouped = {}
    for row in facts:
        if row.get('event_type') == 'corporate_securities_unit_limit' and row.get('effective_date'):
            key = (row['source_content_sha256'], row['effective_date'],
                   tuple(sorted(row.get('combined_products') or [])))
            grouped.setdefault(key, []).append(row)
    changes = []
    for (digest, day, members), rows in grouped.items():
        bases = [p for p in members if re.fullmatch(r'[A-Z]{2}F', p)]
        if len(bases) != 1 or len(members) < 2:
            continue
        base = bases[0]
        standards = [r for r in rows if r['product'] == base]
        if not standards or any(not r.get('issue_date_bound') for r in rows):
            continue
        knowns = {r.get('known_at') for r in rows}
        amounts = {r.get('natural_person_limit') for r in rows}
        units = {r.get('position_unit') for r in standards}
        if len(knowns) != 1 or None in knowns or len(amounts) != 1 or len(units) != 1:
            continue
        known = next(iter(knowns)); amount = next(iter(amounts)); unit = next(iter(units))
        if not amount or not unit or any(r.get('unit') != 'shares' for r in rows):
            continue
        before = [r for r in levels if r['product'] == base and r['effective_date'] <= known[:10]
                  and (not r['valid_until_date_exclusive'] or known[:10] < r['valid_until_date_exclusive'])
                  and r['known_at'] <= known and not r['legal_date_only']]
        after = [r for r in levels if r['product'] == base and r['effective_date'] == day
                 and r['known_at'] < timestamp(date.fromisoformat(day), '00:00:00')
                 and not r['legal_date_only']]
        if len(before) != 1 or len(after) != 1:
            continue
        old, new = before[0], after[0]
        if old['monthly_position_limit'] is not None or new['monthly_position_limit'] is not None:
            continue
        if Fraction(str(amount)) != Fraction(str(old['position_limit'])) * Fraction(str(unit)):
            continue
        doc = archive.document(standards[0]['source_url'])
        if doc['content_sha256'] != digest:
            raise ValueError('corporate grade source identity mismatch')
        views = [compact(t) for t, _ in retained_document_text_views(archive, standards[0]['source_url'], doc)]
        clause = '部位限制數應依本契約最新適用部位限制級數計算'
        clause_scope='corporate_notice';grade_urls=[]
        if not any(clause in text for text in views):
            # A quarterly notice can explicitly replace the adjusted cap's
            # applicable grade even when the earlier corporate table omits
            # that instruction. Verify its own original, never today's law.
            clause='本次部位限制數調整生效後,則依其最新適用之部位限制級數計算'
            if not new.get('limit_follows_applicable_grade'):continue
            for url in new['source_urls']:
                grade_doc=archive.document(url)
                if grade_doc['content_sha256'] not in new['source_content_sha256s']:
                    raise ValueError('quarterly position grade source identity mismatch')
                grade_views=[compact(t) for t,_ in retained_document_text_views(archive,url,grade_doc)]
                if any(re.search(r'本次部位限制數調整生效後,?則依其最新適用之部位限制級(?:數|距)計算',
                        text) for text in grade_views):grade_urls.append(url)
            if not grade_urls:continue
            clause_scope='quarterly_notice'
        corrected = float(Fraction(str(new['position_limit'])) * Fraction(str(unit)))
        combined_known = max(known, new['known_at'])
        sources = sorted({digest, *old['source_content_sha256s'], *new['source_content_sha256s']})
        evidence = dict(product=base, effective_date=day, original_share_limit=amount,
            standard_position_units=unit, prior_contract_limit=old['position_limit'],
            new_contract_limit=new['position_limit'], corrected_share_limit=corrected,
            corporate_known_at=known, grade_known_at=new['known_at'], source_content_sha256s=sources,
            explicit_clause=clause, clause_scope=clause_scope, grade_source_urls=grade_urls,
            combined_products=list(members))
        for row in rows:
            row.update(natural_person_limit=corrected, known_at=combined_known,
                original_natural_person_limit=amount, original_corporate_known_at=known,
                position_grade_source_sha256s=sources,
                position_grade_evidence=json.dumps(evidence, ensure_ascii=False, sort_keys=True),
                extraction='source_composed_position_grade_boundary')
        changes.append(evidence)
    atomic_write_json(archive.bundle/'position_grade_boundary_repairs.json', changes)
    return changes


def repair_active_position_grade_boundaries(archive, facts, corporate=()):
    """Propagate a verified later grade through an active standard-share cap.

    Original tables remain facts. Only an exactly proved printed-period basis
    and the dated original's latest-grade clause permit composed boundaries.
    Enlarged temporary limits, incomplete groups and late clocks stay gaps.
    An unproved intermediate applicability clause does not prevent a later
    source-explicit absolute grade from re-establishing its own dated cap.
    """
    from fractions import Fraction
    # A ticker can be reused for a new physical generation. Printed share caps
    # and unchanged-unit combinations own their separately dated successors;
    # automatic descendants of an old notice cannot overwrite either source.
    active=lambda row: (row.get('extraction')=='source_composed_position_grade_boundary'
        and json.loads(row.get('position_grade_evidence') or '{}').get('clause_scope')
            in ('quarterly_notice_active_period','corporate_notice_active_period'))
    printed=[row for row in facts if row.get('event_type')=='corporate_securities_unit_limit'
             and not active(row)]
    printed_levels,_=position_candidate_intervals(printed)
    printed_states={(row['product'],row['effective_date']):row for row in printed_levels}
    prospective={row['source_url'] for row in printed
        if row.get('effective_phase')=='product_regular_open' and row.get('issue_date_bound')
        and row.get('known_at') and row.get('effective_date')
        and row['known_at'][:10]<row['effective_date']}
    formula_sources={row['source_url'] for row in facts
        if row.get('event_type')=='combined_position_formula'
        and row.get('combined_position_ratio')=='1/1'
        and row.get('issue_date_bound') and row.get('known_at') and row.get('effective_date')
        and row['known_at'][:10]<row['effective_date']}
    literal_sources=prospective|formula_sources
    literal_rows=(retained_corporate_position_text_candidates(archive,list(corporate),
        source_urls=literal_sources) if corporate and literal_sources else [])
    literal_key=lambda row: (row.get('source_content_sha256'),row.get('product'),
        row.get('effective_date'),row.get('effective_phase'),row.get('unit'),
        row.get('natural_person_limit'),row.get('position_unit'),
        tuple(sorted(row.get('combined_products') or [])),
        row.get('valid_until_date_inclusive'),bool(row.get('requires_delisting_clock')))
    literal_keys={literal_key(row) for row in literal_rows}
    successors={}
    printed_groups={}
    for row in printed:
        if literal_key(row) in literal_keys:
            key=(row['source_content_sha256'],row['effective_date'],
                 tuple(sorted(row.get('combined_products') or [])))
            printed_groups.setdefault(key,[]).append(row)
    for (digest,day,members),rows in printed_groups.items():
        bases=[p for p in members if re.fullmatch(r'[A-Z]{2}F',p)]
        if len(bases)!=1 or len(members)<2 or {r['product'] for r in rows}!=set(members):continue
        states=[printed_states.get((p,day)) for p in members]
        if any(s is None or digest not in s['source_content_sha256s']
                or s['unit']!='shares' or s['monthly_position_limit'] is not None
                or tuple(s['combined_products'])!=members for s in states):continue
        if len({s['position_limit'] for s in states})!=1:continue
        if any(not r.get('issue_date_bound') or r.get('effective_phase')!='product_regular_open'
                or not r.get('known_at') or r['known_at'][:10]>=day for r in rows):continue
        for product in members:
            own=[r for r in rows if r['product']==product]
            if len({literal_key(r) for r in own})!=1:continue
            row=own[0]
            evidence={k:row[k] for k in ('product','effective_date','known_at','source_url',
                'source_content_sha256','position_unit','natural_person_limit','combined_products')}
            evidence['successor_kind']='source_printed_securities_cap'
            if evidence not in successors.setdefault(product,[]):successors[product].append(evidence)
    reviewed_formula_rows=[]
    reviewed_formula_sources={row['source_url'] for row in facts
        if row.get('event_type')=='combined_position_formula'
        and row.get('extraction_method')=='source_bound_visual_corporate_position_group'
        and row.get('position_formula_parser')=='explicit_unchanged_unit_corporate_combination'
        and row.get('combined_position_ratio')=='1/1'}
    reviewed_formula_hashes={row.get('visual_review_sha256') for row in facts
        if row.get('source_url') in reviewed_formula_sources
        and row.get('event_type')=='combined_position_formula'
        and row.get('extraction_method')=='source_bound_visual_corporate_position_group'
        and row.get('position_formula_parser')=='explicit_unchanged_unit_corporate_combination'
        and row.get('combined_position_ratio')=='1/1'}
    for source in list(archive.sources.values()):
        if (source['kind']!='visual_position_cell_review'
                or source['sha256'] not in reviewed_formula_hashes):
            continue
        path=archive.bundle/source['path']
        if sha256_file(path)!=source['sha256']:
            raise ValueError('active reviewed member source identity mismatch')
        reviewed_formula_rows.extend(position_source_review_candidates(archive,path,list(corporate),facts,
            source_urls=reviewed_formula_sources))
    def reviewed_formula(row):
        # Reuse the source/image/publication/unit-checked reader. A provenance
        # label alone cannot end an old generation's derived cap.
        keys=('product','effective_date','effective_phase','known_at','source_url','source_content_sha256',
              'combined_position_base_product','combined_position_ratio','visual_review_sha256',
              'position_formula_parser','position_group_unit_evidence')
        return any(all(row.get(k)==proof.get(k) for k in keys) for proof in reviewed_formula_rows)
    def retained_formula(row):
        # Three-member notices can mix unchanged and changed units. Reuse the
        # same byte/receipt/unit-bound parser that emitted the unchanged member;
        # pair wording and provenance labels cannot prove that whole group.
        keys=('product','effective_date','effective_phase','known_at','source_url','source_content_sha256',
              'event_type','unit','natural_person_limit','combined_position_base_product',
              'combined_position_ratio','extraction_method','position_group_unit_evidence')
        return any(proof.get('event_type')=='combined_position_formula'
            and all(row.get(k)==proof.get(k) for k in keys) for proof in literal_rows)
    for row in facts:
        if (row.get('event_type')!='combined_position_formula'
                or not (row.get('extraction_method')=='explicit_unchanged_unit_corporate_combination'
                        or reviewed_formula(row) or retained_formula(row))
                or row.get('combined_position_ratio')!='1/1' or not row.get('issue_date_bound')
                or row.get('effective_phase')!='product_regular_open'):
            continue
        product=row.get('product') or '';base=row.get('combined_position_base_product') or ''
        day=row.get('effective_date');known=row.get('known_at')
        if (not re.fullmatch(r'[A-Z]{2}[1-9]',product) or base!=product[:2]+'F'
                or not day or not known or known[:10]>=day):continue
        doc=archive.document(row['source_url'])
        if doc['content_sha256']!=row['source_content_sha256']:
            raise ValueError('active member successor source identity mismatch')
        views=[compact(t) for t,_ in retained_document_text_views(archive,row['source_url'],doc)]
        if not (reviewed_formula(row) or retained_formula(row)) and not any(product+'與'+base+'部位合併計算' in t
                or base+'與'+product+'部位合併計算' in t for t in views):continue
        evidence={k:row[k] for k in ('product','effective_date','known_at','source_url',
                                   'source_content_sha256','combined_position_base_product')}
        if evidence not in successors.setdefault(product,[]):successors[product].append(evidence)
    def successor(product,start,digest):
        applicable=[r for r in successors.get(product,[]) if r['effective_date']>start
                    and (r['source_content_sha256']!=digest
                         or r.get('successor_kind')=='source_printed_securities_cap')]
        return min(applicable,key=lambda r:r['effective_date']) if applicable else None
    def printed_origin(row):
        current=row;seen=set()
        while active(current):
            ev=json.loads(current['position_grade_evidence']);start=ev.get('origin_effective_date')
            if not start or start>=current['effective_date'] or start in seen:return None
            seen.add(start)
            parents=[r for r in facts if r['product']==current['product']
                and r['source_content_sha256']==current['source_content_sha256']
                and r.get('effective_date')==start
                and sorted(r.get('combined_products') or [])==sorted(ev['combined_products'])]
            if not parents or len({json.dumps(r,sort_keys=True) for r in parents})!=1:return None
            current=parents[0]
        return current.get('effective_date')
    retired=[]
    for row in facts:
        ev=json.loads(row.get('position_grade_evidence') or '{}')
        if not active(row):continue
        origin=printed_origin(row)
        if not origin:continue
        group_boundaries=[s for p in ev['combined_products'] for s in successors.get(p,[])
            if s.get('successor_kind')=='source_printed_securities_cap'
            and s['effective_date']>origin]
        printed_successor=min(group_boundaries,key=lambda s:s['effective_date']) if group_boundaries else None
        recorded_end=ev.get('valid_until_date_exclusive')
        if printed_successor and (row['effective_date']>=printed_successor['effective_date']
                or not recorded_end or recorded_end>printed_successor['effective_date']):
            retired.append(dict(retired_row=row,successor=printed_successor,
                printed_origin_effective_date=origin,
                reason='old_origin_derived_grade_crosses_printed_cap'))
            continue
        next_member=successor(row['product'],origin,row['source_content_sha256'])
        if next_member and row['effective_date']>=next_member['effective_date']:
            retired.append(dict(retired_row=row,successor=next_member,
                reason='old_member_generation_derived_grade_superseded'))
    if retired:
        retired_rows=[r['retired_row'] for r in retired]
        facts[:]=[r for r in facts if r not in retired_rows]
    atomic_write_json(archive.bundle/'superseded_composed_position_grades.json',retired)
    absolute=[r for r in facts if (r.get('event_type') or 'absolute_level')=='absolute_level'
        and r.get('unit')=='contracts']
    levels,_=position_candidate_intervals(absolute)
    corporate=[r for r in facts if r.get('event_type')=='corporate_securities_unit_limit']
    intervals,_=position_candidate_intervals(corporate)
    indexed={(r['product'],r['effective_date']):r for r in intervals}
    groups={}
    for row in corporate:
        if row.get('effective_date'):
            key=(row['source_content_sha256'],row['effective_date'],tuple(sorted(row.get('combined_products') or [])))
            groups.setdefault(key,[]).append(row)
    clause_cache={};changes=[];added=[]
    def quarter_clause(level):
        key=(tuple(level['source_urls']),tuple(level['source_content_sha256s']))
        if key not in clause_cache:
            urls=[]
            if level.get('limit_follows_applicable_grade'):
                for url in level['source_urls']:
                    doc=archive.document(url)
                    if doc['content_sha256'] not in level['source_content_sha256s']:
                        raise ValueError('active quarterly position grade source identity mismatch')
                    if any(re.search(r'本次部位限制數調整生效後,?則依其最新適用之部位限制級(?:數|距)計算',compact(t))
                            for t,_ in retained_document_text_views(archive,url,doc)):
                        urls.append(url)
            clause_cache[key]=urls
        return clause_cache[key]
    def corporate_clause(rows, digest):
        urls=sorted({r['source_url'] for r in rows})
        key=('corporate',digest,tuple(urls))
        if key not in clause_cache:
            matched=[]
            for url in urls:
                doc=archive.document(url)
                if doc['content_sha256']!=digest:
                    raise ValueError('active corporate position grade source identity mismatch')
                if any(re.search(r'部位限制數應依本契約最新適用部位限制級(?:數|距)計算',compact(t))
                        for t,_ in retained_document_text_views(archive,url,doc)):
                    matched.append(url)
            clause_cache[key]=matched
        return clause_cache[key]
    for (digest,start,members),rows in groups.items():
        bases=[p for p in members if re.fullmatch(r'[A-Z]{2}F',p)]
        if len(bases)!=1 or len(members)<2:continue
        base=bases[0];states={p:indexed.get((p,start)) for p in members}
        if any(s is None or digest not in s['source_content_sha256s']
                or s['unit']!='shares' or s['monthly_position_limit'] is not None for s in states.values()):continue
        if any(tuple(s['combined_products'])!=members for s in states.values()):continue
        if len({s['position_limit'] for s in states.values()})!=1:continue
        standards=[r for r in rows if r['product']==base]
        if not standards or any(not r.get('issue_date_bound') for r in rows):continue
        units={r.get('position_unit') for r in standards}
        proofs=[json.loads(r.get('position_grade_evidence') or '{}') for r in rows]
        original_knowns={proof.get('basis_original_known_at') or r.get('original_corporate_known_at') or r['known_at']
            for r,proof in zip(rows,proofs)}
        original_amounts={proof.get('printed_origin_share_limit') or r.get('original_natural_person_limit')
            or r.get('natural_person_limit') for r,proof in zip(rows,proofs)}
        original_periods={proof.get('basis_date') if proof.get('basis_scope')=='announced_grade_at_original_period_start'
            else start for proof in proofs}
        if len(units)!=1 or len(original_knowns)!=1 or len(original_amounts)!=1 or len(original_periods)!=1:continue
        unit=next(iter(units));known=next(iter(original_knowns));original_amount=next(iter(original_amounts))
        if not unit or not known or not original_amount:continue
        def known_basis(basis_day):
            return [r for r in levels if r['product']==base and r['effective_date']<=basis_day
                and (not r['valid_until_date_exclusive'] or basis_day<r['valid_until_date_exclusive'])
                and r['known_at']<=known
                and (not r['admission_not_before'] or r['admission_not_before'][:10]<basis_day)
                and r['monthly_position_limit'] is None]
        before=known_basis(known[:10])
        declared_units={Fraction(str(r['position_unit'])) for r in rows if r.get('position_unit')}
        # The first temporary period may use the adjustment's printed units;
        # its following period can use standard units. Resolve that arithmetic
        # independently, without changing any member's own position weight.
        matching_units=lambda r:{quantity for quantity in declared_units
            if Fraction(str(original_amount))==Fraction(str(r['position_limit']))*quantity}
        basis_day=known[:10];basis_scope='grade_at_original_publication'
        if len(before)!=1 or len(matching_units(before[0]))!=1:
            # A dated corporate table may announce its second period using an
            # already published future grade. Its explicit latest-grade clause
            # plus exact period arithmetic establish that basis; the source
            # must already be known at publication, never merely by execution.
            basis_day=next(iter(original_periods));basis_scope='announced_grade_at_original_period_start'
            if not corporate_clause(rows,digest):continue
            before=known_basis(basis_day)
            if len(before)!=1 or len(matching_units(before[0]))!=1:continue
        prior=before[0]
        grade_basis_unit=next(iter(matching_units(prior)))
        ends=[s['valid_until_date_exclusive'] for s in states.values() if s['valid_until_date_exclusive']]
        end=min(ends) if ends else None
        member_successors={p:next_member for p in members
            if (next_member:=successor(p,start,digest)) is not None}
        following=sorted([r for r in levels if r['product']==base and start<r['effective_date']
            and (not end or r['effective_date']<end)],key=lambda r:r['effective_date'])
        current=states[base]['position_limit']
        current_known=max(s['known_at'] for s in states.values())
        current_sources={sha for state in states.values() for sha in state['source_content_sha256s']}
        unproved=[]
        for grade in following:
            day=grade['effective_date'];sources=quarter_clause(grade)
            clause_scope='quarterly_notice_active_period'
            explicit_clause='latest_grade_in_dated_quarterly_original'
            clause_sources=sources
            if not sources:
                clause_sources=corporate_clause(rows,digest)
                if clause_sources:
                    # The corporate instruction is independently authoritative;
                    # a quarterly flag is not needed. Numeric grade originals
                    # still require exact source identity before composition.
                    for url in grade['source_urls']:
                        if archive.document(url)['content_sha256'] not in grade['source_content_sha256s']:
                            raise ValueError('active quarterly position grade source identity mismatch')
                    sources=grade['source_urls']
                    clause_scope='corporate_notice_active_period'
                    explicit_clause='latest_grade_in_dated_corporate_original'
            joined_known=max(grade['known_at'],current_known)
            if (grade['monthly_position_limit'] is not None
                    or datetime.fromisoformat(joined_known).astimezone(TAIPEI).date()>date.fromisoformat(day)):break
            if not sources:
                # Retain the numerical input for the audit of a later absolute
                # re-entry; do not emit a cap or assign the missing interval.
                for url in grade['source_urls']:
                    if archive.document(url)['content_sha256'] not in grade['source_content_sha256s']:
                        raise ValueError('unproved quarterly position grade source identity mismatch')
                unproved.append(dict(effective_date=day,known_at=grade['known_at'],
                    contract_limit=grade['position_limit'],source_urls=grade['source_urls'],
                    source_content_sha256s=grade['source_content_sha256s'],
                    reason='latest_grade_applicability_clause_unproved'))
                current=float(Fraction(str(grade['position_limit']))*grade_basis_unit)
                current_known=joined_known
                current_sources.update(grade['source_content_sha256s'])
                continue
            corrected=float(Fraction(str(grade['position_limit']))*grade_basis_unit)
            source_ids=sorted({digest,*prior['source_content_sha256s'],*grade['source_content_sha256s'],*current_sources})
            if corrected==current and not unproved:
                current_known=joined_known;current_sources=set(source_ids)
                continue
            evidence=dict(product=base,effective_date=day,origin_effective_date=start,
                original_share_limit=current,printed_origin_share_limit=original_amount,
                basis_original_known_at=known,
                basis_date=basis_day,basis_scope=basis_scope,
                standard_position_units=unit,grade_basis_position_units=float(grade_basis_unit),
                prior_contract_limit=current/float(grade_basis_unit),
                basis_contract_limit=prior['position_limit'],
                new_contract_limit=grade['position_limit'],corrected_share_limit=corrected,
                corporate_known_at=current_known,grade_known_at=grade['known_at'],
                grade_legal_date_only=grade['legal_date_only'],
                grade_admission_not_before=grade['admission_not_before'],
                source_content_sha256s=source_ids,explicit_clause=explicit_clause,
                clause_scope=clause_scope,grade_source_urls=sources,clause_source_urls=clause_sources,
                combined_products=list(members),valid_until_date_exclusive=end)
            if unproved:
                evidence.update(reanchor_after_unproved_grade_boundaries=list(unproved),
                    prior_numeric_grade_is_not_a_verified_corporate_cap=True,
                    missing_period_not_admitted=True)
            active_products=[p for p in members if p not in member_successors
                             or day<member_successors[p]['effective_date']]
            if not active_products:break
            if len(active_products)!=len(members):
                evidence.update(active_products=active_products,
                    member_generation_boundaries=member_successors)
            for product,state in states.items():
                if product not in active_products:continue
                originals=[r for r in rows if r['product']==product]
                if not originals or len({r['position_unit'] for r in originals})!=1:
                    raise ValueError('active grade member lacks unambiguous printed units')
                row=dict(originals[0],effective_date=day,
                    # Preserve the legal date without inventing its intraday
                    # phase. The canonical interval binder then withholds the
                    # uncertain date from both standard and adjusted contracts.
                    effective_phase=('date_only_requires_phase_review'
                        if grade['legal_date_only'] else 'product_regular_open'),
                    natural_person_limit=corrected,known_at=joined_known,
                    original_natural_person_limit=current,
                    original_corporate_known_at=evidence['corporate_known_at'],
                    position_grade_origin_effective_date=start,
                    position_grade_source_sha256s=source_ids,
                    position_grade_evidence=json.dumps(evidence,ensure_ascii=False,sort_keys=True),
                    extraction='source_composed_position_grade_boundary')
                if end:row['valid_until_date_inclusive']=str(date.fromisoformat(end)-timedelta(days=1))
                for key in ('supersedes_source_sha256s','supersession_review_sha256',
                            'position_generation_constraint_evidence'):
                    row.pop(key,None)
                added.append(row)
            current=corrected;current_known=joined_known;current_sources=set(source_ids)
            unproved=[]
            changes.append(evidence)
    facts.extend(added)
    atomic_write_json(archive.bundle/'position_active_grade_boundary_repairs.json',changes)
    return changes


def _named_position_expiry_restoration(caps, identities, lifetimes, observations,
                                      market_dates, proof_sha256s, group_rules):
    """Resolve a named expiry from declared months, never the last quote alone."""
    if any(value is None for value in (lifetimes,observations,market_dates,proof_sha256s,group_rules)):
        return None
    required={'physical_lifetimes','original_daily_source','official_final_source',
              'calendar_source','dated_group_law'}
    if (set(proof_sha256s)!=required or any(not re.fullmatch('[a-f0-9]{64}',v or '')
                                          for v in proof_sha256s.values())):
        raise ValueError('named position restoration requires complete source fingerprints')
    metadata={(r['effective_date'],r.get('termination_product'),
               tuple(sorted(r.get('combined_products') or [])),r.get('conditional_restoration_clause'),
               r.get('natural_person_limit')) for r in caps}
    if len(metadata)!=1:return None
    event,terminal,members,clause,amount=next(iter(metadata))
    if (not terminal or terminal not in members or len(members)<3
            or {r['product'] for r in caps}!=set(members)
            or any(r.get('requires_delisting_clock') is not True for r in caps)):
        return None
    applicable=[r for r in group_rules if r.get('asset_class')=='stock_future'
        and r.get('standard_units')==2000 and r.get('effective_date','')<=event
        and r.get('known_at','')<=min(r['known_at'] for r in caps)
        and r.get('rule')=='same_security_same_direction; unchanged_units_or_explicit_securities_cap_only'
        and re.fullmatch('[a-f0-9]{64}',r.get('source_content_sha256',''))]
    if not applicable:return None
    latest=max(r['effective_date'] for r in applicable)
    applicable=[r for r in applicable if r['effective_date']==latest]
    if len(applicable)!=1:return None
    own=[r for r in identities if r['product']==terminal]
    if (not own or any(not r.get('contract_months') or not r.get('issue_date_bound')
            or r['effective_date']!=event for r in own)):
        return None
    months={month for r in own for month in r['contract_months']}
    conversion={p:{r.get('position_unit') for r in caps if r['product']==p} for p in members}
    for product in members:
        units={r.get('contract_multiplier') for r in identities if r['product']==product}
        if units and units!=conversion[product]:return None
    first=date.fromisoformat(event)
    calendar=sorted(set(market_dates))
    if first not in calendar:return None
    life=lifetimes.filter((pl.col('product')==terminal)
        & pl.col('contract').is_in(sorted(months)) & (pl.col('first_observed_date')>=first))
    if (life.height!=len(months) or set(life['contract'])!=months
            or not life['lifetime_status'].eq('official_final').all()
            or not life['final_fixing_origin'].eq('official_product_final').all()
            or not life['settlement_method'].eq('cash_settlement').all()
            or life['official_expiry'].null_count() or life['first_zero_oi_date'].null_count()!=life.height
            or not life['final_settlement_price'].gt(0).fill_null(False).all()
            or not life['calendar_end'].eq(life['official_expiry']).all()):
        return None
    raw=observations.filter((pl.col('product')==terminal) & (pl.col('date')>=first)
        & (pl.col('date')<=life['official_expiry'].max()))
    if raw.select('date','contract').is_duplicated().any() or set(raw['contract'])!=months:
        return None
    for lifetime in life.iter_rows(named=True):
        month=lifetime['contract'];end=lifetime['official_expiry']
        if month!=end.strftime('%Y%m') or end not in calendar:return None
        expected={day for day in calendar if first<=day<=end}
        observed=raw.filter(pl.col('contract')==month)
        if (set(observed['date'])!=expected or observed['open_interest'].null_count()
                or not observed['open_interest'].ge(0).all()
                or not observed.filter(pl.col('date')<end)['open_interest'].gt(0).all()
                or lifetime['first_observed_date']!=first or lifetime['last_observed_date']!=end):
            return None
    end=life['official_expiry'].max()
    following=[day for day in calendar if day>end]
    if not following:return None
    restored=tuple(p for p in members if p!=terminal)
    return dict(effective_date=str(following[0]),combined_products=restored,
        known_at=timestamp(end,'23:59:59'),conditional_terminal_product=terminal,
        conditional_terminal_months=sorted(months),conditional_terminal_date=str(end),
        original_combined_products=list(members),literal_securities_cap=amount,
        source_clause=clause,termination_evidence_sha256s=proof_sha256s,
        dated_group_rule=applicable[0])


def repair_original_position_restorations(archive, facts, corporate, *,
        lifetimes=None, observations=None, market_dates=None, termination_proof_sha256s=None,
        termination_group_rules=None):
    """Compose a literal restoration with the cap valid before adjustment.

    An original-cap reference is not a new numeric cell or permission to use
    a later grade. Require one prior contract-count level, a dated unchanged
    adjusted unit, and no intervening rule from another notice. Keep both
    originals in the resulting interval's provenance.
    """
    tag='source_composed_original_position_restoration'
    facts[:]=[f for f in facts if f.get('extraction')!=tag]
    # Derived termination clocks are revalidated on every composition. A lost
    # dependency cannot leave the prior end or its restoration admitted.
    for row in facts:
        if row.get('end_rule')=='named_adjusted_contract_delisting_then_contract_count':
            row.update(requires_delisting_clock=True,valid_until_date_inclusive=None)
            row.pop('position_restoration_evidence',None)
    levels,_=position_candidate_intervals([f for f in facts
        if (f.get('event_type') or 'absolute_level')=='absolute_level'])
    unit_levels,_=corporate_terms_intervals(corporate,unit_only=True)
    notices={}
    for f in corporate:
        if (f.get('issue_date_bound') and f.get('source_url') and f.get('effective_date')
                and f.get('from_product')!=f.get('product')):
            key=(f['source_url'],f['source_content_sha256'],f['published_date'],f['effective_date'])
            notices.setdefault(key,[]).append(f)
    changes=[];issues=[]
    code=r'[A-Z]{2}[F1-9]'
    pattern=(r'自'+ROC_DATE+r'起[,，]?('+code+r'(?:(?:與|、|及)'+code+r')+)'
             r'部位合併計算[,，]?並恢復為原公告之交易人部位限制數控管')
    for (url,digest,published,event), identities in notices.items():
        doc=archive.document(url)
        if doc['content_sha256']!=digest:
            raise ValueError('position restoration source identity mismatch')
        matches={}
        for text,_ in retained_document_text_views(archive,url,doc):
            for match in re.finditer(pattern,compact(text).replace('|','')):
                day=str(roc_date(match));members=tuple(sorted(set(re.findall(code,match[4]))))
                matches[(day,members)]=match[0]
        conditional=None
        caps=[f for f in facts if f.get('source_url')==url and f.get('source_content_sha256')==digest
              and f.get('end_rule')=='named_adjusted_contract_delisting_then_contract_count']
        if caps:
            conditional=_named_position_expiry_restoration(caps,identities,lifetimes,observations,
                market_dates,termination_proof_sha256s,termination_group_rules)
            if matches or conditional is None:
                issues.append(dict(source_content_sha256=digest,
                    reason='named_position_termination_clock_unresolved'))
                continue
            literal=conditional['source_clause']
            retained=''.join(compact(text).replace('|','') for text,_ in
                            retained_document_text_views(archive,url,doc))
            if literal not in retained:
                raise ValueError('conditional position restoration lacks its retained literal clause')
            matches[(conditional['effective_date'],conditional['combined_products'])]=literal
        if not matches:continue
        if len(matches)!=1:
            issues.append(dict(source_content_sha256=digest,reason='ambiguous_original_position_restoration'))
            continue
        (day,members),clause=next(iter(matches.items()))
        bases=[p for p in members if re.fullmatch(r'[A-Z]{2}F',p)]
        if (len(bases)!=1 or len(members)<2 or not published<event<day
                or any(p[:2]!=bases[0][:2] for p in members)):
            continue
        base=bases[0];original_known=timestamp(date.fromisoformat(published),'23:59:59')
        known=original_known
        if conditional:known=max(known,conditional['known_at'],key=datetime.fromisoformat)
        active=[r for r in levels if r['product']==base and r['effective_date']<event
            and (not r['valid_until_date_exclusive'] or event<=r['valid_until_date_exclusive'])
            and r['known_at']<=original_known and not r['legal_date_only']]
        adjusted=[p for p in members if p!=base]
        units={p:{r['contract_multiplier'] for r in unit_levels
            if r['product']==p and r['effective_date']<=day
            and (not r['valid_until_exclusive'] or day<r['valid_until_exclusive'])
            and digest in r['source_content_sha256s']} for p in adjusted}
        standard={f['contract_multiplier'] for f in identities
            if f['product'] in adjusted and f.get('from_product')==base}
        # Plain contract counts can combine only explicitly unchanged units.
        if len(active)!=1 or standard!={2000.} or any(units[p]!={2000.} for p in adjusted):
            issues.append(dict(product=base,effective_date=day,
                reason='original_position_cap_or_unchanged_units_unresolved',source_content_sha256=digest))
            continue
        intervening=[f for f in facts if f['product']==base and
            event<=(f.get('effective_date') or f.get('published_date') or '')<=day
            and f.get('source_content_sha256')!=digest]
        if intervening:
            issues.append(dict(product=base,effective_date=day,
                reason='intervening_position_rule_requires_composed_restoration',source_content_sha256=digest))
            continue
        original=active[0]
        if conditional and (original['unit']!='contracts' or original['monthly_position_limit'] is not None
                or original['position_limit']*2000.!=conditional['literal_securities_cap']):
            issues.append(dict(product=base,effective_date=day,
                reason='conditional_restoration_does_not_match_dated_standard_capacity',
                source_content_sha256=digest))
            continue
        for source_url in original['source_urls']:
            old=archive.document(source_url)
            if old['content_sha256'] not in original['source_content_sha256s']:
                raise ValueError('original position restoration prior source mismatch')
        sources=sorted({digest,*original['source_content_sha256s']})
        evidence=dict(product=base,effective_date=day,corporate_effective_date=event,
            original_effective_date=original['effective_date'],original_known_at=original['known_at'],
            restored_contract_limit=original['position_limit'],combined_products=list(members),
            original_source_urls=original['source_urls'],source_content_sha256s=sources,
            explicit_clause=clause)
        if conditional:
            evidence.update(conditional)
            evidence['combined_products']=list(members)
            for row in caps:
                row.update(valid_until_date_inclusive=conditional['conditional_terminal_date'],
                    requires_delisting_clock=False,
                    position_restoration_evidence=json.dumps(evidence,ensure_ascii=False,sort_keys=True))
        common=dict(source_url=url,announcement_url=url,published_date=published,
            source_content_sha256=digest,known_at=known,issue_date_bound=True,
            effective_date=day,effective_phase='product_regular_open',unit='contracts',
            candidate_only=True,point_in_time_verified=False,extraction=tag,
            position_restoration_source_sha256s=sources,
            position_restoration_evidence=json.dumps(evidence,ensure_ascii=False,sort_keys=True))
        facts.append(dict(common,product=base,event_type='absolute_level',
            natural_person_limit=original['position_limit'],
            natural_person_monthly_limit=original['monthly_position_limit']))
        for product in adjusted:
            facts.append(dict(common,product=product,event_type='combined_position_formula',
                natural_person_limit=None,combined_position_base_product=base,
                combined_position_ratio='1/1',
                extraction_method='explicit_unchanged_unit_corporate_combination'))
        changes.append(evidence)
    atomic_write_json(archive.bundle/'original_position_restoration_repairs.json',
        dict(repairs=changes,issues=issues))
    return changes


def retained_corporate_text_candidates(archive, existing):
    """Use retained source text for old scanned/Word notices, without new OCR."""
    rows=[];issues=[]
    retained={}
    for source in archive.sources.values():
        retained.setdefault(source['url'],[]).append(source)
    seen=set()
    query='''SELECT DISTINCT d.url,a.published_date,a.url announcement_url
        FROM announcements a JOIN (SELECT url parent,url child FROM announcements
            UNION SELECT parent,child FROM links) l ON l.parent=a.url
        JOIN documents d ON d.url=l.child
        WHERE a.category='contract_adjustments' AND d.state='complete'
        ORDER BY a.published_date,d.url'''
    for notice in archive.conn.execute(query):
        doc=archive.document(notice['url'])
        for text,extraction in retained_document_text_views(archive,notice['url'],doc,
                sources=retained.get(notice['url'],[])):
            clock=candidate_notice_clock(text,notice['published_date'])
            extracted=corporate_text_candidates(text)
            if doc.get('format')=='legacy_word' and extraction=='canonical_native':
                extracted.extend(corporate_legacy_word_candidates(text))
            for fact in extracted:
                pair=(doc['content_sha256'],fact['from_product'],fact['to_product'])
                identity=(*pair,tuple(fact['contract_months']),fact['contract_months_text'],
                          fact['contract_multiplier'],fact['effective_date'],
                          fact['equity_credit_long_per_contract'],fact['equity_debit_short_per_contract'],
                          fact['deliverable_text'],clock['issue_date_bound'])
                if identity in seen:continue
                seen.add(identity)
                rows.append(dict(fact,source_url=notice['url'],announcement_url=notice['announcement_url'],
                    published_date=notice['published_date'],source_content_sha256=doc['content_sha256'],
                    known_at=timestamp(date.fromisoformat(notice['published_date']),'23:59:59'),
                    point_in_time_verified=False,issue_date_bound=clock['issue_date_bound'],
                    extraction=extraction))
    return rows,issues


def retained_position_text_candidates(archive):
    """Revisit position prose and native listing tables, with their own clocks."""
    rows=[];seen=set()
    query="""SELECT DISTINCT d.url,a.published_date,a.url announcement_url
        FROM announcements a JOIN (SELECT url parent,url child FROM announcements
            UNION SELECT parent,child FROM links) l ON l.parent=a.url
        JOIN documents d ON d.url=l.child
        WHERE d.state='complete' AND (a.title LIKE '%部位限制%' OR a.title LIKE '%上市%')
        ORDER BY a.published_date,d.url"""
    for notice in archive.conn.execute(query):
        doc=archive.document(notice['url'])
        for text,extraction in retained_document_text_views(archive,notice['url'],doc):
            clock=candidate_notice_clock(text,notice['published_date'])
            facts=position_prose_candidates(text)
            if doc.get('format')=='legacy_word' and extraction=='canonical_native':
                facts.extend(position_legacy_word_candidates(text))
            for fact in facts:
                if fact.get('extraction_method')=='named_listing_position_clause':
                    if clock['effective_phase']!='new_contract_listing' or not clock['effective_date']:
                        continue
                    fact['effective_date']=clock['effective_date']
                identity=(doc['content_sha256'],notice['published_date'],json.dumps(fact,sort_keys=True))
                if identity in seen:continue
                seen.add(identity)
                rows.append(dict(fact,source_url=notice['url'],announcement_url=notice['announcement_url'],
                    published_date=notice['published_date'],source_content_sha256=doc['content_sha256'],
                    known_at=timestamp(date.fromisoformat(notice['published_date']),'23:59:59'),
                    point_in_time_verified=False,issue_date_bound=clock['issue_date_bound'],
                    extraction='retained_position_text:'+extraction))
    return rows


def retained_corporate_position_text_candidates(archive, corporate, *, source_urls=None):
    grouped={};rows=[];seen=set();source_views={}
    retained_sources=list(archive.sources.values())
    for source in retained_sources:
        source_views.setdefault(source['url'],[]).append(source)
    # A cap depends on agreed dated physical units, independently of a cash
    # credit or subscription valuation. The existing unit resolver retains
    # identity, quantity, publication and outgoing-transfer barriers; its
    # output cannot stand in for a complete financial deliverable.
    resolved,_=corporate_terms_intervals(corporate,unit_only=True)
    identities={(sha,r['from_product'],r['product'],r['effective_date'],r['contract_multiplier'])
        for r in resolved for sha in r['source_content_sha256s']}
    for fact in corporate:
        identity=tuple(fact.get(k) for k in ('source_content_sha256','from_product','product',
                                           'effective_date','contract_multiplier'))
        if identity in identities and fact.get('issue_date_bound') and fact.get('source_url'):
            grouped.setdefault(fact['source_url'],[]).append(fact)
    # Completed, byte-bound visual grids are retained source views too.
    # Reusing only OCR/native views would lose an inspected multi-origin unit
    # grid and fail to retire the older origin's scheduled derived caps.
    # Reparse through the same reader; a provenance label alone is not proof.
    reviewed_grids={}
    wanted=set(grouped) if source_urls is None else set(grouped)&set(source_urls)
    for source in retained_sources:
        if source['kind']!='visual_corporate_cell_review':continue
        path=archive.bundle/source['path']
        if sha256_file(path)!=source['sha256']:
            raise ValueError('retained corporate visual review SHA mismatch')
        payload=json.loads(path.read_text());reviews=payload.get('reviews',[payload])
        if not any(r.get('source_url') in wanted for r in reviews):continue
        financial,positions,_=corporate_source_review_candidates(archive,path,source_urls=wanted)
        proven={f['source_url'] for f in financial if (
            f['source_content_sha256'],f['from_product'],f['product'],
            f['effective_date'],f['contract_multiplier']) in identities}
        for fact in positions:
            if fact['source_url'] in wanted&proven:
                reviewed_grids.setdefault(fact['source_url'],[]).append(
                    (fact,'visual_corporate_cell_review:'+source['sha256']))
    for url, facts in grouped.items():
        if source_urls is not None and url not in source_urls:
            continue
        doc=archive.document(url)
        if any(f['source_content_sha256']!=doc['content_sha256'] for f in facts):
            raise ValueError('corporate position source identity drift')
        provenance={key:facts[0].get(key) for key in ('source_url','announcement_url',
            'published_date','source_content_sha256','known_at','issue_date_bound')}
        if any(any(f.get(k)!=provenance[k] for k in ('published_date','known_at')) for f in facts):
            continue
        views=source_views.get(url,[])
        grid_facts=list(reviewed_grids.get(url,[]))
        for table in views:
            if table['kind'] not in ('native_table_tables.json','ocr_table_tables.json'):
                continue
            path=archive.bundle/table['path']
            if sha256_file(path)!=table['sha256']:
                raise ValueError('retained corporate position grid SHA mismatch')
            receipt_kind=table['kind'].replace('tables.json','receipt.json')
            bound=False
            for receipt in views:
                if receipt['kind']!=receipt_kind:continue
                proof_path=archive.bundle/receipt['path']
                if sha256_file(proof_path)!=receipt['sha256']:
                    raise ValueError('retained corporate position grid receipt SHA mismatch')
                proof=json.loads(proof_path.read_text())
                if (proof.get('status')=='complete' and proof.get('content_sha256')==doc['content_sha256']
                        and any(f['path']=='tables.json' and f['sha256']==table['sha256']
                                for f in proof.get('files',[]))):
                    bound=True
            if not bound:raise ValueError('retained corporate position grid lacks matching source receipt')
            pages=json.loads(path.read_text())['pages']
            reviewed_owned={(fact['product'],fact['effective_date']) for fact,_ in grid_facts}
            grid_facts.extend((fact,table['kind']+':'+table['sha256'])
                for fact in corporate_position_table_candidates(pages,corporate=facts)
                if (fact['product'],fact['effective_date']) not in reviewed_owned)
        owned={(fact['product'],fact['effective_date']) for fact,_ in grid_facts}
        views_to_add=[(fact,'retained_corporate_position_table:'+extraction)
                      for fact,extraction in grid_facts]
        for text, extraction in retained_document_text_views(archive,url,doc,sources=views):
            if doc.get('format')=='legacy_word' and extraction=='canonical_native':
                native=corporate_position_legacy_word_candidates(text,facts)
                views_to_add.extend((fact,'retained_corporate_position_native_word')
                    for fact in native if (fact['product'],fact['effective_date']) not in owned)
                owned.update((fact['product'],fact['effective_date']) for fact in native)
            # A source-bound grid preserves row and date-column ownership.
            # Flattened views cannot override those same source coordinates.
            views_to_add.extend((fact,'retained_corporate_position_text:'+extraction)
                for fact in corporate_position_text_candidates(text,facts)
                if (fact['product'],fact['effective_date']) not in owned)
        for fact,extraction in views_to_add:
            identity=(doc['content_sha256'],json.dumps(fact,sort_keys=True))
            if identity in seen:continue
            seen.add(identity)
            row=dict(fact);row.update(provenance,point_in_time_verified=False,extraction=extraction)
            rows.append(row)
    return rows


def historical_cash_reform(archive, intervals):
    """Bind the dated reform and its preopen cash-processing clock to raw bytes."""
    base='https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/'
    sources=[('10002003020稿轉函_.pdf','292cfafa2c9f08fcde31b4cdc27e4528f7f064c538673028d88fb2ef274f9219','2011-02-01'),
             ('10003001820函.doc','be56396f1c9cd0f95c31642ba0d42f2775e33155ef4c6c4520c04750a22aa3b8','2011-01-28')]
    docs=[]
    for name,sha,published in sources:
        doc=archive.document(base+name)
        if doc['content_sha256']!=sha:raise ValueError('historical cash-reform source changed')
        check_publication(doc['text'],published)
        docs.append(doc)
    text=compact(docs[0]['text'])
    for phrase in ('本年5月3日前業因標的證券除息','契約代碼則維持不變',
                   '調整買方權益數加項及賣方權益數減項','減資退還現金'):
        if phrase not in text:raise ValueError('historical cash-reform clause missing')
    clock=compact(docs[1]['text'])
    if not all(phrase in clock for phrase in ('上午八時至八時三十分','5月3日')):
        raise ValueError('historical cash-reform preopen clock missing')
    origins={}
    hashes={sha for row in intervals if row['effective_date']<'2011-05-03'
            and row['contract']>='201105' and row['deliverable_cash_twd']>0
            for sha in row['source_content_sha256s']}
    for sha in hashes:
        state=archive.conn.execute('SELECT url FROM documents WHERE content_sha256=? AND state=\'complete\'',(sha,)).fetchone()
        if state is None:continue
        source=archive.document(state['url'])
        # The cited corporate reason must explicitly state dividend or cash
        # capital return. A generic cash leg or other benefit is insufficient.
        reason=compact(source['text']).split('公告事項',1)[0]
        if any(word in reason for word in ('受分配其他利益','合併','分割')):continue
        if '現金減資' in reason and '退還股款' in reason:
            origins[sha]='cash_capital_return'
        elif '分派現金股利' in reason:
            origins[sha]='cash_dividend'
    proof=dict(effective_date='2011-05-03',effective_at='2011-05-03T08:30:00+08:00',
        known_at='2011-02-01T23:59:59+08:00',source_content_sha256=sources[0][1],
        source_content_sha256s=[r[1] for r in sources],source_urls=[base+r[0] for r in sources],
        cash_origins=origins)
    result,issues=stock_futures_cash_reform_intervals(intervals,proof,origins)
    return result,issues,proof


def verified_margin_ratio_reviews(archive, paths):
    """Reparse retained originals before composing a dated single-lot ratio."""
    import gzip,hashlib
    from downloader.taifex_rule_parsing import extract_document
    laws=[]
    for path in paths:
        review=json.loads(path.read_text())
        if review.get('review_kind')!='source_bound_mtx_quarter_margin_standard':
            raise ValueError('unsupported margin ratio source review')
        originals={};original_bytes={};source_files={}
        for source in review['sources']:
            item=path.parent/source['path']
            if not item.exists():item=archive.bundle/source['path']
            if (not any(item.resolve().is_relative_to(root.resolve()) for root in (path.parent,archive.bundle))
                    or sha256_file(item)!=source['sha256']):
                raise ValueError('margin ratio source path or SHA mismatch')
            archive.copy(item,source['sha256'],url=source['url'],kind=source['kind'])
            source_files[source['path']]=item
            if source['kind']=='raw_gzip':
                body=gzip.decompress(item.read_bytes());digest=hashlib.sha256(body).hexdigest()
                original_bytes[digest]=body
                originals[digest]=extract_document(body,'',source['url'])['text']
        law=review['law']
        if (law['product'],law['base_product'],law['ratio'],law['rounding'])!=('MTX','TX','1/4','exact'):
            raise ValueError('margin ratio differs from reviewed original clause')
        if (law['effective_date'],law['effective_phase'],law['known_at'])!=(
                '2011-09-22',0,'2011-08-22T23:59:59+08:00'):
            raise ValueError('margin ratio differs from reviewed original effectiveness')
        rule=compact(originals[review['rule_source_content_sha256']]).replace('|','')
        notice=compact(originals[review['announcement_source_content_sha256']])
        effect=compact(originals[review['effectiveness_source_content_sha256']])
        visual=review.get('effectiveness_review')
        if visual:
            import pymupdf
            if (visual['review_kind']!='source_bound_visual_effectiveness_pages'
                    or visual['content_sha256']!=review['effectiveness_source_content_sha256']):
                raise ValueError('margin ratio effectiveness review belongs to another original')
            with pymupdf.open(stream=original_bytes[visual['content_sha256']],filetype='pdf') as pdf:
                for page in visual['pages']:
                    rendered=pdf[page['page']-1].get_pixmap(dpi=150,alpha=False).tobytes('png')
                    if (hashlib.sha256(rendered).hexdigest()!=page['image_sha256']
                            or sha256_file(source_files[page['image_path']])!=page['image_sha256']):
                        raise ValueError('margin ratio reviewed page does not reproduce its source')
            effect=compact('\n'.join(page['native_text'] for page in visual['pages']))
        if not all(phrase in rule for phrase in (
                'MTX保證金標準取TX保證金標準之四分之一訂定',
                '結算保證金、維持保證金、原始保證金標準')):
            raise ValueError('all three margin standards lack the original ratio clause')
        if ('日期2011/08/22' not in notice or '100年9月22日' not in notice
                or '期貨契約價差部位組合保證金計收方式' not in effect
                or '100年9月22日起實施' not in effect):
            raise ValueError('margin ratio publication or legal effectiveness missing')
        if (set(law['source_content_sha256s'])!=set(originals) or law.get('valid_until_date_exclusive')
                or set(law['fields'])!={'initial','maintenance','clearing'}):
            raise ValueError('margin ratio evidence or field scope differs from originals')
        archive.copy(path,sha256_file(path),url='',kind='dated_margin_ratio_review')
        laws.append(law)
    return laws


def main():
    code_paths=[Path(__file__),Path(__file__).resolve().parents[1]/'downloader/taifex_rule_parsing.py',
                Path(__file__).resolve().parents[1]/'stockagent/data/tw_futures_margin_preparation.py']
    code_sha={str(path):sha256_file(path) for path in code_paths}
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--archive',type=Path,default=Path('data_taifex_public_history/rules'))
    p.add_argument('--reviews',type=Path)
    p.add_argument('--base-candidates',type=Path,
                   help='Hash-verified candidate bundle to extend with source reviews only')
    p.add_argument('--reextract-legacy-word',action='store_true',
                   help='Revisit native Word tables when extending an existing candidate bundle')
    p.add_argument('--refresh-clocks',action='store_true',
                   help='Rebind extracted event clocks to verified text already in the bundle')
    p.add_argument('--refresh-margin-clocks',action='store_true',
                   help='Refresh margin clocks only; retain separately source-bound corporate and position clocks.')
    p.add_argument('--reextract-corporate',action='store_true',
                   help='Re-read the retained corporate tables and their local month captions')
    p.add_argument('--extract-corporate-text',action='store_true',
                   help='Add explicit fields from retained older scanned and Word corporate notices')
    p.add_argument('--reextract-native-corporate',action='store_true',
                   help='Revisit all native corporate tables, including newly supported shared-code grids')
    p.add_argument('--reextract-margins',action='store_true',
                   help='Replace automatic margin candidates with the current parser over retained sources')
    p.add_argument('--extract-position-text',action='store_true',
                   help='Read retained position prose and native listing clauses using product-local clocks')
    p.add_argument('--extract-corporate-position-text',action='store_true',
                   help='Read explicit old combined caps against source-bound corporate identities')
    p.add_argument('--corporate-position-source-scope',type=Path,
                   help='SHA-bound original URLs to re-extract; retain unrelated parent position facts')
    p.add_argument('--extract-margin-text',action='store_true',
                   help='Add product-local margin facts from verified retained text and cell grids')
    p.add_argument('--review-override',type=Path,action='append',default=[],
                   help='Hash-verified improved extraction of identical source bytes')
    p.add_argument('--native-tables',type=Path)
    p.add_argument('--ocr-tables',type=Path,action='append',default=[],
                   help='Add separately labelled ruled OCR grids with retained source evidence')
    p.add_argument('--source-reviews',type=Path,action='append',default=[])
    p.add_argument('--margin-code-reviews',type=Path,action='append',default=[],
                   help='Exact source/page/amount-bound corrections of OCR product codes')
    p.add_argument('--position-code-reviews',type=Path,action='append',default=[],
                   help='Exact source/page/cap/underlying-bound position code corrections')
    p.add_argument('--margin-restoration-inputs',type=Path,
                   help='SHA-bound disposition session counts and official cash prints for conditional restores')
    p.add_argument('--margin-restoration-source-scope',type=Path,
                   help='Replace restorations only for these SHA-bound originals, retaining other parent facts')
    p.add_argument('--margin-session-review',type=Path,
                   help='Dated stock-only single-session rule proof for otherwise ambiguous closes')
    p.add_argument('--margin-ratio-source-review',type=Path,action='append',default=[],
                   help='Original-source dated single-lot margin ratio, with exact target/parent ownership')
    p.add_argument('--corporate-source-reviews',type=Path,action='append',default=[],
                   help='Source-bound visual cells replacing automatic views of those exact PDFs only')
    p.add_argument('--position-source-reviews',type=Path,action='append',default=[],
                   help='Source-bound natural-person caps or dated mini aggregation formulas')
    p.add_argument('--position-notice-amendments',type=Path,action='append',default=[],
                   help='Exact source/notice-number-bound revocations and scheduled exceptions')
    p.add_argument('--position-calendar',type=Path,
                   help='Receipted physical calendar for explicit next-business-day position changes')
    p.add_argument('--repair-position-context', action='store_true',
                   help='Bind separate issuing covers and resolve explicit next-nearby expiry clauses')
    p.add_argument('--repair-margin-context', action='store_true',
                   help='Bind incomplete amount-table clocks to verified views of the same original')
    p.add_argument('--repair-position-grade-boundaries', action='store_true',
                   help='Compose explicit latest-grade clauses at same-day restored share-cap boundaries')
    p.add_argument('--position-lifetimes', type=Path,
                   help='Receipted contract lives required for next-nearby position clock resolution')
    p.add_argument('--output-dir',type=Path,required=True)
    a=p.parse_args()
    if a.corporate_position_source_scope and not a.extract_corporate_position_text:
        p.error('--corporate-position-source-scope requires --extract-corporate-position-text')
    if a.margin_restoration_source_scope and not a.margin_restoration_inputs:
        p.error('--margin-restoration-source-scope requires --margin-restoration-inputs')
    if a.repair_position_context and not (a.position_calendar and a.position_lifetimes):
        p.error('--repair-position-context requires --position-calendar and --position-lifetimes')
    if a.position_lifetimes and not a.repair_position_context:
        p.error('--position-lifetimes requires --repair-position-context')
    if a.base_candidates and a.reviews:
        p.error('--base-candidates reuses PDF extraction; do not also select --reviews')
    if not a.base_candidates and a.reviews is None:
        p.error('--reviews is required for a full extraction')
    if a.reextract_legacy_word and not a.base_candidates:
        p.error('--reextract-legacy-word requires --base-candidates')
    if a.reextract_corporate and not a.base_candidates:
        p.error('--reextract-corporate requires --base-candidates')
    if a.reextract_native_corporate and (not a.base_candidates or not a.native_tables):
        p.error('--reextract-native-corporate requires --base-candidates and --native-tables')
    if a.reextract_margins and (not a.base_candidates or not a.native_tables):
        p.error('--reextract-margins requires --base-candidates and --native-tables')
    if (a.output_dir/'manifest.json').exists(): raise FileExistsError('use a new immutable output directory')
    archive=RuleArchive(a.archive,a.output_dir)
    margin_restoration_scope=None
    if a.margin_restoration_source_scope:
        scope=json.loads(a.margin_restoration_source_scope.read_text());rows=scope.get('sources',[])
        if scope.get('schema_version')!=1 or not rows:
            raise ValueError('margin restoration requires a nonempty original source scope')
        margin_restoration_scope={r['source_url'] for r in rows}
        if len(margin_restoration_scope)!=len(rows):raise ValueError('duplicate margin restoration source')
        for row in rows:
            if archive.document(row['source_url'])['content_sha256']!=row['content_sha256']:
                raise ValueError('margin restoration source SHA mismatch')
        archive.copy(a.margin_restoration_source_scope,sha256_file(a.margin_restoration_source_scope),
                     url='',kind='margin_restoration_original_scope')
    corporate_position_scope = None
    if a.corporate_position_source_scope:
        scope = json.loads(a.corporate_position_source_scope.read_text())
        rows = scope.get('sources', [])
        if scope.get('schema_version') != 1 or not rows:
            raise ValueError('corporate position extraction requires a nonempty original source scope')
        corporate_position_scope = {r['source_url'] for r in rows}
        if len(corporate_position_scope) != len(rows):
            raise ValueError('duplicate corporate position extraction source')
        for row in rows:
            if archive.document(row['source_url'])['content_sha256'] != row['content_sha256']:
                raise ValueError('corporate position extraction source SHA mismatch')
        archive.copy(a.corporate_position_source_scope,sha256_file(a.corporate_position_source_scope),
                     url='',kind='corporate_position_extraction_scope')
    position_dates=None
    if a.position_calendar:
        receipt=a.position_calendar.with_name('manifest.json')
        digest=sha256_file(a.position_calendar)
        if json.loads(receipt.read_text())['outputs'].get(a.position_calendar.name,{}).get('sha256')!=digest:
            raise ValueError('position effective calendar SHA mismatch')
        position_dates=pl.scan_parquet(a.position_calendar).filter(pl.col('product')=='TX').select(
            pl.col('date').unique().sort()).collect()['date'].to_list()
        if not position_dates:raise ValueError('position calendar requires verified TX sessions')
        archive.copy(a.position_calendar,digest,url='',kind='position_effective_calendar')
        archive.copy(receipt,sha256_file(receipt),url='',kind='position_effective_calendar_manifest')
    notices=[] if a.base_candidates else [dict(r) for r in archive.conn.execute(
        "SELECT * FROM announcements ORDER BY published_date,url")]
    if a.base_candidates:
        facts,position_facts,corporate_facts,audit=reuse_candidate_bundle(archive,a.base_candidates)
    else:
        facts=[];position_facts=[];corporate_facts=[];audit=[]
    inherited_position_facts = list(position_facts)
    override_admissions=[retain_review_override(archive,root) for root in a.review_override]
    for ann in notices:
        errors=[];found=0
        urls=[ann['url'],*archive.children(ann['url'])]
        for url in urls:
            state=archive.conn.execute('SELECT * FROM documents WHERE url=?',(url,)).fetchone()
            if state is None or state['state']!='complete' or not state['parsed_path']: continue
            try: doc=archive.document(url)
            except ValueError as exc:
                errors.append(dict(url=url,error=str(exc)));continue
            text=doc['text']; extraction='canonical_native'
            reviewed=a.reviews/'documents'/doc['content_sha256']
            if (reviewed/'receipt.json').exists():
                rec=json.loads((reviewed/'receipt.json').read_text())
                if rec['content_sha256']!=doc['content_sha256']: raise ValueError('review source mismatch')
                for f in rec['files']:
                    if sha256_file(reviewed/f['path'])!=f['sha256']: raise ValueError('review file SHA mismatch')
                if rec['status']=='complete':
                    text=(reviewed/'candidate.txt').read_text()
                    extraction='review_pages_including_ocr' if any(x['extraction']=='ocr_candidate' for x in rec['pages']) else 'review_native'
                    archive.copy(reviewed/'candidate.txt',sha256_file(reviewed/'candidate.txt'),url=url,kind=extraction)
                    archive.copy(reviewed/'receipt.json',sha256_file(reviewed/'receipt.json'),url=url,kind='page_extraction_receipt')
            candidates=margin_table_candidates(text)
            # Native legacy Word tables are not PDF cell grids. Their explicit
            # pipes and local percent cells survive antiword extraction.
            if doc.get('format') == 'legacy_word':
                candidates.extend(margin_legacy_word_levels(doc['text']))
            native_text=None;position_candidates=[];corporate_candidates=[]
            if doc.get('format') == 'legacy_word':
                position_candidates.extend(position_legacy_word_candidates(doc['text']))
            if a.native_tables:
                table_root=a.native_tables/'documents'/doc['content_sha256']
                table_receipt=table_root/'receipt.json'
                if table_receipt.exists():
                    tr=json.loads(table_receipt.read_text())
                    if tr['content_sha256']!=doc['content_sha256']:raise ValueError('native table source mismatch')
                    for f in tr['files']:
                        if sha256_file(table_root/f['path'])!=f['sha256']:raise ValueError('native table SHA mismatch')
                    if tr['status']=='complete':
                        tables=json.loads((table_root/'tables.json').read_text())
                        native_text='\n'.join(page['native_text'] for page in tables['pages'])
                        grid_facts=[]
                        for page in tables['pages']:
                            for ti,table in enumerate(page['tables']):
                                for fact in margin_grid_candidates(table['cells'],table['caption'],source_text=native_text):
                                    grid_facts.append(dict(fact,page=page['page'],table_index=ti,
                                                          extraction_method='native_cell_grid'))
                                for fact in position_grid_candidates(table['cells'],native_text,
                                        caption=table.get('caption',''),published_date=ann['published_date'],
                                        market_dates=position_dates):
                                    position_candidates.append(dict(fact,page=page['page'],table_index=ti,
                                                                    extraction_method='native_cell_grid'))
                        corporate_candidates=corporate_native_table_candidates(tables['pages'])
                        if grid_facts or position_candidates or corporate_candidates:
                            for name in ('tables.json','receipt.json'):
                                archive.copy(table_root/name,sha256_file(table_root/name),url=url,kind='native_table_'+name)
                            candidates.extend(grid_facts)
            # One source can expose the same six numbers through both views.
            unique={}
            for candidate in candidates:
                key=(candidate['product'],candidate['margin_kind'],tuple(candidate['after']),
                     None if candidate['before'] is None else tuple(candidate['before']))
                unique[key]=candidate
            candidates=list(unique.values())
            if not (candidates or position_candidates or corporate_candidates): continue
            clock_text=native_text or text
            clock=candidate_notice_clock(clock_text,ann['published_date'])
            known=timestamp(date.fromisoformat(ann['published_date']),'23:59:59')
            provenance=dict(source_url=url,announcement_url=ann['url'],published_date=ann['published_date'],
                source_content_sha256=doc['content_sha256'],known_at=known,
                point_in_time_verified=False,issue_date_bound=clock['issue_date_bound'])
            for target,extra in ((position_facts,position_candidates),(corporate_facts,corporate_candidates)):
                target.extend(dict(candidate,**provenance) for candidate in extra)
            for candidate in candidates:
                scoped_clock=product_margin_restoration_clock(clock_text,candidate['product'],clock)
                candidate.update(source_url=url,announcement_url=ann['url'],published_date=ann['published_date'],
                    source_content_sha256=doc['content_sha256'],known_at=known,
                    requires_dated_session_rule=True,extraction=extraction,point_in_time_verified=False,**scoped_clock)
                facts.append(candidate);found+=1
        audit.append(dict(announcement_url=ann['url'],published_date=ann['published_date'],title=ann['title'],
                          candidate_tables=found,errors=errors))
    if a.reextract_legacy_word:
        legacy_margins,legacy_positions,legacy_audit=legacy_archive_candidates(archive)
        facts.extend(legacy_margins);position_facts.extend(legacy_positions);audit.extend(legacy_audit)
    if a.reextract_margins:
        facts=[r for r in facts if r.get('extraction')=='source_bound_visual_transcription']
        facts.extend(reextract_retained_margin_text(archive))
        facts.extend(native_position_candidates(archive,a.native_tables,margin=True))
    if a.extract_position_text:
        position_facts=[r for r in position_facts if not (r.get('extraction') or '').startswith('retained_position_text:')]
        position_facts.extend(retained_position_text_candidates(archive))
    if a.base_candidates and a.native_tables:
        position_facts.extend(native_position_candidates(archive,a.native_tables,market_dates=position_dates))
    if a.reextract_native_corporate:
        corporate_facts = [r for r in corporate_facts if r.get('extraction_method') != 'native_cell_grid']
        corporate_facts.extend(native_position_candidates(archive,a.native_tables,corporate=True))
    for root in a.ocr_tables:
        proof=json.loads((root/'manifest.json').read_text())
        if proof['profile'] not in ('source_ruled_ocr_cell_grid_v1',
                                    'source_ruled_ocr_cell_grid_v2',
                                    'source_ruled_ocr_cell_grid_v3') or proof['status']!='complete':
            raise ValueError('OCR grid bundle must have a complete source-bound receipt')
        corporate_facts.extend(native_position_candidates(archive,root,corporate=True))
        position_facts.extend(native_position_candidates(archive,root,market_dates=position_dates))
    for path in a.source_reviews:
        reviewed_margins,reviewed_positions=source_review_candidates(archive,path)
        facts=replace_reviewed_product_facts(facts, reviewed_margins)
        position_facts.extend(reviewed_positions)
    corporate_refreshed=refresh_corporate_candidates(archive,corporate_facts) if a.reextract_corporate else 0
    corporate_text_issues=[];corporate_text_added=0
    if a.extract_corporate_text:
        corporate_facts=[r for r in corporate_facts
                         if r.get('extraction_method') != 'explicit_text_fields_requires_source_review']
        added,corporate_text_issues=retained_corporate_text_candidates(archive,corporate_facts)
        corporate_text_added=len(added);corporate_facts.extend(added)
    for path in a.corporate_source_reviews:
        reviewed_corporate,reviewed_positions,replaced=corporate_source_review_candidates(archive,path)
        # A document can contain several adjusted generations and a separate
        # position table. A review of one table cannot erase its siblings.
        corporate_facts=replace_reviewed_corporate_facts(corporate_facts,reviewed_corporate)
        position_facts=replace_reviewed_product_facts(position_facts,reviewed_positions)
    for path in a.position_source_reviews:
        position_facts=replace_reviewed_product_facts(position_facts,
            position_source_review_candidates(archive,path,corporate_facts,position_facts))
    if a.extract_corporate_position_text:
        # A re-extraction must replace its own prior automatic views; otherwise
        # a corrected parser leaves rejected OCR caps alive in the parent.
        prior=[r for r in position_facts if str(r.get('extraction') or '').startswith(
            ('retained_corporate_position_text:','retained_corporate_position_table:'))
            and (corporate_position_scope is None or r.get('source_url') in corporate_position_scope)]
        atomic_write_json(a.output_dir/'prior_automatic_corporate_position_facts.json',prior)
        position_facts=[r for r in position_facts if r not in prior]
        reviewed={(r.get('source_content_sha256'),r.get('product')) for r in position_facts
            if str(r.get('extraction_method') or '').startswith('source_bound_visual_')
            or r.get('extraction')=='source_composed_position_grade_boundary'}
        fresh=retained_corporate_position_text_candidates(archive,corporate_facts,
                                                        source_urls=corporate_position_scope)
        position_facts.extend(r for r in fresh if (r.get('source_content_sha256'),r.get('product')) not in reviewed)
    if a.extract_margin_text:
        text_facts=retain_margin_code_corrections(facts,reextract_retained_margin_text(archive))
        # Manual reviewed cells retain priority for their exact source/product.
        reviewed={(r['source_content_sha256'],r['product']) for r in facts
                  if r.get('extraction')=='source_bound_visual_transcription'}
        facts.extend(r for r in text_facts if (r['source_content_sha256'],r['product']) not in reviewed)
    refreshed=refresh_candidate_clocks(archive,facts,position_facts,corporate_facts) if a.refresh_clocks or not a.base_candidates else 0
    if a.refresh_margin_clocks and not a.refresh_clocks and a.base_candidates:
        refreshed=refresh_candidate_clocks(archive,facts,[],[])
    if a.repair_margin_context:
        repair_missing_margin_source_context(archive, facts)
    for path in a.margin_code_reviews:
        facts=apply_margin_code_reviews(archive,facts,path)
    for path in a.position_code_reviews:
        position_facts=apply_product_code_reviews(archive,position_facts,path,kind='position')
    for path in a.position_notice_amendments:
        position_facts=apply_position_notice_amendments(archive,position_facts,path)
    bind_retained_single_session_margin_clocks(archive, facts, a.margin_session_review)
    # A native period amendment needs all prior sources and clocks before it
    # can add coverage; restoration remains independently evidence-bound.
    for index,row in enumerate(facts):
        if not row.get('extension_prior_notice_join_required'):
            continue
        document=archive.document(row['source_url'])
        if document['content_sha256']!=row['source_content_sha256']:
            raise ValueError('native margin extension source SHA mismatch')
        joined=bind_native_margin_period_extension(archive,document['text'],row,facts)
        if joined is not None:
            facts[index]=joined
    if a.margin_restoration_inputs:
        facts=rebuild_disposal_restorations(archive,facts,a.margin_restoration_inputs,
                                          source_urls=margin_restoration_scope)
    # Re-extraction cannot supersede exact cells already inspected on the same
    # immutable original. Other products and source notices remain independent.
    corporate_facts=replace_reviewed_corporate_facts(corporate_facts,
        [r for r in corporate_facts if r.get('extraction')=='source_bound_visual_corporate_cells'])
    position_facts=prioritize_reviewed_position_facts(position_facts, inherited_position_facts)
    if a.repair_position_context:
        receipt = a.position_lifetimes.with_name('manifest.json')
        digest = sha256_file(a.position_lifetimes)
        if json.loads(receipt.read_text())['outputs'].get(a.position_lifetimes.name,{}).get('sha256') != digest:
            raise ValueError('position contract lives SHA mismatch')
        lives = pl.read_parquet(a.position_lifetimes)
        archive.copy(a.position_lifetimes, digest, url='', kind='position_expiry_lifetimes')
        archive.copy(receipt, sha256_file(receipt), url='', kind='position_expiry_lifetimes_manifest')
        repaired = repair_position_source_context(archive, position_facts, lives, position_dates)
        atomic_write_json(a.output_dir/'position_context_repairs.json', repaired)
    if a.repair_position_grade_boundaries:
        repair_same_day_position_grade(archive, position_facts)
        repair_active_position_grade_boundaries(archive, position_facts, corporate_facts)
    if a.repair_position_context:
        repair_original_position_restorations(archive, position_facts, corporate_facts)
    a.output_dir.mkdir(parents=True,exist_ok=True)
    # Re-extraction can find the same evidence through multiple archive links.
    # Keep distinct contradictory facts, but do not grow exact duplicates.
    facts,position_facts,corporate_facts=[list({json.dumps(row,sort_keys=True,ensure_ascii=False):row
        for row in group}.values()) for group in (facts,position_facts,corporate_facts)]
    frame=pl.DataFrame(facts,infer_schema_length=None)
    atomic_write_parquet(a.output_dir/'margin_event_candidates.parquet',frame)
    for name,rows in [('position_event_candidates',position_facts),('corporate_event_candidates',corporate_facts)]:
        other=pl.DataFrame(rows,infer_schema_length=None) if rows else pl.DataFrame(schema={'product':pl.String})
        atomic_write_parquet(a.output_dir/(name+'.parquet'),other)
    intervals,term_issues=corporate_terms_intervals(corporate_facts)
    unit_intervals,unit_issues=corporate_terms_intervals(corporate_facts,unit_only=True)
    if unit_intervals:
        atomic_write_parquet(a.output_dir/'corporate_unit_intervals.parquet',
                             pl.DataFrame(unit_intervals,infer_schema_length=None))
    atomic_write_json(a.output_dir/'corporate_unit_issues.json',unit_issues)
    intervals,reform_issues,reform_proof=historical_cash_reform(archive,intervals)
    term_issues.extend(reform_issues)
    atomic_write_json(a.output_dir/'historical_cash_reform.json',reform_proof)
    if intervals:
        atomic_write_parquet(a.output_dir/'corporate_terms_intervals.parquet',pl.DataFrame(intervals,infer_schema_length=None))
    atomic_write_json(a.output_dir/'corporate_term_issues.json',term_issues)
    margin_intervals,margin_issues=margin_candidate_intervals(facts)
    ratio_reviews=list(a.margin_ratio_source_review)+[archive.bundle/s['path']
        for s in archive.sources.values() if s['kind']=='dated_margin_ratio_review']
    ratio_reviews=list({sha256_file(path):path for path in ratio_reviews}.values())
    margin_ratio_laws=verified_margin_ratio_reviews(archive,ratio_reviews)
    if margin_ratio_laws:
        margin_intervals,ratio_issues=compose_margin_ratio_intervals(margin_intervals,margin_ratio_laws)
        margin_issues.extend(ratio_issues)
        atomic_write_json(a.output_dir/'dated_margin_ratio_laws.json',margin_ratio_laws)
    if margin_intervals:
        atomic_write_parquet(a.output_dir/'margin_level_intervals.parquet',
                             pl.DataFrame(margin_intervals,infer_schema_length=None))
    atomic_write_json(a.output_dir/'margin_interval_issues.json',margin_issues)
    position_intervals,position_issues=position_candidate_intervals(position_facts)
    if position_intervals:
        atomic_write_parquet(a.output_dir/'position_level_intervals.parquet',
                             pl.DataFrame(position_intervals,infer_schema_length=None))
    atomic_write_json(a.output_dir/'position_interval_issues.json',position_issues)
    atomic_write_json(a.output_dir/'announcement_audit.json',audit)
    if frame.height:
        products=frame.group_by('product','margin_kind').agg(pl.len().alias('candidate_events'),
            pl.col('published_date').min().alias('first_notice'),pl.col('published_date').max().alias('last_notice'),
            pl.col('chronological').sum().alias('bounded_clock_events'))
        products.sort('product').write_csv(a.output_dir/'product_event_coverage.csv')
    if any(sha256_file(path)!=code_sha[str(path)] for path in code_paths):
        raise ValueError('rule compiler source changed during build; no complete receipt can be published')
    summary=dict(schema_version=1,status='candidate_facts_require_transition_and_source_review',
        builder_source_sha256=sha256_file(Path(__file__)),clock_parser_version=PARSER_VERSION,
        clock_parser_source_sha256=sha256_file(Path(__file__).resolve().parents[1]/'downloader/taifex_rule_parsing.py'),
        preparation_source_sha256=sha256_file(Path(__file__).resolve().parents[1]/'stockagent/data/tw_futures_margin_preparation.py'),
        all_products_training_ready=False,point_in_time_verified=False,created_at_utc=datetime.now(timezone.utc).isoformat(),
        parent_candidate_manifest_sha256=sha256_file(a.base_candidates/'manifest.json') if a.base_candidates else None,
        review_override_admissions=override_admissions,
        refreshed_source_clocks=refreshed,
        clock_refresh_scope='margin_only' if a.refresh_margin_clocks and not a.refresh_clocks else 'all_or_retained',
        refreshed_corporate_documents=corporate_refreshed,
        corporate_text_added=corporate_text_added,corporate_text_issues=corporate_text_issues,
        corporate_position_source_scope_sha256=(sha256_file(a.corporate_position_source_scope)
                                               if a.corporate_position_source_scope else None),
        margin_restoration_source_scope_sha256=(sha256_file(a.margin_restoration_source_scope)
                                                if a.margin_restoration_source_scope else None),
        announcements=len(audit),candidate_tables=len(facts),candidate_products=frame['product'].n_unique() if frame.height else 0,
        bounded_clock_events=frame['chronological'].sum() if frame.height else 0,
        position_candidate_facts=len(position_facts),
        position_candidate_products=len({f['product'] for f in position_facts}),
        position_level_intervals=len(position_intervals),
        position_interval_products=len({f['product'] for f in position_intervals}),
        position_interval_issues=len(position_issues),
        candidate_interval_contract_version=3 if margin_ratio_laws else 2,
        corporate_candidate_facts=len(corporate_facts),
        corporate_candidate_products=len({f['product'] for f in corporate_facts}),
        corporate_term_intervals=len(intervals),corporate_term_products=len({f['product'] for f in intervals}),
        corporate_term_issues=len(term_issues),
        corporate_unit_intervals=len(unit_intervals),corporate_unit_issues=len(unit_issues),
        margin_level_intervals=len(margin_intervals),
        margin_interval_products=len({f['product'] for f in margin_intervals}),
        margin_interval_issues=len(margin_issues),
        sources=list(archive.sources.values()),outputs={f.name:dict(sha256=sha256_file(f))
            for f in a.output_dir.iterdir() if f.is_file()})
    atomic_write_json(a.output_dir/'manifest.json',summary)
    print(json.dumps({k:v for k,v in summary.items() if k not in ['sources','outputs']},ensure_ascii=False))


if __name__=='__main__':main()
