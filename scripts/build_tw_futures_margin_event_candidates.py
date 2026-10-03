#!/usr/bin/env python3
"""Compile hash-bound before/after facts without claiming complete rule history."""
from __future__ import annotations
import argparse
from datetime import date, datetime, timezone
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
    compact,roc_date,ROC_DATE,position_grid_candidates,corporate_grid_candidates,
    margin_legacy_word_levels,position_legacy_word_candidates,corporate_text_candidates,
    corporate_native_table_candidates,
    corporate_terms_intervals, margin_candidate_intervals, corporate_position_table_candidates,
    corporate_position_text_candidates,
    stock_futures_cash_reform_intervals,
    position_prose_candidates,position_candidate_intervals,
    disposal_margin_restorations,
)


def candidate_notice_clock(text: str, published: str) -> dict:
    """Keep product-relative phases and conditional restoration as evidence."""
    issue_bound=False;errors=[]
    try:
        check_publication(text,published);issue_bound=True
    except ValueError as exc:
        # Official press releases use a dated masthead, not 發文日期.
        dense=compact(text)
        mastheads=list(re.finditer(r'[臺台]灣期貨交易所新聞稿(?:部門[:：](?:結算部|交易部|企劃部))?'
                                   r'(?:中華民國)?'+ROC_DATE,dense))
        mastheads.extend(re.finditer(r'[臺台]灣期貨交易所於'+ROC_DATE+r'公告',dense))
        issued={roc_date(m) for m in mastheads}
        explicit_publications={m['date_iso'] for m in temporal_mentions(text) if m['role']=='publication'}
        if (not explicit_publications and len(issued)==1
                and 0 <= (date.fromisoformat(published)-next(iter(issued))).days <= 7):
            issue_bound=True
        else: errors.append(str(exc))
    mentions=temporal_mentions(text)
    ends=[m for m in mentions if m['role']=='effective_end']
    effective=None;phase='unresolved'
    try:
        effective=margin_effective(text)[:10];phase='after_product_regular_close'
    except ValueError as exc:
        starts={(m['date_iso'],m['boundary']) for m in mentions if m['role']=='effective_start'}
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
    return dict(issue_date_bound=issue_bound,effective_date=effective,effective_phase=phase,
        clock_error='; '.join(errors) or None,chronological=bool(effective and effective>published),
        temporary_end_evidence=json.dumps(ends,ensure_ascii=False),
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
        for row in rows:
            if review.get('pages') and row.get('page') not in {p['page'] for p in review['pages']}:
                raise ValueError('margin cell has no inspected page')
            values=[float(row['after'][k]) for k in ('initial','maintenance','clearing')]
            if (not all(math.isfinite(v) and v>0 for v in values) or not values[0]>=values[1]>=values[2]
                    or (review['margin_kind']=='notional_rate' and values[0]>1)):
                raise ValueError('invalid reviewed margin hierarchy or unit')
            facts.append(dict(provenance,product=row['product'],margin_kind=review['margin_kind'],
                after=values,before=None,requires_dated_session_rule=True,
                requires_reversion_review=review.get('requires_effective_date_review',False),
                temporary_end_evidence='[]',event_type='absolute_level'))
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
    return [r for r in extracted if key(r,r['product']) not in corrected]


def replace_reviewed_product_facts(existing, reviewed):
    """A partial table review only replaces that source's reviewed products."""
    keys = {(r['source_content_sha256'], r['product']) for r in reviewed}
    return [r for r in existing if (r['source_content_sha256'], r['product']) not in keys] + reviewed


def position_source_review_candidates(archive, path, corporate_facts=None, position_facts=None):
    """Read inspected position cells without inventing a companion margin row."""
    from fractions import Fraction
    rows=[]
    for review in json.loads(path.read_text())['reviews']:
        is_corporate=review.get('review_kind')=='source_bound_visual_corporate_position_cells'
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
            parsed=corporate_position_table_candidates(review['pages'],corporate=identities)
            if not parsed or any(r['effective_date']<review['effective_date'] for r in parsed):
                raise ValueError('reviewed position cells disagree with corporate terms/clock')
            rows.extend(dict(r,source_url=review['source_url'],announcement_url=review['source_url'],
                published_date=review['published_date'],source_content_sha256=review['content_sha256'],
                known_at=expected,issue_date_bound=True,point_in_time_verified=False,candidate_only=True,
                extraction_method='source_bound_visual_corporate_position_cells',visual_review_sha256=sha256_file(path))
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


def corporate_source_review_candidates(archive, path):
    """Bind a visual cell transcription to the exact original PDF and notice."""
    payload=json.loads(path.read_text());reviews=payload.get('reviews',[payload])
    corporate=[];positions=[];replaced=set()
    for review in reviews:
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
        corporate.extend(dict(fact,**provenance) for fact in facts)
        positions.extend(dict(fact,**provenance) for fact in corporate_position_table_candidates(pages))
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
        found=[]
        for page in ([] if corporate else tables['pages']):
            for ti,table in enumerate(page['tables']):
                rows = (margin_grid_candidates(table['cells'],table.get('caption','')) if margin else
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
            found.extend(corporate_position_table_candidates(tables['pages']))
        if not found: continue
        doc=archive.document(notice['url'])
        table_kind='ocr_table_' if proof['profile'] in (
            'source_ruled_ocr_cell_grid_v1', 'source_ruled_ocr_cell_grid_v2',
            'source_ruled_ocr_cell_grid_v3') else 'native_table_'
        for name in ('tables.json','receipt.json'):
            archive.copy(folder/name,sha256_file(folder/name),url=notice['url'],kind=table_kind+name)
        clock=candidate_notice_clock(source_text,notice['published_date'])
        for row in found:
            row.update(source_url=notice['url'],announcement_url=notice['url'],
                published_date=notice['published_date'],source_content_sha256=doc['content_sha256'],
                known_at=timestamp(date.fromisoformat(notice['published_date']),'23:59:59'),
                point_in_time_verified=False,issue_date_bound=clock['issue_date_bound'])
            if margin:
                row.update(clock)
                row.update(extraction='canonical_native_margin_reextraction',requires_dated_session_rule=True)
            positions.append(row)
    return positions


def reextract_retained_margin_text(archive):
    """Read all retained amount text again after a parser fix, preserving sources."""
    retained={}
    for source in archive.sources.values():
        if source['kind'] in ('review_native','review_pages_including_ocr','page_extraction_receipt'):
            retained.setdefault(source['url'],{})[source['kind']]=source
    rows=[]
    query='''SELECT DISTINCT d.url,a.published_date,a.url announcement_url
        FROM announcements a JOIN (SELECT url parent,url child FROM announcements
            UNION SELECT parent,child FROM links) l ON l.parent=a.url
        JOIN documents d ON d.url=l.child WHERE d.state='complete'
        ORDER BY a.published_date,d.url'''
    for notice in archive.conn.execute(query):
        doc=archive.document(notice['url'])
        text=doc['text'];sources=retained.get(notice['url'],{})
        candidate=sources.get('review_pages_including_ocr') or sources.get('review_native')
        if candidate:
            receipt=json.loads((archive.bundle/sources['page_extraction_receipt']['path']).read_text())
            if receipt['content_sha256']!=doc['content_sha256'] or receipt['status']!='complete':continue
            if not any(f['path']=='candidate.txt' and f['sha256']==candidate['sha256'] for f in receipt['files']):
                raise ValueError('retained margin text differs from receipt')
            text=(archive.bundle/candidate['path']).read_text()
        facts=margin_table_candidates(text)
        if doc.get('format')=='legacy_word':facts.extend(margin_legacy_word_levels(doc['text']))
        if not facts:continue
        clock=candidate_notice_clock(text,notice['published_date'])
        for fact in facts:
            rows.append(dict(fact,**clock,source_url=notice['url'],announcement_url=notice['announcement_url'],
                published_date=notice['published_date'],source_content_sha256=doc['content_sha256'],
                known_at=timestamp(date.fromisoformat(notice['published_date']),'23:59:59'),
                extraction='canonical_retained_margin_reextraction',point_in_time_verified=False,
                requires_dated_session_rule=True))
    return rows


def verified_disposal_restorations(archive, facts, path):
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
        if row.get('requires_reversion_review') and row.get('before'):
            url=row['source_url']
            if url not in texts:
                doc=archive.document(url)
                texts[url]=(doc['content_sha256'],[compact(t) for t,k in retained_document_text_views(archive,url,doc)])
            digest,views=texts[url]
            if digest!=row['source_content_sha256']:raise ValueError('restoration notice SHA mismatch')
            if any(re.search(r'恢復為(?:'+ROC_DATE+r')?(?:調整前)(?:之)?保證金',t) for t in views):
                row['restoration_rule']='return_to_declared_before'
        verified.append(row)
    rows,issues=disposal_margin_restorations(verified,loaded['universe'],loaded['dispositions'],loaded['observations'])
    archive.copy(path,sha256_file(path),url='',kind='restoration_evidence_manifest')
    atomic_write_json(archive.bundle/'restoration_condition_issues.json',issues)
    return rows


def retain_review_override(archive, root):
    """Replace extraction, not source history; verify every declared page file."""
    manifest=json.loads((root/'manifest.json').read_text())
    if manifest['status']!='complete' or manifest['point_in_time_verified'] is not False:
        raise ValueError('review override must be a complete unapproved extraction')
    for record in manifest['documents']:
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
    archive.copy(root/'manifest.json',sha256_file(root/'manifest.json'),url='',kind='review_override_manifest')


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
    clocks={}
    def clock(row):
        key=(row['source_url'],row['source_content_sha256'],row['published_date'])
        if key in clocks: return clocks[key]
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
        clocks[key]=candidate_notice_clock('\n'.join(texts),row['published_date'])
        return clocks[key]
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


def retained_document_text_views(archive, url, document):
    """Read every receipt-bound view of one source, without last-view priority."""
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


def retained_corporate_text_candidates(archive, existing):
    """Use retained source text for old scanned/Word notices, without new OCR."""
    rows=[];issues=[]
    seen=set()
    query='''SELECT DISTINCT d.url,a.published_date,a.url announcement_url
        FROM announcements a JOIN (SELECT url parent,url child FROM announcements
            UNION SELECT parent,child FROM links) l ON l.parent=a.url
        JOIN documents d ON d.url=l.child
        WHERE a.category='contract_adjustments' AND d.state='complete'
        ORDER BY a.published_date,d.url'''
    for notice in archive.conn.execute(query):
        doc=archive.document(notice['url'])
        for text,extraction in retained_document_text_views(archive,notice['url'],doc):
            clock=candidate_notice_clock(text,notice['published_date'])
            for fact in corporate_text_candidates(text):
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


def retained_corporate_position_text_candidates(archive, corporate):
    grouped={};rows=[];seen=set()
    # Use the existing resolver's agreed physical identities. An incomplete
    # OCR view is not another contract generation. Conflicting resolved terms
    # remain a barrier in corporate_terms_intervals; do not guess their units.
    resolved,_=corporate_terms_intervals(corporate)
    identities={(sha,r['from_product'],r['product'],r['effective_date'],r['contract_multiplier'])
        for r in resolved for sha in r['source_content_sha256s']}
    for fact in corporate:
        identity=tuple(fact.get(k) for k in ('source_content_sha256','from_product','product',
                                           'effective_date','contract_multiplier'))
        if identity in identities and fact.get('issue_date_bound') and fact.get('source_url'):
            grouped.setdefault(fact['source_url'],[]).append(fact)
    for url, facts in grouped.items():
        doc=archive.document(url)
        if any(f['source_content_sha256']!=doc['content_sha256'] for f in facts):
            raise ValueError('corporate position source identity drift')
        provenance={key:facts[0].get(key) for key in ('source_url','announcement_url',
            'published_date','source_content_sha256','known_at','issue_date_bound')}
        if any(any(f.get(k)!=provenance[k] for k in ('published_date','known_at')) for f in facts):
            continue
        for text, extraction in retained_document_text_views(archive,url,doc):
            for fact in corporate_position_text_candidates(text,facts):
                identity=(doc['content_sha256'],json.dumps(fact,sort_keys=True))
                if identity in seen:continue
                seen.add(identity)
                rows.append(dict(fact,**provenance,point_in_time_verified=False,
                    extraction='retained_corporate_position_text:'+extraction))
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


def main():
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
    p.add_argument('--extract-margin-text',action='store_true',
                   help='Add newly recognized product-local tables from verified retained text')
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
    p.add_argument('--corporate-source-reviews',type=Path,action='append',default=[],
                   help='Source-bound visual cells replacing automatic views of those exact PDFs only')
    p.add_argument('--position-source-reviews',type=Path,action='append',default=[],
                   help='Source-bound natural-person caps or dated mini aggregation formulas')
    p.add_argument('--position-notice-amendments',type=Path,action='append',default=[],
                   help='Exact source/notice-number-bound revocations and scheduled exceptions')
    p.add_argument('--position-calendar',type=Path,
                   help='Receipted physical calendar for explicit next-business-day position changes')
    p.add_argument('--output-dir',type=Path,required=True)
    a=p.parse_args()
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
    for root in a.review_override:
        retain_review_override(archive,root)
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
                                for fact in margin_grid_candidates(table['cells'],table['caption']):
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
                candidate.update(source_url=url,announcement_url=ann['url'],published_date=ann['published_date'],
                    source_content_sha256=doc['content_sha256'],known_at=known,
                    requires_dated_session_rule=True,extraction=extraction,point_in_time_verified=False,**clock)
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
        facts.extend(reviewed_margins);position_facts.extend(reviewed_positions)
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
        corporate_facts=replace_reviewed_product_facts(corporate_facts,reviewed_corporate)
        position_facts=replace_reviewed_product_facts(position_facts,reviewed_positions)
    for path in a.position_source_reviews:
        position_facts=replace_reviewed_product_facts(position_facts,
            position_source_review_candidates(archive,path,corporate_facts,position_facts))
    if a.extract_corporate_position_text:
        position_facts.extend(retained_corporate_position_text_candidates(archive,corporate_facts))
    if a.extract_margin_text:
        text_facts=retain_margin_code_corrections(facts,reextract_retained_margin_text(archive))
        # Manual reviewed cells retain priority for their exact source/product.
        reviewed={(r['source_content_sha256'],r['product']) for r in facts
                  if r.get('extraction')=='source_bound_visual_transcription'}
        facts.extend(r for r in text_facts if (r['source_content_sha256'],r['product']) not in reviewed)
    refreshed=refresh_candidate_clocks(archive,facts,position_facts,corporate_facts) if a.refresh_clocks or not a.base_candidates else 0
    if a.refresh_margin_clocks and not a.refresh_clocks and a.base_candidates:
        refreshed=refresh_candidate_clocks(archive,facts,[],[])
    for path in a.margin_code_reviews:
        facts=apply_margin_code_reviews(archive,facts,path)
    for path in a.position_code_reviews:
        position_facts=apply_product_code_reviews(archive,position_facts,path,kind='position')
    for path in a.position_notice_amendments:
        position_facts=apply_position_notice_amendments(archive,position_facts,path)
    if a.margin_restoration_inputs:
        facts.extend(verified_disposal_restorations(archive,facts,a.margin_restoration_inputs))
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
    summary=dict(schema_version=1,status='candidate_facts_require_transition_and_source_review',
        builder_source_sha256=sha256_file(Path(__file__)),clock_parser_version=PARSER_VERSION,
        clock_parser_source_sha256=sha256_file(Path(__file__).resolve().parents[1]/'downloader/taifex_rule_parsing.py'),
        preparation_source_sha256=sha256_file(Path(__file__).resolve().parents[1]/'stockagent/data/tw_futures_margin_preparation.py'),
        all_products_training_ready=False,point_in_time_verified=False,created_at_utc=datetime.now(timezone.utc).isoformat(),
        parent_candidate_manifest_sha256=sha256_file(a.base_candidates/'manifest.json') if a.base_candidates else None,
        refreshed_source_clocks=refreshed,
        clock_refresh_scope='margin_only' if a.refresh_margin_clocks and not a.refresh_clocks else 'all_or_retained',
        refreshed_corporate_documents=corporate_refreshed,
        corporate_text_added=corporate_text_added,corporate_text_issues=corporate_text_issues,
        announcements=len(audit),candidate_tables=len(facts),candidate_products=frame['product'].n_unique() if frame.height else 0,
        bounded_clock_events=frame['chronological'].sum() if frame.height else 0,
        position_candidate_facts=len(position_facts),
        position_candidate_products=len({f['product'] for f in position_facts}),
        position_level_intervals=len(position_intervals),
        position_interval_products=len({f['product'] for f in position_intervals}),
        position_interval_issues=len(position_issues),
        candidate_interval_contract_version=2,
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
