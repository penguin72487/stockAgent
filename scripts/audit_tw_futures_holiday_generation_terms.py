#!/usr/bin/env python3
"""Bind holiday generation candidates to exact archived TAIFEX terms.

This source audit does not admit an account transition or infer missing values.
It produces an exact TEJ investigation worklist only for unique matched terms.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import date, datetime, timezone
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import sqlite3

import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.audit_tw_futures_corporate_vendor import _inside
from scripts.audit_tw_futures_holiday_generation_paths import stable_digest


def match_term(candidate: dict, terms: list[dict]) -> dict:
    """Match old/new identity, month, recorded boundary and original notice."""
    sources = candidate.get('pre_boundary_source_sha256s')
    boundaries = candidate.get('recorded_corporate_boundaries')
    target = candidate.get('pre_transfer_target_candidate')
    if not target or not sources or not boundaries:
        raise ValueError('candidate source, target or boundary missing')
    if len(set(sources)) != len(sources):
        raise ValueError('candidate original source identity is ambiguous')
    start, end = date.fromisoformat(candidate['pre_date']), date.fromisoformat(candidate['settlement_date'])
    if any(not start < date.fromisoformat(d) <= end for d in boundaries):
        raise ValueError('candidate boundary is outside its contract interval')
    matches = [r for r in terms if r['product'] == target and r['from_product'] == candidate['product']
               and r['contract'] == candidate['contract'] and str(r['effective_date']) in boundaries
               and set(r.get('source_content_sha256s') or ()) & set(sources)]
    result = {k: candidate[k] for k in ('product', 'contract', 'pre_date', 'pre_physical_contract',
                                       'settlement_date', 'pre_transfer_target_candidate',
                                       'pre_boundary_source_sha256s', 'recorded_corporate_boundaries')}
    result['matching_term_count'] = len(matches)
    result['status'] = ('unique_candidate_term' if len(matches) == 1 else
                        'term_not_found' if not matches else 'ambiguous_candidate_terms')
    if len(matches) == 1:
        term = matches[0]
        result['term'] = {k: term.get(k) for k in (
            'effective_date', 'contract_multiplier', 'deliverable_cash_twd',
            'subscription_rights_at_final_settlement', 'fixed_subscription_rights_twd',
            'fixed_subscription_rights_evidence',
            'equity_cash_credit_twd', 'has_equity_credit_fields', 'carry_quantity_numerator',
            'carry_quantity_denominator', 'known_at', 'source_content_sha256s', 'source_urls',
            'point_in_time_verified', 'requires_phase_and_lifecycle_admission')}
    return result


def verify_originals(workspace: Path, rows: list[dict], additional_sources=()) -> dict[str, dict]:
    root = workspace / 'data_taifex_public_history/rules'
    originals = {}
    with sqlite3.connect(f"file:{root / 'state/queue.sqlite3'}?mode=ro", uri=True) as db:
        digests = {s for row in rows for s in row['pre_boundary_source_sha256s']} | set(additional_sources)
        for digest in sorted(digests):
            found = db.execute('SELECT raw_path,raw_sha256 FROM documents WHERE content_sha256=? AND state=?',
                               (digest, 'complete')).fetchall()
            if len(found) != 1:
                raise ValueError('official original missing or ambiguous')
            raw = _inside(root, found[0][0])
            if (stable_digest(raw) != found[0][1]
                    or hashlib.sha256(gzip.decompress(raw.read_bytes())).hexdigest() != digest
                    or stable_digest(raw) != found[0][1]):
                raise ValueError('official original changed or has a different content hash')
            originals[digest] = {'path': str(raw.relative_to(workspace)), 'raw_sha256': found[0][1]}
    return originals


def build_worklist(rows: list[dict]) -> str:
    output = io.StringIO(newline='')
    writer = csv.DictWriter(output, fieldnames=['requested_fields', 'product', 'contract', 'exact_dates'],
                            lineterminator='\n')
    writer.writeheader()
    for row in rows:
        if row['status'] == 'unique_candidate_term':
            writer.writerow({'requested_fields': 'corporate_adjustment_reference',
                             'product': row['pre_transfer_target_candidate'], 'contract': row['contract'],
                             'exact_dates': row['term']['effective_date']})
    return output.getvalue()


def match_terminal_candidate(candidate: dict, terminals: list[dict]) -> dict:
    """Link terminal rights only by identity, own notice and own settlement day.

    This is source investigation, not an admission into a different rules
    parent. A second recorded boundary requires its own complete identity chain.
    """
    term = candidate.get('term')
    if not term:
        return {'status': 'term_unmatched'}
    flag = term.get('subscription_rights_at_final_settlement')
    if flag is None:
        return {'status': 'rights_flag_unknown'}
    if flag is False:
        return {'status': 'no_outstanding_rights_in_matched_term'}
    if flag is not True:
        raise ValueError('terminal candidate has an invalid rights flag')
    if len(candidate['recorded_corporate_boundaries']) != 1:
        return {'status': 'multi_boundary_source_chain_required'}
    matches = [row for row in terminals
               if row['product'] == candidate['pre_transfer_target_candidate']
               and row['contract'] == candidate['contract']
               and str(row['date']) == candidate['settlement_date']
               and row['notice_content_sha256'] in term['source_content_sha256s']]
    if len(matches) != 1:
        return {'status': 'terminal_not_found' if not matches else 'terminal_ambiguous',
                'matching_rows': len(matches)}
    row = matches[0]
    if (row['rights_twd'] is None or not math.isfinite(row['rights_twd']) or row['rights_twd'] < 0
            or row['fixing_date'] is None or row['fixing_date'] > row['date']):
        raise ValueError('terminal candidate has invalid rights or fixing date')
    return {'status': 'source_bound_terminal_candidate',
            'product': row['product'], 'contract': row['contract'],
            'date': str(row['date']), 'fixing_date': str(row['fixing_date']),
            'rights_twd': row['rights_twd'], 'stock_close': row['stock_close'],
            'subscription_price': row['subscription_price'],
            'notice_content_sha256': row['notice_content_sha256'],
            'subscription_source_sha256': row['subscription_source_sha256'],
            'cash_source_sha256': row['cash_source_sha256']}


def diagnose_current_terminal_bindings(rows: list[dict], terms: pl.DataFrame,
                                       terminals: pl.DataFrame) -> dict:
    """Rebind matched terminal identities without adopting old positional ids.

    The terminal parquet's corporate_term_id belongs to its own rules parent.
    A new rules parquet may reorder every row; matching the notice and the
    interval is evidence for further review, not account or PIT admission.
    """
    from stockagent.data.tw_futures_margin_preparation import bind_dated_corporate_terms

    selected = [r for r in rows if r.get('terminal_rights_candidate', {}).get('status')
                == 'source_bound_terminal_candidate']
    if not selected:
        return {}
    query_rows = [{'date': date.fromisoformat(r['settlement_date']),
                   'product': r['pre_transfer_target_candidate'], 'contract': r['contract']}
                  for r in selected]
    query = pl.DataFrame(query_rows)
    if query.select('date', 'product', 'contract').is_duplicated().any():
        raise ValueError('ambiguous terminal identity for current-rules diagnostic')
    bound = bind_dated_corporate_terms(query, terms)
    current = terms.with_row_index('corporate_term_id').select('corporate_term_id', 'effective_date')
    old = terminals.select('date', 'product', 'contract',
                           pl.col('corporate_term_id').alias('stored_corporate_term_id'),
                           'notice_content_sha256')
    joined = (bound.join(current, on='corporate_term_id', how='left', validate='m:1')
              .join(old, on=['date', 'product', 'contract'], how='left', validate='1:1'))
    if joined.height != len(selected):
        raise ValueError('terminal current-rules binding changed identity count')
    lookup = {(str(x['date']), x['product'], x['contract']): x for x in joined.to_dicts()}
    for row in selected:
        key = (row['settlement_date'], row['pre_transfer_target_candidate'], row['contract'])
        current_row = lookup[key]
        notice_matches = (current_row['notice_content_sha256'] is not None
                          and current_row['notice_content_sha256'] in
                          (current_row['source_content_sha256s'] or []))
        effective_matches = current_row['effective_date'] == row['term']['effective_date']
        valid = (current_row['terms_binding_status'] == 'bound_prior_publication'
                 and current_row['subscription_rights_at_final_settlement'] is True
                 and notice_matches and effective_matches)
        row['terminal_current_rule_diagnostic'] = {
            'status': ('current_identity_matches_review_still_required' if valid
                       else 'current_identity_unresolved'),
            'terms_binding_status': current_row['terms_binding_status'],
            'current_corporate_term_id': current_row['corporate_term_id'],
            'stored_corporate_term_id': current_row['stored_corporate_term_id'],
            'stored_id_matches_current': (current_row['corporate_term_id'] is not None
                                          and current_row['stored_corporate_term_id'] is not None
                                          and current_row['corporate_term_id'] == current_row['stored_corporate_term_id']),
            'notice_matches_current': notice_matches,
            'effective_date_matches_candidate': effective_matches,
            'current_effective_date': current_row['effective_date'],
            'financial_or_training_adopted': False,
        }
    return dict(Counter(r['terminal_current_rule_diagnostic']['status'] for r in selected))


def diagnose_terminal_values(rows: list[dict], terms: pl.DataFrame,
                             terminals: pl.DataFrame, finals: pl.DataFrame) -> dict:
    """Recalculate matched final values without adopting their parent bundle.

    The canonical binder joins rights by date, physical code, month and notice;
    it does not use a term parquet's unstable positional row number. Agreement
    with an earlier candidate is diagnostic, not proof of a holding transition.
    """
    from stockagent.data.tw_futures_margin_preparation import bind_adjusted_terminal_values

    selected = [r for r in rows if r.get('terminal_rights_candidate', {}).get('status')
                == 'source_bound_terminal_candidate']
    if not selected:
        return {}
    keys = ['date', 'product', 'contract']
    query = pl.DataFrame([{'date': date.fromisoformat(r['settlement_date']),
                           'product': r['pre_transfer_target_candidate'], 'contract': r['contract']}
                          for r in selected])
    if query.select(keys).is_duplicated().any() or terminals.select(keys).is_duplicated().any():
        raise ValueError('ambiguous terminal value identity')
    needed = finals.rename({'settlement_date': 'date'}).join(query, on=keys, how='semi')
    if needed.select(keys).is_duplicated().any():
        raise ValueError('ambiguous official final settlement')
    bound = bind_adjusted_terminal_values(needed.select(*keys, 'final_settlement_price',
                                            'final_settlement_value'), terms, terminals)
    values = bound.join(terminals.select(*keys, 'derived_terminal_value_twd'),
                        on=keys, how='left', validate='1:1')
    lookup = {(str(r['date']), r['product'], r['contract']): r for r in values.to_dicts()}
    for row in selected:
        key = (row['settlement_date'], row['pre_transfer_target_candidate'], row['contract'])
        result = lookup.get(key)
        if result is None:
            row['terminal_value_rebinding_diagnostic'] = {'status': 'official_final_missing',
                                                           'financial_or_training_adopted': False}
            continue
        value, old = result['terminal_value_input_twd'], result['derived_terminal_value_twd']
        numeric = (value is not None and old is not None and math.isfinite(value) and math.isfinite(old))
        source_components = result['terminal_value_binding_status'] in {
            'source_bound_subscription_components', 'source_bound_fixed_subscription_components'}
        same = numeric and math.isclose(value, old, rel_tol=0, abs_tol=1e-9)
        row['terminal_value_rebinding_diagnostic'] = {
            'status': ('source_components_match_previous_candidate' if source_components and same
                       else 'source_components_or_value_unresolved'),
            'binding_status': result['terminal_value_binding_status'],
            'current_value_twd': value, 'previous_candidate_value_twd': old,
            'financial_or_training_adopted': False,
        }
    return dict(Counter(r['terminal_value_rebinding_diagnostic']['status'] for r in selected))


def trace_multi_boundary_candidate(row: dict, terms: list[dict]) -> dict:
    """Trace candidate terms by current identity; never join a reused code by month alone."""
    if row['status'] != 'unique_candidate_term' or len(row['recorded_corporate_boundaries']) < 2:
        return {'status': 'not_applicable', 'financial_or_training_adopted': False}
    boundaries = sorted(row['recorded_corporate_boundaries'])
    start, end = date.fromisoformat(row['pre_date']), date.fromisoformat(row['settlement_date'])
    if len(boundaries) != len(set(boundaries)) or any(not start < date.fromisoformat(x) <= end for x in boundaries):
        raise ValueError('invalid candidate corporate boundaries')
    matching = [x for x in terms if x['contract'] == row['contract'] and x['from_product'] == row['product']
                and x['product'] == row['pre_transfer_target_candidate']
                and str(x['effective_date']) == boundaries[0]
                and set(x.get('source_content_sha256s') or ()) & set(row['pre_boundary_source_sha256s'])]
    if len(matching) != 1:
        return {'status': 'first_boundary_not_unique', 'financial_or_training_adopted': False}
    steps = [matching[0]]
    while True:
        current = steps[-1]
        subsequent = [x for x in terms if x['contract'] == row['contract']
                      and x['from_product'] == current['product']
                      and str(current['effective_date']) < str(x['effective_date']) <= row['settlement_date']]
        if not subsequent:
            break
        day = min(str(x['effective_date']) for x in subsequent)
        matches = [x for x in subsequent if str(x['effective_date']) == day]
        if len(matches) != 1:
            return {'status': 'next_boundary_not_unique', 'date': day, 'financial_or_training_adopted': False}
        if str(current.get('valid_until_exclusive')) != day:
            return {'status': 'interval_or_generation_unresolved', 'date': day,
                    'financial_or_training_adopted': False}
        steps.append(matches[0])
    if not set(boundaries).issubset({str(x['effective_date']) for x in steps}):
        return {'status': 'recorded_boundary_missing', 'financial_or_training_adopted': False}
    last_end = steps[-1].get('valid_until_exclusive')
    if last_end is not None and str(last_end) <= row['settlement_date']:
        return {'status': 'terminal_interval_ended', 'financial_or_training_adopted': False}
    if any(not x.get('source_content_sha256s') or not x.get('known_at')
           or str(x['known_at'])[:10] >= str(x['effective_date']) for x in steps):
        return {'status': 'step_source_or_prior_publication_unresolved',
                'financial_or_training_adopted': False}
    fields = ('from_product', 'product', 'contract', 'effective_date', 'valid_until_exclusive',
              'end_includes_outgoing_transfer', 'known_at', 'source_content_sha256s', 'source_urls',
              'contract_multiplier', 'equity_cash_credit_twd', 'fixed_subscription_rights_twd',
              'subscription_rights_at_final_settlement', 'point_in_time_verified',
              'requires_phase_and_lifecycle_admission')
    return {'status': 'source_candidate_chain', 'steps': [{k: x.get(k) for k in fields} for x in steps],
            'terminal_product': steps[-1]['product'], 'financial_or_training_adopted': False}


def diagnose_multi_boundary_terminal(row: dict, chain: dict, terms: pl.DataFrame,
                                     terminals: pl.DataFrame, finals: pl.DataFrame) -> dict:
    """Compare a traced terminal with retained source components; do not adopt it."""
    from stockagent.data.tw_futures_margin_preparation import bind_adjusted_terminal_values
    if chain['status'] != 'source_candidate_chain':
        return {'status': 'chain_unresolved', 'financial_or_training_adopted': False}
    keys = ['date', 'product', 'contract']
    query = pl.DataFrame([{'date': date.fromisoformat(row['settlement_date']),
                           'product': chain['terminal_product'], 'contract': row['contract']}])
    terminal = terminals.join(query, on=keys, how='semi')
    final = finals.rename({'settlement_date': 'date'}).join(query, on=keys, how='semi')
    if terminal.height != 1 or final.height != 1:
        return {'status': 'terminal_source_not_unique', 'terminal_rows': terminal.height,
                'final_rows': final.height, 'financial_or_training_adopted': False}
    old = terminal.to_dicts()[0]
    if old['notice_content_sha256'] not in chain['steps'][-1]['source_content_sha256s']:
        return {'status': 'terminal_notice_mismatch', 'financial_or_training_adopted': False}
    result = bind_adjusted_terminal_values(final.select(*keys, 'final_settlement_price',
                                                        'final_settlement_value'), terms, terminal).to_dicts()[0]
    value, previous = result['terminal_value_input_twd'], old['derived_terminal_value_twd']
    matched = (value is not None and previous is not None and math.isfinite(value) and math.isfinite(previous)
               and math.isclose(value, previous, rel_tol=0, abs_tol=1e-9)
               and result['terminal_value_binding_status'] in
               {'source_bound_subscription_components', 'source_bound_fixed_subscription_components'})
    return {'status': 'source_components_match_previous_candidate' if matched else 'source_components_or_value_unresolved',
            'current_value_twd': value, 'previous_candidate_value_twd': previous,
            'binding_status': result['terminal_value_binding_status'],
            'financial_or_training_adopted': False}


def audit(workspace: Path, candidates: Path, terms: Path, output: Path, worklist: Path,
          terminal: Path | None = None, final: Path | None = None) -> dict:
    workspace = workspace.resolve()
    candidates, terms, output, worklist = (p.resolve() for p in (candidates, terms, output, worklist))
    if (not candidates.is_relative_to(workspace) or not terms.is_relative_to(workspace)
            or not output.is_relative_to(workspace / 'artifacts/markets')
            or not worklist.is_relative_to(workspace / 'artifacts/markets')):
        raise ValueError('inputs and outputs must use their canonical workspace owners')
    if output.exists() or worklist.exists():
        raise ValueError('preserve existing audit and worklist')
    manifest = terms.parent / 'manifest.json'
    input_pins = {str(p.relative_to(workspace)): stable_digest(p) for p in (candidates, terms, manifest)}
    terminal_meta = None
    if final is not None and terminal is None:
        raise ValueError('terminal value comparison requires the terminal components')
    if terminal is not None:
        terminal = terminal.resolve()
        if not terminal.is_relative_to(workspace):
            raise ValueError('terminal source must belong to the workspace')
        terminal_manifest = terminal.parent / 'manifest.json'
        for path in (terminal, terminal_manifest):
            input_pins[str(path.relative_to(workspace))] = stable_digest(path)
        binding_code = workspace / 'stockagent/data/tw_futures_margin_preparation.py'
        input_pins[str(binding_code.relative_to(workspace))] = stable_digest(binding_code)
        terminal_meta = json.loads(terminal_manifest.read_text())
        if (terminal_meta.get('status') != 'source_bound_terminal_components'
                or input_pins[str(terminal.relative_to(workspace))]
                != terminal_meta['outputs'][terminal.name]['sha256']):
            raise ValueError('terminal source manifest identity differs')
    if final is not None:
        final = final.resolve()
        if not final.is_relative_to(workspace):
            raise ValueError('official final source must belong to the workspace')
        final_manifest = final.parent / 'manifest.json'
        for path in (final, final_manifest):
            input_pins[str(path.relative_to(workspace))] = stable_digest(path)
        final_meta = json.loads(final_manifest.read_text())
        if (final_meta.get('status') != 'complete'
                or input_pins[str(final.relative_to(workspace))]
                != final_meta.get('outputs', {}).get('futures_final_settlement_history', {}).get('sha256')):
            raise ValueError('official final settlement source manifest identity differs')
    source = json.loads(candidates.read_text())
    metadata = json.loads(manifest.read_text())
    if (source.get('contract') != 'tw_futures_holiday_generation_paths_source_audit_v1'
            or source.get('financial_or_training_adopted') is not False
            or input_pins[str(terms.relative_to(workspace))] != metadata['outputs'][terms.name]['sha256']):
        raise ValueError('source audit or terms manifest identity differs')
    candidates_rows = source['results']
    if not candidates_rows or len(candidates_rows) != source['candidate_contracts']:
        raise ValueError('candidate count differs')
    keys = [(r['product'], r['contract'], r['pre_physical_contract']) for r in candidates_rows]
    if len(keys) != len(set(keys)):
        raise ValueError('duplicate original physical contract')
    selected = (pl.scan_parquet(terms)
                .filter(pl.col('product').is_in(sorted({r['pre_transfer_target_candidate'] for r in candidates_rows})),
                        pl.col('contract').is_in(sorted({r['contract'] for r in candidates_rows})))
                .collect().to_dicts())
    rows = [match_term(row, selected) for row in candidates_rows]
    current_binding_counts = None
    terminal_value_counts = None
    chain_counts = None
    chain_terminal_counts = None
    chain_sources = set()
    if terminal is not None:
        terminal_frame = pl.read_parquet(terminal)
        terminal_rows = terminal_frame.to_dicts()
        for row in rows:
            row['terminal_rights_candidate'] = match_terminal_candidate(row, terminal_rows)
        term_frame = pl.read_parquet(terms)
        current_binding_counts = diagnose_current_terminal_bindings(rows, term_frame, terminal_frame)
        if final is not None:
            final_frame = pl.read_parquet(final)
            terminal_value_counts = diagnose_terminal_values(rows, term_frame, terminal_frame, final_frame)
            for row in rows:
                if len(row['recorded_corporate_boundaries']) < 2:
                    continue
                relevant = term_frame.filter(pl.col('contract') == row['contract']).to_dicts()
                chain = trace_multi_boundary_candidate(row, relevant)
                row['multi_boundary_chain_diagnostic'] = chain
                row['multi_boundary_terminal_diagnostic'] = diagnose_multi_boundary_terminal(
                    row, chain, term_frame, terminal_frame, final_frame)
                for step in chain.get('steps', []):
                    chain_sources.update(step['source_content_sha256s'])
            chain_counts = dict(Counter(r['multi_boundary_chain_diagnostic']['status'] for r in rows
                                        if 'multi_boundary_chain_diagnostic' in r))
            chain_terminal_counts = dict(Counter(r['multi_boundary_terminal_diagnostic']['status'] for r in rows
                                                 if 'multi_boundary_terminal_diagnostic' in r))
    originals = verify_originals(workspace, rows, chain_sources)
    if any(stable_digest(workspace / name) != digest for name, digest in input_pins.items()):
        raise ValueError('audit input changed during reading')
    text = build_worklist(rows)
    counts = dict(Counter(row['status'] for row in rows))
    result = {'contract': ('tw_futures_holiday_generation_terms_source_audit_v8' if final is not None
                           else 'tw_futures_holiday_generation_terms_source_audit_v6' if terminal is not None
                           else 'tw_futures_holiday_generation_terms_source_audit_v2'),
              'observed_at_utc': datetime.now(timezone.utc).isoformat(),
              'candidate_contracts': len(rows), 'status_counts': counts,
              'official_originals_verified': len(originals), 'official_originals': originals,
              'term_point_in_time_verified': sum(row['term']['point_in_time_verified'] is True
                                                for row in rows if 'term' in row),
              'term_lifecycle_admission_required': sum(row['term']['requires_phase_and_lifecycle_admission'] is True
                                                       for row in rows if 'term' in row),
              'term_subscription_rights_fixing_required': sum(
                  row['term']['subscription_rights_at_final_settlement'] is True
                  for row in rows if 'term' in row),
              'term_subscription_rights_state_unknown': sum(
                  row['term']['subscription_rights_at_final_settlement'] is None
                  for row in rows if 'term' in row),
              'results': rows, 'input_pins': input_pins,
              'audit_script_sha256': stable_digest(Path(__file__)),
              'worklist_path': str(worklist.relative_to(workspace)),
              'worklist_sha256': hashlib.sha256(text.encode('utf-8')).hexdigest(),
              'limitations': ['Candidate terms and archived notices do not prove customer position conversion.',
                              'A zero fixed-rights component does not resolve an outstanding rights-fixing flag.',
                              'PIT, brokerage valuation, quantity, cash legs and end-to-end accounting require separate proof.'],
              'financial_or_training_adopted': False}
    if terminal_meta is not None:
        result['terminal_candidate_status_counts'] = dict(Counter(
            row['terminal_rights_candidate']['status'] for row in rows))
        result['terminal_current_rule_diagnostic_counts'] = current_binding_counts
        result['terminal_rules_parent_matches_current'] = (
            terminal_meta.get('parent_manifest_sha256') == input_pins[str(manifest.relative_to(workspace))])
        result['limitations'].append('Terminal candidates are not a validated rules-parent migration or account transition.')
    if final is not None:
        result['terminal_value_rebinding_diagnostic_counts'] = terminal_value_counts
        result['multi_boundary_chain_diagnostic_counts'] = chain_counts
        result['multi_boundary_terminal_diagnostic_counts'] = chain_terminal_counts
        result['limitations'].append('Matching terminal arithmetic does not prove notice publication, generation transfer or account lifecycle.')
    atomic_write_text(worklist, text, durable=True)
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidates', type=Path, required=True)
    parser.add_argument('--terms', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--worklist', type=Path, required=True)
    parser.add_argument('--terminal', type=Path)
    parser.add_argument('--final', type=Path)
    args = parser.parse_args()
    result = audit(Path(__file__).resolve().parents[1], args.candidates, args.terms, args.output, args.worklist,
                   args.terminal, args.final)
    print(json.dumps({k: result[k] for k in ('candidate_contracts', 'status_counts',
                                            'official_originals_verified', 'term_point_in_time_verified')},
                     ensure_ascii=False))


if __name__ == '__main__':
    main()
