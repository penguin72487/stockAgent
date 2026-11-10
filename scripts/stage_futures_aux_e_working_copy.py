#!/usr/bin/env python3
"""Stage selected retained futures evidence roots on E using the same owner."""
from pathlib import Path
import json
import time

from downloader.artifact_io import atomic_write_json
from scripts import stage_futures_e_working_copy as stage


SOURCES = (
    ('data_tw_index_futures/preparation_sources/equity_clock_rules_20261023_v2', '0050en_20250618.pdf'),
    ('data_tw_index_futures/preparation_sources/historical_price_rules_20261025_v1', 'manifest.json'),
    ('data_tw_index_futures/preparation_sources/margin_repair_inputs', 'ib1_2012_index_lookup.json'),
    ('data_tw_index_futures/preparation_sources/margin_repair_pending', 'corporate_term_issues.json'),
    ('data_tw_index_futures/preparation_sources/official_final_settlement_all_asset_classes_20260929',
     'futures_final_settlement_history.parquet'),
    ('data_tw_index_futures/preparation_sources/stock_futures_block_audit_20261021',
     'block_2025-01-01_2025-12-31.raw'),
    ('data_tw_index_futures/preparation_sources/stock_futures_block_history2011_20261021_v5',
     'block_2011-01-03_2011-12-31.raw'),
    ('data_tw_index_futures/preparation_sources/stock_futures_price_history2011_20261021_v5', 'receipt.json'),
    ('data_tw_index_futures/preparation_sources/universe725_repair_20261025_v1',
     'valuation/sources/valuation_research_policy.json'),
    ('data_tw_index_futures/preparation_sources/universe725_repair_20261025_v2',
     'valuation/sources/sr1_halt_research_input.json'),
    ('data_tw_futures/research', 'official_catalog_20261103_v1/taifex_stock_futures_catalog.json'),
)


def stage_sources(sources, *, prefix, aggregate_name):
    results = []
    for relative, marker in sources:
        source = stage.ROOT/relative
        name = prefix+source.name+'-e-20261103-v1'
        report = (stage.ROOT/'artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1'
                  /f'{name}.json')
        try:
            stage.main(source=source, name=name, marker=Path(marker), report=report,
                       budget=16*1024**3, seed_hardlinks=True)
            readback = json.loads(report.read_bytes())
            proof = Path(readback['relocation_evidence'])
            if (readback.get('state') != 'verified_temporary_e_source_copy'
                    or readback.get('source') != str(source)
                    or readback.get('source_files_deleted') != 0
                    or stage.relocation.digest(proof, no_follow=True) != readback['relocation_evidence_sha256']):
                raise ValueError('individual E source report or proof differs on independent readback')
            results.append({'source':relative, 'state':'verified_e_copy', 'report':str(report)})
        except Exception as error:
            # One mutable or oversized source must not suppress other roots.
            results.append({'source':relative, 'state':'deferred_or_failed',
                            'error_type':type(error).__name__, 'reason':str(error)[:800]})
            print(json.dumps(results[-1], ensure_ascii=False), flush=True)
    complete = all(row['state'] == 'verified_e_copy' for row in results)
    report = {'contract':'futures_aux_e_staging_v1', 'state':'verified' if complete else 'partial',
              'sources':results, 'source_files_deleted':0, 'source_alias_changed':False,
              'financial_or_training_admission':False, 'observed_at_epoch':time.time()}
    path = (stage.ROOT/'artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1'
            /aggregate_name)
    atomic_write_json(path, report)
    print(json.dumps(report, ensure_ascii=False), flush=True)
    return 0 if complete else 1


def main():
    return stage_sources(SOURCES, prefix='futures-aux-', aggregate_name='futures-aux-sources-e-20261103-v1.json')


if __name__ == '__main__':
    raise SystemExit(main())
