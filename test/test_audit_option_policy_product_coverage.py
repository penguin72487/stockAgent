from __future__ import annotations

from datetime import date
import json

import pytest

from downloader.artifact_io import sha256_file
from scripts.audit_option_policy_product_coverage import audit, main


@pytest.fixture
def sources(tmp_path):
    table = tmp_path / 'daily.csv'
    header = '交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,收盤價,結算價,成交量,交易時段\n'
    rows = [
        '2026/01/02,AAA,202601,100,買權,3,3,3,2,一般\n',
        '2026/01/02,BBB,202601,100,賣權,-,-,3,0,一般\n',
        '2026/01/02,CCC,202601,100,買權,-,-,3,-,一般\n',
        '2026/01/03,AAA,202601,100,買權,4,4,4,1,一般\n',
    ]
    table.write_bytes((header + ''.join(rows)).encode('cp950'))
    evidence = tmp_path / 'evidence.json'
    evidence.write_text('{}')
    digest = sha256_file(evidence)
    policy = tmp_path / 'policy.json'
    policy.write_text(json.dumps({
        'contract': 'taifex_option_session_policy_candidate_v1', 'research_candidate_only': True,
        'products': {'AAA': {'day_minutes': [525, 825]}, 'BBB': {'day_minutes': [525, 825]}},
        'inventory_comparison_pins': {'inventory_path': str(evidence), 'inventory_sha256': digest,
                                      'receipt_path': str(evidence), 'receipt_sha256': digest},
        'current_catalog_comparison_pins': {'raw_path': str(evidence), 'raw_sha256': digest,
                                            'receipt_path': str(evidence), 'receipt_sha256': digest},
    }))
    plan = tmp_path / 'plan.json'
    plan.write_text(json.dumps({'contract': 'tw_option_all_product_daily_source_plan_v1', 'sources': [{
        'path': str(table), 'sha256': sha256_file(table), 'start': '2026-01-01', 'end': '2026-01-31'}]}))
    return plan, policy, table


def test_observed_unknown_products_and_missing_volume_stay_distinct(sources):
    plan, policy, _ = sources
    report = audit(plan, policy, date(2026, 1, 2), date(2026, 1, 3))
    products = {row['product']: row for row in report['products']}
    assert report['observed_products'] == 3
    assert report['positive_volume_products'] == 1
    assert report['unknown_observed_products'] == ['CCC']
    assert report['unknown_positive_volume_products'] == []
    assert report['invalid_volume_rows'] == 1
    assert products['AAA']['positive_volume_days'] == 2
    assert products['BBB']['invalid_volume_rows'] == 0
    assert products['CCC']['invalid_volume_rows'] == 1
    assert report['training_ready'] is False


def test_changed_source_rejected(sources):
    plan, policy, table = sources
    table.write_bytes(table.read_bytes() + b'\n')
    with pytest.raises(ValueError, match='digest mismatch'):
        audit(plan, policy, date(2026, 1, 2), date(2026, 1, 3))


def test_strict_cli_publishes_evidence_and_reports_unknown(sources, tmp_path, monkeypatch):
    plan, policy, _ = sources
    output = tmp_path / 'report.json'
    monkeypatch.setattr('sys.argv', ['audit', '--plan', str(plan), '--policy', str(policy),
                                   '--start', '2026-01-02', '--end', '2026-01-03',
                                   '--output', str(output), '--strict'])
    assert main() == 2
    assert json.loads(output.read_text())['unknown_observed_products'] == ['CCC']
    with pytest.raises(FileExistsError):
        main()
