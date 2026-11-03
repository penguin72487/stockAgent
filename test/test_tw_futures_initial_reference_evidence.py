from datetime import date
from decimal import Decimal
import csv
import zipfile

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.audit_tw_futures_initial_reference_evidence import audit, candidate


def test_candidate_requires_finite_positive_operands():
    assert candidate('33.65', '0.10') == Decimal('33.55')
    assert candidate('-', '1') is None
    assert candidate('10', 'NaN') is None
    assert candidate('10', '10') is None


def fixture_inputs(tmp_path):
    refs = tmp_path / 'references.parquet'
    pl.DataFrame({'date': [date(2025, 2, 20), date(2025, 2, 20)],
        'product': ['CAF', 'CBF'], 'contract': ['202504', '202504'],
        'opening_reference_quote': [33.55, 30.0]}).write_parquet(refs)
    omissions = tmp_path / 'omissions.csv'
    pl.DataFrame({'date': [date(2025, 2, 20), date(2025, 2, 20)],
        'product': ['CAF', 'CCF'], 'contract': ['202504', '202504'],
        'materialized_warmup': [True, True], 'outright_volume': [3, 1]}).write_csv(omissions)
    source = tmp_path / 'official.csv'
    with source.open('w', encoding='big5', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['交易日期', '契約', '到期月份(週別)', '收盤價', '漲跌價', '交易時段'])
        writer.writerow(['2025/02/20', 'CAF', '202504', '33.65', '0.10', '一般'])
        writer.writerow(['2025/02/20', 'CBF', '202504', '32', '1', '一般'])
        writer.writerow(['2025/02/20', 'CCF', '202504', '-', '-', '一般'])
        writer.writerow(['2025/02/20', 'CAF', '202504', '1', '1', '盤後'])
    return refs, omissions, source


def test_audit_retains_mismatch_and_never_admits_reconstructed_values(tmp_path):
    refs, omissions, source = fixture_inputs(tmp_path)
    result = audit(references=refs, reference_sha=sha256_file(refs), omissions=omissions,
        sources=[source], start=date(2025, 2, 20), end=date(2025, 2, 20),
        data_output=tmp_path / 'out.parquet', report_output=tmp_path / 'out.json')
    assert result['native_pairs_compared'] == 2
    assert result['native_pairs_exactly_equal'] == 1
    assert result['native_pairs_different'] == 1
    assert result['omitted_warmup_with_numeric_derived_value'] == 1
    assert result['daily_source_available_at_decision_time'] is False
    assert result['training_or_financial_admitted'] is False
    frame = pl.read_parquet(tmp_path / 'out.parquet')
    assert frame.filter(pl.col('product') == 'CBF')['exact_reference_match'].item() is False
    assert frame.filter(pl.col('product') == 'CCF')['derived_reference'].item() is None


def test_multiple_source_records_do_not_select_one_arbitrarily(tmp_path):
    refs, omissions, source = fixture_inputs(tmp_path)
    other = tmp_path / 'duplicate.csv'
    other.write_bytes(source.read_bytes())
    with pytest.raises(ValueError, match='multiple original'):
        audit(references=refs, reference_sha=sha256_file(refs), omissions=omissions,
            sources=[source, other], start=date(2025, 2, 20), end=date(2025, 2, 20),
            data_output=tmp_path / 'out.parquet', report_output=tmp_path / 'out.json')


def test_archive_requires_single_source_member(tmp_path):
    from scripts.audit_tw_futures_initial_reference_evidence import source_rows
    archive = tmp_path / 'ambiguous.zip'
    with zipfile.ZipFile(archive, 'w') as z:
        z.writestr('one.csv', 'a')
        z.writestr('two.csv', 'b')
    with pytest.raises(ValueError, match='one CSV member'):
        list(source_rows(archive))
