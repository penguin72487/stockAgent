"""Independently reconcile a complete spectrum report and both lookup tables.

No model fitting, optimizer, provider requests or data rewriting occurs here.
NumPy recomputes sums/thresholds independently of the Torch spectrum writer.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0,str(ROOT))


def _verify_inverse_markdown(report: str, thresholds: list[dict]) -> None:
    """Reconcile the rendered inverse table as well as the machine CSV."""
    rendered=[]
    for line in report.splitlines():
        if not line.startswith('| '):
            continue
        cells=[value.strip() for value in line.split('|')[1:-1]]
        if len(cells)!=4:
            continue
        try:
            percent=float(cells[0])
        except ValueError:
            continue
        rendered.append((percent,int(cells[1].replace(',','')),float(cells[2]),cells[3]))
    assert len(rendered)==len(thresholds), 'Markdown threshold row inventory differs'
    for actual,expected in zip(rendered,thresholds):
        assert actual[0]==float(expected['target_percent']), 'Markdown target/order differs'
        assert actual[1]==int(expected['dimensions']), 'Markdown dimension differs'
        assert abs(actual[2]-float(expected['retained_percent']))<6e-11, 'Markdown energy differs'
        meaning=('完整 active、不截斷上界' if expected['meaning']=='untruncated_active_coordinate_guarantee'
                 else '最少維度')
        assert actual[3]==meaning, 'Markdown 100-percent/minimum semantics differ'


def verify(root: Path) -> dict:
    import numpy as np
    proof=json.loads((root/'full-feature-spectrum.json').read_text())
    metadata=proof['feature_spectrum']
    assert proof['state']=='accepted_full_training_feature_spectrum_analysis_only'
    assert proof['selected_model_decomposition']=='none'
    assert not proof['optimizer_started'] and not proof['formal_training_started']
    assert proof['ddp_world_size']==metadata['distributed_world_size']==2
    assert metadata['analysis_only'] and not metadata['model_projection_fitted']
    assert metadata['validation_rows_used']==metadata['test_rows_used']==0
    assert metadata['all_training_cells_used'] and not metadata['spectrum_is_truncated']
    assert metadata['gram_dtype']==metadata['eigensolver_dtype']=='float64'
    assert metadata['alive_training_cells']==proof['rms_fit_metadata']['alive_cell_count']
    assert metadata['active_features']==proof['rms_fit_metadata']['active_feature_count']
    assert proof['train_years']==proof['rms_fit_metadata']['train_years']
    with (root/'dimensions-to-energy.csv').open(newline='') as source:
        rows=list(csv.DictReader(source))
    width=metadata['features']
    assert [int(row['dimensions']) for row in rows]==list(range(width+1))
    squared=np.array([float(row['squared_singular_value']) for row in rows[1:]])
    assert squared.shape==(width,) and np.isfinite(squared).all() and (squared>=0).all()
    assert np.all(np.diff(squared)<=0)
    np.testing.assert_array_equal(squared,np.array(metadata['squared_singular_values']))
    energy=float(metadata['total_squared_energy'])
    assert abs(float(squared.sum())-energy)/energy<1e-10
    # In a train-only uncentered RMS view, each active channel has unit second
    # moment within FP32 scaling/rounding. This denominator check is independent
    # of the covariance/eigenvalue computation and detects lost/duplicated cells.
    rms_expected=metadata['alive_training_cells']*metadata['active_features']
    normalization_trace_error=abs(energy-rms_expected)/rms_expected
    assert normalization_trace_error<2e-6
    cumulative=np.minimum(1.,np.cumsum(squared,dtype=np.float64)/energy)
    np.testing.assert_allclose(cumulative,metadata['cumulative_squared_energy_ratio'],rtol=0,atol=2e-14)
    np.testing.assert_allclose([float(row['singular_value']) for row in rows[1:]],np.sqrt(squared),rtol=1e-14)
    np.testing.assert_allclose([float(row['direction_percent']) for row in rows[1:]],squared/energy*100,rtol=1e-14)
    np.testing.assert_allclose([float(row['cumulative_percent']) for row in rows[1:]],cumulative*100,rtol=0,atol=2e-12)
    with (root/'percent-to-dimensions.csv').open(newline='') as source:
        thresholds=list(csv.DictReader(source))
    assert len(thresholds)==104
    assert {float(row['target_percent']) for row in thresholds}==set(range(101))|{99.9,99.99,99.999}
    for row in thresholds:
        percent=float(row['target_percent']);dimensions=int(row['dimensions'])
        if percent==0:
            assert dimensions==0 and float(row['retained_percent'])==0
        elif percent==100:
            assert dimensions==metadata['active_features']
            assert row['meaning']=='untruncated_active_coordinate_guarantee'
            assert abs(float(row['retained_percent'])-100)<1e-8
        else:
            assert dimensions==int(np.searchsorted(cumulative,percent/100))+1
            assert cumulative[dimensions-1]>=percent/100
            assert dimensions==1 or cumulative[dimensions-2]<percent/100
            assert row['meaning']=='minimum_dimensions'
    detail=(root/'dimensions-to-energy.md').read_text()
    detail_rows=[row for row in detail.splitlines() if row.startswith('| ') and row.split('|')[1].strip().isdigit()]
    assert len(detail_rows)==width+1
    assert [int(row.split('|')[1]) for row in detail_rows]==list(range(width+1))
    for row,ratio in zip(detail_rows[1:],cumulative):
        assert abs(float(row.split('|')[3])-ratio*100)<6e-11
    report=(root/'feature-spectrum-report.md').read_text()
    assert '## 1. 執行進度' in report and '100%' in report
    assert '不是宣稱它是精確最小代數秩' in report
    _verify_inverse_markdown(report,thresholds)
    for name in ('dimensions-to-energy.md','dimensions-to-energy.csv',
                 'percent-to-dimensions.csv','full-feature-spectrum.json'):
        assert f']({name})' in report and (root/name).is_file()
    names=('full-feature-spectrum.json','dimensions-to-energy.csv','dimensions-to-energy.md',
           'percent-to-dimensions.csv','feature-spectrum-report.md')
    return {'state':'accepted_independently_reconciled_full_spectrum_report',
            'observed_at_utc':datetime.now(timezone.utc).isoformat(),
            'feature_directions':width,'dimension_rows_with_zero':len(rows),
            'threshold_rows':len(thresholds),'rms_expected_energy':rms_expected,
            'rms_normalization_trace_relative_error':normalization_trace_error,
            'no_projection_or_training_started':True,
            'numerical_rank_not_exact_algebraic_rank':True,
            'checks':['complete direction inventory','complete 0–100 and tail thresholds',
                      'all individual and cumulative energies','numpy minimum-dimension lookup',
                      'independent RMS energy identity','all rendered inverse thresholds and definitions',
                      'full Markdown tables and relative links'],
            'files':{name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in names}}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=Path)
    parser.add_argument('--receipt',type=Path,required=True)
    args=parser.parse_args()
    result=verify(args.root)
    from downloader.artifact_io import atomic_write_json
    atomic_write_json(args.receipt,result)
    print(json.dumps({key:result[key] for key in ('state','feature_directions','threshold_rows',
          'rms_normalization_trace_relative_error')}),flush=True)


if __name__=='__main__':
    main()
