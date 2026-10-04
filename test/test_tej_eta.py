from datetime import UTC, datetime, timedelta
import copy

import pytest

from downloader.tej_eta import CONTRACT, build_staged_eta, query_geometry
from downloader.tej_key_layout import KEY1_CONTRACT, KEY3_CONTRACT
from downloader.tej_planning import build_plan

NOW = datetime(2026, 10, 2, 7, tzinfo=UTC)
CONFIG = {'max_rows_per_export':100, 'max_cells_per_export':2000,
          'max_companies_per_export':8, 'minimum_export_interval_seconds':2}


def table(identity='a', *, phase='P1', query_count=10, known=True, fields=20):
    return {'table_id':identity, 'phase':phase, 'field_phase_counts':{phase:fields},
            'fields':fields, 'frequency':'daily', 'query_type':'stocks', 'category':'prices',
            'source_key_mode':2, 'grid_rows':query_count*100 if known else None,
            'universe_count':20 if known else None, 'grid_dates':50 if known else None,
            'resolved_grid_rows':0, 'exported_rows':0, 'recorded_bytes':0,
            'remaining_download_queries':query_count if known else 0,
            'remaining_discovery_tasks':0 if known else 1, 'blocked_tasks':0}


def sample(identity='a', *, seconds=10, rows=0, expected=100, size=1000, kind='download'):
    return {'table_id':identity, 'frequency':'daily', 'kind':kind,
            'timing_basis':'fresh_end_to_end', 'seconds':seconds,
            'expected_rows':expected if kind=='download' else None,
            'actual_rows':rows, 'actual_bytes':size}


def forecast(tables, samples=None, alive=False):
    return build_staged_eta(tables, samples or [], CONFIG, observed=NOW,
                            cutoff='2026-10-01', alive=alive)


@pytest.mark.parametrize('keys', [1, 2, 3])
@pytest.mark.parametrize('companies', [1, 7, 8, 9, 35])
@pytest.mark.parametrize('fields', [1, 27, 28, 29, 55, 59])
@pytest.mark.parametrize('dates', [1, 7, 37])
@pytest.mark.parametrize('optimized', [False, True])
def test_closed_form_matches_canonical_lazy_planner(keys, companies, fields, dates, optimized):
    from downloader.tej_planning import TILING_CONTRACT
    config={**CONFIG, **({'query_tiling_contract':TILING_CONTRACT} if optimized else {})}
    request={'fields':[str(n) for n in range(fields)], 'max_rows':100, 'max_cells':2000}
    if keys != 2:
        request.update(source_key_mode=keys,key_layout_contract=KEY1_CONTRACT if keys==1 else KEY3_CONTRACT)
    periods=[] if keys==1 else [str(n) for n in range(dates)]
    plan=build_plan(request,[str(n) for n in range(companies)],periods,config,[])
    assert query_geometry(companies, len(periods), fields, keys, config)==(plan['total_queries'],plan['total_work_rows'])


def test_huge_workload_does_not_materialize_requests():
    queries, rows=query_geometry(100_000_000, 100_000, 2000, 2, CONFIG)
    assert queries > 10**12 and rows > 10**14


def test_empty_response_cost_is_service_work_not_downloaded_values():
    result=forecast([table()], [sample(seconds=10),sample(seconds=20)])
    assert result['global_scenarios']['middle']['remaining_seconds']==170
    assert result['global_scenarios']['middle']['remaining_export_rows']==0
    assert result['global_scenarios']['middle']['remaining_local_bytes']==10_000
    assert result['global_scenarios']['fast']['remaining_seconds'] < 170 < result['global_scenarios']['slow']['remaining_seconds']


def test_recovery_legacy_and_nonfinite_samples_cannot_speed_up_forecast():
    records=[sample(),{**sample(seconds=.001),'timing_basis':'recovery_or_unmeasured'},sample(seconds=float('nan'))]
    result=forecast([table()],records)
    assert result['global_scenarios']['middle']['remaining_seconds']==120
    assert result['download_timing_samples']==1


def test_missing_samples_and_unknown_axes_are_not_zero_or_complete():
    result=forecast([table(known=False)])
    assert result['global_scenarios']['middle']['remaining_seconds'] is None
    assert result['global_scenarios']['middle']['remaining_queries'] is None
    assert result['discovery_only_seconds']['middle'] is None
    assert result['scheduled_complete_at_utc'] is None


def test_known_and_modeled_axes_stay_distinct_and_discovery_counted_once():
    rows=[table(), table('b',known=False)]
    result=forecast(rows,[sample(),sample(kind='discover',seconds=20)])
    assert result['known_remaining_queries']==10
    assert result['modeled_axis_tables']==1 and result['discovery_remaining_tasks']==1
    q,_=query_geometry(20,50,20,2,CONFIG)
    assert result['global_scenarios']['middle']['remaining_queries']==10+q
    assert result['global_scenarios']['middle']['remaining_seconds']==(10+q)*12+22


def test_bundled_stage_depends_on_round_robin_owner_not_zero_or_extra_download():
    first=table(query_count=10)
    first['field_phase_counts']={'P1':10,'P2':5,'P3':5}
    longest=table('b',query_count=100)
    result=forecast([first,longest],[sample(),sample('b')])
    stages={p['phase']:p for p in result['phases']}
    assert result['global_scenarios']['middle']['remaining_queries']==110
    assert stages['P1']['scenarios']['middle']['dependency_remaining_seconds']==1320
    assert stages['P2']['scenarios']['middle']['dependency_remaining_seconds']==240
    assert stages['P2']['scenarios']['middle']['additional_queries']==0
    assert stages['P2']['bundled_into_earlier_tables']==1
    assert stages['P3']['scenarios']['middle']['dependency_remaining_seconds']==240
    assert stages['P3']['cross_source_validation_remaining_seconds'] is None
    assert stages['P3']['completion_scope']=='validation_candidates_only_not_cross_source_validation'


def test_owned_phases_are_serial_and_cumulative_not_parallel():
    rows=[table('a',phase='P1',query_count=10),table('b',phase='P2',query_count=20),table('c',phase='P3',query_count=30)]
    result=forecast(rows,[sample('a'),sample('b'),sample('c')])
    phases=result['phases']
    assert [p['scenarios']['middle']['dependency_remaining_seconds'] for p in phases]==[120,360,720]
    assert result['global_scenarios']['middle']['remaining_seconds']==720


@pytest.mark.parametrize('alive',[False,True])
def test_finite_batch_is_never_claimed_as_continuous_schedule(alive):
    result=forecast([table()],[sample()],alive=alive)
    assert result['scheduled_complete_at_utc'] is None
    assert result['phases'][0]['scheduled_complete_at_utc'] is None
    assert result['phases'][0]['scenarios']['middle']['if_started_now_complete_at_utc']==(NOW+timedelta(seconds=120)).isoformat()
    assert result['execution_state']==('finite_batch_running' if alive else 'not_running')


def test_unresolved_source_remains_blocked_and_no_quota_limit_is_invented():
    row=table(known=False);row['blocked_tasks']=1
    result=forecast([table(),row],[sample(),sample(kind='discover')])
    assert result['blocked_tasks']==1 and result['phases'][0]['blocked_dependency_tasks']==1
    assert result['official_quota_wait_seconds'] is None
    assert result['scheduled_complete_at_utc'] is None
    assert result['statistical_confidence_interval'] is False


def test_small_pilot_does_not_explode_per_row_bytes_or_set_bulk_rate():
    row=table(query_count=100);row['grid_rows']=1_000_000
    tiny=sample(seconds=100,expected=1,rows=1,size=10_000)
    bulk=sample('b',seconds=10,expected=10_000,rows=100,size=30_000)
    result=forecast([row],[tiny,tiny,bulk])
    predicted=result['table_forecasts'][0]
    assert predicted['timing_basis']=='same_frequency/similar_scope'
    assert predicted['remaining_seconds']['middle']==1200
    assert predicted['remaining_local_bytes']['middle']==3_000_000


def test_no_in_place_mutation_and_fingerprint_tracks_work_not_clock():
    rows=[table()];original=copy.deepcopy(rows)
    first=forecast(rows,[sample()]);second=forecast(rows,[sample()])
    assert first['contract']==CONTRACT and first['input_sha256']==second['input_sha256']
    assert rows==original
    rows[0]['remaining_download_queries']-=1
    assert forecast(rows,[sample()])['input_sha256']!=first['input_sha256']


def test_empty_fields_have_zero_acquisition_not_false_history_certification():
    row=table(fields=0);row['remaining_download_queries']=0
    result=forecast([row])
    assert result['global_scenarios']['middle']['remaining_seconds']==0
    assert result['phases'][0]['dependency_tables']==0
    assert result['table_forecasts'][0]['geometry_basis']=='empty_field_menu_excluded_not_certified'
