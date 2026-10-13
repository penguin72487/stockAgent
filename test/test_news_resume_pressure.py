"""Recovery must use the same completed pressure scope as capacity health."""
from copy import deepcopy

import pytest

from downloader.news_fulltext_capacity import DEFAULTS, recent_transport_resume


def evidence():
    sample = {'interval_seconds':.2,'parallelism':7,'new_heads_per_second':3,
        'comparable_new_heads_per_second':3,'healthy':True,'workload_family':'article_html',
        'requests':42,'responses':42,'new_stored_url_heads':35,'durable_body_versions':35,
        'transport_or_server_errors':2,'isolated_gateway_host_errors':1,'isolated_robots_origin_errors':1,
        'provisional_gateway_host_errors':0,'pressure_transport_or_server_errors':0,
        'pressure_request_results':40,'pressure_response_results':40,
        'unusable_http_responses':0,'wall_seconds':45,'http_status_counts':{'200':40,'502':2}}
    return {'interval_seconds':.5,'parallelism':4,
        'history':[{**deepcopy(sample),'ended_at_epoch':1000-45*i} for i in range(3)]}


def select(item):
    return recent_transport_resume(item,DEFAULTS,workload='article_html',maximum=8,floor=0,now=1000)


def test_resume_keeps_verified_isolated_failures_without_reclassifying_them_as_source_pressure():
    item = evidence()
    original = deepcopy(item)
    result = select(item)
    assert result is not None
    candidate, proof = result
    assert (candidate['interval_seconds'],candidate['parallelism']) == (.2,7)
    assert candidate['transport_or_server_errors']==2 and candidate['pressure_transport_or_server_errors']==0
    assert proof['recent_healthy_windows_at_exact_capacity']==3
    assert proof['contract_version']==3 and proof['isolated_events_retained_as_failures']
    assert item == original


@pytest.mark.parametrize('change', ['missing_denominator','partial_projection','boolean','negative',
    'wrong_errors','wrong_requests','wrong_responses','unproven_isolation','provisional',
    'too_few_pressure_results','pressure_budget','refusal','unhealthy'])
def test_resume_rejects_incomplete_or_contradictory_pressure_evidence(change):
    item = evidence()
    for sample in item['history']:
        if change=='missing_denominator':
            for key in ('pressure_transport_or_server_errors','pressure_request_results','pressure_response_results'):
                del sample[key]
        elif change=='partial_projection':
            del sample['pressure_response_results']
        elif change=='boolean':
            sample['isolated_robots_origin_errors']=True
        elif change=='negative':
            sample['pressure_transport_or_server_errors']=-1
        elif change=='wrong_errors':
            sample['pressure_transport_or_server_errors']=1
        elif change=='wrong_requests':
            sample['pressure_request_results']=42
        elif change=='wrong_responses':
            sample['pressure_response_results']=42
        elif change=='unproven_isolation':
            sample['isolated_robots_origin_errors']=0
        elif change=='provisional':
            sample['provisional_gateway_host_errors']=1
        elif change=='too_few_pressure_results':
            sample.update(requests=21,responses=21,pressure_request_results=19,pressure_response_results=19)
        elif change=='pressure_budget':
            sample.update(transport_or_server_errors=3,responses=41,
                pressure_transport_or_server_errors=1,pressure_response_results=39)
        elif change=='refusal':
            sample['http_status_counts']['429']=1
        else:
            sample['healthy']=False
    assert select(item) is None


def test_one_percent_budget_uses_the_same_nonisolated_denominator_as_live_health():
    item = evidence()
    for sample in item['history']:
        sample.update(requests=102,responses=101,transport_or_server_errors=3,
            pressure_request_results=100,pressure_response_results=99,pressure_transport_or_server_errors=1,
            http_status_counts={'200':99,'502':2})
    assert select(item) is not None
    item['history'][0].update(requests=101,pressure_request_results=99)
    assert select(item) is None  # 1/99 is above 1%; 1/101 must not dilute it.


def test_legacy_windows_without_any_isolation_keep_the_original_raw_budget():
    item = evidence()
    for sample in item['history']:
        for key in ('pressure_transport_or_server_errors','pressure_request_results','pressure_response_results',
                    'isolated_gateway_host_errors','isolated_robots_origin_errors','provisional_gateway_host_errors'):
            del sample[key]
        sample.update(requests=100,responses=99,transport_or_server_errors=1)
    assert select(item) is not None
    item['history'][0]['requests']=99
    assert select(item) is None
