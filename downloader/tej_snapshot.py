"""Source Key=1 company snapshots. Never turn one current value into history."""
import math
from datetime import datetime

from downloader.tej_key_layout import KEY1_CONTRACT, SNAPSHOT_DATE_AXIS


def snapshot_profile(request: dict, rows: list[list]) -> dict:
    from downloader.tej_history import company_code
    symbols={company_code(label) for label in request['company_labels']}
    found=set();counts=[0]*len(request['fields'])
    for row in rows:
        if not isinstance(row[0],str) or not row[0].split():
            raise ValueError('Missing snapshot company key')
        code=row[0].split()[0]
        if code not in symbols or code in found:
            raise ValueError('Duplicate or out-of-scope snapshot company key')
        found.add(code)
        for index,value in enumerate(row[1:]):
            if isinstance(value,float) and not math.isfinite(value):
                raise ValueError('Non-finite snapshot source value')
            if value is not None and value!='':
                counts[index]+=1
    return {'keys':[(row[0].split()[0],None) for row in rows], 'non_null_counts':counts,
            'first':[None]*len(counts),'last':[None]*len(counts),'requested_query_rows':len(symbols),
            'omitted_query_grid_rows':len(symbols-found),'publication_verified':False,
            'native_observation_completeness_verified':False}


def validate_snapshot(request: dict, payload: dict):
    from downloader.tej_history import MAX_CELLS, validate_source_scope, validate_preview
    validate_source_scope(request,payload,'download')
    cells=payload.get('cells');headers=payload.get('source_key_headers')
    if (payload.get('contract_version')!=4 or payload.get('source_key_mode')!=1
            or payload.get('key_layout_contract')!=KEY1_CONTRACT
            or payload.get('date_axis')!=SNAPSHOT_DATE_AXIS or request.get('date_labels')!=[]
            or payload.get('capture_method')!='native_msaa_preview_full'
            or payload.get('source_value_representation')!='vendor_display_strings_not_underlying_excel_values'
            or payload.get('query_comments_read') is not False
            or not isinstance(cells,list) or len(cells)<2
            or not isinstance(headers,list) or len(headers)!=1
            or not isinstance(headers[0],str) or not headers[0] or len(headers[0])>128):
        raise ValueError('Exact source Key=1 snapshot capture required')
    from downloader.tej_header_mapping import validate_headers
    expected,header_contract=validate_headers(request,payload,headers)
    if (len(set(expected))!=len(expected)
            or any(not isinstance(row,list) or len(row)!=len(expected) for row in cells)
            or payload.get('source_grid_columns')!=len(expected) or payload.get('source_grid_rows')!=len(cells)-1
            or len(cells)-1>request['max_rows'] or len(cells)*len(expected)>min(MAX_CELLS,request['max_cells'])):
        raise ValueError('Snapshot schema/count/capacity mismatch')
    observed=datetime.fromisoformat(payload['observed_at_utc'].replace('Z','+00:00'))
    if observed.tzinfo is None:
        raise ValueError('Actual aware snapshot observation time required')
    profile=snapshot_profile(request,cells[1:])
    profile['preview_sample_rows_verified']=validate_preview(cells[0],cells,payload.get('preview'),date_system=None,keys_count=1)
    profile['preview_header_mapping_contract']=header_contract
    return expected,cells[1:],profile
