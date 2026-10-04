"""Native captions may sanitize commas; observations must remain unchanged."""
from contextlib import closing
import json

import pytest

from downloader.tej_header_mapping import CONTRACT, validate_headers
from downloader.tej_history import connect, run_one, validate_export
from downloader.artifact_io import atomic_write_json
from scripts.audit_tej_history import audit
from test_tej_history import FakeBridge, full_grid_export, registry, request
from test_tej_snapshot import snapshot_export, snapshot_request


def mapped_export(snapshot=False):
    req=snapshot_request() if snapshot else request()
    doc=snapshot_export() if snapshot else full_grid_export()
    req={**req,'fields':['Assets, net','Property, plant']}
    keys=1 if snapshot else 2
    doc['fields']=req['fields']
    doc['cells'][0]=[*doc['cells'][0][:keys],*['Assets  net','Property  plant']]
    # Keep source samples independently populated with the same native header.
    doc['preview'][1][0][1]=doc['cells'][0][:]
    doc['preview_header_mapping_contract']=CONTRACT
    doc['selected_field_order_readback_verified']=True
    return req,doc


@pytest.mark.parametrize('snapshot',[False,True])
def test_caption_mapping_preserves_native_strings_samples_signed_values_and_zero(snapshot):
    req,doc=mapped_export(snapshot);original=json.loads(json.dumps(doc))
    canonical,rows,profile=validate_export(req,doc)
    assert canonical[-2:]==req['fields'] and doc==original
    assert doc['cells'][0][-2:]!=canonical[-2:] and rows==original['cells'][1:]
    assert rows[0][-1]=='-1.5' and any(row[-2]=='0' for row in rows)
    assert profile['preview_header_mapping_contract']==CONTRACT


@pytest.mark.parametrize('mutation',['no_contract','unknown_contract','no_order','other_capture','wrong_fields',
                                     'wrong_key','case','extra_space','other_punctuation','swapped','collision','bad_preview'])
def test_only_versioned_literal_injective_mapping_can_be_adopted(mutation):
    req,doc=mapped_export()
    if mutation=='no_contract':doc.pop('preview_header_mapping_contract')
    elif mutation=='unknown_contract':doc['preview_header_mapping_contract']='strip-all-punctuation'
    elif mutation=='no_order':doc['selected_field_order_readback_verified']=False
    elif mutation=='other_capture':doc['capture_method']='excel_value2'
    elif mutation=='wrong_fields':doc['fields']=['Foreign field','Property, plant']
    elif mutation=='wrong_key':doc['cells'][0][0]='Symbol'
    elif mutation=='case':doc['cells'][0][2]='assets  net'
    elif mutation=='extra_space':doc['cells'][0][2]='Assets   net'
    elif mutation=='other_punctuation':doc['cells'][0][2]='Assets_net'
    elif mutation=='swapped':doc['cells'][0][2:]=doc['cells'][0][2:][::-1]
    elif mutation=='collision':
        req['fields']=['Assets, net','Assets  net'];doc['fields']=req['fields']
        doc['cells'][0][2:]=['Assets  net','Assets  net']
    else:doc['preview'][1][0][1][2]='Foreign field'
    with pytest.raises(ValueError):validate_export(req,doc)


def test_exact_legacy_headers_still_need_no_new_proof():
    req=request();doc=full_grid_export()
    headers,contract=validate_headers(req,doc,['CO_ID','Date'])
    assert headers==doc['cells'][0] and contract is None


def test_writer_and_versioned_audit_preserve_header_contract_and_native_names(registry,tmp_path):
    import pyarrow.parquet as pq
    _,root,_,_=registry
    assert run_one(root,FakeBridge())=='completed_task'
    class Full(FakeBridge):
        def execute(self,root,task):
            payload={**full_grid_export(),'task_id':task['task_id'],
                     'preview_header_mapping_contract':CONTRACT,'selected_field_order_readback_verified':True}
            path=root/'raw'/(task['task_id']+'-full.json')
            atomic_write_json(path,payload);return payload,path,1
    assert run_one(root,Full())=='completed_task'
    with closing(connect(root)) as con:
        task=con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone()
    receipt=json.loads((root/task['receipt_path']).read_text())
    assert receipt['preview_header_mapping_contract']==CONTRACT
    assert receipt['native_source_headers']==full_grid_export()['cells'][0]
    assert pq.read_schema(root/receipt['parquet_path']).metadata[b'stockagent.preview_header_mapping_contract']==CONTRACT.encode()
    report=audit(root,tmp_path/'audit')
    assert report['accepted'] and report['contract']=='tej_local_receipt_integrity_audit_v4'
    assert report['preview_header_mapping_tasks_audited']==1
    # A report must not silently accept altered column-provenance metadata.
    receipt['native_source_headers'][-1]='Foreign field'
    atomic_write_json(root/task['receipt_path'],receipt)
    broken=audit(root,tmp_path/'broken-audit')
    assert broken['accepted'] is False and broken['local_artifact_failures']==1
