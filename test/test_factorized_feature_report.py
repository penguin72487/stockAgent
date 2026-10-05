from dataclasses import asdict
import json

import pytest

from stockagent.data.factorized_panel import file_sha256
from stockagent.data.tw_feature_semantic_report import write_factorized_feature_report
from stockagent.data.tw_public_release_schedule import RULES


@pytest.mark.parametrize("scope", [None,"stock","market"])
def test_native_scope_uses_registered_rule_and_rejects_conflicts(tmp_path,scope):
    feature={"feature":"native_assets","source":"MOPS","rule":asdict(RULES["quarter"])}
    if scope is not None:feature["scope"]=scope
    dictionary=tmp_path/"feature_dictionary.json"
    dictionary.write_text(json.dumps({"features":[feature],"aliases":[],"excluded":[]}))
    manifest=tmp_path/"factorized_manifest.json"
    manifest.write_text(json.dumps({"feature_dictionary_sha256":file_sha256(dictionary),
        "source_snapshot_id":"exact-fixture","dates":["2025-01-02","2025-01-03"],
        "symbols":["2330"],"individual_quantities":1,"shared_quantities":0,
        "logical_model_channels":4,"base_feature_names":[]}))
    before=file_sha256(dictionary)
    if scope=="market":
        with pytest.raises(ValueError,match="scope disagrees"):
            write_factorized_feature_report(manifest,tmp_path/"report")
    else:
        result=write_factorized_feature_report(manifest,tmp_path/"report")
        assert result["selected_quantities"]==1
        body=(tmp_path/"report/stock_0001.md").read_text()
        assert "## native_assets" in body and "## {}" not in body
    assert file_sha256(dictionary)==before # report repair never mutates data


def test_report_coverage_is_scoped_and_bound_to_receipt(tmp_path):
    feature={"feature":"native_assets","source":"MOPS","rule":asdict(RULES["quarter"])}
    dictionary=tmp_path/"feature_dictionary.json"
    dictionary.write_text(json.dumps({"features":[feature],"aliases":[],"excluded":[]}))
    manifest=tmp_path/"factorized_manifest.json"
    manifest.write_text(json.dumps({"feature_dictionary_sha256":file_sha256(dictionary),
        "source_snapshot_id":"exact-fixture","dates":["2025-01-02","2025-01-03"],
        "symbols":["2330"],"individual_quantities":1,"shared_quantities":0,
        "logical_model_channels":4,"base_feature_names":[]}))
    coverage=tmp_path/"feature_coverage.csv"
    coverage.write_text("feature,scope,source,available_panel_cells\nnative_assets,stock,MOPS,1\n")
    proof=write_factorized_feature_report(manifest,tmp_path/"report")
    assert proof["coverage_sha256"]==file_sha256(coverage)
    assert "1／2" in (tmp_path/"report/stock_0001.md").read_text()
