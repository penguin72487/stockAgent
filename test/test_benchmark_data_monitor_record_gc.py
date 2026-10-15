"""Only temporary strict-validation Struct layout may differ in this trial."""

import ast
from datetime import date
import gc
import json
from pathlib import Path
import sys
from types import ModuleType

import pytest

from scripts import benchmark_data_monitor_record_codec as benchmark
from stockagent.live import data_monitor_record_codec as codec
from stockagent.live.data_monitor_inventory import InventorySnapshot, build_record_inventory


def payload(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    source = tmp_path / "data_yahoo/crypto/A_features.parquet"
    source.parent.mkdir(parents=True)
    pq.write_table(pa.table({"date": [date(2026, 10, 1)], "數值": [2**53 + 1]}), source)
    snapshot = InventorySnapshot(tmp_path)
    build_record_inventory(tmp_path, refresh=True, snapshot=snapshot)
    return snapshot.payload


def test_prototype_changes_only_the_four_gc_keywords():
    source = Path(codec.__file__).read_text()
    trees = [ast.parse(source), ast.parse(benchmark.struct_gc_source(source, tracked=False))]
    configurations = []
    for tree in trees:
        flags = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                flags[node.name] = next((ast.literal_eval(keyword.value) for keyword in node.keywords
                                         if keyword.arg == "gc"), True)
                node.keywords = [keyword for keyword in node.keywords if keyword.arg != "gc"]
        configurations.append(flags)
    assert ast.dump(trees[0]) == ast.dump(trees[1])
    assert configurations[1] == dict.fromkeys(benchmark.STRUCT_NAMES, False)


@pytest.mark.parametrize("value", [None, True, -0.0, float("inf"), 2**128, "台灣𝄞"])
def test_both_struct_layouts_preserve_strict_fallback_and_exact_bytes(tmp_path, value):
    pytest.importorskip("msgspec")
    document = payload(tmp_path)
    next(iter(document["files"].values()))["stats"]["non_null"][0] = value
    reference = codec.encode_record_cache(document)
    before = gc.isenabled()
    with benchmark.struct_gc_encoders() as encoders:
        for encoder in encoders.values():
            assert encoder(document) == reference
    assert gc.isenabled() == before


def test_modules_restored_on_exception_and_no_interpreter_gc_change(monkeypatch):
    pytest.importorskip("msgspec")
    name = "_stockagent_record_codec_tracked_native"
    previous = ModuleType(name)
    monkeypatch.setitem(sys.modules, name, previous)
    monkeypatch.setattr(gc, "disable", lambda: pytest.fail("global GC must not be disabled"))
    monkeypatch.setattr(gc, "enable", lambda: pytest.fail("global GC must not be changed"))
    with pytest.raises(RuntimeError, match="consumer failure"):
        with benchmark.struct_gc_encoders() as encoders:
            assert len(encoders) == 2
            raise RuntimeError("consumer failure")
    assert sys.modules[name] is previous
    assert "_stockagent_record_codec_untracked_native" not in sys.modules


def test_cyclic_and_unsupported_inputs_still_raise_original_error(tmp_path):
    pytest.importorskip("msgspec")
    document = payload(tmp_path)
    document["unknown"] = document
    with benchmark.struct_gc_encoders() as encoders:
        for encoder in encoders.values():
            with pytest.raises(ValueError, match="Circular"):
                encoder(document)
            with pytest.raises(TypeError):
                encoder({"unsupported": b"not a JSON string"})


def test_complete_record_workflow_bytes_source_checks_and_actual_layout(tmp_path):
    pytest.importorskip("msgspec")
    payload(tmp_path)
    status = tmp_path / "artifacts/live/data_monitor/public_status.json"
    status.write_text(json.dumps({"sources": []}))
    result = benchmark.benchmark(tmp_path, repeats=1, mode="struct_gc")
    assert result["state"] == "verified"
    assert result["cache_bytes_parity"] and result["parity"]
    assert result["interpreter_gc_unchanged"] and result["actual_struct_configs_verified"]
    assert result["all_source_identities_checked"]
    assert [row["variant"] for row in result["trials"]] == [
        "tracked_native", "untracked_native", "untracked_native", "tracked_native",
    ]
    assert {row["encoder_observation"]["temporary_struct_gc_tracking"]
            for row in result["trials"]} == {True, False}


def test_unknown_struct_shape_is_not_silently_benchmarked():
    source = Path(codec.__file__).read_text()
    with pytest.raises(ValueError, match="Struct shape changed"):
        benchmark.struct_gc_source(source.replace("class _FileEntry(", "class _Unexpected("), tracked=False)
