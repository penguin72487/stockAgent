"""Source-context clocks must invalidate a prepared projection, even on hits."""
import json

import pytest

from scripts.prepare_tw_day_trade_factorized_live import (
    _native_context_identity, _verify_preparation_inputs,
)
from stockagent.data.factorized_panel import file_sha256


def context(tmp_path):
    native = tmp_path / "native"
    native.mkdir()
    manifest = {"source_repairs": {"publication_lower_bounds": "bounds.parquet"}}
    (native / "source_manifest.json").write_text(json.dumps(manifest))
    (native / "bounds.parquet").write_bytes(b"frozen publication bound")
    config = tmp_path / "config.yaml"
    config.write_text("same selected configuration")
    identity = {"config_sha": file_sha256(config), "native_context": _native_context_identity(native, manifest)}
    return native, manifest, config, identity


@pytest.mark.parametrize("changed", ["configuration", "manifest", "bound", "new_uploads"])
def test_any_input_change_refuses_old_generation(tmp_path, changed):
    native, manifest, config, identity = context(tmp_path)
    assert identity["native_context"]["finlab/known_uploads.parquet"] is None
    _verify_preparation_inputs(config, native, manifest, identity)
    if changed == "configuration":
        config.write_text("changed selected configuration")
    elif changed == "manifest":
        (native / "source_manifest.json").write_text("{}")
    elif changed == "bound":
        (native / "bounds.parquet").write_bytes(b"corrected publication bound")
    else:
        (native / "finlab").mkdir()
        (native / "finlab/known_uploads.parquet").write_bytes(b"new actual upload")
    with pytest.raises(ValueError, match="context changed"):
        _verify_preparation_inputs(config, native, manifest, identity)


def test_publication_context_cannot_escape_original_root(tmp_path):
    native, manifest, _config, _identity = context(tmp_path)
    outside = tmp_path / "other.parquet"
    outside.write_bytes(b"not an accepted source input")
    manifest["source_repairs"]["publication_lower_bounds"] = "../other.parquet"
    with pytest.raises(ValueError, match="escaped"):
        _native_context_identity(native, manifest)
