"""Source-context clocks must invalidate a prepared projection, even on hits."""
import json

import pytest

from scripts.prepare_tw_day_trade_factorized_live import (
    _fixed_seed_event_paths, _native_context_identity, _verify_preparation_inputs,
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


@pytest.mark.parametrize("change", ["quote", "same_size_revision", "new_symbol"])
def test_base_quote_bytes_and_membership_invalidate_complete_projection_hits(tmp_path, change):
    from scripts.prepare_tw_day_trade_factorized_live import _base_source_identity
    native, manifest, config, identity = context(tmp_path)
    public = tmp_path / "public"
    stocks = public / "stocks"
    stocks.mkdir(parents=True)
    quote = stocks / "2330_features.parquet"
    # The identity owner hashes bytes, never interprets them as prices.
    quote.write_bytes(b"original-quote-values")
    identity["base_source_sha"] = _base_source_identity(public)
    _verify_preparation_inputs(config, native, manifest, identity, public=public)
    if change == "new_symbol":
        (stocks / "2317_features.parquet").write_bytes(b"new-symbol")
    else:
        before = quote.stat()
        quote.write_bytes(b"revision-quote-values" if change == "same_size_revision" else b"corrected-price")
        if change == "same_size_revision":
            import os
            os.utime(quote, ns=(before.st_atime_ns, before.st_mtime_ns))
    with pytest.raises(ValueError, match="context changed"):
        _verify_preparation_inputs(config, native, manifest, identity, public=public)


def test_reuse_implementation_is_fenced_before_publication_including_cache_hits(tmp_path, monkeypatch):
    from scripts import prepare_tw_day_trade_factorized_live as owner
    native, manifest, config, identity = context(tmp_path)
    identity["storage_implementation"] = owner.storage_implementation_identity()
    _verify_preparation_inputs(config, native, manifest, identity)
    monkeypatch.setattr(owner, "storage_implementation_identity", lambda: {"changed": "dependency"})
    with pytest.raises(ValueError, match="context changed"):
        _verify_preparation_inputs(config, native, manifest, identity)


@pytest.mark.parametrize("layout", ["annual_events", "flattened"])
def test_fixed_seed_supports_original_and_verified_bounded_layouts(tmp_path, layout):
    root = tmp_path / "parent"
    root.mkdir()
    member = root / "annual_events/2013.parquet" if layout == "annual_events" else root / "2013.parquet"
    member.parent.mkdir(exist_ok=True)
    member.write_bytes(b"unchanged original seed")
    manifest = {"annual_events": [{"path": "annual_events/2013.parquet", "sha256": file_sha256(member)}]}
    assert _fixed_seed_event_paths(root / "manifest.json", manifest, {2013}) == [member]


@pytest.mark.parametrize("fault", ["missing", "changed", "duplicate", "escaping", "incomplete"])
def test_fixed_seed_fails_closed_without_altering_sources(tmp_path, fault):
    root = tmp_path / "parent"
    root.mkdir()
    member = root / "2013.parquet"
    member.write_bytes(b"original source")
    proof = {"path": "annual_events/2013.parquet", "sha256": file_sha256(member)}
    manifest = {"annual_events": [proof]}
    years = {2013}
    if fault == "missing":
        member.unlink()
    elif fault == "changed":
        member.write_bytes(b"different source")
    elif fault == "duplicate":
        manifest["annual_events"].append(dict(proof))
    elif fault == "escaping":
        proof["path"] = "../2013.parquet"
    else:
        years.add(2014)
    with pytest.raises(ValueError):
        _fixed_seed_event_paths(root / "manifest.json", manifest, years)
