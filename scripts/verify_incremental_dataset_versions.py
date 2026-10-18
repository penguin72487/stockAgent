#!/usr/bin/env python3
"""Exercise the common all-root version contract in an isolated local fixture.

No provider API, production payload, service, current head or legacy dataset is
modified. Temporary complete materializations prove restoration, then disappear
with this fixture. The durable result is a small engineering receipt only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import time

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from stockagent.data_sync.dataset_version_layout import (  # noqa: E402
    publication_version_options, version_policy,
)
from stockagent.data_sync.desync_snapshots import atomic_write_json  # noqa: E402
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    fetch_packed_snapshot, publish_packed_snapshot, verify_packed_snapshot,
)


def _payload_bytes(cold: Path) -> int:
    return sum(path.stat().st_size for kind in ("blobs", "packs")
               for path in (cold / "objects" / kind).rglob("*") if path.is_file())


def verify(*, output: Path) -> dict:
    import duckdb
    import polars as pl
    import pyarrow as pa
    import pyarrow.parquet as pq

    policy = version_policy(REPO)
    implementation = ["scripts/verify_incremental_dataset_versions.py",
                      "stockagent/data_sync/packed_snapshots.py",
                      "stockagent/data_sync/packed_metadata.py",
                      "stockagent/data_sync/dataset_version_layout.py", "configs/columnar_storage.json"]
    def identities():
        return {name: hashlib.sha256((REPO / name).read_bytes()).hexdigest() for name in implementation}
    code_sha256 = identities()
    started = time.perf_counter()
    rows, reconstructed = [], 0
    with tempfile.TemporaryDirectory(prefix="stockagent-incremental-layout-") as scratch:
        fixture = Path(scratch)
        repo, cold, hot = fixture / "checkout", fixture / "cold", fixture / "hot"
        (repo / "configs").mkdir(parents=True)
        atomic_write_json(repo / "configs/columnar_storage.json",
                          json.loads((REPO / "configs/columnar_storage.json").read_text()))
        chunk_bytes = policy["file_chunk_bytes"]
        # Nonconstant deterministic chunks: equal content is shared physically;
        # provider identity and market values remain separate logical rows.
        a = hashlib.shake_256(b"first source block").digest(chunk_bytes)
        b = hashlib.shake_256(b"second source block").digest(chunk_bytes)
        tail = b"real appended observation bytes" * 7
        for name in policy["roots"]:
            source = repo / name
            source.mkdir()
            table = pa.table({"provider": [name] * 3, "symbol": ["0050", "2330", "2330"],
                "date": ["2020-03-02", "2020-03-02", "2020-03-03"],
                "value": [10.5, -0.75, None], "volume_shares": [1000, 17, None]})
            pq.write_table(table, source / "daily.parquet", compression="zstd")
            (source / "history.bin").write_bytes(a + b)
            entry = {"dataset": name, "source": name, "publish": False}
            options = publication_version_options(repo, entry, reader_contract="packed-v2-chunks")
            assert options == {"file_chunk_bytes": chunk_bytes}

            def seal():
                return publish_packed_snapshot(cold, name, source, node_id="fixture",
                    loose_file_threshold_bytes=1024, pack_buckets=2, **options)

            first = seal()
            before_noop = _payload_bytes(cold)
            noop = seal()
            assert noop.manifest_sha256 == first.manifest_sha256
            assert _payload_bytes(cold) == before_noop
            (source / "history.bin").write_bytes(a + b + tail)
            appended = seal()
            append_bytes = _payload_bytes(cold) - before_noop
            assert 0 <= append_bytes <= len(tail)  # Prior fixture providers may already own this exact chunk.
            (source / "history.bin").write_bytes(a + a + tail)
            revised = seal()
            assert _payload_bytes(cold) == before_noop + append_bytes
            (source / "daily.parquet").unlink()
            deleted = seal()
            for release, content, has_table in ((first, a + b, True),
                                                (appended, a + b + tail, True),
                                                (revised, a + a + tail, True),
                                                (deleted, a + a + tail, False)):
                expected = ["history.bin", *(["daily.parquet"] if has_table else [])]
                proof = verify_packed_snapshot(cold, release, reconstruct_paths=expected)
                assert proof["independently_reconstructed_files"] == len(expected)
                restored = fetch_packed_snapshot(cold, hot, release)
                assert hashlib.sha256((restored / "history.bin").read_bytes()).digest() == hashlib.sha256(content).digest()
                if has_table:
                    path = restored / "daily.parquet"
                    assert pq.read_table(path).equals(table)
                    assert pl.scan_parquet(path).select(pl.len()).collect().item() == 3
                    with duckdb.connect() as con:
                        assert con.execute("SELECT count(*), min(value), count(value) FROM read_parquet(?)",
                                           [str(path)]).fetchone() == (3, -0.75, 2)
                else:
                    assert not (restored / "daily.parquet").exists()
                reconstructed += 1
            rows.append({"root": name, "versions_reconstructed": 4, "noop_payload_bytes": 0,
                "append_payload_bytes": append_bytes, "revision_payload_bytes": 0,
                "full_file_bytes_after_append": len(a) + len(b) + len(tail),
                "source_provenance_merged": False, "raw_values_nulls_schema_and_leading_zero_preserved": True})
        result = {"state": "accepted", "mode": "isolated_engineering_fixture",
            "root_count": len(rows), "versions_reconstructed": reconstructed, "roots": rows,
            "payload_bytes_in_fixture_store": _payload_bytes(cold),
            "file_chunk_bytes": chunk_bytes, "elapsed_seconds": round(time.perf_counter() - started, 3),
            "network_calls": 0, "production_payload_reads": 0, "production_payload_writes": 0,
            "production_heads_changed": 0, "legacy_payload_deletions": 0,
            "production_migration_claim": False}
        result["metadata_inventory_bytes_in_fixture_store"] = sum(
            path.stat().st_size for path in (cold / "objects/inventories").rglob("*.jsonl.gz"))
        result["code_sha256"] = code_sha256
        if identities() != code_sha256:
            raise RuntimeError("implementation changed during roundtrip; rerun under one code identity")
    atomic_write_json(output, result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    result = verify(output=args.output)
    print(json.dumps({k: v for k, v in result.items() if k != "roots"}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
