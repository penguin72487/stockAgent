#!/usr/bin/env python3
"""Verify and promote an isolated MOF original archive into the live producer.

This is for a backfill made while another canonical source writer held the
producer lock. The staging index must still match the live index exactly.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from downloader.download_tw_mof_release_archive import NAME, _verified_previous
from downloader.release_archive_io import write_release_rows_if_changed
from downloader.tw_public_source_lock import source_update_lock


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _copy_verified(source: Path, destination: Path, digest: str) -> None:
    if _sha256(source) != digest:
        raise ValueError(f"staged MOF checksum mismatch: {source}")
    if destination.exists():
        if _sha256(destination) != digest:
            raise ValueError(f"live MOF checksum conflict: {destination}")
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix=f".{destination.name}.", suffix=".tmp",
                                 dir=destination.parent, delete=False) as handle:
        temporary = Path(handle.name)
    try:
        with source.open("rb") as reader, temporary.open("wb") as writer:
            shutil.copyfileobj(reader, writer, length=1024 * 1024)
        if _sha256(temporary) != digest:
            raise ValueError(f"MOF copy checksum mismatch: {temporary}")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def promote(stage: Path, live: Path) -> dict[str, object]:
    stage, live = stage.resolve(), live.resolve()
    if stage == live or stage in live.parents or live in stage.parents:
        raise ValueError("staging and live roots must be separate")
    index_name = "supplemental/mof_macro_release_dates.parquet"
    with source_update_lock(live):
        stage_index = pl.read_parquet(stage / index_name).sort("series", "period")
        live_index = pl.read_parquet(live / index_name).sort("series", "period")
        if not stage_index.equals(live_index):
            raise ValueError("live MOF index changed since staging; refresh the stage first")
        source_path = stage / "supplemental" / f"{NAME}.parquet"
        state = json.loads((stage / "state" / f"{NAME}.json").read_text(encoding="utf-8"))
        if state.get("status") != "complete" or not source_path.is_file():
            raise ValueError("staged MOF archive is incomplete")
        if _sha256(source_path) != state.get("parquet_sha256"):
            raise ValueError("staged MOF receipt checksum mismatch")
        rows = pl.read_parquet(source_path).to_dicts()
        identities = {(row["series"], row["period"]) for row in rows}
        expected = {(row["series"], row["period"]) for row in stage_index.to_dicts()}
        if len(rows) != len(expected) or identities != expected:
            raise ValueError("staged MOF archive differs from its source index")
        translated = []
        for row in rows:
            if not _verified_previous(row):
                raise ValueError(f"staged MOF original is incomplete: {row['series']}/{row['period']}")
            current = dict(row)
            for path_column, hash_column in (("pdf_raw_path", "pdf_sha256"),
                                             ("detail_raw_path", "detail_sha256"),
                                             ("body_raw_path", "body_sha256")):
                raw = row.get(path_column)
                if raw is None:
                    continue
                source = Path(str(raw)).resolve()
                raw_root = stage / "raw" / NAME
                if source != raw_root and raw_root not in source.parents:
                    raise ValueError(f"MOF staged path outside archive: {source}")
                destination = live / source.relative_to(stage)
                _copy_verified(source, destination, str(row[hash_column]))
                current[path_column] = str(destination)
            translated.append(current)
        output = live / "supplemental" / f"{NAME}.parquet"
        digest, changed = write_release_rows_if_changed(
            output, translated, identity_columns=("series", "period")
        )
        receipt = dict(state)
        receipt.update({
            "parquet_path": str(output), "parquet_sha256": digest,
            "parquet_changed": changed,
            "promoted_from_stage": str(stage),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        })
        state_path = live / "state" / f"{NAME}.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8",
                                     prefix=f".{NAME}.", suffix=".tmp",
                                     dir=state_path.parent, delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(receipt, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        try:
            os.replace(temporary, state_path)
        finally:
            temporary.unlink(missing_ok=True)
        return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", type=Path, required=True)
    parser.add_argument("--live", type=Path, required=True)
    args = parser.parse_args()
    receipt = promote(args.stage, args.live)
    print(json.dumps(receipt, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
