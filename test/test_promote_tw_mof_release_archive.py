from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil

import polars as pl
import pytest

from scripts.promote_tw_mof_release_archive import promote


def _fixture(stage: Path, live: Path) -> Path:
    for root in (stage, live):
        (root / "supplemental").mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"series": ["tax"], "period": ["2020-01"],
                      "published_on": ["2020-02-10"],
                      "release_url": ["https://www.mof.gov.tw/download/one"]}).write_parquet(
            root / "supplemental/mof_macro_release_dates.parquet"
        )
    original = stage / "raw/mof_original_release_archive/tax/2020-01/original-a.pdf"
    original.parent.mkdir(parents=True)
    original.write_bytes(b"%PDF-1.4 source bytes")
    digest = hashlib.sha256(original.read_bytes()).hexdigest()
    staged = stage / "supplemental/mof_original_release_archive.parquet"
    pl.DataFrame([{
        "series": "tax", "period": "2020-01", "published_on": "2020-02-10",
        "release_url": "https://www.mof.gov.tw/download/one",
        "pdf_raw_path": str(original), "pdf_sha256": digest,
        "pdf_bytes": original.stat().st_size, "detail_raw_path": None,
    }]).write_parquet(staged)
    (stage / "state").mkdir(exist_ok=True)
    (stage / "state/mof_original_release_archive.json").write_text(json.dumps({
        "status": "complete", "parquet_sha256": hashlib.sha256(staged.read_bytes()).hexdigest(),
        "indexed_releases": 1, "archived_releases": 1,
    }))
    return original


def test_promote_preserves_exact_bytes_and_blocks_stale_or_tampered_stage(tmp_path: Path) -> None:
    stage, live = tmp_path / "stage", tmp_path / "live"
    original = _fixture(stage, live)
    pl.DataFrame({"series": ["tax"], "period": ["2020-02"],
                  "published_on": ["2020-03-10"],
                  "release_url": ["https://www.mof.gov.tw/download/two"]}).write_parquet(
        live / "supplemental/mof_macro_release_dates.parquet"
    )
    with pytest.raises(ValueError, match="index changed"):
        promote(stage, live)
    shutil.copy2(stage / "supplemental/mof_macro_release_dates.parquet",
                 live / "supplemental/mof_macro_release_dates.parquet")
    receipt = promote(stage, live)
    assert receipt["status"] == "complete"
    rows = pl.read_parquet(live / "supplemental/mof_original_release_archive.parquet").to_dicts()
    assert Path(rows[0]["pdf_raw_path"]).read_bytes() == original.read_bytes()
    original.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="incomplete"):
        promote(stage, live)
