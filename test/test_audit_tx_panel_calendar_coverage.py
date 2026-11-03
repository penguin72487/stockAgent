from datetime import date
import json

import numpy as np
import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.audit_tx_panel_calendar_coverage import audit
from stockagent.data.panel_cache import array_content_fingerprint


def _inputs(tmp_path, keep=(0, 1, 2)):
    dates = np.asarray(["2012-02-03", "2012-02-04", "2012-02-06"], dtype="datetime64[ns]")
    panel = tmp_path / "panel"
    panel.mkdir()
    selected = dates[list(keep)]
    np.save(panel / "dates.npy", selected, allow_pickle=False)
    (panel / "meta.json").write_text(json.dumps({"arrays": {"dates": {
        "file": "dates.npy", "content_fingerprint": array_content_fingerprint(selected),
    }}}))
    ref = tmp_path / "tx.parquet"
    pl.DataFrame({
        "date": [date(2012, 2, 3), date(2012, 2, 4), date(2012, 2, 6)],
        "product": ["TX"] * 3, "contract": ["201202"] * 3,
        "tenor_rank": [1] * 3, "close": [100.0, 101.0, 102.0],
        "source_row_observed": [True] * 3,
    }).write_parquet(ref)
    (tmp_path / "manifest.json").write_text(json.dumps({"status": "complete", "outputs": {
        "continuous_daily": {"sha256": sha256_file(ref)},
    }}))
    return panel / "meta.json", ref


def test_audit_checks_weekend_and_detects_an_omitted_session(tmp_path):
    meta, ref = _inputs(tmp_path)
    result = audit(meta, ref, "2012-02-03", "2012-02-06")
    assert result["panel_sessions"] == result["tx_front_observed_sessions"] == 3
    assert result["weekend_sessions"] == ["2012-02-04"]
    assert result["omission_detection_checked"] == "2012-02-04"
    assert result["execution_or_training_admitted"] is False


@pytest.mark.parametrize("keep", [(1, 2), (0, 2), (0, 1)])
def test_audit_rejects_missing_first_interior_or_last_session(tmp_path, keep):
    meta, ref = _inputs(tmp_path, keep)
    with pytest.raises(ValueError, match="market dates differ"):
        audit(meta, ref, "2012-02-03", "2012-02-06")


def test_audit_rejects_panel_fingerprint_changes(tmp_path):
    meta, ref = _inputs(tmp_path)
    np.save(meta.parent / "dates.npy", np.array(["2012-02-03"], dtype="datetime64[ns]"))
    with pytest.raises(ValueError, match="cache fingerprint"):
        audit(meta, ref, "2012-02-03", "2012-02-06")
