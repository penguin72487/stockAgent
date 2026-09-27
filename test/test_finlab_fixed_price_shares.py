import hashlib
import json

import pandas as pd

from scripts.download_finlab_history import safe_stem
from scripts.finlab_fixed_price_shares import FIELDS, load_fixed_price_shares


def test_fixed_price_lots_need_exact_price_amount_proof(tmp_path):
    source = {
        "raw_lots": [15, 2, 2, 0, None],
        "price": [2475, 50, 50, 50, 50],
        "amount": [37_125_000, 1000, 999, 0, 100],
    }
    (tmp_path / "datasets").mkdir()
    (tmp_path / "receipts").mkdir()
    for field, key in FIELDS.items():
        path = tmp_path / "datasets" / f"{safe_stem(key)}.parquet"
        pd.DataFrame({"source_index": [f"2026-09-{n:02d}" for n in range(1, 6)],
                      "2330": source[field]}).to_parquet(path, index=False)
        receipt = {
            "dataset": key, "status": "downloaded_unverified_for_pit",
            "parquet_path": str(path.relative_to(tmp_path)),
            "parquet_size_bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "rows": 5,
        }
        (tmp_path / "receipts" / f"{safe_stem(key)}.json").write_text(json.dumps(receipt))
    frame = load_fixed_price_shares(tmp_path, "2330")
    assert frame["volume_shares"].tolist()[:2] == [15_000, 20]
    assert pd.isna(frame.loc[2, "volume_shares"])
    assert frame.loc[3, "volume_shares"] == 0
    assert pd.isna(frame.loc[4, "volume_shares"])
    assert frame["unit_multiplier"].tolist()[:2] == [1000, 10]
