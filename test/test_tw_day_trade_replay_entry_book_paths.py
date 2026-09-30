from datetime import date
import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

from scripts.rebuild_tw_day_trade_minute_curves import _replay_opening_valuation_prices


DAY = "2026-02-25"


def make_replay(root):
    path = root / "replay_entry_books" / f"{DAY}.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": ["2330", "0050"], "valuation_price_0901": [101.5, None]}).write_parquet(path)
    receipt = {"sessions": [{"session_date": DAY, "historical_entry_books": {
        "path": str(path.resolve()), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }}]}
    write_receipt(root, receipt)
    return path, receipt


def write_receipt(root, receipt):
    (root / "rebuild_receipt.json").write_text(json.dumps(receipt))


def read_replay(root):
    return _replay_opening_valuation_prices(root, start=date.fromisoformat(DAY), end=date.fromisoformat(DAY))


def test_atomic_directory_move_retains_exact_pinned_prices_and_receipt(tmp_path):
    root = tmp_path / "candidate"
    make_replay(root)
    before = (root / "rebuild_receipt.json").read_bytes()
    expected = read_replay(root)
    live = tmp_path / "live"
    root.rename(live)
    assert read_replay(live) == expected == {DAY: {"2330": 101.5}}
    assert (live / "rebuild_receipt.json").read_bytes() == before


def test_retained_copy_uses_canonical_book_even_when_original_location_changed(tmp_path):
    root = tmp_path / "live"
    _, receipt = make_replay(root)
    external = tmp_path / "old" / "replay_entry_books" / f"{DAY}.parquet"
    external.parent.mkdir(parents=True)
    external.write_bytes(b"untrusted old file")
    receipt["sessions"][0]["historical_entry_books"]["path"] = str(external)
    write_receipt(root, receipt)
    assert read_replay(root) == {DAY: {"2330": 101.5}}
    assert external.read_bytes() == b"untrusted old file"


@pytest.mark.parametrize("damage", ["missing", "changed", "wrong_digest", "short_digest", "wrong_leaf", "wrong_parent", "symlink_escape"])
def test_relocated_receipt_cannot_bypass_missing_local_book_or_hash(tmp_path, damage):
    root = tmp_path / "live"
    path, receipt = make_replay(root)
    book = receipt["sessions"][0]["historical_entry_books"]
    external = tmp_path / "old" / "replay_entry_books" / f"{DAY}.parquet"
    external.parent.mkdir(parents=True)
    external.write_bytes(path.read_bytes())
    book["path"] = str(external)
    if damage == "missing":
        path.unlink()
    elif damage == "changed":
        path.write_bytes(b"changed")
    elif damage == "wrong_digest":
        book["sha256"] = "0" * 64
    elif damage == "short_digest":
        book["sha256"] = "0" * 63
    elif damage == "wrong_leaf":
        book["path"] = str(external.with_name("2026-02-26.parquet"))
    elif damage == "wrong_parent":
        book["path"] = str(tmp_path / f"{DAY}.parquet")
    else:
        path.unlink()
        path.symlink_to(external)
    write_receipt(root, receipt)
    with pytest.raises(RuntimeError, match="entry.book"):
        read_replay(root)

