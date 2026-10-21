"""Reuse verified normalized events inside the canonical projection generation.

Unchanged raw receipts are necessary, not sufficient: exact symbol/definition,
publication context, implementation and calendar proofs are also required.
Changed archives still run the original full normalizer. No alternate clocks,
collector, public price permission, TTL or report-period policy lives here.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl
import pyarrow as pa

from stockagent.data.factorized_panel import file_sha256
from stockagent.data.tw_public_incremental import table_content_sha256
from stockagent.data_sync.factorized_projection_reuse import seal_new_file
from stockagent.data_sync.immutable_cache_files import link_immutable_payload

CONTRACT = "factorized_normalized_source_reuse_v1"


def _calendar_sha256(sessions):
    return hashlib.sha256(json.dumps([day.isoformat() for day in sessions], separators=(",", ":")).encode()).hexdigest()


def implementation_identity():
    from scripts import build_tw_release_schedule_dataset as normalizer
    from stockagent.data import finlab_research_overlay, tw_public_release_schedule, tw_public_incremental
    paths = (Path(__file__), *(Path(module.__file__) for module in (
        normalizer, finlab_research_overlay, tw_public_release_schedule, tw_public_incremental)))
    return {"code": {path.name: file_sha256(path) for path in paths},
            "polars": pl.__version__, "arrow": pa.__version__}


def calendar_is_closed(lookup, sessions, *, uploads=None, bounds=None):
    """Only a fully resolved release stream survives a calendar extension.

    An unknown next session or a future issuer-specific publication must be
    re-normalized, even when its raw source did not change.
    """
    if "date" not in lookup.columns or lookup["date"].null_count():
        return False
    for table, column in ((uploads, "known_upload_on"), (bounds, "known_published_on")):
        if table is not None and column in table.columns:
            latest = table[column].max()
            if latest is not None and latest >= sessions[-1]:
                return False
    return True


class NormalizedSourceReuse:
    def __init__(self, staging, previous_root=None, previous=None, *, write_budget=None):
        self.staging = Path(staging)
        self.previous_root = previous_root
        self.previous = previous if isinstance(previous, dict) else {}
        self.members = {}
        self.hits = self.builds = self.rejections = 0
        self.write_budget = write_budget

    def get(self, name, identity, sessions):
        proof = self.previous.get(name)
        if not isinstance(proof, dict) or self.previous_root is None:
            return None
        try:
            count = proof["session_count"]
            if (proof["contract"] != CONTRACT or proof["identity"] != identity
                    or type(count) is not int or not 0 < count <= len(sessions)
                    or _calendar_sha256(sessions[:count]) != proof["sessions_sha256"]
                    or (len(sessions) != count and proof["calendar_closed"] is not True)):
                return None
            relative = proof["path"]
            if Path(relative).name != relative or not relative.startswith("normalized_"):
                raise ValueError("invalid normalized source member")
            target = self.staging / relative
            if self.write_budget is not None:
                self.write_budget.charge(0)
            if not link_immutable_payload(self.previous_root / relative, target,
                    sha256=proof["sha256"], size=proof["bytes"]):
                raise ValueError("normalized source member changed")
            table = pl.read_parquet(target)
            if table_content_sha256(table.to_arrow()) != proof["values_sha256"]:
                raise ValueError("normalized source values changed")
            self.members[name] = proof
            self.hits += 1
            return table
        except (OSError, ValueError, KeyError, TypeError):
            self.rejections += 1
            # A linked immutable donor may be retained only in this private
            # attempt; a miss writes a distinct file and never edits its inode.
            return None

    def store(self, name, identity, sessions, calendar_closed, table):
        token = hashlib.sha256(name.encode()).hexdigest()
        path = self.staging / f"normalized_{token}.parquet"
        if path.exists():
            # A rejected donor remains audit evidence, not an overwrite target.
            path = self.staging / f"normalized_{token}_rebuilt.parquet"
        if self.write_budget is None:
            table.write_parquet(path, compression="zstd")
        else:
            with self.write_budget.open(path) as stream:
                table.write_parquet(stream, compression="zstd")
        values_sha = table_content_sha256(table.to_arrow())
        if table_content_sha256(pl.read_parquet(path).to_arrow()) != values_sha:
            raise ValueError("normalized source roundtrip changed typed values")
        seal_new_file(path)
        self.members[name] = {"contract": CONTRACT, "path": path.name,
            "sha256": file_sha256(path), "bytes": path.stat().st_size,
            "values_sha256": values_sha, "rows": table.height, "identity": identity,
            "session_count": len(sessions), "sessions_sha256": _calendar_sha256(sessions),
            "calendar_closed": bool(calendar_closed)}
        self.builds += 1

    def stats(self):
        return {"contract": CONTRACT, "hits": self.hits, "builds": self.builds,
                "rejections": self.rejections, "changed_source_full_normalization_retained": True}
