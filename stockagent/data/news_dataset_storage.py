"""Copy-on-write parts for the existing news builder, not another collector.

Each release has a complete file inventory. Unchanged immutable parts are
referenced, not copied. A changed part is rewritten only for its retained rows;
new/revised rows go into new parts. Row matching is a multiset: repeated source
observations must not disappear just because their complete values are equal.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq

from downloader.artifact_io import sha256_file
from stockagent.data.news_dataset_sources import checked_path


LAYOUT = "news_incremental_parts_v2"


def news_part_path(root: Path, manifest: dict, part: dict) -> Path:
    if manifest.get("storage_layout") == LAYOUT:
        return checked_path(root, part["path"])
    generation = checked_path(root, manifest["generation"] + "/generation.json").parent
    return checked_path(generation, part["path"])


def news_table_paths(root: Path, table: str, *, manifest: dict | None = None) -> list[Path]:
    """Resolve one pinned complete inventory, including v1 legacy releases."""
    manifest = manifest if manifest is not None else json.loads((root / "manifest.json").read_text())
    if manifest.get("state") != "ready" or manifest.get("contract_version") not in {1, 2}:
        raise ValueError("news dataset is not ready")
    return [news_part_path(root, manifest, p) for p in manifest["parts"] if p["table_name"] == table]


def row_digest(row: dict) -> bytes:
    def temporal(value):
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        raise TypeError(f"unexpected news row type: {type(value).__name__}")
    encoded = json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
                         allow_nan=False, default=temporal).encode("utf-8")
    return hashlib.sha256(encoded).digest()


class CopyOnWriteParts:
    """Use the builder's transaction/cursors for both row matching and writes."""

    def __init__(self, writer, dataset_root: Path, parent: dict, schemas: dict, notify):
        self.writer, self.root, self.parent = writer, dataset_root, parent
        self.schemas, self.notify = schemas, notify
        self.db = writer.db
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS prior_rows(
                table_name TEXT NOT NULL,digest BLOB NOT NULL,path TEXT NOT NULL,
                n INTEGER NOT NULL,used INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(table_name,digest,path)) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS prior_by_path ON prior_rows(path);
        """)
        after, _ = writer.position("prior_parts")
        for number, part in enumerate(parent["parts"]):
            if after and number <= int(after):
                continue
            path = news_part_path(dataset_root, parent, part)
            if sha256_file(path) != part["sha256"]:
                raise ValueError("incremental news base part checksum mismatch")
            relative = str(path.relative_to(dataset_root))
            with pq.ParquetFile(path) as original:
                if original.schema_arrow != schemas[part["table_name"]] or original.metadata.num_rows != part["rows"]:
                    raise ValueError("incremental news base schema/rows mismatch")
                for batch in original.iter_batches(batch_size=2048, use_threads=False):
                    counts = Counter(row_digest(row) for row in batch.to_pylist())
                    self.db.executemany("INSERT INTO prior_rows(table_name,digest,path,n) VALUES (?,?,?,?) "
                                        "ON CONFLICT(table_name,digest,path) DO UPDATE SET n=n+excluded.n",
                                        ((part["table_name"], digest, relative, n) for digest, n in counts.items()))
            writer.checkpoint("prior_parts", number, number + 1)
            if (number + 1) % 64 == 0 or number + 1 == len(parent["parts"]):
                notify("incremental_index", number + 1, len(parent["parts"]))

    def fresh_rows(self, table: str, rows: list[dict]) -> list[dict]:
        digests = [row_digest(row) for row in rows]
        keys, available = list(dict.fromkeys(digests)), defaultdict(list)
        for offset in range(0, len(keys), 400):
            block = keys[offset:offset + 400]
            for item in self.db.execute("SELECT digest,path,n-used FROM prior_rows WHERE table_name=? "
                                        "AND digest IN (" + ",".join("?" for _ in block) + ") AND n>used ORDER BY path",
                                        [table, *block]):
                available[item[0]].append([item[1], item[2]])
        fresh, used = [], Counter()
        for row, digest in zip(rows, digests):
            donors = available[digest]
            while donors and donors[-1][1] == 0:
                donors.pop()
            if not donors:
                fresh.append(row)
            else:
                donor = donors[-1]
                donor[1] -= 1
                used[(digest, donor[0])] += 1
        self.db.executemany("UPDATE prior_rows SET used=used+? WHERE table_name=? AND digest=? AND path=?",
                            ((n, table, digest, path) for (digest, path), n in used.items()))
        return fresh

    def finalize(self):
        if self.writer.position("cow_finalized")[0]:
            return
        # No earlier publication is touched. A crash rolls back the inventory;
        # deterministic new-part names can then be replayed under the build lock.
        for number, part in enumerate(self.parent["parts"]):
            path = news_part_path(self.root, self.parent, part)
            relative = str(path.relative_to(self.root))
            retained = self.db.execute("SELECT coalesce(sum(used),0) FROM prior_rows WHERE path=?", (relative,)).fetchone()[0]
            if retained == part["rows"]:
                self.db.execute("INSERT INTO parts VALUES (?,?,?,?,?)", (relative, part["table_name"],
                                part["rows"], part["sha256"], part["bytes"]))
            elif retained:
                remaining = dict(self.db.execute("SELECT digest,used FROM prior_rows WHERE path=? AND used>0", (relative,)))
                with pq.ParquetFile(path) as original:
                    for batch in original.iter_batches(batch_size=8192, use_threads=False):
                        keep = []
                        for row in batch.to_pylist():
                            digest = row_digest(row)
                            if remaining.get(digest, 0):
                                remaining[digest] -= 1
                                keep.append(row)
                        self.writer.write_part(part["table_name"], keep)
                if any(remaining.values()):
                    raise ValueError("incremental news retained row count mismatch")
            if (number + 1) % 64 == 0 or number + 1 == len(self.parent["parts"]):
                self.notify("incremental_parts", number + 1, len(self.parent["parts"]))
        self.writer.checkpoint("cow_finalized", "complete", len(self.parent["parts"]))

    def statistics(self, parts: list[dict]) -> dict:
        previous = {str(news_part_path(self.root, self.parent, p).relative_to(self.root)) for p in self.parent["parts"]}
        reused = [p for p in parts if p["path"] in previous]
        written = [p for p in parts if p["path"] not in previous]
        matched = self.db.execute("SELECT coalesce(sum(used),0) FROM prior_rows").fetchone()[0]
        dropped = self.db.execute("SELECT coalesce(sum(n-used),0) FROM prior_rows").fetchone()[0]
        return {"policy": LAYOUT, "base_generation": self.parent["generation"],
                "reused_parts": len(reused), "reused_part_bytes": sum(p["bytes"] for p in reused),
                "written_parts": len(written), "written_part_bytes": sum(p["bytes"] for p in written),
                "matched_rows": matched, "removed_or_revised_old_rows": dropped,
                "part_grain": "copy-on-write Parquet parts; retained rows in a changed part may be rewritten",
                "physical_full_copy": False}
