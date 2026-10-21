"""Reader-first admission for the existing TW public whole-tree publisher.

Packed storage still owns inventory, rights, locks and exact reconstruction.
This guard adds the logical-manifest/member relationship; it neither discovers
licensed sources nor enrolls a new publication policy.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from stockagent.data_sync.desync_snapshots import SnapshotError

CONTRACT = "tw_public_partitioned_source_publication_v1"
SUFFIX = ".manifest.json"


def _candidates(root):
    # These are the canonical outputs being migrated, not a recursive walk of
    # raw acquisition trees. Other dataset families retain their own contracts.
    paths = [root / "features/tw_public_stock_daily.parquet"]
    for directory in (root / "stocks", root / "shioaji/stocks"):
        paths.extend(directory.glob("*_features.parquet"))
        paths.extend(path.with_name(path.name.removesuffix(SUFFIX))
                     for path in directory.glob("*_features.parquet" + SUFFIX))
    return sorted(set(path for path in paths if path.exists() or Path(str(path) + SUFFIX).exists()))


class ParquetPublicationSnapshot:
    def __init__(self, root, *, dataset=None, excluded=(), included_root_files=None, file_overrides=None):
        self.root = Path(root).resolve(strict=True)
        self.excluded = tuple(excluded)
        self.included = included_root_files
        self.overrides = file_overrides or {}
        # The catalog identity survives relocation/restore. Basename inference
        # remains compatible for direct callers, not a gate that a renamed
        # canonical tw-public root can silently bypass.
        self.applicable = dataset == "tw-public" or self.root.name == "data_tw_public"
        self.sources = []
        self.selected = {}
        self.membership = self._membership()
        for logical in self.membership:
            from stockagent.data.partitioned_parquet import ParquetSource, resolve_source
            try:
                selected = resolve_source(logical)
                self.selected[logical] = selected
                if not selected.name.endswith(SUFFIX):
                    continue
                # Root-files-only releases deliberately cannot expand into
                # nested payloads. Use the catalog's existing whole-tree scope.
                if self.included is not None:
                    raise SnapshotError("partitioned Parquet requires a whole-tree publication scope")
                relative = str(logical.relative_to(self.root))
                if relative in self.overrides or relative + SUFFIX in self.overrides:
                    raise SnapshotError("retained single-file preparation conflicts with partitioned source; use a fresh preparation")
                source = ParquetSource(selected)
                for member in source.source_paths():
                    if not member.is_relative_to(self.root) or self._excluded(member):
                        raise SnapshotError("partitioned publication omits or redirects a required member")
                self.sources.append(source)
            except (OSError, ValueError, KeyError, TypeError) as error:
                raise SnapshotError(f"invalid partitioned publication source {logical}: {error}") from error
        self.guard()

    def _excluded(self, path):
        name = path.relative_to(self.root).as_posix()
        return any(name == excluded or name.startswith(excluded + "/") for excluded in self.excluded)

    def _membership(self):
        if not self.applicable:
            return ()
        candidates = _candidates(self.root)
        return tuple(path for path in candidates if not self._excluded(path)
                     and (self.included is None or path.name in self.included))

    def guard(self):
        if not self.applicable:
            return
        if self._membership() != self.membership:
            raise SnapshotError("partitioned logical source membership changed during publication")
        from stockagent.data.partitioned_parquet import resolve_source
        try:
            for logical, selected in self.selected.items():
                if resolve_source(logical) != selected:
                    raise SnapshotError("Parquet source format changed during publication")
            for source in self.sources:
                resolve_source(source.path)
                source.verify()
        except (OSError, ValueError) as error:
            raise SnapshotError(f"partitioned source changed before publication: {error}") from error

    @property
    def metadata(self):
        if not self.sources:
            return {}
        proof = [[str(source.path.relative_to(self.root)), source.fingerprints()] for source in self.sources]
        return {"partitioned_source_contract": CONTRACT,
                "partitioned_logical_sources": str(len(self.sources)),
                "partitioned_source_members": str(sum(len(source.members) for source in self.sources)),
                "partitioned_source_identity": hashlib.sha256(json.dumps(proof,
                    sort_keys=True, separators=(",", ":")).encode()).hexdigest()}
