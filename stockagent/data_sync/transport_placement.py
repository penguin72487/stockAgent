"""One live backing for the existing lab203 sender path, independent of cold D."""
from functools import lru_cache
import json
from pathlib import Path

from stockagent.data_sync.desync_snapshots import SnapshotError

CONFIG = Path(__file__).resolve().parents[2] / "configs/data_sync/backup_transport_volume.json"
CANONICAL = Path("/srv/stockagent-backup-ingress-lab203")
D_BACKING = Path("/srv/stockagent-d-volume/stockagent-backup-ingress-lab203")


@lru_cache(maxsize=2)
def _read(generation):
    raw = CONFIG.read_bytes()
    c = json.loads(raw)
    if (c.get("authority_node_id") != "penguin" or c.get("purpose") != "lab203-backup-transport-only"
            or c.get("independent_disaster_recovery_copy") is not False):
        raise SnapshotError("invalid transport volume marker")
    if c.get("schema_version") == 1 and c.get("physical_volume") == "D:":
        return c, raw
    if (c.get("schema_version") != 2 or c.get("physical_volume") != "E:"
            or c.get("backing") != "enrolled_ext4_workspace"
            or c.get("relative_root") != "transport/lab203"):
        raise SnapshotError("invalid E transport placement")
    return c, raw


def placement():
    s = CONFIG.lstat()
    if CONFIG.is_symlink():
        raise SnapshotError("redirected transport enrollment")
    return _read((s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns))


def on_e():
    return placement()[0]["physical_volume"] == "E:"


def backing(*, guard=True):
    c, _ = placement()
    if c["physical_volume"] == "D:":
        return D_BACKING
    from stockagent.data_sync.workspace_volume import check, configuration
    if guard:
        check()
    return Path(configuration()["mount_point"]) / c["relative_root"]


def check_transport(*, physical=False):
    c, raw = placement()
    root = backing()
    if root.is_symlink() or not root.is_dir() or not CANONICAL.is_mount() or not root.samefile(CANONICAL):
        raise SnapshotError("transport alias differs from its enrolled backing")
    marker = root / ".transport-owner.json"
    if marker.is_symlink() or marker.read_bytes() != raw:
        raise SnapshotError("transport backing marker differs")
    if on_e():
        from stockagent.data_sync.workspace_volume import check
        check(physical=physical)
    return {"physical_volume": c["physical_volume"], "backing": str(root)}
