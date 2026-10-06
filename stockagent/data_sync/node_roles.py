"""Locally enrolled node roles; synchronized settings cannot waive consumers."""
from __future__ import annotations

import json
from pathlib import Path
import stat
import subprocess

from stockagent.data_sync.desync_snapshots import SnapshotError

ROLE_FILE = Path("/etc/stockagent/node-storage-role.json")
EXPECTED = {"schema_version": 1, "node_id": "vastai1T", "authority_node_id": "penguin",
            "service_role": "training_only"}


def training_only_node(*, role_file: Path = ROLE_FILE,
                       cold_root: Path = Path("/srv/stockagent-packed"),
                       proc_root: Path = Path("/proc"),
                       edge_state: Path = Path("/var/lib/stockagent-packed-edge/state.json"),
                       repo_root: Path = Path("/root/stockAgent"), run=subprocess.run) -> bool:
    """Exclude foreign service templates only on an explicitly enrolled edge.

    Training/config/FD/mmap, source recovery and transport gates are independent
    and remain mandatory. An unexpected local service makes cleanup fail closed.
    """
    if not role_file.exists() and not role_file.is_symlink():
        return False
    try:
        info = role_file.lstat()
        if (role_file.resolve() != role_file or not stat.S_ISREG(info.st_mode)
                or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_size > 4096):
            raise SnapshotError("node service role is not a private local enrollment")
        value = json.loads(role_file.read_text())
        if value != EXPECTED:
            raise SnapshotError("unknown node service role")
        identity = cold_root / ".local-state/node-id"
        if identity.is_symlink() or identity.read_text().strip() != "vastai1T":
            raise SnapshotError("node service role does not match the initialized edge")
        # The canonical edge state is checked by the transport/retirement owner;
        # node identity alone is deliberately insufficient for role enrollment.
        if (not edge_state.is_file() or edge_state.resolve() != edge_state
                or json.loads(edge_state.read_text()).get("mode") != "index-only"):
            raise SnapshotError("training-only enrollment requires index-only edge state")
        result = run(["supervisorctl", "status"], capture_output=True, text=True, timeout=15)
        if result.returncode not in {0, 3}:
            raise SnapshotError("local service supervision could not be observed")
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) < 2:
                raise SnapshotError("local service inventory is incomplete")
            name, state = parts[:2]
            if name.startswith("stockagent") and state in {"RUNNING", "STARTING", "STOPPING", "BACKOFF"}:
                if not name.endswith("_training"):
                    raise SnapshotError("a project service is still active on the training-only node")
        for process in proc_root.glob("[0-9]*"):
            try:
                argv = process.joinpath("cmdline").read_bytes().decode(errors="replace").split("\0")
            except FileNotFoundError:
                continue
            if any("stockagent.live." in arg or (arg.endswith((".py", ".sh")) and
                   (arg.startswith(str(repo_root) + "/") or arg.startswith("scripts/") or arg.startswith("services/")) and
                   ("/services/" in arg or "/stockagent/live/" in arg or
                    Path(arg).name in {"run_discord_bot.sh", "run_tw_day_trade_service.sh",
                                       "run_tw_overnight_service.sh", "run_taifex_dashboard.sh"}))
                   for arg in argv):
                raise SnapshotError("an independent project service is active on the training-only node")
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        raise SnapshotError("node service role observation is incomplete") from error
    return True
