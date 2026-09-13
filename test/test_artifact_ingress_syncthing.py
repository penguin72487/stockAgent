import copy
import json
from pathlib import Path

import pytest

from scripts.configure_artifact_ingress_syncthing import assert_folder, expected_folder


@pytest.mark.parametrize(
    "role, own, kind",
    [("producer", "vast", "sendonly"), ("receiver", "penguin", "receiveonly")],
)
def test_resolve_exact_peer_identity_and_direction(role, own, kind):
    config = json.loads(Path("configs/data_sync/artifact_ingress.json").read_text())
    devices = [
        {"name": "penguin", "deviceID": "penguin"},
        {"name": "vastai1T", "deviceID": "vast"},
        {"name": "vastai1T-old", "deviceID": "old"},
    ]
    folder, peer = expected_folder(config, role, devices, own)
    assert folder["type"] == kind
    assert {d["deviceID"] for d in folder["devices"]} == {"penguin", "vast"}
    assert peer != own
    assert_folder(folder, folder)
    with pytest.raises(RuntimeError, match="not the configured"):
        expected_folder(config, role, devices, "wrong-self")
    with pytest.raises(RuntimeError, match="not unique"):
        expected_folder(config, role, [*devices, devices[0]], own)


@pytest.mark.parametrize(
    "mutation",
    ["id", "path", "type", "paused", "fsWatcherEnabled", "peers", "ignoreDelete"],
)
def test_existing_folder_is_never_broadly_replaced(mutation):
    expected = {
        "id": "scope",
        "path": "/srv/scope",
        "type": "receiveonly",
        "paused": False,
        "fsWatcherEnabled": True,
        "devices": [{"deviceID": "local"}, {"deviceID": "remote"}],
    }
    current = copy.deepcopy(expected)
    if mutation == "peers":
        current["devices"].append({"deviceID": "unauthorized"})
    elif mutation in ("paused", "fsWatcherEnabled", "ignoreDelete"):
        current[mutation] = not current.get(mutation, False)
    else:
        current[mutation] = "different"
    with pytest.raises(RuntimeError):
        assert_folder(current, expected)
