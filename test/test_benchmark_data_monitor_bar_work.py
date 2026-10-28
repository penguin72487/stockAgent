import json

import pytest

from scripts.benchmark_data_monitor_bar_work import Observation


def test_capture_and_replay_preserve_missing_source_exception(tmp_path):
    observation = Observation(tmp_path)
    path = str(tmp_path / "missing.json")
    def missing():
        raise FileNotFoundError(2, "No such file or directory", path)
    with pytest.raises(FileNotFoundError) as captured:
        observation.invoke("json", path, missing)
    replay = Observation(tmp_path, json.loads(json.dumps(observation.events)))
    with pytest.raises(FileNotFoundError) as restored:
        replay.invoke("json", path, lambda: pytest.fail("replay must not read the source"))
    assert type(restored.value) is type(captured.value)
    assert str(restored.value) == str(captured.value)


def test_unknown_source_error_rejects_replay_without_becoming_an_inventory_error(tmp_path):
    class UnrecognizedSourceError(ValueError):
        pass
    def invalid():
        raise UnrecognizedSourceError("retain original failure")
    observation = Observation(tmp_path)
    with pytest.raises(UnrecognizedSourceError):
        observation.invoke("project", "source", invalid)
    replay = Observation(tmp_path, observation.events)
    with pytest.raises(AssertionError, match="unsupported captured exception"):
        replay.invoke("project", "source", invalid)


def test_reordered_source_work_and_json_parse_errors_are_not_hidden(tmp_path):
    observation = Observation(tmp_path)
    with pytest.raises(json.JSONDecodeError):
        observation.invoke("json", "invalid", lambda: json.loads("{not-json"))
    replay = Observation(tmp_path, json.loads(json.dumps(observation.events)))
    with pytest.raises(AssertionError):
        replay.invoke("json", "different-source", lambda: None)
    replay = Observation(tmp_path, json.loads(json.dumps(observation.events)))
    with pytest.raises(json.JSONDecodeError):
        replay.invoke("json", "invalid", lambda: None)
