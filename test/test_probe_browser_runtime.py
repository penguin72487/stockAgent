from __future__ import annotations

import json
import signal
import subprocess
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from scripts import probe_browser_runtime as probe


def _measurement(browser_profile: str = "default", *, accepted: bool = True) -> dict:
    return {
        "accepted": accepted,
        "playwright": "test-playwright",
        "chromium": "test-chromium",
        "browser_profile": browser_profile,
        "launch": probe.browser_profile_launch_options(browser_profile),
        "frame_observation": {
            "frames": 30 if accepted else 0,
            "sample_ms": 500.0,
            "visibility": "visible",
            "focused": True,
            "rect": {"x": 8, "y": 8, "width": 41, "height": 21},
            "display": "inline-block",
            "css_visibility": "visible",
        },
        "ordinary_click": "passed",
        "clicked": "yes",
    }


@pytest.fixture
def fake_browser(monkeypatch):
    class BrowserTimeout(Exception):
        pass

    page = Mock()
    page.evaluate.side_effect = [_measurement()["frame_observation"], "yes"]
    browser = Mock(version="test-chromium")
    browser.new_page.return_value = page
    chromium = Mock()
    chromium.launch.return_value = browser

    class PlaywrightContext:
        def __enter__(self):
            return SimpleNamespace(chromium=chromium)

        def __exit__(self, *_args):
            return False

    module = ModuleType("playwright.sync_api")
    module.TimeoutError = BrowserTimeout
    module.sync_playwright = PlaywrightContext
    monkeypatch.setitem(sys.modules, "playwright.sync_api", module)
    monkeypatch.setattr(probe, "version", lambda _name: "test-playwright")
    return SimpleNamespace(
        browser=browser, page=page, chromium=chromium, timeout=BrowserTimeout
    )


@pytest.mark.parametrize(
    ("frames", "clicked", "accepted"),
    [(0, "yes", False), (1, "yes", False), (2, "yes", True), (30, None, False)],
)
def test_worker_requires_frames_and_real_click_effect(
    fake_browser, frames, clicked, accepted,
):
    observed = {**_measurement()["frame_observation"], "frames": frames}
    fake_browser.page.evaluate.side_effect = [observed, clicked]

    result = probe.sample()

    assert result["accepted"] is accepted
    assert result["ordinary_click"] == "passed"
    assert result["clicked"] == clicked
    fake_browser.chromium.launch.assert_called_once_with(headless=True, timeout=5_000)
    fake_browser.page.new_page.assert_not_called()
    fake_browser.browser.new_page.assert_called_once_with(
        viewport={"width": 1280, "height": 800}
    )
    navigation = fake_browser.page.goto.call_args
    assert navigation.args[0].startswith("data:text/html,")
    assert navigation.kwargs == {"wait_until": "load", "timeout": 3_000}
    fake_browser.page.get_by_role.assert_called_once_with(
        "button", name="test", exact=True
    )
    fake_browser.page.get_by_role.return_value.click.assert_called_once_with(
        timeout=1_500
    )
    script = fake_browser.page.evaluate.call_args_list[0].args[0]
    assert "requestAnimationFrame" in script
    assert "cancelAnimationFrame" in script
    assert "dispatchEvent" not in script
    assert ".click(" not in script
    fake_browser.browser.close.assert_called_once_with()


def test_worker_click_timeout_is_failure_and_still_closes_browser(fake_browser):
    fake_browser.page.get_by_role.return_value.click.side_effect = fake_browser.timeout(
        "injected ordinary click timeout"
    )

    result = probe.sample()

    assert result["accepted"] is False
    assert result["ordinary_click"] == "timeout"
    assert "injected ordinary click timeout" in result["error"]
    assert "clicked" not in result
    assert fake_browser.page.evaluate.call_count == 1
    fake_browser.browser.close.assert_called_once_with()


@pytest.mark.parametrize("phase", ["new_page", "goto", "evaluate", "close"])
def test_worker_runtime_errors_propagate_without_success_or_skip(fake_browser, phase):
    owner = fake_browser.browser if phase in {"new_page", "close"} else fake_browser.page
    getattr(owner, phase).side_effect = RuntimeError(f"injected {phase} failure")

    with pytest.raises(RuntimeError, match=f"injected {phase} failure"):
        probe.sample()

    fake_browser.browser.close.assert_called_once_with()


@pytest.mark.parametrize("accepted", [False, True])
def test_parent_main_exit_matches_receipt_without_claiming_dashboard_acceptance(
    tmp_path, monkeypatch, capsys, accepted,
):
    result = _measurement(accepted=accepted)
    output = tmp_path / "new" / "receipt.json"
    monkeypatch.setattr(probe, "bounded_sample", lambda _profile: result)
    monkeypatch.setattr(sys, "argv", ["probe", "--output", str(output)])

    assert probe.main() == (0 if accepted else 1)

    receipt = json.loads(output.read_text())
    assert receipt == json.loads(capsys.readouterr().out)
    assert receipt["schema_version"] == 1
    assert receipt["observed_at_utc"]
    assert "not dashboard, API, or network acceptance" in receipt["boundary"]
    assert receipt["measurement"] == result


def test_existing_receipt_is_never_overwritten(tmp_path, monkeypatch, capsys):
    output = tmp_path / "receipt.json"
    original = b'{"original": "immutable diagnostic evidence"}\n'
    output.write_bytes(original)
    monkeypatch.setattr(probe, "bounded_sample", _measurement)
    monkeypatch.setattr(sys, "argv", ["probe", "--output", str(output)])

    with pytest.raises(FileExistsError):
        probe.main()

    assert output.read_bytes() == original
    assert capsys.readouterr().out == ""


def test_worker_entrypoint_emits_measurement_only_without_recursion(monkeypatch, capsys):
    result = _measurement(accepted=False)
    monkeypatch.setattr(probe, "sample", lambda _profile: result)
    parent = Mock(side_effect=AssertionError("worker must not spawn another worker"))
    monkeypatch.setattr(probe, "bounded_sample", parent)
    monkeypatch.setattr(sys, "argv", ["probe", "--worker"])
    install_handler = Mock()
    monkeypatch.setattr(probe.signal, "signal", install_handler)

    assert probe.main() == 0

    assert json.loads(capsys.readouterr().out) == result
    parent.assert_not_called()
    assert install_handler.call_args.args[0] == signal.SIGTERM
    with pytest.raises(SystemExit) as exc:
        install_handler.call_args.args[1](signal.SIGTERM, None)
    assert exc.value.code == 124


@pytest.fixture
def fake_process(monkeypatch):
    process = Mock(pid=991_234, returncode=0)
    process.communicate.return_value = (json.dumps(_measurement()), "")
    process.wait.return_value = 0
    launch = Mock(return_value=process)
    kill_group = Mock()
    monkeypatch.setattr(probe.subprocess, "Popen", launch)
    monkeypatch.setattr(probe.os, "killpg", kill_group)
    return SimpleNamespace(process=process, launch=launch, kill_group=kill_group)


@pytest.mark.parametrize("accepted", [False, True])
def test_parent_validates_worker_measurement_and_uses_private_process_group(
    fake_process, accepted,
):
    expected = _measurement(accepted=accepted)
    fake_process.process.communicate.return_value = (json.dumps(expected), "")

    assert probe.bounded_sample() == expected

    command = fake_process.launch.call_args.args[0]
    assert command[0] == sys.executable
    assert command[-1] == "--worker"
    assert fake_process.launch.call_args.kwargs == {
        "text": True,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "start_new_session": True,
    }
    fake_process.process.communicate.assert_called_once_with(timeout=15)
    fake_process.process.terminate.assert_not_called()
    fake_process.kill_group.assert_not_called()


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        "[]",
        '{"accepted": true}',
        '{"accepted": "true"}',
        json.dumps({**_measurement(), "frame_observation": {"frames": 0}}),
        json.dumps({**_measurement(), "frame_observation": {"frames": True}}),
        json.dumps({**_measurement(), "frame_observation": {"frames": 2.0}}),
        json.dumps({**_measurement(), "ordinary_click": "timeout"}),
        json.dumps({**_measurement(), "clicked": "no"}),
        json.dumps({**_measurement(), "accepted": False}),
    ],
)
def test_parent_rejects_malformed_or_inconsistent_success(fake_process, payload):
    fake_process.process.communicate.return_value = (payload, "")

    result = probe.bounded_sample()

    assert result["accepted"] is False
    assert result["error"] in {"ValueError", "JSONDecodeError"}


def test_parent_nonzero_worker_exit_cannot_publish_success(fake_process):
    fake_process.process.returncode = 7
    fake_process.process.communicate.return_value = (json.dumps(_measurement()), "x" * 5_000)

    result = probe.bounded_sample()

    assert result["accepted"] is False
    assert result["error"] == "worker_failed"
    assert result["returncode"] == 7
    assert result["stderr"] == "x" * 4_096


def test_parent_spawn_failure_is_terminal_failure(fake_process):
    fake_process.launch.side_effect = OSError("injected process creation failure")

    result = probe.bounded_sample()

    assert result["accepted"] is False
    assert result["error"] == "OSError"
    fake_process.process.communicate.assert_not_called()
    fake_process.kill_group.assert_not_called()


def test_deadline_stays_failure_even_if_worker_returns_success_during_grace(fake_process):
    fake_process.process.communicate.side_effect = [
        subprocess.TimeoutExpired("worker", 15),
        (json.dumps(_measurement()), ""),
    ]

    result = probe.bounded_sample()

    assert result["accepted"] is False
    assert result["error"] == "worker_deadline"
    assert result["timeout_seconds"] == 15
    assert "exited_after_termination" in result["cleanup"]
    assert [call.kwargs for call in fake_process.process.communicate.call_args_list] == [
        {"timeout": 15}, {"timeout": 3},
    ]
    fake_process.process.terminate.assert_called_once_with()
    fake_process.kill_group.assert_not_called()


@pytest.mark.parametrize("already_exited", [False, True])
def test_forced_deadline_kills_only_private_group_reaps_and_marks_unverified(
    fake_process, already_exited,
):
    fake_process.process.communicate.side_effect = [
        subprocess.TimeoutExpired("worker", 15),
        subprocess.TimeoutExpired("worker", 3),
    ]
    if already_exited:
        fake_process.kill_group.side_effect = ProcessLookupError("already exited")

    result = probe.bounded_sample()

    assert result["accepted"] is False
    assert result["error"] == "worker_deadline"
    assert "forced" in result["cleanup"]
    assert "unverified" in result["cleanup"]
    fake_process.process.terminate.assert_called_once_with()
    fake_process.kill_group.assert_called_once_with(991_234, signal.SIGKILL)
    fake_process.process.wait.assert_called_once_with(timeout=5)
    assert fake_process.process.communicate.call_count == 2
    fake_process.process.stdout.close.assert_called_once_with()
    fake_process.process.stderr.close.assert_called_once_with()


def test_forced_reap_timeout_retains_unverified_cleanup_and_closes_pipes(fake_process):
    fake_process.process.communicate.side_effect = [
        subprocess.TimeoutExpired("worker", 15),
        subprocess.TimeoutExpired("worker", 3),
    ]
    fake_process.process.wait.side_effect = subprocess.TimeoutExpired("worker", 5)

    result = probe.bounded_sample()

    assert result["accepted"] is False
    assert result["error"] == "worker_deadline"
    assert "unverified" in result["cleanup"]
    fake_process.process.stdout.close.assert_called_once_with()
    fake_process.process.stderr.close.assert_called_once_with()


def test_profiles_are_explicit_fixed_and_return_fresh_options(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_BROWSER_PROFILE", "cpu-2d")
    assert probe.browser_profile_launch_options() == {"headless": True}
    expected = {"headless": True,
                "args": ["--disable-gpu", "--disable-software-rasterizer"]}
    options = probe.browser_profile_launch_options("cpu-2d")
    assert options == expected
    options["args"].append("--enable-begin-frame-control")
    assert probe.browser_profile_launch_options("cpu-2d") == expected
    with pytest.raises(ValueError, match="unknown browser profile"):
        probe.browser_profile_launch_options("unknown")


@pytest.mark.parametrize("click_timeout", [False, True])
def test_cpu_profile_preserves_native_checks_and_never_falls_back(
    fake_browser, click_timeout,
):
    if click_timeout:
        fake_browser.page.get_by_role.return_value.click.side_effect = (
            fake_browser.timeout("ordinary click still failed")
        )

    result = probe.sample("cpu-2d")

    fake_browser.chromium.launch.assert_called_once_with(
        headless=True, timeout=5_000,
        args=["--disable-gpu", "--disable-software-rasterizer"],
    )
    fake_browser.page.get_by_role.return_value.click.assert_called_once_with(
        timeout=1_500
    )
    assert "requestAnimationFrame" in fake_browser.page.evaluate.call_args_list[0].args[0]
    assert "}, 500)" in fake_browser.page.evaluate.call_args_list[0].args[0]
    assert result["browser_profile"] == "cpu-2d"
    assert result["launch"] == probe.browser_profile_launch_options("cpu-2d")
    assert result["accepted"] is (not click_timeout)
    fake_browser.browser.close.assert_called_once_with()


def test_cpu_profile_is_forwarded_to_child_and_checked(fake_process):
    expected = _measurement("cpu-2d")
    fake_process.process.communicate.return_value = (json.dumps(expected), "")

    assert probe.bounded_sample("cpu-2d") == expected

    command = fake_process.launch.call_args.args[0]
    assert command[-3:] == ["--browser-profile", "cpu-2d", "--worker"]
    fake_process.process.communicate.assert_called_once_with(timeout=15)


@pytest.mark.parametrize(
    ("requested", "profile", "launch"),
    [
        ("cpu-2d", "default", {"headless": True}),
        ("cpu-2d", "cpu-2d", {"headless": True}),
        ("default", "cpu-2d", {"headless": True}),
        ("default", "default", {"headless": True, "args": ["--disable-gpu"]}),
        ("default", "default", {"headless": 1}),
    ],
)
def test_parent_rejects_profile_or_launch_mismatch(
    fake_process, requested, profile, launch,
):
    payload = {**_measurement(), "browser_profile": profile, "launch": launch}
    fake_process.process.communicate.return_value = (json.dumps(payload), "")

    result = probe.bounded_sample(requested)

    assert result == {"accepted": False, "error": "ValueError"}


def test_parent_unknown_profile_does_not_launch_worker(fake_process):
    assert probe.bounded_sample("unknown") == {"accepted": False, "error": "ValueError"}
    fake_process.launch.assert_not_called()


def test_cli_cpu_profile_is_explicit_and_discloses_coverage(monkeypatch, capsys):
    result = _measurement("cpu-2d")
    collect = Mock(return_value=result)
    monkeypatch.setattr(probe, "bounded_sample", collect)
    monkeypatch.setattr(sys, "argv", ["probe", "--browser-profile", "cpu-2d"])

    assert probe.main() == 0

    receipt = json.loads(capsys.readouterr().out)
    collect.assert_called_once_with("cpu-2d")
    assert receipt["browser_profile"] == "cpu-2d"
    assert "does not provide GPU/WebGL coverage" in receipt["boundary"]
    assert receipt["measurement"] == result


def test_worker_cli_forwards_profile_without_parent_retry(monkeypatch, capsys):
    result = _measurement("cpu-2d")
    collect = Mock(return_value=result)
    monkeypatch.setattr(probe, "sample", collect)
    monkeypatch.setattr(probe.signal, "signal", Mock())
    monkeypatch.setattr(sys, "argv", ["probe", "--browser-profile", "cpu-2d", "--worker"])

    assert probe.main() == 0

    collect.assert_called_once_with("cpu-2d")
    assert json.loads(capsys.readouterr().out) == result
