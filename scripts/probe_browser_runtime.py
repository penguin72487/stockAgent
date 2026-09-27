#!/usr/bin/env python3
"""Bounded Playwright control-page probe, separate from dashboard acceptance.

Only a private data: page is opened. A frame/click failure remains a failure;
there is no force-click, synthetic DOM click, retry, or environment skip.
The explicit cpu-2d profile is a software-compositor workaround, not GPU or
WebGL acceptance; the default profile remains unchanged for diagnosis.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
from importlib.metadata import version
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from urllib.parse import quote


_BROWSER_PROFILE_FLAGS = {
    "default": (),
    "cpu-2d": ("--disable-gpu", "--disable-software-rasterizer"),
}


def browser_profile_launch_options(browser_profile: str = "default") -> dict:
    """Fixed, explicit launch profiles; no environment choice or fallback.

    Disabling GPU alone still permits software GL. Both cpu-2d flags select
    Chromium's software display compositor, retaining HTML/SVG/Canvas 2D but
    excluding GPU/WebGL coverage. They do not alter native frame/click checks.
    """
    if browser_profile not in _BROWSER_PROFILE_FLAGS:
        raise ValueError(f"unknown browser profile: {browser_profile}")
    options = {"headless": True}
    flags = _BROWSER_PROFILE_FLAGS[browser_profile]
    if flags:
        options["args"] = list(flags)
    return options


def interaction_passed(result: dict) -> bool:
    observation = result.get("frame_observation")
    frames = observation.get("frames") if isinstance(observation, dict) else None
    return (
        type(frames) is int and frames >= 2
        and result.get("ordinary_click") == "passed"
        and result.get("clicked") == "yes"
    )


def sample(browser_profile: str = "default") -> dict:
    from playwright.sync_api import TimeoutError, sync_playwright

    launch_options = browser_profile_launch_options(browser_profile)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(**launch_options, timeout=5_000)
        try:
            page = browser.new_page(viewport={"width": 1280, "height": 800})
            html = '<button onclick="document.body.dataset.clicked=\'yes\'">test</button>'
            page.goto("data:text/html," + quote(html), wait_until="load", timeout=3_000)
            observed = page.evaluate("""() => new Promise(resolve => {
                let frames = 0, handle;
                const started = performance.now();
                const tick = () => {frames++; handle = requestAnimationFrame(tick)};
                handle = requestAnimationFrame(tick);
                setTimeout(() => {
                    cancelAnimationFrame(handle);
                    const button = document.querySelector('button');
                    resolve({frames: frames, sample_ms: performance.now()-started,
                        visibility: document.visibilityState, focused: document.hasFocus(),
                        rect: button.getBoundingClientRect().toJSON(),
                        display: getComputedStyle(button).display,
                        css_visibility: getComputedStyle(button).visibility});
                }, 500);
            })""")
            result = {"playwright": version("playwright"), "chromium": browser.version,
                      "browser_profile": browser_profile,
                      "launch": launch_options, "frame_observation": observed}
            try:
                page.get_by_role("button", name="test", exact=True).click(timeout=1_500)
                result["ordinary_click"] = "passed"
                result["clicked"] = page.evaluate("document.body.dataset.clicked")
            except TimeoutError as exc:
                result["ordinary_click"] = "timeout"
                result["error"] = str(exc)
            result["accepted"] = interaction_passed(result)
            return result
        finally:
            browser.close()


def bounded_sample(browser_profile: str = "default") -> dict:
    """A hung browser/driver cannot prevent a terminal diagnostic receipt."""
    process = None
    try:
        launch_options = browser_profile_launch_options(browser_profile)
        command = [sys.executable, str(Path(__file__).resolve())]
        if browser_profile != "default":
            command.extend(["--browser-profile", browser_profile])
        command.append("--worker")
        process = subprocess.Popen(
            command,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=True,
        )
        try:
            stdout, stderr = process.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            # Playwright launches Chromium in its own group. First let the
            # worker's finally blocks close the browser and its driver normally.
            process.terminate()
            cleanup = "worker_exited_after_termination_browser_cleanup_not_independently_verified"
            try:
                process.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                cleanup = "forced_worker_group_stop_browser_cleanup_unverified"
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    cleanup = "forced_worker_group_stop_reap_and_browser_cleanup_unverified"
            return {"accepted": False, "error": "worker_deadline",
                    "timeout_seconds": 15, "cleanup": cleanup}
        if process.returncode:
            return {"accepted": False, "error": "worker_failed",
                    "returncode": process.returncode, "stderr": stderr[-4_096:]}
        payload = json.loads(stdout)
        if not isinstance(payload, dict) or type(payload.get("accepted")) is not bool:
            raise ValueError("invalid browser probe receipt")
        if payload["accepted"] != interaction_passed(payload):
            raise ValueError("inconsistent browser probe receipt")
        reported_launch = payload.get("launch")
        if (payload.get("browser_profile") != browser_profile
                or reported_launch != launch_options
                or not isinstance(reported_launch, dict)
                or type(reported_launch.get("headless")) is not bool):
            raise ValueError("browser profile differs from requested launch")
        return payload
    except subprocess.TimeoutExpired:
        return {"accepted": False, "error": "worker_deadline", "timeout_seconds": 15}
    except (OSError, ValueError) as exc:
        return {"accepted": False, "error": type(exc).__name__}
    finally:
        if process is not None:
            for stream in (process.stdout, process.stderr):
                if stream is not None:
                    stream.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--browser-profile", choices=tuple(_BROWSER_PROFILE_FLAGS), default="default",
        help="Explicit runtime choice; cpu-2d excludes GPU/WebGL coverage (no fallback).",
    )
    args = parser.parse_args()
    if args.worker:
        def terminate_worker(_signum, _frame):
            raise SystemExit(124)

        signal.signal(signal.SIGTERM, terminate_worker)
        print(json.dumps(sample(args.browser_profile), ensure_ascii=False), flush=True)
        return 0
    result = {"schema_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(),
              "browser_profile": args.browser_profile,
              "boundary": "Private data: page only; not dashboard, API, or network acceptance. "
                          "The cpu-2d workaround does not provide GPU/WebGL coverage.",
              "measurement": bounded_sample(args.browser_profile)}
    encoded = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as handle:
            handle.write(encoded)
    print(encoded, end="")
    return 0 if result["measurement"]["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
