#!/usr/bin/env python3
"""Check official Shioaji releases; validate and update outside both sessions.

No broker login, order submission, or ledger repair is performed by this tool.
The SDK is the only package installed. Long-lived native owners are stopped
before replacement, then restored; a durable journal also drives crash recovery.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, time, timezone
from email.parser import BytesParser
import fcntl
import hashlib
import importlib.metadata as metadata
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.parse import urlparse
from urllib.request import urlopen
import zipfile
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.tags import sys_tags
from packaging.utils import canonicalize_name, parse_wheel_filename
from packaging.version import Version

from downloader.artifact_io import atomic_write_json
from stockagent.live.day_trade_maintenance import control_lease

TAIPEI = ZoneInfo("Asia/Taipei")
STATE = ROOT / "artifacts/operations/shioaji_sdk"
# These are the existing canonical owners, not new broker connections.
OWNERS = (
    "stockagent-shioaji-taifex-bidask.service",
    "stockagent-shioaji-top200.service",
    "stockagent-tw-day-trade-simulation.service",
    "stockagent-tw-overnight-simulation.service",
    "stockagent-shioaji-minute-backfill.service",
    "stockagent-shioaji-historical-market-data.service",
    "stockagent-shioaji-tx-history-backfill.service",
)
TIMERS = tuple(name.replace(".service", ".timer") for name in OWNERS[-3:])
TESTS = (
    "test/test_shioaji_sdk_maintenance.py",
    "test/test_shioaji_stock_quote_provider.py",
    "test/test_shioaji_volume_schema_compat.py",
    "test/test_shioaji_schedule.py",
    "test/test_shioaji_query_timing.py",
    "test/test_shioaji_historical_market_data.py",
    "test/test_shioaji_microstructure.py",
    "test/test_shioaji_tw_minute_kbars.py",
    "test/test_tw_stock_simulation_execution.py",
)
SOURCES = (
    "scripts/maintain_shioaji_sdk.py", "stockagent/live/quote_provider.py",
    "stockagent/live/tw_stock_simulation_execution.py",
    "stockagent/live/shioaji_schedule.py", "downloader/stock_volume_units.py",
    "stockagent/live/market_status.py",
    "downloader/download_shioaji_tw_kbars.py",
    "downloader/download_shioaji_tw_minute_kbars.py",
    "downloader/download_shioaji_historical_market_data.py",
    "downloader/stream_shioaji_tw_microstructure.py",
    "downloader/stream_shioaji_taifex_bidask.py",
)


def now() -> datetime:
    return datetime.now(timezone.utc)


def maintenance_window(value: datetime) -> bool:
    """Night session has ended; finish well before stock/FOP preopen preparation.

    Do not enlarge this window on guessed holidays. Persistent timer catch-up
    outside it checks versions only, never authorizes a daytime restart.
    """
    if value.tzinfo is None:
        raise ValueError("maintenance clock must be timezone-aware")
    return time(5, 15) <= value.astimezone(TAIPEI).time() < time(6, 30)


def sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def evidence(state: Path, name: str, payload: dict) -> None:
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(state / name, {**payload, "observed_at_utc": now().isoformat()})


@contextmanager
def update_lock(state: Path):
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (state / "maintenance.lock").open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def official_release(version: str | None = None) -> dict:
    if version is not None:
        version = str(Version(version))
    url = f"https://pypi.org/pypi/shioaji/{version}/json" if version else "https://pypi.org/pypi/shioaji/json"
    with urlopen(url, timeout=20) as response:
        return json.loads(response.read(16 << 20))


def select_wheel(release: dict, *, rollback: bool = False) -> dict:
    version = Version(release["info"]["version"])
    if not rollback and (version.is_prerelease or version.is_devrelease):
        raise ValueError("only official stable releases may be promoted")
    python_version = Version(".".join(map(str, sys.version_info[:3])))
    if python_version not in SpecifierSet(release["info"].get("requires_python") or ""):
        raise ValueError("release does not support this Python runtime")
    priorities = {tag: index for index, tag in enumerate(sys_tags())}
    candidates = []
    for row in release["urls"]:
        if row.get("packagetype") != "bdist_wheel" or (row.get("yanked") and not rollback):
            continue
        filename = row["filename"]
        if Path(filename).name != filename:
            raise ValueError("invalid wheel filename")
        name, wheel_version, _, tags = parse_wheel_filename(filename)
        parsed = urlparse(row["url"])
        if name != "shioaji" or wheel_version != version:
            raise ValueError("release and wheel identity differ")
        if parsed.scheme != "https" or parsed.netloc != "files.pythonhosted.org":
            raise ValueError("wheel must come from official PyPI storage over HTTPS")
        if python_version not in SpecifierSet(row.get("requires_python") or ""):
            continue
        compatible = tags & priorities.keys()
        if compatible:
            candidates.append((min(priorities[tag] for tag in compatible), row))
    if not candidates:
        raise ValueError("no non-yanked compatible wheel; source builds are not automatic")
    return min(candidates, key=lambda pair: pair[0])[1]


def download_wheel(row: dict, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    target = directory / row["filename"]
    expected = row["digests"]["sha256"]
    if target.exists() and sha(target) == expected:
        return target
    if not 0 < row["size"] <= 256 << 20:
        raise ValueError("unexpected wheel size")
    with tempfile.NamedTemporaryFile(dir=directory, prefix=".wheel-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            with urlopen(row["url"], timeout=30) as response:
                if urlparse(response.geturl()).netloc != "files.pythonhosted.org":
                    raise ValueError("unexpected wheel download redirect")
                total = 0
                while chunk := response.read(1 << 20):
                    total += len(chunk)
                    if total > row["size"]:
                        raise ValueError("wheel exceeds its advertised length")
                    stream.write(chunk)
            stream.flush()
            os.fsync(stream.fileno())
            if total != row["size"] or sha(temporary) != expected:
                raise ValueError("wheel length or SHA256 differs from PyPI")
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
    return target


def check_dependencies(wheel: Path, version: str) -> None:
    """Never upgrade unrelated CUDA/data dependencies to make a candidate pass."""
    with zipfile.ZipFile(wheel) as archive:
        entries = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
        if len(entries) != 1:
            raise ValueError("wheel must contain exactly one package metadata record")
        record = BytesParser().parsebytes(archive.read(entries[0]))
    if canonicalize_name(record["Name"]) != "shioaji" or Version(record["Version"]) != Version(version):
        raise ValueError("wheel metadata identity mismatch")
    for raw in record.get_all("Requires-Dist", []):
        requirement = Requirement(raw)
        if requirement.marker and not requirement.marker.evaluate({"extra": ""}):
            continue
        if requirement.url or Version(metadata.version(requirement.name)) not in requirement.specifier:
            raise ValueError(f"candidate needs a separate dependency review: {requirement.name}")


def execute(argv: list[str], log: Path, *, sdk_root: Path | None = None, timeout: int = 180) -> None:
    environment = dict(os.environ)
    # Tests/smoke cannot accidentally use an inherited live broker credential.
    for key in list(environment):
        if key.startswith(("SHIOAJI_", "SJ_")):
            environment.pop(key)
    if sdk_root:
        environment["PYTHONPATH"] = os.pathsep.join((str(sdk_root), str(ROOT)))
    environment["PYTHONUNBUFFERED"] = "1"
    with log.open("ab") as stream:
        result = subprocess.run(argv, cwd=ROOT, env=environment, stdout=stream,
                                stderr=subprocess.STDOUT, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"validation/installation failed ({result.returncode}); inspect {log}")


def fingerprint() -> str:
    records = {path: sha(ROOT / path) for path in (*SOURCES, *TESTS)}
    records["runtime"] = sys.executable
    records["python"] = sys.version
    # Dependency changes invalidate prior isolated compatibility acceptance.
    packages = []
    for item in metadata.distributions():
        record = item.metadata
        packages.append((record.get("Name"), record.get("Version")))
    records["packages"] = sorted(packages, key=lambda item: (str(item[0]), str(item[1])))
    return hashlib.sha256(json.dumps(records, sort_keys=True).encode()).hexdigest()


def probe(version: str) -> dict:
    import shioaji as sj
    if Version(sj.__version__) != Version(version) or Version(metadata.version("shioaji")) != Version(version):
        raise ValueError("native import and installed distribution versions disagree")
    api = sj.Shioaji(simulation=True)
    for name in ("login", "logout", "snapshots", "ticks", "kbars", "usage", "subscribe", "unsubscribe",
                 "set_event_callback", "set_order_callback", "set_on_quote_stk_v1_callback",
                 "set_on_tick_stk_v1_callback", "set_on_bidask_stk_v1_callback",
                 "set_on_tick_fop_v1_callback", "set_on_bidask_fop_v1_callback", "place_order"):
        if not callable(getattr(api, name, None)):
            raise ValueError(f"missing supported Python API: {name}")
    # Accessing api.contracts requires authentication. Inspect its exported
    # Python type instead; a compatibility probe must not open a broker slot.
    if not all(callable(getattr(sj.ContractsApi, name, None)) for name in ("get", "info")):
        raise ValueError("canonical ContractV2 interface unavailable")
    # Validate call signatures without executing network operations. A future
    # release that removes our callback/timeout/login parameters must fail here.
    calls = {
        "login": {"api_key": "not-used", "secret_key": "not-used", "subscribe_trade": False,
                  "force_refresh": True},
        "snapshots": {"contracts": [], "timeout": 0, "cb": lambda _: None},
        "ticks": {"contract": None, "date": "2000-01-01", "query_type": sj.TicksQueryType.RangeTime,
                  "time_start": "09:00:00", "time_end": "09:01:00", "timeout": 0, "cb": lambda _: None},
        "kbars": {"contract": None, "start": "2000-01-01", "end": "2000-01-01",
                  "timeout": 0, "cb": lambda _: None},
        "subscribe": {"contract": None, "quote_type": sj.QuoteType.Quote},
        "place_order": {"contract": None, "order": None, "timeout": 0},
    }
    for name, arguments in calls.items():
        inspect.signature(getattr(api, name)).bind(**arguments)
    # Construct only: absolutely no login or place_order invocation.
    sj.StockOrder(action=sj.Action.Buy, price=0., quantity=1, price_type=sj.StockPriceType.MKT,
                  order_type=sj.OrderType.IOC, order_lot=sj.StockOrderLot.Common,
                  order_cond=sj.StockOrderCond.Cash)
    native = next(Path(sj.__file__).parent.glob("_core*.so"))
    return {"version": sj.__version__, "native_sha256": sha(native), "native_path": str(native),
            "login_performed": False, "order_submitted": False}


def stage(release: dict, state: Path) -> dict:
    version = release["info"]["version"]
    row = select_wheel(release)
    wheel = download_wheel(row, state / "wheels")
    check_dependencies(wheel, version)
    key = fingerprint()
    directory = state / "validated" / f"{version}-{row['digests']['sha256'][:16]}-{key[:16]}"
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    receipt = directory / "acceptance.json"
    if receipt.exists():
        existing = json.loads(receipt.read_text())
        if existing["fingerprint"] == key and existing["wheel_sha256"] == sha(wheel):
            return existing
    sdk_root = Path(tempfile.mkdtemp(dir=directory, prefix="sdk-"))
    execute([sys.executable, "-m", "pip", "--isolated", "install", "--no-deps", "--no-index",
             "--disable-pip-version-check", "--target", str(sdk_root), str(wheel)], directory / "install.log")
    execute([sys.executable, str(Path(__file__).resolve()), "probe", "--expected", version],
            directory / "probe.log", sdk_root=sdk_root, timeout=45)
    execute([sys.executable, "-m", "pytest", "-q", "-s", *TESTS], directory / "tests.log",
            sdk_root=sdk_root, timeout=600)
    if fingerprint() != key:
        raise ValueError("source or dependencies changed during validation; retry the new generation")
    accepted = {"state": "validated_candidate", "version": version, "fingerprint": key,
                "wheel": str(wheel), "wheel_sha256": sha(wheel), "sdk_root": str(sdk_root),
                "tests": list(TESTS), "tests_log": str(directory / "tests.log"),
                "native_probe_log": str(directory / "probe.log"), "broker_login_performed": False}
    evidence(directory, "acceptance.json", accepted)
    return accepted


def native_users(proc: Path = Path("/proc"), prefix: Path | None = None) -> list[dict]:
    prefix = prefix or Path(sys.prefix)
    rows = []
    for path in proc.iterdir():
        if not path.name.isdigit() or int(path.name) == os.getpid():
            continue
        try:
            mapped = [line.split(maxsplit=5) for line in (path / "maps").read_text().splitlines()
                      if "/shioaji/" in line and str(prefix) + "/" in line]
            if not mapped:
                continue
            group = (path / "cgroup").read_text()
            unit = next((part for part in group.strip().split("/") if part.endswith(".service")), None)
            loaded = []
            for line in mapped:
                filename = line[5].removesuffix(" (deleted)")
                try:
                    current = str(Path(filename).stat().st_ino) == line[4] and not line[5].endswith(" (deleted)")
                except FileNotFoundError:
                    current = False
                loaded.append({"path": filename, "inode": line[4], "matches_disk": current})
            rows.append({"pid": int(path.name), "unit": unit,
                         "matches_installed_native": all(item["matches_disk"] for item in loaded)})
        except (FileNotFoundError, ProcessLookupError):
            continue
    return rows


def systemctl(action: str, units: list[str]) -> None:
    if not units:
        return
    result = subprocess.run(["systemctl", action, *units], capture_output=True, timeout=240)
    if result.returncode:
        raise RuntimeError(f"systemctl {action} failed for reviewed SDK owners")


def active(unit: str) -> bool:
    return subprocess.run(["systemctl", "is-active", "--quiet", unit], timeout=10).returncode == 0


def restore_units(journal: dict) -> None:
    # Start only previously active owners/timers. Preserve intentionally stopped ones.
    for unit in journal["active_owners"]:
        systemctl("start", [unit])
    systemctl("start", journal["active_timers"])
    if not all(active(unit) for unit in (*journal["active_owners"], *journal["active_timers"])):
        raise RuntimeError("an original SDK owner did not restart; business readiness remains separate")


def recover(state: Path) -> dict | None:
    path = state / "transaction.json"
    if not path.exists():
        return None
    journal = json.loads(path.read_text())
    if journal["phase"] in ("complete", "recovered"):
        return None
    if journal["python"] != sys.executable:
        raise ValueError("recovery must use the exact original runtime")
    if not set(journal["active_owners"]) <= set(OWNERS) or not set(journal["active_timers"]) <= set(TIMERS):
        raise ValueError("recovery journal contains an unreviewed service")
    # Recovery is deliberately allowed outside the promotion window: restore
    # interrupted installation rather than leave trading startup broken.
    systemctl("stop", journal["active_timers"])
    systemctl("stop", journal["active_owners"])
    if native_users() and journal["phase"] != "stopping":
        raise RuntimeError("SDK is loaded by an unexpected process; cannot safely recover automatically")
    if journal["phase"] != "stopping":
        wheel = Path(journal["rollback_wheel"])
        if sha(wheel) != journal["rollback_sha256"]:
            raise ValueError("rollback wheel changed")
        execute([sys.executable, "-m", "pip", "--isolated", "install", "--no-deps", "--no-index",
                 "--disable-pip-version-check", "--force-reinstall", str(wheel)], state / "rollback.log")
        execute([sys.executable, str(Path(__file__).resolve()), "probe", "--expected", journal["old_version"]],
                state / "rollback-probe.log", timeout=45)
    restore_units(journal)
    journal["phase"] = "recovered"
    evidence(state, "transaction.json", journal)
    return {"state": "recovered_previous_sdk", "installed_version": metadata.version("shioaji")}


def promote(candidate: dict, rollback: Path, state: Path) -> dict:
    if not maintenance_window(now()):
        raise ValueError("SDK replacement requires the non-trading maintenance window")
    if candidate["fingerprint"] != fingerprint() or sha(Path(candidate["wheel"])) != candidate["wheel_sha256"]:
        raise ValueError("candidate validation generation changed")
    original_native = native_users()
    if any(item["unit"] not in OWNERS for item in original_native):
        return {"state": "waiting_unmanaged_sdk_owner", "native_owners": original_native}
    journal = {"phase": "stopping", "python": sys.executable,
               "old_version": metadata.version("shioaji"), "new_version": candidate["version"],
               "rollback_wheel": str(rollback), "rollback_sha256": sha(rollback),
               "candidate_wheel_sha256": candidate["wheel_sha256"],
               "active_owners": [unit for unit in OWNERS if active(unit)],
               "active_timers": [unit for unit in TIMERS if active(unit)],
               "original_native_owners": original_native}
    evidence(state, "transaction.json", journal)
    try:
        systemctl("stop", journal["active_timers"])
        systemctl("stop", journal["active_owners"])
        if native_users():
            raise RuntimeError("a native owner remains after graceful stop")
        if not maintenance_window(now()):
            raise ValueError("maintenance window expired before replacement")
        journal["phase"] = "installing"
        evidence(state, "transaction.json", journal)
        execute([sys.executable, "-m", "pip", "--isolated", "install", "--no-deps", "--no-index",
                 "--disable-pip-version-check", "--force-reinstall", candidate["wheel"]], state / "upgrade.log")
        execute([sys.executable, str(Path(__file__).resolve()), "probe", "--expected", candidate["version"]],
                state / "installed-probe.log", timeout=45)
        journal["phase"] = "restarting"
        evidence(state, "transaction.json", journal)
        restore_units(journal)
        users = native_users()
        if any(not item["matches_installed_native"] for item in users):
            raise ValueError("a service still holds the previous native SDK")
        journal["phase"] = "complete"
        evidence(state, "transaction.json", journal)
        return {"state": "updated", "installed_version": candidate["version"],
                "native_owners": users, "restarted_owners": journal["active_owners"],
                "quote_login_and_market_readiness": "verify_via_existing_owner_receipts"}
    except Exception:
        recover(state)
        raise


def maintain(state: Path, *, apply: bool = False, validate: bool = False) -> dict:
    with update_lock(state):
        recovered = None
        journal = state / "transaction.json"
        if journal.exists() and json.loads(journal.read_text())["phase"] not in ("complete", "recovered"):
            with control_lease(owner="shioaji-sdk-maintenance", timeout_seconds=0.):
                recovered = recover(state)
        installed = metadata.version("shioaji")
        release = official_release()
        row = select_wheel(release)
        version = release["info"]["version"]
        users = native_users()
        stale_native = any(not item["matches_installed_native"] for item in users)
        result = {"installed_version": installed, "available_version": version,
                  "python": sys.executable, "official_source": "https://pypi.org/project/shioaji/",
                  "maintenance_window_taipei": "05:15..06:30", "native_owners": users,
                  "state": ("loaded_version_mismatch" if stale_native else "latest")
                           if Version(installed) >= Version(version) else "update_available"}
        if recovered:
            result["recovery"] = recovered
        if Version(version) > Version(installed) or (Version(version) == Version(installed) and stale_native):
            if validate or (apply and maintenance_window(now())):
                candidate = stage(release, state)
                # Retain a verified rollback wheel BEFORE stopping any owner.
                old = select_wheel(official_release(installed), rollback=True)
                rollback = download_wheel(old, state / "wheels")
                result.update(state="validated_waiting_window", candidate=candidate,
                              rollback_wheel=str(rollback), rollback_sha256=sha(rollback))
                if apply and maintenance_window(now()):
                    with control_lease(owner="shioaji-sdk-maintenance", timeout_seconds=0.):
                        result.update(promote(candidate, rollback, state))
            else:
                result["candidate_wheel_sha256"] = row["digests"]["sha256"]
        result["installed_version"] = metadata.version("shioaji")
        evidence(state, "status.json", result)
        evidence(state / "checks", f"{now().strftime('%Y%m%dT%H%M%SZ')}.json", result)
        return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("maintain", "probe", "recover"))
    parser.add_argument("--state-dir", type=Path, default=STATE)
    parser.add_argument("--expected")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.action == "probe":
            if not args.expected:
                parser.error("probe requires --expected")
            result = probe(args.expected)
        elif args.action == "recover":
            with update_lock(args.state_dir):
                journal = args.state_dir / "transaction.json"
                if journal.exists() and json.loads(journal.read_text())["phase"] not in ("complete", "recovered"):
                    with control_lease(owner="shioaji-sdk-recovery"):
                        result = recover(args.state_dir)
                else:
                    result = {"state": "no_interrupted_update"}
        else:
            result = maintain(args.state_dir, apply=args.apply, validate=args.validate)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0
    except BlockingIOError:
        print(json.dumps({"state": "another_sdk_maintenance_owner_active"}), flush=True)
        return 0
    except Exception as error:
        result = {"state": "failed", "error_type": type(error).__name__, "reason": str(error)}
        evidence(args.state_dir, "failure.json", result)
        evidence(args.state_dir / "failures", f"{now().strftime('%Y%m%dT%H%M%SZ')}-{os.getpid()}.json", result)
        if args.action != "probe":
            # Do not let a stale successful check hide a subsequent rejected
            # candidate or unsuccessful recovery. Historical acceptance stays.
            evidence(args.state_dir, "status.json", result)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
