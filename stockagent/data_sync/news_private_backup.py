"""Fixed personal news copies through the existing paired backup owner.

The news publication flags keep their original meaning. A separate exact policy
records the owner's later private-copy instruction; it never opens public lake
discovery or changes a publisher license. Parquet and snapshot chunks retain
their original bytes and paths, including references to older generations.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import time

from scripts.backup_delivery_receipt import read_json, validate_ack, validate_readiness
from scripts.verify_backup_delivery import CONTRACT, HASH, identity, regular, verify
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_bytes
from stockagent.data_sync.offhost_backup import _regular, _sha, copy_verified_bytes, private_json

POLICY = "stockagent.news_private_backup_policy.v1"
PLAN = "stockagent.fixed_private_news_backup.v1"
PAIRED = ("producer_device_id", "receiver_device_id", "repository_id")
# Smaller closed waves let the existing receiver overlap transfer, backup and
# restore without waiting for a multi-GiB first publication. This is a target
# below the relay's hard cap, not another source rate/quota limit.
PRIVATE_WAVE_BYTES = 512 * 1024**2


class NewsCapacityDeferred(SnapshotError):
    """The fixed news plan is intact; retry the original publisher later."""


def encoded(value: dict) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2,
                       allow_nan=False) + "\n").encode()


def signature(path: Path) -> list[int]:
    info = _regular(path).stat()
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


def fixed_bytes(path: Path, *, maximum: int = 32 * 1024**2) -> bytes:
    before = signature(path)
    if before[2] > maximum:
        raise ValueError("news backup control exceeds its bounded size")
    raw = path.read_bytes()
    if signature(path) != before:
        raise ValueError("news backup control changed during read")
    return raw


def validate_policy(policy: dict, configuration: dict) -> None:
    if (policy.get("contract") != POLICY or policy.get("private_backup_authorized") is not True
            or policy.get("public_publish_allowed") is not False
            or policy.get("automatic_training_injection_allowed") is not False
            or policy.get("source_licenses_changed") is not False
            or policy.get("source_root") != "data_news"
            or policy.get("route") != "existing_paired_encrypted_Restic_relay"
            or not policy.get("user_instruction") or not policy.get("user_instruction_date")
            or any(policy.get(key) != configuration[key] for key in PAIRED)):
        raise ValueError("private news copy requires an exact explicit paired policy")


def capture_plan(repository: Path, policy: dict, configuration: dict) -> dict:
    """Caller holds the existing news builder shared lock throughout capture."""
    validate_policy(policy, configuration)
    root = repository / policy["source_root"]
    manifest_path = regular(root, policy["generation"] + "/manifest.json")
    raw = fixed_bytes(manifest_path)
    manifest = json.loads(raw)
    if (hashlib.sha256(raw).hexdigest() != policy["manifest_sha256"]
            or manifest.get("generation") != policy["generation"]
            or manifest.get("state") != "ready"
            or manifest.get("storage_layout") != "news_incremental_parts_v2"
            or manifest.get("contract_version") != 2):
        raise ValueError("fixed original news manifest differs from its policy")

    members = {}

    def add(source_relative, target_relative, role, sha=None, nbytes=None):
        source = regular(root, source_relative)
        before = signature(source)
        if nbytes is not None and before[2] != nbytes:
            raise ValueError("news source size differs from original inventory")
        digest = sha or _sha(source)
        if signature(source) != before:
            raise ValueError("news source changed during capture")
        row = {"source_relative": source_relative, "relative": "data_news/" + target_relative,
               "role": role, "sha256": digest, "bytes": before[2], "source_signature": before}
        previous = members.get(row["relative"])
        if previous is not None and previous != row:
            raise ValueError("conflicting duplicate original news member")
        members[row["relative"]] = row

    # The immutable generation manifest supplies the normal reader's root
    # pointer without observing or rewriting a moving latest pointer.
    add(policy["generation"] + "/manifest.json", "manifest.json", "original_manifest",
        policy["manifest_sha256"], len(raw))
    for name in ("generation.json", "snapshot.json", "verification.json", "quality_report.json"):
        relative = policy["generation"] + "/" + name
        if (root / relative).exists():
            add(relative, relative, "original_control")
        elif name == "generation.json":
            raise ValueError("news reader requires its original generation marker")
    for part in manifest["parts"]:
        # Strict unresolved-path checks precede the existing path resolver.
        from stockagent.data.news_dataset_storage import news_part_path
        path = regular(root, part["path"])
        if news_part_path(root, manifest, part) != path:
            raise ValueError("news part resolver differs from the fixed inventory")
        add(part["path"], part["path"], "original_table_part", part["sha256"], part["bytes"])

    snapshot = manifest["source_snapshot"]
    from stockagent.data.news_dataset_snapshots import verify_snapshot
    generation = regular(root, policy["generation"] + "/generation.json").parent
    verify_snapshot(root, generation, snapshot)
    if "snapshot_storage" in snapshot:
        for chunk in snapshot["snapshot_storage"]["chunks"]:
            path = regular(root, chunk["path"])
            whole_file = chunk["offset"] == 0 and path.stat().st_size == chunk["bytes"]
            add(chunk["path"], chunk["path"], "original_source_snapshot",
                chunk["sha256"] if whole_file else None,
                chunk["bytes"] if whole_file else None)
    else:
        relative = policy["generation"] + "/" + snapshot["snapshot_path"]
        add(relative, relative, "original_source_snapshot", snapshot["snapshot_sha256"])
    ordered = sorted(members.values(), key=lambda row: (
        {"original_manifest": 0, "original_control": 1, "original_table_part": 2,
         "original_source_snapshot": 3}[row["role"]], row["relative"]))
    body = {"contract": PLAN, "policy": policy, "policy_identity_sha256": identity(policy),
            "generation": manifest["generation"], "manifest_sha256": policy["manifest_sha256"],
            "row_counts": manifest["row_counts"], "source_files": ordered,
            "files": len(ordered), "source_bytes": sum(row["bytes"] for row in ordered),
            "public_publish_allowed": False, "source_cleanup_authorized": False,
            "automatic_training_injection_allowed": False, "live_database_copied": False}
    return {**body, "identity_sha256": identity(body)}


def validate_plan(plan: dict, policy: dict, configuration: dict) -> None:
    validate_policy(policy, configuration)
    if (plan.get("contract") != PLAN or plan.get("policy") != policy
            or plan.get("policy_identity_sha256") != identity(policy)
            or plan.get("identity_sha256") != identity({k: v for k, v in plan.items() if k != "identity_sha256"})
            or plan.get("source_cleanup_authorized") is not False
            or plan.get("public_publish_allowed") is not False
            or plan.get("automatic_training_injection_allowed") is not False
            or plan.get("live_database_copied") is not False
            or plan.get("files") != len(plan["source_files"])
            or plan.get("source_bytes") != sum(row["bytes"] for row in plan["source_files"])):
        raise ValueError("fixed private news plan or preservation policy differs")
    targets = [row["relative"] for row in plan["source_files"]]
    if len(targets) != len(set(targets)):
        raise ValueError("duplicate private news targets")
    for row in plan["source_files"]:
        for name in ("relative", "source_relative"):
            text = row[name]
            path = PurePosixPath(text)
            if (not isinstance(text, str) or not path.parts or path.is_absolute()
                    or path.as_posix() != text or ".." in path.parts
                    or any(c in text for c in "\\:\r\n\0")):
                raise ValueError("private news member path is unsafe")
        if (not row["relative"].startswith("data_news/")
                or not HASH.fullmatch(row["sha256"])
                or type(row["bytes"]) is not int or row["bytes"] < 0
                or len(row["source_signature"]) != 5
                or any(type(n) is not int or n < 0 for n in row["source_signature"])
                or row["source_signature"][2] != row["bytes"]):
            raise ValueError("private news original member identity is invalid")


def deliveries(plan: dict, *, maximum_bytes: int, maximum_files: int) -> list[dict]:
    """Partition once; each original file appears in exactly one closed wave."""
    maximum_bytes = min(maximum_bytes, PRIVATE_WAVE_BYTES)
    if maximum_files < 4 or maximum_bytes < 2 * 1024**2:
        raise ValueError("private news wave limit is too small")
    prefix = "private-news/" + plan["identity_sha256"] + "/"
    plan_raw = encoded(plan)
    groups, current, size = [], [], len(plan_raw)
    for row in plan["source_files"]:
        if row["bytes"] + 1024**2 >= maximum_bytes:
            raise ValueError("one original news member exceeds the closed wave limit")
        if current and (len(current) >= maximum_files - 3 or size + row["bytes"] + 1024**2 >= maximum_bytes):
            groups.append(current)
            current, size = [], 0
        current.append(row)
        size += row["bytes"]
    if current:
        groups.append(current)
    output = []
    for ordinal, group in enumerate(groups):
        controls = {prefix + "backup-plan.json": plan_raw} if ordinal == 0 else {}
        rows = [{"relative": prefix + row["relative"], "sha256": row["sha256"], "bytes": row["bytes"]}
                for row in group]
        rows += [{"relative": name, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
                 for name, raw in controls.items()]
        body = {"contract": CONTRACT, "source_plan_identity_sha256": plan["identity_sha256"],
                "selection": "fixed_owner_authorized_private_news_copy", "files": rows,
                "all_history_verified": False, "unpublished_sources_included": True,
                "public_publish_allowed": False, "ordinal": ordinal, "total_parts": len(groups)}
        envelope = {**body, "identity_sha256": identity(body)}
        complete = sum(row["bytes"] for row in rows) + len(encoded(envelope)) + 65
        if len(encoded(envelope)) > 1024**2 or complete > maximum_bytes or len(rows) + 2 > maximum_files:
            raise ValueError("private news exact envelope exceeds relay admission")
        output.append({"ordinal": ordinal, "members": group, "controls": controls,
                       "envelope": envelope, "files": rows, "complete_bytes": complete})
    return output


def copy_member(source: Path, destination: Path, row: dict, *, verified_signature=None) -> bool:
    """Resume only a fully verified existing file; never overwrite a partial."""
    if signature(source) != row["source_signature"]:
        raise ValueError("fixed news source generation changed")
    if destination.exists() or destination.is_symlink():
        if verified_signature is not None:
            if signature(destination) != verified_signature:
                raise ValueError("verified private news staging generation changed")
            return False
        if signature(destination)[2] != row["bytes"] or _sha(destination) != row["sha256"]:
            raise ValueError("preserve mismatched private news staging")
        if signature(source) != row["source_signature"]:
            raise ValueError("fixed news source changed during staging readback")
        return False
    copy_verified_bytes(source, destination, expected_sha256=row["sha256"], expected_bytes=row["bytes"])
    if signature(source) != row["source_signature"]:
        raise ValueError("fixed news source changed during copy")
    return True


@contextmanager
def original_owner(queue, *, wait_seconds: int):
    path = queue.state / "owner.lock"
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError("original backup owner path is redirected")
    with path.open("a") as owner:
        acquire_owner(owner, wait_seconds=wait_seconds)
        yield owner


def acquire_owner(owner, *, wait_seconds: int):
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise BlockingIOError("original backup owner busy; retain this fixed plan and retry")
            time.sleep(.5)


def reconcile_plan(queue, plan: dict, progress: Path, *, wait_seconds: int = 1200) -> dict:
    """Reconcile fixed file ACKs without recopying or scanning source payloads."""
    waves = deliveries(plan, maximum_bytes=queue.config['maximum_batch_bytes'],
                       maximum_files=queue.config['maximum_batch_files'])
    with original_owner(queue, wait_seconds=wait_seconds):
        queue.storage_guard()
        ledger = queue.load_ledger()
        queue.recover_publications(ledger)
        errors = queue.ingest(ledger)
        queue._save_ledger(ledger)
        accepted, published = [], []
        for wave in waves:
            key = wave['envelope']['identity_sha256']
            record = ledger['deliveries'].get(key)
            if record is None:
                continue
            dispatch = record['dispatch']
            if (dispatch['catalog_identity_sha256'] != plan['identity_sha256']
                    or dispatch['delivery_kind'] != 'private_personal_news_backup'
                    or dispatch['complete_bytes'] != wave['complete_bytes']
                    or dispatch['complete_files'] != len(wave['files']) + 2):
                raise ValueError('news ACK reconciliation has a different fixed delivery')
            published.append(wave)
            if record.get('acceptance'):
                validate_ack(record['acceptance'], dispatch)
                accepted.append(key)
        result = {'contract': 'stockagent.private_news_backup_progress.v1',
                  'plan_identity_sha256': plan['identity_sha256'], 'total_parts': len(waves),
                  'published_parts': [{'ordinal': w['ordinal'],
                     'envelope_identity_sha256': w['envelope']['identity_sha256'],
                     'complete_bytes': w['complete_bytes']} for w in published],
                  'published_source_files': sum(len(w['members']) for w in published),
                  'published_source_bytes': sum(r['bytes'] for w in published for r in w['members']),
                  'source_files': plan['files'], 'source_bytes': plan['source_bytes'],
                  'accepted_parts': len(accepted), 'NAS_restore_ACK_verified': len(accepted) == len(waves),
                  'source_files_deleted': 0, 'automatic_training_started': False, 'receipt_errors': errors,
                  'state': 'paired_news_NAS_file_ACK_verified' if len(accepted) == len(waves)
                           else 'closed_news_parts_published_waiting_NAS_ACK',
                  'observed_at_utc': datetime.now(timezone.utc).isoformat()}
        if progress.exists() and json.loads(fixed_bytes(progress)).get('plan_identity_sha256') != plan['identity_sha256']:
            raise ValueError('preserve another fixed news progress identity')
        private_json(progress, result)
        return result


def publish_plan(queue, repository: Path, plan: dict, progress: Path, *, wait_seconds: int = 1200,
                 yield_owner_between_waves: bool = False) -> dict:
    """Bounded file workers; serial closed publication; transfer overlaps NAS."""
    if type(yield_owner_between_waves) is not bool:
        raise ValueError('news owner handoff must be an explicit boolean')
    validate_plan(plan, plan["policy"], queue.config)
    waves = deliveries(plan, maximum_bytes=queue.config["maximum_batch_bytes"],
                       maximum_files=queue.config["maximum_batch_files"])
    root = repository / plan["policy"]["source_root"]
    started = time.monotonic()
    state = {"contract": "stockagent.private_news_backup_progress.v1", "plan_identity_sha256": plan["identity_sha256"],
             "source_files": plan["files"], "source_bytes": plan["source_bytes"], "total_parts": len(waves),
             "published_parts": [], "published_source_files": 0, "published_source_bytes": 0,
             "NAS_restore_ACK_verified": False, "source_files_deleted": 0,
             "automatic_training_started": False, "state": "waiting_original_backup_owner"}

    def save(**fields):
        state.update(fields, elapsed_seconds=round(time.monotonic() - started, 3),
                     observed_at_utc=datetime.now(timezone.utc).isoformat())
        private_json(progress, state)

    if progress.exists():
        previous = json.loads(fixed_bytes(progress))
        if previous.get("plan_identity_sha256") != plan["identity_sha256"]:
            raise ValueError("preserve a different private news progress identity")
        for field in ('published_parts', 'published_source_files', 'published_source_bytes'):
            if field in previous:
                state[field] = previous[field]
    save()
    notifications = []
    with ThreadPoolExecutor(max_workers=queue.pipeline.get("copy_workers", 1), thread_name_prefix="news-copy") as copier, \
            ThreadPoolExecutor(max_workers=1, thread_name_prefix="news-notify") as scanner:
        with original_owner(queue, wait_seconds=wait_seconds) as owner:
            queue.storage_guard()
            ledger = queue.load_ledger()
            queue.recover_publications(ledger)
            errors = queue.ingest(ledger)
            queue._save_ledger(ledger)
            if errors:
                raise ValueError("paired backup receipt errors remain; preserve source")
            # Rebuild progress from committed original-owner records, rather
            # than resetting completed work to zero during an interrupted run.
            counted = set()
            state.update(published_parts=[], published_source_files=0, published_source_bytes=0)
            for prior_wave in waves:
                prior_key = prior_wave['envelope']['identity_sha256']
                if prior_key not in ledger['deliveries']:
                    continue
                prior_dispatch = ledger['deliveries'][prior_key]['dispatch']
                if (prior_dispatch.get('catalog_identity_sha256') != plan['identity_sha256']
                        or prior_dispatch.get('delivery_kind') != 'private_personal_news_backup'):
                    raise ValueError('private news committed progress binding differs')
                state['published_parts'].append({'ordinal': prior_wave['ordinal'],
                    'envelope_identity_sha256': prior_key, 'complete_bytes': prior_dispatch['complete_bytes']})
                state['published_source_files'] += len(prior_wave['members'])
                state['published_source_bytes'] += sum(r['bytes'] for r in prior_wave['members'])
                counted.add(prior_key)
            ready = read_json(queue.receipts / "readiness.json")
            validate_readiness(ready, queue.config, admission_only=True)
            if max(wave["complete_bytes"] for wave in waves) > ready["maximum_batch_bytes"]:
                raise ValueError("news wave exceeds the original receiver admission")
            save(state="checking_original_transport_capacity")
            usage = queue.transport_usage()
            unsubmitted = [wave for wave in waves if wave["envelope"]["identity_sha256"] not in ledger["deliveries"]]
            total_new = sum(wave["complete_bytes"] for wave in unsubmitted)
            pending = sum(row["dispatch"]["complete_bytes"] for row in ledger["deliveries"].values() if not row.get("acceptance"))
            if total_new > queue.staging_room(usage) or pending + total_new > queue.config["maximum_pending_bytes"]:
                raise NewsCapacityDeferred("private news bounded staging/pending/physical reserve admission is full")
            # Admission is protected by the same owner; no second capacity
            # writer can race the aggregate snapshot reservation.
            save(state="private_news_capacity_admitted", new_bytes_reserved=total_new, existing_pending_bytes=pending)
            for wave in waves:
                key = wave["envelope"]["identity_sha256"]
                newly_published = key not in ledger['deliveries']
                if key not in ledger["deliveries"]:
                    if yield_owner_between_waves:
                        # Another original-owner turn may have consumed room
                        # while this publisher yielded. Re-admit the complete
                        # remaining fixed plan before allocating another wave.
                        remaining = sum(w['complete_bytes'] for w in waves
                                        if w['envelope']['identity_sha256'] not in ledger['deliveries'])
                        pending = sum(d['dispatch']['complete_bytes'] for d in ledger['deliveries'].values()
                                      if not d.get('acceptance'))
                        if (remaining > queue.staging_room(queue.transport_usage())
                                or pending + remaining > queue.config['maximum_pending_bytes']):
                            save(state='waiting_original_transport_capacity')
                            raise NewsCapacityDeferred('private news remaining fixed plan needs transport room')
                    staging = queue.transport / ".staging" / ("private-news-" + key)
                    if any(p.is_symlink() for p in (staging, *staging.parents)):
                        raise ValueError("private news staging is redirected")
                    staging.mkdir(mode=0o700, parents=True, exist_ok=True)
                    prefix = "private-news/" + plan["identity_sha256"] + "/"
                    resume_signatures = {}
                    if (staging / "READY").exists():
                        save(state="full_native_resume_staging_readback", current_part=wave["ordinal"])
                        # A previous interruption at the read-only publication
                        # barrier left a complete uncommitted wave. Verify it
                        # once, then reuse only these exact fresh signatures;
                        # the final canonical publication verifies it again.
                        resumed = verify(staging, key, include_file_signatures=True,
                                         workers=queue.pipeline.get("verify_workers", 1), native_windows_io=True)
                        resume_signatures = resumed["file_signatures"]
                    save(state="copying_and_hashing_original_news", current_part=wave["ordinal"])
                    def copy(row):
                        relative = prefix + row["relative"]
                        return copy_member(regular(root, row["source_relative"]), staging / relative, row,
                                           verified_signature=resume_signatures.get(relative))
                    # map preserves deterministic progress while copying only
                    # bounded 8 MiB blocks in each existing copy primitive.
                    for number, _ in enumerate(copier.map(copy, wave["members"]), 1):
                        if number % 64 == 0:
                            save(current_part_copied_files=number)
                    for relative, raw in {**wave["controls"], "backup-envelope.json": encoded(wave["envelope"]),
                                          "READY": (key + "\n").encode()}.items():
                        destination = staging / relative
                        if destination.exists():
                            if fixed_bytes(destination) != raw:
                                raise ValueError("preserve mismatched private news control")
                        else:
                            atomic_write_bytes(destination, raw, mode=0o600)
                    rows = [{**row, "relative": "private/" + row["relative"], "role": "private_news"} for row in wave["files"]]
                    save(state="independent_destination_SHA_before_publication")
                    queue.publish_closed(ledger, rows, staging, key, plan["identity_sha256"], "private_personal_news_backup",
                                         native_windows_io=True)
                    queue._save_ledger(ledger)
                prior = ledger["deliveries"][key]
                if (prior["dispatch"].get("catalog_identity_sha256") != plan["identity_sha256"]
                        or prior["dispatch"].get("delivery_kind") != "private_personal_news_backup"):
                    raise ValueError("private news publication binding differs")
                frozen = {"deliveries": {key: json.loads(json.dumps(prior))}}
                notifications.append(scanner.submit(queue.scan_transport, frozen))
                if key not in counted:
                    state["published_parts"].append({"ordinal": wave["ordinal"], "envelope_identity_sha256": key,
                                                    "complete_bytes": prior["dispatch"]["complete_bytes"]})
                    state['published_source_files'] += len(wave['members'])
                    state['published_source_bytes'] += sum(row['bytes'] for row in wave['members'])
                    counted.add(key)
                save(state="closed_news_part_published")
                print(json.dumps({k: state[k] for k in ("state", "total_parts", "published_source_files", "published_source_bytes", "elapsed_seconds")}), flush=True)
                if yield_owner_between_waves and newly_published and wave['ordinal'] != waves[-1]['ordinal']:
                    queue._save_ledger(ledger)
                    save(state='yielding_original_backup_owner', current_part=wave['ordinal'])
                    fcntl.flock(owner, fcntl.LOCK_UN)
                    # Let blocked original controllers ingest ACKs/GC and
                    # publish their bounded work before reacquiring the owner.
                    time.sleep(.05)
                    acquire_owner(owner, wait_seconds=wait_seconds)
                    queue.storage_guard()
                    ledger = queue.load_ledger()
                    queue.recover_publications(ledger)
                    errors = queue.ingest(ledger)
                    queue._save_ledger(ledger)
                    if errors:
                        raise ValueError('paired receipt errors after news owner handoff; preserve source')
            for future in notifications:
                if future.done() and future.result() is not None:
                    queue.record_transport_scan(ledger, future.result())
            errors = queue.ingest(ledger)
            queue._save_ledger(ledger)
            accepted = []
            for wave in waves:
                record = ledger["deliveries"][wave["envelope"]["identity_sha256"]]
                if record.get("acceptance"):
                    validate_ack(record["acceptance"], record["dispatch"])
                    accepted.append(record["acceptance"]["identity_sha256"])
            save(state=("paired_news_NAS_file_ACK_verified" if len(accepted) == len(waves)
                        else "closed_news_parts_published_waiting_NAS_ACK"), NAS_restore_ACK_verified=len(accepted) == len(waves),
                 accepted_parts=len(accepted), receipt_errors=errors)
        # Never hold the source owner while waiting on Syncthing/NAS. Only
        # fixed notification requests remain after the publication barrier.
        save(transport_notifications=[future.result() for future in notifications])
    return state
