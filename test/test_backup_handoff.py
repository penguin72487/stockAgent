"""Frozen receiver package completeness and isolated importability."""
import hashlib
import fcntl
from pathlib import Path
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
import time

import pytest

from scripts.publish_backup_handoff import FILES, PIPELINE_FILES, publish_handoff
from scripts.verify_backup_delivery import verify
from stockagent.data_sync.backup_stream import BackupStream


def test_news_copy_handoff_is_closed_control_only_and_never_changes_worker(handoff_queue):
    import json
    from scripts.publish_backup_handoff import NEWS_PRIVATE_FILES, NEWS_PRIVATE_NAME
    queue = handoff_queue
    proof = publish_handoff(queue, NEWS_PRIVATE_NAME, physical_transport_alias=queue.transport)
    root = Path(proof['source_path'])
    manifest = json.loads((root / 'handoff-manifest.json').read_bytes())
    assert {row['relative'] for row in manifest['files']} == set(NEWS_PRIVATE_FILES)
    assert not manifest['replace_existing_worker']
    assert not manifest['automatic_execution_from_syncthing']
    assert not queue.ledger_path.exists()
    code = ('import sys;sys.path.insert(0,sys.argv[1]);'
            'import scripts.verify_backup_delivery;import scripts.backup_delivery_receipt;'
            'assert not any(n in sys.modules for n in ("torch","pandas","pyarrow","polars"))')
    subprocess.run([sys.executable, '-I', '-B', '-c', code, str(root)], cwd=root.parent, check=True)
    verify(root, proof['envelope_identity_sha256'])


def test_news_copy_scope_is_available_from_the_existing_cli():
    from scripts.publish_backup_handoff import NEWS_PRIVATE_NAME
    script = Path(__file__).resolve().parents[1] / 'scripts/publish_backup_handoff.py'
    result = subprocess.run([sys.executable, str(script), '--help'], capture_output=True, text=True, check=True)
    assert NEWS_PRIVATE_NAME in result.stdout


@pytest.fixture
def handoff_queue(tmp_path):
    root = tmp_path / "transport"
    root.mkdir()
    (root / ".stfolder").mkdir()
    (root / ".stignore").write_text("(?d).staging\n(?d).staging/**\n")
    config = {"schema_version": 1, "cold_root": str(tmp_path / "cold"), "transport_root": str(root),
        "receipt_root": str(tmp_path / "receipts"), "state_root": str(tmp_path / "state"),
        "required_mounts": {}, "require_transport_mount": False, "producer_device_id": "source-test",
        "receiver_device_id": "receiver-test", "repository_id": "a" * 64, "automatic_pruning": False,
        "automatic_batch_deletion": False, "source_cleanup_authorized": False,
        **{k: 64 * 1024**2 for k in ("maximum_batch_bytes", "maximum_batch_files", "maximum_pending_bytes",
                                   "maximum_pending_deliveries", "maximum_retained_transport_bytes", "reserve_bytes",
                                   "readiness_max_age_seconds")}}
    return BackupStream(config)


def test_closed_handoff_has_all_dependencies_without_source_checkout_fallback(handoff_queue):
    queue = handoff_queue
    proof = publish_handoff(queue, "continuous-backup-20261004-v1")
    root = Path(proof["source_path"])
    verified = verify(root, proof["envelope_identity_sha256"])
    assert verified["files_verified"] == len(FILES) + 1
    assert hashlib.sha256((root / "handoff-manifest.json").read_bytes()).hexdigest() == proof["handoff_manifest_sha256"]
    code = "import sys;sys.path.insert(0,sys.argv[1]);import scripts.backup_delivery_receipt;import stockagent.data_sync.backup_recovery;import stockagent.control.recovery;import scripts.configure_backup_receipts_syncthing;import stockagent.data_sync.nas_recovery_acceptance;import scripts.run_nas_recovery_acceptance;import stockagent.data_sync.recovery_queue;import scripts.run_lab203_recovery_queue;import scripts.install_lab203_recovery_queue"
    subprocess.run([sys.executable, "-I", "-B", "-c", code, str(root)], cwd=root.parent, check=True)
    verify(root, proof["envelope_identity_sha256"])
    assert proof["receiver_adapter_deployed"] is False
    with pytest.raises(ValueError, match="previously frozen"):
        publish_handoff(queue, "continuous-backup-20261004-v1")


def test_storage_observer_handoff_is_closed_stdlib_only_and_does_not_upgrade_backup(handoff_queue):
    import json
    from scripts.publish_backup_handoff import STORAGE_OBSERVER_FILES, STORAGE_OBSERVER_NAME
    proof = publish_handoff(handoff_queue, STORAGE_OBSERVER_NAME)
    root = Path(proof["source_path"])
    assert proof["source_files"] == len(STORAGE_OBSERVER_FILES)
    manifest = json.loads((root / "handoff-manifest.json").read_bytes())
    assert manifest["replace_existing_worker"] is False
    assert manifest["automatic_execution_from_syncthing"] is False
    assert "upgrade_existing_service_driver" not in manifest
    code = ("import sys;sys.path.insert(0,sys.argv[1]);"
            "import stockagent.data_sync.storage_monitor;import scripts.lab203_storage_monitor;"
            "import stockagent.data_sync.nas_target;"
            "assert not any(n in sys.modules for n in ('torch','pandas','pyarrow','polars'))")
    subprocess.run([sys.executable, "-I", "-B", "-c", code, str(root)], cwd=root.parent, check=True)
    verify(root, proof["envelope_identity_sha256"])


def test_storage_relay_handoff_contains_full_skill_contracts_and_reviewable_instructions(handoff_queue):
    import json
    from scripts.publish_backup_handoff import STORAGE_RELAY_FILES, STORAGE_RELAY_NAME
    queue = handoff_queue
    before = queue.load_ledger()
    proof = publish_handoff(queue, STORAGE_RELAY_NAME, physical_transport_alias=queue.transport)
    root = Path(proof['source_path'])
    manifest = json.loads((root / 'handoff-manifest.json').read_bytes())
    assert proof['source_files'] == len(STORAGE_RELAY_FILES) == len(set(STORAGE_RELAY_FILES))
    assert sum(row['bytes'] for row in manifest['files']) < 1024**2
    assert manifest['replace_existing_worker'] is False
    assert manifest['automatic_execution_from_syncthing'] is False
    assert 'upgrade_existing_service_driver' not in manifest
    skill = '.agents/skills/stockagent-storage-operations'
    repo = Path(__file__).resolve().parents[1]
    assert {p.relative_to(repo).as_posix() for p in (repo / skill).rglob('*') if p.is_file()} <= set(STORAGE_RELAY_FILES)
    assert (root / 'docs/lab203_relay_storage_handoff_2026-10-25.md').read_bytes() == (
        repo / 'docs/lab203_relay_storage_handoff_2026-10-25.md').read_bytes()
    code = ('import sys;sys.path.insert(0,sys.argv[1]);'
            'import scripts.lab203_storage_monitor;import stockagent.data_sync.storage_monitor;'
            'assert not any(n in sys.modules for n in ("torch","pandas","pyarrow","polars"))')
    subprocess.run([sys.executable, '-I', '-B', '-c', code, str(root)], cwd=root.parent, check=True)
    verify(root, proof['envelope_identity_sha256'])
    assert queue.load_ledger() == before and not queue.ledger_path.exists()
    assert proof['receiver_adapter_deployed'] is False
    with pytest.raises(ValueError, match='previously frozen'):
        publish_handoff(queue, STORAGE_RELAY_NAME, physical_transport_alias=queue.transport)


def test_rescue_handoff_is_metadata_only_complete_and_keeps_original_owner(handoff_queue):
    import json
    from scripts.publish_backup_handoff import STORAGE_RESCUE_FILES, STORAGE_RESCUE_NAME
    queue = handoff_queue
    with (queue.state / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        proof = publish_handoff(queue, STORAGE_RESCUE_NAME, physical_transport_alias=queue.transport)
    root = Path(proof['source_path'])
    manifest = json.loads((root / 'handoff-manifest.json').read_bytes())
    assert proof['source_files'] == len(STORAGE_RESCUE_FILES) == len(set(STORAGE_RESCUE_FILES))
    assert sum(row['bytes'] for row in manifest['files']) < 1024**2
    assert not manifest['replace_existing_worker'] and not manifest['automatic_execution_from_syncthing']
    assert not queue.ledger_path.exists()
    assert all(not row['relative'].startswith(('artifacts/', 'data/', '.env')) for row in manifest['files'])
    skill = '.agents/skills/stockagent-storage-operations'
    repo = Path(__file__).resolve().parents[1]
    assert {p.relative_to(repo).as_posix() for p in (repo / skill).rglob('*') if p.is_file()} <= set(STORAGE_RESCUE_FILES)
    doc = (root / 'docs/lab203_private_rescue_nas_handoff_2026-10-26.md').read_text()
    assert '647174630a7b03d5d0d882ad89ed64e532255cfa5226fbe6949da58d7d37d2d3' in doc
    assert 'private_vast_recovery_backup' in doc
    assert 'semantic restore pending' in doc
    verify(root, proof['envelope_identity_sha256'])


def test_offline_rescue_handoff_is_fixed_stdlib_dependency_closure(handoff_queue):
    import json
    from scripts.publish_backup_handoff import STORAGE_OFFLINE_FILES, STORAGE_OFFLINE_NAME, MAX_CONTROL_HANDOFF_BYTES
    queue = handoff_queue
    proof = publish_handoff(queue, STORAGE_OFFLINE_NAME, physical_transport_alias=queue.transport)
    root = Path(proof['source_path'])
    manifest = json.loads((root / 'handoff-manifest.json').read_bytes())
    assert proof['source_files'] == len(STORAGE_OFFLINE_FILES) == len(set(STORAGE_OFFLINE_FILES))
    assert sum(row['bytes'] for row in manifest['files']) <= MAX_CONTROL_HANDOFF_BYTES
    assert not manifest['replace_existing_worker'] and not manifest['automatic_execution_from_syncthing']
    assert not queue.ledger_path.exists()
    assert not any(row['relative'].startswith(('artifacts/', 'data/', '.env')) for row in manifest['files'])
    repo = Path(__file__).resolve().parents[1]
    for name in ('scripts/verify_training_rescue.py', 'stockagent/training_rescue_worker.py'):
        assert (root / name).read_bytes() == (repo / name).read_bytes()
    code = ('import sys;sys.path.insert(0,sys.argv[1]);'
            'from scripts.verify_training_rescue import verify_restored_plan;'
            'assert not any(n in sys.modules for n in ("torch","pandas","pyarrow","polars"));'
            'assert "stockagent.training_rescue" not in sys.modules')
    subprocess.run([sys.executable, '-I', '-S', '-B', '-c', code, str(root)], cwd=root.parent, check=True)
    verify(root, proof['envelope_identity_sha256'])
    assert proof['source_ledger_modified'] is False and proof['receiver_adapter_deployed'] is False
    with pytest.raises(ValueError, match='previously frozen'):
        publish_handoff(queue, STORAGE_OFFLINE_NAME, physical_transport_alias=queue.transport)


def test_original_runtime_supplement_handoff_is_closed_and_imports_without_mutating_originals(handoff_queue):
    import json
    from scripts.publish_backup_handoff import STORAGE_RUNTIME_FILES, STORAGE_RUNTIME_NAME
    proof = publish_handoff(handoff_queue, STORAGE_RUNTIME_NAME, physical_transport_alias=handoff_queue.transport)
    root = Path(proof['source_path'])
    manifest = json.loads((root / 'handoff-manifest.json').read_bytes())
    assert len(STORAGE_RUNTIME_FILES) == len(set(STORAGE_RUNTIME_FILES)) == proof['source_files']
    assert not manifest['automatic_execution_from_syncthing']
    assert not manifest['private_credentials_included'] and not manifest['replace_existing_worker']
    code = ('import sys;sys.path.insert(0,sys.argv[1]);'
            'import scripts.diagnose_training_rescue_runtime as d;'
            'from stockagent.training_rescue_backup import EVIDENCE_PLAN;'
            'assert not any(n in sys.modules for n in ("torch","pandas","pyarrow","polars"))')
    subprocess.run([sys.executable, '-I', '-S', '-B', '-c', code, str(root)],
                   cwd=root.parent, check=True)
    assert not list(root.rglob('__pycache__'))
    verify(root, proof['envelope_identity_sha256'])
    assert not proof['receiver_adapter_deployed'] and not handoff_queue.ledger_path.exists()


@pytest.mark.parametrize('name,files', [
    ('storage-source-evidence-20261029-v1',
     ('artifacts/operations/storage-single-authority-20261025/lab203-source-evidence-20261029-v1.json',)),
    ('storage-capsule-binding-20261029-v1',
     ('artifacts/operations/storage-single-authority-20261025/lab203-training-capsule-binding-20261029-v1.json',)),
    ('storage-retired-scan-20261029-v1',
     ('artifacts/operations/storage-single-authority-20261025/source-retired-unscanned-20261029.private.json',)),
])
def test_source_evidence_handoff_is_bounded_immutable_and_does_not_modify_ledger(handoff_queue, tmp_path, name, files):
    source = tmp_path / 'fixed-source'
    evidence = source / files[0]
    evidence.parent.mkdir(parents=True)
    evidence.write_text('{"is_batch_or_recovery_ACK": false}\n')
    with (handoff_queue.state / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        proof = publish_handoff(handoff_queue, name, source_root=source,
                                physical_transport_alias=handoff_queue.transport)
    root = Path(proof['source_path'])
    verify(root, proof['envelope_identity_sha256'])
    assert (root / files[0]).read_bytes() == evidence.read_bytes()
    assert proof['source_files'] == 1 and proof['publication_scope'] == 'immutable_control_namespace_only'
    assert not proof['source_ledger_modified'] and not handoff_queue.ledger_path.exists()
    with pytest.raises(ValueError, match='previously frozen'):
        publish_handoff(handoff_queue, name, source_root=source,
                        physical_transport_alias=handoff_queue.transport)


def test_operator_can_wait_for_existing_owner_without_parallel_publication(handoff_queue):
    queue = handoff_queue
    with (queue.state / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            publish_handoff(queue, 'continuous-backup-20261004-v4')
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(publish_handoff, queue, 'continuous-backup-20261004-v4', wait_for_owner=True)
            time.sleep(0.05)
            assert not future.done()
            assert not (queue.transport / 'tools/continuous-backup-20261004-v4').exists()
            fcntl.flock(owner, fcntl.LOCK_UN)
            proof = future.result(timeout=10)
    verify(Path(proof['source_path']), proof['envelope_identity_sha256'])


def test_handoff_can_publish_data_tasks_under_same_owner(handoff_queue, monkeypatch):
    from stockagent.data_sync import recovery_queue
    calls = []
    def requests(config, transport, ledger):
        with (handoff_queue.state / "owner.lock").open("a") as other:
            with pytest.raises(BlockingIOError): fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        calls.append(transport)
        return {"state": "requests_published", "published_requests": 1}
    monkeypatch.setattr(recovery_queue, "publish_requests", requests)
    result = publish_handoff(handoff_queue, "continuous-backup-20261004-v7", publish_recovery_tasks=True)
    assert calls == [handoff_queue.transport] and result["automatic_recovery_requests"]["published_requests"] == 1
    from scripts.install_lab203_recovery_queue import bootstrap_pins
    pins = bootstrap_pins(handoff_queue.transport / "tools/automatic-backup-bootstrap-v7.json")
    assert pins["envelope_identity_sha256"] == result["envelope_identity_sha256"]
    assert pins["handoff_manifest_sha256"] == result["handoff_manifest_sha256"]
    assert pins["receiver_hook_deployed"] is False


def test_installer_bootstrap_does_not_write_bytecode_into_closed_synced_package(handoff_queue):
    proof = publish_handoff(handoff_queue, "continuous-backup-20261004-v7")
    root = Path(proof["source_path"])
    # No -B and no reliance on an inherited environment: the installer protects itself.
    subprocess.run([sys.executable, "-I", str(root / "scripts/install_lab203_recovery_queue.py"), "--help"],
                   cwd=root.parent, capture_output=True, check=True)
    assert not list(root.rglob("__pycache__"))
    verify(root, proof["envelope_identity_sha256"])


def test_v10_parallel_installer_is_frozen_complete_and_does_not_mutate_package(handoff_queue):
    for version in ("v8","v9"):
        with pytest.raises(ValueError,match="targets v10"):
            publish_handoff(handoff_queue,"continuous-backup-20261004-"+version)
    proof = publish_handoff(handoff_queue, "continuous-backup-20261004-v10")
    root = Path(proof["source_path"])
    assert proof["source_files"] == len(FILES)+len(PIPELINE_FILES)
    code = ("import sys;sys.path.insert(0,sys.argv[1]);import stockagent.data_sync.backup_relay_pipeline;"
            "import scripts.benchmark_lab203_backup_pipeline;import scripts.install_lab203_backup_pipeline")
    subprocess.run([sys.executable,"-I","-B","-c",code,str(root)],cwd=root.parent,check=True)
    subprocess.run([sys.executable,"-I",str(root/"scripts/install_lab203_backup_pipeline.py"),"--help"],
        cwd=root.parent,capture_output=True,check=True)
    assert not list(root.rglob("__pycache__"))
    verify(root,proof["envelope_identity_sha256"],workers=4)
    pins = __import__("json").loads((handoff_queue.transport/"tools/automatic-backup-bootstrap-v10.json").read_bytes())
    assert pins["envelope_identity_sha256"] == proof["envelope_identity_sha256"]


def test_control_handoff_does_not_wait_for_or_mutate_the_active_data_owner(handoff_queue):
    queue=handoff_queue
    before=queue.load_ledger()
    with (queue.state/"owner.lock").open("a") as owner:
        fcntl.flock(owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
        proof=publish_handoff(queue,"continuous-backup-20261004-v10",physical_transport_alias=queue.transport)
        assert proof["publication_scope"] == "immutable_control_namespace_only"
        assert queue.load_ledger() == before and not queue.ledger_path.exists()
        verify(Path(proof["source_path"]),proof["envelope_identity_sha256"],workers=4)
    with pytest.raises(ValueError,match="cannot mutate"):
        publish_handoff(queue,"continuous-backup-20261004-v5",physical_transport_alias=queue.transport,publish_recovery_tasks=True)


@pytest.mark.parametrize("scan_state", ["scan_request_acknowledged", "deferred_existing_folder_scan", "timeout"])
@pytest.mark.parametrize("package", ["storage-observer-20261025-v1", "storage-relay-20261026-v1", "storage-relay-20261026-v2", "storage-relay-20261026-v3", "storage-relay-20261027-v4", "storage-relay-20261027-v5"])
def test_control_cli_notifies_only_tools_without_reading_the_data_ledger(handoff_queue, monkeypatch, tmp_path, scan_state, package):
    from scripts import publish_backup_handoff as command
    import json

    queue = handoff_queue
    config = tmp_path / "queue.json"
    config.write_text(json.dumps(queue.config))
    output = tmp_path / "publication.json"
    notifications = []

    def forbidden_ledger_read():
        pytest.fail("control publication must not read the active data ledger")

    def notify(ledger):
        assert queue.config["scan_timeout_seconds"] == 5
        notifications.append(ledger)
        if scan_state == "timeout":
            raise TimeoutError("data scan is already busy")
        return {"state": scan_state, "NAS_backup_verified": False}

    monkeypatch.setattr(queue, "load_ledger", forbidden_ledger_read)
    monkeypatch.setattr(queue, "scan_transport", notify)
    monkeypatch.setattr(command, "BackupStream", lambda _: queue)
    monkeypatch.setattr(sys, "argv", ["publish_backup_handoff", "--config", str(config),
        "--name", package, "--physical-transport-alias", str(queue.transport),
        "--output", str(output)])
    command.main()
    assert notifications == [{"deliveries": {}}]
    proof = json.loads(output.read_bytes())
    verify(Path(proof["source_path"]), proof["envelope_identity_sha256"])
    assert proof["transport_notification"]["state"] == (
        "deferred_to_existing_transport" if scan_state == "timeout" else scan_state)
    if scan_state != "timeout":
        assert proof["transport_notification"]["NAS_backup_verified"] is False
    assert not queue.ledger_path.exists()


@pytest.mark.parametrize("package", ["storage-relay-20261027-v4", "storage-relay-20261027-v5", "storage-relay-20261027-v6", "storage-relay-20261027-v7"])
def test_revised_private_package_imports_input_recipe_in_isolation_without_mutating_it(handoff_queue, package):
    proof = publish_handoff(handoff_queue, package,
                            physical_transport_alias=handoff_queue.transport)
    root = Path(proof['source_path'])
    before = {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in root.rglob('*') if p.is_file()}
    code = (
        "import sys; sys.dont_write_bytecode=True; sys.path.insert(0,sys.argv[1]); "
        "from scripts.verify_training_rescue import RESTORED_PLAN_V2,_restored_input_specs; "
        "from stockagent.training_rescue_recipes import apply_input_recipe; "
        "from stockagent.training_rescue_worker import PRESERVED_FILE_SET_KIND; "
        "from stockagent.training_rescue_bootstrap import BOOTSTRAP_BINDING_V2; "
        "from stockagent.runtime_identity import validate_runtime_lock; "
        "print(RESTORED_PLAN_V2,PRESERVED_FILE_SET_KIND,BOOTSTRAP_BINDING_V2)"
    )
    result = subprocess.run([sys.executable, '-I', '-c', code, str(root)],
                            cwd=handoff_queue.state, capture_output=True, text=True, check=True, timeout=20)
    assert 'closed_plan_v2' in result.stdout and 'preserved_file_set_v1' in result.stdout
    assert 'training_bootstrap_binding.v2' in result.stdout
    assert {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*') if p.is_file()} == before
    verify(root, proof['envelope_identity_sha256'])


def test_incremental_package_is_complete_offline_and_preserves_original_backup_owner(handoff_queue):
    import json
    from scripts.publish_backup_handoff import STORAGE_CANONICAL_NAME, STORAGE_CANONICAL_FILES
    queue = handoff_queue
    with (queue.state / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        proof = publish_handoff(queue, STORAGE_CANONICAL_NAME, physical_transport_alias=queue.transport)
    root = Path(proof['source_path'])
    manifest = json.loads((root / 'handoff-manifest.json').read_bytes())
    assert len(STORAGE_CANONICAL_FILES) == len(set(STORAGE_CANONICAL_FILES)) == proof['source_files']
    assert not manifest['replace_existing_worker'] and not manifest['automatic_execution_from_syncthing']
    assert not proof['source_ledger_modified'] and not queue.ledger_path.exists()
    code = (
        'import sys; sys.path.insert(0,sys.argv[1]); '
        'from stockagent.training_rescue_backup import DELTA_CONTRACT,deliveries,validate_incremental; '
        'from scripts.verify_training_rescue import verify_restored_plan; '
        'assert DELTA_CONTRACT.endswith(".v1"); '
        'assert "incremental_id" in verify_restored_plan.__kwdefaults__; '
        'assert not any(n in sys.modules for n in ("torch","pandas","pyarrow","polars")); '
        'assert "stockagent.training_rescue" not in sys.modules'
    )
    subprocess.run([sys.executable, '-I', '-S', '-B', '-c', code, str(root)],
                   cwd=queue.state, check=True, timeout=20)
    result = subprocess.run([sys.executable, '-I', '-S', '-B',
                             str(root / 'scripts/verify_training_rescue.py'), '--help'],
                            cwd=queue.state, capture_output=True, text=True, check=True, timeout=20)
    assert '--base-restored-part' in result.stdout and '--incremental-id' in result.stdout
    verify(root, proof['envelope_identity_sha256'])


def test_control_package_bound_rejects_oversize_before_publication(handoff_queue, monkeypatch, tmp_path):
    from scripts import publish_backup_handoff as command
    source = tmp_path / 'oversize-source'
    source.mkdir()
    (source / 'too-large.txt').write_bytes(b'x' * (command.MAX_CONTROL_HANDOFF_BYTES + 1))
    monkeypatch.setattr(command, 'STORAGE_REVISED_FILES', ('too-large.txt',))
    with pytest.raises(ValueError, match='two MiB'):
        publish_handoff(handoff_queue, command.STORAGE_REVISED_NAME, source_root=source,
                        physical_transport_alias=handoff_queue.transport)
    assert not (handoff_queue.transport / 'tools' / command.STORAGE_REVISED_NAME).exists()
