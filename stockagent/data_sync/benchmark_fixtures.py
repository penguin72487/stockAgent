"""Reuse sealed benchmark inputs without linking a mutable source or database.

Each experiment still owns its logical input names. A small committed receipt
indexes readonly inputs in sibling experiments; the index itself is not proof.
The existing immutable-payload publisher verifies complete bytes before linking.
There is no payload pool, service, automatic deletion or cold-recovery claim.
"""
from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from datetime import date, datetime
import errno
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import uuid

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from stockagent.data_sync.desync_snapshots import SnapshotError, _fsync_directory
from stockagent.data_sync.immutable_cache_files import (
    link_immutable_payload, payload_info, stable_payload_hash,
)

CONTRACT = "immutable_benchmark_inputs_v1"
RECEIPT = "immutable-inputs.json"
MAX_METADATA_BYTES = 8 * 1024**2
MAX_DONOR_ROOTS = 1024
RESEARCH_STAGING_CONTRACT = "real_captured_research_staging_abba_v1"
PANEL_STORAGE_CONTRACT = "complete_panel_storage_comparison_v1"
OFFICIAL_DAILY_CONTRACT = "complete_official_daily_merge_v1"
FINMIND_RESERVATION_CONTRACT = "complete_finmind_reservation_comparison_v1"
FINMIND_GROUP_STATUS_CONTRACT = "complete_finmind_group_status_comparison_v1"
FINMIND_THROUGHPUT_RESERVATION_CONTRACT = "complete_finmind_throughput_reservation_comparison_v1"
PUBLIC_BUILDER_CONTRACT = "tw_public_complete_builder_exact_parity_v1"
TAIFEX_PROOF_MEMO_CONTRACT = "taifex_complete_curve_source_proof_memo_comparison_v1"
WORKFLOW_ROUND_CONTRACTS = {
    number: f"workflow_speed_round{number}_scoped_acceptance_v1" for number in (6, 7, 8, 9, 10, 11)
}
WORKFLOW_ROUND_RECEIPTS = (
    "optimization_acceptance.json", "optimization_acceptance_readback.json", "source_disposition.json",
)
FINMIND_THROUGHPUT_REPORTS = (
    "reservation_control.json", "publication_query_control.json",
    "periodic_query_control.json", "correction_query_control.json",
)


def _finmind_group_receipts(source: Path) -> list[Path]:
    """Original producer stores its completed comparisons beside the fixture."""
    match = re.fullmatch(r"finmind-group-status-abba-v2-([0-9]{8})-fixture", source.name)
    if match is None:
        return []
    return [source.parent / f"finmind-group-status-abba-v{version}-{match[1]}.json"
            for version in (2, 3)]


def _finmind_throughput_receipts(source: Path) -> list[Path]:
    """The original throughput producer retained four reports outside its fixture."""
    match = re.fullmatch(r"finlab-finmind-throughput-([0-9]{8})", source.parent.name)
    if source.name != "reservation" or match is None or len(source.parents) < 3:
        return []
    try:
        day = datetime.strptime(match[1], "%Y%m%d").date().isoformat()
    except ValueError:
        return []
    if source.parents[1].name != "benchmarks" or source.parents[2].name != "artifacts":
        return []
    directory = source.parents[2] / "data_quality" / f"finlab_finmind_throughput_{day}"
    return [directory / name for name in FINMIND_THROUGHPUT_REPORTS]


def _completion_receipt_paths(source: Path) -> list[Path]:
    return (_finmind_group_receipts(source) or _finmind_throughput_receipts(source)
            or _public_builder_receipts(source))


def _public_builder_receipts(source: Path) -> list[Path]:
    if (re.fullmatch(r"public-feature-assembly-[0-9]{8}[a-z]?", source.name) is None
            or len(source.parents) < 3 or source.parents[1].name != "benchmarks"
            or source.parents[2].name != "artifacts"):
        return []
    return [source.parent / (source.name + suffix)
            for suffix in ("-exact-parity.json", "-retirement-evidence.json")]


def external_benchmark_contract(source: Path) -> str | None:
    """Bind a renamed/retained comparison to the exact original producer ABI."""
    if _finmind_group_receipts(source):
        return FINMIND_GROUP_STATUS_CONTRACT
    if _finmind_throughput_receipts(source):
        return FINMIND_THROUGHPUT_RESERVATION_CONTRACT
    if _public_builder_receipts(source):
        return PUBLIC_BUILDER_CONTRACT
    if _taifex_proof_memo_case(source):
        return TAIFEX_PROOF_MEMO_CONTRACT
    if contract := _workflow_round_contract(source):
        return contract
    return None


def _workflow_round_contract(source: Path) -> str | None:
    """Only reviewed original round ABIs, bound to an exact case.

    A closed round is separate from its still-running parent optimization.
    Future rounds require review; a generic verified/complete flag is insufficient.
    """
    match = re.fullmatch(r"round(6|7|8|9|10|11)", source.name)
    suite = re.fullmatch(r"workflow_speed_optimization_([0-9]{8})", source.parent.name)
    if (match is None or suite is None or len(source.parents) < 3
            or source.parents[1].name != "benchmarks" or source.parents[2].name != "artifacts"):
        return None
    try:
        datetime.strptime(suite[1], "%Y%m%d")
    except ValueError:
        return None
    return WORKFLOW_ROUND_CONTRACTS[int(match[1])]


def _completed_workflow_round8(acceptance: dict, readback: dict, disposition: dict) -> bool:
    """Round 8 separates optimization, confirmation and instrumentation samples.

    Its original producer sealed all three comparisons and one diagnostic
    cohort; raw_samples_verified belongs to a different historical receipt ABI.
    Keep the selected variant as historical metadata, not today's tuning policy.
    """
    for family in ("optimization", "confirmation", "instrumentation_comparison"):
        count = acceptance.get(f"{family}_samples")
        verified = readback.get(f"{family}_samples_verified")
        if type(count) is not int or count <= 0 or type(verified) is not int or verified != count:
            return False
    if (type(acceptance.get("instrumented_diagnostic_samples")) is not int
            or acceptance["instrumented_diagnostic_samples"] <= 0
            or readback.get("diagnostic_counter_coverage_verified") is not True
            or readback.get("remaining_registry_counts_verified") is not True
            or any(type(readback.get(key)) is not int or readback[key] <= 0
                   for key in ("mean_checks", "speed_samples_verified", "summary_groups_verified"))):
        return False
    variant, batch = acceptance.get("selected_variant"), acceptance.get("selected_batch_mib")
    if (not isinstance(variant, str) or not variant.strip()
            or type(batch) is not int or batch <= 0
            or type(acceptance.get("performance_candidate_adopted")) is not bool):
        return False
    for key in ("selected_variant", "selected_batch_mib", "performance_candidate_adopted"):
        if any(type(value.get(key)) is not type(acceptance[key]) or value[key] != acceptance[key]
               for value in (readback, disposition)):
            return False
    return True


def _workflow_selected_source_matches(acceptance: dict, readback: dict, disposition: dict) -> bool:
    """Compare the selected historical source; it is not today's tuning policy."""
    selected = acceptance.get("selected_source")
    batch = acceptance.get("selected_batch_mib")
    workers = acceptance.get("selected_read_workers")
    adopted = acceptance.get("performance_candidate_adopted")
    if (not isinstance(selected, str) or not selected.strip()
            or type(batch) is not int or batch <= 0
            or type(workers) is not int or workers <= 0 or type(adopted) is not bool
            or disposition.get("selected_source") != selected
            or type(disposition.get("selected_batch_mib")) is not int
            or disposition["selected_batch_mib"] != batch
            or type(disposition.get("performance_candidate_adopted")) is not bool
            or disposition["performance_candidate_adopted"] != adopted
            or type(readback.get("performance_candidate_adopted")) is not bool
            or readback["performance_candidate_adopted"] != adopted):
        return False
    return True


def _completed_workflow_round9(acceptance: dict, readback: dict, disposition: dict) -> bool:
    """Validate round 9's original performance and diagnostic readback ABI."""
    for family in ("performance", "diagnostic"):
        count = acceptance.get(f"{family}_samples")
        verified = readback.get(f"{family}_samples_verified")
        if type(count) is not int or count <= 0 or type(verified) is not int or verified != count:
            return False
    operations = readback.get("serialization_operations_verified")
    if (type(operations) is not int
            or operations < acceptance["performance_samples"] + acceptance["diagnostic_samples"]
            or readback.get("remaining_registry_counts_verified") is not True
            or any(type(readback.get(key)) is not int or readback[key] <= 0
                   for key in ("mean_checks", "speed_samples_verified", "summary_groups_verified"))):
        return False
    return _workflow_selected_source_matches(acceptance, readback, disposition)


def _completed_workflow_round10(acceptance: dict, readback: dict, disposition: dict) -> bool:
    """Validate round 10's separate performance and encoding calibration ABI."""
    for family in ("performance", "encoding"):
        count = acceptance.get(f"{family}_samples")
        verified = readback.get(f"{family}_samples_verified")
        if type(count) is not int or count <= 0 or type(verified) is not int or verified != count:
            return False
    if (type(acceptance.get("encoding_diagnostics")) is not int
            or acceptance["encoding_diagnostics"] <= 0
            or type(readback.get("encoding_diagnostic_operations_verified")) is not int
            or readback["encoding_diagnostic_operations_verified"] <= 0
            or readback.get("remaining_registry_counts_verified") is not True
            or any(type(readback.get(key)) is not int or readback[key] <= 0
                   for key in ("mean_checks", "speed_samples_verified", "summary_groups_verified"))
            or disposition.get("source_data_deleted") is not False
            or disposition.get("services_restarted") is not False
            or type(disposition.get("new_training_jobs")) is not int
            or disposition["new_training_jobs"] != 0):
        return False
    return _workflow_selected_source_matches(acceptance, readback, disposition)


def _completed_workflow_round(source: Path, objects: dict, contents: dict) -> bool:
    """Preserve original scoped completion, not current numerical acceptance.

    Full source inventories, D reconstruction and current consumer/quarantine
    checks remain the retirement owner's responsibility. Historical references
    to maintained source are not reinterpreted as today's source fingerprint.
    """
    contract = _workflow_round_contract(source)
    if contract is None or set(objects) != set(WORKFLOW_ROUND_RECEIPTS):
        return False
    acceptance, readback, disposition = (objects[name] for name in WORKFLOW_ROUND_RECEIPTS)
    if (acceptance.get("contract") != contract
            or acceptance.get("state") != "verified_scoped_engineering"
            or readback.get("state") != "verified"
            or disposition.get("state") not in {"reviewed", "prepared"}
            or not isinstance(acceptance.get("scope"), str) or not acceptance["scope"].strip()):
        return False
    number = int(source.name[5:])
    digest_key, count_key, samples_key = (("acceptance_sha256", "evidence_files_verified", "raw_samples_verified")
        if number == 6 else ("seal_sha256", "sealed_files_verified",
                            "performance_samples_verified" if number in (9, 10, 11) else
                            "optimization_samples_verified" if number == 8 else "raw_samples_verified"))
    if (readback.get(digest_key) != hashlib.sha256(contents[WORKFLOW_ROUND_RECEIPTS[0]]).hexdigest()
            or type(readback.get(samples_key)) is not int or readback[samples_key] <= 0
            or type(readback.get("collector_phases_verified")) is not int
            or readback["collector_phases_verified"] != 13):
        return False
    try:
        start = datetime.fromisoformat(acceptance["observed_at_utc"])
        end = datetime.fromisoformat(readback["observed_at_utc"])
        if start.utcoffset() is None or end.utcoffset() is None or end < start:
            return False
    except (KeyError, TypeError, ValueError):
        return False
    boundaries = acceptance if number in (8, 9, 10, 11) else acceptance.get("remaining_boundaries")
    if (not isinstance(boundaries, dict)
            or any(boundaries.get(key) is not False or readback.get(key) is not False
                   for key in ("all_workflows_measured", "hardware_limit_proven"))):
        return False
    if number == 8 and not _completed_workflow_round8(acceptance, readback, disposition):
        return False
    if number == 9 and not _completed_workflow_round9(acceptance, readback, disposition):
        return False
    if number == 10 and not _completed_workflow_round10(acceptance, readback, disposition):
        return False
    evidence = acceptance.get("evidence")
    if (not isinstance(evidence, list) or not 1 <= len(evidence) <= 50000
            or type(readback.get(count_key)) is not int or readback[count_key] != len(evidence)):
        return False
    names = set()
    for row in evidence:
        if (not isinstance(row, dict) or set(row) != {"path", "bytes", "sha256"}
                or not isinstance(row["path"], str) or "\x00" in row["path"]
                or "\\" in row["path"] or not row["path"]
                or type(row["bytes"]) is not int or row["bytes"] < 0
                or not isinstance(row["sha256"], str)
                or re.fullmatch(r"[0-9a-f]{64}", row["sha256"]) is None):
            return False
        path = PurePosixPath(row["path"])
        if (not path.parts or path.is_absolute() or ".." in path.parts or path.as_posix() != row["path"]
                or row["path"] in names):
            return False
        names.add(row["path"])
    return True


def _taifex_proof_memo_case(source: Path) -> bool:
    return (re.fullmatch(r"taifex_proof_memo_[0-9]{8}", source.name) is not None
            and len(source.parents) >= 3 and source.parents[1].name == "benchmarks"
            and source.parents[2].name == "artifacts")


def _completed_taifex_proof_memo(source: Path, objects: dict) -> bool:
    """Recognize the original eight-trial ABI; never rerun a financial oracle.

    This is completion metadata only. The retirement owner separately verifies
    every current original byte, cold reconstruction, consumer and quarantine.
    Failed/partial sibling trials remain inside that exact preserved inventory.
    """
    if (not _taifex_proof_memo_case(source) or set(objects) != {
            "comparison-v2.json", "original-code-proof.json", "benchmark-started.json"}):
        return False
    report = objects["comparison-v2.json"]
    original = objects["original-code-proof.json"]
    order = ["baseline", "candidate", "candidate", "baseline"] * 2
    ranges = {"1h", "1d", "1w", "1mo", "1q", "1y", "all"}

    def digest(value):
        return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None

    def hashes(value, names):
        return isinstance(value, dict) and set(value) == names and all(map(digest, value.values()))

    def clock(value):
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value)
            return parsed if parsed.tzinfo is not None else None
        except ValueError:
            return None

    start, end = clock(objects["benchmark-started.json"].get("at_utc")), clock(report.get("completed_at_utc"))
    if (report.get("contract") != TAIFEX_PROOF_MEMO_CONTRACT
            or report.get("state") != "measurement_complete"
            or report.get("scope") != "complete_seven_range_real_append_curve_durable_readback_and_warm_not_broker_or_whole_host"
            or report.get("fixture") != str(source / "fixture")
            or report.get("trial_order") != order or start is None or end is None or end < start
            or any(report.get(key) is not True for key in (
                "balanced_abba", "code_stable", "source_unchanged", "exact_output_parity"))
            or report.get("production_writes") is not False
            or any(type(report.get(key)) is not int or report[key] != 0
                   for key in ("provider_calls", "model_forward_calls"))):
        return False
    source_names = {str(source / "fixture/state" / name) for name in (
        "state.json", "status.json", "marks.jsonl",
        "backfills/all_strategies_bidask_recapitalizing_v3/marks.jsonl",
        "backfills/all_strategies_bidask_recapitalizing_v3/receipt.json")}
    code = report.get("code_sha256")
    if (not hashes(report.get("source_sha256"), source_names)
            or not isinstance(code, dict) or not code or len(code) > 32
            or not all(isinstance(name, str) and Path(name).is_absolute()
                       and ".." not in Path(name).parts and digest(value) for name, value in code.items())
            or original.get("source") != "stockagent/live/taifex_history_column_cache.py"
            or type(original.get("prefix_bytes")) is not int or original["prefix_bytes"] <= 0
            or not digest(original.get("sha256"))
            or code.get(str(source / "original_column_cache.py")) != original["sha256"]
            or not any(name.endswith("/scripts/benchmark_taifex_source_proof_memo.py") for name in code)):
        return False
    results = {}
    for phase, label in (("setup", "base_setup"), ("update", "trials")):
        trials = report.get(label)
        if not isinstance(trials, list) or len(trials) != len(order):
            return False
        expected = None
        for index, (variant, trial) in enumerate(zip(order, trials, strict=True)):
            wanted = {"1d"} if phase == "setup" else ranges
            if (not isinstance(trial, dict) or trial.get("variant") != variant
                    or type(trial.get("trial")) is not int or trial["trial"] != index
                    or trial.get("phase") != phase
                    or trial.get("durable_complete_readback") is not True
                    or trial.get("warm_exact_output_parity") is not True
                    or trial.get("production_writes") is not False
                    or any(type(trial.get(key)) is not int or trial[key] != 0
                           for key in ("provider_calls", "model_forward_calls"))
                    or not hashes(trial.get("output_sha256"), wanted)
                    or any(type(trial.get(key)) not in (int, float)
                           or not math.isfinite(trial[key]) or trial[key] < 0
                           for key in ("wall_seconds", "cpu_seconds", "warm_wall_seconds", "warm_cpu_seconds"))):
                return False
            if expected is not None and trial["output_sha256"] != expected:
                return False
            expected = trial["output_sha256"]
        results[phase] = expected
    raw = report.get("independent_raw_1d")
    return (isinstance(raw, dict)
            and raw.get("path") == str(source / "comparison-v2/independent-raw-1d.json")
            and digest(raw.get("sha256")) and raw.get("output_sha256") == results["update"]["1d"])


def _finmind_group_quarantine_origin(source: Path, original: Path) -> None:
    """Validate a renamed fixture against its existing durable transaction.

    The original path comes from the caller's reviewed artifact root/spec,
    never from a producer receipt. This admits completion metadata only; the
    retirement owner still proves full bytes, usage and the fixed cold release.
    """
    if (source.name != 'source' or len(source.parents) < 4
            or source.parents[2].name != 'quarantine'
            or source.parents[3].name != 'retirements'
            or re.fullmatch(r'[0-9]+-[0-9a-f]{32}', source.parent.name) is None
            or original.exists() or original.is_symlink()):
        raise SnapshotError('comparison is outside its original retirement quarantine')
    dataset = source.parents[1].name
    state_path = source.parents[3] / (dataset + '.json')
    descriptor = os.open(state_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    with os.fdopen(descriptor, 'rb') as handle:
        before = os.fstat(handle.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_size > 65536
                or before.st_uid != os.geteuid() or before.st_mode & 0o022):
            raise SnapshotError('comparison quarantine state is unsafe')
        raw = handle.read(before.st_size + 1)
        signature = lambda info: (info.st_dev, info.st_ino, info.st_size,
                                  info.st_mtime_ns, info.st_ctime_ns)
        if (len(raw) != before.st_size or signature(before) != signature(os.fstat(handle.fileno()))
                or signature(before) != signature(state_path.lstat())):
            raise SnapshotError('comparison quarantine state changed during read')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate retirement state key')
            result[key] = value
        return result
    try:
        state = json.loads(raw, object_pairs_hook=unique)
    except (ValueError, RecursionError) as error:
        raise SnapshotError('comparison quarantine state is not readable JSON') from error
    relative = state.get('relative_root') if isinstance(state, dict) else None
    parts = PurePosixPath(relative).parts if isinstance(relative, str) else ()
    if (not isinstance(state, dict) or type(state.get('schema_version')) is not int
            or state['schema_version'] != 1 or state.get('state') != 'retiring'
            or state.get('dataset') != dataset or state.get('quarantine') != str(source.parent)
            or len(parts) != 3 or parts[0] != 'benchmarks' or '..' in parts
            or PurePosixPath(*parts).as_posix() != relative
            or tuple(original.parts[-3:]) != parts
            or not isinstance(state.get('snapshot_id'), str) or not state['snapshot_id']
            or not isinstance(state.get('manifest_sha256'), str)
            or re.fullmatch(r'[0-9a-f]{64}', state['manifest_sha256']) is None):
        raise SnapshotError('comparison quarantine original identity differs')


def _completed_finmind_group(source: Path, objects: dict) -> bool:
    paths = _finmind_group_receipts(source)
    if len(paths) != 2 or set(objects) != {p.name for p in paths}:
        return False

    def digest(value):
        return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None

    def timing(value):
        return type(value) in (int, float) and math.isfinite(value) and value >= 0

    def original_path(value, target):
        if not isinstance(value, str):
            return False
        p = PurePosixPath(value)
        if p.as_posix() != value or '..' in p.parts:
            return False
        # Both forms were written by the original CLI, depending on argv.
        relative = PurePosixPath('artifacts/benchmarks') / source.parent.name / target.name
        return value in (str(target), str(relative))

    source_queue = None
    for index, path in enumerate(paths):
        receipt = objects[path.name]
        if (type(receipt.get('schema_version')) is not int or receipt['schema_version'] != 1
                or receipt.get('state') != 'measurement_complete'
                or any(receipt.get(key) is not True for key in
                       ('exact_output_parity', 'all_task_values_equal', 'source_files_stable', 'code_stable'))
                or any(type(receipt.get(key)) is not int or receipt[key] != 0
                       for key in ('provider_calls', 'production_queue_writes'))
                or receipt.get('production_deployed') is not False
                or not original_path(receipt.get('fixture'), source)
                or not digest(receipt.get('snapshot_sha256'))
                or not timing(receipt.get('preparation_wall_seconds'))
                or not isinstance(receipt.get('dataset'), str) or not receipt['dataset']):
            return False
        queue = receipt.get('source_queue')
        if (not isinstance(queue, str) or not PurePosixPath(queue).is_absolute()
                or '..' in PurePosixPath(queue).parts or PurePosixPath(queue).as_posix() != queue
                or PurePosixPath(queue).parts[-2:] != ('complement', 'queue.sqlite3')
                or source_queue not in (None, queue)):
            return False
        source_queue = queue
        count = receipt.get('tasks_per_status')
        if (type(count) is not int or not 1 <= count <= 64
                or type(receipt.get('task_count')) is not int or receipt['task_count'] < count):
            return False
        if index == 1 and (not original_path(receipt.get('prepared_from'), paths[0])
                           or receipt.get('resource_limits_stable') is not True):
            return False
        code = receipt.get('code_sha256')
        if (not isinstance(code, dict) or len(code) < 5
                or any(not isinstance(p, str) or not PurePosixPath(p).is_absolute()
                       or '..' in PurePosixPath(p).parts or not digest(h) for p, h in code.items())
                or not {'benchmark_finmind_group_status.py', 'download_finmind_complement.py',
                        'finmind_updates.py', 'artifact_io.py'} <= {PurePosixPath(p).name for p in code}):
            return False
        initial = receipt.get('initial_full_status')
        if not isinstance(initial, dict) or set(initial) != {'baseline', 'candidate'}:
            return False
        hashes = set()
        for row in initial.values():
            if (not isinstance(row, dict) or not digest(row.get('status_sha256'))
                    or any(not timing(row.get(k)) for k in ('wall_seconds', 'cpu_seconds'))):
                return False
            hashes.add(row['status_sha256'])
        if len(hashes) != 1:
            return False
        originals = receipt.get('source_receipts')
        if (not isinstance(originals, list) or len(originals) != count
                or any(not isinstance(row, dict) or not digest(row.get('sha256'))
                       or not digest(row.get('parquet_sha256')) or type(row.get('rows')) is not int
                       or row['rows'] <= 0 for row in originals)
                or type(receipt.get('rows_published_per_trial')) is not int
                or sum(row['rows'] for row in originals) != receipt['rows_published_per_trial']):
            return False
        trials = receipt.get('trials')
        if not isinstance(trials, list) or len(trials) != 4:
            return False
        parity = {}
        for row, (label, ordinal) in zip(trials,
                (('baseline', 0), ('candidate', 0), ('candidate', 1), ('baseline', 1)), strict=True):
            if (not isinstance(row, dict) or type(row.get('ordinal')) is not int
                    or (row.get('label'), row['ordinal']) != (label, ordinal)
                    or any(not timing(row.get(k)) for k in ('wall_seconds', 'cpu_seconds'))
                    or any(not digest(row.get(k)) for k in ('whole_status_sha256', 'all_receipt_sha256'))
                    or not isinstance(row.get('parquet_bytes_sha256'), list)
                    or len(row['parquet_bytes_sha256']) != count
                    or any(not digest(h) for h in row['parquet_bytes_sha256'])):
                return False
            value = (row['whole_status_sha256'], row['all_receipt_sha256'], tuple(row['parquet_bytes_sha256']))
            if ordinal in parity and parity[ordinal] != value:
                return False
            parity[ordinal] = value
    return True


def _completed_finmind_reservation(objects: dict, *, companion_parent: Path | None = None) -> bool:
    """Recognize the original full reservation/status and durable-write ABI.

    These older measurements predate fixture.json/abba-receipt.json. Keep all
    their original receipts; do not manufacture that newer completion format.
    This admits only completion metadata. Exact source/cold reconstruction,
    current consumers, leases and quarantine remain the archive owner's gates.
    """
    names = (set(FINMIND_THROUGHPUT_REPORTS) if companion_parent is not None else
             {"comparison.json", "comparison-v2.json", "comparison-three-plans.json"})
    if ((companion_parent is not None and set(objects) != names)
            or (companion_parent is None and ("comparison.json" not in objects or not set(objects) <= names))):
        return False

    def digest(value):
        return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None

    def absolute(value):
        return (isinstance(value, str) and PurePosixPath(value).is_absolute()
                and ".." not in PurePosixPath(value).parts
                and PurePosixPath(value).as_posix() == value)

    def timing(value):
        return type(value) in (int, float) and math.isfinite(value) and value >= 0

    source_queue = None
    owners = {"sponsor", "complement"}
    for name, receipt in objects.items():
        if (not isinstance(receipt, dict) or type(receipt.get("schema_version")) is not int
                or receipt["schema_version"] != 1 or receipt.get("state") != "measurement_complete"
                or receipt.get("scope") != "fixed_real_two_owner_queues_full_reservation_status_and_durable_claim_release"
                or any(receipt.get(key) is not True for key in
                       ("code_stable", "exact_output_parity", "source_workflow_unchanged"))
                or any(type(receipt.get(key)) is not int or receipt[key] != 0
                       for key in ("provider_calls", "production_queue_mutations"))
                or not digest(receipt.get("whole_output_sha256"))
                or not absolute(receipt.get("fixture")) or not absolute(receipt.get("source_queue"))):
            return False
        queue, fixture = PurePosixPath(receipt["source_queue"]), PurePosixPath(receipt["fixture"])
        if queue.parts[-2:] != ("complement", "queue.sqlite3") or source_queue not in (None, queue):
            return False
        source_queue = queue
        code = receipt.get("code_sha256")
        if (not isinstance(code, dict) or len(code) < 4
                or any(not absolute(path) or not digest(sha) for path, sha in code.items())
                or not {"benchmark_finmind_reservation.py", "download_finmind_complement.py",
                        "finmind_scheduling.py", "finmind_queue_indexes.py"} <= {PurePosixPath(p).name for p in code}):
            return False
        snapshots, setup = receipt.get("snapshots"), receipt.get("index_setup")
        if not isinstance(snapshots, dict) or not isinstance(setup, dict) or set(setup) != owners:
            return False
        initial = {f"{owner}/queue.sqlite3" for owner in owners}
        reused = {f"{variant}/{owner}" for variant in ("baseline", "candidate") for owner in owners}
        if set(snapshots) not in (initial, reused):
            return False
        for key, row in snapshots.items():
            if (not isinstance(row, dict) or type(row.get("bytes")) is not int or row["bytes"] <= 0):
                return False
            if set(snapshots) == initial:
                if row.get("source_path") != str(queue.parent.parent / key) or not digest(row.get("sha256")):
                    return False
            elif row.get("path") != str(fixture / key / "queue.sqlite3") or not digest(row.get("sha256_before_setup")):
                return False
        for row in setup.values():
            if (not isinstance(row, dict) or any(not timing(row.get(key)) for key in ("wall_seconds", "cpu_seconds"))
                    or any(type(row.get(key)) is not int or row[key] < 0
                           for key in ("allocated_index_bytes", "indexed_rows"))):
                return False
        plans = receipt.get("reservation_plans")
        if not isinstance(plans, dict) or set(plans) not in ({"baseline", "candidate"}, {"baseline", "automatic", "candidate"}):
            return False
        variants = ("baseline", "automatic", "candidate") if "automatic" in plans else ("baseline", "candidate")
        for collection, expected in ((plans, set(variants)), (receipt.get("status_plans"), {"baseline", "candidate"})):
            if not isinstance(collection, dict) or set(collection) != expected:
                return False
            for plan in collection.values():
                if (not isinstance(plan, dict) or set(plan) != owners
                        or any(not isinstance(rows, list) or not rows
                               or any(not isinstance(sql, str) or not sql for sql in rows) for rows in plan.values())):
                    return False
        trials = receipt.get("trials")
        if not isinstance(trials, list) or not 1 <= len(trials) / (2 * len(variants)) <= 5 or len(trials) % (2 * len(variants)):
            return False
        repeats = len(trials) // (2 * len(variants))
        expected = [(variant, repeat) for repeat in range(repeats) for variant in (*variants, *reversed(variants))]
        if any(not isinstance(row, dict) or type(row.get("repeat")) is not int for row in trials):
            return False
        if [(row.get("variant"), row["repeat"]) for row in trials] != expected:
            return False
        for row in trials:
            if (row.get("whole_output_sha256") != receipt["whole_output_sha256"]
                    or type(row.get("queue_changes")) is not int or row["queue_changes"] != 0
                    or any(not timing(row.get(key)) for key in
                           ("wall_seconds", "cpu_seconds", "reservation_wall_seconds", "reservation_cpu_seconds"))):
                return False
        writes = receipt.get("claim_release_trials")
        expected_writes = [(variant, repeat, group) for repeat in range(repeats)
            for variant in (("baseline", "candidate") if repeat % 2 == 0 else ("candidate", "baseline"))
            for group in ("history", "release")]
        if not isinstance(writes, list) or len(writes) != len(expected_writes):
            return False
        task_counts = set()
        for row, expected in zip(writes, expected_writes, strict=True):
            if (not isinstance(row, dict) or type(row.get("repeat")) is not int
                    or (row.get("variant"), row["repeat"], row.get("group")) != expected
                    or type(row.get("tasks")) is not int or not 1 <= row["tasks"] <= 64
                    or type(row.get("transactions")) is not int or row["transactions"] != 2 * row["tasks"]
                    or row.get("exact_task_rows_restored") is not True
                    or any(not timing(row.get(key)) for key in ("wall_seconds", "cpu_seconds"))):
                return False
            task_counts.add(row["tasks"])
        if len(task_counts) != 1:
            return False
        previous = receipt.get("prepared_from")
        if companion_parent is not None:
            index = FINMIND_THROUGHPUT_REPORTS.index(name)
            expected_previous = str(companion_parent / FINMIND_THROUGHPUT_REPORTS[index - 1]) if index else None
            if previous != expected_previous:
                return False
        if previous is not None:
            prior_name = PurePosixPath(previous).name if absolute(previous) else None
            prior = objects.get(prior_name)
            if (prior_name == name or not isinstance(prior, dict) or not absolute(prior.get("fixture"))
                    or previous != str((companion_parent or fixture.parent) / prior_name)
                    or prior["fixture"] != str(fixture)):
                return False
    return True


def _completed_finmind_throughput(source: Path, objects: dict) -> bool:
    paths = _finmind_throughput_receipts(source)
    if not paths or not all(path.name in objects for path in paths):
        return False
    reports = {path.name: objects[path.name] for path in paths}
    if not _completed_finmind_reservation(reports, companion_parent=paths[0].parent):
        return False
    expected_names = set(reports)
    for stage, (name, report) in enumerate(reports.items()):
        if report["fixture"] != str(source):
            return False
        included = ((), (0,), (0, 1), (2,))[stage]
        additions = tuple(("publication_rechecks", "periodic_refreshes", "correction_candidates")[i]
                          for i in included)
        flags = ("includes_us_publication_recheck", "includes_periodic_preparation",
                 "includes_correction_candidates")
        if any(report.get(flag, False) is not (index in included) for index, flag in enumerate(flags)):
            return False
        stem = Path(name).stem
        context_name = stem + "-context.json"
        if report.get("captured_context") != str(source / context_name):
            return False
        context = objects.get(context_name)
        context_keys = {"captured_live_status", "current_stock_ids", "official_delisted_ids", "fixed_clock"}
        if stage == 3:
            context_keys |= {"correction_plan_sha256", "correction_requests"}
        if (not isinstance(context, dict) or set(context) != context_keys
                or not isinstance(context["captured_live_status"], dict)
                or any(not isinstance(context[key], list) or not context[key]
                       or any(not isinstance(value, str) or not value for value in context[key])
                       for key in ("current_stock_ids", "official_delisted_ids"))):
            return False
        try:
            if datetime.fromisoformat(context["fixed_clock"]).utcoffset() is None:
                return False
        except (TypeError, ValueError):
            return False
        if stage == 3:
            requests = context["correction_requests"]
            if (not re.fullmatch(r"[0-9a-f]{64}", str(context["correction_plan_sha256"]))
                    or not isinstance(requests, list) or not requests
                    or any(not isinstance(request, dict) or request.get("owner") != "complement"
                        or any(not isinstance(request.get(key), str) or not request[key]
                               for key in ("request_id", "dataset", "start_date", "end_date"))
                        for request in requests)):
                return False
        expected_names.add(context_name)
        for index, trial in enumerate(report["trials"]):
            output_name = f"{stem}-{trial['variant']}-output-{trial['repeat']}-{index}.json"
            output = objects.get(output_name)
            if not isinstance(output, dict) or set(output) != {"reservation", "status", *additions}:
                return False
            digest = hashlib.sha256(json.dumps(output, sort_keys=True, separators=(",", ":"),
                                               ensure_ascii=False).encode()).hexdigest()
            if digest != trial["whole_output_sha256"]:
                return False
            expected_names.add(output_name)
    # Unknown or incomplete attempts cannot disappear behind a successful report.
    return set(objects) == expected_names


def standalone_benchmark_contract(name: str) -> str | None:
    """Bind a supported standalone name to its original completion ABI."""
    if re.fullmatch(r"research-stage-copy-[0-9]{8}-v[1-9][0-9]*", name):
        return RESEARCH_STAGING_CONTRACT
    if re.fullmatch(r"panel_chunk_storage_[0-9]{8}_(?:final|(?:[1-9][0-9]*m_)?v[1-9][0-9]*)", name):
        return PANEL_STORAGE_CONTRACT
    if re.fullmatch(r"official-real-daily-[0-9]{8}-v[1-9][0-9]*", name):
        return OFFICIAL_DAILY_CONTRACT
    return None


def _completed_official_daily(objects: dict) -> bool:
    """Recognize the original full/daily/unchanged merge producer's ABI.

    The producer writes its receipt only after independent full-value and
    source-byte checks. This admits that completion evidence, not the payload:
    the archive owner must still reconstruct all original bytes and check
    current consumers. No old receipt is rewritten or assigned a new oracle.
    """
    def digest(value):
        return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None

    receipt, progress = objects.get("receipt.json", {}), objects.get("progress.json", {})
    stages = receipt.get("stages")
    expected_stages = {"capture_fixed_sources", "prior_fixture", "prior_full_merge",
        "restore_actual_latest_sources", "new_source_session_merge", "daily_complete_typed_oracle",
        "independent_full_merge", "full_complete_typed_oracle", "unchanged_merge",
        "unchanged_complete_typed_oracle"}
    if (receipt.get("status") != "accepted_real_source_merge_daily_not_full_base_pit_or_live_readiness"
            or receipt.get("retrospective_fixture_not_historical_pit") is not True
            or any(type(receipt.get(key)) is not int or receipt[key] != 0
                   for key in ("production_source_writes", "service_restarts", "ledger_writes"))
            or not digest(receipt.get("benchmark_sha256"))
            or not isinstance(stages, dict) or set(stages) != expected_stages
            or progress.get("stages") != stages or progress.get("code") != receipt.get("code")):
        return False
    for timing in stages.values():
        if (not isinstance(timing, dict) or set(timing) != {"wall_seconds", "cpu_seconds"}
                or any(type(value) not in (int, float) or not math.isfinite(value) or value < 0
                       for value in timing.values())):
            return False
    code = receipt.get("code")
    code_keys = {"builder", "typed_proof", "parser_shared", "group_projection",
                 "merge_projection", "partitioned_parquet"}
    if (not isinstance(code, dict) or set(code) != code_keys | {"polars", "numpy", "arrow"}
            or any(not digest(code[key]) for key in code_keys)
            or any(not isinstance(code[key], str) or not code[key] for key in ("polars", "numpy", "arrow"))):
        return False
    try:
        if date.fromisoformat(receipt["prior"]) >= date.fromisoformat(receipt["latest"]):
            return False
    except (KeyError, TypeError, ValueError):
        return False
    shapes = [receipt.get(key) for key in ("previous_shape", "daily_shape")]
    if (any(not isinstance(shape, list) or len(shape) != 2
            or any(type(value) is not int or value <= 0 for value in shape) for shape in shapes)
            or shapes[0][1] != shapes[1][1] or shapes[0][0] > shapes[1][0]):
        return False
    oracle = receipt.get("oracle")
    if (not isinstance(oracle, dict) or set(oracle) != {"values_sha256", "receipts", "statistics"}
            or not digest(oracle["values_sha256"]) or not isinstance(oracle["statistics"], dict)
            or not isinstance(oracle["receipts"], list) or len(oracle["receipts"]) != 3):
        return False
    captures = receipt.get("source_capture")
    expected_names = {"twse_daily_ohlcv.parquet", "tpex_daily_ohlcv.parquet",
        "tw_corporate_action_reference.parquet", "twse_delisted_company.parquet",
        "tpex_delisted_company.parquet", "twse_listed_company_basic.parquet",
        "tpex_basic_company.parquet", "twse_api_company_newlisting.parquet",
        "twse_taiex_ohlc.parquet", "twse_taiex_ohlc.summary.json"}
    if (not isinstance(captures, list) or len(captures) != len(expected_names)
            or any(not isinstance(proof, dict) or not digest(proof.get("sha256"))
                   or any(not isinstance(proof.get(key), str) or not Path(proof[key]).is_absolute()
                          for key in ("path", "original")) for proof in captures)
            or {Path(proof["path"]).name for proof in captures} != expected_names):
        return False
    reference = receipt.get("fixed_pre_change_reference")
    if reference is not None and (not isinstance(reference, dict) or not digest(reference.get("sha256"))
            or type(reference.get("size")) is not int or reference["size"] <= 0
            or not isinstance(reference.get("path"), str) or not Path(reference["path"]).is_absolute()):
        return False
    work = receipt.get("work")
    if not isinstance(work, dict) or set(work) != {"previous", "daily", "unchanged"}:
        return False
    for name, mode, rows in (("previous", "full", shapes[0][0]),
                             ("daily", "suffix", shapes[1][0]), ("unchanged", "unchanged", shapes[1][0])):
        group = work[name]
        projection = group.get("merge_projection") if isinstance(group, dict) else None
        if (not isinstance(projection, dict) or group.get("contract") != "canonical_row_local_parquet_group_projection_v1"
                or projection.get("contract") != "official_ohlcv_exact_session_merge_v1"
                or projection.get("mode") != mode or projection.get("full_source_receipts_required") is not True
                or any(type(projection.get(key)) is not int or projection[key] < 0
                       for key in ("reused_rows", "rebuilt_rows", "old_generations_removed"))
                or projection["old_generations_removed"] != 0
                or projection["reused_rows"] + projection["rebuilt_rows"] != rows
                or (name == "previous" and projection["reused_rows"] != 0)
                or (name == "unchanged" and projection["rebuilt_rows"] != 0)):
            return False
    return True


def _completed_panel_storage(objects: dict) -> bool:
    """Recognize the existing array writer's terminal, whole-readback receipt.

    This older producer has no contract key or fixture.json. Its exact receipt
    shape, ABBA order and progress sidecar establish completion only. Captured
    bytes, cold reconstruction and current consumers are verified by the
    unchanged archive/retirement owner, independently of these declarations.
    """
    def digest(value):
        return (isinstance(value, str) and len(value) == 64
                and all(character in "0123456789abcdef" for character in value))

    def duration(value):
        return type(value) in (int, float) and math.isfinite(value) and value >= 0

    receipt = objects.get("acceptance.json", {})
    progress = objects.get("progress.json", {})
    # The first producer already recorded terminal ABBA/full-value readback,
    # but did not record serialization settings or code identity. Recognize
    # that complete original shape; never repair its JSON or infer those facts.
    # A partially missing modern header must not fall back to this generation.
    policy_fields = {"source_code", "chunk_bytes", "minimum_chunked_bytes"}
    original = not policy_fields.intersection(receipt)
    if original and set(receipt) != {
            "state", "order", "observed_at_utc", "original_values_receipts_preserved",
            "reader_rollout_required_before_writer_opt_in", "provider_calls",
            "production_cache_mutations", "dates", "symbols", "features",
            "source_manifest", "source_proofs", "records"}:
        return False
    if original:
        try:
            observed = datetime.fromisoformat(receipt["observed_at_utc"])
        except (TypeError, ValueError):
            return False
        if observed.tzinfo is None:
            return False
    if (receipt.get("state") != "accepted_complete_panel_storage_workflow"
            or receipt.get("order") != "ABBA"
            or receipt.get("original_values_receipts_preserved") is not True
            or receipt.get("reader_rollout_required_before_writer_opt_in") is not True
            or any(type(receipt.get(key)) is not int or receipt[key] != 0
                   for key in ("provider_calls", "production_cache_mutations"))
            or any(type(receipt.get(key)) is not int or receipt[key] <= 0
                   for key in (("dates", "symbols", "features") if original else
                               ("dates", "symbols", "features", "chunk_bytes", "minimum_chunked_bytes")))
            or not isinstance(receipt.get("source_manifest"), str)
            or not Path(receipt["source_manifest"]).is_absolute()):
        return False
    code, proofs, records = (receipt.get(key) for key in ("source_code", "source_proofs", "records"))
    if ((not original and (not isinstance(code, dict) or set(code) != {
                "scripts/benchmark_panel_chunk_storage.py", "stockagent/data/panel_cache.py",
                "stockagent/data/panel_chunks.py"}
            or not all(digest(value) for value in code.values())))
            or not isinstance(proofs, list) or not proofs
            or not isinstance(records, list) or len(records) != 4
            or progress.get("records") != records):
        return False
    paths = set()
    for proof in proofs:
        if (not isinstance(proof, dict) or not isinstance(proof.get("path"), str)
                or not Path(proof["path"]).is_absolute() or proof["path"] in paths
                or not digest(proof.get("sha256")) or not isinstance(proof.get("metadata"), dict)
                or type(proof["metadata"].get("size")) is not int or proof["metadata"]["size"] < 0):
            return False
        paths.add(proof["path"])
    if receipt["source_manifest"] not in paths:
        return False
    hashes = None
    for kind, record in zip(("whole", "chunks", "chunks", "whole"), records, strict=True):
        if original and (not isinstance(record, dict) or set(record) != {
                "kind", "stages", "load_seconds", "full_readback_verification_seconds",
                "complete_array_hashes", "complete_axes_equal", "features_memmap",
                "peak_process_rss_bytes", "post_measurement_exact_links"}
                or type(record.get("features_memmap")) is not bool
                or type(record.get("peak_process_rss_bytes")) is not int
                or record["peak_process_rss_bytes"] <= 0
                or type(record.get("post_measurement_exact_links")) is not int
                or record["post_measurement_exact_links"] < 0):
            return False
        if (not isinstance(record, dict) or record.get("kind") != kind
                or record.get("complete_axes_equal") is not True
                or not all(duration(record.get(key)) for key in (
                    "load_seconds", "full_readback_verification_seconds"))
                or not isinstance(record.get("complete_array_hashes"), dict)
                or not record["complete_array_hashes"]
                or not all(isinstance(key, str) and bool(key) and digest(value)
                           for key, value in record["complete_array_hashes"].items())
                or not isinstance(record.get("stages"), dict)
                or set(record["stages"]) != {"initial", "append"}):
            return False
        if hashes is not None and record["complete_array_hashes"] != hashes:
            return False
        hashes = record["complete_array_hashes"]
        for stage in record["stages"].values():
            if (not isinstance(stage, dict)
                    or not all(duration(stage.get(key)) for key in ("wall_seconds", "cpu_seconds"))
                    or not isinstance(stage.get("storage_build"), dict)
                    or set(stage["storage_build"]) != {"contract", "written_arrays", "written_array_bytes",
                        "reused_arrays", "avoided_array_bytes", "written_chunks", "written_chunk_bytes",
                        "reused_chunks", "avoided_chunk_bytes"}
                    or stage["storage_build"].get("contract") != (
                        "panel_time_chunk_reuse_v2" if kind == "chunks" else "panel_array_reuse_v1")
                    or not all(type(value) is int and value >= 0
                               for key, value in stage["storage_build"].items() if key != "contract")):
                return False
    return True


def _completed_research_staging(receipt: dict) -> bool:
    """The existing real-byte staging producer writes one terminal receipt.

    Its ABI has inline ABBA samples instead of fixture/trial sidecars. This
    recognizes that bounded producer's completion, never a full training run
    or cold recovery; the archive owner still verifies every captured byte.
    """
    def digest(value):
        return (isinstance(value, str) and len(value) == 64
                and all(character in "0123456789abcdef" for character in value))

    def duration(value):
        return type(value) in (int, float) and math.isfinite(value) and value >= 0

    source, codes, samples = (receipt.get(name) for name in ("source", "code_sha256", "samples"))
    if (receipt.get("contract") != RESEARCH_STAGING_CONTRACT
            or receipt.get("status") != "accepted_real_byte_staging_not_full_pipeline"
            or type(receipt.get("provider_calls")) is not int or receipt["provider_calls"] != 0
            or receipt.get("production_writes") is not False
            or type(receipt.get("service_restarts")) is not int or receipt["service_restarts"] != 0
            or receipt.get("baseline_has_no_explicit_fsync") is not True
            or receipt.get("candidate_new_writes_fsync") is not True
            or not digest(receipt.get("baseline_ast_sha256"))
            or not isinstance(source, dict) or type(source.get("bytes")) is not int
            or source["bytes"] <= 0 or not digest(source.get("sha256"))
            or not isinstance(source.get("path"), str) or not Path(source["path"]).is_absolute()
            or not isinstance(codes, dict) or len(codes) != 3
            or any(not isinstance(name, str) or not Path(name).is_absolute()
                   or not digest(value) for name, value in codes.items())
            or {Path(name).name for name in codes} != {
                "benchmark_research_stage_copy.py", "stage_tw_public_research_release.py",
                "immutable_cache_files.py"}
            or not isinstance(samples, list) or len(samples) != 4):
        return False
    for variant, sample in zip(("baseline", "candidate", "candidate", "baseline"), samples, strict=True):
        if (not isinstance(sample, dict) or sample.get("variant") != variant
                or not all(duration(sample.get(name)) for name in ("wall_seconds", "cpu_seconds"))
                or type(sample.get("written_payload_bytes")) is not int
                or sample["written_payload_bytes"] != (source["bytes"] if variant == "baseline" else 0)):
            return False
    return all(isinstance(receipt.get(name), dict)
               and all(duration(receipt[name].get(key)) for key in ("wall_seconds", "cpu_seconds"))
               for name in ("baseline_seed", "candidate_seed"))


def _completed_public_builder(source: Path, objects: dict, contents: dict) -> bool:
    """Bind current independent verification to every original completed trial.

    The oldest trial can have a different harness. It must preserve the same
    complete source/output bytes but is never pooled into that timing comparison.
    This admits metadata only; full cold/source and retirement gates still apply.
    """
    paths = _public_builder_receipts(source)
    if len(paths) != 2 or set(objects) != {"fixture.json", *(p.name for p in paths)}:
        return False
    fixture, proof, evidence = (objects["fixture.json"], objects[paths[0].name], objects[paths[1].name])
    digest = lambda value: isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None
    if (fixture.get("contract") != "tw_public_private_full_builder_fixture_v1"
            or proof.get("contract") != PUBLIC_BUILDER_CONTRACT
            or proof.get("state") != "accepted_exact_output"
            or proof.get("proof_method") != "complete_parquet_bytes"
            or proof.get("exact_output_parity") is not True
            or proof.get("repeated_per_variant") is not True
            or proof.get("fixture") != str(source)
            or proof.get("fixture_sha256") != hashlib.sha256(contents["fixture.json"]).hexdigest()
            or evidence.get("contract") != "tw_public_builder_frozen_retirement_evidence_v1"
            or evidence.get("source") != str(source)
            or evidence.get("fixture_sha256") != proof["fixture_sha256"]
            or evidence.get("verification_sha256") != hashlib.sha256(contents[paths[0].name]).hexdigest()
            or evidence.get("source_files") != fixture.get("source_files")
            or any(type(row.get("provider_calls")) is not int or row["provider_calls"] != 0
                   or row.get("production_writes") is not False for row in (proof, evidence))):
        return False
    source_files, rows = fixture.get("source_files"), evidence.get("trials")
    if (not isinstance(source_files, list) or not source_files
            or len(source_files) != proof.get("source_files")
            or not isinstance(rows, list) or not 2 <= len(rows) <= 64):
        return False
    original, outputs, reference = {}, set(), None
    for row in rows:
        if not isinstance(row, dict):
            return False
        name, raw = row.get("trial"), row.get("receipt_raw")
        if (not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", name)
                or name in original or not isinstance(raw, str) or len(raw) > 1024**2
                or hashlib.sha256(raw.encode()).hexdigest() != row.get("receipt_sha256")):
            return False
        try:
            receipt = json.loads(raw)
        except (ValueError, RecursionError):
            return False
        if (not isinstance(receipt, dict) or receipt.get("state") != "completed_trial"
                or receipt.get("build_mode") != "full"
                or receipt.get("variant") not in {"original", "streaming", "eager"}
                or receipt.get("original_code_sha256") != fixture.get("original_code_sha256")
                or receipt.get("symbol_universe_receipt") != fixture.get("symbol_universe")
                or type(receipt.get("provider_calls")) is not int or receipt["provider_calls"] != 0
                or receipt.get("production_writes") is not False
                or receipt.get("model_runtime_imported") is not False
                or any(type(receipt.get(key)) is not int or receipt[key] <= 0 for key in ("rows", "features"))
                or receipt.get("rows") != proof.get("rows") or receipt.get("features") != proof.get("feature_count")
                or not isinstance(receipt.get("source_receipts"), list) or not receipt["source_receipts"]):
            return False
        output = receipt.get("output_receipt")
        if (not isinstance(output, dict) or output.get("name") != "features.parquet"
                or type(output.get("size")) is not int or output["size"] <= 0
                or not digest(output.get("sha256")) or row.get("verified_output") != output):
            return False
        scope = {key: receipt.get(key) for key in ("source_receipts", "symbol_universe_receipt",
            "rows", "stock_rows", "market_rows", "features", "pool_profile", "polars_threads", "arrow_threads")}
        if reference is not None and scope != reference:
            return False
        reference = scope
        outputs.add((output["size"], output["sha256"]))
        original[name] = (row, receipt)
    measured, counts, harnesses = proof.get("measurements"), defaultdict(int), set()
    if not isinstance(measured, list) or not 2 <= len(measured) <= len(original):
        return False
    names = set()
    for measurement in measured:
        if not isinstance(measurement, dict) or measurement.get("trial") not in original:
            return False
        name = measurement["trial"]
        row, receipt = original[name]
        if (name in names or measurement.get("variant") != receipt["variant"]
                or measurement.get("receipt_sha256") != row["receipt_sha256"]
                or measurement.get("output") != receipt["output_receipt"]
                or not digest(receipt.get("harness_code_sha256"))):
            return False
        names.add(name)
        counts[receipt["variant"]] += 1
        harnesses.add(receipt["harness_code_sha256"])
    return len(outputs) == len(harnesses) == 1 and counts["original"] >= 2 and all(v >= 2 for v in counts.values())


def _completed_comparison(fixture: dict, accepted: dict, objects: dict) -> bool:
    """Recognize producer receipts, not an assumed equality of contract names.

    The canonical OpenBB selector benchmark deliberately uses a fixture ABI and
    a different comparison ABI. Bind that exact pair to its original sidecars;
    do not admit arbitrary mismatched contracts or infer raw-payload validity.
    """
    fixture_contract = "openbb_private_full_manifest_source_selector_v1"
    comparison_contract = "openbb_full_manifest_unassigned_source_abba_v1"
    if (fixture.get("contract") != fixture_contract
            and accepted.get("contract") != comparison_contract):
        return (isinstance(accepted.get("state"), str)
                and accepted["state"].startswith("accepted_")
                and isinstance(fixture.get("contract"), str) and bool(fixture["contract"])
                and accepted.get("contract") == fixture["contract"]
                and isinstance(accepted.get("trials"), list) and bool(accepted["trials"])
                and all(isinstance(row, dict) and row.get("state") == "completed_trial"
                        for row in accepted["trials"]))

    def sha256(value):
        return (isinstance(value, str) and len(value) == 64
                and all(character in "0123456789abcdef" for character in value))

    if (fixture.get("contract") != fixture_contract
            or accepted.get("contract") != comparison_contract
            or accepted.get("state") != "accepted_bounded_phase_comparison"
            or not all(sha256(fixture.get(name))
                       for name in ("manifest_sha256", "selector_sha256", "module_sha256"))
            or type(fixture.get("tasks")) is not int or fixture["tasks"] <= 0
            or type(accepted.get("tasks")) is not int or accepted["tasks"] != fixture["tasks"]
            or accepted.get("fixture_manifest_sha256") != fixture["manifest_sha256"]
            or accepted.get("exact_full_ordered_source_parity") is not True
            or any(type(row.get("provider_calls")) is not int or row["provider_calls"] != 0
                   or row.get("production_writes") is not False for row in (fixture, accepted))):
        return False
    names = ("01-original.json", "02-candidate.json", "03-candidate.json", "04-original.json")
    variants = ("original", "candidate", "candidate", "original")
    trials = accepted.get("trials")
    if not isinstance(trials, list) or len(trials) != len(names):
        return False
    parity = set()
    for name, variant, row in zip(names, variants, trials, strict=True):
        if (not isinstance(row, dict) or row.get("name") != name
                or {key: value for key, value in row.items() if key != "name"} != objects.get(name)
                or row.get("state") != "completed_trial" or row.get("variant") != variant
                or type(row.get("limit")) is not int or row["limit"] != 32768
                or row.get("fixture_manifest_sha256") != fixture["manifest_sha256"]
                or row.get("shared_code_sha256") != fixture["module_sha256"]
                or type(row.get("provider_calls")) is not int or row["provider_calls"] != 0
                or row.get("production_writes") is not False
                or row.get("model_runtime_imported") is not False
                or type(row.get("selected_files")) is not int or row["selected_files"] < 0
                or not sha256(row.get("full_ordered_source_contract_sha256"))):
            return False
        parity.add((row["selected_files"], row["full_ordered_source_contract_sha256"]))
    return len(parity) == 1


def completed_benchmark_evidence(source: Path, *, destination: Path | None = None,
                                 required_contract: str | None = None,
                                 original_source: Path | None = None) -> dict:
    """Bounded exact receipts for a completed case, never a numerical oracle.

    Shared by capture admission and local retirement: incomplete/unknown work
    cannot pass merely because a caller skipped the outer command's review.
    Full original/cold hashes and consumer gates are independent. Only small
    producer JSON is retained locally; mutable working DBs are never linked.
    """
    if source.resolve(strict=True) != source.absolute():
        raise SnapshotError('benchmark evidence source is redirected')
    if original_source is not None:
        contracts = {FINMIND_GROUP_STATUS_CONTRACT, FINMIND_THROUGHPUT_RESERVATION_CONTRACT,
                     PUBLIC_BUILDER_CONTRACT, TAIFEX_PROOF_MEMO_CONTRACT, *WORKFLOW_ROUND_CONTRACTS.values()}
        if (required_contract not in contracts
                or original_source.resolve(strict=False) != original_source.absolute()
                or not (_completion_receipt_paths(original_source) or _taifex_proof_memo_case(original_source)
                        or _workflow_round_contract(original_source))):
            raise SnapshotError('retained comparison requires its exact original producer binding')
        if source.name != 'retained-evidence':
            _finmind_group_quarantine_origin(source, original_source)
    records, objects, contents = [], {}, {}
    def unique(pairs):
        value = {}
        for key, child in pairs:
            if key in value:
                raise ValueError("duplicate benchmark evidence key")
            value[key] = child
        return value
    producer_source = original_source if original_source is not None else source
    # These producers include giant JSON measurement payloads. Preserve every
    # original in the full archive; retain only their three bounded completion
    # receipts on C. Do not parse or copy a 200 MB manifest as control metadata.
    paths = ([source / name for name in WORKFLOW_ROUND_RECEIPTS]
             if _workflow_round_contract(producer_source) else sorted(source.glob("*.json")))
    external = _completion_receipt_paths(
        original_source if original_source is not None and source.name != 'retained-evidence' else source)
    if any(path.name in {p.name for p in paths} for path in external):
        raise SnapshotError('external producer comparison collides with fixture metadata')
    paths += external
    paths.sort(key=lambda path: path.name)
    if len(paths) > 128:
        raise SnapshotError('benchmark receipt count exceeds its bound')
    total = 0
    for path in paths:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
        with os.fdopen(descriptor, 'rb') as handle:
            before = os.fstat(handle.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size > 8 * 1024**2:
                raise SnapshotError('benchmark evidence is not bounded regular metadata')
            total += before.st_size
            if total > 32 * 1024**2:
                raise SnapshotError('benchmark evidence exceeds its total byte budget')
            raw = handle.read(before.st_size + 1)
            signature = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
            if (len(raw) != before.st_size or signature(before) != signature(os.fstat(handle.fileno()))
                    or signature(before) != signature(path.lstat())):
                raise SnapshotError('benchmark evidence changed during capture')
        try:
            value = json.loads(raw, object_pairs_hook=unique)
        except (ValueError, RecursionError) as error:
            raise SnapshotError('benchmark evidence is not readable JSON') from error
        if not isinstance(value, dict):
            raise SnapshotError('benchmark evidence is not an object')
        objects[path.name] = value
        contents[path.name] = raw
        records.append({'path': path.name, 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()})
    fixture, accepted = objects.get('fixture.json', {}), objects.get('abba-receipt.json', {})
    staging = objects.get('receipt.json', {})
    if _completed_research_staging(staging):
        producer_contract, producer_state = staging['contract'], staging['status']
    elif _completed_panel_storage(objects):
        producer_contract = PANEL_STORAGE_CONTRACT
        producer_state = objects['acceptance.json']['state']
    elif _completed_official_daily(objects):
        producer_contract = OFFICIAL_DAILY_CONTRACT
        producer_state = staging['status']
    elif _completed_finmind_reservation(objects):
        producer_contract = FINMIND_RESERVATION_CONTRACT
        producer_state = "measurement_complete"
    elif _completed_finmind_group(original_source if original_source is not None else source, objects):
        producer_contract = FINMIND_GROUP_STATUS_CONTRACT
        producer_state = "measurement_complete"
    elif _completed_finmind_throughput(original_source if original_source is not None else source, objects):
        producer_contract = FINMIND_THROUGHPUT_RESERVATION_CONTRACT
        producer_state = "measurement_complete"
    elif _completed_public_builder(original_source if original_source is not None else source, objects, contents):
        producer_contract, producer_state = PUBLIC_BUILDER_CONTRACT, "accepted_exact_output"
    elif _completed_taifex_proof_memo(original_source if original_source is not None else source, objects):
        producer_contract, producer_state = TAIFEX_PROOF_MEMO_CONTRACT, "measurement_complete"
    elif _completed_workflow_round(producer_source, objects, contents):
        producer_contract = _workflow_round_contract(producer_source)
        producer_state = "verified_scoped_engineering"
    elif _completed_comparison(fixture, accepted, objects):
        producer_contract, producer_state = fixture['contract'], accepted['state']
    else:
        raise SnapshotError('benchmark has no supported completed producer comparison receipt')
    if required_contract is not None and producer_contract != required_contract:
        raise SnapshotError('benchmark has no supported completed producer for this standalone case')
    if destination is not None:
        if destination.resolve(strict=False) != destination.absolute():
            raise SnapshotError('benchmark evidence destination is redirected')
        destination.mkdir(mode=0o700, parents=True, exist_ok=True)
        for name, raw in contents.items():
            target = destination / name
            if target.exists() or target.is_symlink():
                info = target.lstat()
                if (not stat.S_ISREG(info.st_mode) or info.st_size != len(raw)
                        or info.st_nlink != 1 or target.read_bytes() != raw):
                    raise SnapshotError('preserved benchmark evidence differs; reconcile the original receipt')
            else:
                atomic_write_bytes(target, raw, mode=0o600)
    original_panel = (producer_contract == PANEL_STORAGE_CONTRACT
                      and 'source_code' not in objects['acceptance.json'])
    return {'contract': 'retained_benchmark_evidence_v1', 'files': records, 'bytes': total,
            'producer_contract': producer_contract, 'producer_state': producer_state,
            'scope': 'producer_declared_completion_not_benchmark_reexecution',
            **({'producer_receipt_format': 'panel_original_abba_without_policy_v1',
                'source_code_identity': 'not_recorded_by_original_producer',
                'chunk_policy': 'not_recorded_by_original_producer'} if original_panel else {})}


def _relative(value: str) -> Path:
    if not isinstance(value, str):
        raise ValueError("fixture member must be a relative path")
    parsed = PurePosixPath(value)
    if not value or parsed.is_absolute() or any(p in {"", ".", ".."} for p in value.split("/")):
        raise ValueError("fixture member escapes its experiment")
    return Path(*parsed.parts)


def _metadata(path: Path) -> dict:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    with os.fdopen(descriptor, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_METADATA_BYTES:
            raise ValueError("unsafe fixture input receipt")
        raw = handle.read(MAX_METADATA_BYTES + 1)
        if len(raw) > MAX_METADATA_BYTES:
            raise ValueError("fixture input receipt grew beyond its bound")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("fixture input receipt is not an object")
    return value


class ImmutableFixtureInputs:
    """One fresh private capture; copied/linked members are immutable thereafter.

    Only source inputs are eligible. Mutable SQLite variants and trial outputs
    must never be passed to this owner. Unknown, busy, writable or corrupt donors
    are misses, not reasons to trust another experiment's acceptance result.
    """

    def __init__(self, root: Path):
        self.root = root.absolute()
        if self.root.resolve(strict=True) != self.root or not self.root.is_dir():
            raise ValueError("fixture root is redirected/not a directory")
        if (self.root / RECEIPT).exists():
            raise ValueError("preserve committed inputs; choose a fresh experiment")
        self.members: list[dict] = []
        self._names: set[str] = set()
        self._committed = False
        self.copied = self.reused = self.copied_bytes = self.reused_bytes = 0
        self.scanned_roots = 0
        self.scan_truncated = False
        self._donors = defaultdict(list)
        self._lock = None

    def __enter__(self):
        path = self.root / ".immutable-inputs.lock"
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        self._lock = os.fdopen(descriptor, "a+b")
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError("unsafe fixture input lock")
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._index_siblings()
            return self
        except BaseException:
            self._lock.close()
            self._lock = None
            raise

    def __exit__(self, *_exc):
        if self._lock is not None:
            self._lock.close()
            self._lock = None

    def _index_siblings(self):
        with os.scandir(self.root.parent) as entries:
            for entry in entries:
                if not entry.is_dir(follow_symlinks=False) or Path(entry.path) == self.root:
                    continue
                if self.scanned_roots >= MAX_DONOR_ROOTS:
                    self.scan_truncated = True
                    break
                self.scanned_roots += 1
                donor = Path(entry.path)
                try:
                    if donor.resolve(strict=True) != donor:
                        continue
                    legacy = not (donor / RECEIPT).exists()
                    if legacy:
                        # The existing full-public capture already sealed its
                        # sources readonly and verifies every byte in workers.
                        # Reuse those copies too; do not recopy merely to add
                        # this new storage receipt to an old experiment.
                        old = _metadata(donor / "fixture.json")
                        if old.get("contract") != "tw_public_private_full_builder_fixture_v1":
                            continue
                        value = {"files": [dict(item, name=f"source/{item['name']}")
                                           for item in old["source_files"]]}
                    else:
                        value = _metadata(donor / RECEIPT)
                        if value.get("contract") != CONTRACT or value.get("state") != "committed_inputs":
                            continue
                    for item in value["files"]:
                        name, size, digest = _relative(item["name"]), item["size"], item["sha256"]
                        if (type(size) is not int or size < 0 or not isinstance(digest, str)
                                or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest)):
                            continue
                        path = donor / name
                        if path.resolve(strict=False) == path:
                            self._donors[(digest, size)].append((donor, path, legacy))
                except (OSError, ValueError, KeyError, TypeError):
                    continue

    @staticmethod
    @contextmanager
    def _donor_guard(donor: Path, legacy: bool):
        if legacy:
            yield  # The old capture never mutates its sealed input names.
            return
        descriptor = os.open(donor / ".immutable-inputs.lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError("unsafe donor input lock")
            fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
            yield

    def _reuse(self, destination: Path, digest: str, size: int) -> bool:
        for donor, path, legacy in self._donors[(digest, size)]:
            try:
                with self._donor_guard(donor, legacy):
                    if link_immutable_payload(path, destination, sha256=digest, size=size):
                        return True
            except ValueError:
                continue
            except OSError as exc:
                if exc.errno not in {errno.ENOENT, errno.EACCES, errno.EPERM, errno.EAGAIN,
                                     errno.ELOOP, errno.ENOTDIR, errno.ESTALE}:
                    raise  # ENOSPC/EIO during publication must not become a cache hit/miss.
        return False

    def copy(self, source: Path, name: str, *, expected_sha256: str | None = None) -> dict:
        if self._lock is None or self._committed:
            raise RuntimeError("fixture capture is unlocked/already committed")
        relative = _relative(name)
        if name in self._names:
            raise ValueError("duplicate fixture input")
        target = self.root / relative
        if target.resolve(strict=False) != target or os.path.lexists(target):
            raise ValueError("fixture input already exists or is redirected")
        before = payload_info(source)
        digest = stable_payload_hash(source, before)
        if expected_sha256 is not None and digest != expected_sha256:
            raise ValueError("source differs from the selected fixture input")
        target.parent.mkdir(parents=True, exist_ok=True)
        reused = self._reuse(target, digest, before["size"])
        if not reused:
            temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.partial")
            try:
                with temporary.open("xb") as writer, source.open("rb") as reader:
                    shutil.copyfileobj(reader, writer, 8 * 1024**2)
                    writer.flush()
                    os.fchmod(writer.fileno(), 0o400)
                    os.fsync(writer.fileno())
                copied = payload_info(temporary)
                if copied["size"] != before["size"] or stable_payload_hash(temporary, copied) != digest:
                    raise ValueError("private fixture copy differs from its selected source")
                os.link(temporary, target, follow_symlinks=False)
                _fsync_directory(target.parent)
            finally:
                temporary.unlink(missing_ok=True)
        after = payload_info(source)
        if after != before:
            # An already frozen source may itself own the selected donor inode.
            # Adding its alias changes nlink/ctime only. Mutable sources retain
            # the strict complete signature fence, including ctime.
            linked = payload_info(target)
            if (not reused or before["mode"] & 0o222
                    or (after["device"], after["inode"]) != (linked["device"], linked["inode"])
                    or any(after[key] != before[key] for key in (
                        "device", "inode", "size", "mtime_ns", "mode", "uid", "gid", "xattrs"))
                    or stable_payload_hash(source, after) != digest):
                raise ValueError("source changed during fixture capture")
        item = {"name": name, "size": before["size"], "sha256": digest}
        self.members.append(item)
        self._names.add(name)
        if reused:
            self.reused += 1
            self.reused_bytes += before["size"]
        else:
            self.copied += 1
            self.copied_bytes += before["size"]
        return item

    def commit(self) -> dict:
        if self._lock is None or (self.root / RECEIPT).exists():
            raise ValueError("fixture capture is unlocked/already committed")
        # No partial source set can seed a later capture after a crash.
        for item in self.members:
            path = self.root / item["name"]
            info = payload_info(path)
            if info["mode"] & 0o222 or info["size"] != item["size"] or stable_payload_hash(path, info) != item["sha256"]:
                raise ValueError("fixture input changed before commit")
        result = {"contract": CONTRACT, "state": "committed_inputs", "files": self.members,
                  "copied_files": self.copied, "copied_bytes": self.copied_bytes,
                  "reused_files": self.reused, "reused_bytes": self.reused_bytes,
                  "donor_roots_scanned": self.scanned_roots,
                  "donor_scan_truncated": self.scan_truncated}
        atomic_write_json(self.root / RECEIPT, result, durable=True)
        self._committed = True
        return result
