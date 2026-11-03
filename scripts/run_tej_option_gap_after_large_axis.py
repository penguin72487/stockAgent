#!/usr/bin/env python3
"""Continue one official option gap after the exact TEJ menu is read back.

The existing owner performs all provider actions. This bounded relay verifies
each retained source before preparing the next finite metadata or query plan.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from downloader.artifact_io import sha256_file
from downloader.provider_priority import inspect
from scripts.audit_tej_large_axis_capture import audit as audit_large
from scripts.audit_tej_option_axis_labels import audit as profile_menu
from scripts.build_tej_option_gap_label_candidates import build as build_candidates
from scripts.register_tej_finite_option_axis_capture import candidate_labels
from scripts.run_tej_finite_option_axis_capture_wait import wait as capture_wait
from scripts.run_tej_finite_option_priority_after_capture import wait as priority_wait


def prepare(root: Path, receipt_path: Path, official: Path, day: date,
            profile: Path, candidate_dir: Path, report: Path) -> tuple[Path, Path]:
    receipt = json.loads(receipt_path.read_text())
    snapshot = Path(receipt['snapshot_manifest_path'])
    proof = audit_large(root, snapshot)
    for key in ('contract', 'task_id', 'query_attempt_id', 'type', 'smart_id', 'table',
                'snapshot_path', 'snapshot_manifest_path', 'snapshot_manifest_sha256', 'source_proof',
                'market_observations_obtained', 'full_axis_admitted_to_plan', 'training_input_admitted'):
        if receipt.get(key) != proof.get(key):
            raise ValueError('Retained large-menu readback does not match its source')
    if not profile.exists():
        profile_menu(receipt_path, profile)
    meta = json.loads(profile.read_text())
    if (meta.get('contract') != 'tej_option_source_menu_label_profile_v1'
            or meta.get('source_receipt_sha256') != sha256_file(receipt_path)
            or meta.get('source_snapshot_sha256') != proof['source_proof']['sha256']
            or meta.get('source_table') != proof['table']
            or meta.get('implementation_sha256') != sha256_file(REPO / 'scripts/audit_tej_option_axis_labels.py')
            or meta.get('market_observations_obtained') is not False
            or meta.get('training_input_admitted') is not False):
        raise ValueError('Retained menu profile does not match its source')
    manifest = candidate_dir / 'manifest.json'
    if not manifest.exists():
        build_candidates(official, profile, candidate_dir, report)
    candidates = json.loads(manifest.read_text())
    if (not report.is_file() or json.loads(report.read_text()) != candidates
            or candidates.get('source_pins', {}).get(str(official.resolve())) != sha256_file(official)
            or candidates.get('source_pins', {}).get(str(profile.resolve())) != sha256_file(profile)):
        raise ValueError('Retained gap candidates do not match the exact inputs')
    labels, _ = candidate_labels(manifest, proof, day)
    print(json.dumps({'state': 'exact_daily_candidates_verified', 'date': day.isoformat(),
                      'labels': len(labels), 'source_queries': 0,
                      'market_observations_obtained': False}), flush=True)
    return snapshot, manifest


def run(root: Path, large_receipt: Path, upstream_run: Path, priority: str, official: Path,
        day: date, output_dir: Path, candidate_dir: Path, hours: float) -> int:
    if not 0 < hours <= 24:
        raise ValueError('A bounded wait is required')
    root, output_dir = root.resolve(strict=True), output_dir.resolve(strict=True)
    tag = day.strftime('%Y%m%d')
    profile = output_dir / f'tej_option_{tag}_source_menu_profile_v1.json'
    report = output_dir / f'tej_option_{tag}_gap_candidates_v1.json'
    witness = output_dir / f'tej_finite_option_axis_{tag}_live_readback_v1.json'
    tracked = [REPO / p for p in ('scripts/tej_smart_wizard_bridge.ps1',
        'scripts/audit_tej_large_axis_capture.py', 'scripts/audit_tej_option_axis_labels.py',
        'scripts/build_tej_option_gap_label_candidates.py', 'scripts/register_tej_finite_option_axis_capture.py',
        'scripts/run_tej_finite_option_axis_capture_wait.py',
        'scripts/run_tej_finite_option_priority_after_capture.py', 'scripts/build_tej_finite_option_priority.py',
        'downloader/tej_capture_claims.py')]
    pins = {p: sha256_file(p) for p in tracked}
    deadline = datetime.now(UTC) + timedelta(hours=hours)
    print(json.dumps({'state': 'waiting_for_exact_large_menu', 'date': day.isoformat(),
                      'deadline_utc': deadline.isoformat(), 'source_queries': 0}), flush=True)
    while not large_receipt.is_file():
        if any(sha256_file(p) != digest for p, digest in pins.items()):
            raise ValueError('Pinned source code changed during the wait')
        if datetime.now(UTC) >= deadline or not inspect(root, priority).get('priority_active'):
            return 75
        if upstream_run.is_file():
            upstream = json.loads(upstream_run.read_text())
            if upstream.get('state') in {'failed', 'cancelled', 'succeeded', 'complete', 'completed'}:
                if large_receipt.is_file():
                    break
                print(json.dumps({'state': 'large_menu_upstream_ended_without_receipt',
                                  'upstream_state': upstream['state'], 'source_queries': 0}), flush=True)
                return 1
        time.sleep(10)
    snapshot, manifest = prepare(root, large_receipt, official, day, profile, candidate_dir, report)
    remaining = (deadline - datetime.now(UTC)).total_seconds() / 3600
    if remaining <= 0:
        return 75
    if not witness.is_file():
        status = capture_wait(root, snapshot, manifest, priority, day, witness, min(remaining, 24))
        if status:
            return status
    remaining = (deadline - datetime.now(UTC)).total_seconds() / 3600
    if remaining <= 0:
        return 75
    return priority_wait(root, witness, priority, output_dir, min(remaining, 24), day)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('root', 'large-receipt', 'upstream-run', 'official-manifest', 'output-dir', 'candidate-dir'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--priority-request-id', required=True)
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--hours', type=float, default=24)
    args = parser.parse_args()
    raise SystemExit(run(args.root, args.large_receipt, args.upstream_run, args.priority_request_id,
                         args.official_manifest, args.date, args.output_dir, args.candidate_dir, args.hours))


if __name__ == '__main__':
    main()
