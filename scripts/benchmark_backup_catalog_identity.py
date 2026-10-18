"""Compare complete fixed catalog hashing, not a capture/registration speedup.

The input is a private, byte-verified copy of a genuine catalog. Neither variant
changes the source catalog, its content identity, a registry or any service.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import resource
import time

from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.benchmark_lakehouse_source_wave import limits, match_native_limits
from scripts.verify_lakehouse_workflow_history import owner_identity
from stockagent.runtime_identity import identity_sha256


def catalog_identity(content):
    """Original sorted JSON object, C-encode one complete list member at a time.

    The catalog contract has string field names. Large outer lists contain
    records; nested ordering, scalar conversion, escaping and numeric spelling
    remain owned by the same standard-library canonical JSON encoder.
    """
    if not isinstance(content, dict) or any(not isinstance(key, str) for key in content):
        raise ValueError('catalog content requires string field names')
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True,
                               separators=(',', ':'), allow_nan=False)
    digest = hashlib.sha256(b'{')
    for index, key in enumerate(sorted(content)):
        if index:
            digest.update(b',')
        digest.update(encoder.encode(key).encode('utf-8'))
        digest.update(b':')
        value = content[key]
        if isinstance(value, (list, tuple)):
            digest.update(b'[')
            for member_index, member in enumerate(value):
                if member_index:
                    digest.update(b',')
                digest.update(encoder.encode(member).encode('utf-8'))
            digest.update(b']')
        else:
            digest.update(encoder.encode(value).encode('utf-8'))
    digest.update(b'}')
    return digest.hexdigest()


def content_identity_input(catalog):
    content = {key: value for key, value in catalog.items()
               if key not in {'identity_sha256', 'observed_at_utc'}}
    content['files'] = [{key: value for key, value in row.items() if key != 'signature'}
                        for row in content['files']]
    return content


def file_sha(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def signature(path):
    observed = path.stat()
    # A read may advance atime; it must not be confused with source mutation.
    return tuple(getattr(observed, name) for name in
                 ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns'))


def validate_copy_receipt(catalog, actual_sha):
    proof = json.loads(catalog.with_name('copy-receipt.json').read_bytes())
    if (proof.get('state') != 'accepted_private_fixed_catalog_copy'
            or proof.get('source_unchanged') is not True
            or proof.get('source_sha256') != actual_sha
            or proof.get('copied_sha256') != actual_sha
            or proof.get('bytes') != catalog.stat().st_size):
        raise ValueError('fixed input lacks a verified genuine catalog copy receipt')
    return proof


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('reference', 'streamed'), required=True)
    parser.add_argument('--catalog', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    base = Path('artifacts/benchmarks').resolve()
    if (args.output.exists() or not args.output.absolute().resolve().is_relative_to(base)
            or not args.catalog.resolve().is_relative_to(base) or args.catalog.is_symlink()):
        parser.error('use a fixed private catalog and fresh receipt under artifacts/benchmarks')
    resource_limits = match_native_limits()
    owner = owner_identity()
    program = file_sha(Path(__file__))
    atomic_write_text(args.output.with_suffix('.probe.py'), Path(__file__).read_text(),
                      mode=0o600, durable=True)
    observed = signature(args.catalog)
    source_sha = file_sha(args.catalog)
    copy_proof = validate_copy_receipt(args.catalog, source_sha)
    with args.catalog.open() as source:
        catalog = json.load(source)
    content = content_identity_input(catalog)
    wall, cpu = time.perf_counter(), time.process_time()
    actual = (identity_sha256(content) if args.mode == 'reference' else catalog_identity(content))
    elapsed, spent = time.perf_counter()-wall, time.process_time()-cpu
    stable = (observed == signature(args.catalog) and source_sha == file_sha(args.catalog)
              and owner == owner_identity() and program == file_sha(Path(__file__))
              and limits() == resource_limits)
    result = {'state': 'accepted_fixed_catalog_identity' if stable and actual == catalog['identity_sha256']
              else 'rejected_fixed_catalog_identity', 'observed_at_utc': datetime.now(UTC).isoformat(),
              'mode': args.mode, 'catalog_file_sha256': source_sha, 'catalog_bytes': observed[2],
              'copy_receipt_sha256': file_sha(args.catalog.with_name('copy-receipt.json')),
              'source_copy_observed_at_utc': copy_proof['observed_at_utc'],
              'catalog_recorded_identity_sha256': catalog['identity_sha256'], 'actual_identity_sha256': actual,
              'release_count': len(catalog['releases']), 'file_count': len(catalog['files']),
              'wall_seconds': elapsed, 'cpu_seconds': spent,
              'fresh_process_peak_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
              'stable_inputs_owner_code_and_limits': stable, 'resource_limits': resource_limits,
              'program_sha256': program, 'provider_calls': 0, 'source_writes': 0,
              'catalog_writes': 0, 'service_controls': 0,
              'caveats': ['Complete fixed-catalog identity only, not complete capture/register/export.',
                          'Peak RSS includes JSON loading; timing covers the full identity operation.',
                          'Real catalog copy may differ in scope/generation from the current lake registry.']}
    atomic_write_json(args.output, result, durable=True)
    print(json.dumps(result))
    if result['state'] != 'accepted_fixed_catalog_identity':
        raise ValueError('complete original catalog identity differs or inputs changed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
