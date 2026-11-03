"""Bind bounded option-object rechecks to existing source audit evidence.

The metadata sidecar is not a provider announcement or a downloader.  Only the
canonical Complement owner applies the resulting exact tasks, under its own
lock, quota and capacity gates. An empty result is never treated as a repair.
"""
from __future__ import annotations

import csv
from datetime import datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
import re
from functools import lru_cache

from downloader.artifact_io import sha256_file


CONTRACT = 'finmind_option_source_conflict_recheck_v1'
SIDECAR = 'option_source_conflict_rechecks.json'
DAILY_URL = 'https://www.taifex.com.tw/cht/3/optDailyMarketView'
PRODUCT_CONTRACT = 'finmind_option_product_open_conflict_v1'
PRODUCT_BATCHES = 'option_product_conflict_batches'
CONTRACT_CONFLICT = 'finmind_option_contract_open_conflict_v1'
CONTRACT_BATCHES = 'option_contract_conflict_batches'
RECENT_CONFLICT = 'finmind_option_recent_tick_conflict_v1'
RECENT_BATCHES = 'option_recent_tick_conflict_batches'


def _stamp(value: str) -> datetime:
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('source-conflict timestamps require a timezone')
    return stamp


def _within(base: Path, name: str) -> Path:
    path = Path(name)
    path = (base / path).resolve() if not path.is_absolute() else path.resolve()
    if not path.is_relative_to(base.resolve()):
        raise ValueError('source-conflict evidence is outside the workspace')
    return path


@lru_cache(maxsize=32)
def _recent_positive_products(path_name: str, source_sha: str, day: str) -> tuple[str, ...]:
    """Identify source-backed same-calendar-day candidate products, not fills.

    The caller hashes the ZIP on every admission. This cache only avoids
    repeatedly parsing the same verified bytes at each owner scheduling pass.
    """
    import io
    import zipfile

    products = set()
    with zipfile.ZipFile(path_name) as archive:
        members = [item for item in archive.infolist() if item.filename.lower().endswith('.csv')]
        if len(members) != 1 or members[0].file_size > 2_000_000_000:
            raise ValueError('ambiguous or oversized official recent option archive')
        with archive.open(members[0]) as binary:
            for row in csv.reader(io.TextIOWrapper(binary, encoding='cp950', newline='')):
                if len(row) < 9 or row[0].strip() != day.replace('-', ''):
                    continue
                product = row[1].strip().upper()
                if product in products:
                    continue
                clock, right = row[5].strip(), row[4].strip()
                if (not re.fullmatch(r'[A-Z0-9]{2,8}', product) or right not in {'C', 'P'}
                        or not clock.isdigit() or not 84500 <= int(clock) <= 161500):
                    continue
                try:
                    price, quantity = float(row[6]), int(row[7])
                except ValueError:
                    continue
                if math.isfinite(price) and price > 0 and quantity > 0:
                    products.add(product)
    if not products:
        raise ValueError('recent option archive has no positive same-day candidate events')
    return tuple(sorted(products))


def load_recent_option_conflicts(root: Path, now: datetime, *, sidecar: Path):
    """Bind exact empty-day rechecks to retained exchange tick ZIPs and receipts."""
    from datetime import date

    raw = sidecar.read_bytes()
    if len(raw) > 64 * 1024:
        raise ValueError('oversized recent option conflict plan')
    plan = json.loads(raw)
    if not isinstance(plan, dict) or plan.get('contract') != RECENT_CONFLICT:
        raise ValueError('invalid recent option conflict plan')
    created, expires = _stamp(plan['created_at_utc']), _stamp(plan['expires_at_utc'])
    if expires <= created or expires - created > timedelta(days=7):
        raise ValueError('invalid recent option conflict duration')
    if not created <= now < expires:
        return None
    workspace = root.resolve().parents[1]
    manifest = _within(workspace, plan['manifest_snapshot_path'])
    manifest_bytes = manifest.read_bytes()
    if hashlib.sha256(manifest_bytes).hexdigest() != plan['manifest_snapshot_sha256']:
        raise ValueError('recent option manifest snapshot changed')
    official = json.loads(manifest_bytes)
    dates = plan.get('dates')
    if not isinstance(dates, list) or not 0 < len(dates) <= 64:
        raise ValueError('recent option plan requires 1 to 64 dates')
    if len({item.get('date') for item in dates}) != len(dates):
        raise ValueError('duplicate recent option conflict date')
    requests = []
    for item in dates:
        day = item['date']
        if date.fromisoformat(day).isoformat() != day:
            raise ValueError('invalid recent option date')
        entries = [entry for entry in official['raw_downloads']
                   if entry.get('kind') == 'options' and entry.get('trading_date') == day]
        expected_url = ('https://www.taifex.com.tw/file/taifex/Dailydownload/'
                        'OptionsDailydownloadCSV/OptionsDaily_' + day.replace('-', '_') + '.zip')
        if len(entries) != 1 or entries[0].get('url') != expected_url:
            raise ValueError('recent option exchange source identity mismatch')
        source = _within(workspace, entries[0]['path'])
        source_sha = entries[0]['sha256']
        if sha256_file(source) != source_sha:
            raise ValueError('recent option exchange archive changed')
        products = _recent_positive_products(str(source), source_sha, day)
        if sha256_file(source) != source_sha:
            raise ValueError('recent option exchange archive changed during verification')
        receipt = _within(workspace, item['original_receipt_snapshot_path'])
        receipt_bytes = receipt.read_bytes()
        receipt_sha = hashlib.sha256(receipt_bytes).hexdigest()
        if receipt_sha != item['original_receipt_sha256']:
            raise ValueError('recent option receipt snapshot changed')
        prior = json.loads(receipt_bytes)
        if (prior.get('dataset') != 'TaiwanOptionTick' or prior.get('data_id') != ''
                or prior.get('partition') != day or prior.get('kind') != 'id_day'
                or prior.get('status') != 'observed_empty' or prior.get('rows') != 0):
            raise ValueError('recent option receipt is not the exact observed-empty task')
        identity = hashlib.sha256(f"{day}:{receipt_sha}:{source_sha}:{','.join(products)}".encode()).hexdigest()
        requests.append({
            'correction_id': f'recent-tick-conflict-{identity}', 'owner': 'complement',
            'dataset': 'TaiwanOptionTick', 'start_date': day, 'end_date': day,
            'data_ids': None, 'required_after_utc': created.isoformat(), 'allow_empty': False,
            'reason': 'official same-day positive option events versus observed-empty provider object',
            'source_url': expected_url, 'watermark_basis': 'source_conflict_detection',
            'exact_source_conflict_key': {'dataset': 'TaiwanOptionTick', 'data_id': '',
                                          'partition': day, 'kind': 'id_day'},
            'original_receipt_sha256': receipt_sha, 'expected_source_products': list(products),
        })
    if sidecar.read_bytes() != raw or manifest.read_bytes() != manifest_bytes:
        raise ValueError('recent option conflict evidence changed during verification')
    digest = hashlib.sha256(raw).hexdigest()
    return {'schema_version': 1, 'plan_id': f'recent-tick-conflict-{digest}', 'requests': requests}, digest


def load_option_conflicts(root: Path, now: datetime, *, sidecar: Path | None = None):
    """Return a correction plan for still-valid, independently pinned conflicts."""
    sidecar = sidecar or root / SIDECAR
    if not sidecar.exists():
        return None
    raw = sidecar.read_bytes()
    if len(raw) > 64 * 1024:
        raise ValueError('oversized source-conflict plan')
    plan = json.loads(raw)
    if not isinstance(plan, dict) or plan.get('contract') != CONTRACT:
        raise ValueError('invalid source-conflict plan')
    created, expires = _stamp(plan['created_at_utc']), _stamp(plan['expires_at_utc'])
    if expires <= created or expires - created > timedelta(days=7):
        raise ValueError('invalid source-conflict plan duration')
    if not created <= now < expires:
        return None
    dates = plan.get('dates')
    if not isinstance(dates, list) or not 0 < len(dates) <= 64 or len(set(dates)) != len(dates):
        raise ValueError('source-conflict plan requires 1 to 64 unique dates')
    from datetime import date
    for value in dates:
        if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
            raise ValueError('invalid source-conflict date')

    workspace = root.resolve().parents[1]
    summary_path = _within(workspace, plan['coverage_summary_path'])
    summary_bytes = summary_path.read_bytes()
    if hashlib.sha256(summary_bytes).hexdigest() != plan.get('coverage_summary_sha256'):
        raise ValueError('source-conflict coverage summary changed')
    summary = json.loads(summary_bytes)
    if (summary.get('contract') != 'taifex_option_annual_positive_days_finmind_receipts_v1'
            or Path(summary['finmind_root']).resolve() != root.resolve()):
        raise ValueError('source-conflict coverage identity mismatch')
    manifest = _within(workspace, summary['source_manifest'])
    if sha256_file(manifest) != summary['source_manifest_sha256']:
        raise ValueError('source-conflict official manifest changed')
    inventory = _within(workspace, summary['day_inventory'])
    inventory_bytes = inventory.read_bytes()
    if hashlib.sha256(inventory_bytes).hexdigest() != summary['day_inventory_sha256']:
        raise ValueError('source-conflict day inventory changed')
    for year in {value[:4] for value in dates}:
        sources = [x for x in summary['annual_sources'] if Path(x['path']).name == f'{year}_opt.zip']
        if len(sources) != 1 or sha256_file(_within(workspace, sources[0]['path'])) != sources[0]['sha256']:
            raise ValueError('source-conflict official annual archive changed')

    import io
    inventory_rows = list(csv.DictReader(io.StringIO(inventory_bytes.decode('utf-8'), newline='')))
    requests = []
    for day in dates:
        rows = [row for row in inventory_rows if row['date'] == day]
        if (len(rows) != 1 or rows[0]['finmind_status'] != 'observed_empty'
                or rows[0]['day_or_unreported_positive_candidate'] != 'True'
                or not (rows[0]['day_positive_products'] or rows[0]['unreported_session_positive_products'])):
            raise ValueError('source-conflict lacks independently positive daily evidence')
        prior_sha = rows[0]['finmind_receipt_sha256']
        if len(prior_sha) != 64 or any(c not in '0123456789abcdef' for c in prior_sha):
            raise ValueError('source-conflict original receipt digest is missing')
        products = sorted({product for field in ('day_positive_products', 'unreported_session_positive_products')
                           for product in rows[0][field].split('|') if product})
        identity = hashlib.sha256(
            f"{day}:{prior_sha}:{plan['coverage_summary_sha256']}:{','.join(products)}".encode()).hexdigest()
        requests.append({
            'correction_id': f'source-conflict-{identity}', 'owner': 'complement',
            'dataset': 'TaiwanOptionTick', 'start_date': day, 'end_date': day,
            'data_ids': None, 'required_after_utc': created.isoformat(),
            'allow_empty': False, 'reason': 'official positive option volume versus unverified missing day object',
            'source_url': DAILY_URL, 'watermark_basis': 'source_conflict_detection',
            'exact_source_conflict_key': {'dataset': 'TaiwanOptionTick', 'data_id': '',
                                          'partition': day, 'kind': 'id_day'},
            'original_receipt_sha256': prior_sha, 'expected_source_products': products,
        })
    if sidecar.read_bytes() != raw:
        raise ValueError('source-conflict plan changed during verification')
    digest = hashlib.sha256(raw).hexdigest()
    return {'schema_version': 1, 'plan_id': f'source-conflict-{digest}', 'requests': requests}, digest


def load_option_product_conflicts(root: Path, now: datetime, *, sidecar: Path):
    """Bind one bounded product-day recheck to the earlier immutable audits.

    The opening price is corroborating offline evidence only. A refreshed
    object passes the product check only if every requested option ID occurs;
    neither condition proves contract-level completeness or a fill.
    """
    import io
    from datetime import date

    raw = sidecar.read_bytes()
    if len(raw) > 64 * 1024:
        raise ValueError('oversized product-conflict plan')
    plan = json.loads(raw)
    if not isinstance(plan, dict) or plan.get('contract') != PRODUCT_CONTRACT:
        raise ValueError('invalid product-conflict plan')
    created, expires = _stamp(plan['created_at_utc']), _stamp(plan['expires_at_utc'])
    if expires <= created or expires - created > timedelta(days=7):
        raise ValueError('invalid product-conflict plan duration')
    if not created <= now < expires:
        return None
    dates = plan.get('dates')
    if not isinstance(dates, list) or not 0 < len(dates) <= 64 or len(set(dates)) != len(dates):
        raise ValueError('product-conflict plan requires 1 to 64 unique dates')
    for day in dates:
        if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
            raise ValueError('invalid product-conflict date')
    workspace = root.resolve().parents[1]

    def pinned_json(path_key: str, digest_key: str):
        path = _within(workspace, plan[path_key])
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != plan[digest_key]:
            raise ValueError(f'{path_key} changed')
        return json.loads(data)

    def pinned_csv(path: str, expected: str):
        data = _within(workspace, path).read_bytes()
        if hashlib.sha256(data).hexdigest() != expected:
            raise ValueError('product-conflict inventory changed')
        return list(csv.DictReader(io.StringIO(data.decode('utf-8'), newline='')))

    coverage = pinned_json('coverage_summary_path', 'coverage_summary_sha256')
    presence = pinned_json('presence_summary_path', 'presence_summary_sha256')
    opening = pinned_json('open_summary_path', 'open_summary_sha256')
    if (coverage.get('contract') != 'taifex_option_annual_positive_days_finmind_receipts_v1'
            or Path(coverage['finmind_root']).resolve() != root.resolve()
            or presence.get('contract') != 'taifex_option_daily_vs_finmind_product_presence_v1'
            or opening.get('contract') != 'taifex_option_missing_open_product_presence_v1'
            or presence.get('coverage_summary_sha256') != plan['coverage_summary_sha256']
            or opening.get('coverage_sha256') != plan['coverage_summary_sha256']
            or opening.get('presence_summary_sha256') != plan['presence_summary_sha256']
            or opening.get('presence_inventory_sha256') != presence['day_inventory_sha256']):
        raise ValueError('product-conflict audit identities disagree')
    if sha256_file(_within(workspace, coverage['source_manifest'])) != coverage['source_manifest_sha256']:
        raise ValueError('product-conflict official manifest changed')
    for year in {value[:4] for value in dates}:
        official = [x for x in coverage['annual_sources'] if Path(x['path']).name == f'{year}_opt.zip']
        open_sources = [x for x in opening['inputs'] if Path(x['path']).name == f'{year}_opt.zip']
        if (len(official) != 1 or len(open_sources) != 1
                or official[0]['sha256'] != open_sources[0]['sha256']
                or sha256_file(_within(workspace, official[0]['path'])) != official[0]['sha256']):
            raise ValueError('product-conflict official annual archive changed')
    coverage_rows = pinned_csv(coverage['day_inventory'], coverage['day_inventory_sha256'])
    presence_rows = pinned_csv(presence['day_inventory'], presence['day_inventory_sha256'])
    open_rows = pinned_csv(opening['inventory'], opening['inventory_sha256'])
    requests = []
    for day in dates:
        cov = [row for row in coverage_rows if row['date'] == day]
        pre = [row for row in presence_rows if row['date'] == day]
        verified_open = [row for row in open_rows if row['date'] == day
                         and row['classification'] == 'official_open_missing_from_object'
                         and int(row['official_positive_open_rows']) > 0]
        if (len(cov) != 1 or cov[0]['finmind_status'] != 'complete_payload_sha_verified'
                or len(pre) != 1 or pre[0]['source_status'] != 'product_readback_verified'
                or not verified_open):
            raise ValueError('product-conflict lacks verified source or official opening')
        missing_products = {row['product'] for row in verified_open}
        if not missing_products.issubset(set(pre[0]['missing_day_products'].split('|'))):
            raise ValueError('product-conflict product set disagrees with presence audit')
        prior_products = {value for value in pre[0]['finmind_products'].split('|') if value}
        if not prior_products:
            raise ValueError('product-conflict lacks prior nonempty product evidence')
        # A recheck that loses an already observed product is not a repair.
        products = sorted(missing_products | prior_products)
        prior_sha = cov[0]['finmind_receipt_sha256']
        if len(prior_sha) != 64 or any(c not in '0123456789abcdef' for c in prior_sha):
            raise ValueError('product-conflict original receipt digest is missing')
        identity = hashlib.sha256(
            f"{day}:{prior_sha}:{plan['open_summary_sha256']}:{','.join(products)}".encode()).hexdigest()
        requests.append({
            'correction_id': f'product-conflict-{identity}', 'owner': 'complement',
            'dataset': 'TaiwanOptionTick', 'start_date': day, 'end_date': day,
            'data_ids': None, 'required_after_utc': created.isoformat(), 'allow_empty': False,
            'reason': 'official ordinary opening product absent from nonempty provider object',
            'source_url': DAILY_URL, 'watermark_basis': 'source_conflict_detection',
            'exact_source_conflict_key': {'dataset': 'TaiwanOptionTick', 'data_id': '',
                                          'partition': day, 'kind': 'id_day'},
            'expected_original_task_state': 'complete', 'original_receipt_sha256': prior_sha,
            'expected_source_products': products,
            'expected_missing_source_products': sorted(missing_products),
        })
    if sidecar.read_bytes() != raw:
        raise ValueError('product-conflict plan changed during verification')
    digest = hashlib.sha256(raw).hexdigest()
    return {'schema_version': 1, 'plan_id': f'product-conflict-{digest}', 'requests': requests}, digest


def load_option_contract_conflicts(root: Path, now: datetime, *, sidecar: Path):
    """Create exact day requests from the pinned contract-key audit."""
    from collections import defaultdict
    import io
    from datetime import date

    raw = sidecar.read_bytes()
    if len(raw) > 64 * 1024:
        raise ValueError('oversized option contract-conflict plan')
    plan = json.loads(raw)
    if not isinstance(plan, dict) or plan.get('contract') != CONTRACT_CONFLICT:
        raise ValueError('invalid option contract-conflict plan')
    created, expires = _stamp(plan['created_at_utc']), _stamp(plan['expires_at_utc'])
    if expires <= created or expires - created > timedelta(days=7):
        raise ValueError('invalid option contract-conflict duration')
    if not created <= now < expires:
        return None
    dates = plan.get('dates')
    if not isinstance(dates, list) or not 0 < len(dates) <= 64 or len(set(dates)) != len(dates):
        raise ValueError('option contract-conflict requires 1 to 64 unique dates')
    for day in dates:
        if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
            raise ValueError('invalid option contract-conflict date')
    workspace = root.resolve().parents[1]

    def read_pinned(path: str, sha: str):
        data = _within(workspace, path).read_bytes()
        if hashlib.sha256(data).hexdigest() != sha:
            raise ValueError('option contract-conflict evidence changed')
        return data

    coverage_bytes = read_pinned(plan['coverage_summary_path'], plan['coverage_summary_sha256'])
    summary_bytes = read_pinned(plan['contract_summary_path'], plan['contract_summary_sha256'])
    coverage, summary = json.loads(coverage_bytes), json.loads(summary_bytes)
    def read_csv(path: str, sha: str):
        return list(csv.DictReader(io.StringIO(read_pinned(path, sha).decode('utf-8'), newline='')))
    audit_inventory = read_csv(summary['day_inventory'], summary['day_inventory_sha256'])
    audit_rows = {x['date']: x for x in audit_inventory}
    if len(audit_rows) != len(audit_inventory):
        raise ValueError('duplicate option contract-conflict audit date')
    if coverage.get('contract') == 'tw_option_all_product_monthly_range_readback_v1':
        from downloader.finmind_option_monthly_evidence import validate_sources
        if (summary.get('contract') not in {'taifex_option_monthly_open_contract_vs_finmind_v1',
                                            'taifex_option_monthly_open_contract_vs_finmind_v2'}
                or summary.get('source_readback_sha256') != plan['coverage_summary_sha256']
                or _within(workspace, summary['source_readback']) != _within(workspace, plan['coverage_summary_path'])
                or Path(summary['finmind_root']).resolve() != root.resolve()):
            raise ValueError('monthly option contract-conflict audit identity mismatch')
        validate_sources(coverage, dates, workspace, plan.get('monthly_source_pins'))
        # The monthly audit independently stores both original vendor receipt
        # identity and the contract comparison on the same date row.
        coverage_rows = audit_rows
    else:
        if (coverage.get('contract') != 'taifex_option_annual_positive_days_finmind_receipts_v1'
                or Path(coverage['finmind_root']).resolve() != root.resolve()
                or summary.get('contract') not in {'taifex_option_ordinary_open_contract_vs_finmind_v1',
                                                   'taifex_option_ordinary_open_contract_vs_finmind_v2'}
                or summary.get('coverage_summary_sha256') != plan['coverage_summary_sha256']):
            raise ValueError('option contract-conflict audit identity mismatch')
        if sha256_file(_within(workspace, coverage['source_manifest'])) != coverage['source_manifest_sha256']:
            raise ValueError('option contract-conflict source manifest changed')
        for year in {day[:4] for day in dates}:
            sources = [x for x in coverage['annual_sources'] if Path(x['path']).name == f'{year}_opt.zip']
            if len(sources) != 1 or sha256_file(_within(workspace, sources[0]['path'])) != sources[0]['sha256']:
                raise ValueError('option contract-conflict annual source changed')
        coverage_rows = {x['date']: x for x in read_csv(coverage['day_inventory'], coverage['day_inventory_sha256'])}
    keys_by_day = defaultdict(set)
    for row in read_csv(summary['missing_inventory'], summary['missing_inventory_sha256']):
        if row['date'] not in dates:
            continue
        strike = float(row['strike'])
        if (not re.fullmatch(r'[A-Z0-9]+', row['product'])
                or not re.fullmatch(r'\d{6}([WF][1-5])?', row['series'])
                or not math.isfinite(strike) or strike <= 0 or row['right'] not in {'C', 'P'}
                or row['session'] not in {'day', 'unreported'}):
            raise ValueError('invalid audited option contract key')
        keys_by_day[row['date']].add((row['product'], row['series'], strike, row['right']))
    requests = []
    for day in dates:
        cov, checked = coverage_rows.get(day), audit_rows.get(day)
        keys = sorted(keys_by_day.get(day, ()))
        if (not cov or cov['finmind_status'] != 'complete_payload_sha_verified'
                or not checked or checked['source_status'] != 'contract_key_readback_verified'
                or int(checked['missing_contracts']) != len(keys) or not keys):
            raise ValueError('option contract-conflict lacks verified exact missing keys')
        receipt_sha, parquet_sha = cov['finmind_receipt_sha256'], cov['finmind_parquet_sha256']
        if any(not re.fullmatch(r'[a-f0-9]{64}', digest) for digest in (receipt_sha, parquet_sha)):
            raise ValueError('option contract-conflict original source digest missing')
        prior = Path(cov['finmind_parquet_path']).resolve()
        if not prior.is_relative_to(root.resolve()):
            raise ValueError('option contract-conflict original object outside owner')
        identity = hashlib.sha256(f"{day}:{receipt_sha}:{plan['contract_summary_sha256']}".encode()).hexdigest()
        requests.append({
            'correction_id': f'contract-conflict-{identity}', 'owner': 'complement',
            'dataset': 'TaiwanOptionTick', 'start_date': day, 'end_date': day,
            'data_ids': None, 'required_after_utc': created.isoformat(), 'allow_empty': False,
            'reason': 'official daily open contract key absent from nonempty vendor object',
            'source_url': DAILY_URL, 'watermark_basis': 'source_conflict_detection',
            'exact_source_conflict_key': {'dataset': 'TaiwanOptionTick', 'data_id': '',
                                          'partition': day, 'kind': 'id_day'},
            'expected_original_task_state': 'complete', 'original_receipt_sha256': receipt_sha,
            'original_parquet_path': str(prior.relative_to(root.resolve())),
            'original_parquet_sha256': parquet_sha, 'expected_source_contracts': [list(x) for x in keys],
        })
    if sidecar.read_bytes() != raw:
        raise ValueError('option contract-conflict plan changed during verification')
    digest = hashlib.sha256(raw).hexdigest()
    return {'schema_version': 1, 'plan_id': f'contract-conflict-{digest}', 'requests': requests}, digest
