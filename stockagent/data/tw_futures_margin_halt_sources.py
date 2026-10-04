"""Bound additional loss-reduction sources for the canonical halt calculator."""
from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_margin_preparation import load_loss_reduction_halt_review
from stockagent.data.tw_futures_margin_release import read_bound_output

CONTRACT = 'bound_loss_reduction_halt_source_delta_v1'


def load_loss_reduction_bundle(source: Path, corporate_path: Path, bundle: Path, *, corporate_frame=None):
    """Validate originals and the same own event against the effective rule input.

    Each portable review retains a finite corporate subset. Unrelated later
    repairs may change the full input; its reviewed event must remain identical.
    """
    manifest_path = bundle / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    if (manifest.get('status') != 'source_bound_loss_reduction_halt_extension'
            or manifest.get('parent_source_manifest_sha256') != sha256_file(source / 'source_manifest.json')
            or manifest.get('implementation_sha256') != sha256_file(Path(__file__))):
        raise ValueError('loss-reduction inputs change their source/code parent')
    paths = []
    items = [*manifest['sources'], *manifest['reviews'], manifest['accounting_library_baseline']]
    covered = {}
    for item in items:
        path = (bundle / item['path']).resolve()
        if not path.is_relative_to(bundle.resolve()) or sha256_file(path) != item['sha256']:
            raise ValueError('loss-reduction input original SHA/path mismatch')
        if path in covered:
            raise ValueError('loss-reduction input repeats an original path')
        covered[path] = item['sha256']
    def require_covered(path, digest=None):
        path = path.resolve()
        if path not in covered or (digest is not None and covered[path] != digest):
            raise ValueError('loss-reduction source manifest omits a referenced original')
    corporate = read_bound_output(corporate_path)[0] if corporate_frame is None else corporate_frame
    episodes = []
    for item in manifest['reviews']:
        path = bundle / item['path']
        payload = json.loads(path.read_text())
        for original in payload['sources']:
            require_covered(path.parent / original['path'], original['sha256'])
        cf_info = payload['corporate_input']
        cf_path = path.parent / cf_info['path']
        require_covered(cf_path, cf_info['sha256'])
        require_covered(cf_path.parent / 'manifest.json')
        commencement = payload['commencement_review']
        commencement_path = path.parent / commencement['path']
        require_covered(commencement_path, commencement['sha256'])
        for original in json.loads(commencement_path.read_text())['sources']:
            require_covered(commencement_path.parent / original['path'], original['sha256'])
        episode = load_loss_reduction_halt_review(path)
        retained, _ = read_bound_output(path.parent / payload['corporate_input']['path'])
        own = corporate.filter((pl.col('from_product') == episode['product'])
            & (pl.col('effective_date') == episode['resumption_date'])
            & (pl.col('source_content_sha256') == episode['source_content_sha256']))
        if (own.height != 1 or not own.select(retained.columns).equals(retained)
                or own['has_equity_credit_fields'].item()
                or any(own[c].item() not in (None, 0) for c in
                       ('equity_credit_long_per_contract', 'equity_debit_short_per_contract'))
                or own['deliverable_cash_twd'].item() != 0
                or own['requires_rights_valuation'].item()
                or not own['deliverable_components_resolved'].item()):
            raise ValueError('loss-reduction review changes its own event or has cash/rights')
        paths.append(path)
        episodes.append(episode)
    if not episodes or len({(e['product'], e['halt_start_date']) for e in episodes}) != len(episodes):
        raise ValueError('loss-reduction bundle must have unique finite episodes')
    for i, episode in enumerate(episodes):
        for earlier in episodes[:i]:
            if (episode['product'] == earlier['product']
                    and episode['halt_start_date'] <= earlier['halt_end_date']
                    and earlier['halt_start_date'] <= episode['halt_end_date']):
                raise ValueError('loss-reduction bundle has overlapping episodes')
    baseline = manifest['accounting_library_baseline']
    identity = dict(contract=CONTRACT, manifest_sha256=sha256_file(manifest_path),
        implementation_sha256=manifest['implementation_sha256'],
        corporate_input_sha256=sha256_file(corporate_path))
    return paths, episodes, identity, (bundle / baseline['path'], baseline['sha256'])


class LossReductionInputSource:
    """Compose review paths without replacing any original source directory."""
    def __init__(self, parent, reviews):
        self.parent, self.reviews = parent, tuple(reviews)

    def __truediv__(self, relative):
        path = Path(relative)
        if path.is_absolute() or '..' in path.parts:
            raise ValueError('halt source composition requires a relative canonical path')
        if str(path) == 'halt':
            return _HaltReviewDirectory(self.parent / path, self.reviews)
        return self.parent / path


class _HaltReviewDirectory:
    def __init__(self, parent, reviews):
        self.parent, self.reviews = parent, reviews

    def glob(self, pattern):
        if pattern != '*/review.json':
            raise ValueError('halt source composition only admits canonical reviews')
        return [*self.parent.glob(pattern), *self.reviews]
