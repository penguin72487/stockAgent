from dataclasses import asdict
from datetime import UTC, datetime, timedelta
import json

import pytest

from stockagent.data.finlab_quota_policy import (
    QuotaPolicy, QuotaSampleWindow, catalog_current, load_policy, quota_admission,
)


NOW = datetime(2026, 10, 9, 13, tzinfo=UTC)
DRAIN = datetime(2026, 10, 9, 23, tzinfo=UTC)  # 07:00 Taipei, before 08:00 reset.


def test_minute_floor_is_independent_of_tick_and_full_history_reserve():
    policy = QuotaPolicy()
    minute = quota_admission(kind="tw_minute", now=NOW, policy=policy, scheduled_reserve_mb=800)
    tick = quota_admission(kind="tw_tick", now=NOW, policy=policy, scheduled_reserve_mb=800)
    assert minute["effective_reserve_mb"] == 50 and tick["effective_reserve_mb"] == 800
    assert minute["normal_reserve_mb"] == 50 and minute["mode"] == "normal"


@pytest.mark.parametrize("kind", ["tw_minute", "tw_tick"])
def test_reset_tail_uses_zero_floor_then_restores_normal_after_reset(kind):
    assert quota_admission(kind=kind, now=DRAIN-timedelta(microseconds=1))["mode"] == "normal"
    result = quota_admission(kind=kind, now=DRAIN, scheduled_reserve_mb=900)
    assert result["effective_reserve_mb"] == 0 and result["mode"] == "pre_reset_drain"
    assert result["reset_at_utc"] == "2026-10-10T00:00:00+00:00"
    assert quota_admission(kind=kind, now=DRAIN+timedelta(hours=1))["mode"] == "normal"
    with pytest.raises(ValueError):
        quota_admission(kind=kind, now=DRAIN.replace(tzinfo=None))


def test_disabled_drain_retains_normal_floor():
    assert quota_admission(kind="tw_minute", now=DRAIN, policy=QuotaPolicy(
        drain_before_reset_minutes=0))["effective_reserve_mb"] == 50


@pytest.mark.parametrize("bad", [True, float("nan"), float("inf"), -1, "50"])
def test_invalid_floor_is_not_a_quota_bypass(bad):
    with pytest.raises(ValueError):
        quota_admission(kind="tw_minute", now=NOW, reserve_mb=bad)


@pytest.mark.parametrize("field,bad", [
    ("drain_before_reset_minutes", 181), ("quota_sample_interval_seconds", 61),
    ("quota_sample_max_requests", 0), ("minute_partitions_per_pass", 0),
    ("tick_partitions_per_pass", 0), ("tick_partitions_per_pass", 2049), ("tick_partitions_per_pass", True),
    ("maximum_intraday_passes", 9), ("catalog_refresh_minutes", True),
    ("minute_reserve_mb", float("nan")), ("schema_version", True),
    ("minute_workers", 33), ("minute_workers", 0), ("minute_workers", True),
    ("minute_range_calendar_days", 32), ("minute_range_calendar_days", 0),
    ("minute_http_keepalive", 1), ("minute_http_keepalive", "true"),
    ("minute_download_threads", 0), ("minute_download_threads", 6), ("minute_download_threads", True),
    ("minute_queue_depth", 0), ("minute_queue_depth", 5), ("minute_queue_depth", True),
    ("minute_authorization_slots", 0), ("minute_authorization_slots", 33), ("minute_authorization_slots", True),
    ("tick_workers", 9), ("tick_workers", 0), ("tick_workers", True),
    ("tick_range_calendar_days", 0), ("tick_range_calendar_days", 32), ("tick_range_calendar_days", True),
    ("tick_queue_depth", 0), ("tick_queue_depth", 5),
    ("tick_authorization_slots", 9), ("tick_http_keepalive", "true"),
    ("unused_option", 1),
])
def test_configuration_rejects_unbounded_or_ignored_options(tmp_path, field, bad):
    path = tmp_path / "policy.json"
    path.write_text(json.dumps({"schema_version": 1, **asdict(QuotaPolicy()), field: bad}))
    with pytest.raises(ValueError):
        load_policy(path)


def test_default_and_explicit_configuration_have_identical_fingerprint(tmp_path):
    policy = load_policy(tmp_path / "missing.json")
    path = tmp_path / "policy.json"
    path.write_text(json.dumps({"schema_version": 1, **asdict(policy)}))
    assert load_policy(path).fingerprint == policy.fingerprint


def test_owner_refresh_projection_includes_bounded_tick_quantum(tmp_path, monkeypatch, capsys):
    from scripts import finlab_refresh_policy
    configs = tmp_path / 'configs'
    configs.mkdir()
    (configs / 'finlab_quota_policy.json').write_text(json.dumps({
        'schema_version': 1, 'tick_partitions_per_pass': 1024, 'tick_workers': 8}))
    monkeypatch.setattr(finlab_refresh_policy, 'ROOT', tmp_path)
    assert finlab_refresh_policy.main() == 0
    value = json.loads(capsys.readouterr().out)
    assert value['tick_partitions_per_pass'] == 1024 and value['tick_workers'] == 8


def test_short_quota_window_is_bounded_by_time_and_request_count():
    window = QuotaSampleWindow(QuotaPolicy())
    assert window.needs_refresh(now=NOW, clock=1, reserve_mb=50)
    window.observe((700, 5000), now=NOW, clock=1, reserve_mb=50)
    for _ in range(4):
        window.requested()
    assert not window.needs_refresh(now=NOW, clock=15.9, reserve_mb=50)
    assert window.needs_refresh(now=NOW, clock=16, reserve_mb=50)
    window.requested()
    assert window.needs_refresh(now=NOW, clock=2, reserve_mb=50)


def test_near_floor_reset_changed_policy_and_failed_refresh_never_use_stale_quota():
    window = QuotaSampleWindow(QuotaPolicy())
    window.observe((60, 5000), now=NOW, clock=1, reserve_mb=50)
    assert window.needs_refresh(now=NOW, clock=1, reserve_mb=50)
    window.observe((700, 5000), now=NOW, clock=1, reserve_mb=50)
    assert window.needs_refresh(now=NOW, clock=2, reserve_mb=0)
    assert window.needs_refresh(now=DRAIN+timedelta(hours=1), clock=2, reserve_mb=50)
    assert window.needs_refresh(now=NOW, clock=0.5, reserve_mb=50)
    assert window.observe(None, now=NOW, clock=2, reserve_mb=50) is None
    assert window.needs_refresh(now=NOW, clock=2, reserve_mb=50)
    for invalid in [(float("nan"), 5000), (True, 5000), (-1, 5000), (5001, 5000)]:
        assert window.observe(invalid, now=NOW, clock=2, reserve_mb=50) is None


def test_tick_sampling_reuses_only_four_requests_five_seconds_far_from_floor():
    window = QuotaSampleWindow(QuotaPolicy(quota_sample_max_requests=32), kind="tw_tick")
    window.observe((700, 5000), now=NOW, clock=1, reserve_mb=500)
    for _ in range(3):
        window.requested()
    assert not window.needs_refresh(now=NOW, clock=5.9, reserve_mb=500)
    assert window.needs_refresh(now=NOW, clock=6, reserve_mb=500)
    window.requested()
    assert window.needs_refresh(now=NOW, clock=2, reserve_mb=500)
    window.observe((600, 5000), now=NOW, clock=2, reserve_mb=500)
    assert window.needs_refresh(now=NOW, clock=2, reserve_mb=500)
    window.observe((700, 5000), now=NOW, clock=2, reserve_mb=500)
    assert window.needs_refresh(now=DRAIN + timedelta(hours=1), clock=3, reserve_mb=500)
    window.observe(None, now=NOW, clock=3, reserve_mb=500)
    assert window.needs_refresh(now=NOW, clock=3, reserve_mb=500)
    stricter = QuotaSampleWindow(QuotaPolicy(quota_sample_interval_seconds=0,
                                           quota_sample_max_requests=1), kind="tw_tick")
    stricter.observe((700, 5000), now=NOW, clock=1, reserve_mb=500)
    assert stricter.needs_refresh(now=NOW, clock=1, reserve_mb=500)


def test_catalog_reuse_requires_validated_bounded_freshness():
    policy = QuotaPolicy()
    catalog = {"observed_at_utc": NOW.isoformat(), "keys": ["price:收盤價"]}
    assert catalog_current(catalog, now=NOW+timedelta(minutes=59), policy=policy)
    assert not catalog_current(catalog, now=NOW+timedelta(hours=1), policy=policy)
    assert not catalog_current(catalog, now=NOW-timedelta(seconds=1), policy=policy)
    assert not catalog_current({**catalog, "keys": []}, now=NOW, policy=policy)
    assert not catalog_current({**catalog, "keys": [None]}, now=NOW, policy=policy)
