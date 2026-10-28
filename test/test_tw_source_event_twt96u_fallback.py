from datetime import datetime
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from downloader.download_tw_public_data import DEFAULT_DATASETS
from scripts import watch_tw_public_source_events as events


CSV = ('集中市場證券代號,集中市場可借券賣出股數,店頭市場證券代號,店頭市場可借券賣出股數\n'
       '"2330","1,000","6488","500"\n').encode()


def test_official_csv_and_json_share_semantic_hash():
    rows = [{"TWSECode": "2330", "TWSEAvailableVolume": "1,000",
             "GRETAICode": "6488", "GRETAIAvailableVolume": "500"}]
    assert events._twt96u_csv_json_sha256(CSV) == events._body_sha256(
        json.dumps(rows).encode(), expected_json=True)


@pytest.mark.parametrize("payload", [b"<html>blocked</html>", b"a,b,c,d\n1,2,3,4\n",
                                      CSV.splitlines()[0] + b"\n", CSV + b"wrong\n"])
def test_official_csv_fails_closed_on_unusable_input(payload):
    with pytest.raises(ValueError):
        events._twt96u_csv_json_sha256(payload)


def test_primary_failure_falls_back_without_hiding_failure(monkeypatch):
    calls = []

    def request(_session, *, url, previous, **_kwargs):
        calls.append((url, previous))
        if len(calls) == 1:
            raise ValueError("primary returned HTML")
        return {"content": CSV, "version": "csv-byte-version", "body_sha256": "old",
                "etag": "different-host-validator", "not_modified": False}

    monkeypatch.setattr(events, "_request", request)
    spec = DEFAULT_DATASETS["twse_api_sbl_twt96u"]
    observed = datetime(2026, 10, 28, 8, 45, tzinfo=ZoneInfo("Asia/Taipei"))
    result = events._probe_one(spec, observed=observed, previous={"etag": "primary"},
                               limiter=object(), args=SimpleNamespace())
    assert result.status == "ok"
    assert result.fallback_used and result.primary_error == "primary returned HTML"
    assert result.version == events._twt96u_csv_json_sha256(CSV)
    assert result.etag is None and calls[1] == (events.TWT96U_CSV_URL, {})


def test_failed_fallback_remains_failed(monkeypatch):
    monkeypatch.setattr(events, "_request", lambda *_a, **_k: (_ for _ in ()).throw(ValueError("unavailable")))
    result = events._probe_one(DEFAULT_DATASETS["twse_api_sbl_twt96u"],
                               observed=datetime(2026, 10, 28, tzinfo=ZoneInfo("Asia/Taipei")),
                               previous={}, limiter=object(), args=SimpleNamespace())
    assert result.status == "failed"
    assert "official CSV fallback" in result.error


def test_changed_fallback_version_is_pending_until_canonical_download():
    spec = DEFAULT_DATASETS["twse_api_sbl_twt96u"]
    state = {"datasets": {spec.name: {"observed_version": "old", "applied_version": "old"}}}
    result = events.ProbeResult(
        dataset=spec.name, status="ok", url=events.TWT96U_CSV_URL,
        checked_at_taipei="2026-10-28T08:45:00+08:00", version="new",
        body_sha256="new", fallback_used=True, primary_error="primary returned HTML",
    )
    args = SimpleNamespace(fast_interval_seconds=5, medium_interval_seconds=30,
                           slow_interval_seconds=60)
    changed = events._apply_probe_results(state, [result], specs_by_name={spec.name: spec}, args=args)
    row = state["datasets"][spec.name]
    assert changed == [spec.name]
    assert row["observed_version"] == "new" and row["applied_version"] == "old"
    assert row["primary_probe_status"] == "failed" and row["official_fallback_used"] is True
    events._summarize_state(state, specs=[spec], changed=changed)
    assert state["blocking_unapplied_event_count"] == 1
