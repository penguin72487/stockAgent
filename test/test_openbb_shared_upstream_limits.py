"""OpenBB adapter aliases must share upstream slots with direct collectors."""
import pytest

from downloader.common import SharedRateLimiter, provider_rate_limit
from downloader.download_openbb_archive import HttpBoundaryRateLimiter, ProviderRuntime


@pytest.mark.parametrize("provider,bucket,rate", [
    ("fred", "fred_api", 0.8),
    ("sec", "sec_edgar", 3.0),
    ("yfinance", "yahoo_finance", 2.0),
])
def test_direct_and_openbb_share_slots_but_keep_slower_provider_rate(
    tmp_path, monkeypatch, provider, bucket, rate,
):
    monkeypatch.setenv("STOCKAGENT_RATE_LIMIT_DIR", str(tmp_path))
    runtime = ProviderRuntime({provider: rate}, {provider: 1}, 20)
    archive = runtime.limiter(provider)
    direct = SharedRateLimiter(provider_rate_limit(bucket).interval_seconds, name=bucket)
    assert archive.name == bucket
    assert archive._state_path == direct._state_path
    assert archive.interval_seconds == pytest.approx(1 / rate)
    assert direct._claim_process_shared()[0]
    claimed, delay = archive._claim_process_shared()
    assert not claimed and delay > 0
    runtime.block_quota(provider, "429 too many requests")
    claimed, delay = direct._claim_process_shared()
    assert not claimed and delay > 18  # the same shared cooldown, without sleeping
    assert runtime.availability(provider)[0] is False


def test_fred_alias_keeps_http_preclaim_deduplication(tmp_path, monkeypatch):
    monkeypatch.setenv("STOCKAGENT_RATE_LIMIT_DIR", str(tmp_path))
    claims = []
    monkeypatch.setattr(SharedRateLimiter, "wait", lambda self: claims.append(self.name))
    limiter = ProviderRuntime({"fred": 2}, {"fred": 1}, 20).limiter("fred")
    assert isinstance(limiter, HttpBoundaryRateLimiter)
    limiter.wait()
    limiter.wait_at_http_boundary()
    assert claims == ["fred_api"]
    limiter.wait_at_http_boundary()
    assert claims == ["fred_api", "fred_api"]


def test_cftc_socrata_does_not_claim_same_bucket_as_legacy_zip_archive(tmp_path, monkeypatch):
    monkeypatch.setenv("STOCKAGENT_RATE_LIMIT_DIR", str(tmp_path))
    socrata = ProviderRuntime({"cftc": 8}, {"cftc": 1}, 20).limiter("cftc")
    legacy = SharedRateLimiter(0.1, name="cftc_public_archive")
    assert socrata.name == "cftc"
    assert socrata._state_path != legacy._state_path
