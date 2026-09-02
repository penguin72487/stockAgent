from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import polars as pl
import pytest

from downloader import download_crypto_etf_history as etf
from downloader import download_dune_crypto_history as dune
from downloader import download_free_public_context as public
from downloader.common import provider_rate_limit


ROOT = Path(__file__).resolve().parents[1]


def test_dune_contracts_are_versioned_and_bound_to_three_active_cex_labels() -> None:
    contracts = dune._load_contracts(
        ROOT / "configs/dune_crypto_queries.json",
        selected=None,
    )

    assert {item.fact_family for item in contracts} == {
        "dex_asset_activity",
        "cex_labeled_token_flows",
        "stablecoin_mint_burn",
    }
    assert len({item.query_id for item in contracts}) == len(contracts)
    for contract in contracts:
        assert "{{start_date}}" in contract.sql
        assert "{{end_date}}" in contract.sql
        assert len(contract.sql_sha256) == 64
        assert set(contract.primary_key) <= set(contract.expected_columns)
        assert contract.performance == "small"
    cex = next(item for item in contracts if item.fact_family == "cex_labeled_token_flows")
    lowered = cex.sql.lower()
    assert all(name in lowered for name in ("binance", "okx", "bybit"))
    assert all(name not in lowered for name in ("coinbase", "kraken", "bitfinex", "hyperliquid"))


def test_dune_partition_validation_rejects_duplicate_and_out_of_window_rows() -> None:
    contract = dune.QueryContract(
        query_id="fixture_v1",
        dune_query_id=0,
        sql_path=Path("fixture.sql"),
        sql="SELECT '{{start_date}}', '{{end_date}}'",
        sql_sha256="a" * 64,
        fact_family="fixture",
        primary_key=("event_date", "asset"),
        event_time_column="event_date",
        expected_columns=("event_date", "asset", "value"),
        history_start=date(2026, 1, 1),
        chunk_months=1,
        cadence_seconds=86400,
        performance="medium",
    )
    partition = dune.Partition(contract, date(2026, 1, 1), date(2026, 2, 1))
    assert partition.partition_id == "2026-01-01"
    good = pl.DataFrame(
        {"event_date": ["2026-01-02"], "asset": ["BTC"], "value": [1.0]}
    )
    assert dune._validate_frame(good, partition).height == 1

    duplicate = pl.concat([good, good])
    with pytest.raises(RuntimeError, match="duplicate"):
        dune._validate_frame(duplicate, partition)
    outside = good.with_columns(pl.lit("2026-02-01").alias("event_date"))
    with pytest.raises(RuntimeError, match="escape partition"):
        dune._validate_frame(outside, partition)


def test_dune_lineage_upgrade_is_local_and_credit_free(tmp_path: Path) -> None:
    contract = dune.QueryContract(
        query_id="fixture_v1",
        dune_query_id=0,
        sql_path=Path("fixture.sql"),
        sql="SELECT '{{start_date}}', '{{end_date}}'",
        sql_sha256="a" * 64,
        fact_family="fixture",
        primary_key=("event_date",),
        event_time_column="event_date",
        expected_columns=("event_date",),
        history_start=date(2026, 1, 1),
        chunk_months=1,
        cadence_seconds=86400,
        performance="medium",
    )
    partition = dune.Partition(contract, date(2026, 1, 1), date(2026, 2, 1))
    parquet_path = dune._parquet_path(tmp_path, partition)
    receipt_path = dune._receipt_path(tmp_path, partition)
    dune._atomic_parquet(
        parquet_path,
        pl.DataFrame(
            {
                "event_date": ["2026-01-01"],
                "_dune_query_id": ["fixture_v1"],
            }
        ),
    )
    dune._atomic_json(
        receipt_path,
        {
            "status": "complete",
            "sql_sha256": contract.sql_sha256,
            "completed_at_utc": "2026-02-01T00:00:00+00:00",
        },
    )

    dune._upgrade_partition_lineage(tmp_path, partition)

    upgraded = pl.read_parquet(parquet_path)
    assert upgraded["_dune_contract_id"].item() == "fixture_v1"
    assert upgraded["_dune_query_id"].item() == 0
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["contract_id"] == "fixture_v1"
    assert receipt["dune_query_id"] == 0


def test_progress_can_publish_external_block_without_fabricating_completion(
    tmp_path: Path,
) -> None:
    progress = dune.PersistentProgress(
        tmp_path / "progress.json",
        label="Dune fixture",
        total=10,
        unit="partitions",
        basis="fixture",
    )
    progress.update("fixture", "blocked_credits")
    progress.finish(state="blocked")

    payload = json.loads((tmp_path / "progress.json").read_text(encoding="utf-8"))
    assert payload["state"] == "blocked"
    assert payload["current"] == 1
    assert payload["ratio"] == 0.1


def test_generic_public_context_cannot_reenable_non_selected_exchanges() -> None:
    deferred = public.DEFERRED_EXCHANGE_DATASETS

    assert {
        "hyperliquid_perp_context",
        "deribit_btc_options",
        "coinbase_exchange_products",
        "kraken_asset_pairs",
        "bitfinex_all_tickers",
    } <= deferred
    active = {spec.dataset for spec in public.DATASETS if spec.dataset not in deferred}
    assert not active & deferred


def test_crypto_etf_registry_has_unique_sec_and_issuer_identity() -> None:
    registry = json.loads(
        (ROOT / "configs/crypto_etf_sources.json").read_text(encoding="utf-8")
    )
    funds = registry["sec"]["funds"]
    issuer_sources = registry["issuer_sources"]

    assert len(funds) >= 30
    assert len({item["ticker"] for item in funds}) == len(funds)
    assert len({item["id"] for item in issuer_sources}) == len(issuer_sources)
    assert {item["ticker"] for item in issuer_sources} == {"IBIT", "ETHA", "BITB"}
    assert provider_rate_limit("sec_edgar").requests_per_second == 10.0


def test_sec_user_agent_preserves_contact_and_normalizes_non_ascii_identity() -> None:
    assert etf._sec_user_agent(
        "研究者 中文組織 researcher@example.org"
    ) == "stockAgent research researcher@example.org"
    assert etf._sec_user_agent(
        "Alice Example Fund alice@example.org"
    ) == "Alice Example Fund alice@example.org"
    assert etf._sec_user_agent("研究者 中文組織", "fallback@example.org") == (
        "stockAgent research fallback@example.org"
    )
    assert etf._sec_user_agent("missing contact") == ""


def test_sec_primary_document_uses_official_complete_submission_fallback(
    tmp_path: Path,
) -> None:
    class FakeClient:
        def get(
            self,
            url: str,
            *,
            profile_name: str,
            user_agent: str,
            accept: str = "",
        ) -> tuple[bytes, dict[str, str]]:
            del profile_name, user_agent, accept
            if url.endswith("submissions.json"):
                return (
                    json.dumps(
                        {
                            "name": "Fixture Trust",
                            "filings": {
                                "recent": {
                                    "accessionNumber": ["0000000001-26-000001"],
                                    "form": ["10-K"],
                                    "primaryDocument": ["broken.htm"],
                                },
                                "files": [],
                            },
                        }
                    ).encode(),
                    {},
                )
            if url.endswith("companyfacts.json"):
                return b'{"facts": {}}', {}
            if url.endswith("broken.htm"):
                raise etf.HttpStatusError(503, url, "fixture unavailable")
            if url.endswith("0000000001-26-000001.txt"):
                return b"<SEC-DOCUMENT>complete fixture</SEC-DOCUMENT>", {}
            raise AssertionError(url)

    result = etf._sec_entity(
        FakeClient(),  # type: ignore[arg-type]
        tmp_path,
        {
            "submissions_url_template": "https://example.test/{cik10}/submissions.json",
            "submission_shard_url_template": "https://example.test/{name}",
            "companyfacts_url_template": "https://example.test/{cik10}/companyfacts.json",
            "primary_document_url_template": (
                "https://example.test/{cik}/{accession_compact}/{primary_document}"
            ),
            "complete_submission_url_template": (
                "https://example.test/{cik}/{accession_compact}/"
                "{accession}.txt"
            ),
            "forms": ["10-K"],
        },
        cik=1,
        tickers=["TEST"],
        assets=["BTC"],
        issuers=["Fixture"],
        title="Fixture Trust",
        user_agent="stockAgent test test@example.org",
        download_documents=True,
        max_documents=0,
    )

    assert result.status == "complete"
    manifest = json.loads(
        (tmp_path / "normalized/sec/0000000001/manifest.json").read_text()
    )
    receipt = manifest["primary_documents"][0]
    assert receipt["status"] == "fallback_complete_submission"
    assert Path(receipt["raw_path"]).is_file()
    assert manifest["document_failures"] == []

    cached_result = etf._sec_entity(
        FakeClient(),  # type: ignore[arg-type]
        tmp_path,
        {
            "submissions_url_template": "https://example.test/{cik10}/submissions.json",
            "submission_shard_url_template": "https://example.test/{name}",
            "companyfacts_url_template": "https://example.test/{cik10}/companyfacts.json",
            "primary_document_url_template": (
                "https://example.test/{cik}/{accession_compact}/{primary_document}"
            ),
            "complete_submission_url_template": (
                "https://example.test/{cik}/{accession_compact}/{accession}.txt"
            ),
            "forms": ["10-K"],
        },
        cik=1,
        tickers=["TEST"],
        assets=["BTC"],
        issuers=["Fixture"],
        title="Fixture Trust",
        user_agent="stockAgent test test@example.org",
        download_documents=True,
        max_documents=0,
    )
    assert cached_result.status == "complete"
    cached_manifest = json.loads(
        (tmp_path / "normalized/sec/0000000001/manifest.json").read_text()
    )
    assert cached_manifest["primary_documents"][0]["status"] == (
        "cached_complete_submission"
    )


def test_sec_submission_and_companyfacts_normalization_keeps_availability() -> None:
    filings = etf._filing_rows(
        {
            "filings": {
                "recent": {
                    "accessionNumber": ["0001-26-000001"],
                    "form": ["10-K"],
                    "primaryDocument": ["annual.htm"],
                }
            }
        }
    )
    assert filings == [
        {
            "accessionNumber": "0001-26-000001",
            "form": "10-K",
            "primaryDocument": "annual.htm",
        }
    ]

    rows = etf._companyfact_rows(
        {
            "facts": {
                "us-gaap": {
                    "Assets": {
                        "label": "Assets",
                        "description": "Total assets",
                        "units": {
                            "USD": [
                                {
                                    "val": 100,
                                    "end": "2025-12-31",
                                    "filed": "2026-02-01",
                                    "form": "10-K",
                                    "accn": "0001-26-000001",
                                }
                            ]
                        },
                    }
                }
            }
        },
        cik="0000000001",
        tickers=["TEST"],
        title="Test Trust",
        retrieved_at="2026-08-17T00:00:00+00:00",
    )
    assert rows[0]["value_float"] == 100.0
    assert rows[0]["available_at_utc"] == "2026-02-01T23:59:59+00:00"


def test_ishares_holdings_and_bitwise_page_adapters_are_source_timestamped() -> None:
    ishares_raw = (
        "iShares Bitcoin Trust ETF\n"
        "Fund Holdings as of,\"Aug 13, 2026\"\n"
        "Shares Outstanding,\"1,000.00\"\n\n"
        "Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Quantity,Market Currency,Accrual Date\n"
        '"BTC","BITCOIN","-","Alternative","100.00","100.00","100.00","1.00000","BTC","-"\n'
    ).encode()
    spec = {
        "id": "ishares_fixture",
        "provider": "iShares",
        "ticker": "IBIT",
        "asset": "BTC",
    }
    parsed = etf._parse_ishares_holdings(
        spec,
        ishares_raw,
        "2026-08-17T00:00:00+00:00",
        "a" * 64,
    )
    assert parsed.holdings[0]["as_of_date"] == "2026-08-13"
    assert parsed.daily_metrics[0]["value"] == 1000.0
    assert parsed.holdings[0]["available_at_utc"] == "2026-08-17T00:00:00+00:00"

    next_data = {
        "props": {
            "pageProps": {
                "fundData": {
                    "data": {
                        "navAndMarketPrice": {
                            "chart": {
                                "nav": [[1704920400000, 25.0]],
                                "marketPrice": [[1704920400000, 25.1]],
                            }
                        },
                        "fundDetails": {"asOfDate": "2026-08-14", "netAssets": 10},
                        "holdings": {"asOfDate": "2026-08-14", "basket": []},
                    }
                },
                "proofOfReservesSnapshotData": {
                    "timestamp": "2026-08-15T01:00:00Z",
                    "totalReserve": 2,
                    "totalNAV": 1.9,
                    "ripcord": False,
                },
            }
        }
    }
    html = (
        '<html><script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(next_data)
        + "</script></html>"
    ).encode()
    bitwise = etf._parse_bitwise(
        {"id": "bitwise_fixture", "provider": "Bitwise", "ticker": "BITB", "asset": "BTC"},
        html,
        "2026-08-17T00:00:00+00:00",
        "b" * 64,
    )
    assert {row["metric"] for row in bitwise.daily_metrics} >= {
        "nav_per_share",
        "market_price_per_share",
        "net_assets",
    }
    assert bitwise.reserves[0]["ripcord"] is False
    assert "not independently" in bitwise.reserves[0]["definition"]
