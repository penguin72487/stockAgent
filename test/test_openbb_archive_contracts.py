from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Literal

import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field

import downloader.download_openbb_archive as archive
from downloader.openbb_archive_contracts import PlanContractAuditor


class _PagedQuery(BaseModel):
    start_date: date | None = Field(
        default=None, description="Start date of the data."
    )
    end_date: date | None = Field(default=None, description="End date of the data.")
    limit: int | None = Field(default=100, description="Number of rows.")
    page: int = Field(default=0, description="Page number.")


class _PagedFetcher:
    @staticmethod
    def fetch_data(params):
        return params


class _EtfSearchQuery(BaseModel):
    exchange: Literal["amex", "nyse", "nasdaq", "tsx", "euronext"] | None = Field(
        default=None, description="Filter by exchange."
    )


class _EtfSearchFetcher:
    @staticmethod
    def fetch_data(params):
        return params


def _fake_openbb() -> SimpleNamespace:
    registry = SimpleNamespace(
        providers={
            "demo": SimpleNamespace(fetcher_dict={"DemoModel": _PagedFetcher})
        }
    )
    registry_map = SimpleNamespace(
        original_models={
            "DemoModel": {
                "demo": {
                    "query": _PagedQuery,
                }
            }
        },
        registry=registry,
    )
    provider_interface = SimpleNamespace(
        _registry_map=registry_map,
        map={"DemoModel": {"demo": {}}},
    )
    command_map = SimpleNamespace(commands_model={".demo.history": "DemoModel"})
    return SimpleNamespace(
        coverage=SimpleNamespace(
            _command_map=command_map,
            _provider_interface=provider_interface,
        )
    )


def test_contract_audit_fails_closed_for_unknown_page_axis(tmp_path: Path) -> None:
    auditor = PlanContractAuditor(
        _fake_openbb(), start_date="2000-01-01", end_date="2026-07-18"
    )
    auditor.register_endpoint("demo.history", ("demo",))
    auditor.observe_task(
        SimpleNamespace(
            endpoint="demo.history",
            kwargs={
                "start_date": "2000-01-01",
                "end_date": "2026-07-18",
                "limit": 100,
                "page": 0,
            },
        )
    )
    rows, summary = auditor.finalize(
        [
            SimpleNamespace(
                endpoint="demo.history",
                decision="included",
                selected_providers="demo",
            )
        ]
    )

    assert summary["passed"] is False
    assert any(
        row.axis == "pagination" and row.status == "unresolved" for row in rows
    )


@pytest.mark.parametrize(
    ("provider", "expected_status"),
    [("fmp", "pass"), ("yfinance", "unresolved")],
)
def test_contract_audit_fails_closed_for_unreviewed_time_shard_provider(
    provider: str,
    expected_status: str,
) -> None:
    obb = _fake_openbb()
    obb.coverage._command_map.commands_model = {".news.company": "DemoModel"}
    obb.coverage._provider_interface._registry_map.original_models = {
        "DemoModel": {provider: {"query": _PagedQuery}}
    }
    obb.coverage._provider_interface._registry_map.registry.providers = {
        provider: SimpleNamespace(fetcher_dict={"DemoModel": _PagedFetcher})
    }
    auditor = PlanContractAuditor(
        obb, start_date="2000-01-01", end_date="2026-07-18"
    )
    auditor.register_endpoint("news.company", (provider,))
    auditor.observe_task(
        SimpleNamespace(
            endpoint="news.company",
            kwargs={
                "start_date": "2000-01-01",
                "end_date": "2026-07-18",
                "limit": 100,
                "page": 0,
            },
        )
    )
    rows, _ = auditor.finalize(
        [
            SimpleNamespace(
                endpoint="news.company",
                decision="included",
                selected_providers=provider,
            )
        ]
    )

    row = next(item for item in rows if item.axis == "source_time_pushdown")
    assert row.status == expected_status


def test_native_inception_does_not_expand_a_later_requested_start() -> None:
    auditor = PlanContractAuditor(
        _fake_openbb(), start_date="2024-01-01", end_date="2024-01-31"
    )
    auditor._values["economy.shipping.port_volume"] = {
        "start_date": {json.dumps("2024-01-01"): "2024-01-01"}
    }
    row = auditor._temporal_row(
        "economy.shipping.port_volume",
        "imf",
        "primary",
        "start_date",
        "Start date of the data.",
    )

    assert row.status == "pass"
    assert row.expected == "2024-01-01..2024-01-31"


def test_temporal_contract_reads_comma_date_grid_boundaries() -> None:
    auditor = PlanContractAuditor(
        _fake_openbb(), start_date="2000-01-01", end_date="2026-07-18"
    )
    endpoint = "fixedincome.government.yield_curve"
    auditor._risk_fields[endpoint] = {"date"}
    auditor.observe_task(
        SimpleNamespace(
            endpoint=endpoint,
            kwargs={"date": "2000-01-03,2000-01-04,2026-07-17"},
        )
    )

    row = auditor._temporal_row(
        endpoint,
        "fred",
        "primary",
        "date",
        "Dates to query.",
    )

    assert row.status == "pass"
    assert "grid=2000-01-03..2026-07-17" in row.evidence
    assert row.observed == '["2000-01-03", "2026-07-17"]'


def test_contract_rejects_unpartitioned_capped_etf_catalog() -> None:
    registry = SimpleNamespace(
        providers={
            "fmp": SimpleNamespace(fetcher_dict={"EtfSearch": _EtfSearchFetcher})
        }
    )
    registry_map = SimpleNamespace(
        original_models={"EtfSearch": {"fmp": {"query": _EtfSearchQuery}}},
        registry=registry,
    )
    provider_interface = SimpleNamespace(
        _registry_map=registry_map,
        map={"EtfSearch": {"fmp": {}}},
    )
    obb = SimpleNamespace(
        coverage=SimpleNamespace(
            _command_map=SimpleNamespace(
                commands_model={".etf.search": "EtfSearch"}
            ),
            _provider_interface=provider_interface,
        )
    )
    auditor = PlanContractAuditor(
        obb, start_date="2000-01-01", end_date="2026-07-18"
    )
    auditor.register_endpoint("etf.search", ("fmp",))
    auditor.observe_task(SimpleNamespace(endpoint="etf.search", kwargs={"query": ""}))

    rows, summary = auditor.finalize(
        [
            SimpleNamespace(
                endpoint="etf.search",
                decision="included",
                selected_providers="fmp",
            )
        ]
    )

    exchange = next(
        row
        for row in rows
        if row.endpoint == "etf.search"
        and row.axis == "dimension"
        and row.field == "exchange"
    )
    assert exchange.status == "unresolved"
    assert "amex" in exchange.evidence
    assert summary["passed"] is False


def test_fmp_page_iterator_has_no_fixed_cap_and_rejects_cycles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[int] = []

    def fake_page(endpoint, params, credential):
        calls.append(int(params["page"]))
        return [{"id": 1}]

    monkeypatch.setattr(archive, "_fmp_page_json", fake_page)
    iterator = archive._iter_fmp_pages(
        "example",
        {},
        "secret",
        page_size=1,
        page_limiter=None,
    )
    assert next(iterator) == (0, [{"id": 1}])
    with pytest.raises(RuntimeError, match="pagination cycle"):
        next(iterator)
    assert calls == [0, 1]


def test_petroleum_status_planner_covers_every_weekly_table(tmp_path: Path) -> None:
    fields = {
        "start_date": SimpleNamespace(annotation=str),
        "end_date": SimpleNamespace(annotation=str),
        "category": SimpleNamespace(annotation=str),
        "table": SimpleNamespace(annotation=str),
    }
    context = archive.PlannerContext(
        schemas={
            ".commodity.petroleum_status_report": {
                "input": SimpleNamespace(model_fields=fields),
                "callable": lambda **kwargs: None,
            }
        },
        commands={".commodity.petroleum_status_report": ["eia"]},
        output_dir=tmp_path,
        start_date="2000-01-01",
        end_date="2026-07-18",
        assets=[],
        etfs=[],
        currencies=[],
        indices=[],
        countries=[],
        allowed_providers=None,
        disabled_providers=set(),
        endpoint_filters=(),
        categories=None,
    )
    tasks, coverage = archive.build_initial_plan(context)
    expected = set(archive._petroleum_status_dimensions())
    observed = {(task.kwargs["category"], task.kwargs["table"]) for task in tasks}

    assert coverage[0].decision == "included"
    assert observed == expected
    assert any(category == "weekly_estimates" for category, _ in observed)


def test_manifest_repairs_missing_fmp_continuation_without_page_ceiling(
    tmp_path: Path,
) -> None:
    fields = {
        "start_date": SimpleNamespace(annotation=str),
        "end_date": SimpleNamespace(annotation=str),
        "limit": SimpleNamespace(annotation=int),
        "page": SimpleNamespace(annotation=int),
    }
    context = archive.PlannerContext(
        schemas={
            ".news.world": {
                "input": SimpleNamespace(model_fields=fields),
                "callable": lambda **kwargs: None,
            }
        },
        commands={".news.world": ["fmp"]},
        output_dir=tmp_path,
        start_date="2000-01-01",
        end_date="2026-07-18",
        assets=[],
        etfs=[],
        currencies=[],
        indices=[],
        countries=[],
        allowed_providers=None,
        disabled_providers=set(),
        endpoint_filters=(),
        categories=None,
    )
    parent = archive.make_task(
        context,
        "news.world",
        "year=2000/topic=general/page=0",
        {
            "start_date": "2000-01-01",
            "end_date": "2000-12-31",
            "limit": 2,
            "page": 0,
        },
        ("fmp",),
    )
    output = Path(parent.output_path)
    output.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([{"id": 1}, {"id": 2}]), output)
    manifest = archive.Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([parent], plan_token="archive")
        manifest.claim([parent])
        manifest.complete(
            archive.TaskResult(parent, "success", "fmp", 2, str(output), 1)
        )
        added = manifest.ensure_fmp_page_continuations(
            context, "archive", show_progress=False
        )
        row = manifest.connection.execute(
            "SELECT scope_key,kwargs_json FROM tasks WHERE scope_key LIKE '%page=1'"
        ).fetchone()
    finally:
        manifest.close()

    assert added == 1
    assert row is not None
    assert row["scope_key"].endswith("page=1")
    kwargs = json.loads(row["kwargs_json"])
    assert kwargs["page"] == 1
    assert len(kwargs["_previous_page_signature"]) == 64
