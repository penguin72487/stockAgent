"""Bounded, read-only paging over one verified public feature generation.

The producer remains the authority for field values and completeness.  This
index only moves search and paging off the browser; it never drops source rows.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Mapping


_SEARCH_FIELDS = (
    "field", "dataset_id", "source_title", "provider", "market_category_label",
)
FEATURE_PREVIEW_LIMIT = 80
FEATURE_SOURCE_PAGE_MAX_ROWS = 512


def feature_page_revision(source_signature: tuple[int, ...]) -> str:
    return hashlib.sha256(
        json.dumps(source_signature, separators=(",", ":")).encode("ascii")
    ).hexdigest()[:32]


def _validated_rows(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    if (
        payload.get("schema_version") != 1
        or payload.get("read_only") is not True
        or payload.get("production_control_possible") is not False
        or not isinstance(payload.get("rows"), list)
        or not isinstance(payload.get("summary"), Mapping)
    ):
        raise ValueError("invalid public feature snapshot")
    rows = payload["rows"]
    if any(
        not isinstance(row, Mapping)
        or any(not isinstance(row.get(key), str) for key in _SEARCH_FIELDS)
        or not isinstance(row.get("market_category"), str)
        for row in rows
    ):
        raise ValueError("invalid public feature row")
    fields = payload["summary"].get("fields")
    if type(fields) is not int or fields != len(rows):
        raise ValueError("feature count does not match rows")
    return rows


def feature_page_projections(
    payload: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, list[Mapping[str, Any]]]]:
    """Build the first page and bounded source pages in one validated pass."""

    rows = _validated_rows(payload)
    categories: dict[str, str] = {}
    sources: dict[str, str] = {}
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        categories.setdefault(row["market_category"], row["market_category_label"])
        sources.setdefault(row["dataset_id"], f"{row['source_title']} · {row['provider']}")
        grouped.setdefault(row["dataset_id"], []).append(row)
    preview = {
        "schema_version": 1,
        "read_only": True,
        "production_control_possible": False,
        "generated_at_utc": payload.get("generated_at_utc"),
        "summary": payload["summary"],
        "filters": {
            "categories": [{"id": key, "label": value} for key, value in categories.items()],
            "sources": [{"id": key, "label": value} for key, value in sources.items()],
        },
        "rows": rows[:FEATURE_PREVIEW_LIMIT],
    }
    small_sources = {
        source: source_rows for source, source_rows in grouped.items()
        if len(source_rows) <= FEATURE_SOURCE_PAGE_MAX_ROWS
    }
    return preview, small_sources


def feature_page_preview(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Small first-page projection from the same validated producer DTO."""

    return feature_page_projections(payload)[0]


def valid_feature_page_preview(preview: Any, *, fields: int) -> bool:
    if not isinstance(preview, Mapping) or not isinstance(preview.get("summary"), Mapping):
        return False
    rows = preview.get("rows")
    filters = preview.get("filters")
    if (
        preview.get("schema_version") != 1
        or preview.get("read_only") is not True
        or preview.get("production_control_possible") is not False
        or type(preview["summary"].get("fields")) is not int
        or preview["summary"].get("fields") != fields
        or not isinstance(rows, list)
        or len(rows) != min(fields, FEATURE_PREVIEW_LIMIT)
        or not isinstance(filters, Mapping)
    ):
        return False
    if any(
        not isinstance(row, Mapping)
        or any(not isinstance(row.get(key), str) for key in _SEARCH_FIELDS)
        or not isinstance(row.get("market_category"), str)
        for row in rows
    ):
        return False
    return all(
        isinstance(filters.get(name), list)
        and all(
            isinstance(item, Mapping)
            and isinstance(item.get("id"), str)
            and isinstance(item.get("label"), str)
            for item in filters[name]
        )
        for name in ("categories", "sources")
    )


def feature_source_pages(payload: Mapping[str, Any]) -> dict[str, list[Mapping[str, Any]]]:
    """Keep complete small-source rows in original order for bounded paging."""

    return feature_page_projections(payload)[1]


def valid_feature_source_pages(pages: Any, *, fields: int) -> bool:
    if not isinstance(pages, Mapping) or len(pages) > 10_000:
        return False
    total = 0
    for source, rows in pages.items():
        if (
            not isinstance(source, str)
            or not isinstance(rows, list)
            or not 0 < len(rows) <= FEATURE_SOURCE_PAGE_MAX_ROWS
        ):
            return False
        total += len(rows)
        if total > fields or any(
            not isinstance(row, Mapping)
            or row.get("dataset_id") != source
            or any(not isinstance(row.get(key), str) for key in _SEARCH_FIELDS)
            or not isinstance(row.get("market_category"), str)
            for row in rows
        ):
            return False
    return True


def page_from_source_rows(
    *, rows: list[Mapping[str, Any]], preview: Mapping[str, Any],
    revision: str, requested_revision: str | None, offset: int, limit: int,
    search: str, category: str,
) -> dict[str, Any]:
    """Project exactly the normal page contract from one complete source."""

    reset_required = requested_revision is not None and requested_revision != revision
    if reset_required:
        offset = 0
    query = search.strip().lower()
    if not query and category == "all":
        total = len(rows)
        page_rows = rows[offset:offset + limit]
    else:
        total = 0
        page_rows = []
        for row in rows:
            if category != "all" and row["market_category"] != category:
                continue
            if query and query not in "\0".join(row[key].lower() for key in _SEARCH_FIELDS):
                continue
            if offset <= total < offset + limit:
                page_rows.append(row)
            total += 1
    return {
        "schema_version": 1,
        "read_only": True,
        "production_control_possible": False,
        "generated_at_utc": preview.get("generated_at_utc"),
        "revision": revision,
        "reset_required": reset_required,
        "offset": offset,
        "limit": limit,
        "matching_total": total,
        "has_more": offset + len(page_rows) < total,
        "summary": preview["summary"],
        "filters": preview["filters"],
        "rows": page_rows,
    }


@dataclass(frozen=True)
class FeaturePageIndex:
    revision: str
    rows: tuple[Mapping[str, Any], ...]
    search_text: tuple[str, ...]
    source_positions: Mapping[str, tuple[int, ...]]
    category_positions: Mapping[str, tuple[int, ...]]
    categories: tuple[dict[str, str], ...]
    sources: tuple[dict[str, str], ...]
    summary: Mapping[str, Any]
    generated_at_utc: str | None

    @classmethod
    def from_payload(
        cls, payload: Mapping[str, Any], *, source_signature: tuple[int, ...],
    ) -> FeaturePageIndex:
        rows = _validated_rows(payload)
        categories: dict[str, str] = {}
        sources: dict[str, str] = {}
        source_positions: dict[str, list[int]] = {}
        category_positions: dict[str, list[int]] = {}
        search_text: list[str] = []
        for position, row in enumerate(rows):
            categories.setdefault(row["market_category"], row["market_category_label"])
            sources.setdefault(
                row["dataset_id"], f"{row['source_title']} · {row['provider']}",
            )
            source_positions.setdefault(row["dataset_id"], []).append(position)
            category_positions.setdefault(row["market_category"], []).append(position)
            search_text.append("\0".join(row[key].lower() for key in _SEARCH_FIELDS))
        revision = feature_page_revision(source_signature)
        return cls(
            revision=revision,
            rows=tuple(rows),
            search_text=tuple(search_text),
            source_positions={key: tuple(value) for key, value in source_positions.items()},
            category_positions={key: tuple(value) for key, value in category_positions.items()},
            categories=tuple({"id": key, "label": value} for key, value in categories.items()),
            sources=tuple({"id": key, "label": value} for key, value in sources.items()),
            summary=payload["summary"],
            generated_at_utc=payload.get("generated_at_utc"),
        )

    def page(
        self, *, offset: int, limit: int, search: str = "",
        category: str = "all", source: str = "all",
        requested_revision: str | None = None,
    ) -> dict[str, Any]:
        reset_required = requested_revision is not None and requested_revision != self.revision
        if reset_required:
            offset = 0
        query = search.strip().lower()
        if not query and category == "all" and source == "all":
            total = len(self.rows)
            page_rows = list(self.rows[offset:offset + limit])
        elif category == "all" and source == "all":
            # The unfiltered text search is the only path that must inspect
            # every row. Keep its direct zip rather than adding indexed gets.
            page_rows = []
            total = 0
            for row, searchable in zip(self.rows, self.search_text, strict=True):
                if query not in searchable:
                    continue
                if offset <= total < offset + limit:
                    page_rows.append(row)
                total += 1
        else:
            candidates: range | tuple[int, ...] = range(len(self.rows))
            if source != "all":
                candidates = self.source_positions.get(source, ())
            if category != "all":
                category_candidates = self.category_positions.get(category, ())
                if len(category_candidates) < len(candidates):
                    candidates = category_candidates
            page_rows = []
            total = 0
            for position in candidates:
                row = self.rows[position]
                if category != "all" and row["market_category"] != category:
                    continue
                if source != "all" and row["dataset_id"] != source:
                    continue
                if query and query not in self.search_text[position]:
                    continue
                if offset <= total < offset + limit:
                    page_rows.append(row)
                total += 1
        return {
            "schema_version": 1,
            "read_only": True,
            "production_control_possible": False,
            "generated_at_utc": self.generated_at_utc,
            "revision": self.revision,
            "reset_required": reset_required,
            "offset": offset,
            "limit": limit,
            "matching_total": total,
            "has_more": offset + len(page_rows) < total,
            "summary": self.summary,
            "filters": {"categories": self.categories, "sources": self.sources},
            "rows": page_rows,
        }


__all__ = [
    "FEATURE_PREVIEW_LIMIT", "FEATURE_SOURCE_PAGE_MAX_ROWS", "FeaturePageIndex",
    "feature_page_preview", "feature_page_projections", "feature_page_revision",
    "feature_source_pages",
    "page_from_source_rows", "valid_feature_page_preview", "valid_feature_source_pages",
]
