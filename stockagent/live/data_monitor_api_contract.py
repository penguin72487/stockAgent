"""Typed contracts for the existing read-only feature paging API.

This module is importable without analytics extras. Schema generation and the
bounded response verifier use msgspec only when explicitly invoked.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict


class FeaturePageQuery(TypedDict):
    offset: int
    limit: int
    search: str
    category: str
    source: str
    revision: str | None


class FeaturePagePayload(TypedDict):
    schema_version: Literal[1]
    read_only: Literal[True]
    production_control_possible: Literal[False]
    generated_at_utc: str | None
    revision: str
    reset_required: bool
    offset: int
    limit: int
    matching_total: int
    has_more: bool
    summary: dict[str, Any]
    filters: dict[str, Any]
    rows: list[dict[str, Any]]


def feature_page_schema() -> dict[str, Any]:
    import msgspec
    schema = msgspec.json.schema(FeaturePagePayload)
    properties = schema["$defs"]["FeaturePagePayload"]["properties"]
    properties["read_only"] = {"const": True}
    properties["production_control_possible"] = {"const": False}
    properties["offset"].update(minimum=0, maximum=1_000_000)
    properties["limit"].update(minimum=1, maximum=5_000)
    properties["matching_total"].update(minimum=0, maximum=2**53-1)
    properties["revision"].update(pattern="^[0-9a-f]{32}$")
    return schema


def verify_feature_page(payload: Any) -> FeaturePagePayload:
    import msgspec
    verified = msgspec.convert(payload, type=FeaturePagePayload, strict=True)
    if verified["read_only"] is not True or verified["production_control_possible"] is not False:
        raise ValueError("feature page must remain read-only")
    if not 0 <= verified["offset"] <= 1_000_000 or not 1 <= verified["limit"] <= 5_000:
        raise ValueError("feature page bounds differ")
    count = len(verified["rows"])
    if not 0 <= verified["matching_total"] <= 2**53-1 or count != min(
            verified["limit"], max(0, verified["matching_total"]-verified["offset"])):
        raise ValueError("feature page counts differ")
    if verified["has_more"] != (verified["offset"]+count < verified["matching_total"]):
        raise ValueError("feature page continuation differs")
    if verified["reset_required"] and verified["offset"] != 0:
        raise ValueError("a generation reset starts at the first page")
    import re
    if re.fullmatch(r"[0-9a-f]{32}", verified["revision"]) is None:
        raise ValueError("invalid feature generation identity")
    return verified
