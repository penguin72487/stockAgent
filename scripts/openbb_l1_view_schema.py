"""Conservative, metadata-only identities for canonical OpenBB L1 view groups.

This is not a file-content audit or a portable Parquet schema serialization.
The caller still owns source validation, immutability, Hive-path checks, and
the DuckDB version/settings policy. Unknown representations opt out of grouping.
"""

from __future__ import annotations

import hashlib
import json
import re

import pyarrow as pa
import pyarrow.parquet as pq


SCHEMA_IDENTITY_REVISION = 1
# The complete group tree is currently exposed through SchemaDescriptor::ToString
# inside ParquetSchema.__repr__, not a stable public serialization API. Re-audit
# that representation before adding a version; an upgrade must not opt itself in.
_SUPPORTED_ARROW_VERSIONS = frozenset({"25.0.0"})
_SCHEMA_HEADER = re.compile(
    r"<pyarrow\._parquet\.ParquetSchema object at 0x[0-9a-fA-F]+>"
)
_GROUP_LINE = re.compile(
    r"(?:required|optional|repeated) group field_id=-?\d+ [^{};\r\n]+ \{"
)
_LEAF_LINE = re.compile(
    r"(?:required|optional|repeated) "
    r"(?:boolean|int32|int64|int96|float|double|binary|fixed_len_byte_array\(\d+\))"
    r" field_id=-?\d+ [^{};\r\n]+;"
)
_PHYSICAL_TYPES = frozenset(
    {
        "BOOLEAN",
        "INT32",
        "INT64",
        "INT96",
        "FLOAT",
        "DOUBLE",
        "BYTE_ARRAY",
        "FIXED_LEN_BYTE_ARRAY",
    }
)


def _schema_tree(schema: pq.ParquetSchema) -> str | None:
    """Keep all group annotations and IDs, but not the object-address header."""

    header, separator, tree = str(schema).partition("\n")
    if not separator or not _SCHEMA_HEADER.fullmatch(header):
        return None
    if not tree.endswith("}\n") or "\x00" in tree:
        return None
    lines = tree.splitlines()
    if not lines or not lines[0].startswith("required group "):
        return None
    if not _GROUP_LINE.fullmatch(lines[0]):
        return None
    depth = 1
    for index, line in enumerate(lines[1:], start=1):
        if line == "  " * (depth - 1) + "}":
            depth -= 1
            if depth == 0 and index != len(lines) - 1:
                return None
        elif depth > 0 and line.startswith("  " * depth):
            body = line[2 * depth :]
            if _GROUP_LINE.fullmatch(body):
                depth += 1
            elif not _LEAF_LINE.fullmatch(body):
                return None
        else:
            return None
    return tree if depth == 0 else None


def schema_identities(parquet_file: pq.ParquetFile) -> tuple[str, str | None]:
    """Compute this open footer's Arrow digest once for both audit contracts.

    No value is supplied by a manifest or reused across files. Invalid Arrow
    metadata still raises for the mandatory integrity audit; an unsupported
    physical representation only disables the optional reader optimization.
    """

    arrow_digest = hashlib.sha256(
        parquet_file.schema_arrow.remove_metadata().serialize().to_pybytes()
    ).digest()
    return arrow_digest.hex(), _physical_schema_identity(parquet_file, arrow_digest)


def physical_schema_identity(parquet_file: pq.ParquetFile) -> str | None:
    """Compatibility capability probe; the joint audit uses schema_identities."""

    try:
        return schema_identities(parquet_file)[1]
    except MemoryError:
        raise
    except Exception:
        return None


def _physical_schema_identity(
    parquet_file: pq.ParquetFile, arrow_digest: bytes,
) -> str | None:
    """Hash already-open metadata without reopening, reading rows, or caching it.

    Arrow schemas alone erase distinctions such as INT96 versus nanosecond
    timestamps. Leaf descriptors alone omit group nodes and footer metadata
    such as GeoParquet's ``geo`` key. Include all three, plus exact metadata KV
    bytes. ``None`` means the caller must keep the ordinary reader path; it is
    never an identity shared by files whose schema could not be understood.
    """

    try:
        version = pa.__version__
        if version not in _SUPPORTED_ARROW_VERSIONS:
            return None
        schema = parquet_file.schema
        tree = _schema_tree(schema)
        if tree is None:
            return None
        columns: list[dict[str, object]] = []
        for index in range(len(schema)):
            column = schema.column(index)
            logical = json.loads(column.logical_type.to_json())
            if (
                not isinstance(logical, dict)
                or not isinstance(logical.get("Type"), str)
                or not logical["Type"]
            ):
                return None
            description = {
                "name": column.name,
                "path": column.path,
                "physical_type": column.physical_type,
                "logical_type": logical,
                "converted_type": column.converted_type,
                "length": column.length,
                "precision": column.precision,
                "scale": column.scale,
                "max_definition_level": column.max_definition_level,
                "max_repetition_level": column.max_repetition_level,
            }
            if any(
                not isinstance(description[key], str)
                for key in ("name", "path", "physical_type", "converted_type")
            ):
                return None
            if description["physical_type"] not in _PHYSICAL_TYPES:
                return None
            if any(
                type(description[key]) is not int
                for key in (
                    "length",
                    "precision",
                    "scale",
                    "max_definition_level",
                    "max_repetition_level",
                )
            ):
                return None
            columns.append(description)

        metadata = parquet_file.metadata.metadata
        if metadata is None:
            metadata = {}
        if not isinstance(metadata, dict) or any(
            not isinstance(key, bytes) or not isinstance(value, bytes)
            for key, value in metadata.items()
        ):
            return None

        digest = hashlib.sha256()

        def add(value: bytes) -> None:
            digest.update(len(value).to_bytes(8, "little"))
            digest.update(value)

        add(b"openbb-l1-physical-schema-identity")
        add(str(SCHEMA_IDENTITY_REVISION).encode("ascii"))
        add(version.encode("ascii"))
        add(arrow_digest)
        add(tree.encode("utf-8"))
        add(
            json.dumps(
                columns, sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode("utf-8")
        )
        add(len(metadata).to_bytes(8, "little"))
        for key, value in sorted(metadata.items()):
            add(key)
            add(value)
        return digest.hexdigest()
    except MemoryError:
        raise
    except Exception:
        # This helper is only an optimization capability probe. Unsupported
        # Arrow APIs/representations must not grant schema equivalence.
        return None
