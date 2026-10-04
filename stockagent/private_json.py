"""Optional private-cache decoder with Python's exact integer semantics.

Public wire bytes keep their canonical owners. Untyped msgspec decoding retains
arbitrary Python integers. Encoding stays with stdlib: native encoders can turn
non-finite floats into null, or change legacy value/key semantics.
"""

from __future__ import annotations

import json
from typing import Any

try:
    import msgspec as _native
except ImportError:
    _native = None


def native_available() -> bool:
    return _native is not None


def loads(body: bytes) -> Any:
    if _native is not None:
        try:
            return _native.json.decode(body)
        except (ValueError, TypeError, OverflowError, RecursionError):
            pass
    return json.loads(body)


def dumps(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")
