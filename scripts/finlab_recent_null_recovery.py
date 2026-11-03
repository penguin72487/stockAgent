"""Safely recover one FinLab 2.1.1 recent-merge schema edge case.

The SDK's on-disk stream merger derives its output schema from the old cache.
An all-null Arrow column cannot accept its first non-null delta value.  This
adapter promotes only such null fields in a temporary copy and then delegates
the actual merge and upstream hash check back to the SDK.  The original cache
is replaced only after that check succeeds, under the SDK's own file lock.
"""
from __future__ import annotations

from contextlib import contextmanager
from importlib.metadata import version
import os
from pathlib import Path
import re
import tempfile

import pyarrow as pa
from pyarrow import ipc


_NULL_CAST = re.compile(r"Unsupported cast from [^\r\n]{1,128} to null using function cast_null\Z")


def _is_sdk_null_cast(exc: BaseException) -> bool:
    if type(exc) is not pa.ArrowNotImplementedError or not _NULL_CAST.fullmatch(str(exc)):
        return False
    tb = exc.__traceback__
    while tb is not None:
        if (tb.tb_frame.f_globals.get("__name__") == "finlab.data.recent_merge"
                and tb.tb_frame.f_code.co_name == "stream_merge_recent_feather"):
            return True
        tb = tb.tb_next
    return False


def _promote_null_fields_and_merge(storage, name, short_df, expected_hash, expiry):
    """Return True on verified publication, False on hash mismatch, None if inapplicable."""
    from finlab.data.recent_merge import stream_merge_recent_feather

    if (not isinstance(expected_hash, str)
            or re.fullmatch(r"[0-9a-f]{7}", expected_hash) is None
            or not short_df.columns.is_unique):
        return None
    # The same SDK method uses this accessor and takes the same exclusive lock.
    file_path = storage._existing_data_file_path(name)
    if file_path is None or storage._is_legacy_pickle_data_file(file_path):
        return None
    directory = Path(file_path).parent
    temporary = []
    try:
        with storage._file_lock(file_path, exclusive=True):
            if not os.path.isfile(file_path):
                return None
            delta_schema = pa.Table.from_pandas(short_df, preserve_index=False).schema
            with pa.memory_map(file_path) as source:
                reader = ipc.open_file(source)
                original = reader.schema
                if (len(set(original.names)) != len(original.names)
                        or not set(delta_schema.names).issubset(original.names)):
                    return None
                fields = []
                promoted_fields = 0
                for field in original:
                    index = delta_schema.get_field_index(field.name)
                    delta_type = delta_schema.field(index).type if index >= 0 else pa.null()
                    if pa.types.is_null(field.type) and not pa.types.is_null(delta_type):
                        fields.append(field.with_type(delta_type))
                        promoted_fields += 1
                    else:
                        fields.append(field)
                if not promoted_fields:
                    return None
                schema = pa.schema(fields, metadata=original.metadata)
                fd, path = tempfile.mkstemp(prefix=".finlab-null-schema-", suffix=".tmp", dir=directory)
                os.close(fd)
                temporary.append(path)
                with ipc.new_file(path, schema, options=ipc.IpcWriteOptions(compression="lz4")) as writer:
                    for index in range(reader.num_record_batches):
                        writer.write_batch(reader.get_batch(index).cast(schema))
            fd, merged = tempfile.mkstemp(prefix=".finlab-null-merge-", suffix=".tmp", dir=directory)
            os.close(fd)
            temporary.append(merged)
            if not stream_merge_recent_feather(path, merged, short_df, expected_hash):
                return False
            os.replace(merged, file_path)
        # Keep the SDK's normal cache invalidation, creation and expiry semantics.
        storage._record_data_file_write(name, expiry=expiry)
        return True
    finally:
        for path in temporary:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass


@contextmanager
def recent_null_recovery(storage, key):
    """Temporarily adapt the current FileStorage instance for one exact key."""
    state = {"recovered": False, "hash_mismatch": False}
    from finlab.data.storage import FileStorage

    eligible = (version("finlab") == "2.1.1" and type(storage) is FileStorage
                and "merge_recent_delta" not in vars(storage))
    if not eligible:
        yield state
        return
    original = storage.merge_recent_delta

    def merge_recent_delta(name, short_df, expected_hash, expiry=None):
        try:
            return original(name, short_df, expected_hash, expiry=expiry)
        except pa.ArrowNotImplementedError as exc:
            if name != key or not _is_sdk_null_cast(exc):
                raise
            result = _promote_null_fields_and_merge(storage, name, short_df, expected_hash, expiry)
            if result is None:
                raise
            state["recovered"] |= result
            state["hash_mismatch"] |= not result
            return result

    storage.merge_recent_delta = merge_recent_delta
    try:
        yield state
    finally:
        del storage.merge_recent_delta
