"""Forward-only ZIP framing retains stock readers, CRC, modes and short-write gates."""
import io
import stat
import zipfile

import pytest

from scripts import benchmark_packed_metadata as module


def test_forward_only_sink_never_seeks_or_queries_underlying_position():
    class Sink(io.BytesIO):
        def seek(self, *args):
            raise AssertionError("underlying output must never seek")
        def tell(self):
            raise AssertionError("underlying output must never query its offset")

    source = Sink()
    writer = module._SequentialPackWriter(source)
    expected = {f"原始/{index:05d}.txt": f"{index},-3.5,NULL,股\n".encode() for index in range(256)}
    with zipfile.ZipFile(writer, "x", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
        for name, raw in expected.items():
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o640) << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            with archive.open(info, "w", force_zip64=True) as member:
                member.write(raw)
    raw = source.getvalue()
    assert writer.tell() == len(raw)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        assert archive.namelist() == list(expected)
        assert archive.testzip() is None
        for info in archive.infolist():
            assert info.flag_bits & 8  # Standard data-descriptor framing.
            assert (info.external_attr >> 16) == (stat.S_IFREG | 0o640)
            assert archive.read(info.filename) == expected[info.filename]


def test_forward_only_sink_does_not_claim_short_write_completion():
    class ShortWriter:
        def write(self, raw):
            return len(raw) - 1
    sink = module._SequentialPackWriter(ShortWriter())
    with pytest.raises(OSError, match="short sequential ZIP write"):
        sink.write(b"original bytes")
    assert sink.tell() == 0


def test_reference_pack_writer_requires_exact_original_source_hash(tmp_path):
    from scripts.benchmark_packed_metadata import pack_writer_reference
    source = tmp_path / "original.py"
    source.write_text("def _write_pack(*args, **kwargs):\n    return None\n")
    with pytest.raises(ValueError, match="source SHA"):
        pack_writer_reference(source, "0" * 64)
