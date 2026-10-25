import gzip
import hashlib
import json

from downloader import download_public_economic_history as owner
from downloader.download_keyed_public_catalogs import RequestBudget
from scripts.benchmark_public_economic_receipts import benchmark


def test_command_entrypoint_loads_from_outside_repository(tmp_path):
    from pathlib import Path
    import subprocess
    import sys
    script = Path(__file__).resolve().parents[1] / "scripts/benchmark_public_economic_receipts.py"
    result = subprocess.run([sys.executable, str(script), "--help"], cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "--source-inventory" in result.stdout


def test_complete_selected_scope_uses_canonical_verifier_and_native_byte_readback(tmp_path):
    root = tmp_path / "native"
    root.mkdir()
    job = owner.core_jobs()[0]

    class Transport:
        def __init__(self, *args, **kwargs):
            pass

        def fetch(self, *args):
            return b'[["time","cell_value","data_type_code","category_code"],["2025-01","-0.000","SM","44X72"]]'

    receipt = owner.acquire(job, root, {"CENSUS_API_KEY": "secret"}, RequestBudget(1),
                            max_bytes=1024, min_free_bytes=0, transport_factory=Transport)
    assert receipt["status"] == "acquired"
    (root / "catalog").mkdir()
    (root / "catalog/moi_seasons.json").write_text('{"jobs":[]}')
    inventory = tmp_path / "inventory.json"
    inventory.write_text(json.dumps({"rows": [
        {"kind": "source_endpoint", "id": f"economic:{job.provider}:{job.dataset}"},
        {"kind": "source_endpoint", "id": "economic:census:unknown"},
        {"kind": "source_endpoint", "id": "other:unrelated"}]}))
    output = tmp_path / "measurement"
    result = benchmark(inventory=inventory, economic_root=root, frankfurter_root=tmp_path / "ff", output=output)
    assert result["source_count"] == 2
    assert result["status_counts"] == {"receipt_verified": 1, "job_definition_unavailable": 1}
    assert result["provider_calls"] == result["source_writes"] == 0
    assert len(result["payloads"]) == len(receipt["files"])
    assert all(row["expected_sha256"] == row["readback_sha256"] for row in result["payloads"])
    for item in result["native_metadata"]:
        raw = gzip.decompress((output / item["archive"]).read_bytes())
        assert hashlib.sha256(raw).hexdigest() == item["sha256"]
    assert owner.read_json(owner.receipt_path(root, job))["query_fingerprint"] == receipt["query_fingerprint"]
    unknown = next(row for row in result["sources"] if row["dataset"] == "unknown")
    assert unknown["source_binding_eligible"] is False
