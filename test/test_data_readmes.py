"""Documentation-only coverage, bounded metadata and preservation contracts."""
from __future__ import annotations

import hashlib
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from scripts import write_data_readmes as docs


def description(title="測試行情"):
    return {"title": title, "category": "台股與台灣衍生品", "role": "來源原值",
            "purpose": "研究來源行情與單位，不將文件抽樣當作全歷史。",
            "grains": ["一個市場、一個標的、一個交易日。"],
            "layout": [["source.parquet", "來源行情"]],
            "fields": [["date", "來源日期"], ["volume", "股數"]],
            "timing": ["交易日追新；本次不改排程。"],
            "cautions": ["缺值不補零；不更改原始值。"],
            "sources": ["downloader/source.py"], "evidence": ["receipt.json"],
            "samples": ["source.parquet"], "related": []}


@pytest.fixture
def repo(tmp_path):
    root = tmp_path / "checkout"
    (root / "downloader").mkdir(parents=True)
    (root / "downloader/source.py").write_text("# test source\n")
    (root / "configs").mkdir()
    (root / "data_prices").mkdir()
    pq.write_table(pa.table({"date": ["2020-01-02", "2020-01-03"],
                             "volume": [1000, None]}), root / "data_prices/source.parquet")
    specs = {"data_prices": description()}
    (root / "configs/data_readmes.yaml").write_text(
        yaml.safe_dump({"version": 1, "datasets": specs}, allow_unicode=True))
    return root


def catalog(repo):
    return repo / "configs/data_readmes.yaml"


def write_catalog(repo, specs):
    catalog(repo).write_text(yaml.safe_dump({"version": 1, "datasets": specs}, allow_unicode=True))


def test_each_root_gets_its_own_readme_and_sources_unchanged(repo):
    source = repo / "data_prices/source.parquet"
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    result = docs.run(repo, catalog(repo), write=True)
    assert result["state"] == "accepted", result["errors"]
    assert result["root_count"] == result["readme_count"] == 1
    assert result["source_data_writes"] == result["network_calls"] == 0
    text = (repo / "data_prices/README.md").read_text()
    assert "# data_prices：測試行情" in text
    assert "不是全庫唯一筆數" in text
    assert "## 版本與增量儲存" in text
    assert "只儲存新增與異動" in text
    assert "README 不是遷移或刪除收據" in text
    assert "2020-01-02" in text and "2020-01-03" in text
    assert all(f"## {section}" in text for section in docs.SECTIONS)
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    assert docs.run(repo, catalog(repo), check=True)["state"] == "accepted"


def test_default_is_read_only_and_unknown_roots_fail(repo):
    assert docs.run(repo, catalog(repo))["state"] == "planned"
    assert not (repo / "data_prices/README.md").exists()
    (repo / "data_new").mkdir()
    result = docs.run(repo, catalog(repo), write=True)
    assert result["state"] == "failed"
    assert "data_new" in " ".join(result["errors"])
    assert not (repo / "data_prices/README.md").exists()


def test_preserve_existing_canonical_owner_document(repo):
    source = repo / "data_news"
    source.mkdir()
    text = "# News owner\n\n完整既有新聞說明，不能被通用模板覆寫。\n"
    (source / "README.md").write_text(text)
    specs = {"data_prices": description(), "data_news": description("新聞")}
    specs["data_news"]["preserve_existing"] = True
    write_catalog(repo, specs)
    result = docs.run(repo, catalog(repo), write=True)
    assert result["state"] == "accepted", result["errors"]
    assert result["preserved"] == ["data_news"]
    assert (source / "README.md").read_text() == text
    assert result["readme_count"] == 2


def test_missing_owner_document_is_not_silently_reported_complete(repo):
    specs = {"data_prices": description()}
    specs["data_prices"]["preserve_existing"] = True
    write_catalog(repo, specs)
    result = docs.run(repo, catalog(repo), write=True)
    assert result["state"] == "failed"
    assert not (repo / "docs/data_datasets.md").exists()


def test_unmanaged_readme_is_not_overwritten(repo):
    path = repo / "data_prices/README.md"
    path.write_text("# Human-owned\n")
    result = docs.run(repo, catalog(repo), write=True)
    assert result["state"] == "failed"
    assert path.read_text() == "# Human-owned\n"
    assert not (repo / "docs/data_datasets.md").exists()


def test_symlink_data_root_has_working_cross_repo_and_local_links(repo, tmp_path):
    physical = tmp_path / "other-service/dataset"
    physical.mkdir(parents=True)
    (repo / "data_link").symlink_to(physical, target_is_directory=True)
    specs = {"data_prices": description(), "data_link": description("符號連結資料")}
    specs["data_link"]["related"] = ["data_prices"]
    write_catalog(repo, specs)
    result = docs.run(repo, catalog(repo), write=True)
    assert result["state"] == "accepted", result["errors"]
    readme = physical / "README.md"
    assert readme.exists() and (repo / "data_link").is_symlink()
    assert not docs.validate_markdown(readme, readme.read_text())
    assert docs.run(repo, catalog(repo), check=True)["state"] == "accepted"


def test_footer_only_inspection_no_table_reads(repo, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("A documentation inventory must not read source rows")

    monkeypatch.setattr(pq, "read_table", forbidden)
    monkeypatch.setattr(pq.ParquetFile, "iter_batches", forbidden)
    monkeypatch.setattr(pq.ParquetFile, "read", forbidden)
    samples = docs.inspect_samples(repo / "data_prices", ["*.parquet"])
    assert samples[0]["rows"] == 2
    assert samples[0]["bounds"] == {"column": "date", "min": "2020-01-02", "max": "2020-01-03"}


def test_no_integer_timestamp_guessing_or_invented_missing_stats(tmp_path):
    pq.write_table(pa.table({"date": [123, 456]}), tmp_path / "integer.parquet")
    pq.write_table(pa.table({"date": ["2020-01-01"]}), tmp_path / "no_stats.parquet",
                   write_statistics=False)
    for filename in ("integer.parquet", "no_stats.parquet"):
        sample = docs.inspect_samples(tmp_path, [filename])[0]
        assert sample["bounds"] is None


def test_zero_row_and_json_only_samples_are_legitimate(tmp_path):
    pq.write_table(pa.table({"date": pa.array([], type=pa.date32())}), tmp_path / "empty.parquet")
    sample = docs.inspect_samples(tmp_path, ["empty.parquet"])[0]
    assert sample["rows"] == 0 and sample["state"] == "footer_only"
    assert sample["bounds"] is None
    assert docs.inspect_samples(tmp_path, []) == []


def test_sampling_bounded_by_patterns_not_all_versions(tmp_path):
    for i in range(10):
        pq.write_table(pa.table({"volume": [i]}), tmp_path / f"source{i}.parquet")
    assert len(docs.inspect_samples(tmp_path, ["*.parquet"])) == 1
    assert len(docs.inspect_samples(tmp_path, [f"source{i}.parquet" for i in range(10)])) == 3


def test_changed_source_rejected_and_error_text_not_disclosed(repo, monkeypatch):
    ticks = iter([(1, 2, 3, 4), (1, 2, 3, 5)])
    monkeypatch.setattr(docs, "stat_identity", lambda path: next(ticks))
    sample = docs.inspect_samples(repo / "data_prices", ["source.parquet"])[0]
    assert sample == {"path": "source.parquet", "state": "changed_during_read"}


def test_actual_reading_example_executes_bounded_batch_and_preserves_nulls(repo, monkeypatch, capsys):
    monkeypatch.chdir(repo)
    samples = docs.inspect_samples(repo / "data_prices", ["source.parquet"])
    example = "\n".join(docs.reading_example("data_prices", samples, description()))
    code = re.search(r"run_fintech_python - <<'PY'\n(.*?)\nPY", example, re.S)[1]
    exec(compile(code, "<readme-example>", "exec"), {})
    assert "此檔列數： 2" in capsys.readouterr().out
    assert pq.read_table(repo / "data_prices/source.parquet")["volume"].null_count == 1


def test_local_link_and_code_checks_find_errors(tmp_path):
    path = tmp_path / "README.md"
    content = "# Bad\n\n[missing](missing.parquet)\n\n```bash\nrun_fintech_python - <<'PY'\nnot valid python !\nPY\n"
    errors = docs.validate_markdown(path, content)
    assert any("Unclosed code fence" in s for s in errors)
    assert any("Broken local link" in s for s in errors)
    assert any("Invalid Python example" in s for s in errors)


def test_unsafe_catalog_paths_are_rejected(repo):
    specs = {"data_prices": description()}
    specs["data_prices"]["samples"] = ["../.env"]
    write_catalog(repo, specs)
    with pytest.raises(ValueError, match="Unsafe relative paths"):
        docs.run(repo, catalog(repo), write=True)


def test_catalog_covers_every_real_existing_dataset():
    real_repo = Path(__file__).resolve().parents[1]
    specs = docs.load_catalog(real_repo, catalog(real_repo))
    roots = docs.discover_roots(real_repo)
    # Source-only CI checkouts need not contain the node's ignored data payloads.
    # Every materialized root must still have its own reviewed description.
    assert roots.keys() <= specs.keys()
    assert len({spec["title"] for spec in specs.values()}) == len(specs)
    assert all(len(spec["grains"]) >= 2 and len(spec["cautions"]) >= 2 for spec in specs.values())
