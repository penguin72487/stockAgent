#!/usr/bin/env python3
"""Maintain human dataset documentation without querying or modifying sources.

Descriptions are reviewed in configs/data_readmes.yaml. File examples only inspect
selected Parquet footers; their row/date bounds are never whole-lake coverage.
"""
from __future__ import annotations

import argparse
import ast
from datetime import date, datetime, timezone
import glob
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import quote, unquote

import yaml


MARKER = "<!-- stockagent:data-readme:v1 -->"
GROUPS = ("台股與台灣衍生品", "新聞", "加密貨幣", "全球金融、總體與公共資料", "舊版相容資料")
SECTIONS = ("一筆資料代表什麼", "目錄裡有什麼", "主要欄位與單位", "時間、更新與可用性",
            "版本與增量儲存", "本機檔案抽樣", "如何讀取", "使用前注意", "來源與查核入口")


def discover_roots(repo: Path) -> dict[str, Path]:
    return {p.name: p for p in sorted(repo.glob("data_*")) if p.is_dir()}


def safe_relative(value: str) -> bool:
    path = Path(value)
    return bool(value) and not path.is_absolute() and ".." not in path.parts


def load_catalog(repo: Path, catalog: Path) -> dict:
    document = yaml.safe_load(catalog.read_text(encoding="utf-8"))
    if document.get("version") != 1 or not isinstance(document.get("datasets"), dict):
        raise ValueError("Expected version=1 and a datasets mapping")
    specs = document["datasets"]
    required = {"title", "category", "role", "purpose", "grains", "layout", "fields",
                "timing", "cautions", "sources", "evidence", "samples", "related"}
    for name, spec in specs.items():
        if not re.fullmatch(r"data_[A-Za-z0-9_]+", name) or required - spec.keys():
            raise ValueError(f"Invalid/incomplete description: {name}")
        if spec["category"] not in GROUPS:
            raise ValueError(f"Unknown category: {name}")
        for key in ("sources", "evidence", "samples"):
            if not all(isinstance(p, str) and safe_relative(p) for p in spec[key]):
                raise ValueError(f"Unsafe relative paths: {name}.{key}")
        for source in spec["sources"]:
            if not (repo / source).is_file():
                raise ValueError(f"Missing implementation reference: {name}: {source}")
        if any(other not in specs for other in spec["related"]):
            raise ValueError(f"Unknown related dataset: {name}")
    return specs


def coverage_errors(roots: dict, specs: dict) -> list[str]:
    return ([f"缺少個別說明：{name}" for name in sorted(roots.keys() - specs.keys())]
            + [f"設定指向不存在的資料目錄：{name}" for name in sorted(specs.keys() - roots.keys())])


def link(label: str, target: Path, base: Path) -> str:
    # A ../docs link inside a symlinked data root resolves from its physical
    # location, not from the checkout's lexical alias.
    relative = os.path.relpath(target, base.resolve())
    return f"[{label}]({quote(relative, safe='/._-')})"


def stat_identity(path: Path) -> tuple[int, int, int, int]:
    info = path.stat()
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def footer_bounds(pf) -> dict | None:
    import pyarrow as pa

    if pf.metadata.num_row_groups > 512:
        return None  # Remain bounded, rather than report a partial-file bound.
    preferred = ("date", "trade_date", "trading_date", "event_ts", "event_ts_utc",
                 "event_date_utc", "observation_date", "source_index")
    for name in preferred:
        if name not in pf.schema_arrow.names:
            continue
        field = pf.schema_arrow.field(name)
        # Never guess epoch units or timezone from an integer named ts/date.
        if not (pa.types.is_date(field.type) or pa.types.is_timestamp(field.type)
                or pa.types.is_string(field.type) or pa.types.is_large_string(field.type)):
            continue
        index = next((i for i in range(pf.metadata.num_columns)
                      if pf.metadata.schema.column(i).path == name), None)
        if index is None:
            continue
        lower, upper = [], []
        complete = True
        for group in range(pf.metadata.num_row_groups):
            chunk = pf.metadata.row_group(group).column(index)
            stats = chunk.statistics
            if stats is not None and stats.null_count == pf.metadata.row_group(group).num_rows:
                continue
            if stats is None or not stats.has_min_max:
                complete = False
                break
            lo, hi = stats.min, stats.max
            if isinstance(lo, (date, datetime)) and isinstance(hi, (date, datetime)):
                lo, hi = lo.isoformat(), hi.isoformat()
            elif isinstance(lo, str) and isinstance(hi, str):
                if not all(re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:[T ].*)?", v) for v in (lo, hi)):
                    complete = False
                    break
            else:
                complete = False
                break
            lower.append(lo)
            upper.append(hi)
        if complete and lower:
            return {"column": name, "min": min(lower), "max": max(upper)}
    return None


def inspect_samples(root: Path, patterns: list[str]) -> list[dict]:
    """At most one file per declared pattern, and three files per data root."""
    import pyarrow.parquet as pq

    result, seen = [], set()
    for pattern in patterns:
        candidate = next((Path(p) for p in glob.iglob(str(root / pattern), recursive=True)
                          if Path(p).is_file()), None)
        if candidate is None or candidate.resolve() in seen:
            continue
        seen.add(candidate.resolve())
        sample = {"path": str(candidate.relative_to(root))}
        try:
            before = stat_identity(candidate)
            with pq.ParquetFile(candidate) as pf:
                sample.update(rows=pf.metadata.num_rows, columns=len(pf.schema_arrow),
                              field_names=pf.schema_arrow.names[:24], bounds=footer_bounds(pf))
            if stat_identity(candidate) != before:
                sample = {"path": sample["path"], "state": "changed_during_read"}
            else:
                sample["state"] = "footer_only"
        except Exception as error:
            # Do not publish raw exceptions, data values or credential URLs.
            sample = {"path": sample["path"], "state": f"unreadable:{type(error).__name__}"}
        result.append(sample)
        if len(result) == 3:
            break
    return result


def table(headers: tuple[str, ...], rows) -> list[str]:
    def cell(value):
        return str(value).replace("|", "\\|").replace("\n", " ")
    return ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |",
            *("| " + " | ".join(cell(c) for c in row) + " |" for row in rows)]


def reading_example(name: str, samples: list[dict], spec: dict) -> list[str]:
    ready = next((s for s in samples if s["state"] == "footer_only"), None)
    if ready:
        code = ["from pathlib import Path", "import pyarrow.parquet as pq", "",
                f"path = Path({str(Path(name) / ready['path'])!r})",
                "with pq.ParquetFile(path) as pf:",
                "    print('此檔列數：', pf.metadata.num_rows)",
                "    print('欄位前 12 個：', pf.schema_arrow.names[:12])",
                "    # 先核對欄位，再只讀少數欄、少量列；不要一次載入全庫。",
                "    columns = pf.schema_arrow.names[:5]",
                "    batch = next(pf.iter_batches(batch_size=128, columns=columns), None)",
                "    print('示範批次列數：', 0 if batch is None else batch.num_rows)"]
    else:
        # JSON/CSV discovery datasets should not acquire fake Parquet examples.
        code = ["from pathlib import Path", "from itertools import islice", "",
                f"root = Path({name!r})",
                "# 只列第一層入口，不讀金鑰、帳戶或原始資料內容。",
                "for path in islice(sorted(root.iterdir()), 30):",
                "    print(path.name, '目錄' if path.is_dir() else '檔案')"]
    return ["以下在專案根目錄執行。例子只讀既有本機檔，不會發出 API 請求、重建資料或寫入來源。",
            "", "```bash", "source scripts/runtime_env.sh", "run_fintech_python - <<'PY'",
            *code, "PY", "```", "",
            "這是檔案結構的最小讀取示例，不是目前有效版本選擇器。正式組裝仍須由下方既有入口",
            "核對 manifest／receipt、時間與單位；不要將所有分區、舊版本與相容視圖無條件串接。"]


def render_readme(repo: Path, name: str, spec: dict, samples: list[dict], stamp: str) -> str:
    root = repo / name
    lines = [f"# {name}：{spec['title']}", "", MARKER, "", spec["purpose"].strip(), "",
             f"分類：{spec['category']}。資料角色：{spec['role']}。", "",
             link("全部資料集索引", repo / "docs/data_datasets.md", root), "",
             "## 一筆資料代表什麼", "", *(f"- {s}" for s in spec["grains"]), "",
             "## 目錄裡有什麼", "", *table(("入口／路徑", "內容與用途"), spec["layout"]), "",
             "上表是本資料集的主要入口；同列的多個名稱是分別的路徑。某個入口未存在時，",
             "請查下方收據與來源程式，不由名稱推定已下載，也不要自行建立空檔冒充取得。", "",
             "## 主要欄位與單位", "", *table(("欄位／欄位族", "意思與單位"), spec["fields"]), "",
             "欄名可能因表格、歷史版本或來源而不同；這是語意導覽，不是所有檔案共用的固定 schema。",
             "讀取前應查看實際 schema，保留原始值、比率、缺值與單位證據，不在讀取時自動套 log 或補零。", "",
             "## 時間、更新與可用性", "", *(f"- {s}" for s in spec["timing"]), "",
             "發布日、資料所屬期間、交易日與下載時間是不同時鐘。已核准的研究估計要保留標記，",
             "不能在文件中升格為精確歷史發布證據。此說明不調整現有排程或研究契約。", "",
             "## 版本與增量儲存", "",
             "版本代表可重現的完整邏輯視圖，不應每次另存一份全量資料。目標是沿用未變動的不可變分區，",
             "只儲存新增與異動；修訂或刪除透過新版本的清單呈現，不覆寫舊版證據。",
             "增量下載不等於增量儲存；實際物理格式與是否已支援分區共用，仍須查本資料集的 writer／manifest。",
             "README 不是遷移或刪除收據；實際進度與已回收容量請看共同架構文件中的正式紀錄。讀取需使用該管線的正式入口，",
             "不要跨版本 glob 或將歷史版本合併加總；任何仍被版本引用的資料都不得回收。", "",
             *(["所有 `data_*` 已登錄共同檔案版本契約："
                + link("增量版本架構與讀取方式", repo / "docs/dataset_incremental_storage.md", root) + "。",
                "共用發布器可將大檔拆為 SHA-256 區塊；未變檔案／區塊共用，新增、修訂與刪除只更新完整清單。",
                "先升級讀取器再啟用 v2；清冊登錄或支援新格式不代表本目錄的舊版原件已搬移，亦不改變來源授權。", "",
                "僅查看本資料集目前格式與既有發布責任（不抓資料、不建新版本）：", "",
                "```bash", "source scripts/runtime_env.sh",
                f"run_fintech_python scripts/publish_data_releases.py layout {name}", "```", ""]
               if (repo / "docs/dataset_incremental_storage.md").is_file() else []),
             "## 本機檔案抽樣", "", f"查核時間：{stamp}。僅查看下列檔案的 Parquet footer，不讀全表。", ""]
    if samples:
        rows = []
        for sample in samples:
            bounds = sample.get("bounds")
            period = (f"{bounds['column']}：{bounds['min']} ～ {bounds['max']}" if bounds
                      else "未知：footer 無適合且完整的日期界限")
            if sample["state"] != "footer_only":
                period = f"未確認（{sample['state']}）；未讀回資料列"
            rows.append((link(sample["path"], root / sample["path"], root),
                         f"{sample['rows']:,}" if "rows" in sample else "未知",
                         sample.get("columns", "未知"), period))
        lines += table(("單一檔案", "此檔列數", "欄數", "此檔日期界限"), rows)
        for sample in samples:
            if sample.get("field_names"):
                lines += ["", f"`{sample['path']}` 的欄位前 {len(sample['field_names'])} 個：",
                          "", ", ".join(f"`{v}`" for v in sample["field_names"]) + "。"]
    else:
        lines += ["本次沒有在宣告的抽樣入口找到可列出的 Parquet；可能是 JSON／CSV 目錄、尚未產生該表，",
                  "或資料仍在其他分段。這不是全庫為空、抓取失敗或歷史已完成的判定。"]
    lines += ["", "這些列數只屬於列出的檔案，不是全庫唯一筆數。日期是檔內欄位的最小／最大值，",
              "不保證中間連續、涵蓋所有標的，亦不等於發布時間或首次可用時間。檔案更新後數字會改變；",
              "全庫總量及完整歷史需由該管線的 manifest／覆蓋稽核另外驗證，不能把多版本或衍生視圖加總。", "",
              "## 如何讀取", "", *reading_example(name, samples, spec), "",
              "## 使用前注意", "", *(f"- {s}" for s in spec["cautions"]),
              "- 原件、修訂版本、快取與研究特徵不是同一資料層；看似重複也不能據此刪除來源或收據。",
              "- 能讀取不代表訓練已驗收。應另核對缺值、唯一鍵、標的生命週期、時點與對應研究版本。", "",
              "## 來源與查核入口", "", "本資料集由下列既有入口負責；閱讀程式不會自動觸發下載：", "",
              *(f"- {link(s, repo / s, root)}" for s in spec["sources"])]
    evidence = [s for s in spec["evidence"] if (root / s).is_file()]
    lines += ["", "本次存在的摘要／收據入口（只確認檔案存在，未在本次重跑取得或完整性驗收）：", ""]
    lines += ([f"- {link(s, root / s, root)}" for s in evidence] if evidence
              else ["- 未找到設定中列出的摘要；請從上述管線確認實際 receipt／manifest 位置，不猜測完成狀態。"])
    lines += ["", "相關資料集：" + "、".join(link(s, repo / s / "README.md", root) for s in spec["related"]) + "。",
              "", "## 這份說明如何維護", "",
              "人類解說由 " + link("data_readmes.yaml", repo / "configs/data_readmes.yaml", root)
              + " 維護；更新實際用途、來源或 schema 時一起修訂。", "",
              "在專案根目錄執行以下命令，只更新說明與選定檔案的 footer 抽樣，不會下載、補值或改動來源資料：", "",
              "```bash", "source scripts/runtime_env.sh",
              "run_fintech_python scripts/write_data_readmes.py --write",
              "run_fintech_python scripts/write_data_readmes.py --check", "```", ""]
    return "\n".join(lines)


def render_index(repo: Path, roots: dict, specs: dict) -> str:
    lines = ["# 本機 data_* 資料集導覽", "", MARKER, "",
             f"目前逐一說明專案根目錄的 **{len(roots)} 個 `data_*` 資料目錄**，含符號連結；每個目錄都有自己的 README。",
             "這是資料用途與讀取入口索引，不是下載進度、完整歷史宣告，也不代表所有資料已注入訓練。", "",
             "## 先從研究問題選入口", "",
             "- 台股日資料與財務：先看台股公開資料、FinLab、FinMind；分鐘與微結構另選對應資料集。",
             "- 期貨／選擇權：分清官方日表、逐合約行情、逐筆、保證金／限額公告及衍生研究視圖。",
             "- 新聞：訓練通常從 data_news 的固定邏輯版本開始；後續版本共用不變分區，data_news_fulltext 是持續寫入的來源庫。",
             "- 加密：Binance、OKX、Bybit 各自獨立；宏觀／鏈上背景不是交易所原生價格。",
             "- 全球總體／外匯：先辨識 provider、經濟序列、單位、期間與修訂版本，再選長表或相容視圖。", ""]
    for group in GROUPS:
        lines += [f"## {group}", ""]
        rows = [(link(name, repo / name / "README.md", repo / "docs"), spec["title"], spec["role"])
                for name, spec in specs.items() if name in roots and spec["category"] == group]
        lines += table(("資料目錄／個別說明", "內容", "角色"), rows) + [""]
    lines += ["## 版本不是全量副本", "",
              "每版應是完整可重現的邏輯視圖；實體沿用未變動分區，只新增或重寫受影響部分。",
              "不是每次將整個 data_* 複製到新版本，也不是用增量下載掩蓋後端每版全量另存。",
              "data_news 的新版 --refresh 已支援此 copy-on-write 清單格式；舊全量基準仍保留。",
              "其他資料目錄仍依各自既有 writer 與 manifest，本次沒有宣稱已全部遷移。",
              *(["全部根目錄已登錄共同政策；共享發布器的大檔區塊版本、reader-first 啟用及實際部署界線見 "
                 + link("全資料集增量版本架構", repo / "docs/dataset_incremental_storage.md", repo / "docs") + "。"]
                if (repo / "docs/dataset_incremental_storage.md").is_file() else []),
              "所有保留版本仍引用的物件都要保護；不能僅看檔案位於舊版本目錄就刪除。", "",
              "## 不能混為一談的數量與時間", "",
              "一列可能是一根 K 棒、一個指標值、一個交易事件、一篇文章、一組股票關聯，或一份資料目錄。",
              "寬表的一列還可能包含數萬個標的欄位。不同粒度不能直接相加；原件、壓實檔、版本與研究視圖也不能重複計數。", "",
              "README 的檔案抽樣只讀少數 footer；抽樣起訖不代表全市場無缺口。正式資料集若有 manifest／驗證收據，",
              "應使用該版本的數量與完整性結論。data_news 的現有人工說明與固定版本統計原樣保留，由新聞建置器維護。", "",
              "資料所屬期間、事件時間、發布時間、交易日與取得時間需分別處理。估計發布規則可以保留供已選研究使用，",
              "但不得改寫成來源證實的精確時刻；缺價格與缺內文不會為湊完整而捏造。", "",
              "## 本次範圍與維護", "",
              "範圍是根目錄實際存在的 data_*，不把 configs/data_sync 等設定當成資料集；子表／分區由各自 README 介紹。",
              "data_bybit 與 data_tw_public 是符號連結，既有入口保持不變；README 工具不搬資料，正式遷移另以逐項收據驗收。", "",
              "人類描述維護在 " + link("configs/data_readmes.yaml", repo / "configs/data_readmes.yaml", repo / "docs") + "。",
              "新增 data_* 時先補上個別用途、粒度與來源；檢查器會拒絕沒有專屬說明的新目錄，不自動生成空泛模板。", "",
              "```bash", "source scripts/runtime_env.sh", "run_fintech_python scripts/write_data_readmes.py --write",
              "run_fintech_python scripts/write_data_readmes.py --check", "```", "",
              "工具只維護 README／此索引及指定的查核收據。不讀 .env、不呼叫 API、不改排程、原值或訓練設定。",
              "資料檔本身維持原本保存政策；可重建的本機 README 抽樣不代表資料可刪除。", ""]
    return "\n".join(lines)


def validate_markdown(path: Path, content: str) -> list[str]:
    errors = []
    if len(re.findall(r"^```", content, re.M)) % 2:
        errors.append(f"Unclosed code fence: {path}")
    for target in re.findall(r"\[[^\]\n]+\]\(([^)]+)\)", content):
        if "://" in target or target.startswith("#"):
            continue
        filename = unquote(target.split("#", 1)[0])
        if filename and not (path.parent / filename).resolve().exists():
            errors.append(f"Broken local link: {path}: {filename}")
    for code in re.findall(r"run_fintech_python - <<'PY'\n(.*?)\nPY", content, re.S):
        try:
            ast.parse(code)
        except SyntaxError:
            errors.append(f"Invalid Python example: {path}")
    return errors


def atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
        os.chmod(temporary, 0o644)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def run(repo: Path, catalog: Path, *, write: bool = False, check: bool = False) -> dict:
    roots = discover_roots(repo)
    specs = load_catalog(repo, catalog)
    errors = coverage_errors(roots, specs)
    if errors:
        return {"state": "failed", "errors": errors, "root_count": len(roots)}
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    updates, samples = {}, {}
    preserved = []
    for name, root in roots.items():
        path = root / "README.md"
        if specs[name].get("preserve_existing"):
            if not path.is_file():
                errors.append(f"Missing canonical-owner README: {name}")
            else:
                preserved.append(name)
            continue
        if write:
            if path.exists() and MARKER not in path.read_text(encoding="utf-8"):
                errors.append(f"Refusing to overwrite unmanaged README: {name}")
                continue
            samples[name] = inspect_samples(root, specs[name]["samples"])
            updates[path] = render_readme(repo, name, specs[name], samples[name], stamp)
    index = repo / "docs/data_datasets.md"
    if write:
        if index.exists() and MARKER not in index.read_text(encoding="utf-8"):
            errors.append("Refusing to overwrite unmanaged docs/data_datasets.md")
        updates[index] = render_index(repo, roots, specs)
        # Validate the entire planned link graph before writing any README.
        for path, content in updates.items():
            for error in validate_markdown(path, content):
                if error.startswith("Broken local link:"):
                    target = unquote(error.split(": ", 2)[-1])
                    if any((path.parent / target).resolve() == p.resolve() for p in updates):
                        continue
                errors.append(error)
        if not errors:
            for path, content in updates.items():
                atomic_text(path, content)
    documents = []
    if write or check:
        for name, root in roots.items():
            path = root / "README.md"
            if not path.is_file():
                errors.append(f"Missing README: {name}")
                continue
            content = path.read_text(encoding="utf-8")
            if not specs[name].get("preserve_existing"):
                for heading in SECTIONS:
                    if f"## {heading}" not in content:
                        errors.append(f"Missing section: {name}: {heading}")
                if not content.startswith(f"# {name}：{specs[name]['title']}"):
                    errors.append(f"Wrong dataset identity: {name}")
            errors.extend(validate_markdown(path, content))
            documents.append({"dataset": name, "sha256": hashlib.sha256(content.encode()).hexdigest(),
                              "bytes": len(content.encode()), "symlink_root": root.is_symlink(),
                              "preserved_existing": name in preserved})
        if index.is_file():
            text = index.read_text(encoding="utf-8")
            errors.extend(validate_markdown(index, text))
            for name in roots:
                if f"[{name}]" not in text:
                    errors.append(f"Missing index entry: {name}")
        else:
            errors.append("Missing docs/data_datasets.md")
    return {"state": "failed" if errors else "accepted" if (write or check) else "planned",
            "checked_at_utc": stamp, "root_count": len(roots), "readme_count": len(documents),
            "updated_count": len(updates) if write and not errors else 0,
            "preserved": preserved, "documents": documents, "footer_samples": samples,
            "network_calls": 0, "source_data_writes": 0, "errors": errors}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--catalog", type=Path, default=Path("configs/data_readmes.yaml"))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help="Refresh managed README files and the index")
    mode.add_argument("--check", action="store_true", help="Check existing documentation; no footer reads")
    parser.add_argument("--receipt", type=Path, help="Optional small documentation acceptance JSON")
    args = parser.parse_args()
    repo = args.repo_root.resolve()
    catalog = args.catalog if args.catalog.is_absolute() else repo / args.catalog
    result = run(repo, catalog, write=args.write, check=args.check)
    if args.receipt:
        receipt = (args.receipt if args.receipt.is_absolute() else repo / args.receipt).resolve()
        if repo / "artifacts" not in receipt.parents:
            parser.error("--receipt must be under this checkout's artifacts/ directory")
        atomic_text(receipt, json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in {"documents", "footer_samples"}},
                     ensure_ascii=False, indent=2))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
