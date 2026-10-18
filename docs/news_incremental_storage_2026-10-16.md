# 新聞資料集：完整邏輯版本、增量實體儲存

## 結論與範圍

使用者要求版本只增加差異，不因新增版本就另存全量。原本新聞建置器的
`--refresh` 每次重寫四張表，現已在同一入口改成 copy-on-write 分區共用。
這是儲存與 manifest ABI 2 的變更，四表欄位、原文、NULL、時鐘、來源與使用旗標不變。
通用 `data_*` README 同步說明這個原則，但**其他 provider 未因此全部轉換格式**。

正式 `data_news` 既有全量基準 `versions/20261016T012637Z-fa6c4df0` 保留；
2026-10-16 第一階段沒有對正式全庫執行 refresh，也沒有回收既有版本。新版路徑已由本機測試驗證，
以下節省數字是小型測試，不是正式數百萬篇語料的實測節省。

2026-10-17 已以原入口執行正式 refresh：
`news-native-migration-20261016T155149-d59732e7`。固定來源投影完成，舊分區索引建置中；
新 head 尚未提交，舊版仍是可用版本，不能把階段進度稱為全庫驗收。完整遷移與實際回收量見
[共同遷移紀錄](dataset_incremental_storage.md#正式遷移紀錄2026-10-17)。

## 儲存與讀取契約

- 每版 manifest 都列出當版的完整 `parts`。ABI 2 路徑相對於資料集根目錄，
  可以指向之前版本的不可變檔案；不是將歷代版本逐一 UNION。
- 完全相同的 typed row 以多重集合匹配；保留原始重複觀測，不以 DISTINCT 去除。
- 新增／修訂列另寫；舊分區全部保留時直接引用。若僅部分保留，重寫其中仍需要的列；
  刪除透過新版清單不再引用呈現。舊檔與舊版仍能讀取。
- 這是 Parquet 分區粒度的增量，不是逐列／逐 byte 的最小 delta。
  部分變動分區的未變列可能隨分區重寫，來源 SHA 改變也可能影響大量 provenance 列。
- 建置器來源投影另以固定 1 MiB chunks 保存；相同位置且相同 bytes 的塊共用。
  舊全量 SQLite 可直接提供有範圍與 SHA 的 bytes，不為了遷移再複製一份基準。
  chunk 大小、每塊 SHA 與完整投影 SHA 均驗證，還原結果需 byte-exact。
- 新交易驗證、發布後，只清理自身的 `snapshot.sqlite3`、`build.sqlite3` 及其 WAL／SHM 暫存。
  完整來源投影仍可精確重建。既有來源、舊版 SQLite、Parquet、收據與 chunks 不刪除。
- 不增加新下載器、全域物件池、自動 GC、壓實服務、同步或訓練注入。
  保留版本引用的任何舊分區或 chunk 都受保護，不能按所在目錄的年代刪除。

讀取使用 `news_table_paths` 或既有文章／文字 iterator，兼容 ABI 1 與 2：

```python
import json
from pathlib import Path
from stockagent.data.news_dataset import news_table_paths

root = Path("data_news")
manifest = json.loads((root / "manifest.json").read_text())  # 一次固定版本
paths = news_table_paths(root, "articles", manifest=manifest)
```

不可再用 `generation / part["path"]` 解讀所有格式，也不可 glob 全部版本。
固定舊版時讀取其 `versions/<版本>/manifest.json`，仍以資料集根目錄解析。
現有來源沒有因此獲得新的使用授權或公開／遠端發布權限。

## 失敗與恢復

原 writer lock、來源投影、游標、builder fingerprint 及驗證後才切換 manifest 的邊界保留。
匹配舊列與寫新檔的索引狀態共用同一 SQLite transaction；中斷可從已提交游標續行。
來源或舊分區 SHA 不符仍拒絕接受；不能用節省空間為理由省略驗證。

本次另修正「發布成功後清理中斷」：若 root manifest 已指向 active generation，
再次啟動只能重驗並完成清理，不能重新進入建置、覆寫已發布分區。
清理也要求 manifest 是該 active transaction 的已發布內容，不得藉入口清掉其他版本。
不相容的未發布建置仍拒絕續跑；已完成候選版可用原有 `--retry-verification`。

## 驗證證據

相關測試：

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s \
  test/test_news_training_dataset.py \
  test/test_news_dataset_snapshots.py \
  test/test_data_readmes.py
```

2026-10-16 本機通過 **57 項**，涵蓋無變動、追加、修訂、移除、重複觀測、
舊版路徑／全量來源投影、chunk 污損／越界、未提交匹配恢復、發布後中斷、
獨立關聯檢查、README 範例與連結。
最初一輪新增的越界測試錯把回到同一目錄的路徑當成越界；修正測試為真正跨 generation，
原失敗 log 保留，不掩蓋成首次即全數成功。

正式 run：`incremental-contract-tests-v2-20261016T124658-4876b3a1`。
任務：`news-incremental-versions-20261016`。
小型實際 SQLite／Parquet 流程另存
`artifacts/data_quality/news_incremental_versions_20261016/fixture_storage.json`：

| 測試情境 | 沿用 Parquet | 新寫 Parquet |
| --- | ---: | ---: |
| 初次基準 | 0 | 9 檔，48,188 bytes |
| 無變動 refresh | 9 檔，48,188 bytes | 0 檔，0 bytes |
| 一個標題修訂 | 7 檔，34,597 bytes | 2 檔，13,593 bytes |

無變動 refresh 的來源快照新增 chunk bytes 亦為 0；manifest／收據仍有少量版本成本，
不能把「零新增 Parquet」說成「整個版本零 bytes」。小來源投影不足 1 MiB，修訂時整塊
28,672 bytes 重寫，展示了粒度限制。另有多塊測試證明只改中間一塊時僅新增 1 MiB。
增量結果與獨立全量重建逐表多重集合相等，舊 Parquet SHA 與建置前後來源 SHA 保持不變。
測試產生的臨時工作目錄於結束時回收，不涉及正式資料刪除。

## 尚未完成的邊界

- 尚未實測正式全庫 refresh 的耗時、峰值空間及總磁碟節省。
- 本次不是建置 CPU 複雜度優化：目前仍掃描、轉換及比較完整來源，建立暫存索引，
  並完整核對候選版的所有引用檔案。峰值工作空間仍可能明顯高於永久增量大小。
- 尚未遷移所有 `data_*` provider；各來源的版本化、修訂與原件責任仍由原 writer 決定。
- 沒有清掉舊全量基準或替使用者決定保留期限，也沒有開啟遠端發布。

後續正常更新仍使用同一命令，不另開平行版本管線：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_news_dataset.py --refresh --verification-memory-mb 4096
```
