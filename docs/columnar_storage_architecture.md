# 欄式資料湖儲存契約

## 第一性原理

每種元件只負責它能提供的最強保證：

| 責任 | 正式元件 | 原因 |
| --- | --- | --- |
| 不可變事實資料 | Parquet | 欄式投影、predicate pushdown、跨語言、可壓縮 |
| 下載／續傳交易狀態 | SQLite WAL | 單機 ACID、可原子 claim、低維運成本 |
| 跨檔 SQL、join、壓實與 query view | DuckDB | 直接查 Parquet，無須另複製進資料庫 |
| canonical ETL／panel | Polars Lazy；超大輸出用 streaming sink | 查詢最佳化與 bounded-memory 執行 |
| schema、metadata 與獨立驗證 | PyArrow | Parquet 原生 schema／row-group 契約 |
| 密集訓練輸入 | 可重建 `.npy` mmap cache | GPU 訓練需要連續陣列，但它不是事實來源 |

DuckDB database 只保存 catalog/view，不保存唯一一份市場事實；Polars／Arrow
也不是下載狀態資料庫。這避免同一份資料同時出現多個互相矛盾的真相。

## 分層

- `L0`：provider／task 原始 shard。不可變、可續傳、可追溯，壓實後也不刪除。
- `L1`：有界批次的 append-only compact segment。供日常查詢，允許下載尚未完成。
- `L2`：整個 endpoint 完成且 audit 通過後的最終 release，可重新分區或排序。
- dense training cache：從 L1/L2 決定性重建，不納入不可變同步發布。

機器可讀契約在 `configs/columnar_storage.json`。目前 OpenBB 已實作 L0→L1；
台股分鐘日分區本來已接近正確大小，所以保留每日 partition。Tick／五檔與大型
交易所 archive 只應在交易日或年月 partition 關閉後壓實，不能改寫仍在串流的檔案。

## OpenBB 增量 L1

手動執行一個有界批次：

```bash
./scripts/run_openbb_l1_compaction.sh
```

預設只在同 endpoint 累積至少 128 個未壓實 shard 時輸出 segment；若要在完整
稽核或測試時收尾，可加 `--include-tail`。每個 segment 執行以下證明後才發布：

1. SQLite task 必須為 active plan 的 `success`。
2. PyArrow metadata rows 必須等於 manifest rows。
3. DuckDB `union_by_name` 寫出 Zstd level 3 Parquet。
4. PyArrow、Polars streaming 與 DuckDB row count 三方相等。
5. 保存來源 path／rows／bytes／mtime／task revision、schema fingerprint 與日期範圍。
6. fsync temporary、atomic rename，再以單一 SQLite transaction 註冊 segment 與成員。
7. 原子重建 `data_openBB/openbb_l1.duckdb` 的 endpoint views。

L1 明確使用 `preserve_insertion_order=false`：task shard 的檔案順序沒有市場語意，
強制保序會讓數千小檔的平行 `COPY` 建立巨大 reorder buffer。需要時間／主鍵排序的
consumer 必須在 SQL 查詢或 L2 release 明確 `ORDER BY`，不能依賴 Parquet 寫入順序。

深度稽核：

```bash
./scripts/run_openbb_l1_compaction.sh --audit-only
```

它會重新檢查每個 L0 成員、每個 L1 segment 與 endpoint DuckDB view。若來源契約
更新、segment 遺失、row count 或 schema 漂移，正常 compaction 會把舊衍生檔移到
`compact_l1/_stale/`，解除成員綁定後重建。隔離檔不會被無聲刪除。

主要產物：

- `data_openBB/compact_l1/<endpoint>/segments/*.parquet`
- `data_openBB/openbb_l1.duckdb`
- `data_openBB/catalog/l1_compaction_status.parquet`
- `data_openBB/catalog/l1_compaction_audit.parquet`
- `data_openBB/_state/l1_compaction_latest.json`

安裝低優先序、每半小時執行的 timer：

```bash
./scripts/install_openbb_l1_compaction_service.sh
```

每半小時最多讀 20,000 個新 shard；單 segment 最多 2,000 個 shard、256 MiB
Parquet 檔案 bytes、metadata 估算 512 MiB 未壓縮 bytes 或 10M rows，先到者先切段。
DuckDB memory limit 4 GiB、systemd `MemoryHigh` 6 GiB／hard cap 8 GiB。獨佔 lock 防止人工
與 timer 重疊；這個限制不會降低下載器各 provider 的獨立官方 RPS。

## 壓縮與檔案大小

- 熱資料：Zstd level 3，優先吞吐量。
- 冷 release：在確認不再頻繁重寫後可用 Zstd level 6。
- 互動查詢檔案目標：64–256 MiB；冷檔上限約 512 MiB。
- row group 目標：未壓縮 64–128 MiB；無法預估時以 122,880 rows 起步再量測。

正規化不是把行情拆成大量關聯表。Tick、分鐘 K、order book 是 append-only fact；
symbol、venue、calendar、corporate action 才是維度表。把高頻 fact 做 3NF/5NF join
會增加 row key、join 與小檔成本，通常比重複少量低基數欄位更昂貴。研究查詢仍可
在 DuckDB view 中建立正規化語意，而不改寫 canonical fact layout。

## 切換與刪除門檻

目前 L1 是 shadow read layer。只有同時滿足下列條件才能讓正式 consumer 切換：

1. endpoint source coverage 與 L1 member coverage 一致；
2. 深度 audit 為零失敗；
3. consumer projection／排序／日期語意測試通過；
4. 真實 full-universe benchmark 證明沒有性能退化；
5. 回滾只需切回 L0/L2 view，且不移除來源。

即使切換完成也不自動刪除 L0。刪除必須是另一個明確、可恢復、有 retention 與
checksum release 證據的操作。
