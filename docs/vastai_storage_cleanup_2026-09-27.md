# vastai1T 安全空間回收紀錄（2026-09-27）

## 第三輪：markets／ablations 盤點與有限去重

06:58 TST 另釋放 **10,558,328,832 bytes（10.56 GB）**：
只把 41 個已完成 run 中 279 份完全相同的回測 NPZ 副本改為硬連結，
保留所有原始路徑、模型與唯一資料；沒有刪除整份成果或冷庫物件。
本日三輪收據累計 **138,455,556,096 bytes（138.46 GB）**。
07:04:42 TST 的可用空間為 **161.08 GB**，不是累計回收量。

`artifacts/markets` 約 166.50 GB、`artifacts/ablations` 約 119.65 GB。
其餘舊成果欠缺完整恢復／使用租期證明，維持保留。
逐項審查、測試、來源與冷庫驗證、未清理原因，見
[本輪完整報告](vastai_artifact_cleanup_2026-09-27.md)。

## 第二輪：保留所有路徑的精確快取去重

2026-09-27 06:31 TST 完成第二輪，**另釋放 61,239,255,040 bytes
（61.24 GB／57.03 GiB）**；沒有刪除任何唯一資料、冷庫物件或邏輯檔案路徑。
加上第一輪 66.66 GB，兩輪收據合計 **127,897,227,264 bytes（127.90 GB）**。
這是移除實體重複副本的累計量，不是磁碟可用空間的淨增量。

06:32:24 TST 的最後檢查：Vast 可用 **154,293,997,568 bytes（154.29 GB）**，
使用率約 85.63%；Bybit 訓練仍在執行，期間也持續寫入，所以容量會變動。

| 區域 | 清理前配置空間 | 清理後配置空間 |
|---|---:|---:|
| `artifacts/cache/tw_minute_paper_parity_09bedd96a2f68c39` | 143.47 GB | 82.23 GB |
| 整個 `artifacts/cache` | 291.80 GB | 230.56 GB |

本輪只把上述分鐘快取裡 **6,235 組、8,786 份** 完全相同的 `.npz`／`.npy`
實體副本改為硬連結。同樣的內容共用一份 inode，但所有原始檔名、路徑、
manifest、READY、驗證收據，以及不相同的內容都還在；使用者不需要重新下載或解壓。
排除較新檔案、非 allowlist 內容及 metadata。這不是刪除整份 cache，也不是
把不同版本視為相同資料。

安全條件與證據：

1. 先以既有 `deduplicate_artifacts.py` 執行只讀 audit；候選須至少七日未修改，
   完整 SHA-256 相等，hash 過程零變動。mtime／名稱／大小本身不是相同內容證明。
2. 在 15 個 cache generation 的既有 writer locks 下，核對各自 READY 的
   manifest SHA-256，以及 manifest 對每個候選的內容 SHA-256／bytes。
3. 排除 fd／mmap／cwd 等程序引用，並解析正在執行的訓練 config；
   Bybit 使用另一組快取，不在本次目標內。途中持續重查。
4. 透過既有 `apply_duplicate_groups` 逐檔重新雜湊後原子替換；
   每組替換後再核對 canonical SHA-256 與所有副本 inode。
   實測目標目錄的 `du` 減量與逐筆配置空間合計完全相符。
5. 最後重查 8,786 個路徑及 15 組 manifest／READY，零差異／零錯誤。
   本機測試 14 passed；Vast 依其既有版本的測試組合為 13 passed。
6. Syncthing `stockagent-packed` 為 idle，本機與 peer need bytes／items／deletes
   皆為 0，completion 100%、remoteState valid、folder／system／pull／watch 錯誤皆為 0；
   實測 QUIC、TLS 1.3。Vast 仍是 index-only，不代表它持有整份冷庫 payload。
7. 原有 RUNNING supervisor 服務及 PID 未變。Bybit 主訓練仍在更新收據，
   最後觀測為 fold 2、epoch 119、validation、error null；沒有停止或重啟它。
8. penguin D guard 通過；沿用當日第一輪的完整雜湊／ZIP CRC 證明，重新核對
   七個精確 release 的 manifest 與 3,461 個去重後物件識別欄位，零差異。
   這不是全冷庫歷史完整性宣告；本輪對 D 冷庫零刪除。

這種硬連結只適用於已確認的不可變 cache payload：現有 writer 使用
「新暫存檔 → 原子 replace」，讀取使用唯讀或 copy-on-write mmap。
不得用原地 `r+`／`wb` 修改共用 inode。舊 run-verification 的 inode 指紋可能失效，
下一次使用會依既有機制重新驗證；沒有手動改寫驗證收據來跳過檢查。

本輪不動使用中的 Bybit 資料／編譯快取、釘選 materialization、唯一訓練成果、
V5 ingress 來源，以及尚未取得完整冷庫恢復證明的舊 quarantine。
沒有更改七日租期、pin、source eviction、自動 GC 或冷庫權威設定。

第二輪完整收據在兩台機器的 `artifacts/operations/vastai_cleanup_20260927_round2/`：
`physical-dedup-plan.json`、`physical-dedup-apply.json`、`post-runtime.json`、
`post-cold-preservation.json`、`summary.json`、測試結果與原始 audit。
同目錄的 `dedup_physical_cache.py` 是固定此精確範圍的一次性 coordinator，
不是一般 cache 刪除器；已有 apply 收據時拒絕重跑。

## 第一輪：冷庫已驗證的本機副本回收

本次依使用者「已進冷庫、沒有在用的本機資料可以清除」的指示，
在 vastai1T 執行一次性提前回收。逐項收據合計釋放
**66,657,972,224 bytes（66.66 GB／62.08 GiB）**。
2026-09-27 02:13:31 TST 的最後系統檢查，可用空間為
**99,977,383,936 bytes**，使用率約 90.69%。容量是當時觀測，會隨其他工作變動。

此處 GB 使用十進位。回收量依實際移除檔案的配置空間計算；
期間同時有資料檢查、訓練與其他工作寫入，不能把整段時間的 `df` 差額當作回收量。

## 已移除的本機副本

| 範圍 | 實際回收 bytes | 約 GB | 收據 |
|---|---:|---:|---|
| Bybit 9/25 舊解壓版本 | 22,357,229,568 | 22.36 | `retire-bybit-apply.json` |
| tw-public 9/21 舊解壓版本 | 15,280,787,456 | 15.28 | `retire-tw-public-apply.json` |
| all-observed research v3 解壓快取 | 1,499,283,456 | 1.50 | `retire-tw-public-research-all-observed-2014-v3-apply.json` |
| 舊 LayerNorm v12 完整訓練成果 | 1,251,397,632 | 1.25 | `artifact-layernorm-apply.json` |
| 舊 attention v12 完整訓練成果 | 1,249,607,680 | 1.25 | `artifact-attention-apply.json` |
| 同一 v12 OFAT 的 baseline 完整訓練成果 | 1,063,387,136 | 1.06 | `artifact-baseline-apply.json` |
| 已解壓的 Bybit 9/26 重複傳輸物件，629 個 blob／pack | 23,956,279,296 | 23.96 | `bybit-payload-apply.json` |

資料收據位於兩台機器的
`artifacts/operations/vastai_cleanup_20260927/`；penguin 另保存
`summary.json`、`cold/`、`post-cold-verification.json` 與遠端證據副本
`remote-evidence/`。每筆收據含精確 release ID、manifest SHA-256、
原始位置、驗證結果與回收量。

三份 artifact 的原始相對路徑（基準為 `/root/stockAgent/artifacts`）：

- `ablations/tw_day_trade_hybrid_minute_v12_reference_architecture_checkpoint_finetune_ofat_v2/layernorm`
- `markets/tw_day_trade_hybrid_minute_v12_attention_full_then_last_layernorm_commission20_capital10m_all_features_v1`
- `ablations/tw_day_trade_hybrid_minute_v12_reference_architecture_checkpoint_finetune_ofat_v2/baseline`

## 驗證與執行方式

1. 確認 penguin 的受保護 D 主冷庫掛載、manifest 與 inventory SHA-256。
2. 驗證七個精確 release 的完整物件集合。只有七日內的 SHA-256 讀回收據，
   且 D volume guard 與 inode／size／mtime／ctime 識別欄位仍符合，才能重用；
   其餘物件重新計算 SHA-256。所有 ZIP pack 另檢查成員列表及 CRC。
3. Vast 的解壓資料與 artifact 逐檔對照 inventory，驗證完整目錄指紋。
   清理前重查 pin、程序 fd／maps／cwd、正在執行的訓練設定及 Syncthing 收斂。
4. 管理中的解壓版使用既有 `manage_packed_edge.py evict`，僅提前越過租期；
   pin、READY、manifest 與使用中檢查保持有效。
5. 三份舊 artifact 均為完整、超過七天未修改的已發布成果；確認無硬連結、
   舊 hot mirror、使用中引用及被保留的舊 Syncthing folder，先寫本機 tombstone，
   原子移入隔離目錄，再次逐檔驗證後移除。保留既有格式的 cold-only 狀態收據。
6. Bybit 傳輸物件只取消當次 Bybit 的 `retained_payloads` 項目。
   先安裝忽略規則、要求 Syncthing 掃描並等待收斂，再透過既有 local-only prune
   移除已逐個雜湊驗證的 629 個物件。保留新收到的 `free-public-context` 物件、
   所有 heads／manifests／inventories、現用 Bybit 解壓版及 pin。
7. 清理後重查七個 release 的 manifest／inventory SHA-256 及
   **3,461 個去重後物件的識別欄位**，零差異、零缺失。

此驗證只涵蓋本次七個 release，不能外推所有歷史 release 均可重建。
penguin D 冷庫沒有刪除任何物件。Vast 仍為 index-only compute node，
其容器資料也未被重新宣告成持久備份。

一次性操作程式存於同一 operations 目錄，使用既有 packed、lease、
process-reference、Syncthing scan 與 pruning 元件；沒有修改訓練策略、
下載來源或自動七日回收規則。這些程式綁定此次精確目標與限時證據，
不是日常維護入口，不應直接拿來清理其他路徑。

## 清理後狀態

- `stockagent-packed`：`idle`，本機 need bytes／items／deletes 全零。
- penguin 完成度 100%、`remoteState=valid`；folder／system／pull／watch 錯誤全零。
- 實測連線為 QUIC，`TLS1.3-TLS_AES_128_GCM_SHA256`。
- Vast payload 僅剩新保留的 `free-public-context`，69 個物件、約 0.877 GB。
  含 metadata 的 `/srv/stockagent-packed` 約 1.81 GB。
- 現用 `data_bybit` 仍指向 9/26 的已釘選解壓版本，資料夾存在。
- Syncthing、Caddy、cron、instance portal、TensorBoard、tunnel manager 仍為 RUNNING，
  PID 與本次開始時相同；本次没有重啟或終止遠端工作。

## 保留的資料與原因

| 區域 | 本次後觀測／處理 |
|---|---|
| `artifacts/cache` | 約 291.8 GB；本次有持續寫入，未取得整棵逐檔冷庫恢復證明 |
| `artifacts/markets`、`artifacts/ablations` | 約 170.9／125.6 GB；其餘成果未通過同一完整冷庫與使用狀態檢核 |
| `artifacts/smoke` | 約 34.35 GB；名稱不能證明內容無用或已保存 |
| 管理中的 materialized tree | 約 96.52 GB，含新工作資料、釘選版本、近期工作引用版本與隔離證據 |
| 舊 quarantine | 約 28.2 GB；對應歷史 release 未取得完整恢復證明，保留 |
| 舊 repo 與 `data_yahoo`／`data_okx` 等來源 | 不能由舊 migration-core 的路徑與大小證明是相同內容，保留 |
| V5 artifact ingress 來源 | 遵守既有來源保護約定，保留兩側來源與現用部署證據 |
| 編譯快取 | 執行中曾因訓練／編譯程序延後；最後一次正式 dry-run 的 `eligible_files=0`，未刪除 |

## 自己取回資料

在 Vast 的 repo 根目錄使用正常的明確啟用入口；它會取得精確冷物件、驗證後解壓，
並建立管理中的七日租期。不要把資料直接寫入 packed 或 materialized tree。

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
stockagent-data status
stockagent-data gc --dry-run
```

例如，從本次收據取回舊 LayerNorm 成果到原路徑：

```bash
cleanup_receipt=artifacts/operations/vastai_cleanup_20260927/artifact-layernorm-apply.json
cleanup_dataset=$(jq -er '.dataset' "$cleanup_receipt")
cleanup_release=$(jq -er '.snapshot_id' "$cleanup_receipt")
cleanup_original=$(jq -er '.source' "$cleanup_receipt")
stockagent-data use "$cleanup_dataset" \
  --snapshot-id "$cleanup_release" \
  --link "$cleanup_original"
```

回復結果為指向受管理不可變資料的連結。需要寫入、續訓時，使用另外的 writable
workspace；若需長期保留部署資料，使用 pin。短工作在開工前先 `use`，
以免完全落在五分鐘使用監控的兩次掃描之間。

恢復舊 dataset 時，以 `retire-*-apply.json` 的 `dataset` 與 `snapshot_id`
傳入 `stockagent-data use DATASET --snapshot-id RELEASE`。這會選擇本機管理中的
current 版本，應在新工作開始前完成並鎖定精確 release，避免更動進行中工作的選版。
預設不加 `--retain-payload`，以免解壓後又常態保留一份完整傳輸副本。
