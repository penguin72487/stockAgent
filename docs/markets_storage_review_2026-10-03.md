# Penguin markets 容量、用途與冷保存審查

範圍：`/root/stockAgent/artifacts/markets`。容量清冊於 2026-10-03 08:48 台北時間完成；
這是持續運作中的檔案系統觀測，不是凍結整機或新增全量副本。
本次只讀來源、模型、服務與 D 冷庫，寫入清冊／驗證收據，**沒有新增刪除、發布、解壓或重啟服務**。

## 結論與容量定義

- 27 個目錄／連結，普通檔案在此範圍內以 inode 去重後配置 **351.218 GB**。
- 最大的 `tw_futures_v8_margin_preparation` 是 **310.144 GB、6,508,253 檔**，約佔 88.3%。
- 該準備區的 **117 個 `all_products_rule_facts_native_*` 版本約 261.105 GB**。
  它們包含官方原件、文字與解析證據，不是能整批丟棄的訓練 cache。
- 未見當前 Discord／overnight／程序引用、排除正在變動準備區及小型 guard 後，
  18 個待審目錄配置量合計約 **34.420 GB**，其中 `tw` 2.462 GB 因事故恢復證據明確保留，
  其餘 17 個一般封存候選約 **31.958 GB**。這是候選上限，**不是已核准或保證可釋放量**。
- 不能宣稱目前所有目錄都已完整冷保存：有未登錄的目錄、只封存小檔的版本，
  以及已封存後再新增／改寫的報表與回測檔。

本文 GB = 1,000,000,000 bytes。主表容量是 `st_blocks * 512` 的普通檔案配置量，
不是檔案大小總和；不包含目錄本身區塊。跨目錄及舊 transport 有 hard link，
各目錄數字不一定可相加，刪一個名稱也不一定釋放其 payload。
兩個 managed symlink 的資料在 `/srv/stockagent-packed-materialized`，另列，
不能用 symlink 本身 0 bytes 判斷資料不存在。
09:29 核對兩個連結的外部 cache，Multi-Basis 22 約 **3.645 GB**、v8 約 **2.647 GB**，
合計約 **6.293 GB**，不在上述 markets 路徑內的 351.218 GB 之中。
09:35 WSL 可用 **161.349 GB**；D 可用約 **2.473 TB**，這些是全 volume 觀測，不是本輪釋放量。

完整精確路徑與數值見 [27 項容量清冊](../artifacts/operations/market_artifact_inventory_20261003/roots.csv)；
冷覆蓋與保留分類見 [分類清冊](../artifacts/operations/market_artifact_inventory_20261003/classified_roots.csv)。

## 容量排名與處置

下表對長名稱使用略稱；實際操作目標一律以清冊完整路徑為準，不能用略稱、萬用字元刪除。

| 目錄／略稱 | 配置 GB | 檔案數 | 用途與處置 |
| --- | ---: | ---: | --- |
| `tw_futures_v8_margin_preparation` | 310.144 | 6,508,253 | 官方來源、解析版本、OCR、逐批修復證據；觀測期間有新寫入，不得整根回收 |
| `tw_public_lantent` | 10.623 | 12,236 | 舊 fold／模型／解釋性研究產物；未見當前服務引用，先完整封存 |
| `forex` | 4.262 | 842 | 舊外匯 fold／訓練成果；未見當前服務引用，先完整封存 |
| `…online_complete_lookback32_v2` | 3.814 | 5,313 | 舊台股研究版本；先核對唯一 checkpoint 與冷保存 |
| `…feature_input_lookback32_v4` | 3.702 | 3,396 | 既有冷版只收小檔；不能回收整個 run |
| `tw_day_trade_100m` | 2.632 | 465 | 當沖／overnight 明確服務依賴，保留 |
| `tw_public_candles_tw_day_trade_select` | 2.504 | 451 | 舊研究模型；未見當前服務引用，先完整封存 |
| `tw` | 2.462 | 1,464 | 此列是 08:48 觀測；後續內容再變動，冷覆蓋以逐檔驗證時間為準；舊 mirror 差異仍保留 |
| `tw_public_all` | 2.432 | 995 | 舊 fold 與研究成果，先完整封存 |
| `tw_day_trade_10m` | 2.427 | 458 | 舊 fold 與模型；未見當前服務引用，先完整封存 |
| `…raw_feature_input_lookback32_v5` | 2.290 | 480 | Multi-Basis 當沖／overnight 模型依賴，保留 |
| `…projection_l1_gelu…capital10m_v1` | 1.763 | 473 | GELU overnight 模型依賴，保留；已封存舊版不等於最新報表已封存 |
| `tw_index_futures_day_canonical` | 1.001 | 481 | 舊期貨 fold／模型，先完整封存 |
| `…v12_attention_full_then_last_layernorm…` | 0.845 | 495 | 已有冷 run，但新增／改寫內容不全在該版中，禁止直接回收 |

其餘項目詳見清冊。三個 Bybit retirement guard 各僅一個約 1 KB 的檔案，
其配置區塊各 4 KB；保留恢復／防誤寫的 guard，清除它們沒有實質省空間效益。
「未見當前引用」只是一個採樣與已實作消費者的範圍，不代表未來不會使用，
更不代表獨有模型可由現在的來源與程式完全重建。

## 最大準備區的組成

`du -x -B1 --max-depth=1` 的整個準備區為 **311.074 GB**，包含目錄區塊。
下表由同一次遍歷分組；共享 inode 只歸屬第一次遇到的子目錄，所以不是
獨立逐目錄刪除量或各版本的完整邏輯大小。

| 類別 | 子目錄數 | 遍歷歸屬配置 GB | 保留判斷 |
| --- | ---: | ---: | --- |
| `all_products_rule_facts_native_*` | 117 | 261.105 | 優先審查歷史版本，保留原件／SHA／時鐘／解析 ABI；未取得完整冷覆蓋 |
| 其它 preparation／來源／驗收版本 | 391 | 40.582 | 需逐項追依賴，不能由日期或名字認定可重建 |
| `remaining_gap_repair*` | 3 | 8.850 | 目前修復工作仍引用，保留 |
| `.rule-source*` 與 `ocr_rapid*` | 4 | 0.521 | 本機 immutable 原件物件與 OCR 模型；不是 D 冷發布證明，模型仍被 OCR config 引用 |

最大的單一修復子目錄是 `remaining_gap_repair_v3_20260930`，7.702 GB；
`all_products_rule_facts_native_v120_20261001` 歸屬 4.475 GB，
v118／v117／v116／v115 各約 4.41–4.45 GB。
v120 的 manifest 列出 **54,152 筆來源引用**，含 TAIFEX 原始 `.body.gz`、文字與 JSON。
不能把來源證據當成無用中間檔；原件有其它冷 source release 的可能性，也必須逐檔取得證明，
不能從同名 dataset 或公開網站仍可連線推論。

現行修復流程仍會依 SHA 鏈重用舊解析／review／增量 parent。
因此「只留最大的最新版」不是安全策略；先解出實際依賴閉包，再將不用的版本
透過既有 content-addressed 冷發布保存，才回收驗證過的熱名稱。

證據：[完整子目錄容量 log](../artifacts/operations/agent-workflow/runs/margin-preparation-size-20261003T004729-5e464939/run.log)、
[當前逐批修復工作](tw_futures_remaining_gap_execution_plan_2026-10-03.md)。

## 冷保存驗證：舊版可還原與目前全部覆蓋分開

對剩餘已知 markets 冷 release 使用既有 manifest／inventory resolver，
核對每個 cold object SHA-256／ZIP，再獨立解碼 inventory 中每個檔案的 SHA-256。
之後逐檔比較現在的熱來源（大小、SHA、permissions），且重查當前 signature，
沒有把資料解出到新熱目錄。範圍是下表選定的 release，不是整個 D 所有歷史 release。

| 項目 | 選定冷版 | 現在熱內容完整覆蓋 | 未覆蓋目前檔案／邏輯 bytes |
| --- | --- | --- | --- |
| v12 attention／LayerNorm full run | 通過全物件／493 檔獨立解碼 | 否 | 52 檔、19,944,089 bytes |
| v8 annual-log-cash managed run | 通過全物件／420 檔獨立解碼 | 否 | 24 檔、61,706,020 bytes |
| GELU overnight run | 通過全物件／501 檔獨立解碼 | 否 | 53 檔、150,939,743 bytes |
| Multi-Basis 22 managed run | 通過全物件／525 檔獨立解碼 | 否 | 39 檔、117,661,394 bytes |
| feature-input v4 small | 通過全物件／3,276 檔獨立解碼；原版明示略過 72 個大檔 | 否，不能整根回收 | 122 檔、4,778,255,774 bytes |
| `tw` legacy archive | 通過全物件／1,452 encoded 檔、1,451 原件獨立解碼 | 否；另有舊 mirror 差異 | 2 檔、596,920 bytes |

v4 的 8 MB 收錄上限不是壓縮後完整資料：冷 manifest 原本只收 3,276 小檔，
明示略過 72 個大檔／4.742 GB，不能用小檔 head 存在稱為整個模型 run 已備份。
各項現在的差異路徑與原因保存在 `.cold-coverage.json`／`extra_cold_coverage.json`。
六個選定冷 release 都通過；零個完整覆蓋其對應的目前熱普通檔。
逐檔 source 比對在各自期間 signature 未變，但不是跨整輪凍結。
例如 `tw` 由容量採樣時 1,464 檔變成冷覆蓋核對時 1,453 檔；
不能把不同時間的容量／檔數硬當成同一來源版本。

`tw` 的兩個未涵蓋檔不是可隨意丟棄的 cache：
`.INCIDENT_RECOVERY_PARTIAL.json`（9,080 bytes）及
`fold_04/checkpoint_best.unresumable_without_optimizer_state.1790961780215340990.pt`
（587,840 bytes）是事故恢復／無 optimizer checkpoint 的保留證據。
此目錄明確標成 incident hold，不列入普通回收候選；byte archive 可還原也不等於訓練可 resume。

兩個 managed run 的熱報表已與固定 cold release 不同。它們是現行服務依賴，
本輪保留、不 GC、不把舊 `READY` 或 head 當成最新內容證明。
後續應將可寫報表／回測放在獨立工作區，模型物件保持 immutable，
並以既有增量出版流程保存新增內容；不能就地改寫既有 cold object。

缺少完整目錄冷 release 的其餘項目，標為「尚未取得完整冷恢復證明」，
不推論所有 bytes 在 D 都不存在，也不因來源可重新下載而丟棄歷史修訂。

## 上一批實際回收已完成

上一批明確允許單次不等 lease 七日的 27 項已於 2026-10-02 18:53 完成，
釋出 **143,124,492,288 allocated file bytes**。每項刪除後全 cold／decode 複驗通過，
源與舊 mirror 名稱都已退役。這次查核也會重看該批固定 manifest、inventory 與
物件存在／大小，27 項全部通過且兩個熱名稱仍不存在；
不把它宣稱成今天重讀 27 項全部 payload checksum。

這批沒有刪 D 冷物件，不包含較早的 15.71／115 GB 清理數字，
也不代表 Windows VHDX 已壓縮。完整收據見
[上一批 summary](../artifacts/operations/market_artifact_manual_retirement_20261002/summary.json)。

## 下一步的安全順序

1. 優先解出期貨準備區的來源／parent／repair 依賴，針對歷史 native 版本作冷封存與 exact-hash 去重審查。
2. 封存約 31.96 GB 的 17 個一般非服務候選（先 `tw_public_lantent`、forex、lookback32 v2），
   保留唯一 checkpoint、完整成果、來源與回復 recipe；沒有冷恢復證明就不刪。
3. 補齊 v4 大檔，以及 v12／GELU／managed runs 的新增與改寫內容，保留每個舊 release。
4. 回收前重新核對完整 D 恢復、來源穩定性、服務／程序、pin／lease、舊 mirror、
   D mount 與適用 Syncthing gates，只用 canonical retirement dry run／fingerprint／apply。

這是後續處理清單，不是本輪已執行封存、可重建承諾或刪除許可。
自動七日規則未變；手動 immediate 只略過 use lease 年齡，不能略過來源或冷驗證。
D 仍是已接受風險的本機單份冷庫，不是獨立災難備份。

09:27 的本機 Syncthing 觀測為 need bytes／items／deletes 0、各項錯誤 0，
但 folder 尚非 idle（仍在掃描），所以本機 retirement transport gate 為 false。
此政策沒有要求 remote peer；這不是 vastai1T 同步／資料可用性的驗收。

## 自助核對與收據

```bash
cd /root/stockAgent
source scripts/runtime_env.sh

# 只讀 mount guard；不是 remount／migration。
bash scripts/mount_packed_d_cold.sh --check

# 原工作、下一步與已完成證據。
stockagent-agent task show markets-storage-audit-20261003

# 之後重新盤點請改用新的 output-dir，保留本次收據。
run_fintech_python scripts/audit_market_artifact_cleanup.py \
  --output-dir artifacts/operations/market_artifact_inventory_NEW_AUDIT
```

- `inventory.json`／`roots.csv`：27 roots、file／inode 容量、觀測引用；不含冷 checksum 證明。
- `cold_coverage.json`：選定冷版全物件／獨立解碼、目前 source 差異及上一批 manifest／存在性重查。
- `extra_cold_coverage.json`：未在當前 cold-artifact registry 的既有 GELU release；沒有新增登錄或退役授權。
- `classified_inventory.json`／`classified_roots.csv`：用途、保留原因、差異量、managed target 容量與冷覆蓋分類。
- `artifacts/operations/agent-workflow/`：監督程序、原始 argv、時間、exit code 與 log；不以 process active 代替 checksum。

所有操作遵守 [儲存契約](agents/storage.md)；本次沒有把冷 release 驗證稱成模型訓練、
服務執行正確性或遠端機器已收齊資料的證明。
