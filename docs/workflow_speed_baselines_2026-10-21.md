# StockAgent 工作流測速基準：2026-10-21

同日後續的六模式、大型歷史、瀏覽器、D 磁碟與同值優化實測見
[工作流補測與優化](workflow_speed_optimization_2026-10-21.md)。本文件保留前次基線觀察。

本次建立跨工作流的入口清單、速度樣本、原始收據及讀回驗證。產物位於
`artifacts/benchmarks/workflow_speed_20261021/`，主索引為 `collection_v3/collection.json`。
入口盤點、既有紀錄、當日實測與未測範圍各自保留。這是加速分析的基線；
來源端點、實驗設定或服務被列入清單，不表示已逐一執行完整測速。

主索引保留 370,267 筆計時欄位、22,413 個依情境區分的彙總群組，來自
1,072 份訓練計時檔及下載、服務、HTTP、推論、儲存、工程命令收據。
清單包含 1,028 個靜態程式入口候選、2,399 個來源端點、17 個訓練模式、50 個 dataset、
195 個 market configs、82 個 deployment configs、164 個 service templates、
盤點時的 95 個服務觀察、70 個 timers、3 個 path triggers 與 4 個登錄節點。
其中 10 個來源已有 registry ID 可精確對應的階段計時；尚未把任何來源宣告為
完整端到端測速已覆蓋。計時欄位、巢狀區間與不同路徑的收據副本不是獨立執行次數。

維護入口為 [`profile_workflow_speeds.py`](../scripts/profile_workflow_speeds.py)，
命令監督與後續計時沿用 [Agent workflow](agent_workflow.md)。
前次語言比較的資料、輸出與語義邊界見
[語言工作流測速](language_workflow_benchmarks_2026-10-21.md)。

## 第一性原理與量測單位

要優化的是同一份工作的完成時間。先定義輸入、工作量、完成條件和輸出，再量測
從開始到完成條件成立的 wall time。CPU time、GPU kernel time、網路等待、磁碟
等待及排隊時間用來解釋它；平行或巢狀區間不能相加冒充總時間。

工作量依責任入口採用 requests、rows、bytes、symbols、batches、epochs、folds
或完整快照。只有確定單位及分母的紀錄才計算吞吐量。空結果、失敗、等待來源更新、
首次呼叫與暖機後樣本分開。程序冷不代表 OS page cache、持久 panel 或遠端來源冷。

相同資料與語義下，若可加速部分占完整時間的比例為 p、該部分加速 s 倍，完整加速
上限為 `1 / ((1-p) + p/s)`。例如只有 20% 能加速，即使該部分快十倍，整體約快
1.22 倍。因此 C++、Rust、向量化或 GPU 的候選應從已辨認的熱點開始，再重測完整
工作；下載、PIT 檢查、驗證、test、曲線、繪圖、checkpoint 與耐久寫入仍屬完成條件。

## 責任入口與目前覆蓋

| 工作流 | 既有責任入口與完成邊界 | 本次保留的速度證據 | 尚未建立的證明 |
| --- | --- | --- | --- |
| 資料取得 | registered daily/intraday/features/backfill、FinMind、TEJ、官方來源、新聞及串流 owners | registered steps、FinMind query-only telemetry、TEJ 完成任務、服務嘗試、離線 HTTP 到 Parquet 完整回放 | 各來源端點逐一對應的完整下載時間、外網速度與全歷史完成 |
| 清洗與資料準備 | canonical normalization、reference/eligibility、panel、feature、split | feature/materialization steps、training startup/pre-epoch 原始檔、既有 benchmark 收據索引 | 每種 panel/config 的當日來源冷重建及獨立完整工作計時 |
| 訓練與評估 | canonical trainer/runner、DDP max-rank、validation/test、曲線、繪圖、checkpoint | epoch/workflow/startup/save/plot/explainability 原始計時；前兩個 epoch 與 epoch 3+ 分組 | 所有模式在所有 GPU 上重新執行完整 fold；工程速度不代替收斂或策略驗收 |
| 推論 | canonical signal engine、checkpoint universe 對齊、完整訊號格式化 | 可執行模式的程序首次呼叫與暖機後樣本、階段、輸出 SHA、GPU/RSS | 真實開盤取價、行情到達、broker 下單與成交延遲 |
| 紙上執行與回測 | canonical decision/fees/lots/ledger、replay、paper persistence | 完整狀態發布回放及既有 ledger/replay 收據索引 | 所有策略當日完整 replay；持久化耗時不代替交易事件全鏈 |
| 儲存與版本 | packed publication/verify/fetch/restore、Temporal lifecycle/replication/dataset versions | 私有 ext4 完整新舊版本往返；三條 pinned Temporal 歷史及活動區間 | D/NAS 全容量吞吐量、所有冷資料恢復；NAS check 不等於完成備份 |
| 服務與呈現 | systemd、owner/public HTTP、canonical dashboards | 每個登錄服務的 coverage、CPU/IO 觀察、完成嘗試、HTTP header/body/parse 階段 | 瀏覽器載入/繪圖、Windows 冷啟動、長期延遲分位數 |
| 工程與跨節點 | agent workflow、四節點已登錄角色、各自 runtime | 本機命令 wall/CPU/IO，完整命令結果與靜態節點角色 | 遠端各主機即時速度與完整跨節點交付；本機快照不能代替遠端讀回 |

完整清單見 `collection_v3/coverage.csv`／`inventory.json`。每列有 kind、owner 或
入口、目前計時覆蓋及下一個應量測的邊界。不同粒度的清單不能相加計算完成百分比。
架構盤點保留兩個歷史 deployment config 的缺失繼承路徑，沒有改寫其實驗設定。

## 當日實測與既有觀察

下表使用各 producer 的原始計時邊界，不把不同資料、日期、快取、硬體或變體混成
一個平均值。樣本數小的 HTTP 與推論測量以中位數和原始樣本留存，未宣稱長期 p95。

| 流程與原始收據 | 速度與工作量 | 適用範圍 |
| --- | --- | --- |
| `live_inference.json`，Multi-Basis | 首次 7.083 秒；暖機後中位數 315.450 ms，3 次；32 × 2,745 × 99 輸入 | 2026-10-20 panel；完整離線訊號，輸出一致；disk panel 已存在 |
| `inference_tw_day_trade_100m.json` | 首次 6.401 秒；暖機後中位數 278.020 ms，3 次；2,744 symbols | 同一離線推論邊界，保留實際 threads 與 checkpoint 身分 |
| `inference_tw_day_trade_multi_basis_22.json` | 首次 6.235 秒；暖機後中位數 372.165 ms，3 次；32 × 2,746 × 24 輸入 | 來源更新完成後沿用 canonical guard 重測成功 |
| `inference_tw_day_trade_v8_annual_log_cash.json` | 首次 7.518 秒；暖機後中位數 500.911 ms，3 次；32 × 2,754 × 99 輸入 | 同一完整離線訊號邊界，保留每次 GPU/RSS 及輸出 SHA |
| `inference_factorized_capacity.json`；第五模式 | 當日未重新執行：可用 GPU 9,219,080,192 bytes，舊峰值 reserved 11,058,282,496 bytes，加 1 GiB 預留需 12,132,024,320 bytes | 不釋放正式 live cache；2026-10-13 舊收據暖機中位數 571.186 ms 另留為歷史觀察，不能當成當日速度 |
| `paper_publication.json` | indented 895.068 ms、compact 502.732 ms，各 4 次；來源狀態 5,837,887 bytes | `_persist` 到 durable publication；保留 file/directory fsync；四份公開輸出值與 monitor 綁定另行完整驗證 |
| `offline_candles/result.json` | stdlib 2.119 秒、pooled 1.321 秒中位數；80 pages、39,920 rows、4 workers | 本機固定 HTTP fixture → normalize/validate → durable Parquet → SHA/完整讀回，ABBA；fixture 含連線設定延遲，不代表外網/provider 吞吐量 |
| `storage_roundtrip.json` | former two-pass 3.812 秒、single-pass 3.789 秒平均值，ABBA 4 次 | 同一約 42.5 MB、2,018,074 rows 來源重複提供兩次；含 publish、完整 verify、durable fetch、新舊版 restore/readback、清理；測到 baseline/no-op，沒有製造資料變更 |
| `owner_http_full.json` | 43 routes 全部成功；暖機中位數例：minute history 348.475 ms、features 326.138 ms、TAIFEX status 215.937 ms | loopback owner :8771；每 route 首次加 3 次後續請求；包含當時一般服務負載與既有快取 |
| `tej_profile.json` | 120 個已完成空結果任務；end-to-end 中位數 19.433 秒、before-submit 16.799 秒 | 既有任務回溯，0 exported rows；未重新呼叫 TEJ，不可換算成非空資料下載速度 |
| `storage_history/storage_profile.json` | NAS check 294 次中位數 0.402 秒；source replication 17 次中位數 24.867 秒；dataset versions 8 次中位數 7,514.385 秒 | 現行 pinned runs 之 server UTC start-to-terminal；工作量不同，可能含 no-op、RPC 與重試，不代表相同 bytes 的 A/B 比較 |
| `service_baseline.json` | 95 個服務：48 有完成程序區間、37 僅 resource sample、10 未測；70 timers、3 paths | failed 嘗試單獨保留；daemon 的 CPU/memory/IO 不是操作延遲，當時沒有匹配的 service-step receipt |
| `collection_v3/finmind_telemetry_snapshot.json` | 按 owner、dataset、partition、requests 保存實際 seconds/rows/bytes | 只讀既有 SQLite 的最近 128 rows/table；校準、物件傳輸、derivation 與 verified cycle wall 分開；共用 cycle 不重複加總 |
| `collection_v3/speed_summary.csv` 的 `recorded_registered_step` | 各次 daily/backfill/features 等完整 step 原值 | 例如某次 Bybit backfill 更新 1,093 秒及 daily materialize 382 秒；是該次工作，不是固定服務延遲 |
| `recorded_training` | 各實驗的 epoch/startup/validation/test/save/plot 等原值及完整原始檔 | 保留 producer timing file、run/rank/world size/regime；歷史 producer 的硬體未從目前收集器推定 |

HTTP 的 `http_baseline.json` 是另一個 public audience。403 與兩次 timeout 原樣保存，
沒有與 owner 成功請求合併，也沒有宣稱公開網站所有功能驗收通過。
最早的其他推論模式批次在第二模式遇到官方來源更新鎖，第一模式已完成；
後續等待來源可一致讀取才重跑剩餘兩個模式，原失敗 run 及 log 保留。
舊 D metadata benchmark 首次呼叫方式錯誤，改用 module 後被現行 same-volume
backup guard 拒絕。這是未量測及拒絕結果，沒有放鬆保護或產生 D/NAS 備份吞吐量。

## 紀錄格式與讀回

- `collection.json`：本次數量、輸入路徑、大小/深度/最近 run 上限、缺口、輸出 SHA。
- `speed_samples.jsonl`：一筆一個區間，seconds、metric、origin/outcome、來源 SHA、
  locator、context、工作量與可計算的 units/sec。失敗及空結果各自留存。
- `speed_summary.csv`：按 workflow/stage/metric/origin/outcome/context 分組的
  n、median/min/max，可直接用試算表或 Python 篩選。
- `coverage.csv`／`inventory.json`：來源、入口、模式、服務、設定、節點與未測範圍。
  FinMind 只有能與 registry ID 精確對上的 dataset 階段才連到 source endpoint；
  這仍是階段計時，沒有宣稱該來源端到端量測完整。
- `reported_metrics.csv`／`benchmark_receipts.csv`：既有 benchmark 的位置及數值索引。
  `producer_reported_requires_scope_review` 可能包含設定值；不能直接當成可比較的實測。
- `environment.json`、`code_manifest.json`、`measurement_sources.json`：目前硬體、
  affinity/cgroup/runtime/threads 與工作樹來源身分；不宣稱常駐服務或歷史任務載入了
  此時的工作樹。收集器、命令 wrapper 與主要 benchmark 程式另保留原始內容。
- `evidence_manifest.json`、`evidence/<sha>.gz`：成功擷取的 producer 原始 bytes。
  讀取途中變更、過大、缺少與無效 JSONL 行保留為 issue；不補零、不假造速度。

所有量測輸出留在忽略的 artifacts，Git 只保留共用工具、語義測試和本報告。
原始 source data、checkpoint、正式帳本、provider queue 和服務排程均由原 owner 持有。
本次沒有啟動完整訓練、歷史回補、下單、清理來源或改動正式發布路徑。

`--verify-only` 會逐檔重算輸出 SHA、解壓原始證據重算 SHA/bytes，檢查每筆樣本的
來源連結、合法秒數、context 與吞吐量分母，再從 JSONL 重新分組核對 CSV 的
count/median/min/max。這證明紀錄與彙總可重播；資料完整性、正式部署及研究接受
仍由各 owner 的契約決定。

`collection_v3_readback.json` 已驗證 12 個輸出、3,827 份獨立原始內容、
370,267 筆計時及 22,413 個彙總群組。`bundle_acceptance.json` 另外核對
2,802 筆 HTTP 的原始單位與結果、238 筆 benchmark 原值及 319 組 native history
事件。相關 collector、agent workflow、service audit、HTTP benchmark 測試共
456 項通過；這是本次相關回歸，沒有宣稱全 repository 測試完成。

收集期間先後觀察到的路徑集合不同。`training_retention.json` 保存 76 份未出現在
最新清單的早期計時原始內容及 SHA；它們在本次探查時的原路徑已不可讀，沒有推定
原因，也沒有用目前資料回填。其 43,282 筆歷史計時另放在
`retained_prior_training_samples.jsonl`／`retained_prior_training_summary.csv`，
與新基準分開。三版完整讀回均通過；重疊紀錄不能相加當成新執行次數。

單檔原始證據上限 64 MiB，整次讀取預算 1 GiB；四份過大的 feature dictionary／
catalog 明列為未擷取。訓練計時使用深度 15，已沒有額外深度缺口；一般 benchmark
索引限制深度 2，保留 192 處深度／目錄預算缺口，避免把數十萬個 fixture/payload
JSON 當成獨立工作。這些限制和輸入名單全部保存在主 manifest。

## 重測方式

先重新取得需要的 canonical 收據，再把下列輸入替換成該次新收據；必須使用新的
output directory。收集器不會為了補齊數字自行重送下載、訓練或儲存活動。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/profile_workflow_speeds.py \
  --output-dir artifacts/benchmarks/NEW_OBSERVATION/collection \
  --training-depth 15 \
  --architecture artifacts/benchmarks/workflow_speed_20261021/architecture.json \
  --services artifacts/benchmarks/workflow_speed_20261021/service_baseline.json \
  --http artifacts/benchmarks/workflow_speed_20261021/owner_http_full.json \
  --tej artifacts/benchmarks/workflow_speed_20261021/tej_profile.json \
  --storage artifacts/benchmarks/workflow_speed_20261021/storage_history/storage_profile.json \
  --benchmark-receipt artifacts/benchmarks/workflow_speed_20261021/live_inference.json

run_fintech_python scripts/profile_workflow_speeds.py \
  --output-dir artifacts/benchmarks/NEW_OBSERVATION/collection \
  --verify-only \
  --validation-output artifacts/benchmarks/NEW_OBSERVATION/readback.json
```

本次精確輸入集合保存在 `collection_v3/collection.json` 的 `input_paths`。
要更新 storage activity 紀錄，沿用儲存 role 的唯讀 API，不派送新 activity：

```bash
bash scripts/run_lakehouse_control.sh python scripts/profile_workflow_speeds.py \
  --storage-history-only \
  --output-dir artifacts/benchmarks/NEW_OBSERVATION/storage_history
```

之後透過 `stockagent-agent run` 等待的命令會自動記錄 monotonic 完整 child wall、
已回收 child CPU 與 block I/O；失敗也記錄。這不會自動替換所有現行服務的執行器。
服務及 trainer 已有的正式計時持續由原 owner 產生，下次收集時形成新的不可覆寫基準。
排隊、最後 run receipt 落盤及 detached workers 在該 command interval 之外。

## 後續加速的分析順序

1. 從有實際使用頻率與完整工作量的流程開始，先排除重試、來源缺口及等待；例如
   TEJ 目前空結果的 before-submit 與整個任務時間接近，應再拆 UI 準備及等待原因。
2. 推論暖機樣本已顯示準備和格式化也占可觀時間。Multi-Basis 的 model 階段約
   74–94 ms、格式化約 123–155 ms；選擇候選時要看完整約 315 ms 的輸出契約。
3. 訓練依同一 experiment/input/hardware 比較 epoch 3+ 的 max-rank，並核對完整
   fold 的準備、驗證、test、圖表與 checkpoint 成本，不能只比某個 kernel。
4. 儲存先對齊 activity 輸入、資料量、實際搬運與 no-op。此小快照的兩個 roundtrip
   變體時間相近，沒有足夠證據宣稱 single-pass 在這份工作量有明顯整體優勢。
5. 最後才針對確定的 CPU loop、解析、序列化或資料搬運選擇語言/向量化/快取方案，
   保留原值、單位、PIT、fees/masks、ABI、耐久性與整份輸出，再做同條件 A/B。

目前仍缺第五個推論模式的當日重測、各來源逐一完整測速、所有配置的來源冷重建、各遠端 GPU 的新完整 fold、
全容量 NAS/跨節點回復、瀏覽器呈現、冷開機及真實交易事件全鏈。
這些缺口在 coverage 中持續可見，沒有以 HTTP 200、服務 active 或既有小基準補成已測。
