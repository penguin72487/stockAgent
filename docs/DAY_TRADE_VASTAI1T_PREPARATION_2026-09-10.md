# Attention LayerNorm 跨日帳務：vastai1T 準備與實測

最新整日 executor／simulator／loss 串接、530 項回歸及遠端 SplitScan 修復見 [本輪整合紀錄](DAY_TRADE_FIFO_SESSION_INTEGRATION_2026-09-10.md)。下方保留前階段資料同步與微測速證據；正式 trainer 尚未完成。

## 1. 執行進度與交付邊界

2026-09-10（Asia/Taipei）。已準備遠端隔離程式碼、兩份精確版本的資料、CUDA 核心測速與驗證設定。**尚未完成正式訓練整合，未啟動正式訓練；不能提供已驗收可用的新訓練指令。**

使用者要求的完整跨日 FIFO 庫存不能替換成日末成本近似。候選設定繼續被 `load_config` 拒絕；沒有刪除 guard、關閉 carry、移除分鐘來源或開啟日 K proxy。年度 expanding walk-forward、指定 attention/full_then_last/LayerNorm 架構、50% 分鐘量及 TWD 10M 本金保持不變。

本次沒有送券商委託、替換正式 checkpoint、重啟 Discord／網頁服務或覆寫 2/25 起的正式帳本。441 項本機測試通過（24.86 秒）不等於 train.py 年度端到端驗收。

沿用 training-reuse 的共用訓練架構與 strategy-switch 的隔離驗收邊界：只擴充專用帳務／來源元件，不新增另一套 trainer，也不以微測速替代正式訓練與 promotion 證據。

## 2. 遠端環境與隔離

| 項目 | 本次實測 |
| --- | --- |
| 遠端原工作目錄 | `/root/stockAgent`；既有 dirty futures 工作未覆蓋 |
| 本次隔離 worktree | `/root/stockAgent-daytrade-parity-20260910` |
| 隔離 Git 基底 | `2d399eccbab241ca8d57a6caf536456c0b60e27f`；另套本次限定路徑 patch |
| GPU | 2 × RTX 5090，各約 32 GiB；核心測速使用 cuda:0 |
| Python／PyTorch | `/venv/fintech/bin/python`；PyTorch 2.11.0+cu128 |
| CUDA 環境 | strict environment check 通過；沒有 CPU fallback |
| CPU 配額 | cgroup 約 53.76 CPU；不是主機顯示的全部 CPU |
| 候選設定 | `configs/deployments/tw_day_trade_attention_parity_vastai1t_candidate.yaml` |
| 遠端 focused tests | 122 passed，8.30 秒 |

候選設定使用兩個精確 materialized release 絕對路徑；panel／minute cache 與產物只寫隔離 worktree 的 `artifacts`。沒有改原工作目錄 `data_tw_public` 指向的舊版本，也没有複製完整原始 artifact tree。

CPU 24／compile 8／panel workers 16 是依現有配額限制的起始設定，**未經完整 epoch 最佳化**。不能把核心基準的 threads=8 或 GPU:0 結果直接當成兩張 GPU 的最佳訓練設定。

Vast guide 與目前能力顯示 `workspace_is_volume=false`：這是容器儲存，**不是已驗證的持久 volume**。重建／銷毀 instance 可能失去隔離目錄及未完成訓練產物；不能宣稱已完成遠端災難備援。Penguin 冷庫仍是資料權威。

## 3. 資料透過 Syncthing 接收，不用 SSH 搬資料

| Dataset | 精確 release | 完整 materialization |
| --- | --- | --- |
| tw-public | `tw-public-20260910T100453368085496Z-l0-penguin-234efa703efde4b2` | 115,997 檔，9,616,112,077 logical bytes |
| tw-minute-train | `tw-minute-train-20260910T121601063496311Z-l0-penguin-09bedd96a2f68c39` | 3,189 檔，13,817,707,306 logical bytes |

都位於 `/srv/stockagent-packed-materialized/<dataset>/<release>`。以 canonical `run_data_cache.sh use --snapshot-id` 在遠端明確驗證與啟用；不是接收時自動 materialize。lease 為 7 日，到期後使用者應重新 `use`，不能直接指向已可能被 GC 的舊路徑。

核對憑證：

- Public manifest SHA-256：`44c1159b0f1fe53d9be75d6c73efc26eeacdeee309dca168242b86a04f2a2e5b`。
- Minute manifest SHA-256：`ee16387fb1161edf228b87120dc7e112dcd4d6541148ff460a0c9d84bf8e72da`。
- Public edge receipt：`/var/lib/stockagent-packed-edge/receipts/packed-edge-1789042697830100928.json`。
- Minute edge receipt：`/var/lib/stockagent-packed-edge/receipts/packed-edge-1789043083375157923.json`。
- 從 penguin 與 vastai1T 兩端重新觀察 `stockagent-packed`：idle、need bytes/items/deletes=0、folder/system/pull/watch error 全部為空／0、peer completion=100%、remoteState=valid，觀察到 QUIC 與 TLS 1.3。這不是 lab203 的驗收。

Edge 生命週期按既有 index-only 規則清理已驗證冗餘 payload：public 1,403 檔／14,288,112,056 bytes；minute 900 檔／14,015,770,970 bytes。只刪遠端可重抓的冷 payload 副本，來源、權威冷庫與 materialized 內容保留，可以再透過 Syncthing 恢復。沒有改 Vast 的 edge 身分。

### 分鐘資料驗證與限制

遠端第一次完整逐分區 SHA-256／row audit：1,594 partitions、310,991,195 rows，2020-03-02～2026-09-10，通過。receipt：`artifacts/operations/daytrade-parity-20260910/minute_audit.json`。

稽核器已修正：manifest 的 `output` 是下載主機 provenance，不能優先選舊工作目錄。現在只驗指定 release 裡的 canonical partition，拒絕越界 symlink、日期清單不一致及稽核中途 manifest 變動，並記錄精確 root／manifest hash。

全日曆比對使用既有 `_validated_taiex_session_dates`，不另造交易日曆。已觀察官方期間 1,592 個 session，無缺整日分區；來源另有 `2020-09-27`、`2024-12-01` 兩個非交易日分區，保留證據，不刪來源或偷偷当交易日使用。**未完成逐股 × 交易日可用性驗收**；manifest 尚列 89 source-gap symbols、127 contract-unavailable symbols。來源整體有資料不能解讀成每檔每天都完整。

第二輪加入精確 release scope／官方 calendar 的全量稽核 receipt 位置：`artifacts/operations/daytrade-parity-20260910/minute_release_calendar_audit.json`；完成狀態以該檔案及本文件末尾驗收更新為準。

## 4. 核心修正及實際帳务對照

1. 庫存 ABI 升至 `tw_day_trade_fifo_physical_inventory_v2`。移除 CUDA device assertion，改由 device-resident validity predicate 原子提交；錯誤事件不部分改帳，failure flag 吸收後續事件，NAV 不得回傳假綠燈。CPU 與 checkpoint acceptance 仍可明確拋錯；不把每次 GPU 帳務轉回 CPU。
2. PyTorch 2.11 不能在此 fullgraph 路徑追蹤 Enum iteration，改用固定欄位索引 tuple；未變更 cohort／費率語義。
3. `reduce_inventory_fifo_path` 用累積容量的分段現金積分取代 270 次 FIFO 更新；不產生 K×S×270 巨型張量，不混合不同取得批次的費率。
4. `inventory_path_nav` 逐分鐘計算淨清算權益，保留剩餘 entry fee／各 cohort exit fee。13:30 當天融資融券轉換成本由 caller 加到終點；估值不是假平倉。
5. `paper_minute_opportunities` 是 executor-only 的來源排程，未進模型特徵。要求 exact session date、合法 stock／ETF tick、OHLC/VWAP 一致及整數股數成交量；VWAP／basis／NAV 不依委託 tick 四捨五入。
6. 正式 paper replay 的 12 組多空情境，每組 270 點、共 3,240 分鐘，與新核心逐點比較通過；不是只比較日末金額。修正測試要傳正式回補的 `historical_minute_valuation=True`，不能把檢查 limit crossing 的 high/low 當 Close 估值。

包含 passive、late quote、stop latch、同分鐘雙 bracket、只有收盤集合競價成交、全日無量殘倉；既有多日 FIFO／換股／現金 claim／週末利息／序列 checkpoint 分段測試也保留。

排程維持 09:00 決策、09:01 回補執行、13:20 送限價、13:24 送市價後第一個可用右標籤為 13:25；13:26～13:29 試撮不能当真實成交量，13:30 只用來源收盤集合競價量。沒有保證殘倉一定成交。[證交所交易制度](https://www.twse.com.tw/zh/products/system/trading.html)

零股按整張行情、全部可轉融資融券仍是使用者指定研究假設，不宣稱券源／授信存在。這些 GPU 核心也不是 Shioaji simulation 的真實成交回報。

## 5. vastai1T 核心測速，不是訓練 epoch

固定 S=2,754、K=8、270 分鐘、float64 帳務、cuda:0，3 次暖機後測量的中位數：

| 核心工作 | 逐分鐘 eager | 積分 eager | 積分 Inductor |
| --- | ---: | ---: | ---: |
| FIFO forward | 282.12 ms | 1.195 ms | 0.290 ms |
| FIFO forward + backward | 712.63 ms | 4.998 ms | 2.035 ms |
| 270 點 minute NAV | 391.73 ms | 2.580 ms | 0.382 ms |

最後一輪曲線 Inductor 首次呼叫 4.634 秒。既有編譯 cache 可能影響 cold 值；不當作全新機器冷啟動或實盤延遲承諾。基準包括 source-state 對照、有限梯度與選定非歧義梯度欄位對照；不是訓練績效驗證。

遠端完整 JSON：`artifacts/operations/daytrade-parity-20260910/fifo_minute_curve_microbenchmark.json`；明確記錄 `full_epoch_measured=false`、`train_py_inventory_integration_complete=false`。尚未驗證大量長期 cohorts、DDP、全模型、optimizer／plot／checkpoint I/O 下的峰值 VRAM 或速度；不能宣稱已達理論極限。

## 6. 歷史企業行動的實際 blocker 與修復

Canonical share-replacement collector 原本在第一筆 TWSE 明細錯誤就停止，後面的健康事件、整個 TPEx 來源都被阻塞。現在按事件／來源隔離錯誤，持續收集其他資料並保留 attempt receipt；只要任何必要來源失敗，**不覆蓋上次 accepted Parquet／summary**。已有失敗後復原、健康事件繼續下載及舊版本不被覆寫測試。

2020-01-01～2026-09-10，加入 MOPS 備援之前的實際重試結果（後續恢復見本文件末尾）：

- completed_reference_rows=163，TPEx 來源處理完成。
- TWSE detail failures=104；HTTP 200 的內容實際為官方 WAF 封鎖頁，並非有效 JSON 明細。
- 修正共用 HTTP cache：HTTP-200 throttle/access-denial 不得變成正式來源 receipt；已快取的拒絕頁保留到 hash-addressed `rejected/*.response` 作診斷，允許未來正常來源回應修復，不會永久吃舊拒絕頁。
- 修正後對 `6243 / FILE_DATE=20211026` 作一次有界 canary：仍 HTTP 428。沒有繞過 CAPTCHA／WAF、換未授權來源或偽造付款日。
- attempt receipt：`/srv/stockagent-live/data_tw_public/execution_actions/tw_share_replacement_reference.attempt.summary.json`。正式 accepted table 未動；未把失敗 attempt 發布成新 cold release。

官方減資查詢提供自 2011 年起資訊，但「網站應有歷史」不代表本機已取得每筆換股／現金條款。不能由除權參考價倒推完整持倉權利或付款日。[官方查詢與參考價公式](https://www.twse.com.tw/zh/announcement/reduction/twtauu.html)

即使解除 WAF，換股零碎股份處分、現金增資／分割取得其他股票、現金付款日仍須逐事件驗證。已解析的參考列數不是 `accounting_ready=true`。

## 7. 遠端可重跑指令與尚未完成的訓練工作

以下是**驗證／測速命令，不是新訓練命令**：

```bash
cd /root/stockAgent-daytrade-parity-20260910
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python -m pytest -q \
  test/test_day_trade_attention_parity_candidate.py \
  test/test_tw_minute_audit_release_scope.py \
  test/test_checkpoint_manifest.py \
  test/test_training_resume_early_stop.py \
  test/test_resume_completion_marker.py
run_fintech_python scripts/benchmark_tw_day_trade_inventory.py \
  --device cuda:0 --symbols 2754 --cohorts 8 --repeats 3 --threads 8 \
  --output artifacts/operations/daytrade-parity-20260910/fifo_minute_curve_microbenchmark.json
```

新訓練命令尚不能放行，剩餘工作不得混稱資料 blocker：

1. 將來源 schedule、每日合法限價及 accepted actions 接入新版本 execution tape；不得當模型特徵，保留 ordered universe／精確 release／hash。
2. 在共用 `simulator → loss → trainer` 全程攜帶 float64 實體庫存、cohort 費率、claim、利息及事件游標。要覆盖 train batch、eval chunk、fold handoff、stitched deployment、NPZ 與 epoch checkpoint；不能用 final_weights 還原 FIFO。
3. 更新 source／execution／checkpoint／artifact fingerprints，舊 checkpoint 不得假稱相容；金融破產與資料失效狀態需分離，避免恢復初始本金。
4. 年度端到端 smoke、精確續訓與逐日／逐分鐘紙帳對照；完成全 universe cold epoch + epoch 2 的 train/validation/test/plots/checkpoint 測速後，才決定 DDP／batch／compile 最適設定。
5. 資料完整期間通過後再發正式 `train.py --config ...` 指令。不能自行改成不足一年的切分來繞過歷史資料不足，也不能訓練舊日末近似版充數。

### 最終驗收更新

遠端第二輪精確 release/calendar 全量稽核已完成：1,594 partitions、310,991,195 rows；官方 1,592 session 全有分區，另兩個非交易日分區保留並明列。資料集內部 `manifest.json` SHA-256 為 `40b0968096146ab32de511129088522fd275ff123e155f66a41e986216409f7b`（與上方 cold release manifest 是不同層）；日曆來源 SHA-256 為 `3f272a327bb55b9a066a3a4c4e1cbebf0f2d2783d57872a87439c9f0432c1416`。

程式碼整合與正式訓練仍未完成，不受資料分區稽核通過影響。

### 使用者要求追加掃描與其他來源（本輪完成，仍有缺口）

- 本機以不忽略 Git-ignore／隱藏檔並跟隨資料連結的檔名 inventory，掃描 canonical live public、materialized public、repo artifacts，共 460,820 paths；比對失敗事件的股票／FILE_DATE 候選 54 個，未找到可直接復原該 104 筆的有效 TWSE detail。這不是全機任意磁碟／所有 cold 歷史 object 的內容掃描。
- Vast 原工作目錄 `data_tw_public` 與 `artifacts` 另掃描 247,485 paths，找到 6243 的舊股利查詢頁；不是減資換股作業公告，不能混用。
- 找到可用的 **MOPS 原始發行公司公告**，不是第三方新聞推測。`6243 / 2021-11-04` 對應的 `2021-10-12 17:42:22 / seq=3` 原文已取得，換股每千股 484.4551 股、停牌 2021-10-28 起，且原文包含零碎股份處理。僅作參考，不把零碎股約定直接改成尚未實作的 cash-in-lieu 帳務。
- 下載器共用 `_issuer_announcements` 查詢，讓付款日與減資原文明細共用查詢／快取／收據。TWSE access-denial 後該次 run 暫停重打同一受阻端點，仍可讀已存在的有效 TWSE cache，再轉 MOPS；保留原始錯誤、是否真的送出 primary request、replacement source/hash。
- 解析必須匹配公司、同一次 resumption、明確 suspension、每千股換股比率；只接受可驗證的 per-share/per-1000 cash 單位換算，不接受「約」或從價格反算。
- MOPS 參考列有 `reference_only=true`。paper 在恢復交易時會拒絕把這些尚未完整處理的 dividend/subscription/fractional-share 條款拿來改帳；下載成功不代表會計完整。
- 已核對 [FinMind 官方 dataset schema](https://github.com/FinMind/FinMind-MCP/blob/master/knowledge/datasets.md)：減資參考價資料不包含本次所有必要的換股、現金付款與零碎股條款，不能直接替代這些來源；未購買或新開資料帳戶。

最後批次已結束：原 104 筆受阻事件中，68 筆恢復官方核心參考條款、36 筆仍未通過；全輪 231 筆參考事件。658 筆 request/response 鏈與 68 個恢復原文雜湊核對通過。公告標題換行、共用每千股單位等解析缺陷已修正並測試；沒有將約數、價格倒推或不完整會計條款當成驗收通過。

詳細逐筆結果、來源 hash、測試及重跑命令見 [歷史減資／換股公告恢復紀錄](DAY_TRADE_SHARE_REPLACEMENT_RECOVERY_2026-09-10.md)。這批新 raw 尚未發布 cold release，因此不在上面固定的 Vast public materialization 中；不能把遠端原 release 驗收誤稱為本輪新來源已同步、可用。正式 trainer 跨日整合仍是獨立未完成項目。
