# 個股期貨當沖：08:45 決策、08:46 執行、13:30 結束

2026-09-09 後續授權改為 **13:30 未成交即留倉，隔日依新目標與持倉差額交易**。
新設定 `tw_stock_futures_day_trade_0845_carry_v8.yaml` 保留原分鐘平倉嘗試，
新增實體合約跨日估值、正式到期結算及持倉歷史。規則、驗證與指令見
[剩餘部位留倉帳務](FUTURES_RESIDUAL_CARRY_2026-09-09.md)。下方 v1–v5 的嚴格歸零規則仍是舊實驗的重現契約。

2026-09-09 最新容量指示：`tw_stock_futures_day_trade_0845_capacity_ceil_v5.yaml`
將 50% 逐分鐘進出場容量無條件進位到口：`ceil(volume * 0.5)`。
零量仍為零，資金可負擔口數不進位；舊 floor 設定與 checkpoint 保留。
使用者已同意將兩個 floor-only 證明失效的缺來源契約日加入隔離；v5 已接入
新資料版本，保留全部 1,573 個決策日與同日其他候選，詳見
[容量進位驗證](FUTURES_CAPACITY_CEIL_2026-09-09.md)。

2026-09-09 空手模型後續修正：`tw_stock_futures_day_trade_0845_gradient_v4.yaml`
沿用 v3 整口帳本／梯度，啟用既有訓練期逐特徵 RMS 正規化；使用獨立產物目錄。
尺度量測、最佳模型選擇與完整第一 fold 對照見
[空手模型診斷與 v4](FUTURES_CASH_POLICY_AUDIT_2026-09-09.md)。

> 2026-09-09 historical／v2-v4 對照僅隔離 `2021-06-21 / LVF:202107`，見 [合約日隔離修正](FUTURES_CONTRACT_QUARANTINE_2026-09-09.md)。最新 v5 另經同意隔離 `2023-07-13 / PZF:202308` 與 `2024-01-10 / LIF:202401`；仍保留同日其他合約及全部 1,573 個決策日。下方整日隔離相關內容為舊版本紀錄。

## 目前準備範圍：2020/03/23 起

使用者 2026-09-08 後續要求只從永豐有的期貨歷史開始。一般 historical YAML
現選定 `data.panel_start_date: 2020-03-23`、`walk_forward.expected_first_year: 2020`，
沿用分鐘時鐘與既有訓練器，使用獨立資料版本和訓練產物目錄。這次範圍不含日線近似；
更早的原始資料與下方 v2 近似能力仍保留。驗收狀態及路徑見
[2020/03/23 起資料準備](futures_from_20200323_preparation.md)。

## 2026-09-07 新增的早期歷史近似

2026-09-08 全量整理已完成：25,574,357 筆已觀測分鐘、297 種商品，但仍有 37,723 個
候選合約日缺口，完整訓練尚未就緒。期間、逐商品明細與驗收證據見
[本次資料與訓練接入報告](FUTURES_MINUTE_TRAINING_2026-09-08.md)。
指定的 vastai1T 已收到私人準備快照與配對日線；交付 SHA、硬體測量及遠端驗收見
[vastai1T 準備報告](vastai1t_futures_training_2026-09-08.md)。

依使用者這次明確指定，新增
`configs/markets/tw_stock_futures_day_trade_0845_historical.yaml`，仍走同一個
`train.py`、FinancialTransformer、98 個前日特徵、BF16 及整數口執行器。
`tw_stock_futures_day_trade_daily_proxy_before: '2020-01-01'` 是**不含當日**的分界：

- 2014–2019：缺分鐘時，使用官方**同一實體期貨**日盤 Open / Close 近似進出。
  以先前交易日成交量限制口數，保留雙邊費用、逐口稅額、標準／小型契約及餘額現金。
  日線 Close 並非已證明的 13:30 成交，這段不能用來證明分鐘退出能力。
- 2020-01-01 起：保留 08:45 決策、08:46 進場、13:20 限價、13:24 撤換、
  13:30 截止。缺來源必須報缺口，不能變成日線成交或零報酬。
- [永豐官方歷史資料文件](https://sinotrade.github.io/zh/tutor/market_data/historical/#_3)
  公布期貨 Tick / KBar 起點為 **2020-03-22**；因此 2020 年初仍有不可由該 API
  補足的區間。尚未另獲使用者指定前，不自動把日線近似延伸到 2020 年 3 月。

2020 年以前的成交資料仍可另向期交所取得。2026-09-07 查閱
[期交所交易歷史資料申購頁](https://www.taifex.com.tw/cht/3/hisAppForm)
及其「交易歷史資料價格及起迄時間一覽表」：期貨成交簡檔自 1998-07-21 起提供，
價格為每半年 NT$1,000，各商品仍以實際上市日期為起點。其格式含成交日期、實體
到期月份、成交時間、價格及買賣雙邊合計數量，可聚合分鐘；買賣雙邊數量須依官方
parser 規則換成撮合口數，不能照搬永豐 Tick 的數量。申購與授權尚未執行。
本次保存價目及格式文件於
`artifacts/operations/futures_minute_training_20260907/taifex_historical_availability.odt`
及 `taifex_simple_trade_format.odt`。

從已下載的連續 Tick 整理資料：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_tw_stock_futures_0900_entries.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_historical.yaml \
  --shioaji-ticks-root data_tw_futures/shioaji_history
```

R1 當下解析到的月份不能用來標示歷史月份。建置器先用官方逐日月契約排序決定
R1 實體身分，再核對當日 OHLC、Tick receipt / SHA、查詢日期與別名；不使用
事後價格挑選表現較好的月份。時間戳是台北牆上時間，不再加八小時。
來源資料正被更新時，前後 SHA 或 receipt 不同會拒絕該次建置。

輸出目錄 `data_tw_futures/taifex_stock_futures_minute_history_v2/`：

| 檔案 | 內容 |
|---|---|
| `all_minutes.parquet` | 已核對實體身分之候選合約的全部日盤 1 分鐘棒；不補價格 |
| `minutes.parquet` | 執行器需要的 08:46、13:20–13:30 事件棒 |
| `coverage.parquet` | 每個候選合約日的來源、SHA、核對狀態及成交量對照 |
| `gaps.parquet` | 缺憑證、空回覆、月份未核實、OHLC 不一致等待補項目 |
| `manifest.json` | 日線來源 SHA、輸出 SHA、日期、分界及完成狀態 |

實際 Tick 成交量可能小於官方日成交量；容量只採已觀測 Tick，不放大補齊。
中間逐日分片在 `artifacts/cache/futures_minute_history/`，不進 cold store。
新資料完整性未通過前，catalog 明確排除 `taifex_stock_futures_minute_history_v2`。
這份 YAML 固定股票 release，並把分鐘 manifest 與獨立保存的日線 SHA 配對。
使用者指定的私人準備交付保留 `partial`，不等於完整 cold release 或正式訓練就緒。
通過完整審核後須發布新 release、固定來源版本，再移除 catalog 排除項。
不得把 partial 成品標成已驗收訓練資料。

日線若已更新，須先保存新的版本，使用 `--daily-data-path` 指定它，並以新的
`--output-dir` 建置配對分鐘；驗收後一起更新 YAML 的日線與分鐘路徑。
不能只替換日線檔、改寫舊 manifest 的 SHA，或讓既有 checkpoint 靜默使用新來源。

資料檢查及原生訓練入口：

```bash
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_historical.yaml --check-data-only
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_historical.yaml
```

若仍有後期缺口，兩個指令都會列出原因並停止。契約 v2 的 67 個執行通道將日線近似
放在獨立欄位，不假造分鐘棒；checkpoint 與報告保留分界，不能續用下列嚴格 v1 實驗。

## 原有嚴格分鐘契約（保留重現）

本契約依 2026-09-06 的使用者要求，沿用一般股票當沖的每日模型與退出節奏。
正式設定為 `configs/markets/tw_stock_futures_day_trade_0845_minute.yaml`，模式為
`tw_stock_futures_day_trade_0845_minute`。模型、BF16、walk-forward、optimizer、
checkpoint、epoch curve 與報告均走既有 `train.py`，每日只產生一次權重。

訓練維持共用 `train.py --config` 入口；原本的
`scripts/run_tw_stock_futures_day_trade_0845_minute.sh` 也轉入同一個 CLI。
正式 YAML 直接指定固定的股票／期貨 release；bash 入口從該 YAML 讀取版本，沿用
`scripts/run_data_cache.sh use` 與 process-reference 續租，不生成另一份 runtime YAML。
`--check-data-only` 直接進入共用 `train.py` 資料驗收，不啟動資料解包或 GPU 訓練。
新資料使用 YAML `runner.output_dir` 指定的獨立 checkpoint 根目錄。
FinancialTransformer、多基底、BF16、1000 epochs、年度切分與原共用訓練流程不變。
操作目錄的 `train_carry.sh` 是全期貨留倉實驗；不能用它取代股票特徵驅動的當沖策略。

## 資訊與訂單

| 時間 | 行為 | 可以使用的資料 |
|---|---|---|
| 08:45 | 模型決策，輸出各股票對應期貨的 signed 權重 | 完成至前一股票交易日的 98 個特徵；不使用當天 09:00 股票開盤 gap |
| 08:46 | 第一個完成分鐘棒執行整數口進場 | `[08:45,08:46)` 的期貨成交 VWAP 與 matched volume；未成交進場量取消 |
| 13:20 | 建立被動限價退出單 | `[13:19,13:20)` 完成棒的 Close |
| 13:21–13:24 | 限價單逐分鐘部分成交 | 後續分鐘棒嚴格穿越限價，僅碰價不成交；v5 容量為 `ceil(50% × 口數)`，舊版為 floor |
| 13:24 | 撤換為市價退出單 | 只影響此時之後的成交 |
| 13:25–13:30 | 市價退出餘量 | `[13:24,13:30)` 各完成棒 VWAP 與容量，最後標籤為 13:30 |

右標分鐘棒將成交記在區間完成時刻，與一般當沖的歷史分鐘研究假設一致。
這是從實際期貨交易聚合的分鐘資料；08:45 開盤不再被當成 09:00 決策的進場價。
VWAP 成交假設仍不等於真實委託簿排隊或券商成交保證。

期貨 13:25–13:30 繼續採期貨市場的成交資料，沒有套用現股的收盤集合競價。
一般股票期貨日盤至 13:45，到期月份最後交易日至 13:30；本策略將每天的最後
退出期限設為 13:30。[期交所股票期貨規格](https://www.taifex.com.tw/cht/2/sTF)

2026-09-09 再次依使用者要求確認：13:30 清倉是本策略的硬限制。
股票期貨不適用交易所的當沖保證金減收制度，不能因「當沖」就將其資金需求減半。
現有完整名目金額 sleeve 是研究曝險限制，不是逐日重建的交易所／期貨商保證金。
[期交所結算問答](https://www.taifex.com.tw/cht/9/tradersQAClearing)。

## 合約、資金與成本

- 沿用 canonical 近月選擇：同實體契約的前一交易日存在性、成交量與未平倉量，
  每標的各選一個標準（乘數 2,000）及小型（100）候選，當日成交量不能改變候選。
- 新台幣 1,000 萬、整數口數、完整名目金額擔保的研究帳戶。每個標的獨立擁有
  模型分配的資金；沿用標準／小型組合配置器，未成交資金不移給其他股票。
- 每側每口手續費 40 元，期交稅沿用既有日期版本與每口四捨五入契約。
  每次退出均依自己的成交價計稅，不使用股票交易稅、T+2 或融資融券。
- v5 每個分鐘棒容量為 `ceil(0.5 * matched contracts)`；舊版設定保留 floor。
  未退出口數逐分鐘保留，
  同一分鐘容量不重複使用。當日損益與成本影響次日可用資金。
- 已驗證但沒有進場成交的交易日仍保留為零交易報酬；不能事後刪除而放大年化績效。
- 13:30 若仍有餘量，保留 `futures_residual_contract_quantities_history`，標記
  `settlement_default` 為執行失敗，停止該帳戶後續新交易。訓練採吸收式失敗懲罰，
  不能把該懲罰解讀成真實平倉價格或已實現損失；不能宣稱當天成功歸零。
- forward 採整數成交與同一費稅帳，backward 使用有容量限制的 fractional shadow。
  空分鐘或容量為零的分鐘，成交量與成交梯度必須同時為零；不能只把容量清零
  後使用 `min(remaining, 0)`，因為零口數的 shadow 仍有梯度。
  2026-09-09 修正使用 `executable_minute_quantity_shadow_v2` checkpoint 契約，
  新實驗設定為 `configs/markets/tw_stock_futures_day_trade_0845_gradient_v2.yaml`。
  分析與完整短程驗證見 [梯度診斷](FUTURES_GRADIENT_AUDIT_2026-09-09.md)。
- v3 實驗改由同一整數成交引擎計算相鄰組合的損益／殘倉斜率；
  空手時仍計入完整費稅，兩方向都不改善時維持零梯度。
  失敗後僅在訓練的 backward 計算後續日期的反事實結果，實際帳戶仍停止執行；
  validation/inference 關閉 recovery。沿用共同訓練器整段日期累積後更新一次的模式。
  新設定 `configs/markets/tw_stock_futures_day_trade_0845_gradient_v3.yaml` 保留 1000 epochs，
  新輸出根目錄與 gradient fingerprint 拒絕直接沿用 v2 optimizer。
  規則、數學、測試與對照見 [官方規則與梯度 v3 稽核](FUTURES_GRADIENT_RULES_2026-09-09.md)。

## 資料建置與指令

### 正式設定與缺檔診斷

自 2026-09-07 的分鐘來源修正起，可直接由既有 Shioaji collector 的 **1 分鐘 KBar**
建置 `minutes.parquet`，不需要 tick。Git 不保存市場資料。
缺少成品時，先確認分鐘 chunk 或可驗證的同源成品在哪裡。目錄名稱相同、
Syncthing 已連線或日線資料存在，都不能證明分鐘資料已就緒。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_tw_stock_futures_0900_entries.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_minute.yaml \
  --minute-root data_tw_shioaji_history --check-only
```

`--config` 解析繼承後的分鐘模式、日線來源、分鐘輸出目錄及 `panel_start_date`。
唯讀的 `--check-only` 驗證 collector 的 `inventory/contracts.parquet`、inventory SHA、
`contracts/futures/*/kbars/*/receipt.json` 與分鐘 Parquet SHA，對照既有日線選出的
每個日期／實體契約。退出碼 2 表示缺來源；0 表示此日線範圍的分鐘來源驗證完成，
仍須用下方 `--check-data-only` 檢查實際股票面板。

KBar 本來就是右標分鐘，08:46 不再向後平移。Shioaji 期貨的 `Volume` 單位是口，
`Amount / Volume` 必須逐棒通過 High／Low 價格檢查，才可接入目前的 VWAP 合約；
不乘股票張數、不再除以二，也不以 OHLC 平均冒充 VWAP。
[Shioaji KBar 官方範例](https://sinotrade.github.io/zh/tutor/market_data/historical/#kbars_1)
零成交事件分鐘保留零容量；缺少某個候選契約的歷史回覆會拒絕建置，不能當成零成交。
已完成 API 回覆也必須晚於查詢末日的日盤結束。新 manifest 記錄
`source_kind: shioaji_exact_futures_kbars_1m` 與每個日期／實體契約的來源 SHA。

2026-09-07 實際資料驗證發現兩項限制：增量查詢可能留下日期重疊的完整 chunk，
接入器現在比較同日全部正成交量日盤 KBar，完全相同才去重，衝突仍拒絕使用；
另外，部分個股期貨的 Amount 精度不足。例如 CAFI6 在 2025-11-12 的單口 KBar
OHLC 全為 49.65，Amount 卻是 49，不能據此宣稱 VWAP 為 49。
目前仍拒絕這類價格，不自動改用 Close、不修改原始來源，也不將已下載標記當成
可訓練證明。實際例證保存在
`artifacts/operations/vastai1t_futures_prepare_20260907/kbar_price_validation.json`。

下載沿用既有 collector，僅選分鐘：

```bash
run_fintech_python downloader/download_shioaji_historical_market_data.py \
  --collections exact_futures --kbars-only
```

此選項保留既有流量、查詢分段、盤中保護及續傳；不建立 tick 任務，也不掃描 tick
收據。分鐘完成狀態另寫 `summary_kbars.json`，保留原本完整歷史的 summary。
目前可查詢的實體契約不等於已到期歷史；R1／R2 的當前 target 不能直接當成歷史月份。
Shioaji 公布的期貨歷史起點為 2020-03-22，因此本選項不會補出 2014～2019 年。

補齊完整歷史來源後，去掉 `--check-only` 並指定
`--output-dir data_tw_futures/taifex_stock_futures_minute_v1` 才建置。
正式 YAML 現在讀 immutable release；建置器會拒絕寫入該目錄。
新成品須從 catalog 可寫來源發布成新 release，再更新正式 YAML 的來源及 output root。
建置器遇到缺少契約／日期會另寫 `build_failure.json`，保留既有已接受成品。
所有來源都有完整日盤後，再檢查訓練實際使用的日期：

```bash
run_fintech_python train.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_minute.yaml --check-data-only
```

此指令不啟動 DDP、CUDA 訓練或產生 checkpoint；會驗證完整資料 SHA、來源收據
及 canonical 股票 panel 的每個日期，可能建立本機 panel cache。一般啟動也會先
驗證來源及設定要求的首年；全部 panel 日期仍由同一 loader 做最後驗收。
日線來源的末日若落後股票面板，必須同時補齊日線與分鐘來源，不能把股票面板截短。

### 舊有官方 ZIP 來源的重現方式

以下僅記錄既有 50 日成品的來源；分鐘 KBar 接入不依賴此路徑。

官方公開下載頁提供[前 30 個交易日期貨每筆成交資料](https://www.taifex.com.tw/cht/3/futPrevious30DaysSalesData)。
完整跨年歷史須使用已保存的官方檔或依[交易歷史資料申請](https://www.taifex.com.tw/cht/3/hisAppForm)
取得；不同交付格式須先確認 parser 相容，不能只改副檔名。若成品位於另一節點，
以 catalog 的 `tw-futures` release 發布、驗證、明確 materialize，並確認
`source_daily_sha256` 與本次日線來源相同；raw ZIP 的 catalog 為
`tw-index-derivatives-ticks`。禁止 downloader／builder 寫入 immutable materialized 目錄。

### 舊有近期資料建置範例（不滿足正式跨年訓練）

延伸既有 09:00 sidecar builder，復用官方 ZIP parser 與 atomic writer：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_tw_stock_futures_0900_entries.py \
  --execution-policy scheduled_0846 \
  --ticks-root data_tw_index_derivatives_ticks/raw/futures \
  --start-date 2026-06-25 --end-date 2026-09-03 \
  --archive-override data_tw_futures/taifex_stock_futures_minute_v1/source_revisions/Daily_2026_08_17.zip

bash scripts/run_tw_stock_futures_day_trade_0845_minute.sh
```

建置輸出為 `data_tw_futures/taifex_stock_futures_minute_v1/`。每個 requested session
都必須有完整官方 ZIP；缺日期時產生 failure receipt，不能覆寫已接受的資料。
Manifest 綁定 daily source SHA、逐日 ZIP SHA、輸出 SHA 與日期清單。Loader 再驗證
資料來源、唯一實體契約分鐘鍵、價格、量能及全部 panel 日期。

2026-09-06 已建置 2026-06-25 至 2026-09-03 的 50 個交易日、75,434 筆實際
契約分鐘棒。原始 08-17 ZIP 僅含夜盤；上例明確指定同一官方網址重新取得的
日盤完整版本，並保留舊檔與修復 receipt。建置器及 loader 均拒絕只有夜盤的
日期證明。

只有覆蓋已驗證的日期才可用於訓練。上面近期資料的建置指令不會產生 2014 年起
的歷史分鐘資料，故不能直接滿足預設跨年 walk-forward。不得以日 K、舊收盤、
13:45 收盤、插值或連續契約別名補足。歷史 09:00 v4/v5 與舊 08:45 日 K 設定
保留重現用途；新模式、資料 SHA 與執行契約均進 checkpoint，相互不可續跑。

## 2026-09-06 工程驗證

- 真實分鐘資料：50 日、250 個期貨標的的 `[50,250,2,63]` 執行張量，在 CUDA
  完成固定測試權重的回放與反向梯度；數值有限，口數與殘倉 artifact 可往返。
  逐日獨立測試中 08-12 有未平餘量，連續帳戶亦在該日停止。
  證明見 `artifacts/smoke/tw_stock_futures_0845_minute_real_tape_20260906/verification.json`。
- 共用訓練生命週期：合成資料的 98 特徵、lookback 32、Financial Transformer、
  CUDA BF16 兩輪訓練，包含 validation/test、逐 fold 九張必要報告及完成後續跑。
  Eager 與 compiled model 均完成；分鐘整數帳本走既有 eager adapter。
  編譯驗證在 `artifacts/smoke/tw_stock_futures_0845_minute_compiled_fresh_lifecycle_20260906/`。
- 共用舊快取曾出現 Triton `Unknown key: 'cubin'`；獨立快取完成編譯驗證。
  正式設定使用此模式專屬的 TorchInductor／Triton 快取目錄。
- 因果、分鐘邊界、整數費稅、long/short、嚴格穿價、容量、零成交日期、殘倉失敗、
  原始檔完整性、artifact、checkpoint、windowed 與既有 TX/TXO 回歸測試均通過。

上述是真實資料執行驗證及合成資料訓練整合驗證，尚未完成跨年真實資料訓練，
也沒有策略獲利或可上線的結論。
