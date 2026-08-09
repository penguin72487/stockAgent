# 台股全市場一分鐘 K 棒研究

這個模組研究的是「以一分鐘 K 棒做決策的台股當沖」，不是 HFT。資料只來自
永豐 Shioaji `api.kbars()`；既有 Tick/BidAsk 秒級擷取服務與
`data_tw_microstructure` 完全不參與這條資料管線。

## 研究範圍

Universe 取自 `data_tw_public/stocks/symbols.csv`，只保留 `stock` 與
`etf`。2026-07-27 的最新封存目標共有 2,744 檔；其中永豐歷史 KBar
可用來源 2,620 檔，Contract V2 明確不存在 124 檔。不存在的合約仍保留在
全市場母體統計，但交易 mask 永遠為 false，不會補造價格。

「全市場」代表每天所有可用股票與 ETF 都是橫斷面候選標的，不代表策略必須
同時持有 2,738 檔。基線會從全市場依已完成 K 棒的分數選出前 `top_n` 檔，
再套用流動性、單檔曝險與單筆金額上限。

目前先做 long-only。單憑歷史 K 棒無法重建每個時間點的先賣後買資格與可借券
庫存；在這些資料接妥前允許回測放空，會把不可執行交易誤當成策略報酬。

## 第一性原理契約

策略有用的必要條件不是毛報酬為正，而是：

```text
可實現淨優勢
= 訊號毛優勢
- 券商手續費
- 證交稅
- 買賣雙邊滑價
- 市場衝擊與未成交
- 資料與時間對齊誤差
```

因此資訊、執行與標籤必須分開：

```text
每一根第 t 分鐘 K 棒收完
  -> 只用第 t 根與更早資料重算全市場分數
  -> 決定新的目標持股
  -> 以第 t+1 根 Open 執行目標與現有持股的差額
  -> 持倉狀態延續到下一分鐘
  -> 13:30 前強制歸零
```

Shioaji 一分鐘 K 棒是右標；例如 `09:30` K 棒代表 `[09:29,09:30)`。
因此 09:30 決策已經看完該分鐘，執行代理價才使用下一根 K 棒的 Open。下一根
OHLCV、當日收盤價與報酬全部是 executor-only 欄位，不能餵給模型。若已選中的
股票下一根資料無效或容量不足，只能減少／取消成交，不能用未來成交資訊改選
下一名。

預設 `minute_rebalance` 從 09:01 到 13:29 每根 K 棒都產生一次決策；需要滾動
特徵的策略在暖機完成前會決策為不交易。每分鐘決策不等於每分鐘完整賣掉重買：
回測器保存現金與股數，只對目標差額收取費稅與滑價。排名緩衝預設為
`top_n * 2`，原持股仍在保留排名時繼續持有，避免臨界排名反覆交換。

`next_minute` 保留為每次進場後在同一根下一分鐘 K 棒收盤出場的高周轉對照組；
`session_close` 則只保留為每日單次決策的舊 ablation，都不是預設模式。

## 成本與容量

預設延用專案的台灣費率設定：

- 手續費率 0.1425%，券商折扣六折，買賣各 8.55 bps。
- 股票當沖賣出稅 15 bps；ETF 當沖賣出稅 10 bps。
- 滑價買賣各 2 bps。
- 下一根一分鐘成交量參與率最多 1%。
- 單筆名目金額最多 100 萬元、單檔權重最多 15%、總曝險 90%。

價格不變時，股票一次來回約先付 36.1 bps，ETF 約 31.1 bps。這就是策略必須
跨過的最低門檻，尚未包含沖擊成本。stateful 模式只有實際調整股數才付成本，
而不是每次「持有」決策都重付一次完整來回成本。

## 資料與可續傳下載

Shioaji 單次 Kbars 查詢最多涵蓋 30 個日曆日，本模組固定使用 29 日分塊：

```text
data_tw_minute/shioaji_1m/
  minute_chunks/{symbol}/{start}_{end}.parquet
  minute_chunks/{symbol}/{start}_{end}.receipt.json
  symbols/{symbol}.manifest.json
  download_summary.json
  progress.json
```

每塊都有 SHA-256、範圍、列數、交易日、simulation 環境與稽核資訊。完整單一
標的會封存成 symbol manifest；跨日重啟直接使用封存 manifest，不重複雜湊所有
已完成資料。最後建研究資料集時會再驗證每個輸入檔 SHA-256。

Contract V2 登入後只載入 `region=TW, security_type=STK` 的 Base contract map，
不跨類型掃描指數、期貨、選擇權與權證。下載器會依公開 point-in-time
正成交量交易日排除生命週期外資料，並稽核移除全零占位列、任一負 Volume／
Amount 的歷史修正列及盤外批次列；各類數量都寫入 receipt。其餘價格不一致、
重複 timestamp 或無法解釋的資料仍 fail-closed，絕不任選重複列或補值。

全市場下載使用 5 個獨立登入程序，但共用帳號級滑動視窗 limiter；需求起始率
上限固定為永豐文件的 50 requests / 5 seconds，也就是 10 req/s。

完整 2020-03-02 至 2026-07-27 快照共有 221,778 個 receipt 分塊。公開
point-in-time panel 顯示其中 54,348 塊沒有任何正成交量交易日，會寫入有
`query_performed=false` 與明確原因的空 receipt，不浪費 API 流量；實際預估需
查詢 Shioaji 的分塊降為 167,430 個。下載器保留 10% 流量額度及 25 MiB 絕對
保留量，達門檻就安全結束，次日繼續；08:30–14:30 也會停止歷史回補，避免和
盤中即時服務競爭。

手動檢查範圍：

```bash
source scripts/runtime_env.sh
run_fintech_python downloader/download_shioaji_tw_minute_kbars.py \
  --simulation --all-symbols \
  --start-date 2020-03-02 --end-date 2026-07-26 --dry-run
```

長期服務：

```bash
bash scripts/install_shioaji_minute_backfill_service.sh
systemctl status stockagent-shioaji-minute-backfill.service
journalctl -u stockagent-shioaji-minute-backfill.service -f
```

服務第一次執行會把「昨天」寫入
`artifacts/data_repair/shioaji_minute_full/target_end_date.txt`，後續重啟仍用
同一終點，避免日期漂移改變所有分塊邊界。服務只登入 simulation 並呼叫歷史
Kbars，不載入憑證、不送單。

## 研究資料集與稽核

下載完成後：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_shioaji_tw_minute_dataset.py
run_fintech_python scripts/audit_shioaji_tw_minute_dataset.py \
  --trade-date YYYY-MM-DD
run_fintech_python scripts/audit_shioaji_tw_minute_dataset.py \
  --all-partitions
```

輸出依交易日分區：

```text
data_tw_minute/research_dataset/
  trade_date=YYYY-MM-DD/data.parquet
  trade_date=YYYY-MM-DD/summary.json
  manifest.json
```

模型欄位只有已完成 K 棒能知道的報酬、缺口、振幅、收盤位置、相對量、實現
波動與日內時間特徵。稽核會拒絕重複 key、盤外時間、日期錯置、跨缺口的一分鐘
標籤、executor-only 欄位外漏，以及無效標籤仍帶未來價格的資料列。

Shioaji 歷史股票 KBar 的 `Volume` 並非全期間固定單位：多數列以交易單位回傳，
部分新上市期間直接回傳股數，特殊 ETF 也可能使用 100 股或個別 1,000 股倍率。
schema-3 會以同列 `Amount` 是否落在 OHLC 可成交金額範圍內，從
`1/10/100/1000/contract_unit` 唯一判定 `source_volume_multiplier`，再產生
`volume_shares`。無法可信判定的列保留價格但成交容量 fail-closed，不會放大成交。

schema-3 manifest 只有在 2,744 檔全部被歸類成 available-source 或
`contract_unavailable`，且 failed／partial 都為 0 時才會標成
`research_ready`。已重試仍由永豐缺資料的日期保留為 source-gap mask；允許研究
使用可取得來源，不代表宣稱永豐對每一檔每一天都有完整歷史。

## 神經網路 walk-forward 訓練

一分鐘模式沿用 `train.py`、年度 expanding walk-forward、BF16 AMP、
Transformer model factory、AdamW、scheduler、early stopping、resume、
`checkpoint_best.pt`、`epoch_curve.jsonl` 與 validation/test artifacts：

```bash
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_minute.yaml \
  --start-fold 1
```

這台雙 RTX 5090 主機使用單 fold 內 DDP；`batch_size_train/eval` 是兩張卡合計的
全域交易日數，並非每張卡各自的數量：

```bash
bash scripts/run_tw_minute_dual_5090.sh \
  --start-fold 1
```

launcher 會把 rank 0/1 分別綁到 GPU 0/1 鄰近的 NUMA CPU，輸出獨立寫到
`artifacts/markets/tw_minute_dual_5090_developing99_raw_v7`。這份設定以
`tw_public_lanten_market_candles.yaml` 為母設定，保留其 FinancialTransformer、
BF16、optimizer、1000 epochs、early stopping 與 scanner 設定，只覆寫分鐘資料／
executor 契約及雙卡容量。每個全域 batch 仍保持日期順序，交易日
才在 rank 間切分；各 rank 會跑完自己交易日內的 270 分鐘狀態帳本，再以 DDP
同步梯度。因此這不是「一張 GPU 跑一個 fold」。

交易規則直接沿用一般台股當沖基準，只把決策頻率改成一分鐘：初始本金 1,000 萬、
raw signed score 經 gross-L1 1.0 正規化且不做 de-mean、
每根 KBar 最多參與 50% 成交量、使用正常手續費與當沖稅，且不加額外滑價、單筆
金額上限、單一標的權重上限或 outside-cash logit。再套逐日精確放空資格、
前一資料 session 的官方放空容量及成交限制；被擋掉或未成交的部位保留為 cash，
不會重新分配到其他股票。這些遮罩是可執行性契約，不是壓力測試。

模型每個 completed minute 都會看到 25 個動態欄位：10 個一分鐘微結構欄位，加上
15 個由「截至當下」session open、累積 high/low/volume 與最新 close 重算的純 K 線
欄位。其餘 84 個 point-in-time 日級欄位只投影一次，但會融合到每個分鐘 token；
因此完整保留母設定的 99 個 feature，且任何分鐘都不會讀到當日未發生的收盤或總量。

雙 RTX 5090 的完整 epoch-2 吞吐掃描採 2 的冪次容量。原始 fold 1 有 456 個
train sessions，所以 nominal batch 64 被均分成八批、實際每批只有 57 天；它沒有
測到 64 天的完整容量。fold 2 首次形成完整 batch 時，每 rank 的
`32 days * chunk 16 = 512` compiled rows 會在 30.46 GiB 後再要求 226 MiB 而 OOM。
目前保留全域 `batch_size_train: 64`、`batch_size_eval: 16` 與 train/eval chunk
`16`。BF16 模型改用 `amp_native_position_add: true`，將小型 position tensor 轉成
activation dtype，避免把完整 `[B,L,S,D]` candle residual 升回 FP32；因此不必增加
日內 chunks 或改變 optimizer-step schedule。另啟用已通過 fullgraph 測試的
`checkpoint_blocks: true`，在 backward 重算 Transformer block 內部活化，替固定圖
與 Triton autotune workspace 保留顯存。fold 2 的完整單 epoch compiled smoke 已跑完
train、validation、first-test-year audit 與最終 validation/test；steady training 約用
31.0 GiB/卡並保留約 1.11 GiB，group 結束後 reserved 由 29.47 GiB 降至 0.07 GiB。
`batch=128/chunk=8` 沒有更快且把每卡顯存推到約 31.5/32.6 GiB，故不採用。

這份執行器是真正的 signed long/short 帳本，不會把負權重截成零。空方權限不從
當代名單或普通 `tradable_mask` 猜測：啟動時從
`data_tw_public/features/tw_public_stock_daily.parquet` 一次讀取當日精確的可當沖／
可先賣旗標，再把官方收盤後融券證據與容量嚴格位移到下一個資料集交易日；缺列、
缺日、零容量都 fail closed。結果只保存緊湊的 `[day,symbol]` bool/int64 sidecar，
不把同一個日規則複製到 270 根分鐘列。

熱路徑不再每個 epoch 對每批重做多次 `np.stack`，而是依確定性的 chronological
batch plan 快取最終 host tensors；每個 optimizer batch 的多個 recurrent chunks
也只在最後一次 backward 做一次 DDP all-reduce。關閉的 `symbol_position` 會從
分鐘 optimizer/DDP bookkeeping 凍結，但仍保留在 state dict。scanner-free CUDA
模式不會在每個 chunk 為 scalar finite check 強制同步，成交數學本身則維持有限與
有界。

同機 fold 1 的完整 epoch 2（456 train sessions＋246 validation sessions）實測由
舊版 27.260 秒降為 24.061 秒，約快 11.7%；其中 train 20.798 秒、validation
3.262 秒。這是舊 FP32 position-add 在 fold 1 未滿 batch 下的 steady-state 參考，
不是 v2 的跨 fold 容量證據；第一個 epoch的 compile／autotune 冷啟動也不納入
預設選擇。

每個 epoch 完成後，rank 0 會在背景更新
`train_YYYY-YYYY/epoch_curve_every1.png` 與
`train_YYYY-YYYY/epoch_timing_every1.png`；直接開著圖片即可一邊訓練一邊查看，
不會等全部 epochs 結束才繪圖。

`execution_mode: tw_minute` 會在 daily panel 建置前分流到專用逐日 streaming
loader；它不會借用 `tw_day_trade` 的每日 open-to-close 標籤。每個交易日展開成
固定 270 分鐘的稀疏全市場 panel，模型每根完成 K 棒輸出一次目標，executor
只交易目標差額。正規化平均與變異數由該 fold 的 train 年份分區統計聚合，
validation/test 不參與。13:29 只能減倉，13:30 強制歸零。

## 透明策略與時間序列驗證

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/research_shioaji_tw_minute_strategies.py \
  --holding-mode minute_rebalance \
  --first-decision-minute 1 --last-decision-minute 269 \
  --top-n 10 --selection-hysteresis-multiplier 2
```

基線包含：

- 一分鐘動能
- 一分鐘反轉
- K 棒壓力
- 放量突破
- 多訊號 blend

日期依序切成 60% train、20% validation、20% test，絕不隨機打散分鐘列。
stateful 評估只走一次連續資金路徑，再從同一路徑切出三個時間區段，避免把同一
分鐘重複模擬。資料少於 120 個交易日或 20 檔股票時，程式仍可做工程 smoke
test，但會設定 `research_ready=false` 並禁止從 validation 宣稱選出策略。

全市場研究逐日讀取 Parquet，不會把多年度所有分鐘列一次載入記憶體。橫斷面
排名改由每分鐘 NumPy stable rank 執行，並共用已驗證的每日分區。2 檔、133 日
pilot 有 70,654 個原始列，每個策略產生 35,245 次分鐘決策；五個策略完整跑完
約 27.6 秒，相較第一版重複 full/split 回測的 85.7 秒快約 3.1 倍。

## 目前限制

- Kbars 沒有 bid/ask、委託佇列、逐筆部分成交與真實沖擊成本。
- `t+1 Open` 與日終強制平倉價是可重現的代理價，不是成交保證。
- 日終平倉超過觀察成交量容量或使用過時最後價格的金額會獨立報告，不會隱藏。
- Shioaji 現有 contracts 可能不包含歷史下市標的；缺少合約會被明確記成
  `contract_unavailable`，因此研究仍需揭露可能的存活者偏誤。
- `contract_unavailable` 與已稽核 source gap 仍造成歷史覆蓋差異；報告必須保留
  這些數量，不能把 available-source 回測寫成無缺口的全市場真值。
