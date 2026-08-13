# 台指期／選擇權逐筆策略模式

這是一組獨立於日線 `tw_index_futures_day` 與股票一分鐘 `tw_minute` 的
completed-second 模式。三種策略共用下載器、資料集、normalizer、walk-forward、
Transformer 基礎元件、scheduler、checkpoint 與曲線輸出：

- `tw_index_derivatives_tick`：用 TXO 鏈特徵交易一個有方向的 TX 曝險。
- `tw_index_options_tick_long`：直接交易固定的 Call／Put 配對，只可買方持倉。
- `tw_index_options_tick_short`：同一配對可做多，也可裸賣 Call 或 Put。

## 因果與執行契約

- 公開成交檔只有整秒時間戳，沒有毫秒、微秒或唯一成交序號。因此同一秒內
  不假設任何先後順序。
- 模型在完整一秒結束後決策。事件標記為 `t` 的秒資料到 `t+1s` 才完整；再加
  預設 250ms 模擬延遲，使用 local `receive_ts` 落在接下來 1 秒內的第一筆
  有效五檔。
- 預設每 5 秒決策一次，使用 60 秒滾動特徵與 300 秒開盤暖機。
- TX 以當日尚未到期的最近月契約為準；第三個星期三之後才換到下個月。
- 買進增量逐檔吃 ask，賣出增量逐檔打 bid；成交量最多是當下五檔顯示量，
  不假設無限流動性。多單以 bid、空單以 ask 做保守立即平倉盯市。
- 報價若缺腿、交叉、買一／賣一零量、simtrade、傳輸延遲超標，或超過執行
  等待窗，即為不可成交；不回退成交價、中間價或 forward-fill。
- 選擇權模式只用暖機截止前已出現的成交，選最近到期、最接近當時 TX 價格且
  同履約價 Call／Put 都存在的配對；選定後整個日盤不換約，避免用全日流動性
  倒看契約。
- 公開檔同秒同權利別成交以 matched-volume-equivalent 加權，不把雙邊列重複
  當兩筆成交。
- 公開成交檔沒有歷史 bid/ask，不能事後還原。五檔必須由 Shioaji
  `TickFOPv1`／`BidAskFOPv1` 盤中向前保存；保存 exchange time、local receive
  time、五檔價量、contract metadata、capture_id、part counts 與 dropped events。

## 選擇權部位與成本契約

- Call、Put 是兩個 pseudo-symbol；兩模式重用
  `TransformerBasePortfolioModel` 與 `masked_signed_action_weights`。買方模式使用
  long+cash，空賣模式使用 long+short+cash，沒有另一套模型或 trainer。
- 買方用權利金資本定大小；空方依單一裸賣腿公式計算原始保證金：
  `權利金市值 + max(A - 價外值, B)`。Call／Put 分別計算後相加。
- 2026-08-12 設定快照為乘數 50、原始保證金 A=187,000、B=94,000 元
  （C=18,800 元；保守裸賣逐腿模式不使用 C）。A/B/C 是可變的
  TAIFEX 參數，歷史實驗必須釘選當時值，不能把目前值回填成歷史事實。
- 選擇權期交稅在買進與賣出每次交易都按實際逐檔成交權利金金額千分之一
  計算；不能沿用現股只有賣方課稅的規則。券商實收手續費仍為設定值。五檔
  已內含 spread 與顯示深度 impact，所以額外滑價預設為 0。
- 每日日盤最後一列強制平倉；非正權益為吸收式破產，後續訊號不得重建資金。
- 這是連續／可微分的分數口研究 executor。已使用五檔可成交方向與顯示量，
  但仍沒有交易所排隊順位、隱藏量、撤單競爭、整數口、維持保證金追繳與組合
  部位折抵；因此是 marketable-fill 模擬，不是保證 live fill，也不宣稱價差、
  跨式、勒式、日曆價差、備兌或 SPAN 優惠。

## 資料與訓練

先安裝盤中 FOP 擷取服務。它重用既有微結構 callback queue、PartWriter 與
`capture_id`，只訂閱前月 TX 與兩個近到期日的近 ATM Call／Put strip：

```bash
sudo bash scripts/install_shioaji_top200_service.sh --taifex-bidask
systemctl status stockagent-shioaji-taifex-bidask.service
```

每日擷取完成後會執行 dropped-event、商品覆蓋、非交叉五檔與傳輸延遲 audit。
再下載官方成交檔並建立 receive-time 對齊的策略資料集：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/download_taifex_recent_index_derivatives_ticks.py \
  --output-dir data_tw_index_derivatives_ticks --days 30
run_fintech_python scripts/build_taifex_index_derivatives_tick_dataset.py \
  --raw-root data_tw_index_derivatives_ticks \
  --output-root data_tw_index_derivatives_ticks/strategy_dataset_bidask \
  --execution-price-source shioaji_bidask \
  --bidask-capture-root data_tw_index_derivatives_ticks/shioaji_fop_captures
```

啟動三種正式設定之一：

```bash
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_index_derivatives_tick.yaml
run_fintech_python train.py \
  --config configs/markets/tw_index_options_tick_long.yaml
run_fintech_python train.py \
  --config configs/markets/tw_index_options_tick_short.yaml
```

訓練入口會重新核對 TAIFEX 與 Shioaji capture manifest、每個五檔 part SHA256、
每日分割 SHA256、特徵契約、checkpoint
契約以及 20/5/5 日的時間序列 train/validation/test 所有權。來源的滾動 30
日內容變動時，舊 checkpoint 不會靜默接到新的資料版本。資料 partition 同時
只納入具有完整 capture receipt 的交集日期；manifest 會列出缺少五檔的日期與
原因。預設 20/5/5 需要至少 30 個已保存交易日。今天以前未曾保存的五檔無法
回補，因此在累積滿 30 日前，正式 BidAsk 訓練會明確因日期不足停止，不能用
成交價代理補齊。

## 目前的模型輸入

共用的 18 個特徵包括 TX 的 1/5/60 秒報酬、成交量、秒內區間與成交筆數，以及
TXO 的 call/put 成交量失衡、價平附近失衡、權利金名目、call 權利金占比、
價平成交占比、成交量加權距價平程度與盤中時間位置。所有標準化統計只用
該 fold 的訓練日估計。直接選擇權模式再為每個 Call／Put 腿加入權利金、
有號價外程度、內含價值、權利別與距到期日，共 24 個每腿特徵。
