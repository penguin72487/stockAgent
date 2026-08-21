# Bybit USDT 永續合約日頻多基底策略

## 可執行契約

- 宇宙只含 Bybit V5 instrument snapshot 中仍為 `Trading`、
  `LinearPerpetual`、quote/settle 均為 USDT、`symbolType` 為空且非
  pre-listing 的標準永續合約。xStocks、ETF、商品、innovation、USDC 與
  inverse 合約不混入同一個帳本。
- 決策截止為每日 `00:00 UTC`。日期 D 的輸入只使用
  `D-1 00:00` 至 `D-1 23:59` 共 1,440 根完整 1m bar；部位可跨日持有。
- 訓練樣本與模型決策頻率都是 `daily`，固定 `lookback=32` 個完整日。
  1m/15m 僅為因果日特徵的原始聚合資料，不會建立盤中訓練樣本或盤中調倉。
- 特徵完整性與執行可得性分開。若前一日少任何 1m bar，該合約的新策略
  target 會保持為零，直到 32 日 lookback 及差分所需的前置日全部完整；但只要
  官方 00:05 execution open 存在，該列仍保留給既有部位估值與減倉，不能因
  特徵缺口假裝市場無法平倉。
- 模型在 D 00:00 建立 signed target weight，明確保留五分鐘運算／送單延遲；
  executor 到 D 00:05 才使用官方 1m Kline open 作 next-trade 研究估值，持有到
  下一日 00:05 再調整。該 execution price 不會進入 D 的模型特徵。
  正向為 long、負向為 short，投影在 L1 ball 內，所以 gross exposure 不超過
  1，且可以保留現金。
- 每次實際買入或賣出名目金額收取 `0.00055` taker fee。正反向換倉按完整
  close + open turnover 收費，不以淨方向省略其中一邊。
- 訓練使用連續權重與官方 completed-session USDT turnover 的 1% 成交量上限。
  panel 以 `turnover / execution_price` 接入既有數量型容量 ledger，乘回執行價後
  恰好還原官方 notional，而不是用全天均價近似。instrument receipt 另保存 tick size、qty
  step、min qty、min notional、max market qty 與 max leverage；目前尚未把這些
  離散限制變成最終下單 oracle，因此這是研究回測，不是可直接送單的結果。

## 資金費與跨日漂移

若 00:05 execution open 為 `P0`、下一日 execution open 為 `P1`，期間資金費事件 k 的官方費率
與 mark 為 `f_k, M_k`，每一單位起始 NAV 的 long 合約報酬係數為：

```text
r_price   = P1 / P0 - 1
c_funding = sum_k (M_k / P0) * f_k
r_effective = r_price - c_funding
```

signed weight `w` 的毛 PnL 是 `w * r_effective`。所以正資金費時 long 付款、
short 收款。資金費是現金流，不會改變合約數量；日末 risky notional 必須用
`r_price` 漂移，而 NAV 用 `r_effective` 與交易費更新：

```text
wealth = 1 + w * r_effective - fees
w_next_before_trade = w * (1 + r_price) / wealth
```

executor 將這兩條路徑分開；活躍部位若缺任一條估值資料會進入 absorbing
failure，而不會把缺值補成零報酬。

Funding history 逐事件抓取；event mark 以 Bybit 官方 hourly mark-price Kline
在 funding timestamp 的 open 表示。若剛上市時第一個 funding event 尚無 mark，
只隔離連續的資料頭並把 coverage start 往後移；任何內部 mark 缺口仍直接失敗。

## 公開資料與因果邊界

- Binance 既有 15m 歷史資料會重新切成 `D-1 00:00 < available_at <= D
  00:00` 的 96 根完整 bar，再聚合成一列日特徵；不會建立 15 分鐘訓練樣本。輸入包含 mark/index
  return 與 range、兩種 basis、已結算 funding、成交量、trade count、taker
  imbalance，以及資料完整時才開啟的 OI/全市場與大戶多空比。每個 15m bar
  一律等到 `bar open + 15m` 才可見，因此 00:00 才公布但被 15m 歷史表表示在
  00:00 bar 的外部事件，會保守地延到 00:15，而不假設 00:00 已知。
- OKX 已有歷史 mark/index、basis、funding、open interest、taker flow 與
  long/short ratio 依相同幣別映射到 Bybit。每根來源 bar 的可得時間定義為
  `bar open + inferred interval`，只有完整 24 小時 session 才進入該日特徵。
- Bybit 自身已結算的前一日 funding rate sum、年化值、實際 mark-notional
  現金係數、最後費率、資料年齡與事件數在下一個決策日才成為模型特徵；
  當期尚未發生的 funding 只屬 executor label，不會回灌模型。
- `data_free_public/observations.parquet` 的 fear/greed、DefiLlama、Bitcoin
  mempool、Hyperliquid 等資料同時要求 `available_at_utc <= decision cutoff`，
  且 event timestamp 不晚於 cutoff。即使檔案含 2018 年事件，首次在本機觀測
  到的 2026-08-16 以前一律不回填。
- 每個稀疏資料族都有 availability feature；缺值才可 zero-fill，不把真正的
  零和未觀測混為一談。
- Coin Metrics 的 latest-view 雖有 2009 年起的事件日期，canonical vintage
  是到 2026-08 才首次在本機觀測；ETF/SEC/issuer 與 crypto-reference 也需要
  各自的發布時間模型，Dune 最新 receipt 則是 credits exhausted、零個到期
  partition 完成。這些來源目前明確列在 public summary 的 excluded 決策中，
  不會為了增加欄位數而倒灌歷史。

## 多基底模型

設定繼承 canonical FinancialTransformer 訓練生命週期、BF16、DDP、
panel-history walk-forward 與 1,000 epochs。32 個已觀測日同時投影到 18 個
固定/可學習 causal basis family：Haar、SWT db2、SWT sym4、wavelet packet、
Walsh、Fourier、DCT、DPSS、local cosine、Morlet、exponential、Laguerre、
difference、AR innovation、B-spline、Legendre、Chebyshev、learned。

所有係數與普通特徵串接後只通過同一個 RMSNorm/feature projection；沒有另設
gate、fusion 或殘差能量捷徑。輸出使用 `projection_l1`，並按 active symbol
count 縮放，保留共同多空方向與合法現金。目前輸入契約共 80 欄：15 個完整
Bybit session K 線特徵、7 個 Bybit funding 特徵、21 個 Binance、20 個 OKX
（包含 funding／positioning／taker 可用旗標）與 17 個
prospective/free-public 特徵；來源時間戳保留在 parquet 供稽核但不餵入
模型。

## 重建命令

```bash
source scripts/runtime_env.sh
run_fintech_python downloader/repair_bybit_1m_gaps.py --workers 96
run_fintech_python downloader/download_bybit_funding_history.py \
  --workers 16 --start-date 2019-01-01
run_fintech_python downloader/materialize_bybit_perpetual_daily.py --workers 12
run_fintech_python scripts/build_bybit_crypto_public_daily_features.py
run_fintech_python train.py \
  --config configs/markets/bybit_perpetual_daily_multi_basis_projection_l1.yaml
```

權威驗收檔為 `funding_coverage.csv`、`funding_summary.json`、
`materialize_report.csv`、`materialize_summary.json`、public feature coverage/summary
與訓練 artifact。不要用程序仍在執行或單一 parquet 存在代替 coverage 驗收。

## Executor 效能驗證

RTX 5070 Ti、PyTorch 2.12.1、`T=128, S=396` 的可重現 recurrent
forward+backward 實測：eager 七次中位 `995.91ms`；已有持久化 Inductor cache
時，固定 4 日 CUDA block 首次呼叫 `4.080s`，七次穩態中位 `44.08ms`，約
`22.6x` speedup。冷 cache 仍需另付編譯成本，不能把穩態數字當第一次啟動
latency。與 eager 的最大
simple-return、final-weight、action-gradient 絕對差分別為 `3.49e-10`、
`4.66e-10`、`5.82e-11`。完整 receipt 在
`artifacts/benchmarks/bybit_crypto_perpetual_executor.json`，可用
`scripts/benchmark_crypto_perpetual_executor.py` 重跑。
CUDA graph 關閉，避免 recurrent output overwrite；非 4 的尾段仍走同一個 eager
日核心，不另建報酬公式。

## 已知研究限制

- universe 是目前仍交易的 instrument snapshot，未恢復已下架合約的歷史成分，
  因而仍有 current-universe survivorship bias。
- gross exposure 限 1 且不借入額外槓桿；ledger 在 NAV 歸零時吸收失敗，但尚未
  重建 Bybit 帳戶模式、maintenance margin、risk tier 與 mark-price liquidation。
- 尚無 qty-step/min-notional 精確下單 rounding、order-book slippage、maker fill
  機率與 latency replay。這些是上線前的獨立 executor 工作，不能由連續研究
  回測的高績效推論為已解決。
- 00:05 Kline open 位於 00:00 決策截止後五分鐘，是因果正確的 next-trade
  研究價，但仍不是指定帳戶在真實
  order book 的保證成交價；上線前必須用 bid/ask、深度與延遲 replay 取代。
