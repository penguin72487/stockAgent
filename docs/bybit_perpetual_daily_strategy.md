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
  官方 00:00 execution open 存在，該列仍保留給既有部位估值與減倉，不能因
  特徵缺口假裝市場無法平倉。
- 模型在 D 00:00 建立 signed target weight；executor 同樣以 D 00:00 的官方
  1m Kline open 作零延遲 counterfactual 估值，持有到下一日 00:00 再調整。
  該 execution price 不會進入 D 的模型特徵，也不代表真實送單可在原地成交。
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

若 00:00 execution open 為 `P0`、下一日 execution open 為 `P1`，期間資金費事件 k 的官方費率
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
  mempool、Blockscout、Hyperliquid 等資料同時要求
  `available_at_utc <= decision cutoff`，
  且 event timestamp 不晚於 cutoff。即使檔案含 2018 年事件，首次在本機觀測
  到的 2026-08-16 以前一律不回填。
- DefiLlama 額外提供跨鏈 TVL、stablecoin circulating supply 與 yield-pool
  TVL；Blockscout 提供 Ethereum gas 與 network utilization；mempool.space
  提供 Bitcoin fee、mempool、difficulty 與 hashrate。這些都是網站／鏈上公開
  資料，不是 Bybit、Binance 或 OKX 交易所欄位，但目前本機資料多為 2026-08
  才開始保存的 snapshot，所以只具有 prospective 意義。
- FRED 只下載 `output_type=4` 的 initial-release vintage，包含政策利率、SOFR、
  2Y/10Y、公債曲線、廣義美元、VIX、高收益利差、Fed 資產負債表、reverse repo
  與 NFCI。API 只給 release date、不給日內發布時間，因此一律等到隔日
  `00:00 UTC` 才可用；later revision 不會覆蓋舊決策。
- SEC crypto-ETF filings 使用 EDGAR `acceptance_datetime`；盤中受理的申報到
  下一個 UTC 午夜才進入 1/7/30 日 filing intensity。SEC 實體集合來自目前的
  crypto-ETF registry，summary 會明列 survivorship-selection risk。
- Coin Metrics 只讀 per-asset canonical retrieval vintage，並限制到明確的原生
  asset-id 白名單；2009 年起的 latest-view history 不會倒填。CoinGecko global
  與 market snapshots、ETF 發行商 holdings／reserve 也只在首次
  `available_at` 後使用。CoinGecko 同代號若最大市值資產占比低於 90%，映射
  直接失敗而不猜測。
- 每個稀疏資料族都有 availability feature；資料準備保留缺值與可用性。
  注意設定中的 `feature_zero_fill` 是「整個匹配欄位歸零」，不是只補缺值。
  現行 deterministic 與 trajectory 設定將 126 個外部欄位全部停用，實際啟用
  的是 15 個 Bybit K 線欄位；不能把 141 欄 ABI 說成 141 個有效訊號。
- Dune 留存結果的歷史 event date 早於 2026-08 retrieval completion，最新抓取
  又受 credits 阻擋；依它自己的因果契約仍排除，不會為了增加欄位數而倒灌。

## 多基底模型

設定繼承 canonical FinancialTransformer 訓練生命週期、BF16、DDP、
panel-history walk-forward 與 1,000 epochs。原始 18-family 設定把 32 個已觀測日投影到 18 個
固定/可學習 causal basis family：Haar、SWT db2、SWT sym4、wavelet packet、
Walsh、Fourier、DCT、DPSS、local cosine、Morlet、exponential、Laguerre、
difference、AR innovation、B-spline、Legendre、Chebyshev、learned。

所有係數與普通特徵串接後只通過同一個 RMSNorm/feature projection；沒有另設
gate、fusion 或殘差能量捷徑。輸出使用 `projection_l1`，並按 active symbol
count 縮放，保留共同多空方向與合法現金。目前輸入契約共 141 欄：15 個完整
Bybit session K 線特徵、7 個 Bybit funding 特徵、21 個 Binance、20 個 OKX
（包含 funding／positioning／taker 可用旗標）與 78 個交易所外公開資料特徵；
來源時間戳與來源風險保留在 summary／quality receipt 供稽核，不餵入模型。

現行 `bybit_perpetual_daily_0000_deterministic.yaml` 及下述 trajectory 設定
繼承 22-family、524-component 版本，另含 Kautz、discrete Hermite、chirplet
與 training-only PCA/KLT。基底數增加不等於獨立歷史資訊增加；不能直接推論
台股同架構的報酬會在加密貨幣重現。上述 126 個停用欄位仍保留 ABI，這次
不混入外部特徵啟用或正規化實驗。

## 績效診斷與待驗證修正

2026-09-06 讀取的控制組為
`artifacts/markets/bybit_perpetual_daily_0000_deterministic_v1`。
`summary.json` 只有兩個完成 fold，第三折曲線未完成；不是完整 walk-forward
驗收。以下數字來自完成 fold 的 `test_backtest.npz`，逐日複利重算與 summary 相符。

| 測試區間 | 扣費累積報酬 | 最大回撤 | 日均換手 / NAV | 固定交易路徑的年化對數費用拖累 |
| --- | ---: | ---: | ---: | ---: |
| Fold 1：2022-01-01 至 2026-08-19 | -63.79% | -69.40% | 104.67% | 21.03 個百分點 |
| Fold 2：2023-01-01 至 2026-08-19 | +27.83% | -47.89% | 120.84% | 24.25 個百分點 |

兩折測試年重疊，不能直接相加。既有 `walkforward_deployment_annual_report.txt`
只串接已完成的 2022、2023 首年部署路徑，累積 -1.11%、最大回撤 -54.73%。
費用拆解使用儲存的對數報酬 `l_t` 與實際換手 `u_t`：

```text
g_t = log1p(expm1(l_t) + 0.00055 * u_t)
annual_log_fee_drag = 365 * mean(g_t - l_t)
```

這只是固定已成交部位與逐日 NAV 分母下的費用加回，不是真正零費率重跑；
費率改變也會改變 NAV、後續權重與 proximal allocation。Fold 1 即使這樣加回
費用，仍為 -4.02%，因此「費用很重」不能替代「訊號有沒有樣本外優勢」。
第一折驗證年 +160.71%、後續測試期 -63.79% 是泛化落差的證據；期間長短與
市場狀態不同，不能僅憑此證明是哪一項超參數造成過擬合。

本次實作分成確定的帳本修正與待驗證的優化假說：

1. **帳本修正：非真實日期完全不推進狀態。** 舊 crypto kernel 在
   `state_advance_mask=False` 時仍可能用重複的有限報酬更新 NAV，或對漂移超過
   gross cap 的持倉強制減倉。現在假日期的報酬、費用與換手為零，持倉與存活
   狀態原封不動；跨過內部假日期的下一個真實交易也與移除假日期一致。
   尾端 padding 的污染未必改變已排除尾列的績效，不能宣稱它解釋整段虧損。
2. **優化假說：每 epoch 固定一組參數。** 舊設定每 batch 更新一次，前三個
   training group 每 epoch 分別約 16、39、62 次更新；訓練曲線混合不同參數的
   帳戶路徑，與驗證時固定模型的路徑不同。新增
   `training.crypto_optimizer_step_per_trajectory: true`，逐日照常估值、付費、
   持倉，每個 chunk 的 log-utility 梯度按有效日期數加權，走完全部訓練日期才
   clip、更新 optimizer 一次。step scheduler 也只推進一次。
   持倉在 batch 邊界仍 detach，是截斷梯度，不是全歷史 BPTT；收益改善待實測。
3. **可稽核與相容性。** epoch curve 新增 `train_optimizer_steps`；trajectory
   模式每個完整 epoch 應為 1。任一 chunk 的 loss/gradient 非有限即整段失敗，
   不跳過壞批次後繼續更新。crypto ledger v2 與 allocator/執行時刻列入
   checkpoint trading fingerprint，更新頻率列入 training fingerprint。
   舊 checkpoint 不可 resume 或覆寫重生 canonical artifacts；模型權重的獨立
   inference 相容性不是舊帳本相容性。

設定仍是 1,000 epochs 上限並保留原 early stopping，不把每 epoch 一次更新
假裝成與原本相同的 optimizer 更新預算。此次沒有增加槓桿、人工 top-K、
換手懲罰、特徵或降低費率，也沒有啟動正式訓練。

### 由使用者執行的新訓練

沿用 `data_bybit/perpetual_daily_0000`、原 public feature 表及原 panel cache。
不需重跑下面的歷史重建命令，不新增 snapshot 或資料副本。
新輸出根僅隔離不相容的訓練／帳本版本：
`artifacts/markets/bybit_perpetual_daily_0000_trajectory_v1`。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/markets/bybit_perpetual_daily_0000_trajectory.yaml \
  --no-resume
```

`--no-resume` 只用於此新根的首次執行；同版本中斷續跑時移除它，不能拿舊根
的 checkpoint 接續。不要在外層加 `torchrun`，fold orchestration 自己管理 DDP。
回傳新根的 summary、完成 fold metrics 與 epoch curve 後，再檢查固定模型
樣本外淨報酬、回撤、換手、更新次數與 fold 完成狀態。已看過的測試期間不是
新的盲測；若要歸因 optimizer cadence，需在相同修正版帳本下另做控制比較，
不能把舊帳本到新帳本的全部差異都算成優化效果。

### DDP 重跑時的超大配置錯誤

若在 `resume_epoch_curve_trim` 的 `all_gather_object` 看到
`Tried to allocate more than 1EB memory`，不能直接解讀成模型需要更多 VRAM。
2026-09-08 的現場已有同次嘗試留下的 validation-best 與 epoch-curve 封存檔；
檢查程式發現封存同步被包在 `checkpoint_best_path.exists()` 裡。rank 0 先
搬走檔案後，較慢的 rank 1 會跳過該次同步，讓後續不同階段的通訊錯位。
這是可重現的競態；缺少現場 rank 1 stack，不能宣稱已還原它當時的每一次通訊。

修正後只有 rank 0 檢查／搬移檔案，每個 pending fold 的所有 rank 都參與
封存階段，即使檔案本來不存在；狀態交換另核對 rank 與 phase 名稱。
兩程序測試刻意讓 worker 等到封存完成才到達，並驗證下一個曲線階段與
tensor collective；也驗證封存錯誤和階段不一致能讓兩邊一起失敗。
這是同步修正，沒有改變模型、資料或帳本契約，不需新建 snapshot 或輸出根。

2026-09-08 驗證：一般相關回歸 372 passed；另在雙 RTX 5090、PyTorch
2.11.0+cu128 跑 Gloo/NCCL 各三種通訊情境，共 6 passed。這些測試不載入
策略資料、不建模型、不更新 optimizer，不等於正式訓練完成。
可重跑 `test/test_ddp_restart_coordination.py`；GPU 通訊測試需明確設定
`STOCKAGENT_TEST_NCCL=1`，平常測試預設跳過 NCCL，避免占用訓練 GPU。
PyTorch 的 [object collectives 說明](https://docs.pytorch.org/docs/stable/distributed.html#object-collectives)
說明它會先交換序列化物件大小、再傳送物件；此處的超大配置位於大小處理階段，
不是策略模型前向／反向的工作張量。

封存訊息是保留舊檔的通知，本身不是錯誤，也不一定代表 checkpoint 損毀：
`--no-resume` 會明確要求從 epoch 1 重跑，即使舊 checkpoint 仍可讀。
一般同版本續跑應移除 `--no-resume`；有意重跑第 2 折才使用
`--start-fold 2 --max-folds 1 --no-resume`。重啟前先確認舊 rank 已退出，
不要刪除 `.unresumable_*` 封存證據或同組 `checkpoint_last.pt`。

## 重建命令

正式資料至少需保留約 32 日的 panel 前置期，因此從 `2020-02-24` 開始抓取，
而策略樣本仍由設定的 `panel_start_date: 2020-03-27` 起算。三個交易所的 1m
資料量很大；先確認資料根位於有足夠空間的 canonical producer workspace，且不要
寫入 packed store 或 materialized cache。

```bash
source scripts/runtime_env.sh

run_fintech_python scripts/check_environment.py --require-cuda --strict

run_fintech_python downloader/download_bybit_perp_1m.py \
  --output-dir data_bybit/1m --mode full \
  --start-date 2020-02-24 --end-date today \
  --categories linear --workers 16
run_fintech_python downloader/download_okx_perp_1m.py \
  --output-dir data_okx/1m --mode full \
  --start-date 2020-02-24 --end-date today \
  --workers 16 --feature-workers 16
run_fintech_python downloader/download_binance_perp_1m.py \
  --output-dir data_binance/1m --mode full \
  --start-date 2020-02-24 --end-date today \
  --workers 16 --feature-workers 16

run_fintech_python downloader/download_bybit_funding_history.py \
  --output-dir data_bybit/funding --workers 16 --start-date 2019-01-01
run_fintech_python downloader/repair_bybit_1m_gaps.py --workers 96
run_fintech_python downloader/materialize_bybit_perpetual_daily.py \
  --input-dir data_bybit/1m --funding-dir data_bybit/funding \
  --output-dir data_bybit/perpetual_daily --workers 12

run_fintech_python downloader/download_fred_crypto_macro_vintages.py \
  --start-date 2000-01-01 --end-date today
run_fintech_python scripts/build_bybit_crypto_public_daily_features.py

run_fintech_python train.py \
  --config configs/markets/bybit_perpetual_daily_multi_basis_projection_l1.yaml
```

零延遲研究版必須使用獨立的 v7 日資料與外部 funding slice，不能只把 v6
設定檔的文字改成 00:00：

```bash
run_fintech_python downloader/materialize_bybit_perpetual_daily.py \
  --input-dir data_bybit/1m --funding-dir data_bybit/funding \
  --output-dir data_bybit/perpetual_daily_0000 --workers 12 \
  --execution-minute-utc 0

run_fintech_python scripts/rebase_bybit_funding_feature_slice.py \
  --base-feature-path data_bybit_daily_synced/public_features/bybit_crypto_public_daily.parquet \
  --bybit-daily-dir data_bybit/perpetual_daily_0000 \
  --output-path data_bybit/public_features/bybit_crypto_public_daily_0000.parquet

run_fintech_python train.py \
  --config configs/markets/bybit_perpetual_daily_0000_execution_multi_basis_22_effective_rank_projection_l1_carry.yaml
```

v7 的資訊集合截至 D-1 23:59 UTC，以 D 00:00 Kline open 作理想化原地
成交價；同在 00:00 的 funding 先結算給跨越該邊界的舊倉，再建立新 target。
這是零延遲 counterfactual，不是真實 order-book fill 聲明。

下載器與 materializer 都會留下 summary/report receipt。任何一步失敗時先修復該步
並原命令續跑，不要加 `--refresh` 重抓已驗證的歷史資料。

設定保留標準 DDP。若主機實際只看得到一張 CUDA GPU，啟動命令需追加
`--multi-gpu-strategy none`；不要把單 GPU smoke 的覆寫寫回正式多 GPU設定。

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
- v6 的 00:05 Kline open 位於 00:00 決策截止後五分鐘；v7 則依使用者指定
  採 00:00 零延遲原地執行。兩者都是研究價而非指定帳戶在真實 order book
  的保證成交價；上線前必須用 bid/ask、深度與延遲 replay 取代。
