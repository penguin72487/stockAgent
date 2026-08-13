# 台指期／台指選日內多基底策略 v5

## 第一性契約

股票是訊號來源，不是下單標的。日期 `t` 的模型只讀到 `t-1` 已完成的
全市場股票與特徵；實際交易軸固定為 `[B,4102]`：

1. 期貨 `E1..E6` 六個相對到期月份，可多可空；整數執行再用 TX、MTX、
   TMF（大台、小台、微台）組合成最接近目標曝險的口數。
2. 4,096 個日期內 packed TXO 候選槽。候選只來自前一交易日已知的合約，
   包含月選 `E1..E5`、週三週選 `E1..E3`、週五週選 `E1..E3` 的所有
   ITM／ATM／OTM Call/Put；沒有資料的槽保持 mask。
3. 隱含現金槽。期貨 signed raw actions 與選擇權 non-negative raw actions
   共同投影到半徑 `0.98` 的 L1 ball；未使用的半徑就是現金。

模型不再使用 futures/options/cash 三槽 softmax。期貨六個 raw scores 保留正負，
選擇權 raw scores 先限制為非負，再與當日 mask 一起做 `projection_l1`。這與
day-trade FinancialTransformer 的「球內保留、球外軟閾值投影」語義一致，且
4,096 個候選會在超出 gross 上限時由投影產生精確零權重。

候選不是永久合約 ID。每腿使用族群、到期排名、DTE、相對前一日 TX
收盤的 log-moneyness、Call/Put、前收權利金與前一日成交量等標準化經濟
特徵，由共享 scorer 評分。模型不建立 `[股票, 選擇權]` 笛卡兒積，也不再
為歷史上出現過的 7,000 多個身分永久輸出再 mask。

目前 2001--2026-08-11 真實資料稽核為：最大前日已知候選 3,498 腿、最大
當日可執行 1,224 腿；最新一日為 3,070／840 腿。4,096 是固定編譯 envelope，
不是只交易 4,096 腿或只交易 ATM。

到期日可以交易，但每天收盤前全部平倉。下一交易日才依新的 E1/E2…映射
重新開倉，因此換月或換約絕不把舊部位直接改名帶過去。

## 報酬與成本

- 訓練、驗證、測試共用 canonical tensor backtest；整數稽核使用同一個
  日期內合約映射。
- TX benchmark 是前月 TX 一倍做多並持續換月到最新資料。
- 本金為 TWD 100,000,000。
- 每口每邊費用：TX 60、MTX 24、TMF 16、TXO 22 元；日內進出各收一次。
- 稅率 `0.0002`，只在賣出腿收取；TXO 另含每邊 0.5 點滑價。
- TXO 標籤保存未截斷的費用後 simple return。便宜 OTM 權利金可能因固定
  費用而低於 `-100%`；只有整體 NAV 可以在破產時截到 `-100%`，之後進入
  吸收狀態。這避免逐腿 `log1p` 截斷造成免費下檔保護。
- 官方 TXO 日資料是第一筆／最後一筆成交 proxy，不是同步 bid/ask；報告
  必須保留此限制。

FinancialTransformer 的 18 個時序基底家族、每族 4 個係數仍是普通輸入，
與原始股票特徵共用 RMSNorm/CandleEncoder 投影。共同輸入寬度為
`F * (1 + 18*4)`，沒有另一個 basis gate 或 encoder。

時間路徑與 day-trade baseline 對齊為 `temporal_pooling: last` 與
`temporal_query_mode: last_only`。

## 建立資料

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/download_taifex_option_daily_history.py \
  --series-scope all \
  --start-year 2001 \
  --output-dir data_tw_index_options_daily \
  --futures-path data_tw_index_futures/day_session_contracts.parquet
```

主要輸入為：

- `data_tw_index_futures/day_session_contracts.parquet`
- `data_tw_index_options_daily/monthly_full_chain.parquet`
- `data_tw_index_options_daily/weekly_full_chain.parquet`

## 雙 RTX 5090 正式訓練

```bash
scripts/run_tw_index_derivatives_day_multi_basis_dual_5090.sh
```

設定保留 1,000 epochs、global batch 128、BF16 AMP、DDP、torch.compile，輸出：

```text
artifacts/markets/tw_index_derivatives_day_multi_basis_100m_relative_tenor_v5_dual5090
```

只做一個 fold／一個 epoch 的隔離 smoke：

```bash
scripts/run_tw_index_derivatives_day_multi_basis_dual_5090.sh \
  --max-folds 1 --epochs 1 --no-resume \
  --output-dir artifacts/smoke/tw_index_derivatives_day_relative_tenor_v5
```

v5 保留 v4 的 4,102 軸候選與 executor 映射，但時間池化與模型 allocation head
已改為 `last + last_only + projection_l1`，因此使用全新 artifact root，不會
續接 v4 optimizer state。

## 計算效率

評估固定使用 `eval_model_chunk_rows: 128`。最後不足 128 列的區段會在
panel-slab 尾端補上不可見的合成列，繼續使用同一個已編譯 fixed-shape
forward，之後只保留真實日期的輸出。補列不會進入 loss、報酬、費用或持倉
狀態，也不會改變任何真實日期的 lookback window。

雙 RTX 5090、Fold 1、244 列 validation／sampled-test 的實測：修正前兩個
epoch 分別為 29.84 秒與 26.54 秒；修正後為 4.09 秒與 0.62 秒。第一個
epoch 仍包含 eval graph warmup，第二個 epoch 是零新 graph 的穩態。完整
2,578 列 final test 與 lifecycle artifact gate 亦已跑完。

## v5 第一性診斷與 cash-Entmax v7

正式 12-fold v5 的 isolated-child root report 曾被最後一個 fold 覆寫，只剩
2026 年 140 列。Parent 現在會在所有子程序完成後，以 12 個 fold 的原始
`requested_weights_history` 重建一個不重設本金的 2,578 日帳戶。修正後
2016-01-04 至 2026-08-04 的 canonical 結果為策略 `-98.49%`、TX rolling
benchmark `+692.85%`；直接 integer replay 與儲存日報酬最大差 `8.75e-8`。

反證顯示將費用、稅與滑價全部設為零仍為 `-96.66%`，因此主要問題不是成本
設定，而是訊號與 checkpoint selection 沒有 out-of-sample 正期望。原 v5
在 100% 日期都把 requested gross 推到 `0.98`，平均只留下 1.62 腿，最大腿
平均占 87.4%。期權雖只在 15 日被選中，但那些日期複利 `-93.86%`。

新設定
`configs/markets/tw_index_derivatives_day_multi_basis_cash_entmax_v7.yaml`
保留 `last + last_only`，但把全域 `projection_l1` 換成既有
`cash_entmax15` 的無額外參數現金保留配置器：

- Entmax-1.5 先以合法方向的分數絕對值，決定 futures-long／short 與
  option-long／short 各腿的相對風險配置；
- 每個被選中腿的投入強度直接使用自身分數的 `|s|/(1+|s|)`；零證據就是
  100% 現金，單一強訊號不會再被數千個弱候選腿取平均而稀釋；
- 不建立可學習 capital gate，也沒有獨立 option gross cap；
- Entmax 可把低證據風險 actions 精確壓成零；輸出再乘原本帳戶層級的
  0.98 上限，保留至少 2% 給整數張數與費用，其餘未用曝險也是現金；
- option-long 的 action 是權利金資本比例；option-short 的 action 是原始
  保證金資本比例。空方逐腿使用
  `權利金市值 + max(A - 價外值, B)`，2026-08-12 快照為原始保證金
  `A=187,000`、`B=94,000`、`C=18,800` 元；週契約與月契約相同；
- 價外值使用收據驗證的官方 TAIEX 同日開盤指數；不以 TX 開盤價代替，因為
  期現貨基差可能使價外值扣除過大而低估保證金。缺少任何同日指數即 fail closed；
- 目前採保守逐腿裸賣，不套用同到期價差、跨／勒式、時間價差或 TX/MTX/TMF
  組合折抵，因此不會低估保證金，但可能低估可交易口數。C 值只被記錄在
  checkpoint contract，尚未用於折抵；
- 這組 A/B/C 是「依最新規則重算歷史樣本」的 dated counterfactual，不是假裝
  2016--2026 每天真的使用 2026-08-12 金額。正式歷史制度回測仍需要補齊
  每次 TAIFEX 調整的 point-in-time margin schedule；
- v5 的 `projection_l1` 路徑仍保留供舊 checkpoint 重現；v7 使用 fresh
  artifact root。舊 `gated_v6` 設定路徑只是 v7 的 compatibility alias。

正式訓練指令：

```bash
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_index_derivatives_day_multi_basis_cash_entmax_v7.yaml
```

v7 是 allocator 與可 abstain 契約的修正，不是已證明獲利的結果。下一次正式比較
必須用多區塊／nested temporal selection、cash baseline、worst-year guardrail
及同步 opening ask／closing bid 資料；不得再用單一年 best validation 宣稱
可部署。

另需注意：目前 `0.0002` 且僅賣出時計稅是依先前指定保留的研究設定；TAIFEX
現行公開費率表列 TXO 權利金交易稅率為 `0.001`。正式可交易性驗證應另開新
成本契約處理，而不是讓舊 checkpoint 靜默改語意。

官方規則來源：

- [TAIFEX 股票指數類保證金一覽表](https://www.taifex.com.tw/cht/5/indexMarging?menuid1=12)
- [TAIFEX 股價指數選擇權各種部位組合保證金公式](https://www.taifex.com.tw/cht/5/margingReqIndexOpt?menuid1=12)
