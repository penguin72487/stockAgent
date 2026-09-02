# 台股日當沖分鐘級紙上執行契約

這份文件描述 `stockagent.live.tw_day_trade_simulation` schema 3，以及
`configs/markets/tw_day_trade_1m_realistic.yaml` 的 hybrid loss，以及
`configs/markets/tw_day_trade_1m_strict_exact_2020.yaml` 的 strict-minute loss。
紙上交易不會呼叫券商下單 API；新訓練契約使用獨立 artifact root，舊日頻
checkpoint 不可續訓。

## 每日模型的訓練 loss

模型仍然每天只輸出一次 signed target weights，不是每分鐘重新決策。執行
label 由 `stockagent.data.tw_day_trade_execution` 壓成 `[日期, 股票, 23]`：

- 官方開盤價只負責把權重換算成整張股數；09:01 第一根完成 K 的
  `Amount / volume_shares` VWAP 才是進場成交價。
- 每次成交容量都是 `floor(50% * 該分鐘成交股數 / 1000) * 1000`。
- 13:20 以已完成 K 的收盤價掛被動限價；13:21--13:23 必須嚴格穿價才
  成交，只有碰價不假設排隊成交。
- 13:24 以該分鐘 VWAP 市價清倉；13:30 以收盤集合競價價及該次量能清倉。
- 13:30 殘部依下節「無違約」研究假設，以正式收盤價做當日會計結清；融資
  與融券額度視為無限，只改收融資融券費用。股票部位當日歸零，但買賣互抵
  後的淨差額必須進 T+2 claim queue。缺正式收盤價的殘部採進場名目全損，
  不可免費沿用 stale mark。
- forward 使用精確 1,000 股整張與真實 fail-closed 結果；只有 backward
  對不可微的取整使用 straight-through estimator。報表損益不使用代理值。

歷史完整市場只有分鐘 K、沒有同期間完整 L1 委託簿，因此訓練 loss 不會
虛構第一檔深度或排隊順位。即時紙上執行則額外取 `min(L1, 50% minute K)`；
兩者的差異屬於資料可觀測性，不得將分鐘 K 回測宣稱為逐筆委託簿重播。

訓練 panel 從 2014-01-06 開始。在第一個已驗證分鐘 partition
2020-03-02 之前，以方向別壓力代理作為可稽核的降階資料：多單
`open + 1 tick` / `close - 1 tick`，空單 `open - 1 tick` /
`close + 1 tick`；代理單分鐘量為當日量除以 271，再依相同 50%
參與率與整張規則限制。從 2020-03-02 開始一律只接受真實分鐘資料；
任何缺 partition 的日期 fail closed，不可偷退回日 K 代理。執行
tape 會按來源 manifest、日期、symbols 與官方開收盤／成交量指紋快取。

strict v9 不使用上述 2014--2020 代理。它從第一個 canonical partition
`2020-03-02` 開始，並設定
`day_trade_minute_execution_allow_daily_proxy: false`；只要 panel 仍含任何
早於此日的 row，loader 會在建 cache／模型前失敗。2020-03-02 以後缺少的
partition、股票、價或量仍各自 fail closed，不會補成日 K。分鐘 tape 內容
本身納入 checkpoint fingerprint，所以換分鐘標籤不可沿用舊 optimizer。

## 模型輸入 feature

全特徵契約實際輸入 99 欄：15 個 OHLCV／蠟燭基礎特徵、83 個
TW-public 歷史特徵，以及當日已觀察的開盤 gap。設定的
`feature_exclude` 為空，所以融資融券、法人、股利／除權息、事件、
匯率、總經與 TAIFEX 等已列入的歷史欄位都會進入 FinancialTransformer。
`temporal_basis_input: input_features` 會對這些輸入全部做多基底處理，不是
只對 OHLCV 處理。只有目前快照而沒有不可變 point-in-time vintage 的
33 個 schema 欄位不得輸入；config loader 會直接拒絕，以防止把現在的
資訊倒灌到 2014。成交分鐘價、當日收盤結果和 label 只供 loss／執行器使用，
不是模型 feature。

strict v9 先以 fold-training-only、零值保持為零的 causal RMS 做尺度化，
再讓全部 99 個連續欄位進入 learned `99 -> 32` bottleneck。22 個 temporal
basis family、共 524 個方向都作用於這 32 維 learned mixture；它不是手動
選特徵。forward/backward smoke 已確認 99 個輸入欄都連到 loss。輸出改用
`executable_portfolio_transformer`：只加入開盤時已知的方向資格與前一交易日
量能容量，禁止 same-session close／volume 洩漏；projection-L1 不去均值、
以 active-count 尺度投影，因此可以合法保留現金而非被迫 gross=1。

## 訓練指令與計算極限

推薦的 strict-minute 正式訓練仍是每日決策模型，不是分鐘決策模型：

```bash
cd /path/to/stockAgent
./scripts/run_tw_day_trade_1m_strict_exact_2020_dual_5090.sh
```

設定保留 1,000 epochs、global train/eval batch 64/32、BF16 model AMP、雙卡
DDP，以及 train／validation／test 共用的 exact-minute loss。事件 loss
將四個連續交易日編成固定 CUDA 核心；批次尾端重複最後一列並以
`state_advance=false` 屏蔽，不改變 forward、NAV recurrence 或 action
gradient。由於 strict tape、模型 head、99-feature bottleneck 與舊 checkpoint
ABI 不相容，新訓練使用獨立 `all_features_bottleneck32_v9` artifact root，
不覆寫或續接 hybrid v8／日頻結果。

64 是 power-of-two 的安全起點，不宣稱跨機器理論最佳。要在目前雙 5090
機器以完整 epoch 吞吐、非零梯度、無 fallback 與 VRAM headroom 選擇
32/64/128，可由使用者另外執行：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_day_trade_batch_sizes.py \
  --config configs/markets/tw_day_trade_1m_strict_exact_2020.yaml \
  --batch-sizes 32,64,128 --batch-size-eval 32 --start-fold 5 \
  --multi-gpu-strategy distributed_data_parallel \
  --cuda-visible-devices 0,1 --cpu-threads 14 --compile-threads 8
```

這個 benchmark 使用獨立 artifact root，不會開始或污染正式 v9 root。

最終 `test_backtest.npz`、`test_integer_share_backtest.npz`、年度報表與 console
的 `exact-minute` 指標都直接來自同一分鐘事件 forward；不再以舊的
open-to-close 整日 audit 覆寫。1 epoch smoke 只驗證執行與 artifact，不代表
策略已收斂或有投資效益。

## 時間與訂單狀態機

| 台北時間 | 行為 | 成交證據 |
|---|---|---|
| 09:00 | 接收使用當日開盤資訊產生的訊號，但不成交 | 僅保存不可變訊號 |
| 09:01 起 | 以官方開盤價計算股數，以第一根完成分鐘 K 後的最佳賣／買價模擬買進／放空 | 同時受第一檔深度及該分鐘 K 成交量 50% 約束 |
| 09:01–13:19 | 停利、停損仍以可執行最佳價與同一分鐘量能約束 | 缺價或缺量為零成交 |
| 13:20–13:23 | 撤換成被動最佳價限價單 | 只有對手價穿越限價才可能成交 |
| 13:24 | 撤單後在連續交易時段送市價清倉 | 最佳價、第一檔深度及分鐘量能 50% |
| 13:25–13:29 | 收盤集合競價只允許 Limit ROD；多單賣出掛跌停，空單回補掛漲停 | 指示價不視為成交 |
| 13:30 | 以收盤撮合價結算，最多使用收盤集合競價分鐘量的 50% | 無收盤價或無成交量即不成交 |
| 13:30 後 | 殘部假設可無限轉融資／融券，按正式收盤價做當日會計結清 | 補收一般交易費稅與最高壓力費率；不保留部位、不產生違約狀態 |

每個一般交易分鐘的容量為

`floor(0.5 * minute_volume_lots) * board_lot_size`，

並再與可見第一檔數量取最小值。13:30 集合競價沒有可驗證的連續交易第一
檔排隊成交，因此只用該次集合競價實際成交量的 50%。累積成交量只有在相鄰
分鐘快照間才可相減；服務漏抓一分鐘時容量直接歸零，不能把數分鐘成交量冒充
成單分鐘深度。

## 升降單位與漲跌停

- 普通股票的日期化升降單位以 2005-03-01 為切換點。舊制為
  `<5:0.01, 5–15:0.05, 15–50:0.1, 50–150:0.5, 150–1000:1,
  >=1000:5`；現制為 `<10:0.01, 10–50:0.05, 50–100:0.1,
  100–500:0.5, 500–1000:1, >=1000:5`。
- 普通股票漲跌幅在 2015-06-01 由 7% 改為 10%。漲停價向下取至合法
  tick，跌停價向上取至合法 tick，確保不超出法定百分比。
- 即時執行優先使用交易所準備的上下限。缺上下限時，只可由同日官方「開盤
  競價基準」推算；不得用昨收或模型 panel fallback 代替。缺官方基準即
  fail closed。這避免除權息、減資及其他基準價調整日產生錯誤限制價。
- 初次上市普通股前五個交易日可能沒有漲跌幅限制。若即時來源未提供可驗證的
  官方上下限，本模擬不交易，不自行套用 10%。

## 13:30 殘部、無違約假設與壓力費率

這個版本不保留跨日股票部位，也不模擬追繳或斷頭。13:30 殘部一律假設券商
接受改列，資金與券源沒有容量上限；當日 loss 仍以正式收盤價完成經濟損益
與 turnover，下一交易日由股票空倉開始。買賣相同數量互抵後只留下含費稅的
淨現金差額：T 日收盤建立應收／應付，T+1 仍待結算，T+2 當天開盤不得用，
T+2 收盤後才進現金，因此下一次模型真正可用於下單 sizing 是 T+3 開盤。
經濟 NAV 在 T 日包含該 claim，但 deployable cash 不包含，兩者不可混用。

多單殘部使用 60% 融資比例，計一日 `16% / 365` 的融資利息，並按一般交易
費稅計入會計結清。16% 是保守壓力假設：融資利率由券商自訂，並不存在所有
券商共用的交易所最高牌告利率。

先賣後買殘部改按一般融券，不再誤用「應付當沖券差」單日 7% 借券費。模型
採公開資料上緣：一次融券手續費 0.10%，另計最高年率 20% 的一天融券費；
原當沖賣出稅與一般融券賣出稅的差額會補收，會計回補另收買進手續費。ETF
依其一般與當沖稅率差計算，不硬套股票的 0.15% 稅差。

## 依據與資料驗證

- [TWSE 集中市場交易制度](https://www.twse.com.tw/zh/products/system/trading.html)：升降單位、漲跌停取整、09:00–13:25 連續交易與 13:25–13:30 集合競價訂單限制。
- [TWSE 當日沖銷交易專區](https://www.twse.com.tw/zh/products/system/day-trading.html)：應付當沖券差、7% 借券費上限與 T+1 強制買回流程。
- [TPEx 當日沖銷制度](https://www.tpex.org.tw/zh-tw/mainboard/trading/day-trading/rules.html)：未完成沖銷及 T 日 18:00 前更改交易類別規則。
- [財政部證券交易稅條例第 2-2 條](https://law-out.mof.gov.tw/LawContent.aspx?id=FL006079)：現股當沖股票賣出稅率 0.15% 的適用期限。
- [Shioaji 股票即時行情](https://sinotrade.github.io/tutor/market_data/streaming/stocks/)：普通交易 `total_volume` 的單位為張；盤中零股才以股計。

本機 `2026-07-27` 研究快照已通過官方 audit：211,981 列、2,284 檔。
以 2330 為例，09:01 第一根 K 為 2,497 張；13:20–13:25 分別為
10、399、59、446、52、211 張；13:30 集合競價 K 為 4,842 張，且
13:26–13:29 沒有虛構的一分鐘連續撮合列。這與上述收盤狀態機一致。

## 已知限制

- 快照差分只能近似完整分鐘 K；正式下單前應改用即時訂閱並保存逐筆成交與
  訂單簿事件，才能估排隊順位、撤單及滑價。
- 目前訓練契約不保存殘部股票，但會跨 batch 保存 T+2 淨差額 queue；在無限
  融資假設下 `settlement_default` 維持 false。真實帳戶仍需確認券商契約、
  融資融券資格與 T 日申報結果；這個簡化不可用來估計追繳或斷頭風險。
- 新訓練 loss 已包含 09:01、13:20--13:24 與 13:30 狀態；但沒有歷史
  L1，所以不估計真實排隊順位、撤單或超過分鐘 VWAP 的市場衝擊。
