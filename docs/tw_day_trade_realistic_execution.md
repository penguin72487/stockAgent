# 台股日當沖分鐘級紙上執行契約

這份文件描述 `stockagent.live.tw_day_trade_simulation`，以及
`configs/markets/tw_day_trade_1m_realistic.yaml` 的每日模型分鐘成交 loss。
紙上交易不會呼叫券商下單 API；新訓練契約使用獨立 artifact root，舊日頻
checkpoint 不可續訓。

## Multi-Basis 逐分鐘 100% 成交量模式

`configs/markets/tw_day_trade_daily_multi_basis_projection_l1_minute_volume100_capital10m.yaml`
是獨立的新研究模式。它沿用正式 1,000 萬 Multi-Basis Projection-L1
模型、BF16、費稅、點時資格、T+2 淨額與 walk-forward 契約，只替換
executor：

- 09:00 使用已完成日資料與當日已觀測開盤資訊，凍結一次 signed target
  和官方開盤價換算的整張數；模型不會在盤中重算訊號。
- 同一張進場單依序使用右標 09:01--13:19 K 棒。每分鐘成交量上限為
  `floor(1.0 * volume_shares / 1000) * 1000`；不足部分延續到下一分鐘。
- 13:20 取消尚未成交的進場餘額。退出單從下一個因果可執行的 13:21 K
  棒開始，逐分鐘使用 13:21--13:30；無連續交易的分鐘自然是零量，13:30
  是收盤集合競價。
- 連續市場價格為該分鐘 `Amount / volume_shares` VWAP；13:30 使用正式
  closing-auction close。缺價、缺量或來源單位未通過稽核時，該分鐘容量為
  零，不以 close、昨收或插值補成交。
- 13:30 後仍未退出的部位不冒充市場成交。它沿用本文件既有研究假設，按
  正式收盤價轉融資／融券會計結清、收一般費稅與壓力成本，只把淨差額放入
  T+2 queue。

100% participation 是「我們可以拿到歷史分鐘全部成交量、且自己的訂單不會
改變價格或成交量」的反事實容量上界。當訂單等於市場全部成交量時，這個
price-taking 假設必然忽略市場衝擊、排隊順位與其他交易者反應，因此報表
不可標為真實可成交績效；正式上線前仍須用 Tick/BidAsk 與較低 participation
做壓力測試。

資料不另造下載器，使用 canonical `data_tw_minute/research_dataset`。完整
準備與稽核命令：

```bash
cd /path/to/stockAgent
source scripts/runtime_env.sh

# 若 receipt-backed research dataset 尚未建立：
run_fintech_python scripts/build_shioaji_tw_minute_dataset.py \
  --input-root data_tw_minute/shioaji_1m \
  --output-root data_tw_minute/research_dataset

# 訓練前必須驗證全部 partitions 與 manifest SHA；不得只看 manifest 名稱。
run_fintech_python scripts/audit_shioaji_tw_minute_dataset.py \
  --dataset-root data_tw_minute/research_dataset \
  --all-partitions \
  --output artifacts/validation/tw_day_trade_minute_volume100_data_audit.json
```

2026-09-01 本機已實際完成上述全量稽核：schema 4、1,585 partitions、
309,756,178 列、2020-03-02 至 2026-08-28、最多每日 2,313 檔，
`failures={}`。這是目前可訓練的完整邊界；原始 hot-tail 雖已有較新的局部
資料，未完成全市場重建與 receipt 前不混入正式訓練集。

先跑一個 fold、一個 epoch 的 bounded real-data smoke；目前完整 panel 實測會
建立 9,446,333,848 bytes（約 9.45 GB）的
memory-mapped execution tape cache，來源 manifest、panel 日期、symbols 與
官方開盤價任一改變都會換 cache key：

```bash
CUDA_VISIBLE_DEVICES=0 run_fintech_python train.py \
  --config configs/markets/tw_day_trade_daily_multi_basis_projection_l1_minute_volume100_capital10m.yaml \
  --output-dir artifacts/smoke/tw_day_trade_multi_basis_minute_volume100_fold1 \
  --start-fold 1 --max-folds 1 --epochs 1 --no-resume --profile-timing \
  --multi-gpu-strategy none
```

smoke 與 artifact contract 通過後，正式訓練：

```bash
run_fintech_python train.py \
  --config configs/markets/tw_day_trade_daily_multi_basis_projection_l1_minute_volume100_capital10m.yaml \
  --profile-timing
```

2026-09-01 的單卡 RTX 5070 Ti 實測 smoke 已完成 lifecycle 與九張必要
walk-forward 圖：首次 panel+tape 冷建構 1,098.3 秒，兩個 cache 命中後為
2.8 秒；真實 compiled forward/backward probe 145.6 秒，epoch 1 為 132.3 秒，
GPU 高點約 15.83/16.30 GiB。該單一 epoch 的 turnover 為零，原因是初始
Projection-L1 權重分散到 2,749 檔後多數不足一張；測試已確認 exact-forward
保持零張而 STE gradient 非零。這只證明管線可訓練，不是績效結論。

這個 config 尚未加入 Discord/公開網頁的 enabled markets。必須等訓練完成、
checkpoint/artifact gate 通過並建立獨立 deployment manifest 後才能啟用；不能
讓不存在或未完成的 checkpoint 使現行三個紙上模式降級。

## 既有 50% event-tape 每日模型 loss

既有模式仍然每天只輸出一次 signed target weights，不是每分鐘重新決策。執行
label 由 `stockagent.data.tw_day_trade_execution` 壓成 `[日期, 股票, 17]`：

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

目前稽核資料共有 1,585 個交易日，從 2020-03-02 到 2026-08-28。訓練
panel 因此從 2020-03-02 開始；2014--2020 缺分鐘路徑的日期不會退回舊
open-to-close proxy。第一次建立約 282 MiB execution tape 後會按來源 manifest、
日期、symbols 與官方開盤價指紋快取。

## 訓練指令與計算極限

正式訓練使用每日 FinancialTransformer，不是分鐘決策模型：

```bash
cd /path/to/stockAgent
source scripts/runtime_env.sh
CUDA_VISIBLE_DEVICES=0 run_fintech_python train.py \
  --config configs/markets/tw_day_trade_1m_realistic.yaml
```

設定保留 1,000 epochs、`batch_size_train: 128`、BF16 model AMP、單卡
RTX 5090，以及相同的 exact-minute loss。事件 loss 將四個連續交易日編成
一個固定 CUDA kernel；批次尾端重複最後一列並以 `state_advance=false`
屏蔽，不改變 forward、NAV recurrence 或 action gradient。完整 batch
不能整段編譯：32 日展開會產生過大的 Triton kernel；2/4/8 日微基準分別
為 0.492/0.466/0.488 ms/日，故固定四日。

在 Fold 6（1,393 個訓練日、2,744 檔）量到的四日核心穩態完整 epoch：
batch 96/128/192 分別是 1.100/0.977/1.089 秒。batch 128 的雙 RTX 5090
DDP 是 0.994 秒，仍慢於單卡，且兩個 rank 的冷編譯約 60.7 秒。原因是每張
卡仍須重播同一條全域 NAV loss 並做 NCCL 同步，所以同一 fold 採單卡；
第二張卡應用於另一個 fold 或獨立實驗。首次 shape 編譯不屬於穩態 epoch，
會額外花數十秒並由 persistent Inductor cache 攤銷。

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
- 舊 17-field 訓練 loss 已包含 09:01、13:20--13:24 與 13:30 狀態；但沒有歷史
  L1，所以不估計真實排隊順位、撤單或超過分鐘 VWAP 的市場衝擊。
