# 五模型 2026/02/25 起全期重算

## 1. 執行進度

2026-09-10 18:22：136 日隔離重播、分鐘獨立估值及完整來源／帳務驗收
均完成，`candidate_acceptance.json` 為 `status=validated`、
`promoted=false`。**未替換正式網頁帳本、未重啟服務、未送券商委託、
未啟動新訓練**；重算候選完成不等於已完成部署或訓練一致性。

- 官方 TAIEX receipt：2026-02-25～2026-09-10，共 136 個完成交易日。
- 範圍：100m、multi_basis、multi_basis_22、原 projection/GELU ID、
  attention_layernorm，共五個獨立紙上帳戶；本金各自保持 100M／10M。
- 680 個已登記訊號都有來源身分；本次是固定其目標權重後重算執行，
  不重訓、不把現在產生的訊號倒填成當天已存在。
- 已驗收分鐘格線：136 × 5 × 270 = 183,600；09:01～13:30。
- 工作根：`artifacts/live/recompute-all-20260225-zCLJ9EEm`。
- `candidate` 是來源錯配時中止的失敗證據，不能當成完整歷史。
- `candidate_v2` 是修復來源解析後的候選，未 promote。

重播得到下列期末值，已通過獨立來源／會計驗收；不是已發布的網頁
績效，也不是券商帳戶報酬。報酬分母是各自初始本金。

| market | 9/10 期末模擬 NAV（元） | 2/25 起報酬 | 殘餘庫存批次 |
| --- | ---: | ---: | ---: |
| tw_day_trade_100m | 108,380,265.42 | 8.3803% | 64 |
| tw_day_trade_attention_layernorm | 14,687,492.13 | 46.8749% | 1 |
| tw_day_trade_multi_basis | 13,415,756.35 | 34.1576% | 0 |
| tw_day_trade_multi_basis_22 | 15,537,034.52 | 55.3703% | 0 |
| tw_day_trade_multi_basis_projection_l1_gelu | 16,246,031.00 | 62.4603% | 1 |

最終驗收證據：

- `candidate_acceptance.json`：136 日全期通過、`error_count=0`、
  `sources_verified=true`；查核 14,577 個來源檔案，最大帳務浮點尾差
  `6.402842700481415e-10` 元。
- 獨立分鐘估值：差異點 0，最大絕對尾差 `1.862645149230957e-09` 元；
  必要股票日期來源 49,470／49,470，缺失 0。
- 183,600 個策略點中，125,571 點包含沿用先前實際成交價的持股估值；
  平均新成交名目金額覆蓋比 0.734352，最低 0。完整分鐘格線不代表
  每檔股票每分鐘都有成交，不插值、不把估值當成成交證據。
- 0050／2330 各 36,856 點（136 × 271），台指連續參考 40,800 點
  （136 × 300），均涵蓋至 9/10。
- 09:01 來源成交 45,103 筆，synthetic fallback／official-open 成交均 0；
  此數只計 09:01 入場，不是全期所有進出場筆數。
- 候選 `state.json` SHA-256：
  `145b146f92039ce25ac7305b02919683b454934d8cfbf8a865f65ca4e1bfa0db`。
- 候選 `fills.jsonl` 在分鐘估值前後不變，SHA-256：
  `58deb42ad58d38ef78dbc4578248978dd10db64a29f975f014acb1d92c95a0a6`。

9/10 原 100m 漏查的 16 檔已有來源 09:01 價量：13 檔各補入 1,000 股
反事實模擬成交；2849、4523、9926 的分鐘量各只有 1,000 股，50% 容量
不足一張，保留未成交。原正式紀錄未改寫。675 個歷史 counterfactual
signal 的官方 open 輸入 CSV 也重新核對 SHA-256，缺失／不符為 0；
另外 5 個是今天保留的 live signal，來源類型不冒充歷史 CSV 產物。

## 2. 本次找到並修正的錯誤

原 replay 只固定 `signal_id`，再去**目前**的 live signal 目錄搜尋。
但先前已驗收的重算訊號存在獨立 audit 目錄，而多次重建可以沿用相同 ID。
因此 ID 相同不代表模型輸入／mask／權重內容相同。本次實際在第一天
100m 讀到不含 `exact_session_eligibility_before_forward_v1` 的舊版本，
正確觸發驗收拒絕，並非最新資料真的不存在。

修正沿用 canonical replay，不建立第二個帳本引擎：

1. 從既有 `rebuild_receipt.json`、新增帳戶的 `account_receipts/*/rebuild_receipt.json`
   與目前 session 的 state 解析已接受的 summary／weights 路徑。
2. 只接受與帳本日期、market、signal ID 對應的來源；strategy replacement
   已明確移除的舊 pin 不會重新加入。
3. 驗證已有 SHA-256，並將本次讀取的 680 組 summary／weights hash 固定
   於候選 receipt；讀取前後重新驗證。資料缺失、hash 變化或相同 ID
   對應不同已接受內容，一律拒絕，不再挑另一個同名檔替代。
4. 沒有歷史 artifact receipt 的舊格式仍可走原 ID 查找；明確提供
   `--signal-root` 的隔離換模流程保留原有行為。

更動：`scripts/rebuild_tw_day_trade_open_price_replay.py` 與其測試；
`stockagent/config.py`、`stockagent/backtest/simulator.py` 的不相容錯誤說明
另外修正為不能以 daily OPEN/CLOSE 充作網頁一致性的替代品，未更改
既有訓練執行數學。新增直接載入指定 attention 設定的拒絕測試。

233 項聚焦測試通過／11.05 秒，涵蓋來源 pin、缺檔／衝突、執行帳務、
價格格線、simulation adapter、minute training、回播與升版 gate。
最後改動後 Ruff E9/F63/F7/F82、py_compile、`git diff --check` 通過；
不是全倉庫測試，也不是新訓練器驗收。
680 筆已固定來源的 signal summary 均額外預檢通過 exact-session mask
契約；以後同類不相容會在讀取交易資料前攔下，不必等到重播至該日。

## 3. 沿用的執行假設與部署界線

- 官方 09:00 open 僅用於模型已記錄的開盤輸入與整張 sizing；歷史
  09:01 VWAP，缺 Amount 時用同根來源 KBar Close。不得用 open、+1 tick
  或無來源價格替代缺失 bar。
- 每模型、每股票、每分鐘共用 50% 來源成交量容量；缺乏 09:01 成交的
  標的不虛構入場，不能把所有 missing-bar row 都解讀成下載失敗。
- 採使用者本串對話明確保留的全部可轉融資融券、隔日目標差額調整、
  零股按整張行情研究假設；未啟用另案 staged strict-intraday 政策。
- 重算保留跨日持股、FIFO 成本、日曆日利息、signed 現金權益、換股及
  停牌估值，不使用合成收盤成交抹除庫存。
- 即時 Shioaji simulation 成交回報是另一種證據，不能由歷史重播生成。
  官方 simulation 不支援零股，因此現有 staged adapter 只對實際零股
  殘餘允許明確標記的本機例外，沒有將整張拒單轉成本機成交。
  [Shioaji 官方 simulation 說明](https://sinotrade.github.io/tutor/simulation/)。
- Canonical strategy-switch `plan` 仍被正式五帳戶未平倉 gate 拒絕。
  候選驗收與正式覆蓋是兩個步驟，沒有停用此保護。
  最終只讀檢查：舊帳本 75／1／2／1／2 批未平倉，共 81 批；
  新候選 64／1／0／0／1 批，共 66 批。不能在未取得明確方向下把舊
  持股直接丟棄或將新回播的持股冒充券商部位。須先決定是否封存舊
  模擬帳本，另行驗收歷史替換與持倉延續的明確流程。

## 4. 新訓練指令的真實前置條件

使用者已選擇新訓練只使用必要資料全部完整的期間，禁止舊日 K／+1 tick
proxy；後續再次明確要求沿用全專案已存在的年度切分。**撤回先前提出的
64／16／16 交易日切分，這不是待使用者同意的方案，也沒有套用到程式
或設定。** 資料不足應查既有來源、修復與驗證，不改切分規則繞過。

### 4.1 共用年度切分已查證，無須另寫

`train.py` 呼叫 `stockagent.data.walkforward.build_expanding_year_folds`：

- train：從有效起始年起累積所有過去年份。
- validation：接續的一年（本設定 `val_years=1`）。
- test：驗證年後的**所有未來年份**，不只是下一年。
- 年度彙總取每個 fold 的第一個測試年；stitched deployment 為另一個
  保留跨 fold 帳務狀態的報表，不混同單一 fold 起始重設的測試帳本。
- `lookback_context=panel_history`：跨年可讀先前已觀察的特徵資料，
  不因 lookback=32 丟棄每個驗證／測試年的前 31 個 target。

指定 attention config 與既有 `run_manifest.json` 的 resolved
`walk_forward` 相同：`min_train_years=1`、`val_years=1`、
`require_future_test_year=true`、`expected_first_year=2014`、
`require_contiguous_years=true`、`lookback_context=panel_history`、
`split_start_year=null`。以產物記錄的年度集合重新呼叫共用 builder，
全部 11 個 fold 的 ID／train／val／test 年份逐一相符：

| fold | 訓練 | 驗證 | 完整測試尾段 |
| --- | --- | --- | --- |
| 01 | 2014 | 2015 | 2016～2026 |
| 02 | 2014～2015 | 2016 | 2017～2026 |
| 10 | 2014～2023 | 2024 | 2025～2026 |
| 11 | 2014～2024 | 2025 | 2026 |

這是既有模型的年度身分，不代表新成交假設已訓練完成。舊 manifest
記錄的 panel 為 2014-01-06～2026-08-19；不可冒充更新到 9/10 的測試。
實測 `test_walkforward_folds.py`、`test_single_fold.py`、
`test_tw_stitched_deployment.py` 共 **26 passed／4.32 秒**。
本次修正沿用 `stockagent-training-reuse` 技能要求的共用入口與
產物查證，不新增 trainer／splitter，也未改動年度切分程式。

### 4.2 資料根與執行契約必須分開核對

現有指定 attention config 的執行設定實際仍解析為：

```text
panel_start_date = 2014-01-06
day_trade_minute_execution_allow_daily_proxy = true
tw_day_trade_unlimited_margin_conversion = false
minute policy = scheduled_events_50pct
initial execution capital = 10000000
volume participation = 0.5
```

分鐘 manifest 宣告從 2020-03-02 起。本次隔離回播指定的
`/srv/stockagent-live/data_tw_public/execution_actions` 精確現金權益與
換股 receipt 覆蓋 2026-01-01～2026-09-10；**不能把這個回播專用目錄
的範圍當成全專案歷史資料上限**。前次據此提出只剩 2026 可用並改用
日數切分，查核範圍不足，現予更正。

在使用者指正後，實際沿 canonical resolver／loader 讀取
`/srv/stockagent-live/data_tw_public` 共用資料根：

- 企業行動 reference：34,463 rows、2,692 symbols，驗證涵蓋
  2000-01-01～2026-09-10。`requested_start_year=2025` 是增量請求邊界，
  累積覆蓋須讀 `coverage_start_year=2000`。
- 現金權益 receipt：2014-01-01～2026-09-10；共 23,184 筆已分類事件，
  loader 驗證出 18,688 筆 `exact_cash`、2,022 symbols。
- 同 receipt 仍有 4,496 筆 `avoid` 分類、760 筆缺精確現金條款及
  6 筆未解 ETF 事件。`coverage_complete=true` 證明請求／分類覆蓋，
  **不是每一事件均可依無條件融資融券持有假設精確結算**；不可將
  不相容的 avoid 規則偷偷代入使用者要求的 carry 契約。
- 共用根換股 receipt 僅列 2026-01-01～09-09、5 筆且
  `complete_all_markets=false`；回播專用根是 2026-01-01～09-10、
  20 筆、雙市場完整。均不能證明 2020～2025 的實體換股事件完整。

上述 loader 實查不等於全樣本新訓練驗收。尚未選入新資料 release、
修改現行 producer symlink 或啟動訓練。

實際呼叫 canonical source loader：企業行動 reference 34,463 rows、精確
cash entitlement 2,106 events、換股 20 records；6 個資料／summary 檔案
通過 hash 驗證。TAIEX 2026-01-02～09-10 共 167 個交易日都有對應分鐘
partition 檔案。這只證明分區存在及企業行動來源驗收，不等於全樣本的
minute-price、capacity、FIFO training objective 已通過。

前次把回播資料限制成這 167 日後呼叫年度 builder，確實得到
`No valid walk-forward folds could be constructed`；這只證明單一年份
無法建立原年度 fold，**不是共用 builder 缺功能，也不是改切分的理由**。

兩項仍未完成，不能把舊命令當作「與網頁一致」的新命令：

1. Tensor minute executor 仍在每日結尾清空股票，只留 T+2 淨現金 claim；
   paper 持有的 FIFO 股票、隔日價差／企業行動與淨值定量尚未整合到
   canonical training loss、跨 batch 狀態與 checkpoint。現行 config
   明確禁止同時打開 minute tape 與 daily unlimited-margin executor。
2. 依共用年度切分維持訓練年＋驗證年＋未來測試尾段；核對所有必要
   來源後才能決定無 proxy 的有效年度範圍。既有歷史 cash archive
   必須優先重用，缺精確條款、完整分鐘與歷史換股證據另列缺口並修復。
   不以 2026 專用回播目錄取代歷史訓練資料、不再要求使用者挑新切法。

新訓練必須使用新 artifact root，不 resume 舊執行契約的 optimizer；
目前沒有宣稱可用的新訓練命令。

## 5. 重播與驗收入口

以下為本次實際重播參數；工具拒絕非空目標，所以重跑須選新的隔離路徑，
不可清除或覆蓋這次的失敗／候選證據。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/rebuild_tw_day_trade_open_price_replay.py \
  --state-dir artifacts/live/recompute-all-20260225-zCLJ9EEm/candidate_v2 \
  --start-date 2026-02-25 --end-date 2026-09-10 \
  --source-ledger-dir artifacts/live/tw_day_trade_simulation \
  --benchmark-state-source artifacts/live/tw_day_trade_simulation/state.json \
  --local-only --replay-intraday-kbars \
  --assume-margin-conversion --assume-odd-lot-board-price \
  --margin-action-data-dir /srv/stockagent-live/data_tw_public/execution_actions
```

完成後必須先跑 canonical 分鐘獨立估值與 carried-inventory source auditor，
再處理 promotion／公開 API 驗收；單純命令 exit 0 不等於完成所有要求。

本次分鐘驗收已實際執行以下命令；`--publish` 僅寫入指定隔離候選，
不更動正式帳本：

```bash
run_fintech_python scripts/rebuild_tw_day_trade_minute_curves.py \
  --state-dir artifacts/live/recompute-all-20260225-zCLJ9EEm/candidate_v2 \
  --start-date 2026-02-25 --end-date 2026-09-10 \
  --output-dir artifacts/live/recompute-all-20260225-zCLJ9EEm/minute_audit \
  --revalue-carried-marks --recompute-existing-strategy-marks \
  --require-revaluation-parity --publish
```

最終總驗收直接呼叫 `scripts.promote_tw_day_trade_replay._validate_rebuild`
（五個明確 market ID、`allow_margin_carry=True`），保存完整回傳值至
工作根 `candidate_acceptance.json`。僅執行候選驗收，沒有呼叫交換目錄
或停止／啟動服務；正式 live-flat gate 未停用。
