# 2026/2/25 起隔日沖歷史試算

本次已完成既有看板四個隔日沖適配器的 2026-02-25 至 2026-09-09 計算，
共 135 個官方完成交易日、540 份重新推論訊號、1,080 個開盤／收盤估值事件。
這是既有當沖 checkpoint 改在 13:25 觀察的診斷試算。新建、僅做多的
`tw_overnight_1325_multi_basis_22_capital10m.yaml` 尚未完成正式訓練，
沒有用工程驗收 checkpoint 代替正式績效。

本機產物：

```text
artifacts/operations/tw_overnight_history_20260225_20260909
```

vastai1T 的交付位置：

```text
/root/stockAgent-overnight-1325-v2/artifacts/operations/tw_overnight_history_20260225_20260909
```

`report.md`、`result.json`、`equity_curve.png` 與 `equity_history.csv` 提供結果。
`trade_reconciliation.csv` 保存 6,137 筆平倉的方向、股數、價格及費後損益獨立核對；
全部 12,291 筆進出場成交均與選取的官方價格相符，股數均為 1,000 的整倍數。
`inputs`、`signals`、`ledgers` 保存來源雜湊、實際推論時間與獨立歷史帳本。
歷史有效時間與實際生成時間分開；`exchange_match_at` 保持空值。

## 時間與資料

- 模型日線特徵截至決策日的前一完成交易日；另外輸入當日 13:25 價格。
- 缺少 13:25 時，依使用者授權使用同日 CLOSE，明示這部分具有前視資訊。
- 13:30 以官方日線 CLOSE 作撮合價格近似，後續交易日 OPEN 作退出價格近似。
  這不證明歷史排隊順位、交易所成交時間或全量成交。
- 最新 9/9 收盤新持倉保留，等待尚未發生的下一開盤；缺少較早退出資料也保留持倉。
- 報表只有開盤／收盤事件估值，沒有插值成每分鐘資料。

## 尚未通過正式績效驗收

算術對帳通過不等於資金及執行假設成立。`result.json` 明示
`fit_for_funded_performance_comparison: false`，理由為：

1. 既有模擬器每天按初始資金配置整張數量，沒有完整的自籌資金與 T+2 現金限制。
   三個策略權益非正後仍續單；保留原始負權益，不能把該比例當成可投入本金的報酬。
2. 119 個 symbol-session 的官方 OPEN/CLOSE 超出歷史重建價格範圍，造成 6 次開盤退出拒絕。
   原 TWSE 重建器通用一般股票的漲跌幅與跳動單位，ETF 商品規則需要補正。
   [證交所 ETF 說明](https://www.twse.com.tw/downloads/zh/ETF/qanda01.pdf)
   區分國內槓桿與海外標的；不能套用同一個 10% 規則。原始衝突列於
   `price_limit_conflicts.csv`，沒有以後見價格放寬界線。
3. 一億策略的 4804 於 4/13 進場後，在選取來源中缺少後續價格，餘 14,000 股未退出，
   因此阻擋該策略後續新一批進場；期末仍含 4/13 舊估值。

共 44 筆平倉晚於下一交易日。上述資料品質與模擬資金規則尚未修復，
這份結果沒有覆寫即時帳本或正式看板，也不是新隔日沖訓練模型的回測。

## 重算入口

在具有相同 checkpoint、設定與資料來源的工作目錄使用共同 runtime：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/rebuild_tw_overnight_history.py \
  --start-date 2026-02-25 --end-date latest \
  --output artifacts/operations/tw_overnight_history_NEW_VERSION
```

`plan`、`inputs`、`signals`、`replay`、`report` 可用 `--stage` 分開執行。
同一輸出目錄的 plan 不可變更；新增最新交易日或來源版本時必須使用新目錄。
歷史價格權限只存在於離線子類別，沿用共同隔日沖數量、費稅與帳務計算。
即時引擎仍要求具交易所時間的非試撮 CLOSE/OPEN。

vastai1T 的原訓練啟動器與已驗收資料版本不受本次計算影響；
啟動方式及既知學習限制見 [訓練準備紀錄](vastai1t_tw_overnight_training_2026-09-09.md)。
