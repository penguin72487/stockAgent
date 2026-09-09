# 13:25 隔日沖：來源與執行契約獨立檢查

2026-09-09。本報告檢查工作區當時正在修改的版本；不是完成訓練、正式部署或
投資績效驗收。檢查期間另有同時修改相同模型／資料／回測檔案的工作，
本次新增的獨立來源稽核不覆寫那些改動。

**結論：遠端基準身分已確認，來源分區的雜湊驗證通過；目前固定隔日沖實作
仍有委託量使用未來價格的問題，不能直接驗收。缺價遮罩的 checkpoint／
股票對齊問題已在同步修改中修復，並經本次重新驗證。**

## 已確認的基準

遠端與本機已物化副本的 `run_manifest.json` SHA-256 相同：

```text
a44a92ad8e731effe6f30fef718f138148b8817b1b091e14ed8fc89e5875bdd5
```

基準路徑為：

```text
artifacts/markets/tw_day_trade_daily_multi_basis_22_effective_rank_projection_l1_tplus2_close_commission20_capital10m_v1
```

實際設定為 FinancialTransformer、22 組有效秩基底、32 日回看、L1 球投影、
TWD 10,000,000、手續費兩折、BF16、DDP、1,000 epochs、panel-history
walk-forward。來源實驗是多空當沖，panel 起點為 2014-01-01；不能將當沖
checkpoint 直接稱為已學會隔夜報酬。

## 執行上的三個獨立時間

| 時間 | 能決定的事情 | 不得使用的資訊 |
|---|---|---|
| t 日 13:25 | 以已完成歷史特徵及當下報價決定方向、委託股數與限價 | t 日最終收盤、次日開盤、全日最終成交量 |
| t 日收盤集合競價 | 用已送出委託判斷實際可成交量與成交價 | 不得以成交價回頭改寫原始委託股數 |
| 下一有效交易日開盤 | 沖銷前一日實際成交庫存 | 不得依該日下午 13:25 訊號決定早上是否沖銷 |

隔夜股票賣出使用一般交易稅，不適用現股當沖減半稅率。13:25–13:30 試撮
不代表成交，部分證券收盤可能延至 13:33；這些規則由
[TWSE 交易制度](https://www.twse.com.tw/zh/products/system/trading.html)與
[TWSE 投資問答](https://investoredu.twse.com.tw/pages/TWSE_InvestmentQA.aspx?ID=14)
說明。[交易所作業規定](https://twse-regulation.twse.com.tw/TW/int/DAT01.aspx?FLCODE=FE064008)
亦允許無收盤撮合時以最後一筆交易價作為日線收盤價，
因此「有日線 Close」與「有收盤競價成交」是不同證據。

## 需要修正的已重現問題

### P1：13:30 價格改變了 13:25 應已決定的委託股數

目前 `tw_dual_session_integer.py` 在收盤呼叫
`_target_holdings_from_weights(..., nav=nav_close, prices=close_marks)`。
13:25 的原始價格只被轉成模型特徵，沒有傳到整股執行器作為 sizing price。

固定資金 1,000 萬、固定模型權重 0.5、13:25 價格 100、每張 1,000 股、零費用，
僅修改未來撮合價，可重現：

| 13:30 價格 | 現行建立股數 | 13:25 應已委託股數 |
|---:|---:|---:|
| 100 | 50,000 | 50,000 |
| 105 | 47,000 | 50,000 |

兩個情境的總成本皆低於資金上限，因此差異不是資金不足所致。
修正需將 13:25 原始價格及當時可用資金保留為 executor context，以既有整股
sizing helper 建立不可變委託；收盤只套用實際價格、費用與成交限制。
連續訓練與 exact integer 報告需維持同一委託語意。

### 已修復並複驗：13:25 缺價遮罩的 checkpoint 與股票對齊契約

已重現兩個數值相同但交易資格不同的 panel：

- A 的 13:25 價格等於前日收盤，gap = 0，available = true。
- B 的該筆價格缺失，現行 gap 填 0，available = false。
- 兩者模型 features 完全相同、availability 不同，但
  `build_checkpoint_manifest(...)["fingerprints"]["data"]` 完全相同。
- `subset_panel_symbols(panel, reordered_symbols)` 後，
  `overnight_1325_available` 變成 `None`，缺價保護可能消失。

上述問題是在較早的工作中版本重現；同步修改後重新執行相同案例，已確認：

- availability 不同時，checkpoint 的 data fingerprint 不同。
- 股票重新排序後，availability 依相同股票索引保留。

這兩項修復由同步修改中的主程式提供，本次負責獨立複驗。原始 13:25 sizing
價格尚未進入執行器，仍屬上一項未解決問題。缺報價與真正的零報酬不能共用
可恢復的 optimizer 身分。

### 研究假設：固定早上沖銷目前繼承強制退出免參與量限制

固定模式把開盤 `force_exit_mask` 設成全 true，沿用既有 mandatory exit 規則。
以 50,000 股庫存、次日參考量 1 股、參與率 50% 測試，仍賣出全部 50,000 股。
這是實際觀測到的執行行為；若保留，必須明示為強制全量成交的研究假設，
不能以它證明開盤撮合有足夠流動性。真實／紙上撮合帳本應保留未成交庫存，
不得憑強制沖銷指令捏造成交。

## 本機來源完整性與證據界線

使用 `scripts/audit_tw_overnight_auction_sources.py` 全量檢查：

- 來源：`data_tw_minute/research_dataset`，2020-03-02 至 2026-09-08。
- 1,592 個 manifest 分區、約 13.79 GB；逐一驗證 SHA-256，讀取前後再次核對。
- 1,592 / 1,592 通過；來源 manifest 在檢查期間未變更。
- 13:25 正價格觀測共 1,801,523 筆；13:30 正價格且正成交量共 2,782,992 筆。
- 2020-09-27 與 2024-12-01 分區沒有 13:25／13:30 目標觀測；這兩天是週日，
  不能把分鐘來源 manifest 當作交易所開市日曆。
- 2026-09-08 有 2,303 個 13:25 正價格，1,963 個 13:30 正價格且正成交量；
  這是來源觀測數，不是當日完整合法交易股票數。

原基準要求 2014 年起的資料，現有來源無法證明 2014–2019 年 13:25 的價格。
應由使用者選擇縮短實驗範圍或先補來源；不能用日線 Close 補成 13:25。
目前工作區的新 config 仍要求 2014 年，loader 會因缺少來源而拒絕啟動。

KBar 的 09:01 `Open` 是該分鐘第一筆成交，沒有單筆交易所時間戳就不能證明
09:00 開盤集合競價；13:30 KBar 的成交量也不證明個別委託排隊與全量成交。
此來源稽核因此刻意分開 `source_receipts_valid=true` 與
`strict_auction_training_ready=false`。

另外，工作區正在建立的新設定目前是 `long_only=true`，與指定基準的
`long_only=false` 不同。空方必須明確選擇一般隔夜融券／借券規則與券源證據，
或確認先做多方版本；不得把當沖賣出資格直接當成可隔夜放空的許可。

## 本次驗證與重跑

新增來源稽核的 4 個測試通過，涵蓋雜湊不符、分區日期被改名、重複股票分鐘、
零成交量不能當成流動性證據；Ruff 與語法檢查通過。這不代表上面的策略實作
問題已修正，也不代表整個訓練測試套件通過。

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s test/test_tw_overnight_source_audit.py
run_fintech_python scripts/audit_tw_overnight_auction_sources.py \
  --minute-root data_tw_minute/research_dataset \
  --baseline-root artifacts/markets/tw_day_trade_daily_multi_basis_22_effective_rank_projection_l1_tplus2_close_commission20_capital10m_v1 \
  --output artifacts/audits/tw_overnight_source_contract_20260909.json
```

完整逐日機器可讀 receipt：
`artifacts/audits/tw_overnight_source_contract_20260909.json`。
本次沒有啟動正式訓練，也沒有變更 Discord、報價收集或交易服務。
