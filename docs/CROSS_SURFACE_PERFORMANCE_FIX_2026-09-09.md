# 網頁、Discord 與盤後績效口徑修正

## 執行結果

已部署核心計算與顯示修正，並補回 2026-09-07～09-09 四個模擬模式缺少的 13:30 終點。未更換模型、checkpoint、策略、成交價格、費率或交易時鐘；未送正式單。

執行證據：`artifacts/operations/cross_surface_fix_20260909/acceptance.json`。
可重跑驗收：

```bash
source scripts/runtime_env.sh
run_fintech_python artifacts/operations/cross_surface_fix_20260909/verify.py
```

此驗收只讀取服務、原始訊號和帳本，僅寫入該 operation 的衍生驗收資料；不發送 Discord 訊息。

## 第一性原理：先分清資料產品，再共用數值契約

| 產品 | 唯一計算依據 | 顯示規則 |
| --- | --- | --- |
| 模擬帳戶累積績效 | 同一個原子 `state.json` 的初始資金與淨清算權益 | 網頁與 Discord 共用 `paper_account_performance`，顯示帳本版本、估值時間 |
| 所選日期區間報酬 | 日期篩選的第一個有效分鐘至期末 | 保留第一個有效分鐘為 0% 的原有契約，不冒充自初始資金起算 |
| 模型歷史回測 | 相容 fold 的 canonical returns artifact | 明示起訖日、回測性質；不是帳戶實績 |
| 開盤／盤中／盤後訊號 | 各次推論的價格、特徵截止日與 signal ID | 分別標示；新目標不是已成交紀錄，不改寫今日開盤持倉 |

修正前，部分消費端把 training artifact 的 log return 當成 simple return。現在統一在讀取邊界轉換：

```text
simple_return = exp(log_return) - 1
close_nav = open_nav * (1 + simple_return)
period_return = exp(sum(log_return)) - 1
account_return = (net_liquidation_equity - initial_capital) / initial_capital
```

原始訓練檔不改單位。Consumer DTO 明示 `return_type=simple`；混合／未知單位和非法非有限值拒絕使用；log `-inf` 保留為歸零，而不是被當缺值忽略。風險統計不再裁切接近 -100% 的合法虧損。

`stock_history`、`portfolio_history`、signal engine 的 recent metrics、Discord 共用解碼／複利及 artifact 選擇邏輯。原有 unlimited-margin conversion 不適用 integer oracle 的相容性限制保留。Discord presentation schema 升為 2；舊快取必須重算，來源失敗不沿用舊數字；成功恢復才重新標示 available。

## 實際數值驗收

| 模式 | 帳戶累積報酬：兩端一致 | 模型最近 32 筆回測 | 回測截止 |
| --- | ---: | ---: | --- |
| 一億 | 6.1917956135% | 0.1728880999% | 2026-09-09 |
| 多基底 | 26.7125893197% | 26.4897681153% | 2026-09-09 |
| 多基底22 | 47.8863638919% | 1.6640625564% | 2026-09-04 |
| LayerNorm v12 | 59.7722160684% | 10.3112978068% | 2026-08-19 |

右欄與左欄不同是期間、交易產品不同，不應硬調成相同。多基底22、v12 的舊回測展示分別約 0.17935%、9.62887%，其單位錯誤已修正；日期落後仍明確揭露，未宣稱完成歷史推論。

網頁卡片保留原日期篩選的大字區間報酬，另外顯示「帳戶累積報酬（Discord 同口徑）」及帳本版本。歷史日期的帳戶 as-of 取該日 mark，不能沿用今天的 state 時間。前端資源版本升為 `app.js?v=68`。

本機快取命中後的訊息整理中位數，首次約 9～12 ms、重測約 6～9 ms（各模式 10 次）。這不包含模型推論、Discord 網路或手機通知時間，不能視為端到端延遲保證。

## 13:30 終點與已平倉帳戶

根因有兩個：13:30:02 被 `time <= 13:30:00` 的秒級比較排除；完全平倉後沒有待查報價，主迴圈便不再觸發帳戶 mark。

現在先以分鐘判斷曲線邊界；平倉帳戶有獨立的分鐘時鐘。收盤後同一交易日、已結算且完全平倉的帳戶，可補寫已知結算 NAV 至 13:30，記錄真正寫入時間，不捏造報價。其他交易日或未證實結算的帳戶不可回填；終點簽章防止一般重啟重複追加。基準報價排程亦涵蓋整個 13:30 分鐘。

9/7、9/8 使用 canonical minute rebuild 工具新增的 `--repair-terminal-only` 路徑，核對原始進出場股數、完整平倉部位、逐筆淨損益、兩個盤中累積已實現損益錨點及同日收盤結算事件，才恢復 8 個終點。任何成交晚於 13:30 分鐘、對帳不符或無來源即拒絕發布。先建立候選、驗證 270 點，持有 engine writer lock 後原子替換 marks；今日 4 個終點由新引擎補上。

另移除了 3 筆「前一交易日標籤、次日 09:01 時間」的平倉重複 mark；它們的 NAV 與已核對終值相同。完整原檔可由以下 before-image 恢復，非刪除原始行情或成交：

`artifacts/operations/cross_surface_fix_20260909/terminal_published/marks.before.jsonl`

恢復明細及原始雜湊：同目錄 `terminal_curve_recovery_receipt.json`。部署後 `fills.jsonl`、`orders.jsonl`、`signals.jsonl` 的 SHA-256 均與修復前一致。四個帳戶初始／最終資金均未改變。

## 測試與運行驗收

- 494 項 Python 測試：單位契約、歷史對帳、快取失效／恢復、來源選擇、歸零風險、帳戶跨端一致、歷史 as-of、平倉時鐘、終點回補拒絕條件、Discord、gateway、分鐘曲線及日曆。
- 14 項 Node 測試：共同 dashboard 元件，以及實際 mode-card renderer 在日期區間改變時只改區間報酬、不改帳戶 DTO。
- JS 語法與 `git diff --check` 驗證。
- 三個相關 service 已重啟；Discord Gateway 連線成功、22 個指令同步、四模型預熱 ready。引擎與 Discord 模式同步，revision lag = 0。
- 公開 gateway API：9/7～9/9 四模式各 270 點，終點均為台北 13:30；今日曲線終值與帳戶完全對齊。
- 全部 135 個已完成交易日、四模式，共 145,800 筆分鐘 mark 通過點數／時鐘檢查。這項檢查不是歷史價格來源完整證明。

## 尚未完成，不能報成全綠

完整來源驗收仍為 degraded。`minute_curve_receipt.json` 的舊驗收止於 9/4；新增成交與 mark 使舊收據的全檔 hash／日期範圍失效，不代表本次更動成交。

9/7～9/9 有 3,228 筆非終點 live mark 尚未具備歷史分鐘重建要求的完整 provenance。既有 live marks 保留，沒有把它們全部標成 historical verified。0050、2330 與台指近基準這三天的既有曲線仍止於 13:29，台指的延長分鐘亦需 canonical benchmark history 補齊。未用 13:29 價或自行插值冒充收盤行情。

這些是另一層來源補齊與完整歷史維護工作，本次未重建其全部歷史，也未延伸多基底22／v12 的正式模型回測。Discord／Web 的帳戶計算一致性與訊號服務可用性，和這些維護缺口分開報告。
