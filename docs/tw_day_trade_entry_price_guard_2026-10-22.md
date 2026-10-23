# 當沖模擬進場 TP／SL 守門（2026-10-22）

程式修正與線上採用是分開的驗收。本輪未改寫公開帳本、未下券商訂單、
未重啟交易引擎。10:28 的 canonical engine-only 部署計畫被拒絕：
`engine reload requires off-session flat state`。不得為部署強制平倉或清除餘單。

## 根因與新契約

原本 `_prepare_entry_plan` 只驗證每日合法價格界線；`register_signal`
決定模擬成交後才建立 TP／SL，所以「每日合法」但已跨策略停損的價格仍能進場。
09:01 回放和續單共用這項缺口，不是只修一個模型的設定即可。

新契約：`strict_inside_original_take_profit_stop_loss_v1`。

| 方向 | 允許的進場價 | 邊界或區間外 |
| --- | --- | --- |
| 多 | `SL < 最佳 Ask < TP` | 不買進 |
| 空 | `TP < 最佳 Bid < SL` | 不放空 |

TP／SL 仍依原先每日界線、該模式內縮 Tick、交易日與 stock／ETF Tick
規則計算。缺漏、非正有限值或方向倒置都拒絕；不取中間價、不移動 TP／SL
遷就行情。新反事實回放用它已聲明的實際計價來源做相同檢查，不冒充即時報價。

首次進場、缺開盤價後補單、部分成交續單、full-target 續單、分鐘容量回放
及手動同價模擬補量入口都適用。已成交股數、成本與費用不因拒絕餘單而消失，
原本停損、減倉及收盤沖銷不受此「進場限定」阻擋。

訊號完整保留；畫面說明觸及 TP／SL，不把取消寫成成交或缺價。
零成交的取消單不會被投影成持倉。取消原因更新僅重套用已快取的訊號分頁，
不因行情更新重新掃描完整歷史。拒絕事件按批次持久化；沒有新增網路請求。
安全重載只承認具有同訊號／交易日、版本、價格判斷與時間證據的明確取消單；
未知取消、未定量目標、未提交帳本及未平倉仍阻擋重啟。

## 今日唯讀查核

10:28:42 狀態的進場均價相對該部位 TP／SL，有 11 筆在界線或外側：
100m 2 筆、MGDA 6 筆、annual-log-cash 3 筆。這是目前模擬帳本的查核，
不是券商成交，也不是自動刪除或回寫這些歷史紀錄的授權。

可追溯證據：

- `artifacts/operations/agent-workflow/runs/entry-bracket-live-audit-20261022T022839-5c1bbf5f/run.log`
- `artifacts/operations/tw_entry_price_guard_20261022/deployment_plan.json`

## 測試與採用

聚焦測試位於 `test/test_tw_day_trade_entry_price_guard.py`，涵蓋多空邊界、
Ask／Bid 而非 last／mid、缺漏／倒置、ETF Tick、補價、部分續單、重啟、
新回放、只減倉豁免及公開分頁無假持倉。相關重載測試位於
`test/test_qualified_day_trade_reload.py`，文字呈現測試沿用共享 Node components。
最終回歸收據記錄於同 task 的 `entry-bracket-regression-final` run；失敗的早期
候選收據保留，不以後續通過抹除。

最終本輪 811 項 Python 測試通過（40.91 秒），共享畫面 44 項 Node 測試
通過；受影響檔案 Ruff、JavaScript 語法及差異空白檢查通過。
完整回歸 run 為 `entry-bracket-regression-final-20261022T022918-745a4874`。
純守門函式在同機六種多空／邊界案例，每例 5 × 100,000 次的中位數為
0.54～0.66 微秒／次；此微測不含報價、模型、序列化或持久化，不能當成
端到端成交延遲。收據在 `entry-bracket-cost-20261022T023102-53d078e6/run.log`。

下一步：盤後確認全部平倉、無活躍委託，再依 canonical reload 的
plan／apply／獨立 runtime readback 採用；確認新訊號／成交具此契約版本。
若另行授權歷史重建，須獨立 candidate、保留舊帳本、來源 pin、270 分鐘
曲線及原子 promotion 驗收，不能直接修改原成交或僅重啟服務就聲稱已修復舊數據。
