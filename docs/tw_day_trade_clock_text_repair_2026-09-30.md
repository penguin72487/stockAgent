# 當沖執行時鐘：收盤文字與測試對齊

## 1. 執行進度

已修正當沖頁面 13:30 說明與過時測試。公開網站及收盤平倉相關回歸共 **124 項通過**。
本機唯讀 gateway 已送出更新後的 HTML；服務 PID 維持 `346363`，不需要重啟。

## 根因與實際規則

`test_day_trade_clock_ends_at_auction_without_overnight_contract_details` 原本要求
「沒有成交證據的部位仍顯示未平倉」。這句預期來自舊的收盤執行規則。
使用者在 2026-09-27 已指定全部啟用當沖模型採 **13:30 有來源收盤價、不限容量紙上平倉**，
頁面亦已更新，但測試仍保留舊斷言。

已直接核對 `TwDayTradeSimulationEngine._settle_unlimited_close`：

- 有效同日官方收盤證據，或有實際非試撮收盤成交及時間證據時，可結清可交付殘倉；不要求足夠成交容量。
- 缺收盤價、錯誤日期／來源、非法 tick、停牌或未交付部位仍受原有守門約束。
- 清算記為紙上研究假設，保留來源收據與實際補登時間。

契約及先前部署證據見 [全部模型不限容量收盤重算](tw_day_trade_all_models_unlimited_close_2026-09-27.md)。

## 修改與驗證

`services/tw_day_trade_dashboard/index.html` 保留原來的 13:30 不限容量規則，
將模糊的「仍需處理」明確改為「缺收盤價、停牌或未交付的部位仍顯示未平倉」。

測試改為核對 **13:30 對應的時鐘項目**，涵蓋收盤價來源、不限容量、未平倉例外與紙上成交性質。
原有六個時鐘節點與隔日沖專屬說明的隔離檢查保留。
本次修改限於文案、測試及紀錄，交易引擎、帳本與模型設定沒有變更。

實際執行：

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q \
  test/test_public_dashboards.py \
  test/test_day_trade_terminal_close.py
```

結果：`124 passed in 27.57s`。涵蓋缺價後重啟重試、非法收盤證據、停牌、試撮、
零容量時的正式紙上清算、去重與獨立清算稽核，以及完整公開網站回歸。

唯讀驗收 `http://127.0.0.1:8770/tw-day-trade/` 回傳 200，本文與工作目錄 HTML 逐 byte 相同；
ETag 與 SHA-256 為 `d909dc82509bf21b9c397d82dd7c7af36a19f05c99f0c7b75890e098e48f22d5`。
既有 gateway 按檔案 metadata 使靜態快取失效，本次直接讀回已確認更新生效。
