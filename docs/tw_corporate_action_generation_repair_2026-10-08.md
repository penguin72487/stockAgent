# 2026-10-08 除權息來源版本一致性修復

## 結論與範圍

10/08 官方參考檔與 exact entitlement 已版本一致，canonical completed-session
收據為 `ok`、`after.current=true`、無衍生錯誤。正式 panel loader 驗收通過，
相關 **451 項測試通過（14.00 秒）**，四模式 canonical 盤後快取驗收
`attempted=4 / failures=0`。只重新載入 Discord consumer；當沖引擎、Shioaji
session、原始訊號與模擬帳本未重啟／改寫，沒有券商下單。

這是來源一致性及相關消費端恢復的驗收，不是全部資料、全部 repository 測試、
所有官方條款或下一交易日開盤一秒 SLO 的完成宣告。

## 根因：多個檔案不能當作獨立完成的交易

canonical producer 已持有 `tw-public-refresh.lock` 的 exclusive lock，依序建置
參考檔、股票 panel、除權息 exact entitlement 及 public features；但是 live
panel/cache reader 未參與同一個鎖。單檔 atomic replace 不等於整組相依資料
一起提交。參考檔先更新、entitlement 尚未完成時，reader 會看到混合版本，
嚴格 size/SHA 檢查因此正確拒絕。

今天的時間線：

- 14:01:33：參考檔從 34,605 更新至 34,606 筆；新檔 374,349 bytes，舊
  entitlement 的依賴收據仍綁定舊參考檔 374,333 bytes。
- 14:19:40：第一輪 stock-delivery 來源發生 14 個 MOPS HTTP-200 限流／拒絕頁
  （4 listing、10 detail）。HTTP 成功不是資料取得成功，原已驗收資料沒有被
  失敗嘗試覆蓋；失敗證據保留於 attempt receipt。
- 14:24:24：**部署本輪程式之前，既有 canonical retry 已成功**；新版
  entitlement 覆蓋至 10/08、`coverage_complete=true / failure_count=0`。
- 14:33 之後完整交易日來源驗收通過；14:41:02 的收據也確認所有相依層 current。
  本輪不把這個既有恢復誤稱為新增程式造成的加速。
- 14:48:52：獨立只讀驗收以正式 panel loader 再確認 reference、entitlement、
  raw manifest 收據與日期範圍內事件鍵一致。
- 14:50 起：收盤且無互動工作時重新載入 Discord；引擎持續運行。最終部署
  invocation、Gateway 與帳本證據保存在本輪 acceptance receipt。

## 修正

1. Live `_build_panel` 共用 producer 的鎖，從 cache fingerprint 到來源及快取
   讀取皆持有 nonblocking shared lease。真實 exclusive owner 仍在工作時，
   回傳型別化 `TWPublicSourceUpdatePending`，不進模型、不等待 inference mutex。
   owner 退出或 crash 後由核心釋鎖；剩餘真正錯配仍由原 SHA／筆數檢查拒絕。
2. Discord arm/cache 與 isolated artifact worker 將這個狀態記為
   `waiting_source` 並重試，而非每秒多模式 traceback。worker 的外層忙碌檢查
   之後若 producer 才取得鎖，也能延期，不會寫出假 `complete`。
3. Entitlement collector 在接受輸出前重新驗證 reference／universe 依賴。
   即使大小不變，只要 SHA 或 reference summary 身分改變，便保留舊已驗收
   parquet／summary，只留下失敗 attempt，拒絕發布過時的計算結果。
   這不宣稱能阻止不遵守 canonical writer lock 的任意外部手動改檔。
4. Stock-delivery company-year 不再要求尚未到來的公告年（今天不抓 2027）。
   已可觀測的歷史次年公告仍保留；未知發放日仍是未知，不會猜日期或補 0。
5. 新收到的 HTTP-200 denial 也保存原始內容與 SHA，且不能加入成功 raw
   manifest；沿用既有退避與官方存取限制，不繞過上游防護。
6. Gateway 快取狀態屬於當次 run，不繼承前一程序的記憶體暖機證明。
   官收 cache reuse 必須匹配當日、部署 fingerprint 與實際 weights artifact；
   `panel_close` 且 `execution_preview_only=true` 是正常盤後不執行訊號，
   與 memory-only provisional quote preview 不同。isolated worker 不覆蓋
   Gateway 的 run-owned provisional-cache 狀態；兩個健康領域保持分開。

collector 的新規則由下一個 canonical 子程序讀取；本輪不為了換標籤無必要地
重新下載已驗收來源。consumer 的 shared lease 已重新載入並通過運行驗收。

## 資料與測速證據

參考檔 SHA：`41a2616ef2d03feba4749ea2a8a03ff491720bd88e0851ed16549cc677002029`。
Entitlement output SHA：`01a4f6191a4247685c0b52737902caf2b799e54ec2c09f0c826400bbc173dc1e`。
新 entitlement 的 reference receipt 精確綁定上述參考檔及其 summary SHA。

- reference 全檔 34,606 筆；entitlement 的 2014-01-01～2026-10-08 範圍
  23,327 筆，兩者在該範圍內的 `(date, symbol)` 集合完全相同。
- 18,819 `exact_cash`、7 `exact_inventory`、4,501 `avoid`；stock delivery
  尚有 2,350 事件未能解析 exact delivery date，保留原安全分類。
  `coverage_complete` 是要求範圍已分類／來源請求驗收，不是每個條款都已知。
- 原 raw manifest 的 8,161 entries、3,671,547 bytes 與 manifest SHA 通過
  canonical 檢查；這不是本輪逐筆重新下載全部來源的宣告。
- 只讀 source acceptance 共 1,296.413 ms；含 canonical full validation，
  **不是** model inference／09:00 latency。
- 1,000 次實際 idle source shared guard：median **0.152967 ms**、p95
  **0.286177 ms**、max **0.539119 ms**。這只量 acquire/read/release 保護，
  不能拿來宣稱取價到成交達一毫秒或一秒。
- isolated restart audit 4 模式 inventory 原樣恢復、未平倉 0；部署前後
  events／orders／fills byte prefix SHA 一致。可變 state checkpoint 和心跳
  不要求逐 byte 相等，以免把正常服務更新誤判為資料破壞。
- 最終 15:01:17 驗收：Discord invocation
  `3597629b9e2243389c2fcc6c88f9e142`、Gateway connected、22 個指令同步、
  engine revision lag 0，重啟後無 fatal／watchdog／版本錯配 traceback。
  4 模式當日官收訊號的來源、checkpoint/config fingerprint 與 weights
  artifact 獨立驗證通過。當沖引擎 invocation 仍為
  `44fb4321f3f542bf9c61ed35441f22a8`。15:00 的下一輪正式歷史維護正在運行、
  failed count 0；不將「正在工作」宣稱為該輪已完成。

## 重測

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_corporate_action_coherence.py \
  --expected-date 2026-10-08 \
  --output artifacts/operations/tw_corporate_action_repair_20261008/recheck.json

run_fintech_python -m pytest -q -s \
  test/test_tw_public_source_guard.py \
  test/test_tw_corporate_action_entitlements.py \
  test/test_tw_corporate_action_reference.py \
  test/test_tw_public_publication_schedule.py \
  test/test_live_signal_helpers.py \
  test/test_discord_bot_helpers.py \
  test/test_discord_bot_formatting.py \
  test/test_discord_intraday_latency.py \
  test/test_discord_artifact_worker_retry_state.py \
  test/test_tw_day_trade_restart_audit.py \
  test/test_reconcile_tw_public_training_features.py
```

完整驗收：[acceptance.json](../artifacts/operations/tw_corporate_action_repair_20261008/acceptance.json)。
來源驗收：[source_acceptance.json](../artifacts/operations/tw_corporate_action_repair_20261008/source_acceptance.json)。
451-test run：`corporate-action-shared-worker-f-20261008T065650-ece8c932`。
Canonical cache run：`corporate-action-canonical-close-20261008T065651-47235abd`。

不宣稱已消除 MOPS 上游限流、任意非 canonical 手動改檔、所有來源缺漏或真實
開盤延遲。保留這些邊界與錯誤，才有後續優化與恢復依據。
