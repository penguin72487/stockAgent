---
name: stockagent-priority-data-query
description: 將有限的 FinLab、FinMind、TEJ 資料查詢插入原下載器，查本機結果、追蹤收據與處理等待。用於其他程式或研究臨時需要資料；沿用原配額與帳號執行者。
---

# 優先查資料

入口是 `scripts/request_provider_data.py`，Python 入口是
`downloader.provider_priority.register/inspect`。註冊只寫派工意圖，沒有來源呼叫；
原下載器在下一個安全邊界接手。不同 provider 可並行，同帳號沿用共享配額，
Smart Wizard 沿用一個桌面執行者。

1. 先讀 [排程與查詢說明](../../../docs/provider_priority_scheduling_2026-10-27.md)。
   用 `list` 找原清冊的 dataset key／task ID，檢查本機已有資料及其日期、單位與收據。
   已完成的 FinMind／TEJ 任務直接讀原資料，不重送查詢。
2. 建立 `provider_priority_query_v1` JSON，包含 provider、具體 reason、帶時區的
   created/expires UTC 和一個以上精確 items；**items 總量沒有固定上限**，優先期限最長七天。
   重複 items 自動合併，排序不同的等價新要求共用 request_id；既有收據仍可讀取。
   FinLab 使用一般 catalog keys，
   或使用 `{'dataset': 'tw_futures_minute:PRODUCT_YYYYMM', 'partition': 'YYYY-MM-DD'}`
   的精確期貨月份／交易日，或 `tw_tick:股票代碼`／`tw_minute:股票代碼` 的精確交易日；
   盤中分區不能和一般表字串混在同一個要求，也不支援 `fresh_after_utc`。
   FinMind 使用 dataset/data_id/partition，TEJ／TEJ API 使用既有 task ID。
   FinLab 預設先用完整性通過的本機資料；確實需要重新確認最新版本時，指定
   `fresh_after_utc`，避免每次讀取都重新下載整表。
3. `submit --plan`，保存回傳 request_id；用 `status --provider --request-id` 輪詢。
   重送相同 JSON 是冪等的。不要讓每次輪詢都產生新 plan／新時間。
4. 消費原資料前核對 native receipt、來源範圍及空值。`complete`、`observed_empty`
   或 `upstream_checked` 不等於所有應有值已補齊；錯誤、冷卻、無權限會原樣顯示。
   到期未取得就回報原因，再依需求建立下一個有限要求。

正式到期追新優先於臨時查詢；臨時查詢優先於一般歷史回補與剩餘配額校驗。
只讓可核對的海外歷史續抓讓路，不挪走其他即將發布資料的預留。
不用第二個 SDK session、桌面、自訂 HTTP client 或重啟服務來搶配額。

一次登記完整需要的範圍，讓原 owner 共用本機結果與在途任務。來源已確認可合併的
連續日期／年份沿用原最大合法範圍下載，再由本機分區與分發；不要逐 item 自行發 API。
不同資料集、帳號或來源不硬合併；來源筆數、分頁、配額、已量測的回應大小與重試限制仍適用。
FinLab 單輪派工數與 SQL 分批是執行批次，並非需求總量上限；未執行的需求仍留在原清冊。

新查詢範圍：此入口不猜測未註冊的資料集／日期或重開未知結果。先沿用原 provider
清冊與種子排程建立範圍。Smart Wizard 新的公司／日期／欄位矩形，使用原生
`run_fintech_python -m downloader.download_tej_history prioritize` 的 source-bound plan；
FinMind 特定產品比較使用 `finmind_product_query_verification`。
FinLab 股票分鐘／tick 仍由原全市場 partition scheduler 驗證交易日及生命週期後排程。
有限期貨分鐘分區
由同一分鐘 owner 派工；缺少支援期貨的 SDK 時，須有固定 SHA 的官方隔離 wheel，
並只沿用原 owner 的 session、配額與來源收據。未知來源結果不視為空資料。

執行 Python 先 `source scripts/runtime_env.sh`，再用 `run_fintech_python`。
