# 當沖開盤延遲分階段修復 2026年10月14日

依最新需求，第一階段是從 09:00 取得合格輸入，到全部啟用模式的**全部目標股數完成紙上市價成交，總共 30 秒內**，再推進至 20、10、5、1 秒。訊號發布、帳本登記與全目標成交分別驗收；現金空倉、09:01 回放與券商成交也分開。已修正來源修復、整批排程等待及計時缺口；**真實開盤的 30 秒整鏈驗收仍未通過**。沒有修改模型、90% 輸入門檻、正式開盤價認定、執行規則或今日帳本。

## 今日基線

[原始稽核](../artifacts/operations/tw_opening_latency_20261014/baseline.json) 與 [分階段稽核](../artifacts/operations/tw_opening_latency_20261014/staged_audit.json) 保留失敗與成功 attempts，不以重啟後的喚醒時間替換當日第一個時鐘。

| 模式 | 原始訊號執行指標發布時間 | 距離 09:00 |
| --- | --- | --- |
| 當沖 1 億 | 09:09:30.417724 | 570.418 秒 |
| 年度 Log Cash | 09:09:31.323184 | 571.323 秒 |
| 多基底22 | 09:09:33.169388 | 573.169 秒 |
| Multi Basis | 09:09:39.072379 | 579.072 秒 |
| 年度因子化 Scale Separated Cash | 09:31:32.779200 | 1,892.779 秒 |

五個模式全部發布耗時約 31 分 32.8 秒。這是不可變 execution pointer 的原始時鐘，不是網頁收到訊號、回放成交的 09:01、Discord 訊息送達或券商成交時間。

## 阻塞來源

排程在開盤後 4.394～9.569 毫秒已醒來。三個來源約在 09:00:00.29 同時開始；截至第 30 秒，逐檔驗證紀錄仍只有 230 個有效開盤價、沒有合格的明確未成交證據，其餘 2,073 個未知。當日官方來源名冊為 2,303 檔，08:45:01 已取得；名冊數不是收到行情數，也不是有首筆成交數。

MIS 第一輪 29 個請求全部逾時，約到 09:00:10.92 結束；沒有證據可將這種失敗解讀為市場未成交。下一輪來源約 09:00:38.35 才重新啟動。開盤來源建置／準備與重試互相阻擋，放大了來源等待。

年度因子化模式另外在開盤後補做來源建置，該次 `preopen_catch_up` 約 529.245 秒。既有維修已修好命令的可執行位元與 source-child watchdog；本輪核對入口為 755、準備排程為 08:15、來源 timeout 為 1,200 秒。年度來源建置的快取優化沿用 [既有服務效能工作](running_service_optimization_2026-10-13.md)，未重做另一條資料管線。

推論本身約 51～490 毫秒；部分模式仍有約 11 秒的盤前骨架重載、2.5～5 秒的入模準備與排隊。這些分段包含巢狀／平行跨度，不能相加成總延遲。

## 已實作修正

- 來源修復可與 `preopen_catch_up`、`realtime_prepare` 並行；真正模型執行及未知階段仍讓位。維持共用 fan-in、in-flight 去重、原生連線與每個供應商的退避，不增加登入。
- `not_started` 的額外重查等待從 60 秒改為 1 秒。首分鐘不再疊加 observer 的 30 秒退避；既有 loop 為每秒一次，實際嘗試耗時仍包含來源 I/O。
- 新增 `resolved_input_count`、`recovered_input_count`，合格的「明確尚無首筆成交」也算取得輸入證據。它仍不是價格，不允許用零價、昨日價或中點成交。
- 每次來源修復記錄 owner run、observer policy、loop interval 與最短重查等待。來源自己的 429／認證／端點／無進展退避保持不變。
- 接收時鐘改用與官方交易名冊對齊的有效開盤價加合格未成交證據，避免模型已通過輸入門檻但計時永遠不足。計時 schema 升至 2；不合法或越界時鐘保持未知，不中止已完成的正確模型計算。
- 稽核加入 1、5、10、20、30 秒觀測點及全部模式的五級發布驗收。只完成最快的模式、缺模式、無時區或倒填時鐘均不能達標。
- 模擬帳本登記時間沿用既有 executor 投影，與訊號發布分開。今日最早登記在 09:09:33.740316，最後在 09:31:39.372365；登記不證明全目標成交。1 億模式訊號後另有約 557.544 秒來源恢復等待，仍需後續獨立優化。
- 排程的輸入工作改為 `session_fenced_single_flight_bounded_poll_v1`：每模式保留一個準備／取價工作，單次 poll 最多 50 毫秒，繼續沿用 100 毫秒排程節奏。慢模式未完成時，健康模式仍可依原退避自行重試；不取消正在執行的來源 thread，也不建立重複 builder。過期配置／交易日的結果完成後丟棄，不能送入模型。
- `preopen_catch_up` 移到同一個持久輸入工作；source-child watchdog 仍驗證原命令的真實子程序和固定期限，且獨立檢查其他卡住的取價工作。推論與來源／checkpoint 重驗保持原有序列化所有權。
- 已發布訊號的報表、Discord 廣播與訂閱通知保持原先順序，但由背景交付 task 處理，不能再阻止其他模式的取價重試。通知失敗仍記錄原錯誤，不重新產生當日訊號。
- 新增純觀測 `execution_latency.jsonl`：只在股數、來源待處理或註冊狀態變更時批次 fsync，記錄完整 state/projection 提交後的真實 publication clock；分鐘估值不變時不重寫此檔。觀測寫入失敗不阻斷沖銷或修改帳務，下一次提交可重試。
- 全成交判定要求原訊號已 durable 註冊、股數契約一致、未成交／待定股數與未完成 sizing 均為零，且沒有 ledger divergence。已取消的未知價目標仍是未完成，不因取消而達標。原本缺價 reason 屬於註冊歷史，之後真實補齊成交時不會被舊 reason 永遠判失敗。
- 全成交時鐘只接受同一交易日／signal ID 的原生紙上執行紀錄。回放、離線事件時間、重啟後才發現的既有成交、缺少時鐘與零目標現金模式，都不能冒充快速完成的實際成交。

## 成交端基線與必要期限

[整鏈基線](../artifacts/operations/tw_opening_latency_20261014/end_to_end_baseline.json) 保留今天 1 億模式 7,000 股、年度因子化模式 4,000 股未成交，以及其他模式的原始回放政策；沒有將這些數量改成零。原生進度觀測器尚未載入的紀錄保持未知，不回填早於觀測的成交時鐘。

現行正常開盤提交期限為 **09:00:15**，逾期會走既有可追溯 09:01 回放。故 30 秒整鏈的工程預算先設為「訊號原始發布 ≤10 秒、正常執行提交 ≤15 秒、全目標成交 ≤30 秒」，不是「30 秒內發布就算成功」。不為測速延長正常期限或改變回放／成交政策。

## 分階段推進

| 全模式全目標成交目標 | 優先消除的成本 | 驗收 |
| --- | --- | --- |
| 30 秒 | 盤前來源未完成、來源重試被長工作阻擋、額外 30／60 秒等待 | 全模式發布／提交期限先通過，再驗證 09:00:30 前的原生全目標股數提交紀錄；今日未通過 |
| 20 秒 | 輸入尚未就緒後的整批排程等待、重啟導致骨架重載 | 持久 single-flight 與獨立重試已實作並隔離對照；仍須真實開盤全成交驗收 |
| 10 秒 | 盤前骨架與入模視窗重建、來源首次掃描缺口 | 完整五模式 working-set 與 source generation 命中證據；增量／快取前後輸入與輸出完全一致 |
| 5 秒 | 剩餘 CPU 格式化、模型鎖排隊、發布與執行取價 | 固定來源測速加真實開盤測速；所有模式／股數完整，不只最快模式 |
| 1 秒 | 已壓縮的內部關鍵路徑加外部全市場資料到齊成本 | 真實 09:00 的同範圍 P95／P99、失敗日與重連日都保留；現有三路尚無穩定次秒覆蓋的驗收證據 |

總耗時由必要依賴的最長路徑決定，不能靠縮短排程 tick、降低覆蓋門檻或省略驗證得到正確答案。未成交／延緩開盤標的只能使用既有合格缺價輸入 ABI，不能承諾每檔同時產生可成交價。排程 lifetime 已修正；後續 working-set、真實供應商首輪覆蓋與執行來源等待仍是獨立成本。今天首次合格來源觀測約 2.8 秒，本輪不能以離線毫秒結果宣稱整鏈已達一秒或理論極限。

## 驗證與重測

受影響的 18 個 Python 測試檔合計 **757 passed in 25.71s**（[最新正式收據](../artifacts/operations/agent-workflow/runs/opening-terminal-path-regression-20261014T053854-daeb8051/run.json)），包含來源 fan-in、局部退避、訊號發布、原始時鐘恢復、壞時鐘、模擬執行／帳務、慢模式 single-flight、過期工作隔離、通知交付順序、成交進度寫入與失敗重試；不是全庫測試通過。

[三組平衡順序對照及完整狀態投影](../artifacts/operations/tw_opening_latency_20261014/retry_projection_final.json) 使用相同固定價格與來源呼叫次數（慢模式 1 次、快模式 2 次）。快模式缺價後再成功約 **704→307～308 毫秒**，兩模式全部準備完成約 **704→609 毫秒**。這是 600 毫秒慢來源、原 250 毫秒退避與 100 毫秒排程節奏的離線排程對照，沒有真實行情／模型／成交；不是 30 秒或 1 秒開盤達標。

另將同一份 5,328,502 bytes 的完整即時 state 唯讀捕捉後，在私人暫存目錄交替執行三組完整投影，價格、股數、損益與來源狀態的 SHA 全部一致。完整投影約 193～262 毫秒；新增五模式進度批次寫入約 **7.3～19.9 毫秒**，不掃歷史帳本。[前一輪隔離測速](../artifacts/operations/tw_opening_latency_20261014/retry_and_projection_benchmark.json) 為 5.1～7.0 毫秒，較慢樣本也保留。整個投影的 A/B 範圍交疊，不宣稱 observer 讓 full projection 更快；這只量到其新增成本。沒有改寫原 state 或交易 ledgers。

[既有供應商退避比較](../artifacts/operations/tw_opening_latency_20261014/provider_backoff_parity.json) 在完整 2,303 軸、3 組平衡順序的離線 stale-response 情境中維持財務／來源陣列 SHA 相同。每組 12 次 observer 呼叫，退避啟用後 provider 呼叫 12→1、request rows 27,636→2,303。這是既有供應商退避的保留驗證，不是本輪 observer 修正的真實開盤加速結果；沒有額外網路請求、登入或下單。

原始日常紀錄繼續由既有服務自動寫入：

- `artifacts/live/tw_opening_snapshots/YYYY-MM-DD.acquisition.jsonl`
- `artifacts/live/tw_day_trade_simulation/opening_source_repair_latency.jsonl`
- `artifacts/live/tw_day_trade_simulation/opening_signal_latency.jsonl`
- `artifacts/live/tw_day_trade_simulation/latency.jsonl`
- `artifacts/live/tw_day_trade_simulation/execution_latency.jsonl`（原生執行觀測器安全啟用後）

次日或任一天都可重跑分階段稽核，不需啟動模型或再呼叫供應商：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_opening_latency.py \
  --session-date 2026-10-14 --runtime \
  --output artifacts/operations/tw_opening_latency_20261014/retest.json

run_fintech_python -m pytest -q -s \
  test/test_discord_intraday_latency.py \
  test/test_tw_opening_source_recovery.py \
  test/test_tw_opening_latency_audit.py \
  test/test_live_signal_helpers.py \
  test/test_opening_execution_latency.py \
  test/test_tw_day_trade_simulation.py

run_fintech_python scripts/benchmark_tw_opening_retry_dispatch.py \
  --state-file artifacts/live/tw_day_trade_simulation/state.json \
  --output artifacts/operations/tw_opening_latency_20261014/retry_projection_retest.json
```

發布分級位於 `opening_publication_slo.stages`，整鏈全成交分級位於 `opening_execution_slo.stages`；原始失敗、來源分段、完整模式範圍與帳本實際登記時鐘均保留。稽核只讀最後 5,000 筆執行進度，缺少原始時鐘會顯示 incomplete，不為達標掃描／猜測／回寫今天缺少的時鐘。

## 部署讀回

最後一次更新於 11:28，僅重啟 Discord 訊號服務；[部署後稽核](../artifacts/operations/tw_opening_latency_20261014/postdeploy.json) 及 [獨立讀回](../artifacts/operations/agent-workflow/runs/opening-repair-runtime-proof-20261014T033702-1fd098a5/run.log) 驗證新 source observer policy 與目前 owner 一致，原始 attempts 和五個訊號 ID 與部署前完全相同。執行引擎 PID 1371032、InvocationID 與 05:30 開始的 owner 未變；未重新推論今日訊號、未人工改寫帳本、未發出券商訂單。

Discord Gateway 連線正常，帳本 integrity divergence 為 0。來源仍有 1 檔 unresolved，訊號與模擬登記的五級目標今日全部未達標；這些狀態不因部署成功而改成完成。後續依上述分級順序推進，下一次真實開盤前不能把離線測試結果當成 30 秒或 1 秒的成功證據。

13:35 的獨立讀回：當沖引擎仍有 305 筆尾盤部位等待沖銷，因此沒有重啟執行引擎；新成交進度觀測器的工程驗證已通過，但尚不能宣稱原生啟用。Discord 的輸入排程可在來源／互動工作閒置並持有既有 control lease 時獨立更新；其最新部署證據另列於本節，不以程式檔案存在代替原生載入證明。

13:38 已在既有 control lease 下獨立更新 Discord（[原生啟用收據](../artifacts/operations/tw_opening_latency_20261014/dispatch_reload.json)）：新 native run `2503228-1791956281745790605` 實際回報 `session_fenced_single_flight_bounded_poll_v1`，Gateway 與 commands sync 正常，五模式 engine acknowledgement lag 為零。原始 opening attempts 的 SHA 與五個 signal ID 均保持一致；當沖、TAIFEX、行情與公開網站六個受保護 owner 身分未變。這只證明排程修正已載入，不證明下一次開盤 30 秒全成交。

[13:38 更新後的整鏈稽核](../artifacts/operations/tw_opening_latency_20261014/end_to_end_postdispatch.json) 仍忠實顯示全部發布分級 `missed`、全部成交分級 `incomplete`；沒有回寫原始訊號或將今日回放當作正常 09:00 成交。盤後 artifact maintenance 仍有 2 個失敗模式，與 Gateway 正常分開，不以本次排程部署清除來源／維護問題。
