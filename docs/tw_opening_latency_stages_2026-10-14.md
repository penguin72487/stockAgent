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

## 2026-10-15：輸入重建風暴、行情排隊與併發去重修復

本節是 10/15 的新觀測，不改寫上文 10/14 的結果。完整基線在
[baseline.json](../artifacts/operations/tw_opening_latency_20261015/baseline.json)，
修復後獨立只讀稽核在
[final_runtime.json](../artifacts/operations/tw_opening_latency_20261015/final_runtime.json)。
**今天沒有達成 30 秒或 1 秒全模式全目標成交。**

### 從依賴關係拆解

每個模式的關鍵路徑是「合格輸入與盤前骨架皆就緒 → 模型所有權 → 完整推論
→ immutable 發布 → 引擎讀回 → 嚴格晚於訊號的可執行價格 → 股數與帳本提交」。
來源、骨架可以並行；有因果依賴的部分不能靠改時間戳、少算標的或放寬價格證據消除。
全部模式完成取最慢者，不取最快模式，也不把訊號發佈等同全目標成交。

本日官方 regular stock/ETF 名冊為 2,312 檔上市櫃、11 檔停牌、2,301 檔交易範圍，
名冊觀測時間 08:45:01.883217；不是硬編碼 2,303。模式依原 checkpoint 完整軸取交集，
原本 90% resolved-input 門檻保持不變。

| 開盤後 | 有效開盤價 | 官方明確未首筆成交 | 來源未知 |
| --- | ---: | ---: | ---: |
| 1 秒 | 0 | 0 | 2,301 |
| 5 秒 | 183 | 0 | 2,118 |
| 10 秒 | 321 | 11 | 1,969 |
| 30 秒 | 1,442 | 312 | 547 |

這是本機取得且通過驗證的來源證據時間，不代表市場當時沒有成交。
第一輪永豐 IPC 的請求排隊約 **1,925.559 ms**，供應商回傳另約 1,025 ms。
來源不足後，09:10 官方來源更新又使相關 panel generation 失效；曾有六個執行緒
重複進入尾端面板建置。輸入工作的真實進度未傳回 watchdog，導致 60／180 秒
逾時重啟，丟失 warm working set，再次冷建置。不能把這段歸咎於排程喚醒：
本日初次 scheduler wake 只有約 2.45–5.46 ms。

### 實作與不變量

- `signal_engine.py`：相同完整來源／feature key 的 panel cold miss 共用一個 Future；
  不同 key 不互鎖。每個 caller 仍持有原來源 read lease，並各自套用 start boundary。
  例外原樣傳給等待者，下次可以重試；cache epoch 防止舊 build 回填新快取。
  退役不再使用的獨立 get/put helper，避免形成兩條 cache ownership 路徑。
- `live_tail_source_cache.py`：不同 feature panel 同時讀相同 raw file、tail length 與
  source identity 時只解碼一次。維持 64 MiB 上限、Arrow 原值與 null、原本 buffer
  compaction、逐 caller 的讀前／讀後 identity 檢查和 clear epoch，不變更資料或單位。
- `opening_price_acquisition.py`：派送鎖內重讀 resolved mask；修正「樂觀讀取缺價 →
  另一 callback 已完成並移出 inflight → 仍依舊缺價清單重送 delta」的實際競態。
  初次各來源完整範圍、來源獨立退避及未知資料 fail-closed 均保留。
- `quote_provider.py` 與原引擎 runner：在**既有引擎與 SDK session** 內設獨立 inbox
  receiver，不再等待帳本主迴圈完成 readiness、benchmark 或回放工作才接收行情請求。
  只有 canonical verified session 授權後才能服務，跨台北日期自動失效；開盤保護期間
  只收 opening purpose，截止時間由 runner 的原 09:00:15 常數傳入。
  本機 inbox 每 5 ms 觀察不等於每 5 ms 呼叫供應商，沒有新增登入或訂閱。
- Discord：輸入 watchdog 依**真正改變的建置里程碑**計 idle，而非總建置時間；
  相同進度重報或 scheduler poll 不續命，原 hot/cold timeout 不縮短也不放寬。
  `opening_input_dispatch.input_stages` 顯示 elapsed、idle 和 last progress。
  `opening_signal_latency.jsonl` 新增 `input_preparation_stages_ms`，記錄 freshness、
  checkpoint、fingerprint、disk/build/alignment、quote transport/join/validation，
  成功與失敗都留存。這些是 prefetch 內含分段，不能再加到總 prefetch 時間。

不變更任何模型／checkpoint、90% 覆蓋、合法價格界線、股張單位、因果 bid/ask、
深度／容量、原始訊號時間或已成交帳本；沒有人工改寫今日帳本或呼叫券商下單。

### 可重跑的配對測速及驗證

[七輪交錯配對結果](../artifacts/operations/tw_opening_latency_20261015/ownership_benchmark_pinned_baseline.json)
使用完整 2,301 檔名單、真正的 filesystem IPC，以及固定的離線 provider fixture。
注入 **200 ms 帳本主迴圈阻塞**後，IPC 往返中位數由 **212.180 → 13.526 ms**，
約 **15.7 倍**；服務排隊由 205.579 → 5.570 ms。這不是今天實盤的 before/after，
更不是供應商 RTT、模型耗時或真實全成交。

原始資料另凍結一份實際 2330 raw 檔及其 logical parts：三個同時讀者的解碼中位數
**3 次 → 1 次**。兩邊都使用 64 MiB cache、相同 compaction；基線讀取器固定在
`79ea789f97d0d5829c40b365289b8a42b0ac51b1`，不以停用 cache 代替原實作。
此小檔測速牆鐘 29.091 → 28.716 ms，差異很小，**不宣稱全市場建置快了三倍**。
所有 Arrow schema／欄位值及 IPC 價格、覆蓋、時間欄位都驗證一致。

2026-10-15 Git 歷史清理後，測速程式的可執行基準改用對應的
`8b094a64b720eeac7cd84b6515bb37566bc129d7`；下文 attachment 基準
`c90e44b370fd41901140192545d07b03fde899db` 對應
`c6f4c70465c43efd4e9ed8d6314c92a4dfa977a9`。兩份基準程式 blob 均逐一確認未變，
原測速收據中的舊 SHA 保留為當時證據，不能拿來當清理後可用的 Git revision。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_ownership.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-15.json \
  --output artifacts/operations/tw_opening_latency_20261015/ownership_retest.json
```

共享輸入、來源一致性、fan-in、延遲稽核、進場持久化、當沖執行與恢復、
raw-tail reader 和測速工具共 **601 項測試通過（37.44 秒）**。
[正式執行紀錄](../artifacts/operations/agent-workflow/runs/opening-shared-input-regression-20261015T014517-4fcc9d58/run.log)
保留測試範圍；不等同全 repository 測試已全部完成。

### 原生採用與剩餘邊界

截至 09:49 的前一階段沒有手動重啟正式服務。Discord 原有 watchdog 在 09:29:39 自動重啟後，
已實際回報新真進度契約，startup panel 亦出現 `shared-rebuilt`；沒有再因持續進度
中的該次建置被 180 秒逾時打斷。五模式原始訊號在 **09:32:25–09:32:48** 發布，
原始延誤仍保留；稍後五模式均被原有引擎接收。09:01 重建保持既有回放標記，
不是追溯成功的 09:00 live fill，更不是本輪手動帳本修復。

當沖引擎仍是 10/14 14:11:51 啟動的 MainPID **2588255**、NRestarts=0，
`production_order_possible=false`。盤中有持倉，因此**獨立行情 receiver、最新 raw
single-flight、delta 競態修正與新增分段計時尚未全部載入正式 owner**；需要盤後
確認平倉、來源／推論工作閒置後，以原有服務控制路徑安全更新並獨立驗證。
不能以程式檔已修改、Git 已提交或離線測速通過冒充正式採用。

09:49 的[隔離恢復驗證](../artifacts/operations/tw_opening_latency_20261015/restart_recovery.json)
使用原始 checkpoint／events，在隔離目錄建構同一個引擎，精確恢復當時 722 筆非零
庫存、訊號 ID、目標與已消費訊號集合；沒有啟動行情 API 或改寫正式 state。
[獨立帳本前綴讀回](../artifacts/operations/tw_opening_latency_20261015/ledger_prefix_readback.json)
亦通過。這是 checkpoint 冷恢復與 append-only 前綴證明，不是已重啟正式服務或
Windows 實體冷開機驗收。盤後 artifact maintenance 的 factorized 失敗仍獨立顯示
`degraded`，沒有因 Gateway 與訊號恢復而清除。

1 秒目標仍需要來源能及時覆蓋完整需求，並量測完整模型與真實可執行價路徑。
[永豐官方 Snapshot 文件](https://sinotrade.github.io/tutor/market_data/snapshot/)
明確區分 request-type snapshot 與即時行情，並警告不要以盤中重複輪詢充當即時串流。
本輪不增加 Snapshot 輪詢頻率，也不藉放寬證據或公開權限換取假性達標。
下一次真正開盤，仍須用既有 acquisition、immutable publication 和 execution receipts
依 30／20／10／5／1 秒逐級驗收。

## Discord 與完整特徵共享續修（2026-10-15）

### 再定位：基礎面板去重不等於整條輸入鏈去重

同一份基礎面板後面仍有 Factorized 成員驗證／掛接與 checkpoint 標的對齊。
原本這兩段都是「讀 cache → 不在 → 建置 → 寫 cache」，暖機與 opening prefetch
同時 miss 時會建立兩個完整 attachment、兩份解碼 source cache，以及重做對齊。
基礎面板 single-flight 不會自動消除這兩個獨立競態。

沿用同一個 Future／epoch／失敗傳播機制，分別保留原有 LRU 容量（attachment 2、
alignment 8），以 cache namespace 隔離 key。不新增無界 dense 常駐區，也不刪特徵。
掛接 key 包含完整成員 identity，前後都確認 generation；每個原始成員仍逐一驗 SHA。
只有真正驗完一個成員才更新 `factorized_member_verify` 進度，重報不讓 watchdog 續命。
來源變更、builder 失敗或 clear epoch 仍拒絕錯誤結果，下個請求可正常重試。

### 實測範圍與可重跑命令

[三輪交錯配對測速](../artifacts/operations/tw_opening_latency_20261015/factorized_stage_benchmark.json)
使用正式 checkpoint `eb27db1a15ac…`、完整 **48 × 2,757 × 14,726** 特徵範圍，
與固定 commit `c90e44b370fd41901140192545d07b03fde899db` 的原始函式比較，
不是停用 cache 的替身。兩個同時讀者的 attachment／alignment／解碼 cache 數均為
**2 → 1**；牆鐘中位數 **825.465 → 726.306 ms（約減少 12.0%）**。
首輪原版 3,259 ms、共享版 726 ms 受 OS 快取先後影響，不宣稱穩定快 4.5 倍。

測速逐成員驗 SHA，並比較完整基礎資料、日期／標的／feature schema identity；
完整輸出位元比較另由 CPU／CUDA 小型回歸測試涵蓋，不把來源 hash 冒充完整 dense
輸出位元逐筆比對。這個實測排除基礎面板準備（本次 disk tier 328.223 ms）、模型、
報價、發布與成交；API／CUDA／帳本寫入均為零，**不能當成 09:00 全流程已達一秒**。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_ownership.py \
  --factorized-market-config services/discord_bot/markets/tw_day_trade_factorized_scale_separated_cash.yaml \
  --repeats 3 \
  --output artifacts/operations/tw_opening_latency_20261015/factorized_stage_retest.json
```

### Discord 版本與冷恢復驗收

實機第一次獨立更新後，Gateway 與 22 個命令同步正常，五個原訊號皆被識別為
`already_recorded_after_restart`，但新程序暖機一直停在 `pending`。根因是暖機排程
把「需要補發訊號」當成「需要暖模型」的條件；訊號已完成後的重啟因此無法恢復
常駐工作集。該次 [未通過驗收的收據](../artifacts/operations/tw_opening_latency_20261015/discord_native_reload.json)
保留 `failed_after_restart`，不是把 Gateway 已連線冒充完整 readiness。

修正後，已驗證交易日、開盤保護期之後且 13:20 之前的冷程序，仍可經原來的
`price_source=panel`、`write=False`、`ensure_previous_signal=False` 路徑暖機。
既有訊號、持倉與待成交委託完全由原引擎繼續處理；正在跑的開盤推論與使用者工作
仍優先，不增加 SDK session、報價或對外訊息。Discord 額外在啟動時凍結 entrypoint
SHA，與模型／資料 implementation SHA 分別驗證；不在每個心跳掃描全部程式。

同一個 reload 工具預設只 plan，必須 `--apply` 才重啟。Discord-only 操作持有既有
control lease，重新檢查閒置、來源 read lease、原訊號 ID，再獨立驗證新 invocation、
程式 identity、當前程序的完整暖機／resident flag、engine ACK、六個受保護 owner
未更換，以及三個 append-only 帳本前綴。閒置的 `pending` 可進入修復計畫，不能被
驗收成 warm；running／未知工作、來源寫入或持倉引擎重啟條件不符仍 fail closed。

```bash
source scripts/runtime_env.sh
# 僅計畫，沒有 restart。
run_fintech_python scripts/reload_tw_day_trade_services_if_flat.py --discord-only
# 明確套用；不重啟當沖、隔日沖、行情或網站 owner。
run_fintech_python scripts/reload_tw_day_trade_services_if_flat.py \
  --discord-only --apply --timeout-seconds 240 \
  --output artifacts/operations/tw_opening_latency_20261015/discord_recovery_acceptance.json
```

10:20:48 的[正式接受收據](../artifacts/operations/tw_opening_latency_20261015/discord_recovery_acceptance.json)
已驗證 Discord Invocation `21325aec3c0d4048b50d571bdfbfb03c`、22 個命令同步、
同程序 **5/5 暖機完成，20.830 秒**。這是 disk-tier 的完整冷程序暖機，並非開盤
執行時間，也不是與先前重建 tier 的 179.461 秒公平配對；不據此宣稱快八倍。
模型／資料 code SHA `bffc9c7dcc29…` 與 entrypoint SHA `2e4af63c67b7…` 一致，
六個受保護 owner、五模式原始訊號及 events／orders／fills 的既有前綴均未改動。
暖機沒有行情請求、發布新訊號或人工帳本寫入；原執行引擎在期間繼續自然追加。
[之後的獨立讀回](../artifacts/operations/tw_opening_latency_20261015/discord_accepted_runtime.json)
仍確認五模式全數 ready／registered、resident flag 與帳本完整性成立。

### 精確 ACK 的採樣混疊修正

實機另發現 guardian 每隔完整一秒採樣，可能與兩個獨立心跳的相位固定，連續看到
一個 revision 的差距。不能把「新鮮」或允許 lag=1 當作解法；新增的共用 reader
只在最多兩秒內每 50 ms 重讀小型收據，前後夾讀 engine，要求同一 run、同一穩定
revision、Discord 精確 ACK，仍由 caller 檢查 freshness／市場／paper-only。
guardian 與 reload acceptance 共用這個 reader，不讀大型 state、不取價或下單。

[實機只讀驗收](../artifacts/operations/tw_opening_latency_20261015/exact_revision_guardian/latest.json)
在 457.343 ms、9 次觀察後取得 lag=0 的穩定 pair；之後原有自動 guardian 也已回報
新觀察契約與 lag=0（一次觀察 5.031 ms）。這是同步觀察耗時，不是新增到每筆交易
的等待，更不是行情／成交延遲。真的持續落後、run 不同、缺檔、破損或讀取中換版
仍不能通過驗收。

本次擴大回歸 **896 項通過（43.57 秒）**，為保留正式 GPU 的空間，該輪明確
排除 15 項 CUDA 測試；前面的 **226 項**完整階段回歸已包含小型 CPU／CUDA
輸入與輸出位元比較，不能把兩輪重複項相加。最後 ACK／guardian／cutover／
Discord 回歸另 **138 項通過（15.31 秒）**。正式 GPU 五模式暖機已實際成功，
沒有在第二份程序並行展開正式 5 GiB 輸入，也不宣稱全 repository 測試通過。

- [896 項回歸紀錄](../artifacts/operations/agent-workflow/runs/opening-discord-final-regression-20261015T022227-a4bc7825/run.log)
- [精確 ACK／恢復回歸紀錄](../artifacts/operations/agent-workflow/runs/discord-exact-revision-observer--20261015T022614-04182038/run.log)

完整歷史 artifact maintenance 仍有獨立來源問題：最新失敗指出目前 Factorized
projection 只有 210 個日期（2025-12-01～2026-10-14），相對完整 benchmark
尚缺 2,906 個交易日（2014-01-06～2025-11-28）。本次不縮短正式歷史、不改成綠燈，
也不在盤中另起完整歷史 GPU 工作；須由完整來源準備與正式 artifact 驗收後消除此項。

完整健康檢查仍如實失敗：今天錯過真正開盤 SLA；磁碟約剩 30 GiB，但只有約
1.5%，未達現有 5% 門檻；另有隔離的 OpenBB／registered-data 維護警示。
本輪沒有刪資料、放寬磁碟門檻或把回放改成即時成交。當沖執行引擎仍維持原
Invocation `8b2b6ae35570419c822af8a0e7851b19`，盤中不重啟；最新獨立行情
receiver 等引擎內改進仍待盤後原生載入驗收。下一次真正開盤的一秒 SLO 未被宣稱完成。

## 開盤來源快取落盤與恢復續修（2026-10-15）

### 根因與共用修正

`persist_tw_opening_snapshot_cache` 原本在全域行情 cache lock 內完成整份資料正規化、
JSON 編碼、file fsync、rename 及 directory fsync；`seed(persist=True)` 又從同一把
RLock 內呼叫它。因此獨立供應商沒有網路相依，仍會被別人的落盤阻塞，並拖延
cache-only 模型讀取。新增並行測試先在舊版重現三個 timeout，不以服務 active
或單次熱快取命中作為此問題已解決的證據。

- 沿用 canonical receipt writer；只有寫入者共用 writer lock。短暫持有行情鎖時
  擷取 copy-on-write row map、完整 scope 及 input revision，編碼與落盤移到行情鎖外。
- 每次原始事實或 requested scope 改變才推進 revision。舊 flush 不清除晚到資料的
  dirty 標記，後者必須另過 durable barrier；連續報價也不使單次 flush 追趕無窮尾端。
- 同時檢查實際 generation 物件和交易日，拒絕跨日或 root A→B→A 的過期提交。
  scope repair 也走相同 merge/flush owner，移除其直接寫入的旁路。
- 另重現並修正 scope-only 寫入的恢復漏洞：即使行情原本 clean，官方名冊版本改變
  後也先保留 dirty，直到 file **和** directory fsync 成功。rename 後看見新 fingerprint，
  不代表斷電可恢復；directory fsync 失敗後重試仍必須重新寫入及同步。
- 保留官方交易名冊、原價格／數量／時鐘、缺價、來源衝突、舊 scope receipt、檔案與
  目錄同步，不增加供應商請求、不降低覆蓋門檻、不寫訊號或人工改帳。

### 完整範圍配對測速，包含未改善的項目

延伸原有 `benchmark_tw_opening_ownership.py`，不建立另一套 acquisition pipeline。
使用實際完整 **2,301** 檔 requested axis、2,300 筆保留的 source rows，在相同輸出
檔案系統的私人暫存目錄做三組交錯 A/B。原版函式取自固定 commit
`5a9383607ddb46fa4951d8f42c0de64afba8d8d0`，不是停用 cache 的替代版本。
每組比較全數價格欄位、float64 位元、時鐘、缺值、衝突與 receipt scope；清除記憶體
cache 冷恢復後，兩版完整輸出 SHA 一致，且每次寫入皆有 file/directory barriers。

以下為中位數，單位 ms；注入的 100 ms 是明確的儲存尾延遲情境，不是真實供應商延遲：

| 隔離情境 | 另一來源合併：舊→新 | cache-only 讀取：舊→新 | 首次 durable barrier：舊→新 |
| --- | ---: | ---: | ---: |
| 無額外儲存延遲 | 169.781→68.592 | 194.879→134.546 | 123.896→229.809 |
| 注入 100 ms 儲存等待 | 273.063→38.468 | 306.145→56.727 | 231.567→265.922 |

移除鎖內 I/O 確實消除「慢碟必然阻塞其他來源」；**沒有證實落盤本身更快**。
這個完整並行 fixture 也增加同程序的 CPU/GIL 競爭，首個 durable barrier 反而變慢；
不能把合併／讀取改善的倍數套在全鏈成交上，或把 CPU 競爭全部歸因於實體 fsync。
先前兩輪較慢／較快樣本原樣保留，不選最好結果冒充穩定加速。

- [完整 100 ms 情境與全部樣本](../artifacts/operations/tw_opening_latency_20261015/cache_flush_stall_final.json)
- [完整零額外等待情境與全部樣本](../artifacts/operations/tw_opening_latency_20261015/cache_flush_no_stall_final.json)
- [舊版並行鎖失敗證據](../artifacts/operations/agent-workflow/runs/opening-cache-lock-baseline-20261015T033131-30d4e3f8/run.log)
- [scope-only 同步漏洞失敗證據](../artifacts/operations/agent-workflow/runs/opening-scope-durability-baselin-20261015T034335-152b0501/run.log)

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_ownership.py \
  --cache-flush --receipt artifacts/live/tw_opening_snapshots/2026-10-15.json \
  --stall-ms 100 --repeats 3 \
  --output artifacts/operations/tw_opening_latency_20261015/cache_flush_retest.json
```

測速沒有 API 呼叫、GPU 推論、正式 receipt 改寫或帳本寫入；不是下一次 09:00
的供應商、模型、發布、可執行價格或全目標成交驗收。

### 持續測速與回歸

成功的 quorum/cache-hit transport 新增 `opening_durable_flush`，每日 acquisition
quorum event 及 provider finished event 新增 `input_flush`。契約為
`captured_opening_input_revision_durable_flush_v1`，保留 writer queue、snapshot capture、
scope check、durable write、commit ACK、total ms，以及捕捉的 revision、row/scope count、
是否實際重寫與是否仍有晚到資料。無重寫時 durable-write 為零；儲存失敗不輸出成功量測。
provider 的 `last_total_ms` 保持原來來源／驗證邊界，另外記錄
`source_and_persistence_ms`，不把重疊跨度相加，也不因儲存失敗丟掉原來源錯誤。
沿用原本事件檔與 fsync，不另加每個階段的檔案或新背景 writer。

[最終受影響回歸](../artifacts/operations/agent-workflow/runs/opening-input-cache-final-regres-20261015T034702-1724f2fc/run.log)
**403 passed in 22.15s**，涵蓋來源 fan-in、並行故障／恢復、官方名冊、持久性、
開盤 pipeline、Discord 排程、stream acquisition、交易時鐘與完整冷恢復測速。
上述隔離測速／回歸未啟動正式模型或 CUDA，Ruff 與 `git diff --check` 通過；
後續原生 Discord 暖機另列於下節，不是全庫測試通過。

### 原生採用邊界

[11:47 只讀核對](../artifacts/operations/agent-workflow/runs/opening-cache-native-readback-20261015T034733-4038d8a8/run.log)
取得穩定同 run/revision 的精確 ACK（lag=0），五模式原訊號 ID 不變，當沖引擎完整性
已接受，production orders 仍為 false。Discord 同程序 5/5 模型已暖機；六個受保護
owner 未更換。但其他工作更新了模型程式，磁碟 implementation SHA 與常駐 admission
已不同，不能以舊 resident flag 宣稱新程式已接受。

因此僅以既有 qualified Discord-only reload 執行新版本採用；要求新 invocation、
完整本程序暖機、版本相符、精確 engine ACK、原訊號與三帳本前綴及六 owner 不變。
這不授權重啟有持倉的當沖引擎；獨立行情 receiver 等引擎內修正仍待安全時段驗收。
真正 09:00→全模式全目標成交的一秒 SLO 仍未達成，歷史 artifact maintenance
缺來源等獨立問題也不由本項修改變成正常。

11:50:21 的[新版本原生接受收據](../artifacts/operations/tw_opening_latency_20261015/cache_fix_discord_acceptance.json)
確認 Discord 新 Invocation `c9ec41f2fc234753b93e230f2e3afec8`、run
`1839325-1792036199633974854`，載入的 model/data SHA 與目前磁碟
`69a70bc12ef7…` 相符，五模式在本程序完成暖機（18.058 秒）。這不是全開盤時間或
嚴格配對的冷啟動加速。精確 ACK lag=0，原五訊號、三帳本前綴及六受保護 owner
驗證均通過；當沖引擎仍為原 Invocation `8b2b6ae35570419c822af8a0e7851b19`。

新的日常 acquisition 紀錄已實際出現三供應商 `input_flush` 契約，不僅是離線 fixture。
例如 11:50:13.218540 官方 MIS 捕捉 2,300 rows、2,301 requested symbols，
快照擷取 0.558 ms、durable write 84.889 ms，並如實標記有晚到資料；後續永豐
flush 的 writer queue 43.703 ms、durable write 64.112 ms，完成後才清除 pending。
這些是盤中補缺資料的實際樣本，不是 09:00 全市場首次取價或成交達標證據。

[11:51 部署後獨立稽核](../artifacts/operations/tw_opening_latency_20261015/cache_fix_postdeploy_runtime.json)
仍保留今天全部訊號發布耗時 **1,968,937.844 ms**，發布五級 SLO 全為 missed，
成交五級全為 incomplete。當時 2,245 個有效開盤價、56 個缺開盤價保持真實缺失，
其中 55 個有官方未首筆成交證據、1 個來源未知，並非將所有缺價判成網路失敗。
來源修復仍為 waiting_source；帳本 divergence=0、Gateway ready 與 artifact maintenance
degraded 分開呈現，不將部署接受或局部測速解讀成「全部問題已修好」。

[11:52 的第二次獨立原生讀回](../artifacts/operations/agent-workflow/runs/opening-cache-final-native-ack-20261015T035222-503cab40/run.log)
再次確認精確 ACK、原引擎 run、5/5 本程序 resident 模型、新磁碟與載入 identity
相符；這項驗證不取行情、不發訊號、不寫帳，亦沒有把 degraded 維護狀態覆蓋掉。

## 盤後獨立行情 receiver 原生採用（2026-10-15 14:38）

上午的 receiver 工程測試不等於執行引擎已載入。這次先核對五模式實際空倉、
沒有未完成 sizing／剩餘委託，再透過原有 qualified reload 新增的 `--engine-only`
選項更新唯一執行引擎；沒有重啟健康的 Discord、另開 SDK 登入或重算當日訊號。

### 恢復流程的兩項誤判與防護

- 閒置引擎的 `published_at` 是最後一筆帳本提交，不是服務心跳。現在以新鮮的
  `heartbeat_at` 證明存活，仍要求 Discord ACK 的 commit 時鐘完全相同，且不晚於
  心跳；過期、未來、缺失及錯誤 ACK 均拒絕，不製造虛假的新帳本提交。
- `pending_entry_orders` 保留的 **399 筆**都是已完成歷史列。只有明確 `filled`、
  整數 `remaining_shares=0` 且 `sizing_pending=false` 才不算待執行；原列全部保留。
  未知／取消／布林股數／缺 sizing 或非零剩餘股數仍阻擋引擎更新。
- 更新前重新核對來源 read lease、程式 SHA 與服務 owner；engine-only 還要求
  不會重啟的 Discord 已載入相同 code／entrypoint，避免重啟後才發現無法接受。
  本輪曾被來源更新鎖、控制租約或過期心跳安全拒絕；未搶占、刪資料或放寬規則。
- 更新後必須是新 engine run、原 Discord run、精確 revision ACK、原五訊號、
  完整持倉／待處理委託／會計投影一致、三帳本前綴及六受保護 owner 不變。
  Discord 盤後釋放 GPU 不被誤認為故障；若更換 Discord，原本本程序完整暖機與
  residency 驗收仍必要，不能套用這項 engine-only 豁免。

### 完整範圍測速，分開看本機與供應商

新增 `--ipc-only` 到原測速入口，避免不相關的 feature source lease 阻擋本機 IPC
診斷。七組交錯配對使用完整 **2,301 檔**與真實檔案 IPC，但供應商是明確的
固定 fixture，注入 200 ms 帳本主迴圈阻塞。所有 snapshot 陣列的 dtype、形狀與
位元均一致，未省略價格、數量、試撮旗標或任何時間欄位。

| 隔離測量中位數 | 原主迴圈接收 | 獨立 receiver |
| --- | ---: | ---: |
| IPC 往返 | 233.533 ms | 28.651 ms |
| server 排隊 | 202.748 ms | 3.935 ms |

這一段約 **8.15 倍**，不是全部取價、模型或成交加速。
[全部樣本與完整輸出比對](../artifacts/operations/tw_opening_latency_20261015/receiver_complete_axis_ipc.json)
可獨立重跑：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_ownership.py --ipc-only \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-15.json \
  --repeats 7 --stall-ms 200 \
  --output artifacts/operations/tw_opening_latency_20261015/receiver_ipc_retest.json
```

[14:38 三路實際網路測試](../artifacts/operations/tw_opening_latency_20261015/receiver_native_sources.json)
從私人空 receipt 開始，使用同一完整名冊，永豐／官方 MIS／FinMind 同時派送。
quorum 返回 **1,129.873 ms**，當時有效開盤價 2,281、已取得合格輸入證據 2,291；
三路完成約 1,758.320 ms，最後 2,281 個有效開盤價、19 個明確未首筆成交、
1 個未知。沒有把缺成交當報價失敗或用中點補開盤。

永豐的原生 response 回報 `server_pid=2239751`、
`server_receiver_thread=tw-shared-quote-owner`，server 排隊 26.417 ms、
provider fetch 767.506 ms、client total 881.633 ms。這是盤後、已登入連線的
真實測量，並非冷登入、09:00 覆蓋、模型輸出或全目標成交；不能直接與上午
1,925.559 ms 排隊當作受控 A/B。來源連線預熱另花 1,135.163 ms，未藏入零成本。

### 原生接受、回歸與剩餘界線

[14:38:36 原生接受](../artifacts/operations/tw_opening_latency_20261015/receiver_engine_native_postsource.json)
確認新 engine Invocation `1187b92a8b9546fca6ddfc20ad73a053`、新 run
`95baa637a5ee4fceae6b100bd48f74ef`，Discord run
`2122668-1792043056514291291` 不變；精確 ACK lag=0，持倉會計完全恢復，
原五訊號及三帳本前綴驗證通過。先前的隔離冷恢復亦確認相同 inventory，沒有
用回放、重算或人工帳本修改來通過更新。

[14:41:54 第二次獨立只讀驗收](../artifacts/operations/agent-workflow/runs/opening-receiver-fresh-native-re-20261015T064151-1142d8f6/run.log)
再次取得新鮮、同 run／revision 的精確 ACK（lag=0），所有 owner 與原五訊號不變。
前一次單點採樣因心跳過期正確拒絕；重查仍維持原 10 秒 freshness 上限，不准許
過期收據或 revision 落後一格來通過。

[最終受影響回歸](../artifacts/operations/agent-workflow/runs/opening-receiver-admission-final-20261015T064020-1cb9798e/run.log)
**371 passed in 33.09s**，涵蓋 IPC、完整欄位 oracle、qualified reload、失敗前置
拒絕、ACK、冷恢復、官方名冊、來源 fan-in／重試、Discord 排程與成交時鐘。
Ruff 與 `git diff --check` 通過；不是全 repository 測試通過。

[部署後獨立稽核](../artifacts/operations/tw_opening_latency_20261015/receiver_postdeploy_runtime.json)
仍保留今天全部訊號發布 **1,968,937.844 ms**，30／20／10／5／1 秒全部 missed，
全目標成交全部 incomplete。Gateway ready、帳本 integrity ready／divergence=0，
與 Factorized 完整歷史來源缺口造成的 artifact maintenance degraded 分開。
磁碟仍只有約 24 GiB 空間、約 1%，未擅自刪除來源或放寬空間健康門檻。

內部排隊修正已原生啟用；**真正 09:00→全模式全目標成交一秒目標尚未達成**。
下一次開盤仍須依既有 acquisition／input stages／publication／execution 原始時鐘
驗收；不得把本節盤後 quorum 或離線 IPC 結果當成整鏈成功。

## FinMind 開盤補查與 11 秒目標（2026-10-16）

最新驗收目標是 **09:00 到全部五模式訊號與全部目標股數完成模擬成交 ≤11 秒**。
這是整鏈工程目標，不是供應商或交易所的保證；不能靠昨日價、未成交價、回放、
零目標或只完成部分模式通過。原模型、完整官方清單、90% 合格輸入契約、整張／
資格／因果報價與實際流動性規則不變，沒有券商下單或人工修改帳本。

### 真實開盤證據

[今日原始稽核](../artifacts/operations/tw_opening_latency_20261016/baseline.json)
使用當日官方名冊 **2,301 檔**，不是固定延用昨日總數。三路在 09:00:00.29 左右
並行啟動；第10秒只有449個有效開盤價，沒有合格的明確未成交證據，仍有1,852個
來源未知。這不是「市場只有449檔成交」的結論。

- FinMind 首批 09:00:01.459540 返回，payload 2,883 列，合格當日開盤價為0；
  第二輪直到 09:00:12.4686 才返回部分有效資料。它已參與補價，並非漏接。
- 官方 MIS 首輪29個80-channel請求全部 ReadTimeout，整輪約8.93秒。
  Shioaji 的獨立 receiver 已載入；本輪讀回 server queue 約4～17毫秒，沒有再把
  幾秒帳本等待誤認成 SDK 網路耗時。
- 08:00驗收曾成功；08:44:08審查程式變更使08:45起的 exact audit cache 失效。
  比對前後收據，當時唯一變更依賴是 `scripts/audit_tw_public_data_layer.py`，
  不是價格／除權息來源 bytes 改變。不能把舊程式的驗收冒充新版本已驗收。
- 09:07的原生追補實際重建公開特徵，再做 strict audit。合法寫入仍需獨占；
  沒有殺掉資料 owner 或在混合版本上推論。

實測 FinMind 09:08 的2,300個匹配列只有1,641列屬於當日，其餘仍是更早交易日。
`total_volume`、`open` 都為正不代表當日資料。後續新 adapter 的真實紀錄也保留
`other_session_rows`，不能用本機收到時間替換交易時間。

### 共用機制修正

1. FinMind 的「約十秒更新」與 client poll 分開：09:00前11秒使用1秒的 **request-start**
   最短間距，之後回到10秒；不再「請求完成後再睡十秒」。官方更新相位並未承諾
   對齊09:00，短暫補查避免剛好錯過第一個新版本。
2. 全市場 snapshot endpoint 以 provider 為單位 single-flight，不讓不同模型的
   disjoint symbol delta 重複送出相同的全市場請求。保留所有原請求標的與成功資料。
3. HTTP／schema 正常但全部列為舊日是 `waiting_source`，不累積成 transport outage；
   真正連線、權限、402／429仍保留較長退避。新增逐類拒收計數，但不記錄密鑰、
   exception文字或將 bid／ask 填成模型開盤價。
4. 08:30驗收先持有 shared read lease，只有確實需要 builder 才升為 exclusive，
   升鎖後重驗相依版本。唯讀與最後 receipt fence 都排除 writer、允許 inference
   reader；無變動路徑的完整依賴掃描由7次減為3次。每次真正重建後仍重新驗證，
   沒有跳過 SHA、日期或來源完整性。bulk opening defer 的實作修正為文件所述09:10，
   不再09:05就提前開工。
5. 既有稽核新增 **11,000毫秒**的來源覆蓋、全部模式發布及全部目標成交驗收點，
   原30／20／10／5／1秒階段仍保留。缺模式、部分成交、缺 sizing 或回放均不通過。
6. qualified reload 若啟動期間程式代次變更，現在立即保留失敗證據，不白等完整
   暖機 timeout 或自行反覆重啟。這不放寬 loaded-code admission。

### 測速與未通過的邊界

[三組平衡順序對照](../artifacts/operations/tw_opening_latency_20261016/finmind_cadence_paired.json)
執行 pinned 原始 owner 與新 owner，保留2,301檔完整軸、驗證與真實暫存 fsync。
在明確設定「來源第9秒更新、每次傳輸1.2秒、poll 0.1秒」的**虛擬供應時鐘**下，
完整來源到齊由12,400降為10,300毫秒，來源值及 exchange clock SHA完全一致。
新制需8次、原制2次模擬請求；這是短暫增加補查換取較小更新相位等待，不是免費
的上游加速。測試沒有網路、模型、訊號發布或成交，不代表實際開盤已達11秒。

[80-channel](../artifacts/operations/tw_opening_latency_20261016/mis80.json) 與
[20-channel](../artifacts/operations/tw_opening_latency_20261016/mis20.json) 都使用完整
名冊、空的隔離 receipt、16 workers 與4秒 request budget，沒有額外永豐登入。
兩者都遇到 MIS RemoteDisconnected、有效來源0，不能以5.34／5.07秒的失敗返回
宣稱加速成功。因此沒有採用未驗證的更小批次或加倍併發。單檔正常／直接路由與
既有 native transport probe 也失敗；當時 MIS 可用性仍是獨立未解問題。

FinMind [官方說明](https://finmind.github.io/tutor/TaiwanMarket/RealTime/) 是約10秒更新，
不是全市場首筆價10秒內到齊的 SLA；[配額](https://finmind.github.io/api_usage_count/)
與 [錯誤退避](https://finmind.github.io/BanIPPolicy/) 也必須遵守。
09:15的本機官方帳號探針為 SponsorPro、20,000 requests/hour、已用1,059；這只證明
該觀測時點的 entitlement／quota，不代表開盤資料完整或11秒成交保證。

受影響的10個測試檔最終重跑完成
[**520 passed in 15.57s**](../artifacts/operations/agent-workflow/runs/opening-20261016-final-shared-re-20261016T012543-0f1c5091/run.log)。
來源事件測試另外固定注入的盤後時鐘，避免測試把真實早晨的 retry completion 與
固定盤後 observation 混用。13個受影響 Python 檔的 Ruff 與定點 `git diff --check`
通過。這不是全庫測試通過，也不是今日11秒整鏈已通過。

### 09:27 交付邊界：程式並行修改仍阻擋暖機

原生08:30資料檢查於09:22:03完成，當時全部156來源、同日資格與 strict audit
接受；09:22:13再用同代次 receipt 重用。這是原本執行中的 producer 完成，不是
聲稱本輪新的 shared final fence 已被舊程序載入。

第一輪獨立 Discord 更新於09:19:46啟動，但另一份工作在09:19:50修改
`stockagent/data/news_dataset*.py`，使整個 inference implementation identity
改變。qualified reload 保留為
[failed_after_restart](../artifacts/operations/tw_opening_latency_20261016/discord_reload_apply.json)，
沒有放寬 loaded-code 檢查。執行引擎、行情 owner、隔日沖與網站 owner 均未重啟。

確認沒有正在執行的 Discord 訊號工作後，09:25:53使用既有的
`--discord-only --code-admission-only` 恢復新程序。當時
[原生 code／Gateway／ACK 接受](../artifacts/operations/tw_opening_latency_20261016/discord_reload_recovery.json)
通過、帳本前綴與受保護 owner 不變；此模式明確 `gpu_warmup_verified=false`，
不是五模型 ready 的證據。09:26:08、09:26:24又有同一組新聞模組的磁碟變更；
獨立讀回確認五模式 `LoadedImplementationChanged`、暖機 **0/5**。不能將較早的
code-only 接受寫成現在已完整恢復，也不再自動反覆重啟。當時建議暫停新聞修改，
但後續查明這是推論相依範圍過廣，而非新聞真的被推論使用；該建議已由下節的
共用相依修正取代，不要求新聞工作或資料下載暫停。

[09:26獨立稽核](../artifacts/operations/tw_opening_latency_20261016/verified_runtime.json)
保留：第11秒449個有效開盤價、1,852個來源未知；今日五模式訊號發布0/5、
全目標成交 SLO `incomplete`。引擎 run
`ae27688aa8ea40a7ad945986d0f42cd1` 與 ledger integrity ready 不變，
Discord Gateway connected／core ready 與推論暖機 degraded 分開呈現。
沒有人工重建訊號、回寫帳本、下券商單，或把10/15訊號當成今日開盤訊號。

下次驗收必須同時有穩定程式代次、當日來源覆蓋、同 run 的5/5 resident warmup、
精確引擎 ACK，再驗證原始09:00→全部訊號→全部原目標成交的11秒時鐘。
若來源、首筆成交或可成交量不足，保留真實失敗與重試，不保證不存在的市場流動性。

### Discord 相依範圍修正與今日訊號恢復（2026-10-16）

根因是 `completed_inference_cache.implementation_identity()` 遞迴納入整個
`stockagent/data`、`stockagent/models` 目錄，未被 Discord 推論使用的新聞程式
也會改變 code identity。修復點在共用 identity，而不是新聞黑名單、關閉版本
檢查，或要求另一個工作停工。

- 現在從 canonical `signal_engine.py` 與 inference cache 入口解析本機 Python
  import 相依閉包；包含函式內 lazy import、相對 import、package initializer、
  循環 import 與字面值 dynamic import。當前218個來源檔沒有新聞模組，仍涵蓋
  模型、特徵、價格規則、帳務與持久快取依賴。不執行可變的訓練 runner dispatch
  來發現推論相依；實際推論 model factory 使用明確 import。
- syntax 以 path＋SHA 有界快取；閉包以來源 generation、已存在及尚未存在的
  import candidate／父目錄 membership 重驗。未使用檔案的新增、修改、刪除不改
  identity；真實依賴修改、新增／刪除被 import 的模組或發現期間變更仍拒絕舊
  owner。回傳值不可反向改寫內部快取，沒有將新磁碟版本冒充舊程序已載入版本。
- [原生測速](../artifacts/operations/agent-workflow/runs/opening-20261016-scoped-identity-20261016T013632-dd22785d/run.log)：
  全新程序首次相依發現5,903.024毫秒；其後21次完整 generation 重驗中位數
  **8.609毫秒**、最大13.025毫秒。冷掃描與穩態分開，這不是模型推論或開盤整鏈
  已達毫秒的證據；測速沒有來源請求、模型 forward、訊號或帳本寫入。
- [最終8檔回歸](../artifacts/operations/agent-workflow/runs/opening-20261016-scoped-inferenc-20261016T013520-4a6ea1ab/run.log)
  **350 passed in70.53s**。涵蓋 unrelated news 獨立性、真實依賴失效、還原mtime、
  lazy／initializer／namespace／dynamic import、程式競態、快取、暖機及 qualified
  reload。這不是全庫測試；也不與前一輪520個重疊測試相加冒充獨立測試總數。
  另外以實存測試入口完成
  [回放／恢復／公開閘道256項](../artifacts/operations/agent-workflow/runs/opening-20261016-recovery-public-20261016T014211-1236eef1/run.log)
  （40.59秒）及
  [ACK／原生驗證40項](../artifacts/operations/agent-workflow/runs/opening-20261016-service-ack-reg-20261016T014244-09a5ad9e/run.log)
  （9.95秒）；定點 Ruff、diff whitespace 與兩份文件的本機連結目標檢查通過。

09:34首次新 plan 的 apply 因正在執行工作而被拒，保留
[拒絕收據](../artifacts/operations/tw_opening_latency_20261016/scoped_discord_apply.json)。
確認 idle 後，09:36只重啟 Discord owner；
[code／Gateway／ACK 接受](../artifacts/operations/tw_opening_latency_20261016/scoped_discord_idle_apply.json)
仍明確 `gpu_warmup_verified=false`。隨後另行讀回同 run 原生暖機：09:36:08.221開始、
09:36:38.300完成，**5/5 resident ready、30.079秒**。
[獨立原生驗證](../artifacts/operations/agent-workflow/runs/opening-20261016-discord-scoped--20261016T014010-6b8af40e/run.log)
核對 loaded code、Gateway、command sync、暖機、精確引擎 ACK 與當次 invocation
沒有 watchdog／fatal。執行引擎 run `ae27688aa8ea40a7ad945986d0f42cd1` 未替換，
該次切換前後受保護 owner 與原帳本前綴保留，沒有券商下單或人工寫帳。
後續 journal 另觀察公開閘道09:38:17 stop/start、09:38:32恢復 listening；這不在
本次 Discord reload 的操作範圍，不能宣稱該閘道全程沒有換 owner。已重新讀回
外部 HTTPS status／signals，保留 `simulation_only=true`、`production_order_possible=false`
及回放原始訊號時鐘；公開頁面的 liveness 不作為五模式成交完成證明。

今天五份 immutable 訊號已由原生排程在09:36:32.996～09:36:40.967發布。
[09:37獨立稽核](../artifacts/operations/tw_opening_latency_20261016/repaired_runtime.json)
確認5/5 published、4/5進入今日引擎；1億模式仍等自己的09:01來源價格，其他模式
不受其網路補查阻擋。錯過開盤的恢復沿用既有 `official_open_signal_0900_execute_0901_vwap`
契約，明確標示 counterfactual 模擬回放，保留實際09:36原始訊號時鐘，不是當時
成交，更不能通過今日11秒整鏈SLO。歷史資料維護的 Factorized 全歷史基準缺口仍
另列 degraded，不把 Gateway、當日推論與全歷史 artifact maintenance 混成一個狀態。

後續原生補查在 **09:47:36完成761/761檔09:01價格**，無 unresolved／unqueried／
retry／error／流量停止；既有引擎隨後登錄1億模式，沒有另起寫帳程序。
[09:48最終稽核](../artifacts/operations/tw_opening_latency_20261016/recovered_final_runtime.json)
確認今日5/5訊號 ready、5/5 engine registered，原引擎 run 不變。
[09:50跨層獨立驗收](../artifacts/operations/agent-workflow/runs/opening-20261016-five-mode-curl--20261016T014948-1682a23d/run.log)
再次核對五個 immutable signal ID、磁碟／載入 code 與 entrypoint、同run五模式暖機、
exact ACK零落後、公網五模式 active及明確回放標記。四模式已達完整目標股數；
09:50:07的1億模式為2,211,000/2,709,000股，498,000股仍等新鮮正常盤報價，
由原引擎持續重試，不能改價或捏造流動性來宣稱全部成交。

公網驗收保留客戶端邊界：
[Python預設HTTP客戶端的嘗試](../artifacts/operations/agent-workflow/runs/opening-20261016-five-mode-publi-20261016T014901-39e46969/run.log)
收到403，不能宣稱所有客戶端都已通過；後續使用原本已驗證可通行、正常驗證TLS的
curl路徑完成上述公網驗收，沒有降低WAF或略過憑證檢查。403的具體規則尚未定位。
今日11秒全部真實開盤模擬成交仍未通過；維護 degraded與待成交保持原狀呈現。

### 11秒日常驗收與獨立 FinMind 喚醒（2026-10-16）

原始來源紀錄仍顯示：第11秒只有449/2,301檔有效開盤價、1,852檔未知，
FinMind當時有效開盤價為0。這不是API回覆200就可視為資料齊備。
[官方即時資料文件](https://finmind.github.io/tutor/TaiwanMarket/RealTime/)
明載股票快照約10秒更新、Sponsor限定，支援一次全市場；它不是09:00:10一定
完整更新的SLA。保留來源成交時間、當日open與試撮合檢查，不能用昨日價、
bid/ask中點、局部訊號或補登回放換取11秒成功。

本輪定位到額外的本機等待：快照已回覆但尚無當日開盤價時，FinMind下一次
補查仍可能等MIS/券商慢工作或整個4.5秒caller wait結束。canonical
`OpeningPriceFanIn.fetch`新增明確的`poll_opening_snapshots`，由共享condition
在開盤前11秒內按FinMind自己的request-start時鐘喚醒；推論與原生來源修復
均接同一個入口。未改價格、標的數、模型門檻、目標股數與帳務。

- 第一輪仍對所有已設定來源請求完整範圍；共享snapshot single-flight防止
  五模式重複送全市場請求。已成功的價格保留，之後只補未解析delta，達quorum
  即返回；不在caller返回後另建永久輪詢程序。
- 健康開盤快照的1秒cadence僅用於原有11秒窗口，其他時間仍10秒；來源
  timeout、401/402/403/429、quota與permission backoff保留，不把快輪詢套到
  失敗帳號。取消／超時、關閉owner、晚到callback與同時多模式均有回歸測試。
  [FinMind封鎖政策](https://finmind.github.io/BanIPPolicy/)也不能被低延遲要求略過。
- [完整2,301檔成對比較](../artifacts/operations/tw_opening_latency_20261016/independent_poll_comparison.json)：
  3輪交替before/after、共6個樣本，完整quorum中位數 **831.170→369.712毫秒**
  （約2.25倍）。價格／open／mask／交易所時間戳SHA一致，每輪完整持久化。
  這是受控來源更新0.2秒、poll0.1秒、慢來源0.65秒的本機排程比較，無外部API、
  模型或帳本寫入；不是實際FinMind更新延遲，也不是11秒端到端已通過。
- [五模式完整輸入準備探針](../artifacts/operations/tw_opening_latency_20261016/input_preparation_probe.json)：
  新程序首用 **9,143.221毫秒**，後兩次保留canonical重驗為
  **280.153／258.358毫秒**。冷路徑含panel fingerprint、Factorized member
  verification與checkpoint alignment；暖快取不可冒充冷成本已消失。
  沒有網路、CUDA初始化、model forward或訊號／帳本寫入；可能寫入既有衍生panel
  cache，收據已明載。`benchmark_opening_preparation.py --inputs-only`保留此重測入口。

#### 正式每日開盤觀察，不放進交易關鍵路徑

沿用`audit_tw_opening_latency.py --daily`，不匯入Discord bot、Torch或報價owner。
新增canonical `stockagent-tw-opening-audit.service/timer`，於Asia/Taipei
08:55、09:00:11、09:00:31、09:01及13:40執行，`Persistent=true`。
service使用`PrivateNetwork=true`、`ProtectSystem=strict`及低CPU/IO weight，
只讀原生狀態／journal，僅可寫`artifacts/live`；不取價、推論、寫交易或重啟。

每日證據放在`artifacts/live/tw_day_trade_simulation/opening_audits/YYYY-MM-DD/`：

- `scope.json`只建立一次，保留實際盤前時鐘與完整enabled模式；晚開機、原始
  時鐘不合格、損壞scope或中途減少模式，不能取得盤前驗收。
- `first_assessment_11000ms.json`及其他1/5/10/20/30秒門檻只建立一次。
  使用原始durable publication與execution commit時鐘，要求全部原目標，保留
  無需交易、缺mode、缺來源、未完成sizing、未全成交與replay的不同狀態。
  回放09:01事件不是當時執行；後來補價或成交不得覆蓋首次判定。
- `latest.json`保留各模式取價／輸入準備／模型／publication spans、失敗次數、
  來源覆蓋及同run暖機上下文；`daily.jsonl`append-only保存歷次摘要。
  觀察owner使用實際讀回時鐘，不能把observer開始後才發布的狀態算成負年齡。
  盤前`armed`只代表觀察範圍已鎖定，並不宣稱行情或模型已ready。
- 休市／日曆未確認不冒充交易日成功；journal損壞、缺時鐘或缺模式明確為
  unverified。audit程序exit0只證明觀察完成，不代表11秒SLO成功。

[正式service原生驗證](../artifacts/operations/agent-workflow/runs/opening-daily-systemd-final-20261016T040916-c7d53ed3/run.json)
確認`Result=success`、timer enabled/active、下一次13:40排程。
當時12:09:20的每日讀回記錄為`missed_or_unverified`；當天午間才安裝，沒有倒填08:55 scope。
原 `artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-16/latest.json` 目前不在 checkout，
這裡保留當時的觀察，不把缺少的原生收據改由後續重測冒充。
原始第11秒有效覆蓋仍為449/2,301，五份訊號原始發布時間仍為09:36。

#### 新版載入及驗收界線

只以既有qualified流程更新Discord，保留盤中執行、行情與網站owner。
[plan](../artifacts/operations/tw_opening_latency_20261016/independent_poll_discord_plan.json)
及[apply](../artifacts/operations/tw_opening_latency_20261016/independent_poll_discord_apply.json)
保存原帳本前綴與五份immutable signal ID；12:04:51切換Discord，新run為
`2145804-1792123489420830419`，loaded code為
`6a2d4d5dc2964b6da311ff69ca1fd75f5ddd0eecfcf1d1d7287b76eb2bffc566`。
code-only接受仍明確`gpu_warmup_verified=false`；另行
[同owner原生驗收](../artifacts/operations/tw_opening_latency_20261016/independent_poll_native_acceptance.json)
核對12:04:52.102～12:05:22.490暖機 **5/5 resident、30.388秒**、Gateway/core、
command sync、該invocation無watchdog/fatal、精確ACK零落後及GPU device handles。
執行引擎run `ae27688aa8ea40a7ad945986d0f42cd1`未替換，受保護owner不變。
沒有人工寫帳或券商下單。

12:07:53的一億模式仍為2,688,000/2,709,000股，21,000股等待新鮮正常盤報價；
其他四模式完整。Factorized全歷史artifact maintenance另列degraded，未塗綠。
[原生整鏈稽核](../artifacts/operations/tw_opening_latency_20261016/independent_poll_runtime.json)
仍保留今日publication約2,200,966.893毫秒、execution incomplete及回放排除。

最終[開盤／observer四檔測試160項](../artifacts/operations/agent-workflow/runs/opening-observer-final-regressio-20261016T040913-39686d5e/run.log)
通過（20.35秒），另[相依／輸入／reload／ACK／Discord八檔353項](../artifacts/operations/agent-workflow/runs/opening-wider-regression-20261016T035940-447ee130/run.log)
通過（79.82秒）；非全庫測試。新增測試曾因fixture漏填`PriceSnapshot.source`
被拒，已按真實ABI補正並重跑；原生驗證曾誤把只回傳None的freshness斷言
當成布林值，修正呼叫方式後通過，未放寬freshness、ACK或市場規則。

這次接受的是排程瓶頸修正、持久測速與當前owner恢復；**不是每交易日11秒保證**。
下一個真實交易日的09:00整鏈尚未發生。外部資料發布、首筆成交、網路與真實可成交
條件仍可能超標；只有每日原生證據能驗收，不能以本機毫秒探針宣稱只剩來源延遲。

### FinMind 首筆／全量測速與449檔根因（2026-10-16）

本次保留官方完整2,301檔範圍，不以provider覆蓋反過來刪除官方標的。
[原始開盤重讀](../artifacts/operations/tw_opening_latency_20261016/finmind_original_opening_forensics.json)
確認第11秒449檔全部來自永豐，其餘1,852檔未知，沒有可證明的no-first-print。
下表為09:00起算的原生觀察時鐘，不是券商成交時鐘：

| 來源 | 首次請求 | 首次有效開盤價 | 第11秒有效數 | 直接證據與瓶頸 |
| --- | ---: | ---: | ---: | --- |
| 永豐共享行情 | 317.64 ms | 1,329.52 ms | 449 | 首輪2,300筆中2,295筆仍為舊／盤前資料，只有5筆有效；未回覆1檔不擅自刪除。 |
| FinMind | 292.65 ms | 12,476.54 ms | 0 | 首個callback在1,459.54 ms收到2,883列但有效數0；第二次請求到11,776.18 ms才開始。 |
| 官方MIS | 292.37 ms | 15,308.95 ms | 0 | 首輪29/29個HTTP請求ReadTimeout，16 workers、80 channels、4秒read budget形成約9秒長尾。 |

FinMind首輪當時沒有保存原始body或逐列拒絕理由，因此不能事後斷言「全部是昨天價」
或「全部無成交量」。舊adapter還會先篩掉已由其他來源取得的股票；它的90/1,797等
紀錄只是consumer gap下界，**不能拿來推論FinMind全市場完整率或全量到達時刻**。
既有第11秒判定及09:36回放訊號均未改寫。

#### 已實作的本機修正與完整測量

- 沿用canonical fan-in，新增process-scoped FinMind connection／decoded snapshot owner。
  thread pool換worker也不丟棄TLS連線；跨模式借用同一次完整回覆，僅投影各自的標的軸。
  交易日、09:00 gate、PID及token rotation都會失效舊view。沿用前節的開盤獨立喚醒，
  不等MIS或caller timeout；失敗／quota backoff不被force-network診斷繞過。
- 保留原始received／exchange clocks，重用回覆明確標記`provider_response_reused`。
  每個完整回覆另存`provider_snapshot`證據與snapshot ID，不再把模型缺口當provider範圍。
  紀錄HTTP開始／headers／完整body／JSON／全市場驗證、worker queue、diagnostics、
  cache merge、耐久flush，以及失敗phase／冷卻。headers到達不冒稱精確的socket首byte；
  收完整body也不等於全部股票已有當日首筆成交。
- 一份完整body只decode／檢查一次；沿用既有`private_json`快速解析及相容fallback，
  避免重複NumPy scalar裝箱、None解析例外與重複JSON工作。來源事件合併append/fsync、
  receipt使用compact JSON，仍保留完整數值、標的、驗證、時鐘及耐久barrier。
- 獨立daily observer新增各來源首筆、50/90/95/99/100%正價覆蓋、完整payload自身覆蓋、
  逐項缺價原因與HTTP snapshot去重；舊證據仍標為lower bound。不存在或不合格的價格
  不補成0、昨日價或bid/ask中點，也不把未知直接改成no-first-print。

#### 實測：保留慢樣本，不挑最快數字

[同原始回覆、五組交錯before/after](../artifacts/operations/tw_opening_latency_20261016/finmind_deployed_local_comparison.json)
保留2,301檔完整投影與耐久寫入，本機完整流程中位數為
**245.47 → 193.15 ms（約21.31%減少、1.27倍）**；價格、mask、exchange及receipt clocks
SHA完全一致。這組在qualified reload同時期執行，包含當時主機競爭，沒有外部網路。
[前一組同範圍測量](../artifacts/operations/tw_opening_latency_20261016/finmind_final_spans_comparison.json)
為167.73 → 99.30 ms；兩組都保留，不把99.30 ms宣稱為每日固定延遲。
更早[首版完整流程](../artifacts/operations/tw_opening_latency_20261016/finmind_retained_comparison.json)
曾因新增量測成本回歸，後續才繼續消除重複工作；未用縮小資料或略過fsync取得加速。

[三次實際網路測量](../artifacts/operations/tw_opening_latency_20261016/finmind_final_network.json)
各自強制新的HTTP回覆，不使用decoded-view cache；完整來源流程為
**990.40／509.55／677.13 ms**，有效數均2,257/2,301。
request到headers為325.57／146.45／335.04 ms，body為245.89／120.83／120.20 ms，
JSON為14.12／7.10／8.61 ms，全市場驗證為59.97（首次）／12.89／12.66 ms。
這是午間網路與本機stage，不是09:00首筆出現延遲，更不是全市場完整取得或成交SLO。
本機成對測試與不同時刻網路測速分開，不把網路波動算成程式加速。

MIS另作了完整2,301檔對照：
[4 workers／80 channels](../artifacts/operations/tw_opening_latency_20261016/mis_four_worker.json)
29/29個請求成功，約10.07秒，2,256正價＋43筆明確no-first-print，仍缺2檔；
[16 workers試驗](../artifacts/operations/tw_opening_latency_20261016/mis_current_transport.json)
則有大量timeout／無效回覆。canonical source-local預設及benchmark現改為4，保留明確override，
**不是網站全域rate limit，也未證明4在所有網況永遠最優**。
[20 channels小批次](../artifacts/operations/tw_opening_latency_20261016/mis_small_batch.json)
雖約3.24秒返回，但只有1,845正價＋34 no-first-print，遭連線拒絕／502且中止剩餘dispatch，
不能作為加速成功；拒絕的具體遠端原因未證實。
[下一組prewarm](../artifacts/operations/tw_opening_latency_20261016/mis_eight_worker.json)
也被拒，故未繼續全量打點或規避來源保護。

#### 正式owner接受、剩餘界線與重測

[qualified Discord-only apply](../artifacts/operations/tw_opening_latency_20261016/finmind_owner_discord_apply.json)
與[獨立原生驗收](../artifacts/operations/tw_opening_latency_20261016/finmind_native_acceptance.json)
確認新run `2235509-1792126067324785233`、loaded code
`248cd0f2d6bed9843409c639d7430c51f58c6d19c84882cb3ef379b69e454a0b`、Gateway/core ready、
同run五模式resident ready（12:47:50.272～12:48:15.403，25.13秒）、exact ACK零落後、
當次invocation無watchdog/fatal。六個受保護service owner前後不變，執行引擎仍為
`ae27688aa8ea40a7ad945986d0f42cd1`；原帳本前綴保留，無人工寫帳、券商下單或引擎重啟。

正式observer已讀到新owner的完整FinMind payload證據：12:48:03為**2,258/2,301**，
42檔其他交易日、1檔（00838B）未出現在回覆中。這些是未知，不證明不交易或不在官方名單。
今天早上的449/2,301不會被午間補價覆蓋。一億模式仍有21,000股等待新鮮正常盤報價；
公開health與Factorized全歷史maintenance仍如實degraded，不宣稱全部成交或全系統已正常。

[最新12檔共569項回歸](../artifacts/operations/agent-workflow/runs/finmind-deployed-shared-regressi-20261016T044822-5ff640fb/run.log)
通過（47.97秒）；涵蓋source owner、完整範圍／時鐘、失敗backoff、cache、observer、模型輸入、
official universe、quote provider、qualified reload及Discord，不是全庫測試。
前輪534／264項測試與本輪重疊，不能相加冒充獨立數量。

重測沿用現有入口，網路診斷保留來源冷卻，不碰正式行情receipt或帳本：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-16.json \
  --finmind-only --rounds 3 --output /PRIVATE/finmind-network.json
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-16.json \
  --compare-finmind-decoder artifacts/operations/tw_opening_latency_20261016/pre_finmind_full_snapshot.py \
  --baseline-quote-owner artifacts/operations/tw_opening_latency_20261016/pre_finmind_quote_provider.py \
  --finmind-payload artifacts/operations/tw_opening_latency_20261016/finmind_retained_response.json \
  --rounds 5 --output /PRIVATE/finmind-local-parity.json
```

`/PRIVATE`須替換為既有私人輸出目錄。測試body只保留在本機0600，未提交Git或公開行情。
下一個真實交易日由既有daily timer保存首筆與完整覆蓋時間，不能在午間重演09:00來冒充實績。
外部發布相位／首筆成交／provider缺檔／quota／WAN不可由本機保證；但**本機仍有約0.1～0.2秒
完整工作、09:00到首次dispatch約0.3秒的歷史成本及冷啟動／推論／發布／執行的獨立驗收**，
尚不能宣稱已把所有可控瓶頸消除、只剩外部限制，或保證11秒／1秒全量成交。

### MIS 29筆讀取逾時續查（2026-10-16盤後）

再次核對09:00首輪：29個請求全部`ReadTimeout`，單次約4.05～4.73秒，16 workers
跑成兩波，first pass 8,925.85 ms、source total 9,108.62 ms。這是讀取路徑未在預算內
完成，不是模型運算、整張股數或開盤價驗證拒收。原紀錄未拆出DNS／TCP／TLS／headers／
body，**無法事後斷言一定被封IP、特定防火牆或MIS內部哪個節點過載**。
高併發／共用session可能放大遠端排隊，但沒有服務端證據，不能把相關性寫成確定根因。

14:46同機單筆出站TLS檢查為DNS約7.41 ms、TCP連線完成約19.34 ms、TLS完成約47.59 ms、
第一個HTTP回應約71.64 ms；正常驗證憑證。另以canonical quote provider取得有效回覆
約81.90 ms，見[單筆診斷](../artifacts/operations/tw_opening_latency_20261016/mis_before_stage_probe.json)。
這排除的是「此刻持續失效」的說法，不能回推開盤當時的網路與MIS負載。

[修正前的四併發完整對照](../artifacts/operations/tw_opening_latency_20261016/mis_prechange_afternoon.json)
29/29成功、1,744.28 ms；
[分段測速版](../artifacts/operations/tw_opening_latency_20261016/mis_staged_afternoon.json)
29/29成功、1,769.78 ms。兩輪均完整請求2,301檔、2,274筆正價＋26筆明確no-first-print，
還有1檔未知；沒有刪標的、沿用正式熱行情receipt或另登入券商。不同時段比12:34的
10.07秒更快，不能把這個變化都算成程式加速，也不能當作09:00保證。

修到的本機缺陷是：所有連線讀取逾時仍接續發出下一波；四併發若整源異常，甚至可能
等更多波。現在連續4個transport failure（包含read timeout）停止未發出的批次，
仍等待已送出的工作並保留其中成功回覆；原請求範圍與deferred channel數完整保留，
不冒稱已查完或已取得價格。既有source owner按失敗冷卻重試未解析delta，其他provider
獨立繼續。這是更快隔離故障，不是把失敗提早返回冒充全量成功。

HTTP使用`stream=True`把headers和body讀取分開，完整消耗或關閉response後才釋放連線；
新增worker queue／session準備／headers／body／JSON spans與`failure_phase`。
連線／TLS／等待headers合列一段，不假裝Requests能再細分socket內部；單筆curl診斷
另有DNS／TCP／TLS證據。HTTP 200之後body逾時，明確列為`body_read`，不是驗證錯誤。
daily observer保留安全的failure-phase摘要、停止原因及未送數量，不複製私密request內容。
另外補正raw MIS snapshot的`requested_count`，即使全失敗也保留完整請求數，而不是預設0。

[最終477項回歸](../artifacts/operations/agent-workflow/runs/mis-timeout-production-final-tes-20261016T065009-b5ce7053/run.log)
通過（38.17秒），涵蓋完整2,301檔逾時故障注入、部分成功不消失、body-timeout分類、
response釋放、既有重試、來源隔離、每日觀察及qualified reload；不是全庫測試。
首次本機測試發現raw requested-count預設0及新fixture漏匯入json，已修正並重新測試。

正式載入沿用Discord-only qualified流程；
[第一次apply](../artifacts/operations/tw_opening_latency_20261016/mis_timeout_guard_discord_apply.json)
被真正的官方source writer以`TWPublicSourceUpdatePending`拒絕，沒有先重啟。
只對此可重試狀態做有界重試，不殺producer、不清鎖或繞過一致性驗收。

後續[第二次重試](../artifacts/operations/tw_opening_latency_20261016/mis_timeout_guard_discord_retry_2.json)
因`Discord has active, pending or unknown work`拒絕；另做五分鐘有界空檔觀察，
既有artifact maintenance仍在執行，未要求重啟。
[15:00:57原生讀回](../artifacts/operations/tw_opening_latency_20261016/mis_timeout_native_readback.json)
確認Gateway／core ready、exact ACK revision lag 0、五個signal ID與六個保護owner不變；
**目前程式已修正及測試，正式Discord owner尚未載入，不能宣稱已上線**。
盤後maintenance仍為degraded，未改寫為正常；下一步待工作自然結束及source一致後，
再走相同qualified流程並獨立驗收載入版本。09:00端到端SLO仍未通過開盤實測。

### MIS完整回應成本與無首筆成交昨收暫估（2026-10-16續查）

使用者確認：當天尚無首筆成交時，模型先用前一交易日收盤價。現有gap ABI已是
`log(previous_close / previous_close) = 0`；本輪保留數值、模型權重及成交契約，
補上`opening_no_print_model_price_policy`與每檔`model_input_price_basis`說明。
前收數值沿用既有`panel_price`欄，不新增可能繞過公網價格授權檢查的行情別名。
僅當日有效來源明確證明no-first-print才能計入resolved inputs；逾時、缺檔、
昨日報價、盤前試撮不是這個證明。原始`open_prices`仍缺值，observed-open mask仍false，
實際可用的當日開盤價到達後自然取代零gap；不把暫估基準寫成券商或模擬成交。

新增完整保留回應比較到既有`benchmark_tw_opening_sources.py`，不建立另一條行情管線：
SHA驗證每筆原始body、禁止網路與券商登入、固定本機測試時鐘並保留原交換所時間，
每輪從空receipt執行同一fan-in、所有29批、全2,301檔、驗證、journal與真正fsync。
金融欄位／時間戳及durable rows均做SHA完全一致驗收。body與基準副本只在忽略的私人
artifacts內，沒有提交或公開；重播耗時不是09:00行情到達或成交SLO。

原始保留回應的cProfile辨識到：每批喚醒都會全市場掃描並逐格填入numpy欄位。
改成同一cache lock內的整欄建構，沒有減少欄位、標的、驗證或持久化。
關閉profiling後做五輪交錯A/B（10次完整工作），
[本機完整成本](../artifacts/operations/tw_opening_latency_20261016/mis_efficiency_retained_unprofiled.json)
中位數927.308→759.124 ms，約減18.1%；CPU中位數亦降低。
profiling開啟時會顯著放大工作成本，已提供`--profile-mis-local`明確選項；
不能把早先帶profiling的約1.9秒與一般計時直接相比。

另外修正兩個確定的排隊缺陷：

- optional暖機原本持有所有HTTP worker也要拿的bootstrap lock，最差讓正式取價等
  frontend／API各8秒；現在網路單獨single-flight，state lock只做短讀寫。
  測試讓探測仍被阻塞時，真實quote worker先完成，並驗證response/session釋放。
- 第1個transport failure後不立刻補送下一波；先drain首批，必要才單筆probe確認原有
  連續4次失敗門檻。四worker全逾時只送4批，而非4～7批；健康peer回覆立即恢復
  完整併發，成功資料不丟。來源重試仍由原owner負責，deferred範圍與dispatch policy
  都保留到每日診斷，不把提前隔離寫成全量成功。

[實際網路續測](../artifacts/operations/tw_opening_latency_20261016/mis_efficiency_network_after.json)
29/29 HTTP成功，完整工作3,306.10 ms，2,274筆真實開盤價＋26筆已證無首筆成交，
尚有1檔未知。與保留原始body那輪4,601.80 ms的條件不同（另含錄製成本與網路波動），
不能把差值全當程式加速，也不能宣稱全2,301檔都已取得真正開盤價。

[588項語意回歸](../artifacts/operations/agent-workflow/runs/mis-efficiency-semantic-regressi-20261016T071440-501a439b/run.log)
通過43.54秒；補上每日dispatch policy後另71項測試通過。涵蓋quote／fan-in／模型gap、
factorized window、故障隔離、官方清單、每日audit與qualified reload；不是全庫測試。

正式採用依使用者要求延長為30分鐘有界等待，
[等待紀錄](../artifacts/operations/tw_opening_latency_20261016/mis_efficiency_guarded_wait.json)
只在native工作自然空閒、版本一致、source guard允許時呼叫既有Discord-only qualified
流程。不殺資料producer、不清鎖、不改帳本、不重啟受保護的當沖引擎；等待與測試成功
本身不是loaded-code驗收，最終狀態須以qualified apply與獨立原生讀回為準。

重測範例（路徑為本機忽略的私人artifact，不隨Git配送）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-16.json \
  --compare-mis-pipeline artifacts/operations/tw_opening_latency_20261016/mis_quote_before_efficiency.py \
  --mis-responses artifacts/operations/tw_opening_latency_20261016/mis_efficiency_retained_responses.json \
  --rounds 5 --output /PRIVATE/mis-local-parity.json
```

### Provider 清冊、三時段可重測證據與本機投影優化（2026-10-16 18:30）

本輪沿用 `tw-opening-latency-20261015`，不是另一條行情／模型／帳本管線。
完整來源與候選、當前權限和實測見
[Provider 最新清冊](tw_opening_provider_plan_2026-10-13.md#最新盤點與實測2026-10-16)。
重新讀回官方資訊公司清冊；實際可用三路仍為永豐共用連線、MIS、FinMind。
新增查核玉山、台新 Nova／MLAPI、國票，將規格衝突的 TWMD 明確列為未合格，
不把沒有憑證或只查過文件的來源算成可用備援。

本機改動保留原始價格、單位、來源時間、驗證、完整分母及持久性：

- `quote_provider` 的 opening cache row 是 copy-on-write。按實際 row revision 共用已驗證投影，
  不再每個 partial batch 都重做同一行的 float／日期驗證；每次讀取仍檢查 provisional TTL、
  no-first-print retry age 與 generation。來源 row／receipt ABI 不變，返回 numpy arrays 不共用可變記憶體。
- cache-only 讀取不做只供 HTTP missing-list 的額外全市場掃描；一次整欄建立 detached arrays。
  row 替換、日期／root／cache object 切換均讓投影失效。不能將不變的開盤事實當成新鮮可成交 book。
- 每個 owner／job 使用獨立 ID，保存 ordered 與 sorted scope SHA；重複價格的 session 級 dedup
  不再消除後續盤中完整 sweep 的測速證據。`coverage_progress` 隨既有 durable event bundle 保存，
  不額外新增逐筆 fsync；保留首批、50／90／95／99／100% 正開盤與 resolved 的獨立單調時鐘。
- 稽核驗證 job 分母 SHA，delta 不算完整 sweep；舊 schema 沒有 per-job coverage 就寫 unmeasured，
  不是 0 ms。失敗／逾時納入來源耗時摘要；觀察到完整欄位與落盤完成另列，不能混為成交完成。
- 修正實際 retry clock：原先在 dispatch journal fsync 前起算，慢 I/O 可能令兩次外部請求間隔
  短於政策；現在從真正呼叫 provider 的時點起算。未增加帳號額度、永豐輪詢或長期 FinMind 請求頻率。

[五輪交錯完整本機對照](../artifacts/operations/tw_opening_latency_20261016/mis_phase_row_views_abba.json)
包含 2,301 檔、29 份保留 HTTP body、fan-in／所有驗證／journal／真正 fsync。
關閉 profiling 後中位數 **773.839 → 657.215 ms，下降約 15.1%**；
financial／clock arrays 與 durable rows SHA 完全一致。這不是網路、09:00 或成交延遲，
也未證明 657 ms 已是理論極限。相同 upstream bytes 的 A/B 才用於本機歸因，不把不同時段網路變快冒充程式加速。

已安裝並實際驗證原生 systemd 單元：

- `stockagent-tw-opening-audit.timer`：原 08:55、09:00:11／31、09:01、13:40，另加 10:31／15:10；
  service 使用 `PrivateNetwork=true`，沒有 provider API、模型推論或帳本寫入。
- `stockagent-tw-quote-phase-benchmark.timer`：週一至週五 10:30／13:40；
  `Persistent=false`，錯過時不補跑成假開盤證據。當日官方 scope、日曆均驗收後才執行一輪，
  只用 MIS／FinMind（與未來明確設定的 optional 非 broker source），永豐由原生 owner 被動觀察，
  不因 benchmark 加一個登入或定期 snapshots。手動 `--scheduled` 也有相同排他鎖與保護時段。
- scoped installer 為 `bash scripts/install_discord_bot_service.sh --opening-audit-only`；
  只裝／啟用觀察 timer，不重啟 Gateway／engine。兩個 service 均已人工啟動驗收 exit 0，
  探測的 [18:23 原生收據](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-16/postclose_182316141202.json)
  保留完整 scope、首次／覆蓋率／來源與落盤時鐘。被動 observer 今天仍誠實顯示
  `missed_or_unverified`，不將盤後新測速改寫成早上 11 秒已達標。

[正式 592 項語意回歸](../artifacts/operations/agent-workflow/runs/quote-phase-semantic-regression-20261016T102425-ff04e480/run.log)
28.50 秒通過；涵蓋 quote、fan-in、官方清單、故障恢復、五模式輸入、factorized window、
source benchmark、每日 observer 與 qualified reload。另通過 Ruff、Bash 語法與 scoped diff whitespace 檢查，
不是全庫測試。測試先找出同 owner 只補未解 delta 的錯誤 fixture 假設，以及 retry clock 確實偏短的缺陷；
修正後重新驗收，沒有放寬全量／來源驗證來讓測試通過。

先前 30 分鐘安全換版等待的最終結果為 `wait_budget_elapsed_without_safe_idle`，未重啟。
本輪 18:25 工作自然閒置後，沿既有 Discord-only `--code-admission-only` qualified 路徑
[完成換版](../artifacts/operations/tw_opening_latency_20261016/quote_phase_discord_apply.json)：
`accepted_code_admission_only`，驗證完整日誌前綴、原 signal IDs、保護 owner、Gateway／core／exact ACK 及載入程式。
[18:28 獨立原生讀回](../artifacts/operations/tw_opening_latency_20261016/quote_phase_native_readback.json)
再次確認當前 code admission、Gateway connected、core ready、精確 ACK、五個 signal IDs 與六個保護 owner 不變。
獨立讀回先計算 code identity 再取得新 heartbeat pair，避免將耗時檔案核對前的舊 pair
當成當下 freshness；不重寫其時間，也不因診斷持有舊快照再重啟服務。
這是 loaded-code／Gateway／ACK 驗收，**不是 GPU 暖機、09:00 或所有來源零錯誤驗收**。
當沖引擎仍保留原 owner；既有兩筆 pending paper orders 不授權繞過 engine reload gate 或清除委託。

仍未解決：完整分母內 `00838B` 的 ISIN 與七類官方 ETF 商品目錄差異。
原始目錄已保存、聯集及完整性已核對；沒有將歷史 raw、今日訊號或盤前 scope 直接重寫。
需要 canonical universe owner 的後續生命週期規則與安全換版驗收。新監測已啟用，
下一個真實交易日的 09:00／盤中樣本仍待發生，不能承諾不存在首筆成交的股票必有正式開盤價，
也不能以一次盤後無 HTTP 錯誤宣稱外部服務永遠零錯誤。

## 2026-10-18：完整來源處理的增量投影與無等待批次合併

本輪繼續同一個 `tw-opening-latency-20261015`，只處理來源取得與本機完整投影，
沒有重做模型、降低 quorum、刪掉分鐘或分母、修改原訊號／帳本、增加永豐登入。
來源與權限更新見 [10/18 Provider 清冊](tw_opening_provider_plan_2026-10-13.md#最新權限讀回2026-10-18)。

### 原理與實作

延遲必須拆成「外部供價 → 本機收 body／解碼 → 驗證與合併 → 喚醒模型 →
必要的持久化」，不是只縮短 HTTP timeout。這次移除已量到的重複工作：

- Opening cache 的 canonical copy-on-write rows 不變，新增最多八個 consumer 軸的
  **私有衍生 columnar view**；只解析變動 row。每次讀取仍逐次檢查 TTL／無首筆時效，
  返回 detached arrays。日期、root、cache object generation、移除／替換 row 都會失效；
  不是把 provisional data 永久凍結，也不是另一個價格 owner。
- float64 欄位以 numpy 批量檢查 finite 與價格／數量合法範圍，避免每個 row／field
  重複建立 scalar；float32、文字與 object 仍沿用原 scalar 語義，不改精度或股／張。
- MIS 只合併目前 `FIRST_COMPLETED` 已完成集合中、標的不重疊的批次進度。
  **不等待尚未回覆的 peer、不加湊批次 timer**；每筆保留自己的原始市場／接收時鐘。
  兩個未知市場 channel 同時回覆相同標的時先 flush，保留先見開盤與衝突證據。
  每個已交付 cohort 仍走 journal barrier；file／directory fsync 和入模前保存門檻都保留。
- MIS 的 UTF-8 JSON 用既有 exact-integer native decoder；非 UTF-8、BOM、NaN／Infinity、
  任意大整數及非法位元組保留 Requests／stdlib fallback，測試包含 native 可用與不可用。
  不改公開 JSON wire bytes，不用把非有限值轉 null 的 encoder 改寫帳務。
- Fugle 顯式要求 `ALLBUT0999`，包括普通股、特別股與 ETF；正常／錯誤都關閉 response。
  新增 headers、body、decode、projection、failure phase 時鐘，403／429 仍用既有
  provider-local 60 秒退避，不阻擋其他來源。單檔 200 不能當成快照權限。
- 保留完整重測入口，修正 retained Friday 回應在 Sunday 重測的**隔離測試時鐘**，
  不放寬 production current-session reader。recovery 測試固定模擬 09:30，
  不再因實際執行測試時間早於 09:00 而失敗；生產盤前禁止取開盤輸入保持不變。

### 完整對照，不以熱快取或最快樣本取代

[最終五輪交錯對照](../artifacts/operations/tw_opening_latency_20261018/final_complete_abba.json)
每次均為相同 2,301 檔、相同 29 份原始 HTTP body、空隔離 receipt、完整 parsing／fan-in／
journal／真正 fsync；沒有網路、模型、訊號發布或券商成交。
本輪 benchmark 不與自己的 pytest 同時執行，但沒有宣稱整台主機無其他負載。

| 完整本機範圍 | 修正前 | 修正後 |
| --- | ---: | ---: |
| workflow 中位數 | 514.061 ms | 310.592 ms |
| process CPU 中位數 | 508.823 ms | 303.157 ms |
| 每輪 MIS HTTP 請求／原始 body | 29 | 29 |
| 進度 callbacks | 29 | 10～15 |
| 實際 fsync calls（含 journal／必要保存） | 35 | 16～21 |

本機 wall time 下降約 **39.6%，1.66 倍**。較少 barrier 來自「已完成資料一次交付」而非
略過持久性；報告另外保留 fsync sum span，不將可重疊分段硬相加成總耗時。
financial／clock SHA `74a0258fa121ad0c235500a8eb2cb9f0fc06814ea6eb910750ca0303561709da`、
durable rows SHA `248aaf68739d90b49bf3e64a9c0e9c237ce8203a6ff3fbe739924634833014f0`
全部一致，仍為 2,274 正開盤／26 已證無首筆／1 未知，沒有刪掉 `00838B`。

保留[前一輪 445.552→247.364 ms](../artifacts/operations/tw_opening_latency_20261018/cohort_complete_abba.json)
及[加入 decoder／barrier 觀測後 571.177→344.974 ms](../artifacts/operations/tw_opening_latency_20261018/native_complete_abba.json)。
主機負載與 cohort 完成順序會影響絕對值，不把較快的一輪挑出來當長期 P99，
也不以跨輪差異推定 decoder 單項帶來額外加速。

[627 項回歸](../artifacts/operations/agent-workflow/runs/opening-complete-verified-regres-20261018T005458-987a25ba/run.log)
24.06 秒通過；另通過 Ruff。完整收集時第三方 matplotlib／pyparsing 的 deprecated API
會觸發 warnings-as-errors，因此本次執行以 CLI `-W ignore::pyparsing.warnings.PyparsingDeprecationWarning`
排除此第三方警告分類；沒有修改專案正式 warnings policy，也不宣稱全庫測試或該依賴已修好。
新增測試涵蓋 generation／axis／移除／輸出隔離、數值 parity、已完成 cohort、未完成 peer
不得阻塞、衝突不可吞掉、Fugle 拒絕仍關閉／計時／退避且其他來源繼續。

### 原生監測與換版狀態

10/18 08:55 原生 `stockagent-tw-opening-audit.service` 實際執行成功，回報
`calendar_closed_or_unverified`；沒有把休市日寫成 0 ms 成功。盤中／盤後 timer 下一次為
10/19 10:30，13:40；受當日官方日曆／scope 守門，僅做有限非 broker probe。
手動 `--scheduled` 在本日 08:56 正確跳過保護時段，未送額外行情請求。
真實 09:00 來源首批與 50／90／95／99／100% 仍由原生 owner 被動記錄；不競爭開盤資源。

`plan` 已通過；08:56 的 [qualified Discord-only apply](../artifacts/operations/tw_opening_latency_20261018/discord_apply.json)
於重啟**之前**因 `TWPublicSourceUpdatePending` 拒絕，`restart_requested=false`。
08:57 獨立 `lslocks` 證實 `refresh_tw_public_release_archives.py --auto-full-sunday`
PID 1856985 正持有 `/srv/stockagent-live/.locks/tw-public-refresh.lock` 的 WRITE flock，
不是殘留鎖檔；沒有殺 producer 或略過 reference／entitlement 一致性守門。
當沖／隔日沖 engine owner 未變，原五個 signal IDs 與兩筆 pending paper orders 保留。
08:58 獨立核對寫鎖已自然釋放，未中斷原始更新。隨後重新走相同守門的
[qualified apply](../artifacts/operations/tw_opening_latency_20261018/discord_after_source_apply.json)，
08:59:26 完成 `accepted_code_admission_only`：Gateway、core、exact engine ACK、當前 loaded
code、ledger prefixes 及受保護 owner 一致；原五個訊號 ID 與兩筆 pending paper orders 保留。
新 Discord InvocationID 為 `383ced587387428d8a8eaad91649bba0`；當沖、隔日沖 engine 未重啟。
這是 Discord 取價 consumer 的載入驗收，不代表舊 engine 已採用新 quote code、GPU 暖機完成、
交易日全量開盤供價完成或成交 SLA。新開盤／盤中／盤後現場樣本仍待發生。

[09:01:11 獨立讀回](../artifacts/operations/tw_opening_latency_20261018/native_readback.json)
再次確認 current file identity 與 runtime admission digest 一致、Gateway connected、core ready、
exact engine ACK、五個訊號與所有 protected owner 相同。第一次獨立讀回遇到當下過期的 engine
heartbeat，正確拒絕；重新取得**新的** exact pair 後通過，沒有重寫舊時鐘、放寬十秒 freshness
或因此再重啟。09:00:11／31 及 09:01 原生 observer 均保留週日休市狀態，不算交易日驗收。

重測：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/operations/tw_opening_latency_20261016/quote_phase_full_market_network.round1.receipt.json \
  --compare-mis-pipeline artifacts/operations/tw_opening_latency_20261018/quote_before.py \
  --mis-responses artifacts/operations/tw_opening_latency_20261016/mis_efficiency_retained_responses.json \
  --rounds 5 --output /PRIVATE/opening-local-retest.json
```

剩餘邊界：Fugle 全市場權限、官方 `00838B` 生命周期與安全換版、實際交易日外部
來源供價／失敗分布、GPU working set 與執行取價仍各自驗收。
現有本機 CPU 成本約 0.31 秒，不是零；並未證明已到理論極限或「只剩 API 限制」。
先保留這個完整可歸因基線，不用一輪離線成功保證外部錯誤永遠為零或每檔一秒有首筆成交。

## 2026-10-19：HTTP 與本機驗證重疊、三時段測速續修

### 這次改了什麼

- 保持 canonical 三家已驗證來源與完整官方分母，盤前清冊更新仍由獨立原生 owner 負責；
  不為測速新增券商登入、重建歷史帳本或發券商委託。
- MIS 舊排程會在每個已完成 cohort 的驗證、合併與同步 journal/fsync 結束後才補發 HTTP。
  現在先補滿**同一個**有界健康 HTTP window，再處理已到的 cohort；新請求與本機處理重疊，
  不等待新請求、不多開併發，也不把快回應扣在慢 peer 後。
  衝突來源仍分批保留 first-open 順序、各自價格及時計；失敗退避、四次 transport failure 熔斷、
  殘缺 scope、已成功值與檔案／目錄 durability 不變。
- 普通股／ETF 的正式 scope 不含創新板；移除 Fugle TIB 自動抓取／暖機的無用 HTTP，
  保留明確 TIB adapter。Fugle 的不同市場 route 不是不同家獨立供應商。
- 每個成功 MIS request 另記 `row_decode_ms`／`request_to_decoded_rows_ms`；
  每個 cohort 記 assembly、callback、來源啟動後完成時間與行數。
  `quote_phase_full_scope_per_job_latency_v2` 把這些與原有 HTTP／body／JSON、cache merge、
  persistence、首筆及 50/90/95/99/100% positive／resolved 放進同一個 job／時段。
  並行／巢狀 wall spans **不可加總**當端到端；只投影命名、有限非負 numeric 欄，
  不複製 HTTP headers、URL、Token 或未列入白名單的 request 內容。舊欄缺省不是 0 ms。

### 全市場、全流程與同版本控制

沿用 10/16 原始 29 份 HTTP body／完整 2,301 檔 scope，不查網路，保留原始交易所時間。
每輪從空的隔離 receipt 開始，包含 parser、fan-in、journal、真實 fsync 與最後獨立讀回；
每組五輪交錯 A/B/B/A，沒有刪股、刪欄或熱快取代替來源。

| 對照 | before 中位 ms | after 中位 ms | 證據／界線 |
| --- | ---: | ---: | --- |
| 初始重疊候選 | 279.046 | 250.004 | [完整結果](../artifacts/operations/tw_opening_latency_20261019/full_local_overlap_abba.json)；尚未加入所有分段測速 |
| 加入測速後首組 | 354.880 | 330.212 | [完整結果](../artifacts/operations/tw_opening_latency_20261019/full_local_overlap_instrumented_abba.json)；保留與其他工作共用主機的慢樣本 |
| 完整回歸後 | 257.951 | 201.394 | [完整結果](../artifacts/operations/tw_opening_latency_20261019/full_local_overlap_final_abba.json) |
| 相同程式 A/A 控制 | 267.974 | 251.585 | [原始控制](../artifacts/operations/tw_opening_latency_20261019/full_local_same_code_control.json)；相同 SHA 仍約有 6.1% 量測差，不算程式加速 |
| 控制後再次 A/B | 283.167 | 236.934 | [完整重測](../artifacts/operations/tw_opening_latency_20261019/full_local_overlap_repeat_abba.json)；中位約少 16.3%，不是網路或真實開盤加速比例 |

所有對照的最終價格／量／mask／trial／各時計及 durable rows SHA 完全一致，每輪恰好 29 個
原始 request，沒有網路、訊號發布、帳本改寫或下單。保留所有樣本，不挑最快 201 ms 宣稱極限。
本機仍約 0.2–0.33 秒；CPU、排程與必要持久化成本不是零。

原始 10/16 registry 早於 10/18 的 membership contract，不能用今天的 verifier 冒充相容。
離線工具現允許**明確指定**封存 fan-in 與原始 registry verifier，對照記錄其 SHA，
保留舊 verifier 的日期、scope、全部 raw source SHA 驗證；只在離線暫時模組綁定，退出即還原。
正式每日新 membership admission 完全未放寬，scheduled probe 禁止載入離線 owner。
首次不相容與中途投影測試失敗的原始 logs 保留，已由後續同 scope 成功收據解除，而非刪除失敗。

重測沿用 `scripts/benchmark_tw_opening_sources.py --compare-mis-pipeline`，本輪完整命令可讀
[受監督回歸與 ABBA log](../artifacts/operations/agent-workflow/runs/opening-1019-regression-latest-20261018T171816-458fe351/run.log)
及其 run receipt 的 argv；封存 quote／fan-in 位於本輪私有 `artifacts/operations/tw_opening_latency_20261019/`，
不用改寫正式來源或使用先前文件不再相容的 registry 讀法。

### 測試、正式換版與今日監測

五模組 [200 passed／13.25 秒](../artifacts/operations/agent-workflow/runs/opening-1019-regression-latest-20261018T171816-458fe351/run.log)，
另八模組 [402 passed／15.67 秒](../artifacts/operations/agent-workflow/runs/opening-1019-boundary-regression-20261018T171910-8a2f4331/run.log)；
共 602 項，包含 HTTP／progress 同時前進的事件測試、快批不等慢批、衝突不丟、全市場故障注入、
單源失敗不拖其他來源、官方 scope、開盤時鐘、因果成交、持久化及安全換版；不是全庫測試。

[Discord-only plan](../artifacts/operations/tw_opening_latency_20261019/discord_deployment_plan.json)
通過後，[01:19:16 qualified apply](../artifacts/operations/tw_opening_latency_20261019/discord_apply.json)
為 `accepted_code_admission_only`：新 Discord Gateway／core ready、當前 loaded code、exact engine ACK
與三份完整 ledger prefix SHA 均通過，六個受保護 owner 和五個已接受訊號不變。
沒有重啟持有行情與 paper 執行的 engine，也沒有更動其 45 筆既有 pending 模擬委託；
這些委託不是本輪行情優化可以偷偷沖銷的工作。離峰 warmup 為原生 `pending`，不冒稱 GPU warm-ready。

01:21 直接讀回一致，但 01:23 的
[受監督獨立讀回](../artifacts/operations/agent-workflow/runs/opening-1019-independent-readbac-20261018T172308-b422b5fe/run.log)
正確拒絕新舊程式不一致：`stockagent/data_sync/packed_snapshots.py` 在 01:22:45 變動。
這是已完成推論快取恢復的真實相依，不是拿不相干新聞目錄當守門；沒有縮小 admission 或改寫已載入指紋。
等待原生盤後工作閒置後，再經
[新 plan](../artifacts/operations/tw_opening_latency_20261019/discord_after_dependency_plan.json)／
[01:26:22 apply](../artifacts/operations/tw_opening_latency_20261019/discord_after_dependency_apply.json)
完成相同 Discord-only 換版，不中斷其他 owner；另
[412 項相依重驗／11.65 秒](../artifacts/operations/agent-workflow/runs/opening-1019-shared-dependency-t-20261018T172622-158b1812/run.log)
通過，包含重複項，不能與上述 602 直接相加。

[01:26:44 穩定版本獨立讀回](../artifacts/operations/agent-workflow/runs/opening-1019-stable-independent--20261018T172642-909b897b/run.log)
再次驗證相同 code、run、entrypoint、engine ACK／零 revision lag、五個訊號及六個 protected owners。
盤後 research maintenance 仍 `degraded`，不等於 Gateway 或來源 owner 中斷，也未被本輪改成正常。

原生 audit／phase probe／每日官方清冊三個 timer 均 active；下一個時間各為 08:55、10:30、07:30。
01:19 實際執行原生 phase probe 正確退出為
[preopen skip](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-19/preopen_011912108127.json)，
沒有送來源 HTTP、不補跑成假的盤後或開盤測速。正式 09:00 被動記錄仍由同一行情 owner 產生；
09:00:11／31／09:01 驗收，10:30 完整 scope MIS／FinMind probe、10:31 audit，13:40 盤後 probe。
Shioaji 不額外定期輪詢；現場 source-first/full、model、paper fill 和 broker fill 分開。

**尚未證明**：今日真正 09:00 供價／全五模式／進場 SLA、全市場 Fugle 權限、外部錯誤永遠為零、
或已到「只剩 API 限制」的理論極限。每日分母仍按當日官方有效清單，不硬寫離線 2,301 或昨日 2,300。

## 2026-10-19：交易日開盤未達標與盤中續修

### 真實開盤：來源與服務瓶頸分開

當日 08:55 的官方清冊已驗收，`tw_official_active_stock_etf_membership_v1` 生效，
掛牌 2,311 檔、當日策略產品範圍可交易 **2,301 檔**；沒有用來源回價數縮小分母。
[原生 11 秒不可變驗收](../artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-19/first_assessment_11000ms.json)
在 09:00:11.469849 記錄 `passed=false`：591 正開盤、0 已證明尚未首筆、1,710 未解；
永豐 591、FinMind 211、MIS 0，來源間重疊不能相加。五模式原始發布 0／5，
沒有拿 10/16 回放的執行紀錄或之後補來的價格改寫今天的原始成績。

MIS 首輪不是 29 個請求都已送出：健康 window 先送 **4 個**，均 ReadTimeout 後熔斷，
其餘 1,981 channels 保留待後續恢復。四筆約 4.12／4.36／5.57／6.42 秒，
沒有 HTTP status／body，不能斷言 IP 封鎖。原始失敗仍在
[同一來源 journal](../artifacts/live/tw_opening_snapshots/2026-10-19.acquisition.jsonl)。

09:29 的額外單檔、唯讀傳輸診斷：DNS 6.322 ms、TCP 15.073 ms、TLS 35.469 ms、
總計 45.251 ms，`curl (52) Empty reply from server`、HTTP 000，連到 117.56.218.179，沒有代理。
這一筆證明 TCP／TLS 已建立後仍未得到 HTTP 回應；不證明供應者封鎖或其內部壅塞原因，
也不是今天 09:00 每個 timeout 的完整網路追蹤。可重測同一公開 API 的單檔傳輸，
不要因此繞過來源政策、輪換身份或對已拒絕端點送全市場 burst。

本機也有可修原因：請求函式入口前有約 76～449 ms 的排程／開始 journal 等待；
08:49 的實際 live 依賴（config、checkpoint／optimizer 等）改動後，舊 Discord process
正確拒絕推論為 `LoadedImplementationChanged`。這不是不相干 news 檔案，也不是 API 慢。
09:17:58 原生 watchdog 因 Factorized 無進度退出，09:18 由 systemd 恢復；不是本輪人工重啟。
後續 catch-up 仍在運算，不能用 PID 存活或 Gateway 連線宣稱五模式已正常。

### 這次的修正

- 開始 journal 與 provider request 重疊：仍使用同一來源 owner 與日誌鎖，記下實際來源工作起點，
  在 progress journal 前 join 開始收據，結束時亦一定 join。來源返回／報錯都不遺留開始工作。
  開始日誌失敗保留 `dispatch_telemetry_recorded=false`；模型輸入原有落盤／fsync 門檻未省略。
- 另記 `provider_invocation_at_taipei`、入口 offset、journal join wait；validation 時鐘在真正驗證時
  凍結，不能因日誌稍後寫入而改成較晚的假來源時鐘。函式入口不是 HTTP on-wire 或交易所發布證據。
- MIS 預熱直接驗證行情 API 的指定 2330 row，取消先等非報價 `stock/index.jsp` 的無用往返；
  空／錯 row 不續期，失敗清除舊 readiness，API cookies 仍交給相同既有 HTTP workers。
- 多源測速的 MIS 預熱移入自己的 worker。失敗只停該來源的診斷 sweep，不延遲 FinMind 等健康來源，
  不改正式開盤的原生取價規則。每輪 FinMind network probe 強制新 HTTP，而不以解碼快取冒充網速；
  quota／error backoff 仍不可強制穿透。
- 報告增加入口、準備、各輪讀回、整體落盤的時鐘，明列 positive／已證明未首筆／unknown、
  `quorum_reached`、`data_complete`、`resolved_complete`。舊 `all_sources_complete_ms` 只代表來源工作已停，
  新 boundary 明寫不是完整報價。最後報告自身的 fsync span 在受監督 stdout 留存，不自填為零。
  來源 worker 結束後清空隔離程序 cache，經 canonical reader 獨立重讀已落盤收據；
  不補寫來掩蓋 producer 的失敗。新增故障注入證明「記憶體三檔、儲存只剩一檔」仍只能報 partial，
  沒有把檔案存在或記憶體全量當作來源持久化完整。
- Discord prewarm 與即時準備在昂貴來源掃描／builder／舊暖機快取前，先重用既有 loaded-code 驗證。
  推論端原有二次驗證不刪；失配時不再做必然會被拒絕的重建，獨立來源修復仍繼續。
- 驗收工具修正：自然暖機驗證器改用既有 lazy loader，不引用已移除的 `native.BOT`；pending／running
  仍不載入推論。ownership benchmark 改隔離完整 canonical module（含欄位快取類別），
  避免類別方法誤讀另一個 trial 的全域 cache；財務／時鐘／持久化完全相同的門檻未放寬。
  純 panel fixture 也不再為了產生樣本就先載入未使用的 trainer／繪圖依賴，未關閉 warnings-as-errors。

### 實測與對照，不混成一秒成功

| 測量 | 結果 | 邊界 |
| --- | --- | --- |
| 今日原生開盤第 11 秒 | 591／2,301；五模式原始發布 0／5 | 未達標，保留不可變原始結果 |
| 09:11 舊盤中 probe | 約 5.4 秒串行 MIS 預熱後才開始其餘來源；1,827／2,301 | 舊 564 ms 漏算前置工作，不是完整端到端 |
| 09:21 新盤中 probe | 函式入口到讀回 683.570 ms、到最後報告落盤 982.523 ms；1,927／2,301 | 僅 partial；不含程序啟動，不是完整報價或成交 |
| 09:37 最終獨立收據重讀 | 函式入口到讀回 1,424.968 ms、到最後報告落盤 1,476.442 ms；2,019／2,301 | 清空程序 cache，不強制清除 OS cache；282 未解，仍 partial |
| 09:42 FinMind 同一連線兩筆新 HTTP | 第一輪來源工作至獨立重讀 672.714 ms、第二輪 332.813 ms；均 2,039／2,301 | 每輪空 receipt、`provider_response_reused=false`；262 未解，不是全量 |
| 完整保留來源 5 輪 A/B/B/A（初組） | median 321.645 → 279.970 ms | 2,301 軸、29 原始 body、無網路；非開盤 SLA |
| 同範圍最終 A/B | median 322.485 → 312.464 ms | 全財務／時鐘／durable rows SHA 一致 |
| 相同程式 A/A 控制 | median 364.471 → 321.039 ms | 相同 SHA 仍差約 11.9%，大於最終 A/B 的 3.1%，不宣稱穩定均值加速 |

事件式故障注入證明「開始 journal 卡住仍能先送來源請求」及「MIS 預熱卡住時健康來源仍前進」，
但共用主機的平均速度差異受噪聲影響，不能用最佳一輪充當極限。
保留原完整 29 份回應、原始時計、所有欄位與實際 durability；14～15 次 fsync 的 cohort 差異亦保留，
沒有為了固定快分數刪掉 barrier。

- [09:11 原始部分報告](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-19/intraday_091146400005.json)
- [09:21 新報告與完整分時段投影](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-19/intraday_092122739781.json)、
  [落盤總時鐘 stdout](../artifacts/operations/agent-workflow/runs/opening-1019-intraday-independen-20261019T012106-43262abd/run.log)
- [09:37 最終網路與獨立讀回](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-19/intraday_093752299696.json)、
  [最終落盤及程序時間](../artifacts/operations/agent-workflow/runs/opening-1019-durable-intraday-20261019T013749-fe4ff003/run.log)；
  受監督 run 從執行開始到完成為 2.865 秒，包含程序啟動，與函式內 1.476 秒分開。
- [09:42 FinMind 兩次真正 HTTP 與原始時鐘](../artifacts/operations/tw_opening_latency_20261019/finmind_two_fresh_responses.json)：
  headers／body／JSON／全市場驗證第一輪為 289.082／155.233／5.870／28.165 ms，
  第二輪為 141.172／45.013／5.153／7.997 ms；兩輪連同最後報告落盤 1,093.160 ms。
  既有持久連線的作用已核對，沒有把解碼快取當 HTTP；這不是全市場已首筆或程式穩定加速倍率。
- [初組全流程對照](../artifacts/operations/tw_opening_latency_20261019/full_dispatch_overlap_abba.json)、
  [最終 A/B](../artifacts/operations/tw_opening_latency_20261019/full_dispatch_overlap_final_abba.json)、
  [相同 SHA A/A](../artifacts/operations/tw_opening_latency_20261019/dispatch_same_code_control.json)
- [最終 683 passed／33.57 秒](../artifacts/operations/agent-workflow/runs/opening-1019-durable-final-tests-20261019T013631-01e982ed/run.log)，
  [前一輪 682 passed／36.31 秒](../artifacts/operations/agent-workflow/runs/opening-1019-complete-boundaries-20261019T013055-b3b077d1/run.log) 保留，兩輪含重複，不相加；
  涵蓋價格、source fan-in、官方 scope、回復、source read guard、completed cache、Discord 排程、
  發布／暖機與測速隔離；不是全庫測試。此前 16 項及後續 1 項失敗原始 logs 都保留，
  按原因修復後才重驗，沒有從驗收範圍刪掉失敗測試。

### 部署與尚未解除的邊界

[09:24 Discord-only qualified plan](../artifacts/operations/tw_opening_latency_20261019/intraday_qualified_plan.json)
因原生 Factorized catch-up 仍在途而 `blocked`，`restart_requested=false`。
沒有取消 uncancellable builder、偽造 idle、縮小 code admission、重啟 paper engine 或改帳本。
09:18 原生 restart 恰逢當次修改，之後 loaded code 又不一致；這個失敗沒有刪除。
09:38:19 原生 watchdog 在來源 operation 固定期限後退出，systemd 09:38:20 自動恢復，
本輪未發 restart。09:40 的
[獨立當前版本讀回](../artifacts/operations/tw_opening_latency_20261019/native_current_code_readback.json)
通過：Discord 新 run `1111107-1792373908858754501`、current loaded code 與 entrypoint SHA 相符、
Gateway／core／exact engine ACK 正常，五模式暖機已完成；觀察者沒有 provider／CUDA／下單操作。
當沖 engine 仍是 00:52:13 啟動的 MainPID 155／run `f646993656444b06bb06f46980a74f7f`。
[09:41 再次 qualified plan](../artifacts/operations/tw_opening_latency_20261019/native_current_qualified_plan.json)
為 `planned`；目前已採用最新碼，不做多餘人工 restart 或清掉已暖機模型。
這是原生 adoption 與工作恢復，不改寫今天原始 deadline，也不宣稱所有原始訊號／成交完成。

三個 canonical timer 仍 active：10:30 盤中 probe／10:31 audit、13:40 盤後 probe、次日每日官方名單更新；
開盤由原生流程被動記錄。永豐不額外登入或新增定期高頻 snapshot 輪詢。

目前限制包括外部供價／尚未首筆、MIS 無回應、Fugle 全市場權限，以及開盤前版本穩定與資料準備。
09:37 FinMind 此筆 headers 等待 1,037.813 ms、body 154.507 ms、JSON 6.442 ms、全量驗證 12.782 ms，
不可把 API 回應時間當成本機排程，也不可由這筆新程序／連線探測推定暖連線的固定下界。
**尚未完成一秒全量與全模式成交驗收，也尚不能說只剩 API 延遲。**
後續必須持續驗證已採用版本與實際交易日原生量測；來源未報價與未知不能填成真實開盤，
尚未發生的 10:30／13:40 也不能寫成已測。未改策略／帳本、未發券商單。

## 2026-10-19：盤後連線重用與完整欄位投影續修

本節更新前節的盤後現況，不改原生 11 秒失敗結果。
10:30 probe 的單檔 MIS 預熱成功（527.629 ms），但四個 80-channel 請求仍逾時，
後續 1,981 channels 未繼續派送；FinMind 有價 2,154，入口至完整來源工作與讀回為 8,133.718 ms。
13:40 單檔 MIS 即斷線，健康來源仍獨立得到 2,272 有價／29 未知，入口至報告前 1,094.076 ms。
單檔預熱不能證明全市場容量或後續每個請求健康。

### 共用修正與驗證

- 成功預熱的 MIS Session 不再立刻丟棄：response 關閉後，原 Session 只移交給一個 HTTP worker。
  其餘 worker 各用自己的 Session；不得跨執行緒同時使用，也不額外增加請求或提高併發。
  工廠／PID fence 防止跨測試或 fork 沿用錯連線；強制預熱失敗關閉未移交的舊 Session。
  每個請求記錄封閉列舉 `http_session_origin`，區分新建、已驗證移交、thread-local keep-alive；
  Session 移交本身不保證底層 TCP socket 永遠不會重連。
- canonical `_OpeningColumns` 仍逐列核對身分、TTL、世代與變更，但把已改動列的數值欄整批賦值，
  減少微小 NumPy 寫入；正開盤／缺價計數共用同一個向量遮罩。不改價格、量、缺值或時間。
- 新增雙 worker 排他移交、失敗清理、Session 工廠／PID fence，及完整欄位逐位元 scalar oracle 測試。
  含 NaN、signed zero、null、刪除／更新的來源值；所有 fsync／原始來源觀察仍保留。
- [最終相關回歸](../artifacts/operations/agent-workflow/runs/opening-1019-final-regression-20261019T101241-7975b518/run.log)：
  **688 passed／36.79 秒**，含取價、fan-in、來源恢復、官方 scope、Discord、暖機與測速；不是全庫測試。

[完整保留資料 A/B/B/A](../artifacts/operations/tw_opening_latency_20261019/evening_bulk_local_abba.json)
維持 2,301 軸、29 份原始 body、原市場／接收時鐘及持久化，3 輪 median **319.317 → 223.192 ms**；
全財務／時鐘與 durable rows SHA 相同。這是無網路的完整本機流程，不是行情外部延遲或成交 SLA。
[同 SHA A/A 控制](../artifacts/operations/tw_opening_latency_20261019/evening_same_code_control.json)
仍有 **238.282／207.306 ms** 差異，約 13% 環境噪聲；不把單組差異當固定生產加速倍率。

### 真實網路與資格邊界

| 實測 | 有正式開盤價／已證明無首筆／未知 | 入口至最後報告寫入前 |
| --- | --- | ---: |
| [17:50 baseline](../artifacts/operations/tw_opening_latency_20261019/evening_full_network_baseline.json) | 2,272／29／0 | 2,135.891 ms |
| [18:04 candidate](../artifacts/operations/tw_opening_latency_20261019/evening_full_network_candidate.json) | 2,272／0／29 | 1,156.951 ms |

第一輪 MIS 29 個請求全部成功；第二輪單檔即 `ConnectionResetError` 而未送全市場 sweep，
所以不能宣稱兩次是同等覆蓋的加速比較，或新移交路徑已通過真實全市場網路驗收。
18:11 額外單批唯讀 MIS 查核 30 channels，387.560 ms、HTTP 200，保留原始 body 與 SHA；
這僅核對 29 檔及 2330 對照，不冒充完整市場。當日資格核對結果為 16 不具當沖／13 具當沖，
完整清單與逐檔證據見[來源規劃](tw_opening_provider_plan_2026-10-13.md#1019-盤後續修與-29-檔的解釋)。
沒有憑零成交縮小官方清冊、改模型軸、回寫歷史成交或新增券商登入。

18:13 最初 Discord-only qualified plan 因服務控制 lease 被另一個 coordinator 持有而拒絕，
`restart_requested=false`；稍後 lease 釋放後新 plan 通過。後續換版另以當前收據驗收，
不由離線測試或磁碟程式已改宣稱 production adoption；交易引擎與帳本不在本輪重載範圍。
[18:14 合格換版](../artifacts/operations/tw_opening_latency_20261019/evening_discord_apply.json)
只重載 Discord，18:14:59 通過 `accepted_code_admission_only`；
[獨立讀回](../artifacts/operations/tw_opening_latency_20261019/evening_discord_independent_readback.json)
確認當前 code／entrypoint SHA、Gateway、core 與 exact engine ACK 相符。
Discord 新 run 為 `2089920-1792404892722410816`；engine MainPID 155／原 run 不變。
這只驗收程式採用與服務交接，盤後暖機仍 `pending`，不冒稱五模式 GPU 已暖機或明日開盤成功。
下一交易日仍須由原生開盤 audit 驗證價格、訊號及模擬成交，**未達一秒全量／全模式成交**。

## 2026-10-19：零成交輸入與今日帳本修復

零成交的前值政策限定模型輸入：同交易日官方無首筆證據可使用前一已完成交易日收盤值，
故開盤 gap 為零；未知、來源失敗及缺少前值不能冒充真實開盤或成交。
保留官方交易清冊與模型軸，不因零成交刪除標的。既有共用政策已包含這項處理，
本次以來源／覆蓋／首次真實成交覆寫前值的測試重驗，沒有另建替代取價管線。

### 根因與共用修正

- 今日其他四模式已有回放成交，保留原訊號、成交與會計；Factorized 則仍停在 10/16。
- 歷史開盤生成器原本先掛接整個最新日曆，誤要求不需要的當日盤後 Factorized 特徵。
  現在先依官方交易日找到必要的 t-1，再截斷基礎面板，最後進行原樣的完整掛接驗證。
  不降低特徵完整性門檻、不換 checkpoint、不使用今天收盤特徵產生早盤訊號。
- scoped 修復增加「追加缺少的已完成交易日」：前一日必須平倉且帳本一致，
  不得重開已成交的當日訊號；不足一張的目標不額外下載分鐘路徑。只使用既有本機分鐘資料。
- 首次正式追加曾因收盤處理器遍歷非目標帳戶而中斷。修正共用處理器只讀明確選取的模式，
  並加入跨模式測試。中斷的暫時成交／原生清倉不冒充原候選驗收成功。
- 新增有界的 interrupted recovery：隔離複製、核對操作前 journal prefixes、從同一來源重跑、
  比對原先已驗證的經濟結果，最後沿用 atomic directory exchange。整份中斷目錄保留為回復證據，
  沒有截斷或刪除 live 原始帳本；其他模式會計有變動即拒絕恢復。

### 已驗證的今日結果

[獨立本機／公網驗收](../artifacts/operations/tw_zero_volume_repair_20261019/ledger_final/acceptance.json)
於 18:49:06 通過，Factorized 的 1802、1815、3406 共 **48,000 股進場／48,000 股出場**，
持倉為零；09:01～13:30 **270 個唯一分鐘點**。零成交分鐘延用既有最後成交估值，
28 個 carried 分鐘皆有明確零量／零金額來源，不捏造新成交或新鮮度。
原始五份 journal 的操作前 SHA prefixes、來源 pins、候選／正式經濟結果、其他四帳戶未變，
均由獨立讀回確認；公開 API 也核對逐筆價格、數量、回放標記與完整分鐘曲線。

[操作收據](../artifacts/operations/tw_zero_volume_repair_20261019/ledger_final/ledger_repair_manifest.json)
的 `atomic_recovery.rollback_directory` 保留整份中斷版本，
`interrupted_manifest.json` 保留第一次失敗原因。所有補登明確為反事實模擬，不是當時即時訊號或券商成交。
[最終相關回歸](../artifacts/operations/agent-workflow/runs/ledger-1019-final-recovery-regre-20261019T104933-46006235/run.log)
為 **322 passed／4 skipped／24.58 秒**（四個未完成盤中的 missing-session 變體不適用），不是全庫測試。

[18:51 Discord 合格換版](../artifacts/operations/tw_zero_volume_repair_20261019/discord_apply.json)
已通過 `accepted_code_admission_only`，Gateway、core 與 exact engine ACK 正常；
盤後 GPU 暖機仍 pending，不能由程式採用宣稱模型已 resident ready。

原生 09:00 延遲失敗稽核沒有改寫。另有盤後特徵維護因磁碟 reserve admission 被拒絕，
不以今日帳本恢復宣稱該資料工作或下一交易日一秒 SLO 已完成；沒有擅自刪除來源騰空間。

## 2026-10-20：完整報價 window 與盤中重試去除本機等待

本輪開始於 10/20 凌晨，尚未到當日開盤。故障診斷使用 **10/19 真實盤中**收據，
新版本的完整流程比較使用明確標示的 **10/16 原始回覆離線重放**；兩者不可混成今日 09:00 成績。
沒有改動正式帳本、模型權重、訊號內容、官方每日分母、輸入門檻或成交規則。

### 來源／責任盤點

| 入口 | 既有執行方式 | 10/19 證據與本輪邊界 |
| --- | --- | --- |
| 永豐原生行情 | 同一 execution owner 的串流與 ≤500 contracts 非同步 snapshots，經 IPC 共用 | 11 秒時已接受 591 個正式開盤；其餘回覆不能因 API 成功就視為正式開盤。本輪不新增登入、不中斷原生引擎。 |
| 官方 TWSE／TPEx MIS | 同一 HTTP pool／keep-alive、80 channels／批、健康時 4 個並行，失敗後降載／熔斷 | 開盤先送出的四個請求皆 ReadTimeout、無 HTTP 回覆；不是 29 個皆已送出。17:50 才有當次完整 29／29 sweep 證據，不能倒填開盤。 |
| FinMind 全市場 snapshot | 單一來源 owner／連線／完整解碼 view，按來源時鐘重試；各模式只取自己的 delta | 11 秒時 211 個正式開盤，與永豐重疊；不是額外 211 檔。其約十秒更新說明不是 11 秒全成交保證。見[官方文件](https://finmind.github.io/tutor/TaiwanMarket/RealTime/)。 |
| Fugle TSE／OTC | 已設定的兩個全市場路由，獨立工作與權限退避 | 10/19 來源紀錄為 HTTP 403；不能將有 key／有 adapter 說成有全市場權限，也不能靠提高併發解除授權。 |
| 其他券商／付費全市場 feed | 未有本輪可用連線與完整權限驗收 | 不建立假來源，不以昨日收盤或盤後檔案冒充即時首筆。 |

來源間維持平行與 single-flight；來源內只把必要的 admission／解析／驗證／合併／持久化依賴序列化。
「完整」分別記錄正式開盤、官方已證實尚無首筆、未知三類。官方零量只允許前一已完成交易日收盤
作模型零 gap 輸入，不是即時成交價，也不從官方交易清冊排除該標的。

### 已重現並修正的兩個本機缺陷

1. **MIS 網路 window 被同步 progress 阻擋。** 舊版雖先補送下一批，再做驗證／日誌，
   但下一批若在 callback 執行期間結束，後續 HTTP 槽位仍會空轉。現在以一個輕量 pump
   持續補給相同 bounded window；呼叫端只驗證／合併已到達的 cohorts，不等慢來源。
   併發、端點失敗預算與熔斷不放寬。handoff 只保留有限請求範圍內已有 rows 的引用；
   callback／fsync 失敗會取消尚未派發的工作，drain 已准入請求並 join，不留下背景工作冒充成功。
   新診斷欄位為 `mis_network_schedule_policy=bounded_window_independent_of_progress_v2`。
2. **09:01 後 observer 又加了 30 秒等待。** 10/19 的 `opening_source_repair_latency.jsonl`
   在 09:01:00.486 已記錄來源自身的 1／10／15／60 秒 deadline，但外層仍另設 30 秒 delay，
   09:01:32.042 才進下一次觀測。現在有進展或來源正在執行時保留短觀測，否則取各來源
   最早的有效 monotonic deadline；無期限資料時才沿用原有有界退避。
   落盤後重新計算期限，避免把 fsync 時間再疊上來源期限。既有每秒 loop、真正推論讓位、
   quota／403／429／端點 backoff 全部保留。新欄位為 `observer_retry_policy=earliest_shared_provider_deadline_v2`。

### 完整範圍測速與故障注入

[串行 ABBA 原始收據](../artifacts/operations/tw_opening_latency_20261020/window_serial_abba.json)
對照 2,301 檔軸、全部 29 份原始回覆，各版五次；每次都包含 canonical fan-in、驗證、journal、fsync
與獨立落盤讀回。原始正式開盤數為 **2,274**，未將全部 rows 冒充正式價格。
所有財務欄位、原始交易所／本機接收時鐘與持久化 rows 的 SHA 完全一致：

| 比較 | 舊版中位數 | 新版中位數 | 邊界 |
| --- | --- | --- | --- |
| 無額外人工延遲的本機完整流程 | 156.518 ms | 136.779 ms | 無網路的完整原始回覆重放，非開盤／成交 SLO |
| 每 HTTP 人工加 5 ms、每 progress 人工加 20 ms | 381.842 ms | 288.229 ms | 明確故障注入，不能稱為實際 API／磁碟速度 |

[同版本 AA 控制組](../artifacts/operations/tw_opening_latency_20261020/window_serial_aa.json)
仍有 166.228／152.308 ms（約 8.4%）的中位數差異；主機負載／暖化會影響結果，
因此不將 ABBA 的約 12.6% 改善宣稱為固定保證。
[故障注入收據](../artifacts/operations/tw_opening_latency_20261020/window_serial_stall.json)
亦通過全欄位／時鐘／落盤 SHA 對照。progress 數量減少只合併**已完成且不衝突**的 rows，
沒有刪掉欄位、標的、驗證或最終 durable 門檻。
[局部 profile](../artifacts/operations/tw_opening_latency_20261020/window_stages.json)
另外保留巢狀、跨 thread 等待的局部診斷，profiling overhead 已計入，不能相加或取代未開 profile 的數字。

新增決定性測試：第一個 callback 被阻擋時第三個 HTTP 仍須開始；快批不等慢批、衝突 venue
不合併覆寫；callback 失敗必須 drain／join；最早健康來源期限不能被失敗來源拖慢；
fsync 後不得重加舊 delay；人工延遲不能混入正式 network／scheduled probe。
[相關回歸](../artifacts/operations/agent-workflow/runs/opening-1020-all-source-regressi-20261019T163929-62454ba7/run.log)
為 **812 passed／27.34 秒**，涵蓋報價、來源 fan-in、原生 broker、持久化、官方名冊、
Discord、原生延遲稽核與合格換版；不是全 repository 測試。
修改前的停頓失敗、初次缺少 math import 及錯誤測試路徑的 run 均保留，不覆寫為成功。

### 上線／仍未驗證的邊界

[00:43 合格換版](../artifacts/operations/tw_opening_latency_20261020/discord_apply.json)
通過 `accepted_code_admission_only`：只替換 Discord owner，Gateway／core／零落差 engine ACK
與原始 journal prefixes 都通過；execution owner／其他受保護服務不變，GPU 暖機仍 pending。
00:50 起另一工作又修改真正相依的 `stockagent/config.py`、`training/checkpoint_contract.py`、
`training/multiobjective.py`；再次讀回確認 model code identity 已失配。
沒有更新舊程序的 admission 或排除真正相依來掩蓋。檔案穩定後，再以完整 consumed-code
identity 及本輪 entrypoint／quote／benchmark SHA 包住測試，確認測試前後皆相同：
[最後穩定版本回歸](../artifacts/operations/agent-workflow/runs/opening-1020-stable-final-20261019T165422-fb3fb059/run.log)
為 **886 passed／36.59 秒**，另涵蓋 completed inference cache 與 live admission。
[00:56 最後合格採用](../artifacts/operations/tw_opening_latency_20261020/discord_final_apply.json)
驗收的 `expected_code_sha256` 與該測試同為 `f25bdead376f138d9ba89534c763ba3cde23831d6387740bed823a76ec776850`；
新程序 Gateway／ACK／journal prefixes 通過，engine 未重啟，GPU 暖機仍未驗證。
這是同版本工程驗收，不允許之後的實際相依變更自動繞過 fresh-process 要求。
[01:00 獨立交付讀回](../artifacts/operations/agent-workflow/runs/opening-1020-handoff-verificatio-20261019T165959-02498c2d/run.log)
再次確認目前 consumed code、測試版本、已採用版本與 entrypoint 一致，Gateway／core 正常，
同一未重啟的 execution owner ACK 落差為 0，相關本機證據連結皆存在。

原生每日 08:55、09:00:11／31、09:01 稽核 timer，與 10:30／13:40 全市場來源 probe timer
已確認啟用；開盤仍保留首份不可變 deadline 結果，來源取完、模型發布、紙上全成交分開驗收。
磁碟一度只剩約 4.8 GiB，後續其他工作整理期間分別觀測約 15／29 GiB，
仍不能據此宣稱盤後 reserve gate 已通過。
本輪沒有刪資料、沒有補寫／修改帳本，沒有聲稱今日開盤已測、外部錯誤永遠為零、
毫秒／一秒全市場全成交已達標，或只剩 API 延遲。

### 同範圍重測

以下只重放保留的原始回覆，禁用真實網路，不連券商；每版五次均保留完整軸與 durable 證據。
不要與其他壓力測試同時執行。原保留清冊驗證器不可省略或換成寬鬆的現在版本。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/operations/tw_opening_latency_20261016/quote_phase_full_market_network.round1.receipt.json \
  --compare-mis-pipeline artifacts/operations/tw_opening_latency_20261020/quote_before.py \
  --retained-universe-owner artifacts/operations/tw_opening_latency_20261018/universe_membership_repair/reader_before.py \
  --mis-responses artifacts/operations/tw_opening_latency_20261016/mis_efficiency_retained_responses.json \
  --rounds 5 --output artifacts/operations/tw_opening_latency_20261020/window_retest.json
```

AA 控制組將 `--compare-mis-pipeline` 換成目前 `stockagent/live/quote_provider.py`，
另選 output 路徑；人工 stall 對照另加 `--mis-simulated-http-delay-ms 5 --mis-simulated-progress-delay-ms 20`，
不可把此參數帶入實際 network 或 scheduled probe。

## 2026-10-20 續修：數值重用、整欄複製與嚴格收據編碼

本輪重新保存 `quote_round2_before.py`／`acquisition_round2_before.py`，比較起點是上一節
已採用的 window／observer 修正，不把上一輪加速重複計算。仍是凌晨工程驗收，最近實際
開盤證據是 [10/19 的首份 11 秒收據](../artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-19/first_assessment_11000ms.json)：
591 個正式開盤、0 個當時已證實無首筆、1,710 個未知；FinMind 的 211 個與永豐重疊。
來源清冊與四個首輪 MIS ReadTimeout／Fugle 403 的診斷沿用上一節，沒有把它們改寫成成功。

### 關鍵路徑與修正

每個官方可交易標的可由最早的合格來源解決；整個範圍的等待受最後未解決標的支配。
不同 provider 的耗時不能直接相加，解析、驗證與網路也能重疊。
正式開盤、官方新鮮零成交證據、未知仍分開；零成交只准模型沿用前一已完成交易日收盤／零 gap，
不是已成交報價、排除官方清冊或修改帳本。沒有首筆成交的標的也不能保證在 09:00 已存在正式開盤價。

本輪修改仍在既有 quote／fan-in／atomic writer 內，未另建來源或成交路徑：

1. 已經由 seed／load 正規化的 copy-on-write row 以私有型別標記，落盤不再重做每列十一欄的
   浮點／數量轉換。舊版未標記 mapping 仍走原始 defensive parser；新來源、快取讀回、freshness、
   欄位 shape 與跨日檢查不省略。正式開盤集合也只計算一次，不再為 count／missing 各掃一次。
2. MIS 不重複目的 index 的批次改為 NumPy 整欄複製；重複／衝突 venue 留原始有序逐列路徑。
   缺欄不覆寫、漲跌停／數量不變，整數時鐘不經 float64；測試另外覆蓋大於 `2**53` 的時鐘與 signed zero。
3. 後續 barrier／增量寫入可借用**實際檔案 bytes 完全相同**的收據 metadata，只保留最新一份。
   不用它供應價格或取代完整 restore validation。scope cache key 加上 ctime／inode／device，
   修正「同長度、保留 mtime 的原子替換」可能誤用舊 fingerprint 的問題；目錄 fsync 失敗仍須重寫。
4. 僅此私有收據可使用既有可選 msgspec 編碼器：價格欄位必須全部來自已驗證 row，header 與
   任意舊 conflict metadata 另外檢查 JSON 型別與非有限值。套件不存在、特殊型別／大整數／alias
   都回原 strict stdlib JSON；bytes、datetime、循環與非法 surrogate 不會被悄悄改值。
   [msgspec 官方文件](https://msgspec.dev/supported-types#float)說明 NaN 預設會編成 null，故不能直接全面取代 JSON。
   本輪沒有改 `stockagent.private_json.dumps` 或公網 wire encoder。file fsync＋rename＋directory fsync 依舊。
5. fan-in 每次 wake 重複建立的 symbol set 改為一次建立後用 isdisjoint 查詢，provider 排程與等待語意不變。

### 完整流程對照與持續測速

[最終 ABBA 收據](../artifacts/operations/tw_opening_latency_20261020/round2_native_abba.json)
每版五次、2,301 檔原始軸、29 份 10/16 原始 HTTP body，包含解析、驗證、fan-in、來源 journal、
durable barrier 與落盤 JSON 解碼。實際為 2,300 個 response rows、2,274 個正式開盤，
沒有把回覆筆數／來源工作結束當全市場價格完整。
所有數值、原始來源／接收時鐘、mask 與 durable rows SHA 均完全一致；真實網路、訊號發布、
券商下單與正式帳本寫入皆為零。

| 範圍／中位數 | 本輪修改前 | 本輪修改後 | 限制 |
| --- | --- | --- | --- |
| 完整本機工作流 | 203.382 ms | 173.129 ms | 約少 14.9%；不是實際開盤／模型／成交時間 |
| 收據投影、編碼與 durable write | 44.906 ms | 19.981 ms | 巢狀 span，不可再加到完整流程上 |
| 人工 HTTP 5 ms／progress 20 ms 的完整流程 | 276.997 ms | 228.285 ms | 明確 fault injection，非實際來源／磁碟成績 |

[同版本 AA](../artifacts/operations/tw_opening_latency_20261020/round2_native_aa.json)
為 167.774／165.593 ms（約 1.3%），兩個 label 的 quote 與 acquisition SHA 相同；仍不承諾固定倍率。
[故障注入收據](../artifacts/operations/tw_opening_latency_20261020/round2_native_stall.json)保留全欄位／時鐘／落盤 parity。
探索期間的 `round2_stage_baseline.json`、`round2_normalized_abba.json`、`round2_final_abba.json`、
`round2_accepted_abba.json` 均保留；中間版本整鏈改善僅約 1–7%，未選那些波動當數倍加速。
通用逐值檢查再 native encode 的單段試驗收益小，最後採來源 schema 已驗證數值的窄路徑，
不是移除 NaN／精度檢查或對全系統換 serializer。

正式 source journal 的 quorum／finished 現在增加 `input_flush.receipt_stages`（熱 barrier 為空）：
row_projection、previous_receipt_read、official_scope_and_archive、payload_assembly、json_encode、
atomic_write_fsync、metadata_publish 的 ms，以及 encoder、encoded_bytes、重用／防守轉換 row count。
舊的 captured revision、writer_queue、newer_input_pending 保留。benchmark 另有涵蓋所有呼叫 thread
的 `local_stage_spans`、所有 `input_durable_flushes` 與 MIS 組裝／callback spans；巢狀／並行數字不可相加。
這些測速不快取上一次 duration，不把慢／失敗的樣本排除。

### 同範圍重測與剩餘邊界

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/operations/tw_opening_latency_20261016/quote_phase_full_market_network.round1.receipt.json \
  --compare-mis-pipeline artifacts/operations/tw_opening_latency_20261020/quote_round2_before.py \
  --baseline-acquisition-owner artifacts/operations/tw_opening_latency_20261020/acquisition_round2_before.py \
  --retained-universe-owner artifacts/operations/tw_opening_latency_20261018/universe_membership_repair/reader_before.py \
  --mis-responses artifacts/operations/tw_opening_latency_20261016/mis_efficiency_retained_responses.json \
  --rounds 5 --output artifacts/operations/tw_opening_latency_20261020/round2_retest.json
```

AA 將 quote／acquisition baseline 同時換成目前 source；stress 另加上一節的人工 delay 參數。
保持串行測試，不與另一測速工作同時跑。工程正確性回歸與合格換版證據見後續讀回；
09:00／11 秒／全模式原始發布與全目標模擬成交，仍由既有每日原生 audit 獨立驗收。
10/20 的 08:55 audit 與 10:30／13:40 全市場 probe timer 已確認排程，沒有用離線結果取代它們。
外部端點不回覆、Fugle 授權與尚無首筆仍是實際來源邊界；本機也仍有必要的驗證／合併／journal 工作，
尚未證明數學極限或只剩 API 延遲，更不保證外部零錯誤／一秒內所有成交。

### 同版本回歸與部署讀回

[最終穩定版本回歸](../artifacts/operations/agent-workflow/runs/opening-1020-round2-stable-20261019T173234-79653034/run.log)
涵蓋 quote、fan-in、benchmark、來源恢復／串流、官方清冊、執行測速、Discord admission／warmup
與 strict JSON 共 22 個模組，**914 passed in 33.90s**。前後 consumed implementation 與四個
直接修改／入口檔案的 SHA 都一致；這不是全庫測試完成。consumed code 為
`4492daee099c75f5b64b3513259454f4f5c30ea0c4b170c95fc5ac42ff69ea1a`，quote 為
`802a2c1c55dc16ad943f1b9337e602076b0e70e70299e7e89f04471d2482609c`，
acquisition 為 `4d3186a508fbbf3e2f1db953b88c6126b74e4ee482e9ad2dbd328a16df804c48`。

換版只走既有 qualified Discord-only code admission，不重啟當沖引擎：即時 plan 的部位為零，
但仍有一筆 pending order，沒有刪除或偽裝為 terminal。首次 plan 遇到協調鎖，
[重讀 plan](../artifacts/operations/tw_opening_latency_20261020/round2_discord_plan_retry.json)才通過；
[首次 apply](../artifacts/operations/tw_opening_latency_20261020/round2_discord_apply.json)
又因 heartbeat 超過 10 秒時效拒絕，`restart_requested=false`。保留原失敗證據，沒有放寬 TTL、
改心跳時間或移除鎖；只在原生心跳繼續更新、Gateway／ACK 正常後重新完整 admission。

[重新 admission／apply](../artifacts/operations/tw_opening_latency_20261020/round2_discord_apply_retry.json)
於 **10/20 01:43:14 +08:00** 通過 `accepted_code_admission_only`；這次才有一個新的
Discord invocation，載入的 consumed code 與 914 項測試相同。Gateway／命令同步正常，
五模式 exact ACK 的 revision lag 為零，沒有當次 watchdog／fatal；六個 protected owner
保持相同 InvocationID／PID，當沖引擎仍為 PID 2157718，未重啟。
events／orders／fills 共三個原始前綴 SHA 重新驗證不變，未送券商單、未操作帳本。

[獨立讀回](../artifacts/operations/tw_opening_latency_20261020/round2_independent_readback.json)
於 **01:45:24** 再驗證同一組測試／source SHA、新 Discord owner、原引擎、五模式 ACK、
完整帳本前綴與當次 journal。兩個既有 timer 仍 enabled，沒有新增 scheduler 或報價登入。
暖機是當前 run 的 `pending`，由原生盤前窗口繼續；這份驗收明確為 loaded-code／Gateway／ACK，
不代表 GPU resident ready、價格齊備或 10/20 開盤 SLO 已通過。

## LoadedImplementationChanged 根因與目錄級版本隔離（2026-10-20）

### 原始事故與責任邊界

10/20 09:00 五個模式在正式取價之前，均因 `LoadedImplementationChanged` 被拒絕。
這與 10/19 的 MIS timeout 是不同事件。服務早先載入的 212 個 consumed Python files 中，
`stockagent/config.py`、checkpoint／optimizer／multiobjective 與兩個 data-sync 共用相依在
盤前被其他工作修改；正式 owner 仍直接使用可變的開發工作樹，磁碟版本因而不再代表其
常駐模型與已載入的 Python 物件。版本檢查是正確的混版保護，不能刪除或把舊快取重新命名。

[09:04 的 Discord-only 原生恢復](../artifacts/operations/tw_opening_latency_20261020/live_incident_discord_apply.json)
已接受新的 code／Gateway／exact ACK，沒有重啟當沖引擎或操作帳本。
之後四個模式恢復原始訊號；Factorized 仍有獨立的 `preopen_catch_up` 工作。
09:24 的原生 source watchdog 因其 1,200 秒準備期限用盡而自行重啟；不是本輪人工重啟，
也不是修復 LoadedImplementationChanged 就能宣稱該資料準備問題消失。

當天[首次 11 秒評估](../artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-20/first_assessment_11000ms.json)
保留原始失敗，沒有用 09:04 以後的恢復或離線測試補寫成功。

### 原理與實作

運行中的程式必須是一個穩定版本，而開發工作樹必須可繼續修改；兩者不能再共用可變的 code inode。
新增共用 `stockagent/live/service_code_release.py` 與只做準備的
`scripts/freeze_discord_code_release.py`，沿用既有 source-release manifest／SHA verifier、
qualified reload、systemd 和同一個行情／推論 owner，不建立第二條交易路徑。

- 每個 release 複製 Git 可見的 `stockagent`、`downloader`、`scripts` 與 Discord Python source，
  不使用 hard link。捕捉前後的完整成員與 SHA 必須一致；程式、收據、復原 zip 均唯讀，
  未記錄成員、symlink、hard link、權限／內容漂移或 archive checksum 不符均拒絕。
- systemd 在服務自己的 namespace 中，把四個**程式目錄**唯讀綁定至原來的絕對路徑。
  子程序、lazy import 與既有資料路徑保持一致。原始 markets／models／staged_accounts 目錄
  維持 live、可寫；`.env` 只綁定原檔，沒有把任何憑證內容複製進快照或 zip。
  其他資料、configs、模型、帳本與 artifacts 不搬動。
- 啟動腳本在每次 pinned owner 啟動前驗證 manifest、原生 cgroup、所有 code inode／SHA
  與八個掛載的 ro／rw 權限，之後才啟動 bot。壞版本不能趁重開機重新 admission。
- `LoadedImplementationChanged` 的實作完全保留；實際 pinned code 變動仍須新 owner。
  普通 Discord reload 和 engine-only reload 的 Discord code 比較會沿用已設定的 release，
  不會因另一個研究工作修改開發檔案而偷偷升版或拒絕未變動的 Discord owner。
- qualified cutover 仍要求原生 ACK、無 active input／互動工作、受保護 owner 不變、
  帳本前綴不變；engine 的盤中／非空倉限制不放寬。單位設定先驗證，daemon-reload 後再
  讀取 systemd D-Bus 的實際解析結果，要求八個 source／target／ro-rw 與已驗證版本相符。
  解析結果不符或 reload 失敗只還原自己的 drop-in，尚不要求 restart；不修改其他服務設定。
- 兩個正式 Discord 暖機／GPU idle 驗證 CLI 也在 active release 的 namespace 執行，
  避免把開發工作樹與正式版本比較而誤報。環境變數本身不是版本證明，仍須驗證實際掛載。

[systemd 官方掛載語意](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html#BindPaths=)
是設計依據；本機以真實 mount namespace／systemd 試驗確認，不只檢查設定字串。

### 故障注入與驗收方式

初版單檔 `bot.py` bind 在新程序的短測通過，但**同一個持續運行程序**測試發現：外部原子替換
入口檔可繞過舊的 mount target dentry。因此未部署該版本，改為整個 Discord code 目錄唯讀，
再把原始 live inputs 接回；原始失敗收據保留。

`test/test_service_code_release.py` 以真實 kernel namespace，測試同一程序跨越工作樹原地修改、
入口原子替換、設定原子替換；程式 SHA 不變、寫程式得到 `EROFS`，設定更新與原路徑寫入均可見。
另涵蓋空的憑證 mount placeholder、快照損壞／新舊成員、unsafe path、未知 drop-in、
更新 release、daemon-reload／解析錯誤 rollback、未知 schema／缺掛載／shadow mount 拒絕，
以及啟動驗證失敗不啟動 bot。
既有 qualified reload 測試同步覆蓋 mutable／pinned 版本、engine-only 不改 Discord drop-in、
開發程式變動不影響 pin，而 pin 自身變動仍立即失敗。

[最終準備收據](../artifacts/operations/tw_opening_latency_20261020/frozen_systemd_final_preparation.json)
本身僅代表 `prepared_not_adopted`。在**該 release 的 namespace**執行的
[同版本回歸](../artifacts/operations/tw_opening_latency_20261020/frozen_systemd_final_regression.json)
為 **992 passed in 40.92s**，consumed identity 與測試檔前後一致。早期測試檔尚在修正的
`implementation_stable=false` 收據保留，不能當同版本驗收。

### 正式採用、部署失誤與獨立讀回

09:43:25 僅設定下次原生啟動使用的 release，七個服務 owner 當時不變，未強制中斷
Factorized 的 input work。09:45 再次碰到原生 1,200 秒 source watchdog 後啟動失敗：
本輪 drop-in 錯把整個 `source:target` 加引號，systemd 雖通過語法驗證，實際卻將冒號當成
來源路徑的一部分。這是本輪引入的 Discord 啟動故障，不能歸咎於行情或隱藏為正常。
修正為受限字元集的無引號絕對路徑並 daemon-reload 後，既有原生重試於 **09:46:18**
恢復。為避免重演，部署工具新增上述 D-Bus 實際解析驗證與回滾，測試也覆蓋此引號錯誤。
測試 probe 改用與正式服務相同的 systemd 掛載實作，不再以另一套自製 mount 流程替代。

[原生採用驗收](../artifacts/operations/tw_opening_latency_20261020/frozen_native_adoption.json)
於 **09:47:45 +08:00** 確認正式 Python PID 4005242、InvocationID
`6462ddb2af60473dac1e3ffeae1cd539`，完整 **1,557** 個 code inode／SHA 與八個掛載相符；
loaded inference code 為 `6545bfefbd49d885c7e4c75eca97b580df4205ac8f12b3d53047b2f3e5e9cb8f`。
Gateway 已連線、原生五模式暖機狀態為 ready、exact engine ACK revision lag 為零。
這不另行宣稱 GPU 數值驗證通過。六個 protected owner 未變，當沖引擎仍為 PID 2157718、
run `c18cd72d1728431186e5203c28cfca49`，三個帳本原始前綴 SHA 不變，未送券商單或操作帳本。

[09:53:39 再次獨立讀回](../artifacts/operations/tw_opening_latency_20261020/frozen_native_repeat_readback.json)
確認同一個原生版本與 owner、八個實際解析綁定、完整 code inode／SHA、exact ACK／lag 0、
Gateway 與六個 protected owner，該 invocation 的 `LoadedImplementationChanged` 為 **0**。
Factorized 仍在 `preopen_catch_up`，沒有把這個獨立資料問題寫成已修復。

[最終部署工具與相關語意回歸](../artifacts/operations/tw_opening_latency_20261020/final_source_pin_deployment_regression.json)
及[原始測試日誌](../artifacts/operations/agent-workflow/runs/opening-1020-final-pin-complete--20261020T015304-78a4dcad/run.log)
為 **1,032 passed in 43.41s**，修改程式與測試 SHA 前後一致，包含新增解析防線與驗證 CLI。
這是目前 operator tools 的驗收，不代表把後續開發檔案自動升版到正在運行的 immutable owner，
也不是全 repository 測試完成。正式改版仍需下一次合格 plan／apply／verify。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/freeze_discord_code_release.py \
  --output artifacts/operations/discord_code_preparation_NEW.json
run_fintech_python scripts/reload_tw_day_trade_services_if_flat.py \
  --discord-only --code-admission-only \
  --source-release artifacts/operations/service_code_releases/RELEASE/release.json \
  --output artifacts/operations/discord_code_plan_NEW.json
# 同版本回歸與安全 plan 通過後，才另以 --apply 建立新的 apply 收據。
# 回滾亦使用同一命令指定先前已驗證的 release，不刪快照、不解除混版保護。
```

## Factorized 盤前準備逾時與最新交易日預設（2026-10-20）

### 不是缺少快取，而是把全歷史 producer 放進盤前 consumer

[當次來源診斷](../artifacts/operations/tw_opening_latency_20261020/factorized_source_diagnosis.json)
確認固定原始 parent、模型設定與計算程式未變；350 個 FinLab receipt heads 的部分內容及
public feature SHA 已更新。舊 `--phase preopen` 與 `completed` 都呼叫 `prepare()`，
因此即使已發布版本完整涵蓋昨日收盤，也會因 raw head 變動重跑 2014 年起的 causal sweep。
既有 payload reuse 省下壓縮與重複寫檔，**沒有省下整段歷史的矩陣計算**，不能稱為真正增量。
現場 Python stack 停在 `causal_state_blocks` 的事件對應與逐日 sweep，不是正在等待 MIS。

10/20 01:51:55 已有完整、immutable 的發布版本：3,119 個交易日、14,726 個模型通道、
2,906 個成員，完成至 10/19、decision 為 10/20；原始 builder 耗時 **2,069.843 秒**，
updater 全程 **2,075.037 秒**。這超過 Gateway preopen 的 1,200 秒期限與一般 completed
command 的 1,800 秒期限，因此每次原生 source watchdog 都可能讓重算重新開始。
提高 Gateway timeout 或略過資料驗證都不消除這個 producer/consumer 責任錯置。

### 已實作的 consumer 修正與驗證範圍

- `scripts/update_tw_day_trade_factorized_live.py`：`preopen` 只啟動既有官方 opening
  activation，再完整驗收發布版本；只有 `completed` 仍交給 canonical `prepare()` 建置。
  不刪歷史、不縮模型輸入、不啟動新行情或交易 owner。
- `stockagent/data/factorized_panel.py`：抽出共同 `verify_factorized_layout`，讓正式模型讀取
  與盤前 reader 共用完整 calendar、股票 universe、channel ABI、date/symbol block 分區檢查；
  保留每個成員和 dictionary 的 SHA 檢查，不以 metadata-only 冒充完整驗收。
- 未建好本交易日／完整 horizon 時，寫出 `projection_refresh_required`、
  `model_executed=false`，交給既有 isolated artifact-maintenance owner；不在 Gateway
  再重跑四次相同全歷史工作。背景原有 accepted-close／休市／kernel lease 與時間預算不放寬。
  修正「已驗收收盤但 current projection 缺檔」先前未要求 source recovery 的遺漏。
- receipt 明確保留原本 `source_identity`、`source_built_at_utc` 與 manifest SHA，並記錄
  `input_policy=published_complete_session_projection_v1`、`latest_raw_heads_claimed=false`。
  這表示消費已發布版本，不聲稱盤前新取得的所有 raw revision 都已加入；新修訂由背景發布。
  最新 eligibility、正式交易清冊與 09:00 報價仍走獨立原生驗收，沒有改成昨日價格或假成交。

[同一候選 immutable namespace 的完整驗證測速](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/frozen_consumer_benchmark.json)
三次為 **8.430／5.804／6.412 秒**，每次皆驗證全部 3,119 日、14,726 通道與 2,906 成員。
未控制 OS page cache，不能標榜為物理冷機；這是**完整已發布投影的消費驗收**，不是完整 raw
source 重建加速到六秒，也不是交易日 09:00 至訊號／模擬成交的一秒或十一秒實測。
分階段 metadata/calendar 與 member/dictionary SHA 耗時均保留於上述收據。

回歸遇到本機 Matplotlib 3.9.1 使用 Pyparsing 3.3.3 舊 alias 的第三方 deprecation：
正式 regression 的 warnings-as-errors 先如實失敗，沒有改金融邏輯、全域 warning policy 或
共用套件環境。後續工程回歸只對該第三方 `PyparsingDeprecationWarning` 作 run-scoped
相容處理，其他警告仍為錯誤；完整 pin 驗證另發現 direct shebang 測試誤用 systemd 的系統
Python，測試改綁本次 canonical Python 的 PATH，與正式 bot 的 source-command resolver 一致。
失敗收據保留，不用較早的測試成功覆蓋。

### 預設最新交易日，歷史仍可選取

當沖與隔日沖共用 `app.js` 現預設 `followLatestSession=true`、
`followPerformanceRange=false`，首頁的 status、持倉／訊號與分鐘曲線只請求目前 session 的範圍。
保留「2026 年起績效」按鈕、自選日曆、完整歷史與原始累積資產，不修改帳本或裁切資料；
手選歷史後仍固定該區間，不被即時 refresh 強制拉回最新日。
同步更新 immutable JS URL 的內容 hash，避免舊瀏覽器資產快取留住原先預設。

[原生瀏覽器驗證](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/latest_session_browser.json)
在兩個正式 localhost 頁面、1366×768 筆電及 390×844 手機四種組合皆確認起訖等於各自
server session、分鐘 history 的 request／回應同日、無 page error、無水平溢出。
當沖為 10/20，隔日沖當時仍為 10/19 的持有 session，沒有套用客戶端今日日期捏造新資料。
18 項 Node 日期／rollover／非同步資料保留測試通過；瀏覽器收據保留各 API 的實際 response
duration（含首次慢與後續快的樣本），不是只挑最快熱快取。

### 真正增量仍是獨立的後端未完成工作

使用者提出的目標正確：常態新交易日只算新增／被修訂的區段。此輪 consumer 修正消除了
盤前不必要重建，**還沒有把 canonical producer 變成真正增量**。不能把檔案 hard-link reuse
或只讀 current 偷換成「最新一天已增量建置」。正確後續邊界為：

1. 保存可驗證的 normalized observations 分區／來源版本與完整 calendar、lifecycle 身分。
2. 保存 exact causal state（Float64 原值、observed clock、NULL barrier、report-period floors、
   TTL／issuer lifecycle），不能從 Float32 展示投影逆推原始狀態。
3. 新來源只影響新日且歷史 prefix 證明不變時，重用 immutable prefix，只算新日；若有歷史
   revision、report reset、calendar／ABI 變更，從最早受影響 boundary 重算，不能硬稱 append-only。
4. 先以完整重建作 equality oracle，驗證所有特徵、缺值與正式模型輸入；冷 bootstrap 和當日
   update 分別測速，記錄 reused／recomputed sessions、失效原因與完整端到端耗時。

此外，01:52 的 isolated artifact-maintenance 原生失敗仍為它本身的
`LoadedImplementationChanged`；Gateway 的 immutable pin 不等於該不同 owner 已採用相同
版本。07:40 的 minute-curve worker 也有獨立 rebuild failure。兩者不可因本次 UI 成功或
Factorized consumer 驗證而改報健康，需各自完成版本／原始結果驗收。

### 本次正式採用與當日自然恢復

[候選同版本回歸](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/frozen_verified_regression.json)
及[完整日誌](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/frozen_verified_regression.log)
為 **1,192 passed in 101.99s**，source identity 與全部測試檔 SHA 前後一致，包含上述有限的
第三方 warning 相容條件。不是全庫測試宣告，也沒有把測試程序當正式 owner。

[10:27:00 的安全 staging](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/source_stage.json)
僅更新下次原生啟動的 code pin；受控制鎖保護，先驗 candidate 全 manifest，核對當前
原生 namespace／Gateway／ACK、七個 owner 與三個帳本前綴，再核對 systemd 實際解析的
八個綁定。此時 owner 均未變，`restart_requested=false`，並未在 input work 執行中強制 reload。
舊 owner 隨後自行遇到原有 1,200.139 秒 source watchdog，systemd 的正常恢復於
**10:27:53** 載入新 release；不是人工重啟交易引擎或中止 source 命令。

[10:31:08 的獨立原生採用驗收](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/native_adoption.json)
確認 Discord MainPID 4088562／Python PID 4088630、InvocationID
`09c67597384d42ada5a0ae71bcc4e33f`，已採用完整 1,557 個 frozen source files 與八個掛載；
loaded inference code 為 `3bf7a203c9bcd51327e9578c4a4748098d6b1e06bca786c1ab73df67170deb11`，
與本次候選 regression 相同。當次 invocation 的 `LoadedImplementationChanged` 為零，
沒有 source watchdog／fatal，Gateway／命令同步正常、五模式當次暖機 receipt ready，
opening input inflight 為零，五個 signal ID 的 engine exact ACK revision lag 為零。
這不是額外的 GPU 數值驗證。

原生 updater 的 consumer 全驗為 **14.353 秒**（metadata/calendar 11.503 秒、
全部 member/dictionary SHA 2.844 秒）；另記 canonical activation **2.282 秒**。
此樣本發生在冷 owner 啟動與五模式暖機同時進行，不能用隔離測速的 5.804 秒取代它，
也不能只把 consumer 計時稱作整個 updater／開盤工作流。
重點是此段按既有 08:15 preparation clock 在開盤前完成，不再在 09:00 內重算約 35 分鐘歷史。

Factorized 隨原生 scheduler 以接受的 published inputs 與多源價格自行恢復，
**10/20 10:28:41** 發布新的真實時間戳模擬訊號，原引擎確認並建立 **2 筆模擬持倉**。
六個 protected owner 的 PID／InvocationID 不變，當沖引擎仍為 PID 2157718、原 run
`c18cd72d1728431186e5203c28cfca49`，三個既有帳本前綴 SHA 不變；正常引擎可繼續 append，
沒有把整個帳本說成未變、未手改歷史或送券商單。
今天原始 09:00／11 秒失敗仍保留，不能把這次延遲恢復回填成準時成交。

[公網 HTTPS 與 JS SHA 讀回](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/public_ui_readback.json)
確認 `penguin72487.dpdns.org` 兩個頁面實際送出新資產；
[最終前端驗證](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/frontend_final.json)
另檢查載入資料前的 accessible button 預設狀態與公網 HTML，保留新版 source／test SHA。

此變更沒有鎖定整台機器的 Mamba／系統套件，也沒有測試 Windows／實體斷電重開。
真正 09:00 的時鐘、外部來源延遲、Factorized 資料準備與全目標模擬成交仍須各自驗收，
不能由版本隔離測試推論為全系統已正常或延遲 SLO 已達標。

## 完整開盤取價與 MIS 斷線恢復（2026-10-21）

### 今日原始失敗與來源責任

此次以官方當日交易清冊 **2,308 檔**為分母，不以模型成員、供應商回傳數或有成交量檔數
縮小範圍（登錄 2,313 檔，官方停交易 5 檔）。五模式盤前 final-arm 於 08:45–08:46
已有 ready 證據；不能把這次延遲籠統歸因為「模型沒有暖機」。

[今日第一份 11 秒驗收](../artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-21/first_assessment_11000ms.json)
保留 `missed_or_unverified`：494 檔真開盤、0 檔官方無首筆、1,814 檔未知，五模式原始訊號
與完整模擬成交均未通過期限。晚到資料或 09:01 回放不能覆蓋這份原始失敗。
[來源與原生狀態取證](../artifacts/operations/tw_opening_latency_20261021/forensics.json)
保留完整分階段日誌；這個檔案較大，日常查閱應只投影指定時間／job 的摘要。

| 階段／來源 | 本次觀察 | 可控部分與外部邊界 |
| --- | --- | --- |
| 官方日曆維護 | 09:00 的半小時 timer 持有共用 source writer lock，整段約 1.214 秒；首輪全來源到 09:00:01.573 才啟動 | 移開不必要的維護 writer，並使取價不再等待模型讀取／今日 receipt 建立；不放寬模型的來源一致性鎖 |
| 永豐共享 owner | 首批約 09:00:01.815；首輪完整 response 約 09:00:03.074，但不是每檔已有有效首筆 | 保留既有 session、分批回呼與原生來源時鐘；全量 response 不等於完整真開盤價 |
| FinMind 全市場 | 首輪 2,886 列只有 3 列有效首筆、2,831 列仍是其他日期；首輪 headers 約 974 ms、body 約 235 ms | 保留共享 single-flight 與首 11 秒一秒 cadence；來源更新前不能把昨日列改成今日價格 |
| 官方 MIS | 首 4 個請求全數 timeout，約 4.18–5.17 秒；6.802 秒結束後又固定等 15 秒，21.899 秒才重試 | 固定靜默可改；timeout 僅定位到 connect/TLS/headers 等待，不能據此臆測是哪個網路節點故障 |
| Fugle | TSE／OTC 回覆 403 | 尚非可用的第四路全市場行情；保留權限退避，不重試轟炸或宣稱已接通 |

目前可用接線仍為永豐共享 owner、官方 MIS、FinMind；「有設定憑證」不是每條路都健康，
也不是全市場串流權限。永豐 Snapshot 是查詢而非即時推播，官方限制與訂閱上限仍須遵守：
[Snapshot](https://sinotrade.github.io/tutor/market_data/snapshot/)、
[使用限制](https://sinotrade.github.io/zh/tutor/limit/)。
FinMind 的[即時資料說明](https://finmind.github.io/tutor/TaiwanMarket/RealTime/)
約十秒更新不是固定相位的 09:00 首筆或十一秒全市場 SLA。

### 共用責任入口的修正

1. 日曆 launcher 在取得 writer lock **前後**檢查開盤保護窗口；已有驗證日曆時延後重複維護。
   缺少／不可信日曆仍可走原 collector bootstrap，真正檢查錯誤不改成成功。
   原 systemd timer 直接呼叫此 launcher，無須重啟交易服務；真正 09:00 行為仍待下一次開盤驗收。
2. `repair_tw_opening_snapshot_receipt` 可從目前官方交易清冊獨立 bootstrap，不再依賴模型先建立
   今日價格 receipt。盤前不送行情請求；錯誤日期、foreign root、毀損 receipt 與缺少官方清冊
   仍拒絕，不用新路徑覆蓋不可信資料。
3. 來源 observer 於 08:59:30–09:00:10 使用 10 Hz 喚醒，其他時間維持一秒；既有來源 owner
   仍掌管 single-flight、持久化與退避。08:59:30–49 每個 root 最多一次平行 transport 預熱，
   不建立新永豐登入、不準備模型，也不把探測回應當開盤資料。避免 08:59:59 才啟動長探測。
4. MIS 全掃斷線後保留 15 秒 full-sweep circuit，但允許 **一檔、單一 inflight** 的 half-open
   探測：0.5、1、2、4、8、15 秒指數等待。收到可解析 requested row 才恢復 unresolved delta；
   舊日列只能證明 transport 恢復，不能證明有效開盤。401／402／403／429 仍保留 60 秒退避。
   並行成功來源與已取得資料不被失敗來源取消；未改官方無首筆、正成交量、來源日期的驗證。
5. 日誌新增 `transport_recovery_probe`、`transport_health_recovered` 與 observer cadence，
   canary 仍標示局部範圍，不能拿它冒充全市場完成。無首筆且官方確認成交量零時，沿用前一
   完成交易日值仍僅是既有模型輸入政策，不是可成交開盤價或帳本事實。

### 重複測速、回歸與部署邊界

[完整分母 ABBA 故障注入](../artifacts/operations/tw_opening_latency_20261021/mis_half_open_abba.json)
以相同 2,308 檔、空的私有 cache、真實退避時鐘跑四次；情境為「首次全掃斷線、來源隨後恢復」。
含驗證、合併、journal/fsync、owner drain 及獨立 durable readback，中位數由 **15,108.636 ms
降至 621.721 ms**。全部金融值、來源時鐘及 durable rows hash 一致。這是**離線故障恢復比較**，
不是實際網路開盤、全部 API 速度提高 24 倍、模型完成或模擬成交 SLA。

重測入口使用既有 benchmark，不新增行情 owner：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-21.json \
  --compare-mis-recovery artifacts/operations/tw_opening_latency_20261021/baseline/opening_price_acquisition.py \
  --rounds 2 \
  --output artifacts/operations/tw_opening_latency_20261021/mis_half_open_abba_repeat.json
```

每次使用未占用的輸出檔名；原始 baseline、比較結果與 SHA 皆保留，不覆蓋舊測速。

10:30 的[原生獨立網路樣本](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-21/intraday_103001748749.json)
全流程約 1,230.213 ms，但僅 2,140 檔真開盤、168 檔未知，`data_complete=false`；MIS 探測失敗、
Fugle 403。這不是完整取價成功，也不是本次變更的實際 before/after。

[工作區相關回歸](../artifacts/operations/agent-workflow/runs/opening-1021-final-source-plus-d-20261021T025427-40a5aa74/run.log)
為 **769 passed in 50.14s**，涵蓋來源 fan-in、清冊、持久化、來源鎖、日曆排程、Discord cadence、
訊號／執行計時與 source namespace。沒有宣稱全庫測試。
部署驗證另找到 relative receipt path 直接進 systemd BindPaths 的錯誤；現在使用 verifier 回傳的
absolute receipt 建立綁定，symlink 拒絕不變，並補相對路徑／symlink 回歸。

第一份部署 plan 因控制鎖被占用而拒絕；第二次 freeze 因 source membership/content 變動而拒絕。
保留全部失敗 run，不清鎖、不忽略版本檢查。後續
[第三份完整凍結版本](../artifacts/operations/tw_opening_latency_20261021/source_release_preparation_v3.json)
與[相對路徑原生 namespace 驗證](../artifacts/operations/tw_opening_latency_20261021/source_release_verification_v3.json)
確認 1,585 source files、唯讀掛載與相同 manifest；此時仍是 `prepared_not_adopted`。
[新的 plan-only](../artifacts/operations/tw_opening_latency_20261021/deployment_plan_v3.json)
已通過 Discord-only code admission 前置檢查，未要求重啟。正式採用、原生模型暖機、下次 09:00
首筆／全量取價及全目標模擬成交須另附證據；不能因 plan 或工程測試成功而預先宣稱完成。

### 凍結版本回歸與正式取價端採用

同一份候選的[原生唯讀 namespace 回歸](../artifacts/operations/agent-workflow/runs/opening-1021-frozen-source-regre-20261021T030038-4835b3ad/run.log)
另通過 **769 passed in 45.03s**，不是只測可能持續修改的工作區。
控制鎖解除、所有前置檢查通過後，於 **11:03:43** 完成
[qualified Discord-only 採用](../artifacts/operations/tw_opening_latency_20261021/source_adoption_v3.json)：
`accepted_code_admission_only`，loaded code SHA 為
`b52f00a25deada847ff57303296be87e2f0e16b61746b3ed8162de12ef2a7702`，Python owner
`1255414-1792551811055849914`；Gateway／命令同步及五模式 exact engine ACK 通過，revision lag 為零。

六個 protected owner 的 PID／InvocationID 不變，當沖引擎仍為 MainPID **536534**、run
`0e95c7f2ed354874a09a76bd0762bb63`；三份 append-only 帳本前綴 SHA 通過，沒有人工改寫訊號、
成交或歷史。這次只重啟 Discord／取價端，沒有重啟交易引擎、改模型或送券商單。
新程式的來源修復 observer 已由新 run 自然執行並寫入新的 cadence policy。
當時的 code-only 驗收明確為 `gpu_warmup_verified=false`；模型快取由原生無下單暖機繼續完成，
不把 Gateway ready 或版本採用成功當成模型駐留就緒。

既有 `stockagent-tw-opening-audit.timer` 與 `stockagent-tw-quote-phase-benchmark.timer` 仍排程運行，
持續保留開盤、盤中與盤後的原生日誌及完整範圍。**下一次真實開盤的首筆、全量輸入、原始訊號
與全目標模擬成交尚未驗收**；來源端發布週期、尚未首筆成交與服務端拒絕仍是獨立外部限制，
不能承諾一秒、零錯誤或宣稱所有瓶頸都已排除。

**11:09 的獨立唯讀驗收**另保存在
[原生採用讀回](../artifacts/operations/tw_opening_latency_20261021/native_adoption_readback_v3.json)。
沿用 qualified reload 的 `native_ready` 完整分支驗證同一新 owner，五模式自然暖機於
**11:06:03** 完成，148.422 秒、5/5 ready、resident ready；重新核對 source namespace、
六個 protected owner、三份帳本前綴及最新 exact ACK（lag=0）。驗證器沒有呼叫 provider、GPU、
重啟或寫帳本。取價 observer 當時為 **2,186 真開盤＋122 官方無首筆＝2,308 已解析輸入**、
unknown=0，仍標示 `awaiting_first_print`；122 檔不能冒充有真實開盤價，11:09 的覆蓋也不能
回填成 09:00 的成績。

獨立讀回的首個工程命令誤用了「盤前自然暖機」validator 的不相容 control schema，
保留其 `KeyError` 失敗收據；改用本次盤中 qualified reload 對應的現有 `native_ready`，
沒有放寬共享 verifier、延長暖機窗口或重新啟動 owner。

## 晚間續修：盤前準備與逐請求時間軸（2026-10-21–22）

本段是上一節的續修，不覆寫 10/21 原始十一秒失敗，也不以重播或離峰樣本宣稱開盤達標。
目前取價流程為「盤前資料／連線準備 → 三路並行取價 → 逐批驗證合併 → durable input
receipt → 各模式推論 → 模擬進場／畫面」，不同完成邊界不得混算。

### 此次新增的實作

- `load_symbol_yahoo_map` 依檔案 device、inode、size、mtime、ctime 重用解析結果，
  每次使用仍檢查檔案版本；回傳獨立字典，不讓單一 consumer 汙染後續請求。
  讀取前後再次核對版本，缺檔／無法解析／讀取中更新不快取失敗，下次正常重試。
  此處僅是交易所路由映射，不能取代官方當日交易清冊或改小市場分母。
- 沿用既有 `OpeningPriceFanIn.warm_transports` 在盤前解析映射，新增
  `local_preparation` 計時；不新增下載／服務 owner、不建立永豐登入、不 seed 價格。
  開盤後若檔案變動，仍重新解析。來源 transport readiness 不等於名冊或價格就緒。
- MIS 原 `mis_total_ms` 起點在映射與陣列準備之後，會漏算這段成本；現在從來源函式入口
  計時，另記 `mis_source_prepare_ms`，並以
  `source_entry_including_venue_map_and_arrays_v2` 明示新邊界。新舊局部計時不直接互比。
- 每個 MIS 請求增加同一 monotonic clock 下的排隊、實際送出、headers、body、列解碼完成
  與失敗 offset。未抵達的階段保持缺席，不填零；日常 audit 只保留封閉白名單的安全數值。
  不記錄 URL、Cookie、Token 或任意錯誤文字。這些 spans 可重疊，不能全部相加當總耗時。

### 離峰網路實測：快不等於完整或穩定

2026/10/21 23:48–23:51，從空的隔離 receipt 對官方 **2,308 檔**範圍測試 MIS，包含來源
驗證、合併、落盤與獨立讀回；沒有額外永豐登入、券商下單、訊號發布或正式帳本修改。
下列併發數為個別測試參數，**沒有修改正式預設值 4**。

| 樣本 | 來源完成與獨立讀回 | 真開盤／官方無首筆／未知 | 請求失敗 |
| --- | --- | --- | --- |
| [4 路首次](../artifacts/operations/tw_opening_latency_20261021/evening_v4/mis_network.json) | 2,672.586 ms | 2,282／26／0 | 0 |
| [8 路兩次](../artifacts/operations/tw_opening_latency_20261021/evening_v4/mis_network_eight.json) | 442.665、282.970 ms | 各 2,282／26／0 | 各 0 |
| [4 路再測](../artifacts/operations/tw_opening_latency_20261021/evening_v4/mis_network_four_repeat.json) | 1,241.603、1,046.565 ms | 2,282／26／0；1,032／8／1,268 | 0；4 |
| [16 路兩次](../artifacts/operations/tw_opening_latency_20261021/evening_v4/mis_network_sixteen.json) | 488.755、317.942 ms | 各 0／0／2,308 | 各 16 |

16 路失敗鏈為 `ConnectionError → ProtocolError → RemoteDisconnected`，發生在收到
headers 之前，沒有 HTTP 錯誤碼。這可證明本次 response 尚未取得，不能單憑它斷言是
來源限流、TLS、ISP 或特定網路節點。測試後停止加壓；沒有繞過拒絕或用最快樣本提高預設。
這些是不同時間的離峰觀察，不是併發設定的受控制因果比較，也不是 09:00 的數據。

26 檔「官方無首筆」只使模型輸入解析完成，不能稱為 26 個真開盤價或可執行成交。
因此成功樣本為 `resolved_complete=true`、`data_complete=false`；失敗樣本仍保留
`resolved_complete=false`。外部未發布首筆、回傳昨日資料與拒絕連線，仍是獨立限制。

### 對照測速與測試邊界

[最終完整本機 ABBA](../artifacts/operations/tw_opening_latency_20261021/evening_v4/delivery_abba.json)
用原始 10/16 的 **2,301 檔、29 份回應**，驗證原回應 SHA、原交換時鐘與當時清冊契約，
凍結比較用本機時鐘，從空隔離 cache 跑五輪。含解析、合併、journal、真 fsync、owner drain
與獨立 durable readback；金融欄位／時鐘及 durable rows hash 全部一致，沒有真實網路請求。
這個 corpus 的 2,301 不是現在的 2,308，不能據此變更正式分母。

本次全段中位數為 **177.851 → 169.293 ms**；另外的
[相同版本 AA 對照](../artifacts/operations/tw_opening_latency_20261021/evening_v4/aa_control.json)
仍出現 **331.443 → 283.141 ms** 的環境／執行順序波動，因此不宣稱全段已可靠提高某個百分比。
可以單獨確認的是：最終對照的映射解析原每次 **4.713–8.752 ms**；候選首次仍花
**7.548 ms**，同版本後續重用為 **0.085–0.131 ms**。這是省下重複解析，不是略過冷讀、
官方清冊或冒充熱開盤報價。盤前準備另由語意測試驗證，沒有算入此 corpus 的來源完成成績。

[HTTP 5 ms／consumer 20 ms 延遲注入](../artifacts/operations/tw_opening_latency_20261021/evening_v4/overlap_injection.json)
也保持所有價格、時鐘、完整請求與落盤列一致；合成延遲僅驗證 pipeline 重疊／完整性，
不當作實際 API 延遲。最終交付檔 SHA 已在 `delivery_abba.json` 保留。

[擴大語意回歸](../artifacts/operations/agent-workflow/runs/opening-1021-evening-broad-final-20261021T155804-4764b049/run.log)
為 **812 passed in 49.53s**；run 的完整命令含匯入／啟動耗時為 **53.820 秒**。
涵蓋 MIS transport、共享永豐、並行 fan-in、stream/IPC、官方清冊、來源鎖、日曆、
Discord 準備與 cadence、失敗恢復、持久化、訊號／執行計時及 namespace，沒有宣稱全庫通過。
新測試包括同大小／mtime 仍須失效、讀取中換檔、consumer 隔離、盤前映射重用、
來源準備計時不可漏算，以及失敗請求不得產生未抵達的時間點。

### 採用狀態與剩餘工作

10/21 23:58 時，本節新增修正仍在工作區，Discord 使用前一節的唯讀 source release；
當時未重啟任何服務，也未宣稱新時間軸已在正式 owner 記錄。使用者 10/22 明確要求立即
部署後，依下節固定版本並驗收採用，以下保留切換前觀察，不以其冒充切換後狀態。

23:58 的唯讀觀察：Discord、當沖引擎與公開面板均 active，PID 分別為 2944796、2773600、
3122280；Discord connected/core ready，六模式暖機紀錄為 6/6，
但 `model_cache_resident_ready=false`，不據此宣稱下一個開盤 GPU／模型已駐留。
既有 audit timer 排定 10/22 08:55，盤中獨立取價測速為 10/22 10:30。

### 10/22 最新版本部署與獨立讀回

- 經既有 `scripts/freeze_discord_code_release.py` 固定 **1,599** 個檔案，
  [準備收據](../artifacts/operations/tw_opening_latency_20261022/source_release_preparation_v1.json)
  對應 release `20261021T162054Z-121a0c4854ca`，source SHA256 為
  `e70743446dba83a68ffb0b268721e2fac5b13b669390bce89ded6672dd663fe4`。
  不將憑證、資料集或模型複製進 release，仍保留原生 live config 掛載。
- 在同一候選版本的唯讀 namespace 重跑前述回歸，另納入此次共享依賴的 private JSON、
  completed cache 與 signal helpers 測試；
  [結果](../artifacts/operations/agent-workflow/runs/opening-1022-frozen-regression-20261021T162150-2fbcdfae/run.log)
  為 **1,010 passed in 50.50s**，完整受監督命令 **54.212 秒、exit 0**。
  沒有宣稱全庫測試通過。
- qualified reload 先 plan，重新確認空倉、無待辦下單、來源鎖與 coherent ACK 後，執行
  `--discord-only --code-admission-only --source-release ... --apply`。
  [採用收據](../artifacts/operations/tw_opening_latency_20261022/source_adoption_v1.json)
  於 **10/22 00:24:35 +08:00** 回報 `accepted_code_admission_only`；僅切換 Discord，
  新原生 run 為 `3558289-1792599867604918119`，loaded code SHA256 為
  `87c6112aa7247dc96ff99f6524c15ed26c31081288b5a30aca1ce94009bf0395`。
- **00:25:04** 的另一個
  [獨立讀回](../artifacts/operations/tw_opening_latency_20261022/independent_readback_v1.json)
  重新確認原生程序實際唯讀掛載 **1,599** 檔，直接讀取 `/proc/<pid>/root` 下取價、fan-in、
  audit 與 benchmark 四檔 SHA；六模式訊號與引擎原 revision 完全 ACK，**lag 0**。
  Gateway connected/core ready，當次 invocation 已完成 command sync 或明確 deferred，
  無 watchdog/fatal 訊息；不使用舊 heartbeat 或環境變數標籤作為載入證明。
- 執行引擎 run 保持 `e08e40180894479e85db16f9439e5b52`，受保護六個服務 owner 未變。
  兩次核對 events/orders/fills 既有前綴 SHA，長度分別 **2,300,465／427,342,677／424,218,829
  bytes**，切換與讀回均未改寫帳本；沒有額外行情登入、行情請求或券商下單。

這是 **loaded-code／Gateway／ACK 採用**，不是盤前 GPU 暖機或 09:00 取價達標。
離峰新 owner 的暖機為 `pending`，由既有準備時鐘負責，不用手動暖機改寫成即時就緒。
盤後 artifact maintenance 的 Factorized、MGDA 兩項原告警仍如實為 `degraded`；
這次沒有刪除失敗收據或把它們偽裝成已修復，亦不將它們等同 Gateway 中斷。

後續仍須在真實交易日核對第一筆、全量已解析輸入、全真開盤、各模型原始訊號與全部模擬
進場的各自時間。既有 10/22 08:55 audit 與 10:30 盤中測速 timer 均保留；沒有新增競爭
行情 owner。尚未達到「只剩 API 限制」或一秒／零錯誤保證；目前可控成本的進展與外部
拒絕均如實保留，任務繼續維持 active，不把後來資料回填成準時成績。

### 10/22 原生開盤失敗與等待路徑減負

今日官方可交易名冊為 **2,309 檔**。依逐筆來源時鐘重建的
[11 秒觀察](../artifacts/operations/tw_opening_latency_20261022/morning_native_audit/2026-10-22/first_assessment_11000ms.json)
只有 **434 個真開盤價、0 個已證明尚未首筆、1,875 個未知**；其中永豐 428、FinMind 188，
兩者有重疊，不可直接相加。這份盤中另建 audit 的模式名冊不是盤前部署證明；
不能用稍後回放、補齊資料或這次部署，改寫今日準時訊號與成交失敗的事實。

完整取價的關鍵路徑是「每檔取所有可用來源中最早的合格觀測，再取全名冊最晚那檔」，
不是最早一個 HTTP 200，也不是各來源總耗時相加。沒有首筆成交時，真開盤價尚不存在；
已有證據的 model-only 沿用參考、未知來源失敗及可執行成交仍分開。
平行 fan-in 可消除慢來源阻擋快來源，但不能使未發布的觀測提早出現；
本機 merge、推論、發布與模擬進場也仍須逐段量測，不能宣稱已只剩 API 成本。

- MIS 首輪四個實際請求在 HTTP 回應前逾時，約 4.2 秒後進入既有斷路／探測流程；
  未發出的頻道不是已查詢成功。盤中單檔診斷的 TCP／TLS 可完成，但接著在回應前斷線，
  仍不足以歸因為特定 ISP、官方限流或本機錯誤，不以增加併發宣稱修復。
- Fugle 回覆 403，維持權限錯誤退避，不能用重試創造資料授權。
- 永豐與 FinMind 部分回覆仍屬前一交易日，不能把非今日回覆當真開盤價，
  亦不能把來源失敗當成官方零成交證明。各來源第一輪已平行啟動；這不是全部等待的終點。

本輪確認一項可控成本：六模式等待覆蓋時，反覆將相同來源的完整逐請求測速、
`last_completed_job` 與 transport 明細序列化並 fsync。現在只有該 owner 的完整
`finished` 事件已成功落盤後，後續 merged 事件才改為 `owner_pid + job_id` 引用；
新 transport 必須另外對應同一完成 job 才可引用。寫入失敗、未完成與不同 job 的資料仍 inline。
完整來源事件、錯誤、時間戳及回傳消費端的完整 DTO 不變；不放鬆價格或帳本契約。

[固定原始日誌前綴的四輪 ABBA](../artifacts/operations/tw_opening_latency_20261022/morning_journal_projection.json)
驗證 **298 個 merged 事件、1,455 個可解析引用**，還原全部 provider metrics 完全相等。
編碼量 **7,966,640 → 1,907,087 bytes（減少約 76%）**，projection 加 JSON 編碼中位耗時
**150.3925 → 47.1535 ms（約 3.19 倍）**。這沒有測網路、fsync 或完整 09:00 流程，
不能解讀為開盤取價加快三倍。沒有新增行情請求、券商下單或操作員帳本寫入。

[共享來源／恢復回歸](../artifacts/operations/agent-workflow/runs/opening-1022-source-regression-20261022T010626-52c6da68/run.log)
為 **572 passed in 42.83s**；另增加完成引用、寫入失敗保留明細、不同 job 與完整 DTO 的測試。
候選版本另走固定 source、同版本回歸、qualified Discord-only plan/apply 與獨立讀回；
部署結果以下載入收據為準，持倉中的交易引擎不重啟。

同版本 [較大範圍回歸](../artifacts/operations/agent-workflow/runs/opening-1022-morning-frozen-regr-20261022T010932-7096cb13/run.log)
完成 **1,034 passed in 78.35s**；不是全庫測試完成。固定版本
`20261022T010738Z-76f0e78551af` 含 **1,602** 個程式檔案，source SHA256 為
`1e42f35481148222d17b7fd7735f0ee03e18448651fe604a5d5ccd73dda59b3b`。

第一次 apply 被另一個 coordinator 的 OS lease 拒絕；第二次偵測到公開面板 owner 已更新，
於重啟前拒絕。兩份拒絕收據均保留，沒有強制解鎖或移除保護。重新通過同一套 admission 後，
[09:14:01 的正式採用](../artifacts/operations/tw_opening_latency_20261022/morning_source_adoption_stable.json)
回報 `accepted_code_admission_only`，Discord run 為 `409506-1792631630921857686`，
loaded code SHA256 為 `923338d39f183078d9ed7b3b34e5e675a5c58012e40950789b039da4b42515e0`。

[09:14:20 獨立讀回](../artifacts/operations/tw_opening_latency_20261022/morning_independent_readback.json)
再次核對原生程序唯讀掛載 **1,602 檔**及 `/proc/<pid>/root` 下四個相關程式檔 SHA，
六模式 ACK 精確匹配、lag 0；Gateway/core ready，當次 journal 沒有 watchdog/fatal。
當沖引擎 run `391d8564a80448c3b4889e229521c8c3` 與六個受保護 owner 不變。
events/orders/fills 的 **2,309,932／427,484,995／424,324,970 bytes** 既有前綴 SHA 全部通過。
只有 Discord／取價端重啟，沒有操作員帳本寫入或額外行情請求。

此驗收只證明固定版本、Gateway、ACK 與帳本保護，不證明 GPU 暖機或開盤 SLO。
MIS 來源前回應失敗、Fugle 403、來源實際發布時鐘與未首筆標的仍未解；
下一交易日仍由既有原生 audit 與分時來源測速核對，保留第一筆、全量解析、真開盤、
各模式發布與全目標模擬成交的獨立時間。沒有宣稱零錯誤、全市場一秒或所有本機成本已移除。

### 10/22 三時段來源清冊與完整本機處理續修

目前完整 provider／權限／容量清冊與開盤特有故障的方案更新在
[來源規劃最新查核](tw_opening_provider_plan_2026-10-13.md#最新查核2026-10-22)。
仍採正式 2,309 軸，保留原生開盤失敗，不換模型、不降低原本 quorum、不改帳本。
只有來源已發生且可取得的觀測，才可能以軟體最佳化提早交付；
`max(每檔所有合格來源最早可得時間)` 是完整 coverage 的下界，另有耐久保存、模型與執行成本。

這次在原 canonical fan-in／journal／audit 修正三點：

1. 完全沒有觀察的失敗／空來源不再搶寫完整輸入收據。消費端仍強制完整 scope／fsync；
   部分成功後失敗仍保存已觀察資料。來源錯誤、配額退避與未知狀態沒有消失。
2. 大批 `observations_delta` 共用欄名，以 row-values 無損保存所有標的數值、來源、
   整數時鐘與 flag；小批及舊格式保持可讀。共享純 parser 同供 audit 與獨立完整 readback，
   不新增 collector／quote cache，仍同步 strict JSON／fsync。非法 count／欄位／列寬拒絕，
   NaN 不會留下部分 journal。超過 2^53 的整數時鐘與 nextafter 浮點亦有逐值測試。
3. 三時段 audit v3 分別記錄 raw response、validated observation、真開盤與 resolved 的
   首筆及 50／90／95／99／100%。保留失敗的 P50／P95／P99 和權限、配額、無 HTTP 回應、
   body／schema 錯誤分類。job 完成但請求部分失敗仍告警；merged 事件不可冒充來源價格。

[同一真實 FinMind 回應的完整本機 ABBA](../artifacts/operations/tw_opening_latency_20261022/combined_local_abba.json)
使用 1,032,677 bytes、2,309 軸與兩個明示的受控失敗來源，四輪交錯比較
ingestion→驗證→完整來源 journal→輸入耐久保存。中位耗時 **149.184 → 109.671 ms**，
減少 **26.5%（約 1.36×）**；journal 約 **508 → 257 KB**，實際 input receipt write **2 → 1**。
所有 financial arrays、時鐘、durable rows 與還原完整 observation journal SHA 一致；
該固定 payload 為 2,060 真開盤／249 未解，沒有縮 scope 或用快取冒充全量。
分別測試的兩個局部 ABBA 不相乘外推。另測 native JSON＋通用遞迴驗證較慢
（22.24 vs 原 strict stdlib 16.26 ms），因此未採用。

真實網路與離線比較不同：09:49／09:58 的完整 probe 為 1.459／1.391 秒，
但 MIS 當時單檔失敗，不能稱全量成功。[10:05 新測試](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-22/intraday_100524023512.json)
MIS 已有部分回覆，單檔預熱 **1,361 ms**、完整來源內 HTTP 流程約 **7,893 ms**，
29 請求有 5 個在回應前失敗；合併最終 2,087 真開盤／178 已證未首筆／44 未解。
90% quorum 雖 **976.097 ms**，完整排空與獨立空快取 readback 至報告寫入前為
**9,624.638 ms**。不能挑前面較快但缺更多資料的測試來宣稱開盤變快。
最近已完成的盤後是 **10/21**：2.134 秒、2,282／2,308 真開盤、26 未解；
今日 13:40 仍待原生排程，不以盤中樣本代替。

[Fugle 單檔 WebSocket 能力測試](../artifacts/operations/tw_opening_latency_20261022/fugle_stream_capability.json)
以既有 key 成功認證、訂閱並在冷連線 **166.557 ms** 收到 2330 snapshot，隨即關閉。
REST 403 不能推論所有介面均無權限；但單檔成功也不是全市場容量或開盤 SLA。
該來源載今日 2330 開盤成交事件為 **09:00:08.980736**；尚未發生的真實成交不可能
在 +1 秒取得。這是 provider 事件鐘，未證實交易所發布與網路傳送的獨立時間。
預開足額全市場串流、完整訂閱 ACK 與 gap 恢復，仍需授權容量與原生開盤實測。

三時段沿用既有 08:55 roster、原生開盤被動 audit、10:30／13:40 非券商完整來源 probe，
及 10:31／15:10 audit timer。它們已核對 active，但 timer active 不代表未來資料齊全。
開盤不額外登入永豐／輪詢 snapshots；行情展示權限隔離不變。

固定版本 `20261022T020943Z-a7638bdaca9f` 的
[842 項整合測試](../artifacts/operations/agent-workflow/runs/opening-1022-adoption-tests-20261022T021109-dfaf2a2e/run.log)
全部通過，耗時 43.50 秒。較早一次用了兩個不存在的測試路徑而沒有收集測試，
該失敗仍保留，不能列入成功數；本輪也沒有宣稱全庫測試通過。
新加的共享 journal helper 僅為既有 writer／audit 共用的可重用 source，
本機行情 payload、benchmark／release artifacts 沒有加入版本控制。

一份 plan 和第一次 apply 遇到其他 coordinator 的 OS lease，均在重啟前拒絕。
沒有強制解鎖；重新執行相同 admission 後，[10:13:54 採用收據](../artifacts/operations/tw_opening_latency_20261022/phase_adoption_deploy_retry.json)
回報 `accepted_code_admission_only`，source SHA256 為
`1c6b6350111cfae52ba33e1fb3bb65edf3b8cd18063b2f05189e3ed0555fa285`，
載入 code SHA256 `7088534a41fd3d9883d4117585097a6897c7e431a354e4a29514fffdc0f6f78d`。
[10:14:29 獨立讀回](../artifacts/operations/tw_opening_latency_20261022/phase_independent_readback.json)
再次核對原生唯讀 source **1,603 檔**與 `/proc/<pid>/root` 下六個相關檔案 SHA，
六模式精確 ACK、lag 0；Discord run `536373-1792635223892501897`，Gateway/core ready。
當沖引擎 run `391d8564a80448c3b4889e229521c8c3` 與六個受保護 owner 不變。
events/orders/fills 各 **2,311,454／428,520,344／425,249,658 bytes** 的既有前綴 SHA
全部通過，允許交易引擎繼續正常 append，沒有操作員帳本寫入或券商委託。
獨立讀回沒有額外行情請求、沒有服務重啟；當次 journal 沒有 watchdog/fatal。

此驗收不證明 GPU 暖機、資料齊備或原生開盤 SLO；原先 Factorized／MGDA
artifact maintenance 仍為 `degraded`，沒有用 Gateway ready 掩蓋。
今天盤後與下一交易日開盤仍須原生排程驗收；需要完整串流授權的候選沒有擅自購買，
MIS 間歇斷線與 44 未解也沒有被改寫為成功。尚未達只剩 API 限制／全量一秒／零錯誤，
原工程任務維持 active，下一階段先驗收足額全市場串流，再對實際最慢階段進行配對重測。

### 最新口徑重測與本機傳輸續修：2026-10-22

本輪仍是完整官方 **2,309 檔**，不縮分母、不降低 quorum、不刪價格驗證。
同日真正首筆優先；只有有效且未過期的無首筆證據，才可使用最後真實成交作
模型參考。未知、過期／未來的 absence proof、模型參考與可執行價分開。

#### 原因與共用修正

- 舊診斷只等 8 秒便關閉來源 owner，11:29 實測 MIS 約 10.218 秒，後到的觀察
  因而被診斷本身的 close fence 排除。舊數字保留為失敗證據，不當完整來源基線。
- `inflight=false` 發生在 worker 的 input fsync／finished journal 之前，不能表示
  工作持久完成。`wait_idle` 現在跟蹤完整 Future；診斷預設最多等 90 秒，逾時
  `all_sources_complete_ms=null` 並顯示 deadline exceeded。這不延長模型 quorum。
- 自訂 cache reader 的 quorum／重新派工也檢查 absence 證據本身的 15 秒年齡，
  不讓另一筆新行情替舊證據續期。報告在 quorum 當下及 canonical 空程序快取
  回讀後各算一次；resolved observation 不冒充模型輸入完整或成交能力。
- 永豐 avoided-query 記錄原本在 cache lock 內、native submission 之前。
  現在移到鎖外、所有原生請求提交後，與原有 callback 等待重疊；沒有少記帳、
  多登入、提高查詢頻率，也不延長 callback 原始截止時間。
- 完整 source DTO 的本機 IPC 可協商 MessagePack，維持舊 client／server JSON
  相容與可選套件缺席 fallback。只有已驗證的 plain JSON 值可走新格式，保留
  float64／NaN／Inf／有號零、int64 時鐘、旗標及缺值；超界整數／legacy 型別
  沿用原 JSON 語義，bytes／datetime 不被悄悄轉換。不影響公開 API、來源收據
  或交易帳本格式，不使用 pickle。新增實際 codec、bytes、讀取／解碼／陣列
  materialize 計時；舊的合計時計仍保留。

#### 可歸因的完整軸本機比較

[最終 10 組交錯配對測速](../artifacts/operations/tw_opening_latest_standard_20261022/ipc_serialization_final_abba.json)
採相同 autonomous owner、相同 frozen scope、零人工延遲；含所有 snapshot 陣列、
缺值／非有限浮點、精確 int64 及逐欄 dtype／shape／位元 SHA 驗證。
不包含實網、模型或成交；沒有 API／生產收據／帳本寫入。

| 中位數 | JSON | 協商 MessagePack |
| --- | ---: | ---: |
| 完整請求至逐欄驗證完成 | 31.769 ms | 21.706 ms |
| client request 至完整 DTO 回傳 | 28.463 ms | 16.266 ms |
| response 編碼與原子發布 | 12.256 ms | 3.941 ms |
| client response 解碼 | 4.857 ms | 0.739 ms |
| response bytes | 348,658 | 298,912 |

[前一組](../artifacts/operations/tw_opening_latest_standard_20261022/ipc_serialization_abba.json)
完整請求至驗證為 25.891→17.298 ms，方向一致、絕對值隨系統負載變動；不稱 P99、
理論極限或真實開盤一秒。輸出數值／旗標／整數時間完全一致。

[避免記錄阻擋提交的配對](../artifacts/operations/tw_opening_latest_standard_20261022/accounting_overlap_abba.json)
顯式注入 accounting 100 ms、response 100 ms，完整 callback→來源持久回讀
293.864→233.114 ms，所有來源陣列／進度／持久 row SHA 相同；這是受控等待，
不是實際網路加速倍數。[無人工延遲對照](../artifacts/operations/tw_opening_latest_standard_20261022/accounting_no_delay_abba.json)
只有 104.760→101.110 ms，小收益照實保留。
[無 profile 的完整 FinMind 本機比較](../artifacts/operations/tw_opening_latest_standard_20261022/finmind_unprofiled_abba.json)
保留原 1,032,677 bytes、2,309 軸、2,060 有效開盤及全部原始時鐘；來源、收據及
journal parity 通過。142.093／124.106 ms 的中位數含初次 scope 驗證與程序快取
次序差異，**不把這個差值歸因為新的 FinMind 演算法加速**。

#### 實網、負向證據與未完成邊界

[13:53 實網重測](../artifacts/operations/tw_opening_latest_standard_20261022/postclose_network.json)：
原 90% quorum 767.602 ms，全部 provider 工作與持久 journal 完成 1,320.166 ms，
probe 入口至最後報告寫入前 1,442.906 ms。canonical 空程序快取回讀只有
**2,275 真開盤＋0 有效無首筆證據＋34 未知**，仍為 partial；不是完整市場取價。
MIS 單檔 prewarm 為 `ConnectionError → ProtocolError → RemoteDisconnected`，
約 1.307 秒；沒有繼續送全市場 burst。Fugle TSE／OTC 仍 403。

較早 [8 workers scout](../artifacts/operations/tw_opening_latest_standard_20261022/mis_workers8_scout.json)
8／8 請求 ReadTimeout，無有效回覆；[4 workers control](../artifacts/operations/tw_opening_latest_standard_20261022/mis_workers4_control.json)
29 批有 2 個失敗、保留 27 個成功原始 body，仍非完整回放語料。兩者不同時點，
不宣稱併發必然導致失敗，也不採用 8 workers。來源恢復、完整全量授權仍未完成。

[今日原生整鏈稽核](../artifacts/operations/tw_opening_latest_standard_20261022/native_opening_audit.json)
仍保留 09:00 第 1 秒 3／2,309、第 11 秒 434／2,309；六模式全數發布為
**09:04:47.162969**。今天是可追溯回放，不算即時成交達標。成功嘗試仍有
準備、排隊、Factorized 推論約 8.018 秒等內部成本；本輪沒有證明只剩 API 延遲。

[唯讀重載計畫](../artifacts/operations/tw_opening_latest_standard_20261022/reload_plan.json)
被 `engine is nonflat or has pending orders: 17/0` 拒絕。沒有強制解鎖、重啟服務或
操作帳本；新 IPC 尚未完成常駐 owner 部署／真實開盤驗收。當前 Gateway ready、
engine ledger integrity ready／divergence 0，不能代替未平倉或資料完整性驗收。
工程任務 `tw-opening-latest-standard-latency-20261022` 保持 active；下一步在原有
flat／ACK／code admission gate 通過後部署，再由原生 09:00 日誌驗收整段，並優先
剖析完整準備／推論／發布的最慢段，不用下午或局部毫秒結果冒稱開盤 SLA。

[最終回歸收據](../artifacts/operations/agent-workflow/runs/latest-standard-final-regression-20261022T060054-be67365e/run.json)：
**1,256 passed in 45.02s**，涵蓋取價、官方清冊、最新最後成交參考、重試、等待
持久完成、過期證據、TP／SL 進場防護、模擬執行、公開價格政策、code admission、
新舊 IPC、可選套件缺席／非法型別、原始 body 保存與診斷預算邊界。相關檔案 Ruff
與 `git diff --check` 通過；不是全庫測試。最終測速與回歸之 quote owner SHA256
一致：`d513fc6dff880ca36bf3e6c7eceaa74acb3d8f901b45a5601b2c05d0f2e704ba`。
初次 benchmark 的靜態巢狀 `with` SyntaxError、超界輪數拒絕均保留原 run／log，
由後續成功同範圍測試在 task 裡標記 superseded，沒有把失敗改寫為成功。

重測入口（零下單；`--runtime` 只讀原生紀錄，不重跑模型）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_ownership.py \
  --receipt artifacts/operations/tw_opening_latest_standard_20261022/frozen_scope_receipt.json \
  --ipc-serialization --repeats 10 \
  --output artifacts/operations/tw_opening_latest_standard_20261022/ipc_recheck.json
run_fintech_python scripts/audit_tw_opening_latency.py \
  --session-date 2026-10-22 --runtime \
  --output artifacts/operations/tw_opening_latest_standard_20261022/opening_recheck.json
```

### 完整六模式準備／推論續測：2026-10-22 15:38

這輪延續最新開盤參考、官方清冊、缺值及成交證據邊界，改量先前尚未分清的
本機準備／推論／發布成本。不是增加 quorum、刪除特徵或改寫缺價；模型、
checkpoint、FP32／BF16 原設定、完整標的軸與交易帳本不變。

#### 根因與修正

- **指定日期未傳入 Factorized 讀取邊界**：BASE 已到 10/22，但特徵仍到 10/21。
  指定 10/21 的重測也附加了 10/22，因覆蓋不足而失敗。現在將明確的輸入日期
  傳到共用 attachment，在驗證 coverage 前截取；以原始 base owner＋日期＋
  全部成員版本作快取鍵。未指定日期的最新資料仍須完整 coverage，不能用舊日期
  假裝最新資料已就緒；共用 base 不被修改。
- **每次重複解析深層目錄**：3,287 個成員各自 `Path.resolve()`；一次 cProfile
  掃描 640.918 ms，其中 resolve 約 378 ms。改為單次掃描內共用已解析父目錄，
  每檔仍重新 lstat，最終 symlink 仍解析實際 target；掃描後重新檢查父目錄路徑
  與 inode／mtime／ctime。沒有跨請求 TTL，也沒有略過首次完整 SHA 驗證、
  attachment 前後版本比對或 canonical source read lease。
- **測速環境不能冒充正式設定**：改走正式 effective market 解析，並載入 MGDA
  原本的 `tw_day_trade_unit_mgda_fold10_runtime.env`。早先指紋不符是測速漏載
  sparse-adjoint 設定，不是選錯 checkpoint；沒有切換 fold 或放寬相容性檢查。
- 可變工作樹的完整測試曾被 `LoadedImplementationChanged` 拒絕；改用既有
  readonly canonical-path namespace 固定程式，不清除或重綁 admission。
  原失敗、fixture 缺欄位、未指定日期的 coverage 失敗均保留 run／log。

#### 無剖析器的固定輸入交錯實測

[最終完整六模式比較](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round2/pipeline_final_abba.json)
採相同收據、10/21 特徵、相同六 checkpoint，順序 A/B/B/A 重複兩組，共四對；
沒有 HTTP、Discord 訊息、券商委託或生產帳本／訊號寫入。A 只還原舊逐檔 resolve，
B 用新版本檢查；其餘流程相同。每次實際執行模型，完整權重、決策及模型解釋
SHA 一致；兩側都完成 rich artifacts，檢查 Parquet 存在與完成指標，沒有跳過報表。

| 中位數 | 舊逐檔 resolve | 新掃描內共用父目錄 |
| --- | ---: | ---: |
| 六模式全部發布 | 4,228.148 ms | 2,874.829 ms |
| 六模式完整產物完成 | 5,214.239 ms | 3,604.687 ms |

本組全部發布中位數減少 **32.0%**，完整產物減少 **30.9%**。四對中三對較快，
一對 control 2,692.828／candidate 2,760.665 ms，保留這項負向樣本；系統有其他
服務負載，不能宣稱每次皆快 32%、P99 或理論極限，也不把其他四模型的波動
全部歸因於只影響 Factorized 的目錄修正。

[單獨十對全成員掃描](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round2/member_signature_final_abba.json)
中位數 **419.267 → 108.509 ms**，全部 3,287 筆 canonical path、device、inode、
size、mtime、ctime tuple 完全相同；不是只量少數 shard。
[最終輸入準備探針](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round2/inputs_final.json)
六模式第一次 6,629.273 ms，後兩輪仍 2,879.018／2,025.851 ms；沒有 GPU、模型
或 HTTP。這組與最初 15,647.632 ms 的不同時點測試並非受控 A/B，不能把差值
全算成加速。舊 Path 版本的 profile 與中間結果亦完整保留於同一 evidence root。

完整推論 benchmark 現在另存兩輪暖機及每模式階段，不把它們丟掉。最終執行的
首次六模式約 **80.939 秒**（包含新私人 panel cache 的來源重建），第二輪約
**4.136 秒**；不是磁碟／OS 全冷啟動保證。較早固定版本的 Factorized 首次
推論 7.502 秒、暖機約 0.3～0.4 秒，證明開盤前的版本一致與實際暖機仍必要，
下午暖快取不等於明天開盤達標。

#### 部署、驗收與剩餘順序

[655 項相關回歸](../artifacts/operations/agent-workflow/runs/latest-standard-final-regression-20261022T072950-4533c971/run.json)
通過（45.29 秒），涵蓋輸入／cache、日期邊界、成員同大小修改、缺檔、final／
parent symlink 變更、六模式測速、最新無首筆參考、來源恢復、公開價格及 pinned
code；不是全庫測試。相關 Ruff 與 `git diff --check` 通過。

最新唯讀部署計畫已由先前 17 筆未平轉為 **0 未平／0 pending**，本輪未強制沖銷。
[正式採用收據](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round2/discord_adoption.json)
只重載 Discord owner；新 run `470700-1792654562409463796`，原生唯讀程式共
1,608 個檔案驗證、code admission 與六模式精確 ACK 通過，revision lag 0。
引擎 run `4ee29549d68149058397e8cb90f05744` 不變，其餘受保護 owner 未重啟。
events／orders／fills 原有前綴 SHA 通過，沒有操作員帳本寫入或券商下單。

[部署後獨立稽核](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round2/post_adoption_audit.json)
確認 Gateway／core ready、引擎 ledger integrity ready、divergence 0；盤後引擎
health 為 waiting。GPU warmup 仍 pending，未將 code-only admission 當作 GPU
ready。Factorized／MGDA 的盤後 artifact maintenance 仍 degraded，不能以這輪
歷史日期推論成功掩蓋；實網來源缺口與真正 09:00 整鏈尚未完成新版本驗收。

下一步按已量到的關鍵路徑：先修復兩個模式的最新盤後特徵與相容產物、驗收
開盤前六模式同版本暖機／final arm；再量原生並行輸入準備、序列 GPU 與持久
發布。約 5.20 GB 的每個 Factorized 全形狀輸入、格式化及來源等待各有成本，
不能未驗證就換算成一秒。維持既有每日原生 observer 記錄來源首筆／quorum／
全量、各模型準備／排隊／推論／發布、引擎註冊及真正 paper fill 時計。

新增重測入口（私人暫存產物；需可用 CUDA；不實際下單）：

```bash
source scripts/runtime_env.sh
set -a
source configs/deployments/tw_day_trade_unit_mgda_fold10_runtime.env
set +a
run_fintech_python scripts/benchmark_opening_preparation.py \
  --compare-member-signatures --repeats 10 --output artifacts/operations/member_scan_recheck.json
run_fintech_python scripts/benchmark_opening_signal_pipeline.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-22.json \
  --feature-date 2026-10-21 --repeats 4 --defer-reports --compare-member-signatures \
  --output artifacts/operations/pipeline_recheck.json
```

若工作樹仍有並行修改，沿用 `service_code_release.freeze_source/run_in_namespace`
固定版本後執行，不能清除 admission 繼續跑。工程任務保持 active；未宣稱全市場
即時取價／六模式成交一秒、零錯誤或已達只剩外部 API 的極限。

### 開盤延遲續測與契約範圍精簡：2026-10-22 晚間

本輪保留完整六模式、同一 10/22 報價收據及 10/21 特徵，不改 checkpoint、
價格／缺值語意、TP/SL、持股或成交證據。所有比較均在 canonical-path readonly
source release 內執行；HTTP 被拒絕，報表和私人模擬產物仍完整建置。

#### 保留未採用的結果

[Candle 診斷四對 ABBA](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round3/coherent/pipeline_candle_abba.json)
核對六個真實模型的完整 forward，只有明確列舉的未使用診斷 tensor 可不配置；
其餘 tensor bytes、完整訊號與解釋一致。但全部發布中位數為
**3,507.996 → 3,808.021 ms**，完整產物為 **4,354.923 → 4,779.702 ms**。
本組未證明整段改善，已撤回即時路徑的候選，不部署。首次私人 panel-cache
來源建置的六模式合計 **95,278.909 ms** 亦保留，不能冒充暖快取或 OS 全冷。

首個比較曾遇到正式 source writer，讀租約拒絕，沒有停止 writer 或忽略鎖。
後續離線比較使用原有 canonical shared read lease 固定全程來源；它可能延後
writer，所以不能用這組比較宣稱真實開盤的來源更新／鎖等待已消失。

#### 精簡即時契約建置，不精簡驗證

`checkpoint_contract.build_inference_checkpoint_manifest` 沿用同一 canonical builder，
schema 4 的即時驗證只建會消費的 `data_schema`、`model` 與其歷史相容候選。
每次仍重建當下模型／特徵／有序標的／環境語意，沒有快取「已通過」決定。
schema 1–3、manifestless、未知格式仍走原完整 builder；未知格式仍被 validator
拒絕。部分 expected manifest 明確拒絕訓練續跑／產物驗收，也不得套到舊 schema。
完整 checkpoint 儲存、指紋與訓練 ABI 不變。

新欄位 `checkpoint_contract_build_ms`、`checkpoint_contract_validate_ms` 同時寫入
訊號摘要及正式開盤 latency journal。兩者是準備階段的子 span：未預取時位於
pre-quote，有效平行預取後則位於 pre-inference，不重複加入總耗時；缺欄位維持
未知，不填零。共用 validator 也修正相容候選缺 key 時的
`None == None` 誤命中，避免不完整資料變成 KeyError。

[首輪四對完整比較](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/pipeline_contract_entry_abba.json)
另接獨立私人 canonical 模擬引擎，含原資格、整張、費用、價格邊界、append 及
持久化；一張報價簿為明確合成測試資料。部分目標仍未全成交，不能把 registration
或最後一筆模擬事件當成全目標成交。

| 首輪中位數（ms） | 完整契約 control | model-scope candidate |
| --- | ---: | ---: |
| 六模式契約建置合計 | 265.696 | 95.174 |
| 六模式全部發布 | 3,962.268 | 4,008.574 |
| 最後私人註冊／持久化完成 | 5,652.861 | 5,587.784 |
| 完整產物完成 | 6,522.371 | 6,837.709 |

契約建置減少約 64.2%，但這組整段中位數尚無穩定改善；保留全部樣本，不把
階段收益推成端到端加速。首次私人 panel-cache 使用 **92,131.322 ms**，第二
完整輪 **3,636.275 ms**。模擬註冊約 345–453 ms／模式，1 億模式約 1,183 ms；
這是待剖析成本，不是外部 API 延遲，也不能刪訊號、fsync 或成交限制來達標。

#### 更接近正式排程的測速邊界

首輪仍是序列準備的對照，正式 Discord 已有平行唯讀預取、序列模型 owner。
測速新增 `--parallel-inputs`，讓同一比較兩側都使用正式排程形狀；新增的
`--compare-model-contract` 則只改契約建置範圍。兩者均保留每次完整權重、決策、
解釋 SHA 與 rich-artifact completion 核對。

同時修正一個測速誤差：預取存在不代表被採用。過期或 key 不符時正式邏輯會
回退，發布 offset 現在依結果中的 `quote_prefetched_outside_model_lock` 選擇
真正 origin，不會沿用較早預取時點而少算 GPU 排隊。測例涵蓋採用、回退、
舊欄位未知及缺失 origin；沒有改正式 freshness gate。

[正式平行形狀的六對 ABBA](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/parallel/pipeline_contract_entry_abba.json)
兩側皆平行唯讀預取、序列 GPU、獨立模擬引擎，72／72 次實際採用預取，六模型
current／compatible 指紋與完整訊號核對通過。全部發布 **5,971.968 → 5,882.061 ms**，
私人註冊完成 **7,017.521 → 7,072.208 ms**，完整產物 **8,146.843 → 8,198.672 ms**；
契約建置合計 **259.240 → 111.803 ms**。發布五對較快、一對較慢，但整鏈仍未
證明數量級收益，也不能把平行 span 加總當作 wall time。首次私人來源建置
**87,064.295 ms**、第二完整輪 **3,533.266 ms** 均保留。

這組不能直接與不同時點的序列組相減當作平行化效果，但足以顯示剩餘本機成本
不只 checkpoint。下一個獨立候選僅協調預取的 CPU 檔案驗證／組裝；來源呼叫、
quorum 等待、同請求 future join、各 provider pool 全部維持原本並行。其完整
交錯比較先通過才考慮採用，不新增全域 rate limit，也不把來源失敗的等待放進
CPU slot。CPU 準備排隊另列 `quote_preparation_queue_ms`，不重複加總。

#### 真實開盤與原生服務，不用離線結果代替

[19:22 原生稽核的精簡 SHA 收據](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/native_summary.json)
指向保留的 63,640,701-byte 完整稽核。官方當日軸為 **2,309 檔**；來源驗證事件
於 1 秒只有 **3** 個真正開盤價，11 秒 **434** 個，30 秒 **1,256** 個，同時點
並無已證實的無首筆資料，未知不得任意轉成昨收或成交。這不是回填當時清冊，
也不是 durable quorum 或可成交報價的時間。

六模式實際首個發布 **09:04:31.992519**，全數發布 **09:04:47.162969**；原生
registration 投影最終 **09:15:59.412730**，全目標成交 SLO 仍 incomplete。
1 億模式 ready-to-ledger 約 672.498 秒，其中 source recovery 約 670.278 秒；
Factorized 該次模型推論約 8.018 秒、原生模式排隊約 10.436 秒。這些都不能用
下午的暖推論中位數覆蓋。仍需逐一分離來源／模型冷載入／排程／執行回補成本。

當時 Gateway／core ready，盤後 artifact maintenance **6／6 ready**，引擎休市
health waiting，GPU warmup 及 postclose resident cache pending。這些是獨立狀態；
不把磁碟產物 ready 改稱 GPU 已 resident，也不把 waiting 當作服務中斷。
既有 `stockagent-tw-opening-audit.timer` 已啟用，下一次為 **10/23 08:55**，沿用
原生 collector／observer 紀錄交易日首筆、全量與執行 clocks，不另造平行下載器。

最後整合相關回歸集合 **1,079 項**通過（52.98 秒），不代表全庫測試。擴充日期測例初版的
fixture 缺欄位／誤用真 manifest 失敗均保留，修正測例後重跑完整相關集合，
正式 feature-source gate 不變。平行測速首個 source freeze 也曾因其他工作樹
變動拒絕；保留未發布 source 與失敗 run，重試固定版本後才測，沒有清除 admission。

#### CPU 準備協調：保留中位數改善，也保留長尾

[六對完整比較](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/cpu_preparation/retry_after_source/pipeline_cpu_preparation_abba.json)
沿用固定 source `20261022T113630Z-f87c68b5bc5d`、同一報價收據、六個原模型與
獨立私人模擬引擎。來源 writer 完成前的 **20,405.067 ms** 等待另存，不混入
timed samples；期間沒有繞過鎖。全部 12 輪的完整訊號／解釋 SHA 相同、快取
均已暖，七個正式 owner 未變，沒有外部請求或正式帳本寫入。

| 中位數（ms） | 無 CPU 準備額度 control | 兩個 CPU 準備額度 candidate |
| --- | ---: | ---: |
| 六模式全部發布 | 7,110.731 | 5,889.434 |
| 最後私人註冊／持久化完成 | 9,153.803 | 8,720.279 |
| 完整產物完成 | 10,655.860 | 10,539.739 |

發布中位數降低約 17.2%，六對中四對改善、兩對退步；但最大發布時間反而由
**22,483.560 → 42,462.558 ms**。兩側各只有 31／36 次預取符合原有 5 秒
freshness gate；回退保持完整取價與驗證，不延長 TTL 來掩蓋。candidate 慢輪
MGDA 推論 **10,223.132 ms**，V8 推論後整理 **18,435.511 ms**，無任何外部
行情請求，因此不能把長尾歸因給 API。此候選尚未據此部署，先做函式級
cProfile、主執行緒／程序 CPU、換頁、context switch、系統 PSI 與 GC 診斷。
剖析會增加開銷，絕對耗時不得與上述未剖析樣本直接相減。

本組首次私人 panel-cache 建置 **99,050.886 ms**、第二完整輪 **2,554.103 ms**
同樣保留；不是 OS 全冷證明，也不是開盤即時來源或全目標成交驗收。

[後續四對剖析](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/cpu_preparation/profile_after_source/pipeline_cpu_preparation_abba.json)
與[資源診斷](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/cpu_preparation/profile_after_source/resource_diagnostics.json)
保留原訊號 SHA；剖析版發布中位數為 **7,125.819 → 7,366.390 ms**，完整產物
**9,173.778 → 10,256.065 ms**，未重現 42 秒 timed sample，但也未確認穩定收益。
因此**撤回兩個 CPU 準備額度的正式預設，維持原本無上限並行**；僅由離線
`--compare-prefetch-preparation` 注入實驗 guard，來源／quorum 仍不占用它。
`quote_preparation_queue_ms` 保留為實測欄位，舊欄位缺失仍未知。

診斷確實觀察到資源等待與一次約 **1,129.528 ms** 的 GC；但首次使用、暖機
timed rounds 與重疊 span 必須分開，不能用系統 PSI 的整機計數、跨執行緒
cProfile cumulative time 或後一輪 GC 倒推已證實原 42 秒的唯一根因。該長尾
仍保留為未閉合問題，沒有改標正常。契約精簡則保留完整相容驗證與既有
六模型 parity，不把這次不穩定的 CPU 排程結果一起上線。

共用測速摘要另增加全部／暖快取樣本的 min、max，以及逐模型預取採用、
回退、未知數；`fully_warm` 只說明四種應用快取，不再讓讀者把它當成全部
預取通過。所有樣本仍保留，不計算沒有足夠樣本支持的 P99 保證。

#### 上線流程修正與 20:14 原生獨立讀回

先前數次更新在重啟前被維護控制鎖或官方 source writer 擋下，失敗收據均保留，
沒有搶鎖、停掉 producer、忽略 admission 或重啟迴圈。另有一次只讀 plan 的
receipt 路徑誤植，亦未執行重啟；它不是正式服務故障。

canonical `reload_tw_day_trade_services_if_flat.py` 增加選用的
`--control-wait-seconds`（0–120，預設仍為 0），使用原 OS lease 有界等待，
並記錄 `control_wait_ms`。官方來源讀租約移到三份帳本初始雜湊之前，來源忙碌
就先拒絕，免做約 **858.72 MB** 的無效掃描；完整前綴驗證、fresh ACK、
code／owner fences、唯讀 namespace 與重啟後獨立讀回全部保留。維持
**control → source** 的鎖順序，不在外層持有 source lease 等待控制鎖。
231 項操作／維護測試通過；與原 848 項一起整合重跑為 **1,079 passed**。
新增測例也證明 source 更新中時，不讀帳本前綴、不安裝 drop-in、不重啟。

[原生更新收據](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/discord_final_apply_wait_accepted.json)
於 **20:14:21** 完成一次 Discord-only code admission，當次獲鎖 **8.021 ms**；
這不是宣稱先前忙碌的鎖已被量成零等待或已消除所有 contention。
[獨立讀回](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/native_final_readback.json)
於 **20:14:37** 再驗證：

- source release：`20261022T121126Z-677d4f188859`，source SHA
  `7391c91dc4027dac7cbb6f165ea3d271786e4d8e11b00a5d11d9c560f5a84980`。
- 原生 Python PID `1042034`，實際 namespace **1,610 檔**逐檔與唯讀掛載核對通過。
  admitted code SHA `f5d0f67a92f0faffa044d95580618d110dc384a0b229d6941c295a7e576fd410`；
  Gateway connected、core ready，六模式訊號／engine ACK 同一 revision **63,696**。
- 交易引擎仍為 `05aa07b6342f481ba1cbb20c68d28b96`；受保護 owner 全部未變。
  實際空倉、待委託均為 **0**。events／orders／fills 合計 **858,722,532 bytes**
  前綴 SHA 與長度未變；沒有券商下單或操作員帳本寫入。
- 盤後 artifact maintenance ready；GPU startup warmup、postclose resident cache
  仍 pending。驗收範圍明確為 loaded code／Gateway／exact ACK，**不是 GPU
  resident、真實 09:00 來源覆蓋或全目標成交 SLO**。

未解事項仍是原生開盤的來源覆蓋、首次私人建置約 99 秒、實際模型冷載入／排隊、
執行 source recovery 約 670 秒與未閉合的 42 秒長尾；不能宣稱已達一秒或只剩
外部 API 延遲。工程任務保持 active，不以本輪 code admission 結束整個目標。

既有每日獨立 observer 已核對為啟用：08:55、09:00:11、09:00:31、09:01、
10:31、13:40、15:10（Asia/Taipei）。下一次為 **2026-10-23 08:55**，讀原生
來源／訊號／執行日誌並保留 deadline assessment；不發行情請求、不下單，也
不把補跑當成原來 deadline 成功。下一交易日仍須以該原生證據驗收實際改善。

## 全步驟重新測速與重疊來源讀取合併（2026-10-22 夜間）

本輪將官方範圍／來源、輸入準備、冷暖模型、排隊、不可變發布、獨立紙上執行、
HTTP、SSE 與真實瀏覽器分開驗收。官方當日清冊為 **2,309 檔**；清冊、
真開盤、官方無首筆證據、模型參考價、可執行報價與成交仍是不同欄位。
不刪分鐘、不縮分母、不放寬 15 秒無首筆證據時效、TP／SL 進場區間或整張規則。
以下夜間實測均不能冒充 09:00 原生行情或券商成交。

### 全鏈基線與尚未解決的瓶頸

| 步驟／邊界 | 本輪讀回／實測 | 解讀 |
| --- | --- | --- |
| 今日原生開盤來源 | 第 11 秒 434 真開盤／1,875 未知；第 30 秒 1,256／1,053 | 分母 2,309；未知不是零成交量，不能直接沿用昨收。 |
| 今日原生訊號發布 | 首模式 271,992.519 ms；六模式全部 287,162.969 ms | 保留原始失敗；不能把夜間回放當成改善後的今日開盤。 |
| 今日原生紙上執行 | 首／末帳本登記 283,924.753／959,412.730 ms | 100m ready→登記 672,497.813 ms，其中來源補查 670,278.018 ms；未取得六模式全目標成交完成證據。 |
| 空私人 panel cache 六模式首次準備＋模型 | 91,356.389 ms；第二完整暖機 3,424.505 ms | 是新私人 cache，不是作業系統全冷或正式服務重開機的測速。首次 Factorized 46,740.210 ms，前置處理占 40,733.277 ms。 |
| 既有正式版本六模式暖輸入→發布 | 中位 6,089.746 ms；完整報表＋獨立私人執行 7,640.248 ms | 六次、36／36 預取通過；所有模型與完整產物保留。 |
| 夜間真實多源查詢→quorum | 2,146.365 ms | FinMind 首個有效輸入 1,896.233 ms；不是 09:00 成績。 |
| 夜間所有 provider＋持久證據完成 | 8,547.211 ms；獨立讀回 8,691.893 ms | 2,275 真開盤＋34 官方無首筆＝2,309 已解析，不是 2,309 個真開盤價。 |
| 一般本機 API | 暖中位多為 2–5 ms | 跨日期 signals 首次觀察仍 24,196.952 ms；全分鐘歷史首次 2,331.996 ms／暖中位 195.412 ms，沒有清 server／OS cache。 |
| 隔離 receipt 更新→本機 SSE consumer | 中位 4.110 ms、最大 4.418 ms，5 次 | 真 inotify／SSE，但不是交易訊號到瀏覽器畫面的全鏈。 |
| 真實瀏覽器初始資料就緒 | 本機 1,653.100 ms／公網 8,735.800 ms | 新 temporary tab、瀏覽器 HTTP cache 關閉；server／OS cache 未控制。 |
| 全分鐘歷史載入／畫面完成 | 本機 1,314.900 ms／公網 4,256.400 ms | **477,628 點、9 series**，Canvas 重建一致、無畫面錯誤、SSE 持續；使用 2D profile，不宣稱 WebGL／GPU 驗收。 |

原生首輪各模式皆為 `TWOpeningInputCoveragePending`；不是模型已完成卻只差畫面。
成功輪仍有 4.15–20.83 秒模式 dispatch queue、Factorized 約 8.02 秒推論及
前置重複掃描。這些 spans 有包含與重疊關係，不能逐欄相加為總延遲。
夜間 FinMind headers／body／JSON／全市場驗證為
391.628／1,410.592／18.430／20.576 ms；MIS 29 chunks、4 workers、
8,481.314 ms、零請求失敗。Fugle 兩市場均為 permission 403，仍列不可用，
不靠高併發繞過拒絕，也不再次採用先前全失敗的 16 路設定。
HTTP benchmark 中受保護路由的 403 保留為權限拒絕，不當成正常業務回應。

證據：
[原生 audit](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/native_audit.json)、
[完整私人 pipeline](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/pipeline_full_current.json)、
[低擾動階段診斷](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/pipeline_stage_diagnostics.json)、
[HTTP／SSE](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/dashboard_http.json)、
[夜間來源](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-22/postclose_214018429545.json)、
[本機瀏覽器](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/browser_navigation_retry/browser_local.json)、
[公網瀏覽器](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/browser_navigation_retry/browser_public.json)。

### 本輪實作與驗證方法

- canonical source read guard 只在**同一 OS shared lease 的重疊讀者**之間，
  single-flight 共用不可變股票 member generation。每個 reader 仍持有自己的
  真 OS lease，最後 reader 離開就捨棄結果；不是 TTL／目錄 mtime 快取。
  外層操作員／benchmark lease 預設不延長這個重用週期。
- 外部 feature、corporate-action 與 sentinel 仍逐次重新檢查；未受該鎖保護的
  來源不能搭便車。完整來源雜湊、模型輸出、權重、價格、解釋與 rich artifacts
  皆與 control 核對，沒有建立第二套來源或模型路徑。
- `benchmark_opening_signal_pipeline.py --compare-source-scans` 保留原 scanner
  為 control，以六對 ABBA、同 source release／receipt／完整六模式作整鏈對照。
  actual scanner 計時只加 observer，不改讀取內容；並行 spans 不加總作牆鐘。
- browser benchmark 修正 WWW→apex／HTTPS 轉址後仍在舊 execution context
  等待的競態；等待真正主文件後才驗證畫面，navigation/readiness 失敗另留收據。
  原失敗 run 不刪除，重測兩個網址都通過，沒有放寬 CSP 或修改頁面來遷就測試。
- **1,151 項 Python 語意回歸通過（66.78 秒）**，包含來源鎖、多讀者、原異常傳遞、
  lease 結束不得重用、外部原地修改即失效、source epoch 更換、checkpoint ABI、
  Discord、報價、執行、發布與 code admission；不是全 repository 全測試宣稱。

第一輪完整對照發布中位 **9,005.546→6,920.521 ms**、完整產物
**11,506.307→9,066.863 ms**；最大發布仍有 **33,986.943／12,469.585 ms**。
該輪有同時進行的回歸工作與既有背景負載，全部樣本保留；不可只挑最快數字。
來源大掃描每輪確實由 **12 次減為 6 次**，其他 sentinel 檢查未減。
第二輪使用同份凍結版本，且不另啟回歸測試，再驗證整鏈，結果另保留於
[獨立重測](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/scan_recheck/pipeline_scan_abba.json)。

獨立重測仍是每組六次、全部 warm，兩組皆 31／36 預取通過；發布中位
**7,868.860→6,894.584 ms（少 12.38%）**，完整報表＋獨立私人執行
**10,131.046→8,509.686 ms（少 16.00%）**。但最大發布為
**9,665.599→10,133.815 ms**、最大完整產物為
**11,895.663→12,892.537 ms**，所以只接受可重現的中位改善與減少重複工作，
不宣稱長尾、P99 或一秒保證。背景 producer／備份／其他工程負載未被停掉，
這不是隔離硬體的最大吞吐實驗。兩輪各自的六模型完整契約 SHA 相同；
兩次都驗證七個正式 owner 不變、零正式寫入／零券商下單。

另以同 source release 的新程序、**保留既有持久 cache** 讀取全部六模式輸入，
不請行情、不初始化 CUDA、不 forward、不發布訊號：首次 **10,137.269 ms**，
後續完整重驗 **1,174.775／904.920 ms**。Factorized／MGDA 首次共用的完整
member 驗證約 **7,411／7,373 ms**（重疊等待，不能加成 14 秒），並非重建
全部歷史。這把「私人空 cache 91 秒」與「現存持久 cache 的新程序 10 秒」
明確分開；兩者均不是物理冷開機或 GPU 冷載入驗收。
見[完整輸入收據](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/persistent_input_preparation/input_preparation.json)。

下一步優先順序：先確認重疊掃描的整鏈改善可重現，再經 flat／idle、來源鎖、
exact ACK 與唯讀 namespace admission 上線；接著分解持久冷恢復、前置轉換／排隊、
Factorized 5.20 GB 邏輯輸入的原樣重建與帳本來源補查。已否決的兩個 CPU slot 與
刪診斷 tensor 方案不重推。獨立 observer 延用既有每日排程，下一交易日原生來源、
六模式發布與**全目標紙上成交**必須各自驗收，仍未達一秒，亦未證明只剩 API 延遲。

### 22:08 採用與 22:09 獨立讀回

先前只讀 plan 的 heartbeat freshness 拒絕、以及更新中 source writer 的
pre-restart 拒絕均保留，沒有鬆開 freshness、搶鎖或改寫來源狀態。
外層等候來源 **131,380.541 ms** 後即釋放探測用 shared lease，再按
**control→source** 順序重新做完整 admission；實際 control 等待 **12.185 ms**。
只重啟 Discord 一次，沒有 restart loop。

[canonical apply](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/discord_apply_after_source.json)
於 **22:08:56** 接受 code-only admission；
[獨立讀回](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/native_final_readback.json)
於 **22:09:56** 確認：

- source release `20261022T134638Z-e8906f8b30bf`，source SHA
  `53537d648e5a61d45f9cfe72fd1b976efb7ea21a98ef3415e9a34bf42b526a8f`；
  Python PID **1347079** 實際 namespace **1,614 檔**逐檔與唯讀掛載驗證通過。
- admitted code SHA
  `11c8e37f173e11a1dadb634dc011413bf65ee979e5d1b13743aefd640d48dd83`；
  Gateway connected、core ready，exact ACK revision **63,808**、lag **0**。
- 引擎仍為 `05aa07b6342f481ba1cbb20c68d28b96`，受保護六個服務 owner 不變；
  空倉、待委託各 **0**。三份帳本共 **858,722,532 bytes**，長度與完整前綴 SHA
  均與更新前相同，零操作員帳本寫入／零券商下單。
- startup GPU warmup、postclose resident cache 仍 **pending**；盤後 maintenance
  為 **waiting_source**，原生原因 `tw_public_refresh_in_progress`、failed count **0**。
  這是對當下來源工作的等候，不改寫成全部 ready；code acceptance 不等於 GPU、
  全資料就緒或下一交易日 09:00 SLO。

Browser benchmark 另新增 DNS／connect／TLS／request／response、H2／H3、
靜態資源 bytes 與各資源瀑布原始時鐘。重用連線或跨來源重導的隱藏階段保留原值，
**不把 0 認定為真實零網路成本**。它只是觀測工具，沒有放寬網站 CSP。
[新增計時公網讀回](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/browser_network_phases/browser_public.json)
為初始 **7,845.200 ms**、全分鐘歷史 **3,315.200 ms**；同輪
[本機讀回](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/browser_network_phases/browser_local.json)
為 **1,848.700／6,999.200 ms**。兩者仍有 477,628 點、9 series、SSE、
Canvas 一致與零畫面錯誤；但本機全歷史這次更慢，不能只報第一次的 1.31 秒。
不是受控 UI 前後 ABBA，不據此宣稱頁面快了某個比例。
公網該次主文件 responseStart **3,813.800 ms**、responseEnd **3,824 ms**、
DOMContentLoaded **5,241.200 ms**，證明傳輸／載入仍需另查，不能全歸於繪圖。

[小型證據索引](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/summary.json)
保留各完整收據的長度／SHA、三組六模型契約相同、兩輪全部樣本分布、cold/warm
邊界與實際採用狀態。每日 observer 仍 enabled／active，下一次為
**2026-10-23 08:55**。工程任務維持 active：長尾、首次完整恢復、HTTP 歷史快取
失效成本、來源補查及真實開盤全目標完成仍未閉合，未宣稱優化已到理論極限。

## 09:00→全目標紙上成交→畫面的完整規劃（2026-10-22 22:40）

本節先固定整體方案，再實作／測速。沿用同一工程任務與 canonical collector、
inference、executor、唯讀 gateway、共用瀏覽器測速，不另開行情登入或交易路徑。
正式帳本、既有成交及模型語意不在這次效能修改的寫入範圍。

### 清冊、時鐘與驗收

| 邊界 | 真值與關聯 | 要量的時間／失敗 |
| --- | --- | --- |
| 09:00 到點／scheduler | 當日官方 session、原生 opening attempt | 第一個喚醒、input job dispatch、重試；不能用回放時間代替實際時鐘。 |
| 每個 provider／每檔價格 | acquisition journal、官方 dated universe、原始 receipt | request queue、headers/body/decode、首筆／全量有效價；逐檔保留錯日、缺資料、試撮、失敗與 permission。 |
| 模型輸入→訊號發布 | market＋session＋signal_id＋immutable publication | 完整 input prep、模型鎖／forward、格式化與發布；冷持久恢復、暖機、暖執行分列。 |
| 訊號→paper executor | 同 signal_id、engine_run_id、state_revision | discovery、source recovery、quote、計算、fsync；首筆成交與**全部原目標成交**分列。 |
| commit→網站資料 | 同 engine run／revision、SSE、status/detail response | 通知、cache rebuild、HTTP headers/body/parse；拒絕、stale response 與失敗不能記為成功。 |
| 網站資料→顯示 | 同 session／mode／signal_id／revision 的實際已套用資料 | DOM 更新後前景 double-rAF 作 paint opportunity 代理；signal/detail 分開，非實體像素掃描證明。 |

所有程序內 duration 使用 monotonic clock。跨程序保留原 wall clock 與 clock basis；
遠端瀏覽器時鐘未校準，不把 client/server wall 差值稱精確網路延遲。
瀏覽器休眠、隱藏、跨篩選／新 revision 淘汰、尚未載入的分頁都不能算已顯示。
僅首屏實際資料可證明首屏顯示；完整訊號表分頁未下載，不冒充全表呈現。
紙上無委託、未定量、TP／SL 拒絕、部分成交、回放、恢復後才發現成交均另列。

價格分析覆蓋完整官方軸及 1／5／10／11／20／30 秒等 deadline，不只分析收盤剩餘
40 檔；後來成功也保留早先未達標。首筆真成交固定 opening；fresh 官方無首筆證據
最多 15 秒，只有 model reference 可沿用上次真成交；網路錯誤絕不推論零成交或下市。
逐來源範圍只有 hash 而無標的清單時，僅列來源／job 層錯誤，不能猜哪一檔請求失敗。
官方觀測時間、歷史訊號時鐘及後驗比較範圍都明示，不偷換分母。

### 依證據排序的實作／優化

1. **先補觀測缺口與失敗清冊**：擴充既有 symbol audit，以一次串流讀 journal
   重建逐檔 deadline 狀態、首次有效時間、後續恢復、provider rejection／job 失敗。
   既有 bulk/legacy observation reader、canonical masks 與 15 秒 TTL 都保留。
   原生 daily observer 重用結果／摘要；不在每個 consumer 重複掃全歷史。
2. **接通 commit→真正套用畫面**：擴充既有 browser-local performance records，
   只允許 signal/session/revision 和耗時，不蒐集價格／帳密、不新增公開寫入 API。
   browser benchmark 保存完整關聯，而非只數 SSE messages；過時與背景樣本拒收。
3. **逐一剖軟體成本**：先查 100m 670 秒 source recovery、六模式 dispatch queue、
   持久 member 驗證與 status/history cache 重建；只有已證明冗餘／可安全 overlap 的
   工作才移除或並行。來源配額、permission 和首筆尚未發生屬外部邊界，不能以
   更短 timeout、假價格、跳過驗證或提高併發冒充優化。
4. **同輸入驗證後採用**：control/candidate 使用同 source release、六模式輸出／
   價格／費用／股數／完整 artifacts oracle；ABBA 保留每輪及負面長尾。共用契約
   回歸與真實 browser readback 依變更範圍執行。只讀 observer／靜態 UI 可獨立上線；
   trading owner 的版本切換仍需 canonical flat/idle admission、exact ACK 及 rollback。
5. **真開盤再驗收**：既有 timer 於 10/23 08:55 固定 scope，開盤採原生 clock，
   每模式 publication/full-target commit 和前景瀏覽器顯示各自驗收。夜間只能做
   真網路盤後測速及明示 private replay；不能承諾未實測的一秒、零錯誤或理論極限。

本輪開始時（22:34）Gateway/core ready、引擎與公開 gateway owner 未更動；盤後
maintenance 已由先前 waiting_source 轉為 ready（22:26:09），不是人工改寫健康狀態。
這仍不等於 09:00 覆蓋、GPU resident readiness 或全目標成交成功。

### 本輪實作、測速與獨立讀回（2026-10-22 23:34）

計畫先於本節實作固定。沿用工程任務
`tw-opening-latest-standard-latency-20261022`，完整證據在
[`end_to_end_round6`](../artifacts/operations/tw_opening_latest_standard_20261022/end_to_end_round6/)。
本輪不修改正式帳本、訊號／成交時鐘、模型權重、TP／SL 或券商委託。

#### 原生開盤：不是 HTTP 成功就有有效價格

新逐檔分析一次固定讀取當日 journal 的 **221,825,705 bytes／65,460 行**，
完整前綴 SHA `36a3a9e46e359ca76eb1e55383b539de8a5577ff24fde8587fbd0753bcbcfb43`；
inode 未換、無損毀行。官方交易軸 **2,309 檔**，fingerprint
`a3c1fcd3e809ac0a7ac3eefd1026a76994307c1c25c9fdb323bcc2561d888d4f`，
觀測時間 **08:45:26.577294**；沒有以後來成功的標的縮小早先分母。

| 09:00 後 | 有效首筆開盤價 | 尚未證實有效／無首筆 |
| --- | ---: | ---: |
| 1 秒 | 3 | 2,306 |
| 5 秒 | 57 | 2,252 |
| 10／11 秒 | 434 | 1,875 |
| 20 秒 | 1,070 | 1,239 |
| 30 秒 | 1,256 | 1,053 |

第 11 秒的 **1,874 檔**共同原因是永豐 shared row 仍為
`stale_or_preopen_time`、FinMind `rejected:other_session_rows`、MIS
`no_source_observation`；另外 **1 檔**的 MIS 也是 stale/preopen。
這是「沒有當下有效證據」，不是判定股票無成交、停牌或下市。
FinMind HTTP 成功、其他日期價格及昨天成交量不能冒充今天首筆成交。
無完整請求軸的 provider job 失敗僅留 job 層原因，不猜分配到每個 symbol。

後來完整快照是 **2,269 真開盤價＋40 fresh 官方無首筆證據**、未知 **0**；
40 檔的代碼、正式名稱與交易所均留在
[完整逐檔收據](../artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-22/first_postclose_symbols.json)。
這不能倒推第 11 秒完整，也不把 40 檔排除或捏造成可成交量。
另有 **1,194 筆**舊版 absence events 缺 proof clock，保守維持未知。

六模式原生最後發布仍是 **09:04:47.162969**；第一個／最後一個 executor
登錄是 **09:04:43.924753／09:15:59.412730**。100m 其中約
**670,278 ms** 落在 missed-opening 歷史價格恢復；這屬錯過開盤後的
09:01 反事實重建，不是正常即時進場的延遲，也不能拿登錄當全目標完成。
同訊號、因果全目標成交 SLO 仍為 **0／6 已驗證達標**，不是宣稱完全沒有成交。
原生階段與 provider job 分布仍見前節 `all_stages_round5/summary.json`。

#### 已完成的觀測與一致性修正

- `audit_tw_opening_symbol_status.py` 新增完整逐檔 deadline timeline，先完整驗證
  bulk／legacy event 才採用、保留未完成尾端／錯日／freshness／provider 原因，
  首筆真成交不被後續 stale row 覆蓋。projected decoder 與 full decoder 的
  **2 組 ABBA、8 樣本整份輸出 hash 相同**，中位數 **10,846.951→9,726.694 ms**。
  約 **10.3%** 僅是盤後逐檔鑑識加速，沒有搬到 09:00 熱路徑冒充開盤提速。
- `audit_tw_opening_latency.py` 在當日 13:35 後由原生 observer 保存一次
  `first_postclose_symbols.json`，後續重用原日期／原 SHA；壞收據顯式失敗而不覆寫。
  這次正式收據於 **23:13:41.733893** 保存，來源 clock **13:29:59**，
  收據 SHA `696329b4873cbb231daecd9558567f34dfbe6d9f1471cc89761b4fc3cb8bef24`。
- backend／TW 與隔日沖共用前端加入 opaque `public_signal_key`、engine generation
  和 state revision。共用 observer 只接納目前篩選、實際套用、前景 double-rAF
  的資料；revision 被淘汰、頁面隱藏、重複回應均不算已顯示。
  模式卡資料與明細 identity 是不同證據；沒有把頁面 identity 當成交 commit。
- `benchmark_dashboard_browser.mjs` 保存每筆 `.updates.jsonl`、HTTP／parse／render
  原始 spans，區分 initial 與真正 revision change；summary 有上限及 drop count，
  不是靜默丟事件。缺 capability／backend identity／可見關聯即驗收失敗。
  新 canonical `benchmark_dashboard_browser.py` 管理唯一 CDP 瀏覽器、限定輸出不覆寫、
  timeout／失敗 sidecar 與確實關閉；CPU-2D 明示，不冒充 GPU/WebGL 測速。
  watch 可在尚無訊號時啟動，不強制載全歷史、不接行情／下單／公開寫入 API。
- benchmark 現在明確載入 canonical MGDA sparse runtime profile 並驗 hash／鍵名；
  profile 缺漏提早失敗，不再誤把 benchmark 的 ABI 缺漏當正式 checkpoint 壞掉。

#### 真正發現並修掉的公網舊資源問題

第一次公網 browser 只有 **0 筆**可見關聯（本機有 2 筆）。查到 gateway 的
`project_html` 把每個新版共用 JS URL 都重寫成固定 `access-v2`，導致公網拿到舊版
immutable cached core：encoded **12,384 bytes**，本機新版 **13,705 bytes**。
因此不能只看 HTML/App 已更新就宣稱整套前端生效。

已改為共用 core **實際內容 SHA** 的 URL；HTML 靜態快取也依賴 core 檔案身份，
只換 core、不換 HTML（包括原子替換／相同大小與恢復 mtime）仍可失效。
重用既有 static cache／ETag，不增加每請求整檔 hashing。預設不再重寫宣告版號，
版本字元嚴格驗證，public／member／owner 過濾與私有欄位遮蔽不變。
正式讀回實際 URL 為
`/dashboard-core.js?v=core-464d13fc3393cef495135299379d460bea5abf304ce2b54e61f127d779a0a595`。

只讀 gateway 本輪共重載 **2 次**，第二次載入上述根因修正：
`opening-display-gateway-rollout-20261022T151505-8a7ff83e`、
`opening-display-content-hash-rol-20261022T152150-91eb66b1` 均 succeeded。
Gateway 現 PID **1535963**、Invocation `6e9e706ab6ef4aa2805ea64603f93a44`。
Discord、當沖／隔日沖、Shioaji top200／TAIFEX bidask／dashboard、cloudflared
**7 個受保護 owner 的 MainPID／InvocationID 均與更新前相同**；沒重啟交易引擎。

#### 同輸入六模式 ABBA：保留完整工作，不把隔離測試當真開盤

[無 profiler 的 8 樣本收據](../artifacts/operations/tw_opening_latest_standard_20261022/end_to_end_round6/pipeline_unprofiled_abba.json)
來自 run `opening-latest-unprofiled-retry-20261022T151704-336ce033`。
固定同一來源／feature date／六模型、2 組平衡 ABBA、每 variant **4 輪**；
訊號、權重、價格／費用／股數及完整 artifact oracle 相同。
沒有網路請求／正式寫入。進場是私有 **synthetic one-lot order book fixture**，
不是今天全部原目標、交易所成交或券商回報。

| 完整工作／中位數 | 各模式重複掃來源 | 同 generation 共用掃描 | 改善 |
| --- | ---: | ---: | ---: |
| 六模式訊號全部發布 | 5,578.349 ms | 5,024.320 ms | 9.93% |
| 含隔離進場與完整 artifacts | 7,971.239 ms | 6,962.215 ms | 12.66% |
| 發布 min～max | 5,130.403～6,457.680 | 4,072.444～5,247.744 | 保留全分布 |
| 完整工作 min～max | 7,840.857～9,719.425 | 5,641.796～7,406.912 | 保留全分布 |

這是重測先前已實作／部署的 shared-generation 掃描，不把它重複計為本輪新改進。
私有 fresh-cache warmup **98,847.935 ms** 另留；8 個計時樣本均 fully warm，
不能把這組稱來源冷啟動／首次恢復。48／48 mode prefetch 都採納，fallback **0**。
第一次重跑被 canonical `TWPublicSourceUpdatePending` admission 擋下，保留 failed
run，等原生 writer 自行完成後才重試，沒有停 writer、搶鎖或混來源 generation。
較早 cProfile 兩輪發布 8.32／8.78 秒、Factorized 曾 2 次 prefetch 過期，也保留；
不能與無 profiler 樣本直接當前後 ABBA。未延長 **5 秒** prefetched mutable quote
TTL，因其中有 bid／ask／volume／no-first 證據，放寬會以正確性換速度。

#### 已上線的瀏覽器與 HTTP 讀回

`opening-display-hashed-public-ac-20261022T152234-2fc6250b` succeeded。
fresh CPU-2D browser、browser HTTP cache disabled；server／OS cache 不受控：

| 範圍 | 首屏 ready | 完整歷史 load | 歷史／繪圖驗收 |
| --- | ---: | ---: | --- |
| 本機 | 1,158.8 ms | 3,054.1 ms | 477,628 點、9 series、Canvas 相同、無錯誤／溢位 |
| 公網 | 6,814.5 ms | 5,336.8 ms | 同完整資料／Canvas／無錯誤；4 筆前景 identity |

這次 **matchedRevisionUpdateSamples=0**：是初始顯示觀測，不是夜間造一筆訊號
當 09:00 更新。公網舊版那輪 17,364 ms 與這輪也不是受控 network ABBA，
不宣稱保證加速倍數。整檔 canonical source 失效的 canary 曾 ready timeout，
失敗證據與持久快取成功路徑各自保留，沒有用後者冒充真正 source-cold 恢復。

最終 HTTP 每 route **first＋3 repeats**，signals／positions 均完整保留首查成本：

| API | 本機 first／中位 ms | 公網 first／中位 ms | 公網 app median ms |
| --- | ---: | ---: | ---: |
| status | 8.101／11.561 | 396.113／220.149 | 1.379 |
| revision | 5.036／27.832 | 302.887／236.057 | 1.373 |
| signals（100 筆，排除零目標） | 423.332／6.149 | 611.534／263.580 | 1.610 |
| positions（100 筆） | 178.420／5.464 | 471.602／226.173 | 1.565 |

本機／apex 公網各 5 routes 無錯誤，但樣本量僅 3，不據此作長期 p99 保證。
最初用 WWW HTTP probe 不跟 redirect，遇 **308 共 20 次**而失敗；這是正常
WWW→apex 導向，不是網站中斷。保留失敗後，以 apex 重測 succeeded；browser
本來會跟導向，其整段時間也包含在公網讀回，不把該成本刪去。

#### 測試、下一次原生證據與尚未閉合的成本

最終統一 run `opening-all-stages-final-regress-20261022T152910-7706b926`：
**529 Python tests passed／66.15 秒＋59 Node tests passed**，ruff 與 Node syntax
checks 通過。這是受影響的 audit、public access/cache、完整行情顯示、revision、
重啟／rollover、benchmark 與共用前端契約，不是全庫測試通過。
中途過期的 static token assertion 曾 **1 failed／211 passed**，修正 expected
TW core 版本後獨立 **38 passed**，並納入上述統一回歸；失敗 log 未刪。

既有 persistent opening audit timer **active**，下一次 **10/23 08:55**；
另外已啟動一次性 systemd timer **10/23 08:58:30**，由 canonical agent workflow
執行公網 browser watch **600 秒**，只讀並保留逐筆更新：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_dashboard_browser.py \
  --url https://www.penguin72487.dpdns.org/tw-day-trade/ \
  --watch-ms 600000 \
  --output artifacts/operations/tw_opening_latest_standard_20261022/end_to_end_round6/native_open_20261023_public.json
```

已用 3 秒 smoke 證實可以在 initial signal rows **0** 時 armed，正常保存 launch／
updates sidecars；不是宣稱產生新交易。這個 browser timer 定義在 `/run`，
**不保證 WSL／Windows 實體重啟後仍存在**；persistent 原生 audit 是另一路證據。
下一交易日必須真的執行、當日官方分母固定、同 signal／engine revision 相符、
原目標全數完成且前景顯示才驗收；只有 timer active 不算達標。

23:28 原生 Discord maintenance **ready／6 markets／failed 0**，但 startup GPU
resident warmup **pending**；公開整體仍 **degraded**，沒有改寫正常。
本輪保留的下一步優先序：**09:00 價格新鮮度與 MIS 外部等待→六模式 dispatch／
input preparation→來源版本／持久 member 驗證與可證明重用→model／formatting→
missed-opening 歷史恢復→公網傳輸與真正 revision paint**。
來源 permission／首筆未發生無法由本機消除；但暖路徑仍有約 **4～5 秒**內部發布
成本及 private fresh warmup，不能宣稱只剩外部 API、11 秒／一秒保證或理論極限。
工程任務維持 active，下一次原生觀測與上述剩餘熱路徑尚未完成。

**23:35:27 獨立末次讀回**：
`opening-round6-final-readback-20261022T153523-c620862e` succeeded；逐一比對 7 個
受保護 owner 的實際 MainPID／InvocationID 全相同且 active，兩個觀測 timer active，
最終回歸／apex HTTP run succeeded，public 完整歷史／關聯證據及本節連結存在性
再次驗證。這仍明確保留 `browser_current_native_opening_proof=false`。

## 無首筆沿用最近成交價：新標準重測規劃（2026-10-22 23:45）

使用者重申：盤前備齊前一交易日收盤／最近真實成交價及可事先準備資料；
09:00 後拿到無首筆證據即沿用，不等首筆才推論。本節於修改程式前固定方案，
沿用原工程任務，不把上一輪「真首筆」數當此新標準的輸入就緒率。

### 全流程清冊與口徑

1. **盤前**：官方交易軸／資格／價格界線、已完成資料與 feature panel、normalizer、
   checkpoint／GPU model、layout、來源版本驗證，以及每檔最近有正成交量的完成日
   Close／來源日期。一般為前一交易日；該日零成交則往前找最後真成交，不用
   日曆昨日或試撮價。原生無行情 `price_source=panel, write=False` 暖機驗證這些
   工作；不偷用開盤後快照作盤前準備。準備耗時與開盤耗時分列。
2. **到點→取價**：原生 09:00 gate、request queue／來源回覆／解析／input fsync。
   每檔分為「真首筆」「已證無首筆且前值可用」「無首筆但前值缺漏」「來源未知」。
   前兩者合計才是新標準 model input readiness；無首筆當下直接用盤前前值，
   不因沒有 positive open 繼續等。timeout／其他日資料不是無首筆證據。
3. **輸入→訊號**：同官方 scope／模型、來源觀測與沿用價格／日期／標記完整保存；
   分別記 prepare、模型排隊、forward、format、durable publish。首個開盤推論與
   已跑過開盤資料的 steady 重複輪分列，避免「二次熱快取」冒充盤前準備效益。
4. **訊號→模擬成交**：沿用價只解模型參考；可執行 Bid／Ask／量、原 TP／SL、
   因果時鐘與未成交目標仍照 canonical executor。首筆、部分、全部原目標、
   waiting_source／bracket rejection／清倉時段拒絕分列，不把模型補價當成交。
5. **commit→網站**：重用 opaque signal／engine／revision→SSE／HTTP→實際套用→
   前景 paint opportunity；完整歷史、首屏、分頁與不同 clock basis 照舊分開。
   原生 10/23 audit＋公網 watch 仍保留；夜間重建只稱可追溯 replay。

### 已找到的具體缺口與依序實作

- canonical `_last_completed_trade_reference` 已在 panel 暖機建立／快取；先重用，
  不再做一份平行價格來源。新增盤前準備／實際沿用清冊與測速證據。
- 目前 compact journal 的 v1 observation 不含無首筆 proof clock，timeline 消費端
  也只投影 symbol／state，無法正確驗收沿用價。擴充 lossless 版本與相容讀取，
  保存 proof 自身時鐘及必要刷新；不能拿任意新 broker row 刷新舊 MIS absence。
  舊資料僅於原始 direct MIS observation 有足夠時鐘／狀態證據時解析，否則保留未知。
- 逐檔 audit 明列新標準 resolved count、無首筆及缺前值原因，保留真首筆獨立欄。
  允許從 pinned journal prefix 產生指定 deadline 的私有診斷價格視圖，來源原值／
  時鐘保留、不反寫原 receipt／帳本；只用該 clock 已知的事件，拒絕未來證據。
- benchmark 新增真正無行情盤前準備與第一輪開盤階段；採 receipt／journal 的
  原始觀測時鐘驗證 absence freshness，不再把午後 absence 放到 09:00 合成 clock
  然後錯判 future。所有 fixture／clock 與真實來源等待分列，禁止網路與正式寫入。
- 依新的分階段剖析只消除可證明的重複掃描／配置／序列化；固定輸入 ABBA／完整
  金融輸出 oracle 後才採用。若 source writer 在更新，保留拒絕並等原 owner 完成。

### 重測中新增的阻塞與修復方案（23:58）

原生 `stockagent-tw-public-cold-publish.service` PID 1611024 在沒有修改來源的
packing／publication 階段持有 `tw-public-refresh.lock` 的 WRITE lease；本輪
benchmark admission 與 qualified reload 都被真鎖拒絕。只讀發布需要排除來源
writer，並不需要排除同 generation 的模型 reader。修復順序為：明確在既有
catalog 登錄 TW public 的 shared-read source lease；wrapper 與 canonical child
一致使用該契約，繼承 FD 只驗證既有持鎖而不改變父程序鎖種類；其他資料與真正
writer 仍維持 exclusive。完整 hash／來源前後 fence、唯一冷發布 owner、D commit
及 strict audit 不變。以真正 flock 及 child inheritance 測試驗收 readers 可併行、
writers／未持鎖 FD 仍拒絕。不動執行中的 owner／鎖；下一次原生啟動才載入。

只切換已固定的 Discord code namespace、且明確使用 `--code-admission-only`
時，驗收對象是載入程式、Gateway 與未變更引擎的精確 ACK，不是來源／GPU
warmup。該狹義路徑不應讀取 panel 或等待來源發布；保留 control lease、所有
帳本 prefix／owner／order-disabled 與 ACK gates，明示不做資料 readiness claim。
正常 GPU-ready、engine 或未固定來源的 reload 仍先取得 source read admission。

先做 focused schema／mask／cache／benchmark tests，再整合六模式、paper executor、
public DTO／瀏覽器回歸。部署交易 owner 前仍做 canonical plan→flat/idle admission→
apply→載入版本／ACK／帳本前綴 verify；未通過就不宣稱新版本已生效。
一秒是全鏈目標；資料未到、未成交、背景頁面或未知證據均不以零耗時算成功。

### 新標準實作與重測結果（2026-10-23 00:25）

本輪以既有 canonical 前值 resolver／來源 fan-in／模型／paper executor 為唯一
實作路徑，不另設下載器，不修改訊號權重、TP／SL、成交容量、費率或正式帳本。
沿用價仍只解模型輸入；真首筆、可執行盤口與模擬成交保持不同契約。

已修正：

- 無首筆 journal v2 保留 proof 自身時鐘；相同價格／狀態但新的實際 absence
  證據也會刷新紀錄。後來的 broker delivery 不得替舊 absence 更新時鐘。
  bulk row-values 仍 lossless、維持既有同步 fsync，舊版原始資料不回寫。
- 完整官方 2,309 檔逐檔 audit 與私有 deadline projection 分開「真首筆」、
  「有效無首筆＋前值候選」及「未知」。只從原始 direct MIS receive clock
  解釋有充分證據的舊 absence，不能把 timeout／其他交易日當作零成交。
- 每日 native audit 新增 `last_trade_reference_standard`：按 deadline 與 proof
  TTL 計算 fresh candidates，並明示尚未核對模型前值庫；舊「曾觀察到」統計
  另列 boundary。共用純時鐘 helper／TTL，不初始化 provider／Discord／GPU。
- benchmark 使用真正 `price_source=panel, write=False` 的無行情盤前準備，
  保留來源原始 clock，分開第一個開盤推論與 steady repeats；跨午夜仍用原始
  交易日執行完整 registry 驗證，不降低 quorum、不放寬 source guard。
  新增 config／checkpoint／panel fingerprint／alignment／取價與驗證分段，
  並輸出 hash-bound 小 manifest；全量逐檔證據仍完整保留。
- TW public 只讀冷發布改為明確登錄的 shared-read source lease，讀者可同時
  工作，真正 writer 仍 exclusive。繼承 FD 驗證不改變父鎖，其他 dataset 不變。
  本輪遇到的舊 owner 自行完成，未解鎖或中止。scope 僅 code-only 的固定版本
  Discord 切換不讀 panel／不聲稱 GPU-ready；真心跳過期只等新心跳，不延長 TTL。

#### 原生 10/22：失敗價格與來源瓶頸

證據：[完整逐檔／原始 clock](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/native_prices.json)。
官方 axis 2,309 不變；數字是全市場 source candidates，尚不是各模型實際前值驗收。

| 09:00 後 | 真首筆 | fresh 無首筆候選 | 未知 | 可用參考候選合計 |
| --- | ---: | ---: | ---: | ---: |
| 1 秒 | 3 | 0 | 2,306 | 3 |
| 5 秒 | 57 | 0 | 2,252 | 57 |
| 11 秒 | 434 | 0 | 1,875 | 434 |
| 30 秒 | 1,256 | 0 | 1,053 | 1,256 |
| 32 秒 | 1,261 | 150 | 898 | 1,411 |
| 59 秒 | 1,280 | 320 | 709 | 1,600 |

首批原生 absence event 到達約 09:00:31.010447。第 11 秒仍未知的 1,875 檔：
Shioaji 全是 stale/preopen time；FinMind 全是 other-session rows；MIS 1,874
檔沒有 source observation，1 檔也是 stale/preopen。這些來源狀態可以重疊，
不是額外三組股票，也不是下市／停牌／已證無成交。來源錯誤另有 permission
或 subscription、連線／回應標頭前失敗、少量 5xx；沒有 scope proof 的 job
錯誤不任意分派到個股。所有個股與 job 原始原因均保留在完整 artifact。

59 秒私有 projection 的第一個受測模型為 1,272 真首筆＋319 前值＝1,591／
2,291，低於原門檻 2,062，正確拒絕；全市場 320 中的一檔不在該模型 scope。
不能把剩餘未知 709 檔直接標成無首筆來製造成功。舊 journal 去重沒有保留每次
absence clock 刷新，因此後續歷史 freshness 也有證據缺口；新 writer 已補記。

#### 六模式固定輸入：盤前準備、首輪與重複輪

完整來源重測原始觀測是 **10/22 13:29:59**，2,269 真首筆＋40 fresh absence。
不是把午後行情冒充 09:00。六模式官方清冊交集為 2,291–2,302；每模式都實際
沿用 40 筆有效前值，缺前值 **0**。5 份 aligned panel 前值庫全部在無行情盤前
建立；opening 階段 144 次 resolver 呼叫均重用同一份 reference 物件。來源日期
與原值完整留存，零成交日會往前找最近真實成交，而非硬填日曆昨日。

證據：[ABBA manifest](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/pipeline_original_clock_abba.manifest.json)，
[分段 trace manifest](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/pipeline_stage_trace.manifest.json)。

| 邊界 | 重測結果 | 不可推論的結論 |
| --- | ---: | --- |
| 全新 process 無行情盤前完整準備 | 106,824.32 ms | 不計作盤後熱快取，也不是開盤等待 |
| 盤前準備後第一次六模式 durable publication | 6,499.46 ms | 此第一輪是 independent-scan control；沒有候選冷首輪配對 |
| ABBA 共享 scan 六模式發布中位數（4 輪） | 6,332.98 ms | 已跑過開盤輸入的 steady，不是 09:00 原生完成 |
| ABBA 獨立 scan 發布中位數（4 輪） | 6,995.25 ms | 相同固定來源／全金融輸出，不能換成不同負載比較 |
| 共享 scan 全部 rich artifacts 中位數 | 7,717.43 ms | 完整 artifact boundary，不等於全部成交 |
| 獨立 scan 全部 rich artifacts 中位數 | 8,604.70 ms | hash／決策／解釋／weights parity 全數驗收 |
| 第二個細分 trace：首輪／steady 發布 | 5,546.29／3,712.02 ms | 不同負載、兩樣本，不能宣稱新的 ABBA speedup |

ABBA 同 process 相同來源下，共享 scan 發布中位數改善約 **9.47%**；保留全部
8 個樣本與尾端惡化（共享 scan 最慢 8,252.82 ms），不刪慢輪。
細分 trace 首輪仍有 config load 66.85–228.92 ms／model、panel fingerprint
120.72–899.03 ms、Factorized／MGDA attachment 759.74–773.57 ms。這些階段會
平行重疊，不能直接相加。六個 forward 首輪合計 1,230.00 ms、steady 合計
930.84 ms，另有格式化、契約／來源檢查及 durable publication；所以尚不能
聲稱只剩外部 API，也不能保證目前六模式全鏈一秒。

#### 成交、網站與部署驗收的邊界

完整來源原始 clock 已是 13:29:59；canonical executor 正確拒絕晚盤新進場。
本輪測到的是 eligibility＋registration/rejection／持久化工程成本，而不是
全部目標成交；synthetic one-lot book 亦不是真實盤口。沒有改原 clock、沒有
把沿用價變成成交、沒有為測速填滿帳本。10/22 六模式原生最後發布仍為
09:04:47.162969，最後 registration 09:15:59.412730；全目標 SLO 仍需逐筆證據。

夜間公網新瀏覽器首屏 **2,989.2 ms**，6 張模式 identity／100 筆訊號，JS
errors 0、水平 overflow 0；2 筆前景 paint identity 是 initial 更新，非開盤
revision-change，不能拼接成假的完整 09:00→畫面。證據：
[公網回讀](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/public_visible.json)。

887 項 focused Python 測試及 Ruff 已通過，涵蓋來源／無首筆／前值／最新 audit、
benchmark、paper／code-only reload、namespace／ACK、source flock／child
inheritance／發布與瀏覽器量測；不是全 repository suite。原生開盤 observer
已持久排程 10/23 08:55 及各 deadline，公網 browser watch 已排 08:58:30。
後者仍是 `/run` transient timer，未證明重開機後存在；不能把排程 active 當成
原生開盤驗收。本輪首個 qualified code-only reload accepted，engine／行情／
公網 owners 未換、3 份帳本 prefix hash 未變；固定版本與最終回讀另見 receipt。

下一個可量化工作：同一負載重測剩餘 config／member proof 成本，僅採用保持完整
source fence 與金融 parity 的變更；接著在真正 09:00 按來源逐檔 readiness、
各模式 durable pointer、完整原目標 fill commit、前景畫面 identity 分別驗收。
有未知／缺證／未成交時保留失敗，不以縮小 scope、造價、補成交或改 clock 達標。

最終固定版本於 00:27 驗收 `accepted_code_admission_only`：code SHA
`67a466045e36dffcab7187d97a8d9fd82a8d8d8992f037ded40592dab22306c6`，
native Python PID 1708813、1,616 個唯讀 namespace 檔案驗證；引擎 run
`05aa07b6342f481ba1cbb20c68d28b96` 未換，revision 63941 精確 ACK、lag 0。
Discord Gateway／core ready，交易引擎、隔日沖、Shioaji 行情及公網 owners
均保持原 invocation；3 份 ledger prefix 的 size／SHA 均相同，operator ledger
writes／broker orders 皆 0。夜間 GPU warmup pending 是另一本契約，未冒充已暖機。
證據：[最終 qualified plan/apply 回讀](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/reload_proof_final_apply.json)，
[每日新標準獨立回讀](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/native_audit_new_standard_summary.json)。

最終切換後再用另一個 fresh browser 回讀：首屏 **2,684.9 ms**、6 模式、
100 筆訊號、errors 0、水平 overflow 0，4 筆 visible identity 仍均為 initial
而非開盤 revision-change；不得與前一次 uncontrolled server/OS cache 的 2,989.2 ms
直接當作優化對照。證據：
[最終公網瀏覽器驗收](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/public_visible_final.json)。

### 新標準全鏈測速續修：來源、CPU 與網站冷啟動（2026-10-23 下午）

#### 先定義正確性與計時，再採用量測勝出的改動

新標準維持：盤前準備完整官方交易清冊、前一個已完成交易時段的真實收盤／
最後成交參考、模型與正規化器、完整特徵／來源契約、合法價格界線與輸出目錄。
09:00 後已有可驗證首筆成交便使用該時段首筆；**同日、有效、未過期的來源
明確證實尚無首筆且量為零**，才以盤前準備的前交易時段收盤作模型參考，
保留來源時鐘、零量與 carry 標記，開盤缺口為零。逾時、來源未回應、盤前
殘值或其他交易日資料仍是 unknown；carry 不會創造可成交容量、成交或損益。

驗收順序為：盤前準備 → 原生 09:00 gate → 逐檔來源到達／有效性 → 各模型
immutable ready／durable pointer → 引擎原目標 paper fills 的實際 commit →
HTTP／SSE → 前景畫面對應 identity。平行階段沿 critical path 判讀，不相加
重疊 CPU／wall，也不把不同探針、不同日期或重建時鐘拼成一個總耗時。

本輪實作／驗證依序處理：

1. 固定完整六模式來源做 config reuse 單變因 ABBA；不採用變慢候選。
2. 增加原 owner thread CPU 與 wall 分段，保留來源等待、失敗及完整 rich artifacts。
3. 對完整 status receipt 做 projection → JSON → canonical gzip／ETag →
   independent full readback，驗證所有保留值完全相同。
4. 實際 gateway 冷啟動；發現失敗後先修復記憶體／重複解析，再用原始完整
   JSONL 比較 stdlib／局部 native decoder，通過才部署。
5. 重測三 audience、保護服務／帳本、公網 HTTP、真瀏覽器與相關回歸；
   原生下一次開盤仍由獨立排程記錄，不以離線速度冒充已達 SLO。

#### 最新完整來源與逐檔失敗原因

10/22 官方軸是 2,309 檔，六個模型交集分母為 2,291–2,302，不能混為同一分母。
完整工程 fixture 的原始取得時間為 **13:29:59**，2,269 真開盤價＋40 個有效
無首筆；40 個 carry 參考在六模式中都可用、缺前值 0。這不是 09:00 的到達證明。

按原 acquisition journal 重算，完整 2,309 檔的狀態如下：

| 原生 deadline | 真開盤 | 有效無首筆 | unknown |
| --- | ---: | ---: | ---: |
| +1 秒 | 3 | 0 | 2,306 |
| +5 秒 | 57 | 0 | 2,252 |
| +11 秒 | 434 | 0 | 1,875 |
| +30 秒 | 1,256 | 0 | 1,053 |
| +32 秒 | 1,261 | 150 | 898 |
| +59 秒 | 1,280 | 320 | 709 |

+11 秒的 1,875 個 unknown：永豐共享來源全部是盤前／過期時鐘；FinMind
全部是其他交易日資料；MIS 是 1,874 檔未觀察到來源、1 檔盤前／過期。這是
同一批標的的不同來源理由，**不能相加為缺價數，也不能說每檔都個別逾時或下市**。
首次有可驗證 freshness 的 MIS 無首筆原生證據仍在 +31.01 秒。

完整逐檔、逐 deadline、各 provider 狀態保留於
[原始分類](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/native_prices.json)
的 `journal_timeline.symbols[*].deadlines`；本輪另以其 SHA 綁定
[精簡失敗統計](../artifacts/operations/tw_opening_latest_standard_20261022/owner_cpu_round9/native_source_failure_summary.json)。
不改分母、不回填原生 clock、不將 unknown 當無首筆，也未另登入永豐或送出券商訂單。

#### 六模式完整流程：保留負結果與內部 CPU 成本

`--compare-config-reuse --candidate-first` 正確單變因測試保留原本已啟用的
schema cache；先前 control 同時停掉 schema cache 的比較屬混雜比較，不採用。
正確 4＋4 輪 B/A/A/B/A/B/B/A：原 loader 發布中位數 **5,212.62 ms**，來源感知
config snapshot 候選 **5,620.72 ms**，反而慢約 7.83%。因此 `signal_engine`
維持原 loader，沒有把這個候選部署到六模式 owner。config snapshot 的失效／
繼承／caller isolation 工具與測試保留供重測；workspace bot helper 的改動亦
未透過本輪 gateway-only 操作部署到 Discord。

完整金融／weights／decisions／explanations parity 通過；證據：
[單變因 manifest](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_config_round8/pipeline_config_abba_single_axis.manifest.json)。

另一個完整 CPU trace 的兩輪發布為 **5,232.16／6,729.60 ms**，rich artifacts
完成為 **6,407.96／8,253.97 ms**。row formatting owner CPU 約 0.7–0.8 秒、
Factorized forward owner CPU 約 0.38–0.45 秒／模式，仍存在可控內部成本。
`perf_counter` wall 與 `thread_time` 只量 owner thread；相減不是純 API 等待，
也不包含 native worker／GPU 的總 CPU。cProfile 是擾動診斷，不作 native SLA。
證據：[CPU trace](../artifacts/operations/tw_opening_latest_standard_20261022/owner_cpu_round9/pipeline_owner_cpu.json)。

原 fixture clock 已是 13:29:59，canonical executor 正確擋下過晚新進場；
本輪不把拒絕／registration 工程成本當作完整目標成交速度，亦未改 clock 製造成交。

#### 網站 status projection 與真正冷啟動

固定同一份 9,527 failures 的完整 public status，以既有 frozen reference 做
8 個 ABBA 樣本。只在公開 DTO 省略 UI 未使用的重複 `failures` 陣列；保留
failure count、latest failures、trend、全部金融欄位與 append-only 原始紀錄。

| 此固定來源階段 | 原版 | 候選 |
| --- | ---: | ---: |
| 完整 decoded status | 4,432,834 bytes | 108,190 bytes |
| gzip wire | 151,168 bytes | 18,137 bytes |
| projection＋JSON＋gzip／ETag＋完整回讀中位數 | 351.55 ms | 14.42 ms |

這是該階段約 24.38 倍，**不是 09:00→成交的加速倍數**。全部保留值 SHA 相同，
source 未變：[完整 wire ABBA](../artifacts/operations/tw_opening_latest_standard_20261022/owner_cpu_round9/status_wire_abba.json)。

首次 gateway-only 部署後，真實 cold 驗證反而發現 HTTP timeout／重啟：
約 **634,859,715 bytes** 的 `opening_signal_latency.jsonl` 被 owner prewarm 與
HTTP 請求各自解析；原 `_tail` 同時保留 chunks、join、splitlines 與完整
transport dictionaries，cgroup 超過 4 GiB high、swap 到 1 GiB。py-spy 兩個
thread 都停在 `_tail → json.loads`。失敗回讀保留於
[不可算成功的第一次部署後測速](../artifacts/operations/tw_opening_latest_standard_20261022/owner_cpu_round9/api_after_local.json)。

根因修復使用同一個 canonical reader：以有限區塊定位最後 N 個實體行，再逐行
解析；失敗列僅快取 summary 實際使用的欄位；相同 path／projection／decoder
請求共用一次 cold build，其他來源仍平行，沒有全域 rate limit 或 32-request cap。
cache identity 含 device／inode／size／mtime／ctime，並保留來源讀取期間變更、
空行、CRLF、未換行尾列、損壞拒絕、decoder 隔離與容量上限契約。

接著在同一完整 10,000 行來源比較 stdlib 與 `msgspec` 局部 Struct 解碼；失敗
transport 不建物件，ready／registered 保留完整 schema。UTF-8 仍全行驗證，
JSON extension／native 不支援的資料回原 decoder，損壞不能默默略過。
8 輪原型完整 summary 中位數 **8,829.36 → 4,364.47 ms**；整合後 canonical
測速工具再做 4 輪，**9,455.59 → 4,251.70 ms**，full summary SHA 全相同、
source SHA 未變。只宣稱這段讀取／解碼／統計／回讀約 2.22 倍。
證據：[整合版本固定來源 ABBA](../artifacts/operations/tw_opening_latest_standard_20261022/owner_cpu_round9/opening_tail_native_production_abba.json)。

完整 OS cache 未清除的 process cold 三入口實測：串流 stdlib 修復後約
16.16–16.30 秒，native 整合後約 **8.21–8.48 秒**；是分次部署的實際觀察，
不是同負載 ABBA。兩次各持續觀察超過 150 秒，沒有再換 gateway invocation；
最後一次 anon 約 538–641 MB，high／oom events 均 0，未提高記憶體上限。
六個 protected owners 原 invocation、paper financial inventory 與 events／orders／
fills 三份 prefix SHA 全相同。只有唯讀 gateway 重新啟動，沒有交易 owner reload。
證據：[最終 cold 與保護邊界](../artifacts/operations/tw_opening_latest_standard_20261022/owner_cpu_round9/gateway_tail_native_deployment.json)。

#### 前端／公網／重開機後的測速

共用 visibility 檢查原先只等 rAF，headless／暫停 compositor 可讓已展開區塊
永久留白；新增一次性 100 ms fallback，正常 frame 取消 fallback，晚到 callback
不能覆蓋新 generation。仍檢查真實可見性，**它不是 paint 成功證據**；原本
前景 double-rAF／hidden／superseded 的 revision-paint 契約沒有放寬。
先前失敗的 FinLab 實際瀏覽器恢復測試現在通過，rollover harness 亦載入真正
共用依賴與 paint helper，沒有刪原測試斷言。

`www` 會 308 轉址到正式無 www 網址。首次 HTTP 測速嚴格不跟轉址，故保留為
[308 非成功結果](../artifacts/operations/tw_opening_latest_standard_20261022/owner_cpu_round9/api_native_public.json)；
修正 browser Python／JS 預設直連正式網址後另外測，不覆寫前次失敗。

本機 hot status 中位數 **2.41 ms**；昨日範圍 signal／position 首次約
1,328.40／178.83 ms，hot 中位數 5.05／6.94 ms。公網正式網址 4 routes 各
6 次觀測全部成功，status 中位數 **768.67 ms**、其 server app 中位數 1.92 ms；
health 首個新連線仍有 **19,340.90 ms**（app 0.29 ms）慢樣本，未刪除。
不可把公網與 origin 的差異全稱為純網路 RTT；它還包含 TLS／Cloudflare／
tunnel／proxy 等等待。另一次唯讀直連 IPv4／IPv6 都 HTTP 200，僅證明此觀測點。
證據：[本機](../artifacts/operations/tw_opening_latest_standard_20261022/owner_cpu_round9/api_native_local.json)、
[正式公網](../artifacts/operations/tw_opening_latest_standard_20261022/owner_cpu_round9/api_native_canonical_public.json)。

fresh public browser：首屏 **3,212 ms**、6 個 mode identity、JS errors 0、水平
overflow 0；前景 render 27.80 ms。當天所選範圍 signals 0，2 個 paint identity
都是 initial，revision-change 0，**不是新開盤訊號／完整成交顯示驗收**。
shared core 資產 URL 使用內容 hash，native 新瀏覽器已讀到新版本。
證據：[最終真瀏覽器](../artifacts/operations/tw_opening_latest_standard_20261022/owner_cpu_round9/public_native_canonical.json)。

最終獨立回讀再次核對 gateway invocation／四份程式 hash、六個 protected
owners、三份完整帳本 prefix、新版 browser core 與正式 HTTP receipts，全部
通過；IPv4／IPv6 此次直連 health 分別為 HTTP 200、1.65／0.95 秒。這是單一
觀測點的 reachability，不是全時段延遲保證。文件新增連結亦全部可解析。
證據：[最終 runtime 回讀](../artifacts/operations/tw_opening_latest_standard_20261022/owner_cpu_round9/final_runtime_readback.json)。

08:58:30 browser observer 已從 transient 改為正式 systemd timer，`Persistent=true`。
late start 真的執行驗收會留下 `missed_opening_observation_window` receipt、不開
browser，不冒充 09:00；每日原生 source／publication／full-paper-target observer
保持獨立。weekday browser observer 不作交易日曆權威，休市可能只留下頁面觀察。

最後擴大相關回歸 **1,463 Python＋90 Node 全通過**，這次沒有排除原 FinLab
失敗案例；Ruff、shell／Node syntax、`git diff --check` 通過。不是全 repository
suite。完整執行紀錄：`opening-full-semantic-regress-20261023T074620-2d0cd634`。

可重跑的固定來源／真 HTTP／真 browser 工具仍為 canonical scripts，例如：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_dashboard_latency.py \
  --profile-opening-tail artifacts/live/tw_day_trade_simulation \
  --local-only --repeats 1 --output /tmp/opening-tail-new-receipt.json
run_fintech_python scripts/benchmark_dashboard_browser.py \
  --url https://penguin72487.dpdns.org/tw-day-trade/ \
  --watch-ms 2000 --output /tmp/opening-browser-new-receipt.json
```

輸出必須是新檔。尚未完成的邊界：新標準原生 09:00 全來源可用性、六模型＋
完整目標 paper fill＋revision-change paint 的同一鏈 SLO；每日追加仍可能讓
telemetry tail 重新解碼，真增量 reader／可信 append prefix 可作下一個固定來源
比較；模型 row／attachment／publication 的內部成本及公網慢尾也未消失。
本輪不宣稱只剩外部 API、每交易日 11 秒保證、全鏈 1 秒或理論極限已達成。

### 新標準續作計畫：盤前準備、同鏈驗收與可驗證增量（2026-10-23 傍晚）

此節先於本輪程式修改記錄計畫，接續原任務
`tw-opening-latest-standard-latency-20261022`，不另建行情、模型或帳本 owner。

#### 完整路徑與盤前／盤後責任

| 階段 | canonical 入口／責任 | 可預先完成 | 仍須當時證據 |
| --- | --- | --- | --- |
| 交易清冊 | `tw_current_trading_universe`／盤前 source owner | 官方 registry、halt/status、模型交集與全分母 | 當日有效 receipt 與最後 arm 的年齡檢查 |
| 固定資料 | bot `_prewarm_market_signal_sync`／`_final_arm_market_signal_sync` | 完成日特徵、checkpoint／ABI、正規化視窗、最後真成交參考、資格、價格界線 | source／config／model generation 不得漂移 |
| 連線 | `OpeningPriceFanIn.warm_transports`／既有共享永豐 owner | 單一登入、venue map、連線／HTTP client 暖機 | 連線成功不是 09:00 價格有效 |
| 開盤取價 | `OpeningPriceFanIn.fetch`／`quote_provider` | provider 設定與完整 scope | 三路平行；逐檔原始 source／receipt clock、有效首筆、有效無首筆、unknown |
| 模型 | `prefetch_live_signal_prices` → `generate_live_signal` | 冷載入、panel／模型 cache、完成日參考 | quorum、正確 session 與當下 opening feature；保留全部六模式／原權重 |
| 發布／成交 | immutable execution pointer → `tw_day_trade_simulation` | 既有目錄、合約與訂閱 | durable commit、strictly-later 非試撮 L1、原 TP／SL、數量與完整目標成交 |
| 網站 | gateway → HTTP／SSE → `measureRevisionPaint` | 共用靜態資產與 cold telemetry preparation | 對應同一訊號／帳本 identity 的新 revision；initial paint 不算開盤完成 |

新標準不變：抓到同交易日有效無首筆／零量證據，填前已完成交易時段真實
收盤／最後成交作模型參考；原無首筆／零量保留。若前一日亦無成交，往前找
可追溯真成交，不能用無量參考值。來源失敗不等於無首筆；carry 不能供應
live Bid／Ask、成交容量或損益。開盤後首筆一旦可知，以該交易時段首筆為準。

#### 依已量測瓶頸安排修改順序

1. **逐檔價格失敗核對**：沿用原始 acquisition journal 與官方分母，核對每個
   deadline 的標的集合、各 provider 失敗理由與無首筆 clock，不用午後 snapshot
   冒充 09:00；重測工具禁止網路／正式寫入的界線維持。
2. **訊號輸出重複工作**：先檢查 weights／decision／rebalance 相同數值、排名、
   action／status 是否可在同一次推論重用。只採用所有列／欄位／排序完全相同的
   改動；不刪 rich artifacts、不改模型輸入／參數／金融歸約。六模式完整流程做
   單變因 ABBA，報告第一個盤前準備後樣本與後續樣本，不挑最快值。
3. **telemetry verified append**：同一 reader 重用已解碼的最後 N 個實體行，
   append 前完整驗證所重用來源前綴；僅解碼新增列，正確處理空白／非物件列、
   尾列補完、truncate／replace／舊前綴改寫及讀取中變動。不能只憑 size 增長
   假定 append-only。前綴 SHA 驗證仍有 O(bytes) I/O，明列它，不宣稱零掃描。
4. **整合驗收／部署**：focused semantic tests → 六模式固定來源完整發布／
   artifacts／隔離 executor probe → gateway／公網／瀏覽器。任何 production
   promotion 依現有 qualified owner 流程；不直接修改帳本／補成交，不用 workspace
   檔案存在當作已部署。保護 owner invocation 與帳本 prefix 獨立回讀。
5. **原生下一交易日**：既有 08:58:30 browser observer 與 daily journal observer
   記錄 09:00 wake、首筆／完整來源、每模式 publication、first／full-target paper
   commit、HTTP／SSE 與新 revision paint。同一鏈有缺項就標未驗證；不能把幾個
   離線／不同日期數字相加成達標。

採用條件：固定輸入完整輸出 parity、合理重複測速有實益、所有失敗證據保留；
未達條件的候選不升級。原生來源晚到、內部模型成本、公網慢尾分別追蹤，
只要仍有可控／未釐清成本，任務保持未完成，不宣稱已到理論極限。

#### 續作中發現的正式版本隔離問題

工作區稀疏 projection 契約已由 v3 更新為 v4，但現行 MGDA checkpoint 仍屬
已驗收的 v3。首次工作區六模式測試正確拒絕不相容的 model fingerprint，保留
失敗收據，不改 fingerprint、不放寬 checkpoint 驗證。單變因比較改在現行唯讀
code namespace 中，只載入明列且 SHA 固定的 row formatter 候選定義；資料、
checkpoint、clock、原始完整輸出與其他函式都共用該相容版本。

若比較通過，正式打包需從既有已驗收 source release 衍生，**僅覆蓋本次明列的
source files**，其餘 bytes 全部保留，不順帶啟用未驗收的訓練／資料／新聞改動。
沿用同一 `freeze_source`、immutable bundle／SHA／readonly namespace 與 qualified
reload；加上 lineage／明列檔案與拒絕 symlink、非 source、讀取中改寫的測試。
這是同一發布工具的版本隔離，不另建推論、行情或帳本路徑。

#### 當日補充驗收計畫：區分來源等待與證據時鐘錯序

10/23 的官方分母已變為 2,311，不能和 10/22 的 2,309 檔 generation
直接比較或共用有效性證明。沿用現有逐檔 auditor，另存同一日完整 journal
與官方清冊的驗收，保留跨 generation 比較被拒絕的紀錄。

當日兩筆 `provider_snapshot` 是完整合法 JSON，但其 `validated_at_taipei`
晚於 event 的 `observed_at_taipei`；這只能證明時鐘順序不成立，不能據此斷言
檔案損毀、NTP 或特定來源故障。下一個修改只擴充 auditor 的 report contract
為 v3：逐行列出封閉的拒絕理由與錯序毫秒，保留原 `malformed_lines`、degraded
及拒絕投影行為。不能平移原時鐘讓它通過，也不改 source、訊號或成交。
用合成錯序／naive／盤前 coverage 測試與當日完整來源 readback 驗收。

瀏覽器的「已有訊號明細」驗收因今日 0 筆訊號正確失敗；保留該失敗。
另用既有 watch observer 檢查六張等待卡、原始異常提示、SSE 與前景 identity，
這是資料等待畫面驗收，不是改小成功門檻或開盤成交成功證明。

#### 本輪結果與採用狀態（2026-10-23 18 時後）

> **18:20 後重要更正，先於下一修改記錄：**10/23 為颱風臨時休市，
> 六模式現有 `holidays` 及已部署 Discord 設定已由另一工作補入，見
> [非交易日 GPU 常駐修復](gpu_idle_load_2026-10-23.md)。
> [官方期交所緊急公告](https://www.taifex.com.tw/cht/11/announcement)
> 亦明列 10/23 全日休市。年度 TWSE 日曆未含臨時公告；本次手動
> source/runtime audit 未附各模式的既有設定休市判斷，導致下列原始
> 0／2,311 觀測曾被誤解為「交易日開盤來源故障」。原 counts、失敗與
> 時鐘證據全部保留，但今天**不得納入有效交易日的開盤延遲／失敗率樣本**，
> 也不能為了製造今天訊號而補昨收。計畫先重用 `tw_stock_day_decision`
> 為手動兩種 auditor 加上 current-config session applicability（明列不是
> 原開盤 owner attestation），測試 closed／unknown／mixed 不會變成成功
> 或刪掉來源證據，再重跑當日完整逐檔與 runtime 回讀。現有 daily observer
> 已讀各模式 `holidays`，不另建日曆、設定或重啟 Discord。

總驗收收據：
`artifacts/operations/tw_opening_latest_standard_20261022/prepared_incremental_round10/continuation_acceptance.json`。
原收據狀態為 `partial_verified_native_opening_source_unresolved`，
`all_requested_work_complete=false`；保留原件。18:20 後休市更正與最終回讀另存
`calendar_corrected_acceptance.json`，不得繼續把 10/23 分類為有效交易日來源故障，
兩份收據皆不表示全鏈達成一秒。

**10/23 非交易日：保留原始誤觸發與取價紀錄，不當成交易日測速：**

- 10/23 官方清冊為 **2,311** 檔。相較 10/22 增加 `6898 程曦資訊`、
  `6938 藍新資訊`，兩者仍為 `listed_for_regular_trading`，不能以缺價排除。
  跨日直接比較 generation 被拒絕的失敗紀錄保留；同日完整驗收另行執行。
- 六模式原始排程在 09:00 後 **3.215～8.077 ms** 觀察到開盤。
  首輪完整三路 dispatch 約 **382～384 ms**。單一 Factorized 首次紀錄中，
  `signal_pre_quote_prepare_ms=2274.639`、
  `quote_prefetch_outside_model_lock_ms=7123.795`，
  `mode_dispatch_queue_ms=7880.222`；這些是含括／重疊 span，不能相加。
- 第 1／5／11／30 秒皆沒有有效開盤或有效無首筆輸入：**0／2,311**。
  六模式均為 `TWOpeningInputCoveragePending`，沒有完整正式發布、
  當日模擬成交或對應新訊號 paint；缺少的耗時保持 `null`，不是 0 ms。
- 第 11 秒逐檔來源狀態：永豐 **2,311** 檔 `stale_or_preopen_time`；
  FinMind **2,311** 檔 `rejected:other_session_rows`；MIS **1,280** 檔舊時段、
  **1,031** 檔在期限前尚無觀測。這些數量跨 provider 重疊，不能累加成缺檔數。
  首個 MIS 完整工作約 09:00:16 回傳 2,309 列，但當時也都沒有有效新開盤。
  Fugle 拒絕／連線問題亦保留，不列為可用成功來源。
- 盤前並非完全沒準備：原開盤 owner 的六模式在 08:15 完成 base preparation，
  08:45～08:46 final-arm 均有 panel／checkpoint／model cache-hit 證據。
  `session_open` 的最後真成交參考亦由既有 `_last_completed_trade_reference`
  在 panel 暖機時預先建置並依 immutable array identity 重用。這不證明當下來源已就緒。
  目前 owner 在 12:01 重新啟動後的 `startup_warmup=pending`，不能倒推成原
  09:00 owner 未暖機；兩個 run ID／時鐘分開保留。
- 休市日沒有新時段輸入符合市場狀態，不能以此斷言券商、交易所、網路
  快取或程式時鐘故障。原排程誤觸發與年度日曆漏掉臨時休市有關；現行六模式
  設定已由另一工作修正並採用，本輪不重做其配置／Discord reload。
  不能用「無首筆沿用昨收」繞過休市、舊時段資料、來源拒絕或未收到資料；
  這些不是正在交易的同日零成交證據。

當日完整明細：`native_20261023_all_prices.json` 與
`native_20261023_symbol_clock_v3.json`（皆位於上述 `prepared_incremental_round10/`）。
後者含所有 2,311 檔的官方狀態、每個 deadline、每個 provider 的理由，以及
原 journal **193,438,515 bytes／48,651 行**的 SHA 前綴證明，不只列抽樣標的。

**新增的證據品質修正：**

- auditor v3 將不合法事件分別標示 partial line、schema／identity 及 coverage
  clock-order 問題，保留舊欄位與所有拒絕結果，不回寫來源。
- 今日第 6,692、43,403 行皆為合法 JSON，分別有 **913.655／634.996 ms** 的
  `full_market_validation_after_event`。這不是已證實的 byte corruption，也
  不能斷言是 NTP；原始 clock、兩筆拒絕與整體 degraded 都不變。
- 新版與舊版的完整 symbols／deadlines／failure jobs／來源前綴內容逐欄一致，
  正規化 SHA 為 `6fc3bb2da91fea684474753e3b8decb5b25530d9486f67d67dca9f5f78ef8962`。
  當日來源另做八次完整 decoder ABBA，parity 通過。

**有採用的讀取效能修改：**

| 相同工作／範圍 | 原路徑中位數 | verified append 中位數 | 驗收 |
| --- | --- | --- | --- |
| 10,000 個實體行的 tail refresh＋完整摘要＋JSON readback；ABBA 各 4 次 | 5,922.593 ms | 1,505.640 ms | 全欄位摘要 SHA 相同、來源未改 |

減少約 **74.6%**，不是刪分鐘、刪失敗列或聲稱初次建置為熱快取。
測試在私有位元組副本追加一條原始 failed row，故兩邊皆為 **9,528** 筆失敗；
正式當時摘要仍為 **9,527**。每次初始 cache seed **4,695.558～6,697.141 ms**
另行保留，沒有算成零成本。新路徑只增量解碼；重用前仍 SHA 驗證全部既有
source bytes，仍是 O(source bytes) I/O。truncate、replace、同 inode 改寫後
增長、未換行尾列、CRLF／空白／非 object、讀取中 append 均有 semantic tests。

**未採用的訊號格式化候選：**

- 六模式真實 checkpoint 在相容唯讀 namespace 中完成兩輪 ABBA，weights、
  decisions、rebalance、model explanations 完整輸出一致；CUDA 實際可用，
  cached model device 為 `cuda:0`，不把慢樣本推論成 CPU fallback。
- 第一輪 publication 中位數 **27,874.188 → 29,116.333 ms**，反而退步；
  反向八次 publication **7,058.683 → 6,936.608 ms**，全部 artifacts
  **8,558.374 → 8,288.131 ms**。第一個盤前準備後樣本與較慢樣本全部保留。
  結果不足以證明穩定的整段加速，**沒有升級 Discord**。
- 此處是固定 10/22 來源、禁止外部網路的工程流程；隔離 executor probe 使用
  明列 synthetic L1，且午後 source clock 會遭正確 cutoff 拒絕。不是 09:00
  開盤取價／完整目標成交成功證明，亦不能把 publication 當成 fill completion。
- 已驗證的明列七檔 source overlay 留在
  `artifacts/operations/service_code_releases/20261023T093847Z-24dd6623bf98/release.json`，
  狀態 `prepared_not_adopted`。未選取的 workspace 訓練／新聞／資料改動不會被帶入。
  v4 workspace／v3 checkpoint 的原始拒絕保留，不改 ABI 或 fingerprint。

**實際部署／回讀：**

- 只透過既有 `install_public_dashboards_service.sh --gateway-only` 重新載入
  唯讀 gateway。新 invocation 為 `631ba9ae09bd481e8614c09a2b8fa8f1`。
- 七個受保護 owner invocation 全部不變；持倉、原目標、已實現損益等 inventory
  一致，events／orders／fills 約 **2.32／429.95／426.45 MB** 原前綴逐 byte SHA
  驗證通過；沒有帳本改寫、券商登入、下單或模擬補成交。
- 新 process 冷 status 三入口實際 **10.409～11.243 秒**，比先前觀察慢，照實
  保留，沒有宣稱冷啟動改善。之後六次本機 status **4.825～16.543 ms**；
  持續約 162 秒 memory.high／OOM／OOM-kill／NRestarts 均為零。
- 公網 HTTP 全部所選 route 回傳有效 JSON，但當沖 status 三次中位數仍
  **2,038.152 ms**；同批 `Server-Timing app` 僅 **1.819～2.173 ms**。
  這證明此次慢尾不在該次 app compute，尚不足以指認哪一段 Cloudflare／
  隧道／網路路徑；不以本機 ms 宣稱公網已達標。
- IPv4／IPv6 的 healthz 與 status 四個 curl 皆成功；此刻 reachability 不代表
  任意用戶網路或所有未來情況可用。首次「已有訊號明細」瀏覽器驗收因今日
  0 筆訊號逾時，失敗收據保留。既有 watch observer 另驗證六張等待卡、
  原始錯誤提示、SSE 與可見 identity：**2,869.5 ms**，無 JS／HTTP 錯誤、
  橫向 overflow=0；對應開盤新 revision 樣本仍為 **0**，不稱開盤完成。
- 修正 benchmark 讀 immutable reference 時意外生成 bytecode 的行為；單一
  38,617-byte generated bytecode 已可恢復移到本輪 `reference_bytecode_quarantine/`，
  原 source bytes／receipt 未改，既有正式 release 完整驗證重新通過。

**測試與尚未完成：**

- 廣域回歸 **1,515 Python＋90 Node 通過**；新增 clock／source audit／release
  聚焦回歸 **216 Python 通過**。兩組 Python 有重疊，不合計為不重複案例數；
  不是全 repository suite 宣告。`git diff --check` 本輪檔案通過。
- 仍優先處理最近有效交易日（10/22）的來源覆蓋與 deadline 問題，而非要求
  10/23 休市日出價／產生訊號；其次實測 panel/member generation
  驗證、模型、prefetch 逾期、rich artifacts，以及 telemetry 全前綴 SHA／冷
  status 的剩餘成本。這些尚未證明可省略，不能只為數字變快而跳過。
- 最後需同一實際交易日證明 09:00 → 六模式 durable publication → full-target
  paper commit → 相同 identity 的網站新 revision paint。現有每日 browser／journal
  observer 保留；不能把不同日或離線時段的數字相加。10/26 為官方補假休市，
  目前已驗證行事曆下一個預計交易日為 10/27，仍須當日證據。
  [TWSE 市場開休市日期](https://accessibility.twse.com.tw/zh/trading/holiday.html)。

本輪任務保持 active；不宣稱所有故障已修好、只剩外部 API 或任何每交易日
11 秒／一秒 SLA。

#### 休市更正完成後的最終驗收

`calendar_corrected_acceptance.json` 為本輪最後摘要，原
`continuation_acceptance.json` 與所有失敗紀錄保留，不回寫原始行情／帳本。

- 共用 `opening_session_context` 重用 `tw_stock_day_decision` 與各模式既有
  `holidays`，附設定投影 SHA、當下觀察時鐘與「不是原 owner attestation」
  邊界。全 closed 為 `not_applicable_closed`；unknown／混合狀態保持未驗證，
  不當成功。scheduled-open 仍不等於當日實際開市證據。
- runtime、完整逐檔 auditor 都已回讀為六模式 closed，
  `native_opening_latency_sample_eligible=false`；逐檔 unknown 仍為 2,311。
  與修正前完整 source/classification 和 journal timeline 逐欄相同（僅排除
  auditor 自身分析耗時）；193,438,515-byte 原 journal SHA 不變。
  既有 daily observer 也回傳 `calendar_closed_or_unverified`、0 provider requests，
  不產生當天開盤 deadline 判定。無刪資料、補價或新增成交。
- `--refresh-unknown-mis` 遇到已驗證休市時保留 requested flag，但 effective
  requests=false，避免休市又對全清冊發出無意義補價；獨立明確的來源 network
  benchmark 仍可做健康診斷。本輪一次 FinMind 真 HTTP 回應約 624 ms 完成
  解析驗證，2,888 rows 中所需 2,311 檔仍屬舊 session，這不是開盤可用證明。
- 新增／相關日曆與逐檔回歸 **147 Python passed in 6.64s**。初次指令誤寫
  不存在的 `test/test_market_status.py`，exit 4、沒有跑測試；失敗保留，正確
  `test/test_market_status_calendar.py` 的後續 run 通過。和前述 1,515／216
  組有重疊，不合計為唯一案例數。
- 最近真正交易日 10/22 的原生 +11 秒仍為 **434／2,309** 真開盤、
  1,875 unknown；+30 秒 1,256 真開盤、1,053 unknown；+59 秒仍有 709
  unknown。詳見 `owner_cpu_round9/native_source_failure_summary.json` 及其
  全標的來源連結，這些問題並未因今天休市或報告更正而解決。
- 18:29 後另觀察到共享磁碟極高 I/O pressure，多個程序在
  `jbd2_log_wait_commit`，約 2.3 TiB root volume 可用空間僅約 17 GiB。
  本輪後續獨立診斷的 supervisor 排隊／durable report write 亦受影響，
  各 run 原生 wall time 照實保留；不能把這段 overhead 歸給行情 API，亦不能
  倒推為 09:00 原因。當下原生 `/proc/pressure/io`／`statvfs` 值另存
  `host_io_context.json`。沒有清理資料、停止其他工作或跳過 fsync。

目前仍需修正有效交易日來源覆蓋與內部成本，並在同一實際交易日驗證
全六模式 durable publication、full-target paper commit 和網站相同 identity
的新 revision paint。臨時公告自動匯入尚未實作；本輪重用已驗收的設定休市與
daily observer，不宣稱具有所有未來臨時休市的自動發現能力。

### 接續計畫：新標準的全鏈分解與搬運成本（2026-10-23 晚間）

本輪仍續用 `tw-opening-latest-standard-latency-20261022`，不改帳本、模型、
官方分母、報價日期或 TP/SL。先完成以下分解再改程式；10/23 休市的回應只可
量 transport，不能納入真交易日 09:00 SLO。

| 順序／責任 | 盤前可完成 | 09:00 後仍必須發生與驗收 |
| --- | --- | --- |
| 官方清冊、交易日與資格、合法界線 | 按當日官方 generation 準備及驗收 | 確認原 generation，缺少證據保持未知，不縮分母 |
| 最後真成交參考 | 完成日 close／正成交量與來源日期投影 | 有當日有效無首筆證據才使用；不創造成交量、深度或 fill |
| config／checkpoint／panel／normalizer／CUDA | 既有六模式 base/final-arm 暖機 | 驗證未變的 generation；新價格只覆蓋已定義的開盤輸入 |
| 永豐／MIS／FinMind | 重用正式連線，盤前 transport 預熱 | 各來源平行；單來源失敗不取消成功來源，未知維持 unknown |
| 逐檔驗證與 fan-in | 預建映射與 fallback | 真首筆／有效無首筆／過期／拒絕／未收到分列，保留原時計 |
| 六模型推論及完整輸出 | 不可用開盤後資料冒充盤前成果 | 相同原始模型、完整 weights／decisions／rebalance／explanations |
| durable publication 與 paper entry | 既有唯一 owner／資料格式 | 同 signal identity，原 TP/SL、因果 Bid/Ask、深度與整張驗收 |
| 網站通知、取得、套用、paint | 靜態檔與頁面可預載 | 相同 signal／engine revision 真的可見；缺任何段不填 0 ms |

最新可用原生比較仍為 10/22：+11 秒 434 真開盤／1,875 unknown，MIS 首四個
80-channel 請求在 headers 前約 4.29 秒逾時。先使用現有來源 benchmark 比較
完整相同官方 scope 的 batch 參數，不提高正式併發、不繞過拒絕；遇連線拒絕即
保留失敗並停止加壓。非開盤測試不能證明開盤負載或全市場成交 SLA。

新的可控假說只改 live 無損搬運：目前 Factorized 先建立小區塊，再複製到約
5.2 GB 的完整 dense 輸入。測試直接還原到本次專有目的切片，省掉第二次完整
複製及區塊配置；不快取跨請求的可變輸入、不改模型 GEMM／RMS／reduction
形狀、精度、欄位、座標或 checkpoint ABI。原有 training slab 呼叫保持原路徑。
先驗證跨 chunk／非連續日期／overlay／NaN／signed-zero 的原始位元與完整模型
輸出，再以已 qualified 六模式 namespace 做固定輸入 ABBA，包含首次暖機、完整
發布、rich artifacts 與 isolated executor probe，清楚分開真 fill 證據。

採用條件為語意等價、完整工作有可重現收益及容量無退步；沒有收益則保留負結果，
不部署候選。版本檢查本輪獨立暖測約 4 ms／次，不能拿舊的競爭下累積 profiler
時間作為刪除版本／來源檢查的依據。整個任務仍 active，並未達成一秒完成。

#### 反向驗證後的下一個單變因（同日晚間，先規劃）

直接目的切片的反向六模式比較，訊號發布中位數 5,242.748 → 5,637.714 ms、
完整產物 6,250.267 → 6,904.933 ms，皆退步；六個完整輸出 digest 與首輪一致。
故此路徑改為明確 opt-in、正式預設保持原搬運，不部署該候選。

接著只測可盤前完成的 host packet 準備：每次 Factorized／MGDA 推論仍把
514,640,064 bytes 的相同來源／座標重新打包到 pinned allocation。重用既有
`_BlockCache` 的同一 byte budget/LRU，按已驗收的私人 live base、不可變 source
identity、完整日期順序、股票順序與 pin policy 區分；只快取原始無損傳輸 bytes，
不保留 GPU dense input、模型輸出或開盤後 overlay。一般 training、可變 base、
非 inference 與容量不足仍走原路徑。建立與 pin 成本納入盤前第一次準備，不能移到
未計量背景工作。來源變更仍由既有完整 member 檢查失效，不以 TTL 略過。

先驗證原始位元、全模型 outputs/aux、跨日期/股票順序、overlay 不污染、容量逐出
與失效，再用相同六模式 namespace、盤前只讀準備及完整產物 ABBA。H2D bytes
仍需傳送，不能把省下 host 重打包宣稱為免除 API、H2D 或整段一秒完成。

#### 單變因驗收結果（2026-10-23 21:45）

證據目錄：`artifacts/operations/tw_opening_latest_standard_20261022/direct_transport_round11/`。
所有比較保留六模式、完整 weights／decisions／rebalance／explanations、durable
publication 及 rich artifacts，不刪股票、欄位或價格失敗紀錄。完整 JSON 與小型
`.manifest.json` 相互綁定 SHA；GPU 嚴格環境檢查均成功。固定輸入來自
10/22 **13:29:59**，不是 09:00 當時可取得的完整報價。

| 固定輸入完整工作 | 原搬運中位數 | 候選中位數 | 採納 |
| --- | ---: | ---: | --- |
| 直接目的切片，反向六模式發布 | 5,242.748 ms | 5,637.714 ms | 否，退步 7.53% |
| 直接目的切片，反向完整產物 | 6,250.267 ms | 6,904.933 ms | 否，退步 10.47% |
| 盤前 host packet，正向六模式發布 | 7,475.712 ms | 5,537.225 ms | 是，改善 25.93% |
| 盤前 host packet，正向完整產物 | 9,109.929 ms | 7,018.396 ms | 是，改善 22.96% |
| 盤前 host packet，反向六模式發布 | 7,027.314 ms | 5,997.905 ms | 是，改善 14.65% |
| 盤前 host packet，反向完整產物 | 8,666.841 ms | 7,271.089 ms | 是，改善 16.10% |

正反各四次／variant，六模式的完整財務與決策 digest 全部相同；兩個寬模型
每次候選皆命中盤前 22 個封包、沒有 miss。每模型封包含座標約 514,711,864
bytes；同一來源 cache resident 1,259,566,716 bytes，小於原 2,147,483,648
bytes 上限，沒有增加第二個無界 cache。H2D 仍傳送 514,640,064 bytes，
完整 dense／GEMM／normalizer 未縮減。反向程序 VmHWM 為 5,900,380 kB、
CUDA peak allocated 為 10,495,610,368 bytes；只是當次程序量測，不是全機用量
或降低記憶體的證明。

正向盤前準備 96,307.606 ms，另有來源 read-lease admission 93,988.656 ms；
反向盤前準備 91,956.032 ms，另有 admission 105.803 ms。所有成本保留，
不算進「省掉」的時間，也沒有把開盤後資料帶進準備。反向盤前準備後首次
發布仍為 **7,670.547 ms**，後續候選中位數 5,253.480 ms；不能拿穩態取代
首次，也不能宣稱已達一秒。全樣本皆實際 forward，`model_cache_hit` 不是
跳過模型，`completed_forward_cache` 皆空。

兩種實驗的 entry probe 都在獨立合成一張 book，且因輸入時點已逾開盤進場
deadline 正確拒絕，沒有建立部位。這不是全目標模擬成交或券商成交的證據。
生產帳本、價格、TP/SL、策略與模型 checkpoint 均未修改。

MIS 另測休市盤後 transport：batch 80 的 29 個實際請求無錯誤，完整工作到來源
持久回讀約 2,158.539 ms，僅返回 2,309／2,311 個陳舊 observation，不能當
全市場新鮮成功；batch 20 第一輪遇 4 個 502／headers 前斷線，第二輪 4 個
請求全部斷線，已停止加壓，沒有用 64.190 ms 的失敗小輪冒充加速。
未完成反向，不能宣布最佳 batch，也沒有提高正式併發。修正來源 benchmark
在 MIS-only 原生 circuit 開啟後不再新建下一輪 fan-in；混合來源仍繼續成功的
provider。離線注入測試保留失敗收據與尚未開始輪次，不重設來源拒絕證據。

預設採納僅限私人、唯讀 live base 的 CUDA inference 原始封包；CPU、training、
可變來源、不同日期／股票順序或容量不足仍正確回退。直接目的切片維持明確
opt-in 且預設關閉。最新相關 189 項測試通過；原先 96-byte 測試 fixture 的
正確容量拒絕失敗已保留，改為明確 4,096-byte fixture 後驗收，不放寬正式容量。

網站同輪只讀實測：本機 revision／status／signals 重複請求中位數
8.097／10.082／7.559 ms，首次 signals 仍有 2,694.645 ms；公網相同重複請求
中位數 294.962／1,156.584／230.148 ms，Server-Timing 多為 2–12 ms。
各組四次包含該組新連線，另保留 `first_observed`、`connection_first` 與
`warm_reuse`；不是四次全部強制熱連線。
公網 Chromium 頁面 ready 1,317.9 ms、render 23.6 ms、無橫向溢位或 JS 錯誤；
但本次休市頁面 signals=0、沒有 matched 新 revision 通知。初始狀態 paint
106.6／107.4 ms 不代表訊號 SSE 延遲，更不能串成真開盤至網站成交證明。

剩餘驗收仍是同一實際交易日的來源完整性、六模式發布、全目標 paper commit
與相同 signal/engine identity 的新 revision paint。10/22 +11 秒仍有 1,875
unknown，逐檔原始證據沿用 `prepared_carry_round7/native_prices.json` 的
`journal_timeline.symbols[*].deadlines`；逾時、其他日期資料或來源未收到均
不能被新標準改寫成無首筆。本輪不能聲稱只剩外部 API 延遲或所有工作已完成。

#### 最終預設與 native code-only 部署回讀（同日 21:53）

最終三檔 overlay 由原相容六模式版本產生
`20261023T134130Z-8c2e561f69f5/release.json`，只含已檢查的
`factorized_panel.py`、`benchmark_opening_signal_pipeline.py`、
`benchmark_tw_opening_sources.py`，不自動帶入其餘新聞／訓練 workspace 改動。
inference code SHA 為
`17b10ee1aff8e7923e9a5204251c81ecc0ed30623a4d665b3fc8ad46cdf38b64`。

`default_six_probe.manifest.json` 是最終預設的**單次 smoke**，不是新的 ABBA：
六模式 digest 與兩組正反比較完全相同，兩個寬模型未傳實驗開關亦各命中
22 個盤前 packet、0 miss，direct destination 關閉。發布 5,878.716 ms、完整
產物 7,372.319 ms，另計來源 admission **282,352.557 ms**、盤前準備
98,729.009 ms，完整受監督命令 432.219 秒。等待後以原鎖正常取得來源，
沒有強解鎖或終止資料工作。這項數據也顯示正式來源發布與盤前準備交接
仍是風險，不能從開盤樣本外移後便宣稱成本消失。

相關 189 項測試再次通過；另有來源合併／恢復／持久化 164 項通過。一次
額外驗證誤指定不存在的 `test_tw_opening_persistence.py`，exit 4、零測試；
改用實際 `test_opening_persistence.py` 的成功 run 已保留，不把未收集算通過。
修改檔案 Ruff 與 `git diff --check` 通過；沒有宣稱全庫測試。

qualified plan 通過；首次 apply 因既有 coordinator 控制鎖而 exit 75，沒有
重啟。保留該證據後，原入口以最多 30 秒 lease 等待重試，21:51:34 得到
`accepted_code_admission_only`。Discord 新原生 Python PID 133618；獨立回讀
再次驗證 1,616 檔 readonly namespace、Gateway connected、exact engine ACK
與 revision lag=0。當沖引擎 PID 145，隔日沖、期貨行情／面板、公網 gateway
及 cloudflared 的 invocation/PID 前後完全相同。events／orders／fills 原始
前綴分別 2,322,659／429,947,537／426,453,722 bytes，SHA 回讀一致，沒有
操作員帳本寫入或券商訂單。

部署範圍僅 `loaded_code_gateway_ack_not_gpu_readiness`，
`gpu_warmup_verified=false`；夜間新 owner 的 warmup pending 是正常保留的
邊界，不清除此欄或宣稱 warm-ready。後續沿用既有盤前 warmup／idle eviction
與已安裝的獨立 opening audit、browser observer，不提前製造成功回執。

完整結論與 SHA 綁定證據：
[本輪驗收](../artifacts/operations/tw_opening_latest_standard_20261022/direct_transport_round11/acceptance.json)。
其中 `all_requested_work_complete=false`、`one_second_slo_verified=false`；
native 完整來源→六模式→全目標 paper commit→同 identity 網站新 paint
仍須在真正交易日實測。任務維持 active，不因單段加速或成功部署而結案。

### 2026-10-23：一秒目標與限時昨收暫估（先規劃、再實作）

使用者本輪明確授權：不必等待全部新開盤價，缺少的新價可先使用盤前
已知的最近真實成交／收盤價。這是新增的 **model-only provisional**
輸入政策，不把來源未知改成「無首筆」，也不授權以昨收建立模擬成交。
原始報價、來源時鐘、官方交易分母及既有帳本保持不動。

| 段落 | 分類與理由 | 本輪實作／驗收 |
| --- | --- | --- |
| 外部首筆成交與來源發布 | 真實觀測的外部限制；但「全部更新才能推論」是可移除的內部障礙 | 命名並指紋化暫估政策，讀取已到達的共享報價；未知只在模型輸入使用有來源日期的最後真實成交 |
| 開盤網路等待／重複登入 | 內部障礙，不是物理下限 | 暫估訊號只讀 canonical 已收集快照，不同步等待 fan-in 或另開券商連線；原行情 owner 繼續取得新價 |
| 盤前資料、模型、輸入搬運 | 內部障礙；先前量測六模式發布仍約 5.9 秒 | 沿用完整六模式與盤前 host packet；逐段 profile，不刪模型／欄位／解釋來換速度 |
| 模型數學與硬體成本 | 尚未證實下限；不可直接宣稱不能再快 | 先保留完整輸出基準，再依實測最高成本選單變因優化 |
| 真正可執行 Bid/Ask 與流動性 | 模擬成交的外部限制 | 仍須訊號後的真實可成交報價、嚴格原 TP/SL、合法整張；暫估價不提供成交容量 |
| 發布、SSE、公網、畫面 | 內部障礙與外部傳輸混合 | 各時鐘分開記錄；不同身份／初始畫面不充當新訊號 paint |

一秒是端到端目標，不是已驗收結果。實驗分為：

1. 以原生 journal 的 +1 秒因果前綴比較嚴格政策與暫估政策；完整保留
   真開盤、有效無首筆、未知、無歷史參考四種計數與逐檔證據。
2. 測量盤前準備、第一輪六模式完整權重／風控／解釋／持久發布與後續
   重測；不以熱快取數字替代第一次開盤，不把完整歷史快照回填到早期。
3. 暫估政策改變輸入，故與最終新價的差異應揭露，不要求跨政策輸出
   相同；同政策固定輸入則要求完整契約雜湊可重現。
4. 聚焦測試包括：未到開盤拒絕、缺少歷史參考拒絕、零成交量不冒充
   真實收盤、未知與無首筆不混淆、暫估不可變成可執行價格，及既有
   strict/latest-quote 行為不變。驗收前不部署新交易語義。

使用者後續確認仍採 **新價優先，缺價才用昨收**，不採全部昨收盤前預算。
首輪 +1 秒因果回放已通過六模式完整輸入，僅 3 檔真開盤、各模式其餘
2,288–2,299 檔為明確標記的來源未知暫估；不是有效無首筆。
profiled 第一輪／重測發布為 7,793.87／6,959.21 ms，含 profiler overhead，
不能當成無 profiler 的改善對照，也尚未達一秒。

下一單變因候選為 **只快取盤前不會變的 Candle 前綴 embedding**：最後一列
全部原始欄位仍由普通 encoder 重算，完整 temporal／portfolio head 每次
重算，不快取最終訊號、不刪任何模型。key 綁不可變來源與日期／標的順序、
全部 encoder 參數／buffer generation、AMP／數值政策；每模型上限 64 MiB，
mutable 或無版本證據即走原路。雖然逐列數學獨立，CUDA GEMM 排程可能因
shape 改變而不同，所以完整金融／權重／風控／解釋 bit parity 與正反 ABBA
是採用門檻，不能以小型單元測試或局部 H2D 加速取代。

此 prefix 實驗的真實六模式比較已被拒絕：
`provisional_deadline_round12/prefix_abba.json` 保留
`rejected_output_parity`，MGDA 完整 digest 不同；432 項回歸通過仍不足以
推翻原生模型失敗。預設未啟用，實驗分支會從維護程式移除，凍結版本
與負面證據保留。其 4,511 ms 發布不能視為有效加速。

下一單變因只嘗試 **盤前完整原始 GPU 輸入重用**：不快取 encoder 結果，
每次 clone 自有完整 FP32 張量、覆寫當次開盤欄，保留原始全 shape／GEMM／
RMS／全部 head 與解釋。限 canonical 私有不可變來源、日期／symbol／feature
指紋及 CUDA device；全部模型共用最多 6 GiB 原始快取，容量不足走原路。
快取由現有 model LRU 持有，弱引用索引不延長 model idle eviction 生命，
跨 CUDA stream 以 event 交接。盤前準備、首次開盤、全部持久產物及峰值
記憶體需另列，完整輸出一致與正反比較通過才考慮採用。

完整原始 GPU 快取亦不採用：雖六模式輸出完全相同，峰值 allocated
15,692,357,120 bytes；兩個 arm 都持有盤前 dense 快取，control 也受到
此記憶體壓力影響，故 11,211.91 → 9,814.20 ms 不能當成有效整鏈加速。
candidate 最慢發布 15,739.72 ms，17/48 次 prefetch 過期；沒有放寬 5 秒
可變報價 age 來掩蓋。證據 `gpu_window_abba.json` 保留，未部署。

依兩個失敗重新分解：重排 GEMM shape 會改變 rounding，完整 dense 常駐
又會壓迫這張 16 GiB GPU。下一候選只保留盤前 narrow 歷史 embedding，
但 **當次 Candle 仍執行原本完整 shape**：GPU 私有零 workspace 裝入當次
最後一列全部真實欄位，原 shape 執行後只取其最後一列；其餘已驗證歷史
embedding 取盤前結果。沒有把零值當成最終模型歷史，完整歷史 embedding／
temporal／portfolio head 都保留；參數、buffer、來源與 AMP generation 改變
即拒絕重用，最多 64 MiB/model，沿用 model LRU 釋放。此方法仍須原生完整
bit parity 驗收，若失敗一樣拒絕，不能把數學逐列可分當成數值相同的證明。

串接檢查另發現，原 background source observer 在 serialized model inference
階段會 defer；舊路徑的模型本身還會呼叫 fan-in，所以沒有失去來源 owner，
但新暫估路徑刻意只讀快照，不能繼續依賴這個隱含副作用。實作前先固定修正：
啟用命名暫估政策的市場允許既有 observer 在模型推論期間繼續；strict 路徑
保留原排程，不增加 provider、登入、下載器或 quota。測試必須涵蓋各 stage、
混合市場及 09:00 的 observer 確實啟動；這不是並行取價的真開盤速度證明。

#### 本輪實測、否證與尚未部署的邊界（23:22）

命名政策 `known_prior_trade_provisional_v1` 已實作在 market loader、quote
prefetch、模型輸入、訊號 provenance 與 paper executor admission。新價僅接受
canonical 同交易日真實 open；沒有新價時，模型才取盤前已知的最後真實成交。
缺少有效歷史參考仍拒絕，不填零、不以試撮或 bid/ask 中點冒充 open。
未知來源、官方證實無首筆、真開盤三種狀態與其時鐘分開保存。
暫估分支不同步等待 fan-in、不額外登入券商；原 source observer 在此政策下
可與推論並行，原 strict 模式的排程保持不動。

paper admission 要求 policy/fingerprint 與 mode 設定一致、全部 active 模型
參考可用、整數計數一致（拒絕 bool）；寫入當日凍結 provenance，不因日後
設定改變而重標舊訊號。測試實際證明：只有 carried reference 時建立 pending，
**零成交**；後續真實且因果合法的 quote 才能進場。TP/SL、整張與成交容量
規則均未放寬。`latest_quote` 明確回到原 strict 政策，不把此例外擴到其他用途。

完整六模式的 full-shape prefix 結果如下；單位 ms，包含所有權重、風控、
解釋與 durable execution pointers，沒有刪任何模式。每組 4 對交錯樣本。

| 同命令比較 | 原完整輸入：全部發布中位數 | 候選：全部發布中位數 | 原／候選：全部 rich 產物中位數 |
| --- | ---: | ---: | ---: |
| 正向 ABBA | 4,883.14 | 3,637.81 | 5,720.32／4,453.50 |
| 反向 BAAB | 4,177.10 | 4,310.60 | 5,464.83／5,293.15 |

正向發布少 25.50%，反向卻慢 3.20%，因此**沒有通過穩定整鏈加速門檻**。
`prepared_full_shape_prefix=False` 維持預設；只保留明確 benchmark opt-in，
不把第一組最好數字當成採納證據。兩組共 96 個 model outputs 的完整 digest
一致，parity 通過不等於速度已達目標。candidate 先跑的第一次開盤輸入也保留：
發布 4,472.07、rich 完成 5,340.71 ms，不以後續熱重測取代。

兩次盤前準備分別 91,780.37／78,139.39 ms，均未使用當日 opening quotes；
來源 read admission 另計 265.84／234,161.65 ms。第二次確實等待既有 writer，
沒有強制解鎖；等待在樣本外列示，不宣稱消失。峰值 GPU allocated 均為
10,517,842,944 bytes。兩寬模型各保留 10,939,776-byte narrow prefix，
當次 logical source 5,196,746,496 bytes、實際尾列 transport 約 162,398,328
bytes；每模型 H2D payload 514,640,064 → 16,146,872 bytes 的局部改善，
不能抵銷整鏈反向比較失敗。

資料與 frozen namespace 綁定證據：

- [正向完整測速](../artifacts/operations/tw_opening_latest_standard_20261022/provisional_deadline_round12/full_shape_abba.manifest.json)
- [反向完整測速](../artifacts/operations/tw_opening_latest_standard_20261022/provisional_deadline_round12/full_shape_reverse.manifest.json)
- [profiled +1 秒輸入診斷](../artifacts/operations/tw_opening_latest_standard_20261022/provisional_deadline_round12/one_second_profile.manifest.json)
- [晚到開盤價工程敏感性](../artifacts/operations/tw_opening_latest_standard_20261022/provisional_deadline_round12/final_open_reference_sensitivity.manifest.json)

開盤第 1 秒的原始 journal 只有 3／2,309 檔真開盤、0 檔有效無首筆，其餘
2,306 檔仍為 unknown。六模型官方交集為 2,291–2,302，各模型有
2,288–2,299 個 provisional reference；不是「少數補價」，也不是來源已完整。
此 +1 秒輸入的取得時間**不包含在**上述 compute/publish ms，不能將測速
樣本說成 09:00 起一秒內完成，更沒有紙上全目標成交／公網新訊號 paint 證據。

另以相同 frozen code/runtime profile、feature date 10/21，比較 +1 秒輸入與
當天 13:29:59 收據所保留的真開盤資料。後者每模式剩 40 個有證據的無首筆，
0 provisional。這是跨命令工程敏感性檢查，並未在兩命令間持續 pin 同一來源
generation；不是嚴格只改價格的因果實驗，也不是當時即有完整價格的證據。
下表為觀察到的原始 model weights 差異；L1 是逐標的絕對權重差加總，單位
percentage points，不是成交 turnover、損益或策略績效。

| 模式 | 權重 L1 差（百分點） | 最大單檔差（百分點） | 非零標的：早期 → 後來 |
| --- | ---: | ---: | ---: |
| Annual v8 | 24.83 | 4.68 | 599 → 327 |
| Multi-Basis 22 | 1.17 | 0.49 | 4 → 4 |
| Multi-Basis | 2.38 | 0.37 | 34 → 34 |
| 1 億 | 2.02 | 0.01 | 2,069 → 2,069 |
| Unit MGDA | 174.68 | 36.54 | 15 → 17 |
| Factorized | 6.00 | 3.38 | 3 → 3 |

尤其 MGDA 的早期／後來非零標的僅 2 檔交集，不能把昨收暫估描述成沒有
研究影響的 transport 優化。保留完整逐標的權重與輸入 provenance 供後續
在同一來源 pin 下重測，不從這一筆觀察推導一般化策略品質或報酬。

最終相關九檔 **799 項測試通過（23.13 秒）**，包含新增 source observer
並行與 paper admission；另有 parser 早期拒絕、strict error payload 向後相容、
參考價來源日期、mutable/參數 generation 失效、CUDA 全 aux parity 與 model
LRU lifetime。Ruff 與 selected `git diff --check` 通過；未宣稱全庫測試。
反向第一次 CLI 驗證缺軸而 exit 2 的 run 已保留並由修正後成功 run supersede，
不能算模型失敗或速度樣本。

**新政策尚未部署，六份 market YAML 尚未改成 provisional，正式仍是原規則。**
只讀 engine-only reload plan 回覆 `startup inference warmup is not idle/ready`，
`restart_requested=false`；沒有重啟服務、沒有改帳本、沒有發券商訂單。
當沖切換技能要求部署身份與帳本／就緒驗收分開，本輪沒有用策略切換入口
重建歷史或繞過暖機門檻。
新的 source-observer 修正通過 workspace 測試，但尚未納入原生相容 bundle：
workspace bot 另有既有 `load_config_cached` 變更，而已接受的 frozen base
沒有該 API。不能只 overlay 整份 bot、引入一個無法 import 的 namespace，
也不能為了此修正覆寫使用者原改動或順便搬入全部 dirty 訓練程式。
因此本輪 native ABBA 證據只涵蓋其各自 receipt 已列的檔案，不當作此最後
observer patch 的原生部署驗收。

後續仍屬可解障礙：把相同來源／模型驗證與 panel attachment 變成盤前準備的
bounded session context，在原 source lease、config/checkpoint/code generation
失效時重建，避免每模式在 09:00 重掃。此次實測 prefetch 的 fingerprint
階段仍常花約 1–1.7 秒，故不能宣稱只剩 API。先完成同來源 pin 敏感性與
canonical owner 的雙端政策啟用，再由既有真交易日 observer 分別驗收 source、
全部發布、全目標 paper commit 與同身份網站 paint；一秒 SLO 維持未通過。

### 2026-10-23：六模式盤前候選訊號與取價優先權

本次需求只改取價排程：正式決策仍是已收到的新價優先、缺價依已選參考政策
處理，不能把盤前試算當成正式訊號、成交或無首筆證據。範圍包含目前啟用的
Annual v8、Multi-Basis 22、Multi-Basis、1 億、Unit MGDA、Factorized 六模式，
以及共用 Shioaji Snapshot、MIS 分批請求、FinMind 全市場回應與背景缺價修復。

實作前規劃：

1. 重用既有盤前暖機與 final-arm 的一次推論，明確以盤前已知最近真成交／
   昨收構造開盤 gap 試算；不另跑六次模型、不寫正式訊號或帳本。
2. 合併六模式所有非零原始模型權重，先排有候選訊號的標的，再排最近成交日、
   是否曾有成交、最近 20 個已完成交易日的平均股數，最後以權重絕對值及代碼
   穩定排序。不加總不同模式重複的成交量，也不以資金規模讓 1 億模式壟斷。
3. 準備資料綁 session、root、完成日與模型身份；盤後、跨日、未完成或損壞的
   preview 不可冒充有效優先序。優先檔是 private acquisition hint，不是價格來源。
4. 共用 fan-in 在批次分割前重排 provider 的請求與對應 fallback，原呼叫端輸出
   軸不動。所有官方標的仍在完整首輪；未準備／新增標的保留並採原順序降級。
   維持既有 provider 單飛、配額／錯誤退避、增量與 source-clock 驗證。
5. 斷線使 MIS 首輪未提交尾批時，後續完整重試給尾端先取得機會，不能一直重試
   同一個高優先 prefix。健康完整 sweep 不為公平性而額外增加網路請求。
6. 以固定輸入驗收全分母、逐股值與輸出軸不變；測 signal union、時點／單位、
   訊號先取得比例、排序開銷與失敗重試公平性。重排只保證有用價格更早排入，
   不預先宣稱全市場完成時間變短，也不把離線批次模擬當成原生開盤 SLO。

實作與驗收（2026-10-24 凌晨）：

- 新共用 owner `stockagent/live/opening_price_priority.py` 只產生、驗證與排序
  acquisition hint。暖機與 final-arm 各重用原本那一次 `write=False` 推論；
  盤前試算將最後列開盤 gap 明確替換成盤前已知的最近真成交／完成日收盤比，
  不偷用歷史面板已知的次日開盤。正常正式推論路徑預設不開此旗標。
- 合併六模式非零 **原始 model weight**，不是整張成交後剩下的持倉；不足一張
  的訊號仍可排隊。成交量使用 canonical `daily_volumes` 的股數，不混張／股，
  也不把六份面板的同一筆成交量加六次。近 20 日不含未知值，明確零量保留零。
- hint 綁 root、交易日、完成資料日、模型／checkpoint 及載入時捕捉的所有繼承
  設定 identity，以私有 `0600` 原子檔保存；跨日、晚於 09:00、損壞、相依檔
  改動時忽略無效模式，全部失效則回原完整請求順序。快取的是 permutation，
  **不是報價**；每次消費仍驗證相依檔，索引／DTO 不把可變快取交給呼叫端。
- 排序接在共用 fan-in 分批之前，Shioaji／MIS 與背景 repair 共用，fallback
  同步重排，輸出軸不變。FinMind 全市場端點只能調整本機投影的順序，不能
  宣稱能指定其遠端產生／回傳個股的優先權。
- MIS 未送出的尾批保留完整先後順序，下一次完整重試先處理尾端，half-open
  單股探測不能清掉它。初版只以集合 membership 加權確實被連續失敗測試
  抓到循環；已改正並保留原失敗紀錄。健康完整 sweep 不增加公平性請求。
- 原生六模型試算另抓到 Annual v8 實際有 **21 份**相依檔；原先 16 份驗證
  上限會錯誤拒絕它。改為有界 256 份並新增 30 父檔測試，驗證最後一份父檔
  改動仍會失效；不能只驗淺層設定就宣稱六模式都能使用。

以 10/22 完整官方收據的 **2,309 檔**固定輸入比較，保留所有標的，含 7 檔
不在六模型準備軸內的標的。下表是**前 320 個請求位置**的候選涵蓋數，
不是 320 ms、實際回包數或成交數；官方交集與 panel 軸的大小不同。

| 模式 | 官方交集候選數 | 原順序前 320 | 新優先序前 320 |
| --- | ---: | ---: | ---: |
| Factorized | 3 | 0 | 3 |
| Unit MGDA | 15 | 1 | 10 |
| Annual v8 | 589 | 11 | 66 |
| Multi-Basis 22 | 4 | 0 | 4 |
| Multi-Basis | 33 | 1 | 21 |
| 1 億 | 2,053 | 310 | 319 |
| 六模式 union | 2,054 | 310 | 320 |

最新完整相依檔驗證的本機 50 對交錯樣本，排序中位數 **1.181 ms**，原索引
順序 **0.031 ms**：排序增加小額 CPU 成本，收益是更早提交有用價格，而不是
「排序本身比不排序快」。真正冷讀／SHA 驗證／建 rank **125.59 ms**，發布
owner 會於盤前預熱；重啟 owner 的冷成本仍存在，未冒充已消除。早期只驗
較少相依檔的 0.53 ms 不是最終完整驗證數字，不混用兩組結果。
最終實際部署的 read-only namespace 另測 50 對：中位數 **1.889 ms**、原順序
**0.043 ms**、真正冷讀 **188.12 ms**；六模式有效、零 stale、完整 2,309 檔、
order SHA 與 workspace 組一致。兩種檔案系統／程序環境分開列示，不只選較快者。

相容 frozen namespace 的六模型盤前準備為 **96,760.95 ms**（移到開盤前，
不用當日開盤價）；來源 read admission 另計 **122.07 ms**。盤後離線餵入
10/22 09:00:01 journal 截止輸入後，全部正式發布中位數 **4,235.23 ms**，
完整產物 **5,520.39 ms**，首輪發布 **4,458.85 ms**；兩個樣本、12/12 inputs
採用預取、零 fallback。與上一次同收據試算按 market 對齊後，12 個 model／
target weights 及全部盤前 priority rows 一致。這是工程一致性核對，不是
一次嚴格 source-pin 的新舊整鏈速度因果實驗，更沒有原生開盤、全目標成交
或網站 paint 證據；**一秒整鏈 SLO 尚未達成，不能說只剩 API 限制**。

最終 13 檔相關測試 **1,115 項通過（47.24 秒）**；Ruff 與 selected diff check
通過，未執行／宣稱全庫測試。兩個已修正的失敗 run 均保留並以後續成功驗收
supersede，沒有刪除負面證據。

可重測命令（產物留在 ignored artifacts，不提交私有 payload）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_opening_quote_priority.py \
  --pipeline artifacts/operations/tw_opening_latest_standard_20261022/priority_round13/native_qualified_previews.json \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-22.json \
  --repeats 50 \
  --output artifacts/operations/tw_opening_latest_standard_20261022/priority_round13/recheck_queue_abba.json
```

新交易日先由既有 `benchmark_opening_signal_pipeline.py` 的
`--preopen-panel-warmup --preopen-quote-priority` 捕捉該日真實六模式輸出，
不可把舊日 preview 重標成新日；完整 native 命令保留於 workflow run。

主要證據：

- [六模型原生試算 compact manifest](../artifacts/operations/tw_opening_latest_standard_20261022/priority_round13/native_qualified_previews.manifest.json)
- [完整相依檔／全分母 ABBA](../artifacts/operations/tw_opening_latest_standard_20261022/priority_round13/six_inheritance_queue_abba.json)
- [最終原生 namespace 排序重測](../artifacts/operations/tw_opening_latest_standard_20261022/priority_round13/deployed_namespace_queue_abba.json)
- [最終回歸 run](../artifacts/operations/agent-workflow/runs/opening-priority-inheritance-reg-20261023T160326-9c46e8fa/run.json)
- [按模式核對 12 份正式輸出的 run](../artifacts/operations/agent-workflow/runs/opening-priority-native-parity-20261023T160459-477b6f39/run.json)
- [相容原生 source qualification](../artifacts/operations/tw_opening_latest_standard_20261022/priority_round13/six_inheritance_qualification.json)

部署驗收：既有 guarded Discord-only reload 於 **2026-10-24 00:07:12** 完成，
receipt 為 `accepted_code_admission_only`；另於 **00:07:44** 獨立重讀，確認
新 Discord run `437303-1792771621052835331`、Gateway connected、六模式 exact
ACK 與 read-only namespace 1,618 檔身份一致。source SHA 為
`b01ddaeaf00108ca3e53059f89607c1966275efa56fe2c20f724a6647cbfd4df`，
receipt `20261023T160338Z-965d7026f21b/release.json`。

交易引擎 run 保持 `b9d75c8d49a24de3b67ef2917a2cc88d`，其他 protected service
invocation 均未改變；events／orders／fills 分別 **2,322,659／429,947,537／
426,453,722 bytes** 的原前綴 SHA 與大小均不變，零 operator 帳本寫入與券商
訂單。六份 market YAML 的既有正式參考價政策未改；沒有把前階段 provisional
policy opt-in 夾帶啟用，也沒有將工作樹不相容的新版訓練 ABI 搬入正式 namespace。
本次只更新 Discord 的開盤取價 owner；交易引擎程序沒有重啟，不能宣稱所有
長駐程序都已載入本輪修改。

這項驗收只證明 loaded code／Gateway／ACK，不是 source／GPU／交易日 readiness。
獨立 readback 的 startup warmup 仍為 `pending`，盤後 maintenance worker 為
`running`，不能把它們改寫為 ready。六模式的盤前候選檔須在下一交易日由
正常暖機產生，再由既有 source observer 驗收實際首批／完整回覆、正式發布、
全目標模擬 commit 及同身份網站 paint。原先一秒整鏈優化工作繼續保留為 active。

- [實際安全 reload 與帳本前綴驗收](../artifacts/operations/tw_opening_latest_standard_20261022/priority_round13/deployment.json)
- [獨立 namespace／六模式 ACK readback](../artifacts/operations/agent-workflow/runs/opening-priority-independent-rea-20261023T160740-8de52ac2/run.json)

### 2026-10-24：全部完成度與缺價敏感度，先測量再規劃

本輪需求是比較各完成度延遲、缺價與完整價格的訊號差異。新增可重跑的離線
診斷，不發布訊號、不改帳本、不重啟服務。本節的行情時間來自原始 journal，
模型時間是離線計算，兩者不可相加後冒充已達成的交易日 SLO。

#### 三種完成度不能混用

- 真開盤價：官方當日清冊為分母；記錄各標的首次收到有效當日開盤價的時間，
  第 p% 門檻為 `ceil(N × p / 100)`。完整保留 **1%–100%**，未達為 null。
- 來源已判明：真開盤價加上仍新鮮的官方無首筆證據。後者會過期，不能累積
  成單調的「永久 100%」。新近收到舊 proof 不代表 proof 變新。
- 模型可計算：在明確 opt-in 的 `known_prior_trade_provisional_v1` 下，缺價可
  以已知最近真實成交收盤值作暫定模型參考。來源未知仍是未知，不能寫進真
  開盤價、成交量、成交價或帳本；本輪沒有替正式 market YAML 啟用此政策。

10/23 的官方清冊有 2,311 檔但收據有效開盤價為 0；六模式目前設定均列休市。
另核對 [TWSE 官方臨時休市公告](https://eshop.twse.com.tw/zh/)，確認 10/23
因颱風休市，**不把這一天當成開盤取價失敗率或訊號 SLO 樣本**。年度日曆
本身不能取代臨時公告；收集 owner 為何在休市日仍持續輪詢，另列待處理事項。
10/23 journal 另有兩筆 `full_market_validation_after_event`，validation clock
比 event clock 晚約 913.66／635.00 ms，分析狀態因此保留為 `degraded`；
這不是 JSON 位元損毀的證明，也不能把原時間改早來消除告警。10/21、10/22
用於正式比較的 journal prefix 均為 `observed`，沒有這些拒絕記錄。

#### 原生取價完成度

下列是當日 09:00 起算、所有來源聯集的真正開盤價首次可用時間，不是 HTTP
回覆筆數，也不是模型補昨收後的可計算率。來源時鐘與 malformed／失敗記錄
保留，沒有重標成另一個交易日。

| 真開盤價完成度 | 10/21，N=2,308 | 10/22，N=2,309 |
| --- | ---: | ---: |
| 1% | 2.12 秒 | 1.73 秒 |
| 10% | 7.44 秒 | 7.16 秒 |
| 25% | 12.26 秒 | 14.84 秒 |
| 50% | 20.04 秒 | 22.69 秒 |
| 75% | 8.86 分鐘 | 11.40 分鐘 |
| 90% | 53.42 分鐘 | 62.00 分鐘 |
| 95% | 135.33 分鐘 | 152.85 分鐘 |
| 99%／100% | 未達 | 未達 |

10/21 最終收據為 **2,279 真開盤＋29 新鮮無首筆＋0 未知**；10/22 為
**2,269＋40＋0**。前者真開盤覆蓋率約 98.74%，後者約 98.27%。這不能
被解讀為「最後 29／40 檔都是 API 故障」，也不能把它們填成真開盤價。
journal 截面與最終收據有不同時鐘精度；最終無首筆的 freshness 另在收據
實際時間檢查，不使用較晚收據證據回填較早截面的 readiness。

10/22 的第一個 provider 工作於 +326.16 ms 開始；Shioaji 首批有效開盤
在 +523.10 ms。至 +11 秒，434 檔真開盤、1,875 檔未知；未知集合內，
Shioaji 為舊／盤前時鐘，FinMind 為 other-session rows，MIS 幾乎沒有收到
可用觀測。這是同一批股票的多源狀態，不可相加成三倍缺價數。

全日各來源曾收到的真開盤價數：Shioaji **2,196**、FinMind **2,269**、MIS
**2**，Fugle TSE／OTC 各 **0**。不同來源有重疊，不可相加當完成度。Fugle
原始 HTTP 403／逾時及 MIS 未收到 HTTP 回覆的失敗保留；這些證據不能單獨
判定是 WAF、ISP、憑證或交易所的哪一方造成。

#### 同一模型、同一歷史 generation 的缺價比較

固定 10/22 官方清冊、10/21 已完成特徵、相同六份 checkpoint／runtime ABI，
在**同一個 canonical source read lease** 下跑：全缺價、+1／5／11／20／
30／60 秒，以及最後可得開盤價。全缺價只替模型準備已知最近成交參考值；
原始 open 保持缺值。最後對照使用最終已取得的 2,269 檔開盤價，其餘沿用
模型參考，不宣稱 2,309 檔都有首筆成交。最終價格是**事後敏感度對照**，
不是 09:00 當時可知資訊，不是回測報酬或券商成交。

所有模型保留完整 checkpoint symbol axis，不把不在模型內的官方股票從市場
分母刪掉。全缺價的六模式模型有效範圍分別為 2,302／2,302／2,300／2,292／
2,291／2,291；均以明確暫定參考達成該模型範圍的可計算率，並非全市場真報價
或可執行容量。兩個端點再反向重跑，六模式原始權重均精確一致。

差異採 `Σ|w_scenario − w_final| × 100`，單位為**權重百分點**，不是損益%、
不是資料錯誤率。下表是原始模型權重；完整證據另保留 target 權重、逐股差值、
新增／消失、方向翻轉、原始價格 basis 與來源日期。

| 模式 | 非零訊號：全缺價→最終 | 全缺價差異 | +11 秒差異 | +30 秒差異 |
| --- | ---: | ---: | ---: | ---: |
| Factorized | 3→3 | 6.00 pp | 7.29 pp | 0.03 pp |
| MGDA | 15→17 | 174.69 pp | 131.81 pp | 1.82 pp |
| Annual v8 | 597→327 | 24.82 pp | 17.43 pp | 2.06 pp |
| Multi-Basis 22 | 4→4 | 0.78 pp | 2.03 pp | 1.41 pp |
| Multi-Basis | 34→34 | 2.38 pp | 1.52 pp | 0.93 pp |
| 1 億 | 2,069→2,069 | 2.08 pp | 2.00 pp | 2.10 pp |

MGDA 兩組訊號僅 **2 檔重疊**，全缺價組有 13 檔在最終組消失、最終組新增
15 檔。Annual v8 有 285 檔消失、15 檔新增。1 億模式有 18 檔原始方向翻轉，
但其單檔權重差皆低於 0.01 pp；不能只數「改變了幾千檔」而忽略權重幅度。
這只是一天的輸入敏感度，不能推論哪個模型投資表現較好。

關鍵推論：**覆蓋率增加不代表訊號差異單調下降**，而且「昨收有訊號的標的」
不是完整的未來候選集合。不能只抓這些標的後永久忽略其餘股票，也不能用
單日 30 秒的收斂現象保證下一日相同。既有候選優先排序應保留，但必須加上
缺價影響／新候選敏感度及未服務尾批的公平性驗收。

#### 實測抓到的正確性障礙與一秒邊界

第一輪全缺價試算失敗：`prices=(2302,) expected=(2757,)`。原因是
`_price_snapshot` 的零 available 分支提早返回，跳過 active subset 回填完整
模型軸。候選修正讓零回覆也經過既有 expansion，保留 NaN、零 available、
原始 proof／時間戳；沒有造出價格、沒有多抓 API，也不啟用 provisional。
嚴格模式仍須通過原本 coverage gate。此正確性修正**只在離線候選 namespace
驗證，本輪沒有部署到正式 owner**。

修正候選下，盤前 panel-only 暖機後，第一次全缺價六模式 `write=False` 計算
5.17 秒，重跑全缺價 3.46 秒；其他不同價格截面的單次計算約 2.38–3.59 秒。
這不是固定輸入 ABBA 性能比較，亦沒有包含正式 durable publication、模擬成交、
Discord DM 或網站 paint，**不能宣稱一秒達標或全鏈只剩外部 API 延遲**。
第一個 Factorized 呼叫拆分約為：pre-quote 460 ms、cache-only quote 92 ms、
pre-inference 221 ms、model 285 ms、post-format 128 ms。暖機後仍有內部成本。

#### 依限制／可解障礙安排下一階段

| 順序 | 問題與分類 | 下一步及驗收 |
| --- | --- | --- |
| P0 | 零回覆軸長錯誤，可解障礙 | 本輪候選及 strict／provisional 測試已通過；另行採用安全部署流程，不能把 analysis namespace 當已上線。 |
| P0 | 休市仍輪詢與 403 無效來源，可解障礙 | 由現有 canonical 交易日／臨時公告 owner gate 採樣；盤前權限失敗與休市不佔開盤快路徑，保留低頻修復及原錯誤。 |
| P0 | 行情 event 與 validation 時鐘倒置，可解障礙 | 在既有 journal owner 分開保留原始 receive、validation-ready、durable-append 時鐘，模型可用性以完成驗證的時點計算；原始來源時鐘不動，補時鐘排序與慢驗證測試，不能靠刪除失敗行或放寬檢查冒充更快。 |
| P1 | +326 ms 才開始派發，可解障礙 | 所有可預備工作於盤前完成；預先 arm 單一 monotonic gate，分別量 timer wake、排隊、真正 provider request start，而非只量排程觸發。 |
| P1 | MIS 連線未回覆與各批波次，外部限制＋可緩解障礙 | 同一 source owner 量 DNS／連線／headers／body／解析；固定清冊 ABBA batch／並行度，保留 timeout 與全批覆蓋；避免無限重試放大外部故障。 |
| P1 | 完整報價尾延遲，可解排程部分＋市場限制 | 沿用既有逐筆保留、unknown 優先及 MIS 未服務尾批公平機制，新增全分位驗收；針對無首筆 proof 更新與價格已定股票量出重試浪費；維持每個 1% 的時間和失敗構成，不只加速首 320 檔。 |
| P1 | 模型缺價敏感度，可解障礙 | 在既有昨收訊號／最近成交／量能優先序上，離線量每組缺價對新增候選及權重的影響；同時追全市場完成度，不能拿稀疏模式改善取代全市場證據。 |
| P2 | 暖機後仍有 2–5 秒內部成本，可解障礙 | 依運算依賴圖拆出與當天開盤無關的子圖；量盤前可重用的 prepared request／ABI／歷史計算／輸入搬移及失效成本，只重算真正依賴新價的支路。保持完整模型語意，固定輸入 ABBA 加總 publication／report 成本；不直接重用已證實整鏈退化的 prefix 候選。 |
| P2 | 取得後顯示延遲，可解障礙 | 沿用既有 revision SSE 與 1 秒 fallback，不另建重複推播 owner；同一 signal ID 串 ready、durable publish、paper commit、SSE delivery、browser paint，量 p50／p95／p99 與錯誤。 |

沒有首筆成交時，交易所尚未產生真實開盤價是市場限制；模型參考可用既有
carry 政策解決，但不能消除此事實。Provider 發布節奏、網路傳輸與可成交容量
亦不能保證零延遲／零錯誤；要用每筆來源失敗可見、冪等重試與持續恢復做驗收。
1 秒是後續 SLO 目標，仍須下一個**實際交易日**完整端到端證據。
下一輪將每個交易日的 100 個門檻原始值、未達／不適用數與 deadline source
構成保留；累積足夠交易日後才報 p50／p95／p99，不從兩日樣本宣稱穩定尾延遲。

#### 工具、測試與重跑證據

- [離線完成度／訊號敏感度工具](../scripts/analyze_opening_price_sensitivity.py)：`source`、`signals`、`summary`；逐股資料保留在 ignored 私有 artifacts，compact summary 以 SHA 綁定完整證據。
- [10/21 全分母來源分析](../artifacts/operations/tw_opening_latest_standard_20261022/analysis_round14/2026-10-21.source.v2.json)
- [10/22 全分母來源分析](../artifacts/operations/tw_opening_latest_standard_20261022/analysis_round14/2026-10-22.source.v2.json)
- [10/23 休市狀態與原錯誤](../artifacts/operations/tw_opening_latest_standard_20261022/analysis_round14/2026-10-23.source.v2.json)
- [固定來源六模式比較 compact summary](../artifacts/operations/tw_opening_latest_standard_20261022/analysis_round14/pinned_six_mode_sensitivity_v2.summary.json)
- [相容候選 namespace qualification](../artifacts/operations/tw_opening_latest_standard_20261022/analysis_round14/qualified_analysis_code_v2.json)
- [完整原生試算 workflow receipt](../artifacts/operations/agent-workflow/runs/opening-pinned-sensitivity-axis--20261023T163135-7465eddc/run.json)

Focused regression：**302 passed，19.45 秒**。覆蓋完成度分母／所有 100 門檻、
零結果 provider、NaN／重複軸拒絕、權重對齊、失敗摘要拒絕，以及 opening
reference／live signal／source recovery／source audit；不是全 repository 測試。
第一輪零回覆維度失敗與 daily observer 歷史日期拒絕均保留，不刪掉失敗收據。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/analyze_opening_price_sensitivity.py source \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-22.json \
  --journal artifacts/live/tw_opening_snapshots/2026-10-22.acquisition.jsonl \
  --output artifacts/operations/tw_opening_latest_standard_20261022/analysis_round14/recheck_source.json
run_fintech_python scripts/analyze_opening_price_sensitivity.py summary \
  --analysis artifacts/operations/tw_opening_latest_standard_20261022/analysis_round14/pinned_six_mode_sensitivity_v2.json \
  --output artifacts/operations/tw_opening_latest_standard_20261022/analysis_round14/recheck_summary.json
```

實際模型試算應使用上述 workflow 的 `run_in_namespace` 命令及 qualification
指定的相容 v3 source，先在相同 frozen source 通過 strict CUDA preflight；
不能直接把目前工作樹其他工作的 v4 訓練 ABI 混入比較。工具拒絕覆寫原證據。

### 2026-10-24：盤後所有完成度實測、失敗隔離與保守優化

本輪加入既有 `benchmark_tw_opening_sources.py --after-hours`，不是另一個行情
collector。仍呼叫 canonical MIS、FinMind 與共享 Shioaji adapter；不發布訊號、
不寫模擬帳本、不開券商登入、不下單。週六凌晨的真實請求時鐘保留為 10/24，
價格／官方分母取最近完成的實際交易日 **10/22、2,309 檔**。官方日曆確認
10/23 無實際交易，不能直接拿「前一個平日」或把週六當作開盤。

**完成度定義。** 每項、每個來源及合併結果均保存 1%～100%，門檻為
`ceil(官方檔數 × 百分比 / 100)`；未達為 `null`，不插值、不刪尾端。
區分原始回覆、驗證後可用列、真正當日開盤價、歷史無首筆證據、同價格日
官方 MIS 零量列，以及歷史觀察已分類覆蓋率。歷史零量分類不是新價、最近
真實成交、fresh no-print proof、模型完整準備或可成交量；不能把 bid/ask
中點當成真正成交。原始來源時間／日期、flags、數值與單位保持不變。

最新序列驗收：先跑測試，再跑固定資料 ABBA，最後才進行真實網路請求，
避免這三個工作彼此爭搶資源。收據：

- [盤後真實請求，全部百分位](../artifacts/operations/tw_opening_latest_standard_20261022/analysis_round15/after_hours_acceptance.json)
- [4／8／8／4 併發的實網觀察，包含失敗輪](../artifacts/operations/tw_opening_latest_standard_20261022/analysis_round15/after_hours_workers_abba.json)
- [較慢且已撤除的時間解析快取候選](../artifacts/operations/tw_opening_latest_standard_20261022/analysis_round15/local_finmind_parse_abba_v2.json)
- [保守投影調整的固定輸入 ABBA 重測](../artifacts/operations/tw_opening_latest_standard_20261022/analysis_round15/local_projection_final_repeat.json)
- workflow `after-hours-final-acceptance-20261023T172716-0ca99610`：`succeeded`。

最新實網時間為 **10/24 01:28**，從各來源平行派送這個邊界開始：

| 完成度 | 全市場原始回覆 | 真正 10/22 開盤價 |
| --- | ---: | ---: |
| 1%／25%／50%／75%／90%／95%／98% | 642.74 ms | 642.74 ms |
| 99%／100% | 642.74 ms | 未達 |

FinMind 是整份 JSON 回覆，一批跨過多個百分位，因此時間相同；不是人工補出
較漂亮的逐股曲線。有效開盤價 **2,275／2,309＝98.53%**。其餘 **34 檔**在
FinMind 是 `other_session_rows`，不是新開盤價。較早成功的 MIS 實測保留了這
34 檔同價格日零成交量／無開盤價的原始快照；最新一輪 MIS 無法重新取得，
所以不拿那份舊回覆充當新的來源成功，最新分類仍忠實保留未完成。

不能只報 642.74 ms：這次獨立冷 CLI 的官方日曆／名冊原始雜湊驗證等準備
**941.32 ms**，來源工作及私有快照讀回 **1,054.92 ms**；函式進入到最終報告
持久提交 **2,036.89 ms**。來源百分位不包含這段冷準備，亦不包含 Python
行程 import、模型、正式發布、模擬進場、Discord 或瀏覽器 paint。正式開盤
的準備工作應在盤前完成；本輪沒有把冷準備藏成熱快取速度。

FinMind 本輪實際新 HTTP 回覆，`provider_response_reused=false`：

| 巢狀階段，不可和父階段重複相加 | 耗時 |
| --- | ---: |
| request → headers | 446.60 ms |
| body read | 164.36 ms |
| JSON decode | 8.27 ms |
| 全市場列驗證 | 12.73 ms |
| 私有快照寫入，含檔案及目錄 fsync | 13.19 ms |
| 同一份 bytes 的 hash／解析／全部金融陣列及時鐘逐位元讀回核對 | 2.37 ms |

**已修正的障礙。**

1. 盤後跨日原本被「必須今天」口徑全數拒絕。新診斷以 canonical 實際交易
   日曆找最近完成日，並用 canonical registry 的原始來源雜湊、membership、
   數量與 fingerprint 驗證完整分母。沒有放寬正式開盤的當日 gate。
2. 休市日原引擎刻意不授權共享報價工作，第一輪診斷卻送入請求並空等
   4 秒。現在明確記為 `skipped_closed_session_owner`，不另開登入繞過。
3. MIS 先前有 502；最新是 headers 前 `RemoteDisconnected`。MIS 自己的
   worker 先走既有 API 單筆 transport proof，失敗就不發出全市場 29 個
   請求；其他來源繼續。重測已驗證只執行單筆探測，不連帶卡住 FinMind。
   另於 01:34 做兩個單標的路徑診斷：環境代理變數皆未設定，沿用環境及
   關閉環境代理兩者均約 1.02 秒後 `RemoteDisconnected`。因此不是 JSON
   驗證失敗，也沒有證據可把問題歸給環境代理；遠端／中間路徑的進一步
   根因仍未定位。不改路由器、不換 IP 繞過限制。原始結果保留於
   `analysis_round15/mis_transport_route_probe.json`。
4. 來源退避沿用 canonical controller，跨 round 保留；403／429 不會因為
   新一輪或強制網路測速重置。Fugle TSE／OTC 本輪仍 403，完整留下失敗，
   沒有宣稱有即時權限。MIS 部分成功／熔斷也正確標為 `degraded`。
5. 計數時間改為驗證、合併及計數完成之後的 monotonic 時鐘。私有快照只
   讀一次 bytes，同時 hash／解析／核對，且讀回耗時包含真正核對工作。
   報告自己的最後一次 fsync 由 workflow stdout 記錄，不製造自參照零耗時。
6. 固定原始 FinMind body 以 SHA256 綁定原始 acquisition journal 接收時鐘；
   offline replay 的來源、quote 與官方名冊驗證共享這個測試時鐘，避免歷史
   回覆被「今天」誤拒。原始資料不改日、不回寫正式來源，無網路請求。
7. 既有 native per-job audit 新增全部百分位，僅對有完整官方軸雜湊證明的
   job 展開；模型 delta 保留原來的時間但不冒充全市場，避免無謂擴大報告。
   原有 10:30／13:40 oneshot timer 已確認 enabled、active，下一次會讀新版
   診斷程式；休市日仍按日曆跳過，不冒充交易日測速。

**性能採用邊界。** 時間字串字典快取雖金融／時鐘／持久列／journal 完全
一致，第一組完整本機 ABBA 中位數仍從 **275.84 → 312.92 ms**，已撤除並
保留候選原碼和失敗性能證據。保留的是把每檔重複查詢的固定日期界線移到
投影迴圈外、日期視圖隔離及診斷修復。最終兩組完整本機讀回的觀察中位數
為 **162.34 → 141.18 ms**、**206.68 → 165.25 ms**；包括真實 fsync 與平行
拒絕／斷線 fixture，金融陣列、時鐘、持久 rows 和 journal 觀察列 hash 均一致
（不是包含測速數值的整份 journal byte hash）。
這不是只有一個 decoder 的 microbenchmark，但磁碟／排程仍有抖動，不能將
全部差額因果歸給單一迴圈，更不能外推為開盤到成交／網站已加速同樣比例。
固定輸入為有原始接收證據的 10/22 09:57 回覆，不冒充現在的盤後資料。

4／8／8／4 的實網 ABBA 最後出現 502、部分請求未完成，因此不能取最快
成功輪宣稱高併發穩定優於原設定；**未調高正式開盤併發**。可修的是冷準備、
重複計算、串行等待、失敗隔離及時鐘；MIS 連線被對方／路徑關閉、Fugle
權限、外部報價新鮮度沒有零錯誤／固定延遲保證。不能宣稱已只剩外部 API，
或全市場真正開盤價／六模式訊號／模擬成交／網站已一秒完成。

最終 **494 個相關測試通過**。另修正了兩個舊測試的隔離問題：body retention
固定為合法開盤時鐘；全來源缺價測試也明確 stub Shioaji，避免其他測試的
報價快取或本機帳戶改變前提。這不是全庫測試。未重啟 Discord／交易引擎、
未載入新的 frozen 生產版本、未更換模型／帳本；新的手動／排程診斷可直接
使用，正式開盤仍須在實際交易日用原生日誌驗證整條鏈。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py --after-hours \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-22.json --rounds 1 \
  --output artifacts/operations/tw_opening_latest_standard_20261022/analysis_round15/recheck_after_hours.json
```

重測時使用當時最近完成日的官方 receipt 及新 output 路徑；工具拒絕過期
分母、開盤保護期及覆寫已有證據。不為了測速清除正式快取或省略持久性。

## 逐版訊號與已成交庫存再平衡候選（2026-10-24）

使用者已明確選擇「新價優先、缺價用最近真實成交參考」，並授權
**已成交的模擬部位也依最新訊號再平衡**。這是新的前向執行策略；不改寫
過去訊號／成交，不代表券商下單，也不把昨收參考冒充可成交報價。

候選 ABI 為 `progressive_open_reference_rebalance_v1`，預設仍是
`opening_once_v1`；目前六份 market YAML 尚未啟用候選。程式接上原有
資料擷取、推論、immutable execution pointer、模擬帳本與 dashboard：

- 每次新的真正當日開盤價進入模型輸入，就產生下一版。摘要保留 root、
  parent、index、input hash 與策略 hash；last／Bid／Ask／接收時間抖動
  不會誤觸發另一個開盤模型。每模式 single-flight，運算途中到來的更新
  合併到下一次最新輸入，不為每個百分位堆積一個模型工作。
- 每版按凍結的 session NAV 計算完整目標，再扣實際 signed inventory。
  同向減倉／反向先 FIFO 沖銷，再增加差額；每版增倉使用獨立 acquisition
  ID，保留原始成本、權重、成交時間、已實現費用和 TP／SL，不重設停損。
  封存的已平部位不會繼續占用全部本金，但實際費用與損失仍減少可用額。
- 缺價保留未知 target 和 source-wait template。補到真實開盤與合法界線後
  才定股數，且先扣既有庫存，避免補成另一份完整部位。停止／過時／跨模型
  revision 不執行；停止風險與 13:20 收盤沖銷優先。晚到的初始訊號仍走
  明示 09:01 反事實回放，不允許逐版訊號重寫該歷史 cohort。
- 交易前保留原 durable intent；失敗在程序內與重啟後均隔離，不猜測已成交
  或重複減倉。決策／進場／停損／出場保留微秒時鐘，不能將未來訊號套到
  較早的 book。一般原有策略的秒精度與既有歷史保持原樣。
- 網站把「本版新增／減少成交」和「目前庫存／完整目標」分開；舊部位盈虧
  歸原成交，不顯示成新版本零成交、空倉或零盈虧。維持既有自動更新路徑，
  不新增整頁刷新；更新 JS cache key。
- 完成度另驗證所有 signed target 減實際庫存；只完成增倉、不代表待減倉也
  完成。source-wait、未減倉或預算不足的差額不會變成 100%。減倉重試後的
  完成／首筆成交也寫入原有 durable execution observer；重啟不補造時鐘。

測速延伸既有 `analyze_opening_price_sensitivity.py signals
--progressive-execution`，以同一份 10/22 原生 acquisition journal、官方完整
分母、同一 feature date／六個 checkpoint，在來源 0、1、5、11、20、30、
60 秒接收 cutoffs 跑完整推論及隔離帳本。另保留晚到全價與重複端點比較。
所有 source/model clock 均為明示離線時鐘；正開盤、真實流動性與公網 paint
未被這個 fixture 驗收。

book 是明示的 engineering one-lot fixture；缺價保持缺價。它測量 canonical
規則、費用、append/fsync 與持久化，不是來源行情或交易所容量。0% 的完整
訊號可包含昨收參考，**零成交仍明確記成零成交，不算成功進場**。
rich artifacts 在所有 execution pointer／帳本提交後完成，完整 scenario
耗時仍包含每份報表，另列 `all_execution_commits_ms`，沒有刪去報表換成績。

第一輪 `progressive-six-mode-v1-20261023T182035-2f290821` 成功完成：各
cutoff 的六模式完整 scenario 約 6.96–11.70 秒，不是 1 秒。函式 profile
`progressive-six-mode-profile-ret-20261023T183517-53997a09` 顯示 JSON 編碼
及完整 state 發布仍是本機成本。20 秒 cutoff 的 100m 模式 register 在
profiling 下約 1.86 秒，其中 JSON 編碼約 1.03 秒；profiling 本身有額外
成本，不能當原生 SLO。

據此移除畫面版本指紋對 pending order 內大型私有 `position` template 的
重複編碼；完整 template 仍原樣寫進 state.json，order 數量、狀態、原因、
時鐘仍納入畫面 invalidation。另對純粹 tick 邊界運算使用有上限、完整
交易日／價格界線／offset／商品類型鍵的快取，不快取市場報價或可成交性。
原生 JSON encoder 的候選沒有直接套用：其非有限數值可能變成 null，不能
用編碼加速改寫原始缺失值語意。參考 [msgspec 官方型別契約](https://msgspec.dev/supported-types)。

證據位於 `artifacts/operations/tw_opening_latest_standard_20261022/analysis_round16/`。
`progressive_candidate_v1/v2/v3/v4/v5.json` 是明示 **prepared_not_adopted** 的
frozen source（基於可用的 v3 model ABI 加選定檔案，不載入整份 dirty
research workspace）。原始 full report／profile 保留；`.summary.json`
是便於後續查看的索引，不能替代原始 hashes、完整權重與私有帳本讀回。

上線預檢 `progressive_reload_preflight.json` 於 02:48 明確拒絕：
`startup inference warmup is not idle/ready`。當時六模式均零開倉，
`ledger_integrity_ready=true`，但 flat 不等於部署完成。未使用 apply、未重啟
服務、未修改正式帳本或啟用六份 YAML。需要先完成原生 owner／暖機驗收，
再循 canonical qualified reload 與下一交易日原生 source→signal→fill→paint
證據接續；不能用離線測速或 HTTP 200 宣稱 0–100%／一秒 SLO 已通過。
此時是休市日、暖機窗口外，原生 warmup 為 `pending`；不是證明 Gateway
中斷或策略故障。也不能把 code-only admission 說成 GPU 暖機完成。

v4 的第一次重測 `progressive-six-mode-v4-20261023T185542-5b54a6e7` 被
既有來源 writer lease 正確擋下，尚未運行模型；失敗證據保留。測速 CLI
新增 `--source-wait-seconds`，僅等待原有 canonical shared-read lease，不
移除鎖、不越過正在更新的來源，也不把等鎖算成開盤處理已完成。

最後一輪 `progressive-six-mode-v5-20261023T190906-e1ea7bad` 成功完成；
摘要 `progressive_six_mode_v5.summary.json` 綁定完整 report 的 SHA256。
`final_repeat` 與 `zero_repeat` 的完整模型權重端點 parity 均通過；此測試
未發出網路請求、未改正式帳本、未聲稱券商成交。來源 cutoff 是明示的
離線資訊集，不代表真實時間已在該秒完成模型或成交。

| 來源接收 cutoff | 六模式 execution 全部提交 | 包含完整 rich artifacts |
| --- | ---: | ---: |
| +0 秒 | 10,176 ms | 11,594 ms |
| +1 秒 | 10,604 ms | 11,957 ms |
| +5 秒 | 10,254 ms | 11,467 ms |
| +11 秒 | 7,918 ms | 8,642 ms |
| +20 秒 | 5,872 ms | 6,627 ms |
| +30 秒 | 6,219 ms | 7,061 ms |
| +60 秒 | 6,245 ms | 7,101 ms |

以上是每個 cutoff 的本機完整 scenario 耗時，不是開盤至成交延遲；隔離
book 的成交規則通過，也不保證所有目標已達成。+0 秒的六模式均為零成交、
`waiting_source`。+11 秒仍有模式缺價或部分成交；+60 秒仍有來源／目標
差額，不能以 signal registration 或沒有新增委託稱為 100% 成交。

v3 與 v5 並非同機負載受控的 ABBA：早期 cutoff 反而變慢，後期有所縮短，
不得宣稱整段一致加速。v5 的 +11 秒 scenario 中，六模式 `model_inference_ms`
合計約 772 ms、`pre_inference_prepare_ms` 約 2,310 ms、register／ledger
persist 約 3,007 ms、延後的 rich artifact 約 723 ms。這些是不同具名階段
的觀察，不把重疊／子階段重複加總；完整 critical-path 耗時以上表為準。
因此仍有本機準備、格式化、完整帳本發布成本，**不能宣稱瓶頸只剩 API，
也沒有達到一秒目標**。

### Round 17：完整六模式、排隊刷新與真正的畫面更新（2026-10-24）

本輪保持六個 checkpoint、完整 symbol axis、模型輸入與金融計算不變。
來源完成度是收到的真正當日開盤觀察；盤前已知成交參考只補模型輸入，
不是新成交或可成交容量。新版仍為 opt-in 的前向紙上策略，未修改六份
正式 YAML，未把 0% 來源參考訊號稱為 100% 真正開盤或成功成交。

先以 v5 的完整 signal/ledger profile 定位，實作在既有 canonical owner：

- 缺價／更新不再反覆拷貝完整 dense 歷史：盤前可準備唯讀 FP32 base，
  progressive CUDA forward 只傳當日更新欄。每次 forward 使用獨立的
  device clone，不能污染舊輸入；cache 受 byte/headroom/epoch 限制，
  有跨 stream 生命週期與 idle allocator release。一般 once 策略不額外
  保留不會使用的 device base。這不是 BF16 改值或刪 feature/window。
- Factorized manifest 的解析／SHA 與 immutable member 列表有 generation
  快取；每個 member 及 parent 身分仍即時驗證，ready/缺漏仍重新判定。
  原檔同尺寸且還原 mtime 的改寫、刪除、caller mutation 與讀中變更均測試。
  不能用 TTL 或只看 manifest 存在取代完整 source 契約。
- 排隊超過 50 ms 時，Discord 候選只刷新 canonical accepted quote cache，
  不在共用模型鎖內重新做 config/checkpoint/panel 準備。消費前仍重建並
  比對完整 request identity；來源、mask、政策、force-fresh 或讀中變更
  不符即拒絕。沒有新增登入或另一套行情 owner。
- private ledger 新增 guarded JSON encoder：只對 exact built-in、有限
  numeric、字串鍵結構使用既有 msgspec；其餘走原 stdlib 語意。原 journal
  prefix、canonical hash/public wire、flush/fsync/atomic replace 均不變。
  前段「不直接套用 native encoder」的限制仍有效，這次新增的是有拒絕／
  fallback 的私有路徑，而非全面替換。`state_encode_abba.json` 的 5.77 MB
  同 state、ABBA 三循環，解析型別和值與 FP64 bits 相等；編碼 median
  171.02 → 87.26 ms，僅代表編碼，不含帳本交易／fsync 或整段 SLO。

完整 v8 離線結果保留在 round17 `progressive_six_mode_v8.json`，所有
0、1、5、11、20、30、60 秒來源 cutoff、六模式皆完成；六模式 execution
提交約 6.92–8.51 秒，含 rich artifacts 約 8.33–9.90 秒。這和 v5 並非
固定負載 ABBA，部分 cutoff 反而較慢，不宣稱整段一致加速。
`v5_v8_output_comparison.json` 逐一核對所有 scenario/symbol 的完整 model
weight、target weight、bounded execution quantities 與模型身分，相等。
此比較不冒充所有 ledger economics 或真實流動性的完整證明。

v7 曾因 report 端對新 window descriptor 使用 `x[-1]` 而失敗；修正為
顯式 `last_row()`，只為報表取末列，不為格式化重建整個 window。失敗
report 與 v6 source freeze race 均保留，不以最後成功結果覆蓋。

#### 網站新版卡片被錯誤略過：重現、修正、瀏覽器驗收

新增同一份 `test_tw_progressive_browser.py` 的 full-page fixture：正式
模擬 engine → 臨時帳本 → 真正 public gateway/sanitizer → 原生檔案通知／
SSE → 不改動的 app → 真實 Chromium 可見卡片。六模式依序增倉、減倉、
歸零，保留所有原始紙上成交；fixture book 明示工程合成，公網價格權限
遮罩保持開啟。沒有把 fixture 行情或本機 paint 說成原生開盤。

`browser_v3` 重現：snapshot 與 HTTP status 都已到 revision 1，但模式
卡片 DOM 仍留 revision 0。根因是 compact status 省略完整 ledger counts，
而 render key 只比權益／持倉數；新目標不一定立即改變這些數值。
修正為納入 canonical **content_revision**、engine generation 及 opaque
signal key/index，不使用 heartbeat 的 state revision 強制重繪。

`browser_v4` 三輪六模式卡片皆到正確訊號 key，無 JS page error、無整頁
導航、無水平溢出。最後一個帳本 commit 到六張可見卡片觀察到 paint
約 357、80、71 ms；但這三輪六模式 register/persist 本身約
1,406、1,400、1,628 ms，**連此小型合成帳本都不能稱為整段一秒**。
持續的 browser-local revision→apply→two-rAF 記錄使用原共用 observer，
不是測試直接呼叫假 notice/applied。`browser_final` 再跑 full-page 和
390/820/1366/2560 responsive component 共 2 tests，另 44 Node tests 通過。
此第二次完整 browser run 的 register/persist 約 3,796／3,121／6,299 ms，
last-commit→可見 paint 約 452／862／439 ms，總計約 4,249／3,983／6,737 ms。
兩輪的耗時差異原樣保留，不能挑第一輪較小值當固定上限；當時其他工作
仍在共用主機執行。這再次證明只是可見畫面鏈路有 subsecond 樣本，而非
六模式 acquisition/model/ledger/paint 一秒保證。

這個前端修正的 `app.js` SHA256 為
`44f1cab4580cef08acf3f19e88d0dedbd38ace78ee246f3bdd4b2c870b726be3`。
`progressive-ui-public-readback-20261024T024959-de5bc344` 核對 workspace、
loopback gateway、公網 HTTPS 回傳 bytes 三者相等，公網 HTML 也指向新
cache key。這只驗證前端檔案已提供，**不代表 progressive 後端已啟用**。

本輪 qualified reload 預檢
`analysis_round17/reload_preflight_current.json` 再次保留
`startup inference warmup is not idle/ready`；`restart_requested=false`、
`broker_orders=0`、`operator_ledger_writes=0`。沒有降低驗收條件或將 code-only
admission 當成 GPU／原生訊號就緒；frozen v10 為 prepared_not_adopted。

剩餘限制與可解障礙分開：

| 項目 | 類別／目前證據 | 後續驗收 |
| --- | --- | --- |
| 尚無第一筆真實成交或上游尚未傳送 | 當下外部限制；可用已知真實成交參考產生模型訊號，但不能造撮合 | 原生交易日逐 provider first/full receipts |
| 六模式準備／full-shape inference／格式化 | 本機可解障礙，已移除部分重工，尚有明顯成本 | 同輸入完整軸比較、完整六模式 critical path |
| 逐模式 durable intent、FIFO/fees、全 state 發布 | 本機可解障礙；不能以省略 fsync 或帳本證據換速度 | 持久化 profile、崩潰／重啟／逐版 economics |
| 網站錯略新訊號 | 已重現並修正；loopback 真實 browser 通過 | 原生版本 key 對上公網可見 paint |
| 六模式 0–100% 到網站一秒 | 尚未達成，亦未原生開盤實測 | source→signal→paper fill→paint 全鏈路，不相加不相干的 microbench |

#### 固定來源 A1/B1/B2/A2 完整比較

`progressive-queued-refresh-abba--20261024T023613-54d8e5c1` 成功完成。
同一個 v10 frozen namespace、同一原生 journal 與 feature date，整輪持有
既有 source shared-read lease；A 保留排隊後完整重準備，B 使用 cache-only
facts refresh。四次皆完成全部六模式／cutoff／endpoint comparator、隔離
帳本與完整 rich artifacts，不只比較熱快取或省略報表。四組 A/B、A/A、B/B
比較皆完整 model/target weights 與 bounded execution quantities 相等。

`queued_refresh_abba_v10.json` 保留完整 run/比較與 source identity；
`queued_refresh_balanced_timing_v10.json` 是綁定該 report SHA256 的精簡
timing projection。下表是每 variant 兩次觀察的平均值，不是 p99 或可靠度
保證。未停止其他工作，因此即使順序平衡，仍不能視為隔離機器的因果證明。

| 接收 cutoff | A 六模式全部提交 | B 六模式全部提交 | A 排隊重準備合計 | B 報價刷新合計 |
| --- | ---: | ---: | ---: | ---: |
| +0 秒 | 8,914 ms | 7,179 ms | 494 ms | 28 ms |
| +1 秒 | 10,140 ms | 8,619 ms | 565 ms | 26 ms |
| +5 秒 | 11,143 ms | 9,537 ms | 608 ms | 25 ms |
| +11 秒 | 8,917 ms | 9,630 ms | 528 ms | 31 ms |
| +20 秒 | 9,893 ms | 10,035 ms | 513 ms | 37 ms |
| +30 秒 | 8,685 ms | 8,751 ms | 446 ms | 31 ms |
| +60 秒 | 7,672 ms | 9,396 ms | 406 ms | 27 ms |

排隊刷新這一段明確減少重工，但後四個 cutoff 的完整 critical path 沒有
跟著縮短，部分反而較慢。保留此負面結果：**不能宣稱六模式整段加速
18 倍，也不能宣稱本機瓶頸已消失**。目前工程 probe 仍是 parallel CPU
prefetch、serial model **及 serial ledger**；原生 Discord 和模擬 engine
是不同 owner，這個診斷排程不等於原生端到端時間。後續應在原生單一
帳本 owner 下量測 GPU/consumer 重疊、準備/格式化與 journal/state 成本，
保持 immutable intent、FIFO、費用與 crash recovery 的同一契約。

最後的同版本回歸
`progressive-v10-frozen-regressio-20261024T025326-21951e72` 使用正式相同的
read-only mount namespace 與 strict CUDA preflight，**766 個相關 Python
測試通過**（179.02 秒）；不是全庫測試。前端 44 Node tests 另行通過。
測試包含 source generation、FP64 JSON 值、owned FP32/跨 stream、每版
FIFO/fees/停止優先、重啟／durable intent、模擬帳本、public DTO 與真實瀏覽器。

此完整 run 另有兩次 background status rebuild 的 `OSError`，沒有覆蓋
負面證據或稱為零錯誤。追加 `progressive-browser-race-diagnos-20261024T025837-de3bfd13`
在正式 snapshot builder 邊界只記錄、原樣 re-raise 例外，不放寬來源版本
檢查，兩個 browser tests 通過；這次 `transient_source_errors=[]`，三次
last-commit→paint 約 172／318／243 ms。前次錯誤訊息只有 exception type，
不能靠後次沒重現就宣稱已定位或根除；完整 logs 與較慢 samples 均保留。
測試現在會在同一份 timing JSON 留下未來復現的 type/errno/message，
並留可見 viewport screenshot；不是新增 production 資料路徑。

下一步區分可排除的障礙與外部限制：

- 可排除障礙：以同來源／checkpoint、固定負載 ABBA 拆解重複模型準備與
  全量 state 編碼，將不變部分盤前準備、可重用的純函式結果增量化；完整
  權重、原成交、費用與 durable intent／fsync／恢復讀回都必須保留。沒有
  先做 financial parity／crash readback，就不能以減少寫入冒充加速。
- 外部限制：首筆成交是否存在、provider 到達時間、來源 writer 一致版本
  與可成交報價。昨收補模型輸入不能解除這些成交限制，來源失敗也不是
  無首筆的證據。原生正常交易日的 source→signal→fill→browser paint 仍
  須獨立驗收，離線 book 或休市日結果不能代替。

最後回歸 `progressive-inventory-completion-20261023T190834-bd95a4d1`：
**614 個 Python 與 27 個 Node 測試通過**，不是全 repository 測試。
真實 Chromium component fixture 在 390／820／1366／2560 像素通過
無 document／table 水平溢位與無 JS error 檢查；已人工查看手機與筆電截圖。
這是新元件與共用樣式驗收，不是公網完整頁或原生開盤 paint 的證明。
最新 JS asset hash 與 `git diff --check` 亦已核對。

最後讀回正式 service_sync：六模式均零開倉、零 pending entry 與 source
count，`ledger_integrity_ready=true`，Discord／當沖引擎服務仍 active。
本輪沒有更動既有六份市場設定；候選 policy 尚未啟用，正式 owner 未載入
新 frozen source。啟用仍須遵循 `stockagent-day-trade-strategy-switch` 的
驗收與 canonical qualified reload；上述 activity 不等於原生開盤已就緒。

### Round 18：先完成整體規劃，再改 critical path（2026-10-24）

使用者重申：依完成度變化發布六模式不可變訊號，盤中已成交紙上庫存亦
隨最新目標再平衡；從獲取至可見畫面以一秒為目標。本節在本輪程式修改
**之前**記錄完整規劃，承接 round17 的負面結果，不重置比較基準。

#### 衡量契約與不可省略的工作

- 官方當日可交易清單是來源分母；每模式保留 checkpoint 完整 symbol axis
  及它與官方清單的交集。新觀察開盤優先，缺價可用盤前已知最近真實成交
  參考供模型；保留來源日與 carry 標記。未知失敗不是零量／無首筆證據。
- 分開記錄 positive-open、有效無首筆證據、模型輸入就緒、目標定量／成交
  完成度；沒有可成交 bid/ask 的部位保持 waiting，不改報為成交成功。
- 一秒 SLO 分成 (a) 09:00 起算實際來源／執行全鏈路、(b) 本機接受一版
  輸入到六模式 durable 結果及可見 paint。兩者均需帶來源與模型版本。
  來源尚未發布、尚無真實成交／可執行報價是當下外部限制；CPU/GPU 準備、
  鎖排隊、重複掃描、編碼、持久化與重繪仍是軟體障礙，不能歸咎 API。
- 記錄單調時鐘各 owner 內耗時、跨 owner UTC 錨點與誤差、完整 revision
  identity、最早待處理事實年齡、合併／跳過版數；不能相減不同程序或
  browser 的 monotonic clocks，也不能只量最後一個 commit 到 paint。
- 同一批 source facts 一次發布完整六模式結果與等待狀態；不為每 1% 人工
  重跑 101 次。實際有效輸入改變才觸發，單模式 single-flight，忙碌期間
  latest-wins；來源已接受的好資料保留，失敗 delta 交既有 owner 重試。
- 原訊號、FIFO acquisition cohorts、費用、TP/SL、嚴格區間進場及停止／
  13:20 風險優先、未定量目標、fsync intent 和重啟防重複一律保留。

#### Canonical 流程與分階段修改

| 階段／既有 owner | 可解障礙與具體方案 | 驗收／不採用條件 |
| --- | --- | --- |
| 盤前資料與行情 owner | 預備清單、來源版本、最近真實成交參考、模型／normalizer／eligibility；持續三路並行及 accepted-cache 增量合併 | 過期／失敗／衝突有原始證據；不得新增 Shioaji login 或將無回應當無成交 |
| Discord 每模式 revision scheduler | source generation 喚醒、每模式單工作與 latest-wins；保留初始六模式公平性、queue／skipped／age 測速 | 來源 churn 不重跑、更新不遺失、錯誤自動重試；不解除全域 GPU ownership 而引入 Torch/cache races |
| CPU 準備／GPU／格式化 | 沿用 round17 immutable base、member generation、cache-only refresh；先 profile 固定來源全六模式，再縮小實際重工；逐步解耦純 CPU 與 serialized model owner | 完整 weights／targets／checkpoint axis 相等，容量／stream／source fences 保留；不重用已拒絕 prefix 實驗當成功 |
| 單一 paper ledger owner | 先量測 producer 推論與單一 consumer 的重疊排程，再拆解 register／journal／state；只在 crash/readback parity 成立後採用增量投影或合併提交 | 不可平行寫同帳本；fsync intent、原 journal prefix、fees／FIFO／降低風險優先與恢復完全相同 |
| Public snapshot／SSE | 先重現同查詢快速 revision 的 concurrent rebuild；將同 query-prefix 重建 single-flight、合併至最新，保留 bounded stale、原查詢隔離、錯誤與重試 | 每 query 最多一個 rebuild；無更新丟失／跨日期錯用、冷啟動可用、錯誤可觀測；不放寬來源 generation 檢查 |
| 網站 DOM／paint | 保留 canonical content revision／opaque signal key，原生 SSE、無整頁刷新；以真實 Chromium 檢查六模式最新卡片與共用 paint observer | 390／820／1366／2560、無 JS error／橫向溢出，畫面値對 revision；不得把 fixture paint 稱作開盤 SLO |
| 部署與長期監測 | compatible frozen release、strict CUDA、native warmup 與 flat/ACK/clock gates；逐交易日原 owner 量 first/full source→signal→paper→browser | 不混用 workspace model-v4 與既有 v3 checkpoint；未通過不啟用／不重啟，不把 active 或 HTTP 200 當 readiness |

實作優先順序為：網站重建競爭的可重現回歸與 owner 級合併 → 完整六模式
單帳本 consumer 重疊 probe／profile → 只針對 profile 命中的 CPU／持久化
成本修改 → 同來源完整比較與故障恢復 → qualified deployment／交易日
原生驗收。這不是全面前端重寫，也不另設資料下載或帳本權威路徑。

測試矩陣包含 0／1／5／11／20／30／60 秒及最終來源的完整六模式，
新增／減少／反向／未知目標、無首筆 carry、book 缺漏、TP/SL 拒絕、
過時／重複／並發更新、磁碟失敗與中斷恢復；A/B 保留完整輸出與負面
樣本。p50/p95/p99 必須有足夠樣本，少數完整試跑只報 observed timings。
目前六模式提交仍約 7–11 秒，正式 policy 仍為 `opening_once_v1`；不能
從規劃或局部毫秒測試宣稱 progressive 已上線或一秒／零錯誤已達成。

#### 本輪已實作及回歸

- Gateway 的 revision key 不再各自啟動同查詢重建：同一 query-prefix
  一個 active consumer 與一個 latest pending；不同日期／路由不互卡，
  沒有加入全域 rate limit。32 版 burst 原本同時重建 32 次，回歸現在
  peak=1、實際建置第 1 與第 32 版。失敗不污染成功 cache，後續版仍會
  排空；冷／過期請求共用同一 scope lock，不能越過 stale 時限。
- 原 private service journal 新增 `dashboard_revision_build` spans：queue、
  scope wait、build、最早／最新請求至結果、superseded pending 數及 ready；
  記錄實際 OSError errno/reason，且 log sink 失敗不能永遠佔住 query。
  這是 gateway monotonic clock，不冒充來源、模型、成交或 physical pixel。
- 前端修復輕量 render 的 paint 觀測遺失：前一版 double-rAF 被取消時，
  即使下一次 apply 不需重畫 mode cards，仍重新安排 canonical observer。
  不強制重畫全頁，也不偽造 SSE notice。新 app SHA256
  `68422e271e4fe8f2eec1f064bb9b317faecb2116a0a2217a0b0a1e40dd9084f0`
  已由 loopback 與公網 HTTPS 獨立讀回相同；只代表靜態 UI 已提供。
- 六模式 probe 新增 spawn 的獨立單一 ledger consumer；模型下一模式可與
  前一模式持久化重疊，但從未平行寫同一本帳。完整報表仍完成並單獨計時，
  projected receipt 的情境資料不偷看最終來源，consumer 使用 parent-observed
  IPC completion，不相減不同程序的 monotonic clocks。
- profile 命中 private ledger JSON guard：改為只 stack containers、inline
  檢查 scalar，避免每個欄位的 recursive generator。保持 exact builtin、
  非有限數、custom type、depth、舊版 fallback 及錯誤語義；原 encoder、
  canonical hash、公開 wire、journal 舊 prefix、fsync 及財務欄位未改。
  同一份 5,765,495-byte 真實 state 的三輪 ABBA，encoding median
  60.359→49.452 ms、bytes 完全相等。這只是編碼子步驟，不是全流程加速比。

`progressive-query-final-regressi-20261024T071536-948c2178`：**165 Python
與 60 Node 測試通過**；`progressive-ledger-guard-parity-20261024T070352-5bd40567`：
**412 相關 Python 測試通過**。這是受影響測試，不是全庫驗收；最終另於
固定相容 namespace 合併重跑，不把重複試跑次數加進測試總數。
前次非同步其他 query 的 log 混入測試 capture，精確依 query 選取後重跑；
原失敗 run `progressive-query-final-regressi-20261024T071118-3606de9f` 留存。

真實 Chromium 使用臨時 canonical 紙上帳本、loopback gateway/SSE，六個
fixture mode 做三版增減／歸零；最後提交至觀測畫面 72.72／145.37／238.76 ms，
首筆 register 起算則 1065.98／888.71／1254.26 ms，連這個小 fixture 也不能
宣稱全部一秒。114 個帶 market／generation／opaque signal key 的 paint
樣本，apply→double-rAF opportunity 12.7–34.9 ms；不是來源／真實六模型
或 physical pixel timing。390／820／1366／2560 像素檢查通過，已人工查看
六模式可見卡片與手機截圖。完整證據位於
`analysis_round18/browser_final_v2/`，保留原本遺失 paint 的失敗 JSON／截圖。

#### 尚未消除的障礙與部署界線

1. 正常 `events.jsonl` 追加可能碰到 strict generation handoff/read fence；
   現已能分辨 `changed before index handoff` 與 `changed during complete
   session read`，但尚未消除。不能為效能放寬同 inode rewrite／truncation
   驗證；下一層需 engine 在 durable commit 發布可驗證 journal byte-prefix，
   snapshot 消費同一 committed prefix，而非混合可變檔案版本。Gateway
   captured token 與 builder 看到較新 state 的競爭也須一起處理。
2. 六模式的 CPU 準備、Factorized/MGDA 的完整 input reconstruction、serialized
   inference 與逐模式帳本提交仍是本機障礙。queue 等待可能已含於 prefetched
   quote 的 end-to-end span，不能直接把 `pre_quote_prepare_ms` 都稱為 CPU
   計算。只量 encoder 或 fixture 不能聲稱只剩 API 限制。
3. 六份正式 YAML 未變，progressive/rebalance 候選尚未啟用。2026-10-24
   15:18 的 qualified reload **read-only** preflight 因 native warmup `pending`
   拒絕，`restart_requested=false`；休市日不在原生準備窗口不等於服務故障。
   Discord connected、引擎六模式零庫存／零 pending、ledger integrity ready，
   只證明當下 owner/帳本狀態，不證明新 frozen source 載入或開盤一秒 SLO。
   未修改正式帳本、未下券商單、未繞過暖機／ACK／部署 gate。

#### 完整六模式同源 ABBA（完成，不是原生開盤驗收）

`progressive-ledger-guard-full-ab-20261024T070727-5ea1a996` 已完成 strict CUDA、
原來源 read leases 跨四輪固定、compatible read-only v11(A)/v12(B) namespaces，
唯一不同 source member 是 `stockagent/private_json.py`。每輪均跑六個完整
checkpoint axes、全部十個情境及 endpoints、獨立單帳本 consumer、報表與
最後讀回，沒有裁剪 symbols／分鐘或跳過 reports。四輪各約 583 MiB 的完整
輸出保留在 `analysis_round18/ledger_guard_{a1,b1,b2,a2}.json`，較小且 hash-bind
的 `.summary.json` 可供日常查閱；總表 `ledger_guard_full_abba.json` 為
`complete_outputs_equal`。

| 邊界（毫秒；兩輪同版本） | A median | B median |
| --- | ---: | ---: |
| 來源 +0s 情境，六模式 durable commits | 7,555.05 | 4,261.15 |
| 來源 +11s 情境，六模式 durable commits | 7,205.25 | 4,346.15 |
| 來源 +60s 情境，六模式 durable commits | 6,507.28 | 5,860.37 |
| 七個可執行情境合計 14 個觀測值 | 6,032.55 | 4,583.29 |

B 的十四個可執行情境仍為 **3,630.15–6,189.12 ms**，未達一秒；每一情境
不是說開盤後該 cutoff 就已完成，而是把當時可知來源固定後本機執行耗時。
包含初始暖機、完整 artifacts／驗證的每輪 wall seconds：A1 311.398、B1
261.940、B2 253.893、A2 289.074。沒有停掉其他工作，A1 有 33,067.65 ms
慢樣本；這些 ABBA 觀測不能全歸因為 JSON guard 的因果加速比，也不適合
宣稱 p95/p99。負面樣本照留：盤後 late-information comparator median
A 2,538.68→B 2,737.28 ms，zero-repeat A 2,379.00→B 2,407.05 ms，未全面變快。

A1/B1、B2/A2、A1/A2、B1/B2 四個對照均通過完整 model/target weights 與
有界 execution quantities 等值、來源／模型 identities 及 endpoints 驗證。
Comparator 明確 `all_ledger_economics_proven=false`，不能延伸成逐筆所有
費用／清算 P&L 的全歷史證明；金融與崩潰語義另由相關 ledger 測試覆蓋。

最後候選 v13：
`artifacts/operations/service_code_releases/20261024T071912Z-6c4329918023/release.json`，
receipt SHA `03c2fcd48a0f009a6b477d1044d4d1aaa951af2dbd42cada0289243e6194effd`，
source SHA `e5b2291ab00cf869a0e6c24bb9f5829bab3854f492ecfcacdde4a3b4e915d107`；
在 v12 上只補本輪 gateway 診斷／timing 最終版，不混入工作樹 model-v4，
仍是 `prepared_not_adopted`。既有 opening-audit 與 opening-browser timers
當下 active，下一平日 browser 排程 2026-10-26 08:58:30；這是已排程，
不是未來交易日已執行成功。

`progressive-frozen-v13-regressio-20261024T072706-01f17113` 最終在 v13
read-only namespace 執行 private JSON、simulation、opening revision、
sensitivity、gateway、updates 與真實 Chromium 測試，**577 passed in
138.98s**。這證明候選同版本工程回歸通過；不等於正式 owner 已重新載入、
GPU 常駐暖機、逐筆全歷史 economics 或原生開盤 SLO 通過。

15:30 後獨立公網 `/tw-day-trade/api/status` 讀回 HTTP 200、6 modes、
`simulation_only=true`、`production_order_possible=false`，單次請求 0.642s；
這不是來源到畫面的全鏈路延遲。預設顯示 session 仍 2026-10-22、health
仍 `degraded`：原有 `official_open_price_unavailable` 與 `entry_partial`
沒有被 UI 改成正常。Discord／當沖引擎／gateway 的 native PID 均未改變，
當下 service-sync 六模式 inventory/pending/source-pending 均零且 ledger
integrity ready；這不解除上述資料問題，也不構成候選後端的部署證明。

### Round 19：逐版全鏈路的後續完整方案（2026-10-24 晚間）

本節在本輪 source 修改之前記錄；沿用 Round18 的 ABBA、失敗與來源契約，
不重問已授權的「新價優先／缺價模型參考／紙上已成交庫存再平衡」。
主機已重啟，22:30 讀回三個 native owners 為新 PID、Discord connected，
暖機仍 pending；磁碟約 48 GiB 可用。舊工程結果可以定位，但不是新 owner
已部署或本輪性能的證明。

完整實作／驗收順序：

1. 同一 compatible frozen v13、完整六模式與原來源情境，strict CUDA 後做
   一次 CPU 全流程 profile。沿用已完成 M05/06、dense opening overlay、
   immutable member generation 及單 consumer，避免重做被拒絕的 prefix
   實驗。只依新 profile 決定盤前 prepared-request／純函數 cache 邊界。
2. 盤前備好 official universe、completed data、checkpoint/normalizer、panel
   axis、最近真實成交參考與可重用 request；開盤後只處理有效新 facts。
   每模式保留 full axis／最新輸入優先，真正 input fingerprint 改變才出
   不可變訊號；來源 receipt churn 不重跑，未知缺價不是已確認無首筆。
3. 推論仍服從已驗證 GPU owner；CPU 準備／結果格式化按 profile 分離，
   單一 ledger consumer 與下一模式計算重疊。風險減碼優先、FIFO、費用、
   brackets、未定量目標與 durable intent／fsync／重啟防重複不變。
4. 修 snapshot identity：不把捕獲的舊 revision token 蓋在較新 state 上。
   對追加型讀取先設計 committed-prefix／immutable projection 的驗證方案，
   必須同時處理 append、same-inode rewrite、truncation、rename、失敗 commit，
   不能只移除 generation fence 或加無限重試。從最小可證明的一致讀取開始。
5. Gateway 同 query single-flight/latest-pending、SSE 與 DOM 原位更新沿用；
   檢查六模式相同 opaque signal identity，分開 latest request latency 與
   oldest accepted work age，保留 superseded/failure/retry。完成度分真實
   開盤、新增有效參考、模型就緒、已定量與已成交，不合併成虛假的 100%。
6. focused semantic/fault tests → compatible namespace 回歸 → 固定來源完整
   六模式對照與原生 Chromium → 符合資格的部署／讀回。若整體尚未到一秒，
   保留各階段真實值與剩餘障礙，不把 fixture／cache-hit／休市日當交易日驗收。

一秒驗收同時要求 source 可知 UTC／owner monotonic spans／published model
identity／durable ledger completion／browser observed revision；跨程序不相減
不相關單調時鐘。供應者未發布首筆與缺可成交 book 是外源限制；目前數秒的
CPU 準備、重建、排隊及提交仍是可解障礙，不能以「API 限制」帶過。

本輪 profile 完成後另確認既有正式路徑已記錄 preparation wall spans，
因此不新增重複 decorator／計時路徑。改將 benchmark 與 Discord 的分段
計時收斂到共用 owner，補 owner-thread CPU 的有效性；shared panel 的
builder 會跨 thread 通知 waiter，不能直接相減兩條 thread 的 CPU clock，
更不能把差額一律歸因 provider wait。跨域或不完整的 CPU span 必須為
unknown，wall span 與所有失敗證據保留，後續仍走原單一 JSONL 日誌。

逐版路徑另檢出一項可直接移除的串行相依：訊號已原子發布後，仍持有
GPU model owner lock 去追加 latency JSONL。該寫入不是模型狀態／execution
commit，不應讓下一模式等日誌 I/O。後續只在鎖內捕獲該版 immutable
identity 與鎖內耗時，解鎖後沿用同一記錄器／原有日誌鎖寫入；不跳過寫入、
不增加背景 writer、不放寬 GPU 互斥。先以 fault/lock 測試證明邊界，再納入
相容候選；未有 paired workflow 數據前不宣稱其毫秒改善量。

#### Round19 已實作與可重跑證據

- `build_dashboard_snapshot(require_committed_revision=True)` 核對同一 engine
  run、state/content revision 與最後 service-sync commit。投影前後再查完整
  dev/inode/size/mtime/ctime，拒絕尚未發布、restore-mtime rewrite、atomic
  replacement 或投影中換版。只有 heartbeat 更新不誤判；原 forensic
  stale-view 預設仍保留，不把原本的故障診斷路徑改成偽正常。
- 當沖與隔日沖共用 `_committed_status_response`：以真正取得 query owner
  時的最新版選 cache key，淘汰已過時排隊工作；body 與 engine/source
  token 一致才發布。commit race 最多重試一次，持續換版不 busy-spin，
  失敗不污染 cache。這修正舊 token 搭新 state，**尚不是完整的 committed
  JSONL prefix**；未提交 append 與正常追加的 reader 競態仍列後續工作。
- `OpeningPhaseTimer` 收斂 benchmark／Discord 準備階段計時。wall 使用
  同一程序 perf-counter，CPU 只在同一 Thread owner 且未觀察跨 thread
  progress 時可相減；unknown 保留 `null`，包含同名 phase 的 A→B→A。
  不以 elapsed-minus-thread-CPU 推算 provider wait。完整輸出與 compact
  manifest 均保留 `same_thread_cpu_or_unknown_v1`，原 callback／異常不變。
- 逐版訊號的 latency append 已移到 model lock 外：鎖內先捕獲已發布版次
  與 queue/locked spans，再用原 logger/append lock 記錄。沒有新 writer、
  沒有省略日誌或放寬 GPU runtime 互斥；尚未量化其全流程改善量。

完整同來源 profile run：
`progressive-full-cpu-profile-v13-20261024T143206-7805cb8b`，exit 0；
`analysis_round19/full_cpu_baseline.json`、`.prof`、`.summary.json`
均在 `artifacts/operations/tw_opening_latest_standard_20261022/` 下保留。
相容 frozen v13、strict CUDA、六模式、10 個完整情境、parallel inputs／
overlap single ledger；source/endpoint parity 通過，沒有 provider request、
券商下單或 production ledger 修改。

cProfile 記錄約 9.81 億 calls／861.50 秒；27 次 `atomic_write_json` 的
累積時間 454.30 秒受 Python JSON encoder 探針嚴重放大。部分 Torch 切換
區段出現 self time 大於 cumulative 或 forward cumulative=0，且 worker
thread 不在這份 CPU 歸屬範圍，不能當精準 CPU breakdown。profile 的前兩
情境 durable completion 11,547.64／8,910.67 ms **不是未剖析的原生速度**，
也不能與 Round18 ABBA 宣稱回退／進步。後續沿新分段 clock 定位剩餘工作。

測試與失敗證據：

- snapshot tests 先 red 17；green 初次 4 個失敗是測試錯用被公開 sanitizer
  刪除的 private signal ID，已改驗公開 opaque key，沒有放寬隱私。
  擴大回歸初次 519 passed／1 failed 是舊假 fixture 缺 commit identity，
  已補正；相容 v14 namespace 最終 520 passed in 79.50s。
- 跨 thread CPU 計時 2 red；實作後 preparation／benchmark／Discord
  197 passed in 17.85s。compact manifest 另補 clock contract 與 null 測試。
- 相容 v15 namespace run
  `progressive-frozen-v15-verified-20261024T150804-a64194a6`：12 個相關測試
  檔 **900 passed in 248.80s**，含真實 Chromium、simulation、FIFO/revision、
  session projection、tail、private JSON 與 gateway。首個 v15 run 因兩個
  測試檔路徑拼錯而 zero tests／exit 4，保留原失敗，修正名稱後完整重跑。
- telemetry-lock 測試先 red，證實 logger 還持鎖；移出後
  `progressive-telemetry-lock-green-20261024T151250-4126870e`：198 passed
  in 24.16s。最終 v16 namespace run
  `progressive-frozen-v16-regressio-20261024T151456-f7066682`：同樣 12 檔
  **901 passed in 163.44s，exit 0**，包含新的 telemetry-lock 測試。
  log 內個別 CLI parser error 是預期拒絕測試，非 native 執行失敗。
  這是相關契約的整合回歸，不是整庫全測試或全歷史 economics 驗收。

#### Round19 原生／公網讀回與一秒目標缺口

`analysis_round19/native_public_readback.json` 與
`analysis_round19/native_loopback_readback.json` 分別由 canonical dashboard
benchmark 量測，目前 native backend，**不是候選 v16 A/B**。每路一個
first-observed 加四個 samples，HTTP errors=0；小樣本 p95 不作 SLA。

| 路由 | 本機直連 median | 本機發出的公網 median | 公網應用 Server-Timing median |
| --- | ---: | ---: | ---: |
| revision | 4.58 ms | 1,339.10 ms | 2.38 ms |
| status | 41.83 ms | 2,359.89 ms | 3.41 ms |

公網 revision first-observed 曾 12,047.01 ms，應用 timing 2.02 ms；status
first-observed 2,337.17 ms。另一次 curl 觀察 HTTP/2、CF SJC、總耗時
1,375.38 ms／應用 1.70 ms。只能確認該次 public/client/transport 路徑在
應用 timing 之外有明顯成本，不能由此斷言所有使用者／所有 CF PoP 一樣慢，
也不能把 Server-Timing 之外全部歸因某單一設備。當下 CPU／I/O／memory
pressure 明顯，亦有其他下載／測試在跑；沒有停止它們來造出漂亮數字。

23:14 後獨立公網 status 仍 session 2026-10-22、6 modes、`degraded`，
`simulation_only=true`／`production_order_possible=false`，Discord/engine
revision synchronized。三個 native service PID 仍 1988／161／1989；沒有
重啟。read-only `native_reload_plan.json` 明確 blocked：
`startup inference warmup is not idle/ready`。該 owner 本次 run 暖機 pending，
舊 run ready receipt 不可挪用；休市／off-hours defer 與服務斷線不同。

| 剩餘步驟 | 分類與下一個可驗收改動 |
| --- | --- |
| 完整輸入準備／Factorized、MGDA 重建 | 工程障礙；沿 canonical source/normalized input owner 做盤前可重用準備與增量，保留外部 manifest/member race fence。 |
| 六模式 serialized runtime／帳本提交 | 工程障礙；只分離已證明不需要 model lock 的工作。commit 合併需先有 durable intent、crash/restart、FIFO／費用等值，不可拿掉 fsync。 |
| append 中 history／snapshot reader | 工程障礙；實作 committed prefix 或 immutable projection，完整驗證 append/rewrite/truncate/rename/failed commit，不移除 source guards。 |
| receipt→SSE→status→paint 的往返 | 工程與網路混合；保留 query coalescing、最新 accepted identity 與 paint observer；評估同一安全 DTO 的推送以少一次 RTT，不能直接推 private state。 |
| 真實首筆／最新可成交 book／對手流動性 | 外源限制；carry 僅補模型 reference。未發布、未知、無量不能偽裝成開盤或完成成交。 |
| native readiness／一秒驗收 | 尚未完成；同版暖機、合格 reload、下一個官方交易日 native 來源→六模式→durable fill→browser readback 是分開的必驗項。 |

最後候選 v16（仍 `prepared_not_adopted`）：
`artifacts/operations/service_code_releases/20261024T151259Z-38c8fa335470/release.json`，
receipt SHA `6054ca1d9b0e9fcd3b6cd36e37132063b597e929013e54becbdf883c0397fcde`，
source SHA `4470f6a5cb6afe53e0dda9d62eaf51718545a882a980de75faa0309e756a0b81`；
v14 只 overlay 共用 phase clock／benchmark／bot，再由 v15 只 overlay
telemetry unlock，保留相容 model ABI，不混入其他工作的 v4 改動。
正式六模式仍 `opening_once_v1`，本輪**沒有啟用 progressive 或修改帳本**。
完整一秒與所有障礙解除均未達成，工作保持 active。

## Round20：四條剩餘路徑的完整修改與驗收計畫（2026-10-24）

本輪延續同一工作，不改模型、交易規則、來源資格或原始帳本。成功條件仍是六模式
同版、完整輸入與輸出、durable paper commit、正確一致讀取及瀏覽器可見；局部命中
快取、HTTP 200 或離線測試不能宣稱一秒開盤成功。

| 路徑 | 可解除的工程障礙／擬實作 | 不可省略的證明 |
| --- | --- | --- |
| Factorized／MGDA 輸入 | 在既有不可變原始 packet owner 重用有界 CUDA raw bytes；不快取模型 activation，不改 full-shape FP32 Candle Encoder。容量不足維持原搬運路徑 | 完整輸入與模型輸出位元 oracle；來源、行／股票排列、overlay、device／stream、可變來源、清除與容量負例；六模式完整 paired workflow |
| 帳本持久化 | 同一 `_persist` 的同目錄替換可共享最後的 directory fsync；每一檔仍先 file fsync，state 與 commit receipt 的恢復順序不變，錯誤不發布成功指標 | 短寫／fsync／replace 失敗、順序、重啟與既有 FIFO／費用／完整帳本測試；逐檔和總 commit 時間 |
| 追加一致讀取 | 先讓 durable commit 明確綁定 journal generation，嚴格互動讀取不得把 uncommitted append 冒充同一 revision；有界重試並保留舊驗證畫面。真正 committed-prefix／append delta 必須涵蓋所有 writer／修復與同 inode rewrite，不能只相信檔案長大 | append-before-state、append-during-read、rewrite／replace、跨模式、舊 receipt 相容與 failed build 不入 cache；是否仍全 prefix hash 必須照實列出 |
| 公網傳輸 | SSE 直接送同一已驗證且已公開去敏的 compact status，消除 revision 後額外 GET 的一輪往返；歷史日期及 detail API 保留各自 scope，慢 client 不持鎖 | 公開／owner 權限分離、同 revision／date、過期事件不倒退、斷線／visibility／poll fallback、實際 Chromium 六模式 paint、body bytes 與實測網路時間 |

執行順序：先確認 canonical owners 和資源容量，再分別實作及 fault tests；以 v16
為相容基線，只 overlay 本輪人工審核檔案，避開其他工作中的模型 ABI v4。候選
通過 focused 與共享 regression 後才跑完整六模式 A/B；沒有實際完成的路徑保留
為 pending，不以 microbenchmark 取代全鏈路。原始 JSON／profiler／負結果留在
`artifacts/operations/tw_opening_latest_standard_20261022/analysis_round20/`。

外部限制分開記錄：開盤首筆尚未發生、provider 發布／配額、WAN／TLS／路由 RTT
不是本機算力；但不把重複搬運、重算、fsync 次數、讀取競態或額外網路往返歸為
外部限制。每條優化都需留下 before／after，任何未量到的收益不下結論。

### 傳輸計畫修正：逐事件 flush 的可協商壓縮（2026-10-25）

v21 已在真實 Chromium fixture 證明六模式逐版更新不需額外 status GET，但現有
native status 約 285,426 bytes，普通 gzip 約 45,633 bytes。直接推 identity JSON
雖省一次 RTT，卻增加約 6.25 倍 payload，不能推論慢速公網一定較快。因此下一個
候選只對已 opt-in 的 `status=1`、且 HTTP Accept-Encoding 允許 gzip 的連線測試
串流 gzip：revision、status、heartbeat 每一事件都 `Z_SYNC_FLUSH`，不得等累積
buffer 或 EOF；舊式純通知 SSE、不接受 gzip 的連線保留 identity。先測原始 socket
在 status builder 未完成時已解出第一事件、壓縮流終止與 framing、encoding 拒絕／
fallback，再用相同六模式 Chromium／paint／無額外 GET oracle 驗收。這不是既有
buffered whole-response gzip，亦不保證外部代理不 buffering；部署後須另驗 CF 路徑。

### 已實作的 owner 與 correctness 邊界

1. `factorized_panel._BlockCache` 延伸既有 prepared-host-packet owner，使用弱參照綁定
   原始 packet 身分；只有 private immutable inference、完整原始 bytes 可留在 CUDA。
   每 owner 跨 devices 上限 `min(host budget, 1 GiB)`，另留 1 GiB 可用 headroom；不足
   時沿用完整搬運，不能縮 feature/window。producer event、consumer wait/record_stream、
   host eviction、source/window/coordinates、clear/retire 都有負例；沒有快取 activation。
   `signal_engine.clear_live_panel_memory_cache` 同時退役 packet，不能讓舊 owner 復活。
2. 同一 `_persist` 的 state/positions/status 仍各自 file-fsync、replace；同目錄 namespace
   共用一次 directory-fsync，之後才 file-fsync/replace/directory-fsync commit marker。
   barrier 從 4 file + 4 directory 變 4 file + 2 directory，不等於取消 durability。
   每檔 encode/write/replace、group barrier、fingerprint 和完整 commit 時間仍有 telemetry。
3. `service_sync` 明確綁定六個 financial journals 與 state/positions/status metadata generation。
   status 嚴格 reader 與兩頁的 signals/positions/events/history 共用前後 commit/source fence；
   最多兩次 retry，過期 key 重新綁定，失敗不入 cache。沒有 history/model lock 卡住 writer，
   舊 receipt 保留舊 diagnostic 語意，不被授予新證明。metadata fence **不是** prefix integrity
   proof，因此原全 prefix hashing、same-inode rewrite/truncate/replace 檢查未刪除。
4. SSE 先推 compact hint，再推既有 public/member/owner scoped sanitizer 產生的同一 compact
   status DTO；前後 revision/engine/content identity 必須相同。append-before-commit race
   留在同一 stream 等下一個 ready event，不因正常競態重連；真正斷線仍有 polling。
   只在成功送出後 acknowledge material identity，heartbeat 不重建／重送整份 status。
   JS 先協商再 opt-in，舊 backend 繼續 revision+GET；推送與 HTTP 使用同一 renderer，歷史
   日期不被當日 push 蓋掉，舊 stream、舊 revision 不能倒退畫面。details 仍各自正確查詢。

file fsync 不保證 directory entry 已落盤，依 [Linux fsync manual](https://man7.org/linux/man-pages/man2/fsync.2.html)
保留 group/marker 的 namespace barriers。CUDA event/order/allocator lifetime 與本機 Torch
2.13.0 的 `record_stream`、`Stream.wait_event` docstring 對照並以實際 GPU 負例驗證；不是
只看 `nvidia-smi`。SSE 多行 `data` 與 UTF-8 framing 依 [HTML EventSource standard](https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream)
處理，包括 JSON 最後的 LF，不能假定每事件只有三個實體行。

### 完整固定來源六模式 ABBA：保留正反結果

canonical workflow run `progressive-r20-full-abba-20261024T155219-030e0349` 已完成。
v16(A) 與 v17(B) 各兩輪，持續持有同一 canonical source read leases，strict CUDA preflight
通過；沒有停止其他下載／測試，沒有 provider 請求、正式帳本寫入或服務重啟。
六模式、十個 source cutoff/repeat scenarios、完整 weights/artifacts、原 snapshot clocks、
獨立單一模擬 ledger owner 全保留。latest v21/v22 的額外 reader/SSE 修正 **不在這組**
模型 ABBA 中，不可將不同候選冒充同一版本。

| 完整 workflow（含十情境與 artifacts） | A1 | B1 | B2 | A2 |
| --- | ---: | ---: | ---: | ---: |
| 秒 | 391.90 | 392.28 | 372.35 | 435.69 |
| warm raw packet H2D bytes／次 | 514,640,064 | 0 | 0 | 514,640,064 |
| warm reconstruct enqueue median ms | 174.83 | 113.54 | 130.56 | 294.23 |

兩個 Factorized-family 模式的完整 logical dense input 始終 **5,196,746,496 bytes**；B
命中 22 raw packets、resident 514,640,064 bytes。H2D=0 只指這批不可變 raw packets，
不含 overlay/mask/其他搬運。enqueue timing 不是 synchronized CUDA kernel duration。

四組 pair 的完整 model/target weights 及 comparator 明列的 requested/filled/pending 等
數量一致；這**沒有**證明所有 financial recurrence/fees/P&L 逐項相同，亦非真實成交。
部分 source-cutoff 的 synthetic books 根本無可執行價格，仍忠實 `waiting_source`，不把
完成模型 reference 說成全部成交。離線模型+durable paper commit 的 B 場景約 2.40～8.83 s，
尚未一秒；A1→B1 有多個場景變慢。兩輪均值的 +5s cutoff 甚至 **慢 16.00%**、+11s
**慢 6.64%**；原始 case 與其他改善全部留存，不能從有雜訊的兩輪挑選最快值當穩定收益。
state publication individual median A1/B1/B2/A2 分別 127.88/160.45/143.04/192.94 ms，
directory barrier 約百分之幾毫秒，主要仍是完整 state encode/fingerprint 與每模式提交；
本輪沒有證明 group barrier 本身帶來可辨識的總體改善。

證據（均為 Git 外 generated/private outputs）：

- `artifacts/operations/tw_opening_latest_standard_20261022/analysis_round20/remaining_paths_full_abba.json`
- 同目錄 `remaining_paths_{a1,b1,b2,a2}.json`（每檔約 583 MiB）及 `.summary.json`，附完整檔 SHA。
- 同目錄 `remaining_paths_metrics.json`，包含每個 cutoff 的兩輪原始值、平均與負改善。

### v21 共享回歸與真正瀏覽器：不是 native 開盤 SLO

`progressive-v21-regression-20261024T162412-a80ec182`：**953 Python passed / 126.30 s**，
**41 Node passed**。13 個 Python modules 涵蓋 private JSON、完整 Factorized GPU/input
bits、simulation FIFO/fees/recovery、durability faults、JSONL append/rewrite、session
projection、revision/rebalance、source sensitivity、gateway/security/audience、SSE、
Chromium、execution/Discord timing 與 benchmark；不是 repository 全測試通過。
tests/static 沒在 Python source namespace 內，另做本輪前後逐檔 SHA，結果 unchanged。
v17 focused 曾 326 pass + 1 HTML asset hash fail、v17 broad 945 pass + 1 core version
assert fail，皆修正；v20 952 pass + 1 SSE test framing fail（JSON 最後 LF 形成多個 data
行），改用正確 EventSource parser 後通過。這些失敗紀錄保留，不改寫成最初即全綠。

真 Chromium 使用六個工程模式、temporary canonical ledger、真 gateway/file watcher/SSE
及原 app，測三輪完整 revision/rebalance，API 公開價格 redaction 未停用：

| revision | 六模式 register+persist | 最後 commit→觀察 paint | 首次 register→觀察 paint |
| --- | ---: | ---: | ---: |
| 1 | 79.94 ms | 176.04 ms | 255.99 ms |
| 2 | 235.18 ms | 327.47 ms | 562.65 ms |
| 3 | 253.86 ms | 253.82 ms | 507.68 ms |

initial status GET=1，revision status GET=**0**，navigation 始終 1、30 paint metrics。
21 個 transient source errors 仍記錄，沒有把 append 中的 generation 檢查取消；最終
全部更新恢復。已人工看 phone 與 full-page cards screenshots，phone/tablet/laptop/
desktop width assertions 通過。此 fixture 的 synthetic books 很小，不能外推 full universe
模型／正式帳本／WAN。JSON 與六張 screenshots 已 copy+readback 留在
`analysis_round20/browser_v21/`，原始 pytest 路徑為 `pytest-53`。

### 00:26 native 公網 transport baseline（仍是舊 backend）

使用現有 canonical `benchmark_dashboard_latency.py`，每 route first-observed + 2 samples，
未改 production cache 或 restart。只是本機發往公網的該次小樣本，不是外部多地 SLO：

| route | loopback median | public median | public app Server-Timing median |
| --- | ---: | ---: | ---: |
| revision | 12.71 ms | 395.14 ms | 1.99 ms |
| status | 12.86 ms | 442.32 ms | 4.13 ms |

所有樣本 HTTP200/JSON-valid；public first status 1,130.20 ms。應用外路徑仍明顯，但
未單獨定位 DNS/TLS/client/router/tunnel/PoP 各項成本；不得歸咎單一 CF 設備。新候選
尚未部署，這是 native baseline，不是新 SSE 路徑的 after 數字。完整 evidence：
`analysis_round20/native_{local,public}_transport_baseline.json`。

### 後續剩餘關鍵路徑：不把設計當成完成

| 優先 | 仍有的實際成本／風險 | 下一個設計與驗收 |
| --- | --- | --- |
| 1 | input reconstruction、quote 後 prepare、format、逐模式 durable state；完整 B 尚 2.40～8.83 s | 沿既有準備 owner 再拆可盤前固定的日曆／eligibility／lag-1 alignment／開盤 overlay；分辨 cumulative queue clock 與自身 work，不直接相加。可共享 full-shape materialization 需先證明兩模型 coordinates/overlay 一致、無 in-place mutation、VRAM headroom與跨 stream lifetime，否則保留 raw packet 路徑。 |
| 2 | state/fingerprint 全量 encode；每一模式各自 commit | 用 canonical journal 的 durable intent/commit batch 或不可變 state shards 減少重編碼；先獨立對照每筆 fills、FIFO/cohorts、fees/taxes、realized/unrealized NAV、restart recovery/idempotency與逐版 visibility。不可只把六個 fsync 改成 deferred 或未落盤 ACK。 |
| 3 | 追加 JSONL 舊 prefix hashing，並非真正 O(delta) | canonical writers 發布 immutable committed chunks / generation manifest，明確 commit byte-boundary、SHA、parent lineage、tombstones及修復 replacement；逐一覆蓋 simulation、minute repair、replay/rebuild與promotion writers，再讓 reader 沿 manifest＋delta 讀取。未遷移/不可信 writer 留完整 prefix fallback；不得用 inode+growth 冒充不可變證明。 |
| 4 | commit rename→final directory-fsync 的 runtime notification 視窗 | 本輪 guard 防混讀，不等於讀者已證明收到最終 durability ACK。若要求這一更強證明，需 canonical writer/runtime ACK 或精確短 commit pin，不能把完整 history/model read 放進 writer lock；測最後 marker fsync 失敗與每個 crash boundary。 |
| 5 | WAN/TLS/PoP、identity SSE payload 與 details 多個 GET | 每事件 flush 的可協商 gzip 先做 raw socket+Chromium；再驗部署後 CF gzip/identity、first useful frame、same-revision six-mode paint、斷線恢復、低頻寬／高 RTT。details 仍需檢查 cache/source identity，不用混版資料省流量。 |
| 6 | native 同版 readiness / 真開盤 SLO | 確認 compatible frozen release、六模式同版 warmup/engine ACK後走 qualified reload；保留原帳本與 opening_once，progressive另做完整 native acceptance 才能啟用。下一官方交易日以原始源時鐘觀察0～100%、所有有用訊號/可執行成交及公網 paint，未實測的市場不能算零延遲。 |

00:26 只讀 runtime evidence：Discord `discord_connected=true`、`core_health=ready`，
同 run ACK engine revision 68581；native warmup 仍 `pending`、markets 空（off-hours defer），
不可借用別的 run 的 ready。wrapper/gateway PIDs 1988/161/1989 未更換，6 modes、
`simulation_only=true`、`production_order_possible=false`。正式設定依然 opening_once；
本輪沒有採用新後端 code release、重啟、改正式帳本或啟用 progressive。前端靜態檔由
既有 gateway 即時讀取，已更新且保留舊後端相容，不能把它說成後端推送已部署。
這是分階段工程結果，**所有瓶頸解除與
一秒 native end-to-end 均未完成**；原工作保持 active，不以測試通過關閉。

### 最後候選 v23：壓縮／損壞 receipt 負例與部署分界

v22 新增 six socket cases：TW/overnight × gzip / explicit gzip;q=0 identity /
identity;q=0+gzip。status builder 故意未就緒，原始 socket 已能解出 revision，完整流
有 gzip trailer、完整 UTF-8/JSON bytes readback；不是等 EOF 才一次解壓。真 Chromium
也要求 `Content-Encoding=gzip`，三輪逐版更新不重載、額外 status GET=0。
第一次 broad 為 958 pass + 1 test race：cache-ready 通知可比手動 heartbeat 先入佇列，
測試不應假設 publish 與 frame 一對一。改為有界等候**相同新 revision**、仍禁止
中途重送 status，未放寬 source identity 後，v22 **959 passed / 136.63 s**。

再補 v23：已存在但損壞／非 object／不支援 schema 的 commit marker，不能因 loader
回 None 就降級為 legacy 不加 fence 的讀取；schema 轉換錯誤回 unavailable。兩頁共
10 個 malformed-marker cases 驗證 builder 未執行、失敗不快取；真正不存在的歷史 marker
及有效舊格式依舊保留 diagnostic 相容。

最終 canonical workflow `progressive-v23-final-regression-20261024T164641-321a34a8`：
**969 Python passed / 124.83 s，41 Node passed / 0 failed**。同一 13 modules、相同 full
GPU/accounting/source/API/browser boundaries；tests/static 前後 SHA unchanged，六個
已審核 workspace source members 與 frozen namespace SHA 一致。final fixture 使用自己
新建且確認為空的 basetemp，不讓其他 pytest cleanup 回收正在保留的 screenshots；
readback/copy receipt 與最新瀏覽器結果在 `analysis_round20/round20_acceptance_v23.json`
及 `analysis_round20/browser_v23/`。
該 v23 gzip fixture 的三輪 register→observed paint 為 **308.43 / 355.79 / 620.72 ms**，
其中最後 commit→paint 為 240.16 / 186.39 / 480.26 ms；initial GET=1、revision GET=0、
navigation=1。16 個 transient source errors 如實保留並恢復；仍僅是小型 synthetic
ledger→loopback gateway→真 Chromium，不是 source/model/WAN 開盤秒數。

對**實際已公開** native 285,425-byte status 做獨立表示層測試：revision+status identity
287,137 bytes → level1 `Z_SYNC_FLUSH` gzip **51,251 bytes**（少約82.15%）；第一 revision
681 bytes 在 EOF 前可解，完整 bytes 逐事件 readback 一致。5 次 compress+independent
decode median 2.84 ms，範圍 2.43～12.02 ms；這不是 public socket latency，也不含
來源／模型／帳本。證據 `analysis_round20/native_status_stream_representation.json`。

最後候選仍 **prepared_not_adopted**：
`artifacts/operations/service_code_releases/20261024T164316Z-99bcb30d4ff6/release.json`，
receipt SHA `010960e6224988b0f423c73e88468f983447dea6b89046aeabf5b6ab178ae4f9`，
source SHA `866dba646a572570902ddadc9f8b9a550083e1b7aaa90df6a512c1b449c6731a`。
只從相容基線 overlay 已審核 source，沒有把別人正在改的 model ABI v4 一起凍結上線。
全模型 ABBA 測的是 v17；v23 額外 read guards/gzip 的完整 workflow timing 尚未重新 A/B，
不得挪用 v17 timing 為 v23 全鏈路效能證明。

Native loopback readback 已讀到 app SHA
`eaaba7056d8d0bfb314464273ceee17fbb38778661d633112e2f42fd456e2453`，但 SSE hint
沒有新 `status_stream_contract`，證明後端仍舊版。前端先 negotiation，故維持普通
revision+GET，不會盲送不支援 query。公網 requests session 亦 HTTP200、app SHA 完全
一致，status HTTP200；一次 default urllib probe 403 仍保留，沒有放寬 WAF/security
規則來換成功數字，不能宣稱所有 user agents 都無阻礙。
分別留存 `analysis_round20/native_static_backend_readback.json` 與
`analysis_round20/public_static_readback.json`。production ledger writes、broker orders、
native service restarts 皆 0；正式 opening_once 與 native warmup/部署 gate 未繞過。

## Round 21：剩餘四條路徑的計畫與實作邊界（2026-10-25）

承接 Round 20 v23，不重做已通過的 raw CUDA packet／SSE gzip 實作；本輪不把
workspace 其他模型 ABI 變更一起部署。要求仍為完整六模式 source → signal →
durable paper execution → browser paint；一秒是尚未達成的整鏈目標。

| 路徑 | 可解的障礙與順序 | 不可省略的驗收／外部限制 |
| --- | --- | --- |
| Factorized／MGDA input | 先重用已驗收 514,640,064-byte immutable raw packets，再量剩餘 full-width reconstruction、quote overlay、格式化；能盤前固定的來源／座標／日曆／eligibility 留在既有 owner。共享 dense base 必須先證明相同完整輸入、無模型 in-place mutation及跨 stream lifetime，否則不直接合併。 | 完整 5,196,746,496-byte logical FP32 input、全部權重與模型 ABI 保留；來源發布及首筆真成交是外部約束，現有 Python／重建成本不是物理極限。 |
| 帳本持久化 | 先修過時測速器：原 whitespace patch 不會改變現用 native encoder，而且新 projection file generations 需先驗實體身分再排除。接著在同一 `_persist` owner 減少 dashboard-only fingerprint 的重複 JSON 成本，保留完整 state／positions／status／commit 與所有 barriers。更大改動才考慮 immutable state parts／bounded commit batching。 | 全金融 state、fills/FIFO/cohorts、fees/taxes、NAV、restart/idempotency；UI invalidation hash 版本可遷移，但不得改 canonical artifact/source hash。公開前不得把未 fsync 的版本當已成交。 |
| 追加一致讀取 | 先補 final commit directory-fsync → reader-visible ACK 的證明；marker rename 與最後 fsync 之間不能誤報完成。再在 canonical append owner 做 immutable chunks／parent lineage／commit boundaries；未知或修復 writer 必須失效並回完整 prefix 驗證，不能只看 inode+size。 | 每個 crash boundary 與 failed commit 都 fail closed；不持有完整查詢期間的 writer lock；所有 repair/replay writer 覆蓋前不宣稱 O(delta)。 |
| 公網傳輸 | 已完成候選的逐事件 flush、協商 gzip、同版 push 與 shared renderer；下一步在同版 native warmup/ACK 合格後才採用後端，量 gzip／identity first useful frame、六模式 paint、斷線恢復與 WAN。 | 同機公網測速不等於外部多地探測；既有 WAF 不放寬。舊 native backend 沒有新 contract 時前端仍走相容 GET，不虛報已部署 push。 |

先完成有界 CPU publication／read consistency 修正及 fault/regression 驗收，再選新的
全六模式固定來源 A/B；不把局部 micro、synthetic browser 或舊 v17 ABBA 的數字
冒充這輪 native opening 成績。原 task 保持 active，未改正式帳本或券商下單。

### Round 21 已實作：完整 publication 與提交後讀者 ACK

1. `_dashboard_material_fingerprint` 使用既有 guarded lossless `ledger_bytes`，
   material contract 升為 `dashboard_material_without_private_order_templates_v2`。
   它只是 private UI invalidation，不是 canonical source/artifact hash。保留全 state、
   金融數值、完整 positions/status、六種 journal 與全部 durability barriers。
2. `_publish_service_sync` 在 marker 自身的 **file fsync + directory fsync 成功返回後**，
   才發布 `service_sync_durable_ack.json`。讀者比對 unique commit id、run/revisions
   與 marker 的 dev/inode/size/mtime/ctime；缺失、損壞、舊 nonce/metadata 都不能
   承認新提交。ACK 是 admission hint 而非第二份金融帳本；ACK 自身 file fsync，
   不額外同步目錄，斷電遺失時 fail closed，下一次 canonical commit/heartbeat 恢復。
   含 ACK 共 5 個 file fsync、2 個 directory barriers；TW/overnight watcher 也觀察 ACK。
3. `signal_engine` 保留原 `pre_inference_prepare_ms`（含 prefetched quote age/queue），
   另記 `quote_age_before_local_prepare_ms`、`post_quote_local_prepare_ms` 與六個互不
   重疊的 local substeps；不從端到端 SLO 減掉等待或重新標記來源時鐘。
4. 固定來源測速新增 `complete_generated_paper_state_and_six_journals_v1`：獨立讀回
   完整 persisted state 與全部六種 journal，保留順序/physical lines/unknown 欄位。
   只正規化 isolated temp-root，排除明列的非金融 run UUID/private UI hash；
   保留 fills/FIFO/cohorts、費稅、損益、數值與決策時鐘。oracle 成本另列，仍包含在
   full worker/workflow wall。它不證明未觀察的全日交易經濟或 native 開盤速度。

`progressive-r21-publication-pair-20261025T033010-68b5de5a` 成功；
`analysis_round21/persistence_current_state.json`：同一份 5,766,370-byte state，
SHA `67891e714dd8da65523c902c44cee8ffa78c4f334b9a227830b0bd6d34dabcbd`。
6 輪交錯、共 12 個未 profile 樣本；兩組都用相同新 ACK 與完整金融 publication。

| 同一 `_persist` 邊界 | stdlib UI fingerprint | guarded native UI fingerprint |
| --- | ---: | ---: |
| 完整 publication median | 246.13 ms | 193.18 ms |
| fingerprint median | 53.03 ms | 27.86 ms |
| 完整 state encode median | 60.45 ms | 59.94 ms |
| 完整 publication range | 213.70–328.87 ms | 167.27–392.43 ms |

完整 publication median 減少約 **21.5%**，保留 native 最差樣本較慢的事實。
四份 decoded views 獨立金融契約一致，先驗真實 file generations/ACK，才排除
機器身分欄位；來源 state 未修改、journal writes=0。這不是 v23→新 ACK 協議的
純 before/after，也不是 model/source/WAN 或 native 一秒驗收。

### Round 21 驗收版本邊界與保留失敗

`progressive-r21-persistence-unit-20261025T032854-1dc8560c`：34 passed、
233 deselected（4.69 s），只是選定 persistence/fingerprint/ACK/heartbeat cases。
v25 frozen broad 收集遇 2 errors（31.01 s）：同時開發的 gateway 已引用新增
`tick_inventory`，selective bundle 卻未包含它。保留 `analysis_round21/regression_v25.json`，
沒有改成 skip 或 fallback 掩蓋。v26 補了 Tick module 與 monitor summary/provider
依賴，結果 1,236 passed / 5 failed（112.55 s）：4 項缺 `parquet_footer_count` 依賴、
1 項 history cache 單元 fixture 使用正式隔日沖目錄、撞上正常進行中的 commit。
後者改為 isolated `tmp_path`，不放寬 production generation guard。

v27 A/B 同時補上已審核的唯讀 footer helper 閉包；保留 workspace 其他工作，
不升級模型 ABI、不增 provider requests。gateway 仍含同步開發的 Tick route 差異，
該 route **不在模型/帳本 A/B 計時中**，不得把它的效能歸給本輪改動。
broader regression、完整 financial parity、native 同版 warmup/qualified adoption
仍需各自驗收；失敗 evidence 保留，不以局部測試通過關閉原 task。
