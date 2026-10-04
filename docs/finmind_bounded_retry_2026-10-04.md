# FinMind 有界重試、保留歷史與排程修正

2026-10-04，台北時間。此報告處理下載任務的失敗生命週期，不把停止重試
當作資料已抓齊，也不判定 FinMind 已永久停止提供該資料。

## 結果與仍缺的資料

131 個 `UKStockPrice / unexpected_empty_after_nonempty` 已退出自動重試，
不再占用追新預留或自動下載 ETA。307,144 筆舊資料、收據、Parquet 雜湊、
筆數及原查詢時間均驗證保留。不是本機歷史完全空白：這些股票原本有資料，
但後續全歷史查詢反而回空，因此不能拿空回覆覆蓋舊值。

兩個下載 owner 共用預設 **5 次連續任務失敗** 的上限；可修改
[`configs/finmind_retry_policy.json`](../configs/finmind_retry_policy.json)。
新任務達上限後持久記錄 `retry_exhausted`，重啟、重新 seed 或其他股票恢復
都不能自動復活它。歷史任務已超過上限者直接退場，不額外再花五次呼叫。

| 驗收項目 | 結果 |
| --- | --- |
| 英股耗盡任務 | 131，原錯誤碼保留；不增加 completed 數 |
| 既有逐任務失敗證據 | 每檔 46–70 次，共 8,521 次；不是由群組探測總數推算 |
| 保留資料 | 307,144 列、4,634,663 bytes；原值日期合計涵蓋 2000-01-03 至 2026-09-29 |
| 自動群組探測 | `active=false`、`next_probe_at_utc=null`；舊探測紀錄保留 |
| 私有 queue 遷移模擬 | 約 0.440 秒；完整原值／收據路徑／日期／最後嘗試／錯誤碼未變 |
| 遷移對工作量的影響 | 可自動排程 -131、待重試 -131、耗盡 +131、完成 +0 |
| 廣泛 Python 回歸 | 1,126 passed；最後政策與 UI 聚焦驗證 73 passed，為上述測試的子集 |
| 共用瀏覽器行為測試 | 38 passed；不是額外下載或資料完整性證明 |
| 公開面板 | 1440／1024／390 寬度驗證；零 JS 錯誤、零水平溢出、零瀏覽器 provider 呼叫 |
| 服務邊界 | 僅重啟 Complement、Sponsor、公開 gateway；Free、Shioaji 與兩個股票模擬交易服務 invocation／重啟數未變 |

資料日期是這 131 份保留資料的合計範圍，不表示每檔自 2000 年都有完整歷史。
它們目前仍缺來源恢復／最新版本驗證；**停止自動重試不是修好來源資料**。

## 第一性原理：把工作、資料、流量與完成分開

每個請求至少有四個不同結果：請求確實送出、回覆通過驗證、資料持久保存、
對應目標歷史已完整。它們不能由同一個「成功／失敗」或進度百分比代替。
本輪先核對共同流量帳本、主責 queue、逐任務觀測、收據、ETA 快照及唯讀投影，
再修共享狀態，沒有新建平行下載管線。

部署前 01:07–01:37 UTC 的 30 分鐘共有 2,725 次請求開始：英股、日股、
美股各 908 次，新聞 1 次。此窗口主要是正常海外日資料追新／修訂檢查，
不能把英股的 908 次全部說成這 131 檔的失敗重試。

當時沒有逐任務的持久重試上限。群組每分鐘換 canary 能減少同源重複失敗，
但沒有退出條件；已失敗數十次的任務仍留在自動工作、預留和 ETA 裡。
因此修正的是有限工作生命週期，而不是任意加速失敗重送或刪掉失敗資料。

## 共用實作與健壯性

[`downloader/finmind_retry_policy.py`](../downloader/finmind_retry_policy.py)
由既有 Complement／Sponsor 初始化及結果儲存入口共用。

- `finmind_task_retries` 持久保存逐任務、逐 cycle 的失敗次數與真實時間。
  相同嘗試時間重複保存不重算；通過原有驗證的成功結果才能重設計數。
- `finmind_retry_events` 保存 `legacy_import`、`retry_exhausted`、`recovered`
  與 `operator_reopened`。耗盡事件保留退出前的完整 queue metadata。
- 只有工作失敗扣次數。限流、IP 冷卻、金鑰／權限等待及
  `local_traffic_busy`／`local_traffic_unavailable` 不消耗個別任務的上限。
  本機衍生資料等待父資料也不扣次數；原本終止性的錯誤仍循原分類。
- 一次性遷移逐資料集按實際觀測時間串流計數，複雜度為 O(觀測＋任務)，
  不為每檔重掃全部失敗觀測，也不掃歷史 Parquet。
- 舊觀測 sidecar 損壞時，丟棄不完整計數，只承認 queue 可證明的最後一次
  失敗並留下不含敏感文字的警告；不因此停止健康 owner。
- 離開群組的 canary 會輪替到仍可重試的同伴；全部耗盡時關閉 gate，
  不讓失效 canary 阻塞其他工作。一次成功也不能復活已耗盡的其他代號。

上限計的是「任務失敗」，不是五個實體 HTTP calls。原有 HTTP 重試、批次
退回較小合法範圍及帳號共用限制仍分別運作，不能混用分母。

## 排程、日曆與面板連動

耗盡任務從自動 dispatch、發布窗口預留、背景 frontier 的未完成等待及
ETA 可自動請求數排除，但仍留在全資料目標與缺口清冊。面板顯示
「重試耗盡，舊資料保留」及保留筆數；只有耗盡缺口時顯示待人工修復，
不報「零分鐘後全部完成」。單一耗盡子集也不停止其他健康代號追新。

dispatch 合約 v7、ETA 快照 v9、公開投影 v9 已驗證一致；舊快照仍依相容
版本與 freshness 守門。瀏覽器只讀 metadata，不帶金鑰、不控制服務或呼叫
FinMind API。全來源清冊也列出耗盡數，不把任務從網站悄悄隱藏。

原有交易日／非觀測日排除、發布窗口追新最高優先、共用配額及九階段歷史
順序不變；美股分鐘 K 仍最後。台股休市不能直接當作海外市場也休市。
[美股官方文件](https://finmind.github.io/tutor/UnitedStatesMarket/Technical/)
列每天 08:00 更新且以 API 實際結果為準；本輪不把英日既有排程時間冒充
已驗證的官方發布承諾。Sponsor 目前驗證的共用上限為 6,000 次／小時，
不因退場這 131 個任務提高 provider 上限或假裝有 Sponsor Pro 權限。

01:55:05–01:59:26 UTC 的約 4.35 分鐘部署後窗口，有 368 次正常請求開始。
逐請求觀測沒有新的 `failed`，仍有英美股來源修訂及未改變的成功回覆。
另驗證一份美股 484 列新收據的 Parquet SHA／footer。這些不是新增歷史
484 列的證明；已完成資料的追新／修訂也可能不增加完成任務數。
當時仍先處理正常到期追新，新增台股分鐘分區為 0，沒有冒稱已加速回補。
短窗口不證明全系統長期吞吐提升；確認的是無限失敗工作已退場、有效下載
仍正常進行，三個受影響服務在驗收窗口沒有 traceback／watchdog／OOM 標記。

## 選擇性重新啟用

只在修正請求或確認來源恢復後，才人工重新開一個 cycle。修改上限或重啟
不會自動復活耗盡任務；下列操作要求明確資料集並取得原 owner 鎖，
保留舊資料和 audit，且操作本身不送 provider 請求、不開始下載 loop。

```bash
source scripts/runtime_env.sh
run_fintech_python -m downloader.download_finmind_complement \
  --root data_finmind/complement --reopen-exhausted --dataset UKStockPrice
```

Sponsor 同樣提供 `--reopen-exhausted --dataset DATASET`。本輪沒有對實際
131 檔執行 reopen；只以測試 queue 驗證。重新啟用保留原錯誤碼，確保後續
仍以全歷史查詢驗證恢復，不拿空的增量 tail 假裝全歷史來源已修好。

## 清冊與證據

完整私有清冊：
[`retry_exhausted_tasks.csv`](../artifacts/data_quality/finmind_bounded_retry_2026-10-04/live/retry_exhausted_tasks.csv)。
逐檔列 owner、代號、錯誤、次數、嘗試／耗盡時間、舊資料日期、筆數、bytes
與收據路徑；這些本機路徑不進公開 DTO。

證據根目錄為 `artifacts/data_quality/finmind_bounded_retry_2026-10-04/`：
`before.json`／`complement_baseline.sqlite3`、`simulation_final/audit.json`、
`live/audit.json`、`after.json`、`deployment/deployment_after.json`、
`history_sequence.json`、`browser_final/browser_acceptance.json` 與 screenshots。
所有診斷 provider calls 為 0；正常 worker 的請求另由共同帳本記錄。
命令／測試的原始成功和失敗收據保留於原生 task
`finmind-bounded-retry-20261004`；最後同名驗證成功才結案。

原來「這些英股仍持續每分鐘探測」的操作描述由本報告取代；
來源仍回空、未知歷史候選、未來增量與其他必要資料尚待正常排程，
沒有宣告 FinMind 全資料下載完成。
