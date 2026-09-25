# StockAgent 服務完整清單（2026-09-23 起逐次更新）

## 2026-09-26 00:33～00:39 TAIFEX 正式輪恢復與新版本驗收

前一輪 TAIFEX 官方歷史資料發布在 D: head 更新後遇到 Syncthing 明確掃描逾時，留下 `failed` 的 systemd 結果；先前的掃描收據補償與舊版本本機校驗，不能代替下一次正式輪成功。離開交易時段後，執行原有 `stockagent-taifex-public-history.service` 一次；官方來源抓取、驗收、冷發布與掃描均走正式入口，**00:36:20 退出 0／Result=success**，systemd 記錄 wall **3 分 16.465 秒**、CPU **19.250 秒**、峰值記憶體 **207.1 MB**。沒有僅用 `reset-failed` 消除錯誤，也沒有重啟行情／交易服務。

本輪新 head 指向 `taifex-public-history-20260925T163533820866595Z-l0-penguin-ff468a5e561f191a`。以 `scripts/packed_snapshot.py verify` 對**該 exact release** 驗證通過：**298 個物件、230,590,817 bytes**，manifest／inventory 雜湊一致；`materialized_verified=false` 表示沒有驗證本機熱資料實體化，不可混用。待掃描收據為空。隨後本機 Syncthing `stockagent-packed` folder 為 `idle`、`needBytes=0`、待傳項目／刪除／folder errors／pull errors／watch error／system errors 全為 0；連線中的 `vastai1T` 回報 completion **100%**、`needBytes/items/deletes=0`、`remoteState=valid`。這證明當時冷位元組傳輸收斂，**不證明**對端已 materialize、在其持久磁碟驗證或能用於訓練。

唯讀全服務基線 `artifacts/benchmarks/service-coverage-20260926T0040-taifex-recovered.json` 涵蓋 **54 service／40 timer／2 path**，`timer_schedule_findings=[]`。六個 localhost 產品探測皆回 HTTP 200，但 TAIFEX 模擬產品仍 `blocked`（到期結算被未平模擬避險部位阻擋、即時來源停在 9/18），當沖 `degraded`、隔日沖與永豐 `waiting`、全資料監控 `critical`；TAIFEX 公開歷史下載恢復**沒有**修復獨立的模擬帳本與即時行情。全資料監控當時仍有 65 `unable`、90 `catching_up`、16 項需憑證處理與 2 項實體無效清冊，應逐一依正式來源與證據修復，不能改 UI 狀態掩蓋。

## 2026-09-26 00:10 D: 待掃描補償排程與 Binance 正式續跑

TAIFEX 的 D: 發布掃描逾時留下 `scan-pending/*.json`，原本只有同資料集再次發版才會自動重試；即使 300 秒整庫 rescan 最終送出位元組，也可能留下無人處理的待掃描收據與 failed 工作。新增 `scripts/retry_packed_syncthing_scans.py`，由 `stockagent-d-cold-scan-retry.timer` 每五分鐘執行一次：先用 canonical `mount_packed_d_cold.sh --check` 驗證 D:，只列舉安全的正式 JSON 收據、拒絕 symlink，每輪**最多一個資料集**，再呼叫既有 `scan_after_publish(..., retry_full=True)`。該函式持有原資料集鎖，仍依記錄先掃物件、再掃 manifest/head；API 失敗保留收據並使單元非零退出。成功的狀態只叫 `scan_request_acknowledged`，收據明示 peer convergence／release verification `not_checked`；不下載來源、不重建 head、不刪除冷物件，也不清掉原發布工作的失敗歷史。

安裝器 `scripts/install_packed_scan_retry_service.sh` 已渲染並驗證 systemd 單元；00:10:07 正式首次執行 exit 0、`idle_no_pending`，腳本內約 **0.095 秒**。00:15:07 自然 timer 第二輪亦成功、腳本內約 **0.085 秒**；00:25 第四輪後下一輪排在 **00:30:09**。這些輪次都沒有待掃描收據，不能把 idle 當成生產 pending case 已驗收；前輪 TAIFEX 的真實 18 路徑手動重試成功和新增的五項故障注入測試分別提供功能證據。Binance／Syncthing／重試／開機契約／限速相鄰測試 **66 個通過**，Ruff、編譯、shell 語法、已安裝 systemd 單元驗證及差異檢查通過；全庫未跑。00:25 全服務唯讀收據 `artifacts/benchmarks/service-coverage-20260926T0016-retry-deployed.json` 覆蓋 **54 service／40 timer／2 path**、`timer_schedule_findings=[]`；六個本機產品狀態仍有 TAIFEX `blocked`、當沖 `degraded`、隔日沖／Shioaji `waiting`、全資料 `critical`，不能由新 timer 成功推導其它服務健康。

Binance 新輪次完成 4,011 標的來源探索後，正式 `download_and_validate` 於 **00:21:33** 完成並 exit 0；最終 `download_summary.json` 記本輪待處理物件 **2,791／2,791** 已完成、`failed_objects=0`、`cycle_state=complete`，進度耗時約 **588.84 秒**。但 `state=partial` 仍是正確的：持久帳本有 **1 筆 `quarantined_source_invalid`**（`BTCUSDT_210326` 的 2021-02-03 官方日檔含 1 筆無效 OHLCV）與 **1,355 筆 `quarantined_repair_required`** 月檔。後者雖有日檔重建路徑，尚未對每個月做足以解除隔離的完整覆蓋證明；不得把成功退出、已下載物件數或來源隔離改寫成整段歷史完整。

## 2026-09-25 23:31 Binance S3 清單截斷的 fail-closed 重試

先前 23:12 啟動的正式 Binance 封存，在 `discover` **1,652／4,011** 後停住，23:31:38 以 exit 1 結束（wall **19 分 31.949 秒**、CPU **52.863 秒**、峰值記憶體 **417.4 MB**）。systemd traceback 定位在 S3 清單 `urlopen(...).read()`：chunked response 不完整，拋出 `http.client.IncompleteRead`；原 `_get` 只重試 `TimeoutError`、`URLError`、`OSError` 與指定 HTTP status，故一次可重試的位元組傳輸中斷直接終止全部 4,011 個標的探索。持久 `progress.json` 仍標 `running`，與 process exit 1 矛盾。這不是容量、安全保留線或資料檢查通過的證據。

現在對唯讀／可重試的 S3 GET，把 `HTTPException` 納入原有的限速、最多設定次數的退避重試；僅當**完整 response** 成功讀取後才解析 XML 或接受封存內容，不會用截斷位元組補資料。若某一標的在所有重試後仍失敗，取消尚未開始的其它清單 future，等待已執行的 worker 結束，將持久探索狀態標為 `failed` 並維持非零退出；不發布部分 plan。新增故障注入覆蓋「首輪 chunk 截斷、次輪成功」、「重試耗盡仍失敗」及「探索失敗收據」，連同相鄰限速測試共 **49 個通過**，Python 編譯、Ruff、`git diff --check` 通過。

23:51:52 正式單元以新 invocation `b2a9427fcbc2494881ccbe107bb23cc1` 重新執行；23:58 持久進度 `discover` **1,172／4,011**。這只證明新版能開始運作且如實回報進度，尚須等全探索、容量再檢、checksum/Parquet 驗證與最終 `download_summary.json`，才能驗收整輪結果。前一輪的錯誤沒有被改寫成成功。

## 2026-09-25 22:38～23:11 D: 冷儲存切換後的 OpenBB 恢復與 TAIFEX 掃描修復

新的儲存契約以 D: 作為唯一的本機冷儲存實體，不能再把先前 C／D 雙份備份的描述當成目前狀態。`scripts/mount_packed_d_cold.sh --check` 通過；根碟可用約 **522,906,947,584 bytes**，已高於 OpenBB 原有 **107,374,182,400 bytes** 安全線。OpenBB 前次 15:15 退出 2 的直接原因仍是當時根碟僅有 **103,404,191,744 bytes**，不是下載器已完成或 provider 健康。唯讀 preflight 確認此既有封存的右界仍釘在 **2026-07-18**，不會因今天重啟而冒充已封存至 9/25。22:38:48 啟動原 `stockagent-openbb-archive.service` 後，正式 supervisor 與下載器程序均在同一 cgroup，invocation `c003edcbc0434efc98df60fb30f09a1b`，原容量前置檢查已通過。22:40 的全服務唯讀稽核 `artifacts/benchmarks/service-coverage-20260925T2240-openbb-resumed.json` 覆蓋 **52 service／39 timer／2 path**；OpenBB 公開 API 為 HTTP 200／`active`，但資料監控仍 `critical`。下載器恢復既有 plan，回報 **8,909,579 個 active tasks**；後續 watchdog 為 `waiting/provider_cooldown`、新嘗試數 0，故本次只證明監督與續跑能力，**沒有**證明新資料已完成或歷史覆蓋足夠。

同一稽核揭露 `stockagent-taifex-public-history.service` 17:44:31 退出 2：當日官方公開資料抓取與來源契約檢查已完成，D: 冷發布完成 head 原子更新後，Syncthing 對 `head-history/...json` 的明確掃描請求逾時；不是官方資料抓取失敗。`/srv/stockagent-packed/.local-state/scan-pending/taifex-public-history.json` 保留了 18 個待掃描路徑。待 Syncthing 的大範圍掃描轉為 idle 後，使用既有 `scan_after_publish(..., retry_full=True)` 對**相同已發布路徑**安全重試，回傳 `True` 且待掃描收據消失；沒有重抓資料、改寫 head 或清除失敗紀錄。再以 `scripts/packed_snapshot.py verify` 驗證 exact release `taifex-public-history-20260925T094229676352787Z-l0-penguin-96544e211c1be2c6`：**282 個物件、230,262,193 bytes**、manifest 與 inventory checksum 通過。當時本機 folder `needBytes=0`、peer 回報 `completion=100`／`remoteState=valid`，但 folder 查詢時正處 `scanning`，依正式同步契約**不能宣稱已完整收斂**；系統服務也仍保留舊的 `failed`，下一次排程是 9/28 17:30。後續仍需在同步真正 idle、所有錯誤計數與 peer 證據通過後驗收；跨資料集 pending-scan 的有界自動重試已在上方下一輪實作。這輪未改動 Syncthing 掃描／發布語意，也未重啟行情或交易服務。

Binance 公開封存的上次 12:30 單元雖顯示 systemd `Result=success`，實際 `ExecMainStatus=75`；容量收據清楚標記 `accepted=false`、未執行遠端探索。D: 切換釋出根碟空間後，23:12:06 用原有鎖與保留線啟動正式單元；新 `data_binance_archive/capacity_receipt.json` 記錄 `accepted=true`、可用 **522,866,450,432 bytes**、10% 保留線 **216,336,110,387 bytes**。23:13 的持久進度為 `discover` **284／4,011** 個 instrument、`state=running`；只證明這次確實越過前置容量阻擋並開始來源探索，尚未證明所有壓縮檔下載或最終驗證完成。這項排程的 exit 75 必須繼續在營運稽核視為非零結果，不能因 systemd 接受它為正常停止就算資料健康。

23:20 再次完整唯讀稽核 `artifacts/benchmarks/service-coverage-20260925T2318-archive-recovery.json` 枚舉 **53 service／39 timer／2 path**、`timer_schedule_findings=[]`；本機六個產品 GET 均為 HTTP 200，但健康仍依序為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料 `critical`。Binance 封存仍在正式 `discover` 階段，23:20 的持久進度 **1,652／4,011**；OpenBB 正在 `provider_cooldown`，兩者皆不能由程序運轉推導資料完整。TAIFEX 掃描收據已修復，但該 systemd 單元的上次結果仍是 `failed`；下次正式輪次及同步 idle 驗收尚待完成。

## 2026-09-25 15:00 官方歷史重抓的相同日期快路徑與分鐘排程重掛

14:50、14:50:30、14:51 的正式 `institutional_initial` 收據都顯示 `twse_institutional_trades` 改變，TWSE 原始回應的 SHA-256 也依序變動；14:52 正式輪則 `changed_dataset_count=0`。因此不能把前幾輪一律當成下載時鐘造成的假更新，亦不能跳過來源修訂。下載器原本即排除 `_downloaded_at_utc` 比較穩定欄位，但**每次先解碼並合併整份約 350.7 萬列歷史檔**，才判定是否無變動。

現在只對一般按 `date` 覆蓋的 Parquet，在共同欄位 dtype 相同、沒有新欄、受影響日期所有非下載時鐘欄位與列序相同、且現有檔日期無空值並已排序時，從 Parquet metadata 返回總列數，不讀全量資料也不覆寫檔案。舊官方欄位可在當日原本全為 null 的前提下補 null 比對：實際 9/24 TWSE 原始回應 **27 欄**、歷史檔 **32 欄**，TPEx 為 **32／33 欄**。來源數值、網址／來源欄位、新欄／共同欄位型別、新日期、未排序檔、snapshot vintage、append-only payload、顯式 refresh 與 TWSE OHLCV 畸形日期修復仍走原完整合併；這是相同資料的局部加速，不改發布契約。當前約 350.7 萬列 TWSE 檔的舊同日期合併判定約 **1.109 秒／5,561,556 KiB 峰值 RSS**；使用真實官方 9/24 raw 重建 incoming，逐欄先確認完全相同後，新判定約 **0.095 秒／285,564 KiB 峰值 RSS**，原 inode／mtime 不變。約 258 萬列 TPEx 同法為 **0.037 秒**且原檔不變。這是單程序 hot-file A/B，不是整個 8 資料集排程提速 10 倍。

回歸測試另發現舊 TWSE OHLCV 修復路徑會先在記憶體移除可由官方日期替代的畸形列，卻被舊的「無變化」判定提前返回，導致回報列數與實體 Parquet 不一致。現只在確有畸形列修復時禁止該提前返回，確保原子寫入；未代替來源取得或補造價格。當前正式 OHLCV 檔唯讀掃描畸形日期為 **0 列**，故這是預防再次發生的修復，不是宣稱本機現有歷史已被改寫。全 `test/test_tw_public_*.py` **424 個 Python 測試**、Ruff 及 `git diff --check` 通過；這仍不是全庫測試。

正式整輪 A/B 亦界定加速的實際適用條件：15:18:29 在 schema-null 修正前、來源穩定的一輪為 **9.18 秒／峰值 6,466,884 KiB**，顯示最初要求 incoming 與多年歷史檔 schema 完全相同的快路徑**沒有命中**。加入舊欄 null 證明後，15:21:23 與 15:21:45 兩輪分別 **14.56／15.45 秒**、峰值約 **6.56／6.60 GB**，但兩輪 TWSE 正式來源 body SHA-256 與 Parquet SHA-256 都變動，正確地走完整合併。因此目前只證明**真實形狀且內容不變**的局部加速；尚未量到「官方來源穩定」的同負載完整排程速度，不能捏造端到端收益。

為避免日後再次用合成同欄資料誤判，新增唯讀 `scripts/benchmark_tw_public_historical_merge.py`：從已接受的當日 raw 重建 incoming，僅以現存檔的單一來源 URL 做同等比對，先證明 `fast_path_eligible`，再分別量快判定與原完整 merge，要求兩者都判定無變化，並檢查原檔 inode／mtime／大小均未動。15:25 在真實 9/24 TWSE 檔量得 **0.099 秒／277,808 KiB** 對 **2.583 秒／5,569,048 KiB**；TPEx **0.076 秒／238,876 KiB** 對 **1.249 秒／4,246,424 KiB**。各為單次、檔案可能在 page cache 且同機有其他服務負載；若官方 raw 已與 Parquet 不同，腳本會回 `fast_path_eligible=false`，不做虛假的 no-op A/B。可重測：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_public_historical_merge.py --dataset twse_institutional_trades --date 2026-09-24
run_fintech_python scripts/benchmark_tw_public_historical_merge.py --dataset tpex_institutional_trades --date 2026-09-24
```

15:00 僅用 `intraday-timer-only` 安裝已驗證的 `OnActiveSec=1min` 模板並重啟 timer，下載 service 及交易行程未更換；由 `active/elapsed` 無下一觸發改為 `active/waiting`。正式 15:01:42 與 15:03:43 **兩輪皆觸發並完成**，各約 60／59 秒、峰值 2.71／2.54 GB，三個分鐘來源的步驟收據均 `complete`；第一輪的 OKX／Bybit／Binance 分別完成 492／867／574 個標的，並排出下一輪 15:05:43。15:05 唯讀全服務稽核 `artifacts/benchmarks/service-coverage-20260925T1505-timer-recovered.json` 覆蓋 52 service／40 timer／2 path，`timer_schedule_findings=[]`。15:00:49～15:04:59 根碟可用 103,415,799,808→103,411,924,992 bytes，屬整個 filesystem 變動，不能只歸因這個下載器；沒有觀察到 GB 級突增。六個產品 HTTP 200，但業務健康仍為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、永豐 `waiting`、OpenBB `stopped`、全資料 `critical`。15:15 OpenBB **自然重試已觸發**，前置檢查讀到 103,404,191,744 bytes，仍低於 100 GiB 安全線 107,374,182,400 bytes，缺口 **3,969,990,656 bytes**，依既有契約退出 2，未開始下載；不是 timer 失效。

15:30、15:30:30、15:31 的正式 `close_revision` timer 自然輪均 `Result=success`／exit 0，分別約 1.48／0.97／0.97 秒；收據都是 `status=skipped_verified_non_session`、`commands=[]`、`changed_dataset_count=0`，官方本機證據 `TWSE schedule as-of 2026-09-16: 中秋節`。14:02 的舊 failed 記錄因此由新的成功執行清除，但這只驗收休市分支，**不是**下個開市日收盤來源或開盤交易就緒。15:32 全服務唯讀收據 `artifacts/benchmarks/service-coverage-20260925T1531-holiday-acceptance.json` 覆蓋 52 service／40 timer／2 path、`timer_schedule_findings=[]`，六個本機 API 皆 200，但健康仍依序 `blocked`／`degraded`／`waiting`／`waiting`／`stopped`／`critical`；OpenBB 是唯一仍 failed 的 StockAgent service。

同輪 OKX `history-candles` 有 **504 次** rate-limiter grant、實際約 **59.50 秒**；[OKX 官方文件](https://www.okx.com/docs-v5/en/)目前仍列該 endpoint 每 IP **20 次／2 秒**，故僅配額的理論下限已約 **50.4 秒**，未計網路與處理。加大 16 個 workers 或取消限速不能在相同端點、安全契約下把這段壓成數秒；若要改用文件另列 **40 次／2 秒**的最新 `market/candles`，必須先證明其 1,440 根範圍、跨停機補抓、隨機快取新舊次序及逐標的實際時間戳不會遺漏，再做帶回退的灰度驗收。本輪未切換端點，也未以標的數縮減冒充提速。

## 2026-09-25 14:34 已註冊 artifact 退役：先做不可退役的廉價證明

為查明能否安全釋放根碟容量，休市時用 `timeout 180s nice -n 15 ionice -c3 ... scripts/retire_enrolled_artifacts.py` 執行**唯讀**計畫；它在 180 秒到期時仍在讀取 C 冷儲存物件（觀測約 8.7 GB read bytes、0 write bytes），退出 124，沒有完成任何 eligibility 判定，且沒有 `--apply` 或刪除。檢查四筆已註冊狀態才發現，三個完整 Bybit run 與一個 legacy crypto 檔案都是 9/25 剛建立／使用，明確受 7 天租約保護，最早到 **2026-10-02 02:54 UTC**；在到期前做 C／D 全量 hash 不可能讓它們符合退役資格。

排程退役入口現在先核對已註冊來源身分、`hot-enrolled`、schema 1 與 `last_used_ns`；只要有效租約未到期，就回 `deferred`／`seven-day-use-lease-active`、精確到期時點，並顯示 `verification=not_checked_active_lease`、`snapshot_id=null`，**不宣稱來源、D 備份或 Syncthing 已驗證**。異常未來時間、到期、狀態損壞或不符時仍走原完整 C／D／來源／引用／pin／peer 驗證才能 apply；沒有更改核准的刪除契約。與實際四筆來源相同的唯讀重跑 **0.373 秒**，全部是 deferred，沒有可用來修復 OpenBB 的核准回收空間。相關排程／完整 artifact／legacy 退役 **19 個測試**、Ruff／whitespace 通過。OpenBB 仍因根碟可用約 **103.4 GB** 低於既有 **100 GiB（107.37 GB）**安全線而停用；不得降低門檻或刪原始快取來假裝恢復。

14:40 再跑全服務唯讀覆蓋 `artifacts/benchmarks/service-coverage-20260925T1440-idle-lease.json`，仍有 **52 service／40 timer／2 path**；六個本機產品端點全為 HTTP 200，但健康依序 `blocked`／`degraded`／`waiting`／`waiting`／`stopped`／`critical`。`stockagent-registered-data-intraday.timer` 仍 `active/elapsed`、沒有下一次觸發；正式 OpenBB 仍 failed，14:02 的官方收盤掃描失敗旗標尚待下次定時服務成功清除。這些沒有因本輪閒置效能改善而被改成正常。當日 `stockagent-registered-data-features.service` 14:29:47 已完成約 29 分 47 秒工作、收據 `completed`；它與停擺的 intraday 計時器是不同工作。

## 2026-09-25 14:28 隔日沖歷史閒置行程的實際啟動成本

前一輪已使「無啟用模式」回 `idle_no_enabled_modes` 而非 failed，但正式 systemd 輪仍花 **8.034 秒**、**7.804 CPU 秒**、峰值 **436.1 MB**；單獨 `import scripts.maintain_tw_overnight_history` 約 **5.285 秒**，而只載入市場設定模組約 **0.089 秒**。瓶頸是進入閒置分支前先匯入完整重建／部署／訓練相依模組，不是讀取市場設定本身。

維護腳本現在先用 canonical `load_market_configs()` 判斷 `enabled && overnight_simulation_enabled`；只有真的啟用模式才延遲載入重建、來源驗證、歷史部署及日曆模組。設定讀取失敗仍報錯，啟用模式仍經原有 lineage／來源／部署檢查；停用時只更新 `latest_attempt.json`，不冒充已完成的歷史資料。新獨立程序匯入約 **0.108 秒**；正式 `stockagent-tw-overnight-history.service` 14:28 再跑成功，CPU **0.169 秒**、峰值 **13,787,136 bytes**（約 13.1 MiB）、同樣回 `idle_no_enabled_modes`。這是當下無模式的資源節省，**不是**已啟用策略的重建速度證明。相鄰發布／隔日沖 **122 個 Python 測試**及 Ruff／whitespace 通過。

## 2026-09-25 14:15～14:21 閒置心跳公開判活與休市工作修正

全服務稽核發現閒置引擎的 `service_sync.json` 心跳雖每約 10 秒更新，但當沖／隔日沖完整 dashboard 與首頁摘要仍以 60 秒一次的 `status.json.updated_at` 套用 30 秒 stale 門檻，故可能把正常運轉誤判為 `stale`。現保留「最後完整提交」時間與年齡，另用與 `state.json`／`status.json` 同一 `state_revision`、同一 `engine_run_id` 的心跳判活；任何缺失或修訂不一致均不得借用心跳變綠。首頁輕量摘要僅核對 status 與 compact receipt，不讀取大型 state；頁面將「帳本心跳」更正為「引擎心跳」，同時顯示最後完整提交年齡。新回歸涵蓋 45 秒閒置心跳、ID 錯配與首頁 fail-closed；當沖／隔日沖／公開網關相關 **413 個 Python 測試**、共用前端 **17 個 Node 測試**、Ruff／whitespace 通過。只重啟唯讀 gateway 與 8766 dashboard 子行程，兩個紙上交易引擎 InvocationID 未變。實測直接 8766 當沖 `degraded`、心跳年齡約 **9.5 秒**、完整提交年齡約 **60 秒**；公開 8770 當沖 `degraded`、隔日沖 `waiting`，IPv4 HTTPS 當沖狀態 HTTP 200（單次約 **260 ms**）。公開完整狀態仍有 55 秒快取／背景重建，回應中的年齡為建置時快照，不能將其當成每次 GET 即時計時；快速 `/api/revision` 與來源時間戳須分別解讀。

14:15 唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260925T1415-idle-heartbeat-fix.json` 覆蓋 **52 service／40 timer／2 path**；六個本機產品端點均 HTTP 200，但依序仍是 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `stopped`、全資料 `critical`。另外發現隔日沖歷史維護 14:10 因目前**沒有啟用的隔日沖模式**而退出 1，以及 14:02 休市的收盤公開資料掃描對 9/25 執行 completed-session finalize 而退出 1；這兩個失敗不能歸咎網站或交易引擎中斷。

隔日沖維護現在先檢查 lineage：只有「確實沒有啟用模式」會寫獨立 `latest_attempt.json`、回 `idle_no_enabled_modes`／exit 0；設定錯誤、資料錯誤仍失敗，過去 `latest.json` 完成收據不被假 idle 覆寫。14:20 正式 `stockagent-tw-overnight-history.service` 重跑為 success，收據 `enabled_market_count=0`；因大型 Python 模組仍需載入，這輪約 **8.0 秒**，不宣稱啟動耗時已優化。收盤官方掃描僅在既有來源稽核確認 TWSE 官方休市時提前記 `skipped_verified_non_session`，不讀大檔、不下載、不執行當日收盤 finalizer；缺漏、未驗證或矛盾的日曆不能走跳過路徑。9/25 官方本機排程證據為 `official TWSE schedule as-of 2026-09-16: 中秋節`；14:21 手動同一正式腳本 `--phase close_initial` 約 **0.5 秒**完成，收據 `commands=[]`、`changed_dataset_count=0`。相關發布／隔日沖歷史 **96 個測試**與 Ruff／whitespace 通過。此手動驗證沒有清除 14:02 的 systemd failed 歷史狀態；尚需等下次 15:30 close phase timer 驗收實際排程。OpenBB 容量安全線、crypto intraday `active/elapsed`、盤後來源健康和下次開盤仍未解決。

## 2026-09-25 13:35～14:04 休市引擎寫入減量：提交與心跳分離

確認 9/25 官方休市期間，當沖引擎仍每約 10 秒執行 `update_readiness()`；它原本每次都增加 state revision，序列化／原子覆寫約 **9.5 MB** `state.json` 及 positions／status／service-sync。自然取樣 13:42:32 的 **302.068 秒**，當沖 cgroup block write **291,753,984 bytes**、CPU 平均 **0.0488 核**；隔日沖 **269,844,480 bytes**、CPU **0.0588 核**。此 I/O 是區塊寫入，不等於磁碟淨增容量。

新契約保留所有真正狀態／成交提交的 `state_revision`、原子檔與 append-only 帳本；只有離峰或已**確證**休市的閒置時段，完整 readiness 更新降至每 60 秒，且每約 10 秒只刷新 `service_sync.json` 的 `heartbeat_at`（原 `published_at` 仍是最後一次完整提交時間，不冒充新修訂）。Dashboard 與無人值守檢查以 heartbeat 判活，仍以原修訂碼對帳；服務收據 revision／engine run ID 不一致時心跳拒絕更新。08:10～09:10 的當沖開盤轉換、隔日沖 08:10～09:11 與 12:45～13:36 兩段撮合窗口保持原快速節奏；官方日曆缺漏／衝突不能走休市慢路徑。Shioaji 行程持有訂閱狀態，故只在證交所公告的休市日重啟紙上交易服務；下次開市仍須驗收登入、訂閱與 09:00 訊號。相關當沖／冷啟動／dashboard／guardian **230 個測試通過**；其中舊冷啟動測試把註冊資料集數硬寫成 156，正式註冊表已是 159，斷言改為比對實際 canonical 清單，未刪任何資料集。

當沖重啟後 invocation `916f2ea796e64e56a499b1b346fe9ee3`；原 9/24 **198 筆**持倉數與訊號／委託／成交／事件帳本檔案大小和時間戳未變，公網 `/tw-day-trade/api/revision` 仍 200 且能顯示獨立 heartbeat。公開 gateway 因自身持有舊版 dashboard 模組，另在 **119 個相關測試通過**後單獨重啟，驗收來源由舊 `engine_age≈29 秒、heartbeat 欄缺失` 轉為新的 heartbeat age 約 **8 秒**；IPv4 HTTPS 仍 200。13:52:33 完整自然輪（**300.478 秒**）當沖區塊寫入 **104,312,832 bytes**、CPU 平均 **0.0270 核**，對照 13:42 同一休市時段約減少 **64% block writes**、**45% 平均 CPU**；配置／背景負載不完全相同，不宣稱開盤熱路徑有相同增益。

隔日沖另發現較大的同型缺陷：目前 `enabled_markets=[]`，但舊 runner 用 `or not specs` 令每約 1 秒都重新載入設定並完整提交。13:53:45～13:54:06 實測 21 秒 **19 個修訂／18,989,056 block write bytes**。當沖與隔日沖兩個 runner 均改為「空模式也算已載入」，只靠時間到期或首次啟動重讀設定；新模式最遲於平常 30～60 秒被偵測，開盤／撮合窗口沿用 30 秒。隔日沖五個相鄰測試檔 **67 個測試通過**。休市重啟後 invocation `adbbd11ac4da4d48a01ad7a9d3183e67`，訊號／委託／成交／事件 SHA-256 全部未變；13:55:08～13:55:41 的約 33 秒 `state_revision=148105` 不動，heartbeat 持續更新。14:02:41 的正式自然輪（**306.806 秒**）隔日沖區塊寫入 **5,103,616 bytes**、CPU 平均 **0.0022 核**，對照 13:42 同樣休市空模式約 **269,844,480 bytes／0.0588 核**，這是約 **98% 寫入減量**與 **96% 平均 CPU 減量**；仍不是下次有啟用策略時的實測。當沖同輪區塊寫入 **83,546,112 bytes**、CPU **0.0159 核**，相對舊休市樣本寫入約減 **71%**。隔日沖沒有啟用模式且 Discord overnight ack revision 仍為 0，公開同步狀態為 `catching_up`；沒有把它說成交易就緒。

再檢查 `systemctl StatusText` 發現休市當沖可能說「等待今日訊號」、空模式隔日沖可能列歷史模式的 `waiting_13_00_switch`。已區分官方確認休市、來源未驗證及無啟用模式；兩個行程在完整五分鐘取樣後另受控重啟載入文字修正。14:04 正式 StatusText 分別為 `verified closed session; carried paper positions=198` 與 `running idle; no enabled modes`，兩個公網 status GET 仍 200；當沖訊號檔的大小／mtime、委託／成交／事件 SHA-256 及隔日沖四種 append-only 帳本 SHA-256 均與修正前完全相同，state／service-sync revision 對齊。合併當沖／隔日沖／冷啟動／dashboard／guardian 回歸 **313 個測試通過**，Ruff 與 whitespace 通過。這仍不等於下一開市日行情訂閱、開盤訊號或出場實測成功。

13:07 合併回歸：資料監控、FinLab、公開網站、服務趨勢、開機契約與備份／保留 **303 個 Python 測試通過**；全工作樹 `git diff --check` 通過。這是相關範圍回歸，不等於全庫測試，也不表示尚未重啟的 gateway／備份行程已載入新程式。根碟可用 **103,670,067,200 bytes**，仍低於 OpenBB 的 100 GiB 安全線；crypto intraday timer 仍 `active/elapsed`／下一觸發 `infinity`。

## 2026-09-25 13:25～13:30 備份 head 唯讀快路徑加固與唯讀 gateway 載入

備份 metadata 階段對已存在的 D: head 原用 `safe_path()` 後 `Path.read_bytes()` 比對內容；前一步雖拒絕當時可見的 symlink，兩步間仍有名稱被替換的時間窗，而且在 DrvFs 重做路徑解析。此唯讀探測已改用既有 `read_existing_metadata_nofollow()`，將目錄與檔案以不跟隨連結的 fd 開啟，讀取前後核對檔案身分及每層目錄名稱；真正缺檔才走原本新 head 的完整物件重驗，symlink／異常則維持 fail-closed。未更動冷物件校驗、D: head 寫入或刪除政策。新增測試防止退回跟隨路徑讀取，且 D: head 被 symlink 取代時 `degraded`、`current_heads_complete=false`；備份／保留 **40 個測試通過**、Ruff 通過。

依證交所當年公告，9/25 為休市，故只重啟**唯讀公開 gateway** 與**獨立 D: 備份**，不動交易／行情／Discord。Gateway 重啟前後本機 `/healthz`、`/api/overview`、`/shioaji/api/status`、`/data-monitor/api/status` 與 IPv4 公網 `/healthz` 全部 HTTP 200；gateway invocation 變為 `c2500d48386a40ee9bc2336a2a19b7a4`，交易、行情、Discord invocation 均保持原值。重啟前相關公開 gateway **119 個測試通過**。新的已載入 gateway 經可重跑工具 8 次／路由、單客戶端取樣，`/api/overview` 中位 **1.181 ms**、`/shioaji/api/status` **2.232 ms**、`/data-monitor/api/status` **11.269 ms**、`/tw-day-trade/api/summary` **1.536 ms**，各路由 0 錯誤，收據 `artifacts/benchmarks/public-gateway-20260925T1330-restart.json`。這是本機熱態取樣，不能推論所有公網路徑或冷啟動同速；Shioaji payload `health=waiting`，沒有改寫資料健康。

備份重啟後 invocation 為 `2be12792bdb24303bbc557903dc18989`。首次正式 current-head pass 為 `up_to_date`、**12,019 物件／125 heads／0 pending／0 error**，metadata **4,701.86 ms**，拆解 manifests **2,049.00 ms**、heads **2,642.69 ms**，destination trust **9,751.11 ms**；D: 可見的 current-head 完整收據未退化。這一輪與重啟前 metadata **6,277.14 ms** 的兩次自然取樣負載不同，不能單憑它宣稱加固提高效能；主要成本仍是每輪物件信任驗證。

載入後完整唯讀稽核 `artifacts/benchmarks/service-coverage-20260925T1330-post-restart.json` 覆蓋 **51 service／40 timer／2 path**。六個產品 probe 均回 HTTP 200，但健康依序仍為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `stopped`、全資料 `critical`；OpenBB 是唯一 failed StockAgent service，crypto intraday timer 仍有 `recurring_timer_without_next_trigger`。部署驗收只證明 gateway 與 D: 備份改動已載入並維持端點／備份可用，不等於所有資料與排程已修復。

## 2026-09-25 13:20 盤中交易服務寫入：監測欄位修正與成本歸因

短取樣曾看到當沖／隔日沖的 cgroup block write，但五分鐘趨勢一直為 `null`。原因是 `service_snapshot()` 已提供直接從 cgroup `io.stat` 取得的 `CgroupIOReadBytes`／`CgroupIOWriteBytes`，趨勢卻讀主機未提供的 systemd `IOReadBytes`／`IOWriteBytes`。已改為與短取樣同一來源；相同 invocation 才相減，缺失或重啟仍為 `null`。相關趨勢／排程／啟動 **42 個測試通過**，Ruff 通過。13:16:53 的正式自然輪（間隔 327.959 秒）首次可見當沖寫 **322,691,072 bytes**、隔日沖寫 **296,828,928 bytes**、Discord 寫 **6,717,440 bytes**；當輪整個 filesystem 可用量只少 **581,632 bytes**。這是區塊寫入，不是各服務新增容量；不能用這 600 多 MB 解釋 OpenBB 的磁碟容量缺口。

當沖 `state.json` 當時約 **9.5 MB**、100m 模式單獨約 **6.55 MB**；其 `carry_cost_ledger` 約 **3.33 MB**、`margin_exit_capacity_used` 約 **1.59 MB**。13:14:39 到 13:18:27 的 `state_revision` 增 **22**，`dashboard_content_revision` 保持 **4682**；至少一次相鄰修訂 **25817→25818** 在排除心跳／修訂欄位後完整狀態 SHA-256 相同。這證實整檔重寫有重複成本，但**不授權直接省略提交**：現行測試要求每次 heartbeat 新修訂碼，state／positions／status／service_sync 修訂對齊，margin cost 收據與累計必須同一原子提交。盤中沒有改交易引擎或帳本。後續要在離峰設計可驗證的狀態分片／心跳分離及崩潰恢復對帳，不能把「面板內容沒變」當成交易狀態沒變。

## 2026-09-25 13:04 FinLab 收據檔案驗證：減少重複路徑解析

全資料監控每輪從 FinLab 目錄與收據列出約 1,113 項，其中 1,104 份已下載收據各自解析 `datasets/<檔案>` 路徑，原 `_finlab_candidate_sources()` 同程序三輪約 **142.555／137.153／159.276 ms**。一般兩層相對路徑改為在已解析的 enrolled root 下，以 `O_DIRECTORY|O_NOFOLLOW` 開啟 `datasets` 並對單一葉檔用不跟隨連結的 `stat` 驗 regular file；回傳前另比對開啟中的目錄與目前路徑的 inode，若目錄換版就回退完整檢查。每筆仍重新觀測檔案，沒有只看目錄 mtime。符號連結、巢狀舊路徑或不支援這些旗標的平台回退原本的 `resolve()` 加根目錄包含檢查；來源外連結仍拒絕。單獨對 1,104 檔的初版原型約 **85.637 → 17.668 ms**；加上目錄換版防護後完整函式三輪 **115.396／114.379／141.253 ms**，不同負載下增益浮動，不能宣稱固定倍率。

FinLab／全資料／公開網站回歸 **188 個測試通過**，另補測檔案與目錄的內部／外部符號連結、缺檔、穿越及絕對路徑；Ruff／whitespace 通過。13:03～13:04 正式自然輪的 `finlab_sources_and_acquisition` 為 **115.683／128.386／121.754 ms**，首輪因程式 mtime 觸發欄位清冊重建使總耗時 **5.083 秒**，後兩輪約 **1.927／1.807 秒**。公開資料健康仍 `critical`、註冊 1,636 項、需注意 115 項；只是降低觀測運算，沒有冒充下載完成。

## 2026-09-25 12:58 盤中資料 timer：隔離安裝與雙時鐘競態修正（尚未部署）

正式 `stockagent-registered-data-intraday.timer` 仍 `active/elapsed`、下一次 monotonic 觸發 `infinity`。原模板同時有 `OnBootSec=2min` 與新的 `OnActiveSec=1min`；前者對已開機很久的 WSL 重新啟用會立即觸發，與「盤中先不要突然啟動下載」相衝突。兩個只執行 `/usr/bin/true` 的 transient timer 實測：含 `OnBootSec` 者剛啟用就產生 `LastTriggerUSec`；僅 `OnActiveSec=1min`／`OnUnitInactiveSec=1min` 者 `LastTriggerUSec` 保持空且 `SubState=waiting`。兩個 probe 測後都已停止／卸載，不涉及資料下載。

模板移除多餘 `OnBootSec`，保留 timer 啟用後一分鐘與每輪結束後一分鐘的時鐘。安裝腳本新增 `intraday-timer-only`，只渲染／驗證／安裝這個 timer，不覆寫下載服務及其他 dirty 模板，不修改無關腳本權限；因已 active 但 elapsed 的 timer 用 `enable --now` 不會重排，該模式明確 `restart` timer 並拒絕仍為 `elapsed` 的結果。`bash -n`、排程／啟動／趨勢 **41 個測試通過**、whitespace 通過。**未在磁碟低於安全線及盤中執行安裝**；正式服務已有較早的完成事件，`OnUnitInactiveSec` 仍可能讓重新啟用後立即觸發，隔離測試沒有排除這點。必須在可接受下載啟動的安全窗口載入，再實測下一觸發、實際工作收據與容量。

## 2026-09-25 12:51 排程失效納入長期服務趨勢

手動全服務稽核雖能找出「enabled／active、實際 elapsed 且沒有下一觸發」的循環 timer，但之前每五分鐘的自然服務趨勢只記 service 資源，故障可能長時間沒有連續證據。現重用相同的 `timer_schedule_findings()` 判定，每五分鐘以一次唯讀 systemd 查詢記 `recurring_timers` 的 `attention`／`observed_clear`／`unavailable`；systemd 查詢失敗或一個 timer 都沒觀測到，不能變成空 findings 的假正常。未改任何 timer、下載或交易行為。

12:51:13 正式自然 `all_service_runtime_sample` 已記 **40 timer**、`state=attention`，唯一 findings 為 `stockagent-registered-data-intraday.timer` 的 `recurring_timer_without_next_trigger`；相鄰趨勢／排程／開機契約 **40 個測試通過**，Ruff／whitespace 通過。這是偵測能力，不是排程恢復：timer 仍 `active/elapsed` 且下一觸發 `infinity`，須在容量與盤中資源安全後載入已修正的 timer 模板並實測執行收據。

## 2026-09-25 12:46 備份 metadata 子階段測速（尚未部署）

備份閒置對帳已量到 metadata 約 10.1 秒，但舊收據無法區分 manifest 與 head。`PackedBackup.metadata()` 現將兩段單獨計時，併入既有 `stage_timings_ms` 的 `metadata_manifests`、`metadata_heads`；校驗、D 槽寫入與 head 提交順序均未更動。備份／保留回歸 **38 個測試通過**，Ruff／whitespace 通過。**正式備份服務仍沿用舊行程（InvocationID `b1815b0439074309a0c43430af794182`）**，因此目前沒有實測新欄位，更不能先宣稱 metadata 耗時已下降；離峰載入後觀測實際子階段，才能決定是否有安全的後續優化。

12:47 唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260925T1247-current.json` 記 **51 service／40 timer／2 path**；crypto intraday 仍被正確報為 `recurring_timer_without_next_trigger`，OpenBB 為唯一 `failed` service。六個產品 probe 分別仍有 `blocked`／`degraded`／`waiting`／`waiting`／`stopped`／`critical`，不能因 HTTP 可達或本輪快取加速便宣稱系統全綠。正式公開 Shioaji probe 約 359.673 ms，符合尚未重啟舊 gateway 的部署邊界。

## 2026-09-25 12:35 全資料清冊：逐檔驗證後重用小型聚合索引

原 30 秒自然輪的清冊約 **1.38～1.48 秒**，其中完整 JSON 快取 **43,319,683 bytes／57,296 檔**的解析約 **0.57～0.60 秒**、發現約 **0.49～0.60 秒**、檔案簽章檢查約 **0.29～0.34 秒**。251 個資料集的公開聚合僅約 **112 KB**。直接跳過掃描或只看目錄 mtime 會漏原地覆寫，不能以此換速度。

現加 `record_inventory_fast_index.json`（約 110 KB）作**可丟棄**的衍生索引：記錄完整快取的 dev／inode／size／mtime／ctime 與每個資料集的精確檔案歸屬、逐檔五元身分指紋、聚合及修訂碼。每輪仍重新發現並 `stat` 所有 57,296 個選取路徑；只有指紋、索引內容 SHA-256 與完整快取簽章都相符時才略過 43 MB 的解析與重算。缺檔、原地更換、同大小同 mtime 的 inode 替換、跨分組搬移、索引損壞／變更都走原完整流程；欄位明細需要重建時仍可讀完整快取。完整快取另記只含資料集與成員路徑的指紋：即使兩檔交換分組後所有聚合數字剛好相同，也會更新 feature revision，避免沿用錯誤欄位清冊。索引寫入失敗只失去加速，不可讓資料清冊失敗或變成假 `verified`。

12:32 首次建索引使該輪額外重算並寫 43 MB 快取，清冊約 **2.54 秒**、欄位快照也因來源快取換版重建，總約 **5.83 秒**；不能把此 warm-up 隱藏。12:33 兩輪正式自然收據：清冊 **680.568／693.222 ms**、整輪 **1,744.328／1,781.394 ms**，`feature_reused=true`、`refreshed_files=0`，對照變更前同類穩定輪約 **2.45～2.49 秒**。12:34 一輪雖清冊 **686.686 ms**，欄位投影因來源檔／實作檔 mtime 變更而重建，總耗時 **4,939.11 ms**，不能宣稱每輪必快。唯讀當下完整快取與快索引的 **251 個 datasets 完全相同、feature revision 相同**；公開 API 仍 `critical`，57,296 選取檔中 19 個 invalid 仍明列。相同聚合不同分組修訂碼、索引損壞回退、相鄰網站及開機契約共 **230 個測試通過**，Ruff／whitespace 通過。最新自然輪 12:44:09 的 `inventory_fast_index_hit=true`：清冊 **689.861 ms**，整輪 **1,755.233 ms**，`feature_reused=true`、`refreshed_files=0`；12:40 的索引版次遷移亦曾造成慢輪，不拿來算穩態。

容量方面，OpenBB `_state/raw_cache` 約 **8.3 GB**，其中 SEC companyfacts 約 3.0 GB、BLS labstat 約 3.3 GB、SEC insider 約 2.1 GB；它是可續傳的請求原始資料，不能把「cache」字樣當成可刪證據。OpenBB SQLite 約 17 GB、journald 約 4.1 GB。核准的已註冊 artifact 退役 dry-run 在交易時段掃描約 40 秒仍產生較高磁碟 I/O；已只停止本次由代理啟動的**唯讀**計畫，退出 143，未執行 `--apply`、未刪來源或備份，也**沒有**可用的退役 eligibility 結論。待離峰再跑並核對 D／Syncthing／lease／進程引用與可回收 bytes；在此之前 OpenBB 仍不能因安全線不足而強行宣稱恢復。

## 2026-09-25 12:18 永豐公開狀態頁：重用已建置的唯讀快照

12:13 全服務唯讀稽核 `artifacts/benchmarks/service-coverage-20260925T1213-post-anchor.json` 列 **51 installed service／40 timer／2 path**，crypto intraday 仍是唯一 `recurring_timer_without_next_trigger`；六個 localhost 產品端點皆 HTTP 200，但永豐狀態回應約 **351.6 ms**、健康 `waiting`，當沖 `degraded`、TAIFEX `blocked`、OpenBB `stopped`、全資料 `critical`。HTTP 200 不是業務健康。

單獨對永豐公開 builder 做本機剖析：首輪約 **461 ms**，其中「本機 service／journal／manifest」約 **187 ms**、「pipeline receipts」約 **248 ms**；無券商登入、無行情查詢。相同程序稍後三輪直接建置約 **443.63／72.70／54.92 ms**。原本每 30 秒的全資料監控工作已為自己的分組重複建置這份 payload，造成公開 gateway 自己再讀同一批收據。

現在 30 秒監測工作只建置一次永豐公開狀態，原值同時交給全資料投影並以原子檔發布 `artifacts/live/data_monitor/shioaji_status.json`；`read_only=true`、`simulation_only=true`、`production_order_possible=false` 才能寫。公開 gateway 每次檢查產物的產生時間（不得逾 60 秒）、安全 regular-file／UID／權限、固定 1 MiB 上限、讀前後 inode／mtime／ctime 簽章及 JSON 有限值，任何缺失都回退原本的本機 builder，**不**以舊檔冒充新鮮資料。首頁與永豐獨立頁共用同一路徑；未加券商連線，也未改交易／報價服務。

12:17 自然監測輪已實際產出 root-owned 0600、47,767 bytes 的有效快照，`health=waiting`、10 項 pipeline；同機三次驗證讀取約 **0.49／0.52／0.61 ms**，和直接建置的健康與 pipeline 數相同。隔離的新程式 HTTP 入口三次約 **3.65／2.59／2.30 ms**，HTTP 200 且 `waiting`。公開頁、永豐、全資料和長期服務趨勢 **189 個測試通過**，Ruff／whitespace 通過。**正式公網 gateway 仍是舊行程，盤中未重啟；此數字不是公網端到端測速。** 離峰須只重啟唯讀 gateway、核對本機與 IPv4 HTTPS、回退情境及交易／行情行程 ID 不變；IPv6 與上游資料健康仍是獨立問題。

## 2026-09-25 12:10 根碟容量趨勢與歸因邊界

根碟 `df` 此刻可用 **103,271,596,032 bytes**，低於 OpenBB 既有 100 GiB（107,374,182,400 bytes）安全線；OpenBB supervisor 仍 `failed/exit 2`，crypto intraday timer 仍 `active/elapsed` 且下一次觸發為 `infinity`。`stockagent-legacy-us-stage.service` 是另一個目前運行的 transient 任務，11:47 啟動，12:08 cgroup 累積 `rbytes≈84.74 GB`、`wbytes=0`；這個樣本**不能**證明它造成先前數 GB 容量損失。journald 總占用約 4.1 GB，但沒有為了跨越安全線而清除可能唯一的稽核紀錄。

現有每五分鐘 `all_service_runtime_sample` 只有同 invocation 的 cgroup 讀寫量；它既不能代表容量增減，也無法判定某個來源樹是否變大。現補一次 `statvfs` 量測：記錄 StockAgent 所在 filesystem 的裝置 ID、總 bytes、可用 bytes、跨樣本可用 bytes 差。跨開機、換 filesystem 或觀測失敗時 delta 留空；事件明確註明這是**整個檔案系統的變動，不可歸因到單一服務，也不等於 cgroup I/O**。無須遍歷 2 TB 來源樹或另起常駐程序；唯讀本機 helper 回報同一掛載可用 **103,270,936,576 bytes**。新功能及相鄰排程／稽核 **39 個測試通過**、Ruff／whitespace 通過。12:20:35 正式自然輪的 `all_service_runtime_sample` 已記錄 51 個 service、`device_id=2096`、可用 **103,268,347,904 bytes**、比上個同機樣本少 **5,341,184 bytes**；這只是約五分鐘整個 volume 的變動，仍不能指定造成變動的服務。

## 2026-09-25 12:04 計時器重啟保證擴展

從 systemd journal 可定位 crypto intraday 最後一輪 9/24 08:19:38 成功完成、08:20 timer 停止、08:53 短暫啟停、09:10 再啟動；09:10 後沒有新工作，現為 `active/elapsed`、無下一觸發。這支持「重啟後缺少獨立錨點」的排程修正；它不表示來源下載、儲存容量或全部資料健康已恢復。

用只執行 `/usr/bin/true`、`ExecCondition=/usr/bin/false` 的獨立 transient timer 實測 `OnActiveSec=1s` 加 `OnUnitInactiveSec=2s`：12:04:13、15、17、20 均因條件跳過，timer 仍回到 `waiting` 並持續觸發；測後已停止。這驗證受保護窗口的 `ExecCondition` 跳過不會讓新錨點停止循環，未呼叫任何實際下載。

通用契約測試掃描所有 `deploy/systemd/*.timer.in`：凡有 `OnUnitActiveSec` 或 `OnUnitInactiveSec`，必須另有相對 timer 啟用的 `OnActiveSec` 或實際日曆 `OnCalendar`。除前述 crypto timer 外，為五個同類模板補獨立啟動錨點：公開資料狀態 5 秒、WSL 回補記憶體檢查 5 分鐘、儲存壓力 15 分鐘、遠端冷 artifact ingress 2 分鐘、未安裝的舊冷 artifact maintenance 5 分鐘；原完成後循環及服務內容不變。相關 boot-recovery／全服務稽核 **30 個測試通過**。這些**只有 repo 模板變更**，未重啟盤中服務，也未把 crypto timer 裝入 systemd；實際服務排程仍須在離峰、磁碟容量足夠後個別安裝並核對下一觸發及收據。

## 2026-09-25 11:58～12:00 systemd 計時器實測與誤報修正

用唯一 `codex-timer-bootstrap-probe-20260925.timer` 作隔離測試，目標只執行 `/usr/bin/true`，`OnActiveSec=2s` 加 `OnUnitInactiveSec=2s`、`AccuracySec=1s`；11:58:24～11:58:40 的 journal 有連續啟動／正常退出，測後已明確 `systemctl stop`，timer 為 `inactive/dead`。此實測證明 timer 啟用錨點可接續完成後循環，但不等於 crypto 下載工作已成功。

實測還發現本機 systemd 259 的 `NextElapseUSecMonotonic=infinity` 可與正常 `SubState=running`、反覆觸發同時存在，因此不能單憑該欄把 timer 判停。全服務稽核已把 `recurring_timer_without_next_trigger` 限定於 **`active/elapsed`、宣告 OnUnitActive／OnUnitInactive 循環、目標不在執行且無可辨下一次觸發**。新隔離回歸保證 `running` 不會誤報；`artifacts/benchmarks/service-coverage-20260925T1159-timer-substate.json` 仍只指認實際停擺的 crypto intraday timer。相鄰量測／稽核 **42 個測試通過**。

## 2026-09-25 11:55 動態 service 與可回收空間界線

全服務稽核的 `systemctl show stockagent-*.service --all` 會包括暫時建立的 systemd 單元，不能把 52 直接解釋為 52 個持久安裝的 service。新增讀取 `Transient`／`UnitFileState` 並在報表逐項註明來源；正式唯讀收據 `artifacts/benchmarks/service-coverage-20260925T1155-origin.json` 分成 **51 installed + 1 transient**，後者為當時仍在執行的 `stockagent-legacy-us-stage.service`。對每個 service 的 wall／CPU／記憶體／區塊 I/O 量測仍照常保留，不因 transient 就漏掉其負載。相鄰稽核／量測 **42 個測試通過**。

再次跑核准 `stockagent-data gc --dry-run`：只查到 3 個受控熱快取，2 個 pinned、1 個 lease-active，`would_evict=0`。C 冷 store 最近核准保留計畫僅約 **274,432 allocated bytes** 可回收；這與編譯快取 0 天 dry-run 的約 **182 MB** 相加仍遠不足根碟約 **4.10 GB** 安全線缺口。沒有解除 pin、縮短 lease、碰來源／帳本，亦沒有啟動滯後的 crypto 下載。公開 OpenBB API 實際列 `storage.free_bytes≈103.28 GB`、`minimum_free_bytes=107.37 GB`、`above_safety_floor=false` 且 `health=stopped`，UI 沒有把停機改寫為正常。

## 2026-09-25 11:47～11:49 磁碟再度低於安全線與跨服務實際 I/O 取樣

根碟可用空間降至約 **103,278,145,536 bytes**，低於 OpenBB 原定的 **107,374,182,400 bytes** 安全線約 4.10 GB；OpenBB 仍為 `failed/exit 2`，15:15 自動重試若容量不變會再次被 preflight 拒絕。核准的編譯快取清理器即使用 **0 天**門檻做唯讀 dry-run，也僅有 **771 檔／182,358,016 allocated bytes** 可回收，遠不足缺口，因此沒有為了顯示服務恢復而再刪資料或壓低安全線。`lsof +L1` 沒有發現大檔刪除後仍開啟；此段仍**未證明** 11:20 後約 5 GB 的所有新增占用來源。

全服務取樣增加 cgroup `io.stat` 的 block read/write bytes 增量，只有同一 service invocation 前後持續執行才計算，缺失或重啟保留 `null`；它是實體區塊 I/O，**不是**來源邏輯新增容量。11:48 收據 `artifacts/benchmarks/service-coverage-20260925T1148-io-pressure.json` 當時列 **52 service／40 timer／2 path**：比 11:22 多了其它工作新增、正在跑的 transient `stockagent-legacy-us-stage.service`，不可假設固定永遠 51 個。5 秒同程序寫入最多為隔日沖約 4.997 MB、TW public source events 約 1.294 MB，其餘採樣中的 service 很低；不能把這個短樣本倒推為先前 5 GB 的來源。編譯快取、來源樹、OpenBB preflight 及外部 Windows/D: 活動屬不同邊界。Crypto intraday timer 的模板修復若在此容量下直接啟用，補寫資料可能加深磁碟壓力；須先有容量或經核准的安全退役方案，並在離峰驗證。

## 2026-09-25 11:42～11:44 盤中資料 timer 無下一次觸發：偵測與待離峰載入

重新核對本機 **51 service／40 timer／2 path** 時，`stockagent-registered-data-intraday.timer` 雖為 `enabled/active`，實際 `SubState=elapsed`、`NextElapseUSecRealtime` 空、`NextElapseUSecMonotonic=infinity`，目標 service 為 `inactive` 且本次開機無執行紀錄。timer 9/24 09:10 啟動後仍記住 08:18 的舊觸發；`registered_intraday_runs.tsv` 最後一筆 9/24 08:19 完成。**不能**因 enabled/active 就宣稱盤中一分鐘資料仍有持續刷新。`OnBootSec` 與重啟／開機時鐘交互的精確內部原因尚未單獨重現；可以確認現有 `OnUnitInactiveSec=1min` 在沒有新的 service 完成事件時無法自行重新排程。

模板加入相對**timer 本身啟用**的 `OnActiveSec=1min` 作獨立啟動錨點，保留原完成後 1 分鐘循環及既有資源窗口 `ExecCondition`；不改來源抓取、配額或發布契約。全服務稽核現在同時讀 monotonic/realtime 下一觸發與所有（可重複）`TimersMonotonic` 規則；只對**有完成／啟動相對循環、無下一觸發且目標不在執行**的 timer 發現 `recurring_timer_without_next_trigger`。新收據 `artifacts/benchmarks/service-coverage-20260925T1141-timer-gap.json` 正好報這一條；三個 Shioaji 歷史回補 timer 因其目標仍 `active/running` 沒有被誤判。全資料監控的自動化判定亦改為將 `timer_unarmed` 顯示為無法執行，不再因 `timer_active=true` 或其它低頻工作排程就把停擺的分鐘尾端說成正常；11:44 自然輪本機 API 中 OKX、Bybit、Binance 與 crypto reference 群組均顯示 `unable/timer_unarmed`，頁面已載入 `app.js?v=40`。相鄰 **91 個測試通過**。**模板尚未裝入 systemd，也尚未重啟 timer**；盤中不啟動重型 crypto 下載。離峰須只更新這份 timer、`daemon-reload`、重啟 timer，再驗其下一次觸發、正式 service 收據及資料更新，不能只看 `active`。

## 2026-09-25 11:34 永豐儲存監測：原生串流掃描與錯誤隔離

正式 07:09 儲存監測快照掃了 **4,587,108 檔／107,078,171,233 bytes**，wall **183.376 秒**；其中 `futures_history` **1,903,379 檔／133.32 秒**。此服務只讀本機檔案，不連券商；成本主要是每檔 Python 目錄項與 stat 處理，不是 API 限速。固定同一個 `data_tw_microstructure/captures` 的 **165,176 檔**做唯讀 A/B：原 `_scan_dataset` **4.347 秒**，GNU `find -type f -printf` 串流到相同 Python 聚合為 **0.627 秒**；檔案數、總 bytes、30 個台北曆日分桶及最新 mtime 完全相等。這只是單一群組、單輪局部數字，不可推論整個 459 萬檔服務固定快 7 倍。

現正式掃描路徑改用受信任 `/usr/bin:/bin` 的 GNU `find` 只輸出數值 metadata、保留不跟隨 symlink、原有的 mtime 成長公式與所有資料群組；缺少 `find` 才回退 Python。對原生掃描的非零退出、錯誤輸出格式則**拒絕發布新快照**，不把部分結果標成 `ready`；來源在遍歷時消失由 `-ignore_readdir_race` 按原掃描容忍。回歸涵蓋檔案／目錄 symlink、單檔來源、台北午夜界線、原生與 Python 輸出逐欄相等，以及掃描失敗保留上一份原子快照。此改動不重啟券商、行情或當沖引擎，下一次 14:09 的自然正式 timer 才能驗收全群組 wall／CPU／記憶體、群組檔數與容量；本段不得提前聲稱全量提速或整個 Shioaji 資料健康恢復。

## 2026-09-25 11:20 核准編譯快取緊急回收

根碟停機時可用 **106,773,131,264 bytes**，低於 OpenBB 既有 **107,374,182,400 bytes** 安全線。預設 14 天編譯快取 dry-run 為 0；將**同一核准清理器**的最小檔齡設為 1 天後，唯讀計畫只在 `/root/.cache/torchinductor` 與 `/root/.cache/triton`（受限於 `/root/.cache`）找到 **35,705 檔／1,412,739,072 allocated bytes**；沒有匹配的訓練／編譯程序、開啟檔案或錯誤。已用 `scripts/maintain_storage_pressure.py --min-age-days 1 --apply` 執行，正式收據 `/var/lib/stockagent-storage-pressure/receipts/storage-pressure-apply-20260925T031959.026723Z.json` 記錄**恰好**刪除上述 35,705 檔、1,412,739,072 bytes，0 個變動／開啟中檔被誤刪、0 錯誤；來源、帳本、模型、D 備份、冷 store、materialized cache 均未碰觸。這些編譯快取可由下一次編譯重新產生，但重編譯有時間成本，不能把清理說成零代價。

刪除後同一根碟可用 **108,207,480,832 bytes**，高於安全線約 **833 MB**；`OPENBB_PREFLIGHT_ONLY=1` 的原入口已退出 0、未啟動下載器。OpenBB 服務仍保持 10:53 的 `failed/exit 2`，沒有在市場時段額外啟動；安全重試 timer 會在 **15:15** 自然嘗試。此容量緩衝很薄，可能隨其他寫入再次耗盡；要持續運作仍需擴充根碟或經完整 D 備份／程序引用／租約證據安全退役資料，不能降低 100 GiB 線。

清理後的唯讀全服務重測收據 `artifacts/benchmarks/service-coverage-20260925T1122-post-cache-recovery.json` 仍涵蓋 **51 service／40 timer／2 path**。六個本機產品 GET 均 HTTP 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、永豐 `waiting`、OpenBB `stopped`、全資料 `critical`；OpenBB service 仍是唯一 `failed` 的 StockAgent unit，另有非 StockAgent 的舊 FinLab transient 探測 scope failed。這些狀態與暫時通過的磁碟 preflight 不矛盾，不能在 15:15 前宣稱服務已恢復。

## 2026-09-25 11:15 實存清冊身分遷移驗收與獨立來源缺口

前輪的舊 Parquet 快取身分重驗已由自然 30 秒 timer 完成：持久清冊 **57,296 筆**均有 `file_identity`，公開 `/data-monitor/api/summary` 同樣顯示 `identity_unbound_files=0`、當輪 `identity_rechecked_files=0`；近幾輪 `feature_reused=true`、欄位數 **87,615**。11:12～11:14 的自然輪總 CPU／wall 隨宿主負載約 **2.87～4.53 秒**，包含每輪掃檔與永豐本機狀態工作；不能把身分遷移完成說成全服務延遲已最佳化。來源健康仍是 `critical`。

清冊另外如實列 **19 個無法讀 footer 的 Parquet**：17 個 `data_yahoo/crypto/*_features.parquet`、2 個舊 `data_parquet/*_features.parquet`。使用 PyArrow 逐檔獨立嘗試，19／19 都是 footer magic bytes 缺失，而非只有監控 UI 的分類錯誤。這些檔案不能當已保存可讀的資料；本輪沒有從其他來源猜補內容、覆寫來源、改成 `complete` 或繞過發布稽核。下一步須對這 19 檔逐一找原始來源或已驗證的 D 備份版本，再以正式來源／冷發版程序修復；與 CBC 清單完整度及 OpenBB 磁碟停機分屬不同健康域。

## 2026-09-25 11:08～11:15 央行清單頁面覆蓋驗證

前一輪的 20→60 筆改動揭露了既有完整度漏洞：原先只比較每頁宣稱的**總頁數**，沒有核對「此頁頁碼」、「公告總筆數」、「實際每頁原始列數」及選定頁寬。若 CDN 將同一頁 200 回給不同網址，可能在去重後少筆，卻仍標 `complete`。現在每個線上及快取頁均解析官方頁面自己的頁碼、總列數、實際含日期的原始列數與選定 20／60 筆；全輪要求頁碼精確對應 URL、全頁總列數相同、頁數等於總列數除頁寬向上取整、每頁列數符合首尾頁容量。任何不符即拒絕發布，不把來源錯頁／清單移動冒充完整歷史。

用正式現存 **393 份** 20 筆來源原文逐頁重驗：官方總計 **7,842 列**，逐頁實數相加也是 **7,842**，頁碼／頁寬／首尾列數 **0 異常**；其中有效 `cp-302` 文章連結比原始列少 38，主因為歷史空 href，這 38 個標題均未提到「外匯存底」，故不能錯把「可用文章連結數」當成官方總列數。60 筆版首／末頁現場抽樣分別有 **60／42** 個原始列，總數 7,842、131 頁；上一輪全部 131 頁與舊版的外匯存底文章 URL 集合仍一致。新增重複頁及缺列 fail-closed 回歸；CBC／歸檔／開機相鄰測試 **41 passed**。這是正確性與故障隔離修正，下一次正式 16:30 執行仍需查看完整稽核、耗時與記憶體，不以離線樣本替代。

## 2026-09-25 11:03 OpenBB 容量停機與安全自動重試

新唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260925T1103-cbc-page-size.json` 已列 **51 個 service／40 個 timer／2 個 path**；比 08:59 增加 `stockagent-enrolled-artifact-retirement.service/.timer`，因此原表的 50／39 是歷史快照。六個本機公開 API 仍 HTTP 200，但 OpenBB 產品健康為 `stopped`，不可說全服務正常。10:53 的 OpenBB journal 證實根碟剩餘 **107,127,414,784 bytes**，小於服務固定的 **107,374,182,400 bytes（100 GiB）**，supervisor 依安全契約退出 2，立刻一次重啟的 preflight 仍拒絕；目前 unit 明確 `failed`。這不是 CBC 分頁變動所致，也不是降低空間保留線就能安全修復的錯誤。

正式 compiler-cache 清理的唯讀 dry-run 為 **0 eligible／0 可回收 bytes**；同時刻核准的 C 冷儲存保留計畫僅 **274,432 allocated bytes** 可回收，遠不足容量缺口，故未刪除任何來源／帳本／快取，亦未強制重啟 OpenBB。原 timer 只在開機後與平日 09:12 觸發，這次 10:53 停機若空間稍後恢復會等到下個工作日；現在增加每日 **15:15（台北）** 安全重試，既有磁碟門檻與開盤資源窗口仍在。已更新 timer 模板與本機已安裝檔、`systemd-analyze verify` 通過，只重啟 timer，不啟動 downloader；下一次為今日 15:15，服務仍保留 `failed/exit 2` 真實狀態。容量需實際增大到安全線之上，15:15 自然嘗試與其來源收據才可證明恢復。相鄰開機／來源歸檔回歸 **39 passed**。

## 2026-09-25 央行外匯存底來源歸檔：同契約較大官方分頁

`stockagent-tw-public-release-archives.service` 07:04～07:10 的正式輪總 wall **369.982 秒**；其中 CBC 外匯存底來源抓取 **129.150 秒**。原來源清單使用每頁 20 筆，該輪須掃 **393 頁**；在既有每請求至少 0.25 秒的共享節流下，光順序請求間隔即約 **98 秒**，不能靠加 worker 解決。已存的官方頁面明列每頁 20／40／60 筆，並給出 `lp-302-1-1-60.html` 形式。直接請求 60 筆版第一頁為 HTTP 200、131 頁；只讀抓完整 131 頁、仍用原 0.25 秒節流後，與本機該正式輪的 393 頁來源逐 URL 對照：雙方均 **317 個**外匯存底公告，無新增／漏失，60 筆版只讀 wall **73.019 秒**。這是不同時間與網路狀態下的來源對照，**不是**正式 A/B，也不代表 369 秒總工時已縮短 56 秒。

程式現將正常線上掃描改為官方的 60 筆分頁，保留完整逐頁掃描、頁數一致性、HTTPS 原文 hash、所有公告 detail 驗證與共享請求節流。新 60 筆頁的本機原文前綴為 `page-0060-`，絕不與舊 20 筆頁混讀；離線／既有快取模式若沒有新首頁，仍可使用舊 20 筆完整快取；若新首頁存在而後續頁未齊則拒絕混合。正式來源摘要新增 `listing_page_size`、`listing_pages` 與 listing／detail／Parquet 三段秒數，便於 16:30 下一次自然輪次驗收；**本輪未**啟動正式來源／特徵重建，也未改動任何已發布來源檔。相鄰 CBC／歸檔稽核／協調器／公開進度與來源 catalog **32 passed**，Ruff 與 `git diff --check` 通過；下一輪還需比對正式 `complete`、317 筆或新增來源的合理變化、逐頁收據、官方審核及總 wall，不能僅用較少頁數宣稱業務完成。

## 2026-09-25 10:42～10:49 舊 Parquet 清冊有界重驗及公開欄位增量收據

上一輪加入新項目的 inode／ctime 綁定，但現存約 57,296 個舊快取項尚無此證據。現在由原本每 30 秒的 supervised 資料監控程序，每輪最多重讀 **1,024** 個舊 Parquet footer；新檔／真正變更仍保有獨立的既有 **4,096** 檔額度，不被遷移佔滿。讀前後檔案身分須一致。每輪公開 `record_inventory_progress.identity_unbound_files` 與 `identity_rechecked_files`；前者是**舊版大小＋mtime 快取尚未重驗數**，不是聲稱那些檔案遺失或歷史資料不完整。頁面同時顯示進度，不能把尚未重驗的舊項說成全數已綁 inode。

舊 footer 重讀若得到完全相同的欄位 schema、筆數、null 統計及資料集首末界線，清冊只更新來源身分證據；用持久 `feature_revision` 綁定 53 MB 公開欄位快照收據，**不重寫相同欄位清單**。任一 footer 統計、成員集合或資料集聚合改變即換 revision、重建欄位快照；收據缺失、revision 不符、來源內容 SHA 不符或程式／catalog 依賴更新仍退回完整解析／重建。現有完整 `/data-monitor/api/features` 契約未刪除，公開 gateway 仍驗快照來源與 sidecar。這個 revision 表示欄位投影的語意版本，**不是**Parquet 內容完整 SHA、資料發布或 PIT 合格證明。

自然 timer 10:46:58 一輪記錄 `identity_rechecked_files=1,024`、剩餘 **47,056**、`feature_reused=true`、總 **4.858 秒**；10:48:29 的一輪剩餘 **43,984**、總 **5.423 秒**（其中服務趨勢取樣 460 ms）。同批期間欄位快照保持 87,615 欄、54,576,257 bytes、SHA-256 `3ab2e0f120edd36b41499134ee2c7bc2d1573b6085b9b4667436afb9162a5445`，沒有每輪重新寫 53 MB；這是**來源統計相同期間**的增量收益，遷移中 footer 讀取與 38 MB 清冊原子更新仍令背景輪次高於遷移前約 2.5 秒。10:49 本機頁面 HTML 已載入 `app.js?v=39`，欄位進度 API 有剩餘 **42,960**、JS GET 200；未做外網瀏覽器跨裝置量測。另修正讀取路徑：若快取仍有舊聚合卻把 `files` 或 `schemas` 寫成非物件，不能沿用「已核實」聚合，必須回到逐來源 `scanning`。監控／閘道／服務測速相鄰 **204 passed**，Ruff、Node syntax、diff check 通過。重測可用 `journalctl -u stockagent-data-refresh-status-snapshot.service -o cat | rg 'data_monitor_timing'` 核對每輪重驗、剩餘、feature reuse 與耗時，並核對公開 API 的 `record_inventory_progress` 及完整欄位檔 SHA。

## 2026-09-25 10:34～10:36 實存清冊讀檔競態防護

欄位／筆數清冊的新增 Parquet footer 原先以讀取前的 `size+mtime` 記錄；若來源在 footer 讀取期間被原子替換，當輪可能把舊統計與新路徑配對。現在每次**新讀取** footer 後重查裝置、inode、大小、mtime、ctime；不一致或檔案消失即丟棄該筆快取、保留 `scanning`，下一輪再試。新寫入的快取項亦綁上述檔案身分，使同大小、還原 mtime 的後續替換會重新讀 footer；舊版約 57,296 項仍按原 `size+mtime` 規則，**不能宣稱歷史項已全部重驗**。若要全面升級須規劃有界 footer 重驗，不可在本輪把舊來源冒充已完成新驗證。

測試涵蓋同大小／同 mtime 但 inode 改變，以及 footer 讀取中被替換時不發布舊筆數；資料監控、公開閘道相鄰 **177 個測試通過**，Ruff／差異檢查通過。10:35:53 首次因程式依賴變動而重建欄位收據、`feature_reused=false`，下一輪 10:36:23 恢復 `feature_reused=true`、總約 **2.827 秒**、本機監控 summary HTTP 200。這是防止錯誤統計的健壯性修正，**不是**宣稱 2.5～6 秒背景輪次全面加速；OpenBB L1 仍受盤中 `ExecCondition` 保護，不為測速啟動，FinLab 仍在配額保留線等待。

## 2026-09-25 10:18～10:28 資料監控逐階段測速與日誌掃描

30 秒 supervised snapshot 現已在原有 `data_monitor_timing` 收據中記錄公開投影各階段，以及 Shioaji 的本機收據／journal、歷史管線等子階段；欄位清冊重建另拆成檔案簽章、欄位聚合及輸出列，這些診斷欄位**不加入公開 API**。10:27:50 一輪 `feature_reused=false` 的自然 timer 記錄總 **5,339 ms**（其中 service 趨勢取樣 601 ms、inventory 1,444 ms、public 1,016 ms、feature 2,154 ms）；feature 的原始檔案簽章檢查 **351 ms**、欄位聚合 **728 ms**、輸出列 **279 ms**。10:28:20 無來源變更、重用完整 feature 收據的一輪總 **2,530 ms**，inventory 1,452 ms、public 927 ms、feature 30 ms；仍完整掃描 57,296 個選定 Parquet 的簽章，不能因熱輪較快就冒稱完整來源重建也快。

公開投影中 Shioaji 狀態每輪約 **0.48～0.59 秒**，其中歷史管線／收據約 **0.30 秒**；監控的永豐頁只讀本機收據及有界 journal，沒有登入或消耗行情連線。日誌進度選取原先對 **11,040 個** intraday 舊 log 建立 Path 並全排序，現在只單次掃描找實際最新修改時間的檔案；獨立熱測 5 次約 **87～102 → 72～80 ms**。新舊測試均保留舊檔若續寫即成為最新的語意。自然 timer 的該階段曾量到 207～268 ms，新版兩輪為 66～69 ms，但受並行負載影響，不能把整段差額全歸因於這個局部改動。監控與永豐相鄰 **103 個測試通過**、Ruff 與 diff check 通過；本機 `/data-monitor/api/summary`、gzip `/api/features` 分別 HTTP 200（單次 11／205 ms）。根磁碟仍 95%，全資料健康與其他服務缺口未因此解決。

## 2026-09-25 TAIFEX ATM 單來源增量投影與 Yahoo 缺口核對

ATM 月／週全史原先在新來源使 manifest 失效時，各自重新解析全部 34 份官方選擇權收據；前次正式階段約 185／107 秒。現把每份收據在當時 TX 日盤合約與開盤價下的**每日候選**存成節點本機可替換快取，正式總表仍走同一個 `build_taifex_opening_atm_straddles` 選取、跨收據衝突和缺日補列規則。每輪先完整 SHA-256 驗所有官方來源，快取另綁來源 SHA、直接程式依賴指紋、該收據所有 TXO 日期的 TX 合約／開盤價指紋與投影內容 SHA；資料變動只重建受影響的收據。即使週選擇權該來源為空，仍記錄只有月選擇權的日期，維持 `no_weekly_txo_listing` 與 `missing_txo_daily_partition` 的區分。快取位於 `artifacts/cache/taifex_option_atm_sources`，不進正式 `data_tw_index_options_daily` 冷發布；正式輸出和 manifest 未被此隔離測試替換。

以正式 `manifest.json`／`manifest_weekly.json` 的各 34 份來源、相同 TX 期貨檔在本機隔離輸出測試：月度首次投影 **59.325 秒**、總表 **0.700 秒**，週度首次投影 **50.241 秒**、總表 **0.624 秒**；輸出 SHA-256 分別與正式檔逐位元組相同（`230ade09e2a8181e020e2c7691a64bb12436bf63601a3fe32151a47bcf8787f9`、`927d24092ad8ab768635e15e99f2f4a60651c7215051d1e99ab445207b243448`）。加入快取路徑符號連結檢查後，以最終程式版本重新預熱 34 份來源，月／週冷投影 **65.768／51.227 秒**、熱快取校驗 **0.943／0.608 秒**；再完整核對每份原始來源及 TX 檔 SHA，熱快取建置正式等價輸出分別 **1.552／1.284 秒**（不含來源 SHA 的預先核對），輸出雜湊仍完全一致。這些是隔離測速，並非下一輪正式 systemd wall，也不把不同負載的 185／107 秒當作嚴格 A/B。相鄰 ATM／全鏈／期貨及研究回歸 **76 passed**，Ruff 和 diff check 通過；正式輪仍需觀察新來源、記憶體峰值、發版、讀者與來源耗時。

Yahoo US 本輪 `daily_update_summary.us_stocks.json` 的 **12 failed** 對應 `repair_report.csv` 中同樣 **12 metadata_invalid**；本機 12 個 Parquet 都仍存在，但其中 11 個缺少可信 Yahoo 來源中繼資料，CWAN 標示 `source=cboe`。本輪對這 12 檔的 Yahoo 請求均回空資料，故不能把現存列直接冒充已核驗的 Yahoo 歷史、補寫來源標籤或把來源失敗改成成功。另有 **618 lagging_skip** 按既定延遲政策未補；`stale=12,219` 已對應同輪 `repaired=12,219`，不可重複計作缺口。約 12,219 次修補在現行 10 req/s 客戶端節流下有約 1,222 秒最簡請求下界，觀察到的 1,281 秒主要受此限制，增加 worker 不會突破該界線。本輪只核對證據，沒有改 Yahoo 請求率或未證實資料。

## 2026-09-25 TAIFEX 選擇權完整鏈增量分片

盤後因果時鐘不變：先驗官方日檔收據與 TX 日盤來源，再以同日 TX open 指定研究用相對履約價軸；各 TXO open／close 仍是該合約首／末筆成交價，不是同步 bid/ask 或券商成交。前一輪全量正規化約 740.978 秒，其中月／週 full-chain 約 228.220／170.658 秒；重複逐年解析是主要可減的計算。曾在單一 2025 年官方 ZIP 的 profiler 量到全鏈約 14.85 秒，其中 `_read_txo_rows` 約 9.97 秒。每系列到期日由同日數千個選擇權腿改為每個到期系列計算一次；同一 223,298 列年分區 A-B-B-A 輸出 SHA 完全相同，新舊測速為 7.427／6.860／7.113／7.036 秒，這只是局部小幅改善。

正式月／週完整鏈現在各按**一份官方來源收據**儲存節點本機可替換的驗證投影快取，並保留既有全歷史正式 Parquet 與 manifest。每輪仍對全部原始來源做完整 SHA-256 與品質驗收；分片重用另核對該來源 SHA、受其影響的實際交易日 TX open 指紋、直接程式依賴雜湊和分片 SHA。來源日包含因 TX open 不可用而沒有輸出列的日子，歷史 TX 修正會使相關分片失效；月與週都沒有有效列的單份來源可有空分片，但合併後的整條鏈仍必須非空。合併時拒絕跨收據交易日重疊、欄位契約漂移與分片在讀取時變動；輸出仍原子替換。來源檔完整 SHA 讀取前後及建置完的 inode／size／mtime／ctime 一致性亦在正式 script 驗證，同大小改寫或改連結會拒絕發版。正式發布、期貨／選擇權價格契約和缺漏 fail-closed 不變。

第一次實驗把分片放進 `data_tw_index_options_daily`，經 catalog 檢查發現這是會進冷發布的正式來源，已將**本輪產生的 136 個分片與收據檔、約 20.2 MB** 精確搬至 `artifacts/cache/taifex_option_full_chain_shards`；正式來源下已無 `full_chain_shards`。程式固定使用節點本機快取並拒絕其落在指定正式來源內，避免把可重建資料混入冷發布。這是同檔案系統搬移，沒有刪除原始來源、正式月／週 Parquet 或 manifest。

最終程式版本以既有正式 `manifest.json`／`manifest_weekly.json` 的 **34 份**官方來源、完整來源 SHA 與相同期貨檔唯讀重建：月鏈 2,358,194 列、週鏈 528,406 列；兩者的輸出 SHA-256 與既有正式檔**逐位元組相同**，品質摘要逐欄相同。一次性全分片建置為月 102.445 秒、週 75.886 秒；來源不變的下一輪 34 分片校驗與合併分別為月 0.032＋1.082 秒、週 0.012＋0.306 秒，且輸出 SHA 仍相同。這些是當下負載的唯讀實測，不能把原 228／170 秒與本次 102／76 秒直接當嚴格 A/B 加速比；現有月／週 ATM 全歷史建置約 185／107 秒仍未分片，來源真的變動時也要重新建相應分片。**尚未**在下一次帶新官方來源的正式 systemd 輪次驗收總 wall、記憶體峰值、發布及讀者；目前正式資料維持原樣，timer 下一次預定為今日 17:00 台北時間，屆時才會載入整合路徑。

## 2026-09-25 容量壓力：來源歸因與保留邊界

09:20 左右唯讀量測：根檔案系統 2,163,361,103,872 bytes，可用 108,627,632,128 bytes（約 101.16 GiB），使用率 95%；獨立 D 槽可用 2,613,418,995,712 bytes。根目錄中的 `artifacts` 實際配置約 592.24 GB，其中 `markets` 184.08 GB、`replays` 139.80 GB、`live` 103.28 GB、`cache` 56.03 GB、`audits` 38.69 GB、`maintenance` 24.79 GB。這些是分層佔用，不可相加成「可刪除」容量。現有 Binance archive 要保留檔案系統 10%（目前約 216.34 GB）與估計新資料峰值空間；9/24 最近一次在遠端探索前以 `filesystem_free_below_required_reserve` 暫停，systemd 因 `SuccessExitStatus=75` 顯示 `Result=success`，但業務結果不是資料更新成功。不能為了讓單元變綠而降低保留量。

`replays` 多個各約 5.4～5.5 GB 的 `signals.jsonl` 是主要佔用。既有去重服務明確排除 `.jsonl`、執行中的 `live` 及未完成 run；9/25 最近一次正式去重只回收 24,576 allocated bytes。`artifacts/cache` 的 12 個 `panel_cache_v2` 根下有 26 個 generation，逐一對照現有 `meta.json`／`variants/*.json` 均仍被引用，沒有可證明的孤兒 generation；獨立 cold materialized-cache GC dry-run 亦無可逐出的版本。這次沒有刪除、硬連結、搬移、降配額或更改冷備份政策。若要回收數十 GB，須先為回放／實驗輸出建立精確的 immutable 完成與使用者參照證據，再依獨立 D 備份、執行程序參照和既定退役政策規劃，或擴充根磁碟；目前不能把目錄名稱當成刪除證明。

同一候選樹的去重唯讀盤點原需完整 SHA-256 雜湊 3,717 個不同 inode，首次實測 28.696 秒；但只找到 4 組、5 個重複 inode，潛在實體回收僅 311,296 bytes。現先依不可變的大小、檔案身分／權限／擴充屬性分組，對仍可能相同的檔案各讀開頭與結尾最多 64 KiB 作**排除用**指紋；樣本相同者仍逐檔做完整 SHA-256，正式替換前原有的逐檔完整 SHA-256 重驗及原子硬連結不變。相同樹測得抽樣 3,647 inode、完整雜湊 9 inode，首次 13.808 秒；緊接著以 Git HEAD 舊程式與新版於同一進程做完整逐組 SHA／路徑對照，4 組完全一致，當輪舊／新耗時 19.428／13.695 秒。這是低優先級例行 I/O 減少，不等於釋出 21 GB 空間，也不推論系統所有服務同倍加速；去重／冷啟動相鄰 13 個測試、Ruff 與差異檢查通過。新版會由 9/26 約 03:33 的下次自然 timer 執行，不重啟交易、行情或公開閘道。

## 2026-09-25 Windows／WSL 公開閘道啟動測速腳本部署

先前約 274 秒的 WSL 後端恢復事件，已定位在 Windows 首次派送至同 VM 網路／Linux userspace 可用之前；但當時 Caddy supervisor 沒記每次 `wsl.exe` 子程序退出碼。先把非阻塞完成紀錄部署到 Windows 排程的安裝檔，受控重新啟動**公開 Caddy 排程本身**後，首次紀錄實際得到 `exit_code=-1`；同時後端已健康只表示舊服務仍在運轉，不能證明派送成功。

用與排程相同的 Windows `ProcessStartInfo` 重現：`--distribution "Ubuntu-26.04"` 回 `WSL_E_DISTRO_NOT_FOUND`／`-1`，而 `--distribution Ubuntu-26.04` 退出 0。Windows registry 的預設發行版名稱精確為 `Ubuntu-26.04`，互動 PowerShell 直接呼叫也退出 0；因此根因是這條啟動鏈的引數組法，不是 WSL 發行版不存在。現在對只含字母、數字、點、底線、連字號的發行版名稱不用多餘引號；不符時明確失敗，不靜默啟動其他發行版。新版亦分開記子程序 `StartTime→ExitTime` 與 5 秒 supervisor 輪詢造成的觀察落後，避免把輪詢延遲冒充 WSL 執行時間。相同的唯讀 `systemctl is-active` 呼叫在修正後 `exit_code=0`、回 `active`。

每次部署都先複製到同一 Windows 目錄的暫存檔、驗 SHA-256，再原子替換安裝檔並保留舊檔備份；最後一份備份為 `C:\Users\agari\AppData\Local\StockAgentPublic\start-caddy.ps1.pre-20260925T024547.bak`。排程 action 已核對指向該安裝檔，PowerShell 語法零錯誤，啟動鏈／覆蓋／趨勢 **31 個測試通過**。02:45:54 受控重啟排程載入最終版本，正式 `startup.log` 出現 `exit_code=0 elapsed_seconds=0.186 observed_lag_seconds=5.532`；這是**已運轉 WSL** 上的子程序執行時間，不是冷 VM 啟動延遲。Windows Task Scheduler 工作仍為 running，`caddy.exe` PID **5060、7172** 維持不變，本機與 DDNS 強制 IPv4 HTTPS `/healthz` 均 HTTP 200（各約 1／16 ms 單次樣本）；當沖、TAIFEX 擷取、Discord 的 PID／InvocationID 亦未變。最終唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260925T0246-wsl-argument-fixed.json` 列 **50 service／39 timer／2 path**、launcher 與 Caddyfile 安裝檔皆 `exact_match=true`，排程為 `Running`；`systemctl --failed` 為 0。尚未做 Windows 真冷開機驗收，也未解決外部 IPv6。Task Scheduler 的既存 `LastTaskResult=0x800710E0` 與工作正在執行並存，不能拿該欄單獨證明成功或失敗。

## 2026-09-25 註冊資料全量回補：避免假成功、保留逐輪證據

最近一輪完整收據 `registered-backfill-20260920T022411Z` 約 65,733 秒，以 `completed_with_failures` 結束。步驟收據顯示 Binance 1m 約 30,213 秒、574 檔中 1 檔失敗；OKX 1m 約 65,726 秒、482 檔價格列完成，但歷史特徵仍有 4 檔 `partial`。舊 OKX 程式只把 `progress.json` 標成 failed，程序仍退出 0，導致步驟被算成成功；Bybit 的 `failed`／`repair_required` 來源有同一類退出碼錯誤。Binance 那輪的逐檔報表已被後續 tail 任務覆寫，不能從現在的報表反推當時失敗標的，也不能用新 tail 成功宣稱完整歷史已修復。

現在 OKX／Bybit 在完成最新報表與進度收據後，若來源或特徵不完整即退出非零；完整結果仍退出 0。三家全量回補都把當輪來源報表封存到該次 `step_receipts/<run-id>/<provider>_source/`，後續 tail 任務不再覆寫這份逐輪證據。這是正確性與可追查性修正，**沒有把 18 小時下載變快，也沒有重跑全量來源**。離線回歸 131 個測試通過，包含完整／不完整兩種退出結果、封存後最新報表被替換的情形；下次實際全量回補仍須核對逐檔報表、步驟退出碼、完整特徵與最終收據，不可僅看 systemd 單元 inactive／Result=success。

## 2026-09-25 永豐公開狀態：收據時間一致性與小幅讀取優化

永豐公開狀態僅讀本機收據與有界 journal，不登入券商，也不在行情 callback 中做同步查詢。單獨行程 profiler 實測冷建置約 520 ms、同一行程第二次約 84 ms；769 份期貨歷史 manifest 是主要讀取群之一。原本每份 manifest 解析後再單獨 `stat` 兩次取得顯示時間，檔案若在中間換版，可能把新時間貼在舊內容。現在 JSON 與 mtime 從同一份前後簽章一致的讀取返回，遇到讀取中換版仍拒絕當輪使用。

同機熱讀、四組交錯舊／新函式的 manifest 清單中位數為 27.64／23.36 ms，769 筆輸出逐筆相同；這只是該子步驟的小幅改善，不能推論整頁冷請求同等加速。Shioaji／公開頁／測速相關 138 個測試通過。只重啟公開 gateway 後，本機與 IPv4 HTTPS `/healthz` 均回 `ok`；永豐 API 的 10 項 pipeline 狀態、`degraded` 健康值、回補／擷取狀態及已用流量在重啟前後相同。Shioaji 行情與交易行程未重啟；`degraded` 的來源證據未被 UI 修飾為正常。

公開路由後續三筆本機樣本約 329／2.9／4.3 ms，IPv4 HTTPS 一筆約 21 ms；因該路由有 8 秒新鮮期與背景更新，快取命中不代表冷建置已縮到數毫秒。夜盤服務 journal 明確記錄 `cannot cash-settle cycle while a shadow futures hedge remains open`，於 9/24 14:51 將策略 bootstrap fail-closed、行情改為 data-only；目前公開狀態的 `strategy_bootstrap_ready=false` 與持續行情擷取一致。這是需另查模擬避險腿與到期結算契約的實際策略阻擋，不在網站測速修正中自動清算或重啟交易行程。

## 2026-09-25 備份監看器：安全性與延遲追蹤

`stockagent-packed-backup.service` 的 `current_heads` 閒置對帳沒有複製物件，卻曾在 D 槽逐物件路徑驗證停留很久；這不是雜湊重讀或來源重建。相同 11,834 個物件、121 個當前 release 的三輪正式收據如下，單位 ms。原始值留在當輪 systemd journal；最新一輪亦在 `/var/lib/stockagent-packed-backup/status.json` 的 `stage_timings_ms`。不同時段 D 槽負載可能不同，不把這些差值當作保證加速比。

| 正式對帳 | 來源清單 | D 槽信任檢查 | metadata | 最後狀態前合計 | 結果 |
|---|---:|---:|---:|---:|---|
| 變更前，02:07 完成 | 1,346 | 246,856 | 12,253 | 260,484 | `up_to_date`，零待搬／零錯誤 |
| 變更後，02:11 完成 | 1,268 | 9,746 | 12,368 | 23,416 | `up_to_date`，零待搬／零錯誤 |
| 相同 metadata 略過重複掛載查詢後，02:13 完成 | 1,195 | 9,498 | 10,129 | 20,845 | `up_to_date`，零待搬／零錯誤 |

原因是舊路徑對每個物件在 Windows D 槽重複進行每層 symlink／`resolve` 查詢；80 個隨機物件的唯讀對照約 1,867 ms，單次 `lstat` 約 203 ms。新路徑對每個物件仍查 regular-file 身分、沿用既有 SHA-256 收據的 inode／大小／mtime／ctime 與 30 日重驗條件；只把同一輪的目錄層以 `O_NOFOLLOW` 描述符固定，結尾重查每層路徑是否被替換。未變動的 metadata 已讀比對相同後不用再逐檔重查掛載；每輪起迄與每次寫入前仍核對 D 槽身分。新增檔案、來源變動、checksum 回讀、head 原子提交不變。正式驗收目前為 `current_heads_complete=true`、`present_objects_complete=true`、`remaining_bytes=0`、`historical_completeness=not_checked`；不能由此聲稱歷史版本完整。C 與獨立 D 完成收據的時間、物件數、驗證 bytes 一致。服務重啟後也會立即標示 `checking` 與逐物件掃描進度，不再把前次 `up_to_date` 當成當輪狀態。相關備份／保留測試 36 個通過，D 槽 80 個隨機物件新舊身分結果一致。

`metadata` 最後一輪仍約 10.1 秒，是下一個已量到的成本；未在缺乏逐子階段證據前放寬 manifest、head 或 object 的完整性驗證。

02:15 的唯讀全服務重測收據為 `artifacts/benchmarks/service-coverage-20260924T181532Z.json`：50 service／39 timer／2 path、10 秒資源取樣，沒有 systemd `failed`。這不能替代產品狀態：六個 localhost API 均 HTTP 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、永豐 `degraded`、全資料 `critical`；OpenBB 為 `active`。這次 Shioaji 狀態請求約 336 ms，仍有 receipts／來源查核成本；備份優化沒有處理該服務。CPU／memory／I/O PSI 的 10 秒平均都為 0，只能代表取樣時段，不能推論日間尖峰。

本清單由實際安裝在 penguin WSL 的 systemd unit files 與當下 runtime 狀態產生，作為逐項測速與優化基準。`inactive/dead` 對一次性 service 是正常待命；`failed` 是失敗。資料新鮮度另看收據。

下表列出 02:14 再枚舉的 **50 service／39 timer／2 path**。狀態欄保留 9/23 13:10 的原始基線；當時尚未安裝的 FinLab 兩項明確標記，不把歷史狀態當成現在健康。即時完整狀態請看 `artifacts/benchmarks/service-coverage-20260924T0214-corrected.json`。

## 50 個已安裝 service

| 類別 | Unit | 安裝方式 | 13:10 狀態 | 對應 timer |
|---|---|---|---|---|
| 儲存／維運 | `stockagent-artifact-dedup.service` | static | inactive/dead | 有 |
| 加密／註冊資料 | `stockagent-binance-public-archive.service` | disabled | inactive/dead | 有 |
| 加密／註冊資料 | `stockagent-crypto-training-refresh.service` | disabled | inactive/dead | 有 |
| 儲存／維運 | `stockagent-data-cache-gc.service` | static | inactive/dead | 有 |
| 儲存／維運 | `stockagent-data-refresh-status-snapshot.service` | static | inactive/dead | 有 |
| Discord | `stockagent-discord-artifact-maintenance.service` | disabled | inactive/dead | 有 |
| Discord | `stockagent-discord-bot.service` | enabled | active/running | — |
| Discord | `stockagent-discord-postclose-cache.service` | disabled | inactive/dead | — |
| FinLab 帳號研究資料 | `stockagent-finlab-local-refresh.service` | disabled | 當時未安裝 | 有 |
| FinLab 帳號配額觀測 | `stockagent-finlab-quota-snapshot.service` | disabled | 當時未安裝 | 有 |
| 儲存／維運 | `stockagent-hot-artifact-sync.service` | disabled | inactive/dead | — |
| OpenBB | `stockagent-openbb-archive.service` | static | active/running | 有 |
| OpenBB | `stockagent-openbb-l1-compaction.service` | static | inactive/dead | 有 |
| 儲存／維運 | `stockagent-packed-backup.service` | enabled | active/running | — |
| 儲存／維運 | `stockagent-packed-retention.service` | static | inactive/dead | 有 |
| 公開網站 | `stockagent-public-dashboards.service` | enabled | active/running | — |
| 加密／註冊資料 | `stockagent-registered-data-backfill.service` | disabled | inactive/dead | 有 |
| 加密／註冊資料 | `stockagent-registered-data-daily.service` | disabled | inactive/dead | 有 |
| 加密／註冊資料 | `stockagent-registered-data-features.service` | disabled | failed/failed | 有 |
| 加密／註冊資料 | `stockagent-registered-data-intraday.service` | disabled | inactive/dead | 有 |
| 儲存／維運 | `stockagent-remote-cold-artifact-ingress.service` | static | inactive/dead | 有 |
| 永豐／行情 | `stockagent-shioaji-historical-market-data.service` | static | active/running | 有 |
| 永豐／行情 | `stockagent-shioaji-minute-backfill.service` | static | active/running | 有 |
| 永豐／行情 | `stockagent-shioaji-storage-monitor.service` | static | inactive/dead | 有 |
| 永豐／行情 | `stockagent-shioaji-taifex-bidask.service` | enabled | active/running | — |
| 永豐／行情 | `stockagent-shioaji-taifex-dashboard.service` | enabled | active/running | — |
| 永豐／行情 | `stockagent-shioaji-top200.service` | enabled | active/running | — |
| 永豐／行情 | `stockagent-shioaji-tx-history-backfill.service` | static | active/running | 有 |
| 儲存／維運 | `stockagent-storage-pressure.service` | static | inactive/dead | 有 |
| TAIFEX 官方資料 | `stockagent-taifex-auxiliary-daily.service` | disabled | inactive/dead | 有 |
| TAIFEX 官方資料 | `stockagent-taifex-futures-daily.service` | static | inactive/dead | 有 |
| TAIFEX 官方資料 | `stockagent-taifex-public-history.service` | disabled | inactive/dead | 有 |
| 時鐘 | `stockagent-time-sync-check.service` | static | inactive/dead | 有 |
| 當沖 | `stockagent-tw-day-trade-eligibility.service` | disabled | inactive/dead | 有 |
| 當沖 | `stockagent-tw-day-trade-margin-actions.service` | static | inactive/dead | 有 |
| 當沖 | `stockagent-tw-day-trade-minute-curves.service` | static | inactive/dead | 有 |
| 當沖 | `stockagent-tw-day-trade-multi-basis-22-history.service` | static | inactive/dead | 有 |
| 當沖 | `stockagent-tw-day-trade-preopen-gate.service` | static | inactive/dead | 有 |
| 當沖 | `stockagent-tw-day-trade-simulation.service` | enabled | active/running | — |
| 當沖 | `stockagent-tw-day-trade-unattended-guardian.service` | static | inactive/dead | 有 |
| 隔日沖 | `stockagent-tw-overnight-history.service` | static | inactive/dead | 有 |
| 隔日沖 | `stockagent-tw-overnight-simulation.service` | enabled | active/running | — |
| 台灣公開資料 | `stockagent-tw-public-0830-check.service` | disabled | inactive/dead | 有 |
| 台灣公開資料 | `stockagent-tw-public-cold-publish.service` | disabled | inactive/dead | 有 |
| 台灣公開資料 | `stockagent-tw-public-feature-reconcile.service` | static | inactive/dead | 有 |
| 台灣公開資料 | `stockagent-tw-public-official-catalogs.service` | static | inactive/dead | 有 |
| 台灣公開資料 | `stockagent-tw-public-publication-sweep.service` | disabled | inactive/dead | 有 |
| 台灣公開資料 | `stockagent-tw-public-release-archives.service` | static | inactive/dead | 有 |
| 台灣公開資料 | `stockagent-tw-public-source-events.service` | enabled | active/running | — |
| 儲存／維運 | `stockagent-wsl-backfill-memory-reclaim.service` | static | inactive/dead | 有 |

## 39 個已安裝 timer

| Timer | 安裝狀態 |
|---|---|
| `stockagent-artifact-dedup.timer` | enabled |
| `stockagent-binance-public-archive.timer` | enabled |
| `stockagent-crypto-training-refresh.timer` | enabled |
| `stockagent-data-cache-gc.timer` | enabled |
| `stockagent-data-refresh-status-snapshot.timer` | enabled |
| `stockagent-discord-artifact-maintenance.timer` | enabled |
| `stockagent-finlab-local-refresh.timer` | enabled |
| `stockagent-finlab-quota-snapshot.timer` | enabled |
| `stockagent-openbb-archive.timer` | enabled |
| `stockagent-openbb-l1-compaction.timer` | enabled |
| `stockagent-packed-retention.timer` | enabled |
| `stockagent-registered-data-backfill.timer` | enabled |
| `stockagent-registered-data-daily.timer` | enabled |
| `stockagent-registered-data-features.timer` | enabled |
| `stockagent-registered-data-intraday.timer` | enabled |
| `stockagent-remote-cold-artifact-ingress.timer` | enabled |
| `stockagent-shioaji-historical-market-data.timer` | enabled |
| `stockagent-shioaji-minute-backfill.timer` | enabled |
| `stockagent-shioaji-storage-monitor.timer` | enabled |
| `stockagent-shioaji-tx-history-backfill.timer` | enabled |
| `stockagent-storage-pressure.timer` | enabled |
| `stockagent-taifex-auxiliary-daily.timer` | enabled |
| `stockagent-taifex-futures-daily.timer` | enabled |
| `stockagent-taifex-public-history.timer` | enabled |
| `stockagent-time-sync-check.timer` | enabled |
| `stockagent-tw-day-trade-eligibility.timer` | enabled |
| `stockagent-tw-day-trade-margin-actions.timer` | enabled |
| `stockagent-tw-day-trade-minute-curves.timer` | enabled |
| `stockagent-tw-day-trade-multi-basis-22-history.timer` | enabled |
| `stockagent-tw-day-trade-preopen-gate.timer` | enabled |
| `stockagent-tw-day-trade-unattended-guardian.timer` | enabled |
| `stockagent-tw-overnight-history.timer` | enabled |
| `stockagent-tw-public-0830-check.timer` | enabled |
| `stockagent-tw-public-cold-publish.timer` | enabled |
| `stockagent-tw-public-feature-reconcile.timer` | enabled |
| `stockagent-tw-public-official-catalogs.timer` | enabled |
| `stockagent-tw-public-publication-sweep.timer` | enabled |
| `stockagent-tw-public-release-archives.timer` | enabled |
| `stockagent-wsl-backfill-memory-reclaim.timer` | enabled |

## 2 個已安裝 path unit

| Path | 安裝狀態 |
|---|---|
| `stockagent-discord-artifact-maintenance.path` | enabled |
| `stockagent-tw-day-trade-minute-curves.path` | enabled |

## 關聯的非 StockAgent systemd 服務

- `syncthing@root.service`：目前 active/running。本機程序活著，不代表所有 peer 已同步或冷資料可重建。
- Windows 排程 `StockAgent Public Caddy`：Windows 工作狀態 Running（TaskState 4），提供公網 HTTPS；`StockAgent Preserve Crash Dumps` 為 Ready；`StockAgent One-Time WSL Filesystem Repair` 與 `WSL Daily Backup - Ubuntu-26.04` 為 Disabled。
- 本機端口：公開 gateway `127.0.0.1:8770`，TAIFEX `127.0.0.1:8765`，當沖引擎面板 `127.0.0.1:8766`，Syncthing 管理介面 `127.0.0.1:8384`。Caddy 是 Windows 排程，未在 WSL systemd 以 unit 呈現。

## Repo 中有模板、但本機未安裝

- `stockagent-cold-artifact-maintenance.service/.timer.in`
- `stockagent-tw-mops-xbrl.service/.timer.in`

這些是可部署定義，不計入本機已安裝服務性能達標。

## 初始異常與測量邊界

- `stockagent-registered-data-features.service` 當下 `failed/failed`；不能把失敗工作算成零延遲。
- `stockagent-registered-data-intraday.service` 在首次盤點時 `activating/start`；完成／失敗需看終端收據。
- 交易、資料、儲存、Discord 的終端驗收須各依正式收據與 source/ledger，不因 unit `active` 或 HTTP 200 直接宣告正常。
- 本輪「所有」指這台 WSL 目前已安裝的 50 個 StockAgent service、39 個 timer、2 個 path，以及相關 Windows Caddy 與 Syncthing；最初 9/23 的 48／37 是 FinLab 兩項安裝前的盤點。遠端 Vast、路由器內部服務、其他 Windows 應用不能從本機清單推斷已驗證。

## 第一輪全服務與全路由實測

原始可重測收據：`artifacts/benchmarks/all_services_latency_2026-09-23.json`。`systemctl show` 對全部 48 個 service 查詢約 153 ms；最近一次完成的 job 中，`registered-data-features` 1,550.8 秒、exit 1，不能算作完成吞吐。其餘較久的成功 job 有 Binance public archive 1,544.7 秒、registered daily 1,312.5 秒、TAIFEX auxiliary daily 833.9 秒、TW public release archives 574.7 秒。這些是**不同工作量**的耗時，不可直接互相比快慢；持續型 daemon 的運作時間也不是請求延遲。

在 11.97 秒的 48-service 資源取樣內，TAIFEX dashboard 約 2.01 CPU cores、公開 gateway 約 0.64、registered intraday 約 0.38。這與同時進行的 HTTP 壓測重疊，不能外推為閒置常態。TAIFEX `marks.jsonl` 約 3.97 GB，mtime 為 9/18，取樣時服務仍按 55 秒 TTL 重建歷史；這是可避免的來源不變重算。

32 條公開 gateway 路由各有 8 個請求，HTTP 錯誤數 0。當沖完整 1m 歷史中位數約 2,294 ms，14.70 MB 解碼、約 3.83 MB gzip；TAIFEX 1d 歷史約 349 ms，其餘多數路由為個位數到百毫秒。這是 loopback 量測，不是 WAN、瀏覽器繪製或開盤訊號端到端延遲。

`registered-data-features` 的 9/22 收據有 574 個 Binance symbol，其中 569 個 partial，569 個均為 taker 統計分頁無前進；舊的根目錄 `data_binance/download_summary.json` 並非目前 1m 任務收據，最新收據在 `data_binance/1m/`。不能因新的分頁單元測試通過就宣告完整歷史資料健康。新的有界分頁先用官方最近三天 BTCUSDT 5m 資料驗證：865 筆、無中間缺口或重複；全量排程結果如下。

14:00 排程已於 14:25:47 完成，service `Result=success`、exit 0；不能只看 exit，另查特徵報表：Binance 574/574 `updated`、各階段 `ok`、0 個錯誤，原 569 個 taker 分頁錯誤為 0；OKX 486/486 `updated`、0 個錯誤。Binance 全步驟約 1,467 秒，整個 features cycle 約 1,547 秒；它是數千個外部資料項目的取得成本，不應拿來與毫秒 HTTP 路由直接比較。同步執行的 intraday job 於 14:24:56 覆寫 `data_binance/1m/download_summary.json` 的「本輪特徵啟用」欄位；本次特徵驗收使用有當次 mtime、574 個逐 symbol 階段及錯誤欄位的 `historical_feature_report.csv`，不把後來另一輪的旗標誤作本輪失敗或成功。

## 已實作並部署的延遲修正與重測

同樣的 32 條路由、每條 8 次請求重測收據為 `artifacts/benchmarks/all_services_latency_2026-09-23_after.json`。所有路由仍是 HTTP 200；當沖完整分鐘歷史中位數由約 2,294 ms 降至約 904 ms。這是相同機器與併發設定下的兩次樣本，**不是**每次請求的保證，也不是跨網路端到端時間。來源沒有變更時，TAIFEX 面板不再每 55 秒掃描 3.97 GB 的 marks；相同壓測下服務 CPU 取樣由約 2.01 cores 降至約 0.006 cores。這個改善是移除無效重算，不影響來源異動時的背景更新與失效檢查。

重測的當沖完整歷史約 904 ms 中位數，伺服器處理約 40 ms、回應體傳輸／解壓約 835 ms、JSON 解析約 245 ms（各分位數**不能相加**，樣本與併發重疊）。因此再微調 Python 迴圈已非主要優先級；後續要比較分區增量／二進位序列化的端到端收益，同時保留每分鐘點與完整來源證據。TAIFEX 1d 歷史約 212 ms 中位數，伺服器約 0.13 ms，主因同樣是回應體處理而非後端查詢。

當沖與隔日沖前端現在只在所選日期或歷史來源版本變動時，才重新下載大型分鐘曲線；狀態／訊號更新仍可個別刷新。完整歷史 gzip 壓縮使用實測較快的 level 3；傳輸位元組略增，壓縮 CPU 降低。當沖資格排程若已能以穩定的 TWSE／TPEx 官方收據驗證當日完整覆蓋，會略過不相關的全域 writer lock；來源異動或缺漏仍必須經原流程取得並驗證，不用舊資料冒充新資料。本機相同日期覆蓋檢查約 37 ms，09:15 排程後的實際耗時尚待次日驗證。

持倉與完整生命週期的歷史定位索引改為 schema v2：保留每份來源檔的原始定位列，僅解碼來源簽章變動的檔案，最後仍按原檔案順序合併並讓較後檔案覆蓋相同 position ID。即使原先覆蓋它的檔案被刪除，前一份檔案的紀錄會恢復。隔離副本基於 725 份實際來源、47,902 個定位列：單檔更動後持久索引增量建置 1.129 秒，同來源強制全掃 5.481 秒，結果逐筆一致；原始檔未被修改。`artifacts/benchmarks/tw_position_index_2026-09-23.json` 可重測。另按 profiler 找到 725 次 `Path.relative_to` 佔約 177 ms，改為受根目錄前綴檢查保護的相同相對路徑後，同一持倉頁 profiler 樣本由 322 ms 降至 65 ms。正式 gateway 已在收盤後套用，第一次重啟建置約 2.4 秒，後續一次不命中頁面快取的後端建置約 56 ms；這不是公網端到端保證。

公網瀏覽器驗收涵蓋 8 頁，1366×768 與 390×844 各一輪：兩者均無控制台錯誤、API 失敗或頁面橫向溢出；手機輪另無過小觸控目標。HTTPS IPv4 可連，但 WSL 對 DDNS AAAA 的 IPv6 連線仍失敗；Windows 本機 IPv6 443 listener 與防火牆規則存在，不能以 IPv4 結果宣稱外部雙棧完成。

追加完整日期區間測試後發現「快速的最新一天」不能代表完整帳本：當沖 2/25～9/23 的訊號／事件頁，無命中來源快取的本機端點分別約 3.62／4.78 秒。事件頁過去每次都重新讀取最多各 10 萬筆委託與成交，且未向 UI 告知截斷；現在加入依帳本與正式歷史檔 `device/inode/size/mtime_ns` 失效的 32 頁有界結果快取，以及逐來源掃描筆數、上限與截斷警示。來源不變時同一事件查詢約 0.24 秒、再一次約 0.19 秒；**首次不同查詢仍可能花數秒**，未冒稱冷查詢已完成最佳化。帳本追加即失效的單元測試已通過。兩個完整區間路由也列入持續測速清單；39 路由×5 次、HTTP 錯誤 0 的收據在 `artifacts/benchmarks/all_services_latency_2026-09-23_final.json`。該收據中完整區間事件頁的 first 僅 28 ms，是先前人工查詢已預熱同一 key 的結果，不可當成真正冷查詢。

當沖完整區間訊號原始投影是 100,000 列、約 34 MiB；頁面仍需套用即時狀態與持倉，不能只把整頁長時間凍結。新增最多 2 個、每個最多 64 MiB 的來源投影快取，僅在帳本 `device/inode/size/mtime_ns` 與所選日期均不變時重用；即時狀態版號仍使最後頁面重算。部署後新程序第一次相同完整區間查詢約 6.01 秒，帳本不變但改用「受阻」篩選約 0.288 秒，再查同一頁約 0.005 秒；來源追加失效與狀態版號改變時重用投影，均有測試。冷查詢數字在不同樣本間波動，不能把 0.005 秒當冷啟動性能。

最終修改後又在公網 Chromium 重測當沖頁：1366×768 與 390×844 都沒有橫向溢出或 JavaScript 例外，手機觸控目標風險 0；完整分鐘曲線保留 317,540 個點／8 個序列。桌面讀取與繪製約 1.078 秒，手機約 0.761 秒；這是一次端到端瀏覽器樣本，不是 P95 保證。報告在 `/tmp/stockagent-dashboard-browser-audit-2026-09-23-postfix*/`，重測時仍有 Permissions-Policy `web-share` 瀏覽器警告，無 JS 例外。

最後一輪手機瀏覽器驗收在切換日期時出現 3 個舊請求 `AbortError`，同一新選擇的端點後續均 HTTP 200；這是前端主動取消過期請求，不是資料服務健康證據，也不能把該輪表述為「完全沒有取消」。

串接驗收發現安全輸出白名單原本未傳出新事件截斷欄位，已補 `scan_limit`、`scan_limit_reached`、逐帳本 `source_rows_scanned`，並保留原始委託／部位 ID 遮罩。公開 HTTPS 完整區間回應已驗證 HTTP 200、`scan_limit_reached=true`、委託與成交各掃 100,000 筆；重啟後第一次事件冷查詢約 7.9 秒，後續同來源快取較快。最終修改後的手機公網 Chromium 再驗證：無橫向溢出、觸控目標風險 0、JS 例外 0，完整曲線仍有 317,540 點。

## 尚未達成與下一輪量測邊界

- `registered-data-features` 的 9/22 失敗已由 9/23 14:00 的完整排程驗收消除；但目前證據只涵蓋這次 574-symbol Binance 與 486-symbol OKX 特徵工作，以及各自報表列出的近期取得區間，不等於保證未來供應商都不缺資料或不再限流。
- TAIFEX auxiliary daily 最後約 834 秒，其中 option daily 會在每個交易日對多年 immutable 官方收據反覆解析，並分別重建月／週 ATM 與 full chain。本輪加入各階段 journal 耗時與同日重試的嚴格重用快路徑：官方收據、期貨檔、直接依賴程式、正規化輸出與 full chain 的雜湊都相同才跳過重算；它不會縮短**有新來源的每日完整建置**。下一步仍需不可變分區投影與增量合併，並以完整輸出逐欄一致、source provenance 與缺漏 fail-closed 驗收；目前**未**宣稱已修好此段。
- Registered daily 約 1,313 秒，其 Yahoo US 股票更新為主要工作；其收據仍含 failed symbol。不能只靠減少抓取量改善表觀耗時，必須先分開 provider 回應、已退市／不存在、更新與本地重算成本。
- `registered-daily` 的逐步收據確認 Yahoo US 股票約 1,294 秒（12,407 個 symbol，12,378 `repaired`、10 `new_symbol_repaired`、6 `not_found`、13 `failed`），CoinMetrics 約 349 秒，其餘主要步驟各低於 91 秒；並行執行使逐步耗時不可相加。現有 Yahoo `us_stocks` 約 8.8 GB、每個 symbol 的單一歷史 Parquet 在增量更新時仍會重寫，這是值得以不變歷史分區＋可驗證 tail 試驗的成本點，但不能在未保證退市股票、schema 與完整讀者相容前直接拆檔或刪除失敗 symbol。
- 09:00 五個模式的 ready gate 實測約 2.68–6.67 秒；第一筆永豐行情收據本身約 1.31 秒、覆蓋證據約 2.16 秒，現階段不能宣稱已達 1 秒端到端。盤前預備、行情到達、推論、持久化與通知仍要分段追蹤，不能用回放時間代替即時延遲。
- 9/23 的 opening latency 收據實際有**五**個模式：第一個 `attention_layernorm` 排程醒來約 6.7 ms，但需等行情覆蓋到 2.163 秒，訊號 ready 於 2.676 秒；其餘四個模式依序 ready 於 3.581、4.671、5.700、6.673 秒。模型推論本身各約 31–63 ms，後四個模式的等待與序列化比推論大得多。這是進一步設計共用來源快照與可驗證併發的證據，不能直接取消模型／帳本鎖或把第二個以後模式當成已完成。
- `tw-day-trade-margin-actions` 曾在今日 07:54 因衍生收據過期失敗而自動重試，後續 08:01 完成；當沖資格 09:15 一次耗 315 秒主要是等不相關 writer lock。服務最終 exit 0 不抹去該次延遲和失敗。
- `crypto-training-refresh` 今日 02:30 的計算已完成資料稽核，但發布時 `download_bybit_perp_1m.py` 正在寫入，正確的發布閘門拒絕了它；不能把這種失敗改為通過。現有 intraday 排程每次結束後 1 分鐘又啟動，單靠把訓練刷新改成另一個固定時刻仍可能競爭。需設計來源穩定快照／共同租約或可驗證的增量恢復，並確認不延緩盤中原始資料更新。
- 今日 systemd 錯誤日誌還有 08:30 官方資料閘門、09:05 盤前閘門與幾次 intraday 收集失敗。09:05 盤前閘門觀察到 2 個尚未套用的來源事件而 fail-closed；來源事件監測在 09:05:27 顯示未套用數回到 0。現在 `events/latest.json` 為 `ok` 不等於當時的失敗沒有發生，也不能由此證明明天盤前不會重演。
- 「全部服務到極限」不存在可從單日樣本證明的絕對界線；各個排程資料量、外部配額和硬體負載都會變。清單是完整作用域，這輪的修正與尚未消除的瓶頸分開記錄，不把其他服務的未量測情境稱為已最佳化。
- 14:14 的公開全資料摘要為 `critical`：326 個啟用資料端點中 221 complete、39 catching up、66 unable。14:25 全量特徵完成後重讀仍為 `critical`，數量變成 223 complete、38 catching up、65 unable。這是資料健康，不是 HTTP 或前端性能；不可因服務與面板正常回應就把其餘來源改成正常。
- `unable` 分散在臺灣官方公開資料、即時 Tick／五檔、TAIFEX Tick、台股分鐘／微結構、Yahoo、Binance、Dune、Pepperstone、免費市場脈絡與數個研究投影群組；這些需要逐一依 source/receipt、配額、憑證與實存覆蓋排障。不能從公開摘要推斷為同一個效能瓶頸，更不能藉刪除來源或改顯示狀態達成「變快」。
- 14:50、14:50:39、14:51、14:52 的 `stockagent-tw-public-publication-sweep` 四次實際嘗試均失敗：8 項中 6 項成功，但 TPEx 當日法人交易表為已驗證開市日無列，TWSE 法人交易回應非有效 JSON；摘要 `coverage_complete=false`、`data_status=incomplete`、exit 1。下次既有 timer 為 15:30；未把 6/8 當 8/8、未人工補造官方來源，也未因單次失敗去重啟交易程序。該工作每次約 15～39 秒、記憶體尖峰約 6.5 GiB；重複讀取多年的資料是下一個可測的成本候選，但先要保持官方來源的缺漏語義。

## 逐服務測速覆蓋與開機鏈追加（15:12～15:30）

新增可重複、唯讀的 `scripts/audit_service_latency_coverage.py`：一次列出全部已安裝的 StockAgent systemd 服務、timer、path，對每個服務區分「已完成 job 最近一次程序耗時」與「常駐程序取樣 CPU／記憶體」。後者**不是**一次功能請求的延遲。執行方式：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_service_latency_coverage.py --sample-seconds 10
```

本輪原始收據 `artifacts/benchmarks/service-coverage-2026-09-23T1512.json` 有 48 service、37 timer、2 path；實際取樣 13.38 秒。當時永豐分鐘回補約 2.37 CPU cores、註冊盤中資料約 1.48 cores、公開 gateway 約 0.03 cores。CPU 是該段負載與程序共享資源的觀察值，不代表各自的任務吞吐或瓶頸必然固定。`publication-sweep` 當時最近一次失敗程序約 22.50 秒；15:30 新一輪已開始，該時點的 `systemctl is-system-running=running` 僅因失敗狀態被新執行取代，15:30 的資料報表仍為 5 項成功、2 項失敗、`coverage_complete=false`，不可用 systemd 顏色取代資料驗收。

擴充 Windows 枚舉後再跑的收據 `artifacts/benchmarks/service-coverage-20260923T073151Z.json` 仍涵蓋 48／37／2、取樣 12.95 秒，另明列 4 個 Windows 排程。當時 `publication-sweep` 2.77 cores、`registered-data-intraday` 2.47、Discord artifact maintenance 1.02；程序樹指出前者正在用 8 workers 重建當日官方標的 Parquet，約 17 GiB RSS。新收盤日輸入發生變化，這次重建不能簡單跳過；若要縮短，須針對官方 panel 的歷史依賴、除權息與生命週期校正做可驗證的增量分區，而非刪除來源或略過正式稽核。此成本仍列為未完成優化。

官方標的建置原本只報整步耗時，無法知道成本在來源合併、symbol 分區、並行逐檔建置，還是 metadata 收尾。已在既有正式 summary 增加 `stage_elapsed_seconds` 四段與總計，保持原有資料／價格運算不變；56 個相關 builder 測試通過。15:30 已啟動的程序載入的是舊版程式碼，**不會**有這些新增欄位；待下次正式建置後，才能用其收據決定哪一段值得改造並驗證收益。

15:38 補跑那輪已載入新版標的 builder，其正式 summary 實測：來源校驗與合併 33.114 秒、symbol 分區 0.314 秒、8 worker 逐標的建置 56.618 秒、metadata 收尾 0.041 秒，寫收據前總計 90.088 秒。瓶頸不是分區或收尾；日後要先處理來源變動判斷及需要重算的 symbol／歷史區間，再比較 CPU 微調。

15:36 同時執行的官方特徵建置約 28 GiB RSS 與 Discord 歷史推論約 42 GiB RSS，WSL 記憶體 `full` pressure 60 秒平均約 8.64%，顯示並行工作已出現可量測的資源爭用。Discord artifact worker 原本只在整輪開頭查官方來源 writer／開盤／互動優先閘門；一輪會連續跑多個大模型，期間開始的官方建置就和下一個模型重疊。現改為**每個**昂貴模型開始前再查一次三個既有閘門，官方 writer 正忙時使用原有有界等待與 `waiting_source` 收據，逾時則明確 `deferred` 而非冒充完成。這不會中斷已在跑的模型，也不會把正式歷史的 1,800 秒推論 timeout 縮短；當前已啟動的 worker 不會立即套用。相關閘門測試 4 個通過，下一輪仍須比較來源發布與整輪 artifact 完成耗時，不能先宣稱總吞吐一定增加。

第三份全服務收據 `artifacts/benchmarks/service-coverage-20260923T074122Z.json` 加入 host PSI：當時官方 sweep 約 12.94 CPU cores（本機 16 cores），記憶體 `full` 60 秒平均 11.35%，I/O `full` 4.83%；這是宿主整體壅塞，不能把百分比全歸給某一個 unit。15:30 的官方 sweep 於 15:38:54 成功結束，systemd 記錄 wall 8 分 54 秒、CPU 累計 47 分 7 秒、35.2 GiB memory peak、7.2 GiB swap peak。它隨即因先前 30 秒排程點而再啟動；兩次來源報表皆為 5 ok／2 failed，且 `changed_dataset_count=0`。第一輪衍生重建收據細分股票 panel 107.7 秒、公開 feature panel 369.6 秒；在來源真正未改變時，重建這些下游不是有效吞吐。

追查發現 TAIEX 官方月資料下載器每次完成 overlap polling，會因 `_downloaded_at_utc` 改變而重寫**價格完全相同**的完整歷史 Parquet 和成功 summary；其 byte SHA 變動使股票 panel／feature 的正式來源依賴校驗失效。修正為：仍照原規則向官方查詢、驗證所有月分並在 `latest_attempt` 記錄此次觀測，但若已驗證的 canonical 收據、起訖範圍及除下載時間外的全部輸出欄位完全相同，就保留 canonical Parquet 和其成功 summary 的原始 bytes；只要任一價格或其他語義欄位變動，就按原流程原子寫入新版本。19 個 TAIEX 下載器測試通過，其中明確驗證「同值刷新不改 canonical／summary」及「收盤指數修正會更新」。目前已在跑的第二輪仍使用舊程式，下一輪才可驗證這項修正對正式下游耗時的實際效果；獨立官方資料的 2 項缺漏並未因此消失。

Windows 啟動鏈目前可唯讀確認 4 項相關排程：`StockAgent Public Caddy` Running，`StockAgent Preserve Crash Dumps` Ready；`StockAgent One-Time WSL Filesystem Repair` 與 `WSL Daily Backup - Ubuntu-26.04` Disabled。Caddy 任務含 Boot／Logon／Time triggers、S4U 登入，Windows 80/443 listener 屬於 Caddy 程序，已安裝 launcher 與 Caddyfile 的 SHA-256 均與 repo 來源一致；WSL `/etc/wsl.conf` 啟用 systemd、gateway `/healthz` 回 HTTP 200。這仍只證明**現在的設定與存活**；未執行會中止行情、交易與回補的 `wsl --shutdown`／實際 Windows 重開機測試，故「無人登入後的冷開機自行恢復耗時／成功率」尚未驗證。上述四個 Windows 排程現在也由測速腳本枚舉，不僅硬編碼 Caddy 一項。

永豐分鐘回補的 14:45 截止只約束新的 **API 查詢連線**。15:09～15:20:54 的高 CPU 實為不登入券商的 `--local-only` 日線重建，2,757 檔中 2,339 complete、127 合約不可用、291 在來源期外、失敗 0；後續混合資料建置到 15:23:54、正式稽核於 15:24:48 `status=ok`，source-gap fallback 保留 68 筆。之後服務為了 89 個歷史 source-gap 等候下個安全連線窗口；程序 `active` 並不表示仍佔用 API 連線。這一輪不能被新程式碼倒算成已提速。

造成 `--local-only` 每日成本的根因是日線 summary 的目標結束日每天變動，既有「同一 manifest SHA」快取無法命中，單檔重讀多年來的 83 個分鐘區塊，再聚合與校驗。已新增逐區塊來源簽章的增量契約：已驗證且未變的前段沿用原日線，從第一個變動區塊開始重新驗證、聚合；前段 source-gap 分類、標的名稱／市場、日線輸出雜湊或區塊身分不相符時，回退完整重建。每檔 summary 記錄重用區塊與完整來源清單，頂層收據記錄增量／全量檔數、重用區塊數及本機 materialization 耗時，供往後逐日比較。首次載入新程式碼的舊 summary 尚無逐區塊身分，必須先完整驗證建立證據；下一個來源日才可能看到增量收益。

在**真實現有分鐘資料**上唯讀抽樣，`2330` 全史校驗＋聚合 0.504 秒／變動尾段 0.024 秒，`0050` 為 0.381／0.020 秒，`6168` 為 0.314／0.025 秒。這證明尾段運算約快 13～21 倍，但不是全 2,757 檔 end-to-end 的倍數；後續約 3 分鐘混合建置與約 54 秒稽核尚未因本次改動縮短。新路徑的三個增量／更正／損毀回退測試，加上既有相鄰模組，合計 43 個 Python 測試通過；下一輪真實全量端到端耗時與輸出逐欄一致仍待排程觀察。

## 全服務持續測速及另一項實測邊界（16:01～16:06）

在既有每 30 秒執行的 `stockagent-data-refresh-status-snapshot.service` 中，加入 `scripts/track_service_runtime_trends.py` 的**每 5 分鐘**取樣閘門，不另建常駐監測程序或 timer。每次取樣對所有已安裝 StockAgent service 記錄最近一次已完成程序耗時、狀態與結束碼；僅對同一個 systemd invocation 計算跨次 CPU／I/O 差，服務重啟、計數器回退、首次取樣則保持 `null`。原子保存一份約 26 KB 的比較基線，長期事件交由既有 journald 輪替，不建立無限長自管檔案。這些都是**程序／資源**指標，不冒充每個 HTTP／模型／交易請求的延遲；逐功能耗時仍要看專屬收據。

正式 worker 首次執行於 16:01:26 辨識到 48 個 service。實跑即抓到 hardened systemd 的 `ProcSubset=pid` 看不到 `/proc/sys/kernel/random/boot_id`，令初版節流每 30 秒重取樣；已依單調時鐘回退與同一 `InvocationID` 約束修正，加入「無 boot ID 的沙盒仍限速」回歸測試。16:05:55 正式日誌的 `service_trend` 階段僅 1.905 ms、未產生第二次取樣，對照修正前取樣輪約 190～266 ms；下一個有效 5 分鐘跨次 CPU／I/O 差須待 16:10 左右的正式 worker 才能驗證。與監測、資料庫存及資料面板相鄰的 76 個測試已通過，Ruff 與 diff whitespace 檢查通過。

16:10:59 已收到下一筆正式樣本，間隔 329.409 秒、48 個服務全數列出，查詢本身 140.819 ms；非取樣輪附加階段約 0.7～1.9 ms。這也抓到初版對**閒置 oneshot** 保留的 `InvocationID` 計算出 `0.0 cores`，容易誤導成工作本身零成本；已改為只有連續執行中的服務才計算跨次 CPU／I/O，閒置工作只展示其最近一次程序耗時。此語意修正後相鄰測試為 77 個通過；16:10 的舊樣本不應拿其 idle CPU `0.0` 作績效判斷，下輪才會顯示 `null`。

16:16:01 正式再次取樣，間隔 302.814 秒、仍為 48 個服務。`stockagent-artifact-dedup.service` 與剛完成的 `registered-data-intraday` 已如契約顯示 CPU 差 `null`、並保留最近一次工作 wall；持續運行的 `public-dashboards` 則記錄約 0.0038 cores。這驗證節流與「閒置非零成本」邊界，而非證明所有 48 項業務操作均已量到 p95 或達最優。

## 下一個開盤瓶頸的精確追蹤

9/23 的第一個當沖模式在 09:00:02.676 訊號 ready，券商本機 callback 第一筆於 09:00:01.310、全必需標的覆蓋於 09:00:02.163；永豐共享報價請求發於 09:00:00.236，broker 約 09:00:01.067 才開始處理，隊列 830.794 ms，provider 擷取 1,192.081 ms。最晚第五模式才在 09:00:06.673 ready，主因是五模式順序執行；模型推論本身單模式約 31～63 ms。此處**不**把本機 callback 視為交易所撮合時間，也不以回放訊號冒充 1 秒即時達標。

共享報價服務設在當沖模擬引擎 loop 中；原有資料只量到「提交至 broker 開始」的 831 ms，無法分辨是同一圈的模式重載、交易日閘門、readiness 更新，還是上一圈的交易帳本工作佔用。已在原有 `shared_quote_request` 稀疏日誌加上上一圈工作、當圈 broker 前置、模式重載、交易日驗證及 readiness 四段毫秒值，只有真的處理請求才輸出，不增加每圈持久檔案或外部 API 查詢。165 個當沖與開盤 pipeline 測試通過；**目前持續執行中的引擎仍載入舊碼，未為純測速而重啟交易引擎**，所以下次正常重新載入後的實際開盤才有這些欄位。下一步先以量測證明 831 ms 等待的來源，再決定是否安全地調整 loop 內優先次序；今天不能從數據推定它全是某一函式造成。

16:30 之後的 5 分鐘全服務樣本中，常駐 `tw-public-source-events` 約 0.415 cores，是當時最大的常駐 CPU 來源；後面是永豐 TAIFEX bid/ask 0.075、Discord bot 0.047、當沖模擬引擎 0.043。前者收據實際為 `degraded`：159 個註冊／159 個已探測來源中，`tpex_daily_valuation` 有一筆尚未套用，官方已驗證開市日 9/23 的 TPEx valuation 回應無列；16 次下載重試仍不能補出官方資料。監測器每 60 秒探測同一來源表示、失敗重試會連帶驗證既有數百萬列歷史。這不是監測進程崩潰，也不能用「服務 active」改成資料健康。雖可拉長重試來省 CPU，但會延後來源探測故障時的補抓；本輪沒有以犧牲發布即時性或忽略缺列的方式製造提速。下一階段若要縮短此路徑，需保留 60 秒來源版本偵測並驗證日期清冊重用或有界負面快取的因果正確性。

目前合併相鄰回歸測試為 470 個 Python 測試通過；不是全庫 5,404 個測試已執行。TAIEX `twse_taiex_ohlc.parquet` 的現有成功摘要在 16:43 唯讀驗證 SHA 相符、1999-01-05～2026-09-23 共 6,870 列，因此下一輪有資格走語意不變路徑；是否真的命中須以實際官方回應與正式 `latest_attempt` 證實，不能提前當成測速成果。

進一步查 9/23 的 `tw_public_stock_daily.summary.json`，960 萬列、約 1.33 GB 的公開特徵表實際 `build_mode=full`、`reused_rows=0`；現有 `--public-feature-incremental-days=14` **不是**來源變動時就能沿用歷史前綴。其嚴格契約要求全部來源與既有輸出位元收據相同，來源新日資料或任何歷史修訂使之失效；這是避免歷史修訂／公告時點改動被舊前綴掩蓋的正確 fail-closed 行為。已把 `base_contract_not_verified` 退回原因與來源證明、逐股特徵、全市場特徵、組裝、Parquet 寫入與再驗證的分段耗時寫入**下一輪正式建置**的摘要；未改資料值或放寬證明。155 個公開特徵／盤後發布相關測試通過，含「來源變動必須全量」及新增的收據欄位測試。真要在新交易日將約 335 秒縮短，需先有能證明前綴未變的來源分片／日期指紋，再容許尾段計算；現有旗標本身不構成該證明。

完全同值的 `unchanged_verified` 路徑現在也會在 CLI 日誌顯示來源／輸出證明耗時，**不改寫**成功摘要；下輪若因 TAIEX 同值輪詢而跳過公開特徵表，就能區分「校驗 1.33 GB 輸出」與「重新計算 960 萬列」的真實成本。上述所有受影響模組合併為 557 個 Python 測試通過，Ruff 與 whitespace 檢查通過；仍未執行全庫 5,404 測試。

對 55,765 個 Parquet 檔的正式資料監測快照進一步 profiling：`inventory` 約 1～3 秒，`public_projection` 約 0.7～1.5 秒，若有來源變動則 6,314 項 feature 投影約 0.4～0.6 秒。一次直接執行腳本的 profile 漏設正式 wrapper 的冷 inventory 持久快取，額外讀取約 16 萬行壓縮 inventory 2 秒；**不能**用該錯誤 profile 宣稱正式服務的瓶頸。帶正式環境變數的 profile 確認主要成本是 55,765 檔發現／校驗及其他實際狀態投影，必須維持變動偵測與壞檔呈現；本輪沒有把刷新週期拉長、少看檔案或沿用可能已變的 footer 來製造表面提速。

15:38:54 第二次官方 sweep 已在 15:46:41 `Result=success`，但全資料 `waiting_publication` 仍含 TWSE、TPEx 兩項 valuation 上游缺漏；其 467.43 秒 wall、約 2,607 秒 CPU 與 71.4 GB cgroup 記憶體尖峰是舊 TAIEX 程式碼的結果，不能拿來評價新 semantic-no-op 修正。下次 17:30 正式執行及新摘要才是驗收點。16:04 `systemctl --failed` 為空、gateway `/healthz` 回 `ok`，但這不是全資料健康、冷重啟或外網雙棧已驗證。

## 17:30 正式排程與公告封存故障

17:30 的正式 `close_final` 先查詢並完整驗證 6,870 列 TAIEX，日誌確認 `semantic_noop=True`，原 canonical 與成功摘要沒有因觀測時間刷新而改寫。該輪仍因其他來源收據改變而需要重建 9,604,259 列公開特徵；正式摘要的分段耗時為來源證明 0.706 秒、逐股特徵 235.221 秒、市場特徵 1.975 秒、輸出組裝 6.699 秒、寫入／校驗 10.612 秒，寫摘要前總計 255.214 秒。完整 sweep 約 5 分 19 秒、32 分 41 秒 CPU、56.8 GiB cgroup 記憶體尖峰。17:35 因密集發布探測點再執行的一輪已重用完成的衍生層、沒有重建特徵，約 47.6 秒；仍取得官方資料，不能稱為零成本。`tpex_daily_valuation` 當日官方回應無列，來源狀態仍是 `waiting_publication`，不能把 `Result=success` 解讀成全部資料完成。

16:30 的宏觀公告封存曾在央行外匯存底第 164 頁遇到單次 HTTP 轉址，整個服務 fail closed。下載器現在對原始官方 URL 有界重試，**不跟隨**未知站點／錯誤頁；持續轉址仍報錯。隔離測試驗證暫時轉址恢復及持續轉址拒絕。修正後的人工正式執行已讓央行外匯存底 317/317 筆、710 個原始檔通過完整性稽核，證明這次 CBC 階段可完成；但服務接著在公開特徵對帳因另一問題失敗，不能宣稱整個 service 恢復。該問題是 9/23 收盤衍生層已被正式 close receipt 接受，舊的全資料下載摘要只到 9/22；TPEx 估值仍待發布，原對帳器卻只接受所有來源完整的 close receipt，故想用 9/22 重建 9/23 特徵並正確被防倒退閘門拒絕。

對帳器現改與完成交易日 finalizer 使用相同的必要條件：TWSE／TPEx 官方收盤兩源俱在、同一 live root、非阻斷失敗為零，才允許保留較新的收盤日期與 `allow_daily_publication_lag` 契約；原來源缺漏仍顯示待發布，沒有補造估值。若當日收盤證據不成立，仍拒絕倒退。對帳及公告封存相關 23 個測試通過；實際 root 的唯讀 dry run 已不再倒退，回報 `end_date=2026-09-23`、`allow_daily_publication_lag=true`。由於期間另有 10 個真實來源位元收據改變，仍須一次完整特徵重建，不能只憑 dry run 宣稱服務已恢復。

18:15 人工重新啟動同一正式 service 後，宏觀公告封存、317/317 筆央行外匯存底原始發表、9/23 官方特徵對帳、MOF 原始公告、研究表與 TAIFEX 附加研究表均跑到終點；systemd `Result=success`，wall 9 分 47 秒、CPU 48 分 57 秒、記憶體尖峰 48 GiB。公開特徵仍因十個來源收據確實改變而全量重建，這輪 `stock_build=289.639s`、總計 309.389 秒；沒有冒充尾段增量，也沒有把缺漏的 TPEx 估值寫成已完成。`cbc_money_release_vintages` 的 2000-06 原始值仍缺，稽核明示 `value_history_complete=false`，雖然此次服務成功但歷史數值覆蓋沒有變成完整。

逐股特徵建置本來只量整段 235～290 秒。已在下一次程式載入的摘要加上 16 個逐股及 11 個市場 feature builder 的獨立耗時，未改來源或欄位計算。另對已經位於驗證交易日的巨大 DataFrame 加入線性日期集合驗證，命中才略過重複排序／as-of join；任何假日或 null 日期仍用原映射。直接以正式 9,604,259 列特徵表的三欄做唯讀比對，快路徑 0.034 秒、舊映射 0.431 秒；排序後逐列相等、沒有遺失 key。這只證明此映射步驟約省 0.4 秒，**不足以解釋或解決 289 秒的主要瓶頸**。未以此小局部增益宣稱完整建置已大幅提速；要用下一次逐 builder 收據與實際下游合併測速判斷大宗成本。

18:48 再掃 48／37／2 個 unit，12.4 秒樣本沒有 systemd failed；常駐 CPU 最高為當沖模擬約 0.043 cores、Discord 0.039、TAIFEX bid/ask 0.033、來源事件 0.023。公網 IPv4 HTTPS 與本機 `/healthz` 成功，DDNS IPv6 HTTPS 仍無法連線。全資料摘要仍 `critical`：326 個啟用端點為 225 complete、35 catching up、65 unable；網站通並不代表資料可用。

最近一次當沖資格服務表面耗時 315.5 秒，但 9/23 09:15:06 已觀察兩個官方來源且本機當日資格最終可重用，直到 09:20:14 才完成；整輪 CPU 只有 5.5 秒。現有流程會在快取穩定驗證未命中時等候全域 TW-public writer lock，這是可能的長等待，但此筆日誌缺少鎖等待分段，不能把五分鐘全歸因給鎖。已在既有成功收據增 `probe_stage_elapsed_ms` 與 `coverage_stage_elapsed_ms`（穩定檢查、writer-lock 等待、持鎖檢查、需要時的官方下載、總計），只在一次完成時寫入，不另添每 2 秒日誌；原 fail-closed／鎖語意未改。下一次遇到同型長耗時即可判明來源、鎖或本地校驗哪段佔用，避免以縮短驗證或繞過寫入鎖冒充提速。

公開特徵整段逐股建置 289 秒仍未解釋清楚，因此在同一份下次建置收據再細分「逐來源 builder」、「交易日映射」、「稀疏特徵逐次 join」三層耗時；每個 join 仍使用原有全外連接與原有欄位衝突語意，沒有在欠缺逐列等價證明前改寫成另一個合併算法。81 個公開特徵測試通過；這一版計時程式還未經真實下一次全量執行，不把它當性能改善成果。

## 19:30～20:03 公開特徵完整來源路徑

細分收據證明 9,604,259 列全量建置的主要重複工作在巨大逐股輸入的 `_finalize_feature_frame`，而不是市場列：19:30 正式公告封存建置耗時約 267 秒，其中單一逐股 join 前的準備約 59 秒。每個來源在 join 前已統一鍵／值型別並去除重複鍵；唯一鍵關聯的 full join 結果仍唯一，因此移除結果上第二次逐欄 group-by。對至少 100,000 列的輸入，先線性檢查 `(date,symbol)` 是否有重複；只有確認沒有重複才免除原有 group-by。實際官方 OHLCV 的 9,746,178 列沒有重複鍵，這段準備由約 59 秒降至隔離全量測試的 1.379 秒。保留有重複鍵的原邏輯，並以重複鍵及欄位衝突的回歸測試比較舊／新輸出。

同一次隔離全量測試從真實 170 個來源收據重建 9,604,259 列、143 個模型特徵，寫入約 1.33 GB Parquet，`total_before_summary=173.727s`、`stock_build=145.221s`、`stock_merge=53.008s`、`parquet_write_and_proof=18.446s`；較先前同型隔離全量測試的 284.814 秒快約 39%，但兩次機器爭用與來源版本不是完全相同。輸出 SHA-256 與目前正式完整表一致；但有三份法人／當沖資格來源位元收據在正式表建置後變更，**相同輸出雜湊不等於來源收據已更新**，因此正式對帳 dry run 仍正確回報 `rebuild=true`。下一步以正式服務取得新來源收據與其實際 wall／CPU 證據。

若唯一變更嚴格限於五份只進入市場列的 TAIFEX 來源，且既有表、特徵 ABI、股票 universe、交易日終點與其餘全部來源收據皆通過位元驗證，新增 `market_only_rebuild` 僅重建市場列、重用舊股票列；任一證據不足即退回完整重建。隔離實表測試在 9,597,389 股票列＋6,870 市場列的樣本約 20.724 秒，對當時完整來源輸出做 9,604,259 列／162 欄逐值相等測試通過。這只縮短符合精確五來源條件的情況，不是任意官方來源更新均可走快路徑。

此輪修改後相關公開特徵、公告、當沖資格、資料對帳及研究表測試分兩批 106＋300 個通過，Ruff 與 `git diff --check` 通過。較廣測試最初被乾淨工作樹中過期的 `audit_cbc_release_vintage_contract` 測試匯入擋住；已將該測試改為現行 `audit_release_vintage_contract`，核對 CBC 公告版本仍 fail closed，然後 300 個測試完整通過。仍非全庫測試，也不代表全部 48 個服務延遲達下界。

20:03 實際啟動既有 `stockagent-tw-public-feature-reconcile.service` 驗收新版：全量 9,604,259 列核心特徵在 214.994 秒完成，後續暫定宏觀、研究表、TAIFEX 研究表及 all-observed 研究表均完成或通過既有相同來源重用閘門；整個正式 unit `Result=success`、wall 246.100 秒、CPU 1,637.236 秒、記憶體尖峰 48 GiB。這比 19:00 另一輪正式核心全量 365.066 秒短約 41%，但負載不同，不能當固定加速率或改善其它服務的證明。正式輸出 SHA-256 `bbe6532ee0ec6aca3ab515ebd6dce82899175ebf65b92d953513a1e1c7238ec5` 與隔離全量輸出一致；正式來源收據已刷新。後續唯讀 dry run 回 `rebuild=false`；獨立 `audit_feature_build_receipt` 六個檢查（schema、availability、來源位元、symbol universe、輸出位元）全部為真、無 finding。

20:15 重掃全服務，枚舉已從 48 service／37 timer 增加為 **49 service／38 timer／2 path**；新的是帳號授權範圍的 `stockagent-finlab-local-refresh` 及其 timer，當時工作正在跑，還沒有完成 wall／資料品質結果。收據是 `artifacts/benchmarks/service-coverage-20260923T1215Z.json`；這說明清單必須每輪自動重新發現，不能把早上的 48 項視為永遠的全部。當次 `systemd` 沒有失敗 unit，但 FinLab、OpenBB 壓縮與外部研究 backtest 同時存在，宿主 memory `full` PSI 的 10 秒平均約 48.85%；正在執行不等於其輸出已通過稽核。沒有為量測或優化而中斷這些其他工作。

## TAIFEX options 原始檔解析的下一個熱點

時鐘／可用性邊界：這裡讀取的是官方**已完成日盤**的交易日期、TX 當日開盤參考與 TXO 每契約開盤、收盤、成交量；ATM／full-chain 為歷史資料投影，既有輸出才決定後續研究可用性。它不把全日成交量挪到開盤下單、不把日線價格稱為 bid/ask 成交；缺少或相衝的官方原始列仍拒絕建置。任何解析優化只能保持每個來源 ZIP／CSV、交易時段、合約月份、買賣權、價格／量與 provenance 的逐列語意，並由來源及輸出收據驗證。

17:00 正式服務的既有分段日誌顯示：官方收據／下載 63.320 秒、monthly ATM 233.043、monthly full chain 218.155、weekly ATM 148.168、weekly full chain 146.737，options 腳本總計 809.982 秒；整個 unit 約 821.951 秒。四個 builder 都重讀官方 CSV，這是結構性重複；目前沒有取得跨年分片的逐值等價證據，不直接改寫 ATM／full-chain ABI 或省略歷史資料。

先在一份真實官方 2025 年 ZIP（18 MB、月契約解析後 243 交易日／223,298 筆）做單一步驟 profile：`csv.DictReader` 與日期解析都是熱點。試改整個 CSV 讀法只使無 profiler 的單次讀取從 11.534 秒變為 11.220 秒，增益小且增加欄位／短列相容性複雜度，已撤回；真正保留的是每次讀檔內 512 筆有界交易日期解析快取，來源日期欄原值不變，無 profiler 的單次讀取為 8.779 秒。這是單樣本約 24% 的解析改善、非正式每日服務 wall 的聲明。資料來源、成交價、可交易欄位及重複列檢查未改，options、下載器、衍生 tick 與舊 TX 策略共 53 個回歸測試通過。下次正式 options 重建仍要驗證輸出收據／前後雜湊與實際分段耗時；此更改的 builder fingerprint 會正確讓既有舊版快取失效。

另在隔離目錄用同一 2025 年官方 ZIP 和正式 TX 日盤期貨來源重新建置四份結果，逐欄／逐列比對正式完整資料的 2025 年切片均相等：monthly ATM 243 列、monthly full chain 223,298 列、weekly ATM 243 列、weekly full chain 107,524 列。這證明該年四個投影在這次解析微調後仍與舊版正式值一致；不是 2001～2026 每個年度均已重建驗收。21:00 左右 FinLab、本機 OpenBB 壓縮與註冊盤中資料仍在執行，沒有為量測強行增加一次完整 13 分鐘正式重建；下一次正常 TAIFEX timer 的分段 wall 與 hash 仍待觀察。

## Yahoo US 每日服務的外部節拍下界

9/23 06:30 正式 `registered-daily` 的 US 股票步驟實際要處理 12,221 個 repair task，20 多分鐘進度持續約每秒 9.5～10.2 項；完整 wall 1,294 秒。當次日誌明示目前本機 Yahoo chart 請求節拍為 0.100 秒／項（10 req/s，屬本機安全策略，**不是 Yahoo 官方保證配額**）。即使所有其它運算零成本，12,221 次請求在此策略下也至少約 1,222 秒；觀測值比這個條件下界多約 72 秒／5.9%。因此單純增加 16 個 worker、微調本地 Parquet 或省幾毫秒 UI 不會消除 21 分鐘；真正縮短得減少有充分 terminal／未變證據的請求，或另行量測較高外部節拍的失敗率與限制，不能擅自略過退市歷史。該輪仍有 13 `failed`，所以更高頻率絕非無風險「最佳化」。

該服務原本把 US 與 forex 分兩次程序跑，兩次都覆寫同一 `daily_update_summary.json`，最終只剩 forex；這是測速與錯誤追蹤的證據缺口，不是 US 沒有下載。現在保留舊合併摘要的相容路徑，同時原子寫出 `daily_update_summary.us_stocks.json`／`.forex.json`，各自含 mode、end date、狀態數量、完成時刻與實際該資產步驟耗時；不碰請求節拍、符號集合、下載或 Parquet 語意。兩次獨立呼叫仍保留前一資產收據的測試通過，整個 Yahoo state 測試 61 個通過。正式下一輪下載尚未執行，故持久收據的實際效益待下一次 timer 驗證。

21:45 左右的唯讀服務覆核：`systemctl --failed` 空；IPv4 公網 HTTPS `/healthz` 為 200、單次 34.844 ms，IPv6 同端點約 228 ms 即連線失敗，未解雙棧。新加入的 FinLab 服務 20:14 開始、21:10 收到 TERM 停止，最後 journal 顯示 55 分 40 秒 wall、8.9 GiB 記憶體尖峰；停止前的同步收據仍為 `partial`，例如 1,109 catalog keys 中只下載 82 個、約 1,027 待下載。後來 systemd 的 `Result=success` 是停止後的 unit 狀態，**不是資料完成證據**。未擅自重啟這個與本輪同時加入、配額及停止原因未明的服務。

## 22:24 後：OKX 歷史特徵的可量測下界與分段收據

新掃描 `artifacts/benchmarks/service-coverage-20260923T1424Z.json` 發現 `stockagent-finlab-quota-snapshot.service/.timer`，所以當時清單是 **50 service／39 timer／2 path**；`systemctl --failed` 仍空。14:00 的 `registered-data-features` 最近一次 wall 約 1,548 秒；並行子步驟是 OKX 1,547 秒、Binance 1,467 秒、Bybit 52 秒，故瓶頸確實在前兩者，不是 Bybit 或 shell 排程。

OKX 這輪 486 個合約、每合約 8 個特徵階段，共 3,888 個進度項。官方 [OKX history-index-candles 文件](https://app.okx.com/docs-v5/en/)列明每頁最多 100 根 1 分鐘 K 線、每 IP 每 2 秒最多 10 請求；現有客戶端維持相同 5 req/s 節拍。若所有 486 個合約都需補整整 24 小時的 1,440 根 index K 線，光此端點約需 `486 × ceil(1440/100) / 5 ≈ 1,458` 秒，接近實際 1,547 秒。這是**條件下界與瓶頸推論**，不是已證實本次每個合約實際都請求 15 頁；須以下輪真實授權數驗證，不能只因 wall 接近就宣稱已達上限。

在既有 OKX `historical_feature_report.csv` 增加每合約的八階段、讀檔、衍生、比較、寫入、覆蓋稽核與總耗時；成功與各階段失敗都保留時間。`download_summary.json` 新增每階段樣本數、P50/P95 與**並行 worker 秒數總和（非服務 wall）**；另外記錄現有跨程序 limiter 的**本程序**各端點 grant 數與 limiter 數，明列 funding 為每合約獨立 limiter，避免把它的總 grant 數錯當共享 5 req/s 下界。未改價格、時間戳、缺漏處理、API 頻率或資料檔語意；10 個相鄰測試通過。

隔離複製正式 BTC-USDT-SWAP hot tail，使用實際 OKX 公開端點跑單合約，沒有寫回正式資料：4,869 列、八階段皆 `ok`、總 5.413 秒，其中 mark 1.834、index 1.680、open interest 0.663、funding 0.337 秒，本地讀／衍生／比較／寫入／覆蓋合計遠小於外部階段。這只是一個 8 小時左右缺口的樣本，不能外推成所有合約或完整每日服務的 P95；下次正式 14:00 收據才足以檢查實際 grants 與階段分布。

## Binance 歷史特徵同源測速與收據保留

Binance 9/23 14:00 的 574 合約歷史特徵並行子步驟耗時 1,467 秒。官方 [USDⓈ-M Futures Market Data 文件](https://developers.binance.info/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)對 `futures/data` 統計資料列出 IP 1,000 requests／5 minutes；現有本機節拍為約 3.33 requests／秒，並未為了縮短 wall 擅自放寬。隔離複製正式 BTCUSDT hot tail、使用公開端點補一段約 8 小時的資料後，十個特徵階段全數成功、5,002 列、總耗時 4.239 秒。此單一合約發出本程序 15 個 grant，其中 `futures/data` 8 個、funding 1 個；本地讀檔 0.424 秒、衍生 0.013、比較小於 0.001、寫入 0.016、覆蓋檢查 0.042 秒。8 次統計端點請求乘 574 合約再除以 3.33 req/s 約 1,378 秒，與 1,467 秒相近，**但只是用單樣本推算的條件下界**，尚非實際全體 request 數或平均；全量下一輪的 grant 收據才可驗證。

已把 Binance 每合約各階段（含失敗）、讀檔、衍生、比較、寫入、覆蓋驗證與總耗時加入現有 `historical_feature_report.csv`，在 `download_summary.json` 記每階段樣本數／P50／P95／並行 worker 秒數，以及本程序統計與 funding 端點授權數。OKX 與 Binance 使用同一個有界統計器；額外將歷史特徵與僅 K 線更新的摘要各寫成 `download_summary.historical_features.json`、`download_summary.candles_only.json`，因為原本共用的 `download_summary.json` 會在稍後的盤中 K 線刷新覆寫 14:00 特徵證據。相容的舊路徑仍維持；新檔只在下一次正式執行產生。兩個交易所相鄰的 30 個測試及 Ruff／whitespace 檢查通過；沒有變更 API 節拍、下載資料粒度或特徵值。

22:56 的新唯讀全服務清單收據 `artifacts/benchmarks/service-coverage-20260923T145652Z.json` 仍為 **50 service／39 timer／2 path**。以上只是完成一項重點服務的瓶頸歸因與測速持久化；其餘服務、真實正式次日耗時及是否接近可達下界，仍列為持續工作，不宣稱「所有服務已到極限」。

## 22:30 Crypto training refresh 與一分鐘原始寫入器競爭

22:56 再查 `systemctl --failed` 發現 `stockagent-crypto-training-refresh.service` 失敗。原始日誌明確顯示它在 22:30:00 啟動、22:30:23 funding 階段完成，22:30:24 便遇到同時起動的 OKX／Bybit／Binance 一分鐘原始下載器；程式當下丟 `raw writer started during refresh`，將整輪標為 failed。過去 9/22 及 9/23 的 02:30 批次也曾在衍生建置或發布邊界遇到相同 writer 競爭。這不是 Discord、當沖引擎或網站掛掉；但它確實使 Bybit 訓練資料刷新／冷發布沒有完成，不能把 defer 說成成功。

已保留原本的「writer 忙時不讀／不發布」失敗封閉規則，把**執行中途**及發布前發現 writer 的情況改寫成含活躍程序、已完成步驟與結束時間的 `deferred_raw_writer` 收據，不再把預期讓路誤報為業務程式失敗。發生於實際發布動作內、不能證明僅由 writer 造成的錯誤仍維持 failed，不掩蓋其它發行問題。新增中途碰撞測試與相鄰 34 個測試通過；22:30 已失敗的 process 不會因修程式自動變成完成。**根治排程上的持續競爭**仍需要可驗證的來源快照／鎖協調或保留明確寫入空窗；目前 1 分鐘原始資料更新週期短於約 2～3 分鐘的衍生建置，單純改成功色或重試頻率不能保證完成，也不應無聲暫停來源蒐集。

23:05 的正式服務日誌更準確量到相鄰原始刷新每輪約 57～60 秒、結束後約 60 秒再次啟動，因此沒有可讓約 145 秒的 Bybit 日表建置獨占執行的既有空窗。唯讀實測真實 BTCUSDT 3,412,383 筆 base 分鐘列加 5,010 筆 hot-tail：全史 `_daily_bars` 3.076 秒，後續日表 funding join 0.077 秒；主要負擔在重讀多年分鐘資料、排序／合併／聚合，非每日 2,373 列的 funding 算式。這是單一高量標的，不外推成 394 標的全輪耗時。下一個有效性能改造應是帶來源版本與 32 日 lookback 證明的分鐘尾段增量建置；發布仍另需隔離持續變動的 `data_bybit` 原始來源，不能直接鬆開 writer gate。

為避免 writer 在日表建置期間完成並退出、使單靠程序掃描漏掉混合版本，已加入一個僅用檔案身分／大小／mtime 的低成本 Bybit 輸入邊界檢查：日表建置前與每一後續步驟／發布前重看 1m base、hot-tail、funding 及兩份來源清冊；改變時保留 `deferred_source_changed`、前後摘要與已完成步驟，停止發布。實際 2,143 個輸入檔一次 metadata 快照約 146.5 ms；它僅防一般原子替換造成的競爭，**不是**防同 metadata 惡意改寫或來源位元完整性證明，最終發行仍必須通過原有 SHA-256 稽核。三個針對中途 writer、原子替換及建置期間來源變動的測試通過；正式 02:30 執行尚未載入驗收。

## Bybit 日表尾段增量的正式接線與隔離驗收

`materialize_bybit_perpetual_daily.py` 現在能利用上一版輸出旁的 materialization 證明，僅重讀 hot-tail 最早受影響日期前 33 天的分鐘資料，再把已驗證的舊前綴與新尾段合併；funding 仍對**完整**日表重新計算。先驗證 contract、輸出 SHA-256、base 的 device/inode/size/mtime、來源 canonical 時間字串與 hot-tail 的日期統計，證據不足即走全量。來源在建置或 Parquet 寫入期間變動時，原子替換前先拒絕；發布前後的來源檢查仍保留。`materialize_report.csv` 與 summary 新增 `full`／`incremental`／`skipped_up_to_date` 方式計數，以免把快取命中冒充增量計算。

隔離測試只讀正式 BTCUSDT 的 3,412,383 筆 base 分鐘列與 5,010 筆 hot-tail，輸出寫在 `/tmp`：新版全量一次 wall 3.53 秒，修改隔離 funding 檔 mtime 後的增量一次 wall 0.90 秒；兩者皆 2,373 日列、2,372 個可執行報酬列，Parquet SHA-256 均為 `2374960d1bd2f6257a0802af6cd7d5d34299b850778d665d482dd44d174b83d3`。這是單標的、無真實 funding 值變化的同值重算測試，**不是** 394 標的正式全輪、來源發布或更新日跨日修訂的完成證據。0／5 分鐘執行契約、尾端修訂、損毀證據回退與替換前拒絕均有測試；擴至 hot-tail、來源範圍、crypto inventory 的相鄰 81 個 Python 測試通過。日表建置也會在載入後和每標的原子替換前後驗證 funding coverage 與 instruments 收據身分，以免來源清冊在批次中途改變而被混合使用。

剩餘主問題仍是原始一分鐘寫入每輪約 57～60 秒、間隔約 60 秒，而全部衍生與冷發布需要更久；增量日表只縮短其中一段，不能單靠這項修正保證 crypto training refresh 完成。不得為讓它看起來成功而放寬 writer gate、跳過完整來源收據或降低原始資料刷新頻率。22:30 的 service 仍顯示 failed，直到下次真正的正式刷新收據驗收前，健康狀態不得改成完成。

## 9/24 00:00 官方當沖資格監測的空窗修正

9/23 22:29 啟動的資格監測器並非掛起：截至 23:57 的 `latest.json` 每 30 秒更新，累積 2,539 次輪詢，官方 TWSE 主檔仍宣告 9/23，未達服務要求的 9/24，有據地維持 `waiting_source`。原本 90 分鐘期限在 23:59:58 到達，正式收據為 `publication_pending_timeout`；舊 unit 的 `RestartSec=5min` 令發布恰好落在期限後時有最長近五分鐘探測空窗。這不是可以用 9/23 舊資料充當 9/24 解決的問題。

已把**僅此** service 的重試間隔改為 30 秒，並用 `StartLimitIntervalSec=10min`／`StartLimitBurst=3` 防止初始化立即失敗時的無限快速重啟；既有輪詢 2 秒、90 分鐘有界等待及官方兩源與精確日期驗證不變。已對本機單一已安裝 unit 驗證並 `daemon-reload`，00:03:34 自動重新進入 `start`，新 `latest.json` 再次顯示 `waiting_source`；並未重啟當沖引擎。75 個官方發布／資格監測相關測試通過。這把**下一次等待期限後**的重試設定空窗縮短，未證明官方何時發布，也未宣稱 9/24 資格已取得。

9/24 00:04 開始的正式一分鐘原始資料輪次可分段核對：Bybit 867 個標的約 36.7 秒、Binance 574 個約 30.1 秒、OKX 491 個約 59.8 秒，三組並行，整輪 wall 約 61.0 秒。OKX 收據有 507 個 `history-candles` grant，現有本機端點節拍 0.1 秒／次；即便本地處理零成本，此設定下約需 50.7 秒。因此本輪 OKX 已接近其**目前節拍條件下**的下界，未經官方限制與失敗率驗證不能把 507 次請求全部無限制併發。timer 是上一輪完成後 1 分鐘再啟動，故實際 start-to-start 約 121 秒；「1m 資料粒度」不等於「每 60 秒完成全市場一次」。若要讓全量衍生與冷發布在此節奏下完成，仍需可證明一致的來源快照或協調機制，而非把必要 491 個標的請求刪掉。

Bybit 日表現也在正式 `materialize_report.csv` 增加每標的 `daily_bars`、`funding_join`、`write_and_proof`、`total` 秒數，summary 用同一套有界最近秩 P50／P95 計法，明示聚合 worker 秒數不是整輪 wall。隔離 BTCUSDT 再跑一次增量，分段為 0.443／0.070／0.012 秒、總 0.525 秒，輸出 SHA-256 仍與前述全量一致；這次重新量測的 105 個 crypto 相鄰 Python 測試通過。分段收據將在**下一次正式 394 標的工作**顯示分布，不能由單一 BTCUSDT 推估全體 P95。

為驗證全體分布，在 `/var/tmp/stockagent-bybit-fullbench-nfo4AbC7` 建立只讀原始來源的硬連結快照，輸出寫到**獨立**日表目錄，8 workers、低 CPU/I/O 優先權，不寫正式訓練資料。固定快照的首次全量為 394／394 成功、wall **189.87 秒**；逐標的 `daily_bars` P50／P95 是 3.194／7.634 秒。只把隔離**輸出**的 mtime 調舊、強制每標的走已驗證增量後，394／394 再次成功、wall **39.48 秒**；`daily_bars` P50／P95 為 0.367／0.603 秒，funding join 0.242／0.450 秒，輸出／證明 0.119／0.350 秒。兩輪均 405,819 日列、405,425 可執行報酬列；對同一 394 份 Parquet 排序逐檔 SHA-256 後的總指紋均為 `4c8d9786a6d047e3cc272fe4ce91dfc1de94de7291c3038efb9ec0a819d0679c`。全量對增量約 4.8 倍是**同一隔離快照、同一主機兩輪**的結果，不等於正式 02:30 service wall 或下一來源日的固定提速率。

這個結果推翻「日表本身一定放不進約 60 秒原始下載空窗」的假設，但**不**解決整條工作：正式 funding 下載、Bybit venue 日特徵、coverage 稽核、各交易所報表與 catalog-backed 冷發布仍需另量並持續保留 writer／來源變動 fail-closed。正式輸出的首次舊版 sidecar 缺失也可能使第一次部署仍需 190 秒左右全量，不能拿已預熱隔離快照冒充正式第一次成功。

另以各自的暫存輸出唯讀量下游：Bybit venue 特徵 2.48 秒，歷史覆蓋稽核 5.36 秒（17 資料集／5 finding，約 5.8 GiB 峰值 RSS），Bybit 訓練報表 4.74 秒，OKX 與 Binance 一分鐘來源報表約 2.68／2.82 秒。隔離 venue 特徵 Parquet 與品質 CSV 的 SHA-256 均和當時正式輸出一致；報表的 `completeness_verified=false` 仍維持原資料健康語義。這些單步在不同時間執行，不能直接把秒數相加當同輪服務 wall。

22:30 正式 funding 步驟曾耗約 23 秒，但原程式沒有分段證據。現於 `funding_coverage.csv` 加每標的 `same_day_proof` 或 `fetch_and_mark`／`merge_write_proof`，於 `funding_summary.json` 加 universe discovery 耗時、逐段 P50／P95 與寫摘要前總耗時。隔離複製當日 397 個既有 funding 檔，再用同一官方合約清冊跑 394 個標的：394／394 均 `skipped_current_snapshot`，instrument discovery 0.241 秒、每標的同日證明 P50 0.006 秒／P95 0.010 秒、整個程序 wall 1.58 秒。這說明 22:30 的 23 秒**不能**歸因為固定 394 檔本地證明成本；可能是當時外部回應或宿主負載，但尚無該輪分段證據，不定論。下一次正式服務載入新版收據後才能歸因。funding 與相鄰 crypto 模組的 70 個測試通過。

最後合併 crypto 來源／日表／歷史特徵、官方發布與當沖資格的 **180 個 Python 測試通過**，Ruff 及 `git diff --check` 通過，未把這 180 個稱為全庫測試。9/24 00:23 的本機 `systemctl --failed` 仍僅有前次 `crypto-training-refresh`，它尚未收到新版正式 02:30 輪驗收；資格監測仍是官方主檔宣告 9/23 的 `waiting_source`。公網 IPv4 `/healthz` HTTP 200，IPv6 連線失敗；全資料公開摘要 `health=critical`，327 個啟用端點為 225 complete、36 catching up、65 unable、1 streaming。這些是不同健康域，均未因隔離提速而改成綠燈。

## 9/24 00:34 FinLab 單鍵無界等待

正式 FinLab 本機刷新上次在 `broker_transactions` 整表 SDK 呼叫停留約 55 分 40 秒，接近 8.9 GiB 記憶體尖峰，最後由外部 TERM 結束；當時只完成 82／1,109 個目錄鍵。既有 4 小時 unit timeout 只能限制整輪，無法讓後續小鍵在同輪前進。該巨大鍵已單獨列為資源暫緩，不把它當成已下載。

現在排程預設每次只處理一鍵，透過 GNU `timeout` 對每個 SDK 子程序設 600 秒上限；超時後只憑相同 attempt ID 與 `runs/latest.json` 的進行中鍵寫入 `timed_out` 收據，保留 `partial`、真實錯誤計數及剩餘估計，接著處理下一鍵。每輪最多容忍兩個鍵逾時，避免長時間佔住重型資料 slice。若子程序已自行完成終端收據，恢復器不改寫其結果；無法辨識正確進行中鍵、非超時異常或超時後強制殺不掉時仍報錯，絕不把它們歸類為成功。手動覆蓋 `FINLAB_SYNC_BATCH_SIZE` 大於一時，上限作用於整個子程序而非單鍵；正式部署仍維持預設一鍵。

這是**有界故障隔離**，不是 SDK 下載加速，也未重啟帳號服務或消耗額度驗收。下次 16:10 正式執行仍須看逐鍵耗時、配額、`latest.json` 終態與私有研究發版守門；不能把 `partial` 說成資料已齊。`test_finlab_history.py`／`test_finlab_dashboard.py`、shell 語法及 Ruff 已完成隔離驗證；正式 run 尚未載入新版。

同輪重新枚舉維持 **50 service／39 timer／2 path**；收據為 `artifacts/benchmarks/service-coverage-20260924T0036-local.json`。它顯示 Crypto 訓練刷新最近一次仍 failed，FinLab 上次 3,340.7 秒程序結束但 `Result=success` 不代表同步完成。`registered-data-intraday` 與官方當沖資格監測當時正在執行，不能對進行中的程序套用「最近一次完成耗時」。

Crypto 訓練刷新新增逐步 `step_attempts`：每個子程序開始即記錄活動命令、結束寫實測 monotonic 秒數及成功／失敗；即使原始 writer 於步驟後出現或來源簽名變動而延後發布，已花的計算時間仍保留。既有 `steps` 仍只收錄通過後續檢查的步驟，發布亦有獨立耗時。這只補量測和中斷證據，不變更 raw-writer／來源位元守門，也未解決與約兩分鐘一輪的原始寫入競爭。FinLab、Crypto 相鄰 **39 個測試**、Ruff、shell 語法及 whitespace 檢查通過；下次正式 Crypto 02:30 的收據仍待驗收。

全服務量測另新增 `last_attempt_outcome`，把正在執行、從未觀測、非零退出、systemd 失敗與程序零碼結束分開；`process_exited_zero` **不代表資料完成**。00:41 的新收據 `artifacts/benchmarks/service-coverage-20260924T0041-local.json` 確認 `crypto-training-refresh` 為 `failed`，FinLab 雖然 systemd `Result=success`、但 `ExecMainStatus=15`，現明確列 `nonzero_exit`。這修復跨 50 個 unit 的監測誤讀，不改 service 本身。相關測速、追蹤、Crypto 與 FinLab **54 個測試**通過，Ruff、shell 語法與 whitespace 檢查通過。

## 9/24 00:46 Windows／WSL 非 systemd 路徑覆核

本機 root crontab 為空、`/etc/cron.d` 未見 StockAgent 工作；root 使用者層 systemd 沒有運行的 StockAgent unit。Windows 四項相關排程仍為公開 Caddy（Running）、crash dump 保存（Ready）、停用的一次性 WSL 修復與停用的 WSL 每日備份。Caddy 的程式與設定 SHA-256 都和 repo 原檔一致，本機 gateway `/healthz` 為 200；這仍不是外網雙棧或冷開機復原證明。

排程每分鐘觸發的 Caddy 工作顯示非零上次結果 `0x800710E0`，但現有實例仍在 Running；新增唯讀 Windows process 證據，記錄兩個 Caddy PID 的父程序與啟動時間，不讀命令列、憑證或設定內容。新收據 `artifacts/benchmarks/service-coverage-20260924T0046-local.json` 顯示 Caddy PID 5060 的父程序是 9/15 已啟動的 PowerShell PID 12128，另一個 Caddy PID 7172 由前者啟動；啟動日誌最後一次 dispatch 也是 9/15。排程設定為 `IgnoreNew`，依 [Microsoft 的正式語意](https://learn.microsoft.com/en-us/windows/win32/taskschd/taskschedulerschema-multipleinstancespolicy-settingstype-element)已有實例時後續觸發不另開實例，因此「重複觸發被忽略」符合觀測；但 Task Scheduler Operational 日誌目前停用，**不能以此單獨證明 `0x800710E0` 的確切成因或真正冷開機恢復時間**。健康判斷須同時看實例、Caddy 程序、backend 與 HTTPS，而非只看最後結果碼。相關唯讀稽核測試 25 個通過。

## 9/24 00:52 Shioaji 儲存監測與 OpenBB L1

`stockagent-shioaji-storage-monitor.service` 最近一次正式掃描 **4,494,363** 個本機實體檔，`scan_seconds=161.304`，CPU 約 161 秒；它不連永豐 API，也不應為了節省掃描成本刪除資料群組、漏算成長或跟隨 symlink。改為先計算最近 30 個台北完整曆日的精確午夜時間界線，每個檔案的 mtime 只做數值區間／二分搜尋；目錄堆疊也不再替每個目錄建立 `Path`。在同一份固定的 123,015 檔、約 5.05 GB 唯讀樣本，舊／新 `_scan_dataset` 回傳值**逐欄完全相等**，交錯三輪中位數 0.7757 → 0.5792 秒（約 25%）；這不是 449 萬檔正式整輪已改善 25% 的證明。新快照另含每資料群組 `scan_seconds`，下一次 01:20 正式排程才能核對完整 wall、CPU、檔數與各群組。相關 Shioaji 儲存及面板測試 25 個通過。

OpenBB L1 最近正式輪次 00:44～00:49 約 5 分 16 秒，新增 19 段、剩餘 4,008,695 個 L0 shard 待壓縮；cgroup 峰值約 2.50 GiB RAM 與 3.64 GiB swap。直接放大 batch／併發或禁用 swap 可能損害同機即時工作，不能因單次 wall 長就盲調。L1 收據目前只有新段與待處理數，已加入來源過期契約稽核、未指派來源載入、批次規劃、段建置、查詢 view 發布、隔離與狀態查詢的分段 monotonic 耗時；**沒有**改 L0、Parquet、SQLite、刪除或發版語意。下次 01:19 正式輪次才可歸因與選擇真正瓶頸；L1 加 Shioaji 儲存相鄰 32 個測試及 Ruff 通過。

00:54 再以 SQLite 唯讀連線核對，manifest 有 **13,287,450 tasks、3,308,947 L1 成員、151,945 segments**。`EXPLAIN QUERY PLAN` 顯示過期契約稽核逐 segment 掃已壓縮成員並依 task 主鍵查證；下一批未壓縮 shard 的查詢則用現有 `idx_tasks_schedule_age_v2` 篩 active/success，但 `ORDER BY endpoint,task_id LIMIT 2048` 需要 **TEMP B-TREE** 排序。這是數百萬列下的明確候選瓶頸，**還不能**僅憑 query plan 推斷其實際耗時或 swap 來源。直接為 1,329 萬 task 建大索引會增加資料庫與 I/O，且可能阻塞同一個 archive writer；先用下一輪分段實測決定是否值得，再採可中止的受保護建置或改寫方案。

另對兩項低優先級服務做現場證據分流，避免只盯長 wall：`stockagent-packed-backup` 00:48 最新 current-heads 收據為 11,787 個物件、121 個 head、464,027,907,190 位元組已驗證、待辦 0，最近無新複製，每次重新調和約 12～15 秒；**歷史完整性仍 `not_checked`**，不能擅自跳過驗證來縮短這段時間。`stockagent-artifact-dedup` 上次只對 3,706 個不同 inode 做內容雜湊，39.7 秒 wall、29.2 秒 CPU，精確重複組 3 組、回收實體配置約 24,576 位元組；其低優先級與無損重驗語意使它目前不是先於 OpenBB swap／公開來源遲滯的可證明主瓶頸。這只是優先序判斷，不是宣稱兩服務達到極限。

## 9/24 01:00 OpenBB L1 稽核單次開檔試驗

正式 L1 稽核對每個既有 segment 原先先開一次 Parquet 讀行數，再重開同一檔案取 schema 指紋。改成同一 `ParquetFile` 同時讀兩者；仍依原先順序檢查缺檔、行數、移除 schema metadata 後的 SHA-256，沒有跳過任何 segment，也不更改來源或發版。從目前 SQLite manifest 唯讀取得 1,024 個已成功 segment，交錯跑兩次：原路徑 6.815／5.542 秒，單次開檔 3.763／3.436 秒；1,024 個行數與 schema 指紋逐一相等。這是同機此樣本的局部 I/O 測速，不把差值線性外推到 15 萬段或宣稱整輪 wall 已改善。OpenBB L1 既有 7 個端到端、失效重建與稽核測試以及 Ruff 通過；下一次 01:19 正式輪仍要看 `stale_contract_audit` 與其它分段收據。

同一回歸測試另加入「衍生 Parquet 行數仍相同但 schema 不同」的實際寫檔破壞，正式 audit 回拒、正常 compact 恢復、後續 audit 通過。01:10 重新枚舉仍是 50／39／2，10 秒取樣的常駐 CPU 最高約為永豐 TAIFEX BidAsk 0.072 cores、Discord 0.044、台灣公開來源事件 0.027、當沖模擬 0.020；宿主 CPU／記憶體／I/O PSI `full avg10` 均為 0。這只界定當時沒有宿主壅塞，不等於每個排程已完成或開盤延遲達標；可重測收據為 `artifacts/benchmarks/service-coverage-20260923T171010Z.json`。

Crypto 發行的獨立現況核對：`data_bybit` 約 22 GB／4,306 個實存檔案，catalog 的 `bybit` source 正是整個 mutable `data_bybit`；冷庫 penguin head 最後是 2026-09-21 02:35 UTC。一次 `publish_data_releases.py status bybit` 在原始 writer 間隙曾顯示 `publish_ready=true`，但那只表示**當下無活躍 writer**，不是有足夠時間對 22 GB 做完整 catalog 發行，也不是新冷版已完成。現行來源發行不可繞過 writer gate；真正排除每兩分鐘反覆競爭，需要先建立可審計、跨整個 22 GB 來源的一致快照／producer 交接契約，並核對全部寫入器的原子性、發布與原始 freshness 的因果界線。尚未實作，不把短暫 ready 冒充成功。

01:19:30 OpenBB L1 正式 timer 載入新版完成：wall 294.633 秒、CPU 222.600 秒、19 個新段、0 stale／failed、4,006,647 個來源 shard 待壓縮，沒有刪 L0。前一輪約 316 秒；新舊輪有不同來源量及同時的 Shioaji 掃描，不能把約 21 秒差全歸因於單次開檔。新版正式收據 `data_openBB/_state/l1_compaction_latest.json` 的分段是來源契約稽核 120.976 秒、未指派 shard 載入 41.661 秒、建段 3.582 秒、DuckDB 查詢 view 發布 118.769 秒、狀態 task count 4.205 秒。cgroup 記憶體尖峰約 2.50 GiB、swap 尖峰約 3.76 GiB；`MemoryHigh=2.5 GiB` 附近仍有明顯換頁，不能稱為整體資源最佳化。下一步的大宗應是 view 發布及未指派查詢，而非增加建段執行緒。

01:20:59 Shioaji 儲存監測正式 timer 完成 4,497,460 檔、142.082 秒、`status=ready`；上次 4,494,363 檔、161.304 秒。此輪與 OpenBB 重疊、檔數多約 3,100，仍短約 19.2 秒／11.9%，但不能把它稱作無爭用的固定加速率。各資料群組：期貨歷史 1,903,284 檔／81.353 秒、FOP 串流 1,625,053 檔／31.577 秒、歷史市場 286,019 檔／16.528 秒、股票分鐘 489,920 檔／9.329 秒。此服務只掃本機儲存、不登入 Shioaji；未因測速重啟券商／行情行程。

DuckDB view 發布原本只量整段 118.769 秒，無法判定是 24,792 段的 ETF 視圖、20,843 段的 SEC filing headers，還是其它端點／CHECKPOINT。已在相同正式收據加入 `view_endpoint_seconds`，逐端點記錄既有建 view＋catalog 寫入耗時；不改任何 SQL、來源路徑、view 名稱、schema 或原子替換。含同筆數 schema 損毀回歸的 7 個 L1 測試通過；**01:19 已啟動的舊版沒有此欄**，下一次正常 timer 才可驗證逐端點瓶頸。

對同一正式 manifest 只讀拆分 2,048 個未指派 shard，原全域查詢一輪 44.741 秒、逐 Parquet metadata 驗證 3.043 秒；原查詢另一輪僅取 task IDs 為 37.354 秒，顯示 OS 快取／爭用會改變單次 wall。用現有 task 主鍵強制排序花 104.082 秒，已淘汰，沒有進入正式程式。另一個受控快路徑先用現有 `idx_tasks_active_plan` 找第一個未指派端點，再只排序此端點；若它不足以填滿要求的 2,048 筆，或指定了端點篩選／缺少索引，就退回原全域查詢。單獨 SQL 測得找首端點 5.070 秒、端點內排序 26.483 秒；與原查詢的 2,048 個 task ID SHA-256 **完全一致**。接線後再次對正式 manifest 唯讀重測為首端點 6.530、端點排序 33.734、metadata 0.707、總 40.971 秒；相鄰舊完整路徑 47.786 秒。這是不同時間的低優先權單輪樣本，不能聲稱固定加速率或下一正式輪已驗收。快路徑、首端點不足回退與缺索引回退的測試，加上原 L1 審計測試共 8 個通過；不新增 1,329 萬 task 的大型索引，也不變更來源集合或排序契約。

為避免把 view 118.769 秒誤歸因於 2 萬段端點的 SQL 本身，又在**隔離 DuckDB** 只讀正式 manifest 完整建 42 view／1 deferred：總 9.054 秒，其中 ETF 24,792 段 4.483、SEC 20,843 段 1.614 秒。單端點隔離重測也只有 6.913／1.047 秒。與正式輪的差距很大；正式輪同時有 cgroup `MemoryHigh` 附近約 3.76 GiB swap 峰值與 I/O 壅塞，較可能是狀態／資源交互，而不是單一 view SQL 固定耗時。下一正式輪的逐端點收據、cgroup 與 PSI 才能決定是否要做安全的 view 增量發布或記憶體生命週期優化。

低優先權、只讀的 20,000 段 Parquet metadata 稽核逐筆行數與 schema 指紋均通過；掃描中程序 RSS 約 95 MB、Arrow pool 0、swap 0，沒有隨段數持續累積。這否定了「單次開檔稽核本身線性洩漏 2.5 GiB」的猜測，卻不能解釋正式輪較後階段的記憶體；因此下一輪正式收據再增加每階段程序 RSS／swap、cgroup memory／swap、`memory.high` 事件及該 cgroup 的 memory／I/O full-stall 累計值。這些是唯讀診斷；欄位缺權限會是 `null`，不影響查詢、輸出、刪除或發版語意；8 個 OpenBB L1 測試通過。

Shioaji 儲存掃描另試了以一次 `DirEntry.stat(follow_symlinks=False)` 的 `st_mode` 判別目錄／檔案，取代三次 `is_symlink`／`is_dir`／`is_file` 判斷。固定 165,176 檔／7,250,320,592 bytes 的 Top-200 來源輸出相等；交錯單輪舊版 1.357、0.944 秒，新版 1.046、0.979 秒，第二對新版較慢，無可信的穩定收益，**未改正式實作**。保留目前已正式改善且等價的午夜界線掃描。

當沖引擎目前程序自 9/23 12:32 起執行；稍後新增的開盤 queue／loop 分段測速尚未載入該程序。9/24 01:42 的正式 `status.json` 唯讀顯示 5 模式中，`tw_day_trade_100m` 有 35 筆 open／margin carry、`execution_evidence_complete=false`，其餘四模式 open 0；ledger integrity `ready=true`／divergence 0 只代表狀態與帳本一致，不代表這 35 筆可成交或當沖紀律已恢復。因此未為純測速重啟持有 Shioaji 報價狀態的當沖引擎；要讓新開盤計時在明日 09:00 生效，仍需單獨完成離峰重啟前後的持倉、帳本、訂閱及模式 revision 驗收。不得以便宜的重啟抹掉留倉問題。

## 9/24 02:10 OpenBB L1 正式瓶頸與增量視圖

01:54:53 正式 L1 輪次完成，wall **282.104 秒**，新增 19 段、尚待 4,004,599 個來源 shard，0 stale／failed，未刪 L0。分段收據：既有段契約稽核 108.369 秒、未指派來源載入 41.910 秒（找首端點 5.427／端點內排序 35.928／metadata 0.555）、新段建置 3.735 秒、DuckDB 視圖發布 **117.414 秒**。逐端點顯示未變動的 `etf.nport_disclosure` 佔 **107.604 秒**，不是新段建立慢。建段後 cgroup memory 約 1.73 GiB／swap 0；視圖發布後 `memory.high` 累計增加到 19,527 次、memory full-stall 約 3.081 秒、I/O full-stall 約 26.428 秒，且服務觀測到約 2.5 GiB memory／3.62 GiB swap 峰值。這是正式當輪觀測；單靠總耗時不能把全部 117 秒歸因於 ETF SQL，cgroup 與主機 I/O 交互仍需後續正式輪分辨。

已改成複製上次原子發布的 DuckDB 資料庫到暫存檔，對每個端點用**有序 segment 路徑清單 SHA-256** 判斷是否需要重建；重用前檢查既有 catalog 的端點／視圖集合、每個視圖的 SQL SHA-256，以及目前來源路徑仍存在。只有簽章一致的視圖重用；變更端點重建，失效／缺舊簽章則完整重建；最後仍以 `os.replace` 原子發布。沒有更改 L0、Parquet 內容、端點命名、查詢輸出語意或原有 schema／行數稽核。收據新增逐端點 `view_endpoint_actions`（`rebuilt`／`reused_verified_paths`／`deferred`），可量測每輪實際命中而不是冒充熱快取。

在**正式 SQLite manifest 唯讀連線、隔離 DuckDB 輸出**上完整測 42 published／1 deferred：首輪建全部視圖 **12.610 秒**（ETF 6.775 秒）；同一份 manifest 第二輪驗證並重用全部 **1.645 秒**（ETF 0.199 秒）。這是無新 segment 的隔離對照，不能聲稱正式 117 秒已縮為 1.645 秒。新增舊版無簽章、視圖 SQL 被異動、兩端點只一端變更、端點延後時移除舊視圖及恢復後重建的回歸，加原有增量、stale 稽核共 **12 個 L1 測試通過**，Ruff 通過。正式 DB 目前仍是舊 schema：**下一次正常 timer 先做一次保守完整重建，之後才可正式驗證重用**；保留各階段 cgroup 與逐端點收據觀察剩餘稽核與來源查詢成本。

02:10 全服務唯讀重新枚舉仍為 50 service／39 timer／2 path，收據 `artifacts/benchmarks/service-coverage-20260924T0210-local.json`。已完成 oneshot 的 CPU counter 不應以取樣前後相同而顯示 `0.0 cores`：這只表示兩次觀察之間未執行，會誤導為工作本身零成本。共用測速現在僅對觀察區間**前後均持續執行且同一 invocation**的服務計算 CPU 差；閒置的最近一次 wall／exit 仍保留，CPU 顯示未知。25 個相關測速與追蹤測試、Ruff 通過；上述 02:10 舊收據的 idle `0.0` 不可作為優化成果。

02:14 新程式重新盤點的 `artifacts/benchmarks/service-coverage-20260924T0214-corrected.json` 再次列出 50／39／2：已結束的 L1 工作 CPU 為 `null`、保留 282.107 秒上次 wall；上次失敗的 Crypto refresh CPU 亦為 `null`、保留 failed／23.643 秒；仍在執行的 TAIFEX BidAsk 則有該次取樣的 0.042 cores。這是修正測速語意的現場驗收，不代表 L1／Crypto 業務功能完成。

此份全服務收據的量測覆蓋是 **34 項最近一次已完成程序 wall、14 項執行中資源取樣、2 項未觀測**；未觀測的是已停用的舊 hot-artifact sync 與 registered-data-backfill，沒有為了製造秒數而啟動它們。這些仍只是 unit 層量測，不能將每項業務步驟或外部供應商延遲宣稱已量到。

02:17 即時 HTTPS 再驗：本機 `/healthz` HTTP 200／約 1.5 ms，DDNS 公網 IPv4 HTTP 200／約 466 ms，強制 IPv6 連線失敗。這只是當次可達性；沒有因此把 IPv6、業務資料、帳本成交或冷開機恢復標記完成。

同時把 `stale_contract_audit` 再拆成來源契約 SQL、L1 衍生 Parquet metadata 掃描與真正 stale 狀態套用三段計時；不變更稽核路徑。下一正式輪可判定約 108 秒應處理 SQLite join、檔案 I/O 還是有錯誤才會執行的資料庫更新，避免僅憑總秒數改索引或放寬驗證。相關 L1 12 個測試與 Ruff 通過。

同時覆核高 CPU 的 `registered-data-intraday` 正式 02:12:56 輪：Binance 574 symbols 約 29.37 秒、Bybit 867 symbols 約 33.22 秒、OKX 491 symbols 約 56.15 秒，三者重疊執行，整輪 **57.11 秒 wall／約 120 秒 CPU／2.2 GiB memory peak**，各來源這輪 `status_counts.updated` 等於其 symbol 數。這顯示現有 1m tail 三家並行，不能把 29+33+56 秒相加當串接延遲，也不能僅因 2.42 cores 的瞬間觀測就再提高 workers。此輪刻意 `historical_features_enabled=false`，所以不證明歷史特徵已更新或所有資料完整。

## 9/24 02:35 正式雙排程驗收

02:30 Crypto refresh 與 02:30:12 OpenBB L1 同時啟動。Crypto funding 1.353 秒成功；Bybit 日表建置 **157.187 秒**後，394 標的中 383 完成、11 失敗，`build_mode_counts` 是 383 full／0 incremental。失敗的 GALA、GAS、GLM、GMT、GMX、GRASS、GRIFFAIN、GRT、GUN、HBAR、HEI 都明確回報 `Bybit source changed during daily materialization`；盤中分鐘原始更新於 02:30 前後及 02:32:50 又起動，使同一標的的讀取與寫入前證明不一致。正式 service **failed**，沒有進入後續公開特徵、稽核或冷發布。這不是前次單一 `raw writer started` 錯誤，不能把兩者混為一談。383 個成功輸出建立了逐檔 sidecar，但整體訓練資料仍未完成；下輪是否能命中增量與完成 11 檔要用收據驗證。服務這輪約 2 分 39 秒 wall、23 分 36 秒 CPU、6.9 GiB memory peak。`materialize_summary.json` 的 11 failed 是 fail-closed 證據，不能用已存在的舊 Parquet 冒充本輪完整。

OpenBB L1 於 02:35:36 完成：**323.859 秒 wall**、19 新段、4,002,551 待壓縮、0 stale／failed、0 L0 delete。因既有 DB 無新簽章，42 published view 全部按設計保守重建、1 deferred；ETF 佔 view 96.010 秒，整段 view 105.189 秒。新增分段顯示來源契約 SQL **75.800 秒**、既有衍生檔 metadata 掃描 **94.274 秒**、stale 狀態套用 0 秒；未指派來源載入 35.026 秒（找首端點 5.745／端點內排序 27.752／metadata 1.528），建新段 3.322 秒。服務峰值 memory 2.5 GiB／swap 3.6 GiB，`memory.high` 20,009 次、視圖階段額外 I/O full-stall 約 13.5 秒。與上一輪 282 秒的工作量／競爭不同，尤其本輪同時有約 8 核的 Crypto 建置；不可將 42 秒差歸因為程式退步。**下一正常輪才是正式視圖重用的驗收點**。既有檔案 metadata 稽核仍佔大量時間，接下來先用固定實檔樣本比較同一行數／schema 契約的開檔並行方案，不減少檢查或改資料。

完成後獨立唯讀開正式 DuckDB 驗 43 筆 catalog、42 個 published view，每個 published view 的 SQL SHA-256 均與新 catalog 相符；1 deferred 沒有視圖／簽章。這證實下輪已有可驗證的重用起點，但不預先保證下輪實際命中或壁鐘收益。

固定 8,192 個正式成功段的唯讀、低優先權交錯測速：單執行緒 4.364／1.991 秒，4 執行緒 4.449／4.950 秒；兩者 8,192 個行數＋schema 校驗皆相同，並行沒有穩定收益，未接線。另一組在相同 8,192 檔上比較每檔先 `is_file()` 再開 Parquet metadata，與直接開 Parquet、僅在開檔錯誤時補 `is_file()` 分類：舊 1.592／1.657 秒，新 **1.387／1.396 秒**，全部 8,192 筆逐一通過。已把後者接進正式衍生檔稽核，仍逐檔驗行數與 schema，缺檔、壞檔仍 fail-closed；新增實際刪除**測試暫存** L1 檔後 audit 拒絕、正常輪重建與原錯誤原因保留的回歸，OpenBB L1 **13 個測試及 Ruff 通過**。這是局部樣本約 13–16% 改善，正式 15 萬檔掃描仍待下一輪驗收；沒有降低稽核頻率或採信 mtime 代替 metadata。

對另一段 `OFFSET 70000` 的 8,192 個真實檔再交錯：舊 3.687／1.468 秒，新 1.247／1.252 秒；第一個舊樣本顯著受冷檔案快取影響，不能用 3.687 對 1.247 宣稱約三倍提速。同為較熱的後半組，新少約 0.216 秒／15%，8,192 筆輸出仍全相同，支持少一次 `stat` 在不同檔群也有穩定局部收益。

02:46 公開 gateway 39 條有限路由各量 `first_observed`＋2 次 HTTP 回應，`artifacts/benchmarks/all_services_latency_2026-09-24_recheck.json` **0 HTTP 錯誤**。當次首次完整 1m 當沖歷史約 975 ms，日期全區間訊號／事件約 1,770／1,814 ms，持倉約 1,889 ms；同 key 後兩次的低個位數 ms 多為已建立的服務快取，不能冒充冷來源重建。全資料 features 首次約 1,088 ms；TAIFEX 1 日首次約 619 ms。樣本只有每路由 3 筆且順序執行，非 p95、WAN、瀏覽器繪圖或資料正確性證明，也不直接拿來和不同日期／快取狀態的 9/23 基線作固定倍數比較。

OpenBB L1 的來源契約 SQL 原先對所有已壓縮成員逐筆呼叫 Python 路徑正規化。正式 manifest 抽樣 10,000 筆皆為「task 相對路徑／member 絕對路徑」的可直接拼接型態；已在 SQLite 先比對精確拼接，只有不相等時才退回原 `stockagent_resolve_path`，所以絕對、`.`、`..` 與非標準路徑仍沿用原判斷。對正式 330 萬筆成員以唯讀 SQL 順序跑原／新查詢，各回傳 0 個 stale 契約，結果 SHA-256 一致；wall **29.746 → 19.212 秒**。這是受不同 OS 快取／同機負載影響的單組只讀比較，不是正式 75.800 秒的下一輪結果。新增相對、帶點、絕對等價路徑及真正換路徑的回歸，OpenBB L1 14 個測試通過；下一正式 timer 要檢查 `stale_source_contract_query` 是否下降、所有稽核結果與資源壓力是否保持正確。

Crypto 日表的 11 個正式失敗標的都有相同的「來源在單標的建置中原子替換」錯誤。新增**僅對此明確版本競爭**的受限第二輪：其他 383 個結果先保留，第一輪結束後對發生競爭的標的重新擷取來源身分、重算並驗證，再將最終每標的結果只計入進度一次；持續變動或其它錯誤仍為 failed，不能發布。此變更不用暫停每分鐘寫入，也不放寬 Parquet 原子替換前後或整體冷發行的來源證據。相鄰 Crypto／OpenBB 74 個測試通過；**下一次正式 Crypto refresh 尚未執行**，單標的重試也不能根治整個 22 GB mutable `data_bybit` 來源與冷發布的交接問題，故服務健康仍維持 failed。

對當沖全期間事件明細另做一次 `cProfile`：查詢掃描 orders／fills 各 100,000 筆，約 20 萬筆事件中 `consume_batches` 的逐列 Python 欄位查表占約 1.34～1.39 秒；臨時 JSONL 投影寫入約 0.56 秒，另外約 0.32 秒是新程序第一次載入交易日曆。改成把 Arrow batch 的 8 個識別欄位順序轉為列並逐列解包，不改身份、去重、排序、筆數或 scan cap。隔離的同範圍三次結果均為 199,506 筆、前 100 筆 SHA-256 完全一致；後兩次非頁面快取建置 1.728／1.793 → 1.592／1.594 秒，屬不同時間、同機競爭下的樣本。當沖／隔日沖／rollover 173 個測試通過。

03:00:38 僅重啟唯讀 `stockagent-public-dashboards.service` 載入事件頁改動；當沖模擬、Discord Gateway、永豐 TAIFEX BidAsk 的 PID／啟動時間保持不變。本機 `/healthz` 與當沖 status、IPv4 公網 `/healthz` 均 HTTP 200。服務重啟後首筆廣範圍事件頁 `Server-Timing build=2653.643 ms`，之後三個不同 `limit` 的實際 build 約 2249／2250／1939 ms；**未能在現場證明正式 HTTP 延遲改善，也未達一秒**，與隔離 benchmark 不等同。後續應在固定資料版次及負載條件下重測，並優先考慮重用事件範圍投影／增量索引，而不是繼續微調每列 Python 幾十毫秒。

## 9/24 03:08 OpenBB L1 新版正式重用驗收

03:05:49～03:07:59 的既有 timer 正式執行：**129.959 秒 wall／114.386 秒 CPU**，新增 18 段、0 stale／failed、仍待 4,000,503 個 L0 shard、`l0_deleted=false`。對照 02:30 那輪 323.859 秒、19 新段，整體較短約 194 秒，但兩輪來源批次與同機負載不同，不能把全部差距視為程式單一改動的因果效果。正式收據 `data_openBB/_state/l1_compaction_latest.json` 的分段是來源契約 SQL **20.650 秒**（前輪 75.800）、衍生 Parquet metadata **60.719 秒**（前輪 94.274）、未指派來源 33.406 秒（前輪 35.026）、建段 3.128 秒（前輪 3.322）、DuckDB 視圖發布 **5.272 秒**（前輪 105.189）。來源 SQL 的唯讀同 manifest 比較與這次正式方向一致；metadata 仍是最大單段，沒有省掉行數或 schema 稽核。

正式 `view_endpoint_actions` 為 **41 `reused_verified_paths`、1 `rebuilt`、1 `deferred`**；舊輪是 42 rebuild／1 deferred。重建的 SEC filing headers 約 3.244 秒；沒有新段的 ETF 視圖本輪驗證重用約 0.231 秒，前輪在資源爭用下重建 96.010 秒。發布後以正式 DuckDB 唯讀重新驗證 catalog／實際 view SQL SHA-256，43 個端點中 42 published／1 deferred，整體校驗通過。cgroup 本輪至發布後觀察的 swap 為 0、`memory.high` 事件 1,346，前輪峰值約 3.6 GB swap、20,009 事件；仍有 2.5 GB memory peak 和 I/O stall，不能宣稱資源已達下界。下一個可量測大宗是約 60.7 秒的 15 萬既有衍生檔 metadata 校驗；除非建立具備內容與失效證明的增量稽核索引，不應以跳過檔案檢查換速度。

OpenBB 與唯讀 gateway 載入新版後，再跑完整 39 條有限 HTTP 路由各 `first_observed+2`，`artifacts/benchmarks/all_services_latency_2026-09-24_after_openbb_and_gateway.json` 為 **39/39 HTTP 成功、0 錯誤**。這一輪全期間當沖 events 首次約 1,556 ms、signals 約 1,735 ms，data-monitor features 首次約 1,030 ms；其餘同 key 的快回應多是服務快取。此順序式 n=2 測試不是無快取來源重建、併發吞吐、瀏覽器端感受或公網 p95，故仍把廣範圍明細與 features 首次顯示列為未達一秒的瓶頸。

03:10 再產生全服務唯讀收據 `artifacts/benchmarks/service-coverage-20260924T0310-post-optimization.json`：仍是 **50 service／39 timer／2 path**，33 項有最近一次完成程序 wall、14 項有連續執行資源取樣、3 項本輪未量到操作 wall／取樣。第三項是恰在 10 秒取樣中途啟動的 `registered-data-intraday`，按測速契約不能把局部樣本計成平均 CPU；另兩項仍為停用的 hot-artifact sync 與未曾完成的 registered-data-backfill。唯一 systemd `Result!=success` 為 Crypto refresh 的正式 failed；未以重試程式已通過單元測試把它改為成功。

FinLab 55 分鐘長輪再查日誌，20:14:59 最後可觀察的供應商請求是 `broker_transactions`，當時帳號 SDK 印出日用量約 4,720／5,000 MB；至 21:10:10 收到 TERM、沒有該 key 的完成收據。該 unit 的 `TimeoutStartSec=4h`、單 key 包裝 `timeout=600s`；單靠這段日誌不能判明是 SDK 網路、配額、記憶體壓力，還是外部停服務造成 55 分鐘，故不把它算作可藉提高 workers 縮短的 CPU 熱點。目前程式的 `AUTOMATICALLY_DEFERRED_KEYS` 已明列此未界定的大表及其需「有界分片」的條件，下一正常 timer 是 16:10；跳過它只避免阻擋其它 key，**不代表 1,109 項帳號資料全部下載**，目前最後收據仍只有 82 項 local download。此輪未擅自重啟付費帳號下載或改配額策略。

當沖 `margin-actions` 在 9/23 18:00 的來源 collector 已建出 34,573 列公司行動參考與 2,212 筆精確權益事件，但共用冷發布因上游 `stale_feature_build_receipt` 拒絕，`Restart=on-failure` 讓整個來源建置 18:09、18:16 又重跑；其約 2～4 分鐘每輪的資源耗費不是新的資料要求。現在僅在 **`run_tw_day_trade_margin_actions.sh`** 的冷發布呼叫加 `--defer-stale-derived-receipts`：`_check_training_receipts` 仍嚴格拒絕任何 stale 發版，收據明列 `status=deferred`、`reason=stale_derived_receipts` 和阻擋碼，但這個已完成來源工作的 unit 不再因無關上游收據重算三遍。獨立的 `tw-public-cold-publish` timer／預設命令仍嚴格失敗並會重試；其它發布錯誤在來源服務也仍失敗。相鄰公告／當沖來源測試 **72 個**與 shell 語法、Ruff 通過。**未觸發今天新的官方下載或正式冷發布**；下次正常 18:00 必須確認來源收據 `source_ready`、冷發布 `deferred` 而非 `ok`，以及沒有重複 collector 啟動。

03:17 再查公開健康：當沖 status 仍 `degraded`，全資料監控仍 `critical`（65 unable、36 catching up）；IPv6 HTTPS 仍 `curl` connect 失敗，沒有以 39/39 的 HTTP 成功把資料品質、交易證據或雙棧連線標成完成。此次程式與測速變更合併的相鄰回歸為 **361＋72 個 Python 測試**，Ruff、shell 語法與 `git diff --check` 通過；這不是全庫測試或開盤／冷啟動驗收。

## 9/24 03:19 之後的剩餘瓶頸排除

OpenBB 的 8,192 個正式成功 L1 檔逐階段唯讀取樣：`ParquetFile` 開檔 1.903 秒、Arrow schema 轉換 0.191 秒、讀行數 0.008 秒，總 2.144 秒；6,364 種 schema 讓單一 schema 快取也不太可能消去大量 CPU。相同檔案的 `pq.read_metadata` 與 `ParquetFile` 交錯重測約 1.17～1.22 秒，沒有穩定收益；2 執行緒約 2.21～2.25 秒、單執行緒 1.19～1.27 秒，並行更慢。152,012 個成功段已分布於 43 個目錄，相鄰列約 99.97% 同目錄，額外排序也無明確改善空間。這些結果排除幾項看似容易、實際增加複雜度或 I/O 壓力的方案；正式每輪約 60.7 秒的 metadata 完整稽核仍未壓至下界。

未指派 L0 查詢的首個端點 `regulators.sec.filing_headers` 有約 5,961,281 筆 active/success task；現行端點索引需以 temp B-tree 排序，唯讀單輪約 27.658 秒。改強制全域 `task_id` 主鍵的相同 2,048 個結果 SHA-256 一樣，卻約 29.372 秒；只取第一筆仍約 24 秒，因早期 task ID 大量已有 L1 成員。故不能靠簡單換查詢提示改善。新增 `(active,plan_token,status,endpoint,task_id)` 類覆蓋索引可能加速讀取，但會在約 1,329 萬 task、16 GB 正在使用的 SQLite manifest 上帶來建置 I/O、寫入放大與 live archive 競爭；未經隔離完整 benchmark 和安全建置窗，不直接在線上盲建。

當沖公司行動來源工作新增單步 `elapsed_seconds`、最後驗證與總耗時到原有 `readiness.json`；collector 成功／非零退出均保留原有結果與錯誤語意。這使下一次正常 18:00 執行可拆分三個官方來源 collector、Parquet／權益契約驗證與冷發布等待，而不是只看整個 unit wall。唯讀隔離 mock 的成功／失敗收據測試共 3 個及 Ruff 通過；沒有為產生秒數再次觸發官方資料下載。

Discord artifact maintenance 的最近 54.675 秒並非歷史推論變慢：23:00:04 明確寫出 `waiting_source/tw_public_refresh_in_progress`，直到 23:00:52 才繼續，最後 10 個市場均 `reconciled_current_artifact`、attempted 0／failures 0；該輪只用約 9.782 CPU 秒。前幾輪無等候時約 7.2～13.5 秒 wall。這是與 TW-public canonical writer 協調的來源等待，不能靠縮短正式歷史推論 timeout 或重啟 Discord Gateway 消去；目前不修改來源閘門，只保留等待與工作耗時的區別。下一 timer 13:40 的排程若再長，可用相同日誌判斷等待或真實工作。

## 9/24 03:36 當沖分鐘曲線重試成本與來源延遲

正式 9/23 18:00 分鐘曲線工作停於 `waiting_source/benchmark_source_preflight`，因為 `data_tw_index_futures/shioaji_history/TXFR1/receipts/trading_date=2026-09-23.json` 確實不存在；未執行基準或曲線重建。單次 `_completed_scope` 約 0.447 秒、對 221 MB／195,765 列 `marks.jsonl` 重掃不可缺的 09:01／13:30 端點約 1.601 秒、TX receipt 查詢不足 1 ms。這區分了空等的 CPU 成本與真正尚未取得來源的時間。

現已在分鐘曲線維護狀態收據附上端點預檢的原始檔 `device/inode/size/mtime_ns/ctime_ns`、交易日、模式集合與契約版號。只有上次確定 **0 個缺漏** 且以上全部相同，才略過 221 MB 的再次掃描；來源增刪改、模式或日期改變即全掃，且全掃前後簽章變動會拒絕發布。相同實帳本的隔離量測為首輪 **1.680 秒**、同源重用約 **0.011 ms**。正式 oneshot 連續啟動：首輪仍報 `waiting_source`、端點 `reused=false`，後輪 `reused=true`，最後一次收據分段 `completed_scope=0.463004s`、`accepted_endpoints=0.000069s`、`benchmark_source=0.000116s`；整個單位從前次約 6 秒降到約 2.6 秒 CPU，仍含 Python 啟動和狀態掃描。這些是同夜不同負載的樣本，不是固定倍數保證；相鄰分鐘曲線與排程測試 **65 個通過**（含掃描中來源變更拒絕），Ruff 與 whitespace 檢查通過。

**截至 03:36 的來源等待（後續已解除，見文末 04:23 續修）**：TX 歷史 runner 9/23 14:31 因 `live_connection_reservation` 寫入等待收據，下次 9/24 05:00:10 才允許登入。這保留夜盤 FOP 三條與股票行情兩條連線；直接取消等待會觸碰每人五條連線與即時行情契約。另 `prepare_query_calendar` 的共用期貨完整日界線是 16:30，即使在 14:31 開放連線，也不會把當日 TX 視為完整。曾考慮 **僅 TXFR1、官方日盤確定收束、在 14:45 前硬停止的優先查詢**；後續確認基準重建其實可使用已留存的完整 FOP 一秒報價，因此沒有修改正在運作的連線排程。05:00 歷史工作仍須依自己的收據驗收，不能因曲線已用本機來源補齊就宣稱 TX 歷史 Tick 已下載。

## 9/24 03:41 OpenBB L1 正式續測

03:39:14～03:41:10 的既有 timer 正常完成：**115.955 秒 wall／109.613 秒 CPU**、19 個新段、0 stale／failed、41 視圖重用、1 重建、1 deferred、4,000,503→3,998,455 個來源 shard 待壓縮、`l0_deleted=false`。正式收據的來源契約 SQL **20.536 秒**、既有 L1 metadata **43.010 秒**、未指派來源 **36.956 秒**（其中 endpoint 內排序 30.992 秒）、建段 **3.396 秒**、view 發布 **5.153 秒**。發布後以正式 DuckDB 唯讀檢查 catalog、實際視圖集合與 SQL SHA-256：42 published／1 deferred 全部通過。與 03:05 輪的 129.959 秒、18 新段相比，來源 SQL 20.650→20.536 秒幾乎不變；metadata 60.719→43.010 秒下降，但同機快取與工作量不同，不把這 17.7 秒全歸於改動。cgroup 分段讀取 `memory.high=0`、swap 0；systemd 記錄峰值約 2.05 GiB，不能推定每輪都無壓力。下一個真成本仍是完整 metadata 驗證與 13m-task SQLite 未指派查詢；前述並行、`read_metadata`、索引提示的負面測速說明不應盲目增加執行緒或略過證據。

03:44 的全服務唯讀重盤收據 `artifacts/benchmarks/service-coverage-20260924T0342-post-openbb.json` 仍是 **50 service／39 timer／2 path**；當時唯一 `Result!=success` 的 StockAgent unit 是 `stockagent-crypto-training-refresh.service`。宿主 CPU、memory、I/O 的 10 秒 `full` PSI 均為 0，這只表示那段時間沒有可觀察的宿主級阻塞，不等於各 job 來源完整。Windows Caddy 工作顯示 Running，但最新排程結果非 0；必須以 HTTPS 實際可達性與 Windows 工作日誌分開驗證，不能直接宣稱其冷啟動鏈健康。

03:45 HTTPS 實際驗證：本機 gateway `/healthz` HTTP 200／約 1.6 ms、DDNS IPv4 HTTPS HTTP 200／約 293 ms、強制 IPv6 HTTPS 仍無法建立連線。這證明當次服務對 IPv4 用戶可達；沒有證明 Windows 工作的非零結果無害、重開機後可恢復、IPv6 可用或業務資料健康。

Windows 排程唯讀檢查顯示 `StockAgent Public Caddy` 為 `Running`、`MultipleInstances=IgnoreNew`、每分鐘有下一觸發，而 `LastTaskResult=2147946720`；這**符合**已執行的長駐任務遇到重複觸發時拒絕新實例的型態，但 Task Scheduler Operational 日誌在本次時窗沒有可用事件，故不將原因定論。未重啟 Caddy 或改排程；目前只驗證既有實例的 IPv4 可達，冷開機路徑仍需另測。

對 1,313 秒級 Yahoo US daily 修復程式碼的唯讀檢查發現一個未修的超時契約缺陷：`_run_parallel_symbol_downloads()` 先用 `as_completed(futures, timeout=None)` 等待工作完成，再呼叫 `future.result(timeout=repair_symbol_timeout_seconds)`；後者對**已完成**的 future 不會提供逐標的 wall-clock 上限。底層每次 Yahoo 網路呼叫另有 `_fetch_with_hard_timeout` 與 socket timeout，但整個標的的重試／合併不受這個宣稱的外層數值約束。不能因設定了 `repair_symbol_timeout_seconds` 就把供應商長尾風險視為已修復；應在下一輪以可中止的工作邊界、完整來源／檔案原子性及實測供應商回應驗收，不直接關閉外層 executor 而留下仍寫檔的執行緒。

## 9/24 03:52 Crypto 正式重試與冷發布範圍

在既有交易開盤保護時窗以外啟動正式 `stockagent-crypto-training-refresh.service`，03:52:18～03:53:16 的日表階段 **55.973 秒**；`data_bybit/perpetual_daily/materialize_summary.json` 證明 394／394 標的完成、0 failed，其中 383 尾段增量、11 首次全量。相較 02:30 的 157.187 秒／383 完成／11 failed，這次確實驗收了單標的有界重試與尾段增量的組合，但不同來源狀態及負載下不能把秒數比值宣稱為固定加速率。正式服務 systemd `Result=success`，**業務收據卻是 `deferred_source_changed`**：日表完成前後的 2,143 個輸入 metadata 簽章不同，只記下 funding 為已完成步驟，未做其後特徵報表、稽核與冷發布。`Result=success` 在此只表示「有證據地讓路」，不等於刷新完成；全流程仍未修復。

隔離測試先前失敗的 11 個標的，在另一輪原始分鐘採集中 11／11 可完成、0 failed；這不代替正式 394 標的與冷發布的驗收。根因是約每兩分鐘的原始寫入持續替換檔案，55 秒日表後的全鏈路不能靠固定排程保證 22 GB mutable 來源持續靜止。此部署的 ext4 對測試檔不支援 `reflink`，因此不能假設零拷貝 CoW 快照可用；短暫 `publish_ready=true` 也不足以讓完整打包／雜湊跨越下一次 writer。後續需要 catalog 約束的、可逐檔證明且不阻塞原始採集的一致來源版本／發布交接機制，不會略過 writer 閘門或冒充已發布。

另依現有「可重建 cache 不進冷 release」契約，catalog 的 Bybit 發布現在排除 `perpetual_daily/panel_cache_v2`。正式來源唯讀盤點：原 4,716 檔／23,380,394,831 bytes；選定發布 4,674 檔／22,282,215,603 bytes，少掃／少傳 **42 個可重建檔、1,098,179,228 bytes（約 1.02 GiB）**，原始 1m、funding、正式日表與收據保留。隔離小型冷 release 及 materialize／checksum 驗證確定資料保留、cache 排除；相關 packed、publisher、Crypto 測試 **25 個通過**、Ruff 與 diff whitespace 通過。這降低下一次成功發版的成本，**沒有發布新的 Bybit head**，也不解決 writer 競爭。

全服務唯讀測速清單現在對 Crypto refresh 額外讀業務收據，並以 systemd 單調啟動時間及本次觀察時鐘驗證它屬於**同一次**執行；過期的 completed 收據不會套到新的一輪。實際 `artifacts/benchmarks/service-coverage-20260924T0410-crypto-semantic.json` 仍列 **50 service／39 timer／2 path**，明確同時顯示 Crypto `Result=success`、exit 0、最近程序 wall 57.886 秒，及相符的業務狀態 `deferred_source_changed`。這補上「程序成功不等於資料完成」的觀測盲點，不假裝改善實際發版延遲；相關稽核／發布／Crypto **30 個測試**與 Ruff 通過。

同一個相符收據欄也接到既有每 5 分鐘長期服務趨勢（不另開 daemon 或無限成長的記錄檔）。04:08:42 正式 `all_service_runtime_sample` 已在 50 項服務中寫下 Crypto 程序退出 0 與 `deferred_source_changed` 並列，未把 deferred 當 completed；1 秒唯讀人工重測同樣辨識成功。完整相鄰回歸現在 **36 個測試**通過，含過期收據不可冒用、長期趨勢與冷 release 排除 cache；Ruff、diff whitespace 通過。長期測速仍只量 unit 資源與最近工作 wall，沒有把每一項業務請求 p95 宣稱已蒐集。

高頻全資料快照的 55,873 個檔、60,280 項 feature 凍結輸入測速：25.7 MB 快取 JSON `read_text+json.loads` 中位約 388 ms，僅用標準庫 `read_bytes+json.loads` 約 336 ms；路徑發現約 610 ms、單次 stat 約 304 ms、完整 feature 投影約 657 ms。正式 30 秒 worker 在 04:12 的兩輪分別約 3.27／4.86 秒，後者刷新 780 個 footer；凍結輸入的局部耗時不可直接相加或外推成正式收益。試作「未變資料組重用彙總」後，55,873 檔／234 組輸出 SHA 一致，但交錯測速原全彙總中位 **80.8 ms**、新驗證重用 **47.9 ms**，只省約 33 ms；已撤回這項額外複雜度，正式仍逐組核對，不以 33 ms 冒充 3～5 秒瓶頸已解。

上述測速時另外發現一個原有的觀測競態：檔案在目錄發現後、footer 更新前消失時，`refreshed_files=0` 原先可直接重用先前 `verified` 總數。現在只要本輪 stat 遇到不可讀檔，即禁止零變更快取短路、重新彙總並顯示 `scanning`／`count=null`；沒有把缺檔變成零列或已完成。以檔案在兩階段之間消失的回歸驗證；資料監控、服務趨勢、Crypto 發行相關 **106 個測試**、Ruff 和 diff whitespace 通過。這是正確性修復，幾乎不影響正常快路徑；真正的大宗 30 秒監測計算仍待優化。

同一 25.7 MB JSON 再做 8 次交錯 `read_text`／`read_bytes` 標準庫解析：值的 SHA 相同，中位 **317.9／309.9 ms**，先前 388／336 ms 受取樣環境影響，較可信的本輪局部收益僅約 8 ms。保留單行 `read_bytes` 以避免一次字串解碼／配置，但不將它視為顯著服務加速；70 個資料監控測試與 Ruff 通過。

## 9/24 04:23～04:40 當沖分鐘曲線來源閘門與實帳本續修

分鐘曲線預檢原先只接受當日 `TXFR1` 歷史 Tick 收據，但正式基準重建器優先接受帶 `complete` manifest、實際開盤報價與完整分鐘覆蓋的本機 FOP 一秒報價。這造成 9/23 已有來源卻被拒絕，空等 9/24 05:00 歷史排程。現在預檢呼叫與正式重建器同一組讀取／檢查函式，先驗留存 FOP capture，必要時才驗 receipt-backed Tick，並將兩者根路徑明確傳給子程序。9/23 實檔為 `TXFJ6`，3 份完整 manifest、**300／300 個分鐘點都是當分鐘新鮮報價**；單次預檢約 **2.08 秒**，沒有新增永豐登入。這解除來源等待，但不代表全部基準及策略已修好。

第一次正式 `--no-fetch` 重建成功補出 **145 個交易日、44,080 筆基準標記**，卻在策略階段因 9/23 `tw_day_trade_multi_basis:6168` 的持倉 35,000 股與僅讀原始 `entry` 18,000 股不符而 fail-closed。帳本另有使用者先前授權、明示 **非券商成交** 的 `entry_completion` 17,000 股，同價 42.65；重建器錯把它當出場。現將原始進場與明示補單相加驗持倉與均價，分鐘估值及攜倉稽核也把它視為有揭露的庫存增加，不冒充交易所撮合；缺原始進場、價量不一致或稽核缺補單證據仍拒絕。相鄰分鐘／攜倉／稽核 **64 個測試**、Ruff、whitespace 通過；未改原始 append-only fills。

第二輪低優先權、`--no-fetch` 正式流程 04:40:05 收據為 `ready`：**145 日 × 5 模式 × 每日 270 點 = 195,750 個策略分鐘點**，09:01 開盤及盤中未驗證來源點均為 0；所需 **52,680 個標的／日期組**本機可用，缺口 0、API 請求 0。公用 gateway 的 9/23 `1m/v2` 實讀有 8 序列：台指 300、0050／2330 各 271、五個策略各 270 點，`historical_minute_missing_price_points=0`；持倉頁 6168 顯示 35,000 股、42.65 元，`entry_completion_broker_fill=false` 與補單契約仍在。這些證明資料顯示與來源覆蓋，不是券商成交證據。

**尚未完成的正確性與性能邊界**：重建收據記錄 9/23 五模式合計 **1,185 個**新舊分鐘權益差異點，最大約 **NT$48,513.82**；本輪未要求獨立估值 parity，故 `ready` 只代表指定的來源／點數／價格契約通過，不能宣稱舊曲線數值逐點相同或已釐清全部差異。`audit_tw_day_trade_margin_replay` 正式回放收據只到 9/22、即時 append ledger 已有 9/23，直接拿它審完整 live 根會因範圍不一致拒絕；需要分離正式回放與即時追加範圍才可得到可解釋的全鏈審計。基準重建第二輪即使 `projection_changed_sessions=[]` 仍花約兩分鐘並重寫總檔；策略全期重估從約 04:33:25 跑到 04:39:44，約 6 分鐘，峰值程序 RSS 約 3.8 GiB、無本程序 swap。這是下一階段增量化與熱點量測的明確目標，不能靠刪除分鐘點或跳過來源驗證。維護收據已新增 `benchmark_rebuild`、`minute_curve_rebuild`、`post_validation` 分段秒數供下輪正式執行驗收；04:40 這次程序在加欄位前已啟動，因此本次收據沒有該新欄。

後續在來源與帳本不變的 `--no-fetch` 正常 no-op 路徑分段量測：未修前端點檢查約 **1.76 秒**；`ready` 收據保存已驗證的 `device/inode/size/mtime_ns/ctime_ns` 與日期／模式指紋後，相同來源下約 **0.0001 秒**，來源任何可觀測異動仍全掃並檢查掃描前後簽章。又發現原流程先用完整 221 MB marks 帳本完成帶輸出 SHA-256 的 270 點／日檢查，隨後再讀它作開盤與盤中價格來源檢查；現在只在前者已驗證所有日期／模式、明確重估 09:01、全部點數齊且來源缺漏為 0 時重用其結論，否則仍走獨立逐筆掃描。相鄰正式 no-op 的 `strategy_price_provenance` 約 **3.15 秒 → 0.000021 秒**，整段 existing validation **10.71 → 4.67 秒**；它們是不同時刻單輪樣本，不能直接聲稱固定提升率。分段計時保留在 `artifacts/operations/tw_day_trade_minute_curves/latest.json`。

基準驗收過去每次反序列化約 **294 MiB** 的總檔；現在先核對總檔 SHA-256、分區 head 的來源 SHA、每個所選分區的 gzip 內容雜湊與行數，再對約 **7.9 MiB** 的正式投影執行相同三基準逐分鐘網格檢查；head／分區缺失或損壞則回退總檔，不會以未驗證快取放行。一次唯讀實檔量測總檔 SHA 約 **0.183 秒**、145 個分區載入驗證 **0.727 秒**；相鄰 no-op 的基準驗證約 **4.59 → 1.46 秒**，但單輪負載有變動。投影快路徑與損毀回退、策略及維護相鄰測試 **71 個通過**。正常 no-op 最近一次仍約數秒，其中完整策略帳本驗證約 3.2 秒、FOP 來源檢查約 3.5 秒，沒有宣稱已到極限。

基準建置器也不再因**僅** live `state.json` 的無關欄位 SHA 改變而重寫語意完全相同的總檔；市場來源、origin 或任何 mark 變動仍重寫。這避免獨立重試時的約 294 MiB 無效輸出與下游失效，但完整日間基準重建本身仍可能花數分鐘。注意維護流程的先建稀疏官方開收基準、再展開完整股價分鐘曲線是**真正不同**的兩份內容，該必要轉換仍會改總檔；不能把這項無效寫入修正說成已消除所有重寫。相關基準與投影測試 **54 個通過**。

04:58 再跑跨模組聯合回歸：當沖基準／分鐘曲線／攜倉稽核、全資料監控、服務測速、冷資料發布共 **175 個測試通過**，Ruff 與 diff whitespace 通過。同期服務盤點仍是 **50 service／39 timer／2 path**，systemd 最近結果均為 success；但 Crypto 業務收據仍為 `deferred_source_changed`，當沖公用 API `health=degraded`，所以不以程序結果宣稱全部資料或交易服務已正常。

## 9/24 05:00 歷史永豐共用登入與全掃成本

05:00 後同時有股票分鐘回補、通用期貨／選擇權歷史回補與 TX 連續期貨歷史回補。股票分鐘工作依五條帳號連線契約使用兩個 worker，並**沒有**持通用／TX 共用的 `login.lock`；TX 的 `history_login_slot_busy` 是通用歷史工作持鎖，不能把等待誤歸因於股票分鐘工作。通用歷史工作首輪發現 5,731 個合約、3,954 個 pending 查詢；其 500 筆有界批次於 05:00:15 啟動，05:08:43 才產生終端摘要，TX 每 60 秒仍留有等鎖收據。日誌中第 99→100、199→200、299→300、399→400、499→500 筆的間隔約 40～49 秒，與 `_write_summary` 每百筆對完整收據樹做驗證相符；實際單次稽核秒數需由新版分段日誌驗收，不能把全部空隙當作純 CPU。

已在通用歷史 runner 加**收據條件的批次公平讓位**：若 TX 的等鎖收據為最近 120 秒內的 `history_login_slot_busy`，通用歷史批次結束並釋放鎖後讓出 75 秒，足以跨過 TX 的一次 60 秒重試；沒有等待者仍只間隔 30 秒。每人五條連線、即時 FOP 與股票報價保留、07:45 歷史查詢截止及 90% 流量上限均不變。下載器另在達到 `--max-queries` 且必定立刻進終端稽核時，省掉重複的「中途」全掃；每百筆非終點與終端的正式收據稽核保留，並在日誌增加 `summary_audit elapsed_seconds`。這是排程／重複驗證的局部優化，不是減少市場查詢、放寬缺漏或假設 TX 資料完成。相關 Shioaji 排程／收據測試 **38 個通過**、Ruff、shell 語法與 diff whitespace 通過。

部署界線：在第一輪無下載子程序的 30 秒批次間，曾 `systemctl stop` 通用歷史 service；啟用中的 `Persistent` timer 隨即於 05:08:57 再啟動，並重新做一次庫存發現。這次切換未讓 TX 先取得鎖，且造成額外庫存成本；因此不再於活躍查詢中重啟／停用 timer。新的 runner 讓位邏輯由 05:08:57 新主行程載入；第二批 500 筆完成後，05:16 正式寫出 `yield_to_waiting_tx_history`，TX 於 **05:16:26** 開始自己的 64 日期批次，證明這項公平讓位在實際共用鎖競爭下生效。TXFR1 的 **2026-09-23** 45,197 筆 Tick 收據於 05:16:47 顯示 `complete`、`session_finalized=true`、實際 TXFJ6 合約，1,169,462-byte Parquet SHA-256 與收據逐位元一致；TXFR1 本機歷史顯示 1,586／1,586 日期已解析。這是該合約／日期的資料證據，**不**代表全期貨產品、整批 64 日期或冷發布已完成。下載器「不重複終端全掃」須等下一個 Python 批次子程序才會載入；其分段稽核秒數、總 wall 與 TX 全批終端收據仍待驗收。

同一 05:16 TX 批次在完成 64 次查詢後仍逐一稽核 773 個 alias × 最多 1,586 個日期，05:24 已掃到第 500 個 alias 且仍持登入鎖；這些剩餘掃描不會增加本批 API 取得量。新版在有界查詢額度耗盡時寫 `batch_partial`、`coverage_scope=scanned_contracts_only`、實際已掃與總合約數，隨即退出釋放登入；只有全目錄掃完才可寫 `batch_finished` 並進入正式發布門檻。下一批由已驗證的逐日收據續跑，沒有把未掃合約當作零缺漏。部分批次以 60 秒重檢；若通用歷史工作正等鎖則先讓位 75 秒，以避免反向飢餓。Contract V2 庫存刷新維持每小時一次，其間只讀已保存目錄，市場歷史查詢仍受 07:45 截止和 90% 流量上限保護。此 TX 變更**尚未載入目前正在執行的舊 Python 子程序**，需在其完成後再部署與驗收。

通用歷史 `_valid_receipt` 增加最多 131,072 筆的程序內驗證快取，以收據與資料檔的 `device/inode/size/mtime_ns/ctime_ns` 作失效條件；首次仍實際讀收據與核對資料 SHA，後續相同檔案重用，掃描中檔案變更拒絕，回傳拷貝避免呼叫端污染。2,000 個正式 futures KBar 收據的獨立唯讀樣本，首次 **1.089 秒**、再次 **0.047 秒**、兩次 2,000 筆狀態 SHA 相同，該程序最高 RSS 約 **106.5 MiB**；此樣本不等於 11 萬個收據的整批正式 wall 或記憶體保證。下一個通用歷史 Python 子程序才會載入快取與 `summary_audit` 分段計時。相關 Shioaji **39 個測試通過**，包括檔案改動、刪除、掃描途中替換與部分批次不可發布；Ruff、shell 語法、whitespace 通過。

## 9/24 05:28～05:42 新批次驗收與跨服務冷啟動

05:28:50 僅在 TX 舊批次子程序已完成、通用歷史持共用登入鎖時，重新載入 TX runner；沒有重啟即時 FOP、股票行情、Discord 或當沖引擎。新版 TX 05:33:56 取得登入、05:34:47 在 64 次查詢額度耗盡時寫 `status=batch_partial`、`coverage_scope=scanned_contracts_only`、**22／773** 個 alias 已掃後釋放鎖；`missing_dates_after_batch=0` 僅是這 22 個的部分視角，`current_query_sweep_complete=false`，不能拿來發布全目錄。通用歷史 05:35:41 重新取得鎖，雙向讓位都已現場觀察。TX cgroup 曾顯示約 7.2 GiB memory，但子程序退出後 `memory.stat` 約 6.7 GiB 是可回收 file cache，不是仍有 7 GiB Python heap；須分開記憶體來源與實際宿主壓力。

通用歷史新版 500 查詢批次的正式完整摘要稽核：query 1／100／200／300／400／500 分別 **13.946／21.697／11.220／11.996／10.758／14.162 秒**；第 500 筆已是 `kind=final`，沒有重複做中途摘要。下一批 query 1／100／200／300／400／500 則約 **12.577／19.621／12.882／14.851／19.575／12.755 秒**，表明冷熱快取與並行負載使絕對秒數浮動。唯讀 cProfile 對 5,731 個合約的完整摘要，一次約 62.141 秒（有額外 profiler 開銷與其他服務競爭）；其中 `_valid_receipt` 呼叫 305,022 次、`stat` 約 1,009,814 次，`build_tasks` 又重掃來源，正式非 profiler 的 10～22 秒也有相同結構。每筆查詢已有獨立 `PersistentProgress` 收據；因此後續新 Python 批次把昂貴的全樹中途摘要由每 100 筆改每 **250** 筆，仍在第 1 筆與批末做完整、fail-closed 稽核，不減少查詢或收據驗證。這項新 cadence 尚待下個子程序實跑；不能把 profiler 62 秒當成既有正式耗時。

另一個跨服務成本來自「僅需退佣時點規則」的執行設定卻連帶載入 PyTorch。現在把純字串／日曆規則置於 `stockagent.backtest.tw_commission_rebate_policy`，原 `tw_commission_rebate` API 仍原物件轉出，訓練的 Torch 計算函式與語意未改；正式基準重建器把只在 `main()` 使用的模型設定匯入延後，讓當沖分鐘曲線預檢不載入訓練 runtime。新 Python 程序的單次匯入量測：`tw_day_trade_simulation` **1.350 秒／約 529 MiB → 0.346 秒／約 68 MiB**；`maintain_tw_day_trade_minute_curves` **3.508 秒／約 577 MiB → 0.446 秒／約 111 MiB**。同一份實帳本以直接 `--no-fetch` 再跑顯示 `ready/no_op_already_complete`、195,750 個策略分鐘點，**6.87 秒 wall、約 399 MiB 峰值**；其中最新基準來源預檢 2.191 秒、策略驗證 2.403 秒、基準驗證 1.238 秒。這些不是與 05:30 嚴重冷 I/O 競爭下 67 秒 systemd 執行的同條件 A/B 比較，不能宣稱全程固定快 10 倍。交易、基準、曲線、退佣、Shioaji 相鄰 **286 個測試通過**，新增的「乾淨子程序匯入不可載入 Torch」與原 API 身分回歸亦通過（後續 57 個測試）。

05:40 全服務唯讀收據 `artifacts/benchmarks/service-coverage-20260924T0540.json` 仍為 **50 service／39 timer／2 path**、systemd 最近結果沒有失敗；10 秒取樣中 Shioaji 通用歷史約 0.595 core、packed backup 0.100、Discord 0.057、當沖模擬 0.054。宿主 memory `full` PSI 10 秒約 0.83%，這是取樣時的競爭，不是任何單一服務固定成本。Windows Caddy 冷啟動、外部 IPv6、Crypto 冷發布、當沖實際成交與 50 服務所有業務收據均未因上述 CPU 優化自動變為完成。

`build_tasks` 的 Tick 目標原先在 KBar 已驗證一次後，又呼叫 `observed_tick_dates` 逐塊重讀同一批收據。現在在原有 `_valid_receipt` 驗證迴圈直接收集 `observed_trading_dates`，原有官方活動日期聯集、來源缺漏與任務排序不變。正式 5,731 合約、79,143 個觀察日期的唯讀對照：新版規劃產生 1,057 個當下待查任務、單次約 **30.708 秒**（與其他 worker 競爭）；舊式**額外**重讀相同已驗證 KBar 日期另花 **2.124 秒**。這 2.124 秒是可刪除的獨立遍歷，不可和不同負載的正式摘要耗時直接相減。單次 KBar 驗證次數及 Tick 目標日期的回歸、Shioaji 排程共 **41 個測試通過**，Ruff、whitespace 通過；待下一個自然啟動的 Python 批次觀察正式耗時。05:45 前仍為舊程序，但 64-query TX 批次已再次正常讓位與續跑，未見共用登入鎖飢餓。

05:46:23～05:49:22 的下一個**自然啟動**通用歷史批次已載入新規則，正式只在 query **1／250／500-final** 寫出全樹稽核，分別 **16.612／10.592／12.226 秒**；仍完成 500 個查詢並保留逐查詢進度。相鄰上一批約 4 分 4 秒，這批約 2 分 59 秒，但合約／待查內容、快取與其他服務負載不同，**65 秒差額不是可歸因的固定加速保證**。05:45:46～05:46:14 的 TX 64-query 批次亦再次退出讓位，沒有重新登入即時 FOP／股票服務。

衍生品成本物件的另一處共用冷啟動耦合：`stockagent.config` 原為了讀 `FuturesCostSchedule`／`OptionDayCostSchedule`，直接匯入包含模型 Tensor 計算的模組，乾淨程序 **1.482 秒／約 563 MiB** 且載入 Torch。依既有衍生品契約只抽出純費率 dataclass 到輕量 policy，舊模組 re-export 同一類別並保留 pickle 舊路徑，原預設、驗證、交易時鐘與 checkpoint 語意不變。新版 `stockagent.config` 匯入 **0.311 秒／約 82 MiB**，`scripts.run_tw_day_trade_simulation` 匯入 **0.322 秒／約 111 MiB**，均未載入 Torch；這是冷匯入局部量測，不能套用為整個訊號延遲的下降。正式衍生品測試與舊類別序列化回歸仍在進行中，未重啟任何盤中引擎。

上述衍生品／配置／舊 class path 測試正式完成 **102 個通過**（74.56 秒）；新測試明確覆蓋兩種費率類別的原 API 物件身分、pickle 還原、預設與無效值，以及乾淨程序匯入 config 不得帶入 Torch。Ruff 與 whitespace 通過。這項抽離不改資料集、模型輸出、決策時間、收費公式或 checkpoint 指紋；技能流程的實資料完整分區與 fold 驗收適用於新增衍生品交易模式，本輪純載入邊界重構沒有宣稱完成新的訓練或策略表現。

05:51 本機公開 gateway `/healthz` 為 HTTP 200／約 1.8 ms、TAIFEX 面板 `/healthz` 約 44 ms；但公開全資料摘要仍是 **critical**（327 個啟用端點：225 complete、37 catching up、65 unable），當沖 `/tw-day-trade/api/status` 仍 **degraded**。這些讀取只證明本機服務可達與目前來源健康分級，不證明公網 IPv6、交易成交或缺漏已消除。

## 9/24 05:54～06:03 公開路由與冷請求續測

全公開閘道 **39 條路由**以單連線／每路由兩次穩態取樣，回應錯誤 0；收據 `artifacts/benchmarks/all_services_latency_2026-09-24_0554.json` 保留首請、Server-Timing、HTTP 傳輸與客戶端 JSON 解析的分離耗時。此小樣本不是 p95 SLA。最慢首次請求是完整當沖 1m/v2 歷史 **8,660 ms**，其中服務端 `build` **8,528 ms**、客戶端解壓後 body 約 129 ms、額外 JSON 解析約 286 ms；隨後同端點 4 次重測首次 83 ms、穩態中位約 98 ms。來源 04:39 重建後，分段投影 head 在 05:52 首位請求才更新，顯示冷建置被轉嫁給訪客，不能以熱快取速度冒充整體體驗。

離線測速腳本未設正式 `STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR` 時，雖聲稱可用 immutable session shards，實際退回完整來源掃描：**4,128 ms、317,840 點、8 序列**。修正測速收據，現在記錄實際索引目錄和分段投影可用性；以與公開服務相同的索引目錄重測、略過最終完整投影但保留已驗證分段，僅 **490 ms**，輸出 SHA-256 與全掃相同。這兩次不是同一 cold OS cache 條件，不能把差值歸於單一演算法；重點是工具不得誤標「分段命中」。

已把完整歷史的**盤後預熱**接在分鐘曲線與三基準通過正式驗證之後：只向固定 `127.0.0.1` 公開唯讀 gateway 讀取完整 1m/v2 曲線，不登入永豐、不觸動交易；最多等 20 秒，結果在維護收據記為 `warmed` 或 `not_warmed`，預熱失敗不會冒充曲線來源失敗。現有資料的直接整合呼叫 HTTP 200、14,720,478 bytes、約 **41 ms**（已熱）；尚未等到下一次**真正來源變更後的正式維護**來驗證它能否把首位訪客 8.66 秒轉移到盤後工作。67 個測速／曲線相關測試通過。

全資料欄位清冊首次 HTTP 約 **1,472 ms**，其中服務端建置約 1,343 ms；37,366,561 decoded bytes／774,195 gzip bytes，瀏覽器仍需解析及處理約六萬筆。凍結來源單步量測：讀檔 33 ms、JSON 解析 511 ms、對已精簡來源再序列化 **441 ms**、gzip 66 ms、SHA-256 18 ms，輸出與來源 bytes 相同。公開閘道現在維持完整 JSON 解析、唯讀／不可控制契約及非有限數拒絕，並要求開檔前後與路徑的 inode／size／mtime／ctime 一致；直接使用已驗證來源 bytes，刪去第二次 37 MB 序列化。隔離本機新流程首建 **414 ms**、熱命中 0.09 ms。公開 gateway 於 06:02 **單獨重啟**載入變更後的實際首次 HTTP 864 ms、後續 3 次中位 111 ms；這些受 60,280 筆客戶端解壓／解析及並行負載影響，不宣稱 414 ms 是公網端到端保證。公開／測速／分鐘曲線聯合 **159 個測試通過**，Ruff 與 `git diff --check` 通過。

部署後本機五條關鍵路由 `/healthz`、overview、欄位清冊、當沖完整曲線及 status 共 0 錯誤，當沖完整曲線首次約 90 ms（重啟預熱已載入既有投影）。DDNS IPv4 HTTPS `/healthz` 為 HTTP 200、約 0.42 秒；IPv6 同端點約 3.49 秒後連線失敗。全資料摘要仍 **critical**（225 complete、37 catching up、65 unable），當沖仍 **degraded**；此輪沒有修復外部 IPv6、來源缺漏或模擬／券商成交差距，也沒有重啟即時交易或報價服務。

06:04 完整重測 39 條 HTTP 路由再次 0 錯誤，收據 `artifacts/benchmarks/all_services_latency_post_deploy_2026-09-24.json`。其中跨 2/25～9/24 的訊號／事件首次仍約 **1,562／1,598 ms**，剖析顯示有界 100,000 筆帳本暫存欄式投影與事件累計為主要成本；這不是熱命中約 2～3 ms 能取代的冷路徑。正式 API 的 `scan_limit_reached=true`：訊號返回 `total=100000`、事件 `total=199506` 只是掃描範圍內統計，UI 已警告較早紀錄未計入。不能為降秒數再降低上限，更不能宣稱跨日明細已含所有歷史；下一步需要依日期分區的精確索引／分頁與真實總數契約。

公開 HTTPS 真正 Chromium 驗收分別以 1366×768 筆電與 390×844 手機跑完整 8 頁，原始報告 `artifacts/benchmarks/public-browser-20260924-1366x768.json`、`artifacts/benchmarks/public-browser-20260924-390x844.json`。兩種視窗全部 8 頁皆無全頁水平溢出、JS console error；筆電沒有表格溢出，手機觸控目標風險清單為 0。單次當沖筆電測得初次資料 request-to-paint opportunity 約 109 ms、完整 317,840 點歷史選取至繪出約 637 ms（API 本身約 275 ms，畫圖準備約 36.3 ms）；這是當下公網瀏覽器個例，不是每種裝置／網路的 SLA。桌面 audit 的自動觸控尺寸規則在一些密集按鈕仍報風險，但手機版對應規則為 0；後續如改善滑鼠密集控制，需保持筆電一眼可見而不增加橫向捲動。

06:09 全服務再盤點 `artifacts/benchmarks/service-coverage-20260924T0609.json` 仍為 **50 service／39 timer／2 path**，systemd 最近程序結果均非 failure；正在執行的註冊資料盤中服務與永豐通用歷史取樣各約 **1.047／0.992 CPU core**，屬當下真實來源工作而非可直接刪掉的背景忙等。Crypto refresh 最近程序雖退出 0，同輪業務收據仍是 `deferred_source_changed`。該短取樣宿主 memory full PSI10 約 0.32%、I/O full PSI10 約 0.15%，因此單端點冷耗時比較仍要標註並行下載負載。不能以 50 個 unit 的 `Result=success` 冒充所有資料、憑證、冷發布與開盤交易鏈健康。

## 9/24 06:12～06:16 高頻欄位清冊重用成本

30 秒一次的資料監控 worker 在 `refreshed_files=0`、欄位清冊可重用的正式輪次，`feature_projection` 仍約 **562～922 ms**；單獨對當下 37.4 MB／60,280 欄來源呼叫舊重用函式約 **533 ms**，主要是每輪重讀並建構整個 JSON 物件，只為取 `len(rows)`。現在每次正式欄位清冊原子發布後寫一份小型私有重用收據，記來源 `device/inode/size/mtime/ctime`、SHA-256 與實際欄數。重用輪先查依賴 mtime，再對來源串流核對完整 SHA-256 與開檔前後身分，只返回欄數；收據遺失、損毀、來源改動或依賴更新均回退完整 JSON 解析／重建，沒有把欄位缺漏視為零。自然排程已在 06:15 產生新收據；同來源直接重測重用路徑約 **23.8 ms**，不是整個 worker 加速量。資料監控／公開閘道相鄰 **163 個測試通過**，含收據損壞、來源替換、非法 JSON 與依賴更新；Ruff、diff whitespace 通過。活躍加密貨幣下載仍使每輪有數十至上千個 footer 更新，正式 `feature_reused=false` 時這條快路徑不會命中，不能宣稱每個 30 秒輪次都會省 0.5 秒。

後續自然排程的真正重用輪 06:16:43／06:18:44 正式收據分別顯示 `refreshed_files=0`、`feature_reused=true`、**60,280 欄不變**，`feature_projection` **20.478／21.297 ms**，整輪仍約 2.046／1.988 秒，主要留在 55,873 檔 inventory 檢查與公開投影，故不能把單階段節省冒充總輪降至 20 ms。私有重用收據後續另加入覆蓋欄數與來源雜湊的自身 SHA-256，以防欄數位元損壞但來源雜湊仍正確時報錯數；v2 收據已於 06:19 由正式排程產生，06:22:15／06:22:45 兩次自然重用輪同樣保留 **60,280 欄**，`feature_projection` **45.975／33.990 ms**，整輪 **2.591／2.193 秒**。凍結來源剖析顯示 `build_feature_inventory` 的 60,280 欄中 **53,966 欄**屬 FinLab 已下載寬表，且真正來源變動時仍需約 1～1.7 秒重建；下一階段考慮按資料集分區的精確投影，但不能用全域舊快取掩蓋其他來源的新 footer。

06:24 唯讀剖析全資料公開投影的首次呼叫約 **3.225 秒**（含其自行讀取庫存），最大子階段是冷 release 清冊重新驗證 **1.639 秒**，Shioaji 公開狀態約 **0.514 秒**，單獨讀取現有記錄庫存約 **0.347 秒**。正式 30 秒 worker 已傳入同輪庫存並使用有界、按 release 身分／每小時再驗證的冷清冊收據，因此不能將這個孤立首次呼叫直接套作正式每輪耗時。後續任何縮短都須保持冷清冊 SHA、head／manifest 身分及來源健康分級，不能只看程序的 `active`。

## 9/24 06:27～06:30 永豐監控讀檔與全服務續測

`build_shioaji_public_status` 的唯讀剖析顯示一次涉及約 1,091 份本地 JSON 收據；既有讀檔快取每份先 `Path.resolve()`，合計約 **194 ms** 花在重複解析實體路徑。改成以絕對路徑作快取鍵，仍每次核對 device／inode／size／mtime，另增 ctime、讀後再次核對；連結目標替換或同大小改寫會重讀，讀取中途來源變動則回報不可用，不把舊版當作當下狀態。修改後在同一程序內每次清空讀檔快取、連續五次真實本機監控投影為 **439、290、283、283、319 ms**，中位約 **290 ms**；修改前只有單次剖析 577 ms，負載不同，不能宣稱嚴格 A/B 的倍數。永豐／測速／資料監控相鄰 **92 個測試通過**，Ruff 和 diff whitespace 通過；公開閘道已單獨重啟載入新版，`/healthz`、永豐、全資料及當沖 status 本機 HTTP 皆 200。永豐仍 `waiting`、全資料仍 `critical`、當沖仍 `degraded`，沒有重啟券商連線或交易服務。

06:30 再次唯讀盤點仍為 **50 service／39 timer／2 path**、沒有 systemd failed unit；收據 `artifacts/benchmarks/service-coverage-20260924T0630.json`。五秒取樣中 OpenBB L1 compaction 約 **0.627 CPU core**，宿主 I/O full PSI10 約 **7.79%**，代表當時真有 I/O 競爭，不能以空閒快取測速推論盤中延遲。Windows Caddy Task Scheduler 的 `LastTaskResult=0x800710E0` 與 Caddy 程序同時存在；這個結果不能單獨證明冷開機復原成功，仍需真正重開機後驗收。重啟公開閘道後 DDNS IPv4 HTTPS `/healthz` **200／約 37 ms**，IPv6 同路徑仍約 **3.32 秒後連線失敗**；雙棧未完成。跨日訊號／事件首次約 1.6 秒且 10 萬筆上限未解，仍是下一個架構性改善目標，現有日索引不足以直接宣稱完整歷史查詢。

06:35 公開閘道載入新版後再次遍歷 **39 條路由、每條 2 次穩態請求，HTTP 錯誤 0**；逐路由原始分段測速見 `artifacts/benchmarks/all_services_latency_2026-09-24_0635.json`。高 I/O 競爭下跨日訊號／事件首個觀測分別 **2,694／2,650 ms**，欄位清冊 **1,252 ms**；各自後續小樣本穩態中位約 **6.55／4.36／131.74 ms**。這些 first-observed 不是清除 OS／應用快取後的 canonical 冷重建，也不能從兩筆樣本報真 p95。跨日查詢的精確總數及可訪問歷史仍受 10 萬筆限制，不能以熱命中掩蓋。

## 9/24 跨日訊號投影分片可行性

正式帳本唯讀盤點：`signals.jsonl` **5.096 GiB／145 個交易日**，每日日索引段中位約 **36.02 MiB**；抽樣三天的既有欄式投影各有 **13,727 行**，逐日讀取約 **50～120 ms**（首天受 OS cache 影響）。其中 9/23 的 31.18 MiB 原始段轉成欄式表後約 4.05 MiB，zstd Parquet **0.118 MiB**，隔離寫／讀約 **7.39／6.66 ms**，讀回逐值相等。這支持「依交易日 immutable 投影分片＋當日增量」方向，但目前**沒有**把一次實驗的臨時 Parquet 冒充正式持久索引，也沒有取消 10 萬筆 API 上限；需要先實作來源段位移／inode／欄位 ABI 驗證、原子發布、歷史精確總數與篩選語意、異常回退，再做新舊輸出逐欄比較與公網重測。

六組相鄰回歸（資料監控庫存、資料監控投影、公開閘道、永豐監控、測速器、Yahoo 狀態）合併執行 **267 passed／21.41 秒**，沒有把各次有重疊的 163、92、64 個測試直接加總成測試數。

## 9/24 06:43～07:00 跨日事件完整查詢與投影快取

先落地事件帳本（`orders.jsonl`、`fills.jsonl`）的**逐交易日精確分片**：只從既有 append-only ledger 的日期位元組索引讀取，將 API 所需欄位投影為可重建的私有 zstd Parquet；每片記來源 dev／inode、位元組 spans、觀測 size／mtime 與 Parquet SHA-256，原子發布。後續追加其它交易日不需重新讀舊日；同日追加、原檔替換、快取損毀則重建／回退帳本。多個日期按原始位元組順序合併；不連續日期段與正式 overnight overlay 仍退回原有逐列路徑，避免重排事件。無正式 overlay、無 Unicode symbol 搜尋時，以欄式去重、精確計數及只物化請求頁取代 39 萬次 Python 逐列轉換。這是衍生索引，不是新的交易帳本或成交證據。

正式 2/25～9/23 事件為來源實掃 **283,927 個 order／107,271 個 fill 行**，去重後 **282,935／107,113 個，合計 390,048**；原先每來源最多 100,000 行的 `scan_limit_reached=true` 不再代表完整資料。以原有無上限嚴格路徑作同時段對照，頁面 100 筆、總數、各事件數、來源掃描數、日期及 `has_more` 全部逐欄相等。低 I/O 優先權的離線新索引首次建置 **8,453 ms**（比原無上限掃描 5,691 ms 慢，沒有冒充冷啟動提速）；建好後逐次清空頁面記憶體快取，完整查詢 **599～844 ms**，對照無上限原路徑約 **4,794～5,691 ms**。145 天兩種事件共 290 個 Parquet 分片，私有 cache 占用約 **9.4 MiB**，原始事件約 0.303 GiB 保持不變。相鄰當沖／overnight 回歸 **157 passed**，涵蓋追加、同大小原檔替換、快取損毀、重複事件、0 數量與分頁；Ruff 與 whitespace 通過。

公開閘道單獨重啟載入新版後，本機首個完整跨日查詢 HTTP 200、服務端 `build` 約 **889 ms**，回應 `total=390048`、`scan_limit_reached=false`，下一次約 **5.5 ms**（頁面快取命中）；公網 IPv4 HTTPS 同路徑 HTTP 200／約 **191 ms**，仍非真 p95。5 次小樣本 route benchmark 中位約 **1.76 ms**，原始收據 `artifacts/benchmarks/event_projection_2026-09-24.json`；不能拿頁面熱快取取代首次 0.9 秒。當沖健康仍 `degraded`、全資料 `critical`、永豐 `waiting`，沒有重啟交易／報價／Discord。**跨日訊號仍維持 100,000 行上限**；其 5.096 GiB 來源需同樣的逐日精確索引，再處理當前訊號摘要與排序的完整歷史語意，不能用事件頁修好就宣稱全部明細完成。

後續檢查發現事件 API 本身仍限制 `offset≤100000`：雖然總數已正確，較深的頁無法查。已讓完整投影路徑把兩種事件在欄式表內排序後直接切出 `offset/limit`，不再把前 39 萬筆轉成 Python 字典；事件路由仍有明確 1,000 萬 offset 上限，訊號路由不變。對 390,048 個真實去重事件，第 0／100,000／200,000／390,040 筆起的頁面與完整來源掃描逐筆相同；欄式路徑約 **1,230／741／624／628 ms**，相同頁的原逐列路徑約 **2,972／8,310／14,757／29,327 ms**（同一輪但順序負載不同，不能宣稱固定倍數）。當沖、overnight、公開閘道 **249 個測試通過**，Ruff、whitespace 通過。此時程式及路由已修改，仍待公開閘道重新載入與實際 HTTP 深頁驗收。

07:05 後公開閘道單獨載入此事件修正，本機 `offset=200000&limit=2` 回應 2 筆、`total=390048`、`scan_limit_reached=false`，IPv4 公網同一路由 HTTP 200／約 **450 ms**。這是實際深頁可達證據；本輪沒有重啟交易、報價或 Discord。

## 9/24 07:06～07:36 跨日訊號完整來源與記憶體邊界

訊號帳本正式來源為 **145 日／1,990,415 行／5.096 GiB**，日期索引所有日期各一個連續 byte span。逐日私有 Parquet 投影在原 ledger 異動或同日追加時按既有收據失效；完整 145 日首次建立新版投影約 **12.41 秒**，不能拿後續熱分片速度冒充原始冷重建。直接把所有欄位拼接、逐筆 Python 計算摘要的原型雖能報真總數，但約 **19.45 秒、3.4 GiB RSS**，沒有部署。改由 Polars 對目前訊號 ID 做完整範圍篩選，以欄式彙總目標／實際多空、開盤價格覆蓋、未成交原因，只排序請求分頁前綴；保留 Python Unicode `casefold` 的標的／名稱搜尋契約，但只對兩個字串欄運算。三交易日實帳本的全部／blocked／單模式／深頁結果及中文搜尋，與原有逐列路徑的頁、總數、方向、開盤稽核逐項相同。沒有以少算來源換取速度。

比對公開 DTO 時發現原投影漏了 `target_unsubmitted_shares`，且省略僅供後端 feature-driver 查詢的 `signal_source_path`，使快路徑未能附上特徵解釋。兩欄已進入私有投影，但長路徑字串只在請求頁逐筆還原，避免完整框架膨脹；公開 sanitizer 仍不對外暴露來源路徑。三日實帳本的公開輸出與原始逐列路徑包括 feature drivers 全部相等；相鄰回歸加入直接來源路徑及未送出股數。完整期 `offset=200000` 的離線新版約 **2.23 秒、2.55 GiB RSS**；這是獨立程序，不是正式端到端 SLA。`offset=1900000` 仍可正確分頁，極深頁的記憶體較高。

公開 gateway 只單獨重啟後，本機 `offset=200000&limit=10` 第一次 HTTP 200／約 **3.89 秒**、回傳 `total=1990415`、`scan_limit_reached=false`、`opening_execution_audit_scope=complete_current_signal_rows_per_mode`，後續不同深頁 `offset=1900000` 約 **1.86 秒**；正式 cgroup `MemoryPeak≈3.44 GiB`，低於 `MemoryHigh=4 GiB`、`MemoryMax=8 GiB`。IPv4 公網相同已熱 key HTTP 200／約 **107 ms**；它只是熱命中，不代表全來源第一次速度。訊號／事件路由各有 1,000 萬 offset 公開上限，不再被 10 萬筆限制截斷；總數大於總行數仍只返回空頁，不會建立假資料。當沖、overnight、公開閘道 **249 個測試通過**、Ruff、whitespace 通過。

重新跑全部 **39 條公開路由各 2 次**，HTTP 錯誤 0，原始收據 `artifacts/benchmarks/all_services_latency_2026-09-24_post_projection.json`。跨 2/25～9/24 的訊號首次觀測約 **1,749 ms**、事件約 **693 ms**；同 key 熱回應約 **7／3 ms**。此二樣本不可當 p95、也未清 OS cache。全資料欄位清冊本次首次約 **560 ms**，包含大型 payload 的網路與解析成本；完整訊號首次多秒、同時多種深頁的記憶體壓力、正式 overnight overlay、外部 IPv6、Windows 冷開機 Caddy、Crypto `deferred_source_changed` 與各服務業務來源健康仍未因這項投影修復而完成。

## 9/24 07:37～07:45 全資料搜尋互動熱點

`/data-monitor/api/features` 為 **60,280 欄／約 37.36 MB 解壓後 JSON**。前端原來每輸入一字便對每列最多五個字串反覆呼叫 `toLocaleLowerCase("zh-Hant")`；真正瓶頸在這個同步 CPU 迴圈，不是初始桌面只渲染 80 列的 DOM 數量。保留同樣的欄位包含搜尋與來源／分類條件，改用預設 Unicode `toLowerCase()`；新版本由 `app.js?v=35` 讓既有瀏覽器更新腳本。可重跑 `node scripts/benchmark_data_monitor_feature_search.mjs`，正式本機清冊五個查詢 `t/tw/twpub/price/台股` 的結果索引完全相同，所有五欄 × 60,280 列的 locale 與預設小寫值差異 **0**；舊／新搜尋 CPU 約 **3,132／112 ms**，但這是 Node 隔離樣本、不可當瀏覽器端到端倍數。收據 `artifacts/benchmarks/feature_search_2026-09-24.json` 保留 JSON 解析與搜尋分段；搜尋以外的後端清冊建置和 37 MB 解壓並未因而縮短。

公網真 Chromium 1366×768 的 `data-monitor` 首頁無 JS 錯誤、全頁橫向溢出 0；功能清冊開啟到可用約 **1,429 ms**，其中 API Resource Timing 約 **868 ms**，五次輸入到兩個 `requestAnimationFrame` 的可繪畫機會約 **25～37 ms**，原始收據 `artifacts/benchmarks/feature_search_browser_2026-09-24.json`。390×844 手機版同頁無 console error、無全頁橫向溢出、無自動觸控目標風險；筆電版密集桌面控制仍有 10 個自動觸控尺寸警示，不等於筆電滑鼠不能使用。兩份畫面收據在 `artifacts/benchmarks/public-feature-20260924/`。這是單次實際瀏覽器證據，不是公網 p95。共享導覽的 Node 回歸原先仍期待 8 頁，與已上線的 FinLab 第九頁不符；測試契約已更新為 9 頁並重跑 **16/16 通過**，資料監控 JS 語法檢查通過。

八頁公網總驗收也已重跑：`artifacts/benchmarks/public-all-pages-20260924/report-1366x768.json` 與 `report-390x844.json` 分別含 8／8 頁，兩種視窗所有頁面皆無全頁橫向溢出、無 console error；手機自動觸控尺寸警示為 0，筆電密集控制合計 110 個警示，仍需依實際滑鼠／觸控裝置權衡。當沖完整分鐘曲線在該筆電樣本選取到顯示約 **1.92 秒**，服務端 build 約 1.05 秒、傳輸 body 約 372 ms、JSON 解析約 63 ms、圖表 prepare 約 80 ms；它是新量到的多階段長尾，不能以先前本機熱命中 98 ms 取代公網首次體驗。

另修正訊號頁的一個快取依賴：原帳本頁快取會把頁面的模型 feature drivers 一起凍住，即使 `summary.json` 後來更新也不變。現在只對同一頁涉及的 feature 來源做最多每秒一次核對；帳本／state 異動仍由既有 cache key 即時失效。單一 summary 即使同大小並把 mtime 設回原值，ctime 異動也能重讀；缺檔不沿用舊特徵說明。特徵摘要快取限制 256 項、每項 JSON 至多 256 KiB，不讓歷史瀏覽無限保留解釋物件。真實完整歷史頁第一次本機 HTTP 約 **1.98 秒**；同 key 超過一秒後第一次重新核對來源約 **82 ms**，緊接四次約 **2.7／1.5／1.7／1.5 ms**。這是「來源正確性」和「熱請求延遲」之間的明確折衷，特徵檔單獨更新最多延後一秒可見。再跑當沖／隔日沖／公開 **249 個 Python 測試**及共享前端 **16 個 Node 測試**皆通過，Ruff、diff whitespace 通過，公開閘道單獨重啟驗收 HTTP 200；未觸動交易／報價服務。

07:45 的全服務收據 `artifacts/benchmarks/service-coverage-20260924T0745.json` 再次確認 **50 service／39 timer／2 path**。`stockagent-finlab-local-refresh.service` 的上次程序 systemd `Result=success` 表面欄與實際 `ExecMainStatus=15` 不同，監控正確標 `nonzero_exit`；該次於 9/23 20:14 起、21:10 被 TERM，停在供應商 `broker_transactions` 呼叫後，不能算資料完整。其後加入的逐 key 600 秒 timeout 程式尚待新的自然批次驗證，不宜在 08:20～09:10 受保護開盤窗口中人工加開供應商工作。07:45 取樣的 OpenBB L1 compaction 與註冊盤中資料確有 CPU／I/O 工作，不應當作無效忙等刪除。

另以 2/25、2/26、4/13、5/26、7/08、8/20、9/23 七個相隔交易日對照 `status=blocked` 的**公開**訊號輸出；逐日投影與逐列原始來源的總數、頁面、方向與 feature-driver 輸出皆完整相等。單日兩條路徑約 0.35～0.72 秒／0.03～0.43 秒，不能宣稱逐日分片一定比直接讀源快；完整跨期的勝出來自避免一再解碼整個 5.1 GiB 帳本。最後一次 OpenBB L1 compaction 07:47 正常完成，該批 18 個新 segment、0 stale／failed、約 **6 分 33 秒** wall、2.5 GiB memory peak；仍有約 398 萬 pending 檔，這是另一個尚未完成的吞吐成本，不因程序退出 0 就宣稱資料已全壓縮。

該次 OpenBB 正式 `stage_seconds` 進一步顯示 `stale_source_contract_query` **194.537 秒**、`stale_derivative_metadata_scan` **56.849 秒**、未指派來源查詢 **72.783 秒**、狀態 task count **52.327 秒**、建新段僅 **3.594 秒**、view 發布 **4.985 秒**。前次唯讀 SQL 樣本約 19 秒與這次正式 194 秒差距很大，須在相同負載／查詢計畫下重測索引與 I/O；不能直接把它解讀為演算法退化，更不能盲目提高並發或跳過來源稽核。07:45 覆蓋表中 33 service 有最近完成程序 wall、15 個常駐服務有資源取樣、2 個停用／未執行服務無可比工作 wall；**並非 50 項都已有功能級 p95**。

只讀 SQLite `EXPLAIN QUERY PLAN` 確認 stale 查詢目前按 segment 主鍵掃描、用 `idx_l1_members_segment` 查每段成員、再依 `tasks` 主鍵逐筆查約數百萬來源任務，另需 `DISTINCT` 暫存 B-tree；`sqlite_stat1` 目前不存在。這解釋為什麼「沒有 stale 輸出」仍有全量核對成本，但**不**單獨證明 194 秒全是 SQLite CPU 或加索引即可解決；需在市場保護窗口外，用相同 DB 快照比較 I/O、查詢計畫、索引大小與寫入影響後才能改 manifest。此輪未執行 `ANALYZE`、建索引、變更 compactor 語意或中斷 OpenBB archive。

08:02 再核對公開路徑：IPv4 HTTPS `/healthz` 為 HTTP 200／約 **51 ms**，IPv6 同 URL 約 3.1 秒後連線失敗。當沖 `degraded`、隔日沖 `waiting`、永豐 `waiting`、全資料 `critical` 仍按真實來源狀態顯示；這些不是前端效能修正可以改寫成正常。Windows Caddy 冷重啟任務的前次結果仍需真重開機驗證，不能以當下 IPv4 可達推論自啟動或雙棧完成。

## 9/24 08:11～08:16 開盤前全服務取樣與欄位搜尋

開盤前唯讀收據 `artifacts/benchmarks/service-coverage-20260924T0811.json` 再列出 50 service／39 timer／2 path。5 秒程序資源樣本中，當時註冊盤中資料約 1.74 CPU cores、公開 gateway 約 0.015；盤中資料工作隨後在 08:11:42 完成該輪，systemd 記錄約 69 秒 wall／114 秒 CPU／2.3 GiB 峰值。這些是單次時窗，不是每筆外部資料的延遲。最近已結束工作中，FinLab 的 3,340.7 秒仍是 exit 15 的失敗，不因 systemd `Result=success` 當作有效完成；後加的每個 key 600 秒期限尚待下一次自然執行驗收。開盤保護時段前未重啟任何交易、報價或資料採集程序。

全資料欄位清冊 60,280 列在每次按鍵重複建立五欄小寫字串。現在只在取得新清冊時建立與原始列一一對應的 NUL 分隔搜尋索引；分類／來源篩選仍先判定，包含 NUL 的特殊查詢退回原逐欄語意。原始欄位和搜尋結果沒有被裁剪；每次新清冊刷新都一起替換列與索引。可重跑 `node scripts/benchmark_data_monitor_feature_search.mjs`：這次同一份約 37.36 MB 解壓清冊五個查詢，既有預設小寫法約 140.94 ms，新索引建置約 52.26 ms、五次搜尋合計約 10.84 ms，結果索引逐筆相等。這是隔離 Node CPU 數字，不代表整頁提速 13 倍；首次取得清冊須付一次建索引成本。

新 `app.js?v=36` 已由本機公開 gateway 和 IPv4 HTTPS 供應。真 Chromium 公網重測 `artifacts/benchmarks/feature_search_browser_index_public_2026-09-24.json`：60,280／60,280 列具索引、五次輸入同步處理約 5～8 ms、實際前端搜尋與逐欄原始結果連特殊 NUL 查詢都逐筆一致，無 JavaScript 例外或全頁水平溢出。不同單次樣本的清冊開啟約 1.35～4.98 秒，公網 API Resource Timing 約 0.90～4.69 秒；後一輪明顯長尾，故不能宣稱首次載入已獲穩定改善。相鄰 `curl` 重測同一壓縮 API，本機約 23 ms、公網 IPv4 約 77 ms，表示瀏覽器長尾需另分解主執行緒、解壓與請求排程；也不能直接推論是伺服器慢。繪畫機會受 60 Hz frame 限制約 10～34 ms，不把它解讀成同步搜尋時間。共享前端 16 個 Node 測試、JS 語法與 `git diff --check` 通過。這輪只修可隔離的互動熱點，未宣稱全部 50 項服務的功能級延遲已到極限。

## 9/24 08:18～08:29 OpenBB 壓縮與開盤資源保護

現場發現 `stockagent-openbb-l1-compaction.timer` 在 08:18:21 啟動低優先權壓縮；原 unit 雖設定 `CPUWeight=10`、`IOWeight=10`、40 分鐘上限，卻沒有開盤時間閘門，因此可一路運行到約 08:58。08:28 前程序仍只開啟 SQLite manifest／WAL／鎖、尚未開啟衍生輸出檔，cgroup 約 2.3～2.5 GiB，宿主 memory full PSI 10 秒均值約 15.72%、I/O full 約 8.64%。這不是把壅塞全部歸因於單一服務，但在開盤保護時段不應讓可重試的壓縮與行情、訊號競爭。

共用 `check_outside_tw_opening_resource_window.py` 新增可選 `--minimum-runway-minutes`，預設 0 保持其他服務原語意；OpenBB unit 專用 45 分鐘提前閘門，比其合法 40 分鐘最長執行時間多 5 分鐘緩衝。平日 07:35～09:10 不再允許**新啟動**，不縮減正式來源稽核、來源數或 L1 輸出。原輪已在新版條件安裝前開始；核對其開啟檔案僅有 DB／鎖後，於 08:28 有意停止這項可重試維護作業，未停 OpenBB archive、當沖引擎或公開 gateway。隨後以 `systemctl start` 唯讀驗證啟動條件：`Result=exec-condition`、exit 1、`ExecMainPID=0`；當時沒有重新啟動壓縮。最新完整 L1 收據仍是 07:47:18 版本，沒有把中斷輪次當成功。壓力再採樣的 memory full／I/O full 10 秒均值約 0.34%／0.04%，這是相鄰觀測而非單因素因果證明。

正式 unit 已 `daemon-reload` 載入新閘門；下一個 08:59 timer 若觸發，應被條件略過，須再現場驗證，09:10 後亦須驗證下一輪能續跑。19 個 guardian／開盤閘門測試、Ruff、whitespace 通過；目前沒有對 16 GiB DB 做會搶占開盤 I/O 的完整一致性掃描，也不能以原始收據未改動單獨證明所有私有暫存物件都已清理。

## 9/24 08:30～08:43 開盤前重工來源與 OpenBB 保序索引實驗

再查 timer 與正式收據，08:00 的官方資料驗收首次運行 **9 分 34.6 秒、63.1 GiB memory peak**，exit 1；其衍生資料由 stale 經重建後已 current，但最後仍因 publication watcher receipt 尚未 current 而失敗，08:10 下一輪才完成。首次主要步驟是官方 symbol panel 約 93 秒、公開 feature panel 約 160 秒、嚴格模型稽核約 291 秒；這些會處理真正變動的來源，不能透過假收據或跳過稽核冒充加速。07:50 官方 sweep 確有 `twse_institutional_trades` 位元變更，並於 07:50:48 完成。08:20 的獨立正式 feature reconcile 隨後重建 9,604,259 列，**3 分 49 秒 wall、48 GiB memory peak**；它不是 OpenBB 壓縮造成的同一項工作，也不能因守住 OpenBB 就宣稱開盤資源競爭全消失。未新增會與 08:00 嚴格稽核重疊的 08:00 feature timer。

永豐股票分鐘、通用歷史及 TX 歷史服務在 08:35 仍顯示 `active`，但其主行程為 Bash，子程序僅 tee／sleep，沒有歷史查詢 Python 子行程；TX 日誌記錄 07:45 後進入 live-priority wait。這一瞬間的 process 證據不等於整日資料回補完成，也不證明即時 Tick／BidAsk 健康；保留券商登入與資料收據的獨立稽核。

OpenBB L1 每輪未指派來源查詢的約 72.8 秒中，約 58.8 秒為第一個 endpoint 按 `task_id` 排序，既有 index 只到 `(active, plan_token, status, endpoint)`；直接用 task 主鍵掃描，在正式 DB 的 1,000 筆有界唯讀嘗試超過 2 秒，不能替代全局排序。新增**僅在索引存在時**才啟用的保序查詢路徑，候選部分覆蓋索引為 `(plan_token, endpoint, task_id, rows) WHERE active=1 AND status='success'`：同一 global `endpoint, task_id` 順序、不刪來源、不改 segment 資料契約；狀態 `COUNT/SUM(rows)` 也可由 covering index 執行。隔離 fixture 的結果與既有路徑逐筆相等、`EXPLAIN` 無 temp B-tree，完整 OpenBB L1 測試 **15 個通過**。正式 **16 GiB manifest 尚未建立此索引**，所以目前不宣稱 72.8 或 52.3 秒已縮短；需要在非開盤時段量索引建置 wall／大小／archive 寫入影響、正式收據與跨輪計畫一致後才部署。

## 9/24 Yahoo 每標的修復期限的真實邊界

每日 Yahoo repair 原本 `as_completed(futures)` 後才對**已完成** future 呼叫 `result(timeout=90)`；這個 timeout 幾乎不可能觸發，不能當作外部長尾已受控。現在單標的 worker 自開始執行起建立 monotonic 時間預算，網路包裝等待不超過剩餘期限；重試、限速排隊後與 Parquet 原子提交前都再次檢查。期限已過則回報原有 `failed`／`repair timed out` 形式，不再嘗試下一個請求或寫入來源。`0` 仍不限制。刪除已無效的完成後 future timeout／handler。更底層原先自稱 daemon 的 `ThreadPoolExecutor(wait=False)` 實際仍會在 Python 行程結束時 join 未歸還的工作；已改為有 **64 個在途上限**的 daemon 工作與包含准入等待的 monotonic 期限。這不是改 Yahoo 請求節拍；正常設定的 16 個標的 worker 不會因 64 上限等待，供應商全面掛起時才避免無限制累積背景線程。隔離子程序中讓背景函數等 30 秒、呼叫端期限 0.02 秒，含 Python 載入的程序 wall 約 **0.51 秒**而非被背景線程拖住 30 秒。**這不是強制中止**：已送出的供應商呼叫無法被 Python 線程取消，可能在背景短暫持續消耗連線／配額；已進入不可中斷的原子寫入也可能超時。不能把設定解讀為整個批次最長 90 秒，也不能拿失敗作完整來源。64 個 Yahoo 狀態／修復測試通過，含慢來源晚歸不得寫檔、預算傳入 worker、遺留 daemon 及並發閘門；Ruff、diff whitespace 通過。**06:30 已啟動的 `registered-data-daily` 行程不會因改檔自動載入新版，也未為此中斷它**；須等後續新行程的真實 US repair 收據證明長尾下降及失敗率不升，本輪未啟動額外 Yahoo 請求。

## 9/24 10:42～10:48 當沖事件與儲存掃描避讓

新唯讀全服務收據 `artifacts/benchmarks/service-coverage-20260924T024231Z.json` 仍列出 **50 service／39 timer／2 path**，當時沒有 failed unit；這只是程序層，不代表交易或資料健康。五個模式今日都已持久寫入 09:00 訊號。開盤測速首個模式的 Snapshot 往返 2,170.373 ms，最後模式因序列排隊約 3,383.444 ms，09:00:03.650 才完成訊號；因此一秒目標尚未達成，也不能用後續紙上補成交冒充 09:00 執行。盤中改用有 `simtrade` 與交易所事件時間的 Shioaji Quote 推送後，10:34 的每分鐘 `quote_fetch_ms` 為 5.419 ms；292 個未平標的超過 200 筆同時訂閱限制，該分鐘僅 133 檔在 10 秒內有事件。完整故障及證據邊界見 `docs/tw_day_trade_live_quote_recovery_2026-09-24.md`。

儲存清冊服務每次 `scandir` 約 452 萬檔，最近一次 10:09 啟動的服務壁鐘約 100.7 秒；它提供容量與近 30 日 mtime 統計，盤中每小時重掃既不能縮短當沖訊號，也會和即時工作搶 CPU／I/O。將 timer 改為台北時間 00～07、14～23 時的每小時 09 分；取消開機 15 分鐘強制掃描與漏跑補執行，避免在 08:00～13:59 啟動。儲存網站仍讀最近的完整快照，盤中數字會延後到 14:09 才刷新，不能把快照當即時磁碟用量。只重載並重啟 timer，**未重啟清冊工作、當沖、報價或 Discord**；`systemd-analyze verify` 通過，下一次觸發顯示 14:09。開機契約／儲存快照相鄰 11 個測試通過。14:09 的正式壁鐘與盤中資源干擾改善仍待自然排程驗收，不能把避免盤中掃描說成掃描本身加速。

同一盤中檢查發現 Bybit 加密訓練刷新 10:30 自動啟動，雖正式收據為 `completed`、壁鐘 94.4 秒，但累計約 269 CPU 秒、記憶體峰值約 21.3 GiB；它不是 09:00 當沖的輸入。停用 10:30 timer 觸發與開機漏跑立即補執行，保留 02:30、16:30、22:30 的完整原工作及發行稽核。已重載 systemd 並只重啟 timer，下一次顯示 16:30；10:30 的**已完成結果不抹除**，若 02:30 後真的失敗，盤中不再重試，會等 16:30，資料健康應保持未完成。這是交易時段資源避讓，不宣稱 Bybit 本身 94 秒耗時縮短或整體市場延遲已降低；需下個開盤測量訊號與系統壓力才可歸因。

OpenBB L1 壓縮也在 10:40:58 盤中執行，約 147 秒壁鐘、111 CPU 秒、2.5 GiB 記憶體峰值；既有 07:35～09:10 開盤閘門不足以保護持續中的當沖。只對該可重試壓縮 unit 延長 `ExecCondition` 到平日 13:35，保留 45 分鐘開盤前 runway、原壓縮與收據語意；其它使用共用腳本的服務仍沿用原 09:10 截止。10:57 現場試啟動的條件回傳 `allowed=false`／`Result=exec-condition`／`ExecMainPID=0`，確認沒有啟動壓縮；原本 10:40 完成的正式結果保留。此修正保護盤中資源，不縮短壓縮本身執行時間；下個 13:35 後自然輪次還須驗收。

## 9/24 11:07～11:26 盤中實際健康與 FinLab 早失敗閘門

正式當沖單元名稱是 `stockagent-tw-day-trade-simulation.service`，11:16 為 `active/running`，不是不存在的 `stockagent-tw-day-trade.service`。本機公開 `/tw-day-trade/api/status` 為 `degraded`：五模式今日訊號齊，但 100m 與 GELU 模式尚有未達標模擬成交；`opening_gate` 仍為 `failed`，其中 09:00 後保留的 10:00 驗收也顯示 final-arm 未就緒。今早 08:54 的門檻檔曾證實五個 final-arm ready，但 08:56 後都消失；原因需結合 Discord 程序當時版本及後續 09:51 重啟再驗證，不以訊號已出反推開盤驗收通過。09:00 最慢不可變訊號為 **3,650.179 ms**，首次 Shioaji 開盤價覆蓋為 **2,514 ms**，一秒目標未達。11:16 股票即時訂閱約 200／291，當分鐘有可用事件約 91／291，不能把無有效非試撮行情的未成交股數補寫成券商成交。

全資料狀態快照每 30 秒重算：FinLab 正在連續新增 Parquet 時，清冊約 2.3～3.6 秒、公開投影約 0.9～1.3 秒、欄位投影約 1.9～3.0 秒，約 6～7 個新檔迫使欄位清冊實際更新。FinLab 停止後 11:23 自然輪次 `refreshed_files=0`、`feature_reused=true`，總壁鐘 **2,711.031 ms**，其中清冊 **1,554.299 ms**、公開投影 **973.741 ms**、欄位計數重用 **30.264 ms**。不能將活動中真變動誤判成「無用重建」而直接省略；仍需在不漏來源的條件下研究增量 per-dataset 投影。大型欄位 API 的一筆本機 gzip 首位元組約 **628 ms**、總 **636 ms**、壓縮下載 **1.16 MB**；單次樣本不代表 p95。

`stockagent-finlab-local-refresh.service` 11:12 失敗：先運作 **36 分 7 秒**、耗 **13 分 37 秒 CPU**，最後 Bash 回報腳本 `done` 附近語法錯誤，完整資料仍為 partial。現有腳本 `bash -n` 已通過；為避免再次讓這類可在啟動前發現的錯誤耗盡長批次，FinLab installer 現在安裝前語法檢查，正式 service 加上 `ExecStartPre=/usr/bin/bash -n ...`，已以 `finlab-only` 安裝、`systemd-analyze verify` 及已載入 unit 驗證。未重跑 FinLab、未抹除失敗；自然下一輪 16:10 的實際資料收據才是恢復證據。這道閘門不能防止執行中腳本被原地改寫，也不能證明供應商下載成功；須另處理穩定部署來源與長工作不中途變更的操作契約。

## 9/24 11:39～11:43 欄位 API 首次請求驗證成本

正式 `feature_inventory.json` 約 **49.20 MB／79,299 欄**；隔離 Python 同一檔 JSON 解析約 **733 ms**、gzip level 3 約 **161 ms**、SHA-256 約 **26 ms**。先前公開閘道即使讀取已完成的 producer 快照，第一次訪客仍重做完整解析，造成單次本機 gzip HTTP 首位元組 **628 ms**、總 **636 ms**。現在共用 `data_monitor_feature_receipt` 契約：producer 僅在唯讀 DTO、無非有限 JSON 及原子檔完成後發布同所有者、不可由群組／他人寫入的 SHA-256 收據；閘道逐位元組與檔案指紋核對後免重解析。缺收據、修改來源、或收據不符都回原來的完整 JSON 驗證路徑，不把快取當認證或業務資料健康證明。這是可信本機 producer 與唯讀 gateway 間的完整性優化；具有 root 寫入權者仍不在此收據的防護範圍。

相關 164 個 Python 測試、Ruff 通過；另將資料監控靜態頁版本斷言與現行 `app.js?v=36` 對齊。只單獨重啟唯讀公開閘道，當沖、Shioaji、Discord 均未重啟。第一次本機 gzip HTTP 200 首位元組 **182 ms**、總 **185 ms**、下載 **1,156,959 bytes**，同 key 熱命中約 **1.9／5.3 ms**；回應解析仍為 79,299 欄、`read_only=true`、`production_control_possible=false`、摘要狀態 `partial`，沒有冒充完整。這是兩筆相鄰樣本，不能當 p95 或瀏覽器載入／搜尋端到端改進；資料來源更新時仍需真實重建欄位快照。

安全複核後收據升為 **schema v3**：只在 producer 以 `allow_nan=false` 序列化並驗證唯讀 DTO 後才簽出；舊 v2 收據回完整解析，不可免除非有限 JSON 檢查。164 個相關 Python 測試再次通過，包含篡改來源／缺收據回退。再次只重啟公開閘道後，v3 冷 HTTP 樣本首位元組 **517 ms**、總 **547 ms**，當時的主機／檔案快取負載不同；同檔三次隔離拆解的讀取／收據 SHA／gzip／回應 SHA 約 **30／24～29／91～103／24 ms**。因此不能把前述 182 ms 當穩定 SLA，需累積多個自然換版的分位數；所有樣本仍保留完整 79,299 欄，沒有縮減內容。

公開 DDNS 的 IPv4 HTTPS 實際 `/healthz`、當沖 `/api/status` 及全資料欄位 API 均回 HTTP 200；其中一筆欄位 API 從本機經 DDNS 約 **738 ms**，連線階段約 **447 ms**，不可把此網路變動誤算為純應用建置。相同網域強制 IPv6 `/healthz` 於 2 秒連線逾時／HTTP 000，雙棧仍未修復。當沖正式 API 仍顯示 `degraded`，公開可達不等於交易或資料健康。

## 9/24 12:00～12:12 回收後的一次性服務測速證據

`systemctl show` 會在一次性 unit 被回收後清空 `ExecMain*` 時戳；原先的 22 項 `not_measured` 有相當部分其實已執行，不能把空欄解讀為未執行或零秒。`scripts/audit_service_latency_coverage.py` 現在只對閒置且缺目前時戳的 unit，讀取最近 14 日 systemd PID 1 的結構化 journal，按同一 boot ID／invocation ID 配對開始與終止事件計算程序壁鐘，並標註 `last_attempt_source=systemd_journal`。短工作可能沒有資源統計事件，因此成功／失敗終止事件亦可作結束時點；不能用更舊的失敗代替較新的短暫成功。未配對、無明確結果或 journal 遺失則保持未知。正式業務收據仍須與這次啟動時間相符，不能拿新舊不同工作的 `latest.json` 塗綠。

最新收據 `artifacts/benchmarks/service-coverage-20260924T1225-final.json` 保留 50 service／39 timer／2 path：**36 項有最近一次已完成程序 wall、13 項只有常駐資源取樣、1 項無近期可配對執行**（已退休且停用的 `stockagent-hot-artifact-sync.service`）。這不是「49 項功能延遲已最佳化」：程序壁鐘包含等待，業務資料須看正式收據，daemon 的 CPU 取樣也不是單次請求的 p95。相鄰測速／FinLab／開盤回歸最後合計 247 個測試通過。

配對後發現 9/20～9/21 的 `registered-data-backfill` 跑 **65,724.113 秒**後失敗，該輪 Binance 574 標的有 1 檔未完成；9/24 的日常 tail 收據另有 574／574 `updated`，但不證明整個歷史特徵回補已完成。`tw-public-cold-publish` 的**最近** systemd 呼叫其實在 9/24 00:00:48～00:00:49 **0.569 秒成功退出**，但資料工作為 `deferred/canonical_refresh_lock_busy`，沒有發布 release。前一版 journal 回收邏輯只取有資源統計的事件，誤把 9/23 23:55 的 19 秒失敗當最近一次；現已納入短工作終止事件修正。冷發布 `latest.json` 已被 08:03 另一條流程覆寫，不屬於午夜 systemd 呼叫；進一步按啟動時間配對其不可變 `runs/20260924T000048721178.json`，正式收據確認 `deferred`。下一次 23:50 timer 和來源完整性稽核仍需驗收。

## 9/24 12:16 FinLab 長工作版本固定

11:12 的 FinLab 腳本在已下載 581／1,109 個目錄鍵後才因 `done` 語法錯誤中止；僅在 `ExecStartPre` 對**當時**的來源做 `bash -n`，仍無法防止長 Bash 工作在執行中逐段讀到後來修改的檔案。正式 unit 現改由 `run_finlab_refresh_frozen.sh` 啟動：建立獨立暫存快照、比對來源位元、對**同一份快照**做 `bash -n`，再執行該快照；原腳本透過固定的 repo root 環境變數保持既有相對路徑與資料語意。installer 同時語法檢查兩份腳本；`finlab-only` 已重裝 unit，`systemd-analyze verify` 通過，下一次 timer 是 **16:10**。沒有重啟既有失敗工作、沒有盤中重新下載，也沒有清除失敗證據。26 個 FinLab／測速相關測試通過；真正完成度、配額與資料品質仍須 16:10 的正式收據驗證。此保護是腳本版本一致性，不是 FinLab SDK 或來源可用性的保證。

## 9/24 12:33～12:40 長期測速與快照熱點再核對

每五分鐘的正式 `all_service_runtime_sample` 現在也對 systemd 已回收的 oneshot 補查同次 journal，保留啟動 ID、boot ID、退出結果與程序壁鐘，再按該次啟動時間尋找業務收據；這使近期執行不再在長期趨勢裡變成空白。12:33:40 自然樣本包含 **50 個服務**，補查的 22 項 journal 約 **477 ms**（隔離呼叫），正式整個 service-trend 階段 **744.896 ms**，只在五分鐘週期發生、不是每 30 秒必跑。當中 `registered-data-backfill` 最近一次 65,724.113 秒／失敗取自 journal；`tw-public-cold-publish` 0.569 秒／程序退出 0、同次不可變收據 `deferred/canonical_refresh_lock_busy`，沒有把退出 0 誤報為來源已發布。67 個相鄰監控／公開狀態測試、Ruff 與 whitespace 通過。

每 30 秒全資料快照的 12:32～12:35 自然樣本總壁鐘約 **2.39～2.63 秒**：來源清冊約 **1.24～1.37 秒**、公開投影約 **0.93～1.11 秒**；沒有趨勢取樣的輪次該階段約 1 ms。用正式冷清冊快取環境進行本機剖析，公開投影約 1.43 秒的 cProfile 樣本中，永豐本機 JSON 收據讀取共 1,097 次、且路徑**全部不同**；它們不是同一收據反覆讀取，盲目加函式內 memoization 不會縮短一次性快照。最大單檔為當沖 `state.json` 約 8.9 MB，讀取／解析的一次樣本約 122 ms；即時監控未登入永豐、未新增行情訂閱。來源清冊的剖析樣本約 1.91 秒，其中檔案枚舉／排序約 0.80 秒、兩份 JSON 解碼約 0.38 秒、約 7.9 萬次檔案 stat 約 0.31 秒；它須核對約 5.6 萬份已登錄檔案的變動。這些是 CPU 剖析下的不同樣本，不能相加成正式延遲，也不能透過略過鮮度檢查或縮減來源宣稱最佳化。下一步應針對檔案發現與大型狀態投影做輸出等價的增量證據，而不是改變 Shioaji 連線或拿熱快取冒充完整來源。

12:47 的跨系統唯讀核對：Windows `StockAgent Public Caddy` 工作排程顯示 **Running**，含開機／登入／定時觸發；Caddy 在 Windows `::`:80/443 與 `192.168.50.211`:80/443 監聽，StockAgent Windows 防火牆允許 TCP 80/443 的 `LocalAddress=Any`。WSL 本機 `127.0.0.1:8770/healthz` 與公網 IPv4 均 HTTP 200。DNS AAAA 當時是 `2001:b011:e610:3a44:7ccc:5dfa:4c50:bcc0`，**不是** Windows `Ethernet 5` 上的 `2001:b011:e610:3a44:b0c6:b1a0:8768:74eb` 或其另一個全域位址；Windows 本機測其 IPv6:443 TCP 成功，但公網以 DDNS IPv6 連線仍逾時。這把當前雙棧阻點收斂到 DDNS 指向路由器 WAN 位址後的 IPv6 防火牆／轉送或 DNS 目標選擇；未取得路由器當前設定與外部 IPv6 對 Windows 位址的探針證據，不能單獨歸因於哪一項，也不能因排程顯示 Running 便宣稱冷重開機已驗收。

公開資料狀態投影原先把同一批 logical sources `_enrich_and_sort_rows` 算兩次：第一次供群組狀態彙總，第二次和群組／實體清冊合併。現在在**同一個快照輪次**重用首次計算結果，僅對群組／實體列新算，再按原排序鍵重排並重編 `sort_index`；來源發現、時效、發布與缺口檢查均不跳過。測試證明每個來源只富化一次、分拆再合併與原全量富化的欄位／排序相同；54 個資料監控測試及 Ruff 通過。在隔離、已提供清冊且把永豐／OpenBB 來源固定為 fixture 的四次相鄰樣本，原雙算約 **320／355 ms**、新單算約 **299／309 ms**；這只有數十毫秒，不可換算為正式完整快照的穩定百分比。自然輪次公開投影仍在約 0.95～1.1 秒範圍，來源清冊約 1.2～1.4 秒，是下一個較大的成本。

Windows 最近實際開機時間是 **9/15 07:31**，不是 9/24 08:50；後者是 WSL 不可達後的恢復事件，不能冒稱冷開機驗收。Caddy supervisor `startup.log` 從 08:50:18 到 08:53:58 約每 15～16 秒派送一次 `wsl.exe` 啟動要求，08:54:04 首次記錄 backend healthy。Linux `uptime -s` 為 08:53:57；systemd 在同秒啟動公開 gateway，08:54:01 開始監聽，因此這段約 3 分 39 秒的主要延遲發生在 Windows 派送 WSL 到 WSL 使用者空間可用之前，**不是** Python gateway 的 4 秒啟動，也不是 Caddy 設定未安裝。已安裝的 Windows launcher/Caddyfile SHA-256 與 repo 版本逐位元相同。需另取 Windows WSL 啟動事件或真實受控重啟測試，才可分辨 WSL VM／磁碟／Windows 資源延遲；本輪不在盤中重啟 WSL 或交易服務製造新的樣本。

## 9/24 13:01～13:15 開盤保護漏網與訊號健康誤判

正式 OpenBB archive 今日 08:58:52 由 `OnBootSec=5min` 啟動，在 09:01 左右才完成約 890 萬工作項的既有計畫載入；其低優先權配置不能保證零 I/O／記憶體競爭。這和 09:00 訊號延遲同時發生，但尚無單因素因果證據。既有 guardian 08:20～09:10 只暫停三個 registered-data 可續傳工作，漏了 OpenBB archive；開機後 08:58 的觸發也沒有 `ExecCondition`。現已在來源模板加入同一開盤閘門，將 archive 納入 guardian 的有收據暫停／恢復清單，並增加工作日 09:12 的 timer 補啟動，供開盤中因 `ExecCondition` 略過的開機觸發於時段後續傳。`Persistent=false` 避免漏跑補啟動直接撞進保護窗口。36 個開機、guardian、archive shell 契約測試通過，calendar 解析下一次為 9/25 09:12；**截至此記錄尚未安裝新版 systemd 模板**，13:20～13:30 出場前不會為此重啟當沖或 OpenBB 工作。

同一時段 guardian 把四個今日 09:00 已持久提交的訊號標成「非因果回放」，根因是 `_classify_session_signals` 只接受舊的 `causal_best_quote` 字串，而四個新模式的正式紙上執行契約是 `causal_market_full_target_at_best_quote`。加入兩者的明確允許集合，仍拒絕官方開盤價回放、非零價格 offset 與缺少訊號／完成時間。27 個 guardian／開機回歸通過；13:11 自然 guardian 收據已由 `failed` 改成 `degraded`、`failures=[]`，沒有改帳本或聲稱券商成交。`degraded` 的剩餘原因是根目錄磁碟 92% 使用率、約 154 GiB 可用，觸發原有早期警告，不是此誤判。

當沖公開狀態仍為 `degraded`：100m 與 GELU 模式各有同一檔 7547 的 2,000／128,000 股紙上進場保持 `waiting_fresh_regular_session_quote`；其餘三模式為紙上目標數量完成。現有 QuoteSTKv1 最多 200 筆同時訂閱，13:08 約 291 檔待估值，該分鐘只有約 116 檔有 10 秒內新事件。沒有正式盤中、非試撮且訊號後的最佳買賣價時不能從 Snapshot 或 1 分鐘 K 線偽造「市價成交」；目前沒有對 7547 做券商委託或新增永豐登入。13:20～13:30 紙上出場與 13:35 後服務部署尚待實際收據驗收。

## 9/24 13:20～13:38 尾盤故障驗收與背景服務隔離

13:20 首次看精簡 `status.json` 時，由其不含 `exit_limit_submitted_at` 誤推斷限價出場未執行；核對 canonical `state.json`、`events.jsonl` 與公開 `/tw-day-trade/api/summary` 後，五模式均為 **13:20:00 已送出**，公開摘要也有時間戳，故沒有為此增加重複投影或修改交易。13:24 五模式均進入市價強制出場，部分已平；13:25:01 均記錄極端合法限價尾盤委託。13:25 尚有 191／5／1／0／1 筆 `closing_auction_order_status=working`，但 `closing_auction_pending_count` 顯示 0：原引擎只在事後結算更新此計數。已在**尚未載入的來源程式**中改為委託送出時立即反映未平筆數；四個相關出場測試通過，沒有把掛單變成成交。

13:30 收盤驗收沒有證明成交，13:35 的原帳本仍有 **191／5／1／0／1，合計 198 筆未平**，逐筆標示 `unfilled_no_causal_auction_evidence`，舊契約將其轉成模擬融資／融券，非券商核准或實際成交。正式五市場 `day_trade_strict_intraday` 旗標仍為 false；9/10 已實作但未啟用的嚴格契約才會等候 13:33 延遲撮合至 13:35，且僅具證據的不利鎖漲跌停可例外留倉。因現有帳本含舊持倉，不能在未核對成本／股數與使用者選擇前把這 198 筆冒充平倉或直接替換策略；已向使用者詢問是否保留原帳本，或另建明示反事實清算新帳本。

現場重複的 `latest_quote` 來源已由共享 broker 請求檔 `requester_pid=773` 確認為**隔日沖**引擎，不是 Discord。舊引擎於 13:30～13:35 因 `close_hot` 每 0.1 秒迴圈重打同一批 424 檔 Snapshot；當沖 broker 日誌該五分鐘共有 1,185 次 `latest_quote`，其中 1,178 次為 424／424，同時 Discord 盤後快速訊號亦需較大的 snapshot。這是與尾盤行情的資源競爭，仍不能單獨證明每筆收盤缺價都由它造成。隔日沖 executor 已加 monotonic **最少 1 秒請求間隔**，保留訊號檔 event-driven 喚醒與最長約 1 秒的新報價輪詢；21 個隔日沖測試、Ruff 通過。13:35 觀察截止後 13:36:15 才重啟隔日沖，四模式未平股數皆為 0，100m 的 `critical_actual_close_print_missing` 保留；下一個實際開／收盤仍須驗收請求率與有效價格。Shioaji 即時訂閱與 Snapshot 是不同證據契約；此節拍不允許把 Snapshot 宣稱為當沖即時非試撮成交。

OpenBB archive 模板已於 13:36 透過正式 installer `--no-start` 套用，重建 3,943 列監控歷史約 4.74 秒；原 archive PID 5880／08:58:52 啟動時間未變，未重啟下載。systemd 已載入 `ExecCondition` 與工作日 09:12 補啟動 timer；離線評估 9/25 08:58:52 被拒、09:12 允許，37 個 guardian／開機／archive 回歸通過。這證明配置與模擬時鐘，不等於下一次真正 WSL 冷啟動及 09:00 資源避讓已驗收。當日根目錄磁碟約 92% 使用、154 GiB 可用，警告仍存在且沒有刪任何資料。

## 9/24 13:42～13:46 永豐歷史服務開機喚醒核對

三個歷史服務的 `active` 起點分別為期貨 Tick 09:03:51、一般歷史 09:05:51、台股分鐘 09:23:56；逐一核對 cgroup 與日誌後，當時都**只有 Bash、tee、sleep**，保護等待至 14:31，沒有盤中歷史 API 查詢。不能把常駐 `active` 推論成搶走報價額度或開盤訊號延遲的因果證據。真正多餘的是開機／啟用 timer 在 07:45～14:31 提早喚醒服務、佔三份常駐程序，且 `NEXT=-` 不易觀察正式 14:31 啟動時點。

三個 service 現共用 `ExecCondition`，交易日 07:45～14:31 不啟動歷史 runner，14:31 日曆 timer 保留。離線時鐘 9/25 09:03:51 拒絕、14:31:00 允許；27 個開機／guardian 測試通過。三份模板已用各自 installer `--no-start` 安裝；確認當時只有 sleep 後停止這三個等待程序，沒有停止 Top-200、TAIFEX、當沖或 Discord。systemd 顯示三個 timer 均 `active`，**下一次均為 9/24 14:31:00**，服務 `inactive` 且結果 `success`。14:31 的真實下載、額度及完整性仍待當次 receipt 驗收；此修正只避免盤中空等與啟動誤判，不把資料標為已完成。

其中期貨 Tick／一般歷史在平日 14:31 啟動後，仍可能依原有 FOP 夜盤連線預留規則等待到次日 05:00；因此 14:31 timer 到點並**不保證**這兩項開始歷史 API 查詢，須看 runner 日誌的實際批次與收據。台股分鐘下載另有 14:31～14:45 的限時視窗。

13:47 儲存壓力驗證：根檔案系統約 2.0 TB，剩餘 **147 GiB／使用率 93%**。Binance public archive 12:30 的 preflight 因 10% 保留空間門檻未達而退出 75；service 的 `SuccessExitStatus=75` 使 `Result=success`，但公開資料監控正確仍列為 `unable/degraded`，不能當成封存完成。正式 compiler-cache 清理在 13:09 的 receipt 是 `eligible_files=0`；同參數唯讀 dry-run 13:47 仍是 0。盤點主要本機目錄：`artifacts` 約 515 GiB、冷 store 約 387 GiB、materialized 約 37 GiB、live producer 約 67 GiB；`artifacts` 中 markets 約 172 GiB、replays 約 105 GiB、live 約 97 GiB、cache 約 51 GiB。這些名稱與 `du` 大小都**不是可刪證據**，本輪沒有清除來源、帳本、冷 release 或快取。04:26 的核准冷 store retention **已**回收約 2.36 GB，仍遠低於恢復約 10% 保留空間所需；需另做逐來源可重建／D 備份／程序引用稽核，不能降低預留門檻冒充有容量。

隔日沖收盤執行另有獨立且可重現的來源契約斷層：100m 模式今天 424 筆 `close_auction_entry` 均在 13:34 標 `expired_without_actual_close_print`，部位 0。共用報價代理的 `latest_quote` 只回 Shioaji Snapshot；目前安裝的 Shioaji 1.7.0 `Snapshot` 型別欄位沒有 `simtrade`，正式官方 Snapshot 欄位表亦沒有它。轉換器因此把 `simtrade_flags` 留成未知，隔日沖 `_auction_print` 必須有 `simtrade is False` 與 13:30～13:33 的交易所時戳才接受，故不能從此來源建立可驗證紙上撮合。10 Hz 舊輪詢造成的重複 Snapshot 已節流到下一交易日最多約 1 Hz，但**節流不會補上缺失的成交證據**。要修復須由現有單一永豐連線的 QuoteSTKv1 非試撮即時事件，提供具交易所時戳的收盤撮合收據，且處理 200 檔訂閱上限；這涉及與當沖共用行情代理協調，不應把 Snapshot `close` 或事後官方收盤價冒充即時可成交報價。今天隔日沖帳本未被重寫。

官方 Shioaji 使用限制還明示盤中不應反覆輪詢 Snapshot 作即時報價，應改用訂閱；因此 1 Hz 只是將原本約 10 Hz 的壓力暫時壓低，**不是合規或來源正確性的終態**。目前與另一位 agent 的當沖帳本工作並行，本輪沒有改共用報價代理／當沖執行器，也未另外登入永豐；要改接收盤事件需一起設計 200 檔上限、訊號優先序、記錄時戳和觀測缺口。14:04 唯讀冷 artifact 狀態顯示 4 個已登錄來源皆未達完整訓練 artifact 契約，故不能以 `artifacts/markets` 名稱或冷 release 存在為理由退役其熱副本。

## 9/24 14:00～14:50 盤後來源耗時與並發失敗

官方 close 初次於 14:00 尚未發布；14:00:30 後接受。盤後 `corporate_action_entitlements` 第一次查詢遇到 14 筆 MOPS HTTP 200 拒絕頁，保留為失敗收據並重試，不把拒絕頁當成功內容。後續正式 completed-session 收據為 `status=ok`，14:41:47 的最新版本確認 TWSE／TPEx close、公司行動、股票 panel 與公開 features 都標到 9/24。`status=ok` 只證明這組衍生來源驗收，不代表所有官方資料已完成或隔日沖成交成立。

股票配發的 MOPS 清單與明細原本各有約數千筆 Future，但只有整批結束才看得到計數。下載器現在每 500 筆（並含首筆與末筆）記錄完成／失敗、單階段 elapsed、實測 rate 與粗估剩餘秒數；正式 summary 增列兩段實際耗時，空候選為 0。它不放寬來源拒絕判斷、不增加請求率，也不冒稱這次 14:00 已使用新程式；下一輪自然執行仍需驗收新欄位與供應商長尾。137 個相鄰公司行動／發布／Shioaji 排程／開機契約測試通過。

隔日沖正式歷史工作 14:28:04 開始，146／146 日 replay 於 14:38:16 算完，隨後依既有 fail-closed 雜湊核對拒絕發布：`twse_daily_ohlcv.parquet` 在其運行期間 14:34:22 變更。服務因此 exit 1，14:38:21 標 `failed`；不能沿用 9/23 的最後完整部署冒稱 9/24 已更新。抽樣比對 9/23、9/24 的官方 OHLC 與舊逐日輸入逐檔相同，但**沒有證明全 146 日、模型 panel 與其他來源都等價**。既有逐日輸入與訊號快取只核對各自檔案雜湊，缺少對新來源版本的等價／失效驗收；直接重跑有混用版本風險。此處尚未修復，不重跑、不清除約 13 GiB 隔日沖歷史工作樹，須先設計來源版本綁定或等價證據，並把來源變動檢查提早到昂貴 replay 中途。

14:31 台股分鐘 K 線 runner 真正啟動查詢，一段現場速率約 4.6 request/s；其後在 14:45 視窗截止後進入既有分鐘資料集建置。一般 Shioaji 歷史與 TX 歷史同時觸發，但兩者只有 Bash／tee／sleep，日誌明記 `live_connection_reservation` 待至次日 05:00，不能標為已下載。14:49 根目錄仍約 140 GiB 可用／使用率 93%；未因空間壓力刪除任何來源或帳本。

## 9/24 15:00～15:15 隔日沖歷史來源版本防線

已在既有歷史重建入口加上來源版本與快取相容性驗收：同一 lineage 工作目錄的已存逐日輸入／訊號若對應不同 `twse_daily`、`minute_manifest` 等全域來源雜湊，**在寫新 plan 與計算前**明確拒絕重用。逐日輸入快取另核對該日漲跌停檔與分鐘 partition 的收據雜湊；同源檔案在重播途中每 20 日比對 metadata，只有變動才重新 SHA-256，末端原有全量 SHA-256 仍保留。這縮短可見的來源競態浪費，沒有跳過原交易、估值或來源語意。

用 9/24 的現存工作目錄只跑 `--stage plan`，約 6 秒即得到預期的 `cannot reuse this cache namespace: minute_manifest, twse_daily`，沒有重新跑 146 日或發布舊訊號。15 個隔日沖相關測試、Ruff、編譯和 diff whitespace 通過。**隔日沖 9/24 歷史仍未修好／未發布**：下一步必須以來源版本綁定的新、可稽核工作區重新產生受影響的逐日輸入與模型訊號，並設計避免每天累積龐大重播檔案的保留規則。現有一次失敗 replay 約 2.3 GiB，其中 `signals.jsonl` 約 2.1 GiB；盲目每天建立新 lineage 會加劇現有約 7% 的低空間壓力，因此本輪沒有這樣做，也沒有刪除任何既有歷史收據。

## 9/24 15:15～15:22 夜盤 FOP 服務分層核對

`stockagent-shioaji-minute-backfill.service` 的券商分鐘 API 段已離開，15:16 仍在本機 `download_shioaji_tw_kbars --local-only` 衍生處理，約 14.3 GiB 記憶體；這不是新增永豐登入，卻仍占 CPU／I/O。同期 `stockagent-shioaji-taifex-bidask.service` 三個 worker 已訂閱，15 點時段 `book_events/trade_date=2026-09-25` 近期 Parquet 持續新增；現場 memory full PSI 10 秒均值約 0、I/O full 約 0.35～1.02%。因此只可說**本次取樣看到行情捕捉在寫入**，不能以兩個 unit `active` 推論無掉包或長期延遲達標；本輪未停任何夜盤服務。

FOP 策略健康與行情捕捉需分開。14:51 的策略 bootstrap 拒絕現存 9/18 到期週期，明確原因 `cannot cash-settle cycle while a shadow futures hedge remains open`；後續 `capture=data_only`。唯讀 `state.json` 有 28 個模擬策略保留非零期貨影子部位，絕對口數合計 115，淨口數 -83；9/18 官方 TXO 結算表有 1 筆價格 47,116，但它**不是影子期貨的平倉或結算證據**。不會清零持倉或把資料捕捉成功冒稱策略啟用。後續需按各期貨實際合約身分、有效撮合／官方最終結算及策略帳本稽核另行修復；本輪僅診斷，沒有改 TAIFEX 策略程式或狀態。

## 9/24 15:29 分鐘補抓終端與本機網站抽樣

分鐘補抓的正式 `download_summary.json` 為目標 9/24、選取／回報各 2,757 檔、失敗與 partial 均 0、`resumable_collection_complete=true`，但 `selected_coverage_complete=false`，仍有 89 檔被分類為來源缺口，不能宣稱全市場完整。後處理 15:29:02 完成 `tw_shioaji_audit status=ok`，混合資料 2,339 檔、不可用 127 檔；runner 隨即進入 `historical_source_gap_retry`，因三條 FOP 夜盤連線加兩條股票保留連線而等待，不是仍在盤中查券商資料。正式收據／狀態的這些層級彼此不同。

15:29 單次本機公開 gateway：`/healthz` HTTP 200／約 1.3 ms，隔日沖 status HTTP 200／約 66.8 ms，當沖 status HTTP 200／約 5.8 ms。實際 payload 中隔日沖 `health=critical`（收盤實際撮合價缺失），當沖 `health=degraded`（未平模擬部位）；HTTP 可達不等於資料或交易正常。本輪遵守與另一位 agent 的工作邊界，沒有修或重算當沖帳本。當時根檔案系統剩餘約 133 GiB／使用率 94%，無安全刪除證據。

## 9/24 16:25 隔日沖同日來源修訂與模型快取範圍

再對 146 個歷史交易日做唯讀語意比對：目前官方 TWSE／TPEx 每檔日線的 OPEN／HIGH／LOW／CLOSE 與舊輸入相同，漲跌停檔亦相同；分鐘 partition 的位元雜湊在 9/15、9/16、9/17、9/18、9/21、9/22、9/23、9/24 共 8 日不同。以 9/24 14:28～14:30 的**兩個當日模型面板快取版本**與目前重新建置的 210 日面板逐日比較，99 與 24 特徵版本各只有 9/24 的 feature row 不同；本次兩次真來源面板建置分別約 35.4／32.3 秒。這只證明兩個 9/24 快取與現在的特徵差異，**不能倒推 9/16～9/23 已產生訊號時使用的所有模型輸入都等價**，也未比對每日日內決策價、其他 masks 與最終撮合。因此目前不沿用舊訊號、不發布 9/24 歷史，也不以 cache 名稱冒充可重用證明。

同日來源更正還可能被另一道捷徑遮蔽：原 `maintain_tw_overnight_history._deployment_current` 只檢查部署日期、lineage 及輸出 SHA。現先以廉價的日期／版本條件排除不相干部署，再核對發布 plan SHA 與每個官方全域來源的目前位元雜湊；任一來源修改就不能回 `already_current`。隔日沖歷史／來源相關 20 個測試、Ruff、編譯與 whitespace 檢查通過。這是**發現錯誤的閘門**，不是 9/24 歷史的重建方案；未重啟交易、行情或當沖服務。16:24 根目錄剩餘約 123 GiB；建立每日 13 GiB 新工作區仍未具備保留／回收證據。

## 9/24 16:10 自然排程後的區分

隔日沖歷史 timer 自然嘗試後仍 `failed/exit 1`，源於來源／舊快取不相容的安全拒絕；公開網站保留 9/23 最後完成部署，不能報成 9/24 最新。FinLab 16:10 工作則在 16:53:58 以程序 exit 0 結束，耗時約 43 分 59 秒／CPU 約 10 分 52 秒、記憶體峰值 6.6 GiB，但正式驗收 `catalog_current=false`：1,109 個目錄鍵中 802 verified、225 missing、82 stale，SDK 有 2 次 600 秒逾時，衍生資料與私有冷發版均未啟動。程序成功與資料完成是不同結論；其中 `rotc_broker_transactions` 逾時已有逐鍵失敗收據及原有短期重試退避，不應直接取消證據閘門或宣稱 all services 正常。16:59 根檔案系統約 115 GiB 可用／使用率 95%。

## 9/24 17:20～17:29 舊歷史訊號可沿用假設遭反證

現存 13 GiB 隔日沖工作樹的主要容量是五個約 2.2～2.3 GiB 的不同日期完整 ledger，並非一份新重建就要再花 13 GiB；訊號快取共約 1.3 GiB。RAM 可用約 84 GiB，當下 memory／I/O full PSI 10 秒均值近 0，FOP 夜盤捕捉仍優先。容量雖可容下一次數 GiB 完整重播，但若每天累積一份而不建立有驗證的保留／回收契約，仍會加速用盡目前 115 GiB 可用空間。

對 9/23 14:28～14:29 當時的 209 日 `panel_cache_v2` 兩種模型版本（99／24 特徵）與目前重新從來源建置的 210 日面板，要求相同 symbol 與 feature schema，按相同日期逐一比對**所有 26 個已存陣列欄位**。兩個版本皆顯示 feature 在 9/16、9/21、9/22、9/23 不同；`can_short_open_mask` 有 13 日不同，`day_trade_can_short_open_mask` 有 51 日不同，`unresolved_corporate_action_mask` 有 19 日不同，另有報酬／量與現金股利欄位差異。兩個真來源面板建置各約 29.3／27.6 秒。9/23 快取不等同每個歷史訊號原始輸入，但這些較早日期的差異已足以**否定**「只有 9/24 需要重新計算」的捷徑；不能把 9/23 已產生權重不加驗證直接搬入新來源版本。此為唯讀診斷，沒有改任何當沖或隔日沖帳本。

## 9/24 17:30～17:42 官方來源修訂與隔日沖模型來源防線

17:33 的正式 `close_final` 掃描有 **1 個真實位元變動**：`tpex_daily_valuation`，舊／新 SHA-256 不同；completed-session 衍生服務因此用約 165.7 秒重建公開特徵，17:36:34 的收據 `status=ok`，不能將這次重建當作重複空轉。當時服務記憶體約 44 GiB，等待其結束後才進行下一輪隔日沖診斷，未和它爭用模型重播資源。

隔日沖歷史 plan 原只綁官方 OHLC、calendar 與分鐘 manifest；模型實際讀取的逐股 Parquet、外部公開特徵、公司行動與配置來源未被釘選。現在沿用 panel 本身的來源解析規則，枚舉這批模型檔、source config 與公司行動收據，逐檔做 SHA-256＋讀取前後檔案簽章檢查，並把列表與雜湊放入隔日沖 plan。輸入準備結束、推論開始／各模式結束、重播開始／每 20 日／發布前都檢查新增、移除與變動；未變檔案的中途檢查只看簽章，末端再全量 SHA。現場為 **2,768 個模型相關檔案／約 1.48 GiB**；單次發現約 0.625 秒、全量雜湊約 1.088 秒，這是額外的真實完整性成本，不是零耗時快取。舊 namespace 缺這份模型來源憑證，`--stage plan` 約 7.37 秒即拒絕舊訊號重用，訊息列 `model_inputs_unversioned(2768)`，這個數字**不是** 2,768 個實際改變的檔案。隔日沖歷史／來源 59 個測試、Ruff、編譯與 diff whitespace 通過；未發布 9/24 歷史、未改當沖帳本。

## 9/24 18:14～18:19 隔日沖新來源版本完整重建與公網驗收

17:30 真實公開特徵來源修訂完成後，以 `nice 19`／idle I/O 在**獨立工作區**完整重建隔日沖 146／146 日、4 模式的價格輸入、訊號與同一反事實撮合帳本；舊 9/23 工作樹未覆寫，當沖帳本完全未觸及。新的正式來源計畫約 693,140 bytes，模型 2,768 檔加官方 4 檔於 plan 驗證約 1.727 秒；整輪完成 18:14:29 並通過部署閘門。`overnight_history.json` 為 `end_date=2026-09-24`、146 日、1,168 個開／收盤權益點、1,208,073 筆歷史訊號、87,520 個事件；`overnight_history_deployment.json` 的結果／plan／歷史／訊號／事件 SHA-256 已寫入。新完整工作樹約 3.6 GiB，18:14 根檔案系統仍約 110 GiB 可用；這是一次性修復，**不是**已解決每日重播與保留的長期容量成本。

歷史發布狀態是 `ready_with_stale_unresolved_position`，保留 1 筆早期未解決持倉與 144 次新收盤訊號被阻擋；即時隔日沖 API 仍為 `critical`，100m 今天缺真實收盤撮合價而未建立部位，且共用開盤驗收問題尚在。公開 gateway 本機 `/tw-overnight/api/status` HTTP 200／約 74 ms，已讀到 9/24；DDNS 強制 IPv4 同 endpoint HTTP 200／約 422 ms（連線 331 ms、TLS 346 ms、首位元組 391 ms），同樣回 `health=critical` 且歷史 9/24。強制 IPv6 `/healthz` 3 秒連線逾時／HTTP 000，雙棧仍未修好。外部網頁抓取工具本身無法存取該站，故以部署主機的實際 HTTPS 路徑測速，不冒稱全球各地可達或瀏覽器端到端驗收。

正式 `stockagent-tw-overnight-history.service` 18:19 再啟動只用約 6.07 秒回 `already_current`，沒有重算或重啟交易；`Result=success`，當時 `systemctl --failed` 為 0。`already_current` 現會驗證發布 plan、官方與全部模型來源檔／清冊，而不只檢查日期。**明天新資料版本的持久增量結構仍未完成**，原排程若直接落入舊 cache namespace 仍會安全失敗；不能把今天手動新工作區的成功當成冷啟動與次日自動恢復已驗收。

重建完成後又移除一個來源驗收自身的重複運算：`infer_signals` 原在四個模式後各自以空簽章重新雜湊約 1.48 GiB 的相同檔案；現在沿用第一次全量驗證的 `(device, inode, size, mtime, ctime)` 清冊，後續模式只重新雜湊真正變動的檔案，並保留重播末端的完整 SHA-256 稽核。這個改動**晚於本次正式完整重播**，已有 59 個相關回歸與 Ruff 通過，但下一次真實多模式工作仍須分段測速確認節省量，不能把 1.1 秒單次雜湊直接乘四當作已量到的服務改善。

## 9/24 18:30～18:45 Binance 每週歷史回補的假缺口

舊 `registered-data-backfill` 9/20～9/21 連續跑約 65,733 秒，Binance 574 檔中 1 檔失敗，因此整批為 `completed_with_failures`；失敗的逐檔報表已被後續尾端排程覆寫，不能倒推該檔已恢復。9/24 14:00 的另一輪尾端＋特徵收據為 574／574 `updated`，只證明其範圍，**不證明歷史頭完整**。

舊歷史頭判斷直接把 `exchangeInfo.onboardDate` 當第一根 K 線時間。現存 574 檔的上線時間與本機首根 K 線比較，有 107 檔差超過 1 分鐘、32 檔超過 1 天、9 檔超過 2 天；最長 ICPUSDT 約 423.81 天。用[官方 USD-M Kline API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data) 的 `startTime`、`limit=1` 唯讀實測這 9 檔，**9／9 官方第一根時間均與本機一致**，各請求約 75.9～103.1 ms。這反證「晚於上線時間＝本機缺頭」；只用上線 metadata 每週可反覆讀取大量已存在資料。

已讓非 tail 的 Binance 增量回補在 metadata 指出缺頭時先以官方首根 K 線探測：官方首根不早於本機首根，便只做尾端重疊更新；官方確有更早 K 線，仍從其實際首根開始補。空回應不當作完整證據，格式／時間不合法則失敗，不改資料列或限流契約。每週非 tail 工作另在來源鎖尚由本輪持有時，把逐檔報表、summary、receipt、symbol 清單與特徵報表寫入 `step_receipts/<run-id>/binance_source/`；後續 tail 輪次覆寫 `data_binance/1m` 最新報表也不會失去當次失敗標的。這只作用於未來 Binance 回補，不碰當沖、隔日沖帳本。53 個 Binance／熱尾端／限流回歸測試、Ruff、Bash 語法與 diff whitespace 通過；**尚未在下一次完整每週排程測得總 wall 改善或證明那 1 檔舊失敗已修復**。

## 9/24 19:00 Windows／WSL 恢復測速與取樣界線

讀取**現有** Windows Caddy `startup.log`，不觸發重啟，將每次 `gateway backend healthy=False` 到下一個 `True` 配對為「已觀察到的後端恢復」，另記第一個 WSL 派送到健康、派送次數、已完成與仍在失聯的狀態。新收據 `artifacts/benchmarks/service-coverage-20260924T1900-startup-recovery.json` 覆蓋 50 service／39 timer／2 path；近 7 天可配對 10 次，最近一次 9/24 11:43 為 **5.604 秒**，最慢一次 9/24 08:49～08:54 為 **274.030 秒**，其中第一個 WSL 派送到健康為 **273.496 秒／18 次派送**。Windows 本次實際開機仍是 9/15 07:31，故 9/24 的長事件是 WSL／gateway 恢復，**不是 Windows 冷開機驗收**；也不能僅憑日誌定位延遲在 VM、磁碟或 Windows 哪一層。

初版收據雖指定 5 秒資源取樣，卻將 Windows 查詢置於兩次 Linux CPU 快照之間，實際間隔 **16.015 秒**；分母是真實 16 秒，沒有捏造 5 秒速率，但不同次 Windows WMI 耗時會令取樣不可比較。現把排程／Windows 查詢移到 `before` 之前，並在啟動鏈收據記錄自己的觀測時間。相同 `--sample-seconds 5` 的 `artifacts/benchmarks/service-coverage-20260924T1905-controlled-sample.json` 為 **5.144 秒**、清單仍 50／39／2、Windows 恢復事件仍 274.030 秒；整體命令 wall 約 9.7 秒，區分了稽核器額外成本與資源觀察窗。17 個測速／趨勢回歸、Ruff、編譯、diff whitespace 通過。沒有停止 WSL、Caddy、Discord、行情或當沖帳本服務。

## 9/24 19:10～19:23 網站非帳本路由冷建置優化

正式 loopback 五條非當沖路由、各五次新連線的前測收據 `artifacts/benchmarks/dashboard-routes-20260924T1910-nonledger.json`：流量歷史首次 **1,113 ms**，其中伺服器建置 **1,109 ms**，熱請求中位 **2.039 ms**；TAIFEX 1 日歷史首次 **1,764 ms**，其中上游代理／淨化建置 **1,735 ms**；功能清冊首次 **323 ms**、後續下載中位 **105 ms**，約 49.9 MB 解碼後 JSON 的單次客戶端解析約 0.47～0.53 秒。不同資料量與快取狀態不可直接比較，最初取樣不等於服務啟動後冷快取保證。

流量歷史 24h 是 15 分鐘趨勢，但原始 store 對每次冷查詢都把約 1,434 筆分鐘紀錄與巢狀路由 histogram 完整 `deepcopy`，然後查詢再只讀聚合。真來源唯讀 profiler 中深拷貝佔約 **1.08 秒／1.59 秒**（profiler 本身會放大 wall），因此保留 `rows_since()` 原有的隔離複製契約，新增**僅供內部流量歷史查詢**的近期唯讀視圖；舊資料／跨最近 2 日仍走原本複製與磁碟邊界。凍結最近兩日匿名 JSONL 到臨時目錄、不修改正式資料，`scripts/benchmark_public_traffic_history.py` 交錯各六次：深拷貝中位 **373.759 ms**、唯讀視圖 **168.728 ms**，12 份完整輸出（去除每次生成時間）SHA-256 全相同，收據 `artifacts/benchmarks/traffic-history-view-20260924T1920.json`。94 個公開閘道測試、加上測速與 Shioaji 流量相鄰共 **118 個**通過，Ruff、編譯與 whitespace 通過。

只重啟**唯讀公開 gateway** 載入程式；當沖、隔日沖、TAIFEX、Discord 的 PID／起始時間前後相同。重啟後第一筆流量歷史 HTTP 200，`Server-Timing` 明記 `cache=build`／`build=172.210 ms`，curl loopback 總 **174.000 ms**；相對前測的 **1,113 ms** 是兩個時間點的現場樣本，不能宣稱穩定改善倍數或 WAN p95。後測同五條路由收據 `artifacts/benchmarks/dashboard-routes-20260924T1922-traffic-view.json` 均 HTTP 200／零錯誤；流量歷史熱中位 **1.150 ms**。`systemctl --failed` 仍 0、gateway `/healthz` 200。資料監控清冊的約 50 MB JSON／客戶端解析，以及 TAIFEX 真來源首次建置仍是未完成的延遲工作，不應用流量頁改善代替全部網站優化。

## 9/24 19:30 全資料欄位清冊的未變更輪詢

全資料頁啟用欄位清冊後每 60 秒會再次請求完整 `/data-monitor/api/features`。目前正式回應約 49.9 MB 解碼後 JSON，gzip 傳輸 **1,203,636 bytes**，瀏覽器解析曾測約 0.47～0.53 秒；資料不變時這些傳輸與解析都不會帶來新資訊。閘道原已提供依完整回應位元計算的 ETag 和 `If-None-Match` 驗證，不需改資料來源或另開快取真相。前端現只在已驗證完整清冊後保存 ETag；下次帶條件請求，304 時讀完空 body、保留既有 rows／搜尋索引／畫面，200 時仍完整解析、檢查與重建。共用 fetch 測速將 304 記成成功的未變更輪詢，不把它算成錯誤。

正式 loopback 以相同 gzip ETag 實測未變更回應 **304／0 bytes／1.496 ms**；故意給不同 ETag 則仍為 **200／1,203,636 bytes**。這是每個保持開啟且清冊不變的頁籤，每分鐘避免一次約 1.2 MB 傳輸和一次巨大 JSON 解析的條件式效益；首次載入與清冊真的變更時的成本沒有消失。`node --test test/test_dashboard_core.mjs` **17/17**、全 45 個 `.mjs` 測試與前端語法檢查通過。隔離 Chromium 的真頁面重測收據 `artifacts/benchmarks/data-monitor-etag-browser-20260924T1937.json` 顯示 80,375 個欄位、搜尋結果與原條件一致、無橫向溢出及 console 錯誤；立即再整理一次時記錄到 304／`outcome=ok`，rows 與搜尋索引維持同一物件，首列 DOM 未替換，計數文字未變。此實測只涵蓋本機瀏覽器與 gateway，不代表外網各設備的速度。測試用獨立瀏覽器與約 25 MB 臨時 profile 已停止／清除；這次只修改靜態前端與測速腳本，公開 gateway 立即讀到新檔，未重啟任何交易、行情或公開服務，沒有修改當沖帳本。

## 9/24 19:45～20:00 WSL 恢復邊界與全資料快照成本

已在唯讀服務覆蓋稽核器加入**目前 WSL boot** 的 systemd userspace 時點。僅當該時點落在 Windows Caddy 同一筆「不健康→健康」事件內，才拆出首個 WSL 派送→Linux userspace 與 userspace→gateway 健康；舊 boot 事件保留未知，不拿目前 boot 時鐘錯配舊紀錄。新收據 `artifacts/benchmarks/service-coverage-20260924T1945-userspace-breakdown.json` 仍為 50 service／39 timer／2 path，1 秒指定資源取樣實際 **1.145 秒**。9/24 08:49～08:54 的 **274.030 秒**失聯中，首派送→userspace **260.353 秒**，userspace→健康 **13.143 秒**；此區分將瓶頸放在 Windows 派送至 WSL Linux 可用的邊界，但**尚不能**在 Windows 排隊、VM 建立、磁碟或時鐘同步之間進一步歸因，也不是 Windows 冷開機測試。本次讀取 root crontab 為「no crontab」，root user systemd 沒有 StockAgent unit，Windows 相關工作名稱仍是 4 個；`supervisorctl` 與 `atq` 此主機未安裝，不能因此推斷遠端或所有其他使用者沒有背景工作。18 個覆蓋／趨勢回歸測試、Ruff 通過，未重啟任何服務。

每 30 秒的 `stockagent-data-refresh-status-snapshot.service` 是另一項持續成本。19:45～19:49 的正式 `data_monitor_timing` 多筆樣本約 **2.15～2.57 秒 wall**／**2.06～2.27 秒 process CPU**；例如 19:46:50 這次 2,344.841 ms 中，服務狀態 114.090 ms、56,696 檔的 record inventory 1,272.046 ms、1,632 個來源的公開投影 930.198 ms、已驗證重用的 feature projection 27.867 ms。inventory 當次 `refreshed_files=0`，代表仍逐檔確認來源簽章、不是重讀 Parquet footer。獨立 profiler 未帶正式 `STOCKAGENT_COLD_INVENTORY_CACHE_PATH` 時會重驗大份冷庫 inventory，**不能**拿其 1.7 秒直接當服務瓶頸；帶正式快取後，永豐監控的讀檔器單次仍呼叫約 1,097 次，其中歷史合約 manifest 目前實存 **768 份**。這些驗證與資料健康有關，尚無等價的失效證據機制前不刪掉或任意延長 30 秒新鮮度契約。OpenBB 服務因 provider cooldown 等到 9/25 04:05，`systemctl` 顯示 cgroup 約 **2.4 GiB** 記憶體，但 `/proc/5927/status` 的 Python RSS 只有約 **223 MiB**；`memory.stat` 中約 **2.24 GiB 是 file cache**、匿名記憶體約 **187 MiB**。cgroup 用量不能直接稱為行程 RSS，不能以此推論中途退出可省 2.4 GiB；本輪沒有中斷該活服務或改其冷卻流程。

為避免往後再將檔案快取誤判成行程堆記憶體，全部 service 的資源快照／5 分鐘歷史現同時保留 `memory_current_bytes`、`memory_anon_bytes`、`memory_file_cache_bytes`，後兩者來自同一 unit 的 cgroup `memory.stat`；這三欄不是互斥加總，kernel/slab 等仍占其餘部分。新收據 `artifacts/benchmarks/service-coverage-20260924T2010-memory-split.json` 的 **13/13 個 active unit** 都取得 anon/file 分拆，OpenBB 分別是 **196,161,536／2,403,622,912 bytes**，公開 gateway 是 **748,625,920／220,868,608 bytes**。正式 20:09 的 5 分鐘 `all_service_runtime_sample` 也已帶出相同 OpenBB 分拆；36 個相關回歸、Ruff、編譯與唯讀現場稽核通過。這是觀測修正，尚未降低服務用量；後續優化必須按 anon、file cache、服務輸出與資料健康分別驗收。

20:12 對 record inventory 的無變動分支做小型整理：56,696 個已選檔仍逐一核對 `stat`，但確認集合與檔案簽章未變後，不再重建 schema 引用圖；若新增／刪除檔案、檔案在核對時不可用或有新 footer，仍走完整重算。原實作在 `max_refresh_files=0` 且剛新增檔案時可能錯用舊資料集總計，現在新增的回歸要求該資料集 `files_total` 立即變 2、`count` 保持未知。實檔 5 次前測 1,254～1,360 ms，中位約 **1,308 ms**；後測 1,290～2,613 ms，中位約 **1,351 ms**（含同機並行排程干擾），**沒有可驗證的 wall-time 改善**，不能宣稱已解決 1.2 秒掃描成本。相關 Python **110** 個測試、Ruff 通過；其中一個既有全資料頁靜態測試還期待舊 `fetchJson("api/features")`，已改為驗證上輪的條件請求與 304 路徑。後續若要真正降到亞秒，需要在保留新增、刪除、同名改寫與資料健康語意的前提下，建立可驗證的變更索引或事件驅動失效，而非單純延長輪詢週期。

20:20 再補一個相同捷徑的邊界：**既有同名檔已改寫、footer 額度為 0** 時，不能因 `refreshed_files=0`、檔名集合不變就沿用舊的 `verified` 總數。現在只要掃描遇到簽章變動，即使因配額中止，也不走舊 aggregate 捷徑；資料集降為 `scanning`、`count=null`，待下一輪讀到 footer 才恢復精確值。舊版本快取也不得直接重用新版本 aggregate。新增實檔同名改寫回歸後，監控／測速相鄰 Python **111** 個測試通過。20:19 正式快照自然執行，`inventory` 約 1.21 秒；此修正是防止錯誤綠燈，**不是延遲已改善的證據**。唯讀 gateway `127.0.0.1:8770/healthz` 及 `/data-monitor/api/status` 均 HTTP 200（單次約 1.6／133 ms），`systemctl --failed` 仍為 0；這只證明可達性，不代表其他資料與交易健康。

## 9/24 20:20～20:28 TAIFEX 唯讀面板的雙來源健康狀態

盤中／夜盤的 TAIFEX `stockagent-shioaji-taifex-bidask.service` 行程仍在，日誌顯示 capture `data_only`；策略啟動的官方結算階段因「模擬期貨避險部位仍未平」而失敗。`state.json` 的 `updated_at_utc=2026-09-24T06:51:35Z`、`engine_status=blocked_subscription_bootstrap_settlement` 與明確原因是**較新的策略狀態**；`status.json` 最後是 9/18，不能拿前者時間當作今天有可成交行情或新估值。舊面板只用後者判健康，20:20 的 `/healthz` 因逾時回 503 `stale`，但沒有顯示今天的策略阻擋。

現在唯讀投影只在 `state.json` 的**明確訂閱啟動結算阻擋**比行情 `status.json` 新時採用其引擎狀態，公開文字僅顯示安全的原因類別，不把狀態檔中的任意例外字串／私有路徑直接公布；`source_updated_at_utc`、`source_age_seconds` 仍嚴格來自舊行情檔。前端同時寫出「策略阻擋」與「最後行情快照逾時，不能當即時估值」。這是來源／策略健康的分離，不產生、修補、結算或沖銷任何帳本與部位。TAIFEX 面板與策略相鄰 **67** 個回歸測試、Ruff、Python/JS 語法和 diff whitespace 通過；現場唯讀快照回 `health=blocked`、`engine_status_source=state`、`source_age≈542,447s`。只重啟 `stockagent-shioaji-taifex-dashboard.service` 載入唯讀程式；行情擷取 PID 149、當沖服務 PID 54773 均未變。重啟後 `/api/status` HTTP 200 且顯示上述狀態，`/healthz` 仍 **503 blocked**，這是正確的失敗訊號，不是策略已修復。正式避險部位／官方結算修復不在本輪；依衍生品契約不能為了消除紅燈自行沖銷或假造結算。

同一時間 20:25 排程的 `stockagent-tw-overnight-history.service` 自行失敗，原因是來源修訂與既有 `c275c5f...` cache namespace 不相容（`minute_manifest`、`twse_daily`、2,768 份未版控模型輸入）；`systemctl --failed` 因而不是 0。本輪沒有啟動它、改隔日沖或當沖帳本，也沒有清掉這筆失敗證據。這項與 TAIFEX 面板重啟不同，必須由該歷史工作流使用新的、完整驗收的 lineage 解決，不能把失敗標成健康。

## 9/24 20:30～20:45 WSL VM 啟動前等待的跨平台時點

對 08:49～08:54 這筆 **274.030 秒**後端失聯，前一個 WSL boot 的 systemd journal 在 **08:49:23** 明確進入 `poweroff.target`／`The system will power off now`，08:49:25 結束。這是正常關機路徑，不能寫成「已證明 VM 崩潰」；目前也沒有可驗證的呼叫者，不能猜是使用者、測試腳本或 Windows 排程。Caddy 首次派送 `wsl.exe` 約 08:49:30.647。Windows `System` 的 Hyper-V VmSwitch 事件 102 與**目前執行中的 `wslhost.exe --vm-id`**相符，最新網路驅動載入是 **08:53:50.5046395**；最早相同 VM 的 `wslhost.exe` 建立於 **08:53:51.480481**，systemd userspace 的秒級時點是 **08:53:51**，gateway 健康於 08:54:04。由此得到首派送→VM 網路驅動約 **259.858 秒**，驅動→userspace 約 **0.495 秒**，userspace→gateway 健康約 **13.143 秒**。`wslhost` 的亞秒建立時點略晚於 systemd 秒級時點，稽核允許至多 1 秒顯示精度差，不硬把它解讀為 Linux 先於宿主行程啟動。

`scripts/audit_service_latency_coverage.py` 現用唯讀 Windows WMI 與 VmSwitch event 102 取得**當前** VM 的相符時點；多個 VM ID、沒有事件或時序不符就保持未知，舊 boot 事件不借用新 VM 證據，也不公開原始 process command line。新收據 `artifacts/benchmarks/service-coverage-20260924T2043-vm-boundary.json` 與最終覆核 `artifacts/benchmarks/service-coverage-20260924T2052-vm-boundary-final.json` 都覆蓋 **50 service／39 timer／2 path**；前者資源取樣實際 1.157 秒，Windows 事件匹配 4 筆、同 VM 的 `wslhost.exe` 6 個。這將長等待定位在 Windows 派送之後、目前 VM 網路驅動可用之前，但仍不能在 `wsl.exe` 退出／重試、VM 建立、儲存或宿主資源競爭之間分出因果。當時 `Tcpip` 4227 與反覆 VmSwitch 285 warning 也存在，卻不是單憑相近時間就能定因；後者在事前事後也每分鐘出現。

Windows Caddy launcher 目前只記派送、不記每次 `wsl.exe` 的**結束碼與耗時**，無法解釋 18 次派送是快速失敗還是短暫成功但 VM 仍未可用。已在 repo 的 `scripts/start_windows_public_caddy.ps1` 增加非阻塞、只對已結束子程序寫出 `exit_code`／`elapsed_seconds` 的日誌，保留同一個 supervisor、10 秒重試、只操作唯讀 gateway 的邊界。**尚未重裝或重啟 Windows 排程**，最終收據的 `installed_caddy_files.launcher.exact_match=false` 如實表示目前仍執行舊版 launcher；下次安全安裝後的自然事件才能驗證新欄位。Windows PowerShell parser 通過，啟動鏈／覆蓋測試共 26 個、Ruff、Python 編譯與 diff whitespace 通過。沒有執行 `wsl --shutdown`、沒有關閉 Caddy、行情、Discord 或當沖服務；三個關鍵行程 PID 分別保持 TAIFEX capture 149、當沖 54773、公開 gateway 610597。此時 systemd 仍因隔日沖歷史工作失敗為 `degraded`，所以不能宣稱整體服務或 Windows 真正冷開機恢復已驗收。

## 9/24 21:05 systemd 以外的專案行程候選覆蓋

`systemctl list-unit-files` 的 **50 service** 是已安裝單元，不等於機器上所有可能的手動背景程序。唯讀覆蓋稽核器現在另外掃 `/proc`，只看目前程序的 cwd 是否在 repo 內，或前 64 KiB argv 是否含 repo 絕對路徑；**只輸出 PID、PPID、程序短名、配對依據、所屬 StockAgent unit 與單行程 RSS**，絕不輸出 argv／命令列、環境變數或開啟的檔案。稽核程序自己的父子鏈排除，避免把本次測速當成漏管服務；無權讀取、程序已退出、遠端主機、Windows 非 Caddy 行程、只持有 repo fd 而 cwd/argv 不含路徑者仍不在此掃描範圍。

收據 `artifacts/benchmarks/service-coverage-20260924T2105-proc-candidates.json` 量到 **50 service／39 timer／2 path**，掃描 103 個 `/proc` PID，47 個符合專案上下文，其中 **36 個行程歸屬 StockAgent systemd unit、11 個為未歸屬候選**。11 個短名組成為 `MainThread` 3、`bash` 3、`sh` 3、`htop` 1、`nvtop` 1，皆以 cwd 配對；其父子關係符合目前編輯器／終端／監看工作，但**未經命令來源與啟動器證明，不能稱 11 項獨立背景服務，更不能因這次沒看到其他名字就宣告全機無漏項**。行程數只是瞬間快照，與 50 個已安裝 unit 並非同一統計母體，也不能相加。相鄰 `/proc`／監控回歸 20 個、Ruff、編譯通過；全服務仍有單次操作 p95 與正式業務收據缺口，這項只補發現層。

## 9/24 21:15 永豐監控的冷／熱成本與優化邊界

單獨以正式資料建置 `build_shioaji_public_status`，同一行程連做五次：首次 **597.297 ms**，接下來 **64.853／57.779／68.712／66.146 ms**。這些是本機單次樣本，不是 p95；跨行程的每 30 秒資料監控快照仍會重新負擔冷讀，不能把同一行程熱值當成正式排程耗時。唯讀 profiler 的首輪約 0.556 秒，涵蓋 1,097 次 `_read_json`、約 768 份不同期貨歷史合約 manifest，以及 journal 的解析；`_history_manifests` 約 0.245 秒、其餘 pipeline 建置約 0.288 秒。現有 JSON 讀取依裝置／inode／大小／mtime／ctime 核對檔案，並在讀取後再確認簽章；不應為了省掉冷讀而無憑證地延長資料新鮮度、跳過修改或讓交易行程承擔監控計算。下一步若要壓縮正式冷成本，應先建立可驗證的跨行程收據變更索引與失效測試，逐項比對完整投影等價後再部署；本輪沒有修改 Shioaji 連線、配額、捕捉或監控來源語意。

## 9/24 21:58～23:21 排程覆蓋與當沖故障隔離

唯讀覆蓋器新增 `/etc/cron*`、root crontab、root user systemd 與 Windows 排程工作動作中的專案線索；只計數與列安全的工作名稱，不公開命令參數或憑證。`artifacts/benchmarks/service-coverage-20260924T2321-post-fault.json` 覆蓋本機 **50 service／39 timer／2 path**，13 個 cron 檔案均可讀、root crontab 為空、root user manager 的 54 個 unit 名稱沒有 StockAgent 匹配；Windows 查到 4 個相關工作。這不是其他 Windows 使用者、遠端節點或所有短命程序的全域證明。當前唯一 `failed` 的已安裝 unit 仍為 `stockagent-tw-overnight-history.service`；其來源版本／快取不相容尚未在此線解決。主機 23:21 的 10 秒記憶體 PSI 為 0，但 21:58 曾量到 `full avg10=35.84`，須區分時點。

9:37／9:39 當沖 unit 曾被 90 秒 outer watchdog 殺死；9:43 唯讀 dashboard 的 8766 port 衝突又使原 `wait -n` 包裝器連帶退出，systemd 連續重啟整個引擎。現已讓引擎為 critical child、面板為可獨立重試的 child，重試間隔 2／4／8／16／30 秒封頂；交易／帳本邏輯未改。22:34 收盤後僅重啟此 unit 載入包裝器；22:35 對**確認在相同 cgroup 的唯讀面板 PID**送 TERM，面板重新監聽 8766，而引擎主 PID `787958`、InvocationID `14def02ad64347fc97006a94bf6eba40` 沒變，`NRestarts=0`。測試前後當沖帳本指紋同為 `8ee9bca3f33088c18914d9e1ac2863dbcd496cb60bee04918894476492d4289f`；公開 API 仍 `health=degraded`、帳本完整性 ready、Discord 同步零落後。程序級故障隔離測試含持續崩潰退避、引擎退出傳遞，加上相鄰監控／守護／隔日沖測試共 **53 passed**。這證明**面板子程序故障不再直接重啟引擎**，不證明 9:37 的長時間 panel/模型建置停頓已根除，也不代表 198 筆當沖未平已結清。

23:21 從部署主機走 DDNS 強制 IPv4 的當沖狀態單次請求 HTTP 200、總耗時約 38 ms；這不是跨地區 p95 或資料健康證據。Windows 已安裝 Caddy launcher 仍是舊檔；repo 的新 WSL 派送完成碼／耗時日誌尚未部署，不能宣稱下次冷啟動已驗收。

23:45 再以同一 gateway 做 **39 條有限路由、每條 2 次、單一客戶端** 的可重測基線，收據在 `artifacts/benchmarks/dashboards/http-20260924T2345-system-baseline.json`；39 條皆 HTTP 成功。首次觀察（不保證冷快取）最慢的是日期範圍當沖訊號約 3.23 秒、完整當沖分鐘史約 0.984 秒、overview 約 0.877 秒、TAIFEX 單日史約 0.553 秒、全資料 feature 約 0.428 秒；同輪第二次分別約 6.3、95.5、1.6、25.5、153.9 ms。兩次樣本不足以估 p95 或承諾服務水準。面板 payload 仍如實為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `critical`、Shioaji `degraded`、全資料 `critical`；OpenBB 顯示 `active` 亦不可推論每筆來源新鮮。使用中的 gateway、行情擷取與交易引擎未因測速重啟。

所有目前安裝的 StockAgent service／timer／path 經 `systemd-analyze verify` 無設定語法錯誤；這不是業務健康檢查。當沖子程序、守護器、服務監測、隔日沖歷史的相鄰回歸 **53 passed**，`bash -n`、Ruff 和 `git diff --check` 均通過；整庫測試與真正斷電冷啟動未驗收。

## 9/25 00:18～00:33 隔日沖來源版本自動隔離與真資料驗收

隔日沖歷史排程先前固定寫入模型 lineage 目錄；官方來源變了卻仍嘗試重用舊 `inputs`／`signals`，正確性檢查只能反覆拒絕。現在由官方全域來源、2,768 個模型輸入、逐交易日漲跌停檔的內容 SHA-256 與模型 lineage 決定 `source_generations/<sha256>`；同版中斷可續跑，變版不借用舊訊號。完成後 maintenance 只接受自己宣告的子目錄、相符的交易日／lineage／來源版本、完整逐日漲跌停清冊，重新核對來源才發布。舊部署缺逐日清冊時仍逐日核對原始輸入收據，不因版本缺欄而假裝來源未變；失敗或資料修訂保留為待重建。此版本隔離**沒有宣稱**跨來源版本的歷史訊號可安全增量沿用，也不自動刪除舊版本。

00:18:39 用正式 `stockagent-tw-overnight-history.service` 在原有低優先權設定下從新來源版本重建；00:30:32 成功退出，146／146 日、四模式均完成，wall **11 分 52.572 秒**、CPU **17 分 8.939 秒**、cgroup 峰值 **12.1 GiB**。階段時點：輸入最後一日 00:19:09、模型訊號四模式結束 00:24:47、回放 00:25:02～00:30:11；不能把 6 秒 `already_current` 當作來源重建延遲。發布結果為 1,208,073 筆歷史訊號、87,520 個事件，狀態 `ready_with_stale_unresolved_position`，仍保留 **1 筆歷史未解決持倉、144 次後續收盤訊號阻擋**。00:33:09 用新版程式正式再啟動同一個 history unit，6.583 秒回 `already_current`，`systemctl --failed` 暫為 0；這不代表即時隔日沖與其他資料健康已全綠。新版逐日漲跌停版本計畫在同一真來源上另做 `--stage plan`，146 個交易日都有 SHA，沒有再次重跑全史。隔日沖版本／拒絕錯誤路徑 **15** 個測試、Ruff、diff whitespace 通過。

容量仍是長期風險：舊固定 lineage 工作樹約 13 GiB，新版每次全來源修訂會產生新完整版本，這次開始前根檔案系統僅約 108 GiB 可用。未建立可驗證的逐 session 增量投影與保留／回收證據前，不應把每日全史重算稱作效能已最佳化，也不能自行刪除仍被部署或稽核引用的版本。正式 TAIFEX 夜盤行情、當沖引擎、Discord 都沒有因本次重建而重啟。

最終相鄰組合（當沖子程序／守護器、全服務覆蓋／長期測速、隔日沖來源／歷史／回放／模擬）**111 passed**；Ruff、shell 語法、`git diff --check` 通過。部署後本機隔日沖 API 在休市時回 `waiting`，歷史 9/24；DDNS 強制 IPv4 同 endpoint 單次 HTTP 200／約 343 ms。當沖仍 `degraded`、TAIFEX `blocked`、全資料 `critical`；這些不受隔日沖歷史排程成功所消除。

## 9/25 00:45～01:43 失敗證據、產品探針與全資料摘要快路徑

全服務覆蓋器仍列出本機 **50 service／39 timer／2 path**，但先前對已卸載的 oneshot unit 可能誤讀 `systemctl show` 預設的 `ExecMainStatus=0`。現以**相同 boot／invocation** 的 systemd `Process ... exited` journal exit 欄位校驗，長期測速趨勢同樣修正；`artifacts/benchmarks/service-coverage-20260925T0050-exit-proof.json` 因此將每週 `stockagent-registered-data-backfill.service` 正確記為 exit 1／failed，而不是假成功。再配對 `registered_{daily,intraday,features,backfill}_runs.tsv` 的起始與耗時收據，`artifacts/benchmarks/service-coverage-20260925T0100-business-receipts.json` 確認每週那次為 `completed_with_failures`、失敗步驟 `binance_perpetuals`、約 **65,733 秒**；後續尾端更新不能倒推全歷史已修好。一般執行時間、業務收據與資料完整性維持分開。

覆蓋器另加六個**本機、唯讀**產品 GET 探針；單次耗時含本機 HTTP 傳輸，但不是外網、瀏覽器、券商下單或上游冷來源延遲。最終 `artifacts/benchmarks/service-coverage-20260925T0145-after-summary.json` 六項均 HTTP 200，TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、永豐 `degraded`、OpenBB `active`、全資料 `critical`。這些健康值依原來源保留，不以成功回應取代。當次 Shioaji 約 **430.5 ms**、隔日沖約 **226.7 ms**；各只有一筆，不是 p95。Shioaji 狀態建置的獨立 profiler 冷約 **0.60 秒**、同一行程熱約 **0.12 秒**，主要是本機約 1,097 份 receipt／manifest、journal 與檔案核對；本輪沒有降低其來源檢查、延長快取新鮮度、使用券商連線或宣稱這兩條已最佳化。

全資料 `/data-monitor/api/summary` 之前每次建置先把約 **6.31 MB** `public_status.json` 整份 JSON 反序列化，然後只抽約 **152 KB** 摘要。現沿用既有每 30 秒快照工作，在完整快照原子發布後，同時寫 `public_summary.json`；摘要及完整檔的裝置／inode／大小／mtime／ctime 與 SHA-256 必須一致，且唯讀／不可控制正式服務的欄位必須有效。任何缺檔、版本錯配、內容改寫或摘要建置失敗都回退原完整路徑，不將失敗標為健康。正式檔單次本機函式比較：驗證並讀小摘要約 **6.0 ms**，原完整 JSON 抽取約 **78.6 ms**，兩者完整投影相等；這是同一時點一筆測速，不是 p95。只重新啟動**唯讀公開 gateway** 後，實際 loopback 首次 HTTP 200、`Server-Timing cache=build/build=8.083 ms`、curl 總 **10.517 ms**，熱請求 **2.081 ms**；重啟前一次 loopback 摘要約 **216.9 ms**。快照每 30 秒更新、閘道摘要 8 秒 TTL，跨世代瞬間可能讀到上一個已驗證版本，不能宣稱每一毫秒都和最新檔案相同；隨後現場比對生成時點及全部摘要欄位一致。健康仍 `critical`。相關公開閘道、資料監控、全服務稽核與趨勢 **195 passed**，Ruff、`git diff --check` 通過；`systemctl --failed` 為 0。未執行全庫測試、Windows 真冷啟動或外網瀏覽器驗收。

第二次 gateway 重啟後立即的第一筆剛好落入快照世代交替，安全回退完整 JSON，`build=75.995 ms`／curl 約 **90.580 ms**；當前 sidecar 再核對為有效約 **6.827 ms**，後續請求仍照原有 stale-while-refresh 契約返回。此例說明新快路徑不是 100% 命中保證；優化的是匹配世代的建置成本，沒有犧牲跨檔一致性。

## 9/25 01:48～01:55 持續快照成本與信任邊界

每 30 秒的全資料快照仍是持續 CPU 成本。唯讀拆解中，約 56,988 個已選檔的檔名發現／排序是其中一段；對所有固定、單層 `*_features.parquet` 來源改用一次 `os.scandir`，保留 `Path.glob` 的隱藏檔、同名目錄與檔案篩選語意，其他遞迴／混合樣式仍沿用原路徑。交錯十輪、同一實際來源集合的局部測速：舊發現中位 **466.8 ms**、新版 **284.7 ms**，兩份 56,988 檔的完整分組／順序相等。這只證明發現階段；正式快照的 inventory 階段仍約 **1.31～1.37 秒**（01:54:50、01:55:20 兩輪），不能將約 182 ms 局部差額當作已量到的端到端節省。36 MB 的 inventory 快取解碼、逐檔簽章核對和約 1,633 來源公開投影仍有成本；沒有跳過新增、刪除或同名改寫檢查。

調查期間兩次約 2.4～2.6 秒的 87,606 欄清冊重建，時間剛好與本輪監控程式碼修改吻合；現有重用契約要求程式碼變更後重建，後續連續四輪 `feature_reused=true`、每輪約 31～38 ms。故不能把這兩次判成無變更狀態下的週期性故障，也沒有為此放寬失效規則。對全資料公開投影做獨立 profiler 時，**未帶正式 `STOCKAGENT_COLD_INVENTORY_CACHE_PATH`** 的一次會重解約 175k 行冷庫 inventory，約 3.5 秒；帶正式快取才約 1.54 秒（仍含 profiler overhead），主要是 Shioaji 本機狀態約 0.57 秒、FinLab 來源約 0.27 秒與其他行列整理。不能拿漏掉正式環境變數的結果歸咎於實際排程。

小摘要快路徑再比照既有功能清冊的本機信任邊界：只讀同擁有者、不可由群組／其他使用者改寫的普通 sidecar，`O_NOFOLLOW` 禁止符號連結；不符時回退完整快照。安全／監控／公開面板相鄰 **200** 個 Python 測試、Ruff 和 diff whitespace 通過。僅重啟唯讀公開 gateway 載入新版；有界重試後摘要 HTTP 200、`Server-Timing build=19.225 ms`（包含重啟後首次來源檢查），熱請求約 **2.309 ms**，`/healthz` 200，`systemctl --failed` 為 0。重試的 curl wall 含一次連線被拒與 1 秒重試，不能當作實際請求延遲。行情、當沖、Discord、TAIFEX 擷取沒有因這次部署重啟，且資料健康紅燈未被消除。

## 9/25 02:50～03:04 跨日期訊號成本與非預期退出保護

當沖訊號頁 8/1～9/24 首次約 **655 ms**、2/25～9/24 首次約 **2,245 ms**（各兩次的正式 loopback HTTP 測速；後續同查詢命中頁快取約 **5 ms**）。完整範圍有 146 個交易日投影、約 **2,004,142** 筆訊號，成本主要在逐日投影的驗證／載入、跨日期統計與排序，不能把熱快取時間當冷建置或宣稱一秒內。若單獨呼叫 Python builder，必須帶服務正在使用的 `STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR` 與相同 `maximum_scan_rows`；漏帶投影快取時曾量到約 **21.65 秒**，那是不同執行路徑，不能用於正式服務前後比較。一次縮欄再回讀當頁的實驗使 100 筆結果跨 69 個日期重讀，未能證明更快或完整輸出相等，已撤回；合併兩次方向統計掃描也未量到可靠的端到端改善，已撤回。保留現有訊號與成交語意，後續需用同一資料版次做逐 session 輕量索引與完整欄位回補的等價驗證，而不是直接對正式帳本動手。

當沖 service launcher 的故障隔離已有引擎／唯讀面板兩個子程序；本輪補正引擎**意外以 0 退出**時，包裝器仍以非零結果退出，避免在 `Restart=on-failure` 等監督政策下被當作正常停機。現場 unit 是 `Restart=always`，故這是防止設定漂移與獨立執行時漏復原，不是假稱已修好既有中斷。新增乾淨退出的程序級測試，合計 **8 passed**；`bash -n`、Ruff、diff whitespace 通過。未重啟仍在運行的當沖引擎，只有下次正常重啟後才會載入新版 launcher。

03:03 唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0310-stability.json`：本機 50 service／39 timer／2 path，`systemctl --failed` 為 0，gateway `/healthz` HTTP 200；六個產品 GET 均 HTTP 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、永豐 `degraded`、全資料 `critical`，不能稱業務全部恢復。當沖資格 watcher 仍 `activating`，最近兩次各跑滿約 90 分鐘後因 TWSE 官方 master 宣告日仍為 9/24 而寫 `publication_pending_timeout`，30 秒後由 systemd 重試；這是**來源等待，不是交易引擎中斷**，但每輪約 2,600 次 2 秒輪詢的長期網路成本尚未優化，不能把 `activating` 當已取得 9/25 資格。Binance archive 最近一次 exit 75 是磁碟可用空間低於既定 10% 安全保留時在遠端發現前拒絕，非下載成功；registered backfill 最近的業務收據仍是 `completed_with_failures`，失敗步驟 `binance_perpetuals`。本輪未繞過磁碟閘門或重跑 18 小時回補。

## 9/25 03:05～03:20 跨日期訊號首建置剖析與記憶體邊界

用正式投影快取環境與相同 2/25～9/24／`offset=17`／`limit=100` 再做單獨進程剖析：完整輸出 SHA-256 `cd6fa9c9c1fb3b3e98ee9729c352bccc340f843042055eeede8c8df46083df42`、總數 2,004,142、wall 約 **2.63 秒**、峰值 RSS 約 **2.67 GiB**。`cProfile` 的另一個 offset 樣本約 **2.95 秒**，其中 146 個逐日投影載入約 **1.13 秒**、跨日期統計約 **0.81 秒**、首次行事曆檢查約 **0.34 秒**、`top_k` 約 **0.20 秒**；這些是同一 call 的累計成本，不是可直接相加的獨立服務延遲。再次嘗試持有全幅 shard、在窄欄上做統計與排序再按原 row index 回補，完整回應 SHA 相同，但 wall 變成約 **3.08 秒**，已撤回；不以較慢或記憶體較大的路徑換取看似簡單的欄位數下降。

對明確結束於過去的訊號日期範圍，現在不再查當前盤前顯示日期；今天及未來範圍仍保留原行事曆路徑，日期無效仍由原驗證器拒絕。相同真實輸入的冷建置樣本約 **2.47 秒**、完整回應 SHA 不變，峰值 RSS 約 **2.60 GiB**。這是約 0.15 秒的單次差值，沒有足夠重複樣本宣稱穩定端到端加速。歷史／當日行事曆分支回歸及當沖／公開 gateway／監督器完整相鄰測試 **252 passed**、Ruff、diff whitespace 通過。此程式變動尚未重啟唯讀 gateway 載入；正在運行的交易引擎未動。

當前公開 gateway 的 systemd `MemoryCurrent≈3.34 GiB`、`MemoryPeak≈3.38 GiB`，`MemoryHigh=4 GiB`、`MemoryMax=8 GiB`，本次 `memory.events` 的 high／oom 均為 0。部署模板原「峰值低於 1 GiB」註解已不符合現場，改為具日期的量測邊界；**未調高限制**，因單次／空閒值不足以推論併發安全。下一個具體效能工作應是逐 session 聚合索引與有界熱門頁，避免每種 offset／篩選都重新解碼全史；需同時驗證歷史訊號輸出等價、失效、併發 RSS 與來源修訂，不能只比一次快取命中。

## 9/25 03:20～03:27 公開安全淨化器的重複欄位判定

廣範圍訊號在內部頁快取已命中時，gateway 每 2 秒失效的 HTTP 回應仍須重新安全淨化及序列化約 1.12 MB JSON。實際 100 筆頁的淨化原約 **60～80 ms**，`cProfile` 顯示 31,247 次重複鍵名正規化、約 44,824 次遞迴呼叫；與舊的 1 秒模型解釋檢查不能混為同一成本。現在將不變的私有／憑證鍵集合與正規表示式移到模組常數，僅對長度不超過 128 的鍵名快取「丟棄／錯誤遮罩／保留」判定，LRU 最多 512 個鍵；長鍵仍完整檢查但不駐留。這**未省略任何欄位安全檢查**，真實整頁用舊演算法在同一程序重算，公開投影完整相等，SHA-256 `a7bbfb786584b21f5751c23e206e424cad4c099d20edbcc33762c6d2731bc402`。新增長鍵與重複鍵的憑證／路徑／錯誤遮罩回歸。

相同單獨程序、同一頁的新版淨化 10 次樣本首筆 **38.64 ms**、後九筆中位 **20.02 ms**。當沖／公開 gateway／監督器相鄰 **253 passed**、Ruff、diff whitespace 通過。03:17 僅重啟唯讀 gateway 載入新程式：首次正式完整範圍 HTTP 200，`Server-Timing build=2361.097 ms`／curl 約 **2.367 秒**；隔超過 2 秒的下一筆熱頁重新建置約 **139.133 ms**／curl 約 **144.6 ms**。上版現場同類熱頁單次約 177～198 ms，但不是交錯控制實驗或 p95，不應宣稱穩定改善幅度；**冷來源首次仍逾兩秒**。本機與 DDNS 強制 IPv4 `/healthz` HTTP 200，`systemctl --failed` 0，當沖引擎 PID `787958`／InvocationID 未變、`NRestarts=0`，訊號帳本 dev／inode／size／mtime 未變。產品資料健康紅燈未因此消失。

## 9/25 03:22～03:28 訊號頁持倉覆蓋與冷啟動驗收

當沖寫入端計算 `dashboard_content_revision` 時刻意排除 `positions`，避免每分鐘估值使訊號全史重建；但訊號頁在選出當頁訊號後，又會從目前 `positions` 覆蓋後續完成成交的股數、價格與狀態。既有頁快取只看帳本與該 content revision，因此**帳本不新增時，持倉完成成交或覆蓋移除都可能一直顯示舊頁**。先加失敗回歸（0→1,000 股仍顯示 0），再把快取條目明確分成原始當頁列、已覆蓋的呈現列、覆蓋欄位指紋與模型解釋檢查時間；只對實際用到的持倉欄位取指紋。覆蓋變更只重套保存的當頁原始列，不再讀完整訊號帳本；持倉移除時能回到帳本原始 0 股。與每分鐘 `last_mark_at` 無關，保留既有「不重掃」契約。這不建立、修改或沖銷成交，也不把帳本審計改成券商填單證據。

真實 2,004,142 筆廣範圍輸出的完整 SHA-256 仍為 `cd6fa9c9c1fb3b3e98ee9729c352bccc340f843042055eeede8c8df46083df42`；隔離進程首次約 **2,539 ms**，同查詢後兩次約 **9.01／6.97 ms**，樣本不足以聲稱 p95 改善。當沖／公開 gateway／監督器 **254 passed**，隔日沖模擬與歷史相鄰 **25 passed**；Ruff 通過。先只重啟公開 gateway，再對同 cgroup、非引擎 PID 的唯讀 8766 子程序送 TERM，監督器約 2 秒後恢復面板；當沖引擎 MainPID `787958`、InvocationID `14def02ad64347fc97006a94bf6eba40`、`NRestarts=0` 不變，帳本 dev／inode／size／mtime 不變。公網強制 IPv4 `/healthz`、公開當沖歷史訊號、公開隔日沖訊號均 HTTP 200；**沒有**聲稱實際持倉今天有新的完成成交，修正由故障注入測試證明。

子面板重新啟動後的第一次**內部** 2/25～9/24 訊號查詢，在 curl 15 秒期限內未收到 body；伺服器日誌稍後記 HTTP 200，但**客戶端已逾時，不能當成功回應**。相同查詢後續約 **277 ms**，新 offset 約 **1,670 ms**，說明第一次冷啟動仍有尾延遲，不能用成功恢復的 `/healthz` 掩蓋。其逐 session 快取已有 146 檔，獨立 warm-index 函式另測約 **1.06 秒**；目前沒有足夠證據把 15 秒歸因於單一來源或剛修改的覆蓋快取。該 cgroup 在完整歷史請求後峰值約 **5.75 GiB**，子面板 `smaps_rollup` 約 4.87 GiB 為 `LazyFree`、可由核心回收，`memory.events` 的 high／OOM 仍為 0；不能把 RSS 直接稱為無法回收的洩漏，也不能沿用部署模板舊「峰值低於 1 GiB」註解。模板註解已更正，但 8G／16G 限制未改。真正冷啟動／多請求並發與 9:00 服務延遲仍須另行量測。

## 9/25 03:30～03:36 子面板冷查詢分段測速

為追查上筆客戶端逾時，先在與子面板相同的 repo 投影快取目錄中做**獨立進程**試驗：順序執行 index warm 約 **1,092 ms**、其後廣範圍訊號約 **5,174 ms**；另一個新進程同時跑兩者時，訊號約 **3,188 ms**。兩者完整輸出 SHA 均與既有基線相同。這些是不同時間點、受 OS cache 與同機排程影響的樣本，並**未證明**預熱競爭就是先前 15 秒逾時原因；該時段亦有 data-refresh snapshot 與 guardian 排程重疊，但相近時點不能當因果。

內部唯讀 8766 JSON 回應現在提供 `Server-Timing` 的 `prepare`（路由解析與來源建置）、`serialize`（JSON 編碼）與 `compress`（實際 gzip 壓縮）耗時；**新增的測速標頭**不包含原始例外或私有路徑。`/healthz` 同時提供 `session_index_warm_status` 與 `session_index_warm_elapsed_ms`，預熱失敗只列 `failed`、不公開預熱錯誤字串；這不代表既有其他錯誤回應已完成全面敏感資訊審計。相關 HTTP、索引、訊號、監督器與錯誤遮罩回歸 **162 passed**，Ruff、Python 編譯與 diff whitespace 通過。僅讓已確認同 cgroup 的唯讀子面板重新載入；引擎 MainPID `787958`／InvocationID 未變、`NRestarts=0`。

正式重啟後 `/healthz` 先觀察到 `warming`，再觀察到 `ready`／預熱 **1,241.056 ms**。第一次廣範圍訊號 HTTP 200，`prepare=2102.869 ms`、`serialize=13.740 ms`、curl **2.121 秒**；前一次正式重啟首次為 `prepare=2207.561 ms`、`serialize=11.980 ms`、curl **2.225 秒**。同頁後續一筆 `prepare=98.692 ms`／curl **114 ms**，新 offset `prepare=1552.998 ms`／curl **1.569 秒**；gzip 熱頁 `compress=7.515 ms`、約 90 KB。主要成本明確在來源建置而非 JSON 或壓縮，但 15 秒異常這兩次未重現，仍保留為未解尾延遲。未改帳本、行情、Discord 或交易引擎，也未把 `/healthz` 200 當作資料完整性證據。

## 9/25 訊號頁跨 offset 範圍統計共用

2/25～9/24 的訊號查詢涵蓋 **2,004,142** 列。範圍方向統計與開盤執行稽核只依賴帳本版本、選取日期、篩選條件及各模式資本，不依賴 `offset`／`limit`；之前每翻一頁都重做完整 Polars group-by。現以最多 16 筆的進程內快取共用這兩項小型聚合結果。鍵包含來源 dev／inode／size／mtime／ctime、正式歷史簽章、日期、模式、標的、狀態、掃描上限及資本；當頁列、持倉覆蓋與模型解釋維持逐請求取得。每次複製聚合稽核再加當前 state 的預期筆數，避免修改快取原件。帳本 append 與資本改變的失效均有回歸測試。

修改前同進程 offset 17／18：**2,568.3／1,829.4 ms**；修改後另一進程：**2,148.5／933.5 ms**，兩頁完整 JSON SHA-256 逐頁與修改前一致。另在單進程不同 offset 量測：強制每頁重算 **4,839.1／2,212.8／1,622.2 ms**，可共用聚合的後三頁 **989.4／987.9／931.1 ms**；非交錯實驗，受 OS page cache 和同機工作影響，不能推成 p95 或百分比承諾。首次來源建置仍約 2 秒以上，剩餘約 0.9 秒主要要再分離投影讀取、串接與 top-k 才能定位。相關當沖／公開 gateway **256 passed**，Ruff 和 diff whitespace 通過。此項只減少翻頁重算，不更改模擬成交、來源帳本、Discord 或資料健康狀態。

03:52 只重啟公開 gateway、終止並由既有監督器拉起唯讀 8766 子面板；交易引擎 MainPID `787958`／InvocationID `14def02ad64347fc97006a94bf6eba40`／`NRestarts=0` 不變，訊號帳本仍為 dev:inode `2096:38156421`、5,504,935,660 bytes。新版內部 HTTP 首頁 `prepare=2453.822 ms`、curl **2.470 秒**；相鄰頁 `prepare=1123.602 ms`、curl **1.142 秒**。公網強制 IPv4 `/healthz` 200；完整訊號頁先前公網首次 **2.540 秒**、相鄰頁 **1.220 秒**，皆 HTTP 200。公開 gateway 與內部面板各有獨立進程快取；首次公網查詢不會因內部面板曾查詢而預熱。前次偶發 15 秒冷啟動逾時本次未重現，未定根因；IPv6 與各產品資料健康不在此次修正範圍。

本輪唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0354-summary-cache.json` 仍列出 50 service／39 timer／2 path；六個產品 API 均可 HTTP 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、永豐 `degraded`、全資料 `critical`。當沖公開狀態對 9/24 仍列 `intraday_residual_open`：1 億模式 191 筆、Attention 模式 5 筆、Multi-Basis 及 Projection-L1-GELU 各 1 筆，合計 **198** 筆；這是成交／強制退出證據未閉合，不能因網頁翻頁加速就宣稱交易服務健壯或平倉完成。本輪沒有改寫這些帳本或製造成交。

## 9/25 04:00 官方當沖資格 watcher 的空輪詢成本

現場 03:01 啟動的資格 watcher 於 03:56 仍在等官方 9/25 宣告日，`waiting_source` 已記 **1,591 次**、2 秒輪詢；這不是取得新資格。TWSE 官方 [OpenAPI 的 TWTB4U 端點](https://openapi.twse.com.tw/) 此刻宣告 9/24；一次正式 `_http_get` 取回 **86,473 bytes／1,232 列**，獨立樣本網路約 **93.7 ms**、JSON＋日期掃描約 **3.0 ms**、完整表解析約 **19.4 ms**。回應有 ETag／Last-Modified；帶實際 ETag 與 cache-buster 的條件請求回 HTTP 304、無內容。因每次空輪詢都重抓與重解析完整舊表，55 分鐘 1,591 次約對應 **137.6 MB** 的重複表內容；這是依本次內容長度的估算，不是網卡實測流量。

程式現保持 2 秒偵測週期與原有共用來源 rate limiter，在既有 `_http_get` 增加可選的 If-None-Match／If-Modified-Since。watcher 只沿用本進程曾讀到的官方完整 body：304 不傳輸、不重解；新版 200 仍核對宣告日、完整解析並等待 TPEx 同日回應，然後才執行原有 canonical downloader／覆蓋稽核。過期日期只做日期預檢，不能觸發就緒；每 **60 秒**強制無條件重取，以防來源端驗證器更新異常。若沒有本地 body／驗證器卻收到 304，維持 `PublicationPending`；等待、成功及逾時收據加入 `full_bodies`、`not_modified`、`body_bytes`，可量測日後實際節省而非假定。正式端點連續兩次試探為 200／86,473 bytes／約 1,245 ms，接著 304／0 bytes／約 18.3 ms；此二樣本受同機共用節流和網路變動影響，不能宣稱 p95 延遲改善，也**沒有**減少請求次數或保證官方確切發布時刻。相關資格與下載器 165 個測試、Ruff 與 whitespace 檢查通過；另外新增一次 stale 來源的完整 CLI 控制流程回歸，確認 `publication_pending_timeout` 不會被寫成成功。

04:01 活躍 watcher 仍為 03:01 啟動的舊 Python 進程；為保留其 90 分鐘等待與終端收據，**未中途重啟它**。更新程式會在本輪正常成功／逾時後的下一次既有 systemd 啟動才生效；須以新收據 `twse_transport` 出現及實際 304 計數驗收，不能先宣稱現場已省流量。當沖引擎、Discord、行情與帳本均未因本項改動重啟或改寫。

## 9/25 04:05 跨服務記憶體壓力的真實增量

先前覆蓋表把 cgroup `MemoryPeak` 與現在的 `MemoryCurrent` 同列，卻沒有 `memory.events` 的時間差，容易將舊的 48 GiB 尖峰當成當下持續壓力。現在保留每個 installed StockAgent unit 的 `MemoryHigh`／`MemoryMax` 與 high／max／OOM／OOM-kill 累計事件，只有在同一個活躍 InvocationID、PID 與 NRestarts 不變且計數器未倒退時，才計算採樣期間增量；重啟、完成的一次性工作或不可讀 cgroup 都顯示 unknown，不填零。新增 36 個服務測速／盤點測試通過，Ruff 與 whitespace 檢查通過。

正式 10 秒收據 `artifacts/benchmarks/service-coverage-20260925T0405-memory-events.json`：仍覆蓋 50 service／39 timer／2 path。`tw-public-source-events` 當前約 **1.20 GiB**、歷史峰值約 **48.00 GiB**、memory.high 門檻 **48 GiB**、累計 high **106,709 次**，但這 10 秒 high／OOM／OOM-kill **增量皆 0**；公開 gateway 約 **3.04 GiB**、high **4 GiB**、這 10 秒 high 增量 0。這只排除短樣本期間的 cgroup 壓力，不證明日後高負載無風險，也不表示 48 GiB 尖峰來源已根治；下一次重建需用同欄位長期樣本定位，不能直接降低上限或停掉來源驗收。

## 9/25 04:20 註冊下載服務的逐步驟瓶頸

原覆蓋表對四個 registered-data 工作只附「與 systemd 啟動相符的整輪 TSV」；18 小時回補的單一 wall 值不能指出哪個供應商工作耗時。現在只在整輪 `run_id` 已由開始／結束時點驗證後，讀取該**完全相同 run_id** 的 `step_receipts` 目錄；最多 128 檔、每檔最多 16 KiB，拒絕 symlink、錯 run_id、錯 step/schema、非有限或負耗時，且只輸出步驟名稱、狀態、耗時與退出碼，不公開命令或日誌路徑。`latest` 步驟捷徑不作為證據；並行步驟耗時不能相加成整輪 wall。相鄰 36 個稽核／測速測試、Ruff 與 diff whitespace 通過。

新唯讀收據 `artifacts/benchmarks/service-coverage-20260925T0420-step-timings.json` 仍覆蓋 50／39／2。最新 backfill 的 systemd wall **65,724 秒**，業務收據為 `completed_with_failures`；同輪 **OKX 1m 65,726 秒 complete**、**Binance 1m 30,213 秒 failed**、Bybit 1m 19,115 秒 complete。每日工作 1,312 秒中 Yahoo US daily 1,295 秒；features 1,953 秒中 Binance 1m 1,953 秒、OKX 1m 1,627 秒。這只確立下個量測／修復優先順序，不表示可以任意加併發或跳過來源完整性；Binance 失敗仍是失敗，服務 exit 0 不能覆蓋業務收據。

回補失敗屬 **9/20 同輪歷史收據**，不能套到較新的來源結果。9/24 features 的 `data_binance/1m/download_summary.json` 已另記 574／574 `updated`、0 failed、約 1,951 秒；OKX 同輪 491／491 `updated`、約 1,625 秒。這說明較新工作成功，不會回寫 9/20 回補那筆 `completed_with_failures`，也不代表全歷史缺口已審計完整。優化優先順序改為現行 features 兩個 27～33 分鐘階段的細分耗時，並獨立稽核舊 backfill 的未完成範圍。

9/24 同輪來源自身已有逐 feature 階段與 limiter 收據：OKX `history-index-candles` **7,359 grants × 0.2 秒 = 約 1,472 秒**的串列下界，該更新整體約 1,625 秒；Binance `futures_data` **5,947 grants × 0.3 秒 = 約 1,784 秒**，整體約 1,951 秒。這是以本地節流設定與實際 grants 推出的下界，不是逐請求真實服務時間相加；兩個工作尚有網路、解析、寫入與其他端點。[OKX 官方文件](https://app.okx.com/docs-v5/zh)目前對該端點列 **10 requests／2 秒、每 IP**，[Binance 官方文件](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)對對應統計端點列 **1,000 requests／5 分鐘、每 IP**，與本地節流相符。此時單純提高 worker 數或解除 limiter 不會合法地消除主要下界；可行方向是證明哪些請求為重複／超過來源粒度需求，再以同來源行數、版本及缺口稽核驗證減量，不能刪資料或把來源不完整標綠。

## 9/25 04:25 註冊資料工作快速重試的收據隔離

四個 `registered-data` 範圍原以 UTC **秒**產生 run ID。systemd 快速重試或人工重啟若落在同一秒，新的 `step_receipts/<run_id>` 與前次同名；尤其 `latest` 之外的「同一 run」證據會被覆寫，導致效能與失敗診斷混合。新工作改用 UTC 奈秒時間戳作 run ID；稽核器同時支援舊秒格式與新奈秒格式，只讀完整匹配的目錄。這不會重跑來源、改寫資料或改動交易引擎；既有工作直到下次正常啟動才會採用新 ID。測試覆蓋同一秒內兩次不同 ID、各自獨立的步驟收據、舊版兼容及非法日期，相關稽核／測速 **44 passed**，`bash -n`、Ruff 與 whitespace 檢查通過。

## 9/25 04:32～04:33 資格 watcher 自然換版驗收

03:01 啟動的舊 watcher 跑完原定 90 分鐘，於 04:31:32 寫下獨立的 `publication_pending_timeout` 收據（2,572 次輪詢，TWSE 仍宣告 9/24），沒有中途強制重啟或刪除記錄。systemd 於 04:32:02 自動啟動新版 PID `1122362`。新版 04:33:03 的 `waiting_source` 收據有 31 次輪詢、`full_bodies=2`、`not_modified=29`、`body_bytes=172946`；兩次完整回應包含 60 秒無條件重驗，其他 29 次 HTTP 304 沒有回應主體。相對每次都下載 86,473-byte 舊表，這個窗口少接收約 **2.51 MB 的回應主體**；不含 HTTP 標頭、TLS 或其他網路流量，亦不是長期 p95。官方宣告仍落後最低可接受日 9/25，**尚未取得 9/25 資格**，等待狀態正確。`systemctl --failed` 空；交易引擎仍為 PID `787958`、相同 InvocationID、`NRestarts=0`。這只驗收來源等待效率與監督器自動重試，沒有驗證今日開盤成交或修復資料健康。

## 9/25 04:30～04:35 磁碟壓力安全邊界

唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0430-resilience.json` 涵蓋 50 service／39 timer／2 path，10.15 秒內沒有觀察到任何 memory.high／OOM 增量；這不能證明長期無壓力。`stockagent-binance-public-archive.service` 最近一次 exit 75；根檔案系統當下約 **94.86% 已用**、可用約 **104 GiB**，不足既定 10% 安全保留。既有編譯快取維護工具只做 dry-run，判定 `under_pressure=true`，但 `eligible_files=0`、可選已分配位元組 0，沒有可安全清除的舊編譯快取。已批准的 packed rolling-retention 最近一輪只證明／回收約 **532 MB**，遠不足解決約 2 TB 檔案系統的 10% 門檻；沒有放寬保護、刪除來源／帳本或擴大清理範圍。後續需按來源 catalog、D 備份與 Syncthing／程序參照驗證具體大檔，再決定擴容或另行批准保留政策；`systemctl --failed` 空不代表這個業務工作成功。

## 9/25 04:40 TAIFEX 輔助資料的空轉冷卻

前一輪 `stockagent-taifex-auxiliary-daily.service` 約 **750.2 秒**；journal 精確分段顯示主要為選擇權正規化 **740.978 秒**，其中官方收據階段 **49.647 秒／34 份**，其後月 ATM **184.951 秒**、月 full-chain **228.220 秒**、週 ATM **106.929 秒**、週 full-chain **170.658 秒**。這四個衍生層共約 690.8 秒，是後續 per-year immutable 投影或單次解析研究的主目標；不得以移除歷史資料或略過來源雜湊／品質驗證縮短。

收據迴圈原來無論檔案是否已在本地，每份驗證後都 `sleep(request_interval=1s)`。共享下載函式現在可選「只有真正完成 HTTP 附件下載後才冷卻」；選擇權任務採用此選項，其他使用者維持原預設，現有檔案仍逐份進行原本的收據格式驗證。以現有 manifest 所列 34 份且全部已落地的附件做唯讀快取命中＋格式驗證，約 **0.011 秒**、零網路請求；上次正式輪次 34 份中約 33 份本地重用，因此理論上移除約 **33 秒**純睡眠，而真實來源請求仍保留 1 秒冷卻。相鄰 TAIFEX **18 passed**、Ruff、whitespace 通過。尚未執行完整來源／衍生層重建，也未宣稱 750 秒端到端已實測下降；必須比較下一次自然排程的相同來源範圍與階段日誌。

唯讀單檔剖析補充：2025 年官方 ZIP **18,743,862 bytes**，目前 `_read_txo_rows` 月序列 **6.846 秒／223,298 列**、週序列 **6.233 秒／108,238 列**。正式建置分別對 ATM 和 full-chain 重複讀取相同來源，故此年單是四次解析就可能約 26 秒；這是單檔、相鄰但非交錯樣本，不能外推為 34 份來源的總節省。下一階段若要處理約 690 秒的主瓶頸，應以雜湊／解析版本綁定的逐來源投影 shard 或單次解析多消費者架構，先建立完整輸出 SHA／列數等價及記憶體峰值門檻，再決定是否上線；不能用無界進程快取把 34 年選擇權鏈留在 RAM。

## 9/25 04:50 Yahoo 長工作「程序成功≠來源完整」收據

9/24 的 Yahoo US daily：正式步驟 exit 0、約 **1,295 秒**，但同輪 `daily_update_summary.us_stocks.json` 記 `failed=12`、`lagging_skip=615`；前者是實際失敗，後者依既定延遲政策未嘗試修補，兩者是 **627 個仍有缺口的標的**。`stale=12,208` 是修補前狀態且同輪另有 `repaired=12,208`，不得再把它加到未解缺口。`not_found_skip`／`delisted_skip` 另保留原狀態，不能任意改稱完整或改成失敗。現行宿主共用 Yahoo 節流預設每秒 10 請求，12,237 個本輪標的即使每檔只需一個請求，也有約 **1,224 秒**的最簡下界；1,294 秒的觀察值已接近它，單加 worker 不會突破這項客戶端節流與供應商風險界線。

現在排程把 run ID 傳入 Yahoo；每個資產的來源摘要保留該 ID。步驟完成或失敗時，只在來源摘要的 run ID、資產、模式、產生時點與有界結構全部相符時，將安全的狀態計數、來源耗時與 `failed`／`still_stale`／`lagging_skip` 缺口數複製進**同 run** 步驟收據；不複製 URL、命令、錯誤原文或憑證。覆蓋表仍分列程序 `state=complete` 與 `source_data_health=reported_gaps`，若摘要缺失或不符則列 `unverified`，不把零退出碼改寫成來源完整。既有 9/24 來源摘要沒有新 run ID，因此**不能倒填為精確關聯**；需等下一次自然 daily 排程驗收。相鄰 Yahoo／收據／服務覆蓋 **93 passed**，Ruff、`bash -n` 與 whitespace 通過；未重跑 12,000 檔來源或改變 Yahoo 節流、資料與交易狀態。

同一份 04:30 全服務覆蓋表的操作測速等級仍是 **35 個只有上次程序 wall、14 個常駐程序只有資源樣本、1 個退役 hot-sync 無現行測量**。因此「50 個 service 已盤點」不等於「50 個功能均有逐步驟延遲」，完整目標尚未達成；後續須逐一補可關聯到同一次執行的業務收據及長期分位數，而不是把 CPU／記憶體或 HTTP 200 當作功能反應時間。

修改後另執行唯讀整體收據 `artifacts/benchmarks/service-coverage-20260925T0500-yahoo-source-contract.json`，仍成功覆蓋 50／39／2。9/24 的舊 Yahoo daily run 與 systemd 啟動相符、16 個步驟可讀，但 `source_data_health` 留空；這是舊摘要沒有 run ID 的正確 fail-closed 行為，不會用當前可變摘要冒充舊同輪收據。

## 9/25 05:03 OpenBB L1 未指派來源保序索引

最近一次正式 L1 compaction 約 **86.98 秒**，其中來源契約／衍生檔稽核約 **43.30 秒**、未指派來源載入約 **29.14 秒**、實際新段建置約 **2.93 秒**。正式 16 GiB SQLite manifest 原先沒有 `idx_l1_tasks_compaction_order`；`EXPLAIN QUERY PLAN` 證實未指派來源查詢使用 `idx_tasks_active_plan` 後仍建立 `TEMP B-TREE FOR LAST TERM OF ORDER BY`。不應削減 L0／L1 來源、行數、schema 稽核來掩蓋排序成本。

將已由 L1 消費端支援的部分覆蓋索引 `(plan_token, endpoint, task_id, rows) WHERE active=1 AND status='success'` 納入下載器的 manifest 初始化契約。現場 provider scheduler 為 `waiting` 且 active／buffered／completed-pending 全部為 0；持有 L1 排他鎖、使用低 CPU／I/O 優先權、檢查可用空間及來源是否繼續等待後，為現有 manifest 原子建立索引，耗時 **54.566 秒**。建索引只改 SQLite 查詢結構，未改 L0 Parquet、L1 段、任務狀態、查詢 view 或交易帳本。

同一份正式資料庫唯讀比對 `LIMIT 2048`：新保序索引 **2.303 秒**、舊 `idx_tasks_active_plan` **29.486 秒**，兩者 2,048 列的 SHA-256 均為 `49efbb74ab7d4f132d602089f88da216779127901f4ba4b4fa39ebcea3aa12aa`；新查詢計畫無暫存排序樹。每 endpoint 的成功任務 COUNT／SUM，新 **0.901 秒**、舊 **3.689 秒**，48 列 SHA-256 同為 `cb7ce27628cd51995a1aa8c5a8eab41001dfc9215566b89a2f8b5dc1464ec483`。這是不同順序的單組熱度樣本，不能宣稱 p95 或整輪提升；新索引亦增加磁碟與未來任務寫入成本。OpenBB L1 全檔 **15 passed**、OpenBB archive downloader 全檔 **277 passed**，Ruff 與 diff whitespace 通過。下一次自然 timer 必須核對 `unassigned_source_index_order`、整輪 wall、來源稽核、pending 數、磁碟／WAL 成長與 archive 寫入延遲；不能因查詢變快就宣稱服務或所有資料已完整。

建索引後 SQLite WAL 仍保留約 **990,971,272 bytes**。在 provider 仍等待、L1 未運行且持有同一排他鎖時，用 SQLite 自身 `wal_checkpoint(TRUNCATE)` 回傳 `(busy=0, log=0, checkpointed=0)`，WAL 從該大小變為 0，檔案系統可用位元組由 **109,104,082,944** 增至 **110,095,040,512**；未直接刪除 WAL。OpenBB archive PID 仍為 5880、`NRestarts=0`，但剩餘容量仍低於既定 10% 保留，Binance archive 的 exit 75 問題未因此解決。

新的唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0510-openbb-index.json` 仍列 50 service／39 timer／2 path。六個產品端點均可 HTTP 200，但實際資料健康分別為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料 `critical`。這些狀態不因 L1 查詢最佳化自動改善；註冊資料 backfill 上次 exit 1 與 Binance archive exit 75 仍需各自處理。OpenBB archive 仍在供應商 cooldown，沒有因此重啟；下一次自然 L1 timer 才能驗證整輪效果。

05:09:32 依原 systemd unit 的開盤保護、CPU／I/O 權重及記憶體限制啟動一輪正式 L1 驗收，05:10:35 正常結束：**63.175 秒 wall／70.305 CPU 秒**，相鄰前輪 **86.983 秒 wall**；兩輪皆新增 **19 段／2,048 來源 shard**、0 stale、0 failed、0 deferred failure、0 L0 delete。`unassigned_source_load` **29.141 → 3.138 秒**，其中新 `unassigned_source_index_order` **1.893 秒**、metadata **1.245 秒**；同輪 `status_task_count` **3.783 → 1.004 秒**。來源契約稽核 **43.303 → 46.659 秒**，建段 **2.929 → 3.020 秒**，view **4.813 → 6.091 秒**。整輪改善方向與索引機制一致，但相鄰輪的 cache／I/O 負載不同，不能把 23.808 秒全部歸因於索引或推成 p95。cgroup 峰值本輪約 **1.8 GiB**、各分段取樣 swap／memory.high 事件皆為 0；剩餘 **3,922,679** 個 L0 shard 待壓縮，並有 1 個原本即 deferred 的 query view。原定自然 timer 的下一輪與 archive 重新寫入後仍須複測，尤其是新索引的長期寫入成本。

本機公開閘道 `/openbb/api/status` 隨後讀到同一筆 05:10:35 L1 收據：`pending_files=3,922,679`、`new_segments=19`、`active_segments=152,741`；OpenBB archive 主程序維持 PID 5880／`NRestarts=0`。這驗證了「服務完成 → 收據 → 本機 API」串接，不代表外部 IPv6 可連或所有 OpenBB 來源已獲取完畢。

## 9/25 05:17～05:34 TAIFEX 期權解析與輔助排程恢復

官方期權 CSV 原本對每個契約列建立 `DictReader` 字典；月／週 ATM 與完整鏈各讀一次同一來源。改為每個來源檔只建立一次標頭欄位索引，逐列仍保留重複標頭取最後欄、短列缺值、額外欄忽略及原本缺值／衝突判定。單一 2025 年官方 ZIP（18,743,862 bytes）月序列解析 **7.016 → 4.396 秒**、週序列 **6.192 → 3.940 秒**；兩邊列數（223,298／108,238）與各自結果摘要完全相同。2001 年 ZIP 及 2026-09-01～09-22 CSV 的月／週也與 `HEAD` 原解析器逐列摘要相同。這是解析局部測速，不等於四個衍生建置整輪的純收益。

05:17 用原 systemd unit 正式執行後，34 份來源收據通過，完整期權衍生建置 **252.630 秒**：來源收據 8.066、月 ATM 60.139、月完整鏈 80.111、週 ATM 49.366、週完整鏈 53.655 秒；相鄰較早正式輪次為 740.978 秒，但同時包含先前的空轉冷卻、日期解析快取及不同 OS cache／負載，**不可將差值全歸於本次 CSV 改動**。本輪月資料 6,083／6,096 可執行、週資料 2,920／3,382 可執行；不可把不足列解釋成解析器效能問題。原服務接下來因逐筆頁面只有 29 個共同日期而退出 1，且 `set -e` 令最後結算階段被跳過。

05:24 的兩個官方逐筆頁面各列 30 條連結，但包含本機當時仍屬未來的 **2026-09-29**；排除當日尚未完成及未來日期後，只有 8/17～9/24 的 **29 個共同已完成交易日**。新流程不把 9/29 或未完成日期算入「最近 30 日」。如果這 29 日**恰好**等於既有完整 30 日 manifest 的後 29 日，先逐一驗證既有 60 個原始 ZIP 的檔名／內容雜湊／成員及 60 個正規化 partition 的收據／內容雜湊，再保留該 manifest 和原有 8/14～9/24 視窗，寫 `state/recent_listing_status.json` 的 `waiting_next_publication`、頁面摘要與被排除日期；任何來源或收據驗證失敗則 `blocked_incomplete_listing` 並非零退出。這是在等待下一筆**可驗證的官方檔**，不是把 29 個新日期宣稱已更新成新的 30 日。

原 wrapper 現將期權、逐筆、最後結算當成獨立階段記錄退出碼與耗時；任一階段失敗仍讓其他階段嘗試，總退出碼維持非零。故下一次逐筆來源不足不會再次無故跳過結算。05:33 原 unit 重新執行 **2.779 秒 wall／1.615 秒 CPU／97.1 MiB 峰值**，三階段各退出 0：期權直接沿用相同來源且雜湊驗證過的產物（0.343 秒）、逐筆 `waiting_next_publication` 保留 SHA-256 為 `58e41c2a1b4af3126ef17fba406f14cb39f319468532e4eae5a26b43ed060ff5` 的完整既有 manifest、最終結算產出 777 筆官方 TXO 結算（2012-11-21～2026-09-23）。`systemctl` 最終為 `Result=success`；這只證明本輪三階段成功或明確等待，**不代表今天的新逐筆 30 日視窗已產生**。相關期權／逐筆／期貨整合 **64 passed**，另外 20 個逐筆鄰近測試、Ruff、`bash -n` 與 whitespace 檢查通過。自然排程在下一個新官方逐筆日期出現後仍要驗證：新完整 30 日 manifest、報表／partition 行數、接續狀態及服務 wall。

05:35 本機全資料監控仍將「TAIFEX 台指期權逐筆成交」群組列為 `blocked`，不是因新 wrapper 的 exit 0 就誤判為完整：tick 子端點為 `complete`／data-through 9/24，但已註冊的選擇權 1m materializer 尚未接入可執行管線，必要子端點為 `unable`。這是**不同的既有缺口**；本輪未製造 1m 資料，也未掩蓋它。

逐筆收據另在真正寫入新完整 manifest 後才從 `listing_ready` 改記 `complete`，並附新 manifest 雜湊；現場這輪仍是 `waiting_next_publication`，不能因原 systemd unit 退出 0 或完整的**舊** 30 日 manifest 而推論新日期已完成。追加修改後的逐筆／選擇權相鄰測試 **17 passed**。

## 9/25 05:47～05:51 全資料監控的實際週期成本

30 秒 supervised snapshot 的現場 `data_monitor_timing` 約 **2.3～4.3 秒／輪**，大輪會重建 **87,606 個欄位**；這是背景生產成本，不能用公開 API 的 9 ms 快取命中替代。對現有 `record_inventory_cache.json`（約 36 MB）、**56,988 個實體 Parquet** 做唯讀剖析，檔案發現約 163 ms、JSON 解碼約 548 ms、重讀各組聚合約 770 ms；cProfile 增加追蹤成本，因此只作歸因。自然 timer 採用新增的細分計時後，05:47:39 一輪實測總 **4,212 ms**：inventory **1,167 ms**（cache 解碼 478、發現 424、簽章掃描 262）；feature 階段 **1,964 ms**，其中來源欄位驗證與聚合 **968 ms**，剩餘時間含公開欄位投影、JSON 寫入與其雜湊收據。這些時間是單輪，不是 p95。

現在每輪 `data_monitor_timing` 都附 `inventory_stages_ms`；重建欄位時再附 `feature_inventory_stages_ms`。它們只測既有工作步驟，不改來源核對或公開輸出契約；相鄰 **77 個資料監控測試通過**。公開欄位投影另將同一 dataset 的來源標題、供應商及市場分類解析共用：回歸測試以同資料集 1,000 欄位證明分類從逐欄重算降為一次，仍保持逐欄輸出內容。現場整輪受其他負載和來源 JSON 寫入影響，尚不能宣稱這項小改動帶來可量測的 p95 改善。測試中的 MsgPack 對 36 MB 快取僅比 JSON 解碼少約 **0.1 秒**，因此沒有引入第二套持久快取格式。服務趨勢採樣失敗的日誌只輸出例外類型，不再原樣輸出可能含私有路徑的錯誤字串。

自然 supervised run 於 **05:49:43** 以新版欄位投影重建 snapshot，此後兩輪重用收據均顯示 `features=87,606`、`refreshed_files=0`、無 traceback；本機唯讀 `/data-monitor/api/features` 回 HTTP 200，gzip 約 **1,401,993 bytes／4.6 ms**。這只證明本機 API 快取命中與功能可讀，不是瀏覽器解析、公開 WAN 或長期尾延遲驗收。獨立的 1m materializer、Binance archive 磁碟保留及全庫資料健康仍未解，整體目標維持未完成。

補跑公開 gateway、資料監控、實存清冊與長期服務趨勢整合回歸 **181 passed／19.55 秒**，Ruff 與 diff whitespace 通過；此覆蓋仍不是全庫測試。

## 9/25 06:00～06:08 Windows／WSL 啟動鏈的非零任務結果

新唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0600-current.json` 仍列 **50 service／39 timer／2 path**；`systemctl --failed` 為空，但這只說明目前沒有 systemd 失敗單元。Windows `StockAgent Public Caddy` 排程為 `Running`、上次任務結果 `0x800710E0`，同時 Caddy 有兩個程序，Windows 對本機全球 IPv6 位址的 443 TCP 測試成功，WSL 內部 8770 `/healthz` 為 200，公開 IPv4 HTTPS `/healthz` 也為 200。**不能把非零 LastTaskResult 直接當成 Caddy 已停，也不能用網站現在可讀證明重開機自動恢復。**

查到該任務使用 `MultipleInstances=IgnoreNew`、有每分鐘重複觸發及 boot／logon 觸發；[Microsoft 的 Task Scheduler 文件](https://learn.microsoft.com/en-us/windows/win32/taskschd/taskschedulerschema-multipleinstancespolicy-settingstype-element)定義 `IgnoreNew` 在既有執行實例仍在跑時不啟動新實例。這使該非零碼與被拒絕的重複觸發**相容**，但 Task Scheduler Operational history 目前為 disabled，尚無同一事件的逐條證據，不能斷言此碼必定只由 IgnoreNew 造成。唯讀稽核現記錄 `multiple_instances_policy`、`repetition_intervals`，並把此組合標成 `ambiguous_nonzero_with_ignore_new_repetition`，保留非零原始結果，不把它改綠。現場新版稽核輸出符合預期；相鄰稽核測試 **21 passed**。

WSL 最慢的已記錄恢復事件為 **274.03 秒**：首次 gateway dispatch 到當前 VM 網路驅動載入約 **259.858 秒**、到 WSL userspace 約 **260.353 秒**，userspace 到 HTTP 健康約 **13.143 秒**；其間舊版啟動日誌記 18 次 WSL dispatch。這是 **9/24 的 WSL 恢復**，不是已重現的 Windows 全機冷開機，也不能據此認定重複 dispatch 是 260 秒延遲的原因。新版 launcher 會記錄每個 `wsl.exe` dispatch 完成的退出碼與命令耗時；唯讀稽核現在另保留最近完成紀錄及七日完成數，不把 `wsl.exe` 退出 0 當成 VM 或 gateway 已就緒。現有最近紀錄為命令退出 0／0.186 秒，而後端健康仍須分開驗證。

這台 WSL 的全球 IPv6 `/128` 恰與 DDNS AAAA 相同，但 WSL namespace 沒有 443 listener；因此從**這個 WSL** 對域名的 `curl -6` 立即連線失敗，而 Windows Caddy 對 `::`:443 監聽、Windows 本機 IPv6 TCP 測試成功。前者是同址本地測試，**不足以判定外部 IPv6 WAN 路徑**；仍須獨立外網 IPv6 節點檢驗路由器、Windows 防火牆及 Caddy。沒有調整 DDNS、路由器或防火牆，也沒有宣稱雙棧已完成。

啟動鏈／唯讀服務趨勢整合 **35 passed**，Ruff 與 diff whitespace 通過。Windows 任務 Operational history 未啟用、外網 IPv6 未獨立驗證、未做冷重啟；這三項維持明確未驗收。

06:09 本機全資料監控 `/data-monitor/api/summary` 仍是 `critical`：327 個必要資料端點中 **65 unable、58 catching_up、204 complete**。故網站與 Caddy 可連不等於所有資料服務健康；此輪未更改任何資料健康狀態或交易帳本。

## 9/25 06:23～06:30 公開特徵全量建置的合併成本與故障邊界

正式 9/24 特徵摘要記錄 9,606,617 列、143 個特徵；`stock_merge` 約 42.9 秒。19:00 與 19:30 兩次任務之間的官方來源收據確實變動，下一輪 dry-run 相對已建版本也有變動來源；**不可**只因同一交易日就略過重建。來源任意舊列可能修訂，現有完整來源雜湊、產物原子替換及建後重驗仍保留。

原實作對已去重的 16 個股票特徵表逐一做 full join，每次都重新建構、合併逐漸變寬的鍵集合。現對首表至少 10 萬列且至少 3 個有效表的情況，先取一次所有 `(date,symbol)` 鍵聯集，再做 left join；小型／兩表路徑保留原 full join。來源表仍逐一用原規則正規化與去重；第一個同名欄位優先、任何後續來源獨有的鍵都保留，最後的正式輸出仍按鍵排序。`stock_join_keys` 與每段 join 秒數進入既有階段收據。

以**同一份真實來源**、相同 `2026-09-24` 截止及相同當前 **2,757** 檔股票 universe，在獨立暫存目標依序建置原合併器與新合併器：兩者均為 **9,606,617 列**，Parquet 內容 SHA-256 均為 `40a9d2d98c16874dcc906539ab8273f11675e8877ece038fb1c9ab985b2f5d23`。原版 `stock_merge=45.898s`、整個 canonical 特徵建置 `146.298s`；新版為 **24.464s、112.446s**，分別少約 **46.7%** 與 **23.1%**。兩份 receipt 的 `name` 不同，故完整 receipt 物件不相等；檔案內容雜湊才是此處的位元組等值證據。暫存輸出在測試結束後移除，正式特徵檔與交易帳本未動。這是相鄰兩輪的觀察值，不是 p95、來源抓取、研究衍生層或整個 19:30 service wall 的承諾；後續仍須比較自然排程的記憶體峰值與完整收據。

本次先試的「少數重複鍵只局部聚合」在真實 margin 混合表**不適用**：盤後特徵與同日規則證據本來大量共鍵，該捷徑較慢，已撤回；沒有把退化實驗留在正式路徑。新增測試覆蓋大量鍵、後續來源獨有鍵、同名欄位優先及少量重複鍵的末筆非空值。公開特徵／reconcile 相鄰回歸 **94 passed**，PyCompile、Ruff、`git diff --check` 通過。

另一個獨立程序、相同來源的新版全量建置仍產出同一 SHA-256，但受同時其他工作負載影響，`stock_merge=37.058s`、整輪 `154.748s`，不能把前次 112.446 秒當穩定延遲或 SLA。該程序的 `ru_maxrss` 峰值為 **44.623 GiB**，低於目前服務 `MemoryHigh=48G`、`MemoryMax=64G`；這只是程序 RSS，不包含 systemd cgroup 的檔案快取與其他計費，也未量到自然排程的峰值。正式排程接續要追 `MemoryPeak`、memory.high 事件及完整 service wall；若長期尾延遲或記憶體壓力退化，需回查鍵聯集大小與回退門檻。

06:30 唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0630-resilience.json` 仍涵蓋 **50 service／39 timer／2 path**，5.18 秒樣本沒有已觀察到的 memory.high／OOM 增量，`systemctl --failed` 為 0。六個本機產品 GET 都 HTTP 200，但實際健康仍為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料 `critical`；Binance 公開 archive 最近 exit 75 的磁碟保留閘門也未解除。這項計算優化不會自動修復來源缺口、盤中成交證據、Windows 真冷啟動或 IPv6 外網路徑；整體健壯性目標仍未完成。

06:37 嘗試只啟用 Windows `Microsoft-Windows-TaskScheduler/Operational` 的既有 10 MiB 循環事件日誌，以便區分 Caddy 任務的 `0x800710E0` 是否真為 IgnoreNew 重複觸發；目前 WSL 呼叫的 Windows 使用者沒有提升權限，`wevtutil` 回 `Access is denied`。重讀設定仍為 `enabled: false`，**沒有啟用**、沒有重啟或修改 Caddy／WSL；需要 Windows 管理員另行啟用並用同一任務事件驗證，不能在缺證據下將非零結果改稱正常。

## 9/25 06:36～06:45 註冊 daily 真實失敗與新掛牌尾端修復

06:30 的 `stockagent-registered-data-daily.service` 於 06:36:09 以 exit 1 結束，正式同輪 `registered_daily_runs.tsv` 明記 `completed_with_failures`、`yahoo,okx_perpetuals`；`systemctl --failed` 因此由 0 變成 1，不能沿用較早的健康快照。Yahoo 群組在開始一秒內因腳本 `set -u` 引用未定義的 `repo_root` 而退出，與供應商資料速度無關。已改用既有 `ROOT_DIR`，並加入只載入腳本、覆寫 `run_step` 的無網路回歸測試，證明 Yahoo 摘要路徑在 nounset 下可解析；沒有重跑數萬筆 Yahoo 來源，因此**本次 Yahoo 資料未宣稱補齊**、原失敗收據保留。

OKX 同輪 492 個合約中只有新掛牌 `KII-USDT-SWAP` 失敗，訊息為 `No rows in requested date range.`。官方即時 instrument API 的上市時間為 **2026-09-24 11:00 UTC**，history-candles API 當場有已完成 1m K；問題在 `tail_only` 對**尚未到來的要求日期 UTC 23:59**倒推 24 小時，台灣清晨執行時起點落在目前已收盤 K 線之後。將新標的尾端起點改為「`min(要求終點, 最新已收盤 K 時間)` 減 24 小時」，並讓 Bybit 的分頁終點也不超過已收盤時間；OKX、Bybit、Binance 同型路徑與新掛牌回歸各自覆蓋。既有標的的增量起點及來源缺失的 fail-closed 狀態未改。

用 OKX 正式來源先在獨立暫存目標驗證 `KII-USDT-SWAP` 取得 **703 列**，再用原 OKX 下載器、原排他資料鎖與官方節流做正式尾端重驗：**492/492 updated、0 failed、59.506 秒**；新掛牌正式檔有 **704 列**（兩次觀察間新增一分鐘）。這個成功是新的一輪來源收據，不會把 06:30 的失敗歷史改寫成成功。相鄰 crypto／來源收據回歸 **88 passed**；Yahoo 尚待不干擾開盤的完整更新或下次自然排程驗收，`stockagent-registered-data-daily.service` 的舊失敗狀態仍可見，未執行 `reset-failed`。

合併本輪公開特徵、三家加密下載、資料收據的相鄰回歸 **182 passed**，PyCompile、Ruff、`bash -n` 與 `git diff --check` 通過。06:47 新唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0647-after-repair.json` 仍列 50 service／39 timer／2 path；`systemctl --failed` 的唯一項仍是上述 06:30 daily 執行，六個產品本機 GET 仍 HTTP 200，但其業務健康仍是 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料 `critical`。未把 OKX 單來源修好等同整輪 daily 或所有產品恢復。

## 9/25 06:46～06:52 OpenBB L1 自然排程的分段驗收

本輪自然執行成功，wall **340.660 秒**、CPU **129.178 秒**、20 個新段、0 stale／failed、L0 未刪除、仍有 **3,916,535** 個來源檔待壓縮；`economy.fred_series` 因 103,223 種 schema 超過 4,096 的已知邊界而繼續延後查詢 view，不能把 42 個 view 已發布解讀為全來源可查。正式收據 `data_openBB/_state/l1_compaction_latest.json` 的 `stale_source_contract_query=250.491s`、`stale_derivative_metadata_scan=49.649s`、`unassigned_source_index_order=5.470s`、`unassigned_source_load=7.143s`、`segment_build=3.470s`、`query_view_publish=5.510s`、`status_member_count=20.990s`。新保序索引已在正式輪被採用，但與先前輪次的來源量和資源爭用不同，不能直接把整輪時差歸因於它。

該 cgroup `MemoryPeak=2,686,447,616 bytes`，收據末段有 `memory.high=24,588` 累計事件、約 5.30 秒累計 memory full stall，最終 swap 約 12.9 MB；當時主機尚有約 87 GiB 可用記憶體，但大量 high 事件本身**不足以**證明 250 秒來源契約查詢由記憶體限制造成。現場 `EXPLAIN QUERY PLAN` 顯示這條查詢按段主鍵掃描成功段、按 `segment_id` 索引找 member、再按 task 主鍵逐筆查來源，最後對失效結果用暫存 B-tree 去重；它仍逐一驗證來源 task 的有效性、路徑、行數與更新時間。後續應在非開盤時段針對正式 manifest 做唯讀、相同資料與熱度的索引／查詢候選測速，驗證失效集合與原因等價及寫入、磁碟代價後才改；本輪**沒有**因為慢而略過來源或衍生檔完整性稽核，也沒有盲目調高記憶體上限。

## 9/25 07:00～07:15 公開來源鎖與休市資格的跨服務修復

07:00 `stockagent-tw-public-release-archives.service` 的開盤保護檢查通過，但原本第二道 `flock -n` 單次預檢恰與正在重試的資格 watcher 碰鎖，unit 以 `exec-condition` **跳過整輪**；這既不是成功建置，也不會由 `systemctl --failed` 提醒。鎖持有者是自 06:02 啟動的 `stockagent-tw-day-trade-eligibility.service`，原本反覆呼叫 canonical 下載器。現在來源歸檔在 Python 生產者內有界等待同一把鎖最多 120 秒、取得後重新檢查距 08:20 至少 45 分鐘的安全緩衝；仍以同一全域鎖保護來源／衍生層，逾時明確失敗。已部署 unit 模板，未重啟當沖引擎或 Discord。

07:04:48 有監控補跑，實測來源鎖等待 **2.010 秒**，總 wall **369.982 秒**、CPU 約 **1,980.136 秒**、cgroup `MemoryPeak=50,693,111,808 bytes`（約 47.2 GiB，低於 48 GiB `MemoryHigh`）、swap peak 約 84 KiB，unit 成功。正式特徵依新來源收據重建為 **9,606,617 列／143 特徵**，`stock_merge=22.728s`、`total_before_summary=109.046s`、輸出 SHA-256 `cc932c65305f7c5d25aac3001e1c7ead14ca1fad202e24749a452c9e601ee8c2`；先前正式輪相應為 42.886s、127.693s。兩輪來源版本不同，故只能說正式環境觀察到約 **47.0%／14.6%** 的階段縮短，不能宣稱位元組等價或 p95；相同來源的隔離新舊輸出等價證據見前節。

資格 watcher 失敗的真正來源日曆原因是把 09-29 主檔之前的 09-25、09-28 當成必須有當沖歷史報表的普通平日；[TWSE 官方 115 年開休市表](https://www.twse.com.tw/holidaySchedule/holidaySchedule?response=html) 明列兩天休市。新規劃只從既有 TWSE OpenAPI 官方日曆、核對 `_dataset`／`_source`／`_as_of_date` 後，跳過同一 next-session 前方已證實休市的平日；無日曆或衝突時 fail closed，不略過任何未證實開市日。資格 watcher 的 canonical 下載命令也改為使用原有 TAIEX Parquet SHA-256／收據綁定的完整實際交易日曆，只要求驗到上一個已開市日再加上 09-29 的精確官方 TWSE／TPEx 規則；不是用明天不存在的指數 K 線補假資料。對直接以 `downloader/download_tw_public_data.py` 路徑執行的 Python import 邊界加了實測回歸，避免服務啟動方式不同而快速失敗。

watcher 在 07:12:57 先以精確會員覆蓋寫出 `status=ok`：TWSE 09-29 **1,231** 檔、TPEx **843** 檔，但當時 TWSE 舊式全史狀態仍有 223 個 weekday 假缺口。其後在正式全域來源鎖及原來源節流下，以 canonical 下載器重驗兩份來源，約 **4.12 秒**，摘要 `ok=2, failed=0, coverage_complete=true`；兩份狀態均為 `checked_through=2026-09-29`、`failed_dates={}`、`missing_dates_after=0`，記錄 `same_session_official_closed_bridge=[2026-09-25,2026-09-28]`。這是下一個開市日資格，不代表 9/25 或 9/28 有交易，也不證明 9/29 開盤成交。休市／鎖／直接啟動的相鄰 **42 passed**，Ruff 與差異檢查通過。

07:15 唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0715-holiday-bridge.json` 仍有 **50 service／39 timer／2 path**；六個本機 API 均 200，但 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、全資料監控 `critical`，這些不因資格修復而變綠。06:30 `stockagent-registered-data-daily.service` 歷史失敗仍保留，已於 07:15 起以原生 systemd unit 重新執行，尚待同輪完成收據。

07:24 在 registered daily 的 Yahoo 更新負載下另存唯讀 `artifacts/benchmarks/service-coverage-20260925T0724-under-daily.json`，仍涵蓋 50／39／2。六個本機 status GET 皆 200；同一順序的 07:15→07:24 單次耗時為 TAIFEX 37.265→36.293 ms、當沖 3.577→4.200 ms、隔日沖 32.317→146.006 ms、Shioaji 331.505→378.215 ms、OpenBB 3.388→3.351 ms、全資料 10.130→9.815 ms；主機 10 秒 I/O PSI `some` 為 0→5.13%。這是**兩個單次樣本**，不能推論因果、p95 或公網視覺完成時間。單獨的公網 IPv4 HTTPS `/healthz` 此時 HTTP 200／約 521 ms，本機 `/healthz` HTTP 200／約 1.45 ms；IPv6 外網路徑與真冷啟動仍未驗收。完整公開資料下載／資格／來源鎖相鄰回歸 **198 passed**，Ruff 與差異檢查通過。

07:30 的自然 `stockagent-tw-day-trade-eligibility.timer` 一輪即成功，systemd wall **923 ms**、CPU **794 ms**、記憶體峰值約 **116.7 MiB**；正式收據 `attempt_count=1`、`status=ok`、`trading_date=2026-09-29`、`reused_existing_exact_session=true`，精確覆蓋確認約 **52.647 ms**。TWSE OpenAPI 回 200／1,231 列、TPEx 回 843 列，兩份 SHA 與 07:12 的成功收據相同。這證明自然排程重用本地已驗證資料，不證明 09-29 市場開盤後的行情或成交。

07:16:47～07:38:27 的原生 `stockagent-registered-data-daily.service` 補跑正式成功：systemd `Result=success`／exit 0／`MemoryPeak=10,308,947,968 bytes`，同輪 TSV `registered-daily-20260924T231647202328080Z` 為 `completed`、wall **1,300 秒**、無失敗步驟，逐步驟收據 **16 complete／0 failed／0 running**。先前實際失敗的 OKX 1m 步驟本輪 **59 秒／exit 0**；Yahoo US stocks **1,281 秒／exit 0**，來源摘要的 run ID 精確匹配本輪，仍明列 **12 failed + 618 lagging_skip = 630** 個未解標的。`stale=12,219` 是修補前計數且同輪 `repaired=12,219`，不能再加到未解缺口。這次只證明 runner 與已修的尾端日期／腳本路徑正常，不證明 Yahoo 美股來源完整；06:30 的原失敗歷史保留，沒有 `reset-failed`。

補跑後唯讀 `artifacts/benchmarks/service-coverage-20260925T0740-post-daily.json` 仍覆蓋 **50 service／39 timer／2 path**，`systemctl --failed` 為 0；六個本機產品 GET 均 200，業務健康仍分別為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料 `critical`。全資料監控 327 個必要端點中 **65 unable、37 catching_up、225 complete**；磁碟仍約 95% 已用／可用約 102 GiB，Binance 公開封存的 10% 安全保留問題未解。補跑後 10 秒 I/O PSI `some=0`，不代表長期低延遲或所有來源就緒。

## 9/25 07:45～07:52 OpenBB L1 來源稽核的反證測速與正式觀測點

07:23 的自然 L1 輪次在 07:29:36 成功結束，總 wall **356.844 秒**／CPU **130.685 秒**、20 個新段、0 stale／failed，但 `stale_source_contract_query=267.209s`，另有衍生 Parquet metadata 掃描 51.249s。這是 17.6 GB SQLite manifest 的完整來源契約稽核；不能藉跳過稽核取得假速度。

新增唯讀 `scripts/benchmark_openbb_l1_stale_scan.py`，在同一 SQLite 讀取交易比對完整查詢及回傳列 SHA-256。候選「先順序掃成員、再查段」耗 **69.478 秒**，原「先掃段、依段索引取成員」耗 **20.458 秒**；兩者都是 0 個失效列，SHA 同為空集合雜湊。因候選更慢，**正式查詢沒有切換**。這組 A/B 候選先跑、原版後跑，cache 熱度不同，不是 p95；收據為 `artifacts/benchmarks/openbb_l1_stale_scan_20260925.json`。

以與正式 unit 相同 `MemoryHigh=2560M`、`MemoryMax=3G`、CPU／I/O 權重建立獨立 transient systemd unit，只做原版查詢：**19.264 秒**、wall **19.617 秒**、CPU **19.566 秒**、記憶體峰值 **51 MiB**。再測 SQLite `cache_size=128 MiB`：原版查詢 **21.925 秒**、wall **22.340 秒**、記憶體峰值 **182.8 MiB**，未見效益，因此未改正式 cache 設定。兩者回傳 0 失效列、同一 SHA。這個熱資料唯讀快測不能解釋自然輪次的 267 秒，更不能歸因於 memory.high；cache 冷熱及同時工作負載仍待正式輪檢驗。對照收據為 `artifacts/benchmarks/openbb_l1_stale_scan_lowmem_20260925.json` 與 `artifacts/benchmarks/openbb_l1_stale_scan_cache128_20260925.json`。

正式 L1 現將既有 `stale_source_contract_query` 再細分為「既有 stale 段查詢」與「來源契約 JOIN」，並在稽核前後記錄本程序實際讀／寫位元組、記憶體與 cgroup stall 事件；不變更失效判定、來源讀取或輸出。既有 stale 段查詢在當下熱資料單獨測為 **0.011 秒／0 列**。下一個符合開盤保護與來源空閒條件的自然執行，才可確認 267 秒是 JOIN 本身、冷磁碟讀取或環境負載；未完成前不提高記憶體上限、不換索引，也不將唯讀 20 秒當作正式服務已優化到此水準。

08:01:02 的下一個 timer 觸發被原有開盤保護 `ExecCondition` 正常跳過（`Result=exec-condition`、正式 compactor 未啟動）；即使今天官方休市，這道共用時間保護目前仍按平日作用。沒有為了跑測速修改或繞過它；下一筆正式稽核收據尚未產生。

## 9/25 07:53～08:00 Shioaji 面板快取上界與閘道就緒

Shioaji 公開頁讀既有本機收據與 systemd/journal，**不登入永豐，也不發 API 歷史查詢**。本機單次冷 HTTP GET 約 **392 ms**、立即重請求約 **1.5 ms**；獨立直呼的冷來源工作：合約 manifest 769 份約 54 ms、最近 capture receipt 約 87 ms、並行本機 service/journal 約 182 ms；各項熱讀為約 26／6／28 ms。這是不同進程、不同 cache 狀態的局部樣本，不能直接相加或宣稱 p95；數百毫秒冷建置並未因本次 LRU 修正消失。短期快取與 stale-while-refresh 已存在，沒有為降低首訪秒數而提高來源過期容忍或額外每秒輪詢券商。

發現程序內 JSON 收據快取以前隨歷史路徑無上限增長；同一輪狀態建置即讀入約 **1,100** 個條目。現用有鎖的 **8,192 條 LRU** 上限，命中仍驗 `dev/inode/size/mtime_ns/ctime_ns`，超限只逐出可重讀的快取，不刪原始資料，且回歸測試涵蓋逐出後檔案變更重讀。這是長期記憶體穩定性保護，**不是**已證明的當前 API 降延遲。

07:57:29 第一次僅重啟唯讀公開閘道時，`systemctl` 立刻回 `active`，隨後立即打本機 `/healthz` 一次得到連線拒絕，約 3 秒後 Python 才監聽；這暴露的是 **Type=simple 的啟動就緒競態**，不是交易或永豐連線中斷。新增固定 loopback `/healthz`、最長 60 秒的 `ExecStartPost`，配套 `TimeoutStartSec=70s`。`systemctl restart` 現在等到端點 HTTP 200 才回成功；07:59:35～07:59:37 的第二次正式重啟確認日誌為 `Starting`→`listening`→`Started`，其後本機健康 200、公開 IPv4 HTTPS 健康及 Shioaji API 均 200，未重啟行情或交易服務。這改善**啟動結果的真實性**，並非零停機切換；重啟期間仍可能短暫無法連線。

08:00 新唯讀覆蓋收據 `artifacts/benchmarks/service-coverage-20260925T0800-gateway-readiness.json` 為 **50 service／39 timer／2 path**，`systemctl --failed` 為 0。本機六產品端點皆 200，但業務健康仍是 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料 `critical`。該輪 Shioaji 本機 GET **174.399 ms**，是單一不同 cache 狀態樣本，不能拿來宣稱從 392 ms 穩定減半。相關公開網頁／Shioaji／OpenBB 測試 **158 passed**、Ruff、Shell 語法與差異檢查通過。Windows 任務非零結果、外網 IPv6、真正冷開機及上述資料健康缺口仍未消除。

08:01 唯讀檢查獨立 D: 冷備份最新 `current_heads` 收據仍是 `current_heads_complete=true`、`remaining_bytes=0`，但當輪 `metadata=20.766s`、`destination_trust=11.757s`，與先前約 10／9 秒不同；DrvFs 與並行 I/O 負載未固定，不能把先前加速當成永久上限。這是下一個需要拆分 manifest 與 head 驗證成本的測速點；本輪沒有中斷正在運行的備份服務或跳過校驗。

## 9/25 08:04～08:18 D: 備份 metadata 與休市守護鏈

新增唯讀 `scripts/benchmark_packed_backup_metadata.py`，從當前 121 個 head 選出 head/manifest 共 **242 檔、3,452,489 bytes**，用 A-B-C-C-B-A 順序比較既有 `safe_path`、每檔 no-follow FD 與目錄 FD 重用，六次檔案內容摘要均為 `c2206b859fe7e98eb4f83100e3f64225973e4600336c564bc81c3a5025e0b4fd`。較接近正式程式的每檔 FD 路徑在一輪測得 **1.846／2.066 秒**，既有路徑 **4.352／4.048 秒**；更快的跨檔目錄 FD 重用 **0.912／0.929 秒**尚未進入正式服務，因其跨檔 pin/recheck 的維護與競態成本需要獨立驗收。這只是當前 head metadata 讀取，沒有測全備份 p95 或恢復。

正式備份只在**既存、完全相同**的 metadata 使用 no-follow FD 快路徑：從根到檔案逐級禁止跟隨符號連結、讀前讀後檢查檔案 signature、最後確認每級目錄仍是原 inode。缺檔、變動及寫入繼續走既有 volume guard、不可變版本檢查、head-history、原子寫入與 readback；不改冷備份保留或刪除語意。38 個相鄰備份／保留測試通過。重啟的只有獨立 `stockagent-packed-backup.service`，沒有重啟交易、行情或 Discord。正式 08:10 與 08:15 兩輪 `current_heads` 收據均 `up_to_date`、`integrity_state=verified`、121 heads／121 manifests／11,834 objects、463,950,911,986 已驗證 bytes、0 剩餘與 0 錯誤；最新 `metadata=5.870s`、`destination_trust=9.433s`、整個前段 `16.405s`。相對 08:01 的 20.766s metadata 為觀察改善，兩輪資源競爭不同，**不能**宣稱穩定 p95；歷史完整度仍 `not_checked`。

08:10 發現 `stockagent-tw-public-0830-check.service` 真實失敗：今天 09-25 為官方日曆已驗證的中秋節休市日，程式仍要求「今日」TWSE／TPEx 當沖資格、跑 >100 秒嚴格稽核，因精確來源實為下一開市日 09-29 而失敗。08:30 驗收改為先調用與開盤就緒檢查**同一交易日契約**；已驗證休市寫 `skipped`、資格／模型稽核欄位均為未通過，未知／衝突日曆仍失敗，不抓假資格也不跑昂貴稽核。08:15 自然 timer 留下 `session_state=closed`、`steps=[]`、同日收據，unit 成功、`systemctl --failed` 清零。守護程序原本只按「平日」判斷，因此每分鐘又重觸發資格及 08:30 驗收、把休市誤報為未就緒；現沿用同一官方契約，已驗證休市不觸發，不暫停背景工作，未知日曆明列失敗。08:17 自然 guardian 收據 `session_state=closed`、無上述兩個警報，只剩磁碟空間警告。相關公開資料／guardian 測試共 **94 passed**；這不代表下一開市日驗收通過。

前一次 08:08 嚴格稽核的真實 `model_safe=false` 仍保留在原 audit 目錄，`findings.csv` 有 **1 critical／1 medium**。critical 是 2026-05 TPEx 一則富邦基金受益憑證 `T1001Y` 的收益分配公告被誤當成未解析股票事件；下載器現在辨識明示的字母前綴證券代碼並把此類**有標記且不在股票／ETF 執行 universe** 的公告列入可追溯的 out-of-universe，而不是隱藏未解析資料。這條規則的 46 個相鄰測試通過。08:21 首次正式重驗因另一個研究特徵工作持有 canonical 全域來源鎖而在 60 秒上限退出，**未搶鎖、未中斷生產者**。待 08:20 正常啟動的特徵工作結束後，08:23 以原下載器、原兩層來源鎖與 10 req/s 共享節流重驗 2026 年：`announcements=1503`、`failure_count=0`、`unparseable=0`；原公告以來源 URL、文號和主旨留在 `out_of_universe_records`，沒有進入股票訊號。正式收據的 `coverage_complete=false` 仍忠實反映歷史交叉覆蓋限制。medium 是官方下市公告交叉覆蓋不足，屬真實歷史來源限制，仍須保留。

同輪發現 08:30 嚴格稽核快取指紋以前未納入 `tw_short_sale_download_report.json`：若該來源直接更新、全球下載摘要未變，可能錯誤重用舊的 `model_safe=true`。現在將放空規則及 TAIEX 官方摘要納入依賴指紋；實檔變更會讓下輪嚴格稽核重新執行，新增隔離回歸確認這一點。**這是快取正確性修復，不是稽核通過證明。** 08:25 使用與 08:30 相同參數的獨立嚴格稽核約 3 分 45 秒、退出碼 2；原 `unparseable` critical 已消失，但當天 08:20 的特徵檔早於 08:23 的放空規則來源修正，`stale_feature_build_receipt` 成為新的 critical，`model_safe=false`，原始失敗收據保留。它也觀察到全量 panel 冷建約 **109.937 秒**、大量記憶體與可見 PSI 壓力；非公開網站 API 的延遲。08:30 用原生 `stockagent-tw-public-feature-reconcile.service` 重新建特徵與研究輸出，`Result=success`、cgroup `MemoryPeak=51,543,433,216 bytes`、沒有 OOM；正式特徵 `build_mode=full`、`incremental_fallback_reason=base_contract_not_verified`、9,606,617 列。獨立的 `audit_feature_build_receipt` 六項檢查（來源 bytes、輸出 bytes、schema、availability、universe）全通過。這仍不是完整稽核；08:33 在 48G MemoryHigh／64G MemoryMax、低 CPU／I/O 權重的獨立 transient unit 跑同契約的完整複驗，待 `summary.json`／exit code 驗收。

08:33 的正式參數複驗已於約 **3 分 59.665 秒**完成，systemd exit 0；`artifacts/data_refresh/tw_public/0830/audit/20260925T0833_final_short_rule_recheck/summary.json` 為 `model_safe=true`、**0 critical／0 high／1 medium**，放空來源六項與特徵來源六項收據均通過。唯一 medium 仍是歷史下市公告交叉覆蓋不足，不能消去。這是**09-25 08:33 當時資料與稽核程式**的合格收據，不是 09-29 開盤成交或所有資料來源健康。複驗 cgroup 峰值約 **48 GiB RAM／8 GiB swap**，`memory.high` 事件逾七萬；panel 冷建約 **114.873 秒**，整輪另有約兩分鐘成本尚未分段定位。為後續精準優化，嚴格稽核下一輪的 `summary.json` 現會附每一主要階段耗時、程序 RSS 當前與最高水位；純觀測，不略過任何驗證。**新觀測欄位尚未經下一輪正式稽核產出**，不能把 08:33 既有 summary 當成有分段的新格式。

複驗後唯讀 `artifacts/benchmarks/service-coverage-20260925T0838-post-audit.json` 保持 50 service／39 timer／2 path，`systemctl --failed` 為 0，六個本機產品 API 均 HTTP 200；真實產品健康仍分別為 TAIFEX `blocked`、當沖 `degraded`、隔日沖 `waiting`、Shioaji `waiting`、OpenBB `active`、全資料監控 `critical`。從本機發起的公開 IPv4 HTTPS `/healthz` 為 HTTP 200／單次約 821 ms，不等於獨立外網或雙棧驗收。複驗負載中單次本機全資料摘要約 1.17 秒、結束後覆蓋探測約 3.455 ms；快取與資源狀態不同，不能直接當成穩定改善。併跑相關資料稽核／公告來源／公開驗收／guardian／備份回歸 **227 passed**、PyCompile、Ruff、差異檢查通過；仍非全庫測試或真冷開機。

08:42 另以**相同資料、同樣嚴格稽核參數**驗收分段觀測版：unit exit 0、`model_safe=true`、0 critical／0 high／1 medium；panel 命中對應來源的合法快取，因此是**熱 panel 對照**，總 wall **1 分 58.455 秒**、峰值約 **6.6 GiB RAM／0 swap**，不能拿來冒充失去 canonical cache 時的冷建速度。`summary.json` 的分段耗時：逐檔行情來源 **42.807s**、TWSE／TPEx tick 格線 **33.733s**、來源與特徵收據 **11.969s**、歷史來源 **9.692s**、標的／價格 provenance **8.522s**、公開特徵表 **3.464s**、panel 快取載入 **3.194s**。此輪驗證證明新觀測欄位可產出且不改 finding；後續建議先針對 42.8s 與 33.7s 各自優化。逐階段現在也輸出短日誌，讓被中止的執行仍有最後完成階段的證據；該新增日誌尚待下一次自然執行驗收。

價格格線的 33.7s 中，安全類型判定原本每列重複呼叫純函數。唯讀 A-B-B-A CPU 基準各取**真實 TWSE／TPEx 前 100 萬列**，逐列結果 SHA-256 在四輪皆相同：TWSE 直接 **0.802／0.806s** 對 bounded-cache **0.195／0.171s**（891 種鍵）；TPEx 直接 **0.940／0.993s** 對 bounded-cache **0.233／0.201s**（3,821 種鍵）。這僅是載入記憶體後的分類 CPU，**不代表完整格線或全稽核可同倍加速**。正式格線稽核已改為單檔 16,384 筆 LRU，鍵保留 `(symbol,name)` 以維持未來可能的名稱規則；價格數值、例外與檔案 SHA 的判定均未改。兩份**完整正式來源檔**再次唯讀執行：TWSE 5,329,567 列 **16.051s**、TPEx 4,419,006 列 **13.272s**，合計 **29.324s**，兩份回傳物件與修正前正式 `summary.json` 的格線欄位**逐欄完全相同**，皆 `quote_grid_valid`。相對前次 33.733s 階段觀察約少 4.4s／13%，但這不是同程序配對的完整 A/B 或 p95，不能全歸因於快取；CPU 前綴 A/B 才隔離了分類函數成本。格線相鄰測試 **59 passed**。

## 9/25 08:53～09:00 跨服務韌性與開盤資源保護

逐檔股票行情來源稽核另以全部 **2,757** 個真實來源檔做唯讀 A-B-B-A：64 workers 為 **43.640／45.579 秒**，16 workers 為 **43.522／43.314 秒**；完整 profile、摘要與 finding 的 SHA-256 四輪完全相同。降低到 16 workers 沒有可重現的吞吐改善，故**沒有**變更正式併發設定，也不以單次秒數推導全服務 p95。基準程式為 `scripts/benchmark_tw_public_quote_audit_workers.py`。

根檔案系統現約 **94.95% 已用、102 GiB 可用**。`scripts/maintain_storage_pressure.py` 的唯讀完整盤點為 `under_pressure=true`，但符合已批准的舊編譯快取檔案 **0 筆／0 bytes**；收據 `/var/lib/stockagent-storage-pressure/receipts/storage-pressure-audit-20260925T005514.349997Z.json`。packed retention 最近的已驗證實際回收為 **532,140,032 bytes**，相對 2 TB 檔案系統的 10% 保留缺口不夠。**沒有**強制清除資料、降低 Binance 封存空間閘門或修改冷備份保留政策；這個來源工作仍受容量限制，須以具體 catalog／D 驗證／程序參照及擴容計畫解決。

08:30 驗收原只在 **08:50** 後禁止*啟動*大型衍生重建／完整稽核；已量到這類工作約數分鐘，08:45 timer 若才啟動，就可能跨越 08:50 並與 09:00 訊號爭資源。現在將這四類非必要大型步驟的**啟動**保護提前到 **08:40～09:05**；同一份快速、已驗證的稽核仍可重用，當日精確資格修復不被此規則禁止，未完成的大型工作在收據標 `deferred` 並交由 09:10 timer 重試。這是針對已測工作時長留 20 分鐘開盤緩衝，不是對任意長尾任務的硬性截止；極慢已在執行的工作仍可能跨界，後續要以自然開市日測速與 cgroup 證據驗收。前置公開來源重抓的步驟另記真實 `elapsed_seconds`，避免把來源等待誤歸因於嚴格稽核。

08:59 唯讀服務覆蓋 `artifacts/benchmarks/service-coverage-20260925T0859-resilience.json` 仍為 **50 service／39 timer／2 path**，沒有 systemd failed 單元。六個本機產品 GET 皆 HTTP 200；單次傳輸時間依序為 TAIFEX **43.472 ms**、當沖 **4.066 ms**、隔日沖 **26.448 ms**、Shioaji **350.716 ms**、OpenBB **4.381 ms**、資料監控 **13.783 ms**。業務健康仍依序 `blocked`／`degraded`／`waiting`／`waiting`／`active`／`critical`。這是不同 cache 狀態的單次 loopback 探測，不是 p95、更不是券商／來源／外網延遲；HTTP 200 與 unit 未 failed 均不可宣稱所有業務正常。09:00 前相鄰公開來源／守護／稽核／備份測試 **235 passed**；PyCompile、Ruff、`git diff --check` 通過，並未執行全庫測試或真冷開機。

## 9/25 09:10～09:17 研究來源額度與公平排程

在執行中的 FinLab 研究同步實測：帳戶 5,000 MB 日額度，09:10～09:12 的 `after_market_fixed_price` 若干大欄位即使內容 `unchanged`，也各需約 23～28 秒並可消耗約 73～75 MB；09:14 的其他歷史小欄位有約 2～4 秒且僅約 0.1 MB 的例子。故每個「鍵」的 API／CPU／額度成本不等；不能從完成鍵數推算剩餘時間或宣稱可在一個額度週期完成全 1,110 鍵。SDK 的 `force_download=True` 是雲端重取，改成無條件本機快取雖快，不能作為發版閘門要求的上游檢查證據；保留此正確性界線。

真正的可修復故障是額度耗盡時的固定順序飢餓：之前所有到期的非精選既存鍵跟著 catalog 字母順序，下一個 08:00 週期又可能重訪同一前綴。現在排序為**精選鍵 → 未下載鍵 → 最久未做雲端檢查的已下載鍵**；不更改每鍵強制查來源、50 MB 明示餘量、逾時／失敗記錄、24 小時全目錄研究發版閘門或原始 Parquet。實際運行在修改後改訪前日 02:36 UTC 起的 `capital_reduction_otc`，不再只訪前日 14:25 UTC 的字母前段。相鄰 FinLab **44 passed**、Ruff 與 whitespace 檢查通過；正式輪尚在執行，最終額度／來源完整度待收據驗收，研究發版仍可能因帳號額度與未取得鍵而保持 blocked。

同時唯讀容量盤點得到 `/srv/stockagent-live` 約 **71.1 GB**、`/srv/stockagent-packed` 約 **414.2 GB**、`/srv/stockagent-packed-materialized` 約 **39.7 GB**；`/root/stockAgent` 項在 90 秒低優先權上限內未掃完，不能把這三個加總當成整個 1.8 TB 根磁碟的完整歸因。受管快取 GC dry-run 的 3 個版本為 2 pinned、1 lease-active，**0 個**可安全逐出；未刪除資料或降低封存的 10% 空間閘門。
