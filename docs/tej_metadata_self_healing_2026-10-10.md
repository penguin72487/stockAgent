# TEJ 目錄、下載故障自動恢復與驗收

2026-10-10 初稿，2026-10-13 更新，Asia/Taipei。10/10 凌晨修復的停滯不是程序停止，而是
目錄準備失敗後，介面檢查也逾時，被誤歸為未知資料查詢，使整個佇列永久等待。
10/10 下午重新盤點時，再重現 SQLite 忙碌導致失敗分類回滾、工作永久留在 `running`
的第二條路徑；已補上負證據本機對帳及原 worker 身分保留。
10/13 再修復「失敗分類已提交、次要操作註記卻回滾」的停止路徑，並在送往
桌面前拆分超出來源選取器容量的範圍，避免原封不動重試必定失敗的操作。
修復沿用同一個下載器、SQLite 佇列、Windows 操作鎖、開機 owner 與唯讀網站。
不清空佇列、不改來源值，也不把失敗、來源空回或重試列成歷史下載完成。

## 停滯原因

10/09 最後一份下載收據為 14:20:39。14:21 清點
`Delisted Option Attribute(2018~2019)` 時，來源選單通知回覆
`Binding event unresolved; no notification retry`；來源已保存精確的
`metadata_preparation_failed_before_preview`，且明確表示資料查詢尚不可能送出。
原程式有隔離目錄故障的功能，但後續 `inspect_notices` 的 30 秒檢查逾時。
最終工作仍被標為 `unknown_outcome_no_auto_retry`。

原排程的原結果恢復與授權重排只支援 `download`，不處理 `discover`；
因此雖然服務一直存活，這筆目錄工作卻無法離開全域等待。10/10 01:07
先用原隔離方法重新核對介面並恢復續抓，再部署下列永久修正。

Windows 通知逾時不能直接證明事件未送達。因此恢復必須另驗證來源的
「未跨 Preview 邊界」證據，而不是只看例外名稱或沒有結果檔。
[Microsoft SendMessageTimeout 文件](https://learn.microsoft.com/en-us/windows/win32/api/winuser/nf-winuser-sendmessagetimeoutw)
說明逾時／失敗回傳與接收執行緒的等待行為；本次保留不重複未知通知的邊界。

## 完整鏈路的恢復分工

| 邊界 | 恢復條件與實作 |
| --- | --- |
| Windows 登入、WSL 與 owner | 沿用既有開機服務與登入後互動 relay；不自動登入、不在 Session 0 操作 GUI、不另開下載管線。 |
| 目錄準備 | 新分類 `metadata_preparation_requires_recovery`。要求相同來源、完整原 request、唯一失敗事件、原不可變 bridge SHA、精確負證據；不是只憑 `active_attempt_id=NULL`。 |
| 共享介面 | 重新讀取指定 owner 的通知與介面；只有原有兩種明確的 metadata runtime notice 可以按既有許可核對。登入、授權、配額、陌生／多重通知不會任意確認。 |
| 目錄重試 | 精確證據與新鮮介面讀回都通過後，原工作排回同一佇列；60、120、240…最多 900 秒的逐表退避持久保存，重啟不歸零。 |
| 已送出或未知 Preview | 仍先採納原結果；原有明確授權重排須核對來源與 idle 介面，每筆滾動一小時最多 2 次、全域 6 次，不新增無條件重送。 |
| 正式驗證入庫 | 只有來源、scope、鍵、schema 與收據檢查通過才增加完成量。成功的目錄重試會釋放表級窗口，否則新產生的下載會一直被該窗口擋住。 |
| 失敗分類提交 | SQLite 忙碌時保留原 owner、task、attempt 與原截止時間。重新驗證原負證據和例外紀錄，只補交原失敗分類；再沿原介面核對及逐表重試，不清空佇列或把工作改成已取得。 |
| 次要操作註記提交 | 失敗分類已提交但 attempt 結束註記回滾時，另驗證原忙碌註記、唯一失敗 traffic、原 request／負證據及 bridge SHA，只補回原結束時間與未送出狀態；不重送查詢、不重設重試預算。 |
| 來源選取維度 | `bounded_source_selector_geometry_v1` 在資料查詢前檢查公司／日期輸入容量，沿用矩形規劃器拆分完整範圍，保留原欄位分區、工作量、失敗紀錄與表級退避；不是調高官方配額或本機原生上限。 |
| 操作時鐘 | Windows 來源觀測與 WSL 提交時間分屬不同時鐘，僅對精確操作窗口接受最多 5 秒偏差；本機檔案順序、唯一 request、原程式 SHA、原截止時間與 Preview 邊界仍照原契約核對。 |
| 網頁 | schema 16。顯示目錄自動恢復、逐表等待及真正的任務狀態；網站只讀取本機去識別化資料，不操作桌面或呼叫來源 API。 |

目錄恢復期間的所有 readback 都驗證時間、私有輸出位置與實際檔案內容。
隔離與重新排程之間會重驗原證據，通過前不先清除未知狀態；每一份原 request、
outcome、失敗 traffic 與不可變程式版本均保留。已知介面停用的舊 latch，只在
同一介面重新核對穩定後解除；其他人工復原或未明狀態的 latch 不會被順手刪掉。

來源檢查失敗與舊證據不符也使用持久冷卻；不在每次健康下載中重讀相同的大型
舊 request。證據讀取使用唯讀 SQLite，不為驗收執行 schema DDL。
已排回的工作到期後仍按既有資料價值／階段選取；到期不代表立即送出查詢，
不為搶先重試而壓過目前較高價值的缺資料工作。

## SQLite 寫入忙碌後的失敗恢復

下午初查時，服務仍在取得資料，最後一份收據為 15:48:06。盤點期間的長時間
唯讀查詢與錯誤分類提交發生競爭：15:51:31 來源已保存尚未跨 Preview 的負證據，
15:51:32 Python 亦保存 `MetadataPreparationError` 的原例外紀錄，但後續 SQLite
交易提交遇到 `database is locked`。交易回滾後，工作和 traffic 仍為 `running`，
worker 被縮減為沒有原 task／owner 的 `waiting_metadata`，因此健康工作也停下來。

此目錄任務沒有成功的 raw 回覆，也沒有 download 的已登記 Preview；原有兩條
恢復路徑都無法處理它。即使讀鎖已釋放，排程仍只有 `inflight_requires_recovery`，
重啟舊版本也不會解決。這是本次盤點重現的故障，不代表此前整個上午都停抓。

目前使用 DELETE rollback journal，保留 hardened gateway 的唯讀來源掛載。
長時間的讀交易可能阻擋寫交易取得提交所需的排他鎖；應縮短盤點讀交易，使用
既有 covering index 與明確 scope，不能用無條件掃描歷史 request blob。
[SQLite 官方鎖定文件](https://www.sqlite.org/lockingv3.html)
說明這個讀鎖與提交等待關係；本次沒有改成 WAL 或放寬網站來源權限。

新增 `exact_negative_plan_failure_commit_reconciliation_v1` 只做本機對帳：必須有
唯一原 traffic、完整精確 request、來源負證據、原不可變 bridge，以及對應的
原例外紀錄，時間與 scope 全部一致。它補交的是原來失敗的分類，不是成功收據、
新資料或重排許可。缺少／不符證據、尚在操作的 worker、Preview 或資料回覆
都不能走這條路徑。既有成功回覆仍優先採納，未知資料查詢保留原安全邊界。

對帳之後，在同一排程週期接續原介面核對；不多等一整個 blocked poll。
SQLite 等待註記保留相同 owner／task／attempt、原開始時間與截止時間，
不因 heartbeat 或資料庫忙碌而延長來源操作期限。正常工作與開機服務仍由原
owner 持有，不另開 GUI writer 或下載管線。

正式 owner 於 16:02:08 自行補交這筆失敗分類，16:03:19 完成介面核對並排回。
含同週期恢復修正的最後版本於 16:07:31 安全重啟，16:07:58 起已有新下載收據；
所有原 request、來源負證據、bridge manifest 和原例外紀錄都保留。

下午的針對性回歸為 **193 passed**，包含真實 SQLite 讀鎖造成提交回滾、
釋放後自動對帳、不同／缺少負證據拒絕恢復、原值和正回覆恢復不受影響，
以及恢復成功不額外等待一個 blocked poll。最後一次完整 TEJ 回歸為
**1,801 passed、1 skipped**，16 項前端測試通過；另對有限的非空保留收據
核對工具補做測試。凌晨以下列證據完成的驗收仍分別保留。

下午最後版本的證據位於
[`tej_metadata_contention_2026-10-10/`](../artifacts/data_quality/tej_metadata_contention_2026-10-10/)。
其中 [`production_acceptance.json`](../artifacts/data_quality/tej_metadata_contention_2026-10-10/production_acceptance.json)
於 16:15:51 驗證部署後真正啟動並完成的 8 份新收據，且原 request、來源負證據、
bridge manifest 與例外紀錄的 SHA 全部未變；唯讀網站當時為 `running`，排程
為 `executing`，沒有全域暫停。這 8 份均為來源明確的 `explicit_empty_scope`，
當批實際取得 0 列／0 個原始儲存格，不能當成新取得的非空資料。
[`retained_value_reference.json`](../artifacts/data_quality/tej_metadata_contention_2026-10-10/retained_value_reference.json)
另以目前 capture／ingest 版本重新核對既有的 32 列／416 個原始儲存格；
這是保留資料的原值驗證，不是部署後新增的下載量。對應核對工具的 8 項測試通過。

## Windows 與 WSL 操作時鐘偏差的恢復

10/12 初查時，最後一筆下載收據停在 10/11 18:07:45，原服務仍存活，
卻持續等待 `metadata_preparation_requires_recovery`。Windows 桌面通道可用；
卡住的是 `Delisted Option Attribute(2018~2019)` 的目錄失敗證據判斷。

原負證據的 Windows 時間為 `2026-10-11T10:10:13.7361149Z`，WSL 的失敗
提交時間為 `2026-10-11T10:10:12.678851+00:00`，前者晚約 **1.057 秒**。
原 request 唯一、原負證據檔案在本機提交前已存在、沒有 Preview 或資料回覆，
但原判斷要求來源時間不得晚於 WSL 完成時間，因此拒絕正確的未送出證據。
63 次本機證據檢查未通過，不能把這個數字當成 63 次新的資料查詢。

[`tej_operation_clock.py`](../downloader/tej_operation_clock.py) 統一使用
`bounded_cross_clock_operation_window_v1`：來源操作觀測必須落在固定本機窗口
前後最多 5 秒內，並保留原來源時間、本機開始／完成時間及窗口外偏差。
5 秒沿用既有新鮮介面讀回的偏差界線，現在雙向一致；不是新的來源流量權限。
本機檔案順序、唯一失敗事件、精確 scope、原不可變 bridge SHA 與無 Preview
證據不變。超出窗口、無時區、缺少或歧義證據仍拒絕恢復，且保存明確原因碼。
這個修正只處理操作時鐘，不改交易日、來源觀察日、特徵發布日或資料原值。

同一判斷也用於已完成目錄結果恢復、未送出 download 的負證據與介面讀回。
900 秒本機 deadline、既有單一 writer 與未知 Preview 的授權核對仍保留。
來源查詢耗時原本即使用 monotonic clock；它不受一般系統時鐘調整影響，
不能與 Windows 的 UTC 顯示時間直接相減作為來源耗時。
[Python 官方 time 文件](https://docs.python.org/3/library/time.html#time.monotonic)
說明 monotonic clock 的性質。

程式更新後只會重新檢查舊版的「本機證據驗證失敗」冷卻；真實來源／介面
失敗的重試次數和退避不歸零。原 owner 於 10/12 **09:42:07** 安全重啟，
**09:42:14** 自行核對原負證據與介面，再將原任務排回同一佇列。來源失敗
次數仍保留為 97 次，原表的下次資格時間為 09:57:14；其餘表已解除全域暫停。
沒有直接修改 SQLite 或重送原資料查詢來製造恢復結果。

此版本 **184 項針對性測試通過**，完整 TEJ 回歸為
**1,814 passed、1 skipped**，另有 16 項前端測試通過。涵蓋雙向偏差、
超過 5 秒拒絕、SQL 回滾後對帳、完成結果恢復、原檔不改寫與未知查詢不盲重送。
本輪證據位於
[`tej_operation_clock_2026-10-12/`](../artifacts/data_quality/tej_operation_clock_2026-10-12/)，
其中 `before_deployment.json` 保留原 request、來源負證據與 bridge manifest 的 SHA，
`retained_value_reference.json` 用目前程式重新核對既有的 32 列／416 個原始儲存格。
這筆既有非空資料原值驗證不計為新下載量。

[`production_acceptance.json`](../artifacts/data_quality/tej_operation_clock_2026-10-12/production_acceptance.json)
於 10/12 **09:54:04** 通過正式驗收：8 份工作均在此次部署之後真正啟動並完成，
原故障 request、負證據及 bridge manifest 的 SHA 全部未變；網站為 `running`，
排程為 `executing`，沒有全域暫停。當批都是來源明確的 `explicit_empty_scope`，
取得 0 列／0 個原始儲存格，不能算新增非空數值；另列的 32 列／416 個儲存格
仍是既有非空資料的逐值驗證。驗收工具本身不發送 provider 查詢。

原表的原生錯誤為 `Catalog axis exceeds reviewed bound`；這個目錄容量問題
仍按表保留並重試，沒有藉由提高未驗證的界線、縮減資料範圍或抹除 97 次失敗
來宣稱抓齊。恢復的是錯誤證據被時鐘偏差誤拒後的全域停滯，其餘 22 個舊隔離
工作仍保留。這輪沒有實體冷開機或全歷史驗收。

TEJ API 試用支線另於 10/12 09:37:35 正常結束，timer 持續啟用，狀態為
`waiting_quota`。本機保守預算已記錄 50,000 列，並非程序崩潰；官方用量快照
仍是舊資料，不能把本機計數當成已驗證的官方當日用量。本次不繞過配額或
另開 API 管線。

## 失敗已提交、操作註記回滾後的自動對帳

10/13 初查時，服務自 10/12 23:38:47 持續存活，最後下載收據卻停在
10/12 **15:03:08**，排程為 `waiting_recovery`。這次不是前述時鐘偏差；
卡住的是 `Contract Adjustment of Stock Futures` 的 download 工作
`49908d89036e011453f1a7f8`。

10/12 **15:03:44**，Windows 已保存 `LOCAL_SELECTION_BEFORE_QUERY: lookup batch
bound differs`，證明尚未送出 Preview；15:03:55 的原忙碌註記要求將 attempt
結束為 `proven_not_submitted`。15:03:58 的失敗 traffic 與 task 分類已成功提交，
但次要 attempt 更新遇到 SQLite 忙碌而回滾，仍為 `prepared`、結束時間為空。
原恢復器只處理未分類的進行中工作；另一條逐表退避則要求 attempt 已結束。
因此兩條路徑互相等待，即使資料庫恢復可寫也不會前進。

新增 `exact_unsent_attempt_annotation_reconciliation_v1` 在原 owner 的鎖內
核對五種原始證據：精確 request、來源未送出 outcome、原忙碌註記、唯一已提交
失敗 traffic，以及不可變 bridge manifest／release SHA。原本機開始／結束／
提交順序與來源跨時鐘窗口也必須一致，不能只用沒有輸出檔推定未送出。
只補回原 attempt 的未送出狀態與**原結束時間**，不把恢復當下時間冒充操作時間。
舊註記沒有獨立結束欄位時使用原註記的時間，新註記直接保存原結束時間。

對帳是冪等的；提交再次忙碌會保留原狀態與持久 intent，下次繼續核對。
後續仍沿原來源介面讀回、逐表退避與資料價值排程。已用完的立即重試預算
不歸零，未知或已跨 Preview 的結果不會被這個修正盲目重送。
正式 owner 在 10/13 **12:26:02** 自行完成對帳並解除全域暫停；原表的失敗
計數保留為 **444**，原任務的下次資格時間為 **12:41:02**，不是人工改 SQLite。

## 超出原生選取器容量的完整範圍拆分

同一筆工作的原範圍為 1 個公司、9 個欄位、10,000 個日期。
Preview 的 10,000 列輸出容量不等於輸入選取容量：原生 bridge 的
`ExactListIndices`、`SelectListBatch`、`TrySelectListBulk` 每批均限制
**2,000 個輸入標籤**。重試相同 10,000 個日期無法排除這個確定性失敗。

`query_selector_contract=bounded_source_selector_geometry_v1` 把此容量納入
既有矩形規劃器，並在 source claim／桌面操作前檢查既有 pending fragment。
1 × 10,000 日期可完整拆成 **5 個 1 × 2,000 工作**；在此單公司、相同欄位與
固定輸入上限下，5 次是覆蓋所有日期所需的最少批次，不代表全來源吞吐量最優。
不刪欄、不縮日期、不把空回補成數值，也不調高原生上限。

替代子工作保留原 field partition、Key1／Key3 語意、密度與列／儲存格容量，
工作分母相加必須與原範圍完全相同；原 pending 標記為版本化 superseded，
原 request、attempt 與已下載資料均保留。已嘗試工作必須先證明原查詢未送出，
未知結果或已入庫範圍不可直接拆分。原表級窗口只移交子工作 owner，不重設
444 次失敗計數或下次資格時間；原 lazy plan 在子工作結束後接續，不另建管線。
`selector_replanning` 的 `prepared` 是持久 intent，須另核對 SQLite 原工作與
子工作才可判定交易已提交，不能把 intent 當作下載完成收據。

含選取容量規劃的最後版本於 **12:36:10** 安全重啟，原生 Windows bridge
未更動。新工作由原序列 owner 執行；重試到期仍須等目前操作及較高價值工作，
不為驗收強改優先級、清除退避或重設操作期限。

正式排程於 **12:42:42** 自行提交原工作的 5 個子工作；
[`selector_repartition_acceptance.json`](../artifacts/data_quality/tej_attempt_annotation_2026-10-13/selector_repartition_acceptance.json)
於 **12:43:39** 獨立核對 SQLite 與原 intent，確認全部 10,000 個公司／日期
位置各覆蓋一次，9 個欄位及原欄位分區完整保留，沒有把原立即重試預算歸零。
第一個子工作 `32cdb453a3bcc8388d307569` 於 **12:43:34** 提交完成收據；
成功後原表級等待窗口才依既有契約釋放，原 444 次失敗紀錄仍在原 audit 中。
另外 4 個子工作在此快照中尚未全部完成，不把排入佇列當成抓齊。

後續 5 個子工作於 **12:45:19** 全部完成；
[`selector-all-five-receipts/audit.json`](../artifacts/data_quality/tej_attempt_annotation_2026-10-13/selector-all-five-receipts/audit.json)
獨立核對全部 5 份收據及原 attempt／Preview／Parquet 綁定，全部通過，
本機 artifact 錯誤為 0。這是原 10,000 個查詢位置完成，而不是 10,000 列
有值資料：5 份仍均為明確來源空回，新增 0 列／0 個原始儲存格。

[`selector-first-child-receipt/audit.json`](../artifacts/data_quality/tej_attempt_annotation_2026-10-13/selector-first-child-receipt/audit.json)
沿用 canonical receipt audit 核對第一個子工作的原 request／attempt SHA、
Preview 提交、來源結果、鍵／schema、Parquet 與收據一致，沒有本機 artifact
錯誤。這筆仍為明確來源空回、0 列，不是假造非空值；證明原本因輸入容量而
無法送出的工作，拆分後確實跨過查詢與正式驗證入庫鏈路。

## 10/13 正式驗收與剩餘邊界

最後完整 TEJ 回歸為 **1,846 passed、1 skipped**，另有 **16 項前端測試通過**。
包含真實 SQLite 讀鎖造成次要操作註記回滾、解除後自動對帳、對帳再次回滾後
恢復、冪等與原時鐘保留、錯誤 scope／SHA／事件拒絕恢復，以及選取分區完整
且不重複、原欄位分區與 Key1／Key3 原生列語意不變。

本輪證據位於
[`tej_attempt_annotation_2026-10-13/`](../artifacts/data_quality/tej_attempt_annotation_2026-10-13/)。
[`production_acceptance.json`](../artifacts/data_quality/tej_attempt_annotation_2026-10-13/production_acceptance.json)
於 **12:40:05** 通過：8 個工作均在最後部署之後真正啟動並完成查詢／驗證／入庫，
原 request、來源負證據、忙碌註記、bridge manifest 與原診斷的 SHA 全部未變。
當時排程為 `executing`、唯讀網站為 `running`，沒有全域暫停。
8 份皆為明確的 `explicit_empty_scope`，取得 0 列／0 個原始儲存格，不能當成
新增非空資料。[`retained_value_reference.json`](../artifacts/data_quality/tej_attempt_annotation_2026-10-13/retained_value_reference.json)
另以目前 capture／ingest 程式核對既有 32 列／416 個原始儲存格，不計入新下載量。
驗收工具本身發送 0 次 provider 查詢。

這輪恢復的是兩種可證明原因的停止／重試路徑，不代表全歷史抓齊。
驗收時 22 個舊隔離工作仍保留，`Catalog axis exceeds reviewed bound` 等
目錄容量問題與這次每批 2,000 個**輸入**標籤的問題不同，未藉此宣稱一併修好。
未驗證實體冷開機、全歷史完整性或 TEJ 顯示值以外的底層精度。試用 API
在 12:26 核對時為保守配額等待，timer 仍啟用；不清零、繞過配額或另開管線。

## 網頁誤判修正

原網頁有兩個與實際下載不同步的顯示。本次區分已到期、實際重試中與已恢復，
不再把過去的到期時刻顯示為未來 ETA；任務完成後也不沿用舊的「排定重試」。

唯讀 gateway 的 `ProcSubset=pid` 看不到通道核對需要的開機識別，不能直接完成
與下載 owner 相同的 Linux birth／relay 檢查。現在由真正的下載 owner 發布
既有 admission 檢查的短時有效結果，網站再核對 owner 與原檢查時刻。
不放寬網站權限，不額外輪詢來源，不把 heartbeat 當成新的通道或 GUI 核對。
介面檢查較早、互動通道可用與資料持續入庫是不同資訊。

## 正式驗證與證據

第一輪部署後驗證 8 份新收據：125 列來源資料、3,243 個非空特徵值、
3,618 個原始儲存格逐欄與 Parquet 相符。兩筆本次失敗的目錄工作都由
新排程自動核對並排回同一佇列；不是人工修改 SQLite 狀態。

新增驗證涵蓋：介面檢查逾時後再次恢復、重啟保留冷卻、指數退避、成功釋放
表級窗口、錯誤來源／程式 SHA／缺少負證據拒絕重排、陌生及配額通知不誤按、
真實任務綁定、不過期的 readiness 及原始欄位逐值比對。
來源值即使被改寫後再重算本機 SHA，也會被逐欄比對拒絕。

凌晨版本 TEJ 回歸為 **1,784 passed、1 skipped**；16 項前端 request／polling
測試全部通過。101 項排程、啟動與目錄恢復的針對性測試亦通過。

正式證據位於
[`artifacts/data_quality/tej_self_healing_2026-10-10/`](../artifacts/data_quality/tej_self_healing_2026-10-10/)：

- `before_deployment.json`：本次負證據與原檔 SHA 基線。
- `production_acceptance.json` 及對應 `-receipts/`：第一輪新收據與逐欄原值驗證。
- `production_value_boundary_v2.json` 及對應 `-receipts/`：最後部署的獨立新收據驗收，分開核對當批來源空回與既有非空資料原值。
- `browser_verified_final/rendered_acceptance.json`：最後版桌面與手機、本機來源進度及獨立 HTTP polling fixture；沒有外部 provider 請求。

最後一批 8 個工作均由來源明確回報 `explicit_empty_scope`，實際取得 0 列、
核對 0 個原始儲存格；這證明查詢與收據鏈路持續前進，不代表取得了有值資料。
非空資料原值證據另引用本次第一輪的 125 列／3,618 個儲存格驗收，並核對
兩輪 capture 與 ingest 程式 SHA 完全相同；不把舊的有值證據冒充當批新資料。

本次不更動已接受的原生批次選取／程式編譯快取 bridge，其 SHA 仍為
`caf7eeb0f316f22567a720bdc0e4cf407e7f66b1e80274398d956de600b42c86`。
既有速度驗收及開機權責見 [完整鏈路加速與開機恢復](tej_chain_boot_2026-10-09.md)。

## 尚未代表完成的部分

驗收時仍保留 22 個舊隔離工作：17 個目錄工作與 5 個來源結果驗證工作。
17 個目錄中，7 個原 request 不符目前精確範圍契約，10 個缺少所需的原始證據；
這不是「來源一定錯誤」或「資料不可修復」的判定。它們不堵住健康表，但不會
為了讓數字好看而改成已完成。其餘資料仍沿原優先順序下載。

已核對 TEJ systemd 開機啟用與本機持續新收據；不宣稱本次實體冷開機、WAN
連線、全歷史完整性、來源底層精度或歷史發布時點全部通過。

## 重跑驗證

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s test -k tej
node --test test/test_tej_dashboard_requests.mjs
```

[`verify_tej_self_healing.py`](../scripts/verify_tej_self_healing.py) 可在新輸出路徑
建立 snapshot；加上 `--since` 與 `--baseline` 後，必須取得該部署時刻之後
真正啟動並完成的 8 個工作，再沿用 canonical receipt audit 核對全部原始欄位。
預設須至少一筆非空資料；若當批全為明確來源空回，可另外指定
`--reference-values-acceptance`，引用 capture／ingest SHA 相同且已通過逐值
核對的非空驗收。報告分別保存當批空回與先前非空原值證據，不能用不明空結果
或不同程式版本替代。所有驗收工具皆不送資料查詢，既有輸出拒絕覆寫。
若目前程式版本尚無非空新收據，可先以 `--values-reference-task TASK_ID`
獨立驗證一筆既有非空收據；輸出明確標為 `retained_nonempty_value_reference`，
不計入新下載量，再將此檔案作為上述原值驗收參照。
