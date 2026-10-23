# TEJ 目錄、下載故障自動恢復與驗收

2026-10-10 初稿，2026-10-21 更新，Asia/Taipei。10/10 凌晨修復的停滯不是程序停止，而是
目錄準備失敗後，介面檢查也逾時，被誤歸為未知資料查詢，使整個佇列永久等待。
10/10 下午重新盤點時，再重現 SQLite 忙碌導致失敗分類回滾、工作永久留在 `running`
的第二條路徑；已補上負證據本機對帳及原 worker 身分保留。
10/13 再修復「失敗分類已提交、次要操作註記卻回滾」的停止路徑，並在送往
桌面前拆分超出來源選取器容量的範圍，避免原封不動重試必定失敗的操作。
10/17 修復 Windows 中斷寫入留下零位元組 stage、原結果解析與授權重排互相
等待的停止路徑；關鍵證據改為完整暫存後發布，並保留跨程序負證據傳輸。
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

## 10/17：中斷寫入造成永久等待

此次服務仍為 `active`，但最後收據停在 10/15 **16:56:12**。
`Contract Adjustment of Stock Futures` 工作 `4cedcef2b372d295b263c276`
在下一次操作中，Windows `WriteAllText` 回報 `The operation was canceled.`。
它已建立 `.stage.json`，內容卻為零位元組。原恢復流程看見檔案就解析，
每次都失敗，未能接續既有的有界授權重排；重啟不會修復這份原始壞檔。

新增 `exact_interrupted_stage_write_authorized_replay_v1` 只辨識此精確邊界：
原 attempt 必須已結束且仍為 unknown、原 request／scope／程式 SHA 一致、
stage 恰為零位元組、沒有 raw／outcome，原三行診斷的行號必須指向不可變
程式內、唯一 Preview 動作前的舊 stage writer。若有進度，只接受同一 attempt
及操作時間內的 `preparing_scope`。任意 JSON 毀損、較晚階段、缺少原程式、
不同 scope 或已有結果均不適用。

這不是把 unknown 改稱未送出。恢復仍使用原先使用者授權、最新 owned idle
介面讀回及原滾動預算：每工作一小時最多 2 次、全域 6 次。核對介面前後均
重驗原證據，先採納可恢復的原結果，不清零冷卻或增加來源配額。
正式排程於 10/17 **22:52:49** 自行完成原工作；原 attempt 仍為
`unknown_outcome`，另以新 attempt／授權稽核記錄重排，五份原故障證據 SHA
全部不變。之前的 595 次證據檢查不是 595 次來源查詢。

### 同類故障的預防與恢復

| 邊界 | 本輪處理 | 驗證限制 |
| --- | --- | --- |
| 關鍵 stage／回應／outcome | 同目錄唯一暫存、完整 UTF-8 寫入與 flush 後，以 rename 發布；本機 I/O 最多 3 次、25／100 ms 退避 | 只重試本機發布，絕不因此再按 Preview；不保證所有檔案系統斷電耐久性 |
| Windows 無法寫回 UNC | 已獲准程序以 stderr 傳回精確負證據，由 Linux 原子保存 | 須核對唯一訊息、attempt、scope、程式 SHA、時鐘與未跨 Preview；已有 stage／raw／outcome 不覆蓋 |
| 原未明操作 | 保留原 request、stage、診斷、manifest、進度及 attempt | 缺少或不符證據仍保持 unknown；不以沒有檔案推定沒送出 |
| 每筆程式部署 | 依不可變 script SHA 重用 Windows 本機程式檔，不再每筆建立 GUID 目錄、複製同一份程式 | 每筆仍核對 SHA、語法、permit、session，使用全新程序；不快取選取、COM、授權或來源值 |

檔案發布使用同目錄 rename，因 WSL UNC 不支援此路徑的 `File.Replace`。
`Flush(true)` 的行為及 rename 旗標分別參見
[Microsoft FileStream.Flush](https://learn.microsoft.com/en-us/dotnet/api/system.io.filestream.flush)
及 [MoveFileExW](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw)。
真正採用的 NTFS／WSL UNC 行為另經下列 fixture 實測，不以文件代替驗證。

### 本輪故障與原值驗證

證據根目錄為
[`tej_resilience_2026-10-17/`](../artifacts/data_quality/tej_resilience_2026-10-17/)。
`native_io_v2/native_io_acceptance.json` 通過 11 項快取檢查及 9 個新程序樣本；
每個子程序另在 NTFS 與 WSL UNC 驗證：檔案獨占鎖定時發布有限失敗、舊內容
不截斷；解鎖後精確 UTF-8 替換，無本輪暫存殘留。這些測試沒有 TEJ 查詢。
`windows_entry_cache_v2/entry_gate_acceptance.json` 的四個真實 Windows 案例
確認：進入 bridge 前的語法錯誤可保存負證據，進入後的錯誤仍為 unknown；
兩者均測試首次 staging 與快取命中，沒有呼叫 TEJ／Excel。

22:56 最後核心版本部署後，
[`production_acceptance.json`](../artifacts/data_quality/tej_resilience_2026-10-17/production_acceptance.json)
於 **22:57:49** 核對八份此次部署後真正啟動、完成完整鏈路的新收據。
原故障證據未變，網站為唯讀，排程持續執行且沒有全域暫停。
這八份都是來源明確空回，新增 **0 列／0 個原始資料格**；不可當成非空 feature
新增。`retained_value_audit_v2.json` 另以目前 capture／ingest SHA 核對既有
5 列／55 個原始資料格，不能冒稱為本輪新下載。

### 完整鏈路實測與仍存在的瓶頸

[`performance_decision.json`](../artifacts/data_quality/tej_resilience_2026-10-17/performance_decision.json)
比較同一份 bridge、相同來源範圍的「每筆複製程式」與「依 SHA 重用程式」。
以 A/B/B/A 執行四條完整實驗鏈，每案例每方法兩次，包含 source admission、
條件準備、查詢、全值讀回、驗證、Parquet／收據與獨立稽核。兩版各取得
12,936 列，同範圍原值／鍵／schema 全部相等；正式既有收據不改寫。
總共送出 12 次資料查詢、5 次目錄操作，不把操作數當成 TEJ 內部 HTTP 次數。

| 相同範圍，每筆平均秒 | 每筆複製程式 | 依 SHA 重用程式 |
| --- | ---: | ---: |
| 股票期貨契約調整，9 個特徵 | 39.76 | 12.66 |
| 相鄰 4 個特徵 | 7.26 | 5.17 |
| 密集價格，4 家 × 1,615 日 × 28 個特徵，每次 6,460 列 | 57.01 | 54.79 |
| 同價格表完整目錄 | 8.91 | 12.73 |

完整取得時間合計 **225.89 → 170.68 秒**，本次觀察耗時少 **24.44%**、
吞吐增加 **32.35%**。這不是穩定保證：第一次整鏈含稽核的 candidate 反而
較慢，目錄平均亦較慢；原版另一輪快取命中仍有 **42.96 秒**的 compiler／
reference 指紋計算等待，該異常完整保留，不歸因成程式複製快取的必然收益。
沒有與回歸測試重疊計時，但其他既有主機工作仍在執行。
完整樣本見
[`full_chain_code_cache_abba/acceptance.json`](../artifacts/data_quality/tej_resilience_2026-10-17/full_chain_code_cache_abba/acceptance.json)。

密集表目前完整讀回平均 **37.50／54.79 秒（68.4%）**，仍是主要瓶頸；本輪
沒有更動該讀回方法或以抽樣替代全值。沿用先前已驗證的最多 4 個唯讀工作者，
不把更多程序等同於來源隔離；先前 8 工作者沒有穩定收益的證據見
[候選與限制](tej_chain_fast_paths_2026-10-05.md#候選實測與沒有採用的方法)。
初始化檔案讀取的異常等待與其他完整匯出介面仍待進一步釐清，不能宣稱已達
理論速度極限或所有可改善瓶頸皆已消除。持續工作仍記在
`tej-resilience-throughput-20261017`，不以此次局部改善關閉原任務。

測試完成後，只由原 benchmark 核對並清除自己持有的介面 gate，正式服務於
**23:08:05** 恢復。Linux 服務持續為 `enabled`，沿用原 Windows boot／logon
owner；本輪未實體重開機，也沒有更改自動登入、來源授權或配額。
該階段 TEJ 回歸為 **1,829 passed**，另 **16 項前端測試通過**；最後版本見下一節。

[`production_after_abba.json`](../artifacts/data_quality/tej_resilience_2026-10-17/production_after_abba.json)
於 **23:11:48** 再驗證八份 23:08 恢復服務後才開始的新收據，全部明確來源空回，
取得 0 列；原故障證據仍未變。當時 owner 正在 `recovering_metadata`，不是正在
抓資料；23:12:02 已回到正式下載並完成第九筆。恢復期間另有四個表的精確公司
標籤在當前選單找不到，保存未送出證據後逐表退避，沒有堵住其他表。仍待核對
最新目錄與原標籤的差別，不猜測改名、不改原 scope，也不宣稱這些表已修復。

`rendered_final/rendered_acceptance.json` 通過本機桌面／手機版實際渲染及六種
獨立 polling 狀態，沒有 JavaScript 錯誤或 provider 請求；來源讀回 100% 不會
冒充已入庫。公網 GET 於 23:12 跟隨 www 轉址至
`https://penguin72487.dpdns.org/tej/` 回傳 200；這只是本機外連的可達證據，
不是另一外地節點或冷開機驗收。沒有修改前端或既有使用者修改的測試 URL。

### 最後部署與 API 空轉成本

後續補上 JSON 型別守門：負證據、manifest 雖可解析，但若根節點為陣列、
純量或 null，必須保留為未驗證／unknown，不能因未分類例外或 `.get` 失敗
使恢復 owner 退出，也不能誤認為未送出。

23:13 再遇到同一張 `Delisted Option Attribute(2018~2019)` 的目錄準備失敗，
具體診斷為 `Catalog axis exceeds reviewed bound`：公司軸沒有通過既有
`Items` 計數界線。該條件同時拒絕負值與超過 150,000 的計數，診斷未留下實際
數字，不能斷言一定是公司過多。後續兩次唯讀檢查逾時，原負證據仍完整保留。
這張表尚未修復，不提高上限或把不完整公司軸當成完整。
獨立穩定介面核對通過後，原目錄恢復於 **23:23:43** 自行排回，沿用已累積的
900 秒逐表退避，其他表繼續下載。

此間發現明確的另一個工具問題：operator lifecycle 從 systemd 繼承 Session 0
relay，`GetActiveObject` 無法看見 Session 1 的 Excel。已改為每階段核對原互動
relay 的 boot／PID birth／socket，並在 Windows 端先檢查 SessionId，再做操作。
實際重啟要求後被原生獨立 add-in 身分守門拒絕，沒有停止 TEJ／Excel；其後只做
穩定介面讀回便恢復，不能稱為已成功驗證 add-in 重啟。

原 30 秒唯讀期限包含初始化，而完整鏈路曾量得 42.96 秒 compiler/reference
指紋等待；因此它可能在尚未檢查介面前就逾時，不足以推定 TEJ 掛死。四個純
唯讀檢查改為有限 **120 秒**總期限；通知確認仍為 30 秒，下載／規劃仍為
900 秒，heartbeat 不延長原期限。本次未證明上述兩次逾時各自卡在哪一個內部
呼叫，不能把初始化異常當成已確認的唯一原因。

最終核心於 **23:26:09** 安全重啟；
[`production_release_v4.json`](../artifacts/data_quality/tej_resilience_2026-10-17/production_release_v4.json)
於 **23:28:09** 驗證八份在此部署後才開始的新完整鏈路收據，原五份故障證據
SHA 不變，正式排程正在執行、沒有全域暫停。這批均為明確來源空回，新增
0 列；`retained_value_audit_v4.json` 另以最後 capture／ingest SHA 重驗既有
5 列／55 個原始資料格，兩項證據不可混算。

API 支線在列數配額已耗盡時，原來仍先執行全來源價值排序。現在先恢復已收到
頁面的本機提交，再判斷配額、冷卻與驗證身分；能抓新頁面時才重算排序。
[`api_quota_idle_measurement.json`](../artifacts/data_quality/tej_resilience_2026-10-17/api_quota_idle_measurement.json)
記錄原正式 unit 的一組前後測：CPU **18.206 → 0.217 秒**，記憶體峰值
**530,010,112 → 17,465,344 bytes**，牆鐘約 39 → 1 秒，新增 0 個來源呼叫、
0 頁。這是等待配額的空轉成本，不是有效下載吞吐；有其他主機服務負載，
也不把此單組測量當成普遍穩定倍數。

本機保守預算為 12 calls／50,000 rows，仍有 488 calls、但剩餘 0 rows，
因此必須等待列數配額；官方 usage 快照不是此次重新取樣。沒有為消耗 calls
繞過每日列數上限、清零流量或改動既有 timer。回歸包含配額耗盡、冷卻、驗證
失敗都不重算排序，但原接收頁面的本機恢復仍執行。

最後全 TEJ 回歸為 **1,848 passed（71.39 秒）**，另 **16 項前端測試通過**。
`windows_entry_release_v4/entry_gate_acceptance.json` 亦重新通過四個真實 Windows
入口故障案例，新增 0 個 TEJ／Excel 操作。原有 30 秒期限斷言已依新的純唯讀
120 秒契約更新；先前失敗收據保留，不把舊斷言失敗當成新版本已通過。

部署後另一次真正故障復原也已觀察：`Delisted Option(from 2026)` 的
`b78817b96b2a06f612f95583` 目錄於 **23:32:52** 失敗後，沒有人工改狀態，
原排程於 **23:33:12** 自行核對介面、保存負證據並排回，仍維持第 119 次累積
失敗及 900 秒退避，沒有重設計數或重複資料 Preview。此表本身尚未取得完整
目錄；此次驗證的是全域不再因它永久停住。
原稽核為
[`metadata_retry-c6079a27804345479a933ee2fa691136.json`](../data_tej/diagnostics/b78817b96b2a06f612f95583-9ee47a6657b644489c1af8cb84d0a98a-metadata_retry-c6079a27804345479a933ee2fa691136.json)。
**23:34:16** 正式排程已完成部署後第 22 筆工作，最後收據 23:34:13、無全域
暫停。數百次相同目錄失敗的重試成本仍值得進一步處理，不能把自動恢復等同
於來源目錄或整體吞吐已修到最優。

### 舊隔離工作的品質邊界

22 個舊隔離工作仍保留，不影響其他健康表。17 個目錄工作仍需各自的完整
來源與容量證據；本輪未重送它們。另五個已完整捕捉的來源回應，共 1,417 列，
有 175 列的原生期間超出原選取日期／月份，全部公司的代號都在原 scope 內。
這是拒絕入庫的具體原因，不是欄位遺失；保留整份回應，沒有只截掉失敗列。
`quarantined_key_audit.json` 保存逐表列數、鍵範例及原回應 SHA，檢查新增 0 次
來源查詢、採納 0 列。尚未證明是來源錯誤或期間模型問題，不能直接放寬日期
驗證、回填發布時間或宣稱不可修復。

## 10/19：缺少公司標籤的分流、開機恢復與重工消除

本輪 Windows 實際於 **00:51:32** 重開機，WSL 開機工作於 00:52:06 啟動。
互動 relay 已恢復，但服務存活不代表有效吞吐：`Ex_right Event`、
`Treasury Stocks Performance`、`Cash Dividends`、`Shareholder Meeting`
反覆在查詢準備階段找不到 `7956==>啟賦科技` 或 `7929==>碩通散熱`。
例如 99 家公司的查詢只缺一家，舊流程仍讓整批等待；換日期或欄位批次後又重做
相同的失敗準備。這是來源精確標籤缺席，不足以證明該公司歷史資料不存在。

修復沿用原佇列、權限、資料價值排序與單一 Wizard writer，沒有猜測更名或代號：

- 原生批次 lookup 必須完整查完、核對 owner／來源綁定及公司清單計數穩定，
  才保存 `exact_current_company_membership_v1` sidecar，與原 request、attempt、
  負證據及不可變 bridge SHA 綁定；部分 lookup、逾時或證據被改動一律不授權拆分。
- 通過原介面獨立核對後，99 家拆成 **98 家續抓、1 家待補**。欄位、日期與
  原工作量分母完全保留；缺標籤不是空資料，也不是已完成。逐表等待改成只擋
  缺標籤的片段，其餘公司及後續歷史分區可繼續生成。
- 同表、同完整來源標籤的其他未嘗試日期／欄位分區，可以沿用原負證據作
  **有期限的排程提示**，只延至原重查時間，不增加來源失敗次數、不重設退避。
  提示不是當前來源證據或 UI 操作許可；健康公司照常做新鮮來源驗證，提示
  過期或綁定不符就恢復正常檢查。API 分工先於此提示，避免搶回 API 已負責的範圍。
  已到期的缺公司片段亦可共用較新的完整 lookup；若曾嘗試，必須額外驗證該片段
  自己的原 request、完整全缺席 sidecar、已結束且確定未送出的原 attempt。
  只更新到較新 lookup 的原重查時間，不覆寫舊 attempt／traffic；尚未到期、
  只有部分標籤重疊、證據不符或 Preview 可能送出，都不能沿用。
- API 分工與 selector 拆分都是未送出查詢的本機交易，不再額外支付 2 秒來源
  查詢間隔；真正查詢的既有 start-to-start／legacy finish-gap 限速及故障退避
  不變。這消除本機排程空等，不代表來源限速或理論最大吞吐已被證明。
- worker 新增 Linux boot UUID。跨 boot 的舊 PID／birth ticks 不再誤認為仍活著，
  可立即分類為原 owner 已死的 **unknown**，不用先空等舊 900 秒期限；仍須先
  對帳原結果、確認舊 Windows owner 退役及精確新介面，才依原授權有限重排。
  同 boot 及舊版 owner 證據仍保留原截止條件；沒有把未知查詢直接改成 pending。
- 網頁分別顯示「其餘公司續抓 · 部分公司待補」、實際待補片段與重查時間。
  缺片段仍在清冊、分母及未完成範圍內，不再把單一公司缺席顯示成全表停止。
  驗收工具改用既有 owner-only `127.0.0.1:8771/tej/api/status`；公開入口的
  私有路由 403 是隔離設計，不為修面板而開放私有路由。

### 本輪獨立驗證

證據集中於 [`tej_resilience_2026-10-19/`](../artifacts/data_quality/tej_resilience_2026-10-19/)，
不包含可公開傳送的原始資料。下列驗收各自保留，不能混算新下載與既有資料：

- 最後完整 TEJ 回歸為 **1,915 passed（54.36 秒）**、**17 項前端測試通過**，
  包含負證據竄改拒絕、全部／部分公司缺席、跨欄位提示、API 分工優先、
  跨 boot unknown 邊界，以及本機分流前後真正查詢的限速不變。
- [`native_membership_v1.json`](../artifacts/data_quality/tej_resilience_2026-10-19/native_membership_v1.json)：
  真實 Windows WinForms 原生選取 fixture 通過 **125 項**，沒有 TEJ 查詢。
- [`live_hint_acceptance.json`](../artifacts/data_quality/tej_resilience_2026-10-19/live_hint_acceptance.json)：
  獨立稽核正式排程已採用的 **9 個**提示分流；逐筆原有 **9,999** 工作列前後
  相等，全部欄位／日期／公司聯集保留，原 task 沒有 desktop attempt，也未增加
  來源失敗計數。稽核本身新增 0 次來源／桌面操作。
- [`shared_recheck_acceptance.json`](../artifacts/data_quality/tej_resilience_2026-10-19/shared_recheck_acceptance.json)：
  獨立稽核最後版本實際採用的 **16 個**共用重查片段，其中 **11 個**保留過去
  已結束的未送出 attempt。逐筆重驗自己的負證據、較新完整 lookup、精確標籤
  及完整工作量；只有原到期時間改為較新 lookup 的既有到期時間。稽核不修改
  正式佇列、不發出來源／桌面操作、不計為新下載。
- [`production_acceptance_release19.json`](../artifacts/data_quality/tej_resilience_2026-10-19/production_acceptance_release19.json)：
  最後版本 **02:00:21** 部署後才開始的 **8 份新收據**，含 **1 列非空資料、
  24 個原始資料格**。目前 capture、ingest、planner、scheduler、startup SHA
  均綁定；原故障證據 SHA 未變，正式排程執行中、沒有全域暫停。
- [`production_acceptance_final.json`](../artifacts/data_quality/tej_resilience_2026-10-19/production_acceptance_final.json)：
  **01:42:09** 部署後才開始的 8 份新下載收據，取得 **30 列、343 個原始資料格**；
  五份原故障證據 SHA 未變，正式排程持續執行。這是加入本機間隔優化前的驗收，
  後續版本另留新 cohort，不覆寫這份證據。
- [`production_acceptance_query_gap.json`](../artifacts/data_quality/tej_resilience_2026-10-19/production_acceptance_query_gap.json)：
  中間版本 **01:51:05** 部署後才開始的 8 份新收據為 **6 列、171 個原始資料格**。
  此 cohort 與最後版本分開記錄，沒有拿舊收據冒充最後部署的新下載。
- [`retained_values_final.json`](../artifacts/data_quality/tej_resilience_2026-10-19/retained_values_final.json)：
  以目前 capture／ingest SHA 重驗既有 **5 列、55 個原始資料格**，不是新下載量。
- [`rendered_final/rendered_acceptance.json`](../artifacts/data_quality/tej_resilience_2026-10-19/rendered_final/rendered_acceptance.json)：
  1440px 桌面與 390px 手機實際渲染通過，無 JS 錯誤或橫向溢出；另驗證六種
  輪詢狀態、篩選／焦點保留，以及讀回尚未提交時不能計為已下載。無外部來源請求。
- [`boot_readback_v1.json`](../artifacts/data_quality/tej_resilience_2026-10-19/boot_readback_v1.json)：
  Windows BootTrigger／S4U WSL 工作、Interactive 登入恢復工作、每分鐘 watchdog、
  pinned launcher、systemd service 與 API timer 均獨立讀回通過。**仍需要 Windows
  使用者登入才能操作 GUI**，沒有啟用自動登入或 Session 0 GUI。實際重開機早於
  本輪修復，沒有為驗收再重開整台電腦，不能宣稱新版本已通過實機冷開機測試。

API 支線當時本機保守預算為 **21 calls／50,000 rows**，剩餘 **479 calls、0 rows**；
API timer 正常等待列數配額，與 Wizard 繼續抓取是兩條不同狀態。官方 usage
快照不是本輪新取樣，不因 calls 尚有剩餘就繞過每日列數上限。

本輪新增的舊 attempt 測試曾因 fixture 缺少 `attempted_at_utc` 而被守門正確
拒絕；fixture 已改用該 registered attempt 的原開始時間，正式守門沒有放寬。
原失敗 log 保留並綁定最後通過的 1,915／17 回歸為 superseded，不覆寫失敗紀錄。

本輪沒有刪除舊收據、縮減欄位、合成歷史值或略過原值／鍵／scope 驗證。
全歷史仍未完成：缺少的來源公司標籤、舊目錄／來源期間隔離及大表完整讀回
仍有待處理。先前同範圍量測的大表讀回占比約 68%，不是本輪重新證明的比例；
本輪新取得資料與舊空回基準不同，也不能相除宣稱速度倍數。canonical 任務
`tej-resilience-throughput-20261017` 保持 active，不將本輪恢復與局部去重稱為
已消除全部瓶頸或達到理論極限。

## 10/21 WSL 重啟但原 Windows 查詢視窗仍存活

本次停止點是 **01:20:17** 開始的下載。WSL 重啟後，原 Linux worker 已死亡，
但 Windows 沒有重開機，原查詢視窗仍在。舊邏輯可辨識跨 boot 的死亡 worker，
卻只接受「原 Windows 視窗也已退役」的授權重排證據；兩者生命週期不同，使
服務持續存活、未知結果卻無法離開等待。原結果缺席不能證明查詢沒有送出。

新增的恢復路徑沿用同一個 owner、查詢鎖與授權帳本：

- 先恢復原結果；只有精確原 worker 已死亡、登記 attempt 仍為 unknown、原
  request 雜湊與 scope 完全一致，才檢查原 Windows 視窗仍存活的情況。
- 新鮮讀回必須確認原 session、活著且身分固定的互動 relay、視窗程序出生時間
  早於原 worker、沒有來源通知、Preview 可用，而且沒有未綁定的既存資料列。
  已保存的 raw、stage 或 outcome 一律先走原結果對帳，不覆蓋既有資料。
- 通過後仍依既有明確授權重排：每筆／每小時最多 2 次、全域／每小時最多
  6 次。原 unknown 證據保留；沒有把它改寫成「確定未送出」。
- 桌面身分或修復證據實際改變時，等待中的排程最多 5 秒內重新檢查本機狀態，
  不必沿用舊介面的檢查冷卻。每分鐘 watchdog 的時間更新不算修復事件，
  也不會重設來源流量預算；本機輪詢不呼叫 TEJ API 或操作 GUI。
- 恢復失敗新增逐次私有診斷，記錄故障階段與例外，權限為 `0600`；公開網站
  不接收這些私有內容。介面狀態讀取取得的 COM 參照改為 `finally` 釋放，
  不釋放其他共享物件或全域強制清理。此生命週期修正遵循
  [Microsoft 的 COM 參照釋放與共享參照注意事項](https://learn.microsoft.com/en-us/dotnet/api/system.runtime.interopservices.marshal.releasecomobject?view=net-10.0)。

原故障於 **02:01:26** 透過既有「桌面已更換」路徑完成重排；它不是新增恢復
分支的現場驗收。新增分支以死亡 worker、仍存活的原桌面及異常證據測試驗證，
正式新版另於 **02:14:14** 安全重啟，並核對部署後才開始的新下載。

### 本次部署驗收與開機恢復

證據集中於 [`tej_resilience_2026-10-21/`](../artifacts/data_quality/tej_resilience_2026-10-21/)。

- 完整 TEJ 回歸 **1,950 passed，67.43 秒**，另 **17 項前端測試通過**。
  真實 Windows 原生 fixture **125 項通過**，沒有連接 TEJ／Excel 或發送來源查詢。
- [`production_acceptance_v1.json`](../artifacts/data_quality/tej_resilience_2026-10-21/production_acceptance_v1.json)
  在 **02:16:04** 核對部署後啟動的 **8 份新收據**。這批均為來源明確空回，
  新增 **0 列**；原故障 request、bridge manifest 與死亡 worker 稽核 SHA 不變。
  [`retained_values_current.json`](../artifacts/data_quality/tej_resilience_2026-10-21/retained_values_current.json)
  另以目前 capture／ingest SHA 重驗既有 **5 列、55 個原始資料格**，不計為新下載。
- [`production_acceptance_v2.json`](../artifacts/data_quality/tej_resilience_2026-10-21/production_acceptance_v2.json)
  在 **02:20:04** 再驗證後續 **8 份新收據、104 列、2,620 個原始資料格**；
  本次確有新增非空資料，正式排程與網站均在執行，原故障證據仍完整保留。
- [`boot_readback_final.json`](../artifacts/data_quality/tej_resilience_2026-10-21/boot_readback_final.json)
  核對實際安裝的 BootTrigger／S4U WSL 工作、登入後 Interactive GUI 工作、
  每分鐘 watchdog、pinned launcher、systemd service／API timer 及活著的互動 relay。
  開機恢復仍須 Windows 使用者登入後才能操作 GUI，沒有啟用自動登入。
  本次沒有重開整台主機，新版本實體冷開機驗收仍為 `false`。
- [`release_summary_v2.json`](../artifacts/data_quality/tej_resilience_2026-10-21/release_summary_v2.json)
  保存最後程式 SHA、正式下載計數、隔離工作、網站狀態及上述獨立驗收的 SHA。
  網站與正式排程連動，不用 HTTP 200 或服務 active 代替來源收據。

### 吞吐瓶頸與尚待處理的範圍

[`baseline32.json`](../artifacts/data_quality/tej_resilience_2026-10-21/baseline32.json)
的 32 份完整鏈路樣本合計 **272.01 秒**、中位數 **6.98 秒**，其中 **24 份明確
空回、8 份非空、150 列**。Windows 條件準備中位數 **3.36 秒**，查詢／回覆
**0.43 秒**；查詢前完整鏈路中位數 **6.17 秒**，不能只優化網路時間。
其中一張表的選單有 56,633 個公司標籤、14,948 個日期，笛卡兒積達
846,550,084 個工作格點；選單日期不是已證實存在資料的日期。

[`after32.json`](../artifacts/data_quality/tej_resilience_2026-10-21/after32.json)
在部署附近取得的另 32 份樣本中位數為 **8.91 秒**，包含部署前工作及並行的
驗收負載；這兩批不同範圍不能拿來聲稱加速倍數或理論極限。本次確實消除的
是原桌面存活時缺少恢復分支、修復後仍等待舊檢查冷卻，以及未釋放的狀態讀取
參照；大量稀疏查詢與大表完整讀回仍需各自的容量證據及同範圍全鏈路量測。

**22 個舊隔離工作**仍保留：17 個目錄工作、5 個來源期間驗證工作。缺公司標籤
的片段也仍在分母內；不刪除年份、不把空回當新增列、不放寬原值與 scope 驗證。
canonical 任務維持 active，後續優先處理稀疏查詢幾何、缺公司片段及逐表來源證據。

## 2026 年 10 月 21 日 傍晚鏈路修正

17:14 的正式排程並非完全停止，17:13 仍有新收據；但一筆來源明確空回耗時
240.43 秒，其中 185.13 秒花在執行環境檔案指紋，來源查詢只有 1.79 秒。
編譯快取已命中，不能把這個等待誤判為每筆都重新編譯。
[`before32.json`](../artifacts/data_quality/tej_resilience_2026-10-21-evening/before32.json)
保存當時 32 筆完整鏈路樣本，中位數 23.64 秒，全部為明確空回、沒有新增資料列。

### 執行環境驗證與可見進度

`Initialize-TejNativeHelper` 仍逐位元雜湊 compiler、runtime references、bridge
及 DLL；檔案讀取改用 1 MiB 緩衝及 `SequentialScan`，不以修改時間／大小代替
內容驗證，不跨 worker 重用可變的查詢狀態。這是
[FileStream 緩衝與讀取選項](https://learn.microsoft.com/en-us/dotnet/api/system.io.filestream.-ctor?view=netframework-4.8.1)
的使用；[循序掃描提示](https://learn.microsoft.com/en-us/dotnet/api/system.io.fileoptions?view=netframework-4.8.1)
並不保證固定倍數的加速。

- [`fingerprint_abba.json`](../artifacts/data_quality/tej_resilience_2026-10-21-evening/fingerprint_abba.json)
  與 [`fingerprint_abba_repeat.json`](../artifacts/data_quality/tej_resilience_2026-10-21-evening/fingerprint_abba_repeat.json)
  在真實 Windows 中擷取新舊 bridge 的原函式，按 A/B/B/A 對相同六個執行環境
  檔案量測；全部 SHA256 一致，沒有連接 TEJ／Excel。首輪舊方法的首次合計
  約 44.14 秒，最後一次卻只剩約 0.125 秒，存在明顯快取／主機狀態影響。
  重複輪的舊／新方法各兩次合計為 0.1672／0.1572 秒；不能將首輪差異當成
  正式下載倍數。
- [`native_cache_fixture.json`](../artifacts/data_quality/tej_resilience_2026-10-21-evening/native_cache_fixture.json)
  通過 11 項實際 Windows 檢查：來源或環境不符、DLL 損壞、路徑轉向均拒絕舊
  generation 並重建，保留原證據；也核對 NTFS／WSL UNC 的完整原子發布。
- Linux owner 在啟動 Windows 之前就寫入綁定本次 task／attempt 的
  `initializing_runtime` 進度，網站顯示「開始啟動桌面橋接／驗證執行環境」。
  顯示失敗不影響下載；進度不授權重試、不延長工作期限，也不計為已下載列數。
- `profile_tej_acquisition.py` 的 v4 報告新增指紋、準備／編譯與載入驗證三段
  allowlist 耗時；SQLite 讀取有兩秒進度中止預算，仍不呼叫來源。

### 本機排程小批次

大量 `selector_scope_repartitioned` 是本機處理缺公司標籤等分片，不是 API
請求。原本每片各自開啟、檢查與提交資料庫；現在沿用同一組 canonical 分片
函式，正式設定 `local_planning_batch_size=8`，攤提連線與外層整備成本，不合併
各片的提交證明。每片仍先保存自己的 durable intent，按原優先順序重新選工作，
片間核對一秒預算後讓出；設定上限為 32。
批次不包含來源查詢；下一次呼叫仍重新通過 unknown、介面及來源安全守門。

[`local_batch_abba.json`](../artifacts/data_quality/tej_resilience_2026-10-21-evening/local_batch_abba.json)
以相同八片有限 fixture、完整 intent／佇列提交作 A/B/B/A 比較：逐片平均
0.2701 秒，八片批次平均 0.1534 秒，這段本機準備少 43.19% 耗時。
canonical 呼叫由八次降為一次，全部範圍、原重試時間及來源呼叫數均相同。
這是本機整備量測，不代表網路、Windows UI 或全歷史下載快 43%。

### 部署與驗證

最後版本在 **17:50:49** 由正式 service 安全接手；SIGTERM 先等待原查詢完成，
沒有強制終止 Windows 查詢、重設 unknown 或啟動第二個桌面 writer。
[`deployment_active_final.json`](../artifacts/data_quality/tej_resilience_2026-10-21-evening/deployment_active_final.json)
核對新 owner、程式 SHA 及已載入的八片批次設定。

- 最後完整 TEJ 回歸 **1,968 passed，113.04 秒**，另 **18 項前端測試通過**。
  新測試包括批次數量／時間預算、每片範圍／重試時間／證據不變、unknown／
  running／介面守門，以及初始化進度不計為下載成果。
- [`rendered_dashboard/rendered_acceptance.json`](../artifacts/data_quality/tej_resilience_2026-10-21-evening/rendered_dashboard/rendered_acceptance.json)
  通過 1440px 桌面與 390px 手機實際瀏覽器顯示，無 JS 錯誤或橫向溢出。
  獨立 response fixture 驗證初始化、等待來源、讀回、提交、HTTP 失敗及恢復
  狀態切換；保留篩選與焦點，沒有 TEJ 查詢。
- [`boot_readback_final.json`](../artifacts/data_quality/tej_resilience_2026-10-21-evening/boot_readback_final.json)
  重新讀回開機／登入兩個 Windows 工作、pinned launcher、每分鐘 watchdog、
  正式 service／API timer 與互動 relay。Linux service 的既有漸進重啟策略也仍
  生效，不會因短期多次失敗永久耗盡 start limit。GUI 仍須 Windows 使用者登入，
  本次沒有重開整台電腦，實體冷開機驗證仍為 false。
- **17:58:18** 的
  [`production_acceptance_final.json`](../artifacts/data_quality/tej_resilience_2026-10-21-evening/production_acceptance_final.json)
  獨立稽核八份在最後換載後才開始的新收據，request／raw／Parquet／receipt
  綁定通過，原始失敗證據雜湊未變。八份均為來源明確空回，新增列數為零，
  不能作為新增非空值證明；另以相同 capture／ingest 程式核對既有五列、55 個
  原始儲存格的
  [`retained_values_final.json`](../artifacts/data_quality/tej_resilience_2026-10-21-evening/retained_values_final.json)，
  明確區分「本輪新收據」與「既有非空值的獨立驗證」。

### 主機壓力與剩餘限制

[`windows_resources.json`](../artifacts/data_quality/tej_resilience_2026-10-21-evening/windows_resources.json)
在 **17:29:32** 量到 Windows 僅剩約 **1.24 GiB** 可用實體 RAM，`vmmemWSL`
working set 約 **91.06 GiB**。這與不規則長等待相容，但單次觀察不能單獨證明
因果。既有回收器只處理 registered backfill 的已丟棄頁；該 cgroup 當時沒有
運行，不能把它擴成任意清理其他下載器／交易服務的全域快取。

本次未更動全域 WSL 記憶體上限、停止其他下載器或強制清快取。
[`final_profile16.json`](../artifacts/data_quality/tej_resilience_2026-10-21-evening/final_profile16.json)
的後續 16 筆中位數 20.71 秒，仍全部明確空回；它與前 32 筆不是同範圍配對，
且含批次最後部署前工作，不能拿兩者相除宣稱整體加速。
缺公司標籤、22 個舊隔離工作、稀疏事件查詢幾何與大表讀回仍各有待解範圍；
全歷史未完成，canonical 任務維持 active，不宣稱已達理論極限。
本輪完整證據索引與未解項目存於
[`release_summary_final.json`](../artifacts/data_quality/tej_resilience_2026-10-21-evening/release_summary_final.json)，
封存時最新完成收據為 **17:59:36**；服務仍由同一正式 owner 持續執行。

## 2026-10-22：交易自鎖、分片重試與稀疏查詢範圍

時間皆為台北時間。不能以 service active 代替下載證明：本輪先確認舊 owner
仍有完成收據，再以正式換載暴露並修復兩個會使進度停止的既有邊界問題。
沒有重設 unknown、刪除原始資料或開第二個桌面 writer。

### 根因與處理

- `proved_unsent_prequery` 已收到原交易連線，卻經 `task_request` 另開會初始化
  schema 的連線。當同一輪 lazy refill 已持有寫鎖時，第二條連線會等自己的鎖，
  形成 `local_metadata_busy`、回滾、重做的迴圈。現以可選的 `connection`
  沿用原交易，仍由相同的 canonical schema／指紋驗證展開 request。
  新測試在真實 SQLite 寫入交易內禁止另開連線，核對舊負面證據與新佇列工作。
- `repartition_selector_pending` 把同表另一片的「缺公司標籤」重試窗口當成全表
  互斥限制，拋出 `Another original fragment owns the table retry window`，
  導致 service 重啟後再撞同一片。現在僅允許這個已定義的 company-fragment
  類型與獨立分片並存，原窗口逐欄不變；其他全表冷卻仍拒絕繞過。
- 效率稽核原本在 SQLite `BEGIN` 中跑全部表的 CPU 規劃，約 150 秒的長讀鎖
  可阻塞 DELETE-journal collector 提交。改成每表擷取有兩秒 SQL 預算的
  一致快照，先 rollback 再解析／規劃。測試在慢規劃中由獨立連線取得
  EXCLUSIVE 寫鎖；[同時期的新收據](../artifacts/data_quality/tej_resilience_2026-10-22/during_nonblocking_audit.json)
  也確認稽核沒有讓下載停止。

### 查詢幾何與效能

`bounded_scope_geometry_independent_native_readback_v1` 分開定義「公司×日期的
潛在查詢範圍」與「實際原生讀回列數」。Key=2 的潛在範圍最多 50,000 組，
仍受 400,000-cell 與 selector 邊界約束；實際 capture／ingest 維持 **10,000
列**，不是提高官方 API 配額。快照與 Key=3 原生多紀錄表不擴大此範圍。

來源若以本次 exact attempt 的新鮮 Preview 證明超量，保存原 request、attempt、
response 與 plan，交 canonical capacity replanner 縮回小分批；規劃本身發送零次
來源查詢。不將潛在組數當實際筆數、不補假列，也不將未知結果視為未送出。
已證明密集的表保留 backoff 標記，後續維護不能又套回大範圍。

- [正式遷移及換載](../artifacts/data_quality/tej_resilience_2026-10-22/deployment_active_verified.json)
  優化 **92 張表**，該批剩餘查詢由 **1,234,241 → 710,283**，少 **523,958**
  次；原完成範圍、原計畫備份及收據保留。這是固定範圍計畫的呼叫數改善，
  不是實測整體下載倍數或無限制裝箱的理論最優。
- 遷移初次遇到舊完成計數不一致時完整拒絕並恢復服務；修正後只保留該表原狀，
  不讓獨立有效表一起失敗。31 張未套用此遷移：6 張快照維持原設定、5 張已有
  來源隔離、18 張需舊鍵範圍調和、2 張需完成範圍調和。它們不等於 31 張都
  無法下載，也不是已修好歷史覆蓋；差額是工作組數，不是缺失實際觀測筆數。
- `build_plan` 將完成範圍按公司建立索引，不再對每個公司反覆掃全部完成證據。
  [A/B/B/A 對照](../artifacts/data_quality/tej_resilience_2026-10-22/planner_abba.json)
  的完整序列化 plan／指紋一致，規劃平均 **0.6609 → 0.3234 秒**，這段 CPU
  工作少約 51.07%；來源查詢、資料量與全鏈路延遲不可套用這個倍數。
- 真實 Windows 的 20,000×12 控件 fixture 完整讀回逾五分鐘期限，候選被拒絕，
  沒有把實際讀回上限改為 50,000 或單純拉長期限。
  [最終 1,000×12 fixture](../artifacts/data_quality/tej_resilience_2026-10-22/native_fixture_final.json)
  通過完整字串雜湊一致及七項異常／穩定性檢查；這不是 TEJ 來源資料。

### 正式驗收與持續恢復

最後正式 owner 在 **01:38:35** 接手；相同的單一桌面 writer 繼續下載。
TEJ 37 個測試檔 **1,996 passed，53.22 秒**；另 **18 項前端測試**及
[七項實際 Windows parser／範圍守門檢查](../artifacts/data_quality/tej_resilience_2026-10-22/native_scope_guard.json)
通過。這不是整個 repository 的全套測試。

[01:40:08 的獨立正式驗收](../artifacts/data_quality/tej_resilience_2026-10-22/production_acceptance_verified.json)
核對八份在最後換載後才開始的新收據、request／raw／Parquet 綁定與原失敗證據
雜湊。八份均為來源明確空回，新增零列；另以相同 capture／ingest 程式
[重驗既有五列、55 個原始格](../artifacts/data_quality/tej_resilience_2026-10-22/retained_values_recovery.json)，
不把舊非空值算成本輪新增。22 個舊隔離工作仍保留，沒有放寬證據門檻。
後續 [01:42:05 的非空正式驗收](../artifacts/data_quality/tej_resilience_2026-10-22/production_acceptance_nonempty.json)
另核對最近八份新收據，實際新增 **20 列、220 個原始格**（180 個非空特徵格），
不是沿用上述舊五列；request／raw／Parquet 與原始儲存字串核對通過。
[換載後另一組八筆 profiling](../artifacts/data_quality/tej_resilience_2026-10-22/final_profile8_verified.json)
中位數 7.36 秒，含兩筆非空回、共 15 列；與前 128 筆不是相同範圍配對，
不能拿中位數相除宣稱整體加速，亦不與後續非空驗收的 20 列混算。
[面板讀回](../artifacts/data_quality/tej_resilience_2026-10-22/owner_status_final.json)
也顯示排程存活、執行中且無全域暫停；面板只讀，不藉顯示呼叫來源。
本輪未重啟共用 dashboard owner；已規劃數量讀取新佇列，新的 scope ETA 假設
與未驗證表外推程式已有測試，但其共用後端換載仍須另行驗證。

[開機／登入讀回](../artifacts/data_quality/tej_resilience_2026-10-22/boot_readback_final.json)
確認兩個 Windows 工作、pinned launcher、每分鐘互動 watchdog、enabled Linux
owner 與 API timer。既有失敗自動重啟採 30–120 秒漸進等待，沒有有限次失敗後
永久停掉的 start-limit。GUI 仍須 Windows 使用者登入；本輪未實體重開機，
`current_code_cold_boot_verified=false`。全歷史及理論極限均尚未驗收，任務維持
active；未為速度縮減來源範圍、跳過驗證或變更全域 WSL／GPU 資源。

## 2026-10-22 上午續修：送出前恢復與監控讀鎖

### 本次確實發生的停止

舊 owner 雖然 active，但最後完成時間停在 **03:01:08**。原工作
`5e5307759a835a43f5b6e175` 在 **03:03:22** 明確失敗於 Preview 送出前，
兩次本機準備重試後進入 `query_preparation_prequery_needs_review`。
後續每分鐘是介面確認，不是每分鐘重新送出市場資料查詢。直到 **08:58:54**
舊 owner 的一次介面讀回成功，才自行解除這筆阻擋；不能以該 process 一直存活
宣稱這六小時正常下載。

原診斷只記錄 notice／disabled root 阻擋 source selection，沒有當時通知原文。
因此不能倒推一定是 MySql 配置失敗或找不到資料表。原 request／負面 outcome
與既有恢復紀錄的雜湊核對一致，另與較早三份失敗證據合併保存於
[`morning_failure_baseline.json`](../artifacts/data_quality/tej_resilience_2026-10-22/morning_failure_baseline.json)。
09:05:46 第一次安全換載後，原阻擋工作在 **09:07:20** 完成，來源明確空回零列，
不是非空新增或完整歷史證明。

### 自我修復的共同實作

- 把既有 metadata 的 owned-notice 恢復抽成
  `restore_owned_metadata_interface`，供已證明未送出的 download 共用。
  健康路徑仍只有原本一次確認；失敗且既有明確設定啟用，才額外診斷。
  僅允許精確 owned 的 `MySql:malloc` 與 `Cannot find table 0.`，最多確認兩次，
  確認後重新讀回穩定狀態。未知、帳號、權限、配額與外來視窗仍拒絕自動確認。
  不送 Preview，不改公司／日期／欄位，不動登入或其他 Excel。
- 排程恢復與即時失敗處理沿用同一條既有政策。原負面證據、finished attempt、
  bounded clock 與預算驗證保留；恢復失敗新增私有 0600 診斷，公開摘要不帶
  原始錯誤文字。成功確認只排回原範圍，不把未知結果改成「未送出」。
- Windows 準備失敗現在先分類，再將同一精確類型寫入 outcome 與 stderr
  attestation。先前 generic 類型可能早於較精確 outcome 寫出；UNC 寫入失敗時
  transport fallback 便可能丟失 list-selection／公司集合證據。本次也讓集合雜湊
  在兩個傳輸通道一致。原生 Windows 純分類檢查 **12 項**通過，未操作 TEJ／
  Excel、未發送來源查詢；它不是實際製造 vendor 錯誤的測試。

### 監控不能阻塞下載

正式驗收另外發現 `/tej/api/status` 超過 15 秒，而 collector 回報
`local_metadata_busy`。實際 reader 持有 DELETE-journal 資料庫；最新完成收據
查詢使用 `tasks_scope_state_counts` 後再建立 temporary sort。
[`morning_monitor_profile.json`](../artifacts/data_quality/tej_resilience_2026-10-22/morning_monitor_profile.json)
的一次完整只讀狀態建置為 1.321 秒，其中 latest SQL 執行已佔 0.819 秒；冷資料
與競爭下更慢，不把這一次熱讀時間當成最壞上限。

在 canonical schema 增加 `tasks_scope_latest` covering index，依 scope／state／
kind／完成時間排序，保留不同 `timing_basis` 的最新收據，不以 ETA 的 fresh-only
樣本偷換活動狀態。監控每條連線另有兩秒 monotonic SQL 執行預算（lock wait
另限 0.25 秒）；超時關閉連線並回報 metadata unavailable，不顯示部分統計。
SQLite progress handler 不是可中止核心磁碟 I/O 的硬即時期限。
新測試核對無 temporary sort、跨 timing basis 最新值，以及中斷後獨立 writer
仍可取得 EXCLUSIVE 鎖。索引安裝在已完成當前工作的安全邊界，未刪除資料。

[`固定快照 A/B/B/A`](../artifacts/data_quality/tej_resilience_2026-10-22/morning_monitor_index_abba.json)
保留當時舊 planner 選用的索引為 A，新 covering index 為 B；四次結果 SHA 完全
一致，平均 **0.696964 → 0.0000696425 秒**。這只證明 latest-receipt SQL，
不能當成整體下載倍數。一次性 schema 安裝連同該測試用時 219.20 秒；不是每筆
查詢都重新建索引。全鏈路仍有 source preparation、來源等待及完整讀回。
另一次[完整只讀狀態建置](../artifacts/data_quality/tej_resilience_2026-10-22/morning_monitor_profile_final.json)
用時 0.929 秒，與先前樣本不是相同時點的全輸入配對，不作倍數宣稱。

共用唯讀 gateway 已在 **09:12:33** 換載，未重啟交易引擎。這取代凌晨「尚未換載
共用後端」的當時狀態；舊失敗驗收因 HTTP timeout 拒絕，沒有抹掉失敗。
正式 collector 最後在 **09:15:44** 換載。之後觀測到啟動時約一分鐘用於價值排序
更新：`install_priorities.scope_identity` 對全 scope 的 request 做前後兩次 SHA。
此完整性檢查尚未優化，沒有拿掉它來改善計時；下一輪須先建立相同範圍的保真
替代驗證，再縮短這段本機維護，而不是誤認為正在送出來源查詢。

最終變更的 TEJ 37 個 Python 測試檔 **2,011 passed，112.40 秒**；共用監控
**200 passed，78.15 秒**，前端另 **18 passed**，跨 timing basis 與讀鎖測試
補驗 **30 passed，4.14 秒**。範圍不是整個 repository 全套測試。
[`桌面／手機實際瀏覽器驗收`](../artifacts/data_quality/tej_resilience_2026-10-22/morning_dashboard_browser/rendered_acceptance.json)
另核對篩選／焦點保留與每五秒更新；其中錯誤／恢復狀態切換使用獨立 HTTP fixture，
不是在正式資料庫注入故障或新增資料。
[`最後開機工作讀回`](../artifacts/data_quality/tej_resilience_2026-10-22/morning_boot_final.json)
確認原雙工作、launcher SHA、互動 relay、enabled Linux owner 與 API timer。
仍未實體重開機，GUI 仍需登入，`current_code_cold_boot_verified=false`。

### 最後正式讀回與未解邊界

[**09:19:53 正式驗收**](../artifacts/data_quality/tej_resilience_2026-10-22/morning_production_final.json)
通過最後換載之後才開始的八份新收據；request／raw／Parquet 與七份原失敗
artifact 雜湊不變。這八筆全部為來源明確空回，**本次新增零列**，不可當成
非空資料取得或原生歷史完整率。另以最後相同 capture／ingest code
[核對既有五列、55 個原始格](../artifacts/data_quality/tej_resilience_2026-10-22/morning_values_final.json)，
不把這五列算成本次新增。

[`最後八筆完整鏈路 profile`](../artifacts/data_quality/tej_resilience_2026-10-22/morning_verified_profile8.json)
同樣都是最後換載後工作，中位數 15.30 秒，送出前中位數 13.84 秒；來源準備
仍是值得測量的下一個主要階段。與先前 32 筆不同範圍，不作全鏈路加速倍數
比較，也不為改善空回率而刪除待抓的公司／日期／欄位範圍。

正式排程與網站驗收時均為執行中、無全域暫停；**22 個舊隔離工作仍保留**，
個別公司缺標籤亦沿既有範圍保存、分片等待重試。全歷史、未知來源限制下的
理論吞吐上限與實機 cold boot 都未驗收。工程任務維持 active，正式服務續跑；
本輪沒有刪除來源值／收據、清空未知工作、跳過原始格驗證或調整交易服務。

## 2026-10-22 晚間：排序長停頓、恢復證據與查詢前細部進度

本節接續同一個 `tej-resilience-throughput-20261017` 工程任務。白天的成功
收據不是晚間仍正常的證據；本輪重新從正式程序、執行堆疊、排程、原始失敗
artifact、Windows transport 到網頁讀回核對。這不是全歷史完成報告。

### 原因與本輪實作

**19:06:16（臺北）** 的[正式程序堆疊](../artifacts/data_quality/tej_resilience_2026-10-22/evening_priority_stall_stack.txt)
停在 `scope_identity → install_priorities → refresh_if_due`。舊實作每次更新
資料價值排序，都在 writer transaction 內對 **109,364** 個工作 request
做前後兩次解析、序列化與 SHA；當時最新收據停在 19:03:34。排序 snapshot
產生於 19:03:59.855，舊刷新狀態直到 19:06:36.225 才寫完，下一份收據於
19:07:01 完成。這段間隔還包括排名建置及後續來源工作，不能全部算成雜湊成本。

本輪保留相同排序及原始範圍，改由 SQLite authorizer 在執行前限制可寫集合：

- 只允許 `tasks.priority`、`download_plans.priority` 與排名 metadata；
  source request、cursor、狀態、收據、流量、原始內容均不可改。
- trigger／外鍵連帶來源修改、DDL、危險函式與一般 `meta` 修改皆拒絕。
  固定字面 key 的排名 metadata 寫入只有獨立、限時能力，不授權任意控制狀態。
- SAVEPOINT 包住本次套用；失敗回復本次變更，不替 caller commit 或撤銷其先前工作。
  相同 priority 不再重寫；不再於正式刷新讀取歷史 request payload。
- 只在真正到期時發布 `refreshing_priorities`，另記錄本機清冊、各通道套用及
  總刷新耗時。監控顯示「更新本機資料價值排序 · 未送來源查詢」，不冒充下載。

新增的 [固定輸入 A/B/B/A 工具](../scripts/benchmark_tej_value_priority.py)
使用隔離合成佇列，每通道 10,000 個工作及每工作 8 KiB payload；比較原始
Git 模組與候選，在計時外以獨立全欄位 oracle 核對 source identity、排名與
最終選取順序。測試另外涵蓋 cached statement、惡意 trigger／FK、失敗 rollback
及 caller transaction。沒有為加速而取消完整性邊界。

| 同一輸入的排名套用與 commit | 原版平均 | 新版平均 | 此步驟加速 |
| --- | ---: | ---: | ---: |
| Smart Wizard | 1.534136 秒 | 0.026194 秒 | 58.57 倍 |
| API | 1.249048 秒 | 0.025440 秒 | 49.10 倍 |

[最終 ABBA 收據](../artifacts/data_quality/tej_resilience_2026-10-22/evening_priority_abba_final.json)
全部 oracle 一致。另一次在正式 `.download.lock` 保護、collector 已排空的
[本機套用](../artifacts/data_quality/tej_resilience_2026-10-22/evening_ranking_apply/application.json)
為 Smart Wizard **0.316 秒**、API **0.174 秒**；包含清冊建置與 CSV 輸出的
完整維護命令仍為 **19.80 秒**。上述倍數只適用排名 installer，不是 TEJ
全鏈路、冷啟動或來源吞吐倍數。

本輪同時修復兩個可重現的健壯性／可觀測性缺口：

1. **壞掉的本機 retry hint 不應拖垮全部工作**：對單一 hint 做型別、大小、
   時區與期限驗證；先以 mode 0600 保留原始 SQLite bytes／SHA，再修復本機
   重試 metadata。僅該候選沿用既有最長 900 秒窗口，其他可下載工作續跑。
   到期仍須原未送出證據與新的 owned interface readback；不把來源未知當成空回，
   不憑壞 hint 重送。正式庫未證實有此損壞；這是故障注入驗證的預防修復。
2. **準備階段不再是一個黑盒**：橋接 best-effort 發布七個 allowlist 子階段：
   綁定來源、欄位、公司名單／精確搜尋、日期模型、公司選取驗證、日期選取驗證、
   最後來源／Preview 守門。只寫狀態，不新增 UI 操作、查詢或期限續租。
   Dashboard ABI 升至 17；未知／過期階段不顯示，開始等待來源後清除舊子階段。
   原生 cache fixture 驗證七個階段、外來 attempt 拒絕及寫入鎖住時不重送。

### Windows 失敗恢復與未證實的原因

第一輪重新啟動後，曾有 Windows worker 在取得許可前無 ACK 的失敗，均留下
`market_data_query_submission_possible=false` 的精確負證據，沒有來源查詢。
既有 bounded recovery 後來自行完成新的工作；最後 collector 換載於
**19:15:49**。未因此放寬 systemd 安全設定、刪除失敗 artifact 或盲目重排未知工作。

為區分 Linux shell 能啟動 Windows 與正式 service 能啟動 Windows，延伸既有
[`verify_tej_windows_transport.py`](../scripts/verify_tej_windows_transport.py)，以實際
relay、Session ID 與 caller mount namespace 執行「只回報 Windows Session ID」
的隔離 probe，完全不接觸 TEJ／Excel。相同 hardened namespace 成功 **2.52 秒**、
外部 namespace 成功 **3.15 秒**；完整正式限制與 canonical gate 路徑亦成功。
因此目前證據**不能歸因**於 mount namespace、systemd 保護、PID 上限或 RAM。
短暫無 ACK 的底層根因仍未確證，沒有把推測寫成已修復的根因。

[正式 Windows transport 四模式驗收](../artifacts/data_quality/tej_resilience_2026-10-22/evening_transport_final.json)
確認：首 relay 失敗後只容許一個 worker；全部未取得許可可安全等待；已取得
許可後出錯或逾時不啟動第二個 worker。原 fixture 用一秒判定 Windows 啟動，
實際注入了錯誤的「未取得許可」故障，該次失敗收據保留；修正為五秒 admission、
十秒總期限與明確超時的純 fixture 命令後四項通過，**未更動正式查詢期限**。
另一次[橋接入口故障測試](../artifacts/data_quality/tej_resilience_2026-10-22/evening_bridge_entry_fixture/entry_gate_acceptance.json)
涵蓋 cache hit／miss、進入橋接前後的分類；沒有送出來源查詢。

### 正式資料、網頁與剩餘瓶頸

[19:21:57 正式驗收](../artifacts/data_quality/tej_resilience_2026-10-22/evening_production_abi17.json)
核對最後換載後才開始的八份 request／raw／Parquet／收據，原七份失敗 artifact
雜湊不變，排程存活且沒有全域暫停。**這八筆都是來源明確空回，新增零列**。
另以本輪相同 capture／ingest code
[獨立重驗既有五列、55 格](../artifacts/data_quality/tej_resilience_2026-10-22/evening_values_reference.json)，
不把既有值當成新增下載。驗收腳本原硬寫 ABI 16，曾正確拒絕新版 17；已改由
canonical `SCHEMA_VERSION` 核對，仍拒絕不同 ABI、非唯讀、洩漏原值、dead、
paused 或停用 metadata recovery 的狀態，沒有降低安全門檻換取通過。

[晚間八筆完整鏈路 profile](../artifacts/data_quality/tej_resilience_2026-10-22/evening_current_profile8.json)
中位數 **14.03 秒**，送出前 **12.74 秒**，仍以 Windows／來源準備為主要成本。
同輪前段 32 筆中位數為 7.79 秒，後段另 32 筆為 12.46 秒；工作範圍與競爭負載
不同，不把 installer 加速外推成整體加速，也**不能宣稱已達理論上限**。
下一步應對固定相同 scope 的準備子階段分別量測與配對，不跳過來源守門、完整
讀回、資料驗證，亦不以縮小公司／年份／欄位範圍來美化速度。

### 追加：跨來源清冊的全域排序成本

新版自動刷新於 **19:25:35** 真正完成；
[獨立保留的刷新收據](../artifacts/data_quality/tej_resilience_2026-10-22/evening_automatic_refresh.json)
顯示總耗時 **35.524 秒**，其中本機清冊 **34.680 秒**，Smart Wizard／API
套用各 **0.581／0.137 秒**，兩者都未解析歷史 task payload。這證明前述 installer
修正已在正式排程生效，同時暴露下一個本機瓶頸，沒有把所有軟體等待歸給來源。

接著對完整 `build_ranking` 做唯讀 cProfile，34.47 秒中 `local_features`
佔 23.33 秒，兩個 FinMind `fetchall` 合計 15.48 秒。原查詢以
`ROW_NUMBER(PARTITION BY dataset...)` 排序所有完成工作的 receipt path，
實際目的卻只是每個 dataset 選一份代表收據。

在既有 `local_features` 共用入口改為 `finmind_local_metadata`：同一 read
snapshot 中取得相同完整工作首末日期，利用既有 dataset 索引，逐 dataset
`ORDER BY ... LIMIT 1` 選相同最新非 NULL 收據。完整工作的無收據日期仍計入
首末範圍；NULL 日期、相同排序鍵、空字串 receipt、非 complete 工作均保留原語義。
不新增 FinMind 索引／writer、不掃原始數值、不做可能過期的快取。

[`--inventory-root` 固定快照 ABBA](../artifacts/data_quality/tej_resilience_2026-10-22/evening_inventory_abba.json)
從原 Git revision 提取原 SQL，逐通道固定同一唯讀 snapshot，對完整日期界限及
**精確選中 receipt path** 做獨立 SHA oracle；105 個 dataset 的四輪結果一致。

| 完整日期界限及代表收據 SQL | 原版平均 | 新版平均 | 此查詢加速 |
| --- | ---: | ---: | ---: |
| sponsor，57 datasets | 0.416898 秒 | 0.150828 秒 | 2.76 倍 |
| complement，48 datasets | 13.439790 秒 | 2.544359 秒 | 5.28 倍 |

較早零等待維護因桌面 readiness 持有同一 lock 而被正確拒絕，未刪 lock、未搶寫。
保留該失敗 run，改為有界等待既有 lock 的
[最後本機套用](../artifacts/data_quality/tej_resilience_2026-10-22/evening_inventory_apply_retry/application.json)，
完整命令 **16.394 秒**，檢查 **9,433** 個本機特徵、排名 **47,352** 欄；佇列
套用 Smart Wizard **0.139 秒**、API **0.033 秒**，來源 scope／狀態不變。
完整命令與前次自動刷新不是相同輸入／I/O 工作量，不能用兩者宣稱全鏈路倍數。
collector 最後於 **19:33:36** 換載這個跨來源查詢修正，並繼續正式下載。

### 工程驗收與限制

最後全部 37 個 TEJ Python 測試檔 **2,076 passed，91.71 秒**；TEJ 前端與
共用 dashboard core 合計 **52 passed**。共用 gateway 另 **173 passed**。
這不是 repository 維護全套測試。原生 helper 的
[11 項 cache／progress 檢查、9 個 fresh process](../artifacts/data_quality/tej_resilience_2026-10-22/evening_native_cache_fixture.json)
通過，七個子階段 NTFS fixture 寫入／讀回約 0.078–0.121 秒；不能當成正式 UNC
路徑的相同成本。

[最後桌面／手機 rendered 驗收](../artifacts/data_quality/tej_resilience_2026-10-22/evening_rendered_handoff/rendered_acceptance.json)
通過 1440／390px、真實來源狀態與 feature 翻頁／篩選、焦點保留及五秒更新；
另外使用隔離 HTTP fixture 驗證七階段、安全 scope 子階段、原始格未入庫不算完成、
HTTP 失敗與恢復。實際檢視 desktop／mobile PNG，mobile 另要求子階段在 viewport
內，不能僅用 CSS visible 當成截圖有拍到。前兩次瀏覽器驗收失敗仍保留；重現
相同篩選可成功，移除驗收中早於實際內容等待期限的多餘五秒空列斷言，另保留
失敗時 scope／request／位置診斷，不改來源或放寬內容、分頁驗證。

[最後開機工作讀回](../artifacts/data_quality/tej_resilience_2026-10-22/evening_boot_handoff.json)
只證明兩個 enabled Windows 工作、固定 launcher、live interactive relay、Linux
owner 與 API timer；**仍未實機 cold boot**，GUI 仍需要登入。

Smart Wizard 的 **22 個舊隔離工作**與局部公司缺標籤證據保留，不把「已知未送出」
延伸成所有未知都能重送。API trial 則是獨立 oneshot／timer；19:13 的本機
保守計量已達每日 **50,000 列**、13 calls，原因 `waiting_quota`，不是 collector
崩潰。26 個既有 API `query_rejected` 也尚未於本輪重新查證，不能稱為權限或來源
資料錯誤已確認；本輪沒有為測試超過其資料配額或清空 blocked 狀態。

本輪修復已完成範圍內的排序長停頓與壞 hint 隔離，且正式工作持續前進；
**全歷史、全部舊來源錯誤、Windows 短暫無 ACK 根因、完整 cold boot 及理論吞吐
上限仍未驗收**。工程任務維持 active，後續應優先固定 scope 量測 source
preparation 與逐表處理原隔離；本輪沒有刪除來源資料、捏造發布日／數值、降低
驗證、開第二個桌面 writer 或更動交易服務。

### 最後換載驗收：19:38:29（臺北）

[`evening_final_release.json`](../artifacts/data_quality/tej_resilience_2026-10-22/evening_final_release.json)
確認 **19:33:36 最後換載之後才開始的八筆**皆完成 request／raw／Parquet／收據
獨立稽核；原失敗證據不變、排程 `executing`、沒有全域暫停。此時正式服務
PID **942194**，active／running。這取代上面 19:21 的中途驗收；不能以舊 PID
或換載前收據作最後版本的證明。

這八筆依然全部是來源明確空回，**新增零列**。
[本輪相同 capture／ingest 的非空參考重驗](../artifacts/data_quality/tej_resilience_2026-10-22/evening_values_handoff.json)
另驗既有五列、55 格，不列入新增資料。
[最後八筆 profile](../artifacts/data_quality/tej_resilience_2026-10-22/evening_final_release_profile8.json)
全鏈路中位數 **10.73 秒**、送出前 **9.67 秒**，SQL 選樣 **0.00249 秒**。
同輪較早兩筆曾為 **19.54／74.28 秒**，其中一筆 Windows bridge 本身約
19.24 秒，仍有橋接外的等待，並非只有來源查詢。這些都是不同工作／負載的
觀測；保留慢樣本，不用最後中位數宣稱已消除所有瓶頸。

重現本輪兩種固定輸入比較：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tej_value_priority.py --baseline-ref 8b0664773fe60f44abb5aec7b6154d863d09d259 --output /tmp/new-tej-priority-abba.json
run_fintech_python scripts/benchmark_tej_value_priority.py --baseline-ref 8b0664773fe60f44abb5aec7b6154d863d09d259 --inventory-root /root/stockAgent --output /tmp/new-tej-inventory-abba.json
```

輸出必須不存在；若重跑請換新檔名，不覆寫舊證據。第二個命令只讀兩個 FinMind
metadata queue，單通道 snapshot 限 120 秒；不會下載或修改任何 provider 資料。

## 2026-10-22 晚間續修：舊清冊隔離遷移與完整鏈路測速

### 根因不是服務是否 active

[20:18 的 32 筆基線](../artifacts/data_quality/tej_resilience_2026-10-22/night_baseline32.json)
全部是來源明確空回，新增零列；全鏈路中位數 8.72 秒、送出前 7.85 秒。
其中一筆 64.35 秒，Windows 初始化即占 45.40 秒，不能把這種長尾全部歸因於
供應者 API。原先橋接外等待也缺少分段證據。

[舊 metadata 稽核](../artifacts/data_quality/tej_resilience_2026-10-22/night_metadata_proofs.json)
找出 17 筆早期 `discover` 隔離：7 筆使用舊輸入契約，且 17 筆都沒有新版
immutable bridge manifest。自動恢復仍正確拒絕；但相同不可補回的舊證據已反覆
檢查數百次，其中一筆 hint 為 880 次。單純縮短 cooldown 無法解決。

### 明確授權的一次性遷移，不偽造「當年沒送出」

在既有 `tej_metadata_recovery.py` 與 canonical CLI 新增 `retry-metadata`，
契約為 `explicit_exact_legacy_metadata_requeue_v1`：

- 限一筆原先隔離的 metadata plan，必須明確接受舊執行可能已耗流量；不接受
  download、未知 Preview、執行中的工作、外來 scope 或已留存正向輸出。
- 核對原請求、唯一失敗事件、完成時窗、來源 scope、無進行中共享操作；重新
  讀取目前精確視窗與來源穩定證據，只允許既有白名單 notice 恢復。
- 先寫 mode 0600 durable intent，保留完整舊 task／traffic、原請求 SHA 與目前
  介面稽核；交易內再次核對，再轉回原 queue，沒有執行 Preview 或採納來源列。
- 原 request／outcome／traffic 不刪改；新嘗試仍由原 collector 執行。後續失敗
  會留下目前版本的證據，沿用已驗證的 60–900 秒有界重試，不新增桌面 writer。
- 舊 `failed_plan_proof` 的自動門檻沒有放寬；原結果未知仍是未知。

[20:34–20:37 遷移收據](../artifacts/data_quality/tej_resilience_2026-10-22/night_legacy_metadata_requeue.json)
確認 **17/17 重新排入、原失敗事件與檔案 SHA 不變、零資料 Preview**；隔離數由
22 降為 5。這是已恢復排程，不是 17 表已下載完成；仍依原資料價值優先級執行，
不把排程 metadata 當成數值或全歷史覆蓋率。

需要處理另一筆相同類型的舊工作時，只使用這個既有入口；先讓正式 writer
完成當前工作，持同一把鎖操作，不批次猜測 task／日期／來源別名：

```bash
source scripts/runtime_env.sh
run_fintech_python -m downloader.download_tej_history retry-metadata \
  --root data_tej --task-id EXACT_TASK_ID \
  --evidence data_tej/requests/EXACT_ORIGINAL_REQUEST.json \
  --acknowledge-unknown-usage
```

### 控制項查找：局部改善沒有通過整體效能驗收，已撤回

曾實作候選：`Control` 的 combo 分支每次建立、即時丟棄的 UIA `CacheRequest`，在同一次
跨程序讀取取得 Name／NativeWindowHandle。完整名稱與大小寫、所有候選中的
重複項目、原生可見性、PID／視窗／父子歸屬仍會驗證；不是快取選取結果或許可。
此方法沿用 [Microsoft 的批次屬性讀取介面](https://learn.microsoft.com/en-us/dotnet/framework/ui-automation/caching-in-ui-automation-clients)。
**完整鏈路測試未改善，最終 production 已撤回這個候選**，回到原生列舉及即時
UIA 名稱核對，並保留回傳前的新歸屬／可見性 guard，不留下未採用的 runtime flag。
候選原碼仍在 immutable bridge release
`698ba0e2b08a7af52bb8737cfde4692ee693440bc6f0b2b3bc5841ddc25503df`，可重測而不覆蓋原證據。

新增可重用的 `scripts/benchmark_tej_control_lookup.py`／`.ps1`，AST 取出原始及
候選的真正 production 函式，固定相同三個控制項，ABBA 各 30 次 lookup，檢查
前後選取與 HWND 完全一致；不送按鍵、來源查詢或修改 queue。

[實際 Wizard 比較](../artifacts/data_quality/tej_resilience_2026-10-22/night_control_live.json)：
baseline 平均 0.31275 秒，candidate 0.24970 秒，這一段約 **1.25×／減少 20.2%**。
[獨立 fixture](../artifacts/data_quality/tej_resilience_2026-10-22/night_control_fixture_final.json)
另通過重複、大小寫、隱藏、錯誤 PID／標題／父層拒絕及停用項目的讀取一致性。

保留兩個反例，不能用局部數字宣稱通用極限：

- MSAA `accName` 不一定等於 UIA `Name`，曾直接替換失敗；已撤回此候選，保留
  [原始反例](../artifacts/data_quality/tej_resilience_2026-10-22/night_control_fixture_v3.json)。
- 同程序 fixture 的候選反而較慢，最後一次約 2.88×；也觀察到 provider 在修改
  原生文字後可能繼續回舊 UIA Name。fixture 改為重建自己擁有的 widget，確認
  故障確實生效後才驗拒絕；不把失敗的故障注入誤認成 production 錯誤。

### 補上 Windows 外的等待分解

`run_guarded_windows` 新增 monotonic ACK admission／permit 寫入／獲准執行／總傳輸
計時，Windows wrapper 另拆 script staging 與 hash／parse。Linux 在既有 progress
目錄 best-effort 寫小型 sidecar，不多開一次 Windows I/O。
`profile_tej_acquisition.py` 契約升為
`tej_bounded_acquisition_timing_profile_v5_transport_and_prebridge`，只有 exact task／
attempt／prepared SHA 相符、有限且白名單內的耗時可顯示；壞 telemetry 不影響
數據採納，也不提供新重試、未送出證明或 heartbeat 續期權限。

[20:39 的八筆觀測](../artifacts/data_quality/tej_resilience_2026-10-22/night_postrestart_profile.json)
全鏈路中位數 5.73 秒、送出前 5.13 秒；其中一筆 8.53 秒的分解為 admission
1.81 秒、script staging／parse 0.32 秒、Windows 初始化 0.31 秒、scope 準備
4.09 秒、來源回應 0.92 秒。這與先前基線不是相同 scopes／負載，**不能相除
當成全鏈路加速率**；45 秒初始化長尾是否已消失也尚未證明。

### 本輪驗證與續跑邊界

- 候選撤回前的 TEJ 37 個 Python 測試檔 **2,124 passed**，前端 **52 passed**；
  [同一 run 的完整 log](../artifacts/operations/agent-workflow/runs/tej-1022-final-regression-20261022T123506-92446a46/run.log)。
- [實體 Windows 傳輸注入](../artifacts/data_quality/tej_resilience_2026-10-22/night_transport_acceptance.json)
  通過先失敗 relay／全部未獲准／獲准後錯誤／獲准後 timeout；
  [入口注入](../artifacts/data_quality/tej_resilience_2026-10-22/night_entry_gate/entry_gate_acceptance.json)
  通過 cache hit／miss 前後邊界：進入前才是 proven-not-submitted，進入後未知不重送。
- [原生編譯快取](../artifacts/data_quality/tej_resilience_2026-10-22/night_native_cache.json)
  11 項真實 Windows 檢查通過，含損壞 DLL／環境／路徑／版本、原代次保留、
  fresh mutable state、NTFS／WSL UNC 原子寫入失敗及 progress 故障不重送來源。
- [20:37 換載後八筆正式驗收](../artifacts/data_quality/tej_resilience_2026-10-22/night_production_acceptance.json)
  核對原失敗證據及新 request／raw／Parquet／receipt，均是來源明確空回、零新增列；
  [非空參考](../artifacts/data_quality/tej_resilience_2026-10-22/night_nonempty_reference.json)
  另核對舊五列、55 原始格，不能算新取得。
- [啟動登錄讀回](../artifacts/data_quality/tej_resilience_2026-10-22/night_boot_readback.json)
  確認既有 boot／interactive logon tasks、正式服務與有效 relay；未執行實體重開機。

較大價格表及非空事件的完整 ABBA 使用既有 `benchmark_tej_full_flow.py`，保留
查詢、全量讀取、正規化、Parquet、收據及獨立 audit。測試持原鎖，正式服務先
正常 drain；本次維護另有 bounded resume guard，在該次 comparison 結束後只
重啟原服務，不清除未知結果或來源 barrier。其結果與最後續跑驗收接續記於下節。

### 完整 ABBA 的負面結果，以及下一個真瓶頸

[完整比較](../artifacts/data_quality/tej_resilience_2026-10-22/night_full_flow_abba/acceptance.json)
包含固定事件 scope（5 列／9 feature）、四家公司 2024-01-01 至 2026-09-25 的
價格 scope（2,648 列／28 feature），以及同一價格表的 metadata discovery。
每版跑兩遍，ABBA 共 8 次資料查詢及 5 次 metadata plan；每版輸出 **5,306 列**，
同 scope 全部數值與 axes 的 semantic SHA 一致，production 收據沒有修改。

| 計時邊界 | baseline | UIA cache 候選 |
| --- | ---: | ---: |
| 六個 canonical 任務／版，含其抓取與落地 | 125.13 秒 | 193.66 秒 |
| 兩條完整 chain／版，另含獨立 audit 等工作 | 153.84 秒 | 215.62 秒 |

共同的一次性 discovery setup 為 14.83 秒，另計；最初等待原 writer 鎖為 98.16 秒，
正常 drain 後才取得。完整 chain 的候選約慢 **40.2%**，因此沒有採為正式加速，
也沒有剔除 77 秒慢樣本、用價格表局部較快或 microbenchmark 來取代整體結果。
這次比較不能單獨證明每一秒差異由 CacheRequest 造成；撤回的理由是沒有通過
所要求的完整效能驗收，而不是宣稱已證明此 API 必然較慢。

[77 秒樣本細分](../artifacts/data_quality/tej_resilience_2026-10-22/night_full_flow_slow_candidate_profile.json)
可明確看到：Windows script staging 17.24 秒、hash／parse 1.10 秒、初始化
29.58 秒（其中 helper fingerprint 8.17 秒）、scope 準備 21.54 秒、來源查詢
1.22 秒、完整讀回 1.09 秒。初始化仍有超過 fingerprint 的未細分等待，不可
稱成供應者限流或硬體理論極限。

為排除「慢在 Python 數據轉檔」的猜測，另用已驗證且不再變動的兩份原始輸出，
在獨立 artifact 根目錄重跑原 `ingest_export`，不呼叫來源、不更動原始檔／收據。
[cProfile 與完整 readback](../artifacts/data_quality/tej_resilience_2026-10-22/night_local_ingestion_profile.json)
結果：5 列為 0.676 秒（含首次套件載入），2,648 列為 0.687 秒；分別核對
55／79,440 個原始格。這只排除該次固定本機輸入下轉檔的主瓶頸，不抹除正式流程
曾有約 10 秒落地前等待的證據，也不省略 fsync 或正規化／收據驗證。

仍需繼續定位的是 Windows 啟動／本機檔案 I/O 長尾及跨表 source preparation；
較大表的完整原生讀回約 14–18 秒。既有 exact lookup、有限同表 locality 及
四個唯讀 MTA worker 保留；不藉由第二個 UI writer、省略完整值驗證、猜公司別名
或把缺席股票標成空資料來縮短時間。**全歷史、所有來源錯誤與理論速度極限尚未完成。**

### 最後換載與獨立讀回：20:54–20:58

撤回 CacheRequest 候選後，正式 bridge SHA 為
`b7d70b6783024adca85427b7da783fbb92c2949cd7ea0ea6ff981ff358d9878a`。
這次保留的是排程恢復、回傳前新歸屬 guard 與細分計時，**不是上述局部 20.2%
改善的候選**。原始值、欄位／公司／日期 scope、單一 UI writer、未知送出邊界及
完整收據驗證都未縮減。

- [最終回歸 log](../artifacts/operations/agent-workflow/runs/tej-1022-release-regression-20261022T125104-26c514e8/run.log)：
  **2,124 項 Python 通過（67.41 秒）、52 項前端通過**。
- [最終 Windows 控制項 fixture](../artifacts/data_quality/tej_resilience_2026-10-22/night_release_control_fixture.json)：
  exact handle／選取一致，重複、大小寫、隱藏、錯誤 PID／標題／父層等 9 項拒絕
  檢查通過；沒有來源查詢或 production queue 修改。
- [同 SHA 的原生快取故障測試](../artifacts/data_quality/tej_resilience_2026-10-22/night_release_native_cache.json)：
  **11 項通過**，沒有來源查詢。
- `stockagent-tej-history.service` 於 **20:54:26（12:54:26 UTC）** 啟動，PID
  `1161690`；網頁 owner schema 17 讀回為 running／executing。
  [開機／登入登錄讀回](../artifacts/data_quality/tej_resilience_2026-10-22/night_release_boot_readback.json)
  確認既有 boot service、API timer、interactive relay；仍需 Windows 使用者登入，
  未以實體重開機驗收目前版本。
- [換載後完整驗收](../artifacts/data_quality/tej_resilience_2026-10-22/night_release_production_acceptance_readback.json)
  核對 **8 筆換載後才開始的新工作**，request／raw／Parquet／receipt 相符，
  原失敗證據 SHA 不變。八筆都是來源明確空回，**新增 0 列**；
  [目前程式的非空參考驗證](../artifacts/data_quality/tej_resilience_2026-10-22/night_release_nonempty_reference.json)
  另核對舊 5 列、55 個原始格，不計為新下載。前一次唯讀驗收因第八筆尚未落地
  而拒絕，原失敗 run 保留；核對已有 11 筆新收據後才重新驗收，沒有重送來源。

[換載後八筆計時](../artifacts/data_quality/tej_resilience_2026-10-22/night_release_profile.json)
總任務時間 118.10 秒、中位數 7.68 秒、送出前中位數 6.82 秒。第一筆仍有
54.54 秒長尾：script staging 0.46 秒、初始化 1.62 秒、scope 準備 37.59 秒
（欄位選取 15.98 秒、context 綁定 12.03 秒）、來源回應 3.99 秒。
這與較早樣本／ABBA scope 不同，不能宣稱全鏈路加速或長尾已消失。

20:57 的驗收快照包含一筆短暫 blocked 的公司標籤缺席工作，隨後服務自行完成
`selector_scope_repartitioned`，20:57:50 又完成下一筆；獨立讀回回到原本 **5 筆
`source_validation_failed_deferred`**。這五筆原生回應的期間超出選取 scope，仍需
精確期間模型或新來源證據，不能當成壞資料已修好。17 筆本輪重新排入的舊目錄
工作仍按資料價值優先級等待；重新排入、空回完成、非空採納是三種不同進度。

可重用的新檔只有控制項 ABBA runner／Windows fixture；測試原始函式、固定
scope 與故障，不複製下載器。私有輸出、fixture 暫存與實測收據留在既有 artifact
根目錄，未加入 Git。下一個工程範圍為 Windows 啟動／檔案 I/O 長尾、跨表準備與
大表 MSAA 讀回；目前只交付已驗證的恢復修正，不把工作標為全體優化完成。
