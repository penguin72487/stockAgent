# TEJ 停滯根因與證據式自我修復

日期：2026-10-04，Asia/Taipei。本次沿用既有下載器、佇列、Windows owner
mutex、systemd 與唯讀網頁；沒有增加另一條下載管線或繞過來源限制。

## 結論與目前界限

原本 10:36 起卡住的 `585bb4000385cc4cec893d00` 已恢復：12:28:24 取得
**來源明確空回**，不是捏造有值的資料。第一次恢復於 12:26:26–12:29:02
連續完成 8 個工作，來源共 17 列、415 個非空值；8 份收據全部通過雜湊、
Parquet 筆數、鍵與 schema 驗證。

驗收期間另發現更新 bridge 與下載器複製它同時發生的競爭條件。12:29:03
工作 `84654ea119e3ea3a244d85e9` 以 `Script copy mismatch` 失敗，沒有留下
資料查詢 stage；原證據仍保留。先前因未知用量風險等待授權；使用者本次
明確同意「都可以允許重排」後，12:46:32 這一筆已用新嘗試完成，取得
**6 列實際來源資料**。原失敗 attempt 仍為 `unknown_outcome`，沒有被改寫成
未送出、未消耗配額或成功；授權與新收據分別保留。

另已加入**明確授權、有界的自動重排**，沿用相同隊列與一次性授權 ledger。
最新部署、持續下載與收據核對，以本目錄新的 `engineering_acceptance_v2.json`
及 `receipts_after_authorized_restore_v2/audit.json` 為準；之前的等待證據不覆寫。

未來的同類檔案複製／語法失敗會有可驗證的「尚未進入 bridge」證據，可走
原本的安全重試流程。沒有充分證據的舊嘗試不因此被改寫成未送出。

仍有 22 個既有來源驗證／清點隔離工作；本次未宣稱它們全部修復、全歷史完成、
來源底層精度或發布時間已驗證。

## 第一性原理：程序存活不是資料前進

一次工作完整時間是「啟動、來源綁定、選擇範圍、查詢、完整讀回、驗證入庫、
必要等待」的總和。來源結果不明時重啟程序，不會自動消除不確定性。
修復必須定位實際邊界，不能只刷新 heartbeat 或反覆送 Preview。

| 原因／邊界 | 本次證據 | 修正與可恢復條件 |
| --- | --- | --- |
| 上層 Type 已是 TDR，下層 SmartID／Data 卻為空 | 獨立選單診斷；原 Windows worker SHA 與精確 stack | 子選單為空時，對仍吻合的上層來源只通知一次，然後重新讀回；不送 Preview |
| 32 位元控制項的 `CB_ERR` 以 `4294967295` 回傳 | 原生 index 與 Windows fixture | 僅將 `0xFFFFFFFF` 解為 `-1`，不截斷其他 index／handle；未選取回傳 null |
| 送出前例外未留下負證據，被誤分類為未知查詢 | 原 catalog throw 與呼叫位置均在唯一 Preview 呼叫之前 | 受控 catch 覆蓋所有查詢準備例外，要求未跨提交邊界、沒有 stage，綁定 task／attempt／來源與觀測時刻 |
| 已送出的查詢晚到，排程只找本機已保存結果 | 原流程沒有自動讀回 retained Preview | 先核對本機收據，再核對原 stage、選擇與全量結果；禁止重送 Preview |
| 更新 checkout 與腳本複製競爭 | 新阻塞工作原診斷 `Script copy mismatch` | 每個 attempt 固定 SHA 版本快照；Windows 副本只對固定 SHA 核對，先 parse 再執行，入程式前失敗留下負證據 |

Microsoft 的 [CB_GETCURSEL 文件](https://learn.microsoft.com/en-us/windows/win32/controls/cb-getcursel)
指出沒有選取時回傳 `CB_ERR`；[CB_FINDSTRINGEXACT 文件](https://learn.microsoft.com/en-us/windows/win32/controls/cb-findstringexact)
指出找不到時亦回傳 `CB_ERR`，且查找不區分大小寫。因此仍需核對完整來源字串、
Unicode 與重複名稱，不能只相信 native index。

## 自動恢復流程與上限

1. 先在同一 dataset lock 下核對已存在的完整結果與送出前負證據。資料庫短暫
   忙碌沿用原流程重試，不重置未知查詢。
2. 來源選單缺失：每次準備最多對每個空的子選單刷新一次吻合的父來源。
   通知逾時不在該次呼叫重複事件；全部 binding 必須再次核對。
3. 可證明未送出的失敗：共用原本兩次立即安全重試；用盡後，先驗證介面，
   再沿用逐表 60、120、240…最高 900 秒的持久重試窗口。沒有七天冷卻，
   可以隔離的工作不堵住其他健康表；不使用無限忙迴圈。
4. 已送出／可能已送出的查詢：僅原有效 stage、唯一完成的 active attempt
   可自動核對 retained response。使用 `recover_preview` 或精確的來源空回，
   不採納舊 Preview、不切換新範圍、不再送資料查詢。
5. 原結果尚未可驗證：持久化 60、120、240、最高 300 秒的讀回檢查間隔，
   即使服務重啟也不重新消耗立即重試；readback-only 耗時不計入新下載吞吐量。
6. 使用者明確授權的未知結果重排：先採納本機結果，再收回原 Preview；只有
   未取得有效結果，且原 request／active attempt／來源 scope 一致、原嘗試
   已結束、目前 pinned 介面獨立核對為穩定可查詢、沒有其他未解嘗試，才產生
   新的一次性授權。原結果可能已消耗流量，這項風險會寫入私有 audit。
   到期重排前會再次檢查晚到結果；不因 readback 的冷卻而跳過可用舊結果。
7. 重排冷卻 60–300 秒；**每筆滾動一小時最多 2 次、全域 6 次**。這是本機
   防止反覆重送的恢復預算，不是官方配額。已消耗的手動重排也計入；SQLite
   持久紀錄不因重啟歸零。用盡後等最早紀錄移出窗口，再自動檢查，不永遠停用。
8. 登入、授權、配額、任意／多重通知、不一致欄位／公司／真正使用的日期軸、
   缺失或無法確認的來源證據、其他未解來源嘗試：保留 barrier，不任意按確定。
   無 stage 只接受已清點的精確舊 WSL／copy 診斷與授權，不能把任意錯誤當成
   未送出。Key=1 快照不要求不存在的歷史日期軸，也不將快照擴寫成歷史。

新設定為 `automation.response_recovery_contract`：
`exact_retained_preview_no_resubmission_recovery_v1`。未知設定在任何來源操作前拒絕。
一般只讀 metadata probe 上限 30 秒；完整 grid readback 仍保留有限的 900 秒。
這些是工作截止時間，heartbeat 不得延長它們。

授權重排契約為 `automation.authorized_unknown_replay`：
`exact_unknown_download_user_authorized_replay_v1`，保存本次明確授權的觀測時刻
與範圍。不支援的契約、未來／無時區授權時刻、無界或型別錯誤的限制，均在
操作來源前拒絕。授權會在檢查介面前與實際消耗一次性授權時各核對一次。
`unknown_outcome_auto_retry=false` 仍代表**未授權、無條件重送關閉**，不表示
新加入的「有授權、有核對、有持久預算」路徑關閉。
若要撤回持續授權，將 `automation.authorized_unknown_replay.enabled` 改成
`false`，再正常重啟同一個 TEJ service；不刪除任何原始資料或收據。

## 程式版本與原證據

- 原 catalog 失敗 worker SHA：
  `05f981e3116de763ae947f579287b03079a46aca46f0c53f5a0c7b8aa675f72f`。
  相容恢復只接受這份原程式的精確 throw、caller、stack 與相同 active attempt，
  再獨立核對原 pinned 介面可用。沒有補造舊 stage 或回寫舊觀測日期。
- `data_tej/bridge_releases/<SHA>.ps1` 保存不可變的程式內容。
  `data_tej/launches/<attempt>.bridge.json` 綁定 prepared request SHA 與 code SHA。
  已存在的 attempt 不可重新綁定版本；被修改的版本物件拒絕覆寫。
- Windows wrapper 在執行 bridge 前完成副本 hash 與 AST parse；進入 bridge
  的旗標在呼叫之前設定。只有旗標仍為 false 的例外可產生入程式前負證據。
- 每份原始結果、Parquet、來源收據、值、單位、範圍與優先排序均保留。
  不因修復加入 log、插值、假數值、歷史完整標記或來源配額猜測。

## 網頁

沿用 [TEJ 進度頁](https://penguin72487.ddnsgeek.com/tej/)，public schema 升為 14。
新增「自動核對原查詢結果 · 不重送」與選單／準備修復標籤，仍每 5 秒讀取
本機唯讀快照。修復不計為下載完成，不假造目前資料表、筆數或完成日期。
瀏覽器不讀私有 worker 路徑、Windows owner、原始數值或 API 金鑰。
新增「依使用者授權核對／重排」、本機重排上限、冷卻／預算的下次到期與
不可確認的介面狀態。修復與重送不假造新下載筆數，只有通過驗證入庫的收據
增加完成量；實際來源操作仍由下載器執行，網頁不呼叫 TEJ API。

## 驗證與重跑

- 原 Windows list fixture 已接受 32／64 位元錯誤碼、空選取、exact case、Unicode、
  重複名稱、owner／disabled controls 與不奪取前景的行為。
- 實機 source binding 修復 6.536 秒，精確回到 TDR / TEJ TDR(TEST) /
  TDR Cash Dividends，`market_data_query_submitted=false`。
- 新 Windows 入程式邊界故障注入使用獨立的假腳本，沒有接觸 TEJ／Excel：
  parse 失敗留下 `proven_not_submitted`；進入假腳本後的錯誤仍是
  `unknown_outcome`，沒有假造負證據。
- 原結果自動恢復的回歸涵蓋完整格點、精確空回、stale／foreign result、
  配額／登入提示、缺原 stage、原證據修改、重啟 backoff 與共用 watch 續排。
  真正「已提交後晚到」的來源故障本次沒有刻意製造；該路徑有整合測試，
  不冒稱已對 TEJ 每種故障做實機重現。
- 第一次完成 8 份來源收據的局部 integrity audit；授權後的新增收據另做局部
  核對，不掃全部舊資料來代替本次接受。
  瀏覽器桌面／手機已驗證正常下載與安全等待畫面、輪詢／篩選與進度量綁定。

證據位於 [本次接受目錄](../artifacts/data_quality/tej_self_healing_2026-10-04/)：
`baseline.json`、`windows_list_*.json`、`windows_entry_gate_final/`、
`receipts_after_first_restore_v1/audit.json`、瀏覽器接受與工程收據。

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q test/test_tej*.py
node --test test/test_tej_dashboard_requests.mjs
run_fintech_python scripts/verify_tej_bridge_entry.py --output artifacts/NEW_ENTRY_ACCEPTANCE
run_fintech_python scripts/verify_tej_dashboard.py --output artifacts/NEW_BROWSER_ACCEPTANCE
```

`NEW_*` 必須是全新的接受目錄；驗證不覆寫舊證據。
舊 copy 失敗的單次 operator replay 仍有獨立 CLI 旗標與流量確認。自動排程
只能透過另行配置、受持久預算限制的本次 standing grant 使用同一個 replay
實作，不把缺 stage 當作授權，也不將未知結果改稱「保證未消耗流量」。

## 授權後新增的驗證

- 13:01:56 的局部收據快照：授權恢復後 **38 個工作、94 列、2,258 個非空值**；
  38 份均有 attempt SHA 綁定、沒有 integrity failure。13:02:35 桌面／手機
  網頁接受，console error 與外部 provider request 都為 0。
- 最終相關回歸為 **1,609 個 Python、15 個 JavaScript 測試**。
  `engineering_acceptance_v2.json` 記錄部署後另完成 10 個工作、目前未知阻塞
  為 0，以及授權／設定／程式／audit 的指紋。這是有時間戳的接受快照，
  不是固定不變的當下進度或全歷史完成證明。
- 既有 22 個隔離工作為：選單準備 15、來源資料驗證 5、來源選單配置 2。
  它們保留原證據，與本次已解的未知查詢阻塞分開；授權不是越過資料驗證。
- 新嘗試取得真實資料，舊 attempt／request／診斷保持原樣；授權不得重複消耗。
- 關閉或撤回授權、錯誤 request、已有本機結果、原嘗試未結束、多重未知工作、
  stale／foreign 介面、登入／配額提示，均不得送新查詢。
- 手動重排納入共用一小時窗口；逐筆與全域用量、到期、probe backoff 不因
  重啟歸零。健康 queue 不增加來源探測或等待，每個 supervision cycle
  最多執行一次新工作；原結果恢復不當成新下載速度樣本。
- 到期重排先重新收回晚到結果，Key=1 快照保持沒有歷史日期的原生粒度。
- 公開狀態僅投影白名單，桌面／手機驗證真實來源輪詢與授權狀態的原位更新。
