# TEJ 大表讀回加速與完整鏈路驗證

## 結果與適用範圍

2026 年 10 月 5 日，針對既有 TEJ Smart Wizard 下載器完成真實來源的交錯
AB／BA 對照。每次價格查詢包含 4 家公司、1,636 個來源日期、6,544 列、
28 個特徵及 2 個鍵，共 30 欄。歷史測試範圍是 2020 年 1 月至
2026 年 9 月 25 日；沒有更改正式下載器的歷史起點或資料範圍。

| 同一份真實大表 各跑兩次 | 修改前平均 | 修改後平均 | 減少 |
| --- | ---: | ---: | ---: |
| 完整取得 包含來源操作 驗證 Parquet 與收據 | 88.37 秒 | 73.66 秒 | 16.65% |
| 完整 Windows 讀回 | 74.34 秒 | 57.89 秒 | 22.13% |
| Windows 橋接程式 CPU 時間 | 26.02 秒 | 20.08 秒 | 22.82% |

修改前兩次完整時間為 87.08／89.66 秒，新版為 75.01／72.30 秒。
以上 CPU 是橋接程式本身的處理時間，不是 TEJ、Excel、來源伺服器或整台主機的
CPU 用量。資料是廠商顯示字串，不把它冒充底層 Excel 數值的精度證明。

另外同時測試 9 列稀疏事件與來源日期／公司目錄規劃。混合工作合計
210.14 秒降至 187.88 秒，減少 10.59%；再包含獨立收據稽核的連續鏈路，
216.71 秒降至 196.97 秒，減少 9.11%。小表有明顯負載波動，不能宣稱每筆都變快。
這些比較的分母不同，不能相加，也不能與[前一輪操作加速](tej_full_flow_speed_2026-10-05.md)
的百分比相加。

[原始完整 AB／BA 結果](../artifacts/data_quality/tej_large_table_speed_2026-10-05/abba-real-large-v2/acceptance.json)
包含每次完整時間、Windows 分段、實際列數、橋接程式版本及完整來源值的語義
雜湊。[大表部署決策](../artifacts/data_quality/tej_large_table_speed_2026-10-05/large_table_decision.json)
保存上述平均值與計算公式的輸入。不是官方最高吞吐、所有表最快或全歷史完整的證明。

## 瓶頸與修改

完整取得的時間由排程、Windows 啟動、來源範圍準備、查詢回覆、完整讀回、
驗證及入庫構成。這次原版的大表讀回占整筆時間約 84%；
[2,648 列既有資料的離線完整驗證](../artifacts/data_quality/tej_large_table_speed_2026-10-05/offline_validation_profile.json)
中位約 0.059 秒，因此沒有省略驗證或更動金融數值來改善時間。

原版讀回前會完整取得每列的 MSAA 物件；讀完所有值後，再完整取得另一組跨程序
物件，確認列角色與筆數。新版仍保留第一次完整列舉、全部儲存格、獨立的物件路徑
樣本、欄位／鍵／容量／原值驗證；第二次則直接核對每個目前子項的角色及完整列數，
不再為檢查建立數千個新物件。

這使用官方 [IAccessible get_accRole](https://learn.microsoft.com/en-us/windows/win32/api/oleacc/nf-oleacc-iaccessible-get_accrole)
的子項 ID 參數。這裡仍是逐列檢查，不把單次 API 呼叫當作整張表檢查。
來源若回報不支援子項 ID，僅限既有的兩種明確 COM 錯誤，退回原本完整物件列舉；
斷線、未知角色、非整數角色、容量超界或列數改變仍會停止採納。

新版也在欄位標頭、形狀、獨立樣本及值讀取回退路徑加上 `finally`，及時釋放
這次取得的 COM 參考。沿用官方 [AccessibleChildren](https://learn.microsoft.com/en-us/windows/win32/api/oleacc/nf-oleacc-accessiblechildren)
的介面生命週期要求；沒有減少資料欄位、將缺值改成零、改單位或改用低精度浮點數。

實作在[原有橋接程式](../scripts/tej_smart_wizard_bridge.ps1)，讀回識別為
`shared_rows_complete_child_role_stability_v2`。完整腳本 SHA 為
`012fb68e455c937c5aa45297baff7f7d16bf7126824628ee5c38f7178264fae1`。
修改前 SHA 為 `fc1bdf963852e053ed6875a29d81898c63bfc59b79368fbea4120b82ec928ae6`；
原版 immutable release、資料、收據與失敗候選均保留。

## 等值驗證與被拒絕的候選

真實對照由原本 `run_one` 執行完整查詢、資料驗證、Parquet 與收據。每個下載
還做一次獨立的 raw／Parquet／schema／鍵／非空計數／收據稽核。
兩種版本各完成 13,106 列，其中大表各 13,088 列、事件各 18 列。
全部欄位、鍵與相應來源顯示值一致；沒有拿估計列數當成觀測列數。
測試額外執行 8 次資料查詢、5 次目錄規劃，正式已完成收據沒有被重置或改寫。

[隔離測試表格](../artifacts/data_quality/tej_large_table_speed_2026-10-05/child_role_fixture_v1.json)
也做完整值與樣本比對。[異常變更測試](../artifacts/data_quality/tej_large_table_speed_2026-10-05/child_role_mutation_acceptance.json)
確認穩定表格可接受、增加資料列會拒絕、還原後可接受、列數上限會拒絕，
以及已銷毀的視窗不能繼續採納。這些 fixture 不代表 TEJ 的財務資料本身。

另試 UIA 批次快取。依官方[執行緒建議](https://learn.microsoft.com/en-us/windows/win32/winauto/uiauto-threading)
改到獨立 MTA 執行緒後，仍未取得可接受的完整結果；64 列測試中，MSAA 約
1.16 秒，快取候選約 10.02 秒後被拒絕。
[拒絕證據](../artifacts/data_quality/tej_large_table_speed_2026-10-05/bulk_uia_MTA_fixture64.json)
保存方法、時間與 `candidate_accepted=false`。程式限制只可存取自己建立的
fixture，沒有對來源提交資料查詢或採納資料。不由這個失敗推論所有 UIA 快取
必然不可用。

沒有用 Excel Value2 或剪貼簿取代既有顯示字串流程；它們仍需要另證大表範圍、
精度、值表示、空值與來源證據等價。單一 Wizard 的查詢條件與結果有共同可變狀態，
因此沒有透過多開競爭同一桌面來換取表面吞吐。

## 正式服務與網站驗收

14:00:47 恢復正式服務，14:05:27 的驗收快照顯示 28 次工作完成，
沒有服務重啟、新增 blocked 或未解除的桌面介面 gate。22 個既有資料驗證／
介面阻擋仍存在，本次沒有把它們算成已修復或全資料完整。

[8 筆部署後收據的獨立稽核](../artifacts/data_quality/tej_large_table_speed_2026-10-05/production_receipts/audit.json)
全部接受：其中 2 筆非空、6 筆來源明確空回，共 6 資料列、138 個非空特徵值。
每筆原始 request、attempt、不可變腳本 manifest 及 SHA 都與上面通過真實
AB／BA 的版本一致。[部署驗收](../artifacts/data_quality/tej_large_table_speed_2026-10-05/deployment_acceptance.json)
另綁定活躍互動 relay、正式服務、實際完成紀錄及網頁目前的工作階段。
28 次是當時的運行計數，不是 28 次均做了上述獨立稽核；稽核範圍明確為 8 筆。

本機與公開的 [TEJ 面板](https://penguin72487.ddnsgeek.com/tej/) 都回報相同的新
完成紀錄，目前工作與階段會由原有 5 秒刷新更新，保持唯讀且不公開原始數值。
網站 GET 不會呼叫 TEJ 來源補畫進度。沒有用 HTTP 200 取代收據驗收，
也沒有由一次服務重啟宣稱完整主機冷開機已測試。

現行完整 TEJ 回歸為 **1,723 passed、1 skipped**，覆蓋來源範圍、原值、
欄位、鍵、容量、未知結果、受控恢復、immutable release 與進度面板。
另有實際 Windows fixture 的 5 項結構／銷毀檢查及真實來源完整 AB／BA。

## 可重跑的量測與恢復

[完整鏈路測試工具](../scripts/benchmark_tej_full_flow.py)新增候選快照參數，
可在不修改正式橋接程式時測量。維持原 canonical collector lock、來源介面
gate、每次不可變腳本、精確 request／attempt 與未知結果保護。
若實際查詢失敗，保留原 attempt 與介面 gate，先核對原結果，不自動重送。

[同一個既有結果的讀回比較](../scripts/benchmark_tej_preview_readback.py)
改用 canonical Windows durable admission 與真正的互動 relay，不再繼承
短命 WSL shell 的 Windows 啟動通道。現行原版已有 `CaptureFullPreview` 時，
比較必須使用同一個完整 capture，而不是讓原版多做一次舊的列舉。
測試變更／銷毀只允許 `--fixture --check-stability`，不能操作真實來源表格。

正式服務會在當前 bounded query 完成後正常停止，驗證通過才套用新版，
再恢復 `stockagent-tej-history.service`。進度仍沿用逐列讀回的既有事件，
讀值到 100% 不等於入庫完成；只有資料／Parquet／收據提交完成才累加已取得。
既有 API 分工、價值優先序、同表有限連抓、官方配額與 boot watchdog 未更動。

這次改善後，全值讀回仍是主要成本，複雜度仍為 `O(列數 × 欄數)`。
來源可用日期、權限、修訂版本及既有 blocked 缺口與效能問題是不同的證明，
不能用服務 active 或本次 benchmark 宣稱所有 TEJ 歷史資料已抓齊。
