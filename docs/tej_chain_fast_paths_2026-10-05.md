# TEJ 全鏈路最快可驗證路徑

## 結果與界線

最終共享讀值版本通過真實來源 AB／BA，已於 15:48:42 恢復正式服務。
本報告的比較限於目前這台電腦、Smart Wizard 4.1.1.7、所列來源範圍及負載。
目標是減少完整取得的時間，不是減少特徵、少讀儲存格或省略驗證。

| 最終版 同一範圍各兩次 | 原版 | 新版 | 減少 |
| --- | ---: | ---: | ---: |
| 混合完整取得 包含操作 驗證 Parquet 收據 | 251.27 秒 | 211.13 秒 | 15.98% |
| 混合鏈路 再含額外獨立工程稽核 | 261.29 秒 | 231.91 秒 | 11.25% |
| 6,544 列大表 完整取得平均 | 78.94 秒 | 55.85 秒 | 29.25% |
| 大表 完整 Windows 讀回平均 | 59.57 秒 | 35.61 秒 | 40.21% |
| 大表 Windows 橋接程式 CPU 時間平均 | 21.40 秒 | 11.51 秒 | 46.22% |

大表原版兩次為 74.04／83.84 秒，新版 52.15／59.55 秒。
CPU 僅是橋接程式，不是 TEJ、Excel、伺服器或整台主機。
小表與目錄不保證更快；本輪事件、相鄰小批次、目錄平均分別慢約
8.4%、12.5%、23.6%，明確保留在[完整分項決策](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/performance_decision.json)。
公司已選清單符合的操作階段仍從 1.65 秒降到 0.077 秒，但編譯、來源等待與
外部負載可抵銷這部分收益。因此只宣稱測量的混合總時間與大表改善，不宣稱
每一筆、每一個階段都變快，也不宣稱已達所有介面的理論極限。

這次的原版腳本 SHA 是
`012fb68e455c937c5aa45297baff7f7d16bf7126824628ee5c38f7178264fae1`。
前面的[原生操作優化](tej_full_flow_speed_2026-10-05.md)及
[大表列舉優化](tej_large_table_speed_2026-10-05.md)保留為各自的歷史實驗，
不同分母的改善百分比不可相加。

## 從成本拆解到方法選擇

完整時間是排程、Windows admission／啟動、範圍操作、來源回覆、完整讀值、
序列化、驗證及收據提交的總和。等 UI 狀態改變與等來源回覆不同；工作者變多
也不會增加來源授權或把來源的串行部分變成並行。

[原版最近 32 筆收據的唯讀剖析](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/baseline_latest32.json)
包含 24 筆來源明確空回及 8 筆非空，完整時間中位 6.88 秒。
查詢前準備中位 6.18 秒；因此小表主要要消除重複操作，大表則主要要改善
逐值跨程序讀取。這是當時的混合樣本，不是每一類表的固定速度。

| 鏈路階段 | 選用或保留的方法 | 驗證與不能省的部分 |
| --- | --- | --- |
| 排程／所有權 | 既有索引排程、價值優先及有限同表連抓；單一 Wizard 寫入者 | 原鎖、API／桌面分工及游標不變；不能同時改一份來源條件 |
| Windows admission／初始化 | 原本 durable admission、活躍互動 relay、不可變腳本 | 每次 request／attempt 與腳本 SHA 綁定；不改成未驗證常駐 UI 程序 |
| 來源／群組定位 | 原生精確控制項查找 | 當前程序、視窗、enabled、來源 binding；不快取權限結論 |
| 欄位選取 | 原生批次與完整欄位讀回；相同當前欄位不重選 | 順序、Unicode、實際來源欄位；不用滾頁或全域鍵盤 |
| 公司查找／選取 | 完整當前已選清單相同時不 Clear／重選 | 當前 binding、完整名稱；全選還須核對當前完整可用清單，不只比筆數 |
| 日期／頻率 | 已驗證日期模型、原生 Search／精確全選 | 理論日期不是來源觀測；完整當前已選日期及頻率仍核對 |
| 查詢／回覆 | 一次 owned Preview，依結果轉換判斷 | 不加固定安全等待；可能送出的結果不能猜成未送出 |
| 完整讀值 | 專用 MTA；大表最多 4 個唯讀工作者，小表串行 | 每列每欄、獨立物件路徑樣本、最新全部角色及列數穩定檢查 |
| 驗證／入庫 | 原 canonical 原值、鍵、單位、Parquet、收據 | 不四捨五入、不改金融精度、不把缺值變零；收據提交後才算完成 |
| 進度／預估 | 既有 2 秒讀回 metadata、5 秒唯讀網站刷新 | 不逐格寫磁碟；讀值 100% 不等於入庫完成；網頁 GET 不呼叫來源 |

[機器可讀方法清單](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/method_plan.json)
保留其他候選、選擇理由與驗收要求。

## 為什麼採用有上限的唯讀併行

原來完整讀回在呼叫端 STA 執行。新版只把 HWND、數量界線及進度識別交給
專用 MTA；所有 COM 介面在該 MTA 內取得及釋放，傳回 managed 字串陣列，
不把 STA 的介面指標直接交給另一個 apartment。依
[Microsoft 的 MTA 契約](https://learn.microsoft.com/en-us/windows/win32/com/multithreaded-apartments)，
同一程序的 MTA 工作者共用 apartment；這不免除跨程序呼叫，也不保證 TEJ 的
伺服端可無限並行。本文的吞吐結論來自實測，不是由文件推論。

只有已完整列舉至少 512 個來源列物件的大表才啟用最多 4 個工作者。
工作者讀取互斥的完整列，寫回原本索引；原子統計總字元數、回退次數及已讀列槽。
串行與併行共同使用一份 `ReadPreviewRow`，避免未來空值、錯誤或欄位布局修正
只修到其中一條路徑。查詢、條件修改、UI 動作及目錄規劃仍串行。

每一格沿用官方 [get_accValue](https://learn.microsoft.com/en-us/windows/win32/api/oleacc/nf-oleacc-iaccessible-get_accvalue)
的來源顯示字串；空值、正負號及原始數值表示不改動。
只有明確不支援 child ID 的兩種原有 COM 錯誤可回退原物件路徑；
RPC 失敗、斷線、未知角色、欄位布局改變、容量超界或不完整列都停止採納。
完整 pool 結束後，才重新檢查目前所有列角色及數量，並依原順序提交。

外層以 [Thread.Join 的有界等待](https://learn.microsoft.com/en-us/dotnet/api/system.threading.thread.join?view=netframework-4.8.1)
保留 STA 訊息／COM 的處理機會；讀回超過 300 秒會保留原 attempt，
不直接重送已可能執行的來源查詢。進度不會產生額外 COM 讀取。

## 候選實測與沒有採用的方法

| 候選 | 本次驗證 | 決策 |
| --- | --- | --- |
| 直接 COM vtable 讀值 | 256 列及 [1,500 列完整 fixture](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/native_value_fixture1500.json)；完整值及樣本比對 | 1,500 列沒有穩定勝過原版，未對來源使用或部署 |
| 單一 MTA 讀回＋公司 no-op | 真實來源完整 AB／BA | 混合時間 217.11 → 185.57 秒；後續用修正全選名稱守門的 4 工作者版本取代 |
| 2 個唯讀工作者 | [1,500 列完整 fixture](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/parallel2_fixture1500.json)、7 項異常／穩定檢查 | fixture 可用；沒有把 fixture 當作真實 TEJ 吞吐證明 |
| 4 個唯讀工作者 | fixture、原版對照、4 對 8 真實來源 AB／BA | 選為本次最快已驗證的有限候選 |
| 8 個唯讀工作者 | [3 次 fixture 交錯](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/parallel8_fixture1500.json)及真實來源完整 AB／BA | fixture 較快，真實混合流程反而比 4 個多 4.08% 時間，不部署 |
| UIA 快取、Excel Value2、剪貼簿 | 參照前輪測試／現有能力契約 | UIA 前輪未取得等價完整結果；另兩種缺少目前大表表示與來源證據等價證明 |
| 編譯快取／持續常駐橋接 | 清點成本與復原責任 | 尚未取得完整鏈路收益及 release／compiler／工作生命週期證明，不冒充已實作 |

[4 工作者對原版](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/abba-real-chain-v5/acceptance.json)
混合完整時間 247.50 → 184.10 秒，減少 25.62%；大表平均 73.54 → 49.30 秒。
此版本尚有重複讀值程式，最終部署另用下節共享讀值版本的精確 SHA 與驗收。

[4 對 8 真實直接比較](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/abba-parallel4-vs8/acceptance.json)
完整混合時間分別 175.44／182.61 秒；大表兩次平均 49.72／53.31 秒。
兩次大表時間範圍仍重疊，不能宣稱 4 個在所有負載下必定最快，或 8 個永遠較慢。
在本次速度優先的有限候選中，採用已量得較短總時間的 4 個。

所有私有候選與失敗證據均保留，沒有重置正式資料或已完成收據。
8 工作者 fixture 的第一次啟動遇到 WSL `UtilAcceptVsock` 錯誤；
原 admission 證明 Windows bridge 尚未獲准執行、資料查詢不可能送出，
才重跑同名工程測試。這是啟動故障，不是把來源結果未知當成安全重送。

## 最終版本完整鏈路驗收

最終結果以
[沒有重疊回歸測試的共享讀值 AB／BA](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/abba-final-isolated-v7/acceptance.json)
為準；測試包括來源操作、完整讀值、正規化、驗證、Parquet 及不可變收據，
每筆下載還做獨立 raw／Parquet／schema／鍵／筆數／雜湊稽核。

前一輪[共享版本在變動負載下的完整對照](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/abba-final-shared-v7/acceptance.json)
也保留：272.18 → 268.37 秒，只減少 1.40%；包含獨立工程稽核反而
285.12 → 287.03 秒。該輪曾重疊完整 TEJ 回歸，且未逐筆量得所有外部負載，
不能把全部差異歸因於回歸，或刪除不利結果後宣稱固定收益。
因此另做了沒有本任務回歸重疊的最終對照；既有其他工作不停止。
[負載觀察](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/runtime_load_during_isolated.json)
是 WSL 在該輪某時點的 16 邏輯 CPU affinity、load 與記憶體，
不是所有樣本、Windows CPU entitlement 或完整 cgroup 配額的證明。

部署腳本是[原橋接程式](../scripts/tej_smart_wizard_bridge.ps1)，SHA
`de02a50def7774e4ad5bfa9869b0e380d0cc4fef4975c0db6bc10462054c51b0`，
讀回識別 `owned_mta4_shared_row_complete_stability_v7`。
每次已測 candidate、正式 request、不可變 release 與新完成收據必須綁定同一 SHA。

每種版本各跑兩次以下工作：9 列除權事件、相鄰不同欄位批次、來源明確空回、
4 家公司 × 1,636 個實際來源日期 × 28 個特徵的大表（6,544 列、含鍵共 30 欄），
以及實際公司／日期／欄位目錄規劃。價格期間為 2020-01-01 到 2026-09-25，
不改正式下載的歷史起點。AB／BA 平衡順序用來降低暖機及先後順序偏誤。

[最終大表 fixture](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/shared_row_fixture1500.json)
及[小表串行 fixture](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/shared_row_fixture256.json)
均已通過完整值／獨立樣本比對及 7 項穩定或拒絕檢查：未改變、加列拒絕、
還原接受、列數超界拒絕、視窗銷毀拒絕、完整讀值欄位改變拒絕、完整讀值已銷毀
視窗拒絕。變更測試只能操作工具建立的隔離表格，不能改真正 TEJ 表格。

完整 TEJ 回歸 **1,732 passed、1 skipped**；讀回與完整鏈路 focused 回歸
另有 **44 passed**。沒有用 fixture 的加速百分比當成來源吞吐，或把
`accepted` 的資料等價旗標誤解為任何候選都比較快。

## 部署與復驗

正式服務在當前 bounded 工作完成後正常停止，再做隔離比較；
新版通過來源驗收及回歸後才恢復。部署需同時核對新完成收據、精確 attempt、
不可變橋接腳本 SHA、目前互動 relay、systemd 與公開唯讀網站，不能只看 active。

[正式部署驗收](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/deployment_acceptance.json)
已通過：服務 active／running、沒有服務重啟、互動 relay 活躍，精確選取的
8 筆新收據都綁定上面的已測腳本 SHA；其中 2 筆非空、6 筆來源明確空回。
[獨立 raw／Parquet／收據稽核](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/production_receipts/audit.json)
全部接受，共 6 列、138 個非空特徵值，沒有本機 artifact 錯誤。
這 8 筆是明確驗收範圍，不把服務完成總數當成每筆已獨立稽核。

本機及公開 [TEJ 面板](https://penguin72487.ddnsgeek.com/tej/) 在驗收時顯示
相同的新完成紀錄及目前工作，保持唯讀、不公開原值，仍由原有 5 秒刷新連動。
本次沒有新增 blocked；22 筆既有資料驗證／介面阻擋仍在，未冒充已修復或
全資料已抓齊。開機 watchdog、API 分工、配額及價值優先排程未更動，
也沒有由本次服務恢復宣稱完整主機冷開機已測試。

[可重跑的精確版本部署核對工具](../artifacts/data_quality/tej_chain_fast_paths_2026-10-05/verify_live_deployment.py)
只讀目前 queue、immutable release、收據、relay 及面板；沿用原稽核和剖析，
不建立第二條下載管線、不呼叫 TEJ 來源、不更改排程或重送查詢。

重測命令使用 `source scripts/runtime_env.sh` 及 `stockagent-agent run`：

```bash
run_fintech_python scripts/benchmark_tej_full_flow.py --root data_tej \
  --baseline data_tej/bridge_releases/012fb68e455c937c5aa45297baff7f7d16bf7126824628ee5c38f7178264fae1.ps1 \
  --candidate scripts/tej_smart_wizard_bridge.ps1 --output NEW_PRIVATE_OUTPUT \
  --case f269f06e5b4f8922d68cf0b2 --case 8bb751bd3fc2043f4602eb85 \
  --field-batch-case f269f06e5b4f8922d68cf0b2 --dense-start 2020-01-01 \
  --allow-source-requeries
run_fintech_python -m pytest -q -s test -k tej
```

上述來源測試每次額外提交 16 次資料查詢、5 次目錄規劃，需先有意識地停止正式
寫入者並遵守來源限制；不是可以任意與正式工作競爭的唯讀診斷。
本任務五輪完整來源比較合計 80 次資料查詢、25 次目錄規劃；
GUI 內部實際 HTTP 次數未知，不能把上述工作數當成官方 API 呼叫量。
既有結果的無查詢比較則使用 `benchmark_tej_preview_readback.py` 的 fixture 或
精確已完成來源模式。來源模式仍需空閒且當前顯示的完整範圍與原收據完全一致。

理論上，逐格介面完整取得 `R × C` 個原值至少要處理同量的輸出，現行讀值仍是
`O(R × C)`；併行降低等待，不把資料量變成常數。尚無可驗證的 GUI 官方最高
吞吐或等價 bulk endpoint 下界，因此只能說本次候選與範圍的實測最快，
不能宣稱所有 TEJ 表已達普遍的理論極限、所有歷史已完整或精確 PIT 安全。
