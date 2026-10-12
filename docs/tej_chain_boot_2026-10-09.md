# TEJ 全流程操作加速與開機恢復（2026-10-09）

## 驗收範圍

本輪使用既有 TEJ Smart Wizard 下載器，測量操作、來源回覆、完整讀值、驗證、
Parquet 及收據提交。正式工作不減少股票、年份、欄位或驗證；來源原值、缺值、
單位、API／桌面分工與未明結果證據不變。

已部署本輪同範圍 AB／BA 實測較快的版本，正式下載持續產生新收據。
完整取得合計 **187.70 → 170.33 秒**，耗時減少 **9.25%**、吞吐量增加 **10.20%**；
加上獨立稽核的完整實驗鏈為 **198.61 → 180.68 秒**，減少 **9.03%**。
兩版各取得 13,124 列，所有同範圍來源值一致；不是靠少抓資料、跳過讀值或降低
驗證換取速度。這是有限候選的最快實測結果，不宣稱所有介面／負載的理論最高速。

- 新版本 8 筆正式收據獨立稽核通過，共 31 列；原始欄位的 870 個資料格全部與 Parquet 一致。
- 相關回歸 1,735 項通過、1 項跳過；Windows 批量選取 122 項、程式快取 11 項及啟動政策 16 項通過。
- Windows boot 任務已實際再派送原 WSL 服務與 API timer；登入後的桌面監督已自動恢復卡住的 launcher。
- 本機面板與 Windows 本機／LAN HTTPS 顯示新收據；公開 DNS 位址連線逾時，不能宣稱 WAN 驗收通過。
- 未強制重開整台電腦；新版下一次實體冷開機尚未驗收。仍有 22 筆既有 blocked 任務及其他暫緩工作，沒有聲稱全歷史完成。

決策及完整計時見 [performance_decision.json](../artifacts/data_quality/tej_chain_boot_2026-10-09/performance_decision.json)。
正式 bridge SHA-256：`caf7eeb0f316f22567a720bdc0e4cf407e7f66b1e80274398d956de600b42c86`。

## 真實來源完整鏈路對照

每種方法／案例測量兩次，順序為 AB／BA，保留原請求、來源、Parquet、收據及
獨立稽核。下表是每筆完整取得的平均時間，包含流程初始化、操作、回覆、全部
資料格讀回、驗證與提交；不是單一按鈕延遲或只計來源等待。

| 相同來源範圍 | 原版秒 | 新版秒 | 結果 |
| --- | ---: | ---: | --- |
| 除權事件，22 個特徵 | 10.54 | 9.41 | 少 10.65% |
| 相鄰 11 個特徵批次 | 7.82 | 7.18 | 少 8.27% |
| 來源明確空回，股票期貨契約調整 | 13.52 | 13.87 | 慢 2.60%，保留失利案例 |
| 密集日價格：4 家 × 1,636 個來源日期，28 個特徵 | 51.69 | 46.69 | 少 9.67%；每次 6,544 列 |
| 同價格表完整欄位／公司／日期目錄 | 10.28 | 8.02 | 少 22.05% |

[原始 AB／BA 收據](../artifacts/data_quality/tej_chain_boot_2026-10-09/abba-native-cache-v2/acceptance.json)
保存全部樣本與各階段時間。原生初始化平均 0.529 → 0.238 秒；查詢／回覆平均沒有
加快。大表讀回實作未改，讀回時間的差異不能單獨歸因於程式快取。
沒有把本輪回歸測試與速度實驗重疊執行，但其他既有主機工作仍在運作。
每案例只有兩次／方法，沒有統計顯著性或對所有資料表的穩定加速保證。

## 成本模型與完整操作清點

完整取得時間為排程與 admission、初始化、來源／欄位／公司／日期準備、來源
回覆、每格讀值、序列化、驗證與持久提交的總和。唯有當收據已提交，才能算
本筆完成；讀回進度 100% 不能當作入庫完成。

[正式版最近 32 筆剖析](../artifacts/data_quality/tej_chain_boot_2026-10-09/baseline_latest32.json)
實際取得 808 列，完整時間合計 195.90 秒、中位 5.86 秒；送出前中位 4.90 秒。
欄位選取中位 1.87 秒，編譯／初始化中位 0.38 秒。這是當時的回顧混合樣本，
不是原版／新版的同範圍對照，也不是全市場資料筆數。

| 階段 | 方法與本輪處理 | 不能省的證據 |
| --- | --- | --- |
| 排程／領取 | 沿用索引、資料價值優先、有限同表連抓與 durable queue | 不調低高價值資料優先序，不重設游標或已完成工作 |
| 所有權／Windows admission | 沿用活躍互動 relay、一次 durable permit、不可變 script release | 一個 Wizard 只可有一個條件寫入者；不同帳號／程序不自動等於來源狀態隔離 |
| 原生 helper 初始化 | 比較每次編譯與精確原生程式快取 | 原 script／C#／compiler／CLR／reference／DLL 雜湊；每筆仍是新工作程序 |
| 來源定位／binding | 原生精確名稱查找、正常 Type／SmartID／Data 通知 | 現有 PID／HWND／工作簿、正常來源綁定與 modal 檢查；不用滑頁、座標或盲打 |
| 欄位 | 當前完整選取相同即不重選；完整 schema 選 All；子集合比較逐項與一次批量 Select | 全部目前名稱、順序、數量；不能只比筆數或沿用舊 binding |
| 公司 | 原生精確搜尋；當前完整集合符合時不 Clear；多選清單可一次 Select | 同一公司集合與正常 ALL 來源刷新；公司按鈕仍核對位置關係而不以座標點擊 |
| 日期／頻率 | 原日期模型、正常 Search；完整集合符合時 Select All；子集合使用相同批量 helper | 完整日期標籤、實際頻率與模型確認；查詢日曆不是原生觀測筆數或發布日 |
| Preview／回覆 | 一次正常 owned action，核對新回覆 transition | 不能重複點擊換取較快回覆；來源未明不冒充未送出 |
| 大表讀回 | 保留專用 MTA、最多 4 個唯讀工作者及共享每列實作 | 所有儲存格、獨立物件路徑樣本、全部目前列角色／數量穩定與容量檢查 |
| 原值／驗證／入庫 | 保留原始顯示字串、鍵、單位、Parquet、收據 | 不補假值、不改精度、不把缺值變零；獨立稽核 raw／Parquet／schema／鍵／SHA |
| 進度／網站 | 原 2 秒讀回中繼資料、5 秒唯讀面板 | 不逐格寫磁碟；網頁 GET 不呼叫 TEJ、不傳原值與帳密 |
| 啟動／恢復 | Windows boot 派送 WSL；登入後監督專用桌面；修正未確認 launcher 永久擋住 | 分清啟動程序與資料查詢；保留原 UNKNOWN、精確 session、正常 toolbar 與有界授權 |

兩鍵歷史表的 Preview 最多 28 個特徵，三鍵 27 個、一鍵快照 29 個。
這來自 [Smart Wizard 官方手冊](https://www.tej.com.tw/tejplus/smartwizard.pdf)
的 30 總欄預覽限制，不泛化成 Excel 匯出、REST API 或來源速率上限。
全欄位讀回的工作量至少隨實際列數 × 欄數增加；多開控制器不會消除此工作。
現有 `minimum_uniform_company_date_rectangles_v1` 只證明有界均勻矩形候選中的
最少查詢數，不是無條件的全域打包最佳解。

原版密集表的讀回平均占 33.21／51.69 秒，約 64.3%。在「保留同一讀回方法及
該次觀測速度」的條件下，即使把其餘步驟壓到零也仍需約 33.21 秒；這只是
Amdahl 條件估計，不是來源介面或硬體的不可突破物理下限。因此後續大表工作
需獨立驗證其他完整匯出／讀值介面，不能只加快按鈕後宣稱整條鏈到達極限。

## 一次批量 Select 的正確邊界

[官方手冊](https://www.tej.com.tw/tejplus/smartwizard.pdf)支持 Ctrl／Shift 多項選取。
新版使用正常原生多選模型與一次 Select，不使用滑鼠、全域鍵盤、視窗注入或
多次 action 重試。單選清單、非來源順序的請求仍在任何改動前回到原逐項路徑。

`LB_SETSEL` 本身不會產生選取通知，因此在全部名稱與清單 owner 都確認後，
須把正常 `LBN_SELCHANGE` 通知送到清單的實際 native parent，等事件完成，
再檢查全部原生選取會員與一次 Select 的完整目的清單。
這是 [Microsoft LB_SETSEL](https://learn.microsoft.com/en-us/windows/win32/controls/lb-setsel)
與 [LBN_SELCHANGE](https://learn.microsoft.com/en-us/windows/win32/controls/lbn-selchange)
的語義，不能把改 index 當作應用程式模型已更新。

第一次真實來源測試發現 TEJ 的清單嵌在 panel 下，測試清單卻直接在 group 下。
原候選因 parent 不符，在送出 Preview 前拒絕，沒有來源查詢。
[原失敗](../artifacts/data_quality/tej_chain_boot_2026-10-09/abba-bulk-v1/failed.json)
與 [精確 gate 調解](../artifacts/data_quality/tej_chain_boot_2026-10-09/bulk-v1-gate-reconciliation.json)
均保留；只釋放這輪測試自己的 gate，不更動正式任務或游標。
修正通知目標並加入巢狀案例後，[Windows fixture](../artifacts/data_quality/tej_chain_boot_2026-10-09/list_bulk_nested_v2.json)
通過 122 項檢查，包含 bound／unbound、兩種多選模式、Unicode、順序、錯誤
owner、錯誤名稱、disabled、幾何及只回一項的故障；任何失敗均不補點第二次。

[批量版真實 AB／BA](../artifacts/data_quality/tej_chain_boot_2026-10-09/abba-bulk-v2/acceptance.json)
原版完整取得 199.96 秒，新版 198.87 秒，只減少約 0.54%；兩版各取得 13,124
列、同範圍全部值一致。大表平均 56.32 → 54.15 秒，但事件與空回平均較慢。
因此不把局部選取減少的操作，宣稱成穩定的大幅整體收益。

## 原生程式快取：只快取程式，不快取查詢權限

快取位於目前 Windows 使用者的 `LocalApplicationData/StockAgent/TEJSmartWizard/NativeHelpers`。
fingerprint 包含完整 bridge SHA、C# source SHA、PowerShell／CLR／bitness、
compiler SHA 與參考組件的全部位元組 SHA。載入前後核對 DLL SHA 與實際
assembly location；不沿用 COM、UI 物件、選取、permit、來源結果或 static worker 狀態。

cache pointer 使用原子更新；來源／環境身分不符、路徑跳脫或 DLL 毀損時，
拒絕舊檔並重新編譯一個新 generation，保留舊 DLL 與 manifest。沒有刪除來源
資料或假裝毀損檔已驗證。[跨程序故障測試](../artifacts/data_quality/tej_chain_boot_2026-10-09/native_cache_v2.json)
通過 11 個案例。第一版快取也保留，因逐檔 PowerShell 呼叫成本，hit 約 0.5 秒；
改用相同 SHA-256 的 managed stream 後，fixture hit 約 0.31–0.34 秒。
fixture 的時間不當作 TEJ 全鏈路速度證明。

## 開機分層與未確認 launcher 修復

目前機器有兩個獨立 Windows 任務：

- `StockAgent TEJ WSL Startup`：S4U／BootTrigger，只啟動 WSL 的原下載服務與 API timer。
- `StockAgent TEJ Desktop Recovery`：Interactive／LogonTrigger 與每分鐘監督，核對及恢復專用查詢視窗與 relay。

GUI 必須登入 Windows；不設定自動登入，不在 Session 0 操作桌面。
[Microsoft TASK_LOGON_TYPE](https://learn.microsoft.com/en-us/windows/win32/api/taskschd/ne-taskschd-task_logon_type)
區分 Interactive 與 S4U 的執行條件。這不表示開機前可以用 S4U 代替已有登入的
Smart Wizard 或把桌面授權當作 API 授權。

今天 07:04:44 Windows 開機、07:05:17 boot 任務成功派送、07:05:21 WSL 服務啟動，
但未確認的 `prepared` launcher 將桌面恢復永久擋住。新版只在同一 boot／工作簿、
原 session 的 process 與 window 都確定消失、全部 TEJAddin 都不存在、專用 Excel
可用且無 modal、正常 Database Settings 命令可用時，依既有授權多送一次 launcher。
原未知 intent 先保留；至少間隔 60 秒且仍遵守每滾動小時最多 3 次的既有限制。
這是視窗啟動恢復，不是把未知的資料 Preview 重新分類成未送出。

[啟動政策 fixture](../artifacts/data_quality/tej_chain_boot_2026-10-09/startup_policy.json)
16 項通過；真實準備狀態在 08:47:34（台北時間）自動恢復為 `desktop_ready`。
09:26:13 實際再次觸發原 boot 任務，讀回成功完成並確認 systemd 的原服務及
API timer 都已 enabled。證據見 [boot_acceptance.json](../artifacts/data_quality/tej_chain_boot_2026-10-09/boot_acceptance.json)。

不為測試強制重新開機整台主機；目前的「今天實際 boot dispatch」與「新版同
boot 恢復」不能冒充「新版在下一次完整冷開機已驗收」。

## 正式入庫、進度與連線驗收

[新正式收據驗收](../artifacts/data_quality/tej_chain_boot_2026-10-09/local_deployment_acceptance.json)
從 09:26:13 起的新完成任務中選 8 筆，逐一綁定相同不可變 bridge release、
prepared request 與 attempt SHA，使用原 [canonical audit](../scripts/audit_tej_history.py)
核對 raw／Parquet／收據／schema／鍵／大小，再獨立比較每一個原始儲存欄位的
全部資料格。31 列、870 格一致，沒有新 blocked 或未明 barrier。
這是本輪選定新收據的驗收，不會把既有 22 筆 blocked 當作已修復或已完成。
儲存衍生 numeric／unit 欄位維持既有契約；本輪不改精度或原值處理方式。

09:37:42 驗收時，原服務 `active/running`、`NRestarts=0`，互動 relay 活躍；
本機 `http://127.0.0.1:8770/tej/api/status` 已反映 09:37:38 新收據。
GET 只讀目前收據及 metadata，不呼叫 provider、不傳原值或帳密。

[Windows HTTPS 唯讀探測](../artifacts/data_quality/tej_chain_boot_2026-10-09/public_gateway_windows_probe.json)
在 09:39:16 核對正常 TLS／SNI：同一 hostname 分別指定 Windows loopback 與 LAN
均回 HTTP 200，安全狀態與最新完成時間一致；使用公開 DNS 的 IPv4 連線逾時。
WSL 的公開 IPv4 也逾時、IPv6 無法連線。這支持「本機前端及面板可用、公開位址
連線路徑未驗收」，不能僅因本機髮夾連線失敗就推論所有外網使用者都無法連線。
本輪未更動路由器、DNS、TLS、Windows 防火牆或登入政策；原公開網址驗收失敗
及探測器在 admission 前的兩次語法拒絕證據均保留。

相關回歸 `-m pytest -q -s test -k tej`：1,735 passed、1 skipped、15,532 deselected，
49.31 秒；其中 TEJ 專用模組另跑 1,690 passed。未跑全專案所有訓練／GPU 測試，
沒有把未執行的測試算成通過。

## 復驗指令

```bash
source scripts/runtime_env.sh

# 唯讀：從真正完成的收據量測全流程，不發來源查詢
run_fintech_python scripts/profile_tej_acquisition.py \
  --root data_tej --limit 32 --output /明確的新目錄/profile.json

# 回歸：TEJ 相關排程、原值、來源界線、入庫、dashboard 與啟動
run_fintech_python -m pytest -q -s test -k tej

# 服務狀態／enabled 不代表資料已完整，仍須查新收據與來源 acceptance
systemctl is-enabled stockagent-tej-history.service stockagent-tej-api-trial.timer
systemctl show stockagent-tej-history.service -p ActiveState -p SubState -p MainPID -p NRestarts
```

新機器安裝使用原 [Windows installer](../scripts/install_windows_tej_startup.ps1) 與
[launcher](../scripts/start_windows_tej.ps1)；不是再建另一個下載器。
Windows 開機任務若尚未註冊成功，安裝需足夠的排程器權限並讀回；本機目前任務
已存在，不能沿用 10/05 的「尚未安裝」結論。

真實來源 AB／BA 是額外、明確允許的有界查詢，需專用 `.download.lock`、新證據
目錄與 `--allow-source-requeries`；不能在正式工作旁搶同一 Wizard，也不能遇到
未明結果就無限制重跑。所有原版與較慢／失敗候選都保留。

本輪使用的完整來源比較命令如下。`--output` 必須換成不存在的明確新目錄；
基準使用保留的不可變 release，兩個案例必須仍有本機來源收據。這會額外發出
16 次 Preview 與 5 次目錄規劃，不是唯讀性能查詢，不要把它當作每日排程。

```bash
run_fintech_python scripts/benchmark_tej_full_flow.py \
  --root data_tej \
  --baseline data_tej/bridge_releases/de02a50def7774e4ad5bfa9869b0e380d0cc4fef4975c0db6bc10462054c51b0.ps1 \
  --candidate scripts/tej_smart_wizard_bridge.ps1 \
  --output /明確的全新實測目錄 \
  --case f269f06e5b4f8922d68cf0b2 \
  --case 8bb751bd3fc2043f4602eb85 \
  --field-batch-case f269f06e5b4f8922d68cf0b2 \
  --dense-start 2020-01-01 --dense-end 2026-09-25 \
  --allow-source-requeries
```

## 未更動與剩餘邊界

本輪不宣稱全歷史、所有 table、底層 Excel 精度、歷史發布日或資料交付／訓練
全部完整。桌面官方速率及每日配額仍未有可驗證證明，不能套用試用 API 的限制。
多 Wizard 寫入、Excel 大表／更寬欄位匯出、常駐 bridge、改變稀疏查詢幾何等
仍需各自的授權／來源狀態隔離／原值等價／完整範圍與恢復驗證；未測的方法
不冒充本輪已證明最快。
