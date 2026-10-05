# TEJ 開機恢復與直接搜尋優化（2026-10-05）

## 結果與邊界

已部署 Windows **登入後**自動恢復、WSL systemd 自動啟動、持續下載與
安全通道交接。沿用原 TEJ 佇列、下載器、來源範圍與收據，不另建下載管線。
正式進度頁：<https://penguin72487.ddnsgeek.com/tej/>。

Windows 登入前的 S4U 開機任務仍未安裝：實際註冊回報
`HRESULT 0x80070005 / Access is denied`。登入後的 Interactive 任務已安裝並驗證，
會啟動 WSL、原下載服務及試用 API timer。因此一般重開機後登入 Windows
不需要手動啟動 TEJ；**尚未登入時，不承諾桌面下載會運行**。
沒有設定 Windows 自動登入，也沒有重開整台主機或中斷其他市場／訓練服務。

此為下載工程驗收，不是全帳號歷史完整、底層數值精度、歷史發布時間或訓練安全驗收。
來源正常空回另外記錄，不能把零筆回覆說成取得歷史資料。

## 原因與設計

總時間拆成：環境／通道建立、選源與範圍準備、來源查詢、讀回與驗證儲存、
等待及故障恢復。這次主要降低準備與不必要等待，不以縮減歷史範圍、略過驗證
或重送未知結果換取較好數字。

本次開機後，Linux 服務已啟動，但原 Excel／TEJ 視窗消失，舊 handle／PID
仍在私有 session。另一個問題是 WSL 開機通道屬於 Windows Session 0，
能啟動 PowerShell，不代表能存取使用者登入的 Session 1 介面。
保存過的 workbook 也會使 Wizard 標題由檔名變成完整路徑，不能只比對 `Book2`。

現在分成三個責任：

| 元件 | 工作／條件 | 防止的問題 |
| --- | --- | --- |
| 原 `stockagent-tej-history.service` | WSL 啟動時自動啟動；單一 scheduler／下載鎖 | 不新增第二套佇列或重複來源工作 |
| `StockAgent TEJ Desktop Recovery` | 使用者登入後執行；每分鐘 recurring；`Interactive`、`IgnoreNew` | Session 0／短命 interop 通道、重複桌面持有者 |
| 私有 startup／transport 收據 | 本次 Linux boot、程序出生時間、Windows session、socket、workbook／視窗身分 | PID 重用、舊通道與無關工作簿誤綁定 |

Interactive 登入型態的條件依據
[Microsoft 排程任務文件](https://learn.microsoft.com/en-us/powershell/module/scheduledtasks/new-scheduledtaskprincipal?view=windowsserver2025-ps)。
啟動 Wizard 使用正常 Excel Add-ins／Database Settings 操作，參照
[TEJ Smart Wizard 官方操作說明](https://www.tej.com.tw/tejplus/smartwizard.pdf)，
沒有擷取憑證、改授權、繞過來源上限或關閉使用者工作簿。

專用檔案是 `%LOCALAPPDATA%\StockAgent\TEJSmartWizard\StockAgent-TEJ-Acquisition.xlsx`。
既有正確查詢保留；只有原視窗與原 TEJ 程序均已消失，才允許以這個專用檔案建立替代介面。
同一次 Windows boot 的未知 launcher 結果不盲目重送；已確認成功的 launcher
可在精確原介面消失後再恢復。原證據另外保留，最多三次／滾動小時是本機保護限制，
不是 TEJ 官方配額。

## 逐操作清點

| 操作 | 現行方法與優化 | 保留的驗證 |
| --- | --- | --- |
| Windows → WSL | 本機固定 SHA launcher；持續持有 Interactive relay | 任務讀回、唯一持有者、session 與程序出生時間 |
| Windows admission | 先確認 session、寫 ready，再核發唯一 durable permit | 發出 permit 後失敗仍屬未知，不能再啟動第二份來源動作 |
| 綁定查詢與 Excel | 精確私有身分；支援檔名／完整路徑標題 | 實際 workbook FullName、window、PID、控制項 owner |
| Type／SmartID／Data | 原生精確搜尋；只讀選中項，不整份列舉選單 | 原生搜尋大小寫差異後再做 ordinal 完全比對，拒絕重複與前綴 |
| 欄位／公司／日期項目 | 原生 list 精確搜尋，不捲動頁面找項目 | 原生與 managed 選取讀回、目標完整 labels／counts |
| 按鈕定位 | 真實 widget＋原生 caption＋角色／幾何／唯一性 | 每次操作重新確認 owner、class、caption、可見與啟用狀態 |
| 按鈕檢查 | 同一 native 呼叫批次完成原有檢查 | 不快取授權判定；錯 parent／PID／caption 拒絕，未送按鈕動作 |
| 核取方塊 | 對應角色 44，避免同名一般按鈕被選中 | 既有狀態與模型讀回，不反覆 toggle |
| binding 事件 | 一次正常事件，等來源處理完成 | 未完成／timeout 不重送 metadata 通知 |
| 欄位選取 | 一次發送；立即檢查，只有未就緒時才短暫等候 | 最終完整欄位與數量驗證；150 秒上限保留 |
| Preview | 一次正常 owned action；即時檢查後才分段等待 | 來源範圍、submission stage、fresh transition／精確 No data 證據 |
| 原結果恢復診斷 | 只讀已選軸及未選列表數量，不再列舉整個公司／日期宇宙 | 不改原來源，不把舊 Preview 當成新查詢結果 |
| 讀回／儲存 | 原 canonical MSAA 讀回、raw／Parquet／收據 | 欄位／主鍵、雜湊、筆數、非空格數、來源 attempt 綁定全部保留 |
| 排程等待 | busy 的 watchdog 不因未取到下載鎖而暫停來源工作 | 每次來源動作仍重新驗證介面；網頁另呈現最近核對狀態 |
| 桌面恢復等待 | 通道就緒後最多約 5 秒重新評估；只有本機讀取 | 不新增 API／GUI 探測；原 heartbeat cadence 保留 |
| 停止／更新通道 | 先標記 draining 拒絕新工作，再等下載鎖釋放 | 原查詢完成後才退出，不拆掉正在使用的 relay |

原生搜尋減少的是跨程序往返與 managed 逐項列舉；沒有宣稱供應商內部搜尋必然是 O(1)。
同一 Wizard 的來源／欄位／公司／日期是共享可變狀態，不以多工共用同一視窗。
實測第二個 Windows worker 在 UI 動作前即被原 mutex 拒絕；獨立試用 API
仍沿用原獨立配額與分工。

## 實測證據

所有證據在 [`tej_startup_search_2026-10-05`](../artifacts/data_quality/tej_startup_search_2026-10-05/)。

| 驗收 | 結果 | 限制 |
| --- | --- | --- |
| 同介面 AB／BA 控制項定位與檢查 | 每輪 24 次×3 控制項：原版中位 2.782 秒、新版 0.485 秒，約 5.7 倍 | 唯讀、未送 Source／UI 動作；不是整筆下載倍數 |
| 同介面選單讀回／精確搜尋 | 原版中位 27.3 ms、新版 7.9 ms，約 3.4 倍 | 本次 62 個選單項目的有限樣本 |
| 近期完整工作時間 | 前 60 筆中位 20.38 秒；後 60 筆 12.53 秒 | 不同來源範圍／空回占比，不是受控同資料吞吐量實驗 |
| 最新 immutable bridge | 11 筆完整 fresh 工作，中位 11.52 秒 | 這一批全為來源正常空回，不能宣稱讀回大表也同樣加速 |
| 來源收據 | 02:33 快照 43 個成功工作：3 個非空、5 分片列／115 非空格；40 個正常空回 | 僅這批收據，不是全歷史完整率 |
| 收據稽核 | 43／43 通過，raw／Parquet SHA、來源／schema／attempt、筆數／非空格數，0 個本機失敗 | 未驗證 provider-native 全歷史或發布日 |
| Windows 控制項 | 69 項通過，Unicode／大小寫／重複項／role／錯 parent／PID／caption | 自有 WinForms fixture，不是所有表逐一下載 |
| 正式網頁 | 桌面 1440、手機 390 px 通過；0 JS 錯誤、0 外部 provider 請求 | 面板是唯讀摘要，不可觸發 provider 呼叫 |
| 真實通道交接 | 在來源工作進行中 SIGTERM watchdog；原 attempt 完成、未重送；排程自行建立新 relay | 未重開整台 Windows／WSL |

最初嘗試的 MSAA 名稱定位比原版慢約一成；該結果也保留。
最後改成原生 caption＋批次檢查，再用同介面 AB／BA 驗證。

死掉、過期的 worker 不等於來源未送出：保留 prepared request 與 UNKNOWN。
先收回已保存的原結果，再以既有使用者授權、精確失去的桌面證據及有界重排規則恢復。
此次原開機中斷工作已恢復；原 UNKNOWN attempt 並未改寫成「未送出」。

## 操作與剩餘權限

WSL 內正常更新服務：

```bash
systemctl restart --no-block stockagent-tej-history.service
systemctl status stockagent-tej-history.service --no-pager
systemctl is-enabled stockagent-tej-history.service stockagent-tej-api-trial.timer
```

`restart` 會等當前 bounded 工作結束；不要以全域 kill、關閉所有 Excel 或
清除舊 attempt 來強行提速。完整來源／PIT 問題依既有 TEJ runbook 個別處理。

如要補裝 **登入前 WSL 任務**，需在目前同一個 Windows 使用者的
「系統管理員 PowerShell」執行以下指令。這不會自動登入 Windows 或在登入前操作 GUI。
需要更高權限的安裝尚未代為執行。

```powershell
$tejRepo = '\\wsl.localhost\Ubuntu-26.04\root\stockAgent'
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File "$tejRepo\scripts\install_windows_tej_startup.ps1" `
  -Launcher "$tejRepo\scripts\start_windows_tej.ps1" `
  -RepoRoot '/root/stockAgent' `
  -Output "$env:LOCALAPPDATA\StockAgent\TEJStartup\admin-install-receipt.json"
Get-ScheduledTask -TaskName 'StockAgent TEJ Desktop Recovery','StockAgent TEJ WSL Startup'
```

舊 `windows_task_install_v2.json` 的登入前成功欄位已被實際任務讀回證偽；
以 `windows_task_install_v3.json`、`windows_boot_registration_diagnostic.json` 及
`windows_tasks_syntax_final.json` 的實測結果為準，不能把註冊呼叫無例外當作安裝完成。

後續新增來源、全歷史／發布日校驗、跨主機資料交付及訓練注入不屬於本次完成宣稱。
