# 遠端編輯效能

`settings.json` 將專案根目錄的 `data_*`、`artifacts`、`artifacts_*`
以及 Python 快取、虛擬環境排除於 VS Code 遞迴監看與預設全文搜尋。
根目錄限定的規則會保留 `configs/data_sync`、程式碼、測試與文件的監看。
VS Code 原有的 Git 等預設排除規則仍由設定合併保留。

這些設定不刪除檔案，也不修改下載、訓練、Git 或 Syncthing 的行為。
資料與訓練輸出仍可從檔案總管手動開啟；排除目錄的背景更新可能需要
按檔案總管的重新整理。需要搜尋輸出或資料時，在搜尋面板關閉
「Use Exclude Settings and Ignore Files」，並指定狹窄的搜尋路徑。
移除對應設定項目即可恢復原來的監看與搜尋範圍。

## 2026-09-08 遠端主機觀察

- 檢查當下 CPU、記憶體與磁碟空間充裕；Git status 約 14 ms，
  尊重忽略規則的 `rg --files` 約 9 ms。這些是當下量測，不能排除間歇性壓力。
- `artifacts` 有 140,040 個檔案；兩個 VS Code fileWatcher 程序各有
  9,914 個 inotify 監看項目。這是調整前數值。
- 設定寫入後，目前視窗的監看項目自動降到 96，減少約 99%。另一個
  視窗仍為 9,914，需在該視窗重新載入設定後確認。已核對程式碼、
  `scripts`、`test`、`configs/data_sync`、文件與 `.vscode` 仍被監看。
  監看項目減少不代表使用者操作延遲同比改善。
- 兩次 extension host 從啟動到 `Eager extensions activated` 分別約
  94 秒與 90 秒。Java modernization 與 .NET upgrade 擴充套件自動啟動，
  日誌另有擴充套件相容性、工具註冊與重連錯誤；尚未逐一歸因。

若仍卡頓，先執行 `Developer: Show Running Extensions`，在重現等待時
錄製 CPU profile。若本專案不使用 Java／.NET 遷移，可在擴充套件面板
對 `vscjava.migrate-java-to-azure` 與 `ms-dotnettools.upgrade-agent`
選擇 `Disable (Workspace)`，於工作儲存後執行 `Developer: Reload Window`
並比較啟動時間。不要直接編輯擴充套件程式或執行中的狀態資料庫。

斷線問題應另外檢查本機的 `Remote - SSH` Output；遠端日誌只能證明
連線曾中斷，不能單獨判定本機介面、網路或 SSH 的根因。

參考：[VS Code 效能診斷](https://github.com/microsoft/vscode/wiki/Performance-Issues)、
[檔案監看機制](https://github.com/microsoft/vscode/wiki/File-Watcher-Internals)、
[搜尋路徑規則](https://code.visualstudio.com/docs/editor/glob-patterns)。

## 2026-09-09：連線後卡頓的修正

當日 Windows 的 Remote - SSH 日誌顯示，09:38 的 SSH 建立約 2 秒；
遠端 CPU／memory／I/O pressure 接近零、Git status 約 10 ms，
兩個 fileWatcher 已分別只有 93／96 個監看項目。

透過執行中 extension host 的 VS Code API 查到同一視窗有 521 個分頁，
其中 501 個是 `artifacts` 輸出。15 秒 CPU profile 顯示垃圾回收約
5.702 秒、閒置約 7.337 秒，另有分頁模型更新與 RPC 反序列化。

先保存完整分頁清單，再透過 `window.tabGroups.close` 關閉 496 個
位於 `artifacts/`、未修改、未釘選、非各群組目前顯示的分頁。
五個目前顯示的圖表、程式碼、對話與終端機均保留，分頁從 521 降至 25。
同一 PID 的後續 15 秒 profile，垃圾回收降為 0.0062 秒，閒置約 96.8%。
這是相鄰取樣的結果，並非每種互動延遲都改善相同比例。

本工作區已設定 `workbench.editor.limit.enabled: true`、`value: 80`、
`perEditorGroup: false`、`excludeDirty: true`，限制整個視窗的普通分頁數；
未儲存與釘選分頁受 VS Code 原生規則保護。重要長期圖表可使用 Pin。
此設定作用於分頁，不會刪除資料或改變訓練；既有 watcher/search 排除保持原樣。
設定經執行中 VS Code API 確認已載入，無須重新載入視窗。

遠端備份與 CPU profiles 位於
`/root/.local/state/vscode-performance/20260909/`，包含
`workspace-settings.before.json`、`tabs-324854-before-close-1788918216787124230.json`
與 `extensionhost-324854-{before,after-close}.cpuprofile`。
要撤回分頁上限，刪除本次新增的四個 `workbench.editor.limit.*` 設定即可，
避免覆寫日後其他設定。收起的分頁可由原檔案或備份中的 URI 重新開啟；
不建議一次恢復全部 496 個分頁。

另觀察到 `.NET` 安裝鎖重試及 Java 遷移套件啟動錯誤；本次未修改或移除
擴充套件。CPU profile 未將持續卡頓歸因於它們，應將啟動延遲另案評估。
