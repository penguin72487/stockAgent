# vastai1T VS Code 連線後卡頓修正

2026-09-09 約 09:38–09:47（Asia/Taipei）實測：主要可重現問題是大量
訓練輸出分頁造成 extension host 分頁模型更新、RPC 反序列化與垃圾回收負擔。
關閉符合條件的背景輸出分頁後，同一程序的取樣負擔明顯下降。

## 證據

| 檢查 | 結果 |
| --- | --- |
| Windows Remote - SSH，09:38 連線 | 約 2 秒完成解析／建立通道 |
| 遠端 CPU、memory、I/O pressure | 檢查期間接近 0 |
| 磁碟 | 使用約 56%，尚餘約 449 GiB |
| Git status，三次 | 10.6／10.0／9.8 ms |
| 已生效的 fileWatcher 排除 | 兩個程序各 93／96 個 inotify 項目 |
| 執行中視窗 | 5 組、521 個分頁，其中 501 個在 `artifacts/` |
| extension host PID 324854 | JS heap used 約 2.53 GB；沒有 cgroup OOM 紀錄 |
| 修正前 15.273 秒 profile | GC 5.702 秒，idle 7.337 秒 |
| 修正後 15.409 秒 profile | GC 0.0062 秒，idle 14.9201 秒（約 96.8%） |
| 修正後命令清單 RPC，五次 | 28.1／29.4／37.6／24.7／23.4 ms |
| 最後 10 秒 CPU 量測，09:46:41 | PID 324854 使用單核心約 0.30% |
| Windows renderer 修正後觀察 | 09:43:36–09:46:30 沒有新增 unresponsive 紀錄 |

Windows renderer 日誌在同一視窗多次記錄 `Extension host (Remote) is unresponsive`，
包括 09:38:54–09:39:04、09:39:08–09:39:29。檔案監看排除在修正前已生效，
不足以處理已經開啟的數百個分頁。profile 取樣是相鄰觀察，並非全部互動的
受控基準；未將 profile 的改善比例外推為實際打字或繪圖速度。

## 已套用

1. 保存完整分頁 metadata 與原設定後，使用 VS Code `window.tabGroups.close` API
   收起 496 個 `artifacts/` 分頁。只處理未修改、未釘選、非各群組目前顯示的
   文字／自訂編輯器分頁；保留目前顯示的 5 個圖表、程式碼、對話與終端機。
2. 分頁當下從 521 降到 25；後續使用者操作後驗收為 27，終端機為 6 個。
3. 在 **vastai1T 的** `/root/stockAgent/.vscode/settings.json` 新增：

   ```json
   {
     "workbench.editor.limit.enabled": true,
     "workbench.editor.limit.value": 80,
     "workbench.editor.limit.perEditorGroup": false,
     "workbench.editor.limit.excludeDirty": true
   }
   ```

   這是整個視窗的分頁限制。未儲存與釘選分頁依 VS Code 原生規則保護。
   已由執行中 extension host 讀取有效設定驗證。既有 watcher/search 排除不變。
4. 更新遠端 `.vscode/README.md`，記錄本次證據、設定與回復方式。

沒有刪除訓練輸出、改動訓練設定或重新啟動 VS Code／終端機。使用 loopback
Node inspector 進行診斷後已關閉診斷埠；不修改擴充套件原始碼或執行中的狀態資料庫。
另一個舊 extension host 在處理前已退出，沒有對其送出關閉分頁指令。

## 備份與撤回

遠端 `/root/.local/state/vscode-performance/20260909/` 保存：

- `workspace-settings.before.json`：本次修改前的完整工作區設定。
- `tabs-324854-before-close-1788918216787124230.json`：分頁清單與群組、URI、狀態。
- `close-324854-1788918216787124230.json`：收起 496 個分頁的執行收據。
- `extensionhost-324854-before.cpuprofile` 與 `extensionhost-324854-after-close.cpuprofile`。
- `verify-324854-1788918312359712406.json`：有效設定與命令往返量測。
- `final-runtime.json`：10 秒 CPU 量測與診斷埠已關閉的檢查。

penguin 另保存副本於 `/root/.local/state/vscode-performance/vastai1t-20260909/`。

要撤回分頁上限，移除此次新增的四個 `workbench.editor.limit.*` 設定；
不要用整份舊備份覆蓋之後的新設定。輸出檔仍在原處，可按需要重新開啟，
或按備份 URI 恢復個別分頁。重要圖表可釘選，避免受普通分頁上限影響。

## 其他觀察與範圍

`.NET upgrade-agent` 在此無已追蹤 Java／C# 專案的 Python workspace 自動啟動，
其安裝工具舊日誌曾對 `/tmp/vscd-installedLk.sock` 重試約 4 萬次，並發生
runtime acquisition failure；Java modernization 也有自動啟動／工具註冊錯誤。
這些可造成另外的啟動負擔，但本次 profile 並未證明它們是持續卡頓的主要原因，
因此沒有移除或停用任何擴充套件。

診斷方式依據 [VS Code 官方效能診斷](https://github.com/microsoft/vscode/wiki/Performance-Issues)，
分頁處理由 [VS Code TabGroups API](https://code.visualstudio.com/api/references/vscode-api#TabGroups)
執行。設定的可用性另與實際安裝版本的設定 schema 核對。
