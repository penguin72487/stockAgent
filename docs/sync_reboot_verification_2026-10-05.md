# 2026-10-05 同步系統重開恢復驗收

01:50（台灣時間）確認 penguin 本次 Windows／WSL 重開後，正式同步、
掛載及六個排程已恢復；lab203、Vast 都連線，當下已發布資料的 completion
均為 100%，need bytes／items／deletes 為零。四個現行 StockAgent folder
皆 idle，errors／pullErrors 為零，watchError 與 system errors 為空。
這是已發布資料的傳輸收斂，不是全歷史 NAS 備份完成證明。

Windows 本次啟動為 01:38:38，WSL 為 01:39:12，boot ID：
`3236b658-aeec-4500-8b92-448892ca8683`。開機 journal 記錄 D cold mount
於 01:39:16 完成、backup transport mount 於 01:39:17 完成，Syncthing
隨後於 01:39:17 自動啟動。中間有正常停止／再啟動紀錄；目前這個 Syncthing
instance 從 01:41:07 持續運行，沒有失敗自動重啟計數。掛載仍指向既有 D
權威路徑，未使用 C fallback。`syncthing@root.service` enabled，並 Requires
兩個掛載服務，Restart 為 on-failure。

六個正式 timer 皆 enabled／active：remote-cold-artifact-ingress、backup-stream、
d-cold-scan-retry、packed-transport、legacy-return@main、legacy-return@partitions。
本次開機 journal 可見它們已觸發。oneshot service 的 activating/start 表示工作
仍執行，執行結束回 inactive/dead 則須與 Result／退出碼一起判斷。
lab203 的 NAS readiness、pipeline、recovery queue 收據在本次開機之後仍持續
回傳；runtime lock、單一 owner 與 NAS mount guard 由接收端回報已驗證。
Vast 現場 supervisor 的 Syncthing RUNNING，autostart=true、
autorestart=unexpected；此次沒有重開 Vast 或 lab203，也未重跑 Windows
登入前／lab203 登出驗收。

Windows `StockAgent Public Caddy` 喚醒 task 現場為 Running、S4U，保留
AtStartup（15 秒）、AtLogOn、每分鐘 liveness 三個 enabled trigger，
StartWhenAvailable 與五次失敗重試設定。只讀核對設定，沒有改寫 Windows task。
Running task 的 LastTaskResult 是觀察值，不當成已完成程序的退出 0 證明。

## 本次修正

兩個 legacy timer 曾因 `progress_items_invalid` 略過 ExecStart。根因是原
canonical inventory 對受保護的 loose file（日誌、鎖檔、收據）沒有目錄
recursive `files` 欄位，condition 卻要求每筆都具備整數 count；主 cohort
有 12 筆、partition cohort 有 209 筆這種合法保護條目。

啟動檢查現在只允許 `non-directory-protected` 且缺少 `files` 的既有型態；
未知狀態缺少 count、bool／負數／null count 與不合法結構仍拒絕。受保護
條目不會因此變成回傳或刪除候選，owner lock、原清冊、來源穩定與冷恢復
門檻不變。只修改共用 condition，沒有改寫任何來源進度收據。

`test_legacy_return_resume.py` 與 `test_remote_legacy_return.py` 共
**37 passed／4.55 秒**，覆蓋混合 loose-file／可重試目錄、全部受保護、
無效 count、原 owner 與進度重導向。兩個現場 condition 均恢復 ready=true；
01:47:22 已由原 service 接手同一 cohort，後續仍按原五分鐘 timer 重試。
worker 啟動不代表歷史回收或本次 NAS snapshot 已驗收完成。

固定證據位於 `artifacts/operations/sync-reboot-verification-20261005/`：

- `boot-acceptance-20261004T175007Z.json`：當前 boot、unit、掛載依賴、
  folder／peer convergence、接收端新收據與本次測試結果。
- `windows-startup-task.json`：Windows boot 與 task／trigger 現場觀察。

USB 金鑰保管沿用使用者已確認完成；lab203 Windows 登出驗收仍為已取消。
全歷史 NAS、一次性 bulk 冷庫整理／回收與實際磁碟格式遷移維持各自原任務，
不以本次開機同步驗收宣稱完成。
