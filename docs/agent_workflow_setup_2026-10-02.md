# Agent 工作流程安裝與驗收

## 任務

- 使用者目標：安裝及準備已分析的必要工具，直接使用新的工作流程。
- 範圍：本機工具、唯讀狀態入口、任務紀錄、tmux／systemd 長命令監督及 repo 操作入口。
- 責任：沿用既有架構盤點、原子寫入、GPU jobs、訓練生命週期及服務；不另建資料或訓練權威。
- 起始 Git：`twRule`，HEAD `894e782dfde5e12d1f1c6a9d94393a8bc3383ba6`，46 個 dirty paths。
- 交付入口：`stockagent-agent`；本任務已用此入口執行安裝、測試、狀態與進度管理。

## 驗收條件

- 必要工具可執行，安裝器重跑不覆寫其他安裝或重複升級已有套件。
- 狀態查詢不下載資料、不查 provider API、不啟停既有服務、不遍歷大型資料樹。
- 任務保存原始目標、目前步驟、Git 身分與證據；同名建立不能覆蓋已有任務。
- 長命令保存實際 exit code、完整 log 與程序身分；斷線和 PID 重用不能冒充成功。
- tmux 與 systemd 各完成實際有界命令驗收；完成任務須有非空證據。
- 使用既有架構盤點，保留已知設定錯誤與資料健康的證據邊界。

## 進度

- [x] 盤點 Git、已安裝工具與現行責任入口。
- [x] 安裝缺少的系統 `jq`、`sqlite3`；保留原 runtime 內的 sqlite3。
- [x] 完成及使用 CLI／任務紀錄／tmux。
- [x] 完成語意測試、長命令退出／中斷與安裝冪等驗收。
- [x] 更新 Runtime 與操作文件入口；最終收據路徑見下方。

## 工具與責任清單

| 項目 | 本機準備與驗收 |
| --- | --- |
| 查詢工具 | jq 1.8.1、系統 sqlite3 3.46.1 已補齊；doctor 同時記錄選定 runtime 與系統工具路徑 |
| Terminal | 既有 tmux 3.6；專用 socket、三個互動 shell；有界前景命令實際完成 |
| Git | 狀態、dirty baseline、worktree／checkout 與 host 身分；跨 checkout／host 的收據拒絕使用 |
| 任務紀錄 | 原始 objective、next action、milestone history、證據 SHA；使用程序鎖與共用 durable atomic JSON |
| 長命令 | systemd／tmux；實際退出碼、完整 log、boot/PID/start time；流程程式變更拒絕排隊執行 |
| 中斷恢復 | 對自有控制 unit 實際 SIGKILL；確認原程序／session／supervisor 已結束，以證據允許同名重跑；原退出碼保留未知 |
| 瀏覽器 | 既有 Playwright/Chromium 的 `cpu-2d` 私有控制頁，實際 frame 與普通 click 通過；不涵蓋 GPU/WebGL 或公開 Dashboard 驗收 |
| Python／驗收 | 使用 runtime_env.sh 選定的 Python；79 項相關測試通過，Ruff、格式、shell 語法與文件連結檢查 |
| 資源／正式工作 | 沿用 GPU jobs、provider limiter、OCR owner、服務資源趨勢與原訓練／部署入口 |

## 驗收證據

- [最終工程收據](../artifacts/operations/agent-workflow/acceptance.json)
- [工具安裝與 runtime 路徑](../artifacts/operations/agent-workflow/installation.json)
- [本機狀態與架構問題](../artifacts/operations/agent-workflow/context.json)
- [實際中斷與恢復](../artifacts/operations/agent-workflow/recovery-acceptance.json)
- [瀏覽器控制頁](../artifacts/operations/agent-workflow/browser-control.json)

每個 run 的 JSON 與完整 log 保留在 `artifacts/operations/agent-workflow/runs/`。
初次測試失敗的紀錄也保留；同名最新 run 通過後才完成本任務。

## 現況限制

本次完成本機工程工具與工作流程。6 份既有期貨回歸設定仍引用缺少的共同基底，
3 個既有服務顯示 failed；它們在狀態收據保留，沒有當作工具安裝成功一併消除。
遠端節點、CUDA 訓練 readiness、資料完整性和正式服務部署仍由各自契約與當次證據判定。

設計按需讀取與保留小型任務紀錄，參考
[OpenAI Docs](https://developers.openai.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra)；
本文件與 repo 實作提供完整使用方式，無需 private skill 或隱藏記憶。
