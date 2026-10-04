# Agent 工作流程

這是本機工程工作的薄層入口。架構、資料、訓練與服務的責任仍由原本元件持有；
任務收據保存目標與操作證據，不能替代 source receipt、訓練 manifest 或部署驗收。

## 安裝與目前狀態

```bash
bash scripts/install_agent_workflow.sh
stockagent-agent doctor
stockagent-agent status
```

安裝器只補缺少的 `tmux`、`ripgrep`、`jq`、`sqlite3` 系統工具；保留已有套件。
它建立 `/usr/local/bin/stockagent-agent` 指向本 checkout 的啟動腳本。
重跑不覆寫其他來源的同名啟動器。無 root／apt 的節點先用自己的套件管理器準備
工具，再執行 `--skip-packages --bin-dir PATH`。Python 一律由 `runtime_env.sh` 選定。

`status` 只讀本機 Git、工具路徑、systemd 程序狀態、既有公開快照的檔案 metadata、
本流程任務與 run 收據。它不查 provider API、不掃大型資料樹、不下載、不啟停服務。
需要全專案配置診斷時才執行 `status --architecture`，沿用
[`audit_project_architecture.py`](../scripts/audit_project_architecture.py)。
其中 `config_errors` 必須保留；狀態查詢成功不等於所有設定通過。

```bash
stockagent-agent status | jq '{
  git: {head:.git.head, branch:.git.branch, dirty_count:(.git.dirty_paths|length)},
  services,
  tasks: [.tasks[] | {task_id,state,next_action}],
  runs: [(.runs | sort_by(.queued_at_utc) | .[-10:])[] | {run_id,name,state,exit_code,log}]
}'
stockagent-agent status --architecture --output artifacts/operations/agent-workflow/context.json
```

摘要只列任務身分／狀態及最近十筆 run，避免把每個任務的完整 Git 基線與歷史
反覆載入對話。需要原目標或較早收據時，用 `task show`／`run-status` 定位原紀錄；
摘要省略不表示已刪除紀錄或完成所有工作。

Git remote refs、遠端節點、載入的服務程式版本、來源 freshness／完整性仍需對應驗收。
快照檔案的 mtime 只表示檔案年齡；`active` 只表示程序活動。

## 接手、執行與交付

實作／維運的多步任務先查相關現況並建立紀錄。小型文字修改可直接完成，
不強制全架構掃描或建立長任務。

```bash
stockagent-agent task start repair-example \
  --objective '修正本次指定問題並通過相關驗收' \
  --next '定位共用責任入口及根因'
stockagent-agent task show repair-example
stockagent-agent task update repair-example --next '執行相關測試' --note '根因與修正範圍已定位'
```

需要固定設定時加 `--config configs/markets/EXACT.yaml`；收據保存檔案 SHA，
正式實驗仍以解析後設定、資料身分與 canonical manifest 為準。
任務的 objective 不會由 update 覆寫；下一步、理由和證據依序保存。
同 ID 再次 start 會拒絕，避免覆蓋未完成工作。`task list` 用於找回既有任務。

長命令選擇其中一個既有監督工具：

```bash
stockagent-agent run --name regression repair-example -- \
  run_fintech_python -m pytest -q -s test/test_runtime_env.py

stockagent-agent session repair-example
stockagent-agent run --backend tmux --name diagnostic repair-example -- \
  run_fintech_python -c 'print("bounded diagnostic")'

stockagent-agent run-status EXACT_RUN_ID
stockagent-agent logs EXACT_RUN_ID --tail 80
```

`run` 接受 argv，不經 shell 解讀；確實需要 shell 的命令請明確傳 `bash` 與既有腳本。
`run_fintech_python` 的首個 argv 會解析成當次已選定的 Python。
stdout／stderr 留在完整 log；實際 exit code、開始／結束時間、boot/PID/start time
留在原子寫入的收據。PID 重用或原 runner 消失會顯示 `interrupted`，不能推定成功。
同名 run 尚在執行或結果未知時拒絕再啟動；需要先核對程序與既有工作狀態。

如果 runner 中斷或未留下開始收據，先依原工作契約核對進度／checkpoint／來源。
保存核對結果後執行下列命令，再用同一 task 和 run 名稱啟動相容的恢復指令：

```bash
stockagent-agent run-reconcile EXACT_RUN_ID \
  --note '已核對原工作狀態，依收據從指定進度恢復' \
  --evidence artifacts/EXACT_RECOVERY_RECEIPT.json
```

核對入口會確認原 runner、child/session 和 supervisor 已結束，才允許同名重跑；
重跑前再次查程序狀態。原 run 保持 `interrupted`、不捏造 exit code 或成功狀態。
無法取得 tmux/systemd 證據時仍拒絕。排隊後若工作流程程式改變，命令不執行，
回傳 exit 78；重新確認版本後再啟動。

此入口適用會等待完成的前景命令。`manage_gpu_jobs.py start` 這類自行 detach 的
入口直接照原 GPU job 工作流程使用；不要把它包進短命 systemd unit，避免 manager
結束時 systemd 回收其背景子程序。GPU 指派、DDP、資料準備、resume、輸出目錄
仍由既有配置及訓練契約管理。

systemd run 使用本次開機的 transient unit、`Restart=no`，不自動重送有副作用的工作。
root 使用 system manager，其餘使用 user manager；user manager 的登入後存活與
linger 要依節點另驗。重開機後由 task/run 收據接手，依原來源／checkpoint 契約恢復。
常駐服務仍使用 repo 既有正式 unit／timer。

tmux 使用獨立 `stockagent-agent` socket 與帶 checkout 指紋的 session，準備
`work`、`logs`、`status` 三個 shell，不改使用者全域 tmux 設定。`session` 回傳 attach
指令；它會補回準備未完成的視窗。tmux 保留斷線後的程序與終端，主機／WSL
關閉後需另行恢復。Python shell 已 source 當前 checkout 的 runtime。

通過驗收後以當次非空結果文件完成任務：

```bash
stockagent-agent task update repair-example --state complete \
  --next '本次範圍已交付' --evidence artifacts/EXACT_ACCEPTANCE.json
```

每個 run 名稱的最新結果須成功，或有下列已驗證的失敗處置；舊失敗收據保留。
完成時必須重新讀取非空證據，
記錄 SHA 與檔案 metadata，拒絕讀取途中變更的文件。超過 16 MiB 的大型 artifact
使用其小型 manifest／receipt 指向。這是工程任務的完成宣告；命令 exit 0 不替代
資料完整性、策略結果、訓練 artifact gate 或實際部署的驗收。

修正後的驗收用了不同 run 名稱，或候選正確被拒絕時，可追加明確的工程處置：

```bash
stockagent-agent task resolve-run repair-example ORIGINAL_FAILED_RUN \
  --outcome superseded --verified-by LATER_SUCCESSFUL_RUN \
  --note '修正原失敗原因，後續驗收已通過；原結果保留' \
  --evidence artifacts/EXACT_REPLACEMENT_ACCEPTANCE.json
```

`expected-rejection` 必須提供同 task 的後續成功診斷 run，以及綁定判準的 JSON：

```json
{
  "state": "accepted_expected_rejection",
  "task_id": "repair-example",
  "run_id": "ORIGINAL_FAILED_RUN",
  "verified_by": "LATER_SUCCESSFUL_DIAGNOSTIC_RUN",
  "criterion": "主鍵不一致時必須拒絕相容宣告；原 artifact 不升版"
}
```

處置僅追加至 active task，不能覆寫既有處置；原 run 的 failed、exit code 和 log
不變。處置綁定原 run／log、同 task 後續成功 run 及當次 evidence 的 SHA，結案
時重新校驗；任一變更或新的未處置失敗仍拒絕完成。queued、running、interrupted、
cancelled 或結果未知的 run 不能用此入口略過。這只解決工程任務的候選／替代關係，
不會清除來源缺口、SLO 失敗、研究不相容或正式部署的驗收要求。

## 修改隔離與資源

獨立實驗使用 Git worktree，記下 Git/config/source release 與獨立 output root。
新 worktree 不自動包含目前 dirty work，只帶入該任務需要的精確差異；資料沿用
既有 catalog／release 按需取用，避免複製整個資料樹。
啟動器辨認目前 StockAgent checkout；也可用 `STOCKAGENT_REPO_ROOT` 明確指定。
舊 checkout 若沒有本工作流程檔案會明確失敗，不能悄悄操作另一份 checkout。

獨立讀取可以並行；修改同一檔案、SQLite writer、GPU owner 與輸出目錄仍需協調。
沿用 `manage_gpu_jobs.py`、既有 provider limiter／鎖及 OCR owner，按量測設定並行數。
資源證據沿用 [`track_service_runtime_trends.py`](../scripts/track_service_runtime_trends.py)，
Dashboard 行為沿用 Chromium/CDP 稽核，Windows TEJ 沿用既有橋接與輸出核對。

需要查看長 Python 工作的實際 stack 時，本機另已安裝獨立 uv tool `py-spy 0.4.2`，
不加入 fintech／CUDA runtime。先從 run receipt 與當前程序核對真正的 Python worker
PID（shell child 可能仍只是 bash），再執行 `py-spy dump --pid PID --json` 保存 stack。
這個命令短暫暫停目標；本次完整回歸 dump 耗時 0.092 秒，確認正在執行 compiled
overnight 梯度測試。`--nonblocking` 的一次實測遇到讀取 thread/code object 的
`Bad address`，不把這種取樣失敗當作工作崩潰。重新安裝可用
`uv tool install py-spy==0.4.2`，並確認 uv tool bin 已在當前 PATH。
實際證據見 [架構續作報告](architecture_readiness_repair_2026-10-03.md)。

操作收據位於 `artifacts/operations/agent-workflow/`，由 repo 的既有 artifacts ignore
規則排除。收據目錄／檔案使用私人權限；不要把含私密 argv 的任務收據公開。
本機工具與流程的驗收見 [`agent_workflow_setup_2026-10-02.md`](agent_workflow_setup_2026-10-02.md)。
