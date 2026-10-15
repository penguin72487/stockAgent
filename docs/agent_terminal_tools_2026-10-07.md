# Agent 終端工具實測與採用方式（2026-10-07）

本次採用既有 Bash runtime、`stockagent-agent` 的 tmux／systemd owner，以及獨立
SSH 連線重用設定。五個相同的短遠端命令，總時間由 1.387 秒降到 0.462 秒，
後者包含首次建立連線。Herdr 已安裝並完成真實 managed pane 試用；zsh 與 mosh
已安裝，但目前沒有證據支持把它們改成預設工程入口。

## 第一性原理與取捨

工程工作的完成時間包含運算、命令派送與連線、找回上下文，以及中斷後重做。
Shell／終端工具能改善後三項；資料整理與訓練速度仍取決於原本的資料布局、
CPU／GPU 工作與 lifecycle。工具選擇因此以完整派送、等待、讀取及驗證時間為準。

| 需求 | 本次選擇 | 實測判斷 |
| --- | --- | --- |
| 重複遠端診斷的連線成本 | OpenSSH ControlMaster／ControlPersist | 同一已登錄 Vast 節點，五個命令含首次連線約快 3 倍 |
| 臨時工作持續執行、保存終端 | 既有 `stockagent-agent` tmux | 派送 CLI 結束後工作完成，完整輸出可取回 |
| 多個終端與 workspace 操作介面 | Herdr 0.9.3，按需使用 | 真實 managed pane 通過派送、等待、輸出核對；未顯示運算加速 |
| 互動 shell 操作 | zsh 5.9，按需使用 | 透過 Bash 執行既有 runtime 比直接 Bash 稍慢；直接 source 尚不相容 |
| 行動網路互動終端 | mosh 1.4.0，保留本機工具 | loopback 加密交換與暫停／恢復通過；現有 Vast 沒有分配 UDP 連接埠 |

正式服務沿用既有 systemd／timer，正式訓練沿用 GPU job manager 與 checkpoint。
工具試用沒有啟動額外 AI agents，也沒有改動正式資料、訓練或交易程序。

## 量測方法與結果

Penguin 為 WSL2，量測時 CPU affinity 可用 16 個邏輯 CPU。以下是這個節點與
當時負載的觀察；本次沒有 GPU benchmark。

終端比較使用同一工作：選定 canonical Python runtime，完整讀取
`scripts/runtime_env.sh` 與 `docs/agents/runtime.md`，計算 SHA-256，等待結束並
核對輸出。四種模式交錯執行各五次，奇偶輪反轉順序，共 20 次；輸入在比較
前後保持相同。數據包含派送、等待及讀取成本。

| 模式 | 完整工作中位數 |
| --- | ---: |
| Bash | 179.68 ms |
| zsh 轉交 Bash runtime | 190.75 ms |
| tmux | 189.16 ms |
| Herdr managed pane | 211.41 ms |

這些短命令不能代表長期或統計顯著的速度差。Herdr 的管理成本在這個案例中
高於 Bash；選用理由是終端管理便利性。

tmux／Herdr 的背景驗證各執行六次延遲計數、寫入獨立結果及唯一 token。
派送 CLI 分別在 9.73／4.47 ms 返回，工作分別在 1.375／1.418 秒完成；結果及
全部計數吻合。另以真正的 `stockagent-agent run --backend tmux` 執行 canonical
runtime，run `adopted-workflow-probe-20261007T043637-8741edc0` 成功，退出碼 0。
本次未實測互動連線掉線或主機重開機。

SSH 使用相同已登錄端點及相同 host verification，交錯執行每種模式五次，
每次核對完整輸出。最終採用設定的結果如下：

| SSH 模式 | 五次總時間 | 單次中位數 |
| --- | ---: | ---: |
| 每次建立新連線 | 1.3869 s | 276.55 ms |
| 重用連線，含首次建立 | 0.4625 s | 38.59 ms |

改善範圍是重複短遠端命令的連線成本，沒有宣稱資料傳輸或訓練吞吐提升。
`ControlPersist 60` 在最後一次使用後保留 60 秒。socket namespace 綁定端點、
登入帳號、identity 與 known-hosts 檔案路徑，變更設定後使用新的 namespace。

mosh 只綁定 `127.0.0.1` 的臨時 UDP 連接埠，進行真正的加密 client/server
交換；首個輸出 0.133 秒，client 暫停 0.35 秒後恢復，同一 server 回覆唯一
token，完整交換 0.512 秒。沒有測試實際網路漫遊。既有 Vast 的配置與環境只
列出 TCP，未分配 UDP；因此遠端 mosh 尚未啟用。

## 已安裝版本

- zsh：5.9（APT `5.9-8ubuntu3`）；未改預設 shell。
- mosh：1.4.0（APT `1.4.0-1ubuntu5`）。
- tmux：既有 CLI 回報 3.6，APT 套件 `3.6a-2ubuntu0.1`。
- Herdr：0.9.3，官方 Linux x86_64 release binary，安裝於
  `/opt/stockagent/agent-tools/herdr-v0.9.3/bin/herdr`，
  `/usr/local/bin/herdr` 指向此版本。未更改 Codex 的全域整合或 skill 設定。

Herdr binary SHA-256 已和官方 release asset digest 核對：

```text
18a8dc65f1c2fa485884344356dea1cfd911c6f06cf46fa78e193f4087f4dba7
```

初次 Herdr 試用遇到 Unix socket 路徑長度限制，以及 headless server 沒有初始
workspace；採用短 private runtime 目錄及真正 TUI／PTY 後通過。測試 worker
繼承真正的 `HERDR_ENV=1` 和 pane ID，建立及關閉自己的 tab，沒有假造環境或
操作使用者的工作區。已結束所有測試 Herdr／mosh 程序。

## 日常使用

在 repo 的 Bash shell 中先使用 canonical runtime。遠端端點由既有私有 rclone
policy 產生；端點變更或重開機後，重新執行 prepare，建立 `/run` 下的 socket
目錄。設定檔為私有權限 600，socket 目錄為 700，不改全域 SSH 設定。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/prepare_agent_ssh_profile.py
ssh -F /etc/stockagent/agent-ssh.conf stockagent-vast 'uname -s'
```

既有長任務使用原工作紀錄與 owner。下例須先建立或找到該 task：

```bash
stockagent-agent session TASK_ID
stockagent-agent run --backend tmux --name diagnostic TASK_ID -- \
  run_fintech_python scripts/check_environment.py
stockagent-agent run-status EXACT_RUN_ID
```

Herdr 可在互動終端啟動：

```bash
herdr --session stockagent-dev
```

Agent 在 Herdr 真正管理的 pane 內，依其 CLI 回傳的 workspace／tab／pane ID
操作自己的終端，並核對 exit code 和完整輸出。當前 Codex 對話的工具後端仍為
原 exec；安裝 Herdr 不會自動移動正在進行的對話或帶入原生 agent resume。

zsh 直接 source 目前 runtime 的實測退出碼為 2；互動時若選擇 zsh，透過 Bash
執行 repo 命令即可：

```bash
bash -c 'source scripts/runtime_env.sh; run_fintech_python scripts/check_environment.py'
```

## 重測與驗收證據

下列 probe 每次使用新的輸出檔，不覆寫既有收據。終端 probe 只建立自己的
Herdr session／tmux socket；mosh probe 只建立 loopback listener。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_agent_terminal_tools.py \
  --herdr /opt/stockagent/agent-tools/herdr-v0.9.3/bin/herdr \
  --output artifacts/operations/agent-tools-repeat/terminal.json
run_fintech_python scripts/probe_mosh_loopback.py \
  --output artifacts/operations/agent-tools-repeat/mosh.json
run_fintech_python -m pytest -q -s \
  test/test_agent_ssh_profile.py test/test_agent_workflow.py test/test_runtime_env.py
```

相關 55 項測試通過：4 項 SSH profile 保護／信任／socket 邊界測試，及 51 項
既有 agent workflow／runtime 回歸。早期測試 fixture 的過長 socket 目錄被
guard 正確拒絕，改用短 fixture 後通過；早期失敗收據仍保留。

證據位於 `artifacts/operations/agent-tool-efficiency-20261007/`，主要收據：

- [完整終端比較](../artifacts/operations/agent-tool-efficiency-20261007/terminal-comparison.json)。
- [最終採用 SSH profile 實測](../artifacts/operations/agent-tool-efficiency-20261007/final-adopted-ssh-profile.json)。
- [mosh loopback 驗證](../artifacts/operations/agent-tool-efficiency-20261007/mosh-loopback-pause-resume.json)。
- [SSH profile 測試](../artifacts/operations/agent-tool-efficiency-20261007/ssh-profile-safety-accepted.xml)
  與 [workflow／runtime 回歸](../artifacts/operations/agent-tool-efficiency-20261007/workflow-runtime-regression.xml)。
- [彙整驗收](../artifacts/operations/agent-tool-efficiency-20261007/tooling-acceptance.json)。

功能依據：[Herdr 安裝](https://herdr.dev/docs/install/)、
[Herdr CLI](https://herdr.dev/docs/cli-reference/)、[mosh 官方說明](https://mosh.org/)、
[tmux 官方入門](https://github.com/tmux/tmux/wiki/Getting-Started)、
[zsh invocation](https://zsh.sourceforge.io/Doc/Release/Invocation.html)。
