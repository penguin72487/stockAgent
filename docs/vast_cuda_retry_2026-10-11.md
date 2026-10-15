# Vast 故障主機 Start／CUDA 複測／Stop（2026-10-11）

使用者授權同一 instance 開機複測，通過才繼續原訓練，失敗就 Stop。
Penguin 沒有帳戶 API 憑證，使用者在 Vast 控制台按 Start；本次沒有新建 instance。

## 結果

- 已連回 instance `45963859`、machine `42054`；容器與 provider 身分相符。
  Provider 複測前為 `running`，仍是 `deverified`，分配兩張 RTX 5090。
- 2026-10-11 09:51:15–09:51:18（台北）執行 canonical
  `scripts/check_environment.py --require-cuda --minimum-cuda-devices 2 --strict`。
  退出 1；`cuda_runtime_initialized=false`、`cuda_compute_verified=false`。
  本次未進入成功的 GPU allocation／kernel，沒有啟動 trainer。
- `/dev/nvidia-uvm`、`/dev/nvidia-uvm-tools` 開啟均回傳 errno 5／I/O error；
  兩張 GPU 的 recovery action 都是 `Reboot`。
- 09:51:18 發出同一 instance 的 `PUT state=stopped`，Vast 回覆 success；
  隨後 SSH 明確 Connection refused。沒有從外部 API 獨立讀回 actual Stopped，
  因而保留 `stop_request_accepted=true`、`actual_stopped_verified=false`。
- 沒有啟動／修改正式訓練、重裝 driver、執行 GPU reset、CPU fallback、Destroy
  或刪除原始資料。既有 31 檔／13,540,936,113 bytes 保全包的三個 paired NAS ACK
  已重新依固定 dispatch 核對，全部通過。

NVIDIA 的 `Reboot` recovery action 要求宿主機 OS／node 恢復；容器
Stop／Start 不等於實體宿主 OS 重啟。本次沒有宿主 kernel log，不能以這個旗標
判定最初的 Xid 或硬體損壞原因。
[NVIDIA 官方說明](https://docs.nvidia.com/deploy/nvidia-smi/index.html#gpu-recovery-action)

## 證據與後續

私有收據：`/var/lib/stockagent/vast-restart-cuda-retry-20261011/cuda-retry-receipt.json`，
包含完整 canonical environment report、固定 provider 身分、Stop 回覆及既有備份
ACK 核對。憑證只在容器程序記憶體使用，沒有寫入 log、Git 或同步目錄。

有界操作腳本：`artifacts/operations/vast-restart-cuda-retry-20261011/probe_and_stop.py`。
任務 `vast-restart-cuda-retry-20261011`；run
`conditional-cuda-retry-20261011T015113-fa94a7d4` 退出 0，代表複測失敗後的
Stop 處理完成，並非 CUDA 通過。

先在 Vast 控制台確認 Stopped，等待 host owner 回覆實體宿主機已恢復後再複測。
兩張 GPU 的 strict 運算檢查通過後，才沿用原
`bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh` 與 canonical resume；
本次沒有改變原選定實驗、checkpoint 或資料 ABI。

Stop 保留磁碟且仍有 storage 費用。
[Vast 官方說明](https://github.com/vast-ai/docs/blob/main/guides/reference/faq/instances.mdx)
先前的完整備份與恢復路徑見
[Stop 前保全紀錄](vast_pre_stop_backup_2026-10-10.md)；初次故障證據見
[CUDA 宿主恢復紀錄](tw_scale_cash_cuda_host_recovery_2026-10-09.md)。
