# vastai1T CUDA 初始化失敗：宿主機恢復邊界與訓練前檢查修復

## 1. 執行進度

- **仍不能開始雙卡訓練**：2026-10-09 複測 instance `45963859`，兩張 RTX 5090 都回報 `GPU Recovery Action: Reboot`。使用者已重啟 instance；本次沒有再次啟停容器、重置 GPU、重裝驅動或搬遷資料。
- 獨立於 PyTorch 的 CUDA Driver API `cuInit(0)` 回傳 `999 / CUDA_ERROR_UNKNOWN`，開啟 `/dev/nvidia-uvm`、`/dev/nvidia-uvm-tools` 都失敗，errno 為 `5 / EIO`。
- 已修正並部署共用環境檢查與消融薄控制入口。失敗時保留完整 JSON 診斷，明確指出宿主機恢復需求；不再因查詢 GPU 名稱而拋 traceback，也不再額外拋 `CalledProcessError`。
- 本機相關回歸 **85 passed**；遠端 Torch 2.11 環境檢查語義測試 **23 passed**。實機入口 preflight 預期退出 **1**，無 traceback；這是正確拒絕，不是 CUDA 已恢復。
- 已重新驗證 accepted frozen source 與 wheel／source bundle；兩份現有正式 `checkpoint_last.pt` 的 SHA256 與診斷前相同。Inductor fullgraph、BF16、模型、loss、資料、27 項消融順序與 canonical resume 均未修改。

## 2. 第一性原理：可看見 GPU，不等於能在 GPU 計算

原本失敗鏈為：NVML 仍能列出兩張 GPU，但 CUDA runtime 不可初始化；環境檢查直接以 `device_count()` 查詢各卡名稱，觸發 lazy initialization，再被外層 `check=True` 包成第二段 traceback。

本次分層證據：

| 層級 | 實測 | 能證明／不能證明 |
| --- | --- | --- |
| NVML／`nvidia-smi` | 可列出兩張 RTX 5090；兩卡 recovery action 都是 `Reboot` | 可管理／查詢，不代表可執行 CUDA |
| 分配的 GPU 節點 | `/dev/nvidia2`、`/dev/nvidia3` 可開啟 | 不是所有已分配 GPU 節點都被權限擋住 |
| UVM | 兩個 UVM 節點開啟皆 EIO | 驅動／宿主機裝置路徑失效；不是模型 forward 失敗 |
| CUDA Driver API | 不匯入 Torch 的新程序 `cuInit(0)` 也回傳 999 | PyTorch 報錯是下游症狀，修改模型不能修復此層 |
| PyTorch／訓練 | CUDA 初始化失敗，未執行訓練 | 本次無法驗收新的雙卡 DDP 運行 |

宿主 kernel module 與注入的 `libcuda.so.1` 都是 `575.64.03`；UVM 節點 major `505` 與 `/proc/devices` 登錄相符。新程序繼承 visibility 或在匯入 Torch **之前**設定 `CUDA_VISIBLE_DEVICES=0,1`，都初始化失敗。因此不能把錯誤提示中的「可能修改 visibility」當成已證明根因，也不能當成單純節點 major 過期。

NVIDIA 定義 `Reboot` 為需要 OS／node 重啟的恢復動作；只重啟 Docker instance 不等於宿主機 OS 重啟。這個旗標**不能辨識觸發它的原始故障**；容器無權讀宿主 kernel log，本次未判定 Xid、硬體損壞或原始 UVM 故障原因。[NVIDIA 官方說明](https://docs.nvidia.com/deploy/nvidia-smi/index.html#gpu-recovery-action)

證據：[初次 driver／syscall 診斷](../artifacts/operations/training_diagnostics/tw_scale_cash_cuda_init_20261009/driver-diagnosis-v1.json)、[宿主恢復需求與保護驗證](../artifacts/operations/training_diagnostics/tw_scale_cash_cuda_init_20261009/host-recovery-required-v1.json)。

## 3. 已修正的軟體邊界

[`scripts/check_environment.py`](../scripts/check_environment.py)：

- 把 discovery、初始化、實際 allocation／kernel／synchronize 證據分開，失敗仍輸出 JSON。
- `--require-cuda` 在每張可見卡實際配置、執行一個元素的運算並同步；`--minimum-cuda-devices 2` 阻擋不足雙卡的 DDP 啟動。
- CUDA 失敗時只讀取裝置 metadata、嘗試開啟節點並查詢 recovery action；`nvidia-smi` 查詢限時 5 秒，缺少指令、舊版不支援、格式錯誤或逾時保留為未知，不掩蓋原本 CUDA 失敗。
- 不在匯入 Torch 後改 visibility，不重建／chmod 裝置，不重置 GPU、不重裝注入驅動、不降到 CPU。驗收失敗不寫入成功 runtime lock。

[`scripts/run_tw_scale_cash_annual_ablations.py`](../scripts/run_tw_scale_cash_annual_ablations.py)：

- 先以目前共用 checker 驗收實際雙卡運算，再保留 immutable release 的 strict package／environment gate。
- 當任一 gate 失敗，直接回傳失敗碼與原因，不啟動 trainer、不改 optimizer。
- 未修改 frozen release 中的 checker 或模型來強迫 source SHA 通過。

保持的 training source：`30db2fe4f91db56bcfc9b0adf4936de1654e18ba3c2fc3703b90a8c2b9f8c530`；head ABI 仍為 `compiled_partition_sm12_narrow_fp32_head_only_aligned_sdpa_adjoint_abi_v9`。本次僅更新可變的操作入口，沒有引入新的模型／optimizer ABI。

本機回歸包括 CUDA discovery 假陽性、雙卡部分失敗、實際 kernel 驗證、CPU 可選查詢、未知 recovery 查詢、host reboot 診斷、runtime identity，以及消融 gate／resume 契約。遠端 23 項為語義測試，不冒充實際 CUDA 或 DDP 成功。實機測試只有共用 checker 與 `_training_preflight`，沒有啟動正式消融隊列。

## 4. 需要 Vast 主機方處理

目前容器為 unprivileged Docker，沒有恢復宿主 OS／驅動的權限。可向 Vast／host owner 提供下列訊息；本次未代為送出客服訊息，也未授權建立其他 instance。

```text
Instance 45963859, 2 x RTX 5090, NVIDIA driver 575.64.03.
Restarting the instance/container has already been tried.
Both allocated GPUs report GPU Recovery Action: Reboot.
cuInit(0) returns 999 / CUDA_ERROR_UNKNOWN even without PyTorch.
Opening /dev/nvidia-uvm and /dev/nvidia-uvm-tools fails with errno 5 (EIO).
Please recover/reboot the physical host OS as required by NVIDIA,
and inspect the host driver/kernel logs for the original fault.
Please preserve this instance filesystem, datasets and training checkpoints;
do not destroy or recycle it without my approval.
```

宿主機恢復後，在 **vastai1T** 驗收：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py \
  --require-cuda --minimum-cuda-devices 2 --strict
```

只有退出 0、`cuda_compute_verified=true` 且兩張卡都通過後，才重新使用原本的前景指令：

```bash
cd /root/stockAgent
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh
```

同 v6 的相容 checkpoint 仍由原入口 canonical resume；不是從頭重訓。先前 source／head 雙卡驗收見[窄 head 修復報告](tw_scale_cash_narrow_head_gradient_repair_2026-10-08.md)，那是 10/08 當時的接受證據，不代表目前已損壞的宿主 CUDA 仍可使用。本次未宣稱宿主機已恢復、完整雙卡訓練已重跑，或能保證未來不再發生主機故障。
