# vastai1T 雙卡 CUDA 恢復複測（2026-10-12）

## 1. 執行進度

- **本次 GPU／雙卡測試通過**。同一 instance `45963859` 的兩張 RTX 5090，現在 `Recovery Action=None`；`cuInit(0)=0`，兩個 UVM 節點都能正常開啟。
- 10:51（台北）執行 canonical strict CUDA preflight，退出 **0**。兩卡均通過實際配置／kernel／同步檢查，`cuda_compute_verified=true`，warnings 與 failures 為空。
- 以原 accepted frozen source 執行雙卡 torchrun／NCCL 測試，每個 rank **3 passed、3 deselected，51.87 秒**。測試包含非對齊完整股票軸的 compiled FIFO repeatability／DDP gradient，以及兩種容量的續單梯度對照；不是 6 個不同測試案例。
- 原前景消融入口 `--dry-run` 退出 **0**，27 項配置與 lookback-shifted calendar gate 通過。26 項最後 fold11，lookback256 最後 fold10；沒有改成全部同一 fold 編號。
- **未啟動正式消融長訓練**，未重啟 instance、重裝 driver、改成 CPU 或 AOT eager。兩份已追蹤正式 checkpoint 的 SHA256 與 10/09 相同。

## 2. 測試與版本邊界

實際環境為 Torch `2.11.0+cu128`、CUDA 12.8、driver `575.64.03`。來源檔案、wheel 與 source bundle 重新核對通過；accepted training source 仍是：

```text
30db2fe4f91db56bcfc9b0adf4936de1654e18ba3c2fc3703b90a8c2b9f8c530
```

沿用 training-reuse 流程：先做共用 CUDA 驗收，再保留 immutable release 的 strict 環境 gate；GPU 測試使用現有 node-global GPU lease 與既有 DDP／會計測試，不另寫訓練器或改正式模型。測試從 frozen source 目錄執行，使用該 source 的 package 路徑；測試檔取自遠端既有 `test/test_day_trade_entry_ddp.py`。

雙卡測試的選擇為：

```text
test_compiled_fifo_search_tail_repeatability_and_ddp_gradient
test_frozen_target_fifo_two_gpu_ddp_gradients（兩個 volume 參數）
```

前者使用 2,754 股票、270 分鐘與 64 FIFO cohorts；後者驗證續單前向／反向結果及 DDP 平均梯度。GPU compile owner 的預設 gate 啟用，對應 shared path 使用 Inductor fullgraph；沒有關閉 compile 來取得通過。測試結束後 `nvidia-smi` 無計算程序殘留。

本次只證明**目前** CUDA runtime、雙卡通訊、所選 compiled 會計／autograd 案例與原啟動 gate 可用。未重跑完整正式 epoch／fold、模型 head BF16 全市場測試、27 項消融報酬／圖表或各 embedding 的 VRAM 驗收。也未證明主機方採取了哪種恢復動作，不能由現在 `None` 推定故障根因或保證以後不再故障。

## 3. 原指令可由使用者前景續跑

在 vastai1T 執行：

```bash
cd /root/stockAgent
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh
```

原 v6 的 canonical resume、完成產物 gate、27 項順序、每項最後 fold 與逐項產圖設定保留。沒有提供新的 optimizer 或要求從頭重跑；沒有 profiling。本次 `--dry-run` 僅重新驗證／整理既有配置與 calendar，不進入 trainer。

## 4. 證據

- [整體複測收據](../artifacts/operations/training_diagnostics/tw_scale_cash_cuda_retest_20261012/acceptance-v1.json)：本機 agent run 身分、actual CUDA report、雙卡測試、calendar 與 SHA。
- [遠端雙卡測試收據](../artifacts/operations/training_diagnostics/tw_scale_cash_cuda_retest_20261012/ddp-acceptance-v1.json)；[雙卡完整 stdout](../artifacts/operations/training_diagnostics/tw_scale_cash_cuda_retest_20261012/ddp-test-v1.stdout)。
- [測試後 driver／UVM／checkpoint 核對](../artifacts/operations/training_diagnostics/tw_scale_cash_cuda_retest_20261012/host-runtime-after-v1.json)；[27 項 calendar](../artifacts/operations/training_diagnostics/tw_scale_cash_cuda_retest_20261012/calendar-context-after-v1.json)。

原 10/09–10/11 故障結果仍保留於[宿主恢復紀錄](tw_scale_cash_cuda_host_recovery_2026-10-09.md)及[10/11 複測與 Stop](vast_cuda_retry_2026-10-11.md)。本次沒有覆寫那些失敗收據。
