# vastai1T v9 寬特徵密集訓練紀錄（2026-09-18）

## 實驗邊界

- 這是從零開始的新實驗；不讀取 v8 checkpoint 或 optimizer，正式產物根目錄是遠端 `artifacts/markets/tw_day_trade_last_last_only_layernorm_minute50_terminal_unlimited_capital10m_batch32_score_entmax_cash_22_effective_rank_v9_research143_fresh_foreground`。
- `training.day_trade_sparse_events: false`。維持 v8 的模型、雙卡 DDP、全域 batch 32、BF16、physical FIFO、官方開收盤日 K 代理和 50% 分鐘容量；不以稀疏事件更換訓練目標。
- 研究表選 93 個數值欄位及 50 個可用性旗標，共 143 個研究通道；v8 的 `next_session_open_gap_logret` 另加一個，實際模型輸入為 144 個通道。
- 研究表含事後修訂的總經值和部分估計公告時點；這是研究實驗，不可稱為嚴格 point-in-time 或可直接部署的回測。

## 固定資料和可重現性

- 正式 `tw-public`：`tw-public-20260918T013010528444073Z-l0-penguin-ac1e4027dfa26b98`。
- 私有研究表：`tw-public-research-wide-2014-20260918T015559385506194Z-l0-penguin-e54bba12a4f6cc9b`，9,614,954 列，截止 2026-09-17。研究表 SHA-256 `d2109865925dbd274416e99770cb3816a8b0f9f37e4c06152b0bfcee01f111b3`；其正式基底 SHA-256 `035b18687e5fa3974d0d87ea2d9013bb15fba0c841b210988d6000f87c5f1dff`。
- 研究快照連同企業行動 reference、entitlement、摘要及摘要引用的原始 manifest 一起發布。先前 7 檔快照缺 manifest，被預檢正確拒絕；目前使用 8 檔新版，不刪舊版。
- 模型特徵讀研究表，實體 FIFO 持倉來源明確讀正式 `tw_public_stock_daily.parquet`。兩者不互相冒充；正式來源仍受既有完整性驗證。
- 遠端 Syncthing index-only edge 稽核 `ok=true`、QUIC、`needBytes=0`、`completion=100`、零拉取／資料夾錯誤；兩個快照都完成 full object verification 與 READY materialization。此證據不代表訓練已完成。

## 驗證與效能

- 遠端嚴格 CUDA 環境檢查通過：兩張 RTX 5090，BF16。遠端研究面板投影／可用性時鐘／TIFRS 到期測試 3 passed，相關既有測試 65 passed；本機 staging manifest 防漏測試 6 passed。
- 第一次 `--check-data-only` 發現研究快照缺 entitlement raw manifest；補齊後第二次發現 physical FIFO 不能把研究特徵表當作正式來源。修正為雙路徑後，第三次預檢退出 0：3100 個 panel session、2755 個 symbol、144 個模型輸入、fold 11，`source_gap_symbol_days=0`。406 個未解換股事件仍按既有合約遮罩受影響前綴，不製造假價。首次實體來源快取建置 503.5 秒；下次內容雜湊完整驗證約 8 秒。
- 先前 v8 雙卡 fold 11 量測的 epoch 134 有 83 個訓練 batch，`train_total=63.217s`、`epoch_total=68.950s`；這不是 v9 的同資料對照，不能直接稱加速。
- v9 獨立 4-epoch 雙卡密集試跑在遠端 `artifacts/markets/tw_day_trade_v9_research143_dense_ddp_profile_e4` 完整退出 0。第 3／4 個 epoch 的最慢 rank wall time 是 70.225／69.660 秒，83 batch/epoch，新編譯圖均為 0；完整 fold 11 測試、逐 epoch 曲線、checkpoint、完成標記與根目錄 walk-forward 報告存在。`validate_completed_training_artifacts`：`ok=true`、`missing=0`、`invalid=0`。4 個 epoch 的投資績效不可作模型結論。
- 曾啟動 1000-epoch 背景 fold 11，但使用者要求前景執行後，已送 SIGINT 並確認 tmux pane 以 130 結束、沒有殘留訓練或 GPU 程序。背景嘗試在第一個 epoch 前停止；未刪除任何檔案。設定已固定到另一個 `_foreground` 產物根目錄。

## 2026-09-18 前景執行故障與修復

- 遠端 `_foreground` 根目錄現有 fold 11 的第 73 epoch 可續訓 checkpoint，也有另一輪選到 fold 1 的第 104 epoch checkpoint。兩個 fold 都沒有 `fold_complete.json`；不能把 checkpoint 說成完整訓練。最近一次 `progress.json` 的 `selected_ids=[1]`，不是使用者要求的 fold 11。沒有刪除或搬動任何產物。
- fold 1 在最終測試第 6/11 塊、`rows=[1280,1536)` 建立第二個 FP64 稠密容量平面時 CUDA OOM：尚需 2.84 GiB、當時只剩 2.70 GiB。故障不是模型 forward，也不是 epoch 104 的 backward；訓練後正式產物回測同樣是完整 fold 契約的一部分。
- 第一性原因是 physical adapter 的 `backtest_runner` 閉包持有上一塊完整分鐘 tape；原迴圈到下一塊綁定時才覆蓋此變數，令舊、新兩塊巨型 CUDA tape 的生命週期重疊。已在每塊回測結果拷貝並分離跨日 FIFO 狀態後，明確釋放閉包與結果；正式 270 點回測另設每塊最多 128 個交易日的峰值上限，逐 epoch 的輕量兩點驗證不受此上限影響。遠端原有 VRAM 自動上限仍可進一步縮小塊數。
- 新增回歸測試：下一塊綁定時前一個閉包已無引用、正式回測塊數受限且仍輸出 270 點。遠端 py_compile 成功，相關 CPU 套件 `24 passed, 3 skipped`；雙卡 DDP 的兩個參數案例各自單獨執行均通過。同一個 pytest 行程連跑兩個 NCCL 初始化案例曾在第二例出現 `socketPollConnect` 競態；單獨執行均通過，這不構成本次長歷史正式產物已驗收的證據。
- 尚未重新完成 fold 1 失敗的 11 塊正式回測，也尚未完成 fold 11 的完整訓練／正式產物驗收。後續只恢復 fold 11，不要把 fold 1 的失敗標記當成 fold 11 已完成。

## 遠端續訓指令（前景執行、只選 fold 11）

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t_v9_research_143_fresh.yaml \
  --start-fold 11 --max-folds 1 --resume
```

指令佔用目前終端直到 fold 11 完成，不使用 `tmux`、`nohup` 或背景符號。`--resume` 覆蓋設定檔的 `runner.resume: false`，應從該 fold 相容的第 73 epoch checkpoint 後續跑；先確認啟動畫面的 fold 選擇與 resume epoch。只有 fold 完成標記與生命週期驗證都通過才能稱完成。
