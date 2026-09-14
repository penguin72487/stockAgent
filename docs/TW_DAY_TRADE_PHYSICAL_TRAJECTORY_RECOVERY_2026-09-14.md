# 台股實體 FIFO 訓練軌跡失效修復（2026-09-14）

## 結論

原始故障不是 DDP 通訊中斷。兩個 rank 都在同一個物理持倉驗收點收到
`inventory.failed`，但舊錯誤只輸出
`physical training trajectory failed; cannot skip a batch`，因此遺失了第一個失敗日期、
股票與不變量。原 epoch 9 checkpoint 的模型及 optimizer 張量均為有限值，帳戶亦未發生
financial default。

同一 checkpoint 重啟後連續完成 epoch 10–20，未再出現固定日期的資料錯誤。這排除了
「每次必然重現的靜態來源缺口」，並把剩餘風險縮小到程序內 compiled session cache／
編譯結果的非決定性失配。因為舊版沒有保留失敗批次的細節，不能把這項推論誤稱為已取得
原始壞日期的直接證據。

## 第一性原理修正

遠端正式訓練 repo：`/root/stockAgent-daytrade-training-20260910`

1. `stockagent/training/day_trade_carry_bridge.py`
   - 健康熱路徑維持原 compiled physical FIFO。
   - compiled 結果出現 non-finite 或 `inventory.failed` 時，使用完全相同的模型動作、
     前一持倉、費率與官方 session，在同一 GPU 執行 `no_grad`、關閉 event compression
     與 compiled wrappers 的獨立 eager dense oracle。
   - eager oracle 會在第一個壞 contract-day 停止，輸出日期、row、持倉來源缺口、
     corporate-action gap、開盤／分鐘／收盤缺價與股票索引。
   - 只有 eager oracle 完全有效時，才把原失敗分類為 compiled/oracle mismatch；此時清除
     程序內 compiled cache，並在 backward 與 optimizer step **之前**重新計算同一批一次。
   - 健康批次不增加額外 DDP collective。若只有一個 rank 需要自癒，其他 rank 會停在既有
     backward 同步點；若該 rank 無法恢復，任何 rank 都不能完成 optimizer step。
   - 若 eager oracle 也失敗，或清 cache 後重試仍失敗，仍然 fail-closed；不跳批次、不清掉
     部分持倉、不製造價格、不沿用失敗狀態。

2. `stockagent/backtest/tw_day_trade_carry.py`
   - 新增只清除 process-local compiled callables 的明確 API。
   - 不重設 telemetry、不修改來源、checkpoint 或物理帳戶。

3. `test/test_day_trade_carry_training.py`
   - 保留真實來源／估值錯誤必須停止訓練的測試。
   - 新增 transient compiled failed-bit 測試：只有 eager oracle 證明同一批有效，才允許清
     cache 並重算；驗證仍完成所有 batch 與 optimizer steps。

## 驗收證據

- 靜態編譯：三個修改檔均通過 `py_compile`。
- 物理訓練、來源與 stateful carry：`39 passed, 3 skipped`；skip 是需由 torchrun 獨立
  啟動的兩個真實兩-rank 測試及 opt-in 全架構 GPU 測試。
- 兩個真實兩-rank NCCL／autograd／optimizer 測試另行執行：`2 passed`；包含健康路徑
  及「只有 rank 1 收到 transient failed bit」的故障注入。兩個 rank 最終參數逐 bit 相同，
  證明恢復 rank 能在 backward 前重算同一批並加入原本的梯度同步。最終程式的 cold/warm
  synthetic integration epochs 約為 7.49s／0.31s；此微型測試用來驗證一致性，不作為正式
  2,754 檔吞吐量基準。
- 正式資料／正式模型從原健康 checkpoint 續跑並完成 epoch 10–20；修正後另完成 epoch
  19–20，皆為 61/61 batches 與 61 optimizer steps，portfolio alive、沒有 settlement
  default、沒有 traceback，也沒有觸發自癒分支。
- CUDA 嚴格環境閘門通過：Python 3.12.14、Torch 2.11.0+cu128、兩張 RTX 5090，
  `warnings=[]`、`failures=[]`。
- 全部 11 folds 的 `--check-data-only` 通過：3,096 sessions、2,754 symbols、99 features，
  exact physical release
  `tw-day-trade-carry:b158cef795b9dae6d087516b5895873c57e6b11073d0480c1cb94a9c00e36372`。
- epoch 20 checkpoint：
  - 大小：21,742,223 bytes
  - SHA-256：`79cfe534e9baf97da97396a16f020bdb072912c5dc29ba6e3d1887149b7e4297`
  - model：126 tensors，全部有限
  - optimizer：366 tensors，全部有限
  - RNG schema 3：兩個 rank 均有獨立狀態
  - effective global batch size：32
- 驗收結束後已停止測試訓練；沒有遺留 `train.py`／`torch.distributed` 程序。

## 正式續訓命令

```bash
cd /root/stockAgent-daytrade-training-20260910
source scripts/runtime_env.sh

run_fintech_python scripts/check_environment.py --require-cuda --strict

run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t.yaml \
  --start-fold 8 \
  --resume \
  --no-retrain-completed-folds
```

此命令會從 fold 8 的 epoch 20 checkpoint 繼續；不要改用新的 artifact root，也不要刪除
既有 checkpoint。遠端這組正式訓練程式與設定目前仍是未追蹤工作樹內容，開始長期訓練前
應把完整遠端訓練變更做語意化 Git 整理；本次沒有擅自 commit 或覆蓋其他既有修改。
