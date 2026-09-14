# TW day-trade physical-FIFO training hot-path optimization (2026-09-14)

## 1. 執行進度與驗收標準

- 效能驗收標準固定為 vastai1T 的雙卡 DDP 完整生命週期；單卡只可用於除錯與語意測試，不可作為加速結論。
- 驗收硬體為兩張 NVIDIA GeForce RTX 5090（各 32,607 MiB），全域 batch 32、每 rank local batch 16。
- 正式程式位於 `/root/stockAgent-daytrade-training-20260910`；正式訓練產物均位於遠端 `/root/stockAgent/artifacts/markets`。
- 使用者停止的 v4 正式訓練沒有被自動續跑，也沒有覆寫其 checkpoint/optimizer。
- 已把通過驗收的前處理快取與首段部署回放重用部署到正式遠端程式及本機原始碼。
- 正式雙卡驗收產物：
  `/root/stockAgent/artifacts/markets/tw_day_trade_hotpath_formal_acceptance_ddp_fold10_e1`。
  `progress.json` 為 `strategy=distributed_data_parallel`、`state=complete`、`failure=null`、完成 fold 10。

## 2. 第一性原理拆解

訓練的不可變語意是：逐交易日、逐分鐘、逐股票執行真實價格網格與 50% 分鐘量容量，維持整張、FIFO 成本、融資融券、費用、公司行動、跨日持倉與 optimizer 每 batch 更新。這些狀態具有時間相依性，不能用 detach、截短 BPTT、打亂交易日或只計算部分事件來換速度。

每一 global step 的牆鐘時間可視為：

`max(rank0 step, rank1 step) + DDP synchronization`

而不是兩個 rank 時間的平均。`forward` 內含 FIFO loss，`backward` 又反向走過同一張 FIFO 計算圖，因此 profile 欄位會嵌套，不能直接相加。

公平的雙卡 DDP baseline（fold 10、五 epochs）在 epoch 3–5 的最大 rank 牆鐘中位數為 `66.735 s/epoch`，每 epoch 76 個 global steps，約 `0.878 s/global step`。其中模型本身僅約 10 ms/batch；主要下界是 exact physical-FIFO forward/backward，加上每 epoch 驗證與測試的精確帳本回測。

## 3. 雙卡 DDP A/B 結果

| 方案 | 雙卡 epoch 牆鐘 | 語意/數值 | 決定 |
|---|---:|---|---|
| 固定最大 cohort baseline，5 epochs | 76.46, 66.49, 65.77, 67.12, 66.74 s | 基準 | 保留 |
| 每 session 最小 cohort，5 epochs | 76.48, 72.86, 70.43, 72.11, 66.61 s | 指標逐值相同 | 拒絕；穩態中位數 70.43 s，比基準慢約 5.5% |
| prepared-batch LRU，3 epochs | 86.96, 76.96, 69.79 s | 指標逐值相同 | 拒絕；rank 間 fetch 不平衡且沒有牆鐘收益 |
| 動態 sparse events | epoch 1 約 316.27 s | 編譯圖爆增且訓練路徑改變 | 拒絕 |
| 固定 sparse ABI，3 epochs | 109.75, 65.33, 66.51 s | 測試累積報酬與 Sharpe 明顯偏離 | 拒絕 |

基準與 cohort-only 的五 epoch 最終指標完全相同（best val `-0.4340216517`、test cumulative return `0.8751808869`、Sharpe `1.119689`、turnover `1.451702`），證明 cohort 版本雖保持語意，卻因較小 kernel 降低 GPU 利用率而較慢。

因此沒有把「看起來張量較少」的候選合併進正式熱路徑。這是用實測決定，不以 Big-O 單獨推論 GPU 速度。

## 4. 已正式採用的優化

### 4.1 內容定址、原子、fail-closed 的訓練轉換快取

Training-only temporal PCA/KLT 與 causal feature RMS 對相同資料 release、fold、特徵內容及設定是純函數。新版快取 key 綁定：

- panel feature/alive-mask 的內容 SHA-256、shape 與 dtype；
- exact valid indices；
- lookback、feature lag、basis families/components、RMS 參數；
- train years 與 fold IDs。

快取 envelope 另有 payload fingerprint；寫入採同目錄暫存檔、file fsync、atomic replace、directory fsync。內容損壞、schema/config/資料不符即忽略並重算，不會使用 stale transform。

雙卡實測：

- 冷啟動 temporal PCA：`48.236 s`
- 冷啟動 RMS：`5.175 s`
- 冷啟動 dataset/transform stage：`53.749 s`
- 相同契約快取命中：`0.324–0.332 s`
- 此階段節省約 `99.4%`，數值不變。

### 4.2 重用剛由 isolated child 寫出的 canonical physical prefix

每個 isolated DDP child 已用 canonical GPU FIFO ledger 完成 fold deployment artifact。舊 parent 隨後會在 CPU 再重播相同第一段帳本。新版只有在以下證據全部相符時才重用：

- ordered global symbols；
- exact dates；
- float64-normalized model request SHA-256；
- physical source release ID；
- initial capital/NAV；
- terminal exchange-session ordinal 與完整 carry artifact contract。

任一項不符即 fail closed，回到 canonical replay。後續 fold 因為必須繼承前一 fold 的 NAV/持倉，不會錯用 reset-capital artifact。

隔離候選與正式雙卡驗收的 `walkforward_deployment_backtest.npz` 所有欄位 SHA-256 完全相同，包括 minute NAV、shares、requests、returns、turnover、carry inventory 與 terminal state。

## 5. compiled 與 DDP 實際狀態

正式雙卡輸出已確認：

- `multi_gpu=DistributedDataParallel`，rank 0/2 與 rank 1/2 分別使用 `cuda:0`、`cuda:1`；
- BF16 autocast，FP64 physical finance ledger 維持精度；
- panel-slab model forward：`torch.compile(fullgraph=True)`；
- physical-FIFO session core：compiled，forward/backward probe 通過；
- AdamW：`fused=True`；
- 固定 symbol shape、固定 batch ABI，無 eager fallback；
- epoch 1 的新 graph 是冷啟動，穩態判定採 epoch 3–5。

## 6. 驗證證據

- 遠端正式針對性測試：`152 passed, 3 skipped in 11.11s`。
- 本機針對性測試：`154 passed, 2 skipped in 32.35s`。
- 遠端 CUDA strict environment gate：兩張 RTX 5090、CUDA 可用、無 warnings/failures。
- 遠端正式雙卡 DDP 完整生命週期：exit 0，fold、test、所有 cumulative walk-forward plots 與 lifecycle 均完成。
- 正式與隔離候選的 deployment artifact 欄位雜湊完全一致。

## 7. 目前演算法下界與後續方向

現有 PyTorch compiled graph 下，模型 forward 已不是瓶頸。此次可無損消除的兩個重複工作已消除：training-only transform 重算、第一段 physical deployment CPU 重播。

exact FIFO 熱路徑若還要明顯下降，下一個合理層級是單一 deterministic CUDA/Triton custom autograd kernel，同時融合事件掃描、FIFO cohort 更新、NAV/fee/carry reduction 與 backward。它必須通過逐 session 的 shares、claims、NAV、loss、gradient 與 optimizer trajectory 等價驗收；在此之前不應用 sparse reduction order 或截短梯度冒充加速。

## 8. 正式訓練命令

以下命令必須直接在 vastai1T 執行；產物會留在遠端 `artifacts/markets`。正式續訓是否啟動仍由使用者決定：

```bash
cd /root/stockAgent-daytrade-training-20260910
source scripts/runtime_env.sh

run_fintech_python scripts/check_environment.py --require-cuda --strict

run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t.yaml \
  --resume \
  --profile-timing
```

該設定的正式 output root 是：

`/root/stockAgent/artifacts/markets/tw_day_trade_last_last_only_layernorm_minute50_margin_carry_capital10m_batch32_share_replacement_v4`

執行後仍應從 log 驗收 `world_size=2`、兩個 rank、兩張不同 GPU 與 `strategy=distributed_data_parallel`，不能只因機器有兩張卡就宣稱是雙卡訓練。
