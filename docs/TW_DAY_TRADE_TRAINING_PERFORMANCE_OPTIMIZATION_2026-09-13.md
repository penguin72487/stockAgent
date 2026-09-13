# 台股當沖訓練全流程效能優化（2026-09-13）

## 結論

遠端 `vastai1T` 的正式設定已通過 CUDA strict gate、完整資料 gate、雙 GPU
三個 epoch 實測與相關回歸測試。先前正式訓練已完成 fold 1--5；本次效能工作沒有
啟動新 epoch，而是保留既有 checkpoint、修復停在 fold 5 reporting 的 lifecycle，並將
舊報表遷移成可增量恢復的 stitched state。所有訓練與測試產物皆位於
`/root/stockAgent/artifacts/markets`。

這次不以刪除 validation/test、縮短資料、改費率、改成交規則或製造假資料換速度。
保留 99 個特徵、2,754 檔、training-only PCA/RMS、BF16 DDP、50% 分鐘量、
09:00 決策／09:01 執行、實體 FIFO 融資融券延續、企業行動、每 epoch 曲線、
完整 test 與 walk-forward 累積報表。

## 第一性原理拆解

總耗時分成五個不可混為一談的階段：

1. 資料與來源驗證：panel、官方企業行動、分鐘／日 K 代理與實體 FIFO source。
2. fold 前處理：training-only PCA/KLT、因果 RMS、split tensor 與 compile probe。
3. epoch 熱路徑：model forward、實體帳本 forward、backward、DDP、validation/test curve。
4. fold 收尾：一次完整 test、checkpoint、表格、圖。
5. 跨 fold 收尾：不中斷現金、庫存與 claims 的 stitched physical account。

Profile 顯示模型 forward 不是主瓶頸；真正昂貴的是 exact physical ledger、其 backward，
以及先前重複執行的 prefix／stitched ledger。優化因此只移除可證明重複的工作。

## 已實作

### 1. 啟動與資料來源

- panel 與 physical source 使用內容定址 cache、SHA-256 `READY`／manifest 驗證。
- 同一 run 的非零 DDP rank 使用 rank-0 驗證 receipt，不再重複完整物件雜湊。
- TPEx 漲跌停 join、tick grid 與價格限制 sidecar 已向量化及快取。
- isolated fold 直接傳遞已驗證 receipt；父子程序仍各自驗證契約，不重建資料。

既有量測：outer startup 約 `40.178s → 18.629s`；isolated child startup 約
`81.459s → 25.950s`。數值來自同一遠端資料／硬體階段的冷啟動比較。

### 2. 完整 test 只跑一次帳本

完整 test 原本先跑全期間，再為 deployment prefix 重播一次 physical FIFO。
現在第一次 canonical pass 在指定 row boundary 同步擷取 exact
`DayTradeCarryState`，deployment prefix 直接切片；chunk boundary 會重新對齊，避免
產生 12-row 小 kernel。

- fold 6 final test stage：`105.165s → 70.426s`，減少 `34.739s`（33.0%）。
- `summary.json` 完全相同。
- `test_backtest.npz`、`deployment_test_backtest.npz`、
  `walkforward_deployment_backtest.npz` 所有陣列逐位相同。
- `model.pt` SHA-256 皆為
  `824640f1908547f1c831ed7d22e9aecd28cf99220e6d3ef0b8232d5839e3f1b7`。

### 3. stitched physical account 改為可恢復增量鏈

每個 fold 完成後仍立即刷新累積報表，但不再從第一天重播所有已驗證區段。

- 父程序最後才原子寫入 `deployment_stitched_context.json`。
- 一般 fold save 會先刪除舊 marker，避免重訓中斷後沿用舊狀態。
- reuse 必須同時符合 fold ID、execution mode、physical release ID、日期、全域股票
  順序、起始 capital/NAV、來源 request SHA-256 與 terminal NAV。
- 任一欄、artifact 或 request 不同即 fail closed，退回 canonical full replay；
  後續相依區段也一併重播。
- 已 reuse 的 fold 不重寫自身 NPZ／圖；root 累積 artifact 與要求的圖仍刷新。

真實 fold 6 resume：

- 首次 canonical refresh：端到端 `82.460s`。
- marker 命中：端到端 `34.893s`。
- 扣除約 `20.65s` 固定啟動後，walk-forward 收尾約 `61.9s → 14.2s`，減少約 77%。
- 快取命中後與快取前所有策略／帳本陣列逐位相同。

正式 artifact root 既有 fold 1--5 的冷遷移另做了完整驗收：resume preflight 明確回報
`5 contract-compatible completed folds`、`0 pending folds`，所以沒有啟動訓練；第一次為
五個舊 fold 建立 receipt 約需 32 分鐘，完成後相同命令的熱路徑為 `76s`（包含
`20.558s` 資料／來源 gate 及全部正式圖表），五個
`deployment_stitched_context.json` 均存在。lifecycle 已由卡在 fold 5 reporting 修復為
`state=complete`、`completed_ids=[1,2,3,4,5]`。後續正式續訓會從 fold 6 開始。

isolated parent 也會記錄最後一次已刷新的 fold 集合；若最後一個 child 後的結果集合與
最終集合相同，就直接重用該次刷新，不再無條件重畫完全相同的 root 報表。resume-only
仍保留一次權威刷新，可用來修復缺失或過期的 root artifacts。

### 4. vastai1T CPU 實體核心配置

正式 deployment config 現在指定 host-wide `cpu_threads: 112`。DDP 會分為每 rank
56 threads，對應遠端的 112 個實體核心；未使用 224 個 SMT logical threads。

fold 11、batch 32、雙 RTX 5090、epoch 2/3 OFAT：

| 指標 | CPU 24 | CPU 112 | 結果 |
|---|---:|---:|---:|
| pre-epoch | 92.559s | 74.245s | -19.8% |
| 穩態 epoch 中位數 | 66.258s | 65.152s | -1.7% |
| 穩態 train 中位數 | 60.827s | 59.048s | -2.9% |
| 完整 epoch 吞吐 | 40.040 rows/s | 40.720 rows/s | +1.7% |
| 峰值 VRAM | 47.51% | 47.49% | 安全 |

兩次完整 run 的 `model.pt` SHA-256 都是
`b49048d6e5dcc767e6778d3caf30b63814576e9785fdca632a1e13b8ea161371`，
epoch loss 與 full test 每個陣列亦相同。stitched turnover 只有 5/168 個 FP64 reduction
值因執行緒加總順序相差最多 `4.44e-16`；成交、NAV、持倉、費用與報酬相同。

CPU24 run 的 `456.077s` 與 CPU112 run 的 `380.953s` 不作為純 OFAT 百分比，因第二次
run 同時命中既有 TorchInductor compiler cache；採用判斷只依上表的 pre-epoch 與
零新 graph 的 epoch 2/3。

## 被實測否決的候選

- `eval_backtest_chunk_rows=512`：RTX 5090 在 rows 756–1268 OOM，使用約
  31,350 MiB 後仍需 5.67 GiB；正式設定保留 auto 256。
- global batch 64：既有實測 OOM；正式設定保留 batch 32。
- sparse-event 路徑雖接近速度，但會改變 FP64 summation／optimizer path，不作為正式
  physical FIFO 訓練替代。
- 不能省略 epoch validation、sampled test curve、完整 final test 或 fold 完成報表；
  它們是模型選擇與 artifact 完整性契約。

## 驗收證據

- Ruff：通過。
- physical carry／artifact／margin／stateful／fold isolation／packed edge：
  `182 passed, 2 skipped`。
- 新增 prefix state、快取命中／拒絕、跨 fold initial NAV 測試：通過。
- `scripts/check_environment.py --require-cuda --strict`：0 warnings、0 failures，
  兩張 RTX 5090、PyTorch 2.11.0+cu128。
- 正式 config `train.py --check-data-only`：退出碼 0；3,096 sessions、2,754 symbols、
  99 features、11 folds、physical source gap 0。
- 正式 artifact resume preflight：fold 1--5 全部契約相容，0 pending；熱快取完整刷新
  `76s`，lifecycle 最終為 `complete`。
- physical release：
  `tw-day-trade-carry:b158cef795b9dae6d087516b5895873c57e6b11073d0480c1cb94a9c00e36372`。

## 正式啟動命令

```bash
cd /root/stockAgent-daytrade-training-20260910
source scripts/runtime_env.sh

run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t.yaml \
  --check-data-only

run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t.yaml
```

最後一條正式命令未由本次效能驗收自動啟動。它會保留已完成的 fold 1--5，從 fold 6
繼續其 1,000 epoch 設定。artifact root 為：

`/root/stockAgent/artifacts/markets/tw_day_trade_last_last_only_layernorm_minute50_margin_carry_capital10m_batch32_share_replacement_v4`

## 邊界

「理論極限」只能針對目前兩張 RTX 5090、112 實體核心、指定資料 release 與 exact
physical FIFO 數學做量測。其餘主要時間是 exact ledger forward/backward；若改用近似
成交、稀疏求和、較少日期或較少驗證，雖可更快，但不再是同一訓練契約，因此未採用。
