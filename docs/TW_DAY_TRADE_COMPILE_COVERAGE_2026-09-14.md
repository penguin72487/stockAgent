# 台股實體 FIFO 訓練 compiled 覆蓋稽核（2026-09-14）

## 結論

遠端 `vastai1T` 的正式訓練不是全 eager，也不是把整個 epoch 包成一個
`torch.compile` graph。目前採用的邊界是：固定形狀且重複執行的 GPU 張量核心編譯，
資料／日期／持倉編排與檔案 I/O 維持 eager。這是正確的責任分界。

正式 resolved config：

- `enable_torch_compile: true`
- `compile_eval_model: true`
- `compile_loss: false`
- `backtest_compile: true`
- `backtest_compile_stateful: true`
- `backtest_compile_dynamic: false`
- `strict_no_fallback: true`
- BF16 AMP、DDP、global train batch 32

`compile_loss: false` 只代表不編譯整個外層 `risk_aware_loss`。physical-FIFO 由專用的
`fullgraph=True, dynamic=False` session core 編譯，因此不能把這個設定解讀成 FIFO
loss 未編譯。

## 實際 compiled 邊界

| 區域 | 狀態 | 理由 |
|---|---|---|
| 訓練模型 panel-slab forward | compiled fullgraph、固定形狀 | 每個唯一 panel row 只投影一次，避免重複 materialize window |
| 驗證／測試模型 forward | 獨立 compiled fullgraph、固定形狀 | 隔離 train/eval graph 與 autograd ABI |
| physical-FIFO 單日 session tensor core | compiled fullgraph、固定形狀、CUDA graph 關閉 | 保留 FIFO、分鐘容量、費用、carry、企業行動及梯度 |
| compiled core 的 backward | AOTAutograd 產生 | `train_backward_*` 是計時名稱，不代表 eager backward |
| backtest input preparation | compiled | epoch telemetry 使用 `bt_prep_compile_hits` 驗收 |
| 外層跨日 session loop | eager | 擁有 Gregorian day、可變 claim/cohort 狀態、錯誤診斷與自我修復 |
| 整個外層 loss wrapper | eager | 全圖編譯會依 batch 長度展開跨日遞迴，造成病態 graph/codegen；核心已另行編譯 |
| NPZ decode、CPU pack、prefetch、H2D orchestration | eager | 檔案 I/O 與 Python 物件生命週期不是張量 kernel |
| DDP/NCCL、fused AdamW、grad clip | 原生 CUDA/通訊實作 | 不需要再包一層 `torch.compile` |
| checkpoint、JSONL、繪圖、報告 | eager/CPU | 有副作用的 I/O，不屬於 compile 加速範圍 |

generic `bt_compile_hits=0` 不是 physical-FIFO 未編譯；該模式繞過 generic simulator
runner，應讀 `day_trade_carry_compiled_session_calls`。同理，CUDA 分段計時為零時，
應以完整 epoch wall time 與 session compile counters 判斷，不可用欄位名稱猜測。

## 目前正式證據

遠端工作目錄：`/root/stockAgent-daytrade-training-20260910`

正式 artifact：
`/root/stockAgent/artifacts/markets/tw_day_trade_last_last_only_layernorm_minute50_margin_carry_capital10m_batch32_share_replacement_v4`

2026-09-14 讀取 fold 9 的 epoch 124--143（20 個完整 steady epochs）所得中位數：

- 完整 epoch：`50.624 s`
- train：`45.347 s`
- source fetch：`70.592 ms/batch`
- compiled model forward：`7.564 ms/batch`
- factor augmentation：`18.272 ms/batch`
- physical-FIFO loss：`327.942 ms/batch`
- backward：`239.922 ms/batch`
- validation physical backtest：`4.271 s`
- sampled test-curve physical backtest：`4.961 s`（與其他工作有重疊，不能直接與 wall time 相加）

epoch 143 的 compile 驗收：

- `dynamo_unique_graphs_total=11`
- `dynamo_unique_graphs_epoch_delta=0`
- `day_trade_carry_compiled_session_calls=2411`
- `day_trade_carry_eager_path_calls=0`
- `day_trade_carry_compile_failures=0`
- `day_trade_carry_eager_fallback_calls=0`
- `bt_prep_compile_hits=69`
- `day_trade_carry_padded_session_calls=2358`

啟動 probe 亦通過：模型 forward/backward probe 約 `6.704 s`；physical-FIFO
forward/backward probe 約 `3.935 s`，Inductor persistent cache 有 hit、無 miss。正式啟動後
有 40 個新檔寫入 `/root/.cache/torchinductor`，`/tmp/torchinductor_root` 無新檔，證明
resolved config 的 persistent cache 正在使用；`/proc/<pid>/environ` 只呈現程序初始環境，
不能用它否定 Python 啟動後的 `os.environ` 更新。

## 下一個可證明的優化方向

目前模型 forward 只佔完整 epoch 約 1%，再調模型 compile 的回報很小。最大候選是
physical-FIFO 的 cohort padding：epoch 143 有 2,358/2,411 次 session call 使用 padding。
可建立「每個 session 取最小 power-of-two cohort ABI」候選，讓早期 session 不必全部用
整個 batch 的最大 cohort 寬度。這項變更必須：

1. 保持完整 session 序列的 return、turnover、minute NAV、最終 FIFO state 及 action/state
   gradient 與目前 eager oracle 一致；
2. 分別量測 cold compile、epoch 2+ steady wall time、graph 數與 peak VRAM；
3. 不截斷跨日梯度、不改 optimizer cadence、不啟用 CUDA graphs；
4. 通過真實雙 rank DDP 測試及完整正式 shape benchmark 後才切換預設。

第二優先才是把 field-major CPU staging 做成有上限的 pinned ring buffer，重疊目前每 batch
約 70.6 ms 的 source fetch／搬運。不能把整份資料無上限 pin 住，也不能以省略正式資料驗證
換取速度。

本次稽核期間 fold 9 正在正式訓練，兩張 RTX 5090 均被 DDP 使用。為避免同一 optimizer
trajectory 的後續 fold 靜默讀到另一版執行器，本次不熱改 source、不停止程序，也不在同一
GPU 上插入會污染 epoch wall time 的候選 benchmark。只有在 checkpoint 邊界停止並以隔離
候選完成等價與吞吐驗收後，才應啟用下一個 compiled ABI。

## 已建立但尚未啟用的隔離候選

候選副本位於：
`/root/stockAgent-daytrade-compile-candidate-20260914`

候選已完成以下實作：

- 每個 session 依目前 logical cohort rows 選最小 power-of-two output ABI；
- compiled callable 仍以 device、dtype、cohort width、symbol width、grad mode 及 executor
  contract 分離快取；
- 增加 compiled/logical cohort-row telemetry，讓正式 benchmark 可直接量化 padding 工作量；
- 不改 FIFO、費用、分鐘容量、持倉、企業行動、return、optimizer cadence 或 BPTT 邊界。

目前在不搶用正式 GPU 的條件下，`py_compile` 通過，CPU/eager 相關測試為
`57 passed, 5 deselected`；deselect 的是需要真實 CUDA compiled path 的測試。這證明候選
沒有改動 eager 語意與一般 ledger 測試，但**尚不足以啟用**。仍需在可獨占 GPU 時完成：

- compiled/eager forward、state、minute NAV、turnover、action/state gradient 等價；
- cold compile 與 steady-state actual-shape benchmark；
- 雙 rank DDP 完整 epoch 及 resume 驗收。
