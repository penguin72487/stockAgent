# Vastai1T 年度訓練結構優化 — 2026-10-14

原本的正式 v8 訓練已停止；本輪只啟動有界測試，尚未重新啟動 1000 輪消融。
這次修改的是計算分工、傳輸與帳務融合，沒有縮小模型、資料、特徵或輸出工作量。

## 停止與基準

09:15:43 UTC 透過核對 boot ID、PID 與 start time 的 canonical workflow runner
取消正式 queue，14 個捕捉到的 descendants 均已退出。第二次獨立 GPU 回讀確認無
訓練程序。四個原有 checkpoint 的 SHA256 保持一致，canonical CPU weights-only
loader 均可載入；baseline 最後 epoch208，Muon 最後 epoch300。

證據：[stop receipt](../artifacts/operations/gpu-structural-optimization-vastai1t-20261014/stop-receipt.json)、
[checkpoint readback](../artifacts/operations/gpu-structural-optimization-vastai1t-20261014/checkpoint-readback.json)。
原 v8 root 與已完成結果不覆寫。
四個 checkpoint 另有本機獨立副本，位於本輪 operation root 的
`preserved-formal-checkpoints/`；傳回後再次逐檔核對 SHA256。

比較基準是遠端實際載入的 immutable source：
`4beacac6b8a9823e0a6f8097ae4f423a74e9e271ba713502c64e73462fc0b2c1`。
隔離 worktree `/root/stockAgent-gpu-structural-20261014` 在修改前核對為完全相同的
source identity。主工作區其他 dirty work 保持原狀。

實際 baseline 為 d32、lookback32、14,726 logical features、2,757 symbol slots，
兩張 RTX5090，global batch256／local128、eval16、BF16 AMP、FP32 master parameters
與敏感 FP64 帳務；最後 fold11。d128 與 lookback256 是原 catalog 的其他消融項目，
沒有為測速更改它們。四輪測試固定原 1000 輪 scheduler horizon 和 32-step warmup，
並保留 validation、test、每輪 curves、plots、checkpoint、最終 backtest 與 lifecycle
驗收。計時用最大 rank 的完整 epoch，另列完整四輪 fold 含啟動和收尾。

## 第一性原理與實作

GPU 的效率由依賴圖、重複工作、記憶體傳輸與呼叫粒度共同決定。年度內
`state[t+1] = F(state[t], action[t], source[t])` 仍有現金、整股、費用、claims 和存活
狀態依賴，因此不能把每日資金當成獨立 batch。已重置的年度帳戶才是合法的獨立軸。

新 `episode_parallel.py` 按 configured period labels 分派完整帳戶，依有效日期
數平衡兩卡，各卡只執行自己的 model 與完整股票帳務，移除逐 batch 的 action gather
及另一卡的相同帳務重播。每卡的 owned 年度保持時間順序，來源仍由 canonical
physical bridge 檢查首日、連續日、年度 reset、未結持倉、claims 與 invalid state。

`L_rank = world_size * sum(owned date losses) / global_valid_dates`，最後才做一次
dense gradient mean、global clipping、optimizer 與 scheduler update。不等數 NN
呼叫直接使用已同步參數的 module，避免 DDP forward 的隱含 collective。局部未使用
而全域有使用的梯度貢獻零；全域未使用者保持 `grad=None`，避免額外 weight decay。
[PyTorch DDP 文件](https://docs.pytorch.org/docs/2.11/generated/torch.nn.parallel.DistributedDataParallel.html)
說明同步點與平均梯度契約；真正驗收來自本輪兩 rank 更新對照測試。

v1 同時按 local128、舊 global256 和年度切批，產生 18 次固定形狀呼叫；v2 移除
舊 global 切點，兩卡變成 12／10 次，分別擁有 1,462／1,223 個有效日期。全體 2,685
個日期沒有遺漏或重複，NN padded slots2,816，補齊占 4.65%。

金融 forward 仍跨日 carry。梯度改在 local128 與年度邊界截斷，屬於使用者明確允許
的梯度實驗。ABI 為 `annual_whole_account_local_actions_weighted_gradient_mean_v2`；
config fingerprint 同時綁定卡數，卡數不符直接拒絕，不沿用舊 optimizer resume。

另一個 opt-in 融合將收盤 NAV、盤中最低值、absorbing default 和 atomic commit
抽為不包含 Python 日期的 Tensor block。公式、FIFO、費率、整股、來源失敗與
存活狀態保持 canonical 行為；`STOCKAGENT_DAY_TRADE_FINALIZE_COMPILE=1` 啟用
fullgraph，strict 模式不接受 eager fallback，版本另進 fingerprint。

## 遠端完整 fold 初篩

下表各為一次完整四輪測試，尚不把小差距當成可重現結論。

| 配方 | 穩態 MAX epoch 秒 | 完整四輪 fold 秒 | 驗收 |
| --- | ---: | ---: | --- |
| 原 frozen control | 74.492 | 510.032 | 通過 |
| immutable metadata 免傳 | 72.446 | 486.048 | 通過 |
| 年度分工 v1 | 約67.317 | 464.085 | 舊 batch-counter gate 拒絕；保留原失敗結果 |
| 年度分工 v2 | 52.894 | 397.349 | 通過；共享 RAM 最少54.69 GiB餘裕 |
| 只有收盤融合 | 71.279 | 1,078.008 | RAM 餘裕15.46 GiB低於16 GiB，拒絕 |
| 年度分工＋收盤融合 | 49.528 | 475.748 | 通過；RAM 最少48.81 GiB餘裕 |

單獨融合的 compiled-loss forward/backward probe 是459秒，年度分工未融合為13秒。
融合的暖機成本不能隱藏。組合穩態更快但此初篩完整 fold 比單獨年度分工更慢，
所以正式多輪訓練與短跑的成本分開列。不是宣稱已跑完 1000 輪。

## 最終 A/B/B/A 驗收

四組均使用最終共用 source
`fddb72ce10f18e1494c99d910af9ee3b620bf760ebb2160b7e20de0513ba6283`
完成 A/B/B/A：A 原分工、B 年度分工加收盤融合，每次四輪；四組皆通過完整 lifecycle。
實際執行環境是 Torch2.11.0+cu128、QuACK0.6.5、CuTe DSL4.8.0；release build
receipt 的本機 Torch2.13 版本不是遠端 benchmark 執行版本。

| 試驗順序 | 穩態 MAX epoch 秒 | 完整四輪 fold 秒 |
| --- | ---: | ---: |
| A1 原分工 | 73.121 | 488.574 |
| B1 年度分工＋融合 | 49.877 | 476.477 |
| B2 年度分工＋融合 | 53.343 | 485.305 |
| A2 原分工 | 75.706 | 492.061 |
| A 平均 | 74.414 | 490.318 |
| B 平均 | 51.610 | 480.891 |

穩態完整 epoch 時間減少 **30.644%**，相同有效日期吞吐為 **1.4418倍**。
含啟動、編譯與收尾的完整四輪 fold 只減少 **1.923%**；此小幅改善不能外推為
1000 輪的已實測結果。候選 epoch 仍有49.88–53.34秒的現場變動。
組合是本輪接受候選中穩態最快的配方；單獨年度分工與組合的差距只有初篩證據，
尚未證明收盤融合相對單獨年度分工有可重現的長期增益。

GPU0 全流程 NVML 平均由18.4–18.7%降為11.8–12.7%，GPU1仍約20–22%；
雙卡最高顯存占用由22.1 GiB降至20.0 GiB。**沒有宣稱平均 GPU busy%提高。**
這個平均包含編譯、eval 與收尾，且不能區分有效運算、重播與 collective 等待，
因此不是 Tensor Core occupancy 或有效效率的直接量測。第一性原理的目標是
每個正確更新所需的時間、傳輸和重複工作；本輪證明了固定工作量更快。

逐組 result SHA、runtime、ownership、GPU samples 與計算式見
[balanced acceptance](../artifacts/operations/gpu-structural-optimization-vastai1t-20261014/balanced-acceptance.json)。

## 正確性、消融與可重用經驗

本機 shared accounting/checkpoint 測試272項通過；年度分工、benchmark admission、
checkpoint suite162項通過；消融 catalog／binding／capacity／failure policy56項通過。
最終修改後 focused contracts71項通過、2項因環境條件跳過。
遠端 Torch2.11.0+cu128 上，年度分工12項通過，收盤 CUDA11項通過，包括跨日梯度、
claims、locked deliveries、terminal close、破產與來源失敗原子性。原始失敗日誌保留。

v1 的舊 batch-counter 拒絕沒有改成強制成功。新 gate 要求 canonical trainer 產出的
`annual_episode_ownership.json`，獨立驗證全日期覆蓋、帳戶唯一 owner、區段、權重、
卡數、補齊量與 update cadence，再核對 epoch curve。它仍拒絕缺漏、重複和虛假權重。

消融 v9 保留 baseline → 七個 optimizer 配方（Muon 在 baseline 後）→ 原27個 OFAT，
每項先跑自己的最後 fold，失敗記錄後繼續，只有 OOM 才使用既有 capacity retry。
`annual_reset__off` 是單一連續帳戶，明列沿用 metadata 免傳的 replicated ledger；
沒有偷偷恢復年度 reset。最終 immutable source 已綁定本輪四組 result 的 SHA；
遠端35/35 generated configs 及各自的 calendar context 均通過 dry-run。
baseline、d128 都是fold11；lookback256按自己的觀測日期選最後fold10，沒有縮短視窗。

原指令現在預設讀取 v9 配方：

```bash
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh
```

正式結果的新 root 是
`artifacts/markets/tw_scale_cash_annual_last_fold_structural_20261014_v9/`；
本輪未執行這個正式長跑。舊結果與 optimizer 狀態保持原樣，不跨新梯度 ABI resume。
綁定見
[structural source](../artifacts/operations/training_launches/tw_scale_cash_annual_last_fold_structural_20261014_v9/structural-source-v1.json)、
[dry-run](../artifacts/operations/gpu-structural-optimization-vastai1t-20261014/remote-structural-dry-run.txt)。

完整 benchmark 留在遠端 `artifacts/benchmarks/gpu_structural_20261014/`；本輪操作與
驗收 receipts 在 `artifacts/operations/gpu-structural-optimization-vastai1t-20261014/`。
輕量結果、完整 epoch curves、ownership、log 與驗收 receipts 另有本機 SHA 核對副本；
大型模型、PNG 與 source data 保持遠端原路徑。
原先 GPU 診斷見 [現場診斷](gpu_utilization_first_principles_2026-10-14.md)；
較早套件試驗見 [kernel inventory](frontier_kernel_optimization_2026-10-13.md)。

目前仍有年度數不整除兩卡的負載差、寬輸入 normalization、可變 column bucket、
Python 每日調度與無法消除的帳務 recurrence。whole-account 分工沒有證明全域計算
最優，也沒有證明策略品質改善；已證明的是這個固定工作量上的工程正確性與加速。
