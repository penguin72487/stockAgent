# 加速配方套用至年度消融，2026-10-13

2026-10-14 使用者追加 optimizer 消融後，預設入口已切換成
[v8：baseline、七個 optimizer、原27項 OFAT](frontier_training_techniques_2026-10-14.md#正式消融baseline-後加入-optimizer)。
原 v7 的訓練已停止，checkpoints 保留；以下是 v7 的歷史實作與量測範圍。

目前正式入口：

```bash
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh
```

v7 新增 baseline，接著依原順序執行 v6 的 27 個 OFAT。全部使用同一份
[已實測配方](../configs/benchmarks/frontier_kernels_best_20261013.yaml)，各項只跑自身
最後一個 canonical fold；雙卡 DDP，獨立實驗並行數為 1。保留原本 1,000 epoch
上限、early stopping、完整 validation/test、曲線、圖、checkpoint 與結尾回測。
`--list` 僅列清冊；`--dry-run` 建立配置並驗證資料／日曆；`--only` 選取指定項目。

## 機制與比較邊界

| 處理 | 原理與不變量 |
| --- | --- |
| NUMA 綁定 | 每個 rank 的 CPU 提交與資料傳輸靠近所屬 GPU；沿用實測拓撲，未增加 CPU quota。 |
| funding preparation v3 | 中性容量 bucket 減少 shape 變動，編譯既有帳務 preparation；股數、fees、claims、funding、absorbing ruin 保留原算法。 |
| calendar cache | 重用唯讀日曆 metadata，保留 device、dtype、stream lifetime 和資料來源。 |
| BF16 temporal、manual attention、QuACK softmax | temporal 沿用已接受精度設定，softmax 使用同一配方；FP32 master/Adam、敏感金融 FP32/FP64 與窄 head 修復保留。數值與梯度差異可能改變後續 optimizer 軌跡，所以 v7 不接 v6 optimizer。 |

baseline 與 27 項固定加速參數一致，未修改研究維度：`embedding__d128` 仍為 d128，
`lookback__d256` 仍為 lookback256。寬度與歷史長度增加必要 GEMM／activation 工作，
不能用縮小這些維度來宣稱完成原消融。

目前完整資料的日曆驗收結果：baseline 與 26 項消融的最後 fold 為 **11**，
lookback256 的最後 fold 為 **10**。後者保留原本 lookback-shifted 年度所有權與上下文；
不把相同 fold 編號當成相同期間。報表保留各項實際日期範圍，使用 absolute 比較。
歷史 calendar fold10 的 71.56 秒不是本次 shifted 最後 fold 的速度保證。

## OOM、失敗與恢復

其他失敗寫入 summary 後繼續下一項，整輪有失敗仍回傳非零碼。關閉盲目 auto-resume，
相容的正式 checkpoint 恢復仍由 canonical runner 的 `--resume` 擁有。

只有完整 worker log 證明 CUDA OOM，才在同一研究項目重試容量設定：global train batch
256 → 128 → 64 → 32 → 16，同時縮小 eval batch／chunk 與有界傳輸 workspace。
最多四次容量重試；未發生 OOM 不改容量，其他類型失敗不藉縮 batch 隱藏。

每次容量版本各有新的 config、輸出與 optimizer。舊 log/checkpoint 保留在
`_capacity/<case>/attempt-XX/`，`selection.json` 記錄原 config SHA、調整值與 OOM log SHA；
公開 case 路徑指向當次選定輸出，因此 trainer、summary、繪圖與續跑讀到同一成果。
批次切分可能改變截斷邊界及梯度，報表須以實際容量版本解讀，不能接回先前 optimizer。

## 驗證與實際執行

配置為 [v7 catalog](../configs/ablations/tw_scale_cash_annual_last_fold_optimized_20261013_v7.yaml)。
正式輸出為 `artifacts/markets/tw_scale_cash_annual_last_fold_optimized_20261013_v7`；
binding／calendar receipts 在同名 `artifacts/operations/training_launches/` 目錄。
凍結訓練 source SHA256：`c830c7bb51b1fadace686a61741ee0d885369b80b84c6e08f3359f1bac2692ba`。
私有環境為 Torch 2.11.0+cu128、QuACK 0.6.5、CuTe DSL 4.8.0；啟動會驗證實際套件版本。

本次 299 項相關測試通過、1 項跳過，涵蓋舊入口、排程、全部研究配置、OOM-only
容量重試、保留 checkpoint、相容恢復、其他失敗繼續、kernel contract、gradient clipping、
精確 carry／checkpoint blocks、lifecycle、圖表與環境檢查。遠端 28 項 calendar gate 全數通過，
兩張 GPU 均通過實際初始化、allocation/kernel/synchronize 的 strict CUDA gate。

第一次受監督 run `formal-v7-20261013T080902-ea247493` 在訓練前被遠端舊 live checker
的環境名稱提示擋下，沒有 optimizer 更新。同步 canonical checker 已有的明確
`FINTECH_ENV_PATH` 驗證後，以 `formal-v7-20261013T081401-cec1f5b5` 重新啟動；原失敗紀錄保留。

測速使用正式 `epoch_curve.jsonl` 的 epoch3+ 最大 rank 完整 epoch 時間，寫入
`baseline-speed.json` 與各 case 的 `*-speed.json`。編譯暖機、來源核對、整輪 elapsed 和
穩態數值分列；沒有 MAX-rank 樣本就保持未知。baseline 完成後依序執行消融，
不把排隊、dry-run 或已通過工程測試描述成 28 項研究結果完成。

操作證據：`artifacts/operations/tw-scale-cash-optimized-ablations-20261013/`。
最新實測與執行進度由該目錄的 `delivery-status.json` 記錄。

截至 2026-10-13 16:31 台北時間，baseline 已完成 epoch1–9，正式訓練持續中。
epoch3–9 的完整 MAX-rank epoch 中位數為 **87.146 秒**，範圍 **73.926–90.385 秒**；
第一輪含暖機為 151.711 秒。epoch3–9 均沒有新增 Dynamo graph、沒有 zero-gradient
batch，裁切前梯度 norm 為 7.652–12.117，全部有限。所有較慢的 epoch 也納入。
當時兩卡使用約 22 GiB，尚未調容量。正式 checkpoint（14,179,549 bytes）與每輪
curve/timing PNG 已產生；原始曲線、manifest、progress 和 PNG 已獨立回讀到
`baseline-readback/`。這是正式訓練中的穩態觀測，整個 fold 與 27 項消融仍未完成。
