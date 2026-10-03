# Fold 14 保證金期貨訓練吞吐量驗證

## 合約與量測範圍

以 `tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_gradient_v3.yaml`
為控制：兩張 RTX 5090、BF16 AMP、FP32 特徵、2011–2024 共 3,430 筆有效訓練日期，
2,754 檔股票脈絡、1,936 個固定期貨槽位，驗證 2025、測試 2026。
保留 1 億元保證金帳戶、TX/MTX 的驗證資料、整口執行、費稅、到期結算、吸收式違約、
log utility 與 margin backward contract v3。基準仍為大台近月連續轉倉的 1x gross 報酬。

每個候選都由原有 `train.py` 啟動雙 GPU DDP，跑 5 個完整 epoch，保留每輪驗證、
測試曲線、checkpoint 與最終完整 fold 報表。以第 3–5 輪最慢 rank 耗時的中位數比較，
分母使用有效日期數，不把 padding 算作吞吐量。啟動、編譯、最後圖表等一次性工作另記。
這是工程測速，不是正式 1,000 epoch 的策略績效結論。

原始證據均在 `artifacts/benchmarks/tw_futures_margin_fold14_20260928/`。
每個 `batch_*/result.json` 包含指令、計時、顯存抽樣與完整生命週期驗收。

## 從計算依賴推導改善位置

每輪成本包含資料準備、模型、逐日帳本、反向傳播、兩個 rank 的驗證／測試，以及儲存工作。
帳本的第 t 日必須使用第 t−1 日的實際部位、NAV 與存活狀態；日期不能直接當成獨立樣本平行化。
DDP 分散模型的日期批次，兩張卡仍各自運算同一份完整帳本。

1. CPU 來源反覆搬運重疊的 lookback 視窗，而兩張卡有足夠顯存。
   使用既有 GPU shared-base cache，train/val/test 共用約 5.74 GB 的 FP32 基礎資料。
2. 每張卡已持有相同的不可變市場資料。使用既有 local-global-metadata 路徑，
   僅對模型產生的 actions 做可微分 all-gather，避免重送已知報酬、遮罩與執行資料。
3. loss 不讀取保證金報表的 12 個診斷欄位。新增預設開啟的報表輸出選項，
   僅在訓練 loss 關閉；資金限制、強制平倉、違約判定和所有遞迴狀態照常計算。
   同一天的部位限制 metadata 也直接重用，不重建相同的 group scatter。
4. 較大的 batch 攤平模型、autograd 與排程成本，但延長截斷反向傳播的時間範圍。
   它是已授權的訓練超參數變更，必須使用新 artifact root，不能冒充梯度完全相同的執行優化。
5. 時間基底已使用代數化投影。測速器改為共用既有內容定址、依訓練日期驗證的 transform cache，
   避免每個新測速目錄重算約 81 秒的相同基底；這項只縮短重複測速的啟動成本。

## 正確性與重現性

新增每輪最慢 rank 計時，保留原有各階段本地計時，避免把不同 rank 的階段最大值相加。
測速器現在驗收最終 fold 生命週期、梯度有效性、編譯穩定性、CUDA graph fallback 與顯存餘裕，
並使用 `PYTORCH_ALLOC_CONF`，移除棄用的配置名稱。

保證金帳本與修改前的程式做了 32 組對照，所有帳戶欄位及梯度逐位相同。
CPU/GPU 測試涵蓋禁止買賣、到期、部位限制、no-advance padding、history 與 recovery 開關。
正式評估與 NPZ 的保證金報表仍由原有測試驗證。

擴大測試重現了捕捉期間重設 CUDA graph 的失敗。循環 GC 可能在新 capture 中回收舊 callable
所持有的 graph；現在在 capture 前回收、捕捉期間暫停循環 GC，並於成功或例外後恢復呼叫端狀態。
修正後完整 CUDA graph／診斷／GC 組合 42 項測試通過，另有 301 項 loss／回測測試通過。

未開決定性演算法的兩次 batch 32 控制，各輪指標與 71 個 NPZ 陣列完全一致，
但少數 attention 參數有最高約 7.45e−8 的差異。因此 checkpoint 逐位一致的驗收另用
相同 batch 的決定性對照；不將這種非決定性計算描述成 checkpoint 位元一致。

`compare_artifacts.py` 可重做模型、optimizer、scheduler、RNG、每輪指標與 NPZ 對照；
只排除記錄不同路徑／執行選項的 experiment manifest，不排除訓練狀態。

決定性 batch 32 控制與 GPU cache + local metadata 候選均完成 5 輪，
733 個 tensor（含模型、optimizer、scheduler/RNG 等保存狀態）、71 個 NPZ 陣列、
所有選定每輪指標及 summary 都逐位一致。證據：`deterministic_parity.json`。
決定性模式在本機較慢，只用於正確性驗收，正式 v4 保留原先的非決定性設定。

## 測速結果

同步 profiling，第 3–5 輪中位數：

| 設定 | Global batch | 完整 epoch 秒 | 有效日期／秒 | 顯存峰值 GiB |
|---|---:|---:|---:|---:|
| CPU staging 控制 | 32 | 9.134 | 375.5 | 4.61 |
| GPU shared-base cache | 32 | 7.386 | 464.4 | 10.33 |
| GPU shared-base cache | 64 | 6.017 | 570.1 | 11.33 |
| GPU shared-base cache | 128 | 5.777 | 593.7 | 14.62 |
| Cache + local metadata + 報表省略 | 128 | 5.541 | 619.1 | 14.53 |
| 同上 | 256 | 5.679 | 604.0 | 21.09 |

顯存為 0.25 秒一次 `nvidia-smi` 抽樣的觀測峰值；不是框架逐配置追蹤的瞬間峰值。
batch 32/64/128 均補齊到 3,456 列，padding 約 0.75%；256 補齊到 3,584 列，padding 約 4.30%。
256 沒有額外的穩定吞吐量收益，還增加約 6.6 GiB 顯存，因此選 128。
驗證和測試分別使用不同 rank，兩者時間不可相加當作完整 epoch。

保守的一般執行模式對照使用相同最新版引擎；控制組也包含報表省略與 GC 修正，
因此另列的吞吐量提升只反映設定調整。兩組都不啟用逐階段同步 profiling。

| 一般訓練模式 | Global batch | Epoch 秒 | Epoch MAD 秒 | 有效日期／秒 | 5 輪完整工作秒 |
|---|---:|---:|---:|---:|---:|
| v3 設定、最新版引擎 | 32 | 8.560 | 0.011 | 400.7 | 111.0 |
| v4 設定、最新版引擎 | 128 | 5.494 | 0.013 | 624.3 | 104.2 |

穩定 epoch 吞吐量提高 **55.80%**，每輪耗時減少 **35.82%**。
5 輪完整工作的差距較小，因為包含資料載入、CUDA capture 暖機和最後報表的固定成本；
不能把 55.80% 當成整個短測工作的加速比例。非同步曲線寫圖會在最終 flush 驗收，
不把不同 epoch 重疊的畫圖工作重複加回每輪耗時。
控制／候選觀測顯存峰值分別約 4.55／14.53 GiB，候選最少仍有約 17.32 GiB 餘裕。
兩組穩定 epoch 都沒有新 Dynamo graph、零梯度 optimizer step 或 CUDA graph fallback，
所有測速候選均通過完整 fold 生命週期檢查。

最終可機讀證據：`artifacts/benchmarks/tw_futures_margin_fold14_20260928/summary.json`，
內含每個候選結果路徑、上述分母及方向明確的加速比例，以及本次程式與 v4 YAML 的 SHA-256。
在 repository root 執行以下指令即可重建彙總：

```bash
source scripts/runtime_env.sh
run_fintech_python artifacts/benchmarks/tw_futures_margin_fold14_20260928/summarize.py
```

## 正式訓練

使用 `configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_throughput_v4.yaml`。
有效設定只改四欄：batch 32→128、train/eval GPU cache false→true、local metadata false→true。
eval batch 和模型 eval chunk 都維持 16，epochs=1000，early-stopping ratio=0.1。
資料、交易、評估、環境四個完整 dataclass 與 v3 相同；training checkpoint 合約只有 batch 欄位不同。

```bash
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_throughput_v4.yaml \
  --start-fold 14 --max-folds 1
```

新結果寫入 `artifacts/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_throughput_v4`。
舊 v2/v3 與所有測速根目錄保留。省略 fold 參數會按照 YAML 從 fold 1 開始。
本次只執行獨立目錄的 5-epoch 測速，沒有啟動正式 1,000 epoch 訓練。
