# 當沖完整推論效能優化與重測 2026 年 10 月 13 日

第五模式 Factorized 無基底 Scale Cash 的完整熱推論中位數已降至
571.19 毫秒，5 次測量介於 471.61～611.13 毫秒。保留完整
32 × 2,757 × 14,726 輸入、原 fold 11 checkpoint、RMS、BF16 AMP 與
FP32 敏感計算；全標的配置、決策及模型說明的輸出雜湊與原版完全相同。
正式 Discord consumer 已載入新版，五模式暖機成功，當沖引擎未重啟，帳本未修改。

此結果是完整離線訊號推論，不是 09:00 到取價、下單或券商成交的測量。

## 測量結果

同一 RTX 5070 Ti 16 GB、PyTorch 2.13.0、實際 16 個 CPU threads，依序執行
原版與新版，不與本次 GPU 回歸測試重疊。兩者使用相同設定及 2 GiB 來源快取
預算；原版保留 dense CPU window 與原本 borrowed-input 保護。
每個程序先執行一次首次呼叫，再執行五次熱推論，沒有使用 profiler。

| 項目 | 原版 | 新版 |
| --- | ---: | ---: |
| 首次呼叫 程序冷且既有 disk panel 命中 | 57.63 秒 | 5.70 秒 |
| 五次熱推論中位數 | 9.40 秒 | 0.571 秒 |
| 熱推論範圍 | 4.49～21.52 秒 | 0.472～0.611 秒 |
| GPU 峰值 allocated | 15.67 GB | 10.48 GB |
| 程序峰值 RSS | 7.49 GB | 3.14 GB |

GB 採十進位；GPU allocated 不含其他程序或驅動記憶體，RSS 不含 OS page cache。
測速期間一般服務仍運作。原版的巨大 GPU 工作集在本機具有明顯延遲波動；
這些數字是本次有界測量，不是跨硬體的固定加速倍率或長期 SLO。
首次呼叫只代表程序／模型快取冷，仍可使用持久 panel 和 OS page cache，
不能稱為 canonical source 完全失效後的來源重建。先前 profiled baseline
及重啟中斷的部分測量不混入上述比較。

原版、新版各六次推論的完整輸出 SHA256 均為：

```text
02d5810adeed6738c15b675970bae522a94d5dd2855d04cd8d20f9f7ee010f3a
```

Checkpoint 為原模型的 `training-bf16/fold_11/checkpoint_best.pt`，SHA256：

```text
eb27db1a15ac32ae1fc359d11290788f46e4872f98661eaff3c98949f2811150
```

## 根因與修改

原路徑先在 CPU 展開約 5.20 GB 的完整 FP32 window，再搬至 GPU；為保護共用
輸入，推論還會保留大型複本。原版樣本中，完整流程 7.44 秒，模型呼叫的同步
子區間約 0.63 秒，其餘模型階段還包含輸入搬運。只調整網頁或少量神經網路
運算，無法移除這項資料搬運與配置成本。

`FactorizedLiveWindow` 重用 canonical lossless codec，依 128 個標的分批搬運，
在 GPU 還原原本完整形狀後才執行原 Candle Encoder 和模型。正零省略只是
儲存格式，負零與所有非零資料仍保留。每次邏輯輸入為 5,196,746,496 bytes，
實際 host payload 為 546,794,024 bytes；模型看到的特徵和維度沒有減少。

GPU 還原輸入由本次呼叫獨占，故原 RMS 的 FP32 運算可原地執行，省去額外
大型保護複本。一般 borrowed Tensor、訓練、基底、SVD、bottleneck、window RMS
及不相容設定不會切換到這條路徑，仍保留原行為。

Live factorized attachment 原先沒有傳入設定中已有的 host cache 預算。
共用入口現在遵守 `factorized_host_cache_bytes`，且快取身分包含容量。
第五模式設定 2 GiB；本次實際工作集約 855.50 MB，首次解碼 44 個 blocks，
後續每次 44 次命中且沒有新增 miss。快取只保存驗證過的來源 blocks，
不跨請求保存模型轉換後的 GPU 輸入或訊號。

## 正確性與恢復

曾測試將 Candle 計算也切成小塊，但完整輸出比對失敗，故候選已撤回。
目前只切資料搬運，保留模型的原本完整形狀與計算順序。PyTorch 也說明
切片與整批計算不保證浮點 bitwise 相同；不能以數學等價取代實測驗收。
[PyTorch numerical accuracy](https://docs.pytorch.org/docs/2.14/notes/numerical_accuracy.html#batched-computations-or-slice-computations)

CPU／CUDA 測試逐 byte 比對完整輸入、權重及 aux，涵蓋負零、NaN sentinel、
日期重排、遮罩與開盤報價覆寫。報價覆寫保留缺價的零 gap 語意，
不製造開盤價、不污染共用來源；不相容模型在讀取 blocks 前即拒絕。
Checkpoint ABI、來源雜湊驗證、producer 讀鎖及交易規則未放寬。

20:39 WSL 重新啟動中斷兩個離線工作；原 run 保留 interrupted，核對 boot ID、
已消失的程序及 supervisor 後，重新 strict CUDA preflight 並序列重跑。
成功驗收包含 377 個較廣回歸測試及最終 110 個 focused 測試，兩組有重疊。
不宣稱全 repository 測試完成。

正式 consumer 本次初次暖機遇到官方來源更新，以 retryable waiting 拒絕混合版本。
來源更新完成後，既有自動重試於 21:01:46 開始，10.925 秒完成五模式暖機。
第五模式耗時 5.425 秒，checkpoint fingerprint 為 `3132bca00eb5`。
驗收確認新 bot run、Gateway 連線、22 個 commands 同步、引擎 revision lag 為 0，
且 signals／orders／fills 檔案 SHA256 不變。引擎原 run 繼續運作。
既有盤後 artifact maintenance 仍 degraded，與這次推論暖機驗收分開顯示。

## 重測與後續診斷

先執行 CUDA 驗收，重測原版和新版時不要同時執行其他 GPU 測試或訓練。
替換日期和輸出目錄可留下不同交易日的紀錄；比較必須使用相同來源、
panel date、checkpoint、CPU threads 與 runtime 精度。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python scripts/benchmark_live_inference.py \
  --factorized-stream 0 \
  --market-config services/discord_bot/markets/tw_day_trade_factorized_scale_separated_cash.yaml \
  --panel-date 2026-10-13 --repeats 5 \
  --output artifacts/benchmarks/live_inference_retest/control.json
run_fintech_python scripts/benchmark_live_inference.py \
  --factorized-stream 1 \
  --market-config services/discord_bot/markets/tw_day_trade_factorized_scale_separated_cash.yaml \
  --panel-date 2026-10-13 --repeats 5 \
  --compare-to artifacts/benchmarks/live_inference_retest/control.json \
  --output artifacts/benchmarks/live_inference_retest/candidate.json
```

Benchmark 禁止行情抓取、不發布訊號、不改帳本，完整輸出不一致會失敗，不能產生
成功收據。JSON 保存完整端到端、原本 live_latency 階段、非加總子區間、
輸入形狀、來源／設定身分、硬體、實際 threads、冷熱樣本及 GPU／RSS 峰值。
新增 live_latency 的 `factorized_input_transport` 保存真實搬運 bytes 和 cache
hit／miss。`input_reconstruct_enqueue_ms` 是 host 解碼／排程時間，不是獨立同步
GPU kernel 時間；模型階段在同步後量測，兩者不能直接相加。

每份收據位於 `artifacts/benchmarks/live_inference_20261013/`：
`control_unprofiled.json`、`candidate_unprofiled.json`、`consumer_reload.json`、
`consumer_acceptance.json`、`restart_recovery.json`。工程 task 為
`factorized-live-inference-performance-20261013`，原部署收據不覆寫。

回退可在原服務環境指定 `STOCKAGENT_LIVE_FACTORIZED_STREAM=0`，沿用原推論路徑；
不需更換 checkpoint 或帳本。下一個真實交易日仍須獨立確認
來源 quorum、開盤價取得、全部模式推論、紙上執行和券商成交的各自時鐘。
本次未以離線 571 毫秒承諾真實開盤成交延遲。
