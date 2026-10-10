# 完整特徵與 scale-separated cash 訓練入口核對

## 1. 執行進度

已核實 Vast 原啟動實際使用 14,726 個輸入通道，並已選擇
`score_entmax_scale_separated_cash`；不是只訓練日誌列出的六欄。
已修正接入後的摘要、補上所選入口的完整 manifest 檢查，保留現有實驗設定。
遠端 45 項相關測試、嚴格雙卡 CUDA 環境、入口 `--check-only` 與實際
`--check-data-only` 均通過。可以直接執行第 4 節指令。
本次只做準備與驗收，不啟動正式長跑；既有產物與使用者先前的啟動紀錄保留。

## 2. 根因與實際輸入

`build_panel` 先載入價格／交易規則基礎快取，此時列印：

```text
[panel] features (6): open_raw, high_raw, low_raw, close_raw, trading_volume_raw, next_session_open_gap_logret
```

接著 `train.py::_build_panel_rank_coordinated` 才呼叫
`attach_factorized_features`，接入設定指定的完整研究特徵。
不應把 `feature_include` 六欄改成所有特徵名稱，否則會破壞基礎 panel 的
ABI 與外接 manifest 的對齊，而不是解決實際缺漏。

| 通道來源 | 數量 |
| --- | ---: |
| 基礎行情／目標欄 | 6 |
| 個股特徵 | 14,655 |
| 全股票共用特徵 | 65 |
| 實際模型輸入 | 14,726 |

遠端解析後 `data.factorized_feature_manifest` 指向
`artifacts/markets/tw_day_trade_factorized_values_20261005_gaprepair_v4/prepared/factorized_manifest.json`。
其 contract 為 `tw_factorized_nullable_panel_values_lag1_v2`、狀態 `complete`、
政策 `value_only`；3,109 個日期、2,757 個目標，2014-01-06 至 2026-10-02。
這是原實驗已選定的資料，不把 10/06 當成已下載完成的資料日。

用戶既有雙卡啟動 `run_id=1791288786-2157453` 的
`training-bf16/startup_timing.jsonl`，階段 `panel_build_or_cache_load` 記錄
`rows=3109, symbols=2757, features=14726, world_size=2`。
這證明輸入接線，不等於該次訓練完成。

本資料仍為 `research_only=true, historical_point_in_time=false`；「全部通道接入」
不表示每個日期／商品的每欄都非 NULL，也不表示已證明歷史公布時間。
沿用原始 NULL、最近可用值與既定數值輸入處理；不另增加可用性、年齡或更新旗標。

## 3. 修正範圍與驗收

`stockagent/data/factorized_panel.py` 僅在來源 SHA、日期、目標、欄位 ABI
及完整區塊對齊成功後輸出實際模型通道數，例如：

```text
[panel-model] features (14726): base=6, individual=14655, common=65; factorized attachment complete, transfer=compact_cuda_cached, research_only=True, historical_point_in_time=False
```

摘要不解壓整張 panel、不讀取額外資料區塊、不輸出一萬多個名稱。
入口前置摘要使用 `expected model input`，與成功接入證據明確區分。
缺 manifest、schema 未完成、只剩六欄或通道數／基礎 ABI 不符會在啟動前失敗；
共用 attach 的來源／對齊錯誤仍向上傳遞，不降級只用六欄。

保留 batch128、eval batch16、fold11、雙卡 DDP、無基底／SVD／稀疏事件、
BF16＋TF32、既定 FP32 敏感區塊、交易規則、評估／曲線／checkpoint。
不改資料、不覆寫舊凍結 release；更新程式使用新 code-release 收據。

本機驗收：adapter／入口／設定 45 項通過；既有新 head／checkpoint ABI 9 項通過。
`bash -n` 與 `git diff --check` 通過。
遠端同套 adapter／入口測試修正後 45 項通過。本機另外完成
既有 batch128 前景入口與修正的 RMS parity 檢查：13 項通過（其中 RMS
與上述 45 項重疊）。沒有把重複測試加總成額外覆蓋。

遠端 canonical `--check-data-only` 退出碼 0，實際成功摘要：

```text
[panel-model] features (14726): base=6, individual=14655, common=65; factorized attachment complete, transfer=compact_cuda_cached, research_only=True, historical_point_in_time=False
[data preflight] accepted exact configured sources; panel_sessions=3109 panel_symbols=2757 panel_features=14726 folds=1
```

預檢 startup 累計約 30.073 秒，其中 panel／physical source 約 27.553 秒。
此為單程序、無 model／optimizer 的資料驗收，不拿它當成雙卡訓練吞吐量。
physical cache 經 `full_sha256` 驗證，manifest SHA
`8daeb8d2b419b1e832ccc0a5b4c416c68ce9e2c2ecae06fb16146238d3b5078e`。
沿用已批准的日 K 代理與未解企業行動排除規則，沒有宣稱所有股票每個時間點
都有完整實際分鐘行情。

新 source SHA：
`14e94f045528db6e9d1447ca66569e4b1f7bbaedea1f3924e7042fc8d287c07f`。
相較原 `620953576610` release，只有
`scripts/run_tw_no_basis_scale_separated_cash_b128_vastai1t.sh` 與
`stockagent/data/factorized_panel.py` 兩個 bundled source 檔改變。
全部配置 SHA 未變，沒有變更模型／optimizer 軌跡、資料或交易會計。
code-release 新建於實驗 root 的
`code-release/20261006T122817687210Z-14e94f045528/`，舊 release 留存。

Vast 驗收證據在同一實驗 root 的 `verification/full-features-20261006/`：
`source-update.json`、`run-spec-before-source-summary.json`、
`data-check.log`、`data-check/startup_timing.jsonl`、
`training-ready-full-features.json`。

遠端原 RMS／window parity 測試的 CPU backward 逐位相等斷言曾失敗。
診斷時輸入逐位相等、112 個 CPU 執行緒，loss 絕對誤差
`3.0517578125e-05`，梯度最大絕對誤差 `6.103515625e-05`、
最大相對誤差 `1.6297107663376664e-07`。
測試加入固定 seed，對模型 loss／gradient 採 `rtol=2e-6, atol=2e-6`；
輸入及 RMS 的逐位相等檢查保留。沒有改 trainer 或財務 reductions。

## 4. Vast 前景訓練指令

```bash
cd /root/stockAgent
bash scripts/run_tw_no_basis_scale_separated_cash_b128_vastai1t.sh
```

固定使用
`configs/deployments/tw_day_trade_factorized_values_20261006_no_basis_scale_separated_cash_b128_v1.yaml`；
產物留在遠端
`artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_scale_separated_cash_v1/training-bf16`。

第一次從頭訓練；再次執行預設 canonical resume，只續本 root 的相容 checkpoint。
不沿用不同 head 的舊模型或 optimizer；完成 fold 不重新訓練。
前景執行、只選 fold11，沒有 profiling，也不開其他正式訓練程序。

僅驗入口、來源身分、CUDA 與既有資料 pin（不訓練）可加 `--check-only`。
完整實際 panel 驗收則用 canonical `train.py --check-data-only`；它不啟動
model／optimizer，不能當成完整雙卡 epoch／fold 成功的替代證明。
