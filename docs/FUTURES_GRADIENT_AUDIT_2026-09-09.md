# 08:45 股期訓練梯度診斷與修正（2026-09-09）

原訓練：`artifacts/markets/tw_stock_futures_day_trade_0845_from_20200323_v2_contract_quarantine_v1_vast5090`。
確認一個反向傳播缺陷：無法成交的分鐘仍參與 fractional quantity shadow 的損益梯度，
會把應該獲利的小額多單往錯誤方向推。已修正並保留原始整數 forward 帳務。
短程實驗證明梯度不再沿原路徑塌縮，但驗證仍選出空手模型；尚未證明策略有獲利能力。

## 原訓練實際發生的事

資料為 2020-03-23 至 2026-09-04、1,573 個日期、2,753 個股票、98 個特徵。
模型為 financial_transformer、626,132 個參數，全部可訓練；沒有凍結或預訓練轉移。
正式預算維持 1000 epochs，各組實際在 101–102 epochs 由既有 early stopping 結束。
因此完整 fold lifecycle 已完成，並不代表已經有有效學習或策略優勢。

| Fold | 訓練年末 | 實際 epochs | 最後 train loss | 最後平均梯度範數（裁切前） | 最佳模型測試成交口數非零格數 |
|---|---:|---:|---:|---:|---:|
| 1 | 2020 | 102 | 15.86625 | 0.0002700 | 10 |
| 2 | 2021 | 101 | 7.93313 | 0.0000358 | 0 |
| 3 | 2022 | 101 | 5.28875 | 0.0000728 | 0 |
| 4 | 2023 | 101 | 4.53321 | 0.0000914 | 0 |
| 5 | 2024 | 101 | 3.52583 | 0.0000692 | 0 |
| 6 | 2025 | 101 | 2.88477 | 0.0000106 | 0 |

各組最後 epoch 都在第一個 batch 的第一筆資料發生殘倉違約。
後續 batch 延續已失效帳戶，梯度為零；這是既有吸收式失敗契約的結果。
不同 fold 的 train loss 不可直接比大小：目前紀錄是各 batch loss 的算術平均，
第一批的固定失敗懲罰會被不同 batch 數稀釋。相同原因，失敗移到較短的尾批時，
epoch loss 反而可能明顯升高。它不等同固定政策、整段日期的終值績效。

Fold 1 測試在 **2025-04-10、3105、小型 QIF:202504** 留下 -1 口。
當天 08:46 有 13 口分鐘成交量、VWAP 81.5，但 13:20–13:30 沒有出場分鐘棒。
原曲線接近 -100% 的落點來自 `log(1e-7)` 執行失敗懲罰，不能解讀為真實平倉虧損。
缺乏出場流動性仍必須記錄殘倉並停止該帳戶；此次未新增事後排除條件。

Fold 6 的 validation 與 test 都使用 2026，沒有獨立測試年份。
這是現行 `require_future_test: false` 的報告範圍，不能視為第六份獨立 OOS 證據。

## 缺陷的最小推導

整數口數函數幾乎處處不可微，現有訓練刻意使用 straight-through shadow：

```text
counts = soft + stop_gradient(integer_counts - soft)
```

因此 forward 的 `remaining=0` 並不代表 `d remaining / d weight=0`。
原出場迴圈先把不可成交分鐘的 capacity 設為 0，再計算 `min(remaining, capacity)`。
PyTorch 對 `min(0, 0)` 的相等分支把梯度各分一半；即使 forward 沒成交，
這一分鐘仍拿到成交梯度。沒有有效價格時，`safe_price=0` 又把它變成虛構的價格損失。
真實有價格但容量為 0、或限價沒有穿越的分鐘，也不能產生這種成交梯度。

解析案例使用進場 100、出場 110、乘數 2000、每側費用 40、每側稅 4、
資金 1,000,000、權重 ±0.0001；不足一口，真實 forward 保持現金。

| 權重 | 原代理梯度 | 修正後代理梯度 | 解析值 |
|---|---:|---:|---:|
| +0.0001 | -0.93126154 | +0.09951621 | (20000 - 88) / 200088 |
| -0.0001 | -0.93041927 | +0.10039583 | (20000 + 88) / 200088 |

這是在驗證約定的 fractional surrogate，沒有把不可微整數目標說成平滑函數。
修正只在「來源有效、方向符合限價穿越、且容量大於零」時傳遞成交量梯度：

```python
filled = torch.where(
    observed & (capacity > 0),
    torch.minimum(remaining, capacity),
    torch.zeros_like(remaining),
)
```

不能另外加 `remaining > 0` 遮罩；那會把尚未買足一口的正常學習訊號也切掉。
進場仍只受進場證據限制，沒有拿未來的出場量反過來篩選開倉。

## 對照與驗證

證據目錄：`artifacts/smoke/futures_gradient_audit_20260909/`。
`evidence.json` 包含原六組訓練摘要、兩次短程逐 epoch 指標、資料 fingerprint、
forward oracle 結果、解析梯度、原測試失敗分鐘證據，以及修正前後 executor SHA256。
`ledger_before.py` 保留修正前 executor，`collect_evidence.py` 可重新產生 JSON 與比較圖。

兩次短程都走原有 `train.py` lifecycle：同一完整資料、seed 42、全股票輸入、
同一 global batch 128、BF16、雙 RTX 5090、6 epochs、第一個 fold，包含驗證、
測試、checkpoint 與累積 walk-forward 圖。只在短程取消 early stopping。
兩次資料 fingerprint 完全一致：
`c47c3676d5b939431e998c0406ea55ae9cd94d0d36154611d4c446d96d382c3b`。

| Epoch | 原 train loss | 修正 train loss | 原梯度範數 | 修正梯度範數 | 原／修正訓練帳戶存活 |
|---|---:|---:|---:|---:|---|
| 1 | 15.86625 | 15.86625 | 20.7289 | 0.5366 | 否／否 |
| 2 | 0 | 0 | 100.9863 | 0.7063 | 是／是 |
| 3 | 15.86624 | -0.0005620 | 14.9375 | 0.6265 | 否／是 |
| 4 | 15.86611 | -0.0015873 | 3.4181 | 0.6906 | 否／是 |
| 5 | 15.86633 | 15.86606 | 0.6295 | 0.4616 | 否／否 |
| 6 | 15.86625 | 56.41224 | 0.0002589 | 1.9908 | 否／否 |

第 6 epoch 修正後仍違約，但首次失敗從 row 0 移至 row 151，且沒有全零梯度 batch。
較大的 loss 包含尾批較短造成的平均尺度差異，不能藏掉或解讀為策略已改善。
修正後最佳 validation loss=0，選出的 checkpoint 測試仍是零成交、零報酬。
6 epochs 只能證明工程路徑與梯度改變，不能證明收斂或投資績效。
兩次都完成原要求的 fold 計算與報表；測試期間有 CPU 單元測試並行，時間不作純速度比較。

驗證包含：

- 修正前 8 個解析方向測試失敗，修正後通過；涵蓋多空、缺棒、零容量、延後出場。
- 無法退出時保留全部殘倉與失敗梯度；無進場容量時保持現金且無梯度。
- 512 個獨立單日案例、128 個跨日帳戶案例的所有回傳欄位 bit-exact 一致，
  包含標準／小型、正負權重、零口數、容量上限、缺棒、daily hybrid 與不推進日期。
- CUDA compiled/eager forward 與梯度相符，梯度有限且非零。
- 來源與 CUDA strict 檢查成功；`source_lease.json` 記錄精確 tw-public release 的使用結果。
- 分鐘／日資料、整數帳戶、quarantine、checkpoint、tensor consistency 共 347 個不同測試通過；
  分組結果見 `tests_focused.txt`、`tests_contract.txt`、`tests_gradient_cuda.txt`。

![六個 epoch 的 loss、梯度與帳戶存活比較](../artifacts/smoke/futures_gradient_audit_20260909/gradient_comparison.png)

## 使用修正版

新設定繼承既有 historical YAML，只指定新名稱與新 artifact root。
保留 1000 epochs、既有訓練器、全部模型設定、資料、98 特徵、交易時鐘、成本、
整數口數、每分鐘 50% 容量與唯一的 LVF:202107 / 2021-06-21 quarantine。
新梯度契約為 `exact_integer_forward_executable_minute_quantity_shadow_v2`；
相容性測試確認舊 optimizer/checkpoint 不能直接 resume，新版相容 checkpoint 可以續跑。

```bash
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_gradient_v2.yaml
```

正式輸出：
`artifacts/markets/tw_stock_futures_day_trade_0845_from_20200323_v2_contract_quarantine_v1_gradient_v2_vast5090`。
此修正未啟動正式 1000-epoch 訓練；原實驗、來源資料與 checkpoint 均保留。
後續正式結果仍需同時看驗證與獨立測試的成交、殘倉、存活狀態與績效，不能只看 loss 或 IC。
