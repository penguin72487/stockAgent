# 期貨 Fold 1 起跑：DDP 特徵快取一致性修正

## 錯誤原因與修正範圍

使用者的記錄來自 `tw_futures_margin_repaired_v2/source/train.py`。
`1/14` 表示第一個訓練群組已完成，後續群組在正式 epoch 前準備特徵快取時失敗。
rank 0 選擇 FP32 CPU 特徵，rank 1 選擇 FP32 CUDA 特徵，舊程式將這個可選快取差異當成致命錯誤。
這段 traceback 沒有顯示 loss、交易結算或梯度計算失敗。

GPU 快取受每張 GPU 當下的可用顯存約束。rank 0 負責正式驗證、測試與產物，舊程式在群組結束時仍留有部分 windowed split、回測結果與模型 wrapper 的區域參照；另一個 rank 已先釋放部分狀態。因此跨群組的快取預算可能不同。
程式碼確認了這項參照生命週期缺口，但原始失敗沒有逐張 GPU 的完整記憶體紀錄，不能斷言它是唯一造成顯存差異的因素。

新版本直接使用既有 trainer、windowed batch API 與 GPU 快取預算檢查，新增共同決策：

1. 各 rank 保留原始 FP32 主記憶體 split，嘗試既有 GPU 快取。
2. 所有 rank 在相同 phase 同步快取錯誤，再比較特徵 dtype 與是否位於本 rank 的 GPU。
3. 若同一 dtype 的可選快取只有部分 rank 成功，所有 rank 回到各自原始主記憶體 split，釋放丟棄的 GPU 副本。
4. batch 仍透過原本 API 搬到本 rank GPU，模型與 DDP backward 繼續在 CUDA 執行。
5. 真正的資料／程式錯誤仍同步中止。明確要求 AMP 特徵儲存時，無法履行也會同步中止，不偷偷改回 FP32。
6. rank 0 完成正式產物後，清除本群組驗證／測試張量、回測結果、共享 split 與模型 wrapper 的參照；另一個 rank 補齊共享 split 與 probe wrapper 清理。

這次修正沒有增加策略限制，沒有改交易量、口數、保證金、費用、結算、目標函數、梯度 surrogate、模型初始化、batch 或學習率。沒有量測完整正式 epoch 吞吐量，因此不宣稱速度或策略績效提升。

## 可自行修改的設定

入口：`configs/markets/tw_futures_v8_margin_components_year_shift32_ddp_cache_capital100m_all_folds_v4.yaml`。
它繼承既有 v3，明確列出常用設定：

| 設定 | 生效值 |
| --- | --- |
| 起始 fold | 1，接續全部 14 folds |
| 初始資金 | 新台幣 100,000,000 |
| 最多 epochs | 1,000 |
| 早停 | 連續 100 次驗證未達既有改善門檻 |
| lookback | 32 個交易日 |
| 年度區間 | `[該年第 33 個交易日, 次年第 33 個交易日)` |
| 特徵 GPU 快取 | 開啟；無法一致放下時共同讀取主記憶體 |
| 驗證／測試 GPU 快取 | 保留原有開啟設定 |
| 特徵永久 AMP 儲存 | 關閉，保留 FP32 值 |
| 模型運算 | 原有雙 GPU DDP、BF16 AMP |
| profiling | 未開啟 |
| checkpoint resume | 新根目錄第一次執行關閉 |

v2 尚未包含年度 `+32` 切法；v4 納入已驗證的 v3 修正。
例如 Fold 14 的測試是 2026-02-26 至 2026-09-04；2026 年一月屬於前一年度區間。
跨多 folds 的完整報酬圖仍可能包含一月，因為它由上一個年度策略負責，不能直接刪掉真實連續交易日。

原 v2 與 v3 的設定、source、release 與訓練產物保留。
由於 v2 到 v4 改變年度資料歸屬，不能沿用 v2 的 optimizer/checkpoint 或完成標記。
新 v4 使用獨立產物根目錄，之後同一 v4 實驗若需要中斷續訓，可在相同指令加 `--resume`。

## 一般訓練指令

主工作樹仍有未完成的 Git merge，這次從已驗證的 v3 code release 建立獨立 v4 runtime，沒有修改衝突中的主程式或舊 runtime。
使用下面的 source 與配套 receipt，避免執行舊 v2 或混合工作樹程式：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
export STOCKAGENT_CODE_RELEASE_RECEIPT="$PWD/artifacts/runtime/tw_futures_margin_ddp_cache_v4/code-release/release.json"

run_fintech_python artifacts/runtime/tw_futures_margin_ddp_cache_v4/source/train.py \
  --config configs/markets/tw_futures_v8_margin_components_year_shift32_ddp_cache_capital100m_all_folds_v4.yaml \
  --start-fold 1 \
  --no-retrain-completed-folds
```

產物：`artifacts/markets/tw_futures_v8_margin_components_year_shift32_ddp_cache_capital100m_all_folds_v4`。
不加 `--max-folds`，讓 Fold 1 至 14 正常接續。
不需要在外層再加 `torchrun`；`train.py` 會依現有設定啟動 DDP。

設定 YAML 是供使用者調整的入口，receipt 固定此版本程式及建置時的設定快照。
若改模型／lookback／交易或資料契約，須使用新的產物根目錄，讓 checkpoint 相容性檢查正常保護舊結果。

## 工程驗證證據

證據目錄：`artifacts/analysis/tw_futures_ddp_cache_fold1_20261003`。

- `environment.json`：strict CUDA 環境檢查，兩張 RTX 5090。
- `ddp_rank0.json`、`ddp_rank1.json`：實際 NCCL 雙 GPU，連續七個快取／錯誤情境。
- `test/test_train_feature_cache_consensus.py`：可重跑的回歸測試；包括單一 rank 預算不足、單一 rank 快取 OOM、共同退回 host 後再次 GPU 快取成功、明確 AMP 儲存失敗，以及單一 rank 真正錯誤的共同中止。
- 快取成功與退回 host 的 batch 各欄逐位元相同；測試以小型 Linear DDP 模型接上正式 windowed batch API，forward 與 backward 梯度也逐位元相同。
- 238 項測試通過，涵蓋原有年度切片、報告、期貨保證金梯度、loss、windowed execution 與快取。初次擴大測試時獨立 runtime 缺少 15 項測試需要的 `experiment_baseline.yaml`；補齊測試 fixture 後，這 15 項全部通過，既有程式未因此修改。
- `code_verification.json`：凍結 source、wheel、source bundle SHA-256 驗證。
- `preflight.log` 與 `preflight/walkforward_period_boundaries.json`：只做完整資料與 Fold 1–14 邊界檢查，不啟動模型訓練。
- `engineering_acceptance.json`：完整驗收結果與證據路徑。

這些證據支持工程修正與資料切片正確性，尚不是 Fold 1–14 正式訓練完成或超越 benchmark 的策略績效證據。正式訓練留給使用者自行啟動。
