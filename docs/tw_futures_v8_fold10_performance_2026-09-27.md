# v5 期貨三模式：fold 10 計算加速

本次沿用原本 `train.py`、v5 骨幹、整口帳本與訓練生命週期，未新增 trainer。
一般模式在 vastai1T 的雙 RTX 5090 完成六個完整 epoch、驗證、測試、checkpoint、
曲線及最終報表；確定性對照的穩態 epoch 從 **29.432 秒降至 5.595 秒，5.26 倍**。
沿用 v5 原本非確定性運算設定時，最終版本為 **3.778 秒／epoch**。
這是目前實測且驗證通過的最快版本，尚不能宣稱已證明硬體的物理計算極限。

所有測速產物位於 `artifacts/markets/tw_futures_v8_fold10_speed_20260927/`。
完整模型與回測留在遠端；本機 `remote_evidence/` 保存相對應的紀錄與收據。
正式策略輸出仍是 `artifacts/markets/tw_futures_v8_{general,intraday,margin}/`。

## 固定的比較條件

- 兩張 RTX 5090，每張 32 GiB；PyTorch 2.11.0+cu128；BF16 AMP。
- global batch 32、eval batch 16；1936 個期貨 slot、2754 檔股票背景、99 個特徵、22 種時間基底。
- fold 10：訓練 2015–2024、驗證 2025、測試 2026；每 epoch 77 個訓練 batch。
- 每條完整軌跡更新一次，保留逐日持倉狀態、整口成交、佣金、稅、容量、違約及恢復梯度。
- 六個 epoch；epoch 1 視為暖機，表格使用 epoch 3–6 **各 epoch 最慢 rank 的牆鐘時間中位數**。
- 每 epoch 保留驗證與測試曲線；fold 結束仍完成全部報表與既有 18 項產物契約檢查。
  表格的 epoch 時間不代表程序啟動、編譯及 fold 結束報表的總時間。

股票背景 release：`tw-public-20260913T030921278338896Z-l0-penguin-248d0869d7d53be4`。
期貨日資料 SHA-256：`70a57dd76de74fd0a3652b1ecf932ccaf32320d01c252bf7b608a30b3f598df5`。
資料仍截至 2026-09-04；原本缺官方結算價的 3597 個實體合約隔離不變。

## 實測結果與第一性原理

| 確定性對照 | epoch 秒 | 對原始加速 | 整體數值驗證 |
|---|---:|---:|---|
| 原始帳本、host panel | 29.432 | 1.00× | 對照 |
| 相同帳本使用 CUDA Graph | 8.083 | 3.64× | 完全一致 |
| 再使用既有 GPU panel cache | 6.857 | 4.29× | 完全一致 |
| 再融合資金選擇、保留原生加總 | **5.595** | **5.26×** | 完全一致 |

原始瓶頸是大量很小的、具有日期相依性的帳務運算，模型 forward 並非主因。
確定性對照每 batch 的 loss 從 257.03 ms 降至 27.06 ms，backward 從 65.45 ms 降至
24.16 ms，transfer 從 19.02 ms 降至 0.39 ms。三個改動依序處理啟動開銷、重複傳輸及
重複的資金候選運算，沒有靠縮小資料或改變訓練批次取得加速。

1. `futures_cuda_graph.py` 只捕捉既有日頻／分鐘執行器的 CUDA 呼叫。
   沿用原本會計及反向計算，按形狀快取；複製輸出避免後續 replay 覆蓋持倉狀態。
   未完成或保留的 backward 會保護既有緩衝區；此類 eager 呼叫有獨立計數。
2. 一般模式開啟既有 `cache_train_tensors_on_gpu` / `cache_eval_tensors_on_gpu`。
   最終原設定實測每卡峰值 7488 MiB，確定性版本最高 8332 MiB，均保有充足餘量。
   當沖／保證金未完成真實全資料 VRAM 驗收，維持 host panel。
3. 只編譯資金候選選擇函式，保留 FP32 中間結果、除法與次正規值語義。
   整個循序帳本的 `backtest_compile` 仍關閉。

單純融合資金選擇曾在真實 fold 通過，但在第 10 組資金臨界值測試失敗：
編譯器改變了全帳戶加總順序。該候選 **未採用**。最終版本將這些加總保留為原生 ATen
運算，再通過 256 組隨機、相同分數及 `nextafter` 資金邊界測試。
失敗紀錄保留於 `funding_boundary_parity.log`，修正版收據為 `funding_boundary_parity.json`。

原本 v5 的 `deterministic_algorithms: false` 沒有更改。該設定下最終版本穩態 3.778 秒，
首 epoch 7.593 秒，六 epoch 整個程序含初始化及報表共 116.955 秒。
最初非確定性基線 rank 0 為 22.222 秒／epoch，但未記錄最慢 rank，且跨 run 本來就不是
逐位元可重現；因此正式加速倍率使用上表的確定性配對結果，不混用兩套設定。

最終正常訓練共有 2 個 ledger CUDA Graph，六 epoch 共呼叫 469 次，busy eager 回退及
eviction 均為 0；epoch 2 起沒有新增 Dynamo graph。
這不表示資料讀取、報表或整個訓練程序都經過編譯。

## 三個模式的驗收邊界

| 模式 | 合成帳本 forward + backward，原始 → 加速 | 真實歷史 fold 狀態 |
|---|---:|---|
| 一般留倉 | 325.93 → 30.04 ms，10.85× | fold 10 六 epoch 完整驗證 |
| 保證金留倉 | 450.08 → 43.70 ms，10.30× | 缺 PIT 歷史保證金及交易規則 |
| 當沖 | 1675.73 → 227.55 ms，7.36× | 缺 50,149 個合約交易日分鐘資料 |

此表使用單 GPU、合成 T=32 / S=1936 輸入、9 次呼叫之後 7 次中位數；
所有輸出帳戶欄位及梯度均逐位元一致。**這些數字不是三種模式的真實完整 epoch 加速比**。
當沖資料起於 2020-03-23，只有 5 folds；對應驗證 2025／測試 2026 的是 fold 5。
不能捏造當沖 fold 10、以日 OHLC 補分鐘資料，或把現今保證金回填成歷史規則。

GPU 測試共 29 passed，涵蓋三模式、正反向、歷史輸出、連續改變 action、保留 backward、
帳戶狀態續接與資金臨界值。本機相關回歸測試 321 passed、31 skipped
（28 項需明確啟動 CUDA 驗收，另外 3 項沿用測試的既有跳過條件）。
新檔 Ruff、Python 編譯及差異空白檢查通過。

確定性六 epoch 的 train/val/test、梯度 norm、optimizer 次數、最後模型、optimizer、
scheduler、GradScaler、RNG 與兩份完整回測 NPZ 全部一致。
另外從原始 eager 的第 6 epoch checkpoint 複本切換到加速設定，成功接續第 7 epoch：
前 6 行曲線原封保留，optimizer 與 scheduler 步數皆從 6 到 7，梯度非零，18 項產物檢查通過。
再以 resume 執行已完成 fold，原本曲線 SHA-256 不變，未重新訓練。
最佳 checkpoint 在這些短測仍是現金基準；這次驗證的是計算與續訓，並非策略獲利能力。

主要證據：

- `remote_evidence/audit_deterministic_eager__deterministic_graph__deterministic_graph_cache__deterministic_graph_cache_fused_native.json`
- `remote_evidence/audit_graph_cache_fused_native.json`
- `remote_evidence/kernel_final_native_measurements.json`
- `remote_evidence/final_cuda_acceptance.log`
- `remote_evidence/resume_acceptance.json`
- `remote_evidence/completed_skip_acceptance.json`
- 各 run 的 `run_manifest.json`、`measurement.json`、`gpu_samples.csv` 與 `train_*/epoch_curve.jsonl`。

## 遠端訓練與重測

在遠端主專案執行，既有入口會使用設定中的雙 GPU DDP：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
```

新的 fold 10 正式實驗，使用獨立產物目錄；再次執行會沿用既有續訓契約：

```bash
run_fintech_python train.py --config configs/markets/tw_futures_v8_general.yaml \
  --start-fold 10 --max-folds 1 --profile-timing \
  --output-dir artifacts/markets/tw_futures_v8_general_fast_fold10
```

重現六 epoch 測速時另加 `--epochs 6 --early-stopping-no-improve-ratio 0 --no-resume`，
並指定一個新的 `artifacts/markets/` 輸出目錄，避免覆寫既有實驗。
確定性嚴格比較可直接使用測速目錄中的 `deterministic_eager.yaml` 及
`deterministic_graph_cache_fused.yaml`；這些設定繼承測速前的凍結設定，不受正式設定更新影響。

當沖與保證金先用既有入口檢查真實資料，缺口未補齊前不會啟動正式訓練：

```bash
run_fintech_python train.py --config configs/markets/tw_futures_v8_intraday.yaml \
  --start-fold 5 --max-folds 1 --check-data-only
run_fintech_python train.py --config configs/markets/tw_futures_v8_margin.yaml \
  --start-fold 10 --max-folds 1 --check-data-only
```

資料通過後移除 `--check-data-only`，加 `--profile-timing` 即可測對應 fold。
一次只使用一組雙 GPU 工作。三模式已設定共用 CUDA Graph；保證金另繼承資金選擇融合，
當沖明確關閉無關的日頻資金融合。

若要還原執行路徑，可在繼承原市場設定的 YAML 中將 `futures_cuda_graph`、
`futures_funding_compile`、`cache_train_tensors_on_gpu`、`cache_eval_tensors_on_gpu` 全設為 false。
這些都是執行設定，不改語義 checkpoint 契約；更換資料、精度、batch 或帳務規則則仍受原契約檢查。
