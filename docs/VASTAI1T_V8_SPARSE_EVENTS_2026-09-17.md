# vastai1T v8 稀疏事件訓練實作與驗收（2026-09-17）

## 範圍與結論

修改位於 vastai1T 的 `/root/stockAgent` 工作樹；本機 penguin 的同名訓練檔仍是較舊版本，不能用本機版本覆寫遠端。遠端工作樹原有其他未提交變更，以下只記錄本次增量，不能把整份 `git diff` 歸因於本次修改。沿用既有 `train.py`、物理 FIFO、來源快取、checkpoint 與雙卡 DDP，沒有另建訓練器。正式 v8 設定仍維持密集路徑；稀疏模式由獨立的 `configs/deployments/tw_day_trade_last_last_only_training_vastai1t_v8_sparse_profile_candidate.yaml` 選用，產物只寫到遠端 `artifacts/markets`。

遠端修改：

- `stockagent/data/tw_day_trade_carry_source.py`：從已驗證的 packed session 直接建立稀疏事件，省去 `[symbol,270,2]` 密集 exit 陣列的展開再壓縮；前分鐘資料時期的官方開／收日 K 代理也直接產生終場雙向事件。保留原本分鐘 mark、漲跌停、權益、公司行動和來源缺口欄位。
- `train.py`：只在設定啟用稀疏模式時，把事件槽上限顯式交給來源閘門。
- `stockagent/data/tw_day_trade_carry_source.py`：`--check-data-only` 即掃描完整已驗證 session 快取與日 K 代理，提前拒絕超過固定編譯槽數的資料；在來源 audit receipt 留下最大事件數和檔名。
- `test/test_day_trade_carry_source.py`、`test/test_tw_day_trade_inventory.py`：逐欄位檢查 packed 直接壓縮與舊路徑完全一致；檢查日 K 代理、空事件、重複事件拒絕、超限預檢和 2,755 檔規模的 FIFO 成交股數。

## 證據

固定資料來源為 `tw-public-20260917T022610857716059Z-l0-penguin-7e9a08bb7548fd88`，物理來源 release 為 `tw-day-trade-carry:781399f74803e6036fafd1d4794c14e58d6805bf5bc942c56fa0c79621501824`。嚴格 CUDA 環境檢查通過，兩張 RTX 5090 可用；`--check-data-only` 退出碼 0，資料 3,099 交易日、2,755 檔、99 特徵。容量預檢覆蓋 1,596 個分鐘快取；最大 189,939 事件（`session-2024-08-05.npz`），日 K 代理最大 3,850，均低於 262,144 槽。相關迴歸 175 passed、3 skipped，`git diff --check` 通過。

真實快取三日抽樣的 29 個 session 欄位與舊壓縮路徑逐位一致。單日壓縮中位時間：新 2.80／3.13／2.98 ms；舊密集 session 壓縮 12.82／14.60／10.02 ms；後者尚未計入前置密集展開，因此只代表此局部步驟。以真實 2,755 檔事件檢查 FIFO，成交股數逐位相同；盈虧最大單檔差約 TWD 1.3e-6，來自全域與逐檔 FP64 前綴加總順序。

最終程式以正式全域 batch 的 fold 11 跑完 4 epoch、雙卡 DDP、完整驗證／測試／報表。第 3／4 epoch 較慢 rank 為 75.442／72.262 秒；同設定先前密集版一次量測為 85.299／76.957 秒。跨次變異存在，不能把單次差異當成有保證的加速。最終稀疏產物：

`/root/stockAgent/artifacts/markets/tw_day_trade_v8_sparse_final_ddp_20260917`

`progress.json` 與 `fold_11/fold_complete.json` 均為 complete。最終及改動前一輪稀疏測試的 `model.pt`、`metrics.json`、`test_backtest.npz` SHA-256 各自完全一致；模型 SHA-256 為 `789e6f753bec114b60ee3caa1c49138951d0d730fe929e77b26ceb2f3242638f`，2026 測試累計報酬 36.9757959523%。密集版舊量測為 36.7711895345%，故稀疏路徑仍不是密集版的逐位等價加速；應視為獨立消融候選，不能續接密集版 optimizer，也不應自動取代正式 v8。此四 epoch 驗收不是全期正式訓練或跨折消融結果。

## 在 vastai1T 自行啟動完整稀疏候選

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t_v8_sparse_profile_candidate.yaml \
  --check-data-only
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t_v8_sparse_profile_candidate.yaml
```

僅在前兩道閘門退出碼皆為 0 時啟動最後一行。Vast 容器工作樹不是持久 Git 發布；保留這些遠端變更需走既有 Git 整合流程，不能把本機較舊的檔案直接覆蓋上去。

## 注意力時間查詢候選（同日追加）

使用者另要求啟用 attention full then last。遠端已新增
`configs/deployments/tw_day_trade_last_last_only_training_vastai1t_v8_sparse_attention_full_then_last.yaml`，
繼承上述 sparse 設定，只把**實際啟用**的 `financial_transformer` 改為
`temporal_pooling: attention`、`temporal_query_mode: full_then_last`；
`attention_mode: market_token` 不變。解析設定時 `executable_portfolio_transformer`
會同步繼承這兩欄，但本實驗的 `model_name` 仍是 `financial_transformer`。
這不是跨股票的全量 `attention_mode: full`。新產物寫到獨立的
`/root/stockAgent/artifacts/markets/tw_day_trade_v8_sparse_attention_full_then_last_v1`，
不得續接正在執行的 last/last_only optimizer。

2026-09-17 遠端解析檢查證實：相對原 sparse 候選，除實驗名稱、輸出目錄和
上述兩項時間注意力欄位（含設定繼承鏡像）外，其餘解析設定完全相同；
仍為 `financial_transformer`、稀疏事件 262,144 槽、雙卡 DDP，
`attention_mode: market_token`。嚴格 CUDA 環境檢查退出碼 0、兩張 RTX 5090
可用；兩項 CPU 模型／編譯回歸通過（2 passed）。
直接用合成特徵建構完整 22-basis 模型會被既有 PCA/KLT 的「必須提供
訓練折 covariance override」閘門拒絕；這是正確拒絕，並非真實資料建模失敗。
隨後改用原 sparse fold 11 的 `temporal_basis_selection.json` 指紋，
從共享 transform cache 定位並驗證唯一相符的訓練折 PCA/KLT override；
以新設定建構完整 22-basis 模型，CPU 小型 panel-slab forward/backward
通過，輸出與輸入梯度皆為有限值。這只證明架構與折專屬 basis 可運作，
不等於新設定的完整 DDP epoch 驗收。
截至檢查時，原 sparse 候選的 fold 11 正在 epoch 43／1000；因此未干擾它、
未同時啟動新 GPU 作業，也未宣稱新架構已通過完整資料閘門或正式訓練。

原作業結束或明確停止後，在 vastai1T 執行：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict &&
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t_v8_sparse_attention_full_then_last.yaml \
  --start-fold 11 --max-folds 1 --check-data-only &&
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t_v8_sparse_attention_full_then_last.yaml \
  --start-fold 11 --max-folds 1
```

這個候選的前幾個 epoch 需重編譯與重測；不能沿用 last/last_only 的速度結論。
