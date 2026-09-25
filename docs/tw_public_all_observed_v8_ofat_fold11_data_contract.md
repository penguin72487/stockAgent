# 用新 428 欄研究資料重跑 v8 OFAT baseline：fold11

## 基準必須以實際配置判斷

指定的完整基準是遠端 `/root/stockAgent/artifacts/ablations/tw_day_trade_last_last_only_v8_twpublic_248d0869_ofat/baseline`，其已完成的 `fold_11/fold_complete.json` 為 `status=complete`。它當時的 `generated_config_20260920_014019.yaml` 已原樣保存為[基準快照](../configs/deployments/tw_day_trade_last_last_only_v8_twpublic_248d0869_ofat_baseline_snapshot.yaml)，SHA-256 `07d009d8c9dddb2b6d28f52e2c1193009d1fd112b3141eb898a30981dcdf534e`，manifest 配置指紋 `633eace1bbfd90f600facdaab9c2d1ab73cd7f06b9053846426f4302805ffc78`。遠端目前的同名 v8 部署檔與這份產物配置有 CPU 與資料根目錄差異，所以此研究以產物快照為準。

基準實際使用 `tw_day_trade` 分鐘執行與 TWD 1,000 萬成本／容量設定、BF16、兩卡 DDP、32 日 lookback、22 個時間基底家族、`score_entmax_cash`、global batch32、1000 epoch、learning rate `0.0001`、實體 FIFO loss、訓練前置模型與完整 fold lifecycle。已完成的 `tw_public_all_observed_window_rms_projection_l1_fold11_v1` 則使用 `naive`、`projection_l1`、Haar/DCT 兩個基底和另一組 loss／learning rate。兩者不能只因 fold 編號相同就稱為同一基準或同一交易假設；既有 v1 產物維持原樣。

## 資料配置與預訓練相容性

[新配置](../configs/deployments/tw_public_all_observed_v8_ofat_fold11_data_v1.yaml) 繼承上述基準快照。遠端 `load_config` 逐欄比較確認 `environment`、`walk_forward`、`trading`、`training` 都與基準完全相同；只更換 `runner.output_dir` 及以下九個資料欄位：

| 資料欄位 | 理由 |
|---|---|
| `parquet_root`、`tw_public_feature_path` | 固定覆蓋至 2026-09-17 的 tw-public release 與 all-observed v3 研究特徵 release；不在訓練途中解析 `latest` |
| `day_trade_physical_public_feature_path` | 交易側的漲跌停與其他實體規則讀取同一個新 tw-public release 的正式表，不把研究特徵表當成實體執行證據 |
| `feature_include`、`feature_availability_indicators` | 取 20 個股票數值、204 個公開數值和 204 個可用旗標的 v3 ABI |
| `feature_shift_next_session`、`tw_public_feature_cutoff` | 保留 v3 對 09:00 決策的隔日位移與 `preopen_0900` 可見時鐘 |
| `day_trade_open_feature` | 使用 v3 的既定 428 通道，而不另加舊基準的開盤缺口特徵 |
| `panel_cache_root` | 與完成產物隔離的衍生快取，不改原始釋出 |

原 baseline 的 minute execution release、費稅、整數張數、容量、T+2 與預訓練根目錄均繼承，沒有換成 `naive` 的代理回測。新的公開特徵接受現在修訂的歷史值、推測公告日和晚起始欄位，`historical_vintage_verified=false`；09:00 位移規則無法把來源變成已驗證的當年原始版本。保留 baseline 舊 minute release 也意味著其覆蓋範圍及允許日線代理的設定仍須在結果中揭露。

基準指定的預訓練來源有 99 個 feature name，其中 98 個與新 428 通道同名；來源的 `next_session_open_gap_logret` 不在新資料 ABI 中，另有 330 個新通道沒有同名預訓練欄位。來源已有學到的 24 維特徵瓶頸，因此現有 `identity_by_feature_name` 轉移只接受完全相同的原始特徵 ABI。使用者啟動 v1 後，兩個 DDP rank 在第一個 epoch 前都因 `adapter=24 source=99` 中止；v1 的 `progress.json` 記錄 `phase=failed`，沒有 checkpoint 或完成標記。原先推論「98 個同名欄位可以沿用預訓練」不成立。即使把缺失欄位補零，來源策略也無法保持原樣，且不能藉此將開盤缺口資訊引入 09:00 前可見的資料。

[可執行的 fresh v2 配置](../configs/deployments/tw_public_all_observed_v8_ofat_fold11_data_fresh_v2.yaml) 繼承 v1 的全部資料與其餘基準設定，僅把 `training.pretrained_initialization_root` 設為 `null`，並使用獨立的實驗名稱及產物目錄。這使 428 通道和 24 維瓶頸由頭初始化；沒有預訓練的 epoch-zero 守門不執行，其餘模型架構、loss、交易、optimizer、batch32、fold11 與時序基底設定仍為基準值。這是明示的初始化差異，不能稱 v2 與原 baseline 所有訓練欄位完全相同。

使用者啟動 fresh v2 後，fold11 的 epoch 1、2 各完成 83 個 train batch，且各有 1 次完整軌跡 optimizer step；epoch 2 的 group checkpoint 與 `epoch_curve.jsonl` 已落盤。第 3 個 epoch 第一個 batch 在 GPU 0 的編譯實體 FIFO `inventory_prefix_sum` 發生 CUDA OOM：31.36 GiB 中約 31.34 GiB 已占用，只剩 2.12 MiB。遠端檢查時兩卡均已釋放，沒有證據顯示另一個 GPU 作業占用。這次故障發生在訓練運算，不是前一版的預訓練 ABI 問題。前兩輪完成不證明第 3 輪或完整 fold 的顯存安全。

[2 的冪次 v3 配置](../configs/deployments/tw_public_all_observed_v8_ofat_fold11_data_pow2_v3.yaml) 保留 v2 的資料、可見時間、實體執行、loss、時序基底與從頭初始化；計算形狀改為兩卡全域 train batch `16`（每卡 `8`）、eval batch `8`、特徵瓶頸 `32`、CPU threads `64`、epoch 上限 `1024`。訓練 batch 減半是針對 FIFO 暫存張量及模型 activation 的顯存負荷；瓶頸 `24→32` 主要滿足計算形狀規則，不應宣稱是解決 FIFO OOM 的原因。資料本身的 428 特徵、2755 股票、fold11、270 個真實分鐘點及資金／費率／公告日期不是可任意改成 2 的冪次的計算設定。v3 使用新產物根目錄；v2 的 checkpoint 屬於不同 batch 與模型 ABI，不能移入 v3 接續。僅靠配置推估無法保證 v3 的 epoch 3 不再 OOM，需由正式執行和峰值顯存證明。

遠端已用 v2 的 fold11 基底權重作為**建模形狀檢查**，成功建立 428 特徵、2755 股票、瓶頸 32 的 v3 模型；此檢查不將 v2 權重或 optimizer 匯入 v3 訓練。`load_config` 比對確認資料、交易、walk-forward、評估相同，並確認所有指定計算大小為 `64/16/8/1024/32`。v3 產物目錄尚不存在，沒有執行任何 epoch。

`tw-public-20260921...` 的價格面板到 2026-09-18，但該 release 的 share-replacement archive 只到 2026-09-17；正式 `tw_day_trade` 實體持股來源因此拒絕此配對。此配置選用已 materialize 且有 `READY` 的 `tw-public-20260918T013010528444073Z-l0-penguin-ac1e4027dfa26b98`，其下載摘要與 share-replacement 收據都結束於 2026-09-17。模型仍使用更新的 v3 研究特徵表；面板自然只對齊至股票資料的 9/17。2026-09-18 的股票價格留待完整 share-replacement 來源發布後，再以新的固定版本加入，不能假裝這一天已通過實體執行資料驗證。

遠端 `train.py --check-data-only --start-fold 11 --max-folds 1` 於 2026-09-21 退出 0：`panel_sessions=3100`、`panel_symbols=2755`、`panel_features=428`、`folds=1`，完整預檢約 873.7 秒；其中實體來源重建約 688.2 秒。實體來源收據列出 `minute_symbol_days=2,798,943`、`daily_proxy_symbol_days=2,996,081`、`source_gap_symbol_days=0`，同時保留 `unresolved_action_gap_symbol_days=563,949` 的未解企業行動證據，不能將「來源沒有缺列」解讀成所有行動均已釐清。此預檢沒有啟動模型、optimizer、checkpoint 或 fold-completion marker。

## v4 實測修正：VRAM、編譯圖與整輪成本

v3 的 428 通道 FP32 訓練面板在每張 GPU 常駐 `14.72 GiB`。它仍需執行每批 8 個交易日、2,755 股票、每交易日 270 個右標分鐘、可變 FIFO cohort 的精確帳本。32 GiB 顯卡在 v3 第 5 輪即量到 GPU 0 `31,664 MiB`，只剩 `447 MiB`；使用者先前在第 4 輪的 20 MiB 配置處 OOM。PyTorch 當時未配置但保留的容量只有約 56 MiB，因此此案主要是實際容量不足，不能以碎片化參數當作修正。

[v4 配置](../configs/deployments/tw_public_all_observed_v8_ofat_fold11_data_cpu_cache_v4.yaml) 只改實驗名稱、產物根目錄，及 `cache_train_tensors_on_gpu=false`、`cache_eval_tensors_on_gpu=false`；遠端解析後逐欄比較證實其餘設定與 v3 相同。基底面板留在主記憶體，批次才轉到 GPU；保留全股票、全 270 分鐘、全部 166 個 global batch、完整軌跡每輪一次 optimizer 更新、驗證、測試曲線與 checkpoint。資料通道和交易數學都沒有裁切。

遠端 vastai1T 兩張 RTX 5090 對 v4 **同一程序連跑 6 輪**，每輪都完成 train、validation、sampled test 與 checkpoint；第 6 輪 `checkpoint_last.pt` 的 `epoch=6` 已實際讀回。GPU 0/1 的 1 Hz 量測峰值分別為 `18,804/13,064 MiB`，沒有 OOM；第 2–6 輪 `dynamo_unique_graphs_epoch_delta=0`，第 1 輪冷啟動為 6。前 5 輪的 train loss、val loss、test loss 和 learning rate 與 v3 **逐值相同**。v4 第 3–6 輪牆鐘為 `70.7、71.5、74.5、72.5 s`，其中位數約 `72.0 s/輪`；v3 第 3–5 輪為 `103.8、97.8、97.7 s`，且逐輪新增編譯圖。這是此設備、資料與版本的實測常數改善，不是對所有資料或 1024 輪的保證。

從成本下界看，現有 FIFO 已將分鐘退出轉成容量與現金前綴和，再以二分搜尋定位 cohort 邊界；未建立 `[cohort, 股票, 分鐘]` 三維退出表。模型本身每輪約 1–2 秒，主要成本仍是精確實體 loss 約 29–32 秒、其 backward 約 22 秒，以及驗證帳本約 10 秒。主記憶體轉送約 4–5 秒；兩 rank 已把 validation 與 sampled test 重疊執行。保持實體張數、逐分鐘容量、FIFO 費用和前後日持倉時，不能靠縮短 270 分鐘、丟股票或跳過訓練 batch 來宣稱等價加速。v4 實際降低了常駐顯存和反覆編譯的成本；未聲稱改變 FIFO 演算法的漸近複雜度。

[先前的雙卡 FIFO 熱路徑 A/B](TW_DAY_TRADE_FIFO_TRAINING_HOTPATH_OPTIMIZATION_2026-09-14.md) 已測過每 session 最小 cohort：數值相同但穩態約慢 `5.5%`；動態 sparse event 造成大量編譯且更慢，固定 sparse ABI 則改變績效指標。因此不把這些方案套入 v4。若要進一步降低 exact FIFO 的主項，應獨立實作並驗收融合的 deterministic GPU kernel，逐 session 比對張數、現金、費用、NAV、梯度與完整 fold 產物後才可升級；目前沒有這項等價證據。

訓練專用時間基底與 RMS 快取預設在 `output_dir` 的父目錄下，以資料內容、有效日期、fold 與設定指紋檢查。正式 v4 和 v3 都在 `artifacts/markets`，兩項均命中；獨立診斷產物放在 `artifacts/diagnostics` 時重新擬合時間基底約花 `279 s`。跨產物父目錄做等價診斷時，可明確設定 `STOCKAGENT_TRAINING_TRANSFORM_CACHE_DIR=/root/stockAgent/artifacts/markets/.training_transform_cache_v1` 以重用同一個已驗證的內容定址快取，不能直接複製無指紋的訓練轉換結果。

另以相同 v4 資料／模型／global batch16、`--epochs 1 --no-resume` 和獨立診斷產物根目錄跑完**雙卡 DDP 完整 fold11 生命週期**，程序退出 0。`progress.json` 回報 `state=complete`、`strategy=distributed_data_parallel`、`failure=null`；`fold_11/fold_complete.json` 為 `status=complete`，`test_backtest.npz`、`deployment_test_backtest.npz` 和根目錄 `walkforward_deployment_backtest.npz` 均已產生。這證明一輪的訓練後收尾可執行，不把這份一輪模型當作正式 1024 輪結果。

正式 v4 產物另用 `--resume` 從第 6 輪 checkpoint 實際進入第 7 輪，雙 rank 都恢復 RNG 並完成 166 個 batch、驗證、測試與第 7 輪 checkpoint；讀回的 `checkpoint_last.pt` 為 `epoch=7`。續訓啟動的是新 Python 程序，因此它的第一輪又量到 6 張冷啟動圖，不能算作原六輪程序的穩態重新編譯。確認 checkpoint 後即以 SIGINT 停止診斷，未在背景留下持續訓練。

## 自行以前景模式續訓

v4 採用 global batch16 與 `score_entmax_cash`，並依上節改動其他計算形狀；它不再是逐欄相同的 OFAT 基準。先前要求的 batch128 是另一個 `naive`／`projection_l1` 配置的實測值；不可把它的顯存結果套用到 22 家族和 FIFO 執行的這個配置。訓練由使用者在遠端終端機自行啟動，標準輸出即時顯示：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
export CUDA_VISIBLE_DEVICES=0,1 PYTHONUNBUFFERED=1
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_public_all_observed_v8_ofat_fold11_data_cpu_cache_v4.yaml \
  --start-fold 11 --max-folds 1 --resume --profile-timing
```

`--resume` 已由配置啟用，也在命令列明示；上述前景命令會接續 v4 的最近一個 checkpoint。v3 的 checkpoint 留在其原產物目錄供稽核，不得跨配置套用。六輪實測及一輪完整 fold 生命週期診斷，不等於 1024 輪已完成；最終模型績效只能由完成的 fold 產物判定。

此配置以 vastai1T 的 `/root/stockAgent` 現行程式執行與預檢；本機工作樹目前是不同 Git 版本，尚不支援基準使用的 `score_entmax_cash` 模式。本機能讀取 YAML 文字，但不能把本機 `load_config` 失敗誤判為遠端已驗證配置失效。
