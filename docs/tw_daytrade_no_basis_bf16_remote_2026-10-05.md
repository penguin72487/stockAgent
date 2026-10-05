# 最新 value-only panel 無基底 BF16 遠端實驗（2026-10-05）

## 1. 執行進度

- 依最新指示修正前景入口：預設 canonical resume、關閉 profiling／debug timing sync，完成 fold 不重訓；移除「正式目錄已存在即拒絕」的錯誤限制。遠端正式 checkpoint 已保存 epoch1／AdamW step1，第2輪中斷退出130；重新執行同一入口會從第2輪接續，而非清空重算。
- 最新指示已回到「不分解」，但仍要求分析完整 0%–100% 的維度／能量關係。新版 v2 明確 `feature_svd_components: 0`；SVD32 不再是選定訓練模型。完整 FP64 光譜另行計算，沒有模型投影或 optimizer。
- v2 保留逐運算 rounding 後，原 BF16 首步仍被拒絕，證據保留。最新 v3 明確把 joint feature stem 納入 FP32 precision island，沒有增減特徵／參數／成交公式；原 oracle 門檻不變。雙卡六步 oracle 已通過、最大梯度差 `4.7684e-7`；實際完整 fold11 三輪、全部 canonical 產物與獨立嚴格 checkpoint 驗收均已通過。
- 獨立工程驗收時間為 `2026-10-05T14:31:33Z`，狀態 `accepted_actual_fold11_dual_gpu_direct_no_basis_engineering`、`training_ready=true`。當時前景入口 `--check-only` 亦退出0：三份 exact source full-SHA／7日 lease與 long-training pins 通過，CUDA strict檢查零 failures／warnings、兩張 RTX5090可用。該歷史收據的 `formal_training_started=false` 僅描述工程驗收時未啟動正式訓練，不能拿來否認後續使用者已保存的正式 checkpoint；沒有使用工程 optimizer。
- 使用者明確要求「先不要玩基底分解的版本」。已停止舊基底版測速，保留原始資料、舊模型及失敗／取消收據；不刪除、不續接舊 optimizer。
- 最新 v3 本機相關測試 199 項通過（23.54 秒）；stateful／precision 相關測試 55 項通過、3 項環境條件跳過（16.54 秒）。兩組分別列示，不和舊版重疊測試相加。
- v3 全新程式與設定位於 Vast 的 `/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261005_no_basis_v3/code`。精確 source SHA：`9491d7928737f21ba38cb1960fd84416ff2c495327fb37e7a5883baa9e7e2c51`。

目前選定設定是 `configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_no_basis_bf16_v3.yaml`；新 root 是 `/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261005_no_basis_v3`。v1、v2 與 SVD32 的失敗／數值證據保留，不續接 optimizer。

[完整 0%–100% 光譜與每一維明細](tw_daytrade_full_feature_spectrum_2026-10-05.md)已另行完成、獨立核對。該分析沒有訓練 optimizer，也不把固定 SVD 接入模型。

## 2. 不變與改變

| 項目 | 設定 |
| --- | --- |
| 最新資料 | 固定截至 2026-10-02；3,109 日、2,757 商品 |
| 輸入 | 14,720 數值 + 6 基本通道 = 14,726；不加 availability／age／update 通道 |
| 原始 NULL | 保留原觀測；模型使用已整理、lag1 的值與中性輸入，不虛構原觀測 |
| 模型 | FinancialTransformer，L32、embedding32、1,133,582 參數 |
| 移除 | 全部 temporal basis families、PCA／KLT fitting、raw-basis FP32 contraction；無 24 維 adapter |
| Attention | 原 selected config 的 factory resolved `latent`；沒有趁機改為 full cross-stock attention |
| 正規化 | 保留 training-only RMS 與 active mask；可用既有嚴格 fingerprint RMS cache，無 val／test 統計 |
| 精度 | BF16 AMP 配置、FP32 master／Adam；v3 的 joint stem、temporal、portfolio 都採 FP32 islands，不能把 AMP 開啟說成全部矩陣都用 BF16 |
| 雙卡 | DDP global batch32、fold11；每輪完整 chronological trajectory 一次 AdamW |
| 歷史成交 | 既有 first-minute-only 50% entry、13:30 官方收盤價 reduction-only 不限容量；無 Shioaji 實盤 |
| 回測區塊 | 原 canonical evaluator，32 日／區塊，逐日連續傳遞狀態，不 reset；需實測金融一致性 |
| 產物 | 每輪 validation／test／同步圖／checkpoint，final best 全 270 分鐘曲線與報告保留 |
| 新正式 root | `/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261005_no_basis_v3/training-bf16` |

`factorized panel` 是共同／個別特徵的儲存方式，不是模型基底分解；本次保留這個有界儲存 adapter，使用已逐 raw 位元驗證的 `compact_cuda` 搬移後完整 dense GPU 模型計算。新 `compact_cuda_packed` 尚未在此實驗啟用。

## 3. 驗收與限制

先通過原門檻的雙卡六步 oracle，再完成真實 fold11 三輪工程 run（僅 engineering CLI epochs3／early_stop_ratio0；正式 epochs1000／ratio0.1 保留）。每輪只有一次完整 chronological trajectory 的 AdamW；最後 optimizer state 的 step 皆為 3，master parameter 全 FP32，兩個 checkpoints 嚴格 manifest／model state 驗證通過。所有 18 個 canonical 必要產物、3 列 epoch curve 與每輪同步圖均通過；第三輪 Dynamo 新 graph 數為 0。

| 真正雙卡完整資料 | train 最大 rank | epoch 最大 rank |
| --- | ---: | ---: |
| 第 1 輪（含 compile warm-up） | 241.25 s | 420.42 s |
| 第 2 輪 | 181.65 s | 315.48 s |
| 第 3 輪 | 187.87 s | 320.04 s |

第 3 輪 validation89.02秒、test-curve131.75秒在不同 rank 並行，不相加為 wall；checkpoint 合計0.39秒。完整 canonical train 子程序含前置 setup、3 epochs、final 回測／曲線／所有產物及 lifecycle 驗收共1,653.05秒；不包含先前六步 fixture、冷庫還原或28.52秒獨立語義驗收。這是新的不分解模型，不能和舊基底模型的耗時當作同模型等價加速比，也不宣稱達到理論極限。

[獨立驗收收據](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-no-basis-v3-metadata/fold11-acceptance-no-basis-v3.json)／[完整工程 timing 收據](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-no-basis-v3-metadata/engineering-lifecycle.json)／[已實測的前景入口 source pins](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-no-basis-v3-metadata/foreground-preflight-pins.json)。

資料為本人受限私人研究包；原 `historical_point_in_time=false`、`research_only=true`、`live_eligible=false` 不變。企業行動等原 masked gaps 仍揭露，不能宣稱全部歷史已完整或策略必然獲利。

## 4. 冷庫回傳與驗收還原

工程結果已於13:31 UTC依既有 completed-return v2 owner 完整回傳、獨立重建，再退休遠端原產物目錄；不是訓練失敗，也沒有刪除原始輸入或 D 冷庫。原始退休收據 identity 為 `793b61f28384a8008daa98633576132cdf08b25fc02c72ca037c8957b4779cb1`。

最後驗收時用 canonical `run_data_cache.sh use --snapshot-id ... --link ... --ttl-days 7 --retain-payload` 重新還原54檔／91,083,581bytes。固定 dataset 為 `artifact-auto-engineering-fold11-3e-62c565ec2e10b49b`；snapshot為 `artifact-auto-engineering-fold11-3e-62c5-20261005T133057769394847Z-l0-penguin-1114c4174a79831c`；manifest SHA `115ce0bc6f4e34ef27163902ce6a9efc5e5e793fdeeab4a14d44a8d898a767c3`。重新取得 full-SHA hot lease 至10/12，原驗收路徑是這個不可變 materialization 的 canonical symlink，不是第二份可變 optimizer。

## 5. Vast 前景訓練指令

直接在 **vastai1T** 執行；不會放到背景，只訓練 fold11：

```bash
cd /root/stockAgent
bash artifacts/markets/tw_day_trade_factorized_values_20261005_no_basis_v3/train_vast_no_basis_fold11_v3.sh
```

入口固定已驗收的 source release／config／14,726 通道 manifest，續租三份 exact input source並核對 long-training pins，檢查CUDA與GPU 0/1 owner，然後在 frozen code 中呼叫原 `train.py --start-fold 11 --max-folds 1 --torch-compile-threads 16 --resume --no-retrain-completed-folds --no-profile-timing --no-debug-timing-sync`。上限1,000epochs／原早停規則、DDP global batch32、不分解／不開稀疏，產物仍在 `artifacts/markets/tw_day_trade_factorized_values_20261005_no_basis_v3/training-bf16`。

預設有相容正式 checkpoint 就續跑，沒有才新建；已完成 fold 由 canonical artifact gate 確認後跳過。入口用 CLI 覆蓋 frozen config 的首次訓練開關，沒有修改固定 source／config SHA 或模型／資料／optimizer contract，也不續接其他實驗或工程 run 的 optimizer。續跑沿用 canonical model、optimizer、scheduler、RNG、早停與曲線修剪流程，仍拒絕不相容 checkpoint。

停止後再執行同一指令即可；續跑粒度是最近已保存的 epoch checkpoint，不是尚未存檔的半輪。`torch.compile` 加速、每輪 validation／test／圖表／checkpoint 保留，關閉的是額外 profiling 與 debug CUDA 同步。

若只想檢查而不開始，可在相同指令末尾加 `--check-only`。本次只執行前置驗收與唯讀 checkpoint 檢查，未替使用者重新啟動正式訓練。入口／續跑／相容性相關測試123項通過（8.57秒），shell語法與新測試 Ruff 檢查通過。測試收據：[default_resume_tests.xml](../artifacts/data_quality/tw_feature_expected_gaps_20261004/default_resume_tests.xml)。

新版遠端 `--check-only` 退出0，三份 exact source full-SHA與7日租約通過，CUDA strict零 warnings／failures。`2026-10-05T15:16:47Z` 唯讀核對實際 frozen `train.py` CLI 解析為 resume=true、profiling=false、debug timing sync=false；兩份正式 checkpoint SHA 完全未變，last checkpoint 保存完整 optimizer／RNG、epoch1、global batch32。收據：[default-resume-verification.json](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-no-basis-v3-metadata/default-resume-verification.json)。本輪没有額外執行一個真正 resumed GPU epoch，不能把前置檢查說成該輪已完成。
