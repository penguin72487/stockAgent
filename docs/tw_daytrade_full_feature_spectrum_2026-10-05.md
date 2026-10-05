# 最新當沖 panel：完整維度／能量對照（2026-10-05）

## 1. 執行進度

- 完成 Vast 雙 RTX5090、fold11 **全訓練資料／全特徵**光譜分析；不是抽樣，也不是從前 128 維外推。
- 完成獨立 NumPy 核對：14,727 列維度表（含 0 維）、104 列門檻表、兩份完整 Markdown／CSV、能量分母與最少維度逐列通過。
- 報告防錯測試 7 項通過：錯維度／門檻／能量、漏列／重複列，以及把 100% 上界誤稱最少維度，均會拒絕；兩份專案 Markdown 共 9 個本機連結已確認可達。
- 訓練模型依最新指示保持**不分解**：沒有 SVD、PCA／KLT、時間基底或 24 維 adapter。原始輸入仍 14,726 維，learned embedding32 保留；光譜僅供分析。
- 舊 direct v1／v2 的 BF16 oracle 失敗證據保留、門檻未放寬。新版 v3 明確為 joint stem 加入 FP32 precision island；雙卡六步 oracle、實際完整 fold11 三輪及獨立嚴格 checkpoint／全部產物驗收均通過。正式訓練未啟動。

v3於10:55 UTC完成三輪工程run；14:31 UTC從固定回傳冷庫版本完整還原後，獨立驗收通過。第三輪最大rank epoch320.04秒、train187.87秒、零新Dynamo graph；[目前前景訓練入口與驗收證據](tw_daytrade_no_basis_bf16_remote_2026-10-05.md)。工程就緒不等於正式長訓練已完成，亦不代表盈利／實盤就緒。

## 2. 這裡的「資訊」是什麼

`E(k) = sum_{i=1..k}(sigma_i²) / ||X_training_RMS||_F²`。

這是**非中心化、平方奇異值能量**，不是互資訊、預測能力、報酬率，也不是 PCA 的中心化 explained variance。SVD 的資料光譜不能直接代表學出來的 embedding32 保留多少可預測資訊；learned embedding 會依金融 loss 調整完整 feature 權重，不是固定保留能量最大的 32 個方向。

表中的維度是 SVD 線性組合方向，不是挑出同樣數量的原始 feature 欄位。現在的模型仍讓所有 14,726 個輸入通道進入可學習投影，不使用這份光譜固定裁切方向。

採 fold11 訓練年 2014–2024，training-only RMS／active mask；unique causal input 日期與 alive 商品細胞，不因重疊 32 日窗口重複加權。全市場共同特徵按既有 panel 的 alive 商品權重計入。只分析輸入資料；沒有讀 validation／test 統計或報酬 label 來選方向。

- 4,873,106 個 alive 日期／商品細胞。
- 完整 schema 14,726 維；training active 10,984 維。
- 3,742 維在此 fold 的 training-only active mask 下為零；不是把原始觀測補零，也沒有從模型 schema 刪除。
- Gram 乘積、累積與完整 eigensolver 均 FP64；正規化遵循實際模型的 FP32 RMS。

## 3. 維度 → 累積能量

| 維度 | 累積能量 |
| ---: | ---: |
| 0 | 0.00% |
| 1 | 3.08% |
| 2 | 5.38% |
| 4 | 8.99% |
| 8 | 13.34% |
| 16 | 18.31% |
| 24 | 21.93% |
| 32 | 24.68% |
| 64 | 31.94% |
| 128 | 40.39% |
| 256 | 50.39% |
| 512 | 62.11% |
| 1,024 | 75.20% |
| 2,048 | 88.77% |
| 4,096 | 98.98% |
| 7,525 | 99.99999999995627%（數值容差秩） |
| 10,984 | 100%（完整 active，沒有截斷；浮點累積約 99.99999999999997%） |
| 14,726 | 100%（完整 schema，含 training inactive 零座標） |

[0–14,726 每一維完整 Markdown](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-full-feature-spectrum-v1/dimensions-to-energy.md)／[CSV](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-full-feature-spectrum-v1/dimensions-to-energy.csv)。明細包含個別奇異值、個別方向能量與累積比例；沒有 Top-K 截斷。

## 4. 能量 → 最少維度

| 目標能量 | 所需維度 |
| ---: | ---: |
| 0% | 0 |
| 10% | 5 |
| 20% | 20 |
| 25% | 34 |
| 30% | 54 |
| 40% | 125 |
| 50% | 250 |
| 60% | 456 |
| 70% | 782 |
| 80% | 1,305 |
| 90% | 2,184 |
| 95% | 2,875 |
| 99% | 4,110 |
| 99.9% | 5,311 |
| 99.99% | 6,094 |
| 99.999% | 6,604 |
| 100%（不截斷的 active 座標上界） | 10,984 |

100% 最小維度等於精確代數秩，但有限精度計算不能證明精確代數秩。本次數值秩為 7,525，判定 `lambda > 0.004022555043798281`，其尾部尚有約 `4.37e-11%` 能量。不能把顯示四捨五入的 100.00% 當成完全無損；10,984 是保留完整 active 座標、不截非零方向的安全上界，不聲稱是精確最小值。

[0%–100% 每 1% 加 99.9%／99.99%／99.999% 完整 Markdown](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-full-feature-spectrum-v1/feature-spectrum-report.md)／[CSV](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-full-feature-spectrum-v1/percent-to-dimensions.csv)。每一門檻都有實際累積比例與定義。

## 5. 舊估計與新完整結果

| 維度 | 舊 randomized 截斷估計 | 新完整 FP64 光譜 |
| ---: | ---: | ---: |
| 32 | 24.65187454% | 24.67709863% |
| 64 | 31.74791179% | 31.94405467% |
| 128 | 39.26693241% | 40.39054505% |

舊結果的分母正確，但範圍估計未完整收斂；不能只把前 128 維加總改成 100%。本次直接求出完整 active Gram 的全部 eigenvalues，之後補回其餘 training inactive 零方向，不再保留未估計的光譜尾部。

## 6. 驗證與可重跑證據

- 全量分母：53,526,196,307.00244。
- 獨立 RMS 理論對照：`4,873,106 × 10,984 = 53,526,196,304`；相對差 `5.6093e-11`，來自 FP32 正規化尺度與運算。
- Gram trace 相對差 0；全 eigenvalue sum 相對差 `1.4254e-16`。
- 最小 raw eigenvalue `-1.0590e-7`，負尾能量比例 `5.3476e-16`，在公開數值容差內；不是資料負能量。
- Gram 105.36 秒、完整 eigensolver 2.85 秒、計算本身 108.21 秒。這不含 panel／RMS cache 讀取，也不是訓練 epoch 耗時。
- 分析程式 source SHA `73db0d26909f3886b0702e4dc6646992eefd75205d83aedefb554e55b5ad42d5`；固定 value-only feature manifest SHA `33bed11d08665f5578b6ef11f3689ee80c99b22b260f661683d3e35eb21d9f50`。
- Vast workflow：`full-feature-spectrum-v1-20261005T102104-58f879c2`。分析產物在 `/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261005_no_basis_v2/full-feature-spectrum-v1`。

[完整數值與 RMS／來源收據](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-full-feature-spectrum-v1/full-feature-spectrum.json)；[獨立驗收收據](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-full-feature-spectrum-v1/independent-validation.json)。重跑與驗證入口分別為 [profile_feature_svd.py](../scripts/profile_feature_svd.py) 的 `--full-spectrum` 與 [verify_feature_spectrum_report.py](../scripts/verify_feature_spectrum_report.py)。

此為受限私人研究資料。原 `historical_point_in_time=false`、`research_only=true`、`live_eligible=false`、企業行動等 masks 與缺口不因光譜分析而消失。僅對此 fold／training RMS 範圍作結論，不宣稱全部來源歷史完整或可實盤。

方法參考：[非中心化 SVD 定義](https://scikit-learn.org/stable/modules/generated/sklearn.decomposition.TruncatedSVD.html)、[PyTorch 2.11 eigvalsh](https://docs.pytorch.org/docs/2.11/generated/torch.linalg.eigvalsh.html)。
