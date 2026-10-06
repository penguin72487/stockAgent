# 最新 value-only panel 特徵 SVD32 遠端實驗

## 1. 執行進度

- 此 SVD32 實驗已被最新指示取代：使用者認為能量损失過大，回到[不分解模型](tw_daytrade_no_basis_bf16_remote_2026-10-05.md)。仍保留光譜分析，但不接進訓練。
- 原全量訓練資料的 randomized 截斷估計已完成：32 維 24.65187454%、64 維 31.74791179%、128 維 39.26693241%；32nd Ritz residual 約 7.34%。不能外推 90%／100% 門檻。
- 原雙卡六步 oracle 在 BF16 第三步拒絕 1 個 joint projection gradient 元素（絕對差 0.0006103515625；原 atol0.0005），未放寬驗收。完整 fold 三輪／正式訓練沒有啟動，失敗 log／diagnostics 保留。
- 已實作特徵軸 SVD 的訓練期擬合、固定 FP32 方向、checkpoint 回讀、嚴格 cache 與 canonical trainer 接線。原始觀測不改動，沒有新增 availability／age／update 通道。
- 此實驗當時本機相關回歸 194 項通過；FIFO／狀態相關 63 項通過、3 項條件跳過（組合有重疊，不相加）。這不等於實際 GPU oracle 或完整 fold 通過。
- 無基底 v1 已被原 BF16 oracle 正確拒絕；其失敗證據保留。它沒有完成訓練，不作為新版通過依據，也不放宽門檻。

## 2. 分解的軸與度量

將每個 causal training input 日期／alive 商品的完整 14,726 個特徵組成一列；共同特徵按 panel 的既有商品權重計入。只使用該 fold 訓練窗已知的列，unique source rows 不因重疊 lookback 重複計數。所有商品仍保留，未上市／非 alive 細胞不擬合。

先用原 training-only RMS／active mask 統一尺度；不中心化，避免讓原中性零輸入變成平均值的相反數。這是非中心化 truncated SVD，不是 PCA explained variance。

`保留 k 維平方能量 = sum_{i=1..k}(sigma_i^2) / ||X_train_RMS||_F^2`。

分母是所有訓練特徵的 FP64 累積能量，不是前 128 個方向的小計。它不是互資訊、可預測資訊或報酬率。高能量的平均水準或共同變化仍可能主導；不能從能量比例保證獲利。

寬矩陣使用全量串流 covariance operator、128 個分析方向、32 個 oversampling、2 次 range refinement，固定 seed7。這是數值截斷估計；報告保留每個方向的 Ritz 相對殘差、全量分母及未覆蓋尾部，沒有抽取少量資料偽稱完整 SVD。

## 3. 接線與不變規則

`完整 lag1 value-only features → fold training-only RMS → 固定特徵 SVD32 → CandleEncoder32 → 原 FinancialTransformer → 原 learned-cash allocation → 原 exact FIFO ledger`。

時間窗口仍是 32 日；未增加 Haar、Fourier、learned bank 或 temporal PCA/KLT。原 Attention factory resolved `latent` 不變；無舊 24 維 adapter、無 learned feature bottleneck、無 sparse-event executor。

保留 BF16 AMP／FP32 master 與敏感金融精度，SVD 擬合／固定投影本身不使用 BF16。每輪仍是完整 chronological trajectory 一次 AdamW；DDP global batch32、只 fold11。進場仍是既有 first-minute-only 50% capacity，13:30 官方收盤 reduction-only 不限容量。

資料仍固定截至 2026-10-02 的 gaprepair_v4 三份來源；受限私人研究包、`historical_point_in_time=false`、`live_eligible=false` 與原企業行動 masks 保留。本次不把研究來源升格實盤資格。

新設定：`configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_svd32_bf16_v1.yaml`。

遠端新 root：`/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261005_svd32_v1`。不續接任何多基底／無基底 checkpoint 或 optimizer。
