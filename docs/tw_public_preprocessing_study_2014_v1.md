# 台股多基底特徵預處理研究（2026-09-20）

## 目前實際契約

- [v3 全特徵配置](../configs/markets/tw_public_preopen_all_observed_research_2014_v3.yaml) 有 428 個模型欄（20 個股票欄、204 個公開值、204 個可用性旗標），已使用訓練期逐欄 RMS 縮放；**它的 `temporal_basis_families=[]`，所以目前不是多基底實驗**。
- `stockagent/training/trainer.py::_fit_group_causal_feature_rms` 只讀取該訓練組可見的 feature rows；validation/test 不參與擬合。零值不減均值，訓練期未見欄位保持停用。縮放值及啟用遮罩隨模型 checkpoint 保存，並寫入 `causal_feature_rms_normalization.json`。
- `CandleEncoder._normalize_raw_features` 在一般輸入和 `temporal_basis_input=input_features` 路徑均於多基底之前作用，後者不會再縮放一次。模型內的逐 token RMSNorm 不能代替逐欄單位縮放。
- v3 接受現行修訂總經值、推測公告日期及 2026 起才捕獲的快照；它是**特徵表示研究資料**。即使統計量完全依訓練期擬合，也不能把此資料的報酬當成有歷史 vintage 證明的交易績效。

## 第一性原理：一筆決策可用的資訊

對決策時點 `t`，只有發布／捕獲且經該策略時鐘允許的 `x_{s,u,f}`，`u<=t`，可進入 trailing window。對一個 walk-forward 訓練組 `G`，預處理參數 `theta_G` 只能由該組訓練窗口的值求得；驗證、測試與推論套用同一個凍結的 `theta_G`。若要模擬每日重新擬合，則 `theta_t` 只能由 `<t` 的值求得，並須把該日版本記進產物；兩種設計不可混用。

`train-only fit` 只解決**統計量洩漏**。另須檢查：來源數值是否為當年的歷史版本、公告及捕獲時間、完成日線是否移至下一個 09:00、股票存續與資格、標籤跨 fold 邊界、評估區間是否重複。測試集不許參與特徵選擇、截尾門檻、early stopping 或最佳方法挑選。

## 預處理候選與合適欄位

| 方法 | 公式或擬合範圍 | 主要用途 | 主要風險 |
|---|---|---|---|
| 保留原值 + train-only RMS | `x / sqrt(mean_train(x²))` | 現有對照；保留零與符號 | 稀疏欄的缺值零參與分母，少量大值與極端值影響尺度 |
| 正值 `log1p` / `log` | 固定數學轉換，`x>=0` / `x>0` | 成交量、金額、股數、正值總量 | `log(x)` 無法處理零；原值和衍生值可能高度重複 |
| `signed log1p` | `sign(z) log(1+abs(z))`，`z=x/RMS_train` | 有正負號的金流、稅額、淨買賣 | 壓小極端值，也可能丟掉絕對規模訊號 |
| `asinh` | `asinh(x/RMS_train)` | 零附近近似線性、兩端近似對數的有號量 | 對 0 附近和尾部的權衡與 signed log 不同 |
| train-only observed RMS / robust scale | 只在可用且存續的訓練格擬合，或用訓練期中位絕對偏差/IQR | 稀疏且長尾的公開欄 | 需保留缺值旗標、樣本數和回退規則；目前尚無正式訓練實作 |
| train-only clipping / winsorization | 門檻從訓練期求得後凍結 | 錯誤尖峰、數值穩定性 | 真正的事件衝擊也可能被裁掉；先審查源資料 |
| trailing rolling/expanding scale | `t` 當下只用已公布歷史 | 非平穩量級、長期通膨 | 初期樣本不足、跨停牌／來源中斷，需記錄每日時點狀態 |
| 逐日橫斷面 rank / z-score | 僅對當下可觀測且合資格的股票 | 當日相對強弱 | 失去市場整體水準；完整當日 universe 與同時可見性必須成立 |
| trailing-window RMS | 每筆決策僅用其可見的 32 列；稀疏欄只算 observed 值 | 短期量級漂移、嚴格逐筆因果 | 絕對水準可能減弱；重疊窗口不能共用同一投影，顯存增加 |

價格優先比較歷史可用的對數報酬、比率與相對價格；原始價格可保留作尺度訊號，但不能用事後調整價取代當時報價或忽略時變 tick。已是 `*_log`、`*_asinh`、`*_logret*` 的欄位先不要重複壓縮。可用性旗標先由 null 產生，再把缺值數值設零，不能在求原始分位數前當真零。本輪非線性壓縮不碰旗標，但既有 RMS **仍會縮放旗標的 1**；稀有旗標可能變得很大，應另做「旗標保持 0/1」的獨立實驗。`twpub_company_industry_code` 在 v3 設定中也是普通數值，不是類別 embedding；它的序數假設應另行檢驗。比率／成長率要檢查分母與定義域；金融報表負值不能直接取自然對數。

## 已接入的第一輪 OFAT

以下三個配置使用相同資料、428 欄、lookback、模型、多基底 `[haar,dct] x 2`、batch 與 train-only RMS；只變更 `*_raw` 欄在 RMS 後、多基底前的壓縮：

| 對照 | 配置 | 選中的原始欄 |
|---|---|---:|
| RMS | [rms](../configs/markets/tw_public_preopen_all_observed_multibasis_rms_2014_v1.yaml) | 不壓縮 |
| RMS + signed log1p | [signed_log1p](../configs/markets/tw_public_preopen_all_observed_multibasis_signed_log1p_2014_v1.yaml) | 79 |
| RMS + asinh | [asinh](../configs/markets/tw_public_preopen_all_observed_multibasis_asinh_2014_v1.yaml) | 79 |

`79` 是 2026-09-20 v3 來源 schema 與本 ABI 的實際匹配數；來源增加欄位後要重查。這輪測試的是「在既有原值加工程值一起輸入時，額外壓縮原值的增益」，**不是**排除工程值後的純 raw 對 log 比較。`signed_log1p` 和 `asinh` 使用無單位的 `x/RMS_train`，因此保留零與負號，也不引入任意「元／百萬元」常數。方法和欄位模式進模型 checkpoint 指紋；三臂各有獨立 artifact root。

## 執行與判定

先以 `source scripts/runtime_env.sh` 啟用環境，對每個配置執行 `run_fintech_python train.py --config <配置> --check-data-only`。這僅驗資料，不產生勝負結論。正式跑前檢查 CUDA 與單卡 16 GiB 的真實 batch 記憶體；若須縮 batch，三臂一起改且重新開始。完整訓練三臂保持相同 fold、seed、epochs、early stopping、評估費用及交易時鐘；不要從一個方法的 checkpoint 微調另一方法。

每個 fold 只用 train 擬合縮放，val 選 epoch／方法，test 保留一次最終評估。現有 walk-forward fold 的 `test` 是之後**所有**年份，會彼此重複；跨 fold 彙總應依第一個後續測試年或明確不重疊所有權，而不能把重複日當獨立樣本。先報每年及每 fold 的淨報酬、回撤、波動、換手、成交／容量、損失，並比較尾部事件、晚起始欄位啟用與各 seed 穩定性。多次候選搜尋後需保留全新、未調參測試期。

第二輪再以勝出的壓縮方式，逐一比較 observed-only RMS、robust 無中心縮放、訓練期截尾、trailing scale、逐日橫斷面尺度；不能一次改多個預處理維度。模型外之歷史版本與 09:00 可用性問題要先建立嚴格 PIT 子集，才用來選實際交易策略。

32 日窗口 observed-only RMS 已另行實作為[同 batch 對照與效能審核](tw_window_rms_causal_speed_2026-09-20.md)。它與上表原有三個 batch 8 壓縮臂分開；做方法選擇時應使用同 batch 的 RMS 對照，避免把 batch 差異算作預處理效果。

## 參考依據

- [scikit-learn：資料洩漏與先分割後擬合](https://scikit-learn.org/stable/common_pitfalls.html)。
- [scikit-learn：各種縮放、長尾和 outlier 的行為](https://scikit-learn.org/stable/auto_examples/preprocessing/plot_all_scaling.html)。
- [scikit-learn：QuantileTransformer 的訓練期分位數與區間外截斷](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.QuantileTransformer.html)。
- [Kim 等：RevIN 原始論文](https://openreview.net/pdf?id=cGDAkQo1C0p)。其時序預測結果是候選機制的依據，並不能直接推出本台股交易模型的績效。
