# 32 日窗口正規化與資料建制速度（2026-09-20）

## 時鐘與「洩漏」的定義

對 09:00 決策 `d`，模型的最後可見 feature row 是 `d - execution_feature_lag(mode)`；只取它和前 31 個交易日。每個欄位、每檔股票、每筆決策各自算一次無中心 RMS。公開欄位若有 `__available` 旗標，分母只算窗口內旗標為 1 的日子，缺值維持 0；旗標本身保持 0/1，類別 ID 不縮放。無觀測值或 RMS 不大於設定的 `causal_feature_scale_epsilon` 時尺度回退為 1。

`scale[d,s,f] = sqrt(sum_{u in W(d), observed} x[u,s,f]^2 / count_observed)`，`z[u,s,f] = x[u,s,f] / scale[d,s,f]`。不減均值，因為缺值零是資料 ABI 的一部分。縮放在多基底分解**之前**，普通輸入與 Haar/DCT 使用相同的窗口尺度；模型輸入仍保留原始欄與原有多基底係數。`_fit_group_causal_feature_rms` 只從訓練區估計欄位啟用遮罩；窗口版不儲存或套用訓練期全域尺度。評估區、測試區與決策之後的列均不進入該筆決策的尺度。

既有 train-only RMS 不使用驗證／測試資料，所以對固定 fold 的未來評估沒有這種統計洩漏；但訓練期早期樣本會被訓練期後期列算出的尺度縮放。模型權重與欄位啟用遮罩本來也是在整個訓練區擬合；窗口版保證的是**每筆輸入的數值縮放**只看它當時可見的 32 列。模型內 token RMSNorm 只在同一個 token 的維度計算，不跨日讀取未來列，單獨不造成時間洩漏；它也不能替代逐欄單位縮放。

這項改動不會補救來源 PIT：現行 428 欄 v3 研究資料接受當前修訂總經值、推測公告日和較晚才捕獲的快照。窗口因果性不能把它變成 2014 年真實可交易證據。標籤時間、股票資格、除權息和決策成交時鐘也仍須獨立審核。

## 實作與可重現配置

- [窗口配置](../configs/markets/tw_public_preopen_all_observed_multibasis_window_rms_2014_v1.yaml) 與[同 batch 對照](../configs/markets/tw_public_preopen_all_observed_multibasis_rms_batch4_2014_v1.yaml) 使用同一 428 欄資料、Haar/DCT x2、batch 4。模型 checkpoint 指紋包含窗口開關，不能與全域 RMS checkpoint 混用。原有 batch 8 的 RMS／壓縮實驗配置保留；不能拿其結果直接與 batch 4 比預處理勝負。
- [模型實作](../stockagent/models/financial_transformer.py) 在 `forward`、`forward_from_panel` 和 slab 輸入使用同一縮放。每個重疊窗口尺度不同，因此 slab 路徑必須先展開窗口，不能沿用「每個原始日期只投影一次」的快路徑。
- [訓練擬合](../stockagent/training/trainer.py) 只保留訓練期啟用遮罩，並沿用既有 transform receipt/cache。面板快取仍存原值，無須為每種正規化建立第二份面板。
- [基準腳本](../scripts/benchmark_tw_window_rms.py) 以實際 feature ABI 的欄名、相同模型設定和合成資料測量 slab 前向與反向；不讀整個面板、不執行回測。

## 2026-09-20 實測與計算量

| 路徑 | 觀察或下界 | 意義 |
|---|---:|---|
| v3 外部特徵 Parquet | 920 MiB | 原始研究來源壓縮檔；不是模型使用的密集面板大小 |
| 面板特徵 `3103×2755×428×float32` | 13.63 GiB | 單個密集特徵陣列的數學下界，不含價格、mask、組裝暫存 |
| 現有 panel cache v2 | 28 GiB、2 個 generation | 實際磁碟觀察；不能把兩代刪除視為正規化優化 |
| 舊紀錄冷建表 | 566.10 秒 | `startup_timing.jsonl` 單次觀察；另有 786.53 秒觀察，來源與快取狀態可能不同 |
| 本次窗口配置 `--check-data-only` | 面板命中 2.348 秒；完整 preflight 5.923 秒 | 3,103 日、2,755 檔、428 欄、12 folds；沒有訓練模型或回測 |
| B=8、L=32、S=2755、F=428 輸入窗口 | 約 1.125 GiB | 只算一份展開後 float32 輸入；訓練還有梯度與暫存 |
| B=8 的重疊投影數 | 全域 RMS 39 個獨特日期；窗口 RMS 256 個窗口日期 | 輸入投影工作數最多約 6.56 倍；不是 epoch 耗時倍數 |

下列 GPU 數字來自 RTX 5070 Ti 16 GiB，當時已有其他程序使用約 8.9 GiB。BF16 autocast，未 compile，兩個方法各有 2 次暖機；數字是模型 slab 前向加反向，不含資料 I/O、精確交易 loss、optimizer、eval、checkpoint 或整個 epoch。共用 GPU 負載會造成波動。

| B / S / F / L | 全域 RMS 中位秒；峰值 MiB | 窗口 RMS 中位秒；峰值 MiB | 範圍 |
|---|---:|---:|---|
| 4 / 64 / 428 / 32，3 次 | 0.0826；119 | 0.0957；152 | 小型探針 |
| 8 / 256 / 428 / 32，7 次 | 0.1043；458 | 0.1025；729 | 局部截面，耗時差低於此環境波動 |
| 2 / 2755 / 428 / 32，2 次 | 0.0875；1408 | 0.1408；1937 | 全檔數、縮小 batch 的模型探針 |
| 4 / 2755 / 428 / 32，2 次 | 0.1703；2359 | 0.1526；3668 | 全檔數、匹配配置的 batch；速度順序受共用 GPU 波動影響 |

沒有完整 fold epoch 數據，故不聲稱窗口版整體訓練速度或績效更好。窗口版在本機全檔數 batch 4 的模型探針峰值已達 3.67 GiB；目前 GPU 上另有約 8.9 GiB 使用中，因此本地研究配置採 batch 4，且另設同 batch 的全域 RMS 對照。完整 2-GPU DDP、正式 batch 的效能須按 `AGENTS.md` 在目標硬體測量 epoch 3+ 的最慢 rank，包含 loss/backtest 和 checkpoint。單卡探針只證明功能與局部資源需求。

## 降低全流程複雜度的順序

1. **不要把每個 fold、每種轉換、每個 32 日窗口落盤。** 保持一份不可變原值面板，窗口由索引懶展開，尺度在模型內現算。這已是本輪實作，避免 `O(folds × methods × T × S × F)` 的資料複製。
2. **只在來源真的變動時重建面板。** `build_panel` 現有來源雜湊與 v2 快取已能重用；本輪增加冷建表的 `symbols/external/assemble_rules/cache_save` 分段秒數，下一次必要的冷建表可定位瓶頸。沒有為了量測而強制重建 28 GiB 快取。
3. **先辨識密集化成本再改資料格式。** 稀疏月報／事件在 `T×S` 展開後佔據大量零格；未來可評估按發布事件保存值與有效區間，在 batch 組裝時投影。這需要與現有 availability、時間戳、面板 checksum、checkpoint 和 PIT 審核逐項對照，不能直接替換 canonical panel。
4. **若窗口版在正式硬體變慢，優先縮小重複工作。** 連續 slab 的平方和與可用筆數可用前綴累加或定長滑動和從 `O(B×L×S×F)` 降至 `O((B+L)×S×F)`；但模型仍需為每個窗口投影同一日期的不同比例值，投影不能直接共用。需先以逐值、梯度和缺值旗標 parity 驗證再上線。

執行資料預檢：`source scripts/runtime_env.sh && run_fintech_python train.py --config configs/markets/tw_public_preopen_all_observed_multibasis_window_rms_2014_v1.yaml --check-data-only`。這不會證明完整訓練可在本機剩餘顯存完成。
