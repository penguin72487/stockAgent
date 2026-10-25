# 期貨策略的市場依賴性、完整商品範圍與相對價值實驗

2026-10-24 **10:26 臺北**獨立讀回 `114.34.26.236:43269`。八組正式案例已依原停止規則完成，
第九組 `clock0900__adamw_lr3e5` 正在第 65 回合；前一完整回合為 64、no_improve=63、零梯度批次為 0。
後面一組 notional＋Muon、兩組共同 2020 個股對照、兩組擴大股票類宇宙仍排隊。
本次新增六組相對價值輸出，已完成來源／設定／日曆准入並由 Supervisor 排隊，尚無正式訓練結果。

## 相同期間的正式成果

以下均為最後 fold 12、2026-02-26～2026-09-24 的相同 **145 個交易日**。
策略沿用一億元起始帳戶、費稅、整口、成交容量、保證金與跨日帳務。
模型由 validation 選 checkpoint；此表是已經檢視的研究期間，不是 untouched holdout。
相關性及 beta 使用對齊日期的**簡單日報酬**，先由 NPZ 保存的 log return 還原；不是累積 NAV 相關性。

| 案例 | 費後報酬 | Sharpe | Sortino | MDD 幅度 | 日報酬相關性 | OLS beta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 08:45 自由輸出 | 25.8949% | 1.03164 | 1.45924 | 24.8705% | 0.65598 | 0.74073 |
| 09:00 自由輸出 | 10.4598% | 0.62828 | 0.86955 | 20.5852% | 0.53938 | 0.43164 |
| 08:45 名目本金座標 | 17.5899% | 1.23174 | 1.77363 | 12.3282% | 0.73192 | 0.48777 |
| **09:00 名目本金座標** | **23.5885%** | **1.78268** | **2.58078** | **12.4790%** | **0.77663** | **0.46720** |
| 08:45 Muon | 28.1750% | 1.13122 | 1.62781 | 24.2815% | 0.61865 | 0.68755 |
| 09:00 Muon | 10.1209% | 0.61604 | 0.85307 | 20.2071% | 0.52850 | 0.41804 |
| 08:45 AdamW LR=3e-5 | **33.0421%** | 1.08992 | 1.59227 | 29.0887% | 0.76057 | 1.01249 |
| 08:45 名目本金＋Muon | 19.1086% | 1.30284 | 1.87872 | 13.1210% | 0.73391 | 0.49901 |
| TX 一倍名目本金近月轉倉毛基準 | 30.7217% | 1.35995 | 2.03942 | 17.2172% | 1 | 1 |

目前 Sharpe／Sortino 最好的是 09:00 名目本金組，但它的報酬尚低於 TX；最高報酬組的 MDD 更大。
尚無一組同時在四個績效指標全面勝過基準。「目前最好」必須明示比較指標。
TX 為現有 `tx_front_rolling_1x_gross`，不是同帳戶、同費稅的整口 TX 執行策略；不混稱為實際成交績效。

目前風險指標最好模型的正式入口是：

`artifacts/markets/tw_stock_futures_history2013_20261023_v2/clock0900__notional/fold_12/`

遠端 `/root/stockAgent/` 下有完整 checkpoint；本機這次補回的是圖表與數值檢視。
八組共 **160 張 PNG、192 個檔案、62,704,693 bytes**，逐檔 SHA、來源前後一致與 PNG 解碼均通過。
[原六組圖表](../artifacts/markets/tw_stock_futures_history2013_20261023_v2/CURVES.md)、
[已完成條件化兩組圖表](../artifacts/markets/tw_stock_futures_conditioning_history2013_20261023_v3/CURVES.md)。
完整 checkpoint 回傳仍沿既有 completed-return owner，未把這次唯讀圖表交付當成冷儲存完成。

## 從第一性原理定位問題

分散商品身分不等於分散風險來源。可將日報酬描述為市場共同變動與剩餘變動：
`r_policy = intercept + beta × r_TX + residual`。這是整段研究期的診斷回歸，不能拿全期估出的
beta 回頭當成可交易避險指令，也不能把 R² 當成獲利占比。

實體部位核對顯示，八組同一測試期間均有 **337 種可執行個股期貨商品代碼**；實際持有
108～184 種，各組亦有 114～145 個空單日。因此，問題不是模型根本沒有多空或只交易 TX。
09:00 名目本金組持有 184 種，R² 卻達 **60.32%**：大量個股仍承擔共同台股風險。
其帳本隔夜損益為最終淨利的約 **125.99%**，其餘開盤後損益與成本合計為負。
最高報酬組的隔夜損益占淨利約 **204.56%**。占比超過 100% 是加總損益抵銷，並非多出一筆報酬。
這個拆解不能替代獨立資金、獨立成交條件的日內／隔夜策略回測。

目前純 log-utility 沒有要求市場中性；若市場漂移在 train／validation 有利，學到正 beta 是
目標下的合理解，不能僅因曲線相似便判定反傳錯誤。另有兩個需避免的機制：

- `-mean(log_return_policy - log_return_benchmark)` 中 benchmark 是參數無關常數，
  梯度與原 log-utility 完全相同，不能用它宣稱已改成獨立 alpha 訓練。
- 只追求小相關性，現金或噪音也可能通過；需同時驗收非空整口交易、費後收益、風險與樣本期間。

既有 [v16 依賴性研究](tw_futures_v16_dependence_and_fold1_2026-09-28.md) 已記錄這些問題；
不能再把同一種常數扣減包裝成新方法。使用者喜歡的
[Unit MGDA 股票當沖策略](tw_day_trade_unit_mgda_deployment_2026-10-21.md) 則是不同商品、
資金、執行／日內持倉及研究回放契約；它支持另尋報酬來源的研究方向，不是目前期貨的直接公平排名。

## 「全部台幣期貨」的實際範圍

**目前仍不是全部。** 目前 GPU 實際訓練是原個股期貨研究；metadata 的廣泛 product-family
名稱不能代替實際 action tape 與 source admission。

新財務 release 的歷史來源目錄要求 765 個 TWD 商品代碼，准入 725 個：
682 個個股、27 個 ETF、16 個指數。尚有 **40 個未准入代碼、132,622 個 blocked account rows**，
`all_requested_product_codes_admitted=false`、`full_history_training_ready=false`、
`runtime_training_verified=false`。以上是歷史代碼盤點，包含調整及到期家族，**不等於今天上市數量**。
未准入清單包含 BRF、TGF、GBF 等；不把調整代碼／非現行商品逐個冒稱成目前都可交易。

個股＋ETF＋指數的 `twd_equity` 雙時鐘來源已驗證，共同窗為 **2020-03-23～2026-09-24**，
993,412 個 entry rows，pair identity
`06c54b6d0884ed00e95c3765bf0f8661fbdd96686391fbdd3c73a8980f6f6514`。
原 2011 CTF 108 vs 107 口來源缺口、其他商品完整 dated rules／clock 與目前上市覆蓋仍未完成。
這輪沒有以刪資料、假成交或現行規格回填歷史來通過它們。

[期交所商品目錄](https://www.taifex.com.tw/cht/2/products)與
[台幣黃金規格](https://www.taifex.com.tw/cht/2/tGF)確認股票類之外仍有需獨立處理的台幣商品。
後續全商品准入需逐契約補上市／下市、乘數及報價／結算單位生效日、到期／最後交易時刻、
稅基、保證金／部位限制、逐時鐘真實成交與來源數量核對；今天的規格頁不能證明 2011 的規則。
既有 retrospective selection／非完整 publication-time PIT 標記保留，沒有宣稱全部資料可直接實盤。

## 文獻脈絡及採用範圍

本次查核基礎至 2026-10-23 的相關原始論文／作者版本，重點閱讀殘差建構、成本與中性投影。
這是有針對性的文獻選取，不宣稱窮盡所有最新論文或已完整重現各篇成果。

| 文獻 | 與本輪問題相關的內容 | 本輪決策 |
| --- | --- | --- |
| [Pairs Trading，1999 工作論文／2006 正式版](https://www.nber.org/papers/w7032) | 從相似資產的相對價差而非單一市場方向尋找收益 | 採相對價值假說；不照抄固定配對門檻 |
| [Avellaneda–Lee，2008／2010](https://math.nyu.edu/~avellane/AvellanedaLeeStatArb071008.pdf) | 因子／ETF 回歸後交易剩餘變動 | 先測廉價淨曝險限制；若仍高相關，才加因果 beta 估計 |
| [Deep Hedging，2018](https://arxiv.org/abs/1802.03042) | 把摩擦及可行策略放進端到端學習 | 延用現有精確帳務；避險不等於可獲利 alpha |
| [Deep Momentum Networks，2019](https://arxiv.org/abs/1904.04912) | 同時學信號與部位，考慮交易成本 | 保留 NN 直接學政策；其 continuous-futures 結果不能替代本案整口帳務驗證 |
| [Deep Learning Statistical Arbitrage，2021／2022](https://arxiv.org/html/2106.04028v2) | 因子剩餘組合→時序信號→受限、含成本政策 | 採收益來源分解；不把本文簡化成本或收益數字搬到台灣期貨 |
| [Implementable Efficient Frontier，2026](https://academic.oup.com/rfs/article/39/10/3035/8524346) | 關注樣本外、成本後可實現前緣 | 同時報成本後收益及風險，不以無摩擦優化值定輸贏 |
| [AlphaZeroBeta，2026-07](https://arxiv.org/html/2607.18001v1) | 中性輸出投影及 neutrality-aware reward | 借鏡投影；本輪保留單一 log-utility，不先增加 PPO 或混合 reward |
| [From pixels to profits，2026-10-23](https://link.springer.com/article/10.1007/s11408-026-00507-x) | 股票、因子 hedge、殘差聯合建模；成本及 beta 估計影響結果 | 讀其殘差／hedge 設計；目前不把已有序列繞成圖片增加前處理 |

上述外國股票／連續期貨的研究不證明本案會賺錢；市場中性也不保證 MDD、Sharpe 或報酬同時更好。

## 已實作的新輸出與六組排程

保留單一手續費後 log-utility，從輸出可行集合改變模型可採取的策略。
帳戶回饋先決定 raw margin weights `u`；用決策前已知的一口名目本金 `N` 與初始保證金 `M`
轉成 `n = u × N/M`。對有效商品均勻扣掉超出淨額上限的共同分量，再轉回 margin coordinates，
必要時只向下縮放，以維持原 NN 學到的 L1 保證金預算。無效商品仍為零。
這是淨額帶的正交投影再做預算縮放，不聲稱是兩個限制交集的單次精確最近點解。

新增 `futures_net_notional_cap: null / 0.25 / 0.0`：自由、多空淨額上限 25% NAV、先驗名目本金中性。
投影在**實際帳戶回饋之後**執行；不再讓後續 feedback 把中性頭蓋掉。
原費稅、價格、容量、整口、公司行動、保證金、違約與跨日帳務均由原 canonical ledger 決定。
因此限制的是先驗請求，**不保證成交後名目淨額、實際 beta 或相關性為零**。
需在保留的 requested weights、整口部位及淨報酬上逐層驗收。

新增 packet version 2；預設關閉仍輸出原 version 1，disabled model fingerprint 相容。
啟用時的 model／trading fingerprint 不同，拒絕從自由輸出 optimizer checkpoint resume。
原型測試發現 `clamp(x, 0, 0)` 在 x=0 的自動微分邊界會留下共同方向；已明確改為零上限常數支線，
加入已中性點的零共同方向梯度檢查。前一 source `d6a4…` 尚未開始 GPU 工作即撤換，保留失敗證據；
實際排程 source 為 **`380ad00ca104c1d889763aa993f4fb7a281fee4e63154630d95e8b0ced512d6a`**。

本機 **175 passed**；遠端從正確 frozen namespace 讀入後，編譯 CPU forward／backward oracle 亦通過。
這是機制驗收，並非六組 GPU 工程或正式收斂完成。

六組為 `free_0845`、`net025_0845`、`neutral_0845` 及相同三種 `0900`。
共同採目前風險指標最佳組的名目本金座標、相同 seed／模型／optimizer，全部從零訓練。
共用 2020 資料窗、最後 fold **5**、validation 2025、test 2026；原 2013 組則是 fold12，
所以新增兩個 free 控制來隔離訓練歷史長度與新宇宙的影響。
保留 d32、lookback32、BF16、雙卡 DDP、1000 回合上限、連續100回合無改善、min_delta=0.0001。

先跑各四回合完整工程，驗收 lifecycle、圖表、checkpoint、非零有限梯度、無穩態新增圖、
非空整口交易；中性組另外要求實際多空整口部位及 feedback 後 cap 檢查。通過的案例才正式訓練；
失敗保留診斷並繼續其他案例，四回合不能用作獲利排名。

- 設定：[六組消融](../configs/ablations/tw_futures_residual_policy_20261024_v1.yaml)。
- 正式產物：[實驗目錄入口](../artifacts/markets/tw_futures_residual_policy_20261024_v1/EXPERIMENTS.md)。
- 新排程：`stockagent-futures-residual-policy-20261024-v2`，10:26 讀回 RUNNING，
  `stage=waiting_for_expanded_universe_queue`、`gpu_training_started=false`。
- 收據：`artifacts/operations/futures-alpha-research-20261024-v2/`。
- 當前狀態：[10:26 獨立讀回](../artifacts/operations/futures-alpha-research-20261024-v2/current-progress-readback-latest.json)；
  八組完成狀態及 10:20 讀回另保留，沒有用排程 RUNNING 代替 GPU 開始的證據。
- 既有條件化→2020 個股對照→擴大宇宙→本輪六組，使用同一 GPU lease；未停止既有訓練。

後續依 validation 選 checkpoint，再用同日費後報酬／Sharpe／Sortino／MDD、絕對相關性、
beta、實際多空曝險及成本比較；現金的未定義相關性不算通過。
若名目中性仍高 beta，下一步應測僅由過去觀測估計的因子中性投影，與本輪控制配對，
不能用整段 test 的 OLS beta 回填。尚無證據宣稱本輪已超越任何正式模型。

## 計算效率及剩餘界線

10:20 兩張 RTX5090 的 GPU utilization 為 **97%／100%**，使用記憶體 **31,806／31,944 MiB**，
各卡總量 32,607 MiB。最新完整 epoch 53 的 train MAX-rank 為 **28.545 秒**、epoch wall 為 **29.600 秒**，
沒有新增 Dynamo graphs；另以 `epoch_workflow_timing.jsonl` 保留含曲線等工作的完整回合計時。
不能把非同步 host latency 各欄相加，或把 finite-check 的等待欄直接當成 GPU 算術成本。

沿用已驗收的 shared transform、日期視圖安全 CSR、稀疏 adjoint 與雙卡執行。
前一批 finite-guard A/B/B/A 的完整穩態 MAX-rank **27.974→27.879 秒**，約 0.34% 改善，
未達原 3% 准入門檻，`selected=false`；本輪不採用，也不改判歷史測速。
新輸出僅增加 **O(T×S)** 的逐商品投影／縮放，沒有 S×S covariance、SVD、額外模型 pass 或 CPU 熱迴圈，
可由原 compiled ledger 處理。工程仍逐組比較含 val／test／圖表／checkpoint 的完整回合，
容量不足只沿既有 batch-capacity retry，不縮特徵、日期或模型維度來偽裝加速。

**尚未證明只剩硬體計算瓶頸。** 高 busy 與穩定圖不足以區分記憶體頻寬、kernel 效率、同步及計算下界；
後續應在同一 GPU lease 內以 CUDA events／trace 定位可改善部分，保留數值／整口 oracle 和完整 fold 比較。
本次已完成資料／帳務診斷、輸出機制實作、CPU 語義驗收及遠端排程；新版本 GPU 工程、正式收斂、
全台幣商品完整性與硬體下界驗證仍是分開的未完成項目。

本機與遠端資料維持 `data_*`、正式訓練在 `artifacts/markets/`。
工程暫存完成後由既有 testing-disposition／discard owner 保留測速及失敗摘要再回收；正式模型及來源資料保留。
本輪另已退休兩個本次建立的私有程式建置暫存：逐檔對上保留的 frozen code、
核對當前程序／FD／mmap、隔離後重新驗證，刪除 6,505 個檔案、釋放檔案 allocated bytes 160,919,552。
正式產物與 cold objects 均刪除 0 個；[精確回收收據](../artifacts/operations/futures-alpha-research-20261024-v2/temporary-build-input-retirement.json)
保留原值恢復位置、私有 index 重建證據及刪後讀回，不是資料根目錄清理。
