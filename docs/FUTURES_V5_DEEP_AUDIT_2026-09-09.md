# 股票期貨當沖容量邊界與學習失效分析

本頁保存嚴格當日歸零規則的診斷。使用者後續已授權未成交部位留倉；最新帳務改為 [v8 隔日差額調整](FUTURES_RESIDUAL_CARRY_2026-09-09.md)，v7 限定訓練在完成第一 fold 的 177 epochs 後中止並保留證據。

最新 v5 的主要異常不是「所有梯度都消失」，而是**整體梯度仍在更新，少數足以使整條帳戶失效的交易卻沒有縮倉梯度**。已完成的四個 fold 中，三個測試帳戶未通過 13:30 清倉契約；另一個只交易四口。最佳驗證模型交易很少，訓練後期模型則反覆進入失效帳戶。這些現象必須分開解釋，不能一概稱為空手，也不能用訓練梯度範數非零宣告修復。

本次確認三個相關問題：容量上限缺少縮倉回饋、失敗標記與殘倉代理懲罰的尺度不同，以及代理斜率可能持續把已獲利且可平倉的一口推向現金邊界。v6 單獨修容量上限，但完整第一組 256 epochs 仍有 96 次訓練失敗，沒有優於 v5 同區間的 79 次。因此容量修正保留為隔離對照，另以 v7 驗證由實際整口效用產生改善方向。

新選項只改訓練反向傳播；資料、進場口數、逐分鐘成交、費稅、帳戶存續、13:30 殘倉判定及驗證選模都沿用同一套執行器。v7 在失敗時比較目前組合與現金的實際效用差，並在已可行、鄰居均無改善時停止推動該動作。

兩個實際案例已驗證容量缺陷。一個來自測試期，一個來自訓練期。訓練期案例更揭露了固定資金重播的不足：必須先按模型實際交易累積資金，再檢查失敗當天的整口門檻。v6 與 v7 均使用新輸出目錄；v5 設定及既有 checkpoint 保留為對照。

工程修正的接受條件是帳務不變、錯誤梯度被修復、共同訓練流程可完成。策略表現的接受條件另外包括驗證與未來資料上的存續、成本後報酬及穩定性。下列結果不能被延伸為獲利保證。

## 實際結果與問題範圍

原始訓練根目錄是
`artifacts/markets/tw_stock_futures_day_trade_0845_from_20200323_v2_contract_quarantine_v2_capacity_ceil_v5_vast5090`。
數據取自 `summary.json`、各 fold 的 `execution_status.json`、回測 NPZ 與各訓練組的 `epoch_curve.jsonl`。可重跑的稽核程式及機器可讀結果在
[audit.py](../artifacts/smoke/futures_v5_deep_audit_20260909/audit.py)、
[audit.json](../artifacts/smoke/futures_v5_deep_audit_20260909/audit.json)。

| Fold | 訓練年 | 驗證年 | 驗證累積報酬 | 測試結果 | 測試進場口數 |
|---|---|---|---:|---|---:|
| 1 | 2020 | 2021 | +0.027894% | 有效，2022–2026 累積 +0.005976% | 4 |
| 2 | 2020–2021 | 2022 | +0.000900% | 2025-04-10 首次殘倉失敗 | 3 |
| 3 | 2020–2022 | 2023 | +0.026214% | 2024-01-17 首次殘倉失敗 | 1 |
| 4 | 2020–2023 | 2024 | 0% | 2025-04-24 首次殘倉失敗 | 2 |

失敗 fold 的數值報酬接近 -100%，源自 `log(1e-7)` 的執行失敗標記。它表示策略未完成指定退出，**不是所有資金已透過實際成交虧損掉的證明**。例如 fold 3 有 11 個失敗前現金日、1 個失敗日、636 個失效後不再執行的日期；後面 636 日不能算成模型主動選擇現金。

| 訓練組 | 已記錄 epochs | 最佳驗證 epoch | 訓練帳戶失效 epochs | 最後一次裁切前梯度範數 |
|---|---:|---:|---:|---:|
| 2020 | 1,075 | 75 | 898 | 20.6684 |
| 2020–2021 | 1,001 | 1 | 837 | 34.5634 |
| 2020–2022 | 1,199 | 199 | 970 | 32.9874 |
| 2020–2023 | 1,001 | 1 | 726 | 21.0006 |
| 2020–2024，未完成 | 682 | 261 | 353 | 16.2144 |

以上曲線中的 `train_zero_grad_batches` 均未記錄零梯度批次。AdamW 每 epoch 更新一次，學習率保持 `0.0003`。這直接排除「完全沒有 backward 或 optimizer.step」作為共同原因，但不能排除某個關鍵交易的梯度為零。

`epochs: 10000` 是最大迭代數。既有 `early_stopping_no_improve_ratio: 0.1` 對應連續 1,000 次未改善即停止，因此 epoch 75 最佳的第一組在 1,075 停止。增加上限同時延長了停滯時間，沒有改變錯誤代理梯度。這個停止行為符合現有設定，並非少跑了指令中的迭代。

## 從單口交易到學習目標

令股期進場價格為 \(P\)、實體契約乘數為 \(M\)、每側佣金為 \(f\)、適用稅率為 \(\tau\)、方向為 \(d\in\{-1,1\}\)。本實驗使用的進場名目金額、進場稅及資金預留是：

\[
N=PM,\quad t_{in}=\lfloor N\tau+0.5\rfloor,\quad C=N+2(f+t_{in}).
\]

模型給股票的權重 \(w\) 是資金 sleeve 的比例；當日資金為 \(E\)，預算是 \(|w|E\)。只有單一契約候選時，口數可以簡化寫為：

\[
q=\min\left(\left\lfloor\frac{|w|E}{C}\right\rfloor,K_{entry}\right),
\quad K_m=\left\lceil 0.5V_m\right\rceil.
\]

實際程式另處理標準／小型的組合選擇及浮點容許誤差，但資金不能超用、口數必須為整數、每分鐘成交不超過 \(K_m\) 的條件相同。**無條件進位只作用於分鐘成交容量，沒有把不足一口的現金預算也進位**。

對每個實際出場 \(j\)，損益是
\(q_j[dM(P_j-P)-f-t_j]\)，再扣全部進場口數的 \(f+t_{in}\)。退出價格和稅額取自當次執行，不以每日收盤補成交。殘倉 \(q-\sum_jq_j\ne0\) 時，該帳戶未滿足退出契約並進入吸收式失效狀態。

帳戶有效時每日對數報酬是 \(\log(1+R_t)\)，完整年度化訓練損失為
\(-252\sum_t r_t/T\)。失敗日使用固定失敗標記，後續真實成交停止。每個 batch 的平均損失按有效日期數占整段日期數加權，再統一裁切梯度與更新一次參數。這保留同一 epoch 內的一致政策，避免將短尾批與完整批次等權平均。

口數對權重是階梯函數；其普通導數幾乎處處為零。非零訓練訊號來自明確選定的代理導數，不是自動微分天然能推導的真實市場敏感度。Bengio 等將這類問題區分為隨機估計、平滑近似與 straight-through estimator；PyTorch 也明確說明不可微運算的梯度規則。[^1][^2]

## 已重現的容量邊界錯誤

v3–v5 以目前整口組合 \(B_0\) 與下一個可行組合 \(B_+\) 的差分產生梯度。設兩者資金成本為 \(c_0,c_+\)，淨損益／資金為 \(R_i\)，殘倉名目金額／資金為 \(D_i\)，則非零權重的代理斜率為：

\[
g_i^+=\frac{d_i}{(c_+-c_0)/E}
\left[\frac{R_i(B_+)-R_i(B_0)}{\max(1+\sum_jR_j(B_0),10^{-7})}
-D_i(B_+)+D_i(B_0)\right].
\]

問題出在容量用滿時：程式找不到 \(B_+\)，把 \(c_+=c_0\)，再將整個 \(g_i\) 設為零。這只能說明「加碼不再增加進場口數」，無法推出「減碼沒有改善」。若最後一口虧損或無法清倉，應保留往更小可行組合移動的訓練方向。

最小例子只有一個標準契約：\(P=100,M=2000,f=40,\tau=0.00002\)，每口需 200,088 元、初始資金 100 萬、進場容量一口、沒有任何合格出場成交。

| 權重 | 實際進場 | 殘倉 | v5 報酬對權重的代理梯度 | v6 |
|---:|---:|---:|---:|---:|
| 0.19 | 0 | 0 | 約 -0.999780 | 相同 |
| 0.25 | 1 | 1 | 0 | 約 -0.999780 |
| 0.90 | 1 | 1 | 0 | 約 -0.999780 |

原版本在尚未買得起一口時知道應避免交易，真正進場且失敗後卻失去回饋。v6 的單參數 SGD 測試能從 0.25 降回不足一口，下一次真實整數執行恢復有效現金帳戶。這是機制證據，不能替代市場策略測試。

### 實際訓練與測試交易

| 案例 | 資金與動作 | 問題 | 舊／新代理梯度 |
|---|---|---|---|
| 第一組最後 checkpoint，2020-07-20，3231 標準候選 | 前一日累積資金 29,296,306；權重 -0.06717847；目前組合成本 1,299,739 | 13:30 殘留空單 9 口，已用滿進場容量 | 0 → +0.999424，增加負權重使其往零縮小 |
| Fold 4，2025-04-24，3105 小型候選 | 單股票重播權重 +0.00122581；組合成本 8,860 | 進場一口、殘留一口；較大組合不存在 | 0 → -0.995485 |
| Fold 2，2025-04-10，3105 小型候選 | 權重 +0.00085807；成本 8,230 | 仍可加一口，故既有差分已有回饋 | -0.995140，這項修正不改它 |
| Fold 3，2024-01-17，3481 標準候選 | 權重 -0.00345285；成本 31,304.22 | 仍可加一口，既有差分已有回饋 | +0.998690，這項修正不改它 |

後三列是保存權重的單股票重播，資金使用初始 1,000 萬，並非宣稱逐值重建整個測試帳戶的所有先前交易。第一列則先完整重播 164 個訓練日期，使用失敗前累積資金定位缺陷。只用初始資金盤點最後模型時沒有任何殘倉格；完整重播卻在 row 48 失敗，顯示資金狀態是不可省略的條件。

## 容量修正的數學定義與實作：v6 對照

新增的 `training.futures_minute_saturation_recovery: true` 只對分鐘整口帳戶、既有 recoverable backward 與完整軌跡更新生效。舊設定預設為 false。

在容量飽和時，先把目前組合資金成本 \(c_0\) 減去一個超過原 allocator 浮點容許誤差的量，再交回**同一個標準／小型 allocator** 找出 \(B_-\)。差額使用其實際資金成本 \(c_-\)，而非任意減掉某個候選的一口。接著仍呼叫原逐分鐘執行器，計算其費稅、出場與殘倉。

\[
g_i^-=\frac{d_i}{(c_0-c_-)/E}
\left[\frac{R_i(B_0)-R_i(B_-)}{\max(1+\sum_jR_j(B_0),10^{-7})}
-D_i(B_0)+D_i(B_-)\right].
\]

容量上限只能往內改善，因此取
\(g_i=d_i\min(d_ig_i^-,0)\)。若最後一口有利且可平倉，梯度維持零，不鼓勵無法增加成交的更大要求；若移除最後一口改善局部代理目標，則保留縮倉梯度。未飽和區域仍使用原本的差分。

反向訊號透過以下零值項接入：

\[
r_{train}=\operatorname{stopgrad}(r_{exact})+
\sum_i(w_i-\operatorname{stopgrad}(w_i))\operatorname{stopgrad}(g_i).
\]

這個項在 forward 為零，因此同一輸入的實際口數、現金、報酬與失效日完全不變。驗證和推論關閉 recovery。訓練結果會因參數更新不同而改變，故 v6 有新的交易指紋及輸出根目錄，禁止把 v5 optimizer 狀態直接續接成 v6。

這仍是局部、帶偏差的代理梯度。殘倉項係數保留為 1，不是對嚴格可行性的完整拉格朗日求解，也不等於 `log(1e-7)` 失敗標記的精確導數。它沒有證明神經網路參數的每一步都改善整條帳戶。失敗後反事實日期仍使用初始資金，batch 間及資金參考仍有 stop-gradient，這些既有近似尚未被消除。

| 層 | 改動 |
|---|---|
| `stockagent/backtest/tw_stock_futures_day_trade.py` | 原 allocator 與 executor 的前一組合差分；容量飽和時保留可行向內梯度；編譯快取區分新舊選項 |
| `stockagent/backtest/simulator.py`、`stockagent/training/loss.py` | 把新選項從共同 loss 傳到原整數執行器 |
| `stockagent/training/trainer.py` | 訓練傳入選項；評估明確關閉；沿用 DDP、AMP、更新與選模流程 |
| `stockagent/config.py` | 預設 false；拒絕不相容 execution/recovery 組合 |
| `stockagent/training/checkpoint_contract.py` | 新語意進入指紋，舊 v5 指紋保持不變 |
| `configs/markets/tw_stock_futures_day_trade_0845_gradient_v6.yaml` | 繼承 v5 的資料、50% ceil 容量與 10,000 epochs，使用新輸出根目錄 |

## 真實效用與可行改善方向：v7

v5 的失敗代價在 forward 是固定 \(-16.118\)，但 backward 使用殘倉名目金額／資金，係數只有 1。一口名目金額 20 萬、資金 100 萬的失敗，代理代價約 0.2；一口名目金額 8,000、資金 1,000 萬的失敗，代理代價只有約 0.0008。兩者都違反清倉契約，forward 卻施加相同失敗標記。這個落差會讓訓練方向與選模懲罰不一致。

另一個最小案例是進場容量兩口、退出只能一口，且第一口實現正損益。原版對目前一口使用「第二口減第一口」的差分，因第二口無法清倉而產生負斜率，持續把目前要求推向現金邊界。v7 直接比較目前、增一個組合、減一個組合與現金：目前一口有利、其餘都更差時，該動作方向應保持零；單參數測試也驗證從零學到一口後可持續保留。

新增 `training.futures_minute_recovery_objective: execution_utility`。對股票 i 的反事實組合 B，其他股票當日淨損益保持固定，定義：

\[
U_i(B)=\begin{cases}
\log(1+\sum_{j\ne i}R_j(B_0)+R_i(B)),&\text{該組合無整口殘倉且資金有效}\\
\log(10^{-7}),&\text{否則}.
\end{cases}
\]

可比較的標籤是下一個組合、前一個組合、現金；目前口數為零時，再加入反方向的第一個組合。所有組合仍使用原 allocator 與原分鐘 executor，不拿未來成交可行性改候選資格。若有正效用改善，選擇效用改善最大的候選，以「改善量／對應資金比例距離」建立方向；若均無改善則為零。

目前組合失敗時，直接使用現金作為可行恢復點。即使前後相鄰組合都失敗、效用同為常數，仍可由現金差分得到縮倉方向。例如前述一口 200,088 元、100 萬資金的案例，v7 的報酬代理梯度為 \(\log(10^{-7})/0.200088\approx-80.555\)，不再是與 forward 脫節的約 -1 或 0。正常梯度裁切仍在累積完整軌跡後進行。

這不是整段帳戶效用的無偏 policy gradient。為讓同一天多個失敗股票都能得到恢復方向，單股票標籤不合併其他股票的殘倉失敗旗標；其他股票損益仍固定保留。失敗後資金參考重設與停止資金反傳的近似也仍存在。因此能證明的是局部候選比較及恢復方向的正確性，不能由此宣稱全域收斂。v7 指紋明確記錄新的失敗效用、容量邊界與局部停駐語意。

在保存的實際動作上，v7 已完成第二次重播：2020-07-20 訓練失敗的 3231，代理梯度由 v5 的 0、v6 的 +0.999424，改為 +363.603；2025-04-24 的 3105 小型一口，則由 0／-0.995485 改為 -18,191.980。這些是尚未套用年度平均及參數梯度裁切前的動作梯度，不能與 epoch 的神經網路梯度範數混為一談。完整數值見 [utility_action_audit.json](../artifacts/smoke/futures_v5_deep_audit_20260909/utility_action_audit.json)。

## 為什麼修好梯度後仍可能低交易或表現差

第一個因素是可執行性。按照目前來源及退出程序，2020 年有 6,246 個可進至少一口的標準契約日，其中 763 個在多空兩個方向都沒有足以成交一口的合格退出；小型為 234 個中 20 個。這分別約為 12.22% 與 8.55%。這是事後候選盤點，涵蓋該年的來源日期，**不是模型條件失敗率**，也沒有拿來建立未來進場遮罩。

第二個因素是目標尺度。用簡化獨立例子表示一次新增交易：成功機率 \(1-p\)、成功時增加對數效用 \(\mu\)、失敗代價 \(K=-\log(10^{-7})\approx16.118\)。期望改善需滿足
\((1-p)\mu-pK>0\)，即 \(p<\mu/(K+\mu)\)。若 \(\mu=0.001\)，門檻約為 0.0062%。這只說明嚴格失敗代價對目標的影響，不能拿前述盤點比例代入，宣稱估得策略的真實預期收益。

第三個因素是可觀測資訊與泛化。目前模型以 98 個上一完整交易日的股票／公開資訊特徵形成 08:45 決策，未直接輸入每個當日期貨契約的未來退出容量。修復代理斜率並不會增加對流動性中斷的預測資訊。第一組只有 164 個有效訓練決策日；反覆優化到能在這些日期取得高收益，不等於後續年度可複製。

第四個因素是固定成本與整口門檻。模型的小權重可能有效降低代理損失，卻仍不足以支付一口所需資金。驗證將其判為現金是正確帳務；直接強迫至少交易一口、提高曝險或把現金預算無條件進位，會變更策略而掩蓋這個問題。

第五個因素是選模。程式按驗證 loss 儲存 checkpoint；測試曲線不參與 optimizer、scheduler 或最佳模型判斷。稀少交易的模型若驗證期稍為獲利、其他模型清倉失敗，前者被選中符合目標。反覆人工檢視同一測試年代也會削弱其作為獨立樣本的地位；本報告將這些年代用於故障診斷，沒有把修正後測試結果當成全新的盲測。

## 與相關方法的差異

| 方法／系統 | 解決的問題 | 本次可採用的部分 | 不能直接沿用的結論 |
|---|---|---|---|
| 本專案股票日內基準 | 股票的持倉、費率、交割與輸入尺度 | 共用訓練器、逐特徵訓練期正規化、日期加權更新 | 股票成交與交割契約不能替代股期整口與分鐘退出 |
| 本專案跨日整數期貨 | 持倉延續、保證金及 recurrent account | 同一政策走完整軌跡；檢查真實資金狀態 | 跨日可持有部位不能拿來放寬 13:30 清倉 |
| Bengio 等 STE | 離散／硬閾值的不可微問題 | 明確區分 forward 與梯度估計器，驗證估計方向 | 非零梯度本身不能證明估計正確或策略收斂 [^1] |
| Yin 等量化網路分析 | 特定模型下 coarse gradient 與真梯度的關係 | 同時檢查方向、飽和邊界與失穩 | 兩層、二元啟動及高斯資料的理論不能推成股期收斂保證 [^3] |
| Vlastelica 等黑箱組合求解器 | 保留原離散 solver 的有效反傳 | 由同一 solver 產生相鄰可行結果，避免另寫近似成交器 | 論文針對線性目標的 solver；本系統還有預算、費用、時序與失敗狀態 [^4] |
| Deep Hedging | 含交易摩擦的避險策略學習 | 把成本與可行性納入目標／執行 | 模擬市場避險成功不等於真實股期當沖有 alpha [^5] |
| No-Transaction Band Network | 避險的動作相依及交易成本 | 利用問題結構；論文也指出 clamp 飽和區的零梯度困難 | 持有避險部位的不交易區間，不是每日強制歸零的當沖策略 [^6] |

No-Transaction Band Network 的第 4.1 節特別提出在 clamp 飽和區保留小斜率以利反傳。這與本次需要查驗「飽和不代表沒有可行改善」的動機一致；v6 採實際整口組合的縮倉差分，沒有照抄任意常數斜率或修改 forward 成交。[^6]

## 資料、交易規則與驗證界線

本次資料預檢接受 2020-03-23 至 2026-09-04 的 1,573 個 panel sessions。來源仍是 receipt-backed 一分鐘 KBars，容量為 `ceil(0.5 * volume)`；沒有下載 tick，沒有補造分鐘成交，也沒有增加隔離範圍。既有三個 contract-day 排除保持為 `2021-06-21/LVF:202107`、`2023-07-13/PZF:202308`、`2024-01-10/LIF:202401`。早期日線代理 cutoff 保留於 ABI，但本次日期區間沒有早期代理行。

策略時鐘維持 08:45 決策、08:46 右標記棒進場；13:20 用已完成價格掛限價，13:24 改單，13:25–13:30 使用市場出場程序。價格、乘數、費稅與容量都依同一份有實體契約身分的資料。清倉失敗不會用每日 CLOSE、期後成交或隔夜持倉掩蓋。

官方「當沖保證金減收」與本次「50% 分鐘成交容量」是不同參數。期交所現行結算問答仍明示股票期貨未納入該減收制度；既有全名目資金 sleeve 也是研究資金約束，不應冒稱交易所歷史保證金。這次沒有將資金需求減半、增加槓桿，或拿官方制度重新解釋策略指定的清倉期限。[^7]

### 驗證證據

- [聚焦測試記錄](../artifacts/smoke/futures_v5_deep_audit_20260909/tests.log)：137 passed，包含 CUDA eager/compiled 帳務及梯度一致性。
- [較廣回歸記錄](../artifacts/smoke/futures_v5_deep_audit_20260909/regressions.log)：355 passed、14 deselected；此輪排除另已驗證的 compiled/CUDA cases。
- [v7 聚焦驗證](../artifacts/smoke/futures_v5_deep_audit_20260909/utility_final_tests.log)：15 passed，包含隨機標準／小型帳戶、共同 loss、checkpoint 防護及 CUDA compiled/eager。
- [v7 共同回歸](../artifacts/smoke/futures_v5_deep_audit_20260909/utility_regressions.log)：368 passed、15 deselected；其後新增的隨機帳戶個案另於聚焦驗證通過。測試集合重疊，不能相加成獨立案例總數。
- [新測試](../test/test_futures_saturation_gradient.py)：多空、恰達門檻／超出門檻、飽和虧損、飽和獲利、部分清倉、現金、padding、共同 risk loss 接線、optimizer 退出失敗區及不相容 checkpoint。
- [資料預檢](../artifacts/smoke/futures_v5_deep_audit_20260909/preflight.log)：全部 1,573 日期與既定來源範圍接受。
- 嚴格 CUDA 環境檢查通過：PyTorch 2.11.0+cu128、兩張 RTX 5090；模型 BF16，帳務及數值敏感運算 FP32。AMP 梯度累積應在統一更新前完成，與 PyTorch 的累積規則一致。[^8]
- 直接比較既有 v5 checkpoint 與目前設定的交易／訓練契約，兩者均相等；啟用 v6 則明確拒絕跨語意 resume。

### 兩個 fold 的共同訓練器對照

對照使用相同 seed、資料、模型、BF16/DDP、batch 128、費稅、容量、退出程序與固定學習率。v5 取原訓練各組前 256 epochs；v6 從新目錄開始，跑完整兩個 fold、各 256 epochs。限定實驗關閉 early stopping，以完成預先指定的比較長度；沒有關閉每輪驗證／測試曲線，也沒有縮短資料年代。

v5 與 v6 的同範圍 256-epoch 對照已完成：

| 訓練期間 | v5 失敗 epoch | v6 失敗 epoch | v5 最佳驗證 epoch | v6 最佳驗證 epoch |
|---|---:|---:|---:|---:|
| 2020 | 79/256 | 96/256 | 75 | 74 |
| 2020–2021 | 92/256 | 81/256 | 1 | 1 |

v6 第一 fold 測試在 2026-03-26 清倉失敗；第二 fold 與 v5 相同，仍在 2025-04-10 失敗。容量邊界梯度修正有單元證據，卻沒有帶來可靠的策略改善。完整數值、曲線與資料分別見 [comparison.json](../artifacts/smoke/futures_v5_deep_audit_20260909/comparison.json)、[comparison.png](../artifacts/smoke/futures_v5_deep_audit_20260909/comparison.png)、[comparison.csv](../artifacts/smoke/futures_v5_deep_audit_20260909/comparison.csv)。

使用者隨後明確改為「無法平倉就留倉，隔日依差額調整」。因此 v7 的嚴格平倉對照已中止，原始曲線與 checkpoint 保留；不能把它列為完成的兩 fold 比較。最新實作與驗證改見[剩餘部位留倉帳務](FUTURES_RESIDUAL_CARRY_2026-09-09.md)。下方 v7 指令僅保留作舊規則的重現紀錄。

## 執行方式

正式設定保留 10,000 epochs 上限，沿用共同入口：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_gradient_v7.yaml
```

新輸出目錄為
`artifacts/markets/tw_stock_futures_day_trade_0845_from_20200323_v2_contract_quarantine_v2_gradient_v7_vast5090`。
既有 v5 結果不會被這個指令覆寫。10000 是上限，原 early stopping 比例仍為 0.1。

本次限定驗證指令為：

```bash
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_gradient_v7.yaml \
  --output-dir artifacts/smoke/futures_v5_deep_audit_20260909/v7_256 \
  --epochs 256 --max-folds 2 --early-stopping-no-improve-ratio 0 --no-resume
```

## Sources

[^1]: Bengio, Y., Léonard, N., Courville, A. [Estimating or Propagating Gradients Through Stochastic Neurons for Conditional Computation](https://arxiv.org/abs/1308.3432), 2013。離散動作梯度估計器的分類；本次沒有援引為金融收斂定理。
[^2]: PyTorch 2.11，[Autograd mechanics — Gradients for non-differentiable functions](https://docs.pytorch.org/docs/2.11/notes/autograd.html#gradients-for-non-differentiable-functions)。依本地 PyTorch 2.11 查閱對應版本。
[^3]: Yin, P. et al. [Understanding Straight-Through Estimator in Training Activation Quantized Neural Nets](https://arxiv.org/html/1903.05662v4), ICLR 2019。第 2–3 節假設與不當 STE 的失穩界線。
[^4]: Vlastelica, M. et al. [Differentiation of Blackbox Combinatorial Solvers](https://arxiv.org/html/1912.02175v2), ICLR 2020。線性目標之離散 solver 與插值反傳。
[^5]: Bühler, H., Gonon, L., Teichmann, J., Wood, B. [Deep Hedging](https://arxiv.org/abs/1802.03042), 2018。成本、流動性與風險限制下的避險學習框架。
[^6]: Imaki, S. et al. [No-Transaction Band Network: A Neural Network Architecture for Efficient Deep Hedging](https://arxiv.org/html/2103.01775v1), arXiv 2021，後刊於 Journal of Financial Data Science。第 4.1 節的架構、clamp 飽和與梯度提示。
[^7]: 臺灣期貨交易所，[交易人最常詢問之問題及解答—結算面](https://www.taifex.com.tw/cht/9/tradersQAClearing)，2026-09-09 查閱。股票期貨不適用當沖減收保證金的問答。
[^8]: PyTorch 2.11，[Automatic Mixed Precision examples — Gradient accumulation](https://docs.pytorch.org/docs/2.11/notes/amp_examples.html#gradient-accumulation)；[AdamW](https://docs.pytorch.org/docs/2.11/generated/torch.optim.AdamW.html)。梯度累積、更新與 optimizer 狀態規則。

本地證據來源另包括前述 v5 原始 artifact、v6 限定對照 artifact、稽核 JSON／NPZ、測試記錄及版本化設定。其價格與績效數字不是從上述文獻推估。
