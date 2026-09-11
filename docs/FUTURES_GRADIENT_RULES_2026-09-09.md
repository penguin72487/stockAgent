# 股票期貨當沖：官方規則、整數帳務與梯度修正 v3

後續空手停滯已另做輸入尺度診斷與 v4 對照，見
[空手模型與逐特徵正規化修正](FUTURES_CASH_POLICY_AUDIT_2026-09-09.md)。
本文件保留 v3 當時的原始結果；v4 沿用其整數帳務與代理梯度。

查核日：2026-09-09。研究對象是 `tw_stock_futures_day_trade_0845` 的個股期貨，
不是臺指期，也不是現股當沖。使用者再次指定：**每日 13:30 必須清倉**。
這個策略約束已保留；沒有把失敗部位改為隔夜持有，也沒有把日收盤價當作必然成交價。

此次實作修正三件事：以同一整數成交引擎的相鄰組合損益取代混合口數代理梯度；
讓失敗帳戶後續日期仍能提供訓練用的反事實梯度；沿用既有完整日期軌跡累積梯度的
更新方式。**實際 forward、測試與帳戶失敗仍維持原契約**。

兩組各 128 epochs 的完整第一 fold 對照已完成。v3 訓練帳戶有效的 epochs
由對照的 7/128 增為 127/128，但最佳模型仍是空手、測試零報酬。
參數與 Adam 動量確實有更新；**尚未證明學到可用的交易優勢**。

前次缺棒梯度修正與原六組訓練的證據見
[v2 稽核](FUTURES_GRADIENT_AUDIT_2026-09-09.md)。本報告將官方事實、
本專案的模擬約束、代理梯度的數學選擇及實驗結果分開，不把其中一項當成另一項的證明。

## 1. 官方規則查到什麼，計算應如何處理

| 問題 | 查核結果 | 本次處理 |
|---|---|---|
| 股票期貨當沖是否保證金減半？ | 不適用。交易所列出的制度限 TX、TE、TF、MTX 指定近月契約 | 不將股票期貨所需資金乘 0.5，也不因此把口數加倍 |
| 所有期貨每天 13:30 必須已平倉？ | 股票期貨通常至 13:45；到期契約最後交易日至 13:30。減收保證金制度的 13:30 是停止新倉、開始代沖銷的時段界線 | 仍遵守使用者更嚴格的每天 13:30 清倉約束 |
| 市價委託是否保證在期限內成交？ | 委託仍需要可成交對手；市價 IOC/FOK 並不創造市場容量 | 使用已驗證一分鐘資料，保留未成交口數與失敗狀態 |
| 股期保證金是否等於契約全額？ | 依契約價值及該標的適用的保證金比率計算，期貨商可能加收 | 現有全名目金額 cash sleeve 明確標為研究曝險／資金限制，不冒稱歷史保證金帳戶 |
| 費稅如何計算？ | 股票期貨交易稅依每次交易契約金額計；經紀佣金須依實際費率 | 保留每側 40 元的實驗假設與逐口逐側稅額，沒有把 40 元稱為法定統一佣金 |

交易所的結算問答直接區分股票期貨與適用減收的商品；制度對適用金額乘 50% 後，
還須按千元進位，並非所有期貨均可直接除以二。
[期交所結算問答](https://www.taifex.com.tw/cht/9/tradersQAClearing)、
[官方問答 PDF，第 15 題](https://www.taifex.com.tw/chinese/9/QA_trader.pdf)。

2022-07-25 官方作業規定列出減收適用商品、比例，以及一般契約
13:00–13:30 通知、13:30–13:45 代沖銷的程序。
其後另有未沖銷部位、全額保證金與追繳處理。因此「13:30 留一口」不能直接被描述成
交易所認定全部資金歸零。本策略的吸收式失敗是**未滿足指定退出契約**。
[期交所當沖作業規定及注意事項](https://www.taifex.com.tw/chinese/11/attach/111%E5%B9%B47%E6%9C%8825%E6%97%A5%E5%8F%B0%E6%9C%9F%E7%B5%90%E5%AD%97%E7%AC%AC1110002405%E8%99%9F.pdf)。

個股期貨的標準／小型乘數、一般交易時間及到期日時間應以個別商品規格為準；
契約調整後不能只靠股票代號推定乘數。本專案仍使用有日期及實體契約身分的執行資料。
[股票期貨契約規格](https://www.taifex.com.tw/cht/2/sTF)。

股票期貨保證金原則為 `價格 × 乘數 × 適用比率`，並依規定取整；
目前一般股票級距的原始保證金比率為 13.5%、16.2%、20.25%，亦有較高的個別風險比率。
**當前比率不能回填成 2020 年起的歷史事實**。本次未找到足以重建每個契約日、
每次調整及經紀加收的本地歷史保證金來源，因此保留原曝險限制，沒有新增缺乏來源的槓桿。
[期交所股票期貨保證金訂定](https://www.taifex.com.tw/cht/5/margingReqSSF)。

股票期貨稅率為契約交易金額的十萬分之二，計算須保留逐口取整的粒度。
交易所收費表與經紀商向客戶收取的總佣金是不同層次。
[財政部股票期貨交易稅問答](https://www.etax.nat.gov.tw/etwmain/tax-info/understanding/tax-q-and-a/national/future-transaction-tax/filing-payment-and-collection-reward/jDxbO8a)、
[財政部稅額計算規定](https://law-out.mof.gov.tw/LawContent.aspx?id=GL006626&media=print)、
[期交所交易及結算費用](https://www.taifex.com.tw/cht/4/feeSchedules)。

分鐘成交量乘以 50%、用完成棒 VWAP 模擬市價成交、限價要求嚴格穿越，
都是本專案的執行估計，不是交易所保證。正式委託還涉及優先順位及每筆口數上限；
分鐘總量無法證明每筆排隊均可成交。
[期交所委託制度](https://www.taifex.com.tw/cht/4/oamIntroduction)、
[委託口數限制](https://www.taifex.com.tw/cht/4/oamOrderlimit)。

## 2. 從一口期貨推導帳務，再決定可以微分什麼

令開倉價為 P、契約乘數為 M、開倉名目金額 N=PM、每側佣金 f、
當日稅率 τ、方向 d∈{-1,+1}。每口進場稅及現有研究用資金預留為：

```text
tax_in = floor(N × τ + 0.5)
C = N + 2 × (f + tax_in)
stock_cash_budget = |w| × E
```

C 是**目前模型的全額資金 sleeve**，不是上節的交易所保證金。
出場後實際每口損益是 `d × M × (P_exit − P) − 2f − tax_in − tax_exit`。
出場稅按實際成交價重新計算，不能把預留稅直接當作真實出場稅。

共同整數分配器只在每個股票自己的 cash sleeve 內比較三類組合：
標準優先搭配小型餘額、少一口標準後補小型、僅小型。
它在這三類可行組合中選最大名目金額；**沒有聲稱求解所有口數組合的全域整數最佳化**。
剩餘現金不跨股票重新分配。此次不改這個既有 forward 選擇規則。

整數口數是階梯函數，真正的無窮小導數幾乎處處為零。
不能同時要求「完全不變的整數 forward」與「它天然具有非零的普通導數」。
straight-through estimator（STE）必須是一個明確、可測試的代理選擇，
不能把任何非零梯度都當作正確訊號。
[Bengio 等，2013](https://arxiv.org/abs/1308.3432)。

前次 v2 修掉 `min(remaining=0, capacity=0)` 在不可成交分鐘分配梯度的缺陷。
PyTorch 在不可微點依運算選擇次梯度，這與實際市場是否可成交無關。
[PyTorch autograd 的不可微函數說明](https://docs.pytorch.org/docs/main/notes/autograd.html)。
但是 v2 還有三個限制：

1. 標準與小型共用 `sum(reserve)` 分母，連進場容量為零的標準契約也可能稀釋小型梯度。
2. 對 `abs/sign` 直接傳遞梯度會在全零輸出卡住；盲目改成多空對稱差分又會抵消固定費用。
3. 帳戶一旦失敗，後續日期全部是失效帳戶的常數輸出；後續可執行機會不再參與學習。

這些是代理目標與狀態傳播的問題；保證金除以二不能修正它們。

## 3. v3 的代理梯度：相鄰整數組合，而非混合分數口數

對每個股票 i，先用**進場時點資訊**與原分配器求目前組合 B0。
令 c0 是 B0 所需資金，c1 是目前預算之後、同一分配器分支的下一個可行資金門檻。
本次全分鐘區間的未來出場價量不能參與 c0/c1 或候選資格的形成。

接著用原 `_scheduled_futures_day`，分別執行 B0、c1 的同方向及反方向組合。
以 `torch.vmap` 向量化原函式，沒有另外抄一份費稅或成交引擎。
未來價量在此只擔任**訓練結果標籤**，不進入決策特徵或進場遮罩。

定義 R_i(B) 為該股票組合的實現淨損益／參考資金，
D_i(B) 為 13:30 殘倉的開倉名目金額／參考資金，h_i=(c1−c0)/E。
對非零權重，局部代理梯度為：

```text
sR_i = sign(w_i) × [R_i(B1) − R_i(B0)] / h_i
sD_i = sign(w_i) × [D_i(B1) − D_i(B0)] / h_i
g_i  = sR_i / max(1 + sum_j R_j(B0), 1e-7) − sD_i
```

這是對局部線性化的日損益套用 log utility，再減去殘倉曝險懲罰。
殘倉係數沿用既有 failure shadow 的 1；它是**訓練約束代價**，不是法定費用，
不是假設成交價格，也不是證明必定能消除殘倉的拉格朗日乘數。

在 w_i=0 時，分別計算多、空相鄰組合相對現金的單側改善斜率，
包括固定費稅及殘倉代價。選有改善的方向；若兩邊都不改善，梯度為零。
這避免把完全空手變成無法離開的陷阱，也避免以「固定費用相消」鼓勵虧損開倉。
沒有下一個可行進場門檻時，採零梯度；達到容量上限後的飽和區仍可能有平台，
沒有任意外推無限容量。

寫入回傳值時只加一個 forward 為零的項：

```text
r_train = stop_gradient(r_exact)
          + sum_i [(w_i − stop_gradient(w_i)) × stop_gradient(g_i)]
```

因此 r_train 的數值仍是原本的整數報酬／失敗懲罰。
它的 backward 是上述代理斜率，**不是聲稱求得原整數總終值的精確梯度**。
STE 的理論正確性需要限定假設；量化神經網路文獻中的結果不能直接移植成這個市場
及整數分配器的收斂保證。
[Yin 等，2019](https://arxiv.org/abs/1903.05662)。

### 首次殘倉失敗後如何學習

真實帳戶的 alive、equity、口數、殘倉、default 與後續停止執行完全照舊。
訓練仍可把後續日期各自放在參考資金下，詢問模型輸出會得到什麼整數結果：
帳戶有效時使用當日真實開盤資金；失效後只在 backward 參考中使用初始資金。

這不會重新啟動真實帳戶，也不能把反事實收益併入績效。
validation/inference 明確關閉 recovery，且沒有 autograd 時 executor 不建立此梯度路徑。
`log(1e-7)` 仍表示執行契約失敗；報表中的接近 -100% 不能當成實際已平倉的財務損失。

## 4. 參數更新：同一政策必須走完整條日期軌跡

原本逐 batch 更新，下一批使用已改變的政策，卻接續上一批的帳戶。
而把各 batch 平均 loss 再平均，會讓較短尾批取得不成比例的權重。

本次啟用專案已有的 `futures_portfolio_optimizer_step_per_trajectory`：

```text
L_epoch = sum_b [(valid_dates_b / total_valid_dates) × L_batch_b]
```

同一 epoch 的全部 chronological batches 使用相同參數；各批 backward 累積後，
做一次梯度裁切、一次 optimizer.step 與一次 step 型 scheduler 更新。
BF16 保留，GradScaler 不啟用；完整日期加權包括不足一批的尾端。
[PyTorch 梯度累積與 AMP 範例](https://docs.pytorch.org/docs/main/notes/amp_examples.html)。

這沿用共同訓練器，沒有建立另一個訓練入口。
batch 間帳戶狀態仍 detach，v3 每日斜率也將參考資金視為常數，
不反傳後續資金變動對更早交易的影響。因此仍有截斷反向傳播與局部資金參考的近似，
不能宣稱是穿過整段歷史所有資金依存的完整 policy gradient。
更新頻率改變後，1000 epochs 的 optimizer step 數也改變，需以新實驗驗證；
不沿用舊 optimizer 動量假裝無縫續訓。

## 5. 驗證設計與市場可行性

證據目錄：`artifacts/smoke/futures_gradient_v3_20260909/`。

| 驗證 | 證據／接受條件 |
|---|---|
| Forward 不變 | `verify_forward.py` 對保存的 `ledger_v2.py`；128 個循環帳戶、10,240 個日期×股票×候選格，所有回傳欄位 bit-exact 相等 |
| 梯度有限 | 同一隨機稽核的 5,120 個權重梯度均有限；不推進日期保持零梯度 |
| 解析梯度 | 多空、標準不可進場／僅小型、第二口不同退出價、空手、全費稅後兩向都虧損、完全無出口 |
| 有意義的學習 | 可控制獲利分鐘資料上，從零部位的單參數模型跨過一口門檻，改善真實整數報酬；僅為機制測試 |
| 狀態傳播 | 同一失敗帳戶切成兩批及一次執行的 forward／梯度相符，失敗後實際成交仍為零 |
| 更新頻率 | batch 1、2、3、4，含 3+1 尾批；與一次計算的日期平均梯度相符，每段只更新一次 |
| CUDA | strict 環境檢查；compiled/eager 的所有帳務欄位與梯度相符 |
| ABI | v2 與 v3 資料／模型 fingerprint 相同，交易／訓練 fingerprint 不同；拒絕跨版本 optimizer resume |
| 設定防護 | 禁止 minute surrogate-only forward；recovery 必須配完整日期更新；保留 1000 epochs |

解析範例：100→110、乘數 2000、每側佣金 40、逐側稅 4、資金 1,000,000。
第一口預留 200,088，多單淨利 19,912。
從空手到第一口的改善斜率是 **19912/200088**，不是忽略成本的 20000/200088。
若只有小型可以進場，使用其自己的 10,080 預留及 920 淨利，
不把容量為零的標準契約資金混入分母。

`market_geometry.py` 從同一完整執行資料做事後可行性盤點，範圍為
2020-03-23–2026-09-04、1,573 日、2,753 股票：

| 候選類型 | 能進至少一口的契約日 | 多空都無法於指定退出程序成交一口 | 比例 | 以進場價估算的來回費稅中位數／名目金額 |
|---|---:|---:|---:|---:|
| 標準 | 53,389 | 8,688 | 16.273% | 0.04131% |
| 小型 | 10,656 | 1,107 | 10.389% | 0.08808% |

這是**目前候選資料、50% 分鐘容量與限價／市價程序下**的契約日盤點，
不是模型失敗機率，也不是整個期貨市場的估計。
沒有把這些未來結果變成進場篩選條件，沒有因此新增 quarantine。
費稅欄按進場價估計來回成本，實際出場稅仍會隨成交價變動。
模型選擇現金可能與成本和退出可行性有關；僅有非零梯度不足以否定空手結果。

### 16-epoch 對照：梯度持續存在，但仍未建立交易優勢

兩組都使用完整股票輸入、seed 42、同一 financial_transformer、BF16、雙 RTX 5090、
global batch 128、2020 訓練／2021 驗證／2022–2026 測試。
每個 epoch 有 164 個有效訓練日期，分為 128+36；兩組都每 epoch 更新一次。
兩組第一個 epoch 的原始 forward 完全相同：首次失敗 row 49、train loss 24.7668285。
其後政策分歧才反映不同代理梯度與 optimizer 動量。

| 指標 | v2 梯度＋完整日期更新 | v3 梯度＋完整日期更新 |
|---|---:|---:|
| 實際 epochs／optimizer steps | 16／16 | 16／16 |
| 訓練帳戶有效的 epochs | 6／16 | 15／16 |
| 最後 train loss | 24.765976 | 0 |
| 最後裁切前累積梯度範數 | 0.793601 | 1.521141 |
| 最佳 validation loss | 0 | 0 |
| 最佳模型 test 非零成交口數格 | 1 | 0 |
| 最佳模型 test 累積報酬 | -0.000800% | 0% |
| test 殘倉失敗 | 0 | 0 |
| 完整 fold lifecycle | complete | complete |

v3 第 2–16 epoch 的整數訓練報酬為零，仍有有限非零代理梯度。
這證明梯度與真實成交可分開觀察，**不能把零損失或存活率當成有 alpha**。
兩組的最佳驗證都沒有超越現金。對照圖與全部逐 epoch 數值由
`compare_runs.py` 產生，原始 CSV 為 `epoch_comparison.csv`。

![同更新頻率的 16-epoch 對照](../artifacts/smoke/futures_gradient_v3_20260909/comparison.png)

為檢查「是否只是短程還未跨過整口門檻」，另以新輸出目錄執行兩組各 128 epochs，
取消短程 early stopping。16 與 128 epochs 的 scheduler 總步數不同，
只在相同預算的兩組之間作因果比較。

### 128-epoch 對照與實際參數更新

兩組均完成原訓練器的完整驗證、測試、checkpoint、逐 epoch 圖及九張根目錄
walk-forward 圖；root lifecycle 都是 `complete`。資料 fingerprint 四次一致：
`c47c3676d5b939431e998c0406ea55ae9cd94d0d36154611d4c446d96d382c3b`。

| 指標 | v2 梯度＋完整日期更新 | v3 梯度＋完整日期更新 |
|---|---:|---:|
| 實際 epochs／optimizer steps | 128／128 | 128／128 |
| 訓練帳戶有效的 epochs | 7／128 | 127／128 |
| 最後 train loss | 24.766829 | 0 |
| 最後裁切前累積梯度範數 | 0.0970551 | 4.7762661 |
| 最佳 validation loss | 0 | 0 |
| 最佳模型 test 非零成交口數格 | 1 | 0 |
| 最佳模型 test 累積報酬 | -0.000800% | 0% |
| test 殘倉失敗 | 0 | 0 |

v3 第 2–128 epoch 的整數訓練損失持續為零；沒有因多訓練一些就出現優於現金的
驗證模型。因此已排除「完全沒有參數更新」這個具體疑點，卻不能排除代理梯度偏差、
成本與流動性、模型可辨識性、整數平台及最佳化設定對空手結果的影響。
本次保留 cash sleeve、模型架構與正式 1000-epoch 預算，沒有用強迫交易或事後篩選
製造改善，也沒有聲稱 128 epochs 能證明不存在可獲利策略。

`verify_optimizer_updates.py` 直接檢查保存的 checkpoint：
兩組 97 個 Adam state slots 都記錄 step=128，第一動量均非零，模型及 optimizer
張量均有限；97 個模型張量相對最佳驗證 checkpoint 有變動。
v3 最後輸出層 `score_head.1.weight` 的差值 L2 為 0.0167420，bias 為 0.00150678。
GradScaler state 為空，符合 BF16 設定。這些是實際更新證據，並非只看 loss 的推測。

![同更新頻率的 128-epoch 對照；有效帳戶包含空手](../artifacts/smoke/futures_gradient_v3_20260909/comparison_128.png)

全部 128 epochs 的數值位於 `epoch_comparison_128.csv`；
彙整與分階段時間位於 `comparison_128.json`，參數證據位於
`optimizer_update_verification.json`。重建指令：

```bash
bash artifacts/smoke/futures_gradient_v3_20260909/run_extended.sh
source scripts/runtime_env.sh
run_fintech_python artifacts/smoke/futures_gradient_v3_20260909/compare_runs.py --epochs 128
run_fintech_python artifacts/smoke/futures_gradient_v3_20260909/verify_optimizer_updates.py
```

計算成本也增加：在此次已暖機的 128-epoch 比較中，第 2–128 epoch 的中位訓練時間
為 v2 0.494 秒、v3 0.687 秒；以每段 164 個有效日期計，分別約 332.0、238.8 日／秒。
包含逐 epoch 驗證／測試與記錄的中位 epoch 時間為 0.828、1.019 秒。
首次 16-epoch v3 工作的第一 epoch 為 46.45 秒，後來重用編譯快取的 128-epoch
第一 epoch 為 15.06 秒；不能把不同快取狀態當成演算法加速。
完整 fold 的保存／作圖另有各自計時檔，這些 epoch 時間不冒充整個命令 wall time。

這是同一個第一 fold 的工程與探索性對照，尚非六個 folds 的正式 1000-epoch 結果。
目前必須分開驗收：已修正可重現的梯度／更新缺陷；實際交易優勢仍未通過驗證。

核心驗證 `final_focused_tests.txt` 共 **120 passed**；
較早的相關資料／帳務／checkpoint suite 為 335 passed，兩者有重疊，不相加冒稱獨立測試數。

### 資料驗證的獨立限制

原始 tw-public 精確 release 為
`tw-public-20260906T151104843336596Z-l0-penguin-246aab6e5c72e427`。
`materialized_source_verification.json` 證明既有 READY、未到期租約、114,841 個檔案的
逐檔 hash、inventory 及整樹 fingerprint。短程使用這份未變更的熱資料。

重新執行 cold `use` 時另發現 36 個缺失 payload，抽查 Syncthing global record 有刪除標記，
不能把 idle／needBytes=0 當成此 release 可重建的證明。
本次已停止自己的等待工作、恢復自己的 hydration exception，**沒有刪除 payload**；
`source_lease.json` 記錄的是中止原因，不能當作冷資料驗證成功。
根因與完整冷副本復原未在此梯度修改中宣稱完成；現有熱資料的完整性已獨立驗證。

## 6. 使用與重現

新設定：`configs/markets/tw_stock_futures_day_trade_0845_gradient_v3.yaml`。
新交易梯度契約：`exact_integer_forward_adjacent_basket_recoverable_shadow_v3`。
只改實驗名稱、輸出位置與共同訓練器的 recovery／trajectory 兩個開關。
原資料 pins、98 特徵、TWD 10M、整數口數、08:45／08:46／13:20／13:24／13:30、
唯一 `2021-06-21 / LVF:202107` quarantine 及正式 1000 epochs 都繼承保留。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_gradient_v3.yaml
```

正式輸出為
`artifacts/markets/tw_stock_futures_day_trade_0845_from_20200323_v2_contract_quarantine_v1_gradient_v3_vast5090`。
此稽核只執行有界短程，沒有替使用者啟動正式 1000-epoch 工作。
原始訓練與 v2 checkpoint 保留。詳細實作入口為
`stockagent/backtest/tw_stock_futures_day_trade.py`、共同 `simulator.py`／`trainer.py`、
設定驗證及 `checkpoint_contract.py`；測試集中於
`test/test_tw_stock_futures_gradient_recovery.py`、`test/test_futures_trajectory_optimizer.py`、
`test/test_checkpoint_manifest.py`。
