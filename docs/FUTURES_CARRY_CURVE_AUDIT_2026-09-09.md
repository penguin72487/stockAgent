# 個股期貨留倉 v8 異常曲線：來源、帳務與梯度稽核

本次直接檢查使用者的 `artifacts/markets/tw_stock_futures_0845_carry_v8_vast5090`。結論是：**有可重現的程式錯誤，但不能把所有績效不佳都歸因於梯度消失。** 第二組訓練在 139 個資料失效的 epoch 仍更新模型；原始日報其實有部分被遺漏的官方結算價；空手附近的數量 STE 漏掉進場費用的邊際影響；留倉後的純現金除息權益調整也尚未入帳。v9 已實作對應修正。完整歷史仍存在持倉行情與調整型契約來源缺口，修正程式不等於已取得完整來源或證明策略獲利。

沿用共同 `train.py`、FinancialTransformer、98 個先前已完成股票特徵、BF16、雙卡 DDP、年度 walk-forward、精確整口帳務與每軌跡一次 optimizer 更新。保留 08:45 決策、08:46 分鐘入場、13:20 限價、13:24 改單至 13:30；未成交實體部位延至下一交易日，交易新目標與舊持倉的差額。容量仍為 `ceil(0.5 × 分鐘成交口數)`，沒有新增日期或合約隔離，也沒有下載 tick。

## 實際曲線說明了什麼

以下資料來自此次重新讀取的原始 JSONL；並非較早文件中的 128-epoch 對照實驗。

| 訓練資料組 | 已記錄 epochs | 資料失效 epochs | 失效後仍更新 optimizer | 零梯度 epochs | 最後 train loss |
|---|---:|---:|---:|---:|---:|
| 2020 | 295 | 0 | 0 | 0 | −3.243263 |
| 2020–2021 | 163 | 139 | 139 | 0 | +8.647752 |

第一組梯度裁剪前範數約 0.616–30.352，第二組約 0.518–19.896。這些數字反駁「整段完全沒有梯度」，但非零梯度本身不能證明方向正確。第二組失效原因均為 `held_settlement_missing`；例如 epoch 15 的 loss 為 9.940122，當次第一個失效位於訓練列 328，optimizer 仍更新一次。

![未平滑的原始訓練、驗證與抽樣測試曲線；紅點為資料失效但仍更新的 epoch](../artifacts/diagnostics/futures_carry_v9_20260909/review/curves.png)

圖中保留原始數值與兩組各自的尺度，未用平滑掩蓋異常。右側 `test_mean` 是既有訓練流程的抽樣測試曲線，不代表每個 epoch 都完成所有正式測試區間。可重算資料：[curves.csv](../artifacts/diagnostics/futures_carry_v9_20260909/review/curves.csv)、[audit.json](../artifacts/diagnostics/futures_carry_v9_20260909/review/audit.json)。

第一個正式 fold 的舊測試報告接近 −100%，實際上在 **2026-09-01** 持有 `VNF:202609`（1519，小型，空 2 口）時缺少該實體合約分鐘來源。程式保留最後有效權益比例 0.8787387，卻把失效日回報寫成 `log(1e-7)`，後續停止交易並填零。故完整期間金融報酬是**無法計算**；截至 2026-08-31 的舊研究估值前綴約 −12.1261%。該前綴仍未補純現金除息，不是修正後績效，也不能拿來與完整測試期間直接比較。

在已保存的這段測試持倉中，找到一筆純現金除息舊倉：2023-03-13、6770、多 1 口、每股現金 0.3；沒有觀察到已持有的非現金調整事件。因此除權合約轉換確實是帳務缺口，但**不能把它宣稱為這次 VNF 缺分鐘或全部失效 epoch 的已證實成因**。

## 先定義帳戶，再定義 loss

以 `q` 表示帶正負號的實際口數、`M` 表示每口契約價值，`E` 表示帳戶權益。沒有交易或公司行動時，留倉一天的損益為：

```text
PnL_t = q_(t-1) × (M_t − M_(t-1))
E_t   = E_(t-1) + PnL_t
```

交易則按真實成交口數與成交價加入損益、手續費及期交稅；只交易 `target − held`，同一分鐘所有增減合計不得超過容量。到期清除部位必須有正式最後結算證據，沒有成交不能虛構平倉。

對所有權益均有定義且為正的軌跡，共用 log-utility 為：

```text
L = −(252 / T) × Σ log(E_t / E_(t-1))
```

缺少持倉價格，表示 `E_t` 未知，而不是已知等於零。舊程式插入的 `log(1e-7) ≈ −16.1181` 會憑空增加巨大 loss；例如 T=408 時僅這個常數就貢獻約 +9.9553。它不是資料缺口的合理經濟梯度；該列之後又進入失效狀態，使剩餘時間的交易與學習訊號被截斷。失效以前的片段仍可產生梯度，因此會看到「loss 很大、梯度非零、optimizer 照常更新」並存。

v9 的 canonical ledger 在任何 loss reduction、`nan_to_num` 或 backward 之前拋出 `FuturesCarryDataError`。訓練器清除該完整軌跡已累積的梯度，不更新 optimizer 或 scheduler。DDP 任一 rank 遇到此類錯誤，其他 rank 也在相同階段同步停止；分配給不同 rank 的 validation 與 sampled test 亦先同步錯誤，再決定是否進行標量廣播、選模與曲線保存。

這裡的「零更新」指失效的訓練軌跡。如果錯誤發生在後續驗證／測試，之前已完成且資料有效的訓練更新不會被追溯撤銷；失效評估不得產生選模值、完成收據或金融績效。

真實的非正權益仍保留經濟破產語意。錯誤代碼 1/2/3/5/6 分別是身分、分鐘、估值、非有限權益、公司行動轉換資料問題；代碼 4 才是可計算出的非正權益。低階 `diagnostic_only=True` 僅供來源稽核，未知日與後續報酬回傳 NaN、保留最後有效權益與持倉；沒有 CLI 開關可以把它帶回正式訓練。舊資料失效標記也不能再送進 `compute_metrics` 或新回測檔案。

這符合 PyTorch 的基本邊界：autograd 依程式定義與非光滑算子的既定規則求導，不會替程式判斷金融損失是否有定義；在無效運算後才遮罩，也不能保證梯度安全。[PyTorch autograd 說明](https://docs.pytorch.org/docs/2.11/notes/autograd.html)

## 沒有單式成交，不等於沒有官方結算價

延長至 32 epochs 的中間驗證，在第二組 epoch 18 重現 2021-09-07 `DCF:202109` 空 1 口缺估值。兩個 rank 同步停止，最後權益 10,467,039 元；`checkpoint_last.pt` 為 epoch 17，AdamW step 也均為 17，沒有把該失效軌跡再更新成 epoch 18。這首先驗證了資料錯誤防線。

再往來源追查，`data_tw_index_futures/raw/annual/2021_fut.zip` 的 `2021_fut.csv` **原本就有可用的結算價**：

| 日期／實體合約 | 收盤 | 結算 | 報告總量 | 價差拆腳量 | 單式量 |
|---|---:|---:|---:|---:|---:|
| 2021-09-06／DCF:202109 | 16.70 | 16.70 | — | — | 1 |
| 2021-09-07／DCF:202109 | 16.70 | **16.75** | 5 | 5 | **0** |

原整理路徑把實體日線的可交易觀測與估值需求綁在一起；只有價差拆腳成交的日子雖已取得零單式成交證據，結算欄位卻未帶入留倉 tape。零單式量應阻止假成交；已報告的結算價仍能計算舊倉權益。期交所日報分開提供交易價量與結算欄位，而日結算不單純等於最後成交價。[官方每日行情欄位](https://www.taifex.com.tw/cht/3/futDailyMarketReport)、[官方結算計價說明](https://www.taifex.com.tw/cht/9/tradersQAClearing)

修復重用 `official_day_evidence` 與原始檔雜湊驗證，建立新的不可覆寫來源目錄 `artifacts/data_preparation/futures_carry_evidence_v2_settlement_20260909`。對完整實體持倉日曆提取確實存在、同日同合約、正且有限的官方結算；有該值時優先於舊 canonical 估值。新增零成交證明仍只限原 coverage 缺鍵，原爭議列不被覆寫。無結算欄位或無該日紀錄時，不生成報價。

DCF 標量回放：前一日空 1 口、乘數 2,000，當日容量零，正確損益為 `−1 × (16.75 − 16.70) × 2,000 = −100` 元；仍持有空 1 口，turnover 為零。實際 canonical ledger 已得到這個結果。[回放憑證](../artifacts/diagnostics/futures_carry_v9_20260909/dcf_settlement_probe.json)

原 2021 官方 ZIP SHA-256 為 `aeb574dd711bab6aebd6c6eac58d8e2008c0047eef0ab239236b568dc8f23ac0`；新證據 Parquet SHA-256 為 `21ea278d13d331b2e0a595eca75f6f33720587e8292f220a9934a7eecd909aa9`。這是取回已有的官方估值，不是把日線 OPEN/CLOSE 改成分鐘成交，也沒有填補不存在的一分鐘資料。

## 一口、兩次費用：可重現的梯度反例

設帳戶 100,000 元，小型合約乘數 100，入場 100，出場 100.5，單邊手續費 40，為隔離費用問題把稅設為零。

```text
一口多單毛利 = (100.5 − 100) × 100 = 50
完整淨利     = 50 − 40 − 40 = −30
一口預算門檻 = 10,000 + 80 = 10,080
權重門檻     = 10,080 / 100,000 = 0.1008
```

在權重 +0.05 時，真實前向買不到一口，損益為零。舊 STE 卻在 `abs(exact_delta=0)` 的進場費用處得到零偏導，保留了部分出場費用偏導，使 `dPnL/dw = +99.20635`；沿此方向增加多單正好跨向虧損 −30 元的一口。市場平盤且權重零時，舊偏導還會鼓勵虧損空單。

| 場景 | 舊 `dPnL/dw` | v9 `dPnL/dw` | 整口經濟意義 |
|---|---:|---:|---|
| 出場 100.5，權重 +0.05 | +99.20635 | −297.61908 | 多一口虧 30，應往現金方向 |
| 出場 99.5，權重 −0.05 | −99.20635 | +297.61908 | 空一口虧 30，應往現金方向 |
| 出場 100，權重 0 | −396.82541 | 0 | 多空各虧 80，現金局部合理 |

v9 重用 `_scheduled_cash_bracket`、`_scheduled_symbol_payoffs`、精確整口配置及同一分鐘退出引擎，計算第一筆可成交整口的雙向淨損益，再用相對現金的割線作為該段梯度。以多單為例：`−30 / 0.1008 = −297.61905`，浮點結果容許正常誤差。加入的修正項在前向恆等於零，不會產生假費用或假成交。

適用範圍刻意有限：帳戶起始為現金、該標的目標仍零口、共享資金足夠，且第一筆多空候選都能在當日真實退出。未退出的候選保留原本跨日 recurrent STE；沒有額外殘倉處罰。已持倉或更高口數附近仍是既有近似梯度。這不是整數階梯函數的數學精確導數，也不是全域最優策略的證明。STE 本來就是離散決策的有偏梯度估計方法。[Bengio 等人的原始論文](https://arxiv.org/abs/1308.3432)

15 個現金邊界案例逐一對照獨立標量公式及真正成交的一口正負倉；另把修正前程式保留於診斷目錄，以固定種子比較 16 條各 8 日、3 標的、含留倉／反向／容量限制的有效路徑。含梯度與無梯度前向均與舊版一致；此比較排除新加入的公司行動日。[逐案 CSV](../artifacts/diagnostics/futures_carry_v9_20260909/cash_gradients.csv)、[比較憑證](../artifacts/diagnostics/futures_carry_v9_20260909/cash_gradient_comparison.json)、[可重跑程式](../artifacts/diagnostics/futures_carry_v9_20260909/compare_cash_gradients.py)

## 留倉必須處理權益調整

現金除息會同時改變價格與舊倉權益。依期交所規則，個股期貨現金股利權益調整以每口為單位，元以下捨去；多方加、空方減。v9 對有效日前既有部位使用：

```text
C_j = floor(每股現金股利 × 該實體合約乘數)
A_t = Σ q_(t-1,j) × C_j
E_open = E_previous + A_t + 舊倉開盤價差
```

`A_t` 只加入一次，事件日新開部位沒有昨日權益。如果沒有有效開盤成交價，開盤預算參考價也減去相同每口現金調整，避免除息本身憑空增加可用預算。單純價格跌 2、每口補回 200 的多空鏡像案例，扣除一次進場費後的權益應保持不變。[期交所結算問答](https://www.taifex.com.tw/cht/9/tradersQAClearing)、[股票期貨交易規則第 21 條修正文件](https://www.taifex.com.tw/file/taifex/eng/eng11/1120003792_%E6%94%BE%E5%85%AC%E5%8F%B8%E7%B6%B2%E7%AB%99.pdf)

來源重用既有釘選 `tw-public` release 的權益檔，驗證 Parquet、收據與原始 manifest 鏈及日期覆蓋；只接受純現金、金額有效、公告不晚於生效日的事件。此檔案不是所有期交所歷史調整公告的完整替代；其餘事件若遇到既有持倉，明確拒絕計算。

官方例子可以驗證「只跟著原代號價格走」不夠：2021-08-31 彰銀調整，既有指定月份 DCF 改為 DC1、乘數 2,020、現金權益調整每口 720，同日仍有新掛牌標準 DCF、乘數 2,000。代號、月份與持倉生效歷史必須一起辨識，不能把新 DCF 行情當舊 DC1。[期交所 2021-08-19 公告](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/2801_20210831.pdf)

本次沒有用猜測的乘數或鄰近月份行情補這類事件，也沒有利用未來公司行動或持倉缺口改寫當天新單候選遮罩。調整型合約的完整移轉需要官方公告與相應實體行情，本次保留為明確的來源／帳務限制。

## 與其他訓練方法比較

| 方法／既有流程 | 可以借用的原理 | 此次採用或保留的界線 |
|---|---|---|
| 同 repo 嚴格當沖 v5–v7 | 鄰近整口與完整費稅比對 | 重用整口候選、分鐘成交函式；不把要求當日歸零的殘倉處罰套回使用者已准許的留倉 |
| 同 repo 全期貨 carry | 狀態、價差與成交增量一致 | 使用同一 trainer 與有狀態回測；不複製不同特徵、保證金或決策時鐘 |
| STE 文獻 | 離散前向與近似反向必須分開驗證 | 補正可證實符號錯誤，不宣稱 STE 普遍無偏 |
| Deep Hedging | 將交易成本、限制、持倉序列放入同一目標 | 支持扣成本的序列帳務；論文的對沖目標不同，不能直接推出股票期貨 alpha |
| No-Transaction Band Network | 有成本時，不交易區間可以是合理解 | 現金不必強迫離開；先用第一口淨收益確認局部決策，再驗證未見資料 |
| PyTorch AMP 梯度累積 | 更新前完成有效累積與正確裁剪 | 保留 BF16 與整軌跡一次更新，不靠增加每 batch 更新次數掩蓋失效 |

文獻依據：[Deep Hedging](https://arxiv.org/abs/1802.03042)、[No-Transaction Band Network](https://arxiv.org/abs/2103.01775)、[PyTorch AMP 梯度累積](https://docs.pytorch.org/docs/2.11/notes/amp_examples.html)。這些原始資料提供方法與條件，不提供本資料集的績效保證。

目前 train 改善而 validation/test 不同步，還可能有過擬合、可預測性不足、交易成本壓過訊號或目標與樣本分佈差異。這些是待驗證假說，不是已定位的程式錯誤。本次先修正可反證的資料／帳務／梯度問題，沒有根據測試報酬調整 learning rate、強迫投資比例或挑選有利年份。

## 來源可用範圍與剩餘限制

使用與正式 attachment 相同的實體合約生命週期產生稽核，而非只檢查當天附近月候選。

| 項目 | 官方結算補回前 | 最終 v9 來源 |
|---|---:|---:|
| 潛在實體持倉延續日 | 400,778 | 400,778 |
| 分鐘來源仍未知 | 3,604 | 3,604 |
| 缺所需帳面／最後結算估值 | 16,046 | 12,065 |
| 分鐘／估值／不支援公司行動缺口聯集 | 16,932 | 12,957 |
| 有不支援公司行動轉換的潛在合約日 | 252 | 252 |
| 無當日來源、仍沿用最後已知估值 | 21,725 | 1,192 |
| 新側錄中可直接使用官方日結算 | — | 364,622 |

這是**潛在持倉延續範圍**，不是已證明模型一定會持有的全部日期，也不是可以拿來排除訓練樣本的清單。3,604 包含先前補證建置列出的 3,602 個缺鍵及 2 個既有爭議覆蓋列；不應混報。[最終稽核 JSON](../artifacts/diagnostics/futures_carry_v9_20260909/settlement_review/audit.json)、[完整缺口 Parquet](../artifacts/diagnostics/futures_carry_v9_20260909/settlement_review/possible_continuation_gaps.parquet)

日線 SHA-256：`70a57dd76de74fd0a3652b1ecf932ccaf32320d01c252bf7b608a30b3f598df5`；coverage SHA-256：`dd4bd3949e4498465591b77bb13863f0299ca7eedd5ec091f78758633eaa82b9`。公司行動來源釘選 `tw-public-20260906T151104843336596Z-l0-penguin-246aab6e5c72e427`；新欄位及來源／收據雜湊納入 checkpoint 契約。

其他仍須明示的界線：

- 日線研究估值可能用收盤或最後已知同合約值，並非逐日已核實官方結算價；沿用值不等於行情沒變，也不會製造成交。
- 新單仍受全名目本金預算限制，未實作完整歷史券商保證金、SPAN、追繳或清算程序。期交所當沖保證金減收商品範圍不包含股票期貨；本次保留的 50% 是成交量參與率。[期交所結算問答](https://www.taifex.com.tw/cht/9/tradersQAClearing)
- 跨 chunk 傳遞精確持倉，但在訓練 chunk 邊界截斷梯度；口數預算的權益採 stop-gradient。這是既有近似，不是完整長期 BPTT。
- 各 fold 評估從現金起始。根目錄 stitched 圖是獨立帳戶研究報酬串接，尚非跨換模邊界轉移真實舊倉的實盤驗證。
- `--check-data-only` 通過，只表示它檢查到的候選來源、釘選檔及公司行動鏈通過。它不能證明所有可能持倉路徑完整；執行期間遇到缺口必須停止。

## 實作位置及驗證

| 位置 | 實際變更 |
|---|---|
| `stockagent/backtest/tw_stock_futures_carry.py` | 現金第一口梯度、舊倉現金權益、資料錯誤與真破產分流 |
| `stockagent/backtest/futures_data_validity.py` | 結構化錯誤與舊 artifact 報酬拒絕 |
| `stockagent/data/tw_stock_futures_carry.py` | 來源驗證、每口 Decimal 捨去、公司行動通道；contract v4、72 欄 |
| `stockagent/data/tw_stock_futures_repair.py`、`scripts/build_tw_stock_futures_carry_evidence.py` | 保留官方結算欄位，對完整實體日曆建置結算證據；與分鐘成交能力分開 |
| `stockagent/data/tw_stock_futures_day_trade.py`、`train.py` | 正式來源 attachment 接線 |
| `stockagent/training/dataset.py`、`windowed.py` | 實體留倉執行 side channel；讀取端共用欄位版本 |
| `stockagent/training/trainer.py` | 訓練及分工評估的 DDP 錯誤同步、梯度清除、失敗收據與 lifecycle |
| `stockagent/backtest/report.py`、`stockagent/evaluation/futures_execution_status.py` | 未知金融績效不得轉為有限虧損 |
| `stockagent/training/checkpoint_contract.py`、`stockagent/config.py` | 新語意／來源指紋與舊 checkpoint 隔離 |
| `scripts/audit_futures_carry_training.py` | 原始曲線、更新次數、實際失效持倉、潛在來源缺口的可重跑稽核 |

主回歸組 **246 passed、6 deselected**；涵蓋整口帳務、費用／容量／殘倉、梯度、更新次數、checkpoint 與分鐘來源。額外資料集接線測試 **4 passed**（其中兩個為新增的 carry case），驗證 72 欄可經 dataset→windowed，且保留 t−1 特徵時點及不成交日。CUDA eager／compiled 前向及梯度一致性 **1 passed**。現金邊界 15 例及修正前後 16 條有效前向比較亦通過。

更關鍵的反例測試是：在有效片段之後注入來源缺失，batch size 1/2/4 都必須維持 optimizer 與 scheduler 零更新、參數完全不變、梯度清空；兩個 Gloo ranks 只有一方出錯也必須一起停止。純現金除息測試包含多空、無開盤成交、新倉不領舊倉權益，以及非現金事件不得冒用原代號。

嚴格 CUDA 環境檢查通過；本次使用 `/venv/fintech`、PyTorch 2.11.0+cu128、兩張 RTX 5090。最初整合 smoke 發現 dataset 硬編碼 carry contract version 3，已改成共用版本常數並加上上述接線測試；那次失敗輸出保留，不算完成驗證。收尾另檢查最終 rank-0 artifact 工作的錯誤傳遞，保留結構化資料錯誤而非包成一般 RuntimeError；windowed 評估錯誤附上 panel 日期列。包含這些修正的重跑相關組為 **85 passed、1 deselected**，含雙 rank 最終 artifact 錯誤同步。

補回官方日結算之前，正式入口 `artifacts/smoke/futures_carry_v9_integrated_20260909` 已完成 **2 folds × 2 epochs**、逐 epoch validation／sampled test、最佳 checkpoint 的完整測試、回測 NPZ、兩個 fold 的根目錄累積刷新及九張必需圖。兩個測試軌跡均有效、期末零口：第一個 1,133 日中交易 6 日、留倉 2 日、報酬 −0.0124%；第二個 887 日中交易 5 日、留倉 1 日、報酬 +0.0042%。四次訓練更新都有有限非零梯度、無資料失效。如此低交易量的中間 smoke 只能驗證執行，不是最終資料版本績效。

最後的來源接線回歸組為 **201 passed、3 deselected**，包括從原 CSV 讀出零成交日結算、按實體月份加入估值、分鐘欄位／容量不變，以及正式最後結算仍優先於當日日結算。這組包含與先前測試重疊的案例，不能把各次通過數相加成獨立案例總數。

最終釘選來源的 `artifacts/smoke/futures_carry_v9_settlement32_20260909` 已正常結束，`progress.json` 為 `complete`。兩組各 **32 epochs／32 次 optimizer 更新**；64 個 epoch 無資料失效、無零梯度，兩個最後 checkpoint 的 AdamW step 均為 32。逐 epoch 驗證及抽樣測試、最佳 checkpoint 的完整測試、NPZ、逐 fold 累積刷新與九張必需圖均完成。先前 DCF 第 18 個 epoch 的來源阻塞已解除。

| 訓練組 | 最佳 epoch | 驗證報酬 | 完整測試報酬 | 測試交易日／全部日 | 留倉日 | 末日口數 |
|---|---:|---:|---:|---:|---:|---:|
| 2020 | 15 | +0.05944% | **−0.26247%** | 227／1,133 | 114 | 1 |
| 2020–2021 | 16 | +0.02018% | **−0.54581%** | 276／887 | 109 | 0 |

第一個 fold 的末日一口依使用者留倉規則估值保留，狀態為 `valid_marked_open_positions`，沒有假稱全部平倉。原 v8 沒有可比的完整有效測試結果，而且訓練長度、梯度與估值來源皆已不同；不能把舊近乎 −100% 標記與新報酬相減，宣稱提高了幾十個百分點。

![最終 v9 的 32-epoch 原始曲線；兩組有效訓練 loss 下降，驗證與抽樣測試後段仍惡化](../artifacts/diagnostics/futures_carry_v9_20260909/final_v9_audit/curves.png)

這張圖指出剩餘問題：**模型有學習、最佳模型有交易，樣本內改善卻尚未泛化至測試期。** 兩組第 32 個 epoch 的 train loss 分別為 −0.163547、−0.148115；驗證選中的仍是較早的 epoch 15／16。這支持繼續檢驗過擬合與市場分佈變化，而非宣稱非零梯度必然正確、更多 epochs 必然獲利，或強制模型滿倉以改善外觀。本次沒有調參選擇測試報酬，正式 1,000／10,000 epochs 及全六組年度訓練未執行。

[最終曲線 CSV](../artifacts/diagnostics/futures_carry_v9_20260909/final_v9_audit/curves.csv)、[最終帳務稽核](../artifacts/diagnostics/futures_carry_v9_20260909/final_v9_audit/audit.json)、[完成／optimizer／程式與來源雜湊憑證](../artifacts/diagnostics/futures_carry_v9_20260909/final_validation.json)。最終 source-scope 回歸與 v5 舊 checkpoint 指紋比較也通過；v5 的 trading／training 指紋保持一致。

驗證日誌位於 `artifacts/diagnostics/futures_carry_v9_20260909/`；`regressions.log`、`final_cuda_parity.log`、`cash_gradient_comparison.json` 分別記錄測試、CUDA 與前向比較。這些屬工程證據，沒有把 2-epoch smoke 或修正後現金模型當作策略成功。

## 使用方式

正式新版本沿用原入口：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_carry_v9.yaml
```

目前實際 v8 YAML 為 `epochs: 1000`，v9 繼承此值。若要 10,000 上限，在同一命令加 `--epochs 10000`；既有 early stopping 仍適用。輸出為 `artifacts/markets/tw_stock_futures_0845_carry_v9_vast5090`；不可載入 v8 optimizer 繼續，避免沿用錯誤軌跡與不同帳務的訓練狀態。原 v8 檔案及全部舊 artifact 保留。

只讀來源與曲線稽核，需指定尚不存在的新診斷目錄：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_futures_carry_training \
  --config configs/markets/tw_stock_futures_day_trade_0845_carry_v9.yaml \
  --run-dir artifacts/markets/tw_stock_futures_0845_carry_v8_vast5090 \
  --output-dir artifacts/diagnostics/futures_carry_v9_recheck
```

若正式訓練遇到 `FuturesCarryDataError`，查看輸出根目錄的 `futures_data_failure_rank*.json`，按日期、實體月份與原因補足收據支持的一分鐘／估值／調整公告資料，再建新釘選版本。不能以歸零、插值、代換近月、截短 fold 或恢復舊有限失敗標記來消除錯誤。

本次來源建置與最終限定範圍驗證可重跑如下；建置及輸出目錄應改為尚不存在的新名稱：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.build_tw_stock_futures_carry_evidence \
  --config configs/markets/tw_stock_futures_day_trade_0845_carry_v9.yaml \
  --official-manifest data_tw_index_futures/manifest.json \
  --output-dir artifacts/data_preparation/futures_carry_evidence_recheck

run_fintech_python train.py \
  --config artifacts/diagnostics/futures_carry_v9_20260909/settlement_stress_32.yaml \
  --max-folds 2 \
  --output-dir artifacts/smoke/futures_carry_v9_settlement32_recheck
```

該診斷 YAML 僅把上限設為 32 並暫停 early stopping，以完整觀察 32 次更新；正式 v9 設定保持原本的 epochs 與 early stopping。
