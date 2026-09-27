# v8 一般期貨策略：績效診斷與修復

本次稽核對象是遠端 `artifacts/markets/tw_futures_v8_general_fast_fold10`。
雖然目錄名稱包含 fold10，實際保存 **10 個 folds**；原始目錄不覆寫。
診斷、來源雜湊、增量修補與驗證位於
`artifacts/markets/tw_futures_v8_general_fold10_diagnosis_20260927/`。

**本次已找到 fold10 測試報酬明顯改善的研究候選：一億元、凍結股票骨幹，
只訓練期貨模組。** 同本金 v7 控制組的測試報酬為 +12.80%，凍結組為
**+56.05%**，Sharpe 0.90→3.07，最大回撤 −16.48%→−11.56%。
凍結組完整完成 387 epochs，以 2025 validation 選出 epoch287；沒有按 test 挑 epoch。
使用者允許不相容的模型／checkpoint 變更；本次沿用 canonical trainer 建立獨立
optimizer，不讓舊狀態限制新實驗。

這不是泛化已解決的結論：凍結組 2025 validation 報酬 25.80%，低於控制組的
28.19%，驗證最大回撤亦較大。2026 已在研究過程被觀察，而且聯亞期貨 OTF
貢獻 58.45% 的測試淨利。依事前固定的 validation 規則，正式候選排名仍以驗證
指標計算；此處如實列出測試改善，不把它改寫成未見資料上的選模成功。
RMS-only 對照亦已完成：驗證 +7.86%、測試 −10.99%，本次不採用。

**使用者指定一般模式本金為 TWD 100,000,000，本次四組均採這個本金。**
使用 `configs/markets/tw_futures_v8_general_tradable_capital100m.yaml`，
在容量候選 v4 上只把 `tw_futures_portfolio_integer_initial_capital` 與
`volume_participation_equity` 同時改成一億元，其餘模型、資料與訓練參數相同。
設定已同步遠端並通過解析、差異及 checkpoint 契約檢查；遠端嚴格 CUDA 環境檢查
與 canonical `--check-data-only` 亦通過（3,096 sessions、2,754 stocks、99 features、
選定 fold10）。
**一億基準已完成 202 epochs，最佳 epoch102，完整產物驗收通過。**
資金梯度修正版亦已完整完成 198 epochs、最佳 epoch98；凍結組完成
387 epochs、最佳 epoch287；RMS-only 組完成 123 epochs、最佳 epoch23。
本節一億元結果與後文舊 TWD 10M 帳戶分開比較，不能按十倍推算。
較大本金會降低同一口合約占 NAV 的比例，但仍受原先成交容量限制。

凍結骨幹研究候選的 fold10 重跑命令（遠端 `/root/stockAgent`，使用新目錄）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/markets/tw_futures_v8_general_tradable_capital100m_frozen.yaml \
  --start-fold 10 --max-folds 1 --no-resume --no-post-train-infer --profile-timing \
  --output-dir artifacts/markets/tw_futures_v8_general_tradable_capital100m_frozen_fold10_repeat
```

一億版本必須建立新的 optimizer trajectory；舊一千萬 checkpoint 的續訓契約已驗證
會拒絕混用。V5 股票骨幹仍沿用相同的原始移植規則。舊一千萬實驗保留供比較。

## 一億元接續實驗：資金充足時的反向梯度

本次在一億元基準完成前重現新的學習訊號缺陷。原有 recoverable backward 把
`softplus((funding_required - 1) / 0.05) × 0.05` 的導數加到 loss，
即使 funding required 遠低於 1，也會有減倉壓力。forward 以零值 STE 隱藏這項
懲罰，所以僅核對報表 loss 或實際成交損益不會看出梯度方向不同。

以一億元帳戶、一口十萬元、每口毛利 200 元、來回費用 80 元為例：
要求 75% 配置成交 749 口；提高至 75.2% 成交 751 口，兩者均資金充足、沒有違約，
且後者淨收益更高。但舊版對 action 的回報梯度為 **−0.00552588**，
會往相反方向學習；無此懲罰時為 +0.00119890。

修正採 `relu(funding_required - 1)`，只對正超額資金啟動此項導數，
其餘精確成交、費稅、資金選擇、違約吸收、soft wealth recovery 均保持。
同一例修正後梯度為 **+0.00119884**；回報、口數與其他公開帳本欄位保持完全一致。
多空、打平、虧損及超額資金案例已有測試；並非讓模型強制加碼或取消風險限制。

recoverable surrogate 升為 v7，舊 v6 optimizer state 必須拒絕續訓。
`configs/markets/tw_futures_v8_general_tradable_capital100m_funding.yaml`
與一億元基準的 data/training/trading/environment 四組設定逐項相同，只新增
獨立實驗名稱及產物路徑。完成基準後才更新遠端，避免同一訓練跨越程式版本。
這個反例證明工程問題，尚不能證明它是實際回測落後主因或修正會改善報酬。
離散整口選擇仍使用近似的 backward；此次修正不代表已證明整個離散配置達到全域最優。

遠端實際配對證據 `capital100m_pairing.json` 確認兩組 dataset fingerprint 均為
`8f4e3e11e8da52c9dd32991fb6045fb01b2eca941e60e578cd6e171954b5c690`，
四組 data/training/trading/environment resolved config 完全相同；共同量測的來源檔
只有 `stockagent/backtest/tw_futures_portfolio.py` SHA 改變。首 epoch train loss
兩組均為 `0.07570816576480865`。非確定性 BF16 與單一種子限制仍然保留。

證據：`capital100m_experiment_plan.md`、`solvent_funding_gradient_before.json`、
`solvent_funding_gradient_after.json`、`solvent_funding_patch/`。

一億元基準及資金梯度修正結果：

| 指標 | 前日容量版 10M（v4） | 同設定 100M（舊 v6 梯度） |
|---|---:|---:|
| 2025 validation 報酬 | 22.9235% | 27.6392% |
| 2026 test 報酬 | 7.4241% | 12.8429% |
| 2026 test Sharpe | 0.9480 | 0.9059 |
| 2026 test 最大回撤 | −7.4428% | −14.8238% |
| 平均要求名目 gross | 74.0305% | 75.1324% |
| 平均實際開盤名目 gross | 40.6359% | 65.7841% |
| 不足一口的非零群組占比 | 79.0832% | 37.9895% |
| 不足一口群組的配置絕對值占比 | 44.8688% | 5.2235% |

一億元改善了整口可配置性，但並未改善本次風險調整績效。TX、TMF 仍零口，
MTX 僅兩個契約日有部位；不能把本金增加當成「已學會配置台指期」。
模型選擇和整口執行是共同原因，門檻指標不能解讀成精確拒單率。

以實際整數口數重建：毛損益 +17,715,947 元、費稅 4,873,006 元、
淨損益 +12,842,941 元，其中多頭淨損益 +22,459,724 元，空頭 −9,616,783 元。
每日 FP32 淨值核對殘差絕對值最大 16.61 元、累積 −11.64 元，單獨保留，
未塞進交易成本。所有非零持倉與交易均找到 canonical 來源。
這是已執行路徑的歸因，並不代表刪掉空頭即可直接獲得多頭那一欄收益。

7 月淨損失 14,420,688 元，月報酬 −12.0804%；當月 TX 毛基準 −8.4917%。
多頭毛損益 −12,134,290 元、空頭毛損益 −1,719,970 元，兩側同時虧損；
單純「因為做空大盤」不足以解釋這段回撤。完整月份及商品拆解保存在
`capital100m_pnl_attribution.json` 與 `capital100m_daily_pnl_attribution.csv`。

一億元基準總程序 879.98 秒，epoch3 起最慢 rank 中位數 3.9614 秒；
兩卡取樣顯存峰值各 7,498 MiB。202 次 optimizer／scheduler 更新、BF16 無 scaler、
20 張 PNG 解碼、完成生命週期和量測來源未變均已驗收。
修正前後本機 256 項測試通過；修正版遠端包含三模式 CUDA Graph/eager 的
141 項測試通過。測試與執行收據位於 `solvent_funding_deployment_20260927/`。

v7 資金梯度修正版 validation 報酬 **28.1891%**、loss **−0.25753358**，
但 test 報酬 **12.7979%**、Sharpe **0.9035**、最大回撤 **−16.4796%**。
相對同本金舊梯度，test 報酬略降 0.0450 個百分點、回撤增加 1.6558 個百分點，
因此此工程修正**沒有證明測試績效改善**。實際 gross 66.0652%，並未逼模型滿倉。
修正後多頭淨收益 20,219,052 元、空頭 −7,421,192 元，費稅 4,752,595 元。
空頭損失與費用雖下降，多頭收益也減少，互相抵銷；不可只挑有利分項報告。
總程序 851.64 秒、epoch3 起最慢 rank 中位數 3.8295 秒、兩卡顯存峰值各 7,496 MiB。
以上是各一場量測，不能據此聲稱穩定加速或已達硬體極限。

從正式訓練不中斷複製 epoch45，於獨立 root 恢復到 epoch46，144 個 optimizer
state 的 step 均 45→46，scheduler、空 BF16 scaler、RNG 與無重複曲線均通過。
該 resumed epoch 的 train loss 和 LR 相同，但更新後 val/test 與不中斷訓練不同；
因此只宣告**功能性續訓成功，不宣告逐位元重現**。詳見
`funding_resume_evidence/acceptance.json` 與 `funding_resume_comparison.json`。

第三組 `tw_futures_v8_general_tradable_capital100m_frozen` 僅設定 canonical
`pretrained_initialization_trainable_parameter_prefixes: [futures_]`，針對訓練／驗證
分歧提出「保留股票表徵、降低可訓練自由度」的假說。模型總參數仍 1,934,875，
實際只訓練 109,314 個期貨參數，其餘設定和 v7 組相同。這是模型假說，並非已證明
原股票骨幹發生災難性遺忘。新舊 optimizer 契約拒絕混用，初始化既有測試 21 項通過。
設計在啟動前保存於 `capital100m_frozen_experiment_plan.md`，選擇仍只依 validation。

## 一億元凍結骨幹結果與收益來源

| 指標 | 全部參數更新，v7 控制 | 只更新期貨模組 |
|---|---:|---:|
| 可訓練參數 | 1,934,875 | 109,314 |
| 2025 驗證報酬 | 28.1891% | 25.7987% |
| 2025 驗證最大回撤 | −11.9510% | −23.4163% |
| 2026 測試報酬 | 12.7979% | 56.0549% |
| 2026 測試 Sharpe | 0.9035 | 3.0708 |
| 2026 測試最大回撤 | −16.4796% | −11.5588% |
| 測試平均實際 gross | 66.0652% | 70.2696% |
| 測試平均實際 net | 29.9158% | 47.5876% |
| 測試平均換手率 | 0.7969 | 0.3620 |
| 測試毛損益，TWD | 17,550,455 | 58,686,060 |
| 測試費稅，TWD | 4,752,595 | 2,631,162 |
| 多頭淨歸因，TWD | 20,219,052 | 63,029,551 |
| 空頭淨歸因，TWD | −7,421,192 | −6,974,653 |
| epoch3 起最慢 rank 時間中位數 | 3.8295 秒 | 2.8976 秒 |

以現有資料衡量，更新整個 193 萬參數骨幹對這次期貨任務不划算：凍結組僅更新
約 5.65% 的參數，換手率下降 54.57%，交易成本下降 44.64%，多頭毛利大幅增加。
這支持「限制更新範圍可保留有用股票表徵、減少配置噪音」這個研究方向；
尚不能證明災難性遺忘是唯一原因。兩組只有 trainable prefixes 不同，資料指紋
與共同來源 SHA 相同，但 BF16 非確定性且計算圖不同，首 epoch train loss
0.0757082／0.0760154 並非逐位元一致，也沒有多種子顯著性證據。

增加的保存淨歸因為 43,257,038 元，其中毛利差 41,135,605 元、費稅減少
2,121,433 元。不能把全部提升都歸因於節省成本；這也是兩條策略路徑的描述性拆解，
不是固定其他因素的因果估計。凍結組實際 beta 對 TX 約 0.442，控制組約 0.222；
年化波動約 22.49%／20.66%，淨多頭亦增加，收益改善包含市場方向曝險變化。

**集中度核對。** OTF／聯亞期貨（標的 3081）的淨歸因 32,766,125 元，占總淨利
58.45%；其餘已保存交易的淨歸因合計 23,288,773 元。後者不是刪掉 OTF 後重新
訓練／重新配置的回測報酬。9 份官方原始月 CSV 的 SHA 與 prepared source 指向
完全相符，258 個 OTF observed rows 的原始開盤價與成交量核對通過。
全策略 11,842 個持倉延續格的 physical contract 連續性通過，沒有在
`executable=false` 的來源列調整持倉；到期採 canonical 官方結算價，不能誤用
slot 後續分配給另一合約的 `next_open` 計損益。

OTF 有 14 個無可成交 OPEN 的估值列，保存淨歸因合計 2,998,000 元；這些是
既有部位的估值，不是成交，可能包含不流動契約的陳舊價格與後續跳變。
日資料與上述核對仍不足以證明 08:45 的報價深度或實盤可成交性。原資料缺乏完整
歷史到期結算所採取的整份合約隔離也含事後品質選擇，不能稱完整歷史可交易宇宙。
這些限制與單商品集中度需一起保留，不能只公布 56.05%。

凍結組完整程序 1,286.71 秒，20 張 PNG 解碼、完整生命週期、26 個 optimizer
state 的 387 次更新、scheduler、BF16 無 scaler 均通過；最佳及最後 checkpoint
中的 122 個股票 tensors 與 v5 移植來源逐值完全相同。兩卡顯存取樣峰值
7,432／6,704 MiB。計算少了骨幹 backward，單 epoch 約快 24.33%；但這是
變更學習範圍的策略實驗，不是輸出完全相同的運算加速，且較多 epochs 使全程較久。
每日 NAV 對精確口數歸因最大 FP32 殘差 34.67 元、總殘差 −79.37 元，單獨保留。

證據：`frozen_evidence/acceptance.json`、`frozen_allocation_gap.json`、
`frozen_pnl_attribution.json`、`frozen_concentration_audit.json`、
`capital100m_frozen_pairing.json`；遠端完整 root 為
`artifacts/markets/tw_futures_v8_general_tradable_capital100m_frozen_fold10/`。

## 四組完整結果與正規化的反證

四組均為一般期貨模式、一億元、兩卡 DDP、global batch32、BF16、seed42，
完整計算每一 epoch 的訓練／驗證／測試及報表；上限 1000 epochs，無改善 100 次早停。
checkpoint 一律依 2025 驗證 loss 選擇，測試期間為 2026-01-02 至 2026-09-04。

| 實驗 | 最佳／最後 epoch | 驗證報酬 | 測試報酬 | 測試 Sharpe | 測試最大回撤 |
|---|---:|---:|---:|---:|---:|
| 一億元控制，舊 v6 梯度 | 102／202 | 27.64% | 12.84% | 0.906 | −14.82% |
| v7 資金梯度修正 | 98／198 | 28.19% | 12.80% | 0.904 | −16.48% |
| v7＋凍結股票骨幹 | 287／387 | 25.80% | **56.05%** | **3.071** | **−11.56%** |
| v7＋期貨 RMS，全部參數訓練 | 23／123 | 7.86% | −10.99% | −1.174 | −12.34% |

第三組是目前已觀察到的測試報酬最佳研究候選；第二組仍是四組中依事前固定
validation 規則提名的版本。兩種敘述同時保留，不在看完 test 後改寫選模規則。
原來 TWD 10M 的原始 fold10 +14.66% 亦低於第三組，但本金改變了整口／容量限制，
不把跨本金的差額全部算成凍結骨幹的效果。尚未重新完成十 folds 或多種子驗證。

RMS-only 實驗相對 v7 控制只開啟既有輸入正規化，沒有加入先前 v3 的面額 encoder，
也沒有凍結股票骨幹。縮放以 2015-01-05 至 2024-12-31 的 2,439 個訓練決策日、
958,935 個有效候選格擬合；15／17 欄有效。股票骨幹的處理保持原設定。
最佳與最後 checkpoint 的 scale/mask 精確等於訓練 receipt，未讀驗證或測試資料來
估計尺度。本機與遠端各 25 項相關測試通過，完整 lifecycle／144 個 optimizer
state 的 123 次更新／20 張 PNG 亦通過，因此虧損結果沒有被當成程式未完成而丟掉。

RMS 組實際平均多頭 28.14%、空頭 36.53%、gross 64.68%；多頭淨歸因
12,029,726 元、空頭 −23,023,565 元、費稅 3,101,926 元。
輸入重新縮放後產生不同的多空選擇，並未在這個固定訓練設定下學到更好的收益。
這否定「尺度更一致就一定報酬更好」的說法，不證明所有正規化設計都無效。
完整程序 639.27 秒、穩態最慢 rank 中位數 3.8744 秒；兩卡顯存峰值各 7,498 MiB。
本次保留已量測的失敗對照，不把 RMS 加到凍結候選的設定。

[四組比較圖](../artifacts/markets/tw_futures_v8_general_fold10_diagnosis_20260927/capital100m_comparison.png)
與 [數值及選模規則](../artifacts/markets/tw_futures_v8_general_fold10_diagnosis_20260927/capital100m_comparison.json)
由 `summarize_experiments.py` 產生。RMS 證據在 `rms_evidence/`、
`rms_allocation_gap.json`、`rms_pnl_attribution.json`、`rms_remote_config_preflight.json`；
遠端產物 `artifacts/markets/tw_futures_v8_general_tradable_capital100m_rms_fold10/`。

2026-09-27 21:31（台灣時間）重新核對原始實驗已記錄的 228 份檔案（含 fold10 NPZ），
雜湊零變更；未對未記錄檔案作完整性宣稱。全部四組新產物保留，沒有替換線上模型。

## 原本表現究竟差在哪裡

| 指標 | fold10 獨立 10M 帳戶測試 | 2017–2026 連續部署 |
|---|---:|---:|
| 策略累積報酬 | +14.66% | +2.96% |
| Sharpe | 2.586 | 0.063 |
| 最大回撤 | −3.89% | −18.03% |
| TX 近月 1 倍名目基準累積報酬 | +49.02% | +566.91% |

fold10 訓練 2015–2024、驗證 2025、測試 2026-01-02 至 2026-09-04，共 163 個測試日。
其 **連續部署** 2026 報酬是 +12.71%，和重新投入 10M 的 +14.66% 是不同帳戶路徑。
基準是 TX 近月、約 1 倍名目曝險、未扣交易費稅；原圖繼承 `2330` 名稱有誤，
不能據此認為原回測基準是台積電。修正圖名不會改變收益。

完整部署在 2017、2018、2020、2023、2025 為負；2019 為現金，2024 接近現金。
這是跨年度泛化不足的實際證據，不能拿 fold10 的正報酬掩蓋。
另一方面，fold10 年化波動約 8.18%，基準約 33.31%；其原始報酬差距包含曝險差異，
不能直接解讀為同風險條件下的選股能力差距，也不能直接放大槓桿推算可達報酬。

## 從配置到實際持倉的落差

以原 NPZ 的整數口數逐筆連接 **同日、同 slot** 的開盤價與契約乘數，
並使用每日開盤帳戶淨值還原實際名目曝險；17,755 個非零配置格與 2,810 個非零持倉格
均找到來源，缺漏為零。公式與來源 SHA 在 `fold10_allocation_gap.json`。

| 日平均 | 模型要求 | 實際持倉 |
|---|---:|---:|
| 多頭名目曝險 | 52.11% | 20.20% |
| 空頭名目曝險 | 22.06% | 6.61% |
| 總名目曝險 | 74.17% | 26.82% |
| 非零 slots | 108.93 | 17.24 |

平均曝險落差 **47.35 個百分點**，不是模型主動決定只持有 26.82% 曝險。
持倉格包含前日延續的部位；不能把持倉格數當成當日成交筆數。

- 85.04% 非零 slot 配置不夠一口；合併同標的、同到期月份的標準／小型契約後，
  仍有 81.34% 非零群組不夠一口，涉及 48.21% 的配置絕對值。
- 27.66% 配置絕對值落在前日成交量可用容量為零的契約。
- 67.63% 配置絕對值所在格沒有持倉。上述原因重疊，百分比不能相加。
- TX、MTX、TMF 在這段測試全程零口。各自一口名目約占當時 NAV 的
  72.91%、18.15%、3.65%，原模型給它們的配置均不到一口。

原模型看得到前期成交量，但沒有啟用契約面額 encoder；期貨連續特徵在進入 Linear
以前也沒有逐欄尺度正規化。先做 Linear 再對 token 做 RMSNorm，無法消除 Linear
之前不同欄位的量級差距。來源樣本中 return/spread 約 0.015–0.034，而 volume/OI log
約 3.9–5.0，差距可達百倍。這是輸入設計的可修正缺口；是否改善收益仍須實測。

## 已重現並修正的工程缺陷

| 問題 | 反例與影響 | 修正 |
|---|---|---|
| 小容量整筆放棄 | 原候選前緣最低約為目標 5/32，mini-only 僅 full/zero；想買 500 口、只能買 20 口時可全部落空 | 先把目標相對既有持倉的增減口數裁切到真實容量與方向限制，再做原有資金選擇 |
| 零口漏算成本梯度 | floor 後為零，`abs(0)` 導數為零；PnL 的 STE 有梯度，entry/exit cost 卻沒有 | 零成交邊界採用連續交易意圖的成本方向導數；forward 仍只收取實際成交費稅 |
| 精確零配置沒有方向梯度 | `sign(w) * STE(abs(w))` 在 w=0 導數消失 | signed identity STE；零報酬不產生固定買入偏向 |
| 浮點容差可選超支部位 | 選擇器容許高於資金的 epsilon，最後審核卻拒絕，差 1 元可能造成假違約 | 全程禁止超支，最後審核沿用選擇器相同 group cash 算式與加總順序 |
| STE 浮點相消 | 理論 exact 的 `shadow + (exact-shadow).detach()` 可產生微小 FP32 誤差 | `exact + (shadow-shadow.detach())`，開關梯度時 forward 逐位元一致 |
| 報表單位錯誤 | `weights_history` 保存 signed contract quantities，部分圖表當成權重 | 沿用帳本 ABI，配置圖與 daily weights 改讀 requested history 並明確標成要求曝險；未冒稱實際成交曝險 |

前四項保留大小契約合併、反向交易先消耗平倉容量、方向限制、保證金強平與整數部位限制。
沒有重新分配未成交現金到其他標的，也沒有強制滿倉。候選仍是原本 1/32 離散前緣，
並非所有整數組合的全域最優求解器。

這些反例證明程式缺陷存在，**不等於每個缺陷都是原始虧損的主因**。
原 fold10 最佳 epoch 的違約指標為零；其梯度也非零。原 folds 2、3、8 留在現金，
是後續模型未通過 validation 的現金基準，而非有證據顯示模型永久卡在零梯度。
當時 epoch0 的現金 checkpoint 是暫存替代，訓練開始前已還原移植模型。

| 選擇現金的 fold | 訓練過程最佳 validation loss | 最後 train loss |
|---|---:|---:|
| 2 | +0.014512 | −1.1814 |
| 3 | +0.004217 | −1.0085 |
| 8 | +0.041016 | −0.8076 |

目標是最小化負 log utility，現金 loss=0；三者都沒有驗證優於現金的已訓練權重。
把現金基準拿掉只會強迫選擇較差的驗證策略，並不能修復泛化。
全 10 folds、1,981 個 epochs 的記錄均無 training default，`train_zero_grad_batches`
也均為零。這不是逐參數梯度正確性的證明，但連同非零成交收益與持續變化的 loss，
足以排除「整段訓練永久停在零配置」的解釋。
詳見 `original_all_fold_training_health.json`。

以 **同一份原 fold10 requests** 重播後，這個判斷更明確：

| 執行器 | 測試報酬 | 平均實際持倉名目 gross | 違約 |
|---|---:|---:|---:|
| 原版 | 14.6632395% | 26.8153% | 0 |
| 只修容量 | 14.6673122% | 26.9809% | 0 |
| 只修資金 | 與原版相同 | 與原版相同 | 0 |
| 全部帳本修正 | 14.6673122% | 26.9809% | 0 |

容量修正只增加 **0.00407 個報酬百分點**；新增 52 個非零契約日，重平衡交易口數
6,890 → 6,964，總費稅 295,522 → 298,526 元。它修掉實際缺陷，卻不是本 fold 大幅
落後基準的主因。原重播的回報、turnover、所有整數口數及期末 NAV 與原 NPZ 逐位元
相同；9 日 equity scale 中間值相差一個 FP32 ULP、最多約 NT$1.19，已記入收據。
固定 action 的 no-grad 重播無法衡量梯度修正效益，必須重訓。
證據：`fold10_saved_requests_replay.json`、四份 `replay_*.npz`。

## 泛化與訓練設定

原 fold10 最佳 epoch=60，validation loss −0.126895、驗證報酬 +13.02%；
早停於 epoch160。train loss 由最佳時 −0.549394 繼續下降至 −0.753825，
validation 卻劣化至 +0.016732，符合訓練集改善、驗證集退步的現象。
早停與最佳 checkpoint 選擇有作用；把訓練跑滿 1000 epochs 不是目前的解法。

v5 移植了 122 個 tensor，但有 26 個期貨 encoder／embedding／attention／head tensor
重新初始化，4 個股票輸出 tensor 不使用。它保留股票背景骨幹，沒有繼承一個已學好的
期貨配置策略。模型每 epoch 是完整 chronological trajectory 的一次 optimizer step，
不應將 77 個 batch 誤認成 77 次更新。

本次對照維持原 seed42、兩張 RTX5090、BF16、global batch32、eval batch16、
學習率 1e-4、1000 epoch 上限與 100 epoch 無改善早停，沒有根據 2026 測試報酬調參。
選擇 checkpoint 仍只依 2025 validation。2026 已被檢視，後續比較屬已觀察期間的
研究性回測，不能再稱為完全未見的最終 holdout。

一般模式仍是全名目資金約束，並未轉成可加槓桿的保證金模式。成交沿用日 OPEN
研究代理、前日成交量 × 50% 的容量上限、每口每邊 40 元與既有日期稅率；未新增
逐筆 Bid/Ask 排隊或市場衝擊模型。這些是本次固定的回測假設，不是已驗證的券商成交。
日資料無法證明逐筆成交品質，本次沒有用虛構價格填補這項缺口。

## 新模型實驗的可檢查契約

`configs/markets/tw_futures_v8_general_causal.yaml` 繼承原 general config，只增加：

1. 前一期同實體合約結算價 × 來源日期乘數，加上當時可知費稅，作為一口資金估計。
   當日 OPEN、當日高低收與當日成交量不進模型；實際整口 sizing 仍由 executor
   根據當日成交價格與動態帳戶淨值完成，模型不再做一次硬性整口投影。
2. 對已 shift 的 17 個期貨連續特徵，以訓練集 `valid_indices` 和 candidate mask
   fit 逐欄 RMS；沿用共用 fit/cache/DDP broadcast 流程。驗證、測試不參與 fit。
   scale 與 active mask 保存於 checkpoint；exact resume 直接還原保存的 buffers，
   不以重新 fit 的值覆蓋。未在訓練期啟用的欄位不因未來大值而啟用。

面額 encoder 以固定 10M 參考本金換算一口比率，尚未把即時 NAV 與既有持倉作為
模型輸入；因此提供的是面額提示，不是模型已掌握所有帳戶限制的證明。
動態 NAV、容量、整口與持倉轉換仍由同一個真實帳本決定。

保持模型可選多、空、現金；不增加最低槓桿、固定 top-K 或強制交易。
新輸入／forward／surrogate 均寫入 checkpoint 指紋，不能用舊 optimizer state 靜默續訓。

另已準備獨立的一因子實驗 `configs/markets/tw_futures_v8_general_tradable.yaml`，
本次接續分析已在遠端完成 **v4 完整 fold10 對照**：
保留原模型與特徵尺度，只要求模型候選具備 **前日已知至少一口的容量**。
原本 27.66% 配置落在零容量標的，對既有執行規則來說當日就是不可改變部位的 action。
以 `floor(previous_volume × participation) >= 1` 過濾模型候選，讓現金選項承擔不交易的選擇。
它不在成交後搬移資金，也不強迫模型滿倉；現有持倉仍保留原帳本價格、數量、
持倉損益與清算規則，完全不能因模型遮罩而消失。
此版本不開啟上述 RMS／面額 encoder，避免把兩個實驗的效果混在一起。
完整固定來源驗證中，候選由 2,166,316 降至 1,284,133；驗證 2025 由 246,334 降至
137,099，測試 2026 由 147,823 降至 95,876。28 個其餘 sidecar 欄位逐項完全相同。
沒有任何原本有候選的日期變成全空，訓練 2,439 日、驗證 243 日、測試 163 日完全保留。
資料收據：`prior_capacity_source_preflight.json`。

股票資料 release 保持
`tw-public-20260913T030921278338896Z-l0-penguin-248d0869d7d53be4`；
期貨 parquet SHA 保持
`70a57dd76de74fd0a3652b1ecf932ccaf32320d01c252bf7b608a30b3f598df5`。
原先因官方到期結算缺漏而整份隔離的 3597 個實體契約不變；這是回溯資料品質隔離，
不能宣稱已涵蓋完整歷史可交易 universe。契約乘數沿用既有日期來源，未額外聲稱
本次完成所有公司行動乘數稽核。

## 對照結果與驗收

遠端 GPU 驗收：100 passed，涵蓋三模式、CUDA Graph/eager 帳戶與梯度、資金臨界值、
forward 有無梯度逐位元一致，以及報表來源單位。

帳本修正版完整 fold10 已完成，exit code 0，總程序 740.59 秒、160 epochs、最佳 epoch60。
原版及修正版 dataset fingerprint 都是
`87800a7be0f0608fc8ad463ccb028c4bfce1b5da1dc3a3d9dca2eea329fc0585`。

| Fold10 | 原版 | 只修帳本後重訓 v2 | 帳本＋RMS＋前期面額 v3 | 帳本＋前日容量候選 v4 |
|---|---:|---:|---:|---:|
| 最佳 validation loss | −0.126895 | −0.134214 | −0.179615 | −0.214036 |
| 最佳 epoch／結束 epoch | 60／160 | 60／160 | 316／416 | 77／177 |
| 2025 validation 報酬 | 13.0165% | 13.8169% | 18.9104% | 22.9235% |
| 2025 validation Sharpe | 1.7126 | 1.7777 | 1.6524 | 1.7471 |
| 2026 test 報酬 | 14.6632% | 14.1124% | −7.3036% | 7.4241% |
| 2026 test Sharpe | 2.5861 | 2.5229 | −1.1506 | 0.9480 |
| 2026 test 最大回撤 | −3.8883% | −3.6434% | −8.7847% | −7.4428% |
| 平均要求名目 gross | 74.1677% | 74.1495% | 80.9322% | 74.0305% |
| 平均實際開盤持倉名目 gross | 26.8153% | 26.8728% | 33.6113% | 40.6359% |

驗證改善、測試報酬略降、測試回撤減輕，是混合結果。工程修正有獨立正確性依據；
這組單一種子、原有非確定性 BF16 實驗不能證明獲利顯著改善。
修正版仍有 81.41% 非零群組不足新開最小一口，指數期貨仍零持倉；
帳本修正沒有消除主要配置／整口缺口，不能將其宣告成已解決的模型問題。
產物：`artifacts/markets/tw_futures_v8_general_repaired_fold10_v2/`。

原版／修正版記錄到的 epoch3 以後最慢 rank 牆鐘中位數為 **3.748 / 3.855 秒**。
原版只有 36/158 期保存此欄，修正版為 158/158，故約 2.9% 的差異不是完整配對測速。
兩者皆每 epoch 一次 optimizer update、2 個帳本 CUDA Graph，
修正版 busy eager fallback 與 graph eviction 都為零。新增正確性檢查保留了既有加速。
來源：`training_comparison_summary.json` 與各 epoch 曲線，並非整個程序都已 compile 的宣稱。

v3 已完整結束，`fold_complete.json` 為 complete、程序 exit code 0；
總程序 1800.74 秒，epoch3 以後 max-rank 中位數 **3.903 秒**，414/414 期有記錄。
它同時增加期貨逐欄 RMS 與前期面額 encoder，不能把結果歸因於其中單一項。
最佳 checkpoint 按 2025 validation loss 選在 epoch316；2026 test 轉為 −7.30%，
因此 **驗證改善並未泛化到這段測試資料**，不能宣稱模型優化已改善獲利。
先前三組比較中 v3 的 validation loss 最低；加入 v4 後，診斷 JSON 的
`selected_candidate=tradable` 表示 v4 validation loss 最低，不代表測試表現最佳、
正式採用或已部署。

v3 實際 gross 增至 33.61%，但模型要求亦增至 80.93%，兩者仍差 **47.32 個百分點**。
78.24% 非零群組仍不足新開最小一口，27.78% 要求配置絕對值仍落在前日零容量契約；
TX／MTX／TMF 仍完全零口。換手率從原版 0.2991 升至 0.4928，
實際平均空頭 gross 從 6.61% 升至 14.21%，平均實際 net 則從 13.59% 降至 5.18%。
這些是配置與交易行為的變化，尚不能單憑彙總數字拆出各項對虧損的因果貢獻。
配置稽核的 13,621 個非零要求格與 2,526 個非零持倉格均有同日、同 slot 來源，
缺漏為零；保存口數均為整數。

v3 遠端完成產物為 `artifacts/markets/tw_futures_v8_general_causal_fold10_v3/`。
本機 `causal_evidence/` 已取回 54 份檔案並逐檔驗證 SHA-256，
`causal_allocation_gap.json` 保存來源與帳戶曝險公式；
[四組比較圖](../artifacts/markets/tw_futures_v8_general_fold10_diagnosis_20260927/experiment_comparison.png)
及 [比較 JSON](../artifacts/markets/tw_futures_v8_general_fold10_diagnosis_20260927/experiment_comparison.json)
由 `summarize_experiments.py` 重建。所有比較採同一個 fold10 獨立測試帳戶，
未混入連續部署路徑，亦未依 test 收益自動選模。

## 接續診斷：實際多空損益與費用

本次依使用者重新要求分析一般模式，接續原先尚未執行的 v4 容量候選實驗。
沿用原資料 SHA、官方結算 SHA 與股票 release；兩張 RTX 5090 執行相同
1000 epoch 上限、100 epoch 無改善早停、seed42 與完整報表生命週期。
原本的當沖資料補抓與暫時優先排程仍獨立執行。

新增 `audit_fold10_pnl.py` 以 canonical builder 重建精確的 11-channel 成交事實，
直接讀取已保存的整數持倉，不重新選股、不推測成交，也不把合約口數當成權重。
每日拆分開盤調整、跨日持倉區間價格變動、到期平倉與費稅；反向交易的平舊倉及
開新倉分別歸入原方向及新方向。每個非零持倉與交易格都有同日同 slot 來源。

| Fold10 測試，TWD | 原版 | 帳本修復 v2 | RMS＋面額 v3 | 容量候選 v4 |
|---|---:|---:|---:|---:|
| 多頭毛損益 | 1,856,504 | 1,790,629 | 415,252 | 1,342,722 |
| 多頭費稅 | 223,546 | 223,129 | 242,878 | 278,191 |
| 多頭淨損益 | 1,632,958 | 1,567,500 | 172,374 | 1,064,531 |
| 空頭毛損益 | −94,663 | −87,123 | −716,170 | −172,529 |
| 空頭費稅 | 71,976 | 69,138 | 186,565 | 149,582 |
| 空頭淨損益 | −166,639 | −156,261 | −902,735 | −322,111 |
| 總費稅 | 295,522 | 292,267 | 429,443 | 427,773 |
| 合計歸因淨損益 | 1,466,319 | 1,411,239 | −730,361 | 742,420 |

與保存 NAV 的單日最大差異分別為 1.72／1.55／1.34 元，累計殘差為
+1.75／−1.28／+3.23 元。這些 FP32 累積與尺度轉換的誤差單獨保存，未灌入多空損益。
原版 29.6 萬元費稅等於初始本金的 2.96%；v3 升到 4.29%。這是固定實際成交路徑的
成本歸因，不能直接當成取消成本後重新訓練／回測的收益。

原版淨利由股票期貨 1,355,060 元及 ETF 期貨 111,259 元構成，沒有指數期貨持倉。
對 TX 每日簡單報酬的描述性 beta 只有 0.081、相關係數 0.329；年化波動 8.21%，
TX 基準 33.37%。因此 TX 的 +49.02% 與本策略 +14.66% 不是同一風險曝險的比較。
v3 空頭淨損 −902,735 元，已超過多頭淨利 172,374 元；增加交易量並沒有產生
足以負擔成本的收益。這可解釋保存帳本的虧損，但不是未來禁止放空的因果證明。
本次保留模型自主多空／現金選擇，沒有因為已看過 2026 而改成只做多。

證據：`original_pnl_attribution.json`、`repaired_pnl_attribution.json`、
`causal_pnl_attribution.json`、`tradable_pnl_attribution.json` 與對應的
`*_daily_pnl_attribution.csv`。v4 單日最大殘差 1.71 元，累計 −2.43 元。

v4 遠端目錄為 `artifacts/markets/tw_futures_v8_general_tradable_fold10_v4/`。
增量部署收據位於診斷目錄的 `prior_capacity_deployment_20260927T1130/receipt.json`，
保留修改前原檔，只套用候選資格及 checkpoint 契約補丁，未覆寫 trainer 或模型。
注意同一 candidate mask 也參與期貨 token attention，因此此實驗是「已知可交易
候選集合」的一因子變更，不能細分成純輸出正規化與 attention 各自的效果。
v3 沒有部署成正式策略；新實驗也不自動替換任何線上模型。

### v4 改善了什麼、還有哪些問題

1. **已知零容量的無效配置已消除。** 測試期該配置質量由 27.66% 降至精確 0%；
   3,910 個非零持倉格、14,772 個非零配置格均有完整來源。
   模型要求／實際 gross 差由 47.35 降至 33.39 個百分點。
2. **整口分散仍存在。** 低於最小新開口數的群組比例只由 81.34% 降至 79.08%，
   涉及 44.87% 的要求配置質量；TX／MTX／TMF 仍零持倉。這不能靠強制滿倉、
   補槓桿或把未成交資金搬到其他標的來冒充已學會可執行配置。
3. **增加成交未改善預測。** v4 比原版少 591,648 元毛損益、增加 132,251 元費稅，
   合計少 723,899 元淨利。這是兩條保存路徑的算術差異，不是成本移除反事實實驗。
   驗證期改善、測試期退步再次出現；只依 2025 單一年份選 checkpoint 的結果，
   不能證明跨市場狀態泛化。完整十 folds／多種子驗證尚未完成。
4. **保留因果邊界。** v4 尚有 5.39% 配置落在來源 `executable=false` 的格子；
   這個來源欄位含當日 observed row 與完整 OPEN/CLOSE 品質，不能把它直接塞進
   盤前候選遮罩來美化成交率。前日容量、當日來源可成交性與已完成整口成交是不同條件。

### 完整生命週期及續訓驗收

v4 程序 exit code 0，總時間 **769.91 秒**；177 epochs、epoch77 最佳；
epoch3 起 175/175 期 max-rank 時間的中位數 **3.862 秒**，與帳本 v2 的 3.855 秒
相近。兩 GPU 取樣最高各 7,498 MiB，未把這項候選修正宣稱為運算加速。
Canonical lifecycle 檢查、144 個 optimizer state 的 177 次更新、BF16 空 scaler、
模型有限值、global batch32、原 v5 checkpoint SHA 與 122 個移植 tensors 均通過；
20 張 PNG 實際解碼成功。`acceptance.json` 保存驗收。

遠端完整 GPU 測試 **114 passed**，涵蓋候選因果性、零容量持倉估值、舊契約續訓拒絕、
帳本資金及成本梯度、三模式 CUDA Graph／eager。另本機 checkpoint／early-stop／
相關契約測試 136 passed，新增拒絕混用契約測試所屬測試檔再次 9 passed。
原目錄 228 份已記錄證據 SHA 重新檢查，零變更。

續訓驗證使用訓練中保存的 epoch107 完整 checkpoint 副本，在獨立目錄
`artifacts/markets/tw_futures_v8_general_tradable_fold10_v4_resume_check/`
重新啟動同一 canonical trainer，實際完成 epoch108 的訓練、驗證、測試與報表。
144 個 optimizer step 均由 107→108，scheduler／空 scaler／RNG 恢復及曲線無重複
通過。只把 smoke 結束 epoch 設為 108；warmup32 與 scheduler 總步數1000 保持。
這不是僅檢查已完成 fold 跳過的測試，也沒有中斷主要實驗。

**續訓功能通過，但不宣稱逐位元重現。** 與未中斷主實驗 epoch108 相比，train loss、
LR、no-improve 與更新次數完全相同；val loss 為 −0.166929／−0.168452，test loss
為 −0.173928／−0.159424。當前 BF16/DDP 配置未啟用確定性演算法，且此 smoke 的
epoch108 為結束 epoch；尚未拆解二者造成差異的比例，不能將差異直接歸因於其中一項。
兩者最後選中的 epoch77 checkpoint SHA 與完整測試報表一致。
細節保存在 `resume_snapshot.json`、`acceptance.json`、`epoch108_scalar_comparison.json`。

## 遠端重現與完整跨年度驗證命令

容量候選修正版使用獨立設定，沒有改動原 general baseline 的預設，避免原 artifact
被用不同候選集合靜默接續。下列是完整十 folds 的研究命令；本次只執行 fold10
對照，**不把單一 fold 結果當成已完成全歷史驗證**。首次使用新目錄：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py --config configs/markets/tw_futures_v8_general_tradable.yaml \
  --start-fold 1 --max-folds 10 --no-resume --no-post-train-infer --profile-timing \
  --output-dir artifacts/markets/tw_futures_v8_general_tradable_all_folds_v4
```

同契約中斷後，以相同目錄把 `--no-resume` 改成 `--resume`；已完成 fold 由既有
生命週期檢查後跳過。舊 general／causal 的 optimizer 不可續入新容量契約。
原 v2、v3 與全部原版產物保留；v3 僅供診斷，不是建議部署的替代策略。
