# 期貨研究：全台幣商品範圍、實測與接續排程

**21:51 更新：**兩個個股對照均已正式收斂並通過獨立 lifecycle／績效讀回；
08:45 測試報酬 25.8949%、Sharpe 1.03164，09:00 報酬 10.4598%、Sharpe 0.62828，
均未勝過同期 TX。08:45 名目本金候選第 55 回合。新共同 2020 期間的個股兩組已准入排隊；
擴大兩組的 1,857 個 MTX 差異已證明為週月價差腿解析缺陷並修正，遠端正在新根重建資料。
最新結果、帳本損益拆分、實作、文獻與排程見
[晚間研究報告](tw_futures_equity_universe_2026-10-23.md)。下文保留 **17:54** 的範圍與工程快照。

目前 GPU 訓練仍是 **個股期貨**，不是全部台幣計價期貨。原始商品目錄、可用財務元件、
指定時鐘的可成交合約與實際入模範圍是四個不同集合。本輪已完成全目錄財務建置及獨立範圍
讀回，沒有把未通過商品悄悄算入模型，也沒有把現在的規則回填到歷史。

## 實際商品範圍

來源目錄 `data_tw_index_futures/preparation_sources/margin_sources/universe/products.csv`
有 765 個歷史商品代碼；這不是今日上市家數，也不是每日同時可買賣的數量。

| 類別／Asset class | 來源歷史代碼 | 本輪建成財務元件的代碼 |
| --- | ---: | ---: |
| 個股期貨／Single-stock futures | 710 | 682 |
| ETF 期貨／ETF futures | 28 | 27 |
| 指數期貨／Index futures | 23 | 16 |
| 商品期貨／Commodity futures | 2 | 0 |
| 利率期貨／Interest-rate futures | 2 | 0 |
| 合計 | 765 | 725 |

原 builder 的 legacy deferred 清單排掉 54 個非個股商品；本輪新增明示的
`--universe-scope all_twd`，不再將那份工程清單當作交易宇宙。原有未指定 scope 的
重現路徑保留。全目錄新一代財務建置實際於遠端完成，耗時 **606.812 秒**：
讀入 4,968,220 個歷史契約日、55,655 個實體生命週期；產出 725 個商品的
2,004,389 個財務契約日，其中日頻 executable rows 為 1,569,941。
這仍未證明逐分鐘流動性、broker fill 或所有日期都已覆蓋。

新 acceptance 明列 `selection_is_retrospective: true`、
`full_history_training_ready: false`、`runtime_training_verified: false`，
保留 132,622 個未通過帳務列及 40 個未入選代碼。範圍建置成功不是全商品訓練成功。
132,622 是 builder 的全歷史診斷口徑；另外明確裁切 2011-01-03～2026-09-24、排除
warmup 後是 **73,159** 列，已分成 799 個商品／原因工作項。原因可重疊：
settlement rule clock 71,338 列、price limits 41,721 列、product specification
40,562 列。原始清單與 SHA 見
[財務補證工作表讀回](../artifacts/operations/futures-universe-20261023-v1/financial-repair-worklist-readback.json)，
不將不同日期分母混用。
個股入模的兩個 2013 起決策時鐘各有 **636** 個歷史代碼、1,705,459 個契約日；
真實 entry-window 交易出現在 08:45 組的 446 個代碼／127,916 列，
09:00 組的 530 個代碼／304,898 列。這些也不是每天可交易家數。

獨立收據：

- [現有個股訓練範圍](../artifacts/markets/tw_futures_universe_audit_20261023_v1/manifest.json)
- [全目錄財務範圍](../artifacts/markets/tw_all_twd_futures_universe_audit_20261023_v1/manifest.json)
- [逐商品範圍 CSV](../artifacts/markets/tw_all_twd_futures_universe_audit_20261023_v1/training_universe_coverage.csv)
- [原始規則有無的獨立盤點](../artifacts/operations/futures-universe-20261023-v1/direct-rule-input-inventory.json)

直接規格表沒有 BRF、TGF、F1F、GBF、I5F、SPF、SXF、TJF、UDF、UNF 的列；
部位表沒有 SSF、USF 的直接列。這只是直接來源盤點，不等同完整的區間／家族繼承檢查。
canonical compiler 實際選入 27 個 ETF，不能直接由上述「沒有直接列」推導成排除。

[期交所商品目錄](https://www.taifex.com.tw/cht/2/products)及
[目前股票期貨標的名單](https://www.taifex.com.tw/cht/2/stockLists)
可補「現在有哪些商品」，無法替代歷史上市／調整／下市紀錄。
目前來源的 TWD 證據是結算幣別，還須逐商品核對報價與契約價值。
[BRF 官方規格](https://www.taifex.com.tw/cht/2/bRF)明列台幣報價、200 桶契約；
[TGF 官方規格](https://www.taifex.com.tw/cht/2/tGF)的契約規模、報價單位與盤別另有定義。
不能直接移除個股類別檢查，再沿用個股稅率、時鐘及股票 underlying mapping。

全商品入模剩餘工作分為三類：補齊有日期效力的來源證據；將 canonical 決策時鐘／稅費／
價值單位與模型 instrument mapping 延伸到各商品；以整口帳務、雙時鐘及 lifecycle 實測驗收。
當前未完成的軟體工作與外部資料缺口分開列示，沒有宣稱只剩外部限制。

## 目前訓練與策略結果

17:54:22 讀回，兩卡正在跑 08:45 基準，正式 **52 回合**已完成；
加上稀疏投影 32 回合及工程 4 回合，共 **88 個市場回合**。
該時點最佳 validation 在第 48 回合，`val_mean=-0.4344628751`，尚未收斂。
第 52 回合 gradient norm 2.1361、零梯度批次 0、adjoint replay error 0、穩態新圖 0。
完整 MAX-rank workflow 第 52 回合為 29.172 秒。
17:45 的 GPU busy 單次讀回兩卡均為 100%，不表示 FLOPS、頻寬或整段 workflow 已達硬體上限。

最新已完成的 08:45 工程對照為四回合，實際測試期 2026-02-26～09-24，共 145 日：

| 策略 | 扣原費稅報酬 | Sharpe | Sortino | MDD 幅度 |
| --- | ---: | ---: | ---: | ---: |
| 新個股期貨 08:45 工程基準 | 28.6367% | 1.24370 | 1.80080 | 22.0333% |
| 同期 TX 近月轉倉，一倍名目、未扣成本 | 30.7217% | 1.35995 | 2.03942 | 17.2172% |

工程基準尚未勝過此 benchmark。這裡 TX 是期貨近月轉倉比較，不是 2330 現股 buy-and-hold。
四回合只驗證可以學習、交易、反傳及完整輸出，不能用來挑正式策略。

舊 [v16 正式研究紀錄](tw_futures_fold14_benchmark_training_2026-09-28.md)為
163 日、報酬 64.1990%、Sharpe 2.32049、Sortino 3.53369、MDD 14.6151%；
其使用 v10 初始化，第 189 回合最佳、第 289 回合早停。新研究從零訓練、資料期與宇宙
不同，不能直接用 145 日工程結果與之排名。驗證選模後仍須同期間、相同初始帳戶與成本
獨立重播；也須揭露多次研究已使用 2026 資料，不能稱為全新未見樣本。

另讀回 [10/04 持倉損益歸因](tw_futures_components_fold14_performance_2026-10-04.md)：
該輪虧損的 83.7% 來自持倉毛損益、16.3% 是費稅，沒有 settlement default；
這支持優先測方向與風險單位，不能期待只減少換手就修復虧損。
[10/04 因果帳戶回饋](tw_futures_causal_account_policy_2026-10-04.md)已實作 16 維
持倉／NAV／保證金觀測，現有配置沿用，不能將同一功能再當新研究成果。
早期股票特徵與績效的研究／PIT 限制見
[10/21 結案](daytrade_research_closeout_futures_2026-10-21.md)；來源財務證據通過不會
自動補齊所有股票外部特徵的歷史發布時點。

本輪保留 d32、lookback32、global batch128／local64／eval16、BF16＋TF32、
一億元、可跨日整口帳務與扣費後單一 Log Utility。上限 1000 回合、
連續 100 回合無 validation 改善才停、min_delta=0.0001；不載入舊模型。

## 已實測的計算改善與下一個瓶頸

先按讀取 → 轉換 → 投影 → 神經網路 → 帳務／loss → 反傳 → 驗證／產物拆解。
高維股票特徵大多稀疏，重建整個 dense 包、保留無用日期及傳輸其零值不是模型需要的計算。
沿用原完整 schema 與 normalizer，使用固定來源、日期 view 與 CSR 投影，保留 affine effect
及參數 adjoint。兩卡完整 4 回合 A/B/B/A、整口 tape、validation/test、曲線、圖及
checkpoint 皆已核對，兩個時鐘共同選入稀疏版本：

| 時鐘 | 原穩態完整 epoch，秒 | 新穩態完整 epoch，秒 | 穩態倍率 | 原完整四回合 case，秒 | 新完整 case，秒 | case 倍率 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 08:45 | 103.9647 | 27.8278 | 3.7360× | 650.2036 | 522.3277 | 1.2448× |
| 09:00 | 103.5657 | 27.7207 | 3.7360× | 648.1929 | 395.2654 | 1.6399× |

短 case 包含首次建置與暖 cache 組合，不能把穩態倍率套在全新節點初始化。
目前 per-batch finite-check 的 host 時間包含等待先前 CUDA 工作，不能把它全部當成
可以刪掉的 CPU 計算。已寫成逐批 device flag、保留中途非有限梯度、更新前跨 rank
共同拒絕的 opt-in 版本，也延後逐批 `.cpu()` loss logging；其兩卡 oracle 與完整
ABBA 排在現有正式對照之後。只有實測與財務／梯度機制通過才採用。
之後須以同步 trace 分離 sparse forward/backward、recurrent ledger、DDP、
validation 與寫檔，再依實測臨界路徑設計專用版本。**尚未達到僅剩純 GPU 計算的判準。**

## 修正消融報表與真正部署的接續隊列

原六組工程並沒有全部完成。第一個 08:45 工程 case 完成四回合後，
`plot_ablation_analysis` 因 absolute validation plot 缺少 `--calendar-receipt`
而中止，導致另五組未開始。現有正式 08:45 對照則已啟動，讓它按原停止規則完成。

本輪修正 canonical calendar checker，依經 SHA 核對的實際 futures decision clock
裁切 feature context，保留原 context 記錄。新日曆 2013-01-02～2026-09-24、3353
sessions、lookback offset32，其 period contract 與正在訓練的基準**完全相同**。
已為六組原實驗及四組追加實驗產生真實日曆憑證，沒有偽造基準或省略圖表。

遠端 Supervisor 實際狀態：

| 程序 | 狀態／責任 |
| --- | --- |
| `stockagent-futures-history2013-20261023-v2` | RUNNING，原 08:45 正式對照 |
| `stockagent-futures-calendar-recovery-20261023-v1` | RUNNING，等待對照完成，再補五組工程與其他正式實驗 |
| `stockagent-futures-conditioning-history2013-20261023-v3` | RUNNING，等待上述 lifecycle，再跑 guard oracle／ABBA 及四組追加實驗 |
| `stockagent-futures-universe-preparation-20261023-v1` | 正常退出，765→725 財務元件已完成；沒有啟動全商品 GPU 訓練 |

補跑每個工程 case 分別啟動，失敗保留診斷後繼續；合格才進正式收斂。
完成的 08:45 正式對照沿 canonical runner 收集，不再從頭重跑。
原 source release／原請求不變，新增控制與 scope release 的 source SHA 為
`ba2f6758fceb47d37b8e4dd5eb4f156014b918a93324f4e164a3540dcdb95b2c`。

| 策略中文／English | 08:45 | 09:00 | 檢驗假說 |
| --- | --- | --- | --- |
| AdamW 基準／AdamW control | 正在正式訓練 | 排入補跑 | 同歷史資訊，不同成交時點 |
| 名目本金座標／Notional-coordinate policy | 排入補跑 | 排入補跑 | 減少保證金／契約價值導致的梯度尺度扭曲 |
| 隱藏矩陣正交更新／Muon hidden matrices | 排入補跑 | 排入補跑 | 改善矩陣更新幾何；其他角色仍用 AdamW |
| 較大 AdamW 步長／Larger AdamW step，3e-5 | 排在後段 | 排在後段 | 識別有效步長不足 |
| 名目座標＋Muon／Notional coordinates + Muon | 排在後段 | 排在後段 | 檢查兩個改動的交互作用 |

這十組是現有個股宇宙的明確對照。全商品實驗目前停在 source／clock／instrument
admission，沒有把不完整的 725 個財務元件冒稱成全商品訓練。原 2011 起個股隊列仍被
2012-06-04 CTF 108 對107 口的逐筆缺口擋下；沒有補造那一口或取消該日。

## 文獻如何變成可檢驗的改動

本輪查核原始論文摘要／可用原文與官方商品規格；下列是選讀，不宣稱讀完所有前沿文獻。

| 論文 | 可用推論與實驗優先序 |
| --- | --- |
| [Kelly，1956，Information Rate](https://www.nokia.com/bell-labs/publications-and-media/publications/a-new-interpretation-of-information-rate/) | 對數成長是保留簡單效用函數的理論起點；不保證特定樣本 Sharpe／MDD。 |
| [Deep Hedging，2018](https://arxiv.org/abs/1802.03042) | 將成本、持倉狀態與限制放入決策／帳務；這是避險方法，不是台股 alpha 證明。現模型已有帳戶觀測，不能把再加同一欄位當新方法。 |
| [Deep Momentum，2019／2020](https://arxiv.org/abs/1904.04912) | 趨勢與倉位尺度可端到端學習；優先測輸出座標與有效步長，避免只堆模型。跨市場結果不直接移植成台灣績效。 |
| [No-Transaction Band Network，2021](https://arxiv.org/abs/2103.01775) | 學習以現持倉為中心的不交易帶，可能降低無效反覆交易。需另開具新 action ABI 的實驗；先依舊損失歸因驗證是否真為 turnover 主導，未混入本輪十組。 |
| [Implementable Efficient Frontier，RFS，2026](https://doi.org/10.1093/rfs/hhag022) | 直接學成本後權重及可實作的風險報酬，而非只看預測誤差。保留 exact 帳務與成本，加入同期間／同名目曝險比較。出版頁可讀摘要；完整出版 PDF 未在本輪全部審讀。 |
| [Cross-Asset Futures Timing，2026-07](https://arxiv.org/abs/2607.00475) | 16 個 CME 期貨的結果顯示模型排名會受成本改變，沒有全市場通用贏家。全商品與簡單規則基準是必要對照；其樣本不是 TAIFEX。 |
| [AsyncSparse，2026-04](https://arxiv.org/abs/2604.17834) | 將稀疏 layout、資料搬移與 kernel 共同設計，支持目前以投影臨界路徑切入。其 H100／較寬矩陣成績不是本機 d32／RTX5090 的實測倍率。 |

先測梯度座標／步長／更新幾何，保持單一扣費 Log Utility。全商品再檢驗有無新增獨立
機會；資料不足不能用預期的分散效果替代驗證。性能與收益分開驗收，以 validation
選 checkpoint，完整收斂與共同帳務重播後才判斷是否勝過舊研究對照。

## 產物、資料與驗證

以下為遠端 `/root/stockAgent/` 下相對位置：

- 目前正式基準與原六組：`artifacts/markets/tw_stock_futures_history2013_20261023_v2/<case>/`
- 追加四組：`artifacts/markets/tw_stock_futures_conditioning_history2013_20261023_v3/<case>/`；尚未開始產生正式模型。
- 全商品財務資料：`data_tw_index_futures/training_prepared/tw_all_twd_futures_history2011_20261023_v1/financial/`
- 現有雙時鐘資料：`data_tw_index_futures/training_prepared/tw_stock_futures_history2013_20261023_v1/clocks/{0845,0900}/`
- 全商品正式根僅為計畫：`artifacts/markets/tw_all_twd_futures_history2011_20261023_v1/`；尚無 GPU 訓練模型。
- [新範圍及修復請求](../artifacts/operations/futures-universe-20261023-v1/queue-request.json)
- [日曆憑證](../artifacts/operations/futures-universe-20261023-v1/calendar-context.json)
- [後段四組請求](../artifacts/operations/futures-conditioning-2013-20261023-v3/queue-request.json)

新增 scope／實際範圍讀回相關 **105 項**、決策時鐘／日曆／報表相關 **69 項**測試通過，
共 174 項；未以 CPU 測試代替尚未執行的 CUDA guard ABBA。
已完成的三個私有 build／pytest 目錄核對 source bundle、變更來源重建、清冊、
process／configuration 引用及 quarantine 後，由 Git／pytest 原 owner 移除；
[清理收據](../artifacts/operations/futures-universe-20261023-v1/temporary-retirement-result.json)
保留。原始資料、正式模型、失敗來源診斷及固定 code release 均保留。
