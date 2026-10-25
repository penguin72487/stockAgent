# 期貨研究：收斂結果、持倉損益與擴大交易宇宙實驗

本報告接續 [17:54 的全目錄盤點](tw_futures_universe_and_progress_2026-10-23.md)。
2026-10-23 **21:51（臺北）**在 `114.34.26.236:43269` 獨立讀回；正式 GPU 訓練仍為個股期貨。
新四組中兩個個股對照已完成資料／日曆准入並排隊；兩個擴大宇宙組發現週月價差腿解析缺陷，
已完成根因驗證、修正並啟動獨立新版本重建。尚未宣稱全台幣商品均已入模，也未證明新策略超越舊模型。

## 22:18 圖表交付修正

遠端已完成的 `clock__0845`、`clock__0900` 各有 20 張 PNG，但先前本機並無對應目錄。
Loss 在 `train_<years>/epoch_curve_every1.png`，並不在 `fold_12`。已補回兩組全部 40 張圖、
逐回合 loss／耗時及績效摘要，入口為
[本機圖表索引](../artifacts/markets/tw_stock_futures_history2013_20261023_v2/CURVES.md)。
逐檔 SHA-256、來源前後一致、PNG 解碼及索引 45 個連結均通過；08:45／09:00 的原始
loss 記錄分別涵蓋 1～290／1～102 回合。本次是使用者要求的唯讀圖表檢視，完整 checkpoint
仍在遠端，沒有刪除來源，也不將圖表交付當成完整模型回傳或冷儲存完成。

既有 completed-return SSH 程式將共用 `_ancestor_process_references` 定義在錯誤的
globals，實測因 `_process_directories` 未載入而發生 `NameError`；修正為在原 maintenance
module namespace 定義，保留程序引用與 fail-closed 檢查。共用 discovery／retirement 使用
同一修正；69 個 ingress／training-return 測試通過，包含實際執行傳送的程式並攔住父目錄
orchestrator 引用的回歸案例。既有 service 22:15:42 以 exit 0 完成一輪，觀察到 2 個案例但
eligible 為 0；遠端原實驗 orchestrator 仍引用整批目錄。另一個獨立全範圍探查曾因短暫
`/proc/.../cwd` 權限不足而拒絕掃描，未放寬保護或將該次失敗稱為成功回傳。
完整回傳仍由既有 owner／timer 處理；本次未另建常駐同步器。
見 [圖表交付與 service 讀回](../artifacts/operations/futures-curve-delivery-20261023-v1/final-readback.json)
及索引內的全檔雜湊收據。

## 已完成的正式基準

`tw_stock_futures_history2013_20261023_v2/clock__0845` 的最後 fold 為 12。
以驗證集選第 **190** 回合，連續 100 回合無改善後於第 **290** 回合停止。
獨立使用該輪原 frozen source 的 lifecycle validator 驗收，並從保留的整口交易 tape
重新計算績效，與 `metrics.json` 一致。

| 帳戶／期間 | 手續費後報酬 | Sharpe | Sortino | MDD 幅度 |
| --- | ---: | ---: | ---: | ---: |
| 08:45，2025 驗證期間 | 67.4245% | 2.44448 | 4.44732 | 8.6590% |
| 08:45，2026-02-26～09-24，145 日 | 25.8949% | 1.03164 | 1.45924 | 24.8705% |
| 09:00，同期 145 日 | 10.4598% | 0.62828 | 0.86955 | 20.5852% |
| 同期 TX 一倍名目本金近月轉倉，毛基準 | 30.7217% | 1.35995 | 2.03942 | 17.2172% |

TX 是專案既有 `tx_front_rolling_1x_gross`，換月按相同實體合約前一收盤銜接；
毛基準不代表相同保證金帳戶的費稅後實際交易。策略列則保留原定費稅、容量及整口帳務。
兩者的會計差異明示保留，不能將毛基準描述成券商實際成交績效。

這個基準尚未超越同期 TX。驗證集強、較晚研究期間弱，是泛化落差的證據；
不能只由此斷定梯度程式錯誤。145 日均有整口持倉，沒有結算違約、追繳或開盤強平。
290 回合的零梯度批次總數為零；工程 adjoint replay 誤差亦為零。
這些證據驗的是已實作的梯度與帳務，不保證模型找到最優策略。

09:00 已在第 **102** 回合停止，驗證選第 **2** 回合；完整 lifecycle 及 tape 指標均已核對。
其驗證報酬 32.3076%、Sharpe 1.99394、Sortino 3.40422、MDD 6.3682%。
08:45 與 09:00 的 test 日期及 TX 向量相同，兩者均無違約。08:45 的 return／Sharpe／Sortino
較高，09:00 的 MDD 較小；兩者不是所有指標一致的支配關係。其他正式候選狀態及完整讀回見
[21:51 收據](../artifacts/operations/futures-equity-universe-20261023-v2/readback-20261023T135120.json)。
08:45 名目本金座標組已進入第 **55** 回合；其餘三組座標／Muon 候選仍排隊，
尚不拿未收斂的 test 快照排正式名次。後段四組條件化／Muon 組合亦保留原順序。
先前被繪圖日曆擋住的另外五組四回合工程，現均通過完整 lifecycle、有限梯度與整口 tape；
每組的 145 個 test／deployment 日均有實際持倉。四回合結果只作工程驗收。

[歷史 v16](tw_futures_fold14_benchmark_training_2026-09-28.md) 的報酬 64.199%、
Sharpe 2.32049、Sortino 3.53369、MDD 14.6151%，使用 v10 初始化及另一段 163 日期間。
本輪從零訓練、來源、可選商品與期間均有差異，不能把兩張表當成相同輸入的排名。
先前日內股票 MGDA 的結果也不列作期貨績效。

## 從帳本定位績效問題

一億元起始帳戶的 `futures_margin_audit` 顯示：

- `overnight_pnl_twd` 合計 **62,264,576 元**。
- `equity_after_mark_twd - equity_before_twd` 合計 **25,894,904 元**，
  與最終 NAV 淨利 25,894,904.14 元的差異來自 audit 儲存精度。
- 兩者差額約 **−36,369,672 元**，為這個既有帳本口徑的其餘日內損益與成本合計；
  尚未拆成價格損益、佣金、稅及執行成本。
- 帳本名目曝險／權益平均 **0.6567**、95 百分位 **1.0138**、最大 **1.3292**。
  追繳日、開盤強平日均為 0。

這支持優先檢查時段與商品選擇，不能只歸因於過高槓桿、手續費或未成交。
依 retained tape 的費後 **log return** 正確還原，最大回撤從 2026-06-25 的高點延續
到 07-30，共 24 個交易日；最差單日為 05-08，簡單報酬 **−7.0332%**。
log return 與簡單報酬分別保留，MDD **−24.8704573%** 與 canonical 指標一致，
見 [回撤時段核讀](../artifacts/operations/futures-equity-universe-20261023-v2/drawdown-readback.json)。
也不能直接推出「只交易隔夜」會獲利 6,226 萬：改變動作便改變之後的庫存、容量、
費稅及風險，必須整條帳戶路徑重播及重新訓練。舊的
[2026-10-04 成分分析](tw_futures_components_fold14_performance_2026-10-04.md)
亦曾發現大部分虧損來自持倉損益，不能用單一減換手方案概括所有問題。

簡單 Log Utility 仍有合理基礎：最大化複利成長，在小報酬近似下同時考慮平均收益與
二階風險。它不保證 Sharpe、Sortino 及 MDD 一起最優；此次不新增四指標平均 loss。
已存在的因果持倉／NAV／保證金回饋，也不重複包裝成新技術。

## 實際商品範圍與本輪改動

原目錄有 **765 個歷史代碼**，其中 710 個股、28 ETF、23 指數、2 商品、2 利率。
既有 full-TWD 財務建置只通過 **725 個歷史代碼**：682 個股、27 ETF、16 指數。
它不是今天上市清單，不是每天同時可成交的商品數，也不是全商品已訓練的證據。
目前正式 2013 起時鐘實際涵蓋 636 個歷史個股代碼。

本輪延伸共用程式，而非另寫帳本或 trainer：

1. 決策時鐘新增 opt-in `asset_scope=twd_equity`，限定具有日期規則的個股／ETF／指數；
   dataset 為 `tw_twd_equity_futures_carry_decision_clock_v2`，原 stock-only v1 ABI 保持。
2. 同一 whole-market tick reader 依實際商品類別驗價格格點與交易時段，保留週契約。
   Float32 價格只容許唯一合法格點且重編碼 bits 相同的來源解碼；不改 raw、不去重真實成交、
   不把 VWAP 四捨五入為假成交。全日數量／HLC 與官方一般交易、鉅額交易扣除證據仍需一致。
3. 同一入場重定價函式按日期及 stock／ETF／stock-index 稅別計稅，
   保留公司行動現金、固定額保證金、比例保證金、前期持倉與獨立到期容量。
   未證明線性契約價值／稅基的商品仍拒收。
4. SR1 是 0050 分割後的調整 ETF 契約。依
   [期交所 2025-06-18 調整公告](https://www.taifex.com.tw/file/taifex/eng/eng11/0050en_20250618.pdf)，
   NY1／SR1 只在公告調整後至最後公告交割月份 202603 的研究窗內繼承 ETF 價格家族。
   日期窗不替代逐契約到期日；40,000／4,000 單位與合併部位限制仍由 dated financial master 擁有。
   歷史價格規則版本升為 **4**，沒有把調整代碼改回普通合約。

原公告 PDF 與 SHA 收據存於
`data_tw_index_futures/preparation_sources/equity_clock_rules_20261023_v2/`；
PDF SHA 為 `328adb793f6ed1b033bf8bd4e7aa49fd8b221e075fcbc8caf85a32a9fc4e9b4a`。
原始資料、原 2013 對照、active controller 及固定 source release 均保留。
本輪五組相關語義／帳務／模型介面測試 **214 passed**，完整輸出見
[semantic-tests.log](../artifacts/operations/futures-equity-universe-20261023-v2/semantic-tests.log)。
追加的週月價差解析及 carry 回歸 **45 passed**，見
[weekly-spread-tests.log](../artifacts/operations/futures-equity-universe-20261023-v3/weekly-spread-tests.log)。

商品及利率期貨仍缺完整 dated financial／clock 證據；全範圍尚有 40 個未入選代碼與
已保存的區間補證工作表。非 stock／TX／MTX 的既有歷史價格合約只驗證到 2020-03-23 起，
所以新宇宙採明示的共同 2020 研究窗；沒有把這稱成完成 2011 全商品請求。
原 2011 的 CTF 108 vs 107 口來源缺口也沒有填補或跳過。

[期交所現行商品目錄](https://www.taifex.com.tw/cht/2/products)及
[股票／ETF 規格](https://www.taifex.com.tw/cht/2/sTF)只用來核對商品身分與制度；
當前網頁不是歷史生效日證據。來源目錄的 TWD 結算幣別不單獨證明所有商品的報價、
歷史可交易性或 broker 權限。現有特徵及財務 release 的研究近似、retrospective selection
標記亦保留，尚非全部 publication-time 的 PIT 完成證據。

## 已排入遠端的四組實驗

| 順序 | 名稱／English | 決策時間 | 交易宇宙 |
| --- | --- | --- | --- |
| 1 | 個股對照／Stock-only control | 08:45 | 同期個股財務來源 |
| 2 | 個股對照／Stock-only control | 09:00 | 同期個股財務來源 |
| 3 | 擴大股票類期貨／Expanded equity futures | 08:45 | 通過來源驗收的個股＋ETF＋指數 |
| 4 | 擴大股票類期貨／Expanded equity futures | 09:00 | 同上 |

全部從零訓練、共用 2020-03-23～2026-09-24 的時鐘窗，使用現行
FinancialTransformer **d32／lookback32**、global128／local64／eval16、兩卡 DDP、BF16+TF32、
一億元原帳戶、Log Utility、cap1000／patience100／min_delta0.0001；容量不足才走原 runner
已設定的有界批次重試。股票歷史特徵相同；期貨開盤參考仍按各決策時鐘選當時已知的
第一筆真實開盤成交，尚未觀測才沿用最近真實歷史成交，並保留來源與可得時鐘。
這是各時間可取得資訊的研究比較，不是固定動作重播。

Supervisor `stockagent-futures-equity-universe-20261023-v2` 已完成 CPU 資料核讀。
個股對照的兩個時鐘均已通過，涵蓋共同窗 **913,707** 筆財務日列。
`stockagent-futures-equity-universe-20261023-v3` 已在原資料 owner 的 idle 邊界接手；
只停止已完成準備的 v2 controller，沒有中斷前兩批正式研究，已通過的個股 raw tick 無需重讀。
之後先在 CPU 驗共同期間、來源 SHA 及所有實際輸入路徑，再等待前兩批 canonical queue，
透過既有 GPU lease 取得兩卡，嚴格 CUDA preflight，完整四回合工程驗收後才啟動正式訓練。
失敗組保留證據，其餘通過組繼續；沒有把排隊、四回合或 all-cash 當成正式策略結果。

**21:50 更新：**原準備 v2 已完成，v3 在其 idle 邊界接手，原正式研究未停。
個股兩組已排定。原擴大宇宙 1,587 個來源日全部讀完，993,424 筆財務日列中，
有 **1,857** 個原標為可執行的契約日未通過日內證據；因此沒有放行這兩組。

根因位於共用 `official_day_evidence`：它只解析月／月價差腿，忽略 MTX 的週／月價差。
官方總成交量含價差腿，因而錯把該部分當成一般交易簿容量。修正接受有界
`YYYYMM`／`YYYYMMW1..W5` 兩腿後，獨立重讀原官方 CSV，再扣原已驗證鉅額成交，
**1,857／1,857** 均與 retained tick 數量及 HLC 一致，未解釋差異 **0**。
其中 6 日原本只有價差腿、無一般交易；沒有捏造這 6 日的開盤、成交或容量。
見 [完整差異核讀](../artifacts/operations/futures-equity-universe-20261023-v3/weekly-spread-diagnostic.json)。

既有財務／時鐘與 active request 均未覆寫；新 Supervisor
`stockagent-futures-equity-weekly-spread-20261023-v4` 已實際啟動 canonical 財務重建，
之後重建並驗收兩個時鐘，對照 v3 個股組相同日曆，再依既有 lease、完整工程及正式 lifecycle
執行擴大宇宙兩組。21:50 已進入 4,968,220 列、55,655 個實體生命週期的財務準備，
這是來源工程進度，不是新候選的 GPU 收斂成果。

- 修正 source SHA `62d12e1c2664e97e7378d98c3facec7e687ffeb9ec976d3ff5dc3546f9dff6a0`，
  共 1,852 個 source/config 成員；新增核心修正僅共用價差腿解析，不改模型／loss／帳本。
- 修正請求：[v4 queue-request.json](../artifacts/operations/futures-equity-weekly-spread-20261023-v4/queue-request.json)，
  SHA `71b096cdc477dab490bf997c89f0984d0afdecbb29ef02fc705e38955df0fe37`，18 個 pins。
- 新財務：`data_tw_index_futures/training_prepared/tw_all_twd_futures_history2011_20261023_v2/financial/`。
- 新時鐘：`data_tw_index_futures/training_prepared/tw_futures_equity_weekly_spread_20261023_v3/twd_equity/clocks/`。
- 擴大兩組的正式目標改為
  `artifacts/markets/tw_futures_equity_weekly_spread_20261023_v3/{twd_equity__0845,twd_equity__0900}/`。
  原 v2 目標只執行已通過的個股兩組；不能把被擋下的舊擴大案例當已排入 GPU 訓練。
  最新 [21:53 遠端排程／階段收據](../artifacts/operations/futures-equity-weekly-spread-20261023-v4/remote-status-readback-20261023T135338.json)
  確認 v4 正在執行 canonical 財務準備，v3 與前兩批研究 owner 均存活；新兩組尚未進入 GPU。

- CPU 準備請求：[v2 queue-request.json](../artifacts/operations/futures-equity-universe-20261023-v2/queue-request.json)，
  SHA `d84eb86ac718b44bdc31987b8ad6264df20cd1f5a83dd5c7b6bfd14ef6bd518e`。
- 訓練接手請求：[v3 queue-request.json](../artifacts/operations/futures-equity-universe-20261023-v3/queue-request.json)，
  SHA `a8d6ca6b44be51863a793bb36cd792aea4c653298862d425fe5769bdead747f2`，共 23 個 pins。
  v3 修正 frozen runner 改變 cwd 後的相對資料路徑，沿用前輪已實測部署的股票輸入綁定。
  [獨立路徑核讀](../artifacts/operations/futures-equity-universe-20261023-v3/runtime-input-path-readback.json)
  證明四組的七項 active 路徑均為絕對路徑、除仍準備中的擴大 clock／reference 外皆存在；
  原 source bytes 未變，runner executable mode 已單獨核對。
  [個股控制日曆預檢](../artifacts/operations/futures-equity-universe-20261023-v3/stock-control-calendar-preflight.json)
  也已由 canonical config builder 與日曆驗收器確認兩個時鐘均為 `accepted_calendar_context`；
  這是 CPU 配置驗收，不是新四組已在 GPU 訓練的證明。
- 固定 source SHA `e02d8712f4567a491ab04b294bdf79765876f8d4982b45387b24ffa8b23e6ad2`；
  遠端已核對 1,851 個 source/config 成員；v2 準備請求有 21 個 pins。
  相對前一 release 的既有程式改動為四個共用檔；另有 22 個來自較新已提交 checkout 的新增工具檔，
  完整差異見 [source-diff-proof.json](../artifacts/operations/futures-equity-universe-20261023-v2/source-diff-proof.json)。
  source receipt 不宣稱完整 imported-code coverage。
- 新資料：`data_tw_index_futures/training_prepared/tw_futures_equity_universe_20261023_v2/{stock_only,twd_equity}/clocks/`。
- 新正式產物目標：`artifacts/markets/tw_futures_equity_universe_20261023_v2/<case>/`；
  是否已產生 checkpoint 必須讀其 lifecycle，不能僅由目標路徑判定。
- 既有六組：`artifacts/markets/tw_stock_futures_history2013_20261023_v2/<case>/`。
- 已驗收的 08:45 模型實際目錄為
  `artifacts/markets/tw_stock_futures_history2013_20261023_v2/clock__0845/fold_12/`；
  09:00 為同根 `clock__0900/fold_12/`。
- 後段四組條件化／Muon 組合：`artifacts/markets/tw_stock_futures_conditioning_history2013_20261023_v3/<case>/`。

## 計算效率與仍未通過的邊界

現行 CSR、日期 view、共享轉換、固定高維 schema 的版本，先前已通過完整兩卡
A/B/B/A：08:45 穩態完整 epoch MAX-rank **103.9647→27.8278 秒（3.736×）**，
09:00 **103.5657→27.7207 秒（3.736×）**。完整四回合 case 則分別只有
**1.2448×／1.6399×**，未把局部或穩態倍率套到整個冷啟動流程。

目前 09:00 的非同步 host timer 曾記錄每批 finite check 約 625ms、forward 約 571ms、
backward 約 610ms，但整批 total 約 1,190ms。這些值包含巢狀計時及隱含 GPU 等待，
不能相加或宣稱全部 625ms 都是可移除的 CPU 浪費。
已排的 device finite guard 用每批 device flag、epoch 更新前統一驗收，仍保留中間非有限
失敗的 sticky flag。需兩卡失敗注入 oracle、完整 A/B/B/A、有限梯度、整口帳務與
至少 3% 的預定性能門檻才採用；新四組只沿用該輪實測接受的結果。

新商品軸同樣必須測完整 epoch／fold，包括驗證、test 曲線、繪圖與 checkpoints。
目前不能宣稱只剩純硬體運算；下一個待驗瓶頸是同步與批次準備，而非再加一個 kernel 套件名。
完成的工程 payload 由原 testing-disposition owner 留下精簡證據後回收；
正式 checkpoint、來源、失敗診斷與重建憑證不混入暫存清理。
本輪本機五個已完成 build／pytest 私有目錄也已沿 Git／pytest owner 退役，
核對 frozen source、bundle SHA／CRC、請求所綁定配置、無程序引用及 quarantine 前後清冊後，
實際釋放 **86,495,232 allocated bytes**。Git move 僅更新 `.git` 指標 mtime，
其 bytes／inode／mode 保持一致；正式模型、來源、資料及冷庫均未刪除。
見 [精確清理收據](../artifacts/operations/futures-equity-universe-20261023-v2/temporary-retirement-result.json)。
週月價差修正後的另兩個私有 build／pytest 目錄也經同一 owner 驗收退役，
另釋放 **84,938,752 allocated bytes**；見
[追加清理收據](../artifacts/operations/futures-equity-weekly-spread-20261023-v4/temporary-retirement-result.json)。

## 文獻與實驗取捨

以下是本輪選讀、可追溯的原始文獻，涵蓋起點與近年方法；不是所有論文均已實作或
在這個市場驗證。網路上的倍率／績效不作本機實測結果。

| 原始來源 | 與本輪的連結及限制 |
| --- | --- |
| [Kelly, 1956](https://doi.org/10.1002/j.1538-7305.1956.tb03809.x) | 複利成長支持簡單 log objective；不構成同時最小化 MDD 的保證。 |
| [Deep Hedging, 2018](https://arxiv.org/abs/1802.03042) | 摩擦與受限持倉須進入序列決策。現有整口帳本與帳戶回饋已具備這項核心，不重複新增。 |
| [Deep Momentum Networks, 2019／2020](https://arxiv.org/abs/1904.04912) | 預測、部位與波動尺度需共同考慮；本輪先隔離名目本金座標及商品軸，不把更多模型層數當成答案。 |
| [No-Transaction Band Network, 2021](https://arxiv.org/abs/2103.01775) | 可作下一階段顯式持倉區間頭的候選；該文的避險理論不保證本策略 alpha。此次未宣稱已新增 band head。 |
| [Cost-aware Portfolios in a Large Universe of Assets, 2024／2025](https://arxiv.org/abs/2412.11575) | 擴大商品池需連同成本／容量比較；該文 US equity 的結果不能直接套給 TAIFEX 整口期貨。 |
| [Multi-objective Portfolio Optimization Via Gradient Descent, 2025](https://arxiv.org/abs/2507.16717) | 釐清多目標組合優化的使用邊界；本輪遵守簡單 Log Utility，不混入新的加權多指標 loss。 |
| [End-to-End Parametric Portfolio Policies for Cross-Asset Futures Timing, 2026-07](https://arxiv.org/abs/2607.00475) | 期貨端到端模型在成本後不必然優於簡單規則；支持保留共同期間與成本完整的對照，不把 Transformer 名稱當優勢證據。 |
| [SparseDitto, 2026-08／09 修訂](https://arxiv.org/abs/2608.05033) | 依矩陣稀疏型態、算子與 GPU 同時選格式及排程，支持量測後改寫專用版本。其 SuiteSparse／GCN 及 RTX PRO 6000／H200 結果不是本機金融模型倍率；先處理已見的同步，再以完整 epoch 驗 kernel。 |

選模仍按驗證 Log Utility；完整列出同期間費後 return、Sharpe、Sortino 及 MDD，
若候選未同時改善，就保留其失敗結果。多次查看過的 2026 期間是研究比較，
不能再稱未碰過的最終測試集，也不因測試結果較漂亮而換選模規則。
