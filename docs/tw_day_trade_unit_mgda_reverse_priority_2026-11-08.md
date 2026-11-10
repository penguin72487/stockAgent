# 台股當沖 1,000 萬：風險報酬優先、倒序 folds

使用者於 2026-11-08 選擇依 CAGR／最大回撤圖的右上角優先，並從較晚的
fold 往前補算。沿用已完成的 34 組 OFAT、1,000 萬本金與收盤未回補漲停
空單 7% 額外成本。這次接續台股當沖研究，沒有啟動期貨實驗。

## 排程與比較範圍

- [原始矩陣](../configs/ablations/tw_day_trade_unit_mgda_10m_ofat_20261104_v1.yaml)
  的 34 組 fold 10 都已有成功的 worker 結果。啟動前另用 canonical lifecycle
  驗證 checkpoint、曲線、報表、回測與完成標記，以及原 100 回合無改善條件。
- 固定資料的 canonical fold IDs 是 **1–10**，沒有 fold 11。每個策略先驗證並
  重用 fold 10，再依 **9→8→…→1** 補算，共 306 個新的策略／fold 工作。
- 策略是外層排序，fold 是內層倒序；不會把較晚 fold 的模型當較早 fold 的
  初始化。每組重建模型與 optimizer、使用自己的訓練資料擬合正規化；倒序
  還會還原該 process 原始的 rank RNG，使較早初始化不依賴較晚訓練多久。
- 最多 1000 epochs、連續 100 回合無改善停止；各消融自己的 d、lookback、
  loss、output、SGD 及已實測可容納的 batch 保留。沒有移植工程測速的權重。
- [倒序矩陣](../configs/ablations/tw_day_trade_unit_mgda_10m_reverse_priority_20261108_v1.yaml)
  只增加策略優先序、fold 執行順序與進度圖設定。

完整 test horizon 仍沿原設定延伸至 2026-10-02：fold 1 為訓練 2015、
驗證 2016、獨立帳戶測試 2017–2026；fold 9 為訓練 2015–2023、
驗證 2024、獨立帳戶測試 2025–2026。另由 canonical stitched deployment
取各 fold 真正接管的日期，不能把重疊的獨立測試帳戶直接串接為一條曲線。

排序固定使用原 fold 10 的 **2025 validation**，不拿 2026 test 選優。
對負值 MDD 與 CAGR 分別在相同 34 點做 min–max 正規化，兩軸均為越大越好，
以到 `(1,1)` 的歐氏距離由小到大排。這是計算順序，不是把兩個數字變成 loss。
排序快照固定在 `_operations/priority.json`，新 folds 完成後不動態重排。

| 優先 | 策略／Strategy | 驗證 CAGR | 驗證 MDD |
| --- | --- | ---: | ---: |
| 1 | 一層時間 Transformer／One temporal layer | 665.06% | -11.11% |
| 2 | 單一對數報酬／Log Return objective | 546.33% | -14.19% |
| 3 | 停用 QK 正規化／QK normalization off | 283.74% | -5.14% |
| 4 | 投影 L1 配置／Projection L1 | 348.82% | -11.23% |
| 5 | 線性輸出頭／Linear output head | 376.97% | -12.86% |
| 6 | 隱藏維度 64／Embedding 64 | 526.35% | -16.34% |
| 7 | 參數空間 Unit MGDA／Baseline | 224.27% | -5.18% |

這些是單一驗證年度的研究模擬，不是跨年度結論。原資料的 retrospective
feature ABI、早期 daily proxy 及研究收盤容量假設仍在，不能稱為 PIT 實盤成交。
補算較早 folds 也是已看過較晚資料後的歷史研究，不能宣稱重新取得未見測試集。

## 計算結構與驗收

1. 原 trainer 已有年度帳戶分卡、factorized features、稀疏投影、逐日帳務
   CUDA Graph 及 return-adjoint 的四次參數 VJP，沿用已驗證的雙 RTX 5090
   執行方案。單個策略在同一組 torchrun processes 中完成所有倒序 folds，
   重用不可變資料與 compiler cache，避免每個 fold 都重新載入整個 panel。
2. 原正常暖啟動 panel 階段約 55–60 秒，baseline 首次冷啟動達 1577.63 秒。
   同一策略合併執行可把九次獨立啟動降成一次；這是省略重複啟動的結構證據，
   不是宣稱每個 epoch 或整體研究已經加速九倍。
3. fold 1 只有一個完整年度帳戶。原 ownership planner 拒絕少於 rank 數的
   帳戶；現在允許空 rank，空 rank 不計算任何日期，但仍參與同一梯度同步、
   clipping、optimizer 與 scheduler 更新。真實雙 process 測試核對獨立帳戶
   oracle、四個參數 VJP、Unit MGDA、unused parameters 及唯一一次更新。
4. 計時顯示 resident metadata cache 沒有命中。canonical physical bridge
   明確丟棄 legacy `overnight_log_returns` 執行資料帶，但舊路徑仍搬運它。
   新的 opt-in `STOCKAGENT_DAY_TRADE_OMIT_LEGACY_TAPE=1` 在正式來源已接管
   帳務時省略這份搬運；CPU 原件、來源覆蓋審查、模型特徵、費稅及 canonical
   帳務保留，驗證與報表也避免配置這份不用的 buffer。
5. 用既有完整 fold benchmark 比較 A：512 MiB cache、B：2 GiB cache、C：
   512 MiB 並省略重複資料帶、D：加上 16 GiB 有界 pinned CPU 帳務來源快取、
   E：再以獨立 CUDA stream 非同步預取下一批的前 32 日。D／E 沿用既有
   immutable source cache、copy event 與 stream lifetime 實作，不快取動作、
   帳戶狀態或模型 activation。先在兩張 GPU 驗證來源 FP64 bits、切片、事件
   同步與 CPU cache 保留，再進行完整 fold。每組四回合都包含 validation、test、曲線、
   checkpoint 與正式 fold 報表；比較 epoch 3–4 的 MAX-rank wall time、GPU
   headroom、四回合 scalar 及完整回測陣列。不同訓練所得的動作不同，不能要求
   兩條獨立策略的交易逐筆相同。驗收改為：來源／日期／benchmark 與 artifact
   schema 相同、實際成交與有限梯度、固定動作下的金融前向／帳戶狀態／VJP
   對照通過；獨立訓練的 scalar／陣列差異仍記錄，沒有改動 loss 或帳務公式。
   新的[固定動作測試](../test/test_day_trade_transport_financial_parity.py)同時測試
   損失使用的壓縮終值 ABI，以及報表所需的完整 270 分鐘 NAV。
   只採用穩態至少快 3%，且包含 setup／report 的 100 回合成本推估也至少快 3%
   的候選。推估為實測四回合完整 fold wall 加上 96 個穩態回合，並非已完成
   100 回合收斂的實測。device prefetch 只適用該實測策略，其他策略須保留各自
   容量餘裕。測速權重不會接到正式訓練。

A 的初測 epoch 3–4 MAX-rank 中位數為 13.415 秒，B 為 15.570 秒；因此不能因
cache 命中次數增加就採用 B。A／B 完整 fold wall 分別為 1125.2／666.3 秒，
但 A 包含首次來源轉換與 compiler 暖機，不能把這個 wall 差額當成 B 的加速。
取資料階段仍約 44–46 ms／批次。C 初測 11.782 秒，D 初測 7.946 秒；D／E
針對這段 CPU packing 與傳輸等待比較。這些初測使用 `4976e4649905`，而非
最後獲選設定的收斂結果。

固定動作測試另外找出未壓縮 packed transport 的單日尾批會誤走 dense loader，
因而把錯誤類別交給 packed constructor。修正在共用
`PreparedDayTradeCarrySource.prepare_batch`，沒有新增獨立資料路徑；CPU 原值
測試與 GPU 完整分鐘 NAV／梯度測試涵蓋此尾批。正式 frozen source 更新為
`39718788cc36`，D／E 與暖機後 A 控制組在相同修正版重新量測，原初測僅保留
為歷史工程證據。這些四回合都尚未達正式研究的 100 回合無改善收斂條件。

修正版的遠端兩卡驗收：年度帳戶／四目標 MGDA 同步測試 8 passed，來源 bits
與預取測試 3 passed，固定動作與單日尾批測試 10 passed。既有
`cusparse_csr_transpose` algorithm 6 在兩張卡、int32／int64 索引、相同輸入的
各 8 次重算均得到 8 個不同結果 hash；最大重算差約 `2.29e-5`，相對 FP64
參考的 L2 誤差約 `2.18e-7`，全數通過既定數值容差。這證實反傳的微小跑次
差異，不能把它當成來源或整數帳務錯誤；也不等於不同訓練策略具有相同績效。
原始數字見 `_operations/sparse-adjoint-repeat.json`。

最終同版結果：暖機 A 為 **12.447 秒／回合**，D 為 **7.783 秒／回合**，
E 為 **10.364 秒／回合**。選用 D：省略不用的 legacy tape、512 MiB metadata
cache、16 GiB pinned CPU source cache，device prefetch 關閉。這個 fold 9 形狀
的穩態回合時間減少 **37.47%**，約 **1.60 倍**吞吐。

四回合完整 fold wall 為 A 608.29 秒、D 696.68 秒；D 有額外的快取建立成本，
不能說四回合完整 fold 較快。含 setup／report 的 100 回合推估為
A 1803.19 秒、D 1443.82 秒，減少 **19.93%**；實際收斂所需回合仍須等正式
訓練。正式首組 fold 9 的前幾回合已產生非零梯度、逐回合 JSONL 與
`epoch_curve_every1.png`、`epoch_timing_every1.png`，讀回詳見
`_operations/formal-start-readback.json`。其他形狀依實際紀錄評估，沒有宣稱
所有消融固定相同倍率或已證明全域硬體極限。

新的通用入口是 `train.py --fold-order descending` 與 ablation spec 的
`experiment_order`／`fold_order`。倒序加 warm start 會被拒絕；priority
必須恰好包含所有實驗一次，不能漏 baseline 或重複某組。原升序與單 fold
預設保留。累計策略報表納入所有完成的 canonical folds；消融總圖使用
absolute 模式並標明未完成期間各策略的 fold 覆蓋不同，不能混作配對比較。

正式矩陣由同一個 canonical ablation process 依序處理 34 組，避免拆成兩次
`--only` 執行後，後半段 summary 暫時遺失前半段比較。
[runtime runner](../scripts/run_ablation_runtime_runner.py)只套用測得的 transport
環境變數，再以原參數 exec frozen `coda_runner.sh`。額外 device prefetch 只對
實測策略與相同 config SHA 開啟；容量重試的新 config 自動回到一般 transport。
這個範圍限制與完整參數轉送有 8 項 CPU 測試。

完成的性能選擇另綁定來源、feature manifest、設定、launch files、GPU UUID／
driver／boot、套件版本及 CPU／cgroup 配置。同一配置重啟可重用驗收，避免在
刪除測速 payload 後再次花整輪時間測速；身分變更時重測。資料來源的完整
SHA、CUDA 與訓練 lifecycle 准入仍各自執行，不以性能快取取代。

目前量測完成與否、獲選方案及實際速度，均以本輪
`_operations/performance-selection.json` 為準；啟動／GPU busy 不代表已達純計算
瓶頸，也不代表新策略已收斂。

## 固定來源與傳輸修復

原 panel source 的 2026-10-04 外層 manifest 已在封裝整理後移除；新封裝
的 5,917 個原件、5,303,374,780 bytes、來源指紋
`871e9712fc465df1918bb9089f07a50299b773ee31d115c4352ff6c6a4fe2368`
與原研究完全相同。先經既有 rclone／edge owner 取得固定新封裝，再把新 inventory
分別對舊、新 materialization 全檔 SHA 驗證；沒有改寫原始觀察或原 feature manifest。
見 `_operations/source-repack-equivalence.json`。

另外修復既有傳輸公鑰失效，保留其他 authorized keys。來源 manifest 在 Penguin
及 Vast 都由 canonical pin 保護；排程 parent 持有實際輸入目錄 FD，經原 live
consumer lease renewal 續租，不為續租再次下載已完整 materialized 的 packed
副本。來源全文驗證、READY、pin、續租及正式訓練驗收分開記錄。

## 遠端產物位置

主機：`root@114.34.26.236:43269`。

- 正式策略、各年度曲線、模型與 folds 繼續放在
  `/root/stockAgent/artifacts/markets/tw_day_trade_unit_mgda_10m_ofat_20261104_v1/`。
- 每組 public 目錄指向已選容量 attempt，例如
  `temporal_layers__one/fold_09/`；本次 fold 9 的年度訓練目錄為
  `temporal_layers__one/train_2015-2016-2017-2018-2019-2020-2021-2022-2023/`。
  其中 `epoch_curve.jsonl`、`epoch_curve_every1.png`、`epoch_timing_every1.png`
  每回合更新；fold 9 正式回測／模型報表在收斂後完成。
- 本輪排程／准入／測速證據位於
  `/root/stockAgent/artifacts/markets/tw_day_trade_unit_mgda_10m_reverse_priority_20261108_v1/_operations/`，
  包括 `queue_status.json`、`calendar-admission.json`、`priority.json`、
  `performance/`、`performance-selection.json` 及 `formal-*.log`。
- Supervisor：`stockagent-daytrade-unit-mgda-reverse-20261108-v1`。
- 暫時的 `artifacts/benchmarks/tw_day_trade_unit_mgda_reverse_20261108_v1/input-transport/`
  在測量及精簡證據讀回後，走既有 explicit testing
  disposition 的 dry-run、程序／來源引用與 inode gates 清理；正式產物與 data
  不在其清除範圍。

測速暫存已走 canonical explicit-discard 完成；結果為
`discarded-without-preservation`，並獨立讀回確認 input-transport 目錄不存在。
曾因容器 `/proc` 權限無法完整掃描而中斷的控制器紀錄保留在 revisions。
目前清理遇到同類門檻會保留資料、記錄 deferred，繼續已獨立驗收的訓練；
不放寬刪除 gate。cleanup-only 控制器修訂以 AST 比較確認 benchmark／training
函式未變，保留原性能收據並另記 binding amendment，沒有重寫測速數字。

本機目前僅鏡像部分 metadata 與比較圖；遠端模型檔存在不表示已全部回傳本機。

本輪 frozen source 是原 `05bd2b506fd3` release 的四檔精確 overlay，再加上
共用 source bridge 的單日尾批修正，正式選用
`39718788cc36d17dcdf297b6676f1822796676ee212925da83daef159cc19a00`。
既有 checkout 其他 dirty work 沒有加入遠端版本。驗收與失敗修復收據分別保留，
包括測速目錄被 storage role gate 拒絕的早期嘗試；沒有把該嘗試當成性能數據。

Overlay 的 source SHA 不代表 shell 入口已有執行位。本輪另外恢復 frozen
`coda_runner.sh` 的 owner execute，再重新核對來源內容，記錄於
`_operations/canonical-runner-exec-admission.json`。

## 2026-11-09：恢復排程與依測試年比較

第一組 `temporal_layers__one` 已完成 fold 10 至 1；其後舊彙整程式因
驗證圖沒有接受的 calendar receipt 而以 1 結束，另外 33 組的較早 folds
當時尚未執行。修復後，彙整使用已准入的實際 fold 日期，單一繪圖失敗
會留 `plot_refresh_failures.jsonl` 並繼續後續策略；最後仍有繪圖錯誤時
整輪以非零狀態呈現。第二個預檢錯誤把完成 10 個 folds 的生命週期
當成必須「只完成 fold 10」而拒絕。現在完整 run 依實際完成集合驗收；
中斷的 run 驗收已完成 fold 10 的檔案並由原 checkpoint 路徑續跑。

排程控制器第 9 修訂及兩次修改前的原件／原效能收據保存在遠端
`_operations/controller-revisions/attempt-06/` 與 `attempt-07/`。
僅報表與排程程式使用新封裝；實際訓練程式仍是 `39718788cc36...`，
原 34 份生成設定的 SHA、兩張 GPU 與同機效能證據均已核對。原暫存
輸送測速結果沒有重算成新的完整 fold 成果。

跨 fold 比較讀每個完整 fold 自己的 `test_backtest.npz`，只取該帳戶
第一個測試年，依年而非 fold 數字對齊。日期及 benchmark hash 是獨立
cohort，異 cohort 不宣稱配對；缺少年份不插零或連線。連接的是每年
風險／報酬點，不是把獨立重設帳戶連成連續績效。獨立年度權益圖
按實際交易日畫已觀察報酬，零報酬自然水平；沒有合成未觀察日期。
2026 僅涵蓋 2026-01-02 至 2026-10-02，共 181 個交易日。

遠端即時圖在
`artifacts/markets/tw_day_trade_unit_mgda_10m_ofat_20261104_v1/live_reports/reverse_test_by_test_year/`：
`all_folds_connected.png`、`year_YYYY.png`、`year_YYYY_equity.png`、
`test_year_fold_metrics.csv` 和 `strategy_zh_en.csv`。獨立唯讀報表
Supervisor 每 60 秒觀察 fold 完成與 summary 變動；只有輸入改變時重算，
未變年度的圖保留。它不寫訓練帳務，也不與原排程的報表寫同一目錄。
本機相同路徑是一份 2026-11-09 取得的快照；遠端版本才自動更新。

台北時間 09:02 讀回：2026 年 34 組均有資料、2017 至 2025 年各只有
第一組，共 43 個完成的策略／年度觀察；第二組 `objective__log_return`
正跑 fold 9，已讀到第 72 epoch，並非完成。baseline 仍在第 7 順位。
2026 是局部年度；資料本身標示 research-only、未宣稱歷史 PIT，故圖表
不是部署績效或前瞻驗證。修正前後的分析程式及排程測試通過，並用
遠端實際資料生成和目視檢查圖片；單年跨不同 fold 編號及無交易日
保留均有聚焦測試；本次相關圖表、排程與 watcher 回歸 64 項通過。

當次可用空間從約 11 GiB 增至 276 GiB，同時遠端
`service_data/pack_compute_cache` 從約 271 GiB 變成 1.8 MiB。這個觀測
不是完成獨立冷還原的證據；本輪沒有自行刪除該快取或解除其他維運
工作的暫停。訓練 source 仍以原 lease 維持，正式模型沒有因此視為已回傳。

## 2026-11-10：恢復既有消融圖樣式，逐 fold 更新

使用者指出上一輪連線圖的畫法與既有消融圖不同。原圖可直接在遠端
`ofat_test_diagnostic_risk_return_by_fold/cagr/all_folds_connected.png`
核對：時間以藍色深淺和點大小編碼、baseline 為菱形與黑色連線，其他
策略為圓點與淡藍連線，全名置於兩側並接到最新端點。上一版年度圖
改用了實驗多色圖例和年份形狀，這不是原消融的視覺契約，現已修正。

原 paired fold 圖與 test-year 圖共用
`scripts/plot_ablation_analysis.py::_risk_return_figure`，不再維護兩套
連線圖畫法。逐年圖和連線圖的座標範圍一致；時間仍依 test 年份排序，
標籤保留原 F 編號，缺失年度保留斷線。五種指標各自輸出，維持：

- `cagr/all_folds_connected.png` 及 `cagr/year_YYYY.png`；
- `sharpe/`、`sortino/`、`turnover/`、`daily_hit_rate/` 下的同名圖；
- 上層既有 `all_folds_connected.png`／`year_YYYY.png` 是 CAGR 圖的相同位元組別名；
- `year_YYYY_equity.png`、指標 CSV 及中英文策略表繼續保留。

上述檔案均在結果根的 `live_reports/reverse_test_by_test_year/`。
只有這個 live report 目錄由逐 fold 報表服務更新；本機同路徑仍是明確
讀回的快照，並非第二個訓練或資料回傳 owner。

報表 watcher 每 10 秒檢查完成 marker／summary，輸入變動才重畫。
`live_report_refreshes.jsonl` 持久記錄每次更新納入的策略、fold、test 年，
並在渲染後再次核對輸入簽章；途中有新完成項目則標為等待續刷，
不將尚未納入的新結果宣稱已更新。年度顏色或共用座標有變時，受影響
的既有圖也會重畫，以免不同年度使用不同尺度；未變圖則重用。

本次僅重載報表 Supervisor，訓練 Supervisor PID `2952814` 持續執行。
CPU 報表版本為 `17f2df49538328263bffef21b4e8b6409067ac12620d706944faff90c0d847fe`；
相對前版僅繪圖與 watcher 兩個來源檔改變，啟動前核對 wheel、source
bundle 和源碼。遠端重畫 64 筆完成的策略／年度觀察、76 張 PNG；與
修改前 CSV 比較，全部 64 筆既有數值逐欄相同。新圖與原圖均實際讀回
目視檢查，相關回歸 15 項及 Ruff 檢查通過。

更新前的報表 wrapper／狀態／CSV 在
`_operations/controller-revisions/report-style-20261110-v1/`；安裝與獨立
讀回分別記錄於 `report-style-20261110-installation.json` 和
`report-style-20261110-readback.json`。當次仍在執行 `output__projection_l1`
的 fold 6，不能將 64 筆完成觀察視為整個 340-fold 矩陣完成。

## 2026-11-10：所有連線圖入口由同一個逐 fold owner 更新

使用者進一步要求原本 `ofat_test_diagnostic_risk_return_by_fold/` 等
入口也一起更新。前版只改 `live_reports`，原驗證／完整測試診斷入口
仍保留最後 fold 的舊圖。現以
`configs/ablations/tw_day_trade_unit_mgda_report_views_20261110_v1.json`
明確宣告四個報表面：live 年度圖、根目錄年度圖、`ofat_val`、
`ofat_test_diagnostic`。根目錄年度圖直接鏡像同一份 live 報表；
所有圖仍由 `_risk_return_figure` 畫出。

新增 completed 報表模式讀所有完整 fold，包含尚未完成相同 fold 數的
策略，不再因 baseline 只有一個完成 fold 而排除其他策略。各點按第一個
test 年對齊，保留原 F 編號；逐年圖另有唯一可對應的 `fold_XX.png`
相同位元組別名。驗證指標與完整測試期指標繼續使用原 summary 的
原始計算窗口，沒有將完整測試期默換成第一年績效。新表格保存實際
計算日期、列數及來源指紋；驗證日期來自接受的逐 fold 日曆區間，
區間 identity 帶 `admitted_interval:` 前綴，沒有冒稱逐日內容雜湊。

為避免舊 ablation engine 在 case 結束後覆寫新版，控制器第 10 修訂
將 engine 的 `postprocess_plots` 交給獨立 observer。這只移交彙整圖，
trainer 的 validation、test、曲線、checkpoint 和正式回測均保留。
移交前完整驗證 fold 6 的產物與第 197 回合 checkpoint
（no-improve=100，SHA `6db95e5725928e3751063480fa00810ef9946de8229723df5127665a1fcbab13`），
並確認 34 份原設定、訓練來源及硬體未變，保留原效能證據與 binding amendment。

最初只停 Supervisor 主群組，未能停止 torchrun 自行建立群組的子程序；
該次拒絕與恢復紀錄保留。其後按精確 case／PID 核對，使用既有
`_terminate_process_tree` 停止整棵舊程序樹，獨立確認舊 GPU ranks 均退出
才重啟排程。沒有以解除 GPU owner 或跳過來源驗證來續跑。22:50 台北
讀回已重用完成 fold 6，正跑 fold 5 第 1 回合的 validation。

新的 CPU 報表來源為
`26cbf83fad5b0d5659602231ecd7e254b59d32faa243d2d7075186d59d938528`。
observer 每 10 秒觀察完成 marker／summary；四個 view 的狀態、實際
納入 folds 及前後輸入簽章同時保存在 `live_report_watch_status.json`
與 `live_report_refreshes.jsonl`。獨立遞迴清點確認此輪根目錄所有
22 張 `all_folds_connected.png` 均被 owner 宣告，沒有遺漏舊入口。
本次新完成的 fold 6 已納入四個 view，各有 65 筆完整策略／fold 觀察：
兩個年度入口各 76 張 PNG、驗證與完整測試診斷各 105 張，共 362 張。
驗證／完整測試指標逐筆與原 summary 相同；原完整測試診斷和驗證圖
均實際讀回目視檢查。

聚焦及相關報表／watcher 回歸共 17 項通過，Ruff 與差異空白檢查通過。
部署 profile 在 `_operations/report-views.json`；安裝、所有入口讀回、
移交與舊子程序停止收據，分別為 `report-suite-20261110-installation.json`、
`report-suite-20261110-readback.json`、`report-owner-handoff-status.json`、
`report-owner-old-descendants-stopped.json`。更動前原件保存在
`controller-revisions/report-suite-20261110-v1/` 與
`controller-revisions/report-suite-owner-20261110-v1/`。
