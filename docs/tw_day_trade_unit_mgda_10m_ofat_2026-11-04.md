# 1,000 萬 Unit MGDA 最後 fold 消融研究

本輪固定初始本金 TWD 10,000,000，沿用上一輪參數空間單位梯度 MGDA
（Unit parameter-space MGDA）的 SGD、d32／lookback32、完整股票特徵與
漲停殘留空單 7% 研究成本，安排 baseline 加 33 組消融。每組從 seed 42
獨立初始化；相容 resume 僅延續該組自己的 checkpoint。

設定見[市場基底](../configs/markets/tw_day_trade_unit_mgda_10m_ofat_20261104_v1.yaml)
與[完整消融清單](../configs/ablations/tw_day_trade_unit_mgda_10m_ofat_20261104_v1.yaml)。

## 研究控制

- 由固定 feature calendar 實際解析每組最後 fold，再以 `--max-folds 1` 執行；
  遠端 34 組均通過最後 fold 10 及完整年度帳戶檢查。
- 訓練：2015-01-05 至 2024-12-31，2,439 個交易日；驗證：2025-01-02
  至 2025-12-31，243 日；測試：2026-01-02 至 2026-10-02，181 日。
  所有組別的目標日期完全一致，lookback 只改歷史特徵窗口。
- 保留原 2,757 股票槽、14,726 通道及 2330 買進持有總報酬基準。
- 收盤漲停且來源回補後仍未回補的空單，在最後研究用無容量限制平倉前，
  按殘留空單收盤市值額外扣 7%；原價差與費稅照算。所有組別都承擔此成本。
- 最多 1,000 epochs，`early_stopping_no_improve_ratio: 0.1`，沿用連續
  100 回合無改善及原 min-delta。每 epoch 保留驗證、測試、曲線、圖與 checkpoint。
- BF16 AMP、敏感帳務原精度、雙 RTX 5090 DDP；global batch 128、eval 16。
  每次一個實驗使用兩張卡，baseline 第一組，依矩陣順序接續。
- 容量失敗才依原 runner 降 global batch 至 64／32，保留各 attempt；容量參數
  改變的結果須標示有效 batch／BPTT，失敗案例亦保留並繼續後續組別。

## 消融目的

| 類別 | 組數 | 比較內容 |
| --- | ---: | --- |
| 梯度幾何 / Gradient geometry | 6 | Raw MGDA、Raw PCGrad、ConFIG、DiBS 1／5 步、偏好 log-return 的 UPGrad |
| 單一目標 / Single objective | 4 | Log Return、Sharpe、Sortino、Calmar 各自訓練 |
| 輸出映射 / Output mapping | 4 | Learned Cash、Projection L1、Score Cash v2、Log Cash |
| 跨股票上下文 / Cross-stock context | 2 | 分別移除 latent factors 或 market tokens |
| 時間結構 / Temporal structure | 8 | 時間層 0／1、位置編碼／RoPE／QK norm 關閉、完整時間 query、平均／可學習池化 |
| 表徵 / Representation | 7 | 特徵 RMS 關閉、RMSNorm、GELU、d64／d128、lookback64／128 |
| 步長與評分頭 / Step and score head | 2 | SGD LR 0.03、線性評分頭 |
| 基準 / Baseline | 1 | 原 Unit MGDA、SGD LR 0.1、d32／lookback32 |

Raw PCGrad 應與 Raw MGDA 配對，才能隔離梯度合併方法；直接與 Unit MGDA
相比會同時改變正規化與合併方法。ConFIG／DiBS／UPGrad 保留既有實作的
MGDA 對應步長控制。Mean／Attention pooling 與 `temporal_query__full_then_last`
配對，隔離 pooling；相較原 last-only baseline 則屬 query 加 pooling 的架構配套。
單一 loss 是目標函數替換，並非四項 loss 平均，也不是逐一刪除某個任務的研究。

驗證期依原 net log utility 選 checkpoint；測試結果只作歷史診斷。本來源仍屬
沿用的回溯研究 ABI，含早期 official OPEN/CLOSE proxy 與研究用最後平倉，
2026 年也已被多輪研究查看；本輪不是新的未見測試集或實盤成交證據。

## 遠端實作與驗收

本輪只增加設定，沿用已驗收 frozen source
`20261103T061646746478Z-05bd2b506fd3`，source SHA256：
`05bd2b506fd3c8313377a059ca8d8592f86b0e71d36ae632ab8d04ab07827cfc`。
新設定的來源 SHA、遠端 launch YAML、消融順序與原加速配方身分另外固定在
`_operations/request.json`；不把目前 checkout 的其他 dirty 改動帶入訓練。

訓練仍由 `scripts/run_ablation_experiments.py`、`coda_runner.sh` 與 canonical
trainer／checkpoint／report 元件執行。Supervisor 只監督本輪啟動器；GPU 由原
`manage_gpu_jobs._gpu_lease_command` 排他准入。啟動前已完成：

1. frozen source／bundle 核驗，三份原 materialized lease、READY、alias 與來源
   inventory 的完整雜湊重驗，沒有續改 lease 或刪來源。
2. 真實雙卡 CUDA strict preflight，無 CPU fallback。
3. 遠端 34 組解析設定 dry-run、固定本金／成本／scratch 檢查。
4. canonical fold、完整年度帳戶與逐組相同 train／val／test 日期檢查。

重用原 verified CSR、內容定址 RMS cache、稀疏 events、CUDA Graph 與平行帳務
adjoint。新模型形狀或正規化會依原內容鍵建立自己的快取；CSR disk cache 保留
原 10 GiB 空間保留線，不把其他研究資料當作可刪暫存。既有 baseline 工程速度
不是全部消融的速度或硬體極限證明，實際耗時由各組完整 epoch／fold 計時回報。

## 產物與進度

遠端根目錄：
`/root/stockAgent/artifacts/markets/tw_day_trade_unit_mgda_10m_ofat_20261104_v1/`。

- 排程准入與啟動階段：`_operations/queue_status.json`。
- 完整清單與設定身分：`_operations/request.json`、`spec.yaml`、`base.yaml`。
- 資料／程式／GPU／日期驗收：`_operations/source-admission.json`、
  `code-admission.json`、`strict-cuda-receipt.json`、`calendar-admission.json`。
- 排程 log：`_operations/formal-ablation.log`。
- baseline 實際訓練：`_capacity/baseline/attempt-00/training/`。
- 其他組別：`_capacity/<experiment_id>/attempt-XX/training/`，獨立保留失敗 attempt。
- 各組即時階段：其 `training/progress.json`；loss 與耗時：
  `training/train_2015-2016-2017-2018-2019-2020-2021-2022-2023-2024/epoch_curve.jsonl`
  及同目錄 PNG；模型與資產曲線在 `training/fold_10/`。
- 完成比較：根目錄 `summary.json`／`summary.csv` 及 val／test 分別產生的消融圖。
  dry-run 的 validated 狀態只證明設定可解析；正式完成看各 fold completion 與實際績效。

Supervisor 程序為 `stockagent-daytrade-unit-mgda-10m-ofat-20261104-v1`。
本次排程已啟動，baseline 先行，其餘 33 組等待；實際 epoch 及完成狀態以當前
canonical progress／curve／completion 讀回為準。
