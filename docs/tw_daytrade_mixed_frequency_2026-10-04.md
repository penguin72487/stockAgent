# 當沖混合更新頻率資料與遠端準備（2026-10-04）

## 1. 執行進度與驗收邊界

已完成來源清點、受限私人來源包、Syncthing exact edge hydration／完整解包驗證、
2014/01/06～2026/10/02 遠端混合頻率建表與正式公司行動伴隨驗證。
完整 dense panel／訓練資料閘門及雙卡 DDP 尚未啟動，原因是遠端磁碟不足。
尚不能把程式測試、CUDA 環境通過或索引同步描述為正式訓練資料就緒。
不啟動正式長跑，不修改網頁／Discord 部署，不中斷既有 GPU 工作。

- 本機任務：`tw-daytrade-mixed-frequency-20261004`。
- 來源截止：`2026-10-02`；原總下載收據仍寫 `2026-10-01`，不混淆兩個證明。
  本輪另外固定實際來源 SHA、衍生表收據、股票檔、交易日曆與公司事件資料。
- 來源包：`tw-daytrade-mixed-sources-20261004-v1`，1,856,526,936 bytes，
  2,757 檔官方股票檔，45 個 canonical 公開數值與 208 個 FinLab 數值候選。
- 僅供本人在 vastai1T 私人非商業研究；使用者 10/4 明確授權且表示帳號允許。
  不公開，不包含帳密，不改原 FinLab／FinMind 全原始庫的不可發布政策。
- 270 項頻率／語意／載入／規則／快取／公司行動／欄位驗收回歸通過；另外 32 項共享回歸通過
  （測試可能重疊，不相加作為唯一測試數）。
  遠端 `check_environment.py --require-cuda --strict` 退出 0。
- 實際保留 251 個數值，45 個公開數值＋206 個 FinLab 數值；四通道與原有
  OHLCV／開盤資訊合計 1,010 通道。特徵表 5,948,237 列、1,901,639,855 bytes，
  完整建表 458.06 秒。股票決策列分母 5,815,801，不含 executor-only 規則列。
  這是資料工程結果，不是模型報酬或完整訓練效能。

## 2. 清點不是模型維度

[完整來源清點](../artifacts/data_quality/tw_daytrade_mixed_frequency_20261004/catalog/catalog_summary.json)
包含 88,762 個物理欄位、2,342 個來源登記列、1,302 個 FinLab 語意資料鍵，
及 404,936 份 FinMind 現行有值完成分區收據。排除股票代碼寬表的重複軸後，
[分類清冊](../artifacts/data_quality/tw_daytrade_mixed_frequency_20261004/admission_v3/feature_classification.csv)
有 12,446 列；這仍不是獨立模型特徵數或完整歷史覆蓋數。

- [本輪選定數值、頻率與覆蓋](../artifacts/data_quality/tw_daytrade_mixed_frequency_20261004/admission_v3/selected_source_features.csv)：253 個來源候選，251 個實際入模數值；另兩個季報指標未通過有變化的觀測門檻。
- [排除清冊](../artifacts/data_quality/tw_daytrade_mixed_frequency_20261004/admission_v3/excluded_features.csv)：含房屋／不動產 3,671 列、來源管理／識別碼 3,025 列，以及標籤、交易規則、損失資訊的舊轉換等。
- [尚待接線與證據](../artifacts/data_quality/tw_daytrade_mixed_frequency_20261004/admission_v3/remaining_feature_worklist.csv)：保留粒度、單位、公布時鐘、修訂版或來源品質待驗證的原因。

覆蓋分母是所有股票決策列，不是「該科目適用的公司數」。例如公開成交量約
99.97%、本益比約 69.80%、股價淨值比約 90.41%；後兩項低於 100% 不證明下載
損壞，須另分虧損／不適用／未發布／來源缺漏。清冊也記錄原觀測數、可用
起訖、日曆範圍外／尾端尚不可映射的期數及已知異常 barrier 數。
本輪確定的 13 筆異常已遮罩且保留原值（持股比例超過 100%、ETF 折溢價
−999.99 等）；未知值不當成零，也沒有把低頻狀態用日頻缺列率判壞。

本輪 footer／現行收據檢查沒有報來源檔案失敗，但仍有 19 列物理清冊部分覆蓋、
13 列所檢範圍全空與 190 列舊 OpenBB 成品需改查新 L1。不是所有 bytes 都已
逐一核 SHA，也不是每支股票在每個交易日都有該指標。
FinMind 的既有 missing-only 經濟定義映射留在清冊，不因名稱相同而當作通過：
必須重查報告期間、單位換算、重疊值與衝突才能補洞；本包尚未加入這些補洞。
TEJ query preview 未證明歷史粒度／時間，不加入；海外、逐筆、訂單簿等未接線
來源保留待辦，不假裝已合併，也不把等待接線稱為下載毀損。

## 3. 不同頻率如何變成 09:00 特徵

| 類型 | 候選數 | 對齊與有效期 |
| --- | ---: | --- |
| 日頻 | 49 | 前一已完成交易日／已映射可用交易日；不跨缺日沿用 |
| 月頻狀態 | 16 | 已公布狀態，最多 62 日；不由參考月份倒填 |
| 季報 | 154 | 已知上傳日優先，否則註明法定期限代理；最多 200 日 |
| 持股分布 | 1 | 使用既有有日期版本的月／週頻規則與 62／14 日期限 |
| 營收 | 8 | 公布月而非營收所屬月，最多 62 日 |
| PMI／NMI | 25 | 前月觀測依公布規律，最多 62 日；NMI 首次公開前不可用 |

每個有效數值配四通道：值、`__available`、`__age_days`、`__updated`。
0 是有效值；NULL／非有限值是不可用，不用未來值插值。
年齡從可用交易日算，不是聲稱知道精確秒級公布時間。
`__updated` 指來源觀測的對齊日，不以數值改變推斷更新；來源明確 NULL／
非有限值的 barrier 也能標示更新，但 `__available=0`，不能當成有效數值。
不存在的報告不會被當成新公布。生命週期重掛不沿用前一期掛牌的舊狀態。
總體／市場狀態不因個別股票新上市而失效，與股票自身生命週期分開處理。

遠端首輪實際建表發現 PMI 最新月份已下載，但交易日曆只到 10/2，尚不能
確認該月規則所需的第三個交易日。新版 `v8_calendar_bounds` 將日曆尾端或
範圍外的未知公布期延後，不捏造未來公布日期；前一期仍須通過有效期才能
沿用。日曆中間缺整個月份仍阻擋。原始來源包的規則收據不改寫，建表另記
實際使用的程式版本與 `unmapped_release_periods`。

使用現有公布規律，再加 **1 個曆日**研究安全延遲；非交易日等待並不是額外
增加安全延遲。期限／規律代理與當期修訂數值不等於歷史原始 PIT，
所以仍標記 `research_only=true`、`historical_point_in_time=false`、
`live_eligible=false`，不能直接用來宣稱實盤無洩漏。

一個重要的接線修正：canonical 當沖模型的特徵視窗到 t−1，所以已經可在
t 開盤使用的衍生特徵存到 t−1 的特徵列。這不是提前交易，也不是第二次公布
延遲；模型仍在 t 決策。`_twpub_*` 資格、事件與執行規則完全留在原日期，
不能隨模型特徵一起移動。

規律參考以生產者為準：[TWSE 第 36 條](https://twse-regulation.twse.com.tw/tw/law/DOC01_print.aspx?FLCODE=fl007009&FLNO=36)、
[NDC PMI／NMI 說明](https://www.ndc.gov.tw/nc_337_2268)、
[TDCC 官方持股分布](https://original-www.tdcc.com.tw/portal/zh/smWeb/qryStock)。
[政府資料目錄](https://data.gov.tw/dataset/11452) 的「每 1 月」標籤與目前 TDCC
生產者週資料語意不同，不能單靠目錄頻率覆蓋有日期的歷史規則。

## 4. 訓練與實作

沿既有 v10 last／last-only、22 基底、d_model 32、零輸入瓶頸與年度 walk-forward；
新增外部數值及四通道 allowlist，不複製 trainer／loss，不啟用稀疏訓練。
新的來源與特徵 ABI 使用全新 artifacts，`resume=false`、無舊 pretrained root。
既有 09:00 決策、歷史 09:01 起 50% 分鐘容量續單及收盤不限容量的研究執行
假設不變；不補造執行價，不呼叫 Shioaji broker 模擬。

核心入口：

- `scripts/prepare_tw_day_trade_feature_catalog.py`：來源現況與物理／語意粒度清點。
- `scripts/stage_tw_day_trade_mixed_sources.py`：本機只整理有 SHA 的來源投影，不建 ML panel。
- `stockagent/data/tw_public_release_schedule.py`：共用有限 carry、NULL barrier、生命週期與更新狀態。
- `scripts/prepare_tw_day_trade_mixed_frequency.py`：在 Vast 分批建特徵視圖與新設定。
- `scripts/report_tw_day_trade_mixed_admission.py`：全來源分類，不把未驗證候選變成合格。
- `scripts/verify_tw_day_trade_mixed_view.py`：全表經濟鍵、Float32／Float64、可用計數與四通道不變量驗收，不建完整 dense panel。
- `stockagent/data/panel.py`：原 Float32 模型視圖保持 Float32 工作集；規則仍為 Float64。

每個 chunk 共用一次日期／股票唯一鍵檢查與排序；按日期排列 Parquet row group
支援真實裁切；規則每年讀一次，不在每個 chunk 重讀整份來源。分批 1／2
交易日的 round-trip 驗證數值、NULL、可用數量、規則與尾日完全相同。
來源保留 Float64 原值，只有 ML 特徵視圖 Float32，沒有永久 BF16 或捨棄
財務精度；金融 reduction／帳戶／執行價格未更改。

遠端有界合成載入實測（100,000 列 × 128 Float32 通道，含 Float64 規則；
交錯各三次、相同 Polars 4 threads）：中位 0.5774 → 0.5299 秒，峰值 RSS
884,400 → 555,396 KiB，輸出 SHA 完全相同。這約 8% 時間／37% 記憶體改善
只屬於該 loader 測試，不是完整 epoch／fold、冷磁碟或投資報酬驗收。

## 5. 遠端交付與剩餘閘門

隔離程式放在
`/root/stockAgent/artifacts/markets/tw_day_trade_mixed_frequency_20261004_v1/code-v7`，
固定 source SHA `085badc83b830e3a205df763b789abde915eb7d8864b31bfc0e93df5aea9bd43`；
1,201 個來源／設定依賴檔由 canonical code release 核驗。
程式／設定可用 SSH 控制傳輸；**資料只走 Syncthing 與原 exact edge owner**。
本次 package JSON 未發現 credential-key 命中，不讀取或複製 `.env`。

固定來源 snapshot：
`tw-daytrade-mixed-sources-20261004-v1-20261004T070957929924630Z-l0-penguin-21fa1362b7b70c17`。
遠端完整 READY 核驗時間 `2026-10-04T07:18:37.079558701Z`，
source manifest SHA `d151a6c4249bb7f0fad48099e2b293c0d37b5cd76f3764aadcd3c10be5d35cb7`。
[交付收據](../artifacts/operations/tw_daytrade_mixed_frequency_20261004/remote_source_delivery.json)
記錄 24 個 payload 物件共 1,854,193,130 bytes；解包含側錄共 2,979 個檔案、
1,860,689,135 bytes。完整驗證後 canonical edge 只移除這 24 份暫存 payload
複本；D 冷庫原物件及遠端 materialized 資料皆保留，可重新取得。

特徵建表沿固定 `code-v5`（SHA
`3b3fc2d7ecf662007ca7b1e4ee5bb628f8cfeff947b4f43e322df58c068c9454`），
`code-v6` 另補足共用 panel 要求的正式公司行動伴隨資料。這層只核 SHA
複製小型 reference／entitlement／summary／raw-manifest，不複製原公開特徵大表、
不改 X；不用會越過 root-bound raw receipt 保護的 symlink。
`code-v7` 加入分類覆蓋與 columnar feature verifier；未改模型、loss 或價格算法。

首輪 `prepared` 的失敗半成品保留；新版使用全新 `prepared_v2`，不覆蓋
已接受資料、舊快取或舊訓練結果。
[特徵視圖收據](../artifacts/operations/tw_daytrade_mixed_frequency_20261004/remote_view/dataset_manifest.json)
固定 matrix SHA `2c9eef73fd43b39e7c73fd3d7529ae6b44ce72b6754881e520a0399e2d1da611`。
[全表欄位驗收](../artifacts/operations/tw_daytrade_mixed_frequency_20261004/columnar_feature_acceptance.json)
已通過，完整 5,948,237 列鍵無重複、可用計數逐項符合建表收據，模型 Float32、
規則 Float64、四通道 NULL／有限值／最長期限不變量通過；耗時 76.41 秒。
這不構成 full panel、金融執行或 GPU 訓練驗收。
[正式公司行動驗收](../artifacts/operations/tw_daytrade_mixed_frequency_20261004/formal_action_acceptance.json)
使用共用 loader 核驗 34,583 個 reference 事件、18,801 個 exact-cash 事件與
3,460 個因果公告避讓事件；exact archive 覆蓋 2014/01/01～2026/10/02，
仍保留 archive 原有 exact／avoid 分類，不聲稱所有減資／換股條款都精確。
尚須 full `train.py --check-data-only` 與雙卡 DDP 完整工作流驗收。
CUDA 嚴格檢查已通過不等於新來源、雙卡 DDP 或完整訓練已通過。
準備過程保留舊程式包、資料、快取與產物，沒有自行刪除其他工作資料。

本輪實際容量預檢：2014 起 3,109 個交易日、2,757 支股票，
已正規化 251 個數值（另有 2 個不合變化條件），cached X 預估 1,009 通道。
單一 Float32 dense cube 為 34,594,626,468 bytes，而當時磁碟剩
23,080,853,504 bytes；還不含其餘執行陣列、分鐘 cache、compile 與訓練產物。
可見 cgroup 採不假設 page-cache reclaim 的保守 RAM 餘額約 51.1 GB。
這是容量估計，不是已建立 panel；在取得足夠空間前不啟動完整 cache 建置，
不縮減使用者選定特徵、不任意刪舊資料，也不將 `training_ready` 改成 true。
canonical edge `gc --dry-run` 已檢查 6 份 materialization，全部因 pin 或有效
lease 保留，`would_evict=0`、payload `would_delete_bytes=0`。這不是可以改用
手動刪除的授權；剩餘容量需擴容或另取得有範圍的安全整理授權。

最終已完成視圖後的
[容量預檢](../artifacts/operations/tw_daytrade_mixed_frequency_20261004/training_capacity_preflight.json)
於 `2026-10-04T07:40:31Z` 記錄磁碟剩 21,384,650,752 bytes、保守 RAM headroom
50,207,424,512 bytes。單一 X cube 已比剩餘磁碟大，未開始 full panel，也未
啟動模型／optimizer。建議先提供至少 60 GB 可用空間再重新量測；這是保留
額外 cache／checkpoint 餘裕的操作目標，不是已測得的完整 peak-space 上界。

15:51 台灣時間再次刷新
[容量收據](../artifacts/operations/tw_daytrade_mixed_frequency_20261004/training_capacity_preflight_final.json)：
其他工作釋出空間後可用 33,975,660,544 bytes、RAM 保守餘額約 84.30 GB。
本輪沒有把此淨變化記為自己的清理成果；單一 X cube 仍超過磁碟餘額約
0.619 GB，而且完整工作流還需其他產物。保留 `training_ready=false`，
後續須重新量測完整磁碟／RAM 峰值，不能因單一 cube 接近放得下就放行。

## 6. 遠端設定與後續命令（目前容量閘門未通過）

設定已產生於遠端 `prepared_v2/training.yaml`，產物根為同一私有 experiment
的 `training`，不寫入其他模型、不續接舊 optimizer。容量處理好後，先用
固定 checkout 重新核對資源，再跑 canonical 資料閘門：

```bash
cd /root/stockAgent/artifacts/markets/tw_day_trade_mixed_frequency_20261004_v1/code-v7
source scripts/runtime_env.sh
export STOCKAGENT_CODE_RELEASE_RECEIPT=/root/stockAgent/artifacts/markets/tw_day_trade_mixed_frequency_20261004_v1/code-release-v7/release.json

run_fintech_python scripts/check_environment.py --require-cuda --strict && \
run_fintech_python train.py \
  --config /root/stockAgent/artifacts/markets/tw_day_trade_mixed_frequency_20261004_v1/prepared_v2/training.yaml \
  --check-data-only
```

這段尚未在全資料上執行，不是正式訓練啟動許可；data-only 不建立模型、
optimizer 或 fold-completion。通過後仍須以雙卡 DDP、原 global batch 32、
完整 fold／每 epoch 評估／曲線／同步 plot／checkpoint 及 epoch 3+ max-rank
wall 驗收。舊 loader 有界 benchmark 不替代這一步，不能宣稱已達理論最低耗時。
