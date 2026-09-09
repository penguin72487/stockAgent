# vastai1T 期貨訓練準備（2026-09-06）

本次準備兩種模式：`tw_futures_portfolio_day` 留倉訓練，以及
`tw_stock_futures_day_trade_0845_minute` 個股期貨當沖訓練。
兩者保留 FinancialTransformer、多基底、BF16、雙 GPU、1000 epochs 與原年度切分。

操作與驗證記錄集中在
`artifacts/operations/vastai1t_futures_prepare_20260906/`。
兩份資料均已通過 `use_tw_public.log`、`use_tw_futures.log` 的 full verification
與 `READY`。留倉面板為 5,328 日、1,936 槽、116 特徵，18 組年度切分檢查通過，
詳見 `carry_data_readiness.json`。

| 資料 | 固定發布版本 |
| --- | --- |
| 台股歷史與特徵 | `tw-public-20260906T151104843336596Z-l0-penguin-246aab6e5c72e427` |
| 期貨日資料、歷史 tick、08:45 當沖分鐘資料 | `tw-futures-20260906T153439664344902Z-l0-penguin-3207765abbe75296` |

台股版本包含 114,841 個檔案；期貨版本包含 1,859,740 個檔案。
資料經 catalog 發布、Syncthing 傳輸及 `stockagent-data use --snapshot-id` 驗證解包。
接收節點使用 index-only 模式；解包後的資料不可寫入。
原有遠端 `data_tw_futures` 目錄與 `data_tw_public` 連結保留，新的 runtime YAML
直接指向這兩個固定版本，面板快取另寫入 `artifacts/cache`。

在 **vastai1T** 的 `/root/stockAgent` 執行留倉訓練：

```bash
cd /root/stockAgent
bash artifacts/operations/vastai1t_futures_prepare_20260906/train_carry.sh
```

啟動器會重新確認指定資料版本、續租快取、檢查 CUDA，然後使用 `carry.yaml` 啟動
既有 `train.py`，並在訓練期間保留可供快取監控器偵測的資料引用。
預設熱快取租約為七天；index-only 節點重新 `use` 時可能需要再次接收
對應封裝物件。訓練輸出使用本次資料版本專屬的目錄，不接續舊資料版本的 checkpoint。

當沖模式先在遠端執行資料預檢：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config artifacts/operations/vastai1t_futures_prepare_20260906/day_trade.yaml \
  --check-data-only
```

當沖時序保留 08:45 決策、首根 08:46 右標分鐘執行，13:20 啟動出場流程，
13:30 前完成沖銷；無成交或流動性不足依執行契約處理。
本次正式分鐘資料只有 50 個交易日（2026-06-25～2026-09-03），75,434 根有效事件分鐘。
原本自 2014 年開始的年度訓練因此仍有歷史缺口，預檢必須拒絕啟動。
以日頻個股期貨候選交易日清單比對，缺少 3,042 個交易日的分鐘來源；
這是候選日期檢查，未取代完整台股面板日期的驗證。
缺口補齊後可使用同目錄的 `train_day_trade.sh` 啟動。

期貨庫另有 742 組 R1／R2 連續合約 tick，歷史最早從 2020-03-23 開始。
抽查資料僅記錄連續合約代號，未帶實體交割月身分，不能直接混入目前的實體合約
分鐘執行契約；也不足以補齊 2014～2019 年。清單存於 `historical_tick_inventory.json`。
未擅自縮短年度切分，也未用日 OHLC 代替分鐘成交。

`cuda_rank0.json`、`cuda_rank1.json` 記錄雙 RTX 5090 的 BF16 反向傳播與 NCCL 檢查。
留倉模式已完成雙卡 BF16 的一個 epoch 工程測試，包含 4,337 日的完整 fold 測試區間、
可讀取的 checkpoint、完成標記與九張必要 walk-forward 圖。
測試輸出位於 `artifacts/smoke/vastai1t_futures_carry_3207765abbe75296_epoch1/`；
正式 1000-epoch 訓練尚未啟動。彙總驗證收據為操作目錄的 `readiness.json`。
`minute_execution_smoke/verification.json` 僅代表實際分鐘 tape 的工程驗證，
使用未訓練的固定權重，不代表策略績效或正式訓練完成。
