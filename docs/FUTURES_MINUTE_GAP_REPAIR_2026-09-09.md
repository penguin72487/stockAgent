# 個股期貨分鐘來源修復與開訓驗收

> 下文是先前整日隔離版本的驗收紀錄，供重現保留。使用者後續已改為僅隔離
> `2021-06-21 / LVF:202107`，同日其他合約與決策日期全部保留。
> 現行版本見 [合約日隔離修正](FUTURES_CONTRACT_QUARANTINE_2026-09-09.md)。

本次保留 2020-03-23～2026-09-04、311 個商品的完整候選範圍，以及
08:45 決策、08:46 進場、13:20 限價、13:24 撤換、13:30 平倉的既有契約。
不延長日線替代期限，不刪除缺資料的商品，不改模型或正式 1000 epochs。

使用者已於 2026-09-09 同意隔離 **2021-06-21 整個決策日**，其餘規則不變。
股票特徵日曆保留連續；該日所有商品的訓練、驗證、測試決策標籤都排除，
不是把未知結果記為零報酬。設定、來源 manifest、checkpoint 相容性契約及
報告均記錄此例外；未明確設定相同隔離日期的 loader 仍拒絕這份資料。

## 最終來源盤點

| 範圍／狀態 | 數量 |
|---|---:|
| 2020-03-23～2026-09-04 原始交易日 | 1,573 |
| 隔離後可用決策日（尚未扣除 lookback） | 1,572 |
| 商品／對應股票 | 311／263 |
| 分鐘來源與實體月份已核實 | 361,686 合約日 |
| 官方證據確認沒有普通單式成交 | 19,599 合約日 |
| 官方成交量上界證明整口容量為零 | 2 合約日 |
| 保留缺來源紀錄、整天已隔離 | 1 合約日 |
| 實際保存分鐘觀測／策略事件分鐘 | 25,893,806／1,097,352 列 |

全部 381,288 個候選合約日都保留於 `coverage.parquet`，1 筆未取得來源仍在
`gaps.parquet`。`preparation_audit.json` 的全商品、全日曆、來源／輸出 SHA、
事件分鐘、實際觀測量、隔離範圍及訓練 preflight 共 17 個檢查均通過。
這是完整候選範圍的可執行資料契約，不宣稱永豐提供了交易所每一筆成交。

來源 manifest SHA-256：
`dd7dd156de880cb5ded02a866024a6ff13be713596449c30b7db6e5dea4a397c`。

## 28,424 個合約日的原因

重新逐列讀取 SHA 核實的期交所年度 ZIP 與當年月 CSV，而不是從整理後的
持倉估值列推測原始成交。結果如下：

| 官方原始證據 | 合約日 |
|---|---:|
| 明確總成交量為零 | 15,686 |
| 總成交量全部由價差對價差成交腿構成 | 3,678 |
| 完整當日行情表沒有該實體月份列 | 234 |
| 尚有單式成交量，必須取得分鐘觀測 | 8,826 |

期交所總成交量與單式委託簿容量不能互換。依
[期交所行情表說明](https://www.taifex.com.tw/cht/3/futDailyMarketReport)，
價差成交一口包含兩個月份各一口；只有明確已報導的價差對價差量才用於核對。
缺報的價差量不當作已知成交。零容量還須通過實體月份 K 棒查詢無單式成交
的交叉檢查；永豐空回覆本身不構成無成交證據。

## 修復路徑與證據

- 已到期商品不在當前商品目錄，不等於歷史查詢一定沒有資料。本次用 SDK
  公開 `BaseContract` 介面實測到期月份資料可取得；這是目前端點的觀察，
  不是保證所有到期代碼皆受支援。官方日線中的商品／年月決定查詢代碼，
  不使用查詢當下的 R1 `target_code` 當成歷史身分。
- `downloader/repair_shioaji_futures_minute_gaps.py` 沿用既有歷史下載器的
  限流、90% 流量預算、交易時段保護、原子檔案、SHA 與查詢憑證。
  5,551 個月份區間可中斷續傳，單次最多 29 日。
- 部分日期的 K 棒 Amount 有整數截斷，部分日期 K 棒 API 回空。
  以 `--method ticks` 取得相同實體月份的逐筆資料，沿用同一聚合器重建
  真正的成交額／成交量 VWAP；不以 OHLC 平均或截斷金額代替。
- 既有 Tick 核實資料仍由同一個分鐘 builder 使用。缺口改查實體月份
  K 棒，保留永豐實際提供的分鐘價格與成交量，不增加至官方日總量。
- 身分已由實體月份確定，日高低價用來檢查觀測價格的範圍，不再要求
  稀疏觀測一定包含全日最高／最低那筆成交。缺少的價格與成交量不補造。
- 超出官方日高低價的非執行分鐘隔離並記錄於
  `non_execution_quarantine`。如果異常落在任何進出場分鐘，仍阻擋訓練；
  不裁切價格、不替換成日線、不把異常執行分鐘視為零成交。
- `official_evidence.parquet` 及其來源 manifest 綁定官方原始檔 SHA。
  新增無單式成交狀態只有在相應證據通過時才被訓練 loader 接受。
  候選遮罩仍由前日資料決定，當日無成交僅影響執行容量。

原始修復工作區 `data_tw_futures/shioaji_gap_repair` 已列入發布排除目錄。
下載完成、來源驗證、遠端交付及真實訓練驗收分別記錄，不能互相替代。

## 最後四筆空回覆的處理

- DPF:202110／2021-10-14：官方 205 口含 5 口價差成交腿；另由
  [期交所鉅額交易下載](https://www.taifex.com.tw/cht/3/dlProductOrderView)
  取得 SHA 核實的 200 口鉅額成交，普通委託簿容量因此為零。
- PZF:202308／2023-07-13、LIF:202401／2024-01-10：全日普通成交上界
  各 1 口。在原有 50% 參與率下，每分鐘整口容量
  `floor(minute_volume × 0.5)` 必然為零。這個證據沒有指定成交時間或價格；
  loader 會以實際設定重新驗證，改為 100% 參與率時就拒絕使用此證據。
- LVF:202107／2021-06-21：全日普通成交上界為 2 口，不能推定零容量。
  實體月份及 R1/R2 的 K 棒、Tick 查詢皆未取得當日觀測，因此保留缺口，
  依使用者批准隔離整天。

另有 2 個非執行分鐘的價格超出官方當日高低範圍，個別隔離紀錄保存在
`quarantined_non_execution_minutes.csv`；沒有改動價格或填造成交量。

## 重現整理

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_tw_stock_futures_0900_entries.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_historical.yaml \
  --shioaji-ticks-root data_tw_futures/shioaji_history \
  --repair-root data_tw_futures/shioaji_gap_repair \
  --official-evidence-dir artifacts/operations/futures_minute_gap_repair_20260909/official \
  --output-dir artifacts/data_preparation/futures_minutes_repaired_20260909
```

操作證據目錄：`artifacts/operations/futures_minute_gap_repair_20260909/`。
## vastai1T 開訓驗收

已完成逐檔 SHA 交付、完整資料載入及快取。原本固定的 `tw-public` 版本
`tw-public-20260906T151104843336596Z-l0-penguin-246aab6e5c72e427`
已由 canonical edge `use` 完成接收、物件校驗、materialization 校驗及七天續租。
期貨修復資料為明確交付的私有、已稽核 snapshot，沒有冒稱已完成全網冷發布。

兩台 RTX 5090 的 3-epoch 完整流程在 164.26 秒完成；使用全部 2,753 檔股票、
98 個特徵、BF16、global batch 128，沿用原本共同訓練器與整口執行器。
第一 fold 的 epoch 2～3 總時間中位數 0.954 秒，GPU 取樣峰值分別
18,290／12,546 MiB。這只代表訓練 2020、驗證 2021 的首 fold；不能直接
推算較長訓練年份或全套 1000 epochs 的總時間。

82 項資料／交易時序測試在本機與遠端皆通過，另有 108 項 checkpoint／續訓
回歸測試通過。來源 17 項驗證、最終 readiness 16 項檢查、canonical completed
artifact conformance 及 9 張 root walk-forward 圖均通過。
`training_readiness.json` 記錄 `ready_to_start_training: true`；正式輸出目錄
尚未建立，沒有代為啟動正式 1000 epochs。

短跑不代表策略通過交易驗收：其測試回測在 **2025-04-10** 有 **1 口**
無法於期限內平倉，依原有規則進入吸收式執行失敗。沒有隔離此交易日、
放寬 50% 參與率或使用日收盤價補平倉。此結果保存在短跑回測與 readiness
憑證，正式研究仍須評估策略是否能學到可執行的持倉選擇。

在 vastai1T 的 `/root/stockAgent` 使用正常入口：

```bash
source scripts/runtime_env.sh
STOCKAGENT_DDP_CPU_AFFINITY='42-55,154-167;28-41,140-153' \
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_historical.yaml
```

原始重建證據保存在本機操作目錄；遠端 `benchmarks/acceptance` 包含已完成的
短跑 checkpoint、逐 epoch 計時、真實回測與報表，與正式輸出目錄分離。
