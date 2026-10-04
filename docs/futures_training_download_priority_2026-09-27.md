# 期貨訓練缺口優先下載：2026-09-27

## 範圍與現場進度

本輪優先服務既有台灣全期貨當沖訓練設定：2020-03-23 至 2026-09-04，
日資料固定 SHA `70a57dd76de74fd0a3652b1ecf932ccaf32320d01c252bf7b608a30b3f598df5`。
不更換訓練 pin、checkpoint、模型或執行規則，不啟動訓練，也不新增原始資料公開分發。

台灣時間 **19:37:38** 的本機稽核：

| 項目 | 觀察結果 | 不代表什麼 |
|---|---|---|
| 當沖 v3 已驗收契約交易日 | 421,460 / 431,434 | 包含官方無單式成交及不足一口證據，不全是分鐘成交 |
| v3 剩餘來源缺口 | 9,974 個商品日；日期落在 2021-06-21 至 2026-08-17 | 不是缺少 9,974 根分鐘 K |
| 缺口原始檔已下載且 SHA 相符 | 4,600 / 9,974 | 尚未全部組裝、驗收為新分鐘包 |
| 尚未下載／通過來源收據檢查 | 5,374 | 掃描期間下載持續進行，之後會再減少 |
| 尚待來源：指數／ETF／股票期貨 | 3,931 / 962 / 481 | 不是完整市場商品數 |
| 本輪原始檔語意抽查 | 128 / 128 通過 | 抽查不是完整包驗收 |
| 最近 10 分鐘 | 983 次，全部為 `TaiwanFuturesTick` | 短期吞吐不是完成保證 |
| 帳號回報 | Sponsor，6,000 次／小時 | 不是每個 worker 各有 6,000 次 |

以這個窗口的 98.3 次／分鐘估計，剩餘來源下載約 **55 分鐘有效下載時間**。
配額等待、API 重試、完整驗證與分鐘包組裝另計。不是整體訓練就緒 ETA。
SHA 掃描用 6.4 秒；本次稽核沒有呼叫任何外部 API。

## 全速的分配方式

- 保留已運行的 `stockagent-futures-gap-priority-20260927.service`，4 個 worker
  共用既有 FinMind transport、帳號 limiter、計量帳本與增量預留。
- Free／Sponsor／Complement 三個一般 FinMind 背景下載服務暫停讓路；未改其
  enabled 狀態及 timers。即時行情、Discord、當沖執行服務不因本輪被停止。
- 只排缺口的唯一「商品、日期」；一次回應保留所有實體月份，避免按到期月份重複請求。
  已有且通過來源 SHA 的資料不重抓。502／504 等暫時錯誤最多重試兩次，重試也算配額。
- [FinMind 官方文件](https://finmind.github.io/tutor/TaiwanMarket/Derivative/#taiwanfuturestick-backersponsor)
  限一般 Tick 查詢每個商品一天；全市場 Parquet 為 SponsorPro 功能。
  目前帳號是 Sponsor，因此不呼叫未授權的全市場端點。
- 永豐已有的歷史管線與期交所規則下載獨立運行，不為此多登入或新增重疊任務。
  [永豐限制](https://sinotrade.github.io/tutor/limit/)是共用帳號行情查詢
  50 次／10 秒、每人最多 5 個連線；已有市場服務的連線及流量保留。
- 本輪既有包裝器最晚於台灣 22:00 停止新增請求，在途 bounded request 可完成寫入。
  `ExecStopPost` 於完成、失敗或停止後恢復原一般 FinMind 服務，並留下恢復收據。
  來源空值、永久錯誤或驗證失敗須檢查，不能改成已完成。

## 本次修正與驗證

`scripts/download_tw_futures_intraday_gaps.py` 原先在整批結束前不扣除
`pending_product_days`，因此一批已成功下載 1,800 次時仍會顯示原先 7,345 待抓。
現在依成功寫入的商品日收據即時扣減，每 30 秒或 100 次請求保存一次進度。
失敗、空資料與重試不增加完成率；結束後仍重新核對來源 SHA，以最終稽核數為準。
另列帳號上限、預留配額、實測成功速率及僅包含下載的 ETA。
計數使用 O(1) 記憶體統計，不為更新進度重掃資料或呼叫 API。

沒有中斷正在跑的批次；程式修正將由續抓包裝器在下一批啟動時自動載入。
相關測試 **76 passed**；修改的下載器與測試檔 Ruff 通過。

資料時鐘與單位沒有變更：只使用日期、商品與實體月份匹配的真實 Tick 重建分鐘，
保留原有右標時間、價格網格與官方量上界。FinMind 單式雙邊量換成單邊「口」，
不把期貨口數改成股票股數；不以日 OHLC 或插值補造分鐘成交。
`training_ready` 保持 false；必須完成全量語意驗證和 canonical builder 組裝後另行驗收。

## 其他訓練來源

官方日資料本機 archive 已到 2026-09-24，最早為 1998-07-21；來源有 2,005,925 筆，
v4 衍生表有 2,940,646 筆，其中 1,119,952 筆為持倉估值沿用列，不能當作成交。
官方最後結算價有 41,084 筆，日期為 2014-01-02 至 2026-09-23；有這份表不等於
每個歷史到期合約都有結算價，既有逐合約驗收仍須保留。
歷史保證金／限額公告仍由既有期交所規則服務續抓；公告抓到不等於數值、生效日及
每個合約的歷史規則已完整。因此目前不能宣稱保證金模式已完整就緒。

## 證據與重跑稽核

- `artifacts/data_quality/futures_training_download_2026-09-27/acceptance.json`
  保存全缺口 SHA 掃描、128 個實體合約抽查、配額和實測流量。
- `artifacts/markets/tw_futures_v8_intraday_preparation/priority_20260927/`
  保存原服務狀態、暫停及恢復收據。
- `data_finmind/futures_intraday/runs/*/progress.json` 保存逐批原始下載進度；
  `source_bounded_resume_20260927/latest.json` 指向當前包裝器狀態。

下列命令只讀來源、重建稽核報告，不下載、不發布、不訓練：

```bash
source scripts/runtime_env.sh
PYTHONPATH=. OMP_NUM_THREADS=2 POLARS_MAX_THREADS=2 run_fintech_python \
  artifacts/data_quality/futures_training_download_2026-09-27/audit.py
```
