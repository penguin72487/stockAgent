# 期貨分鐘資料：只隔離問題合約日

依使用者 2026-09-09 最新指示，historical 實驗只隔離 **2021-06-21 的
`LVF:202107`（中租-KY 期貨 2021 年 7 月契約）**。整日隔離清單為空。
同日其他 209 個候選合約保留，其中 198 個有核實分鐘資料、11 個有官方
無普通成交證據。11 個無成交合約依實際零容量處理，並不填造成交。

2020-03-23～2026-09-04 的全部 **1,573 個決策日**保留，模型仍讀取
2,753 檔股票的 98 項前一完整交易日特徵。只有該實體合約當天的執行槽歸零；
健康替代合約保留原始槽位，其他日期的 LVF 不受此設定影響。

這是使用者指定的事後資料品質排除，不是當年盤前已知的市場交易限制。
缺少的兩口普通成交仍留在 `coverage.parquet` 與 `gaps.parquet`，未改成零成交，
也未以日線或插值補出分鐘價格。`covered_dates` 如實保留一日來源未全齊，
`usable_dates` 則記錄扣除精確合約日後可使用的全部 1,573 天。

交易契約仍為 08:45 決策、08:46 進場、13:20 限價、13:24 撤換市價、
13:30 平倉期限、每分鐘 50% 成交參與率、整口交易與既有費稅。
策略無法依成交容量平倉時，原有失敗判定仍生效，不因此自動新增隔離。

## 現行設定與來源

```yaml
tw_stock_futures_day_trade_quarantine_dates: []
tw_stock_futures_day_trade_quarantine_contract_days:
  - date: '2021-06-21'
    physical_contract: LVF:202107
```

- 設定：`configs/markets/tw_stock_futures_day_trade_0845_historical.yaml`
- 新資料：`artifacts/data_preparation/futures_minutes_contract_quarantine_v1_20260909/`
- manifest SHA256：`9980d5c07b4e1bc6263c032da4411911c76380136c805fa116af380ede94e6c9`
- 新訓練目錄：`artifacts/markets/tw_stock_futures_day_trade_0845_from_20200323_v2_contract_quarantine_v1_vast5090/`
- 稽核與遠端驗收：`artifacts/operations/futures_contract_quarantine_20260909/`

保留舊整日隔離資料版本及其驗收紀錄。新範圍有獨立 manifest、隔離契約版本 1、
checkpoint 與報表語意，不沿用舊整日隔離 checkpoint。分鐘 tensor 仍沿用既有
歷史 v2 格式與共用訓練器。

全候選範圍 381,288 個合約日、311 個商品、263 檔標的：361,686 個分鐘已核實，
19,599 個官方無普通成交、2 個官方成交上界不足整口容量、1 個明確隔離。
完整來源稽核已核對所有輸出 SHA、列數、候選分母、日曆、普通成交證據及成交量。
新舊版本的所有原始觀測 Parquet 逐檔 SHA 完全相同，沒有刪改行情資料。

## 在 vastai1T 開始正式訓練

```bash
cd ~/stockAgent
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_historical.yaml
```

正式設定仍為 1,000 epochs、BF16、雙 GPU DDP、原模型和 batch。
資料檢查也可直接執行同一指令加上 `--check-data-only`。

## 已完成驗收

- 本機資料、執行、歷史相容性測試 114 項通過；checkpoint 與續訓測試 114 項通過。
- 全量 `[1573, 263, 2, 67]` 執行資料比對只有一個合約槽改變：
  2021-06-21、股票 5871、slot 0。其他所有槽位逐值相同。
- vastai1T 已核對交付檔案 SHA，保留遠端其他程式修正；CUDA strict check 無警告或失敗。
- 實際 `train.py --check-data-only` 讀入全部 1,573 天；沒有整日隔離。
- 雙 RTX 5090 完成第一 fold 的 3 epochs：2020 訓練、2021 驗證 244 天、
  2022～2026 測試 1,133 天，包含完整驗證、測試、checkpoint 與 9 張根目錄報表。
- 完整流程 91.09 秒；該首 fold 的穩態 epoch 中位數 0.787 秒，
  GPU 整卡採樣峰值分別 12,544／12,542 MiB。這是首 fold 的實測，不能外推所有年份。
- `remote_evidence/training_readiness.json` 的全部驗收項目通過，正式訓練尚未啟動。

這次短訓練仍在測試期 2025-04-10 遇到一口無法依分鐘容量平倉，原本的失敗
判定如實保留。工程驗收表示資料接入與訓練流程可用，不表示該 3-epoch 模型
已適合交易；沒有排除這筆策略失敗或放寬平倉規則。
