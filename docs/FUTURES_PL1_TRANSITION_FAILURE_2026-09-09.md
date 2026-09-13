# V9 完整測試：合晶 PLF → PL1 契約轉換缺口

目前尚未解除完整訓練阻塞。已修正補抓器漏列調整契約、到期資料解析器漏掉完整契約價值，以及錯誤的股票索引映射；**PL1 分鐘資料與留倉契約轉換仍待接入驗證**。不可只關閉 `unresolved_corporate_transition` 檢查後重跑。

## 實際失敗

`artifacts/markets/tw_stock_futures_0845_carry_v9_vast5090` 的第一組有 166 個 epoch、166 次有效訓練更新，訓練資料錯誤與整個 epoch 零梯度次數均為零。最後在第一 fold 的完整測試中，於 2024-09-24 持有合晶（6182）`PLF:202410` 空單 1 口而中止。最後有效權益為 9,484,238 元；完整測試報酬未定義。

這次不是 NCCL 起因，也沒有證據顯示梯度引發此次例外。較長訓練會選出不同 checkpoint、產生不同留倉；先前 32-epoch 的有限驗證沒有涵蓋此部位路徑。逐 epoch 的抽樣測試亦不等於最後的全歷史測試。

## 官方規則與實際來源

[期交所 2024-09-12 公告](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/6182_20240924.pdf)規定：2024-09-24 起，原合晶期貨 PLF 的指定月份轉為 PL1，乘數仍為 2,000，保留優先參與現金增資的相當價值；同日另掛標準 PLF。舊空單應轉入 PL1，而非變成同日的新 PLF，也不是一筆平倉再開倉成交。

2024 年官方原始日行情確認：`PL1:202410` 從 9 月 24 日到 10 月 16 日有 14 個交易日。9 月 24 日成交量為 17 口、結算價為 31.45；現有訓練分鐘快照沒有此契約的任何列。日資料不能提供 08:46 或 13:20–13:30 的可成交證據。

另外，[期交所 2024 年 10 月最後結算表](https://www.taifex.com.tw/cht/5/sSFFSP?down_type=1&queryYear=2024&queryMonth=10)在本機已驗證的原始 HTML 記錄：

| 契約 | 最後結算日 | 最後結算價 | 約定標的物價值 |
| --- | --- | ---: | ---: |
| PLF:202410 | 2024-10-16 | 29.82 | 59,640 元 |
| PL1:202410 | 2024-10-16 | 29.82 | 59,923 元 |

兩者相差 283 元，因此 `最後結算價 × 2,000` 不能完整表達 PL1 的到期價值。此次網頁工具未能重新開啟歷史查詢 URL；上表取自既有 receipt/SHA 驗證的原始 HTML，並非聲稱即時網頁重新下載成功。

## 已實作

- `downloader/repair_shioaji_futures_minute_gaps.py` 依明確缺口及官方實體契約清單排程，不再先經模型近期標準契約篩選；未知身分明確報錯，不會在 inner join 中消失。維持分段查詢、預設只抓 KBars、既有 receipt 與流量限制。無待辦任務時不登入；缺帳號設定時先保存查詢計畫並清楚說明。
- `scripts/download_taifex_futures_final_settlement_history.py` 新增 `final_settlement_value`，保留官方約定標的物價值；合併資料時亦檢查此值衝突。
- `stockagent/data/tw_stock_futures_carry.py` 讀入該值並優先放入既有到期 notional 通道。舊來源沒有此欄時沿用原公式，現有 v9 的資料 pin 及訓練數值未因此切換。
- `stockagent/training/trainer.py` 的完整測試附上階段標籤；錯誤中的股票索引透過子集合映射回完整 panel，保留原始索引並輸出標的代號。

## 已備妥資料修復工作

證據在 `artifacts/diagnostics/futures_pl1_transition_20260909/`：官方公告 PDF 與 SHA、完整官方契約來源 SHA、原始日行情證據、明確查詢日清單、到期完整價值，以及可重現的 `prepare_case.py`。

公告列出五個月份。現有最後結算表可確認其中 202410、202411、202412 三個月份，合計 110 個契約日：42 日有正的單式成交量、68 日有官方零單式成交量證據。202503、202506 的官方最後結算日仍缺，已列在 `source_audit.json`，不可把此清單誤稱為全部五個月份完整覆蓋。

對已確認的正成交量缺口，現有修復器已產出 5 個有日期範圍的 **KBar** 任務，保存在 `artifacts/data_repair/futures_pl1_kbars_20260909/repair_plan.json`。其中實際失敗的月份為 `PL1J4`，查詢 2024-09-24 至 2024-10-16。本機未發現既有 Shioaji 環境變數、`.env` 或 `.env.futures`；尚未嘗試登入或下載。

取得既有環境檔路徑後，使用同一修復器：

```bash
cd /root/stockAgent
set -a
source /實際路徑/shioaji.env
set +a
source scripts/runtime_env.sh
run_fintech_python -m downloader.repair_shioaji_futures_minute_gaps \
  --daily-data-path artifacts/diagnostics/futures_pl1_transition_20260909/official_inventory.parquet \
  --gaps-path artifacts/diagnostics/futures_pl1_transition_20260909/positive_volume_gaps.parquet \
  --output-root artifacts/data_repair/futures_pl1_kbars_20260909
```

這是**資料補抓指令，不是已可成功訓練的承諾**。空回覆仍為缺資料。補抓後尚需以公告建立 PLF→PL1 的持倉遷移：保留數量與成本基準、轉換本身不計成交費稅、新 PLF 與 PL1 分開使用分鐘容量，並沿用 PL1 的官方估值及完整到期價值。接入後必須先重播此次實際失敗 checkpoint 的完整測試，再建立相容的新訓練資料／輸出版本。不得刪除該股票、截短測試、偽造平倉或將錯誤當作零報酬。

## 驗證

73 項期貨資料／帳本／錯誤處理回歸測試及 4 項期貨 checkpoint 測試通過，包含 CUDA eager/compiled 帳本對照及雙 rank 資料錯誤傳播。嚴格 CUDA 環境檢查通過，偵測到兩張 RTX 5090。這些結果不代表 PLF→PL1 轉換已完成，更不是完整 v9 訓練或投資績效驗收。
