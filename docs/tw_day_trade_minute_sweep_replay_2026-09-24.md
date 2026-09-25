# 台股當沖 09:01 起逐分鐘掃量回補修正紀錄

## 1. 執行進度與結論

- 已完成 2026-02-25 至 2026-09-24、146 個交易日、5 個 paper mode 的隔離重算與驗收。
- 09:00 官方開盤價只負責推論與部位 sizing；回補成交自 09:01 開始，未成交量依序帶入 09:02、09:03，持續到目標完成或 13:20 出場階段開始。
- 每個股票、每一分鐘最多使用該分鐘真實觀察成交量的 50%，並以整張為基本容量；同一分鐘的減倉與加倉共用同一容量，不能重複使用。
- 驗證候選通過資料來源、帳務、持倉、分鐘曲線與成交契約檢查；狀態為 `validated_not_promotable`，原因是線上 4 個模式仍有持倉。未覆蓋線上帳本，也未重啟服務。

## 2. 成交契約

1. 使用 09:00 已完成的官方開盤資料產生固定目標部位。
2. 09:01 使用來源可驗證的 right-labelled 一分鐘價與成交量；價格採該分鐘 VWAP，缺少 VWAP 時才採同一根有效 K 棒的 Close。
3. 單分鐘可成交股數為 `floor(0.5 * observed_volume / 1000) * 1000`，並受資金、合法價格與 eligibility 約束。
4. 09:01 未完成的目標成為 pending order；09:02 起每一個已完成分鐘繼續消化剩餘量。
5. 每分鐘先做減倉，再做加倉。方向翻轉時必須先平掉舊方向，剩餘容量才可建立新方向。
6. `Volume <= 0`、缺 K 棒或缺有效價時不得成交，也不得使用前價、插值、Bid/Ask 或 `+1 tick` 製造成交。
7. 13:20 起出場責任優先；若在此之前市場真實流動性仍不足，保留並揭露剩餘持倉，不能宣稱已完全成交。

因此，「直到完全執行」的精確含義是：只要仍在允許的進場時段且來源持續提供正成交量，就逐分鐘重試到完成；它不是不顧真實市場容量的保證成交。

## 3. 根因與修正

- 原本開盤回補只會在 09:01 嘗試一次，缺少可持久化的跨分鐘剩餘委託。現在 pending reduction/addition 會跨分鐘保存，並由統一容量帳本處理。
- 反向換倉原本可能讓平倉與新倉競爭錯誤容量。現在以 reduction-first 順序處理，且同一股票／分鐘只有一個 50% 容量池。
- 分鐘曲線批次載入路徑曾漏讀 `Volume`，使補齊的零量列可能被誤認為新鮮價格。批次與逐檔路徑現在都讀取並過濾 `Volume > 0`。
- 09:01 初始成交標記為 `entry`，之後的續單標記為 `entry_completion`；持倉與稽核器均把兩者視為同一開盤目標的庫存來源。
- Shioaji KBar 成交量已統一正規化為股數，避免張數／股數單位造成 1000 倍容量誤差。
- 重放來源會記錄路徑與 SHA-256；新版 receipt 另記錄每一來源實際負責的股票集合，防止重疊檔案在日後改變來源優先序。這次既有 v12 候選仍是 legacy ordered-source receipt，但其 8,525 個來源檔均已雜湊驗證；新產生的 replay 才會啟用嚴格 per-symbol pin。
- retained 09:01 Shioaji 開盤證據只有在來源身分、雜湊、09:00:00–09:00:59 時窗、正成交量與價格單位全部通過時才被稽核器接受。

## 4. 驗收證據

- 候選：`artifacts/replays/missed-opening-minute-sweep-20260924-v12-pinned`
- 驗收 receipt：`artifacts/replays/missed-opening-minute-sweep-20260924-v12-validation.json`
- 09:01 成交紀錄：46,804 筆。
- 09:01 之後逐分鐘續單成交：57,238 筆。
- synthetic fallback fills：0。
- 分鐘曲線：197,100 列；原帳本權益差異點 0；最大浮點差 `2.9802322387695312e-08` TWD；未使用線性插值。
- margin/carry audit：146 個交易日、3,002 個來源檔，錯誤 0，最大帳務誤差 `3.725290298461914e-09` TWD。
- `fills.jsonl` SHA-256：`81ff2738243252aa4d77fc6290be7def6619b1fde68a7dda2dd9c145cdbc43de`
- `orders.jsonl` SHA-256：`bd06c1b0b4744825829afd29689817e3ba65e8dd76fcdc86f1fd2ed096a4ccb5`
- `marks.jsonl` SHA-256：`21877da28be4876875b7ff14a533ec6196d02607d1958dd7821f60ef41837b6f`
- 相關單元、整合與完整 simulation 測試：370 passed（219 + 151，兩組測試檔不重複）。
- Ruff、`py_compile` 與 `git diff --check`：全部通過。

## 5. 線上切換邊界

安全驗收正確阻擋切換，因為下列 live paper mode 仍有持倉：

- `tw_day_trade_100m`
- `tw_day_trade_attention_layernorm`
- `tw_day_trade_multi_basis`
- `tw_day_trade_multi_basis_projection_l1_gelu`

`tw_day_trade_multi_basis_22` 沒有線上持倉，但不能單獨把共享帳本的一部分切換。待所有 live 持倉自然結清後，應重新執行 validate-only，確認來源雜湊未變且 `cutover.ready=true`，才可原子切換整組帳本。

本候選是以真實分鐘價量約束建立的反事實 paper replay，不是交易所撮合回報，也不宣稱具有真實委託排隊順位。
