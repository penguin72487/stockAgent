# Shioaji 週選、月選、實際月份期貨與指數歷史資料

此管線保存永豐目前 Contract V2 可解析的四個歷史集合：

1. 最近到期的台指週選，保留該到期日全部 Call／Put 與履約價。
2. 最近到期的 `TXO` 月選，保留該到期日全部 Call／Put 與履約價。
3. 所有目前列示的實際到期月份期貨；每個商品根另存 `tenor_rank`，因此可直接取 R3 以後期限結構。
4. Contract V2 的全部臺灣指數，包括加權、櫃買及產業指數。

另可選擇 `--option-scope all_available`，保存來源當下可列出的所有選擇權商品根與到期系列，而非只取最近台指月選／週選。每個履約價與買賣權仍是獨立契約；目錄保留來源的標的種類、標的代碼、報價幣別與規格種類，不能把報價幣別當成結算幣別，也不能把當前目錄視為歷史上市或可成交證據。此範圍必須使用獨立目錄，例如 `data_tw_shioaji_history/scopes/all_options_v1`；清冊記錄範圍指紋，跨範圍讀寫會拒絕，舊清冊則只屬於原本的 `latest_txo`。全選擇權的請求起點可回到 2011，並沿用來源提供的合約起日；這是請求範圍，不表示來源已證實提供該年代的選擇權資料。收集到的歷史仍受來源可查契約、流量、實際行情及原始收據限制。

入口為 `downloader/download_shioaji_historical_market_data.py`。預設優先序是週選、月選、實際月份期貨、指數；各集合內由最近日期往回。K 棒以不超過 29 個曆日切片呼叫 `api.kbars()`，Tick 再依已驗證 K 棒實際出現的交易日呼叫 `api.ticks()`。這個相依關係避免查詢休市日，同時不把未查證的日期標成完整。

## 完整性邊界

- `inventory/contracts.parquet` 是歷次 Contract V2 觀測聯集；刷新目錄不會刪掉已觀測的到期代號。
- 每個 K 棒切片與 Tick 交易日都必須有 `complete` 或 `source_empty` receipt。
- `complete` receipt 會驗證 Parquet SHA-256；檔案遺失或雜湊錯誤會重新排入佇列。
- 期貨／選擇權的週五夜盤會映射到下一個 TAIFEX 交易日；尚未完整結束的下一交易日不會被保存為完整歷史。
- 歷史 Tick 只保存成交附帶的最佳一檔 Bid/Ask，不宣稱能回補歷史五檔。
- Contract V2 在第一次觀測前已下架的舊代號不能由目前目錄重建，必須維持為明示的來源邊界。

## 操作

安裝並立即執行：

```bash
sudo bash scripts/install_shioaji_historical_market_data_service.sh --run-now
```

查詢服務、進度與摘要：

```bash
systemctl status stockagent-shioaji-historical-market-data.service
journalctl -u stockagent-shioaji-historical-market-data.service -f
source scripts/runtime_env.sh
run_fintech_python -m json.tool data_tw_shioaji_history/progress.json
run_fintech_python -m json.tool data_tw_shioaji_history/summary.json
```

服務沿用主機共用 `shioaji_quote_query` 節流器、歷史流量帳本、07:45–14:31 即時行情優先窗及 90% 安全上限。流量用盡、即時行情保護窗或五連線容量不足時會保留 receipt 並自動重試。

需要全商品選擇權來源補查時，可在相同 runner 指定
`SHIOAJI_HISTORICAL_MARKET_OPTION_SCOPE=all_available`、
`SHIOAJI_HISTORICAL_MARKET_COLLECTIONS=all_options` 與獨立的
`SHIOAJI_HISTORICAL_MARKET_OUTPUT_ROOT` 和 `SHIOAJI_HISTORICAL_MARKET_RUN_ROOT`。
此工作共用歷史登入鎖、原始流量與空間檢查；局部範圍不會自動發布為整個 `tw-shioaji-history` 資料集。

公開唯讀面板：`/shioaji/`；狀態 API：`/shioaji/api/status`。資料同步目錄已登錄為 `tw-shioaji-history`，下載器仍在寫入時禁止發布不完整 cold release。
