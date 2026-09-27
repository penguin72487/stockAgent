# 台股交易日曆與資料作業契約

`stockagent.live.market_status.tw_stock_day_decision` 是台股現貨作業的共同判定入口。它不把「星期一至五」、「有下載收據」或「某供應商沒有回資料」單獨當作開／休市證據。

| 狀態 | 證據 | 台股現貨作業 |
| --- | --- | --- |
| `actual_open` | 位於收據雜湊驗證的 TWSE TAIEX 歷史逐日記錄 | 可作已發生交易日的歷史回補與完整度分母；仍須個別來源成功收據 |
| `scheduled_open` | 截至觀測時刻可用的最新 TWSE 官方年度開休市表；一般平日或明示開市 | 準備開盤／排定盤後取得；實際收盤資料未發布前不可宣稱完成 |
| `closed` | 已驗證歷史區間無交易、官方明示休市或普通週末 | 當日台股收盤價／分鐘線等 session-only 任務不抓、不結算；跨日公開資訊、歷史缺口、加密貨幣仍可運作 |
| `unknown` | 來源缺失、收據不符、舊／新事件衝突、最新快照截斷等 | 不發即時股票交易指令、不把當天排除於歷史分母；修復／重試日曆，可繼續非 session-only 抓取 |

歷史實際交易日優先於星期：官方檔包含過去週六開市。收據的 `effective_end_date` 只表示已檢查到的日期；只有其範圍內**實際有一筆**的日期才是 `actual_open`。今天即使收據涵蓋，也不會僅因尚無資料就宣布休市；當日改看發布時已知的官方排程。官方假日 Parquet 是逐次追加的快照，只採該年份最新 `_as_of_date`，較舊的更正前版本不混入。

已接入：台股市場狀態／資料新鮮度、永豐股票歷史查詢保護時段與分鐘線最新目標、台股公開資料盤後結算跳過條件、全資料面板的股票串流時窗。休市日只釋放**台股**盤中歷史查詢保留額度，不取消 TAIFEX 夜盤的獨立連線優先權。TAIFEX 的日盤／夜盤與交易歸屬日屬另一個日曆；目前既有 TAIFEX 串流時窗仍是正常週曆估計，不得由本台股日曆推定期貨已開市。臨時停市／颱風日須由更新後的官方證據或實際已驗證歷史資料修正，未知期間維持 `unknown`。

唯讀檢查範例：

```bash
source scripts/runtime_env.sh
run_fintech_python -c 'from datetime import datetime; from pathlib import Path; from zoneinfo import ZoneInfo; from stockagent.live.market_status import tw_stock_day_decision; print(tw_stock_day_decision(datetime.now(ZoneInfo("Asia/Taipei")).date(), parquet_root=Path("data_tw_public")))'
```
