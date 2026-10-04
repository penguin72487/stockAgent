# 跨服務延遲量測與改善（2026-09-23）

## 結論與界線

本輪實作的是「刪除同一次觀測中的重複工作」及「重疊互不相依的本機 I/O」，
不是降低資料更新頻率、刪除分鐘點、改交易策略、增加券商登入或移除安全檢查。
也沒有增加全域 rate limit 或 HTTP 併發限制。

完成一次跨公開網站的量測與兩個共同監控路徑的優化；**不是所有服務都已到物理極限，
也不是整個系統資料健康已正常**。已觀察到的下一個重成本是完整歷史逐點投影。

## 可比較的實測

| 範圍 | 對照 | 修改後 | 證據界線 |
|---|---:|---:|---|
| 完整 footer 欄位彙總 CPU | 478.313 ms | 158.120 ms | 4 次交錯順序、同一份凍結輸入；約減少 67% |
| 永豐監控本機 I/O | 659.032 ms | 439.457 ms | 4 次序列／並行對照；每次清除 CLI 自身快取；約減少 33% |
| 排程資料監控整輪 | 5,533 ms | 3,392 ms | 修改前 38 筆、修改後 15 筆 journal 中位數；非固定工作量 A/B |

第一輪 5 次重測得到欄位彙總 662.869 → 178.840 ms、永豐監控
1,138.310 → 631.805 ms。保留兩輪結果，不挑最快一次代替常態。

- 完整欄位實驗涵蓋 55,764 個檔案、6,314 個欄位。舊／新版本的所有欄位、
  null 計數、型別與狀態排序後完整 JSON SHA-256 相同。
- 欄位 CPU replay **不包含**目錄掃描、JSON decode、stat；這三項另列在 receipt。
  不把純 CPU 微測速冒充完整來源重建。
- 永豐實驗使用真實 systemctl、journal 與本機檔案；來源在盤中持續改變，
  receipt 如實記錄 `outputs_stable=false`，不能宣稱所有即時輸出逐位元相同。
  查詢範圍、失敗行為與並行機制由固定輸入的回歸測試約束。
- 排程統計包含實際變動 footer 工作量，也包含驗證工作造成的主機負載。
  單筆仍可能超過 5 秒；不是低尾延遲 SLA 或永久吞吐保證。

## 實作

### 1. 一輪監控，一份清冊觀測

`InventorySnapshot` 僅在本次建置內共用已解碼的約 21 MB footer 清冊與檔案選取結果。
public status 與 feature inventory 不再重新讀取同一份 JSON、掃描同一組目錄。
它不是跨輪 TTL 快取，也不代替檔案新鮮度驗證：彙總欄位前仍逐檔核對 size/mtime，
變更或刪除的來源繼續顯示 partial；不同 repo 的 snapshot 會被拒絕。

按 schema 批次累積每個欄位，避免對每一檔的每一欄反覆初始化集合、字典及行數。
使用 Python 整數加總，保留缺少 null 統計時的未知狀態，不引入浮點或固定寬度溢位。
路徑排序只取一次 parts；維持原本的 Path 字典序。

### 2. 永豐狀態的本機 I/O 並行

保留原本 systemctl 批次查詢與每個 journal 的筆數／時間邊界，將這些獨立程序與
manifest 讀取重疊。最多是既有五個本機查詢工作，不是增加 HTTP 或永豐 API 流量。
自訂 command runner 仍依序執行，避免破壞既有非 thread-safe 呼叫者。

依 Shioaji 技能的行程／連線界線，本次沒有建立 broker client、登入、送單或重啟行情行程。

### 3. 留下能持續重測的紀錄

既有 `benchmark_dashboard_latency.py` 新增：

- `--profile-services`：量測前後擷取所有已載入 StockAgent service（含 inactive/failed）
  的 allowlisted CPU、記憶體、I/O、PID、InvocationID、重啟數與狀態。
  未啟用的 I/O accounting 保留 null；跨重啟或重置不能假造低 CPU。
- `--profile-inventory`：同一份 footer／檔案 metadata 的 CPU replay，另記觀測成本。
- `--profile-inventory-reference PATH`：用明確指定的改前模組交錯 A/B，輸出不一致會回傳失敗。
- `--profile-shioaji-monitor`：同一組本機工作序列／並行比較，無券商 API 呼叫。

每次排程另輸出結構化 `data_monitor_timing` journal：總耗時、Python 行程 CPU、
systemd 查詢、inventory、public projection、feature projection、實際刷新檔案數、
欄位數與是否重用。四個階段互斥；CPU 不包含子程序，服務整體 CPU 另看 systemd counters。
持久性與輪替沿用 journald，不新增無上限 JSONL 檔案。
原有 gateway 每分鐘路由／階段 histogram 與瀏覽器測速繼續運作。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_dashboard_latency.py \
  --repeats 10 --concurrency 2 --profile-services \
  --profile-inventory --profile-shioaji-monitor \
  --output artifacts/benchmarks/services-latency-next.json

journalctl -u stockagent-data-refresh-status-snapshot.service \
  --since today --no-pager -o cat --grep='"event":"data_monitor_timing"'
```

不需要為了測速停止服務、清除 production cache 或重新登入券商。這是本機 HTTP 測量，
不是瀏覽器繪製時間；完整歷史的大 body、解壓、JSON parse 分開記錄。

## 驗證與部署

- 192 項 Python 測試通過：data monitor inventory/dashboard、Shioaji status、benchmark、
  公開 gateway；覆蓋變更、刪檔、不同 root、不同 schema、缺 null 統計、並行 barrier、
  注入 runner 序列契約、resource counter 重啟與不明值。
- Ruff 與 `git diff --check` 通過。本輪沒有宣稱整個 repository 全測試通過。
- 32 個公開路徑、288 次本機 HTTP 請求無錯誤；另有隔離的 receipt → SSE 測量。
  最終 resource snapshot 包含 48 個已載入服務（含 failed／inactive），觀測區間 9.65 秒；
  隔離 SSE 的 4 筆樣本中位數為 2.21 ms，不代表開盤訊號或瀏覽器顯示延遲。
  first-observed 不是 cold cache 證據。不要把重啟前後不同快取狀態當成來源重建改善。
- 公網 IPv4 HTTPS：當沖頁 HTTP 200，約 261 ms；永豐 API HTTP 200，約 45 ms。
  這是各一次觀察，不是 WAN p95；本輪沒有重新驗證 IPv6。
- 只重啟 public gateway，PID 1865832 → 1915070。Discord 1898511、當沖 1898843、
  隔日沖 1140、期貨 BidAsk 1150014、Top200 1152678 未改變；NRestarts 均為 0。
  當沖 engine／Discord 的實測同步 revision lag 為 0。

## 尚未消除的成本與問題

1. 完整當沖歷史約 317,302 個分鐘點的剖析，成本仍集中在逐點 add_values／
   add_complete_ledger 與 benchmark 投影讀取；帶 profiler 的一次來源重建約 6.82 秒，
   不能跟沒有 profiler 的 wall time 比較。沒有刪分鐘、改價格、改帳本或虛報成交。
2. gzip 固定 body 實驗：當沖 14.70 MB JSON 的 level 5 約 378 ms／3.83 MB，
   level 3 約 208 ms／3.89 MB；TAIFEX 降壓縮級別則增加約 24% 傳輸量。
   受限頻寬可能更慢，所以沒有盲目降低全站壓縮級別。
3. 12:59 觀察仍為當沖 `degraded`、隔日沖 `waiting`、全資料監控 `critical`。
   已存在的 `stockagent-registered-data-features.service` 是 09-22 Binance perpetuals
   步驟失敗（exit-code），本輪沒有重跑整個歷史下載或 reset-failed 掩蓋它。
4. 未量測每一家外部資料供應商的 RTT、開盤行情到達下限、所有背景工作完整生命週期，
   也沒有完成長時間負載壓測。資源 counters 和 HTTP 200 都不是這些工作的健康／完整證明。

## 原始證據

- `artifacts/benchmarks/services_latency_2026-09-23_before.json`
- `artifacts/benchmarks/services_latency_2026-09-23_after.json`（重啟後第一次，測試曾同時執行）
- `artifacts/benchmarks/services_latency_2026-09-23_verified.json`（獨立重測及固定輸入實驗）
- `artifacts/benchmarks/services_latency_2026-09-23_final.json`（納入 inactive/failed 服務）
- `artifacts/benchmarks/services_inventory_2026-09-23_paired.json`
- `artifacts/benchmarks/services_shioaji_2026-09-23_paired.json`
- `artifacts/benchmarks/services_snapshot_timing_2026-09-23.json`
- `artifacts/benchmarks/services_compression_2026-09-23.json`
- `artifacts/benchmarks/services_daytrade_history_2026-09-23.prof`

控制版本保存在 `artifacts/benchmarks/services_20260923_control/`，不參與 production runtime；
receipts 保留實際 dirty-worktree 原始碼 SHA-256，不只引用 Git HEAD。
