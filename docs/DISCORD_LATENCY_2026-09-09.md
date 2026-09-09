# Discord 開盤與盤中延遲修正驗收

2026-09-09，penguin。已修改並重啟 Discord 服務；本次沒有更換 checkpoint、重跑交易歷史或送出真實委託。

## 根因與修改

9/8 的盤中 summary 顯示模型通常僅耗時 36–44 ms，而行情請求耗時
1,232–6,309 ms。行情、GPU 鎖、資料檢查及輸出原本串接在同一條路徑。

- MIS 改用程序內共用的 16-worker HTTP 連線池，保留 keep-alive；達到設定的
  90% response coverage 後，跳過多餘的空批次 retry。來源不足仍走原有備援。
- 不同模型共用仍在取得中的重疊股票，只查差額。依各自股票順序組回，保留各自
  fallback 與缺價遮罩。完成後不保留盤中價 TTL，新指令會取得新價。
- 盤中行情在 GPU 鎖外取得；GPU/model runtime 仍序列化。取得鎖後重新比對完整
  quote request；股票池、價格基底、來源或遮罩改變，以及完成接收超過 5 秒時，
  重新取得行情。
- 快取 immutable panel/checkpoint 的排列結果。source object identity 與 checkpoint
  revision 都必須相同；legacy checkpoint 不走此排列快取，manifest 仍逐次驗證。
- 相同的同時指令合併為一次推論；一位使用者取消不會取消其他等待者。
- 開盤邊界先讓排程訊號完成發布，再讓互動請求使用行情與推論資源。
- 盤中指令保留獨立 signal artifact 供明細／按鈕查詢，但不覆蓋排程
  `latest_signal.json`。當沖報酬從正式 returns 取得，省去歷史 preview 目錄掃描。
- 顯示整理移到 worker；audit 增加毫秒時戳、interaction ID、readiness、quote、
  GPU queue、生成及送達耗時。
- 沿用既有 Shioaji 備援與常駐 paper engine。使用 Shioaji skill 的連線重用與
  非阻塞工作分工原則；沒有為 benchmark 登入額外 broker session。

## 測量

四模型使用真實 checkpoint 與已驗收 daily features；下表的對照使用明確標示的
synthetic quote fixture，每次行情 I/O 固定等待 250 ms，未送 Discord REST 訊息。
控制組與改善組都使用本次排列快取，差別是行情是否在 GPU 鎖外取得。

| 對照 | 四模型同時呼叫，整批耗時中位數，3 次 |
|---|---:|
| 行情在 GPU 鎖內的控制組 | 2,667.493 ms |
| 行情預取／共用後 | 1,831.079 ms |

下降 31.35%。結果與逐指令 audit 在
`artifacts/operations/discord_latency_20260909/command_benchmark.json`。
四模型的真實 panel-only 熱推論另量得約 172–198 ms；此數字不含外部行情與 Discord RTT。

benchmark 同時在 temporary directory 驗證四模型均產生完整私有 artifacts，且既有
scheduled pointer 內容保持原樣。測試用收件資料與正式開盤 receipt 目錄已隔離；
本輪舊測試產生的 A/B 假資料 receipt 移至
`artifacts/operations/discord_latency_20260909/test_receipt_20260909.quarantined.json` 保留證據。

實際 MIS 探測在凌晨遇到 `RemoteDisconnected`，完整股票池驗收拒絕了不足覆蓋；
因此本次不宣稱實際盤中行情 RTT 或 Discord 端到端傳輸已達某個保證值。

## 驗收

- Ruff、py_compile、`git diff --check` 通過。
- 390 tests 通過：Discord helper／formatting、quote provider、signal helpers、
  新增 concurrency／freshness／取消與開盤優先測試、當沖模擬及兩階段冷測試。
- 01:01:18 重啟 Discord；22 global commands 同步成功，Gateway connected。
  新程序 4/4 startup warm ready，耗時 8.619 秒。重啟後無 watchdog／fatal error，
  `NRestarts=0`。
- 重啟後以假 Discord transport、真實現行官方 artifacts 呼叫原本的
  `_handle_signal_now_command`；四模型均有 `accepted → cached → delivered`，
  `delivery=original_response`，並產生明細頁。本地總耗時：多基底 501.519 ms、
  一億 352.279 ms、多基底 22 237.053 ms、Projection-L1 GELU 247.654 ms。
- 公開 gateway `/healthz` 為 `ok`；當沖面板 HTTP 200，
  `revision_status=synchronized`。凌晨當沖 engine 的 `health=waiting`，
  不把它當成今日 08:30 資料驗收或 09:00 開盤成交證明。
- 既有正式歷史維護仍有 2 個 market 失敗，maintenance 為 `degraded`；
  interactive jobs 為 `ready`、0 failed／0 waiting。保留這個獨立健康狀態，
  本次沒有清除失敗紀錄。

可重現入口與參數見 [Discord README](../services/discord_bot/README.md)。
