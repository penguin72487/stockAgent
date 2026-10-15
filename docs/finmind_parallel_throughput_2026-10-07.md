# FinMind 最新預估與本機並行吞吐修復

範圍：既有 FinMind Complement 抓取／本機彙總、每分鐘 ETA 與 `/finmind/`。
任務 `finmind-latest-eta-throughput-20261006`。不另建下載管線、不改訓練值，
不重啟 Shioaji、交易或其他 provider 的 collector。量測跨 10/6–10/7（台北）。

## 結果與量測邊界

10/7 00:13 的生產驗收：10 批、152 個完整日期分區、257,447,032 筆原始資料。
批次累積牆鐘 558.24 秒，接受吞吐 980.23 日期分區／小時、461,177 原始筆／秒。
包含下載、檔案驗證、必要分點彙總、Parquet／SHA／收據發布；不是孤立 CPU 測試
或 API 配額。此值不含批次之間的初始化／其他工作等待，且只適用目前分點階段。

修復前同階段的滾動一小時實發量為 82 次，串行自有成本中央容量約 58.64 次／時。
不同時窗、日期大小、其他主機負載與網路狀況不同，不能把相除當成受控全系統加速。
第一、二批含啟動／網路等待分別 116.18／170.42 秒；後續多批明顯較快。
新預估保留慢端樣本，不宣稱必定維持每小時 980 個分區。

服務現在持續抓取；**全部歷史尚未完成**。25,000,000 級的未建搜尋候選主要仍是
排在最後的美股分鐘資料，候選不等於已證實缺失 K 棒。

## 根因

1. 全市場原始檔約兩百萬筆。串行路徑把 Arrow 解成大量 Python 字典、執行 Decimal
   聚合後再轉回 Arrow，單日必要本機處理約 35–47 秒；下載等下一日也被它阻塞。
2. 服務 `MemoryHigh=2G` 已有數千次 high 事件，即使主機尚有大量可用記憶體，
   工作集仍被 cgroup 節流。`Nice=10`、CPU／IO 權重 20 也降低它在競爭時的優先度。
3. 大型檔案的串行等待與 82 次／時的實發量，被拿來限制小型 JSON 追新的容量，
   容易推導出不合理的追新過載、過長 ETA。
4. 最新快照已升版，後端與 JS 仍以固定版本號上限拒絕；固定 status TTL 也可能在
   新原子快照發布後繼續傳舊估時。

## 修改

### 同一 owner 的有界管線

`configs/finmind_object_pipeline.json`：8 HTTP workers、8 CPU processes、每程序 1 Arrow
thread、每批最多 16 次資料 API grant。HTTP 工作各用自己的 Session，所有 grant
仍走同一帳號的 shared limiter／流量帳本；signed object GET 不冒充第二份資料額度。
官方／帳號驗證 SponsorPro 上限仍為 **20,000 次／小時**、pacing 0.18 秒。

```
同一共享限速 → 8 HTTP workers → 原始檔驗證／單一 owner 接受
                                ↓
                            8 CPU processes
                                ↓
                       單一 owner 發布衍生值與收據
```

SQLite 任務領取、失敗處理、收據與目前 head 發布都由原主程序持有。CPU 程序只讀
已驗證父收據，回傳 Arrow Table，不繼承 queue writer。最多 8 個下載與 16 個在途
CPU 工作，有 backpressure，不按日期數無界建立 future。

持久 claim journal 只恢復本管線中斷的 `inflight`，不重開成功項目或英股 131 個
`retry_exhausted`。到期追新、開盤保護、共用預算與磁碟保護停止新 admission；已開始
的工作有界排空，不能把啟動 heartbeat 或原始檔已到達當成必要彙總已完成。

原九類歷史順序、有限指定修復、來源收據與 **WSL 25 GiB** 剩餘空間守門不變。
原始檔位置不變。尚未完成的工作繼續用既有 `--loop` 排程，不需要手動逐批啟動。

### 精確計算而非改變數值

`finmind_broker_aggregation.py`：Arrow 欄式分組、Decimal128 精確乘加與整數股數。
用最小原始 row index 保留原參考實作的第一個分點名稱，重複明細仍參與加總。
加權價格最後仍以原 Decimal 28-digit 契約相除，再轉為既有 float64 輸出。

Decimal128 buffer 的小端 scaled integer 直接進入精確除法，避免數百萬個 Decimal
scalar／Python 字典往返。大端或不支援的數值形態走既有參考路徑，不以浮點乘加
取代，不做截斷、log、補零或偽造缺失成交。衍生收據標記
`aggregation_engine=arrow_decimal128_table_exact_v2` 並綁定父 SHA。

### 主機資源

本機 AMD 5800X3D：8 實體核心、16 logical CPUs。實測 1／2／4／8 程序選 8×1。
同一份八分區工作、兩輪反向順序完整讀取／精確處理／持久化測試：

| CPU 程序 × Arrow threads | 第一輪秒數 | 第二輪秒數 |
| --- | ---: | ---: |
| 1 × 1 | 67.57 | 88.29 |
| 2 × 2 | 41.79 | 56.50 |
| 4 × 2 | 36.92 | 47.52 |
| 8 × 1 | 32.66 | 23.08 |

相同優化演算法下，8×1 兩輪平均比 1×1 約 2.8 倍；全部輸出逐值一致。
這是 warm-cache、無 HTTP 的完整本機測試，不冒充生產端到端速度。未證明其他硬體、
其他資料階段或所有可想像並行度的絕對最佳值。

只提高 FinMind Complement：Nice 0、CPUWeight／IOWeight 100、MemoryHigh 12G、
MemoryMax 20G；保留原 heavy-data slice、25 GiB 空間與開盤保護。
驗收當時 cgroup 記憶體約 5.9 GiB，`high=oom=oom_kill=0`、`NRestarts=0`。
沒有新增 GPU 計算。主機既有其他工作仍會影響速度，未停止它們來製造測試成績。

### ETA 與網頁

- 每分鐘快照契約 13、`scenario_projection_contract=1`。已知能力的新版快照可直接
  投影，未知能力仍拒絕；數字版本號不再作為前後端的固定上限。
- status cache key 包含 ETA 原子檔的 inode／mtime／size，新快照立即失效舊 TTL
  快取；同一代的併發請求仍 coalesce。瀏覽器不呼叫 provider。
- JSON 用近期已接受回應到本機完成的自有成本；無本資料集樣本時明確列同類代理。
- 同配置至少兩批完整接受後才提供並行容量。中間值為
  `sum(accepted_requests) × 3600 / sum(full_batch_wall_seconds)`，不是四筆樣本中
  偏快的 upper median，也不把官方 20,000／時當成檔案完成速度。
- 源檔／必要衍生尾端失敗的批次不算接受容量；不同配置／舊演算法成本不混入。
- 九類逐階段時間累加、發布／到期追新、未知樣本、過期快照、候選與完整性邊界保留。
  全部時間仍是條件模型，不保證停機、來源錯誤、全量儲存足夠或永久追新完成。

## 驗收與重現

- FinMind 全測試 960 passed；公開 dashboard 125 passed；最新估時／即時快取針對性
  75 passed；共用 JS 元件 38 passed。完整專案測試未執行，未宣稱全 repo suite green。
- 本機與公網 1440／1024／390 三種寬度，估時數字／台北完成日／九階段進度／排序
  均驗證，零 JS exception、零整頁水平溢出、零瀏覽器 provider API 呼叫。
- 生產 32 組父子 SHA／原始 bytes／收據檢查；另新日期 2024-03-06 的 2,251,097 筆
  原始明細，由獨立既有 row oracle 重算 613,722 筆衍生結果，所有值完全一致。
- 交易與未指定 worker 的 InvocationID／NRestarts 不變；只重啟 Complement 與公開閘道。

證據根目錄：
[finmind_latest_eta_throughput_2026-10-06](../artifacts/data_quality/finmind_latest_eta_throughput_2026-10-06/)。
`broker_benchmark.json`、`broker_pipeline_optimized.json` 保存參考 SHA／量測；
`after/pipeline_acceptance.json` 保存獨立逐值生產檢查；
`after/pipeline_acceptance_latest.json` 保存十批實際吞吐；
`after/deployment_after.json` 與 `after/browser-*-latest/browser_acceptance.json` 保存部署／渲染。

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.verify_finmind_object_pipeline \
  --baseline artifacts/data_quality/finmind_latest_eta_throughput_2026-10-06/after/deployment_before.json \
  --output artifacts/data_quality/finmind_latest_eta_throughput_2026-10-06/after/independent_recheck.json \
  --verify-values
```

此驗收只讀 queue／來源收據，零 provider 呼叫與 queue 寫入；`--verify-values`
會做一個日期的完整參考運算。未完成其他資料階段／超出儲存容量時不能把服務 active
或本機快測通過寫成全部資料已抓齊。
