# FinLab 昨日效率稽核與 Polars／Arrow 改造（2026-10-11）

## 交付範圍與結論

沿用既有 FinLab collector、帳號 owner lock、systemd、不可變來源物件、
收據與唯讀 gateway；沒有另開下載管線，也沒有改交易／訓練語意。
目前 acquisition policy **v8**：32 個分鐘日期窗程序、31 日曆日窗口、
32 路控制端、每程序一條下載線、有界四倍佇列；一般表每程序最多 8 鍵，
每鍵仍有 600 秒 deadline。每個程序的 Polars／BLAS 計算執行緒設為 1。
這些是本機配置，不是官方 req/s 上限。

FinLab 自有的下載後處理、驗證、tick 聚合、成交量單位視圖與本機稽核，
已移除 pandas import／轉回 pandas 的路徑。只在
`scripts/finlab_columnar.py` 將 SDK 回傳物件一次轉為 Arrow，之後以
Arrow 或 Polars 處理。官方 SDK 2.1.1 本身仍依賴 pandas，不能卸載該
依賴後宣稱原生 SDK 仍能工作。獨立的舊版 pandas 比較器只留在 `test/`，
不被正式服務載入。

截至 10:32 台北時間的驗收，既有已知分鐘窗口的 99 個待補分區已歸零；
其中 84 個舊 provider error 已逐一核對 SHA、大小、筆數、股票／日期／
時區與 OHLCV。這不代表官方尚未上架的歷史已抓完，也不代表 PIT 訓練認證。

## 先量測，再定位問題

昨日「台北日曆日」是 10/10 00:00–24:00；額度週期則是
10/10 08:00–10/11 08:00，兩個分母不同，不能混用。

| 證據 | 數量／耗時 | 意義 |
| --- | ---: | --- |
| 昨日日曆日不可變 intraday 批次 | 130 批、54,160 新收據股日 | 不是面板最後 256 筆樣本推算 |
| 因一般表工作到期而讓出 intraday owner | 39 批 | 一般表啟動與大表重試會切斷分鐘工作 |
| 一般表來源檢查 | 3,118 鍵次，其中 2,802 不變 | 單鍵重啟 SDK／掃全清冊的成本沒有產生新資料 |
| 原 inventory 重新下載路徑 | 600 秒反覆逾時；中止前 cgroup 峰值約 8.74 GB | SDK 大表＋索引字串化與 dataframe 複製，不只是網路等待 |
| 原到期重排上限 | 16 日期窗 | 32 個 worker 仍可能只工作短短一小段就退出 |
| 額度週期最後取樣 | 1,945.891／5,000 MB，38.92% | 額度尚未用完；最後約 59 秒沒有取樣，不能當精確結算 |

intraday owner 聯集耗時約 47,906 秒；外部約 38,494 秒包含一般表、清冊、
overlay、timer 及未識別時間，**不能全部稱為閒置**。

## 根因修正與排程

1. **一般表以有界批次重用 SDK。** 每鍵清掉記憶體 frame／已載入標記，
   保留磁碟快取、generation 和 publication 證據。不呼叫會刪磁碟的
   `FileStorage.clear()`；不支援的 adapter 自動維持一鍵一程序。
   watchdog 使用不能被 SDK `except Exception` 吞掉的 deadline；連續兩鍵
   逾時即退出並留下收據，避免整批無限重試。每鍵重新確認開盤資源保護。
2. **inventory 直接以 Arrow record batch 正規化。** 原始 IPC 與舊 Parquet
   都保留；辨識官方字典編碼後解成真正標籤，不把 dictionary code 當值。
   已驗證的相同 raw SHA／正規化版本可略過重做 Parquet。過期原始快取
   先用官方 `.recent` metadata 判定是否有 delta；有 delta 就走完整官方
   物件取得，不另寫自製 delta merge，也不把 metadata 檢查當成原始取得時間。
3. **分鐘優先、有限錯誤重試、游標後方也補洞。** 同股票按官方最多
   [31 日曆日](https://finlab.finance/docs/details/intraday/) 合併可取的缺日。
   到期重排依 worker／佇列取最多 128 日期窗；最近日期優先，之後已知
   缺口，再往歷史前沿。一般 provider error 每額度週期最多三次短退避；
   耗盡留紀錄並讓其他工作前進，下一週期再查。429／Retry-After 共用冷卻。
4. **待補總量與立即可做工作分離。** 明確顯示 actionable／blocked；
   冷卻中的錯誤不製造無效派工，也不無限阻塞剩餘額度的低優先 Tick。
   未發布來源仍保留 unknown／waiting，不捏造空值或完成。
5. **重置尾端可得性重查。** 平常已知窗口未發布資料每兩小時重查；
   08:00 重置前最後一小時縮到 10 分鐘，並解除本機額度停止線。
   更早未知歷史仍使用獨立有界探查；官方額度、磁碟與開盤守門不取消。
6. **完成時間與新鮮度分開。** `acquisition_completed_at_utc` 只用於判斷
   是否已成功恢復舊錯誤，不能推進 `source_checked_at_utc` 或當發布時間。
   SDK 七字元有效 revision hash 可使用，不能因原本要求至少八字元而
   忽略已經通過官方驗證的不變證據。

## Columnar 處理契約

| 處理 | 現在的實作 |
| --- | --- |
| 原生寬表 | 一次 SDK→Arrow，Arrow 非空統計；不 melt／不複製全表 |
| 多日 intraday | 一次 SDK→Arrow→Polars，按日期一次分組，避免逐日重掃整窗 |
| tick→分鐘 | Polars native 聚合，只有完整單位證據才加 shares；不補零成交分鐘 |
| tick 排序 | O(N) 證明完整 timestamp／sequence 順序，只有不符才穩定排序 |
| 成交量及固定價格盤後 | Polars nullable Int64／向量化金額價格校驗，原始 volume／lots 保留 |
| 存檔／校驗 | Arrow Zstandard Parquet；Polars 投影／Arrow bounded batch，沒有轉回 pandas |

保留原始正負值、比例、null／NaN 的既有 Parquet 語意、欄位順序、股票代號、
多層索引及月份／季度標籤。奈秒與時區都保留；驗證補上不足一微秒的錯誤
分鐘對齊檢查。正常 13:30 及符合年代規則的實際 13:33 收盤成交不搬回早一分鐘。
同時間 tick 保留 sequence；null sequence 維持原本 stable nulls-last 行為。
VWAP 仍是 Float64，跨引擎減法只允許明訂的末位誤差，不改 raw OHLCV。

SDK 正規化版本升為 2，另有 `processing_contract_version`／
`processing_engine`。設定 fingerprint 包含 acquisition version、計算執行緒
及新引擎；ETA 不拿舊引擎／不同設定的批次冒充新版量測。舊來源收據與
   可用物件不因換引擎而批量重抓／刪除。

## 實測與保留的限制

| 實測 | 結果 | 口徑 |
| --- | --- | --- |
| 同 8 張小型財報表，一鍵／8 鍵，1-8-8-1 配對 | 86.61 → 38.50 秒，**2.25×** | 完整來源檢查、清冊、轉存／收據及程序啟動；不是全體大表 |
| 同 cached inventory generation，全 23,374,896 列比對 | 轉換 13.09 秒；含完整值／型別／null／SHA 比較 22.66 秒；程序 RSS 峰值約 810 MiB | 舊 generation 的離線等價證明，不是當日源新鮮度 |
| 正式 inventory 來源取得 | 23,374,896 列，2016-11-04–2026-10-08 | 不可變 raw／來源取得、字典編碼與有效收據另行核對 |
| 同 122,199 筆 2409／2026-10-05 tick，完整驗證＋聚合，五次 AB/BA | 配對總和 2.258 → 0.928 秒，**2.43×** | Polars 單執行緒；所有來源列，比較全部輸出；不是 API／磁碟 I/O 吞吐 |

也保留最初兩次較慢的測試：把「pandas 只聚合、不驗證」與「Polars 含兩次
驗證」相比，候選速率比例為 0.85／0.67，**不能用它們推論完整處理變慢或
宣稱所有階段都更快**。修正比較口徑後，上表兩側都包含來源與分鐘輸出校驗。
沒有隱藏較慢／失敗結果，沒有為速度跳過校驗。

正式來源的 84 個回補分區及最近 8 個分區驗證通過，台股／FinMind 等
受保護服務身份與基線相同。唯讀 gateway 可讀新版 quota policy 2、
acquisition policy 8；不會為了頁面取樣再呼叫 FinLab API。
實際 collector 子程序的 Polars／OMP／OpenBLAS／MKL／NumExpr 限制均為 1。

仍有邊界：來源全部空值的 `management_change_events:變更交易開始日`，
以及尚未上架的古老分鐘／Tick，不得宣稱完成；沒有更早已發布資料時，
不能估一個假的全歷史完成日。已知窗口目前已齊，剩餘額度可做低優先 Tick；
不能保證每天一定用完 5,000 MB 或整體 API 吞吐跟本機加速倍數相同。

## 重現與證據

相關語意／生命週期／單位／唯讀頁面回歸 **585 項通過**；另做正式
來源分區、共享配額、timer、設定身份與受保護服務核對。舊失敗收據保留，
以同任務後續成功結果記錄 superseded，沒有刪掉失敗紀錄來宣稱成功。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_finlab_runtime.py --efficiency-day 2026-10-10 \
  --output artifacts/data_quality/finlab_yesterday_efficiency_2026-10-11/yesterday.json
POLARS_MAX_THREADS=1 OPENBLAS_NUM_THREADS=1 run_fintech_python test/benchmark_finlab_columnar.py \
  --repeat 5 --output artifacts/data_quality/finlab_yesterday_efficiency_2026-10-11/columnar_tick_workflow_benchmark.json
run_fintech_python -m pytest -q -s test/test_finlab*.py test/test_derive_finlab_minute.py \
  test/test_audit_finlab_intraday_share_units.py test/test_public_dashboards.py \
  test/test_downloader_integrity_repair.py
```

證據：[昨日日曆日與額度週期](../artifacts/data_quality/finlab_yesterday_efficiency_2026-10-11/yesterday.json)、
[一般表配對實測](../artifacts/data_quality/finlab_yesterday_efficiency_2026-10-11/general_batch_benchmark.json)、
[inventory 全列等價](../artifacts/data_quality/finlab_yesterday_efficiency_2026-10-11/inventory_arrow_benchmark.json)、
[完整 tick 處理對照](../artifacts/data_quality/finlab_yesterday_efficiency_2026-10-11/columnar_tick_workflow_benchmark.json)、
[正式來源／服務驗收](../artifacts/data_quality/finlab_yesterday_efficiency_2026-10-11/final_acceptance.json)、
[新版唯讀 API／實際處理引擎](../artifacts/data_quality/finlab_yesterday_efficiency_2026-10-11/public_contract_acceptance.json)。
