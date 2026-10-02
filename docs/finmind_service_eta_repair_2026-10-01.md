# FinMind 抓取服務與 ETA 修復（2026-10-01）

修復契約：`finmind_service_eta_repair_v1`；本機 ETA 快照契約升為 2。
沿用三個既有 worker、共用 Sponsor 帳號、SQLite 佇列、每分鐘 quota timer 與唯讀面板。
這次不改資料粒度、原始值／單位、來源發布證據、訓練 ABI 或冷庫同步。

## 先驗證問題，再制定修復計畫

| 層次 | 本次取得的證據 | 修復與驗收要求 |
| --- | --- | --- |
| 抓取服務 | Complement 16:53:04 因 `_record_request_start` 的 `sqlite3.OperationalError: database is locked` 結束；16:54:04 自動重啟。不是 OOM。 | 修共用入口，不能略過記帳後照樣送 API；三個 lane 都要能安全重試。 |
| 請求記帳 | 三個 worker 共用 `request_traffic.sqlite3`；原為 rollback journal、兩秒 busy timeout。連線 context manager 只提交／回滾，不負責明確關閉連線。 | 同主機 ext4 改 WAL、FULL 同步、五秒 busy timeout，明確關閉 writer／quota／面板 reader。未成功持久化的請求不送出。 |
| ETA 更新 | 配額 timer 每分鐘成功，但 ETA 停在 14:32:02，14:37:02 已到期。實際本機重現 `priority_override_query_grain_unverified`。 | 以下載器實際查詢契約計數，不以舊 queue kind 名稱拒絕有效請求；預估失敗與配額取樣成功分開記錄。 |
| 優先任務 | `TaiwanFuturesKBar` 的 CR1／2012-08-22、CJ1／2015-08-13 標成舊 `day`，不是現在的 `id_day`。實際仍由相同商品＋單日契約下載。 | 僅支援已確認 day source＋非空 ID 的兩種名稱；不擅自把未知查詢當成一次。p0 已屬追新，不能再算進 override 一次。 |
| 累計里程碑 | 校驗階段 0 次時，舊 `milestones.all` 複製末階段，誤呈現 `current`／0 次，雖然前面還有數千萬候選請求。 | 工作量、狀態、請求量、有效工時與 blockers 必須累計所有前置階段。單階段和累計里程碑分開。 |
| 估時計算 | frozen-input profile 中大量時間用於反覆重算同一天的時區／開盤邊界。 | 快取單次投影內每一天的不可變邊界與日曆決策；不減少發布事件、階段或改變容量運算順序。 |

主要文件依據：[SQLite WAL](https://sqlite.org/wal.html)、[SQLite transactions](https://sqlite.org/lang_transaction.html)、[FinMind 方案限制](https://finmind.github.io/Pricing/)、[官方帳號用量 API](https://finmind.github.io/api_usage_count/)。
部署當下 SQLite 是 3.53.4，記帳資料庫在本機 ext4；WAL 不適用跨主機網路檔案系統。
本次能證明記帳的鎖定例外，不宣稱已捕捉到當時是哪個 reader／writer 持鎖。

## 實作

- `download_finmind_free._record_request_start` 是所有 lane 的共用記帳入口。鎖定逾時變成 `local_traffic_busy`，15 秒後重試；其他 SQLite 失敗為 `local_traffic_unavailable`，60 秒後重試。保留原本任務與收據，不偽造來源資料，也不送出未記帳的 API 呼叫。
- Complement 將此本機錯誤轉成既有 `SourceError` queue retry；Sponsor 共用同一 `_fetch_rows`，不再讓記帳鎖定跳出正常錯誤處理而殺掉服務。這不是新增來源錯誤／限流繞過。
- quota、海外份額、面板讀取者明確關閉 SQLite 連線。額度模型與 shared limiter 沒有放寬。
- `finmind_eta_work` 依來源的單日契約識別 legacy `day`；p0 不再重複搬到 override。未確認的 grain／缺 ID 仍拒絕提供捏造的估算。
- `finmind_eta_stages` 的累計 milestone 不再只複製最後階段；正常沒有工作、未知等待、在途／本機處理等狀態仍分開。
- `snapshot_finmind_quota` 每次正常配額取樣後都刷新 ETA，包括首次／檔案遺失情境。新增小型本機 `eta_refresh_status.json`，記錄成功／失敗、allowlisted error code 與計算耗時；不保存任意 exception message。失敗保留舊 ETA 證據，仍按五分鐘到期規則失效。
- 每日邊界快取只在一次投影內使用，輸入截止日、發布時點、所有情境與開盤保護都保留；不以快取舊資料取代新佇列盤點。

## 測試與性能

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q test/test_finmind*.py \
  test/test_probe_finmind_max_ranges.py test/test_tw_futures_finmind_ticks.py \
  test/test_market_status_calendar.py
```

最終 **759 項通過**。新增／補強三個 writer 在長讀取快照下並行、鎖定不送 API、保留 queue 重試、連線關閉、legacy day／p0 不重複計數、未知 grain 保持拒絕、里程碑累計、日曆跨午夜與 ETA 健康紀錄／敏感文字隔離。

同一份約 77,888,234 次已知候選工作、70 組發布事件，採 ABBA 順序比較完整 `ordered_estimate`：

| 項目 | 第一次 | 第二次 |
| --- | --- | --- |
| 修改前 | 5.00 秒 | 5.25 秒 |
| 修改後 | 1.58 秒 | 1.65 秒 |

平均減少約 **68.5%**；全部輸出欄位精確相同。這是固定輸入的整段排程投影比較，不是全系統／網路吞吐改善的百分比。
實際 quota＋佇列＋ETA timer 在修改後觀察約 7–15 秒完成；它包含工作清點與其他開銷，不能與模型單獨時間混為一談，也不是固定輸入的 A/B 比較。

## 部署後驗收

- 17:22:56 重載 Free／Sponsor／Complement；17:22:57 重載唯讀 gateway。未重啟其他下載、交易、訓練或儲存服務。
- 17:23:55 驗證記帳資料庫已是 `wal`，重載後仍有真實期貨 K 棒請求，三個 worker heartbeat 均存活。Free／Sponsor 在等待自己的下次發布檢查，不是卡死。
- 17:23、17:24、17:25 的既有 quota timer 連續成功，ETA 一起刷新；不再每分鐘只記到配額成功而估時失敗。
- 17:26:34 針對重載後截至該時間的 **326 個 accepted 分區**逐一核對 queue／收據列數；其中 **210 個非空、116 個來源空回，63,641 列**。非空 Parquet 校驗亦通過，失敗 0。來源空回不算取得了非空資料。
- 外部 `/finmind/api/status` 17:25:17 驗證 132 列、三個 worker 心跳、有效 ETA，以及非零的 core／non_tick／all 累計工作量。HTTP 200 之外，亦核對實際 DTO 與新收據；本次沒有改 UI 排版，沒有宣稱新的視覺驗收。

證據目錄：[本次驗收](../artifacts/data_quality/finmind_service_eta_repair_20261001/)。
重點檔案為 `eta_failure.json`、`pre_deploy.json`、`eta_boundary_cache_benchmark.json`、`post_reload_receipt_acceptance.json`、`public_endpoint_acceptance.json`。
完整回歸結果亦保存為 `finmind_regression.xml`；17:33 讀取新版快照時已確認 `snapshot_contract_version=2`，見 `final_live_eta.json`。

## 目前估時的正確讀法／限制

所有時間都是「已知待抓＋未來追新模型」的條件式預估，不是保證完工日。三個 worker 共用實際帳號觀測的 Sponsor 6,000 次／小時，不是每支各有 6,000 次。

約 7,789 萬次工作大多是分鐘／分點／tick 的歷史日期候選，其中含尚未排除的上市前、休市等日期；**不是已確認缺失的 K 棒數，更不是資料列數**。里程碑仍依優先回補→主要歷史／新聞→其餘明細→tick→校驗順序；不把低優先 tick 的多年投影當成主要資料也要等多年。

17:32:02 台北的 snapshot：主要歷史／新聞（累計包含指定優先期貨回補）約 82,499 次已知待發，條件式完成時點為 10/2 **12:52／16:15／16:51**（最快／中央／保守）。指定優先回補階段本身約在 10/2 **00:59／02:20／02:41**。這是當時的動態模型，不是新的固定排程或承諾；最新數字看 [FinMind 面板](https://penguin72487.ddnsgeek.com/finmind/)。

未來來源修訂、失敗、停機、新商品、配額改變和空間不足會延長時間，完整歷史可取得性未被證明。重載前磁碟約剩 62 GiB，既有 25 GiB guard 保留；估時仍假設空間足夠，不表示全歷史容量已規劃完成。
這次沒有宣布所有 FinMind 歷史資料抓齊、精確發布時間可考或可直接 PIT／實盤訓練。
