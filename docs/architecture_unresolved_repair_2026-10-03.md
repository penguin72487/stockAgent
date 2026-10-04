# 架構未解問題續作（2026-10-03）

本輪接續 [技術實測](architecture_technology_trials_2026-10-03.md)，優先處理已經
阻斷正式工作與尚缺恢復證據的問題。原報告的失敗及未驗證欄位保留；後續成功
由新收據提供證據，不回改當時的觀測。

## 第一性原理與責任入口

資料工作成功需要來源、讀取、衍生、稽核和消費端依序成立。增加 CPU、GPU 或
排程引擎無法修正混合檔案 schema；備份可列出內容也無法證明另一台機器能恢復。
本輪先修共享讀取契約，再驗正式完整工作流與同一快照的離機還原。

| 上輪缺口 | 本輪處理 | 驗收邊界 |
| --- | --- | --- |
| Shioaji 分鐘歷史建置反覆 `SchemaError` | 共用原始分鐘 scanner 按完整 schema 分組批次讀取；只補可選 metadata，保留來源原值與單位推定 | 完整建置、全分區 audit、daily／hybrid 原入口依序驗證；來源缺口仍獨立分類 |
| daily／hybrid 舊稽核允許非交易日行情 | 沿用 receipt-backed TAIEX calendar；原始來源保留，衍生表隔離，舊快取需按來源 SHA／chunk 身分升級 | 當前選定商品有 286 筆 daily 對應 391 筆原始分鐘，另有一筆未被使用的舊檔；不以 weekdays 猜官方交易日 |
| daily control backup 缺少對應快照及離機還原 | `pg_export_snapshot` 綁定所有使用者表的列值／欄位指紋與 `pg_dump`；另一台機器建立獨立 PG cluster 核對 | 不包含 role credentials、來源／模型、HA、PITR 或節點重建 |
| 遠端建置速度只沿用舊結果 | 當下 Mamba role 重測 declaration／explicit；PG 還原測 1／2／4 workers | 三輪交錯完整建置／還原與實際套用，報告觀測均值；不宣稱全域最快 |
| 遠端磁碟持久性未知 | 實查 `vast-capabilities` 與 filesystem／容量 | 本節點 `workspace_is_volume=false`，離機還原成功不能提升為持久副本 |
| control 長連線只在啟動檢查版本 | 每次操作先在 transaction 中 SHARE lock 版本表並核對版本 | 鎖定期間 metadata 更新須等待；版本變更後連線下次操作拒絕，無自動 schema migration |
| DuckLake／Iceberg／Temporal 試驗尚未正式採用 | 查實際呼叫入口、現行 work state 與正式 units | 採用依實際多 writer／多引擎／持久工作鏈需求決定，不把試驗部署稱為全系統遷移 |

## 分鐘資料共享根因

舊原始 chunk 有 11 個 core columns；新 chunk 多出
`source_volume_multiplier`、`volume_shares`、`volume_unit_proof`。原先對全組 paths
直接 `scan_parquet`，要求同 schema，正式建置因此失敗。逐檔建立 lazy scan
會增加大量計畫節點；以第一個檔案的 schema 忽略 extras 又會依檔案順序丟失
來源 metadata。採用等 schema 的批次掃描，再補 typed NULL 的可選欄位。

`downloader.stock_volume_units.scan_stock_minute_sources` 由 research builder 與
日比對工具共用。必需的 Amount／OHLC／Volume 等原始欄位缺少仍拒絕；不相容
數值／字串型別仍拒絕。相同 timezone 的 timestamp 單位安全升為 ns，不增加
時區偏移；全 Null 可選欄位可與後續具體型別相容。原始來源檔不改寫。

既有 Amount／OHLC 推定仍重新計算股數，不能信任 chunk 中的舊衍生股數或倍率。
不插補價格、缺失行情、成交量或歷史可用時間。reader contract
`required_raw_schema_grouped_nullable_metadata_v1` 寫入 manifest／日摘要；正式 reuse
必須同時符合這個 contract、來源 fingerprint、target 和全量 audit SHA。
模型 feature ABI、決策／執行時鐘及交易規則維持既有契約。

## 控制資料庫恢復

正式 `stockagent-control-backup.service` 使用既有 Mamba control role 與私有 env。
每次先取得 repeatable-read exported snapshot，在相同 snapshot 中計算各使用者表
欄位名稱、型別、NOT NULL／default 及按 C collation 排序的 JSONB rows SHA，並
透過 server cursor 串流列值。UTC 排除 session timezone 差異；`pg_dump --snapshot`
使用同一 snapshot。archive、state、receipt 保持私有，保留不同版本。

恢復工具 `scripts/verify_control_plane_offhost_restore.py` 使用既有 trusted SSH，
只交付 hash-bound 的恢復程式及私有備份。獨立 PostgreSQL 18 由 Mamba 建立，
原 remote GPU Python 與 manager base 不更新。新 cluster 使用既有非 root 身分，
socket 0700、沒有 TCP listener、fsync 開啟；只啟停本次 cluster，不動正式服務。
還原用的登入角色為本次獨立建立，沒有傳送原 role password。

先獨立暖機求解／下載，再對 declaration 與 explicit 各測三次完整 fresh-role
建立，只有全部 Conda package builds 相同才比較。還原並行度按 1／2／4 workers
交錯測三輪，每次建立新 DB、完整 `pg_restore`、逐表核對原 state，最後移除本次
測試 DB，停止自己的 cluster，保留 receipts／備份及失敗 logs。不同的 cluster
名稱與 PID 才能建立離機恢復證據，不能用原 DB 的健康檢查代替。

曾遇到 private parent directory 阻止非 root 執行 PG 的試驗失敗。改用獨立的
`/opt/stockagent-control-recovery-*` binary parent 與
`/var/lib/stockagent-control-recovery-*` private state；不放寬既有 `/opt/stockagent`
的權限。原失敗收據保留，新成功驗收才可 supersede。

## 長連線版本准入

上輪工作流程明確記錄只有連線啟動准入，長連線沒有每次操作鎖定。現行
`ControlStore` 增加共用 transaction guard，所有 work／node 操作及 snapshot 都
先 SHARE lock `contract_version`，核對唯一受支援版本，再操作工作表。鎖與其他
正常 worker 相容，metadata 更新／刪除／DDL 必須等待此操作結束；下一次操作
重新檢查。snapshot 保留 repeatable-read／read-only，在第一個 SELECT 前取鎖。
initialize 遇到有工作表但 metadata 遺失或空版本也拒絕，不重新掛上版本認證。

真實 PG 測試對既有連線、九個 public operations、版本清空／未知版本／版本表
刪除做 27 組驗收，確認拒絕後 jobs／nodes 不變。另一組實際並行測試由
`pg_locks` 證明 metadata writer 真的等待操作結束，提交新版本後舊連線即拒絕。
含原 lease／並行 claim／snapshot tests 及 backup snapshot tests 共 **46 passed**。
第一版測試把原 priority 0 誤寫為 50 的 failed receipt 保留；修正期望值後重跑。
不改 stored schema 1、work identity、資料 ABI 或已開始工作的實體副作用；遷移
仍需原 owner 的 drain／部署程序。canonical schema 本次仍沒有正式 jobs。

## 正式採用仍依工作需求

本次檢查 canonical control schema 的 nodes、jobs、dependencies、attempts 都為
空；先前跨節點工作存在隔離試驗 schemas。DuckLake／PyIceberg 呼叫在 lakehouse
trial，Temporal 呼叫在試驗 server／worker；沒有把正式 collectors／GPU／盤中
ledger 接入。現行資料發布由原 owner、source lease、immutable release 與 READY
契約負責，多讀者不等於同表多 writer。

因此現在保留 Parquet／Arrow／Polars／DuckDB 的已實測分工；PostgreSQL 為已準備的
工程控制角色。確實需要同一表跨機提交時，再把那張表的 owner、incremental
commit、還原和消費者驗收一起遷入 DuckLake；需要 Spark／Flink／Trino 互通才
增加 Iceberg 的實際引擎測試；需要原流程跨多階段持久恢復才設計 Temporal
production namespace／persistence／activity idempotency。正式節點程序仍由原
systemd units 管理。這些採用條件不是全 fleet 完成宣稱。

Vast 當前 workspace 沒有持久 volume。需要可存放私有控制資料、容量合適的
持久離機目的地，才能再驗傳輸後 durability／保存政策及節點故障恢復。此處
不得將一次離機 restore、磁碟 mount 或 HTTP 200 當作 HA／災難恢復完備。

使用者在這次量測當時尚未提供現成目的地；沒有建立付費資源。
後續已提供 lab203／QNAP NAS，並轉交首批約 63 MB 固定資料的加密備份、
完整 NAS 還原與排程恢復通過摘要。最新範圍與固定 snapshot 見
[NAS 首批驗收](two_node_nas_backup_2026-10-03.md)。這不是控制 DB 在 NAS 上
的 logical restore、全歷史備份或金鑰異機保管完成；現有 private backup
與可重跑的還原工具仍保留。

證據根目錄：`artifacts/operations/architecture-unresolved-repair-20261003/`。
最終正式資料量、audit 結果、各方法完整 timings 與來源／程式 SHA 由本輪
acceptance 及下列後續驗收段落記錄。

## 已完成的離機恢復與實測

本次 backup **240,523 bytes**，綁定 **50 張使用者表、60 rows、10 個 schemas**。
同一快照 state identity 為
`85b75c8b6d15de5bd00f9eb8871eb065d9dd3802206e08864c0b745eae76065f`。
九次遠端完整還原的 table names、column types／defaults／NOT NULL、全部 rows SHA
與此 state 完全相同。這包括試驗 schemas 中保留的工作／attempt 歷史，不代表
正式 fleet 的工作已移入 control DB。測試 cluster 均已停止，原正式 PG PID
與 gateway PID 保持不變。

| 完整工作量 | 候選 | 三次均值 |
| --- | --- | ---: |
| 獨立 Mamba role 建立及套件 inventory／native 核對 | YAML declaration | 4.097018 s |
| 同一組 48 個 Conda builds 的獨立 role 建立及核對 | explicit lock | **3.006771 s** |
| 建立新 DB、restore、逐表核對 snapshot | 1 worker | 0.227655 s |
| 同上 | 2 workers | 0.226053 s |
| 同上 | 4 workers | **0.167768 s** |

Explicit 的觀測完整建置時間少約 **26.6%**；4-worker 還原比 1-worker 少約
**26.3%**。這是當前小型 control backup／可見約 53.76 CPU quota 的節點結果，
不外推成大型來源資料庫、GPU 工作或其他節點的參數。套件下載／求解暖機
另列，不混進這六個 warm fresh-role 比較；DB teardown 為測試清理，不納入
使用者恢復完成的時間。保留各次樣本與完整 logs。

恢復角色是 Python **3.12.14**、psycopg **3.3.4**、PostgreSQL **18.6**，exact builds
保存於 `configs/environments/locks/control-recovery-linux-64-20261003.explicit.txt`，
SHA 為 `e1a5099642fdeeef48b42c92a895eb1b08054c9c2ad1c5adc8bf6d4bd729d9c7`。
原 remote runtime 的 before／after 指紋相同；manager base 沒有更新。
保留的測試角色使本次 remote free bytes 由約 17.9 GB 降為約 10.7 GB，沒有
刪除既有資料或測試證據；目前不是為 CUDA native 角色做整體遷移的容量驗收。

相關 regression **160 passed**，含分鐘 scanner／單位／來源 audit／reuse 與既有
control／環境契約；真實 PostgreSQL snapshot／concurrency 測試 **18 passed**，
另有 recovery admission **5 passed**。早期缺測試依賴／沒有選入 DB 的啟動結果及
兩次 private-parent PG 啟動失敗均保留；`3 skipped` 不計為 DB 驗收。
新增版本 transaction guard 後，控制／installer／agent workflow／runtime／恢復准入
共享回歸另有 **127 passed**；正式 role 實際載入新 module，canonical counts 仍為零，
完整使用者表 state identity 仍與九次離機還原相同。

前段曾對同一個測試彙總檔追加內容，造成舊 failure-resolution 的 path 所指內容
與當時 SHA 不同。保留原紀錄，按既有 SHA／size 恢復完全相同的三版 bytes 到
不同檔名，見 [測試收據版本](../artifacts/operations/architecture-unresolved-repair-20261003/test-evidence-revisions.json)。
本輪結案使用各 run 名稱的最新成功與新的獨立驗收，不依賴那些指向可變彙總的
舊 failure-resolution。後續新增結果使用新檔，避免再修改已綁定證據。

## 正式分鐘資料驗收

正式既有服務從 2020-03-02 建至 **2026-10-02**，輸出 **1,606 個交易日分區、
313,882,233 rows、2,630 個有來源商品**。全分區 audit 已通過且 SHA 綁定新的
manifest，`source_reader_contract` 為本輪新契約；來源 catalog fingerprint 在
建置後再次相同。不是只以最新日期的一份 audit 或服務 active 判定全量完成。

source history audit 為 `classified_with_limits`，目前選定 universe **2,757** 個
股票／ETF，其中 complete **2,541**、known-source-gap **89**、contract-unavailable
**127**，failed／partial 為零。`selected_coverage_complete=false` 保留；這是當前
登錄 universe，沒有宣稱全部曾上市商品或逐分鐘成交全集。官方非交易日的
391 rows 保留原來源並記錄於 quarantine，不移到其他交易日。

完整 minute research 的證據見
[minute acceptance](../artifacts/operations/architecture-unresolved-repair-20261003/minute-research-acceptance.json)。
daily／hybrid 已沿用原 canonical local-only／builder／audit 入口與同一把
`runner.lock` 完成重建及獨立驗收，見下節。

## 衍生表日曆准入補修

原服務在 15:52:05 完成完整工作流，舊 hybrid audit 回報 `ok`。交叉查來源
quarantine 卻發現 daily 仍保存非交易日：當前 2,339 個 complete 商品有 **286**
筆日資料，對應 **391** 筆分鐘，分別為 2020-09-27 的 1 筆和 2024-12-01 的
390 筆。掃整個 directory 的 287 筆另外包含未被本輪使用的舊檔；該舊檔沒有
進入 hybrid，不把它算入正式受影響資料，也不自行刪除。

新增薄層 `downloader/shioaji_daily_calendar.py`，沿用既有
`_validated_taiex_session_dates` 的官方 receipts／parquet SHA／coverage 檢查，沒有
新增抓取或以 Monday–Friday 替代日曆。local daily 的新契約為
`receipt_verified_taiex_sessions_quarantine_v1`；每份日摘要版本 3 保存官方 calendar
與確切 session-date SHA、按日期的 quarantine 原始分鐘數。hybrid summary／audit
版本 2 也保存日曆契約與 download-summary receipt；model panel 按新來源 bytes
重驗，不沿用舊資料指紋。

舊快取只有 output SHA、完整來源 chunk 身分、source-minute 總數與日 bars 總數、
symbol／market／name／coverage 全部一致才可直接隔離非交易日並升級；來源或
檔案改變便走原始來源驗證。已有新日曆卻發生歷史 session 變動時也需重建。
日曆只延伸新 sessions、且以前的 session-date SHA 相同，則仍可保留已驗 prefix，
避免每天因官方 calendar 檔增加新日期而重讀整段分鐘歷史。原始來源與缺口
分類保留，不重標日期、不補價、不改資料的歷史可用時間。

**91 passed** 包含非交易日隔離、正常日期 exact quotes／股數不變、原始 bytes
不變、升級與完整重建相同、來源／output／日曆變動拒絕、增量與 history audit
以及 local／API hybrid contracts。新版正式 audit 實際拒絕缺日曆契約的舊結果，
見 [拒絕證據](../artifacts/operations/architecture-unresolved-repair-20261003/legacy-calendar-admission-rejection.json)。
本次 calendar 修復只適用於正式使用的 `verified_local_minute`；舊 API／cache
模式沒有因此取得新日曆准入證明。

在原服務正常退出並釋放 `runner.lock` 後，由 agent workflow 持有同一把鎖執行
canonical local-only daily、hybrid builder 和完整 audit。2,339 份 daily 全部在
**47.605 s** 升級，full rebuild 為零、沿用 **168,238** 份已驗來源 chunk 身分，
隔離 391 筆原始分鐘，API requests 為零。這是 calendar-only 升級工作量；不能
與前段新增日期／修正來源的 445.053 s 直接當成相同工作量的效能比較。
含完整 hybrid 重建與全商品 audit 的本次 agent run 共 **383.907 s**。hybrid
**9,005,471 rows** 中，Shioaji **3,213,245**、public **5,792,226**；declared source-gap
public fallback **68** 仍保留。2,757 個商品的 daily／hybrid receipts 全部對帳，
source-minute summary checksum 與 **168,238** 個 source-chunk receipt 身分通過
既有 lineage audit；沒有將這次沿用收據稱為重新 hash 全部原始 bytes。
current admitted daily／Shioaji hybrid 的非交易日為零；quarantine 391 筆與分鐘
research 相同。新 receipt 的 universe complete 表示全部選定商品已分類，不能
取代 minute source 的 `selected_coverage_complete=false` 或交易全集證明。

最終證據：[daily／hybrid acceptance](../artifacts/operations/architecture-unresolved-repair-20261003/daily-hybrid-acceptance.json)、
[來源指紋及研究 reuse](../artifacts/operations/architecture-unresolved-repair-20261003/source-reuse-and-calendar-acceptance.json)、
[最新控制 state](../artifacts/operations/architecture-unresolved-repair-20261003/final-control-state.json)。
來源 catalog 仍為 **313,882,624** 原始分鐘，減去 quarantine 391 後等於研究表的
313,882,233；未變來源下一輪可通過 reuse，不必重建全部 1,606 個日期。
原 service 正常退出，NRestarts 在成功 cycle 仍為 312，沒有手動 restart；正式
PG／gateway PID 不變。下一個 backfill timer 為 2026-10-05 14:31，服務 `inactive`
是這次有界工作結束，未用 unit 狀態推定資料完整性。

本輪 [總驗收](../artifacts/operations/architecture-unresolved-repair-20261003/acceptance.json)
綁定新資料 receipts、離機還原、程式／文件 SHA、最新成功 runs 與剩餘條件。
該份總驗收當時尚無持久離機目的地；後續 NAS 首批檔案驗收已更新於上述
連結。控制 DB 的持久目的地驗收與正式 fleet migration 仍未完成，並非
整個目標架構已遷移的完成宣告。

實際收據：[離機恢復](../artifacts/operations/architecture-unresolved-repair-20261003/offhost-restore-v3/acceptance.json)、
[建置比較](../artifacts/operations/architecture-unresolved-repair-20261003/offhost-restore-v3/role-build-comparison.json)、
[九次還原比較](../artifacts/operations/architecture-unresolved-repair-20261003/offhost-restore-v3/restore-comparison.json)、
[測試 cluster 停止](../artifacts/operations/architecture-unresolved-repair-20261003/offhost-owned-cluster-stop-proof.json)。

工程規格：[PostgreSQL exported snapshot](https://www.postgresql.org/docs/18/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION)、
[pg_dump snapshot](https://www.postgresql.org/docs/18/app-pgdump.html)、
[pg_restore jobs](https://www.postgresql.org/docs/18/app-pgrestore.html)。
