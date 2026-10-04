# 架構技術實測與 Miniforge／Mamba 工作流程

本輪工作是 `architecture-tech-trials-20261003`。將前一輪僅列採用條件的
DuckLake、Iceberg、Temporal 補成實際引擎試驗，同時完善環境重建、日資料
查詢與前端契約。工程證據位於
`artifacts/operations/architecture-tech-trials-20261003/`。

## 第一性原理與採用界線

速度來自少讀資料、少搬位元組、少重做工作，以及依實際瓶頸分配資源。
環境、交易協調、欄式計算、程序管理與持久流程解決不同問題。引入一項技術
必須證明它能改善專案工作，且不改變原始值、可用時間、執行價格或重現身分。

| 技術 | 本輪實作與驗收 | 現行決策 |
| --- | --- | --- |
| Parquet／Arrow | 真實台股主表的完整列投影、年度分區、原始值／NULL／NaN 遮罩核對；重用 packed publication 與還原 | 維持儲存與型別交換責任；分區候選須按消費方式採用 |
| Polars | 相同來源、日期／字串商品篩選、輸出與驗證；在既有 `columnar_lake` 加入共同查詢入口 | 本輪持續查詢最快；保留既有特徵 ETL 分工 |
| DuckDB | 相同查詢流程；正式簽章的 DuckLake／Iceberg 擴充；以原生 `iceberg_scan` 讀取 | 適合 SQL 分析與 lakehouse 試驗；沒有更換訓練 panel backend |
| PostgreSQL | Mamba psycopg 角色與實際 concurrency／fencing／故障測試；兩節點、3 jobs／4 attempts、中斷接手與 exact restore | 控制工作身分、版本輸入、依賴與 attempts；不取代原始資料湖 |
| DuckLake | PostgreSQL catalog、兩個獨立程序的重疊 append、rollback、舊快照、catalog 與獨立資料副本還原 | 多 writer 共用同一分析表時已有可重跑候選；本輪未遷移正式資料表 |
| Iceberg | PyIceberg catalog、實際 optimistic commit retry、舊快照、增加 nullable 欄位、DuckDB 原生跨引擎讀取 | 有多引擎需求時評估；Spark／Flink／Trino 尚未實測 |
| Temporal | 真實持久 server／worker、活動重試、timer、worker SIGKILL、server SIGKILL 後 systemd 重啟與 replay | 跨階段等待與恢復已有實證；本輪為 SQLite dev server，未部署正式 HA |
| TypeScript | Python schema 產生型別與限制；實際瀏覽器驗證器接受未知輸入、通過後縮窄型別；接入正式資料監控頁 | 漸進擴大實際程式檢查，保留既有前端元件與唯讀邊界 |
| systemd | 正式 gateway 與 control backup 保留既有 owners；Temporal 試驗實際由 systemd 接手 server crash | 負責節點程序管理；不代替跨節點 work state |
| Miniforge／Mamba | 獨立 Conda 角色、套件 build／URL／hash 鎖檔、exact rebuild、native before／after | 新工程角色使用 Mamba；來源、實驗與 GPU 原生環境各自保留身分 |

## 實際資料與完整工作量

來源 `/srv/stockagent-live/data_tw_public/features/tw_public_stock_daily.parquet`
共 **9,616,198 列**。本輪讀取全部列的四個既有欄位：`date`、`symbol`、
`twpub_official_trading_volume_raw`、`twpub_official_trading_value_raw`。
這是具名服務資料的工程投影，不是新訓練特徵／標籤資料。

查詢條件為 2025-01-01 起的 `0050`、`2330`、`2317`、`2454`，得到
**1,696 列**。每個 candidate 都完成讀取、篩選、型別核對、排序、寫入、
重新讀取、原始值／缺值遮罩核對及 SHA-256；不是只計算 kernel 時間。
三引擎各交錯測量三次，使用相同的 4-thread 節點預算，沒有清空 OS page cache。

`analytical-layout-final/acceptance.json` 的完整流程平均秒數：

| 引擎 | 三次平均 | iteration 1、2 平均 |
| --- | ---: | ---: |
| Arrow | 0.173589 | 0.170761 |
| Polars | 0.140929 | 0.119310 |
| DuckDB | 0.234750 | 0.201863 |

所有結果 Parquet SHA 都是
`441a62abb3f58880d41ccc8a6f0b8dc96ab705e113c8126ffef722a474afefec`。
較早的冷／熱混合測量曾由 DuckDB 勝出，後續測量沒有穩定重現；不能把
第一個平均值當成整個專案的永久最快引擎。完整 futures epoch／fold、所有
features、cold I/O 與 US panel backend 不在這個查詢結果的證明範圍。

相同四欄的完整列資料，單檔 query/write/read/verify 平均 **0.095269 秒**，
年度分區平均 **0.039285 秒**，這個流程約 **2.43 倍**。
僅看 iteration 1、2，分別約 **0.047508 秒**／**0.036588 秒**，約 **1.30 倍**；
第一輪的 cache 差異使三次平均倍率較大，不能把 2.43 倍當作穩態保證。
單檔 104,800,702 bytes，年度分區 105,006,763 bytes；分區略增檔案大小。
不能據此宣稱所有日期篩選或全表掃描都會改善。

以實際最後十個來源日期重建增量，**28 個年度檔案中只改寫 2026 年的 1 個**，
保留其餘 27 個的完整 bytes/hash；packed objects 沿用 **8 個**、新增 **1 個**。
重複出版相同 inventory 是 semantic no-op。完整增量、出版、物件核對與
完整列還原核對共 **21.671092 秒**。
最新 packed objects 合計 **109,619,763 bytes**，新增物件僅
**4,874,907 bytes**，約占 4.45%。追加的 `post-fetch-acceptance.json` 使用
canonical verifier 核對已還原的全檔案 inventory，`materialized_verified=true`；
保留原先在 fetch 前取得的 cold proof，不回改其 `false` 觀測。
這是有界離線試驗，私有 cold／materialized 位於
`/var/lib/stockagent/architecture-trials/analytical-layout-final/`；
未寫入 fleet cold store，也沒有 Syncthing convergence 或正式資料遷移宣稱。

## Lakehouse 與持久工作試驗

TAIEX 來源是 `twse_taiex_ohlc.parquet` 的 **6,875 筆**實際歷史觀測。
保留全部欄位與來源資訊，以既有歷史列的 base／兩份各 128 列尾段測試 append，
沒有合成市場資料。輸入在試驗前後核對 SHA。

DuckLake 的兩個獨立 OS processes 在相同 base 後建立重疊 transactions，
確認各自完成 COMMIT、無缺列／重複值、舊 snapshot 可讀與 rollback 有效。
`pg_dump`／`pg_restore` 還原到獨立 UUID DB，同時複製全部 Parquet files；
改指向這份資料副本後，最新與舊 snapshot 均逐欄一致。
測試發現 `--no-owner` 還原須明確使用應用角色，否則 catalog 會因 owner
不符而不可讀；程式已修正。最後只移除自己的試驗 DB，保留 dump、data 與 receipts。

Iceberg 的兩個 clients 從相同 metadata 建立 transactions；實際記錄一次
SDK 自動 conflict retry，兩份尾段均完整提交。增加 nullable 欄位沒有改變
原始觀測；DuckDB `iceberg_scan` 直接讀取 Iceberg metadata／data，並驗證
新、舊快照。沒有以 Arrow materialization 冒充另一個原生引擎。
SQLite catalog reopen 是本輪範圍，尚無 remote catalog／Spark／Flink／Trino／HA proof。

Temporal 使用 pinned CLI **1.9.1**、server **1.32.0**、Python SDK **1.34.0**。
activity 重用 `verify_source_release`，驗證 exact source release
`714bd89eecf516a8d7474406e8a7ff8b82ebd0fd955d6362ec0a39104a0a7c8e`，
共 **1,093 個** recorded source files。一次 transient fault 在 handler 前注入；
成功後完成持久 timer，worker 被 SIGKILL，server 被 SIGKILL，systemd 實際
將 MainPID 換成另一個 PID、`NRestarts=1`。新 worker replay 後，原成功 activity
receipt/hash 沒有改變，收到 signal 後完成第二階段。
完整試驗 **24.072255 秒**、26 個 history events；最後停止自己的 server／worker。
這不證明券商副作用 exactly-once 或正式多機 HA。

## Mamba 環境與新入口

宣告位於 `configs/environments/control.yml`、`architecture-pilot.yml`。
角色 lock 位於 `configs/environments/locks/`，使用完整 public package URLs
與 SHA-256／MD5。鎖檔具平台身分，不能直接套用到別種 CPU／OS。
本機 Mamba **2.8.0**；試驗角色為 Python 3.12、DuckDB 1.5.6、Polars 1.38.1、
Arrow 25.0.0、psycopg 3.3.4、PyIceberg 0.12.0、Temporal SDK 1.34.0。
psycopg 3.3.6 尚未在此次 conda-forge 求解中提供，故用 3.3.4 並驗證真實 PG 契約。

本機架構角色宣告建立 **39.211193 秒**，exact lock rebuild **15.526567 秒**；
兩者 167 個 Conda packages、65 個 Python distributions，0 個 pip overlays，
explicit SHA 相同，native runtime 不變。第一次建立含下載／cache warmup，
這兩個數字不能直接視為公平冷態速度倍率。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/manage_runtime_environments.py rebuild \
  --prefix /var/lib/stockagent/environments/NEW_ROLE \
  --specification configs/environments/locks/architecture-pilot-linux-64-20261003.explicit.txt \
  --output artifacts/operations/NEW_ROLE_SETUP
```

`create` 接受 YAML、`rebuild` 接受 explicit lock、`capture` 記錄已存在環境。
每次建立新的 prefix／receipt，不覆寫 native 或未知環境。原生 capture 若有 pip
overlays，只是庫存記錄，不宣稱 compiled CUDA、pip binary 或完整 GPU 訓練已重建。

跨節點 pilot 須傳入 exact control role lock；若遠端確定缺 manager，使用
`--install-remote-miniforge` 建立新的獨立 Miniforge base，不做 shell init。
遠端原生 GPU 路徑當次為 `/venv/fintech`；新 control role 與它分開。

遠端安裝官方 hash-pinned Miniforge **26.7.2-0** 到新的
`/opt/stockagent/miniforge3`，Mamba **2.9.0**。同一個 2 MiB 物件片段的
1／4／8／16 連線探測分別為 **23.136／12.141／9.845／5.112 秒**，選擇 16；
完整 124,514,161-byte 安裝器下載含探測／完整 SHA 驗證共 **281.745 秒**，
加上安裝／版本確認共 **286.572 秒**。此連線數只適用於當次節點與軟體下載，
不套用到有配額的市場 provider，也不宣稱 2 MiB 探測等於完整下載吞吐。

同節點三次交錯建立 control role，**40 個 Conda packages 的 name／version／build
完全相同**；YAML declaration 平均 **5.103118 秒**，explicit lock 平均
**2.950778 秒**。第一份 YAML 建置含 cache warmup；這是當次整體流程比較，
不能當成公平 cold-cache 求解倍率。選擇 explicit 後，以該角色執行實際遠端工作。
本機與遠端完成 **3 jobs／4 attempts**，worker 在 handler 前中斷、lease expiry
後成功接手；15,058-byte schema dump 的獨立 DB 還原狀態完全一致。
完整 pilot **328.850845 秒**，原 native fingerprints、frozen source 與 delivered
controller 均未變動，最後移除 tunnel／remote private DSN，保留可查核 receipts。

Linux x86_64 的 `install_control_plane.py` 已預設使用驗收過的 explicit lock。
正式本機角色 `mamba-locked-20261003` 已透過 `--activate-role-only` 啟用；
切換前後 canonical DB state 相同、private env 仍為 0600。
正式 backup service 已使用這個 Mamba interpreter 寫出 **240,523-byte** logical
archive，runtime fingerprint 與角色一致，archive SHA 核對通過。
PostgreSQL MainPID **1342234**、`NRestarts=0`，本輪切換沒有重啟 DB。
全 DB 備份本次只驗 archive；exact restore 是另一份 pilot schema 的實測，
仍沒有 off-host disaster-recovery／HA 或遠端磁碟持久性證明。

可重跑入口：

```bash
# 使用明確選定的 Mamba architecture role 後 source runtime_env.sh
run_fintech_python scripts/verify_lakehouse_engines.py \
  --source /srv/stockagent-live/data_tw_public/twse_taiex_ohlc.parquet \
  --output artifacts/operations/NEW_LAKEHOUSE_TRIAL \
  --extensions artifacts/operations/NEW_DUCKDB_EXTENSIONS
run_fintech_python scripts/benchmark_analytical_layouts.py \
  --source /srv/stockagent-live/data_tw_public/features/tw_public_stock_daily.parquet \
  --output artifacts/operations/NEW_QUERY_TRIAL \
  --scratch-root /var/lib/stockagent/architecture-trials/NEW_QUERY_TRIAL
```

已有 `stockagent.data.columnar_lake.read_daily_projection` 支援 Arrow／Polars／DuckDB，
按欄位、日期與字串商品識別 push down。DuckDB values 以 parameters 綁定，
source 日期必須本來就是 Arrow date，拒絕將文字或 timestamp 偷換為日觀測。
Arrow／Polars 的 thread pools 在節點工作入口設定；DuckDB thread／memory 是明確
的 engine-specific 參數。沒有建立另一套 panel 或 source fetch framework。

## 驗收與尚未宣稱的範圍

本輪 focused contracts **82 passed**；columnar／gateway／packed／Syncthing／
control regression **221 passed**；後續 runtime／query／new static routes
**20 passed**；Mamba role 的 actual PostgreSQL integration **15 passed**。
TypeScript strict check 與 generated-contract drift check 通過，Node contracts
**15 passed**；工具 Range assembly／corrupt hash／existing owner **3 passed**。
最後新增的角色啟用／平台選擇與 technology boundary regression **47 passed**。
完整測試套件／所有商品 GPU fold 沒有在本輪重跑。

新的 shared feature-page validator 接入公開資料監控頁；Python／JS 同時驗證
readonly flags、schema、safe pagination、revision、reset、完整頁列數與 continuation。
保留 row metadata、前導零商品字串與未知來源值；沒有更改抓取配額與 execution。
正式 gateway 只重啟 `stockagent-public-dashboards.service`，新 MainPID **1738811**。
實際 localhost／公開 HTTPS 的兩支 JS bytes 都與 repository SHA 相同，
Python 與 served JS 對同一個 80-row API page 均驗證通過；revision 相同，
當次 `matching_total=88,752`。這是資料契約與實際 HTTP consumption 驗收，
不是資料新鮮度、完整 browser interaction 或所有 providers 的健康宣稱。

23 個受保護服務的前後比較中，22 個 PID／restart fields 相同；
`stockagent-shioaji-minute-backfill.service` 自行發生失敗／systemd restart。
實際 journal 的既有錯誤是 `source_volume_multiplier` 在混合 Parquet files 中
造成 Polars `SchemaError`，不是本輪發出的 restart，也沒有被本輪修復。
這個資料 schema 缺口獨立記錄，不以新架構 pilot 成功掩蓋正式回填的失敗。

試驗失敗、安裝傳輸逾時與早期格式錯誤的原 receipts/logs 都保留，後續成功只能
作 superseding proof。舊目標報告的 uv／venv、版本與數字是那次的歷史證據；
這份 Mamba 工作流程不回改它們。新正式採用需要原 owner 的版本、ABI、來源
及完整工作量驗收；本輪沒有把所有 collectors、GPU jobs、盤中 ledger 搬進新引擎。

主要工程證據：[資料布局](../artifacts/operations/architecture-tech-trials-20261003/analytical-layout-final/acceptance.json)、
[DuckLake／Iceberg](../artifacts/operations/architecture-tech-trials-20261003/lakehouse-final/acceptance.json)、
[Temporal](../artifacts/operations/architecture-tech-trials-20261003/temporal-v1/acceptance.json)、
[跨節點控制](../artifacts/operations/architecture-tech-trials-20261003/control-pilot-mamba-final/acceptance.json)、
[Mamba 建置比較](../artifacts/operations/architecture-tech-trials-20261003/control-pilot-mamba-final/remote-mamba-build-comparison.json)、
[正式角色啟用](../artifacts/operations/architecture-tech-trials-20261003/mamba-control-locked/control-role-activation.json)、
[公開契約部署](../artifacts/operations/architecture-tech-trials-20261003/dashboard-contract-deployment/acceptance.json)。
最終證據與實作檔案身分彙整於
[本輪 acceptance](../artifacts/operations/architecture-tech-trials-20261003/acceptance.json)。

主要規格來源：[Miniforge](https://github.com/conda-forge/miniforge)、
[Mamba explicit specifications](https://mamba.readthedocs.io/en/latest/user_guide/mamba.html)、
[DuckLake transactions](https://ducklake.select/docs/stable/duckdb/advanced_features/transactions)、
[DuckLake backups](https://ducklake.select/docs/stable/duckdb/guides/backups_and_recovery)、
[PyIceberg API](https://py.iceberg.apache.org/api/)、
[DuckDB native Iceberg](https://duckdb.org/docs/stable/core_extensions/iceberg/overview)、
[Temporal dev-server persistence](https://docs.temporal.io/cli/command-reference/server)。
