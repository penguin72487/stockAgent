# 四節點正式架構：採用方案與服務驗收

後續使用者明確選擇 DuckLake／Temporal／rclone，已另行部署。現行方案與
驗收見[最新部署紀錄](ducklake_temporal_replication_2026-10-05.md)。本文保留
前階段取捨及其證據，不代表最新服務尚未採用這些技術。

2026-10-05 使用者要求重新分析技術、先完成規劃，再實作測試直到設計採用的
正式服務運行。工程 task：`architecture-production-completion-20261005`。
skill 的結構／可被偵測驗收已完成，不能代替本次服務驗收。

## 從不可替代資料與寫入責任推導

1. 不可重新取得的是來源觀察、修訂／時鐘／授權證據、權威 execution ledger、
   已驗證結果與恢復金鑰。這些需要固定身分、單一權威與離機恢復。
2. 訓練 tensor／panel／split／compiler cache 是有界計算工作集，由 Vast 的
   exact source、Git/config/ABI/runtime 重建。傳輸／備份不等於可以下單或刪原值。
3. penguin 是唯一正式來源 publisher，lab203 只有備份中繼，NAS 保存 encrypted
   snapshots，Vast 讀取當期 source 並返回完成結果。四個角色沒有共同修改同一張
   分析表的需求；mutable DB 不能經 Syncthing 當作多機交易協調。
4. 需要共享的工程狀態是版本、依賴、claim／attempt／lease 與可查證的結果。
   PostgreSQL 已備妥但 canonical jobs 為空，這一層仍是 pilot，應把有固定、
   可重試、無交易副作用的 code admission 接上正式 owner，再逐項接其他 workflow。
   source collector、GPU manager、下單 ledger、packed writer 的 physical owner 保留。
5. 日常恢復由固定程序／timer、durable journal／outbox、受限重試與 receipt 接續。
   Codex/tmux 用於工程接手。只有真正增加持久工作語意才值得引進另一套 workflow server。

## 正式技術選擇

| 責任 | 本次採用 | 部署方式／驗收 |
| --- | --- | --- |
| 熱來源／服務／可變狀態 | WSL native ext4、既有 canonical collectors、SQLite WAL／PostgreSQL | source／service consumer 守門；保留原 provider／交易 owner |
| 有型別資料與遠端 ETL | Parquet／Arrow／Polars／DuckDB | 固定 source release、在 Vast 建置；按 workload 實測 CPU／NUMA／cgroup／RAM、驗完整輸出 |
| D HDD 冷資料 | 既有 NTFS、guarded 8 KiB 9p、native byte adapter、immutable CAS／bucket packs | 同一 authority mount namespace；完整 SHA／原值重建，增量只送變更 objects |
| 傳輸 | 既有 Syncthing sealed ingress／反向 receipts；完成 run 用既有 private SSH／rsync | pair／idle／zero need/errors／exact delivery，各 folder 證據分開 |
| NAS 災難恢復 | 既有 SMB3／加密 Restic、同一 lab203 owner／pipeline／semantic hook | fixed snapshot full restore／SHA；packed 與 PG logical recovery 獨立驗收 |
| 共享工程工作／版本 | 既有 PostgreSQL `stockagent_control`、immutable WorkSpec、SKIP LOCKED／lease fencing | 正式固定 code-release 驗證 queue／service，重用 canonical verifier；lease／restart／精確 scope 測試 |
| API／前端契約 | 現有 typed DTO／TypeScript、唯讀 gateway | 繼續原正式服務與型別檢查；不增加可交易的公開入口 |
| 環境 | Miniforge／Mamba、角色 explicit locks／runtime lock | 留住 CUDA／fintech、control／backup 角色隔離；不用 host RAM／cores 猜預算 |
| 節點監督／恢復 | systemd／WSL holder；Vast Supervisor／cron | required owners 正確、timer enabled／active、常駐 daemon running、fresh receipts／業務界線 |

DuckLake 解決多 client／多 writer 共用分析表的 transactional catalog；Iceberg
主要提供多引擎表格式互通。它們是相應需求的替代選項，沒有同時接管本案 D
不可變封存的必要。此輪正式採用欄不包含二者；既有真實引擎試驗保留，未部署
正式湖表不能寫成「已遷移」。未來新增同表多 writer 才用同工作量及恢復成本重評。
[DuckLake 官方 catalog 選擇](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database)
要求 remote multi-user 場景採用 PostgreSQL catalog。

Temporal 的持久跨階段 workflow 有價值，但正式 server 還需要 persistence、
namespace、TLS／權限、監控、版本相容及活動 idempotency。現在的固定 backup／
return 工作已有 durable queue／journal／ACK／bounded retry；第一步應補正式 PG
工程工作與觀測缺口。本輪不把 SQLite dev server 改名為 production，也不增加
尚無整段效益證據的 server。[Temporal 正式自架指南](https://docs.temporal.io/self-hosted-guide)
說明所需運維責任。

## 開發前基線與實際缺口

已查到 PostgreSQL 18/main、public gateway、source backup 和相關 timers 運行，
本次 boot audit 回報 `local_infrastructure_ready=true`、required owners 無缺項。
控制庫 nodes／jobs／attempts 為空，因此不能稱共享 work queue 已正式接管。

Syncthing cold-index 當前為 idle／100%／zero needs/errors。來源 status 的觀測時間
為 `2026-10-05T00:41:08.683920+00:00`，NAS file coverage 為
586,289,837,769／740,338,336,459 bytes，三批約 21.07 GB pending，receipt errors
為空；這是當次 status 的部分覆蓋，不是本輪完成全歷史。lab203 的控制只能透過
既有回傳 machine receipts，沒有 SSH。

IDE shell 的 `stockagent-data automation-status` 回報 cold guard failed，但同時
PID 1 namespace 的 boot audit mount guards 通過。需修正這個查詢入口的 namespace
一致性，不能讓 agent 用假的缺盤狀態規劃、也不能直接免除 cold guard。

## 實作順序與驗收範圍

1. 修正 authority 的唯讀 status／automation 查詢，使整段觀測在正式 namespace；
   Vast edge 和實際 guard 仍保留。擴充既有 automation 觀測至 backup／control owners，
   不增加第二個冷庫 writer。
2. 沿用現有 `verify-code-release` WorkSpec／ControlStore／verifier，建立正式固定
   code admission queue 和受限巡檢 service。claim 綁定 exact work key，避免某個
   frozen release worker 取得另一版本後錯誤耗用 attempts。只驗允許的固定版本，
   不接受 arbitrary shell command，也不接管 provider／GPU／券商副作用。
3. 將本次正式 code release 固定、保留 bundle／manifest／runtime 與驗收 receipts，
   透過同一控制 owner 執行真正的工作。完成後留 DB attempts 與結果；重啟不重送
   已成功工作。新增 code release 使用同一 enrollment／queue 入口，不靠手動 SQL。
4. focused semantic regression、actual PostgreSQL integration、正式完整 code 驗證、
   lease／scope／restart 接手與當前 node hardware／service evidence。新控制服務
   列入 boot/readiness 觀測，沿原 control slice，避免干擾開盤／收集／現有訓練。
5. 完整列出本次採用的每個正式 daemon／timer／worker 與驗收狀態。one-shot
   service 成功後 inactive 與 timer waiting 是正常狀態，不能為使每個 unit 顯示
   running 而掛假常駐程序。確認自動 backup／return／retry 持續進展，更新 skill。

完成條件是以上正式採用服務都有對應配置、有效 supervisor、正確 scope 與當前
驗收收據。全歷史 NAS 覆蓋、尚有真實使用／來源穩定阻礙的遠端根、整機 cold reboot
及硬碟格式遷移是獨立範圍，不拿 service active 填上完成旗標。

後續實作／結果及精確收據追加於本文件，不能改寫上述開發前基線。

## 已實作與現場驗收

### 核心修正

- penguin 的 `stockagent-data status`／`automation-status` 在 canonical wrapper
  整段進入 PID 1 mount namespace。原 IDE 的 `guard-failed` 已變為真實
  `guard-passed`；不豁免 marker／mount／runtime guard，不影響 Vast edge routing。
- 既有 automation view 加入 source NAS backup、control backup 和固定 code
  verification 的原 owner／白名單收據，不增加冷庫 publisher 或清理程序。
- `ControlStore.claim` 增加有界 exact work-key scope；舊 pool 行為保留。正式
  code queue 重用 immutable WorkSpec、canonical verifier、PG attempt／lease，
  只處理 private registry 已登錄的版本。完整 code bundle 與 source bytes 先驗，
  成功版本每輪重驗當前 bytes；不同版本不能互相消耗 attempt。
- 部署 `stockagent-control-release-verification.service`／`.timer`，使用已有
  Miniforge／Mamba control role 和 accepted runtime lock。單一 owner、CPU quota
  1 core／memory high 512 MiB、timeout 180 秒；每輪至多 8 個工作。未重啟 DB、
  未改 CUDA role 或已有 GPU 工作。退出 75 是 deferred，不是驗證成功。
- 開機 observer 新增這個已選定角色的 semantic readiness：timer 正常、最近
  service 退出 0、五分鐘內的收據，以及正數且完整成功的 registered releases。
  empty registry、future／stale receipt、上次失敗或 75 都不算 ready。
- 既有 Vast bulk 回傳 timer 缺少 completion-relative 排程的獨立 restart anchor；
  installer 只准入已知舊 timer（保留檔尾空白），新增 `OnActiveSec=30s`，只重啟
  timer，保留 service、journal、physical owner 與所有資料回收 gates。未知修改拒絕。

### 真正的正式工作與恢復

兩個固定版本已登錄並在正式控制庫成功。最終版本的 code SHA：

```text
50f2f33e6bb5cb3ca9ebf7ef4a4625da03aa4bb040a27620c7874f1d41d625e0
```

receipt 路徑記錄在 `release-build-final.json`；worker 的 exact input 是 receipt
完整 SHA，不追蹤 moving checkout。最終版本首次完整工作巡檢為 **1.374 秒**，
包含本輪新 claim、完整 source 驗證、PG finish 與兩份已登錄 source 的當前驗證。
此數值是當次現場耗時，不是與舊系統比較的 throughput 提升。

timer 實際重啟後自行巡檢，兩個工作仍成功，attempts 維持 **2 → 2**、紀錄相同；
不重送已成功工作。新 control logical backup 於
`2026-10-05T01:46:04.054142+00:00` 產生，archive 可讀與同一 MVCC state 指紋
均通過。這次 dump 尚不能沿用舊 NAS logical restore 的證據，離機傳送與新
identity 的還原由現有 auxiliary／recovery queue 自動接續。

日常建立／登錄／查詢／安裝見[Control workflow](control_plane_workflow.md)。

### 選定正式服務矩陣

| 節點／責任 | 現場狀態與證據 | 範圍 |
| --- | --- | --- |
| penguin 必要 source／web／DB／同步／mount owners | boot observer 的 16 個必要 owners 均 active；local infrastructure ready、無 timer schedule findings | 維持既有 provider 與交易安全界線；active 不證明市場資料完整或 broker fill |
| penguin 儲存／控制排程 | automation 列出的 12 組 timer 均 enabled／active；含新 verifier、backup、return、scan、GC | one-shot 正常完成後 inactive，timer waiting；bounded deferred 繼續按原政策重試 |
| penguin ↔ Vast cold index | paired、idle、100%、所有 need／errors 為零，TLS／QUIC | index-only demand flow；不自動 hydrate 全庫 |
| Vast 正式節點監督 | Syncthing／cron／Caddy／portal／TensorBoard／tunnel 的 Supervisor 狀態 RUNNING；GC／storage-pressure cron configured／active | 已結束或未啟用的歷史訓練 unit 不重新啟動 |
| lab203 NAS owner | paired `pipeline-status` 為 v10／active；四批、backup 2／restore 2／verify 4；fresh readiness 驗 owner／NAS mount／runtime | 現場證據 `2026-10-05T01:42:40Z`，沒有 lab203 SSH |
| penguin ↔ lab203 payload／反向 ACK | 兩個 folder 配對正確、連線、zero errors；接收新批次時 scanning／syncing 有實際 needs | ongoing 傳輸，不把 busy 或 99.7% 說成 fully converged |
| NAS fixed snapshot／語義恢復 | 現有 CFTC／TAIFEX packed、既有 control logical identity 的獨立收據仍有效；新 identity 另排任務 | 不能推定每個全歷史 release 或新 dump 皆已還原 |

Vast 最新 live 觀測 `2026-10-05T01:46:31Z` 的 CPU capacity 是 **53.75999**、
worker budget **53**、memory headroom **38,294,437,888 bytes**；較早同輪觀測
只有約 11.59 GB。兩次都以 visible cgroup／MemAvailable 取保守值，說明 RAM
和工作負載會改變，不能沿用 host 的 224 threads／全部 RAM。既有五個 train-related
程序含 DDP rank，不宣稱五個獨立訓練 job；本輪未啟動或停止 GPU 訓練。

### 測試與收據

| 檢查 | 結果 |
| --- | --- |
| 控制、runtime identity、namespace／automation、boot semantic readiness、timer／installer | `production-focused-final.xml`：**100 passed，0 skipped** |
| 真正 PostgreSQL 版本、並發／lease fencing、exact claim scope、重啟、backup／固定 code queue | `postgres-regression.xml`：**56 passed，0 skipped**，disposable schemas；正式 Mamba 環境套件不變 |
| Python API contract | `api-contract-regression.xml`：**12 passed，0 skipped** |
| 前端契約 | `node-contract-regression.log`：**15 passed，0 skipped** |
| TypeScript／產生契約 | strict check 和 generator `--check` 通過 |
| 語法／差異 | 受影響 Python compile、Bash syntax、scoped diff whitespace 通過 |
| skill | quick validation、4 個相對連結／36 個 repo path、實際 `skills/list(forceReload=true)` 唯一 enabled repo skill 通過 |

完整本輪證據在
[`artifacts/operations/architecture-production-completion-20261005`](../artifacts/operations/architecture-production-completion-20261005/)，
入口為 `production-acceptance.json`。首次失敗測試／不同候選 code release 保留，
沒有把它們改寫成 accepted。原 skill 驗收保留，更新版另有 `skill-acceptance-final.json`。

操作 skill 位於
[`stockagent-storage-operations`](../.agents/skills/stockagent-storage-operations/SKILL.md)，
其他 agent 在此 checkout 使用 `$stockagent-storage-operations`；其 discovery
是文件驗收，與上述正式 service／資料／runtime receipts 分開。

## 持續工作與獨立範圍

最新已完成 source cycle 的觀測 `2026-10-05T01:23:34.930259+00:00` 為
**614,374,847,982／740,338,336,459 bytes**、**20,027／20,776 檔** NAS 覆蓋，
三個 pending delivery 共 21,052,747,492 bytes，receipt／readiness／auxiliary
errors 皆空。其後 source owner 仍執行、receiver heartbeat 更新、payload 與 ACK
正在同步。這是自動補齊進度，保留 status 的原觀測時間，不拿它當即時總量。

本輪完成的是技術採用、缺口修正、正式 code queue 與選定服務運行／排程驗收。
全歷史 NAS 補齊、新 dump 的 NAS logical identity 接收與還原、尚被真實程序引用
的遠端來源回收，繼續由現有 owner 自動處理。D 實際 filesystem 格式遷移和
整台 Windows cold reboot 耗時未在本輪執行；它們各需實體條件與獨立驗收。
USB 保管已確認，Windows 登出驗收已取消，均不重問。

## 補齊 repo skill 備份與最終版本

收尾清冊發現 auxiliary 只列出 docs／configs，漏掉 `.agents/skills`。
已在原 `working_tree_inventory` 擴充 documentation scope；完整公開 skill 的
SKILL／UI／references／helper 一起入庫，設定檔仍核對 private 路徑與實值，
`.env`／Git-ignored 資料不納入。privacy filter 版本升為 **4**；現有 owner
下個 cycle 會保留並隔離舊 pending，再按新內容封閉發布。沒有改 envelope、
working-tree receipt 或 NAS worker 的 wire contract，不需另裝一個 receiver。

備份共享流程新增實測為 `skill-backup-regression-accepted.xml`：
**89 passed，0 skipped**，包含真實 Restic 加密、獨立 fixed snapshot 還原、
完整 SHA、跨批恢復與接手。第一次缺 Restic 測試工具的 13 skipped 紀錄保留，
使用已驗收 Mamba role 的 Restic binary 重跑後才採用這份 accepted 收據。
正式角色套件沒有新增 pytest 或其他工具。

亦以真實 working tree 與 `01:46:04Z` 的控制庫 dump 建立獨立本機加密 snapshot，
實際 read-data／還原後核對 **2,580 個 code 成員、5 個 skill 檔案**；PostgreSQL
獨立新 cluster 的 **50 張表／65 筆**欄位與 rows 指紋完全一致。首次選到 backup
role 的 libpq `pg_config`，該角色沒有 server binaries；已保留 snapshot／還原檔，
改用正式 PG unit 所指的實際 binaries，只接續驗證與 SQL restore，未重做整批。
接續驗證段為 **7.921 秒**，完整初始流程總耗時未作效能比較。僅停止自己的
socket-only 測試 cluster，沒有操作正式 DB；此為本機驗收，NAS provenance 分開。
證據為 `current-code-sql-skill-recovery/acceptance.json` 與 `first-attempt-failure.json`。

上述修正再固定成第三個正式 code release，SHA：

```text
42f580dcfce7b3b81c67929fa45dbdbd848b990537f9bec36b54357454afe312
```

`release-build-final-v2.json` 記錄固定 receipt／wheel。正式巡檢 **2.039 秒**完成
第三次 claim／finish 和三份 source 的目前 bytes 驗證；此為三份輸入的當次
完整工作量。三個固定 code 版本皆 succeeded，各一筆 attempt；新 dump 於
`2026-10-05T02:03:00.690816+00:00` 產生，新的 NAS identity 仍由原 pipeline 驗收。
凍結 code release 與 private binding 的原路徑恢復不屬於 Git working-tree
還原證明；災難恢復後按 control runbook 核對固定來源並重新登錄，不推定舊
artifact 目錄或任意版本已重建。

原先兩個版本與其 timer 重啟收據保留。最終新增的服務／排程、三版本重啟巡檢、
完整 skill 清冊與備份驗收入口為 **`production-acceptance-v2.json`**；
`production-acceptance.json` 保留較早階段的精確範圍，不覆寫舊收據。
