# 大檔資料發布與 Syncthing 同步設計

## 資料集相鄰封裝：E 候選，不等於 D 格式已遷移

`publish_packed_snapshot` 與資料目錄發布器支援選用
`pack_layout: source-dataset-v1`：以一個原來源資料集為單位，新檔按原
相對路徑排列，即使位於不同子目錄也可放在同一個新 ZIP。單次發布的
變更在容量與成員上限內形成一個增量包；較大的變更分成多個有界包，單一
來源檔在一個成員內保持完整。`source-directory-v1` 則可按第一層目錄
細分，適用已知經常只取用某個子目錄的來源。二者均不混合不同資料集；
原始名稱及位元組仍依 inventory 還原。`pack_target_bytes` 是原始邏輯
位元組目標，單一大成員、ZIP 標頭及壓縮結果可能超過目標。資料集整包的
候選預設值 4 GiB 是待量測的起點，不是實際 HDD 效能保證。
若希望小於目標大小的完整檔案一起進包，登錄該資料集時也須將
`loose_threshold_bytes` 明確設成適合的大小，例如等於封包目標；否則
超過原門檻的檔案仍會是獨立 blob 或 chunks，而不會自動變成同一個包。

`large_file_layout: whole-file-v1` 可讓新的大檔保留為完整 blob；預設仍
沿用既有 v2 chunks。兩者使用既有 ZIP／blob 與 v2 讀取契約，不替換或刪除
舊分塊。調高 `loose_threshold_bytes` 時，小於此門檻的完整檔案可直接存入
目錄分組 ZIP；大於門檻的檔案按選定的大檔策略處理。頻繁修改的大檔可能
因整檔重寫而顯著增加增量用量，應按資料集實測，不一律選整檔。

manifest 的 `new_pack_layout`、`new_pack_target_bytes` 和
`new_large_file_layout` 說明**後續新物件的寫入政策**；原樣重用的舊物件
可能仍是路徑雜湊包或分塊，不能僅憑這些欄位宣稱既有資料已完成重封裝。
政策切換會建立新 manifest，保留既有版本、完整還原、引用及 NAS 閘門。
舊工具可能忽略政策欄位，正式對任何資料集啟用前須先核對所有實際 writer
與 reader 的版本，並依資料集容量與使用限制由原 owner 登錄。

增量包服務當前資料；研究實際採用的版本需要固定原始 release、研究
設定、程式、runtime 和成果的可驗證關聯。既有研究配方的持久 `build`
在原 publication／retention 鎖下，將所用 release 的標準 pin、精確配方與
設定摘要登記至 retention 狀態目錄；建置收據再記錄關聯與 runtime。
此 pin 保護冷版本，不會因此永久固定 C 的已還原工作集，後者仍按租約
及活躍程序引用管理。試跑 `trial` 不建立研究 pin。失敗或中斷的持久建置
保留 pin 和失敗收據，待確認研究產物後再處理。這只涵蓋此配方持久建置
路徑，並未證明所有其他研究或歷史執行都已登記；不得把「未找到研究
使用」當作舊版本的刪除證明。版本保留策略也不能刪掉資料集內本身需要
的歷史記錄、公告時間或來源修正紀錄。

2026-11-06 固定 CFTC 歷史版本已在 E 建立、獨立還原和逐檔比對候選；未
更新 D 正式 head、未取得該候選的 NAS 配對驗收，也沒有授權回收舊版。
實測結果、原始版本及限制見
[來源目錄封裝與遷移進度](source_local_packing_migration_2026-11-06.md)。

## 結論

目前部署以 penguin 為唯一資料權威；檔案格式中的多產生者 head 是來源與版本資訊，
不代表多個權威庫。penguin 的冷庫已移至 D 槽單份儲存，
`/srv/stockagent-packed` 是 D 主庫的受保護 bind mount，C 不再保留冷副本。
這犧牲了本機獨立備份；日常操作見 [D 槽遷移與驗收](d_cold_store_migration_2026-09-25.md)。

不要讓 Syncthing 直接索引下載器的工作碎片，也不要把所有資料硬塞進一個
不可增量更新的巨型檔案。資料分成四層：

```text
下載工作區（可續傳、小檔、receipt）
        │ build / audit 成功
        ▼
查詢對齊的 canonical 資料（每日、每 symbol 或每 endpoint Parquet）
        │ packed publish
        ▼
Syncthing 發布庫（依選用政策的 ZIP／blob + manifest/head）
        │ 原子發布後通知掃描（penguin D:）／watcher（其他節點）+ cold verify
        ▼
各節點預設 COLD_ONLY（不自動 fetch/use/materialize）
        │ 僅人工按需 use
        ▼
本機唯讀 materialized 暫存樹
```

`scripts/packed_snapshot.py` 實作發布庫；
`configs/data_sync/packed_datasets.json` 是所有資料集的登錄表；
`scripts/publish_data_releases.py` 負責拒絕仍有下載器寫入的資料集。

可重建的訓練快取不屬於 canonical 資料。登錄表可用
`excluded_subtrees` 明確排除；目前 `tw-public` 排除
`stocks/panel_cache_v2`，各節點在本地按模型設定重建，避免每個 cache variant
都佔用同步流量與永久 CAS 空間。排除清單會寫入 manifest，不能暗中忽略資料。

## 為什麼不是單一巨檔

- 單一巨檔只改一筆也要重傳整檔，且損毀半徑最大。
- 每個小檔直接同步會消耗 Syncthing 掃描、資料庫與 inode。
- 舊預設以固定路徑雜湊分桶建立 base packs；新資料集政策依來源資料集
  聚合。後續 manifest 逐檔比較 SHA；未變檔案沿用既有 pack member，
  只有新增或變更檔案寫入 delta packs。
- 每個增量 manifest 都直接列出完整可重建物件集合，不需要依序重播整條 delta chain；
  被部分沿用的舊 pack 會標記為 subset reference，fetch 只解出目前 inventory 成員。
- 大於門檻的 Parquet/NPY 直接成為 SHA-256 blob，不做無效二次壓縮。
- 接收端仍可 materialize 舊目錄契約，既有訓練 reader 不必同時重寫。

舊預設門檻是 8 MiB；文字類小檔用 Deflate，Parquet、NPY、ZIP、Zstd 等已壓縮
格式用 Stored。每個來源檔、pack、blob、inventory 與 manifest 都有 SHA-256。

這裡的 `snapshot_id` 是向後相容的 release ID 名稱；它不代表每次發布都完整複製一棵
資料樹。真正佔空間的是 content-addressed objects。相同 inventory 會直接重用既有
release；部分變更只新增改變的固定桶／blob，未變資料繼續指向同一物件。

## 現況與目標

2026-08-12 的實機盤點：

| 資料 | 現況 | 新的發布單位 |
|---|---:|---|
| `data_tw_public` | 104,041 檔 / 29.81 GB | 64 個小檔桶 + 大檔 blobs |
| `data_tw_minute` | 約 39 萬檔 / 32.61 GB | 訓練版以每日 Parquet 發布；source chunks 為 cold-audit packs |
| `data_tw_microstructure` | 約 16.6 萬檔 / 9.62 GB | HFT partitions；分析版使用既有 one-file-per-symbol compactor |
| `data_yahoo` | 31,601 inode / 33.55 GB | per-symbol Parquet 保留，傳輸時包成固定桶 |
| `data_openBB` | 約 749 萬 inode / 134.33 GB | 只發布 `compact/**/archive.parquet`，task shards 不同步 |
| 舊 desync store | 58,802 chunks / 64.56 GB | 新發布庫通常只有數十至數百個物件 |

`data_tw_minute/research_dataset_schema2_volume_bug_20260807` 是已知有 volume
錯誤的 quarantine tree，沒有登錄成發布資料。OpenBB task shards 在 compact 與
audit 完成前也不得刪除，只是不進 Syncthing。

## Canonical packed 發布資料夾

所有 current nodes 只使用 Syncthing Folder ID `stockagent-packed`：

```bash
cd /root/stockAgent

./scripts/run_packed_snapshot.sh init \
  --sync-root /srv/stockagent-packed \
  --node-id penguin

./scripts/run_data_release.sh status tw-public
./scripts/run_data_release.sh publish tw-public \
  --sync-root /srv/stockagent-packed \
  --node-id penguin
```

Syncthing 設定：

```text
Folder ID:   stockagent-packed
Folder Path: /srv/stockagent-packed
Folder Type: Send & Receive
```

此 folder 必須開啟 filesystem watcher，且不可 paused。Syncthing 只複寫已發布的
immutable manifests、heads、packs/blobs；每次通過 build/audit 並原子更新 release 後，
變更物件即由 watcher 增量送出。接收端不配置任何自動 `fetch`、`use` 或 materialize
工作，所以同步完成只增加冷庫，不會產生 `data_*` 解封目錄。

`.local-state` 已由 `.stignore` 排除；每台機器有自己的永久 node ID。`heads/<dataset>/<node>.json`
保留多寫者，HLC 後寫者勝出。任何較新的 head 若尚未收齊 objects，接收端會 fail closed，
不會退回舊版本假裝成功。

目前 canonical 拓撲中，penguin 與 vastai1T 加入 `stockagent-packed`；
lab203 已退役，不能重新配對或接受其後續 release。penguin 是 full-replica durable node；
沒有 persistent volume 的 vastai1T 是 index-only edge。歷史 lab203 head、manifest、
objects 留在 penguin D 主冷庫，producer 名稱不能更改。
Vast 只常駐 heads、manifests 與 inventories。Vast 的大量訓練 artifacts 不可把整個 node-local
工作集直接加入 hot folder；edge mode 也禁止直接 cold publish。舊 `stockagent-desync`、
`stockagent-artifacts-live`、`stockagent-artifacts-hot` 與 Git working-tree folder 已退役；
不要重新接受 invitation。penguin 舊 hot transport 的磁碟內容保留待逐檔稽核，
解除 Syncthing 配對不等於刪除這些檔案。

舊 hot artifact transport 和 repository `artifacts` 在 penguin 以 hard link 共用實體內容，
不是兩份可獨立刪除的備份。已完成的完整 run 才能經過 `manage_cold_artifacts.py
retire` 的七日租期、C/D checksum、所有指定 peer、pin/程序引用及 hot mirror 核對，
把兩個熱名稱共同轉成 cold-only；部分小檔 release 不得退役整個 run。需用時以
`manage_cold_artifacts.py use DATASET` 借用既有 materialized-cache 的七日 lease、
READY 與 GC。這個過渡流程不移動或刪除 packed 冷物件與 D 槽歷史備份；詳見
`docs/live_artifact_sync.md` 與 README 的指令。

作業系統的 service manager 不屬於資料契約：penguin 使用 systemd，Vast container
使用平台 supervisor。兩種部署都必須達成同一組內容與連線驗收：永久 Device ID、永久
packed node ID、相同 Folder ID、`needBytes=0`、`needTotalItems=0`、無 errors、object
verify 通過，以及實際觀測到的 TLS/QUIC 連線。

## Vast index-only edge

這個模式解決「非持久計算節點不應為 500 GB 冷庫支付常駐空間」問題，而不是降低資料
保留標準。`.stignore-edge` 只忽略本機 `objects/blobs/**` 與 `objects/packs/**`；heads、
manifests、inventories 仍透過 Syncthing 即時更新。刪除本機 payload 前必須同時成立：

- penguin full-replica 已完成全物件 SHA-256 audit；
- Syncthing 對 penguin 為 connected、completion 100%、`remoteState=valid`；
- folder idle，need bytes/items/deletes 與所有錯誤皆為零；
- 本機 payload 沒有 fd、mmap、cwd、root 或 executable reference；
- ignore include 已落盤並完成一次 Syncthing scan。

先 audit，再分兩步套用：

```bash
source scripts/runtime_env.sh
set -a
source /etc/environment
set +a

./scripts/run_packed_snapshot.sh full-audit
run_fintech_python scripts/manage_packed_edge.py audit
run_fintech_python scripts/manage_packed_edge.py enable --apply
run_fintech_python scripts/manage_packed_edge.py prune --apply
```

`full-audit` 必須在 durable full replica 執行；它驗證所有 manifest 與每個 stored object 的
檔名 digest，包含尚未被 manifest 引用的物件，但只寫
`/var/lib/stockagent-packed-audit/receipts/`，永遠不刪檔。

`enable` 不刪檔；它只寫 node-local ignore/state。`prune` 逐檔重新檢查後，只 unlink 已忽略
的本機 blob/pack，保留所有 metadata、producer source、materialized data 與 penguin 冷庫。
Receipt 位於 `/var/lib/stockagent-packed-edge/receipts`。

需要某份資料時，不可解除整個 payload ignore；只允許 exact release objects：

```bash
run_fintech_python scripts/manage_packed_edge.py use tw-public \
  --link /root/stockAgent/data_tw_public
```

工具先產生 exact negated patterns，等待 Syncthing 收到需要的 objects，逐物件驗 SHA-256，
完成 materialize/READY/lease 後，再移除本機冗餘 packed payload。edge 節點若要發布新資料，
必須先切回 full-replica 或由 durable node 執行受稽核的 ingest；禁止讓 ignored object 配上
已發布 head。

若同一資料集即將反覆發布，可對明確指定的 release 使用 `use DATASET
--snapshot-id ID --retain-payload`。這只在驗證、READY 與租約成功後保留該 release 的
本機 packed payload 快取，最多到七日租約到期；下一版 hydration 暫時保留舊版與新版的
exact objects，驗收新版後清理不再需要的舊物件。它不改變 index-only 節點的發布禁令、
penguin 冷庫權威或 Syncthing 身分。未加選項仍維持原本驗證後立即 prune 的行為。
若工作中斷，重新執行相同 dataset/snapshot 的 `use` 可接續同步中的 exact release，
但 peer 連線、來源有效性與無錯誤閘門仍需通過。

既有的五分鐘 `stockagent-data gc` 排程會在 edge 自動改走 `manage_packed_edge.py gc`。
它只有在 durable peer 當下完整收斂時，才可用外部冷證明刪除到期 hot cache；pin、READY、
manifest hash 與 `/proc` 使用偵測仍照常 fail closed。Edge TTL 最多七天，舊的長 TTL
會安全縮為七天；有 live reference 時從最後觀測時間續租，刻意長期保留應使用 pin：

```bash
stockagent-data gc --dry-run
stockagent-data gc
stockagent-data evict DATASET --dry-run
```

## 接收端範例（已退役的 lab203 不得重新配對）

等 Syncthing 顯示 `idle / Up to Date`、`needBytes=0` 後：

```bash
cd /root/stockAgent

./scripts/run_packed_snapshot.sh init \
  --sync-root /srv/stockagent-packed \
  --node-id NEW_NODE_ID

./scripts/run_packed_snapshot.sh verify tw-public \
  --sync-root /srv/stockagent-packed
```

正常接收流程到此結束，不執行 `fetch`，也不建立 `data_tw_public` symlink。只有本機工作
負載確實需要可瀏覽目錄時才人工解封：`use` 先驗 manifest、inventory、所有 pack/blob，
再逐檔驗 size/SHA；成功後才原子切換 symlink：

```bash
./scripts/run_data_cache.sh use tw-public \
  --link /path/to/stockAgent/data_tw_public
```

工具只會替換既有 symlink，不會覆蓋實體目錄；七日未續租且沒有程序引用時自動回收。
這是明確 opt-in 的本機快取行為，不是 Syncthing 接收流程的一部分。

## 下載完成後自動發布

下載器、build 與 audit 應在同一個命令成功後才發布，避免人工「之後再合併」：

```bash
STOCKAGENT_SYNC_NODE_ID=penguin \
./scripts/run_downloader_with_release.sh tw-public /srv/stockagent-packed -- \
  ./downloader/run_daily_all_markets.sh
```

若一個資料集需要多階段，將完整 pipeline 包在單一既有入口內。例如 OpenBB 必須先
完成下載，再執行 `scripts/run_openbb_archive_compaction.sh` 並通過 catalog/audit，最後才
發布 `openbb-compact`。任何前置命令失敗或被中斷，wrapper 不會更新 head。

目前 OpenBB downloader 正在執行，因此 catalog status 會列出 PID blocker，發布命令會
拒絕它。這是刻意的資料一致性門檻。

## 日常操作

```bash
# 所有資料集、來源與 active blocker
./scripts/run_data_release.sh status

# 單一資料集發布
./scripts/run_data_release.sh publish okx \
  --sync-root /srv/stockagent-packed \
  --node-id penguin

# 確認最新版本
./scripts/run_packed_snapshot.sh status okx \
  --sync-root /srv/stockagent-packed

# 只報告未被 manifest 引用的物件；不刪除
./scripts/run_packed_snapshot.sh objects \
  --sync-root /srv/stockagent-packed
```

## 自助式冷庫與七日工作集

`/srv/stockagent-packed` 是唯一需要長期保存與同步的冷庫；
`/srv/stockagent-packed-materialized` 只是可刪除的本機工作集。所有接收節點的正常穩態
都是 `COLD_ONLY`；只有人工 `use` 才會進入 `HOT`。狀態機是：

```text
COLD_ONLY -- use/完整驗證 --> HOT -- 7 天未續租 --> COLD_ONLY
                       \-- 再次 use：O(1) ready proof + 續租
                       \-- 程序引用：GC monitor 自動從當下續租
```

查看每個資料集的冷庫大小、解封狀態、版本與到期時間：

```bash
./scripts/run_data_cache.sh status
./scripts/run_data_cache.sh status tw-public
./scripts/run_data_cache.sh status --human
```

真的需要使用資料時，才自行解封最新版本並取得路徑：

```bash
./scripts/run_data_cache.sh use tw-public

data_path="$(./scripts/run_data_cache.sh use tw-public --path-only)"
printf 'training data: %s\n' "$data_path"
```

每次 `use` 都把七日租約重新起算。工具也會維護穩定連結：

```text
/srv/stockagent-packed-materialized/current/<dataset>
```

若既有訓練程式要求固定路徑，可在完整驗證後原子切換一個**既有 symlink**；
工具拒絕覆蓋實體目錄：

```bash
./scripts/run_data_cache.sh use tw-public \
  --link /root/stockAgent/data_tw_public
```

想重新做完整逐檔 SHA 驗證時加 `--verify`。平常重複 `use` 只驗 READY proof，
不會每次重讀十萬個檔案。

手動預覽或執行清理：

```bash
./scripts/run_data_cache.sh gc --dry-run
./scripts/run_data_cache.sh gc
./scripts/run_data_cache.sh evict tw-public --dry-run
```

每五分鐘的 GC monitor 會先掃描 `/proc`。若程序的 fd、mmap、cwd、root 或 executable
仍指向 managed materialized tree，就依該 lease 原本的 TTL 從當下自動延長；不遍歷
資料樹，也不依賴 `noatime/relatime` 下不可靠的 access time。短於五分鐘的單次讀取仍應
先執行 `use`，讓 lease 立即續期。

自動清理只會刪除同時符合以下條件的 materialized tree：

- 租約超過七天；
- packed manifest 與全部 cold objects 仍在本機；
- materialization READY proof 與 manifest 相符；
- 沒有 `.pin.json` 保護；
- `/proc` 中沒有程序引用；若有，改為續租而不是刪除。

安裝每五分鐘 monitor；有 systemd 時安裝 timer，vast.ai container 則安裝 cron fallback：

```bash
sudo ./scripts/install_data_cache_gc_service.sh
```

安裝後可在任意目錄直接使用：

```bash
stockagent-data status
stockagent-data status --human
stockagent-data use tw-public --path-only
stockagent-data gc --dry-run
stockagent-data publish-status tw-public
stockagent-data publish tw-public
```

`publish` 仍會套用 catalog 的 active-downloader blocker、排除規則、完整來源穩定性
檢查與原子 head 更新；它不是繞過發布門檻的捷徑。
在 penguin 的 08:30 前官方資料驗收作業中，07:50 全量掃描先要求 156 項來源
零失敗、零 publication lag 並原子提交 live receipt；08:00/08:20/08:29 作業再依序
執行嚴格 audit、`publish tw-public`、完整 materialize 驗證與 symlink 切換。
任何階段失敗都不會切換推論資料。來源 receipt 的 `end_date` 若比現有 cold
manifest 舊，publish 也會 fail closed，避免多寫者用較晚 HLC 發布舊內容。

新資料的增量冷存仍使用既有發布入口。固定 hash bucket 與 content-addressed
blob 使未改變物件直接重用，只傳輸變動 bucket/blob：

```bash
./scripts/run_downloader_with_release.sh tw-public /srv/stockagent-packed -- \
  ./downloader/run_daily_all_markets.sh
```

冷庫 manifest、objects、heads 不受工作集 GC 影響；GC 不會刪
`/srv/stockagent-packed` 內任何資料。

## Penguin D 槽主冷庫與保留政策

`D:\stockagent-cold-primary\packed` 是唯一的 penguin 實體冷庫；受保護掛載維持
`/srv/stockagent-packed` 這個應用程式路徑。原子發布直接寫入 D 主庫，通過稽核後
呼叫 Syncthing REST scan（先 objects，後 manifest/head）；DrvFs 不依賴 inotify，
另有每 300 秒全量掃描。Syncthing 及發布流程在 D 缺席時必須 fail closed，絕不能
在 C 的空掛載點重新建立冷庫。

舊的 `run_packed_backup.sh` C→D 備份服務與 `run_packed_retention.sh apply`
C rolling-retention 已退役。不要重新安裝 timer 或對 D bind mount 執行舊 apply。
D 的舊歷史 manifest、物件與 head-history 繼續保留；目前沒有核准的自動 D
孤兒物件刪除政策。若未來需要 D GC，必須先證明完整 current、pin、租約、
歷史復原範圍和 peer 狀態，再設計獨立工具。current-head 完整不代表每個舊歷史
release 都完整；遷移稽核已記錄歷史缺件。

```bash
./scripts/mount_packed_d_cold.sh --check
systemctl status stockagent-d-cold-mount.service syncthing@root.service --no-pager
./scripts/run_packed_backup.sh status  # 應回報 retired_single_d_primary
stockagent-data status --human
```

這是使用者選擇的單一 D 實體磁碟，沒有本機獨立備份；硬碟損壞、勒索軟體或
整機事故仍可能造成遺失。詳見 [遷移稽核](d_cold_store_migration_2026-09-25.md)。

## 歷史 smoke 證據（2026-08-11）

以下數字只證明當時的 pack、verify 與 fetch 實作通過，不代表目前 Syncthing
進度、資料大小或 latest snapshot。現況一律用 `stockagent-data status`、
`run_packed_snapshot.sh status` 與 Syncthing 的 pending/error 指標重新量測。

來源：`data_tw_public/raw/twse_day_trade_eligibility`

- 來源：3,056 files、151,576,209 bytes
- 發布：4 packs + 1 inventory，共 55,122,722 bytes
- Syncthing payload 物件：由 3,056 降為 5
- manifest SHA-256：`ae67d5677f4555730fb1ae53a308143666cd2998c054625a275565c9d41bc980`
- inventory SHA-256：`4be2abceced85793315490ba63dc50df3615c02f5a3ced9b946d899c580c8e40`
- 完整 object verify、fetch、逐檔 materialized verify：通過

Smoke 位於 `/srv/stockagent-packed-smoke` 與
`/srv/stockagent-packed-smoke-materialized`，沒有加入現有 Syncthing folder，也沒有刪除
任何來源或舊 snapshot。

## 歷史首次正式平行發布（2026-08-11）

當時 `tw-public` 首次非破壞發布到 `/srv/stockagent-packed` 的 receipt 為：

- snapshot：`tw-public-20260811T184933239639615Z-l0-penguin-07e9b4c645ec020c`
- 來源：104,041 files、337 directories、29,811,139,176 bytes
- 發布：207 packs/blobs + 1 inventory，共 208 個同步物件
- 發布 bytes：24,965,534,554
- manifest SHA-256：`30802b9ccf7ccd91f3d9fe3d3825e1f8aa459a2a944f7fc0140f23a54e15541e`
- inventory SHA-256：`07e9b4c645ec020c31751d6182b496c7f416e7e14fac85437bde5aa1058269d3`
- object SHA、ZIP CRC、inventory 與 head resolution：通過
- 未引用 objects：0；刪除 objects：0

目前任何節點都不得沿用這段的「下一步」或 snapshot ID。部署與驗收請依根
[`README.md`](../README.md) 的「資料冷庫與多機同步」及「Syncthing 驗收」執行；
必須先達 `needBytes=0`、`needTotalItems=0`、`errors=0`、`remoteState=valid`，再做
packed object verify；接收端預設不得自動執行 `stockagent-data use`。
