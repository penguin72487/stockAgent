# 大檔資料發布與 Syncthing 同步設計

## 結論

不要讓 Syncthing 直接索引下載器的工作碎片，也不要把所有資料硬塞進一個
不可增量更新的巨型檔案。資料分成四層：

```text
下載工作區（可續傳、小檔、receipt）
        │ build / audit 成功
        ▼
查詢對齊的 canonical 資料（每日、每 symbol 或每 endpoint Parquet）
        │ packed publish
        ▼
Syncthing 發布庫（固定 hash 分桶 ZIP + 大檔 blob + manifest/head）
        │ watcher 即時增量同步 + cold verify
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
- 第一版以固定路徑雜湊分桶建立 base packs。後續 manifest 逐檔比較 SHA；
  未變檔案沿用既有 pack member，只有新增或變更檔案寫入 delta packs。
- 每個增量 manifest 都直接列出完整可重建物件集合，不需要依序重播整條 delta chain；
  被部分沿用的舊 pack 會標記為 subset reference，fetch 只解出目前 inventory 成員。
- 大於門檻的 Parquet/NPY 直接成為 SHA-256 blob，不做無效二次壓縮。
- 接收端仍可 materialize 舊目錄契約，既有訓練 reader 不必同時重寫。

預設門檻是 8 MiB；文字類小檔用 Deflate，Parquet、NPY、ZIP、Zstd 等已壓縮
格式用 Stored。每個來源檔、pack、blob、inventory 與 manifest 都有 SHA-256。

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

目前 canonical 拓撲中，penguin、lab203 與 vastai1T 都加入
`stockagent-packed`；只有 penguin/lab203 加入低延遲 `stockagent-artifacts-hot`。
Vast 的大量訓練 artifacts 只能在通過 completion contract 後選擇性發布成 cold release，
不可把整個 node-local `artifacts` 工作集直接加入 hot folder。舊 `stockagent-desync`、
`stockagent-artifacts-live` 與 Git working-tree folder 已退役；不要重新接受 invitation。

作業系統的 service manager 不屬於資料契約：penguin/lab203 使用 systemd，Vast container
使用平台 supervisor。兩種部署都必須達成同一組內容與連線驗收：永久 Device ID、永久
packed node ID、相同 Folder ID、`needBytes=0`、`needTotalItems=0`、無 errors、object
verify 通過，以及實際觀測到的 TLS/QUIC 連線。

## Lab203 接收（預設 cold-only）

等 Syncthing 顯示 `idle / Up to Date`、`needBytes=0` 後：

```bash
cd /root/stockAgent

./scripts/run_packed_snapshot.sh init \
  --sync-root /srv/stockagent-packed \
  --node-id lab203

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
在 penguin 的 08:30 官方資料驗收作業中，只有主作業成功後才會透過
`ExecStartPost` 自動執行 `publish tw-public`；下載或嚴格 audit 失敗時不會
發布。來源 receipt 的 `end_date` 若比現有 cold manifest 舊，publish 也會
fail closed，避免多寫者用較晚 HLC 發布舊內容。

新資料的增量冷存仍使用既有發布入口。固定 hash bucket 與 content-addressed
blob 使未改變物件直接重用，只傳輸變動 bucket/blob：

```bash
./scripts/run_downloader_with_release.sh tw-public /srv/stockagent-packed -- \
  ./downloader/run_daily_all_markets.sh
```

冷庫 manifest、objects、heads 不受工作集 GC 影響；GC 不會刪
`/srv/stockagent-packed` 內任何資料。

垃圾回收目前刻意只有報告，沒有自動刪除。必須先確認所有節點已收到所有 manifests、
保留版本政策已決定，才可另行加入可恢復的 GC。

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
