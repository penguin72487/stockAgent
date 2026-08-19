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
        │ verify / fetch
        ▼
各訓練機器的唯讀 materialized tree
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

## Penguin 建立新的發布資料夾

不要覆蓋目前可用的 `stockagent-desync`。先用獨立 Syncthing Folder ID
`stockagent-packed` 做平行遷移：

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

`.local-state` 已由 `.stignore` 排除；每台機器有自己的永久 node ID。`heads/<dataset>/<node>.json`
保留多寫者，HLC 後寫者勝出。任何較新的 head 若尚未收齊 objects，接收端會 fail closed，
不會退回舊版本假裝成功。

## Lab203 接收與 materialize

等 Syncthing 顯示 `idle / Up to Date`、`needBytes=0` 後：

```bash
cd /root/stockAgent

./scripts/run_packed_snapshot.sh init \
  --sync-root /srv/stockagent-packed \
  --node-id lab203

./scripts/run_packed_snapshot.sh verify tw-public \
  --sync-root /srv/stockagent-packed

./scripts/run_packed_snapshot.sh fetch tw-public \
  --sync-root /srv/stockagent-packed \
  --materialized-root /srv/stockagent-packed-materialized \
  --pin /srv/stockagent-packed-materialized/tw-public.pin.json
```

`fetch` 先驗 manifest、inventory、所有 pack/blob，再逐檔驗 size/SHA，寫入
`.partial.*`，整棵樹成功才用 `os.replace` 原子升級。舊資料 symlink 只在驗證後切換：

```bash
snapshot_path="$(
  ./scripts/run_packed_snapshot.sh fetch tw-public \
    --sync-root /srv/stockagent-packed \
    --materialized-root /srv/stockagent-packed-materialized \
  | jq -r .materialized_path
)"
# 檢查 snapshot_path 後才執行：ln -sfn "$snapshot_path" data_tw_public
```

文件刻意把實際 `ln -sfn` 切換留為人工確認步驟，避免誤換正式訓練資料。舊 desync
庫至少保留兩次成功發布/接收循環再退役。

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
`/srv/stockagent-packed-materialized` 只是可刪除的本機工作集。狀態機是：

```text
COLD_ONLY -- use/完整驗證 --> HOT -- 7 天未續租 --> COLD_ONLY
                       \-- 再次 use：O(1) ready proof + 續租
```

查看每個資料集的冷庫大小、解封狀態、版本與到期時間：

```bash
./scripts/run_data_cache.sh status
./scripts/run_data_cache.sh status tw-public
./scripts/run_data_cache.sh status --human
```

自行解封最新版本並取得路徑：

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

自動清理只會刪除同時符合以下條件的 materialized tree：

- 租約超過七天；
- packed manifest 與全部 cold objects 仍在本機；
- materialization READY proof 與 manifest 相符；
- 沒有 `.pin.json` 保護；
- `/proc` 中沒有程序的 fd、mmap、cwd、root 或 executable 指向該 tree。

安裝每日 timer；有 systemd 時安裝 timer，vast.ai container 則安裝 cron fallback：

```bash
sudo ./scripts/install_data_cache_gc_service.sh
```

安裝後可在任意目錄直接使用：

```bash
stockagent-data status
stockagent-data status --human
stockagent-data use tw-public --path-only
stockagent-data gc --dry-run
```

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

## 已完成的真實 smoke

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

## 已建立的正式平行發布版

`tw-public` 已非破壞發布到 `/srv/stockagent-packed`，尚未自動加入 Syncthing：

- snapshot：`tw-public-20260811T184933239639615Z-l0-penguin-07e9b4c645ec020c`
- 來源：104,041 files、337 directories、29,811,139,176 bytes
- 發布：207 packs/blobs + 1 inventory，共 208 個同步物件
- 發布 bytes：24,965,534,554
- manifest SHA-256：`30802b9ccf7ccd91f3d9fe3d3825e1f8aa459a2a944f7fc0140f23a54e15541e`
- inventory SHA-256：`07e9b4c645ec020c31751d6182b496c7f416e7e14fac85437bde5aa1058269d3`
- object SHA、ZIP CRC、inventory 與 head resolution：通過
- 未引用 objects：0；刪除 objects：0

下一個外部狀態步驟是把 `/srv/stockagent-packed` 以 Folder ID
`stockagent-packed` 分享給 `lab203`。收到 100% 後在 `lab203` 執行前述 verify/fetch，
成功前不要切換 `data_tw_public`，也不要退役 `stockagent-desync`。
