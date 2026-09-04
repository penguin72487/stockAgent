# stockAgent

台灣市場為主的多資產資料、研究、訓練、回測與即時訊號工作區。

本 README 是日常操作入口。正確性契約以 [AGENTS.md](AGENTS.md) 與
[訓練規格](docs/training_spec.md) 為準；完整文件分類見
[文件索引](docs/README.md)。原始資料與訓練產物不進 Git，實際路徑由市場 YAML、
資料集 catalog 與本機 storage root 決定。

## 目錄

- [第一性架構](#第一性架構)
- [五分鐘開始](#五分鐘開始)
- [資料冷庫與多機同步](#資料冷庫與多機同步)
- [資料冷庫完整指令](#資料冷庫完整指令)
- [發布新資料](#發布新資料)
- [Syncthing 驗收](#syncthing-驗收)
- [Artifacts 同步與去重](#artifacts-同步與去重)
- [資料下載與更新](#資料下載與更新)
- [訓練、GPU 與解釋](#訓練gpu-與解釋)
- [即時訊號與服務](#即時訊號與服務)
- [故障排查](#故障排查)
- [文件分類](#文件分類)

## 第一性架構

### 要解決的限制

1. Git 適合程式與小型設定，不適合數十 GB 資料或模型產物。
2. Syncthing 的主要固定成本之一是路徑與小檔索引；只做 hard link 去重不會減少路徑數。
3. 單一巨型壓縮檔雖然路徑少，但改一個檔案就可能重傳整包，損毀半徑也最大。
4. 訓練需要可直接隨機讀取的目錄；傳輸層則需要少量、可驗證、可增量重用的大物件。
5. 多台機器可以發布，但較晚的時鐘不能讓較舊資料覆蓋較新資料。

因此資料生命週期固定為：

```text
可續傳下載工作區
      │ build + strict audit 成功
      ▼
canonical 可讀資料
      │ publish：逐檔 SHA-256、固定 hash 分桶、重用未變 pack member
      ▼
/srv/stockagent-packed                 長期冷庫、Syncthing 唯一同步資料層
      │ Syncthing watcher 在 release 發布後即時增量複寫
      ▼
接收端預設停在 COLD_ONLY              不自動 fetch/use/materialize
      │ 僅在人工執行 use 時完整驗證並解封
      ▼
/srv/stockagent-packed-materialized    可重建的臨時熱工作集；七日安全 GC
```

核心規則：

- Git 同步程式；Syncthing 不同步 Git working tree，也不直接同步可變訓練資料夾。
- Syncthing 持續監看並同步 packed manifests、heads、packs/blobs；來源資料只有在 build 與
  strict audit 成功、原子發布 release 後才進入同步，不同步下載中的半成品。
- 接收節點預設只保留冷庫，沒有任何 timer、cron 或 service 自動執行 `fetch`、`use` 或
  materialize。解封只能是使用者明確要求的本機動作。
- 冷庫 release 不可變。各節點使用永久名稱，例如 `penguin`、`lab203`、`vastai1T`。
- 多寫者保留各自 head，以 HLC/LWW 決定候選最新版；來源 freshness receipt 仍必須
  不舊於現有 release，否則 fail closed。
- 小檔依固定路徑 hash 分桶；大型或已壓縮檔使用 content-addressed blob。未變內容直接
  重用，所以增量發布只產生並傳送真正改變的物件。
- `use` 只有在 manifest、inventory、pack/blob 與 materialized 檔案驗證成功後才切換
  symlink。熱工作集是快取，不是第二份權威資料。
- 七日回收使用明確 lease，不依賴不可靠的 `atime`。每五分鐘的 GC monitor 會透過
  `/proc` 偵測 fd、mmap、cwd 等程序引用並自動從當下續租；遇到 pin、缺少 cold object
  或 READY proof 不符時也會拒絕刪除。

不同資料層不可混用：

| 層 | 預設位置 | 是否長期保留 | 是否由 Syncthing 同步 |
|---|---|---:|---:|
| 程式與設定 | repository | 是，Git | 否 |
| 下載／修復工作區 | `data_*` | 依 catalog | 否 |
| immutable packed 冷庫 | `/srv/stockagent-packed` | 是 | 是，Folder ID `stockagent-packed` |
| materialized 熱工作集 | `/srv/stockagent-packed-materialized` | 否 | 否 |
| 可變 artifacts hot transport | `/srv/stockagent-artifacts-hot` | 視產物 | 是，獨立 folder |

### Canonical 多機拓撲

「架構一致」是指每一層有相同責任與驗收契約，不是要求每台機器盲目接受相同 folder。
目前的標準拓撲為：

| Folder ID | penguin | lab203 | vastai1T | 用途 |
|---|---|---|---|---|
| `stockagent-packed` | active，`/srv/stockagent-packed` | active，同一路徑 | active，同一路徑 | 所有 canonical 資料與完成 artifacts 的 immutable 冷 release |
| `stockagent-artifacts-hot` | active，transport root | active，工作 artifacts | 不加入 | penguin/lab 的低延遲 operational artifacts |

舊 `stockagent`、`stockagent-desync` 與 `stockagent-artifacts-live` Folder ID 已退役；
penguin 與 vastai1T 的舊 store/materialization 已在 packed release 完整驗證後刪除。
不要重新接受或建立這些 Folder ID。程式一律使用 Git，不同步 working tree。

Vast 的 `artifacts` 是大量訓練／ablation 輸出，不可直接加入
`stockagent-artifacts-hot`：這會把各節點的完整訓練工作集做聯集，增加數百 GB 流量、
索引與其他節點磁碟需求。Vast 的完成產物應先通過 lifecycle gate，再選擇性發布成 packed
cold release；執行中的 run 保持 node-local。

平台服務管理可以不同，但資料契約不可不同：

| 節點 | Syncthing 管理方式 | GUI | 備註 |
|---|---|---|---|
| penguin | `syncthing@root.service` | `127.0.0.1:8384` | systemd；hot artifact bridge 也由 systemd 管理 |
| lab203 | `syncthing@root.service` | `127.0.0.1:8384` | WSL 網路另依 QUIC 驗收 |
| vastai1T | Vast supervisor 的 `syncthing` | `127.0.0.1:18384` | 無 systemd；使用平台配置的 self-mapped TCP/UDP port |

所有節點必須保留自己的 Syncthing cert/Device ID 與 packed `.local-state/node-id`；只能同步
folder 內容，禁止複製另一台機器的 Syncthing config、cert、database 或 `.local-state`。

## 五分鐘開始

所有 repository Python 指令先使用同一個 runtime resolver：

```bash
cd /path/to/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
```

沒有 GPU 的純資料節點可拿掉 `--require-cuda`；訓練設定要求 CUDA 時不可默默退回 CPU。
若環境位置特殊，使用 `FINTECH_ENV_PATH=/path/to/fintech` 或
`PYTHON_BIN=/path/to/python`，不要把某台機器的 Conda 絕對路徑寫進腳本。

查看冷庫與同步狀態；預設不解封：

```bash
stockagent-data status --human
```

只有真的要訓練或查詢時，才人工執行
`stockagent-data use DATASET [--link PATH]`；它不是部署或同步的必要步驟。

開始一般訓練：

```bash
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw.yaml
```

所有入口均可用 `--help` 查目前程式實際支援的參數：

```bash
stockagent-data --help
./scripts/run_packed_snapshot.sh --help
source scripts/runtime_env.sh
run_fintech_python train.py --help
```

## 資料冷庫與多機同步

### 新機器一次性部署

先同步程式碼，再建立該機器自己的永久 node ID。以下以 `lab203` 為例；其他機器只能
替換名稱，不可複製另一台機器的 `.local-state/node-id`。

```bash
cd /path/to/stockAgent
git pull --ff-only origin twRule

sudo install -d -m 0755 /srv/stockagent-packed
sudo install -d -m 0755 /srv/stockagent-packed-materialized

./scripts/run_packed_snapshot.sh init \
  --sync-root /srv/stockagent-packed \
  --node-id lab203

sudo ./scripts/install_data_cache_gc_service.sh
```

安裝器會建立 `/usr/local/bin/stockagent-data`。有 systemd 時安裝每五分鐘 timer；沒有
systemd 的 container 會安裝 `/etc/cron.d/stockagent-data-cache-gc` fallback。

Vast container 不建立假的 `syncthing@root.service`，使用平台 supervisor：

```bash
supervisorctl status syncthing
supervisorctl restart syncthing
tail -f /var/log/portal/syncthing.log
```

Vast recycle 會重建 container filesystem；若 `/workspace` 不是 persistent volume，
`/opt/syncthing`、repository、冷庫與本機 node identity 都不會保留。每次新 instance 必須
重新確認 Device ID、重新 pair、執行 `init --node-id vastai1T`，並重新安裝 cache cron；
不可把舊機器 cert 複製到新 instance 冒充同一個 Syncthing device。

Syncthing 接受：

```text
Folder ID:   stockagent-packed
Folder Path: /srv/stockagent-packed
Folder Type: Send & Receive
```

等 Syncthing 通過[驗收條件](#syncthing-驗收)後，只驗證冷庫狀態，不解封：

```bash
stockagent-data status tw-public
./scripts/run_packed_snapshot.sh verify tw-public \
  --sync-root /srv/stockagent-packed
```

正常部署到此完成；不要建立 `data_tw_public` 等 materialized symlink。只有本機工作負載
確實需要展開資料時，才人工執行 `stockagent-data use`。

### 自訂 root 與預設值

預設值：

```text
STOCKAGENT_PACKED_SYNC_ROOT=/srv/stockagent-packed
STOCKAGENT_MATERIALIZED_ROOT=/srv/stockagent-packed-materialized
STOCKAGENT_DATA_CACHE_TTL_DAYS=7
```

可用環境變數覆寫：

```bash
STOCKAGENT_PACKED_SYNC_ROOT=/mnt/cold/stockagent-packed \
STOCKAGENT_MATERIALIZED_ROOT=/mnt/hot/stockagent-materialized \
STOCKAGENT_DATA_CACHE_TTL_DAYS=14 \
stockagent-data use tw-public
```

或把全域參數放在 subcommand 前：

```bash
stockagent-data \
  --sync-root /mnt/cold/stockagent-packed \
  --materialized-root /mnt/hot/stockagent-materialized \
  status --human
```

## 資料冷庫完整指令

### `status`：查冷／熱狀態

語法：

```text
stockagent-data status [DATASET] [--human]
```

```bash
# 全部資料集，機器可讀 JSON
stockagent-data status

# 單一資料集
stockagent-data status tw-public

# 適合人看的表格
stockagent-data status --human
```

狀態含義：

| `STATE` | 含義 | 動作 |
|---|---|---|
| `cold-only` | 冷庫完整，尚未解封或已安全回收 | 需要時執行 `use` |
| `hot-current` | current link 指向最新且 lease 有效 | 可直接使用；`use` 可續租 |
| `hot-outdated` | 熱資料仍完整，但冷庫有更新版本 | 重新 `use DATASET` |
| `hot-expired` | lease 已過期，等待安全 GC | 正在使用就 `use` 續租 |
| `hot-unmanaged` | 有目錄但沒有本工具 lease | 先查來源，不要手動刪除 |
| `invalid-link` / `broken-link` | current link 不合法或目標不存在 | 先查磁碟與 link，再重新 `use` |

`COLD_PAYLOAD_GB` 是壓縮／packed 冷庫物件大小；`HOT_LOGICAL_GB` 是解封後邏輯
大小，兩者不可相加當成必然實際占用量，因 sparse file、hard link 與 filesystem
allocation 可能不同。

### `use`：解封、驗證、切換並續租

語法：

```text
stockagent-data use DATASET
  [--snapshot-id ID]
  [--ttl-days DAYS]
  [--link PATH ...]
  [--verify]
  [--path-only]
```

```bash
# 解封 deterministic latest；預設 lease 七天
stockagent-data use tw-public

# 原子切換既有資料 symlink
stockagent-data use tw-public \
  --link /path/to/stockAgent/data_tw_public

# 精確重現指定 release
stockagent-data use tw-public \
  --snapshot-id tw-public-YYYYMMDDTHHMMSSZ-l0-NODE-HASH

# 自訂 lease
stockagent-data use tw-public --ttl-days 14

# 即使已有 READY proof，仍重新逐檔 SHA-256
stockagent-data use tw-public --verify

# 供 shell command substitution；stdout 只輸出路徑
data_path="$(stockagent-data use tw-public --path-only)"
printf 'dataset=%s\n' "$data_path"
```

工具永遠維護：

```text
/srv/stockagent-packed-materialized/current/<dataset>
```

`--link` 只會原子更新 symlink；若目標是實體目錄，工具會拒絕覆蓋。訓練期間不要編輯
materialized tree；要產生新資料應在 canonical 工作區完成 audit 後重新發布。

### `gc`：清除所有已到期且安全的熱快取

語法：

```text
stockagent-data gc [--dry-run]
```

```bash
stockagent-data gc --dry-run
stockagent-data gc
```

GC 每次都檢查 managed hot lease。若 `/proc` 顯示程序的 fd、mmap、cwd、root 或
executable 指向資料樹，就把 `last_used_at` 更新為當下，並依原 lease TTL 自動延長；沒有
程序引用時才處理已到期版本。刪除候選仍須 cold release 可完整重建、READY proof 相符且
沒有 pin。冷庫 `/srv/stockagent-packed` 不會被這個命令刪除。

這個偵測不掃描每個資料檔，也不使用可能被 `noatime/relatime` 關閉或延遲的 access time。
每五分鐘取樣適合長時間訓練、mmap 與持續讀取；短於取樣間隔的單次工具仍應先執行
`stockagent-data use DATASET`，該命令會立即續租。

### `evict`：立即要求回收一個熱快取

語法：

```text
stockagent-data evict DATASET [--snapshot-id ID] [--dry-run]
```

```bash
# 一律先預覽
stockagent-data evict tw-public --dry-run

# 回收該資料集的安全、未 pin 熱版本
stockagent-data evict tw-public

# 只選指定 release
stockagent-data evict tw-public \
  --snapshot-id tw-public-YYYYMMDDTHHMMSSZ-l0-NODE-HASH
```

`evict` 會忽略 lease 尚未到期這一項，但不繞過 cold completeness、READY、pin 與
process-reference 安全檢查。

### `publish-status`：發布前檢查

語法：

```text
stockagent-data publish-status [DATASET]
```

```bash
stockagent-data publish-status
stockagent-data publish-status tw-public
```

輸出會列出 catalog source、是否允許發布、active downloader blocker 與 freshness
狀態。它不修改 head。

### `publish`：發布 audited canonical 資料

語法：

```text
stockagent-data publish [DATASET] [--all-ready]
```

```bash
# 發布單一資料集
stockagent-data publish tw-public

# 發布 catalog 中所有 present、publishable、inactive 的資料集
stockagent-data publish --all-ready
```

`DATASET` 與 `--all-ready` 必須二選一。發布者會讀取 `init` 時保存的永久 node ID；
若尚未初始化會拒絕發布，不要用環境變數臨時冒用別台機器名稱。

可發布資料集與排除子樹以
[`configs/data_sync/packed_datasets.json`](configs/data_sync/packed_datasets.json)
為準。`publish` 不會繞過 active writer、來源穩定性、排除規則或 freshness receipt。

### packed 冷庫底層命令

日常優先使用 `stockagent-data`；需要稽核、精確 pin 或 subtree fetch 時才使用底層入口：

```text
./scripts/run_packed_snapshot.sh init
./scripts/run_packed_snapshot.sh publish DATASET SOURCE
./scripts/run_packed_snapshot.sh resolve DATASET
./scripts/run_packed_snapshot.sh status DATASET
./scripts/run_packed_snapshot.sh verify DATASET
./scripts/run_packed_snapshot.sh fetch DATASET
./scripts/run_packed_snapshot.sh fetch-subtree DATASET SUBTREE
./scripts/run_packed_snapshot.sh objects
```

常用完整範例：

```bash
# 初始化節點
./scripts/run_packed_snapshot.sh init \
  --sync-root /srv/stockagent-packed \
  --node-id penguin

# 解析 deterministic latest，並寫入精確 pin
./scripts/run_packed_snapshot.sh resolve tw-public \
  --sync-root /srv/stockagent-packed \
  --pin /srv/stockagent-packed-materialized/tw-public.pin.json

# 顯示 winner 與大小；--snapshot-id 可指定歷史版本
./scripts/run_packed_snapshot.sh status tw-public \
  --sync-root /srv/stockagent-packed

# 驗 manifest、inventory 與全部 cold objects
./scripts/run_packed_snapshot.sh verify tw-public \
  --sync-root /srv/stockagent-packed

# 原子解封整份 release
./scripts/run_packed_snapshot.sh fetch tw-public \
  --sync-root /srv/stockagent-packed \
  --materialized-root /srv/stockagent-packed-materialized \
  --pin /srv/stockagent-packed-materialized/tw-public.pin.json

# 只解封 release 中一個相對子樹
./scripts/run_packed_snapshot.sh fetch-subtree tw-public stocks \
  --sync-root /srv/stockagent-packed \
  --materialized-root /srv/stockagent-packed-materialized

# 統計 stored、referenced、unreferenced objects；永不刪除
./scripts/run_packed_snapshot.sh objects \
  --sync-root /srv/stockagent-packed
```

底層 `publish` 可調 `--pack-buckets`、`--loose-threshold-mib`、
`--compression-level`、`--exclude-subtree`、`--maximum-file-bytes`、`--metadata`
與 `--max-clock-skew-seconds`。正式資料仍應經 catalog 入口，避免漏掉資料集專屬 audit：

```bash
./scripts/run_packed_snapshot.sh publish DATASET SOURCE \
  --sync-root /srv/stockagent-packed \
  --node-id penguin \
  --metadata audit=strict
```

每個 subcommand 的權威參數表：

```bash
./scripts/run_packed_snapshot.sh init --help
./scripts/run_packed_snapshot.sh publish --help
./scripts/run_packed_snapshot.sh resolve --help
./scripts/run_packed_snapshot.sh status --help
./scripts/run_packed_snapshot.sh verify --help
./scripts/run_packed_snapshot.sh fetch --help
./scripts/run_packed_snapshot.sh fetch-subtree --help
./scripts/run_packed_snapshot.sh objects --help
```

## 發布新資料

### 手動發布

```bash
stockagent-data publish-status tw-public
stockagent-data publish tw-public
./scripts/run_packed_snapshot.sh verify tw-public \
  --sync-root /srv/stockagent-packed
```

### 下載成功後才發布

把完整 downloader、build、audit 放在 `--` 後；前段任何命令失敗或被中斷，都不會推進
head：

```bash
STOCKAGENT_SYNC_NODE_ID=penguin \
./scripts/run_downloader_with_release.sh \
  tw-public /srv/stockagent-packed -- \
  ./downloader/run_daily_all_markets.sh
```

penguin 的官方 TW 驗收 service 使用同一原則：主工作成功後才由 `ExecStartPost` 發布
`tw-public`。若 receipt 的 `end_date` 比冷庫現有版本舊，即使本機 HLC 較新仍拒絕發布。

冷庫物件 GC 目前只有 `objects` 報告，沒有自動刪除。熱快取 GC 不等於冷庫 GC；在所有
節點 retention 與 manifest 引用關係未確認前，不可手動刪 cold objects。

## Syncthing 驗收

設定存在或 service 顯示 `active` 都不代表同步成功。每個 peer 必須同時滿足：

```text
folder state       = idle / Up to Date
needBytes          = 0
needTotalItems     = 0
errors             = 0
pullErrors         = 0
remoteState        = valid
```

連線層另看實際 transport，而不是只看設定：

- 優先 `quic-client` / `quic-server`；QUIC 與 TCP 都由 Syncthing TLS 驗證與加密。
- 多通道必須看實際 connection count；只設 `numConnections` 不代表每條都已建立。
- 若落到 relay，資料仍加密，但通常吞吐較低。檢查 UDP 22000、防火牆、路由器 port
  forward；WSL NAT 的 Windows `portproxy` 只處理 TCP，UDP 要用 mirrored networking、
  Windows 原生 Syncthing，或正確的 UDP forwarding。
- 公網 IP 會變時保留 `addresses=["dynamic"]`；不要把短期 IP 當永久機器名稱。

Syncthing 顯示完成後仍做內容層驗證：

```bash
./scripts/run_packed_snapshot.sh verify tw-public \
  --sync-root /srv/stockagent-packed
stockagent-data status tw-public
```

舊 desync store 與 materialized snapshots 已退役並刪除。若要重建資料，只能從
`/srv/stockagent-packed` 的 manifest/object 驗證後 materialize；不要重新建立
`/srv/stockagent-sync` 或 `/srv/stockagent-snapshots`。舊流程文件只保留歷史稽核用途。

## Artifacts 同步與去重

資料集與 artifacts 的生命週期不同：資料集是 canonical release；訓練中 artifacts 可能
持續寫入。可變／大型 artifacts 走 hot transport，已完成 run 的穩定小檔才封成 packed
cold release。

Vast 要發布完整可部署 run 時，先在 `configs/data_sync/cold_artifacts.json` 登錄唯一
dataset，並以 `maximum_file_bytes: null` 明確要求包含所有檔案；仍須通過 lifecycle、
穩定時間、inventory 與 packed-object 驗證。

### cold artifact 完整命令

```text
scripts/manage_cold_artifacts.py [全域路徑參數] status [DATASET]
scripts/manage_cold_artifacts.py [全域路徑參數] publish DATASET [--node-id NODE]
scripts/manage_cold_artifacts.py [全域路徑參數] activate DATASET
  [--conflict-policy fail|local-wins|packed-wins]
scripts/manage_cold_artifacts.py [全域路徑參數] rebuild-ignore
```

```bash
source scripts/runtime_env.sh

run_fintech_python scripts/manage_cold_artifacts.py status
run_fintech_python scripts/manage_cold_artifacts.py publish \
  ARTIFACT_DATASET --node-id penguin
run_fintech_python scripts/manage_cold_artifacts.py activate \
  ARTIFACT_DATASET --conflict-policy fail
run_fintech_python scripts/manage_cold_artifacts.py rebuild-ignore
```

衝突策略：`fail` 最安全；`local-wins` 保留本機；`packed-wins` 以已驗證 release 覆蓋。
跨機器首次啟用不可猜測衝突策略，必須依該機器的權威角色選擇。

全域路徑參數為 `--registry`、`--artifact-root`、`--sync-root`、
`--live-sync-root`、`--state-root`；用 `--help` 查實際預設：

```bash
run_fintech_python scripts/manage_cold_artifacts.py --help
run_fintech_python scripts/manage_cold_artifacts.py activate --help
```

安裝 hot bridge 與檢查：

```bash
sudo ./scripts/install_hot_artifact_sync_service.sh
systemctl status stockagent-hot-artifact-sync.service --no-pager
```

穩定檔案內容去重先 audit，再套用 hard link；它節省 block，不減少 Syncthing 路徑數：

```bash
source scripts/runtime_env.sh

# 唯讀報告
run_fintech_python scripts/deduplicate_artifacts.py \
  --root artifacts \
  --min-age-hours 24 \
  --complete-runs-only

# 套用前再次確認報告，再原子 hard-link byte-identical 檔案
run_fintech_python scripts/deduplicate_artifacts.py \
  --root artifacts \
  --min-age-hours 24 \
  --complete-runs-only \
  --apply

sudo ./scripts/install_artifact_dedup_service.sh
```

完整遷移、ignore 與 penguin 衝突權威規則見
[即時 artifacts 同步](docs/live_artifact_sync.md)。

## 資料下載與更新

### 每日總入口

```bash
# 前景執行一次所有啟用市場
bash downloader/run_daily_all_markets.sh

# 背景排程；不要用無參數呼叫
bash downloader/daily_downloader_daemon.sh start
bash downloader/daily_downloader_daemon.sh status
bash downloader/daily_downloader_daemon.sh restart
bash downloader/daily_downloader_daemon.sh stop
```

常用開關：

```bash
DAILY_PARALLEL_GROUPS=0 bash downloader/run_daily_all_markets.sh
RUN_TW_PUBLIC_DATA=0 bash downloader/run_daily_all_markets.sh
RUN_TW_PUBLIC_FEATURES=0 bash downloader/run_daily_all_markets.sh
RUN_FRANKFURTER=0 bash downloader/run_daily_all_markets.sh
RUN_CEX_PERP=0 bash downloader/run_daily_all_markets.sh
RUN_DATA_QUALITY_AUDIT=1 bash downloader/run_daily_all_markets.sh
```

### 台灣官方資料

```bash
source scripts/runtime_env.sh

# 新機器建立 verified baseline
run_fintech_python downloader/download_tw_official_data.py \
  --mode rebuild \
  --stage-root artifacts/data_rebuild/tw_2000_bootstrap \
  --promote

# 修復與每日增量
run_fintech_python downloader/download_tw_official_data.py --mode repair
run_fintech_python downloader/download_tw_official_data.py --mode daily

# 顯示低階資料集 manifest
run_fintech_python downloader/download_tw_public_data.py \
  --mode list --datasets all

# 建立訓練 feature parquet
run_fintech_python scripts/build_tw_public_training_features.py \
  --input-dir data_tw_public \
  --output-path data_tw_public/features/tw_public_stock_daily.parquet \
  --symbols-root data_tw_public/stocks
```

完整 rebuild、repair、rate limit 與 receipt 規則見
[台灣公開資料續傳文件](docs/tw_public_download_resume_and_rate_limits.md)。

### Yahoo、外匯與加密市場

```bash
source scripts/runtime_env.sh

run_fintech_python downloader/download_yahoo_ohlcv.py --mode incremental --asset all
run_fintech_python downloader/download_yahoo_ohlcv.py --mode repair --asset all
run_fintech_python downloader/download_forex_frankfurter.py \
  --mode daily-update --output-dir data_yahoo/forex

run_fintech_python downloader/download_okx_perp_1m.py --mode incremental
run_fintech_python downloader/download_bybit_perp_1m.py --mode incremental
run_fintech_python downloader/download_binance_perp_1m.py --help
```

首次全量、日期範圍、worker 與 provider-specific 選項請直接看各 downloader `--help`；
不要從其他 provider 猜相同旗標。

### 一分鐘與衍生品資料

```bash
source scripts/runtime_env.sh
run_fintech_python downloader/download_shioaji_tw_minute_kbars.py --help
run_fintech_python scripts/build_shioaji_tw_minute_dataset.py --help
run_fintech_python downloader/stream_shioaji_tw_microstructure.py --help
run_fintech_python downloader/stream_shioaji_taifex_bidask.py --help
```

資料粒度、即時 Tick/BidAsk 與歷史 KBar 的邊界見
[TW minute 研究](docs/tw_minute_kbar_research.md) 與
[Shioaji HFT 資料](docs/shioaji_hft_dataset.md)。

## 訓練、GPU 與解釋

### 單一訓練

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict

run_fintech_python train.py --config configs/markets/tw.yaml
run_fintech_python train.py --config configs/markets/tw_public.yaml

# 帶環境檢查的 runner
./coda_runner.sh -c configs/markets/tw.yaml
./coda_runner.sh --help
```

市場、資料範圍、execution mode、模型、loss、checkpoint 與 output root 都以該次 YAML
為準，不要只看 README 的範例推測實驗契約。

### 多 GPU job manager

先編輯 `configs/gpu_jobs.yaml`，再使用：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/manage_gpu_jobs.py validate
run_fintech_python scripts/manage_gpu_jobs.py start
run_fintech_python scripts/manage_gpu_jobs.py status
run_fintech_python scripts/manage_gpu_jobs.py restart crypto
run_fintech_python scripts/manage_gpu_jobs.py stop us crypto
```

單一 job 直接使用多 GPU 時，可由市場 config 的
`training.multi_gpu_strategy: auto` 配合可見 GPU 數自動選單卡或 DDP。不要另外建立第二套
loss/backtest executor。

### 解釋性分析

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict

# 先看當前完整參數，避免沿用舊機器 scratchpad
run_fintech_python scripts/screen_explainability_features.py --help
run_fintech_python explain_model.py --help

# 最小入口範例
run_fintech_python explain_model.py \
  --config configs/markets/tw_public.yaml \
  --device cuda \
  --amp-dtype bf16 \
  --plots
```

分散式 explainability 必須依實際 GPU、VRAM 與 config 重新決定
`--nproc-per-node`、chunk size、IG/SHAP 參數，不把單台機器的舊數字當固定基線。

## 即時訊號與服務

本機產生訊號：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/live_signal.py \
  --market-config services/discord_bot/markets/tw.yaml \
  --price-source panel
```

Discord bot 入口：

```bash
source scripts/runtime_env.sh
run_fintech_python services/discord_bot/bot.py
```

需要 `DISCORD_BOT_TOKEN`、`DISCORD_CHANNEL_ID`、`STOCKAGENT_MARKETS_DIR` 與
`STOCKAGENT_DEFAULT_MARKET`。市場 YAML 決定 checkpoint discovery、資料更新器與
`live_output_dir`。

systemd 服務一律用同一組操作方式；以下以 Shioaji top-200 為例：

```bash
systemctl status stockagent-shioaji-top200.service --no-pager
journalctl -u stockagent-shioaji-top200.service -f
systemctl restart stockagent-shioaji-top200.service
systemctl stop stockagent-shioaji-top200.service
systemctl disable --now stockagent-shioaji-top200.service
```

`active` 只代表程序存在。仍須檢查 restart count、最新 receipt／status JSON、資料時間、
錯誤 log 與實際 API/檔案輸出。

## 故障排查

### 收到冷庫但看不到 `data_tw_public`

Syncthing 只同步冷庫，不會自動建立使用者可見資料夾：

```bash
stockagent-data status tw-public
stockagent-data use tw-public \
  --link /path/to/stockAgent/data_tw_public
readlink -f /path/to/stockAgent/data_tw_public
test -d /path/to/stockAgent/data_tw_public && echo ready
```

### `publish` 被拒絕

```bash
stockagent-data publish-status DATASET
ps aux | rg 'download|build|repair'
```

常見原因是 active writer、來源 receipt 不完整、來源比 cold winner 舊、catalog 標示
`publish: false`，或來源根本不存在。不要用底層 publish 繞過資料集契約。

### GC 沒刪到期資料

先讀 JSON 中每個候選的 `reason`：

```bash
stockagent-data gc --dry-run
systemctl status stockagent-data-cache-gc.timer --no-pager
journalctl -u stockagent-data-cache-gc.service --since today --no-pager
```

正常阻擋包括 active lease、pin、程序仍引用、cold release 不完整或 READY proof 不符。

### Syncthing 完成但 verify 失敗

```bash
./scripts/run_packed_snapshot.sh status DATASET \
  --sync-root /srv/stockagent-packed
./scripts/run_packed_snapshot.sh verify DATASET \
  --sync-root /srv/stockagent-packed
./scripts/run_packed_snapshot.sh objects \
  --sync-root /srv/stockagent-packed
```

先讓 Syncthing 回到 `needBytes=0`、`needTotalItems=0`；不要在 object 尚未收齊時手動改
head 或複製 manifest。

### 取得精確版本以重現訓練

```bash
./scripts/run_packed_snapshot.sh resolve DATASET \
  --sync-root /srv/stockagent-packed \
  --pin /path/to/run/data.pin.json

stockagent-data use DATASET --snapshot-id SNAPSHOT_ID
```

把 pin 與實驗 artifacts 一起保存；不要在 resume 時重新解析 `latest`。

## 文件分類

| 類別 | 入口 | 用途 |
|---|---|---|
| 現行正確性契約 | [AGENTS.md](AGENTS.md) | point-in-time、backtest、checkpoint、reproducibility |
| 文件總索引 | [docs/README.md](docs/README.md) | 區分現行契約、runbook、研究與歷史文件 |
| 訓練架構 | [training_spec.md](docs/training_spec.md) | 訓練、評估、artifact 驗收 |
| 公開面板 | [public_dashboards_architecture.md](docs/public_dashboards_architecture.md) | 唯讀資料流、快取、前端競態、安全與上線驗收 |
| packed 冷庫 | [packed_dataset_storage.md](docs/packed_dataset_storage.md) | pack/blob、manifest、lease 與 smoke 證據 |
| 舊 desync | [desync_multiwriter_sync.md](docs/desync_multiwriter_sync.md) | 舊版本遷移與救援 |
| artifacts | [live_artifact_sync.md](docs/live_artifact_sync.md) | hot/cold artifact 分層、衝突與去重 |
| 台灣資料 | [tw_public_download_resume_and_rate_limits.md](docs/tw_public_download_resume_and_rate_limits.md) | rebuild、repair、daily、receipt |
| 執行模式 | [tw_execution_modes.md](docs/tw_execution_modes.md) | day/cash/overnight 與交易語意 |
| 分鐘資料 | [tw_minute_kbar_research.md](docs/tw_minute_kbar_research.md) | causal minute dataset 與訓練 |
| TX/TXO | [tw_index_derivatives_tick_strategy.md](docs/tw_index_derivatives_tick_strategy.md) | 期貨選擇權 tick 策略 |
| OpenBB | [openbb_archive_downloader.md](docs/openbb_archive_downloader.md) | archive ingestion 與 compaction |
| 操作補充 | [RUN_GUIDE.md](docs/RUN_GUIDE.md) | 特定 operator 工作流；執行前核對當前 config/path |

README 只保留可重現、跨機器成立的入口。套件全面升級、文字編輯器操作、單台機器 GPU
數字與臨時修復命令不屬於標準部署流程；需要時先用 `--help`、現行 config 與對應 runbook
確認，再執行可回復的變更。



sudo apt update && sudo apt full-upgrade -y && sudo apt autoremove -y && sudo snap refresh
mamba activate fintech
mamba update --all
