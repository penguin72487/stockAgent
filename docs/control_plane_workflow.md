# 跨節點工程工作控制

這是 `target-architecture-20261003` 的可選控制角色。第一個 handler 是
`verify-code-release`，實際工作交給既有
`stockagent.runtime_identity.verify_source_release`。程式核對沒有 provider
配額、GPU、訂單或來源發布副作用。正式 collectors、GPU manager、盤中
ledger、SQLite 續傳與 packed publication 各自保留寫入責任。

## 已準備的環境

本機使用獨立 PostgreSQL 18/main、`stockagent_control` DB／登入角色，僅監聽
`127.0.0.1`。角色沒有 superuser／create database／create role 權限。
`/etc/stockagent/control-plane.env` 是 0600 的私有 role 設定，包含 DSN 與
control Python 路徑。不要將其內容放入命令列、task receipt、Git 或公開 API。

新 control Python 使用獨立 Miniforge／Mamba role，宣告為
`configs/environments/control.yml`，explicit lock 為
`configs/environments/locks/control-linux-64-20261003.explicit.txt`。
`--install-role-only` 只建立新 prefix／套件 receipt，不重啟 DB 或服務；
原生 prefix 與任何既有未知環境都會先拒絕。Linux x86_64 預設使用已通過
雙節點驗收的 exact lock，其他平台先由宣告建立自己的 build inventory。
`--activate-role-only` 核對套件指紋、驗收 prefix 與新／舊 interpreter 讀到的
canonical DB 狀態，再原子調整 private env 的 `CONTROL_PLANE_ENV_PATH`，
保留 credentials、其他設定與 0600 權限；不重啟 DB 或服務。
原 CUDA／fintech／舊 venv 保留。
前一輪 uv／venv 的 accepted receipts 是歷史證據，不作新環境建立方式。
`scripts/runtime_env.sh` 會在追蹤 interpreter symlink 前辨認 `pyvenv.cfg`，
避免 role Python 被誤解析為它的 Conda base。

新節點已有 PostgreSQL binaries、且選定 cluster 沒有其他資料時，可執行：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/install_control_plane.py \
  --cluster 18/main --evidence artifacts/operations/CONTROL_SETUP
```

installer 會拒絕含其他 DB 的 cluster、缺私有設定的既有角色、過寬的 env 權限。
它為獨立 DB 設定 64 MB shared buffers、24 connections 與 control slice。
安裝或更新 backup units 可單獨使用 `--install-backup-only`，不重啟 DB、不安裝
套件。首次工程環境安裝與 credential 更換的 receipts 各自保留。

既有節點的有界切換使用同一份 installation evidence：

```bash
run_fintech_python scripts/install_control_plane.py --install-role-only \
  --env-root /var/lib/stockagent/control-plane/NEW_ROLE \
  --evidence artifacts/operations/NEW_ROLE_SETUP
run_fintech_python scripts/install_control_plane.py --activate-role-only \
  --env-root /var/lib/stockagent/control-plane/NEW_ROLE \
  --evidence artifacts/operations/NEW_ROLE_SETUP
```

## 日常入口

```bash
source scripts/runtime_env.sh
stockagent-agent work status
stockagent-agent work --schema stockagent_control init
```

建立 spec 時必須使用 exact code receipt 與 code SHA。下例只寫工程 spec：

```bash
run_fintech_python - RELEASE/release.json artifacts/work-spec.json <<'PY'
import hashlib, json, sys
from pathlib import Path
from downloader.artifact_io import atomic_write_json
from stockagent.control.contracts import WorkSpec
receipt = Path(sys.argv[1])
value = json.loads(receipt.read_bytes())
spec = WorkSpec('selected-code-validation', {
    'receipt_sha256': hashlib.sha256(receipt.read_bytes()).hexdigest(),
    'source_sha256': value['code']['source_sha256'],
})
atomic_write_json(Path(sys.argv[2]), spec.as_dict())
PY

stockagent-agent work submit artifacts/work-spec.json
stockagent-agent work worker-once \
  --node penguin --worker selected-code-verifier \
  --receipt RELEASE/release.json --root FROZEN_CODE_ROOT \
  --output artifacts/operations/CONTROL_ATTEMPTS
```

`--schema` 放在 subcommand 前。`worker-once` 是一次有界 action；持續 supervision
仍由現有 `stockagent-agent run`／systemd 負責。CLI 不接受任意 command 作為
共享 work kind。每份 spec 的 key、kind、inputs、dependencies、priority、
重試上限與 CPU／RAM／scratch 預算共同構成不可變身分。

相同 key／身分重送是 no-op；相同 key 改輸入會拒絕。依賴必須已提交且真正
成功才可 claim；不能用 worker 已開始或來源可連線取代 completion proof。
失敗依賴的工作保持 pending，等待依賴成功。

## Lease、資源與恢復

claim 在 PostgreSQL transaction 中使用 node row lock 與 job
`FOR UPDATE SKIP LOCKED`；同一工作只能有一個有效 attempt owner。
lease 使用 server clock，鎖等待之後再驗截止時間。heartbeat／finish／fail
核對 node、worker、attempt、token、input identity 與有效 lease。
所有新 worker／reader 也核對 stored schema contract，未知、空版本或已有工作表
而版本表消失皆不准入；不能將既有工作的缺版本狀態當成全新 catalog 初始化。
每次 register／submit／claim／expire／heartbeat／finish／fail／snapshot 都在操作
transaction 中先鎖定版本表並核對版本。SHARE lock 與其他 worker 相容，阻止版本
metadata 的更新／刪除／DDL 與此操作交錯；結束後，已連線的舊 worker 下一次操作
仍需重新准入。snapshot 保留 repeatable-read／read-only，先取得鎖再建立快照。
已連線的 initialize 也不得補建遺失／清空的 metadata 來重新認證既有工作。

工作執行前釋放 transaction lock，hashing 不把整個 DB 鎖住。過期 attempt
保留 `expired`，新的 attempt 使用新 token；舊 token 不能覆寫接手者的結果。
程式錯誤保留歷史、有界延遲與上限；aging 讓長時間等待的低優先工作仍有機會。
可用 `stockagent-agent work expire-leases` 顯式處理到期紀錄。

目前資源是這個 role 的宣告 slots，加上 node 的 affinity、MemAvailable、
scratch 實際觀測，並扣除有效 reservations。這不等於主機 CPU idle，亦沒有
GPU／provider quota 跨機准入。DB row fencing 不能中止已到期的實體 GPU 程序，
也不能保證任意 API／券商副作用 exactly-once；這些仍需原 owner 的 lease、
idempotency、outbox、receipt 與恢復契約。

## 備份與跨節點驗收

`stockagent-control-backup.timer` 每日執行 private logical backup，亦可直接：

```bash
systemctl start stockagent-control-backup.service
systemctl show stockagent-control-backup.timer -p NextElapseUSecRealtime
```

備份存於 `/var/lib/stockagent/control-plane/backups`，directory 0700、archive／
receipt 0600，每次保留不同檔名、checksum、archive 可讀性與執行時間。
它沒有刪除保留版本、沒有將 role password 放進 DB dump，也沒有聲稱同主機
備份可以代替離機災難恢復。

新的備份還附帶 private `.state.json`：所有使用者表的欄位／列值 SHA 由同一個
repeatable-read exported snapshot 計算，`pg_dump --snapshot` 使用該 snapshot。
這避免背景工作寫入造成指紋與備份不同；可列出 archive 仍不代表已完成還原。
在已備妥 trusted Vast SSH、原 runtime 與 Miniforge／Mamba manager 的節點，可用：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/verify_control_plane_offhost_restore.py \
  --backup-receipt /var/lib/stockagent/control-plane/backups/latest.json \
  --output artifacts/operations/UNIQUE_OFFHOST_RESTORE
```

工具建立自己的 PostgreSQL 18 Mamba roles，按當下條件交錯比較 declaration／
explicit 各三次完整建置，再於沒有 TCP listener 的獨立 cluster 比較 1／2／4
workers 各三次實際全 DB 還原，逐表驗欄位及 rows SHA。只清除本次建立的測試
DB／停止自己的 cluster，保留備份與 receipts，原 native、正式 DB 與 GPU owner
不更新。角色密碼不傳送，PG state 不公開；第一次需下載額外 PG 套件與保留
試驗角色的容量。`--output` 必須是新目錄。
離機 restore 與持久保存分開；本次 Vast workspace 無 persistent volume，
`durable_off_host_backup_verified=false` 保留。完整實測及未解條件見
[架構未解問題續作](architecture_unresolved_repair_2026-10-03.md)。

跨節點 proof 的可重複入口：

```bash
run_fintech_python scripts/verify_control_plane_pilot.py \
  --receipt RELEASE/release.json --source-root FROZEN_CODE_ROOT \
  --output artifacts/operations/UNIQUE_CONTROL_PILOT \
  --role-lock configs/environments/locks/control-linux-64-20261003.explicit.txt
```

它使用現有 trusted SSH ingress 設定，將 exact release 與另有 inventory 的
control code 傳到新的 Vast 私有目錄。Vast control Mamba role 與 native runtime 分開；
manager 確定缺少時可加 `--install-remote-miniforge` 安裝 hash-pinned Miniforge
到獨立 prefix，按該節點 Range 實測選下載並行度，不更改原 GPU venv 或 shell init。
每個方法各測三次完整 declaration／explicit 建立，只有 Conda package builds
完全相同才可比較與選擇；原生環境再核對 before／after。
DB 僅透過 loopback reverse SSH forward，驗完停止自己的 tunnel、移除本次遠端
credential。兩個節點各完成同一份 release 驗證，另一個工作在遠端 claim 後
刻意 exit，server lease 到期後由 Penguin 接手；三個 work／四個 attempts 都保留。

同一驗收會 `pg_dump` isolated schema、還原到新測試 DB，逐欄比較 nodes、jobs、
dependencies、attempts，只有完全相同才 accepted，最後只移除自己建立的測試 DB。
2026-10-03 另有 actual whole-DB round-trip proof，所有使用者 schemas／tables
資料一致、原 DB state SHA 不變，測試 DB 已移除；見目標報告中的
`control-whole-backup-restore-acceptance.json`。daily backup 不會每次自動還原。
這不等於 production jobs 已全部遷移、PostgreSQL HA 或 Vast volume persistence。
目前 worker 是可信任的工程 role；多租戶權限沒有接入。
任何版本遷移仍須先停止領取、排空既有 worker，不能依版本檢查來中止已開始的
實體程序。本輪補上每次操作的版本鎖定，不自動遷移 schema；受驗的是新程式
revision，不把舊 frozen controller 當成已更新的部署。

詳細實測、原生環境、故障／還原與版本界線見
[目標架構驗收](target_architecture_execution_2026-10-03.md)。
新的環境與引擎候選實證見
[技術實測與 Mamba 工作流程](architecture_technology_trials_2026-10-03.md)。
