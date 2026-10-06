---
name: stockagent-storage-operations
description: "操作 StockAgent 四節點架構：penguin authority、D immutable lake、DuckLake 版本、Temporal lifecycle、rclone 按需複寫、Syncthing ingress、lab203／NAS archive 與 Restic 備份、Vast 訓練回傳、恢復與回收。用於這些流程的查詢、接手、修復與優化；一般 SQL、策略、模型架構或網站開發不觸發。"
---

# StockAgent 四節點儲存維運

## 先定位責任

1. 找到本次 StockAgent checkout，讀取其 `AGENTS.md`、`docs/agents/storage.md`
   的相關段落；環境／開機恢復另讀 `docs/agents/runtime.md`。先查 Git dirty work，
   保留其他人的修改。lab203 的固定 worker 副本不一定是 Git checkout，不猜 repo 路徑。
2. 依 `configs/data_sync/node_storage_roles.json`、本機已部署 unit／Supervisor、
   mount 與程序辨認角色。共用路徑在不同節點有不同用途；hostname 或角色 JSON
   本身不能證明冷庫有效，也不授權刪檔。
3. 只查進度時執行唯讀入口。實作或維運的多步工作使用既有 `stockagent-agent`
   task／run 收據接手原目標，依需要讀 `docs/agent_workflow.md`。先分析的要求維持分析；
   已授權的修復直接完成，不把本 skill 當作額外批准流程。

| 節點／位置 | 保存與工作責任 |
| --- | --- |
| penguin WSL ext4／SSD | 權威原始觀察與 provenance、可變來源狀態、PostgreSQL／下單 ledger、網站和正式服務必要 projection／模型 |
| penguin D HDD，packed 及 guarded immutable lake | 唯一本機主冷庫；不可變 CAS／packs／manifests／heads、Parquet/ZSTD registry 與封閉版本；mutable PG 放 SSD |
| lab203 | Receive Only 封閉 ingress、中繼 NAS、獨立驗證及回傳機器收據；不承接網站或訓練 |
| NAS | immutable archive 保存 lake/source 版本；Restic 保存 code/config/SQL 與原歷史；保留／prune 是獨立政策 |
| Vastai1T | 非持久訓練節點；cold index、當期 exact source、遠端生成的工作集／cache、尚未驗證回傳的結果 |

## 按任務載入

| 要做的事 | 讀取與使用 |
| --- | --- |
| 查進度、確認同步、補傳、lab203／NAS 維運、開機恢復 | [日常操作與唯一 owner](references/operations.md) |
| 準備當期訓練資料、遠端建置實測、完成成果回傳 | [訓練資料生命週期](references/training-data.md) |
| 清理空間、處理共用鎖、獨立還原、故障接手 | [恢復與回收判準](references/recovery-and-retirement.md) |

從 checkout 執行的唯讀起點：

```bash
source scripts/runtime_env.sh
stockagent-data automation-status --human --live
stockagent-data status --human

# penguin：固定角色及 namespace，查真正的持久 workflows
bash scripts/run_lakehouse_control.sh status
```

評估回收候選時另用 `stockagent-data gc --dry-run`。它不刪除資料，但 Vast 的
edge 路徑會取得操作鎖並寫收據，不屬完全唯讀的進度查詢。

沒有安裝 `stockagent-data` 時使用 `bash scripts/run_data_cache.sh` 加相同參數。
這個 wrapper 會把 penguin 的 `status`／`automation-status` 整段觀測放在正式
PID 1 mount namespace，並把 Vast 的 `use`／`gc`／`evict` 路由至 index-only edge，不能繞過它
直接以 full-replica 的 GC 清理 Vast。`--live` 查目前冷索引傳輸，不會證明全歷史已備份。

## 資料流與操作界線

- **來源到備份：** canonical collector／build／strict audit → catalog-backed 原子發布
  到 D → DuckLake 固定 registry／原 manifest bytes → Temporal 封閉增量波次 →
  Syncthing ingress → lab203 固定 relay → rclone immutable NAS archive、獨立還原
  與全檔 SHA → paired ACK。Restic 接原始 manifest/head、code/config/SQL 傳統備份，原歷史保留；
  packed／PG 語義收據另驗，兩套覆蓋不能直接相加。
- **來源到研究：** 在 Vast 取固定 source release，核對 Git／config／ABI／runtime，
  已登錄 rclone SFTP payload 與 Syncthing index 分工；完整 SHA、demand fence、
  fetch／READY 通過後，才用原 builder 建 panel／tensor／split／cache。冷檔送達不自動
  解封，不跟隨 moving latest，不在 penguin 永久保存訓練工作集。
- **成果回傳：** 沿用 completed-return owner；lifecycle 完整、全檔身分、D 原值
  重建、當前傳輸與新鮮 ACK 全部通過後，才回收該次允許的遠端精確來源名稱。
  模型部署／下單服務啟用另走正式 promotion，不因回傳自動切換。
- **回收：** 先清冊／dry run，核對 exact recovery、程序／服務／FD／mmap、pins／
  leases、來源穩定、同步與 quarantine。`cache`／`features` 檔名不代表可重建；
  原值可能在 `*_features.parquet`。不依目錄名稱批次刪除，D 缺盤不退回 C。
  NAS ACK 先持久化，傳輸 GC 另走有界 timer 與每批鎖；背景 HDD 清理不能
  阻塞下一批 replication。只清暫存 ingress，不清 D authority 或 NAS archive。
- **效能：** 取當前 CPU affinity／cgroup quota／NUMA／RAM headroom／I/O／GPU owner，
  測使用者等待的完整流程並保留 parity／驗證。歷史 threads、雙 socket、codec、
  timings 只作候選；新租用節點、boot 或 workload 身分變動後重測。

日常排程由既有服務持續運作，agent 用來查證與修復；不需要常駐 Codex 對話。
penguin 的固定 code-release admission 已有 PostgreSQL／systemd owner；新版本
由 canonical builder 固定後用同一 enrollment 登錄，依
`docs/control_plane_workflow.md` 操作。它只驗已登錄 code，不接管 collector、
GPU、publisher 或下單，NAS 新 logical state 的獨立還原仍沿原備份管線。
保留 Miniforge／Mamba 的角色環境與 runtime lock，Python 使用
`scripts/runtime_env.sh`／`run_fintech_python`，不硬編碼某台機器的 interpreter。
沿用已有授權來源與 `publish: false` 限制，不把同步流程當作權限擴張。

## 交付與維護

此完整 skill 目錄放在 checkout 的 `.agents/skills/stockagent-storage-operations/`。
其他節點帶入 `SKILL.md`、`agents/` 及 `references/` 全部成員後，從該 checkout
使用 `$stockagent-storage-operations`。沒有完整 repo 的 lab203 agent 先用內附
角色／本機 service 操作；需要未帶入的 topic contract 時取得對應固定版本再修改。
現有 frozen code／runtime bundle 的 scope 不保證含 `.agents/skills` 或 `docs`，
不要把 code receipt 或 NAS 備份完成當作 skill 已部署；以接收節點實際檔案與
skill discovery 核對。讀取 skill 不應改寫既有 frozen code 的身分。
NAS auxiliary 在 `include_documentation: true` 時會備份 Git-visible 的公開
repo skills，包含完整本目錄；備份還原與其他節點實際啟用仍分開驗證。

回報本次節點／資料版本／範圍、最新收據時間、實際完成 bytes／檔數、pending／
deferred 原因與下一個 owner。分開說明可連線、傳輸收斂、檔案備份、語義還原、
可回收、業務恢復；`active`、exit 0 或單批通過都不代表全系統完成。

本 skill 是操作導航；維護中的程式、config 與 topic contract 建立當前實作。
現行部署與完整 scope 見 `docs/ducklake_temporal_replication_2026-10-05.md`；
固定清冊補傳與 NAS 去重進度修正見 `docs/nas_sync_catchup_2026-10-06.md`。
不用歷史「尚未採用」結論取代目前收據。
它不固定硬體調校，也不宣稱全歷史、所有 cache 或實際 filesystem 格式遷移已完成。
架構變更時先更新原契約／runbook，再同步修正此 skill 的入口、範例與 references。
本機 USB 金鑰異機保管已由使用者確認完成，Windows 登出驗收已取消；沒有新範圍
或矛盾時沿用，不重問、不把取消的驗收寫成通過。
