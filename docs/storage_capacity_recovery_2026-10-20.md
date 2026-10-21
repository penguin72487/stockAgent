# 儲存寫入預算與精確回收（2026-10-20）

這次修改的是既有資料產生／解封／回收路徑，不是另外建立鏡像、清理 daemon 或冷庫。
本輪兩個固定舊根及其編碼暫存已精確回收，管理員保留區已恢復原值。
程式修正、程序實載、資料回收和同步恢復仍分開驗收；這不代表全庫遷移完成。

## 本輪精確回收與保留區復原（台北時間）

使用者同意後，已確認 mounted root `/dev/sdd`、ext4 UUID
`b37fb7b3-fe96-4301-a5ba-0e5363ff854c`，將保留區由原本 5,368,709 blocks
暫調為 1,342,177 blocks（約 1% → 0.25%），並完成獨立讀回。這讓
**16,492,675,072 bytes** 成為一般可用空間，不是刪檔、磁碟擴充或 Windows VHDX 回收。

兩個固定 minute-derived roots 均取得 `retired=true` 與 `pruned=true`。下表只計最後
名稱被移除的實體配置 bytes，不加總 hardlink 名稱、不把保留區調整算成刪檔。

| `data_tw_minute/` 下的舊根 | 原檔實際釋放 bytes | 編碼暫存實際釋放 bytes | 合計 GB（十進位） |
| --- | ---: | ---: | ---: |
| `research_dataset_developing_v5` | 17,624,690,688 | 17,639,854,080 | 35.26 |
| `research_dataset_schema2_volume_bug_20260807` | 13,525,262,336 | 13,540,646,912 | 27.07 |
| 合計 | 31,149,953,024 | 31,180,500,992 | **62.33** |

02:12 原工作流 continuation 成功結束，合計釋放 **62,330,454,016 bytes**。
兩份冷庫均已獨立重建所有編碼檔，再解碼核對原檔 SHA／長度／檔數；D 冷物件與 NAS
歷史刪除量均為零。逐根 acceptance／retirement／stage-pruned receipts 保留固定版本與
manifest SHA，不以單純「冷庫檔名存在」代替恢復證明。

02:12:48 在共同容量准入鎖內，核對同一裝置 UUID、容量幾何、現有空間預留與剩餘空間後，
將 reserved blocks 恢復原值 **5,368,709（約 1%）**，並獨立讀回。當時可用
**72,831,148,032 bytes**，高於既定 25 GiB 安全空間；未有 active reserved growth。
先前暫時開放的 16.49 GB 已歸還管理員保留區，未擴充／關閉 WSL，也未壓縮 VHDX。
並行服務仍持續寫入，因此 `df` 的即時差值不等於上述精確回收總數。

02:18:59 最終讀回成功：同一固定冷庫 generation 未變，兩根原檔、quarantine 內的 payload
與編碼暫存均不存在；工作流留下的 per-dataset quarantine 父目錄為空，不另計回收容量。
WSL 當時可用 **71,161,507,840 bytes（71.16 GB）**。

Syncthing canonical local gate 於復原後的 02:18:59 通過：idle、待傳 bytes／items／deletes
均為零，folder／system／pull／watch errors 均為零，service active。其原 1% database reserve 未變。
當前退休規則未指定 required remote peers，因此本機健康不代表全體遠端交付完成。

本輪修復兩項造成等待的因素：

- 01:05 完整稽核結尾遇到定時 scan，只有 `folder_idle` 未通過。legacy retirement 現可
  最多等待五分鐘，每次重跑整套 process／service／configuration／pin／bridge 與即時
  transport gates；非 idle 錯誤、缺資訊、逾時仍保留。最後 gates 前後再核完整 generation
  binding 與原 proof age，不能靠等待延長證明或忽略晚到使用者。
- 低空間時，原生 pack scratch 需要的 `pack bytes + 32 GiB` 無法滿足；沿用 portable
  D ZIP reader 核 SHA／CRC／members，較慢但不另落全量驗證 tree。本輪未降低 scratch
  headroom，也未略過任何還原或解碼步驟。

維護日誌：
[`ext4-reserve-maintenance-20261020.json`](../artifacts/operations/cache-capacity-20261019/ext4-reserve-maintenance-20261020.json)。
成功清理 run：`retry-minute-views-idle-scan-gat-20261019T173521-a4a7ce72`；
保留區復原 run：`restore-ext4-original-reserve-20261019T181243-0ca5b552`。

## 第一性原理與現有責任

必要的永久成本是「獨有來源內容＋必要修訂／模型／帳本＋可恢復證據」；訓練視圖與
編譯快取只有在能節省當前計算成本時才值得落地。內容相同不代表路徑、授權或用途相同。

| 層 | 現行路徑／責任 | 實體保存與回收規則 |
| --- | --- | --- |
| 來源與來源收據 | 原 `data_*` writer／collector | 保留原值、修訂、授權、時鐘；不能當一般 cache 刪除 |
| 正式冷庫 | D 上的 `/srv/stockagent-packed`、immutable lake | 不可變檔／區塊內容共用；版本是完整邏輯清單，不另落一份全量 payload |
| 管理式解封 | `/srv/stockagent-packed-materialized` | 按需固定版本、續用 lease、七日到期；完整冷恢復與熱資料一致才回收 |
| Panel | 原 `panel_cache_v2` writer | 重用既有 chunks；寫入前預留空間；舊 generation 仍受程序引用保護 |
| 相容熱來源／舊傳輸區 | `/srv/stockagent-live`、`/srv/stockagent-artifacts-hot` 等 | 不是整根七日 cache；逐項取得冷恢復、consumer、pin、transport 證據才退役 |
| 遠端訓練 | Vast 原訓練 lifecycle | 從固定來源版本＋Git／config／ABI 生成視圖，不把服務機預算默套到訓練機 |

`/srv/stockagent-packed` 是 D bind，不是 C 上第二份冷庫；同一 inode 的多個名稱也不能
重複加總為可釋放容量。保留舊的邏輯版本不等於保存多份完整實體 snapshot。

日／月分區適合新增觀測；供應商事後更正必須成為帶來源身分的新內容，不能 append 後
直接去重覆蓋原始歷史。共同 byte-chunk 版本化已存在，但 **FinLab 整表修訂尚未全部改為
原生時間分區**，不能把這次容量 guard 說成所有來源格式均已遷移。

## 已修正的共同寫入與回收路徑

1. **寫入前預算。** full fetch、subtree fetch 與 Panel writer 重用同一 kernel-lease
   reservation primitive；跨 cooperating family 保留檔案系統 headroom。有效 cache hit
   不掃 payload、不要求再留一份解封空間。
2. **只登錄 penguin。** [`cache_capacity.json`](../configs/data_sync/cache_capacity.json)
   目前設定 25 GiB 安全空間、256 GiB managed 解封上限、每個 Panel family 64 GiB。
   這是保守的最大增長准入，不是 OS hard quota；未接入的 writer 並不受它強制限制。
3. **量實體配置，而非加總路徑。** census 計算唯一 inode 的 `st_blocks`，不追 symlink，
   不跨 nested mount；超過 200,000 entries／5 秒或讀不到必要資訊就延期，不能以零代替未知。
   目前 managed root 的 bounded census 仍會延期；因此尚不能宣稱已完成全根容量盤點。
4. **檢查真正 backing volume。** WSL 不只看 ext4 虛擬 free space；沿用既有 Windows
   registry／實際 VHDX 容量工具。systemd 沒有登入環境時，只採用本機明確登錄的
   `training_rescue.json` distro，不能猜預設發行版。原 generic recovery 入口仍拒絕缺少身分。
5. **七日 GC 不只驗檔名。** full-replica 模式完整重建 cold objects 並核對熱檔 SHA／樹；
   唯一熱修改、額外熱檔、壞 pin、讀不到程序資訊、最後一刻 pin／檔案變更都保留。
   index-only edge 仍要求既有 durable-peer proof，不放寬為本機單檔存在。
6. **Panel commit 不代表舊資料任意可刪。** 不完整的 variant metadata、使用中 mmap／FD、
   不完整 `/proc` 可見性，都延後舊 generation 回收；已成功寫好的新 commit 不因此回滾。
7. **恢復驗證重用現有編碼。** 舊 archive 若仍有精確 publisher-owned encoding，可以先
   獨立重建 D 上每個 encoded member，完整核 SHA／ZIP CRC，再解碼原檔核 SHA／長度／檔數。
   不再先落另一份完整 encoded tree；仍會重查編碼及 manifest 是否變動。這不是跳過還原。

不新增自動冷物件／歷史 GC，不因容量超限提早刪 active lease／服務模型，不降低 Syncthing
1% database reserve，也不把未管理的所有 `artifacts/` 都改為七日刪除。

## 日常操作

```bash
source scripts/runtime_env.sh

# 唯讀：目前生效的寫入預算與 bounded census
stockagent-data capacity
stockagent-data capacity --panel-cache artifacts/cache/live_signal_panels/panel_cache_v2

# 沿用管理式使用／續期與七日回收入口
stockagent-data use DATASET --path-only
# 再次 use 已有的固定版本即續期，不重建另一份
stockagent-data use DATASET --snapshot-id SNAPSHOT_ID --path-only
stockagent-data gc --dry-run

# 原來源遷移狀態：只讀 journal，不掃全資料或呼叫 provider
run_fintech_python scripts/dataset_migration_status.py
```

`use` 因容量延期時回傳 exit **75**、stderr JSON `state=deferred`；`--path-only` stdout
不輸出假路徑。保留既有資料，解除容量或引用阻塞後再重試。`capacity` 的
`inventory_complete=false` 不是零用量，也不代表目前可接納新寫入。

完整指令、lease、服務 pin 與節點分工仍以 [README](../README.md#資料冷庫完整指令)、
[storage 契約](agents/storage.md) 與[增量版本架構](dataset_incremental_storage.md) 為準。

本輪舊 minute view 如需原檔，沿用既有 archive restore，不重新 publish。以下為第一根
範例，目的地必須是不存在的新路徑；`acceptance.json` 固定該次版本，不追 moving head。
第二根換成其 receipt dataset 即可。還原需要額外空間，以下只記錄用法，本輪未執行。

```bash
source scripts/runtime_env.sh
dataset=legacy-wsl-minute-derived-e50da5cab9ba3a40586064f1
receipt=artifacts/operations/wsl-storage-cleanup-20261006/retired-minute-view-retirement/"$dataset"
snapshot_id=$(jq -er '.snapshot_id' "$receipt/acceptance.json")
run_fintech_python scripts/manage_legacy_artifact_archives.py restore "$dataset" \
  --catalog "$receipt/catalog.json" \
  --snapshot-id "$snapshot_id" \
  --destination /path/to/new-empty-directory
```

## 實機觀察與未完成界線

- 00:24（台北）ext4 可用約 **7.55 GB**，D 可用約 **1.07 TB**；這不是已回收容量。
- 原 Syncthing API 實測 `state=error`：database 所在磁碟剩 0.35%，低於既定 1%。
  `needBytes=0`、`needTotalItems=0` 不足以宣稱同步健康。此狀態會擋住舊來源正式回收。
- 先前約 63 GB 舊副本已完成兩根精確回收，實際最後名稱配置為 62.33 GB，見上表。
  這不授權清除其他來源、使用中模型或未驗證暫存，也不表示完整來源遷移已完成。
- 新容量拒絕已在本機 systemd 程序驗證：低空間下先延期，payload 新寫入與資料刪除均為零；
  缺登入 WSL 變數時的 backing query 也已成功。不表示所有常駐程序均已重新載入程式。
- 原 GC timer／storage-pressure timer 沿用，原 migration／OpenBB owner 不強制重啟。
  transitive worker／服務 audit identity 已納入新 guard；實載更新仍須完成原安全交接與讀回。
- 唯讀 superblock／kernel 查核另發現 ext4 `clean with errors`：上次 fsck 後兩次錯誤都在
  2026-09-01，末次為 `ext4_validate_block_bitmap / EFSCORRUPTED`；目前未觀察到新的
  I/O error。需另排停止 WSL 的離線檢查，不在線上 root filesystem 強跑修復或清除錯誤旗標。
  本輪保留區調整前後 error count 均為 2；只處理可用空間，不修復檔案系統。

## 測試與證據索引

工作流 task：`all-dataset-incremental-layout-20261016`。所有 run 位於
`artifacts/operations/agent-workflow/runs/<run-id>/`；失敗、環境不符及中斷紀錄均保留。

- `encoding-reuse-acceptance-20261019T160800-c7f569dd`：5 passed，含 cold、encoding、晚到變動與外部路徑損毀拒絕。
- `cache-final-stable-v8-20261019T162241-da9ab331`：固定本次 guard／GC／編碼重用程式的 **376 passed**；涵蓋容量、Panel、回收、Windows I/O 與 training-return。
- `lakehouse-lifecycle-role-validat-20261019T162242-4b20253e`：原 lakehouse runtime **92 passed**。
- `retirement-scan-final-gates-v3-20261019T171036-060e19ba`：idle-only scan 競態與退役／同步共同 gates 的 **171 passed**，包括等待後開檔／pin、變動與過期拒絕。
- `native-cache-capacity-admission-20261019T161141-ece8aa88`：本機低容量拒絕，無新增 payload／刪除資料。
- `native-cache-backing-query-v3-20261019T161900-3c251b2e`：本機 systemd 明確 distro 的 backing query 通過。
- `retry-minute-views-idle-scan-gat-20261019T173521-a4a7ce72`：同一 frozen inventory 的兩根原檔與編碼暫存均取得完整回收收據，exit 0。
- `restore-ext4-original-reserve-20261019T181243-0ca5b552`：共同容量鎖內恢復原 1% 保留區，裝置／容量／error count 讀回通過，exit 0。
- `verify-reserve-retirement-v2-20261019T181356-152a52c5`：固定冷庫 generation、cold-only state、原檔／編碼暫存／quarantine payload、1% 保留區與 Syncthing 本機全門檻獨立讀回通過，exit 0。
- 初次 final readback 將 canonical workflow 的空 quarantine 父目錄誤判為剩餘 payload；v2 依既有實作檢查父目錄無內容，未刪資料或放寬 payload／版本驗證。原失敗紀錄保留。

先前工程驗收與當時阻塞原因保留於
`artifacts/operations/cache-capacity-20261019/acceptance-20261020.json`，不覆寫歷史觀測；
本輪實際回收與復原以新的維護日誌及逐根收據為準。不能加總不同修訂的重疊測試數當成
單一完整驗收，也不能把工程測試當全庫資料遷移完成。
