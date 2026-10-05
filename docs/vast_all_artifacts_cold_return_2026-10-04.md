# Vast markets／ablations 回傳 D 冷庫

本次範圍是遠端 `vastai1T` 的完整 `artifacts/markets` 和 `artifacts/ablations`，
不是先前只回收快取的維護。penguin 仍是唯一資料權威；冷庫仍只有
`D:\stockagent-cold-primary\packed`，Vast 維持 index-only。

## 目前證據與限制

2026-10-04 的同一次遠端 `du -x -B1 --max-depth=1` 觀測：

| 範圍 | 配置檔案系統空間 |
| --- | ---: |
| `artifacts/markets` | 285,098,950,656 bytes |
| `artifacts/ablations` | 119,647,420,416 bytes |
| 合計 | 404,746,371,072 bytes |

這是配置空間觀測，不是待傳輸量或已回收量。12:07 完整 metadata 清冊記錄
196 個項目、1,965,183 個檔案、519,044,286,735 邏輯 bytes：markets
362,541,502,541、ablations 156,502,784,194。硬連結等使邏輯大小與配置空間不同。
清冊、inode 分配與逐項狀態以本機私人收據為準；去重或壓縮收益只从實際 release 收據計算。

11:56 遠端 `stockagent/config.py` 的 Git 合併標記使 canonical 服務依賴檢查觸發
`SyntaxError`。12:07 再查已可從該實際檔案解析 130 筆服務引用；本次未改動或
替遠端選擇 Git 合併版本。此類錯誤若再發生，仍允許保存穩定原值，不允許刪除來源。亦觀測到資料準備及
雙卡程序；不停止、不重啟、不搬動其使用中的目錄。合併衝突不能為了清理而
直接選擇任一版本。

第一個實際批次 `markets/tw_futures_v8_preparation`：68 檔、87,471,988
原始 bytes 已回傳、封存、獨立從 D 解碼恢復，通過當時所有來源／consumer／pin／雙端
Syncthing 檢查後回收遠端 87,646,208 配置檔案 bytes。本機 transfer／encoded scratch
已驗證回收；D 原值與 release 留存。這是 scoped pilot，不是整批 519.04 GB 完成。
收據：`/var/lib/stockagent-vast-legacy-return-pilot/`。

12:09 完整 dry run：177 個穩定目錄待嘗試、1 個 pilot 已回收、6 個受保護根目錄、
12 個非目錄項目保留。其中兩個超大根目錄分別為 futures preparation 125.11 GB
及舊當沖 ablation 79.28 GB（邏輯大小），需要另外以完整子分區清冊處理；ablation
亦有不支援連結。`would-return` 不是完成證明，每項 apply 前仍重查所有門檻。

12:16 已從保留的完整清冊建立兩大根的明確子分區清冊：446 個項目，
1,888,498 檔、204,375,518,240 邏輯 bytes；236 個穩定子目錄可嘗試、1 個更新中
子目錄保護、209 個根層散落檔案／連結保留。拆分只改處理單位，不搬動來源或略過
原根散落檔案；完整檔案數守恆檢查已通過。散落檔案尚未取得獨立 file-bundle
冷恢復證明，不能把子目錄完成當作整根已冷存。此 cohort 位於
`/var/lib/stockagent-vast-legacy-return-partitions/`，與主批次共用同一 ingress lock。

## 資料流與安全條件

Vast 原目錄 → SSH/rsync 私人暫存 → 既有 legacy 逐檔编码 → penguin D
content-addressed blobs/packs → 從 D 獨立取回、解碼並核對所有原值 →
當前雙端 Syncthing、完整服務／程序／pin 檢查 → 遠端 dry run／私人 quarantine／回收。

SSH 只搬一次穩定工作資料，Syncthing 仍只同步原子提交後的冷庫物件與 metadata。
Vast 不會發布 cold heads，也不會新增 raw artifacts folder。資料只有經內容
雜湊驗證相同才共用 immutable object；release ID 不代表另一份完整目錄副本。

每項 archive 標示 `deployable=false`、`completion_claim=not_checked`。
封存不是訓練完成、策略可部署或服務可用的證明。新封存包含原始檔案、SHA-256、
權限、修改時間及空目錄。12 小時穩定門檻不代表「有 12 小時沒開啟」；
程序引用／服務依賴另查。共享 inode、pin、未知／不支援的路徑全部保留。

每批只處理一個明確根目錄，與既有完成產物 ingress 共用 owner lock。
暫存使用本機 ext4；開始前同時預算 WSL／Windows C 實際空間、D 空間和 64 GiB
保留額度。超過 64 GiB 的整根會明確保留，需另有完整子分區清冊，不能略過根目錄
散落檔案就宣稱整根完成。中斷／異動／校驗失敗的暫存保留供調查。

## 自助指令

所有指令在 penguin `/root/stockAgent` 執行，沿用 runtime；不要在 Vast 執行 publisher。

```bash
source scripts/runtime_env.sh

# 完整只讀清冊；尚有未完成批次時先核对收據，不覆寫清冊。
run_fintech_python scripts/return_remote_legacy_archives.py inventory

# dry run，記錄候選及保護理由；不搬動或刪除資料。
run_fintech_python scripts/return_remote_legacy_archives.py archive

# 先做一個有界實際批次。預設也只在所有安全檢查通過後才回收 Vast 原副本。
stockagent-agent run --backend tmux --name legacy-return-batch \
  vast-all-artifacts-cold-return-20261004 -- \
  run_fintech_python scripts/return_remote_legacy_archives.py archive --apply --max-items 1

# 已驗收流程後持續處理清冊內所有穩定、有容量且不在用的候選。
stockagent-agent run --backend tmux --name legacy-return-all \
  vast-all-artifacts-cold-return-20261004 -- \
  run_fintech_python scripts/return_remote_legacy_archives.py archive --apply

# 已驗收 pilot 不重搬：需重新從 D 恢復核對、來源確實已不存在，才承接其收據。
run_fintech_python scripts/return_remote_legacy_archives.py archive \
  --reuse-progress /var/lib/stockagent-vast-legacy-return-pilot/progress.json

# 小型進度摘要；完整清冊和每項收據保持可查。
jq . /var/lib/stockagent-vast-legacy-return/summary.json
jq . /var/lib/stockagent-vast-legacy-return-partitions/summary.json
jq '[.items[] | {relative_root,state,logical_bytes,cold_verified,retirement,blockers,error}]' \
  /var/lib/stockagent-vast-legacy-return/progress.json

# 指定超大根以完整清冊產生新 cohort，不修改或刪除原根。
run_fintech_python scripts/return_remote_legacy_archives.py partition \
  --state-root /var/lib/stockagent-vast-legacy-return-NEW-COHORT \
  --from-inventory /var/lib/stockagent-vast-legacy-return/inventory.json \
  --partition-root markets/EXACT_OVERSIZED_ROOT

# 封存恢復到全新目錄；不覆寫現有模型，不代表可部署。
run_fintech_python scripts/manage_legacy_artifact_archives.py restore DATASET \
  --catalog /var/lib/stockagent-vast-legacy-return/archive-catalog.json \
  --destination /ABSOLUTE/NEW/RESTORE/PATH \
  --restore-cache /var/lib/stockagent-vast-legacy-return/restore-cache
```

預設 SSH 沿用已驗證的專用 key 與目前主機/port，可用 `--ssh-target`、`--ssh-port`
及 `--identity-file` 指定；不能從舊 IP 推定仍是同一台主機。node identity／key
不會經資料同步複製。進度、清冊、catalog、ACK 和控制收據位於
`/var/lib/stockagent-vast-legacy-return/`，由 owner 私人保存；不含 Syncthing API key。

`cold-verified-source-protected` 表示資料已從 D 成功恢復，**遠端尚未刪除**；
`return-failed-source-preserved` 表示傳回未完成，來源保留；`remote-source-retired`
才表示遠端回收。批次程序退出成功不表示所有項目都完成：仍須查看完整各項狀態。
獨立程序以 `--config` 載入的來源／初始化／輸出目錄也在回收門檻內；不能因為
資料已載入 RAM、fd 暫時關閉就視為沒在用。回收後再核 D 恢復的失敗保留本機原值 scratch。

## 驗證

### 15:04 增量冷編碼加速

這次取樣看到主程序在 D 的 `_copy_and_hash`／`_fsync_directory`，並不是仍在
下載。23.11 GB 舊批次的未壓縮 NPZ 很多；原冷編碼只壓縮大 CSV，網路端 zstd
不會縮小落地的冷物件。減少需要寫入／讀回的 bytes，才會同時縮短冷發布、完整
恢復和備份成本。沒有增加 D 同時寫入者、停交易服務或跳過恢復驗證。

同一組三個真實 NPZ／Parquet／PT、106,725,752 bytes，以正序／反序各一次比較
**封存發布＋D 全檔解碼驗證＋完整還原＋原值 SHA／mode／mtime**：

| 冷編碼候選 | 完整流程中位秒數 | 冷物件 bytes |
| --- | ---: | ---: |
| 原 CSV-only gzip | 50.808 | 105,922,647 |
| 大檔自適應 gzip level 1 | 12.525 | 13,468,066 |
| 自適應＋集中目錄 fsync | 13.893 | 13,468,066 |

樣本約快 4.06 倍、冷資料少 87.28%；只有兩次交錯測量，沒有清 OS cache，背景
服務仍在運行。這是冷封存／驗證／恢復流程的觀察，不是全 519 GB、網路或遠端
刪除速度的保證。集中目錄 fsync 沒有一致優勢，正式政策保持 `false`。
私有證據：`artifacts/operations/vast_storage_safe_cleanup_20261004/cold-archive-profile-comparison.json`。

既有回傳政策現在選擇 `archive_compression=gzip-1-adaptive-over-8m`：
8 MiB 以上的檔案不靠副檔名猜測，先在有界 C scratch 做完整 gzip-1；只有實際
縮小超過 10% 才採用，否則保留 raw。CSV／二進位都是原值 SHA，不改數值、表格、
模型或 dtype。每檔仍完整解碼核對，恢復保留原名、mode、mtime、空目錄。
Gzip 使用固定空檔名／mtime=0；參見 [Python 官方說明](https://docs.python.org/3/library/gzip.html)。

既有已校驗的 encoded receipt 先原樣重用，不重壓 D 歷史、不刪舊冷物件；因此
已開始的 23 GB 舊 raw 批次不會突然享有同樣壓縮收益。新檔／新批次才使用新選擇。
每次嘗試另外保存 `attempts/` 收據，重新嘗試不能抹掉上一輪錯誤。
主 worker 已在無 ingress lock、無 child 的停止點交接新政策；子分區若還在
交易中，交接程序只等待同樣的安全邊界，不會中止它。

```bash
source scripts/runtime_env.sh

# 原清冊接手；policy 已選自適應，既有 owner 存在時拒絕第二個 writer。
run_fintech_python scripts/return_remote_legacy_archives.py archive --apply --order reclaim-first

# 明確選冷編碼；傳輸編碼是另一個參數，不影響原值或 SHA。
run_fintech_python scripts/return_remote_legacy_archives.py archive --apply \
  --order reclaim-first --archive-compression gzip-1-adaptive-over-8m

# 真實樣本完整冷流程比較；只讀來源，使用新建私人 C/D 測試目录。
run_fintech_python scripts/benchmark_legacy_archive_workflow.py \
  --source /ABSOLUTE/STABLE/READ_ONLY/SOURCE --file EXACT_FILE.npz \
  --rounds 2 --output /ABSOLUTE/NEW/MEASUREMENT.json

# 查看在途階段，不把正在寫 D 的容量當成已驗收／已回收。
jq '{archive_compression,current_root,counts,cold_verified_source_bytes,remote_reclaimed_bytes}' \
  /var/lib/stockagent-vast-legacy-return/summary.json
```

`handoff_remote_legacy_archive_worker.py --help` 是明確 PID／cohort 的受監督升級工具。
先核對 PID、run 與來源；只會在確認沒有共用鎖及子程序的等待點，凍結並再次核對
後交接。新 worker 沿同一 task／cohort；不修改遠端 Git、不複製 identity，不結束
服務或在途資料程序。不能用它任意終止 Python 工作。

持久化候選仍要求所有物件資料與目錄持久化成功才寫 manifest／head；
任何 fsync 失敗阻止發布。檔案與目錄的 fsync 是不同條件，參見
[Linux fsync 手冊](https://man7.org/linux/man-pages/man2/fsync.2.html)。正式預設未啟用候選。

本次編碼／格式／掃描／使用中／回收共用測試 204 passed；最後聚焦測試
75 passed，包含交接的持鎖／child／不完整 `/proc` 觀察拒絕，以及重試保留舊錯誤
收據。`/proc` 的暫時 fd 消失不代表資料 worker 已結束：第一次子分區交接觀察因此
安全退出，原 worker 沒有收到停止信號；修正後的受監督觀察繼續等安全邊界。
原失敗紀錄保留並註記 superseded，不當作資料已完成。性能樣本不是全量同步完成
證據，全部資料仍持續逐項處理。

成功測量 run：`real-cold-profile-comparison-fin-20261004T065852-c87226a2`。
共用回歸 run：`cold-performance-shared-safety-r-20261004T065853-c31e8dc2`。
最後聚焦 run：`adaptive-handoff-final-regressio-20261004T071042-71966fb8`。
主批次 run：`adaptive-complete-root-return-20261004T070356-b4b341e2`。
子分區安全交接 run：`adaptive-partition-safe-handoff--20261004T071136-7ea61326`。

本日初始流程及既有 legacy／packed／materialized 關聯測試：109 passed；加速修改後的
共用回歸結果見上節。
證據：`artifacts/operations/agent-workflow/runs/final-return-guards-fixed-20261004T041916-d6535a07/`。
涵蓋原值／mode／mtime 改變、未知檔案、空目錄、共享 inode、失效 ACK、合併衝突和
post-quarantine 傳輸競態。工程測試不替代實際 404.75 GB 回傳及逐項 D 恢復驗收。
另有完整原始清冊、主批次 dry run、兩大根的守恆分區／dry run、實際 pilot、主批次
及子分區 apply 的獨立 task/run 收據；原失敗測試收據保留，修正 umask 測試隔離後
以成功關聯測試收據註記 superseded，不更改原失敗狀態。
