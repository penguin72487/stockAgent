# 服務資料與產物分離：2026-10-31

使用者指定 `artifacts` 只放產物，服務狀態另放獨立資料夾；不可變原始資料、
封閉備份及還原副本由 D 冷庫保存。可變資料庫、交易帳本、鎖與控制 journal
保留在 WSL 原生檔案系統，避免把原生鎖與 SQLite／PostgreSQL 的寫入路徑直接
改成 NTFS。這是依讀寫行為分工，不能把 `/var/lib/stockagent` 整根當成冷檔。

| 用途 | 新位置 | 本次狀態 |
| --- | --- | --- |
| 當沖、隔夜帳本、追新狀態、監控快照 | `service_data/live/` | 已原子搬移；原帳本鎖、檔案 inode 和服務程序保留 |
| 服務及工作快取 | `service_data/cache/` | 已原子搬移；不是清除快取 |
| Discord 狀態與服務紀錄 | `service_data/discord_bot/` | 已原子搬移；未重啟 bot |
| 原 `/srv/stockagent-live` 工作集 | `service_data/stockagent-live/` | 已原子搬移；未複製另一份資料 |
| `artifacts/operations` 控制與工作 journal | `service_data/operations/` | 現行 owner 使用中；已安裝下次啟動前的交接 |
| 原 `/var/lib/stockagent` 控制及恢復工作區 | `service_data/stockagent/` | 現行 owner 使用中；已安裝下次啟動前的交接 |
| 不可變 raw、封閉來源及已驗證冷版本 | guarded D packed／immutable lake | 沿既有發布、完整恢復與退休 owner；本次不冒稱已把控制樹中的舊副本全數移入 D |
| 訓練結果、報告、驗收證據、封閉版本產物 | `artifacts/` | 保留；資料內容與固定 manifest 不改寫 |

路徑均相對 `/root/stockAgent`。`service_data/` 已排除於 Git；程式、設定及
必要測試留在 Git。`artifacts/storage_layout/` 是本次操作的產物收據，並非
新的服務狀態目錄。舊相容入口與新目錄指向同一批 inode，不能相加當成兩份
資料，也不能把移動目錄宣稱為釋放磁碟空間。本次搬移回收量是 **0 bytes**。

已完成的四組搬移保留舊符號連結，讓舊版程式、排程及固定資料引用繼續讀取。
`stockagent.service_layout` 只接納精確登錄的相容目標；不同的既存狀態根會拒絕
合併，不會產生第二份空帳本。快照及備份讀取仍拒絕未登錄的連結、葉節點
或下層目錄的任意重新導向。當沖／隔夜啟動器及服務快取已使用共用解析器。
新節點的預設工作快取也使用 `service_data/cache`；尚未交接的舊節點會先沿用
原快取 owner，避免新增第二份。固定歷史搬移計畫的舊 namespace 與 identity 保留。

**正在使用的控制樹不在線上強搬。** `stockagent-service-layout.service` 已安裝，
94 個現有 StockAgent unit 的下次啟動要先經過它；既有服務本文及當前程序不
重啟。它必須確認沒有其他 StockAgent service owner 和控制路徑的程序引用，
才交接兩組控制樹。活躍 owner 會記錄 deferred，保留原工作與路徑。
原退休排程鎖忙碌時，放置流程也只記錄 deferred，不會因此阻擋當沖服務啟動。

控制樹搬移採同檔案系統原子交換，並在服務啟動前把舊入口設成原生 bind mount。
既存 controller 繼續使用原生舊介面，保留外部相對連結、FIFO／socket、私有
權限檢查、既存 SQL 檔案及鎖的語義。這種搬移沒有複製或刪除 payload；以
原始子樹 inode 不變與 mount topology 為放置證據，不為改名重新掃描、雜湊
數百萬個未改動檔案。這不是 payload 恢復驗證，也不授權回收任何檔案。

每個控制樹先持久化原始 inode、目標及專用空掛載點 intent，交換後才掛載。
掛載失敗則原子恢復原目錄；中斷或重新開機時只依原 intent 的精確 inode
重掛，不覆蓋獨立目錄。未能判定的名稱保留並報錯。兩組控制樹目前仍未完成
正式遷移，下一次實際啟動仍需核對它們的放置收據及原 owner 恢復結果。

原操作入口：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/organize_service_storage.py             # 清點／dry run
run_fintech_python scripts/organize_service_storage.py --apply     # 活躍控制樹會暫緩
systemctl status stockagent-service-layout.service
```

`--boot-handoff` 給既有開機 unit 使用，不會停止服務或強制取得別人的鎖。
原子放置與回收排程共用原 retirement-wave lock；它不取代來源發布、NAS、
完整冷恢復及原退休 gate。D 原始資料和 NAS ACK 均未由這個放置操作改寫。

WSL 與 C 槽的空間不是同一種數字。WSL 的 `df` 是 ext4 客體虛擬容量，
Windows C 是 VHDX 所在實體磁碟的餘量；其他 Windows 檔案也會改變 C 的數字。
調整虛擬上限不能讓兩者永久相等，不應反覆縮小持有活動檔案的 ext4。
Microsoft 說明虛擬磁碟可呈現大於實體可用量的空間：
[WSL disk space](https://learn.microsoft.com/en-us/windows/wsl/disk-space)。

面板的 WSL 卡片現在優先顯示：

```text
保守寫入餘量 = min(近期 WSL f_bavail, 近期 Windows 所在磁碟可用量)
```

這不是可寫總量的精確上限：沒有另外估算 VHD 內已配置區塊的重用量，亦未扣
各應用程式的保留空間。保留原始容量、系統 reserved blocks、使用率與量測
時間；任一端缺少近期實測就顯示未知，不回退成數百 GiB 的虛擬餘量。舊的
分類清冊若早於服務搬移，會標為搬移前的歷史清冊，待原排程重新清點。

10-31 本次觀測 C 約只剩 14 GiB，而 WSL 客體約剩 211 GiB。這些是不同
時點的觀測，後續仍有寫入；不是靜態保證。VHDX 已釋放的 ext4 區塊也不
代表 NTFS 已回收到相同大小，故需要離線 compaction 另行量測。

已準備 Windows 原生固定 SHA worker 與入口：

```powershell
# 在 Windows「系統管理員 PowerShell」執行；不帶 -Apply 先列計畫。
& "$env:LOCALAPPDATA\StockAgent\WslStorage\compact-current.ps1"
& "$env:LOCALAPPDATA\StockAgent\WslStorage\compact-current.ps1" -Apply
```

`Compact` 沿既有單一 Windows maintenance owner，保存原排程狀態、sync／trim、
正常關閉已登錄 WSL、read-only attach／Quick compaction、恢復排程並驗證原
boot recovery；**不 resize、不更改原虛擬上限、不開啟 unsafe sparse**。
這會暫停 WSL 服務；請在允許維護的時段執行。未指定的其他執行中 distro
仍會阻擋全域關閉，不能自行替使用者擴大停機範圍。Expand 的原 32 GiB
growth reserve 不變；無擴容的 Compact 只為維護 metadata 保留 1 GiB，未
變更下載、驗證或服務的容量保護。當前 WSL 沒有 Windows 管理員 token，
所以本次未執行離線壓縮，沒有宣稱已回收 C 空間。

本次放置與隔離驗收收據：

- `artifacts/storage_layout/20261031-dry-run.json`
- `artifacts/storage_layout/20261031-placement.json`
- `artifacts/storage_layout/20261031-discord-placement.json`
- `artifacts/storage_layout/20261031-native-bind-acceptance.json`

原生 bind 驗收在 private mount namespace 的臨時目錄完成：原檔 inode、已開啟
descriptor、外部相對連結、FIFO 保留，重入及模擬重開後重掛通過。它不是
正式控制樹已遷移的證明。當沖與隔夜的現行 `.engine.lock` FD 已核對仍是
原 inode，程序未重啟，帳本完整性回報 ready 且 divergence_count=0；既有
慢的完整 HTTP 狀態查詢曾在搬移前後逾時，沒有以 HTTP 200 取代業務驗收。

驗證包括 315 個聚焦回歸通過、2 個跳過；後續路徑整合的 95 個測試通過。
CPU 瀏覽器驗收 11 個情境通過，含桌機／手機、展開收合、資料過期、缺少實體
磁碟量測與搬移前清冊標記；沒有新增 provider API 呼叫。線上讀取驗收記錄在
`artifacts/storage_layout/20261031-live-api-acceptance.json`，瀏覽器報告在
`artifacts/storage_layout/browser-20261031/browser_acceptance.json`。
