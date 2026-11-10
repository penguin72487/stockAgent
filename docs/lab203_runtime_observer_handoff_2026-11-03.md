# lab203 每分鐘運行觀測交接

## 2026-11-04 00:04 台北：v2 原始碼審核與原就緒檢查

Penguin 的原 code-only 交接於 11 月 3 日 23:52:22 驗收，新儲存 worker
PID 282975，61 個已驗收固定來源及原 workflow chains 保留，PostgreSQL／
Temporal 與當沖／隔夜未重啟。這不是 lab203 或 NAS 的驗收。

新版 `tools/storage-operations-20261103-v2/` 預定沿原控制包通道交付，包含
本文件、C/E/D 報告，以及 SQL digest 的四份原始碼／測試；保留原 v1。
這是六個成員的原始碼審核包，不是完整 runtime 或自動部署程序。封套自身仍須
由原 verifier 核對 READY、精確清冊、每檔 SHA 與固定 identity；實際 identity
見來源發布收據及到達的封套，不能把這段預告當成接收或 NAS ACK。

1. 既有 CPU／RAM 回執持續更新，不用重新安裝主機 observer。仍請原 backup
   owner 回傳真實 start-pre／job／repository／Restic lock 狀態與新的 readiness；
   目前來源最後接受的正式 readiness 仍為 11 月 2 日 12:49:56。
   歷史 01:14 鎖診斷不代表現在仍持鎖；原就緒與容量檢查通過後再發新回執。
2. 回報私有 cold 的逐批獨立讀回與原始範圍；來源仍是
   718,647,796,617／817,226,447,345 bytes，差 98,578,650,728 bytes。
   已登錄一般 cold 的覆蓋另計，不能將兩個可能重疊的分母相加。
3. 若原生 catalog restore 的寬 metadata 排序也是瓶頸，可審核 v2 中的共用
   `_metadata_rows`／`_registered_table_digest`。來源 `stockagent/control/lakehouse.py`
   固定 SHA 是 `6ea874613bff3473b18db2e0515772fc6ac73ae18a7f6ccac71a89070de04158`。
   實際 snapshot 1049 的全部 10,248 列、3,909,791,481 原內容 bytes，原完整
   `ORDER BY ALL` 與新版的全 SHA 均為
   `e0ce8e232d9b163400a8b36746b3cabf0ca9795da17ce0e46b7f5eee7fee20fa`。
   新版先確認 SQL 排序鍵真的唯一、目前 SQL／host／cgroup 記憶體足夠，再做
   單次排序；舊環境或觀測失效維持原分頁。接收端須固定自己的 verifier 來源
   與契約後再採用；來源測試不能取代接收端還原，也不解除 TW 原 runtime gate。

## 2026-11-03 17:05 台北：沿原固定控制包接手

Penguin 在 17:03:35 完成一組舊 FinLab preparation 的 E 搬移：1,456 檔、
42,125,547,602 logical bytes，WSL 回收 42,137,661,440 allocated bytes。
原 FinLab source、D CAS、NAS 與歷史 recovery receipts 保留；這不是 NAS ACK。

本文件及 C/E/D 佈局報告由既有 `publish_backup_handoff.py` 封裝至
`tools/storage-operations-20261103-v1/`；只含兩份公開 runbook，不含原始
資料、credentials、installer 或自動執行指令。請按原 verifier 核對封套
identity、READY、精確檔案集合及每個 SHA，再由原 owner 接手。

17:05 主機心跳仍按分鐘更新，正式 readiness 仍為 11 月 2 日 12:49:56，
私有備份尚有 98,578,650,728 bytes 等待。歷史 Restic 鎖診斷不能當作當前
鎖狀態。請原 owner 回傳以下實際結果，沿原反向通道，不必重貼全資料清冊：

1. 原 `lab203-backup.service` 當前 start-pre／job／repository 檢查階段、
   原始失敗類型及下一次實際重試時間，不能只回 service active。
2. 在合格工作邊界執行原就緒檢查並發新固定 readiness；若長批次一直沒有
   邊界，評估在原逐批工作間合作讓出／重取自己的 owner。不另起 writer、
   不強殺 NAS syscall、不刪未知 Restic lock，不以 `--no-lock` 或改舊時間免驗。
3. 回傳最新 readiness identity 或具體阻擋證據、逐批 NAS 獨立讀回進度；
   下方精確 CAS 缺件仍依 SHA／長度回覆固定查找結果。

## 2026-11-03 13:06 台北：來源解壓力，接收端仍待原 owner 就緒

Penguin D 舊傳輸副本已依原完整 E 同名讀回及退休 gate 回收
137,066,186,602 logical bytes、44,095 檔；162 個變動／缺件名稱保留，D
主冷庫／NAS 原件未刪。13:04 D 實體可用 117,831,557,120 bytes；E
工作卷可用 857,518,100,480 bytes。這些只說明 Penguin 當時空間，不能
代替 lab203 的 capacity／readiness 或固定批次 ACK。

Penguin 持續收到約每分鐘的新 lab203 主機與容量觀測，13:03:35 的量測正常；
原 readiness 資料時間仍為 11 月 2 日 12:49:56，私有冷庫覆蓋仍是
718,647,796,617／817,226,447,345 bytes。請原 backup owner 依目前進程、
Restic repository／lock／前置檢查重驗，回傳新固定 readiness，或明確阻擋
階段、原始錯誤及下一次重試時間；逐批 NAS 獨立讀回仍沿原通道。
不用為這次交接重新安裝 CPU／RAM observer，也不只刷新舊 readiness 時間。
下方精確 CAS 缺件的唯讀查找仍待固定結果，保留全部原始證據與接收保護。

## 2026-11-03 10:24 台北：Penguin E 交接已完成，請原 owner 更新備份就緒

Penguin 已將 10 個舊 mount namespace 交接到相同 E transport alias；原 sender
及 peer 身分不變，未重新建立備份管線。D 固定傳輸副本的完整 E 讀回及逐檔
回收已執行，10:24 累計回收 31,310,777,433 logical bytes。這不是 WSL 釋放、
NAS ACK 或接收端刪除授權；舊差異、來源、D CAS 及 NAS 原件保留。

最新收到的 lab203 主機／容量心跳為 10:16，CPU／RAM 可讀，但原 readiness
仍是 11 月 2 日 12:49:56，私有備份等待有效就緒。請沿下方原 owner 流程核對
Restic 鎖與前置失敗，回傳新 readiness／失敗原因／實際重試時間及逐批 NAS
獨立讀回進度。不要只更新時間、移除未知鎖或以心跳代替備份驗收。
若已自行恢復，請提供新的固定回執 identity；下方精確 CAS 缺件查找仍待回覆。

## 2026-11-03 08:40 台北：請原 owner 核對就緒與備份進度

Penguin 已收到 08:40:35 的配對 owner 觀測，該時點備份單元處於
`activating/start-pre`，原 owner 觀測為 `held`；先前單元狀態也曾出現
`failed`，目前不能據此認定仍失敗或鎖已失效。回執中的上一個結果為
`main_exit_code=75`、`outcome=deferred`，單元結果欄位為 `success`；請由
原 owner 依該次 unit journal、前置檢查及當前進程／鎖判定實際原因。

原 readiness 的資料時間仍是 **2026-11-02 12:49:56 台北**，觀測器每分鐘
更新不會改變 readiness 有效期。請沿原流程完成實際就緒檢查、回傳新的固定
readiness 與逐批 NAS 獨立讀回；若仍失敗，回傳明確失敗階段、原始錯誤及
下一次重試時間。請勿強制解鎖、改寫舊收據或以狀態心跳冒充 NAS 驗收。
原先要求的缺失冷庫封包唯讀查找仍在下方，若已完成請回傳固定 identity。

## 2026-11-03 01:17 台北：已收到與尚待處理

已透過既有回執收到 `lab203-host-runtime-live-handoff-v1`；接收端固定 release
報告 25 項本機測試通過，其 `storage-status.json` 在 01:16 已包含 CPU／RAM
實際取樣。Penguin 依原 readiness 配對並驗證回執 identity 後顯示各次量測時鐘；
不再要求重做這項交接。這些觀測不是 NAS 的批次還原 ACK。

新的 `lab203-restic-exclusive-lock-diagnostic-v1` 固定回執於 01:14:43 報告：
原備份 guard 遇到 Restic 排他鎖、返回碼 11；該次看到的 PID 不存在，仍不足以
證明鎖過期。01:16 每分鐘 owner 觀測仍指向舊 readiness 並等待原 owner 重查。
請 lab203 的原 backup owner 核對該鎖的來源與目前實際持有者、工作是否仍在、
是否可依原 Restic 程序釋放；恢復後重新執行原就緒檢查，回傳各自時間的診斷與
readiness。**不要僅因 PID 不存在便刪鎖**，也不要改寫既有 NAS ACK 或跳過原
容量／資料驗證。排程與 observer 的每分鐘 heartbeat 不是新備份驗收證據。

下方「唯讀查找一個冷庫缺件」仍待接收端依精確 SHA 查詢。若已回報，請提供
固定版本的回執 identity；不要只憑路徑或檔名判定命中。

目的：沿既有配對、簽章的 `storage-status.json` 回傳接收端主機 CPU／記憶體，
讓 Penguin 的儲存面板使用**該次取樣自己的時間**。容量及各份 NAS／原始資料
驗收仍使用原有回執；本觀測不授權刪除或表示備份完成。

Penguin 工作樹已實作接收與產生邏輯，來源固定 SHA-256：

| 原始程式 | SHA-256 |
| --- | --- |
| `stockagent/data_sync/storage_monitor.py` | `1b8cec8fe31290e75c0bfb872591507a7c29083b2da0f9a60f5b5ec2e930f285` |
| `stockagent/data_sync/storage_runtime.py` | `943dda15cd695aaef94096eb424323c67b6181889622a7709558888cf5ef1d3c` |

這兩個 SHA 指可審核的原始碼，**不是已抵達或已安裝的封閉套件**。目前 D → E
既有傳輸工作正在核對固定來源世代，不要直接往其輸入樹增加控制檔；本交接
此段為交接前原計畫，後來已由 lab203 接收端自行實作，如上方最新狀態。由原 owner 採用等價功能或在現有交接管線建立新的
版本化、逐檔驗證套件，再於原 observer 空閒時交接。不要覆蓋已封存 v1 套件，
也不要改 backup owner、門檻、pin、retry、NAS 內容或直接清理 ingress。

## 接收端實作契約

每個既有 60 秒觀測週期以 `/proc/stat`、`/proc/meminfo` 取樣；CPU 百分比使用
同一次開機前後兩個累積計數差，首次取樣、重開機或取樣失效時省略 CPU 百分比，
不填 0。記憶體 `used = MemTotal - MemAvailable`，單位 bytes；CPU 數來自
實際 CPU affinity。只在既有簽章的 `storage-status.json` 簽章**之前**加入：

```json
{
  "host_runtime": {
    "contract": "stockagent.storage_host_runtime.v1",
    "observed_at_utc": "實際主機取樣時間，ISO 8601 UTC",
    "metrics": {
      "cpu_percent": 0.0,
      "cpu_count": 1,
      "memory_used_bytes": 1,
      "memory_total_bytes": 1
    }
  }
}
```

範例值只表達欄位型別，不是現場數據；不可用範例值代替真實量測。delta 的累積
計數、boot ID 只留在 observer 程序記憶體，不進回執。若量測失敗，保留缺失欄位，
不得為了刷新收據時間重寫先前數據的取樣時間。不得暴露帳號、金鑰、路徑或程序命令。

請沿既有反向管道回報版本與成員 SHA、安裝程序的 verifier 結果、連續兩筆已簽章
回執的各自時間、是否取得 CPU／RAM、observer 服務身分、原 backup owner 身分，
以及有無失敗；不要將狀態回執稱為 NAS 還原 ACK。Penguin 只在原 `readiness.json`
配對與回執驗簽通過後接收，白名單投影數值與取樣時間；超過 180 秒會顯示過期。
尚未升級的舊回執仍可讀，CPU／記憶體顯示未知。

## 唯讀查找一個冷庫缺件

Penguin 正準備回收一份約 5.25 GiB 的舊 `tw-public` 部分還原副本，但本機
精確快照檢查缺少下列 CAS 封包，因此尚不能證明原資料可還原，也沒有刪除它：

`objects/packs/03/039395ce89379b80a370289ccbf4def9291f5c862f361d43f5872390b229c75f.zip`

請原 backup owner 依現有 catalog／snapshot 索引**唯讀**查找這個精確 SHA，
若找到，回報來源 snapshot／封套 identity、實際長度與獨立完整讀回 SHA；若未
找到，回報已查的範圍與尚未查的範圍。這是一個另外發現的封包，不假設與先前
114 個歷史缺件是同一筆。請保留原 NAS／來源及其驗收程序，透過既有回執通道
回報；不要因這個查找自行退休或刪除任何資料。

Penguin 的本地驗證涵蓋 CPU 差分與重開機、接收端不執行系統服務探測、私有
狀態不外送、舊回執相容、錯誤契約／未來時間拒絕、舊取樣不被新收據假裝刷新，
以及同一容量回執的簽章；lab203 版本與新回執證據見本文頂端。
