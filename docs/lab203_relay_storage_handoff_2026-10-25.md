# lab203 Codex 接手：有界中繼與 NAS 冷區

來源工程任務：`storage-single-authority-20261025`。Penguin 沿原
`stockagent-backup-ingress-lab203`／`stockagent-backup-receipts-lab203` 通道工作；
不新增公開入口，不要求密碼或另一組傳輸帳號。

接手文件、完整 repo skill、相關 storage／runtime／Git contracts 及唯讀 observer
可由既有 ingress 的 `tools/storage-relay-20261026-v1/` 固定包取得。
先用既有 delivery verifier 核對 `backup-envelope.json`、`READY` 與每檔 SHA，
再閱讀包內同名文件；是否已到達以 lab203 實際讀回為準。此包不會自動執行，
不替換既有 worker，也不含帳號、憑證或來源 payload。

Penguin 在 2026-10-26 01:55 台北時間觀察到的 receiver readiness 仍為
2026-10-25 10:57:20 UTC，超過 15 分鐘新鮮度要求。請先查原 worker 與反向
receipt 通道；不要由這份舊回執推論 NAS 目前容量或接收能力。
本機 C／WSL 容量也影響 Syncthing 掃描，Penguin 正另行處理，不要求 lab203
降低保留空間或略過 ACK。

以下整段可交給 lab203 的 Codex 執行：

> 請先讀當前節點的 AGENTS.md、完整 `stockagent-storage-operations` skill 及
> 目前 backup pipeline 的設定。Penguin 要求 lab203 為中繼，不永久保留整理後
> payload；NAS 是獨立備份。沿用既有 worker、owner.lock、immutable archive、
> Restic、paired ACK 與反向 receipt folder，勿另開第二個 backup owner。
>
> 1. 先唯讀核對當前 service／timer、pipeline 版本、ingress/state/cache/scratch
>    與 NAS exact roots。以 `du -x -B1` 的實際 allocated bytes 清點（同磁碟同次
>    scan 去除 hardlink 重複），列出每種 payload 的產生者、消費者、在途與已 ACK
>    數量。2026-10-25 舊回執曾顯示 state/cache 約 127.6 GiB、ingress 約 35.8 GiB；
>    這是線索，不能當作可刪清單。
> 2. 找出哪些 cache 是已送達 NAS 的 byte-for-byte 重複、哪些是唯一原始資料、
>    哪些仍供正在跑的 Restic／rclone／驗證使用。對已完成 exact cohort，以 NAS
>    原值完整還原、每檔 SHA、manifest／pairing、程序 FD/mmap、pin／lease／傳輸
>    及 quarantine gate 產生精確 retirement plan，先 dry-run。通過後只 unlink
>    該 plan 的精確中繼名稱；不按目錄名稱或年齡批刪。
> 3. 修正既有 relay 的收據及 GC 邊界：NAS ACK 先持久化，後續有界 GC 不阻塞
>    下一批接收；設定在途 byte ceiling、scratch ceiling 與磁碟 reserve。未 ACK、
>    failed、unknown、被引用的內容保留；容量不足暫停接收，不能刪 NAS 版本。
> 4. 唯讀辨識 NAS 實際底層檔案系統／compression 能力。CIFS mount 只證明共享
>    協定，不能推斷底層是 Btrfs／ZFS 或已開壓縮；若管理端無法查到就記 unknown。
>    先用小型、固定樣本及 SHA 讀回量測，不在整個 NAS 上遞迴重壓或 prune。
>    NAS 可沿 immutable CAS 去重；增量→穩定冷區每日只整理新 cohort。
> 5. 續訓 recovery context 需保留 model/optimizer/RNG/config/source/runtime 證據；
>    目前 D 的 `.local-state/training-rescue` 是私有資料，不能自行擴大 Syncthing
>    或把 secrets 明文放到 archive。請核對既有加密備份 owner 如何接入，和 Penguin
>    對齊固定 recovery capsule 清單後，以獨立 restore proof 驗收。
> 6. 透過既有 paired reverse receipt folder 回傳 `storage-relay-audit.json`，以及
>    現有 observer 的 `storage-status.json`。audit 包含 observed_at_utc、pipeline
>    identity、phase、各種類 allocated bytes、pending/ACK bytes、實際回收 bytes、
>    NAS 還原 proof 摘要、compression 能力/unknown、當前障礙與下一個 owner。
>    不把 IP、UNC、帳號、密碼、金鑰或原始數據寫進公開 projection。若新增收據
>    格式需版本及 deterministic checksum；不要改動既有嚴格 backup ACK ABI。
>
> 請回報精確 scope、程式碼變更、測試、service readback、實體回收 bytes。不要
> 把 rclone/Restic exit 0、Syncthing idle 或容量足夠當成獨立還原已通過。

使用者已要求本次架構實作與安全回收；不需再詢問是否要開始。但會刪唯一資料、
改 NAS 歷史保留、重建 archive 或變更尚未確認的來源授權時，應先保留並回報。
Penguin 端會繼續完成本機與 Vast 工作，不等待此回覆才開始。
