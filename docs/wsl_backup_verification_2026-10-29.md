# WSL 回收與備份驗證：2026-10-29

本頁為 2026-10-29 約 01:06 台北時間的觀察，並非所有資料已備份、
可回收或達到硬體極限的驗收。持續任務沿用
`storage-single-authority-20261025` 與原有 owner。

## 目前進度與等待關係

- Round 7／8 先前的兩份 `retirement.json` 實際記錄刪除並回收
  14,506,016,768 和 13,010,243,584 allocated bytes，合計約 25.63 GiB。
  Round 9、6、10、11 的固定候選約 39.90 GiB，實際可回收量仍須完整
  還原、同步、程序引用、硬連結及逐輪最終 gate 證明，不可當成已釋放。
- Round 9 的原 owner 仍在 `publish-under-shared-owner`，進行 D 封包寫入；
  00:38 左右第 2 個 partial 約 52 MiB，00:51 為 121,080,597 bytes；
  01:00 已轉為寫入第 3 個 partial，01:05 約 67 MiB。
  此值不是整輪完成百分比。Round 6／10 位於同一排程，Round 11 等待該
  排程鎖。現有程序從舊映像繼續；後續子程序才載入既有的 D 小檔預讀
  與進度記錄，不能把新程式當成已作用於 Round 9。
- 獨立的 `tw-public` 固定版本在 01:06:26 進行 `verify-cold-objects`，
  10,727,723,818／35,976,707,808 bytes（29.8%）、968／4,112
  objects（23.5%）；該階段 `acceptance=false`。約 00:38 至 01:06
  的速度約 1.77 MB/s，如維持此速度，僅該階段約 05:00 至 05:10
  完成；這不是
  完整重建、回收或 NAS 的 ETA，負載或物件分佈改變都會影響速度。
- 新聞固定計畫已有 22／30 波本地封閉發布，私人 immutable 備份持有
  原 backup-stream owner，新聞等待這把鎖；新聞先前已在波次間讓出鎖，
  私人備份也有後續波次推進。這不是 Round 9 的發布鎖，不能把兩者混為
  同一把鎖，也不能因工作等待就判定死鎖。兩者尚無全範圍 NAS 還原 ACK。
- WSL `df` 約 316 GiB 可用，Windows VHDX 實體承載空間在 01:04 的
  canonical preflight 為 76,401,324,032 bytes（約 71.2 GiB）；D 約
  380 GiB。兩種可用量不是同一數值；一般下載預檢的 64 GiB 是當時的
  admission threshold，並非已保留空間。當沖服務運行中，維持既有較高
  CPU／I/O 優先權，不因本次備份調低其資源上限。

## 同一封閉交付的驗證最佳化

實際瓶頸之一是 D／WSL 小檔 metadata 往返與 Windows PowerShell
批次啟動。取相同封閉來源，小檔 64 筆完整 SHA 的 WSL／原生兩次
耗時分別為 8.276／4.559 秒及 41.824／30.032 秒；32 個 1 MiB 檔
分別為 4.563／3.506 秒及 12.970／15.352 秒。數據是四 worker、
唯讀 A/B/B/A，見 `news-closed-small-sha-abba-20261029.json`、
`news-closed-1m-sha-abba-20261029.json`。

`scripts/verify_backup_delivery.py` 對啟用 native D I/O 的交付，將不大於
1 MiB 的成員送入同一 worker pool 的直接讀取，其餘維持原 native 路徑。
這是基於兩端實測對中間大小採取的政策，不是每一種檔型與硬體的最佳值。
native 與 portable 任務共用既有 worker 上限；空閒 worker 可以在最後
一個原生批次仍執行時驗證小檔。所有成員保留完整 SHA、bytes、封閉
envelope／READY、exact set、前後檔案身分與來源版本檢查。直接路徑另
以 no-follow fd、fstat 檢查開啟前後身分及 mode／link count，並在驗證前後
核對已登錄 D 掛載、ingress 與根目錄身分；原有對外五欄 signatures ABI
不變。單純監控或交付驗證仍不是 canonical reconstruction 或 NAS ACK。

同一真實封閉交付，509 檔、465,378,271 bytes，四 worker，初版分流
A/B/B/A 為 84.069／126.090／89.071／68.474 秒；所有檔案內容 SHA、
envelope、exact set 和回傳 signatures 一致。加入最終 D guard 及混合
排程後，另一次同交付完整驗證為 72.735 秒。比較期間其他工作同時執行，
波動明顯；不能由此推算整輪回收或宣布已達硬體上限。證據為
`news-closed-full-delivery-abba-20261029.json` 及
`news-closed-full-delivery-final-20261029.json`。上述 guard／混合排程版
在實際 backup role 的 Python 3.12.14 下，也對同一交付完整讀回，耗時
57.530 秒，記錄在 `news-closed-full-delivery-role-20261029.json`。
營運 worker 的既有程序不熱替換，需以後續真正執行的波次確認採用。

整批測試在修正 Windows ctime 與同批失敗取消後，對 Windows adapter、
backup native delivery、offhost、news、relay、stream、private cold、
handoff 與 transport cache 為 **304 passed、14 skipped**；後續加入
私人失敗狀態與原生批次總大小界限後，相關 shared-contract 回歸為
**213 passed、12 skipped**，最終三個受影響測試檔為 **118 passed**。
測試涵蓋臨界大小分流、同長度毀損、符號連結替換、掛載變更、混合工作
重疊、失敗取消與保留原始錯誤。跳過項目不能當成遠端 NAS 驗收。

接著以 Windows 原生 Python 3.13 對同一真實交付測試時，第一版因路徑
`lstat` 與 `fstat` 的 `ctime` 不同而錯誤拒絕；該嘗試沒有產生驗收收據。
已修正 Windows 的跨路徑／描述元比對：只比較兩種 API 共同表示的
device、inode、大小、mtime、mode、nlink；路徑及描述元各自的前後
檢查仍保留 ctime。Windows 3.13 與 3.14 重測逐檔完整 SHA 與 exact set
均通過；最後版本在 Windows 3.14 再次通過，見
`windows314-backup-verifier-readback-final-20261029.json`。實際 backup
role 的修正版另一次完整讀回同一 509 檔交付，耗時 44.618 秒，見
`news-closed-full-delivery-role-v2-20261029.json`。這些僅是傳輸讀回，
不是 NAS 接受或完整還原。

## 失敗的驗證工作與共用鎖

私人備份有一批原生驗證已失敗，但原 `ThreadPoolExecutor` 等待另一個
唯讀 Windows SHA 子程序結束，而該子程序已等待約 501 秒。確認其 parent、
PID、唯讀 hash 呼叫與主執行緒等待狀態後，使用 pidfd 僅向該子程序
送出 SIGTERM；未終止備份 owner，未刪來源或暫存。此動作見
`private-read-only-hash-unblock-20261029.json`。owner 隨後記錄原生批次
退出失敗、254 筆重試及保留的 staging，並開始處理下一波；01:04 此
週期已在本地發布 2 波並繼續處理第 3 波，原 owner 仍在工作，新聞
仍等待該 owner。失敗資訊不足以判定特定檔案或底層 Windows 錯誤。

再由固定 backup role 對同一保留 staging 的 254 檔、535,167,960 bytes
進行逐檔完整 SHA 與 exact set 讀回，75.066 秒通過，見
`private-failed-stage-readback-20261029.json`。這次唯讀診斷既未發布，也
未驗收來源還原或 NAS；一次成功不能反推原錯誤原因。

對後續新啟動的驗證程序，若其中一個平行驗證先失敗，會通知同批其他
唯讀 native SHA 子程序及早結束並回報最先的實際錯誤；不取消複製或
任何寫入。未發生同批失敗時，原有 I/O 期限不變。原生分組也同時遵守
16 個成員及 16 GiB 總量限制。私人備份會在進入下一次容量盤點前先寫
下失敗狀態；新波次進行中保留 `last_failed_wave`，但不把舊錯誤當成
新波次錯誤。進行中的原程序不熱替換，需在下一輪自然啟動後確認採用。

清理方面，本次沒有刪除未驗收的專案來源、D 冷物件、NAS 快照或共用
鎖。`/var/tmp/stockagent-sourceplan-tests-20261028` 約 603 MiB，雖含多組
合成 pytest fixtures，原次執行中止且缺少完整結束證據，暫不把它當成
已完成可丟棄測試清理。另需保持原已交接但未完成的七項根目錄備份
登錄、19 個工作樹事故恢復及歷史物件調查；見
[原架構與剩餘狀態](wsl_root_backup_and_pack_prefetch_2026-10-29.md)。
