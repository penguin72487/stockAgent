# lab203：台股原始續訓來源的固定清冊查找

請由既有 lab203／NAS owner 在不影響原工作及其鎖的前提下，優先用
現有索引查找下列兩個固定版本；不需要為此新增備份或阻塞 Penguin
正在進行的遷移。

| 欄位 | 精確身分 |
| --- | --- |
| Dataset | `tw-daytrade-panel-sources-20261004-gaprepair-v4` |
| Snapshot | `tw-daytrade-panel-sources-20261004-gapre-20261004T163754531166169Z-l0-penguin-6160c54fa6faa702` |
| Packed manifest SHA-256 | `ad0ed2f1154e98fc957581612774d43c1b0a1fea6c6542668b545a4a65354b8a` |
| 封裝內原始 source manifest SHA-256 | `167190ad3e17d43aa590bdb4160e8f9c26e5ffe9951e7d9f9069683e0280811f` |

另請查找以下來源，兩個期貨續訓方案的歷史修訂各自固定引用它：

| 欄位 | 精確身分 |
| --- | --- |
| Dataset | `tw-index-futures` |
| Snapshot | `tw-index-futures-20261023T102203009424803Z-l0-penguin-b6fb99a45032be45` |
| Packed manifest SHA-256 | `ec5ed94c791b9d8a0a123890898a75907cc5466707b46efa9e43059fc8b13202` |

Penguin 於 2026-11-08 22:55 左右核對，這份固定版本的 D manifest 也
不存在；尚未確認 Vast、E 或 NAS 的所有副本，不能推論其全域不存在。
這兩個固定版本的身分均已登錄保護；身分登錄不等於來源已還原。

Penguin 於 2026-11-08 22:04 左右確認本機 D 的固定 manifest 路徑不存在，
Vast 同步索引的相同精確路徑也沒有找到；未遍歷 E 或 NAS 全部歷史，
不代表任何一地的其他副本不存在。先前 [2026-10-25 的固定版本讀回](storage_roles_and_tiers_2026-10-25.md#台股原始來源的完整讀回與有界-ram-reader)
曾核對 5,917 個原檔、5,303,374,780 logical bytes 及 86 個 encoded
objects，這是當時的證據，不證明現在物件仍存在。新 head、其他 snapshot
或上述兩種不同 SHA 均不能互相替代。

若找到，請沿既有回執通道回報實際儲存位置類型、snapshot 身分、manifest
SHA／長度、所需物件的清冊及其已知可用／未核對數量；只有清冊或索引
不代表物件完整、可還原或 NAS 驗收。若本輪未找到，請記錄實際搜尋的
固定 snapshot／索引範圍、未完成或逾時的範圍；不要宣稱全部位置皆無。
如需恢復，再由原 owner 依固定來源及既有接收契約接續，並保留所有
目前 pin、活動程序及原始收據。

## 2026-11-08 23:32 的既有索引回覆

Penguin 收到的固定 non-ACK 回執 `d42351d8614894bbb2a615e02a74396c9aadc4cd2cc8d940be175ba49bb7b1af`
中，上述兩份 manifest SHA 均各有一筆 paired index 相符；回執的 canonical
identity 已核對。台股那筆增量封套列 53 個物件、期貨那筆增量封套列 0 個
新物件；這兩個數字都不是完整版本所需物件總數。兩者的完整物件清冊與
目前 NAS 讀回均未完成，不能宣稱已恢復或可訓練；後續仍依原 owner、原鎖
及回執處理，不阻塞 Penguin 遷移。

## 2026-11-09：E 暫存物件的固定身分交集

Penguin 將 E 暫存的 525 個舊物件身分與上述台股增量索引的 53 個宣告
物件做唯讀 SHA-256／長度交集，找到一項相符：SHA-256
`9eb7677889716d77778d21a6c8cead2b3bd81426363740ec29f3db5b7f60de3b`，
長度 4,812,718 bytes。E 端原交接日誌記錄其舊相對名稱為
`objects/packs/9e/9eb7677889716d77778d21a6c8cead2b3bd81426363740ec29f3db5b7f60de3b.zip`；
既有 E 來源清冊也引用此物件。期貨索引的該增量批次沒有宣告物件，
因此沒有可做此交集的清單。

這是一個可供原 owner 後續對帳的固定身分候選，不代表該物件只屬於
台股版本；本輪沒有重新讀取其 payload，也沒有完整所需物件清冊、
現行 NAS 讀回或原版還原驗收。其餘 52 個宣告物件未在這 525 個 E
暫存物件中匹配；這不代表它們不存在於其他位置。
