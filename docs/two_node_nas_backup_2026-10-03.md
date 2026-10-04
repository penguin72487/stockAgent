# 兩節點與 lab203／NAS 備份實作驗收（2026-10-03）

2026-10-04 最新狀態：第二批 TAIFEX 的 401 檔、409,347,973 bytes 已由使用者
轉交 NAS 驗收成功，完整流程 21.29 秒，固定 snapshot
`d9cf528ac01b42408e00ad41cc3891f7d82ab924c6361dea0ba539836ce32317`。
`lab203-taifex-acceptance-user-report-v4.json` 記錄此回報，原始 NAS 收據仍在 lab203。
接續工作、來源端持續增量服務及實際 code／SQL 還原見
[持續 NAS 備份](continuous_nas_backup_2026-10-04.md)。本文的較早待驗收狀態保留
其觀測日期，不覆寫歷史失敗／缺口。

資料機端已建立獨立 Mamba 備份角色、固定版本的加密備份／還原工具，以及
原生 Windows 可用的交接工具。真實資料的本機加密還原與獨立重建通過；
**已收到使用者轉交的首批 NAS 檔案備份／還原與完整部署摘要；採用已實測
的 WSL/systemd 五分鐘排程。Windows 交接檔本機 SHA／交付已追加回報完成；
使用者取消 Windows 登出驗收，未測試也不標記通過。第二批約 409 MB TAIFEX
已發布並觀測同步完成，NAS 還原驗收與金鑰異機保管仍待完成。**
lab203 由本機 Codex 接手，不使用 SSH。使用者最新指示已進一步授權直接
永久刪除 lab203 的舊 StockAgent 資料，無須備份或恢復證明；先解除舊同步
分享並核對精確本機路徑，保護其他節點與非專案資料。新服務使用新目錄。

## 從需求推導責任

不可取代的資產是來源資料、資料版本與可解讀的還原證據；租用訓練機的
環境與中間產物可以依固定 source release、Git／config／ABI 重建。
因此 penguin 繼續持有來源與網站，Vast 執行可替換的訓練，lab203 只接收
已封閉的備份批次並寫入 NAS。新增接收者不需要成為資料生產者，也不需要
讓資料機登入它的作業系統。

Syncthing 負責傳遞固定批次；NAS 上的 Restic repository 負責加密、去重與
版本還原。READY、來源固定的 envelope 身分、逐檔 SHA 和獨立還原共同
決定一批資料是否可接受。同步鏡像的存在、程序 active 或 TCP 可達不能
替代版本備份，也不能替代還原證明。

工具沿用既有 packed manifest、pinned directory-FD scanner、canonical
verify／fetch 以及 same-MVCC PostgreSQL dump。Windows 使用 stdlib byte
verifier；原 Linux publisher 仍由原 canonical owner 持有。
此次不以 DuckLake、Iceberg 或 Temporal 取代正式流程。

## 實作與量測

| 工作量 | 本機結果 | 能證明的範圍 |
| --- | --- | --- |
| 建立獨立 Miniforge／Mamba role | 8.513 秒；41 個 Conda packages，0 pip overlay | 本機角色建立與原 runtime 身分不變 |
| 當前 cold heads 固定版本盤點 | 158 heads／releases、13,588 unique objects、527,528,155,742 bytes；20.121 秒 | 當前版本 metadata、物件存在與大小；沒有完整讀取 527 GB |
| 128 個物件的 metadata 掃描，交錯各三次 | paths 中位數 1.112 秒；pinned FD 中位數 0.299 秒，約 3.72 倍 | 此資料機 DrvFs 的 metadata 工作量，不能外推 NAS／整體備份速度 |
| 真實來源＋一致 DB dump 的本機加密備份／完整 check／還原／獨立重建／輸出批次 | 9.913 秒；11 個檔案、63,441,242 bytes 逐檔驗證 | 固定來源版本完整重建、dump bytes 還原；不是 NAS 或 SQL logical restore |
| Windows 接收格式的封閉批次，再次加密備份／check／獨立還原／逐檔 SHA | 2.499 秒；11 個 envelope 列入檔案、63,163,322 bytes | 同一便攜 byte verifier 在本機 Restic 還原輸入上的驗收 |
| 備份／NAS 邊界及既有 packed regression | 94 passed，0 skipped；11.43 秒 | 含真實 Restic 整合、錯誤金鑰、partial snapshot、來源改動、READY 最後發布、錯誤 mount 拒絕 |
| PowerShell 5.1 本機 smoke | exit 0，只讀 inventory | penguin 的 Windows 相容性；不是 lab203 或 NAS 授權驗收 |

第一次 paths 掃描為 3.725 秒，其餘為 1.112／1.056 秒；pinned FD 為
0.301／0.299／0.288 秒。平均比值 6.64 包含首次 cache 差異，因此使用
中位數比較，不宣稱整體備份加速 6.64 倍。
不同角色、CPU socket、Windows／WSL 與 NAS 路徑仍要在 lab203 實測完整
相同工作量後選擇，沒有以這台機器的結果指定遠端最佳設定。

環境由 `configs/environments/backup.yml` 建立，Mamba 2.8.0，Restic 0.19.1。
實際 linux-64 explicit lock 位於
`configs/environments/locks/backup-linux-64-20261003.explicit.txt`，SHA-256
`fca71c9d71997a04c9147b5b31d3b6b68322007318aec85cb43158e6fb682213`。
Windows 使用自己的 `backup-windows.yml` 並保存現場 lock，不套用此 Linux lock。
新增 Python 工具與測試的 Ruff 檢查通過。

## 固定試驗身分

| 項目 | 值 |
| --- | --- |
| dataset | `cftc-legacy-pre2000` |
| source snapshot | `cftc-legacy-pre2000-20260902T164739014992260Z-l0-penguin-d53c80bf8d88e2e9` |
| source manifest SHA-256 | `efc966614610dbb35d1d7078e97fca7144bff337eebbcda75621d5d9f8f46322` |
| 獨立重建 source fingerprint SHA-256 | `9069d037cf15525b36eb3bd5f729b40aae6e9789292fd031aae51f6d7df307da` |
| 封閉批次 envelope SHA-256 | `350a50881fdf7ae082c77e03671c6ae61bdcb0d92ea1bfa4206f64c415c0803f` |
| 封閉批次本機 Restic snapshot | `340cc846b2ec47a0f39c96353ac578259dff09c137646c48c6036e405bc8837d` |

這是資料機持有的真實固定來源，不是 lab203 舊資料，也不是來源即時性或全
歷史覆蓋試驗。輸出批次複製 9 個固定輸入檔案，沒有 hardlink，沒有修改
來源 inode／signatures。公開交接批次不包含控制 DB、Restic 金鑰或 node-local state。
READY 只在 portable／canonical 檢查都通過後最後發布。

## 尚未完成的證據

使用者已轉交首批接收與 NAS 還原通過摘要，固定 snapshot ID 見下節。
原始私有 NAS 收據仍在 lab203；資料機未登入 lab203、未代為執行遠端刪除
或 NAS 寫入。較早本機試驗收據的 `durable_off_host_backup_verified` 保持原值
false；新增回報收據只標記這一批 `first_batch_off_host_file_backup_reported_verified`。

此次 dump 使用 exported same-MVCC snapshot，並在加密還原後核對 archive
bytes；SQL logical restore 與 logical state 相容證據仍獨立驗收。
NAS 金鑰副本恢復已回報通過，金鑰在另一台電腦／離線媒介的保管仍待確認。
不能把同一台 lab203 上的副本恢復當作 lab203 故障後仍可取回金鑰的證據。
來源端試驗金鑰不得拿去公開或同步，首批驗收不授權刪除 penguin 來源。

全歷史 capture 另發現資料機某些舊 manifest 引用缺少的 pack：
`0fb95548cf99111e8a06c633cffc5c72e99826c65f2c7a3b245877e3e16595f9.zip`。
此缺口屬 penguin 保存的歷史來源，與 lab203 舊檔案排除無關；保持原缺口
證據，不宣稱全歷史完整，也不要求 lab203 尋找或恢復舊檔案。

## 交接與證據位置

交接入口為 [`lab203_backup_handoff_2026-10-03.md`](lab203_backup_handoff_2026-10-03.md)。
目前依使用者要求直接貼文字，由 lab203 本機 Codex 依交接入口的剩餘工作
繼續驗收；本機路線、固定首批 NAS 還原與 WSL 排程恢復已回報通過。
使用者已核對本機 Syncthing Device ID／新接收路徑，資料機已完成新發送端配對。

來源端工程 task 為 `two-node-lab-nas-backup-20261003`；尚未因本機試驗通過
將整個 NAS 部署標記完成。證據保留於資料機
`artifacts/operations/two-node-lab-nas-backup-20261003/`：

- `environment/acceptance.json`、`current-heads-summary.json`、`scan-benchmark.json`。
- `pilot/acceptance.json`、`pilot/delivery-receipt.json`、`pilot/closed-delivery-acceptance.json`。
- `final-backup-real-restic-tests.xml`、`windows-local-smoke-final.json`。
- `all-history-readiness.json`；保留缺口，沒有刪除來源。

交接 ZIP 只列入明確 allowlist 的工具、設定與文件，另附每個檔案的 SHA-256
manifest；來源 private receipts、dump、試驗 repository 與金鑰均不在 ZIP 中。
該 ZIP 是較早「舊資料排除、不刪除」範圍的封存收據；最新刪除授權與文字
交接以使用者最新訊息及更新後的 handoff 文件為準，ZIP 尚未重新打包。

## lab203 新分享與首批傳輸（21:22 Asia/Taipei）

使用者確認 Device ID `TLOI2HH-6EZOSBC-H6YMGVW-APQOGFW-P2A7FZT-EKX6IUP-2HMRKS6-LBDHOQM`，
Folder ID `stockagent-backup-ingress-lab203`，路徑 `/srv/lab203-backup/ingress`，
模式 Receive Only。資料機以 Send Only 分享 `/srv/stockagent-backup-ingress-lab203`。
沿用既有 Syncthing API helper 與 portable byte verifier，先在未分享的 staging
目錄複製／驗證固定批次，再原子 rename，最後透過單一 device／folder POST
啟用分享，沒有重寫整份 config。其他資料夾、裝置、options／GUI／defaults
逐項核對未變，沒有重新加入舊 cold／hot 分享；autoAcceptFolders／introducer 為 false。

實際 peer 連線為 QUIC／TLS 1.3，觀測 4 個連線 channel。新 folder 為
14 files、63,171,869 bytes；peer completion 100%、remoteState valid，
needBytes／needItems／needDeletes 都為 0，sender idle 且沒有 folder／system errors。
其中固定 envelope 成員是 11 files、63,163,322 bytes，其餘為 envelope／READY
與驗證工具。NAS 金鑰、DB dump、node-local state 都未分享。

這是來源端觀測的傳輸證據，不是 lab203 逐檔 SHA 或 NAS 還原證據；沒有
從累計 connection bytes 與取樣時間推算精確吞吐或宣稱遠端效能最佳。
真實固定批次位於 lab203 ingress 的 `batch-350a50881fdf7ae082c77e03671c6ae61bdcb0d92ea1bfa4206f64c415c0803f`；
portable verifier 位於 `tools/verify_backup_delivery.py`。

證據：`lab203-sender-enrollment.json`、`lab203-transport-observation.json`、
private `syncthing-before-lab203-backup.json`／`syncthing-after-lab203-backup.json`。
canonical agent run `lab203-backup-enrollment-20261003T131733-14a3b395` 成功。
新增 topology 固定於 [`backup_ingress.json`](../configs/data_sync/backup_ingress.json)。

## lab203 首批 NAS 驗收回報與 Windows 檔案交付

使用者轉交 lab203 本機 Codex 的驗收摘要：接收端 idle、待收與 errors 為 0，
READY、固定 envelope 與完整檔案集合通過；NAS Restic 加密 backup、
`check --read-data`、固定完整 snapshot 獨立還原、還原後來源 verifier 與
金鑰副本恢復都通過。NAS snapshot ID：
`45ae68ecb01f77649ff72dcfc7c747dd8b05b6e5a228d32924daffce6d3f4a11`。

這一批共有 13 個檔案（11 個 envelope 成員加 envelope／READY），來源端
合計 63,166,382 bytes。使用者回報這 13 個檔案 SHA-256 全部一致；
資料機重新核對來源固定 envelope、完整集合與上述分母。新增私人收據
`lab203-first-batch-acceptance-user-report.json` 明確記錄 evidence kind 為
`user_relayed_lab203_acceptance_summary`，不是資料機直接讀取 NAS 的驗證。

正式五分鐘排程已回報啟用，服務重啟與 NAS 暫時不可用後恢復通過。
prune 與批次自動刪除未啟用。這些證據只涵蓋首批離機**檔案**備份，
不涵蓋 NAS 還原資料的 packed 重建、PostgreSQL logical restore、全歷史
覆蓋、金鑰異機保管或 Windows 冷開機恢復；不能外推為完整災難復原。

使用者已確認缺少的 Windows 檔案就是下列兩個，資料機已補進現有新分享的
`tools/`，並提供 `windows-handoff-manifest.json`。來源端 21:49
（Asia/Taipei）再次觀測 peer completion 100%、remoteState valid、待收 0。
更新後新 folder 為 17 files、63,183,135 bytes；既有 13 檔試驗批次未改。

| lab203 接收路徑 | SHA-256 |
| --- | --- |
| `/srv/lab203-backup/ingress/tools/prepare_lab203_backup.ps1` | `459cd0b05472e3f3f4bf0f3cb38755d8edf40b17cd386acc8666c44509da5ed8` |
| `/srv/lab203-backup/ingress/tools/backup-windows.yml` | `82e96b7db610daa3f3289451a16de1bcd000fcf7bce23c471b64f1cda51db378` |

交付證據為 `windows-handoff-delivery.json`、`windows-handoff-transport.json`。
兩檔沒有金鑰或 credential；來源端交付不能替代 lab203 本機雜湊比對或
原生 Windows 執行驗收。它們供 Windows 交接／環境準備，不改動現行 NAS
排程。擴大正式備份前仍需 NAS 實際 repository 路徑、可用容量／配額與
完整工作流程量測；本輪未直接傳輸 527 GB 或啟用來源清理。

## 第二次回報：正式 snapshot、路線與平台實測

使用者後續提供完整部署摘要，正式 NAS snapshot 更新為
`2145243446a4b6b3a29523cbfd3efa8d4001f378a1196d296bb377a33ef25444`。
仍是同一個約 63 MB 固定試驗批次，已回報 `check --read-data`、固定 snapshot
NAS 獨立還原、完整 13 檔 SHA 與匯出金鑰副本恢復全部通過；前一個 snapshot
與原始回報收據保持不變。新回報保存於
`lab203-deployment-acceptance-user-report-v2.json`，最新指標收據為
`lab203-latest-reported-acceptance.json`，均標記 user-relayed evidence。

| 完整同批 backup／check／restore／逐檔 SHA 秒數 | 首次 | 暖機 1 | 暖機 2 |
| --- | ---: | ---: | ---: |
| WSL | 13.60 | 7.33 | 7.32 |
| Windows | 12.73 | 6.95 | 7.95 |

各平台三次交錯完整工作量都回報通過。暖機平均 WSL 7.325 秒、Windows
7.45 秒，相差 0.125 秒，約為 Windows 平均的 1.68%；首次 Windows 快 0.87 秒。
這是每平台只有兩個暖機觀測的描述性比較，不做顯著性、大批資料 throughput
或普遍最快宣稱。沿用已通過非互動與故障接手測試的 WSL/systemd，避免為
小幅時序差異再搬動目前 owner／credential／路徑與正式服務。

使用者回報指定 NAS 目錄讀寫成功、目前直連不需 VPN，可見剩餘約
14.08 TiB；帳號 quota 仍需 NAS 管理端確認。可見 filesystem free space
不能代替帳號配額，原始容量精度也不能由小數近似值反推出精確 bytes。
舊分享已解除，舊資料已刪除，WSL 釋放 533.53 GiB；此數值不等同 Windows
VHDX 已縮小或 Windows 主機實際回收相同容量，後者仍未驗證。沒有啟用 SSH。

排程每五分鐘執行，非互動、中途終止後接手與 NAS 暫時不可用後恢復已
回報通過；Windows 登出期間持續執行仍待確認。金鑰異機保管、兩個已同步
Windows 檔案的本機雜湊／Windows 位置，以及 NAS quota／實際 repository
路徑繼續保留在下一步。沒有啟用 prune、自動刪除批次或 penguin 來源清理。

完整原始部署證據位於 lab203 的
`/mnt/c/Lab203Backup/evidence/deployment-report.txt`；此路徑在 penguin 不存在，
本輪未直接讀取該檔。NAS 檔案備份通過不提升為全歷史、NAS packed 重建或
PostgreSQL logical restore 完成。

## 下一批官方 TAIFEX 資料的本機準備階段

資料機重新盤點當前 cold heads，結果仍為 158 個固定 releases、13,588 個
unique objects、527,528,155,742 bytes；本次 metadata capture 的 canonical
run 為 `offhost-next-batch-inventory-20261003T135706-fe03e625`，耗時 27.036 秒。
這是存在／大小與固定版本盤點，沒有完整讀取全部 527 GB。

從已登記可發布的官方來源中，選擇完整 `taifex-public-history` 固定 release
作為下一批，沿用現有 capture／export／portable verifier／canonical verifier。
沒有加入私有研究衍生資料或另一套封裝格式。

| 項目 | 固定值／本機結果 |
| --- | --- |
| source snapshot | `taifex-public-history-20261002T093824628950032Z-l0-penguin-4fc6d275c2162861` |
| source manifest SHA-256 | `c14a9148e86761958ea0d8860e52f10c21e7879d8bef2a51397868dc55f95702` |
| envelope 身分 SHA-256 | `470e7fcbe03d26223007e280cc13aa42fd22bc8dc0d85d546f4c33d121672ede` |
| 固定輸入複製 | 397 個檔案、408,808,157 bytes；沒有 hardlink |
| envelope 成員逐檔 SHA | 399 個檔案、409,255,477 bytes；另有 envelope／READY |
| 含 envelope／READY 的完整批次 | 401 個檔案、409,347,973 bytes |
| 本機完整 capture／copy／驗證／封閉輸出 | 64.118 秒 |
| 發布上限 | 1 GiB；本次低於上限，未分享至 Syncthing |
| 本機 staging | `artifacts/operations/two-node-lab-nas-backup-20261003/next-taifex-ready-delivery` |

canonical archive／object 驗證與便攜全檔 SHA 通過，READY 最後發布；尚未做
這一批 materialized 重建、NAS 加密備份或 NAS 還原。上述 64.118 秒是資料機
本機準備時間，不是跨機吞吐或 NAS 工作流程時間。

準備階段的下一步是由 lab203 本機確認正式接收程序支持多個固定 envelope 的批次，逐批
驗證、保存完整 NAS snapshot ID 並可獨立重試；不能把首批固定身分直接套用
下一批。同時回報實際 Restic repository 路徑與帳號配額，再沿現有專用分享
發布這個已封閉版本，量測較大批次的完整 NAS 備份／check／restore／SHA。
資料機沒有直接傳輸全量、刪除來源或更動現行五分鐘排程。

證據為 `next-current-heads-plan.json`、`next-taifex-plan.json`、
`next-taifex-delivery-receipt.json`、`next-batch-preparation.json`；canonical run
`offhost-next-taifex-preparation-20261003T140036-96df1ebf` 成功。
來源端當前同步與新批次本機範圍的里程碑核對收據為
`lab203-followup-acceptance.json`，保留前後 NAS 回報及交付檔案的 SHA 綁定。

## 第三次回報：接收準備完成與使用者範圍調整

詳細部署摘要及 14:26／14:27 UTC 追加回報保存為
`lab203-deployment-addendum-user-report-v3.json`、
`lab203-deployment-acceptance-user-report-v3.json`；最新指標已指向 v3，v1／v2
及前後 NAS snapshot 收據不覆寫。這些是使用者轉交的遠端驗收；原始私有
environment、NAS、outage 等收據仍在 lab203，資料機未直接取得或執行它們。

lab203 為 DESKTOP-J311GTB，i7-14700F、20 cores／28 threads、64 GB、KC3000
2 TB、Windows 11 Pro 10.0.26200、Ubuntu WSL、單 socket／單 NUMA。現場
Miniforge 26.5.3-0／Mamba 2.5.0、Python 3.12.14、Restic 0.19.1、Syncthing
2.1.3；已保存各平台 explicit locks／package SHA 並逐次核對 runtime-lock。
沒有為單 socket 節點套用另一台雙 socket 主機的參數。

更新後完整同批數字如下，首次含新 repo 初始化、Windows 含 WSL ingress
複製至 NTFS 的準備；沒有宣稱清除 OS 磁碟 cache：

| 完整流程秒數 | 首次 | 暖機 1 | 暖機 2 |
| --- | ---: | ---: | ---: |
| WSL | 13.600 | 7.328 | 7.319 |
| Windows | 12.726 | 6.952 | 7.952 |

由顯示數值計算暖機平均為 7.3235／7.452 秒，相差 0.1285 秒；遠端摘要
報告的暖機平均為 7.323／7.452 秒。均支持沿用已通過恢復驗收的 WSL，
不宣稱統計顯著或推論較大工作量的 NAS throughput。

實際 NAS UNC 為
`\\140.127.208.143\Lab203\學生上傳區\錢昱名\lab203-restic-20261003-210839-9ba17ccf\repository`，
WSL 對應
`/mnt/lab203-nas-cifs/學生上傳區/錢昱名/lab203-restic-20261003-210839-9ba17ccf/repository`。
Repository ID `c717a3d315e20a5a3314c595e66607fcb7d6646cb73ad4412cc2cccb22abb798`，
格式 2。64 MiB 隨機探測寫入 99.2 MiB/s、讀取 64.9 MiB/s 且 SHA 相符；這
是單次探測，不是持續吞吐保證。直連不需 VPN，WSL 使用加密 SMB 3.1.1。
可見容量約 14.08 TiB，使用者確認 user1 帳號無特別配額／無上限；SMB 管理端
查詢回傳 `NT_STATUS_ACCESS_DENIED`，未獨立讀取配額。此容量回報支持有界
第二批試驗，沒有把管理端查詢失敗改寫為獨立驗證成功。

兩個 Windows 交接檔已比對固定 SHA，原樣交付至 `C:\Lab203Backup\handoff\`。
多批次以固定規範化 envelope 與原始 envelope 檔 SHA 處理，逐批 3 次重試
（5／15 秒）、保留失敗批次並續辦後續有效批次；2 個路由＋4 個基礎測試
回報通過。這是工程契約，不是第二個真實批次已備份的證據。

S4U 喚醒 task 的 Session 0 backup／NAS read 退出 0，保留 Ubuntu runtime
holder。使用者明確表示 lab203 不關機、不登出，取消登出驗收；未實際登出，
停止其收集／探測，不再列為本輪待執行條件，也不標記登出或冷開機通過。
金鑰恢復副本仍在 lab203 本機，異機／外接媒體保管尚未確認。舊資料實測
釋放 533.528 GiB 的 WSL 空間，Windows VHDX 縮小未驗證。

## 第二批 TAIFEX 發布與來源端同步驗收

沿用已驗證的封閉批次、portable verifier、canonical archive／object verifier
與既有單向分享。重新核對固定 399 個成員 SHA、原始 envelope SHA、READY、
完整 401 檔集合、原來源 signatures 與已登記 `publish: true` 的官方來源。
staging 與 share 的 `st_dev` 同為 2096，以原子 rename 保留目錄 inode 329159；
額外 payload 複製為 0 bytes，沒有省略發布前／後逐檔 SHA 或 canonical check。

| 工作量／身分 | 實測／固定值 |
| --- | --- |
| 原子 rename | 0.000148 秒；不是整體 workflow 時間 |
| 完整來源端發布重新驗證 | 10.763 秒 |
| 發布重新驗證至觀測 peer 完成 | 51.341 秒；2 秒輪詢，有觀測間隔 |
| 第二批本身 | 401 檔、409,347,973 bytes，含 envelope／READY |
| envelope 原始檔 SHA-256 | `2a1ce4618261f0607c8e057b2c42869eea4677e084b0134b652a18afb8f7cae0` |
| 交接 descriptor | `tools/taifex-batch-handoff.json`；SHA `0a3e2aaf6d52d19a7d61a565c658fa41a71da571479b013bfb75ed25729bc971` |
| 更新後整個 share | 419 檔、472,532,460 bytes；含原批次／工具及新 descriptor |
| sender／peer | sender idle／errors 0，peer completion 100%、valid、need bytes/items/deletes 0 |
| Syncthing 設定 | 發布前／後完整 config SHA 相同；沒有新增裝置／分享或改其他設定 |

資料機批次現在位於
`/srv/stockagent-backup-ingress-lab203/batch-470e7fcbe03d26223007e280cc13aa42fd22bc8dc0d85d546f4c33d121672ede`，
lab203 接收位置為
`/srv/lab203-backup/ingress/batch-470e7fcbe03d26223007e280cc13aa42fd22bc8dc0d85d546f4c33d121672ede`。
準備階段 staging 已原子移入分享，前一階段的 destination 收據保留為歷史
觀測，新的 `next-taifex-publication.json` 明確綁定移動前／後路徑。

這是本機封閉發布與來源端觀測的 transport convergence，尚未取得 lab203
第二批逐檔 SHA／NAS 新 snapshot／固定 ID 還原或完整工作流程計時；不能用
累計 connection bytes 推估硬體峰值，也不能稱第二批 NAS 備份已完成。
原資料、首批輸入與 NAS 密鑰均未更動，prune／批次自動刪除維持關閉。

證據為 `next-taifex-publication.json`、`next-taifex-transport-observation.json`，
canonical run 為 `offhost-next-taifex-publication-20261003T144149-2968c8bc`。
第二批驗收任務及固定兩種 SHA 見
[`lab203_backup_handoff_2026-10-03.md`](lab203_backup_handoff_2026-10-03.md)。
