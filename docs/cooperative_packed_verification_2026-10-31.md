# Penguin／Vast 分工驗證

這次分工以固定版本的 **pack** 為工作單位。本機保留 D 冷庫正本、清冊與最終
驗收責任；本機和已登錄的私人 Vast 計算節點核對不同的 pack，不搬走整個
WSL，也不在遠端建立第二份資料庫。適用的是完整原值重建，不是抽樣。

## 資料與工作流程

1. 原 `dataset_migration._recover` 固定 manifest／inventory，沿原 owner 驗證
   全部清冊與物件存在性。已完成的固定 recovery receipt 繼續使用，不重跑。
2. 單一 D 讀取通道供料，減少 HDD 跳讀。原 Windows `binary_reader_many`
   每批最多 8 個、合計 128 MiB，單件至多 64 MiB；觀測 RAM 足夠才預讀。超出
   範圍或 RAM 不足沿用原有單 pack RAM／SSD／直接 D 路徑。
3. 批次必須先通過原生程序退出、精確 EOF、D mount、全部物件 SHA 與來源
   generation fence，才交出記憶體 buffer。預讀後等待派工的物件也重新查
   generation，不能用舊 buffer 掩蓋其間的來源變動。
4. 本機與 Vast 的工作程序從同一佇列領取不同 pack。讀取下一批時，可與
   上一批的解壓、hash、遠端傳輸重疊；完成的 lane 先領下一件，避免依序
   等待前一個慢工作。依觀測速率在佇列尾端停止遠端派工。
5. 每件核對完整封存 SHA、全部儲存成員 CRC、清冊成員的原值 SHA／長度，
   回傳有序、長度分隔的成員 commitment。subset pack 的未選中舊成員仍讀
   至 EOF 核對 CRC。零長度、負值、Unicode 與原始 bytes 不改寫。
6. 所有工作 join 後，原 verifier 才彙整完整檔數／bytes。blob、chunked file、
   symlink 與 materialization 繼續使用原本判準。最後仍由原 migration owner
   寫正式 recovery receipt；分工收據本身不是 NAS ACK 或來源刪除授權。

遠端使用同一已登錄、嚴格 host-key 的 SSH 端點。持續連線傳送固定程式與
有界控制 frame，pack 只進匿名記憶體；每件結束即釋放，不寫遠端 payload、
不開新服務埠、不建立 mirror／training lease。這是私人驗證計算 RPC，原有
rclone cold payload transport 與唯一 NAS replication owner 繼續負責資料交付。

遠端程式 SHA、boot ID、manifest SHA、工作 nonce、commitment、policy SHA
綁在收據中；遠端失聯、逾時或回傳不同身分，保留失敗原因並以同一 buffer
在本機完整核對。初始化等待與每件工作都有期限。只終止己方 RPC 子程序，
不清 owner lock、pin 或任何來源檔案。

## 設定與接手

模板是 `configs/data_sync/pack_verification.example.json`，預設停用。真正啟用
檔案是 owned／0600 的 `/etc/stockagent/pack-verification.json`，以 `datasets`
逐項登錄；不擴張 `publish: false`、NAS 或其他來源權限。本次候選設定為本機
2 路、遠端 1 路；它是這次實測的併行數，並非硬體永久上限或服務 CPU 配額。

`timeout_seconds` 是每件 RPC 的期限；初始化另限制最長 15 秒。
`initial_*_bytes_per_second` 只用於初始派工估計，完成工作後會更新。
source buffer 批次由觀測的 headroom 准入，不占用另一份解壓樹。

工程重測入口，不會替原 migration owner 寫完成收據：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/verify_distributed_packed_snapshot.py \
  --cold-root /srv/stockagent-packed/.local-state/private-history \
  --dataset EXACT_DATASET --snapshot-id EXACT_SNAPSHOT \
  --policy /etc/stockagent/pack-verification.json \
  --output artifacts/storage_layout/NEW_RECEIPT.json
```

加 `--serial` 可比較原本的完整驗證。輸出必須使用新名稱，保留失敗與舊測量。
生產 `_recover` 的每件／session／summary 記錄在原 recovery receipt 旁的
`pack-verification/<dataset>/<attempt>/`，不另建 publisher 或清理 owner。

程式已納入 `manage_lakehouse.worker_code_identity`，沿
`rollout_lakehouse_source_policy.py --reload-code-only` 的原 owner 邊界載入，
核對原固定 releases、持久 workflow chain、新 worker loaded-code 身分；不重啟
PostgreSQL、Temporal server 或當沖服務。排入交接不等於已載入。

## 實測證據與限制

遠端為現有 `stockagent-vast` 登錄，2026-10-31 核對 boot
`910a0cff-52d2-4043-a020-ca8a7fa8f0a4`。沒有啟動 GPU 工作。

| 固定版本／流程 | 完整驗證結果 |
| --- | --- |
| FRED，1,477 原件／120 packs，最初逐件 SFTP 候選 | 交錯測量平均 serial 25.03 秒、分工 23.85 秒，波動大，僅約 1.05x；保留負面證據，取消逐件連線／落盤 |
| 同一 FRED，持續 SSH 記憶體 RPC | 交錯測量平均 serial 4.16 秒、分工 3.82 秒，約 1.09x；快取與背景負載不同，不可與上一列直接相除 |
| Crypto ETF，1,794 原件／126 packs，持續 RPC、尚未合併 D 讀取 | 149.99 秒，local 88／Vast 38，完整 proof 通過 |
| 同一 Crypto ETF，合併 D 讀取後的單次初測 | 28.62 秒，local 105／Vast 21；63 packs／119,406,441 bytes 合為 8 次原生讀取；完整 proof 通過 |
| 同一 Crypto ETF，最終完整流程 A/B/B/A | serial 144.28／167.40 秒；分工 40.52／47.96 秒；平均 **155.84 → 44.24 秒，3.52x** |

正式交錯比較保留同一 manifest／inventory、126 物件與 123,041,105 encoded
bytes、1,794 個原件與 122,009,219 logical bytes；四次全部 proof 一致。
這是該完整固定版本的結果，不是全市場加速倍數；單次初測也不代表穩態。
以上都包含完整固定版本清冊、物件 SHA／CRC、原值還原；A/B/B/A 外層
另含 runtime wrapper 與匯入時間。遠端建立 payload 檔案數與原始資料刪除數為零。

關鍵差別是消除小檔 native-process 啟動及遠端重連成本，再重疊可分工的計算。
網路、D 持續讀取、清冊與來源發布仍是工作流程的一部分；增加遠端 CPU 不代表
消除這些成本，也不保證任何工作都適合卸載。

可檢查的證據：

- `artifacts/storage_layout/fred-lanes-abba-20261031.json`
- `artifacts/storage_layout/fred-ssh-memory-abba-20261031.json`
- `artifacts/storage_layout/crypto-etf-memory-first-20261031.json`
- `artifacts/storage_layout/crypto-etf-memory-batched-20261031.json`
- `artifacts/storage_layout/crypto-etf-batched-abba-20261031.json`
- `artifacts/storage_layout/cooperative-pack-regression-20261031.log`
- `artifacts/storage_layout/cooperative-control-regression-20261031.log`
- `artifacts/operations/cooperative-pack-history-replay-20261031/acceptance.json`

原 61 個來源的進度、正式載入與來源退休分開核對。這次實測沒有自行清除原始
資料；後續回收仍須原固定 D 還原、適用 NAS 配對、reader／writer 切換、
process 引用及精確退休收據。

12:49 現場仍為 55／61 固定來源完成。原 worker PID 1269 正在處理
OpenBB 的 8,013,131 個來源檔；這是來源 routing／hashing 階段，不是新版分工
還原正在執行。私有政策已逐項登錄這 61 個既有來源、0600，沿用已完成收據。
正式交接 run `cooperative-pack-owner-handoff-20261031T044704-53d95030` 已由
既有 systemd launcher 啟動，沿原 owner 自然邊界等待。等待上限 8 小時是
交接期限，不是全資料完成 ETA；期間候選 code identity 若變動會拒絕載入。
正式採用須查 `artifacts/storage_layout/cooperative-pack-owner-handoff-20261031.json`
的 `accepted` 與新 worker loaded-code identity，不能把 running waiter 當作已部署。

驗證包含 21 項分工／批次來源測試；較廣資料回歸有 882 項通過。初次把控制
測試放在一般 fintech role 時，9 項因未安裝 Temporal SDK 失敗；沒有修改套件
或略過判準，改用原 lakehouse-control role 後，包含這 9 項的 122 項控制測試
全通過。另以原 SDK sandbox 重播目前三條完整持久 history，沒有執行 activity、
重設 workflow、呼叫 provider 或停止服務。完整工程彙整見
`artifacts/storage_layout/cooperative-pack-qualification-20261031.json`。

## 13:29 使用者指定立即切換後的正式驗收

使用者明確要求現在直接使用新版，已取消上面的自然邊界 waiter，保留其
`cancelled`／exit -15 原紀錄；改由既有 `--restart-active-owner` 入口接手。
候選 closure、pack worker、native batch adapter、私有 policy SHA 均與上述
工程驗收相同，沒有混入另一份候選或重做已完成的 55 個固定版本。

原 worker 在 13:28:39 開始 systemd 停止，13:29:25 記錄
`stopped_after_graceful_worker_shutdown`，13:29:26 啟動新版；沒有觸發 KILL
或刪鎖。13:29:28 正式交接收據為 `accepted`：

- 原 PID 1269 → 新 PID **372266**。
- 已載入 closure SHA：`23950d8564b0a52aa5a6bd750808a68ae21aa6073b3559b050da1bc84c546f41`。
- 55 個已完成 releases 的 snapshot／manifest 綁定全部保留。
- 三條持久 workflow 保留原 first-run chain；PostgreSQL、Temporal server 未重啟。
- 當沖服務仍為原 PID 366、10:50:36 啟動，沒有隨儲存 worker 重啟。
- 使用正式 lakehouse-control role 再核對 Vast RPC，可啟動相同 SHA 的固定
  記憶體 worker、核對 boot，並正常關閉；沒有建立遠端 payload 檔案。

新 PID 已寫入原 `dataset-versions` runtime owner，接手 `local-tw-public`
的來源清冊。原 OpenBB 未封閉的 routing／hashing attempt 已由原 owner
記錄 interruption 並排入 retry，保留舊進度證據；不宣稱能從未持久化的
逐檔 hash 中間點續跑。進入固定版本完整重建後使用已啟用的本機 2 路／Vast
1 路設定；來源清冊／發布階段不是雙機 pack reconstruction。

## 14:17 再驗證與排程修正

台股原固定版本已發布，原值為 225,361 檔、84,634,353,640 bytes；完整驗證
另有 9,889 個物件，其中 3,333 個 pack 合計 26,719,373,257 encoded bytes。
其中 1,548 個 pack 超過舊 8 MiB 單件預讀範圍，原先會各自啟動 Windows
讀取程序。擴大批次範圍後，固定 8 packs／106,897,765 bytes 的 A/B/B/A
完整 encoded SHA、所有 ZIP CRC 與 generation 核對平均 **36.89 → 6.24 秒，
5.91x**。另核對這 8 packs 中全部 1,827 個清冊原件的原值 SHA、長度及
有序 commitment，完整通過。後者連清冊初始化花 54.66 秒，不能和前述
僅讀取／CRC 的時間直接相除，也不能把 5.91x 當作整個台股版本的加速倍數。

正式批次仍保留原生子程序退出／EOF／mount／全部 SHA／generation fences，
及依實際 RAM headroom 的准入；沒有加上服務 CPU、記憶體或 I/O 配額。
後續以固定 24 packs 另測 16 件／512 MiB 候選，A/B/B/A 平均
10.50 → 7.74 秒（約 1.36x），保留於 `native16-pack-batch-abba-20261031.json`。
這個候選未載入現行 worker；正在核對的批次不能因為編輯 adapter 原始碼而
破壞其 code fence，重啟也會重做尚未形成完整 recovery receipt 的原值核對。

14:10 原 owner 重新載入後發現兩項獨立的協作錯誤，已修正：

- 非 CLI 的 migration 呼叫缺少 `retention_config`，使 CFTC closed-source
  重用分支拋出 `AttributeError`。現在沿用相同 repo 的原 retention owner，
  明確提供的設定仍保留，不另建 cache 或來源管線。
- 原生讀取子程序在主動停止 worker 時退出，曾被記為一般來源讀取錯誤。
  原 Temporal activity 現在傳遞實際 cancellation／worker-shutdown 狀態；
  這種中斷保留原 processing／recovery 階段、固定 snapshot、錯誤與 attempt
  證據，由下一個 exclusive owner 原樣對帳接續。一般 I/O 失敗仍保留錯誤
  與原重試截止時間，不能以停機判準吞掉真實來源錯誤。

原入口再做三條完整持久 history replay 後，14:16:55 的正式交接收據 accepted：
PID **473333 → 489806**，closure
`0ad9972f9de3df395c9cf72a4e8f770bcefbdd0bf1fc2d348050f673163b632c`。
55 個已完成版本與三條原 workflow chain 保留；PG／Temporal server／當沖
服務沒有重啟。新 attempt `2c478fd7a304484d8c9b30fd1e852a97` 接續相同台股
snapshot 與 manifest `4ecbb9bef99425f350d25be5b6b46f94e38801735733162b1c3c0a09ad81a22a`。
14:22:11 已有 489／3,333 packs 的新證據（Penguin 412、Vast 77）；不是沿用
重啟前 339 個 pack 的舊進度。這仍不是全版本完成或原始資料可刪證明。

本輪驗證：批次來源／封存回收相關 577 項、migration／status／transition
377 項、原 control role 的 activity／continuation 13 項、原 fintech role 的
closed-source 18 項通過；批次退出、錯誤 bytes、來源變動、RAM 不足、shutdown
接續及真實 I/O 失敗均有回歸判準。首次以 control role 混跑 closed-source
測試因 collector 的 `common` 匯入失敗，保留失敗紀錄；按兩個既有角色分開
執行後全部通過，沒有更改環境或來源判準。

第 6～11 輪原退休收據累計回收 **70,365,720,576 allocated bytes（65.54 GiB）**，
對應來源名稱均已不存在，這是既有累計。第 12 輪已按其獨立 scoped receipt
ABI 補上原 completion reader，保留原 failed memory gate 與未通過 remote strict
environment 的結果；不是放寬成 runtime／CUDA 驗收。40,539 個目前來源檔、
7,665,729,536 unique allocated bytes 的清冊已固定，由原 wave scheduler／
retirement owner 在 D 暫存壓縮及核對；完整冷還原、process／pin 與退休收據
仍須全部通過才刪除。未完成的第 12 輪不能計入上述已釋放空間。

主要證據：

- `artifacts/storage_layout/large-pack-batch-abba-20261031.json`
- `artifacts/storage_layout/large-pack-batch-originals-20261031.json`
- `artifacts/storage_layout/cooperative-shutdown-retention-cutover-20261031.json`
- `artifacts/operations/cooperative-shutdown-retention-replay-20261031/acceptance.json`
- `artifacts/storage_layout/cooperative-batch-expanded-regression-20261031.log`
- `artifacts/storage_layout/canonical-shutdown-migration-regression-20261031.log`
- `artifacts/storage_layout/canonical-shutdown-control-focused-v2-20261031.log`
- `artifacts/storage_layout/canonical-retention-closed-source-regression-20261031.log`
- `artifacts/operations/storage-single-authority-20261025/workflow-round12-symlinks-d-20261031.inventory.json`
- `artifacts/operations/storage-single-authority-20261025/workflow-round12-symlinks-d-retirement/progress.json`

正式證據：

- `artifacts/storage_layout/cooperative-pack-immediate-preflight-20261031.json`
- `artifacts/storage_layout/cooperative-pack-immediate-cutover-20261031-before.json`
- `artifacts/storage_layout/cooperative-pack-immediate-cutover-20261031.json`
- `artifacts/storage_layout/cooperative-pack-production-role-rpc-20261031.json`

這份正式載入驗收補上前一輪工程收據的 pending gate；全 61 個來源完成、
NAS 獨立驗收與來源退休仍各自以原 owner 的新收據判定。

## 15:45 封存階段完成後的讀取瓶頸與續接

原 `local-tw-public` 的 3,333 packs 已全部封閉：Penguin 2,759、Vast 574，
原件解壓／SHA／全部 ZIP CRC 階段實際 2,251.71 秒。Vast 已完成這一階段，
之後是 Penguin 核對 D blobs 與 chunked originals；不能仍把遠端列為忙碌。
15:18 的執行堆疊顯示 `sha256_file` 正在讀 D blob，並非等待 owner lock。

修正沿原 migration owner 進行：

1. 原生完整 blob SHA 批次沿用原 primary／private-history 精確身分與權限
   gate，原本 private history 沒啟用這條既有讀取路徑。只讀核對改為最多
   128 件、總計 16 GiB、單件 8 GiB；copy／write 保持原範圍與 16 件邊界。
   metadata 查詢並行，全部 path／generation preflight join 後才啟動原生 I/O。
2. 封閉完整 pack cohort 綁定原 manifest、所有逐包原值 commitment、全部 CRC、
   worker／adapter／policy SHA 與原 node totals，才能在新一輪**全部封存 SHA**
   及前後 generation 重核對後續用。未知、部分、改變的證據拒絕續用；原收據
   保留。這只省掉重做解壓，blob／chunk、總數與原本的正式驗收仍需完成。
3. pack 完成後的進度延續其已核對的物件／原件數，不再顯示回到零；目前
   階段仍各自標記，不能用「檔數幾乎完成」推定大型原件 bytes 已經完成。
4. 原 owner 明確啟用 `d_primary_native_original_hashes`。Windows 直接按照
   清冊順序讀取 chunks，同時計算每段完整 SHA 和**整個原件的 SHA**。
   重複 chunk 會重複計入原件 SHA；不能拿片段 SHA 組合代替原件雜湊。
   傳回小型校驗 metadata，不把所有原始 bytes 複製到 WSL。每批全部 child
   exit、輸出數、完整原件 SHA、每段 SHA、mount、前後 generation 及程式 SHA
   都通過後才交回；全清冊最後仍再次核對。沒有建立任何解壓資料樹。

實測保留冷／熱快取與並行負載差異，不直接外推成整機倍數：

| 相同固定資料與核對範圍 | A/B/B/A 結果 |
| --- | --- |
| 128 real packs，988,937,387 bytes；全部 SHA／path／mount／generation | 16 件批次 25.83／23.71 秒；128 件 4.73／4.70 秒；平均 **24.77 → 4.71 秒，5.25x** |
| 4 個完整原件，34,125,107 bytes；每段 SHA＋依序完整原件 SHA | 原 bytes pipe 15.65／14.18 秒；原生原件 SHA 5.43／4.74 秒；平均 **14.92 → 5.08 秒，2.93x** |
| 全 3,333 packs，26,719,373,257 encoded bytes | 新鮮全部 SHA＋全部 generation＋完整舊原值／CRC 證據重核對，共 **409.08 秒**；未寫原 owner journal、未寫正式 recovery receipt |

原生 member metadata 的另一次 16-pack 比較保留負面證據：首次 serial
6.92 秒、暖 serial 2.23 秒；parallel 2.51／2.54 秒。不能將首次冷讀的差距
全部歸功於 threads，也沒有據此宣稱全部 metadata 都加速。

本輪固定實檔證據：

- `artifacts/storage_layout/native-hash-128-abba-20261031.json`
- `artifacts/storage_layout/native-chunk-original-abba-20261031.json`
- `artifacts/storage_layout/native-hash-metadata-parallel-abba-20261031.json`
- `artifacts/storage_layout/completed-pack-reuse-qualification-v4-20261031/qualification.json`
- `artifacts/storage_layout/tw-cold-object-stack-20261031T0718Z.txt`

中途停止的工程資格測試保留原 failed／stopped run；原 worker、來源、pin 和
退休 owner 一直保留。全量證據使用最後完整 v4，不把較早 partial 當完成。
正式 worker 載入與後續 WSL 實際回收仍須分開讀取當前收據。

## 16:06 正式續接、大型清冊邊界與後續回收

15:47 的原 owner 交接已驗收：載入 `f943aa6e…`，保留原 55 個已驗收
版本與三條持久工作鏈。正式續接重新讀取全部 3,333 個 encoded packs，
26,719,373,257 bytes 的完整 SHA／generation 加上原逐包完整原值與 CRC
證據核對共 338.32 秒；保留的原 local／Vast 分工仍為 2,759／574。
這份 computation summary 的 `acceptance` 與 `source_retirement` 都是 false，
不能代替全來源還原或授權刪除。

後續第一批 chunked originals 暴露了另一個真實協定限制：其中一個
1,341,346,597-byte 原件有 160 個 extents，原 wrapper 將其清冊重新 JSON／
base64 放進單一 Windows 環境變數，需 57,272 字元，超過該介面的限制。
原批次保留 `native-command-rejected`，沒有驗收；以不含來源的相同長度
字串在 Windows `SetEnvironmentVariable` 重現「name or value is too long」。

批次 wrapper 改為把已解析的物件直接傳入每個成員的獨立 scriptblock
scope。清冊仍經既有 stdin／startup nonce 協定進入；不再重新序列化至環境
變數，命令列長度不隨清冊增加。獨立的單件 read／write 介面保持原來的
設定入口。固定 D namespace、來源 generation、完整 chunk 與 original SHA、
輸出數、子程序退出、總 bytes 及最終清冊檢查全部保留。

以剛才被拒絕的**同一個第一批**實檔驗證：128 originals、1,066 extents、
8,396,326,699 original bytes 全部通過，核對階段 106.29 秒；包括 Python
啟動與完整清冊開啟等的監督命令總時間 134.94 秒。這是單批工程驗證，
沒有寫入正式 recovery journal、刪除來源或將它當成新的 NAS ACK。
專門的 protocol／chunk／reuse 回歸 166 項、共用 native publication／pack／
migration 路徑 722 項通過；原三條完整 Temporal history 再次重播通過，
沒有執行 activity 或 reset。

16:05:35 原 owner 再次正常交接，載入 `c54b0c91…`、PID 798274，保留
**56／61** 個已驗收來源；新增的是原 CFTC 舊歷史故障修正後完成的來源。
原設定 SHA、持久工作鏈、PostgreSQL／Temporal server 與當沖服務保留。
TW 的失敗紀錄留存，由原 migration owner 續接；OpenBB 約 801 萬檔的清冊
尚未完成。另兩個期貨來源仍被 `/dev/shm` 的缺失 absolute symlink 阻擋，
相依 futures source 等待其原目標；沒有刪除別名或捏造目標解除 gate。

後續回收已接回原 scheduler：

| 原封閉工作 | 清冊內檔數 | 候選 unique allocated bytes | 16:06 狀態 |
| --- | ---: | ---: | --- |
| Round 12 | 40,539 | 7,665,729,536（7.14 GiB） | D 暫存已編碼並掃完，正在原 owner 打包發布；31,583／40,540 個封存輸入完成 |
| Round 13 | 24,119 | 4,011,114,496（3.74 GiB） | 新清冊無 blockers，原 scheduler 排隊在 Round 12 後 |

Round 13 使用原 `workflow_round13_scoped_optimization_acceptance_v1`
獨立 ABI reader；保留原 failed trial、readback repair 及其未證明全流程／
硬體上限的限制。只修正 completion-reader 不認識原格式的問題，不改寫
來源收據。新清冊固定 SHA 為
`3e5666742e7635fec3d550ef607a48759ad58d50d175d27573a6506bdcd8d999`。
相關 inventory／scheduler 317 項與共用 retirement 519 項測試通過。

兩輪約 10.88 GiB 都還未計入實際回收。Round 13 的
`waiting_for_scheduler` 表示等待仍在工作的 Round 12 原 owner，並非死鎖；
不另開刪除 owner，也不搶占鎖。Round 12 的即時堆疊在 D-stage 原檔讀取／
generation 檢查與四個 prefetch readers，尚未到刪除階段。不能用封存輸入
完成率預測整輪剩餘時間，後面仍有完整還原及消費者／退休檢查。

主要新增證據：

- `artifacts/storage_layout/native-original-hash-cutover-20261031.json`
- `artifacts/storage_layout/windows-environment-size-rejection-20261031.json`
- `artifacts/storage_layout/native-large-chunk-batch-qualification-20261031.json`
- `artifacts/storage_layout/native-large-chunk-metadata-cutover-20261031.json`
- `artifacts/operations/native-large-chunk-history-replay-20261031/acceptance.json`
- `artifacts/storage_layout/native-large-chunk-metadata-regression-20261031.log`
- `artifacts/storage_layout/native-large-chunk-shared-paths-regression-20261031.log`
- `artifacts/storage_layout/round13-original-abi-retirement-v2-tests-20261031.log`
- `artifacts/storage_layout/round13-shared-retirement-tests-20261031.log`
- `artifacts/storage_layout/round12-publication-stack-20261031T0806Z.txt`
- `artifacts/operations/agent-workflow/runs/retirement-round13-apply-20261031T080438-94d4f0b3/run.json`

## 17:12 續修：TW 完整還原、原檔尺寸與三節點分工

本節為 2026-10-31 台北時間的後續觀測；保留前述歷史失敗及時間，不把
工程測速、固定 D 還原、NAS 檔案 ACK 和來源退休合成同一個完成率。

16:56:07，原 migration owner 完成 `local-tw-public` 固定版本：
225,361 原檔、84,634,353,640 original bytes 全部 SHA 還原通過，整體進度
由 56／61 增為 **57／61**。原 3,333 個 pack 的封閉計算全部完成：
Penguin 2,759、Vast 574；重新讀取完整 encoded SHA 後才沿原合約重用計算。
固定 manifest SHA 為
`4ecbb9bef99425f350d25be5b6b46f94e38801735733162b1c3c0a09ad81a22a`。

16:56:18，原 owner 只退休此完成工作所屬的 47 個 preparation/control
暫存檔，收據記錄實際釋放 **1,993,469,952 allocated bytes（1.856 GiB）**；
來源原檔、D authority 與 NAS 檔案刪除均為零。這與先前 Round 6–11 累積
70,365,720,576 allocated bytes 分開計算。

TW 這次完整核對耗時 3,014.44 秒，其中兩個各 14,635,485,808 bytes、
1,745 extents 的 original 超過舊的 8-GiB／1,024-extent 單件上限，轉入
較慢的 WSL raw-byte reader。修正只讓單件使用原來已存在的整批
16-GiB／2,048-extent 範圍；1-MiB streaming buffer、每個 extent SHA、
完整 ordered original SHA、before/after generations、mount、退出及
固定程式身分全部保留。超過整批範圍的原件仍走原完整 streaming fallback。

以其中**同一個實檔**核對：完整 14,635,485,808 bytes 及 1,745 extents
通過，耗時 **97.397 秒（143.30 MiB/s）**，沒有向 WSL 傳原始 payload。
這是唯讀工程驗證，不寫正式 recovery journal；近期正式讀取可能命中
Windows cache，未做 cache eviction，不能視為 cold-disk 或整輪加速倍數。
另一次 SHA provider ABBA 小測未見 `SHA256Cng` 的一致 warm 優勢，保留
原 provider，沒有將不明顯的單次差異當成改善。

原三條完整 Temporal history 重播通過，沒有執行 activity/reset。17:07
原 storage worker 正常交接為 PID 969094，載入 `f1db6f73…`；保留
57 個已驗收版本、原設定與持久工作鏈，PostgreSQL／Temporal server 及
當沖服務未重啟。OpenBB 新一輪約 801 萬檔盤點仍在原 owner 執行；
缺失 `/dev/shm/stockagent-universe725-source-build-v128` 所造成的兩個
index-futures gate 與相依 futures gate 原樣保留，沒有刪別名或捏造目標。

本次快速路徑專門測試 15 項通過，共用路徑 511 項通過、2 項 skipped。
另 16 項 layout fixture 在開始前因新出現、尚未登錄的 `data_fugle`
失敗；不是 SHA 斷言失敗，但亦不能宣稱整組全綠。保留第一輪失敗
紀錄及此盤點問題，沒有為了通過測試修改新來源或放寬清冊 gate。

### NAS 私有波次

原私有 collector 另有固定 512-MiB latency target；即使來源設定及
接收回執允許 8 GiB、在途／staging／實體保留量充足，每輪仍先選小批。
改為直接使用所有原 admission 上界的最小值，仍留 envelope 空間及
512 個檔案的原限制，不提高兩端設定，不繞過完整 transport census。
180 項私有／共用備份與 transport retirement 測試通過；涵蓋雙端 batch、
pending、ingress、NAS、staging 六個上界及無實際大型 payload 的選取驗證。

17:09:59，經原 `private-cycle --wait-for-owner` 已實際進入一批
**512 檔、4,294,967,296 bytes（4 GiB）**的複製與獨立核對。
每波 payload 是原約 512 MiB 的八倍；這仍不是已完成傳輸／NAS ACK，
也不代表整體八倍吞吐。它先完成完整 transport census、沿原 owner 的
kernel lock queue 交接，不搶占或重建 lock。這一 census 的堆疊顯示
D 目錄 metadata I/O 等待，不能把閒置 CPU／GPU 當成解碼算力不足。

### 三節點與後續回收

| 節點 | 本次核對的狀態 | 還不能宣稱的範圍 |
| --- | --- | --- |
| Penguin | TW 完整 D 還原通過；Round 12 冷庫全還原及刪除前完整審核已通過，17:10 進入隔離後完整審核；Round 13 原 scheduler 排在其後 | Round 12 的 7.14 GiB、Round 13 的 3.74 GiB 在退休收據出現前均不計為已回收 |
| lab203／NAS | 17:08 最新反向稽核仍有約 592 GiB admission，receiving_backpressure=false，NAS 約 11.18 TiB free；原 owner 正在逐批核對／回收 | 不是全專案或私有歷史全部已同步；舊 retry 數字不能直接視為新失敗 |
| Vast | SSH、Syncthing／Supervisor 可用，已完成 574 個分配 pack；兩張 RTX 5090 為 0%／各 2 MiB；overlay 約 464 GiB free | 沒有剩餘 pack 就不再為了利用率傳重複 raw；不代表 CUDA 訓練或 strict runtime 驗收通過 |

17:05 容量快照：WSL guest free 176,392,421,376 bytes，實體 C free
44,186,263,552 bytes，D free 311,486,230,528 bytes。寫入 admission
仍以較小的實體 backing 空間為準；WSL 刪檔、Windows 實體回收及離線
VHD compaction 是不同事件。當沖 PID 366 保留；沒有改動下載器的啟停
設定、為效能降低容量保留量或刪除未驗收來源。

### 17:23 續修：逐批傳輸通知與最新 NAS 對帳

第一批 4-GiB 波次已在 17:14:19 封閉發布；第二批在 17:18:49 發布。
兩批合計 8 GiB，封套為 `095d79cc…`、`cbdb9dfd…`，第三批進行中。
第一批從進入 copy/verify 至發布約 260 秒，第二批約 270 秒；包括原
source generations、完整複製／SHA、獨立 destination SHA、flush 及派工，
不是 Windows 單次 hash 的 97 秒，也沒有證明整機達到硬體上限。
17:23 兩批仍未看到配對 NAS ACK，沒有將來源封閉發布算成遠端驗收。

私有流程另有一處與公開 controller 不同：通知原來留到全輪完成後，
會使接收端等待後面的複製。改用原 `scan_transport` 與深拷貝的固定
dispatch snapshot，每封閉一批便交給一條背景通知 lane，下一批照常
複製。worker 不操作 ledger；退出時先釋放 publication owner，再等待
網路，之後重新取得原 owner、重載最新 ledger，僅記錄 identity 相符的
scan proof，不覆寫別人提交的派工。失敗保留封套、READY 及原重試。
公開／私有 ledger、配額與 NAS ACK 契約不變。

新增並行語義測試證明：第一批通知和第二批 copy 重疊，網路等待不
占用 publisher，並行提交的 ledger 欄位保留，scan receipt 不冒充 NAS
ACK；通知失敗也不取消已封閉批次。私有／共用備份回歸 **182 項通過**。
顯式 `--wait-for-owner` 同時沿原 capture lock 的 kernel queue 等待，
在接手前不讀／重捕清冊；一般 timer 保留 nonblocking defer。
新版本已排入原 owner 接續，沒有中斷正在複製的舊一輪。

17:21 對第一批執行原 scanner 的單次通知，回報
`deferred_existing_folder_scan`；未寫 ledger、沒有 NAS ACK。這個結果
仍保留，沒有停止現行 Syncthing scan 或把 peer 零欠檔當成全歷史完成。

17:12:53 新固定公開冷庫對帳為 **103,578／103,584** 個檔案已在兩種
已接受 NAS 收據的去重聯集中覆蓋，剩 **6 檔／1,942,320 bytes**；先前
103,578 檔的範圍已全部覆蓋，新分母包含新增 metadata。這是此固定冷庫
cohort 的檔案覆蓋，不涵蓋未發布來源或整個私有歷史。17:15 lab203
最新回執的 admission 約 592 GiB、NAS 約 11.18 TiB free，背壓仍為 false。
Vast 在 17:15 SSH／Syncthing 可用、兩 GPU 0%，沒有新的 pack 工作。

新增證據：

- `artifacts/storage_layout/private-async-negotiated-wave-tests-20261031.log`
- `artifacts/storage_layout/first-4g-private-wave-scan-20261031.json`
- `artifacts/storage_layout/trinode-public-nas-coverage-20261031T0913Z.json`
- `artifacts/storage_layout/trinode-readiness-20261031T0914Z.json`
- `artifacts/storage_layout/trinode-storage-relay-audit-20261031T0914Z.json`
- `artifacts/storage_layout/cooperative-vast-status-20261031T0914Z.txt`

### 17:35 接手與最後等待點

17:27:53，第一輪增大波次的原 supervised run 成功完成：四批合計
**16 GiB** 封閉發布，整輪 wall 1,268.96 秒，`receipt_errors=[]`。
這包含取得原 owner、完整容量盤點、copy／獨立核對／flush／發布及
通知，不是四批的 NAS ACK；原私有固定清冊仍有 630,905,445,258 bytes
未被接受的 NAS 檔案收據覆蓋，其中包含已發送但未 ACK 的部分。
不能直接把這個分母除以來源 copy 速度當成 NAS 完成時間。

17:28:35，新 `private-async-wave-handoff-20261031T092247-c452b536`
沿原 capture／publication owner 接手，live status 已回報
`transport_scan_mode=per_closed_wave_async`。17:33:57 又封閉發布一批
4 GiB（`20d033e5…`），第二批進行中。本次可確認的新增封閉發布量
因此至少 **20 GiB**；配對 NAS ACK 和來源刪除仍須各自驗證。

Round 12 的隔離後 full audit 約 17:25 完成，最後在原
`wait-for-transport-convergence` 等待。17:27:43 觀測：
`stockagent-packed` 自 17:21:31 起 scanning，原設定為每 3,600 秒掃描、
watcher 關閉；need bytes/items、pull errors、watch errors 全為零。
零欠檔不能取代 idle／peer gates；原 25 分鐘 proof freshness 未延長。
沒有刪除或提高同步通過條件，Round 13 仍排在原 retirement scheduler。
這段缺少可靠的掃描完成分母，**不能再報固定剩餘 44 分鐘**。

17:34:44 容量新取樣：WSL guest free 169,226,166,272 bytes，實體 C free
43,827,449,856 bytes（40.82 GiB），D free 289,435,201,536 bytes。
相較前一取樣的淨變化同時含其他寫入，不能當成原退休動作的釋放量；
本次已取得的 WSL allocated-byte 回收收據仍為 **1.856 GiB**，Round 12
及 Round 13 尚不算已回收。當沖 PID 366、lakehouse PID 969094 保留。

補充證據：

- `artifacts/operations/agent-workflow/runs/private-negotiated-nas-wave-20261031T090639-b52335c6/run.json`
- `artifacts/operations/agent-workflow/runs/private-async-wave-handoff-20261031T092247-c452b536/run.json`
- `artifacts/storage_layout/trinode-transfer-gates-20261031T0928Z.json`
- `artifacts/storage_layout/trinode-final-capacity-20261031.json`
- `artifacts/storage_layout/trinode-final-task-milestone-20261031.json`

主要新增證據：

- `artifacts/storage_layout/native-16g-original-qualification-20261031.json`
- `artifacts/storage_layout/native-original-size-bound-audit-20261031T0855Z.json`
- `artifacts/storage_layout/native-16g-original-cutover-20261031.json`
- `artifacts/operations/native-16g-original-history-replay-20261031/acceptance.json`
- `artifacts/storage_layout/native-16g-original-tests-v2-20261031.log`
- `artifacts/storage_layout/native-16g-original-shared-tests-20261031.log`
- `artifacts/storage_layout/private-negotiated-wave-tests-20261031.log`
- `artifacts/storage_layout/windows-dual-sha-provider-abba-20261031T0858Z.json`
- `artifacts/storage_layout/cooperative-capacity-20261031T0905Z.json`
- `artifacts/storage_layout/private-negotiated-owner-python-stack-20261031T0911Z.txt`

### 17:41 三節點最後核對

17:38:36 的新私有 owner 已封閉發布第二批 4 GiB（`2f44a4f0…`），
本次新增封閉發布量因此至少 **24 GiB**，仍不是配對 NAS ACK。
17:39 Round 12 的唯一 transport check 失敗項仍是 `folder_idle=false`；
`retirement.json`／`acceptance.json` 尚未產生，不計入回收。

lab203 原回執 17:41:06 顯示正在 `retirement_exact_name_unlink`，
累積 allocated-byte 回收 819,861,016,576 bytes；這是 **lab203** 的
累積數，不是 Penguin 這次釋放量。接收背壓 false、admission
636,030,385,571 bytes。17:39:58 readiness 顯示 NAS free
12,287,344,771,072 bytes、原 8 GiB batch 上限不變。
Vast 17:41:06 直接 SSH 核對 Syncthing PID 4959 running；兩張
RTX 5090 均 0%、各 2 MiB，磁碟 available 498,404,077,568 bytes。
沒有新的 pack 工作，已完成的 574 pack 不重算。

Penguin Syncthing PID 1375 的 15 秒唯讀樣本只有 8 KiB physical
read、約 4.68 CPU 秒，冷庫經 Windows D 的 9p／DrvFS metadata 路徑。
這個樣本不足以宣稱硬體已跑滿；目前應區分索引掃描、檔案核對與
傳送完成，不能以 GPU 閒置推論可以直接加速這段 namespace 工作。
樣本：`artifacts/storage_layout/trinode-syncthing-io-sample-20261031.json`。

### 18:56 接續：時效重驗、實際容量與通知排程

本輪重新讀取原 run，確認 Round 12 在 **17:51:11** 以 exit 75 退出，
並非一直等待。17:49 的 transport proof 已通過，但最後 generation／
consumer gates 走完後，原 1,500 秒 full-audit 時效已過；原 quarantine
保留，沒有 `retirement.json`／`acceptance.json`，沒有新增回收。

新增 distinct `LegacyQuarantineProofExpired`：只有完整 quarantine binding
仍一致、同一 owned v2 proof、且尚未 unlink 時，才回到既有三次 full-audit
迴圈重新核對。來源／manifest／owner 改變或其他失敗不套用此分支。
原驗證時間不更新、原 25 分鐘時效不延長。Round 12 已以固定原 inventory／
D stage 排入 `round12-proof-refresh-resume-20261031T104552-7d7a1479`。
Round 13 原程序繼續，**18:50:15** 已完成 24,119／24,119 檔、
3,947,870,885 原始 bytes 編碼，18:50:23 進入 `publish-to-D`；其
3.74 GiB allocated 候選尚未退休，不中斷這個 owner。

廣域回歸另暴露一次真實程序 cwd 在檢查中變動造成的中止。修正原
`artifact_process_references[_many]`，在這種 distinct 變動後有界重做
全部 direct FD/cwd 與 ancestor 檢查；重試最多三次，新 consumer 仍阻擋、
權限遺失及持續變動仍 fail closed，不接受舊的 negative cache。

18:52 的私有 live status 已讀入新的 budget 判讀：pending
34,357,485,021 bytes，32-GiB 窗口只剩 **2,253,347 bytes**，最小可選檔案
加原 envelope reserve 需要 **8,613,888 bytes**。這是 pending window
等待，並非來源單檔超過 8-GiB batch。新程式在這種已知 no-fit 時不再
執行無法改變結果的全 transport census；有配置的波次仍須完整 census。

容量已重新量測；18:56 將 pending upper bound 對齊既有 retained transport
256-GiB ceiling。sender／receiver 的 8-GiB batch、512-file bound、24-GiB
staging、256-GiB retained bound 與雙端 64-GiB reserves 均保留。舊狀態的
保守試算仍可容納 8-GiB wave；此試算不是 admission，原 owner 配置前
仍重新核對全部實際容量，不預配置 256 GiB，也不重啟現有服務。

新 `stockagent-backup-notify.timer` 已部署並啟用，每分鐘沿原
`notify-transport --idle-preflight` 重試，與完整來源清冊 capture 分開。
folder busy 時只核對原 paired folder，pending count 明確 unknown，
不讀／寫大 ledger、不佔 publisher、不掃來源。idle 後仍由原 owner
合併原 dispatch 的 scan receipts。18:44–18:46 已有成功執行的 deferred
觀測，這不是 NAS ACK；下載器、PostgreSQL、Temporal 與當沖未重啟。

18:47 ledger 觀測：本批第一個 4-GiB envelope `095d79cc…` 已有配對
NAS file-restore ACK；另八個私有 delivery 約 31.96 GiB 等待 ACK。
固定私有 cohort 為 **29,790／48,276 檔、190,615,969,383／817,226,447,345
bytes** 已有 NAS 檔案收據覆蓋；其餘 626,610,477,962 bytes 不全是已傳送。
公開 cohort 18:35 為 103,590／103,602，剩 12 檔／9,895,265 bytes。
兩者範圍各自固定，不能合稱所有來源已完成。

Windows 18:48 的三點取樣：D logical disk read 約 7.2–9.7 MB/s、
write 約 0.7–1.3 MB/s，平均 I/O queue 3–9、每秒約 971–1,258 次 transfer。
這與小檔／metadata 存取競爭相符，不能推論 GPU 可加速，也未建立物理
硬體上限。`PercentDiskTime` 可能大於 100，未把它當成單碟使用率。
當次 C free 42,825,388,032 bytes、D free 272,419,635,200 bytes，
WSL guest free 163,580,465,152 bytes；不能相加或以 guest free 取代 C。

新增證據：

- `artifacts/storage_layout/trinode-notify-timer-deployment-20261031.json`
- `artifacts/storage_layout/trinode-pending-window-projection-20261031.json`
- `artifacts/storage_layout/trinode-private-wave-observation-20261031T1049Z.json`
- `artifacts/storage_layout/trinode-disk-io-sample-20261031T1050Z.json`

### 19:10 驗收與候選篩選修正

`trinode-retirement-notify-regres-20261031T105423-d8194019` 在 19:04:10
exit 0，五個相關測試檔 **597 passed in 580.91s**。先前失敗 run 保留：
其中五個測試在測試檔修正前已載入，另一個暴露真實 cwd 變動；沒有
刪除其失敗紀錄。這個通過範圍不是整個專案或實際回收已完成的證明。

接續取樣另找到本輪新增候選預檢的效能回歸：`eligible` comprehension
在每筆檔案內重建 `sent | deferred`。當時 48,276 個 catalog 檔案、
81,712 個已送／接受 key，重複建立集合會讓這個純 CPU 步驟放大成
O(N×P)。已改為每波建立一次 `excluded`，候選檢查與 `select_wave`
共用同一集合，複雜度為 O(N+P)，沒有改變重試、優先序或容量規則。

以固定 catalog／ledger bytes 做 ABBA：512 筆等距真實樣本，舊式
1.913／2.037 秒，新式 0.0070／0.0100 秒，四次選出相同 151 筆且
selection SHA 相同。新版全 48,276 筆篩選 0.0339 秒、14,395 個候選。
未執行舊式全清冊計時；此量測不含 ingest、複製、SHA 或 NAS 讀回，
不能宣稱端到端也有相同倍數。新增線性修正後，私有備份測試檔另跑
**46 passed in 11.68s**，exit 0。

本輪剛啟動的 `private-capacity-window-resume-20261031T110524-3926277c`
經原程序身分、phase 和 stack 核對仍在配置前的候選篩選後，以原
runner 的 SIGINT forward handler 有序停止，原 run 保留 cancelled／
exit -2／signal 2。第一次 pidfd 不可用、第二次 unit MainPID 為 wrapper
而不符合身分檢查，均沒有送訊號；最後才向已核對的原 runner 發 INT。
沒有強殺、清鎖、刪來源或重啟原 retirement／lakehouse／當沖程序。
`private-linear-selection-resume-20261031T110957-1c051cb2` 已接續原
private-capture／publication owner，19:10 為 kernel queue 等待 publication
owner；新配置仍須取得原 owner 後通過全部即時容量驗證。

19:06 Penguin 容量：C 可用 **42,692,694,016 bytes（39.76 GiB）**，
D **272,362,676,224 bytes（253.66 GiB）**；WSL guest free
162,927,767,552 bytes。新 retirement 收據仍未出現，**本輪新增 WSL
allocated-byte 回收為 0**。第 12／13 輪候選合計 11,676,844,032 bytes
（10.87 GiB）仍在原流程，不能先計入已釋放。

19:06:35 Vast 直接核對 Syncthing PID 4959 running，兩張 RTX 5090
0%／各 2 MiB，磁碟可用 498,351,779,840 bytes。已完成 574 個 pack
沒有重算，無新 pack job。Penguin 當次 stack 顯示 Round 13 在 D 的
`_collect_entries`，公開備份在原 metadata capture；後者會釋放 publication
owner，不是因讀清冊一直持有該鎖。沒有用 GPU busy 或 SSH 代表驗收。

lab203 19:06 讀到的 readiness：NAS free 12,285,237,411,840 bytes；
audit 回報接收背壓 false、admission 約 560.84 GiB、累積回收
825,374,167,040 allocated bytes，這是 lab203 的累積數。audit 的
observed clock 曾比 Penguin 讀取時間晚約 60 秒，同期 readiness 沒有
相同情況；Penguin 的原時間檢查器三次 host offset 約 -0.94 ms、ready，
沒有修時。已在 [lab203 接續文件](lab203_source_evidence_handoff_2026-10-29.md)
請原 owner 核對兩個產生器的時鐘來源與實際逐批讀回，不改舊收據。

補充證據：

- `artifacts/storage_layout/trinode-status-20261031T1107Z.json`
- `artifacts/storage_layout/trinode-private-selection-abba-20261031.json`
- `artifacts/storage_layout/trinode-private-linear-handoff-20261031.json`
- `artifacts/storage_layout/trinode-penguin-time-20261031.json`
- `artifacts/operations/agent-workflow/runs/private-linear-selection-tests-20261031T110938-492424f6/run.json`

## 22:22 原 owner 的接續結果與通知修復

22:07 的固定觀測保存在
`artifacts/storage_layout/trinode-storage-progress-20261031T140705Z.json`。
以下進度各有自己的分母，不相加成全系統完成率：

| 範圍 | 本次核對 |
| --- | --- |
| WSL 第 13 輪 | 原 `_accepted` 再核對完成收據、清冊 SHA 與 source absence；實際釋放 **4,011,114,496 allocated bytes（3.74 GiB）**，D stage 另 905,039,872 bytes；原 NAS flag 仍 false |
| WSL 第 12 輪 | 原完整 D recovery 階段 4,991.59 秒完成；22:21 已從 encoded stage hash 進入 `verify-encoded-and-quarantined-originals / decode-originals`；7,665,729,536 allocated bytes 尚未退休 |
| 原來源遷移 | 57／61 verified；OpenBB 原清冊 8,013,131 檔。363／363 packs 全部完成，Penguin 323、Vast 40，5,768.76 秒；之後仍由原 owner 核對 blobs／原值及正式 recovery，不把 pack 完成當整批完成 |
| 私有 NAS 檔案覆蓋 | 固定 2026-10-28 清冊 `56013a7926691b2d4d7d37804729adc2d5c1387fa98ea33de366ac13984e9d1c`：33,881／48,276 檔，224,933,764,711／817,226,447,345 bytes（約 27.52% bytes）；不是全專案或完整 canonical 重建 |
| lab203 | 原反向 audit 回報沒有接收背壓；NAS 約 12.268 TB 可用，pipeline 暫無 jobs。audit clock 比來源讀取時間晚約 44–60 秒，保留原時鐘診斷，不改舊收據 |
| Vast | 原 SSH 端點 114.34.26.236:43269，boot 不變；workspace 約 464 GiB 可用，Syncthing 原 Supervisor 持續。兩張 GPU 各約 2 MiB／0%，本次為 CPU／記憶體驗證，沒有開始訓練 |

22:14 來源確認三個新封閉波次約 24 GiB 已發布；抽核兩個批次的 READY
與 dispatch 均存在，但 Syncthing file API 回 404。同期已索引資料的
peer completion 為 100%、need=0，來源仍 scanning；最新私有 NAS ACK
停在 19:48。這表明「索引既有資料已送齊」不能代表新批次已入索引。
原每分鐘 notifier 的 busy preflight 會一直跳過派工，存在通知飢餓。

修復沿 `BackupStream.notify_transport` 與原 timer：明確登錄
`busy_scan_notification_enabled`，每次只排一個 exact closed batch 與 tools
的指定路徑掃描，交由既有 scanner 接續，原 publication owner 不被網路等待
占住。先寫 intent、獨立 notification lock 排除並行重送；HTTP 逾時／程序
中斷後，未確認 intent 等真正 idle 才重試，不能每分鐘累積未知請求。
只有成功回應的 exact dispatch 寫掃描收據；NAS ACK、資料刪除 gates 不變。
相關 [Syncthing scan API](https://docs.syncthing.net/rest/db-scan-post.html)
及 [v2.1.5 同步掃描實作](https://github.com/syncthing/syncthing/blob/v2.1.5/lib/model/folder.go)
已核對。原 writer／reader 的固定版本及來源完整性不因排程改動而改寫。

修復驗證：針對性 **30 passed／96 deselected（6.45 秒）**；原公開／私有
備份、relay、offhost 四個檔案 **186 passed／14 skipped（42.99 秒）**，
run `busy-scan-notification-regressio-20261031T142047-dcc63ac2`。兩次
較早的 workflow 啟動命令格式失敗均保留，不能算測試通過。真正新批次進入
索引／NAS ACK 仍須後續實機收據，不能從測試推定。

後續補上 notifier 的原 owner blocking waiter，避免週期觸發一直錯過短暫
接手點；獨立 waiting 收據、超時未確認 intent 的保護一併驗證。最終
**205 passed／14 skipped（47.20 秒）**，run
`notifier-waiter-regression-20261031T142428-98c2e704`。原自動 installer
已更新 unit；22:26:52 實機確認 notifier 已取得原鎖、釋放後提交一筆
`5786cfc68a0d494ad6bf572fb6e6f4ac5b212f3e14ad2aff43b8539ceec95f0a`
原封閉批次的掃描 intent，仍等待既有 scanner，未當成 ACK。

22:22–22:27 Vast 曾出現 SSH handshake reset／timeout；22:26:36 的成功
重新連線核對同一 boot，Syncthing 22:24 仍 connected。沒有重啟遠端或換
endpoint，也未因短暫成功宣稱 SSH 已穩定；Vast 先前完成的 40 個 pack
收據保留，新工作若遠端失聯仍走已驗證的原本地 fallback。

最終 unknown-request 的三種非 idle 狀態也加入原語義回歸，
**208 passed／14 skipped（45.43 秒）**，run
`notifier-idle-state-regression-20261031T143043-8931ae19`。來源實機接手、
當前 capacity 與未確認 intent 的固定證據為
`artifacts/storage_layout/trinode-notifier-live-20261031T142832Z.json`。

### 未採用的原生小檔案批次候選

對既有 D encoded stage 的完整一個 shard（118 檔、2,140,385 bytes）做
A/B/B/A；原本 portable 四 workers 為 0.27135／0.22674 秒，新原生批次
為 6.29019／4.27409 秒，平均 **0.24905 對 5.28214 秒**，候選約慢
21.2 倍。這是暖快取及當時並行負載下的該 shard 結果，不是完整遷移基準。
候選程式與試驗測試已全部撤回，保留原有 dirty work；恢復後原相關測試
**435 passed（6.96 秒）**。未中斷現行 R12／lakehouse／當沖程序。

五次 Windows 原生磁碟短測顯示 D 平均讀取約 89.4 MiB/s、佇列約 38.6
（最高 54），C 佇列為 0；這只是當時並行負載，不能宣稱硬體峰值。
22:07 實體 C 可用約 37.25 GiB、D 約 141.68 GiB；WSL guest 顯示約
167 GiB 不能取代 C 實體 headroom。22:00 原 fstrim 記錄 8,620,556,288
bytes trimmed，不能等同 Windows C 已增加相同可用量。

保留的可檢查證據：

- `artifacts/storage_layout/materialized-stage-batch-abba-20261031.json`
- `artifacts/storage_layout/materialized-stage-batch-rejected-source-20261031.json`
- `artifacts/storage_layout/trinode-native-disk-sample-20261031T140958Z.json`
- `artifacts/storage_layout/busy-scan-notification-focused-20261031.log`
- `artifacts/operations/agent-workflow/runs/busy-scan-notification-regressio-20261031T142047-dcc63ac2/run.json`

當時三個期貨來源仍等待原 interface migration；其中兩個清楚保留失效的絕對
symlink `/dev/shm/stockagent-universe725-source-build-v128`。另一個工作視窗
擁有來源產生器，本次按使用者要求只檢查，不自行改寫或排除其成員。
歷史 25 個版本／114 物件、TW 六項 runtime 差異與新聞缺件也不由本次
檔案級進度推定已修復。全系統完成時間仍未知，不沿用舊「44 分鐘」。

## 2026-11-01 期貨來源前置條件修復

使用者本次授權修復上述阻擋。10:43 原 journal 仍是 58／61 verified，
三個來源各已失敗 631–632 次；下游 `local-tw-futures` 尚無已提交版本。
這些失敗次數及原收據保留，不把重試當成驗證進度。

查明 `data_tw_index_futures/build_inputs` 中 v128、v153、v157、v165、v175
五個名稱是指向已消失 `/dev/shm` 程式碼工作目錄的絕對連結。其他既存版本
是原始程式碼工作副本；行情、準備來源及訓練資料在獨立目錄。沒有推定五個
消失目標的內容已恢復，也不修改其他視窗擁有的 factorized 產生器。

修復使用 `scripts/repair_futures_build_input_links.py`：先以固定清冊 SHA、
原 migration 身分與程序／設定／recovery hold 檢查限定這五個名稱，再取得
原 operation、shared/private retention 與兩個 publisher owners。來源內先保存
原路徑、link 文字、完整 metadata 及「目標未恢復」描述；原 inode 以同磁碟
不覆寫 rename 移入 `service_data/futures-source-link-repair`。任何 target 重現、
身分改變、引用、異地寫入或已提交版本均拒絕；中斷後只續跑原固定清冊。
這個操作本身不刪 payload，也不宣稱回收空間或完成冷重建。

排程改為觀察已知錯誤 link 的小量 metadata；不變時不重新掃描、封存或增加
失敗次數，變更後立即由原 publisher 完整重試。下游等原前置 release 完整
恢復後接續，已提交 release 的恢復優先順序不變。網頁仍依實際 recovery
receipt 顯示完成率，等待前置時不編造完成時間。

固定 dry run：
`artifacts/operations/storage-single-authority-20261025/futures-build-link-repair-20261101.inventory.json`，
SHA-256 `b0890837e55d3405ad5bf1419dd3ce8c3401e6de4b12cbb4179b34e1e0aa1d31`；
清冊為五個原 link、零當前引用，實際 apply／冷恢復結果另以原 owner 收據驗收。

相關 migration／修復／進度測試 **392 passed（52.94 秒）**，shared packed／
status／transition 回歸 **395 passed（14.81 秒）**。前者包含實際小型全檔
冷重建、rename 後中斷續跑、拒絕改變的 inode／target／引用與原鎖競用；
後者保留既有 snapshot／版本轉移語義。這些是隔離測試，不能代替三個實際
來源的完整原值 recovery receipt。

第一次直接用一般 fintech runtime 執行交接，被原 runtime lock 在修改服務前
拒絕（`lakehouse Mamba runtime changed`），失敗收據保留。正式交接使用原
`bash scripts/run_lakehouse_control.sh python scripts/rollout_lakehouse_source_policy.py`
選擇已登錄 role runtime 與 mount namespace，不改 runtime lock 避開檢查。

### 完整清冊後的第二批修復

第一批後原 publisher 通過 `build_inputs`，在 `preparation_sources` 發現下一個
絕對 link。另做一次不跟隨 symlink 的完整 metadata 清冊，共 **8,683 目錄、
4,684,151 檔、59.83 秒**，不把這次目錄盤點當內容還原驗收。
剩下 16 條 link 中，15 條對應原 owner 明確宣告的 RAM source-validation／
OCR／amendment 暫存 lease，來自重開機前的 boot，物理目標均已消失；原 lease
保留輸入及來源指紋、reconstructible/candidate 屬性。OCR v117 的產生器由
`data_taifex_public_history/rules` 已保留原件產生衍生 OCR，不是把 RAM view
當行情原件。這些已消失 view 的歷史輸出沒有重新驗證或假裝恢復。

第二批固定清冊
`artifacts/operations/storage-single-authority-20261025/futures-expired-link-repair-20261101.inventory.json`，
SHA-256 `b17622dc20ec218706b79b75f934a3cf1aabc34d5474a19d534184755edc2b51`。
原 lease 全部逐 bytes 綁定，必須維持舊 boot、logical/physical 身分與已宣告的
volatile-view contract；完整 lease 原文與缺失狀態也寫入可冷恢復的 provenance。
不把任意 dangling link 或未宣告來源套用這個例外。

唯一仍有目標的 link 是 execution v8q 指向 v8 的 bulk-readback checkpoint。
保留原絕對 link inode 後，改以來源根內的相對 link 指向同一個已存在 checkpoint；
目標 inode／manifest SHA 前後核對，不複製、修改或重建 checkpoint 內容。
新路徑仍經原 packed link 安全檢查，不能指向根外。

兩批實際 apply 分別保存 **5 與 16 個原 link inode**，原始 payload 刪除數及
宣稱回收量均為 **0**。第二批收據為同目錄的
`futures-expired-link-repair-20261101/result.json`；新增 scope 的實際全檔冷還原、
portable link 還原後仍留在 restore 根內、lease 改變拒絕、rename 後中斷恢復
等修復測試 **22 passed（3.00 秒）**。15 個過期 view 保留
`expired_view_payload_recovered=false` 和 `historical_validation_outputs_verified=false`。

10:57 原 worker 已越過所有已知連結阻擋，`local-tw-index-futures` 正在清點完整
來源（當次觀測 441,598 檔、31,134,132,903 bytes，分母尚未知），總驗收仍為
58／61。這不是已完成封存或訓練可用性證明。

程式自然邊界交接曾因原 catalog owner 忙碌，在 600 秒界線安全逾時；原 worker、
鎖、收據及 accepted releases 不變。已用同一交接入口排入 3,600 秒有界等待，
由 `futures-link-natural-worker-handoff-20261101` 原 task run 監督，等待原 owner
完成當前交易後載入；在正式 `accepted` 與 loaded-code receipt 出現前，不宣稱
新排程已在常駐 worker 生效。沒有以刪鎖、清 pin 或強制中斷來加快換版。

### 實際來源推進與面板驗收

11:08 原 publisher 完成來源清點，實際分母為 **4,684,151 檔、
476,478,228,752 logical bytes（約 443.75 GiB）**，開始
`routing_and_hashing`，由原內容雜湊與物件重用流程處理。清點總大小不是
新傳輸量、實體占用量或冷恢復驗收；當前總驗收仍為 58／61。

`storage_progress.py` 現在只讀取與 active dataset、非空 attempt ID 完全
配對的 publication 收據，分開顯示來源清點、內容核對、封存及原始資料
還原。舊 attempt 或已提交 release 的舊 publication 不會成為新進度。
尚無分母時顯示已清點的檔數／大小及「總量待清點」，不編造百分比或 ETA；
取得分母後只從同一階段的實際增量取樣，估時不包含未知的後續階段。
唯讀觀測維持每分鐘一次；公開頁面載入 `storage.js?v=14`，不呼叫下載 API。

本次完整相關回歸 **796 passed（76.25 秒）**；之後新增來源發布與異常
metadata 的六項測試，進度模組最終 **54 passed（2.18 秒）**，與先前回歸
合計涵蓋 802 項不同測試。公開網站 Chromium **19 項 passed**，包含桌面、
手機、未知分母、折疊狀態、過期觀測及注入防護，provider calls 為 0。

`artifacts/storage_layout/futures-source-link-final-evidence-20261101.json`
再次核對兩份原固定清冊 SHA、21 個原 link inode／metadata／文字、15 份
原 lease bytes、相對 checkpoint 的原 target／manifest、58 份 accepted
release 的固定綁定，以及七個受保護服務 PID／啟動身分。全部通過；原始
payload 刪除數及本次連結修復的回收量仍為 0。唯讀 observer／gateway 已載入
面板修改，常駐 storage worker 的排程修改仍等待上述自然邊界交接收據；
兩者不得混稱同一次部署已完成。

## 2026-11-01：來源尚未封存完即可交給 Vast 預驗證

已加入明確啟用的私有壓縮包 compute cache，以及每輪結束 60 秒後接續的
`stockagent-pack-compute-spool.timer`。它只消費原發布程序已安裝的不可變 CAS
物件，預先計算全 encoded SHA、所有 stored CRC 及原檔 SHA／長度承諾。
正式驗證仍由原 owner 綁定精確固定清冊，重新核對 D／Vast encoded SHA 與
來源世代；預驗證不能代替 NAS ACK 或授權刪除。

Vast 暫存改為明確 opt-in；原無 payload 檔的記憶體模式仍為預設。首輪已完成
8 包、約 6.65 GiB，後續繼續；1／4 槽固定 release 完整比較沒有證明四槽更快，
依 D 上傳／遠端解壓量測選擇兩槽流水線。原長時間 publisher 先保留，等其
固定版本提交再自動載入新版正式 worker。詳細角色、邊界、負面測速與驗收見
[Vast 暫存與預驗證](vast_pack_staging_pipeline_2026-11-01.md)。
