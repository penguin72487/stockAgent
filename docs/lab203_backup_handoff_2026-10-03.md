# lab203 本機 Codex 備份交接（2026-10-03）

lab203 由這台 Windows 電腦上的 Codex 在本機建置，只承擔備份接收與 NAS
備份工作。使用者已取消 SSH 連線方案，並進一步明確授權
**lab203 的舊 StockAgent 資料可完全丟棄、直接永久刪除，不需備份、遷移或恢復證明。**
先核對本機專案目錄並解除舊 Syncthing folder 分享，避免刪除傳播至其他節點；
授權不包含 Windows、其他使用者資料、penguin、Vast 或 NAS 既有內容。
使用新的專用目錄，不接管舊的同步資料夾。penguin 仍是資料／網站權威，
Vast 仍是可替換的訓練節點。

目前已收到使用者轉交的兩批 NAS 檔案備份／還原與完整部署摘要；第二批正式
snapshot 是 `d9cf528ac01b42408e00ad41cc3891f7d82ab924c6361dea0ba539836ce32317`，
401 檔、409,347,973 bytes 全部核對通過，完整流程 21.29 秒。
首批固定 snapshot 為 `2145243446a4b6b3a29523cbfd3efa8d4001f378a1196d296bb377a33ef25444`。
先前 `45ae68ecb01f77649ff72dcfc7c747dd8b05b6e5a228d32924daffce6d3f4a11`
的驗收保留為較早版本，不覆寫原收據。
接收 SHA、NAS read-data／獨立還原、金鑰副本恢復與五分鐘排程故障恢復
已回報通過。追加回報確認兩個 Windows 檔案的本機 SHA 與交付位置、實際
repository 與多批次工程路由。使用者明確取消 Windows 登出驗收，維持常開機、
不登出的運作範圍；登出未測試，也不宣稱通過。金鑰異機保管已選定受控 USB，
實際保管待確認。新的接續工作是
[持續增量同步、回傳收據與 NAS 還原](continuous_nas_backup_2026-10-04.md)。
兩批驗收不代表全歷史、NAS packed 重建或 PostgreSQL logical restore。

## 給 lab203 Codex 的任務

最新接續任務見 [2026-10-04 交接](continuous_nas_backup_2026-10-04.md#lab203-本機-codex-的接續任務)。
下面 TAIFEX 任務已回報完成，保留為第二批驗收的歷史契約；不需要重做已通過的
刪除、平台比較、Windows 交接檔及兩批備份。

> 請驗收第二批官方 TAIFEX 資料，沿用現有 runtime-lock 對應的 Mamba role、
> WSL/systemd 五分鐘排程、單一 owner 與既有加密 NAS repository。
> penguin 已發布並觀測同步完成；批次在 `/srv/lab203-backup/ingress/` 下的
> `batch-470e7fcbe03d26223007e280cc13aa42fd22bc8dc0d85d546f4c33d121672ede`。
> 預期規範化 envelope 身分為
> `470e7fcbe03d26223007e280cc13aa42fd22bc8dc0d85d546f4c33d121672ede`，
> 原始 `backup-envelope.json` 檔 SHA-256 為
> `2a1ce4618261f0607c8e057b2c42869eea4677e084b0134b652a18afb8f7cae0`。
> `tools/taifex-batch-handoff.json` 的 SHA-256 應為
> `0a3e2aaf6d52d19a7d61a565c658fa41a71da571479b013bfb75ed25729bc971`。
> 先確認接收端 idle、待收／errors／local changes 為 0，再沿用來源 portable
> verifier 核對 READY、399 個 envelope 成員與精確集合；含 envelope／READY
> 共 401 檔、409,347,973 bytes。收據保存在同步樹外。
> 完成加密備份與 `check --read-data`，保存本批新產生的完整 snapshot ID，
> 固定該 ID 還原到獨立目錄，重跑驗證器並比對全部 401 檔 SHA-256。
> 回報 repository ID、完整 snapshot ID、全部退出碼、來源／還原逐檔 SHA
> 與完整備份／檢查／還原／驗證流程秒數；packed 重建與 SQL 還原分開記錄。
> 協助使用者確認 Restic 金鑰在另一台受控電腦或外接媒體的可取回副本；
> 只回報目的地代稱與恢復方式，不回傳金鑰內容、不放進 ingress。
> Windows 交接檔已完成，登出驗收已依使用者指示取消，保留免登入喚醒與
> Ubuntu runtime holder。將本輪結果追加至原部署證據；維持 prune 與批次
> 自動刪除關閉。

## 已知設定與授權

| 項目 | 設定／邊界 |
| --- | --- |
| NAS | QNAP，`140.127.208.143`，共享名稱先核對 `Lab203` |
| 使用者目錄 | `學生上傳區/錢昱名`，只在這裡建立新備份 repository／試驗檔 |
| NAS 存取 | 使用者已有帳號；Windows SMB 一般共享權限即可，無須 NAS admin SSH |
| VPN／容量 | 目前直連不需 VPN、可見剩餘約 14.08 TiB；使用者確認帳號無配額上限，SMB 管理端查詢為 `NT_STATUS_ACCESS_DENIED`，未獨立查證管理端配額 |
| 新同步 Folder ID | `stockagent-backup-ingress-lab203`；來源端已配對，僅分享新備份 transport |
| penguin Device ID | `QZTXXEL-YBCBYK7-ZK2ZSMS-DQKVSCE-IFC7MIX-4CWG6LE-KHPDLJI-7DSE6QW` |
| lab203 Device ID | 使用者已確認 `TLOI2HH-6EZOSBC-H6YMGVW-APQOGFW-P2A7FZT-EKX6IUP-2HMRKS6-LBDHOQM`；來源端已觀測此身分的 QUIC／TLS 連線 |
| 新接收資料夾 | 使用者確認 `/srv/lab203-backup/ingress`、Receive Only；penguin 是 `/srv/stockagent-backup-ingress-lab203`、Send Only |
| 生產者 | penguin 發出固定版本、pack/blob 與封閉 envelope；lab203 不發布新 cold heads |
| 舊資料 | lab203 本機舊 StockAgent 資料可直接丟棄／永久刪除；先解除舊分享，無須恢復證明 |
| NAS repository | `/mnt/lab203-nas-cifs/學生上傳區/錢昱名/lab203-restic-20261003-210839-9ba17ccf/repository`；format 2，repo ID `c717a3d315e20a5a3314c595e66607fcb7d6646cb73ad4412cc2cccb22abb798` |
| lab203 硬體 | i7-14700F、20 cores／28 threads、64 GB、單 socket／單 NUMA；採用本機已測的 WSL 路線 |
| Windows 運作範圍 | 使用者表示不關機／不登出，取消登出驗收；S4U Session 0 成功不提升為登出或冷開機通過 |
| 保留政策 | 首輪不執行 `forget`／`prune`；鏡像的 remote delete 不能當作版本備份 |

不要接受舊的 `stockagent`、`stockagent-desync`、`stockagent-artifacts-live` 或
`stockagent-artifacts-hot` 邀請；不要複製裝置憑證、API key、DB 或 node ID。
使用者已交回本機 Device ID 與新接收路徑；penguin 已完成發送端設定。
舊資料只在 lab203 本機刪除，新資料使用全新專用路徑。

## 先清除已授權的舊專案資料

辨識舊 StockAgent 資料根目錄、writer 程序／服務／排程與 Syncthing folder。
先停用不再需要的專案 writer、解除舊 folder 分享並保存最小路徑／操作收據，
再刪除核對過的本機資料根目錄；不需要逐檔備份、hash 還原證明或等待期。
不要沿 symlink／junction 刪除其他位置，不要對整個磁碟或其他使用者目錄遞迴刪除。
保留 Syncthing 身分及新服務所需程式碼，不啟動已退役的 hot transport。

## 先在 Windows 量測

[`prepare_lab203_backup.ps1`](../scripts/prepare_lab203_backup.ps1) 相容 PowerShell
5.1／7；腳本預設只記錄軟體、硬體、磁碟、程序名稱與同步 folder 設定，
不遍歷舊資料樹。它在使用者的 LocalAppData 建立新的私人 NTFS 收據目錄。

```powershell
.\scripts\prepare_lab203_backup.ps1
.\scripts\prepare_lab203_backup.ps1 -TestNas -ConfigureNasCredential
```

第二個命令在本機開啟 `Get-Credential`。NAS 密碼保存為 Windows DPAPI
加密 credential，只有同一電腦、同一 Windows 身分可以解密。
已有 SMB 登入可直接使用 `-TestNas`；重新執行時各自寫入新的收據目錄。
NAS 探測預設三個獨立 32 MiB 隨機檔案，包含 flush、讀回與 SHA-256；
只移除本次自行建立的試驗檔，不遞迴刪目錄。
同一 client 的讀回可能命中 cache，**不是 NAS 冷還原或抗斷電證據**。

請記錄 share/user quota 與可用容量，不能只靠網路連線或 folder 存在。
先比較現有 VPN 開／關的授權存取；不得改全域路由或任意啟停其他服務。
SSH 不在工作流程中。

## 獨立環境與完整工作量比較

使用既有 Miniforge／Mamba；若未安裝，從官方 Miniforge release 取得對應
Windows 架構的固定版本與 SHA-256，只安裝本角色需要的工具。

原生 Windows role 在 [`backup-windows.yml`](../configs/environments/backup-windows.yml)：

```powershell
$backup_prefix = Join-Path $env:LOCALAPPDATA 'StockAgent\envs\backup-20261003'
mamba env create --yes --prefix $backup_prefix --file .\configs\environments\backup-windows.yml
mamba run --prefix $backup_prefix restic version
```

`restic 0.19.1` 的 conda-forge win-64 package 已由官方 metadata 核對存在，
SHA-256 `27406d7facc4674652ee1251a0d12e370515312190afb5f0f7011f9f44397c34`。
這只證明套件存在，不是 lab203 安裝／執行驗收；來源為
[conda-forge 官方 metadata](https://api.anaconda.org/package/conda-forge/restic)。
從實際 installed Conda metadata 保存 package URL／SHA-256 explicit lock，
並核對原有 Python／Miniforge role 沒有變動。不要在未知平台套用 Linux lock。

若已有可用 WSL，才用 [`backup.yml`](../configs/environments/backup.yml) 評估
Linux role。原 cold publisher 依賴 `fcntl`；原生 Windows 不直接執行它。
不要為了備份複製 Linux publisher 或搬動 CUDA／網站／盤中程序。

對同一個固定批次，先分開記錄首次建立／cache priming，再交錯各跑三次。
每次完整計時包含環境就緒、NAS mount/login、全部輸入讀取、加密 backup、
repository check、NAS restore、全部 SHA-256、必要的 canonical reconstruction
及收據寫入。記錄 CPU、記憶體、NAS 寫入 bytes、磁碟占用和失敗。
現有硬體支持雙 socket／NUMA 時，再測 worker／CPU 配置；不沿用 Vast 參數。
以相同還原結果與容量需求，選整條工作流程的實際勝出方式。

## 新批次接收與 NAS 還原驗收

penguin 已實作封閉傳輸：`source-plan.json`、`cold/` immutable objects／metadata、
`backup-envelope.json`、`READY`。批次只有固定版本，不含 live workspace、
密鑰、Syncthing node-local state 或正在寫入的 Restic repository。
封閉完成前不能分享 partial batch。第一批是有界的真實來源試驗；不是 527 GB
整批傳輸指令，確認 lab203／NAS 容量後再決定批次大小。

資料機已備妥並分享第一批；來源端於 2026-10-03 21:22（Asia/Taipei）
觀測 peer completion 100%、remoteState valid、needBytes／needItems／needDeletes
全為 0，sender idle／errors／pullErrors 正常。這是來源端 transport metadata
證據，接收端逐檔 SHA、idle／errors 收據與 NAS 還原仍由 lab203 驗證：

| 項目 | 固定值 |
| --- | --- |
| 來源 | `cftc-legacy-pre2000` 的固定 canonical release；不是 lab203 舊檔案 |
| 來源端目錄 | `/root/stockAgent/artifacts/operations/two-node-lab-nas-backup-20261003/pilot/lab203-ready-delivery` |
| lab203 批次目錄 | `/srv/lab203-backup/ingress/batch-350a50881fdf7ae082c77e03671c6ae61bdcb0d92ea1bfa4206f64c415c0803f` |
| envelope SHA-256 | `350a50881fdf7ae082c77e03671c6ae61bdcb0d92ea1bfa4206f64c415c0803f` |
| envelope 列入檔案 | 11 個，63,163,322 bytes；另有 envelope／READY |
| 已驗證 | 資料機端 canonical object 驗證、封閉批次逐檔 SHA、加密備份／獨立還原 |
| 交接包 | 只有工具、設定和文件；不包含這個資料批次、備份金鑰或 DB dump |
| 已分享工具 | `/srv/lab203-backup/ingress/tools/verify_backup_delivery.py`；SHA-256 `fd9ba053468e3499346ac7b90c23c7d22b83722a4d6dfe15a72e2e727e9a6f30` |

首批傳輸時新 folder 為 14 files、63,171,869 bytes（含 envelope、READY 與工具）；
envelope 的 11 個固定成員仍為 63,163,322 bytes。這兩個分母不可混用。
新 topology 與首批身分記錄於 [`backup_ingress.json`](../configs/data_sync/backup_ingress.json)。

來源端的完整重建、環境與測試結果見
[`two_node_nas_backup_2026-10-03.md`](two_node_nas_backup_2026-10-03.md)。
只分享這個封閉批次與必要驗證工具，不分享整個 artifacts 目錄或套用歷史同步邀請。

從 penguin 取得該批次固定 envelope SHA-256 後，用原生 Windows 的 stdlib 工具
[`verify_backup_delivery.py`](../scripts/verify_backup_delivery.py) 驗證：

```powershell
mamba run --prefix $backup_prefix python .\scripts\verify_backup_delivery.py `
  --root '全新接收資料夾\批次目錄' `
  --expected-envelope-sha256 'penguin提供的64字元身分' `
  --output '私人收據目錄\received.json'
```

使用者提供的目前接收路徑是 Linux／WSL 路徑。請使用已建立的本機 Mamba
備份角色執行接收工具，`--root` 指向上表實際批次目錄，`--output` 指向
同步樹外的全新私人收據路徑；不要套用另一個系統的 Windows prefix。

只有當 Syncthing `idle`、need bytes/items/deletes 為 0、errors/pullErrors 為 0，
且 envelope 全檔 SHA-256 通過，才使用此固定批次進行 Restic backup。
驗證器會拒絕未知檔案、錯誤身分、改動 bytes、symlink／junction。
收據必須放在接收樹外，並置於上述私人 NTFS 目錄。

NAS Restic 密鑰在本機新私人檔案建立，與輸入資料夾分離，禁止放進同步內容、
Git、命令列 password 或聊天室。使用者另保留一份可恢復密鑰；從那份副本
完成一次還原，才能聲稱機器故障後仍可恢復。
輸入批次封閉且唯讀，NAS 是另一台機器上的加密 repository。
禁止同時兩個 owner 修改同一 Restic repo，禁止同步正在寫入的 repo。

對首次成功的 exact snapshot：

1. 保存完整 Restic snapshot ID、repo ID、批次 envelope ID、完整命令 exit code。
2. `restic check --read-data`，從 NAS restore 到新獨立目錄，不能用 `latest`。
3. 在還原出的批次根目錄重跑上面的 byte verifier，固定同一 envelope 身分。
4. 如有 WSL，沿用 canonical `resolve_packed_snapshot_id`、`verify_packed_snapshot`／
   `fetch_packed_snapshot`，核對獨立重建的 source fingerprint；否則將固定還原
   envelope 的證據交回 penguin，canonical 驗證仍由資料機持有。
5. 控制 DB 若列入另批，必須是 canonical exported same-MVCC dump，另行還原
   並核對 logical state。檔案 SHA 通過不能替代 SQL 還原。

NAS 固定 snapshot 獨立還原、完整檔案／byte proof、密鑰副本恢復與目標
路徑匹配，可標記該批次離機檔案備份通過。packed 重建、DB logical restore
與金鑰異機保管各有獨立證據；不得由檔案還原自動提升為完整災難復原，
也不能外推到其他批次或全歷史。

## 排程與交付

原生 Windows 使用 Task Scheduler；已存在 WSL/systemd 的 role 才用 systemd。
排程必須用已驗證可解密 credential、可連 NAS 的相同身分；SSH／SYSTEM／
不同登入帳號的 reachability 不能替代此證據。使用單一 owner 鎖、退出碼、
失敗重試與固定 snapshot receipts；禁止將未備份批次自動刪除。
排程上線後依部署的運作範圍測量非互動執行、程序重啟和 NAS 暫時不可用時
的恢復。此節保留初始部署契約；本輪 Windows 登出驗收已由使用者取消，
不列入待執行工作，也不標記已通過。

交回：本機 inventory、NAS 路線／讀寫／容量、Windows explicit lock、三次
完整比較、Device ID／新 folder path、exact NAS snapshot restore、密鑰恢復、
canonical／DB 相關證據以及 Task Scheduler 或 systemd 的正式收據。
另交回舊專案根目錄、已停用的服務／排程、已解除的 folder 分享、實際刪除結果
與釋放容量；目前 penguin 尚未登入 lab203 或代為刪除。

使用者最新要求改為直接貼文字交接；較早 `lab203-codex-handoff.zip` 中
「排除舊資料、不刪除」的文字已被本次明確刪除授權取代，勿套用舊範圍。

## 已補送的兩個 Windows 交接檔

使用者已確認檔名，資料機已補送到 `/srv/lab203-backup/ingress/tools/`：

| 檔案 | bytes | SHA-256 |
| --- | --- | --- |
| `prepare_lab203_backup.ps1` | 10,461 | `459cd0b05472e3f3f4bf0f3cb38755d8edf40b17cd386acc8666c44509da5ed8` |
| `backup-windows.yml` | 106 | `82e96b7db610daa3f3289451a16de1bcd000fcf7bce23c471b64f1cda51db378` |

同目錄的 `windows-handoff-manifest.json` 為固定檔案清單。來源端已觀測新的
17 files／63,183,135 bytes 完整同步。使用者 14:26 UTC 追加回報 manifest
及兩檔 SHA 相符，已原樣交付至 `C:\Lab203Backup\handoff\`，沒有替換現行
WSL/systemd 五分鐘排程；此項已完成本機回報驗收。

金鑰異機保管由使用者在另一台受控電腦或離線媒介完成；只回報存放代稱、
可取回確認和恢復流程，不回傳金鑰內容、不放進 ingress。服務重啟與 S4U
Session 0 通過，均不代表 Windows 登出／冷開機恢復通過；本輪不再安排登出。

## 最新部署回報的剩餘工作

使用者詳細回報 WSL 已釋放 533.528 GiB，舊分享／舊專案資料已移除，沒有啟用
SSH；Windows VHDX 縮小未驗證。採用 WSL/systemd 五分鐘排程，非互動執行、
中途 SIGKILL 接手與 NAS 實際封鎖後恢復已回報通過；Windows 登出驗收已
取消，保留 S4U 喚醒與 Ubuntu runtime holder。

兩個 Windows 檔案已完成本機 SHA 驗收與 `C:\Lab203Backup\handoff\` 交付。
Miniforge 26.5.3-0、Mamba 2.5.0、Python 3.12.14、Restic 0.19.1、Syncthing
2.1.3 與 Windows／Linux explicit locks 為 lab203 回報的實際環境；資料機沒有
直接讀取遠端 environment-receipts，也不套用資料機的 Linux lock。

金鑰副本仍在 lab203 本機，異機保管完成後只回報受控目的地代稱、可取回
確認與恢復流程。實際 NAS repository／repo ID 已提供；帳號無上限由使用者
確認，獨立管理端 quota 查詢被拒絕。多批次工程路由 2 個測試與基礎 4 個
測試已回報通過，真實第二批 NAS 還原仍待驗收。
最新完整原始證據由 lab203 保存於
`/mnt/c/Lab203Backup/evidence/deployment-report.txt`；這不是 penguin 的本機檔案。

下一批官方 TAIFEX 固定版本已由 penguin 完成本機 capture／copy／canonical
archive／object 驗證與 399 個 envelope 成員逐檔 SHA，合計 409,255,477 bytes，
另有 envelope／READY；本機完整準備耗時 64.118 秒。已重新驗證並以同檔案
系統原子 rename 發布，來源端觀測同步完成；第二批 NAS 備份／還原尚未
取得回報。發布重新驗證與同步觀測合計 51.341 秒。詳細固定身分與
本機證據見 [`two_node_nas_backup_2026-10-03.md`](two_node_nas_backup_2026-10-03.md)。

## 來源文件

SMB 一般使用者權限參考 [QNAP 共享連線說明](https://www.qnap.com/en-us/how-to/faq/article/how-do-i-map-a-nas-shared-folder-as-a-network-drive-in-windows-using-qfinder)。
Windows credential 範圍參考 [Microsoft Export-Clixml](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.utility/export-clixml)。
Restic 加密 repository／密鑰參考 [官方 repository 文件](https://restic.readthedocs.io/en/stable/030_preparing_a_new_repo.html)。
同步 folder type 的刪除傳播參考 [Syncthing folder types](https://docs.syncthing.net/users/foldertypes.html)。
