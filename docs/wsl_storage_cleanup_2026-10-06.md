# Penguin WSL 儲存清理（2026-10-06）

使用者已授權：依最新資料架構清理 WSL，歷史訓練產物先完成冷保存／恢復，
保留網頁服務所需內容，清除未使用且可重建的快取。工作任務為
`penguin-wsl-storage-cleanup-20261006`，證據在
`artifacts/operations/wsl-storage-cleanup-20261006/`。

## 19:09 的已驗收結果與進行中範圍

WSL ext4 可用 **481,617,084,416 bytes（481.62 GB）**，相對起始
63,145,955,328 bytes 淨增加 **418.47 GB**；`df` 使用率由 97% 降至 77%。正式 acquisition、封存及恢復
仍同時運作，因此 `df` 會浮動。這是 ext4 內部可用空間，Windows VHDX
實體縮小尚未驗證，沒有調整 ext4 保留區或關機壓縮 VHDX。

固定選定 **356 個 authority 根目錄**，扣除網頁索引與分鐘行情服務依賴後為
**354 個 unused roots**，目前 **109 個**已有完整 acceptance，剩餘 **245 個**，
`all_scoped_cleanup_verified=false`。另有明確核對的歷史 hot 不同版本，
使用獨立身分保存。逐根收據與即時狀態在
[`live-cleanup-coverage.json`](../artifacts/operations/wsl-storage-cleanup-20261006/live-cleanup-coverage.json)；
其 refresh 工具只讀取 inventory、retirement、stage-prune 及檔案不存在證據，
不執行刪除，也不以 phase、程序活著或目錄已消失替代驗收。

19:09 的 source／mirror 最後連結收據實際釋放 **252.24 GB**，encoding stage
釋放 **58.23 GB**；既有 Vast 回傳私有 scratch 已釋放 **198.50 GB**，
已知 abandoned partial 釋放 **3.67 GB**。
這些收據不是起始容量淨減少量：部分 stage 是本轮才建立，硬連結、正式
下載與同時進行的驗證也會影響 `df`。只依完整成功 journal 與精確路徑
不存在核對 private scratch；未知 failed／partial recovery 仍保留。隱含網頁
索引 `cache/tw_day_trade_dashboard_indexes` 已確定由服務使用，從這次可刪
範圍排除，沒有將服務所需資料當清理失敗。私有回傳暫存另固定 **58 個
驗收單位、114 個互不重疊的路徑**，已完成 **39 個**、剩餘 **19 個**。
來源及暫存均沒有「路徑已消失卻無對應驗收」或無效收據；只讀監測每分鐘
核對兩組完整範圍，不能只完成 authority 目錄就把私有暫存宣稱完成。

其中 partition cohort 的四份舊 capture 仍沒有正式 packed head，舊錯誤為
`powershell.exe` 不在原 service 的 PATH。authority launcher 已補正，原
producer 正在重試；已將四個固定 dataset 的 canonical prune 排在同一
cohort owner 後，沒有第二個 producer、沒有修改遠端原件。必須先完成
真正 D publication 與獨立完整冷還原，才能將這四份暫存計為清理成功。

目前採用的資料分工是：WSL SSD 保留 raw authority、正在收集／更新的
資料、PostgreSQL 正式狀態、目前模型／ledger 與服務 cache；D 的固定
immutable release 保存離線產物；Syncthing 與既有 NAS 排程接續新發布。
每個 offline archive 都是 `deployable=false` 的保存版本，沒有把它升格為
通過研究、模型部署或全歷史來源驗收的版本。

目前仍保留約 50.06 GB 的正式 minute parity cache、2.14 GB 的訊號 panel、
annual／multi-basis 服務所使用的模型 pin、現行獨立 inference、TAIFEX
history cache、Shioaji 收集原值及 producer state。pin 已對照實際
Discord／market／deployment 設定，沒有因為名稱含「pending」而解 pin。
Binance、FinMind、TEJ、FinLab、OpenBB 等來源資料也不是可重建 cache；
OpenBB 的五百萬檔掃描上限仍明確記為不完整。

九個舊模型根目錄（原值約 10.68 GB）的 shared report 在今天 15:21 更新，
尚未滿既有 12 小時穩定門檻。已固定新 inventory 與原 admission 身分，
以同一 canonical gate 排定 **10/7 03:22（台灣時間）**重新核對；沒有改
來源 timestamp 或降低自動政策。後續若出現 consumer、來源變動或新的
capture 身分，會保留資料與失敗原因，沒有自行擴張刪除範圍。

另兩個選擇權下載 cache 在 17:00 更新，最新僅約 31.28 MB；已固定 admission
與新 inventory，排定 **10/7 05:01** 使用原 gate 重新驗收。九個模型根與
這兩個 cache 的等待均由既有 agent workflow 的 systemd 工作處理，沒有
將排程開始當成刪除成功。私有暫存新增的第 58 單位，是已實際驗收的
`4fd23701b2fcda4cb3759e49` 完整恢復 workspace；原 57 單位盤點另行保存，
不是重新掃描後擴張其他刪除範圍。

16:27 已執行不中斷 WSL 的 online TRIM，成功 discard
**86,223,376,384 bytes**。但實測 Windows backing drive 可用容量沒有
增加，因此只宣稱 TRIM 成功，**不宣稱 VHDX 已縮小**。沒有停止正式服務、
調整 sparse／ext4 reserve 或執行離線 compact。

實際 hot 差異之一是 `data_repair/tw_day_trade_minute_curve`：原目錄
benchmark 為 318,656,279 bytes、10/2 版本，retained hot 為
297,225,456 bytes、9/16 版本。這是 benchmark 子目錄量測；完整 authority
根含 **12,177 檔、2,001,033,155 bytes**，不能拿子目錄容量代表全部。新增
的手動 inventory 只選固定
`/srv/stockagent-artifacts-hot`，使用 `retained-hot-mirror-preservation-v1`
來源標記與不同 dataset 身分；兩個版本各自 full SHA／cold decode 通過
後才清理各自副本；authority 根後來因補查到實際服務依賴而完整還原，見下節。
source physical location 與 repository consumer 配置
分開傳遞，保留真正 repository 的服務、程序與配置檢查。

## 補正分鐘行情服務依賴

本輪清理漏掉分鐘曲線維護的 implicit source 與 execution replay source pins，
誤退役 `data_repair/tw_day_trade_minute_curve`。18:00 的維護於 18:07 報出
缺少來源 pin；網頁可讀取不能當成此維護工作正常的證據。這是本輪清理錯誤。

已從刪除前完整恢復過的固定 D release 還原全部 **12,177 檔**、
**2,001,033,155 bytes**，canonical decode 再核對每個 original SHA-256。
固定 manifest 為 `3cf6bcbc0e1c77bbbb8884095cc5dd3165a577445e8d7f48410e78f97fd7135b`，
snapshot 為 `legacy-wsl-offline-555ddc971d073ecac9c3c-20261006T074939853843539Z-l0-penguin-26beb8d9a84c0f8b`。
18:27 又獨立完整 hash **2,786 個位於還原根的 replay-pinned originals**，
全部符合原 SHA；replay receipt 與 pins 未修改。兩個原 timer/path 已恢復，
同一分鐘維護 service 於 18:27 重跑，18:34 完整流程退出 1，現在失敗原因
為保留中的 research_dataset 分區與舊 pin 不一致，原先缺少還原根檔案的
錯誤已排除。未把這次失敗宣稱成維護驗收通過。

共享 consumer gate 現在讀 canonical local minute defaults，以及目前或配置
override 的 `rebuild_receipt.json` 所有 `intraday_replay.source_files`。
不存在的 pin 仍是待恢復依賴，malformed pin 拒絕清理；已恢復給現行服務的
根在 retry 時保留，既有退休收據不當作重刪許可。監測保留原收據，另標示
服務根已恢復，不以歷史 unlink 數量推定當前可用空間。consumer 回歸
**14 passed**，恢復後 retry 的兩個故障／保留情境 **2 passed**。

同時完整讀取 7,838 個 session/source pins、2,951 個唯一來源，發現
**146 個保留中的 research_dataset 分區**與舊 pin 不一致；這些資料不在
退休範圍，檔案於 15:06 更新，早於本輪分鐘資料退役。沒有覆寫研究原值、
重設 pin 或用同價／近似價替代 byte identity。此項與還原根的 2,786 檔
全部驗收分開記錄，不能宣稱所有 replay source pins 都通過。
證據在 `live-minute-source-recovery/restored.json` 與 `pin-verification.json`。
已驗證現存兩個 `tw-minute-train` 固定 manifest／inventory，9/10 版本符合
135 個舊分區 pin，另 11 個 9/10～9/24 的版本尚未找到。這是 cold inventory
比對，不是 135 個檔案已獨立還原；最新研究原值全部保留。

## 其他獨立冷恢復清理

`/srv/stockagent-strategy-analysis` 的一個舊八子樹 selection cache 有
1,901 原值檔、約 12.54 GB，當前 consumer/process 皆無引用。已逐檔核對
其固定舊 D release，移至同一 canonical partial prune scope，等待完整
D object audit 與選定檔案 independent decode 後才 unlink；控制收據保留。
19:04 已完成：完整核對固定 release 的 1,181 個 objects、
136,687,037,830 bytes，獨立重建 1,901 個選定 original files，再逐檔 SHA
核對及清理。實際回收 **12,544,446,464 allocated bytes**；
`SELECTION_RECEIPT.json` 保留，cold files／來源原值刪除數均為 0。
驗收收據為 `old-strategy-selection-prune/partial-apply-1791284644249172964.json`。

`/srv/stockagent-transfer-quarantine/2026-08-20_ssh_transport` 有 1,002 檔、
6,819,694,284 logical bytes，內部 hardlink 去重後為 **3,411,021,824 allocated
bytes**。原 migration release 不包含這批；已使用獨立 source origin、
`legacy-wsl-retired-transfer-` namespace 與專用手動保存 profile，原相對路徑
不變、不假裝是 hot mirror。新的 preserve profile 回歸 **46 passed**，
正式封存／獨立恢復／同一安全 retirement 已完成，source 回收
**3,411,021,824 bytes**，encoding stage 回收 **533,327,872 bytes**。
固定 manifest `98cb152886a2cc2f80e53377d1ed43a056dbe4bcba8c2f3511054e15ab73483b`，
原 NAS／D objects 刪除數為 0。

另選定兩個 service 未使用的舊研究視圖：
`data_tw_minute/research_dataset_developing_v5`（3,135 檔、17.62 GB allocated）
與 `research_dataset_schema2_volume_bug_20260807`（3,123 檔、13.53 GB）。
原 manifest 為 8/13、8/6；不是目前 `research_dataset` 或 `shioaji_1m`。
使用獨立 `retained-minute-derived-view-preservation-v1` origin、
`legacy-wsl-minute-derived-` namespace 與僅允許這兩個根的手動保存 profile。
未知 raw／其他資料根、nested root、frozen inventory 換作用途均拒絕。
這批 **31.15 GB** 正在保存，尚未列入已回收量。

資料的 physical source 是 repository；process ancestor 掃描的領域則是
`data_tw_minute`。直接 cwd／FD／mmap／config／selected path 與 data parent
仍保護，只持有 repository 的 tmux workspace 不視為使用每份資料。
最初 inventory 含自己的 path operands，已保留其證據；更正後來源 fingerprint
相同、既有 capture／publication 均不存在，沒有改來源 timestamp／cold
provenance。`--minute-view-name` 提供固定名稱選擇，不把 source operands
放到 invoker argv。公開可重現盤點方式：

```bash
source scripts/runtime_env.sh
bash scripts/run_authority_storage_operation.sh \
  scripts/retire_local_offline_artifacts.py inventory --minute-derived-view \
  --inventory artifacts/operations/wsl-storage-cleanup-20261006/FRESH_MINUTE_VIEWS.json
```

共享 consumer gate 同時涵蓋相對 `data_*/...` 和 resolved config 的 `Path`
物件。當前兩視圖再次 shadow 核對無 service references。最終 consumer／
independent config regression **41 passed**；新增保存／來源 scope 完整組
**57 passed**，process scope 補正後的 15 項限定回歸及 named CLI selection
再通過。計數有重疊，不能相加。
19:10 再跑目前完整 local retirement／consumer 組，共 **77 passed，201.82 秒**；
涵蓋新增 profile、來源 namespace、data ancestor process 保護、named CLI、
恢復後服務保留與隱含 consumer。`git diff --check`、七個相關 Python 檔 AST、
三個 authority shell 入口語法與兩份文件本機連結均通過。這些工程回歸沒有
將進行中的根目錄或過期 NAS readiness 宣稱成正式驗收完成。

## 本輪已修正的執行瓶頸

原 ingress cycle／legacy publication 曾在完整 D 讀取、gzip decode 與
等待中一直持有共用 owner。現在 ingress 保留自己的 cycle owner；
完整 C／D read、還原與 SHA 在共用 mutation owner 外進行，只在正式
publish、固定 release／fresh gate 檢查及 quarantine／unlink 交易取得
共用 owner。排隊使用有界 blocking flock，避免輪詢搶鎖飢餓。原
NAS、publication、producer 的範圍與 single owner 保留。

完整 proof 超過五分鐘後會重新探測 Syncthing。1500 秒的完整驗證身分
不因等待而延長；過期就重做完整讀取。peer-only 暫時掃描可以等待收斂，
source、mirror、D generation、pin、consumer 或其他 blocker 仍直接拒絕。
只在已觀測沒有子程序、沒有持共用鎖的邊界交接舊 worker；没有強殺
正在 commit／刪除的交易。私有 scratch 清理可以排在原 cohort owner 後，
原 producer 預設仍拒絕第二個 owner。

完整 D 驗證完成後，C encoding stage 的 immediate apply 現在可沿用同一
owner 的固定 full proof；仍檢查 D generation、stage fingerprint、source
absence、cold-only state、pin、process 與 1500 秒期限，quarantine 後仍
完整 SHA／decode C payload。避免僅為 dry-run/apply 再讀同一份 D 全量，
没有延長 proof 時間。26 項 stage failure tests 與 54 項 stage/local integration
回歸通過，真實 margin v24 的 stage-pruned 收據也已使用新 contract。

一個 interrupted simulation 的 full source/hot 保留在私人 quarantine：
原最後 transport gate 曾失敗。補上 canonical quarantine resume，固定
原 state／plan／path／release，先重新獨立 cold recovery，再 full source/hot
hash 和 fresh consumer/pin/transport gate。13 個故障情境通過；未知、部分
unlink、新 original path 或變更 quarantine 均保留。canonical state 現在
同時保存完成的 retirement receipt，避免 CLI 尚未写收據就結束時重刪。
真實 `recompute-all-20260225-zCLJ9EEm` 已沿用原固定身分重新驗收，完成
source／hot **6,800,228,352 allocated bytes** 與 stage **380,858,368 bytes**
回收。retirement／local 整組回歸 **82 passed**，包含上述 13 項情境。

一個既有 778,344,505-byte D blob 的 NTFS native read alias 因 permission
projection（0777／0744）被誤拒絕。修正只容許 permission bits 的 view 差異，
仍要求 regular file、NTFS identity、size、mtime、uid/gid、nlink、
mount／volume marker／canonical object scope 均一致。Windows／Linux
兩侧完整 SHA 符合既有 CAS、generation 不變，84.37 秒；29 項相關回歸通過。

中斷的已知 fetch partial 使用既有 `data_cache.py prune-partial`，此次明確
manual immediate age bypass 不影響自動七日 retention。dry-run 已完整
重建固定 D 中 100 個 encoded members，100 個 C 檔吻合、allocated 約
3.67 GB；17:05 的 apply 已清除這 100 檔、**3,666,087,936 allocated bytes**，
一個 size mismatch 保留，source／cold object 刪除數均為 0。
33 項 partial/materialized 回歸通過。沒有按 partial 名稱廣泛遞迴刪除。

rule-parser 的 113,249 個 C stage 檔案中，多出一份 511-byte Syncthing
控制收據暫存，和固定 manifest 所對應的正式控制收據 byte-identical。
canonical stage prune 現在只接受精確檔名格式、正式 receipt 已納入固定
集合、size／full SHA 均一致的控制副本，將其記入 plan fingerprint 與
quarantine 後檢查；其他 unknown／changed extra 仍保留。5 個限定故障／
原值還原測試通過，真實 rule-parser 已完成 full cold recovery 和 stage
prune，釋放 **9,861,873,664 allocated bytes**；兩個 rule 根目錄均驗收完成。

一個已完成安全交接的 Bash parent 曾因 live wrapper 被原地修改而回報
127，實際新 worker 已啟動，沒有重啟第二份。wrapper 已原子更新，並以
exec 將固定 in-memory shell body 交給 runtime helper；syntax 與實際
authority namespace help 探測通過，原失敗退出碼保留作紀錄。原 remote
legacy return service 也改為沿用這個 launcher，修正 systemd 去除 Windows
PATH 時找不到 PowerShell 的問題，沿用原 cohort owner／timer。實際在
stripped systemd 環境完整核對 8 個 immutable blobs、123,433,930 bytes，
Windows SHA 與固定 CAS／generation 均相符；缺 head 的私有資料仍保留，
不把環境測試當成該批已發布／已清理。

Windows bridge 的 `UtilAcceptVsock ... failed 110` 曾使一個完全沒有
輸出的讀取被視為零 bytes。受影響 D object 經 Windows／Linux 獨立
完整 SHA 均符合 CAS。新的限定重試只適用於該啟動錯誤與無輸出情況；
writer 還須是 offset 0、全新且前後不存在的目的檔、沒有 READY、
沒有送入 bytes。既有 append、已建立 partial、非空失敗與其他錯誤
不重播；native copy batch 不重播。

同一個真實 383,567-byte、兩個原始成員的 D pack 交錯三輪：完整
copy／CRC／fsync／encoded SHA／original decode 均通過，DrvFs 中位數
0.146 秒，native SSD staging 1.764 秒。只在 stored members 至多兩個、
pack 至多 512 KiB 時使用有界 memory reader，完整 hash 與前後 generation
不變；較大 pack、多 member 與 subset 保留 native staging／容量門檻。
未清 OS cache，沒有把單一 pack 時間宣稱為 NAS 或全鏈倍率。

目前三個直接受影響的 archive／retirement 測試檔完整通過 **92 tests**。
native I/O／publication 限定重試組通過 50 tests；blocking owner／small-pack
組通過 83 tests。計數有重疊，完整測試紀錄在
[`final-verification-development-receipt.json`](../artifacts/operations/wsl-storage-cleanup-20261006/final-verification-development-receipt.json)。
測試通過不等於全部 356 根已清理。

## NAS 邊界

19:06 的 source cohort 覆蓋收據為 955.92 GB available、784.04 GB 已有
NAS 覆蓋、171.88 GB 待驗收，`all_history_backup_verified=false`；這是該
固定 source cohort，不能與新增保存批次或其他管線容量相加。
Syncthing 正反向傳輸已收齊、錯誤 0，但 Restic readiness 最後更新為
18:12:44、pipeline status 為 18:13:05，超過既有 900 秒 freshness gate。
來源實際狀態為 `waiting_receiver_readiness`，不是 NAS 已完成全部新增批次。
沒有修改 freshness policy 或製造 heartbeat；既有 timer 與自動重試保留，
須等待 lab203 真正產生新心跳後才能繼續 Restic 發送。immutable lake 的
NAS cohort ACK 另行持續更新，不能代替 Restic readiness。
USB 金鑰保管已完成，
Windows 登出驗收依使用者要求取消；沒有再要求 SSH 或新增付費資源。
沒有啟用 NAS prune、D object GC 或 ingress 批次自動刪除。

17:02 再測當沖／隔日沖／FinMind／TAIFEX 公開狀態 API，均可讀取。
當沖及隔日沖 source age 約 8／10 秒；TAIFEX 維持原有的
`blocked_subscription_bootstrap_settlement` 與 9/18 source，沒有自行沖銷
或修改 ledger。HTTP 成功不等於 TAIFEX 策略準備完成。

以下保留起始盤點及各階段量測，當前進度以最新機器收據為準。

依第一性原理，檔案保留理由是來源權威、當前執行依賴或尚未保存的唯一內容。
目錄叫 cache、backup 或 preparation，均不足以證明可以刪除。可回收量以最後
一個 inode 名稱釋放的 `st_blocks` 計算，不能相加各根共用的硬連結容量。

13:16 的 ext4 起始量測：總容量 2,163,361,103,872 bytes，已用
1,990,247,215,104 bytes，可用 63,145,955,328 bytes，`df` 顯示 97%。
D 冷庫當時仍有約 1.98 TB 可用。全 WSL 的跨根 metadata `du` 盤點已完成；
同一輪會去除跨根 inode 重複，`/tmp` 為 tmpfs，不能當成 ext4 占用相加。
專案角色掃描完成 133 根，其中 `data_openBB` 達到 500 萬檔的有界上限，
保留 `inventory_complete=false`，沒有把來源掃描上限當成資料可以刪除的理由。
掃描期間 acquisition／備份仍持續，不把取樣容量當成固定常數。

初步分根 allocated 占用：`artifacts` 約 632 GB、replay 約 140 GB、研究／服務
cache 約 106 GB；Vast 回傳兩個私有暫存 cohort 約 254 GB。舊 hot bridge
約 185 GB，包含 hardlink 與可能不同版本，不能假定全數和 repository 重複。
服務使用的約 50 GB minute execution cache、部署模型、即時訊號 panel、
live ledger／行情資料，以及 `data_tw_index_futures/preparation_sources` 原值保留。
`data_openBB`、Binance、FinMind、FinLab、券商原值不是本輪一般 cache 刪除目標。

已完成的第一項回收：canonical compiler 工具手動 `--min-age-days 0`，
保留所有 open／mapped／shared／partial／lock 成員並重查程序及檔案簽章；
共 21,717 檔、1,435,922,432 allocated bytes。這是本次已授權手動清理，
沒有降低正式 timer 的 14 日政策。pip 清除 757 個下載快取；Mamba 使用
tarballs／unused packages／index cache 清理，沒有移除環境或使用 force-pkgs-dirs。
`/tmp` 是 tmpfs，其清理不能列入 WSL ext4 空間回收。

發現並修正兩個實際問題：既有 consumer gate 漏掉 `live_signal_panels` 的
隱含服務預設，已改用訊號引擎共用的路徑 resolver；tmux 的掛載 namespace
與 PID 1 不同，初次 cold 操作被 D mount gate 拒絕，未刪除任何產物。
後續操作經 `run_authority_storage_operation.sh` 進入正式 authority 掛載環境。

新的手動範圍延伸既有 legacy archive／retirement，不另建資料格式：逐根固定
inventory → exact capture → canonical D publish → 獨立完整 cold decode →
canonical fresh dry-run／apply → source 與 hot mirror quarantine → 精確回收 →
再次 cold recovery 後清理 C encoding scratch。archive 為非部署保存，
`deployable=false`、`completion_claim=not_checked`。每根保留 catalog、固定
release identity、原值恢復、retirement 與 stage prune 收據；既有 NAS 自動流程
會將新 packed 發布納入。發布到 D 不等於已取得該批 NAS ACK。

```bash
source scripts/runtime_env.sh
bash scripts/run_authority_storage_operation.sh \
  scripts/retire_local_offline_artifacts.py inventory \
  --relative-root replays/EXPLICIT_OLD_RESULT \
  --inventory artifacts/operations/wsl-storage-cleanup-20261006/FRESH_INVENTORY.json
bash scripts/run_authority_storage_operation.sh \
  scripts/retire_local_offline_artifacts.py apply \
  --inventory artifacts/operations/wsl-storage-cleanup-20261006/FRESH_INVENTORY.json \
  --receipt-dir artifacts/operations/wsl-storage-cleanup-20261006/EXACT_COHORT
```

Vast return scratch 使用原 cohort owner 與完整 D 原值驗證，不需要再登入或刪除
Vast 原件。未知／不完整 staging、尚無完整冷恢復的資料與 source mismatch 留下
具體原因。NAS prune、D object GC、解除 pin、Windows WSL shutdown／VHDX
compact 均不在此操作中。

相關 archive／retirement／consumer regression：109 passed；新增隱含服務
cache 與手動保存的 focused regression：17 passed，兩組範圍有重疊，不相加。
return scratch destructive gates：10 passed；native stage-prune regression：
21 passed；offline simulation／既有 retirement regression：49 passed；
native blob publication／packed/native I/O regression：90 passed。測試範圍有重疊。
完整 source/mirror 清理、return scratch 驗收與清理後服務檢查仍在進行；後續
只以逐根 acceptance 與新鮮 `df` 更新實際回收量，沒有宣稱全部清理完成。

完整離線候選 inventory 有 109 根，其中 106 根未見使用依賴，allocated
分根合計約 267.41 GB（仍有跨根 hardlink，不能當作保證回收量）。排除的
三根為正在使用的 TAIFEX 網頁歷史快取與兩個空目錄。另找到 11 根 historical
simulation 目錄位於 `live`，約 75.48 GB；採專用手動契約，只接受已核對的
`official-close-settlement-*`、`account-addition-*`、`recompute-all-*`、
`tw-day-trade-independent-*`。任何已 promotion 給服務、config/process 引用的
根仍拒絕；canonical `tw_day_trade_simulation`／TAIFEX／data_monitor 不在契約內。

以同一封閉隨機來源在實際 C→D 路徑各交錯量測三次，完整寫入、durable
flush 與獨立 D full-SHA readback 全部通過；沒有清除 OS cache，且正式工作
同時運作。64 MiB：DrvFs 中位數 3.957 秒，native 4.664 秒；512 MiB：
DrvFs 36.386 秒，native 18.583 秒。因此小檔保留原路徑，只有明確啟用且
至少 512 MiB 的 blob 使用既有 Windows FileStream adapter，加上 Linux／
Windows 完整 SHA 對比。這是 byte-copy/readback 量測，不是全封存／NAS 倍率。
手動離線 capture 使用四個 pack bucket，降低多個小 pack 的讀取程序啟動成本；
完整 CRC、member SHA、原值 decode 與 quarantine 門檻照常執行。

同步觀測顯示 lab203 在 11:46 後未再連線，來源仍啟用自動重試，沒有改裝置
身分或將 stale heartbeat 當成功。目前 source readiness 拒絕新波次，佇列保留；
D 本機冷存還原與 NAS ACK 分開記錄，不宣稱這輪新 archive 已完成 NAS 驗收。

14:07 新鮮 `df`：可用 86,352,023,552 bytes、使用率 96%；起始可用
63,145,955,328 bytes，當下淨增加約 23.21 GB。正式 acquisition 與封存工作仍
同時增減空間，因此這個差值與逐檔 allocated 回收量分別保存。
三個根已完成 source/mirror retirement 及 encoding stage 清理：舊 replay、
`cache/tw_public_preopen_raw_v1` 與
`cache/tw_day_trade_execution_v7_official_close_1325`，source allocated 共
21,525,741,568 bytes。Vast 私有 scratch 已回收 3,129,454,592 bytes；後續
批次仍在完整原值恢復，沒有將 running 或 source 缺失直接當作驗收完成。

已用原 SIGSTOP／重查／SIGTERM 交接流程，在 worker 無子程序、未持有共用
寫入鎖的鎖等待邊界升級本輪固定清單；不停止正在封存／還原／刪除的交易。
清單 hash、PID 啟動身分、前後觀测與替換 run 身分保留在交接收據。
新增 `phase.json` 只說明工程階段，不能替代 `acceptance.json`。
local handoff／offline contracts：37 passed，另 phase failure gate：1 passed。

Vast 暫存清理會更新原 owner 的 ledger，明確區分「本機暫存已移除」與
「遠端 source 已退休」。原 return worker 以固定 release 再做完整 D 原值
恢復，沿用 remote source／transport／ACK 門檻，避免重新下載已移除的暫存。
release 改變或暫存重新出現都拒絕自動接手；focused scratch／return／bulk
regression：63 passed。此修改未改 NAS 或 remote source retention policy。

14:25 新鮮 `df`：可用 118,293,917,696 bytes、95%，相對起點增加約
55.15 GB。此為觀測差值，包含同時 acquisition／scratch 的增減。
return scratch 第二批已驗收，兩批共 28,697,849,856 allocated bytes；
原 worker 已在 childless、未持共用鎖的等待邊界安全交接。先前兩批的 ledger
補登仍需新 worker 完整重驗固定 D 原值，不因既有收據直接假定完成。

期貨 preparation 再拆成 213 個明確 execution materialization、歷史訓練
輸入與診斷版本；分根 allocated 約 26.70 GB，hardlink 尚未去重，不能當成
保證回收量。`all_products_rule_facts` 維護權威、正式來源 archive、current
fetched kbars 與其備援仍保留。新的完整 metadata inventory 會記錄每一 inode
的完整簽章；僅容許其他已驗收根刪除硬連結造成的 nlink 減少／ctime 增加，
實際 SHA、原值還原與刪除門檻照常重新執行。

同一組八個真實冷庫 blob（123,433,930 bytes）交錯量測三輪完整 SHA：
逐檔 native hash 中位數 6.8286 秒，batch hash 1.5382 秒。完整雜湊與
before/after generation 相符；沒有清除 OS cache，也不推算整條 NAS 倍率。
每批限制 16 物件、單物件 8 GiB、總共 16 GiB；超大物件仍完整串流雜湊。
native hash／publication focused regression（含總 byte budget 與超大物件
corruption fallback）：12 passed。scratch proof／remote return 回歸：41 passed。

完整 D/source/mirror 核對移出共用 mutation owner，以原始時間的 1,500 秒
上限及每個物件／完整 source/mirror/archive generation 綁定再取得刪除權；
鎖內刷新 usage／pin／lease／transport，quarantine 後仍完整 source SHA。
過期或 generation 改變會拒絕刪除，不能用 stat 延長 full recovery 收據。
此優化的驗收與剩餘根回收仍在進行，不宣稱全部完成。

14:58 更新：五份固定清單共 **335 根**，其中 **6 根**已有完整 acceptance，
剩餘 329 根仍在處理。source／mirror allocated 回收 41,283,837,952 bytes，
encoding stage 回收 7,760,465,920 bytes，兩批 private return scratch 回收
28,697,849,856 bytes。npm download cache 另由官方 `npm cache clean --force`
回收 1,390,829,568 allocated bytes，清理前未見程序引用。
新鮮 ext4 可用 **127,892,385,792 bytes**，相對起點淨增加
**64,746,430,464 bytes**。這些數字和目前完整 pending 名單固定於
`live-cleanup-coverage.json`；stage 可能是本輪才建立的暫存，不能把所有
unlink 收據相加當成起始容量的淨減少量。尚未驗證 Windows VHDX 實體縮小。

實際發生的零位元組恢復失敗來自 WSL→Windows bridge 啟動逾時
`UtilAcceptVsock ... failed 110`。同一個受影響 D object 經 Windows native
與 Linux 串流完整 SHA 均符合 CAS 名稱，沒有以此錯誤判定 D 已損毀。
新的 read adapter 僅在還未輸出任何 bytes、stderr 符合該特定啟動錯誤時
最多重啟三次；實際內容損毀、非空串流失敗和其他錯誤維持拒絕並保留證據。
Windows backing-drive 容量查詢採相同限定重試，仍須解析實際發行版／VHDX
及 Windows 實體剩餘容量，不用 ext4 的虛擬容量替代。

IO／capacity／native publication 回歸：54 passed、1 skipped；handoff 回歸：
22 passed；已完成 scratch 的 ledger 補登重試與後續獨立批次回收：2 passed。
三組本輪 worker 已在原本未持鎖、沒有子程序的等待邊界安全交接，固定
inventory SHA 未變。批次配置／服務引用盤點實際完整耗時 **25.16 秒**，
核對 213 根、10,530 檔；未把中途停止的舊掃描當作完整可比 benchmark。
同組三個真實 D 小 blob 的 copy／fsync／還原後完整 SHA 三輪交錯量測，
DrvFs 中位數 6.020 秒、native 4.863 秒，保持既有 native recovery 路徑。
清理與 acquisition 仍同時執行，尚未宣稱整台 WSL 已整理完畢。
