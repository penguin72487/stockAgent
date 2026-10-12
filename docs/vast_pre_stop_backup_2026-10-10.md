# Vast 故障 instance 的 Stop 前保全（2026-10-10）

本次範圍是故障訓練節點 `vastai1T`、instance `45963859`、machine
`42054` 的必要保全與 Stop。保留原始來源，不執行 Destroy，不把備份當成
訓練完成、模型 promotion 或 CUDA 恢復。工程任務為
`vast-pre-stop-backup-20261010`。

## 狀態與證據

Penguin 已完整驗證主包 24 檔、13,457,639,649 bytes；最後補封閉的
Git／公開程式 delta 7 檔也完成獨立還原。合計 31 檔、13,540,936,113 bytes。
Vast 於 2026-10-10 11:34:04（台北，03:34:04 UTC）接受 Stop；隨後原 SSH
連接埠明確回覆 Connection refused。現有 container credential 從 Penguin
讀 API 得到 401，因此僅宣告停止請求已接受，沒有宣告 API actual stopped
已讀回。D 完整原值獨立還原已通過；三批 NAS 固定 snapshot 還原與 paired ACK
全部通過，最後驗收時間為 12:22:36（台北，04:22:36 UTC）。本次必要保全
完成，待確認的是 Vast 控制台的實際 Stopped 狀態；沒有執行 Destroy。

私有證據根目錄：
`/var/lib/stockagent/vast-pre-stop-20261010/`。原始保全包為其
`recovery-package/` 子目錄；根目錄使用私人權限，不放入 Git。

| 收據 | 證明 |
| --- | --- |
| `recovery-package/capture-files.json`、`SOURCE-CAPTURE.READY` | 固定包的精確集合、每檔 SHA-256／bytes 與封閉身分 |
| `recovery-source-acceptance.json` | Penguin 全包 SHA、ZIP 原檔內容／mode、ZSTD 原值解碼及 Git 獨立還原 |
| `final-code-acceptance.json` | 最後公開程式版本與最新 Git HEAD 的獨立還原 |
| `vast-stop-receipt.json` | 固定 instance／machine 的 Stop 已接受；外部 API actual 狀態讀回未完成 |
| `private-recovery-cohort.json` | 封閉 Restic deliveries、來源成員對應、固定 NAS snapshot 與 paired ACK |
| `D-recovery-acceptance.json` | 從實際 D deliveries 獨立還原完整包，再做所有原值解碼 |
| `markets/progress.json`、`markets/recovery/` | 原有 canonical legacy return 的逐根目錄 D 重建結果 |
| `final-acceptance.json` | 固定 31 檔／bytes、來源與 D 重建、三批 NAS ACK 和 Stop 讀回限制的最終核對 |

NAS repository ID：
`c717a3d315e20a5a3314c595e66607fcb7d6646cb73ad4412cc2cccb22abb798`。
三批包含來源 31 檔及各批 envelope／READY，共 37 個 delivery 檔案；
每批完整檔案集合、來源驗證器、全檔 SHA-256、固定 snapshot 獨立還原均通過。
repository check mode 為 `structural_plus_fixed_snapshot_full_restore`；
沒有將本次檢查宣稱成整個 repository 的 `check --read-data`。

| 批次 | 完整 snapshot ID | Delivery 檔數 | Delivery bytes | lab203 驗收流程秒數 |
| --- | --- | ---: | ---: | ---: |
| 1 | `c26bb3207273f19431ed86932952588994a256214f3c1333c1d3061ca8d6cb6f` | 29 | 4,951,007,124 | 169.51 |
| 2 | `2b52eae355dfc67fc74db91c19a7554bff28e2b07bdfe0480b67678ad8ea91af` | 4 | 4,294,968,357 | 242.64 |
| 3 | `64983b47ebf14ec43a46446f4eafc0c20cd50d8bb02c296ca627126e6ce4663d` | 4 | 4,294,968,357 | 225.26 |

秒數來自各批 receiver 收據，不包含整段遠端封存、來源傳輸或等待 timer 的時間，
也不代表長期吞吐。`publish-private-recovery-20261010T035324-24be04e0`
已退出 0；本次三批 NAS 待驗數為 0，不代表未來增量或全歷史皆完成。

## 保存範圍

- 初始 Git HEAD `b3b514ea8bdd403e09402ab1dc348d228fa689cd`、最後 Git HEAD
  `ae0a2ea970324e6f1757f8fbf86256559712fdd0` 的完整 HEAD reachable-history bundles，
  初始及最後公開工作樹、原始 modes、Python／Mamba 套件版本收據。
- 38 個 frozen code releases 及既有 operations／歷史研究產物：49,295 個
  原檔、5,616,970,768 bytes。重建所需的 source／wheel 雜湊另外核對。
- 17 個非空 `markets` 結果根目錄：1,099 個原檔、1,568,466,116 bytes，
  包含 32 個現行 checkpoint；保留未完成狀態及 recovery holds。
- 未被既有 NAS 固定版本精確覆蓋的必要原始資料／歷史成果：16,271 個
  原檔、22,973,880,795 bytes，其中 109 個歷史 `.pt` 檔。
  以 canonical ZSTD tar index 保存原值、mode、mtime、uid／gid；壓縮串流分成
  五個不超過 2 GiB 的 parts。解碼比對只排除新的觀測時間，原檔資料及 metadata
  全部必須一致。

來源 SHA 清冊核對 163,096 檔、135,869,610,631 bytes，其中
141,214 檔、68,737,245,234 bytes 已匹配 1,282 個 machine NAS accepted
固定版本。未匹配不自動代表遺失；本次額外保存必要 originals／results。
44,158,484,602 bytes 的已知衍生 cache／明確退休的舊 research views
未另複製，保留在停止後的 Vast 磁碟上，沒有刪除。
這不是完整磁碟映像，也不是所有歷史來源的新增驗收。

原有 legacy return 共 15 個結果根目錄通過 D 原值重建，兩個根目錄因
recovery hold／metadata 變更仍受保護。完整市場保全包已涵蓋兩者，
沒有解除 hold 或假稱原 return 成功；原流程 exit 0 不能取代逐項驗收。

## 保存與傳輸路徑

1. 遠端封閉 archive → Penguin 私人目錄；rsync 完成後獨立核對完整包。
2. 用現有 `BackupStream.publish_closed` 發布不超過 5 GiB 的有界 deliveries 至 D 上的
   `/srv/stockagent-backup-ingress-lab203/batch-<envelope-id>/`。
   `.staging` 不同步，READY、精確集合、全檔 SHA 與原子 rename 通過後才公布。
   實測 D 尚有約 1.83 TB，可用 staging 為 5.68 GB；既有失敗／測試 staging
   約 11.50 GB 保留。縮小本次波次即能沿用現有 16 GiB staging 政策，
   不需刪除其他工作證據或放大正式服務預算。
3. 原 Syncthing 配對 → lab203 Receive Only → 現有五分鐘 Restic runner
   → NAS 加密 repository → 固定 snapshot 獨立還原、來源驗證器、全部外層
   檔案 SHA → paired machine ACK 回傳 Penguin。
4. 本批使用 `private_vast_recovery_backup` kind。完整 Git 歷史實際含私人設定，
   因此只走加密 Restic，不發布至 plaintext immutable lake；不觸發 source
   deletion、transport GC、PostgreSQL restore 或模型部署。
5. Penguin 本機完整驗證通過後即可 Stop Vast；D、NAS 驗收在 Penguin 繼續，
   不必繼續租用 GPU 等待。Stop 與 Destroy 的保存及收費效果不同。

本次沒有修改正式同步 owner、receiver、prune 或刪除政策，也不需要
lab203 的 SSH 或逐批人工轉貼。

## 恢復與重新啟動

先從 `private-recovery-cohort.json` 的完整 snapshot ID 還原各固定 delivery，
核對 dispatch 與 envelope 身分，再將各 `recovery-package/` 和
`final-code-delta/` 成員合併至新的
私人目錄，拒絕不同內容覆寫。重跑本任務 `verify_recovery_package.py` 的
`verify_package(root / 'recovery-package')`，再以 `verify_final_code_delta.py` 的
`verify_delta(root / 'final-code-delta')` 確認最後程式版本。Git bundle 可以還原至
新的 bare repository，checkpoint 保持原 provenance，不執行 pickle 來驗證檔案。

保全腳本與 log 在 `artifacts/operations/vast-pre-stop-backup-20261010/`，
長任務 log 在 `artifacts/operations/agent-workflow/runs/`。Python 執行仍須
`source scripts/runtime_env.sh` 與 `run_fintech_python`，NAS producer 沿用已驗收的
backup runtime lock 及 PID 1 mount namespace，拒絕 D 缺盤時改寫 C。

主機修復、重新 Start 之後，先讀回硬體／runtime，再執行
`run_fintech_python scripts/check_environment.py --require-cuda --strict`。
NVML 或 `nvidia-smi` 可見 GPU 不等於 CUDA allocation／kernel 已恢復；
通過實際 CUDA preflight 與原 checkpoint／資料 ABI 後才恢復訓練。

Vast 官方 [pricing](https://github.com/vast-ai/docs/blob/main/guides/pricing.mdx)
說明停止後仍保留 storage charges；本次沒有 Destroy。

## 大檔驗證的 I/O 修正

封閉 delivery verifier、`BackupStream.publish_closed` 和 `copy_verified_bytes`
新增預設關閉的 `native_windows_io` 選項。本次私有 producer 在 D verification／
還原讀取時選用既有 `windows_cold_io` 的 guarded native Windows reader。
完整 SHA、regular-file／redirect、physical D volume、前後 inode signature、
精確集合及 fsync 保留；lab203 的可攜來源驗證器與正式服務容量政策維持原值。

完整讀取 171,336,045 bytes 真實 sealed part，SHA 與來源一致，耗時
1.582 秒（103.28 MiB/s）。這是單次實際完整檔案 probe，不是全流程或長期吞吐
保證。已完成的 `.staging` 成員交接至更快的原生讀取，取消的舊 run 和
`optimization-handoff.json` 保留；沒有丟棄來源或正在寫入的 partial。

排除重複大檔 pass：同一 canonical publish gate 仍會在原子曝光之前獨立
讀取目的檔全部 bytes／SHA；D 還原時在複製來源的同一 pass 核對完整 SHA，
新 SSD 還原目錄再獨立核對 archive 原值。沒有縮減驗證範圍。

初次 focused suite：111 passed、2 skipped，10.26 秒。加入提交鎖回歸後再跑
112 passed、2 skipped，14.88 秒。兩項 local encrypted Restic
integration 因本機環境未具備而 skipped，不能稱通過；NAS 本次固定 snapshot
實際還原收據獨立驗收。收緊錯誤型別斷言後，新測試再跑 4 passed；
提交鎖修正最後一次 focused rerun 為 57 passed、8.31 秒。
錯誤 SHA／同長度損毀、讀取中斷保留來源及 partial、不覆寫既有 destination
均有測試。完整證據在 `validation-receipt.json`、`native-D-probe.json`。

既有 source `cycle` 等待掃描 futures 時原本仍持有 owner；900 秒的 scan timeout
會使其他已封閉批次也排隊。現在先持久化 publication，再於網路等待期間釋放
owner，重新取得鎖後讀最新 ledger、ingest ACK，避免覆蓋其他 producer 的提交。
測試以實際 flock 在阻塞 scan 期間提交另一筆日誌，確認鎖可取得、提交不遺失。
現有 oneshot timer 會載入更新，無須停用其他正式服務。

本次私有 wake-up 使用一個背景 scan worker、每次不超過 30 秒，失敗記為
deferred，由既有 Syncthing periodic scan 接手；傳輸／NAS 成功仍以 paired ACK
判定，不拿 scan timeout 或排程活躍當成成功。
