# 恢復與回收判準

先讀本次 checkout 的 `docs/agents/storage.md` 中 exact recovery、consumer、
lease／pin、transport 和 quarantine 契約；此檔教如何找證據，不能替代工具的 gate。

## 各種完成證據

| 證據 | 可說明的範圍 | 尚不能推定 |
| --- | --- | --- |
| service active／HTTP 200／exit 0 | 程序或單次命令狀態 | 資料完整、全歷史備份、業務恢復 |
| sealed envelope、READY、精確集合與 SHA | 本批來源身分／檔案有效 | 接收端已到齊或 NAS 已驗收 |
| paired Syncthing 全 gate 收斂 | 對應索引／delivery 已傳送 | 原值可冷重建、NAS fixed snapshot 還原 |
| NAS fixed snapshot 的完整 restore／SHA ACK | 固定批次的離機檔案恢復 | 每個 packed release canonical 可重建、當前 DB 狀態可還原 |
| packed 全檔重建／PG logical restore semantic receipt | 指定 source release／same-MVCC DB logical state | 其他版本、尚未發布資料、完整全歷史 |
| 新鮮精確 ACK、dry-run／apply／quarantine 收據 | 實際已退休的根／名稱與 allocated bytes | 其他根／別名也可刪或全機清理完成 |

從 `tools/recovery-index.json` 找固定完整 NAS snapshot ID 及證據來源。重建跨批
packed release 用現有 `scripts/assemble_restored_backup.py` 和後續 canonical
verifier，不能只還原任意 `latest` 或其中一個增量片段。PG logical restore 在
隔離控制庫執行，核對 same-MVCC logical identity、schema／rows 與 ledger contract，
不用還原去覆寫線上 penguin。日常新增 DB state 的驗收沿既有 recovery queue；
詳見 `docs/lab203_automatic_backup_2026-10-04.md`。

## 空間清理順序

新 immutable lake 的還原用固定 catalog delivery 及所有必要 raw deliveries；
DuckLake `source_metadata` 保存原 manifest bytes，不能以現行 heads 取代。
`scripts/verify_lakehouse_packed_recovery.py` 在獨立私有 PG／cold scratch，
核對原 manifest、inventory、CAS、canonical fetch 和 source fingerprint；它
把實際 NAS file-restore ACK 與 hash-identical copies 的獨立重建串起來，
不宣稱 penguin 直接讀 NAS。`scripts/verify_lakehouse_recovery.py` 是單一
catalog/data 副本的 native restore；直接 NAS 原生驗收以 paired lab203 ACK 為準。

`stockagent-lake-transport-gc.timer` 只處理 ledger 已接受的 exact wave，沿用
`immutable_transport_cache.retire` 的 dry-run/apply 和 per-delivery lock。primary
D/NAS 永不清除。短寫 partial 只在固定持久 copy intent、原始 full SHA 和無
process 引用時重複複製；未知／改變／redirected 成員保留，不能廣泛掃除 `.staging`。

1. 盤點實際 roots／檔數／logical 與 allocated bytes、hardlink 別名、來源與
   正式／獨立訓練程序。有掃描上限／permission error 的根標為不完整；不能相加
   跨根共享的 `st_blocks` 當可回收量。Supervisor 沒顯示 job 仍要核對獨立程序。
2. 分類權威 source、service／model／ledger、當期訓練 input/cache、完成 output、
   已註冊 legacy cohort、backup transport、己方 restore scratch、未知／失敗資料。
   查 `artifact_consumers`、config、PID/cwd、FD／mmap、source signatures，不能按名字分類後刪除。
3. 從對應 owner 取得 **fresh dry run**。managed materialization 的起點：

   ```bash
   source scripts/runtime_env.sh
   stockagent-data gc --dry-run
   stockagent-data evict DATASET --snapshot-id EXACT_RELEASE_ID --dry-run
   ```

   `evict` 是特定版本的立即候選審核，不免除 pins／active use／cold proof。
   dry run 無候選便保留原因，不能以 `rm`、清 pin、backdate lease 或降低 timer
   retention 繞過。已授權的 apply 繼續走同一 canonical 入口及當前核對，不另建清理器。
4. exact D／NAS recovery、來源前後全 SHA／metadata、當前 consumer／pin／lease、
   當前本機及適用 peer convergence、mount／空間、明確 scope／policy 全部通過後，
   才由同一 owner 做 journal／私人 quarantine，再檢查並 unlink 精確名單。
5. 核對逐項 apply 結果、拒絕／保留項、allocated bytes、cold objects 未受影響與
   需要的掃描收斂。以實際最後 link 釋放的 blocks 報告回收量，不用壓縮前大小代替。

penguin 的 `/var/lib/stockagent-legacy-return/recovery-holds.json` 保護 root 與所有
descendants；缺失／無效 fail closed。`data_tw_index_futures/preparation_sources/`
及其相容來源介面保存原值，不能當一般衍生資料刪除。授權／`publish: false`、
正式 execution ledger、部署模型、獨特 trial results 和失敗 quarantine 同樣保留。

NAS snapshots／repository prune、D object GC、source hot retirement、backup transport
回收和訓練 cache GC 是不同 scope；一種 ACK 不能授權另一種刪除。已同步資料也可能
是唯一副本，正常的源端刪除會傳播到 lab203 Receive Only，必須依原 ACKed policy。

## Legacy 與共用鎖

歷史保存沿 `configs/data_sync/vastai_legacy_archive_return.json` 或
`vastai_bulk_preservation.json` 的**既存已註冊** cohort、policy、source fingerprints。
一次性 tar/zstd 保存不是正常增量規則，也不是 completed-model 證明；不自動加入
新 cache／generation，不將過去的 immediate／capture 授權擴張到未知 roots。

共同發布／退休 owner 是 `/run/lock/stockagent-remote-cold-artifact-ingress.lock`。
私有傳輸／編碼／獨立 immutable D recovery 可在各自 cohort owner 下有界進行；
真正 commit 與再次 fresh-proof retirement 仍重取共同鎖。不要再建立總控 writer。

鎖等待時查 task/run 收據、PID／start ticks、owner、children、journal 階段和進度。
inode lock file 在並不等於鎖仍被持有；刪 lock file 會讓兩個 owner 鎖不同 inode。
不刪鎖、不任意 SIGKILL、不縮短恢復檔案集合。升級既有 worker 只用
`scripts/handoff_remote_legacy_archive_worker.py` 的已支援交接，限已核對的
childless、未持鎖等待點；當前發布、SSH 或恢復交易完成前保留原 worker。

bulk ACK v2 僅在 `unlink_preserved_names_only` 身分／plan 與全部 gate 通過時，
unlink 已完整保存的精確名稱，保留外部 hardlink；v1 對未知 shared names 保持拒絕。
current inode 的外部 FD／mmap、quarantine 後 metadata 與每次 nlink 減一仍需核對。
原 archive 保留 mode／mtime／空目錄；publication 的 unverified receipt 不能拿來刪除。

## 故障接手

- **75／busy／資料未齊：** 記錄 deferred 原因，讓同一 timer 後續重試；不是備份成功。
- **NAS unavailable／readiness 過期：** 保留來源與 sealed delivery，修既有 mount／
  owner，恢復後再驗 fixed snapshot；拒絕本機空目錄充當 NAS。
- **SHA／READY／集合／Git 解析／runtime 不符：** 保留失敗證據，修精確根因；不發 ACK、
  不接受 conflict head、不以另一邊 mtime 覆蓋原值。
- **unknown／missing current object：** 沿原 recovery request、fixed cold proof 或來源
  修復處理，不能繼承已明確放棄的歷史清單為所有 current／future 損壞的免驗條款。
- **中斷／無 exit receipt：** 核對原 journal、checkpoint、來源／owner，按
  `docs/agent_workflow.md` 的 reconcile 接手；未核對前不啟第二份 run。

成功更新本次任務證據，保留 failed／deferred 原結果。回報最新時間與分母／scope；
每批 verified 不代表全歷史已驗收，現有 filesystem 沿用也不表示已格式遷移。

細部 runbooks：`docs/packed_dataset_storage.md`、`docs/automatic_cold_storage.md`、
`docs/vast_all_artifacts_cold_return_2026-10-04.md`、
`docs/vast_bulk_compressed_return_2026-10-04.md`、
`docs/four_node_storage_optimization_2026-10-05.md`。
