# lab203 全量冷資料備份與歷史缺件處置（2026-10-04）

使用者本輪指示：「找不到就算了，直接丟棄就好，請你幫我全部分析整理同步到lab203」。
現存冷庫檔案、公開程式／設定／文件與一致性控制庫備份進入既有備份通道。
缺件的非目前歷史版本結束恢復工作。傳輸、NAS 檔案還原、packed 重建、
資料庫 logical restore 與金鑰異機保管仍各自驗收。

## 第一性原理與盤點

原始資料是恢復的輸入；實際 NAS 還原是離機恢復證據；Syncthing 只是傳輸。
因此保留 penguin 的原始冷庫，lab203 專職接收與 NAS 加密備份，以固定 snapshot
的完整還原及逐檔 SHA-256 收據推進下一批。租用訓練機不作長期權威。

| 本輪盤點 | 結果與範圍 |
| --- | --- |
| 觀察到的 manifest | 267；目前 head 158，物件集合無缺件 |
| 有效保留 release | 242；另外 25 個非目前歷史版本明確放棄恢復 |
| 原始缺件 | 114 個物件、1,053,144,416 bytes；已不存在，處置沒有釋放磁碟容量 |
| 完整可用備份集合 | 17,544 檔、684,032,385,699 bytes；每檔在封閉批次與 NAS 還原時核對完整 SHA |
| 無 manifest 引用的冷物件 | 81 檔、12,316,370,426 bytes；同樣備份，但不賦予 release 歸屬或刪除資格 |
| mutable／歷史 head | 158 個目前 head 加 49 個 immutable head-history 檔；封閉批次保留真正觀察到的 head bytes |
| lab203 可用 ingress | readiness 實測 555,002,097,664 bytes；至少保留 64 GiB |
| NAS 可用空間 | readiness 實測 15,482,468,544,512 bytes；不是使用者配額的獨立管理端證據 |

固定盤點識別為 `50c69ed75eccba2bdba145e3cdc52336f4e0b41de036615623f92730bc4c1012`。
來源仍可能持續發佈；每輪另保存 `catalog_observed_at_utc` 與 catalog identity。
完整原始冷庫 fast inventory 僅證明 metadata／presence，其引用檔尺寸與 SHA
不是在 fast inventory 中驗證；真正 bytes gate 在每批來源 export／讀回與 NAS restore。

14 個 `publish:false` catalog 工作區、未發佈的 downloader 工作檔、私密憑證、
API key、Syncthing 身分、Git object database 與整台 Windows 映像不在本流程。
限制 manifest 的物件不會被改標為「無引用物件」繞過限制。公開 code ZIP 包含
Git-visible 未提交／未追蹤檔與 tracked deletion 清單。

## 缺件歷史的明確終止

[`backup_history_disposition_20261004.json`](../configs/data_sync/backup_history_disposition_20261004.json)
固定每個放棄版本的 manifest SHA、source fingerprint、缺件 SHA 集合與既有恢復查找證據。
處置 identity：`d749cf176b6c749793f4b1385ed2c708eab4f3248edbcedd7405d03d533870ab`。
每輪重新確認這些版本仍非目前 head、manifest 未變、缺件範圍吻合。
新的目前版本缺件不能套用本次放棄。

沒有刪除現存冷物件、manifest、head、pin、quarantine 或歷史 config。
舊 materialization quarantine 與指向舊版本的歷史 config 保留診斷用途；
若今後有人選用已放棄的舊 release，其資料不足仍應拒絕執行。
「放棄恢復」不等於全歷史備份成功。

## 容量決定使用有界傳輸暫存

684 GB 大於 lab203 的 555 GB 可用 ingress；永久保留每批傳輸副本會使全量流程停住。
來源啟用的自動回收範圍精確限定為
`machine_acknowledged_reconstructible_cold_transport`：

1. 每批已有配對 lab203 的不可變收據，固定 repository／完整 NAS snapshot，實際還原、來源驗證器與全部檔案 SHA 都通過。
2. 原始冷庫檔案仍匹配來源 full-SHA 驗證時的 inode／size／mtime／ctime；若 publisher 已更新 head，保留先前真正觀察到的 bytes。
3. 傳輸批次吻合固定 envelope／READY／精確集合與 full-SHA 的檔案身分，無 hard link、symlink、未知檔或程序引用；程序檢查涵蓋該批次的物理／bind mount 別名。
4. 來源 folder 為原配對 Send Only，idle、無錯誤；lab203 completion 100%、remoteState valid、need bytes/items/deletes 全為 0。
5. 保存 dry-run 與 owner journal，原子移到忽略的 `.staging`，只移除清冊中的副本；中斷後可沿用固定意圖接手。
6. 明確執行 Syncthing 完整 scan 索引刪除；NAS recovery index 與所有驗收／覆蓋證據保留。

這會讓來源擁有的刪除同步到 lab203 ingress。lab203 自身 runner 的批次刪除仍關閉，
NAS prune 仍關閉。code／SQL、使用者轉交收據的兩個 pilot、失敗／未 READY staging
與原始冷庫均不在回收範圍。若接收端有版本保留或磁碟未釋放，fresh readiness
的剩餘空間與 reserve gate 仍會停止繼續發送，不能以虛擬磁碟上限替代容量。

第一次現場回收已通過：98 檔、1,055,120 bytes 傳輸副本移除；原始檔 SHA 全部不變，
固定 NAS 收據與覆蓋不變，recovery index 保留 NAS snapshot；lab203 completion
恢復 100%，needDeletes 0。此次 source／NAS 原始物件刪除數均為 0。

## 本機硬體實測後保留 8 KiB

同一 13 個真實成員、68,015,229 bytes 交錯測量，包含完整 export、fsync、
envelope／READY、每檔 SHA、精確集合與第二次讀回驗證。沒有清除 OS cache。

| D 9p 請求設定 | 候選完整流程秒數 | 同輪 8 KiB 秒數 | 較大真實批次 | 正式採用 |
| --- | --- | --- | --- | --- |
| 實際協商 262,144 bytes | 26.919／25.367 | 64.882／72.297 | 約 0.5 GB 多批讀回 ENOMEM，kernel order 6 allocation failure | 否 |
| 65,536 bytes | 12.183／6.942 | 17.681／21.504 | 約 409 MB TAIFEX 讀回 ENOMEM，kernel order 4 allocation failure | 否 |
| 8,192 bytes | 見各輪基線，不能跨輪解讀為可靠暖機速度差 | 同一設定 | 四個既有封閉批次全部完整讀回通過 | 是 |

小樣本的速度優勢未能通過較大批次可靠性驗收。正式 D mount／transport 已回到
原始 8 KiB，失敗候選不會由安裝器重新啟用。保留原始失敗日誌與每輪證據。
正式每批至多 8 GiB／512 檔、最多一批未有 NAS ACK，來源單輪 timeout 延長至 8 小時；
這是容量與可靠性設定，不是已證實全量吞吐的宣稱。

## 收據與啟用的工作流

Repository：`c717a3d315e20a5a3314c595e66607fcb7d6646cb73ad4412cc2cccb22abb798`。

| 機器回傳批次 | 完整檔案／bytes | NAS 完整流程秒數 | 固定 snapshot |
| --- | --- | --- | --- |
| code／control 1 | 7／12,347,275 | 10.029 | `5b86d6c40664fa6cc440c435d8d6a639c2ebc240d805f70dd851f258fd52c1ce` |
| code／control 2 | 7／12,379,256 | 5.255 | `c33db9a94471425b361970a2d07cbdb3401171f4548bd7bf791f049f8b81755b` |
| 第一波增量冷檔 | 98／1,055,120 | 4.882 | `acdb936d919dfd2f8dcc09f2579892ea914603681f40976b7617aff432407933` |
| 本輪公開工作樹／control | 7／12,423,437 | 5.287 | `ed718fe13451b971feecb18c9491210a3e74530ecd989879242be977feaaeafd` |
| 正式 512 成員冷檔批次 | 514／37,628,724 | 6.195 | `4173001e579c72cb1b82943f352e4ecaeadfa77629da59863eaa5485bbd6a6ea` |
| 下一個正式冷檔批次 | 514／101,328,197 | 9.702 | `210a3d5cd0a1e2db9c7f31c7f60f702f770b84b6a007b78a2ca6b507fa19b9ce` |

以上均是固定 NAS snapshot 的檔案還原、來源 verifier、完整 SHA 及命令退出碼 0 的
機器收據。第一波是部分 metadata，不能稱為完整 release 重建。
兩個先前 pilot 收據仍標為使用者轉交，不升格為此次直接取得的機器收據。

來源：`stockagent-backup-stream.service`／五分鐘 timer、原 source owner／ledger。
接收：沿用 `lab203-backup.service`／五分鐘 timer、現有 worker／NAS mount guard。
回傳：`stockagent-backup-receipts-lab203`，lab203 Send Only、penguin Receive Only。
最新全自動要求改用[資料任務／既有 owner post-hook](lab203_automatic_backup_2026-10-04.md)，
初始 packed／SQL 與後續新 DB 邏輯狀態自動驗收，不再逐批轉貼。
依固定 dispatch、READY／完整集合、NAS restore ACK 推進，每輪更新
`tools/source-status.json` 與 `tools/recovery-index.json`；已回收的 batch 標示
`batch_retired_from_transport:true`，恢復應使用其固定 NAS snapshot。

完整證據在 `artifacts/operations/two-node-lab-nas-backup-20261003/`：
`all-available-cold-catalog-20261004.json`、`full-physical-cold-inventory-20261004.json`、
`cold-transport-cache-dry-run-20261004.json`、`cold-transport-cache-live-acceptance-20261004.json`、
兩份 `backup-drvfs-*-complete-comparison-20261004.json`、原始 workflow logs 與 pytest XML。

最新分析、公開 source config／歷史處置與恢復工具以封閉交接包
`tools/continuous-backup-20261004-v4/` 同步至 lab203；v4 補齊正式 514 檔 NAS 驗收及
物理別名引用檢查結果，v3 舊封閉包保留。完整工作樹另以每小時 code ZIP
進入正式 NAS 批次。交接檔不得自動替換已在跑的 worker，來源 config 路徑也不是
lab203 私有部署設定。原已驗收 v1／v2 保留其固定 bytes。

最終相關備份回歸 134 項通過、0 skipped，使用實際 Restic binary；包含物理／bind
別名程序引用及等待原 owner 的封閉交接檢查。證據為
`all-backup-focused-tests-accepted-final-v2-20261004.xml`。
沒有啟動 GPU 訓練或修改盤中執行程序。上面 NAS 時間只計接收端完整備份／還原驗收；
本輪 code／control 的來源 cycle 計時為 229.010 秒，systemd service 總牆鐘
231.204 秒，沒有把它省略為 NAS 耗時。

## 剩餘驗收

全量 684 GB 尚需逐批到達並取得 NAS ACK；不能把排程啟用或 Syncthing idle
當成已完成。packed canonical 重建與 NAS PostgreSQL logical restore 需由 lab203
本機沿用已交付的 verifier 與固定還原 snapshot 實際執行；一般檔案 ACK 沒有宣稱這兩項。
USB 金鑰保管已於 2026-10-04 由使用者明確確認完成，不再詢問或安排媒體操作。
Windows 登出驗收依使用者決定取消。packed 與 SQL 驗收已有來源固定的三項工作，
沿用 lab203 同一 owner，詳見[獨立 NAS 還原驗收](lab203_nas_recovery_acceptance_2026-10-04.md)
及[持續備份操作](continuous_nas_backup_2026-10-04.md)。
