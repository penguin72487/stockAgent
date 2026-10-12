# NAS 冷庫同步完成驗收（2026-10-08）

**15:15:43（Asia/Taipei），本次固定清冊的 36,096 檔、
1,096,604,738,709 bytes 全部取得 NAS 機器驗收；待驗收 0 檔／0 bytes。
同一輪核對正向 ingress 與反向 receipts 都已收斂。**

使用既有 Temporal、Syncthing、lab203 relay、Restic 與備份排程接續處理。
未改 runtime 程式或容量政策，也未另開 publisher、NAS writer 或停止資料收集。
昨日來源修正與原缺口驗收見
[來源容量與 NAS 補齊修正](nas_source_backpressure_repair_2026-10-07.md)。

## 固定驗收範圍

| 項目 | 證據 |
| --- | --- |
| 清冊觀測時間 | 2026-10-08 15:08:36 |
| 清冊身分 | `36be643a6d6b35f5830d96655c4e57e1eb9cb13aa6faefb795f3c63ad768cd56` |
| 清冊檔案 SHA-256 | `7c68bff55e62f72d0902419085958ea159c1473fc9526614c1d211e0de7983e7` |
| NAS 機器驗收 | 36,096／36,096 檔；1,096,604,738,709／1,096,604,738,709 bytes |
| 使用者轉述而未有機器證明的檔案 | 0 |
| 正向／反向 Syncthing | 配對 connected；兩個 folder 均 idle、need bytes／items 0、無錯誤，`converged=true` |
| 正式驗收命令 | 原 `audit_nas_sync.py`，含 `--require-transport-convergence`，退出 0 |

固定清冊按相對路徑與 SHA 去重 Restic／immutable archive 收據，沒有直接相加
兩條管線的容量。原 9 檔／312,697 bytes 在同一備份 owner 的正常接續中取得
NAS 驗收；沒有改寫原檔、偽造 ACK 或以 Syncthing 送達替代還原驗收。

## 來源積壓與持續同步

15:19:47 的正式核對確認來源為 `monitoring_increments`：當時 immutable source
待發布物件 0、待驗收批次 0、待驗收 bytes 0；現行 source 物件容量
1,096,586,163,707 bytes 全部有 NAS archive 覆蓋。早上大量轉存的積壓已清空。
lab203 relay 為 `ready`；來源 receipt／catalog 錯誤、備份 readiness／auxiliary
錯誤與 scan／export 錯誤均為空。

以下既有 owner 均為 `active / enabled`：備份 timer、transport GC timer、
Syncthing、storage lifecycle、Temporal、lifecycle control backup timer 及
control backup timer。保留兩條 RUNNING durable workflows，後續版本仍由原
publication／NAS 驗收／回收程序接手。當前指定 recovery queue 的 15 項請求
均已驗收；這不擴張為每個未來版本或每個 dataset 的語義重建證明。

驗收後來源仍會增加新檔案及 head 世代。15:19:47 的更新清冊另有
**5 檔／243,935 bytes**待 NAS 收據；逐檔核對均在本次固定清冊之外，原固定
清冊重新對帳仍為 0。這些後續增量沿同一正式排程處理，沒有把它們算成原
積壓尚未完成，也沒有宣稱服務會永久 idle。

固定清冊只包含已保留並入庫的 canonical 檔案；不包含未發布工作區、已放棄
且不存在的歷史。`all_history_backup_verified=false` 保留。NAS 檔案驗收、
指定 packed／PG 邏輯還原和未來業務恢復仍各有證據範圍。
本次未刪除 authority 來源或 NAS archive，未啟用 prune。

## 可重查的收據

- [正式完成收據](../artifacts/operations/nas-sync-completion-20261008/completion-acceptance.json)
- [NAS 檔案驗收及雙向傳輸收斂](../artifacts/operations/nas-sync-completion-20261008/nas-completion-observer.json)
- [固定清冊](../artifacts/operations/nas-sync-completion-20261008/fixed-catalog.json)
- [來源 queue 核對](../artifacts/operations/nas-sync-completion-20261008/source-status-observed.json)
- [lab203 relay 核對](../artifacts/operations/nas-sync-completion-20261008/relay-status-observed.json)
- [正式 owner 狀態](../artifacts/operations/nas-sync-completion-20261008/services-accepted.json)
- [後續增量範圍](../artifacts/operations/nas-sync-completion-20261008/current-coverage-observed.json)

日常查詢沿用原入口；重新驗收新清冊時使用新的輸出路徑，不覆寫本次證據：

```bash
bash scripts/run_lakehouse_control.sh status
bash scripts/run_data_cache.sh automation-status --human --live
stockagent-agent run-status nas-current-cohort-completion-20261008T071506-5eb8d58c
```
