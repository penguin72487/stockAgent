# 全資料集增量版本架構

最新固定觀測：**2026-10-19 00:01（台北）41／60 項**已完成固定 D 版本的全原檔還原。
原 owner 已驗收期交所完整根目錄、續跑期貨；OpenBB 中斷的原件工作保留 sealed packs 待續跑，
都尚未計入完成。這輪修正與測量見末節「控制檔漏判、精確位元打包與實際工作時鐘」。
即時進度仍讀原狀態工具，
不能從這段固定觀測推定後續服務已載入或完成。

## 核心：版本是視圖，不是另一份全庫

本契約適用 `configs/columnar_storage.json` → `source_versioning.roots`
登錄的所有 `data_*`。目前有 33 個，包含台股、期貨、FinLab、FinMind、TEJ、
新聞、加密貨幣、外匯、OpenBB、總體與舊版相容資料。新增資料目錄而未登錄，
清冊檢查必須失敗，不能默默漏掉。

每個版本保有**完整邏輯清單**；實際檔案與 SHA-256 區塊只保存一份。
新版本沿用未變內容，只加入新內容。刪除／更正是新清單的變化，不改寫舊清單或原件。
獨立還原任何固定版本不需要從第一版逐版重播，亦不能把所有版本 UNION 當成全量資料。

2026-10-16 第一階段修改共同版本底層與全根目錄的政策／責任接線，當時沒有搬走來源。
2026-10-17 已開始正式遷移與精確副本回收；實際結果見下方「正式遷移紀錄」。
既有 `data_*` 工作區、來源收據、授權、欄位與讀取入口不變。
FinLab 不同內容的整表歷史仍保留，並未全部轉換為分區資料。

## 三種責任分開

| 層 | 保存什麼 | 誰負責 |
| --- | --- | --- |
| 來源工作區 `data_*/` | 原回應、原值／單位、收據、可續傳狀態及必要相容視圖 | 原下載器與 provider 排程 |
| 不可變版本 | 完整路徑清單、共用檔案／區塊、版本與來源身分 | 既有 packed publisher；新聞訓練表沿用 news builder |
| 使用中的訓練／服務視圖 | 固定版本的必要檔案或可重建 panel | 原讀取器、materialization、lease 與訓練 lifecycle |

不新增下載器、全庫鏡像、共享 CAS 或背景 timer。`publish: false` 不會因支援版本化而
變成 `true`；FinLab／FinMind／TEJ／新聞沒有藉此新增同步或匯出權限。
禁止共享的來源使用同一 D authority 下、原同步明確忽略的
`.local-state/private-history/`；必須是 owner 專屬 0700 目錄、有政策身分 marker，
不能由共享 head、Syncthing 或 NAS export 暗中帶出去。
SQLite/WAL 以 SQLite backup API 產生一致狀態，再綁定固定冷版本；
不能拿活躍 DB 的隨意 main/WAL/SHM 檔案副本當成一致來源。

## 本次遷移計畫與完成判準

使用者 2026-10-17 再次確認：全部資料集與冷備份都要實際套用，不只登錄政策。
以下順序沿用原 writer／publisher／備份責任；「已設定」不計入實際搬完。

1. **凍結分工與清冊。** 33 個來源根目錄、50 個具體 catalog entry，加上 10 個根目錄
   私有補集，共 60 個處理項；保留每個原生來源、日期／provider 粒度、舊版本與 consumer。
   其中 32 個共享、28 個 owner-private。兩者與子目錄有覆蓋重疊，不能加總 bytes 當全域分母。
   `publish: false` 的本機資料不能混入共享發布批次。
2. **先升級讀取器。** v1／v2 都可讀，再啟用原 publisher 的區塊版本；不修改舊的固定 pin。
3. **正式搬入既有冷庫。** 第一版需要基準內容；之後只新增尚未存在的 immutable object。
   完全相同檔案直接共用，大檔變動只新增獨有區塊。來源原件不是另一份可隨意清空的 cache。
4. **各原生格式分別處理。** 日／月分區及不可變查詢原件保留自然粒度；新聞採列／part
   COW；FinLab 精確重複檔共用 inode，不把不同修訂的整表誤報成已完成分區遷移。
   共同 chunk 層可減少後續冷版本的重複 bytes，但不等於原生寬表已改成逐期分區。
5. **獨立驗每個固定版本。** 核對 manifest、完整 inventory、每個 object、ZIP CRC、
   每個原檔的完整 SHA／長度；不是只驗 head 或部分檔案。可用串流重建，無須永久再落一份全庫。
6. **接續原 NAS 備份與回收。** 新版本由原 Temporal／relay 傳送相同 CAS；核對固定批次 ACK。
   已驗收的冗餘 transport 交原 GC 回收。NAS 原有 CAS／Restic 本來就按內容共用；
   不為了換格式再另建全庫 tar，也不把仍被舊版本引用的物件或 Restic 快照直接刪除。

版本 `V` 是完整的「邏輯名稱 → 不可變內容」清單，不是全量實體副本；新版本的永久
payload 成本是當次引用內容中、現有 CAS 尚未保存的集合。metadata、必要修訂與暫存工作空間另計。
保留所有舊版本，與刪除仍被它們引用的唯一內容，在邏輯上不能同時成立。
可以回收的是已證明冗餘／可精確恢復的實體副本，而不是靠年代判定「舊版本就是垃圾」。

逐根狀態至少區分：政策登錄、原生 writer 相容、正式 head 提交、固定版本逐檔還原、
NAS 接受、可刪副本／實際回收。任一步仍在等待，就保留該步原因，不能把 33 根的清冊通過
冒充 33 根已遷移完畢，也不能把 local-only 來源標成已送到冷備份。

正式共用冷庫的實體結構沿用現有責任：

```text
/srv/stockagent-packed/
  objects/
    blobs/<sha-prefix>/<sha256>.blob  # 整檔或精確 byte chunk，內容相同即共用
    packs/<sha-prefix>/<sha256>.zip   # 小檔的 path bucket pack
    inventories/<sha-prefix>/<sha256>.jsonl.gz
  manifests/<dataset>/<snapshot-id>.json  # 此版完整 inventory 與全部必要 objects
  heads/<dataset>/<node>.json            # 最後原子切換，不指向半成品
  .local-state/                         # 鎖、staging、節點狀態，不傳輸
    private-history/                   # owner-only 0700；相同 v2 格式，不共享／NAS 匯出
```

路徑以實際 manifest 為準。`source_versioning` 不是第二份發布名單；發布子目錄、
來源鎖、freshness／validation gates、禁止發布範圍仍以 `configs/data_sync/packed_datasets.json`
為準。清冊會列出一個根目錄對應的多個來源分支，不能把它們誤當成整個根目錄已備份。

## v2 改了什麼

原 packed v1 已共用未變檔案，但改變的大檔仍整檔另存。v2 讓改變的大檔
使用預設 **8 MiB 精確 byte 區塊**，直接放入相同 blob CAS；完整檔案 SHA、大小、mode
仍保留在 inventory。不是將來源 JSON／Parquet 改值，也不把不同 provider 的列混在一起。

- 相同格式的未變版本：回傳原 release，不建立新的 payload 或 timestamp-only head。
- 首次明確由 v1 升級 v2，即使資料未變也只新增一次格式清單；共用原 inventory／payload，讓後續原排程繼承新格式。
- 追加或更正大檔：只安裝尚未存在的區塊；完整 inventory 指明順序及長度。
- 相同區塊重複出現：只存一份，邏輯長度按出現次數計算；每個 provider 的路徑與 provenance 獨立。
- 刪檔：下一版不引用該路徑；舊版引用的物件仍保留，不是 GC 指令。
- 舊檔遷移：未變的 v1 blob／pack 繼續共用，不強制全庫重寫；只有改變的檔案採新格式。
- 一次啟用 v2 後，後續原排程會繼承區塊大小；不必每次重新加旗標。
- 只剩小檔或空版本時仍保留該政策；不會在下一次新增大檔時退回整檔另存。
- 還原：共用完整、子樹與有界根檔 staging 的 extraction engine；每個區塊及重建完整檔案皆核對 SHA。
- 驗證／復原失敗不會被宣稱成 READY；source 在擷取中改變則中止，不提交新 head。

這是**精確 bytes 去重**，不是宣稱「理論最小壓縮」。壓縮後的 Parquet 改寫或插入
可能使後續多個 byte 區塊改變。長期仍應按來源自然粒度關閉分區、copy-on-write 受影響分區，
不要每次重寫整張寬表。每個根目錄的 `target_grain` 是遷移設計，不是已完成的證明。

新聞訓練表有更細的列／part copy-on-write，沿用
[新聞增量實作](news_incremental_storage_2026-10-16.md) 與 `news_table_paths`。
兩種 manifest 不是同一 ABI；共同的是「完整視圖、共用未變 payload」原則。

## 查看所有資料集的實際邊界

在專案根目錄執行，只讀設定與選定 head metadata，不遞迴讀來源資料，不呼叫 provider API：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/publish_data_releases.py layout
run_fintech_python scripts/publish_data_releases.py layout data_finlab
bash scripts/run_data_release.sh layout --sync-root /srv/stockagent-packed \
  --layout-output artifacts/data_quality/all_dataset_incremental_layout_20261016/inventory.json
```

清冊逐一包含來源／consumer 入口、原生格式、目標粒度、publication 綁定、排除子目錄、
觀測到的 head ABI 以及 rollout 狀態。`objects_verified: false` 表示這只是 metadata 清點，
不是本次已完整下載、冷庫復原或訓練驗收。`native_workspace_migrated: false` 明確不宣稱原地遷移。

2026-10-16 初次讀回：33 根目錄、39 個當時選取的發布分支綁定；11 個根目錄觀測到既有 head，
沒有觀測到正式 v2 head。新聞當前仍是既有 ABI 1 基準版。這些是當次快照，不是永久進度。

最新持久處理清單、逐來源的寫入／冷驗證階段，使用原日誌的輕量投影：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/dataset_migration_status.py
```

這個查詢不掃原始 payload、不呼叫 provider、不觸發發布。
`verified_sources` 只計入固定版本完整還原通過的項目，`publication_progress` 的 100%
不能取代 `recovery_progress`；未驗收時 `acceptance` 必須保持 false。
私有／共享補集重疊，所以 `global_byte_percent` 明確為 null，不製造錯誤百分比。

## 啟用與讀取

新 reader 支援 v1、v2，但舊 reader 不認識 v2。先部署 reader 再改共用 head：

```bash
# 僅在所有預期 consumer 已支援 v2 後，對原本可發布的來源啟用。
# DATASET 是發布清冊中的 ID，不是任意新路徑；原有鎖、來源稽核和授權檢查全部保留。
bash scripts/run_data_release.sh publish DATASET --sync-root /srv/stockagent-packed \
  --reader-contract packed-v2-chunks
```

全來源部署使用 `publish --all-ready --source-roots-only`；後者明確限縮到登錄的來源根目錄，
不發布整個 artifact cache、completed-forward family 或未登錄來源。
`--reader-contract` 是明確 rollout 宣告，不是假裝已量測遠端 reader。
未提供旗標且尚未進入 v2 的來源維持既有 v1 格式；不會偷偷發出舊 consumer 讀不懂的 head。
已有 v2 head 的來源會持續 v2，`packed-v1` 不是回滾／降級既有版本的指令。

長批次應保留逐項收據並把網路掃描交回既有 drain，不要讓單一同步請求拖住來源鎖：

```bash
bash scripts/run_data_release.sh publish --all-ready --source-roots-only \
  --sync-root /srv/stockagent-packed --reader-contract packed-v2-chunks \
  --defer-scan --batch-progress-receipt artifacts/operations/NEW_BATCH_PROGRESS.json
```

收據路徑必須全新且位於冷庫／選定來源之外。批次在每個來源前後原子保存進度；
中斷時的 `active_outcome: unknown_check_current_head` 要核對真實 head，不能推定未發布後盲目重送。
忙碌的來源鎖立即記為 `retryable`，繼續其他來源；單來源／借用原 owner FD 的流程仍保留原本的有界等待與身分驗證。
`finished` 只表示本次嘗試結束，不表示所有來源成功、peer 收斂或可以刪檔。
已持久化的 `scan-pending` 由 `stockagent-d-cold-scan-retry.timer` 接續；不新增另一個同步服務。

若舊批次沒有逐項收據，先核對已提交的固定 head、持久化掃描待辦及原程序是否仍在寫入。
只有在原交易已結束後，才可用 `--after-dataset EXACT_DATASET` 從清冊中的該項之後續跑，
並指定全新 `--batch-progress-receipt`。游標不在本次範圍內就拒絕執行；不自動跳過
尚未驗收的來源，也不將游標之前的來源算作新收據的成功結果。來源仍忙碌或未通過 gate
的項目需另行讀回、由原排程接續，不能因清冊游標略過便宣稱完成。

對固定實際 release 做完整串流還原驗證：

```bash
# 正式 D 操作在其 systemd owner 的 mount namespace 執行；agent run 可記錄長命令。
bash scripts/run_packed_snapshot.sh verify DATASET --snapshot-id EXACT_RELEASE \
  --sync-root /srv/stockagent-packed --reconstruct-all-files --profile \
  --result-receipt artifacts/operations/NEW_RECOVERY_RESULT.json \
  --progress-receipt artifacts/operations/NEW_RECOVERY_PROGRESS.json
```

每個原檔均從固定冷物件獨立串流出完整 SHA；不建立另一份永久 materialization。
結果收據只在全部通過後產生，progress 永遠不是 acceptance proof；D 還原也不等於 NAS 的固定批次已驗收。

來源亦可在其既有 catalog entry 明列 `file_chunk_bytes` 與 `reader_contract: packed-v2-chunks`；
設定缺 reader 宣告或 chunk 超過 64 MiB 時拒絕載入。2026-10-17 在遠端讀取測試與本機
worker 身分讀回後，50 個具體來源 entry 已明列啟用，另外 10 個完整根補集由同一政策產生；
原 `publish: false` 與共享來源 gates 均未更改。

低階實作仍是 `publish_packed_snapshot(..., file_chunk_bytes=8*1024*1024)`，以及
`scripts/packed_snapshot.py publish --file-chunk-mib 8`。這不是繞過 canonical D mount、
來源鎖／驗證或權限的捷徑；正式管線使用上方 wrapper。

訓練固定既有 source release／pin，由 `stockagent-data use` 還原所需資料，
再從 manifest 選定來源建表。請勿直接用 blob 當成 Parquet，或讀 `versions/**/*.parquet`。
現有 READY、lease、pin、同步驗收與刪除門檻仍按 [儲存契約](agents/storage.md)。

## 驗證與剩餘工作

工程驗證入口：

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q test/test_packed_chunk_versions.py test/test_dataset_version_layout.py
run_fintech_python scripts/verify_incremental_dataset_versions.py \
  --output artifacts/data_quality/all_dataset_incremental_layout_20261016/roundtrip.json
```

後者使用隔離暫存 fixture，逐一走 33 個根目錄的共同 adapter，驗證未變、追加、更正、刪除、
固定舊版還原，以及 Arrow／Polars／DuckDB 的原值、負值、NULL、股數與字串代碼。
它不讀寫正式 payload，不呼叫 API，不是 33 個實際來源的遷移收據。

2026-10-16 當次工程結果：

- 核心、reader、native news、materialization、retention／lakehouse 回歸：437 通過、3 跳過。
- edge／backup／Syncthing 傳輸相容回歸：142 通過。
- [隔離還原收據](../artifacts/data_quality/all_dataset_incremental_layout_20261016/roundtrip.json)：
  33 根目錄 adapter、132 次固定版本還原，負值、NULL、股數、代碼前導零與 schema 保留。
- [個別說明查核](../artifacts/data_quality/all_dataset_incremental_layout_20261016/readme_check.json)：
  33/33 README，零來源寫入與 API 呼叫。
- [正式 metadata 清冊](../artifacts/data_quality/all_dataset_incremental_layout_20261016/inventory.json)：
  只讀回根目錄／綁定／head 格式，不是下載完整性或冷庫逐物件驗證。

fixture 故意共用兩個已知的 8 MiB bytes 區塊：未變操作新增 payload 0、更正重用既有
區塊新增 payload 0，追加的獨有 bytes 合計 217。新版本 inventory metadata 另外計量。
這證明物件／完整視圖的會計與還原行為，不能拿其比例估計真實 Parquet 的壓縮收益。
上述 2026-10-16 第一階段 Ruff 與 `git diff --check` 通過；當時沒有執行完整訓練、正式遷移、資料回收或服務重新部署。

後續仍須按各 native owner 升級不同內容的整表歷史；不能將共用檔案政策視為所有原件已分區化。
舊資料回收仍須通過 inventory、process、pin／lease、精確還原與同步 gates。

## 正式遷移紀錄：2026-10-17

這份紀錄區分「政策已啟用」「正式資料已改」「實際已回收」，不把背景程序啟動當成全數完成。
共同任務為 `all-dataset-incremental-layout-20261016`；詳細原始收據在
`artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/`。

| 範圍 | 實際結果 | 仍保留的邊界 |
| --- | --- | --- |
| FinLab 原生歷史 | 原 `.sync.lock` 下，127 組精確相同內容共用，146 個邏輯路徑更換為唯讀共用實體；實際釋放 191,057,920 bytes | 13,380 份原收據內容不變；不同修訂仍保留；14 份舊式名稱收據未碰；不是全部整表已分區 |
| D 冷庫缺物件 | 從原已取得 NAS 配對接受證明的 sealed transport 精確補回 35 個物件，289,449,999 bytes；逐檔 SHA／大小讀回 | 不改舊 manifest、head；不以最新來源冒充歷史內容 |
| D 傳輸快取 | 原 GC owner 獨立重驗後回收兩批，共 4,284,026,499 bytes | 原冷庫物件、NAS archive、原始資料檔均未刪；可由原冷庫及配對 NAS 備份還原 |
| 遠端 Vast reader | 只部署共用 reader／CLI 與對應測試，保留修改前檔案；94 項 v1／v2 還原回歸通過 | 沒有改訓練來源、checkpoint、GPU 工作或其他服務；舊 frozen capsule 的 v1 pin 仍沿用舊版本 |
| 本機 cold registry worker | 重新載入共用 reader，讀回程式指紋一致；原兩條 Temporal workflow 繼續 | 原 intent、staging、source ledger、NAS 等待狀態未清除 |
| 台股公開資料固定新版 | 完整邏輯視圖 20,063,091,276 bytes，199,560 個原檔；本版新增 268,223,222 bytes／50 個物件 | 正式 head 已提交；尚未完成該版所有原檔的獨立串流還原與 NAS 固定版本驗收 |
| 微結構訓練固定新版 | 完整邏輯視圖 2,018,284,711 bytes，31 個原檔；格式升級新增 payload 為 0；31 檔已從 D 冷庫完整重建並核對 | 全量原檔串流驗證 261.124 秒；這是該固定 release 的 D 證據，不是所有歷史或新 manifest 的 NAS 證據 |
| OpenBB compact 固定新版 | 首次基準 8 個原檔，57,031,609 bytes；8 檔從 D 冷庫完整重建通過，13.353 秒 | 新基準新增 57,034,081 bytes（含 inventory）；不是後續每版都會新增同樣大小，也尚未驗收該版 NAS catalog |
| 33 根目錄／60 處理項 | 50 個具體 entry 加上 10 個完整根私有補集，全部採 8 MiB v2 政策，已進入原 Temporal worker 的持久日誌 | 32 個共享／28 個私有；逐項固定版本還原與備份仍需收據，進入排程不等於遷移通過 |
| 新聞 | 原建置器正式全庫 COW refresh 執行中，已固定來源投影並建立舊分區索引 | 尚未提交新 head，不回收舊版本或當作已驗收資料 |

兩種已完成回收合計 **4,475,084,419 bytes**；不是來源語料或唯一歷史觀測的刪除量。
冷庫修復的 289,449,999 bytes 是補回原件，不能算成額外節省，亦不能用 `df` 變化取代回收收據。
2026-10-17 00:59 的原 managed materialization GC fresh dry run 共核對 6 個視圖，
4 個固定 pin、2 個活躍 lease，`would_evict: 0`；因此沒有強制清 pin、改 lease 或移除這些視圖。

微結構固定驗收收據為
[`microstructure-v2-recovery.json`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/microstructure-v2-recovery.json)。
固定 ID 是 `tw-microstructure-train-20261016T163606677166174Z-l0-penguin-7c0909b2a2cfc494`；
收據核對全物件及全部 31 原檔，不永久另落一份全庫。正式發布／還原／排程隔離回歸在
`migration-cursor-safe-handoff-te-20261016T164850-63f60cea` 通過 328 項；
這些與先前測試有重疊，不能相加成獨立案例數。
再加入 FinLab、新聞、README、materialization／lakehouse 的整組回歸：
`migration-final-shared-regressio-20261016T165431-617e3550` 通過 **498 項**，21.48 秒；
此數亦不與前批加總。OpenBB 的固定還原證據為
[`openbb-v2-recovery.json`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/openbb-v2-recovery.json)。

本次一次性後續驗證 run 是 `migration-verify-after-source-pu-20261016T165904-4260ffa7`：
等待原來源發布結束後，再固定當時的實際 v2 heads，逐一呼叫原 packed verifier，
已完整通過且 snapshot／manifest SHA 完全相同的兩份收據不重驗。
它不發布來源、不新增常駐服務、不做刪除；最多等待發布 6 小時，逾時保留資料並回報 deferred，
不能把「後續驗證已排入」寫成已完成還原。新聞仍由原 builder 自己通過全部列／part 驗證後切換。

本次正式執行入口與證據：

```bash
# FinLab 先 audit 取得 plan fingerprint，再 apply；仍使用原 writer 的同步鎖。
source scripts/runtime_env.sh
run_fintech_python scripts/deduplicate_artifacts.py --finlab-history-root /root/stockAgent/data_finlab \
  --finlab-wait-owner-seconds 300 --report-dir artifacts/data_quality/finlab-history-audit

# 只有所有預期 reader 已驗收後，才讓原發布器切換來源版本。
bash scripts/run_data_release.sh publish --all-ready --source-roots-only \
  --sync-root /srv/stockagent-packed --reader-contract packed-v2-chunks
```

正式批次 `source-incremental-publish-20261016T162256-94638dac`、新聞
`news-native-migration-20261016T155149-d59732e7` 的 `run.json`／`run.log` 在
`artifacts/operations/agent-workflow/runs/`。失敗試跑保留，不冒充成功；不能用
`--result-receipt` 搭配 `--all-ready`，該參數維持原本單一來源的限制。

NAS 不另建第二套全量鏡像。後續新 CAS 物件沿既有配對傳輸／ACK 流程進冷備份；
已被舊固定版本引用的物件與 Restic 歷史不會因「全數遷移」就任意刪除。
尚未有全部 native 歷史、全部冷庫物件復原及 NAS 端舊版回收的完整驗收，不能宣稱全數完成。

## 全來源持久遷移與日常更新

原 `stockagent-storage-lifecycle.service` 新增 `stockagent-dataset-versions-v1`
workflow；沒有另一套下載排程、資料庫或常駐服務。原兩條來源／NAS workflow 保留原 chain。
每個 activity 完成一個來源的發布、固定版本逐檔 SHA 還原及自有 scratch 回收，
其他來源鎖忙碌時記錄原因後繼續可處理項；中斷從原 journal 綁定的固定版本恢復，
不拿 moving latest 頂替。完成一輪後每日檢查，等待期間不反覆掃全庫。
已有固定版本待還原者優先，其次是尚未處理的來源，最後才重試失敗項；
重試遵守各項自己的 `retry_at_unix`，不讓同一個大型失敗來源每輪擋住後面的資料。

發布器對既有全域 publication／retention lock 使用共享鎖，同一 dataset 仍使用排他鎖；
不同 dataset 可並行，retention／刪除操作仍取得全域排他鎖。不可刪除或另建鎖檔來解鎖。
共享 CAS object 先安裝採用不可覆寫的原子 hard link；競爭者必須驗證既有檔的完整 SHA，
不得用 `replace` 改掉另一個發布器已安裝的 inode。這不增加來源 writer 或 NAS 傳輸 owner。

全 60 項先持久保存於
`/var/lib/stockagent/lakehouse-control/dataset-versions/operations/<identity>/plan.json`
及 `progress.json`。目前計畫身分是
`c84f1b36c0362439-c4aed2a3da0f4360910f0e76e27aa4c8`；這是本次 operation 目錄名，
不是將來可硬編碼的 reader 入口。`current.json` 才是原 owner 的當前處理索引。

共享發布仍保留原 writer／freshness／validation 邊界。私有原件保留允許
「固定已觀測檔案集合」：開始後新到檔案留到下一版，選中檔案若改寫則拒絕；
bounded mutable control JSON 先凍結，SQLite 使用 native backup。控制檔辨識會遞迴到
深層收據，例如 FinMind 的 `receipts/.../history.json`、FinLab 的
`intraday/active_run.json` 與 TEJ 的 `worker_status.json`；不把所有原始 JSON 都當成可變狀態。
會被同步程序觸碰的 `.sync.lock` 也保留其固定原始 bytes，不因空鎖檔的 mtime 更新重跑全庫。
凍結從同一個 no-follow file descriptor 讀取並驗證完整 JSON，允許來源用原子 rename
發布下一版，但拒絕讀取中原地改寫的檔案。已準備好的 SQLite 與控制檔重試時沿用；
擴充控制檔規則不重做數十 GB 資料庫備份，也不刷新已固定的舊控制值。
私有 cohort 另明列原 `artifact_io` 的 PID／thread 原子寫入 sibling，以及 FinMind
`[complement|sponsor/]parquet/<dataset>/<hash|all>/<partition>/latest.parquet`
這兩種未提交暫存規則；正常更名不再被誤認成原始觀測遺失。只略過這些精確 writer
命名，保留正式 SHA Parquet、收據及其歷史，並在 manifest 記錄規則／略過暫存數；
普通 `latest.parquet`、`notes.tmp` 或未知變動的原始檔仍不得略過。
這種私有保存明列 `fixed_observed_files_not_atomic_workspace`，不是整個工作區的原子
point-in-time 狀態，更不是完整歷史、正式訓練可用或新發布權限。

每份 SQLite backup 先保存固定 staging intent；中斷重用同一目的檔，完成的 SHA 證據可重用，
不每次重試另留一份大型 DB。只有該 operation 自己建立的 control／SQLite 暫存，
在所有對應冷版本完整還原、無 process reference 且 SHA 再核對後才刪除。
原始來源、固定版本唯一內容、pin／lease、NAS 或 Restic 快照不在這個 scratch 回收範圍。

服務原本已開機啟動，本次保留。三個 activity slot 讓長基準遷移不阻塞既有兩條控制鏈；
`MemoryHigh=6G`／`MemoryMax=8G` 是上限而非預留，CPU 原有 150%／Nice 10。
worker 主執行緒先初始化 DuckDB／Arrow，避免併行 sandbox/activity 首次載入原生模組
造成 `ExplainType` 重複註冊、讓後續 catalog activity 持續失敗。
程式交接先取得遷移來源邊界，再取得原 catalog／replication 鎖；不在寫 head 或驗證中硬停。

進度帶有獨立的 `attempt_id`，新重試不沿用上一輪百分比。舊版進度收據即使存在，
早於本輪開始或 snapshot 不同也只列為前次證據。來源清點階段每約五秒提供已掃檔數／
logical bytes；總量未清點完保持未知，不能把掃描進度當作發布完成率。
控制檔準備另回報已檢視檔數、凍結控制檔數及 bytes；先用檔名分類，避免為尋找少數
控制檔而對數百萬個 immutable payload 額外逐檔 `stat`。
私有原件、共享發布、逐檔還原與 NAS 接受分開顯示，不相加成虛假的全域容量百分比。

## 效能實測與選型

大檔基準與既有 chunk 版本更新改為同一次來源串流完成整檔 SHA、分塊 SHA 與安裝，
移除先讀來源雜湊、再讀來源切塊的重複 I/O；未變檔仍沿用舊 object，完整冷驗證不省略。
選型不靠「壓縮率應該會更好」推定，使用兩份實際 FinLab 原件共 34,136,441 bytes
做四次 A/B/B/A。每次包含複製、發布、完整 object／原檔驗證、還原讀回、未變 no-op、
舊版本再還原與暫存清理。

| 暖快取本機 SSD 隔離測試 | 完整流程平均秒數 |
| --- | ---: |
| 原先雙來源讀取 | 1.267934 |
| 單次來源讀取 | 1.133913 |

這組資料吞吐約提升 11.8%（耗時減少約 10.6%），不代表 D 硬碟、NAS、所有 provider 或
所有 epoch 都同樣加速。收據為
[`complete-pipeline-benchmark.json`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/complete-pipeline-benchmark.json)。

同份真實 Parquet 比較 content-defined chunking：8 MiB 固定切塊 6 塊；8 MiB FastCDC
24 塊仍無重用；1 MiB FastCDC 133 塊只多省 178,230 bytes（約 0.52%）。
因此正式政策維持固定 8 MiB，不因 synthetic insertion 測試漂亮就增加 object／metadata 負擔。
可重跑的隔離工具是 `python -m scripts.benchmark_dataset_version_pipeline`，不是正式資料搬移命令。

設計參照官方 [DuckLake snapshots](https://ducklake.select/docs/stable/duckdb/usage/snapshots)
的邏輯版本與資料檔分離，以及 [Restic design](https://github.com/restic/restic/blob/master/doc/design.rst)
的 snapshot／內容去重分工；沒有為換名再疊一套資料湖。舊 snapshot 過期和刪除仍被引用的
唯一資料是不同操作，參見 [DuckLake expiration](https://ducklake.select/docs/stable/duckdb/maintenance/expire_snapshots)。
本次沒有宣稱理論最小容量、所有 native 整表已分區或全部 NAS 快照已回收。

### 2026-10-17 阻塞修復與驗證

卡住的原因不是只有吞吐低：舊一次性批次長時間持有全域排他鎖，其他來源因此
失敗重試；深層可變收據與 writer 尚未提交的暫存又讓大來源在清點／打包後失效。
上節的共享發布鎖、不可覆寫 CAS、控制檔固定、精確暫存規則及公平重試是同一次修復，
不另開下載管線、不略過原始觀測或驗證。

舊一次性批次已在原冷交易排他邊界退場。交接前後逐一 cold heads SHA 相同，
保留原鎖 inode，未刪來源／冷物件，續行 owner 為原持久 dataset workflow。
證據：[`legacy-publisher-boundary-handoff-20261017.json`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/legacy-publisher-boundary-handoff-20261017.json)。
儲存 worker 的程式更新是另一項交接；等待原 activity 的安全邊界不等於已載入新版，
必須另看 worker-ready 的 loaded SHA 與交接收據，不能用 CLI 計算出的 checkout SHA 頂替。

另一個量到的瓶頸是大清單經 WSL 掛載重複讀取。已沿用原生 Windows byte adapter，
先驗完整壓縮 SHA，再從受容量限制的匿名 SSD 暫存解碼；原始 inode／generation
fence、排序／重複路徑檢查都保留。實際精簡 storage role 讀取 **100,480,531 bytes、
2,564,681 筆**，完整 SHA 加解碼 **73.585 秒**，不是只有約 2 秒的雜湊小測試。
實測最大 RSS 約 2.96 GiB，不能把它當成整個 worker 的峰值。
證據：[`D-inventory-native-complete-decode-20261017.json`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/D-inventory-native-complete-decode-20261017.json)。

實機也捕獲並修復兩項非互動環境問題：scratch gate 誤載訓練依賴，以及 systemd
沒有 WSL 登入環境變數。容量 gate 移至既有無 ML 依賴模組，舊 training-return
入口只是相容委派；wrapper 在沒有既有 WSL 身分時讀取已登錄的節點設定，
仍須確認該 distribution 的實際 registered VHDX。未安裝訓練套件、不猜 distro、不放寬 reserve。

大清點的內部 entries 改為 slots，並在必要驗證後釋放舊 inventory 容器與用完的檔案 hints。
100,000 筆相同欄位的物件分配測試由 19,200,000 降為 10,400,000 bytes，欄位值相同；
這只是物件配置，**不是整條管線 RAM 降幅**。
證據：[`source-entry-allocation-20261017.json`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/source-entry-allocation-20261017.json)。

最後共同回歸 **606 通過**；精簡角色 **160 通過、8 個選用 CDC 跳過**；三條當前
實際 Temporal histories 完整重播通過。再跑完整真實 Parquet A/B/B/A，雙讀平均
1.015206 秒、單讀 0.945970 秒；不同輪次仍有波動，不能將較小隔離測試的 7.3%
吞吐差異宣稱為全來源或 NAS 加速。
證據：[`blocker-final-roundtrip-20261017.json`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/blocker-final-roundtrip-20261017.json)、
[`history replay`](../artifacts/operations/migration-blocker-history-preflight-20261017T0345/acceptance.json)。

11:44（台北）前的固定讀回仍為 60 項中 4 verified、1 processing、31 pending、
21 waiting_retry、3 waiting_source_owner。FinMind 原 activity 的清點路徑持續前進；
已隱藏前一 attempt 的舊進度，不把 01:57 的計數寫成目前進度。原 worker 安全更新
尚待邊界收據，不宣稱新修正已全部在正式程序生效。共享 FinLab 衍生表的來源驗證
也未通過，不假造 release gate 或歷史數據。NAS 傳輸仍在前進，catalog relay 已再次
進入 input_full_SHA；完成接受與舊版回收仍需各自的固定收據。

11:45 的機器讀回已固定於
[`blocker-repair-readback-20261017T0346.json`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/blocker-repair-readback-20261017T0346.json)：
新版 loaded SHA 尚未接手；NAS catalog 已由雜湊階段前進到 immutable_NAS_copy，
peer 尚待約 6.21 GB 傳輸，錯誤計數為零。這是當時的排隊狀態，不是全域完成率。

### 2026-10-17 晚間續查：環境、中斷狀態及備份通知

19:20 台北讀回為 **18／60 項完成固定版本的完整冷還原**，不是 18 個來源的所有
歷史已下載完。原 journal 的另外三列 `processing` 包含兩個中斷前的舊活動；
實際 worker stack 與持續增加的 attempt 進度確認正在處理
`tw-index-derivatives-ticks`。不得把三列都當成三項平行寫入。

本輪根因與修正：

- 分鐘衍生資料驗證器為了取得程式 SHA 而 import NumPy，精簡 storage role 沒有
  該套件，讓同項活動累計重試 129 次。指紋改直接雜湊固定程式檔；真正的完整
  語意／日曆驗證在既有 native data runtime 執行，storage worker 不安裝數值套件。
  全部來源前後 identity、完整分區 SHA、單位與日曆檢查仍保留。依賴失敗只暫緩
  該來源，不再拖住其他來源。
- 取得原 operation 排他鎖後，保存舊 attempt 與中斷收據，再依固定 snapshot 續跑。
  現行狀態同時綁 boot ID、PID/start ticks、activity ID 及 attempt；缺少身分證據
  的舊列顯示 `inflight_unverified`，不是逕自認定仍在處理或已完成。
- SQL 備份成功封存之後的 Syncthing POST scan 曾逾時，造成服務失敗。改沿既有
  通知入口保存 per-envelope scan intent，由原服務重試精確 batch 與 dispatch，
  不因通知逾時重做整份 dump。19:32 實際 service 恢復 `Result=success`，新批次
  `scan_requested`；前一批 11 檔／14,792,647 bytes 已有獨立 NAS restore ACK。

共同回歸 **655 通過**；精簡角色最後一組 **177 通過、8 個選用 CDC 跳過**。
三條原 Temporal 持久歷史均完整重播通過；另用真實一筆原始 Parquet 與其分鐘
衍生分區測 native runtime bridge，日曆／語意驗證通過，修改 bytes 後必須拒絕。
這些是工程驗證，不取代所有正式來源或 NAS 的驗收。

19:34 觀測逐筆來源雜湊已處理 1,911,908／2,471,392 檔、
64,771,403,299／71,179,571,001 logical bytes，仍未進入該來源的完成宣告。
新版主 worker 由 `migration-runtime-safe-handoff-20261017T112901-1a958e2c`
在原 activity 邊界交接；**排隊或 history replay 通過不等於 loaded SHA 已更新**。
交接會另外保存收據並核對三條 chain、PG／Temporal 未重啟與實際 loaded code。

NAS 固定 catalog 575 仍缺該 delivery 的 paired native-restore ACK，receiver
只回 `RetryBackoff`，本機無法從此錯誤類型推定根因。傳輸 peer need bytes/items
為 0，但 ingress 當時為 `scanning`，不能宣稱整體收斂或 NAS 完成；未刪任何
來源、D primary、NAS snapshot 或未驗收的唯一資料。全 60 項任務維持 active。

19:46 第一批修正已實際載入：worker PID 2148 → 117217，載入 SHA
`8fc86ec2a0d07d98dbc2fd57bab20203f2b7bd48f18bc4ce24eac2fd93b5a6bd`；
三條原 chain 保留，PG／Temporal 未重啟。
[`安全交接收據`](../artifacts/operations/migration-runtime-safe-handoff-20261017T1130.json)
不是僅排隊。重新取得原鎖後已保存三筆舊 in-flight 的中斷收據；狀態能核對到
一個真正 active attempt。19:50 已前進到 **20／60**，新增完成
`tw-index-options-daily` 及 `taifex-public-history` 的固定 D 版本完整原檔重建。
NAS catalog 的缺失 ACK 仍是獨立未解邊界。

另查到大型清冊的記憶體瓶頸：舊 worker 曾達約 6 GiB cgroup RAM、5.2 GiB swap，
I/O pressure `full avg60` 約 54%。這是 metadata 工作集換頁，不是網路 API 限速。
第二批修改逐檔釋放已使用的舊 inventory row，最後仍完整掃描所有來源，只不再
配置第二份 `_SourceEntry` 清冊；新增 `verifying_complete_file_storage` 與
`verifying_source_stability` 階段，避免把檔案雜湊 100% 當成整體完成。

[`18,304 檔真實封閉子樹 A/B/B/A`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/census-memory-real-20261017.json)
的 portable／stability 指紋及全部計數一致；Python 配置峰值
10,363,888 → 984,404.5 bytes，降低約 **90.5%**，兩者掃描均約 8.13 秒。
這是 tracemalloc 下的單階段 Python allocation，不是 RSS、全遷移加速倍數或
247 萬檔的實測外推；沒有省略來源／冷庫驗證。

第二批共同回歸 **667 通過**；實際精簡 role **369 通過、8 個選用 CDC 跳過**；
新增 benchmark 的完整 packed/chunk 測試另組 **193 通過**。三條當期持久歷史
完整 replay 通過。
[`33 個 root／132 個版本還原`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/census-memory-all-root-roundtrip-20261017.json)
再次通過，正式 payload／head 零修改，`production_migration_claim: false`。
第二批由 `migration-census-safe-handoff-20261017T115057-b79322d7` 等待原 activity
邊界載入；測試通過本身不表示正式 worker 已載入。

19:55 SQL 狀態再次讀回，本輪第一個新 envelope
`afcb06501b857c580d0927b556f8a833de31163550aa376b6af8e67da19a9ad5`
已於 19:33:06 取得 NAS ACK：11 檔／14,801,859 bytes、完整 SHA 與獨立 restore
皆通過，snapshot 為 `bf18f42c8ae33e862037f41c6d94f5e6a581c172e46851400ec6943e749ee9b1`。
原 timer 已在處理下一批；這個 SQL ACK 不等於 catalog 575 的 native lake ACK。

### 02:14 固定讀回（不是即時進度）

[`application-and-runtime-20261017.json`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/application-and-runtime-20261017.json)
彙整本次接線、測試與實際回收；`migration/result.json` 保留為更早的歷史觀測，不覆寫成新驗收。

- 60 項日誌為 1 個固定來源驗證完成、57 個待處理、2 個等待原服務鎖。
  已驗證來源有 5,614 個原檔、5,069,991,349 logical bytes，逐物件及完整原檔 SHA 均通過。
- 再回收該 operation 自己的 control 暫存 10,543,104 bytes；與先前原生精確副本／
  已接受 transport 回收合計 **4,485,627,523 bytes**。唯一原始觀測、冷物件、NAS／Restic
  歷史及 pin／lease 均未刪除；回收內容有固定還原證據或精確相同原件可供恢復。
- 最新共同回歸 **450 通過，22.67 秒**；locked storage role **41 通過、8 個 optional CDC
  跳過**。原三條實際 workflow 的完整歷史 replay 通過，沒有執行 activity 或重設 chain。
  33/33 個別 README 仍通過，root 清冊零遺漏。
- 冷傳輸、備份、retention、materialization 與原同步重試另組回歸 **140 通過、17 個
  平台／選配項跳過**，7.73 秒；不是 Windows／NAS 實體驗收的替代品，不與其他批次加總。
- 新聞 3,864,379 個全文來源已走完比對，進入 2,413,783 個標題來源；仍須完成 part
  寫入與全量驗證，不能把全文比對完成當成新資料集已提交。
- 原 NAS relay 仍有既有固定 delivery 的 `RetryBackoff`，新 source wave 由原 owner 處理；
  沒有把這些等待寫成 NAS 完成或刪除許可。

需要最新結果時讀 `scripts/dataset_migration_status.py`，不要用這份固定觀測算現在的進度。

02:19 的最後安全交接已通過：worker PID 3741199 → 3797602，載入身分
`daf5e982c138c005e4de80c4bd6eed119d64ac46f1f55fa827a640821dc507e5`
與當前 checkout 相同；三條原 workflow chain 保留、PG／Temporal 未重啟。
收據是 `migration/worker-handoff-backup-intent-v3.json`。先前 600 秒等待逾時只保留為
失敗證據，當時沒有強制停止寫入；後續有界等待取得原交易邊界才完成更新。
新聞 native／FinLab／README／transport cache／原件修復相容性另批 **104 通過**，18.21 秒。
這些工程驗收不等於 60 項搬移與 NAS 驗收都已完成；剩餘實體處理由原工作流程持續。

02:23 讀回已開始下一個來源：1 項通過、1 項處理、56 項待處理、2 項等原鎖。
新 NAS raw wave 的暫緩已明列 `transport_incomplete / wait_for_Syncthing`，
是 payload 傳輸尚未到齊，保留原批次等待，不要求另複製一份；既有 catalog 的
`RetryBackoff` 仍保留，沒有提前刪除任何待驗收資料。
交接後三條完整歷史 replay 亦通過，載入指紋與 checkout 一致；固定讀回在
[`closing-readback-20261017T0224.json`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/closing-readback-20261017T0224.json)。

## 2026-10-17 23 時：大清冊、SQLite 與耐久寫入

這節是後續固定觀測，取代前述歷史段落的「目前」含義；不改寫舊收據。

### 正式進度與實際阻塞

23:09 仍為 **27／60 個 catalog 處理項完整還原驗證**，不是 33 個資料根目錄完成率。
另有 1 項處理、10 項尚待首次處理、17 項等待重試、5 項等待來源責任鎖。
各處理項及父根目錄有覆蓋重疊，不能把各自 logical bytes 相加當成全域分母。
[`固定進度讀回`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/live-progress-20261017T1510.json)。

正在處理 `openbb-task-shards-local`：7,476,049／7,476,049 檔、
92,533,295,320／92,533,295,320 logical bytes 已雜湊，進入 64 個小檔 bucket 打包。
這是來源讀取階段 100%，不是 publication、全原檔還原或 NAS 100%。
舊 worker 只有一個 bucket 完成才回報進度；第一個 ZIP 仍由 749,138,152 bytes
增至 23:13 的 1,108,788,151 bytes，證明正在寫入，不能因完成數不變認定死鎖。

NAS 先前 `RetryBackoff` **已有後續成功證據**：23 時 relay 為 `ready`，
固定 catalog 575 的 paired file/native semantic restore ACK 已接受；23:05 的
`replication-status.json` 亦為 `nas_archive_file_recovery_verified`、
`catalog_semantic_restore_verified: true`，當時 delivery 為
`e20c129e524de0172e69b16215a233c0195ad2c5d45c198d0fb7139f48664dd6`。
這不等於所有後續 wave 或 60 項來源都已在 NAS 驗收；沒有冒充修改 lab203 程式。

其餘日誌仍保留舊失敗：global/source owner busy、來源原件持續變動、FinLab
`.sync.lock`、FinMind promotion、TEJ status，以及新聞 SQLite timeout／消失 SHM。
控制檔／promotion 先前已有精確規則修正；新聞 backup 是本輪修正。
這些尚未全部走到新一輪正式重試，不能把舊錯誤改成成功。未知 Parquet 原件變動
仍必須拒絕提交；正常 downloader／publication owner lock 不能刪掉或繞過。

### 已實作的根因修正

1. **大清冊不再常駐數百萬 Python 物件。** 共用 `MetadataRows` 在 32,768 筆內用
   RAM；超過才轉 private 0700 `/var/tmp` SQLite 索引，8 MiB page cache、空間底線及
   owned cursor 清理。penguin `/tmp` 是 tmpfs，不能把 index database 放那裡假裝省 RAM。
   發布、全檔冷庫驗證及整版 fetch 共用有界 reader；不為每個 bucket 反覆掃全清冊。
   新 pack 最多 32,768 個 member，避免單一 ZIP central directory 隨全市場檔數膨脹。
2. **新小檔不先白讀一次。** 沒有 donor 的小檔在寫 ZIP 的同一 pass 計算完整 SHA；
   保留寫入前後 source stat、原 SHA 比對與最後完整來源穩定性掃描。
   `files_routed`、`files_hashed` 分開；packing 內每五秒可回報真實檔數／bytes，
   不必等巨大 bucket 完成才更新。既有 donor 仍完整雜湊後才判定可沿用。
3. **全檔還原避免重複解壓／索引。** `reconstruct_all=True` 不建立數百萬路徑 set/dict；
   同次解壓完成每個 ZIP member 的 CRC 與每個現存原件的 SHA／長度，包括 reused
   subset pack 中未被選取的舊 member 也完整檢查 CRC。每版所有原件檔數／bytes 必須一致。
   整版 fetch 共享同一索引，不再 verify 完又載入另一份 inventory。
4. **保留每檔耐久性，重疊等待。** 正常 materialize 的四個 bounded fsync worker
   持有各自 dup fd，最多八個 pending；每檔仍 flush、最終 mode、fsync。
   所有 barrier 成功才 rename／`READY`，任一失敗不得提交。沒有以省略驗證或 fsync 換速度。
5. **新聞 SQLite 不因活躍 WAL writer 不斷重啟 backup。** WAL source 先建立 native
   read transaction，固定 backup 的讀取版本，並讓另一連線照常新增；rollback-journal
   source 不套用長讀 transaction。兩端連線明確 close，只將自己的 backup destination
   轉為 standalone DELETE journal，再 quick-check、fsync、完整 SHA 與 seal receipt，
   不改原 DB、不把短暫 WAL／SHM 放入冷版本。既有已完成且 SHA 符合的 backup 仍沿用。

SQLite 頻繁寫入導致 backup restart 與 connection context manager 不負責 close 的依據：
[SQLite Online Backup](https://www.sqlite.org/backup.html)、
[Python sqlite3 connection context manager](https://docs.python.org/3/library/sqlite3.html#how-to-use-the-connection-context-manager)。
測試另以真實 WAL、跨連線並行 commit 與多次 1,024-page backup 證實原件新增不被阻塞，
backup 保持原讀取版本且進度不重置；不是只 mock timeout。

### 有邊界的效能實測

[`最終完整鏈路 A/B/B/A`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/single-pass-complete-abba-20261017.json)
使用每版 2,049 個原檔、強制 512-row spill，以 fresh child 比較同一個 SHA／CRC／reader
實作：reference 為 retained metadata、serial fsync、新小檔預雜湊，**不是舊 production binary**。
測完整 baseline、全檔還原、durable materialization/readback、no-op、增刪改／append、
增量版與舊版本再次完整還原；每次 inventory SHA 與原始 bytes 相同。

| 測量 | Reference 平均 | Candidate 平均 |
| --- | ---: | ---: |
| 完整鏈路 wall | 84.30 s | 35.23 s |
| 完整鏈路 CPU | 15.76 s | 18.97 s |
| fresh-child peak RSS | 43.80 MB | 43.28 MB |

此 fixture 約 **2.39 倍 wall 加速**，但 CPU 約增加 20.4%；不能聲稱所有計算量都已降低。
每輪 wall 有 74.70／93.90 與 45.85／24.61 秒波動，正式服務／磁碟競爭仍存在。
小 fixture 的 RSS 不代表數百萬檔。先前 50,000 檔 benchmark 在逐檔 fsync 等待時
只取消它自己的診斷 run（未中斷 publisher），再找到耐久等待根因；原 cancelled log 保留。

[`300,000 筆 metadata-only A/B/B/A`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/metadata-300k-abba-20261017.json)
用實際預設 32,768-row threshold：RSS **260.98 → 51.31 MB，降低 80.34%**，
wall **6.56 → 26.24 s**。這是受控清冊配置／routing／序列化測試，沒有 payload I/O，
顯示 disk index 的 CPU/I/O 代價；小清冊保留 RAM，不能把這項 tradeoff 藏成全域加速。
現行舊 worker 曾讀到約 5.95 GB RAM、5.03 GB swap，且仍受 6 GiB MemoryHigh／
8 GiB MemoryMax／1.5 core quota 保護，故尚不能聲稱只剩物理硬體上限。
沒有放大配額掩蓋無界工作集，也沒有將小 fixture 外推為 748 萬檔已驗收。

### 驗證、部署與剩餘邊界

- 共享回歸 **686 通過**，101.44 秒；實際 locked role **388 通過、8 個 optional CDC 跳過**。
- [`33 個 root／132 個版本完整還原`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/final-all-root-roundtrip-20261017.json)
  通過，包含原值、負值、NULL、單位與 leading-zero ID；綁定新 metadata helper 的 SHA，
  正式 payload 讀寫／head 變更／舊 payload 刪除均為零。
- [`三條原 workflow 完整 replay`](../artifacts/operations/migration-final-native-history-20261017T1509/acceptance.json)
  通過，events 為 4,129／467／305；沒有 activity execution、provider call、reset 或 service control。
  最初直接 script 入口缺 checkout import 的失敗已改用既有 module 入口驗過，原收據保留。
- 候選 code SHA 為 `f850f3887dd7c650a650c0ae514404f1f7f7406c984c91256324c1193361178e`；
  正式 worker 當時仍載入 `672637325b73b804ed172dc36d29c7fbb314ccc36b52e34c1fab11dedd418792`。
  **測試與 replay 通過不是正式程式已載入。**
- 原 `rollout_lakehouse_source_policy` 已增加最長 24 小時有界等待與 code-drift fence，
  仍先取得 dataset/source/catalog owner locks，才換主 worker，保留三條 chain 與 PG／Temporal。
  第一個六小時等待只取消該維護 run，未停止正式 PID 138561 或任何 publisher；改排
  `migration-bounded-safe-handoff-2-20261017T151311-5ffff5dc` 等待最長 24 小時。
  這是等待期限，不是搬完 ETA；實際載入以預定
  `artifacts/operations/migration-bounded-safe-handoff-24h-20261017.json` 的 accepted 收據及
  `worker-ready.json` 為準，不能因 queued/running 就宣布成功。
- 本輪仍未完成 60 項來源的正式逐檔還原、所有 source/NAS wave 驗收及全歷史整體吞吐測量。
  公開 donor bootstrap 與部分 subtree reader 還有 legacy materialized-inventory 路徑；
  這輪優化是 canonical 發布／整版還原／整版 fetch，不宣稱所有 adapter 的所有工作集都已消除。
  原任務維持 active；本輪未刪來源、D CAS、NAS／Restic 舊版本或未驗收暫存。

## 2026-10-18 01 時：前置清冊修正與正式直接換版

本節是新的固定觀測；以上等待換版的敘述保留為當時歷史。

### 目前進度與接手證據

[`01:25 正式進度`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/live-progress-20261018-final.json)
仍為 **27／60 個遷移處理項完成完整原件還原驗證**，另有 1 項執行中、
7 項尚待首次處理、20 項等待重試、5 項等待來源 owner。這不是下載完整率，
父子來源亦有重疊，不能將各自 bytes 相加當成全域百分比。

上輪等待的 `f850...` 換版其實已於 10/17 23:52 完成，
[`原交接收據`](../artifacts/operations/migration-bounded-safe-handoff-24h-20261017.json) 為 accepted；
OpenBB 並未因此自動成為已驗收，它的中斷紀錄及既有暫存仍保留。

使用者本輪明確允許直接換新版後，已完成
[`正式 worker 替換`](../artifacts/operations/migration-authorized-new-worker-20261018.json)：
PID **533563 → 720193**，01:16:43 讀回實際載入 code SHA
`357b79f52a3ce3d2de2416a85b585f2a1496b761020f1ea1648eeed5e72dc388`。
27 個已驗收 snapshot／manifest 的固定身分均保留，三條 workflow chain 不變，
PostgreSQL 與 Temporal server 未重啟。

這次不是只驗證 active：
[`接手讀回`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/resumed-after-reload-20261018.json)
證明新 PID 已取得原 journal owner lock；舊 `local-tw-minute` attempt
`af0a3a5c02f940c782cfb8149190ab52` 有獨立 interruption receipt，回到 waiting_retry，
原流程依優先順序先處理未開始的 `local-tw-microstructure`，不是把舊進度改成完成。
01:22 已由前置準備、165,244 檔／9,271,512,391 bytes 的清點與 routing，
進入實際打包。未提交 ZIP 不冒充可續接的已完成 pack；再次嘗試仍可能重做未封閉工作。

### 本輪根因與最小共用修正

1. **前置 donor bootstrap 仍曾載入整份 Python list，再建另一個 file dict。**
   現在 canonical private publisher 以 `_iter_inventory` 逐筆讀取 verified gzip，
   用 `MetadataRows.upsert` 保留既有 last-donor-wins 語義，32,768 筆後落入 private SSD index。
   該索引直接交給 publisher 消費與關閉，不再複製 N 筆到第二個索引。
   Public → ignored private 的既有硬連結規則不變；不能反向外傳。
   Hint 仍不是證據：每個目前原件必須完整 SHA，正式完成仍須完整冷庫重建。
2. **SQLite 探索先對每個檔案查 symlink，再判斷副檔名。**
   改成先用名稱篩選 `.db/.sqlite/.sqlite3`，只有候選才查 metadata／header。
   2,048 個普通 Parquet 的測試強制禁止任何額外檔案 lstat，仍能找到真正 SQLite，
   排除偽副檔名與 symlink。準備／探索／bootstrap 都有真實階段與 examined 計數；
   總量未知就保持 NULL，不標成 100%。單個大 donor object 的完整 SHA 仍可能需時。
3. **永豐 `shioaji/download_report.csv` 是持續更新的操作報表，不是行情原件。**
   精確加入 bounded control capture，保留原始每個 byte、stable-FD 與 size/mtime fences。
   Control scope 升為 v3，可相容升級原 v2 intent，沿用原捕獲值／SQLite backup。
   不將所有 CSV 都視為控制檔；普通行情 CSV、Parquet 的變動依然拒絕提交。
4. **只有等待活動邊界不能滿足本次直接換版。**
   原 rollout 入口新增明確 code-only `--restart-active-owner`，先落地 restart intent、
   原 PID、固定已驗收綁定及 in-flight attempt，再停止唯一 worker；之後取得原 owner 鎖，
   不刪鎖、不 reset Temporal、不重開 PG、不捏造舊任務成功。候選漂移、已驗收版本
   遺失／被重新綁定時拒絕 accepted，失敗收尾仍確保原 service 可啟動。

### 實測與限制

[`30 萬筆 bootstrap A/B/B/A`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/bootstrap-300k-abba-20261018.json)
直接執行 canonical donor reader／hardlink／index；reference 模擬原 retained list + dict，
不是舊 binary。四次完整輸出 inventory digest 相同。

| 範圍 | Reference 平均 | Candidate 平均 |
| --- | ---: | ---: |
| donor 清冊 peak RSS | 536.98 MB | 88.55 MB |
| donor 清冊 wall | 17.12 s | 27.97 s |
| donor 清冊 CPU | 16.38 s | 25.01 s |

記憶體降低 **83.51%**，但 index 的序列化／磁碟成本增加；這不是全流程吞吐加速，
亦沒有當成原件還原或實際來源覆蓋驗收。避免無界 RAM／swap 比這段清冊 microbenchmark
的最低 wall 更重要；小清冊仍保留 RAM 快路徑。

[`完整鏈路 A/B/B/A`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/bootstrap-complete-abba-20261018.json)
每版 2,049 原件，包含 baseline、no-op、增刪改與 append、全檔 SHA/CRC 還原、
durable materialization/readback、再次讀回舊版。全部 inventory 與原始 bytes 相同：
wall **109.56 → 37.55 s，約 2.92 倍**；CPU **17.85 → 20.27 s，增加 13.58%**。
Reference 是 retained metadata + serial fsync + new-small-file prehash，不是舊 production binary。
這是有界私有 fixture，並非 748 萬檔、所有 provider 或 NAS 的倍數。

共用回歸 **698 通過**，locked control role **400 通過、8 個 optional CDC 跳過**。
補入 benchmark reader parity 與最終 restart receipt 欄位後的 focused 測試 **150 通過**。
[`33 根目錄／132 版本還原`](../artifacts/data_quality/all_dataset_incremental_layout_20261016/migration/bootstrap-all-root-roundtrip-20261018.json)
再次通過。換版
[`前`](../artifacts/operations/migration-pre-reload-history-20261018/acceptance.json)／
[`後`](../artifacts/operations/migration-post-reload-history-20261018/acceptance.json)
三條正式原生 workflow 完整 deterministic replay 均通過，沒有執行 activity 或重置。
新版亦已實際接手原 journal，不能將 replay 與接手混成同一種證據。

本輪本機收到的 NAS readiness 仍為最新、mount／owner／runtime 三個 gate 均 true；
來源複寫為 monitoring_increments，當前 paired catalog ACK 為
`nas_archive_file_recovery_verified` 且 `catalog_semantic_restore_verified: true`。
這只驗收該固定 delivery，不是全部 60 項都已在 NAS 驗收。

**尚不能聲稱只剩硬體極限。** 正式完整歷史尚未跑完，仍有來源協調鎖、可變原件的
重新嘗試、未封閉 pack 的重做、Python／SQLite 工作與 cgroup 限額；部分 subtree reader
也仍使用 legacy inventory list。還原、刪除及原始單位／NULL／時鐘契約不因提速取消。
原任務維持 active；本輪未刪原始資料、D CAS 或 NAS／Restic 歷史。

## 2026-10-18 02 時：清冊去重、D 原生還原與可恢復封裝

本節延續同一個 `all-dataset-incremental-layout-20261016` 任務與原操作 journal，
沒有另開資料管線。02:13 台灣時間觀察為 **28／60 項完整驗證完成**；
台股微結構固定版本已在 02:11 完成 **165,244 原件、9,271,512,391 logical bytes**
的全部重建，接著處理 `local-taifex-public-history`。這是 D 固定版本還原，
不是全部來源、最新當下全庫、NAS 或來源刪除的驗收。

### 根因與實作

1. 全清冊原本反覆保留各歷史 manifest 的完整 object 字典，並額外組裝整份 JSON
   再雜湊。現在仍逐份讀完原文、驗 SHA／完整 schema／head／HLC／owner，
   但一個 pass 只保存 compact binding、共用 object reference，並串流產生
   **完全相同**的 canonical JSON identity。Pinned no-follow 目錄 FD 經最終重驗後關閉；
   不引入跨 pass 的 stat-only 信任捷徑。也修正「object 缺失時漏掉衝突 bytes descriptor」
   的檢查順序。
2. 大型 private D pack 先前走可攜 Python 隨機讀取。現在只對原本 D 與其精確
   ignored private-history 權限邊界，沿用既有 native SHA／受保護 SSD staging reader；
   mount、owner、0700、marker 與 export-false 均須通過。每個 ZIP member 的 CRC
   和每個當前原件 SHA 仍完整驗證，沒有啟用 private blob 捷徑或跨主機匯出。
3. 首次私有 baseline 未 commit 前，崩潰會丟失已封閉 pack 的路由，重試又封裝一遍。
   現在在**原 operation** 的 `sealed-pack-checkpoints/<dataset>` 保留有界、完整內容
   雜湊的 closed-pack routing hints。只有已完整安裝且目錄同步完成的 pack 能入列；
   重試仍重新 SHA 每個當前原件，完全相同才重用。已 commit inventory 優先，
   partial ZIP／舊 orphan pack 不會自動變成 donor。壞 JSON、過深 JSON、錯誤綁定、
   FIFO、缺失物件留存並降級重新讀取原件，不冒充成功。
4. 暫存索引退役原本每個小檔都重掃 `/proc`、重寫全日誌。現在一次批次檢查，
   任一正引用保守保留整批；逐檔 SHA／nlink／最終身分檢查不省略，先持久化刪除 intent，
   再 unlink 並同步目錄。崩潰後可續接，無法確認的已釋放 bytes 不計入 reclaimed。
   只有固定版本及其 SQLite 狀態全部還原通過後，才刪這些可重建暫存 metadata；
   不刪原始資料、CAS、NAS 或 Restic 歷史。

### 完整流程實測與未採用的候選

| 量測範圍 | Reference 平均 | Candidate 平均 | 結論 |
| --- | ---: | ---: | --- |
| 清冊 capture，2,048 原件／256 歷史版本，wall | 7.765 s | 3.096 s | 約 2.51 倍 |
| 同上 process peak RSS | 606.49 MB | 42.49 MB | 少約 93.0% |
| 真實子程序崩潰、續抓、全還原、增量及舊版讀回，wall | 72.078 s | 37.284 s | 約 1.93 倍 |
| 同上 CPU | 11.368 s | 11.221 s | 接近，不宣稱 CPU 倍數 |
| 同上重試需重新封裝的檔案 | 2,048 | 256 | 少 87.5% |
| Forward-only ZIP 寫入，全鏈路 wall | 62.127 s | 81.642 s | 退步，未採用正式預設 |

清冊 A/B/B/A 的 124,041,277 manifest bytes、524,800 required references
產生完全一致的 catalog identity，並完成兩個固定端點的原件重建。
這是生成清冊 fixture，不是把 production 原先 583.93 秒直接實測降到 3 秒。
收據：[完整清冊 ABBA](../artifacts/benchmarks/dataset_migration_catalog_20261018/capture-2048x256-abba/acceptance.json)。

中斷續抓 A/B/B/A 每輪使用全新程序與私有 SSD fixture，以 `os._exit(88)` 在
第八個 pack 中斷，原先七個已封閉 pack 可重用。全部四輪仍逐原件重建、校驗
SHA／CRC、durable materialization、三種增刪改與再次讀回舊版；兩個完整 inventory
digest 均完全一致。候選 wall 為 34.92／39.65 秒，reference 為 89.75／54.41 秒，
可見 host I/O 仍有抖動；兩種版本使用相同 1.5 CPU、nice／affinity、6／8 GiB 上限。
這是**有中斷**的局部全流程，不是健康首次發布、全庫或 D／NAS 的速度保證。
收據：[續抓完整 ABBA](../artifacts/benchmarks/dataset_migration_catalog_20261018/sealed-pack-retry-2048-native-abba.json)。

Forward-only ZIP 雖加快初次寫入，完整還原／materialization 流程反而慢 31.4%，
已撤回 production 改動，僅保留 benchmark adapter 與格式／CRC 測試。
原 production seekable ZIP writer 未改。
收據：[未採用候選](../artifacts/benchmarks/dataset_migration_catalog_20261018/sequential-pack-native-d-abba.json)。
另一個 native idle signature ABBA 因正式 metadata 在第四輪變動而正確拒絕，
不能引用其中的時間當成已驗收加速。

### 驗證及正式採用邊界

新版共用控制環境回歸 **627 通過、8 個 optional CDC 跳過**；
補入刪除中斷／檢查後競態後，focused **148 通過、8 跳過**。
兩者範圍重疊，不相加成獨立測試數。
換版前[三條原生 workflow 完整 history replay](../artifacts/operations/migration-sealed-pack-history-replay-20261018/acceptance.json)
通過，沒有執行 activity、provider request 或重置 workflow。

第一次正式[清冊／原生還原換版](../artifacts/operations/migration-catalog-native-new-worker-20261018.json)
已由 PID 720193 換為 845339，保留原 27 項固定版本與三條 workflow 鏈；
之後由原 journal owner 自動核對中斷，接回同一個台股微結構版本並驗到完成。
sealed-pack 後續換版與即時進度須分別讀
[正式換版收據](../artifacts/operations/migration-sealed-pack-new-worker-20261018.json)
和 `scripts/dataset_migration_status.py`，不能用離線測試冒充正式 worker 已採用。

02:19 已完成第二次 code-only 換版：**PID 845339 → 908556**，實際載入
`2192a4dd2162eb095319685331d2646449457eadbc0ed6c8f611e5f6a9012272`，
保留全部 **28** 個已驗收版本、三條原工作鏈與原私人政策，沒有重啟 PG／Temporal。
02:19:54 原 owner 已自動保留期交所中斷證據並排回重試，實際開始下一個
`local-tw-futures` attempt `380660bcab254e13b1acc881bcaa02a8`，不是只看到 service active。
[換版後三條 workflow replay](../artifacts/operations/migration-sealed-pack-post-replay-20261018/acceptance.json)
再次通過；其原 run IDs 未改。最新數據 runtime 整合另有 **121 通過**，
不向 locked control role 補入 NumPy／模型套件。

[02:23:30 即時進度](../artifacts/operations/migration-catalog-checkpoint-live-progress-20261018.json)：
28 verified、1 processing、5 pending、21 waiting-retry、5 waiting-source-owner。
當前為期貨 public donor CAS 的完整 SHA／重用準備；Python stack 確認在原
`_bootstrap_public_storage → sha256_file`，不是另一個下載器或跳過雜湊。
大型物件 SHA 期間 row-based 準備進度可能暫不增加，不能把「0 files completed」
解讀為整個服務沒有動作，也不能拿它充作 bytes-completed。來源分支有重疊，
不能把 28／60 換算成全庫 bytes 的 46.7% 或據此給出完成時間。

**尚未達到「只剩硬體」或全數完成。** 尚有真實來源協調鎖、缺少完成 gate 的
來源收據、會變動的原始 payload，以及大型 census／Python／SQLite／還原 I/O。
既有排程會繼續處理其餘來源與到期重試；錯誤文字可能是上次失敗，須以當前
attempt、owner、進度與新驗收收據區分。NAS readiness 的 mount／single-owner／runtime
三 gate 在 02:15 仍為 true，僅表示該觀察的遠端接收條件，不代表全部 60 項都已驗收。
原任務繼續 active，不回收來源或歷史 CAS。

## 2026-10-18 08 時：修復全清冊描述符耗盡並正式接手

08:45 台灣時間，原 journal 為 **32／60 項完整驗證完成**，較 02:23 增加四項：

| 固定來源 | 原件檔數 | logical bytes | 台灣驗收時間 |
| --- | ---: | ---: | --- |
| `local-forex-frankfurter` | 1,781 | 284,554,568 | 04:43 |
| `free-public-context` | 1,527 | 2,797,565,873 | 06:49 |
| `tw-index-futures` | 5,108 | 4,145,799,683 | 06:51 |
| `local-tw-index-futures` | 60,960 | 8,915,541,199 | 07:10 |

這些來源有相互重疊的分支，不相加成全庫唯一原件數或 bytes 百分比。
當時另有 24 項等待重試、3 項等待來源 owner，以及 1 項正在處理。
`local-tw-minute` 已完成清點 **567,978 個檔案、20,123,997,443 logical bytes**；
08:45:35 正在封裝，已雜湊 182,067 個檔案、處理 12,006,470,726 logical bytes，
完成 14／58 個封裝群組。這些是當前 attempt 的雜湊／封裝進度，不是全檔冷還原
驗收、下載歷史完整率或全庫的 bytes 百分比。

### 根因，不是 D 盤突然消失

上輪清冊優化新增的 pass-local metadata 目錄快取沒有容量上限。
實查冷庫有 **1,920 個 metadata 目錄、768 個 object shard 目錄**；
worker `/proc/<pid>/limits` 的 **soft NOFILE 為 1,024**，hard 為 1,048,576。
深歷史測試只有單一 dataset，沒有涵蓋目前的多 dataset 寬度。
原 worker 在 08:30 再次出現 `Errno 24`，甚至無法寫 progress journal，
使 OpenBB 百萬檔清點中斷，其他來源的 subprocess mount guard 也可能失敗。
這不是永久 FD 洩漏：掃描結束後實查降回 22；問題是單輪同時持有無界 FD。

共用 `packed_backup.PinnedObjectSignatures` 現在改為：

- 每個 reader 最多快取 64 個目錄 FD，root 持續 pinned；開啟新 child 時只有一個
  暫時額外 FD。所有已觀察目錄身分仍保留，淘汰後再開必須相符，最後逐一重檢。
- 命中完整 parent 時直接查表，不重走整條 ancestor；新目錄仍逐層 no-follow。
- 共用 export threads 的 lookup／stat／read／evict 由同一 instance lock 保護，
  避免其他執行緒淘汰正在使用的描述符。成功、例外與 file-wrapper 建立失敗都關閉 FD。
- 完整 metadata bytes、SHA、head／manifest／clock 綁定及冷還原全部保留。
  FD 空間由 O(目錄數) 改為有界；必要的目錄身分記錄與最終重檢仍為 O(目錄數)。

`cold_primary` 的錯誤現在保留安全的 exception class／errno 或 timeout，
不再把本機資源耗盡隱藏成沒有細節的 mount error；掛載 guard 仍然 fail closed。
該模組也納入 loaded worker code identity。未提高程序 FD 上限、未減少清冊範圍。

### 驗證與量測邊界

原實作已在真實 soft NOFILE=256 的子程序重現 `Errno 24`。
修正後同一測試完成 400 個 metadata 子目錄與 256 個 object shards 的巢狀掃描，
並另驗 161 個 dataset／322 個固定 release 的完整 capture、head／canonical hash，
以及淘汰目錄被替換、symlink、FIFO、例外關閉與 8-thread 共用情境。
測試只核對己方 fixture 的 FD；不將其他 SQLite finalizer 關閉描述符造成的程序總量
下降誤判成 scanner 洩漏。

256 datasets／4 vintages／64 原件的同條件 A/B/B/A，1,024 releases、
16,697,580 manifest bytes、67,584 required references，兩個固定端點完整原件讀回通過。
四輪 canonical catalog identity 相同，**巢狀 reader cache 峰值 578 → 127**，
少約 78%。初版 wall 0.890 → 0.934 秒；direct-parent 版為 0.879 → 0.956 秒。
這些短 SSD fixture 沒有證明 wall 加速；採用的原因是消除正式全清冊無法完成的
資源耗盡，不以舊版失敗前的部分掃描當作成功且較快的基準。
[完整量測](../artifacts/benchmarks/dataset_migration_fd_20261018/wide-256x4x64-fast-abba/acceptance.json)
不等於 native D、DuckLake register、NAS 或全庫 migration 的速度證明。

控制環境共用回歸 **689 passed、10 optional skips**，資料環境整合
**218 passed、2 optional skips**；兩者重疊，不相加。
換版前後均有[三條原生工作鏈完整 replay](../artifacts/operations/migration-bounded-fd-post-replay-20261018/acceptance.json)，
沒有執行 provider activity 或 reset。

### 實際換版與進度觀測

08:33:55 已由正式 code-only 交接入口將 **PID 908556 → 1808660**，
實載 `bbab5444cf0467424a7702a51d194011fb81697d2fae482ebf8d7421af0411a8`。
原 **32 個固定驗收版本、三條持久 workflow 鏈、私人 policy** 全保留，
未重啟 PostgreSQL／Temporal，未刪 lock 或手動改寫失敗 row。
[交接收據](../artifacts/operations/migration-bounded-fd-native-rollout-20261018.json)。
08:34:34 原 journal owner 自動留下中斷收據並續排，開始 `local-tw-minute`
attempt `c9d4f251957a4792a86c1077376de412`。
08:37 的實際 stack 同時確認正式全 catalog 已進入 object signatures 階段，
而分鐘來源仍在清點；實查共用 worker 約 **151／1,024 FD**，不是只看到 active。

**正式全 catalog 已在 08:41:56 完成登錄**：2,207 個 releases、53,724 個
source-object-version keys、0 個 unavailable releases，完整 register 約
421.196 秒。這是有界 FD 修正後的實際正式完成證據；keys 含 metadata／history
角色，不等於 53,724 個唯一實體 payload，也不表示 2,207 個版本均已完成全檔
SHA／CRC 冷還原。來源清冊與前次不同，421.196 秒不能當作同條件 ABBA 加速倍數。
原 `source-catalog-refresh.json` 已轉為 `registered`；08:45 的來源複寫狀態也已
清除 `catalog_refresh_error` 並收斂至 `monitoring_increments`、pending bytes = 0。
因此先前 processing 階段暫時保留的錯誤不再是目前故障，沒有為清顯示而刪歷史
失敗收據或人工改寫狀態。這個複寫收斂只涵蓋已登錄來源，不代表未完成的 28 項
遷移亦已驗收。

08:45:37 的分鐘封裝仍由同一 owner／attempt 前進，抽樣 **153／1,024 FD**；
台股分鐘來源的 53,257 個原件已有封閉封裝提示載入，全部仍須符合當前原件 SHA
才重用。其餘 retry row 保留的是各項上次失敗；不能把舊 `Errno 24`、舊鎖定
或 generic mount 訊息一律稱為新版現在仍失敗。原 queue 依到期重試與來源完成
gate 續跑，不刪鎖、不回填成功。

唯讀 `scripts/dataset_migration_status.py` 另提供
`runtime_owner.file_descriptors`，契約 `process_fd_budget_observation_v1`：
目前 open／soft limit／remaining／fraction，讀取前後核對 PID start／boot generation。
讀不到或 owner 不相符就標 unknown，不填 0；這是非 atomic 的程序資源觀測，
不冒充某項 activity 的工作量或資料驗收。
[本輪即時快照](../artifacts/operations/migration-bounded-fd-final-progress-20261018.json)
及原入口才是後續進度，不能把本節固定觀察當永久狀態。

實載 worker identity 以交接收據、該程序 generation 及 `worker-ready.json` 核對；
`manage_lakehouse status` 目前的 `worker_code_identity_sha256` 是**工作樹候選**的
計算結果，不能單獨當實載證據。本輪換版後另有其他工作加入 opt-in 的
`runtime_env.sh` helper，不能因此冒稱正在執行的 worker 已載入該新候選；
本輪有界 FD 修正仍由 08:33 的實載指紋與後續完整登錄證明。

**原任務維持 active，尚未達成全庫完成或只剩硬體極限。**
正式 full-catalog register 已通過，仍須各來源重試與全檔冷還原結果；
真實 source locks、缺少 canonical 完成 gate、變動 payload 與大型 census／SQLite／I/O
也尚未全數解除。原始資料、D authority CAS、NAS／Restic 歷史均未由本輪刪除。

## 2026-10-18 午後：可變檔案重試根因、串流擷取與安全暫存清理

13:48 台灣時間原 journal 已有 **37／60** 項完整驗收，較 08:45 多五項。
其中 `local-tw-index-derivatives-ticks` 有 **2,471,393 原件**、
71,179,572,738 logical bytes，10:27 完整驗收；其他新增為
`tw-microstructure-train`、`tw-microstructure-captures-cold`、`openbb-compact`、
`yahoo-market`。仍不能將互相重疊的來源檔數／bytes 相加為唯一全庫容量。
起始現況見[時間固定快照](../artifacts/operations/migration-progress-start-20261018T1348.json)。

### 修正的根因與保留的邊界

1. 分鐘來源的 fingerprint 原本間接匯入 Polars：
   `shioaji_minute_backfill_state` 頂層載入 `stock_volume_units`，但實際只有
   materialization reuse 才需要 reader ABI 常數。改為該函式內延遲載入，
   fingerprint／unchanged guard 維持標準庫；完整語意稽核仍委派原資料環境。
   新測試在子程序禁止 NumPy／Polars／Torch import，仍核對原 identity 相等、
   原件變動必須拒絕；沒有把語意稽核刪掉來讓控制角色通過。
2. `recursive_mutable_controls_v4` 同時辨識 `shioaji/download_report.csv`、
   實際使用的 `shioaji_1m/download_report.csv` 與相應來源根目錄。
   v3 已完成遞迴清點的 preparation 升級時，只探查命名報表及已知輸出目錄，
   不重掃百萬原件、不重做 SQLite backup、不覆寫舊觀測。
   其他 CSV／未知 payload 仍受原始來源變動檢查。
3. 私有 observation cohort 可固定擷取已知 writer **已關閉、原子替換發布**的
   CEX `_hot_tail` Parquet、TAIFEX `rules/normalized` 彙總表，以及 OpenBB
   `openbb_l1.duckdb`／`catalog/l1_compaction_status.parquet`。
   OpenBB producer 先 checkpoint、關閉、驗證暫存庫，才 `os.replace` 正式庫；
   這不是直接複製任意活躍 DuckDB。正式庫旁若有 WAL，拒絕並交原 owner 恢復。
   使用最多 8 MiB 區塊、持有來源 FD、完整 SHA／stat fences、5 GiB scratch reserve、
   fsync 及原子 no-replace 安裝；取消整檔載入記憶體與 64 MiB 單檔門檻。
   70 MiB fixture 的 Python 配置峰值小於 32 MiB，且原件 SHA 相同；這不是 native RSS 證明。
4. OpenBB compactor 的特定 PID 暫存 sibling 由 manifest 宣告的
   `openbb_l1_atomic_sibling_v1` 處理；不忽略其他 provider 或任意 `.tmp` 原件。
   已擷取路徑若在初次 cohort census 前消失／被排除，明確拒絕，不偷偷遺漏。

以上不把分別觀測的檔案包裝成原子 workspace，也不授予私人資料 shared／NAS
export 或 source retirement 權限。公開 OKX／Bybit 的完整來源 gate、未註冊跨根
absolute symlink、FinLab 完成證據等仍各自獨立，不能藉私人固定擷取宣稱已修復。

### 完整流程驗證與量測

最終共用回歸 **650 passed、13 optional skips**；另有資料環境聚焦驗證
**162 passed**（process references、分鐘 state/dataset、runtime 與 migration），
兩者有重疊，不相加。受控 A/B/B/A 包含 v3 preparation 升級、
發布／失敗重試、所有原件的 SHA／CRC 冷重建、獨立實體讀回與己方 scratch 清理。
2,048 個 immutable 原件加一個同值原子替換的尾段，四輪 inventory SHA 完全相同。
reference 兩次 attempt，修正後一次；reference 的實際 1.05 秒 retry wait 仍計入。

在與當時原 owner 相符的 cgroup／affinity／nice 條件，reference 平均
**11.494 秒 → 8.828 秒**，完整 wall 少約 **23.2%**，CPU 約 5.997 → 5.715 秒。
這是本機 SSD 受控 mutable-file rewrite workload，不是所有來源或 D／NAS 的
吞吐量倍數，也沒有證明只剩硬體瓶頸。
[最終完整 ABBA 收據](../artifacts/benchmarks/dataset_migration_mutable_outputs_streamed_20261018.json)。

### 清理範圍與可恢復性

沿原始 EMFILE 失敗 attempt 找出可重建的 `MetadataRows` SQLite 索引，逐項核對
完整 canonical provenance、private ownership、唯一硬連結、SQLite exact schema／
integrity、SHA 與所有程序引用。只有原 owner activity 已被接替且無引用才可進入
quarantine，重新驗證後刪一個 `index.sqlite` 與空目錄；不按目錄前綴猜測刪除。
原失敗收據與索引 SHA 都保留，可由原始來源重建索引，但不保留無用索引內容副本。

最先兩項已實際回收 **1,699,692,544 allocated bytes**；其餘九項亦已完成同樣
dry-run／quarantine／重檢／unlink，回收 **9,346,801,664 allocated bytes**。
本輪共 **11 個索引、11,046,494,208 bytes，約 10.29 GiB**，依實際
`st_blocks × 512` 計算，不用 logical bytes 或硬連結表面大小灌大數字，也不代表
Windows VHDX 已縮小。其餘九項由固定失敗收據明列，不藉 `/var/tmp` 名稱全掃後刪除；
[清點／dry-run](../artifacts/operations/migration-failed-indices-remaining-20261018/audit.json)
與[逐項 apply 收據](../artifacts/operations/migration-failed-indices-remaining-20261018/retirement.json)
分開。`/var/tmp` 當時約 15 GiB → 4.4 GiB，總量亦含其他工作與活躍索引，
不是可再刪 4.4 GiB 的證明。未知用途的暫存、4 個 pins／2 個 active leases 的
materializations 全保留，cache GC dry run 為 **6 checked、0 evictable**。
原始來源、固定 release、D CAS 與 NAS／Restic 歷史不在這次索引清理目標。

### 正式交接與清理後驗證

14:13:23 已沿正式 code-only 入口完成 **PID 1808660 → 2685566**，實載
`b14f8230ddddd52ba45803a29d063d42594bc88dc64fb81fd38aad69153c1fc9`。
[交接收據](../artifacts/operations/migration-streamed-native-rollout-20261018.json)
證明原 37 個固定 snapshot／manifest bindings 保留，三條 workflow 的
first-run／當前 run identity 均保留，私人 configuration 未改，PG／Temporal 未重啟。
換版前後均完成[三條完整工作鏈 replay](../artifacts/operations/migration-streamed-post-replay-20261018/acceptance.json)。

14:15 的獨立清理後 cold readback 核對原固定 `openbb-compact` release 的全部
**8 原件、57,031,609 bytes**，完整 SHA／CRC 還原通過，固定 manifest 未變。
[冷讀回收據](../artifacts/operations/migration-cleanup-fixed-cold-readback-20261018.json)
只涵蓋該 release，不冒充所有版本或 NAS 歷史重新驗收。原 automation live check
仍為 D guard passed、配對同步 idle／100%；此與 NAS 所有來源覆蓋不同。
當時 NAS 去重清冊 1,339.839／1,378.978 GB，尚缺 39.139 GB／5,012 檔；
分母隨其他來源持續新增，不能用前後不同分母百分比判定掉資料。

14:18:33 原 operation 為 **37 verified、20 waiting_retry、2 waiting_source_owner、
1 processing**。新 owner 正在 `binance-public-archive` 同一 attempt，
已從 14:14 的 4,209,639,385 bytes／4,796 原件，前進至
**95,559,304,141／97,928,208,352 logical bytes**、114,749／117,405 原件。
這是 hashing／routing，不是全檔冷還原完成；當時 FD **156／1,024**，
owner lock、PID start／boot 與 attempt 均相符。
[即時投影固定收據](../artifacts/operations/migration-streamed-current-20261018T0618.json)。

**原任務仍 active，尚未全庫完成，也未證明只剩硬體速度。**
已修正的新範圍會沿原 queue 重試；公開來源的穩定發布窗口、
`local-tw-futures` 跨根 compatibility symlink 的 lossless binding／reconstruction、
缺失的 canonical source-completion gates，以及各剩餘大庫全檔重建仍須完成。
不把保留來源 gate、清掉舊錯誤文字或只有 service active 當作修復。

## 2026-10-18：原來源協調、期貨跨根介面及 FinLab 完成證據修復

### 進度定義與目前驗收

15:27:27（Asia/Taipei）原 operation 為 **38／60 verified**，另有
19 waiting_retry、2 waiting_source_owner、1 processing。這個分母是原登錄來源，
`verified` 要求固定 v2 release 與全部原件 cold reconstruction，並非某 API
下載完成數、資料新鮮度或 NAS／遠端訓練就緒數。
[當時即時投影](../artifacts/operations/migration-coordination-current-20261018T0729.json)
的實際時鐘以 `observed_at_utc` 為準。原 owner lock、PID start／boot 均相符，
當時使用 27／1,024 FDs；正在清點 `tw-index-derivatives-ticks`，尚未有完整分母，
不能把清點筆數當作已驗收筆數或推算可信的全庫 ETA。

新增驗收的 `binance-public-archive` 固定版本為
`binance-public-archive-20261018T061857739313722Z-l0-penguin-8f629be180dc37aa`，
manifest SHA `abe1a42cc10237640819359eafac41d3571edc45accde6021c3681e05dbef11e`。
原 operation 的 `recovery/binance-public-archive.json` 證明全部
**117,405 原件、97,928,208,352 logical bytes** 完整冷還原通過，耗時
1,968.78 秒。這是該固定 D release 的證據，不是 Binance 線上來源、所有版本、
NAS 歷史或遠端 materialization 的完成證據。

### 修復一：以原寫入鎖協調，而非程序名稱猜測

`publish_data_releases` 在原 catalog owner 外，同時取得 CEX 實際 collector／daily
materializer 共用的原 grain `.download.lock`，固定順序、共用 bounded wait budget。
all-ready 遇忙立即讓出，不等待 180 秒拖住其他來源；pack 完成後釋放來源鎖，
完整 cold recovery 不持有來源鎖。原 stat／SHA／來源穩定性與完成 gate 全保留。

Bybit 既有的整根 `data_bybit -> /srv/stockagent-live/data_bybit` 是已登錄介面，
不能誤認為不安全 grain redirect。鎖映射先綁定該根的真實路徑；其下的 grain
重導、鎖檔 symlink、非原來源路徑或原 inode 被替換仍拒絕。

另修正「等鎖程序仍被當作寫入者」的假阻塞：只在發布器已持有全部原 native
owners，並由新鮮 `/proc` command／FD 查到已稽核 canonical collector 開啟同一
inode 時，將其記為 `coordinated_writer_waiters`，而不是 active blocker。
未知腳本、其他 output inode、缺少來源收據仍阻擋，status-only 不享有此豁免。
[原機唯讀 admission](../artifacts/operations/migration-native-cex-lock-admission-20261018.json)
中三家均能取得原鎖且 ready；沒有重啟下載器、呼叫 provider 或另開發布。
跨程序測試另外覆蓋真實等鎖子程序、custom output 與未知腳本的隔離。

### 修復二：期貨跨根連結是版本依賴，不是垃圾或任意 follow-link

只為 `local-tw-futures` 的三個已清點介面 `margin_sources`、
`margin_repair_pending`、`margin_repair_inputs` 建立 private typed bindings，
指向原 operation 已完整恢復的 `local-tw-index-futures` 固定版本與子樹。
保留原絕對 link bytes、mode 與前後 inode fences；未知／新增跨根連結仍拒絕。

原先「inventory 少一檔」實際是 SQLite：該 main/WAL 已由原生 backup API
另存成固定一致版本，不應當普通檔案遺失，也不能要求目前的 live main/WAL
與那個一致快照 byte-identical。修正後逐項綁定原 SQLite snapshot／manifest／
完整冷還原收據，且明列這是原固定 cohort，不是假稱目前資料庫最新狀態。

[原機 preflight](../artifacts/operations/migration-futures-source-link-preflight-20261018.json)
用 47.33 秒核對 **57,232 普通原件、6,033,838,338 logical bytes** 及獨立 SQLite
依賴。`packed_snapshot fetch-source-view` 以固定 cold components 組裝可攜式
private view，只有全部完成後的 `VIEW.json` 證明整個介面，單個 `READY` 不夠。
測試覆蓋遷移、全部組裝、SQLite 的負數／NULL 及之後 live DB 改值仍還原原快照。
**preflight 與測試不等於 `local-tw-futures` 原機全庫遷移完成**；原 queue 仍須
完成 main release 與所有固定依賴的完整還原。不擴大 shared／NAS／刪來源權限。

### 修復三：FinLab 依資料粒度驗收，再由原 builder 重建血緣

原目錄 1,352 keys 包含 1,349 張已取得一般表、一張來源真全空表，以及兩個
`tw_minute:2330`／`tw_tick:2330` 分區端點範本。兩個範本不是失敗的整張矩陣，
分鐘／tick 歷史仍由原分區 queue 獨立驗收。新的
`finlab_general_catalog_release_v2` 只對 **1,350 一般 keys** 宣告 resolved，
且明列分區歷史不在此 gate 的驗收範圍。

低頻表依 canonical collector 的 SDK expiry／原 retry deadline 判定 due，
目錄發現仍限 24 小時；不再讓數百張未到期表因 blanket 24h 被判 stale。
真全空表保留原 raw SHA 證據與期限，不補零。完整封版逐一驗 SHA；同一 physical
inode 只讀一次但每個 logical expected SHA 都核對，最後再次檢查所有路徑。
catalog scope、digest、full-hash proof 與最早 valid-until 都進 release receipt，
發布 commit 再檢查，不能把長時間建置耗掉的有效期當作成功。

原 stage 確實攔到 formal／research base 血緣落後，已讓原 FinLab 服務呼叫
既有 union builder 更新，再由既有 overlay／stage／publisher 完成，不改寫完成旗標。
staging 改為 formal source 的 SH 讀租約加自己的 EX 輸出租約；模型的 SH reader
不再阻塞獨立 FinLab 輸出，formal EX writer 與同輸出的 publisher 仍受原 inode
排他協調。

15:24 原服務完成 9,658,031 rows、636 channels、end_date=2026-10-16 的
新版 `tw-public-research-finlab-2014-v4`，固定 snapshot 為
`tw-public-research-finlab-2014-v4-20261018T071953569985458Z-l0-penguin-6c792cd7e9bf05ca`。
[原服務證據](../artifacts/operations/migration-finlab-native-bounded-proof-20261018.json)
含 staged／cold-object verification 原日誌；全部 cold objects 核驗
1,204,745,892 bytes。**官方 base cold feature parity 仍為 false**，不能宣告完整
遠端訓練依賴就緒；它與 FinLab 物件完整性及原 60-source 全檔驗收分開。

### 資源、正式採用與清理邊界

沿既有訓練重用流程，將 union 改為 bounded annual joins，市場跨年 lag 仍先在
完整小型市場序列計算，再分年 join；保留原公式、原始負數、NULL、比率、所有欄位
及重複／缺 key 驗證，失敗不覆蓋舊 accepted 輸出。新策略進 input fingerprint。
原服務完整 9,658,031-row 重建實測 **120.68 秒、process max RSS
3,095,480 KiB（約 2.95 GiB）**；同 source hash 的後續重用為 4.35 秒。
舊 unbounded 流程未完成，曾達 8.79 GB 並累積 201,904 次 memory.high 事件；
因此不能捏造完整 A/B 倍率。
[新版整條服務 counters](../artifacts/operations/migration-finlab-native-memory-counters-20261018.json)
包括 overlay／packaging／分鐘抓取，當時 memory peak 為 7,661,031,424 bytes，
`memory.events` 中 high=0、OOM=0；這和單一 union 程序 RSS 不能混用。

再以真實 Parquet 確認，2014 年 `date.dt.year()==2014` 讀取 79／79 row groups，
等價 `2014-01-01 <= date < 2015-01-01` 只讀 33／79，皆為 385,880 rows。
改成 `bounded_annual_range_join_global_market_transforms_v2`；Date 使用範圍裁剪，
timezone-aware datetime／擴展年曆保留原 year 語義，NULL 另分區。
第一次獨立全量試跑 byte-identical，但 wall=170.06s、CPU=107.83s，缺少分離的
等鎖計時，不能據此推論更快。因此補上 `lock_wait_seconds`，再完成同一來源、
同一 thread=1、同一 builder／SHA／原驗證／寫檔的
[完整 A／B／B／A](../artifacts/operations/migration-finlab-range-abba-20261018.json)。
A 只在獨立試跑 harness 還原原年度 selector；不是新增第二條正式管線。

| 全量底表流程 | A：year expression | B：date range |
| --- | ---: | ---: |
| 四輪順序中的各次 wall，秒 | 122.13、109.40 | 107.74、95.73 |
| 平均完整 wall，秒 | 115.77 | 101.74 |
| 平均 CPU，秒 | 116.56 | 101.00 |
| 兩次 process RSS 最大值，KiB | 3,158,944 | 2,740,536 |

四次等鎖均低於 0.0001 秒，全部 **9,658,031 rows 且輸出整檔 SHA 完全相同**；
本機本工作負載平均 wall 減少 **12.1%**、CPU 減少 **13.4%**、RSS 上界減少
**13.2%**。這不是整個 FinLab collector／overlay／publication 的 A/B，也不是
全庫速度、其他硬體、GPU 或投資結果。另有
[原 FinLab 服務的正式採用證據](../artifacts/operations/migration-finlab-range-native-adoption-20261018.json)：
新版完整底表耗時 100.67 秒、max RSS 2,742,444 KiB，原 overlay 重用且 stage
維持內容不變，沒有為改 selector 再存一份全量資料。
追加 FinLab driver／gate／pipeline 回歸 **117 passed**，focused lint 與 Bash
語法檢查通過；與前述 674／86 測試重疊，不相加灌大總數。

原機 ABBA／首次全量驗證的五個獨立試跑暫存已全部移除，各次約 1.06 GiB。
這是避免留下驗證副本，不是從既有來源／冷庫回收 5 倍淨空間；原 accepted
canonical 檔案與所有歷史保持不變。

15:25:51 已由原 code-only rollout 載入 worker identity
`0ff2bc128ac864ea5a45b957f5709068118ac67ba49d111d35c95af86fc3b72c`，
保留原 38 固定 release bindings、三條 workflow first/current-run identity，
PG／Temporal 未重啟。[換版收據](../artifacts/operations/migration-native-waiter-owner-rollout-20261018.json)
及[換版後三鏈完整 replay](../artifacts/operations/migration-native-waiter-post-replay-20261018/acceptance.json)
分別驗證實載程式與持久歷史相容，不替代逐庫資料驗收。
共享儲存／恢復／協調回歸 **674 passed、13 optional-dependency skips**；
FinLab 另有全表舊 relational expression、跨年 lag 與負數／NULL 等 focused tests。

重新 dry-run 的 cache 仍 **6 checked、0 evictable**；有 pin、lease 或用途未知者
保留。本節沒有再刪來源、D CAS、NAS／Restic 歷史或未知 metadata index。
前一節已回收的 11 個失敗索引、10.29 GiB 不重複算成本節新成果。
**原任務仍 active，尚未全庫完成，也未證明只剩硬體速度**：大庫 census／pack／
全部原件還原、正式來源更新窗口、相容 base cold 及 NAS／remote acceptance 都須
各自留下完成證據。原 lifecycle queue 繼續，不能用舊錯誤文字或驗收數量估成全域 ETA。

15:44:15 的[最新獨立投影](../artifacts/operations/migration-coordination-current-20261018T0744.json)
仍為 **38／60**，原 owner／lock 身分一致，FD **24／1,024**。
`tw-index-derivatives-ticks` 已由 census 轉入 routing／hashing，完整分母為
**2,471,392 files、71,179,571,001 bytes**；當時已處理 480,966 files、
16,145,101,181 bytes。這約為該 hashing 階段 bytes 的 22.7%，**不是全庫或該來源
的完整驗收率**，後續 pack commit、完整 cold reconstruction 仍必須跑完。
兩項 waiting_source_owner／十九項 waiting_retry 是原 queue 上次嘗試的持久狀態；
不能把尚未輪到重試的舊錯誤當作新程式仍失敗，也不能人工改成 verified。

## 2026-10-18 23 時：控制檔漏判、精確位元打包與實際工作時鐘

### 目前進度不是「網站有回應」或「主紀錄沒變」

23:28:26（Asia/Taipei）的[獨立觀測](../artifacts/operations/migration-fresh-work-observation-20261018T1530.json)
為 **40／60 verified**，比前節增加 `tw-index-derivatives-ticks` 及
`news-training-local`。兩者分別完成 2,471,392 原件／71,179,571,001 logical bytes
及 11,692 原件／10,272,226,685 logical bytes 的固定版本完整還原，並非僅完成發布。

當時主 transition journal 停在 18:06，但實際 OpenBB 子工作每五秒更新；
分母是 **7,476,049 原件、92,533,295,320 logical bytes**，觀測已處理
3,191,842 原件／39,899,691,937 bytes。不得因主紀錄較舊，判定這個工作死鎖。
`dataset_migration_status.py` 現在分列 transition timestamp 與綁定當次 attempt 的
工作 timestamp／stage／sample age；缺樣、時鐘錯誤及 stale sample 都不是失敗或
重啟授權。只讀既有 journal，不掃原件、不查 provider、不額外高頻重寫全庫進度。

23:36 的[換版後投影](../artifacts/operations/migration-packbuf-post-rollout-20261018T1537.json)
仍為 40／60、16 waiting_retry、3 waiting_source_owner、1 processing。
當時原 owner 已續跑 `local-tw-public`，不是 OpenBB 舊 attempt 仍執行。
中斷的 OpenBB 保留已 sealed packs；下次仍須重新 census／雜湊當前原件，不得把
舊索引或 packs 當作當前原件已驗證。公開、私有及子來源互有重疊，故不可把各庫
bytes 相加製造全域百分比或以 40／60 線性外推剩餘時間。

### 真正漏掉的兩種控制範圍

1. 既有 `taifex-rule-history` 是 `data_taifex_public_history/rules` 子來源；
   只認完整根目錄會漏掉 `normalized/*.parquet`，因此資料原子更新後仍會失敗。
2. 新聞原準備收據只固定四個根目錄 JSON，未包含 NewsHTTP 實際原子替換的
   `cooldowns/<provider>.json` 與 `robots/<origin-hash>.json`。這些名稱不符合
   status/progress 後綴，所以 `cooldowns/cmoney.json` 確實會再次觸發 source mutation。

`recursive_mutable_controls_v6` 補上精確目錄，並讓 v3／v4／v5 準備狀態透過
named delta probes 升級。沿用原始 SQLite consistent backup 及既有固定觀測，
不重建備份、不回掃幾百萬原件、不放寬所有 JSON、DuckDB 或任意原始資料變動。
來源文章／raw JSON 變動仍必須拒絕，不能為了成功而清空或替換來源值。

[原機控制範圍證據](../artifacts/operations/migration-v6-native-controls-proof-20261018.json)：
新聞新增 81 個控制檔，2.19 秒；規則子來源新增四張正常化表，1.09 秒。
兩份原準備收據及舊控制值完全保留、SQLite backup 重做次數為零，證明試跑自己的
scratch 已移除。這是升級路徑驗證，不冒充兩個來源已完成全庫遷移。

### 減少 ZIP 回補標頭造成的小寫入，保留同一 archive bytes

Python 普通 buffered stream 會因每個 ZIP member 的標頭 seek/backpatch 反覆 flush。
共用 pack writer 改用 **8 MiB 有界 seekable dirty window**，視窗內回補只改 RAM，
跨窗／大 member 保留正常 seek；輸出與舊 ZIP 每一個 byte 相同，不是先前被量測
拒絕的 forward-only 新 ZIP encoding。原 source stat/SHA、完整 CRC／SHA 還原及
fsync-before-CAS-install／head barriers 全保留。短寫、flush 失敗不能推進 accepted head。

[原機 D、同資源 ABBA](../artifacts/benchmarks/dataset_pack_buffer_20261018/native-d-abba.json)
每輪都包括首次發布、完整冷還原、durable materialization／讀回、no-op、增量三項
變更及 append、再次完整還原與舊版本完整讀回，沒有刪驗收階段來計時：

| 同範圍兩輪平均 | 舊 writer | 有界 seekable buffer |
| --- | ---: | ---: |
| 首次發布，秒 | 15.031 | 8.599 |
| 首次發布＋完整冷還原，秒 | 18.007 | 11.481 |
| **全部指定流程 wall，秒** | **53.796** | **52.907** |
| 全部流程 CPU，秒 | 25.992 | 26.368 |
| process RSS 上界，bytes | 55,896,064 | 65,669,120 |

首次發布階段約少 **42.8%**；**完整指定流程僅少 1.65%**，CPU 略增、RSS 約多
9.8 MB，且共用機器有 I/O 變動。所有 originals、inventory hash、archive encoding
及舊版本皆一致。這只支持本 fixture 的精確位元與發布熱點修復，不能外推為 748 萬
OpenBB 原件或整個遷移快 43%，更不是已達全系統硬體上限。

### 原機回歸與清理邊界

鎖定 control role 的十九組共用儲存／恢復／來源／publication 回歸為
**702 passed、13 optional-dependency skips**；另有 fintech focused tests、ruff 及
diff whitespace 檢查。control role 刻意不安裝 Polars；真正的 minute semantic
validator 由既有 `run_fintech_python` 子程序執行，identity／unchanged guard 不再
匯入 numerical runtime。不能把對 control Python 直接 `import polars` 的失敗
誤當作該橋接流程失敗，更不能為此悄悄切換整個服務的環境。

受控交接時，舊 worker 自行清掉它的 `stockagent-packed-metadata-ag6gg5rh`
disposable index（停止前觀測 index logical size 為 6,754,091,008 bytes）。這可由
來源重新 census 重建，不是 unique source、receipt 或 CAS；不是刪除冷歷史，也不
用 logical size 冒充 Windows 實體磁碟回收量。試跑自己的 D／SSD fixture 同樣在
完成讀回後移除。[清理 gate run](../artifacts/operations/agent-workflow/runs/migration-packbuf-cleanup-gates-20261018T153736-d959f268/run.log)
再次確認 **6 checked、0 would_evict**（四個 pin、兩個有效 lease）。未知出處的
三個舊 metadata index 及任何仍活躍的索引保留；沒有移除來源、D CAS、NAS 或
Restic 歷史，也沒有重複申報前輪回收的 10.29 GiB。

Binance／tw-futures 在 23:37 的最新唯讀檢查均為 publish_ready、無 active blocker；
那不是該 operation 的完整還原完成證據。FinLab 原排程 23:42 又發布固定版本
`tw-public-research-finlab-2014-v4-20261018T154221396968049Z-l0-penguin-eda35bcba5dbf8c9`，
資料截止 2026-10-16；source-expiry window 持續由原排程重驗，不能延長期限或
人工改掉失效證據來通過。期貨三個跨根介面仍受前節固定 release／subtree／SQLite
獨立還原契約保護，等待原 queue 全庫驗收後才能宣稱該來源 verified。

### 換版實載與獨立讀回，不冒充全庫完成

23:43:58 的[正式換版收據](../artifacts/operations/migration-v6-native-rollout-20261018.json)
確認 worker 實載
`5e1f6613d891c06af8b54a423791ac7577e1fd5c211393e3d05080bc244bd7b7`，
40 個 accepted release 的 snapshot／manifest binding 全數保留；PG／Temporal
未重啟。三條原 workflow 的 first/current-run chain 全部保留，
[換版後完整持久歷史 replay](../artifacts/operations/migration-v6-post-replay-20261018/acceptance.json)
皆 accepted，沒有啟動新 workflow 或手改佇列為完成。兩次 code-only handoff
都保留中斷紀錄與 sealed packs；重新 census 的交接成本不能藏在效能數字外。

23:47:32 的[FinLab 固定版本獨立完整讀回](../artifacts/operations/migration-finlab-fixed-full-recovery-20261018T1542.json)
完成 **9 files、1,210,552,448 original bytes** 的重建及完整 SHA／大小核對，
另驗 150 個 payload objects、1,204,749,653 stored bytes；含解析及驗證耗時
105.73 秒。此收據不是只驗物件 metadata，也不是 source「目前仍最新」或 remote
READY 證據。原 delivery audit 23:46:46 仍為 `base_cold_feature_match=false`、
`remote_materialization=not_verified`，不可據此宣告遠端訓練輸入全數就緒。

23:47:22 的[期貨介面重新核對](../artifacts/operations/migration-futures-source-link-fresh-20261018T1546.json)
以 64.22 秒驗完目前三個 link identity、完整目錄集合、大小／mode、
**57,232 ordinary files、6,033,838,338 logical bytes** 及固定 native SQLite
依賴，仍吻合原 `local-tw-index-futures` 固定版本。沒有把動態 SQLite main/WAL
bytes 冒充原始 consistent backup；沒有改寫 operation 或宣告整個 `local-tw-futures`
已 verified。原 queue 接續擷取、獨立還原與 compound-view 證據仍需完成。

[23:49 的配對 NAS 狀態觀測](../artifacts/operations/migration-nas-observation-20261018T1550.json)
顯示 receiver `ready`、0 pending／active jobs、無 relay errors；原 source status
最近樣本是 23:42:50，pending／retained transport bytes 為零、receipt errors 為空。
既有來源備份 cohort 涵蓋 56,556 objects／1,393,330,721,226 bytes；53 個 recovery
plans 均 accepted，但原 `full_history_backup_verified` 仍為 **false**。這不是 NAS
仍在 RetryBackoff 的證據，也絕不是全部 60 個來源或所有歷史的 NAS 還原宣告。
本節沒有向遠端發命令、沒有新建傳輸器或放寬刪除門檻。

### 23:59 全庫驗收前進，00:01 續跑原期貨來源

23:59:00 原 owner 完成 `local-taifex-public-history` 的全原件冷還原：
**153,220 files、2,921,257,784 bytes**，recovery phase 耗時 **394.59 秒**。
接續驗完同次原生 SQLite 固定備份及 scratch gate，23:59:10 才正式增加為
**41／60 verified**。不是在 pack 100% 時提前顯示完成。
[固定 attempt／manifest／原生 recovery 收據的獨立觀測](../artifacts/operations/migration-v6-taifex-native-observation-20261018.json)
保留完整綁定；本次正式版本為
`local-taifex-public-history-20261018T155214909868130Z-l0-penguin-f49f8e91cd9a-2657f05a9b311691`。

原退休流程只在上述驗收後移除 **74 個自有 scratch 檔，實際 allocated reclaim
191,897,600 bytes**，包含 native SQLite 備份副本、固定控制檔副本及已完成用途的
sealed-pack hints。原始資料、cold objects、NAS files 的刪除數都為 **0**；移除的
副本可由固定冷版本重建，原收據與回收證據保留。這是本輪另新增約 192 MB 的
有證據回收，不是前輪 10.29 GiB，也不把剛完成的冷版本刪掉。

[00:01 最新投影](../artifacts/operations/migration-v6-current-20261019T0001.json)
為 **41 verified、15 waiting_retry、3 waiting_source_owner、1 processing**；
真正 active 的 `local-tw-futures` attempt
`d6d7a11e7782418db8ebc998503ad4a9` 無當次 error，工作 sample 新鮮。
原 owner／boot／PID start 與鎖核對一致，FD 22／1,024，排程自行繼續。
另外加驗 positive partial write 的補丁／flush 行為，control role 的整個 metadata
focused suite **39 passed**；與前述 702 組回歸互有重疊，不把兩數相加成獨立測試數。

**原全庫任務仍 active，尚未結案。** 其餘十九項仍須由原 queue 實際發布／
完整還原；包括公開來源的原生鎖與來源完成門檻、期貨整庫及 compound-view、
OpenBB 大庫、FinLab 官方 base 版本對齊／遠端 READY，與 NAS 全歷史驗收。
舊錯誤不代表新版重犯，反過來，測試通過、來源可發布或單個固定版本完整讀回，
也不能替代這些尚未完成的驗收。尚未證明只剩硬體計算／傳輸限制。
