# 全資料集增量版本架構

2026-10-20 的[容量與精確回收修正](storage_capacity_recovery_2026-10-20.md)補上共同
fetch／Panel 寫入預算、完整熱冷核驗與既有編碼重用；不是另一套發布／冷庫架構，
也不表示以下待完成的來源遷移或同步已驗收。

最新固定觀測：**2026-10-21 10:58（台北）55／60 項**已完成固定 D 版本的全原檔還原。
相較前次 54 項，新增 `local-tw-public`，204,651 原檔／75,657,681,203 bytes
通過完整冷還原；另回收其已驗收的 **2,042,556,416 allocated bytes** 己方暫存，
原始來源、D 冷物件、NAS 歷史均未刪除。此前公開 `tw-futures` 與私有期貨根的
三個跨根固定依賴也已獨立驗收，不能把兩個期貨根混作同一項。
FinLab 及 OpenBB 分別因目錄清冊、運作日誌更新退回重試；舊 sealed packs 保留，
不把失敗前 83.79／106.07 GB 或 5,446,168／8,010,483 檔樣本當成現在活動。
這輪已修復持續變動的運作檔案捕捉邊界；10:41 正式服務實載新版，55 組固定版本
及三條持久鏈均保留，FinLab 已由原 owner 續跑。10:58 再次讀回核對，不把測試
通過或服務存活當作剩餘五項已完成。
此前約 **19.73 GB** 測試副本與 **4.07 GB** 臨時編碼已通過回收及刪後讀回，
是前一批，不再算作 10:58 新增回收；原值冷庫與六份小型測試證據仍保留。
此前已回收的 62,330,454,016 與 28,611,452,928 allocated bytes 是不同舊批次，
不重算為本輪新增回收；D／NAS 歷史未刪除。
最新觀測、剩餘五項、修正邊界與依工作負載分開報告的完整 ABBA 實測記錄於末節，
不因此宣稱全庫／NAS／遠端訓練已完成。
即時進度仍讀原狀態工具；來源追新、60 項遷移、NAS 全歷史驗收是不同分母。

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

## 2026-10-19：重開機續跑、原生控制範圍與拒絕未證明加速的方案

### 原佇列存活，舊錯誤與當次工作分開

本機 00:52 重開機後，原 `stockagent-storage-lifecycle.service` 自動恢復；
01:23 的[當次 attempt 投影](../artifacts/operations/migration-v7-live-progress-20261019T0125.json)
為 **41 verified、15 waiting_retry、3 waiting_source_owner、1 processing**。
當次 `bybit` 的 owner／boot／PID start／鎖均吻合，已處理
22,911,389,592／22,911,414,661 logical bytes，正在 `packing_changed_small_files`。
主 transition journal 尚停在前次來源切換，不代表這個工作未前進。
來源進度及 bytes 僅描述該 attempt 的發布工作，不能作為全域百分比。

`last_failure` 可能是 10/17 的舊錯誤；本輪確實新失敗的是私有整庫的三個精確位置：

| 來源 | 真正變動的位置 | 修正 |
| --- | --- | --- |
| `local-tw-futures` | `shioaji_history/FT1R1/download.lock` 等 contract 控制鎖內容 | 固定鎖內容觀測；絕不更換／刪除活鎖 inode |
| `local-bybit` | `1m/download_report.csv` | 只固定三種 canonical grain 的控制報告與 `.download.lock` |
| `local-tw-public` | `features/tw_public_stock_daily.parquet` | 已確認原 builder 關檔、驗證來源後才原子替換；有界串流固定這張表 |

`recursive_mutable_controls_v7` 只擴充上述命名範圍；v3–v6 舊準備狀態以 named
probes 升級，不重新遍歷數百萬原件、不重做 SQLite backup、不更新舊固定控制值。
任意原始 CSV／lock／Parquet 仍不能被套用例外。不是把下載中的資料假裝成原子整庫。

[原機獨立預檢](../artifacts/operations/migration-v7-native-controls-proof-20261019.json)
確認期貨新增 **772 個鎖觀測／89,552 bytes**，Bybit 新增 **4 個控制觀測／180,700
bytes**，TW public 新增 **1,336,536,215-byte** 官方特徵表。
全部新增 bytes 均完成 SHA 核對，舊準備收據不變，SQLite backup 重做為零；
試跑自己的 scratch 已刪除。這不是三個完整來源已通過遷移的收據。

分鐘原 collector 正常重用完整 audit，並在持有 FD 9 的同一原生 runner 裡呼叫
原 publisher 的 `--source-lock-fd 9`，依序處理 `tw-minute-source-cold`／
`tw-minute-train`。其他工作等待該 runner 是正常排他，不得拆鎖、刪鎖或繞過驗證。
FinLab 原排程在 01:07 發布新的 research 固定版本，但對應官方 base 的
`base_cold_feature_match=false` 仍是真實缺口。01:09 喚醒原 cold-publish service
得到 `canonical_refresh_lock_busy`，保留 defer；不是把缺少證據改成完成。

### Native chunk 雜湊實驗：位元正確不代表值得採用

唯讀 stack sample 找到已存在 CAS 區塊仍經 WSL 完整雜湊的熱點，因此測試原生
Windows 批次 SHA：每批最多 16 個、有 byte bound，保留完整 hash、D guard、
來源／冷檔世代 fence 與失敗保留舊 head。不是用 metadata cache 免驗。

[同資源 D 槽 ABBA](../artifacts/benchmarks/chunk_reuse_native_20261019/native-d-abba.json)
每輪含 64 MiB fixture 的既有 CAS 發布、完整冷還原、durable materialization／獨立
讀回、no-op、增量修訂、完整新舊版本還原及自有 fixture 清理。四輪原值、inventory
與歷史版本一致；沒有寫 production sources 或 production CAS。

| 平均 wall，秒 | 原方案 | 原生批次候選 |
| --- | ---: | ---: |
| 第一次發布 | 4.066 | 5.441 |
| 增量發布 | 1.680 | 1.549 |
| 兩次發布合計 | **5.746** | **6.990** |
| 含全部還原的完整流程 | 80.191 | 51.155 |

完整均值不能直接解讀成 36% 加速：第一輪原方案的增量還原耗時 70.045 秒，其他
三輪同階段僅 16.492–17.459 秒，差異在未修改的還原路徑；候選要改善的兩次發布
合計反而慢 **21.6%**。另外 Python process CPU 不包含原生子程序，不能把它變少
解讀成整機 CPU 變少。**本輪拒絕部署這個候選**，production 保留原完整 SHA 路徑。
凍結的 baseline／candidate 與收據保留在忽略的 benchmark artifacts；共同 benchmark
支援指定兩份固定 SHA 的 writer，未採用程式不必留在 production hot path。

### 驗證、換版與清理分開

修正與候選並存時的共用回歸為 **795 passed、13 skipped**，其中一個 legacy 案例
在鎖定 control role 缺 numpy，已在既有 fintech runtime 通過，未向 control role
安裝數值套件。撤掉候選後的正式採用範圍 focused suite 為 **183 passed、8 skipped**，
最終二十二組共用回歸為 **781 passed、13 skipped、1 deselected**，另維持原
SHA／CRC／SQLite／來源突變等 gate。這些測試集合互有重疊，不能相加成獨立測試數。

01:23 已排入原 `rollout_lakehouse_source_policy --reload-code-only`，等原 owner
完成當次來源的安全邊界；沒有強制打斷已雜湊 22.9 GB 的來源，也未重啟 PG／Temporal。
**排入不算實載成功**；要以新換版收據、`worker-ready` identity 及後續完整 workflow
history replay 另行驗收。
原管理入口也排入有時限的換版後只讀驗證；只在上述 accepted 收據出現後，才呼叫
原 `verify_lakehouse_workflow_history --include-dataset-versions`，不重設／新開任何
workflow。等待或排程收據不是 replay accepted 證據。

本輪託管 cleanup dry run 是 **6 checked、0 would_evict、0 evicted**；pin／lease
保護維持，來源、CAS、NAS／Restic 歷史刪除數均為零。清掉的是已核對、自有的預檢／
benchmark fixtures，不能把這些測試空間申報成歷史資料回收，也不重複加總前輪回收。
未知 provenance 的舊 metadata scratch 仍保留，不能只憑檔名與年齡刪除。

01:21 配對 NAS 狀態仍為 ready／0 pending bytes／無 receipt errors；這證明當前
transport 工作收斂，不證明 `full_history_backup_verified`，更不證明所有來源
已完成原件還原。**全庫任務保持 active；未證明僅剩硬體上限。**

01:32:45 的[原機獨立投影與只讀 stack sample](../artifacts/operations/migration-v7-final-observation-20261019.json)
仍為 **41／60**，Bybit 已從 publication 推進 `recovery`，固定 snapshot 為
`bybit-20261018T172446908213213Z-l0-penguin-09306704e4ae2cde`。
冷物件 SHA 進度 **983／2,642 objects、7,553,572,285／24,927,559,190 bytes**，
當次 sample 約五秒內；stack 也在完整物件 SHA，不是閒置卡住。此時 NAS 仍為
ready／0 pending jobs，53 recovery plans 的 `full_history_backup_verified=false`。
01:32:42 又喚醒原 `stockagent-tw-public-cold-publish.service`，本次已進入原
source-stability／derived-receipt 檢查，不再只依 01:09 的 defer 推斷後續狀態。
新發布收據尚未產生；不能提前把 official-base 缺口或遠端 READY 畫成通過。

## 2026-10-19：v8 實際換版、數百萬控制檔串流與公平排程

### 進度與真正的阻塞

[09:17 原機讀回](../artifacts/operations/migration-v8-final-observation-20261019.json)
是 **44 verified、13 waiting_retry、2 waiting_source_owner、1 processing**，
仍是原 60 項計畫。原 OpenBB attempt 在換版前已處理約 48.7／92.5 GB、
盤點 7,476,049 個檔案；重載後原 owner 取得原操作排他鎖，保存 interruption 收據，
保留其準備狀態、封閉 pack hints 與所有來源，並讓其他可做來源接續。
單一來源仍可能花數小時；「每 activity 一次嘗試」不是任意中斷期限。

原先等 publication 邊界的 v7 換版一小時後逾時，依賴它的驗證也未執行成功。
這兩份失敗證據保留，不改成成功。根因之一是舊 budget 只計成功來源，失敗鏈仍會
在同一次 activity 內持有共用操作鎖。現改成成功、失敗、busy 都計入一次嘗試；
其餘可做來源的 `next_poll_seconds=0`，全部等待時只睡到最早的有界重試期限。
舊 Temporal 結果沒有這個欄位時仍重播原 30 秒 timer，不改寫既有歷史。

[原 canonical code-only 換版](../artifacts/operations/migration-v8-native-rollout-20261019.json)
已 accepted：worker PID **2263 → 1055923**，實載 identity 為
`7f29774676c2202d77c73cf0c3062e7570c7251e91924bb62f2c1b5684383b22`。
44 個固定驗收 binding 與三條 workflow first-run chain 保留，PG／Temporal server、
下載器與交易服務未重啟。隨後的
[SDK 完整歷史重播](../artifacts/operations/migration-v8-post-replay-20261019/acceptance.json)
分別處理 2,424／216／495 個事件；沒有執行 activity、重設工作流或呼叫 provider。

### 控制檔不再另造數百萬個暫存檔

原機盤點發現 FinMind complement 有 **4,661,254** 份收據，sponsor 有 **256,524** 份；
FinLab intraday receipts 有 **379,472** 份。把新範圍全部預先凍結成小檔，會新增數百萬
次建檔／fsync，且 FinMind 舊準備收據本身已約 30.8 MB，不能反覆塞進進度文件。
因此停止只寫自有 scratch 的 eager-copy 預檢，清掉該預檢自有暫存，改沿用原 ZIP 路徑。

v8 只對 canonical FinLab／FinMind 收據樹、crypto provider/dataset states 與 TEJ
直接 progress JSON 啟用 stable-FD inline observation：完整 JSON、64 MiB 上限、
有界三次嘗試、完整 SHA 與原始 bytes 直接進 pack。原冷物件 fsync／SHA／ZIP CRC／
逐原檔獨立還原都保留；不把 raw JSON、任意 `receipts` basename 或共享來源套入例外。
觀測分屬不同時刻，不宣稱全庫原子快照。舊準備 capture 與 SQLite native backup 保留。
metadata 即使溢出到 SQLite，仍按原 census order 重建相同 portable fingerprint ABI，
並用實際封閉後的大小修正 bytes 進度，不能讓進度超過分母或令原 reader 無法還原。

超過 4,096 項的舊 capture mapping 改由 exact preparation 路徑／SHA／項數引用；
退休仍讀完整證據逐檔驗證，進度只保存摘要，不省略驗收。
[四個真實失敗檔案的原機驗證](../artifacts/operations/migration-v8-native-inline-proof-20261019.json)
通過完整 JSON／CRC／SHA 讀回，原準備日誌不變，沒有新增 loose control copies。
這不是這四個整庫已完成的證據。

### 完整流程實測、回歸與剩餘界線

[D 槽四輪 A/B/B/A](../artifacts/benchmarks/migration_v8_native_inline_abba_20261019.json)
使用 256 份生成收據，包含發布、完整冷驗證、materialization／讀回、no-op、增量、
舊版完整還原與 fixture 清理。eager-copy 設計參考平均 **25.909 秒**，inline 平均
**6.172 秒**；四輪 inventory 完全一致，inline 的額外 loose copies 為零。
參考方案的兩輪有波動；這是兩個安全設計的有界比較，不是失敗的舊 production
已完成同一工作的倍率，更不是全庫或 NAS 的加速倍數。
重現：

```bash
bash scripts/run_lakehouse_control.sh python -m scripts.benchmark_dataset_version_pipeline \
  --native-inline-controls --output artifacts/benchmarks/NEW_RESULT.json
```

二十二組共用回歸 **818 passed、13 skipped、1 deselected**；另在 fintech role
通過被 deselect 的四 bucket 全原件還原案例。最終 inline／metadata 檢查
**180 passed、8 skipped**，新增 benchmark 驗證 **10 passed**。集合重疊，不相加。
測試涵蓋突變時間點、大小變化、磁碟 spill、raw 突變拒絕、FD 關閉、超大／不完整
JSON、共享發布拒絕、準備收據遭改動時禁止退休，以及原 source lock／SQLite 邊界。

09:17 `tw-public` 與 `tw-futures` 的輕量發布條件當時可通過，但不能等同實際發布，
仍由原 owner 再取得鎖並執行稽核。期貨跨根三介面沿用
[固定 index-futures 連結驗證](../artifacts/operations/migration-futures-source-link-fresh-20261018T1546.json)，
不接受任意絕對連結；整個 futures 根還須原 queue 的完整還原。
FinLab general-history gate 當時為 **873 verified、476 stale、1 missing、0 invalid**；
原 local-refresh 正在追新，過期 release proof 不能藉延長日期變成完成。
官方 base 冷版本對齊與 remote READY 仍另驗。

配對 NAS 的來源傳輸狀態是 **0 pending bytes、0 retained transport bytes、無 receipt
errors**；這不是所有歷史都已驗收。managed cache dry run 仍 **6 checked、0 would_evict、
0 evicted**；只清自有預檢／benchmark fixtures，來源、CAS 與 NAS 歷史刪除數為零。
兩筆 transport retirement defer 另外沿 exact journal 診斷，不跳過恢復或程序引用驗證。
未知 provenance 的舊 scratch 不刪。**全庫任務保持 active；未證明只剩硬體限制。**

### v9：續跑發現的公開目錄 metadata，而非放寬所有 raw

v8 真正續跑後，`local-tw-public` 遇到新的當次錯誤：
`metadata/mof_customs_trade.json` 在打包期間被原下載器原子替換。
沿 canonical writer 查證，這是 Data.gov 官方目錄描述，不是金融數值；同目錄
`*_versions.jsonl` 是其 append-only provenance。v9 僅擴充這個來源專屬目錄：
完整 FD／JSON／SHA fence、64 MiB 上限，以及 JSONL 的完整末行／逐行解析；
不截斷、不補值、不更換任何 source lock，也不放寬 raw payload mutation。
新增 capture 在原子安裝前完成 fsync，既有 captures 與 SQLite bindings 不變。

[原機補強驗證](../artifacts/operations/migration-v9-public-metadata-native-20261019.json)
新增 **15 項／1,968,371 bytes**，保留原 16 項觀測、原 preparation 完全不變；
約 0.297 秒，沒有 source／cold object writes、沒有重做 SQLite backups，試跑自有
scratch 已移除。共用回歸為 **830 passed、13 skipped、1 deselected**，另保留前述
fintech 四 bucket 驗證。這仍不代表整個 public 根已完成。

[v9 實載收據](../artifacts/operations/migration-v9-native-rollout-20261019.json)
已 accepted，worker PID 為 **1079228**，identity 為
`4a4e5c2c1ddab1898db28b414763fb9d409e31af2d5252e5b8b76f3e0975c8bf`，
44 個固定驗收 binding 保留。
[三條最新完整歷史重播](../artifacts/operations/migration-v9-post-replay-20261019/acceptance.json)
處理 2,424／216／501 個事件並全部通過，無 activity 執行或 workflow reset。

09:23 嘗試喚醒原 public cold publisher 時，既有
`run_outside_tw_opening_resource_window --minimum-runway-minutes 180 --protected-until 13:30`
ExecCondition 正常略過，沒有繞過盤中保護。FinLab 09:23 gate 為 **1,005 verified、
344 stale、1 missing、0 invalid**；那 1 項是
`management_change_events:變更交易開始日`，其 retained raw SHA 證據有效，來源
147 列的 `raw_non_null_values=0`，但重查期限已到。這是待重查的來源全空，不是
可以補成零的可用觀測；由原 local-refresh 繼續處理。

### 09:28 最終獨立讀回與實際回收

[固定觀測收據](../artifacts/operations/migration-v9-final-observation-20261019.json)
確認原 worker PID **1079228** 仍 active，實載 code identity 與當前程式相符；
原 60 項狀態為 **44 verified、13 waiting_retry、2 waiting_source_owner、1 processing**。
處理中是 `local-tw-futures`，當次 census 仍有新進度，不能把跨根連結預檢當成整根完成。
FinLab 當時已推進 **1,121 verified、228 stale、1 missing、0 invalid**，
general-history gate 尚未 ready。來源追新與固定版本還原仍分開驗收。
最後補上的公開 metadata／JSONL 完整遷移、fetch／原件比對測試，相關三組合計
**196 passed、8 skipped**；與上述 830 組回歸重疊，不加總。

[兩筆精確 transport 回收收據](../artifacts/operations/migration-v8-deferred-transport-cleanup-20261019.json)
已 complete，原 journal 均為 `retired`。操作持有原 transport GC owner lock，
重新通過固定 D／配對 NAS 還原與程序引用檢查後，分別清除
**216,900,372／217,609,741 logical transport bytes**，合計 **434,510,113 bytes**。
這是可精確恢復的傳輸暫存副本，不是 deduplicated 實體磁碟釋放量測；
來源原件、CAS／固定 D 版本與 NAS 歷史刪除數均為零。
原 source replication 當時為 0 pending bytes／0 retained transport bytes、無 receipt errors；
仍不等於全 NAS 歷史驗收。全庫任務保持 active，未宣稱已達硬體速度上限。

### 10/19 盤後續修：49／60、v10 與來源依賴鏈

[18:01 原機觀測](../artifacts/operations/migration-v10-adopted-observation-20261019.json)
為 **49 verified、9 waiting_retry、1 waiting_source_owner、1 processing**。
下列為前次 44 項後，原 queue 新增的固定版本完整還原；不是所有來源歷史已追至今日。

| 來源 | 原檔數 | 邏輯 bytes |
| --- | ---: | ---: |
| `local-tw-futures` | 1,995,553 | 21,229,494,659 |
| `local-bybit` | 4,765 | 22,910,647,570 |
| `tw-minute-source-cold` | 564,735 | 6,084,251,258 |
| `crypto-reference` | 58,174 | 543,470,633 |
| `tej-research` | 1,050,816 | 3,907,437,533 |

**期貨跨根已不只是預檢。** 原 operation 的
`recovery/local-tw-futures.json` 已驗完整 1,995,553 原檔，
`local-tw-futures.source-links.json` 的 `all_fixed_dependencies_recovered=true`；
`margin_sources`／`margin_repair_pending`／`margin_repair_inputs` 分別還原
55,853／1,208／171 檔，綁定原 index-futures 固定 snapshot／manifest 與 SQLite 備份。
沒有將任意絕對連結搬到訓練機。仍在 queue 的公開 `tw-futures` 是不同項，不混算完成。

**v10 修正的是已查證的原子控制檔，不是放寬 raw。** FinLab 的 `raw_receipts/*.json`、
FinMind root／complement／sponsor 的 `json_pipeline_last.json`、OpenBB `_state` 的
`public_priority_cursors.json`／`public_priority_queue_event.json`／`public_priority_plan.json`
均經原 producer 寫入路徑核對。保留 bounded stable FD、完整 JSON／SHA、既有固定 captures
及完整原件還原；未宣告的 raw／`_state` 仍拒絕途中變更。inline selection marker 更新為
`named_stable_fd_json_v2`，v9 preparation 只做具名 delta probes，不重掃整樹或重做 SQLite backup。
自有 scratch 的 hash、程序引用、unlink 與退休結果另報在原 attempt 進度，不把 object 100%
冒充已完成退休。相關回歸 **836 passed、13 skipped、1 deselected**；該四 bucket 測試另在
fintech role 通過。與 focused **229 passed、8 skipped** 有重疊，不加總成覆蓋數。

[v10 正式交接](../artifacts/operations/migration-v10-authorized-native-rollout-20261019.json)
已 accepted：worker PID **2060898**，載入 identity
`bcb6f570d30d95961cbbc16329c37660f103e4719fe784c842eeed5fea5c6dd5`。
原自然交接等待超過 600 秒後，依使用者既有授權走 code-only restart；保留原逾時記錄、
49 個完整 fixed bindings、所有 preparation／中斷日誌，不刪 source lock。
PG／Temporal server 未重啟，三條原 workflow chain 保留。
[換版後完整 history replay](../artifacts/operations/migration-v10-post-replay-20261019/acceptance.json)
驗 132／308／561 個事件；沒有執行 activity 或 reset 工作鏈。不同 run 的事件數不可直接
當總歷史進度。新 owner 已接手 `openbb-task-shards-local`，具當次持續 census 觀測。

**FinLab 少完成證據的另一根因是依賴順序，不是缺值就補零。** 17:49／17:52 的原服務
各重建到 2025 年後才在 2026 年遇到 `official table has keys absent from the research universe`；
每輪約 104–120 秒、記憶體峰值 3.5 GB，接著又重試。現沿原 wide → TAIFEX → union
依序更新：wide 使用 canonical EX lease，overlay 使用 SH lease，來源 busy 時不繞過。
union 在 feature decode／write 前先驗所有年度 keys，仍拒絕重複／缺 key 並保留舊成功輸出；
`build_strategy=bounded_annual_range_join_preflight_keys_v3` 綁定完整 source／output SHA。
原一般目錄在 17:58 為 **1350／1350 verified、0 missing／stale／invalid**；其中明確來源
全空項仍保留 NULL 證據。該 gate 不驗分鐘／tick，也不能延長到期時間充當 fresh。
18:02:05 原機成功產出 **9,660,541 列、204 public features**，union 完整 build 61.657 秒，
peak annual partition 540,330 列，輸出 SHA
`404db2d6e3c97fd33791aad9aa2a58dcc84ffebc498c59e29f10840c03c070ca`。
這是一次成功建置，不是與舊失敗迴圈公平 ABBA 的加速倍率，也不代表後續 overlay／stage／cold
均已完成。FinLab 相關鎖定、依賴失敗、精確值與鍵檢查回歸 **38 passed**。

**真正回收與沒有回收的東西分開。** 17:48:01 原 owner 在 TEJ 全原件還原後，依完整
preparation／SHA／程序引用 gate 退休 136 個自有暫存項，實測回收 **5,627,162,624 allocated bytes**；
原收據為 operation 的 `scratch-retirement/tej-research.json`。這不是前述 434,510,113 logical
transport bytes 的重複加總。新的 managed cache dry run 是 **6 checked、0 would_evict**，
保留原 pin／lease；來源、cold objects、NAS 歷史刪除數皆為零。未知 provenance 暫存不刪。

剩下 11 項仍由原 queue 管理：Binance／OKX／分鐘訓練等候原 collector locks；
FinLab／FinMind／OpenBB 控制檔與 public metadata 待具名修正後的實際重試還原；
分鐘／大型 OpenBB 分片／公開期貨保留原中斷／準備 journal；FinLab 研究 release 待真正完成
overlay／fresh catalog／stage 證據。舊 error 欄保留前次失敗，不冒充新版已再次失敗。

[只讀 I/O 候選測試](../artifacts/benchmarks/migration_v10_native_chunk_read_screen_20261019.json)
固定 TEJ 8 MiB object，完整 SHA／size fence、portable/native/native/portable：portable
約 0.654／0.359 秒，native 約 2.268／2.115 秒；未採用較慢路徑，也未以此宣稱整段硬體上限。
18:00 左右原 source replication 為 0 pending／retained transport bytes、無 receipt errors；
配對 NAS 的 58 個 recovery plans 為 accepted，但 `full_history_backup_verified=false`。
**遷移、來源追新、完整 NAS 歷史與遠端 runtime readiness 仍分開。全庫任務維持 active。**

### 18:06–18:09 獨立讀回與研究封裝的真實邊界

[當輪部分驗收收據](../artifacts/operations/migration-v10-partial-acceptance-20261019.json)
再次確認實載 identity、原 49 個固定 bindings、期貨三介面及 TEJ 退休證據，
狀態明列 `accepted_repairs_migration_incomplete`，沒有重跑全庫 payload 來冒充新驗收。
[18:05 原進度](../artifacts/operations/migration-v10-final-observation-20261019.json)
仍是 49／60；OpenBB 分片 census 已到 1,470,760 檔、20,953,353,757 logical bytes，
總量尚未封存，不能從前次清冊猜一個精確百分比。原服務繼續執行，不因耗時而再重啟。

18:06:14 FinLab overlay 亦實際完成 **9,660,541 列、104 特徵、16 個來源**，
輸出 SHA `ae121b042e05a7fe07e259175e19f7cdbea267d0513061b8c38ff13f028848fb`。
另一次持有原 SH lease 的完整 key anti-join 確認 **missing_rows=0**；這只證明來源鍵包含，
不是所有 feature 數值齊全或歷史 PIT。首次完整依賴鏈／overlay 的服務耗時約 8 分 18 秒、
記憶體峰值 8 GB，不能以單一 union 61.657 秒代表整條鏈路。

18:06:16 stage 正確拒絕 **0 missing、40 stale、0 invalid**：來源時鐘在首次重建期間到期，
不是來源壞值。已喚醒原 FinLab refresh，原紀錄 18:07:52 已處理 8 項、剩 32 項，
0 provider errors／timeouts，持續追新；相同 inputs 可由原 builder 的完整 SHA gate 重用，
不偽造新的日期或略過 fresh catalog／hash 驗證。這仍未算研究 release 發布完成。

18:07:06 原 public cold publisher 回 `deferred: active_registered_writer`；核對到原 event
monitor 的 `finalize_tw_public_completed_session.py` 及其真正持有 EX lease 的
`build_tw_public_training_features.py`。未繞過來源 writer，未以 systemd exit 0 當成發布成功。
後續沿原 publisher／timer 在一致來源完成後重試，不新增競爭 writer。

### 23:22：完成發布與成功同步分開，磁碟阻塞不再連帶空轉

[23:22 原機觀測](../artifacts/operations/migration-outbox-final-observation-20261019.json)
仍為 **49 verified、9 waiting_retry、1 recovery、1 waiting_source_owner**。
OpenBB 的 **7,476,049 檔、92,533,295,320 logical bytes** 已完成封裝；當次固定版本冷物件
核驗到 **262／366、56,579,844,721／95,179,137,144 bytes**，約 59.4%。
這只是 `verify-cold-objects` 階段，後續仍有完整原檔重建與自有 scratch 退休；不是整批 59.4%
或已完成，未因耗時而重啟原 owner。私有期貨根與三個跨根固定依賴的既有驗收保留；
公開 `tw-futures` 尚未完成，不能和私有期貨混算。

**發布通知不是資料交易本身。** `publish_data_releases.py` 的單來源 deferred scan 現允許
publisher 自行取得原 catalog／native owner，仍必須真的持有至少一個來源鎖、綁定唯一
commit receipt，完整來源與 freshness/hash gate 不變。FinLab 原排程提交固定版本後，
把通知留給既有 durable outbox／scan retry timer，不再因網路掃描 500 把研究建置與分鐘排程
一併中止；沒有新增同步服務或放寬資料授權。
23:00:49 的 `artifacts/live/finlab/training_delivery.json` 已記錄固定 research release
`tw-public-research-finlab-2014-v4-20261019T145139739121034Z-l0-penguin-2876f80b681a15c8`，
**cold_objects_verified=true、1,129,389,186 bytes、9,660,541 列、636 model channels**。
原服務 23:01 正常結束。但 `base_cold_feature_match=false`、`remote_materialization=not_verified`；
這不是原始 FinLab 全歷史、NAS 交付或遠端可訓練的證明。

**正常來源競爭與真錯誤分開。** 原 EX／SH producer busy 現沿 typed exception 回傳 75；
FinLab 只延期研究建置／stage，不發布未完成版本，仍讓獨立分鐘佇列執行。來源 hash、鍵、
schema、權限及未知錯誤照常失敗。23:09 原服務已實際由 busy 進入分鐘工作，再正確回報
`disk_reserve_reached`，不是卡死或假完成。

**新根因是系統碟容量，不是 API 配額。** ext4 約剩 7 GiB，Syncthing 回覆
`insufficient space on disk for database`（低於原 1% 門檻）；原分鐘 guard 為 **25 GiB**。
現在 HTTP 500 只輸出 bounded/recognized 原因 `database_disk_reserve`，不洩漏回應內的
私有路徑或認證內容，並關閉錯誤 response FD、保留所有 pending intents。
FinLab 在一般資料與研究分支之後、SDK pool／分區掃描之前，先以同一份 25 GiB 常數檢查空間。
若已有明確磁碟阻塞，而且原研究 writer lease 也真的忙碌，既有每分鐘 ExecCondition 只做
當前 statvfs/flock 判斷便略過；一般追新、gap request、08:00 重置及不確定證據仍走原 recovery。
不延長固定冷卻時間；任一資源恢復，下一分鐘重新判斷。

相近的「0 一般鍵到期、public busy」原機空轉，23:09 為 **21.007 秒 wall／19.869 秒 CPU／
319.3 MiB peak**；23:14 磁碟前置後為 **8.371／7.587 秒／100.2 MiB**。
23:18 起雙重阻塞在入口略過，23:21 為 **0.989 秒 wall／1.019 秒 CPU／60.7 MiB**，
沒有啟動資料下載程序。這是線上狀態觀察，不是公平 ABBA、完整下載加速倍率或硬體上限證明。

**清理沒有越過恢復證據。**
[精確離線清冊](../artifacts/operations/migration-offline-space-inventory-20261019T1514.json)
找到 `openbb_full_source_contract_index_20261014`：17 檔、**23,537,680,384 allocated bytes**，
完成收據與消費者預檢通過，但還沒取得該案例的完整冷回復／同步刪除前置。
既有 C archive stage 另需 source size + 32 GiB，當前容量也不足，不能先複製到滿再說。
另一個約 18.4 GiB 的 native-unassigned 案例缺受支援的 producer 完成收據，保留。
compiler cache dry run 為 **0 eligible**；未知 provenance 的 metadata 暫存與仍開啟的 OpenBB
驗證 index 不刪。本輪新增刪除量為 **0**，前輪 TEJ 的 5,627,162,624 bytes 不重複計算。
已停止本輪自有、耗時的全 repo `du` 只讀盤點；用精確案例清冊接手，未中斷下載或原驗證。

驗證：FinLab／公開研究／發布及通知原路徑完整 focused 集合 **346 passed**；
共用儲存 **839 passed、13 skipped、1 deselected**；發布/outbox/載入閉包收尾
**257 passed**；資源 admission 與 FinLab 原路徑 **173 passed**，集合有重疊，不能相加。
[三條現存完整 workflow history 重播](../artifacts/operations/migration-outbox-pre-reload-replay-20261019/acceptance.json)
通過 **1656／627／561 事件**，沒有執行 activity 或重設工作鏈。
`syncthing_scan.py` 已納入 worker code identity；
`migration-outbox-safe-reload-20261019T151955-13a1718b` 沿原交接工具等待安全 owner boundary，
未使用 active-owner restart。實載仍是 v10，不能把 source tree candidate 當成已載入新版。
配對 NAS 收據仍停在 10:46 UTC、`full_history_backup_verified=false`；需先解除容量問題，
再核對交付與全歷史驗收。全庫任務保持 active，未宣稱只剩硬體速度或已全部完成。
[本輪部分驗收與精確收據 SHA](../artifacts/operations/migration-outbox-partial-acceptance-20261019.json)
另列候選／實載版本、清理零新增量、測試範圍與尚未成立的驗收；不是全庫完成宣告。

## 2026-10-20：52／60、持續變動控制檔與完整區塊驗證

08:57 的[原 owner 狀態讀回](../artifacts/operations/migration-status-20261020T0900.json)
為 **52 verified、6 waiting_retry、1 recovery、1 waiting_source_owner**。新增完成項的
固定 recovery receipts 亦逐一獨立讀回，不以 service active 代替：

| 固定來源 | 完整重建原檔數 | 完整原檔 bytes | 已核驗 cold object bytes |
| --- | ---: | ---: | ---: |
| `openbb-task-shards-local` | 7,476,049 | 92,533,295,320 | 95,179,137,144 |
| `local-tw-minute` | 572,951 | 20,182,707,190 | 19,907,030,382 |
| `okx` | 2,050 | 37,977,834,544 | 37,708,276,445 |

這些分母不同且有來源覆蓋重疊，不把 bytes 相加稱為全庫大小。私有 `local-tw-futures`
的 **1,995,553 原檔、21,229,494,659 bytes** 及 `margin_sources`、
`margin_repair_pending`、`margin_repair_inputs` 三個固定跨根依賴已完成驗收，
`unsafe_absolute_links_materialized=false`。公開 `tw-futures` 尚未完成，沒有混算。

### 精確修復與避免多餘工作

- **公開來源追加式日誌。** 來源 producer 在 `state/journals/*.jsonl` 追加完整 JSON
  加換行並 fsync；長批次期間路徑自然會更新，不能當成永久不可變原件。v11 只在
  已知 `data_tw_public` 的這個直接目錄捕捉完整 stable-FD 觀測；64 MiB 上限、
  全檔 JSONL 解析及最後換行皆須成立。部分列仍拒絕，不截斷、不補值、不宣稱全來源
  同時點快照；金融 payload 的變動偵測不變。
- **OpenBB compactor 原子收據。** 納入 `_state/l1_compaction_attempt_latest.json`
  與 `l1_compaction_latest.json`，沿用原 producer 的固定控制檔捕捉。自 v10 升級時
  只探測新增的明確路徑，保留既有 SQLite backup／原收據，不再掃幾百萬 raw shards。
- **完整 SHA 不需要重讀相同區塊兩遍。** `reconstruct_all=True` 對僅作 chunk 的
  immutable object，將區塊 SHA／長度與原檔 SHA 同一次串流核驗；同時作 legacy
  direct blob 的 object 仍保留原直接驗證。每個 logical extent 仍完整讀取、每個原檔
  核整檔 SHA，最後核唯一 object 數、bytes、前後檔案身分；缺 callback、損毀、增長、
  截短、錯序或驗後變動均失敗。ZIP SHA／CRC 與新舊版本還原沒有移除。
- **進度不先報完成。** 延後合併驗證的 object 不計入已核驗 bytes；只有完整 extent
  驗證後才累加，全部原檔與最後身分檢查通過才送出 object 100%。實際恢復收據另記
  `full_chunk_sha_and_original_sha_single_read_v1` 及省掉的重複冷讀 bytes，不把它當成
  API 抓完、NAS 完成或實測速度倍率。

### 證據、交接與清理邊界

原生 owner 仍在驗證 Binance 固定版本，沒有因這輪修改而中斷。前次交接
`migration-outbox-safe-reload-20261019T151955-13a1718b` 因等待時候選碼變動而拒絕，
原 worker 繼續工作；新程式測試通過、排隊與真正載入仍分開記錄。

[本輪三條完整持續工作流歷史重播](../artifacts/operations/migration-v11-full-history-replay-20261020/acceptance.json)
在 09:06 通過 **1156／654／5 事件**；只重播各現存 pinned run 的完整 history，
不是累計所有 Continue-as-New 歷史，未執行 activity、provider call、reset 或服務控制。
初次共用測試在精簡 SDK 環境因 archive 依賴 NumPy 而 collection failed；archive／
consumer 測試改用原 Fintech runtime，沒有往精簡常駐環境裝進訓練套件。

精簡 SDK 共用回歸為 **993 passed、13 skipped、1 deselected（66.66 秒）**；
這個唯一 deselection 是 archive 環境案例，另與 consumer／retirement 集合移至
Fintech 執行，而非刪掉驗證。最後 benchmark 原件完整性／模式測試 **13 passed**。
focused 25 項與這些集合有重疊，不相加成測試總數。
Fintech 的完整封存／consumer／retirement／cache-capacity 集合已在 09:35 結束，
**179 passed（1657.95 秒）**。這包含前述唯一 archive 環境案例；此集合真的核對
程序引用與原冷庫還原，未因耗時而移除安全驗證。

[34,136,441 bytes 的兩版真實 FinLab Parquet 完整 ABBA](../artifacts/benchmarks/full_chunk_recovery_abba_20261020.json)
保留輸入副本核 SHA、baseline、incremental、semantic no-op、全原檔核驗、durable
新舊版本解封與讀回、fixture 清理。參考／新方案的完整 inventory SHA 亦完全一致。
完整 cold 核驗均值 **0.18485 → 0.10880 秒**，但完整流程均值
**3.60853 → 3.66059 秒（0.9858 倍）**，**沒有證明整體加速**。
兩個 warm SSD 小檔案例中的每次完整核驗，確實分別省去 17,070,154／17,066,287 bytes
的重複冷讀；不能把這個階段改善外推為生產 D／NAS 或全來源的加速倍率。

候選 worker identity 固定為
`90209da1cf4760d3d78dc7db3b1908dab20c033a2e97744ed7ff23812a6f0dc9`。
`migration-v11-safe-handoff-20261020T011119-85f9d2af` 已沿原工具排隊等待 owner 邊界，
未使用 `--restart-active-owner`。真正驗收必須讀回
`artifacts/operations/migration-v11-safe-handoff-20261020.json` 的 accepted、實載 SHA／PID、
既有 fixed-release bindings 與三條 first-run 工作鏈；排隊不是載入完成。

兩份舊分鐘衍生根及編碼暫存的 **62,330,454,016 allocated bytes** 精確回收，見
[容量維護紀錄](storage_capacity_recovery_2026-10-20.md)。這是先前 continuation 已完成的
累計結果，不當成這輪新增刪除量；D／NAS 歷史未刪除。
另對已完成的單一 OpenBB benchmark 案例重新清點：17 檔、
**23,537,680,384 allocated bytes**，producer 完成收據與 consumer 預檢通過。
這只授予進入原冷封存／全還原／最終引用、pin、transport、quarantine gates 的資格，
不是已可直接刪除；沒有完成收據的另一案例與 provenance 不明暫存保留。
新案例沿原 authority wrapper 以固定清冊在 09:10 進入低優先序封存，原操作收據位於
`artifacts/operations/migration-v11-offline-retirement-20261020/`；在 `acceptance.json`
與 stage-pruned／刪後讀回成立前，新增回收量仍記為零。
本次 17 個原檔 **23,536,261,280 bytes** 已編成 **5,073,657,131 bytes**，
其中大型 SQLite 原件採既有 adaptive zstd-1；未換算法、丟欄位或修改原值。
09:19 的固定 D 版本
`legacy-wsl-offline-bbe60254dd68fec65226e-20261020T011934302852990Z-l0-penguin-00e5a71d86002f8b`，
manifest SHA `63b5cc446653bdd715cdd790a0e345f48d1d2a0d259632194db27f5952c87f4d`，
09:22 已進入獨立 D 恢復核驗；這仍不是原檔已刪除。
09:25 獨立 D 解碼／原檔 SHA 核驗通過 **17／17、23,536,261,280 bytes**。
09:30 原來源已通過當前引用、冷庫、同步、quarantine 與刪後讀回 gates，
`retirement.json` 的 `deleted=true`；精確來源目錄已不存在，實際新增回收
**23,537,680,384 allocated bytes**。6 份小型 benchmark 證據保留，固定 D 原件可恢復。
編碼暫存仍在原 `prune-verified-encoding-stage` 流程，暫不把它估算為額外已回收量；
`backup_verified=false` 亦保留，沒有把本機 D 完整恢復冒稱為 NAS 全歷史備份。
09:33 原封存／退休作業已完成，09:35 另以獨立 readback 確認原目錄與 encoding stage
均不存在、6 份／36,487 bytes 小型證據未變，固定 D manifest／物件身分仍相符。
本輪**實際新增回收 28,611,452,928 allocated bytes（28.61 GB／26.65 GiB）**，
其中 source 23,537,680,384、stage 5,073,772,544；D object／NAS snapshot 刪除均為 0。
完成收據：
`artifacts/operations/migration-v11-offline-retirement-20261020/legacy-wsl-offline-bbe60254dd68fec65226e7da/acceptance.json`；
獨立驗收 run：`migration-v11-cleanup-readback-20261020T013515-0b9044d5`。

09:12 本機 Syncthing 回到 idle／本機檢查通過，但配對 NAS 09:11 的 relay 仍有
5 筆 `transport_incomplete`，要求 `wait_for_Syncthing`；這不是已證明物件毀損，不能
強制改成 ACK 或反覆建立相同 redelivery。當前 NAS 去重覆蓋 **93.33%**、仍缺
約 **103.49 GB**，15 批待驗收；本機 idle 不能取代接收端完整檔案／NAS 獨立還原證據。
09:35 的接收端新收據仍為 degraded，但 `transport_incomplete` 已由 5 筆降至 4 筆。
來源端最新完整批次收據停在 09:17，覆蓋 1,448,412,432,691／1,549,747,082,053 bytes，
不是把它回填成 09:35 的即時全庫驗收。

09:35 [再次讀回原 owner](../artifacts/operations/migration-status-20261020T013535Z.json)
仍是 **52／60**，FD 為 24／1024、原程序身分與 lock 持有者一致。
Binance 當下是 cold-object SHA 階段 **102,505,866,383／142,188,772,591 bytes（72.09%）**；
後面仍有全原檔重建，不能把這一階段百分比說成全來源或全庫完成進度。
kernel lock 讀回確認交接 `flock` PID 3936505 在等原 worker PID 2060898；
真正 loaded SHA 仍是 v10，候選 v11 尚未載入，未為了更新顯示而中止原恢復。

所有驗收仍以原操作 journal、固定 recovery receipt 與新增實測收據為準；
目前不宣稱全 60 項、NAS 全歷史、遠端可訓練或「只剩硬體瓶頸」已達成。
[本輪分界清楚的部分驗收與收據 SHA](../artifacts/operations/migration-v11-partial-acceptance-20261020.json)
另記候選／實載、重播、完整流程實測及尚未完成的項目；不是全庫完成收據。

### 11:50 後續：原生二進位串流批次化，完整 D 流程加速

[11:49:56 的狀態讀回](../artifacts/operations/migration-status-20261020T0350Z.json)
仍為 **52／60**。`py-spy` 定點讀取確認 Binance 原 owner 正在全原檔重建，
並非等待死鎖；11:52:50 已核對 **23,511,768,502／143,926,679,414 bytes**。
前面的 cold-object 全 SHA 已結束，但不是原檔或全庫完成。**原生** chunk reader
每個 8 MiB extent 都各自啟動 PowerShell 並重跑掛載檢查，是可消除的軟體開銷；
後續接線核對發現 migration 原本僅啟用 native pack、不啟用 native blob，因此
不能從原 stack 的 `_iter_chunked_file` 一行推斷生產正在使用逐塊 PowerShell。

既有 `windows_cold_io` 與共用 chunk reader 現改為最多 16 個有序 extent 一次
binary stream，不另開服務或下載管線。單 member 8 GiB／batch 16 GiB 上限，
原生端逐 member 檢查精確長度；父程序保留逐 chunk 及整檔 SHA。重複 extent
仍逐次讀取。每個 batch 前後重驗實際 D authority 與每個檔案身分，沒有 TTL 掛載
快取；全部通過後才計入 extent 完成。部分失敗不重播串流，僅空輸出的已知 WSL
startup timeout 有界重試。portable／單 member reader 維持原精確 EOF 檢查。

[完整 D 槽 ABBA 收據](../artifacts/benchmarks/native_stream_complete_abba_20261020.json)
使用前述兩版真實 FinLab Parquet，在既有 worker 相同 quota／affinity／priority 的
獨立 D fixture 執行。每次均包含輸入副本 SHA、baseline、incremental、no-op、
全原檔核驗、durable 新版及舊版還原、獨立原檔讀回、fixture 清理。四次的完整
inventory SHA 相同，讀取 member 都是 **24 個**，Windows 讀取程序 **24 → 8**。

| 完整流程方案 | ABBA 兩次秒數 | 平均秒數 |
| --- | --- | ---: |
| 原逐 extent 原生 reader | 97.04350、72.16195 | 84.60272 |
| 有界原生批次 stream | 41.93318、49.98180 | 45.95749 |

完整流程均值 **1.8409 倍**，約省 **45.68% wall time**。這不是先前 warm SSD
融合核 SHA 的 0.9858 倍結果，兩者不得混用。此測試仍與生產 I/O 共用 D 槽，
不能外推為所有資料量／NAS 速度、全歷史完成時間或硬體極限。
尤其這是 **native 逐塊 → native batch** 的比較，不是既有 portable migration
的加速證明。另用兩版約 83 MB 真實 Parquet 對照實際 portable 路徑的完整 ABBA；
通過且確有改善前不更改遷移的 blob reader 選擇。

本次精簡 SDK 共用回歸 **1053 passed、13 skipped、1 deselected（67.50 秒）**，
唯一 archive 環境案例已在前述 Fintech 179 項集合通過，本次沒有冒稱重跑該集合。
針對性 **144 passed（12.78 秒）** 與共用集合重疊，不相加。
[三條現存 pinned run 的完整歷史重播](../artifacts/operations/native-stream-history-replay-20261020/acceptance.json)
通過 **3107／137／5 事件**；沒有 activity／provider call／reset。

舊 `migration-v11-safe-handoff-20261020T011119-85f9d2af` 仍只等待鎖，候選碼已
被本次修正取代，11:51:57 只取消這個工程等待 run；原 worker PID 2060898
繼續 active，未刪原鎖或資料。完整 ABBA 通過後，依先前明確換版／重排授權，
沿原 `rollout_lakehouse_source_policy.py --reload-code-only --restart-active-owner`
保存 52 個 fixed-release bindings 與 Binance inflight intent，再有界交接。
最終須讀回 [交接收據](../artifacts/operations/native-stream-authorized-handoff-20261020.json)
以及 journal owner 的實際接手，不能把啟動換版當成完成。
11:55 交接已 accepted，PID **2060898 → 106389**，實載
`a40901a9abbc6fb227d9fbfc933d827c9b3b13a68b4a52883f2de9c6abbab5a1`；
52 個原固定版本與三條 first-run 鏈完整保留，PG／Temporal 沒有重啟。
舊服務日誌是 graceful shutdown，非強制 SIGKILL。Binance journal owner
11:55:07 記錄原 attempt `0ffa7906168f4b62b7eee86ff0906c9b` 中斷，沿同一固定
snapshot 重排為 `5ff5cb276b8f470e9e1958b93916cb30`，沒有手改狀態或重抓 API。
[載入後完整歷史重播](../artifacts/operations/native-stream-loaded-history-replay-20261020/acceptance.json)
再通過 **3219／137／5 事件**。這次先載入 v11 控制檔及合併 SHA；原生 blob
批次仍須上述 portable 比較，不能以載入共用函式當作該分支已在生產啟用。

11:58 定點 stack 又證實，原恢復每次依序做兩次 resolver 的全物件 presence，
再由 full verifier 做第三次。候選修正只在即將做完整核驗時省去 resolver 重複
stat；manifest SHA／schema 仍核對，full verifier 的完整 presence／size／SHA
不變。沿用既有成功 receipt 的路徑仍必須重新檢查物件 presence；缺 object 的
cached receipt 仍拒絕。這是減少重複 metadata 工作，不降低資料驗證。
新修正的針對性集合為 **321 passed（29.75 秒）**；精簡 SDK 共用集合再跑為
**1056 passed、13 skipped、1 deselected（83.53 秒）**，archive 環境案例仍沿
前述 Fintech 專屬集合，不混報新增執行。新測試用真的固定 release 核對完整重建
只做一次 presence，沿用 receipt 仍重查，並刪除隔離 fixture 的物件驗證拒絕；
沒有刪除任何生產 cold object。

[較大真實 Parquet 的 portable／native 完整 ABBA](../artifacts/benchmarks/portable_vs_native_stream_large_abba_20261020.json)
在 12:03 完成。兩版輸入合計 **167,345,228 bytes**；完整流程平均
**portable 95.23792 → native batch 82.74688 秒**，速率約 **1.1510 倍**、
wall time 減少 **13.12%**，新舊版全原檔及所有 inventory SHA 相同。
這才是對實際原 chunk reader 的對比；仍是共享 D 槽的固定 fixture，不能
宣稱整個 migration／NAS 的實際提速或硬體極限。

依這個範圍採用獨立、明確的 `d_primary_native_chunk_reads` 接線：只在 canonical
公開 D root 的 **至少 64 MiB** 原檔啟用 native batch；小檔保留 portable，私有根
保留原 reader；不改舊的 native pack/blob 選項含義。64 MiB 是本次大型檔工作集的
保守啟用門檻，不是各種硬體的普遍最優值。逐 chunk、整檔 SHA、每批前後 authority
及 signature 全保留，進度另記實際 `native_chunk_batch_reads`，以便驗證接線
是否真的執行；只是函式存在或 worker ready 不算該分支運行證據。

最終接線、scope／型別拒絕、大／小檔分流、metadata 去重與既有儲存共同契約
回歸為 **1061 passed、13 skipped、1 deselected（77.21 秒）**。候選碼固定
`bdfc9ce9bf165ca58a5417dc7691d984caf06a007ebde7817a70e4a503bc5ec3`；
[此候選完整歷史重播](../artifacts/operations/native-chunk-wiring-history-20261020/acceptance.json)
通過 **3406／137／5 事件**。這些 counts 不與前面的重疊測試相加。
12:06 沿相同已授權的有界 code-only 路徑交接，最終以
[新交接收據](../artifacts/operations/native-chunk-wiring-handoff-20261020.json)、
原 journal 的 interruption／new attempt、同 snapshot 與實際 progress 欄位分別證明。

12:06:55 這次交接已 **accepted**，PID **106389 → 134355**，candidate 與
loaded SHA 均為上述 `bdfc9ce9…`。52 個固定版本、三條原 run／first-run 鏈及私有
設定 SHA 均保留，PG／Temporal 未重啟。12:07:14 原 journal 自行記錄前次 attempt
`5ff5cb276b8f470e9e1958b93916cb30` 中斷，重排為
`5e86cfe5aaeb4042a731d9f2ae80c365`；Binance snapshot 仍為
`binance-20261019T222646248705359Z-l0-penguin-ad445d2d0057a9a1`，沒有重新呼叫來源 API。
[12:08 載入後完整歷史重播](../artifacts/operations/native-chunk-final-loaded-history-20261020/acceptance.json)
通過 **3463／137／5 事件**，核對當前 PID／實載碼；沒有執行 activity、reset、
provider call 或服務控制。

[12:08:28 的獨立狀態讀回](../artifacts/operations/migration-status-final-native-20261020.json)
仍為 **52／60**，原 operation owner 的 PID／鎖身分相符，FD **84／1024**。
Binance 是同 fixed snapshot 的新 attempt，當時在唯一完整 presence／size pass，
尚無新版原生 chunk 的實際讀取樣本；不能沿用換版前的 3.94 GB 作新版進度。
12:10:05 同 attempt 的 cold-object 進度已為 **3／17,352、204,451,044／
142,188,772,591 bytes**；定點 stack 也已由 presence／signature 清點前進到
`_native_pack_source` 讀取。因此不是僅有 ready／heartbeat，但這個 pack 分支仍
不是新增 chunk-batch 分支的執行證據，也不是全部 143.93 GB 原檔已完成。
其餘七項仍循原工作鏈重試，不能把載入修正或歷史重播當成它們已完成：

| 尚未取得本輪完成證據的來源 | 原 journal 狀態／剩餘驗收 |
| --- | --- |
| `finlab-research`、`finmind-free` | 舊 attempt 的變動控制檔錯誤；v11 已載入，仍待原排程重試並完成固定還原 |
| `local-openbb`、`local-tw-public` | 舊 attempt 的 compactor receipt／追加日誌變動；修正已載入，尚未重試完成 |
| 公開 `tw-futures` | `waiting_retry`；不可沿用私有期貨的完成證據 |
| `tw-minute-train` | 舊 attempt 等 `shioaji_minute_full/runner.lock` 逾時，須等真正 owner 邊界，不刪鎖 |
| `tw-public-research-finlab-2014-v4` | `waiting_source_owner`，尚無本輪來源完成證據 |

11:50 NAS relay 有 5 批 `transport_incomplete`；本機唯讀 API 核對接收端仍連線、
未 pause、remote state valid，但 completion **54.7351%**，還缺 **32,435,865,163
bytes／7,039 items**，另有 4,207 個待同步刪除。這是傳輸仍在進行，不強發 ACK、
不反覆重建相同批次。私有期貨三組 fixed dependencies 再讀回仍全部完整，
公開 `tw-futures` 仍要自己的完成證據。
12:08:46 再次唯讀取樣的 paired peer completion 為 **56.3786%**，
`remoteState=valid`，尚缺 **30,333,359,591 bytes／6,829 items**，另有
4,467 個待同步刪除。這是當次傳輸佇列，不是 NAS 全歷史接受率；工作集合會變動，
不能用兩次樣本的百分比直接外推全庫 ETA 或把同步刪除算成已清理 cold history。

先前 **28,611,452,928 allocated bytes** 清理的 exact cold recovery／刪後證據保留，
本段不重算為新增回收。未取得 producer 完成與獨立還原／引用 gates 的來源、
未知暫存、D objects 與 NAS 歷史均未刪除。

## 2026-10-21 00:01：54／60、v12 候選與精確測試副本回收

[本輪狀態讀回](../artifacts/operations/migration-status-20261020T1602Z.json)
為 54 verified、4 waiting_retry、1 processing、1 waiting_source_owner；這是
60 個遷移處理項的分母，不是所有金融觀測筆數、原下載器歷史覆蓋或 NAS 全庫比例。
原 `stockagent-storage-lifecycle.service` 的 PID 134355／鎖身分一致，
當次 FD 為 93／1024，OpenBB 進度距取樣不到三秒，沒有停住或換另一個 owner 重做。

本輪新增兩份獨立完整原檔還原證據；兩份 `materialized_verified` 都仍為 false，
不能把可恢復性當作已永久展開或遠端訓練就緒：

| 固定來源 | 原檔數 | 原檔 bytes | 完成時間（台北） |
| --- | ---: | ---: | --- |
| `binance` | 2,392 | 143,926,679,414 | 2026-10-20 14:18 |
| 公開 `tw-futures` | 1,929,933 | 16,180,735,682 | 2026-10-20 19:42 |

原證據位於原 operation 的 `recovery/binance.json`、`recovery/tw-futures.json`；
公開期貨的還原進度也留下 `native_chunk_batch_reads: true` 的實際執行證據。
私有 `local-tw-futures.source-links.json` 再讀回仍為
`all_fixed_dependencies_recovered: true`、`source_files_deleted: 0`、
`unsafe_absolute_links_materialized: false`，其三個固定依賴不是仍待解決的跨根連結。

### 剩餘工作：按根因處理，不降低完成判準

| 來源 | 本次觀測／下一個原工作鏈動作 |
| --- | --- |
| `finlab-research` | `runs/latest.json` 在打包時被原 writer 原子更新；v12 對這個已核對的控制檔留固定觀測，待安全換版後原 owner 重試 |
| `finmind-free` | 舊中斷 attempt 已由原 owner 對帳為待重試；沒有新 error 不等於已完成 |
| `local-openbb` | 原封裝仍前進，已重用 1,252,994 個原檔的 sealed-pack hints；不為載入新碼犧牲已花時間的八百萬檔清點 |
| `local-tw-public` | 已核對原 builder 原子產生的 summary／research companion 在打包時變動；v12 精確捕捉，原始資料與同名 lookalike 仍嚴格核對 |
| `tw-minute-train` | 原分鐘資料 owner 的協調鎖曾逾時；繼續原鎖協議，不能刪鎖或繞過活躍 writer |
| `tw-public-research-finlab-2014-v4` | 來源未提供當前 `catalog_latest_verified`／`source_hashes_verified` 完成證據；須由 FinLab 原 producer 完成後重建研究 overlay，不能補寫 true 或回溯日期 |

00:07 以原 `scripts/finlab_release_gate.py` 做不讀 payload 的即時診斷：目錄仍新鮮，
一般歷史 1,350 項中 verified 1,142、stale 207、missing 1、invalid 0，
另兩項是原分鐘／tick 分區 capability。故 `ready: false` 有當前來源依據，
不是只有 9 月舊 receipt 缺布林欄位；此次 physical_files_hashed／hash_bytes_read
都是 0，沒有為查狀態多讀整批原件或呼叫 API。

v12 的準備檔升級僅探測具名控制檔，沿用舊 SQLite backup／已捕捉原件，
不為補這幾個控制檔重新遍歷全部來源。每個原子檔有自己的觀測時鐘，
不聲稱 Parquet 與 summary 是跨檔同時提交，更不將這種獨立觀測當訓練發布。
[候選歷史重播](../artifacts/operations/migration-v12-history-replay-20261020/acceptance.json)
已通過 1511／11／29 個事件，沒有執行 activity、reset、API 呼叫或服務控制。
候選身分 `723da1c349aa3fc96d0bc3c4c74ffabdf0d53cc377a5393ac0ced3aa44734c07`
尚未取代實載 v11 身分 `bdfc9ce9bf165ca58a5417dc7691d984caf06a007ebde7817a70e4a503bc5ec3`；
安全換版正在等待原活動邊界，不能把排隊當載入成功。

### 減少浪費的修正與實測邊界

* 共用來源讀取以 `O_NOFOLLOW`／`O_NONBLOCK` 開 FD，在建立 stream 前用
  `fstat` 拒絕非普通檔與來源簽章變動，結束保留路徑核對；FIFO／目錄／symlink
  不會成為阻塞或無聲換源。這是健壯性修正，不是已證實吞吐加速。
* 回收前的 process／argv 路徑解析改為**單次掃描內有上限的共用結果**；離開掃描
  前重查重用名稱。測試中 100 個重複 argv 的解析由 200 次降為 4 次；外部 alias
  指向待刪來源仍阻擋，未知權限仍保守延期。無關 alias 的變動不再誤擋整個回收，
  沒有跨輪 TTL，也沒有用字串前綴排除真正引用。
* 已完成 OpenBB 測試的 fixture／comparison 本來使用不同 ABI，現按原 producer
  的精確對應驗證四組 ABBA sidecar、完整有序來源 SHA、零來源 API／零來源寫入。
  不再因要求兩個不同角色的 contract 字串相同而永久擋住已完成副本的回收；
  缺少或篡改任一證據仍拒絕，原六份小型 JSON 留作刪後獨立讀回。

[4,096 檔完整版本生命週期 ABBA](../artifacts/benchmarks/packed_descriptor_complete_abba_v2_20261020.json)
包含 baseline／incremental／no-op、舊新版本完整原檔恢復、durable materialization
及 fixture 清理，不只計讀取 loop。retained 平均 116.205 秒，FD 候選 120.098 秒，
含子程序完整牆鐘分別為 121.347／125.493 秒，**沒有證明變快**；因此不宣稱已到
硬體極限。保留 FD 改動的理由是拒絕非普通檔與避免阻塞，不是拿小段 syscall 減少
冒充完整鏈路吞吐量提升。

[固定工程觀測](../artifacts/operations/migration-v12-engineering-acceptance-20261021.json)
保留原失敗及對應修正後驗收：來源／packed／版本生命週期組 474 passed、8 skipped，
共用 lifecycle／Windows I/O／同步／還原組 243 passed，Fintech fixture 組
26 passed；路徑引用與 D 容量／scope 的 focused 測試另有 22／12 passed。
各組有重疊，不把它們加總為獨立測試數。00:18 最終六檔 storage regression 已通過
**300 tests／1335.12 秒**；舊 CLI 選項相容性另獨立通過 1 test／1.06 秒。
不把此前不同環境缺少 NumPy／Polars／tqdm 的 collection error 當成來源損壞，
也不在最小 lakehouse SDK 隨意安裝一套訓練依賴來掩蓋執行環境選錯。

回歸定點 stack 另發現 `test_legacy_archive_stage_prune.py` 的每個 unit case
都會走正式 `/srv/stockagent-packed-materialized` 的遞迴 pin 掃描。測試 fixture
現改成在自己的臨時 materialization 根呼叫**原 pin reader**，不是固定回傳空集合。
新增 C／具名 D 兩種 scope 的真實 pin 檔測試：有效 pin 阻擋、損壞 pin 保守拒絕、
刪掉測試 pin 後才可規劃；正式服務的 pin／process／generation gate 沒有改動。
這是隔離單元測試與線上資料的額外成本，不拿它當正式回收吞吐的 ABBA 證據。
00:25 這組完整 stage-prune 回歸為 **55 passed／829.23 秒**，包含新增真實 pin
與既有 C／D、引用、冷物件、隔離改名後變動等判準。

00:27 再修正回收進度的巢狀階段：內層 publication owner 結束後，外層仍會
核對原檔，原先卻只留下子階段 `completed`。新進度契約
`manual-retirement-phase-progress-v2` 保存 `phase_stack`／`phase_depth`，子階段
結束恢復**同一個進程中、同一個目標**真正活躍的父階段及原開始時鐘，保留
`last_subphase` 結果；不從磁碟上舊的 running 紀錄復活父階段，也不跨目標恢復。
失敗傳播、被父階段處理的失敗、跨目標與舊狀態等 **10 tests passed／26.32 秒**。
本輪已在執行的手動回收進程先於此修正載入，仍用 v1；沒有為換顯示重啟重算。
因此當下子階段 completed 不能替代 `progress.json` 的 `all_selected_retired`
及最終 acceptance，v2 從後續原入口執行生效，不是新 timer／新回收管線。

### 回收目前進行中；先解決工作區位置，不降低容量或恢復要求

精確目標僅 `artifacts/benchmarks/running_services_20261013/`
`openbb_native_unassigned_batch_20261014` 的 10 個已完成測試檔，
logical bytes 19,732,024,819、allocated bytes 19,733,061,632。
C 碟無法同時容納原檔大小與 32 GiB 預留，原回收正確拒絕，沒有先刪來源騰空間。
現由原 inventory 凍結 `completed-benchmark-d-scratch-v1`，僅這一個已完成
benchmark 使用 `/srv/stockagent-d-volume/stockagent-completed-benchmark-scratch`；
仍經原 archive → D immutable publication → independent original reconstruction
→ process／pin／lease／transport／generation → quarantine → unlink → stage prune。
普通 D legacy staging、CAS objects 與 NAS 歷史不在新可清理範圍。

[回收進度](../artifacts/operations/migration-v12-selector-d-retirement-20261020/progress.json)
當次仍為 prepare-exact-source-archive，`all_selected_retired: false`、實際回收 0。
這份當次觀測不預支刪除成功；原六份 JSON 已保留，約 19.7 GB payload 尚未刪除。
00:08 已前進至 publish-to-D，00:15 定點 stack 在發布前獨立解碼核對原 SHA，
不是 owner 死鎖或來源 API 重試。D 編碼檔的逐次完整 SHA／解壓仍走檔案讀取，
不以「在算」推論已達 SSD 極限；後續原生 I/O／單次讀取融合仍需完整對照實測，
不能直接省掉獨立還原或把已算過的 hash 無條件跨輪當成新證據。
00:19 主回歸完成後，原回收仍在 `publish-under-shared-owner`，沒有宣稱已 unlink。
[00:19 最新狀態](../artifacts/operations/migration-status-20261020T1620Z.json)仍為
54／60、原 owner 鎖／PID 身分一致、FD 30／1024；同一個 OpenBB attempt 已前進到
4,237,757／8,009,414 檔、61,222,288,023／110,138,611,533 bytes、phase 24／55。
[00:28 再讀回](../artifacts/operations/migration-status-20261020T1629Z.json)仍為 54／60，
同一個 attempt 已到 4,414,676 檔／63,666,418,968 bytes、phase 26／55，
FD 27／1024、進度距取樣約一秒；未用無變動 heartbeat 充作封裝前進。
同輪預設 14 日 cache dry run 為 inventory complete、eligible 0、deleted 0、errors 空；
不為清理量而放寬預設保留期或把未知 `/var/tmp`、活躍索引當垃圾。

15:37 UTC 的原 source-replication 狀態為 monitoring_increments、pending_bytes 0、
receipt_errors 空；lab203 relay 5 批 ready／accepted，明示 acceptance reused、
NAS payload reverification false。這表示當次 transport 批次已接受，不是全 NAS 歷史
重新驗證，不強發 ACK，也不因一個 ready 刪除 cold history。

## 2026-10-21 02:05：重啟接手、完成回收與單次輸入驗證

[獨立讀回](../artifacts/operations/migration-v12-postboot-and-retirement-readback-20261021.json)
核對原 plan `c84f1b36…`、54 組先前已完成的 snapshot／manifest 身分、當前 owner
PID／boot 身分與實載 SHA `723da1c3…`。WSL 在 01:20 與 01:28 重啟，舊手動
handoff 還在等 owner，沒有換版完成收據；新 boot 的正式 service 自動載入相同
v12。原 OpenBB／FinMind interrupted attempt 由原 owner 留下 interruption receipt
並排回原佇列，沒有刪鎖、reset workflow 或開第二個 publisher。

[重啟後完整持久歷史重播](../artifacts/operations/migration-v12-postboot-history-20261021/acceptance.json)
通過 **1733／33／29 個事件**；三條 `first_run_id` 與重啟前相同，零 activity
執行／reset／provider call／service control。這證明目前持久工作鏈和候選相容，
不是所有歷史 continuation、實際完成全部來源或全機冷開機業務驗收。

### 已完成的精確回收

原 [acceptance](../artifacts/operations/migration-v12-selector-d-retirement-20261020/legacy-wsl-offline-94ab61dff3e5e5d8760c455a/acceptance.json)
已核對 `all_selected_retired: true`、來源及臨時 encoding stage 均不在原位置、
保留六份 JSON 的完整 SHA 集合與固定 D manifest／當前 object presence 一致。
此次新增實際 allocated 回收是 **19,733,061,632 bytes（來源）**及
**4,066,828,288 bytes（D 臨時編碼）**；兩個不同 volume 分列，不相加冒充 C 空間。
原完整冷重建及解碼證據是 `cold_verified: true`、`decoded_originals_verified: true`、
10 原檔／19,732,024,819 original bytes。本輪刪後讀回沒有再掃一遍 19.7 GB payload，
不把 presence 核對描述成又一次完整 SHA 還原。

可恢復的固定版本為
`legacy-wsl-offline-94ab61dff3e5e5d8760c4-20261020T162437764330905Z-l0-penguin-c61ab7640d233447`，
manifest SHA 為 `b0b8137b27873adc562c0859cc898c83f6f7195517f1eea1453b974eda5ffac4`。
原 catalog、retirement、stage-pruned、完整 recovery 及保留證據都在同一 receipt
目錄；D cold objects 與 NAS snapshots 刪除數均為 **0**。

### 本輪根因與修正

先前 legacy verifier 先讀整個壓縮檔驗 encoded SHA，再重讀一次解碼驗原值；
raw 格式則對完全相同 bytes 做兩次 SHA。共同入口現融合成一次有界輸入讀取，
同時保留 encoded／original 的完整 SHA 及長度。gzip 多 member、zstd 多 frame／
完整 decoder exit status／輸入 thread 錯誤都須通過；FD 使用 no-follow／nonblocking，
讀完核對原 FD 與原路徑 generation，未知 FIFO／symlink／目錄不會卡在讀取。
zstd 只加一個 bounded feeder，使輸入寫 pipe 與輸出讀取同時前進，不先塞滿 pipe
造成死鎖；失敗會終止 decoder、收回 feeder 後才關原 FD。

此融合**沒有共用跨階段證據**。發布前／後來源比較、canonical cold object／ZIP
完整核驗、獨立冷庫原值還原與真正 retirement／stage prune 都保持原流程。
`scripts/benchmark_legacy_archive_workflow.py` 沿用原真實資料完整發布／冷解碼／
restore／原 SHA・mode・mtime 讀回與私人 scratch 引用檢查；新增兩讀／單讀 ABBA，
可把當次私人 encoding 放 D 模擬這次大檔回收，並記包含 D scratch 回收的總時間。
不清 OS cache、不碰權威 head／既有來源、不省解碼或為速度刪除驗證。

### 仍未完成的來源與排程

* `finlab-research`：原 owner 正處理；02:05 為 962,568 檔、106.07 GB 分母，
  本階段已處理 37.15 GB。這是 source packing，還有小檔封裝及完整冷還原。
* `finmind-free`／`local-openbb`：boot 中斷後由原 owner 排回佇列；sealed packs
  留存可重用，仍需重新核對當前 source bytes，不沿用舊進度宣稱完成。
* `local-tw-public`：v12 已載入具名原子控制檔修正，仍等原 owner 實際重試驗收。
* `tw-minute-train`：本輪已看不到該 lock 的 holder；舊 timeout 記錄保留，
  由原佇列重新取得鎖，沒有刪鎖或繞過協調。
* `tw-public-research-finlab-2014-v4`：一般目錄仍是 verified 1142、stale 207、
  missing 1。stale 中 204 個最近嘗試回報 `vip_only`，3 個 `resource_deferred`；
  唯一 missing 是保留的 source-empty 觀察到期，不能補零或補寫完成旗標。
  當前官方 quota snapshot 為剩餘 0／5000 MB，不能推定是永久無權限或假裝已追新；
  待原配額重置／來源檢查成功，才由原 builder 重建及全 SHA 封閉 overlay。

02:05 source replication 的 pending bytes 為 0、receipt errors 空，paired relay
為 ready；僅限該次封閉 transport 批次，不表示所有 NAS 歷史已重新完整還原。
目前仍有可避免 I/O、首輪大清冊與來源 quota／freshness 邊界，**尚未完成全部遷移，
也未證實只剩硬體上限**。以下是本輪最終對照，不用單純 syscall 數減少代替
完整流程加速。

### 02:27 完整實測及工程封閉

[最終獨立讀回](../artifacts/operations/migration-v13-single-read-final-readback-20261021.json)
分別綁定兩組原始 benchmark、當前遷移觀測、先前完整退休／刪後證據、測試及
NAS 新批次。這個 `v13` 是獨立 archive 驗證優化的工程標籤，**不是**新 migration
worker 版本；原 worker 仍實載 `723da1c3…`，沒有為此中斷正在前進的 FinLab。

第一性原理的下界是：完整驗兩個 hash 至少要看過每個 encoded byte，並完整
解碼原值。舊驗證在同一交易內讀兩次 encoded bytes；新驗證一次串流取得
`H(encoded)`、`H(decoded)` 與兩個長度。複雜度仍是 **O(encoded + original bytes)**，
不是變成常數時間；記憶體有界於 block／pipe。raw 的兩個 hash 相同可只算一次。
不同發布／冷恢復交易仍各自讀回，不能把跨輪 cache 當成當前獨立驗證。

兩組都使用真實來源、每組四輪 **A–B–B–A**，測完整 publish → independent cold
decode → restore → 原 SHA／size／mode／mtime → 己方 D 暫存回收。來源保持不變，
八輪均通過；只刪當輪私人測試 fixture。沒有清 OS cache、停其他服務、修改
權威 head，或聲稱私人 D/9p 測試路徑等同 production canonical native reader。

| 真實來源工作負載 | 完整耗時中位數（含 D 暫存回收） | 完整 CPU 中位數（含 child） | 可成立的結論 |
| --- | --- | --- | --- |
| 139.31 MB Feather → 約 8.57 MB zstd | 17.42 → 14.26 秒 | 5.92 → 5.74 秒 | 此樣本耗時縮短 18.13%、CPU 少 3.09% |
| 486.62 MB 兩個 Parquet，adaptive 選 raw | 325.17 → 325.97 秒 | 42.80 → 39.88 秒 | 耗時持平（+0.25%），CPU 少 6.82%；**未證實大檔整鏈加速** |

原始資料：[壓縮工作負載](../artifacts/benchmarks/legacy_single_read_zstd_complete_abba_20261021.json)、
[原值大檔工作負載](../artifacts/benchmarks/legacy_single_read_complete_abba_20261021.json)。
這是共享 WSL/ext4、D/9p 現場的有界量測，兩個樣本不是全系統最優值或統計顯著性證明。
原值大檔的發布階段中位數少 10%，但 cold fetch／restore 波動抵銷，不能只挑該階段
或第一組快樣本宣稱總流程加速。

較廣 archive／retirement／stage-prune／maintenance／fixture 回歸為 **335 passed
（470.62 秒）**；最終 archive 與完整比較兩檔為 **91 passed（31.35 秒）**，另有
最終串流邊界 **28 passed（2.08 秒）**。這些測試重疊，不加總為獨立測試數。
途中兩次工程失敗各有
[根因及後續驗收處置](../artifacts/operations/migration-v13-engineering-repair-dispositions-20261021.json)：
大型 pytest 參數造成環境長度 E2BIG，改用有界 ID／函式內 payload；D benchmark
錯用 native mutable-state admission，改為保留 C 原 gate、D 按 mount guard 和真實
容量預算。失敗 log／run 保留，沒有把原失敗改寫為成功。

02:27 最新同一 `finlab-research` attempt 已核對 **104,756 檔**、路由 **946,920 檔**、
處理 **52.86／106.07 GB**；仍是封裝階段，之後還要全原檔冷還原。原 54 組
snapshot／manifest 都再次核對未回退，owner lock／process 身分一致，FD 為
**27／1024**。其餘五項仍按前節依賴續跑，不取消來源鎖或 freshness gate。
一般 FinLab quota 重置後由原排程重新確認 stale／empty 觀察，再讓原 builder
產生完整、可驗 hash 的 overlay，不能填旗標代替取得資料。

02:17 新封閉的 NAS wave `30d9466e…` 為 7 個 payload／1,052,963 bytes；先前等待
的是原 Syncthing scan，不是 checksum 錯誤。paired ACK 的 02:21:55 獨立還原已驗
全部 9 檔（含 manifest／READY）、1,056,162 bytes，source owner 再驗身分通過。
02:26 狀態為 monitoring increments、pending bytes 0、receipt errors 空；最新 relay
沿用已驗收收據，不是這次狀態讀取又做一次 NAS payload 全重驗。catalog semantic
restore 仍分開，不能將此 wave 當成全 NAS 歷史驗收。

[本輪 managed-cache dry run](../artifacts/operations/migration-v13-managed-cache-dryrun-20261021.json)
是 **5 checked、0 would-evict、0 evicted**，全部有 pin 或有效 lease。除了上節已經
完整還原驗收並刪後核對的 19.73 GB 來源副本與 4.07 GB D encoding，本輪沒有再
刪冷庫、NAS snapshot、受引用視圖、未知 provenance 暫存或仍在使用的索引。

下一個可量測的效能方向，是原值大檔 full cold fetch／restore 的 D I/O，而非再省
驗證。當前 public native chunk 與 owner-private scope 不同；若擴充私有 batch
reader，須先補 authority／0700／marker／ignored-export／每批 generation guard，
再做完整新舊版本恢復 ABBA、工作歷史 replay 和安全交接。不在 productive attempt
中途直接打開未驗證旗標，也不以「硬體上限」掩蓋未處理的軟體 I/O。

## 2026-10-21 10:58：v14 運作檔案捕捉、實載及真實來源讀回

[本輪固定驗收](../artifacts/operations/migration-v14-final-readback-20261021.json)
及[完整狀態](../artifacts/operations/migration-v14-final-status-20261021.json)仍綁定
原 task `all-dataset-incremental-layout-20261016`、同一 plan／operation；沒有新建
publisher、私有匯出管線或第二個遷移 owner。整體任務仍是 **active，55／60**。

### 根因、計畫與實作

長跑失敗不是 API 沒有下載到資料，而是把會持續更新的運作檔案誤當成整輪
不變的 payload。FinLab 上一輪在 83.79／106.07 GB 後遇到
`catalog/discovery.json` 更新；OpenBB 在 5,446,168／8,010,483 檔時遇到
`logs/supervisor.log` 變更。它們因此拒絕發布是正確的保護，不能關閉原 fence。
只修這兩個 basename 還會輪到相同 producer 的其他 monitor／catalog／rotation 檔。

因此先沿原 writer 盤點整個類別，再用原 frozen-control／packed 路徑修正：

1. control scope 升為 `recursive_mutable_controls_v13`（本節工程標籤是 v14，
   不是前節 archive single-read v13）。FinLab 加入精確的 `catalog/discovery.json`；
   OpenBB 加入已核實 scheduler／monitor／audit／supervisor epoch 控制與目錄輸出。
2. 四個具名 supervisor logs 及兩個監控 JSONL 以有界串流捕捉完整檔案，
   不再因大於 64 MiB 被當成普通長跑 payload。一般拷貝為 8 MiB block；JSONL
   逐列驗證、保留最後換行及 64 MiB 每列上限。拷貝中增長／截斷會先拒絕／重試，
   不裁切半列、不接受 append prefix、不修改原日誌。
3. v12 preparation 只探查具名路徑；保留先前觀測、SQLite 原生備份與 sealed packs，
   不重走八百萬 raw 路徑來找幾個新控制檔，也不重備份既有 DB。普通 raw／
   receipt_history／相似目錄、未知變動 payload 仍拒絕。no-follow/nonblocking
   regular-file open 也避免 lstat 後被換成 FIFO 而卡住。
4. 只省略原 writer 未提交的精確 rotation／epoch／catalog siblings，manifest
   明列 `openbb_operational_atomic_sibling_v1`；不是通用 `.tmp` filter。正式
   `.log`／`.log.1` 與原始資料仍收錄。私有 0700／marker／不匯出政策不變。
5. 完整語義測試、ABBA、原持久歷史 replay 通過後，沿既有 code-only 交接入口
   安裝；最後獨立讀回原 55 組固定身分、實際 owner、當前冷物件存在性及證據。

### 正式交接與真實來源驗證

[正式交接](../artifacts/operations/migration-v14-code-handoff-20261021.json)在 10:41
通過。沿先前「直接換新版」授權使用原 code-only/restart-active-owner 路徑，先
封閉 intent、保留舊 attempt／staging／55 組已驗收身分，再由原 journal 對帳及
重排；沒有刪鎖或 reset workflow。實載 SHA `c6f29612…`，PID `2117 → 1202070`，
PG／Temporal server 與私有設定 SHA 不變。FinMind 當時未封閉的 census 重排，
不得再當成現在仍在工作；其已封閉 packs／六個原生 DB 固定版本仍保留。

[換版後完整持久歷史 replay](../artifacts/operations/migration-v14-post-handoff-history-20261021/acceptance.json)
通過 3241／293／47 個事件，三條 first-run ID 不變，零 activity／reset／來源
呼叫。10:58 讀回實際 loaded code 等於候選，原 55 組 snapshot／manifest 未回退。

另外用已知**真實來源控制檔**做只讀捕捉 → 隔離私域封裝 → 完整冷原值還原 →
再次 fetch／SHA → 己方副本 reference gate／回收：

| 來源與範圍 | 原檔數／bytes | 結果 |
| --- | --- | --- |
| [OpenBB 運作檔案](../artifacts/benchmarks/migration_v14_live_openbb_controls_20261021.json) | 35／774,293,464 | 全原檔讀回通過；原 logs 與 production cold 未改動 |
| [FinLab 目錄／run head](../artifacts/benchmarks/migration_v14_live_finlab_controls_20261021.json) | 2／69,946 | 全原檔讀回通過；原目錄未改動 |

測試只用 `/var/tmp` 己方私域，不把它當成 D/NAS 全庫驗收。首次工具因缺少
private-history marker 被原 guard 正確拒絕，保留固定副本後沿同一 intent 續跑，
沒有繞過保護或重新抓 API；已驗收的真實副本與兩份失敗 unit fixture 都安全清理。
[工程失敗與修復處置](../artifacts/operations/migration-v14-engineering-repair-dispositions-20261021.json)
綁定七個原 failed run 及後續成功驗收，沒有刪除或改寫原失敗證據。

### 完整流程效率與驗證範圍

固定 2048 個 raw 檔、具名 log／完整 JSONL／真正 Parquet 三個運作輸出，每輪包含
原 preparation 升級、實際變動觸發、發布／必要重試、完整原值還原及己方 capture
回收。兩組 A–B–B–A 的原 inventory SHA 完全一致，原方案每輪 2 次、新方案 1 次；
sealed pack 已可重用，pack member writes 都是 2051，**不是 payload 寫入減半**。

| 有界本機 SSD 工作負載 | 完整平均耗時 | 完整平均 CPU |
| --- | --- | --- |
| [第一次 ABBA](../artifacts/benchmarks/migration_v14_operational_controls_abba_20261021.json) | 13.37 → 11.97 秒（少 10.5%） | 5.25 → 4.28 秒（少 18.3%） |
| [最終程式 ABBA](../artifacts/benchmarks/migration_v14_operational_controls_abba_final_20261021.json) | 23.94 → 10.01 秒（少 58.2%） | 6.83 → 4.94 秒（少 27.6%） |

此時同機仍有其他工作；第二組 reference 尤其波動，不能把 58.2% 當成全庫速度
或統計顯著的因果倍數。兩次都能重現的是：消除由該類檔案異動造成的整輪失敗，
原值完全一致、少一個重試，以及這些樣本 CPU 較少。不是 D／NAS 量測，不省
任一原 SHA／CRC／冷恢復驗證。完整驗證目前仍有 O(檔數 + bytes) 下界，
**尚未證實已經只剩硬體限制**。

控制角色完整儲存／生命週期回歸為 **722 passed、8 skipped（86.21 秒）**；
8 項是該角色未裝的實驗性 fastcdc，已在一般角色 **422 passed（35.13 秒）**
中全部涵蓋。測試互相重疊，不加總為獨立通過數，不為測試改動已鎖依賴。

### 現在剩下什麼，以及實際清理

* `finlab-research`：原 owner 活動中。新 attempt 的 census 是 963,599 檔／
  109,731,427,349 bytes，10:56 樣本核對／路由 5,327,474,382 bytes；23 份
  sealed-pack hints 納入 182,208 檔，missing/rejected 皆 0。這是來源核對，
  不是 API 抓取百分比或已冷還原百分比；新下載也使分母較舊 106.07 GB 增加。
* `local-openbb`：類別修正已實載且真實 35 檔完整還原通過；正式全根仍待原
  佇列重試，舊 5.4M 檔不是 active。不是另起一個全根 publisher。
* `finmind-free`：交接後由原佇列保留 interruption 證據及 packs 待續跑；舊
  1.51M census 不能當目前 active 或完成數。
* `tw-minute-train`：前次 coordination timeout 保留；本輪查不到該 lock holder，
  仍須原 owner 成功重取鎖及完整發布驗收，未刪 lock、未繞過來源協調。
* `tw-public-research-finlab-2014-v4`：一般目錄已 **1350 verified，stale／missing／
  invalid 皆 0**，包含 1 個有原證據的 source-empty。這個便宜檢查沒有重讀全部
  payload hashes，不代表已產生最新 overlay；原 builder／全 SHA gate 仍要完成。

新增完成的 `local-tw-public` 是 204,651 檔／75.66 GB 全原檔冷還原；固定版本
`local-tw-public-20261020T193905610461929Z-l0-penguin-f49f8e91cd9a-a7d993906b2ea83b`、
manifest `5381454e8f4f89c4750eb13624581edf023847d992da1c0fb1d1ce33726d867e`。
原完整 recovery 與 10:58 當前 D presence 再核對分開；這次狀態檢查沒有再重讀
75.66 GB，也沒有冒稱又做一次完整 SHA。其 107 個已驗收己方暫存共回收
**2,042,556,416 allocated bytes**；原來源／D cold／NAS 刪除皆 0。

10:37 managed-cache dry run 檢查 5 項，0 可回收、0 刪除（pin 或 lease 保護）。
此前 19.73 GB source／4.07 GB encoding 是前批，不重新算作新增空間。這輪測試
副本回收是測試生命週期，不灌入正式資料集的 source-reclaim 數。

NAS source projection 的 10:45 樣本仍有 12 批／126,825,139 bytes 待 ACK，
receipt errors 與 catalog refresh error 空；10:57 只讀 Syncthing 為 ingress
`scanning`、return `syncing`（尚需 23,049 bytes／10 items），本機 errors 皆 0。
不能把 need=0／沒有錯誤當已送達，更不能拿既有 NAS bytes 當所有歷史獨立恢復。
後續由既有 sender／paired ACK 收斂；全部來源完成、NAS 全歷史驗收與硬體極限
證明都仍明列 **false**，不因這輪工程修復成功而結束整體任務。
