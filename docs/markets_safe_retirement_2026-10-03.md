# Markets 完整冷封存與安全回收 — 2026-10-03

這是 penguin 的操作紀錄，不是訓練成果／期貨規則完整性宣告。
現行正確性邊界見 [儲存契約](agents/storage.md)；清理前容量盤點見
[Markets 儲存審查](markets_storage_review_2026-10-03.md)。

## 授權範圍及進度

使用者要求四個不在當前服務使用的研究目錄先完整保存在 D 冷庫，再立即回收
原目錄與已退役的 mirror，不等七天；不可把只收小檔的舊冷版視為完整備份。

| 原目錄（相對 `artifacts/`） | 完整封存 dataset | 清理前 allocated GB | 狀態 |
| --- | --- | ---: | --- |
| `markets/tw_public_lantent` | `legacy-markets-tw-public-lantent` | 10.623 | 完整冷 head 已提交，回收待驗收 |
| `markets/forex` | `legacy-markets-forex` | 4.262 | 11:58 完整驗收；原檔已回收 4.261622 GB，可從 D 恢復 |
| `markets/tw_public_candles_multi_basis_online_complete_lookback32_v2` | `legacy-markets-online-lookback32-v2` | 3.814 | 完整冷 head 已提交，回收待驗收 |
| `markets/tw_public_candles_multi_basis_online_complete_feature_input_lookback32_v4` | `legacy-markets-feature-input-lookback32-v4-full` | 3.702 | 完整冷／源本比對通過，排程回收中 |

上表是原檔 allocated bytes 除以 10⁹ 的盤點值，不是已回收量。
實際回收量以每項 `acceptance.json` 的
`original_reclaimed_allocated_file_bytes` 為準，不另加 mirror 的 hard-link 名稱，
也不把本次建立再清除的壓縮工作暫存重複算入原空間節省。

`forex` 的 [完整驗收收據](../artifacts/operations/market_safe_retirement_20261003/legacy-markets-forex/acceptance.json)
確認回收 **4,261,621,760 allocated file bytes**：source／mirror／C 編碼暫存
均不存在、842 個原檔共 4,259,813,286 logical bytes 已獨立從 D 重建核 SHA，
11:58 的 local transport proof 全部通過。編碼工作暫存另回收
3,866,501,120 bytes，但不加到原空間節省；本次未刪 D 冷 objects。

期貨規則的既有工作在 12:24 完成 116 個舊目錄回收。唯一維護目錄是
`markets/tw_futures_v8_margin_preparation/all_products_rule_facts`；另保留隱藏
`.rule-facts-baseline` 與 v122 相容 symlink 供 SHA-bound 既有快取使用，不在
該基準上繼續修補。回收期間 WSL 可用空間從 136,683,139,072 增至
392,750,714,880 bytes，增加 **256.068 GB**；因有其它並行寫入，這是容量
觀測，不是精確原檔刪除量，不與四根 allocated-byte 收據相加。
[期貨回收收據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/rule_parser_cleanup_acceptance.json)
綁定正式歷史冷版、52 份補充冷版、當前 manifest 及原基準 SHA。

四項一律是 `legacy-quarantine-archive`：保留原 bytes、路徑、mode、mtime 和
manifest，標示 `deployable=false`、`completion_claim=not_checked`。
保存舊研究成果不代表允許它成為現行模型或 audited provider release。

`lookback32_v2` 的六個舊 mirror metadata 與當前來源不同。先把舊 bytes 以
`_retired_mirror_beforeimages_20261003/` 路徑納入本次完整冷版，驗證兩種 bytes
都可恢復後，才整合 mirror 並由 canonical retirement 一起回收。
`feature_input_lookback32_v4` 原來只收小檔的冷 head／objects 仍保留，這次另外
發布完整封存，沒有覆寫舊冷物件。

## 操作與恢復契約

只有這四個 allowlisted 目錄可明確指定 `--manual-capture`；它使用至少 12 小時
穩定來源、完整 SHA／簽章及前後檔案集合核對，不是把自動七天規則改短。
`--manual-immediate` 則只豁免此次人工授權的 lease 年齡，兩種 flag 意義不同。

冷庫只在 `D:\stockagent-cold-primary\packed`，Linux 入口仍是
`/srv/stockagent-packed`。C 上 `/var/lib/stockagent-legacy-archive-stage` 僅供這次
有空間預算的編碼工作；D 完整可恢復、熱資料回收完畢後，另經 fingerprint、
全檔簽章、process／pin 及完整冷恢復閘門清掉，不留常態 C 冷副本。
先前中止的本次 D 工作暫存保留供稽核，未刪 D 冷 objects／歷史。

冷發布另修正大檔的跨路徑／跨 dataset 重用：來源已在穩定簽章下完整核 SHA，
若對應 D blob 已存在，先核其大小、SHA 及核對前後簽章，再直接重用，不先寫
另一份 D 暫存。損壞、redirected cold path 或來源變動仍拒絕發布，不修寫舊 bytes。
兩個同 bytes 大檔的單元驗證只發生一次 blob copy；其它 dataset 使用相同來源時
是零 blob copy，並經兩份原路徑獨立重建。這證明減少重複工作寫入，尚不宣稱
本次 D 裝置完整工作流的固定加速倍數。

Windows `Get-Partition D | Get-Disk` 與 `Get-PhysicalDisk` 核對：D 是
`ST4000NE001-2MA101`、SATA HDD；C 所在 `ZHITAI TiPlus7100s 4TB` 是 NVMe SSD。
因此自己的 D 大量驗證讀取採串行，避免和其它封存驗證反覆競爭同一 HDD。
操作只用 run receipt／PID start ticks／argv／cgroup／pidfd 身分確認自己的
兩個尚未 source retirement 的 reader；不暫停 provider、交易或 Syncthing。
有逾時及 `finally`／SIGTERM 恢復，不能把自己的 reader 留在停止狀態後交付。
[排程及硬體證據](../artifacts/operations/market_safe_retirement_20261003/owned_d_io_serialization.json)。

使用標準執行環境，先核對 D 掛載：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
bash scripts/mount_packed_d_cold.sh --check
run_fintech_python scripts/manage_legacy_artifact_archives.py verify \
  legacy-markets-forex --cold-only
```

`verify --cold-only` 在編碼工作暫存已回收時，會建立有空間預算的私有驗證暫存，
經全冷物件重建及原檔 decode 校驗後才清除；失敗會保留暫存並回報位置，
不會啟用服務、不會解封到 `artifacts`，也不會建立永久 C 冷副本。

需要舊資料時，換成上表的 dataset 並指定不存在的目的目錄，例如：

```bash
run_fintech_python scripts/manage_legacy_artifact_archives.py restore \
  legacy-markets-forex --destination /root/stockAgent/artifacts/restored-forex
```

恢復是明確的人工作業，不會因收到冷庫自動解出資料；目的路徑不應指向執行中
服務的 model／cache root。日常契約及所有參數以
[README](../README.md) 與指令 `--help` 為準。

刪除前後依序保留 inventory／dry run、獨立冷恢復、原來源及 mirror 比對、
pin／lease／程序／服務引用、D 掛載與 local Syncthing 健康證據。
WSL 檔案 blocks 釋放不代表 Windows VHDX 自動縮小；本次不做 root trim／VHDX
壓縮，也不聲稱 vastai1T 保有整份冷物件副本。

## 期貨 117 個 native 規則版本：尚不可只按版本號刪除

正規名稱 `all_products_rule_facts_native_v<版本>_<日期>` 共 **117** 個目錄。
最高版本號是 **v122**；先前只按名稱前綴得到 122 個，包含五個輔助目錄。
以下來自 2026-10-03 02:14 UTC 前後的唯讀盤點，並非冷恢復證明：

- 110 份 manifest 可讀，7 個版本沒有 manifest，不能以缺 manifest 認定內容無用。
- 可讀 manifest 合計 5,676,057 次來源引用、54,210 個不同 declared SHA；
  每種 SHA 的一份代表檔 stat 合計 4,376,716,330 bytes。
- v122 沒有包含其中 52 個舊來源 SHA，其代表檔合計 11,882,808 bytes。
- v122 的 `all_products_training_ready=false`、`point_in_time_verified=false`，
  狀態仍是 `candidate_facts_require_transition_and_source_review`。
- 當前期貨修復文件仍指定凍結 v63 builder，v54 replay、v936 acceptance 是
  其它 repair artifacts；歷史腳本中的文字引用不等於目前 live 依賴。

這些 SHA 是 manifest 宣告，尚未核對每份實際來源 bytes／所有獨立 derived
輸出；當時也未取得這 117 根的完整 D 冷恢復證明、未刪任何 native 規則版本。
後續來源補齊及回收結果見以下更新，不以早期盤點描述現在的完成狀態。

從第一性原理，保留單位應是唯一來源 bytes／來源時鐘、版本 recipe 與修復驗收
證據，而不是 117 份各自複製的完整來源樹。後續整理順序：

1. 逐檔實際核 SHA，先將所有唯一來源及各版獨有輸出／manifest 完整保存於 D，
   使用現有增量 content-addressed packed 實作，不建立另一套快照框架。
2. 明確核對現行服務、進行中的修復、builder／replay 和解析 ABI 的依賴閉包。
3. 待「最新候選＋必要修復依賴」保留範圍確認，再對其餘逐根執行完整冷恢復、
   dry run 和 process-reference 閘門後回收；最終完整版只有在全商品驗收後認定。
4. 不改 collector 未驗收來源的 `publish:false`，不把純 bytes 封存冒充官方規則
   完整性或 point-in-time 驗收。

### 11:40 台北時間：與現行期貨修復工作對齊

同時進行的 `futures-accounting-gaps-20261003` 已建立
[`all_products_rule_facts` 單一維護入口](tw_futures_rule_workspace_2026-10-03.md)，
來源／既有修補的整合驗證已保存於
[`rule_workspace_acceptance.json`](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/rule_workspace_acceptance.json)。
其 manifest SHA 為 `cd97873d71cb3cf8b027c23e46dbe744acaa470fa90839247ab7c632ba535e82`，
保留 54,532 份來源引用；全商品訓練／PIT 准入仍未通過。

歷史 unique bytes／原版收據已整理到 `.rule-parser-retired`，由既有
`legacy-tw-futures-rule-parser` 冷發布工作處理。11:44 的 run 狀態顯示該次
發布在 Syncthing object scan 遇到 timeout、exit 2，尚缺完整恢復驗收，
不能將原版刪除。這是另一個受監管的執行工作，
本次不另起封存、重寫其來源或暫停它；也不把 staging 收據當作 D 冷恢復證明。
v122 仍是目前會計快取的 SHA-bound 相容输入，不能因新的 current 已存在就
立即刪除。後續舊版回收應以該工作的完整冷恢復及精確引用計畫為準。

### 11:54 台北時間：歷史封存尚缺 52 份獨有來源

追加唯讀檢查所有 110 份歷史 manifest 的實際 SHA，並對未出現在目前
`.rule-parser-retired` 編碼 manifest 的來源逐檔核對 SHA／穩定簽章。
結果確認 **52 份、11,882,808 bytes** 尚未納入該封存；不是僅憑版本號的猜測。
包括 3 份 restoration 證據、40 份 OCR table receipts、6 份 OCR table outputs
及 3 份人工 cell review。此處指歷史封存缺口，不是現行規則金融准入判斷。

[覆蓋審查收據](../artifacts/operations/market_safe_retirement_20261003/native_capture_coverage.json)
保存精確 SHA、原路徑、kind、大小與 manifest binding；本檢查未改寫另一項
工作的 current／history、未刪除版本，亦未宣稱 D 恢復完成。
應先把這 52 份原值增量納入正式冷封存並重新通過恢復；7 個沒有 manifest 的
版本還須依實際檔案集合核對。不能因來源池已有 54,158 個物件就假定覆蓋
54,210 個歷史 declared SHA。現行 prune 的 inode-map 閘門會拒絕未知來源，
不可繞過該拒絕而直接 `rmtree`。

### 12:24 台北時間：補充恢復驗證與 116 根回收完成

既有期貨工作已把缺少的 52 份原值增量發布到
`legacy-tw-futures-rule-parser-sources`，未改寫原歷史冷物件。此項再經本次
獨立 D fetch、decode、原 SHA／大小與原缺口清單逐項比對：**52 份、
11,882,808 bytes 全部可恢復**，私有 C 驗證暫存已清除。
[獨立補充恢復收據](../artifacts/operations/market_safe_retirement_20261003/native_source_supplement_cold_recovery.json)。

對 7 個沒有 manifest 的版本另查實際目錄：19 份 top files 已核 SHA，
356,026 個來源 alias 名稱均被原冷編碼清單覆蓋，無額外未知 digest 名稱。
這項名稱審查不是重 hash 每份 alias 的 bytes；精確 inode／SHA-copy receipt
閘門仍由全 117 根清理計畫核對，該計畫記錄 6,032,083 個 source aliases。
[中斷版本審查](../artifacts/operations/market_safe_retirement_20261003/unmanifested_version_coverage.json)。

正式回收收據確認 116 個舊目錄已移除，原件及研究 receipts 保留在歷史冷版
與補充冷版。要完整重建舊來源集合，須取兩份：

```bash
run_fintech_python scripts/manage_legacy_artifact_archives.py restore \
  legacy-tw-futures-rule-parser --destination /ABSOLUTE/EMPTY/RULE_HISTORY
run_fintech_python scripts/manage_legacy_artifact_archives.py restore \
  legacy-tw-futures-rule-parser-sources --destination /ABSOLUTE/EMPTY/RULE_SUPPLEMENT
```

兩份 `objects/SHA256` 的原值配合 `receipts/原版本名.json` 可重建具名檔案。
只維護固定 current，不將歷史冷封存重新當作日常解析維護分支；當前金融／
全商品訓練准入仍由期貨專屬驗證決定。

四個研究根中的 v4 第一次 apply 因臨時 `cold-peer-not-converged` 在 source
quarantine 前停止，原件仍保留。另有兩小時上限的監督重試，只重試單一
transport blocker 且 state 仍 hot-enrolled／原件存在／無 quarantine 的情況；
其它 blocker 或已經開始 retirement 一律停止，不繞過任何安全閘門。
後續 v2 與 lantent 的監督 continuation 依序處理，不把排程當作完成驗收。

先前按目錄歸屬的約 261 GB 不是獨立唯一冷資料量，也不是精確刪除量。
既有 hard links 不能重複計算；容量變化與 allocated-byte 刪除收據分開記錄。

## 實作與驗證證據

- [操作證據目錄](../artifacts/operations/market_safe_retirement_20261003/)：
  四根 inventory、mirror beforeimages、每項 publication／retirement／stage
  prune／post-retirement cold recovery 及 acceptance。
- [期貨來源集合審查](../artifacts/operations/market_safe_retirement_20261003/native_rule_source_sets.json)：
  117 版 manifest 指紋、不同 declared SHA、最新缺失來源、引用與審查限制。
- [最新相關測試 log](../artifacts/operations/agent-workflow/runs/final-shared-storage-regression-20261003T034256-999817cc/run.log)：
  **123 passed，107.43 秒**（較早大檔重用修正為 122 passed），涵蓋 packed round trip／大檔重用、原封存／retirement、人工 capture、C stage prune、cold-artifact
  與 consumer；包含 post-rename 新檔／冷物件損壞時保留 quarantine 的案例。
- 本次操作任務：`markets-safe-retirement-20261003`。四根及期貨舊版回收的完成
  邊界分開記錄，不以單一部分成功宣稱整個期貨整理已完成。
