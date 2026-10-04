# 台幣期貨單一規則工作目錄（2026-10-03）

目前只維護
`artifacts/markets/tw_futures_v8_margin_preparation/all_products_rule_facts/`。
原始公告、原生表格及人工核對仍保留原值與 SHA；已接受的商品修補集中到這個
目錄，後續不再建立 `all_products_rule_facts_native_v123_*` 等整套複本。
這是目前已有來源與修補的整合版，不代表歷史缺口或全商品訓練准入已完成。

## 更新與中斷處理

沿用既有解析器及 review 參數，只增加 `--update-current`：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_tw_futures_margin_event_candidates.py \
  --update-current \
  --archive artifacts/markets/tw_futures_v8_margin_preparation/all_products_source_archive_20260929
```

解析器預設使用上述固定目錄；完整版本的流水號路徑會被拒絕。更新先寫入固定
`.rule-facts-pending`，驗證 SHA 後以同一檔案系統的原子交換發布。失敗保留原版；
相同內容及契約為 no-op。相同來源 facts 不會遺失已接受的保證金布局與 interval
修補。來源、時鐘或金融欄位改變時，仍依原契約重放受影響商品，不能沿用舊
訓練 checkpoint 或把 candidate 當作正式 PIT。

`.rule-facts-previous` 最多保留一個待封存輸入，避免每次修補新增目錄。
存在未處理的 pending／previous 或程序、服務引用時，更新拒絕覆蓋；先核對收據
與精確冷封存，再清理該次暫存。先前 manifest 不再逐代複製進 current 的 sources。
需要訓練時固定當次 manifest SHA／來源 release，不能在同一個訓練中跟隨變動的
current。收據包含 raw source 不代表有可執行價格、完整會計或 GPU 訓練驗收。

## 舊版清理與還原

已移除 **116 個**舊解析目錄及 5 個舊輸出 log；原有 117 個流水號實體目錄已歸零。
v122 的精確 bytes 移到 `.rule-facts-baseline/`，原名稱只保留一個相容 symlink，
供已保存會計輸入的 SHA 核對使用。它是唯讀基準；只維護固定
`all_products_rule_facts/`。

清理收據：
`artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/rule_parser_cleanup_acceptance.json`。
603 萬個原來源 alias 經原件 SHA／檔案身分與冷封存對照後才刪除；
原始行情、規則原件、受影響商品修補收據與來源物件保留。清理記錄包含
檔案系統可用空間的前後觀測，不能直接宣稱是實際回收的 Windows 磁碟容量。
刪除前程序、服務及保護標記檢查與本機同步閘門通過。清理後 04:26／04:28 UTC
採樣時，同步資料夾仍在索引，need／delete／error 均為零；保留兩次觀測，
不將這次本機規則清理描述成遠端配送或訓練驗收。

清理先將舊 facts、收據及原件依 SHA 整理為一份來源，使用既有
`manage_legacy_artifact_archives.py` 的 `legacy-tw-futures-rule-parser` 清單項目，
發布到正式 D 冷庫並驗證編碼、解碼原值及 packed objects，完成引用／pin／lease
與逐檔清理計畫後才移除原目錄。封存的資料角色為不可部署的歷史證據。
本次可重建的 C 暫存省略逐小檔 fsync，適用於明確指定的 manual capture；
正式 D 冷庫的檔案、manifest 與 head 發布仍保留同步及 checksum 驗證。
原本最新的 v122 是目前會計快取的 SHA-bound 輸入，保留其精確 bytes 作相容核對，
不在該來源上繼續修補。

可用既有還原入口取得封存來源：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/manage_legacy_artifact_archives.py \
  restore legacy-tw-futures-rule-parser --destination /ABSOLUTE/EMPTY/RESTORE_DIR
```

`receipts/原版本名.json` 保存原 manifest；`objects/SHA256` 保存唯一內容。
原 manifest 的 outputs／sources 可重建原邏輯檔名與副檔名；沒有完成 manifest 的
中斷版本，其具名 top files 保留為 `receipts/原版本名--原檔名`。
不得把歷史封存重新當作另一個維護分支。

另有 52 份早期版本引用的來源收據（原值合計 11,882,808 bytes）不在原始
CAS 聯集，已只補這 52 份到 `legacy-tw-futures-rule-parser-sources`。
還原時也以同一入口取得這個來源補充集合，再依 SHA 合併 `objects/`；
這是歷史來源補充，不是另一套解析版本。原件的 mtime 與 bytes 保留，新的
封存與校驗觀測時間記入獨立收據。主封存未重建，同步通知逾時收據仍保留；
以既有批次入口重試通知後，主封存的原件、解碼及冷庫校驗通過。

```bash
run_fintech_python scripts/manage_legacy_artifact_archives.py \
  restore legacy-tw-futures-rule-parser-sources --destination /ABSOLUTE/EMPTY/SUPPLEMENT_DIR
```

## 本批驗收與剩餘資料

- 共用保證金／研究限額回歸 791 passed，0 skipped。
- 固定工作目錄及原解析表格測試 31 passed，2 skipped；另外實測流水號輸出
  exit 2，沒有產生新版本。
- 暫存中斷恢復、精確冷還原、packed 發布及固定目錄回歸 109 passed、2 skipped。
- KF／FT 自身後繼契約八個接續座標已核對；只合併四代碼已保存結果。
  局部 259 代碼剩 1,790 個受阻會計列，部位及其時鐘受阻列均為零。
- FinMind 已有 382 天完整下載收據；1,475 個精確商品／月份／日期清算鍵均被
  來源省略，零正值候選。因此不重抓同樣資料，也不拿近月、後來的最後結算
  或成交收盤冒充每日清算。CPF 早期保證金鏈另在這批快取之外。
- 尚未執行最後全 711 代碼驗收或遠端訓練；沒有重建全 765 商品帳本。

本批逐列、工作目錄與封存／清理收據集中在
`artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/`。
