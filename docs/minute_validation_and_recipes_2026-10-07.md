# 分鐘資料驗證、入庫與訓練組合整理（2026/10/07）

## 執行進度與結論

台股來源及研究分鐘已完成全量內容驗證；不是只有看檔名或 Parquet footer。
有來源缺口／不可查合約，不能宣稱「所有歷史分鐘都抓完整」。
已實作下載完成後的收據／內容驗證、canonical 入庫與新版配方更新。
本次試組合的 derived panel 已移除；原始資料、既有服務依賴與歷史結果沒有刪除。
新增並驗收的完整自動入口是台股原始／研究分鐘；其他來源的現況及未完成項目另列，
不把本次台股修正說成所有 provider 都已完成同等 end-to-end 驗收。

訓練方法及遠端按需重建見 [組合方法手冊](training_dataset_rebuild_methods.md)。
本報告的所有數字以 `artifacts/data_quality/minute_recipes_20261007/` 收據為準，
不沿用之前對話的歷史數字或服務 active 狀態作完整性證據。

## 台股原始分鐘

| 檢查 | 結果 | 證據 |
| --- | --- | --- |
| 要求範圍 | 2020/03/02～2026/10/06；目前官方 stock／ETF universe 2,758 商品 | `download_summary.json`、history summary |
| 終止狀態 | complete 2,540；帶來源缺口 89；contract_unavailable 129；failed／partial 0 | terminal report |
| 實際原始 bar | 314,110,315 筆；170,180 個非空 parquet 全 SHA＋價格／時間／數量驗證 | `stock_source.json` |
| 查詢 chunk 收據 | 220,836 份，含 50,656 個 empty／source-gap chunk；覆蓋集合與日期一致 | `terminal_catalog.json` |
| tick 回復鏈 | 127 份保留的原始 tick 來源 SHA 驗證 | `terminal_catalog.json` |
| 正成交量商品日對照 | expected 3,293,833；observed 3,214,672；source_gap 68；contract_unavailable 79,093；未分類／pending 0 | `stock_history/summary.json` |
| 商品日例外 | 來源 gap 不在公開正量日 59；provider 回傳但不在公開正量日 2,905 | history 分類，不冒充無交易 |

coverage 判準是**目前登錄 universe 的官方正成交量商品日**；不代表所有曾上市商品、
每分鐘所有逐筆成交或歷史 order-book 完整。simulation 是行情帳號環境，並非 broker 成交證明。
全部缺口詳見 [商品日清單](../artifacts/data_quality/minute_recipes_20261007/stock_history/missing_symbol_days.csv)。

### 目錄留存與訓練選取集合不同

raw tree 實際保留 234,317 個 minute parquet、292,634 份 chunk receipt、2,634 份 symbol
manifest；目前選取集合引用的則是 170,180 個非空 parquet 與 220,836 份 terminal receipt。
差額 64,137 個 parquet／71,798 份 receipt **不在目前選取集合內**，不因留存就自動加入
訓練或重複計算。不能僅靠檔名判定它們全部是有效舊版本：本次完整 semantic audit 的
放行範圍是目前 manifest 引用的 chunks；其他留存檔只有清冊及 canonical 打包的 bytes SHA
邊界，沒有宣稱全都通過訓練語意驗收。原始未刪除。
實際檔案分類見 [raw physical inventory](../artifacts/data_quality/minute_recipes_20261007/raw_physical_inventory.json)。
逐檔未選取清單（含固定 cold snapshot、bytes SHA 與不准入標記）共 135,935 筆，
見 [清單收據](../artifacts/data_quality/minute_recipes_20261007/retained_not_selected_chunks.json)，
收據指向完整 CSV；沒有把這些留存檔偷偷刪掉或加入新版訓練。

## 台股研究分鐘

- 1,608 個官方交易日，2,629 個有來源商品，314,109,924 筆。
- 全部 partition SHA、`(ts,symbol)` 唯一性、session、OHLC、數量／notional／tick grid、
  feature／label 有效性與官方交易日已驗證；結果 `research_ready`、failures 為空。
- 421,697 筆零量 bar 不當作成交容量；有效 feature 203,906,536 筆、有效 label
  215,936,193 筆。無效標籤仍保留 mask，沒有補造結果。
- 比原始少 391 筆，是兩個非官方交易日的觀測被 builder 隔離；原始沒有刪除。
- 此項不驗證企業行動、所有商品歷史 eligibility 或真實券源，仍由 selected 訓練契約驗收。

### 真實重建測試

`test_minute_recipe_reconstruction.py` 從原始 chunks 重建 0050、2330 的全部 1,608 日，
共 848,123 筆；逐欄 exact 比對價格、容量、模型特徵、標籤，通過。
耗時 143.33 秒；所有測試輸出在擁有的 `/tmp/stockagent-minute-recipe-*` 內，結束後已移除。
只保留 [測試收據](../artifacts/data_quality/minute_recipes_20261007/temporary_reconstruction.json)。

最初 subset 比對發現 whole-universe schema union 中 `volume_unit_proof` 是全 NULL，
而這兩個商品的 subset 沒有該 metadata 欄。比較器只允許**基準證明全 NULL 的可選證據欄**
對齊 typed NULL；不放過缺價格、缺 feature、非 NULL 證據或數值差異。

## 其他分鐘來源的現況

後續加入 FinMind 分鐘清點與跨來源 row 合併：TAIEX 從 2005 年、個股從 2019 年起，
與上述 Shioaji-only 2020 年來源的驗收範圍不同。原始左右分鐘標籤、撮合機制及股份單位
必須先對齊，不能直接接表或稱全部 provider 都已完成驗收。
詳見 [合併方法與最新固定清冊](tw_raw_source_merge_2026-10-07.md)。

| 來源 | 現有檔案／footer rows | 本次驗證範圍／限制 |
| --- | --- | --- |
| 股期分鐘 v1 | 1 檔／77,075 | 清冊，不等同完整歷史 |
| 股期分鐘歷史 v2 | `all_minutes` 25,574,357；事件分鐘 1,083,437 | 四份輸出全 SHA＋canonical semantic validator 通過；manifest 仍 partial；37,723 缺口 contract-days 未解除 |
| Shioaji 衍生品／指數來源 | 留存 49,796 檔／33,033,782 footer rows | 留存歷史含非本輪選取及重疊，不能當唯一成交筆數 |
| 本輪 Shioaji 合約集合 | 6,811 選取；6,808 complete；9,181,255 kbar rows | waiting_source/source_gaps；14 partial chunk、61 正活動 empty query、28 官方活動缺 kbar 日、1,366 provider-unavailable；16 tick query 未解決 |
| OKX 1m | 501 檔／460,765,929 footer rows | 全檔 OHLCV 檢查另外寫入 crypto 收據；沒有全 provider coverage／tick proof |
| Binance 1m | 575 檔／734,709,799 footer rows | 同上；gap／stale 需分上市／退市／維護，不直接稱資料損壞 |
| Bybit 1m | 本次 hot materialization 下未見 1m tree | 不是原始 cold source 遺失證據；尚未解封其他 source release 驗證 |
| `tw-minute-v5` 舊別名 | 本次未見 hot tree | 歷史派生／退休版本，不算目前原始下載失敗 |

OKX／Binance 1,076 檔的完整 OHLCV 檢查已結束：1,075 `ok`、1 `warn`，
無重複時間、無 OHLC／數量錯誤、無 large-gap；唯一警告是 OKX `ICXUSDTSWAP`
最後日期 2026/09/17（距 audit target 19 天）。此處尚未證明退市／維護或下載停滯，
保留 stale 為待確認，不憑空補 9/18 後價格。完整收據在
[crypto audit summary](../artifacts/data_quality/minute_recipes_20261007/crypto/minute_recipes_20261007/data_quality_summary.json)。

股期 v2 的本次 semantic check 使用 manifest 宣告的 daily hash，只證明該組檔案內部一致；
沒有重新綁定外部 daily panel 或驗證全企業行動，不宣稱正式 futures training ready。

## 根因修正與自動流程

```text
既有下載 owner 結束／追新
  → completion receipt（失敗／未終止則不發布）
  → chunk／manifest／保留 tick 的完整 SHA、內容與覆蓋驗證
  → 研究資料官方交易日、價格格點、單位、mask／label 驗證
  → canonical source lock ＋ source-stability guard ＋ packed 原子發布
  → 固定來源／程式 hash 的新版方法（不建立大型 panel）
  → 遠端明確按需 hydrate／trial／build／原訓練閘門
```

已修正：

1. 原始 builder 不再讀 manifest 裡仍存在的 producer 絕對路徑；只讀選定 root 的 canonical
   相對檔名。遠端重建與原機同時存在時不再誤讀 mutable 舊資料。
2. 驗證 cache 綁定完整 native inode／size／mtime／ctime、預期 SHA 與 validator code。
   第一次全檢，後續只增量驗證；內容更動但回填 mtime 仍失效，失敗結果不寫成成功 cache。
3. 同一來源的完整內容 proof 可復用，但 root tree、驗證程式及官方日曆 bytes 改變就失效。
   cold 發布／遠端重建仍保留自己的 hash 邊界，不因 cache 把傳輸驗證取消。
4. Python `-m` writer 可以被正式 publisher 的 active blocker 辨認，且 all-ready 某來源失敗
   不會遮蔽其他來源，但各項錯誤會保留。source-lock 和 atomic head 不變。
5. WSL 互動終端與 systemd 的 D 掛載 namespace 不一致，曾導致 8 KiB mount gate 拒絕。
   正式 release／recipe shell 入口改用 systemd authority namespace；不放寬卷身分／掛載檢查。
6. 原始分鐘完成後先發布 raw source，再發布通過完整驗證的研究來源。發布成功再更新
   metadata 配方，並由五分鐘 timer 補漏；配方失敗不冒稱下載失敗或把 training ready 改 true。
7. 試組合子程序走 repo `-m` 入口；factorized 臨時 execution cache 也放在擁有的 trial 範圍，
   成功與失敗都清掉自己的臨時輸出，不產生隱藏的大 cache。
8. D 冷庫 head 提交後的 Syncthing scan 曾逾時。新增的分鐘／配方自動發布入口沿用
   已有 `--defer-scan` 與獨立結果收據，將通知持久化到既有 retry timer；沒有放寬
   inherited source-lock／唯一收據／單一 dataset 的 guard。通知重試不重新抓價格或組 panel。
9. 長子程序執行時 rollout 原 shell 檔案，舊 bash 曾在返回後讀到變更的 offset 而 EOF。
   release／recipe 入口先完整解析 main function，再執行；兩個入口都做了原 inode
   執行中覆寫的回歸測試。`nsenter` 後亦明確回到 repo cwd，避免相對收據寫到 `/artifacts`。
10. 配方服務曾因等待大型 raw 發布的共用 retention lock 超過五分鐘而被 systemd 終止。
    保留五分鐘補漏週期，但 service timeout 調成一小時。修正後 oneshot 實跑 exit 0，
    耗時 11.07 秒；不是只看 timer active。

## 本次實際冷庫提交

以下四個 head／manifest 已核對；source publication 的原子提交與傳輸驗收分開。

| Dataset | 固定 source snapshot |
| --- | --- |
| 官方日曆／公開資料 | `tw-public-20261007T050725143636373Z-l0-penguin-ed6213a7b7f097d6` |
| 原始分鐘／收據 | `tw-minute-source-cold-20261007T053017021584082Z-l0-penguin-2935796a6e4a6995` |
| 驗證研究分鐘 | `tw-minute-train-20261007T052016199273549Z-l0-penguin-bbecb931e2f841fb` |

`training-dataset-recipes` head 是 metadata-only 方法包；每次來源／方法變更會更新，
固定 ID、SHA、獨立 cold 解包與 no-op 證據見
[冷庫驗收收據](../artifacts/data_quality/minute_recipes_20261007/publication_acceptance.json)。
原始包 529,718 個檔案、5,828,610,702 logical bytes（包含完整收據與保留來源），
本次只新增 1,231,061,719 sync bytes，復用 380,577 個檔案；不是又複製全量來源。
新 raw／research metadata 明確保留 `all_source_coverage_complete_claim=false`。

早期兩次 shell rollout／scan 失敗的 exit code 與已提交 head 都保留，不把它們偽寫成正常
命令 exit 0。公共／raw publisher 的結果收據已由錯誤 cwd 的確切檔案恢復到 repo。
研究 head 已提交，但即時 scan 逾時使原結果收據未寫出；改以 manifest 的提交驗收明確記錄。
**冷庫提交、scan acknowledgement 與遠端 peer 收齊是三件事；本次未驗收 Vast delivery。**

## 尚未放行的項目

| 範圍 | 未完成／缺失證據 | 本次處置 |
| --- | --- | --- |
| 台股來源完整度 | 89 個帶 gap 商品、129 個不可查合約；不是所有歷史商品的完整 minute universe | 保留缺口分類／mask，不生成假價格 |
| 未選取 raw 留存 | 64,137 個 parquet／71,798 份 receipt 未在当前選取集合 | 與訓練輸入分開清冊；保留 bytes，不冒稱已通過 semantic 准入 |
| 合約分鐘／股期 v2 | 官方 activity／tick queries 仍未解；v2 外部 daily binding 未重新證明 | 沿既有 history owner 的等待／發布閘門，不宣告正式訓練就緒 |
| OKX／Binance | 全檔內容 audit 通過但沒有全 provider lifetime／逐分鐘 coverage proof；OKX ICX stale 原因待查 | 留下逐檔報告；既有 daily audit 選項及發布 owner 不等於本次新增台股流程的驗收 |
| Bybit／退休別名 | 目前 hot tree 不含相應 1m view；未解封其他 cold release | 不判定原始資料遺失，也不造出最新版 |
| 歷史組合 | 6 份設定不能解析，部分 run 缺 companion source SHA；封存 payload 未打開 | 保留原設定及具體缺口，不能把 167 組都稱可精確還原 |
| 遠端新版組合 | 最新程式 checkout、指定包的 peer convergence、完整 input hydrate 與訓練閘門尚未驗收 | 已提供固定版本按需重建方法；未啟動訓練／未切換既有工作 |

## 驗收與保留邊界

測試包含來源破壞／SHA 不符、ctime 防偽舊 mtime、receipt／範圍缺口、tick lineage、
symlink 越界、producer path 誤讀、日曆更動失效、版本不變 no-op、新資料升版、
來源範圍不相容等待、真實 cold fixture hydrate、試組合成功／失敗清除。
最終 12 份相關測試檔共 305 項通過，包含 shared packed publisher 與
canonical factorized／mixed-frequency regression；收據 `focused_tests.xml`。
Ruff、Python compile、shell syntax、改動 whitespace 與新文件 link 檢查通過。
完整全專案 suite、所有歷史組合重建與實際雙卡正式訓練不在此驗收。

此輪沒有 provider API 登入、購買資料、刪除原始數據、變更選定模型／損益假設，
也沒有啟動正式訓練。未完整的 futures／Bybit／歷史來源綁定另列為後續工作，不混同入庫成功。
