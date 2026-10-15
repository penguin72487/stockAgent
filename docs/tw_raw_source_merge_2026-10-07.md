# 同義原始資料合併方法（2026/10/07）

## 執行進度與邊界

已實作 receipt 固定的台股分鐘跨來源合併、共用同義 row 選取器，以及獨立的來源分段、
補缺／拒收／差異 sidecar。依先前要求，試組合不保留組合後資料，只留方法與驗證證據。
沒有刪原始資料、改正式訓練來源、續接不同資料的 optimizer、公開受限資料或啟動 GPU。

**後續 v2 單位補證已完成：原先 140,671 筆缺單位全部補齊，剩餘 0。**
389 個受影響交易日逐一用同日 TWSE 原始報表確認 0050／2330 上市身份，
依 FinMind 官方「張 × 1,000」轉股；價格、已知股數及來源選取不變。
詳見 [單位修復報告](tw_minute_share_unit_repair_2026-10-07.md)。下表保留 v1 原試驗基線，
不是修復後仍有相同缺漏；v1／v2 都不代表全部商品全歷史已能正式訓練。

全部來源的分區範圍、已完成試驗結果、77 個既有財務等價映射，見
[自動產生的 Markdown 清冊](../artifacts/data_quality/tw_raw_source_union_20261007/index.md)。
它依固定來源 inventory 與完成的 merge receipts 產生，不讀 mutable latest 猜結果。
清冊與小範圍／代表商品試驗，不等於全部商品全歷史已逐列完成合併或已能正式訓練。

| 已執行項目 | 本次結果 |
| --- | --- |
| 全來源清冊 | 6,957 個選取分區；逐一固定來源／receipt SHA，不混入退休查詢 |
| 全市場 2020/03/02 試合併 | 355,905 來源列 → 184,022 唯一列；1,981 筆有效來源差異，未覆寫主來源 |
| 0050／2330／TAIEX 全歷史試合併 | 5,350 個 session，3,288,322 來源列 → 2,440,742 唯一列 |
| 歷史試驗來源檔 | 6,957 檔全 bytes SHA／footer 通過；row 語意驗證只聲明上述三商品 |
| 歷史試驗差異／拒收 | 552 筆有效來源差異；壞來源檔 0、非法 row 0 |
| v1 歷史試驗未知股數 | 140,671 筆；這是補證前基線，後續同日官方市場補證見下方 v2 方法 |
| 方法／補值回歸 | 55 項測試通過；包含錯誤備援、零值／NULL、時鐘、重複、SHA、精確分段及 Markdown |
| 保留狀態 | 試組合已移除；原始來源與來源／差異 sidecar 保留；沒有啟動訓練 |

歷史試驗選取 FinMind 1,592,089 列、Shioaji 848,653 列；FinMind 中 1,449,308 是
TAIEX 指數，140,484 是個股歷史延伸，2,297 是相同鍵在主來源缺失的原生觀測。
2,297 也含盤後定價等機制，**不是 2,297 筆普通分鐘缺口都已變成可成交量**。
完整分段 Markdown 可從自動清冊進入；目前未做全商品全歷史 row 合併驗收，
也未重新檢驗全部數千個財務欄位的等價性或更動正式訓練資料。

## 1. 2005 年分鐘資料指的是什麼？

FinMind 官方說明：`TaiwanStockKBar` 個股資料從 2019/01/01 開始，TAIEX 指數從
2005/01/03 開始；全市場 storage 物件從 2019/01/02 開始。
[FinMind 技術面文件](https://finmind.github.io/tutor/TaiwanMarket/Technical/)

本機已選取 3,465 個 2005～2018 的 TAIEX 日分區，以及 1,883 個 2019～2026 的
全市場分區。2020/03/02 的全市場實測也含 TAIEX。2005 年指數可以作相應共同特徵，
**不能填成 2005 年個股分鐘價**，也不因此改掉目前 2014 作上下文的年度訓練契約。
完整查詢狀態、實際 source 範圍及後續新增資料，以帶時間的清冊為準。

目前 FinMind 完成分區到 2026/10/05；10/06、10/07 是 `observed_empty`，
本次不把空回應算成下載到當日分鐘。沒有額外呼叫 provider 或推定下載器已壞。

## 2. 先證明「同義」，才可以接 row

相同欄名並不是相同資訊。合併前固定：

| 維度 | 必須相同／明確轉換 | 本次分鐘規則 |
| --- | --- | --- |
| 實體 | 代號、商品種類、歷史身份 | equity 與 TAIEX index 分開；不猜退休 `001` 為目前 TAIEX |
| 粒度 | 事件、秒、分鐘、日、月、季 | 不把 tick、日 K 或低頻已公布值造為新分鐘 row |
| 價格口徑 | 原價／調整價、幣別或指數單位 | 股票元、指數點分開；原價，不混復權價、不平均差異、不加 tick |
| 數量口徑 | 股／張／合約／指數結構性量 | 確認單位後才生成股份量；無證據維持 NULL |
| 時鐘 | bar 起／末、撮合機制、時區 | Asia/Taipei；一般已完成分鐘右標，特殊撮合保持原時點 |
| 版本 | current revision／original vintage | 本次是當前 provider 版本的研究原生觀測，不宣稱歷史 PIT |
| 財務範圍 | 合併／個別、累計／單季、會計 taxonomy | 沿用已定義的經濟映射與重疊 QA，不靠模糊名字合併 |

分鐘唯一鍵是 `(asset_class, symbol, ts, phase)`，不是只有 `(date, 股票代碼)`。
只有一致的 keys、單位、機制、口徑與版本聲明，才可交給共用 row merger。

## 3. 分鐘原生來源與轉換

FinMind 從 SQLite 的 `state=complete` current receipt 選檔；不掃所有留存版本做 UNION。
退役逐股 query、empty、pending 不納入。永豐從已驗證研究 manifest 選分日檔，只投影原始
OHLC／Volume／Amount／contract_unit；不把既有 labels 或訓練衍生欄當原始觀測。

| 欄位 | Shioaji 來源 | FinMind 來源 | 統一值 |
| --- | --- | --- | --- |
| 商品 | `symbol` | `stock_id` | `symbol`＋明確商品類別 |
| 時間 | 原生 `ts`，已完成分鐘右標 | `date`＋`minute`；一般左標 | 一般 FinMind ＋1 分鐘，撮合時點例外 |
| OHLC | 原生 `Open/High/Low/Close` | 原生 `open/high/low/close` | 原價、Float64，OHLC 整組選取 |
| 股份量 | `Volume`＋獨立 `Amount`／OHLC／已知倍率 | 原生 `volume`＋可證明的同期單位 | `volume_shares`，無證據 NULL |
| 成交金額 | 原生 `Amount` | 此 dataset 沒有同義欄 | `Amount`；FinMind NULL，不以價×量捏造 |

FinMind 一般分鐘左標與永豐右標在重疊原生資料中驗證；規則固定在 named contract。
`13:30` 收盤、`13:33` 延後收盤、`14:30` 盤後定價不加一分鐘；
一般分鐘即使右標同為 13:30，仍靠 `phase` 與收盤撮合區分。
其他非一般 session 的原生觀測另標記，不自動接進當沖執行容量。
FinMind 也曾發布重複列、假收盤列及興櫃轉板修正，應依目前 receipt/correction scope 重驗，
不能把所有舊留存版本都視為新版。[FinMind 更新紀錄](https://finmind.github.io/WhatIsNew/)

FinMind 官方說明上市／上櫃 volume 為張、興櫃為股；現今 StockInfo 可能保留多種身份。
本次不將今天的上市類別直接套用到歷史。v1 只使用相同商品／相同日期至少三個正量、
OHLC 相同且 key 不重複的永豐 bar，因而將沒有永豐歷史的資料保留為未知。

v2 `tw_raw_minute_provider_union_v2_dated_units` 增加另一條獨立證據鏈：
用官方下載器的 append-only journal 固定 TWSE／TPEx **同日原始報表**，驗證 raw bytes
SHA／size、官方 URL，以及 canonical parser 的 top-level／table／params 日期檢查；
僅報表中實際出現的同日同商品得到當日市場身份，再套用 FinMind 官方的張／股定義。
這不是用日成交量反推倍率，也沒有跨日期延伸或將當前 StockInfo 回填到歷史。
日報可含鉅額、零股等交易而與分鐘量不相等，不能強迫兩者加總對平。

同日永豐正量證據仍保留為交叉核對；若獨立證據確認不同倍率，記錄 unit proof conflict，
該商品／日的股份量維持 NULL，不依來源排序猜單位。官方 TWSE／TPEx 日報不涵蓋的
興櫃商品，不因某天沒出現在日報就被猜為興櫃；需另有同日證據才能套用倍率 1。
沒有同期證據時仍保留 NULL 股份量與原始 FinMind 檔。
TAIEX 原生量 0 是結構性值，統一可成交股份量為 NULL，不算 50% 容量。

這不是學出倍率或用統計擬合回推單位。未經驗證的 FinMind 歷史量不能冒充可執行
容量；補出的 OHLC 也不因此具有 Amount、VWAP 或 bid/ask。

## 4. 哪一段是哪家、誰補誰？

已驗證主來源順序固定為 Shioaji → FinMind，主來源正確列不被覆寫。
聯集保留所有來源獨有且可接受的原生觀測，而不是只取兩者交集。

| 情況 | 資料動作 | 留下的原因 |
| --- | --- | --- |
| TAIEX 只有 FinMind | 保留同種類指數，不偽裝個股缺口 | `native_index_source` |
| 個股在 Shioaji 歷史起點之前 | 用 FinMind 真實歷史延伸，不稱下載損壞 | `historical_extension` |
| 主來源有合法 row | 保留 Shioaji 原值 | `primary_valid` |
| 主來源缺同鍵 row | 補 FinMind 合法同義 row | `missing_primary_filled` |
| 主 row 為負價、壞 OHLC、非法時間／數量等 | 拒收主 row，選合法 FinMind row | `repaired_rejected_primary` |
| 主檔 SHA／receipt／footer／schema／session 失敗 | 不讀壞 bytes；補可驗證來源 | `repaired_rejected_primary_input` |
| 雙方有效但數字不同 | 保留主來源、保存雙方原值與不同欄位 | `conflicts.parquet`；不是自動認定 provider 錯 |
| 同 provider 同鍵完全相同 | 精確去重，保留來源依據 | `duplicate_rows` |
| 同 provider 同鍵不同值、又無版本區分 | 全部拒收該鍵，不靠排序猜對者 | `conflicting_provider_duplicates` |
| 雙方缺漏／無法驗證 | 保留缺口／拒收，不造 row、價格或容量 | `rejected`／`rejected_inputs`；不宣稱完整 |

FinMind 獨有 row 不全是「永豐錯誤補好」：可能是指數、歷史延伸、盤後定價、延後撮合、
商品覆蓋差異或真正普通分鐘缺口。報告依 `phase` 與原因區分，不混算成全部可成交。
未決價格差異也不悄悄清零或刪掉原始檔。

## 5. 訓練不必知道 provider，但後人要能追溯

統一數值表僅為 keys＋`Open,High,Low,Close,volume_shares,Amount`。
provider、source SHA、選取原因、下載時間、拒收資訊放在旁邊，不增加模型 feature。
`asset_class`／`phase` 是商品及行情機制 keys，不是新增 alpha 通道。

每個 session 旁的證據如下：

| sidecar | 內容 |
| --- | --- |
| `source_refs.json` | 當日 provider、來源角色、實體路徑、SHA／rows、receipt SHA、原生單位 |
| `lineage_segments.parquet` | 商品、機制、provider、source ref、起止分鐘、列數、補缺原因；缺分鐘必拆段 |
| `conflicts.parquet` | 相同經濟鍵的有效差異；保留雙方原值與不同欄位 |
| `rejected.parquet` | 不接受的原始標準化 row 及原因 |
| `rejected_inputs.json` | 指紋／schema 等檔案失敗的具體檔案、provider、錯誤 |
| `same_day_share_proofs.parquet` | 同商品同日單位倍率與匹配證據數 |
| `dated_market_share_proofs.parquet` | 官方同日市場身份、文件定義、來源 raw SHA 及適用倍率 |
| `unit_evidence_refs.json` | 官方原始報表、journal 完整 entry、URL、parser version／size／SHA |
| `unit_proof_conflicts.parquet` | 不同獨立單位證據矛盾的原值，不猜倍率 |
| `rejected_unit_inputs.json` | 日期／SHA 等不符的單位證據來源 |
| `merge.json` | 當日來源／選取／去重／拒收／差異數與輸出 SHA |

merge report 另固定整份來源清冊 SHA 及實際合併、normalizer、股份量邏輯的程式 SHA。
每一來源列可沿 source ref 回到當日實體來源清單；不是只寫「來自 FinMind」就結束。
首末日期不冒充連續覆蓋；完整分鐘區段在 lineage，而不是只保留 top-K 差異樣本。
對明確指定的代表商品，報告工具另將**全部**精確分鐘區段轉成逐商品 Markdown，
每段連回當日來源收據，不只印一個首末日期。Markdown 本身另有 input／程式／輸出 SHA 收據。

## 6. 其他同義財務／低頻資料沿用什麼方法？

共用 `stockagent.data.source_rows` 已支援明確語意的數值 row 選取，不只 OHLC。
財務實際來源合併仍沿用 `tw_public_cross_source_fill`、
`build_tw_cross_source_fill_bundle.py` 與混頻來源 builder，不能用分鐘規則跳過財務 QA。

已定義的 77 個經濟映射完整列在自動 Markdown 清冊；包括資產／負債／損益／現金流、
EPS、營收、外資持股、TDCC、鉅額交易。既有衍生營收公式另由 canonical 原生期別計算。

財務合併必須先對原始 report period／entity／幣別單位／合併口徑，之後才處理公布時鐘。
FinMind 元轉 FinLab 千元用明示 `.001`，EPS／股數／百分比用明示 `1`，不反推倍率。
累計現金流→單季需同年度相鄰季度；不能用上一筆非相鄰報告亂減。
營收 subject month 與 FinLab release-month index 需轉到同一原始月份。
TDCC 級距須完整且不含 total／調整列；日資料也不能亂接成週頻觀測。

完整財務重疊 QA 通過才做 missing-only；現有門檻、真實零值與原始 NULL 障礙沿用既有
契約。近期已驗收的財務來源、各實際填值及未通過等價比較詳見
[既有逐來源報告](tw_feature_expected_gaps_2026-10-04.md#41-有替代來源值為什麼仍不直接補)。
這次沒有把全部數千項未知 taxonomy 自動合成一份已合格 panel，也沒有降低既有 QA 門檻。

原始低頻未更新日維持 NULL；訓練的 carry、公布下界及過期規則由所選 builder 負責。
即使模型不吃 provider／age／更新旗標，內部仍須證明 availability，不能提前用未公布值。

## 7. 可重現指令

以下只清點／試合併，不訓練、不改 live source、不發布至公開 cold package。
來源持續下載時，每次先重新 inventory，再固定這一次的 inventory JSON。

```bash
cd /root/stockAgent
source scripts/runtime_env.sh

run_fintech_python scripts/merge_tw_minute_sources.py inventory \
  --official-root data_tw_public \
  --report-root artifacts/data_quality/tw_raw_source_union_<新版本>

# 全部商品、全部有效來源：可能耗時較長；不用額外列股票。
# trial 完成／失敗皆移除自己建立的合併資料；只保留查核 sidecar。
run_fintech_python scripts/merge_tw_minute_sources.py trial \
  --inventory artifacts/data_quality/tw_raw_source_union_<新版本>/source_inventory.json \
  --report-root artifacts/data_quality/tw_raw_source_union_<新版本>/all_history

run_fintech_python scripts/report_tw_raw_source_merge.py \
  --inventory artifacts/data_quality/tw_raw_source_union_<新版本>/source_inventory.json \
  --trial-report artifacts/data_quality/tw_raw_source_union_<新版本>/all_history/merge_report.json \
  --output artifacts/data_quality/tw_raw_source_union_<新版本>/index.md
```

可加 `--symbols 0050,2330,TAIEX` 或 `--start-date/--end-date` 限定驗證範圍；報告會明列，
不把 subset 宣稱為全量。`--report-root` 使用新的試驗子目錄，不混前次 sessions。
腳本逐 session 合併，記憶體不載入全歷史；使用受控 Polars threads 可避免壓到行情服務。
試驗在 report-root 的擁有暫存下進行，不用 `/tmp` RAM 磁碟塞數億 row。

已知 v1 缺單位時，不必重跑 5,350 個未受影響 session。以下固定舊 raw inventory，
明確升版官方單位補證，只重組先前有未知股數的交易日；先重現舊 parquet SHA，再逐列
確認 keys、OHLC、Amount、來源選取與已證明的股數不變：

```bash
run_fintech_python scripts/merge_tw_minute_sources.py trial \
  --inventory artifacts/data_quality/tw_raw_source_union_20261007/source_inventory.json \
  --official-root data_tw_public \
  --repair-report artifacts/data_quality/tw_raw_source_union_20261007/full_history_0050_2330_TAIEX/merge_report.json \
  --symbols 0050,2330,TAIEX \
  --report-root artifacts/data_quality/tw_raw_source_union_<新版本>/dated_units_v2_repair
```

新補證清冊與舊 raw inventory 的 SHA 分別保留；舊試驗／cache／optimizer 不會被覆寫。
實際補證的前後結果見 [股份量單位修復報告](tw_minute_share_unit_repair_2026-10-07.md)。

需要保留乾淨原生 source 時，明確改成 `build --output-root <新來源目錄>`；不覆寫已有
來源／模型。產物仍標 `training_ready=false`：官方日曆、歷史商品身份、價位格點、
分鐘量及所選執行 loader 的契約仍需驗收。不能把新來源改名冒充既有
`shioaji_kbars_1m` research-ready 產物、沿用舊 feature/cache fingerprint 或直接接舊 optimizer。

遠端重建再沿用 [訓練組合方法](training_dataset_rebuild_methods.md)：固定受限來源 release、
部署同一程式 SHA，再按需建立所選訓練 view。這次沒有自動傳輸／切換正式訓練。
