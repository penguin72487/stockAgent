# 台股成交量單位與驗證契約（2026-09-26）

**股票的訓練與執行用數量一律以「股」表示。** 原始提供者欄位不覆寫；只有在單位有證據時，才產生 `Trading_Volume` 或 `volume_shares`。張、千股、合約口數及未知 `volume` 不得冒充股。這與「每分鐘一定有成交」是兩件事；零成交分鐘不可憑空補成交量。

| 來源／欄位 | 原始單位 | 可用的股數與證據 |
| --- | --- | --- |
| TWSE／TPEx 日 `成交股數` | 股 | `Trading_Volume` 直接保留股數；價格、金額與股數的量綱另做基本檢查。 |
| Shioaji 股票 K 棒 `Volume` | 提供者原生，歷史上不能預設全為張 | 每根以 `Amount / (Volume × OHLC)` 驗證倍率後記錄 `source_volume_multiplier`、`volume_shares`、`volume_unit_proof`；無法證明的正交易列不可進每日聚合。原始 `Volume` 不改。 |
| FinLab 股票 Tick／分鐘 `volume` | 提供者原生 | 一般交易時段的整日原值總和，須與雜湊已驗證的 Shioaji 同檔同日 K 棒吻合，且該 K 棒倍率由金額／OHLC 證明，才填 `volume_shares`。盤後、零股等其他時段保持 null，另行驗證。原始值與舊物件保留。 |
| FinLab 寬表 `...成交股數`／`...持有股數` | 欄名聲稱股 | 收據記錄 `source_unit=shares`；這是欄位語義，不代表已核對每一日來源報表。比率、占比欄位不當股數。 |
| FinLab 寬表 `...成交張數` | 股票交易張數／可轉債張數 | 收據明確區分。未確認商品及歷史交易單位時不得盲目乘 1,000；盤後定價交易可用 `load_fixed_price_shares(root, symbol)` 同日同檔成交價與金額逐筆反推股數，無法證明則 null。可轉債張數不是股票股數。 |
| FinMind 股票日／週／月 `Trading_Volume`／`trading_volume` | 官方文件標示股 | 原值即股；新收據記錄欄位單位。 |
| FinMind `TaiwanStockLoanCollateralBalance` 部分數量欄位 | 千股 | 保留原值，新增 `*_shares = 原值 × 1,000`；非整股／無效數字拒絕。 |
| FinMind `TaiwanStockStatisticsOfOrderBookAndTrade` | 委託／成交數量為張；成交額為百萬元；委託／成交筆數為筆 | 新增 `TotalBuyVolume_shares`、`TotalSellVolume_shares`、`TotalDealVolume_shares`（原值 × 1,000）及 `TotalDealMoney_twd`（原值 × 1,000,000）。原始欄位及筆數不改；缺值、負值、非整股及溢位不能標為完成。 |
| FinMind 上市／上櫃 Tick／KBar `volume` | 文件標示張；興櫃標示股 | 市場與歷史交易單位未逐筆確認前，不能全市場一律乘 1,000。目前未把這類欄位當成已證明股數。 |
| 期貨、選擇權、加密貨幣 `volume` | 口數、基礎幣、合約或提供者原生 | 不屬於股票股數；須按商品另外定義，絕不乘股數倍率。 |
| OKX 永續 `Trading_Volume` | 報價幣成交額 | 保留既有 quote-currency ABI；`okx_volume_contract`、`okx_volume_base`、`okx_volume_quote` 分開。移除報價幣缺值時以合約口數替代的錯誤；缺值／非有限值／負值拒收，真正的 0 可保留。 |

實際例子：2026-09-24 的 2330，FinLab 一般交易 Tick 原始量合計 12,990，Shioaji K 棒 `Volume` 合計也為 12,990；Shioaji 金額／價格指向每原始單位 1,000 股。FinLab 盤後定價寬表 2330 是 15 張、成交價 2,475、成交金額 37,125,000，獨立反推 15,000 股。這不代表日 `Trading_Volume` 必須等於 Tick 合計：交易時段、零股、大宗等範圍不同。

驗證與殘餘限制：

- `run_fintech_python scripts/download_finlab_history.py audit-volume-units --output-root data_finlab` 只查本機，對相關寬表檢查檔案 SHA-256、列數並更新單位收據，不消耗 FinLab 配額。
- `data_finlab/intraday/receipts` 的 `volume_shares` 與 `canonical_volume_scope=regular_session_only` 表示已驗證的一般交易時段股數；`intraday/derived_minute/receipts` 是從該 Tick 產生的分鐘線。舊原始 Parquet／收據版本仍保留。
- FinLab 其他標示「張數」但無同日金額／價格或逐檔交易單位證據的資料，維持 `canonical_unit=null`；不可因欄名相似強制合併或投入股數特徵。任何新來源都須先加入明確單位／標的／時段契約。
- 本契約只解決量綱與來源證據；不代表資料已通過發布時間、完整性、PIT、跨源同口徑等訓練驗收。

## 2026-09-27 本機驗證

- 永豐分鐘研究資料：2020-03-02～2026-09-24，1,602 個交易日分區、313,047,232 列。使用共用量綱推導逐列重算，既有股數錯誤、倍率變更、未解決列均為 0，因此不重寫這批資料。此專項掃描投影必要欄位，檢查每檔 size／mtime／inode 與 manifest 穩定，**沒有重算全部來源檔案 SHA-256**。證據：`artifacts/data_quality/shioaji_stock_share_units_2026-09-27.json`。
- FinLab：90 個 Tick 分區（58,376 列）、91 個 provider-minute 分區（6,622 列）、90 個 Tick 衍生分鐘分區（6,578 列），合計 271 個收據已升級。稽核重新驗證 SHA-256／大小／列數、91 組獨立 KBar 證據、90 組衍生來源關聯與 388 個保留的原始版本；原始欄位未改。58,350 筆一般時段 Tick 為已證明股數，另外 26 筆其他時段 Tick 的股數保持 null。證據：`artifacts/data_quality/finlab_intraday_share_units/latest.json`。
- FinLab 相關寬表：112 個本機收據已核對並註記單位，0 個失敗。這不是將所有「張數」寬表整批改寫；盤後定價股數仍由經驗證的按需 view 提供，其他未證明欄位保留原值及未知單位。
- FinMind 每 5 秒委託／成交統計：5,343 個既有分區、10,855,833 列全部完成本機單位補強，0 個失敗、0 次 API 請求。獨立驗收逐一檢查 21,372 個原／新來源檔與收據，重新核對 SHA、所有原始 Arrow 欄位與逐列 Decimal 換算。4,365 個原本 complete 與 978 個原本 partial 的狀態沒有改標。來源原值、筆數與發布／取得時間不因此次單位換算而改寫。原 Parquet 共 325,392,925 bytes，新版含 canonical 欄共 590,778,652 bytes，原版另行保留。證據：`artifacts/data_quality/finmind_order_book_share_units/latest.json`。
- FinMind free／complement／Sponsor 三項下載服務已重新載入（free 於 2026-09-27 00:27 台北時間恢復）；Sponsor 實際新下載的借貸擔保品分區已帶有 34 個 `*_shares` 及千股單位證據。這是新碼已運作的證據，不是全資料已抓完的宣稱。
- OKX：對 451 個本機分鐘 Parquet、29,048,995 列投影檢查四個量欄，`Trading_Volume != okx_volume_quote`、非有限／負／缺值均為 0；本次沒有重寫既有 OKX 資料，也沒有把其 ABI 改為基礎幣。

新增資料的收據會攜帶單位證據；FinLab 的來源收據 revision 變更時才重驗未解決量欄，避免每輪重讀 Tick。聚合時任何未知股數不可被 sum 當成 0 後假裝完整。回放／稽核移除 `raw Volume × 預設 1,000` 的容量捷徑；本次未重播任何交易帳本，亦未重啟交易／報價服務。

可重跑的本機指令（先 `source scripts/runtime_env.sh`）：

```bash
run_fintech_python scripts/download_finlab_history.py audit-volume-units --output-root data_finlab
run_fintech_python scripts/audit_finlab_intraday_share_units.py --output artifacts/data_quality/finlab_intraday_share_units/latest.json
run_fintech_python scripts/audit_finmind_order_book_share_units.py --output artifacts/data_quality/finmind_order_book_share_units/latest.json
```

FinMind 的既有分區遷移必須先停同一個 free worker，取得既有 `worker.lock` 後執行：

```bash
run_fintech_python -m downloader.download_finmind_free --root data_finmind --normalize-units-local
```

此模式不建立 API session，不消耗配額。原始 Parquet 與收據先以 SHA-256 命名保留在 `versions/`，才原子更新 canonical 分區。遇到不符雜湊、無效值或無法驗證的舊資料會保留原檔並報錯；原本 partial 不因單位正規化變為 complete。結果保留在 `data_finmind/unit_normalization/TaiwanStockStatisticsOfOrderBookAndTrade.json`。

若程序在 Parquet 已替換、receipt 尚未更新時中斷，重跑僅在完整舊封存能重建出與現有 Parquet 全值／型別／列序／metadata 相同的唯一結果時補完 receipt，並記錄 `recovered_interrupted` 與雙方 SHA。不能把一般損壞、遭修改的資料或無來源收據的檔案當成中斷恢復；任何不吻合仍明確失敗。

## 官方依據

- [FinLab 日內資料的 provider-native 單位與時段驗證範圍](https://finlab.finance/docs/en/details/intraday/)
- [FinLab 借券五欄的股數定義](https://finlab.finance/data/securities-lending)
- [FinMind 技術面：日價、Tick、分鐘及每 5 秒量值單位](https://finmind.github.io/tutor/TaiwanMarket/Technical/)
- [FinMind 籌碼面：千股擔保品與鉅額交易股數](https://finmind.github.io/tutor/TaiwanMarket/Chip/)
- [OKX K 線三種不同的成交量單位](https://www.okx.com/docs-v5/en/#rest-api-market-data-get-candlesticks)
