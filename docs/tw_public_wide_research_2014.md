# 2014–2026 台股寬覆蓋研究特徵（與嚴格 PIT 實驗分開）

這個實驗的優先序是「盡可能使用**確實取得的歷史數值**」，不是只選原始版本／精確秒級公告都齊備的欄位。原本 27 欄的 `tw_public_preopen_raw_2014_v1.yaml` 和正式 `tw_public_stock_daily.parquet` 沒有被改成寬鬆資料；新實驗使用 `tw_public_preopen_wide_research_2014_v1.yaml` 與獨立 `tw_public_research_wide_2014_v1.parquet`。這仍是研究近似，不能聲稱為可執行開盤策略或原始歷史版本 PIT 回測。

## 值、時鐘與缺值分開

一個觀測有統計期間 `s`、該數值版本發布時間 `p`、本機觀測時間 `o`，三者不可互換。新實驗允許今日取得的舊期修訂值作為訓練輸入，但以原始公告日／官方發布規律／已標記推估日映射到 09:00 前首次可用的交易日；不把統計期末直接當公告日，也不額外延後幾天。時間估計不會令「今日修訂值」變成當年原稿。原稿可驗證時，正式表的原始值優先於總體整包修訂值；其餘標成 research-only。來源 SHA-256、合約版本、發布政策與來源檔清單保存在輸出旁的 `.research.json`。

- **每期沿用**：XBRL 財報、M1B/M2 水準、CPI/GDP 水準、MOF 月資料於推定發布後 forward-fill；不得在首次發布前倒灌。XBRL 只取報告**本期**、無維度、單位符合的原值；舊式 `{IASB namespace}Concept`、舊式 TIFRS 綜合損益表及新版 `ifrs-full:Concept` 均可讀。舊 TIFRS 的營業收入／毛利／營業利益獨立命名，不直接與新版 IFRS 欄位混成同一數值；這些舊分類欄最後一次觀測超過 400 個日曆日後停止沿用，`__available` 轉回 0，避免舊分類停用後把多年以前的財報當作現況。2014 年訓練窗之前已發布且仍在 400 日內的舊財報，以其真正來源日計齡後才帶入窗首，不把窗首當作新公告。400 日是明示的研究有效期假設，不是官方公告效期；M2 等一般狀態欄不受此限。損益流量分當季與年初迄今；第一季同一事實要同時填兩種粒度，不能把上一年 Q4 的 YTD 沿用到 Q1。分母為 0 時比率維持缺值。同一估計時間互相矛盾的 XBRL 數值不任選一筆；同日多個季度先在各自期別內計算比率，再取最新報告期。
- **真正事件**：注意／處置只用事件日的 1 旗標，不 forward-fill，也不把 2026 年開始捕捉前的 0 當作「沒有事件」。`*_source_covered` 只有 TWSE 與 TPEx 同次回應均實際捕捉的交易日才為 1；其他日期為未知，不能由缺事件推論未發生。這些事件源本機尚無 2014–2025 可驗證完整歷史，早期 fold 的 training-only RMS 會停用沒有見過的欄位。
- **一般稀疏值**：對 XBRL、M2 等主要稀疏欄與 PE/PB 加 `__available` 通道，在轉 Float32 並將剩餘缺值填 0 **之前**建立。真零 `(value=0, available=1)` 與未知 `(value=0, available=0)` 可分辨；需要下一交易日位移的特徵，其旗標同時位移。模型沿用現有 training-only、非中心化 RMS，不用 log／asinh，也不對保留的比率做回填或偽造事件。從未在該 fold 的訓練年出現的通道不在驗證／測試期間突然啟用。
- **資料形態**：來源 Parquet 保留 Float64 原值；模型張量按既有 ABI 為 Float32。極大金額會失去低位整數精度，不能宣稱逐位原樣，但沒有 log／asinh 壓縮。額外採 training-only RMS 控制量綱，並保留所有比率通道。

研究表目前含 122 個 `twpub_*` 值欄，其中 121 欄有觀測值；配置選入 93 個值欄（包括面板 OHLCV 5 欄），另加 50 個 availability 通道，共 143 個模型欄。2014 年已可觀測 78/88 個選用的外部欄，其餘 10 欄不能因晚近才出現而倒填。未選入的主要是 2026 年才開始有本機快照、對 2014–2025 訓練年無法學習的欄位；`twpub_cbc_overnight_rate_chg` 全空，官方成交股數對已核對的 2330 股票 5,566 個重疊交易日與面板 `trading_volume_raw` 完全相同，因此避免重複通道。研究表約 961 萬個稀疏日期／代號列；2014 年有 7,093 筆帶資產原值的 XBRL 事件列、6,802 筆流動比率事件列、12 期 M2 水準值。列數不是不同公司數，也不代表每家公司每季完整。原有 `twpub_*_log`、`*_logret*`、`*_asinh`、名稱雖無 suffix 但實際經 asinh 的資券／法人流量，以及實際為對數比的 `twpub_cbc_fx_reserves_chg`，不列入研究輸入；原始成交股數、資券張數、法人股數及比率保留。XBRL 另有資產、負債、權益、現金、存貨、營收、利潤、營業現金流及衍生比率。

## 發布依據與限制

[證券交易法第 36 條](https://twse-regulation.twse.com.tw/tw/law/DOC01_print.aspx?FLCODE=fl007009&FLNO=36)規定一般年度報告於會計年度終結後三個月內、第一至第三季於終結後 45 日內、上月營運於次月 10 日前申報；期限是**最晚申報日**，不能當作每家公司實際發布日。XBRL 使用本地逐公司董事會／申報候選日，未有精確申報時刻者視為當日開盤後、下一交易日可用；某些候選日期可能早於真正申報，仍有資訊洩漏風險。取得精確 MOPS 申報時間後應覆蓋估計值並重建研究表。

舊版欄位映射依實際下載的報表概念名稱、期間與單位核對；[證交所 XBRL 分類標準入口](https://www.twse.com.tw/rwd/XBRL/standard)亦列明各業別分類標準的查詢路徑。2014Q1 的 2330 實檔可核對營業收入、淨毛利、營業利益與每股盈餘，研究表的第一季日期為 2014-05-14，單季與 YTD 營收同為 148,215,172,000 元、每股盈餘同為 1.85 元；這驗證數值與粒度映射，不驗證該候選日就是精確申報時間。

[主計總處 2017 年公告](https://www.stat.gov.tw/News_Content.aspx?n=2642&s=97506)說明 2012-05-07 起部分統計在 08:30、2017-07-01 起統一為 16:00；[央行金融情況原稿](https://www.cbc.gov.tw/tw/cp-302-190942-d58df-1.html)可證月資料既有 16:20 預告。總體事件表優先採逐次原稿日期與時鐘，再用有標籤的發布規律推估。宏觀整包值可能修訂，這項研究假設需用歷史版本敏感度實驗檢驗。

月營收、TDCC、借券等 OpenAPI 本機目前主要只有 2026 年開始的回應快照；申報規則只能估日期，**不能生出 2014 年不存在的原值**。因此它們未加入當前 2014–2026 訓練白名單；XBRL 解決的是已有逐季檔的財報，不代表所有 MOPS 特徵都補齊。期交所多數來源亦僅晚近捕捉，待歷史封存下載後再開放，不把晚近缺值灌成 2014 年的 0。

## 建置、驗證、持續更新

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_tw_public_provisional_macro.py
run_fintech_python scripts/build_tw_public_research_features.py
run_fintech_python scripts/inventory_tw_public_feature_table.py \
  --feature-path data_tw_public/features/tw_public_research_wide_2014_v1.parquet \
  --source-schema --output-dir artifacts/data_quality/tw_public_research_2014_v1
run_fintech_python train.py --config configs/markets/tw_public_preopen_wide_research_2014_v1.yaml --check-data-only
```

同樣的來源雜湊與合約版本不變時，研究建置直接重用既有檔（不改 inode）；來源變動則原子替換完整研究表，不假設修訂只出現在尾端。既有台股特徵 reconcile timer 已安裝研究表更新步驟；它每日在 08:20、14:20、19:00（臺北時間）檢查，不在交易時段做全量重建。這個步驟現在先刷新 provisional macro ledger，再建研究表；官方發布封存下載器完成 MOF 封存後亦重建兩者，以免研究表落後一輪。研究表與 receipt 暫不納入 `tw-public` 嚴格冷發布，因其 XBRL 來源在冷庫 catalog 目前排除且研究 PIT 稽核與正式表不同。若要遠端重現，須先建立獨立、可重建且權限許可的 research 發布契約。

目前本機有 54 份可讀的歷史 XBRL 正規化季檔，但 `stockagent-tw-mops-xbrl.timer` **尚未安裝**；只有本地來源有新檔時，上述 reconcile 才會把新 XBRL 寫進研究表。自動取得後續季檔仍需依法定用途核對並提供下載器要求的授權聲明，然後執行 `sudo bash scripts/install_tw_mops_xbrl_service.sh /path/to/TWSE-authorization-attestation.json`。未滿足此門檻前，不能把「研究表會重建」誤述成「XBRL 已自動抓到最新」。

入口的 `--check-data-only` 只證明來源、面板、fold 能建立；不會啟動模型、最佳化器或 checkpoint。實際 fold 訓練與收益／數值穩定性尚需個別驗證。舊 TIFRS 400 日有效期也納入此研究配置的面板快取與 checkpoint 前處理契約，變更政策會使舊快取／checkpoint 不相容。舊 strict checkpoint 與本實驗特徵 ABI、模型和 data fingerprint 不相容，禁止續訓。

2026-09-18 本機實測：最終配置 3,102 交易日、2,755 代號、143 模型通道、12 folds 通過預檢；含窗前來源日與舊 TIFRS 有效期的新契約冷面板建置 407.3 秒，完全相同來源的快取載入 2.5 秒。不同冷建置批次曾測得約 290–407 秒，不能把單次差異歸因於某一程式碼變更；冷建置期間曾觀測到約 49 GB process RSS。這是後續若要降低尖峰記憶體，應優先量測與改善的成本，不可從快取命中速度推論完整訓練吞吐。仍需獨立驗證真正的 fold 訓練、數值穩定性、逐年缺值敏感度，以及新版／舊版 XBRL 跨分類標準可比性。

另有一個已觀測、尚未修正的來源收據性能缺口：01:53 完成的正式表收據已涵蓋 2026-09-17，但 02:12 到達的 2026-09-18 TAIFEX 期貨原檔讓正式表 `--dry-run` 再度回報 `rebuild=true`，即使本研究配置並未選用該檔產生的對數期貨欄。這是正式表「整檔 SHA」與建置截止日不同粒度造成的全歷史重建放大；後續應加入**截止日內、模型語意欄位**的來源收據，經差異驗證後才可免重建。現行排程會按完整性優先在下一輪處理；不能把研究面板的快取命中誤稱為所有來源已同步完成。
