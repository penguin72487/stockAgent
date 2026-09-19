# 臺灣官方歷史資料回補與每日更新

更新於 2026-09-17。範圍是本專案模型會使用的臺灣市場、衍生品與總體公開來源；「網站列出資料」、「舊期數值」、「原始發布版本」分別驗收。`scripts/audit_tw_public_source_catalog.py --format json` 是可重跑清冊，本文數字是此日的收據快照。

本機 `tw-public` 清冊此時有 168 個產品：11 個官方逐日歷史查詢、2 個下市列表、9 個可查舊期但值可能修訂的整包表、137 個僅從首次擷取起有版本的快照，以及 9 個獨立補充／原稿產品。商品名稱與網址逐筆見清冊；這個數目不是 168 個完整歷史庫。

## 有明確歷史查詢或逐期原稿的來源

| 原始粒度 | 官方起點或目前可見起點 | 本機進度 | 後續更新與限制 |
|---|---|---|---|
| TWSE 個股日價、估值、融資融券、三大法人、當沖資格與市場指數（6 組） | 各組分別自 2001-01-01 至 2014-01-06 起；日價自 [2004-02-11](https://www.twse.com.tw/zh/trading/historical/mi-index.html) | 六組 `coverage_complete=true`、至 2026-09-17 缺日 0 | 既有 `download_tw_public_data.py` 逐日查詢、重疊更新及原始回應收據；`twse_market_index` 這支 `type=IND` 查詢的實際起點是 2009-01-05，2004 年呼叫回傳空列，不可直接把頁面標示的 2004 起點當成它的覆蓋；日內使用仍按實際公告時鐘 |
| TWSE 加權指數單獨歷史 | 本機 `twse_taiex_ohlc.summary.json` 自 1999-01-05 | 至 2026-09-17，6,866 個日值，`coverage_complete=true` | 與上一列的 `type=IND` 綜合市場指數端點分開驗收 |
| TPEx 個股日價、估值、融資融券、三大法人、當沖資格（5 組） | 日價自 [2003-08](https://www.tpex.org.tw/en-us/mainboard/trading/info/pricing.html)；其餘見來源清冊 | 五組各自 `coverage_complete=true`、缺日 0，均截至 2026-09-17；11 組共同截至 09-17 | 當沖資格官網 09-17 原始 JSON 已核對並補入正式 Parquet，不將隔日未發布名單提前宣稱為現值 |
| 上市／上櫃下市歷史清單（2 組） | 本機設定起點 2001、1994 | 有檔案及歷史列表；逐檔生存期須另核對 | 快照列表可補下市偏差，但不是每次公告更正的版本 |
| DGBAS CPI、失業率、GDP 與 CBC 外匯存底、貨幣新聞稿（3 庫） | 官網可見期別各異；CBC 貨幣自 1999-12 | 原稿庫 649、317、321 篇已封存；貨幣期別仍缺 2000-06 | `stockagent-tw-public-release-archives.timer` 日常增量、週日完整索引重掃；今日下載的舊原稿位元組不是已證明的首發位元組 |
| CBC USD/TWD 年度每日表、隔夜拆款率每日頁 | 本機觀測分別自 2000-01-03、2002-05-02 | 分別有 6,477、6,071 列及原始頁收據 | 可取得現行官網的舊日期數值；首次公開版本與更正紀錄另驗 |
| MOF 進出口與賦稅逐月新聞稿 | 官網索引可見貿易 2016-01、賦稅 2014-01 | 貿易 128 期、賦稅 149 期原稿已封存；賦稅網站新聞列表漏列的 17 期中，14 期由官方 PDF 補回，仍缺 2018-06、2018-08、2019-07 當期 PDF | 新 `download_tw_mof_release_archive.py` 封存 277 期原始 PDF、HTML 與 SHA-256；每日重查近期，週日重查全部。缺期仍顯示為缺期，PDF 的當前位元組不可倒填為歷史首發版本 |
| TAIFEX 日契約、TX 日盤、TXO 每日全鏈 | 本機合約日資料自 1998-07、選擇權全鏈自 2001-12 | 各有專用資料與收據，按產品及策略稽核 | 每日既有 TAIFEX 排程更新；三大法人、大額交易人公開免費查詢目前僅約三年滾動窗口，舊期不能由今日 OpenAPI 補出 |

## 不能當作完整歷史的來源

- `tw-public` 的 137 個當期快照只從第一次擷取起有版本；9 個整包歷史表雖可查舊期，數值可能已被修訂。兩者都不能直接當成當年模型可見的資料。
- [TWSE OpenAPI](https://openapi.twse.com.tw/v1/swagger.json) 及 [TPEx OpenAPI](https://www.tpex.org.tw/openapi/swagger.json) 的端點標題含「歷史」不保證無限期查詢。要以回傳列的首末日期、官方查詢參數、版本與收據逐端點驗證。
- [MOPS XBRL 季檔索引](https://mopsov.twse.com.tw/mops/web/t203sb02)列出 2009Q4–2026Q2 共 71 個已結束季度連結；只有索引不等於檔案已通過 ZIP、CRC、XML 事實數、公司覆蓋與更正版本稽核。批次自動下載仍以正式可用方式及授權為門檻；本機手動提供的 ZIP 可依既有 `import_tw_mops_xbrl_local.py` 稽核匯入。
- 交易所網站的公開瀏覽權不等於任意大量自動擷取權。對 TWSE 網頁及 MOPS 批次資料，遵守[交易所使用條款](https://www.twse.com.tw/zh/terms/use.html)與各資料集明示授權。

## 每日作業與驗收

1. `stockagent-tw-public-0830-check.timer` 和既有來源事件監測維持交易所逐日來源；日曆、原始回應、缺日及版次收據是驗收依據。
2. `stockagent-tw-public-release-archives.timer` 於交易日 07:00、16:30、19:30 更新總體原稿及 MOF 索引／PDF，週日 03:00 重掃舊期。正式 producer lock 忙碌時安全略過，由後續排程補跑。
3. `stockagent-taifex-public-history.timer` 維持 TAIFEX 滾動免費窗口與 OpenAPI 首次擷取後版本。`openapi_latest.json` 把捕獲與交由專用下載器處理的端點分開。
4. 以 `run_fintech_python scripts/audit_tw_public_source_catalog.py --format json` 查目前來源、缺口與收據。MOF 的 `state/mof_original_release_archive.json` 同時報 `indexed_releases`、`archived_releases`、`index_period_gaps`、`subject_unverified_releases`、`continuous_index_history`；`status=complete` 僅指已列出的稿件均封存，**不**代表索引沒有缺期或每篇 PDF 均能擷取並核對內文標題。
5. 訓練前仍要釘選完整資料 release 並跑選定設定的嚴格 PIT／模型稽核；原稿封存成功本身不會把新欄位自動升格為可訓練特徵。

執行入口：`source scripts/runtime_env.sh && run_fintech_python downloader/download_tw_mof_release_archive.py --output-dir /srv/stockagent-live/data_tw_public`。手動執行前先確認正式 producer lock 未被其他工作持有；一般使用交由上述 systemd 計時器協調。

若正式 producer lock 被長時間的匯入持有，可先在獨立目錄用相同索引回補原稿，再執行 `run_fintech_python scripts/promote_tw_mof_release_archive.py --stage 暫存目錄 --live /srv/stockagent-live/data_tw_public`。匯入器會重新核對索引內容、每份檔案雜湊及正式來源鎖；索引已增加或改版時會拒絕匯入，應由正式下載器依新索引補抓。
