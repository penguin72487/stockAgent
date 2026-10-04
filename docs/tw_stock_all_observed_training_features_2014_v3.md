# 台股完整已觀測研究訓練特徵（v3）

資料表：`artifacts/research_features/tw_public_research_all_2014_v3.parquet`；研究配置：`configs/markets/tw_public_preopen_all_observed_research_2014_v3.yaml`。
本表將既有 266 個去重名稱、10 個期交所新增欄，以及模型需要的 154 個新增可用性旗標合併；所有名稱只列一次。

- 來源列數：9,617,311，2014 年列數：385,880；來源鍵為 `(date, symbol)`。
- 建表契約：v2；輸出 SHA-256：`ad4b955355ae8ffe8b3eb789a8e8daf455af7e9658f2ec1b3f01e7cdbaa9cad4`；輸入 SHA-256 與路徑見同名 `.all_features.json` 收據。
- 來源日期：1999-01-05～2026-09-18；來源含 3,766 個代號（含 `__MARKET__`），模型面板的實際股票數以預檢輸出為準。
- 公開資料值欄：204；2014 年有實值：127；2014 年尚無實值但後續有值：77。
- 晚起始 77 欄按原來源路線：D1 3、E1 8、E2 11、M1 7、M2 17、M3 12、S1 3、T1 10、T2 6；本 ABI 保留它們的後續實值與缺值旗標。
- 2014 年的 127 個值欄分解為：原正式表 58、僅研究表 44、由原值重算 15、期交所新補 10。
- 模型欄：204 個公開值 + 204 個可用性旗標 + 20 個股票面板欄 = 428。`next_session_open_gap_logret` 與 `next_session_1325_gap_logret` 分屬不同決策時鐘，不屬此開盤前研究 ABI。
- 15 個舊衍生欄已從研究原值重算，44 個原本僅研究表有 2014 值的欄位亦保留。原始缺值維持 null，模型在產生可用性旗標後才轉零；較晚開始的欄位在先前日期的旗標為 0。
- 本配置接受現修宏觀值、推測公告日與僅有捕獲日期的快照作研究輸入。這只證明資料可讀，不證明歷史 PIT、完整來源覆蓋或可執行報酬。快照值不得倒填到捕獲日之前。
- 在 09:00 研究時鐘，沒有可靠盤中公告時間的當期快照再向後移一個交易日。僅從 2026 才開始觀測的欄位在先前訓練 fold 未見過，不應把其已列入配置誤說成模型已學到該訊號。
- XBRL 舊 `tifrs` 概念在來源後僅延續 400 曆日。期交所 v2 官方值已對齊下個可用交易日；模型不可再把這些欄當同日 09:00 可見。
- 下列「2014 原始格」與「全期非空」計數是來源表的觀測格，不是前向保留後的模型張量格，也不是逐公司完整率。來源日期多為估計可用日，詳見原始來源收據及 `docs/tw_public_wide_research_2014.md`。
- 原清單欄位的首次／最新日期沿用 2026-09-18 來源盤點；15 個重算欄與 10 個期交所新欄則直接取 v3 的市場觀測日期。回補後若來源新增新日期，需重跑來源盤點與本報告。
- v3 目前在本機 `artifacts/research_features` 工作區；本頁的建表與資料預檢不等於冷庫發布、遠端同步或 GPU 訓練完成。

## 15 個原值重算欄

GDP、CPI、M1B、M2、美元匯率用正值自然對數；各稅、進出口用正值 `log(1+x)`；貿易出超用 `asinh(x/1,000,000)`；美元匯率報酬用相鄰有效觀測值的對數比；隔夜利率直接使用已是小數利率的研究原值，變動量取相鄰有效觀測差。非有限值或對數定義域外維持缺值。

| 衍生特徵 | 原值 | 2014 有效格 | 全期非空格 |
|---|---|---:|---:|
| `twpub_dgbas_gdp_log` | `twpub_dgbas_gdp_raw` | 4 | 55 |
| `twpub_dgbas_cpi_log` | `twpub_dgbas_cpi_raw` | 12 | 165 |
| `twpub_usdtwd_log` | `twpub_usdtwd_raw` | 248 | 3,349 |
| `twpub_usdtwd_logret_1d` | `twpub_usdtwd_raw` | 248 | 3,348 |
| `twpub_cbc_overnight_rate` | `twpub_cbc_overnight_pct_raw` | 248 | 3,349 |
| `twpub_cbc_overnight_rate_chg` | `twpub_cbc_overnight_pct_raw` | 248 | 3,348 |
| `twpub_cbc_m1b_log` | `twpub_cbc_m1b_raw` | 12 | 165 |
| `twpub_cbc_m2_log` | `twpub_cbc_m2_raw` | 12 | 165 |
| `twpub_mof_business_tax_log` | `twpub_mof_business_tax_raw` | 7 | 99 |
| `twpub_mof_export_log` | `twpub_mof_export_raw` | 11 | 152 |
| `twpub_mof_futures_tax_log` | `twpub_mof_futures_tax_raw` | 12 | 165 |
| `twpub_mof_import_log` | `twpub_mof_import_raw` | 11 | 152 |
| `twpub_mof_securities_tax_log` | `twpub_mof_securities_tax_raw` | 12 | 165 |
| `twpub_mof_tax_total_log` | `twpub_mof_tax_total_raw` | 12 | 165 |
| `twpub_mof_trade_balance_asinh` | `twpub_mof_trade_balance_raw` | 11 | 152 |

## 所有去重名稱

| 特徵 | 來源/角色 | 首次來源日期 | 最新來源日期 | 2014 原始格 | 全期非空格 | 此 ABI |
|---|---|---|---|---:|---:|---|
| `body_ratio` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `close_logret_1d` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `close_raw` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `clv` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `clv_centered` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `delta_body_ratio` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `delta_clv` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `high_raw` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `low_raw` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `lower_shadow` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `max_logret_1d` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `min_logret_1d` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `next_session_1325_gap_logret` | 模式專用 | 2020-03-02 | 2026-09-18 | — | — | 僅專用時鐘 |
| `next_session_open_gap_logret` | 模式專用 | 面板計算 | 面板計算 | — | — | 僅專用時鐘 |
| `open_logret_1d` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `open_raw` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `shadow_imbalance` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `signed_body_ratio` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `signed_vol` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `trading_volume_logret_1d` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `trading_volume_raw` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |
| `twpub_attention_close_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 622 | 使用 |
| `twpub_attention_close_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 622 | 使用；基礎值缺時 0 |
| `twpub_attention_count_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 622 | 使用 |
| `twpub_attention_count_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 622 | 使用；基礎值缺時 0 |
| `twpub_attention_event_flag` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 622 | 使用 |
| `twpub_attention_event_flag__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 622 | 使用；基礎值缺時 0 |
| `twpub_attention_pe_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 502 | 使用 |
| `twpub_attention_pe_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 502 | 使用；基礎值缺時 0 |
| `twpub_attention_source_covered` | 待回補 | 2026-07-20 | 2026-09-17 | 0 | 26 | 使用 |
| `twpub_attention_source_covered__available` | 可用性旗標 | 2026-07-20 | 2026-09-17 | 0 | 26 | 使用；基礎值缺時 0 |
| `twpub_borrow_available_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 58,468 | 使用 |
| `twpub_borrow_available_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 58,468 | 使用；基礎值缺時 0 |
| `twpub_cbc_fx_reserves_chg` | 正式表已有 2014 | 2000-06-08 | 2026-09-17 | 12 | 317 | 使用 |
| `twpub_cbc_fx_reserves_chg__available` | 可用性旗標 | 2000-06-08 | 2026-09-17 | 12 | 317 | 使用；基礎值缺時 0 |
| `twpub_cbc_fx_reserves_log` | 正式表已有 2014 | 2000-05-30 | 2026-09-17 | 12 | 318 | 使用 |
| `twpub_cbc_fx_reserves_log__available` | 可用性旗標 | 2000-05-30 | 2026-09-17 | 12 | 318 | 使用；基礎值缺時 0 |
| `twpub_cbc_fx_reserves_usd_billion_raw` | 正式表已有 2014 | 2000-05-30 | 2026-09-17 | 12 | 318 | 使用 |
| `twpub_cbc_fx_reserves_usd_billion_raw__available` | 可用性旗標 | 2000-05-30 | 2026-09-17 | 12 | 318 | 使用；基礎值缺時 0 |
| `twpub_cbc_m1b_log` | 原值重算 | 2013-01-28 | 2026-09-17 | 12 | 165 | 使用 |
| `twpub_cbc_m1b_log__available` | 可用性旗標 | 2013-01-28 | 2026-09-17 | 12 | 165 | 使用；基礎值缺時 0 |
| `twpub_cbc_m1b_raw` | 僅研究表已有 2014 | 2013-01-28 | 2026-09-17 | 12 | 165 | 使用 |
| `twpub_cbc_m1b_raw__available` | 可用性旗標 | 2013-01-28 | 2026-09-17 | 12 | 165 | 使用；基礎值缺時 0 |
| `twpub_cbc_m1b_yoy` | 正式表已有 2014 | 2000-01-26 | 2026-09-17 | 12 | 320 | 使用 |
| `twpub_cbc_m1b_yoy__available` | 可用性旗標 | 2000-01-26 | 2026-09-17 | 12 | 320 | 使用；基礎值缺時 0 |
| `twpub_cbc_m1b_yoy_pct_raw` | 正式表已有 2014 | 2000-01-26 | 2026-09-17 | 12 | 320 | 使用 |
| `twpub_cbc_m1b_yoy_pct_raw__available` | 可用性旗標 | 2000-01-26 | 2026-09-17 | 12 | 320 | 使用；基礎值缺時 0 |
| `twpub_cbc_m2_log` | 原值重算 | 2013-01-28 | 2026-09-17 | 12 | 165 | 使用 |
| `twpub_cbc_m2_log__available` | 可用性旗標 | 2013-01-28 | 2026-09-17 | 12 | 165 | 使用；基礎值缺時 0 |
| `twpub_cbc_m2_raw` | 僅研究表已有 2014 | 2013-01-28 | 2026-09-17 | 12 | 165 | 使用 |
| `twpub_cbc_m2_raw__available` | 可用性旗標 | 2013-01-28 | 2026-09-17 | 12 | 165 | 使用；基礎值缺時 0 |
| `twpub_cbc_m2_yoy` | 正式表已有 2014 | 2000-01-26 | 2026-09-17 | 12 | 320 | 使用 |
| `twpub_cbc_m2_yoy__available` | 可用性旗標 | 2000-01-26 | 2026-09-17 | 12 | 320 | 使用；基礎值缺時 0 |
| `twpub_cbc_m2_yoy_pct_raw` | 正式表已有 2014 | 2000-01-26 | 2026-09-17 | 12 | 320 | 使用 |
| `twpub_cbc_m2_yoy_pct_raw__available` | 可用性旗標 | 2000-01-26 | 2026-09-17 | 12 | 320 | 使用；基礎值缺時 0 |
| `twpub_cbc_overnight_pct_raw` | 僅研究表已有 2014 | 2013-01-02 | 2026-09-18 | 248 | 3,349 | 使用 |
| `twpub_cbc_overnight_pct_raw__available` | 可用性旗標 | 2013-01-02 | 2026-09-18 | 248 | 3,349 | 使用；基礎值缺時 0 |
| `twpub_cbc_overnight_rate` | 原值重算 | 2013-01-02 | 2026-09-18 | 248 | 3,349 | 使用 |
| `twpub_cbc_overnight_rate__available` | 可用性旗標 | 2013-01-02 | 2026-09-18 | 248 | 3,349 | 使用；基礎值缺時 0 |
| `twpub_cbc_overnight_rate_chg` | 原值重算 | 2013-01-03 | 2026-09-18 | 248 | 3,348 | 使用 |
| `twpub_cbc_overnight_rate_chg__available` | 可用性旗標 | 2013-01-03 | 2026-09-18 | 248 | 3,348 | 使用；基礎值缺時 0 |
| `twpub_company_age_years` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用 |
| `twpub_company_age_years__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用；基礎值缺時 0 |
| `twpub_company_has_preferred_stock` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用 |
| `twpub_company_has_preferred_stock__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用；基礎值缺時 0 |
| `twpub_company_industry_code` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用 |
| `twpub_company_industry_code__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用；基礎值缺時 0 |
| `twpub_company_is_foreign` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用 |
| `twpub_company_is_foreign__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用；基礎值缺時 0 |
| `twpub_company_issued_shares_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用 |
| `twpub_company_issued_shares_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用；基礎值缺時 0 |
| `twpub_company_listed_age_years` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用 |
| `twpub_company_listed_age_years__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用；基礎值缺時 0 |
| `twpub_company_paidin_capital_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用 |
| `twpub_company_paidin_capital_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 55,402 | 使用；基礎值缺時 0 |
| `twpub_company_par_value_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 56 | 使用 |
| `twpub_company_par_value_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 56 | 使用；基礎值缺時 0 |
| `twpub_company_private_shares_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 7,541 | 使用 |
| `twpub_company_private_shares_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 7,541 | 使用；基礎值缺時 0 |
| `twpub_cumulative_revenue_yoy` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 51,188 | 使用 |
| `twpub_cumulative_revenue_yoy__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 51,188 | 使用；基礎值缺時 0 |
| `twpub_dealer_net_buy_flow` | 正式表已有 2014 | 2004-06-02 | 2026-09-18 | 302,815 | 5,835,024 | 使用 |
| `twpub_dealer_net_buy_flow__available` | 可用性旗標 | 2004-06-02 | 2026-09-18 | 302,815 | 5,835,024 | 使用；基礎值缺時 0 |
| `twpub_dealer_net_buy_shares_raw` | 正式表已有 2014 | 2004-06-02 | 2026-09-18 | 302,815 | 5,835,024 | 使用 |
| `twpub_dealer_net_buy_shares_raw__available` | 可用性旗標 | 2004-06-02 | 2026-09-18 | 302,815 | 5,835,024 | 使用；基礎值缺時 0 |
| `twpub_dgbas_cpi_log` | 原值重算 | 2013-01-07 | 2026-09-09 | 12 | 165 | 使用 |
| `twpub_dgbas_cpi_log__available` | 可用性旗標 | 2013-01-07 | 2026-09-09 | 12 | 165 | 使用；基礎值缺時 0 |
| `twpub_dgbas_cpi_raw` | 僅研究表已有 2014 | 2013-01-07 | 2026-09-09 | 12 | 165 | 使用 |
| `twpub_dgbas_cpi_raw__available` | 可用性旗標 | 2013-01-07 | 2026-09-09 | 12 | 165 | 使用；基礎值缺時 0 |
| `twpub_dgbas_cpi_yoy` | 正式表已有 2014 | 2004-02-06 | 2026-09-17 | 12 | 273 | 使用 |
| `twpub_dgbas_cpi_yoy__available` | 可用性旗標 | 2004-02-06 | 2026-09-17 | 12 | 273 | 使用；基礎值缺時 0 |
| `twpub_dgbas_cpi_yoy_pct_raw` | 正式表已有 2014 | 2004-02-06 | 2026-09-09 | 12 | 272 | 使用 |
| `twpub_dgbas_cpi_yoy_pct_raw__available` | 可用性旗標 | 2004-02-06 | 2026-09-09 | 12 | 272 | 使用；基礎值缺時 0 |
| `twpub_dgbas_gdp_log` | 原值重算 | 2013-01-31 | 2026-08-03 | 4 | 55 | 使用 |
| `twpub_dgbas_gdp_log__available` | 可用性旗標 | 2013-01-31 | 2026-08-03 | 4 | 55 | 使用；基礎值缺時 0 |
| `twpub_dgbas_gdp_raw` | 僅研究表已有 2014 | 2013-01-31 | 2026-08-03 | 4 | 55 | 使用 |
| `twpub_dgbas_gdp_raw__available` | 可用性旗標 | 2013-01-31 | 2026-08-03 | 4 | 55 | 使用；基礎值缺時 0 |
| `twpub_dgbas_gdp_yoy` | 正式表已有 2014 | 2012-02-01 | 2026-09-17 | 8 | 119 | 使用 |
| `twpub_dgbas_gdp_yoy__available` | 可用性旗標 | 2012-02-01 | 2026-09-17 | 8 | 119 | 使用；基礎值缺時 0 |
| `twpub_dgbas_gdp_yoy_pct_raw` | 正式表已有 2014 | 2012-02-01 | 2026-08-17 | 8 | 118 | 使用 |
| `twpub_dgbas_gdp_yoy_pct_raw__available` | 可用性旗標 | 2012-02-01 | 2026-08-17 | 8 | 118 | 使用；基礎值缺時 0 |
| `twpub_dgbas_unemployment_pct_raw` | 正式表已有 2014 | 2005-03-01 | 2026-08-25 | 12 | 259 | 使用 |
| `twpub_dgbas_unemployment_pct_raw__available` | 可用性旗標 | 2005-03-01 | 2026-08-25 | 12 | 259 | 使用；基礎值缺時 0 |
| `twpub_dgbas_unemployment_rate` | 正式表已有 2014 | 2005-03-01 | 2026-09-17 | 12 | 260 | 使用 |
| `twpub_dgbas_unemployment_rate__available` | 可用性旗標 | 2005-03-01 | 2026-09-17 | 12 | 260 | 使用；基礎值缺時 0 |
| `twpub_disposal_count_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 510 | 使用 |
| `twpub_disposal_count_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 510 | 使用；基礎值缺時 0 |
| `twpub_disposal_event_flag` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 510 | 使用 |
| `twpub_disposal_event_flag__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 510 | 使用；基礎值缺時 0 |
| `twpub_disposal_source_covered` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 27 | 使用 |
| `twpub_disposal_source_covered__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 27 | 使用；基礎值缺時 0 |
| `twpub_dividend_board_approved` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 49,503 | 使用 |
| `twpub_dividend_board_approved__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 49,503 | 使用；基礎值缺時 0 |
| `twpub_dividend_cash_per_share` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 49,503 | 使用 |
| `twpub_dividend_cash_per_share__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 49,503 | 使用；基礎值缺時 0 |
| `twpub_dividend_confirmed` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 29,249 | 使用 |
| `twpub_dividend_confirmed__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 29,249 | 使用；基礎值缺時 0 |
| `twpub_dividend_per_share_log` | 正式表已有 2014 | 2007-01-02 | 2026-09-18 | 104,052 | 2,298,625 | 使用 |
| `twpub_dividend_per_share_log__available` | 可用性旗標 | 2007-01-02 | 2026-09-18 | 104,052 | 2,298,625 | 使用；基礎值缺時 0 |
| `twpub_dividend_per_share_raw` | 正式表已有 2014 | 2007-01-02 | 2026-09-18 | 164,313 | 3,383,025 | 使用 |
| `twpub_dividend_per_share_raw__available` | 可用性旗標 | 2007-01-02 | 2026-09-18 | 164,313 | 3,383,025 | 使用；基礎值缺時 0 |
| `twpub_dividend_stock_per_share` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 49,503 | 使用 |
| `twpub_dividend_stock_per_share__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 49,503 | 使用；基礎值缺時 0 |
| `twpub_dividend_total_cash_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 38,166 | 使用 |
| `twpub_dividend_total_cash_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 38,166 | 使用；基礎值缺時 0 |
| `twpub_dividend_total_stock_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 5,641 | 使用 |
| `twpub_dividend_total_stock_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 5,641 | 使用；基礎值缺時 0 |
| `twpub_dividend_yield` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 373,674 | 8,213,165 | 使用 |
| `twpub_dividend_yield__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 373,674 | 8,213,165 | 使用；基礎值缺時 0 |
| `twpub_dividend_yield_pct_raw` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 373,674 | 8,213,165 | 使用 |
| `twpub_dividend_yield_pct_raw__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 373,674 | 8,213,165 | 使用；基礎值缺時 0 |
| `twpub_exdiv_cash_dividend` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 1,066 | 使用 |
| `twpub_exdiv_cash_dividend__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 1,066 | 使用；基礎值缺時 0 |
| `twpub_exdiv_known` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 1,072 | 使用 |
| `twpub_exdiv_known__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 1,072 | 使用；基礎值缺時 0 |
| `twpub_exdiv_stock_dividend_ratio` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 1,072 | 使用 |
| `twpub_exdiv_stock_dividend_ratio__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 1,072 | 使用；基礎值缺時 0 |
| `twpub_exdiv_subscription_price_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 39 | 使用 |
| `twpub_exdiv_subscription_price_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 39 | 使用；基礎值缺時 0 |
| `twpub_exdiv_subscription_ratio` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 41 | 使用 |
| `twpub_exdiv_subscription_ratio__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 41 | 使用；基礎值缺時 0 |
| `twpub_financial_assets_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 48,627 | 使用 |
| `twpub_financial_assets_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 48,627 | 使用；基礎值缺時 0 |
| `twpub_financial_book_value_per_share_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 48,627 | 使用 |
| `twpub_financial_book_value_per_share_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 48,627 | 使用；基礎值缺時 0 |
| `twpub_financial_current_ratio` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 48,169 | 使用 |
| `twpub_financial_current_ratio__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 48,169 | 使用；基礎值缺時 0 |
| `twpub_financial_debt_ratio` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 48,627 | 使用 |
| `twpub_financial_debt_ratio__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 48,627 | 使用；基礎值缺時 0 |
| `twpub_financial_eps` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 48,621 | 使用 |
| `twpub_financial_eps__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 48,621 | 使用；基礎值缺時 0 |
| `twpub_financial_equity_ratio` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 48,627 | 使用 |
| `twpub_financial_equity_ratio__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 48,627 | 使用；基礎值缺時 0 |
| `twpub_financial_gross_margin` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 47,861 | 使用 |
| `twpub_financial_gross_margin__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 47,861 | 使用；基礎值缺時 0 |
| `twpub_financial_net_income_asinh` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 48,621 | 使用 |
| `twpub_financial_net_income_asinh__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 48,621 | 使用；基礎值缺時 0 |
| `twpub_financial_net_margin` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 48,501 | 使用 |
| `twpub_financial_net_margin__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 48,501 | 使用；基礎值缺時 0 |
| `twpub_financial_operating_margin` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 48,111 | 使用 |
| `twpub_financial_operating_margin__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 48,111 | 使用；基礎值缺時 0 |
| `twpub_financial_revenue_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 48,499 | 使用 |
| `twpub_financial_revenue_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 48,499 | 使用；基礎值缺時 0 |
| `twpub_foreign_net_buy_flow` | 正式表已有 2014 | 2004-06-02 | 2026-09-18 | 302,815 | 5,835,024 | 使用 |
| `twpub_foreign_net_buy_flow__available` | 可用性旗標 | 2004-06-02 | 2026-09-18 | 302,815 | 5,835,024 | 使用；基礎值缺時 0 |
| `twpub_foreign_net_buy_shares_raw` | 正式表已有 2014 | 2004-06-02 | 2026-09-18 | 302,815 | 5,835,024 | 使用 |
| `twpub_foreign_net_buy_shares_raw__available` | 可用性旗標 | 2004-06-02 | 2026-09-18 | 302,815 | 5,835,024 | 使用；基礎值缺時 0 |
| `twpub_insider_holdings_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 51,272 | 使用 |
| `twpub_insider_holdings_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 51,272 | 使用；基礎值缺時 0 |
| `twpub_insider_pledge_ratio` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 51,272 | 使用 |
| `twpub_insider_pledge_ratio__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 51,272 | 使用；基礎值缺時 0 |
| `twpub_insider_transfer_shares_log` | 待回補 | 2026-07-20 | 2026-09-17 | 0 | 42 | 使用 |
| `twpub_insider_transfer_shares_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-17 | 0 | 42 | 使用；基礎值缺時 0 |
| `twpub_institutional_net_buy_flow` | 正式表已有 2014 | 2004-06-02 | 2026-09-18 | 301,999 | 5,828,769 | 使用 |
| `twpub_institutional_net_buy_flow__available` | 可用性旗標 | 2004-06-02 | 2026-09-18 | 301,999 | 5,828,769 | 使用；基礎值缺時 0 |
| `twpub_institutional_net_buy_shares_raw` | 正式表已有 2014 | 2004-06-02 | 2026-09-18 | 301,999 | 5,828,769 | 使用 |
| `twpub_institutional_net_buy_shares_raw__available` | 可用性旗標 | 2004-06-02 | 2026-09-18 | 301,999 | 5,828,769 | 使用；基礎值缺時 0 |
| `twpub_investment_trust_net_buy_flow` | 正式表已有 2014 | 2004-06-02 | 2026-09-18 | 302,815 | 5,835,024 | 使用 |
| `twpub_investment_trust_net_buy_flow__available` | 可用性旗標 | 2004-06-02 | 2026-09-18 | 302,815 | 5,835,024 | 使用；基礎值缺時 0 |
| `twpub_investment_trust_net_buy_shares_raw` | 正式表已有 2014 | 2004-06-02 | 2026-09-18 | 302,815 | 5,835,024 | 使用 |
| `twpub_investment_trust_net_buy_shares_raw__available` | 可用性旗標 | 2004-06-02 | 2026-09-18 | 302,815 | 5,835,024 | 使用；基礎值缺時 0 |
| `twpub_margin_balance_chg` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用 |
| `twpub_margin_balance_chg__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用；基礎值缺時 0 |
| `twpub_margin_balance_log` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 321,948 | 7,982,137 | 使用 |
| `twpub_margin_balance_log__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 321,948 | 7,982,137 | 使用；基礎值缺時 0 |
| `twpub_margin_balance_lots_raw` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用 |
| `twpub_margin_balance_lots_raw__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用；基礎值缺時 0 |
| `twpub_margin_buy_flow` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用 |
| `twpub_margin_buy_flow__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用；基礎值缺時 0 |
| `twpub_margin_buy_lots_raw` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用 |
| `twpub_margin_buy_lots_raw__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用；基礎值缺時 0 |
| `twpub_margin_sell_flow` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用 |
| `twpub_margin_sell_flow__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用；基礎值缺時 0 |
| `twpub_margin_sell_lots_raw` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用 |
| `twpub_margin_sell_lots_raw__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用；基礎值缺時 0 |
| `twpub_material_clause_log` | 待回補 | 2026-07-17 | 2026-09-18 | 0 | 2,718 | 使用 |
| `twpub_material_clause_log__available` | 可用性旗標 | 2026-07-17 | 2026-09-18 | 0 | 2,718 | 使用；基礎值缺時 0 |
| `twpub_material_event_count_log` | 待回補 | 2026-07-17 | 2026-09-18 | 0 | 2,718 | 使用 |
| `twpub_material_event_count_log__available` | 可用性旗標 | 2026-07-17 | 2026-09-18 | 0 | 2,718 | 使用；基礎值缺時 0 |
| `twpub_material_fact_lag_days` | 待回補 | 2026-07-17 | 2026-09-18 | 0 | 2,718 | 使用 |
| `twpub_material_fact_lag_days__available` | 可用性旗標 | 2026-07-17 | 2026-09-18 | 0 | 2,718 | 使用；基礎值缺時 0 |
| `twpub_mof_business_tax_log` | 原值重算 | 2013-01-14 | 2026-08-12 | 7 | 99 | 使用 |
| `twpub_mof_business_tax_log__available` | 可用性旗標 | 2013-01-14 | 2026-08-12 | 7 | 99 | 使用；基礎值缺時 0 |
| `twpub_mof_business_tax_raw` | 僅研究表已有 2014 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用 |
| `twpub_mof_business_tax_raw__available` | 可用性旗標 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用；基礎值缺時 0 |
| `twpub_mof_export_log` | 原值重算 | 2014-02-10 | 2026-09-10 | 11 | 152 | 使用 |
| `twpub_mof_export_log__available` | 可用性旗標 | 2014-02-10 | 2026-09-10 | 11 | 152 | 使用；基礎值缺時 0 |
| `twpub_mof_export_raw` | 僅研究表已有 2014 | 2014-02-10 | 2026-09-10 | 11 | 152 | 使用 |
| `twpub_mof_export_raw__available` | 可用性旗標 | 2014-02-10 | 2026-09-10 | 11 | 152 | 使用；基礎值缺時 0 |
| `twpub_mof_futures_tax_log` | 原值重算 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用 |
| `twpub_mof_futures_tax_log__available` | 可用性旗標 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用；基礎值缺時 0 |
| `twpub_mof_futures_tax_raw` | 僅研究表已有 2014 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用 |
| `twpub_mof_futures_tax_raw__available` | 可用性旗標 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用；基礎值缺時 0 |
| `twpub_mof_import_log` | 原值重算 | 2014-02-10 | 2026-09-10 | 11 | 152 | 使用 |
| `twpub_mof_import_log__available` | 可用性旗標 | 2014-02-10 | 2026-09-10 | 11 | 152 | 使用；基礎值缺時 0 |
| `twpub_mof_import_raw` | 僅研究表已有 2014 | 2014-02-10 | 2026-09-10 | 11 | 152 | 使用 |
| `twpub_mof_import_raw__available` | 可用性旗標 | 2014-02-10 | 2026-09-10 | 11 | 152 | 使用；基礎值缺時 0 |
| `twpub_mof_securities_tax_log` | 原值重算 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用 |
| `twpub_mof_securities_tax_log__available` | 可用性旗標 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用；基礎值缺時 0 |
| `twpub_mof_securities_tax_raw` | 僅研究表已有 2014 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用 |
| `twpub_mof_securities_tax_raw__available` | 可用性旗標 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用；基礎值缺時 0 |
| `twpub_mof_tax_total_log` | 原值重算 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用 |
| `twpub_mof_tax_total_log__available` | 可用性旗標 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用；基礎值缺時 0 |
| `twpub_mof_tax_total_raw` | 僅研究表已有 2014 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用 |
| `twpub_mof_tax_total_raw__available` | 可用性旗標 | 2013-01-14 | 2026-09-14 | 12 | 165 | 使用；基礎值缺時 0 |
| `twpub_mof_trade_balance_asinh` | 原值重算 | 2014-02-10 | 2026-09-10 | 11 | 152 | 使用 |
| `twpub_mof_trade_balance_asinh__available` | 可用性旗標 | 2014-02-10 | 2026-09-10 | 11 | 152 | 使用；基礎值缺時 0 |
| `twpub_mof_trade_balance_raw` | 僅研究表已有 2014 | 2014-02-10 | 2026-09-10 | 11 | 152 | 使用 |
| `twpub_mof_trade_balance_raw__available` | 可用性旗標 | 2014-02-10 | 2026-09-10 | 11 | 152 | 使用；基礎值缺時 0 |
| `twpub_monthly_revenue_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 50,914 | 使用 |
| `twpub_monthly_revenue_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 50,914 | 使用；基礎值缺時 0 |
| `twpub_monthly_revenue_mom` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 51,097 | 使用 |
| `twpub_monthly_revenue_mom__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 51,097 | 使用；基礎值缺時 0 |
| `twpub_monthly_revenue_yoy` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 51,103 | 使用 |
| `twpub_monthly_revenue_yoy__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 51,103 | 使用；基礎值缺時 0 |
| `twpub_official_close_logret_1d` | 正式表已有 2014 | 2003-08-04 | 2026-09-18 | 374,740 | 8,917,664 | 使用 |
| `twpub_official_close_logret_1d__available` | 可用性旗標 | 2003-08-04 | 2026-09-18 | 374,740 | 8,917,664 | 使用；基礎值缺時 0 |
| `twpub_official_close_to_high` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 377,122 | 8,986,497 | 使用 |
| `twpub_official_close_to_high__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 377,122 | 8,986,497 | 使用；基礎值缺時 0 |
| `twpub_official_close_to_low` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 377,122 | 8,986,497 | 使用 |
| `twpub_official_close_to_low__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 377,122 | 8,986,497 | 使用；基礎值缺時 0 |
| `twpub_official_intraday_range` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 377,122 | 8,986,999 | 使用 |
| `twpub_official_intraday_range__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 377,122 | 8,986,999 | 使用；基礎值缺時 0 |
| `twpub_official_trades_log` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 377,601 | 8,998,064 | 使用 |
| `twpub_official_trades_log__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 377,601 | 8,998,064 | 使用；基礎值缺時 0 |
| `twpub_official_trades_raw` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 381,814 | 9,112,200 | 使用 |
| `twpub_official_trades_raw__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 381,814 | 9,112,200 | 使用；基礎值缺時 0 |
| `twpub_official_trading_value_log` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 377,601 | 8,998,056 | 使用 |
| `twpub_official_trading_value_log__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 377,601 | 8,998,056 | 使用；基礎值缺時 0 |
| `twpub_official_trading_value_raw` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 381,814 | 9,112,200 | 使用 |
| `twpub_official_trading_value_raw__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 381,814 | 9,112,200 | 使用；基礎值缺時 0 |
| `twpub_official_trading_volume_log` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 377,601 | 8,998,064 | 使用 |
| `twpub_official_trading_volume_log__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 377,601 | 8,998,064 | 使用；基礎值缺時 0 |
| `twpub_official_trading_volume_raw` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 381,814 | 9,112,200 | 使用 |
| `twpub_official_trading_volume_raw__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 381,814 | 9,112,200 | 使用；基礎值缺時 0 |
| `twpub_official_turnover_ratio` | 正式表已有 2014 | 2004-10-28 | 2026-09-18 | 165,363 | 3,839,319 | 使用 |
| `twpub_official_turnover_ratio__available` | 可用性旗標 | 2004-10-28 | 2026-09-18 | 165,363 | 3,839,319 | 使用；基礎值缺時 0 |
| `twpub_pb_log` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 373,660 | 8,220,898 | 使用 |
| `twpub_pb_log__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 373,660 | 8,220,898 | 使用；基礎值缺時 0 |
| `twpub_pb_raw` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 373,660 | 8,225,480 | 使用 |
| `twpub_pb_raw__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 373,660 | 8,225,480 | 使用；基礎值缺時 0 |
| `twpub_pe_log` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 283,953 | 6,249,105 | 使用 |
| `twpub_pe_log__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 283,953 | 6,249,105 | 使用；基礎值缺時 0 |
| `twpub_pe_raw` | 正式表已有 2014 | 2003-08-01 | 2026-09-18 | 283,953 | 6,249,106 | 使用 |
| `twpub_pe_raw__available` | 可用性旗標 | 2003-08-01 | 2026-09-18 | 283,953 | 6,249,106 | 使用；基礎值缺時 0 |
| `twpub_sbl_balance_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 23,092 | 使用 |
| `twpub_sbl_balance_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 23,092 | 使用；基礎值缺時 0 |
| `twpub_short_balance_chg` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用 |
| `twpub_short_balance_chg__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用；基礎值缺時 0 |
| `twpub_short_balance_log` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 258,326 | 5,621,154 | 使用 |
| `twpub_short_balance_log__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 258,326 | 5,621,154 | 使用；基礎值缺時 0 |
| `twpub_short_balance_lots_raw` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用 |
| `twpub_short_balance_lots_raw__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用；基礎值缺時 0 |
| `twpub_short_buy_flow` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用 |
| `twpub_short_buy_flow__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用；基礎值缺時 0 |
| `twpub_short_buy_lots_raw` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用 |
| `twpub_short_buy_lots_raw__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用；基礎值缺時 0 |
| `twpub_short_sale_available_log` | 待回補 | 2026-07-20 | 2026-09-18 | 0 | 23,389 | 使用 |
| `twpub_short_sale_available_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-18 | 0 | 23,389 | 使用；基礎值缺時 0 |
| `twpub_short_sell_flow` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用 |
| `twpub_short_sell_flow__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用；基礎值缺時 0 |
| `twpub_short_sell_lots_raw` | 正式表已有 2014 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用 |
| `twpub_short_sell_lots_raw__available` | 可用性旗標 | 2001-01-03 | 2026-09-18 | 350,872 | 8,645,733 | 使用；基礎值缺時 0 |
| `twpub_taifex_dealer_net_oi_asinh` | 待回補 | 2026-07-17 | 2026-09-17 | 0 | 9 | 使用 |
| `twpub_taifex_dealer_net_oi_asinh__available` | 可用性旗標 | 2026-07-17 | 2026-09-17 | 0 | 9 | 使用；基礎值缺時 0 |
| `twpub_taifex_foreign_net_oi_asinh` | 待回補 | 2026-07-17 | 2026-09-17 | 0 | 9 | 使用 |
| `twpub_taifex_foreign_net_oi_asinh__available` | 可用性旗標 | 2026-07-17 | 2026-09-17 | 0 | 9 | 使用；基礎值缺時 0 |
| `twpub_taifex_trust_net_oi_asinh` | 待回補 | 2026-07-17 | 2026-09-17 | 0 | 9 | 使用 |
| `twpub_taifex_trust_net_oi_asinh__available` | 可用性旗標 | 2026-07-17 | 2026-09-17 | 0 | 9 | 使用；基礎值缺時 0 |
| `twpub_taifex_tx_final_settlement_logret` | 待回補 | 2025-11-12 | 2025-12-03 | 0 | 4 | 使用 |
| `twpub_taifex_tx_final_settlement_logret__available` | 可用性旗標 | 2025-11-12 | 2025-12-03 | 0 | 4 | 使用；基礎值缺時 0 |
| `twpub_taifex_tx_large_oi_log` | 待回補 | 2026-08-03 | 2026-09-17 | 0 | 22 | 使用 |
| `twpub_taifex_tx_large_oi_log__available` | 可用性旗標 | 2026-08-03 | 2026-09-17 | 0 | 22 | 使用；基礎值缺時 0 |
| `twpub_taifex_tx_official_front_settlement_raw` | 期交所新增 | 1999-01-05 | 2026-09-18 | 248 | 6,867 | 使用 |
| `twpub_taifex_tx_official_front_settlement_raw__available` | 可用性旗標 | 1999-01-05 | 2026-09-18 | 248 | 6,867 | 使用；基礎值缺時 0 |
| `twpub_taifex_tx_official_monthly_final_settlement_raw` | 期交所新增 | 2014-01-16 | 2026-09-17 | 12 | 153 | 使用 |
| `twpub_taifex_tx_official_monthly_final_settlement_raw__available` | 可用性旗標 | 2014-01-16 | 2026-09-17 | 12 | 153 | 使用；基礎值缺時 0 |
| `twpub_taifex_tx_official_open_interest_raw` | 期交所新增 | 1999-01-05 | 2026-09-18 | 248 | 6,867 | 使用 |
| `twpub_taifex_tx_official_open_interest_raw__available` | 可用性旗標 | 1999-01-05 | 2026-09-18 | 248 | 6,867 | 使用；基礎值缺時 0 |
| `twpub_taifex_tx_official_volume_raw` | 期交所新增 | 1999-01-05 | 2026-09-18 | 248 | 6,867 | 使用 |
| `twpub_taifex_tx_official_volume_raw__available` | 可用性旗標 | 1999-01-05 | 2026-09-18 | 248 | 6,867 | 使用；基礎值缺時 0 |
| `twpub_taifex_tx_open_interest_log` | 待回補 | 2026-07-17 | 2026-09-17 | 0 | 27 | 使用 |
| `twpub_taifex_tx_open_interest_log__available` | 可用性旗標 | 2026-07-17 | 2026-09-17 | 0 | 27 | 使用；基礎值缺時 0 |
| `twpub_taifex_tx_settlement_logret_1d` | 待回補 | 2026-07-17 | 2026-09-17 | 0 | 27 | 使用 |
| `twpub_taifex_tx_settlement_logret_1d__available` | 可用性旗標 | 2026-07-17 | 2026-09-17 | 0 | 27 | 使用；基礎值缺時 0 |
| `twpub_taifex_tx_top10_long_ratio` | 待回補 | 2026-08-03 | 2026-09-17 | 0 | 22 | 使用 |
| `twpub_taifex_tx_top10_long_ratio__available` | 可用性旗標 | 2026-08-03 | 2026-09-17 | 0 | 22 | 使用；基礎值缺時 0 |
| `twpub_taifex_tx_top5_long_ratio` | 待回補 | 2026-08-03 | 2026-09-17 | 0 | 22 | 使用 |
| `twpub_taifex_tx_top5_long_ratio__available` | 可用性旗標 | 2026-08-03 | 2026-09-17 | 0 | 22 | 使用；基礎值缺時 0 |
| `twpub_taifex_tx_volume_log` | 待回補 | 2026-07-17 | 2026-09-17 | 0 | 27 | 使用 |
| `twpub_taifex_tx_volume_log__available` | 可用性旗標 | 2026-07-17 | 2026-09-17 | 0 | 27 | 使用；基礎值缺時 0 |
| `twpub_taifex_txo_call_oi_log` | 待回補 | 2026-07-17 | 2026-09-17 | 0 | 23 | 使用 |
| `twpub_taifex_txo_call_oi_log__available` | 可用性旗標 | 2026-07-17 | 2026-09-17 | 0 | 23 | 使用；基礎值缺時 0 |
| `twpub_taifex_txo_call_volume_log` | 待回補 | 2026-07-17 | 2026-09-17 | 0 | 23 | 使用 |
| `twpub_taifex_txo_call_volume_log__available` | 可用性旗標 | 2026-07-17 | 2026-09-17 | 0 | 23 | 使用；基礎值缺時 0 |
| `twpub_taifex_txo_official_call_open_interest_raw` | 期交所新增 | 2001-12-25 | 2026-09-17 | 248 | 6,090 | 使用 |
| `twpub_taifex_txo_official_call_open_interest_raw__available` | 可用性旗標 | 2001-12-25 | 2026-09-17 | 248 | 6,090 | 使用；基礎值缺時 0 |
| `twpub_taifex_txo_official_call_volume_raw` | 期交所新增 | 2001-12-25 | 2026-09-17 | 248 | 6,090 | 使用 |
| `twpub_taifex_txo_official_call_volume_raw__available` | 可用性旗標 | 2001-12-25 | 2026-09-17 | 248 | 6,090 | 使用；基礎值缺時 0 |
| `twpub_taifex_txo_official_put_call_open_interest_ratio_pct_raw` | 期交所新增 | 2001-12-25 | 2026-09-17 | 248 | 6,090 | 使用 |
| `twpub_taifex_txo_official_put_call_open_interest_ratio_pct_raw__available` | 可用性旗標 | 2001-12-25 | 2026-09-17 | 248 | 6,090 | 使用；基礎值缺時 0 |
| `twpub_taifex_txo_official_put_call_volume_ratio_pct_raw` | 期交所新增 | 2001-12-25 | 2026-09-17 | 248 | 6,090 | 使用 |
| `twpub_taifex_txo_official_put_call_volume_ratio_pct_raw__available` | 可用性旗標 | 2001-12-25 | 2026-09-17 | 248 | 6,090 | 使用；基礎值缺時 0 |
| `twpub_taifex_txo_official_put_open_interest_raw` | 期交所新增 | 2001-12-25 | 2026-09-17 | 248 | 6,090 | 使用 |
| `twpub_taifex_txo_official_put_open_interest_raw__available` | 可用性旗標 | 2001-12-25 | 2026-09-17 | 248 | 6,090 | 使用；基礎值缺時 0 |
| `twpub_taifex_txo_official_put_volume_raw` | 期交所新增 | 2001-12-25 | 2026-09-17 | 248 | 6,090 | 使用 |
| `twpub_taifex_txo_official_put_volume_raw__available` | 可用性旗標 | 2001-12-25 | 2026-09-17 | 248 | 6,090 | 使用；基礎值缺時 0 |
| `twpub_taifex_txo_put_call_oi_ratio` | 待回補 | 2026-07-17 | 2026-09-17 | 0 | 23 | 使用 |
| `twpub_taifex_txo_put_call_oi_ratio__available` | 可用性旗標 | 2026-07-17 | 2026-09-17 | 0 | 23 | 使用；基礎值缺時 0 |
| `twpub_taifex_txo_put_call_volume_ratio` | 待回補 | 2026-07-17 | 2026-09-17 | 0 | 23 | 使用 |
| `twpub_taifex_txo_put_call_volume_ratio__available` | 可用性旗標 | 2026-07-17 | 2026-09-17 | 0 | 23 | 使用；基礎值缺時 0 |
| `twpub_taifex_txo_put_oi_log` | 待回補 | 2026-07-17 | 2026-09-17 | 0 | 23 | 使用 |
| `twpub_taifex_txo_put_oi_log__available` | 可用性旗標 | 2026-07-17 | 2026-09-17 | 0 | 23 | 使用；基礎值缺時 0 |
| `twpub_taifex_txo_put_volume_log` | 待回補 | 2026-07-17 | 2026-09-17 | 0 | 23 | 使用 |
| `twpub_taifex_txo_put_volume_log__available` | 可用性旗標 | 2026-07-17 | 2026-09-17 | 0 | 23 | 使用；基礎值缺時 0 |
| `twpub_tdcc_holder_count_log` | 待回補 | 2026-07-20 | 2026-09-17 | 0 | 14,994 | 使用 |
| `twpub_tdcc_holder_count_log__available` | 可用性旗標 | 2026-07-20 | 2026-09-17 | 0 | 14,994 | 使用；基礎值缺時 0 |
| `twpub_tdcc_large_holder_ratio` | 待回補 | 2026-07-20 | 2026-09-17 | 0 | 14,994 | 使用 |
| `twpub_tdcc_large_holder_ratio__available` | 可用性旗標 | 2026-07-20 | 2026-09-17 | 0 | 14,994 | 使用；基礎值缺時 0 |
| `twpub_tdcc_retail_holder_ratio` | 待回補 | 2026-07-20 | 2026-09-17 | 0 | 14,994 | 使用 |
| `twpub_tdcc_retail_holder_ratio__available` | 可用性旗標 | 2026-07-20 | 2026-09-17 | 0 | 14,994 | 使用；基礎值缺時 0 |
| `twpub_twse_taiex_log` | 正式表已有 2014 | 1999-01-05 | 2026-09-18 | 248 | 6,867 | 使用 |
| `twpub_twse_taiex_log__available` | 可用性旗標 | 1999-01-05 | 2026-09-18 | 248 | 6,867 | 使用；基礎值缺時 0 |
| `twpub_twse_taiex_logret_1d` | 正式表已有 2014 | 1999-01-06 | 2026-09-18 | 248 | 6,866 | 使用 |
| `twpub_twse_taiex_logret_1d__available` | 可用性旗標 | 1999-01-06 | 2026-09-18 | 248 | 6,866 | 使用；基礎值缺時 0 |
| `twpub_twse_taiex_pct` | 正式表已有 2014 | 1999-01-06 | 2026-09-18 | 248 | 6,866 | 使用 |
| `twpub_twse_taiex_pct__available` | 可用性旗標 | 1999-01-06 | 2026-09-18 | 248 | 6,866 | 使用；基礎值缺時 0 |
| `twpub_twse_taiex_raw` | 正式表已有 2014 | 1999-01-05 | 2026-09-18 | 248 | 6,867 | 使用 |
| `twpub_twse_taiex_raw__available` | 可用性旗標 | 1999-01-05 | 2026-09-18 | 248 | 6,867 | 使用；基礎值缺時 0 |
| `twpub_usdtwd_log` | 原值重算 | 2013-01-02 | 2026-09-18 | 248 | 3,349 | 使用 |
| `twpub_usdtwd_log__available` | 可用性旗標 | 2013-01-02 | 2026-09-18 | 248 | 3,349 | 使用；基礎值缺時 0 |
| `twpub_usdtwd_logret_1d` | 原值重算 | 2013-01-03 | 2026-09-18 | 248 | 3,348 | 使用 |
| `twpub_usdtwd_logret_1d__available` | 可用性旗標 | 2013-01-03 | 2026-09-18 | 248 | 3,348 | 使用；基礎值缺時 0 |
| `twpub_usdtwd_raw` | 僅研究表已有 2014 | 2013-01-02 | 2026-09-18 | 248 | 3,349 | 使用 |
| `twpub_usdtwd_raw__available` | 可用性旗標 | 2013-01-02 | 2026-09-18 | 248 | 3,349 | 使用；基礎值缺時 0 |
| `twpub_xbrl_assets_twd_raw` | 僅研究表已有 2014 | 2013-04-17 | 2026-09-02 | 7,093 | 103,283 | 使用 |
| `twpub_xbrl_assets_twd_raw__available` | 可用性旗標 | 2013-04-17 | 2026-09-02 | 7,093 | 103,283 | 使用；基礎值缺時 0 |
| `twpub_xbrl_basic_eps_quarter_raw` | 僅研究表已有 2014 | 2013-04-17 | 2026-09-02 | 4,642 | 66,569 | 使用 |
| `twpub_xbrl_basic_eps_quarter_raw__available` | 可用性旗標 | 2013-04-17 | 2026-09-02 | 4,642 | 66,569 | 使用；基礎值缺時 0 |
| `twpub_xbrl_basic_eps_ytd_raw` | 僅研究表已有 2014 | 2013-04-17 | 2026-09-02 | 6,941 | 103,467 | 使用 |
| `twpub_xbrl_basic_eps_ytd_raw__available` | 可用性旗標 | 2013-04-17 | 2026-09-02 | 6,941 | 103,467 | 使用；基礎值缺時 0 |
| `twpub_xbrl_cash_twd_raw` | 僅研究表已有 2014 | 2013-04-17 | 2026-09-02 | 7,091 | 103,149 | 使用 |
| `twpub_xbrl_cash_twd_raw__available` | 可用性旗標 | 2013-04-17 | 2026-09-02 | 7,091 | 103,149 | 使用；基礎值缺時 0 |
| `twpub_xbrl_current_assets_twd_raw` | 僅研究表已有 2014 | 2013-04-18 | 2026-09-02 | 6,802 | 99,760 | 使用 |
| `twpub_xbrl_current_assets_twd_raw__available` | 可用性旗標 | 2013-04-18 | 2026-09-02 | 6,802 | 99,760 | 使用；基礎值缺時 0 |
| `twpub_xbrl_current_liabilities_twd_raw` | 僅研究表已有 2014 | 2013-04-18 | 2026-09-02 | 6,803 | 99,708 | 使用 |
| `twpub_xbrl_current_liabilities_twd_raw__available` | 可用性旗標 | 2013-04-18 | 2026-09-02 | 6,803 | 99,708 | 使用；基礎值缺時 0 |
| `twpub_xbrl_current_ratio` | 僅研究表已有 2014 | 2013-04-18 | 2026-09-02 | 6,802 | 99,674 | 使用 |
| `twpub_xbrl_current_ratio__available` | 可用性旗標 | 2013-04-18 | 2026-09-02 | 6,802 | 99,674 | 使用；基礎值缺時 0 |
| `twpub_xbrl_debt_ratio` | 僅研究表已有 2014 | 2013-04-17 | 2026-09-02 | 7,093 | 103,260 | 使用 |
| `twpub_xbrl_debt_ratio__available` | 可用性旗標 | 2013-04-17 | 2026-09-02 | 7,093 | 103,260 | 使用；基礎值缺時 0 |
| `twpub_xbrl_equity_ratio` | 僅研究表已有 2014 | 2013-04-17 | 2026-09-02 | 7,093 | 103,281 | 使用 |
| `twpub_xbrl_equity_ratio__available` | 可用性旗標 | 2013-04-17 | 2026-09-02 | 7,093 | 103,281 | 使用；基礎值缺時 0 |
| `twpub_xbrl_equity_twd_raw` | 僅研究表已有 2014 | 2013-04-17 | 2026-09-02 | 7,123 | 103,628 | 使用 |
| `twpub_xbrl_equity_twd_raw__available` | 可用性旗標 | 2013-04-17 | 2026-09-02 | 7,123 | 103,628 | 使用；基礎值缺時 0 |
| `twpub_xbrl_gross_margin` | 待回補 | 2019-04-17 | 2026-09-02 | 0 | 40,987 | 使用 |
| `twpub_xbrl_gross_margin__available` | 可用性旗標 | 2019-04-17 | 2026-09-02 | 0 | 40,987 | 使用；基礎值缺時 0 |
| `twpub_xbrl_gross_profit_twd_quarter_raw` | 待回補 | 2019-04-17 | 2026-09-02 | 0 | 41,026 | 使用 |
| `twpub_xbrl_gross_profit_twd_quarter_raw__available` | 可用性旗標 | 2019-04-17 | 2026-09-02 | 0 | 41,026 | 使用；基礎值缺時 0 |
| `twpub_xbrl_gross_profit_twd_ytd_raw` | 待回補 | 2019-04-17 | 2026-09-02 | 0 | 61,981 | 使用 |
| `twpub_xbrl_gross_profit_twd_ytd_raw__available` | 可用性旗標 | 2019-04-17 | 2026-09-02 | 0 | 61,981 | 使用；基礎值缺時 0 |
| `twpub_xbrl_inventories_twd_raw` | 僅研究表已有 2014 | 2013-04-18 | 2026-09-02 | 6,403 | 92,726 | 使用 |
| `twpub_xbrl_inventories_twd_raw__available` | 可用性旗標 | 2013-04-18 | 2026-09-02 | 6,403 | 92,726 | 使用；基礎值缺時 0 |
| `twpub_xbrl_inventory_to_assets_ratio` | 僅研究表已有 2014 | 2013-04-18 | 2026-09-02 | 6,403 | 92,726 | 使用 |
| `twpub_xbrl_inventory_to_assets_ratio__available` | 可用性旗標 | 2013-04-18 | 2026-09-02 | 6,403 | 92,726 | 使用；基礎值缺時 0 |
| `twpub_xbrl_liabilities_twd_raw` | 僅研究表已有 2014 | 2013-04-17 | 2026-09-02 | 7,093 | 103,262 | 使用 |
| `twpub_xbrl_liabilities_twd_raw__available` | 可用性旗標 | 2013-04-17 | 2026-09-02 | 7,093 | 103,262 | 使用；基礎值缺時 0 |
| `twpub_xbrl_net_margin` | 僅研究表已有 2014 | 2013-04-25 | 2026-09-02 | 33 | 41,583 | 使用 |
| `twpub_xbrl_net_margin__available` | 可用性旗標 | 2013-04-25 | 2026-09-02 | 33 | 41,583 | 使用；基礎值缺時 0 |
| `twpub_xbrl_operating_cash_flow_twd_quarter_raw` | 僅研究表已有 2014 | 2013-04-17 | 2026-07-20 | 1,566 | 22,944 | 使用 |
| `twpub_xbrl_operating_cash_flow_twd_quarter_raw__available` | 可用性旗標 | 2013-04-17 | 2026-07-20 | 1,566 | 22,944 | 使用；基礎值缺時 0 |
| `twpub_xbrl_operating_cash_flow_twd_ytd_raw` | 僅研究表已有 2014 | 2013-04-17 | 2026-09-02 | 7,092 | 103,119 | 使用 |
| `twpub_xbrl_operating_cash_flow_twd_ytd_raw__available` | 可用性旗標 | 2013-04-17 | 2026-09-02 | 7,092 | 103,119 | 使用；基礎值缺時 0 |
| `twpub_xbrl_operating_margin` | 待回補 | 2019-04-17 | 2026-09-02 | 0 | 41,343 | 使用 |
| `twpub_xbrl_operating_margin__available` | 可用性旗標 | 2019-04-17 | 2026-09-02 | 0 | 41,343 | 使用；基礎值缺時 0 |
| `twpub_xbrl_operating_profit_twd_quarter_raw` | 待回補 | 2019-04-17 | 2026-09-02 | 0 | 41,395 | 使用 |
| `twpub_xbrl_operating_profit_twd_quarter_raw__available` | 可用性旗標 | 2019-04-17 | 2026-09-02 | 0 | 41,395 | 使用；基礎值缺時 0 |
| `twpub_xbrl_operating_profit_twd_ytd_raw` | 待回補 | 2019-04-17 | 2026-09-02 | 0 | 63,371 | 使用 |
| `twpub_xbrl_operating_profit_twd_ytd_raw__available` | 可用性旗標 | 2019-04-17 | 2026-09-02 | 0 | 63,371 | 使用；基礎值缺時 0 |
| `twpub_xbrl_profit_twd_quarter_raw` | 僅研究表已有 2014 | 2013-04-17 | 2026-09-02 | 4,735 | 66,831 | 使用 |
| `twpub_xbrl_profit_twd_quarter_raw__available` | 可用性旗標 | 2013-04-17 | 2026-09-02 | 4,735 | 66,831 | 使用；基礎值缺時 0 |
| `twpub_xbrl_profit_twd_ytd_raw` | 僅研究表已有 2014 | 2013-04-17 | 2026-09-02 | 7,122 | 103,635 | 使用 |
| `twpub_xbrl_profit_twd_ytd_raw__available` | 可用性旗標 | 2013-04-17 | 2026-09-02 | 7,122 | 103,635 | 使用；基礎值缺時 0 |
| `twpub_xbrl_revenue_twd_quarter_raw` | 僅研究表已有 2014 | 2013-04-25 | 2026-09-02 | 33 | 41,623 | 使用 |
| `twpub_xbrl_revenue_twd_quarter_raw__available` | 可用性旗標 | 2013-04-25 | 2026-09-02 | 33 | 41,623 | 使用；基礎值缺時 0 |
| `twpub_xbrl_revenue_twd_ytd_raw` | 僅研究表已有 2014 | 2013-04-25 | 2026-09-02 | 53 | 63,552 | 使用 |
| `twpub_xbrl_revenue_twd_ytd_raw__available` | 可用性旗標 | 2013-04-25 | 2026-09-02 | 53 | 63,552 | 使用；基礎值缺時 0 |
| `twpub_xbrl_tifrs_gross_margin` | 僅研究表已有 2014 | 2013-04-18 | 2017-12-04 | 4,481 | 22,732 | 使用 |
| `twpub_xbrl_tifrs_gross_margin__available` | 可用性旗標 | 2013-04-18 | 2017-12-04 | 4,481 | 22,732 | 使用；基礎值缺時 0 |
| `twpub_xbrl_tifrs_gross_profit_twd_quarter_raw` | 僅研究表已有 2014 | 2013-04-18 | 2019-03-19 | 4,489 | 27,743 | 使用 |
| `twpub_xbrl_tifrs_gross_profit_twd_quarter_raw__available` | 可用性旗標 | 2013-04-18 | 2019-03-19 | 4,489 | 27,743 | 使用；基礎值缺時 0 |
| `twpub_xbrl_tifrs_gross_profit_twd_ytd_raw` | 僅研究表已有 2014 | 2013-04-18 | 2019-08-12 | 6,596 | 42,869 | 使用 |
| `twpub_xbrl_tifrs_gross_profit_twd_ytd_raw__available` | 可用性旗標 | 2013-04-18 | 2019-08-12 | 6,596 | 42,869 | 使用；基礎值缺時 0 |
| `twpub_xbrl_tifrs_net_gross_margin` | 僅研究表已有 2014 | 2013-04-18 | 2017-12-04 | 4,481 | 22,732 | 使用 |
| `twpub_xbrl_tifrs_net_gross_margin__available` | 可用性旗標 | 2013-04-18 | 2017-12-04 | 4,481 | 22,732 | 使用；基礎值缺時 0 |
| `twpub_xbrl_tifrs_net_gross_profit_twd_quarter_raw` | 僅研究表已有 2014 | 2013-04-18 | 2017-12-04 | 4,489 | 22,769 | 使用 |
| `twpub_xbrl_tifrs_net_gross_profit_twd_quarter_raw__available` | 可用性旗標 | 2013-04-18 | 2017-12-04 | 4,489 | 22,769 | 使用；基礎值缺時 0 |
| `twpub_xbrl_tifrs_net_gross_profit_twd_ytd_raw` | 僅研究表已有 2014 | 2013-04-18 | 2018-07-27 | 6,596 | 35,172 | 使用 |
| `twpub_xbrl_tifrs_net_gross_profit_twd_ytd_raw__available` | 可用性旗標 | 2013-04-18 | 2018-07-27 | 6,596 | 35,172 | 使用；基礎值缺時 0 |
| `twpub_xbrl_tifrs_net_operating_income_twd_quarter_raw` | 僅研究表已有 2014 | 2013-04-18 | 2017-12-04 | 4,490 | 22,786 | 使用 |
| `twpub_xbrl_tifrs_net_operating_income_twd_quarter_raw__available` | 可用性旗標 | 2013-04-18 | 2017-12-04 | 4,490 | 22,786 | 使用；基礎值缺時 0 |
| `twpub_xbrl_tifrs_net_operating_income_twd_ytd_raw` | 僅研究表已有 2014 | 2013-04-18 | 2018-07-27 | 6,606 | 35,287 | 使用 |
| `twpub_xbrl_tifrs_net_operating_income_twd_ytd_raw__available` | 可用性旗標 | 2013-04-18 | 2018-07-27 | 6,606 | 35,287 | 使用；基礎值缺時 0 |
| `twpub_xbrl_tifrs_operating_margin` | 僅研究表已有 2014 | 2013-04-18 | 2017-12-04 | 4,481 | 22,732 | 使用 |
| `twpub_xbrl_tifrs_operating_margin__available` | 可用性旗標 | 2013-04-18 | 2017-12-04 | 4,481 | 22,732 | 使用；基礎值缺時 0 |
| `twpub_xbrl_tifrs_operating_revenue_twd_quarter_raw` | 僅研究表已有 2014 | 2013-04-18 | 2017-12-04 | 4,489 | 22,769 | 使用 |
| `twpub_xbrl_tifrs_operating_revenue_twd_quarter_raw__available` | 可用性旗標 | 2013-04-18 | 2017-12-04 | 4,489 | 22,769 | 使用；基礎值缺時 0 |
| `twpub_xbrl_tifrs_operating_revenue_twd_ytd_raw` | 僅研究表已有 2014 | 2013-04-18 | 2018-07-27 | 6,596 | 35,170 | 使用 |
| `twpub_xbrl_tifrs_operating_revenue_twd_ytd_raw__available` | 可用性旗標 | 2013-04-18 | 2018-07-27 | 6,596 | 35,170 | 使用；基礎值缺時 0 |
| `upper_shadow` | 股票面板 | 面板計算 | 面板計算 | — | — | 使用 |

## 重建與增量更新

1. 先以既有下載器更新正式來源表、2014 寬研究表與期交所 v2 表，各自保留原始檔與收據。
2. 執行 `source scripts/runtime_env.sh && run_fintech_python scripts/build_tw_public_research_all_features.py`。輸入雜湊與輸出雜湊相同時原樣重用；變更時原子重建。研究表值優先，同鍵研究空值由正式表補。
3. 執行 `source scripts/runtime_env.sh && run_fintech_python scripts/report_tw_public_research_all_features.py` 更新本頁，再以 `train.py --config configs/markets/tw_public_preopen_all_observed_research_2014_v3.yaml --check-data-only` 核對模型資料。
4. 開始完整訓練前，需確認顯示的 428 通道、折數、來源簽章、記憶體與輸出目錄；訓練結果只可稱為研究實驗。

資料血緣：`tw_public_research_wide_2014_taifex_v2.parquet` + `tw_public_stock_daily.parquet` → 15 個原值公式 + 同鍵空值補齊 → v3；精確輸入／輸出 SHA-256 在同名 `.all_features.json` 收據。
