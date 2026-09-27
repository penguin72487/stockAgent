# FinMind 呼叫效率唯讀基線 — 2026-09-27

觀測時間：`2026-09-26T23:00:44.100999+00:00`（台北 2026-09-27 07:00:44）。這是優化前、worker 持續運行中的時間點；各 SQLite 查詢是唯讀，但跨資料庫不是原子快照。

## 量測契約

- HTTP 分母：`data_finmind/request_traffic.sqlite3.requests` 的 request-start claim，直接在 data endpoint 發送前寫入。包含失敗嘗試，不含 `user_info` 帳號配額查詢；不是 FinMind 官方計費回條，極小 crash-before-send 窗口仍可能存在。
- 資料產出分子：Sponsor / Complement `tasks` 中 `last_attempt_at_utc` 落在窗口且現在仍為 complete / observed_empty 的分區。這是**保留結果的快照**，不是所有 HTTP 回應歷史；同分區重試只留下最後一次，不能把「任務數」當「HTTP 數」。
- 大小是 Parquet 壓縮後位元組，不是 HTTP 網路流量。未紀錄 wire bytes / response row count，因此不能聲稱精確的平均 API payload 或官方上限。
- 派生 Wide 完全排除 HTTP 產出指標；Free worker 的 receipt 不在兩個任務 queue 中，其產出欄標記 `—`，不是零資料。
- 最早本機非空日期不是官方最早可取得日期；未取得的舊區間不能由當前樣本規律保證。

## 總量

| 窗口 | data HTTP claims | 非空保留分區 | 空保留分區 | 保留列數 | 保留 Parquet bytes |
|---|---:|---:|---:|---:|---:|
| 1h | 5654 | 3325 | 2197 | 8305365 | 173915782 |
| 24h | 133874 | 70834 | 54634 | 271958142 | 3240652151 |

## 已實證的浪費與正確性風險

1. **按日遍歷季報/月營收**：Sponsor 季報三表各 50 個非空分區，均為季度末；各有 4,602 個空日。月營收 153 個非空分區都在每月 1 日，另有 4,499 個空日。四表剩餘任務中非理論 period-anchor 共 15,590 個（Financial 8,611；Balance 753；Cash 2,017；Revenue 4,209）。只有官方契約或成功日期清單可以授權跳過未抓日期，不能僅憑樣本直接宣稱無資料。
2. **最新空分區每 15 分鐘重查**：舊 `_finish` 使用 `period_end >= today`，使昨天的單日分區被當成 current；`_seed` 只延後 today / pending 的假日任務，漏掉 release 前實際目標 yesterday / observed_empty。9/27 星期日早上，多個 series 最近 60 分鐘仍各 4 次抓取 9/26 空資料。
3. **季/月資料反而可能漏發布**：如果只在季末/月初查一次，當時未發布為 empty，過 1–2 天就當歷史完成，11 月公布的 9/30 財報不會再更新。優化必須保留最近待發布 period 的定期刷新，不能為減少呼叫而停止真正追新。
4. **非均勻日期不能推平**：MonthPrice 實際有 14 個 2024-06-03 至 06-21 的非月初非空分區；HoldingSharesPer 有週三／四／五／六日期，包含假日前移。不能硬限月初或星期五。WeekPrice 本機 208 個非空分區皆週一，但更早歷史尚未證明。
5. **來源損壞沒有自動回修**：Wide 2023-08-21 的 parent LONG receipt 宣稱 78,863 rows / 218,614 bytes；對應 content-addressed parquet 實際為 0 bytes，parent 任務卻仍 complete。Wide 每次重算失敗不耗 API，卻無法自行修復 parent。應以單一 parent invalidation + 保留證據 + 一次來源重抓修復，不反覆重新衍生或偽造完成。

## 逐檔日期與指紋獨立檢查

讀取各已完成分區原始 date 欄，逐檔驗證 size 及 SHA-256，6.56 秒完成；不呼叫 API、不修改資料。

| owner | dataset | 已檢查檔 | 列數 | 唯一日期 | 最早 | 最新 | 非標準日期 / hash 失敗 |
|---|---|---:|---:|---:|---|---|---|
| Sponsor | TaiwanStockFinancialStatements | 50 | 1638160 | 50 | 2014-03-31 | 2026-06-30 | 0 / 0 |
| Sponsor | TaiwanStockBalanceSheet | 50 | 7585459 | 50 | 2014-03-31 | 2026-06-30 | 0 / 0 |
| Sponsor | TaiwanStockCashFlowsStatement | 50 | 2175154 | 50 | 2014-03-31 | 2026-06-30 | 0 / 0 |
| Sponsor | TaiwanStockMonthRevenue | 153 | 294818 | 153 | 2014-01-01 | 2026-09-01 | 0 / 0 |
| Complement | TaiwanStockBalanceSheet | 2459 | 8125293 | 55 | 2012-12-31 | 2026-06-30 | 0 / 0 |

Complement 其他三表尚無 completed 檔；不得宣稱 1990–2013 全區間規律已驗證。單次季度資產負債表已觀察到最大 189,367 rows、918,766 Parquet bytes；此值不代表官方 payload cap。

## 建議發布與追新規則（待程式實作驗證）

- 每個 dataset 明確分離 query-date（資料期別）與 publish-time（發佈時間）。只在可能出現新版本時排固定查詢。
- 日行情／交易活動：使用來源市場的可驗證交易日；休市只繼續歷史回補，不重試休市當天。尚未發布的真交易日可短重試，固定資料發布前不得提前消耗 quota。
- 財報：保存可用的季度末 anchor；最新已結束季度／近期修訂季度持續刷新，不能因當日 empty 就永遠停止。不是所有企業都在同一天發布，法定截止日不是實際發布證據。
- 月營收：以來源文件確認月份 anchor 與資料所屬月份，保留最近數期刷新（修訂／晚到）；不假設欄位日期就是發布日。
- 週／月價與集保：優先官方 available-date 清單或 provider-returned dates，不用星期幾硬裁資料。非標準實際日期必須保留。
- 可支援 range 的稀疏來源才擴大日期窗；whole-market date-only 必須保留單日 API，不把本機分區大小當作 API 可接受範圍。大回應必須有截斷／超時檢查，不能以 HTTP 200 宣告全史完整。

## 每個 dataset 的最近實際 data HTTP 與保留產出

24h 分子為兩個 queue 的保留分區快照；60m 僅 HTTP 次數列入此表，完整觀測值由當時 SQLite 查詢取得。依 24h HTTP 降序。

| dataset | HTTP 60m | HTTP 24h | 非空分區 24h | 空分區 24h | 保留 rows 24h | 保留 bytes 24h |
|---|---:|---:|---:|---:|---:|---:|
| UKStockPrice | 0 | 20682 | 9595 | 11059 | 18903152 | 322799361 |
| USStockPrice | 0 | 18529 | 13247 | 5263 | 37240831 | 732254264 |
| TaiwanStockBalanceSheet | 4 | 7021 | 2498 | 4449 | 13809006 | 80240555 |
| TaiwanDailyShortSaleBalances | 242 | 4650 | 3272 | 1309 | 9448068 | 178950380 |
| TaiwanStockMonthRevenue | 4 | 3638 | 118 | 3447 | 218408 | 2229605 |
| TaiwanStockFinancialStatements | 4 | 3637 | 39 | 3524 | 1255765 | 7475754 |
| TaiwanStockCashFlowsStatement | 4 | 3635 | 39 | 3525 | 1611345 | 11505545 |
| TaiwanStockDividendResult | 4 | 3634 | 1597 | 1964 | 15753 | 6010408 |
| TaiwanStockDividend | 1 | 3578 | 2064 | 1497 | 17010 | 16632369 |
| TaiwanOptionDaily | 4 | 3553 | 2306 | 1174 | 19951750 | 91198746 |
| TaiwanFuturesDaily | 4 | 3520 | 2280 | 1163 | 3175070 | 62456856 |
| TaiwanStockStatisticsOfOrderBookAndTrade | 0 | 2865 | — | — | — | — |
| TaiwanVariousIndicators5Seconds | 0 | 2864 | — | — | — | — |
| TaiwanStockInstitutionalInvestorsBuySell | 4 | 2450 | 2382 | 2 | 94414904 | 262490735 |
| TaiwanStockDayTrading | 4 | 2448 | 2380 | 4 | 3117906 | 50555782 |
| TaiwanStockMarginPurchaseShortSale | 4 | 2445 | 2382 | 2 | 3998315 | 123408662 |
| TaiwanStockPriceLimit | 4 | 2443 | 2383 | 2 | 4953926 | 67473601 |
| TaiwanOptionInstitutionalInvestors | 4 | 1969 | 1253 | 640 | 39456 | 6185902 |
| TaiwanFuturesInstitutionalInvestors | 4 | 1967 | 1252 | 640 | 63309 | 7057742 |
| TaiwanStockCapitalReductionReferencePrice | 0 | 1763 | 287 | 1475 | 1015 | 935855 |
| TaiwanFuturesDealerTradingVolumeDaily | 242 | 1561 | 991 | 503 | 4370631 | 9568549 |
| TaiwanFuturesInstitutionalInvestorsAfterHours | 242 | 1561 | 989 | 503 | 39651 | 3049156 |
| TaiwanStockHoldingSharesPer | 242 | 1561 | 212 | 1280 | 12661257 | 96351109 |
| TaiwanStockConvertibleBondDailyOverview | 242 | 1560 | 987 | 501 | 315368 | 20285661 |
| TaiwanStockIndustryChainMoneyFlow | 242 | 1560 | 989 | 502 | 548524 | 19128177 |
| TaiwanStockMonthPrice | 242 | 1560 | 61 | 1432 | 2212692 | 38946848 |
| TaiwanStockWeekPrice | 242 | 1560 | 210 | 1281 | 8744655 | 138281033 |
| TaiwanFuturesOpenInterestLargeTraders | 242 | 1559 | 988 | 503 | 607295 | 38596511 |
| TaiwanOptionDealerTradingVolumeDaily | 242 | 1559 | 988 | 503 | 930180 | 4442572 |
| TaiwanOptionOpenInterestLargeTraders | 242 | 1559 | 988 | 503 | 202614 | 12050487 |
| TaiwanStock10Year | 242 | 1559 | 987 | 501 | 1737699 | 14960031 |
| TaiwanStockConvertibleBondDaily | 242 | 1559 | 988 | 502 | 315987 | 17947174 |
| TaiwanOptionInstitutionalInvestorsAfterHours | 242 | 1558 | 987 | 501 | 5922 | 2755693 |
| TaiwanStockBlockTrade | 242 | 1558 | 987 | 501 | 34357 | 2683771 |
| TaiwanStockLoanCollateralBalance | 243 | 1558 | 989 | 501 | 1965984 | 196905453 |
| TaiwanStockConvertibleBondInstitutionalInvestors | 242 | 1557 | 988 | 502 | 151649 | 7818797 |
| TaiwanStockSecuritiesLending | 242 | 1532 | 988 | 503 | 738507 | 8525224 |
| TaiwanStockShareholding | 242 | 1530 | 988 | 503 | 2147130 | 87353660 |
| TaiwanStockGovernmentBankBuySell | 242 | 1525 | 988 | 502 | 11858829 | 201201621 |
| TaiwanStockMarginMaintenance | 243 | 1522 | 989 | 501 | 1798367 | 29005831 |
| TaiwanStockMarketValue | 243 | 1516 | 1488 | 2 | 3462813 | 37261055 |
| TaiwanStockPriceAdj | 242 | 1516 | 1491 | 2 | 3703858 | 203248698 |
| TaiwanStockMarketValueWeight | 4 | 765 | 154 | 543 | 153136 | 3054114 |
| TaiwanStockActiveETFHolding | 4 | 577 | 344 | 166 | 345513 | 10220191 |
| TaiwanStockActiveETFHoldingChange | 4 | 577 | 332 | 178 | 35253 | 1405421 |
| TaiwanStockBlockTradingDailyReport | 4 | 191 | 105 | 47 | 16385 | 526439 |
| TaiwanStockInfoWithWarrantSummary | 0 | 156 | 153 | 0 | 227440 | 2431616 |
| TaiwanStockPrice | 4 | 25 | 0 | 2 | 0 | 0 |
| TaiwanStockEvery5SecondsIndex | 4 | 24 | 0 | 1 | 0 | 0 |
| TaiwanFuturesFinalSettlementPrice | 4 | 22 | 0 | 13 | 0 | 0 |
| TaiwanOptionFinalSettlementPrice | 4 | 21 | 0 | 13 | 0 | 0 |
| CnnFearGreedIndex | 0 | 16 | 13 | 0 | 3239 | 28666 |
| TaiwanBusinessIndicator | 0 | 16 | 13 | 0 | 151 | 46923 |
| TaiwanStockConvertibleBondPutProvision | 0 | 14 | 13 | 0 | 728 | 37031 |
| TaiwanStockDayTradingSuspension | 0 | 14 | 13 | 0 | 37158 | 184448 |
| TaiwanStockDispositionSecuritiesPeriod | 0 | 14 | 13 | 0 | 6020 | 262040 |
| TaiwanStockSuspended | 0 | 14 | 13 | 0 | 9373 | 68238 |
| TaiwanStockMarginShortSaleSuspension | 0 | 13 | 12 | 0 | 38846 | 188515 |
| TaiwanOptionVix | 0 | 10 | 7 | 0 | 164303 | 418486 |
| GoldPrice | 0 | 6 | 1 | 0 | 53870 | 364052 |
| JapanStockPrice | 0 | 2 | 2 | 0 | 13268 | 168847 |
| TaiwanStockConvertibleBondInfo | 0 | 2 | 1 | 0 | 1844 | 36500 |
| TaiwanStockIndustryChain | 0 | 2 | 1 | 0 | 6909 | 34586 |
| TaiwanTotalExchangeMarginMaintenance | 0 | 2 | 1 | 0 | 177 | 2537 |
| EuropeStockInfo | 0 | 1 | 1 | 0 | 1306 | 20370 |
| JapanStockInfo | 0 | 1 | 1 | 0 | 3640 | 57669 |
| TaiwanFutOptDailyInfo | 0 | 1 | 1 | 0 | 1406 | 12536 |
| TaiwanSecuritiesTraderInfo | 0 | 1 | 1 | 0 | 1043 | 36851 |
| TaiwanStockActiveETFInfo | 0 | 1 | 1 | 0 | 40 | 2281 |
| TaiwanStockInfo | 0 | 1 | 1 | 0 | 4327 | 50361 |
| TaiwanStockInfoWithWarrant | 0 | 1 | — | — | — | — |
| TaiwanStockTradingDate | 0 | 1 | — | — | — | — |
| UKStockInfo | 0 | 1 | 1 | 0 | 24339 | 287782 |
| USStockInfo | 0 | 1 | 1 | 0 | 19470 | 476418 |

## 最近 60 分鐘逐 dataset 產出快照

| dataset | HTTP claims | 非空分區 | 空分區 | 保留 rows | 保留 Parquet bytes |
|---|---:|---:|---:|---:|---:|
| TaiwanStockLoanCollateralBalance | 243 | 155 | 85 | 289823 | 27451235 |
| TaiwanStockMarginMaintenance | 243 | 155 | 85 | 260644 | 4201058 |
| TaiwanStockMarketValue | 243 | 239 | 1 | 474425 | 5085424 |
| TaiwanDailyShortSaleBalances | 242 | 154 | 85 | 291933 | 8904464 |
| TaiwanFuturesDealerTradingVolumeDaily | 242 | 154 | 85 | 647831 | 1469042 |
| TaiwanFuturesInstitutionalInvestorsAfterHours | 242 | 154 | 85 | 4620 | 453225 |
| TaiwanFuturesOpenInterestLargeTraders | 242 | 154 | 85 | 82147 | 5436936 |
| TaiwanOptionDealerTradingVolumeDaily | 242 | 154 | 85 | 173067 | 721262 |
| TaiwanOptionInstitutionalInvestorsAfterHours | 242 | 154 | 85 | 924 | 429956 |
| TaiwanOptionOpenInterestLargeTraders | 242 | 154 | 85 | 32974 | 1802989 |
| TaiwanStock10Year | 242 | 154 | 85 | 273360 | 2341404 |
| TaiwanStockBlockTrade | 242 | 154 | 85 | 4174 | 396461 |
| TaiwanStockConvertibleBondDaily | 242 | 154 | 85 | 44569 | 2516040 |
| TaiwanStockConvertibleBondDailyOverview | 242 | 154 | 85 | 44569 | 2994470 |
| TaiwanStockConvertibleBondInstitutionalInvestors | 242 | 154 | 85 | 18370 | 1102685 |
| TaiwanStockGovernmentBankBuySell | 242 | 153 | 86 | 1522543 | 24365727 |
| TaiwanStockHoldingSharesPer | 242 | 33 | 206 | 1720042 | 13705545 |
| TaiwanStockIndustryChainMoneyFlow | 242 | 154 | 85 | 84847 | 2930778 |
| TaiwanStockMonthPrice | 242 | 8 | 231 | 296705 | 5174077 |
| TaiwanStockPriceAdj | 242 | 238 | 1 | 542963 | 31428945 |
| TaiwanStockSecuritiesLending | 242 | 154 | 85 | 65933 | 1003243 |
| TaiwanStockShareholding | 242 | 154 | 85 | 316194 | 12716518 |
| TaiwanStockWeekPrice | 242 | 33 | 206 | 1112704 | 17276435 |
| TaiwanFuturesDaily | 4 | 0 | 1 | 0 | 0 |
| TaiwanFuturesFinalSettlementPrice | 4 | 0 | 1 | 0 | 0 |
| TaiwanFuturesInstitutionalInvestors | 4 | 0 | 1 | 0 | 0 |
| TaiwanOptionDaily | 4 | 0 | 1 | 0 | 0 |
| TaiwanOptionFinalSettlementPrice | 4 | 0 | 1 | 0 | 0 |
| TaiwanOptionInstitutionalInvestors | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockActiveETFHolding | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockActiveETFHoldingChange | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockBalanceSheet | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockBlockTradingDailyReport | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockCashFlowsStatement | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockDayTrading | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockDividendResult | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockEvery5SecondsIndex | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockFinancialStatements | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockInstitutionalInvestorsBuySell | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockMarginPurchaseShortSale | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockMarketValueWeight | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockMonthRevenue | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockPrice | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockPriceLimit | 4 | 0 | 1 | 0 | 0 |
| TaiwanStockDividend | 1 | 1 | 0 | 4 | 7863 |

## Queue 狀態快照（不等於新增 API 呼叫需求）

Complement delegated pending 可能由 Sponsor 擁有；不可將兩 queue pending 相加當作真實剩餘 API 次數。

| owner | dataset | pending | complete | observed_empty | 其他 state | rows | bytes |
|---|---|---:|---:|---:|---|---:|---:|
| sponsor | CnnFearGreedIndex | 3 | 13 | 0 | — | 3239 | 28666 |
| sponsor | TaiwanBusinessIndicator | 32 | 13 | 0 | — | 151 | 46923 |
| sponsor | TaiwanDailyShortSaleBalances | 6264 | 991 | 503 | — | 1999409 | 61736732 |
| sponsor | TaiwanFuturesDaily | 5663 | 3107 | 1545 | — | 4942070 | 94403458 |
| sponsor | TaiwanFuturesDealerTradingVolumeDaily | 510 | 992 | 503 | — | 4376454 | 9578236 |
| sponsor | TaiwanFuturesFinalSettlementPrice | 16 | 0 | 13 | — | 0 | 0 |
| sponsor | TaiwanFuturesInstitutionalInvestors | 0 | 2025 | 1011 | — | 115080 | 11703104 |
| sponsor | TaiwanFuturesInstitutionalInvestorsAfterHours | 318 | 990 | 503 | — | 39702 | 3052349 |
| sponsor | TaiwanFuturesOpenInterestLargeTraders | 8823 | 989 | 503 | — | 607988 | 38640000 |
| sponsor | TaiwanOptionDaily | 4414 | 3107 | 1545 | — | 26551587 | 128293383 |
| sponsor | TaiwanOptionDealerTradingVolumeDaily | 513 | 989 | 503 | — | 930986 | 4446261 |
| sponsor | TaiwanOptionFinalSettlementPrice | 13 | 0 | 13 | — | 0 | 0 |
| sponsor | TaiwanOptionInstitutionalInvestors | 0 | 2025 | 1011 | — | 62658 | 9988795 |
| sponsor | TaiwanOptionInstitutionalInvestorsAfterHours | 322 | 988 | 501 | — | 5928 | 2758485 |
| sponsor | TaiwanOptionOpenInterestLargeTraders | 8823 | 989 | 503 | — | 202774 | 12063177 |
| sponsor | TaiwanOptionVix | 0 | 7 | 0 | — | 164303 | 418486 |
| sponsor | TaiwanStock10Year | 4236 | 988 | 501 | — | 1739434 | 14975073 |
| sponsor | TaiwanStockActiveETFHolding | 0 | 344 | 166 | — | 345513 | 10220191 |
| sponsor | TaiwanStockActiveETFHoldingChange | 0 | 332 | 178 | — | 35253 | 1405421 |
| sponsor | TaiwanStockBalanceSheet | 762 | 50 | 4602 | — | 7585459 | 37425881 |
| sponsor | TaiwanStockBlockTrade | 6357 | 988 | 501 | — | 34395 | 2686529 |
| sponsor | TaiwanStockBlockTradingDailyReport | 0 | 105 | 47 | — | 16385 | 526439 |
| sponsor | TaiwanStockCapitalReductionReferencePrice | 3 | 13 | 0 | — | 596 | 63185 |
| sponsor | TaiwanStockCashFlowsStatement | 2040 | 50 | 4602 | — | 2175154 | 15305647 |
| sponsor | TaiwanStockConvertibleBondDaily | 4257 | 989 | 502 | — | 316369 | 17968130 |
| sponsor | TaiwanStockConvertibleBondDailyOverview | 4259 | 988 | 501 | — | 315750 | 20308114 |
| sponsor | TaiwanStockConvertibleBondInfo | 0 | 1 | 0 | — | 1844 | 36500 |
| sponsor | TaiwanStockConvertibleBondInstitutionalInvestors | 4257 | 989 | 502 | — | 151825 | 7827346 |
| sponsor | TaiwanStockConvertibleBondPutProvision | 3 | 13 | 0 | — | 728 | 37031 |
| sponsor | TaiwanStockDayTrading | 0 | 3105 | 4 | non_session=1543 | 4489504 | 73746617 |
| sponsor | TaiwanStockDayTradingSuspension | 0 | 13 | 0 | — | 37158 | 184448 |
| sponsor | TaiwanStockDispositionSecuritiesPeriod | 13 | 13 | 0 | — | 6020 | 262040 |
| sponsor | TaiwanStockDividend | 3167 | 2920 | 1732 | — | 25333 | 23553633 |
| sponsor | TaiwanStockDividendResult | 3898 | 2223 | 2429 | — | 23295 | 8433312 |
| sponsor | TaiwanStockEvery5SecondsIndex | 5343 | 0 | 2 | non_session=2592 | 0 | 0 |
| sponsor | TaiwanStockFinancialStatements | 8707 | 50 | 4602 | — | 1638160 | 9863539 |
| sponsor | TaiwanStockGovernmentBankBuySell | 425 | 988 | 502 | — | 11858829 | 201201621 |
| sponsor | TaiwanStockHoldingSharesPer | 4593 | 212 | 1280 | — | 12661257 | 96351109 |
| sponsor | TaiwanStockIndustryChain | 0 | 1 | 0 | — | 6909 | 34586 |
| sponsor | TaiwanStockIndustryChainMoneyFlow | 11194 | 989 | 502 | — | 548524 | 19128177 |
| sponsor | TaiwanStockInfoWithWarrantSummary | 36 | 153 | 0 | — | 227440 | 2431616 |
| sponsor | TaiwanStockInstitutionalInvestorsBuySell | 2236 | 3107 | 2 | non_session=2594 | 168488269 | 473786864 |
| sponsor | TaiwanStockInstitutionalInvestorsBuySellWide | 4010 | 1332 | 2 | failed=1; non_session=2594 | 24589244 | 205641342 |
| sponsor | TaiwanStockLoanCollateralBalance | 5810 | 989 | 501 | — | 1965984 | 196905453 |
| sponsor | TaiwanStockMarginMaintenance | 7906 | 989 | 501 | — | 1798367 | 29005831 |
| sponsor | TaiwanStockMarginPurchaseShortSale | 3227 | 3107 | 2 | non_session=3064 | 5469002 | 166094105 |
| sponsor | TaiwanStockMarginShortSaleSuspension | 0 | 12 | 0 | — | 38846 | 188515 |
| sponsor | TaiwanStockMarketValue | 4104 | 1489 | 2 | non_session=2710 | 3465547 | 37290524 |
| sponsor | TaiwanStockMarketValueWeight | 0 | 154 | 543 | — | 153136 | 3054114 |
| sponsor | TaiwanStockMonthPrice | 8271 | 62 | 1433 | — | 2255631 | 39820009 |
| sponsor | TaiwanStockMonthRevenue | 4352 | 153 | 4499 | — | 294818 | 3030912 |
| sponsor | TaiwanStockPrice | 8428 | 0 | 2 | non_session=3254 | 0 | 0 |
| sponsor | TaiwanStockPriceAdj | 6936 | 1491 | 2 | inflight=1; non_session=3254 | 3703858 | 203248698 |
| sponsor | TaiwanStockPriceLimit | 3498 | 3107 | 2 | non_session=3159 | 6882847 | 92581093 |
| sponsor | TaiwanStockSecuritiesLending | 7787 | 989 | 503 | inflight=1 | 739429 | 8535382 |
| sponsor | TaiwanStockShareholding | 6781 | 989 | 503 | inflight=1 | 2149501 | 87449996 |
| sponsor | TaiwanStockSuspended | 3 | 13 | 0 | — | 9373 | 68238 |
| sponsor | TaiwanStockWeekPrice | 8274 | 210 | 1281 | inflight=1 | 8744655 | 138281033 |
| sponsor | TaiwanTotalExchangeMarginMaintenance | 13 | 13 | 0 | — | 3098 | 38877 |
| complement | CrudeOilPrices | 0 | 2 | 0 | — | 20297 | 128145 |
| complement | EuropeStockInfo | 0 | 1 | 0 | — | 1306 | 20370 |
| complement | EuropeStockPrice | 0 | 787 | 519 | — | 4210326 | 63705883 |
| complement | GoldPrice | 0 | 50 | 77 | — | 637069 | 3308859 |
| complement | GovernmentBondsYield | 0 | 11 | 0 | — | 96813 | 437828 |
| complement | InterestRate | 0 | 12 | 0 | — | 3518 | 32859 |
| complement | JapanStockInfo | 0 | 1 | 0 | — | 3640 | 57669 |
| complement | JapanStockPrice | 0 | 3006 | 621 | — | 16987218 | 272015050 |
| complement | TaiwanDailyShortSaleBalances | 0 | 2570 | 813 | failed=1 | 7962182 | 124797478 |
| complement | TaiwanExchangeRate | 0 | 19 | 0 | deprecated_query_shape=127 | 97143 | 963367 |
| complement | TaiwanFutOptDailyInfo | 0 | 1 | 0 | — | 1406 | 12536 |
| complement | TaiwanFuturesDaily | 1081 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanFuturesDealerTradingVolumeDaily | 1081 | 0 | 0 | deprecated_query_shape=127 | 0 | 0 |
| complement | TaiwanFuturesInstitutionalInvestors | 1081 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanOptionDaily | 265 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanOptionDealerTradingVolumeDaily | 265 | 0 | 0 | deprecated_query_shape=127 | 0 | 0 |
| complement | TaiwanOptionInstitutionalInvestors | 265 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanSecuritiesTraderInfo | 0 | 1 | 0 | — | 1043 | 36851 |
| complement | TaiwanStockActiveETFInfo | 0 | 1 | 0 | — | 40 | 2281 |
| complement | TaiwanStockBalanceSheet | 0 | 2459 | 925 | — | 8125293 | 52032630 |
| complement | TaiwanStockCapitalReductionReferencePrice | 1635 | 274 | 1475 | deprecated_query_shape=127 | 419 | 872670 |
| complement | TaiwanStockCashFlowsStatement | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockDayTrading | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockDelisting | 0 | 26 | 0 | outside_documented_range=101 | 621 | 40102 |
| complement | TaiwanStockDividend | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockDividendResult | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockFinancialStatements | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockInfo | 0 | 1 | 0 | — | 4327 | 50361 |
| complement | TaiwanStockInstitutionalInvestorsBuySell | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockInstitutionalInvestorsBuySellWide | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockMarginPurchaseShortSale | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockMarginShortSaleSuspension | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockMonthRevenue | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockPER | 0 | 2203 | 1181 | — | 7746953 | 61025840 |
| complement | TaiwanStockParValueChange | 0 | 6 | 121 | — | 16 | 16485 |
| complement | TaiwanStockPrice | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockPriceAdj | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockPriceLimit | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockSecuritiesLending | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockShareholding | 3384 | 0 | 0 | — | 0 | 0 |
| complement | TaiwanStockSplitPrice | 0 | 7 | 120 | — | 35 | 19570 |
| complement | TaiwanStockTotalInstitutionalInvestors | 0 | 23 | 0 | outside_documented_range=104 | 27142 | 430341 |
| complement | TaiwanStockTotalMarginPurchaseShortSale | 0 | 26 | 0 | outside_documented_range=101 | 18999 | 605689 |
| complement | TaiwanStockTotalReturnIndex | 0 | 2 | 0 | — | 10938 | 86762 |
| complement | UKStockInfo | 0 | 1 | 0 | — | 24339 | 287782 |
| complement | UKStockPrice | 0 | 11612 | 12725 | — | 23836671 | 399994363 |
| complement | USStockInfo | 0 | 1 | 0 | — | 19470 | 476418 |
| complement | USStockPrice | 0 | 13247 | 5263 | invalid_request=13 | 37240831 | 732254264 |

本次唯讀診斷沒有變更 downloader、runtime queue、正式 Parquet、receipt 或 service；此文件只是時間點證據，後續實作與結果另見主控代理的修復報告。
