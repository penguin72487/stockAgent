# FinMind 一分鐘 ETA 與失敗尾端修復（2026-10-02）

修復／驗收契約：`finmind_eta_retry_repair_v1`。沿用既有 Free、Sponsor、Complement、共用帳號限速、佇列、收據、分鐘 quota timer 和唯讀網站。新聞與逐檔刷新契約升為 2；本機 ETA 快照契約升為 3。

結論：**預估有誤導，下載也確實未完成**。請求持續發出不代表失敗工作已消化；一分鐘的尾端工時或舊卡片的理論請求投影，不是全歷史完成時間。

## 先從實際流程驗證

來源回應 → 驗證 → Parquet／收據 → queue 狀態 → 背景工作量快照 → 唯讀投影 → 網頁。

本次不是再次修昨天的 SQLite／查詢粒度問題：既有 `finmind_service_eta_repair_2026-10-01.md` 及當前 timer 證明該修復已部署，這次 ETA 有每分鐘更新。新的根因在成功判定、失敗排程與時間的呈現。

| 問題 | 實際證據 | 修正 |
| --- | --- | --- |
| 單筆稀疏 metadata 拒絕全日新聞 | 2020-07-28 回應 388 筆，僅 1 筆缺 `link`；整日被記成 `storage_or_schema_error`。修復前新聞有 247 個失敗日。 | 缺網址／股票映射保持來源原值並計數，不刪文章、不捏造連結。日期、查詢日、非字串 ID／URL 型別及非 HTTP(S) scheme 仍驗證。 |
| 空增量被當成空全歷史 | UKStockPrice 有已驗證歷史基底，增量查詢空回卻觸發 `unexpected_empty_after_nonempty`；162 個任務反覆重試。 | 僅已驗證的增量 overlap 空回保留基底，明記 `retain_verified_baseline_no_new_observation`。全歷史空回、ID 錯配、重複日、錯誤日期仍拒收。 |
| 同級重試被待抓搜尋餓死 | 8 個 2012–2013 新聞失敗日已到期，但同一 priority=6 的未查日期始終先被選；實際仍在查 1942 年候選。 | 相同 lane 內先選已到期失敗修復，再新待抓，再到期重驗。既有跨 lane 優先級、p0 dataset 輪替、委派、准入與冷卻不變。 |
| 發出速度冒充成功速度 | 小型失敗尾端被「剩餘 calls ÷ 全帳號 request starts」投成短工時，下一分鐘仍可重現相同短工時。 | 重試數隨實際階段盤點，等待 `waiting_retry` 明示未知完工時間；已知冷卻到期只是可排，不是完成。 |
| 卡片及期限顯示誤導 | 舊 `unbatched_task_projection` 的分割數／官方上限被放在完成時間區；期限過後、快照五分鐘內仍可顯示約一分鐘。 | 移除卡片的反事實分鐘數，保留舊 API 相容欄位；超過正工時估計期限立即標 `overdue`／尚未完成。過期、不明、重試與真實零工作分開。 |

新聞仍採全市場單日一次；不改成逐股重複查詢。[FinMind 官方新聞文件](https://finmind.github.io/tutor/TaiwanMarket/Others/)。1900 是既有搜尋下界，**不是證明 FinMind 有 1900 年的新聞**。空回是來源回應證據，不是非空資料，也不是永久無資料證明。

## ETA 的量與時間不能混用

- 分割／任務數、請求數、資料列數是三種量；合批、分流與空回使它們不相等。
- `request starts/hour` 是共用容量觀測。只有成功收據能證明該工作被查驗；成功空回與非空入庫也分開。
- 三情境工時為已知請求成本與發布／到期插隊模型的條件式估計。失敗等待時間未知時，不把有限工時加在「現在」而捏造完成日期；後續階段也不能越過未知前置時間。
- 失敗數按 backfill／incremental／validation／有限優先 override 分配，搬移時扣回原階段，不重複計數。冷卻上界也按實際類別取樣。
- Free 在既有 receipt 掃描順便產生 retry 數與最早／最晚 retry 時間。ETA／瀏覽器讀小型狀態與 indexed queue，不為顯示掃 Parquet 或呼叫 FinMind。
- 無已知待發工作不是全來源永久完成；在途、本機衍生、阻塞、未知歷史、未來發布另列。指定優先期貨目前無待發時，也不把英股追新尾端標成期貨未完成。

## 部署與實際資料驗收

1. 15:01:50–15:01:51（台北）重載 Free／Complement 與唯讀 gateway；Sponsor 未重啟。既有分鐘 timer 自動使用新快照邏輯。
2. 15:17:57 對兩個受影響 dataset 的重載後成功任務逐一檢查 queue／收據身份、列數、非空 Parquet footer、大小與 SHA-256：**404 個非空分區全部通過，0 個驗收錯誤**。
   - 新聞：242 個非空日，289,519 列；另 856 日來源空回。這些是重載後受驗成功任務，包含修復與正常續抓，不把所有筆數都歸因於修復。
   - 英股：162 檔、372,988 列既有基底原樣保留。**新增來源列數是 0**；每個 hash、舊收據與日期一致，不把查詢空回冒充新行情。
   - 2020-07-28 的 388 筆保存後與先前來源診斷回應逐列 multiset 相同；缺連結的 1 筆也保留，品質 metadata 如實記錄。
3. 15:22:21 通過排程回歸後只重載 Complement。8 個被餓死的新聞日於 15:22:25.315–15:22:29.121 取得，共 218 列；沒有清空／重設收據或縮短來源冷卻。
4. 新聞失敗 **247 → 0**、英股失敗 **162 → 0**。實體 Complement queue 仍保留一筆既有 `TaiwanDailyShortSaleBalances/http_504`；該資料已委派 Sponsor，不能把舊 owner 的失敗再算成當前主責缺口，也不能聲稱所有實體 queue row 沒有失敗。15:30 再查實際委派與主責狀態：Sponsor 此類 5,226／5,226 個任務、失敗 0，來源到 10/1；不因此宣稱尚未發布的 10/2 資料已有。
5. Free 的兩種盤中 session 已各完成 5,347 個可排交易日，來源最新為 2026-10-02。最新日於 15:01:04 完成，**早於本次重載，不把它當本次修復成果**。

沒有重啟交易、訓練或儲存服務；沒有刪除資料、修改股數單位、回填價格或更新模型 ABI。服務 active／HTTP 200 之外，驗收依據是上述來源回應、收據與實際 Parquet。

## 15:23 快照：還要多久

| 範圍 | 已知剩餘查詢候選 | 最快／中央／保守完成時點（台北） |
| --- | ---: | --- |
| 到期追新／指定優先回補 | 0 | 目前無已知待發，不等於未來不再發布 |
| 主要歷史／日資料／財報／總經／新聞 | 24,340 | 10/2 19:57／21:03／21:30 |
| 其餘分鐘／分點明細 | 60,242,110 | 獨立長期條件式投影，不當成主要資料今天的 ETA |
| 最低優先 tick | 17,570,436 | 獨立長期條件式投影，不排在主要資料前 |
| 剩餘跨來源校驗 | 0 | 此刻沒有已知待發；未來校驗另計 |

此時主責估算 retry=0，回到 `conditional`，不是 `current`／全歷史完成。三個 worker 仍共用實際帳號觀測的 Sponsor 6,000 次／小時，沒有各自獨占同一配額。

上表為截止快照的查詢成本，不是缺資料筆數。約 7,784 萬次主要來自日期 × 代號候選，包含未剔除的上市前／休市日；未驗證完整容量或最早可取日。新聞也在搜索來源可能沒有資料的舊日期。長期明細／tick 日期只是此上側候選模型的條件式輸出，不能當實際全資料完工承諾。

後續來源修訂、API 失敗、停機、帳號額度變化、新代號與磁碟 guard 都可能延長上述今晚估計。遇到重試再次顯示等待，而不是每分鐘重新承諾「一分鐘後」。最新狀態看 [FinMind 面板](https://penguin72487.ddnsgeek.com/finmind/)。

## 測試與可重查證據

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q test/test_finmind*.py \
  test/test_probe_finmind_max_ranges.py test/test_tw_futures_finmind_ticks.py \
  test/test_market_status_calendar.py test/test_public_dashboards.py
run_fintech_python -m scripts.snapshot_finmind_quota --eta-only
```

最終 **884 passed**。涵蓋稀疏新聞保存、嚴格日期／型別守門、空增量保留與空全歷史拒收、冷卻與優先級不繞過、重試工作不重計、過期／逾期顯示、DTO allowlist、累計里程碑、發布日曆及 shared quota。Ruff、JS 語法和 `git diff --check` 通過。

實際 localhost 頁面在 1440／1024／390 寬度渲染，五階段表、六類篩選、1h／1d 流量切換皆驗證；無 JavaScript page error、無水平溢出。使用 CPU 2D browser profile，沒有宣稱 GPU WebGL 驗收。公開端點亦確認唯讀、五階段及新版錯誤／重試文字已提供。

修復尾端成功後，另用真正公開 URL 在 1440／390 寬度重驗，確認 retry=0、根狀態 `conditional`、首階段無待發與主要歷史階段的有限 ETA 都已呈現；再次無 page error／水平溢出，亦檢視實際截圖。這不是只拿合成 DTO 或 HTTP 200 當部署成功。

證據目錄：[finmind_eta_repair_2026-10-02](../artifacts/data_quality/finmind_eta_repair_2026-10-02/)。包含 `regression.xml`、`pre_reload.json`、`post_reload_receipt_acceptance.json`、`post_scheduler_reload.json`、`final_live_status.json`、`rendered_acceptance.json`、`public_endpoint_acceptance.json`、`public_post_recovery_acceptance.json`、`delegated_legacy_failure.json` 及桌機／手機截圖。來源診斷回應留在本機，不新增公開原始文章或憑證入口。

本次證明指定錯誤修復與監測呈現正確，**未宣稱全市場全歷史已抓齊、資料可直接 PIT 訓練或所有來源的最早年份已證明**。
