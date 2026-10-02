# FinLab 追新不受本機回補配額門檻限制

2026-10-02；依使用者本次要求調整既有 FinLab 管線。不是取消官方帳號限額，也不代表所有更新都是小檔案或已完成所有歷史。

## 實作結果

先區分「有已驗證基底的 SDK 更新」與「首次歷史／完整物件重抓」。發布到期、資料驗證、來源權限、官方實際拒絕與追新完成是不同條件；不能讓回補預算替代來源是否可更新的判斷。

| 工作 | 本機 50 MB 回補門檻 | 來源實際限額與錯誤 | 帳號用量觀測 |
| --- | --- | --- | --- |
| 已驗證基底的 SDK 追新 | 不檢查、不阻擋；未知或低餘額也允許來源查核 | 仍由 SDK 判定；實際 quota／登入錯誤停止本輪 | 既有每分鐘快照；不為每筆追新另加帳號查詢 |
| 首次歷史、損壞基底修復 | 保留 | 保留 | admission 查核與每分鐘觀測 |
| 三個目前仍整表傳輸的 Arrow adapter | 保留，簽名之前另有完整物件成本及磁碟守門 | 保留 | 同上 |
| Tick | 原有最低優先序及剩餘配額策略不變 | 保留 | 每分鐘觀測 |

三個完整物件 adapter 為 `broker_transactions`、`after_market_fixed_price:市場別`、`after_market_fixed_price:資料來源`。它們目前不走 SDK delta；少量新列不等於少量網路傳輸，不能以「追新」名稱誤放行巨量整表。

一般 SDK 更新也可能因基底／來源 hash 驗證失敗而回退完整抓取。沿用 SDK 原生 merge、原始值、來源查核與不可變 Parquet 收據，不移除驗證換取虛假的更新成功。[FinLab 官方資料 API 文件](https://finlab.finance/docs/en/reference/data/)

## 排程及預估

- 共用 `finlab_acquisition_contract.incremental_quota_exempt`，下載器、workload、公開唯讀 projection 使用同一分類。
- 批次上限計算真正嘗試次數，不計配額暫緩的掃描項目。低配額的歷史前綴不再擋住後方可執行的 SDK 追新。
- 追新仍依 SDK 綁定的到期時間與既有 timer；不因取消本機門檻就反覆抓未到期資料。到期不是正式發布／PIT 證明。
- 預估保留官方總容量情境，不對 SDK 更新扣本機回補保留量。歷史等待配額時，已就緒或較早到期／重試的增量工作可先執行，不直接跳過到隔天重置。
- 缺增量傳輸樣本仍保守估整表，不虛設零成本。因此 ETA 仍是有假設的情境，可能比實際 metadata-only／小型更新慢；不把 ETA 當作下載器的增量 admission。
- 修正「舊資料有效，所以忽略後來的權限失敗」：舊資料仍可用，最新來源更新失敗另外列出、完成時間未知。較新的成功來源查核才會解除該次失敗；重試時間依下載器同一 backoff 計算，跨官方重置仍遵守既有重新 admission 規則。
- 選擇器暫時排除的 metadata 冷卻仍是已排自動重試，不誤標「未排程」；ETA 納入它的真實 retry clock，完整物件成本門檻不放寬。
- workload report contract **3**、quota policy version **1**；footer measure contract 2 未改，可重用 v2 measurement cache。不變更資料／訓練 feature ABI。

## 面板

沿用 [既有 FinLab 頁面](https://penguin72487.ddnsgeek.com/finlab/) 的版面、來源、每分鐘觀測及唯讀 API，不另建網站或讓瀏覽器呼叫 SDK。

- 「歷史／整表保留後可用」不再暗示 SDK 追新也會被該數字擋住，明示本機門檻豁免與官方限額邊界。
- 沒有本輪待辦的已查核階段顯示「本次已查核」，不因另一階段的來源拒絕而顯示「暫無可估時間」。
- 來源全空／權限問題仍未知；100% 可估查核工作量不等於完整歷史、全來源完成或訓練可直接上線。

## 當次觀測及驗收邊界

台北 16:35–16:46 的清冊為 1,114 鍵，排除分鐘／Tick 範例後 1,112 鍵；1,107 鍵適用已驗證 SDK 更新規則，其餘 5 鍵為三個完整物件 adapter 與兩個缺有效值來源。1110 個目錄鍵有有效舊資料收據，不等於所有歷史／所有欄位完整。

16:35 的真實來源查核 `etl:hk_market_value` 成功且內容未變：收據 `source_check_mode=upstream_incremental`、`last_check_result=unchanged`；來源查核時間前進，但原 `fetched_at_utc=2026-10-02T01:41:52.638078+00:00` 未冒充新抓取。該次執行確實記錄一筆 `incremental_quota_exempt_attempted`。之後自然到期工作仍由既有服務執行，未強制提前下載驗收。

16:46–16:51 的下一輪又完成六筆 `after_market_fixed_price` SDK 來源查核：成交價、成交張數、成交筆數、成交金額、最後揭示買量、最後揭示賣量。六筆均 `upstream_incremental`／`unchanged`，保留原 12:29–12:33 抓取時間。市場別／資料來源兩個整表 adapter 則如實留下 `resource_deferred`，未繞過完整物件 reserve。服務於 16:51:18 正常退出（exit 0），15 分鐘 timer 與每分鐘配額 timer 繼續維持。

16:46 的每分鐘帳號觀測使用 4,826.023／5,000 MB、剩 173.977 MB。這次真實餘額尚高於 50 MB；低於門檻／未知餘額的豁免由針對性測試驗證，不把它說成已在正式低配額環境實測。

仍有來源問題：`dividend_otc:權息`、`management_change_events:變更交易開始日` 全空；`important_info_announcement` 的較新查核收到 `vip_only`，保留舊有效資料並照既有節奏重試，不改成成功。沒有繞過權限、清除資料、變更交易服務、發布新訓練包或宣稱歷史已抓齊。

- 相關 FinLab／SDK／Arrow／分鐘合成／單位／admission／UI／公開 gateway 回歸：**324 passed**。
- Ruff、shell／JS 語法、差異空白檢查通過。
- 公開 HTTPS 1440／1024／390 px 實際渲染：六階段、18 個 ETA 欄，政策與 DTO 相符，已完成階段不顯示假未知，無 JS errors、無頁面橫向溢出。使用 CPU 2D browser profile，不宣稱 GPU／WebGL 驗收。
- 低額度、未知額度、損壞基底、整表保護、歷史前綴、後來到期的追新、官方拒絕、舊權限失敗被新成功取代、真實重試時計及過期 UI 都有對應測試。不宣稱本次提升了多少下載吞吐量或節省了多少官方流量。

證據：[回歸 JUnit](../artifacts/data_quality/finlab_incremental_quota_2026-10-02/regression.xml)、[focused JUnit](../artifacts/data_quality/finlab_incremental_quota_2026-10-02/final_focused.xml)、[公開渲染收據](../artifacts/data_quality/finlab_incremental_quota_2026-10-02/public/rendered_acceptance.json)及同目錄截圖。

可重跑：

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q test/test_finlab*.py test/test_derive_finlab_minute.py test/test_stock_volume_units.py test/test_acquisition_policy.py test/test_public_dashboards.py
run_fintech_python scripts/verify_finlab_stage_dashboard.py --output artifacts/data_quality/finlab_incremental_quota_2026-10-02/public
```

若需執行已授權的正常取得流程，可啟動既有 `stockagent-finlab-local-refresh.service`；不用改 `.env`、重新登入或另開下載器。不要為驗收強制抓未到期資料。
