# 當沖候選訓練資料缺口實查

## 1. 執行進度

範圍：2020-03-02 ～ 2026-09-10；官方交易日 1592。
這是來源品質盤點，不是完整分鐘／會計驗收；沒有啟用正式訓練或線上遮罩。

**沿用網頁標準：先查實際交易與持倉需要的資料，不把整份來源目錄的缺項一律當成整段訓練失敗。**
`training_ready=false` 目前還包含年度 trainer 串接未完成，不能全部歸因於下載缺資料。

| 類別 | 實查數量 |
| --- | ---: |
| 有合約但來源缺日 | 127 股票日 / 89 檔 / 27 日期 |
| 無現行合約，但期間有日成交資料 | 71194 股票日 / 124 檔 |
| 上述無合約股票日已有本機 receipt 分鐘證據 | 156 |
| 換股參考資料成功解析（含已公告的未來復牌） | 247 |
| 換股參考資料仍失敗 | 20 |
| 換股 request/response 已核對 | 643 |

無現行合約不等於歷史不存在；已有分鐘證據不等於已進入訓練資料。
換股來源按恢復交易日查詢，包含已公告但尚未到來的復牌事件；不聲稱已觀測未來行情。

## 2. 除權息分類（不是全部精確入帳證明）

| 處理 | 原因 | 事件數 |
| --- | --- | ---: |
| avoid | etf_terms_unavailable_or_noncash_action | 1 |
| avoid | mops_cash_terms_unavailable_or_complex | 113 |
| avoid | noncompany_action | 299 |
| avoid | stock_or_subscription_action | 1719 |
| exact_cash | exchange_etf_exact_cash | 3960 |
| exact_cash | mops_exact_cash | 8926 |

`avoid` 是條款分類，不是下載失敗次數，也不是全域阻擋數。
有舊持倉跨過事件才需要該筆精確股數／現金條款；無舊持倉不領取該次權益。
不能為了避開企業行動而回溯刪掉持倉、改寫訊號，或偷偷強制無限量平倉。

## 3. 最新區間來源缺日

| 股票 | 日期 | 狀態 |
| --- | --- | --- |
| 00643K | 2026-05-05 | unresolved |
| 00643K | 2026-06-16 | unresolved |
| 2321 | 2026-07-30 | unresolved |
| 3629 | 2026-09-10 | unresolved |
| 5878 | 2026-08-20 | unresolved |

## 4. 換股未解決事件

| 股票 | 恢復交易日 | 原因 |
| --- | --- | --- |
| 6271 | 2022-12-12 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 2321 | 2023-02-17 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 1512 | 2023-03-13 | no matching official issuer replacement announcement in bounded archive |
| 6541 | 2023-03-13 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 4960 | 2023-04-24 | issuer cash reduction lacks an explicit cash-per-share amount |
| 2022 | 2023-08-28 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 3321 | 2023-08-31 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 1236 | 2023-10-11 | issuer cash reduction lacks an explicit cash-per-share amount |
| 2489 | 2023-10-11 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 2535 | 2023-10-30 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 3308 | 2024-04-01 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 2489 | 2024-09-30 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 3041 | 2024-11-11 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 5515 | 2024-11-11 | issuer cash reduction lacks an explicit cash-per-share amount |
| 3494 | 2024-11-25 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 2025 | 2025-02-12 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 2489 | 2025-10-07 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 3057 | 2025-10-07 | no matching official issuer replacement announcement in bounded archive |
| 2314 | 2025-10-20 | issuer replacement lacks an explicit unambiguous shares-per-1000 ratio |
| 9927 | 2025-11-24 | issuer cash reduction lacks an explicit cash-per-share amount |

## 5. 遮罩邊界

只略過明列的股票日新單，不重分配權重、不刪整天、不修改價格。
跨日股票庫存碰到整日分鐘來源缺口仍拒絕；不能清空庫存、歸零損益或捏造平倉。
已確定金額／付款日的現金應收應付不需要股票行情，保留原帳並按日結算；未解或修訂條款仍须查核。
企業行動避開期須另有公告及可成交平倉證據，不能僅遮住恢復交易日。

## 6. 限制

- No full symbol x 270-bar coverage certification.
- Positive daily volume is not proof of a regular-session 09:01 fill.
- Retained minute marks require canonical ingestion, volume and execution validation.
- A minute-source mask cannot erase physical inventory; exact cash claims settle independently.
- Unheld action catalogue gaps are not global blockers or permission to mask held rights.
- Source adapter and full annual carry trainer remain pending.

## 7. 保留真實分鐘來源比對

2026-09-09：2330, 0050, 0056, 2321；來源 797 根，
共比對 1080 個分鐘格；缺少首次觀測 236 格維持 NaN，
明示沿用先前成交價 47 格；與既有 paper loader 完全一致。
來源與限價檔雜湊前後不變；這是來源估值測試，不是實際模型績效或成交驗收。
