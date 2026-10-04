# FinMind 呼叫效率修正 — 2026-09-27

後續更新：原先本機設定的「5 年／3 月」上限已取消；改為合併最長連續待抓
區間，並補實測長區間、最後一天邊界及資源失敗縮段。
最新契約與正式下載驗收見 [最大查詢區間](finmind_maximum_range_2026-09-27.md)。
以下 07:xx 的數字是前一階段觀察，不是目前進度快照。

## 實測結果

目標是提高「有效資料／合法 API 請求數」。沿用既有 queue、共享 limiter、
帳號配額、增量預留、Parquet、收據與 systemd，未動交易／行情／Discord。

- 07:10:42 台北，正式 queue 私有備份重播：pending **179,955 → 159,750**，
  少 **20,205** 個尚未發出的休市日請求，約 11.2%；非空分區重分類 **0**。
  以 6,000 次／小時換算約 3.37 小時配額，**不是實測完工時間**。
- 18,305 個原本已查過的財報／營收空日改列 `not_observation_date`。
  這不是額外節省的請求，也不是已下載資料。
- 實際合批查詢：景氣指標兩年 24 列、CNN Fear & Greed 兩年 504 列，
  各一次 HTTP，與原兩年 Parquet 的已驗證 union 逐列一致。
- 07:12:28 重啟 Sponsor／Complement。初期新收據包括資產負債表
  `2012-12-31` **120,883 列／一次全市場查詢**。這是回應量，不是全公司完整證明。
- 07:16 後續觀察：本輪財報已有98個非空期別回應、最早 `1990-03-31`；
  月營收145個非空期別回應、最早 `2002-02-01`。這些包含最近期別刷新，
  不能把總列數全部說成新增加的不重複資料。
- 新啟動兩服務 `active`、`NRestarts=0`；初期沒有新 failed／blocked。
  公開唯讀 `/finmind/api/status` HTTP 200、128 個目錄項目。歷史仍在抓取。

證據：

- [優化前效率與品質基線](finmind_efficiency_audit_2026-09-27.md)
- [零 API 排程副本驗證](../artifacts/data_quality/finmind_request_plan_20260926T231042139318Z.json)
- [三次有配額計帳的實際查詢驗證](../artifacts/data_quality/finmind_efficiency_probe_20260926T230509943641Z.json)

## 查詢與排程

| 資料 | 實作 | 保護邊界 |
|---|---|---|
| 財報三表、月營收 | 全市場季末／月初 anchor 優先；近期每四小時追新、舊期別每30日修訂掃描 | 期別日不是發布時間，輪詢間隔不是官方發布時刻或 PIT 證明 |
| 2014年前未查證非期別日 | 保留必要待抓、排在有效期別後 | 不因現代範例或單一股票樣本就宣稱全市場古老日期皆為空 |
| 已核實交易日來源 | 排除官方日曆範圍內休市日 | 非空衝突使整來源退出排除；日曆外日期全部保留 |
| 景氣指標、CNN指數等已驗區間來源 | 最長連續必要待抓年月；不設固定年數上限 | 不跨已完成、冷卻或缺口；跨必要背景優先級合併，不挪用增量預留 |
| Option VIX | 最長連續待抓月份；64 MiB 解碼回應／100萬列本機保護 | 資源超界或逾時才縮段重試；不宣稱這是官方大小上限 |
| 月K、週K、集保持股分級 | 保留原日期查詢 | 確有非月初、週三／四／六資料，不能套固定日期刪除 |

批次 allowlist 只涵蓋官方區間端點且已驗證 inclusive `end_date`。整批所有
日期驗證通過才逐原分區落地；收據記錄共同 HTTP 邊界。區間契約／400錯誤
留下 audit，停用該來源合批，60秒後走原單次查詢；不封鎖正常單次下載。
部分落地失敗只重試未完成部分。

新增交易日檢查11類：外資持股、借券成交、融券賣出餘額、官股買賣、10年
交易日均價、鉅額交易、借貸擔保餘額、產業鏈成交金流、融資維持率、可轉債
日成交、可轉債法人交易。依交易／帳務觀測日語意及本機非空日期核對，不是
假設發布只能在交易日。未套用期貨、Active ETF、市值權重、可轉債總覽。

日曆證據 `1999-01-05～2026-09-24`、6,871個交易日，SHA-256：
`742876c3307be444fdad9b1a29a405cb0b778bb7a4d968f62d99361718a855b5`。

發布排程補分鐘精度：借券15:00、官股23:30、融資維持率22:30；實際晚到
仍保留追新，表定時間不等於資料已發布。

## 正確性與復原

- 修正把昨天當作 current 的比較；已確認休市不短間隔空抓。真正／未證實
  休市的近期空日仍有七日、每四小時晚到回查。這個有限窗口不保證任意晚到
  都能捕獲；財報／營收另有較長期別刷新與修訂路徑。
- 非空歷史重查若回空，不覆寫 last-good 收據；保留異常回應並重試。
- 刷新前把舊收據原 bytes 保存到內容定址 `receipt_history`；原始Parquet保留。
- 法人long一個內容定址檔實際0 bytes，新增驗證後只重排該parent的修復路徑。
  ledger／原收據 bytes 留audit；遇相同digest的損壞檔，保存原壞bytes及
  repair收據後原子替換。這不是全庫hash驗證，也不是全庫已完整。
- 上述 `2023-08-21` 已在新worker實際修復：long 78,863列，wide 15,783列，
  各自新Parquet與收據SHA一致；原0-byte壞檔及repair證據仍保留。
- 配額沿用實際 Sponsor **6,000次／小時**，共同限流、錯誤冷卻及增量預留；
  沒新增 SponsorPro探測，新聞仍關閉。

## 驗證與重現

相關回歸 **250 passed**，涵蓋日期例外、未驗證舊日期、跨午夜晚到、空回保護、
收據留存、分鐘發布邊界、批次原子claim、整批日期驗證、partial storage failure、
壞批次降回單次、來源修復、配額、單位及面板。py_compile／diff --check通過。

```bash
source scripts/runtime_env.sh
# 零API，不寫正式queue
run_fintech_python -m scripts.audit_finmind_request_plan
# 最多三次data API，共享limiter與增量預留
run_fintech_python -m scripts.probe_finmind_efficiency
```

## 官方來源

- [基本面](https://finmind.github.io/tutor/TaiwanMarket/Fundamental/)：全市場特定日期、財報／營收欄位與期別範例。
- [籌碼面](https://finmind.github.io/tutor/TaiwanMarket/Chip/)：交易／帳務觀測日及更新時刻。
- [技術面](https://finmind.github.io/tutor/TaiwanMarket/Technical/)／[可轉債](https://finmind.github.io/tutor/TaiwanMarket/ConvertibleBond/)：交易日均價、日成交及法人資料。
- [OpenAPI](https://api.finmindtrade.com/openapi.json)：data_id單一字串；SDK多ID平行迴圈不是單次HTTP。
- [官方完整說明](https://finmind.github.io/llms-full.txt)：全日tick／KBar物件下载屬SponsorPro，不是目前Sponsor權限。
