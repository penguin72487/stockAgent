# 分鐘股份量缺單位修復（2026/10/07）

## 執行進度

**140,671 筆全部補證，剩餘缺單位 0 筆**；涉及 0050、2330 的 389 個交易日。
這兩個商品在每個受影響日期都出現在 TWSE 同日官方報表，FinMind 原生量確認為張，
按官方定義乘 1,000 轉股。已完成逐列單位補證與前後比較，耗時 258.09 秒；
沒有重跑其他 4,961 個 session。

結果以 [本輪修復收據](../artifacts/data_quality/tw_raw_source_union_20261007/dated_units_v2_repair/merge_report.json)
及 [來源與驗收 Markdown](../artifacts/data_quality/tw_raw_source_union_20261007/dated_units_v2_repair/index.md)
為準，不以服務 active 或下載百分比替代驗收。

| 驗收 | 實際結果 |
| --- | --- |
| 原缺單位／補證完成／剩餘 | 140,671／140,671／0 筆 |
| 受影響日／重現舊輸出 SHA | 389／389 日 |
| 單位證據衝突／拒收的證據檔 | 0／0 |
| 分鐘來源檔 SHA／footer／session | 506 檔全通過 |
| 單位驗收涉及的官方原始報表 | 778 份（TWSE／TPEx 各 389；這兩商品實際身份均由 TWSE 證明） |
| 0050 修復 | 67,959 筆（普通分鐘 67,364，收盤撮合 272，盤後定價 323） |
| 2330 修復 | 72,712 筆（普通分鐘 72,075，收盤撮合 272，盤後定價 365） |
| 數值／選取保全 | keys、OHLC、Amount、已知股數及 provider 選取逐列不變 |
| 試組合保留 | false；擁有的暫存已移除 |
| 原始量、正式訓練／optimizer | 不覆寫／不切換／不啟動 |
| 核心與組合回歸 | 93 passed，9.16 秒 |
| 共用下載器／receipt／原始單位回歸 | 80 passed，10.90 秒；合計 173 個測試 |

最後另以 2 個針對性測試核對全市場原始 bytes／receipt 的新單位標記與 Markdown
renderer（2 passed）；43,544 個本地 Markdown 連結、兩份輸出 receipt SHA、實際
merger 程式 SHA 及 owned temporary 清除狀態全部通過。

受影響日期的有效跨來源差異從補證前 238 筆變成 606 筆；補證後包含 556 筆
`volume_shares` 數值差異。這是以前 NULL 擋住的數量比較變得可檢驗，不是倍率
證據互相矛盾，也不是新修改價格。仍維持永豐主來源，完整雙方原值在
`conflicts.parquet`；沒有平均、強制對平或將這些差異清零。

固定 raw inventory SHA：`9b27a8dd9f68bf6a6c88b828697ad1c9b4db06505e73e98831c8042fc4dfe5a3`。
固定官方證據 inventory SHA：`8b1847b1be59008a8bf8935e6db0ab1b3a9b376b6b1c3e152175c44f27a96678`。
後者選取 3,727 份當前歷史 raw pointer，本輪只聲明實際驗證的上述 778 份，沒有
將其餘未重讀日期冒充已驗收。實際執行程式 SHA 也已與當前檔案核對一致。

## 原因與來源定義

不是缺少 140,671 筆原始成交量。FinMind 原始 `volume` 已存在；v1 合併器只准入
同日永豐相同 OHLC／正量的股份量交叉證據，沒有第二條官方單位證據路徑。
其中 140,484 筆在永豐本機歷史起點之前，另 187 筆是較晚日期的來源備援，包含
盤後定價與普通分鐘缺口，不能一概叫做下載損壞。

[FinMind 官方技術面文件](https://finmind.github.io/tutor/TaiwanMarket/Technical/)
的 `TaiwanStockKBar`／`TaiwanStockPriceTick` 定義：上市、上櫃原生量是張，
一張為 1,000 股；興櫃原生量是股。TAIEX 的原生零量是指數結構性值，沒有股票
可成交股份量。文件另說明轉板股票在 StockInfo 可能同時有兩個市場身份，且日量
可能包含分鐘／逐筆沒有的鉅額交易。因此不能用今天的身份回推歷史，也不能要求
日量與分鐘量強制相等來「擬合」倍率。

## 可重建的證據鏈

1. 固定原 v1 的 FinMind／Shioaji current-receipt 原始 inventory，不重新選 latest。
2. 用官方下載器 append-only journal 選取 TWSE／TPEx 每一日最新 outcome。
   僅 `status=data` 且包含完整 raw path／size／SHA 的 entry 才可作證據；不回退
   到舊 success 掩蓋最新 failed，也不把只有 parsed output 的紀錄當 raw receipt。
3. 驗證官方 URL、raw SHA／size、原始回應 top-level／table／params 的日期，
   重用官方 canonical parser，而不是自行從檔名填入日期。
4. 只有出現在**同日官方報表**的商品才得到 twse／tpex 身份；與 provider 官方單位
   定義結合，才生成 `volume_shares = native volume × 1,000`。
5. 同日永豐的 Amount／OHLC 股份量證據仍交叉核對。倍率相同是佐證，不同則留
   unit conflict 和 NULL，不平均、不採「先讀到的」倍率，也不跨日傳遞身份。
6. 先重新產生原 v1 parquet，逐日驗證原 `output_sha256`；再確認新版只增加原先
   NULL 的股份量，keys、OHLC、Amount、來源選取及既有股數保持不變。

方法版本為 `tw_raw_minute_provider_union_v2_dated_units`；官方原始證據輸入 SHA 與
分鐘原始 inventory SHA 分開固定，實際 normalizer／parser 程式 SHA 記在修復收據。
缺證資料仍為 NULL；TWSE／TPEx 日報沒出現不等於自動判成興櫃。

## 保存與重建

證據 sidecar 放在
`artifacts/data_quality/tw_raw_source_union_20261007/dated_units_v2_repair/sessions/<日期>/`：

| 檔案 | 證據 |
| --- | --- |
| `unit_evidence_refs.json` | 官方 URL、原始 SHA／size、完整 journal entry、parser version |
| `dated_market_share_proofs.parquet` | 同商品／同日官方市場與文件定義的倍率 |
| `same_day_share_proofs.parquet` | 官方證據與相同日期永豐佐證的合併結果 |
| `unit_proof_conflicts.parquet` | 互相矛盾的單位證據，不能任意選倍率 |
| `merge.json` | 舊輸出 SHA 重現、未知股數前後計數、非單位數值不變驗收 |

完整指令沿用 [同義原始資料合併方法](tw_raw_source_merge_2026-10-07.md#7-可重現指令)
的 `--repair-report` 流程；使用新的 report-root，不覆寫任何舊 raw、audit 或 optimizer。
往後新的 inventory 會自動收納同日官方單位證據。下載器 query／全市場物件 receipt
也共用 provider 單位定義，保留原 native volume，不直接套當前分類。

依先前要求，試組合完成後移除自己建立的暫時組合資料；留下方法與證據，不保留
新的訓練 dataset。沒有公開受限資料、傳去遠端、啟動 GPU 或切換既有正式訓練。

股份量補證不代表已取得 FinMind 沒有的 Amount、VWAP、bid/ask，也不將盤後定價
或興櫃分鐘自動計為當沖普通盤的 50% 容量。模型數值欄位仍不包含 provider／SHA。
