# FinLab 分階段 ETA、增量查核與防飢餓排程

2026-10-01 實作；下列狀態觀測時間為台北 18:50–18:53。這是本次觀測待辦的條件預估，不是所有歷史／未來資料的完成日。

> 2026-10-02：[追新配額政策更新](finlab_incremental_quota_policy_2026-10-02.md)已將已驗證 SDK 追新與歷史回補門檻分離，workload report 升為 v3。下列 v2 設計與低配額等待為當時觀測，不是目前所有追新的阻擋規則。

## 結果與現況

沿用 [FinLab 面板](https://penguin72487.ddnsgeek.com/finlab/) 與既有下載器、每分鐘配額快照、systemd timer。新增六個階段的工作量、不同粒度筆數、快速／參考／較慢完成時間、下次檢查與阻塞原因；公開讀取不呼叫 SDK 或讀取金鑰。

目錄 1,111 鍵，一般整表 1,109 鍵（排除分鐘／Tick 範例鍵）。1,107 鍵已有正值資料收據；本輪可估待辦 59 鍵，另有 2 個來源全空鍵。來源大小加權查核進度 85.54%，不是歷史缺失率或已實際下載流量。

| 階段 | 本次待辦 | 預估傳輸 | 快速 | 參考 | 較慢 |
| --- | ---: | ---: | --- | --- | --- |
| 到期市場行情（含收盤價） | 1 鍵 | 28.1 MiB | 10/2 08:00 | 10/2 08:00 | 10/2 08:00 |
| 未取得的非阻塞歷史表 | 0 鍵 | 0 | 無本次待辦 | 無本次待辦 | 無本次待辦 |
| 來源／市場標籤，已逾期提升優先序 | 2 鍵 | 265.0 MiB | 10/2 08:03 | 10/2 08:04 | 10/2 08:05 |
| 其他到期特徵追新 | 56 鍵 | 544.1 MiB | 10/2 09:12 | 10/2 09:16 | 10/2 09:21 |
| 來源全空／權限／錯誤 | 2 鍵 | 重試樣本另列 | 未知 | 未知 | 未知 |
| 全市場 Tick | 歷史可得範圍待驗 | 未量測 | 未知 | 未知 | 未知 |

時間皆為台北時間，畫面顯示至分鐘；JSON 保留精度不代表預測準確到秒。開盤前的完整 key timeout 與終止寬限必須放得進 08:18 前；預估使用同一開盤資源保護與 09:10 恢復規則。休市仍可使用重置配額，未知日曆的平日維持開盤保護。

目前帳號使用 4,956.523 / 5,000 MB，剩 43.477 MB，低於一般下載保留量 50 MB。預計下一次重置 10/2 08:00；timer 的固定重置啟動為 08:00:05，實際須先確認帳號重置。沒有為驗收增量功能而繞過保留量花剩餘配額。

本機完整原始檔工作權重 6,070,989,094 bytes（約 5.65 GiB），本次可估待辦 877,774,550 bytes（約 837.1 MiB）。MiB 為 1024² bytes；帳號 MB 仍沿用既有下載器的官方帳號欄位及其換算，不與 Parquet 壓縮容量混用。重讀非空數值格、事件列、標籤格分開，絕不相加成「總筆數」。

## 根因與修正

1. **強制整表繞過已存在的增量機制。** 一般 sync 改為窄範圍 `checked_get`，只使目前 key 的 SDK 到期快取失效，使用 SDK 原本 `.recent` 合併及雜湊驗證。每次先清除舊 publication marker；沒有新的、與 cache revision 綁定的 marker，就回退 `force_download=True`。手動完整抓取仍保留。不是自行猜下載網址或重寫 merge。
2. **沒變更仍重轉整表。** SDK 驗證的 publication hash、SDK／正規化版本、volume 單位契約與本機 Parquet integrity 同時符合，才跳過轉存／重算 SHA；原始 fetched time 不改寫成今天。SDK 不提供新雜湊時清除過期 hash 證據，改走正常序列化。內容改變仍保留 content-addressed 新版與原收據。
3. **所有表每天盲目重抓。** 收據新增 SDK 實際回傳且綁定來源查核的 `next_source_check_at_utc`。有有效到期時間就照它追新；既有收據／未知到期時間保留每日 08:00 配額週期 fallback。不猜財報統一季頻而錯過提前申報。到期時間只是下一次檢查建議，不是正式發布或 PIT 證據。
4. **大表忽略同日更新。** 三個 bounded Arrow key 也傳遞 provider expiry；raw normalization 重試只在到期前可重用來源證據，不再以「今天抓過」掩蓋下午更新。
5. **收盤價被當作可選校驗。** 收盤價更新現在是必要行情，不再等待全來源校驗 admission。真正跨 provider 的校驗 gate 不變。FinLab 不覆寫其他官方來源或已公布訓練 ABI。
6. **每日新增追新使標籤永久飢餓。** 行情優先，其後缺失歷史、最舊特徵、標籤、來源重試、Tick；跨完整配額週期仍未查核的標籤提升至最舊特徵修補，數值缺失仍優先。collector 與 forecast 共用規則。
7. **ETA 凍結今天的工作。** v2 以 heap 模擬每日重置、首個已觀測 SDK 到期時間、之後保守 daily fallback、失敗 retry、開盤保護與新到期優先工作。各階段保留各自累計完成時間；任一後段容量不足或缺樣本，不抹掉前段已可估的 ETA。14 日／20,000 次硬上限與容量判定避免無限重算。快速 ×0.75、參考 ×1；較慢 ×1.5 且只分得 50% 配額，不是統計信賴區間。
8. **監控重複解讀收據及解析相同路徑。** 單次規劃的 ContextVar read cache 隨該次規劃清除，不持久快取 freshness；重用已驗證下載判斷避免重複驗證，root path 每次快照只解析一次。Parquet 仍是簽章綁定的 footer cache、預設掃描預算；公開網頁只讀 allowlist 快照。
9. **Tick 固定佔住未必要的配額。** 改為只保護本次重置前有排程的高優先檢查加 50 MB floor；未知成本保留 500 MB，使用者明訂的保留量仍為 floor。權限／資源／其他技術錯誤不得假裝完成。只有 all-null report、key、路徑範圍與實際 raw SHA 一致的證據，可以不永久阻擋無關剩餘 Tick；`required_complete` 與全來源校驗／發布 gate 仍維持 false。

SDK 功能與 clock 說明參照 [官方 data 文件](https://finlab.finance/docs/reference/data/) 與 [官方版本紀錄](https://finlab.finance/docs/change-log/)，實作依本機安裝的 2.1.1 API，不假設新版文件中尚未安裝的方法可用。

## 契約及部署邊界

- workload report contract 升為 **2**；原 footer measure contract 2 不變，既有 footer cache 可遷移。公開 projection 不接受舊 v1 的凍結 ETA。
- 收據新增 `upstream_incremental`：表示真的查過來源，不是 SDK local cache hit；全目錄發布仍要求 24 小時查核、實際 SHA 與原範圍。不放寬授權、來源完整度、冷庫或遠端 READY。
- 模擬目標是**快照時已到期的非阻塞待辦**；後來比它優先的工作會插隊並耗額度。不是未來每一天所有新資料的終點。新一日到期後會由下一分鐘快照重新估算分母與 ETA。
- 觀測到的 SDK payload 是估計，不是 HTTP 帳本或官方計費。未量測或僅 metadata-only 不變的樣本不當作未來零成本；無可靠傳輸樣本仍估整表。因此下次實抓後 ETA 可能明顯改變。
- `dividend_otc:權息`、`management_change_events:變更交易開始日` 仍有 all-null 原始證據，繼續重試、不製造數值或成功日期。這兩個來源及全市場 Tick 不能承諾完成日。
- 更新了 `stockagent-public-dashboards.service` 的唯讀 reader；未重啟交易服務、修改交易／feature 內容、清除來源資料或發布新的訓練資料。

## 驗收與效率證據

- 相關 FinLab、Arrow、分鐘合成、股數單位、acquisition admission、public gateway 回歸 **294 passed**。靜態 Ruff F/E9、JS syntax、shell syntax、diff whitespace 均通過。
- 公開 HTTPS 真實渲染驗收：1440／1024／390 px 均 6 個階段、18 個 ETA 格、無 JS errors、無頁面橫向溢出。保留 screenshot；採明確 CPU 2D compositor profile，不宣稱 GPU／WebGL 驗收。測試遵守 CSP，未放寬 CSP 或使用 force click。
- 同一批資料、同一 as-of／配額／小型 scheduling artifacts、既有 footer cache，交錯 baseline/candidate 六組：CPU 中位 **1.32244 → 0.88591 秒（降低 33.01%）**；wall 中位 **1.32271 → 0.88615 秒**；每組 59 個待辦、0 footer 掃描。baseline 是 Git `862fc3e08b156a411d1b64f54e925faed44247cc`，source SHA 保留於 artifact。這不是網路下載速度或全系統效能保證。
- 初次非交錯測量反而較慢（1.405 → 1.552 秒 CPU），該證據也保留，未挑最快單次宣稱改善；最終採固定 artifacts 的交錯測量。
- 尚未進行新的實際下載：今日帳號保留量不足。增量傳輸與新到期時間的節省須於下次配額重置後由新收據確認，不以 mocks 當作已取得資料。

證據目錄：[finlab_stage_eta_2026-10-01](../artifacts/data_quality/finlab_stage_eta_2026-10-01/)，包括 `local_status.json`、`computation_benchmark_v2.json`、初次測量、browser runtime 與 `public/rendered_acceptance.json`／截圖。

可重跑驗收（不抓 FinLab 原始資料）：

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q test/test_finlab* test/test_derive_finlab_minute.py test/test_stock_volume_units.py test/test_acquisition_policy.py test/test_public_dashboards.py
run_fintech_python scripts/verify_finlab_stage_dashboard.py
```
