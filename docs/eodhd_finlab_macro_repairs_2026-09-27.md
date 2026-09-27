# EODHD 免費能力與 Census／BEA／FinLab 修復

觀測日期：2026-09-27，台北時間約 23:55。這是指定工作範圍的實測，不代表整個 provider 或歷史 PIT 均完整。沒有輸出金鑰、改變訓練契約、刪除原檔或發布新冷庫版本。

## EODHD 免費能力與實際帳戶

依官方 [免費方案清單](https://github.com/EodHistoricalData/eodhd-claude-skills/blob/main/skills/eodhd-api/references/subscriptions/free.md)：

- 支援美股及其他交易所股票／ETF／基金的最近一年日線、調整後價格、拆股／股利、下市資料與延遲報價；另有外匯、加密貨幣日線及延遲報價。
- 可查交易所、商品名單與搜尋。
- 不含一般完整基本面、EPS、總經、財經事件日曆、分鐘歷史、tick、WebSocket 即時資料與 screener。特定示範代碼能讀取付費端點，不代表全市場權限。
- 官方有列新聞，但依使用者既有設定不排新聞。

[官方限制文件](https://eodhd.com/financial-apis/api-limits)：每天 20 個**計費單位**，另有 1,000 HTTP requests/min 速率上限。EOD／拆股／股利每次各 1 單位，整個交易所 bulk 是 100，不能把一次 HTTP 當成只花 1 單位。UTC 00:00（台北 08:00）更新日額度；計數器可能等下一次資料請求才重置。

本機 `/api/user` 實測：方案 `free`、每日上限 20、帳戶計數 0、額外點數 500。原始計數日為 `1970-01-01`，保留 `counter_is_for_today=false`，不把舊計數假裝本日取樣。查此端點不計費；本輪沒有下載 EODHD 行情，也沒有使用額外點數。

- `.env`：`EODHD_API_KEY`；已補 `.env.example` 與 credential registry。
- `scripts/snapshot_eodhd_quota.py` 只保存方案及額度白名單，排除姓名、Email、token 及完整帳戶回應。
- 收據：`artifacts/credentials/eodhd_quota.json`。資料端點權限仍標 `data_endpoints_verified=false`，與帳戶額度驗證分開。

## Census／BEA：指定歷史已取得

| 來源 | 指定範圍 | 來源紀錄 | 可解析數值文字 | 時間範圍 |
|---|---:|---:|---:|---|
| Census EITS | 19/19 查詢：18 類指標，另含各州 QTAX | 2,247,215 | 2,227,339 | 最早 1956-Q1；各序列到 2026 年 7／8 月或 Q2 |
| BEA NIPA | 9/9 表 | 132,883 | 132,883 | 最早 1929；季資料到 2026Q2，月資料到 2026M07 |

Census 另有 19,876 筆原始文字碼：`S=13301`、`(S)=2940`、`Z=2182`、`D=1321`、`(z)=71`、`A=32`、`X=29`。原文保留，不當成數值 0。舊 `mhs` 止於 2014-10，與延伸至 2026-07 的 `mhs2` 分別保留，不把舊系列偽裝成有新觀測。API 紀錄筆數不等於獨立特徵數。

修正：

1. 兩組更新後金鑰都已被官方接受。新增 `--recheck-credentials`，僅略過認證失敗的本機冷卻，不略過 429、配額或其他退避。
2. `m3/advm3/qtax/bfs/mhs2` 的新版 API 要求 `for=us:*`；QTAX 另加 `for=state:*`。依 `geography.json` 實作，不猜地理層級。
3. QTAX 的 `variables.json` 使用大寫欄位，已修查詢與解析，仍保存原始欄名和單位維度。
4. BEA 一次取得一張表的全年份與原生頻率：9 次資料請求完成 9 表。Census 修正後只請求 6 個缺失範圍，13 份有效收據直接重用。
5. 既有每小時 timer 保持啟用；個別資料 TTL 到期後增量抓取，兩年修訂重疊、90 天全史重查。不是每小時重抓全史，也不宣稱每次官方發布都即時捕捉。

官方契約：[M3 範例](https://api.census.gov/data/timeseries/eits/m3/examples.html)、[QTAX 欄位](https://api.census.gov/data/timeseries/eits/qtax/variables.html)、[QTAX 範例](https://api.census.gov/data/timeseries/eits/qtax/examples.html)。期別不等於發布時間；這些是目前可取得的修訂版歷史。

## FinLab：大表改用有界 Arrow 路徑

兩張盤後定價文字表各約 10 萬欄。直接形成 pandas 寬表再轉置合併，先前曾接近 10 GiB 且沒完成。[官方 `start/end` 文件](https://finlab.finance/docs/reference/data/) 描述的是 Arrow 讀取切片，不能當成服務端分區下載。

`scripts/finlab_arrow_history.py` 已接入既有 `fetch_one` 與 timer，不另建下載排程：

- 使用 SDK 現有登入／授權簽址流程；簽址前檢查額度，並限制檔案大小與磁碟餘額。
- HTTP 串流落地、原子保存 SHA-256 原檔；解析失敗可重用已驗證原檔，避免重花配額。
- 文字寬表每次投影 512 欄，轉成 `date/symbol/source_column/value` 稀疏長表。保留原始欄名及名稱變更造成的多欄，不偷偷平均；未列的格子是 null，不是 0。原始 Arrow 保留全部欄名與全空欄。
- 券商長表逐 Arrow record batch 寫 Parquet，不全表轉 pandas、不改原值。實際券商大檔尚待額度足夠後驗證，不能用文字表測試代替驗收。
- 三個大表取消永久暫緩，重新納入排程；數值資料優先於來源標籤。額度／磁碟不足標 `resource_deferred`，不是權限失敗或已完成；新配額日解除該次退避。
- 儲存稽核也改為 65,536 列小批讀日期和索引，並驗證原檔雜湊。

| 資料 | 本次結果 | 非空資料格 | 實際日期 |
|---|---|---:|---|
| `after_market_fixed_price:資料來源` | 官方強制追新成功 | 16,470,471 | 2020-01-02～2026-09-24 |
| `after_market_fixed_price:市場別` | SDK 快取復原；官方追新等額度 | 16,470,471 | 2020-01-02～2026-09-24 |
| `broker_transactions` | 已接入新路徑，餘額不足，尚未完成大檔下載 | — | 不以網站介紹代替本機收據 |

實測：128 欄版本復原兩表耗時 329.9 秒，程序 RSS 峰值 542 MiB。512 欄版本的 `資料來源` 官方下載加轉存耗時 77.7 秒；含券商預檢與空值複查的整輪 83.5 秒，RSS 峰值約 591 MiB、cgroup 峰值 659.2 MiB、swap 0。這是指定文字表的實測，不是所有 FinLab 表的記憶體保證。

本輪後官方配額約 `4890.1/5000 MB`，餘 109.9 MB。券商大表過去一次約 726 MB，新預檢先保留約 1 GiB 預算與 50 MB 餘額。既有 timer 下次配額重置啟動為 **2026-09-28 08:00:05 台北時間**，仍遵守交易日開盤資源保護；不承諾 08:00 已下載完成。

## FinLab 原始全空欄位

`dividend_otc:權息`、`management_change_events:變更交易開始日` 重新向官方查詢後，與原始 Arrow 對照，**非空來源值都是 0**，不是本機轉換掉值。

- 原檔與 SHA-256 證據存於 `data_finlab/empty_evidence/`，由失敗收據引用；不計入可用訓練觀測。
- 新檢查區別「原檔也空」與「原檔有值、SDK 卻變空」；後者另標 `normalization_error`。
- 有值的 `權值`、`息值`、`權+息值`、`變更交易` 已有獨立收據，但不能無依據改名成缺欄的原始真值。
- 保留自動再查；沒有填 0、捏造日期或解除發布／訓練守門。供應商未提供的值無法靠重試保證補出。

## 驗證與重跑

詳細日期、列數、雜湊與數值品質：`artifacts/data_quality/provider_repair_2026-09-27/verification_20260927T155114Z.json`（另有同名 CSV）。28 個 Census／BEA 收據通過檔案 SHA-256、逐批列數與數值檢核；不是 PIT 或所有端點完整性的證明。

相關測試共 **243 passed**；Ruff F/E9 與 `git diff --check` 通過。測試包含認證重驗不略過配額、地理／大小寫契約、Arrow 批次與索引連續性、來源空值／轉換錯誤區分、截斷下載、資源預檢、金鑰白名單及面板狀態。

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.snapshot_eodhd_quota
run_fintech_python -m downloader.download_public_economic_history --execute --providers census bea --recheck-credentials
run_fintech_python -m scripts.audit_public_provider_repairs --validate-macro
```

面板只讀本機收據，不為顯示呼叫來源 API。`/data-monitor/api/status`、`/finlab/api/status` 均實測 HTTP 200；區分快取復原／官方追新、非空資料格／日期列數與資源暫緩。沒有宣稱瀏覽器視覺驗收或遠端訓練資料已就緒。
