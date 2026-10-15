# FinMind 請求效率與美股分鐘歷史下載修復

範圍：FinMind Free／Sponsor／Complement 的查詢清冊、Complement 共享下載器、
每分鐘 ETA 與既有唯讀 `/finmind/`。任務 `finmind-request-efficiency-20261007`。
不新增平行 queue owner，不改原始訓練值，不搬動 WSL 原始資料，不操作 TEJ 或交易。

## 本輪驗收結果（10/7 13:09 台北）

兩個完整 owner 週期共接受 **2,400 次請求／463.91 秒**，實際全帳號實發吞吐
**18,624 次／小時，官方上限的 93.1%**。第一輪 236.05 秒包含 11 次 FixedIncome、
139 次 AssetSwap Option 追新及 1,050 次美股分鐘；第二輪 227.86 秒全為 1,200 次美股分鐘。
這不是宣稱已達每小時 20,000 次端到端完成速度；剩餘約 6.9% 包含初始化、日曆／清冊、
狀態、在途排空、網路與持久發布成本，實際其它工作／主機負載仍可能令速度下降。

美股部分 2,250 次接受查詢保存 **183,719 筆**；1,346 次非空、904 次空回應。
空查仍有來源證據，不以 0 補 K 棒或聲稱完整。ETA 的本配置美股容量為 18,672 次／時，
按該資料集分攤共同成本；它與上面的全帳號完整週期數字定義不同。
64 份最新實際收據驗證日期／ID、單次請求、JSON 契約、Parquet bytes／SHA／筆數，
零新增 US 失敗、零 fatal runtime log、單 queue writer、程序共 55 threads。
修復前約 996 threads；這不是宣稱其它未重啟 worker 的既有 threads 已全部回收。

先前 120-call 中間版本六個完整週期約 14,388 次／時；本輪約高 29%。兩段請求日期、
來源大小與主機負載不同，這是實際不同時窗觀測，不是受控全系統 ABBA 加速比。
保留必要驗證／追新與額度上限；採用既有工作偏好的共享 owner 與完整週期證據，
未為測速省略資料處理或新開另一份額度。

FinMind／共享流量／公開 dashboard 最新完整回歸 **1,163 passed／78.66 秒**；
本機／公網 1440、1024、390 已通過，公網另在真實容量出現後重驗。DTO 區分 JSON 抓取／驗證／發布
與大型物件下載／衍生，保留本配置完整樣本識別與週期數，不洩漏任意 provider 字段。
接受證據：[efficiency_acceptance.json](../artifacts/data_quality/finmind_request_efficiency_2026-10-07/after/efficiency_acceptance.json)。
最終原生測試 `request-efficiency-tests-20261007T051159-7f8ecf88`、來源驗收
`request-efficiency-acceptance-20261007T050956-1873146b`、公網重驗
`request-efficiency-browser-final-20261007T051241-db0118ad` 均 succeeded。
另一次與全測試／瀏覽器重驗重疊的 240-test 子集曾出現 2 個非 FinMind 的 lazy render
timeout（FinLab 與 provider 清冊）；同一最新版完整重跑包含這兩項且全數通過，保留這個
暫態觀測，未為取得綠燈刪除測試或修改未涉及的網站。

## 核心判斷

「有效資料量最大」與「呼叫最快」是兩件事：先選官方允許且覆蓋完整的最大查詢單位，
再讓抓取、驗證與持久發布重疊。吞吐仍受同一帳號配額、HTTP 延遲、本機發布、來源
合法查詢單位及必要追新共同限制，增加 thread 不會增加帳號權限。

- 帳號目前官方驗證為 SponsorPro，20,000 次／小時，全部資料 API 共用 0.18 秒 pacing。
  來源：[官方方案](https://finmind.github.io/Pricing/) 與本機每分鐘 `user_info` 收據。
- 美股分鐘 `USStockPriceMinute` 官方查詢是 **單股票、單日期**，2021-04-28 起；
  每日台北 08:00 發布前一美國日期的資料。來源：
  [官方美股技術文件](https://finmind.github.io/tutor/UnitedStatesMarket/Technical/)。
- 5 次有共享配額、可重用快取的探測：AAPL 2024-06-03／04 分別 391 筆；加 `end_date`、
  不給 `data_id` 查全市場，以及 `datalist` 查分鐘資料，都收到 `provider_bad_request`。
  已停止重試這些不合法形狀。391 是兩次樣本，不是所有股票、半日市或所有日期的應有筆數。
- 美股分鐘不在官方 `storage_objects` 支援的 7 張表內，不假造全市場 bulk grant。
  在目前介面下，仍未知的合法「股票 × 日期」候選至少需要各一次查詢；未知上市／下市
  歷史不能直接用今天的 `IPOYear` 或日 K 空值刪除。空回應保留 `observed_empty`，不是完整。

## 全清冊與每 call 可取範圍

[查詢清冊 CSV](../artifacts/data_quality/finmind_request_efficiency_2026-10-07/final/finmind_query_ranges_20261007T045924503483Z.csv)
與同名 JSON 逐項列出 owner、實際 runtime 契約、配置起日、已見最早／最新日期、
筆數、待抓／失敗／空回應、發布規律、下次檢查與可證明的剩餘呼叫下界。
清冊建立零 provider 呼叫、零 queue 寫入、零完整 Parquet 掃描。

| 查詢類型 | 現有策略 | 邊界 |
| --- | --- | --- |
| 7 張全市場物件 | 台股／期貨分鐘、台股／期貨／選擇權 tick、台股／權證分點，每個日期一個 grant | signed object GET 是另一個傳輸，不是第二份 data API 額度；原始檔完整驗證與必要衍生仍要計時 |
| 11 個已驗證長範圍契約 | 合併所有相連待抓期間，再分回原 canonical 分區 | response bytes／rows 守門保留；「官方未寫上限」不是無限或已證明絕對最長 |
| 其他單 ID 歷史 | 保留既有完整範圍／增量 overlap／參數修復，健康 Sponsor 接管重複台股資料 | registry aliases 不重複計算為另一份資料 |
| 美國分鐘 | 單 ID、單日期，保留官方美股現金市場休市證據排除 | 不用台灣現金交易日曆刪美股、期貨夜盤、公司事件或新聞日期 |

106 是**本專案 registry 聯集的去重資料集數**，不是宣稱官方產品只有 106 張。
132 個 owner aliases 不相加為資料集或剩餘呼叫；11 個 range 與 7 個物件是 runtime
契約清點，不代表本輪新增 18 支重複下載器。歷史 universe 完整性仍未驗證。

## 根因與修正

1. 每 120 次 `run_once` 建一個永不退出的限速 dispatcher。修復前 Python 約 996 threads。
   `finmind_account.rate_limiter` 現在每程序只保留一個 instance，其他程序仍共用同名
   host-global pacing／cooldown；方案變更更新 interval，不另開額度。
2. 120-request owner 週期初始化、清冊／日曆同步、狀態與 pool 收尾約另花 8–9 秒。
   六個完整中間測試週期約 29–31 秒／120 次，只有約 14,300 次／時，不能拿約 21.6 秒
   的 HTTP 小批次冒充全流程。週期 quantum 改為 1,200，仍做原有初始化與驗證。
3. 128 個 lazy frontier 工作耗盡後沒有續補，派工回到數百筆已接受舊歷史複查。
   現在同一 owner 呼叫同一 bounded frontier refill；不用重讀整份 master、重新做遷移
   或再查 API。仍保留每來源 128 history／128 forward 工作集、既有 cursor 與日曆判定。
4. 每 call 排序／掃描大量已接受任務。改為先 indexed seek 待抓、失敗到期與 priority 0
   追新；只有沒有必要工作與未建 frontier 時才查舊資料複查，不改相同條件下的選擇。
   同一只讀 queue 快照、實際 24 個 Sponsor aliases、ABBA 每輪 40 次：1.407／1.491 秒
   對 0.0850／0.0856 秒，全部選擇一致，約 17 倍。這只是本機 selector，不是全下載速度。
5. 連續抓取仍每 120 次排空在途工作，流量帳本顯示約 1–2 秒空窗。小 JSON 改為最多
   1,200 次連續派工，**在途上限仍是 8**，逐次重新檢查共享預算、追新、日曆、開盤與磁碟。
6. 原始歷史也可能持續發布新值。近期非空 supplemental 全歷史（沿用既有海外日資料的
   14 日 recent 分類）提升 priority 0，依原 next-attempt 時鐘追新，避免連續回補造成飢餓。
   老序列仍是維護候選，不被宣稱已下市；空、失敗、blocked／exhausted 不因改優先級而重開。

## 同一管線的有界重疊

`configs/finmind_json_pipeline.json`：8 HTTP workers、每批／owner 最多 1,200 次；
每回應上限 64 MiB、最多 8 個 admitted responses，payload bound 512 MiB。
這不是保證 Python RSS 永不超過 512 MiB。主 CLI Arrow CPU／I/O pools 各 1；
既有大型物件仍用另一份 object pipeline 的 8 HTTP／8 CPU × 1 Arrow，不改精確彙總。

各 HTTP thread 自己保留 keep-alive Session；只有原 owner 領取、保存、驗證、更新
SQLite、SHA 與收據。沿用持久 claim journal，重啟恢復中斷 claim，不重開成功或英股
exhausted。來源失敗隔離到 key；402／429／無權限停止 admission，取消尚未拿到 slot 的
等候，不假造消耗 API、失敗或已成交。錯誤訊息用既有 allowlist，token 不寫入公開輸出。

不適合獨立重疊的 snapshot、衍生、日資料 overlap／合批與全市場物件仍走 canonical
處理器。不是複製另一條管線，也不是將單股票請求偽裝為 server-side bulk。

## ETA／驗收方法

- dispatch 契約 11、JSON 契約 5、ETA 快照契約 14；配置 fingerprint 包含整個 owner
  quantum。120-call operator run、舊配置／舊選擇器樣本不能校準現配置。
- 只有全部接受、未中斷的批次才有資格；完整 owner 週期完成後，將初始化、狀態、
  pool shutdown 的共同成本計入樣本。實際其它工作時間另外記錄，ETA 另算追新／前階段。
- 中間容量使用 `sum(accepted_calls) * 3600 / sum(attributable_cycle_seconds)`。
  實際 API 使用率另從**不扣其它工作成本的完整 owner 牆鐘**及全帳號流量帳本計算。
  不混淆兩個指標，不拿最快一筆或配額充當完成速度。
- 至少兩批／兩個完整 owner 週期、matching fingerprint 才作本輪生產驗收。
  每分鐘估時讀這些實測；樣本不足顯示 warming，不保留舊較快假數字。
- UK exhausted 基線是 133（先前 131 是舊快照）；此輪不重開它們。
- WSL 25 GiB 空間守門、九階段順序（美股分鐘最後）、追新優先與有限人工修復保留。

最終實測另見 `after/efficiency_acceptance.json`（驗收腳本成功才會寫出 accepted 收據）。
此文不把 active、HTTP 200、瀏覽器通過或搜尋候選數當成所有歷史完成。

## 剩餘邊界

目前約 2,526 萬個美股未建合法日期候選；原始曆日候選約 3,675 萬，差額主要是已有
官方美股現金休市證據。不是本輪新證明這些全部缺 K 棒。若每候選需要 1 call，
在所有 20,000／時都給它、全天不停且無新追新時，光 2,526 萬次就至少約 **52.6 天**。
實際還要扣必要追新、失敗／停機、處理與儲存時間；不能改 concurrency 就承諾幾小時。
歷史上市／下市生命週期證據仍可進一步縮小候選，但目前不能安全以未知年份剪掉資料。

AssetSwap 官方另有不給 ID 的**單日期全市場**形狀，可作後續增量合批候選；尚未完成
來源對照與同一 history head 的精確合併／去重契約，沒有假設它支援多日全市場，亦未
以未驗證替代查詢直接覆蓋既有 raw histories。不是宣称 106 個端點都已達數學絕對最低。

全服務 invocation 不變檢查在 12:48 偵測當沖服務被另一個 native task
`tw-daytrade-ledger-recovery-20261007` 重啟，保留 `after/protected_service_delta.json`。
本任務部署只重啟 Complement 與原唯讀閘道；不偽造原基線，不宣稱所有服務都沒變。

## 重現

證據根目錄：`artifacts/data_quality/finmind_request_efficiency_2026-10-07/`。
包含 before、官方形狀探測、selector ABBA、完整週期、流量、來源 SHA／收據與瀏覽器證據。

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_finmind_query_ranges \
  --output-dir artifacts/data_quality/finmind_request_efficiency_2026-10-07/recheck
run_fintech_python -m scripts.verify_finmind_request_efficiency \
  --baseline artifacts/data_quality/finmind_request_efficiency_2026-10-07/before/deployment_before.json \
  --output artifacts/data_quality/finmind_request_efficiency_2026-10-07/recheck/acceptance.json
```

以上不呼叫 provider／寫 queue；第二項會驗證部分最新真實分區的 bytes、SHA、筆數、
日期／ID、單 queue writer、dispatcher 不增長與 source errors。完整專案 test suite
與全部股票逐分鐘覆蓋稽核不在此驗收範圍。
