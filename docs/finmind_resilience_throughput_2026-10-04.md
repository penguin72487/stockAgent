# FinMind 排程、故障恢復與有效吞吐修正

## 目標與驗收邊界

保留既有 Free / Sponsor / Complement、共享帳號與限流器、完整來源範圍、
交易日曆、追新優先及九個歷史階段順序。提升的是有效取得資料的吞吐，
不是單純增加請求數、刪除候選或把失敗改成完成。原始資料、最後有效收據
與來源更正流程均不得省略。此工作不修改交易、模型或遠端儲存。

## 2026-10-04 00:04 台北基線

上一滾動小時共 4,981 個請求起始紀錄（間隔樣本 4,980 個）：

- 台股分鐘 K 4,331 次；其中當小時最後完成的非空分區 3,422 個、286,888 筆。
  這是分區最後狀態，不是已證明的整段歷史淨新增筆數。
- UKStockPrice 空回應後的失敗重試 522 次，涉及 131 個保留有效舊資料的任務。
  不代表來源永久不可用，也不是可以抹掉原資料的理由。
- Sponsor 有 51 個受監測的成功檢查，回應均為零筆；事件型與夜盤資料不得
  因台股休市就直接排除。
- 92 個請求間隔超過兩秒、合計 577.32 秒；包含來源延遲與本機工作，尚未
  歸因成純 CPU 開銷。共享請求中位間隔 0.610 秒，限流器本身已有正常運作。
- 三個服務 active / NRestarts=0，只證明程序運行。Complement 約 20% 單核
  CPU、Python RSS 約 515 MiB；需另外驗收新收據。

官方 Sponsor 為 6,000 次／小時，整日全商品下載屬 Sponsor Pro；不提高
權限、跨日拼接單日端點或繞過限流來追求速度。
[方案](https://finmind.github.io/Pricing/)、
[分鐘 K 單次只提供一天](https://finmind.github.io/tutor/TaiwanMarket/Technical/#taiwanstockkbar-sponsor)。

## 實作計畫

1. 保存固定時間窗請求帳本、佇列聚合與 SQLite 備份，分開量測請求、
   非空完成、失敗、未變更和本機查詢成本。
2. 只有同一海外日線資料集、至少三個不同且有有效非空歷史的代號，在
   短時間內遭遇 `unexpected_empty_after_nonempty` 才合併重試；每分鐘輪替
   一個原失敗任務作為恢復探測。保留全部 failed 任務與最後有效收據，
   任何有效非空探測成功即放行其他修復，沒有一律七天冷卻。
3. 使查詢早期就限制到可執行的資料集與時鐘，維持原排序、衍生父資料、
   冷卻、追新和階段語意；共用同一份當次配額預留計畫，消除重複查詢。
4. 只對可恢復的 SQLite busy / locked 作有界延遲重試；權限、封鎖、
   資料毀損與未知錯誤不可被吞掉，也不可重新登入或繞過帳本。
5. 模擬與回歸驗證後，只重啟有修改的 FinMind 服務；用真實帳本和新收據
   驗收短窗吞吐，保留樣本／來源差異限制，不把局部 SQL 加速當全系統加速。

## 已實作與部署

2026-10-04 **00:25:05 台北時間**只重啟 Complement、Sponsor。Free、永豐、
當沖／隔夜模擬、公開唯讀 gateway 的 InvocationID 及 NRestarts 均未改變。
既有交易、模型、資料格式與冷庫工作沒有修改。

- Complement dispatch contract **5**；同源重試 contract **1**。
- ETA 與 public contract 維持 **7**。131 個失敗仍算待修資料，條件完工試算
  不會變成完工保證；每分鐘的配額／ETA 取樣繼續運作。
- 新增 `finmind_retry_cohorts.py`：三個不同代號在 15 分鐘內，經驗證的舊
  非空完整歷史收到空回應，才開啟同源探測。每分鐘輪替原失敗代號；非空
  完整探測成功立即放行其他修復，其他失敗仍逐一驗證，並非一起宣告成功。
- 這種 full-history failure 的重試必須重新查完整範圍，不能用「保留舊
  baseline 的空增量」冒充恢復。毀損 baseline 加上空來源回應也不能覆寫成
  零筆完成；保留舊證據、標記失敗再修復。
- 探測逾時仍尊重其冷卻；探測代號遇到 terminal error 不會卡死其他代號。
  若來源否認整個 dataset 權限，pending / failed 都停止無效呼叫，另留
  `finmind_entitlement_holds` 原狀態證據；不能因重試而持續觸發 4xx 封鎖。
- 主選擇器提早限制 pending / failed 的資料集，仍保留全部既有 queue
  dataset、追新、操作員有限回補、父資料條件及九階段排序。用主鍵前綴
  索引取得 dataset，不全掃佇列做 DISTINCT。accepted refresh 保留日期索引。
- Sponsor 當次配額判斷只建立一次 reservation plan。預留及 dispatch 使用
  同一個探測準入條件，只預留一個實際 probe，不預留 131 份相同故障。
- 抓完 120 次後，若已有 next task，立即進下一輪，仍由原全域 limiter
  控制下一次發送；沒有下一個可執行任務時保留等待，避免空轉。
- 修正 SQLite 生命週期：`with connection` 只提交／回滾，不關閉連線。
  現在兩個 worker 的正常、提早返回、例外及 idle 查詢都有確實 close。
  SQLite BUSY / LOCKED 用原 worker lock 等五秒後重試；其他錯誤照常失敗，
  不偽造成功 status，也不略過 request ledger。

## 實際驗收（00:36 台北）

以重啟前 15 分鐘、重啟後 10.9805 分鐘的帳本與佇列量測。以下每小時數字
是**短窗等效速率**，不是額外跑滿一小時的量測或長期效能保證。

| 指標 | 重啟前 | 重啟後 | 說明 |
| --- | ---: | ---: | --- |
| 台股分鐘 K 請求／小時 | 4,476 | 5,268 | 約 +17.7%，沒有擴大請求權限 |
| 非空分鐘 K 分區／小時 | 3,552 | 4,579 | 約 +28.9%，不等於每個交易日都有 271 根 |
| 英股失敗重試／小時 | 524 | 71 | 約 -86.4%；含啟動三次，穩態每分鐘一個探測 |
| 分鐘 K 原始列／分鐘 | 5,191 | 4,347 | 此短窗約 -16.3%，股票／日期及交易密度不同 |
| Complement queue descriptors | 27 | 2 | 包含當下正常主連線與短暫讀取；不再累積 idle 連線 |
| Sponsor queue descriptors | 18 | 0 | idle 已關閉；不需要等待 GC |
| Complement Python RSS | 479 MiB | 360 MiB | 只為當下觀察，不當成長期 RAM 上限 |
| Sponsor Python RSS | 387 MiB | 248 MiB | 同上；重啟／工作形態也會影響 RSS |

因此，修正提高了此窗的請求效率及非空分區取得速率，但不能聲稱所有來源
的原始筆數吞吐都提高。需依研究用途分開看有效分區、原始列數、bytes、
延遲、CPU 及來源本身每筆請求的資料密度。

具體資料與完整性證據：

- 部署後 **977** 個請求起始；964 台股分鐘 K、13 英股。
- **838 個新非空分鐘 K 分區、47,733 筆**：和固定備份的 task key 比較，
  且在部署後完成；不是由服務 active 或候選數減少推測。
- 新收據樣本通過 Parquet SHA-256 與 footer 筆數核對。
- **131 個英股仍失敗**；全部原收據 bytes hash、原 Parquet SHA-256、
  footer 筆數及 retained rows 驗證通過。來源仍回空，沒有宣告永久不可用。
- 官方額度 6,000／小時；00:36 provider 用量 5,052、本機滾動小時請求
  5,046，兩者取樣邊界不同，不需要強制相等。
- 實際未完成歷史順序保持：台股分鐘、期貨分鐘、台股分點、權證分點、
  台股 tick、期貨 tick、選擇權 tick、價差 tick、美股分鐘。新聞仍啟用。
- 最新公開唯讀 DTO、共享配額、12 階段估時與進度分母檢查通過；沒有
  改 UI 或重啟 gateway，資料來自既有 minute sampler 及 worker receipts。

## 固定快照與測試

同一佇列備份、時鐘、ownership、七次非變更選擇器取樣：

- selector CPU 中位：45.31 ms → 30.74 ms（約 -32.2%）。
- selector wall 中位：45.32 ms → 30.75 ms。
- 選出的 task 完全相同；沒有 production queue writes / provider calls。
- 初版把條件也推入 accepted refresh 分支後測得變慢，已撤回該分支；
  另修正 benchmark 將新 owner snapshot 放到舊 clock 造成 ownership 漂移
  的量測問題。中間結果不作為最終加速證據。

最終 **948 項** FinMind／日曆／配額／公開面板回歸通過，另有 focused
132 項及 calendar/quota/public 191 項通過，這些測試有重疊、不相加。
新增語意測試涵蓋 distinct failure、輪替探測、重啟、即刻放行、真正常空、
其他錯誤、權限暫緩、full retry、壞 baseline、busy code、零等待、實際
close（保留 connection 強引用，避免 GC 掩蓋）及原排程優先順序。
未執行與本次變更無關的完整模型／GPU 訓練測試。

## 可重做的唯讀驗收

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_finmind_throughput \
  --minutes 15 --output artifacts/data_quality/finmind_throughput_latest.json
run_fintech_python -m scripts.verify_finmind_history_sequence \
  --since 2026-10-03T16:25:05+00:00 \
  --output artifacts/data_quality/finmind_history_sequence_latest.json
```

本次證據：[資料夾](../artifacts/data_quality/finmind_resilience_throughput_2026-10-04/)。

- `predeploy_15m.json` / `postdeploy_final.json`：實際短窗請求、佇列、更新
  observation、131 原資料保留與新分鐘分區計數。
- `before.json` / `complement_baseline.sqlite3` / `query_final.json`：固定快照
  selector source、CPU／wall、ownership 與 task parity。
- `deployment_before.json` / `deployment_after.json`：只重啟兩個 FinMind
  worker、其他程序未動、public / ETA / quota 及資源觀測。
- `history_sequence.json`：新有效收據與階段／public progress 驗收。
- 最終測試 run：`final-finmind-regression-20261003T163426-0b43e6fe`。

歷史全量下載仍未完成；這次完成的是既有抓取服務與排程修復。來源空回應
持續由自動探測處理，不建立另一套下載器、不捏造缺資料、不掩蓋 failed。
