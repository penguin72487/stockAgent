# FinMind 週期成本與發布排程修正（2026-10-04）

本輪範圍：重新核對 Free、Sponsor、Complement 的來源→配額→建單→派工→落盤→
公開估時；不重寫下載架構、不縮減商品／歷史範圍、不跳過資料驗證。
前輪同源重試、連線生命週期與 BUSY 恢復修正見
[前輪報告](finmind_resilience_throughput_2026-10-04.md)。本輪另取新的運行基線，
不把前輪的短窗效益當成現在的數字。

## 基線與根因

觀測截止 2026-10-04 **01:11:15 台北**，前 20 分鐘的共用請求帳本：

| 範圍 | 實測 |
| --- | ---: |
| 所有請求開始 | 1,722 次 |
| 台股分鐘 K | 1,699 次，等比約 5,097 次／小時 |
| 最新任務狀態為非空的分鐘分區 | 1,430 個、93,222 列 |
| 分鐘空回應 | 269 次 |
| UK 來源錯誤探測 | 20 次，原 131 個失敗歷史仍保留 |
| 新聞／交易日曆 | 2／1 次 |

請求開始不是成功回應；latest-attempt 統計不是累加成功率；上述列數也不是
全庫淨增加列數。空回應、已確認休市、來源故障和未建歷史候選保持分開。
三個 worker 此時均 active、零自動 restart，但這本身不證明資料完整。

確認三個可修的本機／排程問題：

1. **最後結算價週末續抓。** 目前有 387 個期貨、59 個選擇權非空商品歷史，
   一律以成功後 3 小時續抓，包含週末；本輪部署前的當小時共預留
   **330 次**，其中 **329 次**是週末結算查詢、1 次是必要的 UK 來源探測。
   FinMind 官方明列結算價更新為
   [週一至五每 3 小時](https://finmind.github.io/tutor/TaiwanMarket/Derivative/)。
2. **已完成優先清單拖慢每次派工。** `finmind_priority_tasks` 的 132,439 個
   期貨分鐘項目均已 complete／observed_empty，查詢仍把所有來源待辦拿來 join。
   應先以既有主鍵找清單的 dataset 前綴，再索引查其 outstanding set。
3. **算完立刻丟掉的全清冊。** `supplemental.seed()` 已算一次完整 frontier，
   worker 未使用返回值，緊接 `_status()` 又算一次。剖析中一次 frontier 約
   0.4 秒；移除這次未使用的聚合，不改網站的完整性或發布頻率。

## 實作與保留的語意

- `finmind_scheduling` 發布契約 **4**：共用結算價的官方 weekdays 與 3 小時週期。
  官方沒有公告固定時段相位，因此不捏造 00/03/06 為真正發布時間；平日保留
  原成功後 3 小時檢查，跨週末則最早在下一發布日 00:00 檢查。
- Complement dispatch 契約 **6**：以 recursive 主鍵 prefix seek 篩出 operator
  override 涉及的來源，保留多來源、精確 key、追新最高優先、冷卻、同源探測、
  九類歷史順序和正常待辦回退。沒有新增全量排序快取或第二個 scheduler。
- 既有成功結算 deadline 由 worker 在自己的 `worker.lock` 下遷移，原 deadline
  另存 `finmind_periodic_clock_migrations`。不改值、收據、last-attempt、筆數或 bytes；
  不抹除 failed，也不延後初次回補、已漏掉的平日工作及公告更正任務。
- **發布日曆不是交易日曆。** 這項週末限制只作用於兩個最後結算價資料集的
  成功續抓；期貨／選擇權夜盤、tick、分鐘資料的搜尋與回補不套用現貨休市。
  原已驗證 TWSE 現貨日曆與未知商品存續區間的處理不變。
- `seed(include_status=False)` 僅省掉呼叫端未使用的統計。預設仍回傳完整清冊，
  worker 的 `_status` 保持原發布頻率、分母、缺口與最新資料資訊。
- ETA snapshot 契約 **8**：平均週期負載與 release-event 模擬均遵守同一發布
  weekdays；跨週末和後續階段也保留 midnight reset phase，不以舊週五時點
  extrapolate 到週一，避免累積階段的時間再次偏移。
- 公開 projection 使用共用當前版本常數，保留 v6/v7 的相容路徑、12 個階段、
  9 個歷史進度及「下次重試成功」條件試算。回歸時曾發現新 v8 沒有被舊硬編碼
  v7 reader 接住、以及發布 metadata tuple 的 JSON 往返差異；均在正式部署前
  修復，並非把它們當成本輪基線效率下降的既有原因。
- 官方 account／共用 limiter／每分鐘 quota 與 ETA 取樣持續沿用。
  [官方 Sponsor 配額](https://finmind.github.io/Pricing/)仍是共用每小時 6,000 次，
  不是每個 worker 各 6,000；沒有繞過配額或變成固定 10 req/s。

按目前 446 個非空商品歷史、原 3 小時週期不變的模型，每週少約
`446 × 48 / 3 = 7,136` 次週末成功續抓。這是**規劃節省量**，不是已量得的
過去 HTTP 差值；新資料、更正及失敗重試仍需另外計量。

## 效能證據

同一 SQLite backup、同一時計與 delegation，無 provider 呼叫／正式 queue 寫入：

| 比較 | 原路徑 CPU 中位 | 新路徑 CPU 中位 | 差異 |
| --- | ---: | ---: | ---: |
| 完整單次 selector（7 次） | 35.27 ms | 18.94 ms | -46.3% |
| 本機批次：建單、120 次 selector、5 次完整 status（3 次交錯順序） | 11.50 s | 9.86 s | -14.2% |

本機批次的所有 task／frontier rows、公開 status 和 120 次選擇結果 SHA 相同；
兩邊使用相同的修正版發布規則，隔離 CPU 改善與政策變更。
wall 中位 11.43→9.79 秒，但樣本區間有重疊且主機有其他背景工作，不能宣稱
所有狀態都固定快這個比例。本機測試未包含 HTTP／原始資料落盤成本，不能當作
provider 吞吐提升百分比。診斷工具的 queue parity hash 已改為逐列串流，避免
為比對另建立巨大的 Python row list／JSON。

## 驗證與部署

- 部署前完整 FinMind／配額／日曆／共享 gateway 回歸：**973 passed**。
  測試包含週末／週一邊界、初次歷史、失敗不延後、已漏平日任務、夜盤不誤排除、
  多來源 override、SHA／frontier parity、JSON 清冊、舊估時順序拒絕與重試條件。
- 2026-10-04 **01:24:36 台北**開始部署：僅重啟 Complement 和唯讀 gateway，
  使下載規則與公開 projection 載入同版。Free、Sponsor、永豐與兩個交易服務
  的 invocation／restart 在 after 驗收中均保持原值。新版 dispatch 6、ETA 8、
  公開 projection 8 已一致載入，所有受保護服務未重啟。
- 01:30:06 的唯讀遷移驗收：**446／446** 筆成功結算歷史的原值、日期範圍、
  筆數、bytes、last-attempt、receipt path／SHA 均未改動，原／新 deadline
  的遷移收據逐筆吻合。下一檢查為 **2026-10-05 00:00 台北**；當小時的
  結算預留 **329→0**，仍保留必要探測 1 次。配額 admission 的另外 2 次
  控制呼叫餘裕保持不變，因此網站 allocation reserve 為 3，不是 0。
- 正式 worker 的新分鐘分區已驗證 receipt／Parquet SHA、footer 筆數與身份，
  並與公開九類進度、12 階段估時對帳。
- 公開網站 1440／1024／390 px 三種寬度驗收均通過：無 JS 錯誤／橫向溢出，
  完整清冊篩選可恢復、9 個歷史進度條與 12 個階段順序一致，瀏覽器不呼叫
  provider API。來源未恢復的估時顯示「等待重試／若下次重試成功」；不拿
  重試倒數宣稱全庫即將抓完，候選搜尋進度也不冒充實際資料完整率。
- 配額仍每分鐘取樣：例如 01:28／29／30／31 的本機 rolling 60m 分別為
  5,299／5,288／5,302／5,282；官方 account 另保留自己的採樣時點與數字。
  account、request-start ledger 與 ETA 不被硬塞成同一個值。

部署後第一個短窗（01:24:36–01:30:07，**5.519 分鐘**，包含重啟與首次建單）：

| 範圍 | 實測 |
| --- | ---: |
| 共用請求開始 | 474 次：台股分鐘 469、UK 探測 5 |
| 分鐘請求速率 | 約 5,099 次／小時；基線約 5,097 |
| 與固定 before queue 對照的新非空分區 | 334 個、38,916 列 |
| 新分鐘空回應 | 134 個 |
| 原 131 個失敗但有舊資料的歷史 | 131 仍失敗；全部舊收據、Parquet SHA／footer 通過 |

短窗與基線的資料日／商品密度不同，而且請求速率幾乎相同；**本輪沒有證據
宣稱 provider 吞吐提高 14% 或 46%**。已驗證的是本機 CPU 浪費下降、週末
錯誤預留釋放、實際新增資料持續落盤與跨層估時一致。後續正常服務持續累積
長窗流量與資料收據，不降低驗證或縮減全歷史範圍來換數字。

## 可重現入口與完整證據

證據目錄：[finmind_cycle_efficiency_2026-10-04](../artifacts/data_quality/finmind_cycle_efficiency_2026-10-04/)。
包含 `before.json`、原 queue backup、`selector_after.json`、`local_cycle_final.json`、
`local_cycle_stream_parity.json`、`periodic_before.json`／`periodic_after.json`、
`deployment/deployment_before.json`／`deployment/deployment_after.json`、
`history_sequence.json`、`browser/browser_acceptance.json`／screenshots 與
`postdeploy_final.json`。最後的逐列串流工具版本已另跑一次完整本機週期，
所有 task／frontier／status／selector 的 hash 再次相同，無 provider 呼叫或
正式 queue／receipt 寫入。

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_finmind_throughput \
  --root data_finmind --minutes 20 --output artifacts/data_quality/finmind_window_current.json
run_fintech_python -m scripts.audit_finmind_throughput \
  --root data_finmind --output artifacts/data_quality/finmind_local_cycle_current.json \
  --benchmark-local-cycle artifacts/data_quality/finmind_cycle_efficiency_2026-10-04/before.json
```

上述驗收當時未解的來源狀況：UKStockPrice 的 131 個原非空歷史仍遇來源
全歷史空回，保留舊原值與失敗證據並每分鐘輪替探測。後續依最新使用者
指示改成[有界重試](finmind_bounded_retry_2026-10-04.md)：達上限保留紀錄、
停止自動探測，不標為已抓齊，也不判定永久來源錯誤。九類分鐘／分點／tick
仍需按原序回補；美股分鐘依使用者要求最後。
