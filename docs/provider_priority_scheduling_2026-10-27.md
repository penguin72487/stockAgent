# FinLab、FinMind、TEJ 排程與優先查詢

任務 `provider-priority-scheduling-20261027`。目標是減少重複工作，讓剩餘歷史、
已到期追新和臨時查詢共用原下載器。資料本身、來源時間、空值、單位與收據不改寫。

## 分工與順序

| 執行者 | 第一階段：追新 | 第二階段：有限查詢 | 第三階段：歷史及剩餘工作 |
| --- | --- | --- | --- |
| FinLab 原 account owner | 原發布／08:00 配額時鐘、一般資料追新 | 一般 catalog key 的有限 upstream check | 原一般資料缺口 → 分鐘 → 最低優先 tick；已有 tick 可合成分鐘，不重抓相同資料 |
| FinMind 原 Complement／Sponsor／Free owners | 正式到期發布最高，僅在對應時窗預留 | 精確既有任務；海外 ID 歷史續抓可讓路 | 原九階段、有限範圍、最大合法查詢、重試與多源校驗分工不變 |
| TEJ 試用 API | 原帳號配額內的獨立增量任務 | 精確既有 API task ID | 原按缺資料價值排序的試用表歷史 |
| TEJ Smart Wizard | 獨立增量計畫；一個桌面 owner | 精確既有 task ID，或原生有來源證據的有限查詢 | 原缺少特徵 → 缺年份／少筆數 → 校驗；API 只能在精確已驗證範圍內取代 Wizard |

各 provider 可同時工作，同帳號不另開不計流量的通道。追新只在到期後搶先；
在途請求正常完成後才切換。來源冷卻、開盤保護、失敗與未知結果的恢復規則照常。

2026-10-28 補充：原 FinLab 分鐘／逐筆 owner 支援有界的 `--priority-only`，
只處理尚未到期且已登錄的精確分區，每批最多 64 筆，序列派工並沿用原帳號鎖。
這是單輪執行批次；可登記的需求總量不限，後續輪次接續處理，不截斷清冊。
入口和每 60 秒的派工邊界核對有限插隊的實體及檔案系統保留線；這不是預留
任意大小的寫入空間。每筆派工前重新核對期限、當沖優先工作、原來源冷卻、
交易時段、配額、既有一般資料與分鐘優先順序。目錄過期時，只有存在待處理
精確分區才由同一 owner 更新原始 catalog 收據；目錄不完整則停止。
一般歷史游標與完整市場狀態不由此模式改寫，獨立批次收據亦不加入一般市場
吞吐校準。狀態記於 `data_finlab/intraday/priority_status_tw_minute.json` 或
`priority_status_tw_tick.json`；狀態完成只代表該次有限派工結束。
維護期間不恢復大量歷史任務，也未將此單次模式另設新的下載服務；正式到期
追新與既有分鐘條件未完成時，有限工作預設仍回傳 75 留待原 owner 續排。

若使用者明確要求查詢，且一般到期追新被**已量測**的普通實體／檔案系統
保留線擋住，可選擇 `--priority-only --kind tw_minute --limit 4
--capacity-deferred-priority`：仍只處理原登錄的精確分區，以同一帳號鎖序列
派送，每次最多四筆。每筆派工前重新檢查一般容量與有限查詢自己的容量、當沖
交接、開盤、來源冷卻及配額；一般容量恢復時立刻讓路。此模式逐筆重新核對
來源剩餘配額，保留至少 2,000 MB 和既有較高的未來排程保留值，即使在重置前
排空時也不放寬。這個額度是保守下限，不是未知大小一般表的完整費用保證；
容量未知、catalog 未核實或其他拒絕理由都不能啟用例外。普通到期工作保持
未完成，不改大量歷史維護暫停，也不啟動第二個 owner。
對已登錄的股票逐筆 `--kind tw_tick`，同樣僅在普通到期工作因可量測的
容量保留線受阻時允許有限例外，每批限一個分區；每次重新驗證原 owner、
日期／標的、來源期限、分鐘與一般工作狀態、當沖交接、開盤、配額及兩級
容量。一般派工恢復或容量未知時，原分鐘優先仍生效；逐筆不是空值證明。
2026-10-28 儲存維護期間，曾觀察 Windows 實體可用空間在短時間波動約
18 GiB，原因尚未確認；這個明確選用的例外在每筆查詢前，額外要求 24 GiB
可用空間，讓原控制及帳務保留線上方有緩衝。這是啟動檢查而非寫入預留，
不能保證其他程序不會同時消耗空間；觀察條件改變時應重新評估。這項例外
只接受容量政策已登錄且實際量測為 `admitted`；未登錄或未知狀態一律延後，
不改動既有普通下載在未登錄主機上的行為。
`scripts/run_finlab_finite_priority_drain.py --max-passes N` 可在一次明確授權的
有限研究查詢中，依序執行最多 32 個四筆上限的既有 owner 批次。各批次之間
釋放帳號鎖；指定 `--kind tw_tick` 時每批最多一筆。容量等待模式在有限次數
內重試共享帳號鎖忙，無鎖等待；遇到當沖交接、容量／配額拒絕、未知回覆、owner 或容量
檢查程式改變即停，記錄的 attempted 只計來源派工。這不是常駐服務，批次結束也不代表歷史
或商品範圍已補齊。
加上 `--wait-for-capacity` 時，預設最多等待 12 小時再次量測上述有限查詢
門檻；明確指定 `--capacity-wait-hours` 可在 1–168 小時內延長，但原查詢的
到期時間不會因此延長。等待時不持有帳號鎖，也不發來源請求。其他拒絕理由仍直接停止，等待到期
與批次耗盡皆不代表已取得歷史資料。

特定期貨分鐘研究若已有一個有限派工程序，但其等待期限早於所登記需求的
期限，可用 `scripts/run_finlab_finite_priority_handoff.py` 指定精確 request ID、
前一程序 PID 與 kernel start time。接手程序先等待原程序結束，之後仍呼叫
上述有限 drain；它不建立 SDK session、不延長查詢期限，也不改配額或容量
門檻。它只對已驗證的期貨分鐘分區運作，在期限內重試已知的容量等待逾時、
帳號忙和配額邊界，遇到來源／guard 變更或未知結果便停止保留證據。等待
狀態、來源 pin 和各 request 的觀察結果寫入指定的 operations 收據；這些狀態
不代表來源資料或歷史完整性已驗收。
正常 `priority_handoff` 表示原 owner 讓位給已到期的當沖需求；接手協調層在
釋鎖後等待五分鐘，再由原 owner 重驗准入，不能把這個已知讓位永久當成未知
失敗。等待仍受原 request 期限限制；未知結果與來源程式變更仍停止保留證據。

2026-11-09 擴充同一交接器的 `--family stock`：只接受已登錄、明確合法的
股票代碼與 ISO 交易日的 `tw_minute` 分區；預設仍是原期貨範圍。股票不能
選期貨專用 workspace 或 preferred-futures 選項。交接仍辨識原程序 PID 與
kernel start time、等其退出、固定程式摘要，並讓既有有限 drain 重檢帳號鎖、
開盤、容量、配額及一般追新。狀態、deadline 和來源改版均留在進度收據；
等待不代表已派工或取得資料，既有有限佇列排序也不變。

2026-10-28 的 Smart Wizard 排程更新：當同一 owner 持續有大量到期增量時，
`finite_query_after_incremental_attempts: 4` 在四次實際增量任務 claim 後，
讓一個已到期、符合隔離條件的有限查詢取得下一個位置；之後回到增量優先。
有限範圍既包括尚未過期的 `provider_priority_query_v1`，也包括原生登記的
-99 至 -1 優先序任務。未啟用下述重試輪替時，沒有增量便按既有順序處理；
設定省略時維持舊行為。
計數與原 claim 同一個 transaction 寫入，查詢候選或本機拆分不增加計數。
這是防止長時間飢餓的排程比例，不增加來源 owner、放寬來源配額、隔離或重試，
也不代表已取得所請求的資料。不同來源計畫仍須各自用原始收據驗證。

2026-10-30 補充：現場發現較早的有限要求含 18 個持續重試的 metadata
任務，四次到期增量後的有限機會都可能被這些任務使用，使較晚登記的新
要求長時間未獲首次嘗試。`finite_retry_after_fresh_attempts: 4` 的 v5
排程保留首次嘗試依要求建立時間排序，每四次實際已登記的新任務 claim
後，安排最久未嘗試的合格重試；沒有新任務時按原實際嘗試時間輪替重試。
兩路均維持原期限、到期、隔離、來源證據與增量保護。計數只在原 owner
真正 claim 的 SQLite transaction 改變，不以查詢候選或本地拆分計數。
未設定時維持 v4 的較早要求優先順序。此設定已寫入工作目錄設定，但
現有長駐 TEJ 程序尚未重載，沒有為插隊而強制重啟；載入版本和實際來源
收據須在後續獨立驗證。佇列快照與測試範圍見
[期貨補檔排程診斷](../artifacts/markets/tw_futures_full_history_v22_20261030_v1/tej-priority-fairness-diagnosis-20261030.md)。

FinMind 的選擇權商品日史有限查詢可登記完整商品集合，由同一 owner 每批
最多六項派工，避開原生冷卻及終止狀態。每筆仍驗證有限計畫、已登記商品、
來源雜湊、權限、重試及期限；普通服務的維護暫停不因此解除。若同一次
實體容量量測證實普通派工因原保留線不足而不能啟動，而有限派工符合
25 GiB 原底線加 24 GiB 緩衝及六個 64 MiB 回應額度，有限查詢可使用
剩餘配額；原到期及未來任務的完整 quota reserve、任務狀態與時鐘保持
不變。普通容量恢復、容量未知或樣本逾時、來源配額不足、排程核對失敗
時不使用此例外。容量檢查不是其他程序寫入的空間預留。這項機制只適用
明確登記的選擇權日史，不適用逐筆或一般回補。相關測試與實機狀態見
[研究紀錄](../artifacts/markets/tw_derivative_scopes_options_20261028_v1/README.md)。

2026-11-03 逐筆商品比較的等待器沿用共用 `AcquisitionCapacitySampler`，
容量不足時不再反覆啟動 Complement owner 或重讀整批原始 payload。
開始時完整核對來源；等待中只查固定程式／計畫、原 priority 狀態及容量。
容量恢復後、每次派工前和終止讀回時，再完整驗證原件，並在實際 child
啟動邊界重查程式與期限。已完成任務及到期讀回不等待容量恢復；未取得
准入時不持有 owner 鎖。原 owner 仍獨立重查來源、配額、容量與鎖。
同一批多商品共用的原件只雜湊一次；相同路徑但不同內容承諾直接拒絕，
並核對每次完整雜湊前後及整批讀回結束的 inode、大小與時間世代。
第 38 批的 ABBA 來源核對由平均 0.173 秒降為 0.039 秒，輸出一致；
這個局部測速不表示下載或訓練變快相同比例，見
[固定來源測量](../artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1/priority-source-hash-abba-20261103-v1.json)。
這只移除無效啟動與重複讀取，不變更容量底線或來源結果。

FinLab／FinMind 的前次歷史修復與實測見
[原排程報告](finlab_finmind_scheduling_2026-10-27.md)。FinMind JSON 派工內部契約升為
v18，避免把舊批次速度混入新排程 ETA。沒有降低原帳號配額或池容量。

## TEJ 增量的保存方式

原歷史 cutoff `2026-10-01`、`download_plans`、尚未完成的 ordinal 和原收據均保留。
新 `tej_separate_incremental_plans_v1` 在同一個 SQLite 保存獨立 cycles／plans，
只有原生查詢及其拆分子任務都完成，才記錄該 cycle `checked`。明確空回應也只是
查詢已結束，不是取得了資料。

`configs/tej_history.json:incremental` 控制每天台北 19:00 的檢查窗；這是本機檢查時間，
**不是 TEJ 精確發布時間**。休市日仍檢查，因財報／事件可能在休市日更新。
一般日／週／月／季／年／事件分別保留 3／8／35／120／370／35 天重疊範圍，
追補延後登錄與近期修訂。未完成 cycle 不再另建相同工作；新 cycle 從已核對末日接續。
重疊範圍之外的歷史修訂仍需來源公告或另一次有界重查，不能聲稱已保證覆蓋所有修訂。

API 的現況／參考表預設每七天檢查，避免每天重抓全表耗完試用額度；已知空表仍會
重新檢查，未捏造它的歷史起點。API 保留每天 500 calls、50,000 rows、每頁 10,000、
單查詢 50,000 的試用上限及原 reset 驗證規則。切日期／代碼不能用來繞過上限。
來源：[TEJ API 文件](https://api.tej.com.tw/document_python.html)；實際帳號權限與 trial
上限仍以本機原 account／usage ledger 為準。

## 其他程式如何插隊

先列原清冊，所有指令均只讀本機，沒有 API 呼叫：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/request_provider_data.py list --provider finlab --match 'price:'
run_fintech_python scripts/request_provider_data.py list --provider finmind --dataset TaiwanStockPrice --data-id 2330
run_fintech_python scripts/request_provider_data.py list --provider tej --table-id EXACT_TABLE_ID
run_fintech_python scripts/request_provider_data.py list --provider tej_api --table-id TRAIL/TAPRCD
```

JSON 範例（時間由呼叫端產生，不要照抄過期的常數）：

```python
from datetime import UTC, datetime, timedelta
from pathlib import Path
from downloader.provider_priority import register, inspect

now = datetime.now(UTC)
plan = {
    "contract": "provider_priority_query_v1",
    "provider": "finlab",
    "reason": "研究程式需要最新收盤價",
    "created_at_utc": now.isoformat(),
    "expires_at_utc": (now + timedelta(hours=2)).isoformat(),
    "fresh_after_utc": now.isoformat(),  # 可省略：優先用已驗證的本機資料
    "items": ["price:收盤價"],  # 用 list 得到的真實 catalog key
}
receipt = register(Path("data_finlab"), plan)
status = inspect(Path("data_finlab"), receipt["request_id"])
```

命令列同樣支援 `submit --plan /var/tmp/my-query.json` 與
`status --provider finlab --request-id SHA256`。
FinMind 的 items 格式是 `[{'dataset': '...', 'data_id': '...', 'partition': '...'}]`；
預設 root 是 `data_finmind/complement`。Sponsor 專屬資料使用
`--root data_finmind/sponsor`，或 Python 傳入該目錄；兩者都在原 selector 內支援插隊。
TEJ／TEJ API 則是原 task ID 字串。每份至少一個、**items 總量無固定上限**，
期限最多七天。重複 items 自動合併；排序不同的等價新 plan 共用 request_id，
原有未正規排序收據的 request_id 仍可使用並保持重送冪等。
FinLab 省略 `fresh_after_utc` 時先用完整性通過的本機資料；指定時要求該時間後的真實
來源檢查，SDK cache 命中不算。查狀態不需要再送出 plan。
只改 metadata，不重設原任務 state、原 priority 或 retry 時鐘。到期會停止插隊，原任務仍可
按正常排程完成。`provider_calls=0` 指註冊／查狀態本身；真正下載仍計入原 quota ledger。

2026-10-28 已移除原 64 項、4,096 活動項目、CLI 1 MiB 與 FinLab 清冊 4 MiB
造成的間接總量限制。清冊列出可用 `--limit 0` 取得全部符合項目，或指定任意正數；
預設 32 筆只為方便閱讀。註冊與狀態以 SQL 分批讀取原任務，批次大小遵守目前
SQLite 的參數限制，整份需求仍完整處理。實際可處理總量受主機記憶體、磁碟及來源
配額約束，沒有另外宣稱無限資源。

減少 API calls 的單位是**來源可共同回答的範圍**，不是 items 的個數：
本機命中不呼叫；多程式重疊需求共用原任務；來源允許的相鄰年份／月份沿用原
範圍合併器，一次取得後分區給各程式。FinLab 一般 key 原本就是整表範圍，不按
每檔股票另抓；FinMind 保留最大已確認區間、全市場／逐檔的原分工；TEJ 沿用已
驗證的欄位／公司／日期切塊、來源列數與分頁上限。不可跨不同來源或不同語義
硬合併；已有有效資料先共用，只有到期追新或來源更正才重新核對。來源冷卻及已量測的回應容量
限制保留，不因需求較大而逐 item 開新連線。

2026-10-28 的隔離驗證結果：

| 需求 | 驗證結果 |
| --- | --- |
| FinMind 4,175 個 items，其中 70 個重複 | 登錄 4,105 個原任務；重排順序仍共用 request_id，註冊不呼叫來源 |
| 同批登錄及狀態查核，SQLite 參數上限 999 | 26 次 SQL 批次查核；原逐 item 寫法需要 8,210 次 SELECT |
| FinLab 6,000 個一般 keys | 超過原 CLI 1 MiB、清冊 4 MiB 邊界仍完整登錄、列出與交付原 owner |
| FinLab 4,105 個分鐘分區 | 原 owner 可讀到最後一個需求，沒有在 4,096 筆截斷 |
| 兩份重疊的 FinMind 年資料需求，涵蓋 2001–2025 | 原 dispatcher 只派一次完整區間來源請求，落成 25 份共用 request_id 的 native receipts |

這些驗證使用隔離 SQLite／Parquet 與模擬來源端點，沒有修改正式任務或消耗供應商配額；
它們證明合併派工及收據語義，不是對線上來源吞吐量或全歷史完整性的宣告。
實際查核數、測試結果及程式身分保存在
[`provider_query_unbounded_2026-10-28`](../artifacts/data_quality/provider_query_unbounded_2026-10-28/)。

`complete`／`observed_empty` 會原樣回傳。先用已存資料，確認 native receipt 與所需範圍後
再用於研究；沒有內文／數值仍保持空值。來源 blocked 或冷卻不會被插隊要求覆蓋。
FinLab 的股票分鐘／逐筆仍走原全市場 partition scheduler；有限的期貨分鐘查詢
也由原分鐘 owner 處理。精確股票日期也可以交給對應的原分鐘／逐筆 owner：
派送前仍核對該 owner 的已觀察交易日與現有股票生命週期，查無此範圍不發來源請求。
一般追新、來源冷卻、配額、容量和開盤保護依舊適用；逐筆還要先確認已知分鐘
工作不再到期。若一般工作被來源或資源阻擋、已確認沒有可執行的一般工作，
有限逐筆查詢可在排定預留額度之外執行；一般逐筆回補仍依原 supplemental gate。
此介面的 `items` 必須全部是精確的商品月份或股票代碼及交易日期，例如：

```python
items = [{'dataset': 'tw_futures_minute:TX_202610', 'partition': '2026-10-02'}]
items = [{'dataset': 'tw_tick:5203', 'partition': '2011-10-03'}]
```

商品與日期須先由外部原始證據核對；建立意圖不代表來源一定有資料。
若既有股票 owner 在已選日期範圍內核對後發現股票不在名錄、交易日未獲確認或
股票生命週期不符合，會寫獨立的 `priority_scope` 本機檢查紀錄，並在同一配額
週期且日曆／名錄／執行程式的來源身分不變時，避免每分鐘空轉。來源身分未知、
改變或進入下一配額週期時立即恢復原本的檢查；自訂較短範圍外的日期不產生此
抑制。這不是供應商呼叫、無成交證據或下載完成，狀態顯示為 `waiting_local_scope`。
這些盤中分區查詢不支援 `fresh_after_utc`，已有合法原生收據時先使用原件，不與一般
catalog 字串混在同一份 plan。當執行 owner 的 FinLab SDK 尚未支援期貨時，
若 `data_finlab/tooling/finlab_futures_sdk_2_2_18_v1/` 的官方 wheel 和隔離安裝
完整通過 SHA 驗證，原 owner 可把自己的既有 session 經私人標準輸入交給一次性
子程序，逐筆順序呼叫；不新增登入、帳號 owner 或繞過原配額。缺少／損壞 wheel
時保留等待狀態，不把 SDK 不支援寫成來源空資料。子程序結果不明也只記錄
`source_outcome_unknown`，不能推定無成交。

期貨原件保留供應商的契約月份、交易日、一般／盤後、未調整價格與原生口數；
分鐘資料的來源窗口、全日成交量、價格、當時發布時間、金融帳務與可執行性仍須
獨立核對。供應商目前記載先提供近期 30 個交易日並逐日累積，不能由此推定更早
資料存在或不存在，應以實際來源回應及原生收據為準。
有健康專屬 owner 的 FinMind delegated 資料仍由原 owner 處理，不能用一般插隊繞過。
新增 TEJ 公司／日期／欄位範圍應走原 native `prioritize` source-bound plan。

可重用 skill：[stockagent-priority-data-query](../.agents/skills/stockagent-priority-data-query/SKILL.md)。

## 驗收與限制

本輪工程證據存於 `artifacts/data_quality/provider_priority_scheduling_2026-10-27/`；
以原服務身分、自然增量／優先查詢收據和讀回結果判定部署，不能只用程序 active。
來源配額仍會限制歷史完成時間；工程測試通過不代表三家的全部歷史已下載完成。

2026-10-27 台北 21:10 的驗收：

- FinLab／FinMind／TEJ 相關回歸 4,230 項通過；後續容量、等待邊界與優先查詢的
  245 項驗證通過。原始失敗紀錄保留，修復後另有通過收據。
- FinMind Complement／Sponsor 已從零在途、零 claim 的凍結等待邊界換代；Free
  從其同步容量等待邊界換代。原當沖與永豐期貨行情服務身分未變。
- TEJ 試用 API 的新追新查詢取得 29 頁、34,044 筆，23 頁明確空回應另計。
  Smart Wizard 截至 21:06 的 28 份已提交增量下載收據合計 38,614 筆，其中 16 份
  非空；原始檔 SHA、Parquet SHA 及 Parquet 筆數全部讀回相符。
- FinLab 收盤價與 FinMind GoldPrice 的本機查詢回傳原收據，沒有新增來源呼叫。
  真正來源派工仍需等待原 owner、到期追新、配額與容量條件。

當前限制是實體容量與來源配額：Windows backing disk 約剩 **16.8 GiB**，低於
既有 **25 GiB** 保留線。`physical_backing_reserve` 現在保留實測 bytes，與
`physical_backing_headroom_unconfirmed`（無法驗證）分開。WSL 內部顯示的約
274 GiB 可用空間不能代替 Windows 實體餘量。FinLab 還有 3,116.946 MB 的
當日來源額度，但新下載仍須通過容量檢查；原 timer／loop 會持續重新檢查，
容量恢復便續排，不把本機停等寫成來源失敗。

FinLab 21:10 的一般資料待更新數為 125，估計 pending payload 202,922,324 bytes；
這是目前可量測的待辦，不是所有分鐘／tick／未知歷史的總量。TEJ API 原有的
HTTP 400 blocked 任務仍需來源錯誤的完整證據分類，沒有因改排程直接重開。
上述數字是有時間邊界的快照，最新狀態仍以原服務及收據為準。
