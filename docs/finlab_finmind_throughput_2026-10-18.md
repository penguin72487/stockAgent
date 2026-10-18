# FinLab／FinMind 請求排程與吞吐修正（2026-10-18）

## 範圍與判準

沿用原有 account owner、queue、collector、收據與網頁投影，不另開下載管線。
其他視窗負責的 factorized 產生器、helper、測試與 benchmark 均不修改。
本次沒有搬移或刪除資料、沒有修改交易服務，也沒有恢復舊的全系統優化目標。

有效吞吐上限為 `min(官方剩餘配額速率、來源回應速率、驗證／發布速率、儲存速率)`；
另外必須扣除沒有可下載資料、尚未發布、優先追新及容量保護的時間。
併發數不是額外配額，HTTP bytes 不是供應商計費流量，request starts 也不是成功取得資料。
「只有 API 限制」必須用完整實際週期驗證，不能從單一 SQL 加速或 worker 數字推出。

## 調查與修正

### FinMind

- Sponsor Pro 官方上限為 20,000 requests/hour，以實際 account observation 為準；
  Free／Sponsor／Complement 共用帳戶 pacing、流量帳本與追新保留。
  目前相當於 0.18 秒一個 slot，沒有沿用任意的 10 req/s。
- 原 heartbeat 每次對數百萬 task 重做完整 aggregate。
  改為同一 SQLite connection 的暫存 trigger 記錄變更主鍵，再讀取已提交的實際列做精確 delta。
  不維護另一個持久 summary，也沒有 TTL。rollback、savepoint、IGNORE、REPLACE、
  extrema 移除、外部寫入、schema 變更、未知數值型別與過大變更集會退回 canonical SQL。
- 追新預留查詢新增小型 covering partial index，保留原交易日曆、發布時段與重試判式。
  只有 index DDL 完全符合合約才使用 hint，舊 schema 繼續走原查詢。
- 美股「在正式發布前取得，需要重查」的原查詢會掃描全部已完成歷史。
  新增寬於正式門檻的 partial index；正式 `julianday` 時間判式原封保留，
  包括浮點邊界和 NULL 行為，不能藉優化延後或提前資料可用時間。
- 其他 lane 的追新暫時優先時，loop 可以保留已準備好的 owner connection／pool／memo，
  讓出配額並等待，不再每分鐘重跑全部初始化。等待期間不持有 write transaction。
  有限次 CLI、不確定排程、硬配額不足仍使用原退出邏輯。
- 週期 snapshot／year 追新查詢改走符合原判式的 covering partial index；
  更正清單先用必要 SQL 範圍縮小候選，仍由原 `_bounds`／`_matches` 判定。
  snapshot 不誤套個股 ID 篩選，whole-market 空 ID、NULL、derived history 與大 ID 清單保留。
- 公平輪替 cursor 與 durable claim 一起提交；結果與 claim release 一起提交。
  減少同一 task 前後相鄰的 queue fsync，並未取消 HTTP 前的 durable claim、來源 fsync、
  SHA、receipt history 或驗證。斷電在來源發布後／queue 提交前，仍由既有 claim recovery 恢復。
- 每次只發布一個已完成 future，立即補空出的 HTTP slot，再處理下一個 response；
  其餘已完成 response 仍計入有界 buffer，不能藉 completion burst 超出記憶體 admission。
- 正式 JSON pool 由 8 調整為 16 workers，decoded response admission 保留 1 GiB 上限；
  不是 16 倍帳戶配額，也不是 Python 整個 process RSS 的硬上限。
- 原生分類證實部分失敗是 `local_traffic_busy`，並非來源資料或官方限流。
  同一 process 先以有界 gate 排隊，才開啟單一 SQLite writer；其他 process 仍由 SQLite 鎖協調。
  僅 gate 不足以消除原生競爭，另保留一條 query-only、無持續 read transaction 的帳務連線，
  避免每次最後關閉 WAL／checkpoint 又和新 reader／writer 競爭。每次真正派送前的獨立
  FULL durability 交易與連線關閉仍保留，不用 NORMAL／OFF 換取速度。
- `local_traffic_busy`／`local_traffic_unavailable` 是 HTTP 前的本機失敗，現在與 limiter cancellation
  分開列為 `local_preflight_holds`，不冒充已送 API call；遇到這類失敗暫停補位並完整 drain，
  由原 owner 短期重試，不讓零 call 批次變成 hot loop，也不計為來源永久失敗。
- 仍執行原有 1,200-call quantum、完整發布驗證與分批來源完整性稽核。
  JSON ETA 合約升為 v12，舊版快照不能校準新版本；完整週期成本保留，
  實測的優先追新等待另列，避免 ETA 重複計算。
- 新增每批實際 call／accepted／nonempty／failed／cancelled／rows 與失敗分類觀察。
  失敗批次不得校準成功 capacity；API 呼叫、收到空值與完整歷史仍是不同概念。

### FinLab

- 原 tick 串行改為有界 process pool。正式政策為 8 workers、queue depth 4、
  authorization slots 8。官方 minute 與 tick 皆允許至多 31 個日曆日的來源視窗；
  已知歷史 tick 實際採 7 日曆日，攤提 manifest／signing，仍逐 stock-day 驗證、發布收據。
  最新日期、未知來源及舊失敗探測維持單日，缺日 range 會解析可取得日期並回到單日修復，
  quota／權限／rate limit 不做盲目 range 重試。
  分鐘既有 32 workers／31 calendar days 的可重用來源視窗保留。
- 已知歷史視窗先收較舊缺口，再向後補滿同一視窗；遇到已快取分區、來源邊界及最新日期即停止，
  避免 oldest-first frontier 每次只有一個交易日。完成 burst 同樣先補 worker，再做第二個 parent 發布。
- tick 使用 SDK 公開的 `save_to_storage=False`，避免 immutable source／receipt 已寫完後，
  SDK 再重複寫 FileStorage 與全域 expiry pickle。SDK 內部來源驗證與官方 quota enforcement 保留。
  分鐘 SDK cache 不關閉，避免失去既有部分視窗重用。
- 移除每個分區強制做 whole-heap GC，改為每 64 分區一次；Python 正常 GC 不關閉。
- tick 額度觀察在遠離保留線時最多重用 4 個請求或 5 秒；距離保留線 100 MB 內逐次觀察。
  跨重置、保留額度改變、時間倒退、觀察失敗立即失效，SDK 每次來源請求仍檢查官方限制。
  額度觀察命中不會冒充新 API sample 或計費流量。
- coverage 改成一次 SQL join／aggregate，重用小型 TEMP universe／calendar 維度，
  不建立 symbol × day Cartesian grid、不在 Python 遍歷全部分區、不使用過期時間快取。
  維度改變、未提交寫入與 rollback 都有測試。
- 舊日期未知來源探測每輪限 16，舊失敗重試限 4；分鐘與 tick 同樣適用。
  普通歷史游標不再繞過 retry cap 重複探測已知失敗日。
- 正式 tick 每輪 quantum 由 256 調整為政策設定 1,024，以攤提 SDK／pool／index 初始化
  及既有 timer 的間隔。這不是 1,024 個 in-flight response；原有有界 pool、每次 dispatch
  的分鐘／一般資料優先、quota、disk guard、1,140 秒執行上限與外層 watchdog 仍保留。
- Acquisition policy v14。正常分鐘保留線 50 MB、tick 500 MB；重置前一小時進入既有 drain
  政策，不能把調整本機保留線當成忽略官方額度。
- 面板 ETA 用完整分鐘／tick 各自的 worker、range、policy version 與 fingerprint 過濾樣本。
  改版後樣本不足便顯示校準中，不用過去單日串行 tick 速率估算新多日平行服務。
  公開面板原 process 載入舊 policy class 曾顯示 unavailable；已經沿 canonical service 重載，
  實際 HTTP readback 為 available／worker_policy_current=true，沒有由網頁補呼叫 provider。

## 可重現的對照驗收

所有本機比較均限制 CPU threads、禁止 CUDA，使用保留的真實 queue／來源 fixture。
沒有為跑比較重抓 provider、掃描整個 Parquet 歷史或開第二個正式 owner。

| 工作範圍 | 對照 | 修正後 | 證據與限制 |
| --- | ---: | ---: | --- |
| 兩個 owner 的預留、狀態、發布重查及 durable claim/release | 27.443 秒 | 7.111 秒 | ABBA、完整輸出相同；不是網路下載速度 |
| 上述工作中的美股發布重查 | 18.217 秒 | 0.0619 秒 | 原始時間判式、相同資料 |
| 上述工作中的週期 snapshot／year 查詢 | 4.074 秒 | 0.00173 秒 | 同樣的 task 結果 |
| 更正清單候選範圍查詢 | 6.654 秒 | 0.353 秒 | ABBA、完整候選集合／值相同，仍由原判式判定 |
| 357 萬 task、實際來源 payload 的完整驗證／落盤／receipt／status | wall 3.768 秒；CPU 2.010 秒 | wall 2.435 秒；CPU 0.747 秒 | 全 task 值、完整 status 與來源值相同 |
| FinLab 全 inventory coverage、完整輸出／fsync | 5.374 秒 | 5.317 秒 | 輸出完全相同；差異很小，不宣稱顯著整體加速 |

FinMind 新增發布重查與預留索引的 Complement 一次性建立成本約 10.76 秒／9.24 MiB；
不能拿首次建立索引的週期當成穩態成本，也不能忽略建立／更新成本。
本機 gate 的 16-thread／512 筆小 WAL ABBA 為 0.279→0.392 秒，並非微型速度提升；
收益目標是降低正式帳本競爭與錯誤重試。query-only anchor 的小 fixture 也沒有單獨速度收益，
不以它宣稱加速。source publication ABBA 不包括網路／帳務鎖壓力，須再看原生完整週期。

完整證據：

- [發布重查／預留／週期對照](../artifacts/data_quality/finlab_finmind_throughput_2026-10-18/periodic_query_control.json)
- [更正清單範圍對照](../artifacts/data_quality/finlab_finmind_throughput_2026-10-18/correction_query_control.json)
- [完整來源發布對照](../artifacts/data_quality/finlab_finmind_throughput_2026-10-18/wal_lifetime_publication_control.json)
- [帳務 gate 小型對照](../artifacts/data_quality/finlab_finmind_throughput_2026-10-18/request_traffic_abba.json)
- [FinLab coverage 對照](../artifacts/data_quality/finlab_finmind_throughput_2026-10-18/finlab_final_coverage/comparison.json)
- [FinMind v12 正式 owner 安全載入與來源保留](../artifacts/data_quality/finlab_finmind_throughput_2026-10-18/native_reload_wal_lifetime.json)
- [正式 FinMind v12 三個完整週期所在的吞吐窗口](../artifacts/data_quality/finlab_finmind_throughput_2026-10-18/native_finmind_three_cycle_throughput.json)
- [FinLab 正式 workload／公開投影／timer／近期 tick 來源 readback](../artifacts/data_quality/finlab_finmind_throughput_2026-10-18/native_finlab_timer_source_audit.json)

## 正式服務觀察與剩餘邊界

FinMind v10 於 18:33:32 UTC 安全重載後，第一個完整 1,200 次派送嘗試的 JSON 批次
為 243.70 秒、1,187 accepted／452 nonempty／80,175 rows、13 failed attempts。
18:33:33–18:39:08 UTC 的 5.59 分鐘實際記錄 1,466 次 request starts，
中位間隔 0.18008 秒、p95 0.27989 秒；有 9 個超過兩秒的間隙，合計 42.82 秒。
全窗口約使用理論平滑速率的 78.7%，**尚未證明只剩 API 限制**。
此窗口包含啟動／準備且同時有其他視窗的高負載工作，不能和舊版不同來源的窗口作因果比較。
需繼續區分初始化、來源延遲、持久化與其他 owner 的優先工作，不能直接把 ETA
改成 `remaining_requests / 20,000`。有失敗的整輪不作成功 capacity 校準，即使後續重試已恢復；
這也是面板樣本不足時不得以舊版樣本冒充的原因。
18:50:33 UTC 完成的分類批次，1,200 次派送嘗試中有 10 次 `local_traffic_busy`；
舊版 call 欄位會把這些 HTTP 前失敗也算入。v11／v12 已修正計數，這不是 provider 失敗。
[原批次快照](../artifacts/data_quality/finlab_finmind_throughput_2026-10-18/native_v10_ledger_failure_batch.json)
保留原始錯誤；v11 僅使用 gate 的正式前段仍有兩次 busy，沒有把它記成已修好。

v12 於 18:59:11 UTC 載入，canonical Complement InvocationID 為
`d5fa0ca693db4627871c6a496ee1ff34`。之後三個不中斷的原生完整週期如下；
`cycle_wall_verified=1`、實際排除時間均為零，不用 reload 截斷的舊批次作完整週期證據。

| UTC 起訖 | 合格 JSON 樣本 calls | 帳戶實發 starts | fetch／validate／publish 樣本秒數 | 完整 owner 週期秒數 |
| --- | ---: | ---: | ---: | ---: |
| 18:59:12.588–19:02:59.085 | 1,200 | 1,201 | 216.307 | 226.497 |
| 19:02:59.115–19:06:48.119 | 1,199 | 1,200 | 218.163 | 229.004 |
| 19:06:48.160–19:10:33.543 | 1,200 | 1,200 | 215.941 | 225.384 |

樣本只納入至少兩筆且整批 accepted／零 failed／無 cancellation 的批次；
第二輪另有一筆單獨派送不納入 capacity 樣本。帳戶 starts 包含其他 owner，不能當成
Complement 成功數。首輪的合格樣本為 352 nonempty／63,637 rows，
第二輪 484／82,036，第三輪 448／73,321；來源正常空回應不是漏計，也不是完整歷史。
第一輪 1,200 筆合格樣本的完整週期折算約 19,073 requests/hour（上限 95.4%），
而 fetch／validate／publish 本身約為平滑配速的 99.9%。三輪仍有約 9–11 秒的
非派送成本，不能宣稱本機計算已歸零。

18:59:12–19:10:58.972 UTC 的 11.783 分鐘帳戶窗口記錄 3,703 次 request starts，
中位間隔 0.179988 秒、p95 0.184102 秒，折算約 18,856 requests/hour（94.3%）。
此窗口含啟動、輪間工作與其他 owner，不是無限期持續吞吐保證。
原生服務 journal 在此期間未再出現 `local_traffic_busy`；現有證據支持本機競爭修正有效，
仍不能推論未來所有來源／負載都沒有局部瓶頸。

19:09 UTC 的實際唯讀面板已讀到當分鐘 ETA；FinMind 約 16,096 次的滾動一小時值仍含
本次較早版本、重載和低效區間，不應直接等同目前約 19k/h 的短期完整週期能力。
ETA 仍計入階段順序、發布追新與尚未證實的容量，沒有硬改成理論 20k/h。
獨立原生 complete-status／uncached readback 曾因採樣期間 owner generation 改變而無法接受，
該報告保留為 inconclusive；離線完整輸出／來源 parity 已通過，兩者不混為同一證明。

FinLab 原串行 256-tick pass 曾為 960.60 秒；4-worker 實際 pass 約 240–264 秒。
股票、回應內容與來源空值不同，不是受控 ABBA。後續原生完整批次如下：

| 政策／視窗 | attempts | 新來源收據 | SDK calls | HTTP POST | HTTP GET | 完整批次 wall |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8-worker／單日 | 1,024 | 1,010 | 1,024 | 2,050 | 1,010 | 522.03 秒 |
| v13／7 日曆日 | 1,024 | 998 | 220 | 431 | 998 | 337.08 秒 |
| v14／7 日曆日、雙向填滿歷史視窗 | 1,024 | 1,004 | 228 | 443 | 1,004 | 320.19 秒 |
| v14／後續完整批次 | 1,024 | 1,017 | 214 | 423 | 1,017 | 301.59 秒 |
| v14／後續完整批次 | 1,024 | 1,016 | 216 | 426 | 1,016 | 301.03 秒 |
| v14／19:09:59 UTC 完成批次 | 1,024 | 1,020 | 211 | 419 | 1,020 | 243.08 秒 |

表列批次均無 429／503。v14 首批原生觀察相較單日批次約少 38.7% wall／78.4% POST，
不是同內容受控加速比。GET 對應實際 day object，沒有聲稱多日視窗把所有 HTTP 合成一個。
v14 歷史視窗是 205 個 window requests，SDK calls 另外包括實際單日 fallback；不能把兩者混列。
官方 billing 不由 HTTP bytes 或本機 Parquet 大小推算。
有資料的 4-worker 舊批次額度查詢由約 256 次降到 65 次，保留 207 次快取命中。
新批次仍有 parent 驗證／發布與 CLI 啟動成本；不能宣稱軟體成本已歸零。

FinLab 本機 readback 已通過 workload／階段／分鐘與 tick 去重、quota policy、
ETA 三情境順序與最近 16 個來源物件 SHA／schema 檢查；不代表尚未發布資料已取得，
也不代表全部歷史完整。來源空值、尚未發布、權限錯誤與本機錯誤仍分開記錄。

FinLab timer 檢查曾誤報未部署：systemd 對多個 `TimersMonotonic`／`TimersCalendar`
輸出同名欄位，原 dict 解析覆蓋前項。已修正保留所有項目，實際 OnUnitInactiveSec=1min、
quota reset／pre-reset 時段都存在；沒有更改正式 timer。
舊 FinLab baseline 對本次授權的 FinMind 重載仍報 identity 改變，這是預期比較差異，
不抹掉該失敗報告。以當時最新服務身分重新驗收，來源／投影／timer／受保護服務皆通過；
本次最初到現在的交易 owner 不變，另由 FinMind reload receipt 證明。

截至 v12 最終完整 focused regression，FinMind／FinLab、runtime 與安全載入相關
**1,780 tests passed**（123.10 秒），公開面板／安全存取另 **145 passed**（46.01 秒）。
驗收收據分別為
`finlab-finmind-v12-full-acceptan-20261017T185840-5e7f8658` 與
`finlab-finmind-dashboard-regress-20261017T183326-4933673f`。
`bash -n scripts/run_finlab_refresh.sh` 與本次修改檔案的
`git diff --check` 通過。未進行無關 GPU 訓練或修改 factorized 檔案。

## 官方限制依據

- [FinMind Pricing](https://finmind.github.io/Pricing/)；實際帳戶 observation 優先。
- [FinMind 美股技術資料](https://finmind.github.io/tutor/UnitedStatesMarket/Technical/)；
  不以無效的多 ID 或被忽略的 end date 宣稱減少 call。
- [FinLab 取得資料](https://finlab.finance/docs/details/get_data/)、
  [分鐘／tick](https://finlab.finance/docs/details/intraday/)、
  [data reference](https://finlab.finance/docs/reference/data/)；
  日額度使用官方 user-info observation，不用 HTTP 壓縮大小代替。
