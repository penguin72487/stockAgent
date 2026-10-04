# FinMind 官方校正公告監看與定向修復

更新日期：2026-09-27。每日監看已部署並完成首次正式檢查，三個既有 worker 已載入修復邏輯；**歷史修復仍在背景進行，不代表全部下載完成**。本次執行證據見文末。

## 目的與邊界

每日讀取 [FinMind 官方更新公告](https://finmind.github.io/WhatIsNew/)，將可明確定位的資料校正轉成既有下載器的修復工作。公告監看本身不呼叫行情資料 API、不建立另一套下載器、不操作交易或模型。

流程是「官方頁面快照 → 公告解析 → 精確範圍計畫 → 原有 worker 接收 → 原有配額／排程下載 → 驗證新收據」。公告取得成功、計畫產生成功、修復已排隊與修復已驗收，是四個不同狀態。

## 排程與手動指令

已提供 `deploy/systemd/stockagent-finmind-announcements.service.in` 與 `.timer.in`：

- 每日 **06:10 Asia/Taipei** 檢查；週末與休市日照常，不依交易日曆略過。
- `Persistent=true`：systemd 恢復時補觸發錯過的計時工作；不是補跑每一天的所有歷史排程。
- 失敗後間隔 15 分鐘重試；`StartLimitIntervalSec=1h`、`StartLimitBurst=3` 限制一小時內最多啟動 3 次，包含首次啟動。
- 單次 timeout 120 秒，使用低 CPU／I/O 權重；一把非阻塞 `watcher.lock` 避免重複監看。

在 repository root 使用已設定的 runtime：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.watch_finmind_announcements --dry-run
```

`--dry-run` 仍會取得並快取官方公告頁面與 parser 狀態，但不發布 `repair_plan.json`，亦不寫入 orchestrator 的監看狀態或檢查紀錄；它不是完全無檔案寫入的指令。

發布修復計畫：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.watch_finmind_announcements
```

可用 `--root PATH` 指定公告工作目錄，預設 `data_finmind/announcements`。CLI 不直接呼叫行情 API；正式 worker 依自己的週期接收計畫，因此發布計畫不等於立即完成下載。

## 狀態與稽核資料

以下路徑以預設公告目錄為基準；執行時應使用目前實際配置的 live root。

| 路徑 | 說明與判讀 |
| --- | --- |
| `data_finmind/announcements/status.json` | **Parser／來源取得狀態**，包括官方頁面取得或解析是否成功。不是 worker 修復完成率。 |
| `data_finmind/announcements/head.json`、`snapshots/` | 最近成功來源及按 SHA-256 保存的原始 HTML；來源取得失敗不能把失敗頁面升成成功 head。 |
| `data_finmind/announcements/monitor_status.json` | **Orchestrator 狀態**：計畫 ID、公告數、scope 數、owner 分布、下次檢查時間與錯誤。`planned` 只代表計畫已產生。 |
| `data_finmind/announcements/repair_plan.json` | 原子發布的 schema 1 修復意圖，含 `requests`、`notices`、持續偵測用 `state` 與來源快照雜湊。 |
| `data_finmind/announcements/checks/` | 非 dry-run 的逐次監看結果。 |
| `data_finmind/sponsor/queue.sqlite3`、`data_finmind/complement/queue.sqlite3` | 沿用既有任務；`finmind_correction_tasks` 保存範圍、先前任務與驗證證據，`finmind_correction_events` 保存排隊事件。 |
| 各 Sponsor／Complement root 的 `correction_status.json` | 當輪套用或驗證摘要；`applied`、`deferred`、`already_refreshed`、`unmatched_requests` 等不應混成下載成功數。 |
| Free 的日資料／snapshot 收據與既有 `status.json` | Free 使用原有收據式排程及 `free_correction_due`；不是另造 Sponsor／Complement 的 SQLite 任務模型。校正 metadata 寫回新收據供驗證。 |

來源取得或 scope 規劃失敗時，既有計畫保留，監看狀態標為 degraded，不能用舊頁面冒稱本次更新成功。`unmatched_requests` 表示尚未匹配到既有任務，不是資料已完整；之後新 seed 的任務仍須再次比對。

## 首輪已審閱範圍

離線驗證來源快照 SHA-256：

```text
decb6d1761fabed35ad455cd2ad2bc67034b2ac7d507a6942b8ee6670012f96e
```

此快照解析出 **177 則公告**，其中 **8 篇**近期校正經人工逐段審閱，產生 **54 個 scope**：Sponsor 50、Free 3、Complement 1，涉及 **20 個直接來源 dataset**。另有 1 個法人寬表衍生 dataset，由原始長表重建，合計 21 個來源／衍生 dataset；**衍生重建不增加 API 呼叫**。54 是修復範圍數，不是請求次數、股票數、任務數或待下載筆數。

| 公告日期／主題 | Dataset 與範圍 | Owner／scope 數 | 已知限制 |
| --- | --- | --- | --- |
| 09-25 每 5 秒指數 | `TaiwanStockEvery5SecondsIndex`，既有起點 2005-01-03 至公告日 2026-09-25，修正收盤值及名稱 | Sponsor／1 | 2017-05-08 上櫃 09:00:00 原始來源未提供，不可偽補。 |
| 09-25 委託成交統計 | `TaiwanStockStatisticsOfOrderBookAndTrade`，2011-01-21、2023-08-04 | Free／2 | 公告另稱部分日期有重複時間點，但未列出日期；該部分保留 needs_review。 |
| 09-25 日／週／月價量 | `TaiwanStockPrice`、`TaiwanStockPriceAdj` 各 4 個確定日期；`TaiwanStockWeekPrice`、`TaiwanStockMonthPrice` 各為 2020-03-01 至 2024-12-31 | Sponsor／10 | 日資料日期為 2015-11-23、2020-03-09、2020-03-10、2020-04-24；上櫃成交量、成交額、筆數口徑修正，不能把所有日期套到所有粒度。 |
| 09-24 主動式 ETF | `TaiwanStockActiveETFHolding`、`TaiwanStockActiveETFHoldingChange` 各為 2026-02-11、2026-02-23 | Sponsor／4 | 00981D、00983A、00988A、00990A 的 2026-02-10 來源未提供持股；不把比較基準日誤排成可修復日。 |
| 09-20 三大法人 | `TaiwanStockInstitutionalInvestorsBuySell`，2017-12-18 至 2018-01-12；同步使 `TaiwanStockInstitutionalInvestorsBuySellWide` 由長表重建 | Sponsor／1 | 19 個交易日的上櫃更正；長寬表不各抓一份。 |
| 09-20 颱風停市日與歸屬日 | 四個日資料集 × 7 天；夜盤法人 2022-07-24；交易日曆 snapshot 2026-07-10 | Sponsor／29、Free／1 | 僅明列的完整日分區允許 authoritative empty；交易日曆需全表更新。逐筆與分鐘夜盤真實成交不受影響。 |
| 09-06 財報及除權息 | `TaiwanStockBalanceSheet` 的 2024-06-30；`TaiwanStockDividendResult` 的 2026-09-02 | Sponsor／2 | 25 家已下市／終止公開發行公司之來源財報不可補；`TaiwanStockFinancialStatements`、`TaiwanStockCashFlowsStatement` 明確未受影響，不重抓。 |
| 09-01 全史修正 | `TaiwanStockPriceAdj`、`TaiwanStockMarginPurchaseShortSale`、`TaiwanStockIndustryChainMoneyFlow`、`TaiwanStockDelisting`，各既有起點至 2026-09-01 | Sponsor／3、Complement／1 | Delisting 沿用 Complement 自 2001 起的歷史任務，不另造 snapshot owner。未啟用的 spread／Tick／KBar 與未列明的週六補行日期維持 not_scheduled／needs_review。 |

颱風部分精確日資料集：`TaiwanFuturesDaily`、`TaiwanOptionDaily`、`TaiwanFuturesDealerTradingVolumeDaily`、`TaiwanOptionDealerTradingVolumeDaily`。七個日期為 **2023-08-03、2024-07-24、2024-07-25、2024-10-02、2024-10-03、2024-10-31、2026-07-10**。另處理 `TaiwanFuturesInstitutionalInvestorsAfterHours` 的 2022-07-24 重複日，及 `TaiwanStockTradingDate` snapshot 的錯列日期。

## 去重、優先序與配額

- Scope registry 按**完整公告 entry_id**及全文固定，不能只憑標題日期套用舊範圍。請求的 `correction_id` 再包含 dataset、日期區間、data IDs 與空資料政策；同一公告的兩個不連續日期具有不同 scope ID。
- 每個 dataset 只有既有主責 owner；Sponsor 已主責者不再交給 Complement 重抓。產品結算歷史仍由 Complement；Free 保留原生盤中資料與日曆等責任。
- 已符合本次門檻且驗證通過的既有收據可記 `already_refreshed`，不消耗重複 API。收據身份、檔案 SHA-256、大小、Parquet 筆數等必須吻合，不只看完成時間。
- Sponsor／Complement 校正任務被降到 `priority=min(priority,1)`，列入必要修復；優先於 P3 跨來源校驗，仍不能越過 P0 追新、保留配額、開盤保護、官方速率、帳戶權限、磁碟與既有 cooldown。
- 不重新排全部歷史：只匹配受影響的既有任務。對必須整史重算的公告，範圍固定為來源已知起點至**公告日**，不因每日監看而持續向後膨脹。
- 一個大 scope 可能跨很多既有分區；多個 scope 也可能由同一次既有批次查詢覆蓋。實際 API 次數取決於分區、可用批次介面、既有收據與配額，不能由 54 scopes 推算下載 ETA。

## 時間證據、保存與安全條件

首次建立 baseline 時，過往公告使用「公告次日 00:00 Asia/Taipei」作收據時間篩選；若該時間仍在未來，使用當下偵測時間，不必等隔天。這是**公告只有日期精度時的工程近似，不是精確發布時間，也不是訓練 PIT 證明**。

之後的新公告或修改公告，即使掛在舊日期章節，門檻一律取首次偵測該新內容的時間。相同 entry_id 每日保留同一門檻，不每天重派。新／修改公告及當日 baseline 不接受「請求先開始、公告後才寫收據」作完成證據：需要 task-bound correction marker，或符合門檻的 `request_started_at_utc` 與取得時間證據。

重抓沿用既有版本化 raw 與收據保存機制；先前收據留在 `receipt_history` 等稽核位置，校正表另記 `prior_task_json` 與事件。不能把改名、HTTP 200 或收到空陣列當成已修復證據。

`allow_empty=true` 只出現在經審閱且精確授權的日 scope。刪除有效分區前必須把 owner／dataset／data_id／partition 與授權 token 對上；不能擴成整個 dataset 的空資料豁免。尤其**交易日歸屬修正不能刪日曆時間的 tick／分鐘夜盤**：停市日 00:00–04:59 可能仍是前一交易日的真實成交。

未知、例子日期、比較基準日、未受影響期別、無法回補項目與模糊範圍不自動猜測。未來只有清楚指定 dataset 與單一日期／單一区間／全歷史的校正才可自動 scope；已人工審閱公告若改字，必須重新審閱，不能沿用舊全文範圍。

## 驗證及正式執行觀察

本次純本機測試指令：

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest test/test_finmind_announcements.py test/test_finmind_correction_plans.py test/test_finmind_corrections.py test/test_finmind_announcement_watcher.py -q
```

最終完整 FinMind 回歸指令為 `run_fintech_python -m pytest -q test/test_finmind*.py test/test_probe_finmind_max_ranges.py`，本次 **483 passed / 16.60s**。另以保存的官方 HTML 離線產生計畫，核對 177 公告、54 scopes、20 個直接來源 dataset 與 owner 分工。

正式驗收仍需確認：timer 實際安裝與下次觸發時間、監看新快照和計畫收據、worker 是否接收、配額是否足夠、受影響分區的新 source receipt 是否通過，以及 known_unavailable／needs_review／unmatched 是否仍存在。服務 active 或計畫 planned 不能替代這些證據。

### 2026-09-27 11:37 台北時間部署證據

- 已透過 `scripts/install_registered_data_refresh_services.sh finmind-announcements-only` 安裝 timer；實際 `active/waiting`，下次為 **2026-09-28 06:10:00 CST**。只部署公告單元，未重新安裝其他 provider 或交易服務。
- 首次正式檢查 `2026-09-27T03:35:40.725608+00:00`：官方 HTTP conditional GET 回 `not_modified`，SHA 與上方一致，service `Result=success/ExecMainStatus=0`。行情 API 用量 **0**；計畫 ID `a7557a443fa4880fa24ee98b0954c731c06b72b2b270da11cd07da701d11b4d4`。
- 三個 FinMind worker 已受控重啟；最新 PID Sponsor 2442029、Complement 2442018、Free 2442019，皆 active/running、NRestarts=0。修復索引 `idx_finmind_correction_task_context`／`idx_finmind_correction_reconcile` 均已在正式 SQLite 建立，避免每個收據掃描整張稽核表。
- `2026-09-27T03:37:36.337307+00:00` 讀取 Sponsor correction ledger：`already_refreshed=226`、`repaired=116`、`queued=21461`、`deferred=23456`。這些是 **公告 scope × 既有任務的稽核紀錄**，有範圍重疊，不能相加當作唯一分區、API 呼叫數或缺失 K 棒筆數。
- Sponsor 已驗收樣本包括 `TaiwanFuturesDaily` 的 2023-08-03、2024-07-24、2024-07-25；精確日更正的官方空回應已保留為 authoritative-empty 收據，並非補造交易資料。
- Complement 26 個受影響年度任務已通過現有收據檢查，記 `already_refreshed`，無須重抓。Free 的 2011-01-21／2023-08-04 委託成交統計也已是公告後版本且通過檔案驗證；本輪 Free 行情請求 **0**。
- `deferred` 不全是錯誤：首輪含 12160 筆非交易日排除，其餘包括尚待分批 SHA/footer 驗證的完成／空回應。為限制 CPU／I/O，每轮檢查至多 128 份既有來源收據；未驗證者不會被標成修復完成。後續數字會持續變動。

仍有官方不可補、公告日期不明、未啟用資料集與歷史未逐段審閱項目，詳見 `repair_plan.json` 的 notices／issues。本次未打包或發布冷庫、未修改訓練 PIT 標記、未調整交易服務。
