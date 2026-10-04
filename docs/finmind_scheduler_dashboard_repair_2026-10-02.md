# FinMind 排程與網頁修復驗收

任務：修正 FinMind 排程、進度與預估不一致，降低派工與網頁的不必要計算。
工作自 2026-10-02 開始，驗收跨至 2026-10-03（Asia/Taipei）。
Git 基線 `894e782d`；保留既有 dirty work，未修改 TEJ、FinLab 或交易服務。
面板：[FinMind](https://penguin72487.ddnsgeek.com/finmind/)。

## 根因與修正

| 根因 | 修正與邊界 |
| --- | --- |
| 每次派工掃描整張 tasks，指定回補也反覆掃已完成的 priority overrides | 分離未完成與到期刷新的 partial indexes；從未完成任務連接有限優先回補。不刪舊任務／收據。 |
| 同優先級的大型分鐘資料集長期占用整個歷史通道 | 各優先級獨立 round-robin；到期追新、完整性修復、指定回補仍優先。tick 的低優先級不變，狀態預覽不推進 cursor。 |
| 已委派的 priority-0 工作及第一筆 override 可以遮住下一筆可執行回補 | 在 SQL 的 LIMIT 前排除已由 Sponsor 承接的 dataset；不把委派當作完成或刪除證據。 |
| 同一次 Complement 派工重算兩次追新預留 | 共用一次帶時間戳的 reservation plan；不同時間／預留量的 plan 不得放行。worker 仍按每次派工的即時帳本 admission，不以網頁快照代替。 |
| Free 已追新被當成整個 FinMind 已追新 | 公開總狀態綜合三個 worker，另保留 Free/Sponsor/Complement 的各自狀態與心跳。 |
| 任務分母含約 7,790 萬未建歷史候選，但文案寫「已建任務」 | 公開已建、已建待辦、未建候選與查驗數；進度明確為候選搜尋比例，非資料完整率或缺漏筆數。 |
| frontier 只算尚未建立的舊歷史，未算停機／有限 working set 尚未建入的新日期 | 按既有 source release cutoff 計入尚未 seed 的 forward periods；已建任務不重複加回，未到發布時間不新增候選。 |
| 空閒 cycle 清掉最近結果，前端又固定偏好 Sponsor 的舊結果 | Complement 在實際完成／失敗時記錄結果時間，跨 cycle 保留而不刷新原時間；前端使用三個 owner 的最近結果投影。舊 worker 無結果時間時只能按 cycle 觀測排序，不聲稱精確完成時間。 |
| 過期、零請求的階段仍可能顯示「無待發請求」 | 零工作提示也必須通過估算期限與阻塞／未排程檢查；過期與超過預估都不得推論成功。 |
| 受限唯讀 gateway 偶爾無法開啟活躍 SQLite WAL；錯誤分支連官方上限也消失 | 在 gateway 的 mount namespace 重現 `SQLITE_CANTOPEN`。既有分鐘 sampler 發布流量＋admission 快照 v2；網頁不再重算 25 小時流量或讀該活躍 WAL。流量讀取失敗仍保留獨立核實的官方上限，未知用量不是零。 |

## 第一性原理的工作量定義

- 已建任務是實際 SQLite 佇列；候選是已知代號 × 日期範圍的搜尋上界，仍需上市期間／交易日／來源有值狀態查驗。
- `candidate_requests` 是預估請求內的子集合，不另外加一次。ETA 和逐資料集狀態各自保留取樣時間，不能要求不同時刻的數字完全相等。
- 請求發出數、成功任務、非空任務、資料筆數、容量、資料完整率是不同單位；來源空回不等於完整歷史，也不能補造價格或數值。
- 主要歷史、分鐘／分點、tick、校驗依目前 staged ETA 分開看；零工作是「目前可盤點的該階段無待發」，不是永遠完成。
- 分階段長期 ETA 保留最快／中間／最慢與新增分區、固定刷新模型；超出可核實日曆、來源歷史代號與尚未發生的修訂仍是不確定性。沒有完成日期的硬保證。

## 效能證據

在同一份 320,621 任務的暫存 SQLite backup 上，每個方案量測 9 次：

| 比較方案 | 派工 wall time 中位數 |
| --- | ---: |
| 原 selector ＋原索引 | 0.254628 秒 |
| 原 selector ＋新索引 | 0.083721 秒 |
| 新 selector ＋新索引 | 0.037988 秒 |

完整修改降低 wall time 與 CPU time 約 **85.08%**；單獨比較新 selector 與已加索引的原 selector，wall time 降低 54.63%。
這是固定快照的派工成本，不是下載速度提升 85%，也不是全系統 CPU 降低 85%。
量測不呼叫 provider、不修改 production queue；原索引重建只作用在 disposable backup。
初次未部署快照曾測得 89.73%；另一個已帶新索引的線上快照曾為 30.17%，不能混用比較基準。

可重跑：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.benchmark_finmind_dispatch \
  --output artifacts/data_quality/finmind_scheduler_dashboard_2026-10-02/dispatch_benchmark_controlled.json
```

## 部署與實際下載

- Complement 2026-10-02 23:49:32 台北重啟；公開 gateway 23:56:02 載入最新版。Free、Sponsor 沿用原服務，未重啟交易服務。
- Complement 公開 `dispatch_contract_version=2`；兩個 partial indexes 已在實際 queue 存在。
- 原本的一分鐘 quota timer 自動發布 `dispatch_status.json` v2；ETA snapshot contract 升為 v4。沒有另開 scheduler、下載管線或 API 探測輪詢。
- Sponsor 帳號當次核實為每小時 6,000 次，三個 worker 共用 limiter；未改為每個 worker 各有一份配額，也未繞過來源限制。[官方帳號流量查詢說明](https://finmind.github.io/api_usage_count/)
- 23:49:21–23:56:55 的 queue 查驗，已看到五個同優先級來源輪替且取得非空資料：期貨分鐘 2 個任務／234 筆、股票分鐘 112／9,234、股票分點 110／195,900、權證分點 95／3,150、美股分鐘 79／11,863。本機分點聚合另完成 110／31,488，不另呼叫 API。此範圍亦有正常來源空回，不能算作有值資料。
- 23:58:32 公開瀏覽器取樣：已查驗 477,174，已建 478,325，未建候選 77,896,340；總候選 78,374,665，候選查詢進度 0.61%。這些都是當次快照，不是永久數量或確定缺漏數。
- 流量及 admission 有相同分鐘取樣時間，官方帳號觀測時間另列；候選目標和 ETA 較舊／較新時以各自時間解釋。

## 驗收

- 最終相關回歸：`855 passed in 70.14s`，包含 FinMind 排程、共用 quota、有限 key／優先回補、bulk、parser、儲存、觀測、ETA、public gateway 及實際 Chromium 測試。
- 實際 HTTPS 網站在 1440／1024／390 CSS px 通過：API 200、零 JavaScript error、零 document horizontal overflow、132 管線的篩選／回復、5 個階段，以及進度數字對帳。瀏覽器沒有呼叫 FinMind provider；已檢視桌機與手機截圖。
- JS syntax 與 affected diff whitespace 檢查通過；原始 source observations、receipts 與既有研究 artifact 未刪除或覆寫。
- 維持 `all_history_complete_claim=false`。分鐘、分點與最低優先 tick 歷史仍在回補；未知歷史主檔、上市期間、資料可用性和 PIT 品質不能由服務存活或該進度百分比保證。

重跑實際網頁驗收（只讀，不呼叫 provider）：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.verify_finmind_scheduler_dashboard \
  --output-dir artifacts/data_quality/finmind_scheduler_dashboard_2026-10-02/browser
```

當次證據位於 `artifacts/data_quality/finmind_scheduler_dashboard_2026-10-02/`：
`deployment_baseline.json`、`dispatch_benchmark_controlled.json`、`runtime_acceptance.json`、
`browser/browser_acceptance.json` 與三種尺寸 PNG。
最終測試收據：`artifacts/operations/agent-workflow/runs/finmind-regression-20261002T155823-df9daa59/`。
