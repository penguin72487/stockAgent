# 架構現代化續作與服務驗收（2026-10-03）

這份報告接續 [架構現代化分析](architecture_modernization_2026-10-03.md)。
本輪以收益語意、來源版本與恢復證據為優先；效能選擇不能掩蓋破產、
未完成採集或不同來源版本。原工程驗收與本輪的服務／資料驗收分開保存。

## 第一性原理與實作

| 問題 | 已確認原因 | 實作與驗收邊界 |
| --- | --- | --- |
| Tensor 報表把破產當成零報酬 | 兩個 metric helper 將 `-inf` 清成 0，並先窄化 FP64 | NumPy 與 tensor 共用 reporting normalization 契約；首次 default 後報表資本吸收於零，累積收益與最大回撤為 -100%。保留既有有限 log-risk floor；不改 loss、執行價格或 optimizer 軌跡。 |
| 舊報表被 resume 當成現行結果 | 完成旗標只證明舊 artifact scope | 報表契約版本 2 寫入 mode contract／run manifest；有 expected checkpoint manifest 的正式 reuse 要求現行報表版本。舊 checkpoint 相容性與報表刷新分開，completion gate 檢查 manifest 與 fold 的版本一致。 |
| 欄位投影混合來源版本 | record／feature 掃描間來源原子更新 | 完整與 shard publisher 在 rename 前驗證來源 observation root 與 footer identity。版本移動時保留前一份有效投影及其原始生成時間，記錄 `deferred_source_revision`，交給既有下一輪 producer 刷新；不新增採集器或輪詢服務。 |
| OKX 五個 OI stage 失敗 | 2026-10-02 收據為 `history page exceeds requested limit` | 同一請求只重試一次，仍經 shared limiter；不截斷非法頁、不移動 cursor，不把 persistent invalid response 判成功。acquisition 契約升為 3，feature ABI 保持 1。 |
| OpenBB supervisor 停止 | 當時 free bytes 低於 100 GiB safety floor | 重新驗證已恢復的容量後啟動原服務；保留 floor 與既有 2026-07-18 封存截止日。active 只證明工作啟動，不能代表所有 provider／archive 完成。 |
| 台股衍生收據失效 | symbol／feature build receipts 與最新來源位元組不符 | 持有原 `tw-public-refresh.lock`，呼叫既有 official-symbol／feature builders。後續市場來源變動由原 reconcile service 以 market-only 路徑更新；保留官方來源、截止日與 publication audit。只重建現行服務相依視圖。 |
| 台股本機提交後仍被判發布失敗 | 本機 head 已提交，但 Syncthing scan 逾時；入口在等待網路時仍占用來源鎖 | 使用既有 durable scan outbox，unique commit receipt 與原 scan-retry service；child 借用同一個來源鎖 FD。本機提交、來源 lease 釋放及掃描確認分開驗收；網路通知失敗由 outbox 重試，不因通知重建來源封存。 |
| 美股舊封存服務曾失敗 | 2026-10-01 Syncthing scan timeout；其熱來源於 2026-10-02 已正式退役 | `verify --cold-only` 明確區分冷資料的 encoded／decoded／packed 證據與熱來源比對，不重新下載、訓練或建立 120 GB 熱副本；舊失敗紀錄保留。 |
| 上輪中斷測試影響 TW fold 4 | 測試曾啟動真實預設 trainer；上輪僅恢復 checkpoint | 從已驗證冷封存恢復 18 份有 before-image 的原檔，含 trainer 格式轉換移除的兩份 CSV；14 份事故輸出可逆隔離。1,451 份冷基線原檔逐位元相同，但事故發生前的完整路徑狀態仍缺證據，維持 partial／不可部署。 |

來源 observation 是本次檔案／footer 身分觀測，不能當作金融資料的歷史
發布時間、PIT 合格證明或全集完整度。保留舊有效投影的期間也不代表它是最新版本。

OKX 行為依官方 [OI 歷史端點](https://www.okx.com/docs-v5/en/#trading-statistics-rest-api-get-contract-open-interest-history)
及實際請求核對。五個原失敗合約的 2026-10-01／02 有界查詢均取得
289 個真實原生 5m observations；這不等於補完每個合約的全歷史，也未將
provider 的一次正常回應當作永久恢復。原非法頁收據與新測試均保留。

選擇標準是可重現的正確結果與完整工作流成本。來源 observation 不一致時，
先保留有效版本並等待既有下一輪；通知問題交給既有 outbox；熱副本已退役時
直接驗證冷封存。這些選擇分別避免錯誤版本、重複封存和 120 GB 熱重建，
沒有另建採集／排程／訓練框架。兩次發布的 wall time 含不同冷 I/O 與來源
狀態，因此不把 793 秒失敗與 228 秒成功直接宣稱為受控效能倍數。

## 驗證與進度

證據根目錄：`artifacts/operations/architecture-readiness-repair-20261003/`。
使用既有 `stockagent-agent` task／systemd supervision；原工作樹中的其他修改保留。

| 驗證 | 目前證據 |
| --- | --- |
| 收益、checkpoint、resume、報表定向回歸 | `metrics-focused.log`：182 passed，1 個歷史產物未在熱目錄的條件式 skip。含 CPU／CUDA default 對照、FP64、benchmark default、空序列與報表版本。 |
| 來源發布／OKX 定向回歸 | `coherence-okx-focused.log`：69 passed。包含來源變動時兩種 publisher 都保留 JSON 與 sidecar，以及有效來源刷新後重新發布。 |
| 冷封存／OKX 其他契約 | `storage-okx-focused.log`：54 passed。包含熱來源不存在仍可冷驗證、損壞物件拒收，以及 publication 不得使用 cold-only 旗標。 |
| 清冊、發布與 lifecycle 共用契約 | `shared-contracts-focused.log`：210 passed。 |
| 過期依賴下保留上一代有效投影 | `retained-generation-focused.log`：183 passed；投影與 sidecar 位元組、mtime 都保留，來源簽章或內容失效仍拒用。 |
| 發布來源鎖／durable outbox | `tw-cold-outbox-focused.log`：197 passed；來源父目錄的 canonical lock、借用 FD、lease 釋放後通知、損壞 commit receipt 拒收均有語意測試。 |
| 共用 packed／crypto／cold／materialization 回歸 | `publication-shared-regression.log`：81 passed。 |
| 最後固定發布樹的增量及共用契約 | `final-snapshot-delta-regression.log`：320 passed，24.41 秒；9 個受影響模組。 |
| 共用配額／cooldown 語意 | `rate-limiter-semantic-focused.log`：30 passed。受控 clock 驗 grant interval 與 defer 發布後／返回前消耗 cooldown 的情境，不改 production limiter、請求成本或配額。 |
| 最新服務與恢復 | 下節的獨立 runtime receipts；不以 systemd 狀態代替結果。 |
| 完整回歸／發布閉包 | 首次固定回歸為 1 failed、12,955 passed、66 skipped，3,915.33 秒；僅 cooldown 測試的起算方式失敗。修正測試後，以 2,241 檔新固定樹重跑全套，另保存 20 項最慢測試耗時與 JUnit XML；原快照及失敗紀錄不改寫。 |

## 實際服務與恢復結果

### Crypto 與 OpenBB

原 `stockagent-registered-data-features.service` 在 08:17:12–08:54:55 執行完畢，
`Result=success`／exit 0。OKX 500、Binance 575 個商品的獨立歷史特徵報告均為
updated、errors 0；OKX 原五個 OI 失敗商品各 stage 均通過。Bybit 873 個當次
有效商品的 OHLCV 也完成更新，879 個儲存商品包含既有退場商品。
這是本次更新驗收，不能推論每個 provider 的每一段歷史皆完整。

盤中 downloader 持續改寫 `download_summary.json`，其最新內容已標明
`historical_features_enabled=false`。特徵證據使用獨立 CSV 的 SHA、mtime、
500／575 列與原服務 journal，沒有把盤中摘要冒充歷史特徵生成收據。
見 `registered-feature-service-acceptance.json`、`okx-current-run-acceptance.json`。

OpenBB 保留原容量 floor 與封存 cutoff，實際恢復採集。08:34 的唯讀證據包括
本次已更新 488 個 success、8 個 empty 任務，以及三個新 OECD 真實成功輸出
238／90／48 列。`monitor_latest.json` 當時仍是 08:05 的啟動時舊監控，不能
將其 stalled 訊號當作 08:34 任務狀態。全封存排程仍進行中，provider 的
永久不可取得／授權限制也未被這次容量恢復消除。見 `openbb-runtime-acceptance.json`。

### 台股衍生與冷發布

Canonical builders 產生 9,005,471 列、2,757 商品的 symbols 及
9,616,198 列、143 欄的 features。新的市場來源變動後，原 reconcile service
完成 market-only rebuild：沿用 9,609,323 個股票列，wall 50.646 秒。
最新 symbols／feature 的來源收據 audit 皆通過；這是既有增量 owner 的使用，
沒有宣稱為本次新增的演算法加速。features requested end 為 2026-10-02，
來源下載的官方截止資訊仍依各自收據解讀。

第一次實際冷發布已提交本機 head，隨後 scan 120 秒逾時，入口總耗時
793.377 秒；原失敗收據保留在 `tw-cold-first-scan-failure.json`。
修正版在 08:55:23–08:59:11 完成，elapsed 227.515 秒，exit 0；unique commit
receipt 與 stdout 一致，狀態為 `committed_scan_pending`。

既有 scan-retry service 在 08:59:11–08:59:13 確認該通知，pending 由 1 降至
0；再次觀測本機 head 與 commit 相符、來源鎖可取得。接收確認不是遠端
收齊證明，`peer_convergence=not_checked` 保留。沒有修改 runtime link 或
materialize training view。見 `tw-cold-publication-acceptance.json`。

`packed_datasets.json` 的 TW row 指定 `.locks/tw-public-refresh.lock` 與
`source_coordination_lock_scope=source_parent`，實際仍是
`/srv/stockagent-live/.locks/tw-public-refresh.lock`。原預設 repo scope 保留；
鎖路徑逃逸、錯誤 scope、錯 inode／FD 均拒收。source lease 內只做來源
audit／freeze／local commit，lease 關閉後才喚醒原 retry service。
本次沒有啟用尚未量測採納的 batch-object scan 選項。

### 中斷測試事故恢復

Canonical restore 從 TW 非部署用途冷封存解碼並驗證 1,451 份原檔、
2,536,379,377 bytes。恢復前再驗 process references，僅當來源仍吻合本輪
已觀測 SHA 才替換已知受影響檔案。16 份已觀測 before-images（含先前已
恢復 checkpoint）與兩份被格式轉換移除的 CSV 共 18 份恢復／確認；14 份
已識別事故輸出移入 evidence 的 quarantine，沒有刪除來源或模型。

恢復後 `inventory_after.different_or_missing=[]`，但冷基線在事故前一天建立，
不能證明 14 個額外路徑在事故發生前都不存在。保留
`.INCIDENT_RECOVERY_PARTIAL.json` 與 `complete_artifact_recovery=false`。
Canonical completion gate 正確拒絕這份 legacy tree：缺正式 fold completion
與新 ABI 等必要產物，沒有模型 promotion。見 `cold-restore.json`、
`incident-recovery-result.json`、`incident-recovery-completion-gate.json`。

### US 封存

US 熱來源已在 2026-10-02 正式退役，receipt 為 `cold-only`；本輪沒有復原
120 GB 熱來源。新 verifier 在 D 冷庫驗 encoded SHA、解碼後 original SHA／size、
allowlist／manifest metadata、packed objects 與 encoded stage 的精確一致。
大檔逐位元校驗仍由受監督工作執行中，驗完才取得 `us-cold-verification.json`。
原 2026-10-01 scan timeout 的失敗服務證據保留，且冷驗證不包含遠端交付。

依實際 stack 再測 gzip 8 MiB 讀取緩衝，以兩個既有冷檔案、ABBA 次序比較，
所有解碼 SHA／size 一致。兩組中位 wall 分別由 2.832→2.575 秒與
0.626→0.620 秒，read syscalls 約 178／214 降至 8。個別樣本改善約
10%／1%，尚未證明整份 verifier 的 wall／CPU 成本改善，因此本輪不更改
canonical verifier 或中斷已在執行的全量校驗。見
`cold-gzip-buffer-benchmark.json`、`cold-gzip-buffer-decision.json`。

## 固定版本與發布

完整回歸 worktree 為 `/root/stockAgent-readiness-regression-20261003`，
base HEAD `894e782dfde5e12d1f1c6a9d94393a8bc3383ba6`，2,241 個 source files 的
inventory SHA 為 `84ad72133d3f34ca6792c8f1e2f67fc7b31d02f74cc4660dff04e87995b2bb37`。
原 checkout 中其他 TEJ、FinMind、期貨與資料整理工作的後續變動不加入此
回歸的受驗宣稱。測試樹不含正式來源／產物。

最後發布使用 `/root/stockAgent-readiness-final-release-20261003`，保持同一
base inventory，只加入 `final-release-source-lineage.json` 的 7 個增量檔。
三個程式增量是 projection producer 與兩個 publication scripts，三個對應
測試檔及一份 packed catalog 亦列明。wheel 與 selected training YAML chain
已建置；code source SHA 為
`9714033544d1d24dd54a81215c667c3b2ad4279b78cd95486f190d68dcc2e841`。
可重現 wheel rebuild、隔離 target 安裝／imports、train help 與啟動 provenance
驗收成功。Packed catalog 綁定在完整固定 inventory 及台股 live acceptance，
不冒充 selected training config chain 裡的內容。

前一個接受的 package 與歷史 full-regression receipts 均保留，沒有改寫已受驗
工作樹。新收益 policy 只更新 reporting metrics contract 2，不改 loss、optimizer
trajectory、execution ABI，也沒有因此重跑或啟用正式 GPU fold／paper 策略。

首次完整回歸唯一失敗是
`test_named_limiters_share_host_global_schedule_and_cooldown`。原測試在
`defer()` 返回後才開始要求再等 30 ms，但 canonical limiter 的 cooldown 是
發布時的絕對 monotonic deadline；返回前的寫入／scheduler 時間可能已將
40 ms cooldown 消耗完。以受控 clock 驗證同 host 的兩個 limiter，正常情況
等待 40 ms，延遲返回 100 ms 的情況應立即放行；兩者皆精確驗 grant 時間。
未降低原配額或修改 cooldown 語意，也沒有把原 failed run 改標成功。

重跑樹為 `/root/stockAgent-readiness-verified-20261003`，只比最後發布樹多這一
份測試修正（相對最初 full snapshot 共 8 個列明增量檔）。已再次核對接受的
915 檔 release：code SHA、bundle、selected config chain 均相符，無需重建
內容相同的 wheel。見 `verified-source-lineage.json`、
`verified-tree-release-acceptance.json`。原驗收 27 份及增量證據的最新狀態
保存於 [本輪驗收總表](../artifacts/operations/architecture-readiness-repair-20261003/acceptance-current.json)。

本輪不把歷史封存當成可部署模型，也不改 paper 策略或宣告全部資料／實盤就緒。
尚待完整回歸及 US 冷校驗最後結果；遠端 delivery、所有 provider 全歷史完整度、
legacy artifact 的事故瞬間完整 before-state 仍各有自己的證據界線。

## 自動收尾

最後兩項校驗由原 systemd run 繼續執行。
`stockagent-readiness-finalize-20261003.service` 是本次的一次性依賴收尾，
沿用 systemd 與 canonical `stockagent-agent task update`，每 30 秒讀取
原 run／PID 身分，不另外啟動 collector、trainer 或封存工作。
程序在斷線後仍執行；本次開機結束後應依原 run 收據接手。

收尾前必須取得 exit 0、完整 pytest 終端摘要、零 failures／errors 的 JUnit、
2,241 檔固定來源位元組／mode／membership、相同的 915 檔已接受套件，
以及與 US 退役收據相同 snapshot 的 encoded／decoded／packed 證據。
驗收總表綁定每份證據 SHA，最後仍由原 task completion gate 要求所有 run
名稱的最新結果成功。中斷、失敗、證據不符或 task 被暫停，都不會誤標完成。
已接受的總表只在完全相同證據重驗時重用，不覆寫原驗收。

`finalizer-focused.log` 的七項測試驗證等待、失敗、中斷、scope 不符、暫停、
證據失敗與成功順序；測試收據只存在 TemporaryDirectory，未完成真實 task。
安裝版本與資源設定保存於 `finalizer-installation.json`。
最新進度及最終結果見
[自動收尾報告](../artifacts/operations/architecture-readiness-repair-20261003/finalization-status.md)
與 [即時狀態](../artifacts/operations/architecture-readiness-repair-20261003/finalization-status.json)。
