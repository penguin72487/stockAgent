# 2026-09-05 全專案第一性原理 Review

本次修復以「事實可追溯 → 決策時鐘正確 → 執行／帳戶守恆 → 成果可重現 → 服務可觀察」
為順序。不是把檔案存在、程序 active、HTTP 200 或測試通過當成資料／投資品質的充分證明。

本次在 `twRule`、起始 HEAD `4725d7addc61` 的乾淨工作樹上進行。未切換分支、合併、
commit、push 或更動遠端預設主幹；使用者明確要求保留 `twRule` 的開發狀態。

## 架構結果

完整責任導航見 [project_architecture.md](project_architecture.md)。
本次由現行 registry、設定與部署模板產生的清單包含：

- 9 個主要 Python package；其中 `strategies/` 只是保留 package，不是策略註冊中心。
- 16 個執行／訓練模式、100 份市場設定、29 個 packed dataset catalog 項目。
- 64 個 systemd 模板：36 個 service、28 個 timer。是否應安裝由節點角色決定。
- 策略身分由市場 config、execution mode、model selection、fold/checkpoint 共同決定。

本機分支為 `twRule`、`main`、`naive`、`AMHGraph` 與兩個 `recovery/*` 分支。
本機已知遠端另有 `daytrade`、`perf/time-block-causal-transformer-logutil`、
`vastai1T/twRule` 等 refs。相對本機 `origin/main` 快照，`twRule` 有 163 個獨有 commit，
`origin/main` 有 1 個獨有 commit；本輪未 fetch，因此這不是遠端即時狀態聲明。

## 已證實問題與修正

| 優先度 | 根因／影響 | 修正與驗證 |
|---|---|---|
| P1 | FRED 每日排程使用台灣日期，可能超過供應商可接受的 realtime 日期，整個每日工作失敗 | 由 API 預設 realtime 日期取得供應商邊界，固定本次 requested/effective/provider 日期；錯誤保留 canonical output。真實下載與 systemd 工作已成功 |
| P1 | 共用 HTTP 錯誤遮罩會把 `output_type=4` 的 `4` 當秘密，破壞 HTTP 400／日期診斷；編碼／跨預覽截斷邊界的憑證及原始 exception chain 仍有洩漏風險 | 已知公開 selector 與秘密分離；URL userinfo、原始／URL 編碼秘密在截斷前遮罩；HTTP／網路例外不帶未遮罩的 traceback chain。新增回歸測試 |
| P1 | FRED HTTP 200 或快取 hash 正確仍不足以證明內容完整；快取也可能錯配系列／視窗 | 驗證 observations schema、回傳 count 與 rows 一致、receipt status/series/window；66 份實際既存 raw payload 也通過新增驗證 |
| P1 | minute 維護只看舊 replay receipt 與最新 state，會漏掉中間已結算交易日，掩蓋 9/3 Multi-Basis 22 的整日缺口 | 納入 append-only closing events；先串流檢查各日期／模式的 09:01、13:30 endpoints；缺口持久記為 `waiting_accepted_endpoints`，不重算或補造策略曲線 |
| P1 | 期貨市場設定繼承已被冷儲存移出的本機 generated YAML，乾淨 checkout 無法載入 | 從冷庫獨立驗證 pack/member hash，確認原 YAML 與 tracked baseline 的有效設定一致，再改用 tracked base；測試禁止 repo 市場 YAML 繼承 node-local artifacts |
| P2 | explainability 缺少 `Sequence` import，存在執行時未定義名稱 | 補正 import；全域 execution-affecting Ruff 檢查通過 |
| P2 | 兩份舊 Transformer smoke 在 import/collection 時就執行模型，並吞掉例外；PreNorm encoder 還啟用無效 nested fastpath | 改成真正 pytest 測試，驗證形狀／有限值／遮罩；FT encoder 明確停用不適用的 nested fastpath |
| P2 | Discord 的上游 audioop deprecation 使 warnings-as-errors 測試無法 collection | 僅針對精確訊息與 `discord.player` 加例外，保留其他警告為錯誤 |
| P2 | NVFP4 optional import 吞掉 PyTorch JIT deprecation，已安裝後端被錯報不存在 | 只在 TE import 範圍排除該上游 deprecation；真正 import failure 保留根因，原生 NVFP4 forward/backward 與 zero fallback 實測通過 |
| P2 | 冷啟動測試工具的假 Discord receipt 缺少 scheduled markets；部分舊測試缺少現行 config 欄位／仍預期只在最後一次刷新 fold 報告 | 更新工具 receipt 與測試 fixture，保持正式同步 gate、mode artifact contract、每 fold 即時報告契約不變 |
| P2 | 舊整股測試預期沒有反映明示的 500 股交易單位 | 30 萬 × 0.5 ÷ 100 = 1,500 股，資金可負擔；修正測試期望與 holdings 值，不改帳務程式 |
| P2 | 舊 DuckDB 靜態測試誤把已明訂的離線 columnar compaction 當成訓練 backend | 離線 SQL import 延後至 compactor；AST 測試只允許這個既有離線角色，panel/runtime 仍禁止 DuckDB |
| P2 | 架構入口分散，容易把策略 config、分支、model、服務混為一談 | 新增責任導航與唯讀 architecture inventory；不另建模型、訓練、checkpoint 或資料發布 registry |

供應商日期依據為 [FRED observations API](https://fred.stlouisfed.org/docs/api/fred/series_observations.html)
的 realtime 預設值。TE 相容問題在本機 PyTorch 2.12 重現；上游棄用背景見
[PyTorch 2.12 release notes](https://pytorch.org/blog/pytorch-2-12-release-blog/)。

期貨 base 修正不改資訊時鐘、OPEN/CLOSE 研究代理、費用、遮罩、模型或 checkpoint ABI。
冷庫原 generated YAML SHA-256：
`e0fe4a79f0bba33e4c34cf882b92301d7ac8676e42c5bb705ef30875edc7a8ed`。
差異僅 experiment/output 名稱與空集合的等價表達，子 config 原已覆寫名稱／輸出位置。

## 效能與服務驗收

- 原缺口路徑重現耗時 150.40 秒、max RSS 1,438,708 KiB；修正後 fail-closed preflight
  4.00 秒、657,688 KiB。這是「避免明知不可能成功的後續 I/O」的實測，不代表完整重建加速 37 倍。
- FRED 真實刷新 66 個視窗、55 個 verified cache、11 個 fetched、34,220 筆；
  requested `2026-09-05` 固定至 provider `2026-09-04`。每日 service 完成為
  `inactive/dead`、`Result=success`、`ExecMainStatus=0`，符合 oneshot 語意。
- loopback 與公開 HTTPS 的 health、overview、revision、data summary、TAIFEX status
  共 10 次 GET 全部 200。兩邊 revision 均觀察到 `synchronized=true`、lag 0、四模式一致。
- 本次 GET 是單次功能／延遲觀測，不是負載測試或 p95/p99。未重新執行瀏覽器視覺 QA。
- Discord、paper engine、公開 gateway 均 active，未為 review 重啟長駐服務；
  因此不能宣稱它們已載入本次未提交修改。下載與維護 oneshot 已用本次修正版實跑。

## 已核准的 9/3 獨立歷史模擬

Multi-Basis 22 的即時 9/3 ledger 原本沒有交易／部位／曲線，本次不覆寫這個事實。
原留存訊號未套用完整 opening feature，盤後訊號不能冒充盤前資訊。

核准後沿用 canonical generator，以 9/2 完整 panel 加 9/3 官方 open 重算：
保留實際生成時間 `2026-09-05T09:29:36+08:00`，另記 historical effective timestamp。
只補抓 2301、3037 兩檔當日 KBar，各 266 個來源分鐘；共 2 次查詢、receipt 無來源缺口。
它們不是 270 筆造出的行情；策略曲線按實際部位／成交事件與來源估值維持 270 個時間點。

- 沿用官方 09:00 open 輸入／sizing → 來源可稽核的 09:01 分鐘價（VWAP，無 tick/VWAP 時取同根 K 棒 Close）→ 歷史 OHLCV 日內出場代理；只有整根 09:01 K 棒缺失才不成交。
- 2 筆模擬進場，共 10,000 股；收盤全部平倉，無價格替代／synthetic fallback。
- 09:01–13:30 恰好 270 點；268 個中間點全部通過歷史來源檢查，首尾接受值保持不變。
- 保留開盤點原有的 stale 標記，不為了美化覆蓋來源狀態。
- 淨損益與費用／部位獨立重算一致：TWD 351,448.52；期末 TWD 10,351,448.52。
  這是單日反事實模型重播，不是實際收益、可交易 Bid/Ask 或投資有效性的證據。
- standalone replay 不包含 live benchmark portfolio，沒有 promotion 到 live dashboard。
  `state/` 是最初缺憑證／缺價的失敗候選，`state_verified/` 才是已驗證結果。

## 測試與證據

嚴格 CUDA 環境預檢通過。第一輪完整 suite 為 4,047 passed、11 failed、1 skipped、1 xfailed，
耗時 885.28 秒；上述 11 個失敗已逐一重現、修復並做 focused regression。
修復後第二輪完整 suite：**4,169 passed、1 skipped、1 xfailed，417.51 秒**；
沒有非預期失敗，未跳過昂貴 CUDA compile/accounting 測試。後續加強的
100 份 config inheritance 隔離測試加 1 項 unit inventory 測試也全部通過（7.08 秒）；
前端 Node suite 為 **12 passed、0 failed**。Ruff execution-affecting 全域檢查、
shell syntax 與 `git diff --check` 通過。

兩次 full suite 使用的編譯快取與暖機狀態不同，不能把 885.28 → 417.51 秒當成
本輪模型／回測吞吐改善的證據。

本機證據根：`artifacts/project_review/20260905/`（Git ignored，跨機器不會隨 Git 自動傳送）。

| 證據 | 用途 |
|---|---|
| `architecture_final.json` | registry/config/Git refs/模板與本機 service 快照 |
| `pytest_baseline.log`、`pytest_review.log`、`pytest_final.log` | collection 基線、第一輪完整結果、修復後完整驗收 |
| `architecture_final_tests.log` | 100 份設定不得繼承 node-local artifact 的額外回歸 |
| `focused_initial.log`、`reproduced_fixes.log`、`last_two_fixes.log` | 根因重現與分組修復，歷史失敗保留不覆寫 |
| `minute_preflight_run_v2.log`、`minute_preflight_status_v2.json` | fail-closed 缺口／耗時 |
| `frontend_tests.log` | 前端 request coordinator、cache、time axis 共 12 項測試 |
| `http_final.json`、`data_attention.json` | 公開／本機 acceptance 與資料缺口快照 |
| `replay_20260903/replay_acceptance.json` | standalone 模擬成交、分鐘來源、端點／帳戶與 hash 驗證 |
| `replay_20260903/state_verified/` | 已驗證重播 state、fills、marks 與來源 receipts |

## 續審：下載狀態與資料完整性（22:17 台北時間）

使用者要求繼續後，針對 Yahoo / Binance 的報表與狀態契約繼續審查，
未將「繼續」解讀成更換主幹、購買額度、修改來源授權或啟用冷庫自動刪除。
本輪沿用資料品質檢查的粒度／分母／完整性驗證，以及既有 `artifact_io`，
沒有新增下載框架或改寫來源市場資料。

| 優先度 | 已重現的問題 | 修正 |
|---|---|---|
| P2 | Yahoo 局部修補後，`symbol_count` 是本輪 1 檔，但 rows/status_counts 卻累計 12,897 檔，分母不一致 | `symbol_count` 改為合併報表列數；另記 `run_symbol_count` 與 `manifest_symbol_count`，不把累積報表當全 universe 的價格覆蓋率 |
| P1 | Yahoo 讀取舊 CSV 時將 `0050` 推成整數 `50`，局部更新會把不在本輪的 ETF 狀態列漏掉 | code 欄明確使用字串；回歸驗證 `0050` 與 `2330` 均保留 |
| P1 | 舊報表讀取／schema 失敗被吞掉，局部更新可直接覆蓋整份舊報表 | 局部更新無法保留舊資料時 fail closed；只有覆蓋完整 manifest 的結果能替換損壞報表 |
| P2 | Yahoo 直接寫 CSV/JSON，讀者可能讀到截斷或尚未寫完的控制檔 | manifest、download/repair report、summary 改用共用原子發布；注入 replace 失敗確認原檔不變、暫存檔清除。每檔原子不等於跨檔交易 |
| P1 | Binance 月檔因 conflicting duplicate timestamp 被隔離後，下一輪修補只識別 invalid OHLCV，漏排每日來源替代 | 以既存隔離狀態為準，同時辨識兩類舊語意錯誤，接回原有每日修補規劃 |
| P1 | Binance 全庫仍有 failed／月檔隔離時，空批次或局部批次成功可能回報 dataset complete | durable failed／quarantine 未通過獨立驗證前維持 `state=partial`；沒有本輪新失敗仍可 `cycle_state=complete` |
| P2 | 監控把 `state=partial, cycle_state=complete` 說成最近批次未完成，混淆操作失敗與資料缺口 | 新程序明示「本輪維護完成；歷史資料仍有未驗證缺口」，不降低 degraded 或偽造進度分母 |

### 真實資料核對與限制

- 2026-09-05 14:17:26 UTC 的 Yahoo 累積 report 為 12,897 個唯一 code，manifest 為
  17,402 筆。7 個 failed（占 report 約 0.0543%）仍保留，不能據此稱整個 manifest
  只有 7 個缺口。49 個 `not_found` 也不是成功取得價格。
- 真實 report 的臨時副本經 canonical writer 重建後，分母為 12,897；
  29,988,216 rows 與 status_counts 完全不變。正式 CSV、summary、repair report、manifest
  的前後 SHA-256 一致；正式 summary 仍是舊版本，等待下一次正常更新寫入修正版。
- Binance 本輪 1,376 個物件皆完成；SQLite 累積為 328,356 complete、
  1,355 `quarantined_repair_required`、1 `quarantined_source_invalid`。
  1,355 是未完成獨立修補驗證的月檔數，不是缺失分鐘數，也不斷言日檔完全未補。
- 無效日檔為 `BTCUSDT_210326` 的 `2021-02-03` 官方 archive，既存 receipt
  記錄 1 筆 invalid OHLCV。本輪只讀取隔離證據，沒有重新下載後宣稱來源已改正。
- 前述新標籤由全新 Python 程序讀取真實摘要驗證。未重啟常駐服務，
  因此不宣稱既有公開服務已載入新碼；查核時 systemd failed list 為空。

可重跑 companion notebook：
`artifacts/project_review/20260905/continuation_01/source_audit.ipynb`。
相鄰 `source_audit.json` 保存查核時間、輸入 hash、有限失敗樣本、原／修正摘要與 SQLite 計數。
notebook 正式來源唯讀、網路請求 0 次，只有臨時副本使用 canonical writer；
使用 fintech Python 從 repo 或子目錄執行。上述 artifacts 為 Git ignored，跨機器不隨 Git 傳送。

回歸先重現 **9 failed / 3 passed**，修正後 Yahoo / Binance **76 passed**；
擴大至 dashboard、共用原語、限流、狀態及契約稽核為 **164 passed**。
全域 execution-affecting Ruff 檢查通過；完整 suite 結果另記
`artifacts/project_review/20260905/pytest_continuation_01.log`：
**4,180 passed、1 skipped、1 xfailed，481.40 秒**。

## 續審：公開服務安全、協定與快取（22:37 台北時間）

再次以「公開層只讀、快取不是資料權威、一次 HTTP 請求的邊界必須明確」檢查。
這一輪未改動任何交易／模型語意，也未放寬全域限流或加設固定併發上限。

| 優先度 | 根因／故障注入 | 修正 |
|---|---|---|
| P1 | 單筆 response 大於 cache byte cap 時，新 entry 永遠排除於淘汰之外，可突破 128 MiB 上限 | 超大回應僅傳給呼叫者，不常駐快取；entry 數量與 resident bytes 同時守住上限 |
| P1 | 建置失敗的不同 query key 永久留在 lock table；LRU 淘汰又可能刪掉已有等待者的 lock identity | 鎖改由建置者／等待者持有 strong reference，table 使用 weak reference；快取淘汰與鎖生命週期分離 |
| P1 | status 遞迴清理只防既知路徑／order ID，上游新增巢狀 `apiSecret`、`Authorization`、`account_id` 等可漏出 | 加入正規化 credential-key guard；測試保留 `revision_token` 且不修改原始輸入。這是防禦性 fault injection，未聲稱發現正式資料已洩漏 |
| P2 | gzip 以字串包含判斷，`gzip;q=0`／`notgzip` 仍被壓縮；ETag 不區分壓縮表示，identity／304 缺 Vary | 遵循 quality negotiation、獨立強 ETag、固定 gzip mtime、weak/list/star conditional matching，以及完整 Vary／304 行為 |
| P2 | 405 不消費本文卻保留 keep-alive；GET/HEAD 也沒有本文契約 | 不支援的本文／chunked input 回 400 並關閉連線；寫入方法維持 405 並關閉，避免本文被當成後續請求 |
| P2 | 無參數 API 靜默忽略 query；畸形 absolute URI 可在 telemetry parse 時直接斷線 | 未支援 query 明確回 400；畸形 request target 回 sanitized 400，仍有有界 telemetry |

HTTP 規則核對 [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#section-12.5.3)，
強 ETag 的表示差異見同文件 §8.8.3.3。未把網路／TLS 延遲當應用效能，也未宣稱投資績效。

- 第一批新增 fault injection：**11 failed**，修正後公開服務模組 **46 passed**。
  再納入 query／URI／request-body／LRU 等邊界，公開層及相關 builder 為 **107 passed**。
- 前端 JS syntax 全部通過；Node suite **12 passed**。JS/CSS 內容未更動，不需要提高 asset 版本。
- 隔離 localhost server：7 個頁面 + 17 個 API 全部 200；檢查的敏感 key／內部 path 為 0。
- 22:33 先部署公開層；22:37 納入最後的 GET/HEAD 本文防護，再單獨刷新公開層。
  兩次均只重啟 `stockagent-public-dashboards.service`。
  day-trade service MainPID `652944`、TAIFEX service MainPID `222`、Discord MainPID `746485`
  前後不變，三者 NRestarts 均為 0；沒有重啟交易引擎。
- 第一輪正式驗收：localhost／HTTPS 共 48 次頁面/API GET 全部 200、敏感掃描為 0；
  gzip 禁用／表示 ETag／304 另做兩側實測。Binance 的「本輪維護完成；歷史資料仍有未驗證缺口」
  已在正式 HTTPS 顯示，並非只修改未部署原始碼。
- revision 是 engine commit 與 Discord ack 的非同步比對。採樣曾遇到 lag 1，
  後續採樣回到 0；原始觀測完整保留，未改旗標來製造同步，也未宣稱持續零延遲。
- 22:37 最後部署後另做 48 次 localhost／HTTPS 頁面/API GET，全部成功；
  兩側 GET/HEAD 本文均為 400、POST 為 405，並關閉連線。
  最終兩側觀測均 `synchronized=true, revision_lag=0`；自最後啟動起的 journal
  沒有 traceback、fatal、watchdog、api_failure 或 background_refresh_failed。
- 22:39 的正式 JSON cache 常駐量為 20 entries、13,967,278 bytes，低於
  512 entries／134,217,728 bytes；這不是整個程序 RSS 或高併發吞吐上限的證明。
- 最新完整 suite：**4,210 passed、1 skipped、1 xfailed，488.92 秒**，
  見 `pytest_final_03.log`，涵蓋最後部署的程式版本。`pytest_final_02.log` 是為補入最後本文防護
  而主動中止的中間輪次，不是完成結果。未做新的瀏覽器視覺 QA 或高併發效能數字聲明。

證據均在 `artifacts/project_review/20260905/`：
`public_gateway_reproduced.log`、`public_gateway_focused_final.log`、
`public_gateway_frontend.log`、`public_gateway_canary.json`、
`public_gateway_deployment.json`、`public_gateway_deployment_final.json`、
`public_gateway_live_acceptance.json`、`public_gateway_final_acceptance.json`。
正式快取占用證據另見 `public_gateway_cache_acceptance.json`。

## 保留邊界與需要使用者決定的事

1. `twRule` 保留開發分支，不進主幹，已遵照執行。
2. `cold-artifact-maintenance` 是唯一未安裝的 optional service/timer pair。
   它會發布 completed artifacts，並在保留期及冷庫／peer 驗證後移除 source。
   未獲明確選擇前不啟用，不把「少一個 service」誤判成必須自動開刪除政策。
3. 首輪資料快照為 critical（非即時讀數）：293 個 active endpoints 中 195 complete、56 catching_up、42 unable；
   另有 13 個群組 unable，不可加進 endpoint 分母。16 個 credential gates 需處理。
   原因包含 Dune credits 不足、Alpaca 等缺憑證、來源歷史根本不存在／未授權、
   已登錄但未實作的來源、部分資料仍在補齊。沒有購買額度、變更授權、移除缺口或改資料契約。
4. full suite、靜態檢查與所選真實流程，不能證明所有外部來源完整、所有 GPU 規模都穩定、
   遠端 cold store 已收斂、每個長駐程序已載入本次 revision，或任何策略具投資績效。
   本次未作大規模重新訓練、跨機發布／刪除、OS／driver 調整。

本次遵循 shared-training reuse、衍生品重用與 Shioaji 資料規範：只修正責任入口，
不複製訓練框架、不更動市場語意，也不把歷史模擬說成實際成交。
