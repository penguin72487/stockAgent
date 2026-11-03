# 2026-10-30 開盤流程、網站與服務故障檢查

本輪於 2026-10-31 凌晨執行。交易日曆確認 10/31 為非交易日，網站目前應保留
10/30；下次交易日切換為 11/02 08:30。以下不是 10/31 真實開盤實測，也不是
六模式一秒完成的驗收報告。

## 結論與處置範圍

網站已修復三個確定故障：跨午夜訊號消失、隔日沖提交後再次改寫狀態造成一致性
失敗，以及 HTTP 304 被誤記為逾時。另修正盤後模型計算的標示，及歷史官方清冊／
異常行情快照的診斷讀取。公網與隔日沖服務已部署；Discord 與當沖執行器維持原
PID，未套用工作樹中的新執行器程式，亦未改寫帳本、送券商委託或補造成交。

10/30 延遲的主要可控障礙是開盤時仍發生約 128～148 秒的完整輸入準備，而非
單純排程晚醒。另有來源延遲、無授權來源、缺少 09:01 可追溯分鐘成交，以及盤後
維護未完成，均不能以網站正常載入宣告已解決。

## 盤前至首批訊號

- 08:15 預熱、約 08:16 完成；08:45 最後預備，約 08:46:18～31 六模式均記錄
  cache hit，當時 panel cache epoch 為 2。
- 官方清冊 08:55:04.581380 產生另一版本，正式分母為 **2,316 檔**。本輪診斷固定
  該版本指紋，沒有用凌晨的新清冊替代當時清冊。
- 09:00 排程實際醒來僅落後 3.776～12.138 ms，但接著發生大量本機資料讀取、
  輸入建置、成員驗證與模式等待。準時醒來不代表輸入已常駐。

| 模式 | 首批訊號完成 | 首批 artifact 發布 | 輸入預取階段 ms | 推論階段 ms | 可解析盤中修訂數 |
| --- | --- | --- | ---: | ---: | ---: |
| 多基底22 | 09:02:18.950078 | 09:02:19.200698 | 127,623.420 | 1,103.911 | 175 |
| Factorized | 09:03:16.437405 | 09:03:16.641282 | 143,288.310 | 44,102.005 | 202 |
| MGDA | 09:03:23.839672 | 09:03:24.031034 | 143,288.825 | 6,780.656 | 203 |
| V8 | 09:03:26.933047 | 09:03:27.166045 | 145,450.693 | 174.850 | 174 |
| Multi-Basis | 09:03:29.026380 | 09:03:29.179688 | 147,906.663 | 488.084 | 175 |
| 1億 | 09:03:31.499280 | 09:03:31.853238 | 145,548.609 | 187.251 | 176 |

以上推論是原始 telemetry 階段名稱，**不等同純 GPU 算術時間**。各階段可能重疊／
巢狀，不應全部相加。Factorized 首批輸入中，source read 16,325.400 ms、load
82,481.394 ms、materialize 21,868.413 ms、member verify 20,430.750 ms；最後
讀取報價快取階段約 79.285 ms。這支持先改善完整輸入重用，而非只縮短 HTTP timeout。

本輪固定讀取 telemetry 前綴 657,642,488 bytes，SHA-256
`d776fcc294d7e1e8c15853123bd5be8c4b8d41f7554547e6c00515ea36dfdc7c`。
可解析到 6 筆首批完成與 **1,105 筆盤中修訂**。原檔第 14,237 行無法解析；已保留
並列為完整性缺口，沒有忽略後宣稱全量完整，也沒有將逐版訊號壓成六個最終訊號。

目前證據尚不足以確認盤前暖機到 09:00 之間究竟是哪個 cache key／來源世代變動
觸發冷路徑。三個 panel key 群組剛好符合現有快取項目數，不能直接歸咎容量並加大
上限。需要留下來源 key、成員世代、實際常駐輸入與失效原因的原生證據。

## 價格何以未準時取得

原始第 11 秒驗收：真正開盤價 **1,311 / 2,316**；另有 130 檔當時仍新鮮的無首筆
證據，875 檔未知。1,441 是可進一步核對的模型參考價候選，不是 1,441 個真實開盤
成交，且該份驗收的前次真實成交價查驗仍未通過。無首筆證據有時效，過期不可永久
算作已解決。

| 來源 | 第一個有效開盤觀測距 09:00 | 本次失敗與限制 |
| --- | ---: | --- |
| 共用 Shioaji | 1,216.474 ms | 首批 2,316 列只有 1,181 列有效，其餘 1,135 為舊值／盤前；首批 SDK snapshot 約 1,494.398 ms。不能把回傳列數當有效開盤覆蓋。 |
| 官方 MIS | 3,597.061 ms | 首個完整工作約 20,812.263 ms，87 個 HTTP 嘗試、4 次失敗；錯誤在連線／TLS／回應標頭等待，沒有 HTTP 錯誤狀態可證明是封鎖。不是今日「29／29 全部失敗」。 |
| FinMind | 1,455.133 ms | 首工作只接納 91 列；1,800 列是其他交易日，425 列時間晚於觀測時點而未接納。不可放寬時間規則冒充即時價。 |
| Fugle 上市／上櫃 | 無有效價格 | 263／264 個工作均 HTTP 403，沒有行情成果；重試不能修復憑證／權限。 |

MIS 全日 4,274 個已結束工作中，3,825 complete、3 degraded、446 failed；這些是
工作數，不是失敗標的數。Shioaji、MIS 的來源覆蓋取自合流 journal 觀測，未必代表
每家 API 原始回應的所有列。不可由這份下界資料推論來源完全沒有其餘價格。

合流真正開盤價首次達 50% 約 2.616 秒，60% 約 57.939 秒；最後仍有缺口。
完整真實新價一秒內不是目前已證實的外部能力。允許的最近真實成交價 fallback
僅供模型參考，須保留日期、來源及無首筆證據，不能變成新價或模擬成交證據。

保留快照另有 00684R 的 `open_timestamp_ms` 位於 capture interval 之外：
快照寫 13:29:59，該欄為 13:30:00。本輪沒有倒填時間或移除原始列。嚴格來源驗收
仍拒絕；新診斷將此列標記 `invalid_snapshot_clock`，並獨立分析其餘 acquisition
journal，避免一筆壞時鐘讓全部 2,316 檔失去可觀測性。未知數使用 null，不能寫 0。

## 模擬執行與缺少分鐘資料

當沖原生六模式的已接受帳本交易日仍停在 10/29，不能把網站 10/30 的模型目標
宣稱為已成交持倉。因首批晚於 09:01，執行器要求可追溯的 09:01 分鐘成交資料。

當日補價狀態：待補聯集 1,047 檔、本機解析 0、遠端未實際查詢，歷史查詢政策延至
13:35；但進場重試在 13:20 結束。這個恢復窗口的先後衝突不能靠重送同一工作解決。

本輪唯讀重查六個原始訊號的非零模型目標聯集 2,064 檔，四個預設本機來源解析仍
為 0，耗時 4,827.276 ms。2,064 是模型目標，不是經資格／整張／價格檢查後的
1,047 筆補價需求。淺層分區清點顯示 `research_dataset` 最新日分區為 10/27，
沒有 10/30 日分區；其他三個預設 root 存在 minute chunks，但這次解析沒有取得
所需資料。這不等於全電腦不存在資料，尚須核對 canonical 下載收據、分塊日期範圍
與來源位置。未另開 Shioaji 連線，未以其他日期或未成交報價代替。

## 本輪實作與部署

| 修正 | 根因與行為 | 部署邊界 |
| --- | --- | --- |
| 共用 HTTP 304 結束處理 | 呼叫端正確不讀無 body 的 304，但 request lifetime 計時器未清除，4～6 秒後誤記 TimeoutError。收到 304 即結束量測並解除 deadline／listener；200 body 的真正逾時檢查保留。 | 公網共用前端已部署，相關訊號／持倉／事件模組一起受益。 |
| 公開訊號採交易日曆日期 | 原先跟著午夜 wall date，週末把週五有效訊號清空；改用 canonical display session。日曆未驗收時明確呈現，不能延用為已驗收。 | 公網已部署。 |
| 盤前狀態採同一顯示交易日 | 同步修正日期投影，但資料 age 仍用真實現在時間；沒有倒退時鐘讓舊預熱變新。 | 公網已部署。 |
| 隔日沖單次提交 | 原本先提交，再覆寫產品專屬 status，造成狀態 hash 與 commit 不一致；改由共用 persistence 在提交前完成產品投影，只寫一次。 | 隔日沖已部署並從公網讀回。當沖原生執行器未重啟。 |
| 盤後訊號標示 | 六個 latest 指標已被 17 點的 `panel_close` 計算更新；網站現明確顯示「盤後計算」，不冒稱開盤交易。 | 公網已部署；原始模型與指標不改寫。 |
| 歷史清冊與壞快照診斷 | 固定日期＋指紋讀取原始清冊、驗 raw receipt 與讀取一致性；壞時鐘不再阻斷獨立 journal 診斷。即時清冊驗收仍維持當日嚴格規則。 | 工作樹診斷工具；不是 Discord／交易執行器的新版本部署。 |

最後驗收時 public、overnight、day-trade、Discord 均 active。Discord Gateway
connected、core ready 不等於盤後資料完成：背景維護仍 degraded，1億及 Multi-Basis
兩模式有失敗歷史工作；Factorized 等待來源。六模式盤後 fast cache ready 也不能
取代正式來源、帳本或開盤驗收。

## 測試與瀏覽器證據

- 最後完整受影響 Python 集合 **676 passed，101.21 秒**；實際結果記錄在
  `final-python-tests.log`，不宣稱全 repository 測試。
- 六個 Node 測試檔最後共 125 passed，涵蓋 304 lifetime、訊號日曆、舊版保留、
  盤後標示、持倉與 rollover。包含必須等待 deadline 後確認沒有假逾時的回歸測試。
- 最終公網瀏覽器讀回兩頁：訊號、持倉、歷史沒有載入錯誤，頁面未重新導覽；
  六個盤後標示、2,719 個非零模型目標可見，**這不是 2,719 筆成交**。
- 1366×768、390×844、768×1024，兩頁均無橫向溢出；主 agent 已檢視筆電截圖。
  隔日沖目前設定為零啟用模式，忠實顯示等待，不是四模式成交驗收。
- 真實 304 公網讀取樣本約 176.7 ms，server 0.2 ms，超過原 deadline 後仍無假
  TimeoutError。這是已快取請求，不是完整開盤、來源冷重建或網際網路上限。
- 全年歷史瀏覽器樣本 383,457 分鐘點、9 系列；首次約 6.218 秒（API 約 5.102 秒），
  未刪分鐘換取速度。原始全歷史 benchmark 因今日無帳本成交列而未通過，保留失敗收據。
- 瀏覽器為 CPU-2D profile，不涵蓋原生 GPU／WebGL。完整 UI audit 仍有 Cloudflare
  注入 script 被 CSP 阻擋的 console 警告，以及桌面部分 20px 觸控目標，不宣稱全 UI
  audit 全數通過。本輪未放寬 CSP、允許 unsafe-inline 或移除安全檢查。

生成證據均保留於 `artifacts/operations/dashboard_recovery_20261031/`：
`all-mode-timing.json`、`local-0901-availability.json`、`local-minute-paths.json`、
`postfix-browser.json`、`final-browser.json`、最後 Python 測試日誌及各尺寸截圖。
完整標的來源診斷為
`artifacts/operations/opening_20261030_symbols_review_20261031.json`。

## 後續規劃：可解障礙與外部限制分開

1. **優先修本機障礙：**把 08:55 最終來源世代、六模式完整輸入、checkpoint/member
   身分及實際常駐狀態綁成同一份盤前證明；失效原因與重新建置成本必須逐段記錄。
   最後來源更新後重驗真正會在 09:00 用到的輸入，而非只相信 08:45 的 cache hit。
   把 82 秒 load、member verify 與完整 materialization 移出開盤關鍵路徑；先保留
   原生樣本，再對真正同版本流程測速，不靠提高項目上限或假熱快取宣稱改善。
2. **修 canonical 分鐘來源與恢復窗口：**核對下載 owner、日期與分塊收據；若盤中
   不允許歷史查詢，必須讓當時的原生分鐘 collector 提供資料，不能排 13:35 的工作
   卻在 13:20 永久停止。完整逐版訊號、成交量股／張、費率、日終沖銷與 270 個右標
   分鐘端點驗證通過後，才可提出／套用獨立回放候選。不可倒填來源時鐘。
3. **來源外部限制：**Fugle 403 先驗權限；FinMind 舊交易日與未來時間列保留拒絕；
   MIS 針對實際 TLS／標頭逾時留下 transport stage，保留晚回覆、合併成功結果且
   持續受控重試。未有第一筆成交時使用可追溯最近真實成交供模型參考，但不能保證
   尚未發布的新價或無成交標的一秒成交。
4. **分開訊號意圖：**目前 latest 會被盤後計算覆蓋，前端已標示但原生 producer
   尚須分離開盤／逐版盤中／盤後意圖；保留每版 identity、journal 與真實發布時間。
   不能將 17 點最新訊號當作 09:00 那一版重建。
5. **受控部署與真正驗收：**先完成 pinned Discord／執行器同版本驗收、帳本恢復
   gate、來源與容量證明，再部署。11/02 真實開盤須串起 source→六模式發布→
   模擬成交持久化→公網 DOM 的 clocks、覆蓋百分位及失敗清單；本輪夜間測試不
   替代下一交易日驗收，也沒有證據可保證每日一秒或零外部錯誤。

## 重開機後追查與實作：2026-10-31 11:58 台北

本節接續上述凌晨觀測，不能把不同 boot、不同程式來源或暖／冷樣本混為同一次驗收。
今天是週六；最新交易日為 10/30，下一交易日的真開盤仍未發生。

### 已修復並部署

1. **服務目錄身分，而非放寬安全檢查。** 服務資料已使用
   `service_data`，舊 `artifacts/cache` 為已登記相容路徑；Parquet projection、
   共用 cache admission、completed-forward 的 producer／cold-binding／restore
   原本部分仍比較字面路徑，產生 `untrusted_namespace` 或拒絕合法 cache root。
   全部沿用 `canonical_service_path` 對準同一 owner、lease 與 capacity namespace。
   任意／改向／下層 symlink 的拒絕規則、保留空間、原 catalog、冷存 pin、完整 payload
   與 receipt 不變。測試核對相容／實體路徑共用一次 admission、同一份 hot hit，
   以及 cold restore 後原始 payload／receipt bytes 完全一致；沒有遷移或刪除資料。
2. **來源與容量等待保留型別。** 正式歷史子程序以原有有界私人 pipe 傳遞
   `CapacityDeferred` 與 source-pending，而非只留下子程序非零結束碼。
   父程序保留 `allocation_admitted=false`，不能因 child 回報就宣稱容量已驗收。
   Discord 維護也保留真正的 deferred，不把未完成來源喚醒記為成功。
3. **盤前輸入觀測。** 保存真正計算過的 base key、cache epoch、常駐 keys、
   hit／tier 與 Factorized member 身分；新增最後五分鐘的 final-arm 階段。
   沒有擴大 cache 上限或忽略 fingerprint。08:55 後來源若再次更新仍須實測精確失效
   與重建；這不是「完整盤前輸入已全部重用」或一秒開盤的證明。
4. **單一模式歷史查詢。** 在完整 shard SHA、schema、筆數及來源世代驗證後，
   將 market filter 下推到窄欄 projection 的 materialization 前；保留先編排的原始
   row index，讓明細 rehydrate 不會對錯列。全範圍 row-count 與最後來源 fence
   沒有刪除，`source_rows_scanned` 仍報完整來源數，不把篩選後筆數冒充全來源。
5. **共用介面與驗收。** 桌面 details summary 原本只有 20px，改為最少 24px，
   平板／觸控仍沿用較大的目標。12 頁共享 CSS 版本同步更新至 v11。
   互動測試新增日期／模式的 selection 與 revision readback、訊號特徵展開、
   全部公開資料進度及六模式逐一切換，不再以發出請求或表格暫時清空判定完成。
   隔日沖零啟用模式只驗收已接受的空範圍，明確記為未測替代模式及成交。

### Discord 原生恢復證明

第一個換代嘗試因維護中而安全拒絕，保留 `followup-discord-apply.json`。
等原生 running count 歸零，才採用受控 code-admission-only 換代，沒有中斷維護工作。
最後使用的唯讀來源為：

- release `20261031T034400Z-a8fb742f7539`；receipt SHA
  `4321df2b2585c8601f969198e2491e3b0c95b9edf925716de4e828c319db1a5c`。
- code SHA `4c602423c128779a39a9aed60e0e419daa18bd8f4386afe50c6e334cace71a03`。
- Discord run `140066-1793418641307486585`；Gateway connected、core ready，
  exact engine ACK matched，revision lag 0。
- 當沖引擎仍為原 PID 366／run `552d89de930e4d5cb71e8193014190f9`；
  `engine_restarted=false`、`broker_orders=0`、`operator_ledger_writes=0`。
  保護的是原帳本前綴與 owner，不是假稱執行器 heartbeat／revision 不再改變。
- 11:51:25～11:52:05 六個模式皆建立新的 `postclose_cache_ready` 結果，
  `panel_date=2026-10-30 13:30:00`。原生 cache 工作 66.216 秒、6 attempted、
  0 failures；這是整個盤後工作，不是一秒開盤測速。
- 再跑正式維護後，11:52:56 原生 aggregate `ready`，cache worker 與 full worker
  皆 `complete`，六模式正式歷史工作皆 `ready`、失敗模式為空。
  啟動原生暖機仍為本 run 的 `pending`，沒有宣稱 GPU 常駐／下次開盤驗收通過。

Factorized 本次新收據於 11:41:40 寫入，decision session 11/02，
`verified_exact_prefix`，重用 440 列、重建 1 列，整段 234.182 秒。
這才是本次自然增量證明；先前讀到的舊 `verified` 收據不能替代它。
先前首次新世代仍重建 441 列，兩個結果及其原因都保留。

### 效能與瀏覽器：有改善，但沒有宣稱到極限

同一個獨立冷 process、1/1～10/30、1 億模式、非零目標、100 列、完整來源：

| 指標 | 修改前 | 修改後 |
| --- | ---: | ---: |
| query + 公開價格投影 + JSON | 6,362.862 ms | 5,068.105 ms |
| peak RSS | 2,845,864 KiB | 1,572,476 KiB |
| 窄欄來源 materialization | 2,052.067 ms | 1,165.957 ms |

單組樣本約快 20.3%、RSS 少 44.7%；沒有控制 OS cache／其他服務負載，不能稱為
ABBA 或硬體極限。來源完整數仍為 7,961,724，篩選後 701,491，首屏 100 列；
結果／summary／公開投影三份 SHA 均相同，沒有刪分鐘或改變金額與費率。
`cProfile` 另外指出歷史首屏的 feature-summary 讀取／JSON decode 約 2.39 秒；
這仍是後續可處理的成本，沒有把它算成已優化。

部署後用真實公網 Chromium CPU-2D profile 重測：

- 1366×768、390×844、768×1024，兩頁皆無頁面橫向溢出、小於 24px 的目標、
  失敗 API 或當前 scope 明細載入錯誤；沒有整頁重刷。
- 筆電實際切完六模式，訊號／持倉首屏皆為所選 market、日期及 revision，沒有混入
  其他模式。各模式查詢／畫面接受約 0.458～5.767 秒；這不是六模型開盤推論測速。
- 全年曲線保留 383,457 分鐘點、9 系列；展開 99 個訊號 feature，頁底讀回
  196 個公開來源，最後樣本完成 164／196。完成數是來源鏡像觀測，不是全部健康。
- 完整 UI audit **仍回傳非零**：Cloudflare 注入的 beacon 與 inline JSD 被
  `script-src 'self'` 阻擋。沒有使用 unsafe-inline 或關閉防護；Cloudflare 帳號
  RUM list 讀回空清單，不能假稱已移除邊緣注入。
  官方自動注入說明見 [Cloudflare Web Analytics](https://developers.cloudflare.com/web-analytics/get-started/)。

### 分鐘來源仍未恢復：保留三條不同根因

- **永豐：** 原 canonical backfill owner 兩個 worker 皆在登入／合約初始化收到
  HTTP 400 `Sign data is timeout.`，因此 KBar requests 為 0。
  將 receive window 試為 60,000 ms 仍於約 100～137 ms 被拒，已恢復預設 30,000；
  不是「等 30 秒網路逾時」。WSL／Windows clock probe 差約 0.56 ms，未證實是
  時鐘漂移。新增逐階段測速與失敗類別，未另開 SDK、force-refresh 或刪憑證。
- **FinLab：** 按本機缺漏清單一次登記 2,064 檔、10/30 精確分鐘分區，
  request `f2c6a6177b5c82f78147db7028c1ab08f36d9fa09a5c7611111e70f68460e582`。
  原 minute owner 的有限批次實測 `waiting_required_general_data`，實際派工 0，
  一般資料 actionable pending 937／required pending 935；不能稱來源無資料。
  未重開大量歷史或移除配額、來源範圍與 owner 檢查。
- **FinMind：** 原全市場 KBar frontier 停在 10/28，10/30 尚無既有任務。
  先為原 10/28 前置任務建立有限查詢
  `84e675baf2e1262bc3055a0ec78cd56ce5d274fa4d5b1c05dfb9ee9e781aee1d`；
  同一 canonical worker 的一個 request 預算實測在來源派工前回傳 `disk_guard`。
  准入實測 Windows physical free 55,644,966,912 bytes（約 51.8 GiB），
  當前要求 68,719,476,736 bytes（64 GiB），即 `physical_backing_reserve`。
  WSL virtual available 約 193.5 GB 不是實體可用空間，不能用來取代。

上述要求只有派工意圖，不代表價格已下載；沒有把原 10/30 訊號改成假成交，
也未套用任何新模擬帳本。必須先解除認證／排程／實體容量阻擋，再依每版訊號
核對 09:01～13:30 價格、股／張、量、費率及日終沖銷，才能建立獨立回放候選。

### 本次測試範圍及後續 gate

- 面板完整受影響集合 `followup-final-web-tests-v2.log`：**546 passed，82.60 秒**。
  共用 Node 六檔 **92 passed**。修正了舊 coalescing 測試仍攔截 legacy reader 的
  問題，現在阻擋實際 canonical projection，仍要求並行相同 query 只讀一次來源。
- 第一階段候選 frozen 原生測試 **741 passed**；最後兩個 forward owner 修正的
  最終 frozen 原生集合 **159 passed，23.78 秒**，含真實 cold restore byte parity。
  兩階段集合有重疊，不能相加成唯一測試總數。
- Workspace 額外 owner／maintenance 測試 **42 passed**，cold restore 檔 **23 passed**。
  最後交付前的受影響合約重測另為 **29 passed，5.57 秒**；集合有重疊，不另加總。
  這不等於全庫測試。workspace inference closure 測試仍有一項失敗，路徑為
  `signal_engine → backtest/simulator → TW futures initial reference → TEJ history →
  provider_priority → FinLab history → acquisition_policy → FinMind ETA →
  download_finmind_complement → finmind_news`；未覆寫其他工作中的依賴。
  部署的是已獨立驗證、不含新聞依賴的 frozen 候選，而非整棵 dirty workspace。
- 不正確 probe 呼叫及將 workspace 外層 controller 測試放入舊 frozen namespace
  所致的失敗均保留；後續以實際 controller／worker 分工重驗，沒有刪測試掩蓋。
- 剩餘：分鐘資料准入、盤前最後世代真正常駐輸入重用、來源變動後的 final-arm、
  原生暖機、逐版模擬帳本與分鐘曲線驗收，以及 11/02 真開盤 source→六模式→
  持久化→公網 DOM clocks。上述未驗收前，不宣稱全系統正常或每日一秒達標。

本節證據同存 `artifacts/operations/dashboard_recovery_20261031/`，主要為
`followup-discord-apply-v3.json`、`followup-native-forward-alias-tests-v3.log`、
`followup-mode-page-{baseline,optimized}.log`、`followup-ui-deployed-*/report-*.json`、
`followup-ui-six-modes/report-1366x768.json`、`followup-finlab-minute-*`、
`followup-finmind-minute-*`。原生服務日誌與來源收據仍是各 owner 的權威證據。

交付前再次讀回 Discord、公開 dashboard、當沖引擎皆為 active/running，PID 分別為
139954、114979、366；僅代表程序存活，資料／執行驗收仍依上述 gate。人工檢視此次
390×844 公網截圖，訊號特徵與來源進度區在手機寬度內；特徵表保留獨立捲動區，
沒有把表內可捲動當作全部欄位能同時看完，也沒有以截圖代替完整 UI audit。
