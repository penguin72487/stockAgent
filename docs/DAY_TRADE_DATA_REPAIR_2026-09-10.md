# 當沖跨日回補資料修復與逐層驗收

## 1. 執行進度

09/10 05:03 更新：已完成使用者授權的「零股按同時點一般盤價格模擬成交」，四模型已啟用 `assumed_odd_lot_at_regular_board_price_v1`，並於 **04:59:28 原子切換正式帳本**。本輪發現的 59 個分鐘來源缺口已補齊；135 日重播、145,800 個策略分鐘點及三條參考曲線均驗收通過。網頁與公開 gateway 的 259,470 個分鐘點完全一致，Discord 四模型預熱及帳本同步通過。部署後分組回歸合計 **760 項 Python 測試、9 項網頁測試通過**。企業行動週排程已啟用，下次 06:00。第 2–11 節保留過程與中間失敗證據，最終結果見第 12 節；既有 Discord 背景歷史維護仍有 3 筆失敗，未冒稱整套服務全綠。

本次範圍：四個既有當沖模型，2026-02-25 至最新官方完成交易日 2026-09-09，135 個交易日。保留模型與既有隔離訊號；補來源及跨日持倉會計，不以改模型、補 tick、線性插值或虛構成交解決缺項。

執行證據集中於 `artifacts/audits/day_trade_margin_data_repair_20260910/`。前一輪的第二個隔離 candidate 曾完成並驗收 02/25–07/22 的 100 個交易日、四模型共 108,000 個分鐘估值點；07/23 遇到 1459 減資換股，停止於持倉會計／零股政策 gate。這是第 2–6 節的歷史背景，不是最新重播進度；後續 v4/v6 已跑完全期，但未通過全部驗收。

第 2–6 節之前一輪檢查時間：2026-09-10 02:37 台北時間。來源補齊、paper 回補、冷儲存可重建與多節點同步分別驗收，沒有合併成一個綠燈。

## 2. 根因與修正

### 2.1 ETF 不是沒有配息資料，而是接錯分類

原下載器僅從 MOPS 公司股利表取得精確現金條款，把 ETF 分為 `avoid / noncompany_action`。新增既有下載器內的兩個官方來源：

- [證交所 ETF 收益分配](https://www.twse.com.tw/zh/ETFortune/dividendList)：年度 HTML 表格。
- [櫃買中心 ETF 收益分配](https://info.tpex.org.tw/ETF/zh/dividend-list.html)：頁面使用的 `POST https://info.tpex.org.tw/api/etfExDiv`。

沿用 host-global 速率限制、重試、原始 request/response 雜湊、immutable receipt manifest、原子 parquet／summary 與 catalog publish wrapper。新增單一 downloader lock，避免並行覆寫。

解析保留正式配息與預告的區別：沒有金額不是零。相同事件優先採正式金額與發放日；跨公告年度依明確公告年份處理更新；同年正式條款相互矛盾仍拒絕。兩交易所重複刊登但財務條款相同可去重。加入事件唯一鍵和 reference 列數守恆驗證。

目前 2014–2026 entitlement 表有 23,175 列，精確現金條款 18,682 筆，其中 ETF 4,436 筆。ETF 尚有 6 筆歷史事件未解決（本次 2026 期間為零）；公司現金事件仍有 760 筆缺精確條款。`coverage_complete=true` 只表示 reference 事件都有分類列，不表示每一列都可用於持倉會計；`avoid` 或未知金額不算資料補完。

實際找到 TPEx 的舊預告 `00764B / 民國190年6月16日`，沒有擅改為民國109年；保留原始證據及 issue，拒絕產生假現金權益。

### 2.2 最新清單不是歷史刪除通知

MOPS 最新 bulk response 缺少部分已公布歷史事件，例如 `5371 / 2026-06-17`；本機 08/19 的官方收據仍有精確條款。新增以 content-addressed 舊 manifest 驗證 request/response 後補缺流程，已救回 19 筆。新回應已存在的條款優先，舊收據不覆蓋新修訂。

### 2.3 現金權益、發放與成交是三件事

除息前保留的 signed 股數決定現金 claim：多頭為應收，假設融券空頭為補償應付。除息日先認列，再執行當日新目標減既有持倉；平倉不消滅已取得 claim。發放日只把應收／應付轉現金，不再增加一次損益。正的未付應收保留為不可再投入預算。

```text
NAV = 初始本金 + 累積已實現淨損益 + 未平倉淨清算損益
      - 跨日成本 + 累積已認列 signed 現金權益
```

Claim identity 綁定取得批次與除息日；重啟不重複領息。已認列金額被來源修訂時停止並要求對帳，不能靜默改帳。非現金／條款未知事件仍維持 fail-closed。

另外修復持倉退出模型名單時的漏價與費率 KeyError：退出名單代表目標歸零，不能讓持倉消失。

### 2.4 跨日曲線不可套每天平倉公式

既有 minute rebuilder 僅在 preserve-only 驗證模式接受 carry 歷史：保留原始 270 點，逐分鐘核對 NAV 含配息與利息；仍拒絕 flat-session 重算、terminal-only 偽補。保留跨日取得批次，對官方明確無成交的持倉只保留標示過期的估值，不下載或編造不存在的成交。

`scripts/audit_tw_day_trade_margin_replay.py` 是可重跑的只讀驗收入口：逐筆進出數量、已實現毛／淨損益、除息前股數、配息、跨日成本、每分鐘持倉數、NAV、來源收據、成交價 OHLC 範圍與共用 50% 分鐘容量；驗收期間檔案變動就拒絕結果。

驗收還會讀取綁定雜湊及收據的官方 TAIEX 交易日曆，核對完整日期集合；整天少跑不能因剩餘資料內部自洽而通過。`--completed-prefix` 只接受未被失敗日部分寫入污染的完整前綴，必定保留 `full_requested_range_passed=false` 並回傳 exit code 2，不是 promotion 成功收據。

### 2.5 停牌不能當作下載缺價，減資不能當一般除息

第一輪全期 replay 在 `2026-07-23 / tw_day_trade_100m / 1459` 停止。證交所普通除權息表不包含這類減資事件，融券公告分類又刻意排除了技術性換股停牌，以免誤判為下市／強制回補。不能直接放寬融券分類來修持倉會計。

新增 `downloader/download_tw_share_replacement_reference.py`，沿用既有全域限速、原始 request/response 收據、原子輸出與發布 wrapper：

- [證交所減資恢復買賣表](https://www.twse.com.tw/exchangeReport/TWTAUU?response=html)：本次 2026-01-01 至 09-09，共 5 筆。
- [證交所減資詳細資料 API](https://www.twse.com.tw/rwd/zh/reducation/TWTAVUDetail?STK_NO=1459&FILE_DATE=20260722&response=json)：停牌日、換股比率、現金退還、併案股利與認購。
- MOPS `ajax_t05st01` 歷史公司公告：依官方表單產生查詢，檢查公司及新股上市日期吻合，採停牌前最新對應公告的發放日。小型「查無資料」頁指定 UTF-8，避免自動編碼判斷誤認字元後造成假解析錯誤。

[1459 證交所原始公告](https://dsp.twse.com.tw/public/static/downloads/announcement/official/ABS-11500127891-1.pdf) 與 MOPS 06/24 更正公告交叉確認：7/23–7/31 停牌，8/3 恢復；1,000 舊股換 750 新股、每舊股退還 2.5 元、8/13 發放。此日的無行情不是能下載補出的分鐘成交。

此來源已列入 catalog writer gate。發現相關既有持倉時，engine 以 `corporate_action_accounting_not_quote_download` 清楚停止，保留原股數與成本，不產生假的出售或新股成交。股數／成本轉換與零股執行政策尚未啟用；沒有把已知條款等同為模擬已完成。此次 structured collector 的市場覆蓋僅 TWSE，TPEx 明確標示未覆蓋，不能稱全部台股企業行動已齊備。

目前是可重跑的 canonical collector，尚未接入固定來源事件排程；不能宣稱未來每日自動更新已驗收。待零股／減資持倉語義確定後，仍需擴大 TPEx 來源、補足缺項、接入來源維護並完成全期會計驗收，不可直接開啟正式 opt-in。

### 2.6 全期驗算抓到盤尾限價跳價錯誤

第一輪前 100 個完成交易日有 108,000 個策略分鐘點，帳面加總誤差最大約 `5.1e-10 TWD`，但 2,005 筆成交價格不在該分鐘 OHLC 範圍內，故 **驗收失敗**。主要原因為舊限價遇下一分鐘跳價，仍記在原限價，即使那一分鐘根本沒有該價位。帳面自洽不代表價格證據合格。

修正：已在前一分鐘生效的限價單，跳過限價開盤時，賣出用 `max(limit, bar_open)`、買回用 `min(limit, bar_open)`，保留價格範圍與容量檢查。新建／改價單不得回頭吃提交之前的 right-labelled KBar。止盈限價的跳價也採相同規則；live best Bid/Ask 分支未改。新增跨價、延後提交與來源範圍 regression，並重新跑 `candidate_cash_v2`，未覆蓋失敗的 v1 證據。

這仍是以 OHLCV 為基礎的 counterfactual 撮合；無法證明排隊順位及觸發後逐筆路徑。單分鐘先停損、50% 容量等近似仍須明示，不把它稱為交易所成交回報。

## 3. 冷儲存另有既存缺件，不能混稱同步成功

Syncthing 曾顯示 canonical folder idle、needBytes=0，但 `tw-public` 最新 head 引用的 117 個物件已被同步刪除；byte convergence 不等於 release 可重建。

新增 `scripts/repair_packed_source_objects.py`，沿用 catalog、manifest/inventory 驗證、既有 pack writer 與 immutable atomic installer，只在重建物件 SHA-256 完全相符時復原，不改 head、不刪來源、不拿新版覆蓋舊 digest。

Dry-run 後已復原 7 個物件、60,108,272 bytes。尚餘 110 個物件：部分 pack 的原始完整成員不在目前 selected inventory，部分 source 已變更。保留 unresolved 清單，不能聲稱舊 release 已修好或此次發布成功。來源救回與 packed 歷史復原必須分開驗收。

也修正 publish writer gate：辨認真正執行中的 Python script argv，避免已結束下載器仍被外層 shell 的 `-c` 字串誤判為 active writer。

### 3.1 原始版本救援與目前版本恢復分開

第二輪從 26 份保留 manifest 找到了所有 110 個物件的完整原始成員清單；但每個物件均有至少一份來源內容已變，不能重建相同雜湊。這些舊版本仍需備份／持有原始位元的節點救回，沒有以新資料冒充舊資料。

新增 catalog publisher 的明確手動修復選項：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/publish_data_releases.py publish tw-public \
  --sync-root /srv/stockagent-packed --recover-missing-base-objects
```

此選項不適用 `--all-ready`。仍必須通過原 catalog 發布授權、來源 writer、freshness 與日期不回退檢查；即使舊 payload 缺件，也保留已驗證 head metadata 的日期下限。缺件所涵蓋的目前來源檔案交給既有 content-addressed pack writer 重建，在更新本機 head **之前**，完整驗證新 manifest、inventory、全部 object SHA-256 與 ZIP CRC／成員。新內容使用新 digest；舊 manifest、inventory、其他節點 head、來源檔與損壞證據不刪除。這是建立可用的新 release，**不是**宣稱舊 release 已重建成功。

正常發布維持原本 fail-closed 預設，未讓背景程序自動忽略缺件。修復後仍需逐一驗證 Syncthing 預期 peer 的完整度，不能把本機發布成功當全網同步完成。

## 4. 邊界

這是 paper 模擬。全部可轉融資融券是使用者指定假設，不是券商核准或保證有庫存；既有利率仍為壓力假設。未模擬個人股利稅負／補充保費及完整券商 T+2 授信現金購買力。歷史 KBar 不是真實委託成交回報，也不保證 09:01 一定有成交量。

此修復不宣稱 2014 年起所有公司非現金企業行動都已具備完整條款，也不把有 `avoid` 分類的事件算成「條款已補齊」。只以本次四模型實際持倉與指定回補期間的驗收結果判斷可用性。

## 5. 驗證證據及尚未完成事項

- `regression_final_v3.log`：最終聚焦回歸 **354 tests passed in 8.59s**；包含現金權益、多空庫存差額、限價跳價、來源解析、官方日曆缺日、冷物件修復及原子發布失敗保護。修改範圍的 Ruff 與 `git diff --check` 通過。不是完整 repo 或明早 Gateway／09:00 壓力驗收。
- `completed_prefix_audit.json`：v1 前 100 日會計自洽，但有 2,005 筆價格範圍錯誤，`passed=false`。
- `completed_prefix_audit_v2_final.json`：v2 前 100 日通過來源及会計驗收，108,000 個分鐘點、7,127 份 source files，`error_count=0`，最大會計誤差 `3.637978807091713e-10 TWD`。`full_requested_range_passed=false`，下一個未完成日期為 07/23。先前 `completed_prefix_audit_v2.json` 是驗收工具尚未修正來源 method-prefix 解析的中間失敗紀錄，保留但不作最終結果。
- 108,000 是每分鐘估值點數，不代表每檔每分鐘都有成交；其中 66,266 筆包含沿用上一筆價格，保持來源過期標示。平均新鮮成交名目金額覆蓋比約 80.36%，最低 0%。沒有把缺乏新交易的分鐘插值成新行情。
- `cold_current_source_recovery.log`：新 release `tw-public-20260909T182026115256137Z-l0-penguin-6d5c00954836641c`，115,610 檔、9,527,936,257 logical bytes；93,413 reused，22,197 rebuilt，新增 87 個同步物件。所有被選用物件在更新 head 前通過完整驗證。其 catalog freshness receipt 日期為 09/08，不冒稱所有公開來源均已驗收到 09/09。
- `cold_final_verification.json`：後續正常發布的最新版 `tw-public-20260909T182425182387684Z-l0-penguin-b6c4e9d1ed5cdc99`，完整驗證 1,084 objects、10,797,202,698 object bytes；manifest SHA-256 `010fc6826560d1abc47bb75895c6010bed8b0aa5eb9eeb6a74f479f0e22f4b3c`。驗收結束 head 未改變；未進行 hot materialization，也未修復舊版剩餘 110 個遺失物件。
- `syncthing_final.json`（02:37）：本機 idle、need=0、errors=0；vastai1T 連線 QUIC、API 回報 100%／remoteState=valid；lab203 離線、API 回報約 52.02%、尚需 250,621,717,033 bytes（整個共享資料夾，不僅本次 TW 修復）。因此 **全體 peer 尚未同步完成**，也沒有驗證遠端 materialization 或磁碟持久性。
- `share_replacement_issuer_retry.log`：已取得 1414、1459、6176 的精確發放日；1563 在已查公告中未列發放日，2380 是彌補虧損、退還股款為零，不應虛構現金發放。

正式四模型歷史尚未 promote；未更換 checkpoint，未送券商訂單，未把零股或停牌部位抹除。依策略切換驗收規則，全期訊號、持倉會計、來源價格與 270 點分鐘曲線都通過後才能正式切換。

## 6. 可重跑驗收與下一個決策

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_day_trade_margin_replay.py \
  --state-dir artifacts/audits/day_trade_margin_data_repair_20260910/candidate_cash_v2 \
  --output artifacts/audits/day_trade_margin_data_repair_20260910/completed_prefix_audit_v2_final.json \
  --completed-prefix
```

此命令目前應回傳 **2**：完成前綴可用於診斷，全期間尚未完成。不要把 exit code 改為成功或放寬正式 promotion gate。

下一個需要使用者決定的執行假設：1459 的 1,000 股會變成 750 股。建議保留零股部位，取得可驗證零股行情後才能成交；不能擅自沿用一般盤分鐘價格假裝零股可成交，也不能把剩餘股數抹除。若選此規則，仍需實作股數／成本轉換、應收退款、零股保留、停牌估值與獨立帳本驗算，再跑 135 日完整候選歷史。lab203 則需重新連線；舊版本損失需真實原始備份，不能由目前不同內容反推原始位元。

## 7. 後續已授權的零股價格假設與換股會計

上一節待決策事項已由使用者回覆取代：零股可按一般盤同時點價格模擬買賣。此授權不代表可以在停牌日成交、忽略成交量、抹除零股，或把模擬稱為券商成交。

採用既有 paper engine，不另造帳本。模型的整張目標不變；換股後留下的零股可作為「目標減庫存」的減倉或補足差額。全部成交仍需來源價格與共用成交量容量；原始委託／成交檔不改寫。API 及面板明示額外假設。

若舊股數為 `q`、換股率 `r`、原取得成本每股 `b`、每舊股退款 `c`：

```text
新股數 = q × r
新庫存成本 = b / r
獨立 signed 退款權益 = q × c
新部位毛損益 + 退款 = q × r × 新價 - q × b + q × c
```

保留原始 `entry_price`；調整後成本另存 `inventory_basis_price`。退款不再同時扣減成本，以免 NAV 重複計算退款。這是本 paper 帳戶的經濟損益記帳方式，不是申報所得稅或券商成本證明。退還本金不會自動拿去償還假設融資，既有利率／融資比例保持不變。

停牌期間保留舊股數與過期估值，不產生交易；恢復日依官方條款換股，建立唯一 `action_id` 與退款 claim，發放日僅轉現金、不重複增加權益。所有受影響部位先驗證再一次提交，任何缺條款不留下半套轉換。小於一股的畸零股份仍要求公司精確處置條款，不把「零股」授權擴張成任意小數股。

驗收新增多空對稱、停牌不可成交、原成交價不變、換股與退款重啟冪等、零股差額追加、股數與原投資本金守恆。正式 promotion 新增明確 `--allow-margin-carry`，必須重新通過全期間來源／會計獨立驗算；舊正式帳戶仍必須平倉，並核對四模型 runtime opt-in。不能以此選項跳過來源、270 點、模式集合或原子交換要求。

`candidate_oddlot_v3` 是早期診斷候選，確認原成交價應與調整成本分欄後主動中止。v4 是 TWSE-only 全期診斷候選；v5 雖加入 TPEx，仍含第 8.3 節估值缺陷，已中止。v6 因第 8.5 節零量問題未通過驗收；下一個完整候選是 v7，不混用中間版本。

## 8. 當日來源與跨日維護接線

### 8.1 模型資料與持倉會計有不同的日期需求

模型盤前資料維持 T-1 已完成交易日，實體跨日持倉卻必須知道 T 日企業行動。新增 `scripts/refresh_tw_day_trade_margin_actions.py`，沿用三個 canonical collector，在 catalog-resolved producer 的 `execution_actions` 子目錄建立 T 日資料，不把當日收盤／尚未發生價格加進模型特徵。

首次初始化遵守原 reference 的 2000 年完整基線規則，不能把僅下載 2026 稱為 baseline。後續只更新必要年份。消失的歷史 MOPS 欄位從 canonical raw manifest 驗證並攜帶原始 request/response bytes 補回，新資料不被舊回應覆蓋。writer 仍通過 catalog publish wrapper。

2026-09-10 03:02 的 `execution_actions/readiness.json` 顯示三個下載器均 exit 0、`source_ready`。reference 34,463 列；本次 2026 entitlement 有 2,106 筆精確現金條款；上市、上櫃共 11 筆減資事件。3152、1563 的退款發放日期仍未知，不影響未持有該事件的模型；若實際持有就停止該帳戶會計並保留原始證據。此處 `source_ready` 不等於所有公司的所有條款均已完整。

TPEx 來源為[櫃買減資恢復買賣資料](https://www.tpex.org.tw/zh-tw/announce/market/reduction/reference.html)，頁面使用 `/www/zh-tw/bulletin/revivt`。檢查 ROC 日期、公司身份、停止與恢復日期、換股率、退還股款、認購條款；不以行情缺口推測換股。每筆 raw manifest 及其引用的回應內容、request hash 均驗證。

排程模板採每週 Mon..Fri 06:00、07:30、08:00、18:00，背景低優先序、失敗有界重試，當日準備不放入 09:00 推論熱路徑。正式服務啟用狀態須以後續實測為準。

### 8.2 不讓跨日帳戶套回每日清空的修補器

新增既有 minute rebuilder 的 `--revalue-carried-marks`：依不可變 entry/exit fill 與 share-action ledger 還原持倉、剩餘手續費、獨立成本、已實現損益、現金權益及利息；只用來源分鐘成交價估值，不重做策略決策與成交。保留已驗收的 09:01／13:30 端點金額，並核對端點帳務；缺失或未有來源的內部分鐘才修補。測試涵蓋隔天無新成交時沿用舊報價、750 股平倉、退款未付應收、利息與輸入不被修改。

發布前重核 fills／orders／marks／benchmark 版本與持倉會計指紋；分鐘檔案寫入及替換使用共同短鎖，避免新 mark 遺失。最後發布時再次保護即時 engine 的盤前盤中時間窗。守護程式另區分「實際是否平倉」及「是否符合授權跨日會計」：合法假設融資持倉不再一律當成盤後故障，但未轉換、數量不符、來源阻擋及估值缺失仍失敗。

### 8.3 帳面守恆通過，仍須另一套分鐘估值交叉驗算

`full_oddlot_v4_audit.json`：135 日、145,800 點、14,714 個來源檔案，成交數量／OHLC 範圍／容量／配息／利息帳務檢查零錯誤，最大浮點帳務差 `6.69e-10 TWD`。但 `minute_revalue_v4_diagnostic` 從相同不可變成交重新估值，發現盤尾部分分鐘仍有最高約 62,717.87 元差異。此 v4 不合格，未 promotion。

根因是 `_eod_kbar_quotes` 的 bid/ask 在 13:21–13:24 使用該分鐘 high/low 判斷「先前限價是否被碰到」；`_mark_mode` 卻把這組撮合代理價格當作分鐘末部位價。修正 `_bar_quote` 明確提供獨立 `valuation_price_0901 = bar.close` 與 historical valuation tag，成交仍使用原撮合條件；估值只使用該已完成分鐘 Close。

另一處修正：沒有新行情時可延用上次「價格」，不能直接延用上次「損益」。部分平倉、換股或轉融資融券會改變股數、未分攤手續費與費率；每次 stale mark 都以當前庫存及成本重新計算，但保留 stale 標示，不製造新報價。

新分鐘維護的 carry 路徑逐檔核對 source chunk 與 canonical collector receipt 的路徑、雜湊及回傳日期；未驗證的 research/tick cache 不當作替代證據，零成交量 KBar 不算 fresh trade。一般盤零股價格假設不包含放大成交量。

Promotion 另外強制要求 `--require-revaluation-parity` 的完整獨立估值收據：原重播與獨立重建的 NAV 差異不得超過 `1e-6 TWD`，成交檔 SHA-256 必須不變，不能僅以會計加總自洽通過。來源分鐘檔案與收據在發布前重新核對 stat identity；期間被修改或移除就拒絕發布。

### 8.4 「恢復交易日」查詢不可截掉已開始的停牌

9/10 另以官方 endpoint 實查 9/10–12/31：TWSE 5 筆、TPEx 4 筆已公告的後續恢復交易，其中 6129 已於 9/3 停牌、預定 9/14 恢復。只查到當日的恢復清單會漏掉這個正在發生的停牌。原 v4 持倉檢查沒有持有上述 9 檔，但不能因此保留下載器漏洞。

下載器改查至 as-of + 90 日的「已公告恢復交易日」，實際資料覆蓋仍記錄 as-of，明示未取得未來市場行情。當日來源 gate 同時要求上市／上櫃與完整持有期間，過期、單一市場、未查後續公告均拒絕。此為公告查詢界線修正，不聲稱未公告事件已知。

03:38:28 `execution_actions/readiness.json` 已更新為 `source_ready`，三個 collector 均 exit 0；目前共 20 筆公告，share reference SHA-256 `dfc0a842e61ebeed4dffec9e6aa03fc22ddedb74fef81671f4c9ce8753bb7bc2`。同日停牌清單含 2321、3356、3591、3710、6129、8059、8277。v6 四模型既有部位之 9/10 08:30 只讀 gate 試跑通過（72/0/0/2 個保留部位），沒有新增待解條款；沒有修改正式狀態。

### 8.5 零成交量 K 棒不等於新成交價

v6 全期獨立估值驗算拒絕發布：145,800 個分鐘點中 842 個不同，最大差 `1,445.2367500066757 TWD`。原始成交及候選曲線保留，沒有提高容許誤差。

實查 9/7 的 8342：前一收盤持有 1,000 股、價格 88；來源 09:01 填入 87，但成交量為零。7782 的 3,000 股也由 26.55 填入 26.4、成交量為零。兩筆差額乘既有淨出售費率後為 `996.715 + 448.52175 = 1,445.23675`，精確對上最大 NAV 差。無成交時可沿用先前已驗證的價，但不能把來源填補的另一個價格冒充新觀測。

修正 canonical intraday bar loader：零／非有限成交量不進入交易與估值路徑，記錄 `ignored_zero_volume_rows`；因此不更新持倉價格，也不能觸發停損。09:01 source book 在沒有正成交量時不提供新估值，仍無成交容量。由於這可能改變後續停損狀態及委託股數，重新跑全期 v7，而非僅改圖表。即時真實 Bid/Ask 估值路徑未套用這個歷史 K 棒過濾規則。

### 8.6 配息數學自洽仍需獨立核對官方金額

全期驗收增加逐筆 `cash_per_share` 與 `payment_date` 對官方 receipt-backed entitlement 的獨立比對；不只檢查「股數 × 帳內金額 = 帳內現金」。一般企業行動、精確現金權益及換股來源均須覆蓋完整回補期間與兩市場，六份 parquet/summary 的 SHA-256 在驗算前後及 promotion 前重新核對。v6 診斷帳的 65 筆一般現金 claim 均與官方來源相符；正式 v7 仍需全期重新通過。

400 項核心回歸與 207 項 Discord/網頁測試通過；上述來源驗收另加 2 項測試，相關 23 項測試通過。最終部署驗收另列於後續紀錄，不以測試通過冒充正式服務已切換。

## 9. v7 完整候選與正式部署驗收

04:04 v7 完成 2026-02-25–09-09，共 135 日、四模型。這一輪不接受 548,395 筆零成交量填補 K 棒為新行情。1459 於 8/3 由 1,000 舊股轉為 750 新股；13:24 市價委託於 right-labelled 13:25 以來源價格 12.75 元模擬退出，退款另列現金 claim。

| 模型 | 9/9 收盤候選權益（元） | 保留部位批次 |
|---|---:|---:|
| 一億 | 108,434,350.67 | 82 |
| 多基底 1,000 萬 | 13,372,642.89 | 0 |
| 多基底 22 | 15,798,402.53 | 0 |
| Projection-L1 LayerNorm v12（既有 GELU market ID） | 16,247,745.35 | 2 |

相對未通過驗收的 v6，權益分別改變 +5,384.16、-500.14、0、-81.89 元。零量 K 棒會影響委託狀態及之後成交，因此不能只替換估值曲線而保留原交易決策。

`next_session_action_gate_v7.json` 對 9/10 08:30 只讀試跑通過；82/0/0/2 個保留部位均無缺失條款，沒有持有當日七檔已公告停牌股票。沒有修改候選或正式帳本；空帳戶不偽造當日持倉來源收據。

第一輪 v7 獨立估值已通過：145,800 點，最大差 `1.4901161193847656e-08 TWD`，超過 `1e-6` 的差異為零，原成交 SHA-256 不變。48,818 個所需股票／日期組合均有来源，無 missing pair。完整重跑測試為 609 項通過。

### 9.1 參考曲線不能只看外層日期

獨立檢查發現沿用的 `benchmark_history.json` 只有 132 日，缺 9/7–9/9；策略曲線通過不代表參考曲線通過。把既有維護器的 `_validate_benchmarks` 移到 promotion 共用，不另建平行規則；授權 carry 發布要求所有完成交易日、0050/2330 各 271 點（09:00–13:30），台指近月各 300 點（08:45–13:44）。回歸測試確認帳務與策略分鐘通過時，缺 benchmark 仍拒絕發布。

`benchmark_repair_v7.log`：canonical benchmark rebuilder 以保留官方日線、企業行動收據、TX Tick/實時行情收據重建至 9/9，40,500 個台指分鐘、540 個股票日內端點，沒有新增 Shioaji 請求。再交既有 minute rebuilder 展開股票分鐘，重新執行完整獨立策略估值，輸出 `minute_oddlot_v7_complete`。

### 9.2 授權保留部位與開盤無報價不是假平倉或補成交

第一輪 promotion 檢查仍假設每天 `settled_official_close`，因此拒絕全部 135 日的 `assumed_margin_inventory_carried`。現在只有明確 `--allow-margin-carry` 且完整獨立會計 audit 通過時，才接受這個不同的盤後結帳狀態；未授權及未解決交割義務仍拒絕。

另有 28,891 筆零成交 entry 紀錄沒有 09:01 價格。例：0056 / 2/25 的 receipt-backed KBar 最早列在 09:03，不能把它往前填成 09:01。新增逐來源檢查：完整當日回傳收據及 parquet 雜湊成立、各保留來源 09:01 沒有非零或不明成交量，才接受「沒有執行證據，所以未成交」；缺當日來源、遺漏已有行情、非有限量或任何偽造成交仍失敗。這不聲稱交易所當時絕對沒有成交，只聲稱保留來源不提供該時點可用執行證據。來源身分在驗收及實際切換前重核，實際全期檢查結果待後續收據確認。

待完成：參考曲線補齊後的分鐘驗收、所有未成交來源檢查、最終發布驗收、正式原子切換、四模型設定及服務/API 驗收。

## 10. 現行合約名冊不等於歷史行情可取得範圍

最終逐來源檢查的 28,737 組未成交股票／日期中，有 44 組缺當日來源：00883B 34 日、4987 4 日、6806 6 日。完整清單留存 `missing_0901_sources_v7.json`。官方日線確認部分缺口日期確實有成交，不能記成整天零量。

三檔都已離開現行合約表。原下載器在 `stock_contract_not_found` 直接返回，沒有送出歷史行情查詢；只讀實測使用相同原始代碼、原交易所的 `BaseContract`，三檔均能回傳歷史 KBar。`retired_contract_probe.json` 保留此結果。00883B 的下櫃日為 2026-05-07，最後交易日為 5/6，參見[發行人終止上櫃通知](https://www.ctbcbank.com/content/dam/twrbo/pdf/investment/20260415_ETF.pdf)。這不是使用另一個商品或現行合約替代歷史。

在既有分鐘下載器新增 `--historical-stock-unit SYMBOL=SHARES`：只對選定公開股票／ETF、明確交易所、已保留公開歷史及人工核實交易單位啟用歷史識別；現行合約查詢、交易資格和即時下單完全不受影響。不預設所有商品都是千股，因境外與加掛商品有例外，參見[證交所交易單位說明](https://www.twse.com.tw/zh/about/company/guide.html)及[櫃買交易制度](https://www.tpex.org.tw/zh-tw/mainboard/trading/rules/system.html)。本次三檔使用已核實的每張 1,000 股／受益權單位，另以每根 KBar 的 Amount、Volume、High、Low 交叉檢驗尺度，任何不一致拒絕落地收據。

仍沿用原本的速率／流量上限、交易時段保護、單日補查、原子 parquet 與 SHA-256 收據。資料落在原本可寫的維護來源 `artifacts/data_repair/tw_day_trade_minute_curve/maintenance/current/fetched_kbars`，不是 packed/materialized tree。收據另記錄歷史識別方式與公開參考檔雜湊。

實測 `missing_0901_source_repaired.log`：三檔 complete、0 source gaps、9 次請求、1 request/s，共 6,967 根分鐘資料；44 個原缺口全部取得當日來源，其中 15 組有 09:01 正成交量，見 `recovered_0901_sources_v7.json`。其餘只有較晚分鐘證據，禁止倒填 09:01。v8 使用補齊來源完整重播，再做獨立分鐘估值和發布驗收；v7 保留為未發布的修復前比較證據。

### 10.1 擴大至未達一張的模型目標仍檢查來源

另外檢查 79,440 組「有非零模型權重、資格通過、沒有 09:01 價格」的股票／日期，發現 3454 尚缺 15 日。其在四模型的 60 筆歷史訊號中，整張目標、要求成交、实际成交均為零（`recovered_3454_no_execution_impact.json`），故不改變 v8 已完成的資金路徑；但仍補資料。

這檔原代碼 KBar（區間與單日）皆回覆 Data not found，逐筆成交 API 卻能回傳。新增既有下載器的 `--fallback-missing-kbars-to-ticks` 選項，僅在 KBar 明確不存在時啟用逐日備援。原始 Tick 落地並保留雜湊，再共用同一個分鐘聚合器；09:00:00–09:00:59 標為 09:01，13:30 撮合保留在 13:30，不填空白分鐘、不回推成交、不插值。成交額為逐筆 `price × volume × 已核實交易單位` 加總，並以官方當日成交股數作獨立容量上界，收據明確標示 `observed_ticks_aggregated_to_right_labelled_1m`，不冒稱原生 KBar。

時間單位測試攔下 Polars 微秒／奈秒轉型差異，修正後才通過驗收並落地正式來源收據。`missing_3454_ticks_repaired_v2.log`：1 次 KBar + 15 次 Tick 查詢，15 日／858 根分鐘，0 source gaps；15 個原始 Tick 檔均核對雜湊。這 15 日補齊與前述 44 日為不同範圍，不把「無目標股數」當成「無來源問題」。

## 11. v8 最終重播與發布前驗收

2026-09-10 04:44：135 個交易日／四模型完成，`v8_replay_summary.json` 記錄結果。正式帳本尚未切換。

| 模型 | 9/9 收盤權益（TWD） | 保留部位筆數 |
|---|---:|---:|
| 一億元 | 108,431,350.81 | 82 |
| 多基底 | 13,372,642.89 | 0 |
| 多基底 22 | 15,798,402.53 | 0 |
| Projection-L1 GELU | 16,247,745.35 | 2 |

一億元模型相較 v7 改變 -2,999.85624805 元，其餘不變。1459 在 8/3 完成 1,000→750 股換股，現金退還獨立入帳，750 股以一般盤 13:25 分鐘的 12.75 元模擬執行 13:24 強制市價退出；換股並非成交、750 股亦未四捨五入為一張。

`next_session_action_gate_v8.json`：在記憶體內模擬 9/10 08:30 企業行動守門，四模型 ready、持有已公告停牌股數為零，候選 state 雜湊不變。這是盤前會計條款檢查，不代表已取得 9/10 開盤行情。

`regression_all_final_oddlot_v8.log`：707 passed；網頁 Node 測試 9 passed。後續強化的來源重核與時間網格測試也通過。維護器與發布器共用精確分鐘格點驗證；只檢查筆數與首尾時間不足以證明中間每分鐘完整。已驗證的成交來源、原始 Tick、企業行動、帳本在發布前再次檢查身分，變動即拒絕切換。

`predeployment_runtime.json`（04:33，切換前）：原正式帳本四模型皆空倉，Discord Gateway connected、core_health=ready；本機與公開 gateway status HTTP 200、revision lag=0、ledger divergence=0。此為舊部署健康證據，不能代替新部署验收。

## 12. 最終驗收、正式切換與後續維護

### 12.1 全期來源與分鐘會計

- `minute_oddlot_v8.log`／`minute_oddlot_v8/minute_curve_receipt.json`：04:48 完成獨立估值，135 日 × 4 模型 × 270 點 = **145,800**。48,837 個所需股票／日期來源全部可用；與原 replay 的最大差為 `1.4901161193847656e-08 TWD`，超過 `1e-6 TWD` 的差異為零。原成交 SHA-256 `a7cdd5a49515bd6b74d2c43a40dd04907f95f2d8cfa84edf06f5c573fbd8264a` 不變。
- 正式 `artifacts/live/tw_day_trade_simulation/promotion_receipt.json`：完整 02/25–09/09、135 日來源／現金／成本／持倉會計通過，`error_count=0`，14,759 份來源檔案核對，最大帳面守恆差 `7.566995918750763e-10 TWD`。28,718 組沒有 09:01 執行價格的目標均另有完整當日來源與零成交驗證，不把後來的價格提前使用。
- 0050、2330 各 36,585 點（每交易日 09:00–13:30，271 點）；台指近月 40,500 點（08:45–13:44，300 點）。除筆數之外，逐日精確分鐘時間集合也通過。
- 沒有新成交的分鐘保留上一筆可觀測價格並標示 stale，沒有插值或編造可成交量。保留股數、原始取得成本、調整後庫存成本、配息／減資退款及利息各自的會計責任。

### 12.2 原子部署與可回復性

沿用策略切換技能要求的隔離重播、全期驗收、短暫排空 writer、同檔案系統原子交換及服務/API 驗收；沒有直接蓋掉舊帳本。

`deployment_operation_v8.json`：04:55:23 起重核，原服務繼續運行；04:59:23 驗收完成後才暫停 writer；04:59:28 完成交換，04:59:37 引擎、Discord、公開 gateway 與既有維護 timer 已恢復。

- 正式資料：`artifacts/live/tw_day_trade_simulation`。
- **舊帳本回復位置**：`artifacts/audits/day_trade_margin_data_repair_20260910/candidate_oddlot_v8`。交換後這個名字已代表舊資料，不能再把它當成新版候選、繼續重播或清除。
- 切換前四份設定原文：`predeployment_config_text.json`；原 checkpoint/config 身分：`predeployment_configs.json`。四模型 fold 11、checkpoint 指紋與初始本金皆未更換。
- 正式設定只新增已授權的 residual margin、一般盤零股價格政策及 catalog-resolved `execution_actions` 來源目錄。回復須協調服務、設定與帳本，不能只拷回其中一部分。

### 12.3 服務與實際 API 驗收

`postdeployment_runtime_acceptance.json`、`postdeployment_minute_api_acceptance.json`、`postdeployment_service_acceptance.json`：

- 引擎新 run ID `e87e8c1e7356405c895c8e89507695cc`；Discord 新 run ID `268958-1788987576456337596`。Gateway connected、22 個 global app commands 同步成功；四模型 startup warmup 於 05:00:50 完成，耗時 72.738 秒，checkpoint 指紋與切換前一致。
- 四模型公開訊號明細可讀、日期涵蓋 02/25–09/09。本機 8766 與公開 gateway 8770 均 HTTP 200；帳本完整性 ready、divergence=0、Discord revision lag=0。
- `resolution=1m` 實際回應 **259,470 點**，沒有降採樣。兩端完整 JSON 相同；逐系列核對 135 日精確分鐘格點及四模型 9/9 收盤 NAV，均與重播吻合。
- 驗收第一次讀 status 未通過瞬時同步檢查，當次即拒絕，未改成容許落後；後續新鮮 API 驗收確認四模型皆零落後。第一次失敗未保存當刻回應，不能僅由後續通過倒推其確切原因。
- 05:02:57 檢查本次 restart 後 journal：無 fatal／traceback／watchdog failure，三服務 `NRestarts=0`。這證明本次切換後的運行狀態，不是未來永不故障的保證，也未替使用者實際發送 Discord 指令或券商訂單。

### 12.4 排程、冷測試與仍需如實揭露的狀態

已安裝並啟用 `stockagent-tw-day-trade-margin-actions.timer`，每週 Mon..Fri 06:00、07:30、08:00、18:00（Asia/Taipei）；下一次為 09/10 06:00。沿用 canonical collector 與 publish wrapper，服務失敗重試、低優先序，加入既有 guardian 的必要 timer 清單。03:38 的實際來源驗收已涵蓋 09/10；不是把 timer active 當成資料已更新證據。

部署後冷測試抓到兩個舊斷言：原本把盤尾非零部位一律要求為 `critical_residual_carried_after_13_30`，與已授權的 margin 政策不符。改為依實際 mode opt-in 分別驗證兩種情境：未授權仍為未解決交割義務；已授權則有正確 margin contract／carry type，空頭 1,000 股完整保留且 `fill_guaranteed=false`。兩者都不能產生 synthetic terminal fill，重啟不得重複成交。新增參數化回歸涵蓋兩條路徑，並保留前次失敗 log。

部署後驗證：`regression_deployed_v8.log` **491 passed / 26.43s**，`regression_discord_deployed_v8.log` **269 passed / 5.97s**；兩組測試檔案互不重複，共 **760 passed**。追加 guardian／冷測試／來源排程 13 項再次通過（不重複加總）；Node 9 passed；Ruff、Bash syntax、JavaScript syntax、`git diff --check` 通過。

`guardian_postdeployment/latest.json`（05:02:12）：未做自動 repair，failures 為零；156 個來源事件觀測、runtime sync、公開 endpoint、磁碟及必要服務/timer 通過。不過 **既有 Discord 背景 formal-history maintenance 仍有 3 筆失敗，因此 guardian 保留 degraded**。這與已通過的 Gateway、命令同步、四模型帳本與本次歷史回補是不同健康域；沒有清掉錯誤收據或捏造 ready。09/10 的正式 08:30 來源／資格及 09:00 行情尚未到驗收時點，也不能在清晨宣稱今天開盤已成功。

本次完成的是指定區間與四模型的 paper 修復。一般盤價格成交零股、全部可轉融資融券仍是使用者授權的模擬假設，不是零股市場實際成交或券商授信／券源證明；舊冷儲存缺件與離線 peer 仍依第 3、5 節分開揭露。
