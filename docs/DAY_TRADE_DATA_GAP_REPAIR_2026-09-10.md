# 當沖來源缺口修復與安全遮罩

後續更正：已按既有網頁標準修正「無持倉企業行動也阻擋」「已確定現金權益仍要求股票行情」與零量 K 棒觸發問題；詳見 [同標準修復與實測](DAY_TRADE_PAPER_STANDARD_ALIGNMENT_2026-09-10.md)。下文記錄的是前一版；v3 的分鐘來源缺口只對實體股票持倉阻擋，已確認金額／付款日的現金應收應付保留並正常結算，不能再把它當成股票缺價。

## 1. 執行進度

本輪已修復換股公告解析與保留分鐘資料恢復，並加入跨日持倉安全遮罩。
**正式年度訓練、來源轉接器整合、全市場資料重建／發布、Vast 新版本同步尚未完成。**
未修改正式網頁帳本、模型選擇、交易假設、交易服務或既有訓練 checkpoint。

最新逐項資料數量與剩餘事件見 [來源品質盤點](DAY_TRADE_DATA_GAPS_2026-09-10.md)。
盤點範圍為 2020-03-02 至 2026-09-10，不是對所有留存年份作完整可交易性認證。

## 2. 修復結果

| 項目 | 修復前 | 修復後／驗證邊界 |
| --- | ---: | --- |
| 換股參考來源失敗 | 36 | 20；成功參考事件 247，仍非完整會計條款 |
| 現行合約缺失的來源股票 | 127 | 124；三檔完整保留行情已恢復來源清單 |
| 已明列的 provider 缺日 | 127 股票日／89 檔／27 日期 | 未用假資料填補，保留精確股票日清單 |
| 本輪恢復的分鐘資料 | 未進目前訓練資料清單 | 801,595 根；隔離研究子集，不冒充全市場 READY |

### 換股公告

延續官方 TWSE detail 與 MOPS 備援，重用成功 cache、保留 request/response。
本輪部分 TWSE 原路徑重新成功，並新增公告明文「減少股數 × 面額」退還現金的解析。
2107／2023-09-18、4994／2023-10-11 的明文公式可換算每舊股現金；使用 Decimal
運算，要求明確非約數的換股比例。不能把不足一股的現金補償誤當每舊股退款；
約數、相互矛盾的每股現金、未綁定主詞仍拒絕。

證交所的[減資參考價說明](https://www.twse.com.tw/zh/announcement/reduction/twtauu.html)
區分減資比例與退還股款；價格公式本身不能補出公司付款日或其他換股權益。
已恢復參考列仍標示會計未完成，沒有由股票價格倒推出付款條款。

- 新 attempt：`generated_at_utc=2026-09-10T14:15:30.970383+00:00`。
- 643 筆 request/response 大小、SHA-256、request hash、路徑範圍全部核對。
- 新 raw manifest：`91da16064930edc9a966a73d29a12a4d7c3d68ccb16c35d2d7cac57f204d66b8`。
- 不完整結果沒有覆蓋 accepted Parquet：SHA-256 仍為
  `dfc0a842e61ebeed4dffec9e6aa03fc22ddedb74fef81671f4c9ce8753bb7bc2`。

### 被現行合約清單漏掉的歷史行情

根因在 `restore_extended_tail_from_archived_manifest` 只允許缺最後一個分塊。
期間從原截止日延長到 9/10，跨過多個分塊時，原有全期行情被降為整檔 unavailable。
修正只接受連續尾端分塊，核對來源 receipt、原始 hash、公開正成交量日期；新區間若有
任何未覆蓋的正成交量日，就不能寫成 empty。新空分塊記錄「未查詢，公開參考無所需
正成交量日期」，不聲稱收到 provider 空回應或證明官方停牌。

| 股票 | 原始分鐘列 | 來源 sessions（未過濾非交易日） |
| --- | ---: | ---: |
| 2867 | 324,646 | 1,576 |
| 4130 | 143,751 | 1,554 |
| 5371 | 333,198 | 1,578 |

本輪沒有呼叫行情 API。官方交易日上的保留來源合計 4,707 股票日；另有四檔合計
156 股票日的局部保留分鐘證據，仍待按完整期間補齊／納入。
現行未恢復清單 124 檔共有 71,194 個正成交量股票日，不能將其全部叫做「歷史無行情」。

保留原始 manifest、catalog 的備份：
`artifacts/operations/daytrade_source_gaps_20260910/pre_retained_recovery/`。
來源 canonical catalog 已更新；既有全市場 `research_dataset` 及 Vast 固定 release 沒有被替換。
三檔透過原有 builder 重建於：
`artifacts/operations/daytrade_source_gaps_20260910/recovered_minute_dataset/`。
其 manifest 為 `research_subset`、`research_ready=false`；有 1,579 個日期分區。

## 3. 遮罩從權益守恆出發

`DayTradeCarrySession.source_gap_mask` 僅屬執行／研究品質資訊，不能進模型輸入，不能
在輸出後把未成交權重重分配給其他股票。沿用同一個 FIFO／NAV／費用引擎：

- 只禁用指定股票日的新單，其他股票與日期不變，缺價仍保留 NaN。
- 對既有 long、short、尚未付款的現金權利或義務，一律拒絕略過；檢查 gross cohort，
  不能用相反部位淨額抵消證據。
- 不造平倉，不刪庫存，不把資料錯誤當破產，不把失敗當零報酬。
- CPU 明確拋錯；CUDA 保留原部位並輸出 failed／NaN，不使用會毀掉 context 的 device assert。
- session ABI 提升至 `tw_day_trade_physical_fifo_sessions_v2`。未接通正式 trainer 前，
  不啟用候選設定、不放行舊 optimizer resume；後續來源版本亦須綁定明列的遮罩範圍。

這個保護已在 canonical carry executor 實作，但沒有把盤點自動變成正式訓練遮罩。
若缺企業行動條款且已有庫存，僅遮復牌日仍不能算出正確權益；需要補條款，或另訂有
公告時點及可成交平倉證據的事前避開區間，不能回看損益後挑日期刪除。

## 4. 驗證

- CUDA strict environment check 通過：RTX 5070 Ti、Torch 2.13.0／CUDA 13.0。
- 11 個來源、FIFO、分鐘排程、遮罩、checkpoint／resume 測試檔：**351 passed，35.35 秒**。
- CUDA 實測缺資料且已有空單：原股數與成本保留，NaN／failed，不誤標破產；之後 CUDA
  張量運算仍正常。此測試不是完整 epoch 測速或交易績效證據。
- Ruff 通過；隔離子集須以明確 `--allow-research-subset` 執行完整分區 audit，輸出仍為
  `research_subset`，不得提升為全市場／完整訓練 readiness。
- 重建子集 **1,579／1,579 分區、801,595 列**雜湊與分鐘列語義驗收完成，`failures={}`。
  收據為 `artifacts/operations/daytrade_source_gaps_20260910/recovered_minute_audit.json`。
  驗收僅證明這個三檔子集的分區完整性；不代表 2,754 檔或所有企業行動已通過。
  子集實際行情截止 2026-08-21，對應 1,578 官方交易日；額外保留的 2024-12-01
  是非交易日來源分區，只能留作證據，不能進交易日訓練／執行。

盤點重跑（只讀來源，寫操作報告）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_day_trade_source_gaps.py \
  --public-root /srv/stockagent-live/data_tw_public \
  --start-date 2020-03-02 --end-date 2026-09-10 --focus-start 2026-02-25 \
  --output docs/DAY_TRADE_DATA_GAPS_2026-09-10.md \
  --inventory artifacts/operations/daytrade_source_gaps_20260910/inventory_after_recovery.json
```

## 5. 仍缺什麼

除了上述 20 筆換股參考、分鐘來源缺口，所选期間除權息表還有 2,132 筆 `avoid`
分類：1,719 配股／認購事件、299 非公司事件、113 現金條款未解或複雜事件及 1 ETF
未解事件。這些是「不能直接走現有 exact-cash 會計」的分類，不等同於 2,132 次下載失敗。
保留全可融資融券的研究假設，不代表可以忽略股票股利、現金認購、零碎股處分與付款日。

剩餘工作包含：缺失來源補齊、企業行動與分鐘資料的因果轉接、正式 trainer 的跨日 state／
年度報告整合、全市場重建與 catalog 發布、Syncthing 新版驗證、Vast 完整 epoch 驗收。
不能在本輪宣稱新的正式訓練指令已全部可用。
