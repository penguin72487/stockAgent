# 當沖 YTD：可執行基準修正與驗收

## 1. 執行進度

2026-09-10 已完成程式修正、398 項相關測試，以及四模式各 135 日（2026-02-25～2026-09-09）、共 540 份隔離訊號重算。

**尚未部署新版、未重啟正式服務、未升版歷史帳本，也未完成全期成交／YTD 重算。**
不是為了讓圖表追上較高的訓練報酬而修改百分比；新版收緊後的第一日實測揭露缺少指定分鐘來源與真實容量不足。候選尚未通過安全切換的完整分鐘、未平倉及資料證據驗收，不能覆蓋正式帳本。

本次所有說明紀錄僅為 Markdown；未建立 notebook、HTML 報告或網站筆記。

## 2. 已修正的責任分工

| 層 | 修正 |
| --- | --- |
| 模型可見 universe | `signal_engine` 在 forward／跨股票注意力／L1 配重之前套用 exact-session 當沖資格，與訓練 `dataset.py` 的 eligibility mask 一致。不再先分配給 alive+有價股票，再讓執行器拒絕大量非法目標。 |
| 方向與成交 | 缺價、賣先禁止、已知價格限制另約束執行目標；不把被拒絕的風險重新分配給其他股票。盤後使用已完成日資格，不要求下一日資格。 |
| 資格來源 | 支援市場設定的 rule root；同日來源快取綁定日期、ordered symbols、檔案 inode／size／mtime。歷史讀取只接受 exact-date rows，不能拿最新名單代替。原本 live readiness 要求 latest==target 的預設未放寬。 |
| 本金與淨值 | 初始本金維持報酬分母；`session_sizing_nav_twd` 為已平倉帳戶當日淨值，跨日承接獲利及虧損，在同日等待報價／重啟期間凍結。NAV 非正不重設本金。 |
| 委託可負擔性 | 重用 canonical integer budget scaler，雙向皆占用總名目本金並預留入場費用；放空收入不當成額外下單資金，不無限加槓桿。這是 paper NAV 風險預算，不是已驗證的券商可用額度。 |
| 09:01 回補 | 保留官方 open 推論／定股數、來源 09:01 VWAP 或同根 KBar Close 執行。新增 50% 該分鐘成交量的整張上限；價格存在不再代表全部數量成交。無 +1 tick 入場替代。 |
| 出場時鐘 | 歷史 right-labelled 13:24 bar 仍屬 13:24 市價送出前的區間；市場單使用 13:25 bar。保留 13:25～13:30 來源分鐘與集合競價量，不以日線 close 虛構深度。 |
| 收盤殘餘 | 新契約不再用 `SIM_TERMINAL` 強制產生平倉成交。剩餘義務留在帳本、顯示 critical、阻擋新增曝險；不自動假定已取得借券或融資額度。舊估值路徑只保留相容控制。 |
| 防止重複 | 拒絕重複 symbol 的訊號，避免同一部位覆寫卻重複消耗流動性。 |
| 歷史明細 | 零成交也寫入空的 dated position archive，包含訊號／checkpoint／config／execution 身分，不再跳過而殘留舊策略部位。正式歷史舊 archive 尚未覆寫。 |
| 對外語義 | 共用 account performance 與 dashboard source contract 明確區分 sizing basis、execution version、未驗證券商額度及研究回測可比性；未調高任何報酬數字。 |

模型權重、原始訓練 artifacts、optimizer／checkpoint ABI、費率設定均未修改。`training/dataset.py` 僅澄清既有 policy-mask 註解，沒有假裝舊日線回測已變成新分鐘回測。

新歷史契約：

```text
retrospective_official_open_signal_at_09_00_observed_09_01_minute_price_volume_capped_nav_counterfactual_v3
```

## 3. 真實模型查核

重用既有 backfill entry point，增加 `--live-output-dir` 以隔離輸出；replay 增加 `--signal-root` 與 `--local-only`，不修改已接受訊號，也不因稽核自動登入券商或消耗歷史查詢額度。

輸出根目錄：`artifacts/audits/day_trade_execution_v3_20260910/full`。

| 模式 | 完成訊號 | checkpoint fingerprint | 不合資格但非零 model weight／target weight |
| --- | ---: | --- | ---: |
| 一億 | 135 | d675da50723c | 0 / 0 |
| 多基底 | 135 | 324b61d09d08 | 0 / 0 |
| 多基底22 | 135 | 44957faaac45 | 0 / 0 |
| LayerNorm v12 | 135 | 4307f9d1b764 | 0 / 0 |

逐一驗證 540 份 summary SHA-256 與 backfill receipt 相符，日期為 02/25～09/09，且所有 summary 具有 `exact_session_eligibility_before_forward_v1`。

v12 07/29 受控實例：同一 checkpoint 原先回補分配 6733、3018 合計 100%；修正後兩者 `tradable=false`、model/target weight 都為 0。新目標包含 1225、4979、00665L、4976、4430、6620。這證明修正發生於配重階段，不是事後修改損益。未宣稱新目標與原訓練保存輸出逐浮點完全相同；資料快照與數值路徑的完整等價證明仍有獨立邊界。

資格讀取測量（2,753 symbols、正式來源 2026-09-09）：冷讀取 42.518 ms；相同來源快取含兩份 mask copy，100 次中位數 0.006642 ms、最大 0.020208 ms。不將此局部時間宣稱為 Discord 端到端或開盤 SLO。

## 4. 第一日可執行回補驗收揭露的阻礙

最終受控候選：`artifacts/audits/day_trade_execution_v3_20260910/smoke_0225_v2`。

以 02/25 一日、四個真實模型、現有本機分鐘資料、canonical paper engine 重播：

| 模式 | 入場成交檔數 | 收盤仍未平倉檔數 | 分鐘點數 |
| --- | ---: | ---: | ---: |
| 一億 | 355 | 12 | 270 |
| 多基底 | 17 | 0 | 270 |
| 多基底22 | 5 | 0 | 270 |
| v12 | 9 | 2 | 270 |

一億殘餘：00641R、00669R、00755B、00898、00986A、00987B、4702、6259、6496、6506、7753、8354。

v12 殘餘：3206 空 1,000 股、3684 空 10,000 股。來源中 13:30 的成交量分別為 36,000 與 22,000 股，50% 整張容量並不足以消化此前剩餘的全部委託；不能把「有收盤價」改解讀成「可無限成交」。

606 個 requested entry symbols 中，本機指定 09:01 價格解出 391 個，215 個未解出；另有 25 列有價格但未形成至少一張的 50% 分鐘容量。後者不可一律稱為下載失敗。例：0056 保留的該日 chunk 首列為 09:03；本次未對交易所當時是否交易作結論，也未使用 09:03／官方 open 冒充 09:01。

四模式各 270 點、沒有 synthetic terminal fills；仍不能因曲線點數足夠就視為可交易驗收通過。候選 close status 為 `unresolved_delivery_obligation_at_close`，下一日不得忽略這些部位重新開始。

**全期新版 YTD 尚未產生，也沒有候選 promotion。** 540 份訊號完成不等於 135 日成交與 NAV 重建完成。

## 5. 驗證與正式服務隔離

以下相關測試合計 **398 passed / 21.29 s**：

```text
test_day_trade_execution_reconciliation.py
test_tw_day_trade_simulation.py
test_live_signal_helpers.py
test_tw_day_trade_two_phase_cold_test.py
test_promote_tw_day_trade_replay.py
test_switch_tw_day_trade_strategy.py
test_tw_day_trade_open_price_replay.py
test_cross_surface_performance.py
test_dashboard_updates.py
test_public_dashboards.py
test_discord_bot_helpers.py
```

新測試涵蓋 exact-date mask、方向 veto 不改 token universe、來源快取失效、獲利／虧損／破產、重啟、分鐘容量、費用預算、無成交殘餘、禁止重複 symbol、空 archive、升版容量驗證、13:24→13:25 因果時鐘及 270 點。

兩階段冷測試保留缺資料→修復→開盤因果報價→盤中→盤後→重啟流程；無 ask、無成交量的空單，正確預期是保留交割義務而不是測試要求假平倉。歷史 synthetic terminal 測試明確設為 legacy 控制。

變更 Python 檔案 Ruff 與 `git diff --check` 通過。CUDA 環境 strict check 通過（fintech Python 3.12.14、RTX 5070 Ti）；實際模型推論已執行，不僅是 mock。未重訓模型，也未宣稱全倉庫測試通過。

正式 service 的 MainPID／啟動時間維持原狀：simulation 53470、Discord 53549、gateway 59136，三者 active；這只證明本次沒有重啟，不代替新版 warmup／Gateway／SLO 驗收。正式四模式仍為 09/09、open_position_count=0、execution_realism_contract 尚未啟用。

正式 `fills.jsonl` SHA-256 與修正前相同：

```text
ab5f4a95938ddd7b778ea716774cb0476cbf322c6aac9007761d477a503033ec
```

## 6. 還需要的決策與證據

後續使用者已明確授權「所有當沖殘餘可轉融資融券、隔日模型差額調整」的模擬假設；下列第 2 點的授權問題已解決。實作、成本與仍未通過的企業行動／正式部署驗收見 [跨日差額調整紀錄](DAY_TRADE_MARGIN_CARRY_2026-09-10.md)。本文件上方保留前次驗收時點的原始結果。

1. 對確切缺失的 symbol/date 走 canonical collector 重新驗證；找不到指定分鐘就保留缺口，不能造價或全市場重抓。此次候選刻意 local-only，未做新的 Shioaji 登入／歷史查詢。
2. 收盤殘餘要採用已核准、有限額度的現金交割／券差借券／隔日回補處理，或事前修改可觀察的倉位／流動性限制。這會改變策略及風險假設，不能為了讓驗收全平而擅自假定無限融資或免費借券。
3. `session_sizing_nav` 是經濟淨值，不是現金餘額。券商下單額度、T+2 應收應付、退佣入帳日與未沖銷成本還需接上相同的帳戶契約；本次只加總 NAV／整張與費用預算，不宣稱已完成券商購買力驗證。
4. 確認上述邊界後，重用新訊號，隔離完成全期 replay、逐筆 fills/NAV 對帳、每模式每天 270 個來源分鐘，再原子升版、保留 rollback、重啟及驗證 web／Discord 共同 revision。不能只重啟服務就說全期修好。

官方依據：當沖以相同數量抵銷後差價交割，不保證反向交易完成；無法買回時仍有券差與交割義務。[TWSE 當日沖銷交易專區](https://www.twse.com.tw/zh/products/system/day-trading.html)。應收應付的交割時點與 NAV 不是同一概念。[TWSE 集中市場交易制度](https://www.twse.com.tw/zh/products/system/trading.html?hl=zh-TW)。本次未代使用者新增真實融資／借券授權或下單。
