# 2011 起股票期貨跨日訓練：實作、來源與驗收

本次將現有股票期貨研究延長至 2011-01-03，期貨觀測截止
2026-09-24；分別排 08:45、09:00 決策，兩組從零訓練。
沿用目前 FinancialTransformer d32、lookback 32、global batch 128、
雙 GPU DDP、BF16 AMP、整口契約及跨日持倉帳務。正式設定仍為
最多 1,000 epochs、連續 100 回合無改善停止，min_delta 0.0001。

目前完成程式修正、本機相關測試及三日真實逐筆語意核對。
遠端 SSH 從本次工作中途起回覆 `No route to host`；尚未交付新版資料，
也沒有新版遠端 GPU 測速、收斂模型或投資績效。交付由既有
`stockagent-agent` 監督，恢復連線後才會在 Vast 重建資料並進入原排程器。
這不宣稱已達硬體計算極限。

## 從底層拆開的問題

股票歷史觀測、股票當沖交易資格、股票期貨交易資格是不同邊界。
舊 factorized builder 的股票當沖來源日曆從 2013 年切起，不能直接當成
2011 股票期貨資料是否存在的判斷。本次擴充同一 builder 的明確
`tw_stock_context_futures_portfolio` 政策：保留真實早期日曆及發布歷史、
關閉現股當沖規則、保留 lag 1，現股的次日開盤 gap 維持中性。
交易 eligibility、保證金、公司行動、到期與損益仍由期貨規則來源決定。

特徵篩選使用與正式 runner 相同的 `lookback_shifted` 年度邊界，
不能拿自然年截止日替代實際 train cutoff。最後 fold 由新面板日期重算，
不沿用 2020 版本的 fold 5。延長訓練歷史不更動年度切割政策；
實際 validation/test 日期仍須在新面板與帳務接合後讀回確認。

## 歷史價格與交易時段

原歷史驗證器只允許 TX/MTX 在 2011 起，其餘產品從 2020 起。
這會阻擋有效的早期股票期貨，並非資料本身缺失。本次將共用
`TAIFEX_FUTURES_HISTORY_CONTRACT_VERSION` 更新為 3：股票期貨
可自 2011-01-03 驗證，其他指數／ETF 的既有範圍各自保留。
商品上市、契約單位、調整與到期仍需 dated physical master，
價格驗證器不提供這些資格。

依[期交所股票期貨原始問答手冊，第 4–5、8–9 頁](https://www.taifex.com.tw/file/taifex/CHINESE/10/stf.pdf)，
早期股票期貨已有六級 quote tick 與一般日盤 08:45–13:45、到期日
08:45–13:30。另與[2016 年小型股票期貨上市官方規格](https://www.taifex.com.tw/file/taifex/event/cht/HSTF/event.html)
交叉核對。既有 2026-07-06 tick 修正仍依日期生效，不回填歷史。

原始 PDF 保存在
`data_tw_index_futures/preparation_sources/stock_futures_price_history2011_20261021_v5/`，
SHA-256 `deb0ddaca528c6bf3e395614e668872666a465db987f24ce751581e75bd1b68d`。
收據記錄本次取得時間，不捏造歷史發布時間。

## 原始資料與真實核對

原本的股票來源 release 已有 2011 前行情，來源 manifest SHA-256 為
`167190ad3e17d43aa590bdb4160e8f9c26e5ffe9951e7d9f9069683e0280811f`。
延長歷史沿用同一固定 source release；財報／公共事件仍保留已揭露的
current revision 與估計發布界線，因此整體仍是 research-only，
不是已證明 historical PIT 的實盤輸入。

先完整讀回 4,346 個非空 FinMind 逐筆日期原始檔及收據 SHA，再依官方
股票期貨一般交易日選出本次需要的 3,850 檔，共 4,125,071,681 bytes。
移除交付清單中的 496 個無關日期，少傳 63,074,909 bytes；沒有刪除源本。
交付只含指定原始檔、原始收據與固定 portable inventory，不複製
collector 的可變 queue database、金鑰或整個 `data_finmind`。
這是私有研究交付，不改全域公開發布權限。

鉅額交易證據使用期交所原下載介面，16 個互不重疊的查詢區間覆蓋
2011-01-03 至 2026-09-24。扣除鉅額及價差腿後才核對連續撮合量，
FinMind 雙邊原始數量仍精確除二，重複的真實成交不去重。

| 真實日期 | 有量且官方明確報量的契約日 | 完整日盤量價通過 | 缺口 |
| --- | ---: | ---: | ---: |
| 2011-01-03 | 68 | 68 | 0 |
| 2014-01-17 | 447 | 447 | 0 |
| 2019-01-02 | 341 | 341 | 0 |

這三日另有 896 列官方成交量未報，保留 unknown，不推成零。
上表是有界來源語意證據，不替代全期間逐契約帳務及分鐘 coverage。
遠端須由 canonical financial builder 與 clock builder 完成整段驗收。
既有保留／deferred 商品、完整帳務連通元件與 retrospective selection
揭露不因延長年份而消失，不宣稱全市場無存活者偏差。

本機兩批互不重疊的相關測試為 162 passed，以及 CPU 可見性下
243 passed / 2 skipped；後兩項是 CUDA 編譯帳務 kernel 測試，
不將略過當成 GPU 驗收。合計 405 passed，涵蓋歷史價格／時段、
舊指數與 ETF 邊界、來源 receipt/SHA、分鐘與開盤參考、特徵因果／
年度歸屬、margin preparation／release、執行條件與舊來源讀取路徑。
另已完成修改檔案的語法與文件連結檢查；不是全 repository suite。

## 計算路徑與排程

沿用共享實作，沒有新增 trainer、normalizer、loss 或 GPU scheduler：

1. 固定 source/config/code SHA，核對遠端容量與 RAM。
2. 遠端重建 2011 起 dated financial release，保留原 margin/rule gates。
3. 遠端建立 nullable factorized 股票特徵與年度事件快取。
4. 每個日檔僅讀一次，同時產生兩個分鐘成交視窗與開盤參考。
5. 資料核驗後等既有 GPU 工作釋放，strict dual-CUDA preflight。
6. 原 `run_ablation_experiments.py` 完成兩組各四回合 train/val/test、
   曲線及 checkpoint 工程驗證；要求 finite loss、非零梯度、帳務 adjoint
   replay 誤差零、穩態無新編譯圖，再啟動正式兩組最後 fold。

輸入沿用 `compact_cuda_cached`，在 GPU 還原完整 FP32 特徵，不縮減欄位。
BF16 僅為 AMP，不把財務 reduction 永久降為 BF16。
較長資料的 epoch 秒數不能直接與 2020 版本當成同工作量比較；
新資料的穩態 maximum-rank epoch 與完整生命週期計時尚待遠端實測。
資料建表、冷啟動、正規化與訓練開銷都仍列入評估。

## 設定與產物位置

- 實驗設定：[paired spec](../configs/ablations/tw_stock_futures_history2011_20261021_v5.yaml)。
- 市場設定：[08:45](../configs/markets/tw_stock_futures_carry_0845_history2011_20261021_v5.yaml)、
  [09:00](../configs/markets/tw_stock_futures_carry_0900_history2011_20261021_v5.yaml)。
- 本機交付、來源核驗與排程收據：
  `artifacts/operations/futures-history2011-20261021-v5/`。
  即時狀態為 `handoff-status.json`；遠端進度將為 `data-status.json`、
  `remote-progress.json`，工程驗收為 `engineering-acceptance.json`。
  目前監督 run 為
  `history2011-verified-source-hand-20261021T095347-357fdb68`，
  每分鐘重試已登錄端點，最長六小時；傳輸斷線可保留 partial 後續傳。
  固定請求 SHA-256 為
  `bc58a68f47c8d31ae9f221db86a07306d55cc1b6be7740c1caaae30ffb4e32f3`。
- 遠端資料預定根目錄：
  `artifacts/data_preparation/tw_stock_futures_history2011_20261021_v5/`，
  子目錄 `financial/`、`features/`、`clocks/0845/`、`clocks/0900/`。
- 遠端四回合工程產物預定：
  `artifacts/benchmarks/tw_stock_futures_history2011_20261021_v5/smoke/`。
- 遠端正式模型預定：
  `artifacts/markets/tw_stock_futures_history2011_20261021_v5/clock__0845/`、
  `artifacts/markets/tw_stock_futures_history2011_20261021_v5/clock__0900/`。

上述「預定」目錄不能當作已有完成模型。實際 run manifest、fold completion、
checkpoint、驗證／測試曲線與 exact data fingerprints 才是訓練驗收依據。
原 2020 版 v4 訓練及產物不覆寫、不捐贈權重或 optimizer。
