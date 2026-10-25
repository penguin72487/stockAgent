# 2011 起股票期貨跨日訓練：實作、來源與驗收

本次將現有股票期貨研究延長至 2011-01-03，期貨觀測截止
2026-09-24；分別排 08:45、09:00 決策，兩組從零訓練。
沿用目前 FinancialTransformer d32、lookback 32、global batch 128、
雙 GPU DDP、BF16 AMP、整口契約及跨日持倉帳務。正式設定仍為
最多 1,000 epochs、連續 100 回合無改善停止，min_delta 0.0001。

目前完成程式修正、本機相關測試及三日真實逐筆語意核對。
舊遠端 SSH 回覆 `No route to host`，六小時交付重試已失敗結束；
尚未交付新版資料，也沒有新版遠端 GPU 測速、收斂模型或投資績效。
2026-10-23 使用者指定新節點遷移，目前進度見文末；資料須在新節點
由原 builder 重建，驗收後才進入原排程器。
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
  舊監督 run 為
  `history2011-verified-source-hand-20261021T095347-357fdb68`，
  每分鐘重試已登錄端點，最長六小時；2026-10-23 已讀回 failed、
  exit 1，對應 systemd unit inactive。不是仍在等待的訓練。
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

## 2026-10-23 新 Vast 節點遷移

使用者指定 `root@114.34.26.236:43269`。首次登入已讀取該節點
`/etc/vast-agents-guide.md`，實測為雙 RTX 5090（各 32607 MiB）、
CPU cgroup 配額 61.43999 cores、RAM 上限 259578134528 bytes、
約 999 GB 可用容器磁碟。原生 Python 3.12.15、Torch 2.11.0+cu128、
Triton 3.6.0；兩張卡已各通過 initialized-compute strict CUDA preflight，
收據為下述 `cuda-preflight.json`。這仍不是 DDP 全生命週期驗收。
沒有既有 StockAgent 工作區或研究訓練程序；容器使用 Supervisor。

固定程式 `00c03c35d752febc8ce44cd85149e720ba78ab6362ed1f52bb449a311d9bbec4`
的 1826 個成員已在本機重新驗證。股票資料固定原 snapshot；期貨來源固定
`tw-futures-margin-sources-20261017T033112492777926Z-l0-penguin-507e72306d051d9b`。
兩個 cold manifest 中的原始 `source_manifest.json` SHA 都與 v5 請求一致；
必要 tick inventory 與 rule delta 的本機指紋也仍相同。

遷移記錄位於 `artifacts/operations/futures-migration-20261023/migration-plan.json`。
同目錄已完成固定程式的遠端 SHA 讀回、離線 wheel SHA 校驗及原生 CUDA
runtime bootstrap，沿用 `scripts/runtime_env.sh`、`check_environment.py`
及既有 v5 preparation/queue。`bootstrap-ready.json` 與 `cuda-preflight.json`
保留本次實際 runtime 身分；不宣稱與原節點逐位元相同。之後仍須完整資料重建、
兩個時鐘各四回合的生命週期驗收；不載入舊現股 v6 或 2020 v4 checkpoint。
另以 canonical exclusive GPU leases 執行有界 NCCL／BF16 DDP backward probe，
雙 rank 的同步梯度最大絕對誤差為 0、非零有限梯度、all-reduce sum 為 3；
`ddp-preflight.json` 明確標記不是策略訓練或完整 fold 驗收。

曾因追加公鑰後認證失敗暫停；使用者經 Vast 對該實例附加公開金鑰後，
已獨立驗證 ED25519 及原 RSA 均可登入。新節點已取代唯一 Vast edge peer，
原 peer 保留為 paused retired identity；`node-enrollment.json` 記錄身分與
端點遷移，沒有刪除原來源、舊模型或冷資料。

最終交付請求 SHA 為
`aa65e02a18dee45731e4f1f579b7cfa74750d8de598b65d3f7ab698022397595`，
監督 run 為 `futures-migration-path-corrected-20261023T043009-91d0a39c`，
已 exit 0。前一請求 `0cc1fd23a0a7...` 在 preparation 前明確停止並保留
`prior-request-0cc1fd23a0a7/`；不能把其 failed run 當成目前訓練失敗。
啟動位置改為已驗證同 1826 個成員 SHA 的 `/root/stockAgent`，
避免從 frozen build-source 的 cwd 解析到錯誤資料／cache 路徑。
原 frozen release／模型／帳務沒有更動；此次 amendment 只固定啟動位置、
spec 位置與交接 wrapper SHA。四個 preparation／runner 入口的 `--help`
依賴檢查也已全部通過；缺少的 BeautifulSoup／SoupSieve 已由 SHA 核驗的
離線 wheels 補齊，實際 runtime 記於 `runtime-after-dependency-repair.json`。
`source-transport-readback.json` 已在遠端逐檔驗證 3850 天官方 tick
及 receipt（raw 4125071681 bytes），另有 2159 個必要補充來源檔。
原 v5 程式／模型／來源 ABI 不變；遷移包另固定其 orchestration SHA。

Syncthing index 已完成 authenticated peer convergence。容器 root 缺少跨 UID
ptrace 能力，原映像的 `user` 身分服務使 canonical process-reference scan
無法完整檢查。保留原設定備份、device cert 與 index，將既有 Syncthing
Supervisor program 改由 root 執行，已讀回同一 device ID 並通過完整程序
引用掃描（`process-scan-repair.json`）；未跳過檢查或另開資料傳輸 owner。
兩個固定來源已由既有 packed transport timer／edge materializer 按需傳送、
完整還原，並逐一核對原 `source_manifest.json`；證據為 `cold-sources-ready.json`。

2026-10-23 04:30 UTC（12:30 臺北）已實際啟動 Supervisor programs：
`stockagent-futures-history2011-preparation-20261023-migrated-v5` 與
`stockagent-futures-history2011-queue-20261023-migrated-v5`。
讀回 preparation 階段為 `preparing_2011_dated_financial_accounts`，
queue 為 `waiting_for_2011_source_admission`；不是已有 GPU epoch。
首個 builder attempt 在舊絕對路徑讀取 final-settlement HTML 時失敗；
原始檔已於 canonical data root，並非來源內容遺失。沿既有
`restore_vast_futures_raw_sources.py` 的 receipt inventory／SHA 檢查恢復
相容連結，430 筆原結算收據均由原路徑讀回相同 SHA；另恢復原三個
`data_tw_futures/margin_*` 相容連結，不改原 manifest 或複製 raw payload。
`source-alias-readback.json` 與 `preparation-retry.json` 保留證據；失敗 output
經程序引用與 metadata 檢查後以同檔案系統 rename 保留在
`failed-preparations/financial-before-source-alias-repair/`，未刪除。
04:32 UTC 已重啟同一 canonical preparation；04:33 UTC 已越過原失敗點，
實際產出 physical calendar 4393766 rows／50092 lifetimes／710 products。
這是重建中的中間產物，完整 financial／feature／clock admission 與 GPU
epoch 仍待後續階段；以最新進度與 lifecycle 收據為準。

即時狀態仍以新遷移目錄為準：`cold-source-status.json`（冷來源）、
`data-status.json`（financial/features/clocks）、`remote-progress.json`（GPU queue）；
成功證據依次為 `cold-sources-ready.json`、`data-admission.json`、
`engineering-acceptance.json`。僅有 source transport 或 CUDA preflight
不代表已開始訓練；模型產物仍須上述 canonical lifecycle 證據。

既有 `stockagent-remote-cold-artifact-ingress.timer` 保持為成果回傳 owner；
端點已更新至新機，移除僅適用舊節點且新機不存在的 storage-layout plan，
預設 include root 改為本次 markets 根目錄。實際仍由 schema-3
`training_return.json` 的 lifecycle-complete／容量／穩定性／七日 hot retention
及 exact cold recovery gates 決定回傳與退休。`return-path-dry-run.json`
已通過新機雙 scope 探索（0 completed runs）；這證明控制路徑可執行，
不是已有模型回傳或冷封存證據。

### 10/23 13:45：雙時鐘完整核驗與新實驗排程

financial／features 已建成，3,826 日期逐筆讀回完成；最後剩
2012-06-04 `CTF:201206@2012-06-20#generation=1` 一個來源缺口：
官方 outright 108 口、既有逐筆 107 口，重新查詢 FinMind 仍為 107。
準備器因此以 `blocked_source_coverage` 停止，基準仍未開始 GPU epoch。
原始與 partial clock 證據保留；不能把檔案已存在當成有效 admission。

另已在新機登記名目座標、較大 AdamW 步長、Muon 與交互組的雙時鐘十組實驗，
等待兩個從零基準完成；沒有插隊或更改原基準請求。
歷史 v8／v10／v16 績效範圍、原始至 2026-10 文獻、設定、工程證據、
確切產物位置及來源補證界線見
[本輪研究與排程](tw_stock_futures_conditioning_2026-10-23.md)。

15:10 後續：新官方日報仍確認上述一口差異。原 2011 基準及十組請求不變；
另宣告獨立 2013-01-02～2026-09-24 的雙時鐘研究，重新驗得 1,705,459 契約日、
1,341,942 個 verified intraday 契約日、零缺口，未放寬 2011 admission。
新機已進入最後 fold12 的 A/B/B/A 投影工程測速，之後一致選路徑、六組工程再從零收斂。
六組保留 d32/L32/global128、Log Utility、連續100回合無改善與金融帳務；
單 fold 改用既有直接 DDP 以省父程序重建。請讀上述研究報告的新增節及
`artifacts/operations/futures-history2013-20261023-v1/remote-progress.json`，
正式目標為 `artifacts/markets/tw_stock_futures_history2013_20261023_v1/<case>/`。
此為獨立期間的新研究，仍不能稱為 2011 全期完成或已超越舊模型。
