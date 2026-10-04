# V8 期貨保證金模式

2026-09-28 更新：使用者同意先使用規則可驗證範圍。TX/MTX 月契約的
2021-05-20～2026-09-04 資料已整理為獨立版本，設定採一億資金與 fold 4。
歷史全商品 `margin_rules_v1` 的缺口仍保留；下文的 fold10 blocked 紀錄是此前全範圍預檢。
本次資料、樣本邊界修正及遠端指令見
[已驗證範圍準備文件](tw_futures_margin_verified_scope_2026-09-27.md)。

## 2026-09-27 本次切換與 fold10 預檢

使用者已將一般模式績效實驗改為保證金模式。本次保留剛完成的一般模式 v3 結果，
停止新增一般模式實驗；原訂 general prior-capacity v4 沒有啟動。
保證金採既有 `tw_futures_v8_margin.yaml`，包含已修好的共用整數帳本與梯度契約，
預定新產物目錄為 `artifacts/markets/tw_futures_v8_margin_fold10_v2/`。
仍沿用 fold10：訓練 2015–2024、驗證 2025、測試 2026；不拿一般模式 checkpoint
直接解讀成保證金預算，也不把一般模式報酬乘固定倍數冒充保證金回測。

遠端 canonical 預檢再次回傳 exit 1，原因為
`data_tw_futures/margin_rules_v1/rules.parquet` 不存在。
紀錄：`artifacts/markets/tw_futures_v8_margin_preparation/margin_fold10_preflight_20260927T080311Z.json`
及同名 `.log`；**尚未啟動保證金正式訓練，沒有本次保證金收益結果**。
所需歷史資料不只包含原始保證金，也包含維持保證金、公告與生效時間、
部位限額與價格界線。現行快照不能自動當成 2015 年已知條件。

已重新核對[期交所結算問答](https://www.taifex.com.tw/cht/9/tradersQAClearing)：
官方指引是從[歷史公告](https://www.taifex.com.tw/cht/11/hisNews)查詢保證金調整。
本次已請使用者選擇補齊真實歷史規則，或另立明示固定比例假設的研究實驗；
在選定前，既有真實規則驗證器保持啟用，沒有寫入假規則或宣稱 PIT 通過。

資料通過後的 fold10 命令如下。第一行預檢失敗就停止；不要略過它續跑：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw_futures_v8_margin.yaml \
  --start-fold 10 --max-folds 1 --check-data-only \
  --output-dir artifacts/markets/tw_futures_v8_margin_fold10_v2 && \
run_fintech_python scripts/check_environment.py --require-cuda --strict && \
run_fintech_python train.py --config configs/markets/tw_futures_v8_margin.yaml \
  --start-fold 10 --max-folds 1 --no-resume --profile-timing \
  --output-dir artifacts/markets/tw_futures_v8_margin_fold10_v2
```

## 模型與資金語義

第三個模式沿用一般模式的股票背景、FinancialTransformer、現金配置與 canonical 訓練流程。
有號模型輸出代表原始保證金預算占帳戶資金的比例；未配置部分保留為可用資金。
整口執行器用當時有效的每口保證金換算口數，名目曝險可以超過權益，並非固定倍率乘上報酬。
例如每口名目金額為保證金的十倍，模型分配 50% 權益作保證金預算，
未計整口與費用前約為五倍名目曝險；分配零即空手，不必湊滿一倍。
模型另讀兩個因果特徵：原始保證金／前次已知名目金額、維持／原始保證金。
當日成交價、未完成日盤損益不進入這兩個模型特徵。

## 因果時間與帳本邊界

08:45 前使用先前完成的股票、期貨與已公告生效規則決策；一般日盤 OPEN 是既有日頻研究成交代理。
每日以官方結算價評價，下一日先計入實體合約隔夜價差，再檢查保證金與調整口數。
到期採官方最後結算價；fold 終止採既有日頻收盤研究代理並保留量價限制。
歷史保證金、部位限制、漲跌停界線或估值資料缺少時，禁止開始正式訓練。

本模式的風險觀察點是日盤開盤與結算，不能稱為已重建盤中或夜盤券商即時追繳。
研究政策是不追加外部資金；前一結算追繳未解除時於下一日開盤平倉，
在可觀察的開盤風險指標低於設定門檻時亦平倉。無法成交的部位與帳戶失敗必須留下紀錄。
結算後權益不足維持保證金時記錄追繳；次日風險檢查採前次結算權益與保證金，
開盤再加實體合約隔夜價差。強制平倉不因帳戶已無可用保證金而取消；可以實現負權益。

原始保證金是資金占用，沒有當作損失扣掉。權益只隨價格損益、佣金與期交稅變動。
既有部位權益落在原始與維持保證金之間時可以續抱；不把「不足原始保證金」直接視為破產。
不足以重新配置時只允許維持或減少既有口數，不讓保證金缺口成為開新倉的資金。
開新部位以「前次結算權益、開盤權益」較小者作資金上限，並預留平倉費用，
不提前花用未結算的持倉利益。逐口保證金由固定台幣金額或名目金額比率計算，採元以下四捨五入。
既有日頻成交量上限限制開盤調整；非到期終止平倉另受當日成交量與方向價格限制。
到期現金結算直接依官方最後結算價終止，不冒稱是在沒有成交量時賣出。

## 官方依據

- 原始保證金、維持保證金、股票期貨比例與元以下四捨五入：
  [期交所股票期貨保證金](https://www.taifex.com.tw/cht/5/margingReqSSF)。
- 保證金為動態公告數值，券商可加收；股票期貨不能一律套當沖減半：
  [期交所結算問答](https://www.taifex.com.tw/cht/9/tradersQAClearing)。
- 追繳至原始保證金與依受託契約代沖銷；約定風險門檻不得低於 25%：
  [期交所交易風險專區](https://www.taifex.com.tw/file/taifex/event/cht/investmentRisk/index.html)。
- 各商品與同標的大小契約的合併部位限制：
  [非個股類](https://www.taifex.com.tw/cht/4/traderPLNonEquity)、
  [個股類](https://www.taifex.com.tw/cht/4/traderPLEquity)。
- 合約乘數、交易時間、到期與契約調整：
  [股票與 ETF 期貨規格](https://www.taifex.com.tw/cht/2/sTF)。

採逐口總額保證金，不計 SPAN、跨月或跨商品折抵；不套用當沖優惠。
同標的不同月份、大小契約依規則的單位換算合併；目前採絕對口數總量上限，
比交易所逐方向限制更保守。買入在上限價格、賣出在下限價格時禁止假設成交；
日頻 OHLC 無法證明漲跌停排隊順位，不能把這項保守限制當作逐筆撮合重播。

帳務版本 2 修正歷史限額降低的處理：持倉上限是合併口數不等式，
不是全帳戶歸零義務。模型減倉後仍超限，才依各實體合約剩餘可平容量
分攤超額減倉，整口向上取整且不超過容量；合法的其他群組不受影響。
只有執行後仍超限才記錄強制減倉失敗。這與保證金追繳／風險門檻觸發的
既有全平義務分開判定。來源 tape schema 維持版本 1；舊帳務版本的
checkpoint 不可續訓或混用歷史產物。訓練 backward 版本 5 也讓真正的
違約在梯度路徑保持吸收狀態，保留造成違約決策的梯度，禁止違約後重新注資。
這些是明示的保守研究政策，不能宣稱是每家券商的實際收費與追繳時限。
交易佣金與日期版本期交稅仍由既有期貨成本程式處理。

券商加收倍數 `tw_futures_portfolio_broker_margin_multiplier` 預設 1.0，同時乘上原始、維持保證金；
代沖銷風險指標門檻 `tw_futures_portfolio_margin_liquidation_ratio` 預設 0.25，兩者可在設定調整。
本模式不模擬券商追繳通知、通知後的實際處分時限，亦不接受外部補款。

## 資料

現有期貨日資料含官方每日結算價，但沒有覆蓋完整回測期間的逐商品保證金、
部位限制與價格界線。現行官方表不能回填成 2015 年已知規則。
來源沿用 `scripts/download_taifex_openapi_catalog.py` 的官方快照與
期交所歷史公告；新增的是訓練用規則驗證與對齊，沒有第二套下載器或訓練入口。

2026-09-27 查核：五類保證金／部位限制快照各有 18 份完成 receipt，
首次擷取為 UTC 2026-09-02 17:07 起（台北 2026-09-03），且 receipt 明訂擷取前不可使用。
既有日資料有 2,926,274 個合約交易日、47,376 個實體合約、507 個商品，
1998-07-21 至 2026-09-04；正式股票背景 fold 自 2015 年訓練，快照不足以填入歷史。
目前 `data_tw_futures/margin_rules_v1/rules.parquet` 尚不存在，正式訓練可用狀態為 **blocked**。
現有個別 TX／MTX／TMF 資本研究的保證金表只驗證一段 2026 年日期，不能外推至全商品、全年份。

規則 release 的每列是 `(date, physical_contract)`，不得重複；所有當期有效合約都必須對齊。
輸入欄位如下，沒有資料時不得用預設比率、無限部位或無限價格界線補值：

| 欄位 | 契約 |
|---|---|
| `margin_kind` | `fixed_twd` 或 `notional_rate` |
| `initial`, `maintenance` | 開盤已公告生效的原始／維持保證金金額或比率 |
| `settlement_initial`, `settlement_maintenance` | 結算時已公告生效的金額或比率 |
| `known_at`, `effective_at` | 明確時區時間，均不得晚於該日 08:45 |
| `settlement_known_at`, `settlement_effective_at`, `settlement_time` | 結算規則公告、生效與該商品結算時鐘 |
| `position_group`, `position_unit`, `position_limit` | 同標的合併群組、標準契約換算單位、歷史帳戶限額 |
| `upper_limit`, `lower_limit` | 已考慮商品、日期及契約調整的價格界線 |

同目錄 `manifest.json` 必須包含 `dataset=taifex_futures_margin_rules`、`schema_version=1`、
`status=complete`、`point_in_time_verified=true`、`source_daily_sha256`、
`outputs.rules.sha256` 及逐個來源 receipt 的相對 `path`／`sha256`。
驗證器會比對實際來源 bytes、日資料 release、規則檔案及兩個資訊時間界線。
布林旗標本身不構成歷史證據；產生 release 前仍須由原本的資料取得流程完成公告對照。

## 執行與驗證

遠端主專案使用同一入口：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw_futures_v8_margin.yaml --check-data-only
# 完整歷史規則通過、GPU 空閒後：
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py --config configs/markets/tw_futures_v8_margin.yaml
```

訓練輸出設定為 `artifacts/markets/tw_futures_v8_margin/`；準備與驗證紀錄在
`artifacts/markets/tw_futures_v8_margin_preparation/`。沒有啟動保證金正式訓練。

保證金 checkpoint 具有獨立 action／execution contract，不能沿用一般模式 checkpoint 續訓。
仍可依既有因果 fold 與 validation gate 初始化 v5 骨幹。模型沿用 v5 編譯設定；
共用帳本 CUDA Graph 與局部資金融合已通過保證金合成帳戶的一致性驗證，
但尚無完整歷史規則支援的真實保證金 fold 測速，不能挪用一般模式速度作為實測值。

正式 NPZ 透過原本 writer 儲存 `futures_margin_audit` 與欄位名稱、每期整口數和剩餘口數。
保留結算權益、原始／維持占用、實際名目槓桿、追繳、強平、未成交口數及隔夜損益。
負權益保存在稽核欄位。一般報酬序列沿用既有破產吸收態；失敗後的零狀態只代表模擬終止，
不代表券商實體部位已成交平倉，應以失敗日剩餘口數查核。

本機 403 項、遠端 218 項測試通過（包含重疊），其中新增保證金測試 37 項。
涵蓋多空／空手／槓桿、反向成本、跨日和跨批次權益、追繳與負權益、無法平倉、
期滿現金結算、合併部位限制、PIT 與來源雜湊、梯度、NPZ 存讀，以及原有期貨、checkpoint 回歸。

另於本機用 56 個合成日期、兩個背景股票、完整 1,936 個期貨 slot 和 v5 的 22 個時間基底，
在明確設為 CPU 的診斷設定跑完 canonical 訓練／驗證／測試／圖表全流程，
再從 epoch 1 checkpoint 恢復至 epoch 2。兩次 lifecycle 均通過 18 項檢查，
曲線恰為 `[1, 2]`，checkpoint 包含 optimizer、scheduler、RNG 與 scaler 狀態。
此為合成資料流程驗證，沒有證明真實資料訓練、策略報酬或雙 GPU 效能。
