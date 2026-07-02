# 國立高雄大學資訊工程學系碩士班論文計畫書

Master's Thesis Proposal

## 一、碩士論文計畫書規定

1. 碩士生應於擬畢業口試日前一學期結束前，提出碩士論文計畫書。
2. 指導教授變更時，須重新提出碩士論文計畫書。

## 二、碩士生資料

| 項目 | 內容 |
| --- | --- |
| 姓名 | 請填寫 |
| 學號 | 請填寫 |
| 指導教授 | 請填寫 |
| 系所 | 國立高雄大學資訊工程學系碩士班 |
| 計畫書日期 | 請填寫 |

## 三、研究題目

中文題目：基於Transformer 的台灣股票跨截面投資組合端到端交易系統

英文題目：An End-to-End Transformer-Based Cross-Sectional Portfolio Trading System for Taiwan Equities

## 四、敘述研究題目之定義與基本假設、研究動機、背景說明與文獻縱覽、研究方法及步驟、設備材料、預期結果、時程安排、參考資料

## 壹、研究題目之定義與基本假設

### （一）研究題目定義

本研究以台灣上市與上櫃股票之日頻資料為主要研究對象，建置一套由資料取得、動態股票池建構、Transformer 訊號生成、投資組合權重決策、台股交易規則約束、成本感知回測、整數股數部位換算與模型解釋所組成的端到端交易研究系統。

本研究所稱「跨截面投資組合」不是只預測單一股票漲跌，而是在每一個決策日同時觀察所有可交易股票，輸出各股票的相對分數與目標權重。模型必須回答兩個問題：第一，哪些股票相對更值得持有或放空；第二，在台灣市場制度、成本、資金與可交易限制下，這些分數如何轉換成實際可執行的部位調整。

本研究所稱「端到端交易系統」包含下列層次：

1. 資料層：下載、修復與對齊台股 OHLCV 日頻資料，建置動態面板與逐日可交易遮罩。
2. 表徵層：以 Transformer 對每檔股票的時間序列特徵與同日市場共同狀態進行建模。
3. 權重層：將模型分數轉換為 long-only 或 long-short 權重，並加入新增之最小權重門檻。
4. 交易規則層：把目標權重差額轉為現股、融資、融券、當沖或不交易等候選行為，並受台股制度與標的資格限制。
5. 回測層：使用成本感知張量回測器與整數股數回測器，模擬持倉延續、交易成本、漲跌停單側不可成交與資金限制。
6. 解釋層：分析特徵歸因、時間位置、股票集中度、市場權杖行為與交易決策來源。

目前 stockAgent 專案已完成資料面板、walk-forward 訓練、Transformer portfolio model、成本感知張量回測、台股漲跌停買賣遮罩、可選最小交易權重門檻、整數股數稽核輸出與可解釋性基礎。現股當沖、融資、融券、券源與保證金等台股交易制度尚未完整整合至訓練與回測，將作為本論文後續實作重點之一。

### （二）專案現況與本論文擴充範圍

本研究以目前專案中的 `configs/markets/tw.yaml` 與核心模組為主要基礎。與論文直接相關的設定如下：

| 層次 | 目前專案狀態 | 論文中之定位 |
| --- | --- | --- |
| 市場 | `data.parquet_root: data_yahoo/tw_stocks` | 以台灣股票日頻資料為主要資料來源 |
| 股票池 | `universe_mode: all_daily_symbols` | 使用動態股票集合，避免只看期末存活股票 |
| 可交易遮罩 | `tradable_mode: tw_limit_guard` | 已考慮台股漲跌停造成的不可買或不可賣 |
| 模型 | `transformer_base_portfolio` | 作為主要 Transformer 端到端投資組合模型 |
| 注意力模式 | `attention_mode: market_token` | 以市場權杖壓縮跨股票共同狀態 |
| 回溯視窗 | `lookback: 32` | 使用最近 32 個交易日形成每次決策輸入 |
| 投資方向 | `long_only: false` | 目前支援 signed weights；實務解讀須由交易規則層約束 |
| 交易成本 | `buy_fee_rate: 0.000855`, `sell_fee_rate: 0.003855` | 回測扣除買進與賣出側成本；賣出側含較高成本假設 |
| 最小交易權重 | `min_trade_weight: 0.0` | 基準不啟用小權重門檻；可於敏感度分析中設定門檻作為後處理 |
| 整數股數稽核 | `save_integer_share_daily_weights_table: true`, `save_integer_share_holdings_table: true` | 將連續權重轉換為資金與股數層級的可執行性檢查 |

因此，本論文不會把尚未完成的制度模組寫成既有成果，而是明確區分：

1. 已完成基礎：市場權杖 Transformer、long-short 權重、成本感知回測、漲跌停遮罩、可選最小權重門檻與整數股數稽核。
2. 預計擴充：現股、融資、融券、當沖、券源、擔保成數、交易單位、資金交割與交易類別混合決策。
3. 驗證方式：先以現有日頻端到端流程建立可重現基準，再逐步加入台股制度約束，觀察績效、換手、未成交率與風險是否合理。

### （三）研究範圍與符號

設 $T$ 為交易日數，$S$ 為股票集合大小，$S_t$ 為交易日 $t$ 可交易股票子集合，$F$ 為特徵數，$L$ 為回溯視窗。資料整理為動態面板：

$$
X \in \mathbb{R}^{T \times S \times F}
$$

其中 `tradable_mask` 表示該股票在該日是否具有有效交易資料，`can_buy_mask` 與 `can_sell_mask` 分別表示是否允許增加或減少部位。目前專案在 `tw_limit_guard` 模式下會依台股 10% 漲跌停近似規則建立單側遮罩：漲停日不允許買進增加部位，跌停日不允許賣出降低部位。

模型對每一股票輸出分數 $z_{i,t}$，再經由權重函數轉換為連續目標權重 $w_{i,t}$。交易規則層進一步根據目前持倉、現金、交易資格與市場制度，決定實際執行後權重 $\hat{w}_{i,t}$。

基礎報酬標籤定義為：

$$
y_{i,t+1} = \log\left(\frac{P^{adj}_{i,t+1}}{P^{adj}_{i,t}}\right)
$$

其中 $P^{adj}$ 為調整後價格，用以降低除權息、拆股與公司行動造成的假訊號。若使用收盤資料產生訊號，正式回測解讀須以「收盤後產生訊號、下一可成交時點執行」為主要可執行情境；close-to-close 結果僅作模型能力與敏感度分析，不作為實務可立即成交之宣稱。

### （四）台股交易制度納入方式

台灣集中市場普通交易採 T+2 交割制度[22]；現股當沖係同一帳戶於同一交易日對同種類有價證券進行現款買進與現券賣出的反向沖銷，並限於普通交易或盤後定價交易可沖銷範圍[22][23]。融資融券則只適用於交易所或櫃買中心公告得為融資融券之有價證券，且零股、鉅額交易等特定交易不得融資融券[24]。

本研究將把上述制度抽象為每日逐股的交易限制與狀態變數，而不是只在回測報告中手動扣成本。預計新增或整理的遮罩與狀態包含：

| 變數 | 意義 |
| --- | --- |
| `spot_buy_allowed_mask` | 現股買進是否可行 |
| `spot_sell_allowed_mask` | 現股賣出是否可行，並受既有庫存與交割限制影響 |
| `day_trade_buy_sell_allowed_mask` | 先買後賣當沖是否可行 |
| `day_trade_sell_buy_allowed_mask` | 先賣後買當沖是否可行，並受券源與暫停名單限制 |
| `margin_buy_allowed_mask` | 融資買進是否可行 |
| `margin_short_allowed_mask` | 融券或借券賣出是否可行 |
| `borrow_available_mask` | 券源、借券額度或可借券賣出股數是否足夠 |
| `lot_unit` | 整股、零股或研究中採用之最小交易單位 |
| `cash_state` | 現金、交割應收應付與可用資金 |
| `margin_state` | 融資餘額、融券餘額、保證金與維持率 |

在日頻資料限制下，本論文主實驗仍以隔日部位調整為核心。當沖部分若缺少完整盤中資料，將作為制度模擬或敏感度實驗，不把日頻 OHLCV 所形成的當沖結果誤解為真實盤中策略。若後續取得逐筆或分鐘級資料，則可將當沖行為納入主要實驗。

### （五）基本假設

1. 資料可得性：研究使用公開市場資料，每一特徵僅使用當時已公開且可取得之資訊。
2. 動態股票池：股票可因上市、下市、停牌、漲跌停、資料缺漏或制度限制而進出每日可交易集合。
3. 時間順序：訓練、驗證與測試依日期切分，不隨機打散；標準化參數僅由訓練期估計。
4. 交易成本：基準情境沿用專案台股設定，買進成本為 0.0855%，賣出成本為 0.3855%，另以多組費率、稅負與滑價情境進行敏感度分析。
5. 最小權重門檻：基準設定為 `min_trade_weight = 0.0`，不壓制小權重。門檻僅作為可選後處理與敏感度分析，不進入預設訓練設定。
6. 投資方向：模型可輸出多空 signed weights；但融券、借券與當沖先賣後買須受標的資格、券源、交易規則與帳戶狀態約束。
7. 資金與股數：連續權重是模型決策中間結果，實際績效須再經過現金、交易單位、股價、手續費與整數股數換算。
8. 研究用途：模型結果僅作學術分析，不構成任何投資建議。

### （六）研究問題與假設

本研究欲回答下列問題：

1. RQ1：Transformer 是否能在台灣動態股票池中學得比傳統模型更穩定的跨截面排序訊號？
2. RQ2：市場權杖機制是否能以較低計算成本保留跨股票共同狀態，並改善樣本外投資組合績效？
3. RQ3：加入成本、漲跌停遮罩、整數股數與最小權重門檻後，模型績效是否仍能優於簡單基準？
4. RQ4：若將現股、融資、融券與當沖視為混合交易動作，模型或規則層是否能降低不可成交率、無效換手與資金使用錯配？
5. RQ5：模型的特徵歸因、市場權杖行為與交易決策是否在不同 walk-forward 折次間具有可解釋的穩定性？

對應假設如下：

1. H1：Market-Token Transformer 之樣本外 Rank IC、ICIR 與投資績效優於 MLP、樹模型與 temporal-only Transformer。
2. H2：成本感知目標函數相較純預測損失，可降低無效換手並改善扣除成本後的風險調整績效。
3. H3：最小權重門檻可減少小額噪音交易，使 turnover、交易筆數與整數股數偏差下降；若門檻過高，則會犧牲分散度與訊號利用率。
4. H4：台股交易規則層可將模型權重轉換為更接近實務的可執行部位，並揭露單純 long-short 權重在融券或券源不足時的績效高估程度。
5. H5：若模型真正學得穩定規律，重要特徵、時間位置、股票集中度與市場權杖行為應在不同期間呈現可觀察的一致性。

## 貳、研究動機

台灣股票市場具有交易制度明確、股票數量足夠、散戶參與度高、漲跌停限制、當沖制度、融資融券與借券制度等特性。這些特性使其很適合作為端到端投資組合學習的研究場域，但也使「模型分數」與「可實際交易」之間存在明顯落差。

第一，股票報酬具有低訊噪比、非平穩與市場狀態轉換等特性。若逐股獨立建模，容易忽略同日不同股票之間的資金輪動與相對強弱；若把所有時間與股票 token 直接丟進 full Transformer，計算成本又會隨股票數與視窗長度平方成長。本研究採市場權杖 Transformer，使模型先學習個股時間序列，再以少量市場權杖吸收全市場共同狀態。

第二，預測準確不等於交易可行。模型可能每天給出大量極小權重、過度換手或集中於漲停無法買進的股票。若回測只看連續權重與理想成交，績效容易高估。專案保留可選的 `min_trade_weight` 後處理來分析這類問題；但基準設定不啟用門檻，以免訓練預設額外壓制模型輸出。

第三，台股不是單一交易行為。投資人可使用現股買賣、先買後賣或先賣後買當沖、融資買進、融券或借券賣出等不同方式；每一種方式有不同的標的資格、成本、交割、券源與風險限制。因此，本研究不只研究「今天買哪些股票」，也研究「在制度限制下用哪一種交易類別達成目標曝險」。

第四，深度交易模型若只報告獲利曲線，很難判斷是否依賴資料洩漏、特殊時期或不可交易假設。本研究將 walk-forward 驗證、統計檢定與模型解釋納入主要成果，檢查模型是否穩定、可重現且具備合理金融意義。

## 參、背景說明與文獻縱覽

### （一）資產配置與成長效用

Markowitz 以報酬與共變異數建立平均數－變異數投資組合理論[1]；Kelly 準則以長期資本成長率為核心，對應最大化對數財富期望[2]。Sharpe ratio 提供每單位波動承擔所獲得之超額報酬衡量[3]，Cover 的 universal portfolio 則說明可在不預知最佳固定權重的情況下逐步逼近其成長表現[4]。本研究延續上述觀點，但將權重函數交由深度模型學習，並在目標函數與回測器中加入交易成本、換手、集中度與交易制度約束。

### （二）機器學習與股票報酬預測

Gu、Kelly 與 Xiu 的研究顯示，非線性機器學習可用於高維資產定價與報酬預測[5]。Fischer 與 Krauss 以 LSTM 建模金融時間序列[6]；Bao 等人結合堆疊自編碼器與 LSTM 進行特徵表示與預測[7]。這些方法能處理時間相依性，但若逐股獨立建模，仍較難捕捉同日股票間的相對關係；若只追求均方誤差，也未直接對應跨截面排序與投資組合效用。

### （三）Transformer 與跨股票關聯

Transformer 以自注意力機制處理長距依賴[8]，Temporal Fusion Transformer 進一步結合變數選擇與可解釋多期預測[9]。在股票預測領域，Temporal Relational Ranking 將股票預測視為排序問題，並利用股票間關係建模跨資產影響[10]；TRA 以路由機制處理多種交易型態[11]；MASTER 將市場資訊納入股票 Transformer，以調整特徵與股票關聯[12]。

上述研究顯示個股時間模式與跨股票市場脈絡皆有價值，但大型動態股票池上的完整注意力成本高，且許多研究並未將預測分數落地到具交易規則的投資組合。本研究以市場權杖作為低維瓶頸，並將模型輸出接到台股制度感知的交易決策層。

### （四）端到端投資組合學習與交易限制

Deep Portfolio Theory 探討以深度階層表示進行投資組合建構[13]；Jiang 等人則以深度強化學習直接輸出資產權重[14]。相較先預測、再以固定規則選股的兩階段流程，端到端學習可直接針對淨報酬或風險調整目標最佳化。然而，若回測器未包含交易成本、持倉延續、買賣限制、整數股數與資金狀態，梯度可能偏好不切實際的高頻重平衡。

本研究因此以成本感知張量回測器作為核心，並把台股實務制度轉成可程式化限制。其目標不是宣稱深度模型能消除交易制度，而是讓制度成為模型訓練、選模與回測的一部分。

### （五）時間序列驗證、回測過度配適與統計推論

時間序列資料不宜任意打散。Bergmeir 與 Benitez 討論時間序列預測評估中的交叉驗證問題[15]；Bailey 等人指出大量策略與參數搜尋可能造成回測過度配適[16]，Deflated Sharpe Ratio 可進一步校正選擇偏誤與非正態性[17]；Lo 則分析 Sharpe ratio 在序列相關下的統計性質[18]。本研究採逐年 walk-forward 驗證，測試資料不參與選模，並以日期區塊 bootstrap 或 stationary bootstrap[21]估計模型差異之信賴區間。

### （六）可解釋人工智慧

Integrated Gradients 以從基準輸入到實際輸入的路徑積分估計特徵歸因[19]；SHAP 以 Shapley value 提供一致性的局部解釋[20]。金融應用中，單次解釋容易受噪音影響，因此本研究除報告個別決策，也彙整特徵群組、時間位置、市場狀態與 walk-forward 折次的穩定性；市場權杖與跨股票擾動則用於觀察共同狀態與可能的傳導關係。

### （七）研究缺口與本計畫定位

1. 大型動態股票池中的完整注意力成本高，需要可擴充的跨股票資訊壓縮方式。
2. 預測分數、投資組合權重與實際台股交易制度常分離，需要端到端且規則感知的流程。
3. 台股漲跌停、當沖、融資融券、券源與交割制度會影響可執行性，不能只用理想化 long-short 權重表示。
4. 微小權重容易造成交易噪音，需要研究最小權重門檻對績效、換手與分散度的影響。
5. 深度模型績效與可解釋性常分開呈現，需要將歸因穩定度、制度限制與消融結果共同納入模型驗證。

## 肆、研究方法及步驟

### （一）整體研究架構

本研究分為資料與面板建置、無洩漏時間切分、Transformer 訊號生成、權重建構、台股交易規則決策、成本感知回測、統計檢定與模型解釋八個階段。所有實驗以 YAML 設定檔記錄資料範圍、特徵、模型、交易限制與隨機種子；每一 walk-forward 折輸出 checkpoint、預測、每日權重、整數股數持倉、績效摘要與解釋資料。

| 模組 | 用途 |
| --- | --- |
| `stockagent.data.panel` | 建立全市場 `PanelData`、特徵、報酬與交易遮罩 |
| `stockagent.data.walkforward` | 建立逐年 expanding-window folds |
| `stockagent.training.windowed` | 建立 lazy windowed tensor，避免永久展開所有視窗 |
| `stockagent.models.transformer_base_portfolio` | 實作 Transformer-base portfolio model 與 market-token 模式 |
| `stockagent.training.loss` | 計算 Rank IC、log utility 與投資組合訓練目標 |
| `stockagent.backtest.simulator` | 執行 canonical tensor backtest、成本感知持倉模擬與整數股數稽核 |
| `stockagent.live.signal_engine` | 產生目標權重、目前權重、差額權重與交易建議表 |
| `stockagent.explainability` | 產生特徵歸因、擾動分析與輔助表徵診斷 |

> 圖一（預計）：端到端交易系統流程圖，包含資料、模型、權重、交易規則、回測與解釋。

### （二）資料來源、研究期間與股票池

資料預定涵蓋 2000 年 1 月 1 日至 2025 年 12 月 31 日之台灣股票日頻資料；若 2026 年資料在研究完成前已具足夠長度，則作為額外前瞻測試，不併入主要調參。股票清單以台灣證券交易所與證券櫃檯買賣中心之公開清單為基礎，價格與成交量由 Yahoo Finance 批次取得；每檔股票儲存為獨立 Parquet，再對齊至共同交易日。

每日股票池由 `alive_mask`、`tradable_mask`、`can_buy_mask` 與 `can_sell_mask` 動態決定。後續若加入融資融券與當沖制度，將再串接每日當沖標的、暫停先賣後買標的、融資融券餘額、停止融資融券註記、可借券賣出股數與券差借券費率等公開資料，形成額外交易資格遮罩。

### （三）特徵工程與資料前處理

第一階段使用專案現有、可由 OHLCV 推導且不依賴未來資訊之技術與 K 線結構特徵。價格與成交量變化採對數差分，以降低尺度差異；K 線比例使用當日高低區間正規化。各折的裁切、穩健標準化與缺值統計只以訓練期間估計，再套用至驗證與測試。

| 特徵群組 | 變數 | 意義 |
| --- | --- | --- |
| 價格對數報酬 | `open_logret_1d`, `max_logret_1d`, `min_logret_1d`, `close_logret_1d` | 開、高、低、收相對前一日之對數變化 |
| 成交量 | `trading_volume_logret_1d`, `signed_vol` | 成交量變化，以及以日內方向調整之量能訊號 |
| K 棒實體 | `body_ratio`, `signed_body_ratio`, `delta_body_ratio` | 實體占高低區間比例、方向與日變化 |
| 收盤位置 | `clv`, `clv_centered`, `delta_clv` | 收盤位於日內高低區間之位置及其變化 |
| 上下影線 | `upper_shadow`, `lower_shadow`, `shadow_imbalance` | 上下影線比例與多空不平衡 |

若加入當沖決策，日頻 OHLCV 只能提供粗略的日內高低與收盤資訊，無法完整描述盤中路徑。因此當沖主實驗須額外取得分鐘級或逐筆資料；若資料不足，當沖僅作制度敏感度分析。

### （四）標籤、資料時點與避免前視偏誤

模型的預測任務為當日可交易股票之下一期報酬排序。主要標籤使用下一期報酬並轉換為每日橫斷面排名；同時保留原始報酬供投資組合損失與回測使用。

資料時點原則如下：

1. 特徵截止：僅使用交易日 $t$ 已知資料。
2. 訊號時間：交易日 $t$ 收盤後或指定決策時點計算分數與目標權重。
3. 成交時間：下一交易日或下一個可交易時點；若漲跌停或停牌，保留原持倉並延後調整。
4. 績效時間：由實際成交後之價格區間計算，不把同一價格同時當作訊號與成交依據。
5. 標準化與選模：每折僅使用訓練期統計量與驗證期績效，測試期完全封存。

### （五）Market-Token Transformer

輸入張量形狀為 $[B, L, S, F]$。模型先以線性層投影特徵，加入時間與股票位置資訊；再對每檔股票沿 $L$ 個交易日執行時間自注意力，取得個股基礎表徵 $z_{base}$。為降低完整跨股票注意力成本，本研究由當日可交易股票表徵的 masked mean、standard deviation 與 dispersion 建立市場摘要，再以少量市場權杖讀取全市場資訊，最後由個股對市場權杖進行交叉注意力。

市場脈絡融合可表示為：

$$
z_{i,t} = \text{Norm}\left(z^{base}_{i,t} + \sigma(g_{i,t}) \odot \Delta z^{market}_{i,t}\right)
$$

模型採 RMSNorm、SwiGLU、RoPE 與 PyTorch scaled dot-product attention，以提升數值穩定性與 GPU 效率。市場權杖數、回溯視窗、模型維度與層數由驗證資料選擇。

> 圖二（預計）：Market-Token Transformer 概念架構。

### （六）由分數轉換為權重與可選最小權重門檻

模型對每檔可交易股票輸出分數 $z_{i,t}$。long-only 模式可產生非負權重；long-short 模式則先對分數做橫斷面中心化，再以 L1 正規化控制總曝險。實作中 `trading.portfolio_activation` 預設為 `identity`，也就是不額外做激活函數轉換；若需要回測或推論後處理，可選 `softsign`、`tanh`、`isru`、`erf`、`atan` 與 `gd`：

$$
w_{i,t} =
G \cdot
\frac{\phi(\tilde{z}_{i,t})}
{\sum_{j \in S_t}|\phi(\tilde{z}_{j,t})|}
$$

其中 $G$ 為總曝險預算；基準中 $\phi(x)=x$。無效股票權重固定為零。若實驗啟用最小交易權重門檻，則接著套用：

$$
w^{thr}_{i,t} =
\begin{cases}
w_{i,t}, & |w_{i,t}| \ge \theta \\
0, & |w_{i,t}| < \theta
\end{cases}
$$

目前基準設定為 $\theta = 0$，不做門檻壓制。正值門檻可降低小額噪音交易、減少交易筆數與整數股數轉換誤差，但也可能犧牲分散化與弱訊號。因此本研究將進行 $\theta \in \{0, 0.001, 0.0025, 0.005, 0.01\}$ 的敏感度分析。

### （七）台股交易規則層與混合動作空間

連續權重只描述理想曝險，不能直接代表實際委託。本研究預計設計交易規則層，將每檔股票的目標權重差額 $\Delta w_{i,t}=w^{thr}_{i,t}-\hat{w}_{i,t-1}$ 轉換為候選交易動作：

| 動作 | 說明 | 主要限制 |
| --- | --- | --- |
| `hold` | 不交易，延續既有部位 | 適用於訊號不足、成本過高或不可交易 |
| `spot_buy` | 現股買進 | 現金、可買、交易單位、漲停限制 |
| `spot_sell` | 現股賣出 | 庫存、可賣、跌停限制 |
| `margin_buy` | 融資買進 | 融資標的資格、融資比率、額度、利息 |
| `margin_repay` | 償還融資或降低融資多頭 | 庫存、可賣、維持率 |
| `short_sell` | 融券或借券賣出 | 融券資格、券源、借券費、平盤下賣出限制或例外 |
| `short_cover` | 回補空頭 | 可買、漲停限制、券源歸還 |
| `day_trade_buy_sell` | 先買後賣當沖 | 當沖標的資格、盤中資料、同日反向成交 |
| `day_trade_sell_buy` | 先賣後買當沖 | 當沖先賣資格、券源、暫停先賣後買名單 |
| `cash` | 保留現金 | 資金避險或無可執行標的 |

交易規則層可採兩種實作路線：

1. 規則式後處理：模型只輸出目標權重，系統依成本、遮罩、資金與持倉決定最接近目標權重的可執行交易。
2. 多頭/空頭/交易類別聯合決策：模型同時輸出目標曝險與交易類別分數，再由約束式最佳化或可微近似選擇動作。

第一階段將採規則式後處理，以避免在資料與制度尚未完整前讓模型學到錯誤的交易捷徑。第二階段再評估是否將交易類別納入模型輸出。

### （八）成本感知目標函數與模擬器

投資組合在下一期的淨報酬由持倉報酬扣除買進成本、賣出成本與滑價；若目標交易超過換手上限，則按比例縮小調整。漲停不可買與跌停不可賣時，模擬器維持不可成交之原部位，使持倉狀態跨日延續。

$$
R^{net}_{p,t+1}
=
\sum_i \hat{w}_{i,t} r_{i,t+1}
- c_{buy} TO^{buy}_t
- c_{sell} TO^{sell}_t
- c_{borrow} B_t
- c_{margin} M_t
$$

其中 $B_t$ 與 $M_t$ 分別代表借券與融資相關成本。基礎版本只使用買賣成本；擴充版本再加入融資利息、融券費、借券費、券差借券費、保證金占用與交割資金狀態。

成本感知目標可寫為：

$$
\mathcal{L}
=
-\lambda_u \operatorname{mean}\left[\log(1 + R^{net}_{p,t})\right]
+ \lambda_{rank}\mathcal{L}_{IC}
+ \lambda_{to}TO
+ \lambda_c\sum_i w_{i,t}^2
+ \lambda_{fail}\mathcal{L}_{unfilled}
+ \lambda_{risk}\mathcal{L}_{risk}
$$

其中 $\mathcal{L}_{unfilled}$ 用於懲罰因交易限制而無法達成目標權重的比例。研究將比較純 Rank IC、成本後 log utility、兩階段訓練與加入制度懲罰項的多任務訓練。

### （九）整數股數、交易單位與資金狀態

目前專案已能在測試階段輸出 `integer_share_daily_weights` 與 `holdings`，用以稽核連續權重轉成股數與現金後的差異。正式論文將進一步區分三種層級：

1. 連續權重回測：用於模型訓練與快速比較，檢查訊號是否有效。
2. 整數股數回測：用於檢查資金有限、股價不同與股數離散化造成的績效差異。
3. 台股交易單位回測：預計加入整股、零股、融資融券不得用零股等制度差異，使部位更接近台股實務。

若研究期間無法取得完整逐筆成交資料，交易單位回測仍以保守估計為原則：無法確認可成交者不假設成交，缺少券源者不建立空頭部位，當沖無法確認盤中路徑者不納入主要績效。

### （十）Walk-forward 驗證

採逐年擴張視窗：前 $k$ 年為訓練，下一完整年度為驗證，其後年度為樣本外測試。模型架構與超參數只依驗證資料選擇；每折測試結果分別報告。為避免同一日期在多折重複計入最終資產曲線，另以每折緊接驗證年之第一個測試年度串接成非重疊 out-of-sample 路徑。

> 圖三（預計）：逐年擴張視窗驗證示意。

### （十一）比較模型與策略基準

| 類別 | 模型或策略 | 比較目的 |
| --- | --- | --- |
| 投資基準 | 全市場等權、台灣加權指數或 0050 類指數代理、簡單動能或反轉 | 確認模型是否優於低複雜度策略 |
| 樹模型 | LightGBM、XGBoost | 建立強健的非線性表格資料基準 |
| 前饋網路 | MLP、Tabular ResNet | 比較不顯式建模時間或跨股票關聯的深度模型 |
| 時間模型 | LSTM、TCN、Temporal-only Transformer | 評估單股時間編碼的增益 |
| 跨股票模型 | Axial Transformer、Latent-factor Transformer | 評估不同跨股票資訊壓縮方式 |
| 提出方法 | Market-Token Transformer | 以市場權杖連結個股時間表徵與全市場狀態 |
| 交易規則基準 | 無制度限制、只現股、現股加當沖、現股加融資融券 | 量化台股制度層對績效與風險的影響 |

### （十二）實驗設計、評估指標與統計檢定

| 面向 | 指標 | 目的 |
| --- | --- | --- |
| 預測能力 | Daily Spearman Rank IC、ICIR、方向正確率、Top-minus-bottom spread | 判斷橫斷面排序與穩定性 |
| 投資績效 | 累積報酬、年化報酬、CAGR、超額報酬 | 衡量財富成長與相對基準 |
| 風險調整 | Sharpe、Sortino、Calmar、最大回撤、CVaR | 衡量波動、下行與尾端風險 |
| 交易品質 | 換手率、交易筆數、未成交比例、平均持有檔數、現金占比 | 確認策略可執行性與成本來源 |
| 制度影響 | 當沖使用率、融資使用率、融券使用率、券源不足率、保證金占用 | 評估混合交易動作是否合理 |
| 權重門檻 | 小權重比例、門檻前後曝險差、整數股數偏差 | 評估新增權重門檻的效果 |
| 運算效率 | 每 epoch 時間、推論延遲、峰值 VRAM、吞吐量 | 驗證市場權杖的擴充效益 |
| 解釋與穩定 | 特徵歸因排名相關、折次一致性、狀態差異、跨股票邊穩定度 | 檢查黑箱依賴與泛化 |

模型差異以同一測試日期的配對方式比較，使用 stationary 或 moving-block bootstrap 建立 95% 信賴區間；Rank IC 均值可另以 Newey-West 標準誤檢定。若進行大量超參數或策略比較，將完整記錄試驗數，並報告 Deflated Sharpe Ratio 或相應的多重比較修正。

### （十三）消融與穩健性分析

1. 注意力模式：full、axial、latent、market-token、temporal-only。
2. 權杖設計：靜態或動態權杖；不同市場權杖數；移除 mean、std 或 dispersion。
3. 時間設計：不同 lookback；last-only 與完整時間查詢。
4. 投資目標：Rank IC、log utility、兩階段訓練及多任務組合。
5. 權重門檻：不同 `min_trade_weight` 對績效、換手、持股數與整數股數誤差之影響。
6. 交易制度：無限制、漲跌停遮罩、只現股、現股加當沖、現股加融資融券、完整混合行為。
7. 成本假設：不同手續費、證交稅、滑價、借券費、融資利率與融券成本。
8. 資料偏誤：有無下市候選、有無漲跌停遮罩、close-to-close 與可執行標籤。

### （十四）模型解釋與案例分析

本研究針對代表性測試日與股票產生 Integrated Gradients、遮蔽擾動與 SHAP surrogate 解釋，彙整至價格動量、成交量、K 棒實體、收盤位置、影線等特徵群組。市場權杖部分則記錄股票對權杖及權杖對股票的注意力，並以 momentum、gap、volume、volatility、liquidity 等擾動測試跨股票分數變化。

交易規則層也需解釋。若模型想放空某檔股票但融券不可用，系統應能指出實際動作為不交易、現股賣出既有庫存、等待券源或改以其他股票表達空方曝險。若權重因低於門檻被歸零，報告也應標示該決策是「模型訊號不足」而非資料缺失。

解釋將檢驗：

1. 特徵重要度在折次間的 Spearman 相關。
2. 多頭與空頭決策是否具有對稱或不對稱依據。
3. 多頭、盤整、空頭市場狀態下之歸因變化。
4. 交易類別選擇是否主要由制度限制、成本或模型訊號驅動。
5. 移除高重要特徵或關閉交易制度模組後，績效是否如預期改變。

### （十五）實作步驟

1. 整理台灣股票清單、下載紀錄、Parquet 資料品質報告與資料快照。
2. 完成動態面板、特徵、報酬標籤、漲跌停買賣遮罩與無洩漏測試。
3. 以現有 `min_trade_weight` 建立權重門檻敏感度實驗。
4. 建立全市場等權、簡單動能/反轉與樹模型基準。
5. 訓練 temporal-only、latent、market-token 等 Transformer 模型，完成 walk-forward。
6. 完成整數股數回測與連續權重回測差異分析。
7. 蒐集或串接當沖、融資融券、券源、暫停交易類別等公開制度資料。
8. 實作交易規則層，先採規則式後處理，再評估聯合決策模型。
9. 進行成本、制度、權重門檻與注意力模式消融。
10. 產生 Integrated Gradients、SHAP surrogate、擾動分析、狀態分析與折次穩定度報告。
11. 彙整統計檢定、圖表、可重現設定檔與論文。

## 伍、設備材料

### （一）硬體設備

| 設備 | 建議規格 | 用途 |
| --- | --- | --- |
| GPU 工作站 | NVIDIA GPU，建議至少 16 GB VRAM；支援 BF16/FP16 與 Tensor Core | Transformer 訓練、混合精度與大股票池推論 |
| CPU | 8 核心以上 | Parquet 讀取、特徵運算、資料對齊與回測前處理 |
| 記憶體 | 至少 32 GB，建議 64 GB | 完整動態面板、快取與多程序資料處理 |
| 儲存空間 | NVMe SSD 至少 1 TB | 原始資料、Parquet、模型 checkpoint、權重與圖表 |
| 備份 | 外接硬碟或校內、雲端儲存 | 保存原始資料快照、設定檔與最終實驗產物 |

### （二）軟體與套件

| 類別 | 內容 |
| --- | --- |
| 作業環境 | Linux 或 Windows 11 + WSL2；Conda/Mamba 環境 |
| 程式語言 | Python 3.x |
| 深度學習 | PyTorch、CUDA、BF16 AMP、`torch.compile`、scaled dot-product attention |
| 資料處理 | NumPy、Numba、Polars、PyArrow、Parquet、PyYAML |
| 比較模型 | LightGBM、XGBoost；必要時使用 scikit-learn 或 cuML |
| 資料取得 | Yahoo Finance 下載器、TWSE/TPEx 公開股票清單、交易日曆、當沖標的、融資融券與借券資料 |
| 解釋與視覺化 | Integrated Gradients、SHAP surrogate、UMAP、Matplotlib、Datashader |
| 版本管理 | Git；每折輸出 JSON/Parquet 與環境鎖定檔 |

### （三）制度資料需求

| 資料 | 用途 |
| --- | --- |
| 每日可當沖標的與暫停先賣後買名單 | 判斷當沖候選動作是否可用 |
| 融資融券標的、餘額、停止融資/融券註記 | 判斷融資買進與融券賣出是否可用 |
| 可借券賣出股數與借券費率 | 判斷空頭部位與先賣後買當沖之券源成本 |
| 交易日曆與休市資料 | 對齊 T+2 交割、持倉延續與缺值 |
| 交易單位、零股規則與股價檔位 | 將連續權重轉為更接近實務的委託數量 |
| 手續費、證交稅、融資利率、融券與借券成本 | 建立成本敏感度與淨報酬 |

## 陸、預期結果

1. 完成可重現的台灣股票動態面板資料流程，能處理上市、下市、缺值、公司行動、漲跌停與單側不可成交。
2. 建立 Transformer 跨截面投資組合模型，並以 walk-forward 嚴格報告樣本外 Rank IC、風險調整績效與交易品質。
3. 驗證市場權杖能否以較低計算成本保留跨股票資訊；預期其峰值 VRAM 與推論時間低於 full 或 axial 注意力。
4. 系統化評估新增最小權重門檻，預期可降低小額噪音交易、換手率與整數股數偏差，但須找出不犧牲績效的合理門檻。
5. 完成台股交易規則層雛形，使模型目標權重能被轉換為現股、融資、融券、當沖或不交易等候選動作。
6. 量化理想化 long-short 權重與實務受限交易之間的落差，避免高估策略可執行績效。
7. 產生特徵歸因、狀態分析、折次穩定度與交易決策解釋，說明模型決策來源並辨識可能失效情境。
8. 完成論文、程式碼、設定檔、測試、資料字典與實驗產物索引，使結果可由相同資料快照重現。

本研究不以單一高報酬回測作為成功標準。若提出模型未顯著優於強基準，仍可由注意力複雜度、資料偏誤、交易成本、權重門檻、制度限制與解釋分析回答研究問題，形成具有科學價值的負面或限制性結論。

## 柒、時程安排

下列時程以 2026 年 7 月至 2027 年 6 月為草案，可依指導教授與實際口試日期調整。

| 工作項目 | 115/7 | 115/8 | 115/9 | 115/10 | 115/11 | 115/12 | 116/1 | 116/2 | 116/3 | 116/4 | 116/5 | 116/6 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 文獻蒐集與研究設計 | ● | ● |  |  |  |  |  |  |  |  |  |  |
| 股票資料下載與品質檢查 | ● | ● | ● |  |  |  |  |  |  |  |  |  |
| 動態面板、標籤與基礎回測器 |  | ● | ● | ● |  |  |  |  |  |  |  |  |
| 權重門檻與整數股數稽核 |  |  | ● | ● | ● |  |  |  |  |  |  |  |
| 基準模型與 walk-forward |  |  |  | ● | ● | ● | ● |  |  |  |  |  |
| Market-Token Transformer 與消融 |  |  |  |  | ● | ● | ● | ● |  |  |  |  |
| 台股交易規則層實作 |  |  |  |  |  | ● | ● | ● | ● |  |  |  |
| 成本敏感度與統計檢定 |  |  |  |  |  |  |  | ● | ● | ● |  |  |
| 可解釋性與案例分析 |  |  |  |  |  |  |  |  | ● | ● | ● |  |
| 論文撰寫、修訂與口試 |  |  |  |  |  |  |  |  |  | ● | ● | ● |

## 捌、參考資料

[1] H. Markowitz, "Portfolio Selection," The Journal of Finance, vol. 7, no. 1, pp. 77-91, 1952.

[2] J. L. Kelly, Jr., "A New Interpretation of Information Rate," Bell System Technical Journal, vol. 35, no. 4, pp. 917-926, 1956.

[3] W. F. Sharpe, "Mutual Fund Performance," The Journal of Business, vol. 39, no. 1, pt. 2, pp. 119-138, 1966.

[4] T. M. Cover, "Universal Portfolios," Mathematical Finance, vol. 1, no. 1, pp. 1-29, 1991.

[5] S. Gu, B. Kelly, and D. Xiu, "Empirical Asset Pricing via Machine Learning," The Review of Financial Studies, vol. 33, no. 5, pp. 2223-2273, 2020.

[6] T. Fischer and C. Krauss, "Deep Learning with Long Short-Term Memory Networks for Financial Market Predictions," European Journal of Operational Research, vol. 270, no. 2, pp. 654-669, 2018.

[7] W. Bao, J. Yue, and Y. Rao, "A Deep Learning Framework for Financial Time Series Using Stacked Autoencoders and Long-Short Term Memory," PLOS ONE, vol. 12, no. 7, Art. no. e0180944, 2017.

[8] A. Vaswani et al., "Attention Is All You Need," in Advances in Neural Information Processing Systems 30, 2017.

[9] B. Lim, S. O. Arik, N. Loeff, and T. Pfister, "Temporal Fusion Transformers for Interpretable Multi-horizon Time Series Forecasting," International Journal of Forecasting, vol. 37, no. 4, pp. 1748-1764, 2021.

[10] F. Feng, X. He, X. Wang, C. Luo, Y. Liu, and T.-S. Chua, "Temporal Relational Ranking for Stock Prediction," ACM Transactions on Information Systems, vol. 37, no. 2, 2019.

[11] H. Lin, D. Zhou, W. Liu, and J. Bian, "Learning Multiple Stock Trading Patterns with Temporal Routing Adaptor and Optimal Transport," arXiv:2106.12950, 2021.

[12] T. Li, Z. Liu, Y. Shen, X. Wang, H. Chen, and S. Huang, "MASTER: Market-Guided Stock Transformer for Stock Price Forecasting," arXiv:2312.15235, 2023.

[13] J. B. Heaton, N. G. Polson, and J. H. Witte, "Deep Portfolio Theory," arXiv:1605.07230, 2016.

[14] Z. Jiang, D. Xu, and J. Liang, "A Deep Reinforcement Learning Framework for the Financial Portfolio Management Problem," arXiv:1706.10059, 2017.

[15] C. Bergmeir and J. M. Benitez, "On the Use of Cross-Validation for Time Series Predictor Evaluation," Information Sciences, vol. 191, pp. 192-213, 2012.

[16] D. H. Bailey, J. M. Borwein, M. Lopez de Prado, and Q. J. Zhu, "The Probability of Backtest Overfitting," The Journal of Computational Finance, vol. 20, no. 4, pp. 39-69, 2017.

[17] D. H. Bailey and M. Lopez de Prado, "The Deflated Sharpe Ratio: Correcting for Selection Bias, Backtest Overfitting, and Non-Normality," The Journal of Portfolio Management, vol. 40, no. 5, pp. 94-107, 2014.

[18] A. W. Lo, "The Statistics of Sharpe Ratios," Financial Analysts Journal, vol. 58, no. 4, pp. 36-52, 2002.

[19] M. Sundararajan, A. Taly, and Q. Yan, "Axiomatic Attribution for Deep Networks," in Proceedings of the 34th International Conference on Machine Learning, pp. 3319-3328, 2017.

[20] S. M. Lundberg and S.-I. Lee, "A Unified Approach to Interpreting Model Predictions," in Advances in Neural Information Processing Systems 30, 2017.

[21] D. N. Politis and J. P. Romano, "The Stationary Bootstrap," Journal of the American Statistical Association, vol. 89, no. 428, pp. 1303-1313, 1994.

[22] 臺灣證券交易所，集中市場交易制度介紹，https://www.twse.com.tw/zh/products/system/trading.html。

[23] 臺灣證券交易所，當日沖銷交易專區，https://www.twse.com.tw/zh/products/system/day-trading.html。

[24] 臺灣證券交易所法規分享知識庫，證券商辦理有價證券買賣融資融券業務操作辦法，https://twse-regulation.twse.com.tw/m/LawContent.aspx?FID=FL007121。

[25] 證券櫃檯買賣中心，上櫃股票融資融券餘額，https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/transactions.html。

## 五、簽名

| 角色 | 簽名 | 日期 |
| --- | --- | --- |
| 碩士生 |  | 年　月　日 |
| 指導教授 |  | 年　月　日 |
| 系主任 |  | 年　月　日 |
