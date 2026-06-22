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

中文題目：基於市場權杖 Transformer 與成本感知張量回測之台灣股票跨截面投資組合學習

英文題目：Market-Token Transformer with Cost-Aware Tensor Backtesting for Cross-Sectional Portfolio Learning in Taiwan Equities

## 四、敘述研究題目之定義與基本假設、研究動機、背景說明與文獻縱覽、研究方法及步驟、設備材料、預期結果、時程安排、參考資料

## 壹、研究題目之定義與基本假設

### （一）研究題目定義

本研究以台灣上市與上櫃股票之日頻資料為研究對象，探討如何同時建模「單一股票的時間序列型態」與「同一交易日不同股票之間的市場共同狀態」，並將模型輸出的跨截面分數轉換為可由回測器檢驗的投資組合權重。

研究核心為一套市場權杖 Transformer（Market-Token Transformer）投資組合學習流程：模型先對每檔股票最近 $L$ 個交易日的特徵進行時間編碼，再以少量市場權杖壓縮全市場資訊，最後將市場脈絡回傳至各股票表徵，產生下一期報酬排序分數與目標權重。此流程的重點不是單純預測某一檔股票是否上漲，而是在每一交易日對所有可交易股票進行相對排序與資金配置。

本研究所稱「成本感知」係指訓練、驗證與測試須一致考慮買進成本、賣出成本、換手率、集中度、總曝險、可交易遮罩，以及台灣市場漲跌停造成的單側不可成交限制。若模型只追求預測誤差或排序指標，而未將分數轉換成可執行權重並扣除交易成本，則不能視為完整的投資組合學習。

研究範圍限定於資料工程、模型訓練、投資組合建構、成本感知回測、統計檢定與模型解釋。即時通知、聊天介面、頻道管理與部署監控等服務層功能均不納入本論文問題、方法與實驗。

### （二）研究範圍與符號

設 $T$ 為交易日數，$S$ 為完整股票集合大小，$S_t$ 為交易日 $t$ 實際可交易之股票子集合，$F$ 為特徵數，$L$ 為回溯視窗。資料整理為動態面板：

$$
X \in \mathbb{R}^{T \times S \times F}
$$

並以 `tradable_mask`、`alive_mask`、`can_buy_mask` 與 `can_sell_mask` 描述上市期間、資料有效性、停牌狀態及漲跌停等交易限制。模型對每一股票輸出分數 $z_{i,t}$，再依投資限制轉換為權重 $w_{i,t}$。

基礎報酬標籤定義為：

$$
y_{i,t+1} = \log\left(\frac{P^{adj}_{i,t+1}}{P^{adj}_{i,t}}\right)
$$

其中 $P^{adj}$ 為調整後價格，用以降低除權息、拆股與公司行動造成的假訊號。專案目前已有 close-to-close 標籤與張量化回測流程；正式論文實驗將明確標示訊號產生、成交與報酬計算時點。若使用收盤資料產生訊號，回測解讀須避免把同一收盤價同時視為可觀測訊號與可成交價格；必要時將另建「收盤後產生訊號、下一可成交時點執行」的敏感度實驗。

### （三）基本假設

1. 資料可得性：研究使用公開市場資料，每一特徵僅使用當時已公開且可取得之資訊。
2. 動態股票池：股票可因上市、下市、暫停交易、漲跌停或資料缺漏而進出每日可交易股票池，不以期末存活股票回推歷史，以降低存活者偏誤。
3. 時間順序：訓練、驗證與測試資料依交易日期切分，不進行隨機打散；標準化參數與特徵處理規則僅由訓練期估計，再套用至驗證與測試期。
4. 交易成本：初始參數沿用專案台股設定，買進成本為 0.0855%，賣出成本為 0.3855%，並以多組交易成本與滑價情境進行敏感度分析。
5. 可交易限制：無成交量、停牌、漲停不可買、跌停不可賣等情境以每日遮罩限制下單，不假設所有目標權重均可立即達成。
6. 投資方向：主模型支援 long-short 權重，以檢驗模型是否能學習正向與負向相對訊號；投資解讀將另以 long-only 或受限多空設定作對照，避免把融券可得性視為無條件成立。
7. 研究用途：模型結果僅作學術分析，不構成任何投資建議，亦不保證未來獲利。

### （四）研究問題與假設

本研究欲回答下列問題：

1. RQ1：少量市場權杖能否在完整台灣股票池中有效擷取跨股票共同狀態，並提升樣本外 Rank IC 與 ICIR？
2. RQ2：將股票分數直接映射為權重，並在訓練與回測中納入成本與交易限制，能否提升扣除成本後的風險調整績效？
3. RQ3：market-token 注意力相較 full 或 axial 注意力，是否能在可接受的預測損失下顯著降低 GPU 記憶體、訓練時間與推論延遲？
4. RQ4：模型特徵歸因與跨股票傳導關係，是否在不同 walk-forward 折次與市場狀態下維持可解釋且穩定的模式？

對應假設如下：

1. H1：Market-Token Transformer 之樣本外 Rank IC 與 ICIR 優於 MLP、樹模型與 temporal-only Transformer。
2. H2：成本感知投資組合學習在相同交易限制下，較只最佳化預測誤差的模型具有更佳或更穩定的 Sharpe ratio、Calmar ratio 與最大回撤。
3. H3：market-token 模式之峰值 VRAM 與推論時間低於 full 或 axial 模式。
4. H4：重要特徵、時間位置與市場權杖行為在折次間具有可觀察的穩定性；若不穩定，則可透過解釋性分析指出模型失效來源。

## 貳、研究動機

股票報酬具有低訊噪比、非平穩、厚尾與市場狀態轉換等特性。傳統研究常針對少數股票各自訓練模型，容易忽略市場共同波動、資金輪動與股票間相對強弱；若直接把所有「時間乘股票」視為完整 Transformer token，注意力成本又會隨股票數與視窗長度平方成長，不利於涵蓋每日數百至數千檔的動態股票池。如何以可擴充方式整合個股時間資訊與全市場脈絡，是本研究第一項動機。

第二，良好的預測誤差不必然轉化為良好的投資績效。當模型頻繁改變排名、集中持有少數股票，或忽略漲跌停與流動性時，毛報酬可能被交易成本與無法成交的部位抵銷。因此，本研究不只預測下一期報酬，也把分數轉換、總曝險、換手、集中度、買賣兩側成本與可交易遮罩納入同一實驗流程，直接評估扣除成本後的資產曲線與風險。

第三，金融機器學習極易發生資料洩漏、存活者偏誤與回測過度配適。隨機切分資料會破壞時間順序；只保留期末仍上市的股票會高估歷史可選標的；重複查看測試結果再調參則會使績效失真。本研究採逐年擴張視窗 walk-forward 驗證，明確區分訓練、驗證與測試，並保留動態股票集合與逐日遮罩，使實驗更貼近實際決策。

第四，深度模型即使取得較佳績效，若無法說明哪些特徵、時間點或市場共同狀態影響權重，仍難以建立研究可信度。本研究將整合 Integrated Gradients、SHAP surrogate、遮蔽擾動、注意力流與折次穩定度分析，檢查模型是否依賴單一期間、單一股票或不合理訊號，並將模型解釋視為結果驗證的一部分。

## 參、背景說明與文獻縱覽

### （一）資產配置、成長效用與風險調整績效

Markowitz 以報酬與共變異數建立平均數－變異數投資組合理論[1]；Kelly 準則則以長期資本成長率為核心，對應最大化對數財富期望[2]。Sharpe ratio 提供每單位波動承擔所獲得之超額報酬衡量[3]，Cover 的 universal portfolio 則說明可在不預知最佳固定權重的情況下逐步逼近其成長表現[4]。本研究延續上述觀點，但將權重函數交由深度模型學習，並在目標函數與回測器中明確加入交易成本、換手與集中度。

### （二）機器學習與深度學習於股票報酬預測

Gu、Kelly 與 Xiu 的研究顯示，非線性機器學習可用於高維資產定價與報酬預測[5]。Fischer 與 Krauss 以 LSTM 建模金融時間序列[6]；Bao 等人結合堆疊自編碼器與 LSTM 進行特徵表示與預測[7]。這類方法能處理時間相依性，但若逐股獨立建模，仍較難捕捉同日股票之間的相對關係；若只追求均方誤差，也未直接對應跨截面排序與投資組合效用。

### （三）Transformer、跨股票關聯與市場狀態

Transformer 以自注意力機制處理長距依賴[8]，Temporal Fusion Transformer 進一步結合變數選擇與可解釋多期預測[9]。在股票預測領域，Temporal Relational Ranking 將股票預測視為排序問題，並利用股票間關係建模跨資產影響[10]；TRA 以路由機制處理多種交易型態[11]；MASTER 將市場資訊納入股票 Transformer，以調整特徵與股票關聯[12]。

相關研究說明「個股時間模式」與「跨股票市場脈絡」皆具有價值，但完整跨股票注意力在大型動態股票池上成本高，且不少研究僅輸出預測排名，尚未完整整合台灣市場的成交限制、成本與可執行回測。本研究採市場權杖作為低維瓶頸：由所有可交易股票的聚合統計生成或調整少量市場權杖，權杖先讀取市場，再讓股票讀回市場脈絡。其目的不是宣稱注意力本身為全新機制，而是驗證此架構在台灣完整股票池、嚴格時間切分及成本感知投資組合中的實用性與可擴充性。

### （四）端對端投資組合學習與成本限制

Deep Portfolio Theory 探討以深度階層表示進行投資組合建構[13]；Jiang 等人則以深度強化學習直接輸出資產權重[14]。相較先預測、再以固定規則選股的兩階段流程，端對端學習可直接針對淨報酬或風險調整目標最佳化。然而，若回測器未包含交易成本、持倉延續、買賣側限制與換手，梯度可能偏好不切實際的高頻重平衡。本研究因此使用具狀態的成本感知張量回測器，並比較預測排名損失、投資效用損失，以及兩階段或多任務訓練流程。

### （五）時間序列驗證、回測過度配適與統計推論

時間序列資料不宜任意打散。Bergmeir 與 Benítez 討論時間序列預測評估中的交叉驗證問題[15]；Bailey 等人指出大量策略與參數搜尋可能造成回測過度配適[16]，Deflated Sharpe Ratio 可進一步校正選擇偏誤與非正態性[17]；Lo 則分析 Sharpe ratio 在序列相關下的統計性質[18]。本研究以 walk-forward 作為主切分，測試資料不參與選模，並以日期區塊 bootstrap 或 stationary bootstrap[21]估計模型差異之信賴區間。

### （六）可解釋人工智慧

Integrated Gradients 以從基準輸入到實際輸入的路徑積分估計特徵歸因[19]；SHAP 以 Shapley value 提供一致性的局部解釋[20]。金融應用中，單次解釋容易被噪音影響，因此本研究除報告個別決策，也彙整特徵群組、時間位置、不同市場狀態與 walk-forward 折次的穩定性；市場權杖與跨股票擾動則用於觀察共同狀態與可能的傳導關係。

### （七）研究缺口與本計畫定位

1. 大型動態股票池中的完整聯合注意力成本過高，需要可擴充的跨股票資訊瓶頸。
2. 預測排名、投資組合權重與實際交易限制常分離評估，需要端對端且成本感知的流程。
3. 台灣市場研究常忽略上市下市、漲跌停、無成交與資料時點對齊，需要逐日遮罩與嚴格防洩漏設計。
4. 深度模型績效與可解釋性往往各自呈現，需要將歸因穩定度、狀態分析與消融結果共同納入模型驗證。

## 肆、研究方法及步驟

### （一）整體研究架構

本研究分為資料與面板建置、無洩漏時間切分、模型訓練、權重建構、成本感知回測、統計檢定與模型解釋七個階段。所有實驗以 YAML 設定檔記錄資料範圍、特徵、模型、交易限制與隨機種子；每一 walk-forward 折輸出獨立 checkpoint、預測、每日權重、持倉、績效摘要與解釋資料。

本研究實作以 stockAgent 專案核心模組為基礎：

| 模組 | 用途 |
| --- | --- |
| `stockagent.data.panel` | 建立全市場 `PanelData`、特徵、報酬與交易遮罩 |
| `stockagent.data.walkforward` | 建立逐年 expanding-window folds |
| `stockagent.training.windowed` | 建立 lazy windowed tensor，避免永久展開所有視窗 |
| `stockagent.models.transformer_base_portfolio` | 實作 Transformer-base portfolio model 與 market-token 模式 |
| `stockagent.training.loss` | 計算 Rank IC、log utility、factor generalization 等訓練目標 |
| `stockagent.backtest.simulator` | 執行 canonical tensor backtest 與成本感知持倉模擬 |
| `stockagent.explainability` | 產生特徵歸因、擾動分析與輔助表徵診斷 |

> 圖一（預計）：研究系統流程與主要輸出。

### （二）資料來源、研究期間與股票池

資料預定涵蓋 2000 年 1 月 1 日至 2025 年 12 月 31 日之台灣股票日頻資料；若 2026 年資料在研究完成前已具足夠長度，則作為額外前瞻測試，不併入主要調參。股票清單以台灣證券交易所與證券櫃檯買賣中心之公開清單為基礎，價格與成交量由 Yahoo Finance 批次取得；每檔股票儲存為獨立 Parquet，再對齊至共同交易日。

每日股票池由 `alive_mask` 與 `tradable_mask` 動態決定。`alive_mask` 表示股票在該日具有有效價格；`tradable_mask` 進一步要求可形成有效報酬與交易資訊；`can_buy_mask` 與 `can_sell_mask` 分別處理漲跌停或其他單側無法成交情況。全市場等權基準以當日可交易股票之報酬算術平均建立，另加入外部市場指數或 ETF 作為敏感度比較。

### （三）特徵工程與資料前處理

第一階段使用專案現有、可由 OHLCV 推導且不依賴未來資訊之技術與 K 線結構特徵。價格與成交量變化採對數差分，以降低尺度差異；K 線比例使用當日高低區間正規化。各折的裁切、穩健標準化與缺值統計只以訓練期間估計，再套用至驗證與測試。

| 特徵群組 | 變數 | 意義 |
| --- | --- | --- |
| 價格對數報酬 | `open_logret_1d`, `max_logret_1d`, `min_logret_1d`, `close_logret_1d` | 開、高、低、收相對前一日之對數變化 |
| 成交量 | `trading_volume_logret_1d`, `signed_vol` | 成交量變化，以及以日內方向調整之量能訊號 |
| K 棒實體 | `body_ratio`, `signed_body_ratio`, `delta_body_ratio` | 實體占高低區間比例、方向與日變化 |
| 收盤位置 | `clv`, `clv_centered`, `delta_clv` | 收盤位於日內高低區間之位置及其變化 |
| 上下影線 | `upper_shadow`, `lower_shadow`, `shadow_imbalance` | 上下影線比例與多空不平衡 |

後續擴充實驗可加入估值、獲利能力、負債比、產業別與市場因子，但須確認公告日與可得日，以避免把事後財報資訊回填至尚未公開的日期。擴充特徵只作額外實驗，不影響核心方法對 OHLCV 特徵之比較。

### （四）標籤、資料時點與避免前視偏誤

模型的預測任務為當日可交易股票之下一期報酬排序。主要標籤使用下一期報酬並轉換為每日橫斷面排名；同時保留原始報酬供投資組合損失與回測使用。若使用收盤資料產生訊號，所有可執行投資解讀至少須延後至下一可成交時點。

資料時點原則如下：

1. 特徵截止：僅使用交易日 $t$ 已知資料。
2. 訊號時間：交易日 $t$ 收盤後或指定決策時點計算 $z_{i,t}$ 與目標權重。
3. 成交時間：下一交易日或下一個可交易時點；若漲跌停或停牌，保留原持倉並延後調整。
4. 績效時間：由實際成交後之價格區間計算，不把同一價格同時當作訊號與成交依據。
5. 標準化與選模：每折僅使用訓練期統計量與驗證期績效，測試期完全封存。

### （五）Walk-forward 驗證

採逐年擴張視窗：前 $k$ 年為訓練，下一完整年度為驗證，其後年度為樣本外測試。模型架構與超參數只依驗證資料選擇；每折測試結果分別報告。為避免同一日期在多折重複計入最終資產曲線，另以每折緊接驗證年之第一個測試年度串接成非重疊 out-of-sample 路徑。

> 圖二（預計）：逐年擴張視窗驗證示意。

### （六）比較模型

| 類別 | 模型或策略 | 比較目的 |
| --- | --- | --- |
| 投資基準 | 全市場等權、外部市場指數、簡單動能或反轉 | 確認模型是否優於可解釋且低複雜度的策略 |
| 樹模型 | LightGBM、XGBoost | 建立強健的非線性表格資料基準 |
| 前饋網路 | MLP、Tabular ResNet | 比較不顯式建模時間或跨股票關聯的深度模型 |
| 時間模型 | LSTM、TCN、Temporal-only Transformer | 評估單股時間編碼的增益 |
| 跨股票模型 | Axial Transformer、Latent-factor Transformer | 評估不同跨股票資訊壓縮方式 |
| 提出方法 | Market-Token Transformer | 以市場權杖連結個股時間表徵與全市場狀態 |

### （七）Market-Token Transformer

輸入張量形狀為 $[B, L, S, F]$。首先以線性層投影特徵，加入時間與股票位置資訊；再對每檔股票沿 $L$ 個交易日執行時間自注意力，取得個股基礎表徵 $z_{base}$。為降低完整跨股票注意力成本，本研究由當日可交易股票表徵的 masked mean、standard deviation 與 dispersion 建立市場摘要，再以少量市場權杖對全市場讀取，最後由個股對市場權杖進行交叉注意力。

市場脈絡融合可表示為：

$$
z_{i,t} = \text{Norm}\left(z^{base}_{i,t} + \sigma(g_{i,t}) \odot \Delta z^{market}_{i,t}\right)
$$

其中 $\sigma(g_{i,t})$ 為可學習門控，用以控制市場資訊對個股表徵的影響。模型採 RMSNorm、SwiGLU、RoPE 與 PyTorch scaled dot-product attention，以提升數值穩定性與 GPU 效率。市場權杖數 $M$、回溯視窗 $L$、模型維度 `d_model` 與層數由驗證資料選擇。

> 圖三（預計）：Market-Token Transformer 概念架構。

### （八）由分數轉換為投資組合權重

模型對每檔可交易股票輸出分數 $z_{i,t}$。long-only 模式可用 masked softmax 產生非負且總和為一的權重：

$$
w_{i,t} =
\frac{\exp(z_{i,t}/\tau)}
{\sum_{j \in S_t}\exp(z_{j,t}/\tau)}
$$

long-short 模式則先對分數做橫斷面中心化，再以 `tanh(score)` 產生方向，並以 L1 正規化控制總曝險：

$$
w_{i,t} =
G \cdot
\frac{\tanh(\tilde{z}_{i,t})}
{\sum_{j \in S_t}|\tanh(\tilde{z}_{j,t})|}
$$

其中 $G$ 為 gross exposure budget，$\tau$ 為 softmax 溫度。無效股票權重固定為零，並可加入單一股票上限、產業上限、換手上限與現金部位。主要報告將清楚區分 long-only、long-short 與受限多空三種情境，避免將研究設定誤解為實務可無限制執行。

### （九）成本感知目標函數與模擬器

投資組合在下一期的淨報酬由持倉報酬扣除買進成本、賣出成本與可選滑價；若目標交易超過換手上限，則按比例縮小調整。漲停不可買與跌停不可賣時，模擬器維持不可成交之原部位，使持倉狀態跨日延續。

$$
R^{net}_{p,t+1}
=
\sum_i w_{i,t} r_{i,t+1}
- c_{buy} TO^{buy}_t
- c_{sell} TO^{sell}_t
$$

成本感知目標可寫為：

$$
\mathcal{L}
=
-\lambda_u \operatorname{mean}\left[\log(1 + R^{net}_{p,t})\right]
+ \lambda_{rank}\mathcal{L}_{IC}
+ \lambda_{to}TO
+ \lambda_c\sum_i w_{i,t}^2
+ \lambda_{risk}\mathcal{L}_{risk}
$$

研究將比較三種訓練策略：只最佳化 Rank IC、只最佳化成本後對數效用，以及先以排序損失預訓練再以投資效用微調。所有係數由驗證資料決定，測試期不再調整。

### （十）實驗設計、評估指標與統計檢定

| 面向 | 指標 | 目的 |
| --- | --- | --- |
| 預測能力 | Daily Spearman Rank IC、ICIR、方向正確率、Top-minus-bottom spread | 判斷橫斷面排序與穩定性 |
| 投資績效 | 累積報酬、年化報酬、CAGR、超額報酬 | 衡量財富成長與相對基準 |
| 風險調整 | Sharpe、Sortino、Calmar、最大回撤、CVaR | 衡量波動、下行與尾端風險 |
| 交易品質 | 換手率、集中度、未成交比例、平均持有檔數 | 確認策略可執行性與成本來源 |
| 運算效率 | 每 epoch 時間、推論延遲、峰值 VRAM、吞吐量 | 驗證市場權杖的擴充效益 |
| 解釋與穩定 | 特徵歸因排名相關、折次一致性、狀態差異、跨股票邊穩定度 | 檢查黑箱依賴與泛化 |

模型差異以同一測試日期的配對方式比較，使用 stationary 或 moving-block bootstrap 建立 95% 信賴區間；Rank IC 均值可另以 Newey-West 標準誤檢定。若進行大量超參數或策略比較，將完整記錄試驗數，並報告 Deflated Sharpe Ratio 或相應的多重比較修正，降低只選最佳回測的偏誤。

### （十一）消融與穩健性分析

1. 注意力模式：full、axial、latent、market-token、temporal-only。
2. 權杖設計：靜態或動態權杖；$M \in \{1,2,4,8,16\}$；移除 mean、std 或 dispersion。
3. 時間設計：$L \in \{5,10,20,32,60\}$；last-only 與完整時間查詢。
4. 投資目標：Rank IC、log utility、兩階段訓練及多任務組合。
5. 交易假設：不同成本、滑價、換手上限、單股上限、long-only 與 long-short。
6. 資料偏誤：有無下市候選、有無漲跌停遮罩、close-to-close 與可執行標籤。

### （十二）模型解釋與案例分析

本研究針對代表性測試日與股票產生 Integrated Gradients、遮蔽擾動與 SHAP surrogate 解釋，彙整至價格動量、成交量、K 棒實體、收盤位置、影線等特徵群組。市場權杖部分則記錄股票對權杖及權杖對股票的注意力，並以 momentum、gap、volume、volatility、liquidity 等擾動測試跨股票分數變化，建立候選傳導圖。

解釋不以單一熱圖作結論，而是檢驗：

1. 特徵重要度在折次間的 Spearman 相關。
2. 多頭與空頭決策是否具有對稱或不對稱依據。
3. 多頭、盤整、空頭市場狀態下之歸因變化。
4. 移除高重要特徵後績效是否如預期下降。

若注意力權重與擾動效果不一致，將以擾動結果作為較直接的敏感度證據。

### （十三）實作步驟

1. 建立台灣股票清單、下載紀錄與 Parquet 資料品質報告。
2. 完成動態面板、特徵、報酬標籤與四類遮罩；撰寫資料時點與無洩漏測試。
3. 建立全市場等權與簡單策略基準，驗證成本感知模擬器。
4. 訓練 LightGBM、XGBoost、MLP、TCN 或 LSTM 與 temporal-only Transformer。
5. 訓練 Market-Token Transformer，完成 long-short 與 long-only 對照。
6. 進行 walk-forward、消融、成本敏感度與運算效率量測。
7. 產生 Integrated Gradients、SHAP surrogate、擾動解釋、狀態分析與折次穩定度報告。
8. 彙整統計檢定、圖表、可重現設定檔與論文。

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
| 資料處理 | NumPy、Numba、Polars、PyArrow、pandas、Parquet、PyYAML |
| 比較模型 | LightGBM、XGBoost；必要時使用 scikit-learn 或 cuML |
| 資料取得 | Yahoo Finance 下載器、TWSE/TPEx 公開股票清單 |
| 解釋與視覺化 | Integrated Gradients、SHAP surrogate、UMAP、Matplotlib、Datashader |
| 版本管理 | Git；每折輸出 JSON/Parquet 與環境鎖定檔 |

即時通知與服務部署相關套件不屬於本論文實驗必要環境；實驗將以資料、訓練、回測與解釋性模組為核心。

## 陸、預期結果

1. 完成可重現的台灣股票動態面板資料流程，能處理上市、下市、缺值、公司行動、漲跌停與單側不可成交。
2. 建立包含傳統策略、樹模型、時間模型與多種 Transformer 的完整基準，並以嚴格 walk-forward 報告樣本外結果。
3. 驗證市場權杖能否以較低計算成本保留跨股票資訊；預期其峰值 VRAM 與推論時間低於 full 或 axial 注意力。
4. 提出成本感知的分數至權重流程；預期在合理成本與限制下，較純預測模型取得更佳或更穩定的 Sharpe、Calmar 與最大回撤。
5. 產生特徵歸因、狀態分析、折次穩定度與跨股票擾動圖，說明模型決策來源並辨識可能的失效情境。
6. 完成論文、程式碼、設定檔、測試、資料字典與實驗產物索引，使結果可由相同資料快照重現。

本研究不以單一高報酬回測作為成功標準。若提出模型未顯著優於強基準，仍可由注意力複雜度、資料偏誤、交易成本、模型穩定度與解釋分析回答研究問題，形成具有科學價值的負面或限制性結論。

## 柒、時程安排

下列時程以 2026 年 7 月至 2027 年 6 月為草案，可依指導教授與實際口試日期調整。

| 工作項目 | 115/7 | 115/8 | 115/9 | 115/10 | 115/11 | 115/12 | 116/1 | 116/2 | 116/3 | 116/4 | 116/5 | 116/6 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 文獻蒐集與研究設計 | ● | ● |  |  |  |  |  |  |  |  |  |  |
| 股票資料下載與品質檢查 | ● | ● | ● |  |  |  |  |  |  |  |  |  |
| 動態面板、標籤與回測器 |  | ● | ● | ● | ● |  |  |  |  |  |  |  |
| 基準模型與 walk-forward |  |  |  | ● | ● | ● | ● |  |  |  |  |  |
| 市場權杖模型與消融 |  |  |  |  |  | ● | ● | ● | ● |  |  |  |
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

## 五、簽名

| 角色 | 簽名 | 日期 |
| --- | --- | --- |
| 碩士生 |  | 年　月　日 |
| 指導教授 |  | 年　月　日 |
| 系主任 |  | 年　月　日 |
