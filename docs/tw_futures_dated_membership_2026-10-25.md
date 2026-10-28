# 逐決策商品集合、完整歷史與低市場曝險實驗

## 問題與結論

使用者要求：歷史商品不必在整段期間存在；只要在對應時間可選且有合法成交與帳務依據，
就允許模型自行選擇，並繼續從零訓練、簡單手續費後log utility與既有100回合無改善停止條件。

**375原本就是2026-02-26至2026-09-24「至少一天有可執行日資料」的商品代碼聯集，
不是所有日期的交集。** 這個報表數字沒有反過來決定模型候選集合。
725則是2011以來的歷史財務清冊，包含公司行動調整代碼；它們不會同時存在，
也不代表725個獨立經濟風險。原始資料、財務准入與窗口容量的完整分解見
[歷史清冊報告](tw_futures_historical_universe_2026-10-25.md)。
後續逐725代碼的來源修復、既有FinMind歷史回讀、300分鐘窗口與新排程見
[725來源修復](tw_futures_universe725_recovery_2026-10-25.md)；該輪尚在準備中的狀態
不能改寫本報告已驗證的舊發布與四組遮罩數字。

本次將原本已有的逐日期條件集中到
`dated_futures_candidate_mask`，版本為`prior_observed_physical_contract_per_decision_v1`。
訓練、直接資料adapter與新審計共用同一個函式；沒有新增「全期都出現」篩選。
晚加入、提早退出、期間缺席後重現都由當天證據決定；改動未來觀測不能影響之前的候選。
這是明確化現行規則與防止回歸，不能包裝成一個已產生績效提升的新交易機制。

08:45、30分鐘窗口的實際canonical遮罩讀回：測試候選聯集376、全145日共同候選234，
**142個只有部分日期可選，仍照日期保留**；每日候選297～342。
全歷史有724個代碼至少一天可選，全歷史共同候選為0。
與375差的一個是LR1：測試有14列財務來源，但都無日可執行成交。
它可留在模型候選／估值脈絡，帳本仍禁止憑空入場。
這也說明不應把候選mask直接當成成交mask。

四組真實canonical attach全部完成；候選mask的全歷史／測試聯集均為724／376，
測試共同候選均為234、部分期間候選均為142。完整帳務adapter後的一口入場代理為：

| 決策與窗口 | 全歷史至少一次候選且有一口入場容量 | 測試至少一次 |
| --- | ---: | ---: |
| 08:45，1分鐘 | 408 | 277 |
| 09:00，1分鐘 | 515 | 322 |
| 08:45，30分鐘 | 671 | 362 |
| 09:00，30分鐘 | 684 | 367 |

這比舊清冊審計的窗口／前日量代理更接近實際帳務，但仍不是實際成交數：
本次另外走過財務／候選條件，尚未套用當次帳戶資金、實際模型下單及方向限制。
也不能把724候選說成724曾可整口交易。每組聚合證據在新正式root的`coverage/`；
LR1差異另存`candidate-versus-executable-readback.json`。
725清冊中唯一全歷史未成為候選的是DR1：已准入片段僅2011-08-17一日，
沒有同實體契約的前次已觀測模型脈絡；其更完整原始片段另有尚未解決的財務延續缺口，
不是要求它存活整段訓練期。

## 從第一性原理分開集合、容量與帳戶

令`U_t`為決策時可選集合。對每個日期和實體契約分別要求：有先前觀測、
與先前為同一實體契約、存在合法參考值、未被財務估值隔離；需要前日容量時，
還要有至少一口的先驗容量。這些條件都不需要未來的商品存活資訊。
全期`union_t U_t`只用於描述研究覆蓋，絕不廣播回每一天的策略輸入。

選入`U_t`也不代表當天可成交：決策後必須真的觀測成交，並通過窗口量、參與率取整、
價格界限、保證金及組合約束。沒有入場成交時，模型仍可能需要辨認、估值及管理已持有契約。
因此不能為了讓候選與成交數相同就清除持倉，或用沿用的開盤參考價製造成交。

完整財務來源仍採已驗證的持倉連通組件；這一層是回溯性的研究准入，
不是已完成歷史point-in-time財務資料修復。KA1／PZ1／RR1／SQF的近期缺口，
也不能靠移除期間交集解決。特徵發布本身仍標記`research_only`與
`historical_point_in_time=false`；單純向後移一日不能證明原始基本面發布時鐘。

## 目前績效證據與可檢驗假說

以既有20組完整結果、共同145個測試日期比較，當前最高Sharpe為
**名目座標Muon／Notional-coordinate Muon，09:00**：報酬24.28%、Sharpe1.803、
Sortino2.618、MDD12.76%，與TX日報酬相關0.780、beta0.477。
位置：`artifacts/markets/tw_stock_futures_conditioning_history2013_20261023_v3/clock0900__notional_muon/fold_12/`。
較寬商品2020起09:00版本報酬45.72%，但Sharpe1.223、MDD28.25%、相關0.880、beta1.386。
目前沒有一個版本同時在所有指標勝出。TX基準30.72%是rolling1x gross，
不能冒充與策略相同費用、資金與整口限制的可執行帳戶。
完整表與比較曲線在
`artifacts/markets/tw_futures_beta_policy_20261025_v1/COMPARISON.md`。

增加代碼不等於增加獨立資訊。假如各商品報酬主要為`beta_i * market + residual_i`，
學到普遍做多可能提高報酬，卻仍只是在承擔市場風險。新實驗同時利用更完整的歷史年代與
較寬的真實成交窗口，再比較先驗beta中性與有限beta，讓神經網路學習殘差機會。
這不是保證Sharpe提升；硬中性也可能排除真正有資訊的方向訊號。
整口取整、持倉、beta估計誤差仍可能使事後報酬相關顯著，必須另外量測。

## 論文脈絡與採用判斷

下列為本次查核的原始研究，涵蓋基礎到2026-10-23；不是全部文獻的系統性回顧，
也沒有把其他市場的論文數字當成本專案已複現的收益。

| 時間與原始來源 | 有用的機制 | 本次採用／後續條件 |
| --- | --- | --- |
| Cover，1991，[Universal Portfolios](https://onlinelibrary.wiley.com/doi/pdf/10.1111/j.1467-9965.1991.tb00002.x) | 對數財富與再平衡組合的經典起點 | 保留簡單log utility；理論不能直接覆蓋整口、交易成本與保證金 |
| Moody與Saffell，2001，[Learning to trade via direct reinforcement](https://pubmed.ncbi.nlm.nih.gov/18249919/) | 直接最佳化交易績效，納入交易成本 | 維持策略到帳務的端到端目標，避免把預測MSE改善誤當策略改善 |
| Zaheer等，2017，[Deep Sets](https://arxiv.org/abs/1703.06114) | 以集合與排列不變／等變映射處理可變成員 | 支持逐日期集合與共享候選編碼的設計，不需要全期間共同成員 |
| Lee等，2019，[Set Transformer](https://proceedings.mlr.press/v97/lee19d.html) | inducing points讓固定中介數下的集合attention線性隨成員數成長 | 現有market queries已使用壓縮交互；不無條件改成完整二次attention |
| Guijarro-Ordonez等，2021，[Deep Learning Statistical Arbitrage](https://arxiv.org/abs/2106.04028) | 因子殘差、訊號與交易政策串接 | 支持低市場曝險對照；目前仍須用相同帳務檢驗成本後結果 |
| [Machine Learning and the Implementable Efficient Frontier](https://academic.oup.com/rfs/article/39/10/3035/8524346)，RFS2026；本次可讀[工作稿](https://www.aeaweb.org/conference/2024/program/paper/SYTF3aaz)頁面版本為2022-08-12 | 交易成本會改變可實現的組合與學習問題 | 用帳戶實現的成本後效用選模型；不把工作稿當2026正式全文 |
| 2026-09-28，[Universal Dynamic Portfolios](https://arxiv.org/abs/2609.34643) | 對數損失的動態比較者、Dirichlet Hedge與fixed share | 可作未來regime／專家切換對照；其凸組合假設不能直接移植整口期貨帳務 |
| 2026-10-19／20，[Amortized Neural Filtering for Calibrated Posteriors in Statistical Arbitrage](https://arxiv.org/abs/2610.25802) | 保留殘差狀態估計的不確定性，攤銷一次前向推論 | 待本輪資料與曝險對照完成後，再獨立測校準與倉位；韓股證據不等於台股期貨獲利保證 |
| 2026-10-23，[From pixels to profits](https://link.springer.com/article/10.1007/s11408-026-00507-x) | 因子、殘差與交易成本影響機器學習套利表現 | 不為圖像表示增加轉換開銷；先確認是否含新增、可交易的資訊 |

## 四組新實驗與對照

新設定：`configs/ablations/tw_futures_dated_membership_20261025_v1.yaml`。
各組從零初始化、d32、lookback32、global batch128、Muon、單一手續費後log utility，
最多1000回合、連續100回合未超過min_delta0.0001則停；不載入舊模型，不改成64回合。
四組均使用完整已准入2011歷史及決策後30分鐘實際觀測VWAP代理，保留1分鐘延遲。

| case後綴 | 中文／英文 | 決策 | 先驗beta名目曝險上限／NAV |
| --- | --- | --- | ---: |
| `union_beta0_0845_30m` | 逐日集合、beta中性／Dated universe, beta neutral | 08:45 | 0 |
| `union_beta025_0845_30m` | 逐日集合、有限beta／Dated universe, bounded beta | 08:45 | 0.25 |
| `union_beta0_0900_30m` | 逐日集合、beta中性／Dated universe, beta neutral | 09:00 | 0 |
| `union_beta025_0900_30m` | 逐日集合、有限beta／Dated universe, bounded beta | 09:00 | 0.25 |

既有historical-universe四組是無beta上限、1／30分鐘與雙決策時間對照；
既有beta-policy六組提供較短2020歷史的對照。這不是完整因子實驗，
例如歷史長度同時改變可用代碼、樣本與市場環境，不能將差異全部歸因於代碼數。
所有新組用相同最後fold14：train2011-03-22至2025-02-25、
val2025-02-26至2026-02-25、test2026-02-26至2026-09-24。
checkpoint只依validation選擇；測試區已被反覆觀察，必須標示研究比較，
不能再稱未觸碰的外部樣本。

排程沿用既有exclusive GPU lease、strict兩GPU CUDA preflight、canonical runner／lifecycle。
新四組先各跑完整4回合工程驗收，包含validation、test、曲線、圖、checkpoint與最終重播；
需非零且有限梯度、沒有穩態重編譯、實際整口多空及正確beta policy packet，
通過者才進正式收斂；失敗保留證據並繼續其他組。

## 計算效率與驗證分母

本次消除同一attach chain內重讀並完整驗證財務規則的工作：先完整驗SHA、來源manifest與
既有財務規則，再以僅限該次呼叫的`ValidatedMarginSource`共用。
所有相依檔案的device／inode／size／mtime_ns／ctime_ns在前後檢查；
回傳獨立manifest與Polars clone。沒有跨程序或跨訓練的未驗證全域快取。
日資料、規則、manifest、收據任一被修改，即使恢復檔案大小及mtime仍會拒絕。

本次332項相關測試通過，涵蓋晚加入／退出／重現、未來資料不回流、
canonical clock裁切、financial mutation、帳務policy、checkpoint manifest與training lifecycle。
遠端另以真實完整資料比較原本重複驗證與共用驗證的A/B/B/A，逐一雜湊候選、特徵、
整數execution、報酬、可執行／持倉mask與benchmark，要求bit相同。
這個測量只涵蓋CPU的期貨資料attach chain，排除stock特徵載入與任何訓練，不能當作epoch加速比。
在現有正式GPU工作持續執行、兩組都使用2條Polars／OpenMP執行緒的同機對照中，
一次ABBA的原本均值105.394215秒、共用驗證79.486021秒，吞吐1.325947倍，
牆鐘時間減少24.582%。所有受檢陣列SHA相同。
這是完整3833日attach chain的量測，沒有省略clock、opening reference或帳務。

| 試次 | 重複完整驗證 | attachment wall秒 | CPU秒 |
| --- | --- | ---: | ---: |
| A1 | 是 | 102.102 | 117.041 |
| B1 | 否，共用同次完整驗證 | 78.596 | 90.782 |
| B2 | 否，共用同次完整驗證 | 80.376 | 89.232 |
| A2 | 是 | 108.687 | 119.170 |

整個ABBA程序的累積peak RSS為14,433,312 KiB；
這個累積值不是各試次獨立記憶體峰值的公平對照。CPU容量准入、原始試次與8類陣列SHA
均保留於operations與coverage receipts。

沿用既有resident CSR、cuSPARSE adjoint、固定形狀compiled模型、帳務CUDA Graph及funding編譯。
相同2013來源先前完整ABBA為熱epoch103.965→27.828秒（3.736倍），
完整4回合650.204→522.328秒（1.245倍）；
已否決的device finite guard僅約0.34%收益、未達3%門檻，繼續關閉。
歷史數字不外推到本次2011來源；新工程測試再報epoch3+ MAX-rank與整個workflow時間。

**仍未證明只剩硬體計算瓶頸。** 非同步stage timer不能直接定位CPU等待，
NVML 100%也不等於SM或Tensor Core有效佔用100%。目前沒有空閒exclusive lease可同步量測
新四組GPU關鍵路徑；工程排程仍保留完整工作流與梯度／重編譯證據。
後續需要相同新來源的同步trace分離CPU gaps、資料搬運、NCCL rank尾端、帳務依賴、
模型forward/backward、optimizer、驗證、繪圖與checkpoint；
對每個仍可改善的段落先建立對照，再採用較快方案。

## 產物與可重現位置

- 資料：`data_tw_index_futures/training_prepared/tw_futures_historical_universe_20261025_v3/`，
  財務資料沿用`tw_all_twd_futures_history2011_20261023_v2/financial/release/`，不再複製來源。
- 新正式產物：`artifacts/markets/tw_futures_dated_membership_20261025_v1/`。
  四個case目錄均為`twd_equity__`加上上表後綴；正式fold為`fold_14/`。
- 實際遮罩與CPU ABBA：同一正式root的`coverage/history_*/actual_membership.json`，
  並保留逐商品與逐日期聚合parquet。
- 排程與精確請求：`artifacts/operations/futures-dated-membership-20261025-v1/`，
  `queue-request.json`、`prelaunch-check.json`、`launch-readback.json`、`progress.json`。
- 工程暫存：`artifacts/benchmarks/tw_futures_dated_membership_20261025_v1/engineering/`；
  每組驗收後沿用canonical retirement helper保留證據、回收已完成測試payload。
- 凍結來源：`artifacts/code_releases/tw_futures_dated_membership_20261025_v1/20261025T055711059385Z-5c188bec7a76/`，
  source SHA `5c188bec7a76a74acdc0f47853c38718e754a8977e156c76f1b4600454c9ff66`。
  較早audit-only失敗版本保留，不覆寫其receipt或失敗紀錄。

本次新的策略績效必須等待正式收斂，不能以資料覆蓋、通過測試或已排程宣稱超越舊模型。

兩次凍結建置留下的精確`/tmp/stockagent-dated-membership-source-*`副本已按既有storage
owner完成逐檔恢復SHA、程序／FD／mmap、設定／服務引用與隔離後複驗，回收6516檔、
161,415,168 allocated bytes。正式產物與冷物件刪除均為0；
完整證據在operations的`temporary-build-input-retirement.json`，
沒有按`cache`或`features`名稱廣泛刪除。
