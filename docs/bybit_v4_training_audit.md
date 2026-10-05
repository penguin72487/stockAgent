# Bybit v4 訓練診斷與 v5 修正

檢查日期：2026-09-26。對象是
`artifacts/markets/bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v4`。
本次沒有啟動或續跑正式訓練，也沒有複製來源、重建 panel、發布 snapshot 或改寫 v4 產物。

後續更新：使用者已要求尋找官方缺值或採取不交易規則。ICX 尾段已修復，
HFT/VINE 採公告後的受限退出；請以 [來源修復與 v6 指令](bybit_source_repairs.md)
為目前入口。下文保留本次 v4 稽核當時的缺口狀態，並非宣稱後續未處理。

## 先定義問題：學的是可執行帳戶的淨成長，不是無成本價格預測

v4 實際設定：Bybit USDT 線性永續、00:00 UTC 資料截止、00:05 Kline open
作成交代理、日決策、32 日資訊窗、可留倉、多空與 learned-cash；不是原先的
00:00 零延遲實驗。公共資訊可以來自其他交易所／歷史宏觀／SEC，但持倉、
成交價、成交容量及資金費必須是 Bybit。共 397 個 panel symbols、133 個
輸入（74 個值欄＋59 個 availability 欄），不是舊版全歸零的外部特徵。

令 `E_t` 為交易前 NAV、`q_i` 為帶正負號的合約數量、`P_i` 為成交代理價。
帳本必須同時滿足：

```text
成交金額 = abs(delta_q_i) * P_i
E_next = E_t + sum(q_i * (P_next_i - P_i))
             - sum(q_i * funding_rate_event * event_mark_price)
             - 買入費率 * 買入金額 - 賣出費率 * 賣出金額
next_weight_i = q_i * P_next_i / E_next
可交易金額 <= 1% * 已完成前一 UTC 日的 turnover_USDT
```

資金費改變現金／NAV，不改變合約數量。下一期沒有資料不是今日可知的
平倉指令；gross 上限也不能生成市場沒有的成交容量。
Bybit 的 [資金費公式](https://www.bybit.com/en/help-center/article/Funding-fee-calculation)
以持倉量乘資金費時點 mark price 計算；[USDT 合約 FAQ](https://www.bybit.com/en/help-center/article/FAQ-USDT-Perpetual-and-Expiry-Contracts)
列出的 taker 費率 0.055% 與研究設定相同，但此處固定費率並不證明每個歷史
日期／帳戶等級皆相同。1% 前日成交量是容量代理，不能保證 00:05 的真實流動性。

## 實際結果：先排除驗證重用和重複測試尾段

來源為保存的 `summary.json`、六份 epoch curve、六份 `test_backtest.npz`，
不是圖形估算。稽核的 21 份來源 SHA-256 存在
`artifacts/markets/bybit_perpetual_daily_0005_training_audit/performance.json`。

| Fold | 訓練年 | 驗證年 | 首測試年 | 淨報酬 | 最大回撤 | 日均換手 |
| --- | --- | --- | --- | ---: | ---: | ---: |
| 1 | 2020 | 2021 | 2022 | -65.91% | -67.24% | 0.536 |
| 2 | 2020–2021 | 2022 | 2023 | -4.30% | -25.09% | 0.393 |
| 3 | 2020–2022 | 2023 | 2024 | +3.35% | -30.21% | 0.229 |
| 4 | 2020–2023 | 2024 | 2025 | -7.99% | -27.71% | 0.444 |
| 5 | 2020–2024 | 2025 | 2026 至 09-24 | +28.15% | -32.12% | 0.568 |
| 6 | 2020–2025 | 2026 | **同一 2026 區間** | +222.58% | -24.58% | 0.678 |

Fold 6 是已設定的最新年實驗，不是獨立測試。排除它，只串接各 fold 的第一年
得到 -60.24%、最大回撤 -77.52%。這仍是**各 fold 重設帳戶的報酬串接**，
沒有跨模型轉倉、平舊倉費用或連續 NAV 的容量重播，不是實盤部署曲線。
本次已檢視上述測試年，後續據此修改屬研究迭代，不能再稱它們完全未接觸的 holdout。

### 不是只要降低手續費就會好

保留保存路徑，把 `0.00055 * turnover` 加回 simple return，再取 log：
2022 年仍為 -62.02%，原本 -65.91%。這只是固定路徑的費用敏感度，不是無費用
重新執行；真實重跑會改變 NAV、容量、持倉與 allocator。
2022 日均執行 gross 0.683、net long 0.451；虧損主要不能由交易費單獨解釋。
「由牛市學到的多頭偏好不能外推熊市」與結果相符，但尚非因果實驗結論。

### 訓練有更新，但泛化不好

實際最佳驗證 epoch 是 105 / 17 / 1 / 75 / 267 / 451；結束 epoch 是
205 / 117 / 101 / 175 / 367 / 551。設定保留 1000 上限，實際都是 100 epoch
沒有驗證改善後 early-stop，不能稱已跑滿 1000。
所有已記錄 epoch 都有一次 trajectory optimizer update。
例如 fold 3 第 1 epoch 已是最佳驗證，之後 train loss 從 +0.151 降至 -10.873，
validation 卻從 -0.348 惡化至 -0.245。不能用訓練 loss 更負或非零梯度當泛化證據。
本次不改測試選模、不添加 top-K／固定多空配額、不任意延長 patience。

此外，既有 panel 的 2020 年只有 BTCUSDT、LINKUSDT、LTCUSDT、XTZUSDT
四個曾可交易合約，平均每天僅 1.29 個；2021 平均 13.83 個，2022 已達
104.67 個（全年曾可交易 123 個）。因此第一個 fold 不只跨牛熊，也面對
資訊／橫截面規模大幅變化。133 維輸入與多基底模型在這種早期小樣本上
過擬合是具體風險；本次沒有藉刪除早年、延後測試起點或篩掉虧損幣別改善報表。
如要擴充早期資料，必須以當時真實上市及可得來源為準，不能使用今天名單回填。

## 已實作的帳本契約 v4（不同於實驗名稱 v4）

1. **絕對成交容量隨 live NAV 換算。** 舊版把相對初始本金的 volume limit
   當成相對當期 NAV 的比例。新帳本攜帶 `equity_scale = NAV / reference_capital`，
   以 `reference_limit / equity_scale` 限制訂單。NAV 翻倍，同一筆 USDT 容量
   的權重上限應減半，不能讓容量也翻倍。訓練、DDP、評估 chunks 及 NPZ
   都傳遞該狀態；NAV 累積採 FP64。
2. **gross 風險縮倉必須受成交規則約束。** 原本在 volume／side mask 後
   再縮整個部位，可能在零容量或禁止減倉時產生假成交。現在先提出風險縮倉，
   再依權限與容量執行；最後只能限制新增曝險。若舊倉漂移超標而不能成交，
   保留真實殘倉、禁止新增，不把無法成交的風險「算掉」。這不是完整維持保證金模型。
3. **取消由下一日缺 label 反推今日強制平倉。** 保留明確 panel force-exit
   的入口，但缺資料本身不產生事件。任一非零持倉缺價格／funding 估值時，
   在 loss/backward 前拋 `CryptoPerpetualDataError`；空倉或 padding 的缺值不影響帳戶。
   真正負資產仍為 absorbing default，與資料錯誤分開。
4. **資料錯誤同步 DDP。** 任一 rank 的訓練／獨立驗證出錯，所有 rank 共同
   退出；清除整條 trajectory 的累積梯度，不提交部分 AdamW 更新。
5. **相容性。** `CRYPTO_PERPETUAL_BACKTEST_CONTRACT_VERSION` 從 3 升至 4，
   checkpoint fingerprint 與 mode contract 一起更新。舊產物保留供診斷，
   不得以新帳本續接 v4 optimizer。舊 NPZ 可讀不代表可在新契約 resume。

## v5 訓練候選：修復特徵量綱，不承諾更高報酬

第一個 fold 的 train-visible 2020 特徵窗中，133 欄非零 RMS 最大／最小相差
179,575 倍。Binance quote-volume、Fed balance-sheet、Binance trade-count 三欄
占 raw squared energy 77.45%。row RMSNorm 只縮放整列，不能移除各欄單位差異。
量測使用 279 個可見日期、505 個 alive symbol-date cells；不是驗證／測試資料。
完整欄位統計、來源 generation/hash 與重現指令在同一 audit root 的
`feature_scaling.json`。

新設定只開啟現成的 `financial_transformer.causal_feature_rms_normalization`：
僅在該 fold 訓練可見資訊擬合，持久化 scale／active mask，resume 精確恢復。
Fold 1 有 9 欄訓練全零，該 fold 後續維持禁用；較後 expanding fold 才可重新擬合。
其餘模型、LR、1000-epoch 上限、early-stop、BF16、full-trajectory 更新、費用、
learned-cash、多空和 00:05 契約保持不變。資料與 panel cache 直接沿用 v4。

這是量綱改善候選，不是報酬已提升的證據。極小 train RMS 也可能放大 OOD 數值；
之後應檢查 train/val 尺度漂移。v5 同時使用修正帳本，不能把 v5 與舊 v4 的
所有變化歸因於正規化；若要歸因，需要新帳本下另設獨立輸出根的 normalizer-off 控制。

## 尚未補齊的來源問題：不能直接宣稱全流程已可完成

既有 v4 cache 排除最後研究尾列後，有三個 `alive & !finite(return)` cell，
共兩天／三個 symbol；明確 panel force-exit 為零：

| 缺完整 forward label 的 row | Symbol | 已查官方事件 | 結論 |
| --- | --- | --- | --- |
| 2026-08-20 | HFTUSDT | 仍需核對精確事件／估值 | 缺值不是 08-20 成交證據 |
| 2026-08-20 | VINEUSDT | 08-19 公告，08-21 09:00 UTC 下架 | 不可把 08-20 00:05 當官方清算 |
| 2026-09-04 | ICXUSDT | 09-16 公告，09-18 09:00 UTC 下架 | 09-04 缺資料早於公告，不能回溯平倉 |

[VINE 公告](https://announcements.bybit.com/en/article/delisting-of-vineusdt-perpetual-contract--art131027cca589/)
及 [ICX 公告](https://announcements.bybit.com/en/article/delisting-of-icxusdt-perpetual-contract--art699d241c959d/)
都規定未平倉按下架前 30 分鐘平均 index price 關閉，不是最後一根日資料價格。
來源修復必須回到 catalog 的可寫 producer，補真實缺 bars／funding 或有時點與
價格證據的清算事件，再走原有驗證／發佈流程；不能改寫 immutable materialization、
填零、刪掉相關幣別、加事後 quarantine 或假裝日內官方結算等於 00:05。
這三筆只出現在 2026，不能拿來解釋 2022 的大跌。新的 fail-closed 帳本若持有
這些合約會停止，所以目前**不宣稱 v5 全折來源 readiness 通過**。

重現缺口（只讀既有 cache，不重建）：

```python
import json, numpy as np
from pathlib import Path
p = Path('artifacts/cache/bybit_perpetual_daily_0005_historical_public_pit_trajectory_v4/panel_cache_v2/generations/1fea51e8c79944c8a4e3d7f127520cdd')
alive = np.load(p / 'alive_mask.npy', mmap_mode='r')
returns = np.load(p / 'returns_1d.npy', mmap_mode='r')
dates = np.load(p / 'dates.npy')
symbols = json.loads((p / 'symbols.json').read_text())
missing = alive & ~np.isfinite(returns)
missing[-1] = False
print([(str(dates[t]), symbols[s]) for t, s in np.argwhere(missing)])
tradable = np.load(p / 'tradable_mask.npy', mmap_mode='r')
years = dates.astype('datetime64[Y]').astype(int) + 1970
for year in np.unique(years):
    rows = tradable[years == year]
    print(int(year), int(rows.any(axis=0).sum()), float(rows.sum(axis=1).mean()))
```

## 使用者執行與驗收

重算診斷，不載模型、不訓練：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_crypto_training.py \
  --root artifacts/markets/bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v4 \
  --output artifacts/markets/bybit_perpetual_daily_0005_training_audit/performance.json
```

來源問題補齊並重新驗證後，新的正式入口如下；本次沒有執行：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/markets/bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v5.yaml
```

新根首次沒有 checkpoint，自然 fresh start；之後只續跑同契約 v5。
不要以 `--no-resume` 重跑已有進度，也不要將 v4 checkpoints 複製過來。
不可僅因新版回報資料錯誤而改回「缺值自動平倉」。

驗證涵蓋獨立 signed-quantity/currency NAV oracle、funding／費用、容量守恆、
blocked-gross、padding、真實違約與缺資料區分、分段值／梯度、canonical eval chunks、
NPZ round trip、checkpoint 舊契約拒絕，以及真實兩程序 Gloo 的共同失敗。
最後一輪上述相關回歸為 **406 passed / 1 skipped**；跳過的是預設 opt-in 的
CUDA 測試，另以 `STOCKAGENT_TEST_CRYPTO_CUDA=1` 單獨執行為 **1 passed**。
strict CUDA 環境檢查也通過。擴大加入 `test/test_crypto_exchange_scope.py` 時，
結果為 413 passed / 1 skipped / 2 failed；兩項失敗皆為工作區缺少
`configs/markets/okx_1m_venue_only_v1.yaml`、
`configs/markets/binance_1m_venue_only_v1.yaml`，並非 Bybit 帳本斷言失敗，
本次未擴大修改這些其他交易所設定。`py_compile` 與 `git diff --check` 通過。

CUDA 檢查與小型 compiled/eager ledger 對照不是模型訓練，也不構成收益／吞吐改善證據。
正式績效需待使用者重跑，並以相同合法帳本比較完整年度、回撤、曝險、換手與成本。

仍未覆蓋：完整歷史 universe、真實 bid/ask 成交、qty-step/min-notional、日內
mark-price 強平／risk tier／維持保證金，以及跨 fold 真實持倉交接。
報表上的高報酬不能消除這些研究限制。
