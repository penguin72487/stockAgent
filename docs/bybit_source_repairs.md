# Bybit 三個缺口：官方補值與公告後退出

> 後續更正：v7 正式訓練在 2026-08-20 / HFT 遇到受限退出後的未知估值。
> 改用下架前的有界 API 查詢已取得 HFT/VINE 尾段 Kline 與 funding marks；
> 下文「API 回空」只描述當時那組查詢，不能當作歷史來源不存在的證明。
> 正式結算價仍需獨立證據，詳見 [v7 失敗調查](bybit_v7_terminal_valuation_failure.md)。

2026-09-26 實作。沒有啟動訓練，沒有修改原始 release、舊 panel cache 或 v4/v5 訓練產物。
本次使用資料品質流程，把「缺觀測」、「停止開倉」與「既有持倉的估值」分開。

## 修復結果

| 合約 | 查到的原因 | 已處理 |
| --- | --- | --- |
| ICXUSDT | 1m 含 hot tail 已到 09-18，funding 與日資料卻停在 09-04；current-universe 迴圈漏掉已下架合約 | 以官方 API 補 57 筆 funding 及對應 hourly mark，核對 2 筆重疊事件；日資料新增 14 列至 09-18，09-04 forward return 恢復有限值 |
| HFTUSDT | 原始分鐘只到 08-20 22:31；目前官方 Kline／mark API 尾段回空清單 | 不捏造 funding mark 或下架結算價；依 08-19 公告，08-20 起停止開倉並請求平倉 |
| VINEUSDT | 原始分鐘只到 08-20 22:32；目前官方 Kline／mark API 尾段回空清單 | 同上，08-20 起停止開倉並請求平倉 |

官方逐筆 archive 仍列有 HFT/VINE 的 08-20、08-21 檔，已抽樣確認檔頭及首筆，
但成交價不等於 funding mark 或下架結算 index 均價，沒有把它們混用補值。
ICX 的 09-04 缺口沒有被 mask；它先以真實資料修復，之後才依 09-16 公告，
從 09-17 決策開始退出。

官方資料：

- [Funding API](https://bybit-exchange.github.io/docs/v5/market/history-fund-rate)
  與 [Mark Kline API](https://bybit-exchange.github.io/docs/v5/market/mark-kline)。
  ICX 查得 59 筆 funding，全部有同時點的 hourly mark open；實際間隔含 1h 與 8h，
  未合成固定 8h 排程。沿用原資料契約的 hourly mark-open 估值，不宣稱是逐筆清算憑證。
- [HFT 公告](https://announcements.bybit.com/en/article/delisting-of-hftusdt-perpetual-contract--art5f92db89ae63/)、
  [VINE 公告](https://announcements.bybit.com/en/article/delisting-of-vineusdt-perpetual-contract--art131027cca589/)：
  08-19 發布、08-21 09:00 UTC 下架。
- [ICX 公告](https://announcements.bybit.com/en/article/delisting-of-icxusdt-perpetual-contract--art699d241c959d/)：
  09-16 發布、09-18 09:00 UTC 下架。不可回溯用來處理 09-04。

完整 bounded API 回應、URL、查詢時點與 hashes 存在
`artifacts/markets/bybit_perpetual_daily_0005_training_audit/source_probe.json`。
公告只有發布日期，沒有可驗證的日內發布秒數，因此保守地從隔日 00:00 決策才可用。
這是將官方發布日按 UTC 日解讀的研究規則，不是已保存的歷史 first-seen 接收紀錄；
若要求分鐘級公告 PIT 證明，仍需補具時區及實際可得時間的原始證據。
公告 metadata 的 hash 不是原 HTML hash；本機直連公告 HTML 為 403，內容經 web 工具讀取核對。

## Mask 的確切意思

採用公告後退出的研究策略，不是聲稱交易所要求在 00:05 強平：

1. 公告隔日以前不改 policy、標籤或交易權限。
2. 公告可知後，該合約 policy mask 關閉，帳本提出零目標。
3. 用當日真實 00:05 mark、原始買賣權限、live-NAV 換算容量及 turnover cap 執行。
   零目標只略過 proximal dead-zone，不略過任何成交限制。
4. 未成交的殘倉保留、繼續計算價格損益與 funding；有後續可估值日期就再次請求退出。
   **若殘倉仍缺估值，仍拋 `CryptoPerpetualDataError`，不把 NaN 填成零。**

因此 HFT/VINE 的最後 forward label 仍是未知；這次沒有宣稱補齊其完整下架帳本，
也不保證所有 learned policy 都能在公告後一次平完。當天 1% 前日成交額容量代理
約 HFT 22,922 USDT、VINE 11,981 USDT；持倉超過可成交量仍可能阻擋訓練。
不能透過提前到公告前平倉、擴大容量或刪除歷史虧損來迴避。

Crypto ledger contract 升為 **5**；crypto 的既有 `force_exit_mask` 欄在此契約
代表受限的公告退出請求，不能當作無限容量的交易所結算。其他市場語意不變。
PyArrow／Polars、日期切片、panel cache 與 dataset batch 都傳遞同一旗標；
原始來源公告欄只供規則與稽核，不添加新的模型數值特徵。

## 儲存方式：不建立另一套 snapshot

目前 `data_bybit` 是 immutable materialization，且本機沒有完整可寫 producer；
`data_bybit_prepacked_20260926` 是舊 15m／0000 資料，不能拿來覆蓋目前來源。

固定的本機衍生位置為 `artifacts/cache/bybit_perpetual_daily_repaired`：

- `perpetual_daily`：394 個未變動檔案只做指向同一已解析 release 的 symlink；
  3 個受影響日表共 **550,905 bytes**。沒有複製整套分鐘／funding／公共特徵。
- `repair_manifest.json`：完整原日表 hashes、相關分鐘／hot-tail／funding hashes、
  API 證據 hash 與 3 個衍生日表 hashes。重新執行會驗證並重用，不生成時間戳目錄。
- `panel`：v6 執行時使用的可重建 cache；舊 v4 cache 保留不動。

此 view 已在 `crypto_exchange_scope.py` 明確註冊。正式前置檢查驗證相同 Bybit release、
完整原 universe、394 個原檔連結、3 個衍生檔與所有 receipt 列出的來源 hashes
（包括分鐘／hot-tail／funding／官方回應）；不是放寬成任意外部路徑。
它不是 producer、Syncthing 資料夾或新發佈 authority。基礎 materialization 若被切換或
移除，檢查會拒絕，必須重新明確選擇／使用同一 release，不能靜默換成 latest。

ICX 舊 09-04 以前日表值保持不變；重算尾段的 total-return index 銜接舊 09-04
已知的 adjclose，避免把不同指數基準誤認價格變化。原始 funding 和原日表完全不寫入。
公共資訊表仍沿用原 v4；未補的外部特徵以原 availability channel 表示，不假裝一併更新。

## 使用方式（由使用者執行訓練）

補值 view 已在這台機器建立；以下準備指令可驗證並重用它：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/prepare_bybit_daily_repairs.py
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/markets/bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v6.yaml
```

v6 沿用 v5 的訓練窗口 RMS、133 特徵、learned-cash、00:05、費率、BF16/DDP、
trajectory 更新與 1000 epoch 上限；只改修復 view／cache 與新訓練輸出根。
輸出在 `artifacts/markets/bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v6`。
不可搬入舊 optimizer；之後只有同契約 v6 可 resume。本次沒有跑此命令或測量收益改善。

## 防止來源流程再漏掉已下架合約

funding downloader 與 daily materializer 新增 `--historical-instruments PATH`，
且強制搭配明確 `--symbols`。歷史 CSV 只證明合約身分，不把舊 `Trading` 狀態當目前
可交易證據；拒絕同名產品衝突，funding coverage 驗證仍保留。預設 current universe 不變。
已核對 2026-08-23 release 的 `perpetual_daily/symbols.csv` 含完整三幣身分。

這兩個 CLI 的 targeted report 仍描述此次選取子集；未來回到可寫 producer 修補時，
須用 bounded stage 並合併 coverage receipts，不能直接以三幣報表覆蓋全 universe 的證據。
不要把 CLI 的預設輸出直接指向目前 immutable `data_bybit`。

驗收範圍是來源補充、因果公告規則、容量／損益守恆與工程回歸；不是完整歷史交易所
結算、真實 bid/ask 流動性、全 fold 訓練完成或績效改善的保證。

本次驗證：相關 CPU／Gloo 回歸 **502 passed / 2 skipped**；兩項預設 opt-in 的
CUDA compiled/eager 值與梯度測試另行執行，**2 passed**。strict CUDA、v6 設定、
完整 venue／外部特徵 schema／修復 view receipt gate、準備指令重複執行與
`git diff --check` 均通過。沒有建立完整訓練 panel、執行模型 forward 或正式 DDP 訓練。
最後補強全部來源 hash gate 後，32 項 repair／panel 測試及實體 v6 前置檢查再次通過。
另行擴大到 exchange-scope 測試仍有兩項既有失敗：缺少
`okx_1m_venue_only_v1.yaml`、`binance_1m_venue_only_v1.yaml`，未擴大修改其他交易所設定。
