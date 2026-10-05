# 台幣期貨保證金模式：最新資料遠端交付（2026-10-04）

依使用者最新指示，早期 CPF 缺保證金規則期間暫不交易，原始資料及索取
清冊保留。完整 711 代碼來源已在遠端整合並發布；fold10 三個 epoch、
驗證、測試、同步 loss 圖與完整報表已通過驗收，正式訓練尚未啟動。

## 最新資料範圍

| 項目 | 驗證結果 |
| --- | --- |
| 原來源／本次檢查範圍 | 765 代碼／依先前授權排除 54 後的 711 代碼 |
| 發布可訓練期間 | 697 代碼、1,896,494 列、3,867 個日期 |
| 帳務日期 | 2010-11-19～2026-09-04；股票 context 從 2010-11-01 載入 |
| 選定發布的受阻帳務列 | 0 |
| 2010-11-01 後整合來源受阻帳務列 | 0 |
| 整合來源尚受阻列 | 2,899，全部為 CPF，2004-06-01～2008-06-12；不納入交易 |
| 初始資金／動作 | 新台幣一億／整口、多空、保證金、跨日持倉 |

2,899 是目前空部位可達性檢查後仍受阻的帳務座標；原先 **11,964 個
早期 CPF 缺規則索取鍵**保留原值，兩種分母不能混用。CPF 為三十天期
利率期貨。它在後續已知規則期間沒有通過目前 50% 成交量參與率的整口
進場證明，故本次沒有可選帳務生命期；並非用未知早期保證金推估交易。

另外 13 個沒有完整可交易期間的代碼為 CG1、CJ2、CV1、DJ1、DK1、DL2、
DM1、EB1、HD1、IT1、JE2、JH1、JM2，原因包含期間早於暖身起點、整口
不可進場，以及承接群組只能保留為 context。逐商品與逐生命期清冊保留；
697 代碼不表示 fold10 各年份均可交易同一批代碼。

- [遠端發布收據](../artifacts/operations/futures_margin_prepare_20261004/remote_build_acceptance.json)
- [711 商品範圍](../artifacts/operations/futures_margin_prepare_20261004/product_scope.csv)
- [原入口資料檢查](../artifacts/operations/futures_margin_prepare_20261004/data_preflight.log)

## 原始資料與遠端建置

原始行情、規則與清算來源留在
`/root/stockAgent/data_tw_futures/margin_sources/`；新增已接受的規則增量留在
`data_tw_futures/margin_repair_pending/`，核准研究政策留在
`data_tw_futures/margin_repair_inputs/accepted_valuation/`。
來源 manifest SHA 為
`86e0f4134125901f934a5b156a38b140202fed36ae3b9df0fbbfd4c91f178b9c`。
只傳來源、政策及固定 SHA 的程式，由遠端既有準備入口建置。

完整金融帳本只編譯一次。發布時發現空部位承接證明需要保留所有祖先，
已修正祖先閉包並重用同一金融檢查點；沒有第二次重建金融列。
新入口 `--integrate-rule-delta` 沿用共用編譯器、發布器及政策身分檢查。
相關回歸分批為 85、34、8 項通過；部分模組重疊，不相加作獨立測試數。

遠端訓練資料固定在
`artifacts/markets/tw_futures_v8_margin_preparation/margin_components_current/release/`：

| 檔案 | SHA-256 |
| --- | --- |
| `daily/continuous_daily.parquet` | `6e1a8e652a64f1a6cd20a33ddc112beec2daaeefca73f58ffde0d6ba0569560a` |
| `rules/rules.parquet` | `43f7fe780a59d1d8ea5aa0c05f9967acf5a961cec6af68d58920cc4928cb3e20` |

股票 context 重用原管理中的精確 tw-public snapshot；READY、manifest 與
未過期 lease 已驗證。這是既有熱資料可用證明，不是全部冷端物件已同步
到遠端的證明。期貨原始資料、準備目錄、程式與工作目錄都在現有專案。

## 研究假設與相容性

schema 8 明示 `frozen_contract_value_research_v1`：停牌缺清算觀察時凍結
之前完整契約價值，再按來源證明的現金／口數轉換接續；部分掛牌終止
部位依使用者核准，以凍結價值研究結清。這些值不是官方清算或成交。
既有 `stock_futures_previous_capacity_research_v1` 部位上限研究政策維持。
原始行情空值、無成交權限、來源雜湊及逐列研究旗標保留。

採完整連通持倉群組選擇，具有回顧性資料選擇偏誤。本次是日線開盤
近似、保證金研究訓練準備；分鐘／BidAsk 成交、券商加收保證金及獲利
沒有因發布驗證而得到證明。不可使用新來源默認接續舊 148-epoch checkpoint。

## 訓練驗收與啟動

嚴格 CUDA 檢查已通過；原 `train.py --check-data-only` 退出 0，接受
3,886 個股票 panel 日期、2,754 個 context symbols、99 個特徵與 fold10。
雙 RTX 5090 實際完成三個 epoch、驗證、2022～2026 測試、逐 epoch 圖、
checkpoint 與完整報表；共用產物 gate 的 18 個必要檔案全部通過。
模型與 optimizer checkpoint 均可讀且張量有限。實際訓練決策日期為
2011-01-03～2020-12-31，驗證年份為 2021；不是由資料暖身起點推測。
切換到正式程式／主設定後，原入口 `--check-data-only` 再次退出 0，
83.11 秒；正式目錄沒有模型 checkpoint，兩張訓練 GPU 均已釋放。

最後 CPU 持倉重播原本超過十分鐘。堆疊確認在共用整口保證金帳本，
設定每 rank 一個執行緒，但實際消耗約 47 個核心。加入
`OMP_THREAD_LIMIT=1`、`OMP_WAIT_POLICY=PASSIVE`、`KMP_BLOCKTIME=0` 後，
同一完成 fold 的續接、來源檢查、持倉重播與全部根目錄報表在 **138.09 秒**
完成。這是報表續接時間，不能冒充三個 epoch 的完整從頭訓練時間。
失敗／停止日誌保留；沒有重訓已完成 epoch 或跳過報表。

續接前後 checkpoint 與三筆曲線 SHA 完全相同。2022 的 246 個持倉重播
日期，CPU 結果與已保存的 GPU fold 前綴在報酬、周轉、原模型請求、
整口部位與保證金稽核數值**逐項完全相同**。正式啟動指令保留這個原生
執行緒設定；PCA／股票 RMS 的既有快取與新測試結果相同，期貨 RMS
已移入正式位置，三種快取均通過共用身分與內容檢查。

三輪短測試的最佳驗證模型，2022～2026 累積測試報酬為 **-71.00%**，
最大回撤 **-83.66%**。這次驗收證明資料／帳本／模型可訓練，沒有證明
報酬改善；短測試結果與研究估值假設均保留，不能當作已可實盤獲利。

- [完整訓練產物驗收](../artifacts/operations/futures_margin_prepare_20261004/smoke_verified.json)
- [CPU／GPU 持倉重播數值核對](../artifacts/operations/futures_margin_prepare_20261004/report_replay_parity.json)
- [正式入口與舊產物封存](../artifacts/operations/futures_margin_prepare_20261004/runtime_promotion.json)
- [正式入口資料檢查](../artifacts/operations/futures_margin_prepare_20261004/canonical_data_preflight.json)
- [最終遠端可訓練收據](../artifacts/operations/futures_margin_prepare_20261004/final_acceptance.json)

維持雙 GPU DDP、全域 train batch 128／eval 16、BF16 AMP、CPU 總預算
2、編譯總預算 16，以及共用 FinancialTransformer 與訓練生命週期。
每 epoch 同步更新 `epoch_curve_every1.png`、`epoch_timing_every1.png`，
`record_epoch_curve: true`、`curve_plot_interval: 1`、
`defer_epoch_curve_plot_until_end: false`、`curve_plot_async: false`。
程式包包含根目錄 `plot_epoch_curves.py`。

固定程式源 SHA 為
`dcc7ba7297fa7937e72f2f97589f4ce199d658ccf77edb6a3fda9b244d9f2f81`，
1176 個來源檔與 ZIP／wheel 均驗證。程式、收據、設定與正式輸出都在
現有 `/root/stockAgent`；小寫 `/root/stockagent` 是同一個 checkout 的連結。
既有 148-epoch 進度及其他舊 fold 產物按完整檔案清冊封存到
`artifacts/runtime/tw_futures_margin/archive/before_research_valuation_20261004/`，
沒有刪除。新資料契約不續接舊 checkpoint，正式輸出目錄已新建供從 epoch 1 開始。

在遠端沿用原入口執行：

```bash
cd /root/stockagent
source scripts/runtime_env.sh
export OMP_THREAD_LIMIT=1 OMP_WAIT_POLICY=PASSIVE KMP_BLOCKTIME=0
export STOCKAGENT_CODE_RELEASE_RECEIPT="$PWD/artifacts/runtime/tw_futures_margin/code-release/release.json"
run_fintech_python artifacts/runtime/tw_futures_margin/source/train.py \
  --config configs/markets/tw_futures_v8_margin_components_capital100m_20261003.yaml \
  --start-fold 10 --max-folds 1 --resume \
  --no-retrain-completed-folds --profile-timing
```

若慣用 `coda_runner.sh`，先執行相同的 `cd`、`source` 與兩行 `export`，
再使用同一份固定程式的原啟動器：

```bash
bash artifacts/runtime/tw_futures_margin/source/coda_runner.sh \
  -c "$PWD/configs/markets/tw_futures_v8_margin_components_capital100m_20261003.yaml" -- \
  --start-fold 10 --max-folds 1 --resume \
  --no-retrain-completed-folds --profile-timing
```

最多 1000 epochs，保留既有 early stopping。首次從 epoch 1 開始；之後
相同來源與設定的重啟，才會從相容 checkpoint 接續。正式訓練留待使用者啟動。

正式產物維持
`artifacts/markets/tw_futures_v8_margin_components_capital100m_fold10/`。
