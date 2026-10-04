# 台幣期貨保證金模式遠端交付（2026-10-03）

目前資料的財務驗證、遠端來源交付及 fold10 完整訓練驗收已完成。
這是使用者已允許的「排除 54 商品、使用完整可接續期間」研究範圍。

## 範圍與剩餘來源缺口

來源保留 765 代碼，先排除已授權的 54 代碼，對其餘 711 完整整合一次。
其中 **685 代碼、4,180,811 個會計列**通過金融規則、估值、終端事件及
持倉接續驗證。任何期間中間受阻，排除整個物理契約及其轉移連通群組，
不只刪掉受阻日；第一觀察暖身不能冒充有交易帳本的商品。

完整來源仍有 **21,560 個受阻列**，其中 2010-11-01 起為 **9,573 列**。
此數包括互相重疊的金融／部位缺口；不能將各旗標相加當成獨立列數。
完整清冊與逐商品日期範圍見固定收據目錄：

- [來源缺口摘要](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/remaining_source_gap_summary.json)
- [逐列來源缺口](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/remaining_source_gaps.csv)
- [原始資料索取鍵](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/remaining_source_requests.csv)
- [逐商品訓練範圍](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/remote_product_scope.csv)

本次完全沒有可用期間的 26 代碼為 CG1、CH1、CV1、CX1、DK1、DM1、FB1、
FM1、FV1、FY1、GG1、GO1、GW1、IT1、IV1、IZ1、JM2、NA1、NB1、NM1、
NQ1、NX1、OZ1、QB1、QB2、VQ1。這與原先排除的 54 代碼分開。
同一代碼也可能只有部分完整契約期間可用。訓練檔有 685 代碼，不代表
fold10 的訓練／驗證／測試年份各自都有全部歷史代碼。

部位上限使用明示的 `stock_futures_previous_capacity_research_v1`：已知
前值與同群組容量優先；缺乏前值時採政策內的起始研究容量。股數與整口
換算、向下取整及每列假設／時鐘／政策 SHA 保留。它不是完整法定限額。
價格、清算、保證金與契約金融單位沒有使用這個推測權限。

無法確認的早期 CPF 保證金調整日期，以及仍可能持倉的停牌清算價、
自身後繼和未知調整單位，保留原始缺值。既有 FinMind 收據已確認省略的
1,475 個清算鍵／382 日不重抓；若要擴大期間，將逐列清冊交給期交所或
期貨商索取精確商品、月份與日期的原始清算／契約調整檔。
TEJ `Shares per Contract` 可供核對股數，不能代替其他現金／權利成分、
過去可知時鐘或法定上限；未平倉量也不能當作上限。授權來源保留原限制。

從會計關係分開處理剩餘缺口：上限限制可選動作，故可以採明示的研究
風控假設；股數、現金、清算與保證金決定損益及資金是否足夠，必須有
來源或當時適用的精確公式。完整來源中 11,976 個 CPF 受阻列位於
2004-06-01～2008-06-13，早於本次訓練起點；其餘範圍也不能靠延長
訓練或提高資金修正。索取清單保留每個物理月份、精確日期及所缺欄位，
不把第一日至最後日之間全部日期冒充缺口。

| 缺口 | 最快下一步及可接受替代 |
| --- | --- |
| 保證金金額與生效時刻 | 取得期交所／期貨商逐日原始保證金檔；只在已證明的生效區間內沿用同一規則 |
| 調整契約股數、現金、口數轉換 | 用既有結構檔及 TEJ 每口股數核對；缺現金／權利成分時索取完整契約調整紀錄 |
| 自己月份的停牌估值、清算與終端值 | 索取原始清算檔，或按該事件及年代適用的官方公式計算；不重抓已確認省略的 provider 格點 |
| 自己的後繼月份與最後持倉日 | 核對自己的實際觀察、契約轉移原件及原始最後交易日；保留整個相依持倉群組的連續性 |
| 部位上限與可知時刻 | 本次股票期貨已使用研究政策；不能將未知金融單位或非股票商品缺口算成已修正 |

## 已修正的根本問題

統一規則來源曾遺失 47 個標準商品的既有保證金區間。修補的來源是增量
事件，不能刪掉同商品未變更的其他公告。已用保存且核准的 facts／review
重建這一類 interval；只重算 103 個相依代碼、692,155 列，其他帳本保留。
完整來源受阻列由 712,825 降到 21,560，沒有額外 API、OCR 或全帳本重算。

902 個沒有開盤成交的調整列，已有編譯器驗證的前日完整價值、現金及
口數轉換：897 個有來自前身的持倉，5 個有不可進入前身持倉的證明。
發布介面現在可接受其依事前已知組成計算的開盤估值參考；原始成交價
空值、零成交權限及全部帳本數字維持。晚知、錯股數、錯金額與缺持倉
證明均拒絕。此發布介面版本為 2。

另補正缺漏的訓練 manifest：資料槽位契約 6、特徵契約 3、固定輸出
2,816 槽。保留原 manifest／建置收據與新修正收據，金融 Parquet SHA
沒有變動，也沒有重新編譯財務列。

規則工作目錄／範圍回歸 40 passed；最終非成交估值／共用發布回歸
33 passed。這些測試有重疊，分批保留收據，不宣稱 73 個獨立測試。

## 原始資料及遠端建置

本機原始／標準化觀察與規則原件放在
`/root/stockAgent/data_tw_futures/margin_sources/`；固定規則編輯入口為
`artifacts/markets/tw_futures_v8_margin_preparation/all_products_rule_facts/`。
來源清冊 55,853 檔，逐檔 SHA／位元組與官方來源發布清冊在遠端核對通過。
來源發布不代表沒有歷史金融缺口。

來源 snapshot 為
`tw-futures-margin-sources-20261003T053014552134764Z-l0-penguin-507e72306d051d9b`，
來源 manifest SHA 為
`86e0f4134125901f934a5b156a38b140202fed36ae3b9df0fbbfd4c91f178b9c`。
這次重用 55,515 檔；冷發布新傳輸物件約 59.9 MB，沒有重傳全部 4.77 GB。
遠端 `data_tw_futures/margin_sources` 現在是專案內的實體目錄。2026-10-03
依使用者要求搬入專案，目錄 inode 與來源 manifest SHA 保持不變；散落在
`/root/stockagent-futures-margin-20261003-*` 的八個暫存目錄已移入專案封存。

只傳來源及固定 SHA 的程式，在遠端建置帳本及 panel。訓練檔位於
`/root/stockAgent/artifacts/markets/tw_futures_v8_margin_preparation/margin_components_20261003/release/`。
股票來源固定原 tw-public snapshot，原 READY 與未過期 hot lease 均驗證。
來源交付採 SSH 與逐檔清冊核對；不宣稱所有 Syncthing 資料夾全面收斂。

## 訓練設定及操作

沿用共用 FinancialTransformer 與訓練／帳本生命週期，從頭訓練，初始
資金新台幣一億。雙 RTX 5090、DDP、全域 train batch 128、eval batch 16、
CPU 總預算 2（每 rank 1）、編譯總預算 16（每 rank 8），
BF16 AMP，財務計算維持穩定精度。CPU 2 與關閉 fold 隔離已寫入主設定，
原本的 `train.py` 入口直接讀取設定就會使用相同的優化。編譯預算仍為 16。
單 fold 直接 DDP，關閉額外的 fold 隔離父程序。
這是保證金、多空、跨日的日線開盤近似；不強制當日平倉，也沒有模型補滿
1:1 資金。它不是已驗證的逐分鐘成交或實際券商成交策略。

資料範圍依完整歷史來源品質選擇，具有回顧性選擇偏誤；測試、金融規則
可用與投資報酬改善是不同證明。正式報酬仍須看獨立未來期間。

正式工作已依使用者要求停止，checkpoint 與 loss JSONL 均保留到第 148
epoch。程式、設定、原始資料及工作目錄全部在現有專案內；小寫
`/root/stockagent` 是連到 `/root/stockAgent` 的相容連結，兩者是同一份專案。
固定程式位於 `artifacts/runtime/tw_futures_margin/source/`，使用同一個
canonical `train.py`；未覆寫主 checkout 中其他工作。由使用者自行執行：

```bash
cd /root/stockagent
source scripts/runtime_env.sh
export STOCKAGENT_CODE_RELEASE_RECEIPT="$PWD/artifacts/runtime/tw_futures_margin/code-release/release.json"
run_fintech_python artifacts/runtime/tw_futures_margin/source/train.py \
  --config configs/markets/tw_futures_v8_margin_components_capital100m_20261003.yaml \
  --start-fold 10 --max-folds 1 --resume \
  --no-retrain-completed-folds --profile-timing
```

若原本慣用 `coda_runner.sh`，在同一目錄及環境執行即可：

```bash
bash artifacts/runtime/tw_futures_margin/source/coda_runner.sh \
  -c "$PWD/configs/markets/tw_futures_v8_margin_components_capital100m_20261003.yaml" -- \
  --start-fold 10 --max-folds 1 --resume \
  --no-retrain-completed-folds --profile-timing
```

正式產物為 `artifacts/markets/tw_futures_v8_margin_components_capital100m_fold10/`。
最多 1,000 epochs，保留既有 early stopping 與相容 resume，不會重新訓練
已完成的 fold。這是前景指令；需要背景管理時，仍可使用既有共用 GPU
manager。這次已透過 manager 停止工作、釋放 GPU，沒有重新啟動訓練。

loss 圖使用既有 `epoch_curve_every1.png`，每個 epoch 同步更新完整歷史；
timing 圖是 `epoch_timing_every1.png`。兩者位於正式產物內的 `train_2011-…-2020/`
群組。設定固定 `record_epoch_curve: true`、`curve_plot_interval: 1`、
`defer_epoch_curve_plot_until_end: false`、`curve_plot_async: false`，不合併略過
epoch 的渲染請求，也不生成大量獨立版本的 loss 圖。

先前程式包漏了共用根目錄的 `plot_epoch_curves.py`，導致有曲線 JSONL
卻沒有 epoch PNG。已修正 canonical 來源清冊、wheel 模組與精確封裝驗收，
新來源 SHA 為 `d67dc441b1dab91058aa9a4b4b663cdb8adbf8f8ce49a6137d9f940e29298c2a`。
原來源的 1,082 個其他程式檔完全相同，模型、資料 builder、財務帳本及
optimizer 實作沒有改動。已補畫真實 148 epoch 的完整 loss/timing 圖；
繪圖前後 checkpoint 和 JSONL SHA 不變。共用 checkpoint 模組確認更新
設定的 optimizer 與交易契約相同，下一次相容續訓從 epoch 149 接續。

- [停止、工作目錄及繪圖驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/stopped_workspace_epoch_plot_acceptance.json)
- [搬移收據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/workspace_relocation.json)
- [舊解壓程式回收收據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/redundant_code_retirement.json)

主設定僅變更 CPU 執行緒預算與單 fold 程序隔離。共用 checkpoint 模組
核對前後 optimizer 及交易契約完全一致；資料、模型、batch、AMP、初始
資金、損益計算及 early stopping 設定沒有改變。驗證收據見
[設定相容性](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/canonical_entry_config_parity.json)。

已完成兩張 GPU 的 fold10 訓練、驗證、完整測試、checkpoint、逐 epoch
曲線、年度圖表、持倉接續重播及根目錄報表；續訓到第 4 個 epoch，前
三個曲線 JSONL 紀錄完全保留，18 項必要產物檢查均通過；這份舊驗收未
覆蓋每 epoch PNG 的漏項。正式工作已停止在保存的 epoch 148；先前執行
觀察與 epoch 計時記錄於
[正式工作觀察](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/formal_execution_observation.json)
，這是歷史執行觀察，不是目前工作仍在執行的證明。

原入口 `--check-data-only` 已退出 0，接受既有 panel 的 3,886 個日期、
2,754 個股票 context symbols、99 個特徵及選定的單 fold；這一檢查沒有
啟動模型或覆寫正式 checkpoint。完整證據見
[原入口驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/canonical_launch_acceptance.json)。

- [原入口資料檢查記錄](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/canonical_entry_data_check.log)
- [更新主設定後的完整短跑復驗](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/canonical_entry_acceptance.json)
- [完整遠端驗收收據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/smoke4_verified.json)
- [計時及數值核對](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/performance_and_parity.json)
- [原 3 epoch checkpoint 基準](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/current/smoke3_checkpoint_baseline.json)

原隔離模式已完成三個 epoch 的訓練與測試，但 CPU 總報表超過十分鐘仍
未完成；保存 checkpoint 後停止本次測試程序，改用既有的直接 DDP 續訓。
沒有修改其他工作的程序、刪除 checkpoint 或捏造原根目錄完成狀態。

新續訓從 GPU manager 啟動到完整生命週期完成共 **484.78 秒**，包含
載入、訓練、驗證、測試與全部報表。PCA 時間基底及因果 RMS 快取均
實際命中，模型 forward/backward probe 使用 2 次 Inductor cache hit、
0 miss，probe 8.57 秒；epoch 前準備 36.41 秒。根目錄重播及報表仍需
175.10 秒，是目前主要固定成本。

原第 3 epoch 的完整最大 rank 時間 11.56 秒，無新編譯圖。續訓第 4
epoch 是新程序的圖捕捉階段，40.93 秒，不能當作穩態速度或宣稱
計算極限。這些舊 epoch 時間未包括目前要求的同步 loss PNG 渲染；
本次遵照使用者要求停止並交付，未重新訓練來量測新完整 epoch 速度。
原量測是暖快取續訓；也不把原未完成的三 epoch 工作與
新的一 epoch 續訓換算成公平的整體加速倍率。

續訓後最佳驗證仍為 epoch 3，最佳 checkpoint、完整測試帳本及年度
報表 SHA 均未變。單 fold 的 246 個部署日另核對 CPU
接續帳本：日期、模型要求、成交／剩餘口數、逐日報酬及拒絕原因完全
一致。資金彙總 audit 的初始／結算保證金最大浮點差為 2 元，隔夜損益
為 0.0625 元；不宣稱全部 audit 欄位位元一致。

短測試的最佳模型在 2022～2026-09-04 完整測試累積報酬為 -15.64%，
最大回撤 -45.53%。這項交付證明資料及訓練流程可用，尚未證明報酬改善。
