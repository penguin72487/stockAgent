# 2026-09-10：第 4 折嘉澤、啟碁與近期官方日報來源修復

後續第 5 折的美律 MKF 調整修復與 v15 入口，見[美律修復紀錄](FUTURES_MKF_TRANSITION_REPAIR_2026-09-10.md)。下列 v14 內容保留為當次驗證紀錄。

遠端 v11 的真正失敗點是第 4 折第 58 個 epoch，`2023-03-10 / RFF:202303 / 3533` 帶有前日多單 1 口，但原資料未支援嘉澤認股權調整。`ChildFailedError` 只是 DDP 子程序失敗的外層訊息；不是 GPU、編譯或隔離日估值錯誤。

v11 的前三折已分別完成 308、147、109 個 epoch，第 4 折保留第 57 epoch 的 last checkpoint 與第 37 epoch 的 best checkpoint。深入驗收另找到兩處真實缺口，最終合併於 v14；使用新來源指紋與輸出目錄，保留原始 checkpoint，不跨來源匯入舊 optimizer。

| 觸發 | 日期／合約 | 根因與修復 |
| --- | --- | --- |
| 使用者的 v11，第 4 折 epoch 58 | 2023-03-10 / RFF:202303 | 補 JFF/RFF → JF1/RF1 官方轉換與調整合約分鐘 |
| 本次 v12 實機驗收，epoch 120 | 2023-11-08 / KGF:202311 | 當時持有空單 1 口，補 KGF → KG1 與六天分鐘 |
| epoch 119 模型完整測試回放的最後一天 | 2026-09-04 / DQF:202612 | 遠端官方補證據清單過舊；補全部近期官方日報，核實當日零成交與結算價 48.6 |

## 來源與實作

依[期交所 2023-02-20 公告](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/3533_20230310.pdf)，2023-03-10 生效時，JFF 既有合約轉為 JF1、RFF 轉為 RF1，乘數分別仍為 2,000、100，既有多空口數一比一轉入。沿用 `tw_stock_futures_transition` 與 carry executor 的持倉轉換，不產生轉倉成交或手續費；調整合約與新標準合約保持不同實體身分。

公告列出五個交割月；原始候選與前日持倉範圍核對後，只有兩種產品的 202303 月需要接續。新增 JF1:202303 與 RF1:202303 自 3 月 10 日至 15 日的全部 8 個合約日：

- 6 個有一般成交的合約日，以永豐 exact contract Tick 共 25 筆重建分鐘，逐日 OHLC 及一般成交量均與完整官方日報相等。
- 2 個合約日由完整官方日報與價差交易腿證明沒有一般成交；沒有用缺資料推定零量。
- 期交所[2023 年 3 月最後結算表](https://www.taifex.com.tw/cht/5/sSFFSP?down_type=1&queryYear=2023&queryMonth=3)記錄 JF1 每口完整價值 1,593,063 元，RF1 為 79,653 元。兩者均包含認股權價值，不能只用 793.13 乘契約乘數。

上述嘉澤資料首先存為不可覆寫的 v4（128 個合約日），SHA-256 為 `0ce43dae9315b0b410e03efe998db14b809c03c2e6e33f307dc88111f38e8ef9`。

後續依[期交所啟碁調整公告](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/6285_20231108.pdf)，補入可由原候選持有的 KG1:202311，自 11 月 8 日至 15 日六個交易日共 700 筆 Tick；逐日 OHLC 與一般成交量全數符合官方。乘數仍為 2,000，口數一比一轉入，不產生轉換成交。[官方 11 月最後結算表](https://www.taifex.com.tw/cht/5/sSFFSP?down_type=1&queryYear=2023&queryMonth=11)的完整每口價值為 278,811 元，不能只算 `137.26 × 2,000`。

最終合約轉換資料為 `artifacts/data_preparation/futures_corporate_transitions_v5_20260910/manifest.json`，SHA-256 `859daa0e0fbf0d1a73075d3769f63563f63e27d380de2b2cce375afde198c2f9`。共 134 個合約日，保留 PL1／CL1／JF1／RF1／KG1 等既有來源。公告、原始 Tick、receipt、官方日報及最後結算 HTML 受 manifest 雜湊約束，載入時重驗原始資料與重建分鐘。

近期缺口則源自遠端官方查核清單標示截至 2026-08-16，實際日報只到 08-11。本機已有通過 producer manifest 雜湊核對的 8、9 月完整官方 CSV。新 `artifacts/data_preparation/futures_carry_evidence_v3_recent_20260910/official_evidence.parquet` 重新核實 08-12 至 09-04 的 **8,425 個合約日**：4,500 個有一般成交、3,609 個零總量、115 個僅價差腿、201 個在完整日報中沒有紀錄。原 400,778 個鍵與 `no_trade_proof_eligible` 標記全數保留，較早 392,353 列不變；沒有用未知來源推定零成交。

近期證據 parquet SHA-256 為 `a366d4d62a6e6d17023ee966a8a7f84073391ee17c6bf261f0fb16770fd24153`，receipt SHA-256 為 `53c2a49e310f1573e4bd7dfd76cb9a68fb9f094af627c200bf20c236926c8da3`。DQF:202612 在 09-04 官方成交量確為 0、結算價為 48.6；當日保留既有口數、零成交，按官方價估值。這不是新增隔離。9 月原始檔可含 09-08，但只接入既有日曆至 09-04 的資料。

`configs/markets/tw_stock_futures_day_trade_0845_carry_v14.yaml` 經 v13／v12 繼承 v11，只更新兩份來源與實驗輸出目錄。維持全部 1,573 個日期、98 個股票特徵、BF16/DDP、1,000 epoch 上限、早停、費稅、整口交易、50% 向上取整容量、08:45／08:46／13:20／13:24／13:30 時鐘，以及使用者核准的隔離日保留持倉與官方結算估值。三個既有精確隔離不變。

`train.py` 沿用 trainer 已寫入的 `futures_data_failure_rank*.json`，在子程序失敗尾端加列本次日期、合約、範圍及原因。只採新寫入且有效的小型 receipt，避免舊錯誤或損壞檔案蓋過本次錯誤。

## 驗證與證據

修復工作目錄為本機與遠端 `artifacts/operations/futures_fold4_transition_repair_20260910/`。

- 嚴格 CUDA 環境檢查通過，沿用兩張 RTX 5090。
- 56 項來源、公司行動、隔離持倉、折隔離與錯誤訊息測試通過；114 項共用 checkpoint／resume 測試通過。修改檔案 Ruff 通過。
- v11 第 4 折 last（epoch 57）與 best（epoch 37）分別完成全部 893 天訓練、242 天驗證、406 天測試區間的唯讀回放，共六次。回放不載入 optimizer，不代表重現原本帶 dropout 的第 58 epoch 更新。
- 以實際來源強制持有 JFF/RFF 各多空 1 口，均正確轉入 JF1/RF1，於到期前後依實際成交／完整結算價值結束持倉。LVF/LIF 的多空隔離估值測試也通過。
- v12 實機驗收通過原 epoch 58，在 epoch 60 保存完整 optimizer 與雙 rank RNG；受控中斷後從 epoch 61 恢復，前 60 列曲線逐位元相同，成功續跑至 119。第 120 epoch 的 KGF 缺口另行保留，不能把此輪標為完成。
- KG1 來源與既有轉換回歸 23 項通過，最終三份真實來源測試 3 項通過（這些重跑項目與上述測試有重疊）。
- 最終 v14 的 epoch 119／37 模型各完成全部 893／242／406 天訓練、驗證、測試回放，共六次。JF1／RF1／KG1、LVF／LIF 隔離，以及 DQF 最後一天零成交估值的實際多空持倉探針均通過。
- 最終 v14 第 4 折已在兩張 GPU 完成 256 個完整 epoch，無資料錯誤。更新期間保持原 1,000 epoch 上限與原早停設定；因最佳驗證結果持續刷新，將此診斷驗收限定於已完整保存的 256 輪，再經同一個 `train.py --epochs 256` 續載收尾完整測試及報表。正式 v14 YAML 的上限仍為 1,000，沒有縮短正式訓練或資料期間。此為有界驗收，不是原 1,000 epoch／早停實驗已完成的宣稱。
- 驗收輸出為 `fold4_acceptance_v14`。收尾與 `verify_final_acceptance.py` 的 checkpoint、曲線、九張累積圖及完成契約檢查已通過，缺漏與非法產物均為零；`final_bound.json` 記錄受控中斷原因與第 256 epoch SHA-256。收尾按 256 上限將 early-stop patience 中繼資料由 100 更新為 26，模型、optimizer、scheduler、scaler 與雙 rank RNG 張量逐一比對相同，沒有額外 optimizer 更新。`final_acceptance.json` 保存完整驗收收據。
- 再次執行同一個有界驗收命令，正確略過已完成且契約相容的第 4 折，新增訓練子程序與重複 epoch 均為零，checkpoint 雜湊不變；`completed_reuse_verification.json` 記錄通過。交付時沒有仍在執行的驗收訓練程序，正式 v14 輸出尚未建立。驗收收據、曲線與圖表已複製回本機工作目錄的 `verified_remote/`。

備份含 v11 原始程式、設定、已完成折證據與 checkpoint；`code.patch` 及 `patched/` 保存遠端增量修改。本機主訓練程式較舊且有其他工作，沒有整份覆寫遠端。

## 仍有的資料邊界

全期間盤點另有 **207 個公司行動事件（230 個實體合約日）**，原始候選可能在事件前持有，但尚未具備受支援的調整證據。這是潛在持倉範圍，並非模型已在這些事件交易或失敗。清單在新來源的 `remaining_unsupported_events.parquet`，訓練輸出也寫入 `futures_carry_physical_contracts.json` 的 `unsupported_pre_event_holding_contract_days`。

沒有新增隔離、刪除日期、強制假成交或取消拒絕條件。實際模型若持倉走到其他未支援事件，仍會停止並留下確切證據；不能把本次修復或單折通過當成所有公司行動已全面補齊。

本次使用前次已完整核實的既有 tw-public materialization 與固定 release；[v11 修復紀錄](FUTURES_QUARANTINED_CARRY_FAILURE_2026-09-10.md)所列舊 cold release 缺 102 個物件的獨立問題仍保留，不以訓練可讀取既有快取宣稱冷儲存完整。

## 遠端入口

在 vastai1T `/root/stockAgent`：

```bash
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_carry_v14.yaml
```

正式輸出為 `artifacts/markets/tw_stock_futures_0845_carry_v14_vast5090`。這是新來源版本的訓練，不能直接接續舊來源的 optimizer；v11 前三折及第 4 折 checkpoint、v12 驗收的第 119 epoch checkpoint 均保留。後續相同 v14 契約可依原 resume 規則續訓。
