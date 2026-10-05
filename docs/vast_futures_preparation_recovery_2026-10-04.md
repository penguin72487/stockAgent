# Vast 期貨 preparation 誤刪恢復與原始資料目錄

本次目標是恢復 vastai1T 誤刪的
`artifacts/markets/tw_futures_v8_margin_preparation` 中重要來源與最新可用資料；
使用者另指定 raw data 改放 `data_tw_index_futures`，其餘既有安全清理繼續。
這不是恢復所有舊研究版本到熱儲存，也不授權刪除唯一冷資料。

## 目錄分工

```text
data_tw_index_futures/preparation_sources/
  margin_sources/          官方原始資料、觀察表、規則與固定來源收據
  margin_repair_pending/   已接受修正规則與其原始附件
  margin_repair_inputs/    已接受估值研究設定及其來源
  official_final_settlement_all_asset_classes_20260929/raw/
                          原始 HTML，保留官方來源與 SHA
  current_release_source_refs/<rules-sha>/
                          node-local 原始附件引用，共用固定來源 inode，不另存 payload

artifacts/markets/tw_futures_v8_margin_preparation/
  margin_components_current/  最新可重建的編譯／訓練資料
  official_final_settlement_all_asset_classes_20260929 -> canonical source
                          僅舊 immutable receipt 的相容連結，沒有第二份 payload
```

`data_tw_futures/margin_sources` 等舊名稱也只保留到新位置的相容連結。
舊收據的內容、日期、原始數值與 SHA 不變；不可重新下載目前版本冒充歷史來源。
新 source staging 的預設輸出與 catalog 直接使用 canonical source 位置。
`tw-futures-margin-sources` 為獨立資料集，父資料集 `tw-index-futures` 排除該子目錄，
避免相同來源再打包一次。訓練 panel/cache 不屬於這些來源目錄。
`current_release_source_refs` 也是排除發布的本機引用視圖，不是另一份冷資料，
也沒有自動七日 GC。當最新編譯器複製原始附件到 `terms` 及 `release/rules` 時，
`compact_vast_futures_release_sources.py` 先核對完整原始／副本 SHA、精確檔名集合、
來源穩定性與程序／inode，再改成此視圖的目錄連結，最後只移除已證明重複的副本。
生成的財務 admission／估值證明留在 artifacts，daily/rules 原 hash 必須保持不變。

## 已核對的救援來源

- vastai1T 被刪根目錄原本是空目錄，沒有可救援的 deleted fd/mmap。
- penguin 仍有 preparation 工作資料；本次不能把其全部 69.47 GB 當成唯一來源大小，
  因為存在硬連結與大量舊版本。
- D 收到的 `markets.tar.zst` 約 71.14 GB，其中 index 有 536,394 個期貨檔名、
  約 28.87 GB 邏輯資料。其原始 stream 可完整解碼，但 producer 曾回報來源變動，
  所以不能宣稱它包含後來才產生的最新 `margin_components_current`。
- 最新固定來源 manifest 仍在 vastai1T，SHA：
  `86e0f4134125901f934a5b156a38b140202fed36ae3b9df0fbbfd4c91f178b9c`。
  原 builder SHA：`33c71e2eb0d6fd216bfb972f79aa927d099005f4ab95257fe73fdf938c0b4f89`。
- 重建所缺的 430 個原始結算 HTML 已從 C 的保留資料驗證並送回，合計
  137,233,448 bytes。每個檔案的完整 SHA／size 均符合原來源 manifest 的收據，
  新位置在 `data_tw_index_futures/preparation_sources/`，原始檔案不再位於 artifacts。

## 2026-10-05 凌晨驗收

- 最新 `margin_components_current` 已用原 builder／來源重建並原子放回 Vast 原路徑：
  697 商品、1,896,494 筆，daily 與 rules 的完整 SHA 均與原發布相同。
- penguin 與 Vast 的三個固定來源 bundle 已搬到 canonical source 位置，沒有複製 payload；
  penguin 的完整官方結算來源目錄也已搬遷，Vast 的 430 份缺失 raw 已按收據恢復。
- Vast 的 10 個生成 raw 附件副本目錄已改為 canonical source reference aliases。
  只移除 113,066 個完整 SHA 已吻合的副本，`st_blocks` 合計 11,527,168,000 bytes。
  固定來源及所有 D cold payload 均未刪；不是宣稱整段救援的磁碟淨增空間為 11.53 GB，
  因為恢復最新編譯資料本身也會占用空間。
- 原 frozen canonical reader 在改完引用後仍通過 56,543 份 source receipt 與完整規則驗證，
  schema 8、research-only 界線不變；沒有啟動訓練、策略或交易。
- 相關 storage/recovery 回歸：76 passed。其餘 markets／ablations／cache 的 bulk 回收
  仍等待既有 cold ingress owner 與獨立 D 恢復等 gate，不能稱為全部清理完成。
  penguin 其他舊 preparation 工作目錄也不因最新救援成功而自動取得整根刪除權。

收據位於：
`artifacts/operations/futures_preparation_recovery_20261004/`。
Vast 私人操作收據：`/var/lib/stockagent-futures-recovery/20261004/`。

## 常用指令

```bash
source scripts/runtime_env.sh

# C / remote 來源目錄搬遷預演；不搬資料、不刪除。
run_fintech_python scripts/relocate_futures_preparation_sources.py
run_fintech_python scripts/relocate_futures_preparation_sources.py --remote-vast

# 確認沒有來源 reader 後搬遷。相同 inode 移動，不增加第二份資料。
run_fintech_python scripts/relocate_futures_preparation_sources.py --apply
run_fintech_python scripts/relocate_futures_preparation_sources.py --remote-vast --apply

# 誤刪來源檔恢復的預演：逐一比對 remote 收據與 C 原始檔。
run_fintech_python scripts/restore_vast_futures_raw_sources.py
# --apply 是本次固定來源的一次性救援，拒絕覆蓋任何既有目的地。

# 原始 builder + 原始來源的最新重建計画；實際 --apply 保留獨立私人輸出。
run_fintech_python scripts/rebuild_vast_futures_preparation_current.py

# 僅本次已驗證的 current 重建才可正式放回；不覆蓋既有目的地。
run_fintech_python scripts/promote_vast_futures_current_recovery.py

# 查明重建造成的重複原始附件，再移除副本，保留 Data 下固定來源。
run_fintech_python scripts/compact_vast_futures_release_sources.py
run_fintech_python scripts/compact_vast_futures_release_sources.py --apply

# 用原 frozen canonical reader 核對搬遷後完整 source receipts；不啟動訓練。
run_fintech_python scripts/verify_vast_futures_recovery_runtime.py

# 所有遠端冷保存／安全回收的只讀進度；不解封、不刪資料。
run_fintech_python scripts/status_vast_bulk_return.py
stockagent-agent task show vast-futures-preparation-recovery-20261004
```

原發布預期 daily SHA：
`6e1a8e652a64f1a6cd20a33ddc112beec2daaeefca73f58ffde0d6ba0569560a`；
rules SHA：`43f7fe780a59d1d8ea5aa0c05f9967acf5a961cec6af68d58920cc4928cb3e20`。
重建只有兩者完全相同才可宣告同一資料內容；工程成功不代表歷史財務缺口消失、
嚴格 PIT、策略部署或 broker fill 已驗證。

## 清理的界線

期貨 preparation 已安裝私人 recovery hold，舊 legacy 與 bulk retirement 都必須
保護整根與子目錄。其他 scope 仍需逐項完整冷恢復、current ACK、process/config
引用、source fingerprint、pin/lease 與同步收斂等 gate 才能刪除。
本次停止的是自己的整根舊歷史解封及等待送回工作，D carrier 未刪；
其他正在執行的 ingress owner 與正式服務沒有停止。
