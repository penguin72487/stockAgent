# Vastai1T 訓練儲存分類與七天保留（2026-10-07）

`artifacts/markets` 的目標是只保存訓練結果及相關 lifecycle／配置／驗收紀錄。
本輪實際辨認 193 項混入的資料、cache、code、測試與操作檔；最新驗收為
**193／193 項分類完成，待處理為 0**。最後一項 cache 在工作釋放引用後，
由原有自動排程搬移；同一 owner 回傳 complete 收據。本輪處理固定清冊，
不代表整台機器、全部歷史資料都已清空。
後續覆核發現舊 frozen benchmark 配置曾重建原 cache；新增 generation 接手後，
首個重建版本也已完整搬移，最終實際目錄檢查的重建待辦為 0。

| 內容 | 位置 |
| --- | --- |
| 正式訓練、checkpoint、曲線、報表及 run metadata | `artifacts/markets/<experiment>/` |
| 原始資料、來源證據 | 原 canonical `data_*`、固定 packed materialization |
| prepared panel、特徵、編譯後執行資料 | `artifacts/data_preparation/<experiment>/` |
| 工作 cache | `artifacts/cache/<experiment>/` |
| source snapshot、wheel、runtime receipt、SDK | `artifacts/code_releases/<experiment>/` |
| 工程效能／驗證暫存 | `artifacts/benchmarks/<experiment>/` |
| 工程工具、操作收據與 local launch copy | `artifacts/operations/` |

## 已遺失的結果與七天保留

`tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_scale_separated_cash_v1`
的 `training-bf16` 並非 smoke 丟棄；原 completed-return owner 在冷保存與獨立驗證
通過後回收了熱端。舊設定只有十分鐘的 publication stability，沒有七天的熱保留。
原訓練於 **10/7 03:57（台灣時間）完成 fold 11**；沒有重新跑訓練。

已固定還原：

- Dataset：`artifact-auto-training-bf16-22d78a855c3a9835`
- Snapshot：`artifact-auto-training-bf16-22d78a855c3a-20261006T203616866474970Z-l0-penguin-8edda4ada771e1c4`
- Manifest SHA-256：`64d488f051d82e499aa4a5763aebb8988efb3f2ac6c337a930f888aab0420066`
- **57 檔、95,875,237 bytes**，原路徑全檔 SHA／mode／精確集合與 completion lifecycle 通過。

Policy v3 啟用 `minimum_hot_retention_hours: 168`，正式回傳、legacy parent、bulk
三條回收路徑共享同一門檻並在 unlink 前再核對。冷保存仍可先做，熱回收等
canonical completion 與最後一次檔案寫入中較晚的時間再加七天。本次還原的實機
gate 給出的最早時間是 **10/14 08:51（台灣時間）**；到期仍須通過完整冷恢復、
新鮮 ACK、程序／服務／hold／link／mount 等原門檻。不要以日期到期當作刪除證據。
不需要的 smoke／benchmark 保持使用者原本允許的獨立丟棄政策。

## 固定搬移與自動接手

固定 plan 身分：`b58b2ae30252ea89022bdcd2cf1f7392995cd13f5cc2fa16d1f98c44b7edbebc`。
位置：`/var/lib/stockagent-training-layout/plan.json`；單一 owner／progress：
`/var/lib/stockagent-training-layout/state/`。沒有新增訓練、NAS pruner 或付費資源。

使用 atomic no-overwrite 同磁碟 rename，沒有複製 payload。核對完整名稱、kind、
檔案 inode／size／mtime／mode／link、symlink target 與 filesystem，保留搬移前後
fingerprints。容器 overlayfs 的 directory entry size 曾從 101 變為 85；此值不是
檔案 payload 身分。修正後只忽略目錄 bookkeeping size，檔案與名稱集合仍須相同，
並實際從原「已改名、未寫成功收據」的 journal 接手驗證，未丟棄資料。

引用保護包括 config 的每層繼承、argv、cwd、FD／mmap、程式碼與 cache 環境變數。
新版 frozen schema 由該工作實際選定的 canonical loader 解讀；control checkout 的
舊 schema 不會被拿來否定合法的新實驗，也不會刪掉陌生欄位來放寬檢查。
三個沒有 client、沒有 child 的舊 agent tmux shell 已切回 canonical checkout；
沒有停止 GPU 工作或改寫 live frozen code。

既有 `stockagent-remote-cold-artifact-ingress.timer` 的 owner 在每輪回傳前獨立重試
固定搬移；上限 32 項／輪。布局錯誤、忙碌或暫時連線失敗會留收據，後續備份
仍可處理。當整個 benchmark 工作目錄的 owner 退出，另等五分鐘穩定後凍結最新
完整檔案集合再搬移。

最後一項曾暫緩：
`tw_day_trade_factorized_panel_20261005_gaprepair_v4/runtime-cache`。
新一輪 `tw_scale_cash_throughput_20261007_annual_v8` 的活躍配置、FD 與 mmap
當時仍引用它，保持原位。兩份 prepared panel、transform cache 與舊 throughput
工作目錄先搬移；原 owner 後續重試在引用解除後完成最後 cache。
可變 cache 在 owner 退出後凍結最新完整集合，保留登錄及搬移時兩份
fingerprints，沒有只搬舊清冊而遺失新檔案。最終 complete 收據明列
completed 193、pending 0，各 launch 為 ready，並非只看排程退出碼。

後續 `annual_v9` 複製舊配置後，又產生原路徑的 runtime cache。原 complete
收據只證明第一份清冊，不能證明舊路徑永不再出現；最終名稱檢查實際抓到此問題。
修正後同一 owner 每輪重查已搬移的 cache。重建版本使用私有固定 child plan
及獨立 journal，忙碌時重新列入 pending；釋放後無覆蓋搬到
`artifacts/cache/<experiment>/runtime-cache.generations/<serial>/`，保留原 canonical
cache，並能從 rename 後、收據前的中斷接手。
首個實機重建版本為 **249 檔、2,640,645,960 bytes**；已搬移，最終 namespace
檢查所有原清冊來源名稱均不存在、所有目的地存在，重建 pending 為 0。
後續若舊 frozen 工作者再產生此 cache，仍走相同自動接手。

另以相同消費者檢查移除七個空目錄，三個仍有 recovery hold 的空根保留。
本次還原 run 的臨時 hold 在全檔與七天門檻再次通過後單獨解除；先前四個
recovery hold 原樣保留，prepared／code 新位置未加入自動 GC。

## 後續產生路徑與啟動

維護中的 release builder、mixed／factorized preparation、feature catalog、value
projection、batch benchmark 與 profile 入口採用共用 output admission，拒絕將非訓練
產物寫入 `markets` 或將 mutable 工作寫到 frozen `code_releases`。舊 frozen bundle
保持原始位元組，source/config/checkpoint 的歷史 provenance 不改寫。
Mixed／factorized builder 新產生的配置也明確選擇 `artifacts/cache` 的 physical
cache，避免繼承歷史 base config 中的 `markets` 路徑。
新 benchmark attempt 的獨立配置也正規化這個 cache 路徑，保持來源配置原樣。
Canonical minute-tape 與 physical-carry cache writer 在讀來源前拒絕 `markets`
cache，包括 alias；已啟動的 frozen 工作保留原版本。

最新 scale-separated cash run 的完整配置從其驗證過的 run manifest 產生 local copy：
`artifacts/operations/training_launches/<experiment>/runtime-config.yaml`，由
`runtime-launch.json` 綁定 config／原 manifest SHA、原 source SHA 與原 fold／output root。
只更新已成功搬移的路徑，不依賴先前退休的工程 base-config chain。原歷史配置仍在，
原訓練 metadata／checkpoint 不修改。

原 run 根的 `train_fold11.sh` 直接使用 shared `scripts/run_relocated_training.py`
並綁定自己的 `runtime-launch.json`，其原始副本保存在 local launch 目錄。
repo 的相容入口目前由另一個工作流程切到最新年度實驗；保留該變更，舊 run
不透過此可變名稱啟動，避免意外切換實驗。
`--check-only` 核對原 frozen code／bundles、local config 與輸入位置；pending inputs
尚未搬完時回傳 75，本輪實機曾回傳 75 並列出最後一項 cache；自動搬移後
重跑舊 run 自己的入口得到退出碼 0，原 frozen code／bundles、local config、四個準備／cache
輸入位置及 14,726 個 value channels 均通過，output 仍是原 `training-bf16`。
將來正式啟動仍須原三份固定 source READY、pin／manifest SHA、完整 lease 與七天
續期，再通過 CUDA／GPU owner；不改成 moving latest。沒有用此份工程配置
啟動新訓練，也未宣稱新的 resume／研究驗收。

## 驗證與收據

本輪 shared return／legacy／bulk／layout／code-release／projection／benchmark 檢查
**228 passed**；後續 overlayfs 接手 13 項、實際 selected frozen-loader 與 producer
修正範圍 **99 passed**，重疊檢查不相加。最後的 producer 與 owner 實機收據另記
在下列同一目錄，避免以先前測試代替後續修改的驗證。

Producer 修正範圍原有 **63 passed**；最後 cache generation／layout 與固定來源續期
**19 passed**，加上生成配置避免繼承舊 cache 路徑的完整受影響範圍
**67 passed**，另留 `final-layout-tests.log`、`latest-producer-layout-tests.log`。
重建 generation／中斷接手、兩個 canonical cache writer 及分鐘／physical source
回歸 **115 passed**；最後含新 benchmark attempt 正規化的 scope **45 passed**。
收據為 `recreated-cache-and-writers-tests.log`、`final-cache-placement-tests.log`。
Vast 實機 batch/profile 入口在 GPU 工作開始前
拒絕 `markets` 測試輸出，未生成目錄。兩個有其他開發工作的工具只套用此輪
路徑 admission，核對修改前 SHA，保留原副本，未覆蓋其效能開發。

本機：`artifacts/operations/vast-training-retention-recovery-20261007/`。
重要收據：`restored-result.json`、`live-seven-day-gate.json`、`layout-selected-moves.json`、
`layout-status.json`、`final-tests.log`、`selected-loader-tests.log`。
實機最後驗證另有 `post-layout-restored-run.json`、`producer-guards-installed.json`、
`producer-runtime-admission.json`、`empty-markets-directories.json` 與
`automatic-owner-layout-status.json`、`ready-training-entry-check.json`、
`producer-cache-defaults-installed.json`、`cache-writer-upgrade-installed.json`、
`final-namespace-check.json`、`acceptance.json`。
遠端：`/var/lib/stockagent-training-layout/`；舊清理收據保留在
`/var/lib/stockagent-storage-audit-20261006/`。

本輪是分類與保留修正，搬移的 logical bytes 不等於釋放容量；沒有將本輪結果
當作 NAS 全歷史、repository prune、語義還原或整台 Vast 清理完成的證據。
