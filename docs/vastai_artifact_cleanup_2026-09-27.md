# vastai1T：markets／ablations 安全審查與去重（2026-09-27）

## 結果

本次範圍是 **vastai1T** 的 `/root/stockAgent/artifacts/markets` 與
`artifacts/ablations`，不是 penguin 的 D 主冷庫。
06:58 TST 完成一次性去重，實際回收 **10,558,328,832 bytes（10.56 GB／9.83 GiB）**。
所有原始檔案路徑、唯一內容、模型權重與訓練收據均保留；沒有刪除整份訓練成果。

| 範圍 | 執行前配置空間 | 執行後配置空間 | 本輪減少 |
|---|---:|---:|---:|
| `artifacts/markets` | 171.09 GB | 166.50 GB | 4.59 GB |
| `artifacts/ablations` | 125.61 GB | 119.65 GB | 5.97 GB |
| 合計 | 296.71 GB | 286.15 GB | 10.56 GB |

容量以同一次 `du -sx -B1` 同時統計兩個路徑；共用 inode 只計一次。
表格採 apply 收據前後值，與逐筆移除的重複 inode 配置空間一致。
使用者先前看到的 197.9／127.9 GB 是舊觀測，不作為本輪基準。

07:04:42 TST 的後驗檢查：Vast 磁碟可用 **161,080,233,984 bytes（161.08 GB）**，
使用率約 85.00%。同機其他工作仍可能寫入，所以 `df` 的淨差額不等於本輪回收量。
加上前兩輪 127,897,227,264 bytes，本日三輪收據累計回收
**138,455,556,096 bytes（138.46 GB）**；其中只有 10.56 GB 是本次新增。

## 實際清理方式：保留路徑，移除重複實體副本

本次把 **41 個已完成且無程序引用的 run** 中，252 組、279 份
SHA-256 完全相同的回測 NPZ 副本改為硬連結。相同位元組共用一份儲存，
原本的 279 個檔名仍可直接讀取，不需要還原或重新解壓。

只允許以下既有原子寫入的產物：

- `test_backtest.npz`
- `deployment_test_backtest.npz`
- `walkforward_deployment_backtest.npz`
- `test_integer_share_backtest.npz`
- `test_backtest_continuous_surrogate.npz`

不處理 `.pt`、metadata、log、CSV、圖表、未完成 run、活躍來源、V5 ingress 來源，
也不把僅同名／同大小／看起來相似的檔案視為重複。
候選在 dry-run 時必須只有一個硬連結，避免改動既有共用關係。

安全檢查：

1. 先只讀盤點 79,864 個檔案、完成狀態、實體 inode、冷庫 metadata 與程序引用。
2. 對 41 個實際受影響 run 執行既有 `training-lifecycle-v1` 驗證，
   確認至少七日未修改；解析執行中的訓練設定，排除受引用路徑。
   此處的檔案穩定期不是整份成果退役所需的「七日使用租期」。
3. 先寫 dry-run 與逐檔 hash 證據；apply 前重核 fingerprint、權限及 lifecycle，
   每 25 組再次檢查使用狀態與完成收據。
4. 使用既有 `apply_duplicate_groups`，逐檔重算 SHA-256 後原子替換；
   另檢查保存的 ZIP CRC、替換後 SHA-256 與共用 inode。
5. 獨立後驗再次讀回 252 組 SHA-256、核對全部 279 個替換路徑。
   11,608 個非目標檔案的識別欄位未變，其中包含 803 個模型檔；
   41 組 lifecycle metadata hash 未變，受保護 V5 的 420 個檔案也未變。
6. 相關測試：penguin 13 passed、Vast 12 passed，零失敗；差異來自兩邊既有
   `artifact_dedup` 測試版本。本次一次性 coordinator 的 7 個安全測試兩邊皆通過。

未更動訓練策略、服務設定、自動 GC、七日租期、pin、cold authority 或 ingress 範圍。
這不是建立新 snapshot，也沒有重新打包一份資料。

硬連結的後續使用限制：不可原地 `r+`／`wb` 修改共用 payload；既有產出路徑使用
「新暫存檔 → 原子 replace」，可安全建立新版本而不改到其他 run。
若需手動修改檔案，先複製成獨立工作檔再替換。替換副本的 inode／mtime 會改變，
舊 inode-based verification 可能需要由既有驗證流程重建，不得手動偽造綠燈收據。

## 審查發現：主要空間不是模型權重

以下是清理前、跨兩個目錄依 inode 去重後的檔案配置空間，不含目錄本身：

| 類型 | 配置空間 | 本次決定 |
|---|---:|---|
| NPZ 回測／陣列產物 | 212.79 GB | 僅處理上述逐檔相同且已完成的子集 |
| CSV | 35.98 GB | 未取得完整恢復證明，保留 |
| Parquet | 23.18 GB | 未證明與 CSV 等價，保留 |
| 模型權重 `.pt` | 16.06 GB | 保留，不刪 checkpoint |
| JSONL／PNG／其他 | 約 8.65 GB | 保留收據、分析與唯一內容 |

不能因 CSV 有同名 Parquet 就刪除，也不能因資料夾叫 ablation、benchmark 或舊實驗，
就推定沒有保留價值。舊格式缺少新版 lifecycle 證明，也不等於當年訓練未完成。

仍需完整封存／恢復審查的主要舊目錄如下；容量按本輪只讀 inventory 的 inode
首次歸屬計算，不代表可直接再回收的量：

| 相對 `artifacts/` 的路徑 | 配置空間 |
|---|---:|
| `ablations/financial_transformer_tw_day_trade_long_short_candles_projection_l1` | 49.49 GB |
| `ablations/transformer_base_portfolio_2006plus_dynamic_s` | 23.13 GB |
| `ablations/financial_transformer_naive_long_short_lb256_bs32` | 17.86 GB |
| `markets/tw_public_candles_snapshot_safe98_gross_tw_cash_lookback256_panel_history_2006plus` | 15.22 GB |
| `markets/tw_public_candles_tw_cash_lookback256_panel_history_2006plus` | 15.12 GB |
| `markets/tw_public_candles_tw_cash_lookback256_panel_history` | 13.36 GB |

以上全部保留。本輪對舊 `stockagent-migration-core` 做路徑／大小預篩，
沒有任何目前完整 top-level run 全部匹配；部分路徑相同也不是內容 hash／恢復證明。
沒有用 migration-core 存在這件事來授權刪除。

## 已可證明保存、但仍未退役的舊 effective-rank 成果

來源：

```text
artifacts/markets/tw_day_trade_daily_multi_basis_22_effective_rank_projection_l1_tplus2_close_commission20_capital10m_v1
```

精確冷 release：

```text
dataset: artifact-tw-day-trade-multi-basis-22-effective-rank-projection-l1-capital10m-v1
release: artifact-tw-day-trade-multi-basis-22-eff-20260902T022947718621394Z-l0-vastai1T-8efa69cfa8e55462
```

這份歷史 metadata 雖標為 `cold-small-files`，實際 inventory 包含全部 525 檔、
3,606,594,912 logical bytes，沒有大小截斷或排除子樹。本輪已：

- 驗證 penguin D volume／bind-mount guard、manifest／inventory hash。
- 核對全部 62 個冷物件；SHA-256 證明僅重用七日內、完整檔案識別欄位仍一致的
  既有讀回收據，並重新檢查 32 個 ZIP 的成員與 CRC。
- 在 Vast 逐檔核對完整來源內容與 inventory，並重新驗證 lifecycle，無程序引用。
- 後驗確認上述 62 個物件與 manifest 未變。

**仍保留來源，未刪除。** 原因是沒有已登錄並到期的七日使用租期；
現有 canonical retirement 也要求正式 `cold-full-run`／完成證明 metadata。
沒有手改歷史 manifest、用舊 mtime 假冒使用租期，或繞過這些 gate。
此外有 11 個 inode 與目標外共用；即使日後通過退役，
本地精確可釋放的檔案配置空間也只有約 2.00 GB，而非 logical 3.61 GB。

後續若要退役，應透過既有合法發布與 lease 流程補齊證據，重新驗證當時的
冷庫、來源、引用、pin、peer 及硬連結；不得直接重跑本次去重 apply 或刪整個目錄。
V5 ingress 另受兩側來源保護規則約束，本次也完全保留。

## 同步與服務後驗

07:04:42 TST 在 Vast 觀察到：

- `stockagent-packed` 為 idle，本機與 penguin peer need bytes／items／deletes 皆為 0。
- peer completion 100%、`remoteState=valid`；folder／system／pull／watch 錯誤全零。
- 當前連線是 `quic-client`、`TLS1.3-TLS_AES_128_GCM_SHA256`。
- 原本 RUNNING 的 Caddy、cron、portal、Syncthing、TensorBoard、tunnel manager
  均維持同一 PID。沒有停止或重啟服務。
- 後驗未觀察到 `train.py`；這不宣告先前訓練成功完成，訓練結果需另查 lifecycle。
- Jupyter／pyworker 的既有 EXITED 狀態及已停止的訓練 supervisor 項目未更動；
  不能把 supervisor 回傳碼 3 誤報成本輪服務中斷。

Vast 仍是 index-only compute node；同步收斂不是持有全冷庫 payload 的證明。
本輪沒有刪除任何 D 冷物件，也不宣稱全部歷史 release 已驗證可恢復。
D 單份主冷庫的既有風險保持不變。

## 證據與操作邊界

兩台機器都保留 `artifacts/operations/vastai_artifacts_cleanup_20260927/`，其中：

- `source-inventory.json`、`completed-artifact-duplicates.json`：只讀盤點與 hash audit。
- `backtest-dedup-plan.json`、`backtest-dedup-apply.json`：計畫與實際逐檔替換收據。
- `post-runtime.json`：獨立內容／路徑／模型識別欄位／服務／同步後驗。
- `cold-effective-rank.json`、`effective-rank-source-audit.json`、
  `effective-rank-retirement-gates.json`、`post-cold-preservation.json`：冷庫與退役 gate 證據。
- `migration-preflight.json`：舊 migration 的預篩結果，不是刪除授權。
- `local-tests.xml`、`remote-tests.xml`、`summary.json`：測試與容量彙總。

一次性 coordinator 綁定本輪來源 inventory、雜湊、完成證明與短效 dry-run；
apply 收據存在時拒絕重跑。它不是通用或定時刪除器。
本輪每個原始資料路徑仍在，因此不需要恢復命令；若要求重新分離實體副本，
應在有足夠空間時複製成新 inode 並原子替換，不能直接寫共用 inode。

前兩輪紀錄見 [vastai1T 安全空間回收紀錄](vastai_storage_cleanup_2026-09-27.md)。
