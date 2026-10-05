# vastai1T 儲存盤點與安全整理 — 2026-10-04

本次是容量、同步與回收的工程驗收，不是訓練完成或模型部署宣告。
沿用 [儲存契約](agents/storage.md)、[七日熱快取](packed_dataset_storage.md)
與 [agent 工作流程](agent_workflow.md)，沒有建立另一套冷庫或重新啟用 raw-artifacts 同步。

## 結果與範圍

penguin 的 `tw_public_lantent`、`lookback32_v2`、`feature_input_lookback32_v4`
三個原目錄、舊 mirror 與本次 C 編碼暫存都已回收，原值經 D 獨立恢復驗證。
三項原檔回收 **18,122,665,984 bytes**；包含先前 forex 則是
**22,384,287,744 bytes**。逐项證據見
[本機回收紀錄](markets_safe_retirement_2026-10-03.md)。

11:45 首輪 vastai1T 精確回收 **14,461,014,016 allocated file bytes**（14.46 GB）：

| 操作 | 已回收 bytes | 證明與保留邊界 |
| --- | ---: | --- |
| inactive 分鐘 cache 的 1,603 組相同原值合併 | 11,553,312,768 | 全檔 SHA、writer locks、metadata 與 process/config 引用核對；所有邏輯路徑保留 |
| 到期 `free-public-context` 熱副本 | 2,031,046,656 | D 完整解碼核 SHA，遠端熱原值逐檔相同；未 pinned、無活躍引用 |
| 該 release 的 69 個冗餘 payload 副本 | 876,654,592 | 精確 object 集合／SHA 與 D 一致；忽略規則生效且收斂後才刪本機副本 |

沒有永久刪除 D 冷 objects、唯一模型或唯一原始觀測。熱 cache 移除的
`data_free_public` 與 `current/free-public-context` 是指向已到期副本的註冊 symlink，
不是另外兩份資料。需要時可由原冷版恢復。

## 儲存佔用與容量口徑

遠端是容量 **1,073,741,824,000 bytes** 的非持久容器；
`workspace_is_volume=false` 已重新查驗，不能當成獨立永久冷備份。
初次 `df` 可用 **3,529,166,848 bytes**；11:40 後測可用約 **27.3 GB**，
11:45 的最終收據則為 **21,112,655,872 bytes**（21.1 GB、約 98.03% 使用率）。
仍有其他並行空間變動，並非空間問題已全部消失；以下回收量只採本次精確收據。

以下是 11:37–11:40 的 allocated-byte 盤點，GB 採 10⁹；資料樹不是交易筆數。
`du` 不跟隨 symlink，且同一呼叫不重算 inode。父／子目錄不能相加；
有其他並行寫入／回收，容量淨變化不等於本次操作精確回收量。

| 範圍 | allocated GB | 本次處置 |
| --- | ---: | --- |
| `artifacts` 全部 | 710.16 | 父集合，不與下列子項相加 |
| `artifacts/markets` | 278.94 | 保留；可能含唯一模型、會計修復結果與持續寫入 |
| `artifacts/cache` | 244.78 | 只合併已證相同且不活躍的那一根；不按 cache 名稱整根刪 |
| `artifacts/ablations` | 119.65 | 保留唯一研究成果；未取得整根 D 恢復證明 |
| `artifacts/smoke` | 34.35 | 名稱不是可刪證據；保留 |
| `artifacts/operations` | 14.39 | 來源差異、quarantine 與操作證據保留 |
| `/srv/stockagent-packed-materialized` | 94.49 | 已減少 2.03 GB；餘下包含 pinned 資料和 28.19 GB quarantine |
| `/srv/stockagent-packed` | 0.98 | 冷索引；本次 payload 已清為 0，非完整冷物件副本 |
| `/root/.cache` | 7.58 | 預設 14 日編譯 cache dry run：0 eligible，未另刪除 |
| `/tmp` | 16.71 | 未取得所有私有暫存的完整使用／重建證據；保留 |

初次盤點 `/root/.cache` 約 24.79 GB，後測約 7.58 GB；這個差異沒有本次
刪除收據，不算本次回收。`markets` 在兩次量測間增加約 6.57 GB，同樣不
歸因於本次清理。其他 worktree、環境、VS Code、TensorBoard 與 ingress 也未整根刪除。

較大的下一批審查項目包括 `tw_futures_v8_margin_preparation` 約 93.42 GB、
`tw_day_trade_v8_web_parity_v7` cache 約 54.04 GB，以及 ablations／quarantine。
penguin 已整理過期貨 native 版本，不代表遠端的所有 outputs 與依賴也已相同；
不得直接複製刪除清單或按版本號刪遠端目錄。

## 同步與 GC 根因修正

1. 初次兩個 Syncthing folder 都留下 database 空間錯誤：可用 0.33%，低於
   最低 1%。先釋放驗證相同的 cache 副本，再掃描各 folder 恢復；沒有降低
   最低空間閘門、清除錯誤冒充成功、重建 device ID 或重啟服務。
2. 遠端 supervisor 使用 `127.0.0.1:18384` 與執行參數內的 GUI 覆寫；
   `config.xml` 的 8384 不是這個 instance 的 API。檢查採實際 instance，
   收據不輸出 API key。
3. 原忽略規則連 blob/pack 的 shard 目錄都排除，導致 penguin 顯示 100%
   卻仍有 **240 個零 bytes 目錄**待同步。改為只忽略 shard 內 payload children，
   不為了消除 `needItems` 而傳回全部冷物件或放寬驗收閘門。
4. `gc`／`evict`／`prune --apply`／解封後 payload prune 現在統一先更新
   ignore → scan → 確認當前收斂 → 再刪本機副本；前後核對 process references。
   既有 owner lock 在成功、拒絕及例外都明確釋放，避免長存呼叫者鎖住後續 GC。

冷庫 folder `stockagent-packed` 雙方已觀測 `idle`、100%、valid、零 need
bytes/items/deletes、零 folder/pull/watch/system errors，QUIC、
`TLS1.3-TLS_AES_128_GCM_SHA256`。完整冷原值仍只在 penguin 的 D：
遠端同步 heads/manifests/inventories，按需才接收指定 payload／解封。
上述四份 legacy manifest／inventory SHA 在兩台一致。

產物 `stockagent-artifact-ingress-vastai1t` 仍是遠端 Send Only、penguin
Receive Only 的有界 quarantine，沒有與常態 artifacts 合併。網路收妥與
penguin 的合法 artifact admission／冷封存仍是不同證據，不能直接發布新模型。
該 folder 的舊空間錯誤也經個別 scan 恢復，11:45 最終雙方都為 idle、valid、
100%、零 need／errors；傳輸區約 3.52 GB 的既有資料未回收。

既有五分鐘 cron GC 保留、維護 owner 已釋放；後測 canonical `gc --dry-run`
通過，四份 pinned release 與目前 lease 尚有效的 tw-public 保留。
不能因這次沒有 live references 就強制解除 pin，也不動 28.19 GB 的歷史 quarantine。
早期完整校驗未通過的歷史 releases 不在本次全庫恢復宣告中。

## 保護與驗證

初次有獨立雙 GPU 期貨 v4 `train.py` 工作，與 supervisor 的 stopped 訓練項目不同。
用它的 immutable source bundle 解析實際 config／資料依賴；主 checkout
`stockagent/config.py` 有既有未完成 Git merge，未修寫或覆蓋該 merge。
後測未見該 train.py 程序；本次未發送停止、重啟或重新訓練指令，
不把程序消失推定為完成，也不把它當成清理成果。
Syncthing、cron、Caddy、TensorBoard 與 tunnel supervisor PID 維持運行。

採用既有使用偏好的「完整工作流與證據分層」：先 inventory／完整 SHA dry run，
再精確 apply／事後核驗；未證可恢復的剩餘資料保留。
本機 focused regression **88 passed**，遠端相關 cache/packed tests **71 passed**；
這不是全專案訓練／服務回歸驗收。

主要 [驗收收據](../artifacts/operations/vast_storage_safe_cleanup_20261004/acceptance.json)
彙整精確回收、雙方同步、D 原值保留及 owner 釋放證明。操作原件在
`artifacts/operations/vast_storage_safe_cleanup_20261004/`，包括：

- `remote-minute-cache-dedup-apply.json`：1,603 組完整 SHA／原路徑／實際 inode 合併。
- `free-context-D-recovery.json`：D 的 69 objects、1,130 檔、2,028,352,027 logical bytes 全量解碼核 SHA。
- `free-context-vast-recovery.json`：遠端熱檔全量相同，不只依賴 READY 標記。
- `free-context-retirement-retry-acceptance.json`：canonical dry run／apply、2.03 GB 熱副本與 69 encoded 副本回收。
- `edge-gc-deployment.json`、`edge-shard-rule-deployment.json`：精確部署前後 SHA 與可回復程式備份。
- `penguin-edge-final.json`、`vastai1T-edge-final.json`：當次 transport 與精確 release 索引。

一次 preflight 因補充 proof 的 kind 不符合 canonical schema 被拒絕，尚未
刪除任何熱檔或 payload；保留該失敗收據，修正為既有 durable-peer schema
加上獨立 D 恢復證據後重新驗收，不放寬 canonical gate。

## 日常檢查與恢復

在 vastai1T，從既有 checkout 執行（API key 不貼到命令或聊天）：

```bash
cd /root/stockAgent
bash scripts/run_data_cache.sh gc --dry-run
bash scripts/run_data_cache.sh status
```

需要剛回收的資料時，按精確版重新接收並續七日 lease：

```bash
bash scripts/run_data_cache.sh use free-public-context \
  --snapshot-id free-public-context-20260926T174248126073150Z-l0-penguin-990e503dd3fdc919 \
  --ttl-days 7 \
  --link /root/stockAgent/data_free_public
```

此 `use` 會接收、核 SHA、解封並續租；不是重建資料來源或重新訓練。
其他根的清理仍須逐根取得相同的完整來源／recipe、D 恢復與實際引用證明，
不能用目錄名稱、檔案年齡或 Syncthing 100% 代替。

## 15:44 增量安全回收與整批進度

本輪新增回收 **12,586,967,040 allocated file bytes**（12.59 GB），來自
`/tmp/torchinductor_root` 的 **102,882 個**超過 14 日未讀寫的編譯快取。
刪除收據為零 errors、零 changed/open/protected-start skips；未使用 `--force`，
未縮短保留期，未刪 D objects、原始觀測、唯一模型或訓練成果。
該根由約 15.27 GB 減為 2.64 GB，年輕、使用中與保護項目仍留存。
其他 `/tmp` 私有暫存（DDP、Git merge 等）不在本次 scope。

根因是 PyTorch 實際 `cache_dir()` 指向 `/tmp/torchinductor_root`，不是
原維護器預設的 `~/.cache/torchinductor`。沿用既有維護器增加明確、手動、
root-owned 的單根選項；沒有清整個 `/tmp`、建立另一套 GC 或擴張自動排程。
共用逐檔門檻增加 ctime／mode／nlink 核對，並每 128 個候選重查程序與 fd/mmap。
其內容是可重新編譯的衍生結果；下次首次使用可能增加編譯延遲，而不是失去模型。
參見 [PyTorch 快取文件](https://docs.pytorch.org/tutorials/recipes/torch_compile_caching_configuration_tutorial.html)。

15:44 遠端可用 **34,016,165,888 bytes**（34.02 GB），仍約 **96.83%** 使用率。
有並行寫入／回收，不能用 `df` 前後差取代逐檔回收收據。今日可核對、去除
重疊 pilot 後的各份收據合計 **33,253,228,544 bytes**（33.25 GB）：

| 已有確切收據的範圍 | allocated GB |
| --- | ---: |
| 11:45 首輪去重／到期熱副本／冗餘 payload | 14.46 |
| 三項完成產物回傳 D、核原值後回收 | 4.59 |
| legacy 主批次／子分區合計 100 根已回收 | 1.62 |
| 本輪過期 `/tmp` 編譯快取 | 12.59 |

三項完成產物與 100 根 legacy 的精確遠端原路徑均再次確認不存在，沒有把
19,951,616-byte legacy pilot 在兩張收據中重算。首個 68-file pilot 因較早的
ledger schema 未記錄 post-retirement 欄位，本輪重新從 D 完整解碼核對
87,471,988 原值 bytes、精確 manifest／release，原冷資料留存。

**整批 markets／ablations 回傳仍未完成。** 15:44 主批次仍有 58 根、子分區
42 根處於 `remote-source-retired`；在途的 futures diagnostic 分區
**8.83 GB／110,068 檔**仍為 `publishing-d-cold`，主批次 ablation
**23.11 GB／11,870 檔**為 `waiting-shared-owner`。這兩批都不能計入已回收量。
目前瓶頸是大量小檔的 D 冷發布／恢復工作，不是缺少 QUIC；未增加第二個 D writer。
既有 adaptive 編碼、受監督交接及五分鐘重試 timer 保留運作。

15:31–15:32 盤點的大宗餘量為 markets **280.70 GB**、cache **244.78 GB**、
ablations **119.46 GB**；這些 allocated 值不代表全都可刪。cache 也可能含唯一
觀測、服務依賴或活躍工作；先證明可重建或取得完整 D 恢復證據，再由 canonical
逐項門檻回收，不能按名稱批量刪除。

後測 packed index 雙方 idle／100%／valid、零 need／errors，QUIC + TLS 1.3。
這只證明已提交冷索引的當前同步，不代表上述在途資料已完成冷恢復。
Caddy、cron、Syncthing、TensorBoard、portal／tunnel supervisor PID 未更動。
本機聚焦回歸 **64 passed**，遠端快取 scope／安全門檻 **12 passed**。

本輪 [驗收收據](../artifacts/operations/vast_storage_safe_cleanup_20261004/tmp-compiler-cleanup-acceptance.json)
保留去重合計、當前遠端空間、雙端收斂及原路徑不存在的證明；同目錄另有
`tmp-compiler-deployment.json`、`tmp-compiler-audit.json`、`tmp-compiler-apply.json`
與 `initial-pilot-current-D-recovery.json`。先前唯讀驗收因 pilot 舊欄位缺少被拒絕，
沒有做額外刪除；補上當前完整恢復後才接受，失敗 run 收據保留。

自助指令已補於 [README 磁碟壓力章節](../README.md#磁碟壓力與可重建快取)
與 [維護說明](storage_pressure_maintenance.md)；正常 D／artifact 回收仍遵循
[整批回傳契約與進度](vast_all_artifacts_cold_return_2026-10-04.md)。
