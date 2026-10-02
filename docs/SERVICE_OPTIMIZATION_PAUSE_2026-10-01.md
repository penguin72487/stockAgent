# 全服務優化暫停交接（2026-10-01 11:43，Asia/Taipei）

**最新狀態：2026-10-02 21:22 使用者再次要求暫停；目標暫停，不是完成。
下方各次 active／paused 紀錄均為歷史；等使用者明確要求繼續才恢復。**

## 最新暫停交接（2026-10-02 21:22，Asia/Taipei）

使用者正在整理／刪除資料，要求先收束手邊工作。本次僅核對工作狀態與整理交接，
沒有新啟動程式修正、測試、測速、來源掃描、下載、重建或發布作業。
正式網站、交易服務、收集器與 systemd 排程未停止或重啟，也未干預其他 actor；
既有排程可能繼續資料寫入，**不是停止所有系統寫入或資料整理的宣稱**。
所有 dirty changes 保留，沒有提交、回退或刪除來源。

### 已收束且保留的工作

- 正式 per-dataset projection shards、共用精確欄位計算、schema pairs 一致性修正、
  完整 JSON／receipt 原子發布與故障 fallback 均保留；沒有為了暫停撤回已部署修改。
- 前一輪證據見[20:52 服務清冊](service_inventory_2026-09-23.md)與
  [驗收及當時 SHA256](../artifacts/benchmarks/data-monitor-shards-validation-20261002.json)：
  共用回歸 1,185 項、針對性 126 項，有重疊，不加總。本次沒有重跑測試。
- 固定真實 metadata 觀察的完整 publisher ABBA 中位數為 2,656.915／547.734 ms，
  不含 cache decode、record refresh、真實冷 I/O 或 browser；自然世代 shadow 仍為
  `inconclusive_initial_generation`，不改成 pass。這些都是前一輪歷史證據。
- 21:22 協作清單只有 root，沒有存活子 agent；只核對已知 data-monitor benchmark、
  coverage audit 與 pytest 程序，沒有發現仍在執行者。沒有本輪待中斷的已追蹤呼叫，
  未以停止其他 actor 或正式服務的方式暫停。
- 上述驗收 JSON 的文件 SHA256 保留為當時快照；本次只更新交接文件與清冊狀態，
  不覆寫舊收據、不把文件更新冒充新的程式／服務驗收。

### 繼續時的起點

1. 等使用者明確要求繼續，再重查 Git／其他 actor 的修改、資料實際位置與身分、
   容量、排程及最新 receipts。整理前的檔案數、容量、耗時與健康狀態不能沿用。
2. 先確認完整來源世代與 projection cache 的一致性；來源消失、重建或移動時維持
   真實 partial／inconclusive 與 canonical fallback，不放寬身分或 receipt 核對。
3. 再處理 record cache 解碼／聚合／寫入、public status 的實際耗時，並接續完整
   服務清冊的效能與可靠性項目。TAIFEX／OpenBB／OKX／Yahoo、cold publication、
   全庫、公網／browser 與 Windows-WSL 冷恢復尚未全部驗收；目標仍未完成。

## 初次暫停交接（2026-10-01 11:43，Asia/Taipei）

使用者正在整理／刪除資料，明確要求先收尾暫停；目標暫停，不代表完成。
不再啟動新測速、來源掃描、下載、重建、服務重啟或資料變更。
既有網站、收集器與系統排程未停止，也未干預其他 actor 的資料整理。

## 本輪保留的修改

- `stockagent/live/data_monitor_inventory.py`：逐資料集成員指紋與綁定根指紋，
  共用 `inventory_dataset_delta`，精確辨認新增／移除／退休／重新歸屬、
  schema 與五部分檔案身分變動。缺契約、綁定損壞或未知版本仍要求全量處理。
- 保留讀者相容的外層 cache **v10**，新增獨立 `dataset_membership_version=1`；
  fast index **v3** 使舊索引重驗一次。短暫外層 v11 曾由自然 producer 寫入，
  已改回 v10，11:32 的現場 cache 讀取確認 v10；遷移保留相同 footer，
  不以丟棄全部快取、重啟常駐 gateway 或放寬身分檢查完成修正。
- 彙總項目與實際 feature owner 分開；沒有刪除合法 group 彙總或公開欄位。
- `scripts/benchmark_data_monitor_feature_delta.py` 不再維護第二份 delta 規則，
  直接共用正式服務的函式；支援退休資料集、有界取得初始完整世代，
  以及用完整成員根指紋驗證已完成世代的清單。來源身分移動仍是證據不足。
- 新測試 `test/test_data_monitor_membership_delta.py` 及既有 inventory／benchmark
  測試補上新增、移除、共用擁有者、排序、损壞契約、遷移與世代綁定。

## 已收斂的驗收

- [相關共用回歸](../artifacts/benchmarks/data-monitor-membership-shared-regression-20261001.log)：
  **1,035 passed／43.85 秒**；包含 data-monitor、FinLab、FinMind、public import
  與 feature benchmark。此試跑在最後的 shadow 世代清單改善之前完成。
- [最後針對性回歸](../artifacts/benchmarks/data-monitor-membership-semantic-regression-20261001-final.log)：
  **103 passed／1.35 秒**；包含最後 shadow 修改。兩次測試有重疊，不加總。
- 正式 inventory 與當時 benchmark 通過 Ruff／py_compile；最後 shadow 修改
  已跑語意測試，未重跑全套 lint 或上述 1,035 項。沒有全庫通過宣稱。
- [自然發布 journal](../artifacts/benchmarks/data-monitor-membership-natural-cycles-20261001.jsonl)
  已看到明確 `changed_dataset_ids`；完整正式 feature 建置仍在執行，
  沒有啟用增量分片發布，也沒有宣稱完整流程已由約 9–12 秒降至極限。
- 兩個早期實際 shadow 證據保留：
  [第一次](../artifacts/benchmarks/data-monitor-membership-natural-delta-20261001.json)
  為 `inconclusive_initial_generation`；
  [第二次](../artifacts/benchmarks/data-monitor-membership-natural-delta-20261001-second.json)
  為 `inconclusive_membership_moved`。**不得把它們說成 parity 通過或效能改善。**
  最後世代清單改善尚未進行新的現場 shadow 試跑，因使用者要求暫停。
- 暫停前已開始的相關測試／audit 程序皆已結束；沒有留下由本輪啟動的
  持續 benchmark 或監控 loop。

## 明確未完成與續接順序

1. 使用者要求繼續後，先重新查 Git／檔案身分／清單與 runtime；資料正在整理，
   不沿用舊路徑、容量、來源數、欄位數或健康快照冒充現況。
2. 重驗最新程式的 lint、相關共用回歸與兩個完整自然世代的 shadow parity。
   有變動／不完整／損壞綁定時，保持 inconclusive 或全量 fallback。
3. 通過後才接正式 per-dataset 投影／分片與原子發布、冷恢復契約。
   維持全來源身分核對、完整欄位、原生整數計算與真實缺項狀態；
   不把此增量分類基礎當成已部署的增量 publisher。
4. 繼續全服務清單上的其他效能與可靠性項目；資料健康、Discord maintenance、
   TAIFEX／OpenBB、全庫、公網 IPv6、browser 與 Windows/WSL 冷啟動等
   尚未由本輪證明全部正常。HTTP 200／active／exit 0 不替代對應驗收。

本輪沒有登入券商、送單、換帳本／模型、修改行情、刪來源、清 OS cache、
重啟核心或提交／回退其他 actor 的工作。所有現有 dirty changes 保留。

## 再次暫停交接（2026-10-01 15:29，Asia/Taipei）

使用者再次明確要求暫停，正在進行資料整理／刪除。以下是恢復工作後的新狀態，
不取代上方 11:43 的歷史證據；以本節作為下一次續接點。
不再啟動新修改、測試、測速、來源掃描、下載或重建。既有網站、交易服務、
收集器與系統排程保持原狀；本次沒有停止或重啟它們，也沒有干预資料整理。

### 保留的新增工作與證據

- 完整世代的 shadow 讀取改為釘住已完成的檔案版本，避免解碼期間下一個
  cache 發布造成無效的競態拒絕；不同來源世代仍不能混用。
- preflight 新增階段耗時與 hint／完整指紋核對的觀察欄位。
  `recent-ctime-desc-v1` 只是目前的試作策略，**未證明自然工作負載整體變快**。
  受控 frozen-metadata ABBA 不能替代完整服務或真實 I/O 的測速。
- 恢復後的共用回歸曾通過 **1,044 項／74.64 秒**；最新已完成的針對性
  回歸為 **109 項／1.59 秒**。它們互有重疊，而且都在最後來源觀察綁定
  修改之前，不能宣稱涵蓋目前所有程式。
- `data-monitor-membership-natural-delta-20261001-latest-generation.json`
  記錄相同 raw revision 下公開 feature rows 不同，candidate parity **不通過**。
- `data-monitor-generation-row-diagnostics-20261001.json` 的完整 rows 重組
  可相符，但現場來源身分仍有 190 個不一致，restricted freshness
  **inconclusive**；這不是正式增量發布或來源完整性驗收成功。

### 最新未完成修改：不得當作已驗收

- `stockagent/live/data_monitor_inventory.py` 新增 `_SourceObservation`、
  `source_observation_root`，fast index 現為 **v4**；cache 外層仍為 v10。
- `stockagent/live/data_monitor_feature_receipt.py` 與
  `scripts/snapshot_data_refresh_services.py` 新增來源觀察根指紋的 receipt
  綁定與重用核對。用途是區分清單驗證與特徵投影之間的來源變動，
  不代表資料發布時間、行情完整或券商成交證據。
- 最後修改通過當時 Ruff，但最後測試命令引用不存在的
  `test/test_data_monitor_feature_receipt.py`，**exit 4、no tests ran**。
  保留 `artifacts/benchmarks/data-monitor-source-observation-first-regression-20261001.log`；
  不把它當作測試通過。沒有為了收尾重跑任何測試。
- 續作必須先處理：record 的實際觀察根指紋與 cache 身分集合不一致時，
  不得發布／重用錯誤 fast index；尤其要測試來源短暫消失又恢復、
  同 footer 統計但檔案身分變動、record／feature 兩階段身分移動。
  shadow 與 benchmark 的新 v4 fixture／receipt 核對也尚未補完。
- 尚未啟用 immutable per-session／per-dataset shards 或正式增量 publisher；
  沒有證明所有服務皆正常或已達效能極限。

### 下一次使用者要求繼續時

1. 先重新核對 Git、實際資料位置、檔案身分與排程狀態；整理／刪除資料之後
   不沿用任何舊的容量、清單、資料數量或健康快照。
2. 先完成上述來源觀察／fast-index 一致性修正與對應測試，再跑正確的
   相關共用回歸、lint 與 shadow；未通過前維持全量 fallback／inconclusive。
3. 重新評估 ctime hint；自然 journal 已有不少 hint 未命中的完整掃描，
   可測試「實際刷新鍵優先＋其餘來源抽樣」但尚未實作或接受此替代策略。
4. 一致性證據成立後，再繼續增量投影與其餘全服務的效能／可靠性項目。

本次查核沒有存活的協作子 agent；已追蹤的本輪測試與 benchmark 呼叫皆已結束。
沒有以停止網站、交易、資料整理或其他 actor 程序的方式暫停優化工作。

## 恢復後的續接點（2026-10-01 下午）

使用者控制的 goal 已恢復為 **active**。上方暫停紀錄仍是歷史，不代表目前暫停。
來源觀察／fast-index／receipt 一致性修正已完成本輪驗收：共用回歸 1,076 項、
gateway 等回歸 160 項（有重疊，不加總），兩個自然完成世代的完整 rows 與局部
footer 重建均相符。最新狀態與續作邊界見
[服務清冊 2026-10-01 16:02 節](service_inventory_2026-09-23.md)及
[驗收與程式／證據 SHA256](../artifacts/benchmarks/data-monitor-source-coherence-validation-20261001.json)。

下一步仍是正式 immutable per-dataset projections／atomic publication 與其餘全服務
的效能、資料健康及可靠性修正；目前尚未部署分片 publisher，沒有全系統完成宣稱。

## 再次暫停交接（2026-10-01 16:28，Asia/Taipei）

使用者正在整理／刪除資料，明確要求手邊工作收尾後暫停。
停止後續效能優化、程式修改與新測試／測速／來源掃描／下載／重建／發布作業；
本次僅核對工作狀態及更新這份交接與既有服務清冊。
沒有停止或重啟既有網站、交易服務、收集器、systemd timers 或其他 actor 的程序；
它們保持原狀，不代表停止所有系統資料寫入。所有 dirty changes 保留，沒有提交或回退。

### 已完成且保留的證據

- 來源觀察、fast-index v4、receipt 綁定與實際刷新鍵提示的修正已完成前一輪驗收；
  不是上方 15:29 時尚未測完的版本。詳見
  [16:02 服務清冊](service_inventory_2026-09-23.md)及
  [驗收與程式／證據 SHA256](../artifacts/benchmarks/data-monitor-source-coherence-validation-20261001.json)。
- [共用回歸](../artifacts/benchmarks/data-monitor-source-coherence-shared-regression-20261001.log)
  為 **1,076 passed／90.59 秒**；
  [gateway 回歸](../artifacts/benchmarks/data-monitor-source-coherence-gateway-regression-20261001.log)
  為 **160 passed／34.16 秒**。兩者有重疊，不加總；不是本次暫停時重新執行。
- [兩個自然世代的 shadow](../artifacts/benchmarks/data-monitor-source-coherence-natural-delta-20261001-new-revision.json)
  已驗證當時完整 rows 重組與局部 footer 重建相同。
  當時的 88,706 rows／59,029 files／來源位置及健康狀態是歷史觀察；
  整理資料後不能沿用它們當作現況。
- 16:28 協作清單只有 root，沒有存活子 agent；僅核對已知 data-monitor benchmark
  與 pytest 程序，沒有找到仍在執行者。本輪沒有新增測試、測速或持續監控 loop，
  沒有需要中斷的本輪已追蹤呼叫；這不是其他資料整理程序已停止的宣稱。

### 尚未實作的下一步

- 正式 immutable per-dataset feature projection shards＋原子完整 JSON 發布
  **尚未實作／部署**；本輪停在介面與一致性設計，沒有半完成的 shard publisher。
- 設計需沿用 canonical field 計算與公開格式，保留全來源逐檔身分核對及完整欄位。
  分片重用需核對上一份 canonical cache 身分、共用 dataset delta、當次逐資料集
  來源觀察根、公開 metadata 及程式 ABI；未知／損壞／跨世代時全量 fallback。
  不能因增量快就跳過 footer／缺項一致性、原子發布或冷恢復驗證。
- 仍需驗證私有分片快取的有界容量、故障／中斷恢復、完整資料逐列一致性與
  真實端到端 ABBA；目前 metadata replay 或 bytes prototype 不是正式發布加速證據。
- 其餘全服務性能與可靠性、資料健康、TAIFEX／OpenBB／Discord maintenance、
  全庫、公網 IPv6／browser／Windows-WSL 冷恢復仍未全部驗收，目標未完成。

### 恢復條件

等使用者明確說「繼續」再恢復。先查 Git／其他 actor 的變更、資料實際位置與
身分、空間／排程及最新 receipts，不沿用整理前的路徑、檔案數或快取有效性。
先做相稱的一致性驗證，再實作上述 shared per-dataset 投影；不自行停止正式服務、
啟動全量重建、改帳本／模型或刪除來源。

## 再次恢復的續接點（2026-10-02，Asia/Taipei）

已重查整理後的 Git、實際資料位置、容量與全服務清單；最新為 66 services、
50 timers、3 paths。正式 per-dataset projection shards 已接入原 30 秒 producer，
不是前述尚未實作的設計；保留完整來源核對與完整舊格式 JSON。
詳見[2026-10-02 20:52 服務清冊](service_inventory_2026-09-23.md)。

最終固定觀察 ABBA／1,185 項共用回歸／126 項針對性測試及現場 journal 有證據；
數字範圍與重疊另列。自然世代 shadow 初次仍 inconclusive，不改成 pass。
其餘全服務、資料健康、全庫、公網／browser／reboot 仍未完成；目標維持 active。
