# 訓練優化成果整理成可重用 skill

## 1. 執行進度

已擴充既有 `stockagent-training-reuse`，並接上
`stockagent-exact-futures-optimization`，不再建立另一套訓練框架或平行帳務 skill。
整理 **18 項方法、8 組歷史案例**，包含適用條件、canonical 程式入口、採用／
拒絕原因、數值等價層級與完整工作量的測速口徑。本次沒有啟動 GPU 訓練、改模型
config、重建 panel、刪 cache 或改正式 checkpoint，也沒有寫個人 memory。

## 2. 為何看起來每次從頭來

過往方法並非沒有實作，而是資訊分散在日期報告、期貨帳務 skill、共用 trainer、
frozen source 與測速收據。原訓練 skill 有生命週期／resume 邊界，卻缺少先判斷
「已實作且生效、此 runtime 未帶入、未實作、已拒絕」的索引。

已有具體重複問題：主 repo 已修 Numba／Torch 共用 OpenMP 初始化，但舊 frozen
source 未帶入；launcher 的 requested threads 又被誤當 actual threads。若接手
只查 main repo 或重跑 profiler，仍會重新探索同一根因。另有獨立 benchmark root
導致已驗證 transform cache 重新擬合，以及 profiling ABI 和實際 inference ABI
不同造成額外 compile。這些都需要責任與 identity 核對，不是重新寫 trainer。

新的入口先讀相符歷史案例與目前 loaded source，只針對變動／未解部分重驗。
換資料寬度、模型、硬體或契約仍需相稱的新測速，不以「避免重做」省略正確性。

## 3. 已整理的方法

| 階段 | 方法與需要保留的條件 |
| --- | --- |
| 訓練前 | 內容定址 normalizer/basis cache；unique panel slab 投影；actual Numba/Torch pool；避免多餘單 fold parent/isolation；持久 compile cache／限定動態軸 |
| 來源搬運 | 批次 CPU allocation/copy；不可變來源的 lossless packed/rectangle H2D；同 slab raw GPU packet cache；共用 base panel與實際 RAM/VRAM guard |
| 訓練中 | VRAM 足夠才保存 encoder activations；固定 block 的 exact recurrence；exogenous metadata hoist；DDP metadata/gradient reduction去重；已有 fused optimizer/projection/kernel |
| 帳務 | prefix sums/searchsorted避免三維FIFO退出表；solvency certificate省已證明無用第二遍；native CUDA scan/index/layout維持真實shape正確性，其他圖仍compile |
| 驗證與訓練後 | model/ledger chunk分開；canonical inference ABI；精確 deployment prefix重用；scoped CPU final replay；rank0 artifact writer＋Gloo等待；所有圖表/checkpoints保留 |

詳細方法與案例放在兩邊相同的 `/root/.codex/skills/stockagent-training-reuse/`：

- [SKILL.md](/root/.codex/skills/stockagent-training-reuse/SKILL.md)：先接續歷史的入口。
- [performance-reuse.md](/root/.codex/skills/stockagent-training-reuse/references/performance-reuse.md)：M01～M18、成本拆解、工具及恢復注意事項。
- [performance-evidence.md](/root/.codex/skills/stockagent-training-reuse/references/performance-evidence.md)：E01～E08、條件、數字與原 repo 報告／收據位置。

原 repo 的 runtime/performance/training 契約仍是單一維護來源，不需要私人 memory
才能理解訓練。skill 只是重用導航，不把舊 batch、CPU threads、compile mode 或
attention 設計固定成永久規則。

## 4. 保留失敗與數值邊界

完整工作流優先於單 kernel，典型例是
[no-basis C0→C2](tw_daytrade_no_basis_pipeline_optimization_2026-10-05.md)：三輪完整
wall 1,396.809→721.961 秒（少48.31%），epoch3 MAX 325.936→194.183 秒。
這是**過去已完成的實測**，本輪只重新讀取收據，不代表本輪再做一次訓練加速。
C2 train 比中間候選 C1 慢，val/test/final較快，所以不能只選 train phase。

嚴格 optimizer bit comparison 仍拒絕；原有 gradient oracle與三輪成交／金融結果
是不同證據，不改 checker、不宣稱未來1000輪永遠同值。稀疏 endpoint、最小
cohort、prepared LRU、較大 batch、chunk8、NVFP4、basis/SVD及取消的 A/B，
都保留拒絕／未完成／架構變更邊界，只有實際條件改變才重開相符實驗。

正式 resume 保留原 optimizer/RMS/RNG/scheduler/early stopping，不能為測一輪
就將1000縮成4而改 scheduler horizon。工程短跑不取代正式 optimizer。

## 5. 使用方式

在 penguin 或 vastai1T 對 Codex 說：

```text
請使用 $stockagent-training-reuse，先重用過往驗證過的優化，
針對這個訓練入口的剩餘瓶頸改善完整工作流，不改目前實驗設定。
```

description 已涵蓋 preparation/train/evaluation/reporting、compile/cache/DDP和
resume；也可依任務自動選用。一般網頁／資料清冊／文字編輯不因此啟動訓練。
這次整理由 `skill-creator` 的按需參考結構與 `check-prompt-audit` 的雙節點
共同基線／不覆蓋未審核差異流程完成。

## 6. 來源、驗證與覆蓋

主資料是本機報告、現有兩個 skill、current code及小型機器收據；原始主對話
沿用上一輪已核來源的 R0021／R0049／R0051／R0055，並讀本輪雙節點增量。
penguin 新增1則要求；Vastai1T沒有新增使用者候選，不重新掃其681MB歷史。
不是逐字重讀所有過往對話，也沒有存取其他平台的私人歷史。

本輪驗收與可復原證據位於
[`20261005T174656Z_training_optimization`](../artifacts/prompt_audit/20261005T174656Z_training_optimization/)：

- 兩個skill的官方 format validator通過；27個技能／報告連結、10個canonical
  程式位置核對通過。測速 helper在兩個節點各有14項 CPU-only行為測試通過，
  涵蓋epoch門檻、新graph、缺MAX、拒絕rank0代用、缺列與壞JSON；不相加成28個
  唯一測試，也不是GPU／完整訓練驗收。
- 重新讀既有完整 A/B 收據，工具給出相同 epoch3 MAX，沒有啟動 GPU。
- [技能合併](../artifacts/prompt_audit/20261005T174656Z_training_optimization/skill-apply-verified/receipt.json)：兩個技能更新至 vastai1T，未修改其他技能內容；共同基線、原版備份與 before.json保留。
- [實際發現](../artifacts/prompt_audit/20261005T174656Z_training_optimization/skill_discovery.json)：兩邊 `skills/list(forceReload=true)` 均啟用這兩個skill、errors為空。
- [零差異再盤點](../artifacts/prompt_audit/20261005T174656Z_training_optimization/skill-idempotency-verified/receipt.json)與[本輪驗收](../artifacts/prompt_audit/20261005T174656Z_training_optimization/acceptance.json)：雙節點33個成員hash/mode一致；相對本輪before只有兩個skills的9個成員變動，其他skills不變。Ruff與`git diff --check`通過。

格式、檔案同步與可發現性各自驗證；不宣稱已量測這個 skill 的長期模型決策
品質，亦不保證所有後續訓練速度或收益。後續新結果應更新這個小型索引與原
任務報告，不逐輪新增 skill 或重做未變動的全套實驗。
