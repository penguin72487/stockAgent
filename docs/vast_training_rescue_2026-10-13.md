# Vast 訓練救援：先保全，故障後由使用者指定新機

## 使用者入口

現在不換機、不租機、不停止現有 GPU 實驗。發生故障時，告訴 penguin 的
agent：「確認救援 `RUN_ID`」，並提供新機的 SSH 主機、port、使用者即可。
agent 當時才產生新的臨時 **SSH 公鑰**；私鑰留在 penguin，不傳給使用者、
新機、Git、Syncthing 或 Discord。這不是 Vast 帳號 API key。

平常查看：

```bash
cd /root/stockAgent
bash scripts/run_training_rescue.sh status
systemctl status stockagent-training-rescue.timer --no-pager
journalctl -u stockagent-training-rescue.service -n 30 --no-pager
```

`recovery_prepared=true` 表示固定程式／環境、完整 checkpoint 和已驗證的冷資料
依賴備妥；**不表示已在新 GPU 續訓**。只有 `resume_progress_verified=true`
表示目的機產生了比救援基線更後面的完整 checkpoint，且已再次回讀保存於 D。

若已確認原 SSH 別名指向另一個 instance／boot，監測顯示
`source_polling.state=waiting_source_reenrollment`，不再每五分鐘登入錯誤機器。
這項負向排程決策綁定原 spec、來源 identity 與 generation，不是時間冷卻，
不證明舊機已停止或 D／NAS 可還原；已保全 checkpoint 及其他工作照常保護。
舊版沒有完整綁定的錯誤會先做一次新身分核對，再建立此等待狀態。
一般 SSH／網路故障仍按原短退避重試；有 active target 的續租及監測不受此等待影響。

修正 SSH 設定、確定仍是原 instance／boot 後，可明確執行原
`register --spec /absolute/path/to/original-spec.json`，或 `capture --run RUN_ID`。
register 須重新核對原來源；capture 須通過完整 D payload 驗證，成功才解除等待。
另一台機器須另走明確登錄／使用者指定的救援流程，不改寫原來源身分。
沒有任何操作時，重複監測不連該別名、不改寫該 run journal，面板保留原錯誤及保存時間。

## 第一性原理與責任分工

恢復訓練需要的是「精確的輸入與程式」和「最近提交的全部訓練狀態」，不是
整台機器的檔案樹。服務、下載器、Git 工作區、可重建 cache 不一起搬走。

```text
Vast canonical train.py
  └─ group-last checkpoint + 同 epoch 曲線 + 設定／normalizer／best weights
       └─ 短暫、封閉的傳輸副本 → SHA 驗證 → D 回讀 → commit ACK
                                                 ↓
使用者給新 SSH + 確認 → 舊 worker 停止證明 → 固定 runtime／source
  → strict CUDA → 固定冷資料按需還原 → 遠端重建 prepared views
  → 原 checkpoint 全狀態還原 → Supervisor + 短租約 → 新 checkpoint 回傳 D
```

| 內容 | 保留位置與方式 | 是否可當作完成／刪除證明 |
| --- | --- | --- |
| 原始資料 | 既有 D immutable cold releases；綁定 manifest SHA，不追 `latest` | 救援不新增刪除權限 |
| prepared views、panel／compile cache | 遠端按固定 builder 重建 | 不在 penguin 常駐另一份 |
| 訓練恢復點 | D 的私有 content-addressed objects；同 SHA 共用 | 未完成 checkpoint 不是完成模型 |
| frozen source ZIP／receipt／config | D 私有救援區；保留原始位元組 | 不改目前 Git HEAD、不改 frozen source |
| 固定執行環境 | D 壓縮 runtime capsule，還原到原 prefix | 須符合 OS／arch／glibc 與 runtime lock |
| journal、租約、SSH 私鑰 | penguin SSD，0600／0700 | journal 不替代 D payload 證明 |

本次新增 D payload 約 8.5 GB runtime，加上約 93 MB 正式恢復點和小型程式包。
三份固定冷資料本來就在 D，約 20.97 GB cold objects，**不再複製進救援區**。
這不是整份資料或磁碟 snapshot；短暫傳輸副本只包含 bounded optimizer context，
有 D ACK 後才回收該副本，不刪原 run。保留歷史恢復點，不自動 GC 冷資料。

私有根目錄 `/srv/stockagent-packed/.local-state/training-rescue` 不加入 Syncthing
payload mirror，也不冒充 lake release head。沿用 D primary mount guard；D 不可用
或剩餘空間低於 64 GiB 時拒絕寫入，不回退到 C。此救援點尚不宣稱獨立 NAS 備份。

## 2026-10-13 已登錄範圍與尚未驗證的邊界

本次只明確登錄 `tw-v6-embedding-d128`，不是自動涵蓋全部實驗：

- 正式 output：`artifacts/markets/tw_scale_cash_annual_last_fold_selected_20261008_v6/embedding__d128`。
- 原訓練已在 epoch 122 的 Broken pipe 後停止；可恢復的完整 checkpoint 為 epoch 121。
- 固定 world size 2、global batch 256，原 fold 11／epoch 上限和 launcher 參數不變。
- 保存 Adam、model、scheduler、scaler、每個 rank 的 RNG、normalizer、early-stop 計數與曲線。
- 不自動重跑完成的其他 v6 cases，也不停止／接管當前 frontier kernel benchmark。

預先驗證包含真實 canonical CPU checkpoint 冷還原後的 optimizer／RNG 續算，
完整權重與 loss 逐值一致；還有 corruption、確認、SSH key、source／runtime
還原、lease replay、子程序退出與 fencing 等測試。CPU 一致性不能推論跨 GPU
硬體 bitwise 一致。新 Vast 的 CUDA、原 prepared 全資料重建、第一個新 epoch
與 WAN RTO，須等使用者提供新機後實測；現在沒有做假故障或啟動新訓練。

### 本次驗收證據（2026-10-13）

- 救援、checkpoint、training lifecycle 與 runtime 相關測試：**213 passed**。
- D 上 25 個獨立 payload objects 已重新逐位元組回讀核對 SHA；真實 epoch 121
  checkpoint 已經冷還原並以 CPU 檢查完整 optimizer、world size 2／batch 256。
- 最新恢復 context 共 16 檔、93,682,072 bytes；closed runtime 壓縮後
  8,514,862,629 bytes。固定三份來源合計 **9,117 原檔**已解碼核對原檔 SHA。
- [本機驗收 receipt](../artifacts/operations/training-rescue/acceptance-20261013.json)
  明確記錄 `real_new_vast_resume_verified=false`；這份 receipt 不是目的 GPU 的完成證明。

需要重新驗收時，使用新的 output 路徑，避免覆蓋既有 receipt：

```bash
sudo nsenter -t 1 -m -- /bin/bash -c 'cd /root/stockAgent; source scripts/runtime_env.sh; run_fintech_python scripts/verify_training_rescue.py --run tw-v6-embedding-d128 --output /root/stockAgent/artifacts/operations/training-rescue/acceptance-NEW_DATE.json'
```

這會完整回讀 D payload、短暫 CPU 還原 checkpoint，並確認 timer 啟用；不啟動訓練。
若 timer 正在保全，驗收等待同一 owner lock，最久 120 秒後拒絕而不是並行寫入。

舊的 preparation build-source 和 code-training 路徑已退役，因此保全原 preparation
release ZIP 的 builder 與 selected_config，不能拿目前 working tree 代替。
`prepared_reconciliation` 逐欄比較 manifest，僅忽略重建耗時；所有 artifact SHA
必須相同後才恢復原 manifest bytes，另留當次重建 receipt。這不是回填來源時間，
不提升 PIT、live eligibility 或 CUDA acceptance。

### 財務與決策時鐘的 CPU 重建驗證（2026-10-26）

共用 adapter 現在接受原財務／決策時鐘 builder 的 `--output`；其餘 builder
維持 `--output-root`。每個 output 必須位於 `artifacts/data_preparation` 的獨立
子目錄，不能涵蓋其他 step 或 raw materialization。`rebuild_source_roots` 可逐步
固定原 preparation code；不執行今日 working tree 代替原始 builder。

`prepared_reconciliation.kind=file_set_v1` 參照有完整 SHA 的
`stockagent.prepared_file_set.v1` 原件清冊；adapter 的唯讀 `prepared-reference`
入口從封閉原件建立它。兩種範圍明確分開：

| artifact_kind | 完整驗證範圍 |
| --- | --- |
| `tw_futures_financial_v1` | 全部 `release/`，以及 `build_acceptance.json`、`accounting_checkpoint.json`、`product_scope.csv`、`lifetime_scope.parquet`；編譯 cache 與整合來源工作副本不在此發布介面範圍 |
| `tw_futures_decision_clocks_v1` | 該次 clock output 的全部檔案，包含已建立的 entry windows、opening references、coverage gaps 與 quarantine |

清冊固定原 builder SHA、original root、scope、完整相對檔名、大小與 SHA。
數值檔必須逐 byte 相同。只為有限大小的已知 JSON 回執保留原 bytes；任意來源
JSON 不視為回執。比較 JSON 時不接受重複 key、非有限數值或 bool/int 型別替換。
財務的兩份 summary 只可改為新的、同相對位置的 `daily`／`rules` 輸出路徑；
`build_acceptance.json` 另允許新的 `created_at_utc`／`elapsed_s`。來源時間、
規則、數值、PIT／research／readiness flags 以及其他欄位一律不能改。

全部原件及 JSON 語義通過後才原子恢復原回執 bytes，實際重建時間與前後 SHA
留在 output 外的 controller marker。每步用 kernel lock 防雙 owner；檢查活躍
builder（含 `--output=...`）後才驗收或隔離中斷半成品；即使 output 尚未 mkdir，
仍檢查已啟動的 writer，避免重試同時開出兩個建置器。已完成 marker 每次重用
仍完整回讀。缺 reconciliation 的 step 在傳輸來源前拒絕，避免傳完才發現不能驗收。

有 `reference_sha256` 的原件清冊必須先保存在既有 D 私有 rescue CAS；來源
驗證會逐份讀回 SHA，並把 `reference_path → SHA` 納入該 spec 的就緒證據。
新機恢復先預檢全部清冊，再用原有 no-overwrite object receiver 傳送，之後才傳
大型 raw sources。缺檔、損壞、過大或 symlink 都在 raw I/O／遠端寫入前拒絕；
參照不能位於任何 rebuild step 的 output 中。同一目標清冊只送一次，已存在且
SHA 相同可重用；同一路徑的衝突 SHA 拒絕。歷史未指定 SHA 的 factorized 參照
仍沿用其原 bootstrap 契約，不能把舊 source-only 證據當成新 pinned oracle 已驗證。

本輪 401 項 preparation／rescue／checkpoint／runtime／lifecycle 相關回歸通過；
當期真實原件的八份 JSON 正例及八個語義改動拒絕案例亦完成獨立 D readback。
後續補上 mkdir 前的 writer 競爭案例後，同範圍 403 項回歸通過；與封閉 D 版本
比較 AST，確認原件清冊生成與驗證程式未改，重用原八正／八反例證據。
證據為 `artifacts/operations/storage-single-authority-20261025/`
`typed-preparation-milestone-readback.private.json` 及
`typed-preparation-writer-guard-readback.private.json`。這是 controller 與固定原件
oracle 的驗收；完整 raw dependency bindings、遠端獨立建置、新目的機 fence／
strict CUDA／新 epoch 及 NAS 備份仍是各自未完成的階段。

加入原件清冊交付後，同範圍 418 項回歸通過，包含實際 byte receiver、重複目標
去重、既有檔重用、全部清冊預檢後才接觸目標、錯誤清冊在 raw I/O 前拒絕及
source-readiness 證據。當期 factorized／financial／clock 三份原件清冊共
16,377,669 bytes 由 D 全 SHA 讀回，財務／clock 的真實八正／八反例再次通過。
證據為 `reference-delivery-milestone-readback.private.json`；兩個當期期貨
enrollment 未改，完整 raw recipe 仍未就緒，未對任何新機傳資料或啟動 builder。

### 有限財務控制檔的表示差異（2026-10-27）

原始完整 CPU builder 的實測出現兩種非數值差異：兩份控制 Parquet 的壓縮／
編碼不同，以及 `zero_print_products` 集合的輸出順序不同，連帶改變引用它們的
SHA。`file_set_v1` 仍維持原有嚴格行為；只有明確指定的 `file_set_v2` 可使用
`stockagent.prepared_file_set.v2` 和額外的 `representation_bundle`。

此 bundle 只含五份固定原件：`lifetime_scope.parquet`、
`release/rules/sources/context_only_suppressed_carries.parquet`、
`release/rules/sources/admission_review.json`、`release/rules/manifest.json`，以及
原 v1 範圍之外、被發布 manifest 引用的 `materialization/manifest.json`。
它是有 SHA／大小上限的恢復控制資源，與原件清冊一樣先保存、完整讀回 D，再由
同一 no-overwrite receiver 交付；不是另一條 raw 管線。可用 adapter 的
`prepared-reference --representation-bundle PATH` 從封閉原件建立，不接受任意
ZIP 成員或額外來源。

仍須先完整檢查全部發布檔。只有上述兩份 Parquet 可比較依序的 Arrow 型別、
數值位元及 null；不排序資料列、不轉型、不四捨五入、不補值，保留 signed zero。
JSON 只允許已指明的相依 SHA 邊、唯一產品集合順序，以及 v1 已允許的建置時間／
輸出根目錄差異；每個 SHA 邊必須先指向實際子檔。數值、產品集合、列序、規則、
PIT／readiness 標記或任意其他欄位改變，都在任何恢復寫入前拒絕。

全部驗證後才恢復有限原件 bytes，最後再次全檔 SHA 驗證；第二次執行仍讀回，
已恢復的原路徑只有在原件 SHA 完全相同時才接受。保留原失敗、診斷和新修復
收據，不用修改 frozen builder 或減少產品／日期來消除差異。此控制契約本身
不證明下一階段 panel／clock 重建、新 GPU 續訓、NAS 還原或 C 原件可退休。

同日後續真實恢復另外發現：遠端 receiver 遇到已存在、完整 SHA 相同的原件
會直接回 ACK，不消耗 stdin；Penguin 的 native D producer 若仍有未讀 stdout，
退出 context 時可能被自己的管線堵住，120 秒後才逾時。`push_object` 與
`push_cold_source` 現在共用 `_push_verified_source`；遇到 BrokenPipe 會完成
有限大小的 D 串流讀取、關閉 stdin，仍檢查 receiver
的 exit／SHA／大小／durable ACK，不延長 timeout，也不覆寫既有遠端原件。
測試使用超過 copy／pipe buffer 的真正 receiver，涵蓋新目標、相同原件與衝突
原件，並要求 native producer 在成功與拒絕時都到 EOF。後續共用回歸
298 項通過；先前只修 metadata 入口的固定版本仍可重現 cold-source 的兩種失敗。
實機 13,907,001-byte 原財務清冊分別經兩個入口重複傳輸，耗時 16.58／14.11 秒；
遠端 inode／ctime／mtime 均未變。此實測的 payload 是既有 private D oracle，
不是新市場下載或任意大型 cold archive 的效能驗收；cold-source 原本的
`None` 回傳介面與不產生第二份 CAS 原檔的規則保持不變。

### 不改寫原 enrollment 的輸入恢復版本（2026-10-27）

原 `spec.json` 維持來源保存的固定合約。補上遺漏的 raw、capsule、control、
CPU 重建及 original oracle 時，可沿同一 rescue owner 執行：

```bash
bash scripts/run_training_rescue.sh register-input-recipe --spec /absolute/reviewed-recovery-spec.json
```

只有上述輸入恢復欄位與 `prepared_build_policy` 可以變動；training argv、
原 config／output／source／runtime、world size、global batch 及 environment
均須完全相同。新版本必須有非空 fixed source 及共用原子 CPU 建置政策。
固定宣告以 canonical JSON SHA 定址，保存在 private D 的
`input-recipes/<run>/<SHA>.json`；SSD `input-recipe.json` 只是已驗證的小型指標。
相同宣告重用原 inode，不改原 enrollment、不連來源或新主機、不啟動訓練。

有效目標 spec 另帶 `input_recipe_revision_sha256`，原來源 capture 仍核對原
spec。完整來源讀回、plan、target、持續 lease 及後續 checkpoint capture 都
綁定相應版本；舊 source proof 不能替新的恢復設定宣告 READY。舊 plan 在
指標變更後失效；已啟動 target 始終使用原批准 plan 的固定版本，不跟隨
moving pointer，也不允許在 active target／migration 途中登錄新版本。

這個入口只登錄已審核依賴，不證明 source 語義相容或歷史完整。實際採用仍
需固定原值／ABI、完整 raw-to-prepared 與逐檔 oracle 驗收；不得拿新數據
改變既有 optimizer 軌跡。NAS 也必須另驗覆蓋新 pointer、D revision、oracle
及完整 source 的固定 export／restore；既有 v3 封閉包保持原 bytes，不能把
本機新版本或 CPU 測試算成 lab203 已採用。兩個 futures 的 v6 原件恢復
recipe 已經原 owner 登錄；原始 enrollment 不變，正式使用仍須該有效
recipe 自己的完整 D 來源讀回。CPU 還原與新 GPU 續訓分開驗收。

### 共用 CPU 衍生資料的完整建置鎖（2026-10-27）

新增明確 opt-in `prepared_build_policy: shared_atomic_v1`；省略時保留原
begin／finish 介面。新政策只接受 `prepare-run`，一次持有原 per-run step
鎖，以及按絕對輸出路徑定址的共同 kernel 鎖，直到原 CPU builder 結束、
所有原件重新驗證及相容 alias 安裝完成。輸出祖先持 shared、葉節點持
exclusive 鎖，避免不同 run 寫入相同或相互包含的目錄。builder 繼承原
lock descriptors；SSH 控制器中斷不會提早解除仍在寫入的 child 的鎖。

共同 claim 位於 `/var/lib/stockagent-training-rescue/prepared-builds`，
身分綁定本階段與上游原 argv、source roots、runtime／公開 environment、
固定 raw／capsule／control 及 original oracle SHA，不綁定另一個訓練
run 的 clock／output 目錄。兩個 run 只有上述身分完全相同才能共用資料。
每次重用仍完整讀回所有原件；claim 或 complete marker 本身不是 READY。
已相同的 factorized manifest 不再重寫 inode、ctime 或重建收據。

外來既有目錄即使 bytes 相同也不自動接管；先前有完整 claim 的失敗工作
只移入自己的精確 quarantine。失敗 log 保留，逾時僅停止本次 owned child
process group，鎖被置換、原件缺失／改值或 recipe 身分不同均拒絕完成。
建置與完成後的完整原件驗證都在失敗處理範圍內；即使原 builder exit 0，
只要 reconciliation 失敗，current claim 和 run marker 也轉為 `failed`，
不再留在 `building`。先追加不可覆寫的 UUID failure journal，保留先前
claim、marker、錯誤階段及 log SHA，再更新 current 指標；重試不改舊 log
或 failure journal。既有 complete 成果若在重用驗證時損壞也會失效，
不以舊 complete 略過。失去 kernel owner 不發布新狀態；SIGKILL 的存活
判斷仍依實際程序和鎖，不假設每次崩潰都能寫出 failed journal。
導入前須完成既有建置 owner 的交接；此 opt-in 不會修改目前 enrollment、
既有來源程序、frozen builder 或自動啟動 GPU。合成 CPU／故障測試與正式
全量 raw-to-prepared、實際 recipe 登錄、新目標故障續訓分開驗收。

原本由 split begin／finish 工程工作產生、但尚未具有 shared claim 的資料，
可以透過 adapter 的明確 `adopt-prepared` 動作交接；不是 `prepare-run`
自動認領。必須固定 prior spec 的 canonical SHA 和原 complete marker 的
file SHA，持有已存在的 `cpu-rebuild.lock`、原 step lock、新 step lock 及
同一階層鎖。原 builder／raw／runtime 輸入需一致；原完整 oracle 及所有
member 先以原 bytes 讀回，再執行完整 reconciliation。舊 owner、lock 或
member 缺失／仍忙／被置換就拒絕，不能補造原 lock 或重編碼 payload。
這個動作只新增 durable ownership metadata，不重寫既有輸出或安裝 alias。
後續另一個相同輸入的 run 仍走原 `prepare-run` 及完整讀回。

`rebuild_working_directories` 可在 `shared_atomic_v1` 中逐步固定資料工作
目錄。省略時沿用對應 `rebuild_source_roots`；明確填相同預設仍保留舊
artifact 身分。不同工作目錄會改變相對來源路徑的解析，因此納入該步及
下游身分，也必須加入 immutable input recipe；不能直接重用另一個 cwd
的 complete claim。目錄需為不經 symlink 的絕對路徑，且不得位於任何
衍生輸出內；執行前必須實際存在。runtime 與 builder 仍從固定 source
release 以絕對路徑啟動，不會改執行資料目錄內的同名程式。這能恢復以
原專案根目錄記錄相對路徑的 raw 清冊，而不複製原件或修改 frozen builder。

節點容量預設仍使用 `MemAvailable` 與各可見 cgroup 的 `max-current`，
不假設回收。`stockagent.remote_build.observe_node` 另提供明確 opt-in
`memory_policy=clean_file_reclaim_v1`，給有完整容量回執的 CPU 恢復判斷：
每個有限 ancestor 的候選額度取 `min(file-shmem, active_file+inactive_file)`，
再扣 dirty、writeback 及全部 unevictable，且不超過 observed current；
最後仍受所有 ancestor 上限與主機 `MemAvailable` 最小值約束。缺少、
負值或重複的必要 counter 拒絕此政策，不把 anon／shmem／slab／swap
當空間。observer 保留未回收額度、原 counter 與新政策身分，後者也進入
machine fingerprint。這是觀測估計，不是記憶體預約或 OOM 不可能的保證。
CPU qualification 使用獨立固定 SHA 的 probe，不改 frozen builder；原
64 GiB admission 門檻保持。額度不足時保存 capacity 並等待，不重跑已完成
builder，也不更改 cgroup／driver、全域 drop cache 或其他程序。counter
定義見 [Linux cgroup v2 官方文件](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html)。

### 明確還原的原版 clock 完整檔集（2026-10-27）

`preserved_file_set_v1` 僅限固定的 `tw_futures_decision_clocks_v1` 及
`shared_atomic_v1`。原版 `file_set_v1` oracle、frozen builder SHA、原
訓練 ABI 和每個成員的 SHA／bytes 都不改；另固定
`original_file_set_bundle: {path, sha256, bytes}`。這個方法還原原版位元組，
不執行 raw clock builder，也不容許 JSON 重編碼、數值容差或浮點 rounding。
恢復方式納入本步及下游的 shared artifact identity；已完成的股票／財報
prefix 可繼續共用，不把原 raw 重建失敗改寫成通過。

原件 ZIP 以 `ORIGINAL_CLOCK_FILES.json` 綁定 oracle SHA／kind／完整邏輯
檔集，每個唯一內容只存一次 `objects/<sha256>`。encoded、decoded 各至多
512 MiB，至多 1,024 個邏輯檔，每檔至多 64 MiB。拒絕額外／缺漏／重複
成員、symlink、加密、路徑重疊、不一致索引及 CRC／SHA／size。只在新建的
owned 輸出內 exclusive 建檔，寫入及目錄同步後，再走原本的全檔 SHA
reconciliation；中斷 partial 沿原 owner 保留、下次只隔離自己的精確輸出。
容量計入全部剩餘增長並保留 25 GiB，generation／owner 改變即拒絕完成。
不同 run 的 complete 重用仍重新讀回每一個原件，不再次解壓。

同一資源清冊涵蓋 D admission、目標 transfer、readiness 與 NAS 封閉包驗證；
新增 archive 不能冒充原 32 MiB metadata，也不能因在 CPU 節點已存在而
從 portable recovery 中省略。實際 D 還原、正式 input-recipe 登錄、NAS
獨立恢復與 GPU 續訓分開驗收。協定及相關恢復回歸
`preserved-clocks-all-recovery-re-20261027T013356-de2597c5` 為 **1,286 項
通過、224.83 秒**；這不是 GPU 或全市場資料完整性驗收。

### 明確保留的 stock 輔助原件（2026-10-27）

`factorized_manifest_v1` 保持原本的逐位元重建要求。另以明確 opt-in 的
`factorized_preserved_auxiliary_v1` 支援 futures stock-context ABI：所有模型
成員仍須先符合原 SHA；只有 feature dictionary、observations 與 annual
events 可以由另行固定 SHA／bytes 的原件包恢復。原 manifest 的非 SHA
欄位、列數、來源及研究／PIT／live 限制須一致，不能把備份恢復說成重新
計算，也不修改 frozen factorized producer。原件包至多 8 GiB，最多 1,024
成員，每個 decoded 成員至多 1 GiB；檔名、重複項、ZIP CRC、完整 SHA 及
所有輸入 generation 都需通過，才可原子替換 owned 候選輸出。

旁置 staging 先保存固定 owner plan，沿用原 step／階層鎖；已完成的 staged
檔案以完整 SHA 重用，未完成者須逐 byte 驗證原件 prefix，才可只追加缺少
尾段。不覆寫損壞 prefix、不另開完整工作副本；容量只計實際剩餘增長，仍
保留原 25 GiB 門檻。中斷後已恢復與未恢復的原件可以分別核對再繼續，
最後仍走原全成員驗證。readiness 及 D／目標 transport 共用同一份資源清冊，
包含既有 financial representation bundle 和新增輔助原件包；缺少任一項
均不宣告來源 READY。

immutable input recipe SHA 必須納入備份依賴。原 manifest 已固定每個輸出
成員 SHA，故兩種恢復方式若產出完全相同的原件，保留相同 shared artifact
identity；原件包的 transport encoding 不另複製一份相同產物。共享重用
仍要求目前 recipe 的完整資源與原件讀回。合成測試、當期 D 原件包的完整
遠端解碼、實際 CPU 恢復與新 GPU 續訓各自驗收；前者不能替代後者。

### 封閉備份的輸入版本驗證（2026-10-27）

原 `stockagent_private_rescue_NAS_closed_plan_v1` 的兩個 run-control 及
namespace 保持原約束。新版 `...closed_plan_v2` 另外固定 optional
`input-recipe.json`、active target 所需 `target-plan.json` 與 D 的
`input-recipes/<run>/<SHA>.json`；run binding 分別列原 enrollment、目前
recovery spec 及執行中 spec 的 SHA。所有控制原件仍由原 encrypted private
CAS 備份管線送達，不讀本機 moving pointer 來修補缺少的還原內容。

`verify_training_rescue.py --restored-root ...` 可用標準函式庫在獨立目錄
讀回兩版封閉 plan。新版本驗證還會納入 financial representation bundle。
舊 checkpoint 可繼續保存，但舊 source proof 不會替新的 recipe 宣告 READY；
尚無相應 full D source readback 時，結果明列 pending。active target 固定
原批准版本，即使備份中 recovery pointer 已不同，也不能改寫執行身分。
這是全檔 SHA 與 recorded binding 稽核，不反序列化 checkpoint、不解碼所有
raw、不啟動 GPU，也不宣稱 NAS semantic restore 或 lab203 已部署新版工具。
既有 v3 封閉工具包及第一批 NAS plan 原 bytes 保持不變。

### 固定 raw 版本、原路徑與快取租約（2026-10-26）

`data_sources` 的 packed binding 可加入 `links`（原路徑清單，指向整個固定
release）及 `subtree_links`（`原絕對路徑 → release 內相對目錄`）。例如財務
source 指向原 2026-10-17 margin snapshot，rule delta 則引用固定
`tw-index-futures` 版本中的 `preparation_sources/margin_repair_pending`；不複製
成另一份 raw 正本，也不以 moving latest 代替原 snapshot ID。

恢復會先核對並交付全部 oracle、解析所有固定來源，傳送一份 ≤512 KiB、SHA
定址的 canonical `materialized_cache.py` 控制資源，再預檢每個 source 的原路徑。
既有檔案／資料夾、指向不同或不存在目標的 alias、redirected parent 都保留並
拒絕。傳完固定來源後，沿原 `use_materialized_snapshot` 完整驗證、建立／續訂
七天租約與 original-path aliases，才執行 CPU builders。競爭建立 alias 時使用
exclusive symlink，不以 rename 蓋過新出現的名稱。

有 subtree aliases 的租約為 schema 2，保留同一完整 release 與各 alias 的相對
子目錄；同一 GC owner 只在原有 recovery／process／pin／lease gates 通過後
回收 hot view 和仍指向它的登錄 alias。舊 cache 程式不認得 schema 2 時會拒絕，
不能將它當成 schema 1 清理。控制資源只由該 bootstrap 子程序載入；原 frozen
packed reader、builder、model、config 與 runtime 沒有換成今日 working tree。

full-source D 驗證改用已有的 `reconstruct_all` 與 native chunk reader，保留全部
原檔 SHA，同時去掉全量 paths 物件、重複 inventory 載入、ZIP CRC／原檔二次
解壓及 chunk 重複冷讀。完整吞吐仍需按實際來源與當時負載量測。

`frozen-cache-controller-compatibility.private.json` 是在原 Vast boot、所選
`c4cff6...` frozen packed code 上的隔離小樣本：兩份原值共 38 bytes，完整
解壓、root／subtree aliases、重複使用 inode、衝突名稱保留與 schema 2 租約均
通過。它不代表真實財務／feature 全量重建、七天實際經過或 GPU 續訓完成。

目前 captured runtime 和歷史 run metadata 的兩個非數值差異是 `soupsieve`
版本與 StockAgent distribution registration；已記錄於 enrollment receipt。
Python／Torch／NumPy／Arrow／Polars／Triton／FlashAttention／xFormers 版本
不允許默默漂移，目的機須與保全的 runtime lock 一致。

## 2026-10-26：目前期貨訓練保全與可續傳修正

`tw-v6-embedding-d128` 保留原身分；目前 SSH alias 已指向不同 boot／instance，
舊 run 的身分拒絕不能用重新登錄掩蓋。另以不同不可變 run ID 被動保全當前
`futures-history-v3-capacity-0845-attempt00` 與
`futures-history-v3-capacity-0900-attempt00`。未接管或重啟訓練。
固定驗收點分別為 epoch 255、80，各 18 檔；D 獨立 SHA 與 CPU checkpoint
檢查包含完整 optimizer、2 rank RNG、global batch 128、相同 epoch 曲線。
timer 可持續前進；上述固定驗收點不冒稱查詢當下的最新 epoch。

當前 `/venv/main` runtime/source 已保全 5 個私有 D 物件，共 4,483,895,485
bytes。獨立串流解碼核對 runtime 全 39,732 成員、7,895,332,601 logical
bytes、1,154 內部 links，以及 source ZIP 全 1,855 成員與 wheel SHA；
C 沒有展開大副本。這兩個新 run 的完整 raw dependency／builder／bootstrap
登錄仍未驗收，`recovery_prepared` 不能因此改填 true；未測新 GPU 故障續訓。

傳輸現在使用每物件鎖及私有 durable intent。只有綁定不可變來源 SHA／大小的
確切 partial 能續傳；核對本地 prefix SHA、遠端 prefix SHA，才傳 suffix。
重試仍須完整來源 SHA、最終大小及 D 完整獨立讀回，SSH 失敗的 stderr 僅保留
有界私有診斷。舊無 intent partial 須以精確名稱及新鮮程序 FD／mmap 檢查
明確接手，不以 glob 猜測、覆寫或刪除其他 partial。
實際 4,455,174,291-byte runtime 已續用 2,480,766,976-byte partial，
只補 1,974,407,315 bytes；完整 D 原值讀回成功。

新 runtime proof 的 `link_target_encoding=raw_readlink_v2` 保存 `os.readlink`
的字面目標。舊無此標記的 proof 依原 `Path.readlink()` 正規化規則核對；
原 tar 字串與原始 proof 均不改寫，未知 encoding 拒絕。相關續傳、物件鎖、
原值與舊／新 link 相容性，以及 bootstrap 來源身分邊界測試共 172 項通過。
`seal-bootstrap` 在保全前後核對 enrolled 主機／boot 身分，來源中途更換就不提交
bootstrap；已保全的不可變物件仍保留。回報契約為
`fixed_source_runtime_preservation_v2`，`source_inputs_rebuilt_on_remote=false`：
這個動作只保全，不執行 raw input builder。完整固定證據與限制見
[分工儲存驗收](storage_roles_and_tiers_2026-10-25.md)。

後續再修正空來源集合的準備判定：`data_sources=[]` 僅允許保全，不得視為
沒有輸入需要恢復。狀態、plan 及已批准 plan 的執行都要求非空固定來源，
並核對完整原值讀回收據與當前 spec digest；空集合的驗證與恢復拒絕。
回報契約 `pinned_sources_and_runtime_v2`，含五項新邊界的最終相關回歸共
177 項通過。該條件不宣稱所有未登錄 dependency 自動完整，仍須先完成
當期 raw dependency 與原 builder 的綁定及重建驗收。

2026-10-26 再完成當期來源的兩項獨立 D 原值讀回：共用 tick 7,672 原件，
以及 financial 的 55,545 不同內容與兩份 manifest。後者重用 D 既有
55,541 原件，只補四份准入證據；沒有再傳四 GB 相同內容或建立 C 工作樹。
26 份固定清單、驗證程式與讀回證據共 61,588,413 bytes 已封存在同一私有
D CAS，並以 native batch 完整 SHA 再驗；其 immutable index 為
`27197ebb6548e040b44be7f0a40958211a805f4f3e5537142555eeb5ccf0ad92`。
私有 D 的 `evidence/storage-single-authority-20261025/` 有同名小型 anchor，
因此不需只靠 C 的操作目錄查找恢復清單。這個 evidence index 沒有建立新
source head、改 enrollment、使 `recovery_prepared` 變 true 或授權來源退休。
完整 scope、時間與未完成項見[分工儲存驗收](storage_roles_and_tiers_2026-10-25.md)。

同日再完成固定台股來源版本全部 5,917 原檔、5,303,374,780 bytes 的 D
原值解碼。原始 source manifest 與 packed transport manifest 各自核對，
沒有把兩層 SHA 不同當成毀損，也沒有在 C 展開副本。原 canonical reader
新增有 headroom admission 的 128 MiB 有界 pack buffer；實際單 pack 全原值
ABBA 為 5.9353 → 3.0038 秒，完整版本的另一次驗收為 150.75 秒。
前者是階段速度，後者不是全訓練流程 benchmark，均不代表已移轉續訓。

`current-recovery-closure-worklist.private.json` 明列仍缺完整固定重建 recipe、
遠端 raw-to-input 驗收、新 GPU fence／strict CUDA、NAS 獨立恢復與 C 退休。
原 producer 維持唯讀，兩份 checkpoint-only enrollment 的
`recovery_prepared` 仍是 false；不可拿新增 D 原件證據略過原恢復 gates。

## 故障時的流程與指令

### 1. 發出本次救援公鑰

```bash
bash scripts/run_training_rescue.sh key --hours 24
```

輸出 `key_id`、`public_key`、`authorized_keys_line` 和 expiry UTC。
以 `authorized_keys_line` 加到 **選定新機**的 `authorized_keys` 可立即由 OpenSSH
強制 `restrict,expiry-time`。如果 Vast UI 只接受裸公鑰，首次 bootstrap 會加上
相同限制；若尚未 bootstrap，帳號級裸公鑰不會因本機 key journal 到期就自動
從 Vast 消失，取消救援時要在 Vast UI 移除它。程式不自行修改 Vast 帳號級 keys。

新機需標準 Linux x86_64 Vast Ubuntu/CUDA image、root 或等效管理權限、
Supervisor owner、至少原 world size 的正常 GPU 和足夠儲存空間。
本次 cheap preflight 要求至少 128 GiB 空間；GPU／RAM 還須容納原 batch／模型，
不能因新機較小便默默降低 batch。缺少 userspace C++ build tools 時可在已確認的新機
安裝 `build-essential`，不重裝 host driver。
本次 runtime 是 glibc 2.39、原 prefix `/venv/fintech`；不覆寫新機已有的不同環境。
不安裝／重置 host NVIDIA driver；不支援從 CPU image 默默退回 CPU 訓練。

### 2. 固定目的地主機身分並產生方案

取得新機 ed25519 host-key line，透過可信的控制台／指紋渠道核對。
`ssh-keyscan` 只是取得候選 key，不等於認證。不要使用 `StrictHostKeyChecking=no`。

```bash
bash scripts/run_training_rescue.sh plan \
  --run tw-v6-embedding-d128 \
  --host NEW_HOST --port NEW_PORT --user root \
  --key-id ISSUED_KEY_ID \
  --host-key-file /absolute/private/verified-new-vast-known-hosts
```

方案綁定目的 SSH、host key、key ID、原 spec、D checkpoint／bootstrap、來源身分、
source generation、destination generation。此步不登入／修改新機；有效期一小時。
用戶確認綁定的 `plan_id` 後才執行；不租機、不選 offer、不更動帳單或 Destroy。

### 3. 取得舊 worker 的真正停止證明

目前原 run 是 legacy launcher，不能把失聯或 GPU fault 當作已停止。
若需要停止整個 Vast instance，先取得使用者對該 instance 的確認並由正式管理入口
停止；救援程式本身 **不呼叫 Stop**。下列命令只 GET 舊 instance，必須看到
相同 instance ID 的 `actual_status=stopped`；`intended_status=stopped`、offline、
Stop accepted 和 SSH timeout 都不通過。

```bash
bash scripts/run_training_rescue.sh fence-provider \
  --run tw-v6-embedding-d128 \
  --api-key-file /absolute/private/vast-read-api-key
```

API key 檔案須 0600；也可使用已安全注入程序的 `VAST_API_KEY`，不要將 token 放在
命令列、log 或聊天。這個帳號 API token 與給新機的 SSH 公鑰是不同東西。
輸出私有 receipt 路徑，只保存 allowlisted readback，不保存 provider `extra_env`。
之後若舊 run 已由本救援 guardian 管理，可以只停止該 run：

```bash
bash scripts/run_training_rescue.sh fence-guardian --run tw-v6-embedding-d128
```

它停止續租，只向符合 boot／PID start-time／generation 的本程式 guardian 發訊號，
並驗證所有 owned DDP 子程序已離開；不影響別的 job。無證明就停在等待確認。

### 4. 明確確認後恢復

```bash
bash scripts/run_training_rescue.sh recover \
  --plan-id EXACT_PLAN_ID --confirm EXACT_PLAN_ID \
  --old-node-stopped-receipt /absolute/private/generated-fence.json
```

先驗證確認與新鮮來源停止 receipt，再持久化 generation fence，才接觸新機。
bootstrap 很久時，launch 前再次讀回舊機停止狀態；不是重播初始 Stop ACK。
恢復順序：固定 runtime → 固定 source → runtime lock → strict CUDA
allocation／kernel／synchronize → 只取三份固定來源 → 原 builder 遠端重建
→ 完整 SHA 與原 prepared ABI → 完整 checkpoint／mode → Supervisor launch。

新機仍呼叫原 `train.py --resume`，沒有另一套 trainer／loss／帳本。
已驗證的 closed objects 可重用；半套 transfer／prepared build 不發布。
已開始的 recovery 不能重播 bootstrap 去覆蓋正在前進的 run。

```bash
bash scripts/run_training_rescue.sh cycle
bash scripts/run_training_rescue.sh status
```

`target_restored_guardian_launched` 只證明已 launch，不是續訓驗收。要看到新的完整
checkpoint epoch 超過基線，而且 D 逐檔回讀成功，才標示 `resume_progress_verified`。
成功後撤銷 incident bootstrap 公鑰；run-scoped forced-command control key
只可 lease／status／health／checkpoint read／owned stop，不能執行 shell 或轉送 port。

手動取消／撤銷 incident key：

```bash
bash scripts/run_training_rescue.sh revoke-key --key-id ISSUED_KEY_ID
```

不刪除其他 SSH authorizations，也不因 bootstrap key 到期而中斷正常續訓。

## 平常保全與增加保護範圍

```bash
# 立即保全最近閉合的正式 epoch，不啟動訓練
bash scripts/run_training_rescue.sh capture --run tw-v6-embedding-d128

# 固定 source、config、reference manifests 與 closed runtime capsule
bash scripts/run_training_rescue.sh seal-bootstrap --run tw-v6-embedding-d128

# 固定來源 full decode／SHA，沒有 C 熱還原副本
bash scripts/run_training_rescue.sh verify-sources --run tw-v6-embedding-d128
```

長操作可用既有 `stockagent-agent run` 監督。`verify-sources` 由同一 rescue owner
先短暫鎖定原 enrollment／有效 input recipe，逐 run 的既有 prepared-lock 實作
保護長時間讀取；完整 D SHA／解碼不占用全域控制鎖，checkpoint capture、fence
及 lease supervision 可以繼續。提交時重新取得控制鎖，核對 recipe、原件 generation
與 claim inode，讀取**最新** journal，只合併 `input_verification`，不覆蓋期間新增的
checkpoint 或 fence。初始 admission 可跳過忙碌 owner；解碼完成後提交至少容許
120 秒的有界等待，避免僅因重疊一筆短保全而重做全量 IO。其他寫入操作仍使用
原全域 owner；直接呼叫 `verify_sources()` 時不要再外包同一把控制鎖。
`owner_busy_no_duplicate_operation`／exit 75 表示保留現有作業、稍後重試。
長保全期間仍維持已啟動健康目標的 lease，避免 WAN I/O 阻塞續租。

要保護另一個 existing TW factorized value-only 正式 run，先確認 source release 和
runtime capsule 對應該 run，再執行：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/enroll_training_rescue.py \
  --run UNIQUE_RUN_ID \
  --output-root /absolute/artifacts/markets/FORMAL_RUN \
  --config-file /absolute/FORMAL_RUN/generated_config.yaml \
  --source-receipt /absolute/code-release/release.json \
  --runtime-capsule /absolute/private/closed-runtime.tar.gz
```

此 enroller 用已登錄 SSH profile，推導 world size／batch／原 launcher／builder 和固定
三類來源；不啟動訓練。其他產品須先審查 spec，再用 `register --spec /absolute/spec.json`，
不可猜它們的期貨原始資料、單位、因果時鐘或 feature dependency。

runtime capsule 在原機用 **該 run 的** runtime 建立，輸出必須是新路徑：

```bash
# 在已授權的原 Vast shell；adapter 由 register 安裝在 frozen tree 外
source /root/stockAgent/scripts/runtime_env.sh
run_fintech_python -c 'import sys; from pathlib import Path; sys.path.insert(0,"/opt/stockagent-training-rescue"); from worker import freeze_runtime; print(freeze_runtime(Path(sys.prefix),Path("/var/lib/stockagent-training-rescue/runtime-capsules/NEW_ID.tar.gz")))'
```

不執行 `conda update`、`pip install -U` 或把 environment.yml 當作已凍結執行環境。
seal 時再比對 capsule 和現行 prefix 的完整檔案 SHA；不符合時須先釐清漂移。

## 補齊原 checkpoint-only 登錄的啟動保全

原 `spec.json` 不改寫。已存在 D 的 runtime archive／proof、frozen source ZIP、
wheel、release receipt 與原訓練設定可用 `bind-bootstrap` 綁定，使用
`stockagent.training_bootstrap_binding.v2`；它不啟動訓練、不更換環境、不選 GPU。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/training_rescue.py --owner-wait-seconds 120 \
  bind-bootstrap --request /absolute/path/to/pinned-attachment.private.json
```

request 為 `stockagent.training_bootstrap_attachment.v1`，列明 `run_id`、
原 `spec_sha256`、原 `source_identity`、`checkpoint_commit_id`、`files`、
`runtime_archive`、`runtime_archive_proof`。每個 file 固定原 source／target 路徑、
role、SHA、bytes；archive／proof 固定來源路徑、SHA、bytes。只補原 file 缺少的
大小，新增的程式檔限原 release receipt 指定的 ZIP／wheel／receipt，不接受任意
新 code 或當前環境 lock。runtime lock 必須讀自該 commit 的原
`run_manifest.json.runtime_provenance.runtime`，並和當前來源 prefix 的完整檔案、
symlink、套件及 frozen source 核對。此 manifest 本身也納入封閉 NAS 清冊。

request 亦可為最多 16 個不同 run 的明確 JSON list。共用 archive／proof 在同次
呼叫只完整讀回一次，後續綁定仍核對 inode／ctime 等 generation；不同程序不共用
這個暫時結果，也不忽略每個 run 的設定及來源身份。每個 run 單獨提交，批次中途
失敗時保留已完成綁定及原 checkpoint，修復後可按原 request 冪等重試。
長時間解碼在原全域 owner lock 外；最後重新讀取最新 journal 再合併 bootstrap，
不覆蓋期間的新 checkpoint、fence 或輸入 revision。重複成功 request 不重寫原控制檔。

`plan`、確認執行、正式 D 驗收及離線還原使用同一 binding validator；v2 的
runtime lock 由 bootstrap 傳給原還原檢查，不把它偷偷注入原訓練 spec。
舊 bootstrap 維持原 file-list 等值驗證。新型 bootstrap 的離線驗證需要同包的
`stockagent/training_rescue_bootstrap.py`，須沿既有 lab203 tool handoff 更新；
不可拿舊 v1／v2 已完成 NAS 批次替新綁定背書。

## 監控、租約、失敗與延遲

`status` 的 `bootstrap_declaration`（`bootstrap_enrollment_declaration_v1`）
另列原登錄缺少的 runtime capsule 路徑、檔案 SHA／大小、runtime lock、
設定與 source release 綁定。這是無遠端連線、無 payload 讀取的宣告檢查，
`metadata_only=true`，不能替代 `bootstrap_preserved`、完整 D 讀回、
prepared 重建或 GPU 續訓。`seal-bootstrap` 在缺少 capsule 或必要檔案
metadata 時，先回報 `bootstrap preservation enrollment is incomplete`，
不登入來源、不寫 bootstrap／checkpoint journal；checkpoint-only 登錄仍可
繼續保存 checkpoint，原 enrollment 不改寫。
`bootstrap_declaration` 仍只描述原 spec；另外的 `bootstrap_binding_contract`
顯示已附掛 v2 啟動保全，原 spec 缺欄位不等於已保存的 binding 無效。

固定輸入規格另支援 `data_capsules`、`input_controls` 與 `prepared_links`。
`data_capsules` 用既有保全物件的 archive／proof／source-plan SHA 與確切大小，
還原 tick、區塊與回執等既有原件。沿用同一 tar decoder，逐檔核對 SHA、mode、
奈秒 mtime、完整成員集合、gzip CRC／EOF 與 tar 結尾；D 端可串流驗證而不在 C
解壓。Vast 的相同 capsule 共用一份不可變工作樹，任何異質目標或未知 partial
保留，不覆蓋原訓練輸入。工作樹由現有 rescue owner 管理，七日 `retain_until`
只是保留提示，不授權自動回收；它不是另一份冷庫正本。

`input_controls` 提供原始 config／quarantine 等小型輸入，最多 512 檔、單檔
32 MiB、合計 128 MiB。所有原件清冊與控制物件先在 D 全 SHA 預檢，之後才
交付目的地。`prepared_links` 將原設定中的舊 prepared 路徑指向新工作樹；
大型傳輸前先檢查衝突，對應 builder 的完整原件比對通過後，在同一 step lock
下才建立 no-overwrite 連結。重試也重新驗證內容，保持原 config／builder／ABI。

接收端的 `durable` 包含資料、原始權限、最終檔名與同檔案系統上的祖先目錄；
先同步內容與權限，再原子建立不覆寫的檔名，最後同步目錄才回覆。
原先僅同步檔案內容不足以保證目錄項目持久化，依據為
[Linux fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)。
同步失敗不回覆成功，保留新 partial／已驗證原檔；重試重新完整 SHA 及同步，
不重寫相同內容或無故更改 ctime。檔案／目錄替換、FIFO、symlink 與跨 consumer
的 hardlink 權限更動均拒絕。跨掛載只同步目標檔案系統，仍核對完整路徑身分；
這些 API 與故障注入驗證不能宣稱已完成真實硬體斷電測試。

2026-10-26 的工程驗證為 952 項回歸通過，以及既有凍結 C4 程式下，真實
7,672 個 tick／區塊原件、4,125,574,234 bytes 的遠端還原與重複全檔讀回。
當期三份 packed source、一份 capsule、12 份控制檔、三份 original oracle
與三個 prepared 相容路徑已整理成私有 proposed spec，未取代原 enrollment。
這些證據不代表三個原 CPU builder 已重新執行或新 GPU 已成功續訓；階段邊界見
[分工儲存驗收紀錄](storage_roles_and_tiers_2026-10-25.md)。

- Penguin timer 在作業結束後 30 秒再巡檢；不是保證每 30 秒產生 checkpoint。
- 來源用低侵入 NVML／recovery-action、SSH、canonical lifecycle 狀態監控；NVML
  visible 不是 CUDA compute proof。新機先做 strict CUDA，再以新 checkpoint 驗收。
- 目的 guardian 租約 120 秒，綁定 run／spec／boot／generation、遞增 sequence，
  用 monotonic deadline 防 replay。中斷續租後終止其 owned process group，
  清掉 descendants 才釋放既有 GPU lease locks，不啟動第二個替代 trainer。
- D 掛載失效、controller 長時間不可達或 GPU fault，受管理的目的 run 會 fail closed；
  這是避免雙跑／無保全前進的取捨，不等於永不中斷。
- RPO 是「最後成功提交並 D ACK 的 epoch」，不是故障前每個 optimizer batch。
- RTO 含 runtime／固定來源傳輸、prepared 重建、strict CUDA、編譯及首個新 epoch。
  不搬整盤、不跑 solver，減少可避免工作；但尚未測得新 Vast WAN 的恢復秒數。
- 若 state 是 `awaiting_user_confirmation`，只保存已有證據；不選機、租機、自動
  restart 或 Destroy。半成品和歷史 receipt 留下，不刪唯一冷資料。

工程實作：`stockagent/training_rescue.py`、`stockagent/training_rescue_worker.py`；
測試：`test/test_training_rescue.py`。這是 active checkpoint preservation owner，
不取代 completed-training return／NAS 備份 owner，沒有提供模型部署或遠端清理 ACK。

## 當期私有 NAS 清冊與交接

由原 rescue wrapper 產生新的封閉 v2 清冊，讓掛載命名空間、工作目錄與
runtime 都由既有入口選定：

```bash
bash scripts/run_training_rescue.sh --owner-wait-seconds 120 inventory-backup \
  --output /absolute/path/to/new-recovery-plan.private.json
```

`scripts/inventory_training_rescue.py` 共用產生器先在原 owner 下凍結 bounded
spec／state、current input-recipe 及 active target plan，再釋放鎖，將小型
控制檔保存到原 D CAS 並清點 immutable cohort。它不登入來源、不啟動訓練、
不改 source enrollment，也不刪原檔或失敗 partial。每個必要 resource 都要
在清冊內，原 enrollment、當期 recovery spec、既有 target execution spec
各自核對；不能拿舊 source proof 宣稱新版準備完成。

清冊只證明凍結控制與 metadata 身分。沿既有 encrypted Restic owner 發布時，
仍逐檔完整 copy SHA 及 destination SHA，NAS 再從所有固定 parts 獨立還原。
維持 8 GiB batch、24 GiB staging、32 GiB pending、64 GiB reserve；沒有
將所有私有原始資料擴為公開 lake。metadata census 超過界線就保留來源並拒絕，
不因多個 CAS 名稱看似有效就略過 full-byte 驗證。

lab203 應用完整的 `tools/storage-relay-20261027-v7/`，其離線依賴包含
input-recipe、原版 clock file-set、bootstrap binding 及固定底本增量還原。
原始全量交接見 [新版接手文件](lab203_revised_recovery_handoff_2026-10-27.md)，
增量命令與固定 plan 身分見 [增量接手文件](lab203_incremental_private_backup_handoff_2026-10-27.md)；
Penguin 傳輸讀回、lab203 實際採用、NAS file restore、checkpoint CPU 與新 GPU
續訓仍分開驗收。舊 v3 與舊 Restic 歷史保留。

### 固定底本的增量備份

`stockagent.training_rescue_backup` 共用原分批及 JSON 封裝格式，產生
`stockagent.private_rescue_incremental_backup.v1`。只有同一私有 authority
的兩份封閉 v2 plan、每個底本成員的 SHA／bytes 完全相同，且全部底本批次
都具有精確 paired NAS file-restore ACK，才允許只傳新成員。manifest 固定
兩份 plan 原 bytes、完整舊 dispatch／ACK、paired producer／receiver／repo、
分批上限與新成員清冊。任何底本刪減、變更或缺 ACK 均拒絕增量。

沿原 encrypted Restic publisher、owner、配額、容量與中斷 staging 接續，
不開第二條備份管線。每個新成員仍驗來源完整 copy SHA 及目的完整 SHA；
重用的底本沒有在本次發布重新讀 D，收據必須分開記錄這個範圍。控制檔先
按明確格式封裝：新 incremental manifest 全部遞迴排序 keys，舊 envelope
保持原 writer 的 insertion order。存讀後重排字典不得改變新批 identity；
也不能用語義相同的另一種編碼取代舊收據。

離線 verifier 的新參數為 `--base-restored-part`（按 ordinal 重複）、
`--base-restored-plan`、`--incremental-manifest` 與固定 `--incremental-id`。
舊批保留原 plan，新批另帶 current plan、base plan 及增量 manifest；每根
payload 必須屬於原固定批次。完整聯集重新驗每檔全 SHA、exact set 與前後
generation，輸出 offline-readback v3；不複製另一份 payload。舊全量 v1／v2
入口維持相容。歷史 ACK 只減少傳輸，不能省掉本次完整還原，也不證明 GPU
optimizer resume、NAS 語義還原或可刪原件。

依據：[Vast instance readback](https://docs.vast.ai/api-reference/instances/show-instance)、
[OpenSSH authorized_keys](https://man.openbsd.org/sshd.8#AUTHORIZED_KEYS_FILE)、
[PyTorch reproducibility boundary](https://docs.pytorch.org/docs/2.11/notes/randomness.html)。
