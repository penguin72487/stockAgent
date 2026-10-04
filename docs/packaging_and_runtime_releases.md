# 程式發布、角色依賴與節點環境

`pyproject.toml` 現在可建置 Python wheel。包裝只搜尋明確的 Python package，
避免把資料、artifact 和私人設定放進 wheel，亦避免搜尋大型資料目錄。
`uv.lock` 管理可攜的 Python 依賴解析；每個節點的受驗 CUDA 環境另有 runtime lock。
兩者的責任不同：lock 中的 PyTorch 版本不代表所有主機的 driver 都能執行它。

## 角色

| 角色 | pyproject extra | 執行與資料責任 |
| --- | --- | --- |
| 基礎程式／資料型別 | 無 | NumPy、Arrow、Polars、YAML 和 HTTP 基礎；不啟動工作 |
| GPU 訓練 | training | 由既有 fintech/CUDA 環境與 preflight 決定原生套件，不直接替換成 lock 的 GPU wheel |
| CPU boosting | boosting | 不另造 neural executor |
| 來源取得 | acquisition | canonical collector、SQLite／receipts／配額續傳；來源仍由 catalog 管理 |
| 盤中與命令服務 | services | Discord／Shioaji，保持獨立程序與 existing unit |
| 監控／分析 | analytics | DuckDB 與 msgspec 私有快取解碼；公網 JSON 寫入格式維持 canonical owner |
| 跨節點工程控制 | control | psycopg／專用 PostgreSQL；唯讀 code-verification trial，collector／GPU／帳本保留原 owner |

FlashAttention、xFormers、Transformer Engine 和 OCR 原生套件維持既有硬體環境
與獨立 requirements。它們會出現在節點 runtime identity，沒有被卸載或改成
portable lock 的假相容輪子。新建一般環境可使用 `uv sync --locked
--no-default-groups`；不要以 `uv sync --active` 修剪現有 fintech 的受驗依賴。

## 建置與驗證

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict \
  --runtime-lock-output artifacts/operations/CURRENT/runtime-lock.json
run_fintech_python scripts/check_environment.py --require-cuda --strict \
  --expected-runtime-lock artifacts/operations/CURRENT/runtime-lock.json

run_fintech_python scripts/build_project_release.py \
  --config configs/markets/SELECTED.yaml \
  --output-dir artifacts/operations/CURRENT/releases
```

建置使用現有受驗 Python 與 build tools、離線執行，逐個核對 wheel 的 Python
原始碼 SHA。來源在建置期間變動會拒絕發布。每份 receipt 綁定 Git head、完整
Python／shell／PowerShell／各 provider 公開前端資產／systemd templates／包裝檔案雜湊、runtime 套件版本與重複 metadata、wheel 與
`source.zip` SHA；建置需要的 README 也納入身分，wheel 包含 `train` 與
`plot_epoch_curves` 模組。共用 trainer 從同一程式根目錄呼叫 loss 繪圖入口；
2026-10-03 已把該入口加入來源清冊、wheel 精確比對及 checkout 外的 import 驗收。
部署 installer 與 service templates 從同一份 `source.zip` 的固定 root 執行；
wheel 提供 Python packages，不能只驗 wheel 便宣稱 systemd／網站資產已交付。
固定 `SOURCE_DATE_EPOCH` 與 private umask `0077` 記入建置 receipt，ZIP 時鐘與
權限不取決於重建時刻或登入 shell 的預設；重建須使用相同條件。
指定的設定連同完整繼承鏈凍結。設定依賴不能逃出 repo 的
`configs`。`source.zip` 不包含資料、模型、私人環境檔或既有 artifact。

在 repository 之外，以獨立 target 安裝 wheel 並匯入 `stockagent`、`downloader`
與 `scripts` 驗證；不要只在原 checkout 執行 import 便宣稱 wheel 可用。
遠端解壓前先核對 ZIP 與 receipt，逐個核對檔案、拒絕路徑逃逸；資料依然由
canonical release/materialization gate 決定可用性。wheel／source receipt
不是 source publication、CUDA image 或已啟動服務的版本證明。

可直接執行可重複的完整包裝驗收；每個 work directory 必須尚不存在：

```bash
run_fintech_python scripts/accept_project_release.py RELEASE/release.json \
  --work-dir artifacts/operations/CURRENT/packaging-acceptance
```

它先檢查 bundle SHA、唯一 regular-file inventory 與路徑，再解壓到獨立目錄，
使用 receipt 中相同 epoch／umask 離線重建相同 wheel SHA；以 `--no-index
--no-deps --target` 安裝、在 checkout 外匯入四個入口並執行 CLI help，最後
核對 frozen source 的 training startup provenance；繪圖模組也須從 wheel target
匯入。所有 log 與中間檔保留，
只有所有步驟通過才寫出 `packaging-acceptance.json`。
wheel 驗收不代表每份服務的私人設定、資產交付及實際啟動都已驗證。

固定程式樹可在沒有 Git 的節點再次驗證：

```bash
run_fintech_python scripts/verify_project_release.py RELEASE/release.json \
  --root FROZEN_CODE_ROOT --verify-bundles --output artifacts/code-acceptance.json
```

以 `STOCKAGENT_CODE_RELEASE_RECEIPT` 指定 receipt 後，訓練啟動會再次核對程式
與凍結設定，並把 receipt/source SHA 納入 manifest provenance；檔案不符便
拒絕啟動。此驗證不宣稱已完整追蹤每個 Python import 或既有服務載入的版本。

空的 legacy `.dist-info` 目錄與 stale metadata finder entry 會記入
`non_distribution_metadata_entries`，保留確切路徑／原因，不假設版本或刪除檔案。
非空但缺有效 package metadata 的目錄是 `metadata_errors`，strict preflight
及發布會拒絕；先確認其安裝來源後再修復。Runtime identity 包含重複 metadata
與排除記錄，變動仍會使既有 lock 驗證失敗。

此 identity 記錄 Python 當下可見的 distribution。CWD／PYTHONPATH 若包含
checkout 的 `stockagent.egg-info`，亦會記入本專案的版本；獨立 frozen source
目錄未必包含此 metadata。請在實際啟動目錄產生並驗證對應 lock，逐項檢查
差異，不刪除 metadata 或放寬 strict 驗證來強行對齊。此次兩個目錄的外部
依賴版本完全相同，唯一差異為本專案 metadata 的可見性，receipt 記錄於
`artifacts/operations/architecture-modernization-20261003/runtime-cwd-comparison.json`。

訓練 manifest 增加 namespaced `runtime_provenance` schema 1，既有 checkpoint
與 numerical resume 契約繼續由 shared lifecycle/checkpoint owner 管理。
不要為了報告欄位修改 optimizer、loss、data fingerprint 或歷史模型設定。

完整回歸也須固定程式樹。若原 checkout 有其他工作同時進行，以既有
`git worktree` 建立 detached checkout，保存 working diff、非 ignored 的
untracked files 與逐檔 SHA receipt，在該目錄執行完整 `pytest -q -s test`。
測試執行期間不修改該 worktree；結束後再次核對 inventory。不要把原 checkout
繼續增加的修改描述成已由舊快照受驗，或把正式資料／artifact 複製進測試樹。

## GPU admission 與資源

`scripts/manage_gpu_jobs.py` 仍是唯一 GPU 指派入口。每個 node 使用 `flock`
在整個 launcher lifetime 持有 GPU lease，涵蓋尚未使用 CUDA 的 CPU 準備階段，
也涵蓋同節點其他 checkout/spec。`allow_gpu_sharing` 仍只接受明確設定。
GPU 上的獨立程序會阻止啟動；停止工作須核對 boot ID、PID start ticks 與
process-group leader，PID 重用不能授權發送 signal。管理與訓練 receipts 分開。

雙 5090／fold11 的 CPU16 profile 位於
`configs/deployments/tw_day_trade_last_last_only_training_vastai1t_v8_twpublic_248d0869_full_cpu16.yaml`，
三輪完整輸出與 112-thread control 逐位元相同；warm 16／48／112 threads
吞吐約差 1%，沒有可確證的 6% 訓練加速。它保持 numerical config，只改
CPU 預算與獨立 output root。其他節點應重新量測，不能直接套用此結論。
`configs/gpu_jobs.yaml` 中的範例預設停用，正式啟動仍須選定 job 與 preflight。

既有 DDP owner 已將 validation 和 test 分別交給 rank0 與 rank1，適用範圍
由原契約決定；完整 epoch 的 max-rank timing 已包含兩者的重疊。
`epoch_parallel_eval_overlap_s` 是重疊量，不能再把 validation、test 時間
相加作為 epoch wall time，也不能將既有行為描述為本次新增的加速。

`--profile-timing` 的 epoch output 增加 process-local inner carry delta、
`day_trade_carry_telemetry_schema_version` 與 `day_trade_carry_telemetry_process_rank`。
outer backtest runner、inner session kernel 與準備快取各有自己的 counter；
不要從單一 compile flag 推論完整覆蓋率，也不要把 rank0 的 counters 報成全 DDP。
既有 transform cache 可用 `STOCKAGENT_TRAINING_TRANSFORM_CACHE_DIR` 明確指定
同節點共享路徑；內容、ABI、fold 和 payload fingerprint 任一不符就重算。
共享 cache、cold source publication 與 immutable code release 仍是不同證明。

比較已完成的 runtime profile 使用既有 lifecycle gate 與逐位元工具：

```bash
run_fintech_python scripts/compare_training_artifacts.py CONTROL_ROOT CANDIDATE_ROOT \
  --folds 11 --output artifacts/operations/CURRENT/training-parity.json \
  --allow-runtime-profile-differences
```

不加最後的參數時，整份已存 checkpoint／manifest 嚴格比較。明確開啟時只
允許 `configuration.environment.cpu_threads` 和 `configuration.runner.output_dir`，
先逐份驗證 canonical configuration fingerprint，再明列欄位與左右值；其他
設定、semantic fingerprints、模型／optimizer／RNG 與曲線仍須相同。
此報告模式不修改 checkpoint 或 canonical resume／deployment 契約。

Penguin 的背景資料父 slice 和子 slice 設定 CPUWeight/IOWeight=20；正式
盤中 loop、Discord 和唯讀 gateway 留在 system.slice。CPU 權重只在爭用時
降低背景優先序，閒置時可使用完整 CPU。既有記憶體上限維持。當 kernel 沒有
`io.weight` 介面時，IOWeight 只是設定意圖，不能報成已生效的 I/O 隔離。

```bash
bash scripts/install_registered_data_refresh_services.sh resource-slices-only
```

此 bounded deployment 只更新兩份 slice，不重裝其他服務或啟動大型下載。
`tmux` 適合工程接手；正式程序仍使用既有 supervisor 與可核對 receipts。

## 前端

共用時間軸以 JSDoc 和 TypeScript contracts 逐步受驗，現有 JavaScript 與
HTML 保持直接部署。`npm ci --ignore-scripts` 還原固定開發工具後執行：

```bash
npm run check:dashboard-types
npm run test:dashboard-contracts
source scripts/runtime_env.sh
run_fintech_python scripts/build_public_api_contracts.py --check
```

既有 feature-page envelope 同時由 Python 型別產生 JSON Schema 與 TypeScript
宣告；來源欄位仍維持動態型別。契約驗證可明確核對唯讀旗標、revision、rows
與 bounds，不在公網熱路徑新增依賴。時間軸與這個 API 已接入型別邊界，其他
DTO 沿既有 API 與 allowlist 逐項擴充。
