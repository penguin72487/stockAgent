# 台股盤後推論快取與常態資源驗收

10 月 14 日 05:41，五個已啟用當沖模式的最新正式收盤快取已重新驗收，
獨立程序可重用同一快取鍵，
不用初始化 CUDA。權重、決策列及模型解釋的完整 SHA256 與未使用前推快取的
對照相同。這一階段降低盤後重複推論與記憶體占用；全服務效能目標仍在執行。

核心驗收為 [最新來源五模式正式回讀與完整對照](../artifacts/operations/running-services-optimization-20261013/postclose-after-source-generation-formal-readback-20261014.json)。
原有 FinMind、OpenBB 與全服務清點見 [本機效能報告](running_service_optimization_2026-10-13.md)。

05:22 原資格 watcher 加入下一交易日資格表，使舊摘要的完整來源世代
失效；這不是 SDK 維護造成的 GPU 重算。五模式最新資料完整重播仍
命中原 forward key，完整輸出一致，禁止模型／CUDA／行情 API 的
[canonical cache-only 維護器](../artifacts/operations/agent-workflow/runs/canonical-postclose-source-refre-20261013T213941-d73be7a1/run.log)
只更新摘要與 weights pointer。05:41 正式讀回證明新來源已接受，
GPU／模型／provider 呼叫均為零，沒有交易寫入或正式歷史重跑。
05:56 已將兩個 committed 資格 parquet 加入原生 path，維持既有
cache-only owner；05:58 工程通知真實喚醒後五個摘要／weights／keys
不變，06:00 撤除探針後三來源及閒置 terminal handle 亦通過。
詳見 [原生事件與完整讀回](running_service_optimization_2026-10-13.md#原生事件連動只在-committed-source-變動時喚醒)。
沒有移除來源 fence；下一次真正資格發布的端到端時點仍待發生與核對。

01:44 的後續回讀拒用第一個模式的舊世代快取，不能再當作最新版五模式 ready。
01:59 的 [唯讀身分差異](../artifacts/operations/agent-workflow/runs/postclose-source-identity-diff-20261013T175922-9c48462f/run.log)
確認原 100M forward receipt 與目前程式只有 `stockagent/config.py` 內容／世代
不同，deployment YAML 未改；新增的是訓練 optimizer 設定。這是實際程式
變更而非只變 mtime，也不是本次查詢證實的行情缺口。沒有回填 fingerprint、
假造新世代收據或啟動 GPU；該次查詢本身沒有證明新版本 ready。

03:01 的 canonical `--signal-cache-only` 檢查五個模式皆沿用已存在的目前
快取，沒有前推日誌，不能把 attempted=5 當成新執行五次模型。
03:03 及 03:15 的正式鍵回讀與 15 次完整訊號測試才證明目前版本可重用：
完整權重、決策與模型解釋 SHA256 與未改寫的獨立控制一致；每次 GPU
allocated／reserved 均為零、CUDA 未初始化。五模式同時受測是正確性
驗收，不拿該批時間當公平 A/B 加速倍率。
03:27 的 [唯讀服務重載](../artifacts/operations/agent-workflow/runs/current-cache-readonly-consumer--20261013T192732-168c570d/run.log)
使 Discord 與網頁讀取服務載入目前程式，Discord 已重新連線；交易與
行情捕捉的原執行身分保持不變。
03:32 的 [重載後正式回讀](../artifacts/operations/running-services-optimization-20261013/postclose-after-consumer-adoption-20261014.json)
再次通過。歷史舊版本拒用與兩個正式歷史工作失敗紀錄仍保留。

## 收盤後的行為

官方完成收盤的收據被接受，且資料新鮮、不是盤中即時觀察時，才能使用這條
快取路徑。相同模型、已解析設定、程式版本、完整窗口、symbol／feature 順序
與注意力遮罩只計算一次，之後讀無損 CPU 前推快取。普通遮罩及全標的原始
分數遮罩是不同輸入，各自計算一次；不是不論修訂都強制每天只算一次。

來源修訂先重新核對價格、規則及展示列。完整模型輸入及遮罩未改變時仍可
重用前推；真正改變時才重算。只追蹤已發布股票分區、外部 feature 與收據、
當沖資格、公司行動參考／權利及 factorized manifest／成員。未被模型使用的
MOPS 等原始表更新、資料夾新增 heartbeat，不再讓已完成訊號失效。
同日替換、原地修訂、增加／移除股票成員仍被偵測；資格 fallback 的 RAM
快取也納入 ctime，不能因保留原 mtime 而沿用舊值。

當沖是獨立每日模型目標，不再為建立今天快取遞迴推論前幾天。已存在的
前期資料仍可讀取作展示；FIFO、持倉、成交及清算仍由原執行服務負責，
沒有回填或變更帳本。盤後訊號是收盤估值的研究／paper 預覽，不是收盤成交。

盤中取價、開盤前暖機及尚在交易的其他市場不使用這項盤後凍結。
台股日盤收盤不等於 TAIFEX 夜盤收盤，也不等於加密貨幣休市。
模型釋放受交易日曆、實際 close_time 與同一模型互斥鎖保護。

## RAM 與 GPU 的成本

完整前推輸出含所有輔助 tensor。實測三個舊模型分別約 131、233、882 MB，
原先 64 MiB 的快取上限因此拒絕三個合法模型。現在保留完整欄位和精度，
使用有限 1 GiB 上限，沒有透過量化、刪除輸出或換模型來縮小資料。

大快取先分段驗證整份 SHA256，再對同一個已用 O_NOFOLLOW 開啟的 regular
inode 作 weights_only、CPU、MAP_PRIVATE 記憶體映射。讀取不再複製整包 bytes
和所有 tensor；修改回傳值不會修改持久檔。單一 tensor 的 FP32／FP64／BF16、
負零、NaN、無限值及容器均有無損與所有權測試。
這沿用本機 PyTorch 2.13 已實測的功能，沒有升級套件。
[PyTorch torch.load](https://docs.pytorch.org/docs/2.14/generated/torch.load.html)

快取位於 `artifacts/cache/completed_live_inference`，可用
`STOCKAGENT_COMPLETED_INFERENCE_CACHE_ROOT` 指定私人位置。目錄 0700，
payload／receipt／lease 0600。每鍵使用 kernel flock 抑制重複計算；
payload 先原子提交且 fsync，再提交 checksum／identity 收據。
損壞、截斷、錯誤 receipt、symlink、提交中斷都不能回傳 ready，會重建。
這不是來源檔、不是訓練 checkpoint，也不公開到瀏覽器。

完成真正 GPU 前推後，於模型互斥鎖內清除 resident model／input／panel
引用、同步、清除 cuBLAS workspaces 並釋放 allocator cache。只做 empty_cache
不能清除仍有 owner 的配置；實測曾留下沒有 Python tensor 引用的 32 MiB
cuBLAS workspace，加入官方記載的清除介面後配置與保留都為 0。
[PyTorch CUDA workspaces](https://docs.pytorch.org/docs/2.14/notes/cuda.html#cublas-workspaces)

這個 0 指受測程序的 PyTorch allocator，不是 Windows 全卡占用。已初始化
程序仍可能保有 driver context；全新讀取程序則完全不初始化 CUDA。
背景釋放檢查改為每 30 秒，但沒有 resident model 就直接返回，沒有每次
呼叫 CUDA 或強制清空正常交易中模型。

## 盤前暖機與釋放窗口

07:03 五模式正常冷暖機曾測得 15.338 秒，原先卻從準備時間 08:15
提前 75 分鐘開始，並另以 07:00 保護顯存。07:19 已在合格空閒窗口
僅重載 Discord，將暖機及常駐保護共用同一個準備前窗口。本機預設
600 秒、即 08:05，保留原 60／120／240 秒重試餘裕；開盤、live 其他
市場、未知日曆、model mutex 與來源 gate 仍保護原工作。

範例設定位於 [Discord 環境範例](../services/discord_bot/.env.example)：
`STOCKAGENT_STARTUP_WARM_LEAD_SECONDS=600`，可依其他主機實測設定
60–4,500 秒；錯誤／非有限值採預設，不修改目前任何金鑰。
成功釋放 models 才清除 process-hot flag，避免同一 owner 隔日因舊
warm-ready flag 永遠跳過重新暖機；歷史完成收據另有 residency flag。
不以更換模型、精度、feature、價格或交易規則換取較低占用。

[正式新 owner 驗收](../artifacts/operations/running-services-optimization-20261013/discord-prewarm-native-idle-acceptance-20261014.json)
確認新 PID 1,674,213 重新連線、commands sync 或明確 defer、五模式
signal/revision 零落後；所有正式收盤 summary SHA、forward keys 與獨立
完整輸出不變。新程序沒有 GPU device handle、沒有提前暖機 ready
日誌。交易、行情捕捉、下載與唯讀展示 owners 皆未重啟。
189 項共享回歸通過；真正 09:00 opening SLO 與兩個 formal-history
失敗仍各自待驗，不能用快取成功覆蓋。

[08:10 原生自然暖機接受](../artifacts/operations/running-services-optimization-20261013/discord-natural-prewarm-terminal-20261014.json)
另確認同一正式 owner 在 08:06:21.418–08:08:10.515 成功完成五模式，
resident-ready、GPU handles 與 engine acknowledgement 零落後；
成功輪耗時 109.097 秒，早於 08:15。08:05 首次遇到來源發布寫鎖，
既有 60 秒重試恢復；沒有強制暖機、重載、provider 呼叫或交易寫入。
這裡的零 CUDA／provider 指唯讀 verifier，不是實際 GPU 暖機本身。
57 項自然暖機、日期／clock、resident 與原生讀回回歸通過。
三個 rebuilt 面板的取價前準備各約 29–37 秒，模型階段約 1–3 秒；
完整階段表見上方完整報告。15.338 秒是另一輪觀察，不能當作固定估計
或宣稱這次暖機變快；盤後 cache-only 與盤前真正模型常駐分開驗收。

控制 NVML 全卡顯存約 15,130 MiB，重載後另記錄實際 board 數字；
07:19 即時取樣約 3,174 MiB。換算約 14.8 → 3.1 GiB，不是每個
程序記憶體可加總、也不是全部顯存差值皆能逐 byte 歸因 Discord。
Windows 桌面、Sunshine 與其他程序沒有停止；詳見
[完整 GPU 取樣與工程界線](running_service_optimization_2026-10-13.md#discord-盤前-gpu-常駐窗口)。

## 完整操作實測

每個正式鍵有獨立全新程序冷讀一次、熱讀兩次，禁止模型建立、call_model、
CUDA availability／init／synchronize／empty_cache，沒有 provider 呼叫與
交易寫入。下表是最終五個獨立程序的完整訊號操作，含原有驗證、所有列與
financial formatting；沒有縮減成 top-N 或只量測 CUDA kernel。

| 模式 | 快取 payload MB | 熱讀中位數秒 | 程序峰值 RSS MB | CUDA 初始化 |
| --- | ---: | ---: | ---: | --- |
| 100M | 13.58 | 0.418 | 1014.65 | 無 |
| Multi Basis | 130.99 | 0.594 | 1013.55 | 無 |
| Multi Basis 22 | 233.01 | 0.712 | 910.41 | 無 |
| V8 Annual Log Cash | 882.48 | 1.066 | 1019.99 | 無 |
| Factorized Scale Separated Cash | 25.15 | 0.521 | 894.57 | 無 |

MB 採十進位。OS page cache 未清空，其他服務仍在執行；每版取樣少，
不是固定硬體吞吐上限。最大的 V8 模型在先前整包讀取版本，獨立程序熱讀
4.453 秒、峰值 RSS 3651.97 MB；第一次 mmap 驗收為 1.350 秒、1019.49 MB，
完整輸出相同。後續來源範圍修正後的最終值列於上表；兩輪差異不能只歸因
於 mmap。熱讀 CPU 訊號可能比保留 GPU 的熱模型慢，主要收益是盤後不再
保留／啟動 GPU，不是宣稱所有請求延遲都下降。

收據：

- [最終五模式正式鍵與零 CUDA 完整對照](../artifacts/operations/running-services-optimization-20261013/postclose-acceptance.json)
- [最大模型整包讀取](../artifacts/operations/running-services-optimization-20261013/postclose-fresh-tw_day_trade_v8_annual_log_cash.json)
- [最大模型首次 mmap 對照](../artifacts/operations/running-services-optimization-20261013/postclose-mmap-fresh-tw_day_trade_v8_annual_log_cash.json)
- [五模式前推後實際配置歸零範例](../artifacts/operations/running-services-optimization-20261013/postclose-final-tw_day_trade_factorized_scale_separated_cash.json)
- [盤後主機基線](../artifacts/operations/running-services-optimization-20261013/postclose-baseline.json)
- [23 時 33 分主機取樣](../artifacts/operations/running-services-optimization-20261013/postclose-final-census.json)
- [23 時 55 分再驗收](../artifacts/operations/running-services-optimization-20261013/postclose-stable-acceptance.json)
- [最終 owner 主機取樣](../artifacts/operations/running-services-optimization-20261013/postclose-owner-census.json)

23 時 33 分取樣涵蓋 86 個已安裝服務、1 個工程 transient、67 個 timers、
3 個 paths。Discord child RSS 從基線 3397.82 MB 到重載後 868.36 MB，
cgroup anonymous 從 2716.58 MB 到 532.62 MB。這是盤後釋放與冷 owner
讀回，不是同一程序的嚴格 A/B。CPU 5 秒取樣由 0.053 到 0.077 core，
不能宣稱 idle CPU 下降；主要已證明的節省是 RAM 與重複 GPU 推論。
最後一次重載後的 23 時 55 分取樣為 child RSS 854.45 MB、anonymous
519.24 MB、0.052 core；五模式正式鍵跨過來源持續更新後仍可零 CUDA 重用。
10 月 14 日 [00:22 全服務取樣](../artifacts/operations/running-services-optimization-20261013/postclose-current-census-20261014.json)
同一 Discord child 的 RSS 為 848.31 MB、anonymous 532.66 MB、0.061 core；
降低 RAM 的結果持續成立，仍不能宣稱 idle CPU 有顯著下降。
僅重載 Discord consumer，當沖執行及 TAIFEX 夜盤 owner 身分與三份帳本
SHA256 維持不變。當下整卡 NVML 為 1% 利用率、3977 MiB 已用記憶體，
屬全機瞬時觀察，不能把全部占用歸於本次受測模型，也不保證遊戲幀率。
23 時 58 分的 [Windows 唯讀 GPU counter 清冊](../artifacts/operations/agent-workflow/runs/windows-gpu-owner-readonly-20261013T155810-2211d6bb/run.log)
另外觀察到 Sunshine、DWM、MuMu、Code、Chrome 等程序；counter instance
不能直接相加當成單卡實體記憶體，也不證明個別程序正做多少 GPU 計算。
沒有關閉、調整或重啟這些 Windows 程序。
跨午夜的 [00:17 正式再回讀](../artifacts/operations/running-services-optimization-20261013/postclose-after-reader-acceptance.json)
仍是五模式、零模型前推、CUDA 未初始化；交易引擎／夜盤 owner 及帳本 SHA 不變。

## 驗證與重現

512 項 shared 回歸通過；另有 12 項 verifier 測試，涵蓋錯誤 hash、錯誤正式 key、
CUDA 初始化及非零 GPU 占用拒絕測試。先前 15 項實際 CPU／CUDA
factorized transport 測試通過；這些計數有重疊，不相加當成不同測試。
修改範圍的 Ruff、git diff --check 通過，沒有宣稱跑完全 repo test suite。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/verify_completed_session_cache.py \
  --benchmarks artifacts/operations/running-services-optimization-20261013/postclose_current_code_20261014 \
  --controls artifacts/operations/running-services-optimization-20261013 \
  --output artifacts/operations/running-services-optimization-20261013/recheck.json
```

這是當下已接受收盤與相同版本的唯讀驗收；來源／部署變更、交易中或
artifact 不完整時失敗是正常保護，不代表可以跳過收據重建。benchmark
需指定 `--cache-root`，新版本或新輸入的第一次會需要 GPU，先做 strict
CUDA preflight。不要在開盤、使用者交易請求或其他 GPU 工作中並行跑測試。

## 全服務目標的剩餘工作

registered intraday、FinMind complement／quota、OpenBB 全庫 audit、資料
狀態投影及正式歷史推論仍需各自完整 profile 和結果一致性驗收。
23 時 33 分最高 CPU 的 registered intraday 是正在執行的抓取／處理，
不能只看 3.51 cores 就當成空轉。OpenBB 的舊 audit 不會因檔案修改 hot reload。
10 月 14 日 00:22:10，該舊版全庫 audit 自行結束，archive owner 回報 exit 2；
611 個 Parquet 未通過成功工作歸屬檢查，來源內容不能因此直接判定錯誤。
詳見 [全量 audit 的當前界線](running_service_optimization_2026-10-13.md#10-月-14-日-全量-audit-結果)，
不會把程序停止的資源下降歸為本次加速，也不會把該服務標成完成。
前一期 formal history 的兩個失敗仍顯示 degraded；最新五模式快取 ready
不會掩蓋它，也不代表下次真實開盤、全歷史及交易成交已驗收。

模型／checkpoint／PIT、來源資料、配額、執行引擎及 TAIFEX 夜盤擷取不改變。
針對 GPU 的下一階段先量測仍在運作的其他 owner，不能把台股收盤狀態套用
在夜盤、全天市場或必要訓練；任何 compiler／新精度／套件升級須另做同輸入
語意驗收。原始來源與舊工程收據保留，沒有藉清空資料或停止服務取得改善。
