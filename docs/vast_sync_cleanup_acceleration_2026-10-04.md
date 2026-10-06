# Vast 同步與空間回收加速驗收（2026-10-04）

已完成四個實際來源回收，共 4,606,332,928 allocated bytes；兩個歷史 cohort
沿原清冊、原 owner 與進度繼續自動回傳。這不是整台 Vast 或全部 NAS 歷史的完成聲明。

## 共用鎖與實際瓶頸

`/run/lock/stockagent-remote-cold-artifact-ingress.lock` 排他保護一個回傳交易：
來源重查、暫存、D 原子發布、獨立還原、同步確認及來源回收。兩個歷史 worker
本來就按根目錄釋放鎖；新 completed-return timer 原先使用 nonblocking flock，
持鎖時跳過，容易持續搶不到。現在等待安全交易邊界，預設最多 1800 秒；
逾時退出 75、保留來源，下一輪再接手。

實測還找到兩個昂貴的重複工作：penguin 每 300 秒掃描約 688.9 GB 的 packed
命名空間，而唯一消費端 Vast 已是 index-only；已有待確認 run 時，timer
還會先遍歷其他完成 run，並在尚未同步收斂時讀取全部既有 cold payload。
小批次曾因 manifest scan 超過 120 秒而保留來源，不能把程式退出 0 當作回收。

## 已部署的修正

- 繼續共用既有 ingress owner；新增每個 cohort 的 owner，防止同一進度有兩個 writer。
  舊 worker 在 blocked flock、無持鎖、無子程序的安全點交接，未停止收集或訓練程序。
- 待確認波次只重查其精確來源。v2 的內容讀取集中在同步確認後的完整獨立重建
  ACK；一般 ingress 預設全內容驗證不變。同尺寸 cold corruption 的測試證明
  損壞會阻止 ACK 及遠端回收，修復原值後才可生成 ACK。
- 歷史封存先持久記錄 scan intent，再從 D 完整還原，最後分批掃描精確物件、
  manifest、head。每組最多 64 路徑／16 KiB；超過 256 個已知路徑仍保持精確
  批次，損壞通知才回退完整物件掃描。既有 retry service 已啟用同一批次策略。
- penguin 完整保留 D 上的 blobs/packs；對唯一、現場確認為 index-only 且無 payload
  exception 的 Vast 消費端，只同步索引。完整 NAS payload 使用原備份 ingress，
  訓練仍按固定 release 從 cold fetch；新增按需發布 timer 自動追蹤 Vast 的
  既有 hydration state／精確 ignore 例外，從 penguin manifest 解析物件後才開放。
  取完自動撤回例外，沒有刪除權威物件。索引範圍約 978 MB；這是傳輸布局調整。
- 全量兜底 rescan 改為每小時，持久通知與 retry 保留。Source `.stignore` 的舊值
  保存於私有收據；新增 packed peer 時按需發布服務自動恢復完整 payload 傳輸，
  仍須獨立驗收新 peer。
- 依獨占可回收 allocated bytes 優先處理；子分區缺少 allocated 清冊時以邏輯大小
  暫排，真正回收仍採 fresh gate 的配置 bytes。共享 inode 的 legacy v1 仍保護。
  新 batch wrapper 使用 runtime resolver、Nice 19 與 idle I/O，與正式服務隔離。
- 遠端與本機 rsync 均實測支援 zstd；Vast 角色採用已量測的 level 1。
  只改傳輸編碼，原值 SHA、完整還原與刪除 gate 不變；一般遠端預設仍為 none。
  兩個 worker 已在無持鎖／無子程序邊界完成傳輸設定交接，沒有中止在途交易。
  後續冷編碼政策的主 worker 亦已安全交接；分區等待其目前交易結束。
- 新增 `stockagent-legacy-return@main.timer` 與 `@partitions.timer`，每五分鐘
  嘗試接手同一保留清冊。只讀 `ExecCondition` 先核對 cohort owner；忙碌時跳過，
  canonical worker 再取得原 cohort／ingress lock，避免檢查後的競態造成雙 writer。
  這是歷史批次的恢復排程；原 completed-return 與 NAS timer 繼續持有原本責任。
  失敗的獨立恢復 scratch 保留，不使用會在服務退出後移除它的 PrivateTmp。

## 實際回收與完整驗證

| 精確來源 | 檔案數 | 原值邏輯 bytes | 已回收 allocated bytes |
| --- | ---: | ---: | ---: |
| `markets/tw_day_trade_daily_no_default_v1` | 495 | 2,886,694,477 | 1,745,305,600 |
| `markets/tw_index_derivatives_day_multi_basis_100m_full_chain_v3_dual5090` | 485 | 2,471,019,077 | 1,630,461,952 |
| `markets/tw_index_derivatives_day_multi_basis_100m_relative_tenor_cash_entmax_v7_dual5090` | 493 | 1,816,673,839 | 1,210,613,760 |
| `markets/tw_stock_futures_0845_carry_v15_vast5090` | 24 | 19,897,960 | 19,951,616 |

三個 v2 的 D 全檔獨立重建、來源 SHA／引用／配置／pin／同步 gate、私人
quarantine 與來源刪除已完成。302／271／267 個共享名稱只移除當前 run 的名稱；
外部別名及權威冷資料保留。三個來源 ACK 與 SSH 讀取的 Vast `retired` journal
結果逐欄一致，原目錄確實不存在。

同一 24 檔歷史批次沿用已保留的 transfer／encoded scratch 和相同 cold release：
先前 scan 階段 120.290 秒超時；修正後 0.860 秒完成。含兩次 D 全內容恢復、
逐檔原值驗證、同步確認、遠端引用與配置檢查、刪除及暫存驗證回收的完整流程
33.391 秒。先前失敗整輪 210.452 秒包含 70.157 秒等鎖，不能據此宣稱完整成功
流程提升幾倍。該結果也不代表新 23 GB 批次的耗時。

同一待確認 v2 波次的未收斂 cycle 從 133.933 秒降至 1.528 秒：省去當下無需的
全內容讀取；真正回收前仍執行全內容重建。兩次觀察負載與 OS 快取不同，不宣稱
統計顯著。新增冷編碼與定時接手後，最後的同步／回收／edge 共用契約回歸
255 項通過；另有 78 項按需發布與 scan、57 項壓縮傳輸與來源 gate 的聚焦驗證。

## 真實按需傳輸與壓縮比較

原有 cold fetch 依賴 Syncthing payload 例外，因此只排除來源 payload 不足以
支援新訓練。現在來源 `stockagent-packed-transport.service`／15 秒 timer 只
管理傳輸 ignore，沒有改遠端 Git 或既有 edge owner。遠端狀態與 ignore 尚未
一致時重試，斷線時保留既有例外；任意遠端物件路徑、缺少固定 manifest 及
未知 source filter 都被拒絕。POST／scan 前記錄持久 intent，逾時後即使 ignore
已更新也會補掃，不能以無變更當成通知成功。

實際 Vast 原先缺少同一固定歷史 release 的全部 19 個 payload；用原
`manage_packed_edge.py use` 自動取得並驗 SHA，14,357,585 payload bytes 通過，
25 個 materialized 成員／19,910,106 bytes 完整還原。受監督命令牆鐘約 94.39 秒，
包含按需偵測、掃描、傳輸、驗證及 payload 回收；不是純下載速度。
隨後只回收獨立測試目錄的到期 cache，確認 target 消失、Vast payload 剩零，
再從 penguin 逐物件核 SHA 確認權威原值保留。原生產 materialized root 未被選入。
真實測試也遭遇一次 POST／scan timeout，持久 intent 的下一輪接手後成功，
服務回到零 request，packed 回到完整收斂。

23 GB 批次中約 20.26 GB 是 NPZ；抽查三份 ZIP header 是 stored／未壓縮，
不能只看副檔名就認為再壓縮沒有收益。選同一組真實 NPZ、Parquet、PT，
共 106,725,752 bytes，依 none1／zstd1／none2／zstd2／none3／zstd3 交錯實測。
每次重新傳至私有空目錄，精確集合與每檔原值 SHA 均驗證，來源前後 SHA 一致。

| 傳輸＋全部 SHA 驗證秒數 | 第一次 | 第二次 | 第三次 | 平均 | rsync received bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| none | 5.977 | 3.918 | 5.413 | 5.103 | 106,752,343 |
| zstd level 1 | 0.926 | 1.093 | 1.059 | 1.026 | 11,372,393 |

此樣本完整傳輸與 SHA 驗證平均約 4.97 倍，接收量少約 89.35%。這不是完整
歷史 archive／NAS 工作鏈的提升倍數；沒有清 OS 快取，當時其他正式 I/O 持續運作。
兩端 rsync 版本為 3.4.1／3.2.7。已驗證的私有測試副本收掉，來源未改。

同組真實樣本另經兩輪正序／反序的完整冷發布、D 解碼驗證、還原及原值
SHA／mode／mtime 比較，自適應 gzip level 1 的中位數為 12.525 秒，原流程
為 50.808 秒；冷物件從 105,922,647 減至 13,468,066 bytes。新政策採實際整檔
壓縮收益決定 raw／gzip，既有已校驗編碼原樣重用。集中目錄 fsync 沒有一致
收益，政策保持關閉。測量及適用邊界見
[冷編碼完整流程實測](vast_all_artifacts_cold_return_2026-10-04.md#1504-增量冷編碼加速)。

## 持續運作與剩餘範圍

已恢復兩個受監督的歷史 cohort，原 `/var/lib/stockagent-vast-legacy-return` 與
`/var/lib/stockagent-vast-legacy-return-partitions` 不重建清冊、不覆寫原 run 日誌。
主批次 `ablations/transformer_base_portfolio_2006plus_dynamic_s` 的
23,108,638,681 邏輯 bytes／11,870 檔已傳完，傳輸階段 1173.134 秒，
第一輪完整嘗試 3309.762 秒後發布失敗，來源保留，沒有可驗收 head，未計入回收。
15:10 的只讀診斷耗時 170.366 秒，逐檔確認保留的編碼與原始檔、完整集合、原值
SHA 全部吻合；這僅證明暫存有效，不代表 D 提交成功。第一輪錯誤在舊的重試
狀態更新時遺失，不能據此斷定根因；現在每次嘗試獨立保留 `attempts/` 收據，
重試不再抹除上一輪錯誤。新主 worker 沿同一暫存重試，目前等待分區釋放 owner。
子分區的 8,830,353,979 邏輯 bytes／110,068 檔，壓縮傳輸階段為 227.005 秒；
目前正在 cold 發布。這是實際在途階段，不是成功回收耗時。
使用中、變更中、無確切恢復證明或 legacy 共享 inode 的項目保留。

14:48 的 SSH 只讀觀察：Vast 可用 26,863,497,216 bytes。可用量包含其他程序的
磁碟活動，不能全部歸因於上述四次回收。NAS 來源狀態於 14:08:36 更新：
已驗收 69,254,001,199／687,094,881,259 可備份 bytes，尚有 25,765,791,176
pending bytes／3 個 delivery。與 lab203 的同步持續運作，完整歷史還在補齊。
14:48 的 source→lab203 completion 與 Vast packed 都為 100%，兩個 peer 連線；
已發布 ingress 在 lab203 的 need bytes／items／deletes 為零。新批次仍會持續
發布，傳輸收斂不等於完整 NAS 歷史驗收。來源完整 NAS cycle 的狀態更新較慢，
不能把其 14:08 的數字當成 15:17 又完成了一次驗收。
目前觀察的 WAN 傳輸約 10.09 MiB/s；總耗時也受重建、NAS check／還原與新資料
增量影響，不能保證立即完成。實際磁碟格式遷移仍屬後續階段。

15:19 又觀察到 backup ingress 一筆 DrvFs/9p `readdirent: cannot allocate memory`。
同一精確目錄當時已可直接讀取；Syncthing MemoryCurrent 約 421 MB、沒有 cgroup
記憶體上限，Linux available 約 86.97 GB，不能把該錯誤直接歸因於其 RAM 配額。
精確 `sub` 重掃的 HTTP 回應在 30 秒逾時，先記錄為 pending，未宣稱成功。
15:24 再讀 REST 驗證，ingress 已 idle、need bytes／items／deletes 和 errors
皆為零；packed 亦 idle、errors 為零。原 pending 收據保留，另保存恢復成功收據。
這證明本次掃描錯誤已恢復，不代表 Windows／9p 的所有暫時故障根因已解決。
15:24 的固定驗收收據確認六個正式 timer enabled／active，lab203 與 Vast
completion 均為 100%、need bytes／items／deletes 為零，三個資料／收據 folder
皆 idle、errors 為零。Vast 可用 24,135,385,088 bytes；其他工作仍在寫入，
磁碟淨變化不能當作本輪回收量。上述兩個大批次來源仍存在，未宣稱刪除。

所有來源清冊／原值收據保留；NAS prune、lab203 新批次自動刪除沒有啟用。
USB 金鑰保管已依使用者確認完成。Vast 的已 pin／有效 lease materialized cache
不因沒有 GPU utilization 就刪除；canonical GC dry run 仍為零可回收候選。

## 22:24 進度與獨立還原暫存修正

22:24:47 的固定觀察收據確認 lab203 與 Vast 均連線，已發布資料的 peer
completion 為 100%，need bytes／items／deletes 為零；packed、backup ingress
與 receipt return 三個 folder 都是 idle、errors 為零。六個正式 timer 隨後
再核對均為 enabled／active。這是當下已發布批次的傳輸收斂，不代表全歷史完成。
NAS 最近一次完整來源 cycle 仍為 21:06:23：已驗收
206,650,510,984／718,450,409,309 bytes（約 28.76%），比前次已報告驗收量
增加 137,396,509,785 bytes。分母包含後續新增冷資料，不是固定的全專案總量；
`all_history_backup_verified` 仍為 false。

110,068 檔／8,830,353,979 邏輯 bytes 的
`component_diagnostic_20261003` 已取得刪除前後、同一固定 snapshot／manifest
的完整冷恢復證明，來源真正回收 9,059,045,376 allocated bytes。
22:24 的 SSH 再核對來源目錄已不存在。另有 334 個 cache alias 的精確去重
獨立核 SHA 收據，釋放 22,541,832,192 allocated bytes；214 個唯一 inode
全部完整核 SHA，邏輯路徑與唯一 payload 都沒有刪除。上述回收量分開列示，
不與舊 cohort 累積數字重複加總。Vast 最新 `df -B1` 可用
106,125,578,240 bytes；這是整台機器的當下餘額，其他程序正在寫入／回收，
不能全部歸因於上述兩份收據。

23,108,638,681 bytes 主批次的後續失敗已保留明確原因：
`verification scratch lacks bounded space plus 32 GiB reserve`。原預設 `/tmp`
是總量 50,623,983,616 bytes 的 tmpfs，甚至空磁碟也小於本批需要的
57,474,426,397 bytes。現改由原 cohort 的 `verification-scratch` 實體磁碟
承接，沿用原生檔案系統、WSL 實體後端可用量、32 GiB 保留空間及完整 decode／SHA
驗證；拒絕與權威冷庫重疊、symlink 重導向。失敗的私有 scratch 保留，成功後
僅清理本次新建的 scratch。真正批次的容量 admission 已通過，當時實體可用
約 280.37 GB；沒有降低 reserve 或省略還原步驟。

五個相關測試模組共 105 passed／178.18 秒。另以既有固定 24 檔／19,897,960
bytes 冷資料實際在新位置獨立還原，9.71 秒通過，完整原值核對且 scratch
已移除。這個 pilot 不代表 23 GB 批次已恢復或可刪。主 worker 已在未持有
共用鎖、沒有 child 的安全邊界接回同一 cohort；新 worker 目前仍等待分區
冷庫 owner。分區正在完整還原，另有受監督的接手 watcher 等待下一個安全邊界，
未中止其交易或正式訓練。共用鎖仍保護冷發布／恢復／回收的整段一致性。

另外的原生 Windows 串流回傳截至 22:24：ablations 的 26,301,075,282 壓縮
bytes 已接收、完整原值核對後排入既有 ingress owner；cache 的
21,223,985,638 壓縮 bytes 已接收，正在解碼。markets 保留的
71,140,449,081 壓縮 bytes 則是 `transport_incomplete_source_preserved`：
tar 退出 1、zstd 退出 0，日誌有來源在讀取前被移除及讀取期間變動，仍需與
各根目錄的冷恢復／回收收據對帳，不能列為成功回傳或冷庫驗收。這三批的
bulk cold proof 及 bulk source retirement 尚未成立；既有個別 cohort 的成功
不能替代它們。詳見 [原生壓縮回傳](vast_bulk_compressed_return_2026-10-04.md)。

本次固定同步觀察：`progress-20261004T142447Z.json`；新增實測與修正證據：
`native-verification-workspace.xml`、`native-verification-real-pilot.json`、
`native-verification-main-handoff.json`。主 worker run 為
`native-verification-root-return-20261004T142004-bb5dcf4f`，安全接手 watcher 為
`native-verification-partition-ha-20261004T142144-10021e52`。全歷史 NAS、剩餘
Vast 回收及實際磁碟格式遷移仍未宣稱完成。USB 保管已完成，Windows 登出
驗收仍依使用者要求取消。

## 證據與操作

私有測量收據位於 `artifacts/operations/four-node-storage-20261004/`：
`legacy-worker-safe-handoff.json`、`packed-rescan-policy-3600.json`、
`penguin-vast-index-transport.json`、三份 `returned-source-retirement-*.json`、
三份 `vast-retirement-journal-*.json`、`legacy-pilot-accepted-retirement.json`、
`current-historical-transfer-progress.json`、`storage-acceleration-final.xml`、
`storage-sync-cleanup-final.xml`、`packed-transport-installation.json`、
`exact-edge-live-hydration.json`、`exact-edge-probe-cache-gc.json`、
`rsync-transport-comparison.json`、`rsync-profile-installation.json`、
`compressed-cohort-handoff-progress.json`、`acceptance-20261004T062354118733Z.json`。
15:10 後新增 `retained-large-encoding-diagnostic.json`、
`legacy-retry-timer-installation.json`；12 個接手測試覆蓋忙碌 owner、owner 釋放、
失敗重試、已回收／受保護狀態、缺少清冊與重導向收據。現場兩個新 service
均為 `Result=exec-condition`、condition exit 1、沒有 ExecStart，證明沒有重複啟動。
最新共用回歸收據為 `storage-sync-retry-final.xml`（255 passed／114.42 秒）。
掃描重試與恢復分别為 `ingress-exact-scan-recovery.json`、
`ingress-exact-scan-recovered.json`，不可用前者的 request timeout 捏造成功。
最新固定觀察為 `acceptance-20261004T072414919470Z.json`；
`final-acceptance.json` 只作最新指標，不替代 task 綁定的固定收據。
各 cohort 的 `summary.json`、`item-status/`、`recovery/` 與
`post-retirement-recovery/` 保留即時階段、耗時、完整恢復及真正回收量。

```bash
# 查當前階段；state=remote-source-retired 且 post recovery 通過才計入回收。
jq . /var/lib/stockagent-vast-legacy-return/summary.json
jq . /var/lib/stockagent-vast-legacy-return-partitions/summary.json

# 故障後接手同一 cohort；owner 尚在時拒絕第二個進度 writer。
stockagent-agent run --backend tmux --name recovered-root-return \
  vast-all-artifacts-cold-return-20261004 -- \
  bash scripts/run_remote_legacy_archive_return.sh archive --apply --order reclaim-first

# 正式自動重試排程，沿用原 cohort 進度；不需另外手動啟動 writer。
systemctl list-timers 'stockagent-legacy-return@*.timer'
systemctl status stockagent-legacy-return@main.service stockagent-legacy-return@partitions.service

# 在已有完整清冊／進度的 penguin 安裝，使用新的私人收據路徑。
source scripts/runtime_env.sh
run_fintech_python scripts/install_legacy_return_timers.py --evidence /ABSOLUTE/FRESH/INSTALLATION.json
```

掃描 `sub` 的範圍與成功回覆依
[Syncthing REST 文件](https://docs.syncthing.net/rest/db-scan-post.html)；
本機實際 v2.1.5 對重複 `sub` 的處理另核對
[官方實作](https://github.com/syncthing/syncthing/blob/v2.1.5/lib/api/api.go)。
同步通知、完整資料恢復與來源回收是分別取得的證據。
