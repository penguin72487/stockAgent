# 檔案系統、增量版本與空間檢查

## 2026-10-25 01:24 增量版本與 public-builder 副本最終驗收

本輪先規劃、量測，再沿既有 writer／publisher／回收 owner 實作。保留 ext4／NTFS；
增量發生在資料版本層：每版保存完整邏輯清單，引用共用的 immutable files／
8 MiB SHA-256 區塊，只有新增或更正的內容需要另存。任一固定版本可獨立還原，
不須從第一版重播差異鏈。01:24 的 metadata-only 觀測再次核對 **33／33 個
`data_*` 根**，沒有遺漏版本入口；這不代表所有來源 producer 都已改成原生分區。

195 檔 public-feature-assembly 工程副本已完成原 owner 的 source／cold 全驗、
新鮮 consumer／FD／mmap／pin／lease／transport 檢查、隔離後核對及精確 unlink。
原交易與 finalizer 均為 `succeeded`／exit 0。另一程序在刪除原副本及編碼暫存後，
從固定 D 版本完整重建並核對 **195／195 原檔的 SHA、大小、集合與 portable
metadata，以及七份原始 trial 證據**；成功工作區已移除，原始小型證據保留
322,240 bytes。不是只驗 head、抽樣或沿用刪除前的 proof。

| 項目 | 實際 allocated bytes | 認列方式 |
| --- | ---: | --- |
| 原有 WSL 工程副本 | **9,353,723,904（9.35 GB／8.71 GiB）** | 最後 inode 連結釋放的 `st_blocks`，為本輪原有空間回收 |
| 本作業新建 D 編碼暫存 | 8,408,584,192（8.41 GB） | 已精確移除，與原有空間回收分開 |
| 八輪成功 ABBA 的新建暫存 | 675,835,904（0.68 GB） | 已移除，與原有空間回收分開 |

原清冊的 10,261,362,655 logical bytes 或 10,262,253,568 配置 bytes 不能當成
本輪實際釋放量；其他連結仍持有的 inode 不重計。固定封存的 15 個冷物件共
1,841,356,395 stored bytes，原 snapshot／manifest 保留。金融來源原件、D
冷物件及 NAS 歷史刪除數均為零。01:24 的可用空間為 **WSL 63.21 GB、D
469.36 GB、C 336.71 GB**；其他 writer 同時運作，不能以磁碟可用量差值推算回收量。
這是檔案系統內的釋放，沒有離線壓縮 Windows VHDX。

本次原交易耗時 5,959.40 秒，包含 Syncthing 約 39 分鐘的 metadata 掃描等待。
等待期間第一份 fresh proof 到期，原流程自動重新全驗，再在收斂後執行 unlink；
沒有修改掃描設定、縮短必要驗證或將傳輸等待誤認為資料錯誤。
新版完整工作流與 **1,483 個不重複語意案例**的驗收見下方；大檔原生 hash/decode
子流程的 6.99 倍只適用該次配對量測。footer 完整工作流仍保留中性／負面結果，
不宣稱整個回收或全系統達到硬體極限。

所有來源的原生逐期分區仍是獨立工作；正式 FinLab writer 保留已驗證的格式，
沒有因新分片 ABI 存在就強制採用較慢方案。未對所有常駐服務換版，也未宣稱
來源全歷史或 NAS 全歷史驗收完成。下方較早的「進行中」紀錄以本節最終驗收為準。

證據：[最終驗收與容量](../artifacts/operations/filesystem-partition-footer-20261024/readback.json)、
[刪後全原檔獨立冷讀回](../artifacts/operations/filesystem-partition-footer-20261024/public-post-unlink-readback.json)、
[33 根當前版本](../artifacts/operations/filesystem-partition-footer-20261024/layout-heads-current.json)、
[原 owner 完成收據](../artifacts/operations/agent-workflow/runs/filesystem-materialized-checkpoi-20261024T153734-7889dd3b/run.json)、
[獨立 finalizer 完成收據](../artifacts/operations/agent-workflow/runs/filesystem-materialized-retireme-20261024T153806-b0a1bc8d/run.json)、
[掃描等待的唯讀觀測](../artifacts/operations/filesystem-materialized-reader-20261024/scan-wait-observation.json)。

## 2026-10-24 共用讀者與 public-builder 副本回收

沿用下方 16:40／16:50 的先行方案。本輪沒有重格式化 ext4／NTFS，資料版本仍是
完整邏輯清單，實體共用 immutable files／8 MiB SHA-256 區塊；新增版本只新增
獨有內容及清單，原版本能獨立恢復，不依賴無限串接的差異檔。

共用 `ParquetSource` 現在於同一個完整驗證的快照重用已解析的 footer，明確關閉
自己開啟的 reader。全部 member SHA／schema／列數、pointer／generation 前後
fence、投影、tail 及 predicate 語意保留；沒有跨程序快取或放寬完整性。
以 receipt 綁定的實際 FinLab 收盤價表量測：4,794 列、2,790 個股票欄位，完整
ABBA 包含兩版本寫入、發布、全原檔冷還原、Arrow／Polars／DuckDB 讀回及追加後
舊 generation 讀回。wall 中位數 27.02 → 26.62 秒，CPU 22.49 → 22.59 秒；
**整體無顯著加速**，不將少一次 footer 解析宣稱為系統突破。正式 FinLab 原生
分片仍保留先前較慢的負面結果，不啟用該格式。

實際瓶頸另在 D completed scratch 的 9p byte I/O。原生 Windows FileStream
唯讀通道已沿原 adapter 接上兩個固定 D scratch；64 MiB～8 GiB 才採用，較小、
更大與未登錄路徑保持原串流。Linux no-follow FD、真正 D 掛載、native exit／
完整 EOF、encoded／decoded 全 SHA 及退出後 FD／path generation 檢查均保留。
同一個 1.09 GB 實際編碼檔，完整 hash/decode ABBA 的 wall 中位數
**97.93 → 14.02 秒（6.99 倍）**，四輪全部符合原 1.34 GB 輸出的 SHA／大小。
這是單一成員的完整驗證子流程，不是整個退休流程或全系統的加速倍率。
自己的舊只讀驗證已在核對 PID／start ticks／boot 後正常中斷，保留 cancelled
收據；原檔、完整編碼、固定 inventory 均保留，沿原 owner 啟動新版。

共同讀者／版本層回歸含後續新增 23 項共 905 項、完成證據／封存／回收回歸
544 項、完整未提交編碼恢復新增三項、原生 reader 新增 31 項，保守核對
**至少 1,483 個不重複
案例通過**。完整原生 adapter／codec 156 項另行通過；既有案例不另納入這個
新案例小計。前者有九個 Temporal 活動測試在一般交易環境缺少
`temporalio`；原失敗收據保留，九個案例已在 canonical lakehouse role 全部通過，
沒有把角色套件灌進交易環境。初跑 84 項與另跑 public-builder 15 項是子集，
不重複加總。恢復路徑另跑八項通過，其中五項重複於 544 項，亦不重計。

重新指定 D authority 的 metadata-only 清點核對 33／33 根：20 個 shared v2、
12 個 host-private v2、1 個 local-news 完成收據；來源 payload 讀寫均為零。
第一份未指定冷庫的清冊只能證明登錄，額外 head 斷言因而失敗；保留該收據，
已沿原 CLI 補上 `--sync-root` 重新觀測。被量測的程式檔案沒有漂移。
入口存在不代表所有來源歷史抓齊，也不替代 NAS 全歷史驗收。後續通用 reader
修改另有下方新版完整 ABBA；初次測量不回填新版程式身分。

空間清理只選本輪新審的
`benchmarks/running_services_20261013/public-feature-assembly-20261015c`：
195 檔、10,261,362,655 logical bytes、清冊配置 10,262,253,568 bytes。
172 個 frozen source 與七份原始完成 trial 輸出均重新核對；不同 harness 的舊
試驗只驗完整來源／輸出，不混入後六份同 harness 的性能比較。新增的有界完成
辨識綁定精確 fixture／proof ABI、原始收據 raw bytes、全輸出 SHA、scope 及零
正式寫入，沿原 capture／quarantine／retained reader 使用同一原路徑身分。
所有歷史失敗、原始 trial 與 fixture 都未回寫或回填成功日期。

**當時清理驗收進行中：**精確封存已建立並進入 D 發布，當時尚未認列原副本回收量。
完整冷恢復、當前 consumer／FD／mmap／pin／lease／transport／quarantine gates、
最後 inode unlink 及另一程序的完整刪後冷讀回，已由上方 2026-10-25 01:24 驗收補齊。

18:53 的 D 提交已完成；隨後 WSL 重啟，使原 one-shot 及等待它的唯讀驗收
沒有留下 terminal 收據。22:28 在正式 systemd／PID 1 mount namespace 重查：
195 原檔清冊指紋未變，完整編碼及固定 D head 保留，head metadata 與原 archive
manifest SHA 相同。這只是恢復定位，不是全冷驗收。兩份舊 run 已由原
`run-reconcile` 記為 interrupted 並保留；22:30 的實際大檔 native 全 SHA／decode
重新通過，單輪 33.77 秒，當時負載已不同，不沿用前輪 6.99 倍為新 boot 的倍率。
互動 namespace 的不同 D mount 被 guard 正確拒絕後，已改由正式 owner 觀測；
沒有改掛載參數、繞過 guard 或刪鎖。沿原 frozen inventory／receipt-dir 續跑。

本輪重啟前 17 時段的其他 audit：FinLab 18,096 份收據、18,082 個保留 payload 路徑，沒有
可合併的重複配置；managed GC 五根中四根 pinned、一根 active lease，沒有
可逐出的候選。首次 331 個大副本的完成證據清點未獲接受；本輪只為上方具名
public-builder 重新核對原始完成證據，不將該次清點當成現在仍有 331 個待處理。
未知副本不依名稱或表面重複刪除。先前回收量、新建 ABBA 暫存及編碼暫存
一律分開計算。

23:00 的唯讀取樣確認：正式發布已離開共用 owner，接續核對 immutable packs、
暫存的完整 materialized SHA 與原始來源。仍有通用 materialized reader 經 9p
讀取大型成員；新增的 native 路徑改善 encoded/decode 子流程，沒有涵蓋所有
通用 reader。因此不宣稱全部 D I/O 已走原生通道、完整退休加快 6.99 倍或已達
硬體極限。進行中的交易保留載入版本，沒有為改測量而插入 monkeypatch。

### 23:20 通用暫存 checksum 追加方案（實作前）

連續 FD 與堆疊取樣確認 `_verify_materialized` 仍以 9p 全量 hash 這兩個 D
暫存區，並不是鎖等待。這個檢查在發布、獨立恢復與 stage 回收都會使用，
只加速解壓入口仍會留下多次慢速 byte I/O。接續只為 64 MiB～8 GiB、兩個
既有唯讀 scratch 前綴的大檔採用原 `windows_cold_io.hash_file`；SSD、其他
namespace、小檔、過大檔保持原 reader。不增加寫入或 copy 權限；完整 SHA、
大小、path／FD generation、manifest 全集合與原 cold／source gates 全保留。

先保留原測量與程式，補 checksum／損壞／native 失敗／FD 及 path 變動／範圍
分流語意測試，重新跑共同回歸與真實完整來源工作流。若通過，只在己方具名
交易仍處於原副本未修改的驗證階段接手新版；中斷收據保留，固定 inventory、
完成編碼與 D head 沿用，後續全部 fresh gates 重新執行。原 ABBA 不回填新
程式身分，也不將新子流程宣稱全系統速度上限。

23:33 的新版真實驗收已通過：固定 D 版本的 **15 個冷物件／1,841,356,395
stored bytes**，以及 **196 個暫存成員**（195 個原檔的 encoding 與 archive
manifest）全部核對完整 SHA、大小與清單，耗時 **310.14 秒**。這個 wall 包含
冷物件及完整 materialized 檢查；不是嚴格配對的舊／新 ABBA，不報加速倍率。
FD／path replacement、append、mode／link 變動、native 失敗、壞 SHA、FIFO／
symlink、尺寸與 namespace 分流及額外成員都有語意測試；23 項新增案例已算在
重新執行的 896 項一般環境通過案例內，另九個正確角色案例與 156 項完整
adapter／codec 回歸亦重新通過，沒有將初次與本次重跑再加總。

新版程式另固定真實 FinLab 表做四輪完整 ABBA，原值、兩版發布／冷還原及
三 reader 均通過。此次負載下 before／after wall 中位數 **37.90／41.46 秒**，
CPU **28.62／32.49 秒**；保留此負面結果，不宣稱 footer reader 穩定加速。
原有比較檔保留，新版 code identity 與冷還原證據另綁定新版比較，沒有修改
早期量測或以較快單輪代替中位數。新建測量暫存的移除亦與原有空間回收分開。

23:37 接手先驗舊 run 的 PID／start ticks／boot、原始發布檢查點與固定 D
身分、新版全部驗收及程式 SHA；只在 `independent-D-original-recovery` 的唯讀
階段送 SIGINT。原 run 留為 cancelled，等待該 run 的舊唯讀驗收也保留退出
收據；沒有 source／head 寫入或 payload 刪除。新版沿同一 inventory／receipt-dir
與不變的發布檢查點續跑，不重新編碼，獨立冷恢復及之後的 fresh gates 全部重做。
完成前不認列原有檔案回收量。

追加證據：[新版完整工作流 ABBA](../artifacts/operations/filesystem-materialized-reader-20261024/comparison.json)、
[固定 D／196 成員完整驗收](../artifacts/operations/filesystem-materialized-reader-20261024/stage-qualified.json)、
[23 項新增語意](../artifacts/operations/agent-workflow/runs/filesystem-materialized-native-s-20261024T152356-7c945937/run.json)、
[新版共同回歸](../artifacts/operations/agent-workflow/runs/filesystem-materialized-shared-r-20261024T152411-19902754/run.json)、
[新版正確角色九項](../artifacts/operations/agent-workflow/runs/filesystem-materialized-canonica-20261024T152545-4fb7d23d/run.json)、
[新版 adapter／codec](../artifacts/operations/agent-workflow/runs/filesystem-materialized-archive--20261024T152545-1ad3a7c1/run.json)、
[唯讀檢查點接手](../artifacts/operations/filesystem-materialized-reader-20261024/handoff-readback.json)。

證據：[真實完整工作流 ABBA](../artifacts/operations/filesystem-partition-footer-20261024/comparison.json)、
[33 根 D 版本入口](../artifacts/operations/filesystem-partition-footer-20261024/layout-heads-current.json)、
[精確 frozen inventory](../artifacts/operations/filesystem-public-assembly-inventory-r2-20261024.json)、
[544 項回歸收據](../artifacts/operations/agent-workflow/runs/filesystem-public-evidence-tests-20261024T085410-5ac668b5/run.json)、
[原生大檔完整驗證 ABBA](../artifacts/operations/filesystem-partition-footer-20261024/native-stage-abba-readback.json)、
[156 項 adapter／codec 回歸](../artifacts/operations/agent-workflow/runs/filesystem-native-stage-final-re-20261024T103658-42877a37/run.json)、
[原回收流程進度](../artifacts/operations/filesystem-public-assembly-retirement-20261024/progress.json)。

## 2026-10-24 16:40 接續方案（實作前）

### D 暫存原生讀取追加方案（實作前）

恢復作業的實際 FD／I/O 觀測顯示，大型已完成編碼仍以 WSL 9p 讀取；單一
1 GB 以上的成員需要數分鐘。編碼與原值的完整 SHA／解壓驗證有必要，逐批
透過 9p 讀取同樣的位元組沒有語意必要。接續沿用現有 Windows FileStream
binary adapter，僅為兩個固定 D completed scratch 前綴加入唯讀 byte I/O；
核對真正 D 掛載、Linux FD／path 的完整前後身分及原本全部雜湊，不新增 writer。
寫入／copy 與其他磁碟拒絕，64 MiB 以下成員保留原 reader，避免程序啟動成本。
私人 pruning rename 後的完整核驗仍沿同一固定 D scratch，沒有略過任何成員。

先保存原程式，測試三種 codec、完整 EOF、來源替換／修改、native 失敗及非 D
路徑；再用目前具名大成員對照完整 encoded／decoded SHA，保留實測與負面結果。
兩個相關檔案及 byte-I/O 腳本未被另一項正在執行的 507-dependency crypto
實驗固定；不更動該實驗。只有測試與實際讀回通過，才在自己的可恢復操作
邊界接手新版本；來源、既有封存、固定清冊及失敗收據全部保留。

### 發布受擋後的恢復最佳化（實作前）

本輪第一次發布在提交前被正在掃描整個 `benchmarks` 的既有效能 profiler
正確擋下，exit 75；原檔、完整編碼與原失敗收據保留，沒有有效 cold head。
該 profiler 的程式 SHA 已被另一項全鏈路測量固定，不修改或終止它。
它退出後重新檢查 source consumer，結果為空，接續原 frozen inventory／owner。

恢復路徑有一輪可證明多餘的讀取：完整未提交編碼已存在時，上層 helper 先跑
`verify_archive_directory`，隨後 canonical `publish_archive` 會再次執行同樣的完整
封存／來源驗證。接續只移除上層這一輪；缺 manifest 的恢復／準備、發布器原本
的提交前後全驗、source guard、獨立冷恢復及退休 gates 全保留。以未提交正常
編碼、來源變動、編碼損壞及既有 fixed-publication 測試確認，不能因省去上層
掃描而讓壞資料發布。完整成功前後耗時尚未配對，不宣稱測得加速百分比。

本次沿用已部署的完整邏輯版本／immutable payload／8 MiB chunks 與 33 根
登錄，不更換 ext4／NTFS，不另建 collector、備份 writer 或回收管線。
上一階段已釋放的容量不重計。正式 FinLab 原生分片的負面結果仍有效。

1. 共用增量讀者在完整 SHA／schema／列數驗證時已解析 member 的 Parquet
   footer，隨後讀取又解析同一份 footer。只在同一個 `ParquetSource` 快照內
   重用該 metadata，不建立跨程序快取、不只用 mtime 判定，也不省略任一
   member 的完整 SHA、pointer／member fence 或篩選與尾端讀取的語意。
   新讀取開啟原檔後仍驗前後身分，結束時明確關閉 owned reader。
2. 用已下載且 receipt 完整 SHA 綁定的實際寬表，對照修改前後完整讀取與
   共同版本發布／全原檔冷還原，檢查所有原值、型別、null、metadata、投影、
   tail 與來源異動拒絕。保留負面結果；單一讀者改善不等於全庫原生分片完成。
3. 新的唯讀清冊先審最大工程副本，第一個具名候選為
   `benchmarks/running_services_20261013/public-feature-assembly-20261015c`。
   只有原完成證據、精確原檔冷重建、當前 consumer／FD／mmap／pin／lease／
   transport 與 quarantine 均通過，才沿原 `retire_local_offline_artifacts` 退休。
   缺少完成證據或仍被使用就保留，不能補寫舊 producer 成功收據。
4. 最後分開回報本輪原有檔案最後連結釋放的 blocks、新增測試暫存的移除、
   WSL 可用空間與 D 冷庫可用空間；不將可用量變化當成刪除量，不清 D／NAS 歷史。

### 16:50 完成證據接線追加方案（實作前）

331 個剩餘大副本未被原完成辨識器接受；不據此刪除。具名 public-feature
副本的 172 個 frozen 原件目前集合及 SHA 可核對，七份原始 trial 均宣告完成。
原歷史 verifier 曾因集合不符失敗；本次重驗七份時，原 a1 與後六份的 harness
不同，被原比較器正確拒絕。保留失敗；同 harness 的 b3/a3/a4/b4/e1/e2 已由
原 `verify_tw_public_feature_trials.py` 新做完整檔案讀回，通過 repeated variants
與 exact output proof。這是現在的獨立驗證，不補寫當年的成功紀錄。

原回收辨識器只讀 root JSON，而這個 producer 把原始完成收據放在各試驗子目錄，
且 fixture 與驗證各有不同 ABI。接續只為這一對明確 ABI 加入有界收據讀取與
完整 SHA／scope／零正式寫入／測量對應驗證：選入比較的試驗維持同 harness，
其他完成試驗只驗原值及來源相同，不混算性能。保留全部原 metadata bytes，
在原 capture、quarantine、resume 與 retained-reader 共用同一辨識。未知檔案與
未完成試驗維持拒絕。然後重新產生 frozen inventory、完整冷還原與 fresh gates；
原本被阻擋的 inventory 和所有 verifier 失敗均保留。

## 2026-10-24 16:04 接續驗收：未變版本零提交，變更分區才預留寫入

下方先行方案已實作到共用 `stockagent/data/partitioned_parquet.py`。
沿用實體 ext4／NTFS 與既有 D authority；版本層以完整邏輯清單引用共用的
immutable payload／8 MiB SHA-256 區塊。版本必須可獨立還原，新增版本不必
複製全部歷史。本次沒有更換正式 FinLab writer 或重新下載來源。

新 writer 完整驗證既有成員、本次 typed digest、來源／schema／程式身分及固定
generation 的位元組。完全相同就沿用原清單 inode，不再做容量預留、hardlink、
rename 或目錄 fsync，修正暫時 hardlink 改變 ctime／nlink、使既有 reader 的
來源驗證失效的問題。有變更才依真正新增的分區加上有界 metadata 預留容量；
來源或 schema metadata 改變仍產生 metadata 版本。新成員完整 typed readback、
耐久提交與舊版本恢復保持有效，沒有以刪除舊成員換取容量。

實測使用已下載且收據綁定的 `price:收盤價` 完整表，4,794 列、2,790 欄；
欄主要是股票維度，不能解讀成 2,790 個模型特徵。私有測試採同一張表的
4096 列分片、修改前後 ABBA 四輪，每輪涵蓋 unchanged 寫入、完整資料驗證、
共同 v2 發布、全部原檔冷還原及新舊固定 generation 讀回。

| 本次具名驗收 | 修改前 | 修改後 |
| --- | ---: | ---: |
| unchanged 完整工作流中位數 | 14.43 秒 | **9.66 秒（減少 33.1%）** |
| 同流程 CPU 中位數 | 10.36 秒 | **8.77 秒（減少 15.4%）** |
| writer 中位數，僅為其中一段 | 5.01 秒 | 0.46 秒 |
| 既有 reader／原清單 inode 維持有效 | 否 | 是 |
| writer 內容量預留／hardlink／rename／目錄 fsync | 有 | 全部 0 |

兩組共用及直接 caller 回歸合計 **792 個不重複案例通過**；最後另跑的 81 個
語意案例重複於共用測試，不加總。追加、metadata 變更、縮減、成員損壞、固定
generation 損壞與缺失皆有涵蓋。量測程式及被測來源 SHA 綁定於驗收紀錄。
這是單一真實完整表的有界量測；所有來源的原生 writer 尚未全部轉換，正式
FinLab 分片仍保留前輪較慢的負面結果，不能宣稱全系統皆快 33% 或只剩硬體瓶頸。

目前版本入口 metadata 觀測為 **33／33**：20 個 shared、12 個 host-private，
以及 1 個 local-news 完成收據。這證明版本入口存在，不替代全部來源歷史或
NAS 全歷史驗收。本次 provider 呼叫、正式來源寫入、D 冷物件刪除及 NAS
歷史刪除均為零。

清理使用原 managed GC 及兩個既有編譯快取範圍的 fresh audit：5 個物化資料根中
4 個 pinned、1 個 active lease，無可回收候選；cache-home 及 tmp-torchinductor
也都沒有符合原清理條件的檔案。因此本接續階段**原有資料實際回收為 0 bytes**。
四份本次新建的 ABBA 私有副本在完整冷恢復通過後，已精確移除共
213,893,120 allocated bytes；這是清除本次測試暫存，不計為既有空間回收。
下方較早階段已驗收的 6.19 GB 等回收量不重計。

15:54:51 的 PID 1 mount namespace 觀測：WSL 工作區 ext4 可用 **60.01 GB**，
D authority 可用 **483.84 GB**（十進位）。其他程序持續寫入，不能把可用空間的
變化當作本次刪除量。

證據：[獨立證據讀回](../artifacts/operations/filesystem-partition-noop-20261024/readback.json)、
[完整 ABBA 與逐輪呼叫計數](../artifacts/operations/filesystem-partition-noop-20261024/comparison.json)、
[33 根版本入口](../artifacts/operations/filesystem-partition-noop-20261024/layout-heads-current.json)、
[目前檔案系統容量](../artifacts/operations/filesystem-partition-noop-20261024/filesystems-current.json)、
[managed GC dry-run](../artifacts/operations/agent-workflow/runs/filesystem-noop-gc-audit-20261024T074601-8ad437b7/run.log)、
[home 編譯快取](../artifacts/operations/filesystem-partition-noop-20261024/compiler-audit/storage-pressure-audit-20261024T074849.074487Z.json)、
[tmp 編譯快取](../artifacts/operations/filesystem-partition-noop-20261024/tmp-compiler-audit/storage-pressure-audit-20261024T075842.369449Z.json)。

## 2026-10-24 接續方案：真正未變的分區不再提交（實作前）

沿用原 33 根版本架構、ext4／NTFS、D authority 與既有 writer／回收 owner。
先前已驗收的回收不重做、不重計；FinLab 較慢的原生分片仍不啟用正式 collector。
本輪先修共用分區 writer 已確認的 no-op 問題，再審當前可回收項目：

1. 原 writer 在完整原值、來源身分及格式皆相同時，仍預留整表容量、建立暫時
   hardlink、重掛 manifest 與同步目錄。hardlink 的 ctime／nlink 變化會使既有
   reader 的完整來源 fence 失效。先完整驗舊成員與本次輸入的 typed SHA，只有
   完整 manifest 與不可變 generation 皆相同時，直接沿用原 inode／版本；不跳過
   原檔完整性，不只比較 mtime、日期、列數或值 hash。
2. 有變更才進入容量 admission／耐久提交，預估新增量依真正要寫的分區計算。
   來源身分、schema metadata 或程式身分改變仍提交 metadata 版本；追加、更正、
   縮減、壞成員及缺少 archive 各自驗證。正式 collector 不因共用 ABI 修改而
   自動切換儲存格式，原值、null、發布時鐘及舊版本恢復契約保持一致。
3. 以同一份真實來源完整表量測含驗證的 unchanged 工作流，對照修改前版本，
   並檢查讀者並存、全部值／schema、原 inode 及實際新增配置。保留完整失敗與
   負面結果，不把少寫 metadata 宣稱全系統只剩硬體極限。
4. 查原 managed GC 的 fresh dry-run；有明確候選才由同 owner 清理。使用中的
   pin／lease、未知或失敗副本、冷物件與 NAS 歷史不靠名稱判定可刪。最終分開
   回報本輪最後連結實際釋放量、歷史回收量及當前可用空間。

## 2026-10-24 14:50 本輪清理與刪除後獨立驗收

本輪依下方 13:00 的先行方案完成增量儲存實測、共同完成辨識與續跑修正，
並沿原 owner 精確退休
`artifacts/benchmarks/finlab-finmind-throughput-20261018/reservation`。
這是已完成的工程副本，正式下載器的 queues 與金融來源正本保持有效。

原副本先固定 D 版本、完整恢復並比對原 SHA，通過當前 source／mirror／D、
consumer／FD／mmap／pin／lease／transport 後隔離及精確 unlink。原回收交易
r4 已成功，全部原有工程副本及本輪編碼暫存均已移除。另一個獨立程序隨後
從固定 D 版本重新取回，對照清理前另存的完整原檔 SHA／大小／精確集合，
**58／58 全部相符**；成功恢復的私人暫存也已清除，不依賴被刪的原副本或 stage。

| 本輪具名範圍 | 驗收結果 |
| --- | ---: |
| 全部原檔 | 58 檔／6,187,523,975 logical bytes |
| 原有 WSL 最後連結釋放的 blocks | **6,188,228,608 bytes（6.19 GB）** |
| 固定冷版本的唯一儲存 payload | 842,826,415 bytes（842.83 MB） |
| 原始完成紀錄另行保留 | 44 檔／9,448,410 bytes |
| 本輪新建後移除的 D encoding stage，另計 | 852,320,256 allocated bytes |
| 刪除後獨立恢復 | 全部原檔 SHA、大小及集合相符 |
| 版本層入口及 worker ready 身分 | 33／33 根相符，已載入程式與 checkout 一致 |
| 金融來源正本／D 冷物件／NAS 歷史刪除 | **0／0／0** |

6.19 GB 僅計本輪原有 inode 的最後連結配置，與清理前 frozen baseline
一致，不重計先前 Panel、group-status、分鐘副本或本輪新增暫存的回收。
842.83 MB 是此固定版本的 payload，不能冒充整個冷庫新增量或全庫壓縮率。

14:49:53 的 PID 1 namespace 新觀測：WSL 工作區 ext4 可用 **63.58 GB**，
D authority 經原 guarded 9p bind 可用 **486.32 GB**。其他工作持續寫入，
即時容量變化與本輪具名回收量分開計量。未進行 Windows VHDX 縮檔；四個
當前 mounted ext4 error counters 為零，這是當時觀測，不是離線檔案系統檢查。

unlink 當刻的本機 Syncthing idle、need bytes/items/deletes、pull/watch/folder/
system errors 均通過。此回收設定沒有要求具名 remote peers；本輪未驗此版本
NAS paired ACK，不據此宣稱離機交付或 NAS 全歷史完整。原因不明、未完成、
失敗的其他副本及有 pin／lease 的物化資料均不以名稱推斷可刪。

先前當前 consumer 阻擋的失敗、同步掃描等待及具名取消收據都保留。修改後
整個回收 CLI **274 passed**；完整冷恢復、原量化基線及目前程式 hash 綁定
於本輪總驗收。原生分片的較慢結果與未啟用範圍見下一節。

證據：[本輪總驗收](../artifacts/operations/filesystem-incremental-throughput-round-final-20261024.json)、
[刪除後全部原檔獨立讀回](../artifacts/operations/filesystem-throughput-independent-readback-20261024.json)、
[固定 D 完整恢復](../artifacts/operations/filesystem-throughput-independent-recovery-20261024.json)、
[33 根與最終容量](../artifacts/operations/filesystem-throughput-final-layout-capacity-20261024.json)、
[最終回歸及程式身分](../artifacts/operations/filesystem-throughput-qualified-tests-final-20261024.json)、
[原同步等待根因](../artifacts/operations/filesystem-throughput-transient-scan-diagnosis-20261024.json)。

## 2026-10-24 原生分片的完整實測與續跑修正

沿下方 13:00 的方案先規劃再實作。實體 ext4／NTFS 與既有 D authority 保留；
33 個資料根的版本層繼續以完整邏輯清單指向共用 immutable 檔案／8 MiB
SHA-256 區塊。版本清單記錄完整可還原集合，實際新增的 payload 才佔用新空間。
這與原生 collector 是否避免整張歷史重寫是兩個不同問題。

共用 `partitioned_parquet` 已增加明確的 `ordered_row_parts_parquet_v1`：
完整來源表按固定列數切片，原順序、索引、數值、null、型別與 metadata 均保留。
未變切片沿用原成員；追加、更正、縮減與 no-op 各自驗證，固定舊 generation
可唯讀還原。此模式不推算日期、不改來源資料；沒有完整舊前綴證明時仍提供完整
canonical 表。容量預留、單一 writer、每個新成員的完整 typed readback、fsync
及原子清單發布沿用原實作。

兩張來源收據綁定的真實完整表，各有 4,794 列、2,790 欄；前版取 4,793 列，
下一版追加來源中真實的下一列。每輪 ABBA 都包含耐久寫入、兩版共同 v2
發布、兩版全部原值冷還原，以及 Arrow／Polars／DuckDB 讀回。provider 呼叫及
正式來源寫入皆為零。逐年分區較大且較慢，結果保留；以下為共同 writer、
相同 ZSTD 編碼、移除重複 Arrow 檢查後的完整流程中位數。

| 真實來源 | 單檔雙版本 payload | 4096 列分片雙版本 payload | 節省 | 單檔總耗時 | 分片總耗時 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `price:收盤價` | 32.81 MB | 21.94 MB | 33.14% | 19.92 秒 | 40.69 秒 |
| `price:成交股數` | 36.52 MB | 32.72 MB | 10.41% | 16.66 秒 | 40.62 秒 |

分片降低新增 payload，但本機完整流程 CPU 分別增加至 1.68／1.87 倍，
總耗時增加至 2.04／2.44 倍。因此保留正式 FinLab 單檔 writer，尚未把此可選
ABI 接到正式 collector 或全部 readers。不能把版本層增量或可選介面完成，
宣稱為所有原生 writer 已改造或全鏈路只剩硬體限制。

原工程副本回收也修正兩個實際阻礙：throughput producer 的四份完成報告原本
位於目錄外，共同辨識現在綁定其固定日期目錄、原始 prepared-from 鏈、36 份
完整輸出 SHA、ABBA／claim-release 與零正式寫入，保留全部 44 份 metadata。
修正階段有獨立準備旗標，不推成累積旗標；原報告沒有補寫或改值。捕捉、隔離、
續跑及刪除後 retained-reader 都綁定同一原來源身分，未知 producer 仍拒絕。

已完成發布的精確 D snapshot／manifest 可接續驗證，不再重做封存準備；
這只重用 publication checkpoint。完整獨立 D 恢復、當前來源／mirror／D
雜湊、consumer／pin／lease／transport、隔離與 unlink 全部保留。真實生命週期
測試證明來源變更、編碼損壞或固定 head 改變仍拒絕刪除。

實際續跑另發現同步掃描超過 300 秒就丟棄仍有效的完整稽核，導致反覆讀取
全部原檔與冷物件。已修正為每次最多 60 秒的同步觀測，在同一程序原有
1,500 秒有效期限內等待；等待時釋放共用 mutation owner，真正 apply 才重新
取得 owner 並驗全部來源、consumer、transport 條件。期限及原時間戳不延長，
跨程序不得重用 owned byte proof，過期才重做完整稽核。新增 6／15 分鐘掃描
案例及原有來源變更、consumer、過期、組合錯誤與 unlink 測試，針對性
**18 passed**；原尚未刪檔的等待 run 已具名停止，原證據保留。

共用封存／恢復／回收／分片回歸 **583 passed**；最終分片模組 **117 passed**；
後續 publication checkpoint 修正的針對性測試 **21 passed**；所有 CLI 修正後
再跑整個回收 CLI，**274 passed**。數量重疊，不加總成不重複測試數，也未宣稱
整個 repository 的 full suite 通過。

證據：[完整雙版本實測](../artifacts/operations/filesystem-shared-row-final-study-20261024.json)、
[保留正式 writer 的決策](../artifacts/operations/filesystem-shared-row-decision-20261024.json)、
[最終測試結果與程式身分](../artifacts/operations/filesystem-throughput-qualified-tests-final-20261024.json)。

## 2026-10-24 13:00 原生分區與本輪清理方案（實作前）

沿用 `all-dataset-incremental-layout-20261016` 與現有 ext4／NTFS、D authority、
33 根完整邏輯清單及 immutable payload。不再新增備份 writer。最新容量查詢為
WSL 48.26 GB、D 524.32 GB；其他工作仍會寫入，這不是本輪回收量。

1. 先以來源收據綁定的完整 FinLab 寬表，比較單檔與逐年／逐月分區。使用真實
   前綴及其原下一列作離線追加案例，保留全部欄、原索引字串、型別、null 與
   metadata；期間標籤只作儲存分區，不推論發布時間。每輪包含耐久寫入、共同
   v2 發布、兩版全部原值冷恢復及 Arrow／Polars／DuckDB 完整讀回，保留負面
   結果。先用實測決定粒度，不能先宣布分區一定更快或更省。
2. 若完整案例支持原生分區，沿既有 `partitioned_parquet` 與 FinLab collector
   共用入口擴充 reader-first ABI。舊單檔及原收據保持有效；分區版本明列完整
   有序成員及內容 hash，只重寫真正改變的期間。任何 reader、完整性檢查、
   release、workload／網站對新格式尚不相容時，不啟用正式 writer。
3. 先檢查既有 managed GC 及已完成工程副本的清冊，再用原凍結 inventory、
   完整固定 D 還原、當前 consumer／FD／mmap／pin／lease／transport、隔離及
   精確 unlink。未完成、失敗、未知副本與來源正本繼續保留；不為清空磁碟而
   刪掉仍有引用的舊版本或移動冷庫。原回收、新建暫存與最後連結配置分開計量。
4. 最終回報本輪實作與實載範圍、完整恢復與實際配置回收量。原生各來源、固定
   來源基線、最新追新及 NAS 全歷史覆蓋各自驗收，不以 33 根版本 head 代替。

## 2026-10-24 12:47 原 Panel 副本與 v29 最終驗收

依下方 11:40 的先行方案完成實作及操作。現有 ext4／NTFS 沿用；33 個
`data_*` 根以完整邏輯清單指向共用 immutable files／8 MiB SHA-256 區塊，
未變內容沿用，新版本保存變更與清單。追加、更正、刪除均保留固定舊版的還原能力。

v29 已於 11:39 沿原 code-only restart 入口實際載入，worker PID 為 2224171，
ready identity 為 `d87a7d8df028228d00d39027afbd4331cabbb20e4f035dc6c7f075f78112416d`。
原 journal、未封閉 staging、PG／Temporal 與三條持久 workflow 保留；換版後
三鏈完整 replay、33 根 head、60 個固定來源基線的原恢復收據綁定及三個期貨
跨根連結通過。12:14 的實際程序觀測及 12:47 的 ready／checkout 身分再次一致。
當輪追新的 30／60 與已完成的固定基線分別計量。

舊 Panel 卡住的原因是第一代 producer 未記錄後來新增的 code／chunk-policy
欄位。共用完成辨識現在支援其精確原始格式，仍要求四輪完整 ABBA、全部陣列
雜湊／axes、timings／counters、原來源證據及 matching progress。缺失欄位維持
`not_recorded_by_original_producer`，原收據沒有改寫；不據此聲稱重現當年的數值效能。
本輪針對性 **322 項**及共用封存／恢復／回收 **252 項**測試均通過，合計
**574 項不重疊測試**。

具名範圍為 `artifacts/benchmarks/panel_chunk_storage_20261016_v1`。原 owner 已
完成固定 D 發布、全部原值恢復、當前來源／程序／pin／lease／transport 審核、
同磁碟隔離、精確 unlink 及 encoding-stage prune。另一程序在原交易成功且
編碼暫存已移除後，從固定 D 版本獨立取回全部原檔，逐一比對清理前固定的 SHA、
大小與精確集合；成功恢復工作區也已清除。

| 本輪原 Panel 工程副本 | 最終結果 |
| --- | ---: |
| 全部原檔及 logical bytes | **180 檔／5,029,589,560 bytes** |
| 原有 WSL 最後連結實際釋放的 blocks | **2,119,032,832 bytes（2.12 GB）** |
| 固定冷版本的唯一儲存 payload | 956,061,267 bytes（956.06 MB） |
| 保留的原完成收據 | 37,816 bytes |
| 本輪新建後移除的 D encoding stage，另計 | 2,112,720,896 allocated bytes |
| 刪除後獨立恢復及版本入口讀回 | **180／180 原檔、33／33 根** |

原檔有硬連結，5.03 GB logical bytes 不等於可回收量。2.12 GB 僅計本輪原有
inode 最後連結釋放的配置，不重計先前回收或本輪新建暫存。956.06 MB 是此固定
版本的唯一 payload，並非整個冷庫本輪新增用量。金融來源正本、D 冷庫物件及
NAS 歷史刪除數皆為 **0**。這批回收提供 D 主冷庫的完整恢復證明；該版本的
NAS paired ACK 屬獨立驗收，本輪未核對其離機交付。

12:13 的原 managed GC dry-run 檢查五個視圖，四個 pinned、一個 lease-active，
全部保留、可回收數為零。12:47 的 PID 1 namespace 容量觀測：工作區為 ext4，
D 主冷庫經原 guarded 9p bind 存取，可用分別為 **48.03 GB／525.41 GB**。
即時可用空間與具名回收量分開記錄；Windows VHDX 沒有縮檔。

唯讀 observer 曾在開始恢復前等待 1,800 秒到期，原交易持續完成。保留其失敗
收據後，未改碼 r3 在原交易成功後完成全部獨立恢復及 33 根讀回；已沿原 task
記錄 superseded 處置。容量查詢第一次的輸出解析指令失敗，完整原觀測保留；
修正為 JSON 解析後，同一 canonical 查詢重新通過，source payload 讀寫皆為零。

原生不同內容的整表逐期分區仍未全數完成；版本層增量與原生 writer 成本分開
驗收。33 根 head 觀測是版本結構的證明，來源全部歷史及 NAS 全歷史完整度各自
依其收據驗收。全鏈路僅受硬體限制的結論仍未證實。

證據：[本輪固定範圍總驗收](../artifacts/operations/filesystem-incremental-v29-original-panel-round-final-20261024.json)、
[574 項最終回歸](../artifacts/operations/filesystem-original-panel-final-regression-20261024.json)、
[v29 正式實載與三鏈讀回](../artifacts/operations/filesystem-v29-final-readback-r3-20261024.json)、
[刪除後全部原 SHA 與 33 根驗收](../artifacts/operations/filesystem-original-panel-readback-20261024.json)、
[完整獨立冷恢復](../artifacts/operations/filesystem-original-panel-independent-recovery-20261024.json)、
[掛載與容量查證](../artifacts/operations/filesystem-original-panel-final-capacity-verified-20261024.json)。

## 2026-10-24 11:40 接續方案（實作前）

現況是 33 個來源根已有 v2 完整邏輯清單與共用 immutable 檔案／8 MiB 區塊，
不是尚未建立版本層；部分原生來源仍寫整張表，兩者須分開報告。沿用 ext4／
NTFS 與既有 authority，不重新格式化，也不新增另一個備份 writer。

1. 先核對原 v29 資格化、完整 workflow replay 與目前 checkout 身分。既有
   安全交接已等待八個多小時，實載仍是 v28。依使用者先前「直接換新版沒有
   問題」的授權，僅在原 canonical code-only rollout 入口採用已支援的
   `--restart-active-owner`；取消的只是本任務等待器與唯讀後驗證器。保存原
   journal、未封閉 staging、PG／Temporal 與固定來源版本，之後驗三條持久
   workflow、全部資料根及新程序實際 ready 身分；不把磁碟上的 code hash 當實載。
2. 舊 `panel_chunk_storage_20261016_v1` 比較已有四輪 ABBA、完整陣列雜湊及
   終端收據，但早期 producer 未寫後來新增的 chunk-policy 欄位。先核對當年
   producer 與原始收據，只有可證明完成時才在共同完成辨識入口兼容原 ABI。
   不改寫舊收據，不以缺欄推測新值。具名 inventory、完整冷還原、當前程序／
   pin／lease／transport／quarantine 均通過後，才沿原回收器精確退休。
3. 受保護的五個 managed materialization 繼續保留。所有回收量只計本次原有
   inode 最後連結實際釋放的 `st_blocks`；既有回收、新建暫存與 VHDX 縮檔另計。
   無法證實的來源原件、冷庫物件、NAS 歷史不在本次刪除範圍。
4. 原生整表逐期分區維持明列未完成；本輪先讓既有增量版本優化真正實載並
   回收已證明不用的副本，不以更多版本命名或 CDC 取代原生分區驗收。

## 2026-10-24 11:13 相鄰收據副本的最終獨立驗收

依下方先行方案完成修正與原 owner 回收。另一個程序在原交易成功、原副本及
encoding stage 均已移除後，從固定 D 冷版本獨立重建；不重用原編碼暫存。
全部原值 SHA、大小、精確集合與原外部完成收據相符，成功還原工作區已移除。

| 本輪具名 FinMind group-status 工程副本 | 最終驗收 |
| --- | ---: |
| 全部原檔 | **868 檔／4,779,350,984 logical bytes** |
| 原 WSL 最後連結實際釋放的 blocks | **4,781,801,472 bytes（4.78 GB）** |
| 冷版本唯一儲存 payload | 338,549,377 bytes（338.55 MB） |
| 所有 encoded member 大小合計，含相同內容 | 676,611,614 bytes |
| 本輪新增後清除的 D encoding stage，另計 | 682,762,240 allocated bytes |
| 保留的原 sibling 比較收據及其 retained bytes | 2 份／34,209 bytes |
| 同次讀回的共用 v2 head | **33／33 個實際頂層資料根** |

原 producer 比較收據、金融來源正本、正式 queue 與固定冷版本保留；provider
原檔、D 冷庫物件與 NAS 歷史刪除數皆為 **0**。4.78 GB 只計本輪原有副本的
最後連結，先前回收與這輪新建暫存不重計。冷庫唯一 bytes 與 encoded member
合計是不同分母，不能直接相加。驗收時 WSL 可用 **49.10 GB**、D 可用
**532.70 GB**；其他工作仍會改變可用空間，不能將淨變化當作本次回收量。
Windows VHDX 沒有縮檔。

11:25 另由原 control runtime 在 PID 1 mount namespace 核對實際掛載：WSL
工作區為 ext4，D 主冷庫經既有 guarded 9p bind 存取。此時可用空間為 WSL
**46.36 GB**、D **531.95 GB**；上述 11:13 讀回後其他工作仍有寫入，因此
不能把兩次可用空間差異算成本輪刪除成效。
[最新掛載與容量觀測](../artifacts/operations/filesystem-final-capacity-observation-20261024.json)。

最終共用封存／回收回歸 **445 項全部通過**，包含實際 quarantine 生命週期、
相鄰收據變動、舊 proof／錯誤完整 counts／manifest SHA、PID／start／boot 不符
拒絕及原 consumer／pin／transport 拒絕。FinLab 的 **170 項**另行通過，兩個
最終範圍合計 615 項；不把先前重疊回歸累加。v2 以測試驗明完整 archive decode
在同一次恢復中只於 mutation 外執行一次，沒有將這個呼叫次數證明當作正式
latency 或所有儲存操作的加速保證。
[最終語義資格化、程式 SHA 與兩輪測試收據](../artifacts/operations/filesystem-owned-quarantine-proof-v2-qualified-20261024.json)。
其後 encoding-stage prune／manual capture／manifest recovery 補充回歸
**92 項全部通過**，三個不重疊的最終範圍合計 **707 項**；
[92 項最終收據](../artifacts/operations/agent-workflow/runs/filesystem-archive-stage-final-r-20261024T030653-9b75e483/run.json)。
[本輪固定範圍總驗收](../artifacts/operations/filesystem-incremental-storage-round-final-20261024.json)
綁定上述測試、還原、程式及最新容量證據，原生分區與 v29 換版仍列為 pending。

第一輪保留證據 reader 漏綁原來源；後續隔離改名又失去原 producer 身分，均
保留原失敗並修正共用入口。原恢復曾被一個以 benchmark 祖先為範圍的實際
profiler 擋住，待該工作自然結束再續接；沒有降低 consumer gate 或停止它。
獨立驗證曾誤用 control runtime、或在等待原交易時到期，沒有發布驗收；後續
用原 data runtime 及最終固定 verifier 全部通過。更新 verifier 時僅取消仍在
等待的本輪唯讀 observer，原回收交易保持同一 owner 直到完成。

證據：[最後完整冷還原與 33 根讀回](../artifacts/operations/filesystem-finmind-group-readback-20261024.json)、
[凍結原清冊](../artifacts/operations/filesystem-finmind-group-inventory-20261024.json)、
[原交易最終接受](../artifacts/operations/filesystem-finmind-group-retirement-20261024/progress.json)、
[逐原檔獨立恢復證據](../artifacts/operations/filesystem-finmind-group-independent-recovery-20261024.json)。

本輪實際回收交易載入舊 owned-resume v1；下方 v2 是後續呼叫的共用鎖優化，
不能用這筆交易宣稱 v2 正式加速。原生不同內容的整表仍未全面分區化；v29
候選也尚無實載驗收，繼續由既有安全交接負責。沒有宣稱硬體理論極限、全部
來源歷史已完成或所有 NAS 版本都已獨立驗收。

## 2026-10-24 09:40 FinLab 原下載器增量序列化驗收

依下方先行方案，固定已由來源收據 SHA 綁定的完整實際寬表，以前 4793 列與
完整 4794 列作離線追加案例；兩版皆保留全部 2790 欄。沒有呼叫 provider、
改寫正式來源或把案例描述為新取得的歷史觀察。每個 A/B/B/A trial 均完成
寫入與 fsync、共同 8 MiB 發布、全部冷重建、materialization、全值／型別／null／
metadata 比對，追加後再讀回舊版。最終雙方同樣使用 Arrow、Polars、DuckDB。

| 成交股數完整案例 | 原 writer | 採用的 4096-row／plain／byte-plane |
| --- | ---: | ---: |
| 新一版增加的冷 payload | 40,117,675 bytes | **9,874,958 bytes（少 75.39%）** |
| 兩版的唯一 payload 合計 | 80,224,272 bytes | **36,521,838 bytes（少 54.48%）** |
| 最終 ABBA 的平均完整 wall | 14.068 秒 | 9.370 秒 |
| 同輪 process CPU 平均 | 14.171 秒 | 12.214 秒 |

這是完整離線案例的測量範圍，不是正式 D／9p、全庫遷移或所有日常增量的速度
保證。原 writer 兩次 wall 分別 18.314／9.821 秒，現場負載有明顯變動；保留
逐次原值，不把平均差推論為整體系統倍數。初選 1024／512 row groups 在價格表
增加 CPU／wall，沒有套用；成交量的僅分組及其他編碼候選亦保留原結果。
其後另將 plain／byte-plane 用於收盤價完整三-reader ABBA：兩版 payload
32,813,215 → 26,517,099 bytes，但首版 16,404,367 → 21,644,161 bytes，
平均 CPU 15.505 → 18.974 秒、wall 15.361 → 15.526 秒。因此仍不套用收盤價，
不能只因增量 bytes 較少就稱整體較快。

原共用 writer 及一般 history collector 現在只對 `price:成交股數`、至少 4096
列／256 欄、字串索引且其他全 Float64 的已測 grain 使用此方案。receipt 明列
`finlab_observed_wide_history_byte_planes_v1`、row group、dictionary 與 byte-plane
三個選項；財務 processing ABI／原值／單位不變。相同 upstream revision 仍走
原 unchanged 路徑，新格式只供後續不同內容，不強迫全庫轉寫。其他型別保留原
writer，沒有宣稱所有原生寬表已按期分區。
原 `run_finlab_refresh.sh` 每鍵另起 `download_finlab_history.py sync --limit` 程序，
後續工作會自然載入新 writer；本輪未重啟下載服務或強迫呼叫來源 API。
尚未觀測到套用新格式的正式來源修訂，完整效能結果仍是上述固定原值離線案例。

最終 **170 項 FinLab 回歸通過**，覆蓋原 collector 原子提交、來源 unchanged、
舊檔與舊版本仍在、股單位及全表 raw parity；新測試另以三 reader 比對 Float32／
Float64 的精確 bits（含負零、帶 payload 的 NaN、極值）、null 及 int64 極值。
首次新測試誤把 list 當 Arrow numpy mask，被正確拒絕，改為 bool ndarray 後通過。
第一輪 byte-plane 比較也因測試程式測量中更動，被 code-stability gate 拒絕；
後續固定程式的對稱三-reader ABBA 及原 native policy 資格化均通過，原失敗保留。

證據：[最終原生 policy 完整資格化](../artifacts/operations/filesystem-finlab-final-native-qualified-20261024.json)、
[最終 170 項回歸](../artifacts/operations/agent-workflow/runs/filesystem-finlab-final-layout-t-20261024T013810-78f83ff1/run.json)、
[價格及初選負面結果](../artifacts/operations/filesystem-finlab-parquet-study-20261024.json)、
[plain 編碼比較](../artifacts/operations/filesystem-finlab-plain-encoding-study-20261024.json)、
[byte-plane 分組比較](../artifacts/operations/filesystem-finlab-byteplanes-groups-20261024.json)。
[收盤價 byte-plane 負面取捨](../artifacts/operations/filesystem-finlab-close-byteplanes-study-20261024.json)
亦保留完整結果。
可重跑的工具為 [`benchmark_finlab_incremental_parquet.py`](../scripts/benchmark_finlab_incremental_parquet.py)。

## 2026-10-24 FinLab 寬表增量寫入方案（實作前）

保留已驗收的 33 根完整邏輯版本與 8 MiB 區塊，不另建檔案系統或發布管線。
現行一般 FinLab 寬表多為單一 row group；尾端追加可能改寫各欄的壓縮頁，
即使只有一筆新觀察，也不一定能沿用舊位元組區塊。此處尚未以實測斷言收益。

1. 只讀已取得且由現行收據完整 SHA 綁定的日收盤與成交股數寬表，固定完整欄位、
   順序、型別、原值及 null。以同一實際表的前綴及新增一列形成離線追加案例；
   不呼叫 provider、不寫正式資料，不將案例當成新增歷史觀察。
2. 比較現行 writer 與固定 row-group 候選，計入完整寫入、所有 8 MiB 區塊 SHA、
   全表獨立讀回、兩版獨有 payload、metadata 及檔案尺寸；失敗或膨脹也保留。
   只有真正減少增量儲存且可接受的完整流程成本，才採用於原共用 writer。
3. 若採用，原 schema／財務 processing ABI 保留，另明列儲存序列化版本；
   舊 receipt／單檔 reader／歷史冷版本仍有效。相同來源修訂維持原 unchanged
   快路，不為新版格式強迫全庫轉寫；新內容由原 collector 原子提交。
4. 清理仍只沿已登錄的具名完成證據、完整冷還原與 fresh reference／pin／lease／
   transport／quarantine gate。未知或未完成 benchmark 保留，不改造舊成功收據。
   只計本次最後連結實際釋放的原有 blocks；v29 的既有安全交接保持原身分與 owner。

本次具名回收候選是 `running_services_20261013/finmind-group-status-abba-v2-20261015-fixture`。
原 producer 把 v2、v3 的完整 A/B/B/A 收據寫在 sibling JSON，沒有寫進 fixture，
因此舊完成檢查錯誤地把它歸為沒有完成證據。先在原共用完成入口驗證這兩份
原收據的 fixture／prepared-from 綁定、完整狀態／Parquet／receipt SHA、全 task
值相等、來源／code 穩定與零正式 queue／provider 寫入，再走原凍結清冊。
保留原 sibling 收據，另由原回收器留存其完整 bytes，不補寫新的舊 producer 成功證明。

## 2026-10-24 隔離恢復的共用鎖優化方案與邊界

實際回收中發現兩個不同問題。第一，原比較收據在 fixture 的相鄰位置，改名成
正式 quarantine 的 `source` 後，舊 reader 不能辨認原 producer，原 gate 因而保留
資料。現在只從已審核的 artifact root／spec 還原原位置，再驗既有 `retiring`
交易、隔離路徑、nonce、固定 snapshot／manifest 與完整原收據；任意名為
`source` 的資料夾不能借用完成證據。原收據不修改，保留其完整 bytes 與 SHA。

第二，已中斷交易的恢復並不再次 rename，但原流程在共用發布鎖外解碼並核對
全部資料後，進鎖又完整解碼一次。新 `owned-legacy-quarantine-resume-v2`
方案將這項大檔工作留在鎖外，同一程序內僅在 PID／start-ticks／boot 相同、
原 1500 秒期限內，且全樹 inode／ctime、固定冷版本、完整檔數／原值 bytes、
archive manifest SHA 和外部完成收據 bytes 完全相符時重用。刪除前後的 fresh
binding、process／service／pin／lease／transport gate 保留；過期、舊 v1 或不同
程序的重用證據拒絕。第一次 rename 與 encoding-stage rename 仍完整驗原值。

實作前記錄在
[方案與當時程式身分](../artifacts/operations/filesystem-owned-quarantine-proof-v2-plan-20261024.json)。
測試實際經過 prepare／publish／rename／中斷恢復／精確 unlink／cold restore；
另以 spy 核對完整解碼在 mutation 外只執行一次，並驗錯誤 counts、manifest SHA、
不同程序、發布證據變動與既有 consumer／transport 拒絕。最終測試結果另記於
本次最終驗收，不將尚在執行的 run 算成功。

本輪原交易早已載入舊 v1 程式，維持原 owner 繼續完成 stage prune；沒有為了
套用優化打斷它。因此本輪回收時間不能作為 v2 加速的實測，也不以隔離測試
宣稱達到全鏈路或硬體理論上限。

## 2026-10-24 08:59 FinMind 副本最終驗收

本輪先依下方方案實作，再完成原 owner 的精確回收及另一程序的完整冷庫讀回。
**33／33 個資料根**的 shared／host-private v2 head 重新核對通過；現有頂層
`data_*` 與註冊清單完全相符。每版完整清單引用共用 immutable files／8 MiB
SHA-256 區塊，未變內容沿用；來源原生逐期分區仍是另一項工作。

| 本次具名工程副本 | 最終結果 |
| --- | ---: |
| 原檔精確集合與全部原值 SHA | 86 檔／10,503,399,433 logical bytes |
| 冷庫封存內容，含原收據 | 1,397,778,892 encoded bytes（約 1.40 GB） |
| 原有 WSL 配置實際新增回收 | **10,504,556,544 bytes（10.50 GB）** |
| 本輪新建後移除的 encoding 暫存，另計 | 1,398,378,496 allocated bytes |
| 原 producer 比較收據，原 SHA 保留 | 3 份／41,544 bytes |

具名範圍是 `finmind_release_reserve_20261015.3tw97C`；金融來源正本、正式
queue、D 冷庫物件與 NAS 歷史刪除數皆為 **0**。原檔與 encoding stage 均已
確認不存在，另一程序完全不依賴該 stage 從固定 D 版本重建，核對精確集合、
大小及每個原檔 SHA；成功讀回暫存亦已移除。10.50 GB 以原檔最後連結的
`st_blocks` 計算，不重計先前回收或本輪新建暫存。

實作在原完成證據入口新增 `complete_finmind_reservation_comparison_v1`：
辨識原有兩個 queue owner、完整 reservation／status、固定順反序 trial、
全輸出 SHA、durable claim／release 恢復、零 provider 呼叫及正式 queue mutation。
沒有修改舊收據或補造新 ABI。較廣回歸為 **361 passed／1 failed**；唯一失敗是
測試假定 WSL `ctime` 單調，但實際觀測倒退 471,357,932 ns，正式 gate 正確拒絕。
僅修正測試，明確覆蓋前進及倒退時鐘；修正後 **28 項針對性測試通過**。
兩輪範圍重疊，不相加或描述成整輪廣回歸全部通過；原失敗及後續處置保留。

測試與私人封存準備並行，正式發布及刪除沿原共同鎖序列化。同步掃描曾在
刪除前預檢啟動，原流程未修改來源、未延長 full-proof 時間，待 idle 後以同一
frozen inventory 成功重試。最後獨立讀回初選 D／9p 工作區，被既有 native
workspace gate 在 fetch 前拒絕；保留空工作區及失敗，改用既有 tmpfs admission。
實測可用記憶體 87,181,578,240 bytes、需求 7,747,818,210 bytes，原 32 GiB
filesystem reserve 保留，未放寬正式 gate。原回收完整命令耗時 **1,359.67 秒**，
後續獨立完整讀回命令 **77.67 秒**；兩者工作量不同，沒有 ABBA 或速度倍數宣稱。

08:59 驗收當時 WSL available **48,887,021,568 bytes（48.89 GB）**，
D available **543,780,589,568 bytes（543.78 GB）**；其他工作仍在寫入或回收，
磁碟餘額差不是本次回收量。唯讀 superblock 檢查為 ext4／`clean`，保留區維持
5,368,709 個 4 KiB blocks；D 仍為原 guarded 9p 掛載。沒有格式轉換、offline fsck
或 Windows VHDX 縮檔，亦不以 `clean` 宣稱歷史問題已做離線修復。

v29 checkout 身分仍是已資格化 `d87a7d8d…12416d`，正式 ready 收據仍為
v28 `d98f04f2…90d3`；原安全交接繼續等待。部分原生來源仍整表修訂，NAS 全歷史
驗收與全鏈路硬體極限亦未由本次證明。managed cache 本輪 dry run 為
5 kept／0 eligible（4 pins、1 active lease）。

證據：[獨立完整讀回](../artifacts/operations/filesystem-finmind-reserve-readback-20261024.json)、
[原精確回收驗收](../artifacts/operations/filesystem-finmind-reserve-retirement-20261024/legacy-wsl-offline-929955edbf9d4b282cc02d0c/acceptance.json)、
[資格與原失敗說明](../artifacts/operations/filesystem-finmind-reserve-admission-20261024.json)、
[檔案系統唯讀檢查](../artifacts/operations/filesystem-superblock-readonly-20261024.json)。
獨立讀回 SHA-256：`6863926824f8471a752a017f1369d0902c943ed39e7d5d5c108fd54405dc331c`。

## 2026-10-24 FinMind 控制路徑副本回收方案（實作前）

沿用原 `all-dataset-incremental-layout-20261016`：33 個資料根的版本仍以完整
邏輯清單引用共用 immutable files／8 MiB SHA-256 區塊。ext4／NTFS、來源工作區、
私有來源權限與既有冷庫 owner 不變；完整版本清單不等於重複保存完整 payload，
也不代表 FinLab 等原生整表修訂已全部改成逐期分區。

本次新鮮 managed-cache dry run 為 5 kept、0 eligible（4 pins、1 active lease）。
不能靠移除這些保護來騰空間。具名工程副本
`benchmarks/running_services_20261013/finmind_release_reserve_20261015.3tw97C`
有三份原 producer 比較收據：兩個 owner 的完整 reservation／status 比對、
durable claim／release 恢復、完整輸出 SHA 一致、來源工作流程不變、零 provider
呼叫及正式 queue mutation；回收器目前未辨識這個完成 ABI。`du` 顯示約 10.50 GB，
尚未核對共享 inode，不能先把此數字當作可回收量。

1. 在原 `completed_benchmark_evidence` 增加此明確 producer 的完整證據辨識；
   驗證每份比較、原始／candidate 順序及 repeat、完整 SHA、兩個 owner 的 snapshot
   描述與恢復交易，不修改舊收據、不重跑 benchmark、不把單一成功旗標當完成。
2. 先跑正反向測試，再由原 exact inventory／dry run 核對具名來源、consumer、
   process、pin／lease 與穩定時間。原 v29 候選身分必須保持不變。
3. 仍由同一回收器在 guarded D scratch 封存、完整原值還原、驗 SHA／集合、
   quarantine 重驗及精確 unlink；傳輸忙碌只重試同一 frozen inventory。
   封存後保留三份原比較收據與原件恢復路徑。來源金融資料、正式 queue、D CAS
   及 NAS 歷史不刪；不新增清理 daemon。
4. 最後獨立讀回此固定冷版本，重新觀察全部資料根及當前 worker 身分；只報
   本次最後連結實際釋放的 `st_blocks`，不重計前輪回收或本次新建 scratch。
   不能從局部加速宣稱全鏈路已達硬體極限。

## 2026-10-24 08:01 五組官方測速副本驗收

已依下方實作前規劃完成本輪。**33／33 個資料根**的 shared 或 host-private v2
head 在清理後重新讀回通過，頂層 33 個 `data_*` 入口沒有漏註冊項目。
每版保有完整邏輯清單，引用共用 immutable files／8 MiB SHA-256 區塊；更新只
保存獨有內容，相同內容沿用原版本。指定版本能直接完整還原，來源的原值、單位、
null、時間及授權邊界沿用。ext4 與 Windows D 掛載、既有下載器及備份 owner 保留。

本輪在原完成證據入口補上 `complete_official_daily_merge_v1`：辨識原 producer 的
terminal receipt、code SHA、十階段 timing、十份 source capture、完整 typed oracle、
full／suffix／unchanged 分工及零正式寫入，讓已完成比較副本走原回收器。
**376 項封存／還原／回收回歸通過**。各案例準備與核對最多兩組並行，發布與
原檔回收依原共同鎖序列化；所有來源／process／pin／lease／transport、完整 SHA、
quarantine 及刪除讀回 gate 保留。

| 已完成案例 | 獨立還原原檔 | 原有 WSL allocated 回收 |
| --- | ---: | ---: |
| `official-real-daily-20261020-v68` | 213 | 803,282,944 bytes |
| `official-real-daily-20261020-v74` | 53 | 513,413,120 bytes |
| `official-real-daily-20261020-v79` | 53 | 513,413,120 bytes |
| `official-real-daily-20261020-v83` | 53 | 513,413,120 bytes |
| `official-real-daily-20261020-v87` | 53 | 513,429,504 bytes |
| 合計 | **425** | **2,856,951,808 bytes（2.86 GB）** |

五組原檔與 encoding stage 均確認不存在，再由另一程序從固定 D 版本完整讀回：
**425 檔、4,554,675,151 logical bytes** 的精確集合、大小與全部原檔 SHA 相符，
包含 50 份原 source capture；20 份 inventory／retirement／acceptance／stage-prune
證據的 SHA 也再次讀回相符。暫存皆沿原 gate prune；本次新建暫存、先前 Panel／
research／其他已完成回收不加入這個新增數字。
原 provider 資料正本、冷庫物件與 NAS 歷史刪除數皆為 **0**，VHDX 未縮檔。
2.86 GB 是最後連結釋放的 WSL 配置量，不是跨盤淨節省量或整顆磁碟餘額差。
08:01 驗收時 WSL available **34,067,001,344 bytes（34.07 GB）**，
D available **551,192,109,056 bytes（551.19 GB）**；其他工作仍持續寫入。

第五組暴露了資源條件觸發的 runtime 問題：可用量跨過 SSD scratch 門檻後，
native ZIP reader 會驗 Windows VHDX 的實際容量，而舊 task launcher 漏傳
`WSL_DISTRO_NAME`。原程序的環境已實讀確認缺少該鍵；原失敗及有界重試證據保留，
第五組最後由原重試完成，沒有重複刪除原檔。已在原 launcher 的非機密 runtime
白名單加入明確 distro，驗證名稱格式，systemd／tmux 一致，金鑰與未驗 bridge
不隨命令傳遞；**61 項 launcher 回歸通過**。修正後的新受控工作核對 enrollment
及 Windows registry，讀得實際 backing free **311,629,316,096 bytes**；只驗 runtime
context 並接受既有最終結果，未重跑五組回收或原件還原。

證據：[獨立完整讀回](../artifacts/operations/filesystem-official-retirement-readback-20261024.json)、
[原五組成功 run](../artifacts/operations/agent-workflow/runs/filesystem-official-safe-retire-20261023T225710-7d5cafed/run.json)、
[376 項回歸](../artifacts/operations/agent-workflow/runs/filesystem-official-regression-20261023T225200-cf803548/run.json)、
[61 項回歸](../artifacts/operations/agent-workflow/runs/filesystem-wsl-runtime-identity-20261023T235100-9cd8d928/run.json)、
[舊程序環境](../artifacts/operations/filesystem-official-old-context-20261024.json)、
[修正後實際 runtime／容量](../artifacts/operations/filesystem-official-runtime-context-20261024.json)、
[新受控工作](../artifacts/operations/agent-workflow/runs/filesystem-official-context-resu-20261023T235226-ecf4bddf/run.json)。

剩餘工作仍由原任務接續：FinLab 等部分 producer 的整表修訂需另做原生分區；
正式 worker 仍為 v28 `d98f04f2…90d3`，已資格化 v29 `d87a7d8d…12416d` 身分
保持不變，原有界安全交接持續執行。08:02 例行 60 來源為 verified 30、waiting_retry
28、processing 1、waiting_source_owner 1；FinMind routing/hashing 已到
**6,766,248／10,573,750 檔**，目前新增冷庫物件 3。這是當前每日發布進度，與
既有固定基線驗收分開。完整流程仍含 9p 掃描及原值驗證成本，本輪沒有硬體上限
或全流程 ABBA 加速倍數的驗收。

## 2026-10-24 07:50 暫存容量分支修復規劃（實作前）

前四組已完成，第五組的原檔亦已回收；原 prune 在進入容量 admission 時拒絕，
原錯誤及 retry 收據保留。原因是回收使 WSL 可用量跨過 32 GiB 加 pack 大小的
門檻，native ZIP reader 改走 SSD scratch，開始驗 Windows VHDX 實際餘額。
`stockagent-agent` 的 systemd runtime 白名單漏傳登入環境已有的
`WSL_DISTRO_NAME`，所以無法辨識 actual distribution；不是冷庫資料錯誤。

修正在原 task launcher 傳遞這個明確的非機密 runtime 身分，沿用既有 distro
名稱格式驗證，不傳入任意環境或金鑰，不修改已資格化 v29 的 reader／設定身分。
測 systemd／tmux 的同一環境語義、拒絕不合法身分、未設定時不猜測及機密不外傳。
等舊 apply 自行結束後，只由原 frozen inventory／retirement receipt 接續第五組
prune，不重新回收前四組；再沿原獨立讀回完整驗收五組及 33 根增量 head。

## 2026-10-24 06:50 接續規劃（實作前）

繼續同一增量資料集任務，原 33 根 v2 布局與已驗收回收不重計。版本的完整名稱
清單引用共用 immutable payload；修訂只增加獨有內容，刪除只從新清單移除引用，
仍被舊版本引用的內容保留。ext4／NTFS、既有 publisher、下載器及備份 owner 沿用。
FinLab 等原生整表修訂與共同冷庫增量是不同層，本次不將前者冒稱已全部分區。

當前 WSL 約剩 33.3 GB，不能以尚未完成的來源交易換版或降低容量保留量來騰空間。
v29 仍在原 24 小時有界 handoff 中；正式 v28 的 FinMind 交易正在處理 10,573,750
檔清冊，本次讀回已到 routing/hashing，保留其原 journal、鎖與程序。

本次新增回收範圍為五個 `official-real-daily-20261020-v{68,74,79,83,87}`
完成比較副本。原 producer `scripts/benchmark_official_daily_merge.py` 先實際核對
daily／full／unchanged 的完整 typed oracle 與原件 SHA，最後才寫 terminal receipt；
它沒有 fixture／ABBA 收據，既有回收器不能辨識。先在原完成證據入口加入這個精確
producer ABI，核對原 code、十階段 timing、progress 一致、十份 source capture、
完整 oracle、full／suffix／unchanged 分工與零正式寫入，不重跑測速、不修改舊收據。
只開放具名 standalone case，不開放 benchmark suite／未知格式。

驗證後使用原 inventory/dry-run、D scratch、逐檔完整冷還原、consumer／FD／mmap、
pin／lease／transport、quarantine 與精確 unlink。不同案例 private prepare 可有界
並行，publication／retirement 仍沿原共用鎖；共享 inode 最後一個名稱移除才計回收。
先實際核對每組來源／在用引用，不能把舊 `du` 的五組約 2.57 GB 相加當可釋放量。
失敗／未知／使用中項目保留。最後從固定 D 版本由另一程序完整讀回，報告本次
新增 `st_blocks` 回收、當前餘額、仍未完成的來源分區／NAS／正式換版範圍。

## 2026-10-24 06:33 本次 Panel 副本最終驗收

共同版本層再次讀回 **33／33 個資料根**的共享或 host-private v2 head：完整邏輯
清單引用共用 immutable files／8 MiB SHA-256 區塊；保留 ext4／NTFS 與原權限。
這是目前版本布局／metadata 驗收，不是所有 provider 的資料都已追到最新。

兩組具名完成案例已沿原 owner 完整 D 還原、精確退役、隔離重驗及暫存 prune，
再由另一程序從固定冷庫版本完整讀回。**941 個原始檔、10,059,587,203 logical
bytes** 的精確集合、長度與全部原檔 SHA 通過；366 組原 inode 的共享名稱亦全部
對應相同完整 SHA。原檔及兩組 encoding stage 均確認不存在，較早缺少完整完成
證據的 `panel_chunk_storage_20261016_v1` 控制收據 SHA 保持原值。

| 固定案例 | 獨立還原原檔 | 原有 WSL allocated 回收 |
| --- | ---: | ---: |
| `panel_chunk_storage_20261016_8m_v1` | 470 | 1,259,814,912 bytes |
| `panel_chunk_storage_20261016_final` | 471 | 1,256,116,224 bytes |
| 合計 | 941 | **2,515,931,136 bytes（2.52 GB）** |

這是原檔最後連結釋放的配置量，等於刪除前最後連結候選；不是整個磁碟前後淨差。
另已移除本次新建的 C／D encoding scratch，共 4,262,117,376 allocated bytes，
**不再加進原有副本回收量**。先前 47.48 GB 與 5.35 GB 也不重計。
原 provider 資料、冷庫物件與 NAS 歷史刪除數均為 **0**；沒有縮小 Windows VHDX。
[最終獨立完整讀回](../artifacts/operations/filesystem-panel-retirement-readback-20261024.json)、
[成功驗證 run](../artifacts/operations/agent-workflow/runs/filesystem-panel-independent-rea-20261023T220527-c0d82963/run.json)、
[8m 原驗收](../artifacts/operations/filesystem-panel-ssd-retirement-20261024/legacy-wsl-offline-23dcba8c9f11ad13ba72eb22/acceptance.json)、
[final 原驗收](../artifacts/operations/filesystem-panel-final-d-retirement-20261024/legacy-wsl-offline-9aa1014c4d9ff197dd0ffef8/acceptance.json)。

這次修正原回收器對既有 Panel 完成收據的辨識，及同步掃描造成的 preflight
重做；**347 項共同回歸通過**，傳輸重試另以 **13 項針對性測試**驗證，不把
重疊測試再加成一個總數。獨立讀回使用原 API 的 tmpfs 暫存，兩次均先驗實際
tmpfs、MemAvailable 與可見 cgroup headroom，約 9.34 GB 工作記憶體預算及原
32 GiB filesystem reserve 保留；成功後暫存皆已移除。

SSD flow 的第一組已完成，第二組因原 source-size + 32 GiB admission 拒絕；
原 exit 75／失敗進度與收據保留。後續 D flow 沿完全相同的 frozen source row
成功接續，兩組由上方共同獨立讀回驗收，未把舊 SSD 進度補寫成全部成功。
第一組獨立讀回與另一組原 owner 的核對並行，刪除仍由原鎖序列化。

06:33 當時 WSL available 為 **33,584,095,232 bytes（33.58 GB）**，D 為
560,302,465,024 bytes（560.30 GB）；其他工作持續寫入，因此不能用餘額差計回收。
單一 D flow 全流程實測 3,104.78 秒，包含準備、發布、全部核對、同步等待與 prune；
這不是受控 ABBA，不據此推估 C／D 速度倍數。堆疊仍觀察到 9p stat、編碼原值
重驗與 ZIP CRC／SHA 的成本，未證明只剩硬體上限。

正式 worker 仍載入 v28 `d98f04f2…90d3`；已資格化 v29 `d87a7d8d…12416d`
身分未變、尚未實載，繼續由原有界交接等待安全邊界。部分原生來源仍整表修訂，
逐期分區與 NAS 全歷史驗收仍分開處理。下方實作前計畫與先前結果保留。

## 2026-10-24 本次接續方案（實作前）

本次沿原 `all-dataset-incremental-layout-20261016` 工作接續。第一原則是：
一個邏輯版本等於完整名稱清單，不等於另一份完整 payload；永久容量只應增加
獨有內容與必要 metadata。來源工作區、不可變版本、使用中視圖、可重建暫存
分別由原 owner 管理。沿用 ext4／NTFS，不在運行中重格式化磁碟。

1. 核對 33 個資料根的共享／private 增量 head，保留 private／授權及原下載器的
   原值、null、單位、修訂與時間；新聞、交易與其他工作不因此改變。
2. 原 v29 已資格化的 metadata 優化維持原有界交接，不藉新清理改動候選身分。
   FinMind 例行 census 的完整總數尚未知，進行中的 30／60 不是固定基線退步。
3. 已有三個 `panel_chunk_storage_20261016_*` 完成比較案例，原 producer 保存
   `acceptance.json`，而回收器只識別 fixture／ABBA 或 research-staging ABI。
   在共同完成證據入口辨識這個既有 producer：完整四次 whole/chunks/chunks/whole、
   相同完整 array hashes、所有 initial／append 階段、原 source/code digests、
   零 provider／正式 cache 寫入，並核對原 progress。原收據不補寫。
4. 只開放這個具名 standalone producer 的命名格式及對應 ABI；不能借用其他
   producer 完成狀態。測試後逐案例做 canonical inventory/dry run、完整 D 還原、
   consumer／pins／transport／quarantine 與精確 unlink；共享 inode 最後一個
   名稱移除後的 blocks 才計回收。舊 input、D 版本、NAS 歷史均保留。
5. 來源整表修訂的逐期分區仍是獨立範圍，不把冷庫分塊說成所有下載器都已避免
   重寫歷史。不能驗收的未完成 factorized 比較及使用中資料保持原樣。

本次 20 秒有界 benchmark metadata 清冊是不完整觀察，不可加總成全庫可刪量。
已完成的 47.48 GB／5.35 GB 回收也不重新計入；本次結果另以獨立讀回保存。

本次實際清冊補充：`final` 與 `8m_v1` 各約 5.03 GB logical、
各約 2.52 GB allocated，canonical dry run 沒有 blocker；跨案例仍有共用 inode，
不能相加成可回收量。較早 `v1` 沒有 source code／chunk-policy 身分，維持拒絕並
保留，不能為了空間補寫舊證據。D scratch 原入口每次只接受一個案例，因此用兩份
獨立 frozen inventory；最初的兩案例 admission 被正確拒絕，未進入編碼或刪除。

容量觀察確認可改用原 SSD scratch，另由原入口取得同樣兩個來源的
[固定 SSD 清冊](../artifacts/operations/filesystem-panel-ssd-inventory-20261024.json)；
沒有改寫原 D 清冊或降低 32 GiB 保留門檻。
[刪除前 inode 觀察](../artifacts/operations/filesystem-panel-allocation-before-20261024.json)
去除兩案例彼此重複後為 3,775,205,376 allocated bytes；仍有其他名稱持有的 inode
不能算釋放，最後連結候選只有 2,515,931,136 bytes，最終以原回收收據為準。

共同回歸 **347 passed**，涵蓋原封存、完整還原、暫存 prune、完成證據辨識及
拒絕未完成／來源變更／目前使用中資料。
[成功資格 run](../artifacts/operations/agent-workflow/runs/filesystem-panel-storage-qualifi-20261023T210115-8993ab1a/run.json)。
小型 JSON 改按已驗 stat 大小加一個 byte 讀取，原上限與前後 FD／路徑 fence 保留；
相同完整收據做 20 次 × A/B/B/A，追蹤記憶體 peak 約 **8.49 MB → 0.15 MB**。
此階段平均 wall 為 **0.16754 → 0.17099 秒**（慢約 2.1%），未證明 wall 加速，
也不是整體 archive／檔案系統的速度宣告。
[原值相等與完整樣本](../artifacts/operations/filesystem-panel-receipt-read-abba-20261024.json)。

05:24 台北實測暴露兩個後續阻擋，修正方案先記錄：第一組 D 完整原值還原已通過，
但等待到 transport idle 後，canonical apply 的新鮮 preflight 又遇到掃描；原入口
因此退回整輪還原。只對 `legacy retirement blocked: cold-peer-not-converged`
這個 **state／quarantine 寫入之前** 的精確錯誤，在原 1,500 秒 proof 期限與同一
mutation owner 內最多試三次，每次仍做原 convergence 與 canonical apply 全部 gates。
其他錯誤、複合 blocker 或進入 quarantine 後的錯誤不重試；不續延 proof。
第二組 SSD admission 當時不足 source-size + 32 GiB，會保留原失敗並沿已完成乾跑的
單案例 D inventory 接續，不降低門檻，也不在相同來源同時啟動第二 writer。
[13 項重試與真實 canonical gates 測試通過](../artifacts/operations/agent-workflow/runs/filesystem-panel-transport-prefl-20261023T212742-01471aa5/run.json)，
包含只在 preflight 重試、三次上限、逾期停止、複合 blocker／來源／unlink 錯誤
立即保留；既有完整 archive 回歸的 347 項結果另列於上方。

獨立讀回沿原 `verify_cold_archive` 執行。來源 D staging 與 native mutable
workspace 的角色不同，不能把 D 路徑塞入 native admission。若 SSD 未達原
encoded-size + 32 GiB，才考慮原 API 的 tmpfs 暫存：先核對實際 tmpfs 容量、
MemAvailable 及可見 cgroup memory headroom，另外保留大於兩倍編碼／最大冷物件
工作量加 4 GiB 的 RAM；不足就保留證據並 defer，不繞過原容量門檻。

## 2026-10-24 本次離線副本回收結果

原回收入口已支援既有 research staging 的完成收據，相關 **307 項測試通過**；
測試包含 canonical 完整冷還原及拒絕未知／未完成／仍被使用的來源。
[資格測試](../artifacts/operations/agent-workflow/runs/filesystem-standalone-benchmark--20261023T194520-eba5843c/run.json)。
初次誤用不含 NumPy 的 minimal control 角色導致收集失敗，保留原 log；
已由原資料環境的成功 run 取代，不修改正式控制環境。

| 固定範圍 | 當前結果 | 本輪已計 WSL allocated 回收 |
| --- | --- | ---: |
| research-stage-copy-20261021-v96 | 原 owner 完整 D 還原、精確退役及暫存 prune 通過 | 2,674,765,824 bytes |
| research-stage-copy-20261021-v94 | 原 owner 完整 D 還原、精確退役及暫存 prune 通過 | 2,674,765,824 bytes |

目前新增 **5,349,531,648 bytes（5.35 GB）**；這是最後連結釋放的原有 source 副本 blocks，
不把本次新建後刪除的 SSD 編碼暫存再加一次。原 provider 資料、唯一冷物件與 NAS 歷史
刪除數仍為 0；原研究 input 與小型完成證據保留。此處更新不把先前 47.48 GB 再算一次。
[SSD scratch case 的原 acceptance](../artifacts/operations/filesystem-research-stage-v96-retirement-20261024/legacy-wsl-offline-dbc3c940dd4553c699f57689/acceptance.json)、
[D scratch case 的原 acceptance](../artifacts/operations/filesystem-research-stage-v94-retirement-20261024/legacy-wsl-offline-74d09816b6c386e0c36c7dfb/acceptance.json)。

**04:30 台北獨立驗收完成：**兩組結束後，另一程序逐一從固定 D manifest 完整解碼，
核對全部 6 個原始成員的 SHA／長度、保留的完成收據、精確 source 名稱消失、
encoding stage 已移除，以及未刪除的原研究 input 完整 SHA。
[獨立還原與 allocated 彙總](../artifacts/operations/filesystem-research-stage-retirement-readback-20261024.json)、
[成功驗證 run](../artifacts/operations/agent-workflow/runs/filesystem-research-stage-indepe-20261023T202319-6df3aa80/run.json)。
當時 WSL available 為 **46,469,632,000 bytes（46.47 GB）**，隨其他工作變動；
D available 為 571,015,766,016 bytes。這不是前後磁碟淨差，也沒有縮小 Windows VHDX。

本次相應測試共 **356 passed（49 + 307）**，另有隔離的 132 個版本實際還原。
兩個真實副本的 private preparation 可並行，正式 publication／retirement 繼續由原鎖
序列化；不同 scratch／冷庫快取與排隊條件不是受控 ABBA，不據此宣稱全鏈路加速倍數。
v29 仍沿原有界 handoff 等待健康 owner 的安全邊界，候選尚未實載；FinLab 等原生整表
修訂的逐期分區，以及 NAS 全歷史驗收，仍是另外的未完成範圍。

04:36 的補充讀回：原 owner 正在 `finmind-free` 的 `censusing_source`，已掃描
3,388,783 檔／102,775,367,230 logical bytes，總數仍未知，不能換算完成百分比或 ETA。
這是進行中的例行輪（30／60 已驗），不覆寫原固定基線 60／60。
正式 worker 仍載入 `d98f04f2…90d3`，checkout 仍為已資格化的 `d87a7d8d…12416d`；
原 24 小時 bounded handoff 與完成後驗證仍由既有監督程序等待。
此時 WSL available 為 45,710,598,144 bytes（45.71 GB），背景寫入會改變餘額；
本輪的 5,349,531,648 allocated 回收量由精確 inode 證據決定，不受此波動改寫。

## 2026-10-24 本次增量版本與原生 sharing 驗收

重新讀回 **33／33 個資料根的共享或 host-private v2 head**；完整邏輯清單
搭配共用 immutable files／8 MiB SHA-256 區塊。這次亦在隔離 fixture 實際完成
33 根 × 新增／append／修訂／刪除的 **132 個固定版本還原**，並核對舊版本、
原值／負值／null／前導零、Polars／DuckDB 相容及相同輸入不新增 payload。
此 fixture 不讀寫正式來源、不移動 head，不代表本輪重新驗了全庫 payload。
[當前全部根](../artifacts/operations/filesystem-current-all-root-readback-20261024.json)、
[132 版實際還原](../artifacts/operations/filesystem-incremental-roundtrip-current-20261024.json)。

原 `scripts/deduplicate_artifacts.py --finlab-history-root` 現在要求 apply 指定
完成 audit 的 64 字元 fingerprint；等待時間限為有限的 0–3600 秒。仍使用原同步鎖，
忙碌回傳可重試的 exit 75；不刪鎖、不打斷下載。詳細歷史診斷留在原 durable JSON，
stdout 只輸出有界摘要。曾試作的重複 module CLI 已撤回，沒有留下第二個入口。
共 **49 項原生分享／共用去重測試通過**，包含原值、收據、busy owner、固定計畫與
大量舊收據診斷。
[正式入口測試](../artifacts/operations/agent-workflow/runs/filesystem-canonical-sharing-fin-20261023T193504-5083d870/run.json)。

| 原 owner 的 fresh audit | 範圍 | 候選／本次回收 |
| --- | --- | --- |
| FinLab history | 17,650 收據、17,636 payload paths；75,963,224,064 allocated bytes | 0／0 |
| 原生分鐘 session cache | 16 個 supported generations；80,883,310,592 allocated bytes | 0／0 |
| 已完成 forward cache | 11,831,656,448 allocated bytes | 0／0 |

三者都是 `sharing_candidates_only` 類型的範圍：只對重複候選做完整 SHA；沒有候選就
不為 audit 重讀所有 payload。表內 bytes 是各自觀察，不相加成全磁碟可回收量。
[FinLab](../artifacts/operations/filesystem-canonical-finlab-audit-20261024/20261023T193210.423113Z-3f1e91d1.result.json)、
[分鐘](../artifacts/operations/filesystem-current-session-sharing-20261024/physical-minute/tw_minute_paper_parity_09bedd96a2f68c39/20261023T193208.452097Z-3ac640e7.result.json)、
[forward](../artifacts/operations/filesystem-current-forward-sharing-20261024/completed-forward/completed_live_inference/20261023T193406.012618Z-f9d6016e.result.json)。

可重跑的原入口（報告目錄應依本次作業另定）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/deduplicate_artifacts.py \
  --finlab-history-root data_finlab \
  --report-dir artifacts/operations/finlab-sharing-reviewed \
  --finlab-wait-owner-seconds 3600

# 僅 audit 有候選且固定計畫仍相符時套用；原始與歷史邏輯名稱保留。
run_fintech_python scripts/deduplicate_artifacts.py \
  --finlab-history-root data_finlab \
  --report-dir artifacts/operations/finlab-sharing-reviewed \
  --expected-plan-fingerprint AUDIT_PLAN_SHA256 --apply
```

這些結果沒有把 FinLab 不同內容的整表歷史改稱相同內容，亦沒有宣告所有原生來源
都已逐期分區。候選 v29 身分仍保持原 admission；正式載入與 NAS 全歷史另驗。

## 2026-10-24 03:45 回收接續方案（實作前）

FinLab、原生分鐘及已完成推論的三次正式 sharing audit 均為 0 候選，
不能靠相同內容共用再釋放空間。benchmark metadata census 為
285,362,438,144 allocated bytes，這只是占用量，並非可刪除量。

已定位 `research-stage-copy-20261021-v94`／`v96` 兩個獨立 producer output。
原 `scripts/benchmark_research_stage_copy.py` 在四次 A/B/B/A、原件與目標完整 SHA、
程式身分穩定後才寫 `receipt.json`，但原回收器只辨識 `fixture.json` +
`abba-receipt.json`，且只接受 benchmark suite 下的一個 child。因此本次：

1. 在原 `completed_benchmark_evidence` 加入這個既有、明確的 producer ABI，
   嚴格核對完成狀態、來源大小／SHA、程式 SHA、四次順序及無 provider／production writes。
   不把任意 `accepted` 字串當完成，亦不補寫舊 benchmark 收據。
2. 只擴充該具名 standalone case 的路徑辨識；仍拒絕 benchmark namespace、整個 suite
   及未知格式。回收仍沿原手動 inventory／owner／D scratch／完整 SHA 還原流程。
3. 通過語意與生命週期測試後，固定這兩個 case 的 inventory；當前 consumer／FD／mmap、
   十二小時穩定、pins／holds／transport 及 source fence 全部通過才 apply。
4. 保留小型原始完成收據及冷版本，獨立讀回精確名稱與最後 inode 的 `st_blocks`。
   檢查失敗就保留原件，不能把本方案的預估 5.35 GB 當成已回收。

接續的排程修正仍使用同一 owner：原讀取 PID 1047498 結束後，改採 canonical
fresh consumer gates；無關的效能報告整體 unit 尚 active 不能繼續算作來源 lease。
只暫停、核對並終止己方無子程序／無 publication lock 的等待 child，保留原 admission，
未停止正式或其他工作的服務。
[等待範圍修正](../artifacts/operations/filesystem-research-stage-wait-scope-amendment-20261024.json)。

第一個 D scratch case 的 prepare 階段已觀察到 192.7 秒，尚在第二個壓縮成員，
不是完整工作流速度。第二組 2,674,747,944 logical bytes 可放入原 C 暫存，
仍保留原 source-size + 32 GiB admission；另建 fresh v96 inventory，舊 D inventory
保留且不套用。SSD 與 HDD 的 private preparation 可並行，publication／retirement
繼續由原共用鎖序列化。此調整不降低容量門檻，不將不同冷庫快取狀態的兩組結果當 ABBA。

## 2026-10-24 03:20 接續計畫（實作前）

這次沿原 `all-dataset-incremental-layout-20261016` 任務接續，不重建另一個資料庫或
重做已完成的 60 項固定基線。現有底層是 WSL ext4 與 guarded D NTFS／9p；
增量由共同版本層提供：完整邏輯清單，共用相同檔案／8 MiB 內容區塊。
每次的永久新增成本應是清單加獨有內容，工作暫存及來源整表修訂另外計算。

本次執行順序：

1. 唯讀核對全部根、正式 owner、原安全換版、容量與回收 dry run。03:16 的 WSL
   available 為 45,358,510,080 bytes；這是容量觀察，不是本輪回收量。
2. 重用 `stockagent.data.finlab_storage.compact_history`，強化原
   `scripts/deduplicate_artifacts.py --finlab-history-root` 的 audit／apply 入口，
   不新增重疊命令。apply 必須指定 audit 的 fingerprint；維持原 `.sync.lock`、逐檔完整 SHA、
   收據不變、FD／mmap、全部 hardlink 別名、唯讀 sharing 與刪後 allocated 讀回。
   不重新下載，不刪邏輯版本，不把不同內容的歷史表格誤判為重複。
3. 先由原 agent workflow 執行 audit；有可回收候選才依同一固定計畫 apply，並獨立
   核對實際回收結果。同步 owner 忙碌就有界等待，不中斷下載或刪鎖。
4. 再讀回全部增量 head、原 catalog 換版與空間證據。33 根版本層、原生來源分區、
   固定 D 還原及 NAS 當前覆蓋分開報告，效能只報實測範圍。

03:17 的 managed cache dry run 為 0 would-evict（4 pinned、1 active lease）；
原 transport GC 為 `no_accepted_transport_candidates`。這些結果不授權另掃目錄刪檔。
既有 v29 換版／驗證命令繼續等待原 owner，這次不修改其已資格化的程式身分。

## 2026-10-24 續修：固定增量架構，減少全庫 metadata 成本

本輪先保存[執行前方案與舊實作身分](../artifacts/operations/filesystem-stat-plan-20261024.json)，
再修改原 catalog 與 layout 入口；沿用 ext4／NTFS、既有 publisher／Temporal／回收 owner，
不另建全量版本目錄或排程。永久版本仍是完整邏輯清單，共用未變檔案與 8 MiB SHA-256
區塊；新增／更正只新增獨有內容，舊版本保留。來源原件、服務視圖與可重建暫存分層處理。

### 已測到的瓶頸與修正

固定 D catalog 的 3,454 檔 metadata census，舊版 A/B/B/A 中 A 平均 **67.005 秒**，
新 B 為 **7.509 秒**，此階段 wall 減少 **88.8%**；CPU 平均 7.790 → 2.914 秒。
每次完整路徑集合、型別與 stat signature 相同。原測速入口新增 `--stat-only`，
不複製 payload、不發布 head、不呼叫 provider；保留兩次 A、兩次 B 的完整結果。
[實際固定輸入比較](../artifacts/operations/filesystem-catalog-stat-abba-20261024/acceptance.json)。

原 `_files` 逐檔等待跨 Windows／WSL 的 `safe`／`lstat`；現在最多四個 worker、
64 個待處理名稱，完整檢查後才離開原 publication owner。小清冊仍走串行；例外先收束
worker 再傳出，來源前後完整 fence、獨立 SHA、seal／READY 和精確集合驗證均保留。
這是**完整 metadata 階段的實測**，不是完整 catalog copy／seal／restore／NAS
加速或硬體上限證明。之前全工作流中斷的比較結果仍保留，不用本次結果替代。

### 清冊現在區分共享與本機私有 head

舊 layout 只查共享 `heads/`，把已有本機私有版本的資料誤顯示為 pending。
現在 `publish:false` 分支沿既有 `private_history_root(..., initialize=False)` 讀回，
核對原 authority、0700 權限、marker 與 Syncthing 排除；不建立私有庫、不賦予同步權限。
本輪已觀測 **33／33 個根的 v2 head**，其中 20 根有共享 head、16 根有私有 head，
兩類有重疊，不能相加當分母。輸出新增 `shared_and_host_private_heads_v1` 觀測契約，
逐分支標出 `storage_scope`；payload 本輪未重新讀取，`objects_verified` 仍為 false。
[當前全部根讀回](../artifacts/operations/filesystem-private-layout-readback-20261024.json)。
這仍不等於 FinLab 所有原生整表修訂都已轉成逐期分區。

### 共同回歸與正式服務交接

固定候選 `d87a7d8d…12416d` 已在原 locked lakehouse role 通過 **1,039 passed／9 skipped**，
並以該環境的真實 rclone 驗證相關整合；另於 FinLab 原本資料環境通過 **14 passed**。
skip 保留原環境條件，並非 NAS 全歷史驗收。
[共同資格](../artifacts/operations/filesystem-v29-qualification-r2-20261024/result.json)及
[FinLab 原生回歸](../artifacts/operations/agent-workflow/runs/filesystem-finlab-native-regress-20261023T185132-3a9cc968/run.json)。
第一次把 FinLab 測試放入不含 NumPy 的控制角色時，測試收集被拒絕；保留原失敗，
改依既有角色分工驗證，未往正式控制環境安裝資料套件。

三條現有 persistent workflow 的完整 event history 已以候選重播成功，沒有執行 activity、
新增或 reset workflow。[候選 replay](../artifacts/operations/filesystem-v29-candidate-history-20261024/acceptance.json)。
新版沿原 `rollout_lakehouse_source_policy --reload-code-only` 等待既有 owner 邊界，
不保留部分取得的其他 owner 鎖；不改私人設定、PG、Temporal 或資料範圍。
[交接 admission](../artifacts/operations/filesystem-v29-rollout-admission-20261024.json)
只證明已具備換版條件；正式載入仍以新的 worker-ready 與換版後 replay 為準。
03:04 台北的原 owner 仍在 FinMind 首次 mutable-control census，已檢查 2,410,496 個
檔名；這不是停止進展。只將本輪兩個 agent 等待命令重排為最長 24 小時的有界等待，
正式 PID 439130、原鎖與 journal 均保留，沒有中斷此來源交易。
等待上限不是完成 ETA；候選改變會拒絕載入，換版接受後才自動執行三鏈 replay 與
33 根 metadata 讀回。[等待範圍修訂](../artifacts/operations/filesystem-v29-handoff-wait-amendment-20261024.json)。
02:59 的另一次[固定基線讀回](../artifacts/operations/filesystem-v29-baseline-readback-20261024.json)
仍核對 60／60 原 manifest／完整還原收據、3 組期貨跨根依賴及已退休精確名稱；
這次未重新讀取全庫 source payload，不將舊完整還原冒稱現在重新還原。
03:07 的[本輪整合讀回](../artifacts/operations/filesystem-incremental-current-readback-20261024.json)
綁定全部當前證據 SHA；正式仍載 v28、`candidate_loaded=false`，兩個有界交接／驗證
run 均由原 agent workflow 監督。FinMind 檔名檢查已到 2,790,400；WSL available
46,134,870,016 bytes，D available 584,666,423,296 bytes。容量是當時觀察值，
不能拿與前值的差額替代精確回收收據。
後續最後計數核對另將「1 份期貨還原收據」與其中「3 個跨根連結」分開，
修正本輪私人換版後 verifier 的收據數假設；正式資料與原收據均不改。
只重啟己方等待驗證命令，原 code-only handoff 仍保持排隊。
[當前受監督交接讀回](../artifacts/operations/filesystem-v29-supervised-handoff-current-20261024.json)
列出最新兩個 run；03:07 舊快照中的驗證等待命令由此更新，原快照保留。

### 本輪空間核對

既有 managed cache dry run 為 0 would-evict；home 與固定 tmp compiler audit 都是
0 eligible，保留 pins／leases。FinLab 原生重複內容分享的本輪 audit 因原 `.sync.lock`
持有中而 defer，沒有搶鎖或停止來源。原 transport GC 已完成一批 NAS 驗收過的
catalog 暫存退休，獨立讀回確認 **3,464 個精確名稱／346,172,605 logical bytes**
已移除，3,462 個 primary source signature 仍相符，配對 ACK 不變。
原 journal 的 `reclaimed_bytes` 實為 logical size，沒有刪除前 `st_blocks`；
本輪因此明列 allocated reclaimed 為 unknown，不把這個數字冒稱實際磁碟釋放量。
[獨立清理讀回](../artifacts/operations/filesystem-transport-retirement-readback-20261024.json)。
本輪停止自己耗時的全工作區 `du` 並保留[未完成清冊紀錄](../artifacts/operations/filesystem-root-census-stop-20261024.json)，
不把局部列或全磁碟 free 的變化當成精確回收量。
先前 **47,481,356,288 bytes** 回收是已驗歷史成果，不算本輪新增；來源、唯一冷物件、
NAS 歷史保留，Windows VHDX 沒有因此縮檔。原有 GC timer 繼續處理其餘各自已驗收批次；
此固定一批的讀回不能相加成全歷史回收或 NAS 完成率。

## 設計與實作順序

版本的永久成本分為「完整清單」與「尚未保存的獨有內容」，暫存空間另計。
每版清單列出所有必要內容；相同內容共用，修訂只新增變動的檔案／區塊。
這樣能直接還原任一固定版本，不需要保留另一份完整實體目錄或逐版重播。

1. **先清點責任。** 分開來源原件、不可變版本、使用中的訓練／服務視圖與可重建暫存。
   本次重新核對 33 個實際 `data_*` 根、登錄與各自 README，沒有遺漏。
   [當前根目錄與掛載清冊](../artifacts/operations/filesystem-v28-current-root-coverage-20261024.json)。
2. **沿用底層磁碟，修改共同版本層。** WSL 工作區為 ext4，D authority 經 DrvFs／9p
   掛載；不格式化、不建立另一套全庫鏡像。共同版本使用 SHA-256 內容位址與完整清單；
   大檔變動使用既有 8 MiB 區塊。DuckLake 已提交的唯讀版本可共用相同檔案，
   mutable producer 仍獨立。來源值、單位、時間、舊版本與既有 reader 保留。
3. **降低必要工作以外的成本。** 相同版本不再產生一份 payload；catalog 只複製新增／
   更正內容。完整中斷編碼可補 metadata 續作，不為了缺少 manifest 再壓縮或搬一遍。
   新 capture 先檢查容量；已完成的 frozen capture 可接續原驗收，避免容量互鎖。
4. **證明可恢復後才回收。** frozen inventory → 固定 D 版本 → 全 SHA／原值還原 →
   當前 process／FD／pin／lease／transport → quarantine → 精確 unlink → 刪後讀回。
   只計最後連結實際釋放的 `st_blocks`；來源原件、唯一冷內容及 NAS 歷史不在清除範圍。

33 根共用增量版本政策與固定 60 項 D 遷移是不同範圍；FinLab 原生不同整表修訂仍保留，
不能把共用版本完成解讀為所有原生來源都已逐列分區化。完整 D catalog ABBA 尚未完成，
也不能由單組空間減少推論完整流程更快或已達硬體上限。

## 2026-10-24 02:02：增量版本與本階段空間回收已驗收

三批選定離線工作檔、四份已驗比較副本、七個空索引及既有 C 編碼暫存均已完成
精確退役與獨立讀回。本階段釋放的 **WSL 原有檔案配置為 47,481,356,288 bytes
（47.48 GB／44.22 GiB）**，依最後連結的 `st_blocks` 計算。

| 範圍 | 已釋放的原有 allocated bytes |
| --- | ---: |
| 三批離線工作檔及四份比較副本 | 19,310,825,472 |
| 七個無 committed rows 的 producer 索引 | 18,321,412,096 |
| 已驗收內容的既有 C 編碼暫存 | 9,849,118,720 |
| 合計 | **47,481,356,288** |

這是 WSL 精確名稱的原有配置回收，**不是所有磁碟的淨減少，也不是 Windows VHDX
實體縮檔**：必要獨有內容已保存到 D 固定版本，不能再刪掉其唯一冷物件。
本輪新建後釋放的 6,536,327,168 bytes 中間編碼另計，未重複加入上表。
02:02 讀回 WSL available 為 48,383,340,544 bytes，會隨其他工作改變。
來源 provider 原件、D authority 物件與 NAS 歷史刪除數均為 0。
[最終彙總與證據 SHA](../artifacts/operations/filesystem-v28-completion-20261024.json)、
[精確名稱／原值還原的獨立讀回](../artifacts/operations/filesystem-v28-reuse-C-final-readback-20261024.json)。

### 減少重複工作並修復中斷接續

最後一批已有 424 份完整編碼、逐檔 receipts 與原 `source_plan`，只缺 manifest。
新增 `recover_complete_archive_manifest`，由原 capture owner 核對完整集合、
每份 encoded／decoded SHA、當前 source SHA、fresh consumers 及前後 source／stage
fences，只補原格式 manifest。舊觀察、編碼與 receipts 不改寫；實際重用
9,844,722,714 bytes 編碼，重新編碼 payload 為 0。
缺少 payload 仍走原 preparation 與完整容量預算，損壞或未知內容留存稽核。
先前 control-rebase 候選沒有套用；實際路徑保存原 controls。

接續保留原 frozen inventory，只在另存的新計畫移除 staging profile，原來源
items／fingerprint／dataset 身分完全相同。補 manifest 後，由原 publisher 發布固定
D 版本，再獨立重建全部 424 檔、9,860,287,927 原值 bytes，最後退役原目錄與 C stage。
[完成接續](../artifacts/operations/filesystem-v28-quality-reuse-C-20261024/progress.json)。

接續最後的 prune 曾被容量查詢阻擋。排程繼承的 `/run/WSL/1_interop` socket 雖存在，
其 Windows process context 無法啟動；不是磁碟已滿或資料錯誤。`physical_free_bytes`
現改用原 native I/O 的有界、已實測 bridge，僅把選定環境傳給固定 registry／volume
查詢，不改全域環境、不猜 distro、不以虛擬容量代替。沿同一 authority alias 實測取得
322,748,739,584 bytes 真實 backing-drive free，原退役續接隨後完成；先前三次失敗
證據仍保留。此修正已在 canonical CLI 實際生效，沒有冒稱已熱載入既有 v28 daemon。
[容量修正資格與實測](../artifacts/operations/filesystem-v28-quality-reuse-C-20261024/capacity-bridge-qualification.json)。

### 驗證與尚未證明的範圍

- 原 v28 共用儲存資格為 **998 passed／8 skipped**，已實載，三鏈 replay 與固定
  60／60 基線保留；這是當時固定程式身分的證據。
- 完整編碼恢復與相關退役六模組為 **357 passed**，包含缺件、損壞、未知成員、
  device 映射、當前來源變動及引用等拒絕案例。
  [回歸結果](../artifacts/operations/agent-workflow/runs/filesystem-complete-stage-final-20261023T171040-5557409f/run.json)。
- 容量橋接與相鄰 recovery／return／cache 行為為 **156 passed／1 skipped**；skip
  為未提供真實 Restic binary，不是 NAS Restic 驗收。實際 C stage prune 與獨立刪後
  讀回另行通過，與單元測試分開記錄。
- 33 根均已登錄共用增量版本並有 README；FinLab 不同整表修訂仍保留，未全改為逐列
  分區。完整 catalog ABBA 因重開中斷，單組省空間但較慢，尚未證明整條鏈加速或僅剩
  硬體限制。來源下載完成、當前追新及 NAS 全歷史驗收仍是各自的證據範圍。

下方保留較早各階段的觀測與規劃；其「尚未回收」和「執行中」由本節當前結果更新。

## 2026-10-24 00:38 現況與修正

沿用 ext4／NTFS 和原 publication／retirement owners，不格式化或複製整套資料庫。
33 個登錄資料根採完整邏輯清單、共用不可變內容；新版本只新增變動內容。
DuckLake catalog 首版獨立複製並封為唯讀，後續從已提交版本共用相同檔案；
mutable producer 不分享 inode。固定版本可獨立還原，不需要逐版重播。
FinLab 不同內容的整表歷史仍保留，不能把共同版本政策說成所有來源都已逐列增量化。

本階段已完成的原有配置回收（按精確 `st_blocks`，不以全磁碟 free 差額計算）：

| 類別 | 已回收 allocated bytes |
| --- | ---: |
| 先前已驗收的離線工作檔與四份比較副本 | 7,926,525,952 |
| 本次明確指定的七個空 metadata 索引 | 18,321,412,096 |
| 合計 | **26,247,938,048** |

七個索引皆為完整 SQLite producer schema、零筆 committed rows、僅 8,192 bytes
committed pages；尾端為中斷後的未提交頁。新增明確 `--empty-index` 計畫模式，
逐檔保留整個檔案 SHA、當前 FD／inode 引用、quarantine 和刪後名稱檢查。
沒有依大小或檔名前綴刪除；有資料或格式／引用不明的索引仍受保護。
[七個精確退役收據](../artifacts/operations/filesystem-v28-empty-index-retirement-20261023/progress.json)。
23:59 新聞 owner 已自動從 `waiting_disk` 恢復 `running`，25 GiB admission
恢復通過；00:09 WSL available 為 30,527,610,880 bytes，會隨既有工作持續變化。
這是 WSL 內可用空間，不代表 Windows VHDX 已實體縮檔。

完整共用儲存回歸為 **998 passed／8 skipped**。其中一次舊 benchmark 失敗是固定
sleep 後 persisted retry deadline 尚未到達；改為等待真實記錄期限，保留 monotonic
超時和實際 retry／完整還原。時鐘後退與期限無法抵達兩種測試已納入。
[完整回歸](../artifacts/operations/agent-workflow/runs/filesystem-v28-qualified-final-20261023T160417-a3f6a417/run.json)。
v28 於 00:07:30 沿原 owner 實載 `d98f04f2…9b90d3`，之後三鏈完整 replay 通過；
固定 60／60 基線、工作鏈和來源／冷庫仍保留。
[實載及 replay 讀回](../artifacts/operations/migration-v28-post-rollout-readback-20261023.json)。
00:41 再讀正式 `worker-ready.json`，載入身分仍為上述 v28；此後工作目錄中的
`downloader/download_tw_public_data.py` 已另有修改，因此當前 checkout 整體身分
與 v28 不同。保留該次其他工作，不把較新來源修改冒稱已隨儲存 worker 部署，
也不為了對齊雜湊覆蓋其內容。儲存核心的本次資格與實載證據仍指向固定 v28。

兩批另計 11,384,299,520 allocated bytes 的舊工作檔仍未回收。開機後的清冊差異
已查明為整樹 device 2096 → 2112；第一批 407 個觀察列，其餘 inode／size／mtime／
ctime／mode／link 全相同、沒有新增刪除。canonical admission 新增只接受這種
單一完整映射的模式，另外保留原 frozen inventory 與完整 current observation；
原逐檔 source／encoded SHA、D 解碼還原和刪除前各 gate 不變。
16 項針對性測試及完整五模組 **336 項回歸**已通過。
[完整清理回歸](../artifacts/operations/agent-workflow/runs/filesystem-reboot-retire-tests-20261023T160843-e2053c24/run.json)。
續接時，另一個既有全工作流效能清冊程序把 `artifacts/operations` 列為讀取範圍，
原 consumer gate 因此保留兩批來源。六次短重試用盡的原結果保留；接續已改為
等待該次實際 PID／start ticks 離開，再沿同份 frozen inventories、原 receipts 與
canonical owner 重新驗證。此等待不持有共同 publication owner，也不改變原量測輸入。
[有界排隊狀態](../artifacts/operations/filesystem-v28-exact-offline-v5-20261024/progress.json)。
未通過的範圍不得先計為回收。舊 C 編碼暫存約 9.85 GB 也仍待獨立證明；
原前置未完成的 watcher 已以 75 結束，後續只接到這次續接，不重送已完成的刪除。
後續控制修復的純規劃器已用該批 424 個舊控制紀錄完成 **19 項檢查**：
接受完全相同或全批單一 device 映射；拒絕部分映射、不同 encoded／original SHA、
codec、path、inode、size、time、mode 和缺／多控制紀錄。測試的 2112 目標是合成
輸入，**不是尚未完成的 D 版本證據**。
[規劃器檢查](../artifacts/operations/filesystem-v28-control-rebase-tests-20261024.json)。
實際處理只在兩批原退役及獨立讀回成功後開始：重建固定 D 的原 manifest，核對
其所有編碼身分；如果只有上述 device 差異，仍先核對原 frozen census、完整讀取
C 的 encoding／original SHA、fresh consumer／pin／generation，持有原 retirement
lock 並持久保存舊控制 bytes，再把 disposable controls 接到這次真實 cold capture。
原觀察另存保留；payload 不變，原 canonical plan／完整還原／quarantine／unlink
仍逐項執行。其他差異全部保留，不能以換 metadata 讓驗證通過。
[舊 C 暫存接續狀態](../artifacts/operations/filesystem-v28-old-quality-encoding-v5-20261024/progress.json)。

Syncthing 曾在 idle 後數毫秒立刻開始下一輪 scan，外層每五秒等 idle 會漏掉這個
邊界。已結束這個冗餘外層等待，並讓原 finally 精確恢復持久 16 GB index reserve，
完整 configuration 與調整前相同。續接直接使用 canonical owner 內持鎖等待與 fresh
convergence gates；沒有把 scanning／need=0 當完成，也沒有降低 transport gate。

原完整 D catalog ABBA 被 20:44 的 WSL 重開中斷，現已 reconcile 為 interrupted，
保留前兩組完整樣本及其餘工作檔。單組 candidate 物理配置少 49.8%，但 wall 多
7.6%；另外兩組和兩次 native restore 沒完成，不能宣稱完整流程加速或硬體上限。

## 23:40 開機後接手與容量解鎖計畫（執行前）

目前 boot ID 已改變，WSL uptime 約三小時。原完整 catalog ABBA 與 scratch
續接程序已不存在，canonical `run-status` 回報 interrupted；保留已完成兩組
量測與所有未驗收副本，不把舊 `running` JSON 當成仍在執行。正式 lifecycle
service 已自動啟動，worker-ready 仍載入 v27 `5dbda2df…10eec7`。

剩餘小型 offline case 上次在獨立 D 全原值重建後，被當時 PID 276998 對
`artifacts/operations` 的 parent-directory 讀取阻擋；不是內容遺失。此程序於本次
開機不存在，仍須重新通過全部 fresh gates。另一份 quality case 未開始，不能
算入已回收量。WSL available 約 12.5 GB，低於持久索引 reserve 16 GB，Syncthing
因此拒絕掃描，形成「先收斂才能退役、先退役才能取得運轉空間」的容量互鎖。

本次只做有界的一次性解鎖：量測實際 index，確認 8 GB 至少為其六倍、available
仍高於 9 GB，保留完整 private before configuration；透過原 local REST 暫改
`minHomeDiskFree` 為 8 GB。沿兩份原 frozen inventory／原 receipts，以同一 canonical
retirement owner 逐一接續，仍要求完整 D 原值重建、當前 consumer／pin／lease／
transport、quarantine 和逐檔 unlink。僅 retry 75 的保留結果，不降低任何刪除 gate。
完成或失敗都恢復持久的 16 GB，且只在該欄仍為本次值時恢復，避免覆蓋別人的更動。
所有其他 configuration 必須保持相同；不清 index、不重啟 Syncthing、不停止來源。
獨立刪後讀回在恢復持久配置後執行。若容量／內容／引用條件不滿足，保留來源和原因。

## 18:56 已完成與接續範圍

增量 catalog 與 capture 容量修正已於 18:15 實載 v27，原固定 60／60 還原基線
與三條 workflow chain 保留。33 個資料根沿完整邏輯清冊、共用 immutable payload；
新 catalog 首次建立唯讀 seed，後續已提交版本才能捐出相同檔案。完整 D catalog
八版本速度比較及兩次 native restore 仍執行中，不能當作已通過的效能證據。

本階段已實際退役的原有副本：

| 範圍 | 檔名數 | 已回收 allocated bytes |
| --- | ---: | ---: |
| `architecture-tech-trials-20261003` 已封閉離線資料 | 440 | 6,515,277,824 |
| 已驗收的四份 v25 SSD 比較副本 | 13,840 | 1,411,248,128 |
| 合計 | 14,280 | **7,926,525,952** |

前者已完成固定 D 全部原值重建，再沿原 consumer／transport／quarantine gate 退役。
其另有 5,345,894,400 bytes 的本輪封裝暫存已回收；那是本輪產生的中間資料，
**不再加到上述原有資料的淨回收量**。固定來源、D 冷物件、NAS 歷史均保留。
證據：[原退役進度](../artifacts/operations/filesystem-v26-offline-eligible-20261023/receipts/progress.json)、
[四份比較副本](../artifacts/operations/migration-v25-replication-abba-20261023/probe-retirement.json)。
[獨立刪後讀回](../artifacts/operations/filesystem-v27-retirement-readback-1-20261023.json)
再次核對上述 7,926,525,952 bytes 的精確名稱不存在、canonical cold-only state、
空 quarantine、原完整 SHA／decode 收據，以及當前 D manifest／物件簽章一致。
這次是原完整還原證據的獨立綁定複核，沒有冒稱重新讀取所有 payload。

18:46:49 已完整還原 Syncthing 原 `minHomeDiskFree=1%`；配置 SHA 與調整前一致，
當時 folder idle、need bytes／items／deletes 及各類錯誤皆為零。
[還原證據](../artifacts/operations/filesystem-v27-index-capacity-recovery-v3-20261023/restored.json)。
WSL 可用空間會被其他工作改變，18:55 為 26.26 GB；這不是回收量的計算方式。

新 D offline staging profile 已通過 17 項針對性測試。第一次完整五模組回歸
319 passed／1 failed：失敗測試仍在舊 encoded SHA 函式注入變動，而實作已用
合併 SHA／decode 讀取，變動未實際發生。測試改在真正的 decode 邊界注入，
並斷言確實改寫一次；修正後該項通過，完整回歸另續跑，保留原失敗證據。
剩餘兩份明確 offline case 約 11.38 GB allocated，改用固定 D 暫存；
不降低 source-size 加 32 GiB reserve，不改寫原已凍結的 C 清冊或既有來源範圍。

固定實際 D 的 17 個 pins 四次 ABBA，所有命中 JSON 集合與 SHA 一致；
原查找平均 4.5403 秒，新查找 3.4660 秒，wall 減少約 23.7%，CPU 減少約 19.6%。
這只涵蓋 pin manifest 選取／完整 JSON 讀取，不能外推到整個回收或 NAS。
[完整查找比較](../artifacts/operations/filesystem-v27-pin-lookup-abba-20261023/acceptance.json)。

下方按時間保留規劃、失敗與階段觀測；目前是否實載及已回收量以上述收據為準。

### 19:08 索引容量政策的後續修正計畫（執行前）

18:46 恢復 `1%` 後，來源服務持續寫入，WSL available 再降至約 23.2 GB。
唯讀兩秒取樣看到 FinMind quota snapshot、registered intraday、news fulltext 等
既有服務寫入；該取樣不能推定每項都是永久新增資料或完整排除重複保存。
索引約 1.16 GB，`1%` 卻取 WSL 虛擬磁碟 2.43 TB 當分母，與索引所需成長預算
沒有直接比例，會再次阻止原退役流程所要求的同步收斂。

後續改為本機索引的固定 **16 GB** 運轉保留，持久寫入原 Syncthing configuration，
不再在本輪完成後恢復這個造成互鎖的比例。只在實際 free ≥17 GB、索引總量的
八倍仍小於 16 GB、原 private config／local-only REST identity／25 GiB capture
policy 均相符時，以原 REST API 最小 PATCH 一個 options 欄位。完整 before／after
SHA 與私人原配置保留；失敗只回滾自己改的欄位，不覆蓋其他配置。
這是本機、當前索引規模的容量設定，不是通用比例或豁免磁碟保護；資料夾、peer、
憑證、網路、index、25 GiB source admission、32 GiB archive reserve 均不變。
配置讀回與真正 folder 收斂分開驗，沒有服務重啟或代發傳輸完成證據。

19:08:54 已由原 REST API 實作，實測索引為 1,153,170,992 bytes、available
23,059,681,280 bytes；只有 `minHomeDiskFree` 一個欄位改為 16 GB，無需重啟。
[配置完整比對](../artifacts/operations/filesystem-v27-index-reserve-policy-20261023/accepted.json)
通過；19:09:59 folder 自動恢復 idle，need bytes／items／errors 歸零。
第一份 D scratch 的原發布因舊 `1%` 錯誤返回 75，原來源、編碼與 checkpoint 保留；
19:10 起沿同份 frozen inventory／原收據續接，不另建第二份來源副本或假造完成。
19:13 的[完整清理回歸](../artifacts/operations/agent-workflow/runs/filesystem-offline-d-regression-20261023T105420-53dd491d/run.json)
為 **320 passed**。19:15 再查正式 worker-ready 與 checkout 的 50 檔 identity，
仍同為 `5dbda2df…10eec7`；這次 cleanup／pin 查找修正沒有冒充另一輪 worker 實載。

19:15 完成第一組完整 candidate 比較：兩個固定版本皆通過 exact set／全 SHA，
第二版的 3,454 檔／343,842,521 bytes 全部共用，新增 data payload 寫入為零。
兩版 unique allocation 為 354,033,664 bytes，對照第一組 baseline 705,503,232 bytes，
減少約 **49.8%**。但單組 wall 為 3,304.21 秒，baseline 3,071.79 秒，
反而多約 **7.6%**；這個負面結果保留，不能把少寫 bytes 宣稱為整體加速。
另外兩組及雙 native restore 尚未完成，尚不是完整 ABBA 結論。
取樣仍落在完整 seal／verify 的 `inventory()` metadata stat；後續優化應量測
這個讀回路徑，不能省掉原完整性／還原證據。
[candidate 原始量測](../artifacts/operations/filesystem-v26-catalog-abba-20261023/trial-1.json)。

## 17:00 起的改造計畫（實作前）

目標是「每個版本可獨立還原，但未變內容只占一份實體儲存」，不是替資料加上另一個
完整副本的版本名稱。沿用 ext4／NTFS、既有 packed v2、原 publication owner、v1
delivery／NAS restore ABI；這輪補 DuckLake catalog 匯出的整樹複製缺口。

1. 先核對原 task、容量、固定基線及當前 writer。16:58 WSL available 約 81.96 GB、
   D 約 613.97 GB；容量是當下觀測，其他下載仍會改變它。先讀已發布 catalog 的固定
   清冊估算重複內容；清冊估算不是已回收量，也不授權刪除歷史。
2. 原 `export()` 的 `shutil.copytree(data)` 改為完整邏輯目錄、增量實體資料。
   第一輪從可變來源獨立複製，持久化並封為唯讀；後續只從原 journal 登錄的上一個
   已提交版本，借用 SHA／大小一致、同磁碟、唯讀的 immutable 檔案。新增、修訂、
   不可分享的檔案另複製；來源永不直接 hardlink。原始 PG same-MVCC dump、table
   digests、manifest、READY、獨立還原及 NAS ACK 保留。來源與 donor 的完整 SHA、
   變動／symlink／型別檢查不能用 mtime 快取取代；理論上減少的是重複寫入。
3. 先驗來源變動、受損 donor、唯讀條件、hardlink 不支援時的 fallback、版本刪除／
   更正與中斷語義，再在實際 D 驗證完整舊版／新版讀回及寫入量。worker identity
   納入變更，重新回歸及 persistent-history replay，沿原安全邊界交接；等待中的
   舊 engineering rollout 明確 supersede，保留正式健康 writer 與所有既有證據。
4. 清理仍沿原 retirement owner：精確 inventory、固定 D 完整原值重建、process／
   FD／mmap／pin／lease／transport gates、journal／quarantine、逐項 unlink、最後
   `st_blocks` 讀回。先回收本輪已完成的比較副本；可變來源、未驗收工作、D／NAS
   歷史不做廣泛 GC。沒有通過 gate 的空間只列候選，不寫成已清理。

NAS 固定程式沿用相同 delivery bytes／logical paths；本機 inode 分享的節省不能
直接算成 NAS 去重節省。這輪不承諾已達硬體速度上限；完整流程量測與必要校驗成本
分開列出，避免用減少 copy 次數替代整體實測。

### 17:35 階段證據

- 固定 catalog 清冊：456 個已發布版本、608,420 個 data 檔案引用，合計
  37,349,221,962 logical bytes；不同內容 340,473,011 bytes，重複邏輯內容上界
  37,008,748,951 bytes。這次 metadata 查詢不逐一 stat 608,420 個 payload，
  只讀有界 manifest／READY 與原 journal、驗 descriptor identity；不是完整 SHA
  或 inode allocation 證明，**不是已釋放 37 GB**。
- 新增 `catalog_incremental.py`，原 `lakehouse.export()` 使用它；worker identity
  已納入該檔。保留原 v1 完整 manifest、same-MVCC PG dump、逐表 row digest、
  seal、複寫與獨立還原；目前 frozen 候選為
  `22d206bb585627eca5d2f6a5416114daf1dbdafd8b07e25995c53d1ccdcc4eb4`。
- 共用回歸 976 passed／8 skipped；分類依賴 24 passed；三條固定 Temporal
  histories replay 通過。D 槽完整新增測試 20 passed／160.42 秒，覆蓋真實
  hardlink、readonly、獨立 producer inode、修訂／刪除、fallback、來源變動、
  受損 donor、不可覆寫、v1 export／retry 與共用名稱 unlink 後的舊版保存。
- 完整舊 v25 複寫 ABBA 已完成，12 次 copy／retry／缺 READY 恢復與完整讀回
  同義；平均 wall 2,482.338 → 1,971.873 秒，下降約 20.6%。這是固定 D 來源到
  私人 SSD 的複寫流程，不是整個匯出／NAS 的速度。
- 新 v26 固定完整 catalog 的 D 上八次物化 ABBA、兩版本獨立 PG／native
  DuckLake 還原仍執行中；候選只排入 admission，**尚未實載**。程序取樣顯示
  9p metadata RPC 等待；沒有以 HTTP／active、少量 fixture 或 inode 數替代
  完整流程驗證。原正式服務與健康來源工作保留。
- 原完整比較的四個 SSD 副本已進入精確 retirement；另兩個離線 root 清冊約
  8.04 GB，以及品質稽核的獨立 `remote` 子目錄約 9.86 GB，沿原 owner 保存／
  還原／回收。整個品質稽核父目錄因含非一般檔案而保留，不跟隨 symlink 清除。
  17:30 另一個全 `artifacts/operations` 掃描工作開始，後續 fresh consumer gate
  阻止 unlink；來源與失敗證據保留，等讀取者離開後再續原收據。目前這些數字
  仍是候選，不能加入已回收量。

新證據：
[descriptor 清冊](../artifacts/operations/filesystem-incremental-v26-20261023/catalog-copy-inventory.json)、
[v26 回歸](../artifacts/operations/agent-workflow/runs/filesystem-v26-control-regressio-20261023T091616-73bd4981/run.json)、
[D 生命周期測試](../artifacts/operations/agent-workflow/runs/filesystem-v26-d-alias-lifecycle-20261023T092757-5e37d5ce/run.json)、
[三鏈 replay](../artifacts/operations/migration-v26-final-history-20261023/acceptance.json)、
[v25 完整比較](../artifacts/operations/migration-v25-replication-abba-20261023/measurement.json)、
[v26 D 比較工作](../artifacts/operations/agent-workflow/runs/filesystem-v26-catalog-full-abba-20261023T091720-dcca2643/run.json)。

### 17:45 交付順序修正

程式正確性 admission 已有共用回歸、分類依賴、三鏈 replay 與實際 D 的 20 項
整合／生命週期驗證。把完整速度比較當成 code-only reload 的前置條件，會把
已通過的修正額外串行等待數小時；因此保留原 fixed admission 的所有 code／policy／
baseline／regression／history SHA，另記 deployment-phase admission，分開程式交接
與完整效能／native restore 比較。後者繼續執行，不能因此宣稱已驗收或回收其副本。
沿使用者既有「直接換新版」授權，只用原 `--reload-code-only --restart-active-owner`
入口，持久保存 checkpoint／verified bindings，保留原 journal／lock／未封閉 staging，
不重啟 PG、Temporal、下載器或 NAS，不改 policy。正式實載與換版後三鏈 replay
仍必須讀回；被中斷的原活動沿自身 heartbeat／journal 接續。

清理的前置失敗是另一項真實的 parent-directory reader，而不是可忽略的假陽性。
它已完成；原 retirement 正在續接。私人監督工作明確等待上一個 owner 結束才接手，
不啟動第二個 apply；完整 D 比較通過後才回收這次 D 上的八個比較副本。

### 17:56 實載與目前工作

[原交接收據](../artifacts/operations/migration-v26-boundary-rollout-20261023.json)確認
17:46 完成 code-only reload，正式 PID 48283；隨後三條 persistent histories
完整 replay 通過。[實載讀回](../artifacts/operations/migration-v26-post-rollout-readback-20261023.json)
與當前 worker-ready／checkout identity 都是 `22d206bb…c4eb4`。原三個 workflow run ID
與 first-run chain 保留，沒有重啟 PostgreSQL／Temporal server，原 20 組維護綁定沒有重做。
17:53 的例行維護為 verified 20、pending 35、waiting_retry 3、processing 1、
waiting_source_owner 1；原先完成的固定基線仍為 60／60，不把每日新檢查輪當成倒退。

目前 catalog 872 是舊程式已進入 `publication_prepared` 的交接續作，沿原 journal 完成，
不改寫舊 staging 冒充新型增量匯出。後續第一次新匯出會建立獨立唯讀 seed，再由下一個
已提交版本共用；舊 writable 歷史維持原 bytes／權限。本輪發現的約 37.01 GB 重複邏輯
內容，還沒有作為舊歷史 inode 回收。

第一個離線 root 已通過固定 D 全部原值重建：440 檔、6,514,131,261 原始 bytes；
09:54 UTC 起進入完整 source／mirror／D retirement audit。
17:53:47 transport 檢查只有 `folder_idle=false`，待收 bytes／items／deletes 與各類 errors
均為零，Syncthing 正在 scanning；完整掃描與後續 quarantine／unlink gate 保留。
這些階段樣本不當作完成收據或已釋放空間。

原完整 D ABBA 仍在完整 baseline seal／verify，取樣是 `inventory()` 的 metadata stat。
D 經 9p 存取 NTFS、小檔完整集合與 durability 工作仍是成本；沒有把少寫一份 payload
等同整體加速，更沒有宣稱只剩硬體極限。原 v25 D→SSD 全流程 ABBA 的 20.6% 改善已
完成，不能套用到這個尚未完成的 D→D catalog 比較。

17:52 的[容量快查](../artifacts/operations/filesystem-incremental-20261023-live-mounts.json)：
WSL available 約 42.93 GB，D 約 611.77 GB。下載、維護與其他工作同時改變磁碟使用量，
因此不以整顆磁碟的 free 差額冒充本輪回收量。清理的兩份固定 inventory 與原收據由
[唯一續接工作](../artifacts/operations/filesystem-v26-continuation-v2-20261023.json)排隊接手，
不與尚在執行的舊 apply 平行處理同一批名稱。

以下 15:31–16:02 區段保留當時觀測；目前部署狀態以上方 17:46 證據為準。

### 18:00 容量根因與追加修正計畫（實作前）

FinLab 例行 host-private 維護已凍結 1,456 個控制／已封閉可變輸出，合計
42,125,547,602 bytes。它們位於原 operation 的 SSD `controls` 暫存；來源仍保留，
只有本輪固定版本完成全原值還原後，才能由原 `_retire_owned_scratch()` 回收。
這解釋了 WSL available 從約 82 GB 下降至約 20 GB，而非資料庫漏刪或硬碟容量消失。

原 streamed capture／native SQLite gate 僅保留 5 GiB，一般小型 JSON capture
甚至沒有總剩餘量 gate。追加最小修正：統一讀取既有 `cache_capacity.json` 的
`minimum_free_bytes`，每次新寫入前用原容量檢查保留該門檻與寫入餘裕；已完成、可驗證
的 frozen observation 在低容量下仍可讀回，不強迫重複複製。容量拒絕沿原 waiting_retry
與原 staging 接續，不刪資料、不另建 writer。增加一般 JSON、streamed payload、
SQLite 與既有 capture 重用的針對性測試，再重新 qualification／三鏈 replay／原交接。
此修正是 cooperating writer 的逐次 admission，不宣稱可阻止不遵守 policy 的外部程序寫滿磁碟。

### 18:18 同步與清理互鎖的處理計畫（執行前）

原 Syncthing journal 已明確回報 index 所在磁碟低於 `minHomeDiskFree=1%`，
並非卡死掃描。WSL 容量約 2.430 TB，該比例要求約 24.3 GB；目前整套索引只有
1,163,026,384 logical bytes，磁碟仍約 19 GB 可用。完整離線副本已經在 D 通過
全 SHA 與原值還原，卻因同步停止不能進入原 quarantine／unlink gate，形成容量互鎖。

先回收已驗收 ABBA 的四份私有 SSD 比較副本；另用原本機 REST API，暫時只把
**索引資料庫**的運轉保留改成固定 16 GB（大於當前索引的 13 倍），讓原同步繼續
驗證已發布清冊，再沿同一份 inventory、原 retirement owner 完成回收。只在可用
空間仍足以保留 16 GB、索引未超出該額度的八分之一、原配置與既有權限完全吻合時
執行。保留完整私有前後配置與 SHA，不變更任何 folder／device／認證／網路／索引。
回收完成或有界操作失敗後，皆恢復原 `1%`；沒有把低容量狀態寫成驗收完成。

下載／capture 的 25 GiB gate、封裝的 32 GiB reserve、D 完整讀回與所有原刪除
條件維持。此為暫時解除索引配置與回收的互鎖，不是關閉容量保護或豁免 transport。
官方語義及最小 PATCH 行為：
[minHomeDiskFree](https://docs.syncthing.net/users/config.html#minhomediskfree)、
[configuration REST](https://docs.syncthing.net/rest/config.html)。

### 18:31 剩餘離線封裝的放置計畫（實作前）

兩份原封裝仍要求在 C／WSL 預留整個 source 加 32 GiB；即使第一份精確回收
完成，也未必能讓下一份約 9.86 GB 的封裝通過。降低 reserve 不能解決責任放置
錯誤：封裝是為 D 冷庫產生的短期輸出，應在有容量的 D 上建立。

擴充原 `retire_local_offline_artifacts.py` 的固定 staging profile：只對一個明確、
已通過既有 offline 審核的 `operations/<case>` 子樹，允許使用另一個固定 D
temporary root。仍要求 12 小時穩定、source-size 加 32 GiB、實際 D mount、
完整原值還原、原 transport／consumer／quarantine gate；封裝暫存也只在原資料
精確退役後按原機制回收。不提供任意 staging 路徑、不擴張 raw source／整個
operations namespace／既有 archive 的範圍。為剩餘兩項產生新的 frozen inventory，
保留原清冊與失敗原因，不就地改寫已凍結的 profile。

### 18:16 新版載入驗證

[v27 交接](../artifacts/operations/migration-v27-boundary-rollout-20261023.json)為 accepted，
實載 PID 125581、code identity `5dbda2df…10eec7`；
[換版後三鏈 replay](../artifacts/operations/migration-v27-post-rollout-history-20261023/acceptance.json)
及[正式讀回](../artifacts/operations/migration-v27-loaded-filesystem-observation-20261023.json)
均通過。核心回歸 983 passed／8 skipped；原 60／60 固定基線、三條 first-run chains
及當前已驗 bindings 保留。這次只交換原 worker 程式，未重啟 PG／Temporal server。

第一次索引保留額調整後，Syncthing 在 18:21:43 回到 idle；原 cleanup 子程序因
誤繼承 control runtime、缺 numpy 而中止，沒有 unlink，1% 設定已完整恢復。
第二次使用 Fintech runtime，遇到完整掃描超過 180 秒的觀察上限；保留 scanning
及錯誤證據，再次完整還原設定。第三次延長的是有界觀察時間，不是延展過期 SHA／
retirement proof；真正回收仍重新取得原 full proof。私人配置／認證不進 Git。

### 18:43 pin 查找的成本修正計畫（實作前）

SSD 比較副本的 fresh gate 取樣停在 `artifact_discard._gates()`：為找少數 pin，
每次都列舉 D 上所有 `manifests/*/*.json`，再用 basename 排除大多數版本。
改成每個 dataset 只取至多 `2 × pin 數 + 1` 個目錄項；小目錄一次完成，大目錄
按尚未命中的完整 pin 檔名直接查找。這避免少量 pin 重掃歷史，也避免對很多小目錄
逐 pin 發出多餘 stat；沒有依歷史 mtime 或估計檔數跳過版本。仍檢查每個可能 dataset、
讀取所有命中的原 JSON、原 path／source dependency，且每個 gate 都重新讀 pin。
拒絕不能當檔名的 pin，保留 symlink／非法 JSON 的原拒絕；不建持久 SHA／pin cache，
也不修改真正 CAS reference graph 必須掃描的所有 manifest。先驗多 dataset 重名、
literal wildcard、無命中／非相關版本、受損 pin 與 live pin 變動，再對實際 D 量測。

## 實際磁碟與掛載

| 位置 | 實際格式／存取方式 | 觀測容量 | 觀測可用空間 |
| --- | --- | ---: | ---: |
| WSL 專案、可變狀態、hot materialization | `/dev/sdd` ext4，4 KiB block | 2.430 TB | 16:02 約 94.04 GB |
| Windows C，包含 WSL 虛擬磁碟 | NTFS | 4.096 TB | 約 349 GB |
| D 主冷庫、immutable lake | Windows NTFS，WSL 經 9p／DrvFS | 4.001 TB | 約 621 GB |
| lab203／NAS | 本輪僅核對配對收據；未直接觀測 NAS 底層格式 | 未測 | 未測 |

使用十進位 GB／TB。WSL 可用空間與 C 剩餘實體空間不能相加；D 的 `packed`、
`d-volume` 是同一磁碟的介面，也不能算成兩顆磁碟。
ext4 查詢回報 clean，保留 5,368,709 個 blocks；本輪沒有縮小 reserve、執行線上 fsck、
格式化磁碟或移動 active DB。Windows C／D 回報 Healthy／OK；這不是全磁碟壞軌掃描。

證據：[掛載與 statvfs](../artifacts/operations/migration-v25-20261023/filesystems.json)、
[Windows volume metadata](../artifacts/operations/migration-v25-20261023/windows-volumes.json)。
Windows C／D 採樣為 15:35；16:02 的 WSL 容量來自
[最新讀回](../artifacts/operations/migration-v25b-filesystem-final-readback-20261023.json)。
下載與其他工作同時持續寫入，因此當前整顆磁碟的 free 變化不是本輪回收量；以逐 inode
退役收據為準。原 `fstrim.timer` 仍 active；WSL 可用空間、VHDX 實體配置及 Windows
剩餘容量分開計量，沒有為了回收顯示值中斷工作或直接壓縮正在使用的 VHDX。

## 增量保存的實際邊界

目前的選定架構保留 ext4／NTFS，把增量與歷史版本放在資料層：

- 原始觀察與 provenance 由各下載器持有；新觀察追加、更正另留版本。
- 每個固定版本的 manifest 描述完整邏輯檔案集合。未變的 SHA-256 物件／8 MiB
  區塊共用，只保存新增內容；獨立還原任何固定版本，不必重播前面所有版本。
- 新聞、分鐘與其他分區資料沿自身的增量分區／part；同一份來源的不同訓練視圖
  與臨時計算結果有獨立生命周期。
- 同內容的已提交服務 payload 使用原 writer lock 下的唯讀 hardlink 分享；保留
  每個邏輯名稱、manifest、READY、原值、執行與會計語義。
- NAS 的獨立還原、配對 ACK 與傳輸暫存回收另驗，不把同步 idle 當備份完成。

[33 根目錄的當前 metadata 清冊](../artifacts/operations/migration-v25-20261023/incremental-layout.json)
沒有清冊錯誤。20 個 shared v2 head、1 個 local news v2；其餘 12 個均是
`publish:false` 私有分支，不能為了顯示完整而改成 shared。原固定 60 項 D 版本基線
已完整還原通過，詳見[原驗收讀回](../artifacts/operations/migration-v24-baseline-readback-20261023.json)。

這沒有把 ext4／NTFS 轉成支援原生快照的其他磁碟格式，也沒有把所有 native workspace
宣稱為逐列增量表。例如 FinLab 的整表來源修訂仍可能產生大量變動；DuckLake 新
catalog 匯出已加入 immutable donor 共用，但仍保留完整清冊、SHA 與獨立讀回。
其來源格式、備份 ABI 和 NAS 固定 restore 程式要分開驗證，不能直接 hardlink
可變 Parquet／SQLite 或略過完整 hash。

## 空間清冊與清理順序

一次完整 ext4 `du` 讀回 `artifacts` 為 **826,796,793,856 allocated bytes**。
它是當次檔名／inode 的清冊，活躍寫入之後可能改變；尚不是可刪清單。

| 類別 | 該次 allocated bytes，約 GB | 後續操作 |
| --- | ---: | --- |
| benchmarks | 263.36 | 逐個測試 case 核對 producer 完成證據及來源，不整包刪 suite |
| cache | 172.47 | 由原 policy 查重、pin／lease／程序與 commit gates |
| live | 126.64 | 含執行、訊號、修復與研究結果，保留業務用途及會計證據 |
| operations | 75.81 | 區分小型證據與可重建 payload；保留 logs／manifest／code |
| markets | 52.28 | 由原完整 lifecycle／cold retirement 接手 |
| data_preparation | 35.72 | 核對實際 consumer 與精確冷恢復，不按名稱刪 feature |

[完整一次清冊](../artifacts/operations/agent-workflow/runs/filesystem-artifact-allocation-2-20261023T072247-ee6f102f/run.log)
耗時 158.42 秒；[定點分布](../artifacts/operations/agent-workflow/runs/filesystem-hot-allocation-202610-20261023T072451-b696b675/run.log)
另量 cache／live／operations。不同遍歷的 hardlink 歸屬與時點不同，表格只使用完整同一遍清冊。

本輪兩組服務快取 dry run 都沒有可分享的候選，candidate allocated bytes 為 0：
[physical minute](../artifacts/operations/filesystem-service-cache-audit-20261023/physical-minute/tw_minute_paper_parity_09bedd96a2f68c39/latest.json)、
[completed forward](../artifacts/operations/filesystem-service-cache-audit-20261023/completed-forward/completed_live_inference/latest.json)。
前者保留不完整 generation，後者保留未驗證的 commit／外部 hardlink；沒有憑空清掉原件。

另對既有 `running_services_20261013` 的 94 個 case 做有界完成證據分類：
[分類清冊](../artifacts/operations/migration-v25-20261023/benchmark-completion-census.json)。
5 個有可識別的完整 producer 比較，其餘不把「目錄很舊」當作已完成。這 5 個已由原
`retire_local_offline_artifacts.py` 完成 inventory、D 發布／獨立原值還原、程序／服務／
pin／lease／傳輸 gates、quarantine 和精確退役：
[完成收據](../artifacts/operations/filesystem-completed-cases-20261023/receipts/progress.json)。

本輪新回收 **280,576,000 allocated bytes／180 個檔名**，5／5 成功，完整流程
462.84 秒。其己方 encoding stage 的 112,062,464 bytes 也已退役；它是本輪臨時產生的
配置，不再加成額外的淨回收量。完整 producer 證據與小型收據保留本機。
[獨立讀回](../artifacts/operations/filesystem-completed-cases-20261023/readback.json)核對來源、
mirror、stage 都不存在，五組固定 D manifest、原完整解碼收據與物件 inode 證據仍一致。
此讀回是原 byte-level proof 的綁定及存在性複核，不宣稱再次全讀 payload。
下載器的來源資料、D 物件及 NAS 歷史刪除均為 0。

沿用原 14 天規則分別檢查
[home compiler cache](../artifacts/operations/filesystem-compiler-cache-20261023/home/storage-pressure-audit-20261023T074509.225245Z.json)
與[固定 tmp compiler cache](../artifacts/operations/filesystem-compiler-cache-20261023/tmp/storage-pressure-audit-20261023T074510.362308Z.json)，
兩者 inventory 完整、errors 為空、0 eligible／deleted。本機沒有擴充 scope 的本地 policy；
保留該次拒絕證據，改用既有固定 scope，沒有新增 enrollment 或縮短保留天數。

今天先前 FinMind／FinLab 的己方暫存已累計回收 **44,306,771,968 allocated bytes**，
共 76,542 個名稱；這是原 operation 的歷史已完成結果，不重算成這次新清理。
本段 16:02 時新 v25 ABBA 的四個副本仍在等待完整比較；18:35 已退役，見上方
本階段讀回。仍在進行的 v26 D 比較、下載、還原與 NAS ingress 不清。

## 已實作的效率修正與查詢方式

v25 修正同次複寫的重複完整來源 inventory、交接等待占住其他 owner，以及進度分子
混入歷史不同範圍的問題。主回歸 957 passed／8 skipped；舊冷證據重驗及遠端 SSH
deferred 修正 103 passed。實際載入狀態與完整比較見[原 v25 紀錄](dataset_incremental_storage.md#2026-10-23-v25)。
比較仍在進行時，不以局部省讀取次數宣稱完整流程已到硬體極限。

16:02 共同工作目錄的分類依賴變更已保留並重驗：共用回歸 957／8、分類 24 項、
三鏈 replay 通過，[v25b admission](../artifacts/operations/migration-v25b-rollout-dispatch-20261023.json)
綁定完整 49 檔；舊等待工作已取消。**16:02 當時**正式服務仍為 v24，新候選尚未實載，
[原監督續接](../artifacts/operations/agent-workflow/runs/migration-v25b-finish-existing-r-20261023T080113-3becf85f/run.json)
繼續等待安全交接與完整比較，沒有為了部署中斷健康的來源還原。

新增以下快查，沿用原 authority 入口，不掃全庫；初版 16 項測試通過，正式整個命令實測
0.441 秒。再補掛載 JSON 非 list／空 list／null 成員的拒絕檢查，
[最終 19 項測試通過](../artifacts/operations/agent-workflow/runs/filesystem-final-mount-tests-20261023T074550-f470dd06/run.json)：

```bash
bash scripts/run_authority_storage_operation.sh scripts/audit_node_storage_roles.py \
  --filesystems-only \
  --filesystem-path /root/stockAgent \
  --filesystem-path /srv/stockagent-packed \
  --filesystem-path /srv/stockagent-d-volume \
  --output artifacts/operations/filesystems-current.json
```

資料版本進度使用既有 `scripts/dataset_migration_status.py`；目錄版本範圍使用
`bash scripts/run_data_release.sh layout`。掛載／容量觀察、metadata 清冊、完整原件還原、
NAS 驗收及可回收判準各自保留，不因某一項通過而宣稱其他項目完成。
