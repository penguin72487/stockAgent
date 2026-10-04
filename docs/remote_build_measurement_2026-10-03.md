# 遠端建置條件與完整實測（2026-10-03）

使用者要求每次遠端建置依該機器實際條件選最快方法，除了經驗還要實測。
任務 `remote-build-measurement-20261003` 延續
[目標架構](target_architecture_execution_2026-10-03.md)。前次 2／8 threads 的
ABBA 只證明那兩個候選的差異，不能宣稱整台機器最適設定。

## 第一性原理與本次條件

建置時間受可用 CPU 時間、記憶體／NUMA、來源 layout、儲存及 workload 制約。
CPU 可見數不等於容器額度；pool 數不等於 CPU entitlement；歷史勝者不能
代替新機器的資料。以完整 wall time 選最快，另記 CPU-seconds／peak memory
和證據，保持 correctness oracle、全部資料與所有前後校驗。

04:23 UTC 的實際遠端觀測（`remote-baseline.json`）：

| 條件 | 實際觀測與意義 |
| --- | --- |
| 節點 | `d2ad4577b190`，2 × AMD EPYC 7663，112 physical cores／224 affinity CPUs |
| CPU quota | `5375999 100000`，約 53.76 CPU 時間額度，不能從 224 推定 224 CPU entitlement |
| RAM | cgroup `memory.max=259591766016`，約 241.76 GiB；host MemAvailable 不代表此容器 headroom |
| 初始 memory.current | `119676735488` bytes；保守使用 max-minus-current 及 host MemAvailable 中較小者，不預支 cache reclaim |
| 輸出 scratch | 約 74.93 GB free；預先容納全部候選完整表與 receipts，不刪舊資料 |
| GPU | 2 × RTX 5090，當下 utilization 0；這是觀測，不是 GPU readiness，本次只測 CPU 建表 |
| 負載 | load 6.49／5.39／4.85，CPU／I/O pressure avg10=0；每輪另留當下觀測 |

操作前讀 `/etc/vast-agents-guide.md`，hash 與本次保存副本一致；不改 kernel／
quota／driver、不啟停其他程序、不使用 GPU。獨立、新私有輸出逐一執行。

## 可重用實作與預先宣告的比較

`stockagent/remote_build.py` 探測 affinity、可見 cgroup-v2 祖先、CPU topology／
NUMA、memory headroom、mount、load／pressure，分開 node identity 與即時使用量。
硬體／boot／quota／memory limit／mount 變更會改 identity；memory.current 變動
另外做當下准入，不能用舊 headroom。CPU quota 限制時間，初次 bounded pool
搜尋可到 min(affinity, 2×ceil(quota))，本機 cap 為 108，非全域最優保證。

`benchmark_tw_public_remote_derivation.py` 用 canonical 完整 builder：

1. 每輪獨立 process、全部來源／code 前後 SHA、完整表／summary。
2. 初次比較 2／4／8／16／32／53／64／108 的 Polars／Arrow pools。
3. 在觀測最快 pool 上測 Arrow 候選與可行的 NUMA physical／logical affinity。
4. 前兩名各做正反順序兩次追加實測，每個 finalist 至少三次完整樣本。
5. 最低完整平均 wall 的 eligible 設定用 `--tuning-receipt` 再真正建表驗證。

選擇必須保持原 9,584,859-row／143-feature correctness oracle 的完整欄位、
source receipts、universe、schema 與 output SHA。每輪 peak RSS 不超過64 GiB，
當下 headroom 與 scratch 要足夠；候選超時／失敗／不相容便不參與排名，原紀錄保留。
SHA 校驗會暖 file cache，不稱冷磁碟 benchmark。量測包含 process 啟動與全部
verifier 工作，不能只選最短 builder 階段。時長區間重疊時只稱觀測均值最快。

試行 code SHA `2903199d3eefff5157d050d1523dda972c2b066cbb3c5e412a28ca50a5d2282d`；
源於原 accepted `d3bc…` 樹，只加入8個 explicit code／tests／runbook deltas，
固定樹 2,273 files。新 wheel／source.zip 包含1,093 source／selected config files，
獨立相同 SHA wheel 重建、外部安裝／CLI／provenance 已 accepted，25.844 秒。
新語意、guard、version、owned-process 取消測試41 passed；固定樹再驗41 passed。
沒有把原 full-suite 12,994 tests 重新標成此新版本的測試。

選定 source 仍為 exact `tw-public-20260913T030921278338896Z-l0-penguin-248d0869d7d53be4`，
cutoff 2026-09-11；current consumer 沒有改動。既有 canonical lease admission
確認有效至 2026-10-10T03:35:02Z，sources／READY／manifest 仍由原 owner 管理。
實測途中每輪重新核對131,332 source files。cold object recovery／來源刪除
並非本次量測的結論。

## 第一輪實測及拓撲修正

`2903199d3eef…` 固定版本完成15次完整建置，全部 exact output parity 通過；
量測總 wall 1,782.938 秒。初篩各一個樣本，不能直接當成重複均值：

| Polars／Arrow | 完整秒數 | 狀態 |
| --- | ---: | --- |
| 2／2 | 308.385 | accepted |
| 4／4 | 186.797 | accepted |
| 8／8 | 133.694 | accepted |
| 16／16 | 106.881 | accepted |
| 32／32 | 95.462 | accepted |
| 53／53 | 92.026 | accepted |
| 64／64 | 95.904 | accepted |
| 108／108 | 109.465 | accepted |

53／26 的三個完整樣本均值92.726秒、範圍92.318–93.521秒；53／53 均值
93.878秒、範圍92.026–94.914秒。範圍重疊，只稱53／26觀測均值最低。
該 receipt 真正套用再建一輪92.008秒，全部來源／輸出／資源 gate 通過。
108／108 單輪消耗2,057.601 CPU秒，64／64 為1,182.524 CPU秒；增加到108
在此輪反而更慢、更耗 CPU。初篩原始結果、決賽與套用各自保留。

核對實際 `node-before.json` 後確認是8個 NUMA 區，每區14實體核心／28 logical
CPUs；原單區候選都不足容納53 pool，未執行 NUMA 綁核，不把它說成已驗。
本機每個 socket 含4個 NUMA 區，仍有56實體核心／112 logical CPUs 可比較。
因此共享候選生成器補上 per-socket physical／logical 及全機physical affinity，
依 CPU 集合去重；另將選擇收據驗證補上實際 affinity 與 RAM budget。
NPS4、多重 NUMA／socket、去重與不相容證據測試45 passed，固定樹另驗45 passed。

第二輪程式 SHA `714bd89eecf516a8d7474406e8a7ff8b82ebd0fd955d6362ec0a39104a0a7c8e`，
保留原固定樹，只改候選 helper、相關測試與 runbook 三檔，canonical builder
檔案 SHA 沒有改。新 wheel 可重建／外部安裝／CLI／provenance 驗收23.324秒。
第二輪以32／53／64作較小候選集，重新比較 Arrow、可行的插槽／實體核心
配置、正反順序 finalists 與實際套用；不混用兩個程式版本的選擇均值。

## 雙 socket 與資源變動

第二輪完整初篩（各一個樣本）中，Polars／Arrow 53／26 開放224 logical
CPUs 是90.600秒；綁全機112個physical cores、使用兩socket是92.187秒。
socket 0 的56 physical cores為82.890秒，socket 1為89.091秒。
socket 0 physical的三次完整均值84.042秒、範圍82.890–84.650秒；
同socket logical的三次均值84.581秒。這兩組的範圍重疊，保留觀測限制。

使用者再提出兩socket都用，新增均衡53 physical cores：socket 0分27、
socket 1分26，分散於全部8個NUMA區，CPU集合
`0-6,14-20,28-34,42-47,56-62,70-76,84-89,98-103`。
三次完整建置89.954／89.822／91.662秒，逐份來源和輸出SHA通過，
均值90.479秒；沒有因新增候選而重跑已完成的16個相容完整樣本。

原第二輪14個完整樣本後，勝者套用被verifier的RAM／scratch准入拒絕，
原失敗log與選擇候選完整保留。後續均衡測試在第三個樣本啟動前，又被
64 GiB記憶體headroom門檻擋下；這次沒有啟動建表程序，不填入虛構時長。
後續當下觀測headroom為67,056,586,752 bytes，scratch為9,099,915,264
bytes，已不足准入。開頭74.93 GB free的觀測不能替代當下磁碟容量。

盤點確認第一輪15個本任務私有完整輸出皆為相同1,317,309,373-byte表，
各有accepted canonical收據／穩定SHA，無程序引用，也不是來源、model、
service cache或受pin／lease保護的materialization。使用既有
`stockagent.data_sync.artifact_dedup.apply_duplicate_groups`，先read-only inventory，
套用時重驗SHA／metadata／process gates。這是保留全部邏輯路徑的相同內容
去重，不是資料eviction；不使用或繞過`stockagent-data gc`。
盤點與套用收據保存於`socket-phase/private-output-dedup-inventory.json`和
`private-output-dedup-acceptance.json`。套用14個相同內容的hardlink替換後，
逐份重驗全部15個表SHA，統一唯讀；14份重複allocated bytes共
18,442,346,496 bytes，scratch free由4,584,189,952回到23,026,626,560
bytes。收據／logs的SHA全部不變，來源、model、pin、lease與其他工作輸出
沒有改動。下一輪fresh headroom為85,703,938,048 bytes，才准入剩餘兩個
完整建置；仍保留64 GiB RAM budget與10 GiB scratch reserve。

## 結果與真正套用

下表全部為相同`714bd89eecf5…`程式、來源、runtime與完整工作流程，
Polars／Arrow固定53／26。confirmation獨立於三次決賽均值。

| CPU配置 | 完整樣本數 | 完整均值秒 | min–max秒 | 最大peak GiB | 平均CPU秒 |
| --- | ---: | ---: | --- | ---: | ---: |
| socket 0，56 physical cores | 3 | **84.042** | 82.890–84.650 | 51.33 | 866.212 |
| socket 0，112 logical CPUs | 3 | 84.581 | 83.363–85.331 | 51.72 | 889.152 |
| 兩socket，27＋26 physical cores | 3 | 90.479 | 89.822–91.662 | 52.93 | 889.682 |

實測勝者是socket 0的56 physical cores，CPU集合`0-55`，Polars53、Arrow26。
使用新不可變`selection-resume-candidate.json`真正套用後，獨立完整重建
**85.727秒**、peak51.38 GiB、876.559 CPU秒，全部來源／程式前後校驗、
pool／實際affinity／RAM budget、完整schema／summary／輸出SHA均通過。
驗收後的`selection-resumed.json`保存三次basis的SHA與這次confirmation，
不能用初篩的單輪時間或把confirmation混入決賽均值。

本輪兩socket配置已實測，結果較慢；兩socket仍共用53.76 CPU時間額度，
開放更多physical/logical CPUs沒有增加額度。CPU affinity比較也不等同於
驗證NUMA memory binding。沒有將本建表結果外推成GPU訓練或所有workload
都應綁單socket的結論。前兩名區間重疊，只稱此次eligible candidates中
觀測均值最低，不稱統計顯著或全域最優。

第一輪15次、第二版18次，共**33次完整建置**的9,584,859 rows、143 features、
162 columns全部保留同一輸出SHA：
`85dab0305dc562815c9de1429af89eb1f1b46478ad460bfbd319d21cf7888589`。
第一輪15份輸出先獨立重驗，再於相同內容去重後逐路徑重驗；第二版全部18份
於量測完成後另重新計算實際parquet SHA。第二版42份原始receipts／logs／
progress副本帶精確SHA保存於本機。原第二輪verifier拒絕與後續監督流程的
啟動前RAM拒絕，仍是兩個failed runs；舊progress／logs沒有覆寫或改稱成功。

固定程式45項相關語意測試通過；2,273-file lineage、8個explicit deltas、
獨立重建wheel／checkout外安裝與startup provenance另驗。原生Python套件
環境保持原baseline身分。沒有移動current consumer、promotion、來源刪除、
新增GPU工作或更改金融／特徵算法。

第一輪監督量測1,782.938秒；第二版兩次原監督流程加剩餘遠端執行共
1,601.033秒，其中最後兩個完整建置及編排177.742秒。第二版時長包含已失敗
監督工作的耗時，不包含中間idle、本機包裝及獨立去重工作；各run有自己的
真實wall receipt。探索本身也有成本。未變更的exact node／boot／code／
source／runtime／workload可重用已驗收receipt，但每次仍做當下資源准入；
條件改變時以上次勝者及相鄰候選開始較小的完整實測，而非每次從2 threads
重跑整個初次matrix。

遠端已驗收版本的重用方法（`NEW_PRIVATE_BUILD_ROOT`必須是新私有輸出）：

```bash
cd /root/stockagent-remote-build-measurement-20261003-714bd89eecf5/source
source scripts/runtime_env.sh
run_fintech_python scripts/verify_tw_public_remote_derivation.py \
  --code-root "$PWD" --code-receipt ../release.json \
  --packed-root /srv/stockagent-packed \
  --source-root /srv/stockagent-packed-materialized/tw-public/tw-public-20260913T030921278338896Z-l0-penguin-248d0869d7d53be4 \
  --snapshot-id tw-public-20260913T030921278338896Z-l0-penguin-248d0869d7d53be4 \
  --end-date 2026-09-11 --build-count 1 \
  --tuning-receipt ../measurements/selection-resumed.json \
  --output NEW_PRIVATE_BUILD_ROOT
```

工程證據集中於`artifacts/operations/remote-build-measurement-20261003/`，
第二版在其`socket-phase/`：

- [原始實測與實際套用驗收](../artifacts/operations/remote-build-measurement-20261003/socket-phase/remote-acceptance.json)
- [所有樣本與決賽表](../artifacts/operations/remote-build-measurement-20261003/socket-phase/benchmark-tables.md)
- [已真正套用的選擇收據](../artifacts/operations/remote-build-measurement-20261003/socket-phase/remote-selection.json)
- [輸出重驗與原始證據SHA](../artifacts/operations/remote-build-measurement-20261003/socket-phase/remote-evidence-acceptance.json)
- [私有輸出去重驗收](../artifacts/operations/remote-build-measurement-20261003/socket-phase/private-output-dedup-acceptance.json)

操作與套用方法見[遠端建置流程](remote_build_workflow.md)；共同要求已記錄在
[效能契約](agents/performance.md#remote-node-selection)，不是私人記憶規則。
