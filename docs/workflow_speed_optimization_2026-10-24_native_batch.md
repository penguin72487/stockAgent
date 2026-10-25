# 原生批次讀取：全量比較與正式完整流程驗收通過

本輪針對 R47 的 `Evidence` 讀取瓶頸實作 C++ 候選。R85 已接入既有正式
backend，合計 264 項相關檢查通過；R87 完整 13 階段、重建與原版 reader
驗收已完成。這證明正式整合的完整性；完整 13 階段的受控加速比例仍未測定。
R87 驗收對應 profile `2219c318…`；後續 CEX 原生時計接入已通過 R90 的
290 項語意測試及全部原始列驗證，R92 完整 13 階段驗收也已通過，詳見
[目前接入狀態](workflow_speed_optimization_2026-10-24_crypto_feature_clocks.md#正式收集入口已通過語意與原始列驗證)。
後續 R107 保留全部原 scope 並加入 R96 原始 journal，新的完整 13 階段及全部
原始 oracle 也已通過；來源覆蓋為 **2,399／1,653 stage-bound／746 未綁定**，
詳見[發布時鐘 reader 的完整再驗收](workflow_speed_optimization_2026-10-24_native_tw_public_clocks.md#r105r106r107發布時鐘共用讀取與完整範圍再驗收)。
再後續 R116 已保留全部原始 oracle，加入七個已驗證 issuer 離線解析階段，
當時為 **2,399／1,660 stage-bound／739 未綁定**，詳見
[R116 完整驗收](workflow_speed_optimization_2026-10-24_crypto_feature_clocks.md#r116完整-13-階段與七個-issuer-的精確綁定)。
R141 再完整驗收 Dune／SEC 的三個綁定，最新為
**2,399／1,663 stage-bound／736 未綁定**，見同報告的 R141 接續驗收。
R47 讀取 worker 的批次 inclusive 為 255.410382 秒 wall／85.799700 秒
thread CPU，其中 bounded read 164.986169／31.144985 秒、stat
44.321431／22.912367 秒。區間包含子操作且與主執行緒重疊；wall 減 CPU
不能直接當成磁碟時間。[原始計時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round47/collection_full/inspection_profile.json)

## 候選與正確性

每批一次跨入 C++，釋放 GIL 後依序 stat、open、bounded read、stat。
SHA、分類、消費端再次檢查世代、預算及收據封裝仍由既有 `Evidence` 負責。
額外 fstat 防止 stat／open 之間換成非普通檔；推測性失敗仍在原順序重試。
保留原始讀取上限、短讀取補足與符號連結換代檢查。

原生暫存與 Python bytes 轉換會短暫共存，容量計畫已計入額外副本；
不是只以最終輸出大小估記憶體。每次完整比較前檢查 root 保留 8 GiB、D
保留 32 GiB、可用記憶體至少 24 GiB，沒有刪除來源或舊證據。

- C++ source SHA：`ab93c80d1f5734918dad124f3f8fc93fdb550cbf9a08e6d9a89c508282b484ac`
- adapter SHA：`090fe22b65102d943c2eff46446cd033a357996b551634f6f77508203c10973d`
- source／ABI 綁定 module SHA：`53c60303d234ef32ffcc5d253fa33145ab473e2a3c40c2f60bd11e497aae08b7`

R63 編譯 3.155160 秒 wall／1.576509 秒 descendant CPU。R66 實際完成
**16 tests、0 failure／error／skip**，包含短讀取、精確 ns、異常檔名、FIFO
略過、容量、三種世代換代、檔案晚到、消費端失敗與 worker 收尾；並重用
維護中的完整輸出 parity oracle。檢查命令 0.927557 秒 wall／0.679415 秒
descendant CPU。[檢查與來源收據](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round66/completion.json)

R63 測試 import、R64 封存 module ROOT、R65 封存清單含目錄等啟動器錯誤
皆保留 failed 原紀錄，由實際成功的 R66 取代；不是原生讀取的效能結果。

## R67 小規模八輪：結果未定

固定 R4 封存輸入，保留全部 107 份 explicit receipts，再每 96 個輸入抽一個，
合計 **3,986 inputs／2,006 samples／722 groups**。八輪均完成六份輸出與
新程序讀回，六份語意指紋完全相同，無 semantic exclusion。

| 順序 | baseline command wall 秒 | candidate command wall 秒 | wall 比率 | descendant CPU 比率 |
| --- | ---: | ---: | ---: | ---: |
| ABBA | 5.908191 | 4.911274 | 0.831265 | 0.854530 |
| BAAB | 4.712293 | 4.673297 | 0.991725 | 1.023890 |

判準是各半部 wall ≤ 0.98、CPU ≤ 1.02；第二半部未通過。這個小規模結果
不支持採用，也不能據此否定完整工作量的效益，接續以全量測量解決規模差異。
[八輪原始數值與 parity](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round67/comparison.json)

## R69 全量比較已完成

完整固定 **372,550 inputs／107 explicit receipts**，輸入 manifest SHA
`f4bb9b16c7d423b7dbb85bc1f1c61cb5e990596de2c2fe881672572b0959044d`。
基準與候選都使用一個 read worker、一個 pending batch、256 MiB batch 上限、
同一份 canonical profile `f0693863…` 與 `packed_gzip_v1`，兩組均保留相同的
原生實作與 binary 證據。每輪含全部六份輸出、原始 SHA 及新程序讀回；
比較完整 child command wall 與 descendant CPU，不以單一 read syscall 代替。

R68 在來源封存時碰到同名檔案，尚未開始 trial，保留 failed 原紀錄。
R69 改用唯一的來源封存名稱。至 **01:27 UTC**，八輪完整 command、每輪
新程序讀回與跨組六份輸出語意比較均成功，owner 已 succeeded／inactive／
MainPID 0。每輪保留 **2,006 samples／722 groups／705,929 reported fields**，
獨立驗證 **372,550 個選定輸入身分、六份輸出、3,752 份 unique originals**。
沒有 semantic exclusion，也沒有以 inspection 單段取代完整 child command。

| 順序 | baseline command wall 秒 | candidate command wall 秒 | wall 比率 | descendant CPU 比率 | 判定 |
| --- | ---: | ---: | ---: | ---: | --- |
| ABBA | 341.686498 | 326.269786 | 0.954881 | 0.909383 | 通過 |
| BAAB | 336.552021 | 323.195588 | 0.960314 | 0.895548 | 通過 |

兩半 wall 分別降低 **4.51%／3.97%**，CPU 降低 **9.06%／10.45%**，均符合
事前 wall ≤ 0.98、CPU ≤ 1.02 門檻。最後一輪候選完整 command 為
302.120115 秒；不單獨選用最快樣本作倍率。R67 小規模未定結果仍保留。

[完整 protocol](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round69/protocol.json)
[八輪、雙順序門檻與六份輸出 parity](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round69/comparison.json)
[原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r69-native-full-corpus-20261024T003123-ad5b4e69/run.json)

這是 **固定 inspection phase（含完整輸出與獨立讀回）通過**，不能直接當作
完整 13 階段的速度改善。R85 正式整合及目前驗收狀態見下方。
不能把局部百分比套到整體工作流；R92 時 748 個異質來源身分未綁定階段計時，
後續 R107 加入真實原生事件後為 746 個，R116 接入七個 issuer 解析階段後為
739 個。所有工作流測完及硬體極限均未證明。

## R85：正式整合與完整驗收

原生批次函數併入既有 `workflow_metric_tree.cpp`，共用同一個 source／ABI
綁定的 cache 和 loader。Python `Evidence` 保留 SHA、消費順序、世代重查、
容量判定及同步重試。支援範圍外的 path／整數上限與 timestamp 在原 Python
路徑處理；新增保守 ns 範圍檢查，避免 C++ 有號溢位。raw payload 副本上界
明列為 `(2 * workers + 1) * (batch_bytes + 1)`，不包含 path／物件 metadata。

訓練 discovery 同時補入 `epoch_curve_plot_timing_everyN.json` 的正整數間隔，
維持原來深度預算與 parser。R85 保留 R47 全部 231 份 explicit receipts、
五個 training roots、三份 operation journals，再加入 R73 已驗證的原生
計時目錄與八份收據，成為六個 roots／239 份 explicit receipts；沒有減少原工作。

02:06 UTC，四套相關測試 **236 passed，0 failure／error／skip**。完整 pytest
child **15.137224 秒 wall／10.829853 秒 descendant CPU**，owner succeeded／
inactive／MainPID 0。包括原有 214 項檢查、18 項原生讀取檢查、四種 discovery
threads 的名稱／深度驗收；新增極遠 timestamp 與超大 Python 整數的 fallback。
正式 source SHA：profile `2219c318…`、C++ `7782d70f…`、loader `145214c7…`。

R78 寫出時 SHA 比較因外部重疊修改 `private_json.py` 而在第四輪新程序
讀回中拒絕，原 failed run 與前三輪全量輸出保留。新檔保留每個既有非 docstring
AST node，僅新增此收集流程未呼叫的帳本 encoder；R85 保留該修改並重新驗收。
**寫出時 SHA 未採用，沒有八輪加速結論。**完整原因及 SHA 已封存在 R85 準備收據。

- [正式 Evidence 整合](../scripts/profile_workflow_speeds.py)
- [共用原生 backend](../stockagent/workflow_metric_tree.cpp)
- [R85 決策、全部來源 SHA、scope 與可逆補丁](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round85/preparation.json)
- [236 項完整檢查與時鐘](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round85/checks.json)
- [R85 完整流程與獨立原版 oracle 程式](../artifacts/benchmarks/workflow_speed_optimization_20261021/round85/run_native_integration.py)

完整流程另要求實際 `native_prefetch_batches > 0`、全部 14 份輸出及每份原始
內容讀回、R47 的 2,399 個來源與 1,651 個 stage bindings 不遺失、原三份
journal clock oracle、R45 原計時與 R73 的 14 檔／6,655 筆完整 row multiset。
不同保留快照可能重疊，不將它們相加為獨立訓練工作或新來源 coverage。

R86 另完成共用 walker／cache 的 **28 tests，0 failure／error／skip**，
**4.164773 秒 wall／3.490711 秒 descendant CPU**；含數值物件身分、負零
bits、Unicode redaction、深樹與特殊容器 fallback、callback 錯誤、cache 上限、
ABI／module hash、失敗編譯與損壞 cache。連同 R85 合計 264 項相關檢查。
[R86 完整檢查收據](../artifacts/benchmarks/workflow_speed_optimization_20261021/round86/completion.json)

R85 首次 full 在 collector 啟動前，因 `signal_engine.py` 的外部編輯而拒絕，
未產生 collection。該檔僅為觀測盤點對象，沒有載入執行；原 gate 將觀測來源
誤當成執行依賴。R87 保留此失敗，將 27 份 execution／parser 來源維持嚴格
凍結，13 份觀測來源繼續完整擷取 bytes／SHA，結束時另報目前版本是否相同；
不以目前服務檔案相同推定其歷史載入身分。沒有移除任何 measurement source。

R87 沿用相同正式程式及已通過的 264 項檢查；六個 training roots、239 份
explicit receipts、三份 journals、全部輸出、復原及 original-reader oracle 都保留，
只將 collection destination 移到新 cohort。02:15:16 UTC 由 canonical owner
啟動；02:45:19 UTC succeeded／exit 0，實查 inactive／MainPID 0。
完整 collector、canonical reader、實體重建、未修改的原版 reader 與額外原生
時鐘 oracle 均通過。這次完整驗收共 **1,797.671285 秒**，不省略任何驗證。

| 完整工作段 | wall 秒 | CPU 秒 | CPU 範圍 |
| --- | ---: | ---: | --- |
| collector（全部 13 階段） | 626.471373 | 372.529694 | descendant |
| canonical 獨立讀回 | 186.633538 | 41.995114 | descendant |
| 實體 legacy 重建 | 282.544137 | 30.551595 | process |
| 原版 reader 加來源／原生時鐘 oracle | 698.783051 | 111.515582 | descendant |

重建 **15 份控制檔、6,554 份 gzip originals，共 936,805,907 bytes**。
兩種 reader 的基礎結果全部相同：**14 outputs／6,554 unique originals／
249,757 samples／29,667 groups／13 phases**。非計時 payload 的 353,993 筆
身分與 18,114 筆分類重用鏈已驗證；這些 payload 並非全部封存、重讀。
不同實體布局和額外 oracle 使上述兩個 reader 的耗時不能當成受控速度比較。

來源 oracle 核對原 **2,399 IDs／1,651 bindings** 的集合，皆無遺失；
剩餘 **748** 個仍未綁定來源專屬計時。三份 journals 保留 21 個 unique
operations，重複匯出不重算；R45 四檔／226 samples、R73 十四檔／6,655
samples 的完整 JSON row multiset 與原 parser 一致。33 份 measurement
source 的封存 bytes／SHA 亦通過。`signal_engine.py` 在觀測期間又有外部
修改，前後 SHA 已分列，沒有將目前檔案當成服務歷史載入身分。

FinMind 四筆 `latest_success_without_consistent_transport_clock` 原有缺口
仍保留，不補造耗時。實際使用 **11,613 native batches／371,165 prefetched
reads**，不是只驗證開關。R85 啟動前的來源角色誤判由這次實際成功驗收取代；
R78 未完成的 SHA 比較仍不具採用結論。

### R87 全部 collector 階段計時

以下 CPU 為 collector process CPU，不能與上表的 descendant CPU 混用。
wall 與 CPU 的差包含等待及排程，尚不能全部歸因於磁碟。

| 階段 | wall 秒 | process CPU 秒 |
| --- | ---: | ---: |
| code inventory／source capture | 6.493330 | 0.230441 |
| environment observation | 0.067365 | 0.021284 |
| scope inventory | 0.778962 | 0.278387 |
| service／HTTP／TEJ／storage receipts | 0.143526 | 0.120145 |
| acquisition receipts | 6.086314 | 3.971469 |
| engineering command receipts | 0.931621 | 0.778619 |
| training discovery | 8.202552 | 0.216053 |
| training ingestion | 11.055786 | 8.480638 |
| benchmark discovery | 23.284896 | 23.161519 |
| benchmark path selection | 34.780518 | 31.743907 |
| benchmark evidence inspection | 295.488740 | 192.379126 |
| aggregation／output serialization | 129.191645 | 34.763191 |
| final output hashes | 106.463038 | 9.261872 |

下一個可驗證的優化是減少輸出後的重讀。寫出時 SHA 候選須使用凍結的
實際程式依賴、相同全量輸入及雙順序完整比較，保留獨立 reader 重新雜湊。
106.463 秒是這次全部 output hash 階段的觀測，不是候選可全數消除的時間；
atomic JSON 仍保留 full-read fallback。所有工作流已測完及硬體極限仍未證明。

- [R85 原 full 失敗紀錄](../artifacts/operations/agent-workflow/runs/workflow-r85-full-native-workflo-20261024T020858-5a7064f3/run.json)
- [R87 執行／觀測來源分類修正與衍生 SHA](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round87/source_role_amendment.json)
- [R87 完整流程與獨立原版 oracle](../artifacts/benchmarks/workflow_speed_optimization_20261021/round87/run_full_native_workflow.py)
- [R87 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r87-full-native-workflo-20261024T021516-28b598b9/run.json)
- [R87 完整驗收、各段計時與所有 oracle 結果](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round87/full_acceptance.json)
- [R87 全部 13 階段原始計時](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round87/collection_full/collection.json)

## R89：凍結實際程式後重測輸出 SHA

R88 的準備程序誤把 mount shell SHA 填成 `d_guard.py` SHA，在任何 cohort
寫入前拒絕；原 failed／exit 1 保留。R89 僅改 cohort 路徑與已分別核對的
Python guard SHA，沒有改掛載、namespace 或容量門檻。準備 owner 已成功／
inactive／MainPID 0，**90.168329 秒 wall／14.231548 秒 descendant CPU**。

R89 封存 **19 份 source／native cache files，共 564,971 bytes**，亦封存
215,504,512-byte 固定輸入清單；比較程序本身從 D 上的封存檔執行。每個
trial 及新程序 reader 都從同一份 capsule import，核對實際載入的 project
modules 路徑與 SHA，拒絕從工作目錄載入。兩組都用 R85 已驗收的正式 C++
batch，只改 CSV／JSONL 寫出時計算 SHA；JSON fallback 和獨立 reader 的
重新雜湊完整保留。兩個 serializer 的 AST 與 R82 已驗收 helper 對應版本一致。

128-input smoke 含全部 107 份 explicit receipts，兩組六份輸出語意及完整
輸入身分讀回均通過；這不是速度選擇。02:55:02 UTC 由 canonical owner
啟動，02:55:38 UTC 實查已進入全量第一輪。固定 **372,550 inputs／107
explicit receipts**，八輪順序仍為 ABBA／BAAB，每半完整 child command
（包括全部輸出與獨立 reader）wall ≤ 0.98、CPU ≤ 1.02 才接受。
來源 SHA、CPU、wall、peak RSS、block I/O、序列化與重讀耗時分別保留。
04:34:34 UTC 全八輪已完成，owner succeeded／exit 0，實查 inactive／MainPID 0。
每輪全部 child command、六份輸出語意及獨立讀回皆通過，包括完整
372,550 個所選輸入身分。每輪的 16 個實際 project imports 均來自封存 capsule。
以下是完整結果；不與 R69／R78 的不同 cohort 相除。

| 全量輪次 | 方案 | complete child wall 秒 | descendant CPU 秒 |
| --- | --- | ---: | ---: |
| 0 | baseline | 859.659471 | 350.204922 |
| 1 | candidate | 608.211128 | 271.248287 |
| 2 | candidate | 585.433262 | 248.102319 |
| 3 | baseline | 671.717545 | 253.788847 |
| 4 | candidate | 559.987961 | 225.182098 |
| 5 | baseline | 495.570262 | 204.025124 |
| 6 | baseline | 495.486542 | 214.538486 |
| 7 | candidate | 502.988349 | 209.304522 |

| 固定順序半組 | baseline 平均 wall 秒 | candidate 平均 wall 秒 | wall 比 | CPU 比 | 原門檻 |
| --- | ---: | ---: | ---: | ---: | --- |
| ABBA | 765.688508 | 596.822195 | 0.779458 | 0.859861 | 通過 |
| BAAB | 495.528402 | 531.488155 | 1.072569 | 1.038042 | 拒絕 |

同一 baseline 的 inspection 在第 0／3／5 輪分別為
353.111593／257.360416／175.498383 秒，顯示與候選 SHA 無關的工作段也在變動。
後半組 candidate 的完整 child wall 增加 **7.26%**、descendant CPU 增加
**3.80%**，沒有通過原先兩半各自 wall ≤ 0.98／CPU ≤ 1.02 的門檻。
**候選拒絕，寫出時 SHA 不採用**；不改門檻、不刪負結果，也不以階段雜湊
變快取代整段判定。

全部六份輸出的語意 digest 相同：2,006 samples、722 groups、705,929
reported metric rows、372,550 receipts 及兩份完整 JSON。跨 trial 的額外
語意 digest 在 child 計時之外執行，完整 canonical run 仍記錄這段成本：
**5,967.699373 秒 wall／2,352.781278 秒 descendant CPU**。這是完整比較成本，
不是單輪速度；本次也不證明完整 13 階段加速或硬體極限。

R90 已於 04:35:34 UTC 成功完成六套共 290 項測試／26 個新案例及 R53
14,366 JSON rows／953 workers 的完整讀回。R92 於 04:57:33 UTC 完成全
13 階段、原版 oracle 與額外 CEX 全列驗收，完整執行區間 1,287.066609 秒。
兩者與 R89 不同驗收範圍，不將其成功視為 SHA 候選通過。
profile `80b3304d…`／新測試 `3bb57f5d…` pins 均保持原樣。

遠端既有正式訓練的少量末端 metadata 已由
[R91 獨立讀回](workflow_speed_optimization_2026-10-24_remote_reuse.md#r91-0329-utc小型完成-metadata-與末端成本)：
三例已有 owner 完成標記，第四例仍在訓練。這次有界觀測不替代完整 GPU
生命週期驗收，也沒有與此全量比較並跑新的重型測速工作。

- [R88 原準備失敗](../artifacts/operations/agent-workflow/runs/workflow-r88-freeze-output-hash-20261024T025138-f2f325d1/run.json)
- [R89 凍結來源、scope 與事前判準](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round89/protocol.json)
- [R89 實際封存比較程序](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round89/orchestration.py)
- [R89 smoke 完整兩組輸出 parity](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round89/smoke_completion.json)
- [R89 八輪結果、完整語意與原門檻判定](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round89/comparison.json)
- [R89 全量比較 owner](../artifacts/operations/agent-workflow/runs/workflow-r89-frozen-sha-full-20261024T025502-9e18cda8/run.json)

## R93：保留準備證據，依 R89 負結果拒絕採用

R93 沿用 R84 已保留的 `workflow_output_hash.py` 與 16-case 測試來源，並
只抽取原 R83 的 `replace`／`hash_profile` 函數，把候選機械套至目前 CEX
profile 的私有副本。**沒有修改 canonical source**，R90／R92 的
`80b3304d…` pin 不變。46 個原有函數／類別 AST 保持原樣，只改
`write_csv`、`json_lines` 與 `collect`；三個 CEX 函數、所有資料解析、
independent reader 與來源綁定 body 都未變。七個原始測試 body／16 個
parameterized cases 保留；這份 canonical 適配尚未執行語意測試。

| 候選檔案 | SHA-256 |
| --- | --- |
| `scripts/profile_workflow_speeds.py` | `d22a0d79703c1244a74353e45b025f4120786911d1ed5e64b193d0c20ea5a6c7` |
| `stockagent/workflow_output_hash.py` | `b9f21da101eee9f98460ebee17f7a2ffc320c0ab2698f6d441f314728a5c0ceb` |
| `test/test_workflow_output_hash.py` | `6cc4ce4009c4f0eb9649f69f8f4964407143a26f3b9c68fbae6f126d98137045` |

保留補丁 SHA 為
`9c390212782a8c69da1746e34ad4d88680d042edd1baf6a8dd61b1c857886652`。
準備 owner 已 succeeded／exit 0、實際 inactive／MainPID 0，完整 child
**3.381759 秒 wall／0.848773 秒 descendant CPU**；這是準備成本。
事前條件要求 R89 全八輪的兩個順序半組均通過；實際 BAAB 失敗，因此
**這份補丁拒絕採用**。canonical helper／測試未加入，正式 profile 也沒有套用
此 SHA 候選。原準備與補丁保留供稽核，不因 R90／R92 的獨立驗收而翻轉此判定。
沒有自動套用或重啟服務。

- [重用原 builder 與候選的準備程序](../artifacts/benchmarks/workflow_speed_optimization_20261021/round93/prepare_current_hash_candidate.py)
- [R93 檔案身分、AST 範圍及採用條件](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round93/preparation.json)
- [已拒絕、保留的精確補丁](/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts/benchmarks/round93/adoption.apply_patch)
- [R93 原 supervisor 收據](../artifacts/operations/agent-workflow/runs/workflow-r93-prepare-cex-hash-20261024T034306-321c863b/run.json)
