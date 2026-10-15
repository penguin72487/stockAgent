# 全資料面板內部清冊編碼最佳化

全資料面板的每 30 秒更新包含約 123 MB 的本機 footer 清冊。新增的原生
編碼器已由原排程載入，兩輪完整清冊重播的中位耗時分別下降 15.3% 與 29.2%。
每輪都涵蓋 63,376 個來源路徑，全部輸出內容一致。公開 JSON、原始資料、
交易帳本、GPU 推論與抓取配額不變；全服務最佳化目標仍持續進行。

## 實測結果

每輪固定同一組真實 footer 清冊及 stat／缺席觀察，交錯執行三組 ABBA，
共 12 次。兩輪各有獨立來源快照，不能把不同世代的 hash 當成同一份資料。
主機上仍有抓取、備份與其他工程工作，倍率只適用這兩輪的受測範圍。

| 完整受測操作 | 原版 | 新版 | 降幅 |
| --- | ---: | ---: | ---: |
| 第一輪清冊更新牆鐘中位數 | 4.414 秒 | 3.737 秒 | 15.3% |
| 第一輪 CPU 中位數 | 4.411 秒 | 3.737 秒 | 15.3% |
| 第二輪清冊更新牆鐘中位數 | 5.179 秒 | 3.669 秒 | 29.2% |
| 第二輪 CPU 中位數 | 5.173 秒 | 3.658 秒 | 29.3% |

受測操作包含完整清冊解碼、來源身分核對、聚合、delta 與原子 cache／index
寫入；來源 stat 是固定觀察重播，footer refresh 預算為零。它不包含 imports、
目錄發現、真實來源 I/O、異動 footer 解碼、特徵發布及瀏覽器顯示，不能把
此倍率當成整個 snapshot service 或下載器的端到端倍率。

初次正式觀察中，每輪總 CPU 約 10～15 秒，除了編碼仍有來源掃描、聚合、
FinLab 狀態及特徵投影成本。新版正式週期仍可見約 10～14 秒的不同工作量；
來源未變時約 4 秒的整包重用並非新編碼器造成，不能混入 A/B。
benchmark 的 RSS 是同一 ABBA 程序高水位，不提供兩版分別的記憶體降幅。

## 型別驗證與相容性

既有原生解碼已在使用；新增的
[專用編碼器](../stockagent/live/data_monitor_record_codec.py) 僅服務內部 footer 清冊。
使用目前已安裝的 msgspec 0.22.0，沒有升級 runtime 或更換公開端點的 encoder。

完整型別驗證先確認已知 metadata 結構，所有數值目標只接受整數，不接受
float 或 bool 代替 count；每層 object 都拒絕未知欄位。驗證副本只用來確認型別，
實際編碼原物件，因此不加入預設欄位、不丟掉原欄位，也不改原 dictionary 順序。
官方 [convert 文件](https://msgspec.dev/converters) 說明其驗證與 strict 模式。

這個保護很重要：原生 JSON encoder 會把非有限 float 編成 null，而且 Unicode
採 UTF-8 而非 ASCII 轉義，見 [官方型別映射](https://msgspec.dev/supported-types)。
任一舊格式、未知欄位、float、未配對 surrogate 或原生相依缺失，都沿原 stdlib
writer，保留既有數值與錯誤。任意大整數維持 Python int；cycle、bytes、date
等原本不能編碼的物件仍失敗，不偷偷轉成別的內容。

內部 JSON wire bytes 明確改為 UTF-8，兩版檔案 SHA 不同；第一輪大小為
123,120,754 與 122,991,127 bytes，差異僅約 0.1%，不是金融來源壓縮成果。
每輪 12 次完整解碼後的內容 SHA 相同，所有來源身分均有核對，程式 hash
在各輪內未變。清冊版本仍為 reader-compatible v10；公開 wire bytes 不改。

新增 `cache_encode`、`cache_write` 分段計時與 `inventory_cache_encoding`
診斷，未變更 30 秒 timer，也未移除五項來源身分、coherence、footer 或發布檢查。
服務由原 oneshot 自然載入，不重啟 gateway、交易引擎或夜盤擷取。

## 驗收與證據

300 項完整共享清單回歸通過，涵蓋 source revision、會員清冊 delta、feature
shards／pages、JSON 精度與原子發布；先前 39 項是其子集合，不另相加。
另修正原 HEAD 已是 frontend v15、測試卻仍期待 v14 的既有 literal 不符；
沒有修改網站資產或排除該測試。修改範圍 Ruff 與 git diff --check 通過。

- [第一輪 12 次完整重播](../artifacts/benchmarks/running_services_20261013/record_inventory_encode_final_v2.json)
- [第二輪 12 次及個別 source 與 trial-seed hash](../artifacts/benchmarks/running_services_20261013/record_inventory_encode_source_bound.json)
- [最終 300 項回歸](../artifacts/operations/agent-workflow/runs/record-inventory-final-contract--20261013T170537-3a6725b7/run.log)
- [正式排程讀回與載入版本](../artifacts/operations/agent-workflow/runs/record-inventory-formal-owner-re-20261013T165918-e4f30f42/run.log)
- [正式清冊完整舊 reader 相容性](../artifacts/operations/agent-workflow/runs/record-inventory-live-reader-acc-20261013T165603-94d0edf3/run.log)
- [五模式盤後再驗收](../artifacts/operations/running-services-optimization-20261013/postclose-after-monitor-encoder-acceptance.json)
- [本輪服務清點](../artifacts/operations/running-services-optimization-20261013/continued-services-baseline-20261014.json)

第一輪歷史收據的 `input_sha256` 是 captured source 的 SHA，而 `input_bytes`
是重編碼 trial seed 的大小；第二輪已分開標示並完整重驗證，原收據保留。

正式清冊讀回綁定穩定檔案身分，stdlib 與既有 native decoder 的完整內容 SHA
相同；這是格式與當下 footer coherence 驗收，不是歷史資料完整或 PIT 證明。
五個台股模式再次正式讀回，零模型前推、CUDA 未初始化、零 provider 呼叫及
交易寫入。工程失敗與較早收據保留，不替換成成功紀錄。

## 重現與剩餘工作

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_data_monitor_record_codec.py \
  --mode encode --repeats 3 --output artifacts/benchmarks/NEW_RECORD_ENCODING_RESULT.json
```

output 必須是新檔；source capture 使用當下觀察，不保證下一輪硬體負載或
來源內容相同。`--mode decode` 仍只比較原生解碼，固定原 stdlib writer。
第二輪明確分開 captured source 的 UTF-8 SHA 與 ASCII-escaped trial seed
的 SHA／bytes，不將不同 JSON 表示誤當同一個 byte identity。

後續優先量測 FinMind complement 的完整實際工作、清冊剩餘掃描及特徵發布。
OpenBB 的 611 個來源歸屬異常、Bybit 失效 alias 與其他服務的端到端驗收仍
未完成；不得以減少狀態更新、略過檢查或停止必要工作代替優化。全服務現況
與其他階段見 [主報告](running_service_optimization_2026-10-13.md)。
