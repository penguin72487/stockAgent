# 期交所 OCR：GPU 資料搬移、精確前處理與高解析度預算

本次針對保留的期交所公司行動公告 PDF：繁體中文、契約代號、日期、
金額／股數及表格位置。保留 PP-OCRv5 偵測／辨識權重、方向分類器、
原始 PDF、PNG、原生文字與每個文字區塊的座標；OCR 仍為候選資料。

## 從完整路徑量測

來源驗證 → PDF 轉圖 → 偵測前處理 → Det → 裁切／方向分類 → Rec →
字典解碼 → 文字／座標／PNG 保存 → 雜湊與收據。

先前的四頁 CPU/GPU 測試不能代表全量正確率。本次保留舊執行器快照，
以相同來源重新抽取 189 份、447 頁，並以新流程逐頁核對；測試不得
藉命中舊收據、降低 DPI、刪除輸出或跳過困難文件縮短時間。

初步診斷的 5 份、13 頁包含密集表格、四頁附件與高解析度失敗文件：

| 階段 | 舊 GPU 路徑 |
| --- | ---: |
| 偵測前處理 | 10.293 秒 |
| 偵測模型 | 4.350 秒 |
| 偵測後處理 | 0.540 秒 |
| 方向模型 | 1.916 秒 |
| 辨識模型與輸出搬移 | 13.825 秒 |
| CTC 解碼 | 0.589 秒 |

這次量測顯示偵測的主要 CPU 成本其實是前處理，不能只看整個 Det
階段就把問題歸因於輪廓後處理。原辨識器在這 13 頁回傳
3,108,829,960 bytes 的完整機率；解碼器只用每個字位的最大值與索引。
樣本內部完整抽取計時由 39.37 秒降至 23.12 秒，517 個區塊的文字、
順序、座標與 confidence 完全相同；此計時含模型初始化及輸出，
不含 Python 啟動與 bootstrap 依賴驗證。

## 實作

- [`fastpath.py`](../stockagent/ocr/fastpath.py)：精確 uint8 查表、GPU
  輸入、CTC 縮減輸出與依文字寬度限制的小批次。
- [`rapidocr.py`](../stockagent/ocr/rapidocr.py)：單一 GPU owner、獨立
  模型記憶體預算、可量測的 session 包裝、裝置與來源驗證。
- [`build_ocr_detector_lut.py`](../scripts/build_ocr_detector_lut.py)：
  從原偵測模型衍生 uint8 NHWC 輸入與精確 normalization LUT。
- [`build_ocr_compact_ctc.py`](../scripts/build_ocr_compact_ctc.py)：
  從原辨識模型衍生 ArgMax、ReduceMax 與 NaN 檢查輸出。
- [`benchmark_taifex_ocr.py`](../scripts/benchmark_taifex_ocr.py)：執行原
  抽取入口，記錄完整程序耗時、CPU 時間、RSS、GPU 裝置取樣及逐頁比對。

### 精確前處理

RapidOCR 先把 uint8 像素乘上 float32 比例，再與 float64 mean/std
運算，最後轉回 float32。每個通道只有 256 種輸入，因此可先算完
768 個輸出，避免對整張高解析度頁面建立多個 float64 暫存陣列。
CPU 路徑使用 OpenCV LUT；GPU 路徑保留原空間縮放，傳送 uint8 NHWC，
在 ONNX 圖中查表並轉成 NCHW。不得改寫成數值近似的 fused arithmetic。
GPU 模型的 LUT 雜湊必須與當次原生 normalization 的逐值結果一致。

### CTC 輸出縮減

保留完整字典、softmax 與原生 CTC decoder；不刪罕見中文字、不套用
推測的日期或金額。GPU 只回傳每個字位的 int64 索引、float32 最大機率
及 NaN 狀態，避免搬回 18,385 個字元的機率向量。
重複字、blank、同值最大值的第一個索引與字框解碼仍沿用原規則。
附加的 NaN 檢查避免 reduction 掩蓋非有限值。
算子契約見 [ONNX ArgMax](https://onnx.ai/onnx/operators/onnx__ArgMax.html)；
GPU/CPU 資料搬移機制見 [ORT I/O Binding](https://onnxruntime.ai/docs/performance/tune-performance/iobinding.html)。

### 高解析度與記憶體

既有 400 DPI 批次在 Rec batch 6、2 及縮小偵測尺寸後仍出現 arena
配置失敗。新的寬度預算在原生 batch 已完成補白之後才切分；每行的
補白寬度、順序與輸入像素保持一致。單純修改 `Rec.rec_batch_num`
會連帶改變補白寬度，兩者不能視為同一種優化。

可分別設定 Det／Rec／Cls arena 上限、關閉動態形狀 memory pattern，
並透過 run options 回收 arena；這些選項必須連同速度實測。
依 [ORT arena shrinkage 契約](https://github.com/microsoft/onnxruntime/blob/main/include/onnxruntime/core/session/onnxruntime_run_options_config_keys.h)
回收 GPU pool，並不代表限制整張 GPU 的總記憶體；CUDA context、cuDNN
與其他 Windows 程序仍占用額外空間。GPU 失敗不會改用 CPU。
[cuDNN workspace 選項](https://onnxruntime.ai/docs/execution-providers/CUDA-ExecutionProvider.html)
亦須比較冷啟動及完整批次，不能只報單次模型 forward。

實際採用 CPU 精確 LUT、GPU CTC 縮減、原生補白後的寬度預算 12,288，
並在每頁各模型的最後一次呼叫回收 arena。保留 memory pattern；
逐小批次回收、GPU normalization、擴大 cuDNN workspace 在本機測試
均未優於此組合。12,288 高於舊 GPU 全量 3,025 次 Rec 呼叫的最大
`batch × padded_width = 11,442`，因此 300 DPI 對照不需要改變原批次。
較寬的高解析度輸入才切分 batch，單行本身不縮放。

短樣本調參的內部完整抽取計時（不含 Python 啟動，並非以下正式程序 wall time）：

| 路徑 | 秒 | 與舊 GPU 文字／座標 |
| --- | ---: | --- |
| 原路徑 | 39.37 | 參考 |
| CPU 精確 LUT＋compact CTC＋直接像素，無 arena 回收 | 23.11 | 相同，confidence 也相同 |
| 改為 GPU LUT，其餘相同 | 30.20 | 相同 |
| GPU LUT＋每次推論回收 | 36.52 | 相同 |
| 再限制辨識寬度 4,096 | 31.47 | 相同，confidence 最大差 0.00001 |
| 再允許較大 cuDNN workspace | 34.71 | 相同，confidence 最大差 0.00001 |
| TF32 候選 | 49.37 | 7 頁座標不同 |

表內無回收版本只是速度診斷；v2 額外保留按頁回收與較大的 Det 預算，
以驗證高解析度的持續記憶體需求。此處是本機、這組模型與來源的結果，
不是所有硬體的最佳參數，也未宣稱探索過所有可能的實作。

### BF16 實測

RTX 5070 Ti 的本機 CUDA 檢查支援 BF16。現有 ONNX 模型為 FP32，
PyTorch autocast 不會自動改變 ORT session 的計算精度。
新增 [`build_ocr_bf16_candidate.py`](../scripts/build_ocr_bf16_candidate.py)，
將可辨識為 FP32 的 Conv／MatMul 轉為 BF16；正規化、其他算子、
softmax、CTC 與外部 I/O 保留 FP32。這是混合精度候選，並非全圖 BF16。
原權重檔不變；衍生圖保留來源雜湊、轉換節點清單與獨立模型輸出。
BF16 的風險是模型內部四捨五入改變字元機率或偵測閾值，進而改變
輸出的字元／字框；並不是把辨識後的金額文字存成 BF16。

[ONNX Conv schema](https://onnx.ai/onnx/operators/onnx__Conv.html) 的
BF16 型別需要 opset 22，因此另外產生只升級 opset 的 FP32 對照，
避免把版本轉換差異歸因於精度。CUDA trace 實際記錄 BF16 Conv／MatMul
輸入與 CUDA provider，沒有 CPU 神經網路算子。

同一組 5 份／13 頁／FP32 517 個區塊，皆含啟動、輸出與 profiling：

| 候選 | 完整程序耗時 | 文字不同頁數 | 座標不同頁數 | 數字序列不同頁數 |
| --- | ---: | ---: | ---: | ---: |
| FP32 opset 22 對照 | 29.21 秒 | 0 | 0 | 0 |
| Det／Cls／Rec BF16，原 Tensor Core 開關 | 75.54 秒 | 12 | 13 | 9 |
| Det／Cls／Rec BF16，允許 Tensor Core | 51.89 秒 | 12 | 13 | 9 |
| 僅 Rec BF16，原 Tensor Core 開關 | 45.99 秒 | 7 | 0 | 2 |

ORT 的 cuDNN frontend 在 `use_tf32=false` 時會排除 Tensor Core plan；
BF16 初版因此反覆嘗試替代 plan。允許 Tensor Core 後沒有相同重試訊息，
但仍未超過 FP32，且數值差異未消失。此選項也可能影響剩餘 FP32 算子，
故明確記錄配置，不能把 BF16 候選稱為純 BF16。
技術依據為 [ORT CUDA Conv 實作](https://github.com/microsoft/onnxruntime/blob/main/onnxruntime/core/providers/cuda/nn/conv.cc)。

只降低 Rec 精度時，一個低 confidence 區塊由 `35` 變成 `3`，
另一個月份行少了一個 `0`；這是與 FP32 的差異，並非人工真值準確率。
這批模型的前處理／搬移成本、頻繁形狀變化與精度轉換成本，使 BF16
算力峰值不足以代表完整流程速度。此次不採用 BF16 或 TF32 為預設；
也不把此混合精度轉換的結果概括為所有 BF16 OCR 實作都較慢。
完整差異及節點型別見
[`bf16_comparison.json`](../artifacts/ocr/gpu_optimization_20260929/bf16_comparison.json)。

### 來源與品質邊界

原模型不改寫。衍生模型保留 source SHA-256、recipe、LUT／字典與建置
收據；執行 fingerprint 另綁定設定、依賴、模組及原抽取器程式碼。
直接用 PyMuPDF 像素進行辨識，但仍保存相同 PNG，避免寫檔後再次讀取
與解壓縮。新設定使用獨立輸出目錄，不能混入舊收據。

舊 GPU 與舊 CPU 的全量差異必須另外列出。447 頁中 26 頁座標不同、
8 頁文字不同、5 頁數字 token 序列不同；數字序列差異可能包含排序或
標點差異，不可直接當成 5 頁數字錯誤。已觀察到一份榮化公告的
契約月份行在舊 GPU 為低 confidence 誤讀；與舊 GPU 完全相同只證明
優化未新增差異，不等於文字正確。不得把效能驗收或候選辨識結果
提升為正式歷史資料、PIT 證明或可交易契約。
逐行覆核清單保留於
[`cpu_gpu_review_differences.json`](../artifacts/ocr/gpu_optimization_20260929/cpu_gpu_review_differences.json)。

## 驗收紀錄

完整證據根目錄：`artifacts/ocr/gpu_optimization_20260929/`。
2026-09-29 至 09-30，RTX 5070 Ti 16 GB、WSL、16 個邏輯 CPU；
Torch 2.13.0／CUDA 13.0／cuDNN 9.26，隔離 ORT 1.30.0。
189 份、447 頁的 300 DPI 完整重新抽取，包含啟動、模型初始化、
轉圖、PNG、文字與座標、雜湊及收據；不計收據命中。驗證程式的事後
逐頁比較不列入兩組抽取計時。

| 指標 | 舊 GPU 執行器快照 | v2 |
| --- | ---: | ---: |
| 完整程序 wall time | 1,176.61 秒 | 792.64 秒 |
| CPU user＋system | 1,134.47 秒 | 735.97 秒 |
| 最大 child RSS | 3,313,352 KiB | 3,029,692 KiB |
| 偵測前處理 | 396.43 秒 | 128.26 秒 |
| 辨識模型與回傳 | 398.55 秒 | 344.00 秒 |
| 辨識後處理 | 17.40 秒 | 1.56 秒 |
| 辨識器回傳陣列資料量 | 93,850,571,360 bytes | 15,326,308 bytes |

完整耗時減少 **32.63%**，吞吐約 **1.48 倍**，CPU 計算時間減少
**35.13%**。CTC 的回傳陣列縮減 **99.98%**；這是 Rec 輸出，並非整個
OCR 的 PCIe 流量或 GPU 配置量，Det 輸出等仍有其他搬移。

189 份全部完成、0 失敗。16,349 個 GPU 參考區塊的文字、順序、數字序列、
座標與 confidence 全部相同；confidence 最大差為 0。來源 PDF、PNG、
原生文字、candidate.txt 及每份 receipt 的檔案雜湊均通過比對；
文件集合與頁數無缺漏。舊 CPU 的區塊數為 16,350，與 GPU 的既有差異
保留於獨立覆核清單，沒有將其算成 v2 的一致性。

證據：[`full_comparison.json`](../artifacts/ocr/gpu_optimization_20260929/full_comparison.json)、
[`final_full/benchmark.json`](../artifacts/ocr/gpu_optimization_20260929/final_full/benchmark.json)、
[`ocr_execution.json`](../artifacts/ocr/gpu_optimization_20260929/final_full/extraction/ocr_execution.json)。
舊模組、當次程式碼快照與檔案 SHA 保留於 `before/`、`revisions/final/`
及 `implementation_snapshot.json`。

測試期間主機另有 Windows GPU 與資料下載工作；這是同機依序執行的
實測，非獨占 GPU 的統計估計。v2 裝置取樣為整張卡 7,892–11,439 MiB，
不能稱為 OCR 程序的私人 VRAM 峰值。實際 OCR affinity 12–15、nice 10、
OpenCV/native 計算執行緒 1；不代表已驗證整台 Windows／WSL 的互動延遲。

高解析度驗收：2 份曾失敗文件、7 頁、400 DPI 已完成，16.62 秒。
CUDA profile 的 Det／Rec／Cls node event 分別為 1,792／11,564／8,771；
沒有 CPU Conv／MatMul／Gemm／LSTM。Rec 的 ArgMax 49 次、ReduceMax
98 次均由 CUDA 執行。此結果確認實際 GPU 計算，並非只看 provider 名稱。
完整 44 份／113 頁／4,176 個區塊的 400 DPI 壓力測試也全部完成，
**0 失敗、195.13 秒**。來源集合、全部頁數、每份 receipt 的檔案雜湊、
有限 confidence／座標均驗證通過。保留原有 DPI、Det 尺寸設定及權重，
沒有使用另一批重試所採用的較小偵測尺寸。

此批最大 `batch × padded_width = 11,820`，未觸發 12,288 的切分上限；
所以不能把成功歸因於文字切分。實際接受的是獨立模型預算與按頁回收
的組合；微批次保護另由單元測試及較小預算樣本驗證。
這是指定來源的記憶體穩定性驗證，不是所有未來 PDF／解析度皆不會 OOM
的保證，也不是 400 DPI 人工標註正確率或不同 DPI 之間的文字一致性證明。
詳見 [`high_dpi_acceptance.json`](../artifacts/ocr/gpu_optimization_20260929/high_dpi_acceptance.json)。

基礎環境的範圍回歸測試為 **194 passed、7 skipped**；使用實際 OCR
隔離套件環境補跑相同完整範圍，結果為 **201 passed、0 skipped**，
涵蓋所有 uint8 正規化值、CTC
重複字／blank／相同最大值／NaN、原補白與順序、小批次回收邊界、
模型來源、裝置擁有權、精度轉換及差異比對失敗條件。PyCompile 與
`git diff --check` 通過。測試明細見
[`test_validation.json`](../artifacts/ocr/gpu_optimization_20260929/test_validation.json)。

## 使用 v2

[`taifex_rapidocr_cuda13_v2.json`](../configs/ocr/taifex_rapidocr_cuda13_v2.json)
指向獨立隔離依賴與保留模型，不修改主訓練環境。v1 配置與既有候選資料
保留原樣；v2 使用新的輸出目錄及 fingerprint。建立衍生 CTC 模型時需
ONNX（本次為 1.23.0），推論仍使用原隔離套件，不在執行時下載模型。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python scripts/build_ocr_compact_ctc.py \
  --config configs/ocr/taifex_rapidocr_cuda13_v1.json \
  --output artifacts/ocr/gpu_optimization_20260929/models/ch_PP-OCRv5_rec_server_ctc_v1.onnx

run_fintech_python scripts/extract_taifex_rule_review_candidates.py \
  --archive artifacts/markets/tw_futures_v8_margin_preparation/all_products_source_archive_20260929 \
  --output-dir artifacts/ocr/taifex_gpu_review_v2 \
  --category contract_adjustments \
  --document-sha256-file artifacts/ocr/gpu_optimization_20260929/full_selection.txt \
  --rapidocr-config configs/ocr/taifex_rapidocr_cuda13_v2.json \
  --workers 1 --ocr-dpi 300 --force-ocr
```

此 SHA 清單限定本次 189 份來源。選取其他文件時另建精確 SHA 清單；
勿省略後誤跑目前 archive 的整個分類。另可用 `benchmark_taifex_ocr.py`
的 `--config`、`--selection`、`--output-dir`、`--reference` 重做完整計時與比對，
每次要求全新輸出目錄。
