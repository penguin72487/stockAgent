# 期交所文件 GPU OCR 模組與主機卡頓診斷

後續最佳化與全量驗證見
[`ocr_gpu_optimization_2026-09-29.md`](ocr_gpu_optimization_2026-09-29.md)。
本篇保留 v1 的四頁測試觀察；不能把它延伸為所有 DPI／文件的驗收。

## 問題與計算分工

2026-09-29 22:33–22:36（Asia/Taipei）的主機有 16 個邏輯 CPU。
12 個 OCR 子程序同時辨識 300 DPI 公告，WSL load average 約 35–39；
早期連續取樣的 CPU idle 僅 3–6%，system CPU 約 53–65%。
Windows 曾量到 CPU 100%，WSL 約用掉 8.9 個核心。
磁碟當時尚餘 344 GiB，Windows 磁碟佇列及分頁讀寫沒有飽和證據。

這批工作於 22:36:33 自然完成，manifest 為 189 份、0 失敗；
後續 Windows CPU 約 51%。操作中的程序排程調整只涵蓋部分執行緒，
程序即正常退出，故不能把負載下降宣稱成完整限流 A/B 結果。
診斷原始紀錄：`/root/.local/state/system-performance/20260929/`。
另觀察到 WSLg Weston 約每兩分鐘 signal 11 重啟，尚未歸因或修復；
本次 OCR 改動不代表整台 Windows/WSL 的所有延遲原因皆已排除。

OCR 必須從來源 PDF 產生文字與位置。各步驟的成本不同：

| 步驟 | 執行位置 | 原因與實作 |
| --- | --- | --- |
| 驗證 PDF、解碼、頁面轉圖 | CPU | 沿用 PyMuPDF 與 verified archive；逐頁處理 |
| 文字偵測 Det、方向 Cls、辨識 Rec | GPU | 保留現有三個 ONNX 模型；CUDA 執行神經網路 |
| 裁切、排序、字典解碼、座標還原 | CPU | 沿用 RapidOCR，限制 OpenCV/ORT/native threads |
| 文字、座標、SHA-256、receipt | CPU/檔案系統 | 沿用原子寫入；保持來源與候選資料契約 |

GPU 加速的前提是模型運算占有足夠成本；模型載入、PDF 解碼、PNG 保存、
解碼後處理與收據仍有成本，故完整計時包含這些步驟，也分列初始化與推論。
同一 GPU 由一個程序持有三個長存 session，辨識階段沿用每批 6 個文字區塊。
不使用跨文件無界佇列。v1 的 ORT arena 仍可能保留不同形狀的配置；
後續 400 DPI 批次實際出現配置失敗，因此不能把四頁測試當成持續記憶體上限的證明。

## 實作與邊界

- 模組：[`stockagent/ocr/rapidocr.py`](../stockagent/ocr/rapidocr.py)。
- 原入口：[`scripts/extract_taifex_rule_review_candidates.py`](../scripts/extract_taifex_rule_review_candidates.py)。
- 設定：[`configs/ocr/taifex_rapidocr_cuda13_v1.json`](../configs/ocr/taifex_rapidocr_cuda13_v1.json)。
- 依賴：[`requirements-ocr-gpu.txt`](../requirements-ocr-gpu.txt)，安裝於隔離目錄。

GPU 設定預設 `--workers 1`，拒絕多個 worker 複製 CUDA session。
同使用者、同 CUDA device 的 OCR owner 有非阻塞檔案鎖。
GPU/CPU 受控執行器預設 1 條 CPU 計算執行緒、最多 4 個允許的邏輯 CPU、
nice 10。`PR_SET_THP_DISABLE` 僅作用於 OCR 本身，避免短命大型陣列引發
同步 huge-page compaction；不改主機全域 THP、swap 或 WSL 設定。

保留 FP32，CUDA EP 關閉 TF32，模型、DPI、文字閾值與版面邏輯沿用原設定。
每個 session 的 GPU arena 上限 2 GiB；這**不是**程序或整張顯示卡的總 VRAM
硬上限，CUDA context、其他配置與其他應用程式的佔用需分別觀察。

CUDA 不可用、載入後首要 provider 為 CPU、GPU OOM 或推論錯誤均不會
改用 CPU 重跑。ORT Python 的 execution-provider fallback 明確停用。
少數 Shape 等控制運算可以在 CPU；啟用 profiling 時，驗證每個模型存在
實際 CUDA node event，且 Conv/MatMul/Gemm/LSTM 沒有落到 CPU。

新輸出 fingerprint 綁定配置、模型雜湊、套件版本及模組程式雜湊；
GPU receipt 另包含 GPU 型號、Torch/CUDA/cuDNN 版本。
舊 CPU 設定的 profile/receipt 保持相容，新配置須指定新輸出目錄。
保留 `candidate_only=true`、`point_in_time_verified=false`，GPU OCR 文字
仍須依原流程檢查，不能直接當作可交易契約或正式歷史數值。

## 使用方式

從 repository root 執行。三個已保留模型的檔案路徑和 SHA-256 見配置；
缺檔或雜湊不符直接失敗，模組不會下載替代模型。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python -m pip install \
  --target artifacts/ocr/rapidocr_cuda13_v1/libraries \
  -r requirements-ocr-gpu.txt

run_fintech_python scripts/extract_taifex_rule_review_candidates.py \
  --archive artifacts/markets/tw_futures_v8_margin_preparation/all_products_source_archive_20260929 \
  --output-dir artifacts/ocr/taifex_gpu_review_v1 \
  --category contract_adjustments \
  --document-sha256-file artifacts/ocr/gpu_acceptance_20260929/source_sha256.txt \
  --rapidocr-config configs/ocr/taifex_rapidocr_cuda13_v1.json \
  --workers 1 --ocr-dpi 300 --force-ocr \
  --ocr-profile-dir artifacts/ocr/taifex_gpu_review_v1/profiles
```

上述 SHA 清單為此次接受測試的兩份真實文件。正式選取其他來源時使用
該次已驗證的精確 SHA 清單；只有明確要處理整個 category 時才省略它。
`--ocr-profile-dir` 保留節點 trace，適合驗證裝置；大量已驗證工作可省略，
仍保留 provider 與執行預算紀錄，但不得把 provider 名稱當作新的逐節點證明。
每份文件沿用 `receipt.json`，整批新增 `ocr_execution.json` 及其 manifest 雜湊。

## 驗證方法

使用既有 CPU receipt，從兩頁文件中選取 OCR 行數的中位數文件與最高行數文件，
共 2 份、4 頁、原始 CPU 181 個文字區塊。來源選取與資料位於
`artifacts/ocr/gpu_acceptance_20260929/selection.json`。

分別執行既有 CPU runtime、相同新版 runtime 的受控 CPU、受控 CUDA。
每次使用獨立程序與全新輸出目錄；不以命中舊 receipt 的耗時當作重新辨識效能。
比較完整 wall time、初始化、推論、CPU user/system time、文字順序、數字、座標、
confidence 及來源 PNG/PDF 雜湊。CUDA trace 核對模型實際執行裝置。

RTX 5070 Ti、Torch 2.13.0 / CUDA 13.0 / cuDNN 9.26 的實測：

| 路徑 | 完整 wall time | OCR 推論 | CPU user + system | 最大 child RSS |
| --- | ---: | ---: | ---: | ---: |
| 原 CPU entrypoint、ORT 1.23.2、1 worker | 78.39 s | 未獨立計時 | 78.21 s | 1.90 GiB |
| 受控 CPU、ORT 1.30.0 | 84.47 s | 81.46 s | 83.76 s | 2.35 GiB |
| 受控 GPU、ORT 1.30.0 | 16.95 s | 11.22 s | 16.56 s | 2.57 GiB |

相同新版環境的 CPU/GPU 比較，完整流程約 **4.98 倍**，CPU 計算時間
減少約 **80.2%**。GPU 初始化 2.93 秒，CPU 初始化 0.83 秒，已包含於
完整 wall time。相對原 CPU entrypoint 的完整流程約 4.62 倍。
這次沒有證明單獨限制 CPU/THP 會提速；受控 CPU 對照反而較慢，不能把
GPU 的效益歸給 CPU 排程。原 CPU 路徑也有 runtime 與初始化方式差異，
故以相同新版環境的對照判斷 GPU 效果。

四頁的 181 個文字區塊，與兩組 CPU 對照的文字、順序、數字序列、座標及
PNG SHA-256 均一致，confidence 最大差異 0.00002。
三組的 PDF、檔案清單、worklist、execution receipt 與 profile 雜湊均通過驗證。
這是樣本的 CPU/GPU 一致性，不是人工標註準確率或全量文件一致性證明。

CUDA profile 的實際 node event：Det 1,024、Rec 7,623、Cls 5,907。
Rec 另有 66 個 CPU 控制節點，為處理長度 1–4 的 int64 shape 向量的
Slice/Concat；沒有 CPU Conv/MatMul/Gemm/LSTM。
ORT 1.30 初始化有 plugin-device 查找警告，但實際傳統 CUDA EP 建立成功，
provider、節點執行與輸出一致性均另外驗證，未憑警告或 provider 名稱判定成功。

測試時曾觀察整張卡使用約 10.9 GiB，包含 Windows 桌面與其他 GPU 程序，
不能當作 OCR 本身的 VRAM 峰值。RSS 為單一 child 最大值，並非原多程序
總和；小樣本 GPU 的主機 RSS 比單個 CPU worker 高，未宣稱單程序記憶體降低。
主機還有其他工作，此次為短樣本接受測試，未驗證整批 189 份的吞吐或持續桌面延遲。

真實 subprocess 的三個失敗檢查皆通過：無 CUDA 時退出、第二個 GPU owner
被拒絕、CPU 配置不能恢復 GPU 產物且原 manifest 雜湊不變。
最終對照直接繼承 shell 環境，沒有由 benchmark 另外設定 CPU thread 變數。
模組在依賴初始化前自行套用預算；受控 CPU 實際共 3 個執行緒，全部
affinity 12–15、nice 10，`THP_enabled=0`，OpenCV/native 計算執行緒設定為 1。
入口會在 collector/PyMuPDF 等可能載入 NumPy 的依賴之前選定隔離套件，
避免套件 metadata 和已載入的二進位模組不一致；主訓練環境仍為 NumPy 2.4.6。

最終範圍測試為 **166 passed、2 skipped**，涵蓋新模組的預算、來源完整性、
provider/程序擁有權與 profile 契約，以及既有公告解析、規則歷史與覆蓋率測試。
跳過項目保留於測試記錄，沒有視為通過；實際 CUDA 推論另以上述來源樣本驗證。

可檢查的證據：

- [`comparison.json`](../artifacts/ocr/gpu_acceptance_20260929/comparison.json)：
  耗時、來源與八組逐頁對照。
- [`gpu_final/ocr_execution.json`](../artifacts/ocr/gpu_acceptance_20260929/gpu_final/ocr_execution.json)：
  實際 CPU 預算、GPU runtime 與三個模型的 profile。
- [`failure_gates.json`](../artifacts/ocr/gpu_acceptance_20260929/failure_gates.json)：
  真實 CLI 失敗檢查。
- [`run_benchmark.py`](../artifacts/ocr/gpu_acceptance_20260929/run_benchmark.py)：
  調用原入口的測試 driver；每次要求新目錄。
- [`installation.json`](../artifacts/ocr/gpu_acceptance_20260929/installation.json)：
  隔離依賴下載來源及雜湊。
- [`focused_tests.log`](../artifacts/ocr/gpu_acceptance_20260929/focused_tests.log)：
  最終範圍測試結果。

技術依據：
[ONNX Runtime CUDA EP](https://onnxruntime.ai/docs/execution-providers/CUDA-ExecutionProvider.html)、
[RapidOCR 原始實作](https://github.com/RapidAI/RapidOCR)、
[Linux per-process THP 控制](https://docs.kernel.org/admin-guide/mm/transhuge.html#process-thp-controls)。
