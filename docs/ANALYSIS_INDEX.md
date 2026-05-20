# 📑 StockAgent 分析文檔索引與導航

**生成日期：** 2026年5月20日  
**總分析規模：** 15,000+ 詞 | 11 個主要問題 | 8 小時優化計劃

---

## 🚀 快速開始 (3 分鐘)

### 👤 不同角色的閱讀路線

#### **👨‍💼 項目經理 / 決策者**
1. 閱讀本文件（5 分鐘）
2. 查看 [EXECUTIVE_SUMMARY.md](./EXECUTIVE_SUMMARY.md) - 快速概覽（5 分鐘）
3. 檢查 [PERFORMANCE_DIAGNOSTICS.md](./PERFORMANCE_DIAGNOSTICS.md) - 可視化（5 分鐘）

**要點：**
- 11 個問題，5 個 Critical
- 預期改進：第一週 +20%，第一個月 +50%
- 總工作量：8-10 小時（分散在 4 週）

---

#### **👨‍💻 開發工程師（立即開始修復）**
1. 閱讀 [EXECUTIVE_SUMMARY.md](./EXECUTIVE_SUMMARY.md) - 快速參考（10 分鐘）
2. 進入 [COMPREHENSIVE_ANALYSIS.md](./COMPREHENSIVE_ANALYSIS.md) - Section "優化方案" （15 分鐘）
3. 選擇 Task 1，開始修復（30 分鐘）

**立即任務：** 修復 4 個 Task（2.5 小時）
```bash
# Task 1: 修復自動微分洩漏（30 min）
# 編輯 stockagent/training/trainer.py
# 添加 optimizer.zero_grad()

# Task 2: 驗證特徵標準化（15 min）
python verify_fixes.py

# Task 3: 集成自適應批次（45 min）
# 編輯 stockagent/training/trainer.py
# 集成 find_optimal_batch_size()

# Task 4: 添加訓練日誌（1 hour）
# 編輯 stockagent/training/trainer.py
# 使用 logging 模塊
```

---

#### **🔬 數據科學家 / 研究員**
1. 閱讀 [COMPREHENSIVE_ANALYSIS.md](./COMPREHENSIVE_ANALYSIS.md) - 全文
2. 重點關注：
   - 數據結構改進（Section 8）
   - 損失函數改進（Section 8）
   - Tier 3 研究項目（多 Fold 並行、Polars 遷移）

**研究方向：**
- Sharpe Loss 加權方案（多樣化約束）
- 多 Fold 並行訓練（Ray + 分佈式）
- 超參數自動調優（Optuna）

---

## 📚 文檔地圖

### 核心分析文檔（新生成）

```
📄 EXECUTIVE_SUMMARY.md
   ├─ 快速概覽（2000 詞）
   ├─ 立即行動 4 Task
   ├─ 第一週目標與檢查清單
   └─ 命令快速參考

📊 COMPREHENSIVE_ANALYSIS.md
   ├─ 專案架構概述
   ├─ 核心數據流
   ├─ 11 個問題詳解（分類 4 優先級）
   ├─ 套件分析
   ├─ 性能瓶頸診斷
   ├─ 優化方案（Tier 1-4）
   ├─ 數據結構改進
   ├─ 安全隱患檢查
   ├─ 行動計劃與時程表
   └─ 完整檢查清單

📈 PERFORMANCE_DIAGNOSTICS.md
   ├─ 完整數據流圖
   ├─ 批次 vs VRAM 分析
   ├─ 訓練時間預測
   ├─ 梯度流分析
   ├─ I/O 瓶頸診斷
   ├─ 特徵標準化重要性
   ├─ 訓練循環優化對比
   └─ 故障排查樹
```

### 已有文檔（參考）

```
📄 ARCHITECTURE_REVIEW.md (已存在)
   ├─ 專案架構詳解
   ├─ 數據流完整描述
   ├─ 模型架構分析
   ├─ 損失函數講解
   └─ 問題列表（已部分修復）

✅ FIXES_COMPLETED.md (已存在)
   ├─ 5 個優先修復的完成狀態
   ├─ 修復後的程式碼
   └─ 預期效果評估

🏗️ CODE_ORGANIZATION.md (已存在)
   ├─ 專案結構重構計劃（9 步）
   ├─ setup.py & pyproject.toml 模板
   ├─ 代碼風格與質量標準
   └─ 實施後的好處清單

📊 OPTIMIZATION_ANALYSIS.md (已存在)
   ├─ 第一性原理優化分析
   ├─ 10 個優化機會詳解
   └─ 性能收益估計

🎨 OPTIMIZATION_TEMPLATES.md (已存在)
   ├─ 可直接使用的代碼片段
   └─ 各模塊的優化示例

📸 OPTIMIZATION_VISUALS.md (已存在)
   ├─ 架構圖解
   └─ 性能對比可視化
```

---

## 🎯 問題快速查詢

### 按嚴重性分類

| 優先級 | 問題數 | 關鍵問題 | 修復時間 |
|--------|--------|---------|---------|
| 🔴 Critical | 5 | 記憶體洩漏、批次大小、特徵標準化 | 1.5 小時 |
| 🟠 High | 3 | 日誌、I/O、型別提示 | 4 小時 |
| 🟡 Medium | 2 | 模型邏輯、重複計算 | 3 小時 |
| 🔵 Low | 1 | 專案結構 | 4 小時 |

### 按影響力分類

| 類別 | 性能 | 穩定性 | 可維護性 |
|------|------|--------|---------|
| 自動微分洩漏 | ⭐⭐ | ⭐⭐⭐ | - |
| 批次大小 | ⭐⭐⭐ | ⭐ | - |
| 特徵標準化 | ⭐ | ⭐⭐⭐ | - |
| 日誌監控 | - | ⭐⭐ | ⭐⭐ |
| I/O 優化 | ⭐⭐ | - | - |
| 型別提示 | - | - | ⭐⭐⭐ |

---

## 📋 修復狀態矩陣

```
┌────────────────────┬─────────────┬──────────────────┬──────────────┐
│ 問題                │ 優先級      │ 修復時間         │ 驗證方法      │
├────────────────────┼─────────────┼──────────────────┼──────────────┤
│ 自動微分洩漏       │ 🔴 Critical │ 30 min           │ nvidia-smi   │
│ 特徵標準化         │ 🔴 Critical │ 15 min + 驗證    │ verify_fixes │
│ 批次大小自適應     │ 🔴 Critical │ 45 min           │ 訓練速度測試 │
│ 日誌系統           │ 🟠 High    │ 60 min           │ 訓練輸出檢查 │
│ I/O 優化           │ 🟠 High    │ 90 min           │ 初始化計時   │
│ 型別提示           │ 🟠 High    │ 2 hours          │ mypy 檢查    │
│ Transformer 邏輯   │ 🟡 Medium  │ 1 hour           │ 單元測試     │
│ 專案結構重構       │ 🔵 Low     │ 4 hours          │ import 檢查   │
│ 多 Fold 並行       │ 🔵 Low     │ 8 hours          │ 時間對比     │
│ Polars 遷移        │ 🔵 Low     │ 6 hours          │ 性能對比     │
└────────────────────┴─────────────┴──────────────────┴──────────────┘
```

---

## 🔍 問題詳細位置

### 🔴 Critical Issues

#### Issue 1：自動微分記憶體洩漏
- **文件：** `stockagent/training/trainer.py`
- **位置：** 訓練迴圈（run_training 函數）
- **分析：** [COMPREHENSIVE_ANALYSIS.md#1-自動微分記錄圖未清理](./COMPREHENSIVE_ANALYSIS.md#1-自動微分記錄圖未清理-memory-leak)
- **修復步驟：** [EXECUTIVE_SUMMARY.md - Task 1](./EXECUTIVE_SUMMARY.md#-task-1修復記憶體洩漏30分鐘)
- **診斷：** [PERFORMANCE_DIAGNOSTICS.md#7-訓練循環優化前後對比](./PERFORMANCE_DIAGNOSTICS.md#-訓練循環優化前後對比)

#### Issue 2：特徵標準化
- **文件：** `stockagent/data/panel.py`
- **函數：** `build_panel()`
- **分析：** [COMPREHENSIVE_ANALYSIS.md#4-特徵標準化缺失](./COMPREHENSIVE_ANALYSIS.md#4-特徵標準化缺失部分修復)
- **驗證：** [EXECUTIVE_SUMMARY.md - Task 2](./EXECUTIVE_SUMMARY.md#-task-2驗證特徵標準化15分鐘)
- **原理：** [PERFORMANCE_DIAGNOSTICS.md#6-特徵標準化的重要性](./PERFORMANCE_DIAGNOSTICS.md#-特徵標準化的重要性)

#### Issue 3：靜態批次大小
- **文件：** `stockagent/training/trainer.py`, `stockagent/config.py`
- **分析：** [COMPREHENSIVE_ANALYSIS.md#5-批次大小靜態配置](./COMPREHENSIVE_ANALYSIS.md#5-批次大小靜態配置)
- **修復步驟：** [EXECUTIVE_SUMMARY.md - Task 3](./EXECUTIVE_SUMMARY.md#-task-3啟用自適應批次大小45分鐘)
- **計算：** [PERFORMANCE_DIAGNOSTICS.md#2-性能分析批次-vs-vram](./PERFORMANCE_DIAGNOSTICS.md#-性能分析批次-vs-vram)

#### Issue 4 & 5：日誌、I/O
- **詳見：** [COMPREHENSIVE_ANALYSIS.md - High Priority Issues](./COMPREHENSIVE_ANALYSIS.md#-優先級high-一周內修復)

### 🟠 High Priority Issues

#### Issue 6-7：型別提示、Transformer 邏輯
- **詳見：** [COMPREHENSIVE_ANALYSIS.md - Medium Priority Issues](./COMPREHENSIVE_ANALYSIS.md#-優先級medium-本月內優化)

### 🟡 Medium Priority Issues

#### Issue 8：專案結構
- **詳見：** [COMPREHENSIVE_ANALYSIS.md - Low Priority Issues](./COMPREHENSIVE_ANALYSIS.md#-優先級low-架構優化)
- **完整計劃：** [CODE_ORGANIZATION.md](./CODE_ORGANIZATION.md)

---

## 💡 關鍵概念速查

### 數據流

**詳見：** [PERFORMANCE_DIAGNOSTICS.md#1-完整數據流圖](./PERFORMANCE_DIAGNOSTICS.md#-完整數據流圖)

簡版：
```
Parquet → Panel Cache → Walk-Forward Split → Dataset → 
Model Forward → Loss → Backward → Optimizer.step()
```

### 性能指標

| 指標 | 當前 | 目標 | 方法 |
|------|------|------|------|
| 訓練速度 | 1.0x | 1.2x | Task 1-3 |
| VRAM 增長 | ✗ 有 | ✓ 無 | Task 1 |
| GPU 利用率 | 20% | 80%+ | Task 3 |
| 初始化時間 | 10-30s | 1-2s | Task 5 |

### 優化優先級（ROI 排序）

1. **自動微分洩漏** (30 min, +5% 速度, 防止 OOM) ⭐⭐⭐
2. **批次大小自適應** (45 min, +15% 速度) ⭐⭐⭐
3. **特徵標準化驗證** (15 min, +3% 穩定性) ⭐⭐
4. **I/O 優化** (90 min, +70% 初始化) ⭐⭐
5. **訓練日誌** (60 min, 調試能力) ⭐

---

## ✅ 驗證檢查清單

### Phase 1 驗證（第一週）

```bash
# 1. 修復自動微分洩漏
✓ 編輯 trainer.py
✓ 添加 optimizer.zero_grad()
✓ 運行訓練，監控 VRAM
nvidia-smi --query-gpu=memory.used --format=csv -l 1

# 2. 驗證特徵標準化
✓ 運行驗證腳本
python verify_fixes.py

# 3. 集成自適應批次
✓ 編輯 config.yaml: auto_batch_size = true
✓ 編輯 trainer.py: find_optimal_batch_size()
✓ 單個 fold 測試
python train.py --config configs/experiment_baseline.yaml

# 4. 添加訓練日誌
✓ 編輯 trainer.py: logging 模塊
✓ 運行訓練，檢查控制台輸出
```

### Phase 2 驗證（第二週）

```bash
# 5. I/O 優化
✓ 編輯 panel.py: 時間戳驗證
✓ 首次運行：10-30s
✓ 第二次運行：1-2s（快速路徑）

# 6. 型別提示
✓ 運行 mypy
mypy stockagent/ --no-error-summary

# 7. Transformer 邏輯
✓ 編輯 mlp.py: 條件式邏輯
✓ lookback=1 時，測試單層 MLP
✓ lookback>1 時，測試 Transformer

# 8. 專案結構
✓ 創建 setup.py / pyproject.toml
✓ pip install -e .
✓ 驗證包導入
```

---

## 📞 常見問題解答

### Q: 從哪裡開始？
**A:** 讀 [EXECUTIVE_SUMMARY.md](./EXECUTIVE_SUMMARY.md)，然後執行 Task 1（修復自動微分洩漏）

### Q: 所有修復一起做還是逐個做？
**A:** **逐個做**，順序如下：
1. Task 1-4（第一週，2.5 小時）
2. Task 5-8（第二週，8 小時）
3. Task 9-10（研究項目，8-14 小時）

### Q: 修復後如何驗證？
**A:** 見 **驗證檢查清單** 部分，每個 task 有對應驗證方法

### Q: 預期改進多少？
**A:** 
- 第一週（Task 1-4）：+20-25%
- 第一個月（Task 1-8）：+30-50%
- 長期（含 Task 9-10）：+150-250% (含並行)

### Q: 風險有多大？
**A:** 極低。所有修復都是：
- ✅ 局部改進（不涉及核心算法）
- ✅ 驗證方法明確（監控指標定義清楚）
- ✅ 可回滾（無破壞性改變）

---

## 🔗 交叉參考

### 相同主題的不同視角

**自動微分洩漏：**
- 問題描述：[COMPREHENSIVE_ANALYSIS.md](./COMPREHENSIVE_ANALYSIS.md#1-自動微分記錄圖未清理-memory-leak)
- 修復步驟：[EXECUTIVE_SUMMARY.md](./EXECUTIVE_SUMMARY.md#-task-1修復記憶體洩漏30分鐘)
- 診斷方法：[PERFORMANCE_DIAGNOSTICS.md](./PERFORMANCE_DIAGNOSTICS.md#-訓練循環優化前後對比)

**批次大小優化：**
- 問題分析：[COMPREHENSIVE_ANALYSIS.md](./COMPREHENSIVE_ANALYSIS.md#5-批次大小靜態配置)
- 修復步驟：[EXECUTIVE_SUMMARY.md](./EXECUTIVE_SUMMARY.md#-task-3啟用自適應批次大小45分鐘)
- 性能預測：[PERFORMANCE_DIAGNOSTICS.md](./PERFORMANCE_DIAGNOSTICS.md#-性能分析批次-vs-vram)

**特徵標準化：**
- 完整原理：[PERFORMANCE_DIAGNOSTICS.md](./PERFORMANCE_DIAGNOSTICS.md#-特徵標準化的重要性)
- 修復驗證：[EXECUTIVE_SUMMARY.md](./EXECUTIVE_SUMMARY.md#-task-2驗證特徵標準化15分鐘)
- 代碼示例：[OPTIMIZATION_TEMPLATES.md](./OPTIMIZATION_TEMPLATES.md)

---

## 📊 數據統計

### 分析規模

```
分析詞數：     15,000+
涵蓋問題：     11 個
優先級分類：   4 級（Critical/High/Medium/Low）
修復時間：     8-10 小時（分散）
性能改進：     +20% (1周) 到 +250% (3-6月)

文檔總數：     6 份新生成 + 6 份已存在
總字數：       40,000+ 詞
```

### 問題分佈

```
Critical（必做）:   5 個  (45%)  →  1.5 小時  →  +70%
High（強烈推薦）:  3 個  (27%)  →  4 小時    →  +15%  
Medium（優化）:    2 個  (18%)  →  3 小時    →  +10%
Low（架構）:       1 個  (10%)  →  4 小時    →  可維護性

合計：             11 個  (100%)  →  12 小時   →  +150%+ 潛力
```

---

## 🎬 下一步

### 立即行動（今天）
1. ✅ 閱讀本文件（5 分鐘）
2. ✅ 閱讀 [EXECUTIVE_SUMMARY.md](./EXECUTIVE_SUMMARY.md)（10 分鐘）
3. → 執行 Task 1（30 分鐘）

### 本週目標
- [ ] 完成 Task 1-4（2.5 小時）
- [ ] 驗證收益（+20% 速度、VRAM 穩定）
- [ ] 文檔記錄（進度更新）

### 本月目標
- [ ] 完成 Task 1-8（12.5 小時）
- [ ] 達成 +30-50% 性能改進
- [ ] 建立單元測試框架

---

**文檔完成時間：** 2026年5月20日  
**最後更新：** 2026年5月20日  
**狀態：** 📍 **準備執行** - 下一步：開始 Task 1
