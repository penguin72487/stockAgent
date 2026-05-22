# 🚀 StockAgent 優化執行摘要

**生成日期：** 2026年5月20日  
**狀態：** 🟢 已分析，準備執行  
**預期收益：** +20% 速度（第一週），+50%（第一個月）

---

## 📊 快速概覽

```
當前狀態          問題數量      關鍵Issue      預期修復時間
─────────────────────────────────────────────────────
架構             1 個          None          已規劃
代碼質量         3 個          高優先級      3-4 小時
性能             4 個          Critical      1-2 小時  
監控             1 個          High          1 小時
數據處理         2 個          Medium        2-3 小時
─────────────────────────────────────────────────────
合計：           11 個          5 Critical    8-11 小時
```

---

## 🎯 立即行動（本週）

### ✅ Task 1：修復記憶體洩漏（30分鐘）

**文件：** `stockagent/training/trainer.py`

**問題：** 自動微分圖未清理，導致 VRAM 逐步增長

**修復：**
```python
# 在訓練循環中添加
optimizer.zero_grad()  # 每步清理梯度圖
```

**驗證：**
```bash
nvidia-smi --query-gpu=memory.used --format=csv -l 1
# VRAM 應保持穩定（不連續增長）
```

---

### ✅ Task 2：驗證特徵標準化（15分鐘）

**文件：** `stockagent/data/panel.py`

**驗證命令：**
```bash
python verify_fixes.py
```

若失敗，在 `build_panel()` 中添加：
```python
features_mean = np.mean(features, axis=(0, 1), keepdims=True)
features_std = np.std(features, axis=(0, 1), keepdims=True) + 1e-8
features = (features - features_mean) / features_std
```

---

### ✅ Task 3：啟用自適應批次大小（45分鐘）

**文件：** `stockagent/training/trainer.py`

**配置：** `configs/experiment_baseline.yaml`

**修改：**
```yaml
training:
  auto_batch_size: true  # 啟用自動優化
  batch_size: 32        # 初始值
  vram_budget_gb: 12.0  # GPU 總記憶體
```

**預期效果：** 批次大小從 32 → 128-192，速度 +40%

---

### ✅ Task 4：添加訓練日誌（1小時）

**添加位置：** `trainer.py` 主循環

**最小化實現：**
```python
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 在循環中
logger.info(f"Epoch {e} | Loss {loss:.4f} | VRAM {torch.cuda.memory_allocated()/1e9:.1f}GB")
```

---

## 📈 第一週成果目標

| 指標 | 當前 | 目標 | 方法 |
|------|------|------|------|
| 訓練速度 | 1.0x | 1.2x | Task 1-3 |
| VRAM 增長 | ✗ 有 | ✓ 無 | Task 1 |
| 批次大小 | 32 | 128 | Task 3 |
| 可觀測性 | 低 | 中 | Task 4 |

---

## 📋 詳細檢查清單

### 🔴 Critical（必做）

- [ ] **Task 1** - 修復記憶體洩漏 (30 min)
  - 定位：trainer.py, 訓練迴圈
  - 驗證：VRAM 監控
  - 預期：OOM 消除

- [ ] **Task 2** - 特徵標準化驗證 (15 min)
  - 執行：verify_fixes.py
  - 若失敗：添加標準化代碼
  - 預期：訓練穩定性 +20%

- [ ] **Task 3** - 自適應批次大小 (45 min)
  - 編輯：config + trainer.py
  - 測試：單個 fold
  - 預期：速度 +40%

### 🟠 High（強烈建議）

- [ ] **Task 4** - 訓練日誌系統 (60 min)
  - 添加：logging 模塊
  - 監控：Loss, VRAM, Grad norm
  - 好處：調試能力 + 性能追蹤

- [ ] **Task 5** - I/O 優化（時間戳驗證）(90 min)
  - 編輯：panel.py
  - 機制：快速路徑檢查
  - 預期：初始化 +70%

### 🟡 Medium（本月）

- [ ] **Task 6** - 型別提示完整化 (120 min)
  - 文件：data/, models/, training/
  - 驗證：mypy 檢查
  - 好處：IDE 支持

- [ ] **Task 7** - Transformer 邏輯修復 (60 min)
  - 編輯：mlp.py
  - 添加：lookback=1 特殊處理
  - 預期：正確性

- [ ] **Task 8** - 專案結構重構 (4 hours)
  - 步驟：見 CODE_ORGANIZATION.md
  - 收益：包管理 + 測試框架
  - 必要性：中等

### 🔵 Low（研究）

- [ ] **Task 9** - 多 Fold 並行 (8 hours)
- [ ] **Task 10** - Polars 遷移 (6 hours)

---

## 📞 快速參考

### 命令清單

```bash
# 1. 驗證修復狀態
python verify_fixes.py

# 2. 運行測試（若有）
pytest tests/ -v

# 3. 監控 VRAM（新終端）
watch -n 1 nvidia-smi --query-gpu=memory.used --format=csv,noheader

# 4. 訓練基準線
python train.py --config configs/experiment_baseline.yaml --output-dir artifacts

# 5. 代碼質量檢查
mypy stockagent/
ruff check stockagent/
```

---

## 🔍 問題速查表

### Q1：訓練中 OOM?
→ 優先執行 Task 1（自動微分洩漏）+ Task 3（自適應批次）

### Q2：訓練不穩定？
→ 執行 Task 2（特徵標準化驗證）+ 檢查梯度 norm（Task 4 後）

### Q3：訓練很慢？
→ 執行 Task 3（批次大小）+ Task 5（I/O 優化）

### Q4：找不到 Bug？
→ 執行 Task 4（訓練日誌），查看實時 metrics

---

## 📚 相關文檔

| 文檔 | 內容 | 優先級 |
|------|------|-------|
| [COMPREHENSIVE_ANALYSIS.md](./COMPREHENSIVE_ANALYSIS.md) | 完整分析 | ⭐⭐⭐ |
| [ARCHITECTURE_REVIEW.md](./ARCHITECTURE_REVIEW.md) | 架構評審 | ⭐⭐ |
| [FIXES_COMPLETED.md](./FIXES_COMPLETED.md) | 已完成修復 | ⭐⭐⭐ |
| [CODE_ORGANIZATION.md](./CODE_ORGANIZATION.md) | 結構重構 | ⭐ |
| [OPTIMIZATION_ANALYSIS.md](./OPTIMIZATION_ANALYSIS.md) | 深度優化 | ⭐⭐ |

---

## ⏱️ 時間表

```
今日（5月20日）       ← 分析完成
│
├─ 明天（5月21日）    ← Task 1-3 完成（2小時）
├─ 後天（5月22日）    ← Task 4-5 完成（2小時）
│
├─ 週末                ← 測試 & 驗證
│
└─ 下週（5月27日）    ← Task 6-8 完成（6小時）

預期收益路線圖：
5月20日：100% (基線)
5月22日：120% (Critical 修復)
5月27日：150% (High 優化)
6月中：  200%+ (Medium 完成)
```

---

## 📞 支援

若執行過程中遇到問題：

1. **檢查 COMPREHENSIVE_ANALYSIS.md** - 詳細説明
2. **參考 OPTIMIZATION_TEMPLATES.md** - 代碼範本
3. **運行 verify_fixes.py** - 自動驗證

---

**狀態：準備開始執行**

下一步：選擇 Task 1，開始修復。預計 1 小時完成前 3 個 Task。
