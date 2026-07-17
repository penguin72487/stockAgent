# StockAgent 代碼整理與結構優化建議

## 📁 當前專案結構分析

### 現狀
```
stockAgent/
├── stockagent/              ← 主包
│   ├── backtest/           ← 回測模擬
│   ├── data/               ← 數據處理
│   ├── evaluation/         ← 指標評估
│   ├── models/             ← 模型定義
│   ├── training/           ← 訓練器
│   ├── config.py          ← 配置管理
│   └── __init__.py
├── configs/                ← 實驗配置
├── data_parquet/           ← 原始數據
├── artifacts/              ← 訓練輸出
├── docs/                   ← 文檔
├── test_*.py              ← 單元測試 (根目錄!)
├── train.py               ← 主入口
└── README.md
```

**問題:**
- ❌ 測試文件散落在根目錄 (`test_*.py`)
- ❌ 沒有 `tests/` 目錄統一管理
- ❌ 沒有 `setup.py` 或 `pyproject.toml` (包管理)
- ❌ 沒有類型提示檔案 (`.pyi`)
- ❌ 配置文件沒有版本管理
- ❌ 工具腳本 (`coda_runner.sh`) 混在根目錄

---

## 🎯 **推薦重構方案**

### **新結構**
```
stockAgent/
├── src/                          ← ✅ 新增: 源代碼隔離
│   └── stockagent/              
│       ├── __init__.py
│       ├── config.py
│       ├── main.py              ← ✅ 新增: 統一入口
│       ├── backtest/
│       │   ├── __init__.py
│       │   ├── simulator.py
│       │   └── report.py
│       ├── data/
│       │   ├── __init__.py
│       │   ├── panel.py
│       │   ├── walkforward.py
│       │   └── validation.py     ← ✅ 新增: 數據驗證
│       ├── evaluation/
│       │   ├── __init__.py
│       │   └── metrics.py
│       ├── models/
│       │   ├── __init__.py
│       │   ├── mlp.py
│       │   └── base.py           ← ✅ 新增: 基類
│       └── training/
│           ├── __init__.py
│           ├── trainer.py
│           ├── dataset.py
│           ├── loss.py
│           └── batch_optimizer.py
│
├── tests/                        ← ✅ 新增: 測試框架
│   ├── __init__.py
│   ├── conftest.py              ← pytest 配置
│   ├── test_data.py             ← 從 test_*.py 遷移
│   ├── test_models.py
│   ├── test_training.py
│   └── fixtures/                ← 測試數據
│
├── configs/                      ← 實驗配置
│   ├── experiment_baseline.yaml
│   └── experiment_v2.yaml       ← ✅ 版本管理
│
├── scripts/                      ← ✅ 新增: 工具腳本
│   ├── train.py                 ← ✅ 遷移自根目錄
│   ├── evaluate.py              ← ✅ 新增: 評估腳本
│   ├── visualize.py             ← ✅ 新增: 可視化腳本
│   └── runner.sh                ← ✅ 遷移 coda_runner.sh
│
├── notebooks/                    ← ✅ 新增: 分析筆記本
│   ├── exploratory.ipynb
│   └── performance_analysis.ipynb
│
├── data_parquet/                ← 原始數據 (不變)
├── artifacts/                   ← 輸出目錄 (不變)
├── docs/                        ← 文檔 (擴展)
│   ├── training_spec.md
│   ├── API.md                   ← ✅ 新增: API 文檔
│   └── DEVELOPMENT.md           ← ✅ 新增: 開發指南
│
├── .github/                     ← ✅ 新增: CI/CD
│   └── workflows/
│       └── test.yml
│
├── pyproject.toml               ← ✅ 新增: 包配置
├── setup.py                     ← ✅ 新增: 安裝腳本
├── requirements.txt             ← 保留 (簡化版)
├── requirements-dev.txt         ← ✅ 新增: 開發依賴
├── pytest.ini                   ← ✅ 新增: pytest 配置
├── .gitignore                   ← 擴展
├── LICENSE                      ← ✅ 新增
├── CHANGELOG.md                 ← ✅ 新增
├── ARCHITECTURE_REVIEW.md       ← 已建立
├── FIXES_IMPLEMENTATION.md      ← 已建立
└── README.md
```

---

## 📝 **具體實施步驟**

### **Step 1: 創建新目錄結構** (10 分鐘)

```bash
# 在根目錄執行
mkdir -p src/stockagent
mkdir -p tests/fixtures
mkdir -p scripts
mkdir -p notebooks
mkdir -p .github/workflows
```

### **Step 2: 遷移源代碼** (30 分鐘)

```bash
# 移動 stockagent 包到 src/
mv stockagent src/stockagent

# 驗證結構
ls -la src/stockagent/
```

### **Step 3: 設置 Python 路徑** (20 分鐘)

創建 `setup.py`:
```python
from setuptools import setup, find_packages

setup(
    name="stockagent",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.10",
    install_requires=[
        "torch>=2.0.0",
        "polars>=0.20.0",
        "numpy>=1.24.0",
        "pyyaml>=6.0",
        "tqdm>=4.65.0",
        "transformers>=4.30.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0",
            "black>=23.0",
            "flake8>=6.0",
            "mypy>=1.0",
        ]
    },
)
```

創建 `pyproject.toml`:
```toml
[build-system]
requires = ["setuptools>=65.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "stockagent"
version = "0.1.0"
description = "Taiwan stock trading research system"
requires-python = ">=3.10"

[tool.black]
line-length = 100
target-version = ["py310"]

[tool.mypy]
python_version = "3.10"
warn_return_any = true
warn_unused_configs = true

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py", "*_test.py"]
```

### **Step 4: 遷移測試** (20 分鐘)

```bash
# 遷移測試文件到 tests/
mv test_mlp_simple.py tests/test_models.py
mv test_single_fold.py tests/test_training.py
mv test_transformer_simple.py tests/test_transformers.py

# 更新導入路徑 (見下方)
```

**更新導入** (`tests/test_models.py`):
```python
# ❌ 舊
from stockagent.models import CrossSectionalMLP

# ✅ 新
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from stockagent.models import CrossSectionalMLP
```

更好的做法是使用 `conftest.py`:
```python
# tests/conftest.py
import sys
from pathlib import Path

# 自動添加 src 到路徑
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
```

### **Step 5: 遷移腳本** (10 分鐘)

```bash
# 遷移主訓練腳本
mv train.py scripts/train.py
mv coda_runner.sh scripts/runner.sh

# 更新引入
sed -i 's/from stockagent/from src.stockagent/g' scripts/train.py
```

### **Step 6: 更新根目錄 train.py** (5 分鐘)

創建新的根目錄 `train.py` (透明代理):
```python
"""
Root-level entry point for backward compatibility.
Delegates to scripts/train.py
"""
import sys
from pathlib import Path

# 添加 src 到路徑
sys.path.insert(0, str(Path(__file__).parent / "src"))

if __name__ == "__main__":
    from stockagent.config import load_config
    from stockagent.data.panel import build_panel
    from stockagent.data.walkforward import build_expanding_year_folds
    from stockagent.training.trainer import run_training
    import argparse
    import json
    from dataclasses import asdict
    import torch
    
    parser = argparse.ArgumentParser(description="Train the stockAgent baseline model")
    parser.add_argument("--config", default="configs/experiment_baseline.yaml", help="Path to experiment config")
    parser.add_argument("--output-dir", default="artifacts", help="Directory for training outputs")
    args = parser.parse_args()
    
    config = load_config(args.config)
    if config.environment.device == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA requested but not available")
    
    panel = build_panel(config.data.parquet_root)
    folds = build_expanding_year_folds(panel.dates, config.walk_forward.min_train_years)
    results = run_training(panel, folds, config, args.output_dir)
    
    # ... 保存結果邏輯
```

### **Step 7: 更新配置** (10 分鐘)

創建 `requirements-dev.txt`:
```
# 開發依賴
pytest>=7.0
pytest-cov>=4.0
black>=23.0
flake8>=6.0
mypy>=1.0
ipython>=8.0
jupyter>=1.0
```

### **Step 8: 添加新的支持文件** (20 分鐘)

**`tests/conftest.py`** (pytest 配置):
```python
import sys
from pathlib import Path
import pytest

# 自動添加 src 到 Python 路徑
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

@pytest.fixture
def sample_panel_data():
    """提供示例面板數據"""
    import numpy as np
    from stockagent.data.panel import PanelData
    
    return PanelData(
        dates=np.arange(100),
        symbols=["2330", "0050", "1101"],
        feature_names=["open", "close", "volume"],
        features=np.random.randn(100, 3, 3),
        returns_1d=np.random.randn(100, 3),
        tradable_mask=np.ones((100, 3), dtype=bool),
        alive_mask=np.ones((100, 3), dtype=bool),
        benchmark_returns=np.random.randn(100),
    )
```

**`.github/workflows/test.yml`** (CI/CD):
```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.11"]
    
    steps:
    - uses: actions/checkout@v3
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        pip install -e .[dev]
    
    - name: Run tests
      run: pytest --cov=src/stockagent tests/
    
    - name: Run linter
      run: flake8 src/stockagent tests/
```

### **Step 9: 驗證導入** (10 分鐘)

```python
# 測試舊導入方式 (應仍有效)
python -c "from stockagent.models import CrossSectionalMLP; print('✅ Old import works')"

# 測試新導入方式
python -c "import sys; sys.path.insert(0, 'src'); from stockagent.models import CrossSectionalMLP; print('✅ New import works')"
```

---

## 📊 **代碼風格與質量標準**

### **類型提示** (需要添加)

```python
# ❌ 舊
def build_panel(parquet_root):
    pass

# ✅ 新
from pathlib import Path
from typing import Optional

def build_panel(parquet_root: str | Path) -> PanelData:
    """
    構建面板數據。
    
    Args:
        parquet_root: Parquet 文件目錄
    
    Returns:
        PanelData: 結構化面板
    
    Raises:
        FileNotFoundError: 若無找到 Parquet 文件
    """
    pass
```

### **文檔字符串標準**

```python
def sharpe_aware_loss(
    weights: Tensor,
    future_log_returns: Tensor,
    tradable_mask: Tensor,
    fee_per_side: float = 0.0,
    gamma_sharpe: float = 1.0,
    gamma_turnover: float = 0.1,
) -> Tensor:
    """
    計算 Sharpe 比率感知的損失函數。
    
    該損失結合投資組合的 Sharpe 比率和交易成本，用於優化日內交易策略。
    
    Args:
        weights: [B, S] 每個股票的投資組合權重
        future_log_returns: [B, S] 隔日對數回報
        tradable_mask: [B, S] 可交易掩膜 (bool)
        fee_per_side: 單邊手續費 (默認 0.0)
        gamma_sharpe: Sharpe 項的權重 (默認 1.0)
        gamma_turnover: 交易成本項的權重 (默認 0.1)
    
    Returns:
        loss: 標量損失值
    
    Example:
        >>> weights = torch.randn(32, 100)
        >>> returns = torch.randn(32, 100)
        >>> mask = torch.ones(32, 100, dtype=torch.bool)
        >>> loss = sharpe_aware_loss(weights, returns, mask)
        >>> loss.backward()
    """
```

### **命名規範**

| 類型 | 規範 | 例子 |
|-----|------|------|
| 類 | PascalCase | `CrossSectionalMLP` |
| 函數 | snake_case | `build_panel`, `_masked_softmax` |
| 常量 | UPPER_SNAKE | `RESERVED_COLUMNS` |
| 私有 | `_leading_underscore` | `_vectorized_backtest` |
| 保護 | `_leading_underscore` | 同上 |

---

## 🧪 **測試結構建議**

### **測試組織**
```
tests/
├── __init__.py
├── conftest.py                ← 共享 fixtures
├── test_data.py              ← 數據管道測試
├── test_models.py            ← 模型單元測試
├── test_training.py          ← 訓練器集成測試
├── test_backtest.py          ← 回測模擬測試
├── test_integration.py       ← 端到端測試
└── fixtures/
    ├── sample_panel.npz      ← 測試數據
    └── sample_config.yaml
```

### **測試命名**
```python
# ✅ 好的測試名稱
def test_panel_builder_loads_parquet_files():
    pass

def test_cross_sectional_dataset_prevents_look_ahead_bias():
    pass

def test_sharpe_loss_computes_stable_gradients():
    pass

# ❌ 壞的名稱
def test_panel():
    pass

def test_stuff():
    pass
```

---

## ✅ **完整檢查清單**

- [ ] 創建 `src/`, `tests/`, `scripts/`, `notebooks/` 目錄
- [ ] 移動 `stockagent/` 到 `src/stockagent/`
- [ ] 移動 `test_*.py` 到 `tests/`
- [ ] 移動 `train.py`, `coda_runner.sh` 到 `scripts/`
- [ ] 更新所有導入路徑
- [ ] 創建 `setup.py` 和 `pyproject.toml`
- [ ] 創建 `requirements-dev.txt`
- [ ] 創建 `pytest.ini` 和 `conftest.py`
- [ ] 創建 `.github/workflows/test.yml`
- [ ] 添加類型提示到核心模塊
- [ ] 添加完整文檔字符串
- [ ] 運行 `pytest tests/` 確保所有測試通過
- [ ] 運行 `flake8 src/` 檢查代碼風格
- [ ] 運行 `mypy src/` 檢查類型

---

## 🎁 **實施後的好處**

| 方面 | 改進 |
|-----|------|
| **可維護性** | 代碼組織清晰，易於導航 |
| **可測試性** | 單元測試 + 集成測試框架完整 |
| **CI/CD** | 自動化測試和代碼質量檢查 |
| **協作** | 新開發者更易上手 |
| **發佈** | 支持通過 `pip install` 安裝 |
| **版本控制** | 清晰的依賴管理和版本記錄 |
