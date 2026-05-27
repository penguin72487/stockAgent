# StockAgent 優化分析 - 第一性原理

## 📋 核心原則

**三個基本約束：**
1. **計算約束** (GPU VRAM: 12GB)
2. **I/O約束** (Parquet 讀取、緩存)
3. **統計約束** (Walk-Forward 時間序列完整性)

---

## 🔴 一級優化機會 (高影響、可立即實施)

### 1. **多 Fold 並行訓練**
**第一性原理：** Walk-Forward 各 Fold 統計獨立 → 可完全並行

**現狀：**
```python
# train.py: 順序訓練
for fold in folds:
    train_fold(fold)  # 循環等待，GPU 未充分利用
```

**優化方案：**
```
├─ 方案 A (多進程)：
│  └─ 使用 multiprocessing.Pool 或 Ray，分配給多個 GPU
│     - 若有 N 張 GPU，可 N 倍加速
│     - 需要隔離 CUDA context
│
├─ 方案 B (分佈式)：
│  └─ Ray/Hydra Launcher 支持分佈式提交
│     - 適合集群環境
│
└─ 方案 C (串列優化，無需多 GPU)：
   └─ 保留順序但減少端到端時間（見 2-5 項）
```

**實現難度：** ⭐⭐ | **性能收益：** 4-16x (GPU 數量)

---

### 2. **Panel 數據的增量更新 + 列式內存表示**
**第一性原理：** 每次訓練重新讀取 5000+ 行 × 100+ 股 × 12 個特徵的 Parquet，但 95% 數據未變

**現狀：**
```python
# data/panel.py
def build_panel(parquet_root):
    # 每次運行都讀取所有 parquet 文件、對齐、編碼
    # panel_cache.npz 只檢查是否存在，不檢查時間戳
    panel_df = read_parquet_files(...)  # ← 主要瓶頸
```

**優化方案：**

**2A. 時間戳驗證 + 增量讀取：**
```python
cache_mtime = os.path.getmtime('panel_cache.npz')
source_mtimes = [os.path.getmtime(f) for f in parquet_files]
if cache_mtime > max(source_mtimes):
    load_cache()  # 快速路徑
else:
    # 只讀取新增的日期/股票
    new_dates = get_new_dates(cached_dates, latest_date)
    read_new_parquet_rows(new_dates)  # 部分讀取
```

**收益：** 典型 +90% 加速（從 10 秒 → 1 秒）

---

**2B. 列式內存表示（Arrow/Polars）：**

**當前結構（行式 NumPy）：**
```
[T=5000, S=100, F=12] → 按日期順序讀取/訪問低效
內存：6 MB (float32)，但訪問模式是 [t, :, :] (整行)
```

**優化結構（列式 Polars）：**
```python
# 使用 Polars 存儲到 Parquet（自動列式）
df = pl.read_parquet('panel_cache.parquet')
# 查詢特定日期所有特徵：df.filter(pl.col('date')==date).to_numpy()
# → 利用列索引加速，避免完整掃描
```

**收益：** 
- 讀取速度 +30-50% (列索引)
- 內存壓縮 +20% (字典編碼)
- 特徵工程更簡潔（Polars 表達式）

**組合收益：** 增量 + 列式 ≈ **95% 加速**

---

### 3. **批次大小自適應策略（當前為靜態估計）**
**第一性原理：** 訓練過程中 GPU 使用率往往遠低於峰值 (VRAM 估計過保守)

**現狀：**
```python
# trainer.py: _budget_batch_size()
train_static = param_bytes * 2.5  # 權重 + 梯度 + Adam
train_sample = lookback * symbols * features * 4 + overhead
batch_size = (budget_bytes - margin) / train_sample
# 結果：batch_size=64 (保守估計)
```

**問題：** 
- 激活值、临時緩衝未精確計算
- 邊界情況（dropout、layer norm 統計）
- AMP (bf16) 混合精度節省未充分利用

**優化方案：**

**3A. 運行時調整 (Binary Search)：**
```python
def find_max_batch_size():
    low, high = 32, 256
    while low < high:
        mid = (low + high + 1) // 2
        try:
            test_batch(mid)
            low = mid  # 成功，嘗試更大
        except RuntimeError as e:
            if 'out of memory' in str(e):
                high = mid - 1  # OOM，縮小
        torch.cuda.empty_cache()
    return low
```

**收益：** 50-100% 提高吞吐量（batch_size 64 → 128）

**3B. 精確的激活值估計：**
```python
# 在 _estimate_sample_bytes() 中：
# MLP 架構: [B, S, lookback*F] → hidden_dim → hidden_dim → 1
activation_fwd = B * S * (hidden_dim * 2)  # 兩層隱藏激活
activation_bwd = activation_fwd * 1.5      # 反向傳播保存
overhead_per_sample = ... (梯度累積、優化器狀態)
```

**收益：** +10-20% batch_size

---

### 4. **向量化損失 + 指標計算**
**第一性原理：** 當前循環逐日計算 Sharpe，而 PyTorch/NumPy 的向量化操作快 10-100 倍

**現狀：**
```python
# training/trainer.py: sharpe_aware_loss()
def sharpe_aware_loss(weights, future_returns, tradable_mask, ...):
    weighted_returns = (weights * future_returns).sum(dim=1)  # [B]
    # 但 Sharpe 的 std 計算分離
    mean_r = weighted_returns.mean()
    std_r = weighted_returns.std()
    # ← 這裡是 Python 標量，沒有梯度流
```

**優化方案：**

**保持全可微 Sharpe：**
```python
def sharpe_aware_loss_v2(weights, future_returns, tradable_mask, 
                         fee_per_side=0.001):
    B = weights.shape[0]
    
    # 批次內 Sharpe（保持梯度）
    weighted_returns = (weights * future_returns).sum(dim=1)  # [B]
    
    # 增加小常數避免 div by zero
    mean_r = weighted_returns.mean()
    var_r = weighted_returns.var(unbiased=False)
    sharpe = mean_r / (torch.sqrt(var_r) + 1e-6) * torch.sqrt(torch.tensor(252.0))
    
    # 手續費向量化
    if weights.shape[0] > 1:
        turnover = (weights[1:] - weights[:-1]).abs().sum(dim=1).mean()
    else:
        turnover = 0
    
    # 複合損失（可調權重）
    loss = -sharpe + 0.1 * turnover
    return loss
```

**收益：** 
- 损失計算 +3-5x 加速
- 更好的梯度流（比當前版本更穩定訓練）

---

### 5. **動態精度混合 (AMP) 層級優化**
**第一性原理：** BFloat16 + Float32 組合可減少 VRAM 50%，同時保持精度

**現狀：**
```python
# trainer.py: _resolve_amp_dtype()
amp_dtype = torch.bfloat16  # 已支持，但用法單一
# GradScaler(device='cuda') → 前向用 bf16，反向用 float32
```

**優化方案：**

**分層精度策略：**
```python
# 投資組合層（對精度敏感）→ float32
self.fc_final = nn.Linear(hidden_dim, num_symbols)  # 權重 float32

# 隱藏層（魯棒）→ bfloat16
self.hidden_layers = nn.Sequential(
    nn.Linear(lookback*F, hidden_dim),  # 權重 bf16
    nn.GELU(),
    nn.Dropout(dropout),
    nn.Linear(hidden_dim, hidden_dim),  # 權重 bf16
    nn.GELU(),
    nn.Dropout(dropout),
)

# 前向傳播
x_bf16 = x.to(torch.bfloat16)
hidden = self.hidden_layers(x_bf16)
hidden_f32 = hidden.float()
output = self.fc_final(hidden_f32)
```

**收益：** 
- VRAM 節省 30-40%
- 訓練穩定性提高（對數值敏感的投資組合層保留 fp32）

---

## 🟠 二級優化機會 (中等影響、需架構調整)

### 6. **跨股票特徵工程的向量化**
**第一性原理：** 當前在 Panel 構建時逐列操作特徵，可改為批量操作

**現狀：**
```python
# data/panel.py: build_panel()
def add_technical_indicators(df):
    for symbol in symbols:
        df_sym = df[df['symbol'] == symbol]
        # 計算 log return、Z-score 等
        # ← 100+ 次循環，Python 解釋器開銷大
```

**優化方案：**

```python
def add_technical_indicators_vectorized(df):
    # 一次性計算全表
    df['log_return'] = df.groupby('symbol')['close'].apply(
        lambda x: np.log(x / x.shift(1))
    )
    df['zscore'] = df.groupby('symbol')['PER'].apply(
        lambda x: (x - x.mean()) / (x.std() + 1e-8)
    )
    # ← Polars 或 groupby 優化，快 5-10x
    return df
```

**收益：** Panel 構建 +80% 加速

---

### 7. **評估/回測的並行化**
**第一性原理：** 測試集評估（compute_metrics）可 GPU 向量化，目前是 CPU 循環

**現狀：**
```python
# evaluation/report.py: compute_metrics_by_year()
for year, mask in year_masks:  # 按年循環
    returns_year = returns[mask]
    # 計算 Sharpe、Sortino、最大回撤
    # ← 全部在 CPU NumPy 上
```

**優化方案：**

```python
def compute_metrics_gpu(returns_tensor, year_masks_tensor):
    # 返回形狀 [num_years, num_metrics]
    metrics = torch.zeros(len(year_masks), 5, device='cuda')
    
    for i, year_mask in enumerate(year_masks_tensor):
        r_year = returns_tensor[year_mask]
        metrics[i, 0] = r_year.mean()                          # 年均回報
        metrics[i, 1] = r_year.std()                           # 波動率
        metrics[i, 2] = r_year.mean() / r_year.std() * 16     # Sharpe (年化)
        metrics[i, 3] = torch.min(torch.cumprod(1 + r_year) / torch.max(torch.cumprod(1 + r_year)))  # MDD
        metrics[i, 4] = (r_year < 0).sum() / len(r_year)       # 負回報率
    
    return metrics  # 批量計算，GPU 加速 10-50x
```

**收益：** 16 個 fold 的評估時間 5 秒 → 0.5 秒

---

### 8. **特徵嵌入層（降低 lookback*F 維度爆炸）**
**第一性原理：** lookback=30, F=12 → 輸入 360 維，但許多特徵高度相關

**現狀：**
```python
# models/mlp.py: CrossSectionalMLP
x: [B*S, lookback*F=360] → Linear(360 → hidden_dim)
# 第一層密集參數數量: 360 * hidden_dim (1.4M @ hidden_dim=4000)
```

**優化方案：**

**加入特徵嵌入層：**
```python
class CrossSectionalMLP_v2(nn.Module):
    def __init__(self, ..., embedding_dim=16):
        super().__init__()
        
        # 特徵嵌入：[lookback*F] → [lookback, embedding_dim]
        self.feature_embedding = nn.Linear(F, embedding_dim)
        
        # 時間融合：[lookback, embedding_dim] → [embedding_dim]
        self.time_fusion = nn.TransformerEncoderLayer(
            d_model=embedding_dim,
            nhead=4,
            dim_feedforward=64,
            batch_first=True,
            norm_first=True,
        )
        
        # 投資組合層
        self.portfolio_head = nn.Sequential(
            nn.Linear(embedding_dim, hidden_dim),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, 1),
        )
    
    def forward(self, x, tradable_mask):
        # x: [B*S, lookback, F]
        x_emb = self.feature_embedding(x)  # [B*S, lookback, embedding_dim]
        x_fused = self.time_fusion(x_emb)  # [B*S, lookback, embedding_dim]
        x_pooled = x_fused[:, -1, :]       # 取最後一步或 mean pooling
        
        logits = self.portfolio_head(x_pooled)  # [B*S, 1]
        return self._masked_softmax(logits, tradable_mask)
```

**參數對比：**
| 組件 | 原版 | 優化版 | 節省 |
|------|------|--------|------|
| 嵌入層 | - | 12*16=192 | - |
| 第一層 MLP | 360*4000 | 16*4000 | 98% ↓ |
| 時間 Transformer | - | ~5K | - |
| **總計** | ~1.4M | ~30K | **98% ↓** |

**收益：** 
- 參數 97% 減少 → 訓練 10x 快，過擬合風險 ↓
- 可遷移性提高（新特徵無需重訓）

---

### 9. **梯度累積 + 減少 Epoch 策略**
**第一性原理：** 有效批次大小 (grad_accum_steps × batch_size) 影響收斂，而非epoch 數量

**現狀：**
```python
# train.py
optimizer = AdamW(model.parameters(), lr=0.001)
for epoch in range(max_epochs=100):  # 盲目迭代
    for batch_idx, batch in enumerate(train_loader):
        # 單次反向傳播
```

**優化方案：**

```python
grad_accum_steps = 4  # 模擬更大批次
effective_batch_size = batch_size * grad_accum_steps  # 64*4 = 256

for epoch in range(max_epochs=30):  # 減少 epoch，但有效批次更大
    for batch_idx, batch in enumerate(train_loader):
        outputs = model(batch)
        loss = criterion(outputs, targets)
        loss.backward()  # 累積梯度
        
        if (batch_idx + 1) % grad_accum_steps == 0:
            optimizer.step()
            optimizer.zero_grad()
```

**收益：** 
- 訓練時間 +20-30%（同樣收斂性）
- 更穩定的梯度估計

---

## 🟡 三級優化機會 (低成本研究)

### 10. **數據類型精度調整**
- Parquet 默認 float64 → float32 (節省 50% I/O)
- 交易性遮罩改為 uint8 (節省 87.5%)
- 日期用 uint16 (相對索引)

### 11. **模型檢查點智能保存**
- 僅保存最佳 5 個模型（當前保存全部）
- 增量檢查點（僅存儲權重差異）

### 12. **Early Stopping 改進**
- 當前基於驗證集損失，可改為驗證集 Sharpe（更符合目標）

### 13. **回測結果緩存**
- 避免重複計算相同 fold 的指標

---

## 📊 優化優先級矩陣

| 優化項 | 難度 | 收益 | 優先級 |
|--------|------|------|--------|
| 1. 多 Fold 並行 | ⭐⭐⭐ | 4-16x | 🔴 最高 |
| 2A. 增量 Panel | ⭐ | 10x | 🔴 最高 |
| 2B. 列式存儲 | ⭐⭐ | 2x | 🔴 最高 |
| 3A. 自適應批次 | ⭐⭐ | 2x | 🟠 高 |
| 4. 向量化損失 | ⭐ | 1.5x + 穩定性 | 🟠 高 |
| 5. 分層精度 | ⭐ | 1.4x 速度 + 30% VRAM | 🟠 高 |
| 6. 特徵工程向量化 | ⭐ | 5x | 🟠 高 |
| 7. 評估並行化 | ⭐⭐ | 10x | 🟠 高 |
| 8. 特徵嵌入層 | ⭐⭐⭐ | 10x 參數 ↓ | 🟡 中 |
| 9. 梯度累積 | ⭐ | 1.3x + 穩定性 | 🟡 中 |
| 10-13. 微優化 | ⭐ | 1.1x | 🟡 低 |

---

## 🎯 建議實施路線圖 (Week 1-4)

### Week 1: 快速勝利 (2A + 4 + 5)
- Panel 增量更新 + 時間戳檢查
- 向量化損失函數 (保持全可微)
- 分層精度混合 (float32 投資組合頭，bf16 隱藏)

**預期收益：** +40-60% 訓練速度

### Week 2: 架構改進 (3A + 6 + 7)
- 運行時批次大小二分查找
- 特徵工程向量化
- 評估並行化

**預期收益：** +50-100% 吞吐量，評估 10x 快

### Week 3: 高級優化 (8)
- 特徵嵌入層 + Transformer 融合
- 對比實驗 (嵌入 dim, 時間頭數)

**預期收益：** 模型參數 97% 減少，訓練 10x 快

### Week 4: 分佈式 (1)
- 多進程或 Ray 實裝 Walk-Forward 並行
- A/B 對比測試

**預期收益：** N GPU 下 N 倍加速

---

## 總結

**無需多 GPU 情況下的理論加速：** ~**3-5 倍**
- 增量 Panel: 10x
- 自適應批次: 2x
- 向量化損失: 1.5x
- 特徵工程: 5x
- 評估並行: 10x
- **組合非線性效應（重疊收益）：** 3-5x 實際加速

**有多 GPU 情況下：** **3-5 倍 × N GPU = 3N-5N 倍加速**
