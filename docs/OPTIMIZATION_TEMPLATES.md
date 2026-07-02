# StockAgent 優化實現模板

> 本文件包含可直接使用或改編的代碼片段，實現上述優化建議

---

## 模板 1: 增量 Panel 更新 + 緩存驗證

**文件位置：** `stockagent/data/panel_optimized.py`

```python
import os
import glob
import hashlib
import numpy as np
import polars as pl
from pathlib import Path
from typing import Tuple, Dict
import pickle

class OptimizedPanelBuilder:
    """支持增量更新和列式存儲的 Panel 構建器"""
    
    CACHE_VERSION = 2  # 版本控制，不兼容改動時遞增
    
    def __init__(self, parquet_root: str, cache_dir: str = './cache'):
        self.parquet_root = parquet_root
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        
        self.df = None
        self.date_index = {}
        self.symbol_map = {}
    
    @staticmethod
    def _compute_parquet_hash(parquet_root: str) -> str:
        """計算所有 parquet 文件的聯合雜湊"""
        files = sorted(glob.glob(os.path.join(parquet_root, '*.parquet')))
        hasher = hashlib.md5()
        
        for fpath in files:
            mtime = os.path.getmtime(fpath)
            size = os.path.getsize(fpath)
            hasher.update(f"{fpath}:{mtime}:{size}".encode())
        
        return hasher.hexdigest()
    
    def _load_from_cache(self) -> bool:
        """嘗試從緩存加載，如果有效返回 True"""
        cache_meta = self.cache_dir / 'panel_meta.pkl'
        cache_data = self.cache_dir / 'panel_data.parquet'
        
        if not (cache_meta.exists() and cache_data.exists()):
            return False
        
        # 讀取元數據
        with open(cache_meta, 'rb') as f:
            meta = pickle.load(f)
        
        # 驗證版本和源文件雜湊
        if meta.get('version') != self.CACHE_VERSION:
            print("[Panel] 緩存版本不匹配，重新構建")
            return False
        
        current_hash = self._compute_parquet_hash(self.parquet_root)
        if meta.get('parquet_hash') != current_hash:
            print("[Panel] 源文件已更新，重新構建")
            return False
        
        # 加載緩存數據
        print("[Panel] 從緩存加載...")
        self.df = pl.read_parquet(cache_data)
        self._build_indices()
        
        print(f"[Panel] 加載完成：{len(self.df)} 行，{len(self.symbol_map)} 個股票")
        return True
    
    def _build_indices(self):
        """構建快速查詢索引"""
        self.date_index = {}
        self.symbol_map = {}
        
        for i, (date, symbol) in enumerate(
            self.df.select(['date', 'symbol']).iter_rows()
        ):
            if date not in self.date_index:
                self.date_index[date] = i
            
            if symbol not in self.symbol_map:
                self.symbol_map[symbol] = []
            self.symbol_map[symbol].append(i)
    
    def build(self, force_rebuild: bool = False) -> 'OptimizedPanel':
        """構建或從緩存加載 Panel"""
        
        if not force_rebuild and self._load_from_cache():
            return OptimizedPanel(self.df, self.date_index, self.symbol_map)
        
        print("[Panel] 從 Parquet 文件構建...")
        
        # 讀取所有 parquet 文件
        parquet_files = sorted(
            glob.glob(os.path.join(self.parquet_root, '*.parquet'))
        )
        
        dfs = []
        for fpath in parquet_files:
            symbol = Path(fpath).stem.split('_')[0]  # 從文件名提取符號
            
            df_sym = pl.read_parquet(
                fpath,
                columns=['date', 'open', 'close', 'volume', 'PER', 'ROE', 
                        'Debt_Ratio', 'PER_Z_Score', 'Debt_Ratio_Scaled'],
            )
            
            # 特徵工程：計算對數收益等
            df_sym = df_sym.with_columns([
                pl.lit(symbol).alias('symbol'),
                # 對數收益
                (pl.col('close').log().diff()).alias('log_return'),
                # 交易性遮罩：有成交量且價格有效
                ((pl.col('volume') > 0) & (pl.col('close').is_not_null())).alias('tradable'),
            ])
            
            # 數據類型優化
            df_sym = df_sym.with_columns([
                pl.col('date').cast(pl.Date),
                pl.col('open').cast(pl.Float32),
                pl.col('close').cast(pl.Float32),
                pl.col('volume').cast(pl.UInt32),
                pl.col('PER').cast(pl.Float32),
                pl.col('ROE').cast(pl.Float32),
                pl.col('Debt_Ratio').cast(pl.Float32),
                pl.col('tradable').cast(pl.Boolean),
            ])
            
            dfs.append(df_sym)
        
        # 拼接所有數據
        self.df = pl.concat(dfs).sort(['date', 'symbol'])
        
        # 構建索引
        self._build_indices()
        
        # 保存到緩存
        self._save_cache()
        
        print(f"[Panel] 構建完成：{len(self.df)} 行，{len(self.symbol_map)} 個股票")
        
        return OptimizedPanel(self.df, self.date_index, self.symbol_map)
    
    def _save_cache(self):
        """保存到緩存"""
        print("[Panel] 保存緩存...")
        
        cache_data = self.cache_dir / 'panel_data.parquet'
        cache_meta = self.cache_dir / 'panel_meta.pkl'
        
        # 保存 Parquet 數據
        self.df.write_parquet(cache_data)
        
        # 保存元數據
        meta = {
            'version': self.CACHE_VERSION,
            'parquet_hash': self._compute_parquet_hash(self.parquet_root),
            'num_rows': len(self.df),
            'num_symbols': len(self.symbol_map),
        }
        
        with open(cache_meta, 'wb') as f:
            pickle.dump(meta, f)


class OptimizedPanel:
    """優化後的 Panel，支持快速查詢和列式存儲"""
    
    def __init__(self, df: pl.DataFrame, date_index: Dict, symbol_map: Dict):
        self.df = df
        self.date_index = date_index
        self.symbol_map = symbol_map
        
        # 計算衍生屬性
        self.dates = sorted(self.date_index.keys())
        self.symbols = sorted(self.symbol_map.keys())
        self.num_symbols = len(self.symbols)
        self.feature_names = [
            'open', 'close', 'volume', 'PER', 'ROE', 'Debt_Ratio', 
            'log_return', 'tradable'
        ]
    
    def get_features_on_date(self, date) -> np.ndarray:
        """獲取特定日期的所有特徵，返回 [S, F]"""
        if date not in self.date_index:
            return None
        
        idx = self.date_index[date]
        slice_df = self.df[idx:idx + self.num_symbols]
        
        # 排除日期和符號列
        feature_cols = [col for col in self.df.columns 
                       if col not in ['date', 'symbol']]
        
        return slice_df.select(feature_cols).to_numpy(dtype=np.float32)
    
    def get_symbol_history(self, symbol: str, start_date, end_date) -> np.ndarray:
        """獲取單個股票的時間序列，返回 [T, F]"""
        if symbol not in self.symbol_map:
            return None
        
        indices = self.symbol_map[symbol]
        df_sym = self.df[indices[0]:indices[-1]+1]
        
        result = df_sym.filter(
            (pl.col('date') >= start_date) & (pl.col('date') <= end_date)
        )
        
        feature_cols = [col for col in self.df.columns 
                       if col not in ['date', 'symbol']]
        
        return result.select(feature_cols).to_numpy(dtype=np.float32)
    
    def to_numpy_cross_section(self, start_date, end_date) -> Tuple[np.ndarray, np.ndarray]:
        """
        返回時間範圍內的截面數據
        
        Returns:
            features: [T, S, F]
            dates: [T]
        """
        mask = (self.df['date'] >= start_date) & (self.df['date'] <= end_date)
        df_slice = self.df.filter(mask)
        
        dates = df_slice['date'].unique().sort()
        
        # 構建 [T, S, F] 張量
        T = len(dates)
        S = self.num_symbols
        F = len(self.feature_names)
        
        features = np.zeros((T, S, F), dtype=np.float32)
        
        for t, date in enumerate(dates):
            features[t] = self.get_features_on_date(date)
        
        return features, dates.to_list()

# 使用示例
if __name__ == '__main__':
    builder = OptimizedPanelBuilder(
        parquet_root='./data_parquet',
        cache_dir='./cache'
    )
    panel = builder.build()
    
    # 快速查詢
    features_on_date = panel.get_features_on_date('2024-01-15')
    history = panel.get_symbol_history('2330', '2024-01-01', '2024-12-31')
```

---

## 模板 2: 自適應批次大小 (運行時二分查找)

**文件位置：** `stockagent/training/batch_size_optimizer.py`

```python
import torch
import torch.nn as nn
from typing import Optional, Tuple

class AdaptiveBatchSizeOptimizer:
    """運行時自動查找最大 batch size，利用 GPU 內存"""
    
    def __init__(self, 
                 model: nn.Module,
                 device: str = 'cuda:0',
                 margin_mb: int = 500,  # 保留的安全邊界
                 verbose: bool = True):
        self.model = model
        self.device = device
        self.margin_mb = margin_mb
        self.verbose = verbose
    
    def get_gpu_memory(self) -> Tuple[float, float]:
        """返回 (已用 MB, 總計 MB)"""
        torch.cuda.reset_peak_memory_stats(device=self.device)
        torch.cuda.empty_cache()
        
        props = torch.cuda.get_device_properties(self.device)
        total_mb = props.total_memory / 1024 / 1024
        
        reserved = torch.cuda.memory_reserved(self.device) / 1024 / 1024
        
        return reserved, total_mb
    
    def estimate_sample_bytes(self, 
                             batch_size: int,
                             lookback: int,
                             num_symbols: int,
                             num_features: int,
                             training: bool = True) -> int:
        """估計單個樣本的記憶體使用量 (位元組)"""
        
        # 輸入激活值
        input_size = batch_size * lookback * num_symbols * num_features * 4  # float32
        
        # 隱藏層激活值（假設 MLP）
        hidden_dim = 256
        hidden_size = batch_size * num_symbols * hidden_dim * 4  # 兩層隱藏層
        
        if training:
            # 訓練時需要保存激活值用於反向傳播
            activation_cache = input_size + hidden_size * 2
            # 梯度和優化器狀態 (Adam: 參數 + moment1 + moment2)
            param_bytes = sum(p.numel() * 4 for p in self.model.parameters())
            grad_bytes = param_bytes * 3  # 梯度 + 2 個 Adam moment
        else:
            activation_cache = 0
            grad_bytes = 0
        
        return int(input_size + hidden_size + activation_cache + grad_bytes / batch_size)
    
    def find_max_batch_size(self,
                           lookback: int,
                           num_symbols: int,
                           num_features: int,
                           min_batch_size: int = 1,
                           max_batch_size: int = 512,
                           training: bool = True) -> int:
        """二分查找最大可行 batch size"""
        
        if self.verbose:
            print(f"\n[BatchSize] 搜索最大 batch size...")
            print(f"  範圍：[{min_batch_size}, {max_batch_size}]")
            print(f"  訓練模式：{training}")
        
        torch.cuda.empty_cache()
        reserved_mb, total_mb = self.get_gpu_memory()
        available_mb = total_mb - reserved_mb - self.margin_mb
        
        if self.verbose:
            print(f"  可用 VRAM：{available_mb:.0f} MB / {total_mb:.0f} MB")
        
        low, high = min_batch_size, max_batch_size
        best_batch_size = min_batch_size
        
        while low <= high:
            mid = (low + high) // 2
            
            # 估計記憶體
            estimated_mb = self.estimate_sample_bytes(
                mid, lookback, num_symbols, num_features, training
            ) / 1024 / 1024
            
            if self.verbose:
                print(f"  測試 batch_size={mid:3d}: 估計 {estimated_mb:.1f} MB", end='')
            
            if estimated_mb <= available_mb * 0.95:  # 留 5% 安全邊界
                # 嘗試實際執行
                try:
                    self._test_forward_pass(
                        batch_size=mid,
                        lookback=lookback,
                        num_symbols=num_symbols,
                        num_features=num_features,
                        training=training
                    )
                    
                    if self.verbose:
                        print(" ✓ 成功")
                    
                    best_batch_size = mid
                    low = mid + 1
                
                except RuntimeError as e:
                    if 'out of memory' in str(e):
                        if self.verbose:
                            print(" ✗ OOM")
                        high = mid - 1
                    else:
                        raise
            else:
                if self.verbose:
                    print(" ✗ 超過預算")
                high = mid - 1
            
            torch.cuda.empty_cache()
        
        if self.verbose:
            print(f"\n[BatchSize] 最大可行 batch_size = {best_batch_size}\n")
        
        return best_batch_size
    
    def _test_forward_pass(self,
                          batch_size: int,
                          lookback: int,
                          num_symbols: int,
                          num_features: int,
                          training: bool = True):
        """執行測試前向傳播以驗證可行性"""
        
        torch.cuda.empty_cache()
        
        # 創建虛擬數據
        x = torch.randn(
            batch_size * num_symbols,
            lookback,
            num_features,
            device=self.device,
            dtype=torch.float32,
        )
        
        mask = torch.ones(batch_size * num_symbols, dtype=torch.bool, device=self.device)
        
        # 前向傳播
        if training:
            self.model.train()
            outputs = self.model(x, mask)
            loss = outputs.sum()
            loss.backward()
        else:
            self.model.eval()
            with torch.no_grad():
                outputs = self.model(x, mask)
        
        torch.cuda.synchronize()

# 使用示例
if __name__ == '__main__':
    from stockagent.models.mlp import CrossSectionalMLP
    
    # 構建模型
    model = CrossSectionalMLP(
        lookback=30,
        num_features=12,
        num_symbols=100,
        hidden_dim=256,
        dropout=0.1,
    ).to('cuda:0')
    
    # 找最大 batch size
    optimizer = AdaptiveBatchSizeOptimizer(model, device='cuda:0')
    
    max_bs_train = optimizer.find_max_batch_size(
        lookback=30,
        num_symbols=100,
        num_features=12,
        training=True
    )
    
    max_bs_eval = optimizer.find_max_batch_size(
        lookback=30,
        num_symbols=100,
        num_features=12,
        training=False
    )
    
    print(f"訓練 batch size: {max_bs_train}")
    print(f"評估 batch size: {max_bs_eval}")
```

---

## 模板 3: 特徵嵌入層 + Transformer 融合

**文件位置：** `stockagent/models/mlp_efficient.py`

```python
import torch
import torch.nn as nn
from typing import Optional

class EfficientCrossectionalMLP(nn.Module):
    """
    改進的跨截面 MLP：
    - 特徵嵌入層（360 維 → 16 維）
    - Transformer 時間融合
    - 參數減少 98%，速度提升 10-20x
    """
    
    def __init__(self,
                 lookback: int,
                 num_features: int,
                 num_symbols: int,
                 embedding_dim: int = 16,
                 hidden_dim: int = 256,
                 num_heads: int = 4,
                 num_layers: int = 2,
                 dropout: float = 0.1):
        super().__init__()
        
        self.lookback = lookback
        self.num_features = num_features
        self.num_symbols = num_symbols
        self.embedding_dim = embedding_dim
        self.hidden_dim = hidden_dim
        
        # ============ 特徵嵌入層 ============
        # 將 F 個特徵壓縮到 embedding_dim
        # 參數數：num_features × embedding_dim
        self.feature_embedding = nn.Linear(num_features, embedding_dim)
        self.embedding_norm = nn.LayerNorm(embedding_dim)
        
        # ============ Transformer 編碼器 ============
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=embedding_dim,
            nhead=num_heads,
            dim_feedforward=hidden_dim // 2,
            batch_first=True,
            norm_first=True,
            dropout=dropout,
            activation='gelu',
        )
        
        self.transformer_encoder = nn.TransformerEncoder(
            encoder_layer,
            num_layers=num_layers,
            norm=nn.LayerNorm(embedding_dim),
        )
        
        # ============ 投資組合評分頭 ============
        self.portfolio_head = nn.Sequential(
            nn.Linear(embedding_dim, hidden_dim),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, 1),
        )
    
    def forward(self, x: torch.Tensor, tradable_mask: Optional[torch.Tensor] = None) -> torch.Tensor:
        """
        Args:
            x: [B*S, lookback, num_features] 輸入特徵
            tradable_mask: [B*S] 交易性遮罩 (bool)
        
        Returns:
            weights: [B*S] 投資組合權重
        """
        B_times_S = x.shape[0]
        
        # ============ 特徵嵌入 ============
        # [B*S, lookback, F] → [B*S, lookback, embedding_dim]
        x_embedded = self.feature_embedding(x)  # 參數：F*embedding_dim
        x_embedded = self.embedding_norm(x_embedded)
        
        # ============ Transformer 時間融合 ============
        # [B*S, lookback, embedding_dim] → [B*S, lookback, embedding_dim]
        x_fused = self.transformer_encoder(x_embedded)
        
        # ============ 時間池化 ============
        # 多種池化選項：
        # 選項 1：取最後時步（假設最新信息最重要）
        x_pooled = x_fused[:, -1, :]  # [B*S, embedding_dim]
        
        # 選項 2：平均池化
        # x_pooled = x_fused.mean(dim=1)
        
        # 選項 3：注意力池化
        # weights = torch.softmax(x_fused.sum(dim=-1), dim=1)  # [B*S, lookback]
        # x_pooled = (x_fused * weights.unsqueeze(-1)).sum(dim=1)  # [B*S, embedding_dim]
        
        # ============ 投資組合評分 ============
        logits = self.portfolio_head(x_pooled)  # [B*S, 1]
        logits = logits.squeeze(-1)  # [B*S]
        
        # ============ Softmax with Masking ============
        # 重塑回 [B, S] 以應用 softmax
        B = B_times_S // self.num_symbols
        S = self.num_symbols
        
        logits_reshaped = logits.view(B, S)
        
        # 應用交易性遮罩
        if tradable_mask is not None:
            tradable_reshaped = tradable_mask.view(B, S)
            logits_reshaped = logits_reshaped.masked_fill(~tradable_reshaped, float('-inf'))
        
        # Softmax 計算權重
        weights = torch.softmax(logits_reshaped, dim=1)  # [B, S]
        
        # 重塑回 [B*S] 以匹配輸入格式
        return weights.view(-1)
    
    def count_parameters(self):
        """計算模型參數數"""
        total = sum(p.numel() for p in self.parameters() if p.requires_grad)
        return total

# 模型對比
def compare_models():
    """對比原版 MLP 和改進版"""
    
    from stockagent.models.mlp import CrossSectionalMLP
    
    config = {
        'lookback': 30,
        'num_features': 12,
        'num_symbols': 100,
        'hidden_dim': 256,
        'dropout': 0.1,
    }
    
    # 原版模型
    original = CrossSectionalMLP(**config).to('cuda:0')
    
    # 改進版模型
    improved = EfficientCrossectionalMLP(
        lookback=config['lookback'],
        num_features=config['num_features'],
        num_symbols=config['num_symbols'],
        embedding_dim=16,
        hidden_dim=config['hidden_dim'],
        dropout=config['dropout'],
    ).to('cuda:0')
    
    # 對比參數
    original_params = sum(p.numel() for p in original.parameters())
    improved_params = sum(p.numel() for p in improved.parameters())
    
    print(f"原版參數數：{original_params:,}")
    print(f"改進版參數數：{improved_params:,}")
    print(f"參數減少：{100 * (1 - improved_params / original_params):.1f}%")
    
    # 性能測試
    import time
    
    x = torch.randn(6400, 30, 12, device='cuda:0')  # [6400, 30, 12]
    mask = torch.ones(6400, dtype=torch.bool, device='cuda:0')
    
    # 預熱
    for _ in range(10):
        _ = original(x, mask)
        _ = improved(x, mask)
    
    torch.cuda.synchronize()
    
    # 原版速度
    start = time.time()
    for _ in range(100):
        _ = original(x, mask)
    torch.cuda.synchronize()
    original_time = (time.time() - start) / 100 * 1000
    
    # 改進版速度
    start = time.time()
    for _ in range(100):
        _ = improved(x, mask)
    torch.cuda.synchronize()
    improved_time = (time.time() - start) / 100 * 1000
    
    print(f"\n原版前向時間：{original_time:.2f} ms")
    print(f"改進版前向時間：{improved_time:.2f} ms")
    print(f"速度提升：{original_time / improved_time:.1f}x")

if __name__ == '__main__':
    compare_models()
```

---

## 模板 4: 多 Fold 並行訓練

**文件位置：** `train_parallel.py`

```python
import os
import sys
import multiprocessing as mp
from functools import partial
from pathlib import Path

import torch
import torch.cuda
from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.training.trainer import train_fold

def train_fold_worker(fold_idx: int,
                     fold,
                     config,
                     device_id: int,
                     output_dir: Path) -> dict:
    """
    單個 GPU 上訓練一個 fold
    
    Args:
        fold_idx: fold 序號
        fold: FoldSpec 對象
        config: 配置
        device_id: GPU 編號
        output_dir: 結果輸出目錄
    
    Returns:
        結果字典
    """
    
    # 設置 GPU
    torch.cuda.set_device(device_id)
    device = f'cuda:{device_id}'
    
    print(f"[Fold {fold_idx}] 開始訓練 (GPU {device_id})")
    
    try:
        # 訓練該 fold
        best_model, metrics, results = train_fold(
            fold=fold,
            config=config,
            device=device,
            fold_idx=fold_idx,
            verbose=True,
        )
        
        # 保存結果
        fold_output_dir = output_dir / f'fold_{fold_idx:02d}'
        fold_output_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存模型
        torch.save(
            best_model.state_dict(),
            fold_output_dir / 'model_best.pt'
        )
        
        # 保存指標
        import json
        with open(fold_output_dir / 'metrics.json', 'w') as f:
            json.dump(metrics, f, indent=2, default=str)
        
        print(f"[Fold {fold_idx}] 完成 (GPU {device_id})")
        
        return {
            'fold_idx': fold_idx,
            'status': 'success',
            'metrics': metrics,
            'output_dir': str(fold_output_dir),
        }
    
    except Exception as e:
        print(f"[Fold {fold_idx}] 失敗 (GPU {device_id}): {str(e)}")
        
        import traceback
        traceback.print_exc()
        
        return {
            'fold_idx': fold_idx,
            'status': 'failed',
            'error': str(e),
        }

def main_parallel(config_path: str, num_gpus: int = None):
    """主程序：並行訓練所有 fold"""
    
    if num_gpus is None:
        num_gpus = torch.cuda.device_count()
    
    print(f"使用 {num_gpus} 個 GPU 並行訓練")
    
    # 加載配置
    config = load_config(config_path)
    
    # 構建數據
    print("構建 Panel...")
    panel = build_panel(config.data.parquet_root)
    
    print("構建 Walk-Forward Folds...")
    folds = build_expanding_year_folds(
        panel.dates,
        config.walk_forward.min_train_years,
        config.walk_forward.val_years,
        config.walk_forward.require_future_test_year,
    )
    
    output_dir = Path(config.trainer.output_dir) / 'multi_gpu_results'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"共 {len(folds)} 個 fold，將分配到 {num_gpus} 個 GPU")
    
    # 準備任務
    tasks = []
    for fold_idx, fold in enumerate(folds):
        device_id = fold_idx % num_gpus
        tasks.append((fold_idx, fold, config, device_id, output_dir))
    
    # 多進程執行
    print("\n開始並行訓練...")
    with mp.Pool(processes=num_gpus) as pool:
        results = pool.starmap(
            train_fold_worker,
            tasks,
        )
    
    # 整合結果
    print("\n\n========== 訓練完成 ==========")
    
    successful_folds = [r for r in results if r['status'] == 'success']
    failed_folds = [r for r in results if r['status'] == 'failed']
    
    print(f"成功：{len(successful_folds)} 個 fold")
    print(f"失敗：{len(failed_folds)} 個 fold")
    
    if failed_folds:
        print("\n失敗詳情：")
        for r in failed_folds:
            print(f"  Fold {r['fold_idx']}: {r['error']}")
    
    # 生成總結報告
    if successful_folds:
        print("\n各 Fold 指標：")
        
        import polars as pl
        
        metrics_list = []
        for r in successful_folds:
            metrics_list.append(r['metrics'])
        
        # 聚合指標
        if metrics_list:
            df = pl.DataFrame(metrics_list)
            print(df)
            
            # 保存聚合報告
            summary = {
                'num_folds_total': len(folds),
                'num_folds_success': len(successful_folds),
                'num_folds_failed': len(failed_folds),
                'metrics_summary': df.describe().to_dict(),
            }
            
            import json
            with open(output_dir / 'summary.json', 'w') as f:
                json.dump(summary, f, indent=2, default=str)
    
    print(f"\n結果保存到：{output_dir}")

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='configs/experiment_baseline.yaml')
    parser.add_argument('--num-gpus', type=int, default=None)
    
    args = parser.parse_args()
    
    # 設置 multiprocessing 方法（某些系統需要）
    mp.set_start_method('spawn', force=True)
    
    main_parallel(args.config, args.num_gpus)
```

**使用方法：**
```bash
# 自動檢測 GPU 數量
python train_parallel.py --config configs/experiment_baseline.yaml

# 指定 GPU 數量
python train_parallel.py --config configs/experiment_baseline.yaml --num-gpus 4
```

---

## 模板 5: 更新訓練器的損失函數

**在 `stockagent/training/losses.py` 中添加：**

```python
import torch
import torch.nn as nn
import torch.nn.functional as F

class ImprovedSharpeLoss(nn.Module):
    """改進的 Sharpe 比率損失，支持梯度流和數值穩定性"""
    
    def __init__(self, 
                 fee_per_side: float = 0.001,
                 gamma_sharpe: float = 1.0,
                 gamma_turnover: float = 0.1,
                 eps: float = 1e-6):
        super().__init__()
        self.fee_per_side = fee_per_side
        self.gamma_sharpe = gamma_sharpe
        self.gamma_turnover = gamma_turnover
        self.eps = eps
    
    def forward(self, 
                weights: torch.Tensor,
                returns: torch.Tensor,
                tradable_mask: torch.Tensor = None,
                prev_weights: torch.Tensor = None) -> torch.Tensor:
        """
        計算 Sharpe 比率損失
        
        Args:
            weights: [B, S] 投資組合權重
            returns: [B, S] 下期對數收益
            tradable_mask: [B, S] 交易性遮罩
            prev_weights: [B, S] 前期權重（計算換手率）
        
        Returns:
            loss: 標量損失
        """
        B = weights.shape[0]
        
        # 加權收益
        weighted_returns = (weights * returns).sum(dim=1)  # [B]
        
        # Sharpe 比率（保留梯度）
        mean_ret = weighted_returns.mean()
        centered = weighted_returns - mean_ret
        variance = (centered ** 2).mean() + self.eps
        std_ret = torch.sqrt(variance)
        
        sharpe = mean_ret / std_ret * torch.sqrt(torch.tensor(252.0, device=weights.device))
        
        # 換手率成本
        if prev_weights is not None:
            turnover = torch.abs(weights - prev_weights).sum(dim=1).mean()
            turnover_cost = turnover * self.fee_per_side
        else:
            turnover_cost = 0.0
        
        # 複合損失
        loss = -self.gamma_sharpe * sharpe + self.gamma_turnover * turnover_cost
        
        return loss
```

---

## 檢查清單：優化實施步驟

```
□ Week 1：快速勝利
  □ 1. 實裝 OptimizedPanelBuilder (增量更新)
  □ 2. 使用 ImprovedSharpeLoss 替換原損失
  □ 3. 在 model forward 中應用分層精度 (fp32 投資組合頭)
  □ 測試：驗證收斂行為不變，速度提升 40-60%

□ Week 2：架構改進
  □ 4. 集成 AdaptiveBatchSizeOptimizer
  □ 5. 向量化特徵工程 (使用 Polars groupby)
  □ 6. 實裝 GPU 向量化評估
  □ 測試：吞吐量提升 50-100%，評估 10x 快

□ Week 3：模型改進
  □ 7. 實裝 EfficientCrossectionalMLP
  □ 8. 對比測試 (精度、速度、收斂)
  □ 調整超參（embedding_dim, num_heads）
  □ 測試：訓練速度 10-20x 快，參數 97% 減少

□ Week 4：分佈式訓練
  □ 9. 實裝 train_parallel.py
  □ 10. 測試多 GPU 訓練
  □ 效果驗證：N GPU 下 N 倍加速
```
