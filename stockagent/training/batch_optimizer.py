from __future__ import annotations

import torch
import torch.nn as nn


class AdaptiveBatchSizeOptimizer:
    """Runtime binary search to find max batch size given GPU memory."""
    
    def __init__(self, model: nn.Module, device: str = 'cuda:0', margin_mb: int = 500):
        self.model = model
        self.device = device
        self.margin_mb = margin_mb
    
    def estimate_sample_bytes(self, batch_size: int, lookback: int, num_symbols: int, 
                              num_features: int, training: bool = True) -> int:
        """Rough estimate of per-sample GPU memory in bytes."""
        hidden_dim = 256
        input_size = batch_size * lookback * num_symbols * num_features * 4
        embedding_dim = 16
        hidden_size = batch_size * num_symbols * hidden_dim * 4 * 2
        
        if training:
            activation_cache = input_size + hidden_size * 2
            param_bytes = sum(p.numel() * 4 for p in self.model.parameters())
            grad_bytes = param_bytes * 3
        else:
            activation_cache = 0
            grad_bytes = 0
        
        return int(input_size + hidden_size + activation_cache + grad_bytes / max(batch_size, 1))
    
    def find_max_batch_size(self, lookback: int, num_symbols: int, num_features: int,
                           min_bs: int = 1, max_bs: int = 512, training: bool = True) -> int:
        """Binary search for maximum batch size."""
        torch.cuda.empty_cache()
        
        props = torch.cuda.get_device_properties(self.device)
        total_mb = props.total_memory / 1024 / 1024
        reserved = torch.cuda.memory_reserved(self.device) / 1024 / 1024
        available_mb = total_mb - reserved - self.margin_mb
        
        low, high, best_bs = min_bs, max_bs, min_bs
        
        while low <= high:
            mid = (low + high) // 2
            est_mb = self.estimate_sample_bytes(mid, lookback, num_symbols, num_features, training) / 1024 / 1024
            
            if est_mb <= available_mb * 0.95:
                try:
                    self._test_fwd(mid, lookback, num_symbols, num_features, training)
                    best_bs = mid
                    low = mid + 1
                except RuntimeError:
                    high = mid - 1
            else:
                high = mid - 1
            
            torch.cuda.empty_cache()
        
        return best_bs
    
    def _test_fwd(self, batch_size: int, lookback: int, num_symbols: int, 
                  num_features: int, training: bool):
        """Test forward pass with given batch size."""
        torch.cuda.empty_cache()
        
        x = torch.randn(batch_size * num_symbols, lookback, num_features, 
                       device=self.device, dtype=torch.float32)
        mask = torch.ones(batch_size * num_symbols, dtype=torch.bool, device=self.device)
        
        if training:
            self.model.train()
            out = self.model(x.view(batch_size, lookback, num_symbols, num_features), mask)
            loss = out.sum()
            loss.backward()
        else:
            self.model.eval()
            with torch.no_grad():
                _ = self.model(x.view(batch_size, lookback, num_symbols, num_features), mask)
        
        torch.cuda.synchronize()
