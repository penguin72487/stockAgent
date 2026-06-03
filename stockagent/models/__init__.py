"""Model definitions."""

from stockagent.models.factory import build_model, model_hidden_dim_hint
from stockagent.models.bottleneck_portfolio_autoencoder import BottleneckPortfolioAutoencoder
from stockagent.models.cross_sectional_temporal_portfolio_model import CrossSectionalTemporalPortfolioModel
from stockagent.models.efficient_tcn_tabular_set_portfolio import EfficientTCNTabularSetPortfolioModel
from stockagent.models.ft_transformer import CrossSectionalFTTransformer
from stockagent.models.mlp import CrossSectionalMLP
from stockagent.models.multi_stock_tcn import CrossSectionalMultiStockTCN
from stockagent.models.tabular_resnet import CrossSectionalTabularResNet
from stockagent.models.tcn_hybrid_tabular_resnet import CrossSectionalTCNHybridTabularResNet
from stockagent.models.temporal_tabular_resnet import CrossSectionalTemporalTabularResNet
from stockagent.models.tree_models import CrossSectionalLightGBM, CrossSectionalXGBoost

__all__ = [
	"build_model",
	"model_hidden_dim_hint",
	"BottleneckPortfolioAutoencoder",
	"CrossSectionalTemporalPortfolioModel",
	"EfficientTCNTabularSetPortfolioModel",
	"CrossSectionalFTTransformer",
	"CrossSectionalMultiStockTCN",
	"CrossSectionalTabularResNet",
	"CrossSectionalTCNHybridTabularResNet",
	"CrossSectionalTemporalTabularResNet",
	"CrossSectionalMLP",
	"CrossSectionalLightGBM",
	"CrossSectionalXGBoost",
]
