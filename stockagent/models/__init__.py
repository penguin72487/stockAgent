"""Model definitions."""

from stockagent.models.factory import build_model, model_hidden_dim_hint
from stockagent.models.ft_transformer import CrossSectionalFTTransformer
from stockagent.models.mlp import CrossSectionalMLP
from stockagent.models.tabular_resnet import CrossSectionalTabularResNet
from stockagent.models.tcn_hybrid_tabular_resnet import CrossSectionalTCNHybridTabularResNet
from stockagent.models.temporal_tabular_resnet import CrossSectionalTemporalTabularResNet
from stockagent.models.tree_models import CrossSectionalLightGBM, CrossSectionalXGBoost

__all__ = [
	"build_model",
	"model_hidden_dim_hint",
	"CrossSectionalFTTransformer",
	"CrossSectionalTabularResNet",
	"CrossSectionalTCNHybridTabularResNet",
	"CrossSectionalTemporalTabularResNet",
	"CrossSectionalMLP",
	"CrossSectionalLightGBM",
	"CrossSectionalXGBoost",
]
