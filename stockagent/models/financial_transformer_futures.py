"""The canonical candle/basis encoder with the existing TAIFEX action adapter."""

from stockagent.models.cross_sectional_all_futures import CrossSectionalAllFuturesModel
from stockagent.models.financial_transformer import FinancialTransformerModel


class FinancialTransformerFuturesModel(
    CrossSectionalAllFuturesModel, FinancialTransformerModel
):
    """Keep the FinancialTransformer stem and emit only physical-futures targets.

    Cooperative initialization reuses both existing implementations. Stock
    histories are information, never cash-stock orders. Whole-contract sizing
    belongs exclusively to the account's live-equity executor.
    """

