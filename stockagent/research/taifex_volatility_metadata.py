"""Shared strategy registry for the TAIFEX Bid/Ask simulation dashboard.

The registry is the single source of truth for live strategy IDs, executable
leg recipes, hedge policies, dashboard labels, and the educational catalogue.
Keeping those fields together prevents the dashboard from describing a policy
that the simulation engine does not actually run.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Final


MODEL_BLACK_SCHOLES: Final[str] = "black_scholes"
MODEL_HESTON_SVI: Final[str] = "heston_svi_proxy"
MODEL_SABR: Final[str] = "sabr_hagan_beta1"
MODEL_LOCAL_VOL: Final[str] = "local_vol_surface_proxy"
MODEL_SLV: Final[str] = "slv_blend_proxy"
MODEL_ROUGH_VOL: Final[str] = "rough_vol_power_law_proxy"

VOLATILITY_MODEL_IDS: Final[tuple[str, ...]] = (
    MODEL_BLACK_SCHOLES,
    MODEL_HESTON_SVI,
    MODEL_SABR,
    MODEL_LOCAL_VOL,
    MODEL_SLV,
    MODEL_ROUGH_VOL,
)

VOLATILITY_MODEL_LABELS: Final[dict[str, str]] = {
    MODEL_BLACK_SCHOLES: "Black-Scholes（平坦 IV）",
    MODEL_HESTON_SVI: "Heston（raw-SVI 曲面代理）",
    MODEL_SABR: "SABR（Hagan beta=1）",
    MODEL_LOCAL_VOL: "Local Vol（平滑曲面 Delta 代理）",
    MODEL_SLV: "SLV（Local/Heston 曲面混合代理）",
    MODEL_ROUGH_VOL: "Rough Vol（冪律期限偏斜代理）",
}

VOLATILITY_MODEL_IMPLEMENTATION: Final[dict[str, str]] = {
    MODEL_BLACK_SCHOLES: "direct_formula",
    MODEL_HESTON_SVI: "surface_proxy_not_heston_characteristic_function",
    MODEL_SABR: "direct_hagan_asymptotic_formula_beta_1",
    MODEL_LOCAL_VOL: "surface_delta_proxy_not_dupire_pde",
    MODEL_SLV: "surface_blend_proxy_not_particle_calibrated_slv",
    MODEL_ROUGH_VOL: "power_law_surface_proxy_not_rough_bergomi_monte_carlo",
}

CLASSIC_VARIANT_ID: Final[str] = "classic_opening_straddle"
PUT_CALL_PARITY_TX_STRATEGY_ID: Final[str] = "put_call_parity_tx"
ROLLING_ITM_LONG_STRADDLE_ID: Final[str] = "long_rolling_itm_straddle"
ROLLING_ITM_SHORT_STRADDLE_ID: Final[str] = "short_rolling_itm_straddle"
ROLLING_OTM_LONG_STRADDLE_ID: Final[str] = "long_rolling_otm_straddle"
ROLLING_OTM_SHORT_STRADDLE_ID: Final[str] = "short_rolling_otm_straddle"
ROLLING_STRADDLE_IDS: Final[tuple[str, ...]] = (
    ROLLING_ITM_LONG_STRADDLE_ID,
    ROLLING_ITM_SHORT_STRADDLE_ID,
    ROLLING_OTM_LONG_STRADDLE_ID,
    ROLLING_OTM_SHORT_STRADDLE_ID,
)
MODEL_VARIANT_PREFIX: Final[str] = "daily_vol_model_gamma__"
STRATEGY_MODE_DAILY: Final[str] = "daily_close_next_open"
STRATEGY_MODE_INTRADAY_FUTURES: Final[str] = "intraday_futures"
STRATEGY_MODES: Final[tuple[str, ...]] = (
    STRATEGY_MODE_DAILY,
    STRATEGY_MODE_INTRADAY_FUTURES,
)
CATALOG_EXPANSION_ENTRY_NEXT_CYCLE: Final[str] = "next_cycle"
CATALOG_EXPANSION_ENTRY_IMMEDIATE_LIVE: Final[str] = "immediate_live"
CATALOG_EXPANSION_ENTRY_POLICIES: Final[tuple[str, ...]] = (
    CATALOG_EXPANSION_ENTRY_NEXT_CYCLE,
    CATALOG_EXPANSION_ENTRY_IMMEDIATE_LIVE,
)


def _expand_exposure_groups(
    groups: tuple[tuple[str, tuple[str, ...]], ...],
) -> dict[str, str]:
    result: dict[str, str] = {}
    for exposure_type, strategy_ids in groups:
        for strategy_id in strategy_ids:
            if strategy_id in result:
                raise RuntimeError(f"duplicate exposure classification: {strategy_id}")
            result[strategy_id] = exposure_type
    return result


_MODEL_STRATEGY_IDS: Final[tuple[str, ...]] = tuple(
    f"{MODEL_VARIANT_PREFIX}{model_id}" for model_id in VOLATILITY_MODEL_IDS
)
BS_DELTA_BAND_STRATEGY_IDS: Final[tuple[str, ...]] = (
    "bs_delta_band_10",
    "bs_delta_band_20",
    "bs_delta_band_30",
    "bs_delta_band_50",
)
BS_PARTIAL_DELTA_STRATEGY_IDS: Final[tuple[str, ...]] = (
    "bs_partial_hedge_25",
    "bs_partial_hedge_50",
    "bs_partial_hedge_75",
)
BS_OVER_DELTA_STRATEGY_IDS: Final[tuple[str, ...]] = (
    "bs_overhedge_125",
    "bs_overhedge_150",
)
BS_GAMMA_PRICE_GRID_STRATEGY_IDS: Final[tuple[str, ...]] = (
    "bs_gamma_price_grid_25",
    "bs_gamma_price_grid_50",
    "bs_gamma_price_grid_100",
)
BS_GAMMA_TIME_GRID_STRATEGY_IDS: Final[tuple[str, ...]] = (
    "bs_gamma_time_grid_5m",
    "bs_gamma_time_grid_15m",
    "bs_gamma_time_grid_30m",
)

_DIRECTIONAL_EXPOSURE_BY_ID: Final[dict[str, str]] = _expand_exposure_groups(
    (
        (
            "bullish",
            (
                "long_atm_call",
                "long_strap_2c1p",
                "long_call_fan",
                "underlying_hedge_future_long",
                "protective_put_with_future",
                "naked_short_put",
                "bull_call_debit",
                "bull_put_credit",
                "bull_risk_reversal",
                "call_ratio_spread",
                "call_ratio_backspread",
                "synthetic_long",
                "covered_call",
                "collar",
                "jade_lizard",
                "bullish_seagull",
            ),
        ),
        (
            "bearish",
            (
                "long_atm_put",
                "long_strip_1c2p",
                "long_put_fan",
                "underlying_hedge_future_short",
                "protective_call_with_future",
                "naked_short_call",
                "bear_put_debit",
                "bear_call_credit",
                "bear_risk_reversal",
                "put_ratio_spread",
                "put_ratio_backspread",
                "synthetic_short",
            ),
        ),
        (
            "neutral",
            (
                CLASSIC_VARIANT_ID,
                "long_otm_strangle_1",
                "long_otm_strangle_2",
                "long_itm_guts_1",
                "long_wide_wings",
                "short_atm_straddle",
                "short_otm_strangle",
                "short_itm_guts",
                "long_call_butterfly",
                "long_put_butterfly",
                "iron_butterfly",
                "iron_condor",
                "variance_vol_swaps",
                *ROLLING_STRADDLE_IDS,
            ),
        ),
        (
            "hedged_neutral",
            (
                *_MODEL_STRATEGY_IDS,
                *BS_DELTA_BAND_STRATEGY_IDS,
                *BS_PARTIAL_DELTA_STRATEGY_IDS,
                *BS_OVER_DELTA_STRATEGY_IDS,
                *BS_GAMMA_PRICE_GRID_STRATEGY_IDS,
                *BS_GAMMA_TIME_GRID_STRATEGY_IDS,
                "conversion",
                "reversal",
                PUT_CALL_PARITY_TX_STRATEGY_ID,
            ),
        ),
        (
            "relative_neutral",
            (
                "long_box",
                "calendar_spread",
                "diagonal_spread",
                "calendar_box",
                "skew_term_carry",
                "dispersion",
                "mean_reversion",
            ),
        ),
        (
            "adaptive",
            (
                "dynamic_recenter",
                "fixed_tp_sl",
                "trend_breakout",
                "market_making",
                "ml_rl_policy",
            ),
        ),
    )
)

_VOLATILITY_EXPOSURE_BY_ID: Final[dict[str, str]] = _expand_exposure_groups(
    (
        (
            "long_volatility",
            (
                CLASSIC_VARIANT_ID,
                "long_atm_call",
                "long_atm_put",
                "long_otm_strangle_1",
                "long_otm_strangle_2",
                "long_itm_guts_1",
                "long_strap_2c1p",
                "long_strip_1c2p",
                "long_call_fan",
                "long_put_fan",
                "long_wide_wings",
                *_MODEL_STRATEGY_IDS,
                *BS_DELTA_BAND_STRATEGY_IDS,
                *BS_PARTIAL_DELTA_STRATEGY_IDS,
                *BS_OVER_DELTA_STRATEGY_IDS,
                *BS_GAMMA_PRICE_GRID_STRATEGY_IDS,
                *BS_GAMMA_TIME_GRID_STRATEGY_IDS,
                "protective_put_with_future",
                "protective_call_with_future",
                "call_ratio_backspread",
                "put_ratio_backspread",
                ROLLING_ITM_LONG_STRADDLE_ID,
                ROLLING_OTM_LONG_STRADDLE_ID,
            ),
        ),
        (
            "short_volatility",
            (
                "naked_short_call",
                "naked_short_put",
                "short_atm_straddle",
                "short_otm_strangle",
                "short_itm_guts",
                "bull_put_credit",
                "bear_call_credit",
                "iron_butterfly",
                "iron_condor",
                "call_ratio_spread",
                "put_ratio_spread",
                "covered_call",
                "jade_lizard",
                ROLLING_ITM_SHORT_STRADDLE_ID,
                ROLLING_OTM_SHORT_STRADDLE_ID,
            ),
        ),
        (
            "mixed_relative_volatility",
            (
                "bull_call_debit",
                "bear_put_debit",
                "long_call_butterfly",
                "long_put_butterfly",
                "bull_risk_reversal",
                "bear_risk_reversal",
                "collar",
                "bullish_seagull",
                "calendar_spread",
                "diagonal_spread",
                "calendar_box",
                "skew_term_carry",
                "dispersion",
            ),
        ),
        (
            "volatility_neutral",
            (
                "long_box",
                "synthetic_long",
                "synthetic_short",
                "conversion",
                "reversal",
                PUT_CALL_PARITY_TX_STRATEGY_ID,
            ),
        ),
        (
            "signal_dependent",
            (
                "dynamic_recenter",
                "fixed_tp_sl",
                "variance_vol_swaps",
                "ml_rl_policy",
            ),
        ),
        (
            "not_applicable",
            (
                "underlying_hedge_future_long",
                "underlying_hedge_future_short",
                "trend_breakout",
                "mean_reversion",
                "market_making",
            ),
        ),
    )
)

_HEDGE_TYPE_BY_ID: Final[dict[str, str]] = _expand_exposure_groups(
    (
        (
            "none",
            (
                CLASSIC_VARIANT_ID,
                "long_atm_call",
                "long_atm_put",
                "long_otm_strangle_1",
                "long_otm_strangle_2",
                "long_itm_guts_1",
                "long_strap_2c1p",
                "long_strip_1c2p",
                "long_call_fan",
                "long_put_fan",
                "long_wide_wings",
                "naked_short_call",
                "naked_short_put",
                "short_atm_straddle",
                "short_otm_strangle",
                "short_itm_guts",
            ),
        ),
        ("dynamic_delta", _MODEL_STRATEGY_IDS),
        ("delta_band", BS_DELTA_BAND_STRATEGY_IDS),
        ("partial_delta", BS_PARTIAL_DELTA_STRATEGY_IDS),
        ("over_delta", BS_OVER_DELTA_STRATEGY_IDS),
        ("price_grid_delta", BS_GAMMA_PRICE_GRID_STRATEGY_IDS),
        ("time_grid_delta", BS_GAMMA_TIME_GRID_STRATEGY_IDS),
        (
            "directional_linear",
            ("underlying_hedge_future_long", "underlying_hedge_future_short"),
        ),
        (
            "fixed_future_overlay",
            (
                "protective_put_with_future",
                "protective_call_with_future",
                "covered_call",
                "collar",
                "bullish_seagull",
            ),
        ),
        (
            "option_leg_offset",
            (
                "bull_call_debit",
                "bear_put_debit",
                "bull_put_credit",
                "bear_call_credit",
                "long_call_butterfly",
                "long_put_butterfly",
                "iron_butterfly",
                "iron_condor",
                "bull_risk_reversal",
                "bear_risk_reversal",
                "call_ratio_spread",
                "call_ratio_backspread",
                "put_ratio_spread",
                "put_ratio_backspread",
                "jade_lizard",
            ),
        ),
        (
            "parity_locked",
            ("long_box", "conversion", "reversal", PUT_CALL_PARITY_TX_STRATEGY_ID),
        ),
        ("dynamic_option_roll", ROLLING_STRADDLE_IDS),
        ("synthetic_linear", ("synthetic_long", "synthetic_short")),
        (
            "contract_pending",
            (
                "calendar_spread",
                "diagonal_spread",
                "calendar_box",
                "dynamic_recenter",
                "fixed_tp_sl",
                "skew_term_carry",
                "dispersion",
                "variance_vol_swaps",
                "trend_breakout",
                "mean_reversion",
                "market_making",
                "ml_rl_policy",
            ),
        ),
    )
)

EXPOSURE_TAXONOMY: Final[dict[str, dict[str, dict[str, str]]]] = {
    "directional_exposure": {
        "bullish": {
            "label": "多方／偏多",
            "definition": "主要獲利區或線性斜率偏向指數上漲。",
        },
        "bearish": {
            "label": "空方／偏空",
            "definition": "主要獲利區或線性斜率偏向指數下跌。",
        },
        "neutral": {
            "label": "方向中性",
            "definition": "策略設計不先押單一方向，但實際 Delta 會隨價格與時間漂移。",
        },
        "hedged_neutral": {
            "label": "中性對沖目標",
            "definition": "以期貨或 parity 組合追求方向中性；不代表每一刻實測 Delta 都是零。",
        },
        "relative_neutral": {
            "label": "相對價值中性",
            "definition": "交易腿間、履約價或期限關係，而非單一方向。",
        },
        "adaptive": {
            "label": "動態／訊號決定",
            "definition": "方向由觸發條件、庫存或模型訊號決定。",
        },
    },
    "volatility_exposure": {
        "long_volatility": {
            "label": "多波動",
            "definition": "主要持有正凸性／長波動部位，通常支付權利金。",
        },
        "short_volatility": {
            "label": "空波動",
            "definition": "主要收取權利金／Theta，承擔波動放大與尾端風險。",
        },
        "mixed_relative_volatility": {
            "label": "混合／曲面波動",
            "definition": "淨 Vega 會依履約價、價格、期限與曲面改變，不能只靠口數判為純多或純空。",
        },
        "volatility_neutral": {
            "label": "波動中性／Parity",
            "definition": "理論 payoff 主要抵銷同履約價波動敏感度，仍有成交與基差風險。",
        },
        "signal_dependent": {
            "label": "多空波動由訊號決定",
            "definition": "需由尚未固定的交易訊號決定波動方向。",
        },
        "not_applicable": {
            "label": "非選擇權波動策略",
            "definition": "目前分類為線性期貨、關係或流動性策略。",
        },
    },
    "hedge_type": {
        "none": {"label": "無額外避險", "definition": "除原始策略腿外不另加對沖腿。"},
        "dynamic_delta": {
            "label": "動態 Delta 中性",
            "definition": "依因果模型 Delta 動態調整期貨目標。",
        },
        "delta_band": {
            "label": "Delta band 中性",
            "definition": "超過 no-trade band 才調回中性目標。",
        },
        "partial_delta": {
            "label": "部分 Delta 避險",
            "definition": "只執行完整 Delta 中性目標的一部分。",
        },
        "over_delta": {
            "label": "過度 Delta 避險",
            "definition": "執行超過完整 Delta 中性目標的期貨量。",
        },
        "price_grid_delta": {
            "label": "點數網格 Gamma Scalping",
            "definition": "標的自上次成功重平衡移動指定點數後，才重新計算並交易 Delta 中性期貨量。",
        },
        "time_grid_delta": {
            "label": "時間網格 Gamma Scalping",
            "definition": "按固定分鐘間隔重新計算並交易 Delta 中性期貨量。",
        },
        "directional_linear": {
            "label": "未避險線性部位",
            "definition": "固定方向期貨本身是主要曝險，不是對沖。",
        },
        "fixed_future_overlay": {
            "label": "固定期貨覆蓋",
            "definition": "以固定指數等價期貨疊加或覆蓋選擇權腿。",
        },
        "option_leg_offset": {
            "label": "選擇權腿間對沖",
            "definition": "多空選擇權腿彼此限制或改變 payoff；不保證 Delta 中性。",
        },
        "parity_locked": {
            "label": "Parity／鎖定對沖",
            "definition": "以 put-call parity 或 box 結構鎖定相對現金流。",
        },
        "dynamic_option_roll": {
            "label": "動態選擇權換腿",
            "definition": "依即時價內／價外狀態先平舊腿，再以嚴格較晚的五檔報價換到新 ATM 腿。",
        },
        "synthetic_linear": {
            "label": "合成線性",
            "definition": "Call/Put 組合抵銷部分波動敏感度並形成方向性線性 payoff。",
        },
        "contract_pending": {
            "label": "執行契約待完成",
            "definition": "尚未具備可驗證的即時執行、保證金或結算契約。",
        },
    },
}

EXPOSURE_RATIO_BASIS: Final[str] = (
    "Option long/short ratios use absolute contract quantities in the minimum "
    "strategy recipe or current ledger. They are not Delta, Vega, notional, "
    "premium, margin, or scenario-risk ratios."
)


@dataclass(frozen=True, slots=True)
class StrategySpec:
    strategy_id: str
    label: str
    family: str
    category: str
    summary: str
    entry_rule: str
    exit_rule: str
    risk_note: str
    implementation_level: str
    availability: str
    broker_monitoring: str
    option_legs: tuple[tuple[str, int, int], ...] = ()
    hedge_policy: str = "none"
    hedge_parameter: float | None = None
    option_roll_policy: str = "none"
    blocker: str | None = None

    def exposure_payload(self) -> dict[str, object]:
        direction = _DIRECTIONAL_EXPOSURE_BY_ID[self.strategy_id]
        volatility = _VOLATILITY_EXPOSURE_BY_ID[self.strategy_id]
        hedge_type = _HEDGE_TYPE_BY_ID[self.strategy_id]
        option_long_contracts = sum(
            quantity for _right, _offset, quantity in self.option_legs if quantity > 0
        )
        option_short_contracts = sum(
            -quantity for _right, _offset, quantity in self.option_legs if quantity < 0
        )
        option_gross_contracts = option_long_contracts + option_short_contracts
        ratio_known = option_gross_contracts > 0
        if ratio_known:
            long_ratio = option_long_contracts / option_gross_contracts
            short_ratio = option_short_contracts / option_gross_contracts
            net_ratio = (
                option_long_contracts - option_short_contracts
            ) / option_gross_contracts
            ratio_label = (
                f"多 {long_ratio:.0%} / 空 {short_ratio:.0%} "
                f"({option_long_contracts}:{option_short_contracts} 口)"
            )
            ratio_status = "known_contract_quantity"
        else:
            long_ratio = short_ratio = net_ratio = None
            ratio_label = (
                "執行契約待完成"
                if self.availability == "blocked_contract"
                else "無選擇權腿"
            )
            ratio_status = (
                "blocked_contract"
                if self.availability == "blocked_contract"
                else "not_applicable"
            )
        fixed_future_target = (
            self.hedge_parameter
            if self.hedge_policy in {"fixed_future", "fixed_index_equivalent"}
            else None
        )
        return {
            "directional_exposure": direction,
            "directional_exposure_label": EXPOSURE_TAXONOMY["directional_exposure"][
                direction
            ]["label"],
            "volatility_exposure": volatility,
            "volatility_exposure_label": EXPOSURE_TAXONOMY["volatility_exposure"][
                volatility
            ]["label"],
            "hedge_type": hedge_type,
            "hedge_type_label": EXPOSURE_TAXONOMY["hedge_type"][hedge_type]["label"],
            "design_option_long_contracts": option_long_contracts,
            "design_option_short_contracts": option_short_contracts,
            "design_option_gross_contracts": option_gross_contracts,
            "design_option_long_ratio": long_ratio,
            "design_option_short_ratio": short_ratio,
            "design_option_net_ratio": net_ratio,
            "design_option_ratio_label": ratio_label,
            "design_option_ratio_status": ratio_status,
            "design_futures_target_index_equivalent": fixed_future_target,
            "exposure_ratio_basis": EXPOSURE_RATIO_BASIS,
        }

    def dashboard_payload(self) -> dict[str, object]:
        payload = asdict(self)
        payload["option_legs"] = [
            {"right": right, "strike_offset": offset, "quantity": quantity}
            for right, offset, quantity in self.option_legs
        ]
        payload.update(self.exposure_payload())
        return payload


def _live(
    strategy_id: str,
    label: str,
    family: str,
    category: str,
    summary: str,
    entry_rule: str,
    exit_rule: str,
    risk_note: str,
    implementation_level: str,
    *,
    option_legs: tuple[tuple[str, int, int], ...] = (),
    hedge_policy: str = "none",
    hedge_parameter: float | None = None,
    option_roll_policy: str = "none",
    broker_monitoring: str = "ideal_only",
) -> StrategySpec:
    return StrategySpec(
        strategy_id=strategy_id,
        label=label,
        family=family,
        category=category,
        summary=summary,
        entry_rule=entry_rule,
        exit_rule=exit_rule,
        risk_note=risk_note,
        implementation_level=implementation_level,
        availability="live_ideal",
        broker_monitoring=broker_monitoring,
        option_legs=option_legs,
        hedge_policy=hedge_policy,
        hedge_parameter=hedge_parameter,
        option_roll_policy=option_roll_policy,
    )


_ATM_STRADDLE: Final[tuple[tuple[str, int, int], ...]] = (
    ("C", 0, 1),
    ("P", 0, 1),
)

_STATIC_LIVE_SPECS: Final[tuple[StrategySpec, ...]] = (
    _live(
        CLASSIC_VARIANT_ID,
        "經典開盤 ATM Straddle",
        "long_volatility",
        "選擇權經典",
        "同履約價各買一口 Call 與 Put，方向中性地買進波動與凸性。",
        "新週期第一個完整可成交 ATM Call/Put book；買進一律取 best ask。",
        "週到期使用 TAIFEX 官方最後結算價現金結算。",
        "最大損失為權利金、手續費與稅；時間價值衰減會持續侵蝕部位。",
        "direct_long_premium",
        option_legs=_ATM_STRADDLE,
        broker_monitoring="shared_reference_option_position",
    ),
    _live(
        "long_atm_call",
        "ATM Long Call",
        "directional_convexity",
        "方向凸性",
        "只買 ATM Call，保留上漲凸性並把最大損失限制在權利金。",
        "週期開啟時買進 ATM Call best ask。",
        "持有至官方週結算。",
        "方向判斷錯誤或盤整時可能損失全部權利金。",
        "direct_long_premium",
        option_legs=(("C", 0, 1),),
    ),
    _live(
        "long_atm_put",
        "ATM Long Put",
        "directional_convexity",
        "方向凸性",
        "只買 ATM Put，保留下跌凸性並把最大損失限制在權利金。",
        "週期開啟時買進 ATM Put best ask。",
        "持有至官方週結算。",
        "方向判斷錯誤或盤整時可能損失全部權利金。",
        "direct_long_premium",
        option_legs=(("P", 0, 1),),
    ),
    _live(
        "long_otm_strangle_1",
        "一檔寬 Long Strangle",
        "long_volatility",
        "選擇權經典",
        "買進 ATM 上一檔 Call 與下一檔 Put，以較低權利金等待較大行情。",
        "用同週序列履約價階梯的 +1 Call、-1 Put best ask。",
        "持有至官方週結算。",
        "損益兩平區比 Straddle 更寬，小波動時更容易損失權利金。",
        "direct_long_premium",
        option_legs=(("C", 1, 1), ("P", -1, 1)),
    ),
    _live(
        "long_otm_strangle_2",
        "兩檔寬 Long Strangle",
        "long_volatility",
        "選擇權經典",
        "買進更遠 OTM Call/Put，以更低成本換取尾端凸性。",
        "用同週序列履約價階梯的 +2 Call、-2 Put best ask。",
        "持有至官方週結算。",
        "需要更大幅度行情才能覆蓋雙邊權利金。",
        "direct_long_premium",
        option_legs=(("C", 2, 1), ("P", -2, 1)),
    ),
    _live(
        "long_itm_guts_1",
        "一檔寬 Long Guts",
        "long_volatility",
        "選擇權經典",
        "買進一檔 ITM Call 與 Put；權利金較高，但一開始就含雙邊內含價值。",
        "用同週序列 -1 Call、+1 Put best ask。",
        "持有至官方週結算。",
        "資金占用高，仍會承受時間價值與 bid/ask 成本。",
        "direct_long_premium",
        option_legs=(("C", -1, 1), ("P", 1, 1)),
    ),
    _live(
        "long_strap_2c1p",
        "Long Strap（2C+1P）",
        "directional_volatility",
        "偏多波動",
        "ATM Straddle 多加一口 Call，在保留雙向凸性時提高上漲敏感度。",
        "同履約價買進兩口 Call 與一口 Put。",
        "持有至官方週結算。",
        "偏多結構支付較多權利金；盤整或下跌不足時 Theta 較重。",
        "direct_long_premium",
        option_legs=(("C", 0, 2), ("P", 0, 1)),
    ),
    _live(
        "long_strip_1c2p",
        "Long Strip（1C+2P）",
        "directional_volatility",
        "偏空波動",
        "ATM Straddle 多加一口 Put，在保留雙向凸性時提高下跌敏感度。",
        "同履約價買進一口 Call 與兩口 Put。",
        "持有至官方週結算。",
        "偏空結構支付較多權利金；盤整或上漲不足時 Theta 較重。",
        "direct_long_premium",
        option_legs=(("C", 0, 1), ("P", 0, 2)),
    ),
    _live(
        "long_call_fan",
        "Long Call Fan",
        "directional_convexity",
        "上漲尾端",
        "同時買進 ATM、上一檔與上兩檔 Call，觀察多履約價上漲凸性。",
        "買進 Call 履約價 offset 0/+1/+2 的 best ask。",
        "持有至官方週結算。",
        "三腿權利金與價差成本較高，並非免費放大上漲曝險。",
        "direct_long_premium_basket",
        option_legs=(("C", 0, 1), ("C", 1, 1), ("C", 2, 1)),
    ),
    _live(
        "long_put_fan",
        "Long Put Fan",
        "directional_convexity",
        "下跌尾端",
        "同時買進 ATM、下一檔與下兩檔 Put，觀察多履約價下跌凸性。",
        "買進 Put 履約價 offset 0/-1/-2 的 best ask。",
        "持有至官方週結算。",
        "三腿權利金與價差成本較高，並非免費放大下跌曝險。",
        "direct_long_premium_basket",
        option_legs=(("P", 0, 1), ("P", -1, 1), ("P", -2, 1)),
    ),
    _live(
        "long_wide_wings",
        "Long Wide Wings",
        "tail_convexity",
        "雙尾風險",
        "只買兩檔外的 Call 與 Put，作為低成本、低命中率的雙尾凸性基準。",
        "買進 +2 Call 與 -2 Put best ask。",
        "持有至官方週結算。",
        "絕大多數小行情可能雙腿歸零；不能用少數尾端獲利推論穩定性。",
        "direct_long_premium",
        option_legs=(("C", 2, 1), ("P", -2, 1)),
    ),
)


_ROLLING_STRADDLE_LIVE_SPECS: Final[tuple[StrategySpec, ...]] = (
    _live(
        ROLLING_ITM_LONG_STRADDLE_ID,
        "Long Rolling ITM Straddle",
        "dynamic_straddle_roll",
        "動態選擇權",
        "開盤買進 ATM Call 與 Put；其後只把已進入價內的舊腿平倉並換成同權利別的新 ATM 腿。",
        "週期第一組完整 ATM Call/Put 五檔以 Ask 建倉；TX 中價只使用決策前已收到的報價。",
        "Call 履約價低於 TX 中價或 Put 履約價高於 TX 中價時產生換腿訊號；舊腿賣 Bid、新 ATM 腿買 Ask，兩者都必須是訊號後嚴格較晚且完整的五檔，最後以官方週結算。",
        "頻繁穿越履約價會反覆支付價差、手續費與稅；長方最大損失不只初始權利金，還包含後續換腿投入。",
        "live_causal_atomic_single_leg_itm_to_atm_roll",
        option_legs=_ATM_STRADDLE,
        option_roll_policy="itm_to_atm",
    ),
    _live(
        ROLLING_ITM_SHORT_STRADDLE_ID,
        "Short Rolling ITM Straddle",
        "dynamic_straddle_roll",
        "動態選擇權",
        "開盤賣出 ATM Call 與 Put；其後只把已進入價內的舊腿回補並重新賣出同權利別的新 ATM 腿。",
        "週期第一組完整 ATM Call/Put 五檔以 Bid 建倉；逐腿採保守裸賣保證金，不套未驗證的組合折抵。",
        "Call 履約價低於 TX 中價或 Put 履約價高於 TX 中價時產生換腿訊號；舊腿回補 Ask、新 ATM 腿賣 Bid，兩者都必須是訊號後嚴格較晚且完整的五檔，最後以官方週結算。",
        "裸賣雙尾損失可能遠超權利金；追著價內腿換倉會實現虧損並承受價差、稅、流動性與強平風險。",
        "live_causal_atomic_single_leg_itm_to_atm_roll_conservative_naked_margin",
        option_legs=(("C", 0, -1), ("P", 0, -1)),
        option_roll_policy="itm_to_atm",
    ),
    _live(
        ROLLING_OTM_LONG_STRADDLE_ID,
        "Long Rolling OTM Straddle",
        "dynamic_straddle_roll",
        "動態選擇權",
        "開盤買進 ATM Call 與 Put；其後只把已進入價外的舊腿平倉並換成同權利別的新 ATM 腿。",
        "週期第一組完整 ATM Call/Put 五檔以 Ask 建倉；TX 中價只使用決策前已收到的報價。",
        "Call 履約價高於 TX 中價或 Put 履約價低於 TX 中價時產生換腿訊號；舊腿賣 Bid、新 ATM 腿買 Ask，兩者都必須是訊號後嚴格較晚且完整的五檔，最後以官方週結算。",
        "反覆把價外腿拉回 ATM 會持續支付較高時間價值、價差、手續費與稅，震盪行情可能快速侵蝕資金。",
        "live_causal_atomic_single_leg_otm_to_atm_roll",
        option_legs=_ATM_STRADDLE,
        option_roll_policy="otm_to_atm",
    ),
    _live(
        ROLLING_OTM_SHORT_STRADDLE_ID,
        "Short Rolling OTM Straddle",
        "dynamic_straddle_roll",
        "動態選擇權",
        "開盤賣出 ATM Call 與 Put；其後只把已進入價外的舊腿回補並重新賣出同權利別的新 ATM 腿。",
        "週期第一組完整 ATM Call/Put 五檔以 Bid 建倉；逐腿採保守裸賣保證金，不套未驗證的組合折抵。",
        "Call 履約價高於 TX 中價或 Put 履約價低於 TX 中價時產生換腿訊號；舊腿回補 Ask、新 ATM 腿賣 Bid，兩者都必須是訊號後嚴格較晚且完整的五檔，最後以官方週結算。",
        "裸賣仍承擔雙尾與保證金風險；重賣 ATM 雖提高收取權利金，也會重設最大 Gamma 區並增加換手成本。",
        "live_causal_atomic_single_leg_otm_to_atm_roll_conservative_naked_margin",
        option_legs=(("C", 0, -1), ("P", 0, -1)),
        option_roll_policy="otm_to_atm",
    ),
)


_MODEL_LIVE_SPECS: Final[tuple[StrategySpec, ...]] = tuple(
    _live(
        f"{MODEL_VARIANT_PREFIX}{model_id}",
        f"{VOLATILITY_MODEL_LABELS[model_id]} Gamma Scalping",
        "model_gamma_scalping",
        "Gamma Scalping／模型避險",
        "持有正 Gamma 的 ATM Long Straddle，使用即時 Bid/Ask IV 曲面估計 Delta，再以避險期貨反向調整。",
        "ATM Straddle 以 best ask 建倉；曲面只讀決策時間以前已收到的 book。",
        "期貨每個日／夜盤在截止前平倉；選擇權持有至官方週結算。",
        "Gamma scalping 只有已實現波動足以覆蓋 Theta、價差、稅費與模型誤差時才可能有利；模型名稱不等於完整隨機過程校準。",
        VOLATILITY_MODEL_IMPLEMENTATION[model_id],
        option_legs=_ATM_STRADDLE,
        hedge_policy=f"vol_model:{model_id}",
        broker_monitoring="mirrored_futures_plus_shared_option_reference",
    )
    for model_id in VOLATILITY_MODEL_IDS
)


_POLICY_LIVE_SPECS: Final[tuple[StrategySpec, ...]] = (
    _live(
        "bs_delta_band_10",
        "BS Delta-band 0.10",
        "delta_band_gamma_scalping",
        "交易成本控制",
        "只有淨 Delta 絕對值超過 0.10 才把 ATM Straddle 拉回 BS Delta-neutral。",
        "ATM Straddle 建倉後，以因果 Bid/Ask 曲面計算 BS Delta。",
        "期貨盤末歸零；選擇權週結算。",
        "較緊的 band 對方向漂移反應快，但通常會增加價差、稅費與 hedge churn。",
        "black_scholes_delta_band",
        option_legs=_ATM_STRADDLE,
        hedge_policy="bs_delta_band",
        hedge_parameter=0.10,
    ),
    _live(
        "bs_delta_band_20",
        "BS Delta-band 0.20",
        "delta_band_gamma_scalping",
        "交易成本控制",
        "只有淨 Delta 絕對值超過 0.20 才把 ATM Straddle 拉回 BS Delta-neutral。",
        "ATM Straddle 建倉後，以因果 Bid/Ask 曲面計算 BS Delta。",
        "期貨盤末歸零；選擇權週結算。",
        "較少換手但會容忍方向曝險；門檻不是獲利保證。",
        "black_scholes_delta_band",
        option_legs=_ATM_STRADDLE,
        hedge_policy="bs_delta_band",
        hedge_parameter=0.20,
    ),
    _live(
        "bs_delta_band_30",
        "BS Delta-band 0.30",
        "delta_band_gamma_scalping",
        "交易成本控制",
        "只有淨 Delta 絕對值超過 0.30 才避險，作為較寬 no-trade zone。",
        "ATM Straddle 建倉後，以因果 Bid/Ask 曲面計算 BS Delta。",
        "期貨盤末歸零；選擇權週結算。",
        "換手較低但未避險方向風險更高。",
        "black_scholes_delta_band",
        option_legs=_ATM_STRADDLE,
        hedge_policy="bs_delta_band",
        hedge_parameter=0.30,
    ),
    _live(
        "bs_delta_band_50",
        "BS Delta-band 0.50",
        "delta_band_gamma_scalping",
        "交易成本控制",
        "只有淨 Delta 絕對值超過 0.50 才避險，作為低換手的寬 no-trade zone。",
        "ATM Straddle 建倉後，以因果 Bid/Ask 曲面計算 BS Delta。",
        "期貨盤末歸零；選擇權週結算。",
        "換手最低但允許較大的方向曝險，跳空時可能明顯偏離 Delta-neutral。",
        "black_scholes_delta_band",
        option_legs=_ATM_STRADDLE,
        hedge_policy="bs_delta_band",
        hedge_parameter=0.50,
    ),
    _live(
        "bs_partial_hedge_25",
        "BS 25% Partial Hedge",
        "partial_delta_hedge",
        "避險比例",
        "只執行 BS 完整 Delta-neutral 目標的四分之一，保留較多方向曝險。",
        "ATM Straddle 與因果 BS Delta；期貨目標乘 0.25 後才整數化。",
        "期貨盤末歸零；選擇權週結算。",
        "換手較少，但多數 Delta 風險仍留在帳本內。",
        "black_scholes_scaled_delta",
        option_legs=_ATM_STRADDLE,
        hedge_policy="bs_delta_scale",
        hedge_parameter=0.25,
    ),
    _live(
        "bs_partial_hedge_50",
        "BS 50% Partial Hedge",
        "partial_delta_hedge",
        "避險比例",
        "只執行 BS 完整 Delta-neutral 目標的一半，保留部分方向曝險。",
        "ATM Straddle 與因果 BS Delta；期貨目標乘 0.50 後才整數化。",
        "期貨盤末歸零；選擇權週結算。",
        "降低 hedge churn 的同時會增加未避險損益波動。",
        "black_scholes_scaled_delta",
        option_legs=_ATM_STRADDLE,
        hedge_policy="bs_delta_scale",
        hedge_parameter=0.50,
    ),
    _live(
        "bs_partial_hedge_75",
        "BS 75% Partial Hedge",
        "partial_delta_hedge",
        "避險比例",
        "執行 BS 完整 Delta-neutral 目標的四分之三，在換手與方向風險間取折衷。",
        "ATM Straddle 與因果 BS Delta；期貨目標乘 0.75 後才整數化。",
        "期貨盤末歸零；選擇權週結算。",
        "仍有未避險 Delta，且整數口數會令實際比例偏離 75%。",
        "black_scholes_scaled_delta",
        option_legs=_ATM_STRADDLE,
        hedge_policy="bs_delta_scale",
        hedge_parameter=0.75,
    ),
    _live(
        "bs_overhedge_125",
        "BS 125% Over-hedge",
        "over_delta_hedge",
        "避險比例",
        "把 BS Delta-neutral 目標放大到 125%，測量過度避險與反向曝險。",
        "ATM Straddle 與因果 BS Delta；期貨目標乘 1.25 後才整數化。",
        "期貨盤末歸零；選擇權週結算。",
        "可能把小估計誤差放大成反向部位，通常也增加換手成本。",
        "black_scholes_scaled_delta",
        option_legs=_ATM_STRADDLE,
        hedge_policy="bs_delta_scale",
        hedge_parameter=1.25,
    ),
    _live(
        "bs_overhedge_150",
        "BS 150% Over-hedge",
        "over_delta_hedge",
        "避險比例",
        "把 BS Delta-neutral 目標放大到 150%，刻意形成更大的反向 Delta 曝險。",
        "ATM Straddle 與因果 BS Delta；期貨目標乘 1.50 後才整數化。",
        "期貨盤末歸零；選擇權週結算。",
        "會放大模型誤差、反向部位與換手成本，只適合作為壓力比較策略。",
        "black_scholes_scaled_delta",
        option_legs=_ATM_STRADDLE,
        hedge_policy="bs_delta_scale",
        hedge_parameter=1.50,
    ),
    _live(
        "underlying_hedge_future_long",
        "避險期貨純多基準",
        "underlying_benchmark",
        "方向基準",
        "不持有選擇權；每個可交易盤維持一口避險期貨多單。",
        "第一個新鮮期貨 book 以 best ask 建立一口多單。",
        "日盤與夜盤各自在設定的 flatten time 前歸零。",
        "線性曝險、需原始保證金，沒有選擇權凸性保護。",
        "direct_futures_benchmark",
        hedge_policy="fixed_future",
        hedge_parameter=1.0,
    ),
    _live(
        "underlying_hedge_future_short",
        "避險期貨純空基準",
        "underlying_benchmark",
        "方向基準",
        "不持有選擇權；每個可交易盤維持一口避險期貨空單。",
        "第一個新鮮期貨 book 以 best bid 建立一口空單。",
        "日盤與夜盤各自在設定的 flatten time 前歸零。",
        "線性曝險、需原始保證金，沒有選擇權凸性保護。",
        "direct_futures_benchmark",
        hedge_policy="fixed_future",
        hedge_parameter=-1.0,
    ),
    _live(
        "protective_put_with_future",
        "Protective Put + 避險期貨多單",
        "protected_directional",
        "保護性方向",
        "一口期貨多單搭配 ATM Long Put，形成有下檔凸性保護的多方基準。",
        "期貨以 best ask、Put 以 best ask；兩者都必須有新鮮可成交 book。",
        "期貨盤末歸零，Put 持有至官方週結算。",
        "Put 權利金會拖累盤整／上漲報酬；不同乘數下以等 Delta 口數換算。",
        "long_premium_plus_fixed_future",
        option_legs=(("P", 0, 1),),
        hedge_policy="fixed_index_equivalent",
        hedge_parameter=1.0,
    ),
    _live(
        "protective_call_with_future",
        "Protective Call + 避險期貨空單",
        "protected_directional",
        "保護性方向",
        "一口等價指數期貨空單搭配 ATM Long Call，形成有上檔凸性保護的空方基準。",
        "期貨以 best bid、Call 以 best ask；兩者都必須有新鮮可成交 book。",
        "期貨盤末歸零，Call 持有至官方週結算。",
        "Call 權利金會拖累盤整／下跌報酬；不同乘數下以等 Delta 口數換算。",
        "long_premium_plus_fixed_future",
        option_legs=(("C", 0, 1),),
        hedge_policy="fixed_index_equivalent",
        hedge_parameter=-1.0,
    ),
)


_GAMMA_TRIGGER_LIVE_SPECS: Final[tuple[StrategySpec, ...]] = (
    *(
        _live(
            f"bs_gamma_price_grid_{points}",
            f"BS Gamma Scalping {points} 點網格",
            "price_grid_gamma_scalping",
            "Gamma Scalping／點數觸發",
            f"持有 ATM Long Straddle；TX forward 自同週期上次成功重平衡移動至少 {points} 點時，才重算 BS Delta 並調整期貨。",
            "首次可計算時先建立 Delta 避險錨點；其後只使用決策前已收到的完整 Bid/Ask book 比較點數門檻。",
            "期貨盤末歸零；選擇權持有至官方週結算；新週期重新建立錨點。",
            "網格太小會增加換手，太大會留下方向風險；已實現波動仍須覆蓋 Theta、價差、稅費與模型誤差。",
            "black_scholes_price_grid_gamma_scalping",
            option_legs=_ATM_STRADDLE,
            hedge_policy="bs_gamma_price_grid",
            hedge_parameter=float(points),
        )
        for points in (25, 50, 100)
    ),
    *(
        _live(
            f"bs_gamma_time_grid_{minutes}m",
            f"BS Gamma Scalping {minutes} 分鐘網格",
            "time_grid_gamma_scalping",
            "Gamma Scalping／時間觸發",
            f"持有 ATM Long Straddle；同週期每滿 {minutes} 分鐘才重算 BS Delta 並調整期貨。",
            "首次可計算時先建立 Delta 避險錨點；之後以每次成功重平衡的決策時間計算下一個因果觸發。",
            "期貨盤末歸零；選擇權持有至官方週結算；新週期重新建立錨點。",
            "固定時間間隔不會因跳空提前避險；短間隔通常增加成交成本，長間隔則增加方向漂移。",
            "black_scholes_time_grid_gamma_scalping",
            option_legs=_ATM_STRADDLE,
            hedge_policy="bs_gamma_time_grid",
            hedge_parameter=float(minutes),
        )
        for minutes in (5, 15, 30)
    ),
)


def _structure(
    strategy_id: str,
    label: str,
    family: str,
    summary: str,
    option_legs: tuple[tuple[str, int, int], ...],
    *,
    hedge_policy: str = "none",
    hedge_parameter: float | None = None,
) -> StrategySpec:
    has_short = any(quantity < 0 for _right, _offset, quantity in option_legs)
    return _live(
        strategy_id,
        label,
        family,
        "賣方與多腿組合" if has_short else "選擇權經典",
        summary,
        "同一週序列依履約價 offset 組成各腿；買 Ask、賣 Bid，數量必須通過五檔深度。",
        "持有至官方週結算；期貨腿另於每個日／夜盤 flatten time 前歸零。",
        (
            "空方腿逐腿使用保守裸賣保證金，不套用未驗證的組合折抵；"
            "權益不足會以五檔可成交價強制平倉，非正權益後永久停止。"
            if has_short
            else "最大損失以淨權利金與各腿到期 payoff 為準。"
        ),
        (
            "live_multi_leg_conservative_naked_margin_no_offsets"
            if has_short
            else "live_multi_leg_long_premium"
        ),
        option_legs=option_legs,
        hedge_policy=hedge_policy,
        hedge_parameter=hedge_parameter,
    )


_MULTI_LEG_LIVE_SPECS: Final[tuple[StrategySpec, ...]] = (
    _structure(
        "naked_short_call",
        "Naked Short ATM Call",
        "naked_short_option",
        "單腿裸賣 ATM Call 收取權利金，承擔指數上漲時理論無上限的損失。",
        (("C", 0, -1),),
    ),
    _structure(
        "naked_short_put",
        "Naked Short ATM Put",
        "naked_short_option",
        "單腿裸賣 ATM Put 收取權利金，承擔指數大跌時的大額損失。",
        (("P", 0, -1),),
    ),
    _structure(
        "short_atm_straddle",
        "Short ATM Straddle",
        "short_volatility",
        "賣出 ATM Call 與 Put 收取 Theta，承擔雙尾風險。",
        (("C", 0, -1), ("P", 0, -1)),
    ),
    _structure(
        "short_otm_strangle",
        "Short OTM Strangle",
        "short_volatility",
        "賣出一檔 OTM Call/Put，收取較少權利金並放寬獲利區。",
        (("C", 1, -1), ("P", -1, -1)),
    ),
    _structure(
        "short_itm_guts",
        "Short Guts",
        "short_volatility",
        "賣出一檔 ITM Call/Put，收取高權利金並承擔雙尾風險。",
        (("C", -1, -1), ("P", 1, -1)),
    ),
    _structure(
        "bull_call_debit",
        "Bull Call Debit Spread",
        "vertical_spread",
        "買 ATM Call、賣上一檔 Call 的有限風險偏多價差。",
        (("C", 0, 1), ("C", 1, -1)),
    ),
    _structure(
        "bear_put_debit",
        "Bear Put Debit Spread",
        "vertical_spread",
        "買 ATM Put、賣下一檔 Put 的有限風險偏空價差。",
        (("P", 0, 1), ("P", -1, -1)),
    ),
    _structure(
        "bull_put_credit",
        "Bull Put Credit Spread",
        "vertical_spread",
        "賣下一檔 Put、買下兩檔 Put 的信用偏多價差。",
        (("P", -1, -1), ("P", -2, 1)),
    ),
    _structure(
        "bear_call_credit",
        "Bear Call Credit Spread",
        "vertical_spread",
        "賣上一檔 Call、買上兩檔 Call 的信用偏空價差。",
        (("C", 1, -1), ("C", 2, 1)),
    ),
    _structure(
        "long_call_butterfly",
        "Long Call Butterfly",
        "butterfly",
        "以 -1/0/+1 Call 組成 1:-2:1 到期帳篷。",
        (("C", -1, 1), ("C", 0, -2), ("C", 1, 1)),
    ),
    _structure(
        "long_put_butterfly",
        "Long Put Butterfly",
        "butterfly",
        "以 +1/0/-1 Put 組成 1:-2:1 到期帳篷。",
        (("P", 1, 1), ("P", 0, -2), ("P", -1, 1)),
    ),
    _structure(
        "iron_butterfly",
        "Iron Butterfly",
        "iron_butterfly",
        "賣 ATM Straddle、買一檔外雙翼，交易到期落點集中。",
        (("C", 0, -1), ("P", 0, -1), ("C", 1, 1), ("P", -1, 1)),
    ),
    _structure(
        "iron_condor",
        "Iron Condor",
        "iron_condor",
        "賣一檔 OTM Strangle、買兩檔外保護翼。",
        (("C", 1, -1), ("P", -1, -1), ("C", 2, 1), ("P", -2, 1)),
    ),
    _structure(
        "bull_risk_reversal",
        "Bull Risk Reversal",
        "risk_reversal",
        "買 OTM Call、賣 OTM Put，建立偏多偏斜曝險。",
        (("C", 1, 1), ("P", -1, -1)),
    ),
    _structure(
        "bear_risk_reversal",
        "Bear Risk Reversal",
        "risk_reversal",
        "買 OTM Put、賣 OTM Call，建立偏空偏斜曝險。",
        (("P", -1, 1), ("C", 1, -1)),
    ),
    _structure(
        "call_ratio_spread",
        "1x2 Call Ratio Spread",
        "ratio_spread",
        "買一口 ATM Call、賣兩口 OTM Call；上漲尾端重新裸露。",
        (("C", 0, 1), ("C", 1, -2)),
    ),
    _structure(
        "call_ratio_backspread",
        "Call Ratio Backspread",
        "ratio_backspread",
        "賣一口 ATM Call、買兩口 OTM Call，偏多尾端凸性。",
        (("C", 0, -1), ("C", 1, 2)),
    ),
    _structure(
        "put_ratio_spread",
        "1x2 Put Ratio Spread",
        "ratio_spread",
        "買一口 ATM Put、賣兩口 OTM Put；下跌尾端重新裸露。",
        (("P", 0, 1), ("P", -1, -2)),
    ),
    _structure(
        "put_ratio_backspread",
        "Put Ratio Backspread",
        "ratio_backspread",
        "賣一口 ATM Put、買兩口 OTM Put，偏空尾端凸性。",
        (("P", 0, -1), ("P", -1, 2)),
    ),
    _structure(
        "long_box",
        "Long Box Spread",
        "arbitrage",
        "Bull Call Spread 加 Bear Put Spread，鎖定相鄰履約價到期現金流。",
        (("C", 0, 1), ("C", 1, -1), ("P", 1, 1), ("P", 0, -1)),
    ),
    _structure(
        "synthetic_long",
        "Synthetic Long Future",
        "synthetic",
        "買 ATM Call、賣 ATM Put，合成線性多方 payoff。",
        (("C", 0, 1), ("P", 0, -1)),
    ),
    _structure(
        "synthetic_short",
        "Synthetic Short Future",
        "synthetic",
        "買 ATM Put、賣 ATM Call，合成線性空方 payoff。",
        (("P", 0, 1), ("C", 0, -1)),
    ),
    _structure(
        "covered_call",
        "Covered Call",
        "covered_option",
        "一口等價指數期貨多單搭配 Short ATM Call。",
        (("C", 0, -1),),
        hedge_policy="fixed_index_equivalent",
        hedge_parameter=1.0,
    ),
    _structure(
        "collar",
        "Collar",
        "covered_option",
        "等價期貨多單、Long OTM Put、Short OTM Call。",
        (("P", -1, 1), ("C", 1, -1)),
        hedge_policy="fixed_index_equivalent",
        hedge_parameter=1.0,
    ),
    _structure(
        "conversion",
        "Conversion",
        "arbitrage",
        "等價期貨多單、Long Put、Short Call 的 parity 組合。",
        (("P", 0, 1), ("C", 0, -1)),
        hedge_policy="fixed_index_equivalent",
        hedge_parameter=1.0,
    ),
    _structure(
        "reversal",
        "Reversal",
        "arbitrage",
        "等價期貨空單、Long Call、Short Put 的 parity 組合。",
        (("C", 0, 1), ("P", 0, -1)),
        hedge_policy="fixed_index_equivalent",
        hedge_parameter=-1.0,
    ),
    _live(
        PUT_CALL_PARITY_TX_STRATEGY_ID,
        "Put–Call Parity + TX 成本後套利",
        "arbitrage",
        "套利",
        "掃描同到期月選 Call/Put 與 TX；只有整包 Bid/Ask 毛利扣除手續費、進場交易稅與估計結算稅後嚴格為正才建立鎖定組合。",
        "以 4 組 TXO 對 1 口 TX 同時計算兩個方向；先用完整五檔形成訊號，再等待每一腿都收到嚴格較晚的新報價並重新驗證淨利。",
        "不做一般策略的日／夜盤末平倉；同到期 TXO 與 TX 持有至 TAIFEX 官方最後結算價現金結算。",
        "理想帳假設三腿可整包完成，沒有跨商品原子委託保證；資金採逐腿裸賣與 TX 原始保證金、不套組合折抵，融資利率設為 0。",
        "live_same_expiry_put_call_parity_cost_gate",
        option_legs=(("C", 0, -4), ("P", 0, 4)),
        hedge_policy="same_expiry_tx_parity_lock",
        hedge_parameter=1.0,
        broker_monitoring="ideal_only_no_broker_submission",
    ),
    _structure(
        "jade_lizard",
        "Jade Lizard",
        "hybrid_credit",
        "Short OTM Put 加 Bear Call Spread 的信用組合。",
        (("P", -1, -1), ("C", 1, -1), ("C", 2, 1)),
    ),
    _structure(
        "bullish_seagull",
        "Bullish Seagull",
        "hybrid_directional",
        "Long Put Spread 加 Short OTM Call，搭配等價期貨多單。",
        (("P", -1, 1), ("P", -2, -1), ("C", 1, -1)),
        hedge_policy="fixed_index_equivalent",
        hedge_parameter=1.0,
    ),
)


LIVE_STRATEGY_SPECS: Final[tuple[StrategySpec, ...]] = (
    *_STATIC_LIVE_SPECS,
    *_ROLLING_STRADDLE_LIVE_SPECS,
    *_MODEL_LIVE_SPECS,
    *_POLICY_LIVE_SPECS,
    *_GAMMA_TRIGGER_LIVE_SPECS,
    *_MULTI_LEG_LIVE_SPECS,
)
STRATEGY_IDS: Final[tuple[str, ...]] = tuple(
    spec.strategy_id for spec in LIVE_STRATEGY_SPECS
)
STRATEGY_SPEC_BY_ID: Final[dict[str, StrategySpec]] = {
    spec.strategy_id: spec for spec in LIVE_STRATEGY_SPECS
}
DYNAMIC_HEDGE_STRATEGY_IDS: Final[tuple[str, ...]] = tuple(
    spec.strategy_id
    for spec in LIVE_STRATEGY_SPECS
    if spec.hedge_policy not in {"none", "same_expiry_tx_parity_lock"}
)


def _reference(
    strategy_id: str,
    label: str,
    family: str,
    category: str,
    summary: str,
    blocker: str,
) -> StrategySpec:
    return StrategySpec(
        strategy_id=strategy_id,
        label=label,
        family=family,
        category=category,
        summary=summary,
        entry_rule="依各策略的多腿契約與觸發規則建倉。",
        exit_rule="必須由對應的多腿執行器、保證金與強平狀態決定。",
        risk_note="未通過目前即時執行契約，不列入績效排名。",
        implementation_level="catalogue_only_fail_closed",
        availability="blocked_contract",
        broker_monitoring="not_submitted",
        blocker=blocker,
    )


REFERENCE_STRATEGY_SPECS: Final[tuple[StrategySpec, ...]] = (
    _reference(
        "calendar_spread",
        "Calendar Spread",
        "calendar",
        "跨期",
        "同履約價跨到期買遠賣近。",
        "需要多到期持倉生命週期與各自官方結算，不可共用舊價。",
    ),
    _reference(
        "diagonal_spread",
        "Diagonal Spread",
        "diagonal",
        "跨期",
        "跨到期且跨履約價的價差。",
        "需要跨期／跨履約價配對、保證金與逐到期結算。",
    ),
    _reference(
        "calendar_box",
        "Cross-expiry Calendar Box",
        "arbitrage",
        "套利",
        "跨月／週序列的盒式關係交易。",
        "每一腿必須在自己的結算日與官方價格結算，且需同時深度證據。",
    ),
    _reference(
        "dynamic_recenter",
        "Re-center / Ratchet / Roll",
        "dynamic_options",
        "動態選擇權",
        "價格、時間或 trailing trigger 後換到新 ATM 腿。",
        "需要先平舊腿再開新腿的因果四腿執行與未成交復原。",
    ),
    _reference(
        "fixed_tp_sl",
        "Option TP/SL",
        "dynamic_options",
        "動態選擇權",
        "依可清算價觸發固定停利停損。",
        "需要觸發後第一個可成交 book、深度與 terminal flatten 證據。",
    ),
    _reference(
        "skew_term_carry",
        "Skew / Term-structure Carry",
        "relative_value",
        "相對價值",
        "交易不同履約價或期限的隱含波動差。",
        "需要完整多期限 quote、跨腿同步門檻與組合保證金。",
    ),
    _reference(
        "dispersion",
        "Index Dispersion",
        "relative_value",
        "相對價值",
        "指數選擇權對成分股選擇權波動。",
        "目前沒有完整成分股 option book、相關性與組合保證金資料。",
    ),
    _reference(
        "variance_vol_swaps",
        "Variance / Volatility Swaps",
        "otc_volatility",
        "波動衍生品",
        "以實現變異數或波動率結算。",
        "TAIFEX/Shioaji 現有可交易合約不是此 OTC payoff。",
    ),
    _reference(
        "trend_breakout",
        "Trend / Breakout",
        "systematic_futures",
        "系統期貨",
        "以價格突破、均線或通道追蹤方向。",
        "需先固定特徵窗口、訊號時間與不同盤別的可成交規則。",
    ),
    _reference(
        "mean_reversion",
        "Mean Reversion / Pairs",
        "systematic_futures",
        "系統期貨",
        "偏離均值或跨合約關係後反向交易。",
        "需 point-in-time 配對、展期與關係失效風控。",
    ),
    _reference(
        "market_making",
        "Market Making",
        "liquidity_provision",
        "造市",
        "同時掛 bid/ask 賺取價差並管理庫存。",
        "目前只有 taker 可成交價理想帳；沒有排隊順位、撤單延遲與 maker fill 模型。",
    ),
    _reference(
        "ml_rl_policy",
        "ML / RL Policy",
        "learned_policy",
        "學習策略",
        "由監督式、強化學習或序列模型決定多腿目標。",
        "需獨立訓練、checkpoint、因果特徵與線上延遲驗證，不能只用名稱加入。",
    ),
)

STRATEGY_CATALOG: Final[tuple[StrategySpec, ...]] = (
    *LIVE_STRATEGY_SPECS,
    *REFERENCE_STRATEGY_SPECS,
)

_catalog_ids = {spec.strategy_id for spec in STRATEGY_CATALOG}
for _classification_name, _classification in (
    ("directional", _DIRECTIONAL_EXPOSURE_BY_ID),
    ("volatility", _VOLATILITY_EXPOSURE_BY_ID),
    ("hedge", _HEDGE_TYPE_BY_ID),
):
    _missing = _catalog_ids - _classification.keys()
    _unknown = _classification.keys() - _catalog_ids
    if _missing or _unknown:
        raise RuntimeError(
            f"{_classification_name} exposure classification mismatch: "
            f"missing={sorted(_missing)}, unknown={sorted(_unknown)}"
        )


__all__ = [
    "BS_DELTA_BAND_STRATEGY_IDS",
    "BS_GAMMA_PRICE_GRID_STRATEGY_IDS",
    "BS_GAMMA_TIME_GRID_STRATEGY_IDS",
    "BS_OVER_DELTA_STRATEGY_IDS",
    "BS_PARTIAL_DELTA_STRATEGY_IDS",
    "CATALOG_EXPANSION_ENTRY_IMMEDIATE_LIVE",
    "CATALOG_EXPANSION_ENTRY_NEXT_CYCLE",
    "CATALOG_EXPANSION_ENTRY_POLICIES",
    "CLASSIC_VARIANT_ID",
    "DYNAMIC_HEDGE_STRATEGY_IDS",
    "EXPOSURE_RATIO_BASIS",
    "EXPOSURE_TAXONOMY",
    "LIVE_STRATEGY_SPECS",
    "MODEL_BLACK_SCHOLES",
    "MODEL_HESTON_SVI",
    "MODEL_LOCAL_VOL",
    "MODEL_ROUGH_VOL",
    "MODEL_SABR",
    "MODEL_SLV",
    "MODEL_VARIANT_PREFIX",
    "PUT_CALL_PARITY_TX_STRATEGY_ID",
    "ROLLING_ITM_LONG_STRADDLE_ID",
    "ROLLING_ITM_SHORT_STRADDLE_ID",
    "ROLLING_OTM_LONG_STRADDLE_ID",
    "ROLLING_OTM_SHORT_STRADDLE_ID",
    "ROLLING_STRADDLE_IDS",
    "REFERENCE_STRATEGY_SPECS",
    "STRATEGY_CATALOG",
    "STRATEGY_IDS",
    "STRATEGY_MODE_DAILY",
    "STRATEGY_MODE_INTRADAY_FUTURES",
    "STRATEGY_MODES",
    "STRATEGY_SPEC_BY_ID",
    "StrategySpec",
    "VOLATILITY_MODEL_IDS",
    "VOLATILITY_MODEL_IMPLEMENTATION",
    "VOLATILITY_MODEL_LABELS",
]
