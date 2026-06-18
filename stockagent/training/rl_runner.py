from __future__ import annotations

import json
import warnings
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import gymnasium as gym
import numpy as np
import torch
from gymnasium import spaces
from stable_baselines3 import DDPG, PPO, SAC, TD3
from stable_baselines3.common.noise import NormalActionNoise
from stable_baselines3.common.vec_env import DummyVecEnv
from tqdm import tqdm

from stockagent.backtest.report import (
    compute_metrics,
    generate_annual_report,
    plot_annual_performance,
    plot_equity_curve,
    plot_equity_curve_log,
    plot_fold_first_year_returns,
)
from stockagent.backtest.simulator import BacktestResult, run_backtest_integer_shares, run_backtest_torch
from stockagent.data.panel import PanelData
from stockagent.data.walkforward import WalkForwardFold
from stockagent.evaluation.metrics import compute_ic_series_torch, ic_summary


@dataclass(slots=True)
class FoldResult:
    fold_id: int
    train_years: list[int]
    val_years: list[int]
    test_years: list[int]
    best_val_loss: float
    val_ic: dict[str, float]
    val_metrics: dict[str, float]
    test_ic: dict[str, float]
    test_metrics: dict[str, float]


def _fold_dir(output_path: Path, fold_id: int) -> Path:
    return output_path / f"fold_{fold_id:02d}"


def _metrics_path(fold_dir: Path) -> Path:
    return fold_dir / "metrics.json"


def _backtest_path(fold_dir: Path) -> Path:
    return fold_dir / "test_backtest.npz"


def _model_path(fold_dir: Path) -> Path:
    return fold_dir / "rl_model.zip"


def _summary_path(output_path: Path) -> Path:
    return output_path / "summary.json"


def _save_backtest_artifact(output_path: Path, strategy, dates: np.ndarray) -> None:
    np.savez_compressed(
        output_path,
        strategy_returns=strategy.strategy_returns,
        benchmark_returns=strategy.benchmark_returns,
        turnovers=strategy.turnovers,
        weights_history=strategy.weights_history,
        dates=np.asarray(dates),
    )


def _load_backtest_artifact(output_path: Path) -> tuple[BacktestResult, np.ndarray]:
    data = np.load(output_path)
    result = BacktestResult(
        strategy_returns=data["strategy_returns"].astype(np.float32),
        benchmark_returns=data["benchmark_returns"].astype(np.float32),
        turnovers=data["turnovers"].astype(np.float32),
        weights_history=data["weights_history"].astype(np.float32),
    )
    dates = np.asarray(data["dates"])
    return result, dates


def _save_holdings_csv(output_path: Path, holdings) -> None:
    import pandas as pd

    rows = [
        {
            "date": row.date,
            "symbol": row.symbol,
            "shares": int(row.shares),
            "price": float(row.price),
            "market_value": float(row.market_value),
            "holding_ratio": float(row.holding_ratio),
            "is_cash": bool(row.is_cash),
        }
        for row in holdings
    ]
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["date", "holding_ratio", "symbol"], ascending=[True, False, True])
    df.to_csv(output_path, index=False)


def _load_fold_result(metrics_path: Path) -> FoldResult:
    with metrics_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return FoldResult(**payload)


def _write_summary(results: list[FoldResult], output_path: Path) -> None:
    summary_path = _summary_path(output_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump([asdict(result) for result in results], handle, indent=2)


def _load_completed_fold_result(output_path: Path, fold_id: int) -> FoldResult | None:
    fold_dir = _fold_dir(output_path, fold_id)
    metrics_path = _metrics_path(fold_dir)
    model_path = _model_path(fold_dir)
    backtest_path = _backtest_path(fold_dir)
    if metrics_path.exists() and model_path.exists() and backtest_path.exists():
        return _load_fold_result(metrics_path)
    return None


def _refresh_walkforward_artifacts(output_path: Path, results: list[FoldResult]) -> None:
    _write_summary(results, output_path)

    all_strategy_returns: list[np.ndarray] = []
    all_benchmark_returns: list[np.ndarray] = []
    all_turnovers: list[np.ndarray] = []
    all_weights: list[np.ndarray] = []
    all_dates: list[np.ndarray] = []
    all_first_year_dates: list[np.ndarray] = []
    all_first_year_strategy_log: list[np.ndarray] = []
    all_first_year_baseline_log: list[np.ndarray] = []

    for result in sorted(results, key=lambda item: item.fold_id):
        fold_dir = _fold_dir(output_path, result.fold_id)
        backtest_path = _backtest_path(fold_dir)
        if not backtest_path.exists():
            continue
        fold_backtest, fold_dates = _load_backtest_artifact(backtest_path)
        all_strategy_returns.append(fold_backtest.strategy_returns)
        all_benchmark_returns.append(fold_backtest.benchmark_returns)
        all_turnovers.append(fold_backtest.turnovers)
        all_weights.append(fold_backtest.weights_history)
        all_dates.append(fold_dates)

        years = np.asarray(fold_dates, dtype="datetime64[D]").astype(object)
        years = np.array([d.year for d in years])
        if years.size > 0:
            first_year = int(np.min(years))
            mask = years == first_year
            all_first_year_dates.append(fold_dates[mask])
            all_first_year_strategy_log.append(
                np.nan_to_num(fold_backtest.strategy_returns[mask], nan=0.0).astype(np.float64)
            )
            all_first_year_baseline_log.append(
                np.nan_to_num(fold_backtest.benchmark_returns[mask], nan=0.0).astype(np.float64)
            )

    if not all_dates:
        return

    combined_backtest = BacktestResult(
        strategy_returns=np.concatenate(all_strategy_returns, axis=0),
        benchmark_returns=np.concatenate(all_benchmark_returns, axis=0),
        turnovers=np.concatenate(all_turnovers, axis=0),
        weights_history=np.concatenate(all_weights, axis=0),
    )
    combined_dates = np.concatenate(all_dates, axis=0)

    plot_equity_curve_log(
        combined_backtest,
        combined_dates,
        output_path / "walkforward_equity_curve_log.png",
    )

    if all_first_year_dates:
        plot_fold_first_year_returns(
            all_first_year_dates,
            all_first_year_strategy_log,
            all_first_year_baseline_log,
            output_path / "walkforward_first_year_cumulative_returns.png",
        )


def _normalize_weights(action: np.ndarray, mask: np.ndarray, long_only: bool) -> np.ndarray:
    w = np.nan_to_num(np.asarray(action, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0)
    m = np.asarray(mask, dtype=bool)
    w = np.where(m, w, 0.0)

    if long_only:
        w = np.clip(w, 0.0, None)
        total = float(w.sum())
        if total <= 1e-8:
            valid = np.flatnonzero(m)
            if valid.size > 0:
                w[valid] = 1.0 / float(valid.size)
            return w.astype(np.float32)
        return (w / total).astype(np.float32)

    gross = float(np.abs(w).sum())
    if gross <= 1e-8:
        return np.zeros_like(w, dtype=np.float32)
    return (w / gross).astype(np.float32)


def _select_symbol_subset(panel: PanelData, train_indices: np.ndarray, max_symbols: int) -> np.ndarray:
    tradable_counts = panel.tradable_mask[train_indices].sum(axis=0)
    order = np.argsort(-tradable_counts)
    top = order[: min(max_symbols, order.size)]
    return np.sort(top)


@dataclass(slots=True)
class RLSplit:
    features: np.ndarray
    returns: np.ndarray
    tradable_mask: np.ndarray
    benchmark: np.ndarray
    open_prices: np.ndarray
    close_prices: np.ndarray
    dates: np.ndarray


def _build_rl_split(panel: PanelData, date_indices: np.ndarray, symbol_indices: np.ndarray, lookback: int) -> RLSplit:
    date_indices = np.array(sorted(date_indices.tolist()), dtype=np.int64)
    fold_start_idx = int(date_indices[0])
    min_valid_idx = fold_start_idx + lookback - 1
    valid_indices = date_indices[date_indices > min_valid_idx]
    if valid_indices.size == 0:
        raise ValueError(f"Split has insufficient data for lookback={lookback}")

    feat = panel.features[valid_indices][:, symbol_indices, :].astype(np.float32, copy=False)
    rets = np.nan_to_num(panel.returns_1d[valid_indices][:, symbol_indices], nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32)
    mask = panel.tradable_mask[valid_indices][:, symbol_indices].astype(bool, copy=False)
    bench = np.nan_to_num(panel.benchmark_returns[valid_indices], nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32)
    opens = panel.open_prices[valid_indices][:, symbol_indices].astype(np.float32, copy=False)
    closes = panel.close_prices[valid_indices][:, symbol_indices].astype(np.float32, copy=False)
    dates = panel.dates[valid_indices]
    return RLSplit(feat, rets, mask, bench, opens, closes, dates)


class PortfolioRLEnv(gym.Env):
    metadata = {"render_modes": []}

    def __init__(
        self,
        split: RLSplit,
        *,
        lookback: int,
        fee_per_side: float,
        long_only: bool,
    ) -> None:
        super().__init__()
        self.split = split
        self.lookback = int(max(1, lookback))
        self.fee_per_side = float(fee_per_side)
        self.long_only = bool(long_only)

        self.num_steps = int(split.features.shape[0])
        self.num_symbols = int(split.features.shape[1])
        self.num_features = int(split.features.shape[2])

        obs_dim = self.lookback * self.num_symbols * self.num_features + self.num_symbols
        self.observation_space = spaces.Box(
            low=-np.inf,
            high=np.inf,
            shape=(obs_dim,),
            dtype=np.float32,
        )
        self.action_space = spaces.Box(low=-1.0, high=1.0, shape=(self.num_symbols,), dtype=np.float32)

        self._idx = 0
        self._prev_weights = np.zeros((self.num_symbols,), dtype=np.float32)

    def _build_observation(self) -> np.ndarray:
        start = max(0, self._idx - self.lookback + 1)
        window = self.split.features[start : self._idx + 1]
        if window.shape[0] < self.lookback:
            pad = np.zeros((self.lookback - window.shape[0], self.num_symbols, self.num_features), dtype=np.float32)
            window = np.concatenate([pad, window], axis=0)
        mask = self.split.tradable_mask[self._idx].astype(np.float32)
        return np.concatenate([window.reshape(-1), mask], axis=0).astype(np.float32)

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        self._idx = 0
        self._prev_weights = np.zeros((self.num_symbols,), dtype=np.float32)
        return self._build_observation(), {}

    def step(self, action):
        mask = self.split.tradable_mask[self._idx]
        w = _normalize_weights(action, mask, self.long_only)
        daily_ret = float((w * self.split.returns[self._idx]).sum())
        turnover = float(np.abs(w - self._prev_weights).sum())
        reward = daily_ret - self.fee_per_side * turnover

        self._prev_weights = w
        self._idx += 1

        terminated = self._idx >= self.num_steps
        truncated = False
        if terminated:
            obs = np.zeros(self.observation_space.shape, dtype=np.float32)
        else:
            obs = self._build_observation()

        info = {
            "daily_return": daily_ret,
            "turnover": turnover,
            "benchmark": float(self.split.benchmark[min(self._idx - 1, self.num_steps - 1)]),
        }
        return obs, float(reward), terminated, truncated, info


def _build_algo(
    algo_name: str,
    env: DummyVecEnv,
    *,
    learning_rate: float,
    gamma: float,
    batch_size: int,
    device: str,
    hidden_dim: int,
    n_steps: int,
    buffer_size: int,
    learning_starts: int,
):
    policy_kwargs = dict(net_arch=[hidden_dim, hidden_dim])

    if algo_name == "ppo":
        return PPO(
            "MlpPolicy",
            env,
            learning_rate=learning_rate,
            gamma=gamma,
            batch_size=batch_size,
            n_steps=n_steps,
            policy_kwargs=policy_kwargs,
            verbose=0,
            device=device,
        )

    action_dim = env.action_space.shape[-1]
    noise = NormalActionNoise(mean=np.zeros(action_dim), sigma=0.1 * np.ones(action_dim))

    if algo_name == "ddpg":
        return DDPG(
            "MlpPolicy",
            env,
            learning_rate=learning_rate,
            gamma=gamma,
            batch_size=batch_size,
            buffer_size=buffer_size,
            learning_starts=learning_starts,
            action_noise=noise,
            policy_kwargs=policy_kwargs,
            verbose=0,
            device=device,
        )

    if algo_name == "td3":
        return TD3(
            "MlpPolicy",
            env,
            learning_rate=learning_rate,
            gamma=gamma,
            batch_size=batch_size,
            buffer_size=buffer_size,
            learning_starts=learning_starts,
            action_noise=noise,
            policy_kwargs=policy_kwargs,
            verbose=0,
            device=device,
        )

    if algo_name == "sac":
        return SAC(
            "MlpPolicy",
            env,
            learning_rate=learning_rate,
            gamma=gamma,
            batch_size=batch_size,
            buffer_size=buffer_size,
            learning_starts=learning_starts,
            policy_kwargs=policy_kwargs,
            verbose=0,
            device=device,
        )

    raise ValueError(f"Unsupported RL algorithm: {algo_name}")


def _rollout_policy(model, env: PortfolioRLEnv) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    obs, _ = env.reset()
    weights: list[np.ndarray] = []
    rets: list[np.ndarray] = []
    masks: list[np.ndarray] = []
    bench: list[float] = []
    done = False
    while not done:
        action, _ = model.predict(obs, deterministic=True)
        idx = env._idx
        m = env.split.tradable_mask[idx]
        w = _normalize_weights(action, m, env.long_only)
        weights.append(w)
        rets.append(env.split.returns[idx])
        masks.append(m)
        bench.append(float(env.split.benchmark[idx]))
        obs, _, terminated, truncated, _ = env.step(action)
        done = bool(terminated or truncated)

    return (
        np.asarray(weights, dtype=np.float32),
        np.asarray(rets, dtype=np.float32),
        np.asarray(masks, dtype=bool),
        np.asarray(bench, dtype=np.float32),
    )


def _resolve_rl_device(config, algo_name: str) -> str:
    requested = (config.training.rl_device or config.environment.device or "cpu").strip().lower()
    if requested.startswith("cuda"):
        if not torch.cuda.is_available():
            raise RuntimeError(
                "RL device is set to CUDA, but torch.cuda.is_available() is False. "
                "Please switch to a GPU-enabled environment or set training.rl_device=cpu."
            )
        if algo_name == "ppo":
            # SB3 warns that PPO+MLP may run faster on CPU; user explicitly requests GPU.
            warnings.filterwarnings(
                "ignore",
                message=r"You are trying to run PPO on the GPU.*",
                category=UserWarning,
                module=r"stable_baselines3\.common\.on_policy_algorithm",
            )
        return "cuda"
    return requested


def run_training_rl(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    config,
    output_dir: str | Path,
    resume: bool = True,
) -> list[FoldResult]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    algo_name = config.training.model_name.strip().lower()
    if algo_name not in {"ppo", "ddpg", "td3", "sac"}:
        raise ValueError(f"run_training_rl supports PPO/DDPG/TD3/SAC, got {algo_name}")

    sb3_device = _resolve_rl_device(config, algo_name)

    results_by_fold: dict[int, FoldResult] = {}
    fold_list = list(folds)

    if resume:
        for fold in fold_list:
            completed = _load_completed_fold_result(output_path, fold.fold_id)
            if completed is not None:
                results_by_fold[fold.fold_id] = completed
        if results_by_fold:
            _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))

    for fold in tqdm(fold_list, desc="RL folds", unit="fold"):
        if fold.fold_id in results_by_fold:
            continue

        fold_dir = _fold_dir(output_path, fold.fold_id)
        fold_dir.mkdir(parents=True, exist_ok=True)

        symbol_indices = _select_symbol_subset(panel, fold.train_indices, config.training.rl_max_symbols)

        train_split = _build_rl_split(panel, fold.train_indices, symbol_indices, config.training.lookback)
        val_split = _build_rl_split(panel, fold.val_indices, symbol_indices, config.training.lookback)
        test_split = _build_rl_split(panel, fold.test_indices, symbol_indices, config.training.lookback)

        train_env = DummyVecEnv(
            [
                lambda: PortfolioRLEnv(
                    train_split,
                    lookback=config.training.lookback,
                    fee_per_side=config.trading.fee_per_side,
                    long_only=config.trading.long_only,
                )
            ]
        )

        model = _build_algo(
            algo_name,
            train_env,
            learning_rate=float(config.training.learning_rate),
            gamma=float(config.training.rl_gamma),
            batch_size=int(config.training.rl_batch_size),
            device=sb3_device,
            hidden_dim=int(config.training.rl_policy_hidden_dim),
            n_steps=int(config.training.rl_n_steps),
            buffer_size=int(config.training.rl_buffer_size),
            learning_starts=int(config.training.rl_learning_starts),
        )
        model.learn(total_timesteps=int(config.training.rl_total_timesteps), progress_bar=False)
        model.save(str(fold_dir / "rl_model"))

        val_env_eval = PortfolioRLEnv(
            val_split,
            lookback=config.training.lookback,
            fee_per_side=config.trading.fee_per_side,
            long_only=config.trading.long_only,
        )
        val_w, val_r, val_m, val_b = _rollout_policy(model, val_env_eval)
        val_bt = run_backtest_torch(
            torch.from_numpy(val_w),
            torch.from_numpy(val_r),
            torch.from_numpy(val_m),
            torch.from_numpy(val_b),
            config.trading.fee_per_side,
        )
        val_ic = ic_summary(
            compute_ic_series_torch(
                val_bt.weights_history,
                torch.from_numpy(val_r),
                torch.from_numpy(val_m),
            ).cpu().numpy()
        )
        val_metrics = compute_metrics(val_bt.to_numpy())
        best_val_loss = -float(val_metrics.get("sharpe", 0.0))

        test_env_eval = PortfolioRLEnv(
            test_split,
            lookback=config.training.lookback,
            fee_per_side=config.trading.fee_per_side,
            long_only=config.trading.long_only,
        )
        test_w, test_r, test_m, test_b = _rollout_policy(model, test_env_eval)
        test_bt_t = run_backtest_torch(
            torch.from_numpy(test_w),
            torch.from_numpy(test_r),
            torch.from_numpy(test_m),
            torch.from_numpy(test_b),
            config.trading.fee_per_side,
        )
        test_ic = ic_summary(
            compute_ic_series_torch(
                test_bt_t.weights_history,
                torch.from_numpy(test_r),
                torch.from_numpy(test_m),
            ).cpu().numpy()
        )

        test_dates = test_split.dates
        test_open_prices = test_split.open_prices
        test_close_prices = test_split.close_prices
        test_symbols = [panel.symbols[i] for i in symbol_indices.tolist()]
        execution_mode = config.trading.execution_mode
        if execution_mode == "intraday_next_open":
            buy_fee_rate = config.trading.intraday_buy_fee_rate
            sell_fee_rate = config.trading.intraday_sell_fee_rate
        else:
            buy_fee_rate = config.trading.overnight_buy_fee_rate
            sell_fee_rate = config.trading.overnight_sell_fee_rate

        test_bt, holdings_records = run_backtest_integer_shares(
            weights=test_bt_t.weights_history.detach().cpu().numpy(),
            future_returns=test_r,
            tradable_mask=test_m,
            benchmark_returns=test_b,
            initial_capital=1_000_000.0,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            min_fee=config.trading.min_fee,
            execution_mode=execution_mode,
            lot_size=config.trading.lot_size,
            settlement_delay_days=config.trading.settlement_delay_days,
            open_prices=test_open_prices,
            close_prices=test_close_prices,
            symbols=test_symbols,
            dates=test_dates,
        )
        test_metrics = compute_metrics(test_bt)

        fold_result = FoldResult(
            fold_id=fold.fold_id,
            train_years=fold.train_years,
            val_years=fold.val_years,
            test_years=fold.test_years,
            best_val_loss=best_val_loss,
            val_ic=val_ic,
            val_metrics=val_metrics,
            test_ic=test_ic,
            test_metrics=test_metrics,
        )
        results_by_fold[fold.fold_id] = fold_result

        with _metrics_path(fold_dir).open("w", encoding="utf-8") as f:
            json.dump(asdict(fold_result), f, indent=2)

        _save_backtest_artifact(_backtest_path(fold_dir), test_bt, test_dates)
        report = generate_annual_report(test_bt, test_dates)
        with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as f:
            f.write(report)

        plot_equity_curve(test_bt, test_dates, fold_dir / "equity_curve.png")
        plot_equity_curve_log(test_bt, test_dates, fold_dir / "equity_curve_log.png")
        plot_annual_performance(test_bt, test_dates, fold_dir / "annual_performance.png")
        _save_holdings_csv(fold_dir / "holdings.csv", holdings_records)

        _refresh_walkforward_artifacts(output_path, list(results_by_fold.values()))

    return [results_by_fold[fold.fold_id] for fold in fold_list if fold.fold_id in results_by_fold]