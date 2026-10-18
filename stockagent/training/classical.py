"""Classical models inside the canonical training / accounting lifecycle."""
from __future__ import annotations

from dataclasses import asdict
import gc
import math
from pathlib import Path
import time

import numpy as np
import torch

from downloader.artifact_io import atomic_write_json
from stockagent.training.classical_data import CausalWindowMatrix, PhysicalCSRStore
from stockagent.training.classical_solvers import (
    LaggedLinearOperator, lightgbm_dataset, lightgbm_continue, lightgbm_scores,
)
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.windowed import dataset_to_windowed_tensors


def _dataset(panel, indices, config, *, allow_empty=False):
    return CrossSectionalDataset(panel, indices, config.training.lookback,
        allow_empty=allow_empty, execution_mode=config.trading.execution_mode,
        lookback_context=config.walk_forward.lookback_context,
        short_capacity_limit_enabled=config.trading.tw_short_capacity_limit_enabled,
        day_trade_unlimited_margin_conversion=config.trading.tw_day_trade_unlimited_margin_conversion,
        tw_corporate_action_mode=config.trading.tw_corporate_action_mode,
        tw_commission_rebate_timing=config.trading.tw_commission_rebate_timing)


def evaluate_scores(model, scores, matrix, split, config, runtime, device):
    """One allocation function and one exact executor for every model family."""
    from stockagent.training import trainer as t
    idx = split.valid_indices
    full = np.zeros(len(matrix.dates) * matrix.symbols, dtype=np.float32)
    full[matrix.flat] = np.asarray(scores, dtype=np.float32)
    if not np.isfinite(full).all():
        raise FloatingPointError("nonfinite classical model prediction")
    mask = split.tradable_mask[idx].to(device)
    weights = model.weights(full.reshape(len(matrix.dates), matrix.symbols), mask)
    def rows(name):
        value = getattr(split, name)
        return None if value is None else value[idx]
    extra = {
        "overnight_log_returns_all": "overnight_log_returns",
        "can_short_open_open_mask_all": "can_short_open_open_mask",
        "volume_notional_all": "volume_notional",
        "short_capacity_notional_all": "short_capacity_notional",
        "short_margin_rate_all": "short_margin_rate",
        "state_advance_mask_all": "session_advance_mask",
        "day_trade_eligible_mask_all": "day_trade_eligible_mask",
        "day_trade_can_buy_open_mask_all": "day_trade_can_buy_open_mask",
        "day_trade_can_sell_open_mask_all": "day_trade_can_sell_open_mask",
        "unresolved_corporate_action_mask_all": "unresolved_corporate_action_mask",
        "cash_dividend_yield_all": "cash_dividend_yield",
        "cash_dividend_payment_delay_sessions_all": "cash_dividend_payment_delay_sessions",
        "session_month_ids_all": "session_month_ids",
        "commission_rebate_payment_eligible_mask_all": "commission_rebate_payment_eligible_mask",
    }
    with torch.no_grad():
        bt, _ = t._run_eval_backtest_from_weight_buffers(
            weights, rows("future_log_returns"), rows("tradable_mask"),
            rows("can_buy_mask"), rows("can_sell_mask"), rows("can_short_open_mask"),
            rows("force_short_cover_mask"), rows("force_exit_mask"), rows("benchmark"),
            device=device, non_blocking=False, long_only=config.trading.long_only,
            buy_fee_rate=config.trading.buy_fee_rate, sell_fee_rate=config.trading.sell_fee_rate,
            max_turnover_ratio=config.trading.max_turnover_ratio, gross_leverage=1.0,
            min_trade_weight=config.trading.min_trade_weight,
            backtest_chunk_rows=int(config.training.eval_backtest_chunk_rows),
            compute_metrics_summary=True, return_weights_history=True,
            profile_timing=True, progress_label=None, timing=t.TimingBreakdown(),
            reset_at_rows=None, portfolio_activation=config.trading.portfolio_activation,
            max_volume_participation=config.trading.max_volume_participation,
            volume_participation_equity=config.trading.volume_participation_equity,
            execution_runtime=runtime, physical_split=split,
            **{key: rows(value) for key, value in extra.items()})
        loss = float(t._loss_from_backtest_result(bt, config,
            t._normalize_risk_objective(config.training.loss_type)).cpu())
        metrics = t._compute_metrics_from_tensors(bt.strategy_returns,
            bt.benchmark_returns, bt.turnovers,
            periods_per_year=config.evaluation.eval_log_utility_periods_per_year)
    return bt, loss, metrics


def _predict_matrix(model, matrix):
    if model.kind == "ridge":
        # Linear prediction shares the same full-window SpMM specialization.
        operator = LaggedLinearOperator(matrix, backend="cpu", threads=model.threads)
        try:
            return operator._matvec(model.coefficient) + model.intercept
        finally:
            operator.close()
    output = np.empty(matrix.shape[0], dtype=np.float32)
    for start, stop, x in matrix.batches():
        output[start:stop] = model.predict(x)
    return output


def _safe_solver_state(state):
    return {k: torch.from_numpy(v.copy()) if isinstance(v, np.ndarray) else v
            for k, v in state.items()}


def run_classical(panel, folds, config, output_dir, *, resume=True,
                  deployment_folds=None, lifecycle=None, inference_only=False):
    from stockagent.training import trainer as t
    from stockagent.models.factory import build_model
    from stockagent.training.runtime import load_checkpoint
    settings = config.training.classical
    if settings.threads < 1 or settings.batch_rows < 1:
        raise ValueError("classical threads/batch_rows must be positive")
    if config.training.multi_gpu_strategy not in {"none", "single", "auto"}:
        raise ValueError("native classical solvers own their backend; do not wrap them in DDP")
    if config.training.pretrained_initialization_root:
        raise ValueError("classical baselines train from scratch; pretrained transfer is not supported")
    if config.training.loss_type != "log_utility":
        raise ValueError("classical v1 selects checkpoints with canonical log_utility")
    device = t._resolve_device(config)
    runtime = t._build_execution_runtime(panel, config, device)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    manifest = t._checkpoint_manifest(panel, config)
    folds = list(folds)
    next_folds = t._next_fold_by_id(folds if deployment_folds is None else deployment_folds)
    store = PhysicalCSRStore(panel.features, settings.cache_root,
        cache_bytes=settings.cache_bytes)
    patience = max(1, int(math.ceil(config.training.epochs *
                                  config.training.early_stopping_no_improve_ratio)))
    results = []
    root_refresh_pending = False
    for fold_index, fold in enumerate(folds, 1):
        fold_started = time.perf_counter()
        completed = (t._load_completed_fold_result(output, fold.fold_id,
            expected_manifest=manifest, expected_fold=fold) if resume and not inference_only else None)
        if completed is not None:
            results.append(completed)
            root_refresh_pending = True
            if lifecycle:
                lifecycle.finish_fold(fold.fold_id)
            continue
        group = t._group_dir(output, fold.train_years)
        group.mkdir(parents=True, exist_ok=True)
        folder = t._fold_dir(output, fold.fold_id)
        folder.mkdir(parents=True, exist_ok=True)
        if lifecycle:
            lifecycle.start_group(group_name=group.name, group_index=fold_index,
                group_total=len(folds), fold_ids=[fold.fold_id],
                epoch_total=config.training.epochs)
        def progress(done, total):
            if lifecycle:
                lifecycle.update_work(completed=done, total=total, unit="input_work")
            if done == total or done % max(1, total // 20) == 0:
                print(f"[classical-prepare] {done}/{total}", flush=True)
        train_ds = _dataset(panel, fold.train_indices, config)
        val_ds = _dataset(panel, fold.val_indices, config)
        test_ds = _dataset(panel, fold.test_indices, config)
        matrices = [CausalWindowMatrix(store, ds, training=index==0,
                    batch_rows=settings.batch_rows)
                    for index, ds in enumerate((train_ds, val_ds, test_ds))]
        train, val, test = matrices
        preparation = t._PreEpochTimingRecorder(group / "pre_epoch_timing.jsonl",
            train_years=fold.train_years, fold_ids=[fold.fold_id], scope="classical")
        preparation.checkpoint("canonical_classical_datasets", train_rows=train.shape[0],
            features=train.shape[1], source_cache=str(store.root))
        val_split, test_split = map(dataset_to_windowed_tensors, (val_ds, test_ds))
        model = build_model(config=config, lookback=config.training.lookback,
            num_features=len(panel.feature_names), num_symbols=panel.num_symbols,
            feature_names=panel.feature_names)
        model.threads = settings.threads
        best_path = t._best_checkpoint_path(folder)
        last_path = t._group_checkpoint_path(output, fold.train_years)
        best, no_improve, start_epoch = math.inf, 0, 0
        previous = None
        restore_path = best_path if inference_only else last_path
        if (resume or inference_only) and restore_path.exists():
            previous = load_checkpoint(restore_path)
            t._validate_checkpoint_manifest(previous, manifest, checkpoint_path=restore_path,
                expected_train_years=fold.train_years,
                scope="inference" if inference_only else "resume")
            model.load_state_dict(previous["model_state_dict"])
            t._restore_rng_state(previous["rng_state"])
            start_epoch = int(previous["epoch"])
            no_improve = int(previous["no_improve_epochs"])
            best = float(previous["best_val_loss"])
        model.target_scale = (float(np.std(train.y, dtype=np.float64))
                              if previous is None else model.target_scale)
        if not math.isfinite(model.target_scale) or model.target_scale <= 1e-12:
            raise ValueError("classical training targets have no finite variance")
        curve = t._EpochCurveLifecycle(t._group_curve_path(output, fold.train_years),
            enabled=config.training.record_epoch_curve,
            interval=config.training.curve_plot_interval,
            async_enabled=config.training.curve_plot_async,
            defer_until_end=config.training.defer_epoch_curve_plot_until_end)
        if not inference_only:
            curve.prepare_resume(start_epoch)
        started = time.perf_counter()
        atomic_write_json(folder / "classical_contract.json", dict(
            input=store.identity, full_input_shape=train.shape,
            unobserved_training_labels=train.unobserved_tradable_labels,
            lookback=train.lookback, feature_count=train.features,
            train_dates=[str(panel.dates[i]) for i in train.dates],
            validation_dates=[str(panel.dates[i]) for i in val.dates],
            test_dates=[str(panel.dates[i]) for i in test.dates],
            model=asdict(settings), target_scale=model.target_scale,
            supervised_objective=("daily_group_lambdarank" if settings.kind == "lightgbm_ranker"
                                  else "standardized_future_log_return_squared_error"),
            selection_objective="canonical_fee_adjusted_log_utility",
            output="score_entmax_scale_separated_cash", test_used_for_selection=False))
        last_iteration_time = time.perf_counter()
        native_training_scores = None
        def checkpoint(epoch, val_scores, test_scores, *, train_loss=None, solver_state=None):
            nonlocal best, no_improve, last_iteration_time
            iteration_compute_s = time.perf_counter() - last_iteration_time
            epoch_start = last_iteration_time
            if lifecycle:
                lifecycle.set_phase("validation", fold_id=fold.fold_id, epoch=epoch)
            _, val_loss, val_metrics = evaluate_scores(model, val_scores, val,
                val_split, config, runtime, device)
            val_eval_s = time.perf_counter() - epoch_start - iteration_compute_s
            if lifecycle:
                lifecycle.set_phase("testing", fold_id=fold.fold_id, epoch=epoch)
            _, test_loss, test_metrics = evaluate_scores(model, test_scores, test,
                test_split, config, runtime, device)
            improved = val_loss < best - max(0.0, config.training.early_stopping_min_delta)
            if improved:
                best, no_improve = val_loss, 0
            else:
                no_improve += 1
            payload = dict(epoch=int(epoch), train_years=fold.train_years,
                fold_id=fold.fold_id, val_years=fold.val_years, test_years=fold.test_years,
                model_state_dict=model.state_dict(), optimizer_state_dict=None,
                scaler_state_dict=None, scheduler_state_dict=None,
                rng_state=t._capture_rng_state(), experiment_manifest=manifest,
                no_improve_epochs=no_improve, best_val_loss=best,
                early_stop_patience=patience,
                effective_train_batch_size=settings.batch_rows,
                classical_solver_state=_safe_solver_state(solver_state or {}),
                classical_native_train_scores=(None if native_training_scores is None else
                    torch.as_tensor(np.asarray(native_training_scores).copy())),
                classical_val_scores=torch.as_tensor(np.asarray(val_scores).copy()),
                classical_test_scores=torch.as_tensor(np.asarray(test_scores).copy()))
            if improved:
                t._atomic_torch_save(payload, best_path)
            t._atomic_torch_save(payload, last_path)
            epoch_wall_s = time.perf_counter() - epoch_start
            curve.record(dict(epoch=epoch, train_loss=train_loss,
                val_mean=val_loss, test_mean=test_loss, best_val_loss=best,
                improved=int(improved), no_improve=no_improve,
                lr=settings.learning_rate if settings.kind != "ridge" else None,
                train_compute_s=iteration_compute_s, val_eval_s=val_eval_s,
                epoch_wall_s=epoch_wall_s, epoch_total_s=epoch_wall_s,
                **{f"val_{k}":v for k,v in val_metrics.items()},
                **{f"test_{k}":v for k,v in test_metrics.items()}), request_plot=True)
            if lifecycle:
                lifecycle.finish_epoch(epoch=epoch, epoch_total=config.training.epochs,
                    metrics=dict(val_mean=val_loss, best_val_loss=best,
                                 no_improve=no_improve, epoch_total_s=epoch_wall_s))
            print(f"[classical] {settings.kind} iteration={epoch} val={val_loss:.8g} "
                  f"best={best:.8g} no_improve={no_improve} wall={epoch_wall_s:.3f}s", flush=True)
            last_iteration_time = time.perf_counter()
            return no_improve >= patience

        if not inference_only and no_improve < patience:
            if settings.kind.startswith("lightgbm"):
                import lightgbm as lgb
                params = dict(objective="lambdarank" if settings.kind.endswith("ranker") else "regression",
                    metric="None", max_bin=settings.max_bin, zero_as_missing=False,
                    use_missing=False, feature_pre_filter=False,
                    bin_construct_sample_cnt=settings.bin_sample_count,
                    num_threads=settings.threads, force_col_wise=True,
                    num_leaves=settings.num_leaves, max_depth=settings.max_depth,
                    learning_rate=settings.learning_rate, lambda_l2=settings.reg_lambda,
                    min_data_in_leaf=settings.min_data_in_leaf,
                    device_type=settings.backend, gpu_device_id=settings.gpu_device_id,
                    verbosity=-1, seed=config.training.seed)
                labels = train.rank_labels(settings.rank_levels) if settings.kind.endswith("ranker") else train.y / model.target_scale
                groups = train.groups[train.groups > 0] if settings.kind.endswith("ranker") else None
                bin_cache = Path(settings.cache_root)/"native-lightgbm-bins-v1"
                native_train = lightgbm_dataset(train, labels, params, groups=groups,
                    progress=progress,cache_root=bin_cache)
                ranking = settings.kind.endswith("ranker")
                native_val = lightgbm_dataset(val,
                    np.zeros(len(val.y)) if ranking else np.nan_to_num(val.y / model.target_scale),
                    params, reference=native_train,
                    groups=val.groups[val.groups > 0] if ranking else None,
                    progress=progress,cache_root=bin_cache)
                native_test = lightgbm_dataset(test,
                    np.zeros(len(test.y)) if ranking else np.nan_to_num(test.y / model.target_scale),
                    params, reference=native_train,
                    groups=test.groups[test.groups > 0] if ranking else None,
                    progress=progress,cache_root=bin_cache)
                # Ranker evaluation below uses the financial executor, never a
                # validation ranking label or a future test-derived transform.
                model.native = lightgbm_continue(params, [native_train,native_val,native_test],
                    previous=model.native if previous else None,
                    initial_scores=([previous["classical_native_train_scores"].numpy(),
                        previous["classical_val_scores"].numpy(),
                        previous["classical_test_scores"].numpy()] if previous else None))
                last_iteration_time = time.perf_counter()
                for epoch in range(start_epoch + 1, config.training.epochs + 1):
                    done = model.native.update()
                    model.iteration = model.native.current_iteration()
                    native_training_scores = lightgbm_scores(model.native, 0)
                    stop = checkpoint(epoch, lightgbm_scores(model.native, 1), lightgbm_scores(model.native, 2))
                    if done or stop:
                        break
                model.native.free_dataset()
                del native_train,native_val,native_test
            elif settings.kind == "ridge":
                from threadpoolctl import threadpool_limits
                with threadpool_limits(settings.threads):
                    operator = LaggedLinearOperator(train, backend=settings.backend,
                        gpu_device_id=settings.gpu_device_id,
                        device_budget_bytes=int(settings.device_memory_budget_gib * 1024**3), progress=progress,
                        threads=settings.threads)
                    evaluation_device = (1-settings.gpu_device_id
                        if settings.backend == "cuda" and torch.cuda.device_count()==2
                        else settings.gpu_device_id)
                    val_operator, test_operator = (LaggedLinearOperator(m,
                        backend=settings.backend, gpu_device_id=evaluation_device,
                        device_budget_bytes=int(settings.device_memory_budget_gib*1024**3),
                        threads=settings.threads) for m in (val, test))
                    state = None if previous is None else {k:(v.numpy() if isinstance(v,torch.Tensor) else v)
                        for k,v in previous["classical_solver_state"].items()}
                    def step(epoch, coefficient, intercept, state):
                        model.coefficient, model.intercept, model.iteration = coefficient, intercept, epoch
                        return checkpoint(epoch, val_operator._matvec(coefficient) + intercept,
                            test_operator._matvec(coefficient) + intercept,
                            train_loss=state["relative_residual"], solver_state=state)
                    try:
                        operator.solve(train.y / model.target_scale, alpha=settings.ridge_alpha,
                            tolerance=settings.ridge_tolerance,
                            max_iter=min(settings.ridge_max_iter, config.training.epochs), callback=step, state=state)
                    finally:
                        for owned_operator in (operator,val_operator,test_operator):
                            owned_operator.close()
            elif settings.kind == "catboost":
                from catboost import CatBoostRegressor, Pool
                if settings.backend != "cpu":
                    raise ValueError("CatBoost financial per-iteration stopping requires CPU init_model; use the declared CPU case")
                # Admission precedes the expanded allocation, never an OOM kill.
                raw = train.materialize(budget_bytes=int(settings.host_memory_budget_gib*1024**3))
                native_val = val.materialize(budget_bytes=int(settings.host_memory_budget_gib*1024**3)-raw.data.nbytes-raw.indices.nbytes)
                native_test = test.materialize(budget_bytes=int(settings.host_memory_budget_gib*1024**3)-raw.data.nbytes-raw.indices.nbytes-native_val.data.nbytes-native_val.indices.nbytes)
                pool = Pool(raw, label=train.y/model.target_scale, thread_count=settings.threads)
                # Pool.quantize does not accept thread_count (unlike Pool and
                # fit); its native quantizer owns its worker configuration.
                pool.quantize(border_count=settings.max_bin,
                    random_seed=config.training.seed)
                del raw
                for epoch in range(start_epoch + 1, config.training.epochs + 1):
                    update = CatBoostRegressor(iterations=1, depth=settings.max_depth,
                        learning_rate=settings.learning_rate, l2_leaf_reg=settings.reg_lambda,
                        thread_count=settings.threads, random_seed=config.training.seed + epoch - 1,
                        loss_function="RMSE", task_type="CPU", allow_writing_files=False,
                        verbose=False)
                    update.fit(pool, init_model=model.native)
                    model.native = update
                    model.iteration = epoch
                    if checkpoint(epoch, model.predict(native_val), model.predict(native_test)):
                        break
            else:
                import xgboost as xgb
                # Dense external-memory input preserves real numerical zeros.
                class Iterator(xgb.DataIter):
                    def __init__(self, matrix, cache):
                        self.matrix = matrix
                        super().__init__(cache_prefix=str(cache), on_host=True)
                    def reset(self):
                        self.iterator = iter(self.matrix.batches())
                    def next(self, input_data):
                        try:
                            a, b, csr = next(self.iterator)
                        except StopIteration:
                            return False
                        dense = csr.toarray()
                        if settings.backend == "cuda":
                            import cupy as cp
                            dense = cp.asarray(dense)
                        input_data(data=dense, label=self.matrix.y[a:b]/model.target_scale)
                        return True
                dense_min = train.shape[0] * train.shape[1] * max(1, math.ceil(math.log2(settings.max_bin + 1))) / 8
                import shutil
                available = min(settings.host_memory_budget_gib*1024**3, shutil.disk_usage(folder).free * .8)
                if dense_min > available:
                    raise MemoryError(f"numeric-zero XGBoost histogram lower-bound estimate {int(dense_min)} bytes exceeds cache admission {int(available)}; CSR missing semantics rejected")
                native_train = xgb.ExtMemQuantileDMatrix(Iterator(train, folder/"xgb-train"), max_bin=settings.max_bin)
                params = dict(objective="reg:squarederror", tree_method="hist",
                    device=f"cuda:{settings.gpu_device_id}" if settings.backend=="cuda" else "cpu",
                    nthread=settings.threads, max_bin=settings.max_bin, max_depth=settings.max_depth,
                    eta=settings.learning_rate, reg_lambda=settings.reg_lambda,
                    seed=config.training.seed)
                if model.native is None:
                    model.native = xgb.Booster(params=params, cache=[native_train])
                model.native.set_param(params)
                for epoch in range(start_epoch + 1, config.training.epochs + 1):
                    model.native.update(native_train, epoch - 1)
                    model.iteration = epoch
                    if checkpoint(epoch, _predict_matrix(model, val), _predict_matrix(model, test)):
                        break
        curve.flush()
        if not best_path.exists():
            raise RuntimeError("classical fit produced no finite selected checkpoint")
        selected = load_checkpoint(best_path)
        t._validate_checkpoint_manifest(selected, manifest, checkpoint_path=best_path,
            expected_fold=fold, scope="inference" if inference_only else "artifact")
        model.load_state_dict(selected["model_state_dict"])
        val_scores = (_predict_matrix(model, val) if inference_only
                      else selected["classical_val_scores"].numpy())
        test_scores = (_predict_matrix(model, test) if inference_only
                       else selected["classical_test_scores"].numpy())
        val_bt, val_loss, val_metrics = evaluate_scores(model,
            val_scores, val, val_split, config, runtime, device)
        test_bt, _, test_metrics = evaluate_scores(model,
            test_scores, test, test_split, config, runtime, device)
        test_bt = test_bt.to_numpy()
        dates = panel.dates[test.dates]
        prefix = t._deployment_test_prefix_rows(panel, fold,
            next_folds.get(int(fold.fold_id)), config.training.lookback,
            test_ds.valid_indices, execution_mode=config.trading.execution_mode,
            lookback_context=config.walk_forward.lookback_context)
        deployment_bt = (t._replay_physical_carry_split_prefix(test_bt, test_split,
            prefix, runtime=runtime, config=config) if runtime.day_trade_carry_source is not None
            else t._prefix_backtest_result(test_bt, prefix))
        result = t.FoldResult(fold_id=fold.fold_id, train_years=fold.train_years,
            val_years=fold.val_years, test_years=fold.test_years, best_val_loss=val_loss,
            val_ic={}, val_metrics=val_metrics, test_ic={}, test_metrics=test_metrics,
            test_integer_metrics=test_metrics if config.trading.execution_mode=="tw_day_trade" else None)
        t._atomic_torch_save(model.state_dict(), t._model_path(folder))
        save_timing, plot_timing = t._save_fold_output_artifacts(
            fold_dir=folder, fold_result=result, model=model, test_backtest=test_bt,
            test_dates=dates, symbols=list(panel.symbols), config=config,
            test_future_returns=test_ds.future_log_returns_t[test.dates],
            test_integer_backtest=(test_bt if config.trading.execution_mode=="tw_day_trade"
                and not config.trading.tw_day_trade_unlimited_margin_conversion else None),
            deployment_backtest=deployment_bt, deployment_dates=dates[:prefix],
            write_model_checkpoint=False, mark_complete=True,
            day_trade_carry_context=t._physical_carry_artifact_context(runtime=runtime,
                symbols=list(panel.symbols), config=config,
                initial_nav=float(config.trading.volume_participation_equity)),
            source_cache_snapshot=t._physical_source_cache_snapshot(runtime))
        atomic_write_json(folder/"plot_timing.json", plot_timing)
        results.append(result)
        t._refresh_walkforward_artifacts(output, results, panel=panel, config=config)
        root_refresh_pending = False
        atomic_write_json(folder/"classical_timing.json", dict(
            fold_workflow_wall_s=time.perf_counter()-fold_started,
            fit_and_artifact_wall_s=time.perf_counter()-started,
            excludes="process startup and shared panel loading",
            selected_iteration=model.iteration, best_val_loss=best,
            save=save_timing, plot=plot_timing, source_cache_hits=store.hits,
            source_cache_misses=store.misses))
        if lifecycle:
            lifecycle.finish_fold(fold.fold_id)
        del matrices, model
        gc.collect()
    if root_refresh_pending:
        t._refresh_walkforward_artifacts(output, results, panel=panel, config=config)
    return results
