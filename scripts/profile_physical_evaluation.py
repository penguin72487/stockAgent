"""Profile canonical physical evaluation with exact actions and smaller chunks.

Both GPUs run the same role as the complete DDP workflow: rank zero evaluates
validation, rank one evaluates test. No optimizer is advanced. The trainer
builds action/permission buffers once; only its existing replay chunk schedule
is varied. This probe never replaces whole-fold acceptance.
"""
from __future__ import annotations

import argparse
from dataclasses import fields
from datetime import timedelta
import gc
import os
from pathlib import Path
import statistics
import sys
import time


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code-root",type=Path,required=True)
    parser.add_argument("--config",type=Path,required=True)
    parser.add_argument("--checkpoint",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--chunks",type=int,nargs="+",default=[256,64,32,16])
    parser.add_argument("--repeats",type=int,default=3)
    parser.add_argument("--model-chunk",type=int,default=16)
    parser.add_argument("--cpu-threads",type=int,default=24)
    args=parser.parse_args()
    assert args.repeats>=3 and all(value>0 for value in args.chunks)
    sys.path.insert(0,str(args.code_root.resolve(strict=True)))
    import torch
    from downloader.artifact_io import atomic_write_json
    from stockagent.backtest.tw_day_trade_carry import (
        _compact_detached_carry_state,get_day_trade_carry_compile_stats,
    )
    from stockagent.config import load_config
    from stockagent.data.factorized_panel import attach_factorized_features,file_sha256
    from stockagent.data.panel import build_panel
    from stockagent.data.walkforward import build_expanding_year_folds
    from stockagent.data.tw_day_trade_execution import load_tw_day_trade_execution_tape
    from stockagent.data.tw_day_trade_carry_source import build_prepared_day_trade_carry_source
    from stockagent.models.factory import build_model
    from stockagent.models.temporal_basis_fit import temporal_basis_overrides_from_state_dict
    from stockagent.runtime_identity import verify_source_release
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.checkpoint_contract import build_checkpoint_manifest,validate_checkpoint_manifest
    from stockagent.training.runtime import load_checkpoint,load_model_state_dict
    from stockagent.training.windowed import dataset_to_windowed_tensors
    from stockagent.training import trainer
    from train import _build_panel_kwargs
    TimingBreakdown=trainer.TimingBreakdown

    rank=int(os.environ["LOCAL_RANK"])
    assert torch.cuda.is_available() and torch.cuda.device_count()==2
    torch.cuda.set_device(rank)
    torch.distributed.init_process_group("nccl",timeout=timedelta(minutes=30))
    assert torch.distributed.get_world_size()==2
    torch.set_num_threads(args.cpu_threads)
    torch.set_float32_matmul_precision("highest")
    device=torch.device("cuda",rank)
    output=args.output/f"rank_{rank}"
    output.mkdir(parents=True,exist_ok=False)
    source=verify_source_release(Path(os.environ["STOCKAGENT_CODE_RELEASE_RECEIPT"]),args.code_root)
    config=load_config(args.config)
    assert config.environment.amp_dtype=="bf16" and config.runner.require_cuda
    assert config.training.multi_gpu_strategy=="distributed_data_parallel"
    assert config.training.batch_size_train==32 and not config.training.day_trade_sparse_events
    panel=attach_factorized_features(build_panel(config.data.parquet_root,**_build_panel_kwargs(config)),
        config.data.factorized_feature_manifest,transfer_mode=config.data.factorized_transfer_mode)
    panel.day_trade_minute_execution=load_tw_day_trade_execution_tape(
        config.data.day_trade_minute_execution_root,panel_dates=panel.dates,panel_symbols=panel.symbols,
        official_open_prices=panel.open_prices,official_close_prices=panel.close_prices,
        daily_volume_shares=panel.daily_volumes,cache_dir=config.data.day_trade_minute_execution_cache_dir,
        allow_daily_proxy=config.data.day_trade_minute_execution_allow_daily_proxy,
        daily_proxy_price_policy=config.data.day_trade_minute_execution_daily_proxy_price_policy,
        policy=config.data.day_trade_minute_execution_policy)
    panel.day_trade_carry_source=build_prepared_day_trade_carry_source(
        panel=panel,minute_root=config.data.day_trade_minute_execution_root,
        public_feature_path=config.data.day_trade_physical_public_feature_path or config.data.tw_public_feature_path,
        cache_dir=config.data.day_trade_minute_execution_cache_dir,
        allow_daily_proxy=config.data.day_trade_minute_execution_allow_daily_proxy,
        daily_proxy_price_policy=config.data.day_trade_minute_execution_daily_proxy_price_policy,
        corporate_action_mode=config.trading.tw_corporate_action_mode,
        subscription_right_policy=config.trading.tw_day_trade_subscription_right_policy,
        entry_remainder_policy=config.trading.tw_day_trade_entry_remainder_policy,
        terminal_liquidation_unlimited_capacity=config.trading.tw_day_trade_terminal_liquidation_unlimited_capacity,
        sparse_event_slots=None)
    fold=next(value for value in build_expanding_year_folds(panel.dates,
        min_train_years=config.walk_forward.min_train_years,val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
        split_start_year=config.walk_forward.split_start_year) if value.fold_id==11)
    indices=fold.val_indices if rank==0 else fold.test_indices
    dataset=CrossSectionalDataset(panel,indices,config.training.lookback,include_volume_notional=True,
        execution_mode=config.trading.execution_mode,lookback_context=config.walk_forward.lookback_context,
        short_capacity_limit_enabled=config.trading.tw_short_capacity_limit_enabled,
        day_trade_unlimited_margin_conversion=config.trading.tw_day_trade_unlimited_margin_conversion,
        tw_corporate_action_mode=config.trading.tw_corporate_action_mode,
        tw_commission_rebate_timing=config.trading.tw_commission_rebate_timing)
    split=dataset_to_windowed_tensors(dataset)
    split.features=split.features.pin_memory()
    runtime=trainer._build_execution_runtime(panel,config,device)
    checkpoint=load_checkpoint(args.checkpoint)
    validate_checkpoint_manifest(checkpoint,build_checkpoint_manifest(panel,config),
        checkpoint_path=args.checkpoint,scope="artifact")
    state=checkpoint["model_state_dict"]
    model=build_model(config=config,lookback=config.training.lookback,num_features=len(panel.feature_names),
        num_symbols=panel.num_symbols,feature_names=panel.feature_names,
        temporal_basis_overrides=temporal_basis_overrides_from_state_dict(state))
    load_model_state_dict(model,state,strict_no_fallback=True)
    model.factorized_input_compile=True
    model=model.to(device).eval()
    wrapped=trainer._PanelSlabForwardWrapper(model)
    del state,checkpoint
    gc.collect()
    buffers={}
    canonical=trainer._run_eval_backtest_from_weight_buffers
    def capture(*values,**options):
        buffers.update(args=values,kwargs=dict(options))
        return canonical(*values,**options)
    trainer._run_eval_backtest_from_weight_buffers=capture
    try:
        reference,_,_=trainer._evaluate_windowed_tensor_batch_decoupled(
            model,wrapped,split,device,torch.bfloat16,True,config.trading.long_only,
            config.trading.buy_fee_rate,config.trading.sell_fee_rate,config.trading.max_turnover_ratio,
            1.0,config.trading.min_trade_weight,model_chunk_rows=args.model_chunk,
            backtest_chunk_rows=max(args.chunks),portfolio_activation=config.trading.portfolio_activation,
            compute_ic=False,compute_metrics_summary=False,return_weights_history=False,profile_timing=True,
            progress_label=f"[physical-eval-probe rank={rank}]",
            max_volume_participation=config.trading.max_volume_participation,
            volume_participation_equity=config.trading.volume_participation_equity,execution_runtime=runtime)
    finally:
        trainer._run_eval_backtest_from_weight_buffers=canonical
    assert len(buffers["args"][0])==len(split)
    original_state=_compact_detached_carry_state(reference.day_trade_carry_state)
    def compare(result):
        differences={}
        for name,tolerance in (("strategy_returns",1e-12),("turnovers",1e-10),
                               ("minute_nav",1e-8),("shares_history",1e-8),("weights_history",1e-8)):
            actual,expected=getattr(result,name),getattr(reference,name)
            assert (actual is None)==(expected is None),name
            if actual is None:continue
            torch.testing.assert_close(actual,expected,rtol=0,atol=tolerance)
            differences[name]=float((actual-expected).abs().max()) if actual.numel() else 0.
        actual=_compact_detached_carry_state(result.day_trade_carry_state)
        assert actual.last_session_day==original_state.last_session_day
        torch.testing.assert_close(actual.last_nav,original_state.last_nav,rtol=0,atol=1e-8)
        torch.testing.assert_close(actual.alive,original_state.alive,rtol=0,atol=0)
        for field in fields(actual.inventory):
            torch.testing.assert_close(getattr(actual.inventory,field.name),
                getattr(original_state.inventory,field.name),rtol=0,atol=1e-8)
        return differences
    records=[]
    for chunk in args.chunks:
        for repetition in range(args.repeats+1):
            torch.cuda.synchronize();torch.distributed.barrier()
            timing=TimingBreakdown()
            options={**buffers["kwargs"],"backtest_chunk_rows":chunk,"timing":timing,"progress_label":None}
            before=get_day_trade_carry_compile_stats()
            torch.cuda.reset_peak_memory_stats()
            started=time.perf_counter()
            with torch.inference_mode():
                result,_=canonical(*buffers["args"],**options)
            torch.cuda.synchronize()
            elapsed=time.perf_counter()-started
            maximum=torch.tensor(elapsed,dtype=torch.float64,device=device)
            torch.distributed.all_reduce(maximum,op=torch.distributed.ReduceOp.MAX)
            record={"chunk_rows":chunk,"warmup":repetition==0,"repetition":repetition,"wall_s":elapsed,
                "maximum_role_wall_s":float(maximum),"peak_allocated_bytes":torch.cuda.max_memory_allocated(),
                "compile_delta":{key:value-before.get(key,0) for key,value in get_day_trade_carry_compile_stats().items()}}
            try:
                record["max_abs_differences"]=compare(result)
                record["existing_financial_tolerances_passed"]=True
            except AssertionError as error:
                record["existing_financial_tolerances_passed"]=False
                record["failure"]=str(error)[:3000]
            records.append(record)
            atomic_write_json(output/"progress.json",records)
            print({"rank":rank,**record},flush=True)
            del result
    report={"state":"completed_canonical_physical_eval_chunk_probe","rank":rank,"world_size":2,
        "role":"validation" if rank==0 else "test","rows":len(split),"symbols":panel.num_symbols,
        "code_source_sha256":source["source_sha256"],"config_sha256":file_sha256(args.config),
        "checkpoint_sha256":file_sha256(args.checkpoint),"optimizer_updates":0,"model_actions_reused":True,
        "sparse_events":False,"full_fold_acceptance":False,"records":records,
        "maximum_role_median_s":{str(chunk):statistics.median(row["maximum_role_wall_s"] for row in records
            if row["chunk_rows"]==chunk and not row["warmup"]) for chunk in args.chunks}}
    atomic_write_json(output/"profile.json",report)
    torch.distributed.barrier();torch.distributed.destroy_process_group()


if __name__=="__main__":
    main()
