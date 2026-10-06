"""Fit the actual fold's feature-axis SVD without advancing an optimizer.

Uses the same panel, dataset ownership, RMS fit/cache and SVD fit/cache as
train.py. A dual-GPU distributed run shards input dates, not features.
"""
from __future__ import annotations

import argparse
from datetime import timedelta
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--fold',type=int,default=11)
    parser.add_argument('--cpu-threads',type=int,default=24)
    parser.add_argument('--full-spectrum',action='store_true',
        help='Analysis only: every singular value, FP64 Gram; no model projection')
    args=parser.parse_args()
    import torch
    from downloader.artifact_io import atomic_write_json, atomic_write_text
    from stockagent.config import load_config
    from stockagent.data.factorized_panel import attach_factorized_features
    from stockagent.data.panel import build_panel
    from stockagent.data.walkforward import build_expanding_year_folds
    from stockagent.models.factory import build_model
    from stockagent.models.feature_svd import full_training_feature_spectrum
    from stockagent.runtime_identity import verify_source_release
    from stockagent.training.dataset import CrossSectionalDataset, execution_feature_lag
    from stockagent.training.trainer import (
        _apply_causal_feature_rms_to_model, _apply_feature_svd_to_model,
        _fit_group_causal_feature_rms, _fit_group_feature_svd,
    )
    from train import _build_panel_kwargs

    config=load_config(args.config)
    assert config.runner.require_cuda and config.environment.amp_dtype=='bf16'
    assert torch.cuda.is_available() and torch.cuda.device_count()==2
    assert config.training.multi_gpu_strategy=='distributed_data_parallel'
    rank=int(os.environ['LOCAL_RANK'])
    torch.cuda.set_device(rank)
    torch.set_num_threads(args.cpu_threads)
    torch.set_float32_matmul_precision('highest')
    torch.distributed.init_process_group('nccl',timeout=timedelta(minutes=30))
    try:
        assert torch.distributed.get_world_size()==2
        source=verify_source_release(Path(os.environ['STOCKAGENT_CODE_RELEASE_RECEIPT']),ROOT)
        panel=attach_factorized_features(build_panel(config.data.parquet_root,**_build_panel_kwargs(config)),
            config.data.factorized_feature_manifest,transfer_mode=config.data.factorized_transfer_mode)
        fold=next(value for value in build_expanding_year_folds(panel.dates,
            min_train_years=config.walk_forward.min_train_years,val_years=config.walk_forward.val_years,
            require_future_test_year=config.walk_forward.require_future_test_year,
            split_start_year=config.walk_forward.split_start_year) if value.fold_id==args.fold)
        dataset=CrossSectionalDataset(panel,fold.train_indices,config.training.lookback,
            include_volume_notional=True,execution_mode=config.trading.execution_mode,
            lookback_context=config.walk_forward.lookback_context,
            short_capacity_limit_enabled=config.trading.tw_short_capacity_limit_enabled,
            day_trade_unlimited_margin_conversion=config.trading.tw_day_trade_unlimited_margin_conversion,
            tw_corporate_action_mode=config.trading.tw_corporate_action_mode,
            tw_commission_rebate_timing=config.trading.tw_commission_rebate_timing)
        rms=_fit_group_causal_feature_rms(config=config,panel=panel,train_ds=dataset,
            train_years=fold.train_years,group_folds=[fold],output_path=args.output)
        if args.full_spectrum:
            assert config.training.financial_transformer.feature_svd_components==0
            assert not config.training.financial_transformer.temporal_basis_families
            assert rms is not None
            scale,active,rms_metadata=rms
            import json
            result=full_training_feature_spectrum(panel.features,panel.alive_mask,
                dataset.valid_indices,lookback=config.training.lookback,
                feature_lag=execution_feature_lag(dataset.execution_mode),scale=scale,active_mask=active,
                device=torch.device('cuda',rank),distributed=True,
                progress=lambda value:print(json.dumps(value),flush=True))
            metadata=result.metadata
            assert metadata['alive_training_cells']==rms_metadata['alive_cell_count']
            if rank==0:
                proof={
                    'state':'accepted_full_training_feature_spectrum_analysis_only',
                    'optimizer_started':False,'formal_training_started':False,
                    'code_source_sha256':source['source_sha256'],'fold_id':fold.fold_id,
                    'train_years':fold.train_years,'validation_years':fold.val_years,
                    'test_years':fold.test_years,'source_features':len(panel.feature_names),
                    'selected_model_decomposition':'none','basis_banks':[],
                    'ddp_world_size':2,'feature_spectrum':metadata,
                    'rms_fit_metadata':rms_metadata,
                }
                atomic_write_json(args.output/'full-feature-spectrum.json',proof)
                spectrum=['dimensions,squared_singular_value,singular_value,direction_percent,cumulative_percent',
                          '0,0,0,0,0']
                details=['# 完整特徵維度與平方奇異值能量',
                         '', '僅 fold11 訓練資料；這是能量，不是預測資訊。模型不使用此分解。',
                         '', '| 維度 | 個別方向能量 % | 累積能量 % |',
                         '| ---: | ---: | ---: |', '| 0 | 0 | 0 |']
                for index,(value,ratio) in enumerate(zip(metadata['squared_singular_values'],
                        metadata['cumulative_squared_energy_ratio']),1):
                    single=value/metadata['total_squared_energy']*100
                    spectrum.append(f'{index},{value:.17g},{value**.5:.17g},{single:.17g},{ratio*100:.17g}')
                    details.append(f'| {index} | {single:.10f} | {ratio*100:.10f} |')
                atomic_write_text(args.output/'dimensions-to-energy.csv','\n'.join(spectrum)+'\n')
                atomic_write_text(args.output/'dimensions-to-energy.md','\n'.join(details)+'\n')
                threshold_csv=['target_percent,dimensions,retained_percent,meaning']
                report=['# Fold11 完整特徵光譜（分析用，不接入訓練）',
                        '', '## 1. 執行進度',
                        '', '- 全訓練資料、全特徵光譜已計算；沒有啟動 optimizer 或正式訓練。',
                        '- 最新模型設定為不分解；此分析不改動資料、模型或成交規則。',
                        '', '## 2. 範圍與定義',
                        '', f"訓練年：{fold.train_years}；{metadata['alive_training_cells']:,} 個 unique alive 日期／商品細胞。",
                        f"完整輸入 {metadata['features']:,} 維；training RMS active {metadata['active_features']:,} 維，",
                        f"另 {metadata['training_inactive_zero_coordinates']:,} 維在此訓練正規化下為零，仍留在模型 schema。",
                        '', '`E(k) = sum_{i<=k} sigma_i² / ||X_training_RMS||_F²`。不中心化；不是 PCA variance、互資訊或報酬率。',
                        '只使用 unique causal training input 日期，不重複計數 lookback，不含 validation／test。',
                        '', f"FP64 Gram 與完整 eigvalsh；分母獨立累計 {metadata['total_squared_energy']:.12g}。",
                        f"Gram trace 相對誤差 {metadata['trace_relative_error']:.3g}；全部 eigenvalue sum 相對誤差 {metadata['eigen_sum_relative_error']:.3g}。",
                        f"數值秩 {metadata['numerical_rank']:,}，lambda 容差 {metadata['numerical_rank_tolerance']:.12g}；不聲稱是精確代數秩。",
                        '', '100% 列採完整 active 座標上界、不截任何非零方向；不是宣稱它是精確最小代數秩。',
                        '99.9%／99.99%／99.999% 是可核對的最少維度，不能把顯示四捨五入的 100.00% 當完全無損。',
                        '', '## 3. 百分比 → 所需維度',
                        '', '| 目標能量 % | 維度 | 實際累積能量 % | 定義 |',
                        '| ---: | ---: | ---: | --- |']
                for row in metadata['percent_to_dimensions']:
                    threshold_csv.append(f"{row['target_percent']:.12g},{row['dimensions']},{row['retained_percent']:.17g},{row['meaning']}")
                    label='完整 active、不截斷上界' if row['target_percent']==100 else '最少維度'
                    report.append(f"| {row['target_percent']:.12g} | {row['dimensions']:,} | {row['retained_percent']:.10f} | {label} |")
                report += ['', '## 4. 完整明細與限制', '',
                    '[每一維完整 Markdown](dimensions-to-energy.md)、[維度→能量 CSV](dimensions-to-energy.csv)、',
                    '[百分比→維度 CSV](percent-to-dimensions.csv)、[精確數值與來源收據](full-feature-spectrum.json)。',
                    '', f"Gram {metadata['gram_wall_s']:.2f}s，完整 eigensolver {metadata['eigensolver_wall_s']:.2f}s，分析總計 {metadata['fit_wall_s']:.2f}s。",
                    '此數字不是訓練吞吐量。既有私人來源授權／research-only／非歷史 PIT 限制與企業行動 masks 不變。',
                    '', '方法參考：[PyTorch eigvalsh](https://docs.pytorch.org/docs/2.11/generated/torch.linalg.eigvalsh.html)、',
                    '[非中心化 TruncatedSVD](https://scikit-learn.org/stable/modules/generated/sklearn.decomposition.TruncatedSVD.html)。']
                atomic_write_text(args.output/'percent-to-dimensions.csv','\n'.join(threshold_csv)+'\n')
                atomic_write_text(args.output/'feature-spectrum-report.md','\n'.join(report)+'\n')
                print(json.dumps({'state':proof['state'],'analysis_wall_s':metadata['fit_wall_s'],
                    'top32_percent':metadata['cumulative_squared_energy_ratio'][31]*100,
                    'numerical_rank':metadata['numerical_rank']}),flush=True)
            torch.distributed.barrier()
            return
        fitted=_fit_group_feature_svd(config=config,panel=panel,train_ds=dataset,
            train_years=fold.train_years,group_folds=[fold],output_path=args.output,
            device=torch.device('cuda',rank),causal_feature_rms=rms)
        model=build_model(config=config,lookback=config.training.lookback,num_features=len(panel.feature_names),
            num_symbols=panel.num_symbols,feature_names=panel.feature_names)
        _apply_causal_feature_rms_to_model(model,rms)
        _apply_feature_svd_to_model(model,fitted)
        assert model.temporal_basis_feature_encoder is None
        assert model.temporal_basis_input_feature_builder is None
        assert not model.temporal_basis_families
        assert model.candle_encoder.joint_projection.proj.in_features==32
        assert model.candle_encoder.continuous_feature_bottleneck is None
        metadata=fitted[1]
        if rank==0:
            atomic_write_json(args.output/'svd-spectrum-profile.json',{
                'state':'accepted_full_training_feature_spectrum_only',
                'optimizer_started':False,'formal_training_started':False,
                'code_source_sha256':source['source_sha256'],'fold_id':fold.fold_id,
                'train_years':fold.train_years,'validation_years':fold.val_years,
                'test_years':fold.test_years,'source_features':len(panel.feature_names),
                'model_parameters':sum(value.numel() for value in model.parameters()),
                'model_buffers':sum(value.numel() for value in model.buffers()),
                'resolved_attention_mode':model.attention_mode,'feature_svd':metadata,
                'selected_dimensions':{str(k):metadata['cumulative_squared_energy_ratio'][k-1]
                    for k in (1,2,4,8,16,24,32,48,64,96,128)
                    if k<=len(metadata['cumulative_squared_energy_ratio'])},
                'basis_banks':[], 'ddp_world_size':2,
            })
        torch.distributed.barrier()
    finally:
        torch.distributed.destroy_process_group()


if __name__=='__main__':
    main()
