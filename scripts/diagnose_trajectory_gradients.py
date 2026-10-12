"""Bounded, read-only numerical replay of a frozen trainer's first trajectory.

Uses the canonical train.py forward/loss/backward unchanged. Diagnostic output
is isolated; every rank stops before the first optimizer update. This is not a
training or performance acceptance run.
"""
from __future__ import annotations

import argparse
import ast
import inspect
import json
import os
from pathlib import Path
import runpy
import sys


class DiagnosticComplete(RuntimeError):
    pass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--runtime-launch", type=Path, required=True)
    parser.add_argument("--control-entry", type=Path, required=True)
    parser.add_argument("--capture-head-batch", type=int, default=0)
    parser.add_argument("--stop-after-batches", type=int, default=0)
    parser.add_argument("--eager-head", action="store_true",
                        help="Oracle only: keep compiled encoder/loss; run the ordinary head eagerly.")
    args = parser.parse_args()
    source = args.source_root.resolve()
    output = args.output_dir.resolve()
    if "training_diagnostics" not in output.parts:
        parser.error("output must be isolated under training_diagnostics")
    if int(os.environ.get("WORLD_SIZE", "1")) != 2:
        parser.error("launch with two-rank torchrun; do not use a single GPU")
    rank = int(os.environ["RANK"])
    output.mkdir(parents=True, exist_ok=True)
    evidence_path = output / f"gradient-diagnostic-rank{rank}.jsonl"
    if evidence_path.exists():
        parser.error("existing diagnostic evidence must not be overwritten")
    sys.path.insert(0, str(source))
    # The thin control's import side effects select its live checkout, which
    # need not have the frozen trainer's config ABI. Reuse only its declared
    # environment function; never import live stockagent into this replay.
    control_tree = ast.parse(args.control_entry.read_text())
    environment_node = next(node for node in control_tree.body
                            if isinstance(node, ast.FunctionDef)
                            and node.name == "_environment")
    environment_scope = {"os": os, "Path": Path}
    exec(compile(ast.Module(body=[environment_node], type_ignores=[]),
                 str(args.control_entry), "exec"), environment_scope)
    launch = json.loads(args.runtime_launch.read_text())
    proof = json.loads(Path(launch["acceptance_receipt"]).read_text())
    launch["source_receipt"] = str(source.parent / "release.json")
    os.environ.update(environment_scope["_environment"](launch, proof, source))
    import torch
    from stockagent.training import trainer
    from stockagent.models import factorized_input

    active: dict = {}

    def emit(kind: str, **values) -> None:
        record = {"kind": kind, "rank": rank, **values}
        line = json.dumps(record, allow_nan=False)
        with evidence_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        print("[gradient-diagnostic] " + line, flush=True)

    def describe(tensor) -> dict:
        tensor = tensor.detach()
        finite = torch.isfinite(tensor)
        clean = torch.where(finite, tensor, torch.zeros_like(tensor))
        return {
            "shape": list(tensor.shape), "dtype": str(tensor.dtype),
            "finite": bool(finite.all().item()),
            "nan_count": int(torch.isnan(tensor).sum().item()),
            "inf_count": int(torch.isinf(tensor).sum().item()),
            "max_finite_abs": float(clean.abs().max().item()),
            "first_bad_indices": (~finite).nonzero()[:8].cpu().tolist(),
        }

    original_bind = trainer.bind_physical_carry_loss

    def bind(*positional, **kwargs):
        loss_fn = original_bind(*positional, **kwargs)
        row_start = int(kwargs.get("start", -1))

        def loss(*inputs, **options):
            weights = inputs[0]
            if active:
                emit("weights", row_start=row_start, **describe(weights))
                if weights.requires_grad:
                    def on_gradient(gradient):
                        emit("loss_weights_gradient", row_start=row_start,
                             **describe(gradient))
                    weights.register_hook(on_gradient)
            result = loss_fn(*inputs, **options)
            if active:
                emit("loss", row_start=row_start, value=float(result.detach().item()))
            return result
        return loss

    original_epoch = trainer._train_epoch_windowed_tensor_ddp
    epoch_signature = inspect.signature(original_epoch)
    original_partition = factorized_input._call_partition

    def partition(model, fn, *inputs, **options):
        is_head = fn is model.__dict__.get("_factorized_head_fn")
        capture = (active and args.capture_head_batch > 0
                   and active["batches"] + 1 == args.capture_head_batch
                   and is_head)
        if args.eager_head and is_head:
            result = model._forward_stock_embeddings(*inputs, **options)
        else:
            result = original_partition(model, fn, *inputs, **options)
        if capture:
            state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
            snapshot = {"model_state": state, "embeddings": inputs[0].detach().cpu(),
                        "mask": inputs[1].detach().cpu(), "weights": result.detach().cpu(),
                        "batch_index": args.capture_head_batch,
                        "autocast_enabled": torch.is_autocast_enabled("cuda")}
            def save(gradient):
                snapshot["upstream_gradient"] = gradient.detach().cpu()
                torch.save(snapshot, output / f"head-snapshot-rank{rank}.pt")
                emit("head_snapshot_saved", batch_index=args.capture_head_batch)
            result.register_hook(save)
        return result

    def epoch(*positional, **kwargs):
        bound = epoch_signature.bind(*positional, **kwargs)
        active.update(model=bound.arguments["model"], batches=0)
        emit("trajectory_start", global_batch_size=bound.arguments["batch_size"],
             optimizer_updates_allowed=0, trainer_source=str(trainer.__file__))
        return original_epoch(*positional, **kwargs)

    original_accumulate = trainer._accumulate_gradient_norm_diagnostic_

    def inspect_gradients(batch_index):
        model = trainer._unwrap_model(active["model"])
        gradients = [(name, p.grad.detach()) for name, p in model.named_parameters()
                     if p.grad is not None]
        norms = torch.stack(torch._foreach_norm([g for _, g in gradients], float("inf")))
        norms_cpu = norms.cpu().tolist()
        bad = [name for (name, _), norm in zip(gradients, norms_cpu)
               if not float("-inf") < norm < float("inf")]
        norm64 = torch.linalg.vector_norm(torch.stack([
            torch.linalg.vector_norm(g, dtype=torch.float64) for _, g in gradients
        ]))
        norm64_value = float(norm64.item())
        emit("parameter_gradients", batch_index=batch_index,
             gradient_tensors=len(gradients), nonfinite_parameter_names=bad,
             max_norm_fp64=(norm64_value if not bad else None),
             max_finite_abs=max((norm for norm in norms_cpu
                                if float("-inf") < norm < float("inf")), default=0.0))
        if not bad and norm64_value > 1e19:
            emit("largest_parameter_gradients", batch_index=batch_index,
                 values=sorted(zip([name for name, _ in gradients], norms_cpu),
                               key=lambda item: item[1], reverse=True)[:8])
        for name, gradient in gradients:
            if name in bad:
                emit("bad_parameter_gradient", name=name, **describe(gradient))
                break
        return bool(bad)

    def accumulate(*positional, **kwargs):
        original_accumulate(*positional, **kwargs)
        if active:
            active["batches"] += 1
            bad = inspect_gradients(kwargs.get("batch_index"))
            if bad:
                raise DiagnosticComplete("first non-finite backward; no optimizer update")
            if args.stop_after_batches and active["batches"] >= args.stop_after_batches:
                raise DiagnosticComplete("bounded batches inspected; no optimizer update")

    def finalize(**kwargs):
        inspect_gradients(kwargs["batch_index"])
        emit("stop_before_optimizer", reason="full trajectory inspected")
        raise DiagnosticComplete("complete first trajectory; no optimizer update")

    trainer.bind_physical_carry_loss = bind
    trainer._train_epoch_windowed_tensor_ddp = epoch
    trainer._accumulate_gradient_norm_diagnostic_ = accumulate
    trainer._finalize_trajectory_optimizer_step = finalize
    factorized_input._call_partition = partition
    sys.argv = [str(source / "train.py"), "--config", str(args.config.resolve()),
                "--output-dir", str(output), "--start-fold", "11", "--max-folds", "1",
                "--multi-gpu-strategy", "distributed_data_parallel", "--no-resume",
                "--cpu-threads", "8", "--torch-compile-threads", "16",
                "--no-profile-timing", "--no-debug-timing-sync", "--no-isolate-train-folds"]
    try:
        runpy.run_path(str(source / "train.py"), run_name="__main__")
    except DiagnosticComplete as exc:
        emit("diagnostic_complete", reason=str(exc), optimizer_updates=0)


if __name__ == "__main__":
    main()
