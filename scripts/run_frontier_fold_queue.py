#!/usr/bin/env python3
"""Receipt-bound sequential full-fold recipes; the canonical trainer owns work."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import yaml
from downloader.artifact_io import atomic_write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--baseline-config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--after-exit", type=Path)
    parser.add_argument("--kernel-python", type=Path)
    args = parser.parse_args()
    if args.after_exit:
        while not args.after_exit.exists():
            time.sleep(10)
    recipes = yaml.safe_load(args.spec.read_text())["full_fold_candidates"]
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "queue-spec.yaml").write_bytes(args.spec.read_bytes())
    results = []
    for recipe in recipes:
        name = recipe["name"]
        overrides = args.output / f"{name}-overrides.yaml"
        overrides.write_text(yaml.safe_dump(recipe["overrides"], sort_keys=False))
        runtime = args.output / f"{name}-runtime.yaml"
        runtime.write_text(yaml.safe_dump(recipe.get("runtime_overrides", {}), sort_keys=False))
        python = args.kernel_python if recipe.get("kernel_environment") else Path(sys.executable)
        if python is None:
            raise ValueError("kernel-dependent recipe needs --kernel-python")
        code = Path(recipe.get("code_root", ROOT)).resolve()
        command = [str(python), str(ROOT / "scripts/run_frontier_training_trial.py"),
            "--code-root", str(code), "--source-receipt", str(code.parent / "release.json"),
            "--baseline-config", str(args.baseline_config), "--output", str(args.output / name),
            "--name", name, "--overrides", str(overrides), "--runtime-overrides", str(runtime),
            "--batch-size", str(recipe.get("batch_size", 256)),
            "--cpu-threads", str(recipe.get("cpu_threads", 8)),
            "--compile-threads", str(recipe.get("compile_threads", 16)),
            "--min-vram-headroom-gib", str(recipe.get("min_vram_headroom_gib", 1.5)),
            "--max-peak-vram-fraction", str(recipe.get("max_peak_vram_fraction", 0.96)),
            "--epochs", str(recipe.get("epochs", 4)), "--timeout-s", str(recipe.get("timeout_s", 2400))]
        if recipe.get("profile_timing"):
            command.append("--profile-timing")
        if recipe.get("nsys_trace"):
            command.append("--nsys-trace")
        env = os.environ.copy()
        env.update(PYTHON_BIN=str(python), FINTECH_ENV_PATH=str(python.parent.parent))
        if recipe.get("cuda_home"):
            sdk = Path(recipe["cuda_home"]).resolve()
            nvcc = sdk / "bin/nvcc"
            if not nvcc.is_file() or not os.access(nvcc, os.X_OK):
                raise ValueError(f"declared CUDA SDK has no executable nvcc: {sdk}")
            # The canonical Python and shell runtimes prioritize this explicit
            # override over an environment's headers-only CUDA target tree.
            env.update(CUDA_HOME=str(sdk), STOCKAGENT_CUDA_ROOT=str(sdk))
        if recipe.get("transformer_engine_environment"):
            # This overlay belongs only to the isolated TE trial process tree.
            # Native training continues to load its original cuDNN library.
            import hashlib
            overlay = python.parent.parent / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages/nvidia/cudnn/lib"
            library = overlay / "libcudnn.so.9"
            if not recipe.get("kernel_environment") or not library.is_file():
                raise RuntimeError("TE recipe requires its isolated cuDNN overlay")
            env["LD_PRELOAD"] = str(library) + (":" + env["LD_PRELOAD"] if env.get("LD_PRELOAD") else "")
            env["LD_LIBRARY_PATH"] = str(overlay) + (":" + env["LD_LIBRARY_PATH"] if env.get("LD_LIBRARY_PATH") else "")
            env["STOCKAGENT_FRONTIER_CUDNN_OVERLAY"] = str(overlay)
            atomic_write_json(args.output / f"{name}-library-overlay.json", {
                "library": str(library), "sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
                "scope": "isolated trial process tree", "native_runtime_modified": False})
        started = time.perf_counter()
        with (args.output / f"{name}-launcher.log").open("w") as log:
            result = subprocess.run(command, cwd=code, env=env, stdout=log, stderr=subprocess.STDOUT)
        results.append({"name": name, "returncode": result.returncode,
            "wall_s": time.perf_counter() - started, "output": str(args.output / name)})
        atomic_write_json(args.output / "queue-progress.json", results)
        print(json.dumps(results[-1]), flush=True)
    atomic_write_json(args.output / "queue-exit.json", {"completed": len(results), "results": results})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
