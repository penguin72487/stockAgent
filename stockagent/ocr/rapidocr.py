"""Bounded, single-owner OCR using retained RapidOCR models and ONNX Runtime.

PDF rendering and source/receipt publication stay with the canonical extractor.
Only neural inference moves to CUDA; preprocessing, decoding and coordinates
retain RapidOCR's implementation. GPU failures never retry the document on CPU.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from functools import cache
from importlib import import_module, metadata
import json
import os
from pathlib import Path
import sys
import time

from downloader.artifact_io import sha256_file

NATIVE_THREAD_VARIABLES = (
    "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS",
)


@dataclass(frozen=True)
class ExecutionBudget:
    device: str = "cuda"
    device_id: int = 0
    cpu_threads: int = 1
    cpu_cores: int = 4
    nice: int = 10
    gpu_memory_mb: int = 2048  # Per ONNX session arena, not total device memory.
    min_free_gpu_mb: int = 2048
    disable_thp: bool = True

    def __post_init__(self):
        if self.device not in {"cpu", "cuda"}:
            raise ValueError("OCR device must be cpu or cuda")
        if type(self.disable_thp) is not bool:
            raise ValueError("disable_thp must be boolean")
        limits = {
            "device_id": (0, 63), "cpu_threads": (1, 8), "cpu_cores": (1, 256),
            "nice": (0, 19), "gpu_memory_mb": (128, 131072),
            "min_free_gpu_mb": (128, 131072),
        }
        for name, (low, high) in limits.items():
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"invalid OCR execution budget: {name}")


@cache
def rapidocr_runtime(config_path):
    """Verify retained models/packages before importing an inference engine.

Legacy CPU configuration and receipt fields are deliberately kept compatible.
An execution block opts into the bounded runner and a separate fingerprint.
"""
    config_path = Path(config_path)
    config = json.loads(config_path.read_text())
    libraries = Path(config["libraries"]).resolve()
    if not libraries.is_dir():
        raise ValueError(f"OCR dependency directory does not exist: {libraries}")
    if set(config["models"]) != {"Det", "Rec", "Cls"}:
        raise ValueError("all RapidOCR components must use retained local models")
    hashes = {}
    for name, entry in config["models"].items():
        if sha256_file(Path(entry["path"])) != entry["sha256"]:
            raise ValueError("RapidOCR model hash mismatch: " + name)
        hashes[name] = entry["sha256"]
    package_names = set(config["packages"])
    runtime_packages = package_names & {"onnxruntime", "onnxruntime-gpu"}
    if len(runtime_packages) != 1 or package_names != {
        "rapidocr", "numpy", "opencv-python", *runtime_packages
    }:
        raise ValueError("bind exactly one ONNX runtime and all OCR packages")
    if "execution" in config:
        budget = ExecutionBudget(**config["execution"])
        if budget.device == "cuda" and runtime_packages != {"onnxruntime-gpu"}:
            raise ValueError("CUDA OCR requires the pinned onnxruntime-gpu distribution")
        # Mixing binary modules from two isolated directories cannot be repaired
        # by changing sys.path after import. Use a fresh process instead.
        for name in ("numpy", "cv2", "rapidocr", "onnxruntime"):
            module = sys.modules.get(name)
            if module is not None and getattr(module, "__file__", None):
                if not Path(module.__file__).resolve().is_relative_to(libraries):
                    raise RuntimeError(f"{name} is already loaded from another runtime; use a fresh process")
        # The CLI calls this before collector imports can initialize NumPy's
        # native pools. Setting these only when creating ORT sessions is late.
        for key in NATIVE_THREAD_VARIABLES:
            os.environ[key] = str(budget.cpu_threads)
    sys.path.insert(0, str(libraries))
    installed = {name: metadata.version(name) for name in sorted(package_names)}
    if installed != config["packages"]:
        raise ValueError("RapidOCR dependency versions differ from the retained profile")
    runtime = dict(engine="rapidocr", packages=installed, model_sha256=hashes,
                   config_sha256=sha256_file(config_path))
    if "execution" in config:
        runtime.update(execution=asdict(budget), implementation="bounded_rapidocr_v1",
                       implementation_sha256=sha256_file(Path(__file__)))
    return config, runtime


def _parameters(config):
    from rapidocr import ModelType, OCRVersion
    params = dict(config["params"])
    for key, value in tuple(params.items()):
        if key.endswith(".model_type"):
            params[key] = ModelType(value)
        if key.endswith(".ocr_version"):
            params[key] = OCRVersion(value)
    for name, entry in config["models"].items():
        params[name + ".model_path"] = entry["path"]
    return params


def require_provider(session, device):
    required = "CUDAExecutionProvider" if device == "cuda" else "CPUExecutionProvider"
    providers = session.get_providers()
    if not providers or providers[0] != required:
        raise RuntimeError(f"OCR requires {required}; actual providers: {providers}")
    # ORT's Python wrapper can retry on CPU after an EP runtime failure unless
    # explicitly disabled, even after successful CUDA session initialization.
    session.disable_fallback()
    return providers


def summarize_profile(path):
    """Actual executed nodes, distinct from an advertised provider list."""
    counts, duration, cpu_compute = Counter(), Counter(), Counter()
    for event in json.loads(Path(path).read_text()):
        args = event.get("args", {})
        provider = args.get("provider")
        if event.get("cat") != "Node" or not provider:
            continue
        counts[provider] += 1
        duration[provider] += event.get("dur", 0)
        op = args.get("op_name", "")
        if provider == "CPUExecutionProvider" and any(k in op for k in ("Conv", "MatMul", "Gemm", "LSTM")):
            cpu_compute[op] += 1
    return {"path": str(path), "sha256": sha256_file(path),
            "node_events": dict(counts), "node_duration_us": dict(duration),
            "cpu_neural_compute": dict(cpu_compute)}


class RapidOCRRunner:
    """One process owns the three reusable sessions and bounded line batches.

The caller uses a dedicated process: thread budgets, niceness and affinity are
process-wide. Concurrent GPU OCR owners are rejected with a per-device lock.
"""

    def __init__(self, config_path, *, profile_dir=None):
        config, runtime = rapidocr_runtime(config_path)
        self.budget = ExecutionBudget(**config["execution"])
        self.owner_pid = os.getpid()
        self.closed = False
        self.lock = None
        self.sessions = {}
        self.calls = 0
        self.elapsed_s = 0.0
        self.profile_dir = Path(profile_dir) if profile_dir is not None else None
        self.runtime = dict(runtime)
        started = time.perf_counter()
        try:
            self._initialize(config)
        except BaseException:
            self.close()
            raise
        self.initialization_s = time.perf_counter() - started

    def _initialize(self, config):
        budget = self.budget
        if budget.disable_thp:
            if not sys.platform.startswith("linux"):
                raise RuntimeError("the bounded OCR THP policy requires Linux")
            import ctypes
            libc = ctypes.CDLL(None, use_errno=True)
            # Per-process only: large temporary NumPy arrays must not stall
            # this host in synchronous huge-page memory compaction.
            if libc.prctl(41, 1, 0, 0, 0) != 0 or libc.prctl(42, 0, 0, 0, 0) != 1:
                raise OSError(ctypes.get_errno(), "cannot disable THP for this OCR process")
        for key in NATIVE_THREAD_VARIABLES:
            os.environ[key] = str(budget.cpu_threads)
        tids = [int(p.name) for p in Path('/proc/self/task').iterdir()] if sys.platform.startswith('linux') else [0]
        if hasattr(os, "sched_getaffinity"):
            cpus = sorted(os.sched_getaffinity(0))[-budget.cpu_cores:]
            for tid in tids:
                try:
                    os.sched_setaffinity(tid, cpus)
                except ProcessLookupError:
                    pass
        if hasattr(os, "setpriority"):
            for tid in tids:
                try:
                    os.setpriority(os.PRIO_PROCESS, tid, max(budget.nice, os.getpriority(os.PRIO_PROCESS, tid)))
                except ProcessLookupError:
                    pass
        ort = import_module("onnxruntime")
        cv2 = import_module("cv2")
        cv2.setNumThreads(budget.cpu_threads)
        if budget.device == "cuda":
            if "CUDAExecutionProvider" not in ort.get_available_providers():
                raise RuntimeError("CUDAExecutionProvider unavailable; CPU fallback is forbidden")
            import fcntl
            self.lock = open(f"/tmp/stockagent-ocr-{os.getuid()}-cuda-{budget.device_id}.lock", "a+")
            try:
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("another GPU OCR process owns this device") from exc
            # Importing the project's CUDA-compatible torch loads CUDA/cuDNN
            # libraries without modifying the training environment installation.
            import torch
            if not torch.cuda.is_available() or budget.device_id >= torch.cuda.device_count():
                raise RuntimeError("requested CUDA device unavailable; CPU fallback is forbidden")
            torch.set_num_threads(budget.cpu_threads)
            free_bytes, _ = torch.cuda.mem_get_info(budget.device_id)
            if free_bytes < budget.min_free_gpu_mb * 1024**2:
                raise RuntimeError("insufficient free GPU memory for the OCR budget")
            self.runtime["cuda_runtime"] = dict(torch=torch.__version__, cuda=torch.version.cuda,
                cudnn=torch.backends.cudnn.version(), device=torch.cuda.get_device_name(budget.device_id))
        if self.profile_dir:
            self.profile_dir.mkdir(parents=True, exist_ok=True)
        for name, entry in config["models"].items():
            opts = ort.SessionOptions()
            opts.intra_op_num_threads = budget.cpu_threads
            opts.inter_op_num_threads = 1
            opts.enable_cpu_mem_arena = False
            opts.add_session_config_entry("session.intra_op.allow_spinning", "0")
            opts.add_session_config_entry("session.inter_op.allow_spinning", "0")
            if self.profile_dir:
                opts.enable_profiling = True
                opts.profile_file_prefix = str(self.profile_dir / name)
            providers = ["CPUExecutionProvider"]
            if budget.device == "cuda":
                providers.insert(0, ("CUDAExecutionProvider", {
                    "device_id": budget.device_id,
                    "gpu_mem_limit": budget.gpu_memory_mb * 1024**2,
                    "arena_extend_strategy": "kSameAsRequested",
                    "cudnn_conv_algo_search": "HEURISTIC",
                    "cudnn_conv_use_max_workspace": "0",
                    "use_tf32": "0", "do_copy_in_default_stream": "1",
                }))
            session = ort.InferenceSession(entry["path"], sess_options=opts, providers=providers)
            require_provider(session, budget.device)
            self.sessions[name] = session
        if "character" not in self.sessions["Rec"].get_modelmeta().custom_metadata_map:
            raise ValueError("retained recognition model lacks its character dictionary; downloads are forbidden")
        from rapidocr import RapidOCR
        from omegaconf import flag_override
        sessions = self.sessions

        class RetainedSessionsOCR(RapidOCR):
            def _load_config(self, config_path, params):
                cfg = super()._load_config(config_path, params)
                with flag_override(cfg, "allow_objects", True):
                    for name, session in sessions.items():
                        cfg[name].session = session
                return cfg

        self.engine = RetainedSessionsOCR(params=_parameters(config))
        self.process_budget = dict(
            cpu_affinity=sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
            nice=os.getpriority(os.PRIO_PROCESS, 0) if hasattr(os, "getpriority") else None,
            opencv_threads=cv2.getNumThreads(), disable_thp=budget.disable_thp,
            native_thread_env={key: os.environ[key] for key in NATIVE_THREAD_VARIABLES})

    def __call__(self, image):
        if self.closed or os.getpid() != self.owner_pid:
            raise RuntimeError("OCR session must be used by its live owning process")
        for session in self.sessions.values():
            require_provider(session, self.budget.device)
        started = time.perf_counter()
        result = self.engine(image)
        self.last_call = dict(wall_s=time.perf_counter() - started,
                              stages_s=[float(x) if x is not None else None for x in result.elapse_list],
                              device=self.budget.device)
        self.calls += 1
        self.elapsed_s += self.last_call["wall_s"]
        return result

    def finish_profiling(self):
        profiles = {}
        if self.profile_dir:
            for name, session in self.sessions.items():
                profiles[name] = summarize_profile(session.end_profiling())
            self.profile_dir = None
            if self.budget.device == "cuda" and self.calls:
                for name, profile in profiles.items():
                    if not profile["node_events"].get("CUDAExecutionProvider") or profile["cpu_neural_compute"]:
                        raise RuntimeError(f"{name}: GPU neural execution not established by profile")
        return dict(runtime=self.runtime, owner_pid=self.owner_pid,
                    initialization_s=self.initialization_s, pages_inferred=self.calls,
                    inference_wall_s=self.elapsed_s,
                    process_budget=self.process_budget,
                    providers={k: s.get_providers() for k, s in self.sessions.items()}, profiles=profiles)

    def close(self):
        self.closed = True
        self.sessions.clear()
        self.engine = None
        if self.lock is not None:
            self.lock.close()
            self.lock = None


@cache
def rapidocr_engine(config_path, profile_dir=None):
    config, _ = rapidocr_runtime(config_path)
    if "execution" in config:
        return RapidOCRRunner(config_path, profile_dir=profile_dir)
    if profile_dir is not None:
        raise ValueError("execution profiling requires a bounded OCR execution configuration")
    from rapidocr import RapidOCR
    return RapidOCR(params=_parameters(config))
