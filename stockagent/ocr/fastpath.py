"""Exact OCR data movement optimizations, without changing retained weights."""
from __future__ import annotations

from dataclasses import dataclass
import time


@dataclass(frozen=True)
class Optimizations:
    det_normalize_lut: bool = False
    gpu_det_normalize: bool = False
    direct_pixels: bool = False
    compact_ctc: bool = False
    memory_pattern: bool = True
    arena_shrink: bool = False
    arena_shrink_per_page: bool = False
    conv_workspace: bool = False
    use_tf32: bool = False
    conv_search: str = "HEURISTIC"
    rec_batch_width_budget: int = 0

    def __post_init__(self):
        for key in self.__dataclass_fields__:
            if key not in {'conv_search', 'rec_batch_width_budget'} and type(getattr(self, key)) is not bool:
                raise ValueError(f"OCR optimization {key} must be boolean")
        if self.conv_search not in {'HEURISTIC', 'EXHAUSTIVE', 'DEFAULT'}:
            raise ValueError('invalid cuDNN convolution search')
        if self.det_normalize_lut and self.gpu_det_normalize:
            raise ValueError('select either CPU or GPU detection normalization')
        if type(self.rec_batch_width_budget) is not int or not 0 <= self.rec_batch_width_budget <= 65536:
            raise ValueError('invalid recognition batch width budget')
        if self.rec_batch_width_budget and not self.compact_ctc:
            raise ValueError('bounded recognition microbatches require compact CTC')
        if self.arena_shrink_per_page and not self.arena_shrink:
            raise ValueError('page arena reclamation requires arena_shrink')


class ExactDetPreprocess:
    """Evaluate the original normalization once for every uint8/channel value.

    RapidOCR promotes each full page to float64 when subtracting its mean.
    This LUT retains the final float32 bits, original resize and CHW layout,
    avoiding the full-page float64 temporaries. Non-uint8 inputs use the native
    implementation; no clipping, rounding or changed resize is introduced.
    """

    def __init__(self, native):
        import numpy as np
        self.native = native
        values = np.repeat(np.arange(256, dtype=np.uint8)[:, None, None], 3, axis=2)
        self.lut = native.normalize(values).astype(np.float32)

    def __call__(self, image):
        import cv2
        import numpy as np
        if image.dtype != np.uint8 or image.ndim != 3 or image.shape[2] != 3:
            return self.native(image)
        resized = self.native.resize(image)
        if resized is None:
            return None
        return np.ascontiguousarray(cv2.LUT(resized, self.lut).transpose(2, 0, 1)[None])


class CompactCTC:
    """Native CTC decoder interface backed by two reduced GPU outputs."""

    def __init__(self, indices, scores, invalid):
        import numpy as np
        if (indices.ndim != 2 or indices.shape != scores.shape
                or indices.dtype != np.int64 or scores.dtype != np.float32
                or bool(np.any(invalid)) or not np.isfinite(scores).all()):
            raise ValueError('invalid compact CTC output')
        self.indices, self.scores = indices, scores

    def argmax(self, axis):
        if axis != 2:
            raise ValueError('compact CTC only supports character-axis decoding')
        return self.indices

    def max(self, axis):
        if axis != 2:
            raise ValueError('compact CTC only supports character-axis decoding')
        return self.scores


class GPUDetPreprocess:
    """Keep the native spatial resize; send uint8 NHWC to the exact GPU LUT."""

    def __init__(self, native, expected_lut_sha256):
        import hashlib
        self.native = native
        lut = ExactDetPreprocess(native).lut.reshape(256,3).T.copy().reshape(-1)
        if hashlib.sha256(lut.tobytes()).hexdigest() != expected_lut_sha256:
            raise ValueError('GPU detector LUT differs from native normalization')

    def __call__(self, image):
        import numpy as np
        if image.dtype != np.uint8 or image.ndim != 3 or image.shape[2] != 3:
            raise ValueError('GPU detector normalization requires uint8 BGR pixels')
        resized = self.native.resize(image)
        return None if resized is None else np.ascontiguousarray(resized[None])


class TimedCall:
    def __init__(self, operation, label, events):
        self.operation, self.label, self.events = operation, label, events

    def __call__(self, *args, **kwargs):
        started = time.perf_counter()
        result = self.operation(*args, **kwargs)
        self.events.append(dict(stage=self.label, wall_s=time.perf_counter()-started))
        return result


class InferenceCall:
    """Use the retained session, collect actual shapes/bytes, optionally reduce CTC."""

    def __init__(self, session, name, events, *, compact=False, run_options=None, batch_width_budget=0,
                 shrink_per_page=False):
        self.session, self.name, self.events = session, name, events
        self.compact, self.run_options = compact, run_options
        self.batch_width_budget = batch_width_budget
        self.shrink_per_page = shrink_per_page
        self.remaining_calls = None
        self.inputs = [i.name for i in session.get_inputs()]
        self.outputs = [o.name for o in session.get_outputs()]
        if len(self.inputs) != 1:
            raise ValueError('OCR models must have exactly one input')
        if compact and self.outputs != ['ctc_indices', 'ctc_scores', 'ctc_invalid']:
            raise ValueError('invalid compact CTC model outputs')

    def __call__(self, tensor):
        shrink = True
        if self.shrink_per_page:
            if not self.remaining_calls:
                raise RuntimeError('missing OCR component batch boundary')
            self.remaining_calls -= 1
            shrink = self.remaining_calls == 0
        if self.batch_width_budget and tensor.shape[0] > 1:
            # Keep each line's ORIGINAL padded width. Changing RapidOCR's batch
            # setting would also change padding and can change recognition.
            batch = max(1, self.batch_width_budget // tensor.shape[-1])
            if batch < tensor.shape[0]:
                import numpy as np
                outputs = [self._run(tensor[i:i+batch], shrink=shrink and i+batch>=tensor.shape[0])
                           for i in range(0, tensor.shape[0], batch)]
                return CompactCTC(np.concatenate([o.indices for o in outputs]),
                                  np.concatenate([o.scores for o in outputs]), np.array(0))
        return self._run(tensor, shrink=shrink)

    def _run(self, tensor, *, shrink=True):
        started = time.perf_counter()
        options = self.run_options if shrink else None
        outputs = self.session.run(self.outputs, {self.inputs[0]: tensor}, options)
        event = dict(stage=self.name+'.network', wall_s=time.perf_counter()-started,
                     input_shape=list(tensor.shape), input_bytes=tensor.nbytes,
                     output_shapes=[list(o.shape) for o in outputs],
                     output_bytes=sum(o.nbytes for o in outputs), arena_reclaimed=options is not None)
        self.events.append(event)
        return CompactCTC(*outputs) if self.compact else outputs[0]


class ComponentBoundary:
    """Shrink only after this page's last model call, including microbatches."""

    def __init__(self, operation, adapter, batch_size, *, detector=False):
        self.operation, self.adapter, self.batch_size = operation, adapter, batch_size
        self.detector = detector

    def __call__(self, images, *args, **kwargs):
        count = 1 if self.detector or not isinstance(images, list) else len(images)
        self.adapter.remaining_calls = (count+self.batch_size-1)//self.batch_size
        return self.operation(images, *args, **kwargs)
