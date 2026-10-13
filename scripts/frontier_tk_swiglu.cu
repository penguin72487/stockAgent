// Diagnostic TK tile implementation; training admission still needs a fold.
#include <torch/extension.h>
#include <ATen/cuda/CUDAContext.h>
#include <c10/cuda/CUDAGuard.h>
#include <c10/cuda/CUDAException.h>
#include "kittens.cuh"
using namespace kittens;
using Layout = gl<float, -1, -1, -1, -1>;

template<bool Backward>
__global__ void swiglu_tile(Layout a, Layout b, Layout g, Layout out_a, Layout out_b) {
    rt_fl<16, 32> x, y, s, t, result;
    warp::load(x, a, {0, 0, int(blockIdx.x), int(blockIdx.y)});
    warp::load(y, b, {0, 0, int(blockIdx.x), int(blockIdx.y)});
    warp::mul(s, x, -1.f);
    warp::exp(s, s);
    warp::add(s, s, 1.f);
    warp::mul(t, x, 0.f);
    warp::add(t, t, 1.f);
    warp::div(s, t, s);
    if constexpr (Backward) {
        rt_fl<16, 32> grad;
        warp::load(grad, g, {0, 0, int(blockIdx.x), int(blockIdx.y)});
        warp::mul(t, s, -1.f);
        warp::add(t, t, 1.f);
        warp::mul(t, t, x);
        warp::add(t, t, 1.f);
        warp::mul(result, s, t);
        warp::mul(result, result, y);
        warp::mul(result, result, grad);
        warp::store(out_a, result, {0, 0, int(blockIdx.x), int(blockIdx.y)});
        warp::mul(result, x, s);
        warp::mul(result, result, grad);
        warp::store(out_b, result, {0, 0, int(blockIdx.x), int(blockIdx.y)});
    } else {
        warp::mul(result, x, s);
        warp::mul(result, result, y);
        warp::store(out_a, result, {0, 0, int(blockIdx.x), int(blockIdx.y)});
    }
}

std::vector<at::Tensor> run(at::Tensor a, at::Tensor b, at::Tensor g, bool backward) {
    TORCH_CHECK(a.is_cuda() && b.device() == a.device(), "CUDA tensors required");
    TORCH_CHECK(a.scalar_type() == at::kFloat && b.scalar_type() == at::kFloat, "FP32 tile inputs required");
    TORCH_CHECK(a.is_contiguous() && b.is_contiguous() && a.sizes() == b.sizes(), "matching contiguous inputs required");
    TORCH_CHECK(a.dim() == 2 && a.size(0) % 16 == 0 && a.size(1) % 32 == 0, "tile-aligned diagnostic required");
    c10::cuda::CUDAGuard guard(a.device());
    auto da = at::empty_like(a), db = at::empty_like(b);
    Layout la{a.data_ptr<float>(), 1, 1, size_t(a.size(0)), size_t(a.size(1))};
    Layout lb{b.data_ptr<float>(), 1, 1, size_t(b.size(0)), size_t(b.size(1))};
    Layout lg{g.data_ptr<float>(), 1, 1, size_t(a.size(0)), size_t(a.size(1))};
    Layout lout{da.data_ptr<float>(), 1, 1, size_t(a.size(0)), size_t(a.size(1))};
    Layout ldout{db.data_ptr<float>(), 1, 1, size_t(a.size(0)), size_t(a.size(1))};
    dim3 grid(a.size(0) / 16, a.size(1) / 32);
    auto stream = at::cuda::getCurrentCUDAStream();
    if (backward) swiglu_tile<true><<<grid, 32, 0, stream>>>(la, lb, lg, lout, ldout);
    else swiglu_tile<false><<<grid, 32, 0, stream>>>(la, lb, lg, lout, ldout);
    C10_CUDA_KERNEL_LAUNCH_CHECK();
    return {da, db};
}
PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) { m.def("run", &run); }
