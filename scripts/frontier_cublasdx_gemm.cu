// Diagnostic cuBLASDx 0.4.1 tile GEMM, compatible with the selected CUDA 12 SDK.
#include <torch/extension.h>
#include <ATen/cuda/CUDAContext.h>
#include <c10/cuda/CUDAGuard.h>
#include <c10/cuda/CUDAException.h>
#include <cuda_bf16.h>
#include <cublasdx.hpp>
using BLAS = decltype(cublasdx::Size<32, 32, 32>() +
    cublasdx::Precision<__nv_bfloat16, __nv_bfloat16, float>() +
    cublasdx::Type<cublasdx::type::real>() + cublasdx::Function<cublasdx::function::MM>() +
    cublasdx::Arrangement<cublasdx::row_major, cublasdx::row_major, cublasdx::row_major>() +
    cublasdx::Block() + cublasdx::BlockDim<128>() + cublasdx::SM<1200>());

__global__ void tiled_mm(const __nv_bfloat16* a, const __nv_bfloat16* b,
                        __nv_bfloat16* out, int m, int n, int k) {
    extern __shared__ __align__(16) char storage[];
    auto [sa, sb, sc] = cublasdx::slice_shared_memory<BLAS>(storage);
    auto ta = cublasdx::make_tensor(sa, BLAS::get_layout_smem_a());
    auto tb = cublasdx::make_tensor(sb, BLAS::get_layout_smem_b());
    auto tc = cublasdx::make_tensor(sc, BLAS::get_layout_smem_c());
    for (int i = threadIdx.x; i < 1024; i += blockDim.x) tc(i / 32, i % 32) = 0.f;
    __syncthreads();
    for (int offset = 0; offset < k; offset += 32) {
        for (int i = threadIdx.x; i < 1024; i += blockDim.x) {
            ta(i / 32, i % 32) = a[(blockIdx.x * 32 + i / 32) * k + offset + i % 32];
            tb(i / 32, i % 32) = b[(offset + i / 32) * n + blockIdx.y * 32 + i % 32];
        }
        __syncthreads();
        BLAS().execute(1.f, ta, tb, 1.f, tc);
        __syncthreads();
    }
    for (int i = threadIdx.x; i < 1024; i += blockDim.x)
        out[(blockIdx.x * 32 + i / 32) * n + blockIdx.y * 32 + i % 32] = __float2bfloat16(tc(i / 32, i % 32));
}
at::Tensor mm(at::Tensor a, at::Tensor b) {
    TORCH_CHECK(a.is_cuda() && b.device() == a.device(), "CUDA tensors required");
    TORCH_CHECK(a.scalar_type() == at::kBFloat16 && b.scalar_type() == at::kBFloat16, "BF16 inputs required");
    TORCH_CHECK(a.is_contiguous() && b.is_contiguous() && a.dim() == 2 && b.dim() == 2, "contiguous matrices required");
    TORCH_CHECK(a.size(1) == b.size(0) && a.size(0) % 32 == 0 && a.size(1) % 32 == 0 && b.size(1) % 32 == 0, "32-aligned tile diagnostic required");
    c10::cuda::CUDAGuard guard(a.device());
    auto out = at::empty({a.size(0), b.size(1)}, a.options());
    tiled_mm<<<dim3(a.size(0) / 32, b.size(1) / 32), 128,
        cublasdx::get_shared_storage_size<BLAS>(), at::cuda::getCurrentCUDAStream()>>>(
        reinterpret_cast<__nv_bfloat16*>(a.data_ptr<at::BFloat16>()),
        reinterpret_cast<__nv_bfloat16*>(b.data_ptr<at::BFloat16>()),
        reinterpret_cast<__nv_bfloat16*>(out.data_ptr<at::BFloat16>()), a.size(0), b.size(1), a.size(1));
    C10_CUDA_KERNEL_LAUNCH_CHECK();
    return out;
}
PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) { m.def("mm", &mm); }
