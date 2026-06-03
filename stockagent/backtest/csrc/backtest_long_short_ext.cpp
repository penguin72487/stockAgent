#include <torch/extension.h>

#include <vector>

namespace {

inline torch::Tensor _to_bool(const torch::Tensor& t) {
    if (t.dtype() == torch::kBool) {
        return t;
    }
    return t.to(torch::kBool);
}

}  // namespace

std::vector<torch::Tensor> long_short_forward(
    const torch::Tensor& weights,
    const torch::Tensor& future_returns,
    const torch::Tensor& tradable_mask,
    const torch::Tensor& can_buy_mask,
    const torch::Tensor& can_sell_mask,
    double buy_fee_rate,
    double sell_fee_rate,
    double max_turnover_ratio,
    double gross_budget) {
    TORCH_CHECK(weights.dim() == 2, "weights must be [T, S]");
    TORCH_CHECK(future_returns.dim() == 2, "future_returns must be [T, S]");
    TORCH_CHECK(weights.sizes() == future_returns.sizes(), "weights and future_returns size mismatch");
    TORCH_CHECK(tradable_mask.sizes() == weights.sizes(), "tradable_mask size mismatch");
    TORCH_CHECK(can_buy_mask.sizes() == weights.sizes(), "can_buy_mask size mismatch");
    TORCH_CHECK(can_sell_mask.sizes() == weights.sizes(), "can_sell_mask size mismatch");
    TORCH_CHECK(weights.is_cuda(), "long_short_forward expects CUDA tensor inputs");

    auto tradable_b = _to_bool(tradable_mask);
    auto can_buy_b = _to_bool(can_buy_mask);
    auto can_sell_b = _to_bool(can_sell_mask);

    auto options = weights.options();
    const auto t_len = weights.size(0);
    const auto n_symbols = weights.size(1);

    auto strategy_returns = torch::empty({t_len}, options);
    auto turnovers = torch::empty({t_len}, options);
    auto weights_history = torch::empty({t_len, n_symbols}, options);

    auto allowed_mask = torch::zeros({t_len, n_symbols}, options);
    auto turnover_scales = torch::ones({t_len}, options);
    auto gross_scales = torch::ones({t_len}, options);
    auto delta_sign = torch::zeros({t_len, n_symbols}, options);

    auto prev = torch::zeros({n_symbols}, options);

    for (int64_t t = 0; t < t_len; ++t) {
        auto target_t = torch::where(tradable_b[t], weights[t], prev);
        auto delta_raw = target_t - prev;

        auto pos = delta_raw > 0;
        auto neg = delta_raw < 0;
        auto allow = (pos.logical_and(can_buy_b[t])) | (neg.logical_and(can_sell_b[t]));
        auto allow_f = allow.to(weights.scalar_type());

        auto masked_delta = delta_raw * allow_f;

        auto turn_scale = torch::ones({}, options);
        if (max_turnover_ratio > 0.0) {
            auto turnover_raw = masked_delta.abs().sum();
            auto cap = torch::full({}, max_turnover_ratio, options);
            turn_scale = torch::minimum(torch::ones_like(turnover_raw), cap / turnover_raw.clamp_min(1e-12));
        }

        auto pre = prev + masked_delta * turn_scale;
        auto gross_raw = pre.abs().sum();
        auto gross_cap = torch::full({}, gross_budget, options);
        auto gross_scale = torch::minimum(torch::ones_like(gross_raw), gross_cap / gross_raw.clamp_min(1e-12));
        auto next = pre * gross_scale;

        auto delta_final = next - prev;
        auto buy_turn = delta_final.clamp_min(0.0).sum();
        auto sell_turn = (-delta_final).clamp_min(0.0).sum();
        auto turnover = buy_turn + sell_turn;
        auto gross_ret = (next * future_returns[t]).sum();
        auto strategy_ret = gross_ret - buy_turn * buy_fee_rate - sell_turn * sell_fee_rate;

        strategy_returns[t].copy_(strategy_ret);
        turnovers[t].copy_(turnover);
        weights_history[t].copy_(next);

        allowed_mask[t].copy_(allow_f);
        turnover_scales[t].copy_(turn_scale);
        gross_scales[t].copy_(gross_scale);
        delta_sign[t].copy_(torch::sign(delta_final));

        prev = next;
    }

    return {
        strategy_returns,
        turnovers,
        weights_history,
        allowed_mask,
        turnover_scales,
        gross_scales,
        delta_sign,
    };
}

torch::Tensor long_short_backward(
    const torch::Tensor& grad_strategy_returns,
    const torch::Tensor& grad_turnovers,
    const torch::Tensor& future_returns,
    const torch::Tensor& allowed_mask,
    const torch::Tensor& turnover_scales,
    const torch::Tensor& gross_scales,
    const torch::Tensor& delta_sign) {
    TORCH_CHECK(grad_strategy_returns.dim() == 1, "grad_strategy_returns must be [T]");
    TORCH_CHECK(grad_turnovers.dim() == 1, "grad_turnovers must be [T]");
    TORCH_CHECK(future_returns.dim() == 2, "future_returns must be [T, S]");
    TORCH_CHECK(allowed_mask.dim() == 2, "allowed_mask must be [T, S]");
    TORCH_CHECK(delta_sign.dim() == 2, "delta_sign must be [T, S]");
    TORCH_CHECK(future_returns.is_cuda(), "long_short_backward expects CUDA tensor inputs");

    const auto t_len = future_returns.size(0);
    const auto n_symbols = future_returns.size(1);

    auto options = future_returns.options();
    auto grad_weights = torch::zeros({t_len, n_symbols}, options);
    auto grad_next = torch::zeros({n_symbols}, options);

    for (int64_t t = t_len - 1; t >= 0; --t) {
        auto grad_t = grad_next + grad_strategy_returns[t] * future_returns[t];

        auto grad_turn_component = grad_turnovers[t] * delta_sign[t];
        grad_t = grad_t + grad_turn_component;
        auto grad_prev_from_turn = -grad_turn_component;

        auto grad_pre = grad_t * gross_scales[t];
        auto grad_delta = grad_pre * turnover_scales[t];

        auto allow_t = allowed_mask[t];
        auto grad_target = grad_delta * allow_t;
        grad_weights[t].copy_(grad_target);

        auto grad_prev = grad_pre + grad_prev_from_turn - (grad_delta * allow_t);
        grad_next = grad_prev;
    }

    return grad_weights;
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("long_short_forward", &long_short_forward, "Long-short differentiable backtest forward (CUDA tensors)");
    m.def("long_short_backward", &long_short_backward, "Long-short differentiable backtest backward (CUDA tensors)");
}
