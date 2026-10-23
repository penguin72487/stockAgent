// Benchmark candidates only; canonical Python/Numba remains the semantic oracle.
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

extern "C" void limits(const double* price, const int64_t* dates, double* out,
                       size_t n, int64_t tick_cutoff, int64_t ratio_cutoff,
                       double ratio) {
    for (size_t i = 0; i < n; ++i) {
        const bool up = ratio > 1.0;
        const double r = dates[i] < ratio_cutoff ? (up ? 1.07 : .93) : (up ? 1.10 : .90);
        const double theoretical = price[i] * r;
        if (!std::isfinite(theoretical) || theoretical <= 0.0) {
            out[i] = std::numeric_limits<double>::quiet_NaN();
            continue;
        }
        const bool old = dates[i] < tick_cutoff;
        const double tick = theoretical < (old ? 5. : 10.) ? .01 :
                            theoretical < (old ? 15. : 50.) ? .05 :
                            theoretical < (old ? 50. : 100.) ? .1 :
                            theoretical < (old ? 150. : 500.) ? .5 :
                            theoretical < 1000. ? 1. : 5.;
        const double rounded = (ratio < 1.0 ? std::ceil(theoretical / tick - 1e-12) :
                                std::floor(theoretical / tick + 1e-12)) * tick;
        out[i] = std::floor(rounded * 100. + .5) / 100.;
    }
}

extern "C" void moments(const double* values, double* means, double* stds,
                        size_t n, size_t window) {
    // Same prefix order/subtractions as tw_all_futures._rolling_moments.
    std::vector<double> sums(n), squares(n);
    std::vector<int64_t> counts(n);
    double sum = 0., square = 0.;
    int64_t count = 0;
    for (size_t i = 0; i < n; ++i) {
        const double v = std::isfinite(values[i]) ? values[i] : 0.;
        count += std::isfinite(values[i]);
        sum += v;
        square += v * v;
        counts[i] = count;
        sums[i] = sum;
        squares[i] = square;
    }
    for (size_t i = 0; i < n; ++i) {
        const int64_t c = counts[i] - (i >= window ? counts[i - window] : 0);
        const double s = sums[i] - (i >= window ? sums[i - window] : 0.);
        const double q = squares[i] - (i >= window ? squares[i - window] : 0.);
        const double mean = c ? s / static_cast<double>(c) : 0.;
        const double second = c ? q / static_cast<double>(c) : 0.;
        const double variance = second - mean * mean;
        means[i] = mean;
        stds[i] = std::sqrt(variance > 0. ? variance : 0.);
    }
}
