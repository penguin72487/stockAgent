// Benchmark-only C ABI, bulk borrowed arrays; no Python-object conversion.
use std::slice;

#[no_mangle]
pub unsafe extern "C" fn limits(price: *const f64, dates: *const i64, out: *mut f64,
    n: usize, tick_cutoff: i64, ratio_cutoff: i64, ratio: f64) {
    let (prices, dates, out) = (slice::from_raw_parts(price, n),
        slice::from_raw_parts(dates, n), slice::from_raw_parts_mut(out, n));
    for i in 0..n {
        let r = if dates[i] < ratio_cutoff {
            if ratio > 1.0 { 1.07 } else { 0.93 }
        } else if ratio > 1.0 { 1.10 } else { 0.90 };
        let theoretical = prices[i] * r;
        if !theoretical.is_finite() || theoretical <= 0.0 {
            out[i] = f64::NAN;
            continue;
        }
        let old = dates[i] < tick_cutoff;
        let tick = if theoretical < if old { 5.0 } else { 10.0 } { 0.01 }
            else if theoretical < if old { 15.0 } else { 50.0 } { 0.05 }
            else if theoretical < if old { 50.0 } else { 100.0 } { 0.1 }
            else if theoretical < if old { 150.0 } else { 500.0 } { 0.5 }
            else if theoretical < 1000.0 { 1.0 } else { 5.0 };
        let rounded = if ratio < 1.0 { (theoretical / tick - 1e-12).ceil() }
                      else { (theoretical / tick + 1e-12).floor() } * tick;
        out[i] = (rounded * 100.0 + 0.5).floor() / 100.0;
    }
}

#[no_mangle]
pub unsafe extern "C" fn moments(values: *const f64, means: *mut f64, stds: *mut f64,
    n: usize, window: usize) {
    let values = slice::from_raw_parts(values, n);
    let means = slice::from_raw_parts_mut(means, n);
    let stds = slice::from_raw_parts_mut(stds, n);
    let (mut sums, mut squares, mut counts) = (vec![0.0; n], vec![0.0; n], vec![0i64; n]);
    let (mut sum, mut square, mut count) = (0.0, 0.0, 0i64);
    for i in 0..n {
        let finite = values[i].is_finite();
        let v = if finite { values[i] } else { 0.0 };
        count += i64::from(finite);
        sum += v;
        square += v * v;
        counts[i] = count;
        sums[i] = sum;
        squares[i] = square;
    }
    for i in 0..n {
        let c = counts[i] - if i >= window { counts[i - window] } else { 0 };
        let s = sums[i] - if i >= window { sums[i - window] } else { 0.0 };
        let q = squares[i] - if i >= window { squares[i - window] } else { 0.0 };
        let mean = if c > 0 { s / c as f64 } else { 0.0 };
        let second = if c > 0 { q / c as f64 } else { 0.0 };
        let variance = second - mean * mean;
        means[i] = mean;
        stds[i] = if variance > 0.0 { variance.sqrt() } else { 0.0 };
    }
}
