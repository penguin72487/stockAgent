# Scale-separated sparse cash output for fold 11

The fold-11 Taiwan stock day-trade policy has 2,755 candidates, an exact
whole-lot/FIFO account, and a TWD 10M starting account in the existing artifact.
A requested gross weight is not a fill. At TWD 50 per share, one 1,000-share
board lot has TWD 50,000 notional, or 0.005 of that starting capital before
fees. Spreading unit gross uniformly over 2,755 stocks requests only
0.000363 per stock. The output must let the model concentrate its requests;
the execution ledger still owns lot, cash, fee, shorting, settlement, capacity,
and fill rules.

The completed v4 `score_entmax_cash` fold-11 test artifact has 172 sessions:
median requested gross 0.437, median largest absolute stock weight 0.0811,
median effective count 11.5, and nonzero turnover on 172 days. The v5
`projection_l1` fold-11 artifact has median requested gross 0.0631, median
largest weight 0.000154, median effective count 1,344, and nonzero turnover on
3 days. These are observations about those trained checkpoints, not evidence
that the new mode improves returns. Effective count here is
`(sum(abs(w)))**2 / sum(w**2)`.

## Construction

For each row, let `L` be the causal legal stock set and `z_i` the model's
existing signed score. A disallowed short score is clipped to zero **before**
normalization. On the remaining legal scores, define

```
a_i = abs(z_i)
r   = sqrt(sum(i in L, a_i**2) / |L|)
p   = entmax_1.5(a / r) over L
q   = sum(i in L, p_i * a_i)
w_i = p_i * z_i / (1 + q)
```

The all-zero or empty legal set returns all cash. The implementation computes
`r` after scaling by the maximum magnitude and floors the RMS before the
square root at FP32 machine epsilon. That avoids both overflow and an infinite
zero-score derivative. The final requested weights remain FP32 under BF16 AMP.
No cash token, cash head, new trainable parameter, fixed top-K, or exposure
target is introduced.

The RMS is the length of the score vector per legal stock: `r = ||a||_2 / sqrt(|L|)`.
It separates the score vector's radial size from its angular pattern. For any
nonzero row, `max(a_i / r) <= sqrt(|L|)`, so an isolated signal can dominate a
wide universe without the normalized selector seeing an unbounded value.
Using the mean absolute score instead would allow a maximum of `|L|` and can
make selection unnecessarily sharp in the presence of one outlier.

Because `p` is a probability vector and legal `a_i=abs(z_i)`, requested gross
is exactly `q/(1+q)` and requested residual cash is `1/(1+q)` before floating
point rounding. For a common positive score multiplier `c`, `r(cz)=c*r(z)`,
so `p(cz)=p(z)` outside the numerical zero floor, while `q(cz)=c*q(z)`.
The model can therefore vary gross and cash by changing common score scale
without changing support and relative stock allocation. The prior
`score_entmax_global_cash` gives entmax the unnormalized `a`; common scaling
changes both concentration and gross. This separation is the specific new
output contract, not a claimed new entmax algorithm.

There are two unavoidable limits. Equal stock scores give equal weights in a
permutation-equivariant map; at this universe width those requests may remain
below the lot threshold. An exactly excluded stock has zero direct gradient
through its own weight and may have little or no indirect gradient; RMS
normalization and the shared backbone can couple scores, but that is not an
exploration guarantee. Exact forward accounting, early stopping,
and held-out net wealth remain the acceptance tests for usefulness.

## Implementation and verification

`portfolio_output_mode: score_entmax_scale_separated_cash` is wired through
the Financial Transformer/Transformer Base output, gradient-boosted variant,
score reallocation for explanations, flat-cash pretrained fallback, and a
versioned checkpoint manifest. Its separate OFAT config is
`configs/deployments/tw_public_all_observed_v8_ofat_fold11_data_cpu_cache_score_entmax_scale_separated_cash_10240_v9.yaml`.
It inherits v7's exact data release, model, DDP batch 16, CPU tensor cache,
validation every epoch, and 0.1 no-improvement early-stop ratio. Despite the
historical `10240` name, v6 sets an effective **1,024-epoch** ceiling. Only the
output mode and artifact root change relative to v7.

Deterministic mapping checks on a 2,755-stock row with one 0.1 score and all
others zero produce 0.090909 on that stock in the new mode versus 0.000476 in
v7. This crosses the illustrative 0.005 one-lot request threshold; it does
not establish a fill or profit. Tests cover scale separation, masks, no-short,
all-zero gradients, extreme finite scores, FP16/BF16 output precision, model
parameter equality, explanation parity, checkpoint isolation, and inherited
configuration. A CUDA BF16 `torch.compile(mode="reduce-overhead")` batch-16 by
2,755 forward/backward smoke produced finite FP32 weights and finite nonzero
gradients. A compiled BF16 all-zero row remained exactly flat with a finite
`1/2755` active-score gradient; an all-masked row had zero gradient. A full
fold-11 lifecycle subsequently completed for v9; its held-out performance and
failure modes are reported in
[the 2026-09-22 diagnosis](tw_public_fold11_v9_performance_diagnosis_2026-09-22.md).

The v9 `train.py --check-data-only --start-fold 11 --max-folds 1` preflight
exited successfully without constructing a model or checkpoint. It accepted
the pinned panel cache with 428 features, 3,100 physical-carry sessions,
2,755 symbols, `source_gap_symbol_days=0`, and a full-SHA-256 run-verification
receipt. This establishes source usability for this configuration at the
check time, not the eventual trained strategy's performance.

The historical launch command was:

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
export CUDA_VISIBLE_DEVICES=0,1 PYTHONUNBUFFERED=1
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_public_all_observed_v8_ofat_fold11_data_cpu_cache_score_entmax_scale_separated_cash_10240_v9.yaml \
  --start-fold 11 --max-folds 1 --resume --profile-timing
```

Research basis: entmax supplies the sparse simplex mapping, as developed in
[Peters et al. (ACL 2019)](https://aclanthology.org/P19-1146/). The separate
scale normalization and signed cash-budget map above are project-specific
derivations from the exact-account output constraints. Fixed trading costs and
other discrete execution features cannot generally be reduced to a convex
continuous portfolio map, as discussed by
[Lobo, Fazel, and Boyd (2007)](https://web.stanford.edu/~boyd/papers/portfolio.html);
the existing exact ledger remains the forward authority here.
