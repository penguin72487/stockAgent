# Temporal multi-basis representation

## First-principles contract

A basis transform of the same 32 observations does not add information. Its
possible value is an inductive bias: it can make local changes, low-frequency
shape, and periodic structure easier for the optimizer to isolate. Basis
coefficients are therefore ordinary input features, not a privileged model
branch.

The implementation runs inside the model on the already-causal `[B,L,S,D]`
window embeddings. It does not materialize rolling columns in parquet, does not
read beyond the current window endpoint, and preserves the panel-slab fast path.
Every fixed bank excludes the constant/DC vector, so static symbol/category
information is not mistaken for a time-varying signal.

## Implemented online-safe families

| Family | Mechanism | Role |
| --- | --- | --- |
| Raw time-domain | Existing model input and temporal attention | Main path, never removed |
| Learned representation | Candle Encoder plus temporal Transformer | Main learned representation |
| Haar wavelet | Orthonormal local differences from coarse to fine scales | Local regime changes and jumps |
| Stationary db2 / sym4 | Causal dilated wavelet filter banks without downsampling | Shift-tolerant multi-scale changes |
| db2 wavelet packet | Low/high filter compositions at multiple depths | Localized band structure |
| Walsh | Sequency-ordered binary sign patterns | Abrupt alternating regimes |
| Real Fourier | Low-frequency sine/cosine coefficients | Periodicity with phase |
| DCT-II | Low-frequency cosine coefficients | Smooth trend/shape under finite-window boundaries |
| DPSS/Slepian | Eigenvectors with maximal finite-window band concentration | Leakage-resistant spectral shape |
| Local cosine | Gaussian-windowed sine/cosine atoms | Time-localized frequency changes |
| Morlet | Localized oscillatory wavelets | Short-lived cycles and bursts |
| Exponential | Log-spaced decay kernels | Multi-horizon recency memory |
| Laguerre | Decaying orthogonal polynomial modes | Compact stable state-space memory |
| Difference | Multi-horizon first/second endpoint differences | Momentum, acceleration, and breaks |
| AR innovation | Latest value minus multi-horizon exponential predictors | Surprise relative to recent history |
| Cubic B-spline | Compact local smooth atoms | Local smooth shape without global polynomials |
| Legendre/Chebyshev | Orthogonal polynomial coefficients | Optional low-priority shape ablations |
| Learned dictionary | Trainable, row-normalized non-DC atoms initialized from DCT | Data-adaptive temporal shapelets |
| Eigenfactor/PCA role | Existing latent factors and market tokens | Cross-stock common-factor axis |

The ordered public list is `ONLINE_SAFE_TEMPORAL_BASIS_FAMILIES`. Deterministic
banks are built once at model construction, exported as fixed non-persistent
buffers, and never recomputed with a per-window decomposition. The learned bank
is a checkpointed parameter; its rows are re-centered and normalized during
forward. All other banks are non-DC and orthonormalized in float64 before being
stored as float32.

For basis matrices `B_f`, the complete mechanism is:

```text
c_f = B_f @ temporal_embeddings
input_features = concat(raw_stock_embedding, flatten(c_1), ..., flatten(c_F))
stock_embedding = Linear(input_features)
```

That is the entire basis integration. There is no family-specific MLP, learned
coefficient mixer, energy path, fusion network, gate, or residual addition.
Raw/learned and basis values are columns in one feature vector and the same
linear feature projection decides their weights. Detailed aux output exposes
the exact family coefficients, concatenated input features, and projected
output.

## Why these are online-safe

Here, online means live inference from the already-observed trailing 32 rows;
it does not mean continuously fitting the model after every quote. Every family
has a static coefficient count, deterministic initialization, no future rows,
no data-dependent rank, and no iterative solver in `forward`. The same feature
path therefore works through raw windows, `forward_from_panel`, and the contiguous
panel-slab compile path.

Exact rolling PCA/ICA is not duplicated on the temporal axis: PCA's useful
market meaning is cross-stock common variation, already modeled by the
configured latent-factor and market-token bottlenecks. A trainable temporal
dictionary covers data-adaptive within-stock shapes without a rolling SVD.

Exact SSA, EMD/HHT, VMD, rolling PCA/ICA, DMD, and Prony decompositions remain
outside the live training hot path. They require per-window SVD/eigendecomposition
or iterative/data-dependent mode extraction; ranks and mode identities can jump
between adjacent windows, and boundary behavior is especially fragile at only
32 observations. They should first be evaluated as offline,
receipt/fingerprint-backed ablations. If one shows stable walk-forward lift
after costs, its output can be distilled into the learned dictionary rather
than placing the solver in every model forward.

## Configuration

The compact starting experiment is `configs/markets/tw_public_multi_basis.yaml`:

```yaml
training:
  transformer_base_portfolio:
    temporal_basis_families: [haar, fourier, dct]
    temporal_basis_components: 8
```

An empty family list is the backward-compatible default and creates no new
parameters. Enabling basis features changes the model/config fingerprint and
must start under a fresh artifact root.

The exhaustive online experiment is
`configs/markets/tw_public_multi_basis_online_complete.yaml`. It enables all 18
families with four components each. This is intentionally a discovery/config
ablation baseline, not an assertion that all correlated banks should survive
into the final production model. Use coefficient diagnostics, projection
weights, family ablations, and walk-forward net returns to prune redundant
families after training.

## Engineering smoke benchmark

On the local RTX 5070 Ti with PyTorch 2.12.1, the latest same-process eager BF16
comparison measured:

| Branch | Live forward `[1,32,2304,131]` | Train fwd+bwd `[4,32,2304,131]` | Peak train allocation | Parameters |
| --- | ---: | ---: | ---: | ---: |
| Raw/learned only | 12.521 ms | 60.100 ms | 1594.1 MiB | 169,509 |
| Compact feature input, 3 x 8 | 15.524 ms | 60.718 ms | 1630.9 MiB | 195,141 |
| Complete feature input, 18 x 4 | 16.700 ms | 74.494 ms | 1631.9 MiB | 244,421 |

Values are medians across two order-reversed rounds; each round used 15 steady
forward repetitions or nine training repetitions after warmup, with aux
collection disabled. Relative to raw, the complete set adds about 4.18 ms to a
full-universe live forward and 24.0% to this model-only training step, while
adding about 37.8 MiB peak allocation. A CUDA BF16
`torch.compile(mode="reduce-overhead", fullgraph=True)` forward/backward smoke
also completed with finite output, input gradient, feature-projection gradient,
and learned-basis gradient. Cold compilation must be warmed and cached before
live service startup.

These are model-kernel engineering smokes, not compiled full-epoch benchmarks
and not evidence of strategy lift. Daily live use easily fits this latency; a
high-frequency runner should benchmark its own batch/symbol shape and normally
start from the compact family set.
