# TW minute explicit cash asset contract

The active minute model emits one raw score per stock plus one contextual cash
score. Cash is the final asset column:

```text
score_logits_with_cash = [stock_score_1, ..., stock_score_S, cash_score]
```

The cash token receives the masked mean market embedding and is evaluated by
the same shared score head as the stock embeddings. Because borrowing cash is
not part of this strategy, its unconstrained score is mapped to a positive
allocation score with `softplus`.

For signed stock allocation scores `a_i` and positive cash score `c`:

```text
D              = sum_i(abs(a_i)) + c
stock_weight_i = a_i / D
cash_weight    = c / D
```

Therefore every model row satisfies:

```text
sum_i(abs(stock_weight_i)) + cash_weight = 1
```

This is different from executor residual cash. Eligibility, volume capacity,
available capital, and the proximal no-trade rule are applied after the model
allocation and may leave additional cash. Reports keep the two concepts
separate:

- `mean_model_requested_cash_weight`: explicit model cash action before execution constraints.
- realised holdings/exposure: actual filled inventory after execution constraints.

The active config is `configs/markets/tw_minute_dual_5090.yaml`, uses
`portfolio_output_mode: cash_l1`, and writes to a fresh checkpoint root because
the added cash token is not optimizer-compatible with older runs.

Run with:

```bash
bash scripts/run_tw_minute_dual_5090.sh --start-fold 1
```
