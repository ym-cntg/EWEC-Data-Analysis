"""Error metrics (pandas). Scored on daytime-window rows with a usable label.

- mape_pct:  mean |pred - actual| / actual × 100, only where actual > `min_actual_mw` (Phase 1 rule: 150 MW)
- nmae_pct:  mean |pred - actual| / capacity × 100
- rmse_pct:  sqrt(mean (pred - actual)²) / capacity × 100
- bias_pct:  mean (pred - actual) / capacity × 100   (+ = over-forecast)
"""
import numpy as np
import pandas as pd


def score(df: pd.DataFrame, pred: str, actual: str, cap: str, min_actual_mw: float = 150.0) -> dict:
    d = df[[pred, actual, cap]].dropna()
    if d.empty:
        return {"n": 0, "n_mape": 0, "mape_pct": np.nan, "nmae_pct": np.nan, "rmse_pct": np.nan, "bias_pct": np.nan}
    err = d[pred] - d[actual]
    big = d[actual] > min_actual_mw
    return {
        "n": int(len(d)),
        "n_mape": int(big.sum()),
        "mape_pct": float((err[big].abs() / d.loc[big, actual]).mean() * 100) if big.any() else np.nan,
        "nmae_pct": float((err.abs() / d[cap]).mean() * 100),
        "rmse_pct": float(np.sqrt((err ** 2).mean()) / d[cap].mean() * 100),
        "bias_pct": float((err / d[cap]).mean() * 100),
    }
