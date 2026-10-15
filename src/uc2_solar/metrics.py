"""Error metrics (pandas). Scored on daytime rows with a usable label.

- nmae_pct:  mean |pred - actual| / capacity × 100
- mape_pct:  mean |pred - actual| / actual × 100, only where actual > `min_share` × capacity
- rmse_pct:  sqrt(mean (pred - actual)²) / capacity × 100
- bias_pct:  mean (pred - actual) / capacity × 100   (+ = over-forecast)
"""
import numpy as np
import pandas as pd


def score(df: pd.DataFrame, pred: str, actual: str, cap: str, min_share: float = 0.10) -> dict:
    d = df[[pred, actual, cap]].dropna()
    if d.empty:
        return {"n": 0, "nmae_pct": np.nan, "mape_pct": np.nan, "rmse_pct": np.nan, "bias_pct": np.nan}
    err = d[pred] - d[actual]
    big = d[actual] > min_share * d[cap]
    return {
        "n": int(len(d)),
        "nmae_pct": float((err.abs() / d[cap]).mean() * 100),
        "mape_pct": float((err[big].abs() / d.loc[big, actual]).mean() * 100) if big.any() else np.nan,
        "rmse_pct": float(np.sqrt((err ** 2).mean()) / d[cap].mean() * 100),
        "bias_pct": float((err / d[cap]).mean() * 100),
    }
