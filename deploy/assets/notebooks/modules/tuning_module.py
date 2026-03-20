# Fabric Notebook -- Module
# tuning_module.py -- Hyperparameter tuning via randomized grid search + time-series CV

import warnings
import itertools
import numpy as np
import pandas as pd
from typing import Callable

warnings.filterwarnings("ignore")


def time_series_cv_splits(n: int, n_splits: int = 3, min_train_size: int = 24):
    """Generate expanding-window CV indices for time series.
    Returns list of (train_indices, test_indices) tuples."""
    if n < min_train_size + n_splits:
        return [(list(range(int(n * 0.8))), list(range(int(n * 0.8), n)))]

    test_size = max(1, (n - min_train_size) // (n_splits + 1))
    splits = []
    for i in range(n_splits):
        test_start = min_train_size + i * test_size
        test_end = min(test_start + test_size, n)
        if test_start >= n:
            break
        splits.append((list(range(test_start)), list(range(test_start, test_end))))
    return splits if splits else [(list(range(int(n * 0.8))), list(range(int(n * 0.8), n)))]


def _sample_param_grid(param_grid: dict, n_iter: int, rng: np.random.RandomState) -> list:
    """Sample n_iter random combinations from parameter grid."""
    keys = list(param_grid.keys())
    all_combos = list(itertools.product(*[param_grid[k] for k in keys]))

    if len(all_combos) <= n_iter:
        return [dict(zip(keys, combo)) for combo in all_combos]

    indices = rng.choice(len(all_combos), size=n_iter, replace=False)
    return [dict(zip(keys, all_combos[i])) for i in indices]


def random_search_cv(series_values: np.ndarray, fit_and_predict_fn: Callable,
                     param_grid: dict, n_iter: int = 10, n_splits: int = 3,
                     metric: str = "rmse", seed: int = 42) -> dict:
    """Randomized search with time-series CV for a single series.

    fit_and_predict_fn(train_values, test_length, **params) -> np.ndarray of predictions
    Returns dict with 'best_params', 'best_score', 'all_results'.
    """
    rng = np.random.RandomState(seed)
    candidates = _sample_param_grid(param_grid, n_iter, rng)
    splits = time_series_cv_splits(len(series_values), n_splits)

    results = []
    for params in candidates:
        fold_scores = []
        for train_idx, test_idx in splits:
            train = series_values[train_idx]
            test = series_values[test_idx]
            try:
                preds = fit_and_predict_fn(train, len(test), **params)
                if preds is None or len(preds) == 0:
                    continue
                preds = np.array(preds[:len(test)])
                score = _compute_cv_metric(test, preds, metric)
                if score is not None and np.isfinite(score):
                    fold_scores.append(score)
            except Exception:
                continue

        avg_score = float(np.mean(fold_scores)) if fold_scores else float("inf")
        results.append({"params": params, "score": avg_score, "n_folds": len(fold_scores)})

    results.sort(key=lambda x: x["score"])
    best = results[0] if results else {"params": {}, "score": float("inf"), "n_folds": 0}
    return {"best_params": best["params"], "best_score": best["score"], "all_results": results}


def _compute_cv_metric(y_true: np.ndarray, y_pred: np.ndarray, metric: str) -> float:
    residuals = y_pred - y_true
    if metric == "rmse":
        return float(np.sqrt(np.mean(residuals ** 2)))
    elif metric == "mae":
        return float(np.mean(np.abs(residuals)))
    elif metric == "mape":
        nonzero = y_true != 0
        if not nonzero.any():
            return float("inf")
        return float(np.mean(np.abs(residuals[nonzero] / y_true[nonzero])) * 100)
    return float(np.sqrt(np.mean(residuals ** 2)))


# ── Model-specific fit-predict wrappers for CV ──────────────────

def sarima_fit_predict(train: np.ndarray, horizon: int, order=(1,1,1),
                       seasonal_order=(1,1,1,12)) -> np.ndarray:
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    model = SARIMAX(train, order=order, seasonal_order=seasonal_order,
                    enforce_stationarity=False, enforce_invertibility=False)
    fitted = model.fit(disp=False, maxiter=200)
    return fitted.forecast(steps=horizon)


def ets_fit_predict(train: np.ndarray, horizon: int, trend="add",
                    seasonal="add", seasonal_periods=12) -> np.ndarray:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    model = ExponentialSmoothing(train, trend=trend, seasonal=seasonal,
                                 seasonal_periods=seasonal_periods)
    fitted = model.fit(optimized=True)
    return fitted.forecast(steps=horizon)


def prophet_fit_predict(train_ds: np.ndarray, train_y: np.ndarray, horizon: int,
                        yearly_seasonality=True, weekly_seasonality=False,
                        changepoint_prior_scale=0.05) -> np.ndarray:
    """Prophet wrapper -- needs ds+y arrays, not just values."""
    try:
        from prophet import Prophet
    except ImportError:
        from fbprophet import Prophet
    df = pd.DataFrame({"ds": pd.to_datetime(train_ds), "y": train_y})
    model = Prophet(yearly_seasonality=yearly_seasonality, weekly_seasonality=weekly_seasonality,
                    daily_seasonality=False, changepoint_prior_scale=changepoint_prior_scale)
    model.fit(df)
    future = model.make_future_dataframe(periods=horizon, freq="MS")
    forecast = model.predict(future)
    return forecast["yhat"].iloc[-horizon:].values


# ── Default parameter grids ─────────────────────────────────────

SARIMA_PARAM_GRID = {
    "order": [(1,1,1), (0,1,1), (1,1,0), (2,1,1), (1,1,2), (0,1,2), (2,1,0)],
    "seasonal_order": [(1,1,1,12), (0,1,1,12), (1,1,0,12), (2,1,1,12)],
}

ETS_PARAM_GRID = {
    "trend": ["add", "mul", None],
    "seasonal": ["add", "mul", None],
    "seasonal_periods": [12],
}

PROPHET_PARAM_GRID = {
    "changepoint_prior_scale": [0.001, 0.01, 0.05, 0.1, 0.5],
    "yearly_seasonality": [True],
    "weekly_seasonality": [False],
}

VAR_PARAM_GRID = {
    "maxlags": [4, 6, 8, 12],
    "ic": ["aic", "bic", "hqic"],
}


def tune_sarima(series_values: np.ndarray, param_grid: dict = None,
                n_iter: int = 10, n_splits: int = 3, metric: str = "rmse",
                seed: int = 42) -> dict:
    grid = param_grid or SARIMA_PARAM_GRID
    return random_search_cv(series_values, sarima_fit_predict, grid,
                            n_iter=n_iter, n_splits=n_splits, metric=metric, seed=seed)


def tune_ets(series_values: np.ndarray, param_grid: dict = None,
             n_iter: int = 10, n_splits: int = 3, metric: str = "rmse",
             seed: int = 42) -> dict:
    grid = param_grid or ETS_PARAM_GRID
    return random_search_cv(series_values, ets_fit_predict, grid,
                            n_iter=n_iter, n_splits=n_splits, metric=metric, seed=seed)


def tune_var_single(df_values: np.ndarray, param_grid: dict = None,
                    n_iter: int = 8, n_splits: int = 3, metric: str = "rmse",
                    seed: int = 42) -> dict:
    """Tune VAR -- operates on multivariate array, returns best maxlags + ic."""
    from statsmodels.tsa.api import VAR as VARModel
    grid = param_grid or VAR_PARAM_GRID
    rng = np.random.RandomState(seed)
    candidates = _sample_param_grid(grid, n_iter, rng)
    splits = time_series_cv_splits(len(df_values), n_splits)

    results = []
    for params in candidates:
        fold_scores = []
        for train_idx, test_idx in splits:
            train = df_values[train_idx]
            test = df_values[test_idx]
            try:
                model = VARModel(train)
                fitted = model.fit(maxlags=params["maxlags"], ic=params["ic"])
                lag_input = train[-fitted.k_ar:]
                forecast = fitted.forecast(lag_input, steps=len(test))
                preds = forecast[:, 0]
                score = _compute_cv_metric(test[:, 0], preds, metric)
                if score is not None and np.isfinite(score):
                    fold_scores.append(score)
            except Exception:
                continue
        avg = float(np.mean(fold_scores)) if fold_scores else float("inf")
        results.append({"params": params, "score": avg, "n_folds": len(fold_scores)})

    results.sort(key=lambda x: x["score"])
    best = results[0] if results else {"params": {"maxlags": 12, "ic": "aic"}, "score": float("inf")}
    return {"best_params": best["params"], "best_score": best["score"], "all_results": results}
