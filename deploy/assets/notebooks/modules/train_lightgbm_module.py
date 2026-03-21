# Fabric Notebook -- Module
# train_lightgbm_module.py -- Global pooled LightGBM training with MLflow persistence

import warnings
import pickle
import tempfile
import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import logging

logger = logging.getLogger("ibp")

warnings.filterwarnings("ignore")


def _get_lgb_feature_cols():
    """Derive LightGBM feature columns from frequency config."""
    target = cfg("target_column")
    lags = freq_params("default_lags")
    rolling = freq_params("default_rolling")

    cols = [f"{target}_lag_{l}" for l in lags]
    for w in rolling:
        cols.append(f"{target}_roll_mean_{w}")
        cols.append(f"{target}_roll_std_{w}")
    cols += ["month_sin", "month_cos", "quarter"]
    cols += cfg("feature_columns")
    return cols


def train_lightgbm_global(df: pd.DataFrame, date_column: str, grain_columns: list,
                           target_column: str, test_ratio: float = 0.2,
                           experiment_name: str = "", model_name: str = "",
                           min_series_length: int | None = None) -> tuple:
    """Train a global pooled LightGBM model across all grains.

    Unlike per-grain models, LightGBM pools data from every grain into a
    single training set.  Grain identity is captured via label-encoded
    categorical features so the model learns cross-grain patterns.

    Returns (results_df, agg_metrics) matching the interface of the other
    train_*_per_grain functions.
    """
    import lightgbm as lgb

    min_series_length = min_series_length or freq_params("min_train_periods")
    if experiment_name:
        ensure_experiment(experiment_name)

    base_feature_cols = _get_lgb_feature_cols()
    available_cols = [c for c in base_feature_cols if c in df.columns]

    label_encoders = {}
    df_enc = df.copy()
    grain_enc_cols = []
    for gc in grain_columns:
        enc_col = f"{gc}_enc"
        le = LabelEncoder()
        df_enc[enc_col] = le.fit_transform(df_enc[gc])
        label_encoders[gc] = le
        grain_enc_cols.append(enc_col)

    feature_cols = available_cols + grain_enc_cols

    logger.info(f"[lightgbm] Building train/test pools from "
                f"{df_enc.groupby(grain_columns).ngroups} grains...")

    train_frames, test_frames = [], []
    groups = df_enc.groupby(grain_columns)

    for grain_key, group in groups:
        if isinstance(grain_key, str):
            grain_key = (grain_key,)
        series = group.sort_values(date_column)
        if len(series) < min_series_length:
            continue
        split_idx = int(len(series) * (1 - test_ratio))
        train_frames.append(series.iloc[:split_idx])
        test_frames.append(series.iloc[split_idx:])

    if not train_frames:
        logger.warning("[lightgbm] No grains with sufficient data.")
        return pd.DataFrame(), {}

    train_pool = pd.concat(train_frames).dropna(subset=feature_cols + [target_column])
    test_pool = pd.concat(test_frames).dropna(subset=feature_cols + [target_column])

    logger.info(f"[lightgbm] Train pool: {len(train_pool)} rows, "
                f"Test pool: {len(test_pool)} rows, "
                f"Features: {len(feature_cols)}")

    n_estimators = int(cfg("lightgbm_n_estimators") or 800)
    max_depth = int(cfg("lightgbm_max_depth") or 7)
    learning_rate = float(cfg("lightgbm_learning_rate") or 0.02)
    num_leaves = int(cfg("lightgbm_num_leaves") or 40)
    min_child_samples = int(cfg("lightgbm_min_child_samples") or 10)

    model = lgb.LGBMRegressor(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        num_leaves=num_leaves,
        min_child_samples=min_child_samples,
        subsample=0.8,
        colsample_bytree=0.7,
        reg_alpha=0.5,
        reg_lambda=1.0,
        verbose=-1,
        random_state=42,
    )
    model.fit(train_pool[feature_cols], train_pool[target_column])

    test_preds = np.clip(model.predict(test_pool[feature_cols]), 0, None)

    results = []
    for i, (_, row) in enumerate(test_pool.iterrows()):
        r = {
            date_column: str(row["period"]),
            "actual": float(row[target_column]),
            "predicted": float(test_preds[i]),
            "model_type": "lightgbm",
        }
        for gc in grain_columns:
            r[gc] = row[gc]
        results.append(r)

    agg_metrics = compute_metrics(
        test_pool[target_column].values, test_preds
    )

    logger.info(f"[lightgbm] Backtest MAPE: {agg_metrics.get('mape', 'N/A')}")

    full_pool = pd.concat(train_frames + test_frames).dropna(
        subset=feature_cols + [target_column]
    )
    model.fit(full_pool[feature_cols], full_pool[target_column])
    logger.info(f"[lightgbm] Refitted on full data ({len(full_pool)} rows) "
                "for forward scoring.")

    if experiment_name:
        try:
            import mlflow
            with mlflow.start_run(run_name=f"{model_name}_aggregate") as run:
                log_metrics_to_mlflow(agg_metrics, prefix="lightgbm")
                mlflow.log_metric("n_grains", len(train_frames))
                mlflow.log_metric("n_features", len(feature_cols))
                mlflow.log_metric("train_rows", len(full_pool))

                tmp = tempfile.mkdtemp()
                pkl_path = os.path.join(tmp, "lightgbm_model.pkl")
                model_data = {
                    "model": model,
                    "feature_cols": feature_cols,
                    "label_encoders": label_encoders,
                }
                with open(pkl_path, "wb") as f:
                    pickle.dump(model_data, f)
                mlflow.log_artifact(pkl_path, "models")
                logger.info(f"[lightgbm] Logged model to MLflow "
                            f"run {run.info.run_id}")
        except Exception as e:
            logger.warning(f"[lightgbm] MLflow logging warning: {e}")

    logger.info(f"[lightgbm] Complete. {len(results)} prediction rows.")
    return pd.DataFrame(results), agg_metrics
