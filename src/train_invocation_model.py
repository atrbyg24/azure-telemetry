"""
train_invocation_model.py
=========================
Trains a LightGBM binary classifier to predict whether a given Azure Function
(hashfunction) will be invoked at any point in the future.

Strategy
--------
- Use global_minutes 1–14400  (days 1–10) as the historical feature window.
- Use global_minutes 14401–20160 (days 11–14) as the prediction window.
- Target: 1 if the function has ANY invocations in the prediction window.

Feature engineering (computed from the training window):
  total_invocations    — total call count
  active_minutes       — number of distinct minutes with at least 1 call
  days_active          — number of distinct days with at least 1 call
  max_invocations      — peak calls in a single minute
  recent_invocations   — calls in days 8–10 (recency signal)
  early_invocations    — calls in days 1–3 (baseline signal)
  trend_ratio          — recent / (early + 1)  (growth/decline signal)
  avg_inv_per_min      — total / active_minutes (intensity signal)
  activity_density     — active_minutes / (days_active × 1440)

Chunking
--------
1. The parquet file (140 M rows) is read in batches via pyarrow to stay
   within memory limits during feature aggregation.
2. After aggregation the feature matrix (one row per function) is split into
   N_TRAIN_CHUNKS subsets and LightGBM is trained incrementally using
   init_model so each chunk continues from where the previous left off.
"""

import os

import lightgbm as lgb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SRC_DIR      = os.path.dirname(os.path.abspath(__file__))
PARQUET_PATH = os.path.join(SRC_DIR, '..', 'data', 'processed', 'invocations_combined.parquet')
MODEL_PATH   = os.path.join(SRC_DIR, '..', 'models', 'lgbm_invocation_model.txt')

# ---------------------------------------------------------------------------
# Time-window constants  (all in global minutes)
# ---------------------------------------------------------------------------
TRAIN_END     = 10 * 1440   # 14 400  — last minute of day 10 (inclusive)
RECENT_START  =  7 * 1440 + 1   # 10 081  — first minute of day 8  (recent = days 8–10)
EARLY_END     =  3 * 1440   #  4 320  — last minute of day 3    (early  = days 1–3)

# ---------------------------------------------------------------------------
# Tuning knobs
# ---------------------------------------------------------------------------
READ_CHUNK_SIZE = 5_000_000   # rows per parquet read batch
N_TRAIN_CHUNKS  = 5           # incremental LightGBM chunks
ROUNDS_PER_CHUNK = 100        # trees added per incremental chunk
EARLY_STOPPING  = 50          # rounds with no val-AUC improvement → stop

FEATURE_NAMES = [
    'total_invocations',
    'active_minutes',
    'days_active',
    'max_invocations',
    'recent_invocations',
    'early_invocations',
    'trend_ratio',
    'avg_inv_per_min',
    'activity_density',
]


def read_and_aggregate(parquet_path: str):
    """
    Stream through the parquet file in READ_CHUNK_SIZE-row batches and
    accumulate per-function statistics needed for feature engineering.
    Returns four objects (all indexed by hashfunction):
        sum_df   — DataFrame with summable columns
        max_ser  — Series of per-function max invocations
        day_df   — DataFrame of unique (hashfunction, day) pairs (train window)
        target   — Series: total invocations in the prediction window
    """
    pf = pq.ParquetFile(parquet_path)

    sum_df   = None   # accumulated sums
    max_ser  = None   # element-wise max across chunks
    day_parts = []    # list of (hashfunction, day) pair DataFrames — train only
    target   = None   # total invocations in prediction window

    print(f"Streaming {parquet_path}")
    print(f"  {pf.metadata.num_rows:,} rows | {pf.metadata.num_row_groups} row groups\n")

    for batch_idx, batch in enumerate(pf.iter_batches(batch_size=READ_CHUNK_SIZE)):
        # Convert to columnar dicts for fast slicing before pandas
        d = batch.to_pydict()

        # Build a minimal pandas-compatible dict and create DataFrame
        chunk = pd.DataFrame({
            'hashfunction': d['hashfunction'],
            'invocations':  d['invocations'],
            'global_minute': d['global_minute'],
        })

        # ---- Split into training / prediction windows ----
        # global_minute is 1-based: day D spans minutes (D-1)*1440+1 .. D*1440
        train_mask = chunk['global_minute'] <= TRAIN_END
        train = chunk[train_mask].copy()
        pred  = chunk[~train_mask]

        # ---- Aggregate training-window features ----
        if not train.empty:
            train['is_recent'] = (train['global_minute'] >= RECENT_START).astype('int8')
            train['is_early']  = (train['global_minute'] <= EARLY_END).astype('int8')
            # day number: minute 1–1440 → day 1, 1441–2880 → day 2, etc.
            train['day']       = (train['global_minute'] - 1) // 1440 + 1

            # Summable columns
            g = (
                train.assign(
                    recent_inv = train['invocations'] * train['is_recent'],
                    early_inv  = train['invocations'] * train['is_early'],
                )
                .groupby('hashfunction', sort=False)
                .agg(
                    total_invocations  = ('invocations',  'sum'),
                    active_minutes     = ('invocations',  'count'),   
                    recent_invocations = ('recent_inv',   'sum'),
                    early_invocations  = ('early_inv',    'sum'),
                )
            )
            sum_df = g if sum_df is None else sum_df.add(g, fill_value=0)

            # Element-wise max
            g_max = (
                train.groupby('hashfunction', sort=False)['invocations']
                .max()
                .rename('max_invocations')
            )
            if max_ser is None:
                max_ser = g_max
            else:
                max_ser = pd.concat([max_ser, g_max]).groupby(level=0).max()

            # Unique (hashfunction, day) pairs for days_active calculation
            pairs = (
                train[['hashfunction', 'day']]
                .drop_duplicates()
            )
            day_parts.append(pairs)

        # ---- Aggregate prediction-window target ----
        if not pred.empty:
            g_tgt = (
                pred.groupby('hashfunction', sort=False)['invocations']
                .sum()
                .rename('target')
            )
            target = g_tgt if target is None else target.add(g_tgt, fill_value=0)

        n_funcs = len(sum_df) if sum_df is not None else 0
        print(f"  Batch {batch_idx + 1:3d} processed — functions seen so far: {n_funcs:,}")

    # Compute days_active across all chunks (deduplicated)
    print("\nComputing days_active across all batches...")
    all_pairs   = pd.concat(day_parts, ignore_index=True).drop_duplicates()
    days_active = all_pairs.groupby('hashfunction').size().rename('days_active')

    return sum_df, max_ser, days_active, target


def build_feature_matrix(sum_df, max_ser, days_active, target):
    feat = sum_df.copy()
    feat = feat.join(max_ser,    how='left')
    feat = feat.join(days_active, how='left')
    feat['days_active'] = feat['days_active'].fillna(1).clip(lower=1)

    feat['avg_inv_per_min']  = feat['total_invocations'] / feat['active_minutes'].clip(lower=1)
    feat['trend_ratio']      = feat['recent_invocations'] / (feat['early_invocations'] + 1)
    feat['activity_density'] = feat['active_minutes'] / (feat['days_active'] * 1440)

    feat = feat[FEATURE_NAMES]

    labels = (target.reindex(feat.index).fillna(0) > 0).astype('int8')

    pos_rate = labels.mean()
    print(f"\nFeature matrix: {len(feat):,} functions × {feat.shape[1]} features")
    print(f"Class balance : {pos_rate:.1%} positive (invoked in days 11–14)\n")

    return feat.values.astype('float32'), labels.values



def train_lightgbm_incremental(X, y):
    """
    Split X_train into N_TRAIN_CHUNKS subsets and train LightGBM incrementally
    using init_model so each chunk continues from the previous checkpoint.
    """
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    print(f"Train size: {len(X_train):,}  |  Val size: {len(X_val):,}\n")

    dval = lgb.Dataset(X_val, label=y_val, free_raw_data=False)

    params = {
        'objective':         'binary',
        'metric':            'auc',
        'learning_rate':     0.05,
        'num_leaves':        63,
        'min_child_samples': 20,
        'feature_fraction':  0.8,
        'bagging_fraction':  0.8,
        'bagging_freq':      5,
        'is_unbalance':      True,    
        'verbose':           -1,
    }

    model = None
    chunk_size = max(1, len(X_train) // N_TRAIN_CHUNKS)

    for chunk_idx in range(N_TRAIN_CHUNKS):
        start = chunk_idx * chunk_size
        end   = start + chunk_size if chunk_idx < N_TRAIN_CHUNKS - 1 else len(X_train)

        Xi, yi = X_train[start:end], y_train[start:end]
        dtrain  = lgb.Dataset(Xi, label=yi, free_raw_data=True)

        print(f"--- Training chunk {chunk_idx + 1}/{N_TRAIN_CHUNKS}  "
              f"({end - start:,} samples, rows {start}–{end}) ---")

        model = lgb.train(
            params,
            dtrain,
            num_boost_round=ROUNDS_PER_CHUNK,
            init_model=model,           
            valid_sets=[dval],
            valid_names=['val'],
            callbacks=[
                lgb.early_stopping(stopping_rounds=EARLY_STOPPING, verbose=True),
                lgb.log_evaluation(period=25),
            ],
        )

    val_preds = model.predict(X_val)
    auc       = roc_auc_score(y_val, val_preds)
    print(f"\n{'='*60}")
    print(f"Final Validation AUC: {auc:.4f}")
    print(f"Total trees in model: {model.num_trees()}")
    print('='*60)
    print(classification_report(
        y_val,
        (val_preds >= 0.5).astype(int),
        target_names=['Not invoked (0)', 'Invoked (1)'],
    ))

    print("\nTop feature importances (gain):")
    importances = sorted(
        zip(FEATURE_NAMES, model.feature_importance(importance_type='gain')),
        key=lambda x: x[1], reverse=True
    )
    for name, imp in importances:
        print(f"  {name:<25} {imp:,.1f}")

    return model


def main():
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)

    sum_df, max_ser, days_active, target = read_and_aggregate(PARQUET_PATH)

    X, y = build_feature_matrix(sum_df, max_ser, days_active, target)

    model = train_lightgbm_incremental(X, y)

    model.save_model(MODEL_PATH)
    print(f"\nModel saved → {MODEL_PATH}")


if __name__ == '__main__':
    main()
