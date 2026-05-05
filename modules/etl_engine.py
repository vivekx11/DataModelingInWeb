"""
Smart ETL Engine Module
Upload CSV → Auto Detect → Clean → Feature Engineer → Auto ML → Dashboard
"""

import os, io, json, sqlite3, uuid, traceback, warnings, math
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.metrics import (mean_squared_error, r2_score,
                             accuracy_score, mean_absolute_error)
try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
warnings.filterwarnings("ignore")

BASE_DIR = Path("etl_output")
BASE_DIR.mkdir(exist_ok=True)
DB_PATH = BASE_DIR / "analytics.db"

SESSIONS = {}


def clean_for_json(obj):
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif hasattr(obj, 'item'):
        val = obj.item()
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return None
        return val
    return obj


def detect_encoding(raw):
    for enc in ["utf-8", "latin-1", "cp1252", "utf-16"]:
        try:
            raw.decode(enc)
            return enc
        except Exception:
            continue
    return "utf-8"


def detect_delimiter(sample):
    counts = {d: sample.count(d) for d in [",", ";", "\t", "|"]}
    return max(counts, key=counts.get)


def infer_column_types(df):
    types = {}
    for col in df.columns:
        s = df[col].dropna()
        if s.empty:
            types[col] = "empty"
            continue
        try:
            pd.to_datetime(s.head(100), infer_datetime_format=True)
            types[col] = "datetime"
            continue
        except Exception:
            pass
        try:
            pd.to_numeric(s)
            nuniq = s.nunique()
            types[col] = "binary" if nuniq == 2 else ("categorical_num" if nuniq <= 15 else "numeric")
            continue
        except Exception:
            pass
        nuniq = s.nunique()
        types[col] = "categorical" if nuniq / len(s) < 0.5 else "text"
    return types


def smart_impute(df, col_types):
    logs = []
    for col, ctype in col_types.items():
        missing = df[col].isna().sum()
        if missing == 0:
            continue
        pct = round(missing / len(df) * 100, 1)
        if pct > 70:
            df.drop(columns=[col], inplace=True)
            logs.append(f"Dropped '{col}' — {pct}% missing")
            continue
        if ctype in ("numeric", "binary", "categorical_num"):
            fill = df[col].median()
            df[col] = df[col].fillna(fill)
            logs.append(f"'{col}': filled {missing} missing with median ({fill:.2f})")
        elif ctype == "datetime":
            df[col] = df[col].ffill()
            logs.append(f"'{col}': forward-filled {missing} missing datetime values")
        else:
            fill = df[col].mode()[0] if not df[col].mode().empty else "Unknown"
            df[col] = df[col].fillna(fill)
            logs.append(f"'{col}': filled {missing} missing with mode ('{fill}')")
    return df, logs


def remove_outliers(df, numeric_cols):
    before = len(df)
    for col in numeric_cols:
        q1, q3 = df[col].quantile(0.01), df[col].quantile(0.99)
        iqr = q3 - q1
        df = df[(df[col] >= q1 - 3 * iqr) & (df[col] <= q3 + 3 * iqr)]
    return df, before - len(df)


def feature_engineer(df, col_types):
    logs = []
    cols = list(df.columns)
    rev_col = next((c for c in cols if any(k in c.lower() for k in ["revenue","sales","income","amount","price"])), None)
    cost_col = next((c for c in cols if any(k in c.lower() for k in ["cost","expense","spend","expenditure"])), None)
    if rev_col and cost_col:
        try:
            df["Profit"] = pd.to_numeric(df[rev_col], errors="coerce") - pd.to_numeric(df[cost_col], errors="coerce")
            df["Profit_Margin_%"] = (df["Profit"] / pd.to_numeric(df[rev_col], errors="coerce") * 100).round(2)
            logs.append(f"Engineered 'Profit' and 'Profit_Margin_%'")
        except Exception:
            pass
    for col, ctype in col_types.items():
        if ctype == "datetime" and col in df.columns:
            try:
                dt = pd.to_datetime(df[col], infer_datetime_format=True, errors="coerce")
                df[f"{col}_year"] = dt.dt.year
                df[f"{col}_month"] = dt.dt.month
                df[f"{col}_day"] = dt.dt.day
                df[f"{col}_dow"] = dt.dt.dayofweek
                logs.append(f"Extracted date parts from '{col}'")
            except Exception:
                pass
    qty_col = next((c for c in cols if any(k in c.lower() for k in ["qty","units","quantity","count"])), None)
    price_col = next((c for c in cols if any(k in c.lower() for k in ["price","rate","unit_price"])), None)
    if qty_col and price_col:
        try:
            df["Total_Value"] = pd.to_numeric(df[qty_col], errors="coerce") * pd.to_numeric(df[price_col], errors="coerce")
            logs.append(f"Engineered 'Total_Value' = {qty_col} × {price_col}")
        except Exception:
            pass
    return df, logs


def detect_problem_type(df, target, col_types):
    ctype = col_types.get(target, "unknown")
    nuniq = df[target].nunique()
    if ctype in ("categorical", "binary") or nuniq <= 10:
        return "classification"
    return "regression"


def auto_ml(df, target, problem):
    feature_cols = [c for c in df.columns if c != target]
    df_ml = df[feature_cols + [target]].copy().dropna()
    for col in df_ml.select_dtypes(include="object").columns:
        le = LabelEncoder()
        df_ml[col] = le.fit_transform(df_ml[col].astype(str))
    X = df_ml[feature_cols].select_dtypes(include=[np.number])
    y = df_ml[target]
    if len(X) < 20:
        return {"error": "Not enough data for ML"}
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)
    results = {}
    if problem == "regression":
        models = {
            "Ridge Regression": Ridge(alpha=1.0),
            "Random Forest": RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1),
        }
        if HAS_XGB:
            models["XGBoost"] = xgb.XGBRegressor(n_estimators=100, random_state=42, verbosity=0, eval_metric="rmse")
        for name, model in models.items():
            try:
                Xtr = X_train_s if name == "Ridge Regression" else X_train
                Xte = X_test_s if name == "Ridge Regression" else X_test
                model.fit(Xtr, y_train)
                preds = model.predict(Xte)
                results[name] = {
                    "R2": round(r2_score(y_test, preds), 4),
                    "RMSE": round(np.sqrt(mean_squared_error(y_test, preds)), 4),
                    "MAE": round(mean_absolute_error(y_test, preds), 4),
                }
            except Exception as e:
                results[name] = {"error": str(e)}
        valid = [k for k in results if "R2" in results[k]]
        if not valid:
            return {"error": "All models failed"}
        best_name = max(valid, key=lambda k: results[k]["R2"])
        best_model = models[best_name]
        Xfull = X_train_s if best_name == "Ridge Regression" else X_train
        Xte_f = X_test_s if best_name == "Ridge Regression" else X_test
        best_model.fit(Xfull, y_train)
        final_preds = best_model.predict(Xte_f)
        feat_imp = {}
        if hasattr(best_model, "feature_importances_"):
            feat_imp = dict(zip(X.columns, [round(float(v), 4) for v in best_model.feature_importances_]))
            feat_imp = dict(sorted(feat_imp.items(), key=lambda x: x[1], reverse=True)[:10])
        actual_vs_pred = [
            {"index": int(i), "actual": round(float(a), 4), "predicted": round(float(p), 4)}
            for i, (a, p) in enumerate(zip(y_test.values[:100], final_preds[:100]))
        ]
        return {
            "problem_type": "regression", "target": target,
            "best_model": best_name, "all_scores": results,
            "best_metrics": results[best_name], "feature_importance": feat_imp,
            "actual_vs_predicted": actual_vs_pred, "features_used": list(X.columns),
        }
    else:
        models = {
            "Random Forest": RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1),
        }
        if HAS_XGB:
            models["XGBoost"] = xgb.XGBClassifier(n_estimators=100, random_state=42, verbosity=0, use_label_encoder=False, eval_metric="logloss")
        le_target = LabelEncoder()
        y_train_enc = le_target.fit_transform(y_train.astype(str))
        y_test_enc = le_target.transform(y_test.astype(str))
        for name, model in models.items():
            try:
                model.fit(X_train, y_train_enc)
                preds = model.predict(X_test)
                results[name] = {"Accuracy": round(accuracy_score(y_test_enc, preds), 4)}
            except Exception as e:
                results[name] = {"error": str(e)}
        valid = [k for k in results if "Accuracy" in results[k]]
        if not valid:
            return {"error": "All models failed"}
        best_name = max(valid, key=lambda k: results[k]["Accuracy"])
        best_model = models[best_name]
        best_model.fit(X_train, y_train_enc)
        final_preds = best_model.predict(X_test)
        feat_imp = {}
        if hasattr(best_model, "feature_importances_"):
            feat_imp = dict(zip(X.columns, [round(float(v), 4) for v in best_model.feature_importances_]))
            feat_imp = dict(sorted(feat_imp.items(), key=lambda x: x[1], reverse=True)[:10])
        actual_vs_pred = [
            {"index": int(i),
             "actual": str(le_target.inverse_transform([int(a)])[0]),
             "predicted": str(le_target.inverse_transform([int(p)])[0])}
            for i, (a, p) in enumerate(zip(y_test_enc[:100], final_preds[:100]))
        ]
        return {
            "problem_type": "classification", "target": target,
            "best_model": best_name, "all_scores": results,
            "best_metrics": results[best_name], "feature_importance": feat_imp,
            "actual_vs_predicted": actual_vs_pred, "features_used": list(X.columns),
        }


def save_to_sqlite(df, job_id):
    conn = sqlite3.connect(DB_PATH)
    table = f"cleaned_{job_id[:8]}"
    df.to_sql(table, conn, if_exists="replace", index=False)
    conn.close()
    return table


def run_etl_pipeline(raw_bytes, filename, job_id):
    logs = []
    logs.append({"phase": "EXTRACT", "msg": f"Reading file: {filename}"})
    enc = detect_encoding(raw_bytes)
    sample = raw_bytes[:4096].decode(enc, errors="replace")
    delim = detect_delimiter(sample)
    logs.append({"phase": "EXTRACT", "msg": f"Detected encoding: {enc} | delimiter: '{delim}'"})
    df = pd.read_csv(io.BytesIO(raw_bytes), encoding=enc, sep=delim, low_memory=False, on_bad_lines="skip")
    original_shape = df.shape
    logs.append({"phase": "EXTRACT", "msg": f"Loaded {original_shape[0]:,} rows × {original_shape[1]} columns"})
    col_types = infer_column_types(df)
    for ctype in set(col_types.values()):
        cols = [c for c, t in col_types.items() if t == ctype]
        logs.append({"phase": "EXTRACT", "msg": f"Type '{ctype}': {', '.join(cols)}"})
    dups = df.duplicated().sum()
    df.drop_duplicates(inplace=True)
    logs.append({"phase": "TRANSFORM", "msg": f"Removed {dups:,} duplicate rows"})
    zero_var = [c for c in df.columns if df[c].nunique() <= 1]
    df.drop(columns=zero_var, inplace=True, errors="ignore")
    if zero_var:
        logs.append({"phase": "TRANSFORM", "msg": f"Dropped zero-variance columns: {', '.join(zero_var)}"})
    df, imp_logs = smart_impute(df, col_types)
    for l in imp_logs:
        logs.append({"phase": "TRANSFORM", "msg": l})
    num_cols = [c for c in df.columns if c in col_types and col_types[c] == "numeric"]
    df, outliers_removed = remove_outliers(df, num_cols)
    logs.append({"phase": "TRANSFORM", "msg": f"Outlier removal: removed {outliers_removed:,} rows"})
    df, eng_logs = feature_engineer(df, col_types)
    for l in eng_logs:
        logs.append({"phase": "TRANSFORM", "msg": l})
    logs.append({"phase": "TRANSFORM", "msg": f"Clean shape: {len(df):,} rows × {len(df.columns)} columns"})
    col_types = infer_column_types(df)
    num_cols_final = df.select_dtypes(include=[np.number]).columns.tolist()
    stats = {}
    for col in num_cols_final:
        s = df[col].dropna()
        stats[col] = {
            "min": round(float(s.min()), 4), "max": round(float(s.max()), 4),
            "mean": round(float(s.mean()), 4), "median": round(float(s.median()), 4),
            "std": round(float(s.std()), 4), "sum": round(float(s.sum()), 4),
            "missing_pct": round(df[col].isna().mean() * 100, 2),
        }
    distributions = {}
    for col in num_cols_final[:8]:
        hist, edges = np.histogram(df[col].dropna(), bins=20)
        distributions[col] = {
            "bins": [round(float(e), 2) for e in edges[:-1]],
            "counts": [int(c) for c in hist],
        }
    cat_counts = {}
    cat_cols = [c for c in df.columns if col_types.get(c) in ("categorical", "binary")]
    for col in cat_cols[:6]:
        vc = df[col].value_counts().head(10)
        cat_counts[col] = {"labels": list(vc.index.astype(str)), "values": [int(v) for v in vc.values]}
    corr_cols = num_cols_final[:12]
    corr = df[corr_cols].corr().round(3) if len(corr_cols) > 1 else pd.DataFrame()
    corr_data = {"columns": corr_cols, "matrix": corr.values.tolist()} if not corr.empty else {}
    ml_result = {}
    target_candidates = [c for c in df.columns if any(k in c.lower() for k in
                         ["target","label","class","outcome","profit","revenue","sales","price","churn","fraud"])]
    target = target_candidates[0] if target_candidates else (num_cols_final[-1] if num_cols_final else None)
    if target and target in df.columns:
        logs.append({"phase": "ML", "msg": f"Auto-selected target: '{target}'"})
        problem = detect_problem_type(df, target, col_types)
        logs.append({"phase": "ML", "msg": f"Problem type: {problem.upper()}"})
        ml_result = auto_ml(df, target, problem)
        if "error" not in ml_result:
            logs.append({"phase": "ML", "msg": f"Best model: {ml_result['best_model']}"})
    else:
        logs.append({"phase": "ML", "msg": "No suitable target — skipping ML"})
    csv_path = BASE_DIR / f"cleaned_{job_id}.csv"
    df.to_csv(csv_path, index=False)
    logs.append({"phase": "LOAD", "msg": f"Saved cleaned CSV"})
    table_name = save_to_sqlite(df, job_id)
    logs.append({"phase": "LOAD", "msg": f"Saved to SQLite '{table_name}'"})
    logs.append({"phase": "LOAD", "msg": "Pipeline complete ✓"})
    preview_rows = df.head(200).replace({np.nan: None}).to_dict(orient="records")
    return clean_for_json({
        "job_id": job_id, "filename": filename,
        "original_shape": {"rows": original_shape[0], "cols": original_shape[1]},
        "clean_shape": {"rows": len(df), "cols": len(df.columns)},
        "columns": df.columns.tolist(), "col_types": col_types,
        "stats": stats, "distributions": distributions,
        "cat_counts": cat_counts, "corr_data": corr_data,
        "ml": ml_result, "logs": logs, "preview": preview_rows,
    })


def process_upload(raw_bytes, filename):
    job_id = str(uuid.uuid4())
    result = run_etl_pipeline(raw_bytes, filename, job_id)
    SESSIONS[job_id] = result
    return job_id, result


def get_results(job_id):
    return clean_for_json(SESSIONS.get(job_id))


def get_download_path(job_id):
    path = BASE_DIR / f"cleaned_{job_id}.csv"
    return path if path.exists() else None
