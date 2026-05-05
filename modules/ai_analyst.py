"""
AI Data Analyst Module
Upload CSV/Excel → Auto Clean → Plotly Visualizations → ML Metrics → Gemini AI Chat
"""

import os, io, json, traceback
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.utils
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
import warnings
warnings.filterwarnings('ignore')

# Gemini Setup
try:
    import google.generativeai as genai
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
        gemini_model = genai.GenerativeModel("gemini-1.5-flash")
    else:
        gemini_model = None
except Exception:
    gemini_model = None

# In-memory storage
ai_cleaned_df = None
ai_original_df = None
ai_current_changes = {}


def clean_data_advanced(df):
    changes = {
        "original_shape": list(df.shape),
        "duplicates_removed": 0,
        "missing_values": {},
        "outliers": {}
    }

    before = len(df)
    df = df.drop_duplicates()
    changes["duplicates_removed"] = before - len(df)

    for col in df.columns:
        missing = df[col].isnull().sum()
        if missing > 0:
            if df[col].dtype in ['int64', 'float64']:
                df[col] = df[col].fillna(df[col].median())
                changes["missing_values"][col] = f"{missing} (filled with median)"
            else:
                mode_val = df[col].mode()[0] if not df[col].mode().empty else "Unknown"
                df[col] = df[col].fillna(mode_val)
                changes["missing_values"][col] = f"{missing} (filled with '{mode_val}')"

    for col in df.select_dtypes(include=[np.number]).columns:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        outliers = df[(df[col] < lower) | (df[col] > upper)]
        if len(outliers) > 0:
            changes["outliers"][col] = len(outliers)
            df = df[(df[col] >= lower) & (df[col] <= upper)]

    changes["final_shape"] = list(df.shape)

    score = 100
    total_missing = sum(df[col].isnull().sum() for col in df.columns)
    if total_missing > 0:
        score -= min(30, (total_missing / (df.shape[0] * df.shape[1])) * 100)
    if changes["duplicates_removed"] > 0:
        score -= min(10, (changes["duplicates_removed"] / changes["original_shape"][0]) * 50)
    changes["quality_score"] = max(0, int(score))

    return df, changes


def calculate_accuracy_metrics(df):
    metrics = {}
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    if len(numeric_cols) >= 2:
        target = numeric_cols[-1]
        features = numeric_cols[:-1]
        X = df[features].fillna(df[features].mean())
        y = df[target].fillna(df[target].mean())

        if len(X) > 10 and len(features) > 0:
            try:
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
                model = RandomForestRegressor(n_estimators=50, random_state=42)
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
                metrics["R² Score"] = round(r2_score(y_test, y_pred), 4)
                metrics["RMSE"] = round(np.sqrt(mean_squared_error(y_test, y_pred)), 4)
            except Exception:
                pass

    return metrics


def generate_chart(df, chart_type):
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()

    fig = None
    if chart_type == 'bar' and categorical_cols and numeric_cols:
        fig = px.bar(df, x=categorical_cols[0], y=numeric_cols[0])
    elif chart_type == 'pie' and categorical_cols:
        fig = px.pie(df, names=categorical_cols[0])
    elif chart_type == 'line' and len(numeric_cols) >= 2:
        fig = px.line(df, x=numeric_cols[0], y=numeric_cols[1])
    elif chart_type == 'heatmap' and len(numeric_cols) >= 2:
        corr = df[numeric_cols].corr()
        fig = px.imshow(corr, text_auto=True, aspect="auto")
    elif chart_type == 'scatter' and len(numeric_cols) >= 2:
        fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1])
    elif chart_type == 'box' and numeric_cols:
        fig = px.box(df, y=numeric_cols[0])

    if fig is None:
        return None

    return json.dumps(fig.to_dict(), cls=plotly.utils.PlotlyJSONEncoder)


def gemini_response(df, user_message, changes):
    if not gemini_model:
        return "Gemini API key not configured. Please add GEMINI_API_KEY environment variable."

    context = f"""
    Dataset: {df.shape[0]} rows, {df.shape[1]} columns
    Columns: {list(df.columns)}
    Cleaning: Removed {changes.get('duplicates_removed',0)} duplicates
    Quality Score: {changes.get('quality_score',0)}/100

    User: {user_message}
    Respond helpfully about this data.
    """
    try:
        response = gemini_model.generate_content(context)
        return response.text[:1000]
    except Exception:
        return "AI analysis temporarily unavailable."


def process_upload(file):
    global ai_cleaned_df, ai_original_df, ai_current_changes

    try:
        filename = file.filename
        if filename.endswith('.csv'):
            df = pd.read_csv(file, nrows=100000)
        else:
            df = pd.read_excel(file, nrows=100000)

        ai_original_df = df.copy()
        ai_cleaned_df, changes = clean_data_advanced(df)
        ai_current_changes = changes

        accuracy_metrics = calculate_accuracy_metrics(ai_cleaned_df)
        changes['accuracy_metrics'] = accuracy_metrics

        if accuracy_metrics and 'R² Score' in accuracy_metrics:
            changes['accuracy_score'] = round(accuracy_metrics['R² Score'], 4)
        else:
            changes['accuracy_score'] = changes.get('quality_score', 85)

        return changes

    except Exception as e:
        return {"error": str(e)}


def get_chart(chart_type):
    global ai_cleaned_df
    if ai_cleaned_df is None:
        return {"error": "No data available"}
    chart_json = generate_chart(ai_cleaned_df, chart_type)
    if chart_json:
        return {"chart": chart_json}
    return {"error": "Cannot create chart with current data"}


def get_chat_response(message):
    global ai_cleaned_df, ai_current_changes
    if ai_cleaned_df is None:
        return {"response": "Please upload data first."}
    response = gemini_response(ai_cleaned_df, message, ai_current_changes)
    return {"response": response}


def download_cleaned(fmt):
    global ai_cleaned_df
    if ai_cleaned_df is None:
        return None
    output = io.BytesIO()
    if fmt == 'csv':
        ai_cleaned_df.to_csv(output, index=False)
        output.seek(0)
        return output, 'text/csv', 'cleaned_data.csv'
    elif fmt == 'excel':
        ai_cleaned_df.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)
        return output, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'cleaned_data.xlsx'
    return None
