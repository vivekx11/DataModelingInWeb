"""
SQL Analytics Engine Module
Upload CSV → Load to SQLite → Run 15+ SQL Queries → Live SQL Editor → Export .sql
"""

import os, io, uuid, json, traceback, math, re, sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
from flask import jsonify
import warnings
warnings.filterwarnings("ignore")

SESSIONS = {}
REPORT_DIR = Path("sql_reports")
REPORT_DIR.mkdir(exist_ok=True)


def clean(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if hasattr(v, 'item'):
        val = v.item()
        return None if isinstance(val, float) and (math.isnan(val) or math.isinf(val)) else val
    return v


def clean_dict(d):
    if isinstance(d, dict):
        return {k: clean_dict(v) for k, v in d.items()}
    if isinstance(d, list):
        return [clean_dict(i) for i in d]
    return clean(d)


def safe_col(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(name).strip())


def detect_cols(columns, df=None):
    rev = next((c for c in columns if any(k in c.lower() for k in ['revenue','sales','income','amount','price','total'])), None)
    cost = next((c for c in columns if any(k in c.lower() for k in ['cost','expense','spend'])), None)
    date = next((c for c in columns if any(k in c.lower() for k in ['date','month','period','year','time'])), None)
    qty = next((c for c in columns if any(k in c.lower() for k in ['qty','units','quantity','count','volume'])), None)
    cat = None
    for c in columns:
        if any(k in c.lower() for k in ['category','product','region','segment','department','type','channel']):
            if df is not None:
                nuniq = df[c].nunique()
                if nuniq <= 50:
                    cat = c
                    break
            else:
                cat = c
                break
    return rev, cost, cat, date, qty


def run_query_sqlite(sql, db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
        cols = [d[0] for d in cur.description] if cur.description else []
        return rows, cols
    finally:
        conn.close()


def load_csv_to_db(df, table_name, db_path):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False, chunksize=1000)
    conn.close()


def build_queries(table, columns, rev, cost, cat, date, qty):
    T = table
    queries = {}
    queries['overview'] = {
        'title': 'Dataset Overview',
        'sql': f"SELECT COUNT(*) as total_rows FROM {T}",
        'desc': 'Basic row count'
    }
    if rev:
        queries['total_revenue'] = {
            'title': 'Total & Average Revenue',
            'sql': f"""SELECT ROUND(SUM({rev}), 2) AS total_revenue, ROUND(AVG({rev}), 2) AS avg_revenue,
                       ROUND(MAX({rev}), 2) AS max_revenue, ROUND(MIN({rev}), 2) AS min_revenue FROM {T}""",
            'desc': f'Aggregate metrics from {rev}'
        }
        if cat:
            queries['revenue_by_cat'] = {
                'title': f'Revenue by {cat} — GROUP BY',
                'sql': f"""SELECT {cat}, ROUND(SUM({rev}), 2) AS total_revenue, ROUND(AVG({rev}), 2) AS avg_revenue,
                           COUNT(*) AS transactions FROM {T} GROUP BY {cat} ORDER BY total_revenue DESC""",
                'desc': f'GROUP BY query grouping revenue by {cat}'
            }
            queries['top_performers'] = {
                'title': f'Top 5 {cat} by Revenue',
                'sql': f"""SELECT {cat}, ROUND(SUM({rev}), 2) AS total_revenue FROM {T}
                           GROUP BY {cat} ORDER BY total_revenue DESC LIMIT 5""",
                'desc': 'Top performers'
            }
            queries['bottom_performers'] = {
                'title': f'Bottom 5 {cat} by Revenue',
                'sql': f"""SELECT {cat}, ROUND(SUM({rev}), 2) AS total_revenue FROM {T}
                           GROUP BY {cat} ORDER BY total_revenue ASC LIMIT 5""",
                'desc': 'Bottom performers'
            }
            queries['having_filter'] = {
                'title': f'High Revenue {cat} — HAVING',
                'sql': f"""SELECT {cat}, ROUND(SUM({rev}), 2) AS total_revenue FROM {T}
                           GROUP BY {cat} HAVING total_revenue > (SELECT AVG(sub.total) FROM
                           (SELECT SUM({rev}) as total FROM {T} GROUP BY {cat}) sub)
                           ORDER BY total_revenue DESC""",
                'desc': 'HAVING clause filters groups above average'
            }
            queries['cte_ranked'] = {
                'title': f'CTE — Revenue Share per {cat}',
                'sql': f"""WITH revenue_totals AS (SELECT {cat}, SUM({rev}) AS cat_revenue FROM {T} GROUP BY {cat}),
                           grand_total AS (SELECT SUM({rev}) AS total FROM {T})
                           SELECT r.{cat}, ROUND(r.cat_revenue, 2) AS revenue,
                           ROUND(r.cat_revenue / g.total * 100, 2) AS revenue_share_pct
                           FROM revenue_totals r, grand_total g ORDER BY revenue DESC""",
                'desc': 'CTE to compute revenue share'
            }
            queries['window_rank'] = {
                'title': 'Window Function — RANK()',
                'sql': f"""SELECT {cat}, ROUND(SUM({rev}), 2) AS total_revenue,
                           RANK() OVER (ORDER BY SUM({rev}) DESC) AS revenue_rank
                           FROM {T} GROUP BY {cat} ORDER BY revenue_rank""",
                'desc': 'RANK() window function'
            }
            queries['window_running'] = {
                'title': 'Window Function — Running Total',
                'sql': f"""SELECT {cat}, ROUND(SUM({rev}), 2) AS revenue,
                           ROUND(SUM(SUM({rev})) OVER (ORDER BY SUM({rev}) DESC), 2) AS running_total
                           FROM {T} GROUP BY {cat} ORDER BY revenue DESC""",
                'desc': 'Cumulative SUM window function'
            }
        if cost:
            queries['profit_analysis'] = {
                'title': 'Profit Analysis',
                'sql': f"""SELECT {'`' + cat + '`' if cat else '"All"'} {'AS ' + cat if cat else ''},
                           ROUND(SUM({rev}), 2) AS revenue, ROUND(SUM({cost}), 2) AS total_cost,
                           ROUND(SUM({rev}) - SUM({cost}), 2) AS net_profit
                           FROM {T} {'GROUP BY `' + cat + '` ORDER BY net_profit DESC' if cat else ''}""",
                'desc': 'Computed profit using SQL arithmetic'
            }
        if date:
            queries['revenue_trend'] = {
                'title': f'Revenue Trend by {date}',
                'sql': f"""SELECT {date}, ROUND(SUM({rev}), 2) AS total_revenue, COUNT(*) AS transactions
                           FROM {T} GROUP BY {date} ORDER BY {date}""",
                'desc': 'Time-series aggregation'
            }
        queries['above_avg'] = {
            'title': 'Records Above Average Revenue',
            'sql': f"""SELECT * FROM {T} WHERE {rev} > (SELECT AVG({rev}) FROM {T})
                       ORDER BY {rev} DESC LIMIT 20""",
            'desc': 'Subquery inside WHERE clause'
        }
    queries['null_check'] = {
        'title': 'NULL Value Count',
        'sql': f"""SELECT {', '.join([f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_nulls" for c in columns[:6]])}
                   FROM {T}""",
        'desc': 'CASE WHEN for data quality'
    }
    queries['distinct_counts'] = {
        'title': 'Distinct Value Counts',
        'sql': f"""SELECT {', '.join([f"COUNT(DISTINCT {c}) AS {c}_unique" for c in columns[:6]])}
                   FROM {T}""",
        'desc': 'COUNT DISTINCT per column'
    }
    if qty and rev:
        queries['qty_revenue_corr'] = {
            'title': f'{qty} vs Revenue',
            'sql': f"""SELECT ROUND(AVG({qty}), 2) AS avg_qty, ROUND(AVG({rev}), 2) AS avg_revenue,
                       ROUND(SUM({rev}) / SUM({qty}), 2) AS overall_rev_per_unit
                       FROM {T} WHERE {qty} > 0""",
            'desc': f'Relationship between {qty} and {rev}'
        }
    return queries


def run_pipeline(file_bytes, filename):
    logs = []
    if filename.lower().endswith('.csv'):
        df = pd.read_csv(io.BytesIO(file_bytes), low_memory=False)
    else:
        df = pd.read_excel(io.BytesIO(file_bytes))
    logs.append(f"Loaded {len(df):,} rows × {len(df.columns)} columns")
    df.columns = [safe_col(c) for c in df.columns]
    before = len(df)
    df = df.drop_duplicates()
    logs.append(f"Removed {before - len(df):,} duplicate rows")
    filled = 0
    for col in df.columns:
        miss = df[col].isna().sum()
        if miss == 0:
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].median())
        else:
            df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else 'Unknown')
        filled += miss
    logs.append(f"Imputed {filled:,} missing values")
    rev, cost, cat, date, qty = detect_cols(df.columns.tolist())
    if rev and cost and cost in df.columns:
        df['Profit'] = pd.to_numeric(df[rev], errors='coerce') - pd.to_numeric(df[cost], errors='coerce')
        df['Profit_Margin_Pct'] = (df['Profit'] / pd.to_numeric(df[rev], errors='coerce') * 100).round(2)
        logs.append("Engineered 'Profit' and 'Profit_Margin_Pct'")
    job_id = str(uuid.uuid4())
    table_name = f"data_{job_id[:8]}"
    db_path = Path(f"db_{job_id[:8]}.db")
    load_csv_to_db(df, table_name, db_path)
    logs.append(f"Loaded into SQLite table '{table_name}'")
    rev, cost, cat, date, qty = detect_cols(df.columns.tolist(), df)
    queries = build_queries(table_name, df.columns.tolist(), rev, cost, cat, date, qty)
    results = {}
    for key, q in queries.items():
        try:
            rows, cols = run_query_sqlite(q['sql'], db_path)
            results[key] = {
                'title': q['title'], 'desc': q['desc'], 'sql': q['sql'],
                'columns': cols, 'rows': rows[:100], 'row_count': len(rows), 'error': None
            }
            logs.append(f"✓ '{q['title']}' → {len(rows)} rows")
        except Exception as e:
            results[key] = {
                'title': q['title'], 'desc': q['desc'], 'sql': q['sql'],
                'columns': [], 'rows': [], 'row_count': 0, 'error': str(e)
            }
            logs.append(f"✗ '{q['title']}' failed")
    num_cols = df.select_dtypes(include=np.number).columns.tolist()
    stats = {}
    for col in num_cols[:8]:
        s = df[col].dropna()
        stats[col] = {
            'min': clean(round(float(s.min()), 2)), 'max': clean(round(float(s.max()), 2)),
            'mean': clean(round(float(s.mean()), 2)), 'sum': clean(round(float(s.sum()), 2)),
        }
    chart = {}
    if rev and rev in df.columns:
        df[rev] = pd.to_numeric(df[rev], errors='coerce').fillna(0)
    if cat and rev and cat in df.columns:
        try:
            grp = df.groupby(cat)[rev].sum().sort_values(ascending=False).head(8)
            chart['cat_revenue'] = {'labels': list(grp.index.astype(str)), 'values': [clean(round(float(v), 2)) for v in grp.values]}
        except Exception:
            pass
    if date and rev and date in df.columns:
        try:
            grp2 = df.groupby(date)[rev].sum()
            chart['trend'] = {'labels': list(grp2.index.astype(str)), 'values': [clean(round(float(v), 2)) for v in grp2.values]}
        except Exception:
            pass
    preview = df.head(50).replace({float('nan'): None, float('inf'): None, float('-inf'): None}).to_dict(orient='records')
    return clean_dict({
        'job_id': job_id, 'filename': filename, 'db_type': 'sqlite',
        'table_name': table_name, 'shape': [len(df), len(df.columns)],
        'columns': df.columns.tolist(), 'logs': logs, 'queries': results,
        'stats': stats, 'chart': chart, 'preview': preview,
        'detected': {'revenue': rev, 'cost': cost, 'category': cat, 'date': date, 'qty': qty},
        'db_path': str(db_path)
    })


def process_upload(file_bytes, filename):
    result = run_pipeline(file_bytes, filename)
    SESSIONS[result['job_id']] = result
    return result['job_id'], result


def get_results(job_id):
    return SESSIONS.get(job_id)


def run_custom_query(job_id, sql):
    if job_id not in SESSIONS:
        return {'error': 'Session not found'}
    if not sql.strip().upper().startswith('SELECT'):
        return {'error': 'Only SELECT queries allowed'}
    try:
        db_path = SESSIONS[job_id].get('db_path')
        rows, cols = run_query_sqlite(sql, db_path)
        return clean_dict({'columns': cols, 'rows': rows[:200], 'row_count': len(rows)})
    except Exception as e:
        return {'error': str(e)}


def export_queries(job_id):
    if job_id not in SESSIONS:
        return None
    s = SESSIONS[job_id]
    lines = [f"-- SQL Analytics Report: {s['filename']}",
             f"-- Table: {s['table_name']}",
             f"-- Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n"]
    for key, q in s['queries'].items():
        if q['error']:
            continue
        lines.append(f"-- === {q['title']} ===")
        lines.append(f"-- {q['desc']}")
        lines.append(q['sql'].strip() + ";\n")
    content = "\n".join(lines)
    return io.BytesIO(content.encode())
