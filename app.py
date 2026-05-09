"""
Data Mining Pro — Unified Web Application
Merges 4 projects: AI Data Analyst, Smart ETL, BI Reporter, SQL Analytics Engine
Flask backend serving all modules via a single interface
"""
# maim py
import os, io, traceback
from pathlib import Path
from flask import Flask, request, jsonify, send_file

# Import modules like analyst elt bi and sql 
from modules import ai_analyst, etl_engine, bi_reporter, sql_engine

app = Flask(__name__, static_folder='static', static_url_path='/static')
app.secret_key = os.getenv('SECRET_KEY', 'datamining-pro-secret-key-2024')
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB


# ══════════════════════════════════════════
# MAIN ROUTE — Serve the unified frontend
# ══════════════════════════════════════════
@app.route('/')
def home():
    html_path = Path(__file__).parent / 'index.html'
    if html_path.exists():
        return html_path.read_text(encoding='utf-8')
    return "Data Mining Pro is running!"


# ══════════════════════════════════════════
# MODULE 1: AI DATA ANALYST
# ══════════════════════════════════════════
@app.route('/ai/upload', methods=['POST'])
def ai_upload():
    try:
        file = request.files['file']
        result = ai_analyst.process_upload(file)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/ai/chart', methods=['POST'])
def ai_chart():
    data = request.json
    chart_type = data.get('chart_type', 'bar')
    result = ai_analyst.get_chart(chart_type)
    return jsonify(result)


@app.route('/ai/chat', methods=['POST'])
def ai_chat():
    data = request.json
    message = data.get('message', '')
    result = ai_analyst.get_chat_response(message)
    return jsonify(result)


@app.route('/ai/download/<fmt>')
def ai_download(fmt):
    result = ai_analyst.download_cleaned(fmt)
    if result is None:
        return "No data available", 404
    output, mimetype, filename = result
    return send_file(output, mimetype=mimetype, as_attachment=True, download_name=filename)


# ══════════════════════════════════════════
# MODULE 2: SMART ETL ENGINE
# ══════════════════════════════════════════
@app.route('/etl/upload', methods=['POST'])
def etl_upload():
    try:
        file = request.files['file']
        if not file.filename.lower().endswith('.csv'):
            return jsonify({"error": "Only CSV files supported for ETL"}), 400
        raw = file.read()
        if len(raw) == 0:
            return jsonify({"error": "Empty file"}), 400
        job_id, result = etl_engine.process_upload(raw, file.filename)
        return jsonify({"job_id": job_id, "status": "complete"})
    except Exception as e:
        return jsonify({"error": traceback.format_exc()}), 500


@app.route('/etl/results/<job_id>')
def etl_results(job_id):
    result = etl_engine.get_results(job_id)
    if result is None:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(result)


@app.route('/etl/download/<job_id>')
def etl_download(job_id):
    path = etl_engine.get_download_path(job_id)
    if path is None or not path.exists():
        return jsonify({"error": "File not found"}), 404
    return send_file(path, media_type="text/csv", filename=f"cleaned_{job_id[:8]}.csv")


# ══════════════════════════════════════════
# MODULE 3: BI REPORTER
# ══════════════════════════════════════════
@app.route('/bi/upload', methods=['POST'])
def bi_upload():
    try:
        file = request.files['file']
        allowed = {'.csv', '.xlsx', '.xls'}
        if not any(file.filename.lower().endswith(e) for e in allowed):
            return jsonify({'error': 'Only CSV and Excel files supported'}), 400
        file_bytes = file.read()
        job_id, result = bi_reporter.process_upload(file_bytes, file.filename)
        return jsonify({'job_id': job_id, 'status': 'complete'})
    except Exception as e:
        return jsonify({'error': traceback.format_exc()}), 500


@app.route('/bi/results/<job_id>')
def bi_results(job_id):
    result = bi_reporter.get_results(job_id)
    if result is None:
        return jsonify({'error': 'Not found'}), 404
    return jsonify(result)


@app.route('/bi/download/<job_id>')
def bi_download(job_id):
    path = bi_reporter.get_download_path(job_id)
    if path is None or not path.exists():
        return jsonify({'error': 'Report not found'}), 404
    return send_file(path, as_attachment=True,
                     download_name=f"BI_Report_{job_id[:8]}.xlsx",
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


# ══════════════════════════════════════════
# MODULE 4: SQL ANALYTICS ENGINE
# ══════════════════════════════════════════
@app.route('/sql/upload', methods=['POST'])
def sql_upload():
    try:
        file = request.files['file']
        allowed = {'.csv', '.xlsx', '.xls'}
        if not any(file.filename.lower().endswith(e) for e in allowed):
            return jsonify({'error': 'CSV or Excel only'}), 400
        file_bytes = file.read()
        job_id, result = sql_engine.process_upload(file_bytes, file.filename)
        return jsonify({'job_id': job_id, 'status': 'complete'})
    except Exception as e:
        return jsonify({'error': traceback.format_exc()}), 500


@app.route('/sql/results/<job_id>')
def sql_results(job_id):
    result = sql_engine.get_results(job_id)
    if result is None:
        return jsonify({'error': 'Not found'}), 404
    return jsonify(result)


@app.route('/sql/query/<job_id>', methods=['POST'])
def sql_custom_query(job_id):
    sql = request.json.get('sql', '').strip()
    if not sql:
        return jsonify({'error': 'No SQL provided'}), 400
    result = sql_engine.run_custom_query(job_id, sql)
    if 'error' in result:
        return jsonify(result), 400 if result['error'] != 'Session not found' else 404
    return jsonify(result)


@app.route('/sql/export/<job_id>')
def sql_export(job_id):
    buf = sql_engine.export_queries(job_id)
    if buf is None:
        return jsonify({'error': 'Not found'}), 404
    return send_file(buf, as_attachment=True,
                     download_name=f"queries_{job_id[:8]}.sql",
                     mimetype='text/plain')


# ══════════════════════════════════════════
# HEALTH CHECK
# ══════════════════════════════════════════
@app.route('/health')
def health():
    return jsonify({
        'status': 'ok',
        'modules': ['ai_analyst', 'etl_engine', 'bi_reporter', 'sql_engine'],
        'version': '1.0.0'
    })


@app.route('/api/project-status')
def project_status():
    root = Path(__file__).parent
    runtime_items = {
        'etl_output': root / 'etl_output',
        'reports': root / 'reports',
        'sql_reports': root / 'sql_reports',
    }
    project_files = ['app.py', 'index.html', 'requirements.txt', 'Dockerfile']
    data_files = list(root.glob('*.csv')) + list(root.glob('*.db'))

    return jsonify({
        'status': 'ready',
        'project_files': {
            name: (root / name).exists() for name in project_files
        },
        'runtime_folders': {
            name: path.exists() for name, path in runtime_items.items()
        },
        'local_data_files': [
            {
                'name': path.name,
                'size_mb': round(path.stat().st_size / (1024 * 1024), 2)
            }
            for path in data_files[:12]
        ],
        'modules': [
            {'id': 'ai', 'name': 'AI Data Analyst', 'formats': ['CSV', 'XLSX', 'XLS']},
            {'id': 'etl', 'name': 'Smart ETL', 'formats': ['CSV']},
            {'id': 'bi', 'name': 'BI Reporter', 'formats': ['CSV', 'XLSX', 'XLS']},
            {'id': 'sql', 'name': 'SQL Analytics', 'formats': ['CSV', 'XLSX', 'XLS']},
        ]
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000, debug=False)
