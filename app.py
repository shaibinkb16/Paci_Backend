from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import traceback
from reconcile_logic import extract_expense_data, extract_statement_entries, reconcile_with_llm
from s3_utils import list_s3_files, download_s3_file, upload_to_s3
import json
from reconcile_logic import reconcile_preview

app = Flask(__name__)
CORS(app)

EXPENSE_PREFIX = "expenses/"
STATEMENT_PREFIX = "statement/"
PURCHASE_JSON_KEY = "purchases/email_processing_results.json"
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

@app.route('/api/reconcile', methods=['POST'])
def reconcile_endpoint():
    try:
        # Run the preview reconciliation (returns a dict with all results)
        result = reconcile_preview()

        # Download both summaries from S3 (for redundancy, but also use result)
        saving_summary = download_s3_file(S3_BUCKET, "reconciliation/saving_account_summary.txt")
        current_summary = download_s3_file(S3_BUCKET, "reconciliation/current_account_summary.txt")
        saving_summary = saving_summary.decode("utf-8") if saving_summary else ""
        current_summary = current_summary.decode("utf-8") if current_summary else ""

        # Also get the current account report data (Excel "matched" sheet)
        current_report_bytes = download_s3_file(S3_BUCKET, "reconciliation/current_account.xlsx")
        current_report = []
        if current_report_bytes:
            import pandas as pd
            from io import BytesIO
            df = pd.read_excel(BytesIO(current_report_bytes), sheet_name="matched")
            current_report = df.to_dict(orient='records')

        return jsonify({
            "success": True,
            "saving_summary": saving_summary,
            "current_summary": current_summary,
            "saving_metrics": result['savings_metrics'],
            "current_metrics": result['current_metrics'],
            "saving_report": result['df_matched_savings'].to_dict(orient='records'),
            "current_report": current_report,
            "expenses": result['all_expenses'],
            "statements": result['all_savings_statements']
        })
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route('/api/purchases', methods=['GET'])
def get_purchases():
    try:
        json_bytes = download_s3_file(S3_BUCKET, PURCHASE_JSON_KEY)
        if not json_bytes:
            return jsonify({"error": "Purchase data not found"}), 404

        purchase_data = json.loads(json_bytes.decode('utf-8'))

        return jsonify({
            "success": True,
            "purchases": purchase_data.get("purchase_orders", []),
            "summary": purchase_data.get("summary", {})
        })
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route('/api/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']
        file_type = request.form.get('type')

        if not file_type or file_type not in ['expense', 'statement']:
            return jsonify({"error": "Invalid file type"}), 400
        if file.filename == '' or not file.filename.lower().endswith('.pdf'):
            return jsonify({"error": "Invalid PDF filename"}), 400

        prefix = EXPENSE_PREFIX if file_type == 'expense' else STATEMENT_PREFIX
        s3_key = f"{prefix}{file.filename}"

        success = upload_to_s3(file.read(), s3_key, content_type="application/pdf")
        if not success:
            return jsonify({"error": "Failed to upload to S3"}), 500

        return jsonify({
            "success": True,
            "message": "File uploaded successfully",
            "filename": file.filename
        })
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    try:
        # List all files in the bucket
        files = list_s3_files(S3_BUCKET)
        if files is None:
            return jsonify({"success": False, "message": f"Failed to connect to S3 bucket '{S3_BUCKET}'."})

        # Separate folders and files
        folders = set()
        file_list = []
        for key in files:
            parts = key.split('/')
            if len(parts) > 1:
                folders.add(parts[0] + '/')
            if not key.endswith('/'):
                file_list.append(key)

        return jsonify({
            "success": True,
            "message": f"S3 bucket '{S3_BUCKET}' connected successfully.",
            "folders": sorted(list(folders)),
            "files": file_list
        })
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    app.run( host='0.0.0.0', port=5000)