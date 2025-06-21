import os
import re
import json
import pandas as pd
from io import BytesIO
from pypdf import PdfReader
from datetime import datetime
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from s3_utils import list_s3_files, download_s3_file, upload_to_s3
from typing import List, Dict, Tuple
from openpyxl.utils.dataframe import dataframe_to_rows

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

# === PDF Parsing ===
def parse_date(date_str):
    try:
        return datetime.strptime(date_str.strip(), "%d-%b-%Y").date()
    except Exception as e:
        print(f"❌ Date parsing failed for: {date_str} - {e}")
        return None

def extract_expense_data_from_s3(key: str) -> List[Dict]:
    pdf_bytes = download_s3_file(S3_BUCKET, key)
    if not pdf_bytes:
        return []

    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        text = "\n".join([page.extract_text() for page in reader.pages])
        lines = [line.strip() for line in text.split("\n") if line.strip()]
    except Exception as e:
        print(f"Failed to read PDF {key}: {e}")
        return []

    entries = []
    date, description, category, amount = None, None, None, None

    for line in lines:
        if re.match(r"\d{1,2}-[A-Za-z]{3}-\d{4}", line):
            date = parse_date(line)
        elif re.search(r"inr\s*[0-9]+\.[0-9]{2}", line.lower()):
            amt_match = re.search(r"([0-9]+\.[0-9]{2})", line)
            if amt_match:
                amount = float(amt_match.group(1))
        elif line in ["Travel", "Meals", "Utilities", "Office Supplies"]:
            category = line
        elif date and not description:
            description = line

        if all([date, description, category, amount]):
            entries.append({
                "file": key.split("/")[-1],
                "date": str(date),
                "description": description,
                "category": category,
                "amount": amount
            })
            date, description, category, amount = None, None, None, None

    if not entries:
        entries.append({
            "parsing_failed": True,
            "file": key.split("/")[-1],
            "reason": "Could not extract complete expense entry",
            "amount": amount,
            "description": description or "Unknown",
        })
    return entries

def extract_statement_entries_from_s3(key: str) -> List[Dict]:
    pdf_bytes = download_s3_file(S3_BUCKET, key)
    if not pdf_bytes:
        return []

    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        text = "\n".join([page.extract_text() for page in reader.pages])
        lines = [line.strip() for line in text.split("\n") if line.strip()]
    except Exception as e:
        print(f"Failed to read PDF {key}: {e}")
        return []

    entries = []
    date, description, category, type_, amount = None, None, None, None, None

    for line in lines:
        if re.match(r"\d{1,2}-[A-Za-z]{3}-\d{4}", line):
            date = parse_date(line)
        elif line in ["Travel", "Meals", "Utilities", "Office Supplies", "Cash", "Personal", "Charges"]:
            category = line
        elif line.lower() in ["debit", "credit", "fee"]:
            type_ = line.lower()
        elif re.match(r"-?[0-9]+\.[0-9]{2}$", line):
            amount = float(line)
        elif date and not description:
            description = line

        if all([date, description, category, type_, amount is not None]):
            entries.append({
                "date": str(date),
                "description": description,
                "category": category,
                "type": type_,
                "amount": amount
            })
            date, description, category, type_, amount = None, None, None, None, None

    if not entries:
        entries.append({
            "parsing_failed": True,
            "file": key.split("/")[-1],
            "reason": "Could not extract complete statement entry"
        })
    return entries

# === LLM Reconciliation ===
def reconcile_with_llm(expenses: List[Dict], statements: List[Dict]):
    valid_expenses = [e for e in expenses if not e.get("parsing_failed")]
    parsing_failed_expenses = [e for e in expenses if e.get("parsing_failed")]
    valid_statements = [s for s in statements if not s.get("parsing_failed")]

    if not valid_expenses or not valid_statements:
        msg = "❌ Not enough valid data for LLM reconciliation."
        upload_to_s3(msg.encode(), "reconciliation/summary.txt", content_type="text/plain")
        return pd.DataFrame(), msg

    parser = JsonOutputParser()

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a smart and strict finance assistant. Your job is to reconcile two lists of financial records by thinking step by step and returning only valid JSON. No markdown or commentary."),

        ("user", """
    You are given:
    Expense Entries:
    [
    {{ "date": "2024-12-01", "amount": 100.0, "description": "cab fare to airport" }},
    {{ "date": "2024-12-01", "amount": 100.0, "description": "Cab Fare to Airport" }}
    ]

    Statement Entries:
    [
    {{ "date": "2024-12-01", "amount": 100.0, "description": "Uber Trip to airport" }}
    ]

    Reconcile them.

    Think step by step:
    - Both expenses are identical → first is valid, second is a duplicate.
    - One matching statement → it matches the valid expense.
    - No unmatched charges.
    - No reimbursements.
    - One duplicate expense.

    Return:
    {{
    "summary": "1 match, 1 duplicate expense.",
    "matched": [{{ "date": "2024-12-01", "amount": 100.0, "description": "cab fare to airport" }}],
    "unmatched_expenses": [],
    "unmatched_charges": [],
    "reimbursements": [],
    "duplicates": {{
        "expenses": [{{ "date": "2024-12-01", "amount": 100.0, "description": "Cab Fare to Airport" }}],
        "statements": []
    }}
    }}
    """),

        ("user", """
    You are given two lists:
    1. `expenses`: extracted entries from employee-submitted expense bills.
    2. `statements`: bank statement transactions.

    Reconcile them using these rules:
    - ✅ Match if `date` and `amount` are exact, and description is semantically similar.
    - 🔁 Mark identical entries (same `date`, `amount`, `description`) beyond the first as `duplicates`.
    - ❌ Do NOT include duplicates in `matched` or `unmatched`.
    - 🟢 Statement entries with negative amounts are reimbursements.

    Return only valid JSON in this format:
    {{
    "summary": "...",
    "matched": [...],
    "unmatched_expenses": [...],
    "unmatched_charges": [...],
    "reimbursements": [...],
    "duplicates": {{
        "expenses": [...],
        "statements": [...]
    }}
    }}

    Expense Entries:
    {expenses}

    Statement Entries:
    {statements}
    """)
    ])

    chain = prompt | ChatGroq(model="llama3-8b-8192", api_key=GROQ_API_KEY) | parser

    try:
        response = chain.invoke({"expenses": valid_expenses, "statements": valid_statements})
    except Exception as e:
        try:
            fallback_chain = prompt | ChatGroq(model="llama3-8b-8192", api_key=GROQ_API_KEY) | StrOutputParser()
            raw_output = fallback_chain.invoke({"expenses": valid_expenses, "statements": valid_statements})
            json_str = raw_output[raw_output.find('{'): raw_output.rfind('}') + 1]
            response = json.loads(json_str)
        except Exception as fallback_error:
            error_text = f"❌ LLM reconciliation failed: {fallback_error}"
            upload_to_s3(error_text.encode(), "reconciliation/summary.txt", content_type="text/plain")
            return pd.DataFrame(), error_text

    for e in parsing_failed_expenses:
        response.setdefault("unmatched_expenses", []).append({
            "date": "UNKNOWN DATE",
            "amount": e.get("amount", "N/A"),
            "description": f"{e.get('description', 'N/A')} (parsing failed)"
        })

    summary_lines = []
    def log_section(title, entries):
        if entries:
            summary_lines.append(title)
            for e in entries:
                amount = e.get("amount", "N/A")
                date = e.get("date", "N/A")
                desc = e.get("description", "N/A")
                summary_lines.append(f"  • ₹{amount} on {date} - {desc}")
            summary_lines.append("")

    log_section("✅ Matched:", response.get("matched", []))
    log_section("❌ Unmatched Expenses:", response.get("unmatched_expenses", []))
    log_section("❌ Unmatched Charges:", response.get("unmatched_charges", []))
    log_section("🟢 Reimbursements:", response.get("reimbursements", []))
    log_section("🔁 Duplicates in Expenses:", response.get("duplicates", {}).get("expenses", []))
    log_section("🔁 Duplicates in Statements:", response.get("duplicates", {}).get("statements", []))

    summary_text = "\n".join(summary_lines) or response.get("summary", "No summary returned.")
    upload_to_s3(summary_text.encode("utf-8"), "reconciliation/summary.txt", content_type="text/plain")

    with pd.ExcelWriter("llm_reconciliation_output.xlsx") as writer:
        for section in ["matched", "unmatched_expenses", "unmatched_charges", "reimbursements"]:
            pd.DataFrame(response.get(section, [])).to_excel(writer, sheet_name=section, index=False)
        for key, val in response.get("duplicates", {}).items():
            pd.DataFrame(val).to_excel(writer, sheet_name=f"duplicates_{key}", index=False)

    with open("llm_reconciliation_output.xlsx", "rb") as f:
        upload_to_s3(f.read(), "reconciliation/llm_reconciliation_output.xlsx",
                     content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    return pd.DataFrame(response.get("matched", [])), summary_text

# === Main to run independently ===
def reconcile_preview():
    EXPENSE_FOLDER = "expenses/"
    STATEMENT_FOLDER = "statement/"

    print("🔄 Starting LLM-based reconciliation...")

    # 1. Savings Account Reconciliation
    expense_keys = list_s3_files(EXPENSE_FOLDER)
    all_expenses = []
    for key in expense_keys:
        print(f"📥 Extracting from Expense File: {key}")
        all_expenses.extend(extract_expense_data_from_s3(key))
    print(f"✅ Total expense entries extracted: {len(all_expenses)}")

    all_statement_keys = list_s3_files(STATEMENT_FOLDER)
    savings_keys = [k for k in all_statement_keys if "saving" in k.lower()]
    print(f"📄 Found {len(savings_keys)} 'saving' statement file(s): {savings_keys}")

    all_savings_statements = []
    for key in savings_keys:
        print(f"🏦 Extracting from Savings Statement File: {key}")
        all_savings_statements.extend(extract_statement_entries_from_s3(key))
    print(f"✅ Total savings account entries extracted: {len(all_savings_statements)}")

    print("🤖 Running LLM reconciliation for Savings + Expenses...")
    df_matched_savings, summary_savings = reconcile_with_llm(all_expenses, all_savings_statements)

    print("\n📊 === SAVINGS ACCOUNT RECONCILIATION SUMMARY ===")
    print(summary_savings.strip())

    # ✅ Save savings summary only
    upload_to_s3(summary_savings.encode("utf-8"), "reconciliation/saving_account_summary.txt", content_type="text/plain")
    print("✅ Uploaded to S3: reconciliation/saving_account_summary.txt")

    # 2. Current Account Reconciliation
    print("\n🔁 Now starting CURRENT ACCOUNT + INVOICE reconciliation...")
    summary_current = reconcile_current_account()

    # ✅ Save current account summary only
    upload_to_s3(summary_current.encode("utf-8"), "reconciliation/current_account_summary.txt", content_type="text/plain")
    print("✅ Uploaded to S3: reconciliation/current_account_summary.txt")

    print("\n📄 === CURRENT ACCOUNT SUMMARY ===")
    print(summary_current.strip())

    print("\n✅ Reconciliation completed.")
    return df_matched_savings, summary_current, all_expenses, all_savings_statements




def reconcile_invoices_with_llm(invoices: List[Dict], current_statements: List[Dict]) -> Tuple[pd.DataFrame, str]:
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.output_parsers import JsonOutputParser, StrOutputParser

    valid_invoices = [i for i in invoices if not i.get("parsing_failed")]
    valid_statements = [s for s in current_statements if not s.get("parsing_failed")]

    if not valid_invoices or not valid_statements:
        msg = "❌ Not enough valid data for LLM invoice reconciliation."
        upload_to_s3(msg.encode(), "reconciliation/summary.txt", content_type="text/plain")
        return pd.DataFrame(), msg

    parser = JsonOutputParser()

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a strict financial assistant trained to reconcile invoices with current account payments. Always output strict JSON. No markdown, no explanations."),
        ("user", """
You are given two lists:
1. `invoices`: system-generated invoice entries.
2. `payments`: credit entries from the current account.

Your task is to reconcile invoice payments using this logic:
- ✅ Match if `amount` is the same and `description` semantically links invoice number or vendor.
- 📅 Dates may differ by up to 3 days.
- 🔁 Detect and separate duplicates (same invoice repeated).
- ❌ Do NOT count duplicates in `matched`.
- 🟢 Statement entries with no corresponding invoice may be reimbursements.
- Always return valid JSON in this format:

{{
  "summary": "...",
  "matched": [...],
  "unmatched_invoices": [...],
  "unmatched_payments": [...],
  "reimbursements": [...],
  "duplicates": {{
    "invoices": [...],
    "statements": [...]
  }}
}}

Example:

Invoices:
[
  {{ "date": "2025-06-20", "amount": 27033.29, "description": "Invoice INV-20250620-996A7766" }}
]

Payments:
[
  {{ "date": "2025-06-22", "amount": 27033.29, "description": "ACH Payment - Omkar Mestry", "type": "credit" }}
]
"""),
        ("user", """
Invoices:
{invoices}

Payments:
{payments}
""")
    ])

    chain = prompt | ChatGroq(model="llama3-8b-8192", api_key=GROQ_API_KEY) | parser

    try:
        response = chain.invoke({
            "invoices": valid_invoices,
            "payments": valid_statements
        })
    except Exception as e:
        # fallback to loose parsing
        try:
            fallback_chain = prompt | ChatGroq(model="llama3-8b-8192", api_key=GROQ_API_KEY) | StrOutputParser()
            raw_output = fallback_chain.invoke({"invoices": valid_invoices, "payments": valid_statements})
            json_str = raw_output[raw_output.find('{'): raw_output.rfind('}') + 1]
            response = json.loads(json_str)
        except Exception as fallback_error:
            error_text = f"❌ Invoice LLM reconciliation failed: {fallback_error}"
            upload_to_s3(error_text.encode(), "reconciliation/summary.txt", content_type="text/plain")
            return pd.DataFrame(), error_text

    # Summary formatting
    summary_lines = []
    def log_section(title, entries):
        if entries:
            summary_lines.append(title)
            for e in entries:
                amount = e.get("amount", "N/A")
                date = e.get("date", "N/A")
                desc = e.get("description", "N/A")
                summary_lines.append(f"  • ₹{amount} on {date} - {desc}")
            summary_lines.append("")

    log_section("✅ Matched:", response.get("matched", []))
    log_section("❌ Unmatched Invoices:", response.get("unmatched_invoices", []))
    log_section("❌ Unmatched Payments:", response.get("unmatched_payments", []))
    log_section("🟢 Reimbursements:", response.get("reimbursements", []))
    log_section("🔁 Duplicates in Invoices:", response.get("duplicates", {}).get("invoices", []))
    log_section("🔁 Duplicates in Statements:", response.get("duplicates", {}).get("statements", []))

    summary_text = "\n".join(summary_lines) or response.get("summary", "No summary returned.")
    upload_to_s3(summary_text.encode("utf-8"), "reconciliation/summary.txt", content_type="text/plain")

    with pd.ExcelWriter("llm_invoice_reconciliation.xlsx") as writer:
        for section in ["matched", "unmatched_invoices", "unmatched_payments", "reimbursements"]:
            pd.DataFrame(response.get(section, [])).to_excel(writer, sheet_name=section, index=False)
        for key, val in response.get("duplicates", {}).items():
            pd.DataFrame(val).to_excel(writer, sheet_name=f"duplicates_{key}", index=False)

    with open("llm_invoice_reconciliation.xlsx", "rb") as f:
        upload_to_s3(f.read(), "reconciliation/llm_invoice_reconciliation.xlsx",
                     content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    return pd.DataFrame(response.get("matched", [])), summary_text, response



def extract_invoice_data_from_s3(key: str) -> List[Dict]:
    pdf_bytes = download_s3_file(S3_BUCKET, key)
    if not pdf_bytes:
        return []

    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        text = "\n".join([page.extract_text() for page in reader.pages])
    except Exception as e:
        print(f"❌ Failed to read invoice PDF {key}: {e}")
        return []

    # Extract invoice entries
    entries = []
    invoice_no_match = re.search(r"Invoice Number:\s*(\S+)", text)
    invoice_date_match = re.search(r"Invoice Date:\s*([A-Za-z]+ \d{1,2}, \d{4})", text)
    total_match = re.search(r"TOTAL:\s*\$([0-9,.]+)", text)

    if invoice_no_match and invoice_date_match and total_match:
        try:
            invoice_no = invoice_no_match.group(1)
            invoice_date = datetime.strptime(invoice_date_match.group(1), "%B %d, %Y").date()
            amount = float(total_match.group(1).replace(",", ""))
            entries.append({
                "date": str(invoice_date),
                "description": f"Invoice {invoice_no}",
                "amount": amount
            })
        except Exception as e:
            print(f"⚠️ Invoice parsing failed for {key}: {e}")
    else:
        print(f"⚠️ Missing fields in invoice PDF {key}")
    return entries

def extract_current_account_entries_from_s3(key: str) -> List[Dict]:
    pdf_bytes = download_s3_file(S3_BUCKET, key)
    if not pdf_bytes:
        return []

    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        text = "\n".join([page.extract_text() for page in reader.pages])
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        print("\n🧾 RAW TEXT FROM CURRENT ACCOUNT STATEMENT:")
        print(text[:2000])
    except Exception as e:
        print(f"❌ Failed to read PDF {key}: {e}")
        return []

    entries = []

    for line in lines:
        match = re.match(
            r"^(\d{4}-\d{2}-\d{2})\s+(.+?)\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})$",
            line
        )
        if match:
            date, description, amount_str, balance_str = match.groups()
            try:
                amount = float(amount_str.replace(",", ""))
                balance = float(balance_str.replace(",", ""))
                entries.append({
                    "date": date,
                    "description": description.strip(),
                    "amount": amount,
                    "balance": balance,
                    "type": "credit"  # Assume all payments related to invoices are credit
                })
            except:
                continue

    if not entries:
        entries.append({
            "parsing_failed": True,
            "file": key.split("/")[-1],
            "reason": "Could not extract complete statement entry"
        })

    return entries


import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows

def reconcile_current_account() -> str:
    print("\n=== 🔄 Reconciling Invoices with Current Account ===")

    # Step 1: Load invoices
    invoice_keys = list_s3_files("invoices/")
    all_invoices = []
    for key in invoice_keys:
        print(f"📥 Extracting from Invoice File: {key}")
        all_invoices.extend(extract_invoice_data_from_s3(key))
    print(f"✅ Total invoice entries extracted: {len(all_invoices)}")

    # Step 2: Load current account statements
    statement_keys = list_s3_files("statement/")
    current_keys = [k for k in statement_keys if "current" in k.lower()]
    all_current_statements = []
    for key in current_keys:
        print(f"🏦 Extracting from Current Account Statement File: {key}")
        all_current_statements.extend(extract_current_account_entries_from_s3(key))
    print(f"✅ Total current account entries extracted: {len(all_current_statements)}")

    # Debug print parsed entries
    print("\n🧾 Parsed Invoices:")
    for entry in all_invoices:
        print(entry)

    print("\n🏦 Parsed Current Account Entries:")
    for entry in all_current_statements:
        print(entry)

    # Step 3: Reconcile
    print("🤖 Running LLM reconciliation for Current Account + Invoices...")
    df_matched, summary, response = reconcile_invoices_with_llm(all_invoices, all_current_statements)
    print("\n📊 === INVOICE RECONCILIATION SUMMARY ===")
    print(summary.strip())

    # ✅ Step 4: Upload separate summary
    clean_summary = f"--- CURRENT ACCOUNT SUMMARY ---\n\n{summary.strip()}"
    upload_to_s3(clean_summary.encode("utf-8"), "reconciliation/current_account_summary.txt", content_type="text/plain")
    print("✅ Uploaded to S3: reconciliation/current_account_summary.txt")

    # ✅ Step 5: Create new Excel (not modifying any existing workbook)
    excel_key = "reconciliation/current_account.xlsx"
    out_stream = BytesIO()

    with pd.ExcelWriter(out_stream, engine="openpyxl", mode="w") as writer:
        def safe_write(sheet_name, data):
            df = pd.DataFrame(data)
            if not df.empty:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        safe_write("matched", response.get("matched", []))
        safe_write("unmatched_invoices", response.get("unmatched_invoices", []))
        safe_write("unmatched_payments", response.get("unmatched_payments", []))
        safe_write("reimbursements", response.get("reimbursements", []))
        safe_write("duplicates_invoices", response.get("duplicates", {}).get("invoices", []))
        safe_write("duplicates_statements", response.get("duplicates", {}).get("statements", []))

    upload_to_s3(out_stream.getvalue(), excel_key,
                 content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    print("✅ Uploaded to S3: reconciliation/current_account.xlsx")

    return summary




#reconcile_preview()
