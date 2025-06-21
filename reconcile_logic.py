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
from typing import List, Dict, Optional, Union, Tuple

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

def parse_date(date_str: str) -> Optional[datetime.date]:
    """Parse date string in format 'dd-MMM-yyyy' to date object with enhanced pattern matching"""
    try:
        patterns = [
            r"(\d{1,2}-[A-Za-z]{3}-\d{4})",  # 01-Jan-2023
            r"(\d{1,2}/\d{1,2}/\d{4})",      # 01/01/2023
            r"(\w+ \d{1,2}, \d{4})"          # January 01, 2023
        ]
        for pattern in patterns:
            match = re.search(pattern, date_str.strip())
            if match:
                date_str = match.group(1)
                if "-" in date_str:
                    return datetime.strptime(date_str, "%d-%b-%Y").date()
                elif "/" in date_str:
                    return datetime.strptime(date_str, "%d/%m/%Y").date()
                else:
                    return datetime.strptime(date_str, "%B %d, %Y").date()
        print(f"❌ Date parsing failed for: {date_str} - No matching pattern found")
        return None
    except Exception as e:
        print(f"❌ Date parsing failed for: {date_str} - {e}")
        return None

def extract_expense_data(pdf_input: Union[str, bytes, bytearray]) -> List[Dict]:
    """Enhanced expense data extraction with better error handling and field validation"""
    if isinstance(pdf_input, (bytes, bytearray)):
        reader = PdfReader(BytesIO(pdf_input))
        file_name = "uploaded.pdf"
    else:
        reader = PdfReader(pdf_input)
        file_name = os.path.basename(pdf_input)
    try:
        text = ""
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += f"\nPAGE_SEPARATOR\n{page_text}"
        lines = [line.strip() for line in text.split("\n") if line.strip()]
    except Exception as e:
        print(f"❌ Failed to read PDF: {e}")
        return [{
            "parsing_failed": True,
            "file": file_name,
            "reason": f"PDF read error: {str(e)}"
        }]
    entries = []
    current_entry = {
        "file": file_name,
        "date": None,
        "description": None,
        "category": None,
        "amount": None
    }
    for line in lines:
        if line == "PAGE_SEPARATOR":
            continue
        date_match = (
            re.match(r"\d{1,2}-[A-Za-z]{3}-\d{4}", line) or 
            re.match(r"\d{1,2}/\d{1,2}/\d{4}", line)
        )
        if date_match:
            if all(current_entry.values()):
                entries.append(current_entry.copy())
            current_entry["date"] = parse_date(line)
            current_entry["description"] = None
        amount_match = re.search(
            r"(?:inr|rs|₹|usd|€|£)\s*([0-9,]+\.\d{2})|([0-9,]+\.\d{2})\s*(?:inr|rs|₹|usd|€|£)?", 
            line.lower()
        )
        if amount_match:
            amount_str = amount_match.group(1) or amount_match.group(2)
            try:
                current_entry["amount"] = float(amount_str.replace(",", ""))
            except:
                pass
        categories = ["Travel", "Meals", "Utilities", "Office Supplies", 
                     "Entertainment", "Software", "Hardware", "Miscellaneous"]
        if line in categories:
            current_entry["category"] = line
        elif current_entry["date"] and not current_entry["description"]:
            current_entry["description"] = line
    if all(current_entry.values()):
        entries.append(current_entry)
    if not entries:
        return [{
            "parsing_failed": True,
            "file": file_name,
            "reason": "No complete expense entries found (missing one or more of date/description/category/amount)"
        }]
    return entries

def extract_invoice_data(pdf_input: Union[str, bytes, bytearray]) -> List[Dict]:
    """Enhanced invoice extraction with better pattern matching"""
    if isinstance(pdf_input, (bytes, bytearray)):
        reader = PdfReader(BytesIO(pdf_input))
        file_name = "uploaded.pdf"
    else:
        reader = PdfReader(pdf_input)
        file_name = os.path.basename(pdf_input)
    try:
        text = "\n".join([page.extract_text() for page in reader.pages if page.extract_text()])
    except Exception as e:
        print(f"❌ Failed to read PDF: {e}")
        return [{
            "parsing_failed": True,
            "file": file_name,
            "reason": f"PDF read error: {str(e)}"
        }]
    invoice_no_match = re.search(
        r"(?:invoice\s*#?|no\.?)\s*([A-Z0-9-]+)", 
        text, 
        re.IGNORECASE
    )
    invoice_date_match = re.search(
        r"(?:date|invoice\s*date):?\s*([A-Za-z]+\s*\d{1,2},?\s*\d{4}|\d{1,2}-[A-Za-z]{3}-\d{4}|\d{1,2}/\d{1,2}/\d{4})", 
        text, 
        re.IGNORECASE
    )
    total_match = re.search(
        r"(?:total|amount|balance)\s*(?:due|payable)?\s*[:=]?\s*[\$₹€£]?\s*([0-9,]+\.\d{2})", 
        text, 
        re.IGNORECASE
    )
    if invoice_no_match and invoice_date_match and total_match:
        try:
            invoice_no = invoice_no_match.group(1)
            invoice_date = parse_date(invoice_date_match.group(1))
            amount = float(total_match.group(1).replace(",", ""))
            if not invoice_date:
                raise ValueError("Invalid date format")
            return [{
                "file": file_name,
                "date": invoice_date,
                "description": f"Invoice {invoice_no}",
                "category": "Accounts Payable",
                "amount": amount
            }]
        except Exception as e:
            print(f"⚠️ Invoice parsing error: {e}")
            return [{
                "parsing_failed": True,
                "file": file_name,
                "reason": f"Invoice parsing failed: {str(e)}"
            }]
    else:
        return [{
            "parsing_failed": True,
            "file": file_name,
            "reason": "Missing required invoice fields (number, date, or total)"
        }]

def extract_statement_entries(pdf_input: Union[str, bytes, bytearray]) -> List[Dict]:
    """Enhanced statement parsing with transaction type detection"""
    if isinstance(pdf_input, (bytes, bytearray)):
        reader = PdfReader(BytesIO(pdf_input))
        file_name = "uploaded.pdf"
    else:
        reader = PdfReader(pdf_input)
        file_name = os.path.basename(pdf_input)
    try:
        text = "\n".join([page.extract_text() for page in reader.pages if page.extract_text()])
        lines = [line.strip() for line in text.split("\n") if line.strip()]
    except Exception as e:
        print(f"❌ Failed to read PDF: {e}")
        return [{
            "parsing_failed": True,
            "file": file_name,
            "reason": f"PDF read error: {str(e)}"
        }]
    entries = []
    current_entry = {
        "file": file_name,
        "date": None,
        "description": None,
        "category": None,
        "type": None,
        "amount": None
    }
    for line in lines:
        date_match = (
            re.match(r"\d{1,2}-[A-Za-z]{3}-\d{4}", line) or 
            re.match(r"\d{4}-\d{2}-\d{2}", line)
        )
        if date_match:
            if all(v for k, v in current_entry.items() if k != "file"):
                entries.append(current_entry.copy())
            current_entry.update({
                "date": parse_date(line),
                "description": None,
                "category": None,
                "type": None,
                "amount": None
            })
        type_match = re.search(
            r"(debit|credit|payment|deposit|withdrawal|fee|charge|refund)", 
            line.lower()
        )
        if type_match:
            current_entry["type"] = type_match.group(1)
        amount_match = re.search(
            r"(-?\s*[\$₹€£]?\s*[0-9,]+\.\d{2})\b", 
            line
        )
        if amount_match:
            try:
                amount_str = amount_match.group(1).replace(",", "").replace(" ", "")
                current_entry["amount"] = float(amount_str)
            except:
                pass
        categories = ["Travel", "Meals", "Utilities", "Office Supplies", 
                     "Transfer", "Payment", "Service"]
        if any(cat.lower() in line.lower() for cat in categories):
            current_entry["category"] = line
        elif current_entry["date"] and not current_entry["description"]:
            current_entry["description"] = line
    if all(v for k, v in current_entry.items() if k != "file"):
        entries.append(current_entry)
    if not entries:
        return [{
            "parsing_failed": True,
            "file": file_name,
            "reason": "No complete statement entries found"
        }]
    return entries

def extract_current_account_statement(pdf_input: Union[str, bytes, bytearray]) -> List[Dict]:
    """Enhanced current account statement parser with better pattern matching"""
    if isinstance(pdf_input, (bytes, bytearray)):
        reader = PdfReader(BytesIO(pdf_input))
        file_name = "uploaded.pdf"
    else:
        reader = PdfReader(pdf_input)
        file_name = os.path.basename(pdf_input)
    try:
        text = "\n".join([page.extract_text() for page in reader.pages if page.extract_text()])
        lines = [line.strip() for line in text.split("\n") if line.strip()]
    except Exception as e:
        print(f"❌ Failed to read PDF: {e}")
        return [{
            "parsing_failed": True,
            "file": file_name,
            "reason": f"PDF read error: {str(e)}"
        }]
    entries = []
    pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2})\s+"
        r"(.*?)\s+"
        r"(INV-[A-Z0-9-]+|ACH\s+\w+|Wire\s+\w+)?\s*"
        r"(Debit|Credit)\s+"
        r"([\d,]+\.\d{2})"
    )
    for line in lines:
        match = pattern.search(line)
        if match:
            try:
                date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
                desc = match.group(2).strip()
                ref = match.group(3) or ""
                txn_type = match.group(4).lower()
                amount = float(match.group(5).replace(",", ""))
                entries.append({
                    "file": file_name,
                    "date": date,
                    "description": f"{desc} {ref}".strip(),
                    "category": "Current Account",
                    "type": txn_type,
                    "amount": amount
                })
            except Exception as e:
                print(f"⚠️ Line skipped due to parsing error: {line} — {e}")
    if not entries:
        return [{
            "parsing_failed": True,
            "file": file_name,
            "reason": "No current account transactions found"
        }]
    return entries

# === S3-based Extraction and LLM Reconciliation (Alternate/Preview Logic) ===

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
                    "type": "credit"
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

def reconcile_with_llm(expenses: List[Dict], statements: List[Dict]):
    valid_expenses = [e for e in expenses if not e.get("parsing_failed")]
    parsing_failed_expenses = [e for e in expenses if e.get("parsing_failed")]
    valid_statements = [s for s in statements if not s.get("parsing_failed")]
    if not valid_expenses or not valid_statements:
        msg = "❌ Not enough valid data for LLM reconciliation."
        upload_to_s3(msg.encode(), S3_BUCKET, "reconciliation/summary.txt", content_type="text/plain")
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
            upload_to_s3(error_text.encode(), S3_BUCKET, "reconciliation/summary.txt", content_type="text/plain")
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
    upload_to_s3(summary_text.encode("utf-8"), S3_BUCKET, "reconciliation/summary.txt", content_type="text/plain")
    with pd.ExcelWriter("llm_reconciliation_output.xlsx") as writer:
        for section in ["matched", "unmatched_expenses", "unmatched_charges", "reimbursements"]:
            pd.DataFrame(response.get(section, [])).to_excel(writer, sheet_name=section, index=False)
        for key, val in response.get("duplicates", {}).items():
            pd.DataFrame(val).to_excel(writer, sheet_name=f"duplicates_{key}", index=False)
    with open("llm_reconciliation_output.xlsx", "rb") as f:
        upload_to_s3(f.read(), S3_BUCKET, "reconciliation/llm_reconciliation_output.xlsx",
                     content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    return pd.DataFrame(response.get("matched", [])), summary_text

def reconcile_invoices_with_llm(invoices: List[Dict], current_statements: List[Dict]) -> Tuple[pd.DataFrame, str, dict]:
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.output_parsers import JsonOutputParser, StrOutputParser

    valid_invoices = [i for i in invoices if not i.get("parsing_failed")]
    valid_statements = [s for s in current_statements if not s.get("parsing_failed")]

    if not valid_invoices or not valid_statements:
        msg = "❌ Not enough valid data for LLM invoice reconciliation."
        upload_to_s3(msg.encode(), S3_BUCKET, "reconciliation/summary.txt", content_type="text/plain")
        return pd.DataFrame(), msg, {}

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

Always return valid JSON in this format:
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
        try:
            fallback_chain = prompt | ChatGroq(model="llama3-8b-8192", api_key=GROQ_API_KEY) | StrOutputParser()
            raw_output = fallback_chain.invoke({"invoices": valid_invoices, "payments": valid_statements})
            json_str = raw_output[raw_output.find('{'): raw_output.rfind('}') + 1]
            response = json.loads(json_str)
        except Exception as fallback_error:
            error_text = f"❌ Invoice LLM reconciliation failed: {fallback_error}"
            upload_to_s3(error_text.encode(), S3_BUCKET, "reconciliation/summary.txt", content_type="text/plain")
            return pd.DataFrame(), error_text, {}

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
    upload_to_s3(summary_text.encode("utf-8"), S3_BUCKET, "reconciliation/summary.txt", content_type="text/plain")

    # --- FIX: Always write at least one sheet ---
    with pd.ExcelWriter("llm_invoice_reconciliation.xlsx") as writer:
        wrote_sheet = False
        for section in ["matched", "unmatched_invoices", "unmatched_payments", "reimbursements"]:
            df = pd.DataFrame(response.get(section, []))
            if not df.empty:
                df.to_excel(writer, sheet_name=section, index=False)
                wrote_sheet = True
        for key, val in response.get("duplicates", {}).items():
            df = pd.DataFrame(val)
            if not df.empty:
                df.to_excel(writer, sheet_name=f"duplicates_{key}", index=False)
                wrote_sheet = True
        if not wrote_sheet:
            pd.DataFrame([{"info": "No data"}]).to_excel(writer, sheet_name="empty", index=False)
    # --- END FIX ---

    with open("llm_invoice_reconciliation.xlsx", "rb") as f:
        upload_to_s3(f.read(), S3_BUCKET, "reconciliation/llm_invoice_reconciliation.xlsx",
                     content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    return pd.DataFrame(response.get("matched", [])), summary_text, response

def reconcile_current_account() -> str:
    print("\n=== 🔄 Reconciling Invoices with Current Account ===")
    invoice_keys = list_s3_files(S3_BUCKET, "invoices/")
    all_invoices = []
    for key in invoice_keys:
        print(f"📥 Extracting from Invoice File: {key}")
        all_invoices.extend(extract_invoice_data_from_s3(key))
    print(f"✅ Total invoice entries extracted: {len(all_invoices)}")
    statement_keys = list_s3_files(S3_BUCKET, "statement/")
    current_keys = [k for k in statement_keys if "current" in k.lower()]
    all_current_statements = []
    for key in current_keys:
        print(f"🏦 Extracting from Current Account Statement File: {key}")
        all_current_statements.extend(extract_current_account_entries_from_s3(key))
    print(f"✅ Total current account entries extracted: {len(all_current_statements)}")
    print("\n🧾 Parsed Invoices:")
    for entry in all_invoices:
        print(entry)
    print("\n🏦 Parsed Current Account Entries:")
    for entry in all_current_statements:
        print(entry)
    print("🤖 Running LLM reconciliation for Current Account + Invoices...")
    df_matched, summary, response = reconcile_invoices_with_llm(all_invoices, all_current_statements)
    print("\n📊 === INVOICE RECONCILIATION SUMMARY ===")
    print(summary.strip())
    clean_summary = f"--- CURRENT ACCOUNT SUMMARY ---\n\n{summary.strip()}"
    upload_to_s3(clean_summary.encode("utf-8"), S3_BUCKET, "reconciliation/current_account_summary.txt", content_type="text/plain")
    print("✅ Uploaded to S3: reconciliation/current_account_summary.txt")
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
    upload_to_s3(out_stream.getvalue(), S3_BUCKET, excel_key,
                 content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    print("✅ Uploaded to S3: reconciliation/current_account.xlsx")
    return summary

def reconcile_preview():
    EXPENSE_FOLDER = "expenses/"
    STATEMENT_FOLDER = "statement/"
    print("🔄 Starting LLM-based reconciliation...")

    # Extract data
    expense_keys = list_s3_files(S3_BUCKET, EXPENSE_FOLDER)
    all_expenses = []
    for key in expense_keys:
        print(f"📥 Extracting from Expense File: {key}")
        all_expenses.extend(extract_expense_data_from_s3(key))
    print(f"✅ Total expense entries extracted: {len(all_expenses)}")

    all_statement_keys = list_s3_files(S3_BUCKET, STATEMENT_FOLDER)
    savings_keys = [k for k in all_statement_keys if "saving" in k.lower()]
    print(f"📄 Found {len(savings_keys)} 'saving' statement file(s): {savings_keys}")

    all_savings_statements = []
    for key in savings_keys:
        print(f"🏦 Extracting from Savings Statement File: {key}")
        all_savings_statements.extend(extract_statement_entries_from_s3(key))
    print(f"✅ Total savings account entries extracted: {len(all_savings_statements)}")

    # Run reconciliations
    print("🤖 Running LLM reconciliation for Savings + Expenses...")
    df_matched_savings, summary_savings = reconcile_with_llm(all_expenses, all_savings_statements)

    print("\n🔁 Now starting CURRENT ACCOUNT + INVOICE reconciliation...")
    summary_current = reconcile_current_account()

    # --- Metrics Calculation Functions ---
    def calculate_savings_metrics(summary_text):
        metrics = {
            'Total Expense Bills': 0,
            'Matched with Bank Statement': 0,
            'Unmatched Expense Bills': 0,
            'Unmatched Bank Debits': 0
        }
        if not summary_text:
            return metrics
        sections = {
            '✅ Matched:': 'Matched with Bank Statement',
            '❌ Unmatched Expenses:': 'Unmatched Expense Bills',
            '❌ Unmatched Charges:': 'Unmatched Bank Debits'
        }
        current_section = None
        for line in summary_text.split('\n'):
            line = line.strip()
            if line in sections:
                current_section = sections[line]
            elif line.startswith('• ₹') or line.startswith('  • ₹'):
                if current_section:
                    metrics[current_section] += 1
        metrics['Total Expense Bills'] = (
            metrics['Matched with Bank Statement'] +
            metrics['Unmatched Expense Bills']
        )
        return metrics

    def calculate_current_metrics(summary_text):
        metrics = {
            'Total Expense Bills': 0,
            'Matched with Bank Statement': 0,
            'Unmatched Expense Bills': 0,
            'Unmatched Bank Debits': 0
        }
        if not summary_text:
            return metrics
        sections = {
            '✅ Matched:': 'Matched with Bank Statement',
            '❌ Unmatched Invoices:': 'Unmatched Expense Bills',
            '❌ Unmatched Payments:': 'Unmatched Bank Debits'
        }
        current_section = None
        for line in summary_text.split('\n'):
            line = line.strip()
            if line in sections:
                current_section = sections[line]
            elif line.startswith('• ₹') or line.startswith('  • ₹'):
                if current_section:
                    metrics[current_section] += 1
        metrics['Total Expense Bills'] = (
            metrics['Matched with Bank Statement'] +
            metrics['Unmatched Expense Bills']
        )
        return metrics

    # Get the metrics
    savings_metrics = calculate_savings_metrics(summary_savings)
    current_metrics = calculate_current_metrics(summary_current)

    # Upload summaries
    upload_to_s3(summary_savings.encode("utf-8"), S3_BUCKET, "reconciliation/saving_account_summary.txt", content_type="text/plain")
    upload_to_s3(summary_current.encode("utf-8"), S3_BUCKET, "reconciliation/current_account_summary.txt", content_type="text/plain")

    print("\n✅ Reconciliation completed.")

    return {
        'df_matched_savings': df_matched_savings,
        'summary_savings': summary_savings,
        'summary_current': summary_current,
        'all_expenses': all_expenses,
        'all_savings_statements': all_savings_statements,
        'savings_metrics': savings_metrics,
        'current_metrics': current_metrics
    }

