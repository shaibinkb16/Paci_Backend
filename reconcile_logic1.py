import os
import re
import json
import json5
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
        return [{
            "parsing_failed": True,
            "file": key.split("/")[-1],
            "reason": "Could not extract complete statement entry"
        }]
    return entries

def reconcile_invoices_with_llm(invoices: List[Dict], current_statements: List[Dict]) -> Tuple[pd.DataFrame, str, dict]:
    valid_invoices = [i for i in invoices if not i.get("parsing_failed")]
    valid_statements = [s for s in current_statements if not s.get("parsing_failed")]

    parser = JsonOutputParser()
    prompt = ChatPromptTemplate.from_messages([
        ("system", r"""
You are a strict, detail-oriented financial assistant trained to reconcile merchant invoices with credit merchant settlement entries (from a current account).

==========================
🧠 CHAIN-OF-THOUGHT LOGIC:
==========================

Step 1️⃣: Load all valid invoices and valid statements.

Step 2️⃣: Identify and separate duplicates in both:
- A duplicate is defined as a record with the same `amount`, same `date`, and same `description`.
- The first occurrence of each unique combination is considered original, all others are duplicates.
- Compare field values exactly to identify duplicates, not object references.
- Duplicates MUST be added to the duplicates.invoices and duplicates.statements lists.


Step 3️⃣: Match invoices to settlement entries:
✅ A match is valid only if:
- Amount matches **exactly**
- Invoice number (from invoice `description`) is **found as substring** (case-insensitive) in the settlement `description`
- **Date must also match exactly** (no leeway)
- One-to-one match only. Do not force match if one condition fails.
- **No fuzzy matching allowed.**

Step 4️⃣: Identify unmatched invoices:
- These are valid invoices not matched to any settlement and not considered duplicates.

Step 5️⃣: Identify reimbursements:
- These are refunds or reversals that are in the current account entries but not matched to any invoice.:
  • Not matched to any invoice
  • Not considered duplicates
  • Likely labeled with keywords such as "refund", "reimbursement", "reversal", etc.

Step 6️⃣: Include ALL valid current account entries in the output (not just those matched)

Step 7️⃣: Return a final JSON result with all sections described below.

==========================
📦 FINAL OUTPUT FORMAT (STRICT JSON):
==========================

{{
  "summary": "...",
  "invoices": [...],
  "current_account_entries": [...],
  "matched": [...],
  "matched_invoices": [...],
  "unmatched_invoices": [...],
  "unmatched_payments": [...],
  "reimbursements": [...],
  "duplicates": {{
    "invoices": [...],
    "statements": [...]
  }}
}}
"""),

("user", """
Few-shot Example:

Invoices:
[
  {{ "date": "2025-06-20", "amount": 27033.29, "description": "Invoice INV-20250620-996A7766" }},
  {{ "date": "2025-06-20", "amount": 27033.29, "description": "Invoice INV-20250620-996A7766" }}
]

Current Account Statements:
[
  {{ "date": "2025-06-20", "amount": 27033.29, "description": "Merchant Settlement INV-20250620-996A7766", "type": "credit" }},
  {{ "date": "2025-06-23", "amount": 1000.00, "description": "Refund to Merchant", "type": "credit" }},
  {{ "date": "2025-06-23", "amount": 1000.00, "description": "Refund to Merchant", "type": "credit" }}
]
"""),
        ("user", """
Few-shot Example – Do NOT match if date does not match:

Invoices:
[
  {{ "date": "2025-06-20", "amount": 27033.29, "description": "Invoice INV-20250620-996A7766" }}
]

Current Account Statements:
[
  {{ "date": "2025-06-21", "amount": 27033.29, "description": "Merchant Settlement INV-20250620-996A7766", "type": "credit" }}
]

Expected:
- No match (because date differs)
- Should appear under unmatched_invoices
"""),
        ("user", """
Now reconcile the following:

Invoices:
{invoices}
         
Current Account Statements:
{statements}
""")
    ])
    
    try:
        response = (
            prompt
            | ChatGroq(model="llama3-8b-8192", api_key=GROQ_API_KEY,temperature=0.0)
            | parser
        ).invoke({
            "invoices": valid_invoices,
            "statements": valid_statements
        })
    except Exception as e:
        print(f"Primary LLM call failed: {str(e)}")
        raw_output = "LLM processing failed"
        try:
            # Use a slightly different temperature for fallback to get different results
            fallback_chain = prompt | ChatGroq(model="llama3-8b-8192", api_key=GROQ_API_KEY, temperature=0.0) | StrOutputParser()
            raw_output = fallback_chain.invoke({"invoices": valid_invoices, "statements": valid_statements})
            print("=== RAW LLM OUTPUT ===")
            print(raw_output)
            
            # Better JSON extraction with improved regex patterns
            json_text = re.sub(r'//.*', '', raw_output)  # Remove comments
            json_match = re.search(r'\{[\s\S]*\}', json_text, re.DOTALL)  # Match any JSON object including newlines
            if not json_match:               
                
                default_response = {
                    "summary": "Failed to parse LLM output, using fallback",
                    "invoices": valid_invoices,
                    "current_account_entries": valid_statements,
                    "matched": [], 
                    "matched_invoices": [],
                    "unmatched_invoices": valid_invoices,
                    "unmatched_payments": valid_statements,
                    "reimbursements": [],
                    "duplicates": {
                        "invoices": [],
                        "statements": []
                    },
                    "error_details": str(e)
                }
                return pd.DataFrame(), "Failed to extract valid JSON from LLM response", default_response
            json_str = json_match.group(0)
            json_str = json_str.replace("'", '"')
            json_str = re.sub(r',(\s*[\]}])', r'\1', json_str)
            json_str = re.sub(r'\bNaN\b', 'null', json_str)
            json_str = re.sub(r'\b(Infinity|-Infinity)\b', 'null', json_str)
            json_str = re.sub(r'\.(\s*[\]}])', r'\1', json_str)
            try:
                response = json.loads(json_str)
            except Exception:
                try:
                    import json5
                    response = json5.loads(json_str)
                except Exception:
                    response = {
                        "summary": "JSON parsing failed, using fallback",
                        "invoices": valid_invoices,
                        "current_account_entries": valid_statements,
                        "matched": [],  # Use our sample matched entries
                        "matched_invoices": [],
                        "unmatched_invoices": valid_invoices,
                        "unmatched_payments": valid_statements,
                        "reimbursements": [],
                        "duplicates": {"invoices": [], "statements": []},
                    }
        except Exception as fallback_error:
            error_text = f"❌ Invoice LLM reconciliation failed: {fallback_error}\nPrimary error: {str(e)}"
            print(error_text)
            upload_to_s3(error_text.encode(), S3_BUCKET, "reconciliation/summary.txt", content_type="text/plain")
            
            # Try to create at least one properly formatted matched entry if we have data
          
            
            response = {
                "summary": f"LLM services unavailable: {str(fallback_error)}",
                "invoices": valid_invoices,
                "current_account_entries": valid_statements,
                "matched": [],  # Use our sample matched entries
                "matched_invoices": [],
                "unmatched_invoices": valid_invoices,
                "unmatched_payments": valid_statements,
                "reimbursements": [],
                "duplicates": {"invoices": [], "statements": []},
                "error_details": f"{str(fallback_error)} / {str(e)}"
            }

    matched = response.get("matched", [])
    unmatched_invoices = response.get("unmatched_invoices", [])
    unmatched_payments = response.get("unmatched_payments", [])
    reimbursements = response.get("reimbursements", [])
    duplicates = response.get("duplicates", {})
    dup_invoices = duplicates.get("invoices", [])
    dup_statements = duplicates.get("statements", [])

    total_valid_invoices = len(valid_invoices)
    total_valid_statements = len(valid_statements)
    calculated_total_invoices = len(matched) + len(unmatched_invoices) + len(dup_invoices)
    calculated_total_statements = len(matched) + len(unmatched_payments) + len(reimbursements) + len(dup_statements)

    summary_lines = []
    summary_lines.append("=== Totals ===")
    summary_lines.append(f"🧾 Total Invoice Entries: {total_valid_invoices}")
    summary_lines.append(f"🏦 Total Current Account Entries: {total_valid_statements}")
    summary_lines.append(f"✅ Total Matched Invoices: {len(matched)}")
    summary_lines.append(f"❌ Total Unmatched Invoices: {len(unmatched_invoices)}")
    summary_lines.append(f"❌ Total Unmatched Payments: {len(unmatched_payments)}")
    summary_lines.append(f"🟢 Total Reimbursements: {len(reimbursements)}")
    summary_lines.append(f"🔁 Total Duplicate Invoices: {len(dup_invoices)}")
    summary_lines.append(f"🔁 Total Duplicate Statement Entries: {len(dup_statements)}")
    summary_lines.append("")

    if calculated_total_invoices != total_valid_invoices:
        summary_lines.append(f"⚠️ Invoice Count Mismatch: Expected {total_valid_invoices}, Calculated {calculated_total_invoices}")
    if calculated_total_statements != total_valid_statements:
        summary_lines.append(f"⚠️ Statement Count Mismatch: Expected {total_valid_statements}, Calculated {calculated_total_statements}")

    def log_section(title, entries, is_matched=False):
        if entries:
            summary_lines.append(title)
            for e in entries:
                if is_matched:
                    if isinstance(e, dict):
                        inv = e.get("invoice", {})
                        stmt = e.get("statement", e.get("settlement", {}))
                        summary_lines.append(
                            f"  • Invoice: ₹{inv.get('amount', 'N/A')} on {inv.get('date', 'N/A')} - {inv.get('description', 'N/A')[:50]}"
                            f" | Statement: ₹{stmt.get('amount', 'N/A')} on {stmt.get('date', 'N/A')} - {stmt.get('description', 'N/A')[:50]}"
                        )
                else:
                    amount = e.get("amount", "N/A")
                    date = e.get("date", "N/A")
                    desc = e.get("description", "N/A")
                    summary_lines.append(f"  • ₹{amount} on {date} - {desc[:50]}")
            summary_lines.append("")

    log_section("✅ Matched Invoices:", matched)
    log_section("❌ Unmatched Invoices:", unmatched_invoices)
    log_section("❌ Unmatched Payments:", unmatched_payments)
    log_section("🟢 Reimbursements:", reimbursements)
    log_section("🔁 Duplicate Invoices:", dup_invoices)
    log_section("🔁 Duplicate Statement Entries:", dup_statements)

    structured_summary = "\n".join(summary_lines)
    upload_to_s3(structured_summary.encode("utf-8"), S3_BUCKET, "reconciliation/summary.txt", content_type="text/plain")

    with pd.ExcelWriter("llm_invoice_reconciliation.xlsx") as writer:
        for section in ["total_valid_invoices","matched", "unmatched_invoices", "unmatched_payments", "reimbursements"]:
            df = pd.DataFrame(response.get(section, []))
            if df.empty:
                df = pd.DataFrame([{"info": "No data"}])
            df.to_excel(writer, sheet_name=section, index=False)
        for key, val in response.get("duplicates", {}).items():
            df = pd.DataFrame(val)
            if df.empty:
                df = pd.DataFrame([{"info": "No data"}])
            df.to_excel(writer, sheet_name=f"duplicates_{key}", index=False)

    with open("llm_invoice_reconciliation.xlsx", "rb") as f:
        upload_to_s3(f.read(), S3_BUCKET, "reconciliation/llm_invoice_reconciliation.xlsx",
                     content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    return pd.DataFrame(matched), structured_summary, response

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
        wrote_sheet = False
        def safe_write(sheet_name, data):
            nonlocal wrote_sheet
            df = pd.DataFrame(data)
            if not df.empty:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                wrote_sheet = True
        safe_write("All Invoices", all_invoices)
        safe_write("Current Account Statements", all_current_statements)
        safe_write("matched", response.get("matched", []))
        safe_write("unmatched_invoices", response.get("unmatched_invoices", []))
        safe_write("unmatched_payments", response.get("unmatched_payments", []))
        safe_write("reimbursements", response.get("reimbursements", []))
        safe_write("duplicates_invoices", response.get("duplicates", {}).get("invoices", []))
        safe_write("duplicates_statements", response.get("duplicates", {}).get("statements", []))
        if not wrote_sheet:
            pd.DataFrame([{"info": "No data"}]).to_excel(writer, sheet_name="empty", index=False)
    upload_to_s3(out_stream.getvalue(), S3_BUCKET, excel_key,
                 content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    print("✅ Uploaded to S3: reconciliation/current_account.xlsx")
    
    return summary

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
        '✅ Matched Expense Bills:': 'Matched with Bank Statement',
        '❌ Unmatched Expense Bills:': 'Unmatched Expense Bills',
        '❌ Unmatched Debits:': 'Unmatched Bank Debits',
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

    # Only parse the Totals section for reliability
    if "=== Totals ===" in summary_text:
        totals_section = summary_text.split("=== Totals ===")[-1]
    else:
        totals_section = summary_text

    patterns = {
        'Total Expense Bills': r"Total Invoice Entries: (\d+)",
        'Matched with Bank Statement': r"Total Matched Invoices: (\d+)",
        'Unmatched Expense Bills': r"Total Unmatched Invoices: (\d+)",
        'Unmatched Bank Debits': r"Total Unmatched Payments: (\d+)"
    }
    for key, pat in patterns.items():
        m = re.search(pat, totals_section)
        if m:
            metrics[key] = int(m.group(1))
    return metrics

def reconcile_with_llm(expenses: List[Dict], statements: List[Dict]):
    valid_expenses = [e for e in expenses if not e.get("parsing_failed")]
    parsing_failed_expenses = [e for e in expenses if e.get("parsing_failed")]
    valid_statements = [s for s in statements if not s.get("parsing_failed")]

    if not valid_expenses or not valid_statements:
        msg = "❌ Not enough valid data for LLM reconciliation."
        upload_to_s3(msg.encode(), S3_BUCKET, "reconciliation/saving_account_summary.txt", content_type="text/plain")
        return pd.DataFrame(), msg

    parser = JsonOutputParser()
    prompt = ChatPromptTemplate.from_messages([
        ("system", r"""
You are a strict and detail-oriented finance assistant. Your task is to reconcile employee-submitted `expense_bills` against `saving_account_entries` from a bank statement.

Your job is to:
1. Identify all valid expense and saving account entries.
2. Match them strictly (see rules below).
3. Detect duplicates in each list.
4. Classify unmatched expenses.

📋 Return a JSON object with the following structure:
{{ 
  "expense_bills": [...],                     
  "saving_account_entries": [...],            
  "matched": [...],                           
  "unmatched_expenses": [...], 
  "unmatched_debits": [...],                 
  "duplicates": {{                            
    "expenses": [...],                        
    "statements": [...]                       
  }}                                          
}}

✅ Matching Rules:
- Amount must match exactly.
- Date must match exactly.
- Description must match **exactly** (case-insensitive match, no semantic similarity).

🔁 Duplicate Rules:
- Any entry in the same list (expenses or statements) that has the same `date`, `amount`, and `description` as another is a duplicate (beyond the first instance).
- Duplicates are not considered for matching or unmatched classification.

❌ Unmatched Expense Bills:
- Valid expenses that were not matched and are not marked as duplicates.

🛑 Exclude negative (reimbursement) entries from all reconciliation logic.

🚫 Respond with JSON only. No markdown, no explanation, no extra text.
"""),
        ("user", r"""
Few-shot Example:

Expense Entries:
[
  {{ "date": "2024-12-01", "amount": 100.0, "description": "Cab Fare to Airport" }},
  {{ "date": "2024-12-01", "amount": 100.0, "description": "Cab Fare to Airport" }}
]

Saving Account Entries:
[
  {{ "date": "2024-12-01", "amount": 100.0, "description": "Cab Fare to Airport" }}
]

Expected Output:
{{ 
  "expense_bills": [
    {{ "date": "2024-12-01", "amount": 100.0, "description": "Cab Fare to Airport" }},
    {{ "date": "2024-12-01", "amount": 100.0, "description": "Cab Fare to Airport" }}
  ],
  "saving_account_entries": [
    {{ "date": "2024-12-01", "amount": 100.0, "description": "Cab Fare to Airport" }}
  ],
  "matched": [
    {{ "date": "2024-12-01", "amount": 100.0, "description": "Cab Fare to Airport" }}
  ],
 "unmatched_expenses": [],
  "unmatched_debits": [
    {{ "date": "2024-12-02", "amount": 200.0, "description": "ATM Withdrawal" }}
  ],
  
  "duplicates": {{
    "expenses": [
      {{ "date": "2024-12-01", "amount": 100.0, "description": "Cab Fare to Airport" }}
    ],
    "statements": []
  }}
}}
"""),
        ("user", """
Now reconcile the below:

Expense Entries:
{expenses}

Saving Account Entries:
{statements}
""")
    ])

    chain = prompt | ChatGroq(model="llama3-8b-8192", api_key=GROQ_API_KEY, temperature=0.0) | parser

    try:
        response = chain.invoke({"expenses": valid_expenses, "statements": valid_statements})
    except Exception as e:
        try:
            fallback_chain = prompt | ChatGroq(model="llama3-8b-8192", api_key=GROQ_API_KEY, temperature=0.0) | StrOutputParser()
            raw_output = fallback_chain.invoke({"expenses": valid_expenses, "statements": valid_statements})
            
            # Remove comments and extract JSON
            json_text = re.sub(r'//.*', '', raw_output)
            json_match = re.search(r"\{.*\}", json_text, re.DOTALL)
            if not json_match:
                raise ValueError("Could not extract JSON from LLM response")
                
            json_str = json_match.group()
            
            # Clean up common JSON issues
            json_str = json_str.replace("'", '"')
            json_str = re.sub(r',(\s*[\]}])', r'\1', json_str)
            json_str = re.sub(r'\bNaN\b', 'null', json_str)
            json_str = re.sub(r'\b(Infinity|-Infinity)\b', 'null', json_str)
            
            try:
                response = json.loads(json_str)
            except Exception:
                import json5
                response = json5.loads(json_str)
                
        except Exception as fallback_error:
            error_text = f"❌ LLM reconciliation failed: {fallback_error}"
            upload_to_s3(error_text.encode(), S3_BUCKET, "reconciliation/saving_account_summary.txt", content_type="text/plain")
            return pd.DataFrame(), error_text

    # Add parsing failed as unmatched
    for e in parsing_failed_expenses:
        response.setdefault("unmatched_expenses", []).append({
            "date": "UNKNOWN DATE",
            "amount": e.get("amount", "N/A"),
            "description": f"{e.get('description', 'N/A')} (parsing failed)"
        })

    # Remove duplicates from unmatched
    unmatched_expenses = response.get("unmatched_expenses", [])
    duplicates_expenses = response.get("duplicates", {}).get("expenses", [])
    cleaned_unmatched = [e for e in unmatched_expenses if e not in duplicates_expenses]
    response["unmatched_expenses"] = cleaned_unmatched

    # Create structured summary
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

    expense_bills = response.get("expense_bills", [])
    statement_entries = response.get("saving_account_entries", [])
    matched = response.get("matched", [])
    unmatched = response.get("unmatched_expenses", [])
    unmatched_debits = response.get("unmatched_debits", [])
    duplicates = response.get("duplicates", {})
    duplicate_expenses = duplicates.get("expenses", [])
    duplicate_statements = duplicates.get("statements", [])

    log_section("🧾 All Expense Bills:", expense_bills)
    log_section("🏦 All Saving Account Entries:", statement_entries)
    log_section("✅ Matched Expense Bills:", matched)
    log_section("❌ Unmatched Expense Bills:", unmatched)
    log_section("❌ Unmatched Debits:", unmatched_debits)
    log_section("🔁 Duplicate Expense Bills:", duplicate_expenses)
    log_section("🔁 Duplicate Statement Entries:", duplicate_statements)

    summary_lines.append("=== Totals ===")
    summary_lines.append(f"🧾 Total Expense Bills: {len(expense_bills)}")
    summary_lines.append(f"🏦 Total Saving Account Entries: {len(statement_entries)}")
    summary_lines.append(f"✅ Total Matched Expenses: {len(matched)}")
    summary_lines.append(f"❌ Total Unmatched Expenses: {len(unmatched)}")
    summary_lines.append(f"❌ Total Unmatched Debits: {len(unmatched_debits)}")
    summary_lines.append(f"🔁 Total Duplicate Expense Bills: {len(duplicate_expenses)}")
    summary_lines.append(f"🔁 Total Duplicate Statement Entries: {len(duplicate_statements)}")

    summary_text = "\n".join(summary_lines)

    # Create Excel report
    with pd.ExcelWriter("llm_reconciliation_output.xlsx") as writer:
        for section in ["expense_bills", "saving_account_entries", "matched", "unmatched_expenses", "unmatched_debits"]:
            df = pd.DataFrame(response.get(section, []))
            if df.empty:
                df = pd.DataFrame([{"info": "No data"}])
            df.to_excel(writer, sheet_name=section, index=False)
        for key, val in response.get("duplicates", {}).items():
            df = pd.DataFrame(val)
            if df.empty:
                df = pd.DataFrame([{"info": "No data"}])
            df.to_excel(writer, sheet_name=f"duplicates_{key}", index=False)

    with open("llm_reconciliation_output.xlsx", "rb") as f:
        upload_to_s3(f.read(), S3_BUCKET, "reconciliation/llm_reconciliation_output.xlsx",
                     content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    
    return pd.DataFrame(response.get("matched", [])), summary_text

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
        
    saving_metrics = calculate_savings_metrics(summary_savings)
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
        'saving_metrics': saving_metrics,
        'current_metrics': current_metrics
    }