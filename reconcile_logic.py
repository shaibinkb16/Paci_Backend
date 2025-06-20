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
from langchain_core.output_parsers import JsonOutputParser
from s3_utils import list_s3_files, download_s3_file, upload_to_s3
from typing import List, Dict, Optional, Union

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

def parse_date(date_str: str) -> Optional[datetime.date]:
    """Parse date string in format 'dd-MMM-yyyy' to date object"""
    try:
        match = re.match(r"(\d{1,2}-[A-Za-z]{3}-\d{4})", date_str.strip())
        if match:
            return datetime.strptime(match.group(1), "%d-%b-%Y").date()
        print(f"❌ Date parsing failed for: {date_str} - No date found")
        return None
    except Exception as e:
        print(f"❌ Date parsing failed for: {date_str} - {e}")
        return None

def extract_expense_data(pdf_input: Union[str, bytes, bytearray]) -> List[Dict]:
    """Extract expense data from either file path or bytes"""
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
            "reason": f"Failed to read PDF: {str(e)}"
        }]

    entries = []
    date, description, category, amount = None, None, None, None

    for line in lines:
        if re.match(r"\d{1,2}-[A-Za-z]{3}-\d{4}", line):
            date = parse_date(line)
        elif line.lower().startswith("inr") or re.search(r"inr\s*[0-9]+\.[0-9]{2}", line.lower()):
            amt_match = re.search(r"([0-9]+\.[0-9]{2})", line)
            if amt_match:
                amount = float(amt_match.group(1))
        elif line in ["Travel", "Meals", "Utilities", "Office Supplies"]:
            category = line
        elif date and not description:
            description = line

        if all([date, description, category, amount]):
            entries.append({
                "file": file_name,
                "date": date,
                "description": description,
                "category": category,
                "amount": amount
            })
            date, description, category, amount = None, None, None, None

    if not entries:
        entries.append({
            "parsing_failed": True,
            "file": file_name,
            "reason": "Could not extract complete expense entry (missing one or more of date/description/category/amount)"
        })

    return entries

def extract_invoice_data(pdf_input: Union[str, bytes, bytearray]) -> List[Dict]:
    """Extract invoice data from either file path or bytes"""
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
            "reason": f"Failed to read PDF: {str(e)}"
        }]

    invoice_date = None
    total_amount = None
    description = "Invoice Transaction"
    category = "Current Account"

    for i, line in enumerate(lines):
        if "Invoice Date:" in line and not invoice_date:
            match = re.search(r"Invoice Date:\s*(\w+ \d{1,2}, \d{4})", line)
            if match:
                try:
                    invoice_date = datetime.strptime(match.group(1), "%B %d, %Y").date()
                except Exception as e:
                    print(f"❌ Date parsing failed: {e}")

        if line.strip().upper() == "TOTAL:" and not total_amount:
            if i + 1 < len(lines):
                amt_match = re.search(r"\$?([0-9,]+\.[0-9]{2})", lines[i + 1])
                if amt_match:
                    total_amount = float(amt_match.group(1).replace(",", ""))

    if invoice_date and total_amount:
        return [{
            "file": file_name,
            "date": invoice_date,
            "description": description,
            "category": category,
            "amount": total_amount
        }]
    else:
        return [{
            "parsing_failed": True,
            "file": file_name,
            "reason": "Missing invoice date or total"
        }]

def extract_statement_entries(pdf_input: Union[str, bytes, bytearray]) -> List[Dict]:
    """Extract statement entries from either file path or bytes"""
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
            "reason": f"Failed to read PDF: {str(e)}"
        }]

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
                "file": file_name,
                "date": date,
                "description": description,
                "category": category,
                "type": type_,
                "amount": amount
            })
            date, description, category, type_, amount = None, None, None, None, None

    if not entries:
        entries.append({
            "parsing_failed": True,
            "file": file_name,
            "reason": "Could not extract complete statement entry (missing one or more of date/description/category/type/amount)"
        })

    return entries

def extract_current_account_statement(pdf_input: Union[str, bytes, bytearray]) -> List[Dict]:
    """Extract current account statement data from either file path or bytes"""
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
            "reason": f"Failed to read PDF: {str(e)}"
        }]

    entries = []
    for line in lines:
        match = re.match(r"(\d{4}-\d{2}-\d{2})\s+(INV-[A-Z0-9-]+)\s+.+?\s+TechSolutions Inc\.\s+(Debit|Credit)\s+([0-9,]+\.\d{2})", line)
        if match:
            try:
                date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
                desc = match.group(2)
                txn_type = match.group(3).lower()
                amount = float(match.group(4).replace(",", ""))
                entries.append({
                    "file": file_name,
                    "date": date,
                    "description": desc,
                    "category": "Invoice",
                    "type": txn_type,
                    "amount": amount
                })
            except Exception as e:
                print(f"⚠️ Line skipped due to parsing error: {line} — {e}")
    return entries

"""def reconcile(expenses: List[Dict], statements: List[Dict]) -> tuple[pd.DataFrame, str]:
    summary = []
    summary.append("==== EXPENSE RECONCILIATION SUMMARY ====")
    summary.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Separate valid entries and failures
    valid_expenses = [e for e in expenses if not e.get("parsing_failed")]
    failed_expenses = [e for e in expenses if e.get("parsing_failed")]

    valid_statements = [s for s in statements if not s.get("parsing_failed")]
    failed_statements = [s for s in statements if s.get("parsing_failed")]

    df_exp = pd.DataFrame(valid_expenses)
    df_stmt = pd.DataFrame(valid_statements)

    if df_exp.empty or df_stmt.empty:
        summary.append("❌ Expense or Statement data is empty. No reconciliation performed.")
        summary_text = "\n".join(summary)
        upload_to_s3(summary_text.encode("utf-8"), S3_BUCKET, "reconciliation/reconciliation_summary.txt", content_type="text/plain")
        return pd.DataFrame(), summary_text

    # Find duplicates before merging
    exp_duplicates = df_exp[df_exp.duplicated(subset=["date", "description", "amount"], keep=False)]
    stmt_duplicates = df_stmt[df_stmt.duplicated(subset=["date", "description", "amount"], keep=False)]

    # Perform the merge
    merged = df_exp.merge(
        df_stmt,
        on=["date", "amount", "description"],
        how="left",
        indicator=True,
        suffixes=("_exp", "_stmt")
    )
    merged["match_status"] = merged["_merge"].map({
        "both": "✅ Matched", 
        "left_only": "❌ Missing in Statement"
    })
    merged.drop(columns=["_merge"], inplace=True)

    # Identify various cases
    unmatched_expenses = merged[merged["match_status"] == "❌ Missing in Statement"]
    unmatched_charges = df_stmt[~df_stmt.apply(tuple, 1).isin(merged.apply(tuple, 1))]
    reimbursements = df_stmt[df_stmt["amount"] < 0]

    # Build summary report
    summary.append("📊 OVERVIEW:")
    summary.append(f"- Total Expense Bills: {len(df_exp)}")
    summary.append(f"- Matched with Bank Statement: {(merged['match_status'] == '✅ Matched').sum()}")
    summary.append(f"- Unmatched Expense Bills: {len(unmatched_expenses)}")
    summary.append(f"- Unmatched Bank Debits: {len(unmatched_charges[unmatched_charges['amount'] > 0])}")
    summary.append(f"- Reimbursements Detected: {len(reimbursements)}")
    summary.append(f"- Duplicate Expense Bills: {len(exp_duplicates)}")
    summary.append(f"- Duplicate Statement Entries: {len(stmt_duplicates)}\n")

    # Add detailed sections
    sections = [
        ("🔴 UNMATCHED EXPENSE BILLS", unmatched_expenses, 
         lambda r: f"  • File: {r.get('file', 'Unknown')} | ₹{r['amount']} on {r['date']} - {r['description']}"),
        ("🟡 UNMATCHED BANK DEBITS", unmatched_charges[unmatched_charges['amount'] > 0],
         lambda r: f"  • ₹{r['amount']} on {r['date']} - {r['description']}"),
        ("🟢 REIMBURSEMENTS / CREDITS", reimbursements,
         lambda r: f"  • ₹{r['amount']} on {r['date']} - {r['description']}"),
        ("🔁 DUPLICATE EXPENSE ENTRIES", exp_duplicates,
         lambda r: f"  • ₹{r['amount']} on {r['date']} - {r['description']} (File: {r['file']})"),
        ("🔁 DUPLICATE STATEMENT ENTRIES", stmt_duplicates,
         lambda r: f"  • ₹{r['amount']} on {r['date']} - {r['description']}")
    ]

    for title, data, formatter in sections:
        if not data.empty:
            summary.append(f"{title}:")
            for _, row in data.iterrows():
                summary.append(formatter(row))
            summary.append("")

    # Add parsing failures if any
    if failed_expenses:
        summary.append("\n🚨 EXPENSE PARSING FAILURES:")
        for fail in failed_expenses:
            summary.append(f"  • {fail['file']}: {fail['reason']}")
    
    if failed_statements:
        summary.append("\n🚨 STATEMENT PARSING FAILURES:")
        for fail in failed_statements:
            summary.append(f"  • {fail['file']}: {fail['reason']}")

    # Generate and upload reports
    summary_text = "\n".join(summary)
    
    # Upload summary
    upload_to_s3(
        summary_text.encode("utf-8"), 
        S3_BUCKET, 
        "reconciliation/reconciliation_summary.txt", 
        content_type="text/plain"
    )
    
    # Upload detailed report
    merged.to_excel("reconciliation_report.xlsx", index=False)
    with open("reconciliation_report.xlsx", "rb") as f:
        upload_to_s3(
            f.read(), 
            S3_BUCKET, 
            "reconciliation/reconciliation_report.xlsx",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    return merged, summary_text """

def reconcile_with_llm(expenses: List[Dict], statements: List[Dict]) -> tuple[pd.DataFrame, str]:
    """Reconciliation using LLM for smarter matching and uploads results to S3."""
    valid_expenses = [e for e in expenses if not e.get("parsing_failed")]
    valid_statements = [s for s in statements if not s.get("parsing_failed")]

    if not valid_expenses or not valid_statements:
        msg = "❌ Not enough valid data for LLM reconciliation."
        upload_to_s3(msg.encode(), S3_BUCKET, "reconciliation/llm_summary.txt", content_type="text/plain")
        return pd.DataFrame(), msg

    parser = JsonOutputParser()

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a smart finance assistant. Your job is to reconcile business expense records with bank statement entries."),
        ("user", """Reconcile the following data:
Expense Entries: {expenses}
Statement Entries: {statements}

Compare based on `date`, `description`, and `amount`. Mark each expense as:
- ✅ Matched (with a statement)
- ❌ Missing in statement
Also identify:
- 🟢 Reimbursements (negative amounts in statements)
- 🔁 Duplicates

Return JSON:
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
}}""")
    ])

    chain = prompt | ChatGroq(model="llama3-8b-8192", api_key=GROQ_API_KEY) | parser

    try:
        response = chain.invoke({"expenses": valid_expenses, "statements": valid_statements})
    except Exception as e:
        error_text = f"❌ LLM reconciliation failed: {e}"
        upload_to_s3(error_text.encode(), S3_BUCKET, "reconciliation/llm_summary.txt", content_type="text/plain")
        return pd.DataFrame(), error_text

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
    upload_to_s3(summary_text.encode("utf-8"), S3_BUCKET, "reconciliation/llm_summary.txt", content_type="text/plain")

    # Save Excel
    with pd.ExcelWriter("llm_reconciliation_output.xlsx") as writer:
        for section in ["matched", "unmatched_expenses", "unmatched_charges", "reimbursements"]:
            pd.DataFrame(response.get(section, [])).to_excel(writer, sheet_name=section, index=False)
        for key, val in response.get("duplicates", {}).items():
            pd.DataFrame(val).to_excel(writer, sheet_name=f"duplicates_{key}", index=False)

    with open("llm_reconciliation_output.xlsx", "rb") as f:
        upload_to_s3(f.read(), S3_BUCKET, "reconciliation/llm_reconciliation_output.xlsx",
                     content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    return pd.DataFrame(response.get("matched", [])), summary_text

def reconcile_with_llm_current(expenses: List[Dict], statements: List[Dict], name: str) -> tuple[pd.DataFrame, str]:
    """LLM reconciliation for current account transactions"""
    valid_expenses = [e for e in expenses if not e.get("parsing_failed")]
    valid_statements = [s for s in statements if not s.get("parsing_failed")]

    if not valid_expenses or not valid_statements:
        msg = f"❌ Not enough valid data for {name} reconciliation."
        upload_to_s3(msg.encode(), S3_BUCKET, f"reconciliation/{name}_summary.txt", content_type="text/plain")
        return pd.DataFrame(), msg

    parser = JsonOutputParser()
    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a smart finance assistant. Reconcile invoices with bank statement entries."),
        ("user", """Reconcile the following:
Invoices: {expenses}
Statement Entries: {statements}

Compare by `date`, `amount`, and `description`. Mark:
✅ Matched
❌ Missing
🟢 Reimbursements
🔁 Duplicates

Return JSON:
{
  "summary": "...",
  "matched": [...],
  "unmatched_expenses": [...],
  "unmatched_charges": [...],
  "reimbursements": [...],
  "duplicates": {
     "expenses": [...],
     "statements": [...]
  }
}""")
    ])

    chain = prompt | ChatGroq(model="llama3-8b-8192", api_key=GROQ_API_KEY) | parser

    try:
        response = chain.invoke({"expenses": valid_expenses, "statements": valid_statements})
    except Exception as e:
        error = f"❌ LLM reconciliation failed: {e}"
        upload_to_s3(error.encode(), S3_BUCKET, f"reconciliation/{name}_summary.txt", content_type="text/plain")
        return pd.DataFrame(), error

    summary_lines = []
    def log_section(title, entries):
        if entries:
            summary_lines.append(title)
            for e in entries:
                summary_lines.append(f"  • ₹{e.get('amount', 'N/A')} on {e.get('date', 'N/A')} - {e.get('description', 'N/A')}")
            summary_lines.append("")

    log_section("✅ Matched:", response.get("matched", []))
    log_section("❌ Unmatched Invoices:", response.get("unmatched_expenses", []))
    log_section("🟡 Unmatched Bank Debits:", response.get("unmatched_charges", []))
    log_section("🟢 Reimbursements:", response.get("reimbursements", []))
    log_section("🔁 Duplicates in Invoices:", response.get("duplicates", {}).get("expenses", []))
    log_section("🔁 Duplicates in Statements:", response.get("duplicates", {}).get("statements", []))

    summary = "\n".join(summary_lines) or response.get("summary", "No summary returned.")
    upload_to_s3(summary.encode("utf-8"), S3_BUCKET, f"reconciliation/{name}_summary.txt", content_type="text/plain")

    with pd.ExcelWriter(f"llm_reconciliation_output_{name}.xlsx") as writer:
        for section in ["matched", "unmatched_expenses", "unmatched_charges", "reimbursements"]:
            pd.DataFrame(response.get(section, [])).to_excel(writer, sheet_name=section, index=False)
        for k, v in response.get("duplicates", {}).items():
            pd.DataFrame(v).to_excel(writer, sheet_name=f"duplicates_{k}", index=False)

    with open(f"llm_reconciliation_output_{name}.xlsx", "rb") as f:
        upload_to_s3(f.read(), S3_BUCKET, f"reconciliation/llm_reconciliation_output_{name}.xlsx",
                     content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    return pd.DataFrame(response.get("matched", [])), summary