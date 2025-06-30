import os
import re
import boto3
from typing import List
from pydantic import BaseModel
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import ResponseSchema, StructuredOutputParser

# === Load environment variables ===
load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "paci-test-poc")
S3_KEY_INPUT = "reconciliation/current_account_summary.txt"
S3_KEY_OUTPUT = "reconciliation/profitloss1.txt"

# === Pydantic Output Schema ===
class PnLResult(BaseModel):
    revenue: float
    matched_invoice_ids: List[str]
    expense_items: List[str]
    operating_expenses: float

# === Utility Functions ===
def extract_invoice_amounts(raw_text: str, matched_ids: List[str]) -> float:
    total = 0.0
    for inv_id in matched_ids:
        pattern = rf"[₹$]?([\d,]+\.\d{{2}})[^\n]*Invoice\s+{re.escape(inv_id)}"
        match = re.search(pattern, raw_text, flags=re.IGNORECASE)
        if match:
            amt = float(match.group(1).replace(",", ""))
            total += amt
        else:
            print(f"⚠️ Could not find amount for invoice ID: {inv_id}")
    return round(total, 2)

def extract_amount(entry: str) -> float:
    match = re.search(r"\$([\d,]+\.\d{2})", entry)
    return float(match.group(1).replace(",", "")) if match else 0.0

def is_valid_expense(item: str) -> bool:
    revenue_keywords = ["payment from", "received from client", "credited", "invoice inv-"]
    return not any(keyword in item.lower() for keyword in revenue_keywords)

def is_return_entry(item: str) -> bool:
    return "return" in item.lower() or "refund" in item.lower()

# === Analyzer Class ===
class ProfitLossAnalyzer:
    def __init__(self):
        self.llm = ChatGroq(temperature=0, model_name="llama3-8b-8192", api_key=GROQ_API_KEY)
        self.s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        self.parser = StructuredOutputParser.from_response_schemas([
            ResponseSchema(name="revenue", description="Total revenue from matched invoices, in float"),
            ResponseSchema(name="matched_invoice_ids", description="List of matched invoice IDs"),
            ResponseSchema(name="operating_expenses", description="Sum of all valid expense entries from the current account, in float"),
            ResponseSchema(name="expense_items", description="List of individual operating expense descriptions with amounts"),
        ])

        self.prompt_template = ChatPromptTemplate.from_messages([
            ("system", "You are a financial analyst assistant who extracts PnL values from reconciliation reports."),
            ("user", '''Given the reconciliation result below:

{recon_text}

Instructions:

1. **Revenue**:
   - Sum of all invoice amounts listed under `All Invoice Entries` **that are matched** in the `All Current Account Entries`.
   - Unmatched invoices should not be included in revenue.

2. **COGS**:
   - 70% of the revenue.

3. **Operating Expenses**:
   - Start with all entries under `All Current Account Entries`.
   - STRICTLY EXCLUDE:
     a. Any entries that match invoice payments already counted in revenue.
     b. Any **incoming payments** or **client payments**, such as:
        - Lines containing "Payment from", "received", "Invoice INV-...", "credited", "client transfer"
        - Example: "Payment from TechDynamics LLC Invoice INV-TD-1234" — this is an income, not an expense.
     c. Any duplicate entries — count only the first occurrence.

   - INCLUDE only genuine **outgoing business expenses**, such as:
     - Rent, Employee Salaries, Subscriptions, Internet Bills, Utilities, Office Purchases, etc.
     - Do not include ambiguous or unclear items.

4. **Validation**:
   - Ensure `operating_expenses` matches the sum of values from `expense_items`.
   - Format each expense item as `Description ($amount)` using exactly 2 decimal places.

5. **Currency**:
   - ₹ is to be treated as equivalent to $ (₹1 = $1)

6. **Returns / Refunds**:
   - Identify entries that clearly mention a return or refund (e.g., "Return -", "Refund -").
   - Treat these as **business losses**, not operating expenses.
   - Do **not** include them in the `operating_expenses` or `net profit`.
   - Format each return/refund item as `Description ($amount) (return)`
   - At the end, include the total loss under `"Total Loss from Returns"`.

📘 Example Output Format:
{{
  "revenue": 55552.14,
  "matched_invoice_ids": ["INV-20250625-2D2DAEB5", "INV-20250625-A9A82DBB"],
  "expense_items": [
    "Monthly Office Rent ($4000.00)",
    "Software Subscription - Adobe Suite ($189.99)",
    "Internet Bill ($79.99)",
    "Employee Salary - June ($55000.00)",
    "Utility Payment - Electricity ($750.00)",
    "Return - Amazon Business ($320.50) (return)",
    "Return - Adobe Suite Subscription ($189.99) (return)"
  ],
  "operating_expenses": 60719.98
}}'''),
            ("user", "{format_instructions}")
        ])

    def load_text(self) -> str:
        obj = self.s3.get_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY_INPUT)
        return obj["Body"].read().decode("utf-8")

    def save_result(self, text: str):
        self.s3.put_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY_OUTPUT, Body=text.encode("utf-8"))
        print(f"\n✅ Result saved to S3 as: s3://{S3_BUCKET_NAME}/{S3_KEY_OUTPUT}")

    def run(self):
        raw_text = self.load_text()

        formatted_prompt = self.prompt_template.format_prompt(
            recon_text=raw_text,
            format_instructions=self.parser.get_format_instructions()
        )

        response = self.llm.invoke(formatted_prompt)
        print("\n🔍 Raw LLM Response:")
        print(response.content)

        parsed_raw = self.parser.parse(response.content)
        parsed = PnLResult(**parsed_raw)

        revenue = extract_invoice_amounts(raw_text, parsed.matched_invoice_ids)
        cogs = round(0.70 * revenue, 2)
        gross_profit = round(revenue - cogs, 2)

        return_items = [item for item in parsed.expense_items if is_return_entry(item)]
        tagged_return_items = [f"{item}" for item in return_items]
        non_return_expenses = [item for item in parsed.expense_items if is_valid_expense(item) and not is_return_entry(item)]

        corrected_operating_expenses = round(sum(extract_amount(item) for item in non_return_expenses), 2)
        return_loss_total = round(sum(extract_amount(item) for item in return_items), 2)
        net_profit = round(gross_profit - corrected_operating_expenses, 2)

        result_lines = []
        result_lines.append("💸 Operating Expense Items (Validated):")
        for item in non_return_expenses:
            result_lines.append(f" - {item}")
        result_lines.append(f"\n🧮 Total Operating Expenses (Excludes Returns): ${corrected_operating_expenses:,.2f}")

        if tagged_return_items:
            result_lines.append("\n🔁 Returned / Refunded Items (Shown as Loss):")
            for item in tagged_return_items:
                result_lines.append(f" - {item}")
            result_lines.append(f"\n💥 Total Loss from Returns: ${return_loss_total:,.2f}")

        result_lines.append("\n📊 Profit & Loss Summary")
        result_lines.append(f"Revenue: ${revenue:,.2f}")
        result_lines.append(f"COGS (70% of Revenue): ${cogs:,.2f}")
        result_lines.append(f"Gross Profit: ${gross_profit:,.2f}")
        result_lines.append(f"Operating Expenses (Validated): ${corrected_operating_expenses:,.2f}")
        result_lines.append(f"Loss from Returns (Excluded from Net Profit): ${return_loss_total:,.2f}")
        result_lines.append(f"Net Profit: ${net_profit:,.2f}")

        result_lines.append("\n📄 Matched Invoice IDs:")
        for inv in parsed.matched_invoice_ids:
            result_lines.append(f" - {inv}")

        final_output = "\n".join(result_lines)

        print("\n" + final_output)
        self.save_result(final_output)

# === Entry Point ===
if __name__ == "__main__":
    analyzer = ProfitLossAnalyzer()
    analyzer.run()
