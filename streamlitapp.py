import streamlit as st
import os
import json
import pandas as pd
import traceback
from dotenv import load_dotenv
from s3_utils import download_s3_file
from llm_reconsciliation import reconcile_preview
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
from langchain_core.output_parsers import StrOutputParser

# === ENVIRONMENT ===
load_dotenv()
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

# === Streamlit UI Config ===
st.set_page_config(page_title="Expense Reconciliation", layout="wide")
st.title("💼 Expense Reconciliation Agent")

# === Session State Initialization ===
if "reconcile_done" not in st.session_state:
    st.session_state.reconcile_done = False

# === Reconciliation Trigger ===
if not st.session_state.reconcile_done:
    if st.button("🔄 Reconcile Now"):
        with st.spinner("Processing LLM-based reconciliation..."):
            try:
                df_matched, summary_text, all_expenses, statements = reconcile_preview()
                if not df_matched.empty:
                    st.session_state.reconcile_done = True
                    st.session_state.report_df = df_matched
                    st.session_state.summary_text = summary_text
                    st.session_state.all_expenses = all_expenses
                    st.session_state.statements = statements
                    st.success("✅ Reconciliation completed successfully.")
                else:
                    st.warning("⚠️ No matched transactions found.")
            except Exception as e:
                st.error(f"❌ Reconciliation failed: {e}")
                traceback.print_exc()

if st.session_state.reconcile_done:
    st.success("✅ Reconciliation already completed.")

    # === CSV Download ===
    st.download_button(
        "📥 Download Report CSV",
        st.session_state.report_df.to_csv(index=False).encode("utf-8"),
        file_name="reconciliation_report.csv"
    )

    # === KPI Tabs ===
    tab1, tab2 = st.tabs(["📘 Saving Account Summary", "📙 Current Account Summary"])

    def extract_kpis_from_txt(s3_key: str):
        try:
            content = download_s3_file(S3_BUCKET, s3_key).decode("utf-8")

            # Show raw content for debugging
            st.markdown(f"#### 🐞 Raw Summary from `{s3_key}`")
            st.code(content)

            prompt = ChatPromptTemplate.from_template("""
    You are a helpful assistant. Read the reconciliation summary below and count the number of entries (lines starting with • or bullet points) in each of the following categories. Return a JSON object with the exact following keys and their integer counts:

    {{
    "Total Expense Bills": total number of expenses listed in the summary (matched + unmatched + duplicates),
    "Matched with Bank Statement": number of matched entries,
    "Unmatched Expense Bills": number of unmatched expenses,
    "Unmatched Bank Debits": number of unmatched bank debits,
    "Reimbursements Detected": number of reimbursements,
    "Duplicate Expense Bills": number of duplicate expenses,
    "Duplicate Statement Entries": number of duplicate bank debits (if any)
    }}

    Reconciliation Summary:
    {summary}

    Respond with JSON only. Do not add any text, explanations, or markdown.
    """)

            chain = prompt | ChatGroq(model="llama3-8b-8192", api_key=os.getenv("GROQ_API_KEY")) | StrOutputParser()
            raw = chain.invoke({"summary": content})

            st.markdown("#### 🐞 Raw LLM Output")
            st.code(raw)

            json_str = raw[raw.find("{"): raw.rfind("}") + 1]
            return json.loads(json_str)

        except Exception as e:
            st.error(f"⚠️ KPI parsing failed for {s3_key}: {e}")
            return {
                "Total Expense Bills": 0,
                "Matched with Bank Statement": 0,
                "Unmatched Expense Bills": 0,
                "Unmatched Bank Debits": 0,
                "Reimbursements Detected": 0,
                "Duplicate Expense Bills": 0,
                "Duplicate Statement Entries": 0
            }


    # === Saving Account KPIs ===
    with tab1:
        st.subheader("📘 Saving Account Summary")
        kpis = extract_kpis_from_txt("reconciliation/saving_account_summary.txt")
        st.markdown("#### ✅ Extracted KPI JSON")
        st.json(kpis)

        col1, col2, col3 = st.columns(3)
        col1.metric("📄 Total Bills", kpis.get("Total Expense Bills", 0))
        col2.metric("✅ Matched", kpis.get("Matched with Bank Statement", 0))
        col3.metric("❌ Unmatched Bills", kpis.get("Unmatched Expense Bills", 0))

        col4, col5, col6 = st.columns(3)
        col4.metric("🏦 Unmatched Debits", kpis.get("Unmatched Bank Debits", 0))
        col5.metric("💰 Reimbursements", kpis.get("Reimbursements Detected", 0))
        col6.metric("🧾 Duplicate Bills", kpis.get("Duplicate Expense Bills", 0))

        col7, _, _ = st.columns(3)
        col7.metric("📑 Duplicate Statements", kpis.get("Duplicate Statement Entries", 0))

    # === Current Account KPIs ===
    with tab2:
        st.subheader("📙 Current Account Summary")
        kpis = extract_kpis_from_txt("reconciliation/current_account_summary.txt")
        st.markdown("#### ✅ Extracted KPI JSON")
        st.json(kpis)

        col1, col2, col3 = st.columns(3)
        col1.metric("📄 Total Bills", kpis.get("Total Expense Bills", 0))
        col2.metric("✅ Matched", kpis.get("Matched with Bank Statement", 0))
        col3.metric("❌ Unmatched Bills", kpis.get("Unmatched Expense Bills", 0))

        col4, col5, col6 = st.columns(3)
        col4.metric("🏦 Unmatched Debits", kpis.get("Unmatched Bank Debits", 0))
        col5.metric("💰 Reimbursements", kpis.get("Reimbursements Detected", 0))
        col6.metric("🧾 Duplicate Bills", kpis.get("Duplicate Expense Bills", 0))

        col7, _, _ = st.columns(3)
        col7.metric("📑 Duplicate Statements", kpis.get("Duplicate Statement Entries", 0))

    st.divider()

    # === Optional Section for Raw Data Inspection ===
    col1, col2, col3 = st.columns(3)

    if col1.button("📂 Expense"):
        st.info("Showing parsed expense bills...")
        df_exp = pd.DataFrame(st.session_state.all_expenses)
        if not df_exp.empty:
            st.markdown("### 📋 Raw Expense Data")
            st.dataframe(df_exp)
        else:
            st.warning("⚠️ No expense data found.")

    if col2.button("🏦 Account Statement"):
        st.info("Showing parsed account statements...")
        df_stmt = pd.DataFrame(st.session_state.statements)
        if not df_stmt.empty:
            st.markdown("### 📋 Raw Statement Data")
            st.dataframe(df_stmt)
        else:
            st.warning("⚠️ No statement data available.")

    if col3.button("🧾 Purchase"):
        st.warning("🔧 Purchase order integration not implemented in this version.")
