import streamlit as st
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import re
import numpy as np
from reconcile_logic import extract_expense_data, extract_statement_entries, reconcile
import json

# Folder Paths
BASE_DIR = os.path.join(os.getcwd(), 'data')
EXPENSE_FOLDER = os.path.join(BASE_DIR, 'expenses')
STATEMENT_FOLDER = os.path.join(BASE_DIR, 'statements')
PURCHASE_JSON = os.path.join(BASE_DIR, 'purchases', 'email_processing_results.json')


# Set page config
st.set_page_config(page_title="Expense Reconciliation", layout="wide")
st.title("💼 Expense Reconciliation Agent")

# State setup
if "reconcile_done" not in st.session_state:
    st.session_state.reconcile_done = False

# Show only reconcile button initially
if not st.session_state.reconcile_done:
    if st.button("🔄 Reconcile Now"):
        with st.spinner("Processing reconciliation..."):
            all_expenses = []
            for file in os.listdir(EXPENSE_FOLDER):
                if file.lower().endswith(".pdf"):
                    all_expenses.extend(extract_expense_data(os.path.join(EXPENSE_FOLDER, file)))

            # Define the path to the statement file (update the filename as needed)
            statement_files = [f for f in os.listdir(STATEMENT_FOLDER) if f.lower().endswith(".pdf")]
            if statement_files:
                STATEMENT_PATH = os.path.join(STATEMENT_FOLDER, statement_files[0])
                statements = extract_statement_entries(STATEMENT_PATH)
                report_df, summary = reconcile(all_expenses, statements)
            else:
                st.error("⚠️ No statement PDF files found in the statements folder.")
                st.stop()

            if not report_df.empty:
                st.session_state.reconcile_done = True
                st.session_state.report_df = report_df
                st.session_state.summary = summary
                st.session_state.all_expenses = all_expenses
                st.session_state.statements = statements
                st.success("✅ Reconciliation complete.")
            else:
                st.error("⚠️ No matches found or error in processing.")

# Show post-reconciliation UI
if st.session_state.reconcile_done:
    st.success("✅ Reconciliation already completed.")

    st.download_button(
        "📥 Download Report CSV",
        st.session_state.report_df.to_csv(index=False).encode("utf-8"),
        file_name="reconciliation_report.csv"
    )

    # Download reconciliation_summary.txt
    if os.path.exists("reconciliation_summary.txt"):
        with open("reconciliation_summary.txt", "r", encoding="utf-8") as f:
            txt_data = f.read()
        st.download_button("📄 Download Summary Report (TXT)", txt_data, file_name="reconciliation_summary.txt")

    # --- Summary KPIs ---
    st.markdown("### 📊 Summary KPIs")
    lines = st.session_state.summary.splitlines()
    metrics = {
        "Total Expense Bills": 0,
        "Matched with Bank Statement": 0,
        "Unmatched Expense Bills": 0,
        "Unmatched Bank Debits": 0,
        "Reimbursements Detected": 0,
        "Duplicate Expense Bills": 0,
        "Duplicate Statement Entries": 0,
    }

    for line in lines:
        for key in metrics:
            if key in line:
                try:
                    metrics[key] = int(re.findall(r"\d+", line)[-1])
                except:
                    pass

    col1, col2, col3 = st.columns(3)
    col1.metric("📄 Total Bills", metrics["Total Expense Bills"])
    col2.metric("✅ Matched", metrics["Matched with Bank Statement"])
    col3.metric("❌ Unmatched Bills", metrics["Unmatched Expense Bills"])

    col4, col5, col6 = st.columns(3)
    col4.metric("🏦 Unmatched Debits", metrics["Unmatched Bank Debits"])
    col5.metric("💰 Reimbursements", metrics["Reimbursements Detected"])
    col6.metric("🧾 Duplicate Bills", metrics["Duplicate Expense Bills"])

    col7, _, _ = st.columns(3)
    col7.metric("📑 Duplicate Statements", metrics["Duplicate Statement Entries"])

    st.divider()

    # --- Operation Buttons ---
    col1, col2, col3 = st.columns(3)

    if col1.button("📂 Expense"):
        st.info("Showing parsed expense bills...")
        df_exp = pd.DataFrame(st.session_state.all_expenses)

        if not df_exp.empty:
            df_exp = df_exp.drop(columns=["file"], errors="ignore")
            st.markdown("### 📋 Raw Expense Data")
            st.dataframe(df_exp)

            if df_exp['amount'].dtype == 'object':
                df_exp['amount'] = pd.to_numeric(df_exp['amount'], errors='coerce')
            df_exp = df_exp.dropna(subset=['amount'])

            category_totals = df_exp.groupby("category", as_index=False)["amount"].sum()
            category_totals = category_totals.sort_values("amount", ascending=False)

            col_left, col_right = st.columns(2)

            with col_left:
                st.markdown("### 📊 Expense Amount by Category (Bar Chart)")
                fig, ax = plt.subplots(figsize=(6, 4))
                bars = ax.bar(category_totals['category'], category_totals['amount'],
                              color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
                for bar, amount in zip(bars, category_totals['amount']):
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height + max(category_totals['amount']) * 0.01,
                            f'₹{amount:,.0f}', ha='center', va='bottom', fontsize=8, fontweight='bold')
                ax.set_xlabel('Category', fontsize=10)
                ax.set_ylabel('Amount (₹)', fontsize=10)
                ax.set_title('Total Amount Spent per Category', fontsize=12)
                ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'₹{x:,.0f}'))
                plt.xticks(rotation=45, fontsize=8)
                plt.yticks(fontsize=8)
                plt.tight_layout()
                st.pyplot(fig)
                plt.close()

            with col_right:
                st.markdown("### 🥧 Category Distribution (Pie Chart)")
                fig, ax = plt.subplots(figsize=(6, 4))
                wedges, texts, autotexts = ax.pie(category_totals['amount'],
                                                  labels=category_totals['category'],
                                                  autopct=lambda pct: f'₹{pct/100*category_totals["amount"].sum():,.0f}\n({pct:.1f}%)',
                                                  startangle=90)
                for text in texts: text.set_fontsize(8)
                for autotext in autotexts:
                    autotext.set_color('white')
                    autotext.set_fontweight('bold')
                    autotext.set_fontsize(7)
                ax.set_title('Expense Distribution by Category', fontsize=12)
                centre_circle = plt.Circle((0, 0), 0.40, fc='white')
                fig.gca().add_artist(centre_circle)
                plt.tight_layout()
                st.pyplot(fig)
                plt.close()

            st.markdown("### 📈 Expense Analysis Summary")
            total_amount = category_totals['amount'].sum()
            highest_category = category_totals.iloc[0]['category']
            highest_amount = category_totals.iloc[0]['amount']

            col_summary1, col_summary2, col_summary3 = st.columns(3)
            col_summary1.metric("💰 Total Expenses", f"₹{total_amount:,.0f}")
            col_summary2.metric("🏆 Highest Category", highest_category, f"₹{highest_amount:,.0f}")
            col_summary3.metric("📊 Categories Count", len(category_totals))
        else:
            st.warning("⚠️ No expense data to show.")

    if col2.button("🏦 Account Statement"):
        st.info("Showing account statement details...")
        df_stmt = pd.DataFrame(st.session_state.statements)

        if not df_stmt.empty:
            st.markdown("### 🧾 Bank Statement Overview")
            total_debit = df_stmt[df_stmt["type"] == "debit"]["amount"].sum()
            total_credit = df_stmt[df_stmt["type"] == "credit"]["amount"].sum()
            total_txns = len(df_stmt)
            cash_withdraw = df_stmt.query("category == 'Cash' and type == 'debit'")["amount"].sum()
            personal_exp = df_stmt.query("category == 'Personal' and type == 'debit'")["amount"].sum()
            charges = df_stmt.query("type == 'fee' or category == 'Charges'")["amount"].sum()

            d1, d2, d3 = st.columns(3)
            d1.metric("💳 Total Debits", f"₹{total_debit:,.2f}")
            d2.metric("💰 Total Credits", f"₹{total_credit:,.2f}")
            d3.metric("📄 Total Transactions", total_txns)

            d4, d5, d6 = st.columns(3)
            d4.metric("🏧 Cash Withdrawals", f"₹{cash_withdraw:,.2f}")
            d5.metric("🛒 Personal Expenses", f"₹{personal_exp:,.2f}")
            d6.metric("⚙️ Charges / Fees", f"₹{charges:,.2f}")

            st.markdown("### 📋 Raw Statement Entries")
            st.dataframe(df_stmt)
            st.markdown("### 📈 Transaction Trends Over Time (Line Chart)")
            try:
                df_stmt_sorted = df_stmt.copy()
                df_stmt_sorted["date"] = pd.to_datetime(df_stmt_sorted["date"])
                df_stmt_sorted = df_stmt_sorted.sort_values("date")

                line_data = df_stmt_sorted.groupby(["date", "type"], as_index=False)["amount"].sum()

                fig, ax = plt.subplots(figsize=(8, 4))
                for txn_type, group in line_data.groupby("type"):
                    ax.plot(group["date"], group["amount"], marker='o', label=txn_type.title())

                ax.set_title("Transaction Amounts Over Time", fontsize=12)
                ax.set_xlabel("Date", fontsize=10)
                ax.set_ylabel("Amount (₹)", fontsize=10)
                ax.legend()
                ax.grid(True, linestyle="--", alpha=0.5)
                plt.xticks(rotation=45, fontsize=8)
                plt.tight_layout()
                st.pyplot(fig)
                plt.close()
            except Exception as e:
                st.error(f"⚠️ Could not render line chart: {e}")

        else:
            st.warning("⚠️ No statement data available.")

    if col3.button("🧾 Purchase"):
            st.info("Loading and analyzing purchase orders...")

            try:
                with open(PURCHASE_JSON, "r", encoding="utf-8") as f:
                    purchase_data = json.load(f)

                purchase_orders = purchase_data.get("purchase_orders", [])
                summary = purchase_data.get("summary", {})

                if not purchase_orders:
                    st.warning("⚠️ No purchase orders found.")
                else:
                    df_orders = pd.json_normalize(purchase_orders)

                    total_orders = summary.get("purchase_orders", len(purchase_orders))
                    total_amount = sum(po["order_details"]["total_amount"] for po in purchase_orders)
                    latest_order_date = max(po["order_details"]["order_date"] for po in purchase_orders)
                    latest_delivery = max(po["order_details"]["delivery_date"] for po in purchase_orders)
                    sources = [po.get("source", "unknown") for po in purchase_orders]
                    source_counts = pd.Series(sources).value_counts()

                    st.markdown("### 🧾 Purchase Order KPIs")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("📦 Total Purchase Orders", total_orders)
                    col2.metric("💰 Total Purchase Amount", f"₹{total_amount:,.2f}")
                    col3.metric("📅 Latest Order Date", latest_order_date)

                    col4, col5, col6 = st.columns(3)
                    col4.metric("🚚 Latest Delivery Date", latest_delivery)
                    col5.metric("📤 Unique Source Types", len(set(sources)))
                    col6.metric("📄 Total Source Entries", len(sources))

                    st.markdown("### 📊 Purchase Orders by Source Type")
                    st.dataframe(source_counts.rename_axis("Source").reset_index(name="Order Count"))

                    st.markdown("### 📋 Raw Purchase Order Snapshot")
                    st.dataframe(df_orders)

            except Exception as e:
                st.error(f"❌ Failed to load or parse JSON file: {e}")

