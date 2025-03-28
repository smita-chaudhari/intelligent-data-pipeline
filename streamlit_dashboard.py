import duckdb
import streamlit as st
import os
import pandas as pd

# Point to your parquet folder
parquet_path = os.path.expanduser('~/intelligent-data-pipeline/output/parquet')

# Load parquet files using DuckDB
query = f"""
SELECT * FROM parquet_scan('{parquet_path}/*.snappy.parquet')
"""
df = duckdb.query(query).to_df()

# Coerce 'amount' column to float safely
if 'amount' in df.columns:
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')


# Streamlit UI
st.title("📊 Financial Transactions Dashboard")
st.markdown("Visualizing real-time simulated financial data from Snappy Parquet files.")

# Dropdown filter
region = st.selectbox("Filter by Region", ["All"] + sorted(df["region"].unique()))
filtered_df = df if region == "All" else df[df["region"] == region]

# Display metrics
st.metric("Total Transactions", f"{len(filtered_df):,}")
st.metric("Total Amount", f"${filtered_df['amount'].astype(float).sum():,.2f}")

# Data preview
st.dataframe(filtered_df)

# Bar chart by security type
if "security_type" in filtered_df.columns:
    chart = filtered_df.groupby("security_type")["amount"].sum().reset_index()
    st.bar_chart(data=chart, x="security_type", y="amount", use_container_width=True)


# 📈 Line Chart: Daily Total Transactions
if "transaction_date" in filtered_df.columns:
    st.subheader("📆 Daily Total Transactions Over Time")
    time_df = filtered_df.copy()
    time_df["transaction_date"] = pd.to_datetime(time_df["transaction_date"], errors="coerce")
    daily_volume = time_df.groupby(time_df["transaction_date"].dt.date)["amount"].sum().reset_index()
    st.line_chart(daily_volume.rename(columns={"transaction_date": "Date", "amount": "Total Amount"}))

# 🥧 Pie Chart: Transaction Share by Region
if "region" in filtered_df.columns:
    st.subheader("🌍 Transaction Share by Region")
    import plotly.express as px  # 👈 Add this import at the top of your script if not already
    region_share = filtered_df.groupby("region")["amount"].sum().reset_index()
    fig = px.pie(region_share, values="amount", names="region", title="Transaction Share by Region")
    st.plotly_chart(fig)

# 📊 Histogram: Distribution of Transaction Amounts
st.subheader("🔍 Distribution of Transaction Amounts")
st.bar_chart(filtered_df["amount"].dropna().astype(float).clip(upper=filtered_df["amount"].quantile(0.95)))

# 🔥 Heatmap: Region vs Security Type
if {"region", "security_type"}.issubset(filtered_df.columns):
    st.subheader("📊 Heatmap of Regions vs Security Types")
    pivot = filtered_df.pivot_table(values="amount", index="region", columns="security_type", aggfunc="sum", fill_value=0)
    st.dataframe(pivot.style.background_gradient(cmap="YlGnBu"))

# 💰 Top 10 Biggest Transactions
st.subheader("💰 Top 10 Biggest Transactions")
st.dataframe(filtered_df.sort_values("amount", ascending=False).head(10))

