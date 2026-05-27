import streamlit as st

st.set_page_config(page_title="CX Dashboard", layout="wide")

monthly = st.Page("pages/cx_monthly_metrics.py", title="CX Monthly Metrics", icon="📊")
weekly  = st.Page("pages/cx_weekly_metrics.py",  title="CX Weekly Metrics",  icon="📅")

pg = st.navigation([monthly, weekly])
pg.run()
