import streamlit as st

st.set_page_config(page_title="CX Dashboard", layout="wide")

user = st.experimental_user
if not user.is_logged_in:
    st.title("CX Dashboard")
    st.info("Please sign in with your SpotOn Google account to continue.")
    st.button("Sign in with Google", on_click=st.login)
    st.stop()

if not user.email.endswith("@spoton.com"):
    st.error(f"Access denied. This dashboard is restricted to @spoton.com accounts. You are signed in as **{user.email}**.")
    st.button("Sign out", on_click=st.logout)
    st.stop()

monthly = st.Page("pages/cx_monthly_metrics.py", title="CX Monthly Metrics", icon="📊")
weekly  = st.Page("pages/cx_weekly_metrics.py",  title="CX Weekly Metrics",  icon="📅")

pg = st.navigation([monthly, weekly])
pg.run()
