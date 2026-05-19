import io
import calendar
from datetime import date, timedelta
import streamlit as st
import snowflake.connector
import pandas as pd

# ── Snowflake connection ──────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def get_connection():
    cfg = st.secrets["snowflake"]
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        token=cfg["token"],
        authenticator="programmatic_access_token",
        role=cfg.get("role", "bi_tools_finance"),
        warehouse=cfg.get("warehouse", "ANALYTICS_WH"),
        database=cfg.get("database", "ANALYTICS"),
        session_parameters={"QUERY_TAG": "streamlit_cx_dashboard"},
    )

def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql(sql, conn)
    df.columns = df.columns.str.lower()
    return df

# ── SQL builder ───────────────────────────────────────────────────────────────

def build_weekly_sql(year: int, month: int) -> str:
    cur_month = f"'{year}-{month:02d}-01'::DATE"
    py_month  = f"'{year - 1}-{month:02d}-01'::DATE"

    def s(metric, prefix, extra=""):
        cond = f"f.metric_name = '{metric}'" + (f" AND {extra}" if extra else "")
        return f"SUM(CASE WHEN {cond} THEN f.metric_value ELSE 0 END) AS {prefix}"

    def med(metric, prefix):
        return f"MEDIAN(CASE WHEN f.metric_name = '{metric}' THEN f.metric_value END) AS {prefix}"

    arr_churn_names = "'rooftop_churn_premier_arr','rooftop_churn_segment_a_arr','rooftop_churn_segment_b_arr','rooftop_churn_segment_c_arr','rooftop_churn_segment_d_arr','rooftop_churn_segment_total_arr'"

    cols = ",\n            ".join([
        s("rooftop_installs_total",              "total"),
        s("rooftop_installs_total",              "restaurant",      "dom.merchant_vertical = 'Restaurant'"),
        s("rooftop_installs_total",              "express",         "dom.merchant_vertical = 'Express'"),
        s("internal_installs",                   "internal"),
        s("third_party_installs",                "external"),
        med("time_to_install",                   "tti"),
        s("installed_arr",                       "arr"),
        s("workable_all_segments_count",         "workable_all"),
        s("non_workable_all_segments_count",     "non_workable_all"),
        s("installed_within_4_months_of_booking","b2i_num"),
        s("booked_prior_4_months",               "b2i_den"),
        s("express_to_rpos_flip",                "express_to_rpos"),
        s("rpos_to_express_flip",                "rpos_to_express"),
        s("activated_threshold_arr_total",       "thr_arr"),
        s("activated_full_month_arr_total",      "fm_arr"),
        s("activated_threshold_mids_total",      "thr_mids"),
        s("activated_full_month_mids_total",     "fm_mids"),
        s("activated_numerator",                 "act_num"),
        s("installs_prior_3_months",             "inst_prior_3m"),
        s("migrations_not_new_biz",              "migr"),
        s("quality_count_green",                 "q_green"),
        s("quality_count_yellow",                "q_yellow"),
        s("quality_count_red",                   "q_red"),
        s("quality_count_not_submitted",         "q_not_sub"),
        s("sfdc_cancellations_premier_count",    "sfdc_premier"),
        s("sfdc_cancellations_segment_a_count",  "sfdc_a"),
        s("sfdc_cancellations_segment_b_count",  "sfdc_b"),
        s("sfdc_cancellations_segment_c_count",  "sfdc_c"),
        s("sfdc_cancellations_segment_d_count",  "sfdc_d"),
        s("sfdc_cancellations_all_segments_count","sfdc_total"),
        s("rooftop_churn_premier_count",         "churn_premier"),
        s("rooftop_churn_segment_a_count",       "churn_a"),
        s("rooftop_churn_segment_b_count",       "churn_b"),
        s("rooftop_churn_segment_c_count",       "churn_c"),
        s("rooftop_churn_segment_d_count",       "churn_d"),
        s("rooftop_churn_all_segments_count",    "churn_total"),
        s("rooftop_churn_premier_arr",           "churn_arr_premier"),
        s("rooftop_churn_segment_a_arr",         "churn_arr_a"),
        s("rooftop_churn_segment_b_arr",         "churn_arr_b"),
        s("rooftop_churn_segment_c_arr",         "churn_arr_c"),
        s("rooftop_churn_segment_d_arr",         "churn_arr_d"),
        f"SUM(CASE WHEN f.metric_name IN ({arr_churn_names}) THEN f.metric_value ELSE 0 END) AS churn_arr_total",
        s("120_day_retention_count",             "ret_num"),
        s("activations_prior_4_months",          "ret_den"),
        s("120_day_retention_count",             "ret_num_premier", "domp.success_segment = 'Premier'"),
        s("activations_prior_4_months",          "ret_den_premier", "domp.success_segment = 'Premier'"),
        s("120_day_retention_count",             "ret_num_a",       "domp.success_segment = 'Segment A'"),
        s("activations_prior_4_months",          "ret_den_a",       "domp.success_segment = 'Segment A'"),
        s("120_day_retention_count",             "ret_num_b",       "domp.success_segment = 'Segment B'"),
        s("activations_prior_4_months",          "ret_den_b",       "domp.success_segment = 'Segment B'"),
        s("120_day_retention_count",             "ret_num_c",       "domp.success_segment = 'Segment C'"),
        s("activations_prior_4_months",          "ret_den_c",       "domp.success_segment = 'Segment C'"),
        s("120_day_retention_count",             "ret_num_d",       "domp.success_segment = 'Segment D'"),
        s("activations_prior_4_months",          "ret_den_d",       "domp.success_segment = 'Segment D'"),
        s("at_risk_count",                       "at_risk"),
        s("at_risk_resolved",                    "at_risk_res"),
        s("at_risk_cancelled",                   "at_risk_can"),
        s("processing_active_count",             "proc_active"),
        s("core_bundle_attached_count",          "cb_attached"),
        s("core_bundle_products_activated",      "cb_products"),
    ])

    metric_names = ",".join([f"'{m}'" for m in [
        "rooftop_installs_total", "internal_installs", "third_party_installs",
        "time_to_install", "installed_arr",
        "workable_all_segments_count", "non_workable_all_segments_count",
        "installed_within_4_months_of_booking", "booked_prior_4_months",
        "express_to_rpos_flip", "rpos_to_express_flip",
        "activated_threshold_arr_total", "activated_full_month_arr_total",
        "activated_threshold_mids_total", "activated_full_month_mids_total",
        "activated_numerator", "installs_prior_3_months",
        "migrations_not_new_biz",
        "quality_count_green", "quality_count_yellow", "quality_count_red", "quality_count_not_submitted",
        "sfdc_cancellations_premier_count", "sfdc_cancellations_segment_a_count",
        "sfdc_cancellations_segment_b_count", "sfdc_cancellations_segment_c_count",
        "sfdc_cancellations_segment_d_count", "sfdc_cancellations_all_segments_count",
        "rooftop_churn_premier_count", "rooftop_churn_segment_a_count",
        "rooftop_churn_segment_b_count", "rooftop_churn_segment_c_count",
        "rooftop_churn_segment_d_count", "rooftop_churn_all_segments_count",
        "rooftop_churn_premier_arr", "rooftop_churn_segment_a_arr",
        "rooftop_churn_segment_b_arr", "rooftop_churn_segment_c_arr",
        "rooftop_churn_segment_d_arr", "rooftop_churn_segment_total_arr",
        "120_day_retention_count", "activations_prior_4_months",
        "at_risk_count", "at_risk_resolved", "at_risk_cancelled",
        "processing_active_count",
        "core_bundle_attached_count", "core_bundle_products_activated",
    ]])

    return f"""
        SELECT
            DATE_TRUNC('week', f.metric_date) AS week_start,
            f.metric_month,
            {cols}
        FROM ANALYTICS.SO_FINANCE.FACT_CX_MONTHLY_METRIC f
        LEFT JOIN ANALYTICS.SO_FINANCE.DIM_ORIGINAL_MERCHANT dom
            ON f.original_merchant_id = dom.original_merchant_id
        LEFT JOIN ANALYTICS.DEV_FINANCE.DIM_ORIGINAL_MERCHANT_PLUS domp
            ON f.original_merchant_id = domp.original_merchant_id
        WHERE f.metric_name IN ({metric_names})
        AND f.metric_month IN ({cur_month}, {py_month})
        GROUP BY DATE_TRUNC('week', f.metric_date), f.metric_month
        ORDER BY f.metric_month, week_start
    """


@st.cache_data(ttl=3600, show_spinner="Loading data…")
def load_weekly(year: int, month: int) -> pd.DataFrame:
    return run_query(build_weekly_sql(year, month))

# ── Formatting helpers ────────────────────────────────────────────────────────

def safe_div(num, den):
    try:
        n, d = float(num or 0), float(den or 0)
        return (n / d * 100) if d != 0 else None
    except Exception:
        return None

def fmt_val(val, fmt_type):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    f = float(val)
    if fmt_type == "arr_k":   return f"${f / 1000:,.0f}"
    if fmt_type == "pct":     return f"{f:.1f}%"
    if fmt_type == "days":    return f"{f:.0f}"
    if fmt_type == "decimal": return f"{f:.2f}"
    return f"{f:,.0f}"

def fmt_chg(abs_val, pct_val, fmt_type, style):
    null_abs = abs_val is None or (isinstance(abs_val, float) and pd.isna(abs_val))
    null_pct = pct_val is None or (isinstance(pct_val, float) and pd.isna(pct_val))

    def fmt_abs(v, ft):
        sign = "-" if v < 0 else ""
        av = abs(v)
        if ft == "pct":     return f"{sign}{av:.1f}%"
        if ft == "arr_k":   return f"{sign}${av / 1000:,.0f}"
        if ft == "decimal": return f"{sign}{av:.2f}"
        return f"{sign}{av:,.0f}"

    if style == "% only":
        if null_pct: return ""
        sign = "-" if pct_val < 0 else ""
        return f"{sign}{abs(pct_val):.1f}%"
    if style == "Δ + %":
        if null_abs: return ""
        delta = fmt_abs(abs_val, fmt_type)
        pct_str = f" ({('-' if pct_val < 0 else '')}{abs(pct_val):.1f}%)" if not null_pct else ""
        return f"{delta}{pct_str}"
    if null_abs: return ""
    return fmt_abs(abs_val, fmt_type)

# ── Pandas Styler ─────────────────────────────────────────────────────────────

def style_changes(df: pd.DataFrame, color: bool, invert: bool, chg_col_pairs):
    if not color:
        return df.style

    def colorize(row):
        styles = {c: "" for c in df.columns}
        sign_flip = -1 if (invert and row.get("inverse", False)) else 1
        for chg_col, abs_col in chg_col_pairs:
            if chg_col not in df.columns or abs_col not in df.columns:
                continue
            v = row[abs_col]
            if v is None or pd.isna(v) or v == 0:
                continue
            direction = (1 if v > 0 else -1) * sign_flip
            if direction > 0:
                styles[chg_col] = "color: #137d3a; background-color: rgba(19,125,58,.08)"
            else:
                styles[chg_col] = "color: #c0303f; background-color: rgba(192,48,63,.08)"
        return pd.Series(styles)

    return df.style.apply(colorize, axis=1)

# ── Build display DataFrame ───────────────────────────────────────────────────

def week_label(week_start: pd.Timestamp, year: int, month: int) -> str:
    last_day = calendar.monthrange(year, month)[1]
    month_start = pd.Timestamp(year, month, 1)
    month_end   = pd.Timestamp(year, month, last_day)
    ws = max(week_start, month_start)
    we = min(week_start + pd.Timedelta(days=6), month_end)
    if ws.day == we.day:
        return f"{ws.strftime('%b')} {int(ws.day)}"
    return f"{ws.strftime('%b')} {int(ws.day)}–{int(we.day)}"


def build_weekly_display_df(current_rows, prior_rows, year, month, wtd_idx=None):
    def gv(row, col):
        return row.get(col, 0) or 0

    SECTIONS = [
        ("INSTALLS", [
            ("Rooftop Installs Total",             lambda r: gv(r, "total"),            "count", False),
            ("Rooftop Installs — Restaurant",      lambda r: gv(r, "restaurant"),       "count", False),
            ("Rooftop Installs — Express",         lambda r: gv(r, "express"),          "count", False),
            ("Internal Installs",                  lambda r: gv(r, "internal"),         "count", False),
            ("External Installs",                  lambda r: gv(r, "external"),         "count", False),
            ("Time to Install",                    lambda r: r.get("tti"),              "days",  True),
            ("Installed ARR ($000s)",              lambda r: gv(r, "arr"),              "arr_k", False),
            ("Workable All Segments",              lambda r: gv(r, "workable_all"),     "count", False),
            ("Workable %",                         lambda r: safe_div(gv(r, "workable_all"), gv(r, "workable_all") + gv(r, "non_workable_all")), "pct", False),
            ("Non-Workable All Segments",          lambda r: gv(r, "non_workable_all"), "count", True),
            ("Booked to Install % (4 months)",     lambda r: safe_div(gv(r, "b2i_num"), gv(r, "b2i_den")), "pct", False),
            ("Booked to Install Numerator",        lambda r: gv(r, "b2i_num"),          "count", False),
            ("Booked to Install Denominator",      lambda r: gv(r, "b2i_den"),          "count", False),
            ("Express to RPOS Flip",               lambda r: gv(r, "express_to_rpos"),  "count", False),
            ("RPOS to Express Flip",               lambda r: gv(r, "rpos_to_express"),  "count", False),
        ]),
        ("ACTIVATION", [
            ("Threshold ARR ($000s)",              lambda r: gv(r, "thr_arr"),          "arr_k", False),
            ("Full Month ARR ($000s)",             lambda r: gv(r, "fm_arr"),           "arr_k", False),
            ("Threshold MIDs",                     lambda r: gv(r, "thr_mids"),         "count", False),
            ("Full Month MIDs",                    lambda r: gv(r, "fm_mids"),          "count", False),
            ("Install to Activation % (3 months)", lambda r: safe_div(gv(r, "act_num"), gv(r, "inst_prior_3m")), "pct", False),
            ("Activated Numerator",                lambda r: gv(r, "act_num"),          "count", False),
            ("Installs Prior 3 Months",            lambda r: gv(r, "inst_prior_3m"),    "count", False),
        ]),
        ("QUALITY", [
            ("Quality % Green",                    lambda r: safe_div(gv(r, "q_green"), gv(r, "q_green") + gv(r, "q_yellow") + gv(r, "q_red")), "pct", False),
            ("Quality % Yellow",                   lambda r: safe_div(gv(r, "q_yellow"), gv(r, "q_green") + gv(r, "q_yellow") + gv(r, "q_red")), "pct", True),
            ("Quality % Red",                      lambda r: safe_div(gv(r, "q_red"), gv(r, "q_green") + gv(r, "q_yellow") + gv(r, "q_red")), "pct", True),
            ("Quality Not Submitted",              lambda r: gv(r, "q_not_sub"),        "count", True),
            ("Quality Yellow + Red",               lambda r: gv(r, "q_yellow") + gv(r, "q_red"), "count", True),
            ("Quality % Yellow + Red",             lambda r: safe_div(gv(r, "q_yellow") + gv(r, "q_red"), gv(r, "q_green") + gv(r, "q_yellow") + gv(r, "q_red")), "pct", True),
        ]),
        ("RETENTION", [
            ("Migrations (Not New Biz)",           lambda r: gv(r, "migr"),             "count", False),
        ]),
        ("ROOFTOP RETENTION", [
            ("SFDC Cancellation Seg Premier",      lambda r: gv(r, "sfdc_premier"),     "count", True),
            ("SFDC Cancellation Seg A",            lambda r: gv(r, "sfdc_a"),           "count", True),
            ("SFDC Cancellation Seg B",            lambda r: gv(r, "sfdc_b"),           "count", True),
            ("SFDC Cancellation Seg C",            lambda r: gv(r, "sfdc_c"),           "count", True),
            ("SFDC Cancellation Seg D",            lambda r: gv(r, "sfdc_d"),           "count", True),
            ("SFDC Cancellation Total",            lambda r: gv(r, "sfdc_total"),       "count", True),
            ("Rooftop Churn Seg Premier",          lambda r: gv(r, "churn_premier"),    "count", True),
            ("Rooftop Churn Seg A",                lambda r: gv(r, "churn_a"),          "count", True),
            ("Rooftop Churn Seg B",                lambda r: gv(r, "churn_b"),          "count", True),
            ("Rooftop Churn Seg C",                lambda r: gv(r, "churn_c"),          "count", True),
            ("Rooftop Churn Seg D",                lambda r: gv(r, "churn_d"),          "count", True),
            ("Rooftop Churn Total",                lambda r: gv(r, "churn_total"),      "count", True),
            ("ARR Churn Seg Premier ($000s)",      lambda r: gv(r, "churn_arr_premier"), "arr_k", True),
            ("ARR Churn Seg A ($000s)",            lambda r: gv(r, "churn_arr_a"),      "arr_k", True),
            ("ARR Churn Seg B ($000s)",            lambda r: gv(r, "churn_arr_b"),      "arr_k", True),
            ("ARR Churn Seg C ($000s)",            lambda r: gv(r, "churn_arr_c"),      "arr_k", True),
            ("ARR Churn Seg D ($000s)",            lambda r: gv(r, "churn_arr_d"),      "arr_k", True),
            ("ARR Churn Total ($000s)",            lambda r: gv(r, "churn_arr_total"),  "arr_k", True),
            ("120 Day Rooftop Retention %",        lambda r: safe_div(gv(r, "ret_num"), gv(r, "ret_den")), "pct", False),
            ("120 Day Retention Numerator",        lambda r: gv(r, "ret_num"),          "count", False),
            ("120 Day Retention Denominator",      lambda r: gv(r, "ret_den"),          "count", False),
            ("120 Day Rooftop Retention % - Premier", lambda r: safe_div(gv(r, "ret_num_premier"), gv(r, "ret_den_premier")), "pct", False),
            ("120 Day Rooftop Retention % - Seg A",   lambda r: safe_div(gv(r, "ret_num_a"), gv(r, "ret_den_a")), "pct", False),
            ("120 Day Rooftop Retention % - Seg B",   lambda r: safe_div(gv(r, "ret_num_b"), gv(r, "ret_den_b")), "pct", False),
            ("120 Day Rooftop Retention % - Seg C",   lambda r: safe_div(gv(r, "ret_num_c"), gv(r, "ret_den_c")), "pct", False),
            ("120 Day Rooftop Retention % - Seg D",   lambda r: safe_div(gv(r, "ret_num_d"), gv(r, "ret_den_d")), "pct", False),
        ]),
        ("RISK", [
            ("At Risk Count",                      lambda r: gv(r, "at_risk"),          "count", True),
            ("At Risk %",                          lambda r: safe_div(gv(r, "at_risk"), gv(r, "proc_active")), "pct", True),
            ("At Risk Resolved",                   lambda r: gv(r, "at_risk_res"),      "count", False),
            ("At Risk Cancelled",                  lambda r: gv(r, "at_risk_can"),      "count", True),
            ("At Risk % Cancellations",            lambda r: safe_div(gv(r, "at_risk_can"), gv(r, "churn_total")), "pct", True),
            ("Processing Active",                  lambda r: gv(r, "proc_active"),      "count", False),
        ]),
        ("CORE BUNDLE", [
            ("Core Bundle Attached",               lambda r: gv(r, "cb_attached"),      "count", False),
            ("Core Bundle Products Activated",     lambda r: gv(r, "cb_products"),      "count", False),
            ("Core Bundle 2+ Products Activated",  lambda r: safe_div(gv(r, "cb_products"), gv(r, "cb_attached")), "decimal", False),
        ]),
    ]

    def chg_pair(a, b):
        try:
            fa, fb = float(a or 0), float(b or 0)
            diff = fa - fb
            pct = (diff / fb * 100) if fb != 0 else None
            return diff, pct
        except Exception:
            return None, None

    n = len(current_rows)

    week_labels = [
        week_label(pd.Timestamp(r["week_start"]), year, month)
        for r in current_rows
    ]

    # W/W compares the two completed weeks before the WTD week.
    # For past months (no WTD), compares the last two weeks of the month.
    # Returns nulls if fewer than two completed weeks exist before the reference point.
    if wtd_idx is not None:
        ref  = wtd_idx - 1   # last completed week
        ref2 = wtd_idx - 2   # week before that
    else:
        ref  = n - 1
        ref2 = n - 2

    records = []
    for section_label, metrics in SECTIONS:
        for label, get_fn, fmt_type, inverse in metrics:
            cur_vals = [get_fn(r) for r in current_rows]

            curr = cur_vals[ref]  if ref  >= 0 else None
            prev = cur_vals[ref2] if ref2 >= 0 else None

            ww_abs, ww_pct = chg_pair(curr, prev)

            rec = {
                "section":  section_label,
                "metric":   label,
                "inverse":  inverse,
                "fmt_type": fmt_type,
            }
            for i, lbl in enumerate(week_labels):
                rec[lbl] = fmt_val(cur_vals[i] if i < n else None, fmt_type)

            rec.update({"ww_abs": ww_abs, "ww_pct": ww_pct})
            records.append(rec)

    return pd.DataFrame(records), week_labels

# ── Month picker options ──────────────────────────────────────────────────────

def month_options():
    today = date.today()
    out = []
    for y in range(2025, today.year + 1):
        last_m = today.month if y == today.year else 12
        for m in range(1, last_m + 1):
            out.append((y, m, date(y, m, 1).strftime("%b '%y")))
    return out

MONTHS = month_options()
default_idx = next(
    (i for i, (y, m, _) in enumerate(MONTHS) if y == date.today().year and m == date.today().month),
    len(MONTHS) - 1,
)

# ── Sidebar ───────────────────────────────────────────────────────────────────

ALL_SECTIONS = ["INSTALLS", "ACTIVATION", "QUALITY", "RETENTION",
                "ROOFTOP RETENTION", "RISK", "CORE BUNDLE"]

with st.sidebar:
    st.markdown("### Filters")
    sel_idx = st.session_state.get("weekly_month_idx", default_idx)
    _c_prev, _c_pick, _c_next = st.columns([1, 4, 1])
    if _c_prev.button("‹", use_container_width=True, disabled=sel_idx == 0):
        st.session_state["weekly_month_idx"] = sel_idx - 1
        st.rerun()
    if _c_next.button("›", use_container_width=True, disabled=sel_idx == len(MONTHS) - 1):
        st.session_state["weekly_month_idx"] = sel_idx + 1
        st.rerun()
    chosen = _c_pick.selectbox(
        "Month",
        options=list(range(len(MONTHS))),
        index=sel_idx,
        format_func=lambda i: MONTHS[i][2],
        label_visibility="collapsed",
    )
    if chosen != sel_idx:
        st.session_state["weekly_month_idx"] = chosen
        st.rerun()
    query = st.text_input("Search metrics", placeholder="e.g. install, ARR…")
    visible_sections = st.pills(
        "Sections",
        options=ALL_SECTIONS,
        default=ALL_SECTIONS,
        selection_mode="multi",
    )
    st.divider()
    st.markdown("### Display")
    change_style   = st.radio("Change cells", ["Δ only", "% only", "Δ + %"], horizontal=True)
    color_changes  = st.toggle("Color W/W",                  value=True)
    invert_inverse = st.toggle("Invert color for ↓-better", value=True)

# ── Header ────────────────────────────────────────────────────────────────────

st.title("CX Weekly Metrics")
st.caption(
    f"Customer Experience · As of **{date.today().strftime('%b %d, %Y')}** · "
    "Weeks run Monday → Sunday, clipped at month boundary"
)

year, month, month_lbl = MONTHS[chosen]

# ── Load data ─────────────────────────────────────────────────────────────────

raw = load_weekly(year, month)

if raw.empty:
    st.warning("No data found for this month.")
    st.stop()

cur_ts = pd.Timestamp(year, month, 1)
py_ts  = pd.Timestamp(year - 1, month, 1)

raw["metric_month"] = pd.to_datetime(raw["metric_month"]).dt.to_period("M").dt.to_timestamp()

current_rows = (
    raw[raw["metric_month"] == cur_ts]
    .sort_values("week_start")
    .to_dict("records")
)
prior_rows = (
    raw[raw["metric_month"] == py_ts]
    .sort_values("week_start")
    .to_dict("records")
)

if not current_rows:
    st.warning("No weekly data available for the selected month.")
    st.stop()

# ── WTD detection (before df build so W/W logic can use it) ──────────────────

wtd_idx = None
wtd_col = None
today   = date.today()
if year == today.year and month == today.month:
    for i, r in enumerate(current_rows):
        ws = pd.Timestamp(r["week_start"]).date()
        we = ws + timedelta(days=6)
        if ws <= today <= we:
            wtd_idx = i
            break

# ── Build display dataframe ───────────────────────────────────────────────────

df_full, week_labels = build_weekly_display_df(current_rows, prior_rows, year, month, wtd_idx)

# Rename WTD column
if wtd_idx is not None and wtd_idx < len(week_labels):
    original_lbl = week_labels[wtd_idx]
    wtd_lbl = f"{original_lbl} 🟢 WTD"
    df_full = df_full.rename(columns={original_lbl: wtd_lbl})
    week_labels = [wtd_lbl if l == original_lbl else l for l in week_labels]
    wtd_col = wtd_lbl

# ── Sections ──────────────────────────────────────────────────────────────────

ww_hdr   = "W/W (2 wks before current)"
ww_d_hdr = "W/W Δ"
ww_p_hdr = "W/W %"

ALWAYS_HIDDEN = {
    "section": None, "inverse": None, "fmt_type": None,
    "ww_abs": None, "ww_pct": None,
}

BASE_COLS = {
    "metric": st.column_config.TextColumn("Metric", width="large"),
    **{lbl: st.column_config.TextColumn(lbl, width="small") for lbl in week_labels},
}

if change_style == "Δ + %":
    chg_col_pairs = [("W/W Δ", "ww_abs"), ("W/W %", "ww_abs")]
    col_cfg = {
        **ALWAYS_HIDDEN, **BASE_COLS,
        "W/W":   None,
        "W/W Δ": st.column_config.TextColumn(ww_d_hdr, width="small"),
        "W/W %": st.column_config.TextColumn(ww_p_hdr, width="small"),
    }
else:
    chg_col_pairs = [("W/W", "ww_abs")]
    col_cfg = {
        **ALWAYS_HIDDEN, **BASE_COLS,
        "W/W":   st.column_config.TextColumn(ww_hdr, width="small"),
        "W/W Δ": None, "W/W %": None,
    }

def apply_wtd_style(df, wtd_col):
    styles = pd.DataFrame("", index=df.index, columns=df.columns)
    if wtd_col and wtd_col in df.columns:
        styles[wtd_col] = "font-style: italic; font-weight: 700; background-color: rgba(19,125,58,.05)"
    return styles

# Status bar
n_weeks = len(week_labels)
month_start = date(year, month, 1)
month_end   = date(year, month, calendar.monthrange(year, month)[1])
status_parts = [
    month_lbl,
    f"{n_weeks} week{'s' if n_weeks != 1 else ''}",
    f"{month_start.strftime('%b')} {month_start.day} – {month_end.strftime('%b')} {month_end.day}",
]
status_left = "  ·  ".join(status_parts)
wtd_note = "🟢  Current week is week-to-date" if wtd_col else ""
st.caption(f"{status_left}" + (f"   {wtd_note}" if wtd_note else ""))

st.divider()

for section_name in ALL_SECTIONS:
    if section_name not in visible_sections:
        continue
    section_df = df_full[df_full["section"] == section_name].copy()
    if query:
        section_df = section_df[section_df["metric"].str.contains(query, case=False, na=False)]

    label = f"{section_name}  ·  {len(section_df)} metrics"
    with st.expander(label, expanded=True):
        if section_df.empty:
            st.info(f'No metrics matching "{query}".')
        else:
            if change_style == "Δ + %":
                section_df["W/W Δ"] = section_df.apply(
                    lambda r: fmt_chg(r["ww_abs"], r["ww_pct"], r["fmt_type"], "Δ only"), axis=1)
                section_df["W/W %"] = section_df.apply(
                    lambda r: fmt_chg(r["ww_abs"], r["ww_pct"], r["fmt_type"], "% only"), axis=1)
            else:
                section_df["W/W"] = section_df.apply(
                    lambda r: fmt_chg(r["ww_abs"], r["ww_pct"], r["fmt_type"], change_style), axis=1)

            styled = (
                style_changes(section_df, color_changes, invert_inverse, chg_col_pairs)
                .apply(apply_wtd_style, axis=None, wtd_col=wtd_col)
            )
            st.dataframe(
                styled,
                column_config=col_cfg,
                hide_index=True,
                use_container_width=True,
                height=min(38 * (len(section_df) + 1) + 6, 800),
            )

# ── Export ────────────────────────────────────────────────────────────────────

export_df = df_full[df_full["section"].isin(visible_sections)].copy()
export_df["W/W"] = export_df.apply(
    lambda r: fmt_chg(r["ww_abs"], r["ww_pct"], r["fmt_type"], "Δ only"), axis=1)
export_cols = ["section", "metric"] + week_labels + ["W/W"]
export_df = export_df[export_cols].copy()
export_df.columns = ["Section", "Metric"] + week_labels + ["W/W"]

buf = io.StringIO()
export_df.to_csv(buf, index=False)

st.download_button(
    label="⬇  Export CSV",
    data=buf.getvalue(),
    file_name=f"cx_weekly_metrics_{year}-{month:02d}.csv",
    mime="text/csv",
)
