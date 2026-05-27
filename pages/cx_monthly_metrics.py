import io
import streamlit as st
import pandas as pd
from datetime import datetime
from snowflake.snowpark.context import get_active_session

# ── Snowflake connection ──────────────────────────────────────────────────────

def run_query(sql: str) -> pd.DataFrame:
    session = get_active_session()
    df = session.sql(sql).to_pandas()
    df.columns = df.columns.str.lower()
    return df

# ── SQL builder ───────────────────────────────────────────────────────────────

def build_sql() -> str:
    MONTHS = [("m0", "cur"), ("m1", "m1"), ("m2", "m2"), ("m12", "yp")]

    def s(metric, prefix, extra=""):
        cond = f"f.metric_name = '{metric}'" + (f" AND {extra}" if extra else "")
        return ", ".join([
            f"SUM(CASE WHEN {cond} AND f.metric_month = m.{mv} THEN f.metric_value ELSE 0 END) AS {prefix}_{sx}"
            for mv, sx in MONTHS
        ])

    def med(metric, prefix):
        return ", ".join([
            f"MEDIAN(CASE WHEN f.metric_name = '{metric}' AND f.metric_month = m.{mv} THEN f.metric_value END) AS {prefix}_{sx}"
            for mv, sx in MONTHS
        ])

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
        ", ".join([f"SUM(CASE WHEN f.metric_name IN ({arr_churn_names}) AND f.metric_month = m.{mv} THEN f.metric_value ELSE 0 END) AS churn_arr_total_{sx}" for mv, sx in MONTHS]),
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
        WITH months AS (
            SELECT
                DATEADD(month, -1,  DATE_TRUNC('month', CURRENT_DATE)) AS m0,
                DATEADD(month, -2,  DATE_TRUNC('month', CURRENT_DATE)) AS m1,
                DATEADD(month, -3,  DATE_TRUNC('month', CURRENT_DATE)) AS m2,
                DATEADD(month, -13, DATE_TRUNC('month', CURRENT_DATE)) AS m12
        )
        SELECT
            MIN(m.m0)  AS current_month,
            MIN(m.m1)  AS m1_month,
            MIN(m.m2)  AS m2_month,
            MIN(m.m12) AS year_prior_month,
            {cols}
        FROM ANALYTICS.SO_FINANCE.FACT_CX_MONTHLY_METRIC f
        LEFT JOIN ANALYTICS.SO_FINANCE.DIM_ORIGINAL_MERCHANT dom
            ON f.original_merchant_id = dom.original_merchant_id
        LEFT JOIN ANALYTICS.DEV_FINANCE.DIM_ORIGINAL_MERCHANT_PLUS domp
            ON f.original_merchant_id = domp.original_merchant_id
        CROSS JOIN months m
        WHERE f.metric_name IN ({metric_names})
        AND f.metric_month IN (m.m0, m.m1, m.m2, m.m12)
    """


@st.cache_data(ttl=3600, show_spinner="Loading data…")
def load_scorecard() -> pd.DataFrame:
    return run_query(build_sql())

# ── Build DataFrame ───────────────────────────────────────────────────────────

def build_df(row) -> pd.DataFrame:
    def safe_div(num, den):
        try:
            n, d = float(num or 0), float(den or 0)
            return (n / d * 100) if d != 0 else None
        except Exception:
            return None

    def v(col):
        return row.get(col, 0) or 0

    SECTIONS = [
        ("INSTALLS", [
            ("Rooftop Installs Total",             lambda s: v(f"total_{s}"),            "count", False),
            ("Rooftop Installs — Restaurant",      lambda s: v(f"restaurant_{s}"),       "count", False),
            ("Rooftop Installs — Express",         lambda s: v(f"express_{s}"),          "count", False),
            ("Internal Installs",                  lambda s: v(f"internal_{s}"),         "count", False),
            ("External Installs",                  lambda s: v(f"external_{s}"),         "count", False),
            ("Time to Install",                    lambda s: row.get(f"tti_{s}"),        "days",  True),
            ("Installed ARR ($000s)",              lambda s: v(f"arr_{s}"),              "arr_k", False),
            ("Workable All Segments",              lambda s: v(f"workable_all_{s}"),     "count", False),
            ("Workable %",                         lambda s: safe_div(v(f"workable_all_{s}"), v(f"workable_all_{s}") + v(f"non_workable_all_{s}")), "pct", False),
            ("Non-Workable All Segments",          lambda s: v(f"non_workable_all_{s}"), "count", True),
            ("Booked to Install % (4 months)",     lambda s: safe_div(v(f"b2i_num_{s}"), v(f"b2i_den_{s}")), "pct", False),
            ("Booked to Install Numerator",        lambda s: v(f"b2i_num_{s}"),          "count", False),
            ("Booked to Install Denominator",      lambda s: v(f"b2i_den_{s}"),          "count", False),
            ("Express to RPOS Flip",               lambda s: v(f"express_to_rpos_{s}"),  "count", False),
            ("RPOS to Express Flip",               lambda s: v(f"rpos_to_express_{s}"),  "count", False),
        ]),
        ("ACTIVATION", [
            ("Threshold ARR ($000s)",              lambda s: v(f"thr_arr_{s}"),          "arr_k", False),
            ("Full Month ARR ($000s)",             lambda s: v(f"fm_arr_{s}"),           "arr_k", False),
            ("Threshold MIDs",                     lambda s: v(f"thr_mids_{s}"),         "count", False),
            ("Full Month MIDs",                    lambda s: v(f"fm_mids_{s}"),          "count", False),
            ("Install to Activation % (3 months)", lambda s: safe_div(v(f"act_num_{s}"), v(f"inst_prior_3m_{s}")), "pct", False),
            ("Activated Numerator",                lambda s: v(f"act_num_{s}"),          "count", False),
            ("Installs Prior 3 Months",            lambda s: v(f"inst_prior_3m_{s}"),    "count", False),
        ]),
        ("QUALITY", [
            ("Quality % Green",                    lambda s: safe_div(v(f"q_green_{s}"), v(f"q_green_{s}") + v(f"q_yellow_{s}") + v(f"q_red_{s}")), "pct", False),
            ("Quality % Yellow",                   lambda s: safe_div(v(f"q_yellow_{s}"), v(f"q_green_{s}") + v(f"q_yellow_{s}") + v(f"q_red_{s}")), "pct", True),
            ("Quality % Red",                      lambda s: safe_div(v(f"q_red_{s}"), v(f"q_green_{s}") + v(f"q_yellow_{s}") + v(f"q_red_{s}")), "pct", True),
            ("Quality Not Submitted",              lambda s: v(f"q_not_sub_{s}"),        "count", True),
            ("Quality Yellow + Red",               lambda s: v(f"q_yellow_{s}") + v(f"q_red_{s}"), "count", True),
            ("Quality % Yellow + Red",             lambda s: safe_div(v(f"q_yellow_{s}") + v(f"q_red_{s}"), v(f"q_green_{s}") + v(f"q_yellow_{s}") + v(f"q_red_{s}")), "pct", True),
        ]),
        ("RETENTION", [
            ("Migrations (Not New Biz)",           lambda s: v(f"migr_{s}"),             "count", False),
        ]),
        ("ROOFTOP RETENTION", [
            ("SFDC Cancellation Seg Premier",      lambda s: v(f"sfdc_premier_{s}"),     "count", True),
            ("SFDC Cancellation Seg A",            lambda s: v(f"sfdc_a_{s}"),           "count", True),
            ("SFDC Cancellation Seg B",            lambda s: v(f"sfdc_b_{s}"),           "count", True),
            ("SFDC Cancellation Seg C",             lambda s: v(f"sfdc_c_{s}"),           "count", True),
            ("SFDC Cancellation Seg D",            lambda s: v(f"sfdc_d_{s}"),           "count", True),
            ("SFDC Cancellation Total",            lambda s: v(f"sfdc_total_{s}"),       "count", True),
            ("Rooftop Churn Seg Premier",          lambda s: v(f"churn_premier_{s}"),    "count", True),
            ("Rooftop Churn Seg A",                lambda s: v(f"churn_a_{s}"),          "count", True),
            ("Rooftop Churn Seg B",                lambda s: v(f"churn_b_{s}"),          "count", True),
            ("Rooftop Churn Seg C",                lambda s: v(f"churn_c_{s}"),          "count", True),
            ("Rooftop Churn Seg D",                lambda s: v(f"churn_d_{s}"),          "count", True),
            ("Rooftop Churn Total",                lambda s: v(f"churn_total_{s}"),      "count", True),
            ("ARR Churn Seg Premier ($000s)",      lambda s: v(f"churn_arr_premier_{s}"), "arr_k", True),
            ("ARR Churn Seg A ($000s)",            lambda s: v(f"churn_arr_a_{s}"),      "arr_k", True),
            ("ARR Churn Seg B ($000s)",            lambda s: v(f"churn_arr_b_{s}"),      "arr_k", True),
            ("ARR Churn Seg C ($000s)",            lambda s: v(f"churn_arr_c_{s}"),      "arr_k", True),
            ("ARR Churn Seg D ($000s)",            lambda s: v(f"churn_arr_d_{s}"),      "arr_k", True),
            ("ARR Churn Total ($000s)",            lambda s: v(f"churn_arr_total_{s}"),  "arr_k", True),
            ("120 Day Rooftop Retention %",        lambda s: safe_div(v(f"ret_num_{s}"), v(f"ret_den_{s}")), "pct", False),
            ("120 Day Retention Numerator",        lambda s: v(f"ret_num_{s}"),          "count", False),
            ("120 Day Retention Denominator",      lambda s: v(f"ret_den_{s}"),          "count", False),
            ("120 Day Rooftop Retention % - Premier", lambda s: safe_div(v(f"ret_num_premier_{s}"), v(f"ret_den_premier_{s}")), "pct", False),
            ("120 Day Rooftop Retention % - Seg A",   lambda s: safe_div(v(f"ret_num_a_{s}"), v(f"ret_den_a_{s}")), "pct", False),
            ("120 Day Rooftop Retention % - Seg B",   lambda s: safe_div(v(f"ret_num_b_{s}"), v(f"ret_den_b_{s}")), "pct", False),
            ("120 Day Rooftop Retention % - Seg C",   lambda s: safe_div(v(f"ret_num_c_{s}"), v(f"ret_den_c_{s}")), "pct", False),
            ("120 Day Rooftop Retention % - Seg D",   lambda s: safe_div(v(f"ret_num_d_{s}"), v(f"ret_den_d_{s}")), "pct", False),
        ]),
        ("RISK", [
            ("At Risk Count",                      lambda s: v(f"at_risk_{s}"),          "count", True),
            ("At Risk %",                          lambda s: safe_div(v(f"at_risk_{s}"), v(f"proc_active_{s}")), "pct", True),
            ("At Risk Resolved",                   lambda s: v(f"at_risk_res_{s}"),      "count", False),
            ("At Risk Cancelled",                  lambda s: v(f"at_risk_can_{s}"),      "count", True),
            ("At Risk % Cancellations",            lambda s: safe_div(v(f"at_risk_can_{s}"), v(f"churn_total_{s}")), "pct", True),
            ("Processing Active",                  lambda s: v(f"proc_active_{s}"),      "count", False),
        ]),
        ("CORE BUNDLE", [
            ("Core Bundle Attached",               lambda s: v(f"cb_attached_{s}"),      "count", False),
            ("Core Bundle Products Activated",     lambda s: v(f"cb_products_{s}"),      "count", False),
            ("Core Bundle 2+ Products Activated",  lambda s: safe_div(v(f"cb_products_{s}"), v(f"cb_attached_{s}")), "decimal", False),
        ]),
    ]

    def fmt_val(val, fmt_type):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "—"
        f = float(val)
        if fmt_type == "arr_k":   return f"${f / 1000:,.0f}"
        if fmt_type == "pct":     return f"{f:.1f}%"
        if fmt_type == "days":    return f"{f:.0f}"
        if fmt_type == "decimal": return f"{f:.2f}"
        return f"{f:,.0f}"

    def chg(a, b):
        try:
            fa, fb = float(a or 0), float(b or 0)
            diff = fa - fb
            pct = (diff / fb * 100) if fb != 0 else None
            return diff, pct
        except Exception:
            return None, None

    rows = []
    for section_label, metrics in SECTIONS:
        for label, get_fn, fmt_type, inverse in metrics:
            cur_v = get_fn("cur")
            m1_v  = get_fn("m1")
            m2_v  = get_fn("m2")
            yp_v  = get_fn("yp")
            mom_abs, mom_pct = chg(cur_v, m1_v)
            yoy_abs, yoy_pct = chg(cur_v, yp_v)
            rows.append({
                "section":  section_label,
                "metric":   label,
                "inverse":  inverse,
                "fmt_type": fmt_type,
                "yp":       fmt_val(yp_v,  fmt_type),
                "m2":       fmt_val(m2_v,  fmt_type),
                "m1":       fmt_val(m1_v,  fmt_type),
                "cur":      fmt_val(cur_v, fmt_type),
                "mom_abs":  mom_abs,
                "mom_pct":  mom_pct,
                "yoy_abs":  yoy_abs,
                "yoy_pct":  yoy_pct,
            })

    return pd.DataFrame(rows)

# ── Module-level change formatter ────────────────────────────────────────────

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


# ── Pandas Styler for change columns ─────────────────────────────────────────

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

# ── Load data ─────────────────────────────────────────────────────────────────

raw = load_scorecard()

if raw.empty:
    st.warning("No data found.")
    st.stop()

row = raw.iloc[0].to_dict()

def month_label(col):
    val = row.get(col)
    return pd.Timestamp(val).strftime("%b '%y") if val and not pd.isna(val) else "—"

cur_lbl = month_label("current_month")
m1_lbl  = month_label("m1_month")
m2_lbl  = month_label("m2_month")
yp_lbl  = month_label("year_prior_month")

# ── Sidebar ───────────────────────────────────────────────────────────────────

ALL_SECTIONS = ["INSTALLS", "ACTIVATION", "QUALITY", "RETENTION",
                "ROOFTOP RETENTION", "RISK", "CORE BUNDLE"]

with st.sidebar:
    st.markdown("### Filters")
    query = st.text_input("Search metrics", placeholder="e.g. install, ARR…")
    visible_sections = st.pills(
        "Sections",
        options=ALL_SECTIONS,
        default=ALL_SECTIONS,
        selection_mode="multi",
    )
    st.divider()
    st.markdown("### Display")
    change_style = st.radio(
        "Change cells",
        ["Δ only", "% only", "Δ + %"],
        horizontal=True,
    )
    color_changes  = st.toggle("Color M/M & Y/Y",        value=True)
    invert_inverse = st.toggle("Invert color for ↓-better", value=True)

# ── Header ────────────────────────────────────────────────────────────────────

st.title("CX Monthly Metrics")
st.caption(
    f"Customer Experience · As of **{datetime.today().strftime('%b %d, %Y')}** · "
    f"Comparing last 3 months vs. {yp_lbl}"
)

# ── Headline metrics ──────────────────────────────────────────────────────────

def _pct(num_key, den_key, suffix="cur"):
    n = float(row.get(f"{num_key}_{suffix}") or 0)
    d = float(row.get(f"{den_key}_{suffix}") or 0)
    return (n / d * 100) if d else None

def _delta_str(cur, prev, fmt_type):
    if cur is None or prev is None:
        return None
    diff = float(cur or 0) - float(prev or 0)
    sign = "+" if diff >= 0 else ""
    if fmt_type == "pct":     return f"{sign}{diff:.1f}pp vs {m1_lbl}"
    if fmt_type == "arr_k":   return f"{sign}${diff / 1000:,.0f}K vs {m1_lbl}"
    return f"{sign}{diff:,.0f} vs {m1_lbl}"

installs_cur  = float(row.get("total_cur") or 0)
installs_prev = float(row.get("total_m1") or 0)
arr_cur       = float(row.get("arr_cur") or 0)
arr_prev      = float(row.get("arr_m1") or 0)
act_cur       = _pct("act_num", "inst_prior_3m", "cur")
act_prev      = _pct("act_num", "inst_prior_3m", "m1")
ret_cur       = _pct("ret_num", "ret_den", "cur")
ret_prev      = _pct("ret_num", "ret_den", "m1")

ww_cur  = float(row.get("workable_all_cur") or 0) + float(row.get("non_workable_all_cur") or 0)
ww_m1   = float(row.get("workable_all_m1") or 0) + float(row.get("non_workable_all_m1") or 0)
work_cur  = (float(row.get("workable_all_cur") or 0) / ww_cur * 100) if ww_cur else None
work_prev = (float(row.get("workable_all_m1") or 0) / ww_m1 * 100) if ww_m1 else None

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Rooftop Installs",        f"{installs_cur:,.0f}",
          _delta_str(installs_cur, installs_prev, "count"))
c2.metric("Installed ARR",           f"${arr_cur / 1_000_000:.1f}M",
          _delta_str(arr_cur, arr_prev, "arr_k"))
c3.metric("Workable %",              f"{work_cur:.1f}%" if work_cur else "—",
          _delta_str(work_cur, work_prev, "pct"))
c4.metric("Install to Activation %", f"{act_cur:.1f}%" if act_cur else "—",
          _delta_str(act_cur, act_prev, "pct"))
c5.metric("120-Day Retention %",     f"{ret_cur:.1f}%" if ret_cur else "—",
          _delta_str(ret_cur, ret_prev, "pct"))

st.divider()

# ── Build full dataframe ──────────────────────────────────────────────────────

df_full = build_df(row)

# ── Sections ──────────────────────────────────────────────────────────────────

mom_hdr   = f"M/M  {m1_lbl}→{cur_lbl}"
yoy_hdr   = f"Y/Y  {yp_lbl}→{cur_lbl}"
mom_d_hdr = f"M/M Δ  {m1_lbl}→{cur_lbl}"
mom_p_hdr = f"M/M %  {m1_lbl}→{cur_lbl}"
yoy_d_hdr = f"Y/Y Δ  {yp_lbl}→{cur_lbl}"
yoy_p_hdr = f"Y/Y %  {yp_lbl}→{cur_lbl}"

ALWAYS_HIDDEN = {
    "section": None, "inverse": None, "fmt_type": None,
    "mom_abs": None, "mom_pct": None, "yoy_abs": None, "yoy_pct": None,
}

BASE_COLS = {
    "metric": st.column_config.TextColumn("Metric", width="large"),
    "yp":     st.column_config.TextColumn(yp_lbl,   width="small"),
    "m2":     st.column_config.TextColumn(m2_lbl,   width="small"),
    "m1":     st.column_config.TextColumn(m1_lbl,   width="small"),
    "cur":    st.column_config.TextColumn(cur_lbl,  width="small"),
}

if change_style == "Δ + %":
    chg_col_pairs = [
        ("M/M Δ", "mom_abs"), ("M/M %", "mom_abs"),
        ("Y/Y Δ", "yoy_abs"), ("Y/Y %", "yoy_abs"),
    ]
    col_cfg = {
        **ALWAYS_HIDDEN,
        **BASE_COLS,
        "M/M":   None,
        "Y/Y":   None,
        "M/M Δ": st.column_config.TextColumn(mom_d_hdr, width="small"),
        "M/M %": st.column_config.TextColumn(mom_p_hdr, width="small"),
        "Y/Y Δ": st.column_config.TextColumn(yoy_d_hdr, width="small"),
        "Y/Y %": st.column_config.TextColumn(yoy_p_hdr, width="small"),
    }
else:
    chg_col_pairs = [("M/M", "mom_abs"), ("Y/Y", "yoy_abs")]
    col_cfg = {
        **ALWAYS_HIDDEN,
        **BASE_COLS,
        "M/M":   st.column_config.TextColumn(mom_hdr, width="small"),
        "Y/Y":   st.column_config.TextColumn(yoy_hdr, width="small"),
        "M/M Δ": None, "M/M %": None, "Y/Y Δ": None, "Y/Y %": None,
    }

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
                section_df["M/M Δ"] = section_df.apply(
                    lambda r: fmt_chg(r["mom_abs"], r["mom_pct"], r["fmt_type"], "Δ only"), axis=1
                )
                section_df["M/M %"] = section_df.apply(
                    lambda r: fmt_chg(r["mom_abs"], r["mom_pct"], r["fmt_type"], "% only"), axis=1
                )
                section_df["Y/Y Δ"] = section_df.apply(
                    lambda r: fmt_chg(r["yoy_abs"], r["yoy_pct"], r["fmt_type"], "Δ only"), axis=1
                )
                section_df["Y/Y %"] = section_df.apply(
                    lambda r: fmt_chg(r["yoy_abs"], r["yoy_pct"], r["fmt_type"], "% only"), axis=1
                )
            else:
                section_df["M/M"] = section_df.apply(
                    lambda r: fmt_chg(r["mom_abs"], r["mom_pct"], r["fmt_type"], change_style), axis=1
                )
                section_df["Y/Y"] = section_df.apply(
                    lambda r: fmt_chg(r["yoy_abs"], r["yoy_pct"], r["fmt_type"], change_style), axis=1
                )
            styled = style_changes(section_df, color_changes, invert_inverse, chg_col_pairs)
            st.dataframe(
                styled,
                column_config=col_cfg,
                hide_index=True,
                use_container_width=True,
                height=min(38 * (len(section_df) + 1) + 6, 800),
            )

# ── Export ────────────────────────────────────────────────────────────────────

export_base = df_full[df_full["section"].isin(visible_sections)].copy()
export_base["M/M"] = export_base.apply(
    lambda r: fmt_chg(r["mom_abs"], r["mom_pct"], r["fmt_type"], "Δ only"), axis=1
)
export_base["Y/Y"] = export_base.apply(
    lambda r: fmt_chg(r["yoy_abs"], r["yoy_pct"], r["fmt_type"], "Δ only"), axis=1
)
export_df = export_base[["section", "metric", "yp", "m2", "m1", "cur", "M/M", "Y/Y"]].copy()
export_df.columns = ["Section", "Metric", yp_lbl, m2_lbl, m1_lbl, cur_lbl, "M/M", "Y/Y"]

csv_buf = io.StringIO()
export_df.to_csv(csv_buf, index=False)

st.download_button(
    label="⬇  Export CSV",
    data=csv_buf.getvalue(),
    file_name=f"cx_monthly_metrics_{datetime.today().strftime('%Y-%m-%d')}.csv",
    mime="text/csv",
)
