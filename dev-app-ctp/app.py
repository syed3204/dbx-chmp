import streamlit as st
import pandas as pd
import numpy as np
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, LongType, DecimalType, DateType, IntegerType, DoubleType, FloatType,
)

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
TABLE_FQN = "uc_vdm_salesmargin.default.vendor_master"

NUMERIC_COLS = [
    "vendor_id", "credit_limit", "annual_spend",
    "vendor_rating", "on_time_delivery_pct", "employee_count",
]

STRING_COLS = [
    "vendor_name", "category", "address", "city", "country",
    "currency_code", "payment_terms", "status", "email", "phone", "tax_id",
]

DATE_COLS = ["created_date", "last_order_date", "contract_expiry_date"]


# ──────────────────────────────────────────────
# Spark session (cached)
# ──────────────────────────────────────────────
@st.cache_resource
def get_spark():
    return DatabricksSession.builder.serverless(True).getOrCreate()


# ──────────────────────────────────────────────
# Data helpers (cached per session / refresh)
# ──────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner="Loading table data …")
def load_profile():
    """Run all profiling queries in a single pass and return results dict."""
    spark = get_spark()
    df = spark.table(TABLE_FQN)
    results = {}

    # ── 1. Overview ──────────────────────────
    total_rows = df.count()
    total_cols = len(df.columns)
    dup_rows = total_rows - df.dropDuplicates().count()
    dup_pct = round(dup_rows / total_rows * 100, 2) if total_rows else 0.0

    results["overview"] = {
        "total_rows": total_rows,
        "total_cols": total_cols,
        "dup_rows": dup_rows,
        "dup_pct": dup_pct,
    }

    # ── 2. Completeness ─────────────────────
    null_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]
    null_row = df.select(null_exprs).collect()[0]
    completeness_records = []
    for c in df.columns:
        nulls = int(null_row[c])
        null_pct = round(nulls / total_rows * 100, 2) if total_rows else 0.0
        completeness_records.append(
            {
                "Column": c,
                "Null Count": nulls,
                "Null %": null_pct,
                "Completeness %": round(100 - null_pct, 2),
            }
        )
    results["completeness"] = pd.DataFrame(completeness_records)

    # ── 3. Uniqueness ───────────────────────
    distinct_exprs = [F.countDistinct(F.col(c)).alias(c) for c in df.columns]
    dist_row = df.select(distinct_exprs).collect()[0]
    uniqueness_records = []
    for c in df.columns:
        dist = int(dist_row[c])
        uniq_ratio = round(dist / total_rows * 100, 2) if total_rows else 0.0
        uniqueness_records.append(
            {
                "Column": c,
                "Distinct Count": dist,
                "Uniqueness %": uniq_ratio,
                "Potential ID": "Yes" if dist == total_rows else "",
            }
        )
    results["uniqueness"] = pd.DataFrame(uniqueness_records)

    # ── 4. Numeric statistics ───────────────
    present_numeric = [c for c in NUMERIC_COLS if c in df.columns]
    num_records = []
    for c in present_numeric:
        stats_row = (
            df.select(
                F.min(F.col(c).cast("double")).alias("min_val"),
                F.max(F.col(c).cast("double")).alias("max_val"),
                F.mean(F.col(c).cast("double")).alias("mean_val"),
                F.stddev(F.col(c).cast("double")).alias("stddev_val"),
                F.expr(f"percentile_approx(CAST(`{c}` AS DOUBLE), 0.5)").alias("median_val"),
                F.sum(F.when(F.col(c) == 0, 1).otherwise(0)).alias("zeros"),
                F.sum(F.when(F.col(c) < 0, 1).otherwise(0)).alias("negatives"),
            )
            .collect()[0]
        )
        num_records.append(
            {
                "Column": c,
                "Min": stats_row["min_val"],
                "Max": stats_row["max_val"],
                "Mean": round(stats_row["mean_val"], 4) if stats_row["mean_val"] is not None else None,
                "Median": stats_row["median_val"],
                "Std Dev": round(stats_row["stddev_val"], 4) if stats_row["stddev_val"] is not None else None,
                "Zeros": int(stats_row["zeros"]),
                "Negatives": int(stats_row["negatives"]),
            }
        )
    results["numeric"] = pd.DataFrame(num_records)

    # ── 5. String statistics ────────────────
    present_string = [c for c in STRING_COLS if c in df.columns]
    str_records = []
    for c in present_string:
        s_row = (
            df.select(
                F.min(F.length(F.col(c))).alias("min_len"),
                F.max(F.length(F.col(c))).alias("max_len"),
                F.round(F.mean(F.length(F.col(c))), 2).alias("avg_len"),
                F.sum(F.when((F.col(c) == "") | F.col(c).isNull(), 1).otherwise(0)).alias("empty_count"),
            )
            .collect()[0]
        )
        # Top-5 frequent values
        top5_rows = (
            df.filter(F.col(c).isNotNull())
            .groupBy(c)
            .count()
            .orderBy(F.desc("count"))
            .limit(5)
            .collect()
        )
        top5_str = ", ".join([f"{r[c]} ({r['count']})" for r in top5_rows])
        str_records.append(
            {
                "Column": c,
                "Min Length": s_row["min_len"],
                "Max Length": s_row["max_len"],
                "Avg Length": s_row["avg_len"],
                "Empty/Null Count": int(s_row["empty_count"]),
                "Top 5 Values": top5_str,
            }
        )
    results["string"] = pd.DataFrame(str_records)

    # ── 6. Date statistics ──────────────────
    present_date = [c for c in DATE_COLS if c in df.columns]
    date_records = []
    today = pd.Timestamp.now().date()
    for c in present_date:
        d_row = (
            df.select(
                F.min(F.col(c)).alias("min_date"),
                F.max(F.col(c)).alias("max_date"),
                F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias("nulls"),
                F.sum(F.when(F.col(c) > F.current_date(), 1).otherwise(0)).alias("future"),
            )
            .collect()[0]
        )
        min_d = d_row["min_date"]
        max_d = d_row["max_date"]
        span = (max_d - min_d).days if min_d and max_d else None
        date_records.append(
            {
                "Column": c,
                "Min Date": str(min_d),
                "Max Date": str(max_d),
                "Span (days)": span,
                "Null Count": int(d_row["nulls"]),
                "Future Dates": int(d_row["future"]),
            }
        )
    results["date"] = pd.DataFrame(date_records)

    # ── 7. Data Quality Score ───────────────
    comp_df = results["completeness"]
    avg_completeness = comp_df["Completeness %"].mean() if len(comp_df) else 100
    uniq_df = results["uniqueness"]
    avg_uniqueness = uniq_df["Uniqueness %"].mean() if len(uniq_df) else 100
    # Validity: penalise future dates & high duplicate %
    date_df = results["date"]
    future_penalty = 0
    if len(date_df) and total_rows:
        total_future = date_df["Future Dates"].sum()
        future_penalty = (total_future / (total_rows * len(date_df))) * 100
    validity_score = max(100 - dup_pct - future_penalty, 0)
    quality_score = round((avg_completeness * 0.4 + avg_uniqueness * 0.3 + validity_score * 0.3), 2)
    results["quality_score"] = {
        "overall": quality_score,
        "completeness": round(avg_completeness, 2),
        "uniqueness": round(avg_uniqueness, 2),
        "validity": round(validity_score, 2),
    }

    return results


# ──────────────────────────────────────────────
# Streamlit UI
# ──────────────────────────────────────────────
st.set_page_config(page_title="Data Profiling App", layout="wide")

# Sidebar
with st.sidebar:
    st.title("Data Profiler")
    st.markdown(f"**Table:** `{TABLE_FQN}`")
    if st.button("Refresh Profile", use_container_width=True):
        load_profile.clear()
        st.rerun()
    st.divider()
    st.caption("Metrics are cached for 10 min. Click Refresh to recalculate.")

st.title("Data Quality Profile")
st.markdown(f"#### `{TABLE_FQN}`")

# Load data
profile = load_profile()

# ── KPI row ─────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Rows", f"{profile['overview']['total_rows']:,}")
k2.metric("Total Columns", profile["overview"]["total_cols"])
k3.metric("Duplicate Rows", f"{profile['overview']['dup_rows']:,}")
k4.metric("Duplicate %", f"{profile['overview']['dup_pct']}%")
qs = profile["quality_score"]
k5.metric("Quality Score", f"{qs['overall']}%")

st.divider()

# ── Tabs ────────────────────────────────────
tab_overview, tab_complete, tab_unique, tab_numeric, tab_string, tab_date, tab_quality = st.tabs(
    [
        "Overview",
        "Completeness",
        "Uniqueness",
        "Numeric Stats",
        "String Stats",
        "Date Stats",
        "Quality Score",
    ]
)

# ---- Overview tab ----
with tab_overview:
    st.subheader("Table Overview")
    ov = profile["overview"]
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Rows", f"{ov['total_rows']:,}")
        st.metric("Duplicate Rows", f"{ov['dup_rows']:,}")
    with col2:
        st.metric("Total Columns", ov["total_cols"])
        st.metric("Duplicate Row %", f"{ov['dup_pct']}%")

# ---- Completeness tab ----
with tab_complete:
    st.subheader("Column Completeness")
    comp = profile["completeness"].copy()
    avg_comp = comp["Completeness %"].mean()
    st.progress(avg_comp / 100, text=f"Average Completeness: {avg_comp:.1f}%")
    st.dataframe(
        comp.style.background_gradient(subset=["Completeness %"], cmap="RdYlGn", vmin=0, vmax=100),
        use_container_width=True,
        hide_index=True,
    )

# ---- Uniqueness tab ----
with tab_unique:
    st.subheader("Column Uniqueness")
    uniq = profile["uniqueness"].copy()
    potential_ids = uniq[uniq["Potential ID"] == "Yes"]["Column"].tolist()
    if potential_ids:
        st.success(f"Potential ID columns (100% unique): **{', '.join(potential_ids)}**")
    st.dataframe(
        uniq.style.background_gradient(subset=["Uniqueness %"], cmap="YlOrRd", vmin=0, vmax=100),
        use_container_width=True,
        hide_index=True,
    )

# ---- Numeric tab ----
with tab_numeric:
    st.subheader("Numeric Column Statistics")
    num_df = profile["numeric"]
    if len(num_df):
        st.dataframe(num_df, use_container_width=True, hide_index=True)
    else:
        st.info("No numeric columns detected.")

# ---- String tab ----
with tab_string:
    st.subheader("String Column Statistics")
    str_df = profile["string"]
    if len(str_df):
        st.dataframe(str_df, use_container_width=True, hide_index=True)
    else:
        st.info("No string columns detected.")

# ---- Date tab ----
with tab_date:
    st.subheader("Date Column Statistics")
    dt_df = profile["date"]
    if len(dt_df):
        future_total = dt_df["Future Dates"].sum()
        if future_total > 0:
            st.warning(f"{int(future_total)} future-dated records found across date columns.")
        st.dataframe(dt_df, use_container_width=True, hide_index=True)
    else:
        st.info("No date columns detected.")

# ---- Quality Score tab ----
with tab_quality:
    st.subheader("Data Quality Score")
    qs = profile["quality_score"]
    st.markdown(
        """
        The overall score is a weighted composite:  
        **40%** Completeness + **30%** Uniqueness + **30%** Validity
        """
    )
    q1, q2, q3, q4 = st.columns(4)
    q1.metric("Overall", f"{qs['overall']}%")
    q2.metric("Completeness", f"{qs['completeness']}%")
    q3.metric("Uniqueness", f"{qs['uniqueness']}%")
    q4.metric("Validity", f"{qs['validity']}%")

    # Visual bar
    score_df = pd.DataFrame(
        {
            "Dimension": ["Completeness", "Uniqueness", "Validity", "Overall"],
            "Score": [qs["completeness"], qs["uniqueness"], qs["validity"], qs["overall"]],
        }
    )
    st.bar_chart(score_df.set_index("Dimension"), height=350)
