import math
from datetime import date, datetime
from typing import Optional

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, row_number, current_timestamp
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import (
    StructType, StructField, LongType, IntegerType, StringType, DateType, TimestampType
)

# -----------------------------------------------------
# Setup
# -----------------------------------------------------
session = get_active_session()

# External constants & SQL
from sql_files import TARGET_DB        # e.g. "FUNWORKSPACEDB"
from sql_files import TARGET_SCHEMA    # e.g. "DATA_ANALYTICS"
from sql_files import MILESTONE_TBL    # e.g. "MILESTONE_APPROVAL_HISTORY_FORBRIGHT"
from sql_files import DATA_TBL         # e.g. "COGS_811_DATA"
from sql_files import sql1             # CREATE TABLE IF NOT EXISTS output history table

PAGE_SIZE = 200

# Fully-qualified names:
# - Snowpark APIs (session.table, save_as_table) -> UNQUOTED
# - Raw SQL strings                              -> QUOTED
SRC_TBL_SP   = f"{TARGET_DB}.{TARGET_SCHEMA}.{DATA_TBL}"
HIST_TBL_SP  = f"{TARGET_DB}.{TARGET_SCHEMA}.{MILESTONE_TBL}"
SRC_TBL_SQL  = f'"{TARGET_DB}"."{TARGET_SCHEMA}"."{DATA_TBL}"'
HIST_TBL_SQL = f'"{TARGET_DB}"."{TARGET_SCHEMA}"."{MILESTONE_TBL}"'

# -----------------------------------------------------
# UI
# -----------------------------------------------------
st.set_page_config(page_title="Milestone Tracking", layout="wide")
st.image("servicing_image.png", width=200)
st.title("Milestone Approval Date Input App")

# -----------------------------------------------------
# Identity helpers
# -----------------------------------------------------
def get_current_user() -> Optional[str]:
    try:
        u = getattr(st, "user", None)
        if not u:
            return None
        # most of the time we don't have usernames / names, only email
        for field in ("email", "username", "name"):
            v = getattr(u, field, None)
            if v:
                return str(v)
    except Exception:
        pass
    return None  # explicit

def resolve_audit_user_lazy() -> Optional[str]:
    """Invisible until needed; caches once entered."""
    if "resolved_user" in st.session_state and st.session_state.resolved_user:
        return st.session_state.resolved_user

    auto = get_current_user()
    if auto:
        st.session_state.resolved_user = auto
        return auto

    # Only render if still missing
    with st.expander("Set audit username", expanded=True):
        user = st.text_input("Username", key="audit_user_input") or None
        if user:
            st.session_state.resolved_user = user
            st.success(f"Saved audit user: {user}")
            st.rerun()
    return st.session_state.get("resolved_user")

# -----------------------------------------------------
# Business helpers
# -----------------------------------------------------
def milestone_names(milestone) -> Optional[str]:
    try:
        m = int(milestone)
    except (TypeError, ValueError):
        return None
    if m == 1:
        return "Equipment"
    elif m == 2:
        return "Installation"
    elif m == 3:
        return "Operation"
    else:
        return None

def to_pydate(x):
    """Coerce to python date or None (handles Timestamp/str/NaT)."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if pd.isna(x):
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, (pd.Timestamp, datetime)):
        return x.date()
    try:
        return pd.to_datetime(x).date()
    except Exception:
        return None

# -----------------------------------------------------
# Ensure history table exists
# -----------------------------------------------------
session.sql(sql1).collect()

# -----------------------------------------------------
# Session state for load gating
# -----------------------------------------------------
if "loaded" not in st.session_state:
    st.session_state.loaded = False

# -----------------------------------------------------
# Filters
# -----------------------------------------------------
st.subheader("Filters")
with st.spinner("Loading filter options..."):
    loan_ids_all = (
        session.table(SRC_TBL_SP)
        .select(col("LOAN_ID")).filter(col("LOAN_ID").is_not_null())
        .distinct().sort(col("LOAN_ID")).to_pandas()["LOAN_ID"]
        .astype("int64", errors="ignore").tolist()
    )
    opp_ids_all = (
        session.table(SRC_TBL_SP)
        .select(col("OPPORTUNITY_ID")).filter(col("OPPORTUNITY_ID").is_not_null())
        .distinct().sort(col("OPPORTUNITY_ID")).to_pandas()["OPPORTUNITY_ID"]
        .astype("int64", errors="ignore").tolist()
    )
    cp_names_all = (
        session.table(SRC_TBL_SP)
        .select(col("CHANNEL_PARTNER_NAME")).filter(col("CHANNEL_PARTNER_NAME").is_not_null())
        .distinct().sort(col("CHANNEL_PARTNER_NAME")).to_pandas()["CHANNEL_PARTNER_NAME"]
        .astype("string").tolist()
    )
    milestones_all = (
        session.table(SRC_TBL_SP)
        .select(col("MILESTONE")).filter(col("MILESTONE").is_not_null())
        .distinct().sort(col("MILESTONE")).to_pandas()["MILESTONE"]
        .astype("int64", errors="ignore").tolist()
    )

# Ensure we have an audit user (prompt only if missing)
resolved_user = resolve_audit_user_lazy()

c1, c2, c3, c4 = st.columns(4)
with c1:
    sel_loan_ids = st.multiselect("loan_id", loan_ids_all, key="loan_ids_sel")
with c2:
    sel_opp_ids = st.multiselect("opportunity_id", opp_ids_all, key="opp_ids_sel")
with c3:
    sel_cp_names = st.multiselect("channel_partner_name", cp_names_all, key="cp_sel")
with c4:
    sel_milestones = st.multiselect("milestone", milestones_all, key="milestones_sel")

st.caption(f"{len(loan_ids_all)} loan_ids • {len(opp_ids_all)} opp_ids • "
           f"{len(cp_names_all)} partners • {len(milestones_all)} milestones")

has_any_filter = any([sel_loan_ids, sel_opp_ids, sel_cp_names, sel_milestones])

load_cols = st.columns([1, 1, 3])
with load_cols[0]:
    load_clicked = st.button("Load results", type="primary", disabled=not has_any_filter, key="btn_load")
with load_cols[1]:
    clear_clicked = st.button("Clear results", type="secondary", key="btn_clear")

# Maintain loaded state across reruns
if clear_clicked:
    st.session_state.loaded = False
if load_clicked:
    st.session_state.loaded = True

if not st.session_state.loaded:
    if not has_any_filter:
        st.info("Select at least one filter, then click **Load results** to view rows.", icon="ℹ️")
    st.stop()

# -----------------------------------------------------
# Data loading & prefill
# -----------------------------------------------------
def build_filtered_base_snow():
    sp_df = session.table(SRC_TBL_SP).select(
        col("LOAN_ID"),
        col("MILESTONE"),
        col("OPPORTUNITY_ID"),
        col("PROJECT_ID"),
        col("CHANNEL_PARTNER_NAME"),
    )
    if sel_loan_ids:
        sp_df = sp_df.filter(col("LOAN_ID").isin([int(x) for x in sel_loan_ids]))
    if sel_opp_ids:
        sp_df = sp_df.filter(col("OPPORTUNITY_ID").isin([int(x) for x in sel_opp_ids]))
    if sel_milestones:
        sp_df = sp_df.filter(col("MILESTONE").isin([int(x) for x in sel_milestones]))
    if sel_cp_names:
        sp_df = sp_df.filter(col("CHANNEL_PARTNER_NAME").isin(sel_cp_names))

    # Deduplicate to one row per (loan_id, milestone), stable order by opportunity_id
    w = Window.partitionBy(col("LOAN_ID"), col("MILESTONE")).orderBy(col("OPPORTUNITY_ID").desc_nulls_last())
    sp_df = sp_df.with_column("RN", row_number().over(w)).filter(col("RN") == 1).drop("RN")
    return sp_df

def load_base_df() -> pd.DataFrame:
    base_sp = build_filtered_base_snow()
    base_df = base_sp.to_pandas()
    if base_df.empty:
        return base_df

    # Pull latest history for the keys to prefill
    keys = [(int(r["LOAN_ID"]), int(r["MILESTONE"])) for _, r in base_df.iterrows()]
    values_sql = ", ".join([f"({k[0]}, {k[1]})" for k in keys])

    hist_sql = f"""
        WITH keys(loan_id, milestone) AS (SELECT * FROM VALUES {values_sql}),
        latest_hist AS (
            SELECT
                h.loan_id,
                h.milestone,
                h.milestone_complete_date,
                h.milestone_complete_user,
                h.last_updated_ts
            FROM {HIST_TBL_SQL} h
            JOIN keys k
              ON k.loan_id = h.loan_id
             AND k.milestone = h.milestone
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY h.loan_id, h.milestone
                ORDER BY h.last_updated_ts DESC
            ) = 1
        )
        SELECT * FROM latest_hist
    """
    latest_hist = session.sql(hist_sql).to_pandas()

    merged = base_df.merge(
        latest_hist, on=["LOAN_ID", "MILESTONE"], how="left", suffixes=("", "_hist")
    )

    merged["MILESTONE_NAME"] = merged["MILESTONE"].apply(milestone_names)
    merged["PREFILL_COMPLETE_DATE"] = merged["MILESTONE_COMPLETE_DATE"]
    if "PREFILL_COMPLETE_DATE" in merged.columns:
        merged["PREFILL_COMPLETE_DATE"] = pd.to_datetime(merged["PREFILL_COMPLETE_DATE"]).dt.date
    merged["EDIT_COMPLETE_DATE"] = merged["PREFILL_COMPLETE_DATE"]

    # Desired column order (put editable date next to partner name)
    cols = [
        "LOAN_ID",
        "MILESTONE",
        "MILESTONE_NAME",
        "OPPORTUNITY_ID",
        "PROJECT_ID",
        "CHANNEL_PARTNER_NAME",
        "EDIT_COMPLETE_DATE",
        "PREFILL_COMPLETE_DATE",
    ]
    merged = merged[cols].sort_values(["LOAN_ID", "MILESTONE"], kind="mergesort").reset_index(drop=True)
    return merged

@st.cache_data(show_spinner=False)
def get_base_df_cached(filter_signature: str):
    return load_base_df()

filter_signature = str((
    tuple(sel_loan_ids) if sel_loan_ids else (),
    tuple(sel_opp_ids) if sel_opp_ids else (),
    tuple(sel_cp_names) if sel_cp_names else (),
    tuple(sel_milestones) if sel_milestones else (),
))

with st.spinner("Loading rows..."):
    base_df = get_base_df_cached(filter_signature)

total_rows = len(base_df)
st.caption(f"{total_rows} row(s) found. (Paginated, {PAGE_SIZE} per page)")

if total_rows == 0:
    st.stop()

num_pages = max(1, math.ceil(total_rows / PAGE_SIZE))
page = st.number_input("Page", min_value=1, max_value=num_pages, value=1, step=1, help="Navigate pages to edit all rows.")
start_idx = (page - 1) * PAGE_SIZE
end_idx = min(start_idx + PAGE_SIZE, total_rows)
page_df = base_df.iloc[start_idx:end_idx].copy()

# -----------------------------------------------------
# Editable table
# -----------------------------------------------------
st.subheader("Edit milestone complete date")

form_key = f"edit_form_page_{page}"
column_order = [
    "LOAN_ID",
    "MILESTONE",
    "MILESTONE_NAME",
    "OPPORTUNITY_ID",
    "PROJECT_ID",
    "CHANNEL_PARTNER_NAME",
    "EDIT_COMPLETE_DATE",
    "PREFILL_COMPLETE_DATE",
]

with st.form(key=form_key):
    edited = st.data_editor(
        page_df,
        num_rows="fixed",
        use_container_width=True,
        hide_index=True,
        column_order=column_order,
        column_config={
            "LOAN_ID": {"disabled": True},
            "MILESTONE": {"disabled": True},
            "MILESTONE_NAME": {"disabled": True},
            "OPPORTUNITY_ID": {"disabled": True},
            "PROJECT_ID": {"disabled": True},
            "CHANNEL_PARTNER_NAME": {"disabled": True},
            "EDIT_COMPLETE_DATE": {
                "label": "New milestone_complete_date (editable)",
                "type": "date",
                "required": False,
                "help": "Leave blank to keep unchanged. Clearing a non-blank prefill will save a NULL.",
            },
            "PREFILL_COMPLETE_DATE": {"disabled": True, "label": "Latest saved date"},
        },
    )
    submit_clicked = st.form_submit_button("Save changes for this page")

# -----------------------------------------------------
# Save logic
# -----------------------------------------------------
def insert_history_rows(rows: list[dict], user_name: str) -> int:
    if not rows:
        return 0

    schema = StructType([
        StructField("OPPORTUNITY_ID", LongType()),
        StructField("LOAN_ID", LongType()),
        StructField("PROJECT_ID", LongType()),
        StructField("MILESTONE", IntegerType()),
        StructField("MILESTONE_NAME", StringType()),
        StructField("CHANNEL_PARTNER_NAME", StringType()),
        StructField("MILESTONE_COMPLETE_USER", StringType()),
        StructField("MILESTONE_COMPLETE_DATE", DateType()),
        StructField("LAST_UPDATED_USER", StringType()),
        StructField("LAST_UPDATED_TS", TimestampType()),
    ])

    records = []
    for r in rows:
        records.append({
            "OPPORTUNITY_ID": int(r["OPPORTUNITY_ID"]) if pd.notna(r["OPPORTUNITY_ID"]) else None,
            "LOAN_ID": int(r["LOAN_ID"]),
            "PROJECT_ID": int(r["PROJECT_ID"]) if pd.notna(r["PROJECT_ID"]) else None,
            "MILESTONE": int(r["MILESTONE"]),
            "MILESTONE_NAME": milestone_names(int(r["MILESTONE"])),
            "CHANNEL_PARTNER_NAME": str(r["CHANNEL_PARTNER_NAME"]) if pd.notna(r["CHANNEL_PARTNER_NAME"]) else None,
            "MILESTONE_COMPLETE_USER": user_name,
            "MILESTONE_COMPLETE_DATE": to_pydate(r["EDIT_COMPLETE_DATE"]),
            "LAST_UPDATED_USER": user_name,
            "LAST_UPDATED_TS": None,
        })

    try:
        df = session.create_dataframe(records, schema=schema) \
                     .with_column("LAST_UPDATED_TS", current_timestamp())
        df.write.mode("append").save_as_table(HIST_TBL_SP)
        return len(records)
    except Exception as e:
        st.error(f"Insert failed: {e}")
        return 0

if submit_clicked:
    user_name = st.session_state.get("resolved_user") or resolved_user
    if not user_name:
        st.error("A username is required to save changes. Please enter it above.")
    else:
        to_insert = []
        for _, row in edited.iterrows():
            prefill = to_pydate(row["PREFILL_COMPLETE_DATE"])
            new_date = to_pydate(row["EDIT_COMPLETE_DATE"])
            changed = (
                (prefill is None and new_date is not None) or
                (prefill is not None and new_date is None) or
                (prefill is not None and new_date is not None and new_date != prefill)
            )
            if changed:
                to_insert.append(row)

        st.caption(f"Save debug → rows changed on this page: {len(to_insert)}")
        inserted = insert_history_rows(to_insert, user_name)

        if inserted == 0:
            st.info("No changes saved (either nothing changed or insert failed).")
        else:
            st.success(f"Saved {inserted} change(s) to history.")
            st.cache_data.clear()
            st.rerun()