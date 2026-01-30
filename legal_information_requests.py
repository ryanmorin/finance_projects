# --- imports ---
import math
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
from sql import sql1, sql2, sql3
from datetime import date as dt_date
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

st.set_page_config(page_title="Annual Report", layout="wide")

ANNUAL_REPORT_DATASET = sql1   # The query
STATES = sql2                  # select state from ...
WAREHOUSE_NAMES = sql3         # select warehouse_name from ...
PAGE_SIZE = 90_000            ############ this looks to be a safe number ---> MUST BE LESS THAN 32 MB

LOAN_COL  = "LOAN_NUMBER"    # <-- deterministic, indexed if possible
ORDER_BY  = f"{LOAN_COL}, 1" # add a tiebreaker to be fully deterministic

# ------------ UI ---------------
st.image("servicing_image.png", width=200)
st.title("Annual Report - Compliance")
st.write("""The reporting was prepared for the Compliance team for annual reporting purposes.""")
st.divider()

st.write("""Please click the Open Glossary button for a description of the reports fields.""")
st.link_button("Open Glossary (Confluence)", "https://joinmosaic.atlassian.net/wiki/x/AQD6_/")
st.divider()

session = get_active_session()

# ---------- helpers ----------
def get_data_df(session, sql_str):
    return session.sql(sql_str)

def build_filtered_sql(
    base_sql: str,
    start_dt,
    end_dt,
    states: list[str] | None,
    account_status: list[str] | None,
    lender: list[str] | None,
    warehouse_name: list[str] | None,) -> str:
    
    """Return a SQL string with additive, optional filters."""
    # Normalize Nones -> empty lists
    states = states or []
    account_status = account_status or []
    lender = lender or []
    warehouse_name = warehouse_name or []

    def _sql_list(vals: list[str]) -> str:
        # Escape single quotes for SQL string literals
        return ", ".join("'" + v.replace("'", "''") + "'" for v in vals)

    def _in_clause(column: str, vals: list[str]) -> str | None:
        if not vals:
            return None  # no restriction if user didn’t pick anything
        return f"{column} IN ({_sql_list(vals)})"

    def _not_exist_clause() -> str:
        return f"NOT EXISTS (SELECT 1 FROM filtered_cancel_pif WHERE concord.loan_number = filtered_cancel_pif.mosaic_loan_id)"

    parts: list[str] = []

    # MUST have dates - dates correspond to effective_date (TBAL) and posting_date (TXN REPORT)
    if start_dt and end_dt:
        start_dt_str = "'" + start_dt.strftime("%Y-%m-%d") + "'"
        end_dt_str = "'" + end_dt.strftime("%Y-%m-%d") + "'"
        parts.append(f"WITH dates AS (SELECT {start_dt_str} AS start_date, {end_dt_str} AS end_date),")

    # Always wrap base_sql so we can safely add WHERE without worrying if base_sql has one
    parts.append(base_sql)

    # Build additive filters (only add when provided)
    conditions = []
    # Beginning of the conditions build
    c_state = _not_exist_clause()
    conditions.append(c_state)
    
    c_state = _in_clause("install_address.state", states)
    if c_state:
        conditions.append(c_state)

    c_status = _in_clause("concord.pit_concord_account_status", account_status)
    if c_status:
        conditions.append(c_status)

    c_lender = _in_clause("concord.lender", lender)
    if c_lender:
        conditions.append(c_lender)

    c_wh = _in_clause("concord.warehouse_name", warehouse_name)
    if c_wh:
        conditions.append(c_wh)

    if conditions:
        parts.append("WHERE " + " AND ".join(conditions))

    return "\n".join(parts)

@st.cache_data(ttl=3600) # states list rarely changes 
def load_states(): 
    df = ( 
        get_data_df(session, STATES) 
            .select(col("STATE")) 
            .filter(col("STATE").is_not_null()) 
            .distinct() .sort(col("STATE")) 
            .to_pandas() 
    ) 
    return df["STATE"].astype("string").tolist() 
    
def load_warehouse_names(): 
    df = ( 
        get_data_df(session, WAREHOUSE_NAMES) 
            .select(col("WAREHOUSE_NAME")) 
            .filter(col("WAREHOUSE_NAME").is_not_null()) 
            .distinct() .sort(col("WAREHOUSE_NAME")) 
            .to_pandas() 
    ) 
    return df["WAREHOUSE_NAME"].astype("string").tolist() 
    
# ---------- UI filters ---------- 
states_all = load_states() 
warehouse_names_all = load_warehouse_names() 
lender_all = ['connexus', 'mosaic', 'webbank', 'suntrust', 'dcu'] 
account_status_all = ['cancelled', 'transferred to 3rd party (goal)', 'paid in full', 'active'] 

(c1, c2) = st.columns(2) 
with c1: 
    states_all_id = st.multiselect("State(s)", states_all, key="states_all_id") 
    start_date_input = st.date_input( "Start date", value=None, min_value=dt_date(1900, 1, 1), key="start_date_input_key", format="YYYY-MM-DD", ) 
    end_date_input = st.date_input( "End date", value=None, min_value=dt_date(1900, 1, 1), key="end_date_input_key", format="YYYY-MM-DD", ) 
    
with c2: 
    account_status_all_id = st.multiselect("Account Status(es)", account_status_all, key="account_status_all_id") 
    lender_all_id = st.multiselect("Lender(s)", lender_all, key="lender_all_id") 
    warehouse_names_all_id = st.multiselect("Warehouse name(s)", warehouse_names_all, key="warehouse_names_all")

# ---------- Run / build the report context ----------
run = st.button("Run Report", type="primary")

if run:
    # validate dates
    if (start_date_input and not end_date_input) or (end_date_input and not start_date_input):
        st.warning("Please provide both a start and end date (or leave both blank).")
        st.stop()

    base_sql = build_filtered_sql(
        base_sql=ANNUAL_REPORT_DATASET,
        start_dt=start_date_input,
        end_dt=end_date_input,
        states=states_all_id,
        account_status=account_status_all_id,
        lender=lender_all_id,
        warehouse_name=warehouse_names_all_id,
    )

    # Count total rows on the server
    count_sql = f"SELECT COUNT(*) AS CNT FROM ({base_sql})"
    with st.spinner("Counting rows..."):
        total_rows = int(session.sql(count_sql).collect()[0]["CNT"])

    if total_rows == 0:
        # clear any previous run
        st.session_state.pop("report_ctx", None)
        st.info("No rows found for the given filters.")
        st.stop()

    num_pages = max(1, math.ceil(total_rows / PAGE_SIZE))

    # seed session state for pagination
    st.session_state.report_ctx = {
        "base_sql": base_sql,
        "total_rows": total_rows,
        "num_pages": num_pages,
        "page_size": PAGE_SIZE,
        "order_by": ORDER_BY,
    }
    # reset page to 1 for a fresh run
    st.session_state.page = 1

# ---------- Render pager + current page (persists across reruns) ----------
# ---- Pagination state (outside of `if run:`) ----
# Ensure we have a place to store "report_ctx" and "page"
if "report_ctx" in st.session_state and "page" not in st.session_state:
    st.session_state.page = 1

def _sync_page_from_input():
    # When user types a number, copy it into the source-of-truth page
    st.session_state.page = st.session_state.page_input

def _goto_prev():
    if st.session_state.page > 1:
        st.session_state.page -= 1
        # keep the input box in sync for the next run
        st.session_state.page_input = st.session_state.page
        st.rerun()

def _goto_next():
    if st.session_state.page < st.session_state.report_ctx["num_pages"]:
        st.session_state.page += 1
        st.session_state.page_input = st.session_state.page
        st.rerun()

# ---- Render pager + page fetch ----
if "report_ctx" in st.session_state:
    ctx = st.session_state.report_ctx
    num_pages = ctx["num_pages"]
    page_size = ctx["page_size"]

    # 1) Initialize state BEFORE any widgets/callbacks
    if "page" not in st.session_state:
        st.session_state.page = 1
    if "page_input" not in st.session_state:
        st.session_state.page_input = st.session_state.page

    # 2) Handle Prev/Next *before* rendering the number input
    ctopA, ctopB, ctopC, ctopD = st.columns([2,2,1,1])
    with ctopA:
        st.caption(f"{ctx['total_rows']:,} row(s) found. Showing {page_size:,} per page.")
    with ctopC:
        prev_clicked = st.button("◀ Prev", use_container_width=True, key="btn_prev")
    with ctopD:
        next_clicked = st.button("Next ▶", use_container_width=True, key="btn_next")

    # Update page from buttons first (bounded)
    if prev_clicked and st.session_state.page > 1:
        st.session_state.page -= 1
        st.session_state.page_input = st.session_state.page  # keep widget in sync
    if next_clicked and st.session_state.page < num_pages:
        st.session_state.page += 1
        st.session_state.page_input = st.session_state.page  # keep widget in sync

    # 3) Render the number input using a different key, and sync -> page on change
    def _sync_page_from_input():
        # Bound and copy typed value into source-of-truth "page"
        p = int(st.session_state.page_input)
        if p < 1: p = 1
        if p > num_pages: p = num_pages
        st.session_state.page = p

    with ctopB:
        st.number_input(
            "Page",
            min_value=1,
            max_value=num_pages,
            value=st.session_state.page_input,
            step=1,
            key="page_input",
            on_change=_sync_page_from_input,
        )

    # Use the source-of-truth page for OFFSET
    current_page = st.session_state.page
    offset = (current_page - 1) * page_size

    page_sql = f"""
        SELECT *
        FROM ({ctx['base_sql']})
        ORDER BY {ctx['order_by']}
        LIMIT {page_size} OFFSET {offset}
    """
    with st.spinner(f"Loading page {current_page} of {num_pages}..."):
        page_df = session.sql(page_sql).to_pandas()

    st.dataframe(page_df, use_container_width=True, hide_index=True)
else:
    st.info("Set filters and click **Run Report** to load results.")