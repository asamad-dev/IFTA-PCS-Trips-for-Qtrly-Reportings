import os
import io
import asyncio
from datetime import datetime

import streamlit as st
import pandas as pd

import prototype as proto


def step1_clean_and_prepare_from_upload(pcs: pd.DataFrame, inv: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Replicates prototype.step1_read_excel_data cleaning for uploaded DataFrames.
    Applies: trimming/uppercase, date parsing, column renames, Q2 2025 filter, and inventory unit cleanup.
    """
    # Data cleanup and standardization (following prototype Step 1.2)
    pcs = pcs.copy()
    inv = inv.copy()

    # Normalize core columns if present
    for col in ["Truck", "Trailer", "Ship City", "Cons City"]:
        if col in pcs.columns:
            pcs[col] = pcs[col].astype(str).str.strip()

    for col in ["Ship St", "Cons St"]:
        if col in pcs.columns:
            pcs[col] = pcs[col].astype(str).str.upper().str.strip()

    # Date processing and rename to match prototype
    if "PU Date F" in pcs.columns:
        pcs["PU Date F"] = pd.to_datetime(pcs["PU Date F"], errors="coerce")
    if "Del Date F" in pcs.columns:
        pcs["Del Date F"] = pd.to_datetime(pcs["Del Date F"], errors="coerce")

    pcs = pcs.rename(columns={"PU Date F": "PU", "Del Date F": "DEL"})

    # Q2 2025 filter (April 1 - June 30, 2025) to match prototype behavior
    if "PU" in pcs.columns:
        initial_row_count = len(pcs)
        pcs = pcs[(pcs["PU"] >= "2025-04-01") & (pcs["PU"] <= "2025-06-30")]
        st.info(f"Q2 2025 date filter applied: {initial_row_count} → {len(pcs)} rows")

    # Inventory cleanup
    if "Unit" in inv.columns:
        inv["Unit"] = inv["Unit"].astype(str).str.strip()

    return pcs, inv


def run_pipeline(pcs_df: pd.DataFrame, inv_df: pd.DataFrame, api_key: str, max_concurrent: int = 10) -> pd.DataFrame:
    # Step 1 equivalent: clean uploaded data
    pcs_clean, inv_clean = step1_clean_and_prepare_from_upload(pcs_df, inv_df)

    # Step 2
    pcs_filtered = proto.step2_filter_fleet_data(pcs_clean, inv_clean)

    # Step 3
    pcs_with_refs = proto.step3_detect_round_trips(pcs_filtered)

    # Step 5 prerequisites
    states_gdf = proto.load_state_boundaries()

    # Step 5 concurrent mileage
    result_df = asyncio.run(
        proto.step5_calculate_mileage_concurrent(
            pcs_with_refs, states_gdf, api_key, max_concurrent=max_concurrent
        )
    )

    return result_df


st.set_page_config(page_title="IFTA State Miles Calculator", layout="wide")
st.title("IFTA PCS Trips – State Miles Calculator")

st.markdown(
    "Upload an Excel file containing two sheets named `Export Research` and `Inventory details`.\n"
    "The app will run the processing pipeline from `prototype.py` (Steps 1–5) and return state-mile results."
)

with st.sidebar:
    api_key_input = st.text_input("HERE API Key", type="password", help="Required for routing (HERE v8)")
    max_concurrent = st.number_input("Max concurrent requests", min_value=1, max_value=30, value=10, step=1)
    run_button = st.button("Run Calculation", type="primary")

uploaded_file = st.file_uploader("Excel file (.xlsx)", type=["xlsx"]) 

expected_pcs_sheet = "Export Research"
expected_inv_sheet = "Inventory details"

if run_button:
    if not uploaded_file:
        st.error("Please upload an Excel file.")
        st.stop()

    # Determine API key
    api_key = None
    if api_key_input:
        api_key = api_key_input.strip()
    else:
        # Try to load from environment or secrets.toml via prototype
        try:
            api_key = proto.load_api_key()
        except Exception:
            api_key = None

    if not api_key:
        st.error("HERE API key is required (enter in sidebar or configure `secrets.toml`).")
        st.stop()

    try:
        with st.spinner("Reading Excel sheets..."):
            # Read the two required sheets
            pcs_df = pd.read_excel(uploaded_file, sheet_name=expected_pcs_sheet, keep_default_na=False)
            inv_df = pd.read_excel(uploaded_file, sheet_name=expected_inv_sheet, usecols=["Unit", "Company"])

        st.success(
            f"Loaded {len(pcs_df)} rows from `{expected_pcs_sheet}` and {len(inv_df)} rows from `{expected_inv_sheet}`."
        )

        with st.spinner("Running pipeline (Steps 1–5)... this may take several minutes"):
            result_df = run_pipeline(pcs_df, inv_df, api_key, max_concurrent=max_concurrent)

        if result_df is None or result_df.empty:
            st.warning("No results produced.")
            st.stop()

        st.subheader("Results")
        st.dataframe(result_df, use_container_width=True)

        # Provide downloads
        csv_bytes = result_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download CSV",
            data=csv_bytes,
            file_name="state_miles_results.csv",
            mime="text/csv",
        )

        # Offer a formatted view similar to the (commented) Step 6
        output_columns = [
            "Company",
            "Ref No",
            "Load",
            "Trip",
            "Truck",
            "Trailer",
            "PU Date F",
            "Del Date F",
            "State",
            "Miles",
        ]
        formatted_df = result_df.copy()
        # Ensure date formatting
        if "PU Date F" in formatted_df.columns:
            formatted_df["PU Date F"] = pd.to_datetime(formatted_df["PU Date F"], errors="coerce").dt.strftime("%m/%d/%Y")
        if "Del Date F" in formatted_df.columns:
            formatted_df["Del Date F"] = pd.to_datetime(formatted_df["Del Date F"], errors="coerce").dt.strftime("%m/%d/%Y")

        formatted_df = formatted_df[[c for c in output_columns if c in formatted_df.columns]]
        excel_buf = io.BytesIO()
        with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
            formatted_df.to_excel(writer, index=False, sheet_name="State Miles")
        st.download_button(
            label="Download Excel (formatted)",
            data=excel_buf.getvalue(),
            file_name="state_miles_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.success("Done.")

    except Exception as e:
        st.exception(e)

