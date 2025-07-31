#!/usr/bin/env python3
"""
IFTA PCS Trips Processing System - Complete Prototype
Based on plan.md requirements with phases 1-6 implementation
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', message='invalid value encountered in intersection')  # Suppress shapely geometric warnings

import pandas as pd
import numpy as np
import requests
import polyline
import shapely.geometry as geom
import geopandas as gpd
import toml
import aiohttp
import asyncio
from datetime import datetime
import logging
import math
import json
import time

# ──────────────────────────────────────────────────────────────────────────────
# Configuration & Constants
# ──────────────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
INPUT_FILE = BASE_DIR / "M-G PCS Trips PCS A sterling group 2Q 2025 07.23.2025 - AJ.xlsx"
PCS_SHEET = "Export Research 07-22-2025 "  # Note: trailing space in actual Excel file
INV_SHEET = "Inventory details"
STATE_SHP = BASE_DIR / "cb_2024_us_state_500k.shp"
OUTPUT_DIR = BASE_DIR / "output"
DEBUG_DIR = BASE_DIR / "debug"  # Directory for phase-by-phase CSV outputs
SECRETS_FILE = BASE_DIR / "secrets.toml"
COMPANY_NAME = "Ansh Freight"

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure debug directory exists
DEBUG_DIR.mkdir(exist_ok=True)

# State abbreviation to full name mapping
STATE_MAPPING = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
    'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
    'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
    'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
    'DC': 'District of Columbia'
}

# def clean_location_name(city: str) -> str:
#     """
#     Clean location names for better geocoding accuracy
#     Strip warehouse IDs, building numbers, and other details that can cause geocoding errors
#     """
#     if not city or pd.isna(city):
#         return city
    
#     # Convert to string and clean
#     city = str(city).strip()
    
#     # Remove common warehouse/building identifiers
#     unwanted_patterns = [
#         r'\s+\d+$',  # Remove trailing numbers (building numbers)
#         r'\s+#\d+.*$',  # Remove # followed by numbers
#         r'\s+BLDG.*$',  # Remove BLDG and everything after
#         r'\s+BUILDING.*$',  # Remove BUILDING and everything after  
#         r'\s+WAREHOUSE.*$',  # Remove WAREHOUSE and everything after
#         r'\s+DC\s*\d*$',  # Remove DC (Distribution Center) with optional numbers
#         r'\s+WH\s*\d*$',  # Remove WH (Warehouse) with optional numbers
#     ]
    
#     import re
#     for pattern in unwanted_patterns:
#         city = re.sub(pattern, '', city, flags=re.IGNORECASE)
    
#     # Clean up extra spaces
#     city = ' '.join(city.split())
    
#     return city

def load_api_key() -> str:
    """Load HERE API key from environment or secrets.toml"""
    key = os.environ.get("HERE_API_KEY")
    if key:
        return key
    
    try:
        cfg = toml.load(SECRETS_FILE)
        key = cfg.get("HERE_API_KEY") or cfg.get("HERE_KEY")
        if key:
            return key
    except Exception as e:
        logger.error(f"Error loading secrets.toml: {e}")
    
    raise RuntimeError("HERE_API_KEY not found in environment or secrets.toml")

# # ──────────────────────────────────────────────────────────────────────────────
# # Phase 1: Data Import and Initial Processing
# # ──────────────────────────────────────────────────────────────────────────────

def step1_read_excel_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Phase 1: Read Excel data and perform initial cleanup
    Following plan.md Step 1.1 & 1.2
    """
    logger.info("Phase 1: Reading and cleaning Excel data...")
    
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")
    
    # Read main trip data and inventory data
    pcs = pd.read_excel(INPUT_FILE, sheet_name=PCS_SHEET, keep_default_na=False)
    inv = pd.read_excel(INPUT_FILE, sheet_name=INV_SHEET, usecols=["Unit", "Company"])
    logger.info(f"Read {len(pcs)} rows from {PCS_SHEET} sheet")
    logger.info(f"Read {len(inv)} rows from {INV_SHEET} sheet")
    
    # Data cleanup and standardization (following plan.md Step 1.2)
    logger.info("Performing data cleanup...")
    pcs["Truck"] = pcs["Truck"].astype(str).str.strip()
    pcs["Trailer"] = pcs["Trailer"].astype(str).str.strip()
    pcs["Ship City"] = pcs["Ship City"].astype(str).str.strip()
    pcs["Ship St"] = pcs["Ship St"].str.upper().str.strip()
    pcs["Cons City"] = pcs["Cons City"].astype(str).str.strip()
    pcs["Cons St"] = pcs["Cons St"].str.upper().str.strip()
    
    # Date processing
    pcs["PU Date F"] = pd.to_datetime(pcs["PU Date F"], errors='coerce')
    pcs["Del Date F"] = pd.to_datetime(pcs["Del Date F"], errors='coerce')
    
    # Rename columns for consistency (following plan.md)
    pcs.rename(columns={"PU Date F": "PU", "Del Date F": "DEL"}, inplace=True)
    
    # CRITICAL FIX: Filter for Q2 2025 only (April 1 - June 30, 2025)
    initial_row_count = len(pcs)
    pcs = pcs[(pcs['PU'] >= '2025-04-01') & (pcs['PU'] <= '2025-06-30')]
    logger.info(f"Q2 2025 date filter applied: {initial_row_count} → {len(pcs)} rows")
    
    # Inventory cleanup
    inv['Unit'] = inv['Unit'].astype(str).str.strip()
    
    logger.info("Phase 1 completed successfully")
    
    # Save debug CSV outputs
    pcs_debug_file = DEBUG_DIR / "phase1_pcs_cleaned.csv"
    inv_debug_file = DEBUG_DIR / "phase1_inventory.csv"
    pcs.to_csv(pcs_debug_file, index=False)
    inv.to_csv(inv_debug_file, index=False)
    logger.info(f"Phase 1 debug files saved:")
    logger.info(f"  • PCS data: {pcs_debug_file}")  
    logger.info(f"  • Inventory data: {inv_debug_file}")
    
    return pcs, inv

# # ──────────────────────────────────────────────────────────────────────────────
# # Phase 2: Data Filtering and Preparation
# # ──────────────────────────────────────────────────────────────────────────────

def step2_filter_fleet_data(pcs: pd.DataFrame, inv: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 2: Filter fleet data following plan.md Step 2.1 & 2.2
    
    NOTE: Plan.md has incorrect filter logic for Company field.
    Plan shows: pcs[pcs["Company"].isna()] (keep owner-operators)
    Correct: pcs[pcs["Company"].notna()] (keep company-owned units)
    
    Also using more robust numeric truck filter instead of specific "OP"/"NA" checks.
    """
    logger.info("Phase 2: Filtering fleet data...")
    
    initial_count = len(pcs)
    
    # Apply filters in correct order (data quality checks first)
    # 2-A: Drop purely intrastate trips
    pcs = pcs[pcs["Ship St"] != pcs["Cons St"]]
    logger.info(f"After dropping intrastate trips: {len(pcs)} rows")
    
    # 2-B: Drop owner-operators with 'OP' in Truck column (enhanced filter)
    pcs = pcs[~pcs['Truck'].str.contains(r'\bOP\b', na=False, case=False)]
    logger.info(f"After filtering out OP trucks: {len(pcs)} rows")
    
    # 2-C: Keep only numeric truck numbers (additional robustness check)
    pcs = pcs[pd.to_numeric(pcs["Truck"], errors='coerce').notna()]
    logger.info(f"After keeping only numeric truck numbers: {len(pcs)} rows")
    
    # 2-D: Handle 5-digit permit cards vs 4-digit inventory units
    # Create cleaned truck number (first 4 digits) for inventory lookup
    pcs['Truck_clean'] = pcs['Truck'].str[:4]
    logger.info(f"Created Truck_clean column for inventory matching")
    
    # 2-E: Merge with inventory using cleaned truck numbers
    pcs = pcs.merge(inv, how="left", left_on="Truck_clean", right_on="Unit")
    logger.info(f"After merging with inventory: {len(pcs)} rows")
    logger.info(f"Company-owned units found: {pcs['Company'].count()}")
    
    # 2-F: Keep company-owned units (CORRECTED from plan.md)
    pcs = pcs[pcs["Company"].notna()]
    logger.info(f"After keeping company-owned units: {len(pcs)} rows")
    
    # Initialize reference system (following plan.md Step 2.2)
    pcs["Ref"] = pd.NA
    # pcs["CA_Cities"] = ""
    
    # Sort by trailer first, then chronological PU date (Step 3 will not resort)
    pcs = pcs.sort_values(["Trailer", "PU", "Load"]).reset_index(drop=True)
    
    # Drop unnecessary columns (including Truck_clean helper and duplicate Unit from merge)
    columns_to_drop = ["TLH Rev", "Class", "Status", "Customer", "Cust Ref", "Delivered By", "Shipper", "Consignee", "Load Notes", "Unit", "Truck_clean"]
    pcs = pcs.drop(columns=[col for col in columns_to_drop if col in pcs.columns], errors='ignore')
    
    logger.info(f"Phase 2 completed: Filtered from {initial_count} to {len(pcs)} rows")
    logger.info(f"--------------------------------")
    
    # Save debug CSV output
    debug_file = DEBUG_DIR / "phase2_filtered_fleet.csv"
    pcs.to_csv(debug_file, index=False)
    logger.info(f"Phase 2 debug file saved: {debug_file}")
    
    return pcs

# # ──────────────────────────────────────────────────────────────────────────────
# # Phase 3: Trip Grouping and Reference Assignment
# # ──────────────────────────────────────────────────────────────────────────────

def step3_detect_round_trips(pcs: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 3: Assign reference numbers by Truck+Trailer with date gap validation
    Data should already be sorted by Trailer, PU date from Step 2
    CORRECTED logic per feedback.md: No CA consolidation, each load gets own decimal
    Note: One trailer can have multiple trucks over time (chronologically)
    """
    logger.info("Phase 3: Detecting round trip patterns and assigning references...")
    
    pcs = pcs.copy()
    ref_counter = 1
    round_trips_found = 0
    
    # Initialize reference column
    pcs["Ref"] = pd.NA
    
    # Get unique trailer numbers - data already sorted by Trailer, PU date from Step 2
    unique_trailers = sorted(pcs['Trailer'].unique())
    logger.info(f"Processing {len(unique_trailers)} unique trailers")
    
    for trailer in unique_trailers:
        # Get all loads for this trailer (already sorted by PU date from Step 2)
        trailer_loads = pcs[pcs['Trailer'] == trailer].copy()
        trailer_indices = trailer_loads.index.tolist()
        
        logger.debug(f"Processing Trailer {trailer}: {len(trailer_loads)} loads")
        
        decimal_counter = 1
        prev_del_date = None
        prev_truck = None
        
        for idx in trailer_indices:
            current_load = pcs.loc[idx]
            current_truck = current_load['Truck']
            
            # Check for reference group break conditions:
            # 1. Date gap > 3 days (feedback.md requirement)
            # 2. Truck change (different truck assigned to same trailer)
            should_break = False
            break_reason = ""
            
            if prev_del_date is not None:
                gap_days = (current_load['PU'] - prev_del_date).days
                if gap_days > 3:
                    should_break = True
                    break_reason = f"Date gap {gap_days} days > 3"
            
            if prev_truck is not None and current_truck != prev_truck:
                should_break = True
                if break_reason:
                    break_reason += f" and truck change ({prev_truck} → {current_truck})"
                else:
                    break_reason = f"Truck change ({prev_truck} → {current_truck})"
            
            if should_break:
                # Start new reference group
                ref_counter += 1
                decimal_counter = 1
                logger.debug(f"{break_reason}: Load {current_load['Load']} starts new reference group {ref_counter}")
            
            # Assign reference: each load gets own decimal (feedback.md requirement)
            pcs.loc[idx, 'Ref'] = f"{ref_counter}.{decimal_counter}"
            
            # Check for round trip pattern (CA return)
            if (decimal_counter > 1 and current_load["Cons St"] == "CA"):
                round_trips_found += 1
                logger.debug(f"Round trip detected: Truck {current_truck}, Trailer {trailer}, Ref {ref_counter}.{decimal_counter}")
            
            logger.debug(f"Load {current_load['Load']}: Truck {current_truck}, Trailer {trailer} | {current_load['Ship City']}, {current_load['Ship St']} → {current_load['Cons City']}, {current_load['Cons St']} (Ref: {ref_counter}.{decimal_counter})")
            
            prev_del_date = current_load['DEL']
            prev_truck = current_truck
            decimal_counter += 1
        
        # After processing all loads for this trailer, move to next reference group
        if len(trailer_loads) > 0:
            ref_counter += 1
    
    logger.info(f"Phase 3 completed:")
    logger.info(f"  • Total reference groups: {ref_counter-1}")
    logger.info(f"  • Round trips detected: {round_trips_found}")
    logger.info(f"  • Maintaining chronological order (no optimization)")
    logger.info(f"  • Each load has own decimal reference (no CA consolidation)")
    logger.info(f"  • Handles truck changes within same trailer")
    logger.info(f"--------------------------------")
    
    # Save debug CSV output
    debug_file = DEBUG_DIR / "phase3_round_trips.csv"
    pcs.to_csv(debug_file, index=False)
    logger.info(f"Phase 3 debug file saved: {debug_file}")
    
    return pcs


def run_validation_test():
    """
    Run validation test on representative trips from feedback.md
    Tests loads: 174418/174520, 175029-031-150
    """
    logger.info("🧪 RUNNING VALIDATION TEST ON REPRESENTATIVE TRIPS")
    
    try:
        # Load test data
        pcs = pd.read_excel(INPUT_FILE, sheet_name=PCS_SHEET, keep_default_na=False)
        inv = pd.read_excel(INPUT_FILE, sheet_name=INV_SHEET, usecols=["Unit", "Company"])
        
        # Test loads from corrected validation requirements
        test_loads = [175029, 175031, 175030, 175150, 174418, 174520, 174861, 174899]  
        test_data = pcs[pcs['Load'].isin(test_loads)].copy()
        
        if len(test_data) == 0:
            logger.warning("No test loads found in data")
            return
            
        logger.info(f"Found {len(test_data)} test loads")
        
        # Process through our pipeline
        logger.info("Processing test data through pipeline...")
        
        # Apply same processing as main pipeline
        test_pcs, test_inv = step1_read_excel_data()
        test_filtered = step2_filter_fleet_data(test_pcs[test_pcs['Load'].isin(test_loads)], test_inv)
        test_with_refs = step3_detect_round_trips(test_filtered)
        
        logger.info("Test pipeline completed successfully!")
        logger.info(f"Test results:")
        logger.info(f"  • Input loads: {len(test_data)}")
        logger.info(f"  • After filtering: {len(test_filtered)}")
        logger.info(f"  • With references: {len(test_with_refs)}")
        
        # Show reference assignments
        logger.info("Reference assignments:")
        for _, row in test_with_refs.iterrows():
            logger.info(f"  Load {row['Load']}: {row['Ship City']}, {row['Ship St']} → {row['Cons City']}, {row['Cons St']} (Ref: {row['Ref']})")
            
        # Save test results
        test_output_file = DEBUG_DIR / "validation_test_results.csv"
        test_with_refs.to_csv(test_output_file, index=False)
        logger.info(f"Test results saved: {test_output_file}")
        
        return test_with_refs
        
    except Exception as e:
        logger.error(f"Validation test failed: {e}")
        return None



# ──────────────────────────────────────────────────────────────────────────────
# Main Processing Function
# ──────────────────────────────────────────────────────────────────────────────

def main():
    """
    Main processing function - executes all phases following plan.md
    """
    logger.info("Starting IFTA PCS Trips Processing System...")
    
    try:
        # Load API key
        api_key = load_api_key()
        logger.info("HERE API key loaded successfully")
        
        # Execute all phases following plan.md structure
        pcs, inv = step1_read_excel_data()
        pcs_filtered = step2_filter_fleet_data(pcs, inv)
        pcs_with_refs = step3_detect_round_trips(pcs_filtered)
        
        # Skip Step 4 - virtual returns not needed with proper Truck+Trailer logic
        
        # Load state boundaries and calculate mileage (async version for performance)
        states_gdf = load_state_boundaries()
        # output_df = asyncio.run(step5_calculate_mileage_concurrent(pcs_with_refs, states_gdf, api_key, max_concurrent=10))
        
        # excel_file, csv_file = step6_generate_output(output_df)
        
        # # Summary
        # logger.info("="*60)
        # logger.info("PROCESSING COMPLETED SUCCESSFULLY!")
        # logger.info(f"Processed {len(pcs_filtered)} truck trips")
        
        # # Count error vs valid records in final output
        # error_records = output_df[output_df['State'] == 'ERROR']
        # valid_records = output_df[output_df['State'] != 'ERROR']
        
        # logger.info(f"Generated {len(output_df)} total records:")
        # logger.info(f"  • Valid state-mile records: {len(valid_records)}")
        # if len(error_records) > 0:
        #     logger.info(f"  • Error records (failed calculations): {len(error_records)}")
        
        # logger.info(f"Final output files:")
        # logger.info(f"  • Excel: {excel_file}")
        # logger.info(f"  • CSV: {csv_file}")
        # logger.info("="*60)
        
        # # Show debug files created
        # logger.info("DEBUG FILES CREATED FOR ANALYSIS:")
        # debug_files = [
        #     "phase1_pcs_cleaned.csv - Cleaned PCS data after initial processing",
        #     "phase1_inventory.csv - Cleaned inventory data",
        #     "phase2_filtered_fleet.csv - Data after filtering (company-owned, interstate, numeric trucks)",  
        #     "phase3_round_trips.csv - Data with round trip detection and proper references",
        #     "phase4_virtual_returns.csv - Data with virtual empty return legs added",
        #     "phase5_state_miles.csv - Raw state-by-state mileage records (includes ERROR records for failed routes)",
        #     "phase6_final_output.csv - Final formatted output (includes ERROR records - filter State!='ERROR' for valid data)"
        # ]
        # for debug_file in debug_files:
        #     logger.info(f"  • {DEBUG_DIR}/{debug_file}")
        # logger.info("="*60)
        
        # return excel_file, csv_file
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    import sys
    
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "validate":
            run_validation_test()
        else:
            main()
    else:
        main() 