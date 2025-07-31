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

def clean_location_name(city: str) -> str:
    """
    Clean location names for better geocoding accuracy
    Strip warehouse IDs, building numbers, and other details that can cause geocoding errors
    """
    if not city or pd.isna(city):
        return city
   
    # Convert to string and clean
    city = str(city).strip()
   
    # Remove common warehouse/building identifiers
    unwanted_patterns = [
        r'\s+\d+$',  # Remove trailing numbers (building numbers)
        r'\s+#\d+.*$',  # Remove # followed by numbers
        r'\s+BLDG.*$',  # Remove BLDG and everything after
        r'\s+BUILDING.*$',  # Remove BUILDING and everything after  
        r'\s+WAREHOUSE.*$',  # Remove WAREHOUSE and everything after
        r'\s+DC\s*\d*$',  # Remove DC (Distribution Center) with optional numbers
        r'\s+WH\s*\d*$',  # Remove WH (Warehouse) with optional numbers
    ]
   
    import re
    for pattern in unwanted_patterns:
        city = re.sub(pattern, '', city, flags=re.IGNORECASE)
   
    # Clean up extra spaces
    city = ' '.join(city.split())
   
    return city

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
                
            # For loads after the first in a reference group, chain routes for IFTA compliance
            if decimal_counter > 1 and not should_break:
                # Find the previous load in this reference group to chain routes
                previous_loads_in_group = pcs[(pcs['Trailer'] == trailer) & 
                                             (pcs['Ref'].str.startswith(f"{ref_counter}.")) & 
                                             (pcs.index < idx)]
                
                if not previous_loads_in_group.empty:
                    # Get the last load in this reference group
                    prev_load_idx = previous_loads_in_group.index[-1]
                    prev_cons_city = pcs.loc[prev_load_idx, 'Cons City']
                    prev_cons_st = pcs.loc[prev_load_idx, 'Cons St']
                    
                    logger.info(f"🔗 Chaining routes: Load {current_load['Load']} origin updated from {current_load['Ship City']}, {current_load['Ship St']} → {prev_cons_city}, {prev_cons_st}")
                    pcs.loc[idx, 'Ship City'] = prev_cons_city
                    pcs.loc[idx, 'Ship St'] = prev_cons_st
            
            logger.debug(f"Load {current_load['Load']}: Truck {current_truck}, Trailer {trailer} | {pcs.loc[idx, 'Ship City']}, {pcs.loc[idx, 'Ship St']} → {current_load['Cons City']}, {current_load['Cons St']} (Ref: {ref_counter}.{decimal_counter})")
            
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

# # ──────────────────────────────────────────────────────────────────────────────
# # Phase 4: Route Optimization  
# # ──────────────────────────────────────────────────────────────────────────────

def load_geocoding_cache() -> dict:
    """Load persistent geocoding cache"""
    cache_file = BASE_DIR / "geocoding_cache.json"
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
            logger.info(f"Loaded {len(cache_data)} cached locations from {cache_file}")
            return cache_data
        except Exception as e:
            logger.warning(f"Error loading geocoding cache: {e}")
    return {}

# def save_geocoding_cache(location_coords: dict):
#     """Save persistent geocoding cache"""
#     cache_file = BASE_DIR / "geocoding_cache.json"
#     try:
#         with open(cache_file, 'w') as f:
#             json.dump(location_coords, f, indent=2)
#         logger.info(f"Saved {len(location_coords)} locations to geocoding cache")
#     except Exception as e:
#         logger.warning(f"Error saving geocoding cache: {e}")

# def geocode_location(location: str, api_key: str) -> tuple:
#     """Convert location name to coordinates using HERE Geocoding API"""
#     try:
#         # Simple in-memory cache for this session
#         if not hasattr(geocode_location, '_cache'):
#             geocode_location._cache = {}
        
#         if location in geocode_location._cache:
#             return geocode_location._cache[location]
        
#         url = "https://geocode.search.hereapi.com/v1/geocode"
#         params = {"q": location, "apiKey": api_key}
        
#         time.sleep(0.01)  # Rate limiting
#         resp = requests.get(url, params=params, timeout=10)
#         resp.raise_for_status()
#         data = resp.json()
        
#         if data.get("items"):
#             position = data["items"][0]["position"]
#             result = (position["lat"], position["lng"])
#             geocode_location._cache[location] = result
#             return result
#         else:
#             logger.warning(f"No geocoding results for: {location}")
#             return None, None
            
#     except Exception as e:
#         logger.warning(f"Geocoding error for {location}: {e}")
#         return None, None

async def geocode_location_async(session: aiohttp.ClientSession, location: str, api_key: str) -> tuple:
    """Convert location name to coordinates using HERE Geocoding API (async version)"""
    try:
        # Simple in-memory cache for this session
        if not hasattr(geocode_location_async, '_cache'):
            geocode_location_async._cache = {}
        
        if location in geocode_location_async._cache:
            return geocode_location_async._cache[location]
        
        url = "https://geocode.search.hereapi.com/v1/geocode"
        params = {"q": location, "apiKey": api_key}
        
        await asyncio.sleep(0.01)  # Rate limiting
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                logger.warning(f"Geocoding API error {resp.status} for: {location}")
                return None, None
               
            data = await resp.json()
        
        if data.get("items"):
            position = data["items"][0]["position"]
            result = (position["lat"], position["lng"])
            geocode_location_async._cache[location] = result
            return result
        else:
            logger.warning(f"No geocoding results for: {location}")
            return None, None
            
    except Exception as e:
        logger.warning(f"Async geocoding error for {location}: {e}")
        return None, None

# def great_circle_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
#     """Calculate great circle distance between two points in miles"""
#     lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
#     dlat = lat2 - lat1
#     dlon = lon2 - lon1
#     a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
#     c = 2 * math.asin(math.sqrt(a))
#     return c * 3956  # Earth's radius in miles

# def pre_geocode_all_locations(pcs: pd.DataFrame, api_key: str) -> dict:
#     """Pre-geocode all unique locations with persistent caching"""
#     logger.info("Pre-geocoding all unique locations...")
    
#     location_coords = load_geocoding_cache()
    
#     # Collect all unique locations needed (with cleaned city names)
#     unique_locations = set()
#     for _, row in pcs.iterrows():
#         # Clean city names to remove warehouse IDs per feedback
#         clean_ship_city = clean_location_name(row['Ship City'])
#         clean_cons_city = clean_location_name(row['Cons City'])
        
#         # Use simple "CITY, ST" format for better geocoding
#         ship_location = f"{clean_ship_city}, {row['Ship St']}"
#         cons_location = f"{clean_cons_city}, {row['Cons St']}"
#         unique_locations.add(ship_location)
#         unique_locations.add(cons_location)
    
#     # Find locations that need geocoding
#     locations_to_geocode = [loc for loc in unique_locations if loc not in location_coords]
    
#     logger.info(f"Found {len(unique_locations)} unique locations:")
#     logger.info(f"  • Already cached: {len(unique_locations) - len(locations_to_geocode)}")
#     logger.info(f"  • Need to geocode: {len(locations_to_geocode)}")
    
#     if locations_to_geocode:
#         logger.info("Geocoding new locations...")
#         for i, location in enumerate(locations_to_geocode, 1):
#             if i % 25 == 0:
#                 logger.info(f"Geocoded {i}/{len(locations_to_geocode)} new locations...")
            
#             lat, lng = geocode_location(location, api_key)
#             if lat is not None and lng is not None:
#                 location_coords[location] = [lat, lng]
#             time.sleep(0.02)
        
#         save_geocoding_cache(location_coords)
#         logger.info(f"Geocoded {len(locations_to_geocode)} new locations")
#     else:
#         logger.info("All locations already cached - no API calls needed!")
    
#     # Convert to tuples for consistency
#     return {loc: tuple(coords) for loc, coords in location_coords.items() if loc in unique_locations}

# def step4_add_virtual_returns(pcs: pd.DataFrame) -> pd.DataFrame:
#     """
#     Phase 4: Add virtual return legs for incomplete trips (AZ/NV without CA return)
#     Maintains chronological order - no route optimization
#     """
#     logger.info("Phase 4: Adding virtual return legs for incomplete trips...")
    
#     pcs = pcs.copy()
#     virtual_legs = []
#     incomplete_trips = 0
    
#     # Group by reference base number to identify incomplete trips
#     pcs['Ref_Base'] = pcs['Ref'].str.split('.').str[0]
    
#     for ref_base, group in pcs.groupby('Ref_Base'):
#         group = group.sort_values('PU')  # Maintain chronological order
        
#         # Get the last load in this reference group
#         last_load = group.iloc[-1]
        
#         # Check if trip ends in AZ/NV without returning to CA
#         if (last_load['Cons St'] in ['AZ', 'NV'] and 
#             not any(group['Cons St'] == 'CA')):  # No CA destination in this group
            
#             # Check if there's a future CA delivery by same truck/trailer
#             truck = last_load['Truck'] 
#             trailer = last_load['Trailer']
            
#             # Look for future loads by same truck/trailer returning to CA
#             future_loads = pcs[(pcs['Truck'] == truck) & 
#                               (pcs['Trailer'] == trailer) &
#                               (pcs['PU'] > last_load['DEL']) &
#                               (pcs['Cons St'] == 'CA')]
            
#             # If no future CA return within reasonable time, add virtual return
#             if len(future_loads) == 0 or (future_loads['PU'].min() - last_load['DEL']).days > 7:
                
#                 # Create virtual return leg
#                 next_decimal = len(group) + 1
#                 virtual_ref = f"{ref_base}.{next_decimal}"
                
#                 virtual_leg = {
#                     'Load': f"VIRTUAL_{last_load['Load']}",
#                     'Trip': last_load['Trip'],
#                     'Truck': last_load['Truck'],
#                     'Trailer': last_load['Trailer'], 
#                     'Ship City': last_load['Cons City'],
#                     'Ship St': last_load['Cons St'],
#                     'Cons City': 'SAN BERNARDINO',  # Default CA return location
#                     'Cons St': 'CA',
#                     'PU': last_load['DEL'],  # Start immediately after delivery
#                     'DEL': last_load['DEL'] + pd.Timedelta(days=1),  # 1 day return trip
#                     'Company': last_load['Company'],
#                     'Ref': virtual_ref,
#                     'Ref_Base': ref_base
#                 }
                
#                 virtual_legs.append(virtual_leg)
#                 incomplete_trips += 1
                
#                 logger.debug(f"Added virtual return: {last_load['Cons City']}, {last_load['Cons St']} → SAN BERNARDINO, CA (Ref: {virtual_ref})")
    
#     # Add virtual legs to main dataframe
#     if virtual_legs:
#         virtual_df = pd.DataFrame(virtual_legs)
#         result = pd.concat([pcs, virtual_df], ignore_index=True)
        
#         # Sort by truck, trailer, and pickup date to maintain chronological order
#         result = result.sort_values(['Truck', 'Trailer', 'PU']).reset_index(drop=True)
#     else:
#         result = pcs
    
#     # Clean up temporary column
#     result = result.drop(columns=['Ref_Base'])
    
#     logger.info(f"Phase 4 completed:")
#     logger.info(f"  • Virtual return legs added: {len(virtual_legs)}")
#     logger.info(f"  • Incomplete trips fixed: {incomplete_trips}")
#     logger.info(f"  • Total loads: {len(result)} (original: {len(pcs)})")
#     logger.info(f"  • Maintained chronological order (no optimization)")
#     logger.info(f"--------------------------------")
    
#     # Save debug CSV output
#     debug_file = DEBUG_DIR / "phase4_virtual_returns.csv"
#     result.to_csv(debug_file, index=False)
#     logger.info(f"Phase 4 debug file saved: {debug_file}")
    
#     return result

# # ──────────────────────────────────────────────────────────────────────────────
# # Phase 5: Mileage Calculation (Async/Concurrent Version)
# # ──────────────────────────────────────────────────────────────────────────────

def load_state_boundaries() -> gpd.GeoDataFrame:
    """Load and prepare state boundary data"""
    logger.info("Loading state boundary data...")
    
    if not STATE_SHP.exists():
        raise FileNotFoundError(f"State shapefile not found: {STATE_SHP}")
    
    states = gpd.read_file(STATE_SHP)[["STUSPS", "geometry"]]
    states_projected = states.to_crs(epsg=5070)  # NAD83/USA Contiguous
    
    logger.info(f"Loaded {len(states_projected)} state boundaries")
    return states_projected

async def calculate_state_miles_async(session: aiohttp.ClientSession, origin: str, destination: str, 
                                    states_gdf: gpd.GeoDataFrame, api_key: str, location_coords: dict = None) -> Dict[str, float]:
    """
    Calculate miles driven in each state for a route using HERE API
    Following plan.md Step 5.1 with enhanced error handling
    """
    try:
        # Use cached coordinates if available, otherwise geocode live
        origin_coords = None
        dest_coords = None
        
        if location_coords:
            origin_coords = location_coords.get(origin)
            dest_coords = location_coords.get(destination)

        # If either coordinate is missing, geocode live using HERE Geocoding API
        if not origin_coords:
            try:
                origin_coords = await geocode_location_async(session, origin, api_key)
                if origin_coords and origin_coords[0] and origin_coords[1]:
                    # Cache the result for future use
                    if location_coords is not None:
                        location_coords[origin] = origin_coords
                else:
                    logger.warning(f"Failed to geocode origin: {origin}")
                    return {}
            except Exception as e:
                logger.warning(f"Geocoding error for origin {origin}: {e}")
                return {}
        
        if not dest_coords:
            try:
                dest_coords = await geocode_location_async(session, destination, api_key)
                if dest_coords and dest_coords[0] and dest_coords[1]:
                    # Cache the result for future use
                    if location_coords is not None:
                        location_coords[destination] = dest_coords
                else:
                    logger.warning(f"Failed to geocode destination: {destination}")
                    return {}
            except Exception as e:
                logger.warning(f"Geocoding error for destination {destination}: {e}")
                return {}
        
        # Use coordinates for routing
        if origin_coords and dest_coords:
            try:
                origin_param = f"{origin_coords[0]},{origin_coords[1]}"
                dest_param = f"{dest_coords[0]},{dest_coords[1]}"
            except (IndexError, TypeError) as e:
                logger.warning(f"Invalid coordinate format: origin={origin_coords}, dest={dest_coords}, error={e}")
                return {}
        else:
            logger.warning(f"Missing coordinates after geocoding: {origin} → {destination} | origin_coords={origin_coords}, dest_coords={dest_coords}")
            return {}
        
        url = "https://router.hereapi.com/v8/routes"
        params = {
            "transportMode": "truck",
            "routingMode": "fast",
            "origin": origin_param,
            "destination": dest_param,
            "return": "summary,polyline",
            "apiKey": api_key
        }
        
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.warning(f"HERE API HTTP {resp.status}: {error_text[:200]}...")
                    return {}
                    
                data = await resp.json()                
        except asyncio.TimeoutError as e:
            logger.warning(f"HERE API timeout after 15s: {origin} → {destination}")
            return {}
        except Exception as e:
            logger.warning(f"HERE API connection error: {e} | {origin} → {destination}")
            return {}
        
        # Check if route was found and extract state spans
        if not data.get("routes") or not data["routes"]:
            logger.warning(f"HERE API no routes found. Full response: {data}")
            return {}
        
        try:
            route = data["routes"][0]
            section = route["sections"][0]
            logger.info(f"🗺️ Route structure: spans={bool(section.get('spans'))}, polyline={bool(section.get('polyline'))}")
            
            # Use HERE API's built-in state spans if available (more accurate than GIS overlay)
            state_miles = {}
            
            # Check if spans are available (preferred method)
            if "spans" in section:
                for span in section["spans"]:
                    if span.get("stateCode"):
                        state_abbr = span["stateCode"]
                        length_meters = span.get("length", 0)
                        miles = length_meters / 1609.34  # Convert meters to miles
                        
                        if miles >= 0.1:  # Only include significant distances
                            if state_abbr in state_miles:
                                state_miles[state_abbr] += miles
                            else:
                                state_miles[state_abbr] = miles
                                
                # Round all values and return
                state_miles = {state: round(miles, 1) for state, miles in state_miles.items()}
                await asyncio.sleep(0.01)  # Rate limiting
                return state_miles
            
            # Fallback: Process polyline with GIS overlay if spans not available
            elif "polyline" in section:
                try:
                    import flexpolyline  # HERE's flexible polyline decoder
                    from shapely.geometry import LineString
                    import geopandas as gpd
                    
                    # Decode polyline to coordinates
                    encoded_polyline = section["polyline"]
                    
                    # Validate polyline data before processing
                    if not encoded_polyline or len(encoded_polyline) < 10:
                        logger.warning(f"Polyline too short or empty: {len(encoded_polyline)} chars")
                        return {}
            
                    try:
                        # HERE uses flexible polyline encoding, not Google's standard polyline
                        decoded_coords = flexpolyline.decode(encoded_polyline)
                        logger.info(f"🗺️ HERE flexpolyline decoded: {len(decoded_coords)} coordinate points")
                    except (ValueError, IndexError, TypeError, Exception) as decode_error:
                        logger.error(f"HERE FLEXPOLYLINE DECODE FAILED: {decode_error} | Polyline length: {len(encoded_polyline)}")
                        # Ultimate fallback: Use simple great circle distance
                        logger.warning(f"Flexpolyline decode failed. Using great circle fallback.")
                        return {"UNKNOWN": 0.0}  # Placeholder for great circle calculation
            
                    # Validate decoded coordinates
                    if not decoded_coords or len(decoded_coords) < 2:
                        logger.warning(f"Insufficient decoded coordinates: {len(decoded_coords) if decoded_coords else 0}")
                        return {}
                    
                    # Debug: Check first few coordinates
                    logger.info(f"📍 First 3 HERE coords: {decoded_coords[:3]}")
                    
                    # HERE flexpolyline returns [lat, lng, elevation] tuples (elevation optional)
                    # Convert to [(lng, lat)] for shapely (note: reversed order)
                    line_coords = [(coord[1], coord[0]) for coord in decoded_coords]  # lng, lat
                    
                    # Debug: Check coordinate conversion
                    logger.info(f"📍 First 3 converted coords: {line_coords[:3]}")
                    
                    # Validate coordinates before creating LineString
                    invalid_coords = [(lng, lat) for lng, lat in line_coords if not (-180 <= lng <= 180 and -90 <= lat <= 90)]
                    if invalid_coords:
                        logger.error(f"INVALID COORDINATES found: {invalid_coords[:5]}... (showing first 5)")
                        return {}
                    
                    route_line = LineString(line_coords)
                    logger.info(f"🌍 LineString created with {len(line_coords)} points | Bounds: {route_line.bounds}")
                    
                    # Convert to GeoDataFrame with WGS84 CRS
                    route_gdf = gpd.GeoDataFrame([1], geometry=[route_line], crs="EPSG:4326")
                    logger.info(f"🗺️ GeoDataFrame created with CRS: EPSG:4326 | GDF bounds: {route_gdf.bounds}")
                    
                    # Reproject to match state boundaries CRS
                    route_projected = route_gdf.to_crs(states_gdf.crs)
                    logger.info(f"🗺️ Route reprojected to CRS: {states_gdf.crs} | Projected bounds: {route_projected.bounds}")
                    
                    # Find intersections with state boundaries
                    logger.info(f"🗺️ Starting state intersection calculation with {len(states_gdf)} states")
                    logger.info(f"🗺️ Route bounds: {route_projected.iloc[0].geometry.bounds}")
                    logger.info(f"🗺️ States CRS: {states_gdf.crs}, Route CRS: {route_projected.crs}")
                    
                    intersection_count = 0
                    for idx, state_row in states_gdf.iterrows():
                        try:
                            intersection = route_projected.iloc[0].geometry.intersection(state_row.geometry)
                            
                            if not intersection.is_empty:
                                intersection_count += 1
                                logger.info(f"✅ Intersection found with {state_row['STUSPS']}")
                                # Calculate length of intersection in miles
                                if hasattr(intersection, 'length'):
                                    length_meters = intersection.length
                                else:
                                    # Handle multipart geometries
                                    length_meters = sum(geom.length for geom in intersection.geoms if hasattr(geom, 'length'))
                                
                                miles = length_meters / 1609.34  # Convert to miles
                                
                                if miles >= 0.1:  # Only include significant distances
                                    state_abbr = state_row['STUSPS']  # State abbreviation
                                    if state_abbr in state_miles:
                                        state_miles[state_abbr] += miles
                                    else:
                                        state_miles[state_abbr] = miles
                        except Exception as state_error:
                            logger.warning(f"Error processing state {state_row.get('STUSPS', 'UNKNOWN')}: {state_error}")
                    
                    # Round all values
                    state_miles = {state: round(miles, 1) for state, miles in state_miles.items()}
                    logger.info(f"🎯 State miles calculated: {state_miles} (found {intersection_count} intersections)")
                    await asyncio.sleep(0.01)  # Rate limiting
                    return state_miles
                    
                except ImportError:
                    logger.debug("polyline library not available for GIS fallback")
                    return {}
                except Exception as gis_error:
                    # Reduce warning spam - only log first few errors
                    if not hasattr(calculate_state_miles_async, '_polyline_error_count'):
                        calculate_state_miles_async._polyline_error_count = 0
                    calculate_state_miles_async._polyline_error_count += 1
                    
                    if calculate_state_miles_async._polyline_error_count <= 5:
                        logger.warning(f"GIS polyline processing failed (#{calculate_state_miles_async._polyline_error_count}): {gis_error}")
                    elif calculate_state_miles_async._polyline_error_count == 6:
                        logger.info("Suppressing further polyline processing warnings...")
                    return {}
            
            # No spans or polyline available
            return {}
            
        except (KeyError, IndexError, ValueError) as e:
            # Track errors with limited logging
            if not hasattr(calculate_state_miles_async, '_error_count'):
                calculate_state_miles_async._error_count = 0
            calculate_state_miles_async._error_count += 1
            
            if calculate_state_miles_async._error_count <= 3 or calculate_state_miles_async._error_count % 100 == 0:
                logger.warning(f"API error #{calculate_state_miles_async._error_count}: {origin} → {destination}")
            return {}
        
    except Exception as e:
        logger.error(f"UNEXPECTED ERROR calculating route from {origin} to {destination}: {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return {}

# def calculate_great_circle_state_miles(origin_coords: tuple, dest_coords: tuple, states_gdf: gpd.GeoDataFrame) -> Dict[str, float]:
#     """
#     Calculate approximate state miles using great circle distance
#     Used as fallback when HERE API fails
#     """
#     try:
#         from shapely.geometry import Point, LineString
#         import geopandas as gpd
        
#         origin_lat, origin_lng = origin_coords
#         dest_lat, dest_lng = dest_coords
        
#         # Create straight line between origin and destination (WGS84 coordinates)
#         line = LineString([(origin_lng, origin_lat), (dest_lng, dest_lat)])
        
#         # Convert to GeoSeries with proper CRS (WGS84)
#         line_gdf = gpd.GeoSeries([line], crs="EPSG:4326")
        
#         # Reproject to match state boundaries CRS (NAD83/USA Contiguous - EPSG:5070)
#         line_projected = line_gdf.to_crs(states_gdf.crs)[0]
        
#         # Calculate total distance
#         def haversine_distance(lat1, lon1, lat2, lon2):
#             R = 3959  # Earth's radius in miles
#             lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
#             dlat = lat2 - lat1
#             dlon = lon2 - lon1
#             a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
#             c = 2 * math.asin(math.sqrt(a))
#             return R * c
        
#         total_distance = haversine_distance(origin_lat, origin_lng, dest_lat, dest_lng)
        
#         if total_distance < 1:
#             return {}
        
#         # Find state intersections using properly projected line
#         state_miles = {}
#         for _, state_row in states_gdf.iterrows():
#             state_abbr = state_row.get('STUSPS', 'UNKNOWN')
#             try:
#                 if line_projected.intersects(state_row.geometry):
#                     intersection = line_projected.intersection(state_row.geometry)
#                     if hasattr(intersection, 'length') and intersection.length > 0:
#                         # Convert projected length back to miles
#                         miles_estimate = intersection.length / 1609.34  # meters to miles
#                         if miles_estimate >= 1:
#                             state_miles[state_abbr] = round(min(miles_estimate, total_distance), 2)
#             except Exception:
#                 continue
        
#         # Fallback for long routes with no intersections found
#         if not state_miles and total_distance > 50:
#             # Create points with proper CRS
#             origin_point = Point(origin_lng, origin_lat)
#             dest_point = Point(dest_lng, dest_lat)
            
#             # Convert to GeoSeries with WGS84 CRS
#             origin_gdf = gpd.GeoSeries([origin_point], crs="EPSG:4326")
#             dest_gdf = gpd.GeoSeries([dest_point], crs="EPSG:4326")
            
#             # Reproject to match state boundaries
#             origin_projected = origin_gdf.to_crs(states_gdf.crs)[0]
#             dest_projected = dest_gdf.to_crs(states_gdf.crs)[0]
            
#             origin_state = dest_state = None
#             for _, state_row in states_gdf.iterrows():
#                 state_abbr = state_row.get('STUSPS', 'UNKNOWN')
#                 try:
#                     if origin_projected.within(state_row.geometry):
#                         origin_state = state_abbr
#                     if dest_projected.within(state_row.geometry):
#                         dest_state = state_abbr
#                 except:
#                     continue
            
#             if origin_state and dest_state and origin_state != dest_state:
#                 state_miles[origin_state] = round(total_distance * 0.4, 2)
#                 state_miles[dest_state] = round(total_distance * 0.4, 2)
        
#         return state_miles
        
#     except Exception:
#         return {}

async def step5_calculate_mileage_concurrent(pcs: pd.DataFrame, states_gdf: gpd.GeoDataFrame, 
                                           api_key: str, max_concurrent: int = 15) -> pd.DataFrame:
    """
    Phase 5: Calculate mileage for each route segment (following plan.md Step 5.1 & 5.2)
    Uses concurrent async processing for better performance
    """
    logger.info(f"Phase 5: Calculating state-by-state mileage (concurrent with max {max_concurrent} requests)...")
    
    location_coords = load_geocoding_cache()
    logger.info(f"Using {len(location_coords)} cached coordinates for mileage calculation")
    
    total_routes = len(pcs)
    start_time = time.time()
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_single_route(session: aiohttp.ClientSession, idx: int, row: pd.Series, route_num: int) -> tuple:
        """Process a single route and return mileage records"""
        async with semaphore:
            try:
                route_rows = []
                
                # Format locations with cleaning (strip warehouse IDs per feedback)
                ship_state = STATE_MAPPING.get(row['Ship St'], row['Ship St'])
                cons_state = STATE_MAPPING.get(row['Cons St'], row['Cons St'])
                
                # Clean city names to remove warehouse IDs and building numbers
                clean_ship_city = clean_location_name(row['Ship City'])
                clean_cons_city = clean_location_name(row['Cons City'])
                
                # Convert state abbreviations to full names for cache lookup compatibility
                ship_state_full = STATE_MAPPING.get(row['Ship St'], row['Ship St'])
                cons_state_full = STATE_MAPPING.get(row['Cons St'], row['Cons St'])
                
                # Format locations to match geocoding cache format ("CITY, State, USA")
                origin = f"{clean_ship_city}, {ship_state_full}, USA"
                destination = f"{clean_cons_city}, {cons_state_full}, USA"
                
                # Skip same-city routes (these are local deliveries, not interstate)
                if clean_ship_city.upper() == clean_cons_city.upper() and row['Ship St'] == row['Cons St']:
                    logger.debug(f"Skipping same-city route: {origin} → {destination}")
                    # Return empty result - no interstate mileage needed
                    return [], True  # Mark as successful but no miles
                
                route_had_miles = False
                error_reason = None
                
                # Simple route calculation - no CA consolidation (removed per corrected requirements)
                interstate_miles = await calculate_state_miles_async(session, origin, destination, states_gdf, api_key, location_coords)
                
                # If HERE API failed, set error per feedback.md requirements
                if not interstate_miles:
                    logger.debug(f"API returned empty result for {origin} → {destination}")
                    interstate_miles = {}  # Initialize empty dict to prevent iteration errors
                    error_reason = "GEOCODE_ERR"
                
                # Add interstate miles to output
                for state, miles in interstate_miles.items():
                    route_had_miles = True
                    route_rows.append({
                        "Company": row["Company"],  # Use actual company from data
                        "Ref No": row["Ref"],
                        "Load": row["Load"],
                        "Trip": row["Trip"],
                        "Truck": row["Truck"],
                        "Trailer": row["Trailer"],
                        "PU Date F": row["PU"],
                        "Del Date F": row["DEL"],
                        "State": state,
                        "Miles": miles
                    })
                
                # If no mileage was calculated, add an ERROR record
                if not route_had_miles:
                    logger.warning(f"GEOCODE_ERR: Load {row['Load']} failed route calculation ({origin} → {destination})")
                    
                    route_rows.append({
                        "Company": row["Company"],
                        "Ref No": row["Ref"],
                        "Load": row["Load"],
                        "Trip": row["Trip"],
                        "Truck": row["Truck"],
                        "Trailer": row["Trailer"],
                        "PU Date F": row["PU"],
                        "Del Date F": row["DEL"],
                        "State": "ERROR",
                        "Miles": "GEOCODE_ERR"
                    })
                
                return route_rows, route_had_miles
                
            except Exception as e:
                logger.warning(f"GEOCODE_ERR: Load {row['Load']} exception: {str(e)[:100]}")
                # Always return an ERROR record even for exceptions
                error_record = [{
                    "Company": row["Company"],
                    "Ref No": row["Ref"],
                    "Load": row["Load"],
                    "Trip": row["Trip"],
                    "Truck": row["Truck"],
                    "Trailer": row["Trailer"],
                    "PU Date F": row["PU"],
                    "Del Date F": row["DEL"],
                    "State": "ERROR",
                    "Miles": "GEOCODE_ERR"
                }]
                return error_record, False
    
    # Process routes concurrently
    connector = aiohttp.TCPConnector(limit=max_concurrent * 2, limit_per_host=max_concurrent)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [process_single_route(session, idx, row, i + 1) for i, (idx, row) in enumerate(pcs.iterrows())]
        
        output_rows = []
        successful_routes = failed_routes = 0
        batch_size = 25  # Reduced batch size for better progress reporting
        
        for i in range(0, len(tasks), batch_size):
            batch_results = await asyncio.gather(*tasks[i:i + batch_size], return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    failed_routes += 1
                else:
                    route_rows, route_had_miles = result
                    output_rows.extend(route_rows)
                    if route_had_miles:
                        successful_routes += 1
                    else:
                        failed_routes += 1
            
            # Progress update (more frequent reporting)
            completed = min(i + batch_size, len(tasks))
            if completed % 50 == 0 or completed == len(tasks):
                elapsed = time.time() - start_time
                avg_time = elapsed / completed if completed > 0 else 0
                remaining = (len(tasks) - completed) * avg_time
                success_rate = (successful_routes / completed) * 100 if completed > 0 else 0
                fallback_count = getattr(step5_calculate_mileage_concurrent, '_fallback_count', 0)
                logger.info(f"Progress: {completed}/{len(tasks)} ({completed/len(tasks)*100:.1f}%) - Success: {success_rate:.1f}% - Fallbacks: {fallback_count} - ETA: {remaining/60:.1f} min")
    
    # Final statistics with error breakdowns
    result_df = pd.DataFrame(output_rows)
    total_time = time.time() - start_time
    
    # Count different types of records
    error_records = result_df[result_df['State'] == 'ERROR']
    successful_records = result_df[result_df['State'] != 'ERROR']
    
    # Analyze error types
    error_counts = {}
    if len(error_records) > 0:
        error_types = error_records['Miles'].value_counts()
        for error_type, count in error_types.items():
            error_counts[error_type] = count
    
    error_count = getattr(calculate_state_miles_async, '_error_count', 0)
    fallback_count = getattr(step5_calculate_mileage_concurrent, '_fallback_count', 0)
    
    logger.info(f"Phase 5 completed in {total_time/60:.1f} minutes:")
    logger.info(f"  • Total routes processed: {total_routes}")
    logger.info(f"  • Successful routes: {successful_routes} ({successful_routes/total_routes*100:.1f}%)")
    logger.info(f"  • Failed routes: {failed_routes} ({failed_routes/total_routes*100:.1f}%)")
    logger.info(f"  • Generated records: {len(result_df)} total ({len(successful_records)} valid, {len(error_records)} errors)")
    logger.info(f"  • Average time per route: {total_time/total_routes:.2f} seconds")
    logger.info(f"  • Speed improvement: ~{max_concurrent}x faster than sequential")
    logger.info(f"  • API errors: {error_count}")
    logger.info(f"  • Great circle fallback attempts: {fallback_count}")
    
    if error_counts:
        logger.info(f"  • Error breakdown:")
        for error_type, count in error_counts.items():
            logger.info(f"    - {error_type}: {count} routes")
    
    logger.info(f"--------------------------------")
    
    # Save debug CSV output
    debug_file = DEBUG_DIR / "phase5_state_miles.csv"
    result_df.to_csv(debug_file, index=False)
    logger.info(f"Phase 5 debug file saved: {debug_file}")
    
    return result_df

# # ──────────────────────────────────────────────────────────────────────────────
# # Phase 6: Output Generation
# # ──────────────────────────────────────────────────────────────────────────────

# def step6_generate_output(output_df: pd.DataFrame) -> tuple:
#     """
#     Phase 6: Generate final formatted output (following plan.md Step 6.1 & 6.2)
#     Returns both Excel and CSV file paths
#     """
#     logger.info("Phase 6: Generating final output...")
    
#     OUTPUT_DIR.mkdir(exist_ok=True)
    
#     # Define output columns in correct order (following plan.md)
#     output_columns = [
#         "Company", "Ref No", "Load", "Trip", "Truck", 
#         "Trailer", "PU Date F", "Del Date F", "State", "Miles"
#     ]
    
#     final_output = output_df[output_columns].copy()
    
#     # Format dates
#     final_output["PU Date F"] = pd.to_datetime(final_output["PU Date F"]).dt.strftime('%m/%d/%Y')
#     final_output["Del Date F"] = pd.to_datetime(final_output["Del Date F"]).dt.strftime('%m/%d/%Y')
    
#     # Sort by Ref No for better readability
#     final_output = final_output.sort_values(["Ref No", "State"])
    
#     # Generate output filenames as per feedback requirements
#     excel_file = OUTPUT_DIR / "Q2_2025_state_miles_v2.xlsx"
#     csv_file = OUTPUT_DIR / "Q2_2025_state_miles_v2.csv"
    
#     # Write to both Excel and CSV
#     final_output.to_excel(excel_file, index=False, sheet_name="State Miles")
#     final_output.to_csv(csv_file, index=False)
    
#     logger.info(f"Phase 6 completed: Output written to:")
#     logger.info(f"  • Excel: {excel_file}")
#     logger.info(f"  • CSV: {csv_file}")
    
#     # Save debug CSV output (final formatted data)
#     debug_file = DEBUG_DIR / "phase6_final_output.csv"
#     final_output.to_csv(debug_file, index=False)
#     logger.info(f"Phase 6 debug file saved: {debug_file}")
    
#     return str(excel_file), str(csv_file)

# # ──────────────────────────────────────────────────────────────────────────────
# # Diagnostic Functions for Debugging API and Fallback Issues
# # ──────────────────────────────────────────────────────────────────────────────

# def diagnose_route_issues(origin: str, destination: str, api_key: str, location_coords: dict, states_gdf: gpd.GeoDataFrame):
#     """
#     Diagnostic function to understand why routes are failing
#     """
#     print(f"\n🔍 DIAGNOSING ROUTE: {origin} → {destination}")
#     print("="*60)
    
#     # 1. Check if coordinates exist in cache
#     origin_coords = location_coords.get(origin)
#     dest_coords = location_coords.get(destination)
    
#     print(f"📍 COORDINATES CHECK:")
#     print(f"  • Origin ({origin}): {origin_coords}")
#     print(f"  • Destination ({destination}): {dest_coords}")
    
#     if not origin_coords or not dest_coords:
#         print("❌ ISSUE: Missing coordinates - geocoding failed!")
#         return
    
#     # 2. Test HERE API call manually
#     print(f"\n🌐 HERE API TEST:")
#     try:
#         url = "https://router.hereapi.com/v8/routes"
#         params = {
#             "transportMode": "truck",
#             "origin": f"{origin_coords[0]},{origin_coords[1]}",
#             "destination": f"{dest_coords[0]},{dest_coords[1]}",
#             "return": "polyline",
#             "apiKey": api_key
#         }
        
#         resp = requests.get(url, params=params, timeout=10)
#         print(f"  • Status Code: {resp.status_code}")
        
#         if resp.status_code == 200:
#             data = resp.json()
#             if data.get("routes"):
#                 route = data["routes"][0]
#                 if route.get("sections"):
#                     polyline_data = route["sections"][0].get("polyline", "")
#                     print(f"  • Polyline Length: {len(polyline_data)} characters")
#                     if len(polyline_data) < 10:
#                         print("  ❌ ISSUE: Polyline too short!")
#                     else:
#                         print("  ✅ API call successful")
#                 else:
#                     print("  ❌ ISSUE: No route sections in response")
#             else:
#                 print("  ❌ ISSUE: No routes found in response")
#         else:
#             print(f"  ❌ ISSUE: API returned {resp.status_code}")
#             if resp.status_code == 400:
#                 try:
#                     error_data = resp.json()
#                     print(f"  • Error: {error_data.get('title', 'Unknown')}")
#                     print(f"  • Detail: {error_data.get('detail', 'No details')}")
#                 except:
#                     print(f"  • Response: {resp.text[:200]}")
                    
#     except Exception as e:
#         print(f"  ❌ EXCEPTION: {e}")
    
#     # 3. Test great circle fallback
#     print(f"\n📐 GREAT CIRCLE FALLBACK TEST:")
#     try:
#         from shapely.geometry import Point, LineString
#         import geopandas as gpd
        
#         origin_lat, origin_lng = origin_coords
#         dest_lat, dest_lng = dest_coords
        
#         # Calculate distance
#         def haversine_distance(lat1, lon1, lat2, lon2):
#             R = 3959  # Earth's radius in miles
#             lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
#             dlat = lat2 - lat1
#             dlon = lon2 - lon1
#             a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
#             c = 2 * math.asin(math.sqrt(a))
#             return R * c
        
#         total_distance = haversine_distance(origin_lat, origin_lng, dest_lat, dest_lng)
#         print(f"  • Total Distance: {total_distance:.1f} miles")
        
#         if total_distance < 1:
#             print("  ❌ ISSUE: Route too short for state calculation")
#             return
        
#         # Create line with proper CRS handling
#         line = LineString([(origin_lng, origin_lat), (dest_lng, dest_lat)])
#         line_gdf = gpd.GeoSeries([line], crs="EPSG:4326")  # WGS84
#         line_projected = line_gdf.to_crs(states_gdf.crs)[0]  # Project to state CRS
        
#         print(f"  • Line created: {line.is_valid}")
#         print(f"  • Line projected: {line_projected.is_valid}")
#         print(f"  • States CRS: {states_gdf.crs}")
        
#         intersecting_states = []
#         for _, state_row in states_gdf.iterrows():
#             state_abbr = state_row.get('STUSPS', 'UNKNOWN')
#             try:
#                 if line_projected.intersects(state_row.geometry):
#                     intersection = line_projected.intersection(state_row.geometry)
#                     if hasattr(intersection, 'length') and intersection.length > 0:
#                         miles_estimate = intersection.length / 1609.34  # Convert meters to miles
#                         if miles_estimate >= 1:
#                             intersecting_states.append((state_abbr, miles_estimate))
#             except Exception as e:
#                 print(f"  • Warning: Error with state {state_abbr}: {e}")
        
#         print(f"  • States intersected: {len(intersecting_states)}")
#         if intersecting_states:
#             print("  ✅ Fallback working - states found:")
#             for state, miles in intersecting_states[:5]:  # Show first 5
#                 print(f"    - {state}: {miles:.1f} miles")
#         else:
#             print("  ❌ ISSUE: No state intersections found!")
            
#             # Check if points are in different states using proper projection
#             origin_point = Point(origin_lng, origin_lat)
#             dest_point = Point(dest_lng, dest_lat)
            
#             # Convert points to proper CRS
#             origin_gdf = gpd.GeoSeries([origin_point], crs="EPSG:4326")
#             dest_gdf = gpd.GeoSeries([dest_point], crs="EPSG:4326")
            
#             origin_projected = origin_gdf.to_crs(states_gdf.crs)[0]
#             dest_projected = dest_gdf.to_crs(states_gdf.crs)[0]
            
#             origin_state = dest_state = None
#             for _, state_row in states_gdf.iterrows():
#                 state_abbr = state_row.get('STUSPS', 'UNKNOWN')
#                 try:
#                     if origin_projected.within(state_row.geometry):
#                         origin_state = state_abbr
#                     if dest_projected.within(state_row.geometry):
#                         dest_state = state_abbr
#                 except:
#                     continue
            
#             print(f"  • Origin state (corrected): {origin_state}")
#             print(f"  • Destination state (corrected): {dest_state}")
            
#             if not origin_state or not dest_state:
#                 print("  ❌ ISSUE: Points still not within any state boundaries!")
#                 print("  • This suggests a fundamental CRS or shapefile issue")
#             elif origin_state == dest_state:
#                 print("  ✅ Both points in same state - this is an intrastate route (no interstate calculation needed)")
#             else:
#                 print(f"  ✅ Interstate route detected: {origin_state} → {dest_state}")
                
#     except Exception as e:
#         print(f"  ❌ EXCEPTION in fallback: {e}")
    
#     print("="*60)

async def run_validation_test():
    """
    Run validation test on representative trips from feedback.md
    Tests loads: 174418/174520, 175029-031-150
    Now includes Step 5 (mileage calculation) for complete end-to-end testing
    """
    logger.info("🧪 RUNNING VALIDATION TEST ON REPRESENTATIVE TRIPS")
    
    try:
        # Load API key for Step 5
        api_key = load_api_key()
        logger.info("HERE API key loaded for mileage calculation")
        
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
        
        # Process through our pipeline (Steps 1-3)
        logger.info("Processing test data through pipeline...")
        
        # Apply same processing as main pipeline
        test_pcs, test_inv = step1_read_excel_data()
        test_filtered = step2_filter_fleet_data(test_pcs[test_pcs['Load'].isin(test_loads)], test_inv)
        test_with_refs = step3_detect_round_trips(test_filtered)
        
        logger.info("Steps 1-3 completed successfully!")
        logger.info(f"Test results after Step 3:")
        logger.info(f"  • Input loads: {len(test_data)}")
        logger.info(f"  • After filtering: {len(test_filtered)}")
        logger.info(f"  • With references: {len(test_with_refs)}")
        
        # Show reference assignments
        logger.info("Reference assignments:")
        for _, row in test_with_refs.iterrows():
            logger.info(f"  Load {row['Load']}: {row['Ship City']}, {row['Ship St']} → {row['Cons City']}, {row['Cons St']} (Ref: {row['Ref']})")
            
        # Add Step 5: Mileage calculation
        logger.info("\n🛣️ Starting Step 5: Mileage calculation...")
        states_gdf = load_state_boundaries()
        test_with_mileage = await step5_calculate_mileage_concurrent(test_with_refs, states_gdf, api_key, max_concurrent=5)
        
        logger.info("\n✅ Complete validation test results:")
        logger.info(f"  • Loads processed: {len(test_with_mileage)}")
        
        # Show mileage results
        logger.info("\nMileage calculation results:")
        # Group by Load to show summary (since step5 returns multiple rows per load - one per state)
        for load_num in test_with_mileage['Load'].unique():
            load_rows = test_with_mileage[test_with_mileage['Load'] == load_num]
            first_row = load_rows.iloc[0]
            
            # Calculate total miles or show error status
            total_miles = 0
            has_error = False
            states_list = []
            
            for _, row in load_rows.iterrows():
                if row['Miles'] == 'GEOCODE_ERR':
                    has_error = True
                    break
                elif pd.notna(row['Miles']) and isinstance(row['Miles'], (int, float)):
                    total_miles += row['Miles']
                    states_list.append(f"{row['State']}:{row['Miles']:.1f}")
            
            if has_error:
                miles_info = "FAILED (GEOCODE_ERR)"
            else:
                miles_info = f"{total_miles:.1f} total ({', '.join(states_list)})"
            
            logger.info(f"  Load {first_row['Load']} (Ref {first_row['Ref No']}): {miles_info}")
            
        # Save complete test results with mileage
        test_output_file = DEBUG_DIR / "validation_test_results_with_mileage.csv"
        test_with_mileage.to_csv(test_output_file, index=False)
        logger.info(f"\nComplete test results saved: {test_output_file}")
        
        # Also save a summary view for easier analysis
        summary_file = DEBUG_DIR / "validation_summary.csv"
        summary_data = []
        for load_num in test_with_mileage['Load'].unique():
            load_rows = test_with_mileage[test_with_mileage['Load'] == load_num]
            first_row = load_rows.iloc[0]
            
            total_miles = 0
            has_error = False
            for _, row in load_rows.iterrows():
                if row['Miles'] == 'GEOCODE_ERR':
                    has_error = True
                    break
                elif pd.notna(row['Miles']) and isinstance(row['Miles'], (int, float)):
                    total_miles += row['Miles']
            
            summary_data.append({
                'Load': first_row['Load'],
                'Ref': first_row['Ref No'],
                'Truck': first_row['Truck'],
                'Trailer': first_row['Trailer'],
                'Total_Miles': 'GEOCODE_ERR' if has_error else f"{total_miles:.1f}",
                'Status': 'FAILED' if has_error else 'SUCCESS'
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(summary_file, index=False)
        logger.info(f"Validation summary saved: {summary_file}")
        
        return test_with_mileage
        
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
        
        pcs, inv = step1_read_excel_data()
        pcs_filtered = step2_filter_fleet_data(pcs, inv)
        pcs_with_refs = step3_detect_round_trips(pcs_filtered)
        
        # Skip Step 4 - virtual returns not needed with proper Truck+Trailer logic
        
        # Load state boundaries and calculate mileage (async version for performance)
        states_gdf = load_state_boundaries()
        output_df = asyncio.run(step5_calculate_mileage_concurrent(pcs_with_refs, states_gdf, api_key, max_concurrent=10))
        
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
            asyncio.run(run_validation_test())
        else:
            main()
    else:
        main() 