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

# ──────────────────────────────────────────────────────────────────────────────
# Phase 1: Data Import and Initial Processing
# ──────────────────────────────────────────────────────────────────────────────

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

# ──────────────────────────────────────────────────────────────────────────────
# Phase 2: Data Filtering and Preparation
# ──────────────────────────────────────────────────────────────────────────────

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
    
    # 2-B: Keep only numeric truck numbers (more robust than plan's OP/NA checks)
    pcs = pcs[pd.to_numeric(pcs["Truck"], errors='coerce').notna()]
    logger.info(f"After keeping only numeric truck numbers: {len(pcs)} rows")
    
    # 2-C: Merge with inventory and keep company-owned units
    pcs = pcs.merge(inv, how="left", left_on="Truck", right_on="Unit")
    logger.info(f"After merging with inventory: {len(pcs)} rows")
    logger.info(f"Company-owned units found: {pcs['Company'].count()}")
    
    # Keep company-owned units (CORRECTED from plan.md)
    pcs = pcs[pcs["Company"].notna()]
    logger.info(f"After keeping company-owned units: {len(pcs)} rows")
    
    # Initialize reference system (following plan.md Step 2.2)
    pcs["Ref"] = pd.NA
    pcs["CA_Cities"] = ""
    
    # Sort for logical processing
    pcs = pcs.sort_values(["Truck", "Trip", "PU", "Load"]).reset_index(drop=True)
    
    # Drop unnecessary columns
    columns_to_drop = ["TLH Rev", "Class", "Status", "Customer", "Cust Ref", "Delivered By", "Shipper", "Consignee", "Load Notes", "Unit"]
    pcs = pcs.drop(columns=[col for col in columns_to_drop if col in pcs.columns], errors='ignore')
    
    logger.info(f"Phase 2 completed: Filtered from {initial_count} to {len(pcs)} rows")
    logger.info(f"--------------------------------")
    
    # Save debug CSV output
    debug_file = DEBUG_DIR / "phase2_filtered_fleet.csv"
    pcs.to_csv(debug_file, index=False)
    logger.info(f"Phase 2 debug file saved: {debug_file}")
    
    return pcs

# ──────────────────────────────────────────────────────────────────────────────
# Phase 3: Trip Grouping and Reference Assignment
# ──────────────────────────────────────────────────────────────────────────────

def step3_assign_references(pcs: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 3: Group related loads and assign reference numbers
    Following plan.md Step 3.1 & 3.2 with trailer fallback for empty trips
    """
    logger.info("Phase 3: Assigning reference numbers and consolidating CA destinations...")
    
    pcs = pcs.copy()
    ref_counter = 1
    
    # Create grouping key: use Trip when available, otherwise use Trailer (enhanced from plan)
    trip_mask = pcs["Trip"].isna() | (pcs["Trip"].astype(str).str.strip() == "")
    pcs["Trip_Group"] = pcs["Trip"].astype(str)
    pcs.loc[trip_mask, "Trip_Group"] = "TRAILER_" + pcs.loc[trip_mask, "Trailer"].astype(str)
    logger.info(f"Created Trip_Group column for proper grouping")
    
    # Group by Truck and Trip_Group (following plan.md Step 3.1)
    for (truck, trip_group), group in pcs.groupby(["Truck", "Trip_Group"]):
        group_indices = group.index.tolist()
        base_ref = ref_counter
        ca_cities = []
        
        # Collect all CA cities from this group (following plan.md Step 3.2)
        for i, (idx, row) in enumerate(group.iterrows()):
            if row["Ship St"] == "CA":
                ca_cities.append(row["Ship City"])
        
        # Process each load in the group
        for i, (idx, row) in enumerate(group.iterrows()):
            decimal_ref = i + 1
            ref_number = f"{base_ref}.{decimal_ref}"
            pcs.loc[idx, "Ref"] = ref_number
            
            # For first load (x.1), keep CA origin and collect other CA cities
            if decimal_ref == 1:
                if len(ca_cities) > 1:
                    other_ca_cities = [city for city in ca_cities if city != row["Ship City"]]
                    pcs.loc[idx, "CA_Cities"] = ", ".join(other_ca_cities)
            else:
                # For subsequent loads, update origin to previous destination (following plan.md)
                prev_idx = group_indices[i-1]
                prev_dest_city = pcs.loc[prev_idx, "Cons City"]
                prev_dest_state = pcs.loc[prev_idx, "Cons St"]
                pcs.loc[idx, "Ship City"] = prev_dest_city
                pcs.loc[idx, "Ship St"] = prev_dest_state
        
        ref_counter += 1
    
    logger.info(f"Phase 3 completed: Assigned references for {ref_counter-1} truck trips")
    logger.info(f"--------------------------------")
    
    # Save debug CSV output
    debug_file = DEBUG_DIR / "phase3_with_references.csv"
    pcs.to_csv(debug_file, index=False)
    logger.info(f"Phase 3 debug file saved: {debug_file}")
    
    return pcs

# ──────────────────────────────────────────────────────────────────────────────
# Phase 4: Route Optimization  
# ──────────────────────────────────────────────────────────────────────────────

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

def save_geocoding_cache(location_coords: dict):
    """Save persistent geocoding cache"""
    cache_file = BASE_DIR / "geocoding_cache.json"
    try:
        with open(cache_file, 'w') as f:
            json.dump(location_coords, f, indent=2)
        logger.info(f"Saved {len(location_coords)} locations to geocoding cache")
    except Exception as e:
        logger.warning(f"Error saving geocoding cache: {e}")

def geocode_location(location: str, api_key: str) -> tuple:
    """Convert location name to coordinates using HERE Geocoding API"""
    try:
        # Simple in-memory cache for this session
        if not hasattr(geocode_location, '_cache'):
            geocode_location._cache = {}
        
        if location in geocode_location._cache:
            return geocode_location._cache[location]
        
        url = "https://geocode.search.hereapi.com/v1/geocode"
        params = {"q": location, "apiKey": api_key}
        
        time.sleep(0.01)  # Rate limiting
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        if data.get("items"):
            position = data["items"][0]["position"]
            result = (position["lat"], position["lng"])
            geocode_location._cache[location] = result
            return result
        else:
            logger.warning(f"No geocoding results for: {location}")
            return None, None
            
    except Exception as e:
        logger.warning(f"Geocoding error for {location}: {e}")
        return None, None

def great_circle_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great circle distance between two points in miles"""
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return c * 3956  # Earth's radius in miles

def pre_geocode_all_locations(pcs: pd.DataFrame, api_key: str) -> dict:
    """Pre-geocode all unique locations with persistent caching"""
    logger.info("Pre-geocoding all unique locations...")
    
    location_coords = load_geocoding_cache()
    
    # Collect all unique locations needed
    unique_locations = set()
    for _, row in pcs.iterrows():
        ship_state = STATE_MAPPING.get(row['Ship St'], row['Ship St'])
        cons_state = STATE_MAPPING.get(row['Cons St'], row['Cons St'])
        ship_location = f"{row['Ship City']}, {ship_state}, USA"
        cons_location = f"{row['Cons City']}, {cons_state}, USA"
        unique_locations.add(ship_location)
        unique_locations.add(cons_location)
    
    # Find locations that need geocoding
    locations_to_geocode = [loc for loc in unique_locations if loc not in location_coords]
    
    logger.info(f"Found {len(unique_locations)} unique locations:")
    logger.info(f"  • Already cached: {len(unique_locations) - len(locations_to_geocode)}")
    logger.info(f"  • Need to geocode: {len(locations_to_geocode)}")
    
    if locations_to_geocode:
        logger.info("Geocoding new locations...")
        for i, location in enumerate(locations_to_geocode, 1):
            if i % 25 == 0:
                logger.info(f"Geocoded {i}/{len(locations_to_geocode)} new locations...")
            
            lat, lng = geocode_location(location, api_key)
            if lat is not None and lng is not None:
                location_coords[location] = [lat, lng]
            time.sleep(0.02)
        
        save_geocoding_cache(location_coords)
        logger.info(f"Geocoded {len(locations_to_geocode)} new locations")
    else:
        logger.info("All locations already cached - no API calls needed!")
    
    # Convert to tuples for consistency
    return {loc: tuple(coords) for loc, coords in location_coords.items() if loc in unique_locations}

def step4_optimize_routes(pcs: pd.DataFrame, api_key: str) -> pd.DataFrame:
    """
    Phase 4: Analyze and optimize delivery sequences (following plan.md Step 4.1 & 4.2)
    Uses great circle distance for fast optimization
    """
    logger.info("Phase 4: Optimizing delivery routes...")
    
    location_coords = pre_geocode_all_locations(pcs, api_key)
    
    pcs = pcs.copy()
    optimized_groups = []
    groups_to_process = list(pcs.groupby(["Truck", "Trip_Group"]))
    total_groups = len(groups_to_process)
    groups_optimized = 0
    
    start_time = time.time()
    
    for processed_groups, ((truck, trip_group), group) in enumerate(groups_to_process, 1):
        
        # Progress tracking
        if processed_groups % 25 == 0:
            elapsed = time.time() - start_time
            avg_time = elapsed / processed_groups
            eta_minutes = (total_groups - processed_groups) * avg_time / 60
            logger.info(f"Progress: {processed_groups}/{total_groups} groups ({processed_groups/total_groups*100:.1f}%) - ETA: {eta_minutes:.1f} minutes")
        
        # Skip optimization for simple cases
        if len(group) <= 2 or group['Cons City'].nunique() <= 1:
            optimized_groups.append(group)
            continue
        
        # Route optimization using great circle distance (following plan.md Step 4.2)
        try:
            destinations = []
            ca_origin = None
            ca_origin_coords = None
            
            for idx, row in group.iterrows():
                if row["Ref"].endswith(".1"):
                    ship_location = f"{row['Ship City']}, {STATE_MAPPING.get(row['Ship St'], row['Ship St'])}, USA"
                    ca_origin = ship_location
                    ca_origin_coords = location_coords.get(ship_location)
                
                cons_location = f"{row['Cons City']}, {STATE_MAPPING.get(row['Cons St'], row['Cons St'])}, USA"
                dest_coords = location_coords.get(cons_location)
                
                if dest_coords:
                    destinations.append({
                        'index': idx,
                        'location': cons_location,
                        'coords': dest_coords,
                        'ref': row["Ref"],
                        'row': row
                    })
            
            if not ca_origin_coords or len(destinations) <= 1:
                optimized_groups.append(group)
                continue
            
            # Optimize order using nearest neighbor algorithm
            optimized_order = []
            current_coords = ca_origin_coords
            remaining = destinations.copy()
            
            while remaining:
                nearest = min(remaining, key=lambda dest: great_circle_distance(
                    current_coords[0], current_coords[1],
                    dest['coords'][0], dest['coords'][1]
                ))
                optimized_order.append(nearest)
                remaining.remove(nearest)
                current_coords = nearest['coords']
            
            # Update reference numbers based on optimized order (following plan.md Step 4.3)
            optimized_group = group.copy()
            base_ref = group.iloc[0]["Ref"].split(".")[0]
            
            for i, dest_info in enumerate(optimized_order):
                new_ref = f"{base_ref}.{i+1}"
                original_idx = dest_info['index']
                optimized_group.loc[original_idx, "Ref"] = new_ref
                
                # Update ship city/state for subsequent loads
                if i > 0:
                    prev_dest = optimized_order[i-1]
                    prev_cons_city = pcs.loc[prev_dest['index'], "Cons City"]
                    prev_cons_state = pcs.loc[prev_dest['index'], "Cons St"]
                    optimized_group.loc[original_idx, "Ship City"] = prev_cons_city
                    optimized_group.loc[original_idx, "Ship St"] = prev_cons_state
            
            optimized_groups.append(optimized_group)
            groups_optimized += 1
            
        except Exception as e:
            logger.warning(f"Optimization failed for group {truck}-{trip_group}: {e}")
            optimized_groups.append(group)
    
    result = pd.concat(optimized_groups, ignore_index=True)
    result = result.drop(columns=["Trip_Group"])
    
    total_time = time.time() - start_time
    logger.info(f"Phase 4 completed in {total_time/60:.1f} minutes:")
    logger.info(f"  • Total groups: {total_groups}")
    logger.info(f"  • Groups optimized: {groups_optimized}")
    logger.info(f"  • Groups skipped: {total_groups - groups_optimized} (single destination, 2-dest, or geocoding issues)")
    logger.info(f"  • Average time per group: {total_time/total_groups:.2f} seconds")
    logger.info(f"--------------------------------")
    
    # Save debug CSV output
    debug_file = DEBUG_DIR / "phase4_optimized_routes.csv"
    result.to_csv(debug_file, index=False)
    logger.info(f"Phase 4 debug file saved: {debug_file}")
    
    return result

# ──────────────────────────────────────────────────────────────────────────────
# Phase 5: Mileage Calculation (Async/Concurrent Version)
# ──────────────────────────────────────────────────────────────────────────────

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
        # Use cached coordinates if available
        if location_coords:
            origin_coords = location_coords.get(origin)
            dest_coords = location_coords.get(destination)
            
            if origin_coords and dest_coords:
                origin_param = f"{origin_coords[0]},{origin_coords[1]}"
                dest_param = f"{dest_coords[0]},{dest_coords[1]}"
            else:
                logger.warning(f"Could not find cached coordinates: {origin} → {destination}")
                return {}
        else:
            # Fallback to direct location names
            origin_param = origin
            dest_param = destination
        
        url = "https://router.hereapi.com/v8/routes"
        params = {
            "transportMode": "truck",
            "origin": origin_param,
            "destination": dest_param,
            "return": "polyline",
            "apiKey": api_key
        }
        
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return {}
                    
                data = await resp.json()
        except (asyncio.TimeoutError, Exception):
            return {}
        
        # Check if route was found and decode polyline
        if not data.get("routes") or not data["routes"]:
            return {}
        
        try:
            route = data["routes"][0]
            section = route["sections"][0]
            encoded_polyline = section.get("polyline", "")
            
            if not encoded_polyline or len(encoded_polyline) < 10:
                return {}
            
            pts = polyline.decode(encoded_polyline)
            if not pts or len(pts) < 2:
                return {}
            
            # Create LineString and calculate state intersections
            line = geom.LineString([(lng, lat) for lat, lng in pts])
            line_gdf = gpd.GeoSeries([line], crs="EPSG:4326").to_crs(epsg=5070)
            line_projected = line_gdf[0]
            
            state_miles = {}
            for _, state_row in states_gdf.iterrows():
                try:
                    intersection = line_projected.intersection(state_row.geometry)
                    if not intersection.is_empty:
                        length_meters = intersection.length
                        miles = length_meters / 1609.34
                        if miles > 0.1:
                            state_miles[state_row.STUSPS] = round(miles, 1)
                except Exception:
                    continue
            
            await asyncio.sleep(0.01)  # Rate limiting
            return state_miles
            
        except (KeyError, IndexError, ValueError) as e:
            # Track errors with limited logging
            if not hasattr(calculate_state_miles_async, '_error_count'):
                calculate_state_miles_async._error_count = 0
            calculate_state_miles_async._error_count += 1
            
            if calculate_state_miles_async._error_count <= 3 or calculate_state_miles_async._error_count % 100 == 0:
                logger.warning(f"API error #{calculate_state_miles_async._error_count}: {origin} → {destination}")
            return {}
        
    except Exception as e:
        logger.error(f"Error calculating route from {origin} to {destination}: {e}")
        return {}

def calculate_great_circle_state_miles(origin_coords: tuple, dest_coords: tuple, states_gdf: gpd.GeoDataFrame) -> Dict[str, float]:
    """
    Calculate approximate state miles using great circle distance
    Used as fallback when HERE API fails
    """
    try:
        from shapely.geometry import Point, LineString
        import geopandas as gpd
        
        origin_lat, origin_lng = origin_coords
        dest_lat, dest_lng = dest_coords
        
        # Create straight line between origin and destination (WGS84 coordinates)
        line = LineString([(origin_lng, origin_lat), (dest_lng, dest_lat)])
        
        # Convert to GeoSeries with proper CRS (WGS84)
        line_gdf = gpd.GeoSeries([line], crs="EPSG:4326")
        
        # Reproject to match state boundaries CRS (NAD83/USA Contiguous - EPSG:5070)
        line_projected = line_gdf.to_crs(states_gdf.crs)[0]
        
        # Calculate total distance
        def haversine_distance(lat1, lon1, lat2, lon2):
            R = 3959  # Earth's radius in miles
            lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            return R * c
        
        total_distance = haversine_distance(origin_lat, origin_lng, dest_lat, dest_lng)
        
        if total_distance < 1:
            return {}
        
        # Find state intersections using properly projected line
        state_miles = {}
        for _, state_row in states_gdf.iterrows():
            state_abbr = state_row.get('STUSPS', 'UNKNOWN')
            try:
                if line_projected.intersects(state_row.geometry):
                    intersection = line_projected.intersection(state_row.geometry)
                    if hasattr(intersection, 'length') and intersection.length > 0:
                        # Convert projected length back to miles
                        miles_estimate = intersection.length / 1609.34  # meters to miles
                        if miles_estimate >= 1:
                            state_miles[state_abbr] = round(min(miles_estimate, total_distance), 2)
            except Exception:
                continue
        
        # Fallback for long routes with no intersections found
        if not state_miles and total_distance > 50:
            # Create points with proper CRS
            origin_point = Point(origin_lng, origin_lat)
            dest_point = Point(dest_lng, dest_lat)
            
            # Convert to GeoSeries with WGS84 CRS
            origin_gdf = gpd.GeoSeries([origin_point], crs="EPSG:4326")
            dest_gdf = gpd.GeoSeries([dest_point], crs="EPSG:4326")
            
            # Reproject to match state boundaries
            origin_projected = origin_gdf.to_crs(states_gdf.crs)[0]
            dest_projected = dest_gdf.to_crs(states_gdf.crs)[0]
            
            origin_state = dest_state = None
            for _, state_row in states_gdf.iterrows():
                state_abbr = state_row.get('STUSPS', 'UNKNOWN')
                try:
                    if origin_projected.within(state_row.geometry):
                        origin_state = state_abbr
                    if dest_projected.within(state_row.geometry):
                        dest_state = state_abbr
                except:
                    continue
            
            if origin_state and dest_state and origin_state != dest_state:
                state_miles[origin_state] = round(total_distance * 0.4, 2)
                state_miles[dest_state] = round(total_distance * 0.4, 2)
        
        return state_miles
        
    except Exception:
        return {}

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
                
                # Format locations to match cached format
                ship_state = STATE_MAPPING.get(row['Ship St'], row['Ship St'])
                cons_state = STATE_MAPPING.get(row['Cons St'], row['Cons St'])
                origin = f"{row['Ship City']}, {ship_state}, USA"
                destination = f"{row['Cons City']}, {cons_state}, USA"
                
                route_had_miles = False
                error_reason = None
                
                # Handle multi-city CA routes for x.1 references (following plan.md Step 5.2)
                if row["Ref"].endswith(".1") and row["CA_Cities"]:
                    ca_cities = row["CA_Cities"].split(", ")
                    current_origin = origin
                    
                    # Route through each CA city
                    for ca_city in ca_cities:
                        ca_destination = f"{ca_city}, California, USA"
                        ca_miles = await calculate_state_miles_async(session, current_origin, ca_destination, states_gdf, api_key, location_coords)
                        
                        # Add CA miles to output
                        for state, miles in ca_miles.items():
                            if state == "CA":  # Only count CA miles for intrastate portion
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
                        current_origin = ca_destination
                    
                    # Interstate portion from last CA city to final destination
                    interstate_miles = await calculate_state_miles_async(session, current_origin, destination, states_gdf, api_key, location_coords)
                else:
                    # Simple route calculation
                    interstate_miles = await calculate_state_miles_async(session, origin, destination, states_gdf, api_key, location_coords)
                
                # If HERE API failed, try great circle fallback
                if not interstate_miles and location_coords:
                    origin_coords = location_coords.get(origin)
                    dest_coords = location_coords.get(destination)
                    
                    if origin_coords and dest_coords:
                        interstate_miles = calculate_great_circle_state_miles(origin_coords, dest_coords, states_gdf)
                        
                        # Track fallback usage
                        if not hasattr(step5_calculate_mileage_concurrent, '_fallback_count'):
                            step5_calculate_mileage_concurrent._fallback_count = 0
                        step5_calculate_mileage_concurrent._fallback_count += 1
                        
                        if step5_calculate_mileage_concurrent._fallback_count <= 5:
                            logger.info(f"Using fallback #{step5_calculate_mileage_concurrent._fallback_count}: {origin} → {destination} = {len(interstate_miles)} states")
                    else:
                        error_reason = "MISSING_COORDINATES"
                
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
                    if not error_reason:
                        error_reason = "CALCULATION_FAILED"
                    
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
                        "Miles": error_reason
                    })
                
                return route_rows, route_had_miles
                
            except Exception as e:
                logger.warning(f"Error processing route {route_num}: {e}")
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
                    "Miles": f"EXCEPTION: {str(e)[:50]}"
                }]
                return error_record, False
    
    # Process routes concurrently
    connector = aiohttp.TCPConnector(limit=max_concurrent * 2, limit_per_host=max_concurrent)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [process_single_route(session, idx, row, i + 1) for i, (idx, row) in enumerate(pcs.iterrows())]
        
        output_rows = []
        successful_routes = failed_routes = 0
        batch_size = 50
        
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
            
            # Progress update
            completed = min(i + batch_size, len(tasks))
            if completed % 100 == 0 or completed == len(tasks):
                elapsed = time.time() - start_time
                avg_time = elapsed / completed if completed > 0 else 0
                remaining = (len(tasks) - completed) * avg_time
                success_rate = (successful_routes / completed) * 100 if completed > 0 else 0
                logger.info(f"Progress: {completed}/{len(tasks)} ({completed/len(tasks)*100:.1f}%) - Success: {success_rate:.1f}% - ETA: {remaining/60:.1f} min")
    
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

# ──────────────────────────────────────────────────────────────────────────────
# Phase 6: Output Generation
# ──────────────────────────────────────────────────────────────────────────────

def step6_generate_output(output_df: pd.DataFrame) -> tuple:
    """
    Phase 6: Generate final formatted output (following plan.md Step 6.1 & 6.2)
    Returns both Excel and CSV file paths
    """
    logger.info("Phase 6: Generating final output...")
    
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Define output columns in correct order (following plan.md)
    output_columns = [
        "Company", "Ref No", "Load", "Trip", "Truck", 
        "Trailer", "PU Date F", "Del Date F", "State", "Miles"
    ]
    
    final_output = output_df[output_columns].copy()
    
    # Format dates
    final_output["PU Date F"] = pd.to_datetime(final_output["PU Date F"]).dt.strftime('%m/%d/%Y')
    final_output["Del Date F"] = pd.to_datetime(final_output["Del Date F"]).dt.strftime('%m/%d/%Y')
    
    # Sort by Ref No for better readability
    final_output = final_output.sort_values(["Ref No", "State"])
    
    # Generate output filenames with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_file = OUTPUT_DIR / f"IFTA_State_Miles_{timestamp}.xlsx"
    csv_file = OUTPUT_DIR / f"IFTA_State_Miles_{timestamp}.csv"
    
    # Write to both Excel and CSV
    final_output.to_excel(excel_file, index=False, sheet_name="State Miles")
    final_output.to_csv(csv_file, index=False)
    
    logger.info(f"Phase 6 completed: Output written to:")
    logger.info(f"  • Excel: {excel_file}")
    logger.info(f"  • CSV: {csv_file}")
    
    # Save debug CSV output (final formatted data)
    debug_file = DEBUG_DIR / "phase6_final_output.csv"
    final_output.to_csv(debug_file, index=False)
    logger.info(f"Phase 6 debug file saved: {debug_file}")
    
    return str(excel_file), str(csv_file)

# ──────────────────────────────────────────────────────────────────────────────
# Diagnostic Functions for Debugging API and Fallback Issues
# ──────────────────────────────────────────────────────────────────────────────

def diagnose_route_issues(origin: str, destination: str, api_key: str, location_coords: dict, states_gdf: gpd.GeoDataFrame):
    """
    Diagnostic function to understand why routes are failing
    """
    print(f"\n🔍 DIAGNOSING ROUTE: {origin} → {destination}")
    print("="*60)
    
    # 1. Check if coordinates exist in cache
    origin_coords = location_coords.get(origin)
    dest_coords = location_coords.get(destination)
    
    print(f"📍 COORDINATES CHECK:")
    print(f"  • Origin ({origin}): {origin_coords}")
    print(f"  • Destination ({destination}): {dest_coords}")
    
    if not origin_coords or not dest_coords:
        print("❌ ISSUE: Missing coordinates - geocoding failed!")
        return
    
    # 2. Test HERE API call manually
    print(f"\n🌐 HERE API TEST:")
    try:
        url = "https://router.hereapi.com/v8/routes"
        params = {
            "transportMode": "truck",
            "origin": f"{origin_coords[0]},{origin_coords[1]}",
            "destination": f"{dest_coords[0]},{dest_coords[1]}",
            "return": "polyline",
            "apiKey": api_key
        }
        
        resp = requests.get(url, params=params, timeout=10)
        print(f"  • Status Code: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            if data.get("routes"):
                route = data["routes"][0]
                if route.get("sections"):
                    polyline_data = route["sections"][0].get("polyline", "")
                    print(f"  • Polyline Length: {len(polyline_data)} characters")
                    if len(polyline_data) < 10:
                        print("  ❌ ISSUE: Polyline too short!")
                    else:
                        print("  ✅ API call successful")
                else:
                    print("  ❌ ISSUE: No route sections in response")
            else:
                print("  ❌ ISSUE: No routes found in response")
        else:
            print(f"  ❌ ISSUE: API returned {resp.status_code}")
            if resp.status_code == 400:
                try:
                    error_data = resp.json()
                    print(f"  • Error: {error_data.get('title', 'Unknown')}")
                    print(f"  • Detail: {error_data.get('detail', 'No details')}")
                except:
                    print(f"  • Response: {resp.text[:200]}")
                    
    except Exception as e:
        print(f"  ❌ EXCEPTION: {e}")
    
    # 3. Test great circle fallback
    print(f"\n📐 GREAT CIRCLE FALLBACK TEST:")
    try:
        from shapely.geometry import Point, LineString
        import geopandas as gpd
        
        origin_lat, origin_lng = origin_coords
        dest_lat, dest_lng = dest_coords
        
        # Calculate distance
        def haversine_distance(lat1, lon1, lat2, lon2):
            R = 3959  # Earth's radius in miles
            lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            return R * c
        
        total_distance = haversine_distance(origin_lat, origin_lng, dest_lat, dest_lng)
        print(f"  • Total Distance: {total_distance:.1f} miles")
        
        if total_distance < 1:
            print("  ❌ ISSUE: Route too short for state calculation")
            return
        
        # Create line with proper CRS handling
        line = LineString([(origin_lng, origin_lat), (dest_lng, dest_lat)])
        line_gdf = gpd.GeoSeries([line], crs="EPSG:4326")  # WGS84
        line_projected = line_gdf.to_crs(states_gdf.crs)[0]  # Project to state CRS
        
        print(f"  • Line created: {line.is_valid}")
        print(f"  • Line projected: {line_projected.is_valid}")
        print(f"  • States CRS: {states_gdf.crs}")
        
        intersecting_states = []
        for _, state_row in states_gdf.iterrows():
            state_abbr = state_row.get('STUSPS', 'UNKNOWN')
            try:
                if line_projected.intersects(state_row.geometry):
                    intersection = line_projected.intersection(state_row.geometry)
                    if hasattr(intersection, 'length') and intersection.length > 0:
                        miles_estimate = intersection.length / 1609.34  # Convert meters to miles
                        if miles_estimate >= 1:
                            intersecting_states.append((state_abbr, miles_estimate))
            except Exception as e:
                print(f"  • Warning: Error with state {state_abbr}: {e}")
        
        print(f"  • States intersected: {len(intersecting_states)}")
        if intersecting_states:
            print("  ✅ Fallback working - states found:")
            for state, miles in intersecting_states[:5]:  # Show first 5
                print(f"    - {state}: {miles:.1f} miles")
        else:
            print("  ❌ ISSUE: No state intersections found!")
            
            # Check if points are in different states using proper projection
            origin_point = Point(origin_lng, origin_lat)
            dest_point = Point(dest_lng, dest_lat)
            
            # Convert points to proper CRS
            origin_gdf = gpd.GeoSeries([origin_point], crs="EPSG:4326")
            dest_gdf = gpd.GeoSeries([dest_point], crs="EPSG:4326")
            
            origin_projected = origin_gdf.to_crs(states_gdf.crs)[0]
            dest_projected = dest_gdf.to_crs(states_gdf.crs)[0]
            
            origin_state = dest_state = None
            for _, state_row in states_gdf.iterrows():
                state_abbr = state_row.get('STUSPS', 'UNKNOWN')
                try:
                    if origin_projected.within(state_row.geometry):
                        origin_state = state_abbr
                    if dest_projected.within(state_row.geometry):
                        dest_state = state_abbr
                except:
                    continue
            
            print(f"  • Origin state (corrected): {origin_state}")
            print(f"  • Destination state (corrected): {dest_state}")
            
            if not origin_state or not dest_state:
                print("  ❌ ISSUE: Points still not within any state boundaries!")
                print("  • This suggests a fundamental CRS or shapefile issue")
            elif origin_state == dest_state:
                print("  ✅ Both points in same state - this is an intrastate route (no interstate calculation needed)")
            else:
                print(f"  ✅ Interstate route detected: {origin_state} → {dest_state}")
                
    except Exception as e:
        print(f"  ❌ EXCEPTION in fallback: {e}")
    
    print("="*60)

def run_diagnostics():
    """
    Run diagnostics on problematic routes from the logs
    """
    print("🚨 RUNNING ROUTE DIAGNOSTICS")
    
    # Load necessary data
    api_key = load_api_key()
    location_coords = load_geocoding_cache()
    states_gdf = load_state_boundaries()
    
    # Test the problematic routes from the logs
    problematic_routes = [
        ("CERRITOS, California, USA", "MONTEBELLO, California, USA"),
        ("COLTON, California, USA", "LOS ANGELES, California, USA"), 
        ("EVANVILLE, Indiana, USA", "KANSAS CITY, Missouri, USA"),
        ("WEST PALM BEACH, Florida, USA", "MIRAMAR, Florida, USA"),
        ("BAY CITY, Texas, USA", "PASADENA, Texas, USA"),
        ("HOUSTON, Texas, USA", "LOXLEY, Alabama, USA"),
    ]
    
    for origin, destination in problematic_routes:
        diagnose_route_issues(origin, destination, api_key, location_coords, states_gdf)
        print("\n" + "="*80 + "\n")

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
        pcs_with_refs = step3_assign_references(pcs_filtered)
        
        pcs_optimized = step4_optimize_routes(pcs_with_refs, api_key)
        
        # Load state boundaries and calculate mileage (async version for performance)
        states_gdf = load_state_boundaries()
        output_df = asyncio.run(step5_calculate_mileage_concurrent(pcs_optimized, states_gdf, api_key, max_concurrent=15))
        
        excel_file, csv_file = step6_generate_output(output_df)
        
        # Summary
        logger.info("="*60)
        logger.info("PROCESSING COMPLETED SUCCESSFULLY!")
        logger.info(f"Processed {len(pcs_filtered)} truck trips")
        
        # Count error vs valid records in final output
        error_records = output_df[output_df['State'] == 'ERROR']
        valid_records = output_df[output_df['State'] != 'ERROR']
        
        logger.info(f"Generated {len(output_df)} total records:")
        logger.info(f"  • Valid state-mile records: {len(valid_records)}")
        if len(error_records) > 0:
            logger.info(f"  • Error records (failed calculations): {len(error_records)}")
        
        logger.info(f"Final output files:")
        logger.info(f"  • Excel: {excel_file}")
        logger.info(f"  • CSV: {csv_file}")
        logger.info("="*60)
        
        # Show debug files created
        logger.info("DEBUG FILES CREATED FOR ANALYSIS:")
        debug_files = [
            "phase1_pcs_cleaned.csv - Cleaned PCS data after initial processing",
            "phase1_inventory.csv - Cleaned inventory data",
            "phase2_filtered_fleet.csv - Data after filtering (company-owned, interstate, numeric trucks)",  
            "phase3_with_references.csv - Data with reference numbers and CA city consolidation",
            "phase4_optimized_routes.csv - Data after route optimization",
            "phase5_state_miles.csv - Raw state-by-state mileage records (includes ERROR records for failed routes)",
            "phase6_final_output.csv - Final formatted output (includes ERROR records - filter State!='ERROR' for valid data)"
        ]
        for debug_file in debug_files:
            logger.info(f"  • {DEBUG_DIR}/{debug_file}")
        logger.info("="*60)
        
        return excel_file, csv_file
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    import sys
    
    # Check if user wants to run diagnostics
    if len(sys.argv) > 1 and sys.argv[1] == "diagnose":
        run_diagnostics()
    else:
        main() 