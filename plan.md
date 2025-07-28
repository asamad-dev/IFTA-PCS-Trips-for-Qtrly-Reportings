# IFTA PCS Trips Processing System - Implementation Plan

## Overview
This system processes truck delivery data to calculate state-by-state mileage for IFTA quarterly reporting. The workflow involves data cleaning, route optimization, and mileage calculation using HERE API and geospatial data.

## Input Data
- **Primary File**: `M-G PCS Trips PCS A sterling group 2Q 2025 07.23.2025 - AJ.xlsx`
- **Sheets Required**:
  - `Inventory details` - Unit and Company information
  - `Export Research 07-22-2025` - Trip data
  - `Output` - Template for results

## Business Logic Summary
- All drivers start/end trips in California (CA)
- Trips involve multiple states requiring route optimization
- Calculate miles driven in each state for IFTA compliance
- Group related loads by truck and optimize delivery sequence

---

## Phase 1: Data Import and Initial Processing

### Step 1.1: Read Excel Data
```python
# Read main trip data
pcs = pd.read_excel(INPUT_FILE, sheet_name="Export Research 07-22-2025", keep_default_na=False)

# Read inventory data for company-owned units
inv = pd.read_excel(INPUT_FILE, sheet_name="Inventory details", usecols=["Unit","Company"])
```

### Step 1.2: Data Cleanup and Standardization
```python
# Basic field cleanup for deterministic grouping
pcs["Truck"] = pcs["Truck"].str.strip()
pcs["Trailer"] = pcs["Trailer"].astype(str).str.strip()
pcs["Ship St"] = pcs["Ship St"].str.upper().str.strip()
pcs["Cons St"] = pcs["Cons St"].str.upper().str.strip()

# Date processing
pcs["PU Date F"] = pd.to_datetime(pcs["PU Date F"])
pcs["Del Date F"] = pd.to_datetime(pcs["Del Date F"])

# Rename columns for consistency
pcs.rename(columns={
    "PU Date F": "PU",
    "Del Date F": "DEL"
}, inplace=True)

# Inventory cleanup
inv['Unit'] = inv['Unit'].astype(str).str.strip()
pcs['Truck'] = pcs['Truck'].astype(str).str.strip()
```

---

## Phase 2: Data Filtering and Preparation

### Step 2.1: Filter Fleet Data
```python
def step2_filter_fleet(pcs, inv):
    # Merge to identify company-owned units
    pcs = pcs.merge(inv, how="left", left_on="Truck", right_on="Unit")
    
    # Apply business filters:
    # 2-A: Drop owner-operators (no Company)
    pcs = pcs[pcs["Company"].isna()]
    
    # 2-B: Drop purely intrastate trips
    pcs = pcs[pcs["Ship St"] != pcs["Cons St"]]
    
    # 2-C: Drop "OP" tractors
    pcs = pcs[pcs["Truck"] != "OP"]
    
    # 2-D: Drop "NA" tractors
    pcs = pcs[pcs["Truck"] != "NA"]
    
    return pcs
```

### Step 2.2: Initialize Reference System
```python
# Add reference column for trip grouping
pcs["Ref"] = pd.NA

# Sort for logical processing
pcs = pcs.sort_values(["Truck", "PU", "Load"])
```

---

## Phase 3: Trip Grouping and Reference Assignment

### Step 3.1: Group Related Loads
**Business Rule**: Loads with same Truck and Trip should be grouped and assigned sequential reference numbers (e.g., 9.1, 9.2, 9.3)

### Step 3.2: Consolidate CA Destinations
**Logic**: 
- First load in group (x.1) keeps CA as origin
- Collect all CA destinations from group into `CA_Cities` column
- Subsequent loads (x.2, x.3, etc.) start from previous destination

**Example Transformation**:
```
Before:
Load 175029: COLTON, CA → HOUSTON, TX (Ref: 9.1)
Load 175031: HUNTINGTON BEACH, CA → PANAMA CITY, FL (Ref: 9.2)  
Load 175030: LOS ANGELES, CA → LOXLEY, AL (Ref: 9.3)

After:
Load 175029: COLTON, CA → HOUSTON, TX (Ref: 9.1, CA_Cities: "HUNTINGTON BEACH, LOS ANGELES")
Load 175031: HOUSTON, TX → PANAMA CITY, FL (Ref: 9.2)
Load 175030: PANAMA CITY, FL → LOXLEY, AL (Ref: 9.3)
```

---

## Phase 4: Route Optimization

### Step 4.1: Analyze Delivery Sequence
**Challenge**: Original sequence may not be geographically optimal
- Use HERE API to calculate distances between destinations
- Use `cb_2024_us_state_500k.shp` for state boundary validation
- Reorder destinations to minimize total travel distance

### Step 4.2: Route Optimization Algorithm
```python
def optimize_route_sequence(destinations):
    """
    Input: List of destinations from CA loads
    Output: Optimized sequence with updated reference numbers
    
    Example:
    Original: CA → TX → FL → AL
    Optimized: CA → TX → AL → FL (AL is between TX and FL)
    """
    # Implementation options:
    # 1. Distance-based optimization using HERE API
    # 2. Geospatial analysis using shapefile data
    # 3. LLM-assisted route optimization
```

### Step 4.3: Update Reference Numbers
After route optimization, update the `Ref` numbers to reflect the new sequence.

---

## Phase 5: Mileage Calculation

### Step 5.1: Route Calculation Using HERE API
For each trip segment:
1. Get detailed route from HERE API
2. Calculate miles in each state traversed
3. Use `cb_2024_us_state_500k.shp` to validate state boundaries

### Step 5.2: Handle Multi-City CA Routes
For reference x.1 trips:
1. Calculate route through all CA cities first
2. Then calculate interstate portion to final destination
3. Sum CA miles from all intrastate segments

---

## Phase 6: Output Generation

### Step 6.1: Create Final Output Format
```python
# Output columns
output_columns = [
    "Company", "Ref No", "Load", "Trip", "Truck", 
    "Trailer", "PU Date F", "Del Date F", "State", "Miles"
]
```

### Step 6.2: Generate State-by-State Records
For each trip, create one record per state traversed:
```
Company: "Ansh Freight"
Ref No: "9.1"
Load: 175029
Trip: 19566
Truck: "1501"
Trailer: "124" 
PU Date F: "5/29/2025"
Del Date F: "6/2/2025"
State: "CA" (then AZ, NM, TX, etc.)
Miles: [Calculated from HERE API]
```

---

## Technical Implementation Details

### Required Libraries
- `pandas` - Data manipulation
- `requests` - HERE API integration
- `geopandas` - Shapefile processing
- `openpyxl` - Excel file handling

### HERE API Integration
- **Endpoint**: Routing API v8
- **Purpose**: Calculate routes and distances between cities
- **Documentation**: https://www.here.com/docs/

### File Dependencies
- `cb_2024_us_state_500k.shp` - US state boundary shapefile
- Associated files: `.cpg`, `.shp.ea.iso.xml`, `.shp.iso.xml`

---

## Quality Assurance

### Data Validation Steps
1. Verify all interstate trips are captured
2. Validate state abbreviations against standard codes
3. Check total miles against known distances
4. Ensure no duplicate state entries per trip

### Output Verification
1. Sum miles by state for quarterly reporting
2. Cross-reference with previous quarters for consistency
3. Validate against truck GPS data if available

---

## Success Metrics
- All company-owned truck trips processed
- Accurate state-by-state mileage calculation
- Properly formatted output for IFTA compliance
- Optimized routes reducing total travel distance 



## Some example of Above steps
After phase 2 we will have lets say 
Load	Trip	TLH Rev	Class	Status	Customer	Cust Ref	Delivered By	Truck	Trailer	Shipper	Ship City	Ship St	Consignee	Cons City	Cons St	Inv No	Inv Date	PU Date F	Del Date F	Load Notes					
175029	19566	2700	LTL	Arrived	BLUEGRACE LOGISTICS	BG958725525	SINGH MANPRIT  	1501	124	MDM PACKAGING COLTON	COLTON	CA	BRANCH 109     	HOUSTON 	TX	175029	5/13/2025 0:00	5/29/2025 0:00	6/2/2025 0:00	
175031	19566	1800	LTL	Arrived	CLH TRANSPORTATION	161893	SINGH MANPRIT  	1501	124	CAMBRO NEW BUILDING    	HUNTINGTON BEACH	CA	LINEA PENNISULAR INC	PANAMA CITY	FL	175031	5/13/2025 0:00	5/30/2025 0:00	6/2/2025 0:00	
175030	19566	1900	LTL	Arrived	CLH TRANSPORTATION	161870	SINGH MANPRIT  	1501	124	QA LOGISTICS	LOS ANGELES	CA	ALDI                               	LOXLEY 	AL	175030	5/13/2025 0:00	5/30/2025 0:00	6/2/2025 0:00	


then phase three will give us something like this 
Load	Trip	TLH Rev	Class	Status	Customer	Cust Ref	Delivered By	Truck	Trailer	Shipper	Ship City	Ship St	Consignee	Cons City	Cons St	Inv No	Inv Date	PU Date F	Del Date F	Load Notes	TEST\	Test 2	Test res    Ref     CA_Cities
175029	19566	2700	LTL	Arrived	BLUEGRACE LOGISTICS	BG958725525	SINGH MANPRIT  	1501	124	MDM PACKAGING COLTON	COLTON	CA	BRANCH 109     	HOUSTON 	TX	175029	5/13/2025 0:00	5/29/2025 0:00	6/2/2025 0:00	9.1     HUNTINGTON BEACH, LOS ANGELES
175031	19566	1800	LTL	Arrived	CLH TRANSPORTATION	161893	SINGH MANPRIT  	1501	124	CAMBRO NEW BUILDING    	HOUSTON	TX	LINEA PENNISULAR INC	PANAMA CITY	FL	175031	5/13/2025 0:00	5/30/2025 0:00	6/2/2025 0:00	9.2
175030	19566	1900	LTL	Arrived	CLH TRANSPORTATION	161870	SINGH MANPRIT  	1501	124	QA LOGISTICS	PANAMA CITY	FL	ALDI                               	LOXLEY 	AL	175030	5/13/2025 0:00	5/30/2025 0:00	6/2/2025 0:00	9.3

Then after phase 4 we will have something like this
ref, orignal_route, changed_start_route, updated_start_route_using_hereAPI, new_ref
9.1, CA → TX,	CA → TX,	CA → TX,	9.1
9.2, CA → FL,	TX → FL,	AL → FL,	9.3
9.3, CA → AL,	FL → AL,	TX → AL,	9.2

After phase 5 and 6 we have something like this
Company	Ref No	Load	Trip	Truck	Trailer	PU Date F	Del Date F	State	Miles
Ansh Freight	9.1	175029	19566	1501	124	5/29/2025 0:00	6/2/2025 0:00	CA	(Miles calaulated from HERE)
Ansh Freight	9.1	175029	19566	1501	124	5/29/2025 0:00	6/2/2025 0:00	AZ	(Miles calaulated from HERE)
Ansh Freight	9.1	175029	19566	1501	124	5/29/2025 0:00	6/2/2025 0:00	NM	(Miles calaulated from HERE)
Ansh Freight	9.1	175029	19566	1501	124	5/29/2025 0:00	6/2/2025 0:00	TX	(Miles calaulated from HERE)
Ansh Freight	9.2	175031	19566	1501	124	5/30/2025 0:00	6/2/2025 0:00	TX	(Miles calaulated from HERE)
Ansh Freight	9.2	175031	19566	1501	124	5/30/2025 0:00	6/2/2025 0:00	LA	(Miles calaulated from HERE)
Ansh Freight	9.2	175031	19566	1501	124	5/30/2025 0:00	6/2/2025 0:00	MS	(Miles calaulated from HERE)
Ansh Freight	9.2	175031	19566	1501	124	5/30/2025 0:00	6/2/2025 0:00	AL	(Miles calaulated from HERE)
Ansh Freight	9.3	175031	19566	1501	124	5/30/2025 0:00	6/2/2025 0:00	FL	(Miles calaulated from HERE)
