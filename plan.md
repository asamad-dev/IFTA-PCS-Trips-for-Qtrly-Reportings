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
- **Round Trip Pattern**: Trucks start in CA, deliver loads to other states, then return to CA
- **Trip Definition**: A complete trip consists of outbound (CA→X) + return (X→CA) legs
- **Chronological Order**: Loads must maintain exact PU Date sequence
- **Virtual Returns**: AZ/NV loads without CA returns need virtual empty legs
- Calculate miles driven in each state for IFTA compliance

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

## Phase 3: Round Trip Detection and Reference Assignment

### Step 3.1: Detect Round Trip Patterns
**Business Rule**: Group loads by Truck+Trailer in chronological order (PU Date F)
- **Round Trip**: CA→X followed by X→CA (same state destinations)
- **Date Gap Filter**: Max 3 days between Del Date F and next PU Date F
- **Sequential References**: Each complete round trip gets new integer (16.1, 16.2, then 17.1, 17.2)

### Step 3.2: Assign Reference Numbers
**Logic**: 
- Maintain chronological order (no optimization)
- Each load keeps its own decimal reference
- No CA destination consolidation

**Example Transformation**:
```
Before:
Load 174418: CITY OF INDUSTRY, CA → MT STERLING, KY (04/11)
Load 174520: OWENSBORO, KY → ONTARIO, CA (04/25) 
Load 174861: RIVERSIDE, CA → SAPULPA, OK (05/16)
Load 174899: DURANT, OK → EL MIRAGE, AZ (05/21)

After:
Load 174418: CITY OF INDUSTRY, CA → MT STERLING, KY (Ref: 16.1)
Load 174520: OWENSBORO, KY → ONTARIO, CA (Ref: 16.2)
Load 174861: RIVERSIDE, CA → SAPULPA, OK (Ref: 17.1) 
Load 174899: DURANT, OK → EL MIRAGE, AZ (Ref: 17.2)
Virtual: EL MIRAGE, AZ → SAN BERNARDINO, CA (Ref: 17.3) [Empty return]
```

---

## Phase 4: Virtual Return Leg Processing

### Step 4.1: Identify Incomplete Trips
**Rule**: Find loads that end in AZ/NV without subsequent CA delivery
- Check if load ends in AZ or NV
- Look ahead for future CA delivery by same truck/trailer
- If no CA return within reasonable timeframe, create virtual return

### Step 4.2: Generate Virtual Return Legs
```python
def add_virtual_return_leg(last_load):
    """
    Input: Load ending in AZ/NV without CA return
    Output: Virtual empty leg back to CA
    
    Example:
    Last Load: DURANT, OK → EL MIRAGE, AZ (Ref: 17.2)
    Virtual:   EL MIRAGE, AZ → SAN BERNARDINO, CA (Ref: 17.3) [Empty]
    """
    virtual_leg = {
        'Load': f"VIRTUAL_{last_load['Load']}",
        'Ship_City': last_load['Cons_City'], 
        'Ship_St': last_load['Cons_St'],
        'Cons_City': 'SAN BERNARDINO',
        'Cons_St': 'CA',
        'Note': f'Empty return ({last_load["Cons_St"]}→CA)'
    }
```

### Step 4.3: Maintain Chronological Order
**Critical**: Do NOT reorder loads - maintain exact PU Date F sequence for ELD compliance

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

---

## CORRECTED EXAMPLE - Round Trip Processing

### Real Data Example (Truck 1501, Trailer 258):
**After Phase 2** (filtered data):
```
Load 174418: CITY OF INDUSTRY, CA → MT STERLING, KY (04/11/2025)
Load 174520: OWENSBORO, KY → ONTARIO, CA (04/25/2025) 
Load 174861: RIVERSIDE, CA → SAPULPA, OK (05/16/2025)
Load 174899: DURANT, OK → EL MIRAGE, AZ (05/21/2025)
```

**After Phase 3** (round trip detection):
```
Load 174418: CITY OF INDUSTRY, CA → MT STERLING, KY (Ref: 16.1) [Round Trip 16 - Outbound]
Load 174520: OWENSBORO, KY → ONTARIO, CA (Ref: 16.2) [Round Trip 16 - Return]  
Load 174861: RIVERSIDE, CA → SAPULPA, OK (Ref: 17.1) [Round Trip 17 - Outbound]
Load 174899: DURANT, OK → EL MIRAGE, AZ (Ref: 17.2) [Round Trip 17 - Incomplete]
```

**After Phase 4** (virtual return legs):
```
Load 174418: CITY OF INDUSTRY, CA → MT STERLING, KY (Ref: 16.1)
Load 174520: OWENSBORO, KY → ONTARIO, CA (Ref: 16.2)
Load 174861: RIVERSIDE, CA → SAPULPA, OK (Ref: 17.1)  
Load 174899: DURANT, OK → EL MIRAGE, AZ (Ref: 17.2)
Virtual: EL MIRAGE, AZ → SAN BERNARDINO, CA (Ref: 17.3) [Empty return AZ→CA]
```

**Final Output** (each load produces state-by-state records):
```
Company: Ansh Freight, Ref No: 16.1, Load: 174418, State: CA, Miles: XX
Company: Ansh Freight, Ref No: 16.1, Load: 174418, State: NV, Miles: XX  
Company: Ansh Freight, Ref No: 16.1, Load: 174418, State: UT, Miles: XX
Company: Ansh Freight, Ref No: 16.1, Load: 174418, State: CO, Miles: XX
Company: Ansh Freight, Ref No: 16.1, Load: 174418, State: KY, Miles: XX
Company: Ansh Freight, Ref No: 16.2, Load: 174520, State: KY, Miles: XX
Company: Ansh Freight, Ref No: 16.2, Load: 174520, State: CA, Miles: XX
[... and so on for all loads including virtual returns ...]
``` 



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
