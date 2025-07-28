# IFTA PCS Trips Processing System

## Overview

This system processes truck delivery data to calculate state-by-state mileage for IFTA (International Fuel Tax Agreement) quarterly reporting. The system reads Excel-based trip data, optimizes delivery routes, and uses HERE API with geospatial analysis to determine miles driven in each state.

## 🎯 Key Features

- **Automated Data Processing**: Reads and cleans Excel data from multiple sheets
- **Smart Route Optimization**: Uses HERE API to optimize delivery sequences
- **State Mileage Calculation**: Calculates miles driven in each state using GPS routes
- **Reference Assignment**: Groups and numbers trips logically (9.1, 9.2, 9.3, etc.)
- **CA Cities Consolidation**: Handles multiple California pickups efficiently
- **IFTA-Compliant Output**: Generates properly formatted Excel reports

## 📁 Project Structure

```
.
├── prototype.py              # Main processing system
├── plan.md                   # Detailed implementation plan
├── here.py                   # HERE API test utilities  
├── requirements.txt          # Python dependencies
├── secrets.toml              # API credentials (not in repo)
├── output/                   # Generated output files
├── M-G PCS Trips...xlsx      # Input Excel file
└── cb_2024_us_state_500k.*   # US state boundary shapefiles
```

## 🚀 Quick Start

### Prerequisites

1. **Python 3.8+** with the following packages:
   ```bash
   pip install -r requirements.txt
   ```

2. **HERE API Key**: Get a free key from [HERE Developer Portal](https://developer.here.com/)

3. **Input Files**: 
   - Excel file with trip data
   - US state boundary shapefiles (included)

### Setup

1. **Configure API Key**: Create `secrets.toml` in the project root:
   ```toml
   HERE_API_KEY = "your_api_key_here"
   ```

2. **Verify Input File**: Ensure your Excel file contains these sheets:
   - `Export Research 07-22-2025 ` - Trip data (note trailing space)
   - `Inventory details` - Unit/Company information

### Basic Usage

```bash
python prototype.py
```

The system will:
1. Read and clean your Excel data
2. Filter for company-owned interstate trips
3. Assign reference numbers and optimize routes
4. Calculate state-by-state mileage using HERE API
5. Generate formatted Excel output in `output/` directory

### Expected Output
```
2025-01-23 14:30:15,123 - INFO - Starting IFTA PCS Trips Processing System...
2025-01-23 14:30:15,456 - INFO - HERE API key loaded successfully
2025-01-23 14:30:15,789 - INFO - Phase 1: Reading and cleaning Excel data...
2025-01-23 14:30:16,234 - INFO - Read 1247 rows from Export Research 07-22-2025  sheet
2025-01-23 14:30:16,567 - INFO - Read 458 rows from Inventory details sheet
...
2025-01-23 14:45:23,456 - INFO - ============================================================
2025-01-23 14:45:23,789 - INFO - PROCESSING COMPLETED SUCCESSFULLY!
2025-01-23 14:45:24,012 - INFO - Processed 641 truck trips
2025-01-23 14:45:24,234 - INFO - Generated 2156 state-mile records
2025-01-23 14:45:24,456 - INFO - Output file: output/IFTA_State_Miles_20250123_144524.xlsx
2025-01-23 14:45:24,678 - INFO - ============================================================
```

## 📊 Data Flow

### Phase 1: Data Import & Cleanup
- Reads Excel sheets
- Standardizes data formats
- Cleans truck/trailer identifiers
- Processes dates

### Phase 2: Data Filtering
- Merges inventory data to identify company-owned units
- Drops owner-operators, intrastate trips, and invalid tractors
- Initializes reference system

### Phase 3: Trip Grouping
- Groups loads by Truck + Trip
- Assigns sequential reference numbers (9.1, 9.2, 9.3)
- Consolidates CA cities into first load
- Updates origins for subsequent loads

### Phase 4: Route Optimization
- Uses HERE API to calculate distances between destinations
- Applies nearest-neighbor algorithm for route optimization
- Updates reference numbers based on optimized sequence

### Phase 5: Mileage Calculation
- Gets detailed GPS routes from HERE API
- Intersects routes with state boundaries using GIS
- Calculates miles driven in each state
- Handles multi-city CA routes separately

### Phase 6: Output Generation
- Formats data according to IFTA requirements
- Generates timestamped Excel files
- Sorts by reference number and state

## 📋 Input Data Format

### Trip Data Sheet (`Export Research 07-22-2025 `)
Required columns:
- `Load` - Load number
- `Trip` - Trip identifier  
- `Truck` - Truck number
- `Trailer` - Trailer number
- `Ship City`, `Ship St` - Origin location
- `Cons City`, `Cons St` - Destination location
- `PU Date F`, `Del Date F` - Pickup/delivery dates

### Inventory Sheet (`Inventory details`)
Required columns:
- `Unit` - Unit identifier (matches Truck)
- `Company` - Company name (filled = company-owned, blank = owner-operator)

## 📄 Output Format

Generated Excel file contains these columns:

| Column | Description |
|--------|-------------|
| Company | Company name ("Ansh Freight") |
| Ref No | Trip reference (e.g., "9.1", "9.2") |
| Load | Original load number |
| Trip | Original trip identifier |
| Truck | Truck number |
| Trailer | Trailer number |
| PU Date F | Pickup date (MM/DD/YYYY) |
| Del Date F | Delivery date (MM/DD/YYYY) |
| State | State abbreviation (CA, TX, etc.) |
| Miles | Miles driven in that state |

### Example Output
```
Company      Ref No  Load    Trip   Truck  Trailer  PU Date F    Del Date F   State  Miles
Ansh Freight 9.1     175029  19566  1501   124      05/29/2025   06/02/2025   CA     45.2
Ansh Freight 9.1     175029  19566  1501   124      05/29/2025   06/02/2025   AZ     312.7
Ansh Freight 9.1     175029  19566  1501   124      05/29/2025   06/02/2025   NM     128.9
Ansh Freight 9.1     175029  19566  1501   124      05/29/2025   06/02/2025   TX     89.3
```

## 🔧 Configuration

### Constants (in `prototype.py`)
```python
INPUT_FILE = "M-G PCS Trips PCS A sterling group 2Q 2025 07.23.2025 - AJ.xlsx"
PCS_SHEET = "Export Research 07-22-2025 "  # Note trailing space
INV_SHEET = "Inventory details"
COMPANY_NAME = "Ansh Freight"
```

### API Configuration Examples

**Environment Variable (Linux/Mac)**:
```bash
export HERE_API_KEY="your_api_key_here"
python prototype.py
```

**Environment Variable (Windows)**:
```cmd
set HERE_API_KEY=your_api_key_here
python prototype.py
```

**Using secrets.toml**:
```toml
# secrets.toml
HERE_API_KEY = "your_api_key_here"
```

### API Settings
- **Timeout**: 15 seconds for route calculations
- **Transport Mode**: "truck" (optimized for commercial vehicles)
- **Minimum Miles**: 0.1 miles (filters out insignificant state crossings)

## 💡 Usage Examples

### 1. Running Individual Phases
```python
#!/usr/bin/env python3
from prototype import *

# Initialize
api_key = load_api_key()

# Phase 1: Data Import
pcs, inv = step1_read_excel_data()
print(f"Loaded {len(pcs)} trip records")

# Phase 2: Filtering  
pcs_filtered = step2_filter_fleet_data(pcs, inv)
print(f"After filtering: {len(pcs_filtered)} records")

# Phase 3: Reference Assignment
pcs_with_refs = step3_assign_references(pcs_filtered)
print(f"Assigned references to {len(pcs_with_refs)} records")

# Check reference assignment
ref_counts = pcs_with_refs['Ref'].value_counts()
print(f"Reference distribution:\n{ref_counts.head()}")
```

### 2. Testing Route Optimization
```python
#!/usr/bin/env python3
from prototype import *

# Load data through Phase 3
api_key = load_api_key()
pcs, inv = step1_read_excel_data()
pcs_filtered = step2_filter_fleet_data(pcs, inv)
pcs_with_refs = step3_assign_references(pcs_filtered)

# Test specific truck/trip optimization
test_group = pcs_with_refs[
    (pcs_with_refs['Truck'] == '1501') & 
    (pcs_with_refs['Trip'] == 19566)
].copy()

print("Before optimization:")
for _, row in test_group.iterrows():
    print(f"  {row['Ref']}: {row['Ship City']},{row['Ship St']} → {row['Cons City']},{row['Cons St']}")

# Run optimization
optimized = step4_optimize_routes(test_group, api_key)

print("\nAfter optimization:")
for _, row in optimized.iterrows():
    print(f"  {row['Ref']}: {row['Ship City']},{row['Ship St']} → {row['Cons City']},{row['Cons St']}")
```

### 3. Testing State Mileage Calculation
```python
#!/usr/bin/env python3
from prototype import *

# Setup
api_key = load_api_key()
states_gdf = load_state_boundaries()

# Test single route
origin = "Los Angeles,CA"
destination = "Houston,TX"

state_miles = calculate_state_miles(origin, destination, states_gdf, api_key)
print(f"Route from {origin} to {destination}:")
for state, miles in state_miles.items():
    print(f"  {state}: {miles} miles")

total_miles = sum(state_miles.values())
print(f"Total: {total_miles} miles")
```

### 4. Data Validation
```python
#!/usr/bin/env python3
from prototype import *

# Load and examine data
pcs, inv = step1_read_excel_data()

# Check required columns
required_pcs_cols = ['Load', 'Trip', 'Truck', 'Trailer', 'Ship City', 'Ship St', 'Cons City', 'Cons St', 'PU', 'DEL']
missing_cols = [col for col in required_pcs_cols if col not in pcs.columns]
if missing_cols:
    print(f"Missing PCS columns: {missing_cols}")
else:
    print("✅ All required PCS columns present")

# Check data completeness
print(f"\nData completeness check:")
print(f"Total trips: {len(pcs)}")
print(f"Unique trucks: {pcs['Truck'].nunique()}")
print(f"Date range: {pcs['PU'].min()} to {pcs['PU'].max()}")
print(f"States involved: {sorted(pcs['Ship St'].unique())}")

# Check for problematic data
print(f"\nPotential issues:")
print(f"Missing pickup dates: {pcs['PU'].isna().sum()}")
print(f"Missing delivery dates: {pcs['DEL'].isna().sum()}")
print(f"Same origin/destination: {(pcs['Ship St'] == pcs['Cons St']).sum()}")
```

### 5. Processing Large Datasets
```python
#!/usr/bin/env python3
from prototype import *
import pandas as pd

def process_in_batches(pcs, batch_size=50):
    """Process mileage calculation in batches to avoid API limits"""
    api_key = load_api_key()
    states_gdf = load_state_boundaries()
    
    all_results = []
    total_batches = (len(pcs) + batch_size - 1) // batch_size
    
    for i in range(0, len(pcs), batch_size):
        batch_num = i // batch_size + 1
        batch = pcs.iloc[i:i+batch_size]
        
        print(f"Processing batch {batch_num}/{total_batches} ({len(batch)} records)")
        
        try:
            batch_results = step5_calculate_mileage(batch, states_gdf, api_key)
            all_results.append(batch_results)
            
            # Pause between batches to respect API limits
            if batch_num < total_batches:
                time.sleep(5)
                
        except Exception as e:
            print(f"Error in batch {batch_num}: {e}")
            continue
    
    return pd.concat(all_results, ignore_index=True)

# Usage example
# pcs_optimized = ...  # From previous phases
# output_df = process_in_batches(pcs_optimized, batch_size=25)
```

## 📈 Business Logic

### Reference Number Assignment
- Groups trips by Truck + Trip combination (or Truck + Trailer when Trip is missing)
- Assigns sequential decimal references: 9.1, 9.2, 9.3
- Base number increments for each truck's trip group
- Uses Trailer number as fallback grouping when Trip number is empty

### CA Cities Consolidation
- Collects all CA origins from a trip group
- Stores additional CA cities in `CA_Cities` column of first load (x.1)
- Routes through all CA cities before going interstate

### Route Optimization Example
```
Original:  CA → TX → FL → AL
Optimized: CA → TX → AL → FL  (AL is between TX and FL)
```

The system reorders destinations to minimize total travel distance.

## 🛠️ API Reference

### Configuration & Constants

#### Global Constants
```python
BASE_DIR: Path                    # Project root directory
INPUT_FILE: Path                  # Path to input Excel file
PCS_SHEET: str                    # Name of trip data sheet
INV_SHEET: str                    # Name of inventory sheet
OUTPUT_SHEET: str                 # Name of output template sheet
STATE_SHP: Path                   # Path to state boundaries shapefile
OUTPUT_DIR: Path                  # Output directory for results
SECRETS_FILE: Path                # Path to API credentials file
COMPANY_NAME: str                 # Default company name for output
```

### Core Functions

#### `load_api_key() -> str`
**Purpose**: Load HERE API key from environment or configuration file

**Returns**: `str` - Valid HERE API key

**Raises**: `RuntimeError` - If API key not found in environment or secrets.toml

**Sources** (in priority order):
1. `HERE_API_KEY` environment variable
2. `HERE_API_KEY` or `HERE_KEY` in `secrets.toml`

#### `step1_read_excel_data() -> Tuple[pd.DataFrame, pd.DataFrame]`
**Purpose**: Read and perform initial cleanup of Excel data

**Returns**: `Tuple[pd.DataFrame, pd.DataFrame]` - (trip_data, inventory_data)

**Processing Steps**:
1. Validates input file exists
2. Reads trip data from `PCS_SHEET`
3. Reads inventory data from `INV_SHEET`
4. Performs data cleanup and standardization
5. Converts dates to datetime objects
6. Standardizes string fields (strip, uppercase)

#### `step2_filter_fleet_data(pcs: pd.DataFrame, inv: pd.DataFrame) -> pd.DataFrame`
**Purpose**: Filter trip data for company-owned interstate trips

**Filtering Logic**:
1. **Merge inventory**: Identify company-owned units
2. **Keep company-owned units**: Keep rows where `Company` is NOT null
3. **Drop intrastate**: Remove trips where `Ship St == Cons St`
4. **Keep numeric trucks only**: Remove non-numeric truck identifiers ("OP", "NA", etc.)

#### `step3_assign_references(pcs: pd.DataFrame) -> pd.DataFrame`
**Purpose**: Group related loads and assign sequential reference numbers

**Reference Assignment Logic**:
1. **Group by**: `["Truck", "Trip"]` (or `["Truck", "Trailer"]` when Trip is missing)
2. **Base reference**: Incremental counter (1, 2, 3, ...)
3. **Decimal reference**: Sequential within group (.1, .2, .3)
4. **Final format**: "9.1", "9.2", "9.3"
5. **Fallback grouping**: Uses Trailer number when Trip is empty/null

#### `calculate_distance(origin: str, destination: str, api_key: str) -> float`
**Purpose**: Calculate driving distance between two points using HERE API

**Parameters**:
- `origin`: Origin location as "City,State" format
- `destination`: Destination location as "City,State" format  
- `api_key`: HERE API key

**Returns**: `float` - Distance in miles, or `float('inf')` on error

#### `step4_optimize_routes(pcs: pd.DataFrame, api_key: str) -> pd.DataFrame`
**Purpose**: Optimize delivery sequences using nearest-neighbor algorithm

**Optimization Algorithm**:
1. **Process each truck/trip group separately**
2. **Skip single-load groups** (no optimization needed)
3. **Extract destinations** from all loads in group
4. **Apply nearest-neighbor**: Start from CA, find closest unvisited destination
5. **Update references**: Renumber based on optimized sequence
6. **Update origins**: Chain destinations (9.1→9.2→9.3)

#### `load_state_boundaries() -> gpd.GeoDataFrame`
**Purpose**: Load and prepare state boundary GIS data

**Returns**: `gpd.GeoDataFrame` - State boundaries projected to EPSG:5070

**Processing**:
1. Loads shapefile from `STATE_SHP` path
2. Selects `["STUSPS", "geometry"]` columns
3. Reprojects to NAD83/USA Contiguous (EPSG:5070)

#### `calculate_state_miles(origin: str, destination: str, states_gdf: gpd.GeoDataFrame, api_key: str) -> Dict[str, float]`
**Purpose**: Calculate miles driven in each state for a specific route

**Processing Steps**:
1. **Get route polyline** from HERE API
2. **Decode GPS coordinates** using polyline library
3. **Create LineString geometry** from coordinates
4. **Reproject route** to match state boundaries (EPSG:5070)
5. **Calculate intersections** with each state boundary
6. **Convert lengths** from meters to miles
7. **Filter minimum miles** (>0.1 miles)

#### `step5_calculate_mileage(pcs: pd.DataFrame, states_gdf: gpd.GeoDataFrame, api_key: str) -> pd.DataFrame`
**Purpose**: Calculate state-by-state mileage for all trip segments

**Special Handling for CA Routes** (x.1 references):
1. **Route through CA cities first**: Uses `CA_Cities` column
2. **Calculate intrastate CA miles**: Each city-to-city segment
3. **Calculate interstate miles**: From last CA city to final destination
4. **Combine results**: CA miles + interstate miles

#### `step6_generate_output(output_df: pd.DataFrame) -> str`
**Purpose**: Format and export final IFTA-compliant Excel report

**Processing Steps**:
1. **Create output directory** if it doesn't exist
2. **Select and order columns** according to IFTA requirements
3. **Format dates** as MM/DD/YYYY strings
4. **Sort records** by reference number and state
5. **Generate timestamped filename**
6. **Export to Excel** with proper sheet name

### Data Structures

#### Input DataFrames

**Trip Data (PCS) Columns**:
```python
{
    'Load': int,           # Load number
    'Trip': int,           # Trip identifier  
    'Truck': str,          # Truck number (cleaned)
    'Trailer': str,        # Trailer number (cleaned)
    'Ship City': str,      # Origin city
    'Ship St': str,        # Origin state (uppercase)
    'Cons City': str,      # Destination city
    'Cons St': str,        # Destination state (uppercase)
    'PU': datetime,        # Pickup date
    'DEL': datetime,       # Delivery date
    'Ref': str,            # Reference number (e.g., "9.1")
    'CA_Cities': str       # Additional CA cities (comma-separated)
}
```

**Inventory Data Columns**:
```python
{
    'Unit': str,           # Unit identifier (matches Truck)
    'Company': str         # Company name (filled = company-owned, NaN = owner-operator)
}
```

#### Output DataFrame Columns
```python
{
    'Company': str,        # "Ansh Freight"
    'Ref No': str,         # "9.1", "9.2", etc.
    'Load': int,           # Original load number
    'Trip': int,           # Original trip identifier
    'Truck': str,          # Truck number
    'Trailer': str,        # Trailer number
    'PU Date F': str,      # Pickup date (MM/DD/YYYY)
    'Del Date F': str,     # Delivery date (MM/DD/YYYY)
    'State': str,          # State abbreviation
    'Miles': float         # Miles driven in state
}
```

## 🚨 Error Handling & Troubleshooting

### Common Issues & Solutions

#### File Not Found
```
FileNotFoundError: Input file not found
```
**Solution**: 
- Verify Excel file exists in project directory
- Check filename matches `INPUT_FILE` constant

#### API Key Missing
```
RuntimeError: HERE_API_KEY not found
```
**Solution**:
- Create `secrets.toml` with valid HERE API key
- Or set `HERE_API_KEY` environment variable

#### Sheet Not Found
```
Error reading PCS sheet: Worksheet 'Export Research 07-22-2025' does not exist
```
**Solution**:
- Verify sheet names in Excel file (note trailing space in sheet name)
- Update `PCS_SHEET` constant if needed

#### No Data After Filtering
```
Phase 2 completed: Filtered from 1000 to 0 rows
```
**Solution**:
- Check if inventory data correctly identifies company-owned units
- Verify interstate trips exist (Ship St ≠ Cons St)

### Exception Types and Handling

#### File System Errors
```python
FileNotFoundError: 
    # Input Excel file or shapefile missing
    # Check file paths and existence

PermissionError:
    # Cannot write to output directory
    # Check folder permissions
```

#### Data Processing Errors
```python
KeyError:
    # Missing required columns in Excel sheets
    # Verify sheet structure matches expectations

ValueError:
    # Data type conversion failures
    # Check data format and content
```

#### API Errors  
```python
requests.exceptions.RequestException:
    # HERE API connection/authentication issues
    # Verify API key and network connectivity

requests.exceptions.Timeout:
    # API request timeout (10-15 seconds)
    # Automatic retry or graceful degradation
```

#### GIS Processing Errors
```python
gpd.errors.CRSError:
    # Coordinate system projection issues
    # Verify shapefile CRS compatibility

shapely.errors.GEOSException:
    # Geometry intersection calculation errors
    # Skip problematic geometries, log warnings
```

### Error Recovery Strategies

1. **API Failures**: Return `float('inf')` for distances, empty dict for state miles
2. **Missing Data**: Skip problematic records, log warnings
3. **File Issues**: Provide clear error messages with solution steps
4. **Geometry Errors**: Continue processing, log warnings for manual review

### Robust API Call Example
```python
#!/usr/bin/env python3
from prototype import *
import time

def robust_api_call(origin, destination, api_key, max_retries=3):
    """API call with retry logic"""
    for attempt in range(max_retries):
        try:
            distance = calculate_distance(origin, destination, api_key)
            if distance != float('inf'):
                return distance
            else:
                print(f"API returned error, attempt {attempt + 1}")
        except Exception as e:
            print(f"API error on attempt {attempt + 1}: {e}")
        
        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)  # Exponential backoff
    
    return float('inf')

# Test usage
api_key = load_api_key()
distance = robust_api_call("Los Angeles,CA", "Houston,TX", api_key)
print(f"Distance: {distance} miles")
```

## 🔍 Logging & Monitoring

The system provides detailed logging:
- Phase completion status
- Row counts after each filter
- Route optimization progress
- API call status
- Final output summary

### Log Levels
- **INFO**: Normal processing updates
- **WARNING**: Non-critical issues (API timeouts)
- **ERROR**: Critical failures requiring attention

### Logging Configuration
```python
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
```

## ⚡ Performance Considerations

### API Rate Limiting
- HERE API calls are sequential (not parallelized)
- 10-15 second timeouts prevent hanging
- Progress logging for long-running operations

### Memory Usage
- DataFrames processed in-memory (suitable for typical IFTA datasets)
- GIS operations use efficient libraries (geopandas, shapely)
- Temporary geometries cleaned up automatically

### Processing Time Estimates
- **Phase 1-3**: < 1 minute (local processing)
- **Phase 4**: 1-5 minutes (API calls for optimization)
- **Phase 5**: 5-30 minutes (API calls for mileage calculation)
- **Phase 6**: < 1 minute (local processing)

**Total**: 10-40 minutes depending on trip count and API response times

### Memory Usage Monitoring
```python
#!/usr/bin/env python3
import psutil
import os
from prototype import *

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

# Monitor memory during processing
print(f"Initial memory: {get_memory_usage():.1f} MB")

# Phase 1
pcs, inv = step1_read_excel_data()
print(f"After Phase 1: {get_memory_usage():.1f} MB")

# Phase 2
pcs_filtered = step2_filter_fleet_data(pcs, inv)
print(f"After Phase 2: {get_memory_usage():.1f} MB")

# Clean up large objects if needed
del pcs, inv
print(f"After cleanup: {get_memory_usage():.1f} MB")
```

## 📚 Advanced Scenarios

### Scenario 1: Processing Monthly Data
```python
#!/usr/bin/env python3
from prototype import *
import pandas as pd

def filter_by_month(pcs, year, month):
    """Filter trips by specific month"""
    mask = (pcs['PU'].dt.year == year) & (pcs['PU'].dt.month == month)
    return pcs[mask].copy()

# Process only June 2025 data
pcs, inv = step1_read_excel_data()
pcs_filtered = step2_filter_fleet_data(pcs, inv)

june_data = filter_by_month(pcs_filtered, 2025, 6)
print(f"June 2025 trips: {len(june_data)}")

if len(june_data) > 0:
    # Continue with normal processing
    pcs_with_refs = step3_assign_references(june_data)
    # ... rest of processing
```

### Scenario 2: Specific Truck Analysis
```python
#!/usr/bin/env python3
from prototype import *

def analyze_truck(truck_number):
    """Analyze trips for a specific truck"""
    pcs, inv = step1_read_excel_data()
    pcs_filtered = step2_filter_fleet_data(pcs, inv)
    
    truck_data = pcs_filtered[pcs_filtered['Truck'] == truck_number].copy()
    
    if len(truck_data) == 0:
        print(f"No data found for truck {truck_number}")
        return
    
    print(f"Analysis for Truck {truck_number}:")
    print(f"Total trips: {len(truck_data)}")
    print(f"Unique trip IDs: {truck_data['Trip'].nunique()}")
    print(f"Date range: {truck_data['PU'].min()} to {truck_data['PU'].max()}")
    print(f"States visited: {sorted(truck_data['Cons St'].unique())}")
    
    # Process this truck's data
    pcs_with_refs = step3_assign_references(truck_data)
    print(f"Reference assignments: {sorted(pcs_with_refs['Ref'].unique())}")
    
    return pcs_with_refs

# Usage
truck_analysis = analyze_truck('1501')
```

### Scenario 3: State-Specific Reporting
```python
#!/usr/bin/env python3
from prototype import *
import pandas as pd

def generate_state_summary(output_file):
    """Generate summary by state from processed output"""
    df = pd.read_excel(output_file)
    
    state_summary = df.groupby('State').agg({
        'Miles': ['sum', 'count', 'mean'],
        'Ref No': 'nunique'
    }).round(1)
    
    state_summary.columns = ['Total Miles', 'Trip Count', 'Avg Miles', 'Unique Refs']
    state_summary = state_summary.sort_values('Total Miles', ascending=False)
    
    print("State Summary Report:")
    print("=" * 60)
    print(state_summary)
    
    # Save to file
    summary_file = output_file.replace('.xlsx', '_state_summary.xlsx')
    state_summary.to_excel(summary_file)
    print(f"\nSummary saved to: {summary_file}")
    
    return state_summary

# Usage after processing
# output_file = "output/IFTA_State_Miles_20250123_144524.xlsx"
# summary = generate_state_summary(output_file)
```

## 🧪 Testing & Validation

### Data Validation
- Compares total miles against known distances
- Validates state abbreviations
- Checks for duplicate state entries per trip
- Ensures all interstate trips are captured

### Quality Checks
- Cross-reference with previous quarters
- Validate against truck GPS data
- Review optimized routes for reasonableness

## 💡 Tips and Best Practices

### Before Running
- Verify Excel file exists and has correct sheet names
- Test API key with a simple HERE API call
- Check that output directory is writable
- Review input data for obvious issues

### During Processing
- Monitor memory usage for large datasets
- Watch for API timeout errors
- Check log output for warnings
- Ensure stable internet connection

### After Processing
- Validate output data completeness
- Compare total miles against expectations
- Review state distributions for reasonableness
- Archive input and output files

### Troubleshooting
- Check logs for specific error messages
- Verify API key is valid and has quota remaining
- Test with a small subset of data first
- Contact HERE support for persistent API issues

## 🤝 Contributing

### Development Setup
1. Clone repository
2. Install dependencies: `pip install -r requirements.txt`
3. Configure API key in `secrets.toml`
4. Run tests: `python -m pytest` (when available)

### Code Style
- Follow PEP 8 guidelines
- Use type hints for function parameters
- Add docstrings for all functions
- Include error handling for external API calls

## 📄 License

This project is proprietary software for IFTA compliance reporting.

## 📞 Support

For technical issues or questions:
1. Check the error logs for specific error messages
2. Verify API key and input file format
3. Review the troubleshooting section above
4. Contact system administrator for assistance

---

**Last Updated**: January 2025  
**Version**: 1.0.0  
**Python Version**: 3.8+ 