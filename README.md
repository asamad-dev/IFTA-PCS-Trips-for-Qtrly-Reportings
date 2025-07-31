# IFTA PCS Trips Processing System

## Overview
Processes truck delivery data to calculate state-by-state mileage for IFTA quarterly reporting. Uses HERE API and geospatial analysis to determine miles driven in each state.

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure API Key
Create `secrets.toml` in project root:
```toml
HERE_API_KEY = "your_here_api_key_here"
```
Get free API key from [HERE Developer Portal](https://developer.here.com/)

### 3. Prepare Input File
Ensure Excel file contains these sheets:
- `Export Research 07-22-2025 ` (note trailing space)
- `Inventory details`

### 4. Run Processing
```bash
python prototype.py
```

## What It Does

1. **Reads Excel data** from trip and inventory sheets
2. **Filters for company-owned interstate trips** 
3. **Groups and assigns reference numbers** (9.1, 9.2, 9.3)
4. **Optimizes delivery routes** using HERE API
5. **Calculates state-by-state mileage** via GPS routing
6. **Generates IFTA-compliant Excel output**

## Processing Phases

**Data Preparation**
- Reads Excel sheets (`Export Research 07-22-2025`, `Inventory details`) 
- Merges inventory to identify company-owned units
- Filters for interstate trips only (`Ship St ≠ Cons St`)
- Drops owner-operators and invalid truck numbers (OP and NA)
- Standardizes data formats, cleans truck/trailer IDs, processes dates

**Trip Grouping & Reference Assignment** ⭐
- **Grouping Logic**: Groups loads by `Truck + Trip` (or `Truck + Trailer` when Trip missing)
- **Reference System**: Assigns sequential references within each group (9.1, 9.2, 9.3)
- **CA Consolidation**: Collects all CA destinations from a trip group into first load (.1)
- **Route Chaining**: Updates subsequent loads to start from previous destination
- **Example**: 3 loads from CA→TX, CA→FL, CA→AL become: CA→TX (9.1), TX→AL (9.2), AL→FL (9.3)

**Route Optimization** ⭐  
- **Distance Calculation**: Uses HERE API to get actual driving distances between all destinations
- **Optimization Algorithm**: Applies nearest-neighbor logic starting from California
- **Geographic Logic**: Reorders stops to minimize total travel (e.g., CA→TX→AL→FL instead of CA→TX→FL→AL)
- **Reference Updates**: Renumbers trips based on optimized sequence
- **Result**: Geographically logical routes that reduce total mileage

**Mileage Calculation & Output**
- Gets detailed GPS polylines from HERE API for each optimized route
- Intersects route polylines with US state boundaries using GIS
- Calculates miles driven in each state (with fallback for API failures)
- Generates IFTA-compliant Excel/CSV output with ERROR tracking

## Expected Output

**Files Generated:**
- `output/IFTA_State_Miles_[timestamp].xlsx` - Main IFTA report  
- `output/IFTA_State_Miles_[timestamp].csv` - CSV version
- `debug/phase[1-6]_*.csv` - Debug files for each processing phase

**Output Format:**
| Company | Ref No | Load | Trip | Truck | Trailer | PU Date F | Del Date F | State | Miles |
|---------|--------|------|------|-------|---------|-----------|------------|-------|-------|
| Ansh Freight | 9.1 | 175029 | 19566 | 1501 | 124 | 05/29/2025 | 06/02/2025 | CA | 45.2 |
| Ansh Freight | 9.1 | 175029 | 19566 | 1501 | 124 | 05/29/2025 | 06/02/2025 | AZ | 312.7 |

## Error Tracking

Routes that fail mileage calculation get ERROR records:
```csv
Chase Carrier Inc,174.4,174565,19444,1552,224,04/29/2025,05/02/2025,ERROR,CALCULATION_FAILED
```

Filter out ERROR records for valid data: `State != 'ERROR'`

## Troubleshooting

**Common Issues:**

| Issue | Solution |
|-------|----------|
| `FileNotFoundError` | Check Excel file exists and path is correct |
| `HERE_API_KEY not found` | Create `secrets.toml` with valid API key |
| `Worksheet not found` | Verify sheet names (note trailing space) |
| `No data after filtering` | Check inventory sheet marks company units correctly |
| `API timeout errors` | Retry processing - system has built-in error handling |

**Log Monitoring:**
- Processing shows progress through 6 phases
- Success rate displayed (typically >98%)
- ERROR records tracked and reported

## Project Files

```
├── prototype.py              # Main processing system
├── requirements.txt          # Python dependencies  
├── secrets.toml             # API key (create this)
├── M-G PCS Trips...xlsx     # Input Excel file
├── cb_2024_us_state_500k.*  # US state shapefiles
├── output/                  # Generated reports
└── debug/                   # Debug CSV files
```

---
**Processing Time:** 10-40 minutes depending on trip count  
**Success Rate:** Typically >98% with ERROR tracking for failures 