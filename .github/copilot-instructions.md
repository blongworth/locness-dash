# LOCNESS Underway Dashboard - AI Coding Agent Instructions

## Project Overview

This is a real-time oceanographic data dashboard built with Dash/Plotly for visualizing LOCNESS (Lab-on-Chip-for-Ocean-Chemistry at Near-realtime Environmental Sample Sensing) ship underway data. The dashboard displays marine chemistry data (pH, density, temperature, salinity) with interactive timeseries plots and ship track maps.

## Architecture

### Core Components
- **`app.py`**: Main Dash application with UI layout and callbacks  
- **`data.py`**: `DataManager` class handling multi-source data ingestion
- **`plots.py`**: Plotly visualization functions (timeseries, maps, correlations)
- **`config.toml`**: Configuration for data sources and app settings

### Data Sources (Multi-Backend Pattern)
The `DataManager` supports three data sources configured via `config.toml`:
1. **SQLite** - `data_path = "file.db"` (Unix timestamps)
2. **Parquet** - `data_path = "file.parquet"` (Unix timestamps)  
3. **DynamoDB** - `dynamodb_table = "table-name"` (ISO string timestamps)

Key pattern: Timestamp normalization happens in `DataManager` - all sources converted to pandas datetime.

### Real-Time Data Flow
1. Background thread (`background_update()`) polls for new data every `update_interval` seconds
2. `DataManager.get_new_data()` queries since `last_datetime_utc`
3. Dash interval component triggers UI updates via callbacks
4. `update_plots()` callback handles time filtering, resampling, and plot generation

## Development Patterns

### Configuration Management
Always use `config.toml` for settings. Load via:
```python
with open("config.toml", "rb") as f:
    config = tomllib.load(f)["locness_dash"]
```

### Logging Convention
Use consistent logger naming:
```python
logger = logging.getLogger("locness_dash.module_name")
```

### Dash Callback Patterns
- **Auto-update mode**: Use data-dependent `uirevision` for plot updates
- **Fixed mode**: Use constant `uirevision` to preserve user interactions
- Time range logic: Auto-update moves end time to latest, preserves user's start time

### Data Type Handling (DynamoDB)
DynamoDB returns Decimal objects - convert in `_ensure_proper_dtypes()`:
```python
if isinstance(sample_val, Decimal):
    df[col] = df[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
```

### Resampling Logic
Prevent upsampling - only downsample when requested frequency > data frequency:
```python
if requested_seconds > median_seconds:
    data.set_index("datetime_utc", inplace=True)
    resampled_data = data.resample(resample_freq).mean()
```

## Key Files for Understanding

- **`data.py`**: Study `DataManager.__init__()` and `get_data()` for multi-source pattern
- **`app.py`**: Lines 500-670 show complex time range and auto-update logic
- **`plots.py`**: `create_map_plot()` demonstrates quantile-based color scaling for skewed data
- **Debug scripts**: `debug_*.py` and `test_*.py` show testing patterns
- **Sample data**: `sample_data_setup.py` creates realistic test data

## Development Workflow

### Package Management with uv
This project uses `uv` for fast Python package management:
```bash
# Install dependencies (creates .venv automatically)
uv sync

# Add new dependencies
uv add package-name

# Run Python scripts with uv
uv run python script.py
```

### Local Development
```bash
# Install dependencies
uv sync

# Create sample data for testing
uv run python sample_data_setup.py

# Run development server
uv run python app.py  # Uses debug=True, port from $PORT or 8050
```

### Production Deployment
```bash
# Railway/Render deployment
gunicorn app:server --bind 0.0.0.0:$PORT

# Local production test
./run.sh  # Uses gunicorn with 2 workers
```

### Testing Data Sources
```bash
# Test SQLite data
uv run python sqlite_summary.py path/to/data.db

# Test Parquet data  
uv run python parquet_summary.py path/to/data.parquet

# Test DynamoDB connection
uv run python debug_dynamodb.py
```

## Common Tasks

### Adding New Plot Types
1. Create function in `plots.py` following existing patterns
2. Add to callback in `app.py` with proper `uirevision` handling
3. Include template parameter for theme switching

### Adding New Data Fields
1. Ensure field appears in numeric columns filter: `data_manager.data[col].dtype in ["float64", "int64"]`
2. Update `get_available_fields()` exclude list if needed
3. For map fields, verify not in location field excludes

### Debugging Data Issues
Use the debug scripts with uv:
- `uv run python debug_dynamodb_fields.py` - Check DynamoDB field types
- `uv run python debug_resampling.py` - Test resampling logic
- `uv run python test_dynamodb_datatypes.py` - Full DynamoDB integration test

### Marine Chemistry Context
- pH fields: `ph_corrected`, `ph_total` with moving averages (`*_ma`)
- Density: `rho_ppb` (parts per billion)
- Core fields for ship track: `latitude`, `longitude`, `datetime_utc`
- Dispersal view shows pH and rho with current values in cards

Remember: This handles real oceanographic data with specific ranges (pH ~8.1, salinity ~34.5 PSU, temperature varies with latitude). Visualization should account for realistic data distributions and ranges.
