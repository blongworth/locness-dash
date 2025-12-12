# LOCNESS Underway Dashboard

A real-time oceanographic data visualization dashboard for ship underway data from LOC-NESS.

## Features

- **Real-time data monitoring** with automatic updates
- **Interactive timeseries plots** for marine chemistry data (pH, density, temperature, salinity)
- **Ship track maps** with color-coded data fields
- **Multiple data sources**: SQLite, Parquet, or DynamoDB
- **Configurable resampling** and time range filtering
- **Correlation analysis** and Bland-Altman plots
- **Theme switching** (light/dark mode)

## Installation & Setup

UV is the easiest way to manage dependencies and run the app.
Using other tools with `pyproj.toml` or `requirements.txt` should also work.

```bash
# Install dependencies
uv sync

# Run development server
uv run python app.py
```

## Configuration

Edit `config.toml` to specify your data source:

```toml
[locness_dash]
# Local file (SQLite or Parquet)
data_path = "./underway_data.db"

# Or use DynamoDB
# dynamodb_table = "locness-underway-summary"
# dynamodb_region = "us-east-1"

update_interval = 10  # seconds
default_resampling = "10s"
```

## Deployment

The app runs at `http://localhost:8050` by default.

For local network deployment (e.g., on a ship),
serve the app with gunicorn or waitress using the provided `run.sh` 
and `run.ps1` scripts.

Configuration files are also provided for deployment to the Railway platform.

Local files or SQLite DB can be used for local deployment, 
A DynamoDB table is recommended for production use with Railway.

## Project Structure

- **`app.py`** - Main Dash application with UI and callbacks
- **`data.py`** - `DataManager` class for multi-source data ingestion
- **`plots.py`** - Plotly visualization functions
- **`config.toml`** - Configuration settings