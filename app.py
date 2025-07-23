import dash
from dash import dcc, html, Input, Output, State
import pandas as pd
import tomllib
from datetime import datetime
import threading
import time

import dash_bootstrap_components as dbc

from data import DataManager
from plots import create_timeseries_plot, create_map_plot


# Load configuration from config.toml
with open("config.toml", "rb") as f:
    toml_config = tomllib.load(f)

# Use the top-level [locness_dash] section in config.toml
config = toml_config.get("locness_dash", {})


# Initialize data manager
data_manager = DataManager(config["db_file_path"])
data_manager.load_initial_data()

# Initialize Dash app
external_stylesheets = [dbc.themes.BOOTSTRAP]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


# Get available fields (excluding timestamp, datetime_utc and id columns)
def get_available_fields():
    if data_manager.data.empty:
        return []
    return [
        col
        for col in data_manager.data.columns
        if col not in ["timestamp", "datetime_utc", "id"]
        and data_manager.data[col].dtype in ["float64", "int64"]
    ]


def get_map_fields():
    if data_manager.data.empty:
        return []
    required_fields = ["latitude", "longitude"]
    available_fields = data_manager.data.columns.tolist()

    if all(field in available_fields for field in required_fields):
        return [
            col
            for col in available_fields
            if col not in ["timestamp", "datetime_utc", "id", "latitude", "longitude"]
            and data_manager.data[col].dtype in ["float64", "int64"]
        ]
    return []



app.layout = html.Div([
    html.H1("LOCNESS Underway Dashboard", style={"textAlign": "center"}),
    html.Div([
        html.Div(
            [
                html.Label("Timeseries Fields:"),
                dcc.Dropdown(
                    id="timeseries-fields-dropdown",
                    options=[],
                    value=["rho_ppb", "ph_corrected_ma"],
                    multi=True,
                    placeholder="Select marine data fields for timeseries",
                ),
                html.Br(),
                html.Label("Map Field:"),
                dcc.Dropdown(
                    id="map-field-dropdown",
                    options=[],
                    value="rho_ppb",
                    placeholder="Select field for ship track visualization",
                ),
                html.Br(),
                html.Label("Resample Interval:"),
                dcc.Dropdown(
                    id="resample-dropdown",
                    options=[
                        {"label": "1 Minute", "value": "1min"},
                        {"label": "10 Minutes", "value": "10min"},
                        {"label": "1 Hour", "value": "1H"},
                    ],
                    value=config["default_resampling"],
                    clearable=False,
                ),
                html.Br(),
                html.Div(id="last-update-display", style={"marginTop": "10px"}),
                html.Div(id="most-recent-timestamp-display", style={"marginTop": "10px"}),
            ],
            style={
                "width": "250px",
                "padding": "20px",
                "background": "#f8f9fa",
                "borderRight": "1px solid #ddd",
                "height": "100%",
            },
        ),
    ], id="sidebar-container", style={"position": "fixed", "top": 60, "left": 0, "zIndex": 1000}),
    html.Div([
        html.Div([dcc.Graph(id="timeseries-plot", style={"height": "500px"})]),
        html.Div([dcc.Graph(id="map-plot", style={"height": "500px"})]),
    ], style={"marginLeft": "270px", "padding": "20px"}),
    dcc.Store(id="last-update-time"),
    dcc.Store(id="time-range-store"),
    dcc.Interval(
        id="interval-component",
        interval=config["update_interval"] * 1000,
        n_intervals=0,
    ),
])


# Callback to update dropdown options and set default values
@app.callback(
    [
        Output("timeseries-fields-dropdown", "options"),
        Output("timeseries-fields-dropdown", "value"),
        Output("map-field-dropdown", "options"),
        Output("map-field-dropdown", "value"),
    ],
    [Input("interval-component", "n_intervals")],
    [
        State("timeseries-fields-dropdown", "value"),
        State("map-field-dropdown", "value"),
    ]
)
def update_dropdown_options(n, ts_value, map_value):
    ts_fields = get_available_fields()
    map_fields = get_map_fields()

    ts_options = [{"label": field, "value": field} for field in ts_fields]
    map_options = [{"label": field, "value": field} for field in map_fields]

    # Set defaults if current value is empty or not in available fields
    ts_default = ["rho_ppb", "ph_corrected_ma"]
    map_default = "rho_ppb"

    # Validate timeseries value
    if not ts_value or not isinstance(ts_value, list) or not any(val in ts_fields for val in ts_value):
        ts_value_out = [val for val in ts_default if val in ts_fields]
    else:
        # Only keep values that are still valid
        ts_value_out = [val for val in ts_value if val in ts_fields]
        if not ts_value_out:
            ts_value_out = [val for val in ts_default if val in ts_fields]

    # Validate map value
    if not map_value or map_value not in map_fields:
        map_value_out = map_default if map_default in map_fields else (map_fields[0] if map_fields else None)
    else:
        map_value_out = map_value

    return ts_options, ts_value_out, map_options, map_value_out


# Callback to update plots
@app.callback(
    [
        Output("timeseries-plot", "figure"),
        Output("map-plot", "figure"),
        Output("time-range-store", "data"),
        Output("last-update-time", "data"),
        Output("last-update-display", "children"),
        Output("most-recent-timestamp-display", "children"),
    ],
    [
        Input("interval-component", "n_intervals"),
        Input("timeseries-fields-dropdown", "value"),
        Input("map-field-dropdown", "value"),
        Input("resample-dropdown", "value"),
    ],
    [
        State("timeseries-plot", "relayoutData"),
        State("last-update-time", "data"),
        State("time-range-store", "data"),
    ],
)
def update_plots(
    n_intervals,
    ts_fields,
    map_field,
    resample_freq,
    relayout_data,
    last_update,
    stored_time_range,
):
    # Get time range from timeseries plot
    time_range = stored_time_range
    if relayout_data and "xaxis.range[0]" in relayout_data:
        time_range = {
            "start": relayout_data["xaxis.range[0]"],
            "end": relayout_data["xaxis.range[1]"],
        }

    # Get data
    start_time = pd.to_datetime(time_range["start"]) if time_range else None
    end_time = pd.to_datetime(time_range["end"]) if time_range else None

    data = data_manager.get_data(start_time, end_time, resample_freq)

    # Create timeseries plot
    ts_fig = create_timeseries_plot(data, ts_fields or [])

    # Create map plot
    map_fig = create_map_plot(data, map_field)

    # Get the most recent timestamp from the data
    most_recent_timestamp = data["timestamp"].max() if not data.empty else None
    most_recent_timestamp_iso = most_recent_timestamp.isoformat() if most_recent_timestamp else "N/A"

    current_time = datetime.now().replace(microsecond=0).isoformat()

    return (
        ts_fig,
        map_fig,
        time_range,
        current_time,
        f"Last Update: {current_time}",
        f"Most Recent Data Timestamp: {most_recent_timestamp_iso}",
    )


# Background thread to check for new data
def background_update():
    while True:
        time.sleep(config["update_interval"])
        new_data = data_manager.get_new_data()
        if not new_data.empty:
            print(f"Retrieved {len(new_data)} new records")


# Start background thread
update_thread = threading.Thread(target=background_update, daemon=True)
update_thread.start()

if __name__ == "__main__":
    app.run(debug=True, port=8050)
