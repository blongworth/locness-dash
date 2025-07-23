import dash
from dash import dcc, html, Input, Output, State, callback_context
import pandas as pd
import tomllib
from datetime import datetime
import threading
import time

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
app = dash.Dash(__name__)


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


# App layout
app.layout = html.Div(
    [
        html.H1("Ship Underway Data Dashboard", style={"textAlign": "center"}),
        # Controls
        html.Div(
            [
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
                    ],
                    style={
                        "width": "30%",
                        "display": "inline-block",
                        "paddingRight": "10px",
                    },
                ),
                html.Div(
                    [
                        html.Label("Map Field:"),
                        dcc.Dropdown(
                            id="map-field-dropdown",
                            options=[],
                            value="rho_ppb",
                            placeholder="Select field for ship track visualization",
                        ),
                    ],
                    style={
                        "width": "30%",
                        "display": "inline-block",
                        "paddingRight": "10px",
                    },
                ),
                html.Div(
                    [
                        html.Label("Resample Interval:"),
                        dcc.Dropdown(
                            id="resample-dropdown",
                            options=[
                                #{"label": "No Resampling", "value": None},
                                {"label": "1 Minute", "value": "1min"},
                                {"label": "10 Minutes", "value": "10min"},
                                {"label": "1 Hour", "value": "1H"},
                            ],
                            value=config["default_resampling"],
                            clearable=False,
                        ),
                    ],
                    style={"width": "30%", "display": "inline-block", "paddingRight": "10px"},
                ),
            ],
            style={"padding": "20px"},
        ),
        # Plots
        html.Div([dcc.Graph(id="timeseries-plot", style={"height": "500px"})]),
        html.Div([dcc.Graph(id="map-plot", style={"height": "500px"})]),
        # Store components for data management
        dcc.Store(id="last-update-time"),
        dcc.Store(id="time-range-store"),
        # Interval component for periodic updates
        dcc.Interval(
            id="interval-component",
            interval=config["update_interval"] * 1000,  # Convert to milliseconds
            n_intervals=0,
        ),
    ]
)


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
    ctx = callback_context


    # Check if this is a new data update or just a UI change
    is_data_update = "interval-component" in [
        p["prop_id"].split(".")[0] for p in ctx.triggered
    ]

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

    # Preserve zoom/pan for timeseries plot (all subplots)
    if relayout_data and is_data_update:
        # Find all xaxis ranges in relayoutData
        axis_updates = {}
        for key in relayout_data:
            if key.startswith("xaxis") and key.endswith(".range[0]"):
                axis_prefix = key[:-len(".range[0]")]
                range0 = relayout_data.get(f"{axis_prefix}.range[0]")
                range1 = relayout_data.get(f"{axis_prefix}.range[1]")
                if range0 is not None and range1 is not None:
                    axis_updates[axis_prefix] = {"range": [range0, range1]}
        if axis_updates:
            ts_fig.update_layout(**axis_updates)

    # Create map plot
    map_fig = create_map_plot(data, map_field)

    # Preserve zoom/pan for map plot
    if relayout_data and is_data_update:
        mapbox_update = {}
        if "mapbox.center" in relayout_data:
            mapbox_update["center"] = relayout_data["mapbox.center"]
        if "mapbox.zoom" in relayout_data:
            mapbox_update["zoom"] = relayout_data["mapbox.zoom"]
        if mapbox_update:
            # Merge with existing mapbox layout (preserve style, etc.)
            current_mapbox = map_fig.layout.mapbox.to_plotly_json() if hasattr(map_fig.layout, "mapbox") and map_fig.layout.mapbox else {}
            current_mapbox.update(mapbox_update)
            map_fig.update_layout(mapbox=current_mapbox)

    current_time = datetime.now().isoformat()

    return ts_fig, map_fig, time_range, current_time


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
