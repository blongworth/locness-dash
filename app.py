import tomllib
from datetime import datetime
import threading
import time
import pandas as pd

import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc

from data import DataManager
from plots import create_timeseries_plot, create_map_plot, create_dispersal_plot

# TODO: Plot selection bug: selecting on plot filters data, can't zoom out
# TODO: Fix "jump" on data update
# TODO: Add data to traces rather than redrawing entire plot
# TODO: Dispersal View
# TODO: Diagnostics View
# TODO: Property plot view

# Load configuration from config.toml
with open("config.toml", "rb") as f:
    toml_config = tomllib.load(f)

# Use the top-level [locness_dash] section in config.toml
config = toml_config.get("locness_dash", {})


# Initialize data manager
data_manager = DataManager(config["file_path"])
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
                        {"label": "1 Hour", "value": "1h"},
                    ],
                    value=config["default_resampling"],
                    clearable=False,
                ),
                html.Br(),
                html.Label("Time Range:"),
                dcc.RangeSlider(
                    id="time-range-slider",
                    min=0,
                    max=1,
                    step=1,
                    value=[0, 1],
                    marks={},
                    tooltip={"placement": "bottom", "always_visible": False},
                    allowCross=False,
                ),
                html.Br(),
                html.Div([
                    html.Label("Last update:"),
                    html.P(id="last-update-display"),
                    html.Label("Last timestamp:"),
                    html.P(id="most-recent-timestamp-display"),
                    html.Label("Total rows (all data):"),
                    html.P(id="total-rows-all-data"),
                    html.Label("Total rows (filtered):"),
                    html.P(id="total-rows-filtered"),
                ])
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
        dcc.Tabs([
            dcc.Tab(label="Main View", children=[
                html.Div([
                    html.Div([dcc.Graph(id="map-plot", style={"height": "500px"})]),
                    html.Div([dcc.Graph(id="timeseries-plot", style={"height": "500px"})]),
                ], style={"marginLeft": "270px", "padding": "10px"}),
            ]),
            dcc.Tab(label="Dispersal View", children=[
                html.Div([
                    html.Div([
                        html.Div(
                            style={"display": "flex", "alignItems": "center", "height": "500px"},
                            children=[
                                dcc.Graph(id="timeseries-plot-dispersal", 
                                          style={"height": "500px", "width": "75%", "flex": "0 0 75%"}),
                                html.Div(
                                    id="ph-value-box",
                                    style={
                                        "height": "200px",
                                        "width": "20%",
                                        "textAlign": "center",
                                        "padding": "20px",
                                        "border": "1px solid #ddd",
                                        "background": "#f8f9fa",
                                        "marginLeft": "20px",
                                        "flex": "0 0 20%",
                                    },
                                    children=[
                                        html.Div("pH (2min avg)", style={"fontSize": "16px", "marginBottom": "15px"}),
                                        html.Div("No Data", id="ph-value", style={"fontSize": "36px", "color": "black", "fontWeight": "bold"}),
                                    ],
                                ),
                            ]
                        )
                    ]),
                    html.Div([dcc.Graph(id="map-plot-dispersal", style={"height": "500px"})]),
                ], style={"marginLeft": "270px", "padding": "10px"}),
            ]),
        ]),
    ]),
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

# Callback to update time range slider properties
@app.callback(
    [
        Output("time-range-slider", "min"),
        Output("time-range-slider", "max"),
        Output("time-range-slider", "marks"),
        Output("time-range-slider", "value"),
    ],
    [Input("interval-component", "n_intervals")],
    [State("time-range-slider", "value")]
)
def update_time_slider(n, current_value):
    if not data_manager.data.empty and "timestamp" in data_manager.data.columns:
        timestamps = pd.to_datetime(data_manager.data["timestamp"])
        min_ts = timestamps.min()
        max_ts = timestamps.max()
        slider_min = int(min_ts.timestamp())
        slider_max = int(max_ts.timestamp())
        marks = {
            slider_min: min_ts.strftime("%Y-%m-%d %H:%M"),
            slider_max: max_ts.strftime("%Y-%m-%d %H:%M"),
        }
        # If current_value is valid, preserve it
        if (
            isinstance(current_value, list)
            and len(current_value) == 2
            and slider_min <= current_value[0] <= slider_max
            and slider_min <= current_value[1] <= slider_max
        ):
            slider_value = current_value
        else:
            slider_value = [slider_min, slider_max]
    else:
        slider_min = 0
        slider_max = 1
        marks = {}
        slider_value = [0, 1]
    return slider_min, slider_max, marks, slider_value


# Callback to update plots
@app.callback(
    [
        Output("timeseries-plot", "figure"),
        Output("map-plot", "figure"),
        Output("timeseries-plot-dispersal", "figure"),
        Output("map-plot-dispersal", "figure"),
        Output("time-range-store", "data"),
        Output("last-update-time", "data"),
        Output("last-update-display", "children"),
        Output("most-recent-timestamp-display", "children"),
        Output("total-rows-all-data", "children"),
        Output("total-rows-filtered", "children"),
    ],
    [
        Input("interval-component", "n_intervals"),
        Input("timeseries-fields-dropdown", "value"),
        Input("map-field-dropdown", "value"),
        Input("resample-dropdown", "value"),
        Input("time-range-slider", "value"),
    ],
    [
        State("last-update-time", "data"),
        State("time-range-store", "data"),
    ],
    prevent_initial_call=True
)
def update_plots(
    n_intervals,
    ts_fields,
    map_field,
    resample_freq,
    time_range_slider,
    last_update,
    stored_time_range,
):
    # Map slider UNIX timestamps to actual timestamps in the data
    if not data_manager.data.empty and "timestamp" in data_manager.data.columns:
        timestamps = pd.to_datetime(data_manager.data["timestamp"])
        slider_min = int(timestamps.min().timestamp())
        slider_max = int(timestamps.max().timestamp())
        # Clamp slider values
        start_ts = max(slider_min, min(time_range_slider[0], slider_max))
        end_ts = max(slider_min, min(time_range_slider[1], slider_max))
        # Find closest timestamps in the data
        start_time = timestamps.iloc[(timestamps - pd.to_datetime(start_ts, unit="s")).abs().argmin()]
        end_time = timestamps.iloc[(timestamps - pd.to_datetime(end_ts, unit="s")).abs().argmin()]
    else:
        start_time = None
        end_time = None

    data = data_manager.get_data(start_time, end_time, resample_freq)

    # Calculate total rows
    total_rows_all_data = len(data_manager.data)
    total_rows_filtered = len(data)

    # Create plots with uirevision set from the beginning
    ts_fig = create_timeseries_plot(data, ts_fields or [])
    map_fig = create_map_plot(data, map_field)

    # Set uirevision immediately during creation to minimize jumps
    if ts_fig:
        ts_fig.update_layout(
            uirevision="timeseries-constant",
            transition={'duration': 100}  # Disable animations to reduce visual jumps
        )
    
    if map_fig:
        map_fig.update_layout(
            uirevision="map-constant",
            transition={'duration': 100}  # Disable animations to reduce visual jumps
        )

    # Create a custom timeseries plot for the Dispersal View
    dispersal_fig = create_dispersal_plot(data)

    # Get the most recent timestamp from the data
    most_recent_timestamp = data["timestamp"].max() if not data.empty else None
    most_recent_timestamp_iso = most_recent_timestamp.isoformat() if most_recent_timestamp else "N/A"

    current_time = datetime.now().replace(microsecond=0).isoformat()

    # Return selected time range
    time_range = {"start": str(start_time), "end": str(end_time)} if start_time and end_time else None
    return (
        ts_fig,
        map_fig,
        dispersal_fig,  # Custom figure for dispersal view
        map_fig,  # Reuse the same map figure for dispersal view
        time_range,
        current_time,
        f"{current_time}",
        f"{most_recent_timestamp_iso}",
        f"{total_rows_all_data}",
        f"{total_rows_filtered}",
    )


# Callback to update the pH value box
@app.callback(
    Output("ph-value", "children"),
    Output("ph-value", "style"),
    Input("interval-component", "n_intervals"),
)
def update_ph_value_box(n_intervals):
    if not data_manager.data.empty and "ph_corrected_ma" in data_manager.data.columns:
        latest_ph = data_manager.data["ph_corrected_ma"].iloc[-1]
        color = "red" if latest_ph > 8 else "black"
        return f"{latest_ph:.2f}", {"fontSize": "36px", "color": color, "fontWeight": "bold"}
    return "No Data", {"fontSize": "36px", "color": "black", "fontWeight": "bold"}


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