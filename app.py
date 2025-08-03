import tomllib
from datetime import datetime
import threading
import time
import pandas as pd

import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc

from data import DataManager
from plots import create_timeseries_plot, create_map_plot, create_dispersal_plot, create_correlation_plot, create_bland_altman_plot


# TODO: test automatic updates of all plots and data
# TODO: local server
# TODO: prevent "too much data"
# TODO: test no network connection
# TODO: calculate ph ma here and compare to ph_corrected_ma
# TODO: dark mode
# TODO: Diagnostics View
# TODO: Add data to traces rather than redrawing entire plot


# Load configuration from config.toml
with open("config.toml", "rb") as f:
    toml_config = tomllib.load(f)

# Use the top-level [locness_dash] section in config.toml
config = toml_config.get("locness_dash", {})

# Initialize data manager
data_manager = DataManager(config["data_path"])
data_manager.load_initial_data()

# Initialize Dash app
external_stylesheets = [dbc.themes.BOOTSTRAP]
app = dash.Dash(__name__, 
                title="LOCNESS Underway Dashboard",
                external_stylesheets=external_stylesheets)
server = app.server


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
                        # {"label": "No Resampling", "value": "None"}, # intolerable
                        {"label": "10 Seconds", "value": "10s"}, # slow
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
            dcc.Tab(label="Main View", value="Main View", children=[
                html.Div([
                    html.Div([dcc.Graph(id="map-plot")]),
                    html.Div([dcc.Graph(id="timeseries-plot")]),
                ], style={"padding": "10px"}),
            ]),
            dcc.Tab(label="All Fields Timeseries", value="All Fields Timeseries", children=[
                html.Div([
                    dcc.Graph(id="all-fields-timeseries-plot")
                ], style={"padding": "10px"}),
            ]),
            dcc.Tab(label="Dispersal View", value="Dispersal View", children=[
                html.Div([
                    html.Div([
                        dcc.Graph(
                            id="timeseries-plot-dispersal",
                            style={
                                "height": "100%",
                                "width": "80%",
                                "minHeight": "200px",
                                "minWidth": "600px",
                                "display": "inline-block",
                                "verticalAlign": "middle",
                            },
                        ),
                        html.Div(
                            [
                                html.Div(
                                    id="ph-value-box",
                                    style={
                                        "height": "120px",
                                        "width": "150px",
                                        "display": "flex",
                                        "flexDirection": "column",
                                        "alignItems": "center",
                                        "justifyContent": "center",
                                        "textAlign": "center",
                                        "padding": "10px",
                                        "border": "1px solid #ddd",
                                        "background": "#f8f9fa",
                                        "marginBottom": "20px",
                                    },
                                    children=[
                                        html.Div("pH (2min avg)", style={"fontSize": "16px", "marginBottom": "10px"}),
                                        html.Div("No Data", id="ph-value", style={"fontSize": "46px", "color": "black", "fontWeight": "bold"}),
                                    ],
                                ),
                                html.Div(
                                    id="rho-value-box",
                                    style={
                                        "height": "120px",
                                        "width": "150px",
                                        "display": "flex",
                                        "flexDirection": "column",
                                        "alignItems": "center",
                                        "justifyContent": "center",
                                        "textAlign": "center",
                                        "padding": "10px",
                                        "border": "1px solid #ddd",
                                        "background": "#f8f9fa",
                                    },
                                    children=[
                                        html.Div("Rho (ppb)", style={"fontSize": "16px", "marginBottom": "10px"}),
                                        html.Div("No Data", id="rho-value", style={"fontSize": "46px", "color": "black", "fontWeight": "bold"}),
                                    ],
                                ),
                            ],
                            style={
                                "display": "flex",
                                "flexDirection": "column",
                                "alignItems": "center",
                                "justifyContent": "center",
                                "height": "100%",
                                "marginLeft": "20px",
                            },
                        ),
                    ], style={"height": "30vh", "minHeight": "150px", "width": "100%", "marginBottom": "10px", "display": "flex", "alignItems": "center", "justifyContent": "flex-start"}),
                    html.Div([
                        dcc.Graph(id="map-plot-dispersal", style={"height": "100%", "minHeight": "200px", "width": "100%"})
                    ], style={"height": "50vh", "minHeight": "200px", "width": "100%"}),
                ], style={"padding": "10px"}),
            ]),
            dcc.Tab(label="Correlation", value="Correlation", children=[
                html.Div([
                    html.Div([
                        html.Div([
                            html.Label("X Axis:"),
                            dcc.Dropdown(
                                id="correlation-x-dropdown",
                                options=[],
                                value=None,
                                placeholder="Select X variable",
                            ),
                        ], style={"flex": 1, "marginRight": "10px"}),
                        html.Div([
                            html.Label("Y Axis:"),
                            dcc.Dropdown(
                                id="correlation-y-dropdown",
                                options=[],
                                value=None,
                                placeholder="Select Y variable",
                            ),
                        ], style={"flex": 1, "marginLeft": "10px"}),
                    ], style={"display": "flex", "flexDirection": "row", "width": "700px", "padding": "20px", "background": "#f8f9fa", "borderTop": "1px solid #ddd", "margin": "0 auto"}),
                    html.Div([
                        dcc.Graph(id="correlation-scatterplot")
                    ], style={"width": "100%", "padding": "10px"}),
                    html.Div([
                        dcc.Graph(id="bland-altman-plot")
                    ], style={"width": "100%", "padding": "10px"}),
                ], style={"display": "flex", "flexDirection": "column", "alignItems": "center"}),
            ]),

        ], id="main-tabs", value="Dispersal View"),
        dcc.Store(id="last-update-time"),
        dcc.Store(id="time-range-store"),
        dcc.Interval(
            id="interval-component",
            interval=config["update_interval"] * 1000,
            n_intervals=0,
        )
    ], style={"marginLeft": "270px", "padding": "0px 10px 10px 10px", "minWidth": 0}),
])


# Callback to update correlation dropdown options
@app.callback(
    [
        Output("correlation-x-dropdown", "options"),
        Output("correlation-x-dropdown", "value"),
        Output("correlation-y-dropdown", "options"),
        Output("correlation-y-dropdown", "value"),
    ],
    [Input("interval-component", "n_intervals")],
    [
        State("correlation-x-dropdown", "value"),
        State("correlation-y-dropdown", "value"),
    ]
)
def update_correlation_dropdowns(n, x_value, y_value):
    if data_manager.data.empty:
        return [], None, [], None
    exclude = ["id", "datetime_utc", "timestamp"]
    numeric_cols = [col for col in data_manager.data.columns if col not in exclude and data_manager.data[col].dtype in ["float64", "int64"]]
    options = [{"label": col, "value": col} for col in numeric_cols]
    # Set defaults if current value is not valid
    x_out = x_value if x_value in numeric_cols else (numeric_cols[8] if numeric_cols else None)
    y_out = y_value if y_value in numeric_cols else (numeric_cols[10] if len(numeric_cols) > 9 else None)
    return options, x_out, options, y_out


# Callback to update correlation scatterplot and Bland-Altman plot
@app.callback(
    [
        Output("correlation-scatterplot", "figure"),
        Output("bland-altman-plot", "figure"),
    ],
    [
        Input("interval-component", "n_intervals"),
        Input("correlation-x-dropdown", "value"),
        Input("correlation-y-dropdown", "value"),
        Input("resample-dropdown", "value"),
        Input("time-range-slider", "value"),
    ]
)
def update_correlation_and_bland_altman(n_intervals, x_col, y_col, resample_freq, time_range_slider):
    # Always return empty figures if x_col or y_col is None
    if not x_col or not y_col:
        return {}, {}
    if not data_manager.data.empty and "datetime_utc" in data_manager.data.columns:
        datetime_utcs = pd.to_datetime(data_manager.data["datetime_utc"])
        slider_min = datetime_utcs.min().timestamp()
        slider_max = datetime_utcs.max().timestamp()
        start_ts = max(slider_min, min(time_range_slider[0], slider_max))
        end_ts = slider_max
        start_time = datetime.fromtimestamp(start_ts)
        end_time = datetime.fromtimestamp(end_ts)
        if resample_freq == "None":
            data = data_manager.get_data(start_time, end_time)
        else:
            data = data_manager.get_data(start_time, end_time, resample_freq)
        fig_corr = create_correlation_plot(data, x_col, y_col)
        if fig_corr:
            fig_corr.update_layout(uirevision="correlation-constant", transition={'duration': 100})
        fig_ba = create_bland_altman_plot(data, x_col, y_col)
        if fig_ba:
            fig_ba.update_layout(uirevision="bland-altman-constant", transition={'duration': 100})
        return fig_corr, fig_ba
    return {}, {}


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
    if not data_manager.data.empty and "datetime_utc" in data_manager.data.columns:
        datetime_utcs = pd.to_datetime(data_manager.data["datetime_utc"])
        min_ts = datetime_utcs.min().timestamp()
        max_ts = datetime_utcs.max().timestamp()
        slider_min = int(min_ts)
        slider_max = int(max_ts)
        marks = {
            slider_min: datetime.fromtimestamp(slider_min).strftime("%Y-%m-%d %H:%M"),
            slider_max: datetime.fromtimestamp(slider_max).strftime("%Y-%m-%d %H:%M"),
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
        Output("all-fields-timeseries-plot", "figure"),
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
    if not data_manager.data.empty and "datetime_utc" in data_manager.data.columns:
        datetime_utcs = pd.to_datetime(data_manager.data["datetime_utc"])
        slider_min = datetime_utcs.min().timestamp()
        slider_max = datetime_utcs.max().timestamp()
        # Clamp slider values
        start_ts = max(slider_min, min(time_range_slider[0], slider_max))
        end_ts = slider_max
        #end_ts = max(slider_min, min(time_range_slider[1], slider_max))
        # Convert Unix timestamps to datetime
        start_time = datetime.fromtimestamp(start_ts)
        end_time = datetime.fromtimestamp(end_ts)
    else:
        start_time = None
        end_time = None

    # Add check for resample_freq is None
    if resample_freq == "None":
        data = data_manager.get_data(start_time, end_time)
    else:
        data = data_manager.get_data(start_time, end_time, resample_freq)

    # Calculate total rows
    total_rows_all_data = len(data_manager.data)
    total_rows_filtered = len(data)

    # Create plots with uirevision set from the beginning
    ts_fig = create_timeseries_plot(data, ts_fields or [])
    map_fig = create_map_plot(data, map_field)

    # Exclude datetime_utc and index columns
    exclude = ["datetime_utc", "index", "id"]
    fields = [col for col in data.columns if col not in exclude]
    all_ts_fig = create_timeseries_plot(data, fields)

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
    most_recent_timestamp = data_manager.data["datetime_utc"].max() if not data_manager.data.empty else None
    most_recent_timestamp_iso = most_recent_timestamp.isoformat() if most_recent_timestamp else "N/A"

    current_time = datetime.now().replace(microsecond=0).isoformat()

    # Return selected time range
    time_range = {"start": str(start_time), "end": str(end_time)} if start_time and end_time else None
    return (
        ts_fig,
        map_fig,
        dispersal_fig,  # Custom figure for dispersal view
        map_fig,  # Reuse the same map figure for dispersal view
        all_ts_fig,
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
    Output("rho-value", "children"),
    Output("rho-value", "style"),
    Input("interval-component", "n_intervals"),
)
def update_value_boxes(n_intervals):
    ph_val = "No Data"
    ph_style = {"fontSize": "46px", "color": "black", "fontWeight": "bold"}
    rho_val = "No Data"
    rho_style = {"fontSize": "46px", "color": "black", "fontWeight": "bold"}
    if not data_manager.data.empty:
        if "ph_corrected_ma" in data_manager.data.columns:
            latest_ph = data_manager.data["ph_corrected_ma"].iloc[-1]
            ph_val = f"{latest_ph:.2f}"
            ph_style = {"fontSize": "46px", "color": "red" if latest_ph > 8 else "black", "fontWeight": "bold"}
        if "rho_ppb" in data_manager.data.columns:
            latest_rho = data_manager.data["rho_ppb"].iloc[-1]
            rho_val = f"{latest_rho:.1f}"
    return ph_val, ph_style, rho_val, rho_style


# Background thread to check for new data
def background_update():
    while True:
        time.sleep(config["update_interval"])
        new_data = data_manager.get_new_data()
        if not new_data.empty:
            print(f"Retrieved {len(new_data)} new records")
        else:
            print("No new data available")


# Start background thread
update_thread = threading.Thread(target=background_update, daemon=True)
update_thread.start()

if __name__ == "__main__":
    app.run(debug=True, port=8050)