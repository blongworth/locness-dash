import tomllib
import logging
from datetime import datetime, timezone
import threading
import time
import pandas as pd

import dash
from dash import dcc, html, Input, Output, State, ctx
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import ThemeSwitchAIO

from data import DataManager
from plots import (
    create_timeseries_plot,
    create_map_plot,
    create_dispersal_plot,
    create_correlation_plot,
    create_bland_altman_plot,
)

# TODO: test automatic updates of all plots and data
# TODO: prevent "too much data"
# TODO: test no network connection
# TODO: fix jitter
# TODO: remove app-calculated ph ma once tested
# TODO: Add data to traces rather than redrawing entire plot


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("locness_dash")

# Load configuration from config.toml
with open("config.toml", "rb") as f:
    toml_config = tomllib.load(f)

# Use the top-level [locness_dash] section in config.toml
config = toml_config.get("locness_dash", {})


# Initialize data manager
try:
    if config.get("dynamodb_table"):
        logger.info("Initializing DataManager with DynamoDB table '%s' in region '%s'", config["dynamodb_table"], config.get("dynamodb_region", "us-east-1"))
        data_manager = DataManager(
            data_path=config.get("data_path"),
            dynamodb_table=config["dynamodb_table"],
            dynamodb_region=config.get("dynamodb_region", "us-east-1")
        )
    else:
        logger.info("Initializing DataManager with data_path '%s'", config.get("data_path"))
        data_manager = DataManager(config["data_path"])
    data_manager.load_initial_data()
    logger.info("Initial data loaded successfully.")
except Exception as e:
    logger.error("Failed to initialize DataManager: %s", e, exc_info=True)
    raise

# Initialize Dash app
dark_theme = "darkly"
light_theme = "bootstrap"
url_dark_theme = dbc.themes.DARKLY
url_light_theme = dbc.themes.BOOTSTRAP
dbc_css = (
    "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates@V1.0.1/dbc.min.css"
)
app = dash.Dash(
    __name__,
    title="LOCNESS Underway Dashboard",
    external_stylesheets=[url_light_theme, url_dark_theme, dbc_css],
)
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
    dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H1("LOCNESS Underway Dashboard", className="text-center mb-4"),
            ], width=12)
        ]),
        dbc.Row([
            # Sidebar
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        ThemeSwitchAIO(
                            aio_id="theme",
                            themes=[url_light_theme, url_dark_theme],
                        ),
                        html.Hr(),
                        dbc.Label("Timeseries Fields:"),
                        dcc.Dropdown(
                            id="timeseries-fields-dropdown",
                            options=[],
                            value=["rho_ppb", "ph_corrected_ma"],
                            multi=True,
                            placeholder="Select marine data fields for timeseries",
                            className="mb-3"
                        ),
                        dbc.Label("Map Field:"),
                        dcc.Dropdown(
                            id="map-field-dropdown",
                            options=[],
                            value="rho_ppb",
                            placeholder="Select field for ship track visualization",
                            className="mb-3"
                        ),
                        dbc.Label("Resample Interval:"),
                        dcc.Dropdown(
                            id="resample-dropdown",
                            options=[
                                {"label": "No Resampling", "value": "None"}, # intolerable
                                {"label": "10 Seconds", "value": "10s"},
                                {"label": "1 Minute", "value": "1min"},
                                {"label": "10 Minutes", "value": "10min"},
                                {"label": "1 Hour", "value": "1h"},
                            ],
                            value=config["default_resampling"],
                            clearable=False,
                            className="mb-3"
                        ),
                        dbc.Label("Time Range:"),
                        dbc.Switch(
                            id="auto-update-toggle",
                            label="Auto-Update",
                            value=True,
                            className="mb-2",
                            persistence=True,
                            persistence_type="session"
                        ),
                        dbc.Switch(
                            id="time-range-mode",
                            label="Last 4h",
                            value=True,
                            className="mb-2",
                            persistence=True,
                            persistence_type="session"
                        ),
                        dcc.RangeSlider(
                            id="time-range-slider",
                            min=0, max=1, step=1, value=[0, 1],
                            marks={}, tooltip={"placement": "bottom", "always_visible": False},
                            allowCross=False, className="mb-3"
                        ),
                        html.Hr(),
                        dbc.Card([
                            dbc.CardBody([
                                html.P([dbc.Badge("Last update:", color="secondary", className="me-2"), 
                                       html.Span(id="last-update-display")]),
                                html.P([dbc.Badge("Last timestamp:", color="secondary", className="me-2"), 
                                       html.Span(id="most-recent-timestamp-display")]),
                                html.P([dbc.Badge("Total rows (all):", color="info", className="me-2"), 
                                       html.Span(id="total-rows-all-data")]),
                                html.P([dbc.Badge("Total rows (filtered):", color="info", className="me-2"), 
                                       html.Span(id="total-rows-filtered")]),
                                html.P([dbc.Badge("Missing rows (all):", color="info", className="me-2"), 
                                       html.Span(id="missing-rows-all-data")]),
                            ])
                        ], color="light", outline=True)
                    ])
                ])
            ], width=3),
            # Main content
            dbc.Col([
                dbc.Tabs([
                    dbc.Tab(label="Dispersal View", tab_id="dispersal", children=[
                        dbc.Row([
                            dbc.Col([
                                dcc.Graph(id="timeseries-plot-dispersal", 
                                         style={"height": "30vh", "minHeight": "200px"})
                            ], width=10),
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardBody([
                                        html.H6("pH (2min avg)", className="text-center"),
                                        html.H2(id="ph-value", children="No Data", 
                                               className="text-center", style={"fontSize": "2.5rem"})
                                    ])
                                ], className="mb-2", color="light", outline=True),
                                dbc.Card([
                                    dbc.CardBody([
                                        html.H6("Rho (ppb)", className="text-center"),
                                        html.H2(id="rho-value", children="No Data", 
                                               className="text-center", style={"fontSize": "2.5rem"})
                                    ])
                                ], color="light", outline=True)
                            ], width=2)
                        ], className="mb-3"),
                        dbc.Row([
                            dbc.Col([
                                dcc.Graph(id="map-plot-dispersal", 
                                         style={"height": "50vh", "minHeight": "300px"})
                            ], width=12)
                        ])
                    ]),
                    dbc.Tab(label="Main View", tab_id="main", children=[
                        dcc.Graph(id="map-plot", style={"height": "80vh", "minHeight": "300px"}),
                        dcc.Graph(id="timeseries-plot",
                                  #style={"height": "40vh", "minHeight": "300px"}
                                  )
                    ]),
                    dbc.Tab(label="All Fields Timeseries", tab_id="all-fields", children=[
                        dcc.Graph(id="all-fields-timeseries-plot", 
                                 #style={"height": "80vh", "minHeight": "600px"}
                                 )
                    ]),
                    dbc.Tab(label="Correlation", tab_id="correlation", children=[
                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardBody([
                                        dbc.Row([
                                            dbc.Col([
                                                dbc.Label("X Axis:"),
                                                dcc.Dropdown(id="correlation-x-dropdown", options=[], 
                                                           placeholder="Select X variable")
                                            ], width=6),
                                            dbc.Col([
                                                dbc.Label("Y Axis:"),
                                                dcc.Dropdown(id="correlation-y-dropdown", options=[], 
                                                           placeholder="Select Y variable")
                                            ], width=6)
                                        ])
                                    ])
                                ], className="mb-3")
                            ], width=12)
                        ]),
                        dcc.Graph(id="correlation-scatterplot", 
                                 style={"height": "40vh", "minHeight": "300px"}),
                        dcc.Graph(id="bland-altman-plot", 
                                 style={"height": "40vh", "minHeight": "300px"})
                    ])
                ], id="main-tabs", active_tab="dispersal")
            ], width=9)
        ]),
        dcc.Store(id="last-update-time"),
        dcc.Store(id="time-range-store"),
        dcc.Interval(id="interval-component", interval=config["update_interval"] * 1000, n_intervals=0),
    ], fluid=True)
], id="app-container")


# Simple theme function using Bootstrap classes
def get_bootstrap_theme_class(is_light_theme):
    """Get Bootstrap theme class"""
    return "light" if is_light_theme else "dark"


# Simple Bootstrap theme callback
@app.callback(
    Output("app-container", "data-bs-theme"),
    [Input(ThemeSwitchAIO.ids.switch("theme"), "value")]
)
def update_bootstrap_theme(toggle):
    """Update Bootstrap theme using data-bs-theme attribute"""
    return "light" if toggle else "dark"


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
    ],
)
def update_correlation_dropdowns(n, x_value, y_value):
    if data_manager.data.empty:
        return [], None, [], None
    exclude = ["id", "datetime_utc", "timestamp"]
    numeric_cols = [
        col
        for col in data_manager.data.columns
        if col not in exclude and data_manager.data[col].dtype in ["float64", "int64"]
    ]
    options = [{"label": col, "value": col} for col in numeric_cols]
    # Set defaults if current value is not valid
    x_out = (
        x_value
        if x_value in numeric_cols
        else (numeric_cols[2] if numeric_cols else None)
    )
    y_out = (
        y_value
        if y_value in numeric_cols
        else (numeric_cols[3] if len(numeric_cols) > 9 else None)
    )
    return options, x_out, options, y_out


# Callback to update correlation scatterplot and Bland-Altman plot
@app.callback(
    [
        Output("correlation-scatterplot", "figure"),
        Output("bland-altman-plot", "figure"),
    ],
    [
        Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
        Input("interval-component", "n_intervals"),
        Input("correlation-x-dropdown", "value"),
        Input("correlation-y-dropdown", "value"),
        Input("resample-dropdown", "value"),
        Input("time-range-slider", "value"),
        Input("time-range-mode", "value"),
        Input("auto-update-toggle", "value"),
    ],
)
def update_correlation_and_bland_altman(
    toggle, n_intervals, x_col, y_col, resample_freq, time_range_slider, time_range_mode, auto_update
):
    # theme template
    template = light_theme if toggle else dark_theme
    # Always return empty figures if x_col or y_col is None
    if not x_col or not y_col:
        return {}, {}
    if not data_manager.data.empty and "datetime_utc" in data_manager.data.columns:
        datetime_utcs = pd.to_datetime(data_manager.data["datetime_utc"])
        slider_min = datetime_utcs.min().timestamp()
        slider_max = datetime_utcs.max().timestamp()
        start_ts = max(slider_min, min(time_range_slider[0], slider_max))
        end_ts = slider_max
        start_time = datetime.fromtimestamp(start_ts, tz=timezone.utc)
        end_time = datetime.fromtimestamp(end_ts, tz=timezone.utc)
        if resample_freq == "None":
            data = data_manager.get_data(start_time, end_time)
        else:
            data = data_manager.get_data(start_time, end_time, resample_freq)
        fig_corr = create_correlation_plot(data, x_col, y_col, template=template)
        fig_ba = create_bland_altman_plot(data, x_col, y_col, template=template)
        
        # Determine uirevision strategy based on auto-update mode
        if auto_update:
            # In auto-update mode, use data-dependent uirevision to allow updates when new data arrives
            data_hash = f"{len(data)}-{n_intervals}"
            corr_uirevision = f"correlation-auto-{data_hash}"
            ba_uirevision = f"bland-altman-auto-{data_hash}"
        else:
            # In fixed mode, use constant uirevision to preserve user interactions
            corr_uirevision = "correlation-constant"
            ba_uirevision = "bland-altman-constant"
        
        if fig_corr:
            fig_corr.update_layout(
                uirevision=corr_uirevision, transition={"duration": 100}
            )
        if fig_ba:
            fig_ba.update_layout(
                uirevision=ba_uirevision, transition={"duration": 100}
            )
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
    ],
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
    if (
        not ts_value
        or not isinstance(ts_value, list)
        or not any(val in ts_fields for val in ts_value)
    ):
        ts_value_out = [val for val in ts_default if val in ts_fields]
    else:
        # Only keep values that are still valid
        ts_value_out = [val for val in ts_value if val in ts_fields]
        if not ts_value_out:
            ts_value_out = [val for val in ts_default if val in ts_fields]

    # Validate map value
    if not map_value or map_value not in map_fields:
        map_value_out = (
            map_default
            if map_default in map_fields
            else (map_fields[0] if map_fields else None)
        )
    else:
        map_value_out = map_value

    return ts_options, ts_value_out, map_options, map_value_out



# Callback to update time range slider properties (simplified - no value logic)
@app.callback(
    [
        Output("time-range-slider", "min"),
        Output("time-range-slider", "max"),
        Output("time-range-slider", "marks"),
    ],
    [Input("interval-component", "n_intervals")],
)
def update_time_slider_properties(n):
    if not data_manager.data.empty and "datetime_utc" in data_manager.data.columns:
        datetime_utcs = pd.to_datetime(data_manager.data["datetime_utc"])
        min_ts = datetime_utcs.min().timestamp()
        max_ts = datetime_utcs.max().timestamp()
        slider_min = int(min_ts)
        slider_max = int(max_ts)
        marks = {
            slider_min: datetime.fromtimestamp(slider_min, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            slider_max: datetime.fromtimestamp(slider_max, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
        }
    else:
        slider_min = 0
        slider_max = 1
        marks = {}
    return slider_min, slider_max, marks


# Callback to update plots and time slider value
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
        Output("missing-rows-all-data", "children"),
        Output("total-rows-filtered", "children"),
        Output("time-range-slider", "value"),
    ],
    [
        Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
        Input("interval-component", "n_intervals"),
        Input("timeseries-fields-dropdown", "value"),
        Input("map-field-dropdown", "value"),
        Input("resample-dropdown", "value"),
        Input("time-range-slider", "value"),
        Input("time-range-mode", "value"),
        Input("auto-update-toggle", "value"),
    ],
    [
        State("last-update-time", "data"),
        State("time-range-store", "data"),
    ],
    prevent_initial_call=False,
)
def update_plots(
    toggle,
    n_intervals,
    ts_fields,
    map_field,
    resample_freq,
    time_range_slider,
    time_range_mode,
    auto_update,
    last_update,
    stored_time_range,
):
    # theme template
    template = light_theme if toggle else dark_theme

    # Check what triggered this callback
    triggered_id = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else None
    switch_triggered = triggered_id in ["time-range-mode", "auto-update-toggle"]
    
    # Determine time range based on switches and slider
    if not data_manager.data.empty and "datetime_utc" in data_manager.data.columns:
        datetime_utcs = pd.to_datetime(data_manager.data["datetime_utc"])
        slider_min = datetime_utcs.min().timestamp()
        slider_max = datetime_utcs.max().timestamp()
        
        # Calculate default range based on time-range-mode switch
        if time_range_mode:  # Last 4h mode
            four_hours_ago = slider_max - 4 * 3600
            default_start = max(slider_min, int(four_hours_ago))
            default_range = [default_start, slider_max]
        else:  # All data mode
            default_range = [slider_min, slider_max]
        
        # Determine actual time range based on what triggered the callback
        if switch_triggered:
            # Switch was changed - use the default range for the new switch state
            actual_range = default_range
        elif auto_update:
            # Auto-update mode: keep user's start time, update end to latest
            if (
                isinstance(time_range_slider, list)
                and len(time_range_slider) == 2
                and slider_min <= time_range_slider[0] <= slider_max
            ):
                # Keep user's start time, but update end time to latest
                actual_range = [time_range_slider[0], slider_max]
            else:
                actual_range = default_range
        else:
            # Fixed mode: use slider value if valid, otherwise use default
            if (
                isinstance(time_range_slider, list)
                and len(time_range_slider) == 2
                and slider_min <= time_range_slider[0] <= slider_max
                and slider_min <= time_range_slider[1] <= slider_max
            ):
                actual_range = time_range_slider
            else:
                actual_range = default_range
        
        # Convert to datetime objects
        start_ts = max(slider_min, min(actual_range[0], slider_max))
        end_ts = slider_max  # Always end at latest data
        start_time = datetime.fromtimestamp(start_ts, tz=timezone.utc)
        end_time = datetime.fromtimestamp(end_ts, tz=timezone.utc)
        
        # Set the slider value to what we're actually using
        slider_value = actual_range
    else:
        start_time = None
        end_time = None
        slider_value = [0, 1]

    # Add check for resample_freq is None
    if resample_freq == "None":
        data = data_manager.get_data(start_time, end_time)
    else:
        data = data_manager.get_data(start_time, end_time, resample_freq)

    # Calculate total rows
    total_rows_all_data = len(data_manager.data)
    missing_rows_all_data = data_manager.data.isnull().any(axis=1).sum()
    total_rows_filtered = len(data)

    # Create plots with uirevision set from the beginning
    ts_fig = create_timeseries_plot(data, ts_fields or [], template=template)
    map_fig = create_map_plot(data, map_field, template=template)

    # Exclude datetime_utc and index columns
    exclude = ["datetime_utc", "index", "id"]
    fields = [col for col in data.columns if col not in exclude]
    all_ts_fig = create_timeseries_plot(data, fields, template=template)

    # Determine uirevision strategy based on auto-update mode
    if auto_update:
        # In auto-update mode, use data-dependent uirevision to allow updates when new data arrives
        # This ensures plots redraw only when there's actually new data, not on every callback
        data_hash = f"{total_rows_all_data}-{n_intervals}"
        ts_uirevision = f"timeseries-auto-{data_hash}"
        map_uirevision = f"map-auto-{data_hash}"
        dispersal_uirevision = f"dispersal-auto-{data_hash}"
        all_fields_uirevision = f"all-fields-auto-{data_hash}"
    else:
        # In fixed mode, use constant uirevision to preserve all user interactions
        ts_uirevision = "timeseries-constant"
        map_uirevision = "map-constant"
        dispersal_uirevision = "dispersal-timeseries-constant"
        all_fields_uirevision = "all-fields-constant"

    # Set uirevision based on auto-update mode
    if ts_fig:
        ts_fig.update_layout(
            uirevision=ts_uirevision,
            transition={"duration": 100},  # Disable animations to reduce visual jumps
        )

    if map_fig:
        map_fig.update_layout(
            uirevision=map_uirevision,
            transition={"duration": 100},  # Disable animations to reduce visual jumps
        )

    if all_ts_fig:
        all_ts_fig.update_layout(
            uirevision=all_fields_uirevision,
            transition={"duration": 100},  # Disable animations to reduce visual jumps
        )

    # Create a custom timeseries plot for the Dispersal View
    dispersal_fig = create_dispersal_plot(data, template=template)

    # Set uirevision for dispersal plot
    if dispersal_fig:
        dispersal_fig.update_layout(
            uirevision=dispersal_uirevision,
            transition={"duration": 100},  # Disable animations to reduce visual jumps
        )

    # Get the most recent timestamp from the data
    most_recent_timestamp = (
        data_manager.data["datetime_utc"].max() if not data_manager.data.empty else None
    )
    most_recent_timestamp_iso = (
        most_recent_timestamp.isoformat() if most_recent_timestamp else "N/A"
    )

    current_time = datetime.now().replace(microsecond=0).isoformat()

    # Return selected time range
    time_range = (
        {"start": str(start_time), "end": str(end_time)}
        if start_time and end_time
        else None
    )
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
        f"{missing_rows_all_data}",
        f"{total_rows_filtered}",
        slider_value,
    )


# Simplified value boxes callback
@app.callback(
    [Output("ph-value", "children"), Output("ph-value", "style"),
     Output("rho-value", "children"), Output("rho-value", "style")],
    [Input("interval-component", "n_intervals")]
)
def update_value_boxes(n_intervals):
    ph_val = "No Data"
    ph_style = {"fontSize": "2.5rem", "fontWeight": "bold"}
    rho_val = "No Data"
    rho_style = {"fontSize": "2.5rem", "fontWeight": "bold"}
    
    if not data_manager.data.empty:
        if "ph_corrected_ma" in data_manager.data.columns:
            latest_ph = data_manager.data["ph_corrected_ma"].dropna().iloc[-1]
            ph_val = f"{latest_ph:.2f}"
            # Use Bootstrap text colors
            if latest_ph > 8:
                ph_style = {"fontSize": "2.5rem", "fontWeight": "bold", "color": "red"}
        if "rho_ppb" in data_manager.data.columns:
            latest_rho = data_manager.data["rho_ppb"].dropna().iloc[-1]
            rho_val = f"{latest_rho:.1f}"
    
    return ph_val, ph_style, rho_val, rho_style


# Background thread to check for new data

def background_update():
    while True:
        time.sleep(config["update_interval"])
        try:
            new_data = data_manager.get_new_data()
            if not new_data.empty:
                logger.info("Retrieved %d new records", len(new_data))
            else:
                logger.info("No new data available")
        except Exception as e:
            logger.warning("Error during background update: %s", e, exc_info=True)


# Start background thread
update_thread = threading.Thread(target=background_update, daemon=True)
update_thread.start()

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8050))
    logger.info("Starting LOCNESS Dash app on port %d", port)
    try:
        app.run(debug=True, host="0.0.0.0", port=port)
    except Exception as e:
        logger.error("App failed to start: %s", e, exc_info=True)

