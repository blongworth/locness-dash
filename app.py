import tomllib
import logging
import os
import time
from datetime import datetime, timezone
import pandas as pd

import dash
from dash import dcc, html, Input, Output, State, callback
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

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("locness_dash")

# Load configuration
with open("config.toml", "rb") as f:
    config = tomllib.load(f).get("locness_dash", {})

# Initialize data manager
try:
    if config.get("dynamodb_table"):
        logger.info("Initializing DataManager with DynamoDB table '%s'", config["dynamodb_table"])
        data_manager = DataManager(
            data_path=config.get("data_path"),
            dynamodb_table=config["dynamodb_table"],
            dynamodb_region=config.get("dynamodb_region", "us-east-1"),
            spot_feed_id=config.get("spot_feed_id"),
            drifter_update_interval=config.get("drifter_update_interval", 300)
        )
    else:
        logger.info("Initializing DataManager with data_path '%s'", config.get("data_path"))
        data_manager = DataManager(
            config["data_path"],
            spot_feed_id=config.get("spot_feed_id"),
            drifter_update_interval=config.get("drifter_update_interval", 300)
        )
    data_manager.load_initial_data()
    logger.info("Initial data loaded successfully.")
    
    # Load initial drifter data if available
    if data_manager.spot_api:
        data_manager.update_drifter_data(force_update=True)
        logger.info("Initial drifter data loaded successfully.")
    
except Exception as e:
    logger.error("Failed to initialize DataManager: %s", e, exc_info=True)
    raise

# Initialize Dash app
dark_theme = "darkly"
light_theme = "bootstrap"
url_dark_theme = dbc.themes.DARKLY
url_light_theme = dbc.themes.BOOTSTRAP
dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates@V1.0.1/dbc.min.css"

app = dash.Dash(
    __name__,
    title="LOCNESS Underway Dashboard",
    external_stylesheets=[url_light_theme, url_dark_theme, dbc_css],
)
server = app.server

# Simplified data access without background thread caching

def get_available_fields():
    """Get numeric fields for dropdowns"""
    if data_manager.data.empty:
        return []
    return [
        col for col in data_manager.data.columns
        if col not in ["timestamp", "datetime_utc", "id", "latitude", "longitude", "partition"]
        and data_manager.data[col].dtype in ["float64", "int64"]
    ]

def get_filtered_data(time_range_mode, auto_update, resample_freq, n_intervals):
    """Get filtered data - simplified without caching"""
    
    # Skip data updates during debug reload
    is_debug_reload = os.environ.get('WERKZEUG_RUN_MAIN') == 'true'
    
    # Check for new data when in auto-update mode (but not during debug reload)
    # Skip on initial callback (n_intervals=0) since we already loaded drifter data during init
    if auto_update and data_manager.spot_api and not is_debug_reload and n_intervals > 0:
        try:
            new_data = data_manager.get_new_data()
            if not new_data.empty:
                logger.info("Retrieved %d new records", len(new_data))
            
            # Update drifter data periodically
            data_manager.update_drifter_data()
        except Exception as e:
            logger.warning("Error during data update: %s", e)
    
    # Calculate time range
    if data_manager.data.empty or "datetime_utc" not in data_manager.data.columns:
        return pd.DataFrame()
    
    datetime_utcs = pd.to_datetime(data_manager.data["datetime_utc"])
    max_ts = datetime_utcs.max().timestamp()
    
    # Simple time range calculation
    hours_map = {0: None, 1: 24, 2: 8, 3: 4, 4: 2, 5: 1}
    hours = hours_map.get(time_range_mode, 4)
    
    if hours is None:  # All data
        start_time = None
    else:
        start_ts = max_ts - (hours * 3600)
        start_time = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    
    end_time = datetime.fromtimestamp(max_ts, tz=timezone.utc)
    
    # Get filtered data
    if resample_freq == "None":
        data = data_manager.get_data(start_time, end_time)
    else:
        data = data_manager.get_data(start_time, end_time, resample_freq)
    
    return data

# App layout (simplified)
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
                        ThemeSwitchAIO(aio_id="theme", themes=[url_light_theme, url_dark_theme]),
                        html.Hr(),
                        dbc.Label("Timeseries Fields:"),
                        dcc.Dropdown(
                            id="timeseries-fields-dropdown",
                            options=[],
                            value=["rho_ppb", "ph_corrected_ma"],
                            multi=True,
                            className="mb-3"
                        ),
                        dbc.Label("Map Field:"),
                        dcc.Dropdown(
                            id="map-field-dropdown",
                            options=[],
                            value="rho_ppb",
                            className="mb-3"
                        ),
                        dbc.Switch(
                            id="show-drifters-toggle",
                            label="Show Drifters",
                            value=False,
                            className="mb-2",
                            persistence=True,
                            persistence_type="session"
                        ) if data_manager.spot_api else html.Div(),
                        dbc.Label("Resample Interval:"),
                        dcc.Dropdown(
                            id="resample-dropdown",
                            options=[
                                {"label": "No Resampling", "value": "None"},
                                {"label": "10 Seconds", "value": "10s"},
                                {"label": "1 Minute", "value": "1min"},
                                {"label": "10 Minutes", "value": "10min"},
                                {"label": "1 Hour", "value": "1h"},
                            ],
                            value=config.get("default_resampling", "1min"),
                            clearable=False,
                            className="mb-3"
                        ),
                        dbc.Switch(
                            id="auto-update-toggle",
                            label="Auto-Update",
                            value=True,
                            className="mb-2",
                            persistence=True,
                            persistence_type="session"
                        ),
                        dbc.Label("Time Range:"),
                        dcc.Slider(
                            id="time-range-mode",
                            min=0, max=5, step=1, value=3,
                            marks={
                                0: "All", 1: "24h", 2: "8h",
                                3: "4h", 4: "2h", 5: "1h"
                            },
                            className="mb-3"
                        ),
                        html.Hr(),
                        dbc.Label("Status information:"),
                        dbc.Card([
                            dbc.CardBody([
                                html.P([dbc.Badge("Last update:", color="secondary", className="me-2"), 
                                       html.Span(id="last-update-display")]),
                                html.P([dbc.Badge("Last timestamp:", color="secondary", className="me-2"), 
                                       html.Span(id="most-recent-timestamp-display")]),
                                html.P([dbc.Badge("Total rows:", color="info", className="me-2"), 
                                       html.Span(id="total-rows-all-data")]),
                                html.P([dbc.Badge("Rows with missing data:", color="info", className="me-2"), 
                                       html.Span(id="missing-rows-all-data")]),
                                html.P([dbc.Badge("Filtered/resampled rows:", color="info", className="me-2"), 
                                       html.Span(id="total-rows-filtered")]),
                                html.P([dbc.Badge("Drifter positions:", color="warning", className="me-2"), 
                                       html.Span(id="drifter-status-display")]) if data_manager.spot_api else html.Div(),
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
                                         style={"height": "30vh"})
                            ], width=10),
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardBody([
                                        html.H6("pH", className="text-center"),
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
                                         style={"height": "50vh"})
                            ], width=12)
                        ])
                    ]),
                    dbc.Tab(label="Main View", tab_id="main", children=[
                        dcc.Graph(id="map-plot", style={"height": "80vh", "minHeight": "300px"}),
                        dcc.Graph(id="timeseries-plot")
                    ]),
                    dbc.Tab(label="All Fields", tab_id="all-fields", children=[
                        dcc.Graph(id="all-fields-timeseries-plot")
                    ]),
                    dbc.Tab(label="Correlation", tab_id="correlation", children=[
                        dbc.Row([
                            dbc.Col([
                                dbc.Label("X Axis:"),
                                dcc.Dropdown(id="correlation-x-dropdown", options=[])
                            ], width=6),
                            dbc.Col([
                                dbc.Label("Y Axis:"),
                                dcc.Dropdown(id="correlation-y-dropdown", options=[])
                            ], width=6)
                        ], className="mb-3"),
                        dcc.Graph(id="correlation-scatterplot", style={"height": "40vh"}),
                        dcc.Graph(id="bland-altman-plot", style={"height": "40vh"})
                    ])
                ], id="main-tabs", active_tab="dispersal")
            ], width=9)
        ]),
        dcc.Interval(id="interval-component", interval=config.get("update_interval", 5) * 1000, n_intervals=0),
    ], fluid=True)
], id="app-container")

# Optimized callbacks

@callback(
    Output("app-container", "data-bs-theme"),
    Input(ThemeSwitchAIO.ids.switch("theme"), "value")
)
def update_theme(toggle):
    return "light" if toggle else "dark"

@callback(
    [Output("timeseries-fields-dropdown", "options"),
     Output("timeseries-fields-dropdown", "value"),
     Output("map-field-dropdown", "options"),
     Output("map-field-dropdown", "value"),
     Output("correlation-x-dropdown", "options"),
     Output("correlation-y-dropdown", "options")],
    Input("interval-component", "n_intervals"),
    [State("timeseries-fields-dropdown", "value"),
     State("map-field-dropdown", "value")]
)
def update_dropdown_options(n, ts_value, map_value):
    """Single callback to update all dropdown options with proper defaults"""
    fields = get_available_fields()
    options = [{"label": field, "value": field} for field in fields]
    
    # Default values from original app
    ts_default = ["rho_ppb", "ph_corrected_ma"]
    map_default = "rho_ppb"
    
    # Validate timeseries value
    if (
        not ts_value
        or not isinstance(ts_value, list)
        or not any(val in fields for val in ts_value)
    ):
        ts_value_out = [val for val in ts_default if val in fields]
    else:
        # Only keep values that are still valid
        ts_value_out = [val for val in ts_value if val in fields]
        if not ts_value_out:
            ts_value_out = [val for val in ts_default if val in fields]
    
    # Validate map value
    if not map_value or map_value not in fields:
        map_value_out = (
            map_default
            if map_default in fields
            else (fields[0] if fields else None)
        )
    else:
        map_value_out = map_value
    
    return options, ts_value_out, options, map_value_out, options, options

@callback(
    Output("resample-dropdown", "value"),
    Input("time-range-mode", "value"),
    State("resample-dropdown", "value")
)
def auto_adjust_resample(time_range_mode, current_resample):
    """Auto-adjust resample frequency based on time range"""
    resample_map = {0: "1min", 1: "1min", 2: "1min", 3: "10s", 4: "None", 5: "None"}
    return resample_map.get(time_range_mode, "1min")

@callback(
    [Output("timeseries-plot", "figure"),
     Output("map-plot", "figure"),
     Output("all-fields-timeseries-plot", "figure")],
    [Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input("interval-component", "n_intervals"),
     Input("timeseries-fields-dropdown", "value"),
     Input("map-field-dropdown", "value"),
     Input("resample-dropdown", "value"),
     Input("time-range-mode", "value"),
     Input("auto-update-toggle", "value"),
     Input("show-drifters-toggle", "value")] if data_manager.spot_api else
    [Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input("interval-component", "n_intervals"),
     Input("timeseries-fields-dropdown", "value"),
     Input("map-field-dropdown", "value"),
     Input("resample-dropdown", "value"),
     Input("time-range-mode", "value"),
     Input("auto-update-toggle", "value")]
)
def update_main_plots(toggle, n_intervals, ts_fields, map_field, resample_freq, 
                     time_range_mode, auto_update, show_drifters=False):
    """Main plots callback - simplified and optimized"""
    template = light_theme if toggle else dark_theme
    
    # Get filtered data using cache
    data = get_filtered_data(time_range_mode, auto_update, resample_freq, n_intervals)
    
    if data.empty:
        empty_fig = {"data": [], "layout": {"template": template}}
        return empty_fig, empty_fig, empty_fig
    
    # Get drifter data if enabled
    drifter_data = None
    if show_drifters and data_manager.spot_api:
        # Get drifter data for the same time range as the main data
        if not data.empty:
            start_time = data["datetime_utc"].min()
            end_time = data["datetime_utc"].max()
            drifter_data = data_manager.get_drifter_data(start_time=start_time, end_time=end_time)
    
    # Create plots
    ts_fig = create_timeseries_plot(data, ts_fields or [], template=template)
    
    # For map uirevision, create a stable ID based on main data characteristics
    # This ensures drifter updates don't reset the map view
    if auto_update:
        # Use main data hash for stable uirevision when auto-updating
        # This way, only changes to the main data will reset the map view
        main_data_signature = f"{len(data)}-{data['datetime_utc'].max().isoformat() if not data.empty and 'datetime_utc' in data.columns else 'empty'}"
        map_uirevision = f"map-auto-{main_data_signature}"
    else:
        map_uirevision = "map-constant"
    
    map_fig = create_map_plot(data, map_field, template=template, 
                             drifter_data=drifter_data, show_drifters=show_drifters,
                             uirevision=map_uirevision)
    
    # All fields plot
    exclude = ["datetime_utc", "index", "id", "partition"]
    all_fields = [col for col in data.columns if col not in exclude]
    all_ts_fig = create_timeseries_plot(data, all_fields, template=template)
    
    # Set uirevision for timeseries plots (can be different from map)
    ts_uirevision = f"ts-auto-{len(data)}" if auto_update else "ts-constant"
    
    for fig in [ts_fig, all_ts_fig]:
        if fig and hasattr(fig, 'update_layout'):
            fig.update_layout(uirevision=ts_uirevision, transition={"duration": 100})
    
    return ts_fig, map_fig, all_ts_fig

@callback(
    [Output("timeseries-plot-dispersal", "figure"),
     Output("map-plot-dispersal", "figure")],
    [Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input("interval-component", "n_intervals"),
     Input("map-field-dropdown", "value"),
     Input("resample-dropdown", "value"),
     Input("time-range-mode", "value"),
     Input("auto-update-toggle", "value"),
     Input("show-drifters-toggle", "value")] if data_manager.spot_api else
    [Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input("interval-component", "n_intervals"),
     Input("map-field-dropdown", "value"),
     Input("resample-dropdown", "value"),
     Input("time-range-mode", "value"),
     Input("auto-update-toggle", "value")]
)
def update_dispersal_plots(toggle, n_intervals, map_field, resample_freq, 
                          time_range_mode, auto_update, show_drifters=False):
    """Dispersal view plots"""
    template = light_theme if toggle else dark_theme
    
    data = get_filtered_data(time_range_mode, auto_update, resample_freq, n_intervals)
    
    if data.empty:
        empty_fig = {"data": [], "layout": {"template": template}}
        return empty_fig, empty_fig
    
    # Get drifter data if enabled
    drifter_data = None
    if show_drifters and data_manager.spot_api:
        # Get drifter data for the same time range as the main data
        if not data.empty:
            start_time = data["datetime_utc"].min()
            end_time = data["datetime_utc"].max()
            drifter_data = data_manager.get_drifter_data(start_time=start_time, end_time=end_time)
    
    dispersal_fig = create_dispersal_plot(data, template=template)
    
    # Use stable uirevision for map to preserve pan/zoom when only drifter data changes
    if auto_update:
        main_data_signature = f"{len(data)}-{data['datetime_utc'].max().isoformat() if not data.empty and 'datetime_utc' in data.columns else 'empty'}"
        map_uirevision = f"dispersal-map-auto-{main_data_signature}"
    else:
        map_uirevision = "dispersal-map-constant"
    
    map_fig = create_map_plot(data, map_field, template=template,
                             drifter_data=drifter_data, show_drifters=show_drifters,
                             uirevision=map_uirevision)
    
    # Use separate uirevision for dispersal plot
    dispersal_uirevision = f"dispersal-{len(data)}" if auto_update else "dispersal-constant"
    
    if dispersal_fig and hasattr(dispersal_fig, 'update_layout'):
        dispersal_fig.update_layout(uirevision=dispersal_uirevision, transition={"duration": 100})
    
    return dispersal_fig, map_fig

@callback(
    [Output("correlation-scatterplot", "figure"),
     Output("bland-altman-plot", "figure")],
    [Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input("interval-component", "n_intervals"),
     Input("correlation-x-dropdown", "value"),
     Input("correlation-y-dropdown", "value"),
     Input("resample-dropdown", "value"),
     Input("time-range-mode", "value"),
     Input("auto-update-toggle", "value")]
)
def update_correlation_plots(toggle, n_intervals, x_col, y_col, resample_freq,
                           time_range_mode, auto_update):
    """Correlation plots"""
    template = light_theme if toggle else dark_theme
    
    if not x_col or not y_col:
        empty_fig = {"data": [], "layout": {"template": template}}
        return empty_fig, empty_fig
    
    data = get_filtered_data(time_range_mode, auto_update, resample_freq, n_intervals)
    
    if data.empty:
        empty_fig = {"data": [], "layout": {"template": template}}
        return empty_fig, empty_fig
    
    corr_fig = create_correlation_plot(data, x_col, y_col, template=template)
    ba_fig = create_bland_altman_plot(data, x_col, y_col, template=template)
    
    uirevision = f"correlation-{len(data)}" if auto_update else "correlation-constant"
    
    for fig in [corr_fig, ba_fig]:
        if fig and hasattr(fig, 'update_layout'):
            fig.update_layout(uirevision=uirevision, transition={"duration": 100})
    
    return corr_fig, ba_fig

@callback(
    [Output("ph-value", "children"),
     Output("ph-value", "style"),
     Output("rho-value", "children"),
     Output("last-update-display", "children"),
     Output("most-recent-timestamp-display", "children"),
     Output("total-rows-all-data", "children"),
     Output("total-rows-filtered", "children"),
     Output("missing-rows-all-data", "children"),
     Output("drifter-status-display", "children")] if data_manager.spot_api else
    [Output("ph-value", "children"),
     Output("rho-value", "children"),
     Output("last-update-display", "children"),
     Output("most-recent-timestamp-display", "children"),
     Output("total-rows-all-data", "children"),
     Output("total-rows-filtered", "children"),
     Output("missing-rows-all-data", "children")],
    [Input("interval-component", "n_intervals"),
     Input("time-range-mode", "value"),
     Input("resample-dropdown", "value"),
     Input("auto-update-toggle", "value")]
)
def update_status_info(n_intervals, time_range_mode, resample_freq, auto_update):
    """Update status information including comprehensive statistics"""
    ph_val = "No Data"
    ph_style = {"fontSize": "2.5rem"}  # Default style
    rho_val = "No Data"
    
    if not data_manager.data.empty:
        # pH value from latest data
        if "ph_corrected_ma" in data_manager.data.columns:
            latest_ph = data_manager.data["ph_corrected_ma"].dropna()
            if not latest_ph.empty:
                ph_value = latest_ph.iloc[-1]
                ph_val = f"{ph_value:.2f}"
                
                # Add threshold-based coloring and animation
                if ph_value > 10:  # Critical threshold - red, bold, and bright
                    ph_style = {
                        "fontSize": "2.5rem", 
                        "color": "red",
                        "fontWeight": "bold",
                        "filter": "brightness(1.5)"
                    }
                elif ph_value > 8.7:  # Warning threshold - red only
                    ph_style = {"fontSize": "2.5rem", "color": "red"}
                else:  # Normal range - default color
                    ph_style = {"fontSize": "2.5rem"}
        
        # Rho value from latest data
        if "rho_ppb" in data_manager.data.columns:
            latest_rho = data_manager.data["rho_ppb"].dropna()
            if not latest_rho.empty:
                rho_val = f"{latest_rho.iloc[-1]:.1f}"
    
    # Current time for last update
    current_time = datetime.now().strftime("%H:%M:%S")
    
    # Most recent timestamp from data
    most_recent_timestamp = (
        data_manager.data["datetime_utc"].max() if not data_manager.data.empty else None
    )
    most_recent_timestamp_display = (
        pd.to_datetime(most_recent_timestamp).strftime("%Y-%m-%d %H:%M:%S") 
        if most_recent_timestamp else "N/A"
    )
    
    # Total rows in all data
    total_rows_all_data = len(data_manager.data)
    
    # Calculate missing rows (rows with any missing data)
    missing_rows_all_data = data_manager.data.isnull().any(axis=1).sum()
    
    # Get filtered data count (using same logic as get_filtered_data)
    try:
        filtered_data = get_filtered_data(time_range_mode, auto_update, resample_freq, n_intervals)
        total_rows_filtered = len(filtered_data)
    except Exception as e:
        logger.error(f"Error getting filtered data count: {e}")
        total_rows_filtered = 0
    
    # Drifter status with asset information
    drifter_status = "N/A"
    if data_manager.spot_api:
        drifter_info = data_manager.get_drifter_info()
        if drifter_info["status"] == "Active":
            asset_count = drifter_info.get("unique_assets", 0)
            total_positions = drifter_info["total_positions"]
            
            if asset_count > 1:
                drifter_status = f"{asset_count} drifters, {total_positions} positions"
            elif asset_count == 1:
                drifter_status = f"1 drifter, {total_positions} positions"
            else:
                drifter_status = f"{total_positions} positions"
        else:
            drifter_status = "No data"
    
    if data_manager.spot_api:
        return (
            ph_val, 
            ph_style,
            rho_val, 
            current_time,
            most_recent_timestamp_display,
            str(total_rows_all_data),
            str(total_rows_filtered),
            str(missing_rows_all_data),
            drifter_status
        )
    else:
        return (
            ph_val, 
            ph_style,
            rho_val, 
            current_time,
            most_recent_timestamp_display,
            str(total_rows_all_data),
            str(total_rows_filtered),
            str(missing_rows_all_data)
        )

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8050))
    logger.info("Starting optimized LOCNESS Dash app on port %d", port)
    app.run(debug=True, host="0.0.0.0", port=port)
