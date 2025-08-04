from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.express as px
import numpy as np


def create_timeseries_plot(data, fields, subplot_height=200, template="bootstrap"):
    """Create timeseries plot with subplots for multiple fields"""
    if data.empty or not fields:
        fig = go.Figure()
        return fig

    n_fields = len(fields)

    fig = make_subplots(
        rows=n_fields,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
    )

    for i, field in enumerate(fields):
        if field in data.columns:
            fig.add_trace(
                go.Scatter(
                    x=data["datetime_utc"],
                    y=data[field],
                    mode="lines+markers",
                    name=field,
                    line=dict(width=2),
                    marker=dict(size=4),
                ),
                row=i + 1,
                col=1,
            )
            # Set y axis title for each subplot
            fig.update_yaxes(title_text=field, row=i + 1, col=1)

    fig.update_layout(
        template=template,
        height=150 + subplot_height * len(fields),
        showlegend=False,
        # margin=dict(t=10, b=10, l=10, r=10)  # Adjust margins to reduce whitespace
    )
    for i in range(1, len(fields) + 1):
        fig.update_xaxes(
            title_text="",
            row=i,
            col=1,
            rangeselector=dict(
                buttons=list(
                    [
                        dict(count=1, label="1h", step="hour", stepmode="backward"),
                        dict(count=6, label="6h", step="hour", stepmode="backward"),
                        dict(count=12, label="12h", step="hour", stepmode="backward"),
                        dict(count=1, label="1d", step="day", stepmode="backward"),
                        dict(step="all"),
                    ]
                )
            )
            if i == 1
            else None,
            rangeslider=dict(
                visible=False
            ),  # Disable rangeslider to prevent compression issues
            type="date",
        )
    return fig


def create_dispersal_plot(data, template="bootstrap"):
    dispersal_fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1
    )

    if "ph_corrected" in data.columns and "ph_corrected_ma" in data.columns:
        dispersal_fig.add_trace(
            go.Scatter(
                x=data["datetime_utc"],
                y=data["ph_corrected"],
                mode="lines",
                name="pH",
                line=dict(color="lightblue"),
            ),
            row=1,
            col=1,
        )
        dispersal_fig.add_trace(
            go.Scatter(
                x=data["datetime_utc"],
                y=data["ph_corrected_ma"],
                mode="lines",
                name="pH (2 min avg)",
                line=dict(color="blue"),
            ),
            row=1,
            col=1,
        )

    if "rho_ppb" in data.columns:
        dispersal_fig.add_trace(
            go.Scatter(
                x=data["datetime_utc"],
                y=data["rho_ppb"],
                mode="lines",
                name="Rho (ppb)",
                line=dict(color="red"),
                showlegend=False,  # Hide legend for rho_ppb
            ),
            row=2,
            col=1,
        )

    dispersal_fig.update_layout(
        template=template,
        title="Dispersal View Timeseries",
        uirevision="dispersal-timeseries-constant",
        transition={"duration": 100},
        yaxis1_title="pH",  # Label for pH subplot
        yaxis2_title="Rho [ppb]",  # Label for Rho subplot
        legend=dict(
            x=1,  # Move legend back to right edge
            y=1,
            xanchor="right",
            yanchor="top",
            bgcolor="rgba(0,0,0,0)",  # Make the background transparent
        ),
        margin=dict(t=50, b=50, l=50, r=50),  # Standard margins
    )

    # Update x-axes to add padding and prevent edge collision
    #dispersal_fig.update_xaxes(
    #    row=1, col=1, rangeslider=dict(visible=False), type="date", automargin=True
    #)
    #dispersal_fig.update_xaxes(
    #    row=2, col=1, rangeslider=dict(visible=False), type="date", automargin=True
    #)

    # Add some padding to prevent data from touching the edges
    #if not data.empty and "datetime_utc" in data.columns:
    #    time_range = data["datetime_utc"].max() - data["datetime_utc"].min()
    #    padding = time_range * 0.02  # 2% padding on each side
    #    dispersal_fig.update_xaxes(
    #        range=[
    #            data["datetime_utc"].min() - padding,
    #            data["datetime_utc"].max() + padding,
    #        ]
    #    )

    return dispersal_fig


def create_map_plot(df, field, template="bootstrap", style=None):
    """
    Create a map plot with optional style.
    style: 'dark' (default) or 'bathymetry'.
    """
    if df.empty:
        return go.Figure()
    track_data = df

    if style is None:
        if template == "darkly":
            style = "dark"
        else:
            style = "light"

    # if style not in ['white-bg', 'light', 'dark', 'carto-darkmatter', 'carto-positron']:
    #     style = 'dark'  # Default to dark if unsupported style is provided

    # Calculate zoom and center BEFORE creating the map figure
    if not track_data.empty:
        min_lat = track_data["latitude"].min()
        max_lat = track_data["latitude"].max()
        min_lon = track_data["longitude"].min()
        max_lon = track_data["longitude"].max()
        center_lat = (min_lat + max_lat) / 2
        center_lon = (min_lon + max_lon) / 2
        lat_range = max_lat - min_lat
        lon_range = max_lon - min_lon
        max_range = max(lat_range, lon_range)
        if max_range < 0.002:
            zoom = 15
        elif max_range < 0.01:
            zoom = 13
        elif max_range < 0.05:
            zoom = 11
        elif max_range < 0.2:
            zoom = 9
        else:
            zoom = 7
    else:
        center_lat, center_lon = 42.3601, -71.0589
        zoom = 12

    fig = go.Figure()
    if field and field in track_data.columns:
        color_param = field
        color_vals = track_data[color_param]

        # For skewed data, use a more sophisticated quantile approach
        # Define quantile breakpoints to spread colors more evenly
        quantiles = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
        q_values = [color_vals.quantile(q) for q in quantiles]

        # Use log scale for highly skewed data if the range is large and all values are positive
        is_log_suitable = (
            color_vals.min() > 0 and (color_vals.max() / color_vals.min()) > 10
        )

        if is_log_suitable:
            # Use log-transformed values for color mapping
            color_for_map = np.log10(
                color_vals + 1e-10
            )  # Add small epsilon to avoid log(0)
            qmin = np.log10(color_vals.quantile(0.01) + 1e-10)
            qmax = np.log10(color_vals.quantile(0.99) + 1e-10)

            # Create custom colorbar with original scale labels
            tickvals = np.log10(np.array(q_values) + 1e-10)
            ticktext = [f"{v:.2f}" for v in q_values]

            colorbar_dict = dict(
                title=color_param.capitalize(), tickvals=tickvals, ticktext=ticktext
            )
        else:
            # Use percentile-based scaling for better color distribution
            color_for_map = color_vals
            qmin = color_vals.quantile(0.01)
            qmax = color_vals.quantile(0.99)
            colorbar_dict = dict(title=color_param.capitalize())

        if qmin == qmax:
            qmin = color_vals.min()
            qmax = color_vals.max()
            color_for_map = color_vals
            colorbar_dict = dict(title=color_param.capitalize())

        scatter = go.Scattermap(
            lat=track_data["latitude"],
            lon=track_data["longitude"],
            mode="markers+lines",
            marker=dict(
                size=10,
                color=color_for_map,
                colorscale="Viridis",
                cmin=qmin,
                cmax=qmax,
                colorbar=colorbar_dict,
                showscale=True,
            ),
            name=f"Track ({color_param})",
            text=[f"{color_param}: {v:.2f}" for v in color_vals],
            hovertemplate="Lat: %{latitude:.4f}<br>"
            + "Lon: %{longitude:.4f}<br>"
            + f"{color_param}: %{{marker.color:.2f}}<extra></extra>",
        )
        fig.add_trace(scatter)
    else:
        fig.add_trace(
            go.Scattermap(
                lat=track_data["latitude"],
                lon=track_data["longitude"],
                mode="lines",
                line=dict(width=2, color="blue"),
                name="Track",
                hoverinfo="skip",
            )
        )
    if not df.empty:
        latest = df.iloc[-1]
        fig.add_trace(
            go.Scattermap(
                lat=[latest["latitude"]],
                lon=[latest["longitude"]],
                mode="markers",
                marker=dict(size=15, color="red"),
                name="Current Position",
                hovertemplate="<b>Current Position</b><br>"
                + "Lat: %{latitude:.4f}<br>"
                + "Lon: %{longitude:.4f}<br>"
                + f"Rho: {latest['rho_ppb']:.1f} ppb<br>"
                + f"Average pH: {latest['ph_corrected_ma']:.2f}<extra></extra>",
            )
        )

    fig.update_layout(
        template=template,
        map=dict(style=style, center=dict(lat=center_lat, lon=center_lon), zoom=zoom),
        margin=dict(t=10, b=10, l=10, r=10),  # Adjust margins to reduce whitespace
        legend=dict(
            x=0.99,
            y=0.99,
            xanchor="right",
            yanchor="top",
            bgcolor="rgba(255,255,255,0.7)",
            bordercolor="rgba(0,0,0,0.2)",
            borderwidth=1,
            font=dict(size=13),
        ),
    )
    return fig


def create_correlation_plot(data, x_col, y_col, template="bootstrap"):
    if (
        data is None
        or data.empty
        or x_col not in data.columns
        or y_col not in data.columns
    ):
        return go.Figure()
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=data[x_col],
            y=data[y_col],
            mode="markers",
            marker=dict(size=8, line=dict(width=1, color="DarkSlateGrey"), opacity=0.7),
            name=f"{x_col} vs {y_col}",
        )
    )
    fig.update_layout(
        template=template,
        title=f"Correlation: {x_col} vs {y_col}",
        xaxis_title=x_col,
        yaxis_title=y_col,
        margin=dict(l=40, r=20, t=40, b=40),
    )
    return fig


def create_bland_altman_plot(data, col1, col2, template="bootstrap"):
    """
    Create a Bland-Altman plot comparing two columns in the DataFrame.
    Plots the difference (col1 - col2) vs the mean of the two columns.
    """
    if (
        data is None
        or data.empty
        or col1 not in data.columns
        or col2 not in data.columns
    ):
        return {}
    x = data[[col1, col2]].dropna()
    mean_vals = x[[col1, col2]].mean(axis=1)
    diff_vals = x[col1] - x[col2]
    mean_diff = diff_vals.mean()
    std_diff = diff_vals.std()
    upper = mean_diff + 1.96 * std_diff
    lower = mean_diff - 1.96 * std_diff

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=x[col1],
            y=diff_vals,
            mode="markers",
            name="Difference",
            marker=dict(size=8, color="blue", opacity=0.7),
        )
    )
    # Add mean line
    fig.add_hline(
        y=mean_diff,
        line_dash="dash",
        line_color="green",
        annotation_text="Mean",
        annotation_position="top left",
    )
    # Add upper and lower lines
    fig.add_hline(
        y=upper,
        line_dash="dot",
        line_color="red",
        annotation_text="+1.96 SD",
        annotation_position="top left",
    )
    fig.add_hline(
        y=lower,
        line_dash="dot",
        line_color="red",
        annotation_text="-1.96 SD",
        annotation_position="bottom left",
    )

    fig.update_layout(
        template=template,
        title=f"Difference Plot: {col1} vs {col2}",
        xaxis_title=f"{col1}",
        yaxis_title=f"Difference ({col1} - {col2})",
        margin=dict(l=40, r=20, t=40, b=40),
    )
    return fig

