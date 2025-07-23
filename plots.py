import plotly.graph_objects as go
from plotly.subplots import make_subplots

def create_timeseries_plot(data, fields):
    """Create timeseries plot with subplots for multiple fields"""
    if data.empty or not fields:
        fig = go.Figure()
        fig.update_layout(
            title="Timeseries Plot", xaxis_title="Time", yaxis_title="Value", height=500
        )
        return fig

    n_fields = len(fields)
    fig = make_subplots(
        rows=n_fields,
        cols=1,
        shared_xaxes=True,
        subplot_titles=fields,
        vertical_spacing=0.1,
    )

    for i, field in enumerate(fields):
        if field in data.columns:
            fig.add_trace(
                go.Scatter(
                    x=data["timestamp"],
                    y=data[field],
                    mode="lines+markers",
                    name=field,
                    line=dict(width=2),
                    marker=dict(size=4),
                ),
                row=i + 1,
                col=1,
            )

    fig.update_layout(
        height=500,
        title="Marine Data Timeseries",
        xaxis=dict(
            rangeslider=dict(visible=True),
            rangeselector=dict(
                buttons=list(
                    [
                        dict(count=1, label="1h", step="hour", stepmode="backward"),
                        dict(count=6, label="6h", step="hour", stepmode="backward"),
                        dict(count=1, label="1d", step="day", stepmode="backward"),
                        dict(count=7, label="7d", step="day", stepmode="backward"),
                        dict(step="all"),
                    ]
                )
            ),
        ),
        showlegend=True,
    )

    return fig

def create_map_plot(data, field):
    """Create ship track map plot"""
    if (
        data.empty
        or not field
        or "latitude" not in data.columns
        or "longitude" not in data.columns
    ):
        fig = go.Figure(go.Scattermapbox())
        fig.update_layout(
            mapbox=dict(
                style="open-street-map", center=dict(lat=40.7128, lon=-74.0060), zoom=5
            ),
            title="Ship Track",
            height=500,
        )
        return fig

    map_data = data.dropna(subset=["latitude", "longitude", field])

    if map_data.empty:
        fig = go.Figure(go.Scattermapbox())
        fig.update_layout(
            mapbox=dict(
                style="open-street-map", center=dict(lat=40.7128, lon=-74.0060), zoom=5
            ),
            title="Ship Track",
            height=500,
        )
        return fig

    fig = go.Figure()
    fig.add_trace(
        go.Scattermapbox(
            lat=map_data["latitude"],
            lon=map_data["longitude"],
            mode="lines",
            line=dict(width=2, color="blue"),
            name="Ship Track",
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scattermapbox(
            lat=map_data["latitude"],
            lon=map_data["longitude"],
            mode="markers",
            marker=dict(
                size=8,
                color=map_data[field],
                colorscale="Viridis",
                showscale=True,
                colorbar=dict(title=field),
            ),
            text=[
                f"{field}: {val:.3f}<br>Time: {time}<br>Lat: {lat:.4f}<br>Lon: {lon:.4f}"
                for val, time, lat, lon in zip(
                    map_data[field],
                    map_data["timestamp"],
                    map_data["latitude"],
                    map_data["longitude"],
                )
            ],
            hovertemplate="<b>%{text}</b><extra></extra>",
            name=f"{field} Values",
        )
    )
    center_lat = map_data["latitude"].mean()
    center_lon = map_data["longitude"].mean()
    lat_range = map_data["latitude"].max() - map_data["latitude"].min()
    lon_range = map_data["longitude"].max() - map_data["longitude"].min()
    max_range = max(lat_range, lon_range)
    if max_range > 10:
        zoom = 3
    elif max_range > 5:
        zoom = 4
    elif max_range > 1:
        zoom = 6
    elif max_range > 0.1:
        zoom = 8
    else:
        zoom = 10
    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=zoom,
        ),
        title=f"Ship Track - {field}",
        height=500,
        showlegend=True,
    )
    return fig
