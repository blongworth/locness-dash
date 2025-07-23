import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import time


def create_sample_database(db_path="underway_data.db"):
    """Create sample SQLite database with ship underway data"""

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table with the exact schema provided
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS underway_summary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        datetime_utc INTEGER NOT NULL UNIQUE,
        latitude REAL,
        longitude REAL,
        rho_ppb REAL,
        ph_total REAL,
        vrse REAL,
        ph_corrected REAL,
        temp REAL,
        salinity REAL,
        ph_corrected_ma REAL,
        ph_total_ma REAL
    )
    """)

    # Generate sample data for a ship track
    start_time = datetime.now() - timedelta(days=7)  # 7 days of data
    end_time = datetime.now()

    # Define a realistic ship track (e.g., transatlantic route)
    # Starting from New York area, moving towards Europe
    start_lat, start_lon = 40.7128, -74.0060  # New York
    end_lat, end_lon = 51.5074, -0.1278  # London

    # Generate time series data
    current_time = start_time
    data_points = []
    total_duration = (end_time - start_time).total_seconds()

    while current_time <= end_time:
        # Calculate ship position along the route
        progress = (current_time - start_time).total_seconds() / total_duration

        # Add some realistic ship movement with slight randomness
        base_lat = start_lat + (end_lat - start_lat) * progress + random.gauss(0, 0.01)
        base_lon = start_lon + (end_lon - start_lon) * progress + random.gauss(0, 0.01)

        # Generate realistic marine chemistry data
        # Temperature varies with latitude and season
        temp_base = (
            15 + 10 * np.cos(np.radians(abs(base_lat) - 40)) + random.gauss(0, 1)
        )

        # Salinity - typical ocean values
        salinity = 34.5 + random.gauss(0, 0.5)  # Practical Salinity Units

        # pH values - typical ocean chemistry
        ph_total_base = 8.1 + random.gauss(0, 0.05)
        ph_corrected_base = ph_total_base + random.gauss(0, 0.02)

        # Moving averages (simulate some smoothing)
        ph_total_ma = ph_total_base + random.gauss(0, 0.02)
        ph_corrected_ma = ph_corrected_base + random.gauss(0, 0.02)

        # VRSE (Voltage Reference Standard Electrode)
        vrse = 0.4 + random.gauss(0, 0.01)

        # Rho (density-related parameter) in ppb
        rho_ppb = 1025000 + random.gauss(0, 100)  # Ocean water density ~ 1025 kg/m³

        data_points.append(
            {
                "datetime_utc": int(current_time.timestamp()),  # Unix timestamp
                "latitude": round(base_lat, 6),
                "longitude": round(base_lon, 6),
                "rho_ppb": round(rho_ppb, 2),
                "ph_total": round(ph_total_base, 4),
                "vrse": round(vrse, 4),
                "ph_corrected": round(ph_corrected_base, 4),
                "temp": round(temp_base, 2),
                "salinity": round(salinity, 3),
                "ph_corrected_ma": round(ph_corrected_ma, 4),
                "ph_total_ma": round(ph_total_ma, 4),
            }
        )

        current_time += timedelta(seconds=10)  # 10-second intervals

    # Insert data into database
    df = pd.DataFrame(data_points)
    df.to_sql("underway_summary", conn, if_exists="append", index=False)

    print(f"Created sample ship database with {len(data_points)} data points")
    print(f"Time range: {start_time} to {end_time}")
    print(f"Ship track: {start_lat:.4f},{start_lon:.4f} to {end_lat:.4f},{end_lon:.4f}")

    # Print sample data info
    print("\nSample data ranges:")
    for col in ["temp", "salinity", "ph_total", "ph_corrected", "rho_ppb"]:
        values = df[col]
        print(f"  {col}: {values.min():.3f} to {values.max():.3f}")

    conn.close()


def add_new_data_point(db_path="underway_data.db"):
    """Add a new data point (for simulating real-time ship data)"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get the latest timestamp and position
    cursor.execute(
        "SELECT MAX(datetime_utc), latitude, longitude FROM underway_summary"
    )
    result = cursor.fetchone()

    if result[0]:
        last_timestamp = result[0]
        last_lat = result[1] if result[1] else 40.7128
        last_lon = result[2] if result[2] else -74.0060
        if last_timestamp > int(datetime.now().timestamp()):
            new_timestamp = last_timestamp + 1  # Add 1 second
        else:
            new_timestamp = int(datetime.now().timestamp())
    else:
        new_timestamp = int(datetime.now().timestamp())
        last_lat = 40.7128
        last_lon = -74.0060

    # Simulate ship movement (small incremental change)
    new_lat = last_lat + random.gauss(0, 0.002)  # Small movement
    new_lon = last_lon + random.gauss(0.01, 0.005)  # Slightly eastward movement

    # Generate new readings based on position
    temp = 15 + 10 * np.cos(np.radians(abs(new_lat) - 40)) + random.gauss(0, 1)
    salinity = 34.5 + random.gauss(0, 0.5)
    ph_total = 8.1 + random.gauss(0, 0.05)
    ph_corrected = ph_total + random.gauss(0, 0.02)
    ph_total_ma = ph_total + random.gauss(0, 0.02)
    ph_corrected_ma = ph_corrected + random.gauss(0, 0.02)
    vrse = 0.4 + random.gauss(0, 0.01)
    rho_ppb = 1025000 + random.gauss(0, 100)

    cursor.execute(
        """
    INSERT INTO underway_summary 
    (datetime_utc, latitude, longitude, rho_ppb, ph_total, vrse, ph_corrected, 
     temp, salinity, ph_corrected_ma, ph_total_ma)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            new_timestamp,
            round(new_lat, 6),
            round(new_lon, 6),
            round(rho_ppb, 2),
            round(ph_total, 4),
            round(vrse, 4),
            round(ph_corrected, 4),
            round(temp, 2),
            round(salinity, 3),
            round(ph_corrected_ma, 4),
            round(ph_total_ma, 4),
        ),
    )

    conn.commit()
    conn.close()

    new_time = datetime.fromtimestamp(new_timestamp)
    print(
        f"Added new ship data point at {new_time} - Lat: {new_lat:.4f}, Lon: {new_lon:.4f}"
    )


def continuous_data_simulation(db_path="underway_data.db", interval_seconds=10):
    """Continuously add new data points to simulate real-time ship data"""
    print(f"Starting continuous data simulation (every {interval_seconds} seconds)")
    print("Press Ctrl+C to stop")

    try:
        while True:
            add_new_data_point(db_path)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("\nStopping data simulation")


if __name__ == "__main__":
    # Create initial sample database
    create_sample_database()

    print("\nTo start continuous data simulation, uncomment the line below:")
    print("# continuous_data_simulation()")

    # Uncomment the following line to start continuous data simulation
    continuous_data_simulation()
