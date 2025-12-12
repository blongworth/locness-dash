import sqlite3
import pandas as pd

# Path to your database (from config.toml)
db_path = "../locness-datamanager/data/locness.db"

# Connect to the SQLite database
conn = sqlite3.connect(db_path)

# Load the summary table into a DataFrame
df = pd.read_sql_query("SELECT * FROM underway_summary", conn)


# Convert 'datetime_utc' from Unix integer timestamp to pandas datetime if it exists
if 'datetime_utc' in df.columns:
    df['datetime_utc_pd'] = pd.to_datetime(df['datetime_utc'], unit='s', utc=True)

# Show the first few rows
print("First 5 rows:")
print(df.head())

# Show descriptive statistics
print("\nDescriptive statistics:")
print(df.describe(include='all'))

# Close the connection
conn.close()
