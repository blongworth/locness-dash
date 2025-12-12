import sqlite3
import pandas as pd
import sys

def display_sqlite_summary(db_path, table_name = "underway_summary"):
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)

        # Load the table into a Pandas DataFrame
        query = f"SELECT * FROM {table_name}"
        data = pd.read_sql_query(query, conn)

        # Display summary information
        print("Dataset Summary:")
        print(f"Number of rows: {len(data)}")
        print(f"Number of columns: {len(data.columns)}")
        print("Column Names:")
        print(data.columns.tolist())
        print("\nData Types:")
        print(data.dtypes)

        # If datetime_utc column exists, convert it to pandas datetime and sort/display first/last rows
        if 'datetime_utc' in data.columns:
            data['datetime_utc'] = pd.to_datetime(data['datetime_utc'], unit='s')
            print("\nFirst 5 Rows:")
            print(data.head())
            print("\nLast 5 Rows:")
            print(data.tail())
            print("\nFirst rows by datetime_utc:")
            print(data.sort_values("datetime_utc").head())
            print("\nLast rows by datetime_utc:")
            print(data.sort_values("datetime_utc").tail())

        # Close the connection
        conn.close()
    except Exception as e:
        print(f"Error reading SQLite database: {e}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python sqlite_summary.py <path_to_sqlite_db>")
    else:
        db_path = sys.argv[1]
        display_sqlite_summary(db_path)

if __name__ == "__main__":
    main()
