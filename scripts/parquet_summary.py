import pandas as pd
import sys

def display_parquet_summary(data_path):
    try:
        # Load the Parquet dataset
        data = pd.read_parquet(data_path, engine="pyarrow")

        # Display summary information
        print("Dataset Summary:")
        print(f"Number of rows: {len(data)}")
        print(f"Number of columns: {len(data.columns)}")
        print("Column Names:")
        print(data.columns.tolist())
        print("\nData Types:")
        print(data.dtypes)
        print("\nFirst 5 Rows:")
        print(data.head())
        print("\nLast 5 Rows:")
        print(data.tail())
        print("\nFirst rows by datetime_utc:")
        print(data.sort_values("datetime_utc").head())
        print("\nLast rows by datetime_utc:")
        print(data.sort_values("datetime_utc").tail())
    except Exception as e:
        print(f"Error loading Parquet file: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_parquet_summary.py <path_to_parquet_file>")
    else:
        data_path = sys.argv[1]
        display_parquet_summary(data_path)
