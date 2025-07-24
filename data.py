import pandas as pd
import sqlite3
import threading

class DataManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.last_datetime_utc = None
        self.data = pd.DataFrame()
        self.lock = threading.Lock()
        self.is_parquet = db_path.endswith(".parquet")

    def get_connection(self):
        if self.is_parquet:
            raise ValueError("Parquet does not use a database connection.")
        return sqlite3.connect(self.db_path)

    def load_initial_data(self):
        """Load all existing data from the data source"""
        with self.lock:
            try:
                if self.is_parquet:
                    self.data = pd.read_parquet(self.db_path, engine="pyarrow")
                else:
                    conn = self.get_connection()
                    query = "SELECT * FROM underway_summary ORDER BY datetime_utc"
                    self.data = pd.read_sql_query(query, conn)
                    conn.close()

                if not self.data.empty:
                    # Convert Unix timestamp to datetime
                    self.data["timestamp"] = pd.to_datetime(
                        self.data["datetime_utc"], unit="s"
                    )
                    self.last_datetime_utc = self.data["datetime_utc"].max()

            except Exception as e:
                print(f"Error loading initial data: {e}")
                self.data = pd.DataFrame()

    def get_new_data(self):
        """Get data newer than last_datetime_utc"""
        if self.last_datetime_utc is None:
            return pd.DataFrame()

        try:
            if self.is_parquet:
                # Filter Parquet data for new entries
                new_data = pd.read_parquet(self.db_path, engine="pyarrow")
                new_data = new_data[new_data["datetime_utc"] > self.last_datetime_utc]
            else:
                conn = self.get_connection()
                query = """
                SELECT * FROM underway_summary 
                WHERE datetime_utc > ? 
                ORDER BY datetime_utc
                """
                new_data = pd.read_sql_query(query, conn, params=(self.last_datetime_utc,))
                conn.close()

            if not new_data.empty:
                # Convert Unix timestamp to datetime
                new_data["timestamp"] = pd.to_datetime(
                    new_data["datetime_utc"], unit="s"
                )

                with self.lock:
                    self.data = pd.concat([self.data, new_data], ignore_index=True)
                    self.last_datetime_utc = new_data["datetime_utc"].max()

                return new_data
        except Exception as e:
            print(f"Error getting new data: {e}")

        return pd.DataFrame()

    def get_data(self, start_time=None, end_time=None, resample_freq=None):
        """Get data with optional time filtering and resampling"""
        with self.lock:
            data = self.data.copy()

        if data.empty:
            return data

        # Filter by time range
        if start_time:
            data = data[data["timestamp"] >= start_time]
        if end_time:
            data = data[data["timestamp"] <= end_time]

        # Remove 'partition' column if it exists
        if "partition" in data.columns:
            data = data.drop(columns=["partition"])

        # Resample if requested
        if resample_freq and not data.empty:
            data.set_index("timestamp", inplace=True)
            resampled_data = data.resample(resample_freq).mean()
            resampled_data.reset_index(inplace=True)
            data = resampled_data

        return data
