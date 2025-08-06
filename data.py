import pandas as pd
import sqlite3
import threading

class DataManager:
    def __init__(self, data_path):
        self.data_path = data_path
        self.last_datetime_utc = None  # This will store pandas datetime
        self.data = pd.DataFrame()
        self.lock = threading.Lock()
        self.is_parquet = data_path.endswith(".parquet")

    def get_connection(self):
        if self.is_parquet:
            raise ValueError("Parquet does not use a database connection.")
        return sqlite3.connect(self.data_path)

    def load_initial_data(self):
        """Load all existing data from the data source"""
        with self.lock:
            try:
                if self.is_parquet:
                    self.data = pd.read_parquet(self.data_path, engine="pyarrow")
                    # Convert Unix timestamp to datetime if needed
                    if "datetime_utc" in self.data.columns:
                        self.data["datetime_utc"] = pd.to_datetime(
                            self.data["datetime_utc"], unit="s"
                        )
                else:
                    conn = self.get_connection()
                    query = "SELECT * FROM underway_summary ORDER BY datetime_utc"
                    self.data = pd.read_sql_query(query, conn)
                    conn.close()
                    # Convert Unix timestamp to datetime after reading from SQLite
                    if "datetime_utc" in self.data.columns:
                        self.data["datetime_utc"] = pd.to_datetime(
                            self.data["datetime_utc"], unit="s"
                        )

                if not self.data.empty:
                    # Set timestamp column (same as datetime_utc for consistency)
                    self.last_datetime_utc = self.data["datetime_utc"].max()

            except Exception as e:
                print(f"Error loading initial data: {e}")
                self.data = pd.DataFrame()

    def get_new_data(self):
        """Get data newer than last_datetime_utc"""
        if self.last_datetime_utc is None:
            return pd.DataFrame()

        print(f"Fetching new data after {self.last_datetime_utc} (UTC timestamp)")
        try:
            if self.is_parquet:
                # Filter Parquet data for new entries
                new_data = pd.read_parquet(self.data_path, engine="pyarrow")
                # Convert Unix timestamp to datetime if needed
                if "datetime_utc" in new_data.columns and new_data["datetime_utc"].dtype != 'datetime64[ns]':
                    new_data["datetime_utc"] = pd.to_datetime(
                        new_data["datetime_utc"], unit="s"
                    )
                new_data = new_data[new_data["datetime_utc"] > self.last_datetime_utc]
            else:
                conn = self.get_connection()
                # Convert pandas datetime to Unix timestamp for SQLite query
                last_datetime_unix = int(self.last_datetime_utc.timestamp())
                query = """
                SELECT * FROM underway_summary 
                WHERE datetime_utc > ? 
                ORDER BY datetime_utc
                """
                new_data = pd.read_sql_query(query, conn, params=(last_datetime_unix,))
                conn.close()
                # Convert Unix timestamp to datetime after reading from SQLite
                if not new_data.empty and "datetime_utc" in new_data.columns:
                    new_data["datetime_utc"] = pd.to_datetime(
                        new_data["datetime_utc"], unit="s"
                    )

            if not new_data.empty:

                with self.lock:
                    self.data = pd.concat([self.data, new_data], ignore_index=True)
                    # Update last_datetime_utc to the maximum datetime in the new data
                    self.last_datetime_utc = new_data["datetime_utc"].max()

                print(f"DataManager: data shape after append: {self.data.shape}")
                print(f"DataManager: last_datetime_utc updated to {self.last_datetime_utc}")

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

        print(f"DataManager.get_data: Original data shape: {data.shape}")
        if not data.empty and "datetime_utc" in data.columns:
            print(f"DataManager.get_data: Data time range: {data['datetime_utc'].min()} to {data['datetime_utc'].max()}")
        print(f"DataManager.get_data: Filter start_time: {start_time}, end_time: {end_time}")

        # Add moving averages
        # TODO: Checkbox to turn on/off moving averages
        self.add_2min_moving_averages()

        # Filter by time range (start_time and end_time should be pandas datetimes)
        if start_time:
            # Ensure start_time is a pandas datetime
            if not isinstance(start_time, pd.Timestamp):
                start_time = pd.to_datetime(start_time)
            # Convert to timezone-naive if data is timezone-naive
            if start_time.tz is not None and data["datetime_utc"].dt.tz is None:
                start_time = start_time.tz_convert(None)
            data = data[data["datetime_utc"] >= start_time]
            print(f"DataManager.get_data: After start_time filter: {data.shape}")
        if end_time:
            # Ensure end_time is a pandas datetime
            if not isinstance(end_time, pd.Timestamp):
                end_time = pd.to_datetime(end_time)
            # Convert to timezone-naive if data is timezone-naive
            if end_time.tz is not None and data["datetime_utc"].dt.tz is None:
                end_time = end_time.tz_convert(None)
            data = data[data["datetime_utc"] <= end_time]
            print(f"DataManager.get_data: After end_time filter: {data.shape}")

        # Remove 'partition' column if it exists
        if "partition" in data.columns:
            data = data.drop(columns=["partition"])

        # Resample if requested
        if resample_freq and not data.empty:
            data.set_index("datetime_utc", inplace=True)
            resampled_data = data.resample(resample_freq).mean()
            resampled_data.reset_index(inplace=True)
            data = resampled_data

        print(f"DataManager.get_data: Final data shape: {data.shape}")
        return data

    def add_2min_moving_averages(self):
        """Add 2-minute moving averages for ph_corrected and ph_total as new columns, without overwriting existing *_ma columns."""
        if self.data.empty or "datetime_utc" not in self.data.columns:
            return
        df = self.data.copy()
        df = df.sort_values("datetime_utc")
        if not pd.api.types.is_datetime64_any_dtype(df["datetime_utc"]):
            df["datetime_utc"] = pd.to_datetime(df["datetime_utc"])
        df.set_index("datetime_utc", inplace=True)
        if "ph_corrected" in df.columns:
            colname = "ph_corrected_ma_app"
            if colname not in df.columns:
                df[colname] = df["ph_corrected"].rolling("2min", min_periods=1).mean()
        if "ph_total" in df.columns:
            colname = "ph_total_ma_app"
            if colname not in df.columns:
                df[colname] = df["ph_total"].rolling("2min", min_periods=1).mean()
        df.reset_index(inplace=True)
        # Only update the new columns in self.data
        for col in ["ph_corrected_ma_app", "ph_total_ma_app"]:
            if col in df.columns:
                self.data[col] = df[col]