import pandas as pd
import sqlite3
import threading
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
import logging

# Use the same logger name as in app.py for consistency
logger = logging.getLogger("locness_dash.data")

class DataManager:
    def __init__(self, data_path, dynamodb_table=None, dynamodb_region='us-east-1'):
        self.data_path = data_path
        self.dynamodb_table = dynamodb_table
        self.dynamodb_region = dynamodb_region
        self.last_datetime_utc = None  # This will store pandas datetime
        self.data = pd.DataFrame()
        self.lock = threading.Lock()
        self.is_parquet = data_path.endswith(".parquet") if data_path else False
        self.is_dynamodb = dynamodb_table is not None
        
        # Initialize DynamoDB resource if needed
        if self.is_dynamodb:
            self.dynamodb = boto3.resource('dynamodb', region_name=self.dynamodb_region)
            self.table = self.dynamodb.Table(self.dynamodb_table)

    def get_connection(self):
        if self.is_parquet:
            raise ValueError("Parquet does not use a database connection.")
        if self.is_dynamodb:
            raise ValueError("DynamoDB does not use a traditional database connection.")
        return sqlite3.connect(self.data_path)

    def _convert_dynamodb_timestamps(self, data):
        """Convert ISO string timestamps from DynamoDB to pandas datetime and handle numeric types"""
        if not data.empty and "datetime_utc" in data.columns:
            # Convert ISO string format to pandas datetime with explicit format
            # DynamoDB timestamp format: 2025-08-09T02:37:42Z
            data["datetime_utc"] = pd.to_datetime(data["datetime_utc"], format='%Y-%m-%dT%H:%M:%SZ', utc=True)
            
            # Convert numeric columns from DynamoDB Decimal/string types to proper numeric types
            # DynamoDB often stores numbers as Decimal objects or strings which need conversion
            for col in data.columns:
                if col in ["datetime_utc", "static_partition", "partition"]:
                    # Skip datetime and partition columns
                    continue
                    
                # First try to convert from Decimal objects to float
                if data[col].dtype == 'object':
                    # Check if column contains Decimal objects
                    sample_vals = data[col].dropna().head()
                    if len(sample_vals) > 0 and isinstance(sample_vals.iloc[0], Decimal):
                        data[col] = data[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
                
                # Then convert to numeric types, keeping non-numeric as-is
                # Use try/catch to skip non-numeric columns like partition keys
                try:
                    data[col] = pd.to_numeric(data[col])
                except (ValueError, TypeError):
                    # Keep as original type if conversion fails
                    pass
                    
        return data

    def _ensure_proper_dtypes(self, df):
        """Ensure all columns have proper pandas dtypes for DynamoDB data"""
        logger.debug("DynamoDB: Converting data types to proper pandas dtypes")
        
        for col in df.columns:
            if col == "datetime_utc":
                # Ensure datetime_utc is datetime64[ns]
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col])
                    logger.debug(f"  {col}: converted to datetime64[ns]")
                continue
            
            if col in ["static_partition", "partition"]:
                # Skip partition columns - keep as string
                df[col] = df[col].astype('string')
                logger.debug(f"  {col}: kept as string (partition column)")
                continue
                
            # Handle other columns
            original_dtype = df[col].dtype
            
            # Skip if already numeric
            if pd.api.types.is_numeric_dtype(df[col]):
                continue
                
            # Try to convert object/string columns to numeric
            if df[col].dtype == 'object':
                # First handle Decimal objects from DynamoDB
                if df[col].dropna().empty:
                    continue
                    
                sample_val = df[col].dropna().iloc[0]
                
                # Convert Decimal objects to float
                if isinstance(sample_val, Decimal):
                    df[col] = df[col].apply(lambda x: float(x) if isinstance(x, Decimal) and x is not None else x)
                
                # Try numeric conversion
                numeric_converted = pd.to_numeric(df[col], errors='coerce')
                
                # If conversion was successful (not all NaN), use it
                if not numeric_converted.isna().all():
                    # Check if we should use int or float
                    if numeric_converted.dropna().apply(lambda x: x.is_integer()).all():
                        # Try to convert to int64 if all values are integers
                        try:
                            df[col] = numeric_converted.astype('Int64')  # Nullable integer
                            logger.debug(f"  {col}: {original_dtype} -> Int64")
                        except Exception:
                            df[col] = numeric_converted.astype('float64')
                            logger.debug(f"  {col}: {original_dtype} -> float64")
                    else:
                        df[col] = numeric_converted.astype('float64')
                        logger.debug(f"  {col}: {original_dtype} -> float64")
                else:
                    # Keep as string if numeric conversion failed
                    df[col] = df[col].astype('string')
                    logger.debug(f"  {col}: {original_dtype} -> string")
                    
        return df

    def _query_dynamodb_data(self, start_time=None, limit=None):
        """Query data from DynamoDB using partition key 'data' and sort key 'datetime_utc'"""
        try:
            # First try efficient query approach (requires partition + sort key schema)
            return self._query_dynamodb_with_keys(start_time, limit)
        except Exception as e:
            # Check if it's a ValidationException indicating schema mismatch
            if "ValidationException" in str(e) or "sort key" in str(e).lower():
                logger.warning(f"DynamoDB: Query with keys failed ({e}), falling back to scan operations")
                return self._scan_dynamodb_fallback(start_time, limit)
            else:
                # Re-raise other exceptions
                raise

    def _query_dynamodb_with_keys(self, start_time=None, limit=None):
        """Efficient query using partition key 'data' and sort key 'datetime_utc'"""
        if start_time:
            # Convert pandas datetime to ISO string for DynamoDB query
            if hasattr(start_time, 'isoformat'):
                start_time_iso = start_time.isoformat()
            else:
                start_time_iso = pd.to_datetime(start_time).isoformat()
            
            # Add 'Z' suffix if not present for proper UTC comparison
            if not start_time_iso.endswith('Z') and '+' not in start_time_iso:
                start_time_iso = start_time_iso + 'Z'
            
            logger.debug(f"DynamoDB: Querying partition 'data' for datetime_utc > {start_time_iso}")
            
            # Use efficient query with partition key and sort key condition
            response = self.table.query(
                KeyConditionExpression=Key('static_partition').eq('data') & Key('datetime_utc').gt(start_time_iso),
                Limit=limit if limit else 1000,
                ScanIndexForward=True  # Sort ascending by datetime_utc
            )
        else:
            # Query entire partition for initial load
            logger.debug("DynamoDB: Querying entire 'data' partition")
            response = self.table.query(
                KeyConditionExpression=Key('static_partition').eq('data'),
                Limit=limit if limit else 1000,
                ScanIndexForward=True  # Sort ascending by datetime_utc
            )
        
        items = response['Items']
        
        # Handle pagination
        while 'LastEvaluatedKey' in response and len(items) < (limit or 10000):
            if start_time:
                response = self.table.query(
                    KeyConditionExpression=Key('static_partition').eq('data') & Key('datetime_utc').gt(start_time_iso),
                    ExclusiveStartKey=response['LastEvaluatedKey'],
                    Limit=(limit - len(items)) if limit else 1000,
                    ScanIndexForward=True
                )
            else:
                response = self.table.query(
                    KeyConditionExpression=Key('static_partition').eq('data'),
                    ExclusiveStartKey=response['LastEvaluatedKey'],
                    Limit=(limit - len(items)) if limit else 1000,
                    ScanIndexForward=True
                )
            items.extend(response['Items'])
            
        logger.debug(f"DynamoDB: Retrieved {len(items)} total items from query")
        return self._process_dynamodb_items(items, start_time)

    def _scan_dynamodb_fallback(self, start_time=None, limit=None):
        """Fallback scan operation for tables without expected key schema"""
        try:
            if start_time:
                # Convert pandas datetime to ISO string for DynamoDB filter
                if hasattr(start_time, 'isoformat'):
                    start_time_iso = start_time.isoformat()
                else:
                    start_time_iso = pd.to_datetime(start_time).isoformat()
                
                # Add 'Z' suffix if not present for proper UTC comparison
                if not start_time_iso.endswith('Z') and '+' not in start_time_iso:
                    start_time_iso = start_time_iso + 'Z'
                
                logger.debug(f"DynamoDB: Scanning for datetime_utc > {start_time_iso}")
                
                # Use scan with filter for datetime_utc
                response = self.table.scan(
                    FilterExpression=Key('datetime_utc').gt(start_time_iso),
                    Limit=limit if limit else 1000
                )
            else:
                # Scan entire table for initial load
                logger.debug("DynamoDB: Scanning entire table")
                response = self.table.scan(Limit=limit if limit else 1000)
            
            items = response['Items']
            
            # Handle pagination
            while 'LastEvaluatedKey' in response and len(items) < (limit or 10000):
                if start_time:
                    response = self.table.scan(
                        FilterExpression=Key('datetime_utc').gt(start_time_iso),
                        ExclusiveStartKey=response['LastEvaluatedKey'],
                        Limit=(limit - len(items)) if limit else 1000
                    )
                else:
                    response = self.table.scan(
                        ExclusiveStartKey=response['LastEvaluatedKey'],
                        Limit=(limit - len(items)) if limit else 1000
                    )
                items.extend(response['Items'])
                
            logger.debug(f"DynamoDB: Retrieved {len(items)} total items from scan")
            return self._process_dynamodb_items(items, start_time)
            
        except Exception as e:
            logger.error(f"Error scanning DynamoDB: {e}", exc_info=True)
            return pd.DataFrame()

    def _process_dynamodb_items(self, items, start_time=None):
        """Common processing for DynamoDB items regardless of query/scan method"""
        df = pd.DataFrame(items)
        df = self._convert_dynamodb_timestamps(df)
        
        # Convert all DynamoDB data types to proper pandas dtypes
        if not df.empty:
            df = self._ensure_proper_dtypes(df)
        
        # Additional client-side filtering for precise timestamp filtering if needed
        # (Should not be necessary with efficient query, but kept for safety)
        if start_time and not df.empty:
            initial_count = len(df)
            df = df[df["datetime_utc"] > start_time]
            if initial_count != len(df):
                logger.debug(f"DynamoDB: Client-side timestamp filtering removed {initial_count - len(df)} items")
        
        # Data should already be sorted by datetime_utc from DynamoDB query, but ensure it
        if not df.empty and "datetime_utc" in df.columns:
            df = df.sort_values("datetime_utc").reset_index(drop=True)
            logger.debug(f"DynamoDB: Final sorted data has {len(df)} rows")
            
            # Debug: Print column data types for troubleshooting dropdown issues
            logger.debug("DynamoDB: Final column data types:")
            for col, dtype in df.dtypes.items():
                logger.debug(f"  {col}: {dtype}")
        
        return df

    def load_initial_data(self):
        """Load all existing data from the data source"""
        with self.lock:
            try:
                if self.is_dynamodb:
                    self.data = self._query_dynamodb_data()
                elif self.is_parquet:
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
                    
                    # Remove duplicates after loading initial data
                    self._remove_duplicates_internal()

            except Exception as e:
                logger.error(f"Error loading initial data: {e}", exc_info=True)
                self.data = pd.DataFrame()

    def get_new_data(self):
        """Get data newer than last_datetime_utc"""
        if self.last_datetime_utc is None:
            return pd.DataFrame()

        logger.info(f"Fetching new data after {self.last_datetime_utc} (UTC timestamp)")
        try:
            if self.is_dynamodb:
                # Query DynamoDB for new data
                new_data = self._query_dynamodb_data(start_time=self.last_datetime_utc)
            elif self.is_parquet:
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
                logger.debug(f"DataManager: Found {len(new_data)} new rows")

                with self.lock:
                    self.data = pd.concat([self.data, new_data], ignore_index=True)
                    # Update last_datetime_utc to the maximum datetime in the new data
                    self.last_datetime_utc = new_data["datetime_utc"].max()
                    
                    # Remove duplicates after adding new data
                    self._remove_duplicates_internal()

                logger.debug(f"DataManager: data shape after append: {self.data.shape}")
                logger.debug(f"DataManager: last_datetime_utc updated to {self.last_datetime_utc}")

                return new_data
        except Exception as e:
            logger.error(f"Error getting new data: {e}", exc_info=True)

        return pd.DataFrame()

    def get_data(self, start_time=None, end_time=None, resample_freq=None):
        """Get data with optional time filtering and resampling"""
        with self.lock:
            data = self.data.copy()

        if data.empty:
            return data

        logger.debug(f"DataManager.get_data: Original data shape: {data.shape}")
        if not data.empty and "datetime_utc" in data.columns:
            logger.debug(f"DataManager.get_data: Data time range: {data['datetime_utc'].min()} to {data['datetime_utc'].max()}")
        logger.debug(f"DataManager.get_data: Filter start_time: {start_time}, end_time: {end_time}")
        
        # log missing data
        missing_before = data.isnull().sum().sum()
        if missing_before > 0:
            logger.warning(f"DataManager.get_data: Missing values before dropna: {missing_before}")

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
            logger.debug(f"DataManager.get_data: After start_time filter: {data.shape}")
        if end_time:
            # Ensure end_time is a pandas datetime
            if not isinstance(end_time, pd.Timestamp):
                end_time = pd.to_datetime(end_time)
            # Convert to timezone-naive if data is timezone-naive
            if end_time.tz is not None and data["datetime_utc"].dt.tz is None:
                end_time = end_time.tz_convert(None)
            data = data[data["datetime_utc"] <= end_time]
            logger.debug(f"DataManager.get_data: After end_time filter: {data.shape}")

        # Remove partition columns if they exist
        partition_columns = ["partition", "static_partition"]
        for col in partition_columns:
            if col in data.columns:
                data = data.drop(columns=[col])
                logger.debug(f"DataManager.get_data: Removed partition column '{col}'")

        # Resample if requested (downsampling only - never upsample)
        if resample_freq and not data.empty:
            logger.debug(f"DataManager.get_data: Resampling to {resample_freq} from {data.shape}")
            
            # Calculate the actual data frequency to prevent upsampling
            if len(data) > 1:
                data_sorted = data.sort_values("datetime_utc")
                time_diffs = data_sorted["datetime_utc"].diff().dropna()
                median_interval = time_diffs.median()
                logger.debug(f"DataManager.get_data: Median data interval: {median_interval}")
                
                # Convert resample frequency to timedelta for comparison
                import re
                freq_match = re.match(r'(\d+)([a-zA-Z]+)', resample_freq)
                if freq_match:
                    freq_value = int(freq_match.group(1))
                    freq_unit = freq_match.group(2).lower()
                    
                    # Convert to seconds for comparison
                    unit_to_seconds = {
                        's': 1, 'sec': 1, 'second': 1, 'seconds': 1,
                        't': 60, 'min': 60, 'minute': 60, 'minutes': 60,
                        'h': 3600, 'hour': 3600, 'hours': 3600,
                        'd': 86400, 'day': 86400, 'days': 86400
                    }
                    
                    requested_seconds = freq_value * unit_to_seconds.get(freq_unit, 60)  # default to minutes
                    median_seconds = median_interval.total_seconds()
                    
                    logger.debug(f"DataManager.get_data: Requested interval: {requested_seconds}s, Data interval: {median_seconds:.1f}s")
                    
                    # Only resample if requested frequency is LOWER (larger interval) than data frequency
                    if requested_seconds > median_seconds:
                        logger.debug(f"DataManager.get_data: Downsampling from {median_seconds:.1f}s to {requested_seconds}s")
                        data.set_index("datetime_utc", inplace=True)
                        
                        # Separate numeric and non-numeric columns for resampling
                        numeric_columns = data.select_dtypes(include=['number']).columns
                        non_numeric_columns = data.select_dtypes(exclude=['number']).columns
                        
                        # Resample only numeric columns with mean
                        resampled_numeric = data[numeric_columns].resample(resample_freq).mean()
                        
                        # For non-numeric columns, take the first value in each group
                        if len(non_numeric_columns) > 0:
                            resampled_non_numeric = data[non_numeric_columns].resample(resample_freq).first()
                            resampled_data = pd.concat([resampled_numeric, resampled_non_numeric], axis=1)
                        else:
                            resampled_data = resampled_numeric
                        
                        resampled_data.reset_index(inplace=True)
                        data = resampled_data
                        logger.debug(f"DataManager.get_data: After resampling shape: {data.shape}")
                    else:
                        logger.debug("DataManager.get_data: Skipping resampling - would upsample data (not allowed)")
                else:
                    logger.warning(f"DataManager.get_data: Could not parse resample frequency '{resample_freq}', skipping")
            else:
                logger.warning("DataManager.get_data: Not enough data points for resampling frequency analysis")

        logger.debug(f"DataManager.get_data: Final data shape: {data.shape}")
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

    def _remove_duplicates_internal(self):
        """Internal method to remove duplicates without acquiring lock (assumes caller holds lock)"""
        if not self.data.empty and "datetime_utc" in self.data.columns:
            initial_count = len(self.data)
            self.data = self.data.drop_duplicates(subset=['datetime_utc'], keep='last')
            final_count = len(self.data)
            if initial_count != final_count:
                logger.warning(f"DataManager: Removed {initial_count - final_count} duplicate rows")
                # Update last_datetime_utc after deduplication
                if not self.data.empty:
                    self.last_datetime_utc = self.data["datetime_utc"].max()

    def remove_duplicates(self):
        """Remove duplicate rows based on datetime_utc, keeping the last occurrence"""
        with self.lock:
            self._remove_duplicates_internal()

    def get_data_info(self):
        """Get information about the current data for debugging"""
        with self.lock:
            data = self.data.copy()
        
        if data.empty:
            return "No data loaded"
        
        info = {
            "total_rows": len(data),
            "data_source": "DynamoDB" if self.is_dynamodb else ("Parquet" if self.is_parquet else "SQLite"),
            "time_range": f"{data['datetime_utc'].min()} to {data['datetime_utc'].max()}" if "datetime_utc" in data.columns else "No datetime column",
            "last_datetime_utc": str(self.last_datetime_utc),
            "columns": list(data.columns),
        }
        
        # Check for duplicates
        if "datetime_utc" in data.columns:
            duplicate_count = data['datetime_utc'].duplicated().sum()
            info["duplicate_timestamps"] = duplicate_count
        
        return info