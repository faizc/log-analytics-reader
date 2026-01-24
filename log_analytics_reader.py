"""
Azure Log Analytics Chunked Query Reader

This module reads data from Log Analytics tables in small time chunks
(e.g., 30-second intervals) over a specified number of days.

Features:
- Multi-threaded parallel query execution
- Detailed logging with record counts
- CSV output per chunk
- Utility to combine all CSVs in chronological order
"""

import os
import logging
import threading
import glob
import base64
import gzip
import io
import re
from datetime import datetime, timedelta
from typing import Generator, Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from tqdm import tqdm
import pandas as pd
import json
import time
import csv

load_dotenv()

# Thread-safe lock for file operations
file_lock = threading.Lock()
log_lock = threading.Lock()


def decompress_gzip_base64(s: str) -> str:
    """
    Decompress a gzip-compressed, base64-encoded string.
    
    This is the reverse of the KQL function gzip_compress_to_base64_string().
    
    Args:
        s: Base64-encoded gzip-compressed string
        
    Returns:
        Original decompressed string, or None if input is None/empty
    """
    if s is None or (isinstance(s, str) and not s.strip()):
        return None
    try:
        raw = base64.b64decode(s)
        with gzip.GzipFile(fileobj=io.BytesIO(raw), mode='rb') as f:
            return f.read().decode('utf-8')
    except Exception:
        # Return original value if decompression fails
        return s


def decompress_dataframe_columns(
    df: pd.DataFrame, 
    compressed_columns: Dict[str, str]
) -> pd.DataFrame:
    """
    Decompress specified columns in a DataFrame and optionally rename them.
    
    Args:
        df: DataFrame with compressed columns
        compressed_columns: Dict mapping compressed column name to original column name
                           e.g., {"SyslogMessage_base64": "SyslogMessage"}
    
    Returns:
        DataFrame with decompressed and renamed columns
    """
    if not compressed_columns:
        return df
    
    df = df.copy()
    
    for compressed_col, original_col in compressed_columns.items():
        if compressed_col in df.columns:
            # Decompress the column
            df[compressed_col] = df[compressed_col].apply(decompress_gzip_base64)
            # Rename to original column name
            df.rename(columns={compressed_col: original_col}, inplace=True)
    
    return df


class WorkspaceConnectionError(Exception):
    """Raised when unable to connect or query the Log Analytics workspace."""
    pass


@dataclass
class TimeRange:
    """Represents a time range for querying."""
    start: datetime
    end: datetime
    chunk_index: int = 0
    
    def __str__(self):
        return f"{self.start.isoformat()} - {self.end.isoformat()}"
    
    def get_filename(self) -> str:
        """Generate a sortable filename for this time range."""
        return f"chunk_{self.chunk_index:08d}_{self.start.strftime('%Y%m%d_%H%M%S')}.csv"


@dataclass
class QueryConfig:
    """Configuration for Log Analytics queries."""
    workspace_id: str
    table_name: str
    days_to_query: int = 90
    chunk_seconds: int = 30
    additional_filter: Optional[str] = None
    columns: Optional[List[str]] = None
    max_threads: int = 5
    output_dir: str = "./output"
    log_file: str = "./query_log.csv"
    compressed_columns: Optional[Dict[str, str]] = None  # Maps output column name to original column name
    
    @classmethod
    def from_env(cls) -> "QueryConfig":
        """Create config from environment variables."""
        additional_filter = os.getenv("ADDITIONAL_FILTER")
        columns = os.getenv("COLUMNS")
        compressed_columns = cls._parse_compressed_columns(os.getenv("COMPRESSED_COLUMNS", ""))
        return cls(
            workspace_id=os.getenv("LOG_ANALYTICS_WORKSPACE_ID", ""),
            table_name=os.getenv("QUERY_TABLE_NAME", ""),
            days_to_query=int(os.getenv("DAYS_TO_QUERY", "90")),
            chunk_seconds=int(os.getenv("CHUNK_SECONDS", "30")),
            additional_filter=additional_filter if additional_filter else None,
            columns=columns.split(",") if columns else None,
            max_threads=int(os.getenv("MAX_THREADS", "5")),
            output_dir=os.getenv("OUTPUT_DIR", "./output"),
            log_file=os.getenv("LOG_FILE", "./query_log.csv"),
            compressed_columns=compressed_columns if compressed_columns else None,
        )
    
    @staticmethod
    def _parse_compressed_columns(compressed_columns_str: str) -> Dict[str, str]:
        """
        Parse COMPRESSED_COLUMNS configuration string.
        
        Format: "OutputCol1=gzip_compress_to_base64_string(OriginalCol1),OutputCol2=gzip_compress_to_base64_string(OriginalCol2)"
        
        Returns:
            Dict mapping output column name to original column name
        """
        result = {}
        if not compressed_columns_str:
            return result
        
        # Pattern: ColName=gzip_compress_to_base64_string(OriginalColName)
        pattern = r'(\w+)\s*=\s*gzip_compress_to_base64_string\s*\(\s*(\w+)\s*\)'
        
        for col_def in compressed_columns_str.split(','):
            col_def = col_def.strip()
            if not col_def:
                continue
            match = re.match(pattern, col_def)
            if match:
                output_col = match.group(1)  # e.g., SyslogMessage_base64
                original_col = match.group(2)  # e.g., SyslogMessage
                result[output_col] = original_col
        
        return result


class QueryLogger:
    """Thread-safe logger for query execution status."""
    
    def __init__(self, log_file: str):
        """
        Initialize the query logger.
        
        Args:
            log_file: Path to the CSV log file
        """
        self.log_file = log_file
        self._ensure_log_file()
    
    def _ensure_log_file(self):
        """Create log file with headers if it doesn't exist."""
        if not os.path.exists(self.log_file):
            os.makedirs(os.path.dirname(self.log_file) if os.path.dirname(self.log_file) else ".", exist_ok=True)
            with open(self.log_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp', 'chunk_index', 'start_time', 'end_time',
                    'status', 'record_count', 'output_file', 'error_message',
                    'duration_seconds', 'thread_id'
                ])
    
    def log_query(
        self,
        chunk_index: int,
        start_time: datetime,
        end_time: datetime,
        status: str,
        record_count: int,
        output_file: str = "",
        error_message: str = "",
        duration_seconds: float = 0.0
    ):
        """Log a query execution result."""
        with log_lock:
            with open(self.log_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    datetime.utcnow().isoformat(),
                    chunk_index,
                    start_time.isoformat(),
                    end_time.isoformat(),
                    status,
                    record_count,
                    output_file,
                    error_message,
                    f"{duration_seconds:.3f}",
                    threading.current_thread().name
                ])


class LogAnalyticsChunkedReader:
    """
    Reads data from Azure Log Analytics in chunked time intervals.
    
    This is useful for:
    - Avoiding query timeouts on large datasets
    - Processing data in manageable batches
    - Rate limiting API calls
    
    Features:
    - Multi-threaded parallel query execution
    - Detailed logging with record counts
    - CSV output per chunk
    """
    
    def __init__(self, config: QueryConfig, credential=None):
        """
        Initialize the reader.
        
        Args:
            config: Query configuration
            credential: Azure credential (defaults to ClientSecretCredential or DefaultAzureCredential)
        """
        self.config = config
        self.logger = QueryLogger(config.log_file)
        
        # Setup console logging with file handler
        self.console_logger = logging.getLogger(__name__)
        self.console_logger.setLevel(logging.DEBUG)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_format)
        
        # File handler for detailed logs
        log_dir = os.path.dirname(config.log_file) if os.path.dirname(config.log_file) else "."
        os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.FileHandler(
            os.path.join(log_dir, 'query_activity.log'),
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        
        # Clear existing handlers and add new ones
        if not self.console_logger.handlers:
            self.console_logger.addHandler(console_handler)
            self.console_logger.addHandler(file_handler)
        
        self.console_logger.info("="*60)
        self.console_logger.info("Initializing LogAnalyticsChunkedReader")
        self.console_logger.info(f"Workspace ID: {config.workspace_id}")
        self.console_logger.info(f"Table: {config.table_name}")
        self.console_logger.info(f"Output directory: {config.output_dir}")
        self.console_logger.info("="*60)
        
        if credential:
            self.credential = credential
            self.console_logger.info("Using provided credential")
        elif os.getenv("AZURE_CLIENT_SECRET"):
            self.console_logger.info("Using ClientSecretCredential (Service Principal)")
            self.credential = ClientSecretCredential(
                tenant_id=os.getenv("AZURE_TENANT_ID"),
                client_id=os.getenv("AZURE_CLIENT_ID"),
                client_secret=os.getenv("AZURE_CLIENT_SECRET")
            )
        else:
            self.console_logger.info("Using DefaultAzureCredential")
            self.credential = DefaultAzureCredential()
        
        # Create a client per thread to avoid concurrency issues
        self._thread_local = threading.local()
        
        # Ensure output directory exists
        os.makedirs(config.output_dir, exist_ok=True)
    
    def _get_client(self) -> LogsQueryClient:
        """Get or create a thread-local LogsQueryClient."""
        if not hasattr(self._thread_local, 'client'):
            self._thread_local.client = LogsQueryClient(self.credential)
        return self._thread_local.client
    
    def validate_workspace_connection(self) -> bool:
        """
        Validate that we can connect to the Log Analytics workspace.
        
        Returns:
            True if connection is valid
            
        Raises:
            WorkspaceConnectionError: If unable to connect to the workspace
        """
        self.console_logger.info("Validating workspace connection...")
        
        # Simple query to test connectivity
        test_query = f"{self.config.table_name} | take 1"
        client = self._get_client()
        try:
            response = client.query_workspace(
                workspace_id=self.config.workspace_id,
                query=test_query,
                timespan=timedelta(days=1)
            )
            
            if response.status == LogsQueryStatus.SUCCESS:
                self.console_logger.info("Workspace connection validated successfully")
                return True
            #elif response.status == LogsQueryStatus.PARTIAL:
            #    self.console_logger.warning("Workspace returned partial results, but connection is valid")
            #    return True
            else:
                error_msg = f"Workspace query failed with status: {response.status}"
                self.console_logger.error(error_msg)
                raise WorkspaceConnectionError(error_msg)
                
        except WorkspaceConnectionError:
            raise
        except Exception as e:
            error_msg = f"Failed to connect to Log Analytics workspace: {e}"
            self.console_logger.error(error_msg)
            raise WorkspaceConnectionError(error_msg) from e
    
    def get_time_range_from_table(self) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        Query the table to get the min and max TimeGenerated values.
        
        Returns:
            Tuple of (min_date, max_date)
            
        Raises:
            WorkspaceConnectionError: If unable to query the workspace
        """
        query = f"""
            {self.config.table_name}
            | summarize MinTime = min(TimeGenerated), MaxTime = max(TimeGenerated)
        """
        if self.config.additional_filter:
            query = f"""
                {self.config.table_name}
                | where {self.config.additional_filter}
                | summarize MinTime = min(TimeGenerated), MaxTime = max(TimeGenerated)
            """
        
        client = self._get_client()
        
        try:
            self.console_logger.info(f"Querying table for min/max TimeGenerated {query} ...")
            response = client.query_workspace(
                workspace_id=self.config.workspace_id,
                query=query,
                timespan=timedelta(days=90)  # Look back up to 3 months
            )
            
            if response.status == LogsQueryStatus.SUCCESS:
                if response.tables and len(response.tables[0].rows) > 0:
                    row = response.tables[0].rows[0]
                    min_time = row[0]
                    max_time = row[1]
                    
                    # Check if we got valid data
                    if min_time is None or max_time is None:
                        error_msg = f"Table '{self.config.table_name}' exists but contains no data"
                        self.console_logger.error(error_msg)
                        raise WorkspaceConnectionError(error_msg)
                    
                    # Convert to datetime if needed and ensure offset-naive
                    if isinstance(min_time, str):
                        min_time = datetime.fromisoformat(min_time.replace('Z', '+00:00')).replace(tzinfo=None)
                    elif hasattr(min_time, 'tzinfo') and min_time.tzinfo is not None:
                        min_time = min_time.replace(tzinfo=None)
                    
                    if isinstance(max_time, str):
                        max_time = datetime.fromisoformat(max_time.replace('Z', '+00:00')).replace(tzinfo=None)
                    elif hasattr(max_time, 'tzinfo') and max_time.tzinfo is not None:
                        max_time = max_time.replace(tzinfo=None)
                    
                    self.console_logger.info(f"Table data range: {min_time} to {max_time}")
                    return min_time, max_time
                else:
                    error_msg = f"Table '{self.config.table_name}' returned no data"
                    self.console_logger.error(error_msg)
                    raise WorkspaceConnectionError(error_msg)
            else:
                error_msg = f"Query failed with status: {response.status}"
                self.console_logger.error(error_msg)
                raise WorkspaceConnectionError(error_msg)
            
        except WorkspaceConnectionError:
            raise
        except Exception as e:
            error_msg = f"Error querying table time range: {e}"
            self.console_logger.error(error_msg)
            raise WorkspaceConnectionError(error_msg) from e
    
    def determine_query_range(
            self,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
        ) -> Tuple[datetime, datetime]:
        """
        Determine the optimal query range based on table data and configuration.
        
        Logic:
        - If start_date/end_date are provided, use them
        - Otherwise, calculate the allowed query window based on current date:
          - query_window_start = current_date - DAYS_TO_QUERY
          - query_window_end = current_date
        - Query the table for min/max dates
        - Final start_date = MAX(table_min, query_window_start)
        - Final end_date = MIN(table_max, query_window_end)
        
        This ensures:
        - We don't query before the allowed DAYS_TO_QUERY window
        - We don't query before data exists in the table
        - We don't query beyond the current date
        - We don't query beyond the available data in the table
        
        Args:
            start_date: Optional explicit start date
            end_date: Optional explicit end date
            
        Returns:
            Tuple of (start_date, end_date) to use for queries
        """
        # If both dates are provided, use them as-is
        if start_date is not None and end_date is not None:
            return start_date, end_date
        
        # Get min/max from table first (this will raise WorkspaceConnectionError if it fails)
        table_min, table_max = self.get_time_range_from_table()
        
        # Calculate the allowed query window based on current date
        now = datetime.utcnow()
        query_window_start = now - timedelta(days=self.config.days_to_query)
        query_window_end = now
        
        self.console_logger.info(f"Current date: {now}")
        self.console_logger.info(f"Allowed query window: {query_window_start} to {query_window_end}")
        self.console_logger.info(f"Table data range: {table_min} to {table_max}")
        
        # Determine start_date: MAX(table_min, query_window_start)
        # Don't query before data exists OR before the allowed window
        if start_date is None:
            if table_min > query_window_start:
                start_date = table_min
                self.console_logger.info(
                    f"Table min date ({table_min}) is more recent than query window start ({query_window_start}). "
                    f"Starting from table min date."
                )
            else:
                start_date = query_window_start
                self.console_logger.info(
                    f"Table min date ({table_min}) is older than query window start ({query_window_start}). "
                    f"Starting from query window start (current_date - {self.config.days_to_query} days)."
                )
        
        # Determine end_date: MIN(table_max, query_window_end)
        # Don't query beyond available data OR beyond current date
        if end_date is None:
            if table_max < query_window_end:
                end_date = table_max
                self.console_logger.info(
                    f"Table max date ({table_max}) is earlier than current date ({query_window_end}). "
                    f"Ending at table max date."
                )
            else:
                end_date = query_window_end
                self.console_logger.info(
                    f"Table max date ({table_max}) is at or beyond current date ({query_window_end}). "
                    f"Ending at current date."
                )
        
        print(f'table_min: {table_min}, table_max: {table_max}, query_window_start: {query_window_start}, query_window_end: {query_window_end}')
        print(f'Final range: start_date: {start_date}, end_date: {end_date}')
        
        return start_date, end_date
    
    def get_completed_chunks(self) -> set:
        """
        Get the set of chunk indices that have already been completed.
        
        This checks both:
        1. The query log file for SUCCESS/EMPTY status entries
        2. Existing CSV files in the output directory
        
        Returns:
            Set of completed chunk indices
        """
        completed = set()
        
        # Method 1: Check query log file
        if os.path.exists(self.config.log_file):
            try:
                df = pd.read_csv(self.config.log_file)
                # Consider SUCCESS and EMPTY as completed (no need to retry)
                completed_df = df[df['status'].isin(['SUCCESS', 'EMPTY'])]
                completed.update(completed_df['chunk_index'].astype(int).tolist())
                self.console_logger.info(f"Found {len(completed)} completed chunks in log file")
            except Exception as e:
                self.console_logger.warning(f"Could not read log file: {e}")
        
        # Method 2: Check existing CSV files in output directory
        if os.path.exists(self.config.output_dir):
            csv_files = glob.glob(os.path.join(self.config.output_dir, "chunk_*.csv"))
            for csv_file in csv_files:
                filename = os.path.basename(csv_file)
                # Extract chunk index from filename: chunk_00000123_YYYYMMDD_HHMMSS.csv
                try:
                    chunk_index = int(filename.split('_')[1])
                    completed.add(chunk_index)
                except (IndexError, ValueError):
                    pass
        
        return completed
    
    def get_failed_chunks(self) -> List[int]:
        """
        Get list of chunk indices that failed in previous runs.
        
        Returns:
            List of failed chunk indices
        """
        failed = []
        
        if os.path.exists(self.config.log_file):
            try:
                df = pd.read_csv(self.config.log_file)
                failed_df = df[df['status'].isin(['FAILED', 'ERROR'])]
                failed = failed_df['chunk_index'].astype(int).tolist()
            except Exception as e:
                self.console_logger.warning(f"Could not read log file: {e}")
        
        return failed
    
    def round_to_chunk_boundary(self, dt: datetime, round_up: bool = False) -> datetime:
        """
        Round a datetime to the nearest chunk boundary.
        
        For example, with chunk_seconds=60:
        - 12:50:37.768 -> 12:50:00.000 (round down)
        - 12:50:37.768 -> 12:51:00.000 (round up)
        
        Args:
            dt: Datetime to round
            round_up: If True, round up to next boundary; if False, round down
            
        Returns:
            Datetime aligned to chunk boundary
        """
        chunk_seconds = self.config.chunk_seconds
        
        # Get total seconds since midnight
        seconds_since_midnight = dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1_000_000
        
        # Round to chunk boundary
        if round_up:
            # Round up: ceiling division
            aligned_seconds = ((int(seconds_since_midnight) + chunk_seconds - 1) // chunk_seconds) * chunk_seconds
        else:
            # Round down: floor division
            aligned_seconds = (int(seconds_since_midnight) // chunk_seconds) * chunk_seconds
        
        # Reconstruct datetime with aligned time
        aligned_hour = aligned_seconds // 3600
        aligned_minute = (aligned_seconds % 3600) // 60
        aligned_second = aligned_seconds % 60
        
        # Handle day overflow (e.g., rounding 23:59:59 up with 60s chunks -> next day 00:00:00)
        if aligned_hour >= 24:
            return (dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        return dt.replace(hour=aligned_hour, minute=aligned_minute, second=aligned_second, microsecond=0)
    
    def generate_time_ranges(
        self, 
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[TimeRange]:
        """
        Generate time ranges for chunked queries.
        
        The start time is rounded DOWN to the nearest chunk boundary.
        For example, with 60-second chunks:
        - Start 12:50:37 -> Chunk 0 starts at 12:50:00
        - Chunk 1 starts at 12:51:00, etc.
        
        Args:
            start_date: Start date (defaults to 90 days ago at midnight)
            end_date: End date (defaults to today at midnight)
            
        Returns:
            List of TimeRange objects for each chunk
        """
        if end_date is None:
            end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        
        if start_date is None:
            start_date = end_date - timedelta(days=self.config.days_to_query)
        
        # Round start_date DOWN to chunk boundary to get clean chunks
        start_date = self.round_to_chunk_boundary(start_date, round_up=False)
        
        # Round end_date UP to chunk boundary to ensure we capture all data
        end_date = self.round_to_chunk_boundary(end_date, round_up=True)
        
        self.console_logger.debug(f"Generating chunks from {start_date} to {end_date} (aligned to {self.config.chunk_seconds}s boundaries)")
        
        chunk_delta = timedelta(seconds=self.config.chunk_seconds)
        current = start_date
        ranges = []
        chunk_index = 0
        
        while current < end_date:
            chunk_end = min(current + chunk_delta, end_date)
            ranges.append(TimeRange(start=current, end=chunk_end, chunk_index=chunk_index))
            current = chunk_end
            chunk_index += 1
        
        return ranges
    
    def count_total_chunks(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> int:
        """Calculate total number of chunks for progress tracking."""
        if end_date is None:
            end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        
        if start_date is None:
            start_date = end_date - timedelta(days=self.config.days_to_query)
        
        total_seconds = (end_date - start_date).total_seconds()
        return int(total_seconds / self.config.chunk_seconds) + 1
    
    def build_query(self, time_range: TimeRange) -> str:
        """
        Build KQL query for a specific time range.
        
        Args:
            time_range: The time range to query
            
        Returns:
            KQL query string
        """
        columns = "*"
        if self.config.columns:
            columns = ", ".join(self.config.columns)
        
        query = f"""
{self.config.table_name}
| where TimeGenerated >= datetime('{time_range.start.isoformat()}Z')
| where TimeGenerated < datetime('{time_range.end.isoformat()}Z')
"""
        
        if self.config.additional_filter:
            query += f"| where {self.config.additional_filter}\n"
        
        if self.config.columns:
            query += f"| project {columns}\n"
        
        return query.strip()
    
    def query_chunk(
        self, 
        time_range: TimeRange,
        retry_count: int = 3,
        retry_delay: float = 1.0,
        save_to_file: bool = True
    ) -> Tuple[Optional[pd.DataFrame], str, int]:
        """
        Execute query for a single time chunk.
        
        Args:
            time_range: The time range to query
            retry_count: Number of retries on failure
            retry_delay: Delay between retries in seconds
            save_to_file: Whether to save results to CSV file
            
        Returns:
            Tuple of (DataFrame or None, output_file_path, record_count)
        """
        query = self.build_query(time_range)
        start_execution = time.time()
        output_file = ""
        record_count = 0
        
        client = self._get_client()
        self.console_logger.debug(
            f"Chunk {time_range.chunk_index}: Querying {time_range.start} to {time_range.end} - retry_count:{retry_count} "
        )
        
        for attempt in range(retry_count):
            try:
                # Use tuple of (start, end) to specify exact time range
                # This ensures we can query data older than 30 days
                response = client.query_workspace(
                    workspace_id=self.config.workspace_id,
                    query=query,
                    timespan=None  # Rely on WHERE TimeGenerated clauses in query
#                    timespan=(time_range.start, time_range.end)
#                    timespan=timedelta(days=30)  # Safety timespan
                )
                
                duration = time.time() - start_execution
                
                if response.status == LogsQueryStatus.SUCCESS:
                    if response.tables and len(response.tables[0].rows) > 0:
                        table = response.tables[0]
                        df = pd.DataFrame(
                            data=table.rows,
                            columns=table.columns
                        )
                        record_count = len(df)
                        
                        # Decompress any compressed columns before saving
                        if self.config.compressed_columns:
                            df = decompress_dataframe_columns(df, self.config.compressed_columns)
                        
                        # Save to CSV file
                        if save_to_file:
                            output_file = os.path.join(
                                self.config.output_dir, 
                                time_range.get_filename()
                            )
                            with file_lock:
                                df.to_csv(output_file, index=False)
                        
                        # Log success
                        self.logger.log_query(
                            chunk_index=time_range.chunk_index,
                            start_time=time_range.start,
                            end_time=time_range.end,
                            status="SUCCESS",
                            record_count=record_count,
                            output_file=output_file,
                            duration_seconds=duration
                        )
                        
                        self.console_logger.debug(
                            f"Chunk {time_range.chunk_index}: SUCCESS - {record_count} records in {duration:.2f}s"
                        )
                        
                        return df, output_file, record_count
                    
                    # No data in this chunk
                    self.logger.log_query(
                        chunk_index=time_range.chunk_index,
                        start_time=time_range.start,
                        end_time=time_range.end,
                        status="EMPTY",
                        record_count=0,
                        duration_seconds=duration
                    )
                    self.console_logger.debug(
                        f"Chunk {time_range.chunk_index}: EMPTY - no data in {duration:.2f}s"
                    )
                    return None, "", 0
                    
                elif response.status == LogsQueryStatus.PARTIAL:
                    self.console_logger.warning(f"Partial results for {time_range}")
                    if response.partial_data:
                        table = response.partial_data[0]
                        df = pd.DataFrame(
                            data=table.rows,
                            columns=table.columns
                        )
                        record_count = len(df)
                        
                        # Decompress any compressed columns before saving
                        if self.config.compressed_columns:
                            df = decompress_dataframe_columns(df, self.config.compressed_columns)
                        
                        if save_to_file:
                            output_file = os.path.join(
                                self.config.output_dir, 
                                time_range.get_filename()
                            )
                            with file_lock:
                                df.to_csv(output_file, index=False)
                        
                        self.logger.log_query(
                            chunk_index=time_range.chunk_index,
                            start_time=time_range.start,
                            end_time=time_range.end,
                            status="PARTIAL",
                            record_count=record_count,
                            output_file=output_file,
                            duration_seconds=duration
                        )
                        
                        self.console_logger.debug(
                            f"Chunk {time_range.chunk_index}: PARTIAL - {record_count} records in {duration:.2f}s"
                        )
                        
                        return df, output_file, record_count
                    return None, "", 0
                else:
                    self.console_logger.error(f"Query failed for {time_range}")
                    self.logger.log_query(
                        chunk_index=time_range.chunk_index,
                        start_time=time_range.start,
                        end_time=time_range.end,
                        status="FAILED",
                        record_count=0,
                        error_message="Query returned failure status",
                        duration_seconds=duration
                    )
                    return None, "", 0
                    
            except Exception as e:
                print(f'\n\n chunk_index={time_range.chunk_index} Exception: {e} \n\n')
                duration = time.time() - start_execution
                if attempt < retry_count - 1:
                    self.console_logger.warning(
                        f"Retry {attempt + 1}/{retry_count} for {time_range}: {e}"
                    )
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    self.console_logger.error(f"Failed after {retry_count} attempts: {e}")
                    self.logger.log_query(
                        chunk_index=time_range.chunk_index,
                        start_time=time_range.start,
                        end_time=time_range.end,
                        status="ERROR",
                        record_count=0,
                        error_message=str(e),
                        duration_seconds=duration
                    )
                    raise
        
        return None, "", 0
    
    def query_all_chunks(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        progress_callback: Optional[callable] = None,
        delay_between_chunks: float = 0.1
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Query all time chunks sequentially and yield results.
        
        Args:
            start_date: Start date
            end_date: End date
            progress_callback: Optional callback for progress updates
            delay_between_chunks: Delay between queries in seconds
            
        Yields:
            Dict with 'time_range' and 'data' (DataFrame or None)
        """
        time_ranges = self.generate_time_ranges(start_date, end_date)
        total_chunks = len(time_ranges)
        
        for i, time_range in enumerate(tqdm(time_ranges, total=total_chunks, desc="Querying chunks")):
            try:
                df, output_file, record_count = self.query_chunk(time_range, save_to_file=False)
                yield {
                    "time_range": time_range,
                    "data": df,
                    "chunk_index": i,
                    "total_chunks": total_chunks,
                    "record_count": record_count
                }
                
                if progress_callback:
                    progress_callback(i, total_chunks, time_range, df)
                
                # Small delay to avoid rate limiting
                if delay_between_chunks > 0:
                    time.sleep(delay_between_chunks)
                    
            except Exception as e:
                yield {
                    "time_range": time_range,
                    "data": None,
                    "error": str(e),
                    "chunk_index": i,
                    "total_chunks": total_chunks,
                    "record_count": 0
                }
    
    def query_all_chunks_parallel(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        resume: bool = True,
        stop_on_failure: bool = True
    ) -> Dict[str, Any]:
        """
        Query all time chunks in parallel using multiple threads.
        
        Args:
            start_date: Start date
            end_date: End date
            resume: If True, skip chunks that were already completed in previous runs
            stop_on_failure: If True, stop all processing when any chunk fails
            
        Returns:
            Dict with summary statistics
        """
        time_ranges = self.generate_time_ranges(start_date, end_date)
        total_chunks = len(time_ranges)
        
        # Check for completed chunks if resuming
        completed_chunks = set()
        skipped_count = 0
        if resume:
            completed_chunks = self.get_completed_chunks()
            if completed_chunks:
                skipped_count = len(completed_chunks)
                self.console_logger.info(f"Resume mode: Found {skipped_count} already completed chunks")
                # Filter out completed chunks
                time_ranges = [tr for tr in time_ranges if tr.chunk_index not in completed_chunks]
                self.console_logger.info(f"Remaining chunks to process: {len(time_ranges):,}")
        
        chunks_to_process = len(time_ranges)
        
        self.console_logger.info(f"Starting parallel query with {self.config.max_threads} threads")
        self.console_logger.info(f"Total chunks in range: {total_chunks:,}")
        if skipped_count > 0:
            self.console_logger.info(f"Skipped (already completed): {skipped_count:,}")
        self.console_logger.info(f"Chunks to process: {chunks_to_process:,}")
        self.console_logger.info(f"Output directory: {self.config.output_dir}")
        self.console_logger.info(f"Log file: {self.config.log_file}")
        self.console_logger.info(f"Stop on failure: {stop_on_failure}")
        
        # Statistics
        stats = {
            "total_chunks": total_chunks,
            "skipped": skipped_count,
            "chunks_to_process": chunks_to_process,
            "successful": 0,
            "empty": 0,
            "failed": 0,
            "cancelled": 0,
            "total_records": 0,
            "output_files": [],
            "start_time": datetime.utcnow(),
            "end_time": None,
            "stopped_due_to_failure": False
        }
        
        if chunks_to_process == 0:
            self.console_logger.info("All chunks already completed. Nothing to do.")
            stats["end_time"] = datetime.utcnow()
            stats["duration_seconds"] = 0
            return stats
        
        stats_lock = threading.Lock()
        stop_event = threading.Event()  # Signal to stop all threads
        first_error = {"message": None, "chunk_index": None}  # Store first error details
        
        def process_chunk(time_range: TimeRange) -> Dict[str, Any]:
            """Process a single chunk and return result."""
            # Check if we should stop before starting
            if stop_event.is_set():
                return {
                    "time_range": time_range,
                    "success": False,
                    "cancelled": True,
                    "error": "Cancelled due to previous failure"
                }
            
            try:
                df, output_file, record_count = self.query_chunk(time_range, save_to_file=True)
                
                # Check again after query completes
                if stop_event.is_set():
                    return {
                        "time_range": time_range,
                        "success": True,  # Query succeeded but we're stopping
                        "record_count": record_count,
                        "output_file": output_file,
                        "stopping": True
                    }
                
                with stats_lock:
                    if df is not None and record_count > 0:
                        stats["successful"] += 1
                        stats["total_records"] += record_count
                        stats["output_files"].append(output_file)
                    else:
                        stats["empty"] += 1
                    
                    # Log periodic progress every 100 chunks
                    processed = stats["successful"] + stats["empty"] + stats["failed"]
                    if processed % 100 == 0:
                        self.console_logger.info(
                            f"Progress: {processed}/{chunks_to_process} chunks processed | "
                            f"Success: {stats['successful']} | Empty: {stats['empty']} | "
                            f"Failed: {stats['failed']} | Records: {stats['total_records']:,}"
                        )
                
                return {
                    "time_range": time_range,
                    "success": True,
                    "record_count": record_count,
                    "output_file": output_file
                }
            except Exception as e:
                with stats_lock:
                    stats["failed"] += 1
                    # Store first error details
                    if first_error["message"] is None:
                        first_error["message"] = str(e)
                        first_error["chunk_index"] = time_range.chunk_index
                
                self.console_logger.error(
                    f"Chunk {time_range.chunk_index} FAILED: {str(e)[:100]}"
                )
                
                # Signal all threads to stop if stop_on_failure is enabled
                if stop_on_failure and not stop_event.is_set():
                    stop_event.set()
                    self.console_logger.error(
                        f"STOPPING ALL THREADS due to failure in chunk {time_range.chunk_index}"
                    )
                    stats["stopped_due_to_failure"] = True
                
                return {
                    "time_range": time_range,
                    "success": False,
                    "error": str(e)
                }
        
        # Execute queries in parallel with progress bar
        self.console_logger.info("Starting parallel execution...")
        with ThreadPoolExecutor(
            max_workers=self.config.max_threads, 
            thread_name_prefix="QueryThread"
        ) as executor:
            futures = {
                executor.submit(process_chunk, tr): tr 
                for tr in time_ranges
            }
            self.console_logger.debug(f"Submitted {len(futures)} tasks to thread pool")
            
            with tqdm(total=chunks_to_process, desc="Querying chunks") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    pbar.update(1)
                    
                    # Count cancelled tasks
                    if result.get("cancelled"):
                        with stats_lock:
                            stats["cancelled"] += 1
                    
                    # Update progress bar description with stats
                    pbar.set_postfix({
                        'success': stats["successful"],
                        'empty': stats["empty"],
                        'failed': stats["failed"],
                        'records': stats["total_records"]
                    })
                    
                    # If stop event is set and we want to exit early
                    if stop_event.is_set() and stop_on_failure:
                        # Cancel remaining futures
                        for f in futures:
                            if not f.done():
                                f.cancel()
        
        stats["end_time"] = datetime.utcnow()
        stats["duration_seconds"] = (stats["end_time"] - stats["start_time"]).total_seconds()
        
        self.console_logger.info(f"\n{'='*60}")
        self.console_logger.info("Query Summary:")
        self.console_logger.info(f"  Total chunks in range: {stats['total_chunks']:,}")
        if stats['skipped'] > 0:
            self.console_logger.info(f"  Skipped (already completed): {stats['skipped']:,}")
        self.console_logger.info(f"  Processed this run: {stats['chunks_to_process']:,}")
        self.console_logger.info(f"  Successful (with data): {stats['successful']:,}")
        self.console_logger.info(f"  Empty (no data): {stats['empty']:,}")
        self.console_logger.info(f"  Failed: {stats['failed']:,}")
        if stats['cancelled'] > 0:
            self.console_logger.info(f"  Cancelled (due to failure): {stats['cancelled']:,}")
        self.console_logger.info(f"  Total records fetched: {stats['total_records']:,}")
        self.console_logger.info(f"  Duration: {stats['duration_seconds']:.2f} seconds")
        self.console_logger.info(f"  Output files created: {len(stats['output_files']):,}")
        
        if stats['stopped_due_to_failure']:
            self.console_logger.error(f"\n  *** PROCESSING STOPPED DUE TO FAILURE ***")
            self.console_logger.error(f"  First error in chunk {first_error['chunk_index']}: {first_error['message'][:200]}")
            self.console_logger.info(f"  Re-run to resume from where it stopped (completed chunks will be skipped)")
        elif stats['failed'] > 0:
            self.console_logger.info(f"  Note: Re-run to retry {stats['failed']} failed chunks")
        
        self.console_logger.info(f"{'='*60}")
        
        return stats
    
    def query_to_dataframe(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        delay_between_chunks: float = 0.1
    ) -> pd.DataFrame:
        """
        Query all chunks and combine into a single DataFrame.
        
        Args:
            start_date: Start date
            end_date: End date
            delay_between_chunks: Delay between queries
            
        Returns:
            Combined DataFrame with all results
        """
        all_dfs = []
        
        for result in self.query_all_chunks(start_date, end_date, delay_between_chunks=delay_between_chunks):
            if result.get("data") is not None:
                all_dfs.append(result["data"])
        
        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame()
    
    def query_to_files_parallel(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Query all chunks in parallel and save each to separate CSV files.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            Dict with summary statistics
        """
        return self.query_all_chunks_parallel(start_date, end_date)
    
    def query_to_files(
        self,
        output_dir: str,
        file_format: str = "csv",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        delay_between_chunks: float = 0.1,
        group_by_day: bool = True
    ) -> List[str]:
        """
        Query all chunks and save to files.
        
        Args:
            output_dir: Directory to save files
            file_format: 'csv' or 'json'
            start_date: Start date
            end_date: End date
            delay_between_chunks: Delay between queries
            group_by_day: If True, group results by day into single files
            
        Returns:
            List of created file paths
        """
        os.makedirs(output_dir, exist_ok=True)
        created_files = []
        
        if group_by_day:
            # Group by day
            day_data: Dict[str, List[pd.DataFrame]] = {}
            
            for result in self.query_all_chunks(start_date, end_date, delay_between_chunks=delay_between_chunks):
                if result.get("data") is not None:
                    day_key = result["time_range"].start.strftime("%Y-%m-%d")
                    if day_key not in day_data:
                        day_data[day_key] = []
                    day_data[day_key].append(result["data"])
            
            # Save each day's data
            for day_key, dfs in day_data.items():
                combined_df = pd.concat(dfs, ignore_index=True)
                
                if file_format == "csv":
                    file_path = os.path.join(output_dir, f"{day_key}.csv")
                    combined_df.to_csv(file_path, index=False)
                else:
                    file_path = os.path.join(output_dir, f"{day_key}.json")
                    combined_df.to_json(file_path, orient="records", indent=2)
                
                created_files.append(file_path)
                print(f"Saved {len(combined_df)} records to {file_path}")
        else:
            # Save each chunk separately
            for result in self.query_all_chunks(start_date, end_date, delay_between_chunks=delay_between_chunks):
                if result.get("data") is not None:
                    timestamp = result["time_range"].start.strftime("%Y%m%d_%H%M%S")
                    
                    if file_format == "csv":
                        file_path = os.path.join(output_dir, f"chunk_{timestamp}.csv")
                        result["data"].to_csv(file_path, index=False)
                    else:
                        file_path = os.path.join(output_dir, f"chunk_{timestamp}.json")
                        result["data"].to_json(file_path, orient="records", indent=2)
                    
                    created_files.append(file_path)
        
        return created_files


def combine_csv_files_streaming(
    input_dir: str,
    output_file: str,
    pattern: str = "chunk_*.csv",
    delete_source_files: bool = False,
    buffer_size: int = 64 * 1024  # 64KB buffer
) -> Tuple[int, int]:
    """
    Combine CSV files using streaming (memory-efficient for large files).
    
    This method reads and writes files line by line without loading entire
    files into memory. Ideal for combining many large CSV files.
    
    Args:
        input_dir: Directory containing the chunk CSV files
        output_file: Path for the combined output file
        pattern: Glob pattern to match CSV files (default: chunk_*.csv)
        delete_source_files: If True, delete source files after combining
        buffer_size: Read/write buffer size in bytes (default: 64KB)
        
    Returns:
        Tuple of (total_records, total_bytes_written)
    """
    import shutil
    
    # Find all matching CSV files
    csv_files = glob.glob(os.path.join(input_dir, pattern))
    
    if not csv_files:
        print(f"No CSV files found matching pattern '{pattern}' in {input_dir}")
        return 0, 0
    
    # Sort files by chunk index (filename format: chunk_00000000_YYYYMMDD_HHMMSS.csv)
    csv_files.sort(key=lambda x: os.path.basename(x))
    
    # Calculate total size for progress bar
    total_size = sum(os.path.getsize(f) for f in csv_files)
    print(f"Found {len(csv_files)} CSV files to combine")
    print(f"Total source size: {total_size / (1024*1024*1024):.2f} GB")
    
    total_records = 0
    bytes_written = 0
    header = None
    
    with open(output_file, 'wb') as outfile:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc="Combining (streaming)") as pbar:
            for i, csv_file in enumerate(csv_files):
                file_size = os.path.getsize(csv_file)
                file_records = 0
                
                try:
                    with open(csv_file, 'rb') as infile:
                        # Read header line
                        first_line = infile.readline()
                        
                        if i == 0:
                            # First file: write header
                            header = first_line
                            outfile.write(first_line)
                            bytes_written += len(first_line)
                        # Skip header for subsequent files (already read it)
                        
                        # Stream remaining content
                        while True:
                            chunk = infile.read(buffer_size)
                            if not chunk:
                                break
                            outfile.write(chunk)
                            bytes_written += len(chunk)
                            # Count newlines for approximate record count
                            file_records += chunk.count(b'\n')
                    
                    total_records += file_records
                    pbar.update(file_size)
                    
                except Exception as e:
                    print(f"\nError reading {csv_file}: {e}")
                    pbar.update(file_size)
                    continue
    
    print(f"\nCombined {len(csv_files)} files into {output_file}")
    print(f"Total records (approx): {total_records:,}")
    print(f"Output file size: {bytes_written / (1024*1024*1024):.2f} GB")
    
    # Optionally delete source files
    if delete_source_files:
        print("Deleting source files...")
        deleted_count = 0
        for csv_file in csv_files:
            try:
                os.remove(csv_file)
                deleted_count += 1
            except Exception as e:
                print(f"Error deleting {csv_file}: {e}")
        print(f"Deleted {deleted_count} source files")
    
    return total_records, bytes_written


def combine_csv_files_chunked(
    input_dir: str,
    output_file: str,
    pattern: str = "chunk_*.csv",
    delete_source_files: bool = False,
    chunksize: int = 50000  # Read 50k rows at a time
) -> int:
    """
    Combine CSV files using pandas chunked reading (medium memory usage).
    
    This method reads files in chunks using pandas, which is useful when
    you need to apply transformations or handle encoding issues.
    
    Args:
        input_dir: Directory containing the chunk CSV files
        output_file: Path for the combined output file
        pattern: Glob pattern to match CSV files (default: chunk_*.csv)
        delete_source_files: If True, delete source files after combining
        chunksize: Number of rows to read at a time per file
        
    Returns:
        Total number of records in combined file
    """
    # Find all matching CSV files
    csv_files = glob.glob(os.path.join(input_dir, pattern))
    
    if not csv_files:
        print(f"No CSV files found matching pattern '{pattern}' in {input_dir}")
        return 0
    
    # Sort files by chunk index
    csv_files.sort(key=lambda x: os.path.basename(x))
    
    print(f"Found {len(csv_files)} CSV files to combine")
    print(f"Using chunked reading with {chunksize:,} rows per chunk")
    
    total_records = 0
    header_written = False
    
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        for csv_file in tqdm(csv_files, desc="Combining (chunked)"):
            try:
                # Read file in chunks
                for chunk_df in pd.read_csv(csv_file, chunksize=chunksize):
                    if not header_written:
                        # Write header for first chunk
                        chunk_df.to_csv(outfile, index=False, mode='a')
                        header_written = True
                    else:
                        # Append without header
                        chunk_df.to_csv(outfile, index=False, header=False, mode='a')
                    
                    total_records += len(chunk_df)
                    
            except Exception as e:
                print(f"\nError reading {csv_file}: {e}")
                continue
    
    print(f"\nCombined {len(csv_files)} files into {output_file}")
    print(f"Total records: {total_records:,}")
    
    # Optionally delete source files
    if delete_source_files:
        print("Deleting source files...")
        for csv_file in csv_files:
            try:
                os.remove(csv_file)
            except Exception as e:
                print(f"Error deleting {csv_file}: {e}")
        print(f"Deleted {len(csv_files)} source files")
    
    return total_records


def combine_csv_files(
    input_dir: str,
    output_file: str,
    pattern: str = "chunk_*.csv",
    delete_source_files: bool = False,
    method: str = "streaming"
) -> int:
    """
    Combine all CSV files in chronological order into a single file.
    
    The files are sorted by their chunk index (embedded in filename) to ensure
    proper chronological ordering.
    
    Args:
        input_dir: Directory containing the chunk CSV files
        output_file: Path for the combined output file
        pattern: Glob pattern to match CSV files (default: chunk_*.csv)
        delete_source_files: If True, delete source files after combining
        method: Combine method - 'streaming' (fastest, lowest memory), 
                'chunked' (medium memory, uses pandas), or 'memory' (loads all into memory)
        
    Returns:
        Total number of records in combined file
    """
    print(f"Using '{method}' method for combining files")
    
    if method == "streaming":
        records, _ = combine_csv_files_streaming(
            input_dir=input_dir,
            output_file=output_file,
            pattern=pattern,
            delete_source_files=delete_source_files
        )
        return records
    
    elif method == "chunked":
        return combine_csv_files_chunked(
            input_dir=input_dir,
            output_file=output_file,
            pattern=pattern,
            delete_source_files=delete_source_files
        )
    
    elif method == "memory":
        # Original in-memory method (for small datasets)
        csv_files = glob.glob(os.path.join(input_dir, pattern))
        
        if not csv_files:
            print(f"No CSV files found matching pattern '{pattern}' in {input_dir}")
            return 0
        
        csv_files.sort(key=lambda x: os.path.basename(x))
        print(f"Found {len(csv_files)} CSV files to combine")
        print("WARNING: Loading all data into memory. Use 'streaming' for large datasets.")
        
        total_records = 0
        first_file = True
        
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            for csv_file in tqdm(csv_files, desc="Combining (memory)"):
                try:
                    df = pd.read_csv(csv_file)
                    
                    if first_file:
                        df.to_csv(outfile, index=False)
                        first_file = False
                    else:
                        df.to_csv(outfile, index=False, header=False)
                    
                    total_records += len(df)
                    
                except Exception as e:
                    print(f"Error reading {csv_file}: {e}")
                    continue
        
        print(f"\nCombined {len(csv_files)} files into {output_file}")
        print(f"Total records: {total_records:,}")
        
        if delete_source_files:
            print("Deleting source files...")
            for csv_file in csv_files:
                try:
                    os.remove(csv_file)
                except Exception as e:
                    print(f"Error deleting {csv_file}: {e}")
            print(f"Deleted {len(csv_files)} source files")
        
        return total_records
    
    else:
        raise ValueError(f"Unknown method: {method}. Use 'streaming', 'chunked', or 'memory'")


def get_query_log_summary(log_file: str) -> Dict[str, Any]:
    """
    Read the query log file and return a summary.
    
    Args:
        log_file: Path to the query log CSV file
        
    Returns:
        Dict with summary statistics
    """
    if not os.path.exists(log_file):
        return {"error": f"Log file not found: {log_file}"}
    
    df = pd.read_csv(log_file)
    
    summary = {
        "total_queries": len(df),
        "successful": len(df[df['status'] == 'SUCCESS']),
        "empty": len(df[df['status'] == 'EMPTY']),
        "partial": len(df[df['status'] == 'PARTIAL']),
        "failed": len(df[df['status'].isin(['FAILED', 'ERROR'])]),
        "total_records": df['record_count'].sum(),
        "avg_duration": df['duration_seconds'].astype(float).mean(),
        "min_duration": df['duration_seconds'].astype(float).min(),
        "max_duration": df['duration_seconds'].astype(float).max(),
    }
    
    # Get failed queries details
    failed_df = df[df['status'].isin(['FAILED', 'ERROR'])]
    if len(failed_df) > 0:
        summary["failed_chunks"] = failed_df['chunk_index'].tolist()
        summary["error_messages"] = failed_df['error_message'].unique().tolist()
    
    return summary


def main():
    """Example usage of the LogAnalyticsChunkedReader with parallel execution."""
    
    print(f"\n{'='*60}")
    print("Azure Log Analytics Chunked Reader - Parallel Execution")
    print(f"Started at: {datetime.utcnow().isoformat()} UTC")
    print(f"{'='*60}\n")
    
    # Load configuration from environment
    config = QueryConfig.from_env()
    
    if not config.workspace_id or not config.table_name:
        print("Error: Please set LOG_ANALYTICS_WORKSPACE_ID and QUERY_TABLE_NAME in .env file")
        return
    
    print(f"Configuration:")
    print(f"  Workspace ID: {config.workspace_id}")
    print(f"  Table: {config.table_name}")
    print(f"  Days to query (max): {config.days_to_query}")
    print(f"  Chunk size: {config.chunk_seconds} seconds")
    print(f"  Max threads: {config.max_threads}")
    print(f"  Output directory: {config.output_dir}")
    print(f"  Log file: {config.log_file}")
    print(f"  Activity log: {os.path.dirname(config.log_file) or '.'}/query_activity.log")
    if config.additional_filter:
        print(f"  Additional filter: {config.additional_filter}")
    print()
    
    # Initialize reader
    print("Initializing reader and Azure credentials...")
    reader = LogAnalyticsChunkedReader(config)
    
    # Validate workspace connection and determine query range
    try:
        print("\nValidating workspace connection...")
        reader.validate_workspace_connection()
        
        print("Determining optimal query range...")
        start_date, end_date = reader.determine_query_range()
    except WorkspaceConnectionError as e:
        print(f"\n{'='*60}")
        print("FATAL ERROR: Unable to connect to Log Analytics workspace")
        print(f"{'='*60}")
        print(f"Error: {e}")
        print("\nPossible causes:")
        print("  1. Invalid workspace ID")
        print("  2. Invalid or expired credentials")
        print("  3. Network connectivity issues")
        print("  4. Table does not exist or has no data")
        print("  5. Insufficient permissions on the workspace")
        print(f"\nPlease verify your configuration in .env file and try again.")
        print(f"{'='*60}")
        return
    
    # Calculate total chunks for the determined range
    total_chunks = reader.count_total_chunks(start_date, end_date)
    print(f"\nQuery range determined:")
    print(f"  Start: {start_date}")
    print(f"  End: {end_date}")
    print(f"  Total days: {(end_date - start_date).days}")
    print(f"  Total chunks to process: {total_chunks:,}")
    estimated_time = total_chunks * 0.1 / config.max_threads / 60
    print(f"  Estimated time with {config.max_threads} threads: {estimated_time:.1f} minutes")
    print(f"{'='*60}\n")
    
    # Run parallel queries
    print("Starting query execution...")
    stats = reader.query_to_files_parallel(start_date=start_date, end_date=end_date)
    
    # Show log summary
    print(f"\n{'='*60}")
    print("Query Log Summary:")
    print(f"{'='*60}")
    log_summary = get_query_log_summary(config.log_file)
    for key, value in log_summary.items():
        if key not in ['failed_chunks', 'error_messages']:
            print(f"  {key}: {value}")
    
    # Show completion status
    print(f"\n{'='*60}")
    print(f"Completed at: {datetime.utcnow().isoformat()} UTC")
    if stats.get('failed', 0) > 0:
        print(f"WARNING: {stats['failed']} chunks failed. Re-run to retry.")
    else:
        print("All chunks completed successfully!")
    print(f"{'='*60}")
    
    # Offer to combine files
    print(f"\nTo combine all CSV files into a single file, run:")
    print(f"  python log_analytics_reader.py combine -i {config.output_dir} -o combined_output.csv")
    print(f"{'='*60}")


def reset_query_state(
    output_dir: str = "./output",
    log_file: str = "./query_log.csv",
    pattern: str = "chunk_*.csv",
    confirm: bool = True
) -> Dict[str, int]:
    """
    Reset all query logs and output files to start fresh for a new table.
    
    This function deletes:
    - All chunk CSV files in the output directory
    - The query log file
    
    Args:
        output_dir: Directory containing chunk CSV files
        log_file: Path to the query log file
        pattern: Glob pattern to match chunk CSV files
        confirm: If True, prompt for confirmation before deleting
        
    Returns:
        Dict with counts of deleted files
    """
    import shutil
    
    # Find files to delete
    csv_files = glob.glob(os.path.join(output_dir, pattern))
    log_exists = os.path.exists(log_file)
    
    print(f"\n{'='*60}")
    print("RESET QUERY STATE")
    print(f"{'='*60}")
    print(f"\nThis will delete:")
    print(f"  - {len(csv_files)} chunk CSV files in '{output_dir}'")
    if log_exists:
        print(f"  - Query log file: '{log_file}'")
    else:
        print(f"  - Query log file: '{log_file}' (not found)")
    
    # Calculate total size
    total_size = sum(os.path.getsize(f) for f in csv_files if os.path.exists(f))
    if log_exists:
        total_size += os.path.getsize(log_file)
    print(f"  - Total size: {total_size / (1024*1024):.2f} MB")
    
    if len(csv_files) == 0 and not log_exists:
        print("\nNothing to reset. State is already clean.")
        return {"csv_files_deleted": 0, "log_file_deleted": 0}
    
    # Confirm if needed
    if confirm:
        response = input("\nAre you sure you want to delete these files? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Reset cancelled.")
            return {"csv_files_deleted": 0, "log_file_deleted": 0}
    
    # Delete chunk CSV files
    deleted_csv = 0
    for csv_file in csv_files:
        try:
            os.remove(csv_file)
            deleted_csv += 1
        except Exception as e:
            print(f"Error deleting {csv_file}: {e}")
    
    # Delete log file
    deleted_log = 0
    if log_exists:
        try:
            os.remove(log_file)
            deleted_log = 1
            print(f"Deleted query log: {log_file}")
        except Exception as e:
            print(f"Error deleting log file {log_file}: {e}")
    
    print(f"\n{'='*60}")
    print(f"Reset complete!")
    print(f"  - Deleted {deleted_csv} chunk CSV files")
    print(f"  - Deleted {deleted_log} log file(s)")
    print(f"\nYou can now start fresh with a new table query.")
    print(f"{'='*60}")
    
    return {"csv_files_deleted": deleted_csv, "log_file_deleted": deleted_log}


def reset_cli():
    """CLI entry point to reset query state."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Reset query logs and output files for a fresh start')
    parser.add_argument('--output-dir', '-o', default='./output',
                        help='Directory containing chunk CSV files (default: ./output)')
    parser.add_argument('--log-file', '-l', default='./query_log.csv',
                        help='Path to query log file (default: ./query_log.csv)')
    parser.add_argument('--pattern', '-p', default='chunk_*.csv',
                        help='Glob pattern to match chunk CSV files (default: chunk_*.csv)')
    parser.add_argument('--yes', '-y', action='store_true',
                        help='Skip confirmation prompt')
    
    args = parser.parse_args()
    
    reset_query_state(
        output_dir=args.output_dir,
        log_file=args.log_file,
        pattern=args.pattern,
        confirm=not args.yes
    )


def combine_cli():
    """CLI entry point to combine CSV files."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Combine chunk CSV files into a single file')
    parser.add_argument('--input-dir', '-i', default='./output', 
                        help='Directory containing chunk CSV files')
    parser.add_argument('--output-file', '-o', default='combined_output.csv',
                        help='Output combined CSV file path')
    parser.add_argument('--pattern', '-p', default='chunk_*.csv',
                        help='Glob pattern to match CSV files')
    parser.add_argument('--delete-source', '-d', action='store_true',
                        help='Delete source files after combining')
    parser.add_argument('--method', '-m', choices=['streaming', 'chunked', 'memory'],
                        default='streaming',
                        help='Combine method: streaming (fastest, lowest memory), '
                             'chunked (medium memory, pandas), memory (loads all into RAM)')
    
    args = parser.parse_args()
    
    combine_csv_files(
        input_dir=args.input_dir,
        output_file=args.output_file,
        pattern=args.pattern,
        delete_source_files=args.delete_source,
        method=args.method
    )


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "combine":
        # Remove 'combine' from args and run combine CLI
        sys.argv.pop(1)
        combine_cli()
    elif len(sys.argv) > 1 and sys.argv[1] == "reset":
        # Remove 'reset' from args and run reset CLI
        sys.argv.pop(1)
        reset_cli()
    else:
        main()
