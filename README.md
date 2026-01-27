# Azure Log Analytics Chunked Reader

A Python utility to read data from Azure Log Analytics tables in small time chunks (e.g., 30-second intervals) over extended periods (e.g., 90 days).

## Features

- **Chunked Queries**: Break down large time ranges into small 30-second chunks to avoid query timeouts
- **Multi-threaded Execution**: Run up to 5 (configurable) parallel queries for faster processing
- **Progress Tracking**: Visual progress bar with tqdm showing real-time statistics
- **Detailed Logging**: All query executions logged to CSV with status, record counts, and timing
- **Retry Logic**: Automatic retries on transient failures
- **CSV Output**: Each chunk saved as separate CSV file for reliability
- **Combine Utility**: Merge all chunk CSVs into a single file in chronological order
- **Rate Limiting**: Configurable delay between queries to avoid throttling

## Time Range Example

For 90 days with 30-second chunks:
- Day 1, Hour 0: `12:00:00 AM - 12:00:30 AM`, `12:00:30 AM - 12:01:00 AM`, ...
- Total chunks per day: 2,880
- Total chunks for 90 days: 259,200

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

1. Copy `.env.template` to `.env`:
```bash
cp .env.template .env
```

2. Fill in your values:
```env
# Azure Authentication
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret

# Log Analytics
LOG_ANALYTICS_WORKSPACE_ID=your-workspace-id
QUERY_TABLE_NAME=YourTableName

# Query Settings
DAYS_TO_QUERY=90
CHUNK_SECONDS=30

# Parallel Execution
MAX_THREADS=5

# Output
OUTPUT_DIR=./output
LOG_FILE=./query_log.csv
```

## Usage

### Command Line - Run Queries

```bash
python log_analytics_reader.py
```

### Command Line - Combine CSV Files

```bash
# Combine all chunk CSV files using streaming (RECOMMENDED for large files)
python log_analytics_reader.py combine -i ./output -o combined_output.csv

# Use chunked pandas method (useful if you need transformations)
python log_analytics_reader.py combine -i ./output -o combined_output.csv -m chunked

# Use memory method (only for small datasets)
python log_analytics_reader.py combine -i ./output -o combined_output.csv -m memory

# Options:
#   -i, --input-dir    Directory containing chunk CSV files (default: ./output)
#   -o, --output-file  Output combined CSV file path (default: combined_output.csv)
#   -p, --pattern      Glob pattern to match CSV files (default: chunk_*.csv)
#   -d, --delete-source  Delete source files after combining
#   -m, --method       Combine method: streaming, chunked, or memory (default: streaming)
```

#### Combine Methods Comparison

| Method | Memory Usage | Speed | Best For |
|--------|--------------|-------|----------|
| `streaming` | Very Low (~64KB) | Fastest | Large files (350k+ rows/file), GB-scale data |
| `chunked` | Medium (~50k rows) | Medium | When pandas transformations needed |
| `memory` | High (entire file) | Slow | Small datasets only |

### Command Line - Reset for New Table

```bash
# Reset all query logs and output files to start fresh
python log_analytics_reader.py reset

# Skip confirmation prompt
python log_analytics_reader.py reset -y

# Specify custom paths
python log_analytics_reader.py reset -o ./output -l ./query_log.csv

# Options:
#   -o, --output-dir   Directory containing chunk CSV files (default: ./output)
#   -l, --log-file     Path to query log file (default: ./query_log.csv)
#   -p, --pattern      Glob pattern to match chunk files (default: chunk_*.csv)
#   -y, --yes          Skip confirmation prompt
```

### Programmatic Usage

```python
from datetime import datetime, timedelta
from log_analytics_reader import (
    LogAnalyticsChunkedReader, 
    QueryConfig,
    combine_csv_files,
    get_query_log_summary
)

# Create config
config = QueryConfig(
    workspace_id="your-workspace-id",
    table_name="ContainerLog",
    days_to_query=90,
    chunk_seconds=30,
    max_threads=5,
    output_dir="./output",
    log_file="./query_log.csv",
    additional_filter="ContainerID == 'abc123'",  # Optional KQL filter
    columns=["TimeGenerated", "LogEntry", "ContainerID"]  # Optional column selection
)

# Initialize reader
reader = LogAnalyticsChunkedReader(config)

# Option 1: Query in parallel and save to CSV files (RECOMMENDED)
stats = reader.query_to_files_parallel()
print(f"Total records fetched: {stats['total_records']}")
print(f"Successful chunks: {stats['successful']}")
print(f"Failed chunks: {stats['failed']}")

# Option 2: Combine all CSV files into single file
total_records = combine_csv_files(
    input_dir="./output",
    output_file="combined_output.csv",
    delete_source_files=False  # Set True to clean up after combining
)

# Option 3: Get query log summary
summary = get_query_log_summary("./query_log.csv")
print(f"Total queries: {summary['total_queries']}")
print(f"Success rate: {summary['successful'] / summary['total_queries'] * 100:.1f}%")

# Option 4: Process chunks sequentially (for smaller datasets)
for result in reader.query_all_chunks():
    time_range = result["time_range"]
    data = result["data"]
    if data is not None:
        print(f"{time_range}: {result['record_count']} records")
```

### Custom Date Range

```python
from datetime import datetime

# Query specific date range
start = datetime(2025, 10, 1, 0, 0, 0)
end = datetime(2025, 12, 31, 23, 59, 59)

stats = reader.query_to_files_parallel(start_date=start, end_date=end)
```

### Handling Large Columns with Compression

When dealing with tables that have very large text columns (e.g., `SyslogMessage`, `LogEntry`, JSON payloads), you may encounter partial data retrieval issues. **Azure Log Analytics has a 64MB response size limit per query** - if the data retrieved even at a 1-second interval exceeds this limit, the response will be truncated, resulting in incomplete data.

#### The Solution: `gzip_compress_to_base64_string`

Use the built-in KQL function `gzip_compress_to_base64_string()` to compress large columns during query execution. This significantly reduces the response size and helps avoid the 64MB limit.

#### Configuration

In your `.env` file, specify columns with compression using the `COLUMNS` setting:

```env
# Define columns to fetch, with compression for large text columns
COLUMNS=TimeGenerated, Computer, EventTime, Facility, HostName, SeverityLevel, SyslogMessage_base64=gzip_compress_to_base64_string(SyslogMessage), ProcessID, HostIP, ProcessName, Type, _ResourceId

# Specify which columns need decompression when writing output
COMPRESSED_COLUMNS=SyslogMessage_base64=gzip_compress_to_base64_string(SyslogMessage)
```

#### How It Works

1. **Query Phase**: The KQL query uses `gzip_compress_to_base64_string(ColumnName)` to compress the column data on the server side before transmission
2. **Transfer Phase**: Compressed data (typically 70-90% smaller) is transferred, staying well under the 64MB limit
3. **Output Phase**: The reader automatically decompresses the data when writing to CSV files

#### Example Scenarios

| Scenario | Without Compression | With Compression |
|----------|---------------------|------------------|
| 10,000 rows × 50KB message | ~500MB (FAILS) | ~50-100MB compressed |
| Syslog with stack traces | Frequent partial data | Complete data retrieval |
| JSON payloads in logs | 64MB limit hit quickly | 5-10x more data per query |

#### When to Use Compression

- ✅ Tables with large text columns (`SyslogMessage`, `LogEntry`, `Message`, `RawData`)
- ✅ JSON or XML data stored in log columns
- ✅ Stack traces or verbose error messages
- ✅ When you see "partial" status in query logs frequently
- ✅ When reducing `CHUNK_SECONDS` to 1 still results in truncated data

#### Syntax Format

```env
# Format: OutputColumnName=gzip_compress_to_base64_string(OriginalColumnName)
COLUMNS=..., CompressedCol=gzip_compress_to_base64_string(OriginalCol), ...
COMPRESSED_COLUMNS=CompressedCol=gzip_compress_to_base64_string(OriginalCol)
```

> **Note**: The `COMPRESSED_COLUMNS` setting tells the reader which columns to decompress when writing output. If omitted, compressed columns will remain as base64 strings in the output CSV.

## Output Files

### Chunk CSV Files
Each query chunk is saved as a separate CSV file with a sortable name:
```
output/
├── chunk_00000000_20251001_000000.csv
├── chunk_00000001_20251001_000030.csv
├── chunk_00000002_20251001_000100.csv
├── ...
└── chunk_00259199_20251231_235930.csv
```

### Query Log File (query_log.csv)
Tracks all query executions with detailed information:
| Column | Description |
|--------|-------------|
| timestamp | When the query was executed |
| chunk_index | Sequential index of the chunk |
| start_time | Query time range start |
| end_time | Query time range end |
| status | SUCCESS, EMPTY, PARTIAL, FAILED, ERROR |
| record_count | Number of records fetched |
| output_file | Path to output CSV file |
| error_message | Error details if failed |
| duration_seconds | Query execution time |
| thread_id | Thread that executed the query |

### Combined Output
After running queries, combine all CSVs:
```
combined_output.csv  # All records in chronological order
```

## Estimated Processing Time

With 5 parallel threads:

| Days | Chunks (30s) | Time @ 0.1s/query |
|------|--------------|-------------------|
| 1    | 2,880        | ~1 minute         |
| 7    | 20,160       | ~7 minutes        |
| 30   | 86,400       | ~29 minutes       |
| 90   | 259,200      | ~87 minutes       |

## Workflow

1. **Configure**: Set up `.env` with your Azure credentials and query settings
2. **Run**: Execute `python log_analytics_reader.py` to run parallel queries
3. **Monitor**: Watch progress bar and check `query_log.csv` for detailed status
4. **Combine**: Run `python log_analytics_reader.py combine` to merge all CSVs
5. **Verify**: Check the combined file and query log for any issues

## Error Handling

- **Automatic Retries**: Each chunk query is retried up to 3 times on failure
- **Graceful Degradation**: Failed chunks are logged but don't stop other queries
- **Resume Support**: Re-run with same config; existing files won't be overwritten
- **Error Analysis**: Use `get_query_log_summary()` to identify failed chunks

## Notes

- The reader uses UTC time by default
- Empty chunks (no data in time range) are logged but no CSV file is created
- Each thread creates its own Log Analytics client to avoid concurrency issues
- Consider running long queries overnight or in the background
