# Inspecting Delta Lake Data in MinIO

## Overview

Delta Lake stores data in a structured format that combines:
- **Parquet files**: Columnar data files (part-*.parquet)
- **Transaction logs**: JSON files in `_delta_log/` directory
- **Checkpoints**: Periodic snapshots for faster reads

## Quick Inspection Methods

### Method 1: MinIO Web Console (Easiest)

1. **Access MinIO Console**: http://localhost:9001
2. **Login**: `minioadmin` / `minioadmin`
3. **Navigate**: `business-data` → `delta` → `ride-bookings`
4. **View Structure**:
   - `_delta_log/` - Transaction logs (JSON)
   - `part-*.parquet` - Data files
   - `_checkpoints/` - Streaming checkpoints

### Method 2: Command Line (MinIO Client)

```bash
# List bucket contents
docker exec business_minio mc ls minio/business-data/delta/ride-bookings/ --recursive

# Download a Parquet file to inspect
docker exec business_minio mc cp minio/business-data/delta/ride-bookings/part-00000-xxx.parquet /tmp/
```

### Method 3: Spark SQL (Best for Data Inspection)

```bash
# Access Spark container
docker exec -it business_spark bash

# Run inspection script
python3 /opt/spark-apps/inspect-delta-table.py
```

Or use Spark SQL shell:

```bash
docker exec -it business_spark /opt/spark/bin/spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false

# In Spark SQL:
spark-sql> USE delta.`s3a://business-data/delta/ride-bookings`;
spark-sql> SELECT * FROM ride_bookings LIMIT 10;
spark-sql> DESCRIBE HISTORY ride_bookings;
```

### Method 4: Using Docker Compose Profile

```bash
# Run inspection service
docker compose --profile inspect up inspect-delta
```

## What Delta Lake Format Looks Like

### Directory Structure

```
business-data/
└── delta/
    └── ride-bookings/
        ├── _delta_log/                    # Transaction log directory
        │   ├── 00000000000000000000.json   # Initial transaction log
        │   ├── 00000000000000000001.json   # Next transaction
        │   └── ...
        ├── part-00000-xxx.parquet          # Data file 1
        ├── part-00001-xxx.parquet          # Data file 2
        └── _checkpoints/                   # Streaming checkpoints
            └── ...
```

### Transaction Log Format (JSON)

Each `_delta_log/*.json` file contains:

```json
{
  "commitInfo": {
    "timestamp": 1234567890000,
    "operation": "WRITE",
    "operationParameters": {
      "mode": "Append",
      "partitionBy": "[]"
    }
  }
}
{
  "add": {
    "path": "part-00000-xxx.parquet",
    "partitionValues": {},
    "size": 12345,
    "modificationTime": 1234567890000,
    "dataChange": true
  }
}
```

### Parquet Files

- **Format**: Columnar storage (Parquet)
- **Readable**: Yes, with Parquet tools or Spark
- **Compressed**: Yes (Snappy by default)
- **Schema**: Embedded in Parquet metadata

## Expected Data Schema

Based on `stream_kafka.py`, the Delta table should have:

```
root
 |-- id: long (nullable = true)
 |-- op: string (nullable = true)           # Operation type (c, u, d, r)
 |-- created_at_ts: timestamp (nullable = true)
 |-- updated_at_ts: timestamp (nullable = true)
 |-- booking_value: double (nullable = true)
 |-- ride_distance: double (nullable = true)
 |-- processed_at: timestamp (nullable = false)
```

## Verification Checklist

✅ **Data is being written if you see:**
- Files in `business-data/delta/ride-bookings/`
- `_delta_log/` directory with JSON files
- `part-*.parquet` files
- Spark logs showing "Streaming query" is active

✅ **Data is correct if:**
- Parquet files have non-zero size
- Transaction logs show "WRITE" operations
- Spark can read and display the data
- Row count increases over time

## Troubleshooting

### No data visible?

1. **Check Spark is running**: `docker ps | grep spark`
2. **Check Spark logs**: `docker logs business_spark -f`
3. **Check Kafka has messages**: `docker exec business_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic business_postgres.public.business_ncr_ride_bookings --from-beginning --max-messages 5`
4. **Check business simulator ran**: `docker logs business_simulator`

### Can't read Delta table?

1. **Verify bucket exists**: `docker exec business_minio mc ls minio/business-data/`
2. **Check Spark has Delta packages**: Look for `delta-spark_2.12` in Spark logs
3. **Verify S3 configuration**: Check Spark logs for S3 connection errors

## Reading Parquet Files Directly

If you want to inspect Parquet files without Spark:

```python
# Using pandas (if you have pyarrow)
import pandas as pd
df = pd.read_parquet('s3://business-data/delta/ride-bookings/part-00000-xxx.parquet',
                     storage_options={
                         'key': 'minioadmin',
                         'secret': 'minioadmin',
                         'client_kwargs': {'endpoint_url': 'http://localhost:9000'}
                     })
print(df.head())
```

## Time Travel (Delta Lake Feature)

Delta Lake allows you to read previous versions:

```python
# Read version 0
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("s3a://business-data/delta/ride-bookings")

# Read by timestamp
df_timestamp = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("s3a://business-data/delta/ride-bookings")
```

