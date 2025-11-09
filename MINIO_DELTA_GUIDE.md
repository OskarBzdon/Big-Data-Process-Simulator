# MinIO and Delta Lake Integration Guide

## Overview

This project integrates **MinIO** (S3-compatible object storage) with **Delta Lake** for storing processed streaming data from Kafka. Spark Structured Streaming reads JSON messages from Kafka, transforms them, and writes them to Delta Lake tables stored in MinIO.

## Architecture

```
Kafka → Spark Structured Streaming → Delta Lake (MinIO)
```

1. **Kafka**: Receives CDC events from PostgreSQL via Debezium
2. **Spark**: Processes streaming data, performs transformations
3. **Delta Lake**: Provides ACID transactions, time travel, and schema evolution
4. **MinIO**: S3-compatible object storage for Delta tables

## Components

### MinIO Service

MinIO is configured in `docker-compose.yml` with:
- **API Port**: `9000` (S3 API)
- **Console Port**: `9001` (Web UI)
- **Default Credentials**:
  - Access Key: `minioadmin`
  - Secret Key: `minioadmin`
- **Data Volume**: `minio_data` (persisted)

### Spark Configuration

Spark is configured with:
- **Delta Lake**: `io.delta:delta-spark_2.12:3.5.1`
- **Hadoop AWS**: `org.apache.hadoop:hadoop-aws:3.3.4`
- **S3 Endpoint**: `http://minio:9000`
- **Path Style Access**: Enabled (required for MinIO)

### Delta Lake Table Path

Default path: `s3a://business-data/delta/ride-bookings`

The bucket (`business-data`) is automatically created by Spark when writing data.

## Accessing MinIO

### Web Console

1. Open browser: `http://localhost:9001`
2. Login with:
   - Username: `minioadmin`
   - Password: `minioadmin`
3. Navigate to `business-data` bucket
4. Explore Delta Lake files in `delta/ride-bookings/`

### Using AWS CLI

```bash
# Configure AWS CLI for MinIO
aws configure set aws_access_key_id minioadmin
aws configure set aws_secret_access_key minioadmin
aws configure set default.region us-east-1

# List buckets
aws --endpoint-url http://localhost:9000 s3 ls

# List Delta table files
aws --endpoint-url http://localhost:9000 s3 ls s3://business-data/delta/ride-bookings/

# Download a file
aws --endpoint-url http://localhost:9000 s3 cp s3://business-data/delta/ride-bookings/part-00000-xxx.parquet ./
```

### Using Python (boto3)

```python
import boto3

# Create S3 client
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

# List buckets
buckets = s3_client.list_buckets()
print([b['Name'] for b in buckets['Buckets']])

# List objects in Delta table
objects = s3_client.list_objects_v2(
    Bucket='business-data',
    Prefix='delta/ride-bookings/'
)
for obj in objects.get('Contents', []):
    print(obj['Key'], obj['Size'])
```

## Reading Delta Lake Tables

### Using Spark (PySpark)

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("ReadDeltaTable")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# Read Delta table
df = spark.read.format("delta").load("s3a://business-data/delta/ride-bookings")
df.show()

# Query with SQL
df.createOrReplaceTempView("ride_bookings")
spark.sql("SELECT * FROM ride_bookings WHERE booking_value > 1000").show()

# Time travel (read previous version)
df_v1 = spark.read.format("delta").option("versionAsOf", 0).load("s3a://business-data/delta/ride-bookings")
```

### Using Spark SQL Shell

```bash
# Access Spark container
docker exec -it business_spark bash

# Start Spark SQL with Delta Lake
/opt/spark/bin/spark-sql \
  --packages io.delta:delta-spark_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false

# In Spark SQL shell:
spark-sql> USE delta.`s3a://business-data/delta/ride-bookings`;
spark-sql> SELECT * FROM ride_bookings LIMIT 10;
spark-sql> DESCRIBE HISTORY ride_bookings;
```

## Delta Lake Features

### Time Travel

Query historical versions of the table:

```python
# Read version 0
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("s3a://business-data/delta/ride-bookings")

# Read by timestamp
df_timestamp = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("s3a://business-data/delta/ride-bookings")
```

### Schema Evolution

Delta Lake automatically handles schema changes. New columns are added as nullable fields.

### ACID Transactions

All writes are transactional, ensuring data consistency even with concurrent writes.

### Table History

View table history:

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "s3a://business-data/delta/ride-bookings")
delta_table.history().show()
```

## Monitoring

### Check Spark Streaming Status

```bash
# View Spark logs
docker logs business_spark -f

# Check streaming query status in logs
docker logs business_spark | grep "Streaming query"
```

### Verify Data in MinIO

1. **Web Console**: `http://localhost:9001`
2. **AWS CLI**: `aws --endpoint-url http://localhost:9000 s3 ls s3://business-data/delta/ride-bookings/ --recursive`
3. **Python**: Use boto3 to list objects

### Check Delta Table Statistics

```python
# Get table statistics
delta_table = DeltaTable.forPath(spark, "s3a://business-data/delta/ride-bookings")
delta_table.detail().show()
```

## Troubleshooting

### Issue: Spark cannot connect to MinIO

**Solution**: Verify MinIO is healthy:
```bash
docker ps | grep minio
curl http://localhost:9000/minio/health/live
```

### Issue: Access Denied

**Solution**: Check credentials match in:
- `docker-compose.yml` (Spark environment variables)
- MinIO console/API calls

### Issue: Delta table not found

**Solution**: 
1. Verify Spark job is running: `docker logs business_spark`
2. Check if data has been written: `aws --endpoint-url http://localhost:9000 s3 ls s3://business-data/delta/ride-bookings/`
3. Ensure bucket exists: Spark creates it automatically, but verify in MinIO console

### Issue: Schema mismatch

**Solution**: Delta Lake handles schema evolution automatically. If issues persist:
1. Check Spark logs for schema errors
2. Verify JSON schema in `stream_kafka.py` matches Kafka messages
3. Use `DESCRIBE` to inspect current schema

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_ENDPOINT` | `http://minio:9000` | MinIO endpoint URL |
| `AWS_ACCESS_KEY_ID` | `minioadmin` | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin` | MinIO secret key |
| `DELTA_TABLE_PATH` | `s3a://business-data/delta/ride-bookings` | Delta table storage path |

## References

- [Delta Lake Documentation](https://docs.delta.io/)
- [MinIO Documentation](https://min.io/docs/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Hadoop S3A Configuration](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

