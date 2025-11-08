# Spark with Kafka Integration Guide

## How Spark Works

Apache Spark is a distributed data processing engine that can process large-scale data in real-time (streaming) or batch mode.

### Key Concepts:

1. **SparkSession**: Entry point for Spark applications
2. **DataFrames**: Distributed collections of data organized into named columns (like SQL tables)
3. **Structured Streaming**: Real-time processing of data streams
4. **Transformations**: Operations that create new DataFrames (lazy evaluation)
5. **Actions**: Operations that trigger computation and return results

## How Spark Works with Kafka

### Architecture Flow:

```
Kafka Topic → Spark Kafka Connector → Spark Structured Streaming → Transformations → Output
```

### Your Current Setup:

1. **Kafka Producer**: Debezium captures PostgreSQL changes and sends JSON messages to Kafka
2. **Kafka Topic**: `business_postgres.public.business_ncr_ride_bookings`
3. **Spark Consumer**: Reads messages from Kafka topic
4. **Spark Processing**: 
   - Parses JSON messages
   - Cleans data (removes `b'...'` wrapper)
   - Converts timestamps
   - Transforms data types
5. **Output**: Currently writes to console (can be changed to files, databases, etc.)

## Requirements for Spark to Run

### 1. **Dependencies**
- **Kafka Connector Package**: `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`
  - Allows Spark to read from Kafka
  - Automatically downloaded when specified with `--packages`

### 2. **Configuration**
- **Bootstrap Servers**: Kafka broker addresses (`kafka:29092`)
- **Topic Subscription**: Which Kafka topic to read from
- **Checkpoint Location**: Where Spark stores progress/state (`/tmp/spark-checkpoints`)
- **Starting Offset**: Where to begin reading (`latest` = only new messages)

### 3. **Network Access**
- Spark container must be on same network as Kafka
- Must be able to reach Kafka broker at `kafka:29092`

### 4. **Storage**
- **Checkpoint Directory**: Writable location for Spark to track progress
- **Ivy Cache**: Directory for downloaded dependencies (`/tmp/.ivy2`)

### 5. **Resources**
- Memory: Spark needs sufficient memory for processing
- CPU: For parallel processing

## Your Current Spark Job (`stream_kafka.py`)

### What It Does:

1. **Connects to Kafka**:
   ```python
   spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", kafka_bootstrap)
       .option("subscribe", topic)
   ```

2. **Reads JSON Messages**:
   - Extracts `value` field (contains JSON string)
   - Parses JSON with defined schema

3. **Transforms Data**:
   - Cleans byte-like strings (`b'1138.0'` → `1138.0`)
   - Converts microsecond timestamps to readable timestamps
   - Casts string numbers to proper numeric types

4. **Outputs Results**:
   - Writes to console (for debugging/monitoring)
   - Can be changed to write to files, databases, or other Kafka topics

## Processing Flow Example

### Input (from Kafka):
```json
{
  "id": 75204,
  "ncr_ride_bookings.csv/booking+value": "b'1138.0'",
  "ncr_ride_bookings.csv/ride+distance": "b'45.75'",
  "created_at": 1762514972953131,
  "__op": "r"
}
```

### Output (after Spark processing):
```
+-----+---+-------------------+-------------------+-------------+--------------+
|  id | op|      created_at_ts|      updated_at_ts|booking_value|ride_distance |
+-----+---+-------------------+-------------------+-------------+--------------+
|75204|  r|2025-11-07 09:25:23|2025-11-07 09:25:23|       1138.0|         45.75|
+-----+---+-------------------+-------------------+-------------+--------------+
```

## Common Spark Operations You Can Add

### 1. **Filtering**:
```python
result.filter(col("booking_value") > 1000)
```

### 2. **Aggregations**:
```python
result.groupBy("op").agg(avg("booking_value").alias("avg_booking"))
```

### 3. **Window Operations**:
```python
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("op").orderBy("created_at_ts")
result.withColumn("row_number", row_number().over(windowSpec))
```

### 4. **Write to Different Sinks**:
```python
# Write to Parquet files
result.writeStream.format("parquet")
    .option("path", "/output/data")
    .start()

# Write to another Kafka topic
result.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("topic", "processed-data")
    .start()
```

## Troubleshooting

### Issue: Cache Directory Permissions
- **Solution**: Use `/tmp/.ivy2` which is always writable
- Set `IVY_HOME=/tmp/.ivy2` environment variable

### Issue: Kafka Connection Failed
- **Check**: Network connectivity between Spark and Kafka
- **Verify**: Kafka broker is accessible at `kafka:29092`

### Issue: No Messages Processed
- **Check**: `startingOffsets` setting (`latest` only reads new messages)
- **Change to**: `earliest` to read from beginning of topic

### Issue: Checkpoint Errors
- **Solution**: Ensure checkpoint directory is writable
- **Location**: `/tmp/spark-checkpoints` (set in config)

## Performance Tuning

1. **Partitions**: Adjust `spark.sql.shuffle.partitions` based on data size
2. **Batch Size**: Control how much data processed per batch
3. **Parallelism**: Match Kafka topic partitions for optimal performance

## Next Steps

1. **Add More Transformations**: Clean more fields, calculate metrics
2. **Change Output**: Write to database, files, or another Kafka topic
3. **Add Error Handling**: Handle malformed JSON, missing fields
4. **Monitoring**: Add metrics and logging for production use

