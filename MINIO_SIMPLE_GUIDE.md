# Simple MinIO & Delta Lake Guide

## Quick Start

1. **Start everything**: `docker compose up -d`
2. **Check MinIO**: http://localhost:9001 (login: minioadmin/minioadmin)
3. **Verify data**: Browse to `business-data` → `delta` → `ride-bookings`

## What Gets Created

When Spark writes data, you'll see:
- `part-*.parquet` files (your data)
- `_delta_log/` folder (transaction logs)
- `_checkpoints/` folder (streaming checkpoints)

## Simple Verification

### Method 1: MinIO Web Console (Easiest)
1. Go to http://localhost:9001
2. Login: `minioadmin` / `minioadmin`
3. Click `business-data` bucket
4. Navigate to `delta/ride-bookings/`
5. You should see `.parquet` files

### Method 2: Command Line
```bash
# List files
docker exec business_minio mc ls minio/business-data/delta/ride-bookings/ --recursive

# Count files
docker exec business_minio mc ls minio/business-data/delta/ride-bookings/ --recursive | wc -l
```

### Method 3: Check Spark Logs
```bash
docker logs business_spark -f | grep -i "delta\|writing\|batch"
```

## Expected Data

The Parquet files contain:
- `id` - Record ID
- `op` - Operation type (c=create, u=update, d=delete, r=read)
- `created_at_ts` - Creation timestamp
- `updated_at_ts` - Update timestamp  
- `booking_value` - Booking value (double)
- `ride_distance` - Ride distance (double)
- `processed_at` - When Spark processed it

## Troubleshooting

**No files?**
- Check Spark is running: `docker ps | grep spark`
- Check Spark logs: `docker logs business_spark`
- Check if business simulator ran: `docker logs business_simulator`

**Files exist but want to read them?**
- Use MinIO console to download
- Or use Spark SQL (see advanced section)

## Next Steps

Once you verify files are being created, we can:
1. Add data inspection tools
2. Add query capabilities
3. Add monitoring
4. Optimize performance

