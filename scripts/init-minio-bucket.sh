#!/bin/bash
# Initialize MinIO bucket for Delta Lake storage
# This script creates the bucket if it doesn't exist

set -euo pipefail

echo "🪣 Initializing MinIO bucket for Delta Lake..."

# Get credentials from environment
MINIO_USER=${MINIO_ROOT_USER}
MINIO_PASSWORD=${MINIO_ROOT_PASSWORD}

# Wait for MinIO to be ready
until curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; do
  echo "⏳ Waiting for MinIO to be ready..."
  sleep 2
done

echo "✅ MinIO is ready"

# Configure MinIO client alias
mc alias set myminio http://localhost:9000 "$MINIO_USER" "$MINIO_PASSWORD" 2>/dev/null || true

# Create bucket if it doesn't exist
if mc ls myminio/business-data > /dev/null 2>&1; then
  echo "✅ Bucket 'business-data' already exists"
else
  echo "📦 Creating bucket 'business-data'..."
  mc mb myminio/business-data
  echo "✅ Bucket 'business-data' created successfully"
fi

echo "🎉 MinIO bucket initialization completed"

return 0

