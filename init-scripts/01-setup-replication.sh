#!/bin/bash
# Setup PostgreSQL replication for Debezium
# This script substitutes environment variables and executes SQL

set -euo pipefail

# Get credentials from environment
DEBEZIUM_USER=${DEBEZIUM_USER}
DEBEZIUM_PASSWORD=${DEBEZIUM_PASSWORD}

# Create SQL file with substituted values
SQL_FILE=$(mktemp)
cat > "$SQL_FILE" <<EOF
-- Enable logical replication for Debezium
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_connections = 200;

-- Create replication user for Debezium
CREATE USER ${DEBEZIUM_USER} WITH REPLICATION LOGIN PASSWORD '${DEBEZIUM_PASSWORD}';

-- Grant necessary permissions
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${DEBEZIUM_USER};
GRANT USAGE ON SCHEMA public TO ${DEBEZIUM_USER};
EOF

# Execute SQL
psql -U postgres -f "$SQL_FILE"
rm -f "$SQL_FILE"

echo "✅ Replication setup completed"

