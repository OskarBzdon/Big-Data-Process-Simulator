-- Enable logical replication for Debezium
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_connections = 200;

-- Create replication user for Debezium
CREATE USER debezium_user WITH REPLICATION LOGIN PASSWORD 'debezium_password';

-- Grant necessary permissions
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;
