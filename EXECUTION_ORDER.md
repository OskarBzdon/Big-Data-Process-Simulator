# Business Process Simulator Execution Order

## Current Setup

### Dockerfile
- **Default CMD**: `python business_process_simulator.py`
- The simulator runs automatically when the container starts

### Execution Order in docker-compose.yml

1. **Infrastructure Services** (start in parallel):
   - `zookeeper` → `kafka` (waits for Kafka to be healthy)
   - `postgres` (waits for PostgreSQL to be healthy)
   - `schema-registry` (waits for Kafka to be healthy)

2. **Kafka Setup**:
   - `kafka-topics-init` (waits for Kafka to be healthy)
     - Creates custom Kafka topics

3. **Debezium Setup**:
   - `kafka-connect` (waits for Kafka to be healthy)
     - Starts Kafka Connect service
   - `connect-register` (waits for PostgreSQL + Kafka Connect to be healthy)
     - Registers Debezium connector
     - **Completion**: `service_completed_successfully`

4. **Consumers Start** (in parallel, after Debezium registration):
   - `spark` (waits for Kafka to be healthy)
     - Downloads Kafka connector dependencies
     - Starts Spark Structured Streaming job
     - **Healthcheck**: Verifies `spark-submit` process is running
     - **Health Status**: `service_healthy` (after 60s start period)
   
   - `consumer` (waits for Kafka + Debezium registration)
     - Python Kafka consumer starts listening
     - **Healthcheck**: Verifies Kafka connection
     - **Health Status**: `service_healthy`

5. **Business Simulator Runs** (waits for ALL consumers):
   - `business_app` depends on:
     - ✅ `connect-register`: `service_completed_successfully`
     - ✅ `consumer`: `service_healthy`
     - ✅ `spark`: `service_healthy` ← **NEWLY ADDED**
   
   - **Execution**: Only runs after ALL consumers are ready and healthy
   - **Process**:
     1. Connects to PostgreSQL
     2. Fetches/loads CSV data
     3. Creates tables dynamically
     4. Inserts data into PostgreSQL
     5. Debezium captures changes → Kafka
     6. Spark and Python consumer process the messages

## Key Points

### Spark Healthcheck
- **Test**: Checks if `spark-submit` process is running
- **Start Period**: 60 seconds (allows time for dependency download)
- **Interval**: 30 seconds
- **Retries**: 5 times

### Consumer Healthcheck
- **Test**: Verifies Kafka connection using kafka-python
- **Start Period**: 20 seconds
- **Interval**: 15 seconds
- **Retries**: 5 times

### Business Simulator Execution
- **Trigger**: Runs automatically via Dockerfile CMD
- **Prerequisites**: 
  - Debezium connector registered
  - Python consumer healthy and listening
  - Spark healthy and streaming
- **Result**: Data inserted → Debezium captures → Kafka → Consumers process

## Verification

To verify the execution order:
```bash
# Check service status
docker compose ps

# Check logs in order
docker compose logs connect-register
docker compose logs consumer
docker compose logs spark
docker compose logs business_app
```

## Timeline Example

```
0s    → Infrastructure starts (zookeeper, kafka, postgres)
30s   → Kafka healthy
45s   → Kafka Connect healthy
60s   → Debezium connector registered
75s   → Spark starts downloading dependencies
90s   → Consumer connects to Kafka (healthy)
120s  → Spark process running (healthy)
125s  → Business simulator starts ← ALL CONSUMERS READY
130s  → Data inserted into PostgreSQL
135s  → Debezium captures changes → Kafka
140s  → Spark and Consumer process messages
```

