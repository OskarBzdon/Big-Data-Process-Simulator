## WPBD: Business Process Simulator, Debezium CDC, and Kafka JSON Streaming

This repository demonstrates an end-to-end data workflow:
- Simulate a business process that reads CSVs, dynamically creates PostgreSQL tables, and inserts data (Task 1)
- Capture database changes with Debezium and stream JSON events to Kafka (Task 2)
- Run a Kafka cluster with JSON serialization and validate events with a consumer (Task 3)

For a deeper overview and setup steps, see `DOCUMENTATION.md`.

### Quick start
- Build and run the stack: `docker compose up --build`
- Optional: manage topics: `./manage-kafka-topics.sh`
- Verify Kafka streaming: `python verify-kafka-streaming.py`
- Verify Debezium CDC: `python verify-debezium.py`

### File responsibilities

- `business_process_simulator.py`: Orchestrates the business process. Connects to PostgreSQL, loads CSVs (local and via `data_seeder.py`), infers types, creates tables dynamically, inserts data, and generates a JSON report.
- `data_seeder.py`: Fetches Kaggle data using `mlcroissant`, cleans column names, fills missing values, and writes CSVs to `data/`.
- `docker-compose.yml`: Defines the runtime stack: PostgreSQL (with logical replication), pgAdmin, Zookeeper, Kafka, Kafka Connect (Debezium), Schema Registry, and the `business_app` container running the simulator.
- `Dockerfile`: Builds the Python image for `business_app` with system libraries for PostgreSQL, installs `requirements.txt`, and sets a default command.
- `requirements.txt`: Python dependencies for CSV processing, PostgreSQL access, Debezium/Kafka verification, and utilities.
- `DOCUMENTATION.md`: Detailed project overview, architecture, configuration, and troubleshooting guide.

- `data/`: Input CSVs for the simulator (e.g., `ncr_ride_bookings.csv`).
- `logs/`: Output directory for runtime logs (mounted into the app container).

- `init-scripts/setup-replication.sql`: Postgres init script enabling logical replication and creating the `debezium_user` role with grants. Executes only on first DB initialization of the volume.

- `debezium-connectors/postgres-connector.json`: Debezium PostgreSQL connector config. Captures `public.business_*` tables, emits JSON (schemas disabled), unwraps the envelope, and adds `op` and `ts_ms` fields.

- `register-debezium-connector.sh`: Registers the Debezium connector with Kafka Connect via REST using the JSON in `debezium-connectors/`.

- `manage-kafka-topics.sh`: Creates and lists Kafka topics used by the examples (e.g., `business-ncr-ride-bookings`).
- `kafka-json-consumer.py`: Simple JSON consumer that connects to Kafka, subscribes to business topics, validates/prints JSON payloads.
- `verify-kafka-streaming.py`: Verifies Kafka cluster availability, Schema Registry, JSON producer/consumer path, and basic consumer validation.
- `verify-debezium.py`: Inserts a test row into `business_ncr_ride_bookings` and confirms a JSON CDC event arrives on the Debezium topic.

### Service endpoints
- PostgreSQL: `localhost:5432` (DB `business_db`, user `postgres`/`password`)
- pgAdmin: `http://localhost:8080` (admin@business.com / admin123)
- Kafka: `localhost:9092`
- Kafka Connect (Debezium): `http://localhost:8083`
- Schema Registry: `http://localhost:8081`

### Notes
- Compose mounts `./init-scripts` into Postgres; the SQL init runs only when the data volume is created fresh.
- Debezium connector captures tables named `public.business_*` that the simulator creates from CSVs.

