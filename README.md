# Business Process Simulator with Kaggle Integration

A Python application that simulates a business process by fetching data from Kaggle datasets, reading local CSV files, creating PostgreSQL tables dynamically, and inserting the data into a PostgreSQL database. This system integrates with Kaggle's mlcroissant library to fetch real-world datasets.

## Features

- **Kaggle Data Integration**: Fetches real-world datasets from Kaggle using mlcroissant
- **CSV Data Processing**: Reads data from multiple CSV files (both Kaggle and local)
- **Dynamic Table Creation**: Automatically creates PostgreSQL tables based on CSV structure
- **Data Type Inference**: Intelligently infers PostgreSQL column types from pandas DataFrames
- **Batch Data Insertion**: Efficiently inserts large amounts of data
- **Docker Support**: Complete Docker Compose setup with PostgreSQL and pgAdmin
- **Comprehensive Logging**: Detailed logging for monitoring and debugging
- **Business Reporting**: Generates JSON reports of the simulation process
- **Data Cleaning**: Automatic data cleaning and standardization
- **Multi-Source Processing**: Handles both Kaggle and local data sources

## Project Structure

```
WPBD/
├── business_process_simulator.py # Main application script with Kaggle integration
├── data_seeder.py               # Kaggle data fetching script
├── docker-compose.yml           # Docker Compose configuration
├── Dockerfile                   # Docker image definition
├── requirements.txt             # Python dependencies
├── config.env.example          # Environment configuration template
├── README.md                   # This file
├── test_simulator.py           # Test suite
├── data/                       # CSV test data files
│   ├── customers.csv
│   ├── orders.csv
│   ├── products.csv
│   └── kaggle/                 # Kaggle data (created at runtime)
└── logs/                       # Application logs (created at runtime)
```

## Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)
- PostgreSQL (if running locally without Docker)

## Quick Start

### Using Docker (Recommended)

1. **Clone and navigate to the project directory**:
   ```bash
   cd /Users/oskarbzdon/Repositories/WPBD
   ```

2. **Run the simulator**:
   ```bash
   docker-compose up --build
   ```

3. **Access pgAdmin** (optional):
   - Open http://localhost:8080
   - Login with: admin@business.com / admin123
   - Connect to PostgreSQL server: postgres:5432

### Local Development

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up environment variables**:
   ```bash
   cp config.env.example .env
   # Edit .env with your database credentials
   ```

3. **Start PostgreSQL** (if not using Docker):
   ```bash
   docker-compose up postgres -d
   ```

4. **Run the application**:
   ```bash
   python business_process_simulator.py
   ```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | localhost |
| `DB_PORT` | PostgreSQL port | 5432 |
| `DB_NAME` | Database name | business_db |
| `DB_USER` | Database user | postgres |
| `DB_PASSWORD` | Database password | password |

### CSV File Configuration

The application expects CSV files in the `data/` directory:
- `customers.csv` - Customer information
- `orders.csv` - Order details
- `products.csv` - Product catalog

## Data Processing

### Table Creation

The application automatically:
1. Analyzes CSV file structure
2. Infers appropriate PostgreSQL data types
3. Creates tables with proper constraints
4. Adds metadata columns (id, created_at, updated_at)

### Data Type Mapping

| Pandas Type | PostgreSQL Type |
|-------------|-----------------|
| int64 | INTEGER |
| float64 | DECIMAL(15,4) |
| bool | BOOLEAN |
| datetime64 | TIMESTAMP |
| object/string | VARCHAR(n) |

## Output

### Logs
- Application logs: `business_process.log`
- Console output with real-time progress

### Reports
- JSON report: `business_process_report.json`
- Contains processing statistics and metadata

### Database Test Tables
- `business_[kaggle_dataset_name]` - Tables from Kaggle datasets
- `business_local_customers` - Local customer data
- `business_local_orders` - Local order information
- `business_local_products` - Local product catalog

## Monitoring

### Health Checks
- PostgreSQL health check: `pg_isready`
- Service dependencies properly configured

### Logging
- Structured logging with timestamps
- Multiple log levels (INFO, ERROR, DEBUG)
- File and console output

## Troubleshooting

### Common Issues

1. **Database Connection Failed**:
   - Check if PostgreSQL is running
   - Verify connection credentials
   - Ensure port 5432 is available

2. **CSV File Not Found**:
   - Verify files exist in `data/` directory
   - Check file permissions
   - Ensure proper file paths

3. **Permission Denied**:
   - Check Docker volume permissions
   - Ensure proper file ownership

### Debug Mode

Enable debug logging by setting:
```bash
export LOG_LEVEL=DEBUG
```

## Development

### Adding New CSV Files

1. Add CSV file to `data/` directory
2. Update `csv_file_paths` in `main()` function
3. Restart the application

### Customizing Table Creation

Modify the `infer_column_types()` method to customize data type mapping.

### Extending Functionality

The `BusinessProcessSimulator` class is designed for extension:
- Add data validation methods
- Implement custom business rules
- Add data transformation logic

## License

This project is for educational and demonstration purposes.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request
