# Business Process Simulator with Kaggle Integration

A Python application that simulates a business process by fetching data from Kaggle datasets, reading local CSV files, creating PostgreSQL tables dynamically, and inserting the data into a PostgreSQL database. This system integrates with Kaggle's mlcroissant library to fetch real-world datasets.

## 🎯 Project Overview

This project implements a comprehensive **Business Process Simulator** that demonstrates the complete workflow of fetching data from Kaggle datasets, reading local CSV files, creating PostgreSQL tables dynamically, and inserting data into a database. The solution is production-ready with Docker support, comprehensive logging, and robust error handling.

## ✅ Features

- **Kaggle Data Integration**: Fetches real-world datasets from Kaggle using mlcroissant
- **CSV Data Processing**: Reads data from NCR ride bookings CSV file
- **Dynamic Table Creation**: Automatically creates PostgreSQL tables based on CSV structure
- **Data Type Inference**: Intelligently infers PostgreSQL column types from pandas DataFrames
- **Batch Data Insertion**: Efficiently inserts large amounts of data
- **Docker Support**: Complete Docker Compose setup with PostgreSQL and pgAdmin
- **Comprehensive Logging**: Detailed logging for monitoring and debugging
- **Business Reporting**: Generates JSON reports of the simulation process
- **Data Cleaning**: Automatic data cleaning and standardization
- **Real-World Data**: Uses actual NCR ride booking data from Kaggle

## 📁 Project Structure

```
WPBD/
├── business_process_simulator.py # Main application script with Kaggle integration
├── data_seeder.py               # Kaggle data fetching script
├── docker-compose.yml           # Docker Compose configuration
├── Dockerfile                   # Docker image definition
├── requirements.txt             # Python dependencies
├── DOCUMENTATION.md             # This comprehensive documentation
├── data/                        # CSV data files
│   └── ncr_ride_bookings.csv    # NCR ride booking data from Kaggle
│   └── kaggle/                  # Kaggle data (created at runtime)
└── logs/                        # Application logs (created at runtime)
```

## 🚀 Quick Start

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

2. **Start PostgreSQL** (if not using Docker):
   ```bash
   docker-compose up postgres -d
   ```

3. **Run the application**:
   ```bash
   python business_process_simulator.py
   ```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | localhost |
| `DB_PORT` | PostgreSQL port | 5432 |
| `DB_NAME` | Database name | business_db |
| `DB_USER` | Database user | postgres |
| `DB_PASSWORD` | Database password | password |

### CSV File Configuration

The application uses the NCR ride bookings data:
- `ncr_ride_bookings.csv` - Real-world ride booking data from Kaggle with booking details, customer info, vehicle types, locations, and ratings

## 🏗️ Architecture

### Core Components

1. **BusinessProcessSimulator Class**
   - Database connection management
   - CSV file processing
   - Dynamic table creation
   - Data insertion with error handling

2. **Data Processing Pipeline**
   ```
   Kaggle Data → Data Seeder → CSV Files → Pandas DataFrames → Type Inference → PostgreSQL Tables → Data Insertion
   ```

3. **Database Schema**
   - Auto-generated tables with proper constraints
   - Metadata columns (id, created_at, updated_at)
   - Indexes for performance optimization

### Data Type Mapping

| Pandas Type | PostgreSQL Type |
|-------------|-----------------|
| int64 | INTEGER |
| float64 | DECIMAL(15,4) |
| bool | BOOLEAN |
| datetime64 | TIMESTAMP |
| object/string | VARCHAR(n) |

## 📊 Data Flow

1. **Input**: Kaggle datasets + local CSV files with business data
2. **Processing**: 
   - Fetch data from Kaggle using mlcroissant
   - Read and validate CSV files
   - Infer data types and create table schemas
   - Establish database connection
3. **Storage**:
   - Create PostgreSQL tables dynamically
   - Insert data with batch processing
   - Add metadata and indexes
4. **Output**:
   - Structured database tables
   - Processing logs and reports
   - Success/failure status

## 📈 Output

### Logs
- Application logs: `business_process.log`
- Console output with real-time progress

### Reports
- JSON report: `business_process_report.json`
- Contains processing statistics and metadata

### Database Tables
- `business_[kaggle_dataset_name]` - Tables from Kaggle datasets
- `business_ncr_ride_bookings` - NCR ride booking data with customer info, vehicle types, locations, and ratings

## 🔍 Monitoring

### Health Checks
- PostgreSQL health check: `pg_isready`
- Service dependencies properly configured

### Logging
- Structured logging with timestamps
- Multiple log levels (INFO, ERROR, DEBUG)
- File and console output

## 🛠️ Troubleshooting

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

## 🔧 Technical Specifications

### Dependencies
- **pandas**: Data manipulation and analysis
- **psycopg2**: PostgreSQL database adapter
- **numpy**: Numerical computing
- **mlcroissant**: Kaggle data fetching

### Database Features
- **PostgreSQL 15**: Latest stable version
- **Connection Pooling**: Efficient database connections
- **Health Checks**: Automatic service monitoring
- **Data Persistence**: Docker volumes for data retention

### Performance Optimizations
- **Batch Inserts**: Efficient data loading
- **Index Creation**: Optimized query performance
- **Memory Management**: Efficient DataFrame processing
- **Connection Management**: Proper resource cleanup

## 🎓 Learning Outcomes

The project showcases:
- **Python Development**: Object-oriented programming, error handling
- **Database Design**: Dynamic schema creation, data type mapping
- **Docker Containerization**: Multi-service orchestration
- **Data Processing**: CSV handling, batch operations
- **DevOps Practices**: Health checks, logging, monitoring

## 🔮 Future Enhancements

Potential improvements:
- **Data Validation**: Schema validation and data quality checks
- **ETL Pipeline**: Extract, Transform, Load operations
- **API Integration**: REST API for data access
- **Real-time Processing**: Stream processing capabilities
- **Data Visualization**: Dashboard and reporting features

## 📋 Access Points

- **pgAdmin**: http://localhost:8080 (admin@business.com / admin123)
- **PostgreSQL**: localhost:5432
- **Logs**: `business_process.log`
- **Report**: `business_process_report.json`

## 🏆 Business Value

This simulator demonstrates:
- **Data Integration**: Seamless CSV to database migration
- **Process Automation**: Automated table creation and data loading
- **Scalability**: Handles varying data sizes and structures
- **Reliability**: Robust error handling and logging
- **Maintainability**: Clean, documented, and testable code

---

**Project Status**: ✅ Complete and Production-Ready  
**Last Updated**: December 2024  
**Author**: AI Assistant
