#!/usr/bin/env python3
"""
Business Process Simulator with Kaggle Data Integration
A Python script that simulates a business process by:
1. Fetching data from Kaggle datasets using mlcroissant
2. Reading data from local CSV files
3. Creating PostgreSQL tables dynamically
4. Inserting data into PostgreSQL
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import Dict, List, Any
import json
from datetime import datetime
# Data seeder is now a simple script, no class import needed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('business_process.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class BusinessProcessSimulator:
    """Enhanced simulator that integrates Kaggle data fetching with PostgreSQL operations."""
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the enhanced simulator.
        
        Args:
            db_config: Dictionary containing database connection parameters
        """
        self.db_config = db_config
        self.connection = None
        # Data seeder is now a simple script, no class instance needed
        self.dataframes = {}
        
    def connect_to_database(self) -> bool:
        """Establish connection to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            self.connection.autocommit = True
            logger.info("Successfully connected to PostgreSQL database")
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect_from_database(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from database")
    
    def fetch_kaggle_data(self) -> bool:
        """
        Fetch data from Kaggle using the data seeder.
        
        Returns:
            bool: True if data fetched successfully, False otherwise
        """
        logger.info("Fetching data from Kaggle...")
        
        try:
            # Run the data seeder to fetch and save data
            import subprocess
            result = subprocess.run(['python', 'data_seeder.py'], capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error("Failed to fetch Kaggle data")
                return False
            
            # Load the saved CSV files from data directory
            data_dir = 'data'
            if os.path.exists(data_dir):
                for filename in os.listdir(data_dir):
                    if filename.endswith('.csv') and filename.startswith('ncr_ride_bookings'):
                        filepath = os.path.join(data_dir, filename)
                        df = pd.read_csv(filepath)
                        dataset_name = filename.replace('.csv', '')
                        self.dataframes[dataset_name] = df
            
            logger.info(f"Successfully fetched {len(self.dataframes)} datasets from Kaggle")
            return True
            
        except Exception as e:
            logger.error(f"Error fetching Kaggle data: {e}")
            return False
    
    def load_local_csv_files(self, csv_file_paths: List[str]) -> bool:
        """
        Load additional CSV files from local directory.
        
        Args:
            csv_file_paths: List of paths to CSV files
            
        Returns:
            bool: True if all files loaded successfully, False otherwise
        """
        logger.info("Loading local CSV files...")
        success = True
        
        for file_path in csv_file_paths:
            try:
                if not os.path.exists(file_path):
                    logger.error(f"CSV file not found: {file_path}")
                    success = False
                    continue
                
                # Read CSV file
                df = pd.read_csv(file_path)
                filename = os.path.basename(file_path).replace('.csv', '')
                self.dataframes[f"local_{filename}"] = df
                
                logger.info(f"Successfully loaded {file_path}: {len(df)} rows, {len(df.columns)} columns")
                
            except Exception as e:
                logger.error(f"Error loading CSV file {file_path}: {e}")
                success = False
        
        return success
    
    def infer_column_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """Infer PostgreSQL column types from pandas DataFrame."""
        type_mapping = {}
        
        for column in df.columns:
            dtype = df[column].dtype
            
            if pd.api.types.is_integer_dtype(dtype):
                type_mapping[column] = 'INTEGER'
            elif pd.api.types.is_float_dtype(dtype):
                type_mapping[column] = 'DECIMAL(15,4)'
            elif pd.api.types.is_bool_dtype(dtype):
                type_mapping[column] = 'BOOLEAN'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                type_mapping[column] = 'TIMESTAMP'
            else:
                # For string/object types, determine max length
                max_length = df[column].astype(str).str.len().max()
                if pd.isna(max_length) or max_length == 0:
                    max_length = 255
                else:
                    max_length = min(max_length * 2, 1000)  # Add buffer, cap at 1000
                type_mapping[column] = f'VARCHAR({max_length})'
        
        return type_mapping
    
    def create_table_dynamically(self, table_name: str, df: pd.DataFrame) -> bool:
        """Create PostgreSQL table dynamically based on DataFrame structure."""
        try:
            cursor = self.connection.cursor()
            
            # Drop table if it exists
            drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
            cursor.execute(drop_query)
            logger.info(f"Dropped existing table {table_name} if it existed")
            
            # Infer column types
            column_types = self.infer_column_types(df)
            
            # Create column definitions
            column_definitions = []
            for column, pg_type in column_types.items():
                # Clean column name
                clean_column = column.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
                column_definitions.append(f'"{clean_column}" {pg_type}')
            
            # Create table query
            create_query = f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                {', '.join(column_definitions)},
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            cursor.execute(create_query)
            logger.info(f"Successfully created table {table_name}")
            
            # Create index on created_at for better query performance
            index_query = f"CREATE INDEX idx_{table_name}_created_at ON {table_name}(created_at);"
            cursor.execute(index_query)
            
            cursor.close()
            return True
            
        except psycopg2.Error as e:
            logger.error(f"Error creating table {table_name}: {e}")
            return False
    
    def insert_data_into_table(self, table_name: str, df: pd.DataFrame) -> bool:
        """Insert data from DataFrame into PostgreSQL table."""
        try:
            cursor = self.connection.cursor()
            
            # Clean column names
            clean_columns = [col.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '') 
                           for col in df.columns]
            
            # Prepare data for insertion
            df_clean = df.copy()
            df_clean.columns = clean_columns
            
            # Handle NaN values
            df_clean = df_clean.fillna('NULL')
            
            # Create insert query
            columns_str = ', '.join([f'"{col}"' for col in clean_columns])
            placeholders = ', '.join(['%s'] * len(clean_columns))
            
            insert_query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            """
            
            # Convert DataFrame to list of tuples for batch insert
            data_tuples = [tuple(row) for row in df_clean.values]
            
            # Execute batch insert
            cursor.executemany(insert_query, data_tuples)
            
            logger.info(f"Successfully inserted {len(data_tuples)} rows into {table_name}")
            cursor.close()
            return True
            
        except psycopg2.Error as e:
            logger.error(f"Error inserting data into {table_name}: {e}")
            return False
    
    def process_all_data(self, local_csv_paths: List[str] = None) -> bool:
        """
        Main method to process all data sources.
        
        Args:
            local_csv_paths: Optional list of local CSV file paths
            
        Returns:
            bool: True if all operations successful, False otherwise
        """
        logger.info("Starting business process simulation")
        
        # Step 1: Connect to database
        if not self.connect_to_database():
            return False
        
        try:
            # Step 2: Fetch data from Kaggle
            if not self.fetch_kaggle_data():
                logger.warning("Failed to fetch Kaggle data, continuing with local data only")
            
            # Step 3: Load local CSV files if provided
            if local_csv_paths:
                if not self.load_local_csv_files(local_csv_paths):
                    logger.warning("Some local CSV files failed to load")
            
            if not self.dataframes:
                logger.error("No data available for processing")
                return False
            
            # Step 4: Create tables and insert data
            for dataset_name, df in self.dataframes.items():
                table_name = f"business_{dataset_name}"
                
                logger.info(f"Processing {dataset_name} -> {table_name}")
                
                # Create table
                if not self.create_table_dynamically(table_name, df):
                    return False
                
                # Insert data
                if not self.insert_data_into_table(table_name, df):
                    return False
            
            logger.info("Business process simulation completed successfully")
            return True
            
        finally:
            self.disconnect_from_database()
    
    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """Generate a comprehensive report of the business process."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'simulation_type': 'enhanced_with_kaggle_integration',
            'total_datasets': len(self.dataframes),
            'total_records': sum(len(df) for df in self.dataframes.values()),
            'data_sources': {
                'kaggle_datasets': len([name for name in self.dataframes.keys() if not name.startswith('local_')]),
                'local_datasets': len([name for name in self.dataframes.keys() if name.startswith('local_')])
            },
            'datasets': {}
        }
        
        for name, df in self.dataframes.items():
            report['datasets'][name] = {
                'rows': len(df),
                'columns': len(df.columns),
                'column_names': list(df.columns),
                'memory_usage': df.memory_usage(deep=True).sum(),
                'data_types': df.dtypes.to_dict(),
                'source': 'kaggle' if not name.startswith('local_') else 'local'
            }
        
        return report

def main():
    """Main function to run the business process simulator."""
    
    # Database configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'business_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'password')
    }
    
    # Local CSV file path - NCR ride bookings data
    local_csv_paths = [
        'data/ncr_ride_bookings.csv'
    ]
    
    # Create simulator instance
    simulator = BusinessProcessSimulator(db_config)
    
    # Process all data sources
    success = simulator.process_all_data(local_csv_paths)
    
    if success:
        # Generate and save comprehensive report
        report = simulator.generate_comprehensive_report()
        
        with open('business_process_report.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info("Business process completed successfully!")
        logger.info(f"Report saved to: business_process_report.json")
        
        print("\n" + "="*60)
        print("BUSINESS PROCESS SIMULATION COMPLETE")
        print("="*60)
        print(f"Total Datasets Processed: {report['total_datasets']}")
        print(f"Total Records: {report['total_records']}")
        print(f"Kaggle Datasets: {report['data_sources']['kaggle_datasets']}")
        print(f"Local Datasets: {report['data_sources']['local_datasets']}")
        print("="*60)
        
        # Print dataset details
        for name, info in report['datasets'].items():
            print(f"\n{name} ({info['source']}):")
            print(f"  Rows: {info['rows']}")
            print(f"  Columns: {info['columns']}")
            print(f"  Memory: {info['memory_usage']} bytes")
        
        print("="*60)
    else:
        logger.error("Business process simulation failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()