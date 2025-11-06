"""
Weather Data Loading Module

This module handles loading transformed weather data into databases.
Supports both PostgreSQL and SQLite with automatic schema creation and data validation.
"""

import pandas as pd
import sqlite3
import psycopg2
import logging
from datetime import datetime
from typing import Dict, Optional, List, Union
import os
from contextlib import contextmanager
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
import json


class DatabaseConfig:
    """Configuration class for database connections"""
    
    def __init__(self, db_type: str, **kwargs):
        self.db_type = db_type.lower()
        
        if self.db_type == 'postgresql':
            self.host = kwargs.get('host', 'localhost')
            self.port = kwargs.get('port', 5432)
            self.database = kwargs.get('database', 'weather_db')
            self.username = kwargs.get('username', 'postgres')
            self.password = kwargs.get('password', '')
            self.connection_string = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        
        elif self.db_type == 'sqlite':
            self.database_path = kwargs.get('database_path', 'weather_data.db')
            self.connection_string = f"sqlite:///{self.database_path}"
        
        else:
            raise ValueError(f"Unsupported database type: {db_type}")


class WeatherDataLoader:
    """Handles loading weather data into databases"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = None
        self.logger = self._setup_logger()
        self._connect()
    
    def _setup_logger(self) -> logging.Logger:
        """Set up logging configuration"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _connect(self):
        """Establish database connection"""
        try:
            self.engine = create_engine(self.config.connection_string, echo=False)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info(f"Successfully connected to {self.config.db_type} database")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
    
    def create_schema(self):
        """Create database schema for weather data"""
        self.logger.info("Creating database schema...")
        
        # Weather data table
        weather_table_sql = self._get_weather_table_sql()
        
        # Data quality metrics table
        metrics_table_sql = self._get_metrics_table_sql()
        
        # Load history table
        load_history_sql = self._get_load_history_table_sql()
        
        try:
            with self.engine.connect() as conn:
                # Create tables
                conn.execute(text(weather_table_sql))
                conn.execute(text(metrics_table_sql))
                conn.execute(text(load_history_sql))
                
                # Create indexes for better performance
                self._create_indexes(conn)
                
                conn.commit()
                self.logger.info("Database schema created successfully")
                
        except Exception as e:
            self.logger.error(f"Failed to create schema: {e}")
            raise
    
    def _get_weather_table_sql(self) -> str:
        """Get SQL for creating weather data table"""
        if self.config.db_type == 'postgresql':
            return """
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                city VARCHAR(100) NOT NULL,
                country VARCHAR(10) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                date DATE NOT NULL,
                hour INTEGER NOT NULL,
                day_of_week VARCHAR(15) NOT NULL,
                month VARCHAR(15) NOT NULL,
                season VARCHAR(10) NOT NULL,
                temperature REAL NOT NULL,
                feels_like REAL NOT NULL,
                humidity INTEGER NOT NULL,
                pressure INTEGER NOT NULL,
                description VARCHAR(100) NOT NULL,
                wind_speed REAL NOT NULL,
                wind_direction INTEGER NOT NULL,
                cloudiness INTEGER NOT NULL,
                visibility REAL NOT NULL,
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                temp_category VARCHAR(20),
                humidity_category VARCHAR(20),
                wind_category VARCHAR(20),
                comfort_index REAL,
                location VARCHAR(150),
                coord_string VARCHAR(50),
                quality_score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(city, country, timestamp)
            );
            """
        else:  # SQLite
            return """
            CREATE TABLE IF NOT EXISTS weather_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT NOT NULL,
                country TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                date TEXT NOT NULL,
                hour INTEGER NOT NULL,
                day_of_week TEXT NOT NULL,
                month TEXT NOT NULL,
                season TEXT NOT NULL,
                temperature REAL NOT NULL,
                feels_like REAL NOT NULL,
                humidity INTEGER NOT NULL,
                pressure INTEGER NOT NULL,
                description TEXT NOT NULL,
                wind_speed REAL NOT NULL,
                wind_direction INTEGER NOT NULL,
                cloudiness INTEGER NOT NULL,
                visibility REAL NOT NULL,
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                temp_category TEXT,
                humidity_category TEXT,
                wind_category TEXT,
                comfort_index REAL,
                location TEXT,
                coord_string TEXT,
                quality_score REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(city, country, timestamp)
            );
            """
    
    def _get_metrics_table_sql(self) -> str:
        """Get SQL for creating data quality metrics table"""
        if self.config.db_type == 'postgresql':
            return """
            CREATE TABLE IF NOT EXISTS data_quality_metrics (
                id SERIAL PRIMARY KEY,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_records_input INTEGER,
                total_records_output INTEGER,
                data_retention_rate REAL,
                average_quality_score REAL,
                missing_values_percentage REAL,
                unique_cities INTEGER,
                unique_countries INTEGER,
                timestamp_min TIMESTAMP,
                timestamp_max TIMESTAMP,
                metrics_json TEXT
            );
            """
        else:  # SQLite
            return """
            CREATE TABLE IF NOT EXISTS data_quality_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                load_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                total_records_input INTEGER,
                total_records_output INTEGER,
                data_retention_rate REAL,
                average_quality_score REAL,
                missing_values_percentage REAL,
                unique_cities INTEGER,
                unique_countries INTEGER,
                timestamp_min TEXT,
                timestamp_max TEXT,
                metrics_json TEXT
            );
            """
    
    def _get_load_history_table_sql(self) -> str:
        """Get SQL for creating load history table"""
        if self.config.db_type == 'postgresql':
            return """
            CREATE TABLE IF NOT EXISTS load_history (
                id SERIAL PRIMARY KEY,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                records_loaded INTEGER,
                records_updated INTEGER,
                records_failed INTEGER,
                load_duration_seconds REAL,
                status VARCHAR(20),
                error_message TEXT,
                source_info TEXT
            );
            """
        else:  # SQLite
            return """
            CREATE TABLE IF NOT EXISTS load_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                load_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                records_loaded INTEGER,
                records_updated INTEGER,
                records_failed INTEGER,
                load_duration_seconds REAL,
                status TEXT,
                error_message TEXT,
                source_info TEXT
            );
            """
    
    def _create_indexes(self, conn):
        """Create database indexes for performance"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_weather_city_timestamp ON weather_data(city, timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_weather_country_date ON weather_data(country, date);",
            "CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_data(timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_weather_location ON weather_data(lat, lon);",
            "CREATE INDEX IF NOT EXISTS idx_weather_quality ON weather_data(quality_score);",
        ]
        
        for index_sql in indexes:
            try:
                conn.execute(text(index_sql))
            except Exception as e:
                self.logger.warning(f"Failed to create index: {e}")
    
    def load_data(self, df: pd.DataFrame, load_strategy: str = 'upsert') -> Dict:
        """
        Load weather data into the database
        
        Args:
            df: DataFrame with transformed weather data
            load_strategy: 'insert', 'upsert', or 'replace'
            
        Returns:
            Dictionary with load statistics
        """
        if df.empty:
            self.logger.warning("No data to load")
            return {'status': 'skipped', 'records_loaded': 0}
        
        start_time = datetime.now()
        self.logger.info(f"Loading {len(df)} records using {load_strategy} strategy")
        
        load_stats = {
            'records_loaded': 0,
            'records_updated': 0,
            'records_failed': 0,
            'status': 'success',
            'error_message': None
        }
        
        try:
            if load_strategy == 'replace':
                load_stats = self._load_replace(df)
            elif load_strategy == 'upsert':
                load_stats = self._load_upsert(df)
            else:  # insert
                load_stats = self._load_insert(df)
            
            # Calculate load duration
            load_duration = (datetime.now() - start_time).total_seconds()
            load_stats['load_duration_seconds'] = load_duration
            
            # Log load history
            self._log_load_history(load_stats, len(df))
            
            self.logger.info(f"Data load completed: {load_stats['records_loaded']} loaded, "
                           f"{load_stats['records_updated']} updated, "
                           f"{load_stats['records_failed']} failed")
            
        except Exception as e:
            load_stats['status'] = 'failed'
            load_stats['error_message'] = str(e)
            self.logger.error(f"Data load failed: {e}")
            self._log_load_history(load_stats, len(df))
            raise
        
        return load_stats
    
    def _load_insert(self, df: pd.DataFrame) -> Dict:
        """Load data using simple insert (may fail on duplicates)"""
        try:
            records_loaded = df.to_sql(
                'weather_data', 
                self.engine, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            return {
                'records_loaded': records_loaded or len(df),
                'records_updated': 0,
                'records_failed': 0,
                'status': 'success',
                'error_message': None
            }
        except Exception as e:
            return {
                'records_loaded': 0,
                'records_updated': 0,
                'records_failed': len(df),
                'status': 'failed',
                'error_message': str(e)
            }
    
    def _load_replace(self, df: pd.DataFrame) -> Dict:
        """Load data by replacing the entire table"""
        try:
            records_loaded = df.to_sql(
                'weather_data', 
                self.engine, 
                if_exists='replace', 
                index=False,
                method='multi'
            )
            return {
                'records_loaded': records_loaded or len(df),
                'records_updated': 0,
                'records_failed': 0,
                'status': 'success',
                'error_message': None
            }
        except Exception as e:
            return {
                'records_loaded': 0,
                'records_updated': 0,
                'records_failed': len(df),
                'status': 'failed',
                'error_message': str(e)
            }
    
    def _load_upsert(self, df: pd.DataFrame) -> Dict:
        """Load data using upsert (insert or update on conflict)"""
        if self.config.db_type == 'postgresql':
            return self._load_upsert_postgresql(df)
        else:
            return self._load_upsert_sqlite(df)
    
    def _load_upsert_postgresql(self, df: pd.DataFrame) -> Dict:
        """Upsert for PostgreSQL using ON CONFLICT"""
        records_loaded = 0
        records_updated = 0
        records_failed = 0
        
        try:
            with self.engine.connect() as conn:
                for _, row in df.iterrows():
                    try:
                        # Prepare the upsert query
                        upsert_sql = """
                        INSERT INTO weather_data (
                            city, country, timestamp, date, hour, day_of_week, month, season,
                            temperature, feels_like, humidity, pressure, description,
                            wind_speed, wind_direction, cloudiness, visibility, lat, lon,
                            temp_category, humidity_category, wind_category, comfort_index,
                            location, coord_string, quality_score
                        ) VALUES (
                            :city, :country, :timestamp, :date, :hour, :day_of_week, :month, :season,
                            :temperature, :feels_like, :humidity, :pressure, :description,
                            :wind_speed, :wind_direction, :cloudiness, :visibility, :lat, :lon,
                            :temp_category, :humidity_category, :wind_category, :comfort_index,
                            :location, :coord_string, :quality_score
                        )
                        ON CONFLICT (city, country, timestamp) 
                        DO UPDATE SET
                            temperature = EXCLUDED.temperature,
                            feels_like = EXCLUDED.feels_like,
                            humidity = EXCLUDED.humidity,
                            pressure = EXCLUDED.pressure,
                            description = EXCLUDED.description,
                            wind_speed = EXCLUDED.wind_speed,
                            wind_direction = EXCLUDED.wind_direction,
                            cloudiness = EXCLUDED.cloudiness,
                            visibility = EXCLUDED.visibility,
                            quality_score = EXCLUDED.quality_score
                        """
                        
                        result = conn.execute(text(upsert_sql), row.to_dict())
                        
                        # PostgreSQL doesn't easily tell us if it was insert or update
                        # For simplicity, count all as loaded
                        records_loaded += 1
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to upsert record for {row['city']}: {e}")
                        records_failed += 1
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Upsert operation failed: {e}")
            raise
        
        return {
            'records_loaded': records_loaded,
            'records_updated': records_updated,
            'records_failed': records_failed,
            'status': 'success',
            'error_message': None
        }
    
    def _load_upsert_sqlite(self, df: pd.DataFrame) -> Dict:
        """Upsert for SQLite using REPLACE"""
        records_loaded = 0
        records_failed = 0
        
        try:
            with self.engine.connect() as conn:
                for _, row in df.iterrows():
                    try:
                        # SQLite REPLACE statement
                        replace_sql = """
                        REPLACE INTO weather_data (
                            city, country, timestamp, date, hour, day_of_week, month, season,
                            temperature, feels_like, humidity, pressure, description,
                            wind_speed, wind_direction, cloudiness, visibility, lat, lon,
                            temp_category, humidity_category, wind_category, comfort_index,
                            location, coord_string, quality_score
                        ) VALUES (
                            :city, :country, :timestamp, :date, :hour, :day_of_week, :month, :season,
                            :temperature, :feels_like, :humidity, :pressure, :description,
                            :wind_speed, :wind_direction, :cloudiness, :visibility, :lat, :lon,
                            :temp_category, :humidity_category, :wind_category, :comfort_index,
                            :location, :coord_string, :quality_score
                        )
                        """
                        
                        conn.execute(text(replace_sql), row.to_dict())
                        records_loaded += 1
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to replace record for {row['city']}: {e}")
                        records_failed += 1
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Replace operation failed: {e}")
            raise
        
        return {
            'records_loaded': records_loaded,
            'records_updated': 0,
            'records_failed': records_failed,
            'status': 'success',
            'error_message': None
        }
    
    def load_quality_metrics(self, metrics: Dict):
        """Load data quality metrics into the database"""
        try:
            metrics_record = {
                'total_records_input': metrics.get('total_records_input', 0),
                'total_records_output': metrics.get('total_records_output', 0),
                'data_retention_rate': metrics.get('data_retention_rate', 0),
                'average_quality_score': metrics.get('average_quality_score', 0),
                'missing_values_percentage': metrics.get('missing_values_percentage', 0),
                'unique_cities': metrics.get('unique_cities', 0),
                'unique_countries': metrics.get('unique_countries', 0),
                'timestamp_min': metrics.get('timestamp_range', {}).get('min'),
                'timestamp_max': metrics.get('timestamp_range', {}).get('max'),
                'metrics_json': json.dumps(metrics)
            }
            
            with self.engine.connect() as conn:
                insert_sql = """
                INSERT INTO data_quality_metrics (
                    total_records_input, total_records_output, data_retention_rate,
                    average_quality_score, missing_values_percentage, unique_cities,
                    unique_countries, timestamp_min, timestamp_max, metrics_json
                ) VALUES (
                    :total_records_input, :total_records_output, :data_retention_rate,
                    :average_quality_score, :missing_values_percentage, :unique_cities,
                    :unique_countries, :timestamp_min, :timestamp_max, :metrics_json
                )
                """
                
                conn.execute(text(insert_sql), metrics_record)
                conn.commit()
                
            self.logger.info("Quality metrics loaded successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to load quality metrics: {e}")
            raise
    
    def _log_load_history(self, load_stats: Dict, total_records: int):
        """Log the load operation to history table"""
        try:
            history_record = {
                'records_loaded': load_stats.get('records_loaded', 0),
                'records_updated': load_stats.get('records_updated', 0),
                'records_failed': load_stats.get('records_failed', 0),
                'load_duration_seconds': load_stats.get('load_duration_seconds', 0),
                'status': load_stats.get('status', 'unknown'),
                'error_message': load_stats.get('error_message'),
                'source_info': f"Total records: {total_records}"
            }
            
            with self.engine.connect() as conn:
                insert_sql = """
                INSERT INTO load_history (
                    records_loaded, records_updated, records_failed, load_duration_seconds,
                    status, error_message, source_info
                ) VALUES (
                    :records_loaded, :records_updated, :records_failed, :load_duration_seconds,
                    :status, :error_message, :source_info
                )
                """
                
                conn.execute(text(insert_sql), history_record)
                conn.commit()
                
        except Exception as e:
            self.logger.warning(f"Failed to log load history: {e}")
    
    def get_data_summary(self) -> Dict:
        """Get summary statistics from the loaded data"""
        try:
            with self.engine.connect() as conn:
                summary_sql = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT city) as unique_cities,
                    COUNT(DISTINCT country) as unique_countries,
                    MIN(timestamp) as earliest_data,
                    MAX(timestamp) as latest_data,
                    AVG(temperature) as avg_temperature,
                    AVG(humidity) as avg_humidity,
                    AVG(quality_score) as avg_quality_score
                FROM weather_data
                """
                
                result = conn.execute(text(summary_sql)).fetchone()
                
                return {
                    'total_records': result[0] if result else 0,
                    'unique_cities': result[1] if result else 0,
                    'unique_countries': result[2] if result else 0,
                    'earliest_data': result[3] if result else None,
                    'latest_data': result[4] if result else None,
                    'avg_temperature': round(result[5], 2) if result and result[5] else None,
                    'avg_humidity': round(result[6], 2) if result and result[6] else None,
                    'avg_quality_score': round(result[7], 2) if result and result[7] else None,
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get data summary: {e}")
            return {}


def main():
    """Main function for running loading as a standalone script"""
    import sys
    from src.ingest import WeatherDataIngestion
    from src.transform import WeatherDataTransformer
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get API key from environment
    api_key = os.getenv('OPENWEATHER_API_KEY')
    if not api_key:
        print("Error: OPENWEATHER_API_KEY environment variable not set")
        sys.exit(1)
    
    # Database configuration
    db_type = os.getenv('DB_TYPE', 'sqlite')
    
    if db_type.lower() == 'postgresql':
        config = DatabaseConfig(
            'postgresql',
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', 5432)),
            database=os.getenv('DB_NAME', 'weather_db'),
            username=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', '')
        )
    else:
        config = DatabaseConfig('sqlite', database_path='weather_data.db')
    
    # Initialize components
    ingestion = WeatherDataIngestion(api_key)
    transformer = WeatherDataTransformer()
    loader = WeatherDataLoader(config)
    
    # Create schema
    loader.create_schema()
    
    # Get command line arguments
    cities_config = sys.argv[1] if len(sys.argv) > 1 else None
    load_strategy = sys.argv[2] if len(sys.argv) > 2 else 'upsert'
    
    # Run ETL pipeline
    print("Starting ETL pipeline...")
    
    print("1. Ingesting weather data...")
    weather_data = ingestion.ingest_data(cities_config)
    
    print("2. Transforming weather data...")
    transformed_df = transformer.transform_weather_data(weather_data)
    
    print("3. Loading weather data...")
    load_stats = loader.load_data(transformed_df, load_strategy)
    
    # Load quality metrics
    metrics = transformer.get_quality_metrics()
    loader.load_quality_metrics(metrics)
    
    # Print summary
    print(f"\nETL Pipeline Completed!")
    print(f"Load Statistics: {load_stats}")
    
    summary = loader.get_data_summary()
    print(f"Database Summary: {summary}")


if __name__ == "__main__":
    main()