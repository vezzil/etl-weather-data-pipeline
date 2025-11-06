"""
Tests for the weather data loading module
"""

import pytest
import pandas as pd
import sqlite3
from unittest.mock import Mock, patch
from datetime import datetime
from src.load import DatabaseConfig, WeatherDataLoader


class TestDatabaseConfig:
    """Test cases for DatabaseConfig"""
    
    def test_postgresql_config(self):
        """Test PostgreSQL configuration"""
        config = DatabaseConfig(
            'postgresql',
            host='localhost',
            port=5432,
            database='test_db',
            username='user',
            password='pass'
        )
        
        assert config.db_type == 'postgresql'
        assert config.host == 'localhost'
        assert config.port == 5432
        assert config.database == 'test_db'
        assert config.username == 'user'
        assert config.password == 'pass'
        assert 'postgresql://user:pass@localhost:5432/test_db' in config.connection_string
    
    def test_sqlite_config(self):
        """Test SQLite configuration"""
        config = DatabaseConfig('sqlite', database_path='test.db')
        
        assert config.db_type == 'sqlite'
        assert config.database_path == 'test.db'
        assert config.connection_string == 'sqlite:///test.db'
    
    def test_unsupported_database(self):
        """Test unsupported database type"""
        with pytest.raises(ValueError):
            DatabaseConfig('mysql')


class TestWeatherDataLoader:
    """Test cases for WeatherDataLoader"""
    
    def test_init(self, test_database_config):
        """Test loader initialization"""
        with patch('sqlalchemy.create_engine') as mock_engine:
            mock_engine.return_value.connect.return_value.__enter__.return_value.execute.return_value = None
            
            loader = WeatherDataLoader(test_database_config)
            assert loader.config == test_database_config
            assert loader.logger is not None
    
    def test_create_schema_sqlite(self, test_database_config):
        """Test schema creation for SQLite"""
        with patch('sqlalchemy.create_engine') as mock_engine:
            mock_conn = Mock()
            mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
            
            loader = WeatherDataLoader(test_database_config)
            loader.create_schema()
            
            # Verify that SQL commands were executed
            assert mock_conn.execute.call_count >= 3  # At least 3 tables created
            mock_conn.commit.assert_called()
    
    @patch('sqlalchemy.create_engine')
    def test_load_data_insert(self, mock_engine, sample_weather_dataframe):
        """Test data loading with insert strategy"""
        mock_conn = Mock()
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        loader = WeatherDataLoader(DatabaseConfig('sqlite', database_path=':memory:'))
        
        # Mock the to_sql method
        with patch.object(sample_weather_dataframe, 'to_sql', return_value=3) as mock_to_sql:
            result = loader.load_data(sample_weather_dataframe, 'insert')
        
        assert result['status'] == 'success'
        assert result['records_loaded'] == 3
        assert result['records_failed'] == 0
        mock_to_sql.assert_called_once()
    
    @patch('sqlalchemy.create_engine')
    def test_load_data_replace(self, mock_engine, sample_weather_dataframe):
        """Test data loading with replace strategy"""
        mock_conn = Mock()
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        loader = WeatherDataLoader(DatabaseConfig('sqlite', database_path=':memory:'))
        
        with patch.object(sample_weather_dataframe, 'to_sql', return_value=3) as mock_to_sql:
            result = loader.load_data(sample_weather_dataframe, 'replace')
        
        assert result['status'] == 'success'
        assert result['records_loaded'] == 3
        mock_to_sql.assert_called_with(
            'weather_data',
            loader.engine,
            if_exists='replace',
            index=False,
            method='multi'
        )
    
    @patch('sqlalchemy.create_engine')
    def test_load_data_empty_dataframe(self, mock_engine):
        """Test data loading with empty DataFrame"""
        mock_conn = Mock()
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        loader = WeatherDataLoader(DatabaseConfig('sqlite', database_path=':memory:'))
        empty_df = pd.DataFrame()
        
        result = loader.load_data(empty_df)
        
        assert result['status'] == 'skipped'
        assert result['records_loaded'] == 0
    
    @patch('sqlalchemy.create_engine')
    def test_load_upsert_sqlite(self, mock_engine):
        """Test upsert functionality for SQLite"""
        mock_conn = Mock()
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        loader = WeatherDataLoader(DatabaseConfig('sqlite', database_path=':memory:'))
        
        # Create test DataFrame
        data = {
            'city': ['London', 'Paris'],
            'country': ['GB', 'FR'],
            'timestamp': [datetime.now(), datetime.now()],
            'date': [datetime.now().date(), datetime.now().date()],
            'hour': [12, 12],
            'day_of_week': ['Tuesday', 'Tuesday'],
            'month': ['November', 'November'],
            'season': ['Autumn', 'Autumn'],
            'temperature': [15.0, 18.0],
            'feels_like': [14.0, 17.0],
            'humidity': [65, 60],
            'pressure': [1013, 1015],
            'description': ['cloudy', 'sunny'],
            'wind_speed': [3.0, 2.0],
            'wind_direction': [180, 90],
            'cloudiness': [40, 20],
            'visibility': [10.0, 12.0],
            'lat': [51.5, 48.9],
            'lon': [-0.1, 2.3],
            'temp_category': ['Cool', 'Cool'],
            'humidity_category': ['Moderate', 'Moderate'],
            'wind_category': ['Light', 'Light'],
            'comfort_index': [14.5, 17.5],
            'location': ['London, GB', 'Paris, FR'],
            'coord_string': ['51.5,-0.1', '48.9,2.3'],
            'quality_score': [95.0, 98.0]
        }
        df = pd.DataFrame(data)
        
        result = loader._load_upsert_sqlite(df)
        
        assert result['status'] == 'success'
        assert result['records_loaded'] == 2
        assert result['records_failed'] == 0
        assert mock_conn.execute.call_count == 2  # One call per record
        mock_conn.commit.assert_called_once()
    
    @patch('sqlalchemy.create_engine')
    def test_load_quality_metrics(self, mock_engine):
        """Test loading quality metrics"""
        mock_conn = Mock()
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        loader = WeatherDataLoader(DatabaseConfig('sqlite', database_path=':memory:'))
        
        metrics = {
            'total_records_input': 10,
            'total_records_output': 9,
            'data_retention_rate': 0.9,
            'average_quality_score': 85.5,
            'missing_values_percentage': 5.0,
            'unique_cities': 3,
            'unique_countries': 3,
            'timestamp_range': {
                'min': '2023-11-07T12:00:00',
                'max': '2023-11-07T13:00:00'
            }
        }
        
        loader.load_quality_metrics(metrics)
        
        mock_conn.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
    
    @patch('sqlalchemy.create_engine')
    def test_get_data_summary(self, mock_engine):
        """Test getting data summary"""
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.__getitem__.side_effect = [100, 5, 3, '2023-11-07', '2023-11-07', 15.5, 65.0, 90.0]
        mock_conn.execute.return_value.fetchone.return_value = mock_result
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        
        loader = WeatherDataLoader(DatabaseConfig('sqlite', database_path=':memory:'))
        summary = loader.get_data_summary()
        
        assert summary['total_records'] == 100
        assert summary['unique_cities'] == 5
        assert summary['unique_countries'] == 3
        assert summary['avg_temperature'] == 15.5
        assert summary['avg_humidity'] == 65.0
        assert summary['avg_quality_score'] == 90.0
    
    def test_get_weather_table_sql(self):
        """Test weather table SQL generation"""
        # Test PostgreSQL
        postgres_config = DatabaseConfig('postgresql')
        loader = WeatherDataLoader(postgres_config)
        sql = loader._get_weather_table_sql()
        
        assert 'CREATE TABLE IF NOT EXISTS weather_data' in sql
        assert 'SERIAL PRIMARY KEY' in sql
        assert 'TIMESTAMP' in sql
        
        # Test SQLite
        sqlite_config = DatabaseConfig('sqlite', database_path=':memory:')
        loader = WeatherDataLoader(sqlite_config)
        sql = loader._get_weather_table_sql()
        
        assert 'CREATE TABLE IF NOT EXISTS weather_data' in sql
        assert 'INTEGER PRIMARY KEY AUTOINCREMENT' in sql
        assert 'TEXT' in sql
    
    def test_get_metrics_table_sql(self):
        """Test metrics table SQL generation"""
        loader = WeatherDataLoader(DatabaseConfig('sqlite', database_path=':memory:'))
        sql = loader._get_metrics_table_sql()
        
        assert 'CREATE TABLE IF NOT EXISTS data_quality_metrics' in sql
        assert 'total_records_input' in sql
        assert 'data_retention_rate' in sql
    
    def test_get_load_history_table_sql(self):
        """Test load history table SQL generation"""
        loader = WeatherDataLoader(DatabaseConfig('sqlite', database_path=':memory:'))
        sql = loader._get_load_history_table_sql()
        
        assert 'CREATE TABLE IF NOT EXISTS load_history' in sql
        assert 'records_loaded' in sql
        assert 'load_duration_seconds' in sql


@pytest.mark.integration
class TestWeatherDataLoaderIntegration:
    """Integration tests for WeatherDataLoader"""
    
    def test_sqlite_full_workflow(self, sample_weather_dataframe, tmp_path):
        """Test complete workflow with SQLite"""
        # Use temporary database file
        db_path = tmp_path / "test_weather.db"
        config = DatabaseConfig('sqlite', database_path=str(db_path))
        loader = WeatherDataLoader(config)
        
        # Create schema
        loader.create_schema()
        
        # Verify tables were created
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        assert 'weather_data' in tables
        assert 'data_quality_metrics' in tables
        assert 'load_history' in tables
        
        # Add required columns to test DataFrame
        enhanced_df = sample_weather_dataframe.copy()
        enhanced_df['date'] = enhanced_df['timestamp'].dt.date
        enhanced_df['hour'] = enhanced_df['timestamp'].dt.hour
        enhanced_df['day_of_week'] = enhanced_df['timestamp'].dt.day_name()
        enhanced_df['month'] = enhanced_df['timestamp'].dt.month_name()
        enhanced_df['season'] = 'Autumn'
        enhanced_df['temp_category'] = 'Cool'
        enhanced_df['humidity_category'] = 'Moderate'
        enhanced_df['wind_category'] = 'Light'
        enhanced_df['comfort_index'] = enhanced_df['temperature'] - 1
        enhanced_df['location'] = enhanced_df['city'] + ', ' + enhanced_df['country']
        enhanced_df['coord_string'] = enhanced_df['lat'].astype(str) + ',' + enhanced_df['lon'].astype(str)
        enhanced_df['quality_score'] = 95.0
        
        # Load data
        result = loader.load_data(enhanced_df, 'insert')
        
        assert result['status'] == 'success'
        assert result['records_loaded'] == 3
        
        # Verify data was loaded
        cursor.execute("SELECT COUNT(*) FROM weather_data")
        count = cursor.fetchone()[0]
        assert count == 3
        
        # Test upsert (update existing records)
        enhanced_df.loc[0, 'temperature'] = 20.0  # Change temperature
        result = loader.load_data(enhanced_df, 'upsert')
        
        assert result['status'] == 'success'
        
        # Verify data was updated (should still be 3 records)
        cursor.execute("SELECT COUNT(*) FROM weather_data")
        count = cursor.fetchone()[0]
        assert count == 3
        
        # Load quality metrics
        metrics = {
            'total_records_input': 3,
            'total_records_output': 3,
            'data_retention_rate': 1.0,
            'average_quality_score': 95.0,
            'missing_values_percentage': 0.0,
            'unique_cities': 3,
            'unique_countries': 3,
            'timestamp_range': {
                'min': '2023-11-07T12:00:00',
                'max': '2023-11-07T12:00:00'
            }
        }
        loader.load_quality_metrics(metrics)
        
        # Verify metrics were loaded
        cursor.execute("SELECT COUNT(*) FROM data_quality_metrics")
        count = cursor.fetchone()[0]
        assert count == 1
        
        # Get data summary
        summary = loader.get_data_summary()
        assert summary['total_records'] == 3
        assert summary['unique_cities'] == 3
        
        conn.close()
    
    def test_database_error_handling(self):
        """Test error handling for database operations"""
        # Use invalid database path to trigger connection error
        config = DatabaseConfig('sqlite', database_path='/invalid/path/test.db')
        
        with pytest.raises(Exception):
            loader = WeatherDataLoader(config)
    
    def test_load_data_error_handling(self, sample_weather_dataframe):
        """Test error handling during data loading"""
        config = DatabaseConfig('sqlite', database_path=':memory:')
        
        with patch('sqlalchemy.create_engine') as mock_engine:
            # Setup mock to raise exception
            mock_engine.return_value.connect.return_value.__enter__.return_value = Mock()
            loader = WeatherDataLoader(config)
            
            # Mock to_sql to raise exception
            with patch.object(sample_weather_dataframe, 'to_sql', side_effect=Exception("Database error")):
                result = loader.load_data(sample_weather_dataframe, 'insert')
            
            assert result['status'] == 'failed'
            assert 'Database error' in result['error_message']