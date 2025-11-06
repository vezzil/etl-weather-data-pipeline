"""
Integration tests for the complete ETL pipeline
"""

import pytest
import os
import tempfile
import pandas as pd
from unittest.mock import patch, Mock
from datetime import datetime

from src.ingest import WeatherDataIngestion, WeatherDataPoint
from src.transform import WeatherDataTransformer
from src.load import WeatherDataLoader, DatabaseConfig


@pytest.mark.integration
class TestETLPipelineIntegration:
    """Integration tests for the complete ETL pipeline"""
    
    @patch('src.ingest.WeatherAPIClient.fetch_multiple_cities')
    def test_full_etl_pipeline_sqlite(self, mock_fetch):
        """Test complete ETL pipeline with SQLite"""
        # Setup mock data
        mock_weather_data = [
            WeatherDataPoint(
                city="London", country="GB", timestamp=datetime(2023, 11, 7, 12, 0, 0),
                temperature=15.0, feels_like=14.0, humidity=65, pressure=1013,
                description="cloudy", wind_speed=3.0, wind_direction=180,
                cloudiness=40, visibility=10, lat=51.5, lon=-0.1
            ),
            WeatherDataPoint(
                city="Paris", country="FR", timestamp=datetime(2023, 11, 7, 12, 0, 0),
                temperature=18.0, feels_like=17.0, humidity=60, pressure=1015,
                description="sunny", wind_speed=2.0, wind_direction=90,
                cloudiness=20, visibility=12, lat=48.9, lon=2.3
            )
        ]
        mock_fetch.return_value = mock_weather_data
        
        # Create temporary database
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
            db_path = tmp_db.name
        
        try:
            # Initialize components
            ingestion = WeatherDataIngestion("test_api_key")
            transformer = WeatherDataTransformer()
            config = DatabaseConfig('sqlite', database_path=db_path)
            loader = WeatherDataLoader(config)
            
            # Create database schema
            loader.create_schema()
            
            # Step 1: Ingest data
            raw_data = ingestion.ingest_data()
            assert len(raw_data) == 2
            assert all(isinstance(point, WeatherDataPoint) for point in raw_data)
            
            # Step 2: Transform data
            transformed_df = transformer.transform_weather_data(raw_data)
            assert isinstance(transformed_df, pd.DataFrame)
            assert len(transformed_df) > 0
            
            # Verify transformed data has required columns
            required_columns = [
                'city', 'country', 'timestamp', 'temperature', 'humidity',
                'pressure', 'quality_score', 'temp_category', 'season'
            ]
            for col in required_columns:
                assert col in transformed_df.columns
            
            # Step 3: Load data
            load_result = loader.load_data(transformed_df, 'insert')
            assert load_result['status'] == 'success'
            assert load_result['records_loaded'] > 0
            
            # Step 4: Load quality metrics
            quality_metrics = transformer.get_quality_metrics()
            loader.load_quality_metrics(quality_metrics)
            
            # Verify data in database
            summary = loader.get_data_summary()
            assert summary['total_records'] > 0
            assert summary['unique_cities'] == 2
            assert summary['unique_countries'] == 2
            
        finally:
            # Cleanup
            os.unlink(db_path)
    
    @patch('src.ingest.WeatherAPIClient.fetch_multiple_cities')
    def test_etl_pipeline_with_data_quality_issues(self, mock_fetch):
        """Test ETL pipeline handling data quality issues"""
        # Setup mock data with quality issues
        mock_weather_data = [
            WeatherDataPoint(
                city="London", country="GB", timestamp=datetime(2023, 11, 7, 12, 0, 0),
                temperature=15.0, feels_like=14.0, humidity=65, pressure=1013,
                description="cloudy", wind_speed=3.0, wind_direction=180,
                cloudiness=40, visibility=10, lat=51.5, lon=-0.1
            ),
            WeatherDataPoint(
                city="Invalid", country="XX", timestamp=datetime(2023, 11, 7, 12, 0, 0),
                temperature=999.0, feels_like=999.0, humidity=150, pressure=-100,  # Invalid data
                description="invalid", wind_speed=-10.0, wind_direction=500,
                cloudiness=150, visibility=-1, lat=200.0, lon=-300.0  # Invalid coordinates
            ),
            WeatherDataPoint(
                city="Paris", country="FR", timestamp=datetime(2023, 11, 7, 12, 0, 0),
                temperature=18.0, feels_like=17.0, humidity=60, pressure=1015,
                description="sunny", wind_speed=2.0, wind_direction=90,
                cloudiness=20, visibility=12, lat=48.9, lon=2.3
            )
        ]
        mock_fetch.return_value = mock_weather_data
        
        # Create temporary database
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
            db_path = tmp_db.name
        
        try:
            # Initialize components
            ingestion = WeatherDataIngestion("test_api_key")
            transformer = WeatherDataTransformer()
            config = DatabaseConfig('sqlite', database_path=db_path)
            loader = WeatherDataLoader(config)
            
            loader.create_schema()
            
            # Run ETL pipeline
            raw_data = ingestion.ingest_data()
            transformed_df = transformer.transform_weather_data(raw_data)
            load_result = loader.load_data(transformed_df, 'insert')
            
            # Verify that invalid data was filtered out
            quality_metrics = transformer.get_quality_metrics()
            assert quality_metrics['total_records_input'] == 3
            assert quality_metrics['total_records_output'] < 3  # Some records filtered
            assert quality_metrics['data_retention_rate'] < 1.0
            
            # Verify that valid data was still loaded
            assert load_result['status'] == 'success'
            summary = loader.get_data_summary()
            assert summary['total_records'] > 0
            
        finally:
            os.unlink(db_path)
    
    @patch('src.ingest.WeatherAPIClient.fetch_multiple_cities')
    def test_etl_pipeline_upsert_functionality(self, mock_fetch):
        """Test ETL pipeline upsert functionality"""
        # Initial data
        initial_data = [
            WeatherDataPoint(
                city="London", country="GB", timestamp=datetime(2023, 11, 7, 12, 0, 0),
                temperature=15.0, feels_like=14.0, humidity=65, pressure=1013,
                description="cloudy", wind_speed=3.0, wind_direction=180,
                cloudiness=40, visibility=10, lat=51.5, lon=-0.1
            )
        ]
        
        # Updated data (same city, time, but different temperature)
        updated_data = [
            WeatherDataPoint(
                city="London", country="GB", timestamp=datetime(2023, 11, 7, 12, 0, 0),
                temperature=20.0, feels_like=19.0, humidity=65, pressure=1013,  # Updated temperature
                description="cloudy", wind_speed=3.0, wind_direction=180,
                cloudiness=40, visibility=10, lat=51.5, lon=-0.1
            )
        ]
        
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
            db_path = tmp_db.name
        
        try:
            # Initialize components
            ingestion = WeatherDataIngestion("test_api_key")
            transformer = WeatherDataTransformer()
            config = DatabaseConfig('sqlite', database_path=db_path)
            loader = WeatherDataLoader(config)
            
            loader.create_schema()
            
            # First load
            mock_fetch.return_value = initial_data
            raw_data = ingestion.ingest_data()
            transformed_df = transformer.transform_weather_data(raw_data)
            load_result = loader.load_data(transformed_df, 'insert')
            
            assert load_result['status'] == 'success'
            initial_summary = loader.get_data_summary()
            assert initial_summary['total_records'] == 1
            
            # Second load with updated data (upsert)
            mock_fetch.return_value = updated_data
            raw_data = ingestion.ingest_data()
            transformed_df = transformer.transform_weather_data(raw_data)
            load_result = loader.load_data(transformed_df, 'upsert')
            
            assert load_result['status'] == 'success'
            
            # Should still have only 1 record, but with updated values
            final_summary = loader.get_data_summary()
            assert final_summary['total_records'] == 1
            
            # Temperature should be updated (can't easily verify without direct DB query)
            
        finally:
            os.unlink(db_path)
    
    def test_etl_pipeline_error_propagation(self):
        """Test error propagation through ETL pipeline"""
        # Test with invalid API key (should fail at ingestion)
        ingestion = WeatherDataIngestion("")  # Empty API key
        
        with patch('src.ingest.WeatherAPIClient.fetch_multiple_cities', return_value=[]):
            raw_data = ingestion.ingest_data()
            assert len(raw_data) == 0  # No data ingested
        
        # Test transformation with empty data
        transformer = WeatherDataTransformer()
        transformed_df = transformer.transform_weather_data([])
        assert len(transformed_df) == 0
        
        # Test loading with empty DataFrame
        config = DatabaseConfig('sqlite', database_path=':memory:')
        loader = WeatherDataLoader(config)
        load_result = loader.load_data(transformed_df)
        
        assert load_result['status'] == 'skipped'
        assert load_result['records_loaded'] == 0
    
    @patch('src.ingest.WeatherAPIClient.fetch_multiple_cities')
    def test_etl_pipeline_performance_metrics(self, mock_fetch):
        """Test ETL pipeline performance tracking"""
        # Create larger dataset to test performance
        mock_weather_data = []
        cities = ["London", "Paris", "Tokyo", "New York", "Sydney"]
        
        for i, city in enumerate(cities):
            mock_weather_data.append(
                WeatherDataPoint(
                    city=city, country="XX", timestamp=datetime(2023, 11, 7, 12, i, 0),
                    temperature=15.0 + i, feels_like=14.0 + i, humidity=65, pressure=1013,
                    description="cloudy", wind_speed=3.0, wind_direction=180,
                    cloudiness=40, visibility=10, lat=51.5 + i, lon=-0.1 + i
                )
            )
        
        mock_fetch.return_value = mock_weather_data
        
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
            db_path = tmp_db.name
        
        try:
            # Time the ETL process
            start_time = datetime.now()
            
            ingestion = WeatherDataIngestion("test_api_key")
            transformer = WeatherDataTransformer()
            config = DatabaseConfig('sqlite', database_path=db_path)
            loader = WeatherDataLoader(config)
            
            loader.create_schema()
            
            # Run pipeline
            raw_data = ingestion.ingest_data()
            transformed_df = transformer.transform_weather_data(raw_data)
            load_result = loader.load_data(transformed_df, 'insert')
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Verify performance metrics
            assert load_result['load_duration_seconds'] > 0
            assert duration < 30  # Should complete within 30 seconds
            
            # Verify data quality metrics
            quality_metrics = transformer.get_quality_metrics()
            assert quality_metrics['total_records_input'] == 5
            assert quality_metrics['total_records_output'] == 5  # All should be valid
            assert quality_metrics['data_retention_rate'] == 1.0
            assert quality_metrics['unique_cities'] == 5
            
            summary = loader.get_data_summary()
            assert summary['total_records'] == 5
            assert summary['unique_cities'] == 5
            
        finally:
            os.unlink(db_path)


@pytest.mark.slow
class TestETLPipelineStress:
    """Stress tests for ETL pipeline"""
    
    @patch('src.ingest.WeatherAPIClient.fetch_multiple_cities')
    def test_large_dataset_processing(self, mock_fetch):
        """Test ETL pipeline with large dataset"""
        # Create large mock dataset
        mock_weather_data = []
        for i in range(100):  # 100 records
            mock_weather_data.append(
                WeatherDataPoint(
                    city=f"City{i}", country="XX", timestamp=datetime(2023, 11, 7, 12, i % 60, 0),
                    temperature=15.0 + (i % 30), feels_like=14.0 + (i % 30), 
                    humidity=50 + (i % 50), pressure=1000 + (i % 50),
                    description="test", wind_speed=i % 20, wind_direction=(i * 10) % 360,
                    cloudiness=i % 100, visibility=10 + (i % 10), 
                    lat=40.0 + (i % 180) - 90, lon=-74.0 + (i % 360) - 180
                )
            )
        
        mock_fetch.return_value = mock_weather_data
        
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
            db_path = tmp_db.name
        
        try:
            start_time = datetime.now()
            
            ingestion = WeatherDataIngestion("test_api_key")
            transformer = WeatherDataTransformer()
            config = DatabaseConfig('sqlite', database_path=db_path)
            loader = WeatherDataLoader(config)
            
            loader.create_schema()
            
            # Process in batches
            batch_size = 25
            total_processed = 0
            
            for i in range(0, len(mock_weather_data), batch_size):
                batch = mock_weather_data[i:i + batch_size]
                
                transformed_df = transformer.transform_weather_data(batch)
                load_result = loader.load_data(transformed_df, 'insert')
                
                assert load_result['status'] == 'success'
                total_processed += load_result['records_loaded']
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Performance assertions
            assert duration < 60  # Should complete within 1 minute
            assert total_processed > 50  # Most records should be processed
            
            summary = loader.get_data_summary()
            assert summary['total_records'] > 50
            
        finally:
            os.unlink(db_path)