"""
Tests for the weather data transformation module
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from src.transform import WeatherDataTransformer


class TestWeatherDataTransformer:
    """Test cases for WeatherDataTransformer"""
    
    def test_init(self):
        """Test transformer initialization"""
        transformer = WeatherDataTransformer()
        assert transformer.logger is not None
        assert transformer.quality_metrics == {}
    
    def test_transform_weather_data_empty_input(self):
        """Test transformation with empty input"""
        transformer = WeatherDataTransformer()
        result = transformer.transform_weather_data([])
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_transform_weather_data_success(self, sample_weather_data):
        """Test successful data transformation"""
        transformer = WeatherDataTransformer()
        result = transformer.transform_weather_data(sample_weather_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        
        # Check that all required columns are present
        expected_columns = [
            'city', 'country', 'timestamp', 'temperature', 'feels_like',
            'humidity', 'pressure', 'description', 'wind_speed', 'wind_direction',
            'cloudiness', 'visibility', 'lat', 'lon', 'date', 'hour',
            'day_of_week', 'month', 'season', 'temp_category', 'humidity_category',
            'wind_category', 'comfort_index', 'location', 'coord_string', 'quality_score'
        ]
        
        for col in expected_columns:
            assert col in result.columns
        
        # Check data types
        assert pd.api.types.is_datetime64_any_dtype(result['timestamp'])
        assert pd.api.types.is_numeric_dtype(result['temperature'])
        assert pd.api.types.is_numeric_dtype(result['quality_score'])
    
    def test_convert_to_dataframe(self, sample_weather_data):
        """Test conversion from WeatherDataPoint objects to DataFrame"""
        transformer = WeatherDataTransformer()
        df = transformer._convert_to_dataframe(sample_weather_data)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert 'city' in df.columns
        assert 'temperature' in df.columns
        assert pd.api.types.is_datetime64_any_dtype(df['timestamp'])
    
    def test_clean_data(self, sample_weather_dataframe):
        """Test data cleaning functionality"""
        transformer = WeatherDataTransformer()
        
        # Add some dirty data
        dirty_df = sample_weather_dataframe.copy()
        dirty_df.loc[3] = dirty_df.loc[0].copy()  # Duplicate row
        dirty_df.loc[3, 'city'] = ' london '  # Needs cleaning
        dirty_df.loc[3, 'description'] = ' PARTLY CLOUDY '  # Needs cleaning
        
        result = transformer._clean_data(dirty_df)
        
        # Check that duplicates are removed
        assert len(result) == 3  # Should remove the duplicate
        
        # Check text cleaning
        cities = result['city'].tolist()
        assert all(city.istitle() for city in cities)  # Title case
        
        countries = result['country'].tolist()
        assert all(country.isupper() for country in countries)  # Upper case
        
        descriptions = result['description'].tolist()
        assert all(desc.islower() for desc in descriptions)  # Lower case
    
    def test_handle_missing_values(self):
        """Test missing value handling"""
        transformer = WeatherDataTransformer()
        
        # Create DataFrame with missing values
        data = {
            'temperature': [15.0, 18.0, None],  # Missing critical field
            'humidity': [65, 70, 55],
            'pressure': [1013, 1015, 1020],
            'wind_speed': [3.2, None, 1.5],  # Missing optional field
            'wind_direction': [180, None, 45],  # Missing optional field
            'visibility': [10.0, None, 15.0],  # Missing optional field
            'lat': [51.5, 40.7, 35.7],
            'lon': [-0.1, -74.0, 139.7]
        }
        df = pd.DataFrame(data)
        
        result = transformer._handle_missing_values(df)
        
        # Should remove rows with missing critical fields
        assert len(result) == 2
        
        # Should fill missing optional fields
        assert result['wind_speed'].isna().sum() == 0
        assert result['wind_direction'].isna().sum() == 0
        assert result['visibility'].isna().sum() == 0
    
    def test_normalize_data(self):
        """Test data normalization"""
        transformer = WeatherDataTransformer()
        
        # Create DataFrame with values that need normalization
        data = {
            'humidity': [65, 150, -10],  # Values outside 0-100 range
            'cloudiness': [40, 120, -5],  # Values outside 0-100 range
            'wind_direction': [180, 450, -30],  # Values outside 0-360 range
            'visibility': [10.0, 5.0, -2.0],  # Negative visibility
            'temperature': [15.123456, 18.987654, 22.345678],  # High precision
            'lat': [51.123456789, 40.987654321, 35.567890123],  # High precision
            'lon': [-0.123456789, -74.987654321, 139.567890123],  # High precision
        }
        df = pd.DataFrame(data)
        
        result = transformer._normalize_data(df)
        
        # Check humidity normalization
        assert all(0 <= h <= 100 for h in result['humidity'])
        
        # Check cloudiness normalization
        assert all(0 <= c <= 100 for c in result['cloudiness'])
        
        # Check wind direction normalization
        assert all(0 <= w < 360 for w in result['wind_direction'])
        
        # Check visibility normalization
        assert all(v >= 0 for v in result['visibility'])
        
        # Check precision rounding
        assert all(len(str(t).split('.')[-1]) <= 1 for t in result['temperature'])
        assert all(len(str(lat).split('.')[-1]) <= 6 for lat in result['lat'])
    
    def test_enrich_data(self, sample_weather_dataframe):
        """Test data enrichment with derived fields"""
        transformer = WeatherDataTransformer()
        result = transformer._enrich_data(sample_weather_dataframe)
        
        # Check temporal fields
        assert 'date' in result.columns
        assert 'hour' in result.columns
        assert 'day_of_week' in result.columns
        assert 'month' in result.columns
        assert 'season' in result.columns
        
        # Check categorization fields
        assert 'temp_category' in result.columns
        assert 'humidity_category' in result.columns
        assert 'wind_category' in result.columns
        
        # Check derived fields
        assert 'comfort_index' in result.columns
        assert 'location' in result.columns
        assert 'coord_string' in result.columns
        assert 'quality_score' in result.columns
        
        # Check specific values
        assert all(isinstance(score, (int, float)) for score in result['quality_score'])
        assert all(0 <= score <= 100 for score in result['quality_score'])
    
    def test_validate_data(self):
        """Test data validation and outlier removal"""
        transformer = WeatherDataTransformer()
        
        # Create DataFrame with outliers
        data = {
            'temperature': [15.0, -70.0, 70.0, 18.0],  # Extreme temperatures
            'pressure': [1013, 700, 1200, 1015],  # Extreme pressures
            'wind_speed': [3.2, 250.0, 1.5, 2.8],  # Extreme wind speed
            'lat': [51.5, 100.0, 35.7, 40.7],  # Invalid latitude
            'lon': [-0.1, -200.0, 139.7, -74.0],  # Invalid longitude
            'humidity': [65, 70, 55, 60],
            'wind_direction': [180, 90, 45, 270],
            'cloudiness': [40, 0, 10, 20],
            'visibility': [10.0, 12.0, 15.0, 8.0]
        }
        df = pd.DataFrame(data)
        
        result = transformer._validate_data(df)
        
        # Should remove outliers
        assert len(result) < len(df)
        
        # Remaining data should be within valid ranges
        assert all(-60 <= t <= 60 for t in result['temperature'])
        assert all(800 <= p <= 1100 for p in result['pressure'])
        assert all(ws <= 200 for ws in result['wind_speed'])
        assert all(-90 <= lat <= 90 for lat in result['lat'])
        assert all(-180 <= lon <= 180 for lon in result['lon'])
    
    def test_get_season(self):
        """Test season determination"""
        transformer = WeatherDataTransformer()
        
        assert transformer._get_season(12) == 'Winter'
        assert transformer._get_season(1) == 'Winter'
        assert transformer._get_season(2) == 'Winter'
        assert transformer._get_season(3) == 'Spring'
        assert transformer._get_season(4) == 'Spring'
        assert transformer._get_season(5) == 'Spring'
        assert transformer._get_season(6) == 'Summer'
        assert transformer._get_season(7) == 'Summer'
        assert transformer._get_season(8) == 'Summer'
        assert transformer._get_season(9) == 'Autumn'
        assert transformer._get_season(10) == 'Autumn'
        assert transformer._get_season(11) == 'Autumn'
    
    def test_categorize_temperature(self):
        """Test temperature categorization"""
        transformer = WeatherDataTransformer()
        
        assert transformer._categorize_temperature(-5) == 'Freezing'
        assert transformer._categorize_temperature(5) == 'Cold'
        assert transformer._categorize_temperature(15) == 'Cool'
        assert transformer._categorize_temperature(22) == 'Mild'
        assert transformer._categorize_temperature(27) == 'Warm'
        assert transformer._categorize_temperature(35) == 'Hot'
    
    def test_categorize_humidity(self):
        """Test humidity categorization"""
        transformer = WeatherDataTransformer()
        
        assert transformer._categorize_humidity(25) == 'Low'
        assert transformer._categorize_humidity(45) == 'Moderate'
        assert transformer._categorize_humidity(75) == 'High'
    
    def test_categorize_wind(self):
        """Test wind speed categorization"""
        transformer = WeatherDataTransformer()
        
        assert transformer._categorize_wind(0.5) == 'Calm'
        assert transformer._categorize_wind(3) == 'Light'
        assert transformer._categorize_wind(8) == 'Gentle'
        assert transformer._categorize_wind(15) == 'Moderate'
        assert transformer._categorize_wind(25) == 'Fresh'
        assert transformer._categorize_wind(35) == 'Strong'
        assert transformer._categorize_wind(45) == 'Gale'
    
    def test_calculate_comfort_index(self):
        """Test comfort index calculation"""
        transformer = WeatherDataTransformer()
        
        temperature = pd.Series([20, 25, 30])
        humidity = pd.Series([50, 80, 30])
        
        comfort = transformer._calculate_comfort_index(temperature, humidity)
        
        assert isinstance(comfort, pd.Series)
        assert len(comfort) == 3
        
        # High humidity should increase perceived temperature
        assert comfort.iloc[1] > temperature.iloc[1]
        
        # Low humidity should decrease perceived temperature
        assert comfort.iloc[2] < temperature.iloc[2]
    
    def test_calculate_quality_score(self):
        """Test quality score calculation"""
        transformer = WeatherDataTransformer()
        
        # Create test DataFrame
        data = {
            'wind_speed': [3.2, None, 1.5],
            'visibility': [10.0, 0.0, 15.0],
            'temperature': [15.0, 50.0, -35.0]  # Normal, very hot, very cold
        }
        df = pd.DataFrame(data)
        
        scores = transformer._calculate_quality_score(df)
        
        assert isinstance(scores, pd.Series)
        assert len(scores) == 3
        assert all(0 <= score <= 100 for score in scores)
        
        # First record should have highest score (normal values)
        assert scores.iloc[0] >= scores.iloc[1]
        assert scores.iloc[0] >= scores.iloc[2]
    
    def test_quality_metrics(self, sample_weather_data):
        """Test quality metrics calculation"""
        transformer = WeatherDataTransformer()
        result = transformer.transform_weather_data(sample_weather_data)
        
        metrics = transformer.get_quality_metrics()
        
        assert isinstance(metrics, dict)
        assert 'total_records_input' in metrics
        assert 'total_records_output' in metrics
        assert 'data_retention_rate' in metrics
        assert 'average_quality_score' in metrics
        assert 'unique_cities' in metrics
        assert 'unique_countries' in metrics
        
        assert metrics['total_records_input'] == 3
        assert metrics['total_records_output'] == len(result)
        assert 0 <= metrics['data_retention_rate'] <= 1
        assert 0 <= metrics['average_quality_score'] <= 100
    
    def test_save_transformed_data(self, tmp_path, sample_weather_dataframe):
        """Test saving transformed data to file"""
        transformer = WeatherDataTransformer()
        
        # Test CSV format
        csv_file = tmp_path / "test_data.csv"
        transformer.save_transformed_data(sample_weather_dataframe, str(csv_file), "csv")
        assert csv_file.exists()
        
        # Test JSON format
        json_file = tmp_path / "test_data.json"
        transformer.save_transformed_data(sample_weather_dataframe, str(json_file), "json")
        assert json_file.exists()
        
        # Test unsupported format
        with pytest.raises(ValueError):
            transformer.save_transformed_data(sample_weather_dataframe, str(tmp_path / "test.xml"), "xml")


@pytest.mark.integration
class TestWeatherTransformationIntegration:
    """Integration tests for weather data transformation"""
    
    def test_full_transformation_pipeline(self, sample_weather_data):
        """Test complete transformation pipeline"""
        transformer = WeatherDataTransformer()
        
        # Run full transformation
        result = transformer.transform_weather_data(sample_weather_data)
        
        # Verify output structure
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        
        # Verify all required columns exist
        required_columns = [
            'city', 'country', 'timestamp', 'temperature', 'humidity',
            'pressure', 'quality_score', 'temp_category', 'season'
        ]
        for col in required_columns:
            assert col in result.columns
        
        # Verify data quality
        metrics = transformer.get_quality_metrics()
        assert metrics['data_retention_rate'] > 0.8  # Most data should survive
        assert metrics['average_quality_score'] > 50  # Reasonable quality
    
    def test_transformation_with_invalid_data(self, invalid_weather_data):
        """Test transformation handles invalid data gracefully"""
        transformer = WeatherDataTransformer()
        
        # Should not crash, but may filter out invalid records
        result = transformer.transform_weather_data(invalid_weather_data)
        
        # Check that quality metrics reflect the issues
        metrics = transformer.get_quality_metrics()
        assert metrics['data_retention_rate'] < 1.0  # Some data should be filtered