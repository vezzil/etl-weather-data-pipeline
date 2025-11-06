"""
Tests for the weather data ingestion module
"""

import pytest
import requests
from unittest.mock import Mock, patch
from datetime import datetime
from src.ingest import WeatherAPIClient, WeatherDataIngestion, WeatherDataPoint


class TestWeatherAPIClient:
    """Test cases for WeatherAPIClient"""
    
    def test_init(self):
        """Test client initialization"""
        client = WeatherAPIClient("test_api_key")
        assert client.api_key == "test_api_key"
        assert client.base_url == "https://api.openweathermap.org/data/2.5"
        assert client.session is not None
        assert client.logger is not None
    
    def test_init_custom_base_url(self):
        """Test client initialization with custom base URL"""
        custom_url = "https://custom.api.com"
        client = WeatherAPIClient("test_api_key", base_url=custom_url)
        assert client.base_url == custom_url
    
    @patch('requests.Session.get')
    def test_fetch_current_weather_success(self, mock_get, mock_api_response):
        """Test successful weather data fetching"""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_get.return_value = mock_response
        
        # Test the method
        client = WeatherAPIClient("test_api_key")
        result = client.fetch_current_weather("London", "GB")
        
        # Assertions
        assert result is not None
        assert isinstance(result, WeatherDataPoint)
        assert result.city == "London"
        assert result.country == "GB"
        assert result.temperature == 15.5
        assert result.humidity == 65
        
        # Verify API call
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "q=London,GB" in call_args[1]['params']['q']
        assert call_args[1]['params']['appid'] == "test_api_key"
    
    @patch('requests.Session.get')
    def test_fetch_current_weather_without_country(self, mock_get, mock_api_response):
        """Test weather fetching without country code"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_get.return_value = mock_response
        
        client = WeatherAPIClient("test_api_key")
        result = client.fetch_current_weather("London")
        
        assert result is not None
        call_args = mock_get.call_args
        assert call_args[1]['params']['q'] == "London"
    
    @patch('requests.Session.get')
    def test_fetch_current_weather_api_error(self, mock_get):
        """Test handling of API errors"""
        mock_get.side_effect = requests.exceptions.RequestException("API Error")
        
        client = WeatherAPIClient("test_api_key")
        result = client.fetch_current_weather("London", "GB")
        
        assert result is None
    
    @patch('requests.Session.get')
    def test_fetch_current_weather_invalid_response(self, mock_get):
        """Test handling of invalid API response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"invalid": "data"}  # Missing required fields
        mock_get.return_value = mock_response
        
        client = WeatherAPIClient("test_api_key")
        result = client.fetch_current_weather("London", "GB")
        
        assert result is None
    
    @patch('src.ingest.WeatherAPIClient.fetch_current_weather')
    @patch('src.ingest.time.sleep')
    def test_fetch_multiple_cities(self, mock_sleep, mock_fetch):
        """Test fetching weather for multiple cities"""
        # Setup mock return values
        mock_weather_data = [
            WeatherDataPoint(
                city="London", country="GB", timestamp=datetime.now(),
                temperature=15.0, feels_like=14.0, humidity=65, pressure=1013,
                description="cloudy", wind_speed=3.0, wind_direction=180,
                cloudiness=40, visibility=10.0, lat=51.5, lon=-0.1
            ),
            WeatherDataPoint(
                city="Paris", country="FR", timestamp=datetime.now(),
                temperature=18.0, feels_like=17.0, humidity=60, pressure=1015,
                description="sunny", wind_speed=2.0, wind_direction=90,
                cloudiness=20, visibility=12.0, lat=48.9, lon=2.3
            )
        ]
        mock_fetch.side_effect = mock_weather_data
        
        client = WeatherAPIClient("test_api_key")
        cities = [
            {"city": "London", "country_code": "GB"},
            {"city": "Paris", "country_code": "FR"}
        ]
        
        result = client.fetch_multiple_cities(cities)
        
        assert len(result) == 2
        assert result[0].city == "London"
        assert result[1].city == "Paris"
        assert mock_fetch.call_count == 2
        assert mock_sleep.call_count == 2  # Rate limiting
    
    @patch('src.ingest.WeatherAPIClient.fetch_current_weather')
    def test_fetch_multiple_cities_with_failures(self, mock_fetch):
        """Test fetching multiple cities with some failures"""
        # First call succeeds, second fails, third succeeds
        mock_weather_data = WeatherDataPoint(
            city="London", country="GB", timestamp=datetime.now(),
            temperature=15.0, feels_like=14.0, humidity=65, pressure=1013,
            description="cloudy", wind_speed=3.0, wind_direction=180,
            cloudiness=40, visibility=10.0, lat=51.5, lon=-0.1
        )
        mock_fetch.side_effect = [mock_weather_data, None, mock_weather_data]
        
        client = WeatherAPIClient("test_api_key")
        cities = [
            {"city": "London", "country_code": "GB"},
            {"city": "InvalidCity", "country_code": "XX"},
            {"city": "Paris", "country_code": "FR"}
        ]
        
        result = client.fetch_multiple_cities(cities)
        
        assert len(result) == 2  # Only successful fetches
        assert all(data.city in ["London", "Paris"] for data in result)
    
    def test_parse_weather_data(self, mock_api_response):
        """Test parsing of API response data"""
        client = WeatherAPIClient("test_api_key")
        result = client._parse_weather_data(mock_api_response)
        
        assert isinstance(result, WeatherDataPoint)
        assert result.city == "London"
        assert result.country == "GB"
        assert result.temperature == 15.5
        assert result.feels_like == 14.2
        assert result.humidity == 65
        assert result.pressure == 1013
        assert result.description == "few clouds"
        assert result.wind_speed == 3.2
        assert result.wind_direction == 180
        assert result.cloudiness == 40
        assert result.visibility == 10.0  # Converted from meters to km
        assert result.lat == 51.5074
        assert result.lon == -0.1278


class TestWeatherDataIngestion:
    """Test cases for WeatherDataIngestion"""
    
    def test_init(self):
        """Test ingestion initialization"""
        ingestion = WeatherDataIngestion("test_api_key")
        assert ingestion.client.api_key == "test_api_key"
        assert ingestion.logger is not None
    
    @patch('src.ingest.WeatherAPIClient.fetch_multiple_cities')
    def test_ingest_data_default_cities(self, mock_fetch):
        """Test data ingestion with default cities"""
        mock_weather_data = [
            WeatherDataPoint(
                city="London", country="GB", timestamp=datetime.now(),
                temperature=15.0, feels_like=14.0, humidity=65, pressure=1013,
                description="cloudy", wind_speed=3.0, wind_direction=180,
                cloudiness=40, visibility=10.0, lat=51.5, lon=-0.1
            )
        ]
        mock_fetch.return_value = mock_weather_data
        
        ingestion = WeatherDataIngestion("test_api_key")
        result = ingestion.ingest_data()
        
        assert len(result) == 1
        assert result[0].city == "London"
        mock_fetch.assert_called_once()
    
    @patch('src.ingest.WeatherAPIClient.fetch_multiple_cities')
    @patch('builtins.open')
    @patch('json.load')
    @patch('os.path.exists')
    def test_ingest_data_custom_cities(self, mock_exists, mock_json_load, mock_open, mock_fetch):
        """Test data ingestion with custom cities configuration"""
        # Setup mocks
        mock_exists.return_value = True
        mock_json_load.return_value = [{"city": "Paris", "country_code": "FR"}]
        mock_weather_data = [
            WeatherDataPoint(
                city="Paris", country="FR", timestamp=datetime.now(),
                temperature=18.0, feels_like=17.0, humidity=60, pressure=1015,
                description="sunny", wind_speed=2.0, wind_direction=90,
                cloudiness=20, visibility=12.0, lat=48.9, lon=2.3
            )
        ]
        mock_fetch.return_value = mock_weather_data
        
        ingestion = WeatherDataIngestion("test_api_key")
        result = ingestion.ingest_data("custom_cities.json")
        
        assert len(result) == 1
        assert result[0].city == "Paris"
        mock_exists.assert_called_with("custom_cities.json")
        mock_json_load.assert_called_once()
    
    @patch('src.ingest.WeatherAPIClient.fetch_multiple_cities')
    @patch('os.path.exists')
    def test_ingest_data_file_not_found(self, mock_exists, mock_fetch):
        """Test data ingestion when cities file doesn't exist"""
        mock_exists.return_value = False
        mock_weather_data = [
            WeatherDataPoint(
                city="London", country="GB", timestamp=datetime.now(),
                temperature=15.0, feels_like=14.0, humidity=65, pressure=1013,
                description="cloudy", wind_speed=3.0, wind_direction=180,
                cloudiness=40, visibility=10.0, lat=51.5, lon=-0.1
            )
        ]
        mock_fetch.return_value = mock_weather_data
        
        ingestion = WeatherDataIngestion("test_api_key")
        result = ingestion.ingest_data("nonexistent.json")
        
        # Should fall back to default cities
        assert len(result) == 1
        mock_fetch.assert_called_once()
    
    def test_load_cities_config_default(self):
        """Test loading default cities configuration"""
        ingestion = WeatherDataIngestion("test_api_key")
        cities = ingestion._load_cities_config()
        
        assert isinstance(cities, list)
        assert len(cities) > 0
        assert all('city' in city and 'country_code' in city for city in cities)
        
        # Check some expected default cities
        city_names = [city['city'] for city in cities]
        assert "London" in city_names
        assert "New York" in city_names
        assert "Tokyo" in city_names


@pytest.mark.integration
class TestWeatherIngestionIntegration:
    """Integration tests for weather data ingestion"""
    
    def test_real_api_call(self):
        """Test actual API call (requires valid API key)"""
        import os
        api_key = os.getenv('OPENWEATHER_API_KEY')
        
        if not api_key:
            pytest.skip("No API key provided for integration test")
        
        client = WeatherAPIClient(api_key)
        result = client.fetch_current_weather("London", "GB")
        
        if result:  # API might be down or rate limited
            assert isinstance(result, WeatherDataPoint)
            assert result.city == "London"
            assert result.country == "GB"
            assert isinstance(result.temperature, (int, float))
            assert 0 <= result.humidity <= 100