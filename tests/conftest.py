"""
Test configuration and fixtures for the weather ETL pipeline
"""

import pytest
import pandas as pd
from datetime import datetime
from src.ingest import WeatherDataPoint
from src.load import DatabaseConfig


@pytest.fixture
def sample_weather_data():
    """Fixture providing sample weather data points for testing"""
    return [
        WeatherDataPoint(
            city="London",
            country="GB",
            timestamp=datetime(2023, 11, 7, 12, 0, 0),
            temperature=15.5,
            feels_like=14.2,
            humidity=65,
            pressure=1013,
            description="partly cloudy",
            wind_speed=3.2,
            wind_direction=180,
            cloudiness=40,
            visibility=10.0,
            lat=51.5074,
            lon=-0.1278
        ),
        WeatherDataPoint(
            city="New York",
            country="US",
            timestamp=datetime(2023, 11, 7, 12, 0, 0),
            temperature=18.0,
            feels_like=17.5,
            humidity=70,
            pressure=1015,
            description="clear sky",
            wind_speed=2.8,
            wind_direction=90,
            cloudiness=0,
            visibility=12.0,
            lat=40.7128,
            lon=-74.0060
        ),
        WeatherDataPoint(
            city="Tokyo",
            country="JP",
            timestamp=datetime(2023, 11, 7, 12, 0, 0),
            temperature=22.3,
            feels_like=21.8,
            humidity=55,
            pressure=1020,
            description="sunny",
            wind_speed=1.5,
            wind_direction=45,
            cloudiness=10,
            visibility=15.0,
            lat=35.6762,
            lon=139.6503
        )
    ]


@pytest.fixture
def sample_weather_dataframe():
    """Fixture providing a sample weather DataFrame for testing"""
    data = {
        'city': ['London', 'New York', 'Tokyo'],
        'country': ['GB', 'US', 'JP'],
        'timestamp': [
            datetime(2023, 11, 7, 12, 0, 0),
            datetime(2023, 11, 7, 12, 0, 0),
            datetime(2023, 11, 7, 12, 0, 0)
        ],
        'temperature': [15.5, 18.0, 22.3],
        'feels_like': [14.2, 17.5, 21.8],
        'humidity': [65, 70, 55],
        'pressure': [1013, 1015, 1020],
        'description': ['partly cloudy', 'clear sky', 'sunny'],
        'wind_speed': [3.2, 2.8, 1.5],
        'wind_direction': [180, 90, 45],
        'cloudiness': [40, 0, 10],
        'visibility': [10.0, 12.0, 15.0],
        'lat': [51.5074, 40.7128, 35.6762],
        'lon': [-0.1278, -74.0060, 139.6503]
    }
    return pd.DataFrame(data)


@pytest.fixture
def test_database_config():
    """Fixture providing test database configuration"""
    return DatabaseConfig('sqlite', database_path=':memory:')


@pytest.fixture
def invalid_weather_data():
    """Fixture providing invalid weather data for testing error handling"""
    return [
        WeatherDataPoint(
            city="",  # Empty city name
            country="GB",
            timestamp=datetime(2023, 11, 7, 12, 0, 0),
            temperature=999.0,  # Invalid temperature
            feels_like=14.2,
            humidity=150,  # Invalid humidity (>100)
            pressure=-100,  # Invalid pressure
            description="test",
            wind_speed=-5.0,  # Invalid wind speed
            wind_direction=450,  # Invalid wind direction (>360)
            cloudiness=120,  # Invalid cloudiness (>100)
            visibility=-1.0,  # Invalid visibility
            lat=200.0,  # Invalid latitude (>90)
            lon=-200.0  # Invalid longitude (<-180)
        )
    ]


@pytest.fixture
def mock_api_response():
    """Fixture providing mock API response data"""
    return {
        "coord": {"lon": -0.1278, "lat": 51.5074},
        "weather": [{"id": 801, "main": "Clouds", "description": "few clouds", "icon": "02d"}],
        "main": {
            "temp": 15.5,
            "feels_like": 14.2,
            "temp_min": 13.0,
            "temp_max": 18.0,
            "pressure": 1013,
            "humidity": 65
        },
        "visibility": 10000,
        "wind": {"speed": 3.2, "deg": 180},
        "clouds": {"all": 40},
        "dt": 1699358400,
        "sys": {"type": 1, "id": 1414, "country": "GB", "sunrise": 1699339200, "sunset": 1699372800},
        "timezone": 0,
        "id": 2643743,
        "name": "London",
        "cod": 200
    }