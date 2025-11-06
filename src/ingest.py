"""
Weather Data Ingestion Module

This module handles fetching weather data from the OpenWeatherMap API.
It includes error handling, rate limiting, and data validation.
"""

import requests
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional
import os
from dataclasses import dataclass
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class WeatherDataPoint:
    """Data class for weather data point"""
    city: str
    country: str
    timestamp: datetime
    temperature: float
    feels_like: float
    humidity: int
    pressure: int
    description: str
    wind_speed: float
    wind_direction: int
    cloudiness: int
    visibility: int
    lat: float
    lon: float


class WeatherAPIClient:
    """Client for fetching weather data from OpenWeatherMap API"""
    
    def __init__(self, api_key: str, base_url: str = "https://api.openweathermap.org/data/2.5"):
        self.api_key = api_key
        self.base_url = base_url
        self.session = self._create_session()
        self.logger = self._setup_logger()
        
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
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def fetch_current_weather(self, city: str, country_code: Optional[str] = None) -> Optional[WeatherDataPoint]:
        """
        Fetch current weather data for a specific city
        
        Args:
            city: City name
            country_code: Optional ISO 3166 country code
            
        Returns:
            WeatherDataPoint object or None if failed
        """
        try:
            location = f"{city},{country_code}" if country_code else city
            
            params = {
                'q': location,
                'appid': self.api_key,
                'units': 'metric'  # Use Celsius
            }
            
            self.logger.info(f"Fetching weather data for {location}")
            
            response = self.session.get(
                f"{self.base_url}/weather",
                params=params,
                timeout=10
            )
            
            response.raise_for_status()
            data = response.json()
            
            return self._parse_weather_data(data)
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed for {city}: {e}")
            return None
        except KeyError as e:
            self.logger.error(f"Missing expected field in API response: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching weather for {city}: {e}")
            return None
    
    def fetch_multiple_cities(self, cities: List[Dict[str, str]]) -> List[WeatherDataPoint]:
        """
        Fetch weather data for multiple cities
        
        Args:
            cities: List of dictionaries with 'city' and optional 'country_code' keys
            
        Returns:
            List of WeatherDataPoint objects
        """
        weather_data = []
        
        for city_info in cities:
            city = city_info.get('city')
            country_code = city_info.get('country_code')
            
            if not city:
                self.logger.warning(f"Skipping invalid city info: {city_info}")
                continue
            
            data_point = self.fetch_current_weather(city, country_code)
            if data_point:
                weather_data.append(data_point)
            
            # Rate limiting - OpenWeatherMap free tier allows 60 calls/minute
            time.sleep(1)
        
        self.logger.info(f"Successfully fetched weather data for {len(weather_data)} cities")
        return weather_data
    
    def _parse_weather_data(self, data: Dict) -> WeatherDataPoint:
        """Parse API response into WeatherDataPoint object"""
        return WeatherDataPoint(
            city=data['name'],
            country=data['sys']['country'],
            timestamp=datetime.fromtimestamp(data['dt']),
            temperature=data['main']['temp'],
            feels_like=data['main']['feels_like'],
            humidity=data['main']['humidity'],
            pressure=data['main']['pressure'],
            description=data['weather'][0]['description'],
            wind_speed=data.get('wind', {}).get('speed', 0),
            wind_direction=data.get('wind', {}).get('deg', 0),
            cloudiness=data['clouds']['all'],
            visibility=data.get('visibility', 0) / 1000,  # Convert to km
            lat=data['coord']['lat'],
            lon=data['coord']['lon']
        )


class WeatherDataIngestion:
    """Main class for orchestrating weather data ingestion"""
    
    def __init__(self, api_key: str):
        self.client = WeatherAPIClient(api_key)
        self.logger = logging.getLogger(__name__)
    
    def ingest_data(self, cities_config: str = None) -> List[WeatherDataPoint]:
        """
        Main method to ingest weather data
        
        Args:
            cities_config: Path to JSON file with cities list or None for default cities
            
        Returns:
            List of WeatherDataPoint objects
        """
        cities = self._load_cities_config(cities_config)
        return self.client.fetch_multiple_cities(cities)
    
    def _load_cities_config(self, config_path: Optional[str] = None) -> List[Dict[str, str]]:
        """Load cities configuration from file or use defaults"""
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"Failed to load cities config: {e}. Using defaults.")
        
        # Default cities for demonstration
        return [
            {"city": "London", "country_code": "GB"},
            {"city": "New York", "country_code": "US"},
            {"city": "Tokyo", "country_code": "JP"},
            {"city": "Sydney", "country_code": "AU"},
            {"city": "Berlin", "country_code": "DE"},
            {"city": "Paris", "country_code": "FR"},
            {"city": "Mumbai", "country_code": "IN"},
            {"city": "São Paulo", "country_code": "BR"},
            {"city": "Cairo", "country_code": "EG"},
            {"city": "Toronto", "country_code": "CA"}
        ]


def main():
    """Main function for running the ingestion as a standalone script"""
    import sys
    
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
    
    # Initialize ingestion
    ingestion = WeatherDataIngestion(api_key)
    
    # Get cities config path from command line args
    cities_config = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Ingest data
    weather_data = ingestion.ingest_data(cities_config)
    
    print(f"Ingested weather data for {len(weather_data)} cities")
    for data in weather_data[:3]:  # Show first 3 for demonstration
        print(f"  {data.city}, {data.country}: {data.temperature}°C, {data.description}")


if __name__ == "__main__":
    main()