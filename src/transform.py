"""
Weather Data Transformation Module

This module handles cleaning, normalizing, and validating weather data.
It uses pandas for efficient data manipulation and includes data quality checks.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import json
from dataclasses import asdict
from src.ingest import WeatherDataPoint


class WeatherDataTransformer:
    """Handles transformation and cleaning of weather data"""
    
    def __init__(self):
        self.logger = self._setup_logger()
        self.quality_metrics = {}
    
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
    
    def transform_weather_data(self, weather_data: List[WeatherDataPoint]) -> pd.DataFrame:
        """
        Main transformation method that applies all cleaning and enrichment steps
        
        Args:
            weather_data: List of WeatherDataPoint objects
            
        Returns:
            Cleaned and transformed pandas DataFrame
        """
        if not weather_data:
            self.logger.warning("No weather data provided for transformation")
            return pd.DataFrame()
        
        self.logger.info(f"Starting transformation of {len(weather_data)} weather records")
        
        # Convert to DataFrame
        df = self._convert_to_dataframe(weather_data)
        
        # Apply transformations
        df = self._clean_data(df)
        df = self._normalize_data(df)
        df = self._enrich_data(df)
        df = self._validate_data(df)
        
        # Calculate quality metrics
        self._calculate_quality_metrics(df, len(weather_data))
        
        self.logger.info(f"Transformation completed. Final dataset has {len(df)} records")
        return df
    
    def _convert_to_dataframe(self, weather_data: List[WeatherDataPoint]) -> pd.DataFrame:
        """Convert list of WeatherDataPoint objects to pandas DataFrame"""
        data_dicts = [asdict(point) for point in weather_data]
        df = pd.DataFrame(data_dicts)
        
        # Ensure timestamp is datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the data"""
        self.logger.info("Cleaning weather data...")
        
        # Remove duplicates based on city, country, and timestamp (within 1 hour)
        df_clean = df.copy()
        df_clean['timestamp_hour'] = df_clean['timestamp'].dt.floor('H')
        df_clean = df_clean.drop_duplicates(subset=['city', 'country', 'timestamp_hour'])
        df_clean = df_clean.drop(columns=['timestamp_hour'])
        
        if len(df_clean) < len(df):
            self.logger.info(f"Removed {len(df) - len(df_clean)} duplicate records")
        
        # Handle missing values
        df_clean = self._handle_missing_values(df_clean)
        
        # Clean text fields
        df_clean['city'] = df_clean['city'].str.strip().str.title()
        df_clean['country'] = df_clean['country'].str.upper()
        df_clean['description'] = df_clean['description'].str.lower().str.strip()
        
        return df_clean
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing or invalid values in the dataset"""
        
        # Fill missing wind data with 0 (calm conditions)
        df['wind_speed'] = df['wind_speed'].fillna(0)
        df['wind_direction'] = df['wind_direction'].fillna(0)
        
        # Fill missing visibility with median value
        df['visibility'] = df['visibility'].fillna(df['visibility'].median())
        
        # Remove records with missing critical fields
        critical_fields = ['temperature', 'humidity', 'pressure', 'lat', 'lon']
        before_count = len(df)
        df = df.dropna(subset=critical_fields)
        after_count = len(df)
        
        if before_count > after_count:
            self.logger.warning(f"Removed {before_count - after_count} records with missing critical data")
        
        return df
    
    def _normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize and standardize data values"""
        self.logger.info("Normalizing weather data...")
        
        df_norm = df.copy()
        
        # Ensure humidity is between 0-100
        df_norm['humidity'] = df_norm['humidity'].clip(0, 100)
        
        # Ensure cloudiness is between 0-100
        df_norm['cloudiness'] = df_norm['cloudiness'].clip(0, 100)
        
        # Ensure wind direction is between 0-360
        df_norm['wind_direction'] = df_norm['wind_direction'] % 360
        
        # Convert negative visibility to 0
        df_norm['visibility'] = df_norm['visibility'].clip(lower=0)
        
        # Round numeric values to appropriate precision
        df_norm['temperature'] = df_norm['temperature'].round(1)
        df_norm['feels_like'] = df_norm['feels_like'].round(1)
        df_norm['wind_speed'] = df_norm['wind_speed'].round(1)
        df_norm['visibility'] = df_norm['visibility'].round(1)
        df_norm['lat'] = df_norm['lat'].round(6)
        df_norm['lon'] = df_norm['lon'].round(6)
        
        return df_norm
    
    def _enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived fields and enrichments"""
        self.logger.info("Enriching weather data with derived fields...")
        
        df_enriched = df.copy()
        
        # Add derived temporal fields
        df_enriched['date'] = df_enriched['timestamp'].dt.date
        df_enriched['hour'] = df_enriched['timestamp'].dt.hour
        df_enriched['day_of_week'] = df_enriched['timestamp'].dt.day_name()
        df_enriched['month'] = df_enriched['timestamp'].dt.month_name()
        df_enriched['season'] = df_enriched['timestamp'].dt.month.map(self._get_season)
        
        # Add weather categorizations
        df_enriched['temp_category'] = df_enriched['temperature'].apply(self._categorize_temperature)
        df_enriched['humidity_category'] = df_enriched['humidity'].apply(self._categorize_humidity)
        df_enriched['wind_category'] = df_enriched['wind_speed'].apply(self._categorize_wind)
        
        # Add comfort index (heat index approximation)
        df_enriched['comfort_index'] = self._calculate_comfort_index(
            df_enriched['temperature'], 
            df_enriched['humidity']
        )
        
        # Add location metadata
        df_enriched['location'] = df_enriched['city'] + ', ' + df_enriched['country']
        df_enriched['coord_string'] = df_enriched['lat'].astype(str) + ',' + df_enriched['lon'].astype(str)
        
        # Add data quality score
        df_enriched['quality_score'] = self._calculate_quality_score(df_enriched)
        
        return df_enriched
    
    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data quality and remove outliers"""
        self.logger.info("Validating weather data quality...")
        
        df_valid = df.copy()
        initial_count = len(df_valid)
        
        # Remove extreme outliers
        
        # Temperature outliers (outside -60°C to 60°C)
        temp_mask = (df_valid['temperature'] >= -60) & (df_valid['temperature'] <= 60)
        df_valid = df_valid[temp_mask]
        
        # Pressure outliers (outside 800-1100 hPa)
        pressure_mask = (df_valid['pressure'] >= 800) & (df_valid['pressure'] <= 1100)
        df_valid = df_valid[pressure_mask]
        
        # Wind speed outliers (above 200 km/h is extremely rare)
        wind_mask = df_valid['wind_speed'] <= 200
        df_valid = df_valid[wind_mask]
        
        # Coordinate validation
        lat_mask = (df_valid['lat'] >= -90) & (df_valid['lat'] <= 90)
        lon_mask = (df_valid['lon'] >= -180) & (df_valid['lon'] <= 180)
        df_valid = df_valid[lat_mask & lon_mask]
        
        removed_count = initial_count - len(df_valid)
        if removed_count > 0:
            self.logger.warning(f"Removed {removed_count} records due to invalid data")
        
        return df_valid
    
    def _get_season(self, month: int) -> str:
        """Determine season based on month (Northern Hemisphere)"""
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Autumn'
    
    def _categorize_temperature(self, temp: float) -> str:
        """Categorize temperature into ranges"""
        if temp < 0:
            return 'Freezing'
        elif temp < 10:
            return 'Cold'
        elif temp < 20:
            return 'Cool'
        elif temp < 25:
            return 'Mild'
        elif temp < 30:
            return 'Warm'
        else:
            return 'Hot'
    
    def _categorize_humidity(self, humidity: int) -> str:
        """Categorize humidity levels"""
        if humidity < 30:
            return 'Low'
        elif humidity < 60:
            return 'Moderate'
        else:
            return 'High'
    
    def _categorize_wind(self, wind_speed: float) -> str:
        """Categorize wind speed using Beaufort scale approximation"""
        if wind_speed < 1:
            return 'Calm'
        elif wind_speed < 6:
            return 'Light'
        elif wind_speed < 12:
            return 'Gentle'
        elif wind_speed < 20:
            return 'Moderate'
        elif wind_speed < 29:
            return 'Fresh'
        elif wind_speed < 39:
            return 'Strong'
        else:
            return 'Gale'
    
    def _calculate_comfort_index(self, temperature: pd.Series, humidity: pd.Series) -> pd.Series:
        """Calculate a simple comfort index based on temperature and humidity"""
        # Simplified heat index calculation
        comfort = temperature.copy()
        
        # Adjust for humidity effects on perceived temperature
        high_humidity_mask = humidity > 70
        comfort.loc[high_humidity_mask] += (humidity.loc[high_humidity_mask] - 70) * 0.1
        
        low_humidity_mask = humidity < 30
        comfort.loc[low_humidity_mask] -= (30 - humidity.loc[low_humidity_mask]) * 0.05
        
        return comfort.round(1)
    
    def _calculate_quality_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate a data quality score for each record"""
        scores = pd.Series(100, index=df.index)  # Start with perfect score
        
        # Deduct points for missing optional fields
        if 'wind_speed' in df.columns:
            scores -= df['wind_speed'].isna() * 5
        if 'visibility' in df.columns:
            scores -= (df['visibility'] == 0) * 3
        
        # Deduct points for extreme values (might be valid but less reliable)
        scores -= (df['temperature'] > 45) * 5  # Very hot
        scores -= (df['temperature'] < -30) * 5  # Very cold
        scores -= (df['wind_speed'] > 100) * 10  # Very high wind
        
        return scores.clip(0, 100)
    
    def _calculate_quality_metrics(self, df: pd.DataFrame, original_count: int):
        """Calculate and store data quality metrics"""
        self.quality_metrics = {
            'total_records_input': original_count,
            'total_records_output': len(df),
            'data_retention_rate': len(df) / original_count if original_count > 0 else 0,
            'average_quality_score': df['quality_score'].mean() if 'quality_score' in df.columns else 0,
            'missing_values_percentage': (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
            'unique_cities': df['city'].nunique() if 'city' in df.columns else 0,
            'unique_countries': df['country'].nunique() if 'country' in df.columns else 0,
            'timestamp_range': {
                'min': df['timestamp'].min().isoformat() if len(df) > 0 else None,
                'max': df['timestamp'].max().isoformat() if len(df) > 0 else None
            }
        }
    
    def get_quality_metrics(self) -> Dict:
        """Return the calculated quality metrics"""
        return self.quality_metrics
    
    def save_transformed_data(self, df: pd.DataFrame, output_path: str, format: str = 'csv'):
        """Save transformed data to file"""
        try:
            if format.lower() == 'csv':
                df.to_csv(output_path, index=False)
            elif format.lower() == 'json':
                df.to_json(output_path, orient='records', date_format='iso')
            elif format.lower() == 'parquet':
                df.to_parquet(output_path, index=False)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            self.logger.info(f"Transformed data saved to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save transformed data: {e}")
            raise


def main():
    """Main function for running transformation as a standalone script"""
    import sys
    import os
    from src.ingest import WeatherDataIngestion
    
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
    
    # Initialize components
    ingestion = WeatherDataIngestion(api_key)
    transformer = WeatherDataTransformer()
    
    # Get cities config path from command line args
    cities_config = sys.argv[1] if len(sys.argv) > 1 else None
    output_path = sys.argv[2] if len(sys.argv) > 2 else 'transformed_weather_data.csv'
    
    # Ingest and transform data
    print("Ingesting weather data...")
    weather_data = ingestion.ingest_data(cities_config)
    
    print("Transforming weather data...")
    transformed_df = transformer.transform_weather_data(weather_data)
    
    # Save transformed data
    transformer.save_transformed_data(transformed_df, output_path)
    
    # Print quality metrics
    metrics = transformer.get_quality_metrics()
    print(f"\nTransformation Quality Metrics:")
    print(f"  Input records: {metrics['total_records_input']}")
    print(f"  Output records: {metrics['total_records_output']}")
    print(f"  Retention rate: {metrics['data_retention_rate']:.2%}")
    print(f"  Average quality score: {metrics['average_quality_score']:.1f}/100")
    print(f"  Unique cities: {metrics['unique_cities']}")


if __name__ == "__main__":
    main()