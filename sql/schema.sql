-- PostgreSQL Database Schema for Weather ETL Pipeline

-- Create database (run this separately as a superuser)
-- CREATE DATABASE weather_db;

-- Connect to the weather_db database before running the rest

-- Create weather_data table
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

-- Create data quality metrics table
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

-- Create load history table
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

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_weather_city_timestamp ON weather_data(city, timestamp);
CREATE INDEX IF NOT EXISTS idx_weather_country_date ON weather_data(country, date);
CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_weather_location ON weather_data(lat, lon);
CREATE INDEX IF NOT EXISTS idx_weather_quality ON weather_data(quality_score);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_timestamp ON data_quality_metrics(load_timestamp);
CREATE INDEX IF NOT EXISTS idx_load_history_timestamp ON load_history(load_timestamp);

-- Create views for common queries

-- Daily weather summary by city
CREATE OR REPLACE VIEW daily_weather_summary AS
SELECT 
    city,
    country,
    date,
    AVG(temperature) as avg_temperature,
    MIN(temperature) as min_temperature,
    MAX(temperature) as max_temperature,
    AVG(humidity) as avg_humidity,
    AVG(pressure) as avg_pressure,
    AVG(wind_speed) as avg_wind_speed,
    COUNT(*) as observation_count,
    AVG(quality_score) as avg_quality_score
FROM weather_data
GROUP BY city, country, date
ORDER BY date DESC, city;

-- Latest weather data for each city
CREATE OR REPLACE VIEW latest_weather AS
SELECT DISTINCT ON (city, country)
    city,
    country,
    timestamp,
    temperature,
    feels_like,
    humidity,
    pressure,
    description,
    wind_speed,
    temp_category,
    quality_score,
    location
FROM weather_data
ORDER BY city, country, timestamp DESC;

-- Data quality summary
CREATE OR REPLACE VIEW data_quality_summary AS
SELECT 
    DATE(load_timestamp) as load_date,
    AVG(data_retention_rate) as avg_retention_rate,
    AVG(average_quality_score) as avg_quality_score,
    SUM(total_records_output) as total_daily_records,
    COUNT(*) as load_count
FROM data_quality_metrics
GROUP BY DATE(load_timestamp)
ORDER BY load_date DESC;

-- Weather trends by season
CREATE OR REPLACE VIEW seasonal_weather_trends AS
SELECT 
    season,
    temp_category,
    COUNT(*) as observation_count,
    AVG(temperature) as avg_temperature,
    AVG(humidity) as avg_humidity,
    AVG(wind_speed) as avg_wind_speed
FROM weather_data
GROUP BY season, temp_category
ORDER BY season, temp_category;

-- Grant permissions (adjust as needed for your user)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO weather_etl_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO weather_etl_user;