# Weather Data ETL Pipeline 🌤️

A comprehensive ETL (Extract, Transform, Load) pipeline for weather data that ingests real-time weather information from the OpenWeatherMap API, transforms and cleans the data, and stores it in a database for analytics. This project demonstrates modern data engineering practices with Python, Docker, and Apache Airflow.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌──────────────────┐
│                 │    │                  │    │                 │    │                  │
│  OpenWeatherMap │───▶│   Ingestion      │───▶│  Transformation │───▶│     Loading      │
│      API        │    │  (ingest.py)     │    │ (transform.py)  │    │   (load.py)      │
│                 │    │                  │    │                 │    │                  │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └──────────────────┘
                                                                                  │
                       ┌──────────────────┐    ┌─────────────────┐              │
                       │                  │    │                 │              │
                       │  Apache Airflow  │───▶│   PostgreSQL    │◀─────────────┘
                       │  Orchestration   │    │   Database      │
                       │                  │    │                 │
                       └──────────────────┘    └─────────────────┘
```

## 📊 Features

### Core ETL Pipeline
- **Extract**: Fetches weather data from OpenWeatherMap API for multiple cities
- **Transform**: Cleans, validates, and enriches data with derived metrics
- **Load**: Stores data in PostgreSQL/SQLite with upsert capabilities

### Data Quality & Monitoring
- Comprehensive data validation and quality scoring
- Outlier detection and data cleansing
- Quality metrics tracking and reporting
- Load history and performance monitoring

### Orchestration & Scheduling
- Apache Airflow DAG for automated scheduling
- Task dependency management
- Error handling and retry logic
- Email notifications for failures

### Development & Operations
- Docker containerization for easy deployment
- Multiple database support (PostgreSQL/SQLite)
- Comprehensive testing framework
- CI/CD ready configuration

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- OpenWeatherMap API key ([Get one free here](https://openweathermap.org/api))

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/etl-weather-data-pipeline.git
cd etl-weather-data-pipeline
```

### 2. Set Up Environment
```bash
# Copy environment template
cp .env.template .env

# Edit .env file and add your OpenWeatherMap API key
# OPENWEATHER_API_KEY=your_api_key_here
```

### 3. Start the Stack
```bash
# Build and start all services
docker-compose up -d

# Check service status
docker-compose ps
```

### 4. Access the Applications
- **Airflow Web UI**: http://localhost:8080 (admin/admin123)
- **Jupyter Lab**: http://localhost:8888
- **PgAdmin**: http://localhost:5050 (admin@example.com/admin123)

### 5. Run Your First ETL Job
```bash
# Trigger the DAG manually in Airflow UI, or run directly:
docker-compose exec etl-dev python src/load.py
```

## 📁 Project Structure

```
etl-weather-data-pipeline/
├── src/                          # ETL source code
│   ├── ingest.py                 # Data extraction from API
│   ├── transform.py              # Data transformation and cleaning
│   └── load.py                   # Data loading to database
├── airflow_dag/                  # Airflow DAG definitions
│   └── weather_etl_dag.py        # Main ETL pipeline DAG
├── config/                       # Configuration files
│   └── cities.json               # Cities to fetch weather data
├── sql/                          # Database schemas and queries
│   └── schema.sql                # PostgreSQL database schema
├── tests/                        # Test suite
│   ├── test_ingest.py           # Ingestion tests
│   ├── test_transform.py        # Transformation tests
│   └── test_load.py             # Loading tests
├── notebooks/                    # Jupyter notebooks for analysis
├── docs/                         # Additional documentation
├── docker-compose.yml            # Multi-container Docker setup
├── Dockerfile.airflow           # Airflow container setup
├── Dockerfile.etl              # ETL development environment
├── Dockerfile.jupyter          # Jupyter analytics environment
├── requirements.txt             # Python dependencies
├── .env.template               # Environment variables template
└── README.md                   # This file
```

## 🔧 Configuration

### Environment Variables
Key environment variables in `.env`:

```bash
# API Configuration
OPENWEATHER_API_KEY=your_openweather_api_key

# Database Configuration
DB_TYPE=postgresql  # or sqlite
DB_HOST=localhost
DB_PORT=5432
DB_NAME=weather_db
DB_USER=postgres
DB_PASSWORD=your_password

# Data Quality Thresholds
MIN_RETENTION_RATE=0.8
MIN_QUALITY_SCORE=70
MIN_RECORDS=5
```

### Cities Configuration
Edit `config/cities.json` to customize which cities to monitor:

```json
[
  {"city": "London", "country_code": "GB"},
  {"city": "New York", "country_code": "US"},
  {"city": "Tokyo", "country_code": "JP"}
]
```

## 📈 Data Schema

### Main Weather Data Table
```sql
weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    country VARCHAR(10),
    timestamp TIMESTAMP,
    temperature REAL,
    feels_like REAL,
    humidity INTEGER,
    pressure INTEGER,
    description VARCHAR(100),
    wind_speed REAL,
    wind_direction INTEGER,
    cloudiness INTEGER,
    visibility REAL,
    lat REAL,
    lon REAL,
    -- Derived fields
    temp_category VARCHAR(20),
    humidity_category VARCHAR(20),
    wind_category VARCHAR(20),
    comfort_index REAL,
    quality_score REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

## 🎯 Usage Examples

### Running ETL Components Individually

#### 1. Data Ingestion
```bash
docker-compose exec etl-dev python src/ingest.py config/cities.json
```

#### 2. Data Transformation
```bash
docker-compose exec etl-dev python src/transform.py config/cities.json output.csv
```

#### 3. Data Loading
```bash
docker-compose exec etl-dev python src/load.py config/cities.json upsert
```

### Running Complete Pipeline
```bash
# Using the main load script (runs full ETL)
docker-compose exec etl-dev python src/load.py

# Via Airflow (automated scheduling)
# Access Airflow UI at http://localhost:8080
# Enable and trigger the 'weather_etl_pipeline' DAG
```

### Querying the Data

#### Connect to PostgreSQL
```bash
docker-compose exec postgres psql -U postgres -d weather_db
```

#### Example Queries
```sql
-- Latest weather for all cities
SELECT * FROM latest_weather;

-- Daily temperature trends
SELECT city, date, avg_temperature 
FROM daily_weather_summary 
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY date DESC, city;

-- Data quality overview
SELECT * FROM data_quality_summary 
ORDER BY load_date DESC LIMIT 10;
```

## 🧪 Testing

### Run Test Suite
```bash
# Run all tests
docker-compose exec etl-dev python -m pytest tests/ -v

# Run with coverage
docker-compose exec etl-dev python -m pytest tests/ --cov=src --cov-report=html

# Run specific test module
docker-compose exec etl-dev python -m pytest tests/test_transform.py -v
```

### Test Categories
- **Unit Tests**: Individual component functionality
- **Integration Tests**: End-to-end pipeline testing
- **Data Quality Tests**: Validation of transformation logic

## 📊 Monitoring & Quality

### Data Quality Metrics
The pipeline tracks:
- **Data Retention Rate**: Percentage of records that pass validation
- **Quality Score**: Weighted score based on data completeness and validity
- **Processing Time**: ETL performance metrics
- **Error Rates**: Failed operations tracking

### Airflow Monitoring
- **Task Success/Failure Rates**: Monitor pipeline reliability
- **Execution Time Trends**: Track performance over time
- **Data Volume Trends**: Monitor ingestion patterns

### Database Views
Pre-built views for common analytics:
- `latest_weather`: Most recent data per city
- `daily_weather_summary`: Aggregated daily statistics
- `data_quality_summary`: Quality metrics over time
- `seasonal_weather_trends`: Weather patterns by season

## 🔄 Development Workflow

### Adding New Data Sources
1. Extend `WeatherAPIClient` in `src/ingest.py`
2. Update transformation logic in `src/transform.py`
3. Modify database schema if needed
4. Add corresponding tests

### Customizing Transformations
1. Edit transformation functions in `WeatherDataTransformer`
2. Update quality scoring logic
3. Add new derived fields as needed
4. Test with sample data

### Scaling Considerations
- **Horizontal Scaling**: Use Celery workers with Redis
- **Database Optimization**: Add indexes for query patterns
- **API Rate Limiting**: Implement exponential backoff
- **Data Partitioning**: Consider time-based partitioning

## 🚨 Troubleshooting

### Common Issues

#### API Rate Limits
```bash
# Check API key and rate limits
curl "https://api.openweathermap.org/data/2.5/weather?q=London&appid=YOUR_API_KEY"
```

#### Database Connection Issues
```bash
# Check PostgreSQL connectivity
docker-compose exec etl-dev python -c "
from src.load import DatabaseConfig, WeatherDataLoader
config = DatabaseConfig('postgresql', host='postgres', database='weather_db', username='postgres', password='postgres')
loader = WeatherDataLoader(config)
print('Connection successful!')
"
```

#### Airflow Issues
```bash
# Check Airflow logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler

# Reset Airflow database
docker-compose exec airflow-webserver airflow db reset
```

### Debugging Tips
1. Enable debug logging: Set `LOG_LEVEL=DEBUG` in `.env`
2. Check data quality reports in `/opt/airflow/data/quality_reports/`
3. Monitor database logs for constraint violations
4. Use Jupyter notebooks for data exploration

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Make changes and add tests
4. Run the test suite: `pytest`
5. Commit changes: `git commit -am 'Add new feature'`
6. Push to branch: `git push origin feature/new-feature`
7. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [OpenWeatherMap](https://openweathermap.org/) for providing the weather API
- [Apache Airflow](https://airflow.apache.org/) for workflow orchestration
- [PostgreSQL](https://www.postgresql.org/) for robust data storage
- [Docker](https://www.docker.com/) for containerization

## 🔮 Roadmap

- [ ] Real-time streaming with Apache Kafka
- [ ] Machine learning weather prediction models
- [ ] Interactive dashboard with Streamlit/Grafana
- [ ] Cloud deployment templates (AWS/GCP/Azure)
- [ ] Historical weather data backfill
- [ ] Weather alerts and notifications
- [ ] Multi-language support for cities
- [ ] Advanced geospatial analytics

---

**Made with ❤️ for the Data Engineering Community**

*This project demonstrates production-ready ETL pipeline patterns and best practices for data engineering interviews and real-world applications.*