"""
Weather ETL Airflow DAG

This DAG orchestrates the weather data ETL pipeline:
1. Ingest weather data from OpenWeatherMap API
2. Transform and clean the data
3. Load data into the database
4. Generate data quality reports

Schedule: Daily at 6 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import logging
import os
import sys
import pandas as pd

# Add src directory to path for imports
sys.path.append('/opt/airflow/dags/src')

# Import our ETL modules
try:
    from src.ingest import WeatherDataIngestion
    from src.transform import WeatherDataTransformer
    from src.load import WeatherDataLoader, DatabaseConfig
except ImportError as e:
    logging.error(f"Failed to import ETL modules: {e}")
    # Modules will be imported in task functions


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'catchup': False,
}

# DAG definition
dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='Weather Data ETL Pipeline',
    schedule_interval='0 6 * * *',  # Daily at 6 AM UTC
    max_active_runs=1,
    tags=['weather', 'etl', 'data-pipeline'],
)


def get_api_key():
    """Get API key from Airflow Variables or environment"""
    try:
        return Variable.get("OPENWEATHER_API_KEY")
    except:
        return os.getenv('OPENWEATHER_API_KEY')


def get_database_config():
    """Get database configuration from Airflow Variables or environment"""
    try:
        db_type = Variable.get("DB_TYPE", "sqlite")
        
        if db_type.lower() == 'postgresql':
            config = DatabaseConfig(
                'postgresql',
                host=Variable.get("DB_HOST", "localhost"),
                port=int(Variable.get("DB_PORT", "5432")),
                database=Variable.get("DB_NAME", "weather_db"),
                username=Variable.get("DB_USER", "postgres"),
                password=Variable.get("DB_PASSWORD", "")
            )
        else:
            db_path = Variable.get("DB_PATH", "/opt/airflow/data/weather_data.db")
            config = DatabaseConfig('sqlite', database_path=db_path)
        
        return config
    except Exception as e:
        logging.error(f"Failed to get database config: {e}")
        # Default to SQLite
        return DatabaseConfig('sqlite', database_path="/opt/airflow/data/weather_data.db")


def ingest_weather_data(**context):
    """Task function to ingest weather data from API"""
    logging.info("Starting weather data ingestion...")
    
    # Import modules here to avoid import issues
    from src.ingest import WeatherDataIngestion
    
    # Get API key
    api_key = get_api_key()
    if not api_key:
        raise ValueError("OpenWeather API key not found in Variables or environment")
    
    # Initialize ingestion
    ingestion = WeatherDataIngestion(api_key)
    
    # Get cities configuration path from Variables
    try:
        cities_config = Variable.get("CITIES_CONFIG_PATH", None)
    except:
        cities_config = None
    
    # Ingest data
    weather_data = ingestion.ingest_data(cities_config)
    
    if not weather_data:
        raise ValueError("No weather data was ingested")
    
    # Store data temporarily for next task
    temp_file = "/tmp/raw_weather_data.csv"
    
    # Convert to DataFrame and save
    data_dicts = [
        {
            'city': point.city,
            'country': point.country,
            'timestamp': point.timestamp.isoformat(),
            'temperature': point.temperature,
            'feels_like': point.feels_like,
            'humidity': point.humidity,
            'pressure': point.pressure,
            'description': point.description,
            'wind_speed': point.wind_speed,
            'wind_direction': point.wind_direction,
            'cloudiness': point.cloudiness,
            'visibility': point.visibility,
            'lat': point.lat,
            'lon': point.lon
        }
        for point in weather_data
    ]
    
    df = pd.DataFrame(data_dicts)
    df.to_csv(temp_file, index=False)
    
    logging.info(f"Ingested {len(weather_data)} weather records")
    
    # Push metrics to XCom
    context['task_instance'].xcom_push(
        key='ingestion_metrics',
        value={
            'records_ingested': len(weather_data),
            'cities_count': len(set(point.city for point in weather_data)),
            'ingestion_timestamp': datetime.now().isoformat()
        }
    )
    
    return temp_file


def transform_weather_data(**context):
    """Task function to transform and clean weather data"""
    logging.info("Starting weather data transformation...")
    
    # Import modules here
    from src.transform import WeatherDataTransformer
    from src.ingest import WeatherDataPoint
    
    # Get input file from previous task
    temp_file = context['task_instance'].xcom_pull(
        task_ids='ingest_weather_data'
    )
    
    if not temp_file or not os.path.exists(temp_file):
        raise ValueError("Raw weather data file not found")
    
    # Read raw data
    df_raw = pd.read_csv(temp_file)
    
    # Convert back to WeatherDataPoint objects for transformation
    weather_data = []
    for _, row in df_raw.iterrows():
        point = WeatherDataPoint(
            city=row['city'],
            country=row['country'],
            timestamp=datetime.fromisoformat(row['timestamp']),
            temperature=row['temperature'],
            feels_like=row['feels_like'],
            humidity=row['humidity'],
            pressure=row['pressure'],
            description=row['description'],
            wind_speed=row['wind_speed'],
            wind_direction=row['wind_direction'],
            cloudiness=row['cloudiness'],
            visibility=row['visibility'],
            lat=row['lat'],
            lon=row['lon']
        )
        weather_data.append(point)
    
    # Initialize transformer and transform data
    transformer = WeatherDataTransformer()
    transformed_df = transformer.transform_weather_data(weather_data)
    
    if transformed_df.empty:
        raise ValueError("No data survived the transformation process")
    
    # Save transformed data
    output_file = "/tmp/transformed_weather_data.csv"
    transformed_df.to_csv(output_file, index=False)
    
    # Get quality metrics
    quality_metrics = transformer.get_quality_metrics()
    
    logging.info(f"Transformed data: {len(transformed_df)} records, "
                f"quality score: {quality_metrics.get('average_quality_score', 0):.1f}")
    
    # Push metrics to XCom
    context['task_instance'].xcom_push(
        key='transformation_metrics',
        value=quality_metrics
    )
    
    # Clean up temporary file
    if os.path.exists(temp_file):
        os.remove(temp_file)
    
    return output_file


def load_weather_data(**context):
    """Task function to load weather data into database"""
    logging.info("Starting weather data loading...")
    
    # Import modules here
    from src.load import WeatherDataLoader
    
    # Get input file from previous task
    transformed_file = context['task_instance'].xcom_pull(
        task_ids='transform_weather_data'
    )
    
    if not transformed_file or not os.path.exists(transformed_file):
        raise ValueError("Transformed weather data file not found")
    
    # Read transformed data
    df = pd.read_csv(transformed_file)
    
    # Convert timestamp column back to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = pd.to_datetime(df['date']).dt.date
    
    # Initialize loader
    config = get_database_config()
    loader = WeatherDataLoader(config)
    
    # Create schema if needed
    try:
        loader.create_schema()
    except Exception as e:
        logging.warning(f"Schema creation issue (may already exist): {e}")
    
    # Load data using upsert strategy
    load_stats = loader.load_data(df, load_strategy='upsert')
    
    # Load quality metrics
    quality_metrics = context['task_instance'].xcom_pull(
        task_ids='transform_weather_data',
        key='transformation_metrics'
    )
    
    if quality_metrics:
        loader.load_quality_metrics(quality_metrics)
    
    # Get database summary
    summary = loader.get_data_summary()
    
    logging.info(f"Data loading completed: {load_stats}")
    logging.info(f"Database summary: {summary}")
    
    # Push metrics to XCom
    context['task_instance'].xcom_push(
        key='load_metrics',
        value={
            'load_stats': load_stats,
            'database_summary': summary
        }
    )
    
    # Clean up temporary file
    if os.path.exists(transformed_file):
        os.remove(transformed_file)
    
    return load_stats


def generate_data_quality_report(**context):
    """Generate a data quality report"""
    logging.info("Generating data quality report...")
    
    # Get metrics from previous tasks
    ingestion_metrics = context['task_instance'].xcom_pull(
        task_ids='ingest_weather_data',
        key='ingestion_metrics'
    )
    
    transformation_metrics = context['task_instance'].xcom_pull(
        task_ids='transform_weather_data',
        key='transformation_metrics'
    )
    
    load_metrics = context['task_instance'].xcom_pull(
        task_ids='load_weather_data',
        key='load_metrics'
    )
    
    # Generate report
    report = {
        'pipeline_run_date': datetime.now().isoformat(),
        'ingestion': ingestion_metrics,
        'transformation': transformation_metrics,
        'loading': load_metrics,
        'overall_status': 'SUCCESS'
    }
    
    # Save report
    report_file = f"/opt/airflow/data/quality_reports/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    
    import json
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logging.info(f"Quality report saved to {report_file}")
    
    # Push report to XCom for potential email notification
    context['task_instance'].xcom_push(
        key='quality_report',
        value=report
    )
    
    return report_file


def check_data_quality_thresholds(**context):
    """Check if data quality meets minimum thresholds"""
    quality_report = context['task_instance'].xcom_pull(
        task_ids='generate_quality_report',
        key='quality_report'
    )
    
    if not quality_report:
        raise ValueError("Quality report not found")
    
    # Define quality thresholds
    min_retention_rate = 0.8  # 80%
    min_quality_score = 70    # 70/100
    min_records = 5           # At least 5 records
    
    transformation_metrics = quality_report.get('transformation', {})
    load_metrics = quality_report.get('loading', {})
    
    # Check thresholds
    retention_rate = transformation_metrics.get('data_retention_rate', 0)
    quality_score = transformation_metrics.get('average_quality_score', 0)
    records_loaded = load_metrics.get('load_stats', {}).get('records_loaded', 0)
    
    issues = []
    
    if retention_rate < min_retention_rate:
        issues.append(f"Data retention rate {retention_rate:.2%} below threshold {min_retention_rate:.2%}")
    
    if quality_score < min_quality_score:
        issues.append(f"Average quality score {quality_score:.1f} below threshold {min_quality_score}")
    
    if records_loaded < min_records:
        issues.append(f"Records loaded {records_loaded} below threshold {min_records}")
    
    if issues:
        error_msg = "Data quality issues detected:\n" + "\n".join(issues)
        logging.error(error_msg)
        raise ValueError(error_msg)
    
    logging.info("All data quality thresholds met")
    return True


# Task definitions
with dag:
    
    # Task 1: Ingest weather data
    ingest_task = PythonOperator(
        task_id='ingest_weather_data',
        python_callable=ingest_weather_data,
        doc_md="""
        ### Ingest Weather Data
        
        This task fetches current weather data from the OpenWeatherMap API
        for a configured list of cities.
        
        **Outputs:**
        - Raw weather data saved to temporary CSV file
        - Ingestion metrics pushed to XCom
        """
    )
    
    # Task 2: Transform weather data
    transform_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data,
        doc_md="""
        ### Transform Weather Data
        
        This task cleans, validates, and enriches the raw weather data:
        - Removes duplicates and outliers
        - Adds derived fields (season, temperature category, etc.)
        - Calculates data quality scores
        
        **Outputs:**
        - Transformed weather data saved to temporary CSV file
        - Quality metrics pushed to XCom
        """
    )
    
    # Task 3: Load weather data
    load_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data,
        doc_md="""
        ### Load Weather Data
        
        This task loads the transformed weather data into the database:
        - Creates database schema if needed
        - Uses upsert strategy to handle duplicates
        - Stores quality metrics
        
        **Outputs:**
        - Load statistics and database summary pushed to XCom
        """
    )
    
    # Task Group: Data Quality Monitoring
    with TaskGroup('data_quality_monitoring') as quality_group:
        
        # Generate quality report
        quality_report_task = PythonOperator(
            task_id='generate_quality_report',
            python_callable=generate_data_quality_report,
        )
        
        # Check quality thresholds
        quality_check_task = PythonOperator(
            task_id='check_quality_thresholds',
            python_callable=check_data_quality_thresholds,
        )
        
        quality_report_task >> quality_check_task
    
    # Optional: Cleanup task
    cleanup_task = BashOperator(
        task_id='cleanup_temp_files',
        bash_command='find /tmp -name "*weather_data*.csv" -type f -delete || true',
        doc_md="Clean up any remaining temporary files"
    )
    
    # Optional: Success notification
    success_notification = BashOperator(
        task_id='success_notification',
        bash_command='echo "Weather ETL pipeline completed successfully at $(date)"',
        trigger_rule='all_success'
    )

# Task dependencies
ingest_task >> transform_task >> load_task >> quality_group
quality_group >> cleanup_task >> success_notification

# Add failure notification if email is configured
try:
    email_list = Variable.get("FAILURE_EMAIL_LIST", "").split(',')
    if email_list and email_list[0]:
        failure_notification = EmailOperator(
            task_id='failure_notification',
            to=email_list,
            subject='Weather ETL Pipeline Failed',
            html_content="""
            <h3>Weather ETL Pipeline Failure</h3>
            <p>The weather data ETL pipeline has failed.</p>
            <p>Please check the Airflow logs for more details.</p>
            <p>Run Date: {{ ds }}</p>
            <p>Run ID: {{ run_id }}</p>
            """,
            trigger_rule='one_failed',
            dag=dag
        )
        
        # Connect failure notification to all main tasks
        [ingest_task, transform_task, load_task] >> failure_notification

except Exception as e:
    logging.warning(f"Email notification setup failed: {e}")


# Add documentation
dag.doc_md = """
# Weather ETL Pipeline

This DAG implements a complete ETL pipeline for weather data:

## Pipeline Steps
1. **Ingest**: Fetch current weather data from OpenWeatherMap API
2. **Transform**: Clean, validate, and enrich the data
3. **Load**: Store data in database with upsert strategy
4. **Monitor**: Generate quality reports and check thresholds

## Configuration

### Required Airflow Variables
- `OPENWEATHER_API_KEY`: OpenWeatherMap API key
- `DB_TYPE`: Database type ('postgresql' or 'sqlite')

### Optional Airflow Variables
- `CITIES_CONFIG_PATH`: Path to cities configuration JSON file
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`: PostgreSQL config
- `DB_PATH`: SQLite database file path
- `FAILURE_EMAIL_LIST`: Comma-separated email addresses for failure notifications

## Data Quality Monitoring
- Minimum data retention rate: 80%
- Minimum quality score: 70/100
- Minimum records loaded: 5

## Schedule
Runs daily at 6:00 AM UTC
"""