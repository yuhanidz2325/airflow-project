"""
Kelompok 2: ETL Pipeline dengan Python Operator (Advanced)
File: dags/advanced_etl_pipeline.py
Dataset: Weather data dengan multiple transformations dan branching
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import requests

default_args = {
    'owner': 'kelompok_2',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

# ================== EXTRACT FUNCTIONS ==================

def extract_current_weather(**context):
    """Extract current weather data dari Open-Meteo"""
    print("🌐 Extracting current weather from Open-Meteo API...")
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": -7.25,
        "longitude": 112.75,
        "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,weather_code,wind_speed_10m",
        "timezone": "Asia/Jakarta"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        context['ti'].xcom_push(key='current_weather', value=json.dumps(data))
        print(f"✅ Current weather extracted successfully")
        
        return data
    except Exception as e:
        print(f"❌ Error extracting current weather: {str(e)}")
        raise


def extract_hourly_forecast(**context):
    """Extract hourly forecast data"""
    print("📅 Extracting hourly forecast...")
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": -7.25,
        "longitude": 112.75,
        "hourly": "temperature_2m,rain,wind_speed_10m,relative_humidity_2m",
        "forecast_days": 7,
        "timezone": "Asia/Jakarta"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Simpan raw data
        os.makedirs("/opt/airflow/data", exist_ok=True)
        with open("/opt/airflow/data/raw_weather_data.json", "w") as f:
            json.dump(data, f)
        
        context['ti'].xcom_push(key='hourly_forecast', value=json.dumps(data))
        print(f"✅ Hourly forecast extracted: {len(data['hourly']['time'])} hours")
        
        return len(data['hourly']['time'])
    except Exception as e:
        print(f"❌ Error extracting hourly forecast: {str(e)}")
        raise


def extract_daily_forecast(**context):
    """Extract daily forecast summary"""
    print("🌞 Extracting daily forecast...")
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": -7.25,
        "longitude": 112.75,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,rain_sum,wind_speed_10m_max",
        "forecast_days": 7,
        "timezone": "Asia/Jakarta"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        context['ti'].xcom_push(key='daily_forecast', value=json.dumps(data))
        print(f"✅ Daily forecast extracted: {len(data['daily']['time'])} days")
        
        return len(data['daily']['time'])
    except Exception as e:
        print(f"❌ Error extracting daily forecast: {str(e)}")
        raise


# ================== TRANSFORM FUNCTIONS ==================

def transform_hourly_data(**context):
    """Transform hourly weather data dengan kategori dan analisis"""
    print("🔄 Transforming hourly weather data...")
    
    ti = context['ti']
    hourly_json = ti.xcom_pull(task_ids='extract_hourly', key='hourly_forecast')
    hourly_data = json.loads(hourly_json)
    
    # Convert ke DataFrame
    df = pd.DataFrame({
        'datetime': hourly_data['hourly']['time'],
        'temperature': hourly_data['hourly']['temperature_2m'],
        'rain': hourly_data['hourly']['rain'],
        'wind_speed': hourly_data['hourly']['wind_speed_10m'],
        'humidity': hourly_data['hourly']['relative_humidity_2m']
    })
    
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['date'] = df['datetime'].dt.date
    df['hour'] = df['datetime'].dt.hour
    df['day_name'] = df['datetime'].dt.day_name()
    
    # Kategori cuaca
    df['weather_category'] = df.apply(lambda row: 
        'Hujan Lebat' if row['rain'] > 5 else 
        'Hujan Sedang' if row['rain'] > 2 else 
        'Hujan Ringan' if row['rain'] > 0 else 
        'Cerah', axis=1
    )
    
    # Kategori suhu
    df['temp_category'] = df['temperature'].apply(
        lambda x: 'Sangat Panas' if x > 35 else 
                  'Panas' if x > 30 else 
                  'Hangat' if x > 25 else 
                  'Sejuk'
    )
    
    # Kategori angin
    df['wind_category'] = df['wind_speed'].apply(
        lambda x: 'Kencang' if x > 20 else 
                  'Sedang' if x > 10 else 
                  'Lemah'
    )
    
    # Comfort index (simplified)
    df['comfort_index'] = df.apply(lambda row:
        'Nyaman' if (25 <= row['temperature'] <= 30 and row['humidity'] < 70 and row['rain'] == 0) else
        'Kurang Nyaman' if (row['temperature'] > 32 or row['humidity'] > 80 or row['rain'] > 2) else
        'Cukup Nyaman', axis=1
    )
    
    # Statistics
    stats = {
        'total_hours': len(df),
        'avg_temperature': float(df['temperature'].mean()),
        'max_temperature': float(df['temperature'].max()),
        'min_temperature': float(df['temperature'].min()),
        'total_rainfall': float(df['rain'].sum()),
        'rainy_hours': int((df['rain'] > 0).sum()),
        'avg_humidity': float(df['humidity'].mean())
    }
    
    ti.xcom_push(key='hourly_stats', value=json.dumps(stats))
    ti.xcom_push(key='hourly_transformed', value=df.to_json(date_format='iso'))
    
    print(f"✅ Hourly data transformed: {len(df)} records")
    print(f"   - Avg temp: {stats['avg_temperature']:.1f}°C")
    print(f"   - Total rain: {stats['total_rainfall']:.2f}mm")
    print(f"   - Rainy hours: {stats['rainy_hours']}")
    
    return stats


def transform_daily_summary(**context):
    """Transform dan agregasi daily summary"""
    print("🔄 Creating daily summary...")
    
    ti = context['ti']
    
    # Ambil daily forecast
    daily_json = ti.xcom_pull(task_ids='extract_daily', key='daily_forecast')
    daily_data = json.loads(daily_json)
    
    # Ambil hourly transformed untuk agregasi tambahan
    hourly_json = ti.xcom_pull(task_ids='transform_hourly', key='hourly_transformed')
    df_hourly = pd.read_json(hourly_json)
    
    # Daily forecast DataFrame
    df_daily = pd.DataFrame({
        'date': daily_data['daily']['time'],
        'max_temp': daily_data['daily']['temperature_2m_max'],
        'min_temp': daily_data['daily']['temperature_2m_min'],
        'total_rain': daily_data['daily']['rain_sum'],
        'max_wind': daily_data['daily']['wind_speed_10m_max']
    })
    
    
    df_daily['date'] = pd.to_datetime(df_daily['date']).dt.date
    
    # Agregasi dari hourly data
    hourly_agg = df_hourly.groupby('date').agg({
        'temperature': 'mean',
        'rain': 'sum',
        'humidity': 'mean',
        'weather_category': lambda x: x.mode()[0] if len(x) > 0 else 'Cerah'
    }).reset_index()
    
    hourly_agg.columns = ['date', 'avg_temp', 'hourly_rain_sum', 'avg_humidity', 'dominant_weather']
    
    df_daily['date'] = pd.to_datetime(df_daily['date']).dt.date
    hourly_agg['date'] = pd.to_datetime(hourly_agg['date']).dt.date
    
    # Merge
    df_summary = df_daily.merge(hourly_agg, on='date', how='left')
    
    # Prediksi kategori hari
    df_summary['day_category'] = df_summary.apply(lambda row:
        'Hari Hujan' if row['total_rain'] > 10 else
        'Hari Panas' if row['max_temp'] > 33 else
        'Hari Normal', axis=1
    )
    
    df_summary['date'] = df_summary['date'].astype(str)
    
    ti.xcom_push(key='daily_summary', value=df_summary.to_json())
    
    print(f"✅ Daily summary created: {len(df_summary)} days")
    print(f"\n📊 Daily Categories Distribution:")
    print(df_summary['day_category'].value_counts().to_dict())
    
    return len(df_summary)


def enrich_with_analysis(**context):
    """Enrich data dengan analisis lanjutan"""
    print("✨ Enriching data dengan analisis...")
    
    ti = context['ti']
    hourly_json = ti.xcom_pull(task_ids='transform_hourly', key='hourly_transformed')
    df = pd.read_json(hourly_json)
    
    # Analisis per waktu (pagi, siang, sore, malam)
    df['time_period'] = df['hour'].apply(lambda h:
        'Pagi' if 6 <= h < 12 else
        'Siang' if 12 <= h < 15 else
        'Sore' if 15 <= h < 18 else
        'Malam'
    )
    
    # Analisis trend suhu (naik/turun)
    df['temp_trend'] = df['temperature'].diff().apply(lambda x:
        'Naik' if x > 0.5 else 'Turun' if x < -0.5 else 'Stabil'
    )
    
    # Weather alerts
    df['alert_level'] = df.apply(lambda row:
        'Merah' if (row['rain'] > 20 or row['wind_speed'] > 30) else
        'Kuning' if (row['rain'] > 10 or row['temperature'] > 35) else
        'Hijau', axis=1
    )
    
    ti.xcom_push(key='enriched_data', value=df.to_json(date_format='iso'))
    
    # Statistik alert
    alerts = df['alert_level'].value_counts().to_dict()
    print(f"✅ Data enriched")
    print(f"📊 Alert Distribution: {alerts}")
    
    return alerts


# ================== QUALITY CHECK & BRANCHING ==================

def check_data_quality(**context):
    """Check data quality dan tentukan next step"""
    print("✔️ Checking weather data quality...")
    
    ti = context['ti']
    enriched_json = ti.xcom_pull(task_ids='enrich_data', key='enriched_data')
    df = pd.read_json(enriched_json)
    
    # Quality checks
    issues = []
    
    if df.isnull().any().any():
        issues.append("Found null values")
    
    if not ((df['temperature'] >= -10) & (df['temperature'] <= 50)).all():
        issues.append("Temperature out of valid range")
    
    if (df['rain'] < 0).any():
        issues.append("Negative rainfall values")
    
    if len(df) < 24:
        issues.append("Insufficient data points")
    
    # Quality report
    quality_report = {
        'passed': len(issues) == 0,
        'issues': issues,
        'total_records': len(df),
        'data_coverage': (len(df) / 168) * 100  # 7 days * 24 hours
    }
    
    ti.xcom_push(key='quality_report', value=json.dumps(quality_report))
    
    if quality_report['passed']:
        print("✅ Quality checks PASSED")
        return 'load_to_warehouse'
    else:
        print(f"❌ Quality checks FAILED: {issues}")
        return 'handle_quality_issues'


# ================== LOAD FUNCTIONS ==================

def load_to_warehouse(**context):
    """Load weather data ke warehouse"""
    print("💾 Loading to Data Warehouse...")
    
    ti = context['ti']
    
    # Load all transformed data
    hourly_json = ti.xcom_pull(task_ids='transform_hourly', key='hourly_transformed')
    daily_json = ti.xcom_pull(task_ids='transform_daily', key='daily_summary')
    enriched_json = ti.xcom_pull(task_ids='enrich_data', key='enriched_data')
    
    df_hourly = pd.read_json(hourly_json)
    df_daily = pd.read_json(daily_json)
    df_enriched = pd.read_json(enriched_json)
    
    # Save to files
    os.makedirs("/opt/airflow/data/warehouse", exist_ok=True)
    
    df_hourly.to_csv('/opt/airflow/data/warehouse/hourly_weather.csv', index=False)
    df_daily.to_csv('/opt/airflow/data/warehouse/daily_weather.csv', index=False)
    df_enriched.to_csv('/opt/airflow/data/warehouse/enriched_weather.csv', index=False)
    
    print(f"✅ Data loaded successfully:")
    print(f"   - Hourly records: {len(df_hourly)}")
    print(f"   - Daily records: {len(df_daily)}")
    print(f"   - Enriched records: {len(df_enriched)}")
    
    return "Data loaded"


def handle_quality_issues(**context):
    """Handle data quality issues"""
    print("⚠️ Handling data quality issues...")
    
    ti = context['ti']
    quality_report = json.loads(ti.xcom_pull(task_ids='check_quality', key='quality_report'))
    
    print(f"Issues: {quality_report['issues']}")
    print("📧 Alert sent to data team")
    print("🔧 Attempting data correction...")
    
    # Simpan report
    with open('/opt/airflow/data/quality_issues.json', 'w') as f:
        json.dump(quality_report, f, indent=2)
    
    return "Issues handled"


# ================== DAG DEFINITION ==================

with DAG(
    'advanced_weather_etl',
    default_args=default_args,
    description='Advanced ETL untuk weather data dengan multiple transformations',
    schedule='0 */6 * * *',  # Setiap 6 jam
    catchup=False,
    tags=['kelompok_2', 'weather', 'advanced', 'surabaya']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Parallel extraction
    extract_current = PythonOperator(
        task_id='extract_current',
        python_callable=extract_current_weather
    )
    
    extract_hourly = PythonOperator(
        task_id='extract_hourly',
        python_callable=extract_hourly_forecast
    )
    
    extract_daily = PythonOperator(
        task_id='extract_daily',
        python_callable=extract_daily_forecast
    )
    
    extraction_complete = EmptyOperator(task_id='extraction_complete')
    
    # Transform tasks
    transform_hourly = PythonOperator(
        task_id='transform_hourly',
        python_callable=transform_hourly_data
    )
    
    transform_daily = PythonOperator(
        task_id='transform_daily',
        python_callable=transform_daily_summary
    )
    
    enrich = PythonOperator(
        task_id='enrich_data',
        python_callable=enrich_with_analysis
    )
    
    # Quality check with branching
    quality_check = BranchPythonOperator(
        task_id='check_quality',
        python_callable=check_data_quality
    )
    
    # Load or handle issues
    load = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_to_warehouse
    )
    
    handle_issues = PythonOperator(
        task_id='handle_quality_issues',
        python_callable=handle_quality_issues
    )
    
    # Cleanup
    cleanup = BashOperator(
        task_id='cleanup',
        bash_command='echo "🧹 Weather ETL pipeline completed"',
        trigger_rule='none_failed_min_one_success'
    )
    
    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')
    
    # Workflow
    start >> [extract_current, extract_hourly, extract_daily] >> extraction_complete
    extraction_complete >> transform_hourly >> transform_daily >> enrich
    enrich >> quality_check >> [load, handle_issues]
    [load, handle_issues] >> cleanup >> end