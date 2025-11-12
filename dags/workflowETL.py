"""
Kelompok 1: Intro DAG dan Desain Workflow ETL
File: dags/intro_etl_workflow.py
Dataset: Weather data dari Open-Meteo API (Surabaya)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import requests

# Default arguments untuk DAG
default_args = {
    'owner': 'kelompok_1',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Fungsi Extract - Mengambil data cuaca dari Open-Meteo API
def extract_weather_data(ti, **kwargs):
    """Extract data cuaca dari Open-Meteo API untuk Surabaya"""
    print("🔍 Memulai proses Extract data cuaca...")
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": -7.25,
        "longitude": 112.75,
        "hourly": "temperature_2m,rain",
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
        
        # Push ke XCom untuk task berikutnya
        ti.xcom_push(key='raw_weather_data', value=json.dumps(data))
        
        # Hitung statistik dasar
        total_hours = len(data['hourly']['time'])
        print(f"✅ Data cuaca berhasil diambil: {total_hours} jam data")
        print(f"📍 Lokasi: Surabaya (Lat: {params['latitude']}, Lon: {params['longitude']})")
        print(f"📅 Timezone: {params['timezone']}")
        
        return "Extract completed"
        
    except Exception as e:
        print(f"❌ Error saat extract data: {str(e)}")
        raise


# Fungsi Transform - Membersihkan dan transformasi data cuaca
def transform_weather_data(ti, **kwargs):
    """Transform dan cleaning data cuaca"""
    print("🔄 Memulai proses Transform data cuaca...")
    
    # Pull data dari XCom
    raw_data_json = ti.xcom_pull(task_ids='extract_task', key='raw_weather_data')
    raw_data = json.loads(raw_data_json)
    
    # Convert ke DataFrame
    df = pd.DataFrame({
        'datetime': raw_data['hourly']['time'],
        'temperature': raw_data['hourly']['temperature_2m'],
        'rain': raw_data['hourly']['rain']
    })
    
    # Parse datetime
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['date'] = df['datetime'].dt.date
    df['hour'] = df['datetime'].dt.hour
    
    # Transform: Agregasi harian
    daily_summary = df.groupby('date').agg({
        'temperature': ['mean', 'min', 'max'],
        'rain': 'sum'
    }).reset_index()
    
    daily_summary.columns = ['date', 'avg_temp', 'min_temp', 'max_temp', 'total_rain']
    daily_summary['date'] = daily_summary['date'].astype(str)
    
    # Transform: Kategorisasi cuaca
    df['weather_condition'] = df['rain'].apply(
        lambda x: 'Hujan Lebat' if x > 5 else 'Hujan Ringan' if x > 0 else 'Cerah'
    )
    
    df['temp_category'] = df['temperature'].apply(
        lambda x: 'Panas' if x > 30 else 'Hangat' if x > 25 else 'Sejuk'
    )
    
    # Push hasil transformasi
    ti.xcom_push(key='daily_summary', value=daily_summary.to_json())
    ti.xcom_push(key='hourly_processed', value=df.to_json(date_format='iso'))
    
    print(f"✅ Data transformed:")
    print(f"   - Total {len(df)} jam data")
    print(f"   - Ringkasan {len(daily_summary)} hari")
    print(f"   - Suhu rata-rata: {df['temperature'].mean():.2f}°C")
    print(f"   - Total hujan: {df['rain'].sum():.2f}mm")
    
    return "Transform completed"


# Fungsi Load - Menyimpan data ke destination
def load_weather_data(ti, **kwargs):
    """Load data cuaca ke data warehouse"""
    print("💾 Memulai proses Load data cuaca...")
    
    # Load daily summary
    daily_json = ti.xcom_pull(task_ids='transform_task', key='daily_summary')
    df_daily = pd.read_json(daily_json)
    
    # Load hourly processed
    hourly_json = ti.xcom_pull(task_ids='transform_task', key='hourly_processed')
    df_hourly = pd.read_json(hourly_json)
    
    # Simpan ke file CSV (simulasi load ke database)
    daily_path = '/opt/airflow/data/daily_weather_summary.csv'
    hourly_path = '/opt/airflow/data/hourly_weather_processed.csv'
    
    df_daily.to_csv(daily_path, index=False)
    df_hourly.to_csv(hourly_path, index=False)
    
    print("\n📊 Daily Weather Summary (Top 5 hari):")
    print(df_daily.head().to_string(index=False))
    
    print(f"\n✅ Data loaded successfully:")
    print(f"   - Daily summary: {daily_path}")
    print(f"   - Hourly data: {hourly_path}")
    
    return "Load completed"


# Fungsi validasi data quality
def validate_weather_quality(ti, **kwargs):
    """Validasi kualitas data cuaca"""
    print("✔️ Memulai Data Quality Check untuk data cuaca...")
    
    hourly_json = ti.xcom_pull(task_ids='transform_task', key='hourly_processed')
    df = pd.read_json(hourly_json)
    
    # Validasi checks
    checks = {
        'no_nulls': df.isnull().sum().sum() == 0,
        'valid_temperature_range': ((df['temperature'] >= -50) & (df['temperature'] <= 60)).all(),
        'valid_rain_values': (df['rain'] >= 0).all(),
        'sufficient_data': len(df) > 0
    }
    
    print("\n🔍 Data Quality Results:")
    for check, result in checks.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {check}: {status}")
    
    # Tambahan statistik
    print(f"\n📊 Data Statistics:")
    print(f"  - Total records: {len(df)}")
    print(f"  - Temperature range: {df['temperature'].min():.1f}°C - {df['temperature'].max():.1f}°C")
    print(f"  - Total rainfall: {df['rain'].sum():.2f}mm")
    print(f"  - Rainy hours: {(df['rain'] > 0).sum()} dari {len(df)} jam")
    
    if all(checks.values()):
        print("\n✅ All data quality checks passed!")
        return "Validation passed"
    else:
        raise ValueError("Data quality checks failed!")


# Definisi DAG
with DAG(
    'intro_etl_workflow',
    default_args=default_args,
    description='DAG ETL untuk data cuaca Surabaya dari Open-Meteo API',
    schedule='@daily',  # Jalankan setiap hari
    catchup=False,
    tags=['kelompok_1', 'etl', 'weather', 'surabaya']
) as dag:
    
    # Start task
    start = EmptyOperator(task_id='start')
    
    # Extract task - Ambil data dari API
    extract = PythonOperator(
        task_id='extract_task',
        python_callable=extract_weather_data,
    )
    
    # Transform task - Proses dan agregasi data
    transform = PythonOperator(
        task_id='transform_task',
        python_callable=transform_weather_data,
    )
    
    # Data quality validation
    validate = PythonOperator(
        task_id='validate_task',
        python_callable=validate_weather_quality,
    )
    
    # Load task - Simpan ke storage
    load = PythonOperator(
        task_id='load_task',
        python_callable=load_weather_data,
    )
    
    # Cleanup task
    cleanup = BashOperator(
        task_id='cleanup_task',
        bash_command='echo "🧹 Cleanup completed - Weather ETL pipeline finished"'
    )
    
    # End task
    end = EmptyOperator(task_id='end')
    
    # Definisi alur workflow
    start >> extract >> transform >> validate >> load >> cleanup >> end