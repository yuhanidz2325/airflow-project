"""
Kelompok 3: Fixed Version - Simple & Working
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import requests
import traceback

# ===================== DAG 1: MASTER =====================

default_args_master = {
    'owner': 'kelompok_3',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def collect_master_weather(**context):
    print("📥 Collecting master weather data...")
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": -7.25,
        "longitude": 112.75,
        "hourly": "temperature_2m,rain,wind_speed_10m,relative_humidity_2m",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "Asia/Jakarta"
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    os.makedirs("/opt/airflow/data/master", exist_ok=True)
    with open("/opt/airflow/data/master/raw_weather.json", "w") as f:
        json.dump(data, f)
    
    metadata = {
        'timestamp': datetime.now().isoformat(),
        'hourly_records': len(data['hourly']['time']),
        'daily_records': len(data['daily']['time']),
    }
    
    context['ti'].xcom_push(key='master_metadata', value=json.dumps(metadata))
    print(f"✅ Collected {metadata['hourly_records']} hourly records")
    return metadata

def validate_master_weather(**context):
    print("✔️ Validating...")
    metadata = json.loads(context['ti'].xcom_pull(task_ids='collect_master', key='master_metadata'))
    
    if metadata['hourly_records'] > 0:
        print("✅ Validation PASSED")
        return 'PASSED'
    else:
        raise ValueError("Validation failed")

with DAG('master_weather_collection', default_args=default_args_master, schedule='0 */3 * * *', catchup=False, tags=['kelompok_3', 'master']) as dag_master:
    start = EmptyOperator(task_id='start')
    collect = PythonOperator(task_id='collect_master', python_callable=collect_master_weather)
    validate = PythonOperator(task_id='validate_master', python_callable=validate_master_weather)
    
    trigger_hourly = TriggerDagRunOperator(
        task_id='trigger_hourly_analysis',
        trigger_dag_id='hourly_weather_analysis',
        wait_for_completion=False,
    )
    
    trigger_daily = TriggerDagRunOperator(
        task_id='trigger_daily_summary',
        trigger_dag_id='daily_weather_summary',
        wait_for_completion=False,
    )
    
    end = EmptyOperator(task_id='end')
    start >> collect >> validate >> [trigger_hourly, trigger_daily] >> end


# ===================== DAG 2: HOURLY =====================

default_args_hourly = {'owner': 'kelompok_3', 'start_date': datetime(2024, 1, 1), 'retries': 1}

def analyze_hourly_weather(**context):
    try:
        print("📊 Starting hourly analysis...")
        
        # Load data
        print("Loading data from /opt/airflow/data/master/raw_weather.json")
        with open("/opt/airflow/data/master/raw_weather.json", "r") as f:
            data = json.load(f)
        print(f"✅ Data loaded: {len(data['hourly']['time'])} hourly records")
        
        # Create DataFrame
        print("Creating DataFrame...")
        df = pd.DataFrame({
            'datetime': data['hourly']['time'],
            'temperature': data['hourly']['temperature_2m'],
            'rain': data['hourly']['rain'],
            'wind_speed': data['hourly']['wind_speed_10m'],
            'humidity': data['hourly']['relative_humidity_2m']
        })
        print(f"✅ DataFrame created: {len(df)} rows")
        
        # Process
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['hour'] = df['datetime'].dt.hour
        
        hourly_pattern = df.groupby('hour').mean(numeric_only=True).reset_index()
        print(f"✅ Aggregated data: {len(hourly_pattern)} hour groups")
        
        # Calculate analysis
        analysis = {
            'total_hours': len(df),
            'avg_temperature': float(df['temperature'].mean()),
            'total_rainfall': float(df['rain'].sum()),
        }
        
        context['ti'].xcom_push(key='hourly_analysis', value=json.dumps(analysis))
        
        # CRITICAL: Create folder and save CSV
        print("Creating analysis folder...")
        os.makedirs("/opt/airflow/data/analysis", exist_ok=True)
        
        csv_path = '/opt/airflow/data/analysis/hourly_pattern.csv'
        print(f"Saving to {csv_path}...")
        hourly_pattern.to_csv(csv_path, index=False)
        
        # Verify file was created
        if os.path.exists(csv_path):
            file_size = os.path.getsize(csv_path)
            print(f"✅✅✅ CSV FILE CREATED SUCCESSFULLY!")
            print(f"   Path: {csv_path}")
            print(f"   Size: {file_size} bytes")
            print(f"   Rows: {len(hourly_pattern)}")
        else:
            print(f"❌ ERROR: File not created at {csv_path}")
        
        print(f"✅ Analysis completed: {analysis}")
        return analysis
        
    except Exception as e:
        print(f"❌❌❌ ERROR in analyze_hourly_weather:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        traceback.print_exc()
        raise

def generate_hourly_report(**context):
    print("📋 Generating report...")
    analysis = json.loads(context['ti'].xcom_pull(task_ids='analyze_hourly', key='hourly_analysis'))
    report = f"Total Hours: {analysis['total_hours']}, Avg Temp: {analysis['avg_temperature']:.1f}°C"
    print(report)
    return "Report generated"

with DAG('hourly_weather_analysis', default_args=default_args_hourly, schedule=None, catchup=False, tags=['kelompok_3', 'hourly']) as dag_hourly:
    start = EmptyOperator(task_id='start')
    analyze = PythonOperator(task_id='analyze_hourly', python_callable=analyze_hourly_weather)
    report = PythonOperator(task_id='generate_report', python_callable=generate_hourly_report)
    
    trigger_alert = TriggerDagRunOperator(
        task_id='trigger_alert_check',
        trigger_dag_id='weather_alert_system',
        wait_for_completion=False,
    )
    
    end = EmptyOperator(task_id='end')
    start >> analyze >> report >> trigger_alert >> end


# ===================== DAG 3: DAILY =====================

default_args_daily = {'owner': 'kelompok_3', 'start_date': datetime(2024, 1, 1), 'retries': 1}

def create_daily_summary(**context):
    try:
        print("📅 Starting daily summary...")
        
        with open("/opt/airflow/data/master/raw_weather.json", "r") as f:
            data = json.load(f)
        print(f"✅ Data loaded: {len(data['daily']['time'])} daily records")
        
        df_daily = pd.DataFrame({
            'date': data['daily']['time'],
            'max_temp': data['daily']['temperature_2m_max'],
            'min_temp': data['daily']['temperature_2m_min'],
            'total_rain': data['daily']['precipitation_sum']
        })
        
        summary = {
            'total_days': len(df_daily),
            'avg_max_temp': float(df_daily['max_temp'].mean()),
            'rainy_days': int((df_daily['total_rain'] > 0).sum()),
        }
        
        context['ti'].xcom_push(key='daily_summary', value=json.dumps(summary))
        
        # CRITICAL: Create folder and save
        print("Creating analysis folder...")
        os.makedirs("/opt/airflow/data/analysis", exist_ok=True)
        
        csv_path = '/opt/airflow/data/analysis/daily_summary.csv'
        print(f"Saving to {csv_path}...")
        df_daily.to_csv(csv_path, index=False)
        
        if os.path.exists(csv_path):
            print(f"✅✅✅ CSV FILE CREATED!")
            print(f"   Path: {csv_path}")
            print(f"   Size: {os.path.getsize(csv_path)} bytes")
        
        print(f"✅ Summary: {summary}")
        return summary
        
    except Exception as e:
        print(f"❌❌❌ ERROR in create_daily_summary:")
        print(f"Error: {str(e)}")
        traceback.print_exc()
        raise

def generate_weekly_forecast(**context):
    print("🔮 Generating forecast...")
    summary = json.loads(context['ti'].xcom_pull(task_ids='create_summary', key='daily_summary'))
    forecast = f"Period: {summary['total_days']} days, Rainy Days: {summary['rainy_days']}"
    print(forecast)
    return "Forecast generated"

with DAG('daily_weather_summary', default_args=default_args_daily, schedule=None, catchup=False, tags=['kelompok_3', 'daily']) as dag_daily:
    start = EmptyOperator(task_id='start')
    summary = PythonOperator(task_id='create_summary', python_callable=create_daily_summary)
    forecast = PythonOperator(task_id='generate_forecast', python_callable=generate_weekly_forecast)
    end = EmptyOperator(task_id='end')
    start >> summary >> forecast >> end


# ===================== DAG 4: ALERTS =====================

default_args_alert = {'owner': 'kelompok_3', 'start_date': datetime(2024, 1, 1), 'retries': 1}

def check_weather_alerts(**context):
    print("🚨 Checking alerts...")
    
    with open("/opt/airflow/data/master/raw_weather.json", "r") as f:
        data = json.load(f)
    
    df = pd.DataFrame({
        'temperature': data['hourly']['temperature_2m'],
        'rain': data['hourly']['rain'],
        'wind_speed': data['hourly']['wind_speed_10m']
    })
    
    alerts = []
    if (df['rain'] > 10).any():
        alerts.append({'level': 'HIGH', 'type': 'HEAVY_RAIN'})
    if (df['temperature'] > 35).any():
        alerts.append({'level': 'MEDIUM', 'type': 'HIGH_TEMP'})
    
    print(f"✅ Found {len(alerts)} alerts")
    return {'total_alerts': len(alerts), 'alerts': alerts}

def send_alert_notifications(**context):
    print("📧 Sending notifications...")
    return "Sent"

with DAG('weather_alert_system', default_args=default_args_alert, schedule=None, catchup=False, tags=['kelompok_3', 'alerts']) as dag_alert:
    start = EmptyOperator(task_id='start')
    check = PythonOperator(task_id='check_alerts', python_callable=check_weather_alerts)
    send = PythonOperator(task_id='send_notifications', python_callable=send_alert_notifications)
    complete = BashOperator(task_id='complete', bash_command='echo "✅ Pipeline completed!"')
    end = EmptyOperator(task_id='end')
    start >> check >> send >> complete >> end
