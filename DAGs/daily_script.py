from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import os
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['sylvester.anthony.a@gmail.com'],
    'retries': 1,
}

# Define the DAG
with DAG(
    'daily_scraping_with_taskflow',
    default_args=default_args,
    description='Run a scraping script daily with TaskFlow API',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to execute the Python script using @task
    @task
    def run_scraping_script():
        import subprocess
        script_path = "/Users/sylvesteranthony/Documents/Ride_prediction/data_collector.py"  # Update with correct path
        try:
            result = subprocess.run(
                ['python3', script_path],
                check=True,
                capture_output=True,
                text=True
            )
            logging.info(result.stdout)
        except subprocess.CalledProcessError as e:
            logging.info(e.stderr)
            raise

    # Run the task
    run_scraping_script()
