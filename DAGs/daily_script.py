from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import os
import subprocess
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
        # import subprocess
        logger = logging.getLogger('airflow.task')
        script_path = "/Users/sylvesteranthony/Documents/Ride_prediction/data_collector.py"  # Update with correct path
        try:
            process = subprocess.Popen(
                ['python3', script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

        # Stream stdout
            for line in iter(process.stdout.readline, ''):
                logger.info(line.strip())  # Log each line as INFO

        # Stream stderr
            for line in iter(process.stderr.readline, ''):
                logger.error(line.strip())  # Log each line as ERROR

            process.wait()  # Wait for the subprocess to finish
            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, process.args)

        except subprocess.CalledProcessError as e:
            logger.error(f"Script failed with error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    # Run the task
    run_scraping_script()
