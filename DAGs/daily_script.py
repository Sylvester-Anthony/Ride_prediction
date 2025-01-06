from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['sylvester.anthony.a@gmail.com'],  # Replace with your email
    'retries': 1,
}

# Define the DAG
with DAG(
    'daily_scraping_with_taskflow',
    default_args=default_args,
    description='Run a scraping script daily with TaskFlow API',
    schedule_interval='@daily',  # Adjust the schedule as needed
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to execute the Python script using @task.bash
    @task.bash
    def run_scraping_script():
        return """
        python /Users/sylvesteranthony/Documents/Ride_prediction/data_collector.py
        """

    # Run the task
    run_scraping_script()