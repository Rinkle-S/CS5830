from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import os
import random

# Base URL and year
BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
# Placeholder for year (to be dynamically replaced in tasks)
YEAR = "YYYY"

def fetch_page():
    """
    Downloads the webpage listing NOAA climate data files for the specified year.
    """
    command = f"wget {BASE_URL}{YEAR}"
    os.system(command)

def select_random_files(num_files):
    """
    Selects a random set of data files from the downloaded webpage.
    """
    files = os.listdir(YEAR)
    selected_files = random.sample(files, num_files)
    return selected_files

def fetch_individual_files(file):
    """
    Task to fetch individual data files
    """
    command = f"wget {BASE_URL}{YEAR}/{file}"
    os.system(command)

def zip_files(files):
    """
    Creates a ZIP archive containing the selected NOAA climate data files.
    """
    command = f"zip {YEAR}_archive.zip {' '.join(files)}"
    os.system(command)

def move_archive():
    """
    Moves the created ZIP archive to a predefined location.
    """
    command = f"mv {YEAR}_archive.zip /path/to/required/location/"
    os.system(command)

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define DAG
dag = DAG(
    'ncei_noaa_data_pipeline',
    default_args=default_args,
    description='Fetch and process NOAA data using a simple DAG',
    schedule_interval=None,  # Manual execution for now
)

# Define tasks in the DAG
fetch_page_task = BashOperator(
    task_id='fetch_page',
    bash_command=fetch_page,
    dag=dag,
)

select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=select_random_files,
    op_kwargs={'num_files': 4},  # Adjust number as required (manual)
    dag=dag,
)

fetch_files_tasks = []
for i in range(4):  # The number of selected files
    task = BashOperator(
        task_id=f'fetch_file_{i}',
        bash_command=fetch_individual_files,
        env={'file': "{{ task_instance.xcom_pull(task_ids='select_files')[%d] }}" % i},
        dag=dag,
    )
    fetch_files_tasks.append(task)

zip_files_task = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    op_kwargs={'files': "{{ task_instance.xcom_pull(task_ids='select_files') }}"},
    dag=dag,
)

move_archive_task = BashOperator(
    task_id='move_archive',
    bash_command=move_archive,
    dag=dag,
)

# Define task dependencies
fetch_page_task >> select_files_task
select_files_task >> fetch_files_tasks
fetch_files_tasks >> zip_files_task
zip_files_task >> move_archive_task