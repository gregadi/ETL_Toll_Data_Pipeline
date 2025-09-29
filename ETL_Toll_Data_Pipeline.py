# Python Libary Imports
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# DAG Arguments
default_args = {
    'owner': 'Alexander',
    'start_date': datetime.today(),
    'email': ['alexandergregory@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Task Definitions

# define task unzip_data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging',
    dag=dag,
)

# define task extract_data_from_csv
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="cut -d',' -f1-4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv",
    dag=dag,
)

# define task extract_data_from_tsv
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -f5,6,7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv | tr '\t' ',' > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv",
    dag=dag,
)

# define task extract_data_from_fixed_width
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="cut -c44-47,49-50 /home/project/airflow/dags/finalassignment/staging/payment-data.txt | tr ' ' ',' > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv",
    dag=dag,
)

# define task consolidate_data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="paste -d',' /home/project/airflow/dags/finalassignment/staging/csv_data.csv /home/project/airflow/dags/finalassignment/staging/tsv_data.csv /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv",
    dag=dag,
)

#define task transform_data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="cut -d',' -f1-3,4 --output-delimiter=',' /home/project/airflow/dags/finalassignment/staging/extracted_data.csv | awk -F',' '{OFS=\",\"; $4=toupper($4); print}' > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv",
    dag=dag,
)

# Task Pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data




