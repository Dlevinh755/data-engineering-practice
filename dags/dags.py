from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import os
import sys
import os
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
default_args = {
    'owner': 'airflow',
    'retries': 1,
}
dag = DAG(
    dag_id='DATA_ENGINEER_EXERCISE',
    description='A simple DAG',
    default_args=default_args,
    schedule_interval= None,
    start_date=datetime(2025, 1, 1),
    catchup=False
)


Bai2 = BashOperator(
    task_id='Ex2_scraping_data',
    bash_command ='python /var/tmp/app/Exercise_2/main.py',
    dag=dag
)

Bai4 = BashOperator(
    task_id='Ex_4_Json_to_csv',
    bash_command ='python /var/tmp/app/Exercise_4/main.py',
    dag=dag
)
Bai5 = BashOperator(
    task_id='Ex5_import_data',
    bash_command ='python /var/tmp/app/Exercise_5/main.py',
    dag=dag
)
Bai3 = BashOperator(
    task_id='Ex3_transform_data',
    bash_command ='python /var/tmp/app/Exercise_3/main.py',
    dag=dag
)

Bai1 = BashOperator(
    task_id='Ex1_collection_data',
    bash_command='python /var/tmp/app/Exercise_1/main.py',
    dag=dag
        )

Bai7 = BashOperator(
    task_id='Ex7_pyspark_job',
    bash_command='python /var/tmp/app/Exercise_7/main.py',
    dag=dag
)





Bai2
Bai4
Bai5
Bai3
Bai1
Bai7