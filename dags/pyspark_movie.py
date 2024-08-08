from datetime import datetime, timedelta
from textwrap import dedent
import subprocess
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        PythonOperator,
        BranchPythonOperator,
        PythonVirtualenvOperator
        )

os.environ['LC_ALL'] = 'C'

with DAG(
     'pyspark_movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='hello world DAG',
    schedule="10 2 * * *",
    start_date=datetime(2020, 1, 1),
    catchup=True,
    tags=['pyspark', 'spark'],
) as dag:




    def tmp():
    
        print("tmp")


    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')


    re_partition = PythonVirtualenvOperator(
            task_id='re.partition',
            python_callable=tmp,
            requirements=["git+https://github.com/hamsunwoo/movie.git@0.3/api"],
            system_site_packages=False
             )   


    join_df = BashOperator(
            task_id='join.df',
            bash_command="""
                echo "join"
            """
            )   

    agg_df = BashOperator(
            task_id='agg.df',
            bash_command="""
                echo "agg"
            """
            )   


    start >> re_partition >> join_df >> agg_df >> end



