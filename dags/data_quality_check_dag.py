from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    DataQualityOperator
)
from helpers import SqlQueries
from airflow.contrib.hooks.aws_hook import AwsHook

# AWS_KEY =  os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'isyi',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('data_quality_check_dag',
          start_date=datetime.utcnow(),
          default_args=default_args,
          description='Check data quality in Redshift',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    query_list=[
        'SELECT COUNT(*) FROM artists',
        'SELECT COUNT(*) FROM songs',
        'SELECT COUNT(*) FROM users',
        'SELECT COUNT(*) FROM time',
        'SELECT COUNT(*) FROM songplays',
        'SELECT COUNT(*) FROM songplays WHERE start_time IS NULL'
    ],
    pass_result_list=[
        DataQualityOperator.NON_ZERO,
        DataQualityOperator.NON_ZERO,
        DataQualityOperator.NON_ZERO,
        DataQualityOperator.NON_ZERO,
        DataQualityOperator.NON_ZERO,
        DataQualityOperator.ZERO
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> run_quality_checks
run_quality_checks >> end_operator
