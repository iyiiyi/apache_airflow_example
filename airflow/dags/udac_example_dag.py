from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
    PostgresOperator
)
from helpers import SqlQueries
from airflow.contrib.hooks.aws_hook import AwsHook

# Default task setting
default_args = {
    'owner': 'isyi',
    'depends_on_past': False,
    'start_date':datetime(2018, 11, 1),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# Create a dag
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval= '@monthly',
          catchup=False,
          max_active_runs=1
        )

# Start task
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Create stage_events table task
create_stage_events_table = PostgresOperator(
    task_id="create_stage_events_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.CREATE_STAGING_EVENTS_TABLE_SQL
)

# Create stage_songs table task
create_stage_songs_table = PostgresOperator(
    task_id="create_stage_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.CREATE_STAGING_SONGS_TABLE_SQL
)

# Create songplays table task
create_songplays_table = PostgresOperator(
    task_id="create_songplays_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.CREATE_SONGPLAYS_TABLE_SQL
)

# Create artists table task
create_artists_table = PostgresOperator(
    task_id="create_artists_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.CREATE_ARTISTS_TABLE_SQL
)

# Create songs table task
create_songs_table = PostgresOperator(
    task_id="create_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.CREATE_SONGS_TABLE_SQL
)

# Create time table task
create_time_table = PostgresOperator(
    task_id="create_time_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.CREATE_TIME_TABLE_SQL
)

# Create users table task
create_users_table = PostgresOperator(
    task_id="create_users_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.CREATE_USERS_TABLE_SQL
)

# Stage events data from S3 to redshift task
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    redshift_table="staging_events",
    s3_bucket="udacity-dend",
    #s3_key="log_data/{execution_date.year}/{execution_date.month}/{execution_date.year}-{execution_date.month}-{execution_date.day:02d}-events.json",
    s3_key="log_data/{{ execution_date.year }}/{{ execution_date.month }}/",
    jsonpath="s3://udacity-dend/log_json_path.json",
    provide_context=True
)

# Stage songs data from S3 to redshift task
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    redshift_table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    jsonpath="auto"
)

# Load songplays data in redshift
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    fact_table="songplays",
    load_query=SqlQueries.songplay_table_insert
)

# Load user data in redshift
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table="users",
    load_query=SqlQueries.user_table_insert,
    truncate_yn=True
)

# Load song data in redshift
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table="songs",
    load_query=SqlQueries.song_table_insert,
    truncate_yn=True
)

# Load artist data in redshift
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table="artists",
    load_query=SqlQueries.artist_table_insert,
    truncate_yn=True
)

# Load time data in redshift
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table="time",
    load_query=SqlQueries.time_table_insert,
    truncate_yn=True
)

# Check data quality
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

# End task
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define task dependencies
start_operator >> create_stage_events_table
start_operator >> create_stage_songs_table
start_operator >> create_songplays_table
start_operator >> create_artists_table
start_operator >> create_songs_table
start_operator >> create_time_table
start_operator >> create_users_table

create_stage_events_table >> stage_events_to_redshift
create_stage_songs_table >> stage_songs_to_redshift

create_songplays_table >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

create_songs_table >> load_song_dimension_table
load_songplays_table >> load_song_dimension_table

create_users_table >> load_user_dimension_table
load_songplays_table >> load_user_dimension_table

create_artists_table >> load_artist_dimension_table
load_songplays_table >> load_artist_dimension_table

create_time_table >> load_time_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator