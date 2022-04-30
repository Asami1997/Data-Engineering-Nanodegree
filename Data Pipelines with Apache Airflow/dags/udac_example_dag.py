from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
     'owner': 'udacity',
     'start_date': datetime(2019, 1, 12), 
     'retries' : 3,
     'retry_delay' : timedelta(minutes = 5),
     'email_on_retry': False,
     'catchup' : False # turn off backfill
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id= "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key  = "log_data",
    table = "events",
    execution_date="{{execution_date}}",
    file_format = 'JSON',
    provide_context = True   
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id= "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key  = "song_data",
    table = "songs",
    execution_date="{{execution_date}}",
    file_format = 'JSON',
    provide_context = True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query =  SqlQueries.songplay_table_insert,
    provide_context = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'user',
    sql_query = SqlQueries.user_table_insert,
    provide_context = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'song',
    sql_query =  SqlQueries.song_table_insert,
    provide_context = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'artist',
    sql_query = SqlQueries.artist_table_insert,
    provide_context = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'time',
    sql_query = SqlQueries.time_table_insert,
    provide_context = True
)

#
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag ,
    redshift_conn_id = 'redshift',
    tables = ['user','song','artist','time'],
    provide_context = True
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Define task dependencies 
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
load_songplays_table << stage_events_to_redshift
load_songplays_table << stage_songs_to_redshift
load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,
                         load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator