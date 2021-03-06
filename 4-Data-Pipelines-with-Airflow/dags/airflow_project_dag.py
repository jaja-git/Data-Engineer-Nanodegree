from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.load_fact import LoadFactOperator

from plugins.helpers.sql_queries import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('airflow_project_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11",
    table="staging_events",
    json_format="s3://udacity-dend/log_json_path.json",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A",
    table="staging_songs",
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    sql='insert into {} {}'.format('songplays', SqlQueries.songplay_table_insert)
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    truncate_first=True,
    sql='insert into {} {}'.format('users', SqlQueries.user_table_insert)
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    truncate_first=True,
    sql='insert into {} {}'.format('songs', SqlQueries.song_table_insert)
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    truncate_first=True,
    sql='insert into {} {}'.format('artists', SqlQueries.artist_table_insert)
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    truncate_first=True,
    sql='insert into {} {}'.format('time', SqlQueries.time_table_insert)
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
