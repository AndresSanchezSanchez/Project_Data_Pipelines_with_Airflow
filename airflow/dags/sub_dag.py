from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries
from airflow.operators.dummy_operator import DummyOperator

def create_table(
        parent_dag_name,
        trips_task_id,
        redshift_conn_id,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{trips_task_id}",
        **kwargs
    )

    create_staging_events_table = PostgresOperator(
        task_id=f"create_staging_events_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.staging_events_table_create
    )
    
    create_staging_songs_table = PostgresOperator(
        task_id=f"create_songs_events_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.staging_songs_table_create
    )
    
    create_songplays_table = PostgresOperator(
        task_id=f"create_sonplays_songs_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.songplays_table_create
    )
    
    create_artists_table = PostgresOperator(
        task_id=f"create_artists_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.artists_table_create
    )
    
    create_songs_table = PostgresOperator(
        task_id=f"create_songs_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.songs_table_create
    )
    
    create_time_table = PostgresOperator(
        task_id=f"create_time_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.time_table_create
    )
    
    create_users_table = PostgresOperator(
        task_id=f"create_users_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueries.users_table_create
    )
    
    schema_created = DummyOperator(task_id='Schema_created', 
                               dag=dag)
    
    create_staging_events_table >> schema_created
    create_staging_songs_table >> schema_created
    create_songplays_table >> schema_created
    create_artists_table >> schema_created
    create_songs_table >> schema_created
    create_users_table >> schema_created
    create_time_table >> schema_created

    return dag