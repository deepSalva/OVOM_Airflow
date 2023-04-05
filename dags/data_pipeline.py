from datetime import datetime, timedelta
import pendulum

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from sql_files import create_tables_sql, stage_redshift_sql

DEFAULT_ARGS = {
    'owner': 'salvig',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

CREATE_TABLE_QUERIES = [
    create_tables_sql.PATIENT,
    create_tables_sql.DOCTOR,
    create_tables_sql.STAGING_LOGS,
    create_tables_sql.STAGING_PATIENT,
    create_tables_sql.LAB,
    create_tables_sql.BLOOD_TEST,
]

LOAD_REDSHIFT_QUERIES = stage_redshift_sql.SqlQueries

REDSHIFT_CONN_ID = 'redshift'

AWS_CREDENTIALS = 'aws_credentials'


@dag(
    default_args=DEFAULT_ARGS,
    description='Load and transform data in Redshift with Airflow',
    start_date=pendulum.now(),
    schedule_interval='0 * * * *'
)
def dwh_redshift_load():
    @task
    def create_tables():
        redshift_hook = PostgresHook(REDSHIFT_CONN_ID)
        for table_query in CREATE_TABLE_QUERIES:
            redshift_hook.run(table_query)

    @task
    def stage_redshift(**kwargs):
        table = kwargs['params']['table']
        bucket = kwargs['params']['bucket']
        json_format = kwargs['params']['json_format']
        metastore_backend = MetastoreBackend()
        aws_connection = metastore_backend.get_connection(AWS_CREDENTIALS)
        redshift_hook = PostgresHook(REDSHIFT_CONN_ID)
        sql_stmt = LOAD_REDSHIFT_QUERIES.COPY_SQL.format(
            table,
            bucket,
            aws_connection.login,
            aws_connection.password,
            json_format
        )
        redshift_hook.run(sql_stmt)
        print('Stage To Redshift Task Implementation Success')

    @task
    def load_fact_table(**kwargs):
        table = kwargs['params']['table']
        redshift_hook = PostgresHook(REDSHIFT_CONN_ID)
        redshift_hook.run(LOAD_REDSHIFT_QUERIES.bloodtest_table_insert)
        print('Fact Table {} load succesfully'.format(table))

    @task
    def load_dimension_tables(**kwargs):
        table = kwargs['params']['table']
        truncate_switch = kwargs['params']['truncate']
        insert_query = kwargs['params']['insert_query']
        redshift_hook = PostgresHook(REDSHIFT_CONN_ID)
        if truncate_switch:
            redshift_hook.run(insert_query[1])
        else:
            redshift_hook.run(insert_query[0])
        print('Dimension Table {} load succesfully'.format(table))

    @task
    def data_quality_check(**kwargs):
        table = kwargs["params"]["table"]
        column = kwargs["params"]["column"]
        redshift_hook = PostgresHook(REDSHIFT_CONN_ID)
        records = redshift_hook.get_records(LOAD_REDSHIFT_QUERIES.quality_null.format(column, table))
        if records[0][0] > 0:
            raise ValueError(f"Data quality check failed. Table {table} with column {column} returned NULL values")
        print(f"Data quality on table {table} with column {column} check passed with no NULL values")

    @task
    def patient_monitoring():
        redshift_hook = PostgresHook(REDSHIFT_CONN_ID)
        records = redshift_hook.get_records(LOAD_REDSHIFT_QUERIES.patient_monitoring)
        red_list = []
        for re in records:
            if re[1] > 10:
                red_list.append(re[0])
        print(red_list)
        # if records[0][0] > 0:
        #     raise ValueError(f"Data quality check failed. Table {table} with column {column} returned NULL values")
        # print(f"Data quality on table {table} with column {column} check passed with no NULL values")

    start_operator = EmptyOperator(task_id='Begin_execution')

    create_tables = create_tables()

    stage_logs_to_redshift = stage_redshift(
        params={
            "table": "staging_logs",
            "bucket": "s3://salvig-sst-ovom-airflow/blood_test",
            "json_format": "format as json 'auto'"
        }
    )

    stage_patient_to_redshift = stage_redshift(
        task_id='stage_patient_to_redshift',
        params={
            "table": "staging_patient",
            "bucket": "s3://salvig-sst-ovom-airflow/patient",
            "json_format": "format as json 'auto'",
        }
    )

    load_fact_table = load_fact_table(
        params={
            'table': 'bloodtest'
        }
    )

    load_patient_table = load_dimension_tables(
        task_id='load_patient_table',
        params={
            'table': 'patient',
            'insert_query': LOAD_REDSHIFT_QUERIES.patient_table_insert,
            'truncate': False
        }
    )

    load_doctor_table = load_dimension_tables(
        task_id='load_doctor_table',
        params={
            'table': 'doctor',
            'insert_query': LOAD_REDSHIFT_QUERIES.doctor_table_insert,
            'truncate': False
        }
    )

    load_laboratory_table = load_dimension_tables(
        task_id='load_laboratory_table',
        params={
            'table': 'laboratory',
            'insert_query': LOAD_REDSHIFT_QUERIES.laboratory_table_insert,
            'truncate': False
        }
    )

    patient_monitoring_check = patient_monitoring()

    end_operator = EmptyOperator(task_id='end_execution')

    start_operator >> create_tables

    create_tables >> stage_logs_to_redshift
    create_tables >> stage_patient_to_redshift

    stage_logs_to_redshift >> load_fact_table
    stage_patient_to_redshift >> load_fact_table

    load_patient_table << load_fact_table
    load_doctor_table << load_fact_table
    load_laboratory_table << load_fact_table

    patient_monitoring_check << load_patient_table
    patient_monitoring_check << load_doctor_table
    patient_monitoring_check << load_laboratory_table

    patient_monitoring_check >> end_operator


run_project = dwh_redshift_load()
