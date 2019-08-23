"""
This DAG creates a Postgres Connection (for Hooks to use) for the Airflow session
"""

from airflow.models import Connection, settings, DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import os
from configparser import ConfigParser
from datetime import datetime
import logging


# get path to config file relative to this file
current_dir = os.path.dirname(__file__)
filepath = os.path.join(current_dir, '../config.ini')
# read config.ini file
config = ConfigParser()
config.read(filepath)

default_args = {
    'owner': 'Stephen Xie',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 20),
    'email': ['stephen29xie@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    dag_id='create_postgres_connection',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    max_active_runs=1
)


def create_postgres_connection(conn_id, host, schema, login, password, port=5432, **kwargs):
    """
    Creates a Postgres Connection (for Hooks to use) for the Airflow session

    :param conn_id: Str - name of Airflow Connection
    :param host: Str - db host
    :param schema: Str - db name
    :param login: Str - db login
    :param password: Str - db password
    :param port: Str - db port
    :param kwargs: Dict - keyword arguments
    :return: None
    """
    logging.info('Creating Postgres Connection...')

    # Create connection to our Postgres instance
    pg_connection = Connection(
        conn_id=conn_id,
        conn_type='postgres',
        host=host,
        schema=schema,
        login=login,
        password=password,
        port=port
    )

    # Add the Connection to the Airflow session
    session = settings.Session()
    session.add(pg_connection)
    session.commit()
    logging.info('Successfully created Postgres Connection')
    session.close()


create_postgres_connection = PythonOperator(task_id='create_postgres_connection',
                                            python_callable=create_postgres_connection,
                                            op_kwargs={
                                                'conn_id': config['Airflow']['postgres_conn_id'],
                                                'host': config['reddit_news_db']['host'],
                                                'schema': config['reddit_news_db']['dbname'],
                                                'login': config['reddit_news_db']['username'],
                                                'password': config['reddit_news_db']['password'],
                                                'port': config['reddit_news_db']['port']
                                            },
                                            provide_context=True,
                                            dag=dag)

start = DummyOperator(task_id='start_dummy',
                      dag=dag)
end = DummyOperator(task_id='end_dummy',
                    dag=dag)

start >> create_postgres_connection >> end
