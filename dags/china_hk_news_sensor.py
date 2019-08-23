"""
A DAG to check (using a sensor) for news about China or HK in the Reddit News db and sends an email alert if found
"""

from airflow.models import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.sql_sensor import SqlSensor

from datetime import datetime
import os
from configparser import ConfigParser

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
    'email': ['stephenxie.airflow@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(dag_id='chinese_hk_news_sensor',
          default_args=default_args,
          schedule_interval='@hourly',
          max_active_runs=1,
          catchup=False)

# select posts that include 'china' or 'hong kong' in the title within the previous hour
QUERY = """
        SELECT *
        FROM {table}
        WHERE (post_title ILIKE '%hong kong%' OR post_title ILIKE '%china%')
        AND creation_datetime BETWEEN DATE_TRUNC('hour', NOW() - interval '1 hour') AND DATE_TRUNC('hour', NOW())
        """.format(table=config['reddit_news_db']['table'])

sensor = SqlSensor(task_id="news_sensor",
                   soft_fail=True,  # mark task as SKIPPED on failure
                   conn_id=config['Airflow']['postgres_conn_id'],
                   sql=QUERY,
                   poke_interval=1,  # fail right away if no records are found
                   timeout=1,
                   dag=dag)

email_alert = EmailOperator(task_id='email_alert',
                            to=['stephenxie.airflow@gmail.com'],
                            subject='China/Hong Kong news available',
                            html_content='<h1>There is news about China or Hong Kong within the previous hour</h1>',
                            dag=dag)

sensor >> email_alert
