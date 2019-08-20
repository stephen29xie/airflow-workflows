"""
This DAG is used as an example for a failed task. This is a purely a dummy DAG
"""

from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'Stephen Xie',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 20),
    'email': ['stephenxie.airflow@gmail.com'],
    'email_on_failure': True,
    'retries': 0,
}

dag = DAG(
    dag_id='fail_example',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)


def divide_by_zero():
    """
    This will return a ZeroDivisionError
    :return: None
    """
    return 1/0


start = DummyOperator(task_id='start_dummy',
                      dag=dag)

end = DummyOperator(task_id='end_dummy',
                    dag=dag)

fail_task = PythonOperator(task_id='fail_task',
                           python_callable=divide_by_zero,
                           dag=dag)

start >> fail_task >> end
