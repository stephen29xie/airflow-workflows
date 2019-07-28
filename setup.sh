#!/usr/bin/env bash

pip install -r requirements.txt

export AIRFLOW_HOME=$(pwd)

echo "$AIRFLOW_HOME"

airflow initdb

airflow webserver --port 8080 & airflow scheduler &

# Useful commands

# airflow trigger_dag -e YYYY-MM-DD <dag_id>
# airflow list_dags
# airflow list_tasks <dag_id>
# airflow test <dag_id> <task_id> <start_date>

# stop airflow services
# kill $(ps -ef | grep "airflow webserver" | awk '{print $2}')
# kill $(ps -ef | grep "airflow scheduler" | awk '{print $2}')