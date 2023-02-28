#!/usr/bin/env bash

airflow db init

# create user
airflow users create \
          --username airflow \
          --firstname airflow \
          --lastname airflow \
          --password airflow \
          --role Admin \
          --email airflow@airflow.com

# run scheduler
airflow scheduler &

# setup connections
python3 "$AIRFLOW_HOME/setup_connection.py"

# run webserver
exec airflow webserver
