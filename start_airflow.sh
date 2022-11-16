#!/bin/bash

# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/src/dags

# initialize the database
airflow db init

airflow users create \
--username admin \
--firstname Jon \
--lastname Snow \
--role Admin \
--email john@snow.org

# start the web server, default port is 8080
airflow webserver --daemon --port 8080

# start the scheduler
# open a new terminal or else run webserver with ``-D`` option to run it as a daemon
airflow scheduler --daemon

# visit localhost:8080 in the browser and use the admin account you just
# created to login.

# Wait till airflow web-server is ready
echo "Waiting for Airflow web server..."
while true; do
    _RUNNING=$(ps aux | grep airflow-webserver | grep ready | wc -l)
    if [ $_RUNNING -eq 0 ]; then
        sleep 1
    else
        echo "Airflow web server is ready"
        break;
    fi
done