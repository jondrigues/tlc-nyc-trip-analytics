#!/usr/bin/env bash

service=$1

echo "Starting DB"
if [ $service = "webserver" ]
then
  airflow db check
  airflow db init
  airflow connections add 'pg_analytics' \
      --conn-type 'postgresql' \
      --conn-login 'admin' \
      --conn-password 'admin' \
      --conn-host 'pg_analytics' \
      --conn-port '5432' \
      --conn-schema 'trusted'
  airflow users create -e admin@test.com -f admin -l admin -p admin -u admin -r Admin
fi
echo "Starting" $service
airflow $service