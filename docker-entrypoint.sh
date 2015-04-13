#!/bin/bash
set -e

HOME_DIR=/home/pganalyze
CONFIG_FILE=$HOME_DIR/.pganalyze_collector.conf

if [ -z "$PGA_API_KEY" ]; then
  echo "Error: You need to set PGA_API_KEY to your pganalyze API key"
  exit 1
fi

if [ -z "$DB_NAME" ]; then
  echo "Error: You need to set DB_NAME to your database name"
  exit 1
fi

echo """
[pganalyze]
api_key: $PGA_API_KEY
db_name: $DB_NAME
db_host: ${DB_HOST:-db}
db_port: ${DB_PORT:-5432}""" > $CONFIG_FILE

if [ ! -z "$DB_USERNAME" ]; then
  echo "db_username: $DB_USERNAME" >> $CONFIG_FILE
fi

if [ ! -z "$DB_PASSWORD" ]; then
  echo "db_password: $DB_PASSWORD" >> $CONFIG_FILE
fi

if [ ! -z "$PGA_API_URL" ]; then
  echo "api_url: $PGA_API_URL" >> $CONFIG_FILE
fi

chown pganalyze:pganalyze $CONFIG_FILE
chmod go-rwx $CONFIG_FILE

echo "Doing initial collector test run..."

setuser pganalyze python $HOME_DIR/pganalyze-collector.py

echo "*/10 * * * * pganalyze /usr/bin/python /home/pganalyze/pganalyze-collector.py --cron 2>&1 | /usr/bin/logger -t collector" > /etc/cron.d/pganalyze

echo "Good to go, collector will run every 10 minutes"

exec "$@"
