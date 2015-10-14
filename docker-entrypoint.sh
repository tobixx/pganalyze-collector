#!/bin/sh

set -e

if [ "$1" != 'collector' ]; then
  exec "$@"
fi

HOME_DIR=/home/pganalyze
CONFIG_FILE=$HOME_DIR/.pganalyze_collector.conf

if [ -z "$PGA_API_KEY" ]; then
  echo "Error: You need to set PGA_API_KEY to your pganalyze API key"
  exit 1
fi

echo """
[pganalyze]
api_key: $PGA_API_KEY
"""  > $CONFIG_FILE

if [ ! -z "$PGA_API_URL" ]; then
  echo "api_url: $PGA_API_URL" >> $CONFIG_FILE
fi

if [ ! -z "$DB_URL" ]; then
  echo "db_url: $DB_URL" >> $CONFIG_FILE
else
  if [ -z "$DB_NAME" ]; then
    echo "Error: You need to set DB_NAME to your database name"
    exit 1
  fi

  echo "db_name: $DB_NAME" >> $CONFIG_FILE
  echo "db_host: ${DB_HOST:-db}" >> $CONFIG_FILE
  echo "db_port: ${DB_PORT:-5432}" >> $CONFIG_FILE

  if [ ! -z "$DB_USERNAME" ]; then
    echo "db_username: $DB_USERNAME" >> $CONFIG_FILE
  fi

  if [ ! -z "$DB_PASSWORD" ]; then
    echo "db_password: $DB_PASSWORD" >> $CONFIG_FILE
  fi
fi

chown pganalyze:pganalyze $CONFIG_FILE
chmod go-rwx $CONFIG_FILE

echo "Doing initial collector test run..."

gosu pganalyze python $HOME_DIR/pganalyze-collector.py $OPTS

rm /var/spool/cron/crontabs/root
echo "*/10 * * * * /usr/bin/python /home/pganalyze/pganalyze-collector.py --cron $OPTS 2>&1 | /usr/bin/logger -t collector" > /var/spool/cron/crontabs/pganalyze

echo "Good to go, collector will run every 10 minutes"

# These automatically run in the background
/sbin/syslogd
/usr/sbin/crond

touch /var/log/messages

tail -f /var/log/messages
