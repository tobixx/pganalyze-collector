#!/usr/bin/env python

# Set up vendor include path
import sys
sys.path.insert(1, sys.path[0] + '/vendor/')

import os
import time
import calendar
import datetime
import re
import json
import urllib
import logging
from optparse import OptionParser

compressor_lib = None

try:
    import zlib as compressor
    compressor_lib = 'zlib'
except Exception as e:
    pass

from pgacollector.PostgresInformation import PostgresInformation
from pgacollector.PgStatStatements import PgStatStatements
from pgacollector.SystemInformation import SystemInformation
from pgacollector.DB import DB
from pgacollector.Configuration import Configuration

MYNAME = 'pganalyze-collector'
VERSION = '0.8.1'
API_URL = 'https://api.pganalyze.com/v1/snapshots'
dbconf = {}

def setup_database():
    return DB(querymarker=MYNAME, host=dbconf['host'], port=dbconf['port'], username=dbconf['username'],
              password=dbconf['password'], dbname=dbconf['dbname'])

def is_remote_system():
    global dbconf
    is_awshost = dbconf['host'] != None and re.search('amazonaws.com$', dbconf['host']) != None
    return is_awshost or SystemInformation().on_heroku

def parse_options(print_help=False):
    parser = OptionParser(usage="%s [options]" % MYNAME, version="%s %s" % (MYNAME, VERSION))

    parser.add_option('-v', '--verbose', action='store_true', dest='verbose',
                      help='Print verbose debug information')
    parser.add_option('--config', action='store', type='string', dest='configfile',
                      default='$HOME/.pganalyze_collector.conf, /etc/pganalyze_collector.conf',
                      help='Specifiy alternative path for config file. Defaults: %default')
    parser.add_option('--generate-config', action='store_true', dest='generate_config',
                      help='Writes a default configuration file to $HOME/.pganalyze_collector.conf unless specified otherwise with --config')
    parser.add_option('--api-key', action='store', type='string', dest='apikey',
                      help='Use specified API key when writing a fresh config file')
    parser.add_option('--cron', '-q', action='store_true', dest='quiet',
                      help='Suppress all non-warning output during normal operation')
    parser.add_option('--dry-run', '-d', action='store_true', dest='dryrun',
                      help='Print JSON data that would get sent to web service and exit afterwards.')
    parser.add_option('--no-postgres-settings', action='store_false', dest='collect_postgres_settings',
                      default=True,
                      help='Don\'t collect Postgres configuration settings')
    parser.add_option('--no-postgres-locks', action='store_false', dest='collect_postgres_locks',
                      default=True,
                      help='Don\'t collect Postgres lock information')
    parser.add_option('--no-postgres-functions', action='store_false', dest='collect_postgres_functions',
                      default=True,
                      help='Don\'t collect Postgres function/procedure information')
    parser.add_option('--no-postgres-queries', action='store_false', dest='collect_postgres_queries',
                      default=True,
                      help='Don\'t collect Postgres queries information')
    parser.add_option('--no-postgres-bloat', action='store_false', dest='collect_postgres_bloat',
                      default=True,
                      help='Don\'t collect Postgres table/index bloat statistics')
    parser.add_option('--no-postgres-views', action='store_false', dest='collect_postgres_views',
                      default=True,
                      help='Don\'t collect Postgres view/materialized view information')
    parser.add_option('--no-system-information', action='store_false', dest='systeminformation',
                      default=True,
                      help='Don\'t collect OS level performance data')
    parser.add_option('--no-reset', '-n', action='store_true', dest='_dummy_noreset',
		              help='Dummy option, no-reset is required default since 0.7')
    parser.add_option('--no-compression', action='store_false', dest='compression_enabled',
                      default=True,
                      help='Disable gzip compression for statistics data sent')

    if print_help:
        parser.print_help()
        return

    (options, args) = parser.parse_args()
    options = options.__dict__
    options['configfile'] = re.split(',\s+', options['configfile'].replace('$HOME', os.environ['HOME']))
    options['api_url'] = API_URL

    return options


def configure_logger():
    logtemp = logging.getLogger(MYNAME)

    loglevel = logging.DEBUG if option['verbose'] else logging.INFO
    logformat = '%(levelname)s - %(asctime)s %(message)s'

    logargs = {
        'format': logformat,
        'level': loglevel,
    }
    logging.basicConfig(**logargs)

    return logtemp


def fetch_system_information():
    SI = SystemInformation(db)
    info = {}

    info['os'] = SI.os()
    info['cpu'] = SI.cpu()
    info['scheduler'] = SI.scheduler()
    info['storage'] = SI.storage()
    info['memory'] = SI.memory()

    return info


def fetch_postgres_information():
    """
    Fetches information about the Postgres installation

    Returns a groomed version of all info ready for posting to the web
"""
    PI = PostgresInformation(db)

    info = {}
    schema = {}

    table_bloat_stats = {}
    index_bloat_stats = {}

    if option['collect_postgres_bloat']:
        for row in PI.table_bloat():
            table_bloat_stats[row['oid']] = row['wasted_bytes']

        for row in PI.index_bloat():
            index_bloat_stats[row['index_oid']] = row['wasted_bytes']

    for row in PI.relations(option['collect_postgres_views']):
        oid = row.pop('oid')
        schema[oid] = dict((k, row[k]) for k in ('schema_name', 'table_name', 'relation_type'))
        schema[oid]['stats'] = dict((k, row[k]) for k in set(row.keys()) - set(['relid', 'relname', 'schema_name', 'schemaname', 'table_name', 'relation_type']))
        schema[oid]['stats']['wasted_bytes'] = table_bloat_stats.get(oid)
        schema[oid]['columns'] = []
        schema[oid]['indices'] = []
        schema[oid]['constraints'] = []

    if option['collect_postgres_views']:
        for row in PI.view_definitions():
            schema[row['oid']]['view_definition'] = row['view_definition']

    for row in PI.columns(option['collect_postgres_views']):
        oid = row.pop('oid')
        schema[oid]['columns'].append(row)

    for row in PI.indexes(option['collect_postgres_views']):
        oid = row.pop('oid')
        row['wasted_bytes'] = index_bloat_stats.get(row.pop('index_oid'))
        schema[oid]['indices'].append(row)

    for row in PI.constraints():
        oid = row.pop('oid')
        schema[oid]['constraints'].append(row)

    # Populate result dictionary
    info['schema']   = schema.values()
    info['version']  = PI.version()
    info['server']   = PI.server_stats()
    info['database'] = PI.db_stats()
    info['bgwriter'] = PI.bgwriter_stats()
    info['backends'] = PI.backends()
    info['replication']           = PI.replication()
    info['replication_conflicts'] = PI.replication_conflicts()

    if option['collect_postgres_functions']:
        info['functions'] = PI.functions()

    if option['collect_postgres_settings']:
        info['settings'] = PI.settings()

    if option['collect_postgres_locks']:
        info['locks']    = PI.locks()

    return info


class DatetimeEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return str(obj)

        return json.JSONEncoder.default(self, obj)


def post_data_to_web(data):
    data = json.dumps(data, cls=DatetimeEncoder)

    to_post = {}

    if option['compression_enabled'] and compressor_lib:
        logger.debug("Compressing data using %s", compressor_lib)
        to_post['data'] = compressor.compress(data)
        to_post['data_compressor'] = compressor_lib
    else:
        to_post['data'] = data

    to_post['api_key'] = dbconf['api_key']
    to_post['collected_at'] = calendar.timegm(time.gmtime())
    to_post['submitter'] = "%s %s" % (MYNAME, VERSION)
    to_post['system_information'] = option['systeminformation']
    to_post['query_source'] = option.get('query_source')
    to_post['no_reset'] = True

    if option['dryrun']:
        logger.info("Dumping data that would get posted")

        to_post['data'] = json.loads(data)
        print(json.dumps(to_post, sort_keys=True, indent=4, separators=(',', ': '), cls=DatetimeEncoder))

        logger.info("Exiting.")
        sys.exit(0)

    num_tries = 0
    while True:
        try:
            # FIXME: urllib doesn't do any SSL verification
            res = urllib.urlopen(dbconf['api_url'], urllib.urlencode(to_post))
            message = res.read()
            code = res.getcode()
        except IOError as e:
            message = str(e)
            code = 'exception'

        num_tries += 1
        if code == 200 or message == 'ERROR: Invalid API key' or num_tries >= 3:
            return message,code
        if not option['quiet']:
            logger.info("Got %s while posting data: %s, sleeping 60 seconds then trying again" % (code, message))
        time.sleep(60)


def fetch_query_information():
    query = "SELECT extname FROM pg_extension"

    extensions = map(lambda q: q['extname'], db.run_query(query))
    if 'pg_stat_statements' in extensions:
        logger.debug("Found pg_stat_statements, using it for query information")
        return ['pg_stat_statements', PgStatStatements(db).fetch_queries()]
    else:
        logger.debug("Trying to enable pg_stat_statements...")
        db.run_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", commit = True)
        return ['pg_stat_statements', PgStatStatements(db).fetch_queries()]


def main():
    global option, logger, dbconf, db

    option = parse_options()
    logger = configure_logger()

    c = Configuration(option)

    if option['generate_config']:
        c.write()
        sys.exit(0)

    dbconf = c.read()
    db     = setup_database()

    if db.version_numeric < 90200:
        logger.error("To use the collector you must have at least Postgres 9.2 or newer")
        sys.exit(1)

    if is_remote_system():
        option['systeminformation'] = False

    data = {}
    if option['collect_postgres_queries']:
        (option['query_source'], data['queries']) = fetch_query_information()

    if option['systeminformation']:
        data['system'] = fetch_system_information()

    data['postgres'] = fetch_postgres_information()

    (output, code) = post_data_to_web(data)
    if code == 200:
        if not option['quiet']:
            logger.info("Submitted successfully")
    else:
        logger.error("Rejected by server: %s" % output)


if __name__ == '__main__':
    main()
