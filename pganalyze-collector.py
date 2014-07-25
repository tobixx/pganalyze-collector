#!/usr/bin/env python

# Copyright (c) 2013, pganalyze Team <team@pganalyze.com>
#  All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# * Neither the name of pganalyze nor the names of its contributors may be used
# to endorse or promote products derived from this software without specific
# prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

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
from pgacollector.PostgresInformation import PostgresInformation
from pgacollector.PgStatPlans import PgStatPlans
from pgacollector.PgStatStatements import PgStatStatements
from pgacollector.SystemInformation import SystemInformation
from pgacollector.DB import DB
from pgacollector.Configuration import Configuration


MYNAME = 'pganalyze-collector'
VERSION = '0.6.1-dev'
API_URL = 'https://pganalyze.com/queries'
dbconf = {}


def check_database():
    global db
    db = DB(querymarker=MYNAME, host=dbconf['host'], port=dbconf['port'], username=dbconf['username'],
            password=dbconf['password'], dbname=dbconf['dbname'])

    if not SystemInformation().on_heroku and not db.run_query('SHOW is_superuser')[0]['is_superuser'] == 'on':
        logger.error("User %s isn't a superuser" % dbconf['username'])
        sys.exit(1)


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
    parser.add_option('--no-reset', '-n', action='store_true', dest='noreset',
                      help='Don\'t reset statistics after posting to web. Only use for testing purposes.')
    parser.add_option('--no-query-parameters', action='store_false', dest='queryparameters',
                      default=True,
                      help='Don\'t send queries containing parameters to the server. These help in reproducing problematic queries but can raise privacy concerns.')
    parser.add_option('--no-system-information', action='store_false', dest='systeminformation',
                      default=True,
                      help='Don\'t collect OS level performance data'),

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

    indexstats = {}
    tablestats = {}

    #Prepare stats for later merging
    for row in PI.index_stats():
        del row['table']
        indexkey = '.'.join([row.pop('schema'), row.pop('index')])
        indexstats[indexkey] = row

    for row in PI.table_stats():
        tablekey = '.'.join([row.pop('schema'), row.pop('table')])
        tablestats[tablekey] = row

    # Merge Table & Index bloat information into table/indexstats dicts
    for row in PI.bloat():
        tablekey = '.'.join([row.get('schemaname'), row.pop('tablename')])
        indexkey = '.'.join([row.pop('schemaname'), row.pop('iname')])
        if tablekey in tablestats:
            tablestats[tablekey]['wasted_bytes'] = row['wastedbytes']
        if indexkey in indexstats:
            indexstats[indexkey]['wasted_bytes'] = row['wastedibytes']

    # Combine Table, Index and Constraint information into a combined schema dict
    for row in PI.columns():
        tablekey = '.'.join([row['schema'], row['table']])
        if not tablekey in schema:
            schema[tablekey] = {}

        schema[tablekey]['schema_name'] = row.pop('schema')
        schema[tablekey]['table_name'] = row.pop('table')
        schema[tablekey]['size_bytes'] = row.pop('tablesize')
        schema[tablekey]['stats'] = tablestats[tablekey]

        if not 'columns' in schema[tablekey]:
            schema[tablekey]['columns'] = []
        schema[tablekey]['columns'].append(row)

    for row in PI.indexes():
        statskey = '.'.join([row['schema'], row['name']])
        tablekey = '.'.join([row.pop('schema'), row.pop('table')])

        #Merge index stats
        row = dict(row.items() + indexstats[statskey].items())

        if not 'indices' in schema[tablekey]:
            schema[tablekey]['indices'] = []
        schema[tablekey]['indices'].append(row)

    for row in PI.constraints():
        tablekey = '.'.join([row.pop('schema'), row.pop('table')])
        if not 'constraints' in schema[tablekey]:
            schema[tablekey]['constraints'] = []
        schema[tablekey]['constraints'].append(row)


    # Populate result dictionary
    info['schema']   = schema.values()
    info['version']  = PI.version()
    info['settings'] = PI.settings()
    info['bgwriter'] = PI.bgwriter_stats()
    info['database'] = PI.db_stats()
    info['locks']    = PI.locks()
    info['backends'] = PI.backends(option['queryparameters'])

    return info


class DatetimeEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return str(obj)

        return json.JSONEncoder.default(self, obj)


def post_data_to_web(data):
    to_post = {}
    to_post['data'] = json.dumps(data, cls=DatetimeEncoder)
    to_post['api_key'] = dbconf['api_key']
    to_post['collected_at'] = calendar.timegm(time.gmtime())
    to_post['submitter'] = "%s %s" % (MYNAME, VERSION)
    to_post['query_parameters'] = option['queryparameters']
    to_post['system_information'] = option['systeminformation']
    to_post['query_source'] = option['query_source']

    if option['dryrun']:
        logger.info("Dumping data that would get posted")

        to_post['data'] = json.loads(to_post['data'])
        for query in to_post['data']['queries']:
            for plan in query['plans']:
                if 'explain' in plan:
                    plan['explain'] = json.loads(plan['explain'])
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
        # FIXME: Fail fast on wrong API key
        if code == 200 or num_tries >= 3:
            return message,code
        logger.debug("Got %s while posting data: %s, sleeping 60 seconds then trying again" % (code, message))
        time.sleep(60)


def fetch_query_information():
    query = "SELECT extname FROM pg_extension"

    extensions = map(lambda q: q['extname'], db.run_query(query))
    if 'pg_stat_plans' in extensions:
        logger.debug("Found pg_stat_plans, using it for query information")
        return ['pg_stat_plans', PgStatPlans(db).fetch_queries(option['queryparameters'])]
    elif 'pg_stat_statements' in extensions:
        logger.debug("Found pg_stat_statements, using it for query information")
        return ['pg_stat_statements', PgStatStatements(db).fetch_queries()]
    else:
        logger.error("Couldn't find either pg_stat_plans or pg_stat_statements, aborting")
        sys.exit(1)


def main():
    global option, logger, dbconf

    option = parse_options()
    logger = configure_logger()

    c = Configuration(option)

    if option['generate_config']:
        c.write()
        sys.exit(0)

    dbconf = c.read()

    check_database()

    data = {}
    (option['query_source'], data['queries']) = fetch_query_information()

    if option['systeminformation'] and not SystemInformation().on_heroku:
        data['system'] = fetch_system_information()

    data['postgres'] = fetch_postgres_information()

    (output, code) = post_data_to_web(data)
    if code == 200:
        if not option['quiet']:
            logger.info("Submitted successfully")

        if not option['noreset']:
            logger.debug("Resetting stats!")
            if option['query_source'] == 'pg_stat_plans':
                db.run_query("SELECT pg_stat_plans_reset()")
            elif option['query_source'] == 'pg_stat_statements':
                db.run_query("SELECT pg_stat_statements_reset()")
    else:
        logger.error("Rejected by server: %s" % output)


if __name__ == '__main__':
    main()
