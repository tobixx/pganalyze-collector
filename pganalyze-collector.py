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


import os
import sys
import subprocess
import time
import calendar
import datetime
import re
import json
import urlparse
import urllib
import logging
import ConfigParser
from optparse import OptionParser
from stat import *
import platform
from pgacollector.PostgresInformation import PostgresInformation
from pprint import pprint

ON_HEROKU = os.environ.has_key('DYNO')


while True:
    try:
        import psycopg2 as pg
        break
    except Exception as e:
        pass

    try:
        import pg8000 as pg
        break
    except Exception as e:
        pass

    print("*** Couldn't import database driver")
    print("*** Please install the python-psycopg2 package or the pg8000 module")
    sys.exit(1)


MYNAME = 'pganalyze-collector'
VERSION = '0.5.1-dev'
API_URL = 'https://pganalyze.com/queries'




class PgStatPlans():

    def __init__(self):
        pass

    def fetch_queries(self):
        both_fields = ["userid", "dbid",
                       "calls", "rows", "total_time",
                       "shared_blks_hit", "shared_blks_read", "shared_blks_written",
                       "local_blks_hit", "local_blks_read", "local_blks_written",
                       "temp_blks_read", "temp_blks_written"]

        query_fields = ["time_variance", "time_stddev"] + both_fields

        plan_fields = ["planid", "had_our_search_path", "from_our_database",
                       "query_explainable", "last_startup_cost", "last_total_cost"] + both_fields

        query = "SELECT pq.normalized_query AS pq_normalized_query"
        query += ", p.query AS p_query"

        # Generate list of fields we'e interested in
        query += ", " + ", ".join(map(lambda s: "pq.%s AS pq_%s" % (s, s), query_fields))
        query += ", " + ", ".join(map(lambda s: "p.%s AS p_%s" % (s, s), plan_fields))

        query += " FROM pg_stat_plans p"
        query += " LEFT JOIN pg_stat_plans_queries pq ON p.planid = ANY (pq.planids)"

        #FIXME: Should all these exclusions moved down to the query_explainable check?

        # EXPLAIN, COPY and SET commands cannot be explained
        query += " WHERE p.query !~* '^\\s*(EXPLAIN|COPY|SET)\\y'"

        # Plans in pg_catalog cannot be explained
        query += " AND p.query !~* '\\ypg_catalog\\.'"

        # We don't want our stuff in the statistics
        query += " AND p.query !~* '^%s'" % re.sub(r'([*/])', r'\\\1', db.querymarker)

        # Remove all plans which we can't explain
        query += " AND p.from_our_database = TRUE"
        query += " AND p.planid = ANY (pq.planids);"

        fetch_plan = "SELECT pg_stat_plans_explain(%s, %s, %s) AS explain"
        set_explain_format = "SET pg_stat_plans.explain_format TO JSON; "

        db.run_query(set_explain_format, True)

        queries = {}

        # Fetch joined list of all queries and plans
        for row in db.run_query(query, False):

            # merge pg_stat_plans_queries values into result
            query = dict((key[3:], row[key]) for key in filter(lambda r: r.find('pq_') == 0, row))
            normalized_query = query['normalized_query']

            logger.debug("Processing query: %s" % normalized_query)

            # if we haven't seen the query yet - add it
            if 'normalized_query' not in queries:
                queries[normalized_query] = query

            # merge pg_stat_plans values into result
            plan = dict((key[2:], row[key]) for key in filter(lambda r: r.find('p_') == 0, row))

            # Delete parmaterized example queries if wanted
            if not option['queryparameters']:
                del (plan['query'])

            # initialize plans array
            if 'plans' not in queries[normalized_query]:
                queries[normalized_query]['plans'] = []

            # try explaining the query if pg_stat_plans thinks it's possible
            if plan['query_explainable']:
                try:
                    result = db.run_query(fetch_plan % (plan['planid'], plan['userid'], plan['dbid']), True)
                    plan['explain'] = result[0]['explain']
                except Exception as e:
                    logger.debug("Got an error while explaining: %s" % e)
                    plan['explain_error'] = str(e)
                    db.rollback()
                    db.run_query(set_explain_format, True)

            queries[normalized_query]['plans'].append(plan)

        return queries.values()

class PgStatStatements():

    def __init__(self):
        pass

    def fetch_queries(self):
        columns = ["userid", "dbid",
                       "calls", "rows", "total_time",
                       "shared_blks_hit", "shared_blks_read", "shared_blks_dirtied", "shared_blks_written",
                       "local_blks_hit", "local_blks_read", "local_blks_dirtied", "local_blks_written",
                       "temp_blks_read", "temp_blks_written",
                       "blk_read_time", "blk_write_time"]

        query = "SELECT query AS normalized_query"

        # Generate list of fields we'e interested in
        query += ", " + ", ".join(columns)

        query += " FROM pg_stat_statements"

        # We don't want our stuff in the statistics
        query += " WHERE query !~* '^%s'" % re.sub(r'([*/])', r'\\\1', db.querymarker)
        # Filter out queries we shouldn't see in the first place
        query += " AND query <> '<insufficient privilege>'"
        # Only get queries from current database
        query += " AND dbid IN (SELECT oid FROM pg_database WHERE datname = current_database())"

        queries = []

        for row in db.run_query(query, False):
            row['plans'] = []
            queries.append(row)

        return queries


class SystemInformation():
    def __init__(self):
        self.system = platform.system()

    def OS(self):
        osinfo = {}
        osinfo['system'] = platform.system()
        if self.system == 'Linux':
            (osinfo['distribution'], osinfo['distribution_version']) = platform.linux_distribution()[0:2]
        elif self.system == 'Darwin':
            osinfo['distribution'] = 'OS X'
            osinfo['distribution_version'] = platform.mac_ver()[0]

        osinfo['architecture'] = platform.machine()
        osinfo['kernel_version'] = platform.release()

        # FIXME: Refactor to use /sys/devices/virtual/dmi/id/{sys_vendor,product_name}
        dmidecode = find_executable_in_path('dmidecode')
        if dmidecode:
            try:
                vendor = subprocess.check_output([dmidecode, '-s', 'system-manufacturer']).strip()
                model = subprocess.check_output([dmidecode, '-s', 'system-product-name']).strip()
                if vendor and model:
                    osinfo['server_model'] = "%s %s" % (vendor, model)

            except Exception as e:
                logger.debug("Error while collecting system manufacturer/model via dmidecode: %s" % e)

        return osinfo

    def CPU(self):
        result = {}

        if self.system == 'Linux':
            (procstat, cpuinfo) = self._fetch_linux_cpu_data()

            result['busy_times'] = self._parse_linux_cpu_procstat(procstat)
            result['hardware'] = self._parse_linux_cpu_cpuinfo(cpuinfo)

        else:
            return None

        return (result)

    def _fetch_linux_cpu_data(self):

        with open('/proc/stat', 'r') as f:
            procstat = f.readlines()

        with open('/proc/cpuinfo', 'r') as f:
            cpuinfo = f.readlines()

        return procstat, cpuinfo

    def _parse_linux_cpu_procstat(self, procstat):

        # Fetch combined CPU counter from lines
        os_counters = filter(lambda x: x.find('cpu ') == 0, procstat)[0]

        # tokenize, strip row heading
        os_counters = os_counters.split()[1:]

        # Correct all values to msec
        kernel_hz = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
        os_counters = map(lambda x: int(x) * (1000 / kernel_hz), os_counters)

        os_counter_names = ['user_msec', 'nice_msec', 'system_msec', 'idle_msec', 'iowait_msec',
                            'irq_msec', 'softirq_msec', 'steal_msec', 'guest_msec', 'guest_nice_msec']

        return dict(zip(os_counter_names, os_counters))

    def _parse_linux_cpu_cpuinfo(self, cpuinfo):

        # Trim excessive whitespace in strings, return two elements per line
        cpuinfo = map(lambda x: " ".join(x.split()).split(' : '), cpuinfo)

        hardware = {}
        hardware['model'] = next(l[1] for l in cpuinfo if l[0] == 'model name')
        hardware['cache_size'] = next(l[1] for l in cpuinfo if l[0] == 'cache size')
        hardware['speed_mhz'] = next(round(float(l[1]), 2) for l in cpuinfo if l[0] == 'cpu MHz')

        try:
            hardware['sockets'] = int(max([l[1] for l in cpuinfo if l[0] == 'physical id'])) + 1
        except ValueError:
            # Fallthrough - we didn't find any physical id stanza, assuming one socket
            hardware['sockets'] = 1

        try:
            hardware['cores_per_socket'] = next(int(l[1]) for l in cpuinfo if l[0] == 'cpu cores')
        except StopIteration:
            # Fallthrough - we didn't find cpu cores stanza
           pass

        # We didn't get cpu core identifiers, just use the count of processors
        if not 'cores_per_socket' in hardware:
            try:
                hardware['cores_per_socket'] = int(max([l[1] for l in cpuinfo if l[0] == 'processor'])) + 1
            except ValueError:
                # All bets are off
                hardware['cores_per_socket'] = 1

        return hardware

    def Scheduler(self):
        result = {}
        if self.system != 'Linux': return None

        with open('/proc/stat', 'r') as f:
            os_counters = f.readlines()

        os_counters = [l.split() for l in os_counters if len(l) > 1]

        result['interrupts'] = next(int(l[1]) for l in os_counters if l[0] == 'intr')
        result['context_switches'] = next(int(l[1]) for l in os_counters if l[0] == 'ctxt')
        result['procs_running'] = next(int(l[1]) for l in os_counters if l[0] == 'procs_running')
        result['procs_blocked'] = next(int(l[1]) for l in os_counters if l[0] == 'procs_blocked')
        result['procs_created'] = next(int(l[1]) for l in os_counters if l[0] == 'processes')

        with open('/proc/loadavg', 'r') as f:
            loadavg = f.readlines()

        loadavg = map(lambda x: float(x), loadavg[0].split()[:3])

        result['loadavg_1min'] = loadavg[0]
        result['loadavg_5min'] = loadavg[1]
        result['loadavg_15min'] = loadavg[2]

        return result

    def Storage(self):
        result = {}

        if self.system != 'Linux':
            return None

        # FIXME: Collect information for all tablespaces and pg_xlog

        data_directory = db.run_query('SHOW data_directory')[0]['data_directory']

        result['name'] = 'PGDATA directory'
        result['path'] = data_directory
        result['mountpoint'] = self._find_mount_point(data_directory)

        vfs_stats = os.statvfs(data_directory)

        result['bytes_total'] = vfs_stats.f_bsize * vfs_stats.f_blocks
        result['bytes_available'] = vfs_stats.f_bsize * vfs_stats.f_bavail

        devicenode = os.stat(data_directory).st_dev
        major = os.major(devicenode)
        minor = os.minor(devicenode)

        sysfs_device_path = "/sys/dev/block/%d:%d/" % (major, minor)

        # not all devices have stats
        if os.path.exists(sysfs_device_path + 'stat'):
            with open(sysfs_device_path + 'stat', 'r') as f:
                device_stats = map(int, f.readline().split())

            stat_fields = ['rd_ios', 'rd_merges', 'rd_sectors', 'rd_ticks',
                           'wr_ios', 'wr_merges', 'wr_sectors', 'wr_ticks',
                           'ios_in_prog', 'tot_ticks', 'rq_ticks']

            result['perfdata'] = dict(zip(stat_fields, device_stats))

        # Vendor/Model doesn't exist for metadevices
        if os.path.exists(sysfs_device_path + 'device/vendor'):
            with open(sysfs_device_path + 'device/vendor', 'r') as f:
                vendor = f.readline().strip()

            with open(sysfs_device_path + 'device/model', 'r') as f:
                model = f.readline().strip()

            result['hardware'] = " ".join(vendor, model)

        return ([result])

    def Memory(self):
        result = {}

        if self.system != 'Linux': return None

        with open('/proc/meminfo') as f:
            meminfo = f.readlines()

        # Strip whitespace, drop kb suffix, split into two elements
        meminfo = dict(map(lambda x: " ".join(x.split()[:2]).split(': '), meminfo))

        # Initialize missing fields (openvz et al), convert to bytes
        for k in ['MemTotal', 'MemFree', 'Buffers', 'Cached', 'SwapTotal', 'SwapFree', 'Dirty', 'Writeback']:
            if not meminfo.get(k):
                meminfo[k] = 0
            else:
                meminfo[k] = int(meminfo[k]) * 1024

        result['total_bytes'] = meminfo['MemTotal']
        result['buffers_bytes'] = meminfo['Buffers']
        result['pagecache_bytes'] = meminfo['Cached']
        result['free_bytes'] = meminfo['MemFree']
        result['applications_bytes'] = meminfo['MemTotal'] - meminfo['MemFree'] - meminfo['Buffers'] - meminfo['Cached']
        result['dirty_bytes'] = meminfo['Dirty']
        result['writeback_bytes'] = meminfo['Writeback']
        result['swap_total_bytes'] = meminfo['SwapTotal']
        result['swap_free_bytes'] = meminfo['SwapFree']

        return result

    def _find_mount_point(self, path):
        path = os.path.abspath(path)
        while not os.path.ismount(path):
            path = os.path.dirname(path)
        return path


class DB():
    querymarker = '/* ' + MYNAME + ' */'

    def __init__(self, dbname, username=None, password=None, host=None, port=None):
        self.conn = self._connect(dbname, username, password, host, port)
        logger.debug("Connected to database")

        # Convert decimal values to float since JSON can't handle Decimals
        if pg.__name__ == 'pg8000':
            self._pg8000_numeric_in = self.conn.pg_types[1700][1]
            self.conn.pg_types[1700] = (pg.core.FC_TEXT, self._pg8000_float_numeric_wrapper)

        if pg.__name__ == 'psycopg2':
            dec2float = pg.extensions.new_type(
                pg.extensions.DECIMAL.values,
                'DEC2FLOAT',
                lambda value, curs: float(value) if value is not None else None)
            pg.extensions.register_type(dec2float)

    def run_query(self, query, should_raise=False):
        # pg8000 is picky regarding % characters in query strings, escaping with extreme prejudice
        if pg.__name__ == 'pg8000' and '%' in query:
            logger.debug("Escaping % characters in query string")
            query = query.replace('%', '%%')

        logger.debug("Running query: %s" % query)

        # Prepending querymarker to be able to filter own queries during subsequent runs
        query = self.querymarker + query

        cur = self.conn.cursor()

        try:
            cur.execute(query)
        except Exception as e:
            if should_raise:
                raise e
            logger.error("Got an error during query execution")
            for line in str(e).splitlines():
                logger.error(line)
            sys.exit(1)

        # Didn't get any column definition back, this is most likely a return-less command (SET et al)
        if cur.description is None:
            return []

        # Fetch column headers
        columns = [f[0] for f in cur.description]

        # Build list of hashes
        result = [dict(zip(columns, row)) for row in cur.fetchall()]
        return result

    def rollback(self):
        self.conn.rollback()

    def _pg8000_float_numeric_wrapper(self, data, offset, length):
        return float(self._pg8000_numeric_in(data, offset, length))

    def _connect(self, dbname, username, password, host, port):
        try:
            kw = {
                'database': dbname,
                'user': username,
                'password': password,
                'host': host,
                'port': port,
            }
            # psycopg2 <= 2.4.2 fails if you pass None arguments, filter them out by hand.
            kw = dict((key, value) for key, value in kw.iteritems() if value is not None)
            # pg8000 expects port to be of type integer
            if 'port' in kw:
                kw['port'] = int(kw['port'])
            logger.debug("Connecting to database, using driver %s, parameters: %s" % (pg.__name__, kw))
            return pg.connect(**kw)
        except Exception as e:
            logger.error("Failed to connect to database: %s", str(e))
            sys.exit(1)


def find_executable_in_path(cmd):
    for path in os.environ['PATH'].split(os.pathsep):
        test = "%s/%s" % (path, cmd)
        logger.debug("Testing %s" % test)
        if os.path.isfile(test) and os.access(test, os.X_OK):
            return test
    return None


def check_database():
    global db
    db = DB(host=db_host, port=db_port, username=db_username, password=db_password, dbname=db_name)

    if not ON_HEROKU and not db.run_query('SHOW is_superuser')[0]['is_superuser'] == 'on':
        logger.error("User %s isn't a superuser" % db_username)
        sys.exit(1)

    if not db.run_query('SHOW server_version_num')[0]['server_version_num'] >= 90100:
        logger.error("You must be running PostgreSQL 9.1 or newer")
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


def read_heroku_config():
    logger.debug("Reading heroku-style config from environment DATABASE_URL")
    urlparse.uses_netloc.append('postgres')
    url = urlparse.urlparse(os.environ['DATABASE_URL'])
    
    global db_host, db_port, db_username, db_password, db_name, api_key, api_url
    db_username = url.username
    db_password = url.password
    db_host = url.hostname
    db_port = url.port
    db_name = url.path[1:]
    
    api_key = os.environ['PGANALYZE_APIKEY']
    api_url = API_URL


def read_config():
    logger.debug("Reading config")

    configfile = None
    for file in option['configfile']:
        try:
            mode = os.stat(file).st_mode
        except Exception as e:
            logger.debug("Couldn't stat file: %s" % e)
            continue

        if not S_ISREG(mode):
            logger.debug("%s isn't a regular file" % file)
            continue

        if int(oct(mode)[-2:]) != 0:
            logger.error("Configfile is accessible by other users, please run `chmod go-rwx %s`" % file)
            sys.exit(1)

        if not os.access(file, os.R_OK):
            logger.debug("%s isn't readable" % file)
            continue

        configfile = file
        break

    if not configfile:
        logger.error("Couldn't find a readable config file, perhaps create one with --generate-config?")
        sys.exit(1)

    configparser = ConfigParser.RawConfigParser()

    try:
        configparser.read(configfile)
    except Exception as e:
        logger.error(
            "Failure while parsing %s: %s, please fix or create a new one with --generate-config" % (configfile, e))
        sys.exit(1)

    configdump = {}
    logger.debug("read config from %s" % configfile)
    for k, v in configparser.items('pganalyze'):
        configdump[k] = v
        # Don't print the password to debug output
        if k == 'db_password': v = '***removed***'
        logger.debug("%s => %s" % (k, v))

    # FIXME: Could do with a dict
    global db_host, db_port, db_username, db_password, db_name, api_key, api_url
    db_username = configdump.get('db_username')
    db_password = configdump.get('db_password')
    db_host = configdump.get('db_host')
    # Set db_host to localhost if not specified and db_password present to force non-unixsocket-connection
    if not db_host and db_password:
        db_host = 'localhost'
    db_port = configdump.get('db_port')
    db_name = configdump.get('db_name')
    api_key = configdump.get('api_key')
    api_url = configdump.get('api_url', API_URL)

    if not db_name and api_key:
        logger.error(
            "Missing database name and/or api key in configfile %s, perhaps create one with --generate-config?" % configfile)
        sys.exit(1)


def write_config():

    apikey = option['apikey'] if option['apikey'] is not None else 'fill_me_in'

    sample_config = '''[pganalyze]
api_key: %s
db_name: fill_me_in
#db_username:
#db_password:
#db_host: localhost
#db_port: 5432
#api_url: %s
''' % (apikey, API_URL)

    cf = option['configfile'][0]

    try:
        f = os.open(cf, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0600)
        os.write(f, sample_config)
        os.close(f)
    except Exception as e:
        logger.error("Failed to write configfile: %s" % e)
        sys.exit(1)
    logger.info("Wrote standard configuration to %s, please edit it and then run the script again" % cf)


def fetch_system_information():
    SI = SystemInformation()
    info = {}

    info['os'] = SI.OS()
    info['cpu'] = SI.CPU()
    info['scheduler'] = SI.Scheduler()
    info['storage'] = SI.Storage()
    info['memory'] = SI.Memory()

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
    for row in PI.IndexStats():
        del row['table']
        indexkey = '.'.join([row.pop('schema'), row.pop('index')])
        indexstats[indexkey] = row

    for row in PI.TableStats():
        tablekey = '.'.join([row.pop('schema'), row.pop('table')])
        tablestats[tablekey] = row

    # Merge Table & Index bloat information into table/indexstats dicts
    for row in PI.Bloat():
        tablekey = '.'.join([row.get('schemaname'), row.pop('tablename')])
        indexkey = '.'.join([row.pop('schemaname'), row.pop('iname')])
        if tablekey in tablestats:
            tablestats[tablekey]['wasted_bytes'] = row['wastedbytes']
        if indexkey in indexstats:
            indexstats[indexkey]['wasted_bytes'] = row['wastedibytes']

    # Combine Table, Index and Constraint information into a combined schema dict
    for row in PI.Columns():
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

    for row in PI.Indexes():
        statskey = '.'.join([row['schema'], row['name']])
        tablekey = '.'.join([row.pop('schema'), row.pop('table')])

        #Merge index stats
        row = dict(row.items() + indexstats[statskey].items())

        if not 'indices' in schema[tablekey]:
            schema[tablekey]['indices'] = []
        schema[tablekey]['indices'].append(row)

    for row in PI.Constraints():
        tablekey = '.'.join([row.pop('schema'), row.pop('table')])
        if not 'constraints' in schema[tablekey]:
            schema[tablekey]['constraints'] = []
        schema[tablekey]['constraints'].append(row)


    # Populate result dictionary
    info['schema']   = schema.values()
    info['version']  = PI.Version()
    info['settings'] = PI.Settings()
    info['bgwriter'] = PI.BGWriterStats()
    info['database'] = PI.DBStats()
    info['locks']    = PI.Locks()
    info['backends'] = PI.Backends(option['queryparameters'])

    return info


class DatetimeEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return str(obj)

        return json.JSONEncoder.default(self, obj)


def post_data_to_web(data):
    to_post = {}
    to_post['data'] = json.dumps(data, cls=DatetimeEncoder)
    to_post['api_key'] = api_key
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
            res = urllib.urlopen(api_url, urllib.urlencode(to_post))
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
        return ['pg_stat_plans', PgStatPlans().fetch_queries()]
    elif 'pg_stat_statements' in extensions:
        logger.debug("Found pg_stat_statements, using it for query information")
        return ['pg_stat_statements', PgStatStatements().fetch_queries()]
    else:
        logger.error("Couldn't find either pg_stat_plans or pg_stat_statements, aborting")
        sys.exit(1)


def main():
    global option, logger

    option = parse_options()
    logger = configure_logger()

    if option['generate_config']:
        write_config()
        sys.exit(0)
    
    if ON_HEROKU:
        read_heroku_config()
        option['systeminformation'] = False
    else:
        read_config()

    check_database()

    data = {}
    (option['query_source'], data['queries']) = fetch_query_information()

    if option['systeminformation']:
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


if __name__ == '__main__': main()
