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


import os, sys, subprocess
import time, calendar
import re, json
import urllib
import logging
import ConfigParser
from optparse import OptionParser
from stat import *
import platform
from pprint import pprint


API_URL = 'https://pganalyze.com/queries'

MYNAME = 'pganalyze-collector'
VERSION = '0.2.1-dev'


class PostgresInformation():

	def __init__(self):
		return


	def Columns(self):
		query = """
SELECT n.nspname AS schema,
       c.relname AS table,
       pg_catalog.pg_table_size(c.oid) AS tablesize,
       a.attname AS columname,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS columntype,
  (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
   FROM pg_catalog.pg_attrdef d
   WHERE d.adrelid = a.attrelid
     AND d.adnum = a.attnum
     AND a.atthasdef) AS defaultvalue,
       a.attnotnull AS NOTNULL,
       a.attnum AS columnposition
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid
WHERE c.relkind = 'r'
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY n.nspname,
         c.relname,
         a.attnum;
"""
		#FIXME: toast handling, table inheritance

		result = db.run_query(query)
		return(result)



	def Indexes(self):
		query = """
SELECT n.nspname AS schema,
       c.relname AS table,
       i.indkey AS indexedcolumns,
       c2.relname AS indexname,
       pg_relation_size(c2.oid) AS size_bytes,
       i.indisprimary,
       i.indisunique,
       i.indisvalid,
       pg_catalog.pg_get_indexdef(i.indexrelid, 0, TRUE),
       pg_catalog.pg_get_constraintdef(con.oid, TRUE)
FROM pg_catalog.pg_class c,
     pg_catalog.pg_class c2,
     pg_catalog.pg_namespace n,
     pg_catalog.pg_index i
LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid
                                           AND conindid = i.indexrelid
                                           AND contype IN ('p', 'u', 'x'))
WHERE c.relkind = 'r'
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
  AND c.oid = i.indrelid
  AND i.indexrelid = c2.oid
  AND n.oid = c.relnamespace
ORDER BY n.nspname,
         c.relname,
         i.indisprimary DESC,
         i.indisunique DESC,
         c2.relname;
"""
		#FIXME: column references for index expressions

		result = db.run_query(query)
		for row in result:
			row['indexedcolumns'] = map(int, str(row['indexedcolumns']).split())
		return(result)


	def Constraints(self):
		query = """
SELECT n.nspname AS SCHEMA,
       c.relname AS TABLE,
       conname AS constraintname,
       pg_catalog.pg_get_constraintdef(r.oid, TRUE) AS constraintdef,
       r.conkey AS localcolumns,
       n2.nspname AS foreignschema,
       c2.relname AS foreigntable,
       r.confkey AS foreigncolumns
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_constraint r ON r.conrelid = c.oid
LEFT JOIN pg_catalog.pg_class c2 ON r.confrelid = c2.oid
LEFT JOIN pg_catalog.pg_namespace n2 ON n2.oid = c2.relnamespace
WHERE r.contype = 'f'
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY n.nspname,
         c.relname,
         constraintname;
"""
		#FIXME: This probably misses check constraints and others?
		result = db.run_query(query)
		for row in result:
			row['foreigncolumns'] = map(int, row['foreigncolumns'].strip('{}').split(','))
			row['localcolumns'] = map(int, row['localcolumns'].strip('{}').split(','))
		return(result)

	def Triggers(self):

		#FIXME: Fix query
		query = """
SELECT t.tgname, pg_catalog.pg_get_triggerdef(t.oid, true), t.tgenabled
        FROM pg_catalog.pg_trigger t
        WHERE t.tgrelid = '16795' AND NOT t.tgisinternal
        ORDER BY 1
"""


class SystemInformation():

	def __init__(self):
		self.system = platform.system()
		if self.system != 'Linux':
			raise Exception("Unsupported system: %s" % self.system)

	def OS(self):
		os = {}
		os['system'] = platform.system()
		if self.system == 'Linux':
			(os['distribution'], os['distribution_version']) = platform.linux_distribution()[0:2]
		elif self.system == 'Darwin':
			os['distribution'] = 'OS X'
			os['distribution_version'] = platform.mac_ver()[0]

		os['architecture'] = platform.machine()
		os['kernel_version'] = platform.release()

		# This only works when run as root - maybe drop again?
		dmidecode = find_executable_in_path('dmidecode')
		if dmidecode:
			try:
				vendor = subprocess.check_output([dmidecode, '-s', 'system-manufacturer']).strip()
				model = subprocess.check_output([dmidecode, '-s', 'system-product-name']).strip()
				if vendor and model:
					os['server_model'] = "%s %s" % (vendor, model)


			except Exception as e:
				logger.debug("Error while collecting system manufacturer/model via dmidecode: %s" % e)

		return os


	def CPU(self):
		result = {}
		if self.system != 'Linux': return None

		with open('/proc/stat', 'r') as f:
			procstat = f.readlines()

		# Fetch combined CPU counter from lines
		os_counters = filter(lambda x: x.find('cpu ') == 0, procstat)[0]

		# tokenize, strip row heading
		os_counters = os_counters.split()[1:]

		# Correct all values to msec
		kernel_hz = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
		os_counters = map(lambda x: int(x) * (1000 / kernel_hz), os_counters)

		os_counter_names = [ 'user_msec', 'nice_msec', 'system_msec', 'idle_msec', 'iowait_msec',
				     'irq_msec', 'softirq_msec', 'steal_msec', 'guest_msec', 'guest_nice_msec']

		result['busy_times'] = dict(zip(os_counter_names, os_counters))


		with open('/proc/cpuinfo', 'r') as f:
			cpuinfo = f.readlines()

		# Trim excessive whitespace in strings, return two elements per line
		cpuinfo = map(lambda x: " ".join(x.split()).split(' : '), cpuinfo)

		hardware = {}
		hardware['model'] = next(l[1] for l in cpuinfo if l[0] == 'model name')
		hardware['cache_size'] = next(l[1] for l in cpuinfo if l[0] == 'cache size')
		hardware['speed_mhz'] = next(round(float(l[1]),2) for l in cpuinfo if l[0] == 'cpu MHz')
		hardware['sockets'] = int(max([l[1] for l in cpuinfo if l[0] == 'physical id'])) + 1
		hardware['cores_per_socket'] = next(int(l[1]) for l in cpuinfo if l[0] == 'cpu cores')

		result['hardware'] = hardware


		return(result)


	def Scheduler(self):
		result = {}

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

		return(result)



	def Storage(self):
		result = {}

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


			stat_fields = [ 'rd_ios', 'rd_merges', 'rd_sectors', 'rd_ticks',
					'wr_ios', 'wr_merges', 'wr_sectors', 'wr_ticks',
					'ios_in_prog', 'tot_ticks', 'rq_ticks' ]

			result['perfdata'] = dict(zip(stat_fields, device_stats))

		# Vendor/Model doesn't exist for metadevices
		if os.path.exists(sysfs_device_path + 'device/vendor'):
			with open(sysfs_device_path + 'device/vendor', 'r') as f:
				vendor = f.readline().trim()

			with open(sysfs_device_path + 'device/model', 'r') as f:
				model = f.readline().trim()

			result['hardware'] = " ".join(vendor, model)

		return([result])
	
	def Memory(self):
		result = {}
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

		return(result)

	def _find_mount_point(self, path):
		path = os.path.abspath(path)
		while not os.path.ismount(path):
			path = os.path.dirname(path)
		return path


class PSQL():
	def __init__(self, dbname, username=None, password=None, psql=None, host=None, port=None):
		self.psql = psql or self._find_psql()

		if not self.psql:
			raise Exception('Please specify path to psql binary')

		logger.debug("Using %s as psql binary" % self.psql)
		
		# Setting up environment for psql
		logger.debug("Setting PGDATABASE to %s" % dbname)
		os.environ['PGDATABASE'] = dbname
		if username:
			os.environ['PGUSER'] = username
			logger.debug("Setting PGUSER to %s" % username)
		if password:
			os.environ['PGPASSWORD'] = password
			logger.debug("Setting PGPASSWORD")
		if host:
			os.environ['PGHOST'] = host
			logger.debug("Setting PGHOST to %s" % host)
		if port:
			os.environ['PGPORT'] = port
			logger.debug("Setting PGPORT to %s" % port)

	def run_query(self, query, should_raise=False, ignore_noncrit=False):

		logger.debug("Running query: %s" % query)

		colsep = unichr(0x2764)

		cmd = [self.psql, "-F" + colsep.encode('utf-8'), '--no-align', '--no-password', '--no-psqlrc']
		lines = []

		p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

		(stdout, stderr) = p.communicate(query)


		# Fail on all invocations where exitstatus is non-zero
		# When exitstatus is zero, we might have only encountered notices or warnings which might be expected.
		if p.returncode != 0 or (stderr and ignore_noncrit == False):
			if should_raise:
				raise Exception(stderr)
			logger.error("Got an error during query execution, exitstatus: %s:" % p.returncode)
			for line in stderr.splitlines():
				logger.error(line)
			sys.exit(1)

		# If we've got anything left in stderr it's probably warning/notices. Dump them to debug
		if stderr:
                        logger.debug("Encountered warnings/notices:")
			for line in stderr.splitlines():
				logger.debug(line)

		stdout = stdout.decode('utf-8')
		lines = stdout.splitlines()

		# Drop number of rows
		lines.pop()
		# FIXME: Skip first row if it's from a SET statement
		if lines[0] == 'SET':
			lines.pop(0)
		# Fetch column headers
		columns = lines.pop(0).strip().split(colsep)

		resultset = []
		for line in lines:
			values = line.strip().split(colsep)
			values = self._magic_cast(values)
			resultset.append(dict(zip(columns, values)))

		return resultset

	def ping(self):
		logger.debug("Pinging database")
		self.run_query('SELECT 1')
		return True

	def _find_psql(self):
		logger.debug("Searching for PSQL binary")
		return find_executable_in_path('psql')

	def _magic_cast(self, values):
		nicevalues = []
		for value in values:
			try:
				nicevalues.append(int(value))
				continue
			except Exception as e:
				pass

			if value == 't':
				nicevalues.append(True)
				continue

			if value == 'f':
				nicevalues.append(False)
				continue
			
			nicevalues.append(value)
		return(nicevalues)


def find_executable_in_path(cmd):
	for path in os.environ['PATH'].split(os.pathsep):
		test = "%s/%s" % (path, cmd)
		logger.debug("Testing %s" % test)
		if os.path.isfile(test) and os.access(test, os.X_OK):
			return test
	return None



def check_database():
	global db
	db = PSQL(host=db_host, port=db_port, username=db_username, password=db_password, dbname=db_name)

	if not db.ping():
		logger.error("Can't run query against the database")
		sys.exit(1)
	
	if not db.run_query('SHOW is_superuser')[0]['is_superuser'] == 'on':
		logger.error("User %s isn't a superuser" % db_username)
		sys.exit(1)

	if not int(db.run_query('SHOW server_version_num')[0]['server_version_num']) >= 90100:
		logger.error("You must be running PostgreSQL 9.1 or newer")
		sys.exit(1)

	try:
		if not db.run_query("SELECT COUNT(*) as foo FROM pg_extension WHERE extname='pg_stat_plans'", True)[0]['foo'] == 1:
			logger.error("Extension pg_stat_plans isn't installed")
			sys.exit(1)
	except Exception as e:
		logger.error("Table pg_extension doesn't exist - this shouldn't happen")
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
	parser.add_option('--cron', '-q', action='store_true', dest='quiet',
			help='Suppress all non-warning output during normal operation')
	parser.add_option('--dry-run', '-d', action='store_true', dest='dryrun',
			help='Print data that would get sent to web service and exit afterwards.')
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

	if option['verbose']:
		logtemp.setLevel(logging.DEBUG)
	else:
		logtemp.setLevel(logging.INFO)

	lh = logging.StreamHandler()
	format = '%(levelname)s - %(asctime)s %(message)s'
	lf = logging.Formatter(format)
	lh.setFormatter(lf)
	logtemp.addHandler(lh)

	return logtemp

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
		logger.error("Failure while parsing %s: %s, please fix or create a new one with --generate-config" % (configfile, e))
		sys.exit(1)

	configdump = {}
	logger.debug("read config from %s" % configfile)
	for k, v in configparser.items('pganalyze'):
		configdump[k] = v
		# Don't print the password to debug output
		if k == 'db_password': v = '***removed***'
		logger.debug("%s => %s" % (k, v))

	# FIXME: Could do with a dict
	global db_host, db_port, db_username, db_password, db_name, api_key, psql_binary
	db_username = configdump.get('db_username')
	db_password = configdump.get('db_password')
	db_host = configdump.get('db_host')
	# Set db_host to localhost if not specified and db_password present to force IP connection
	if not db_host and db_password:
		db_host = 'localhost'
	db_port = configdump.get('db_port')
	db_name = configdump.get('db_name')
	api_key = configdump.get('api_key')
	psql_binary = configdump.get('psql_binary')


	if not db_name and api_key:
		logger.error("Missing database name and/or api key in configfile %s, perhaps create one with --generate-config?" % configfile)
		sys.exit(1)


def fetch_queries():
	both_fields = ["userid", "dbid",
		"calls", "rows", "total_time",
		"shared_blks_hit", "shared_blks_read", "shared_blks_written",
		"local_blks_hit", "local_blks_written",
		"temp_blks_read", "temp_blks_written"]

	query_fields = ["planids", "calls_per_plan", "avg_time_per_plan",
		"time_variance", "time_stddev"] + both_fields

	plan_fields = ["planid", "had_our_search_path", "from_our_database",
		"query_explainable", "last_startup_cost", "last_total_cost"] + both_fields

	query = "SELECT translate(pq.normalized_query, chr(10) || chr(13), '  ') AS pq_normalized_query"
	query += ", translate(p.query, chr(10) || chr(13), '  ') AS p_query"
	query += ", " + ", ".join(map(lambda s: "pq.%s AS pq_%s" % (s, s), query_fields))
	query += ", " + ", ".join(map(lambda s: "p.%s AS p_%s" % (s, s), plan_fields))
	query += " FROM pg_stat_plans p"
	query += " LEFT JOIN pg_stat_plans_queries pq ON p.planid = ANY (pq.planids)"
	# EXPLAIN, COPY and SET commands cannot be explained
	query += " WHERE p.query !~* '^\\s*(EXPLAIN|COPY|SET)'"
	# Plans in pg_catalog cannot be explained
	query += " AND p.query !~* '\\spg_catalog\\.'"
	# We don't want our stuff in the statistics
	query += " AND p.query !~* '\\spg_stat_plans\\s'"
	# Remove all plans which we can't explain
	query += " AND p.from_our_database = TRUE"
	query += " AND p.planid = ANY (pq.planids);"

	fetch_plan = "SET pg_stat_plans.explain_format TO JSON; "
	fetch_plan += "SELECT translate(pg_stat_plans_explain(%s, %s, %s), chr(10) || chr(13), '  ') AS explain"

	queries = {}

	# Fetch joined list of all queries and plans
	for row in db.run_query(query, False, True):

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
			del(plan['query'])

		# initialize plans array
		if 'plans' not in queries[normalized_query]:
			queries[normalized_query]['plans'] = []

		# try explaining the query if pg_stat_plans thinks it's possible
		if plan['query_explainable']:
			try:
				result = db.run_query(fetch_plan % (plan['planid'], plan['userid'], plan['dbid']), True, False)
				plan['explain'] = result[0]['explain'] 
			except Exception as e:
				plan['explain_error'] = str(e)
			
		queries[normalized_query]['plans'].append(plan)

	return queries.values()


def fetch_system_information():

	SI = SystemInformation()
	info = {}

	info['os'] = SI.OS()
	info['cpu'] = SI.CPU()
	info['scheduler'] = SI.Scheduler()
	info['storage'] = SI.Storage()
	info['memory'] = SI.Memory()

	return(info)

def fetch_postgres_information():

	PI = PostgresInformation()

	info = {}
	schema = {}

	for row in PI.Columns():
		tablekey = '.'.join([row['schema'], row['table']])
		if not tablekey in schema:
			schema[tablekey] = {}
		schema[tablekey]['schema'] = row.pop('schema')
		schema[tablekey]['table'] = row.pop('table')
		schema[tablekey]['size_bytes'] = row.pop('tablesize')
		if not 'columns' in schema[tablekey]:
			schema[tablekey]['columns'] = []
		schema[tablekey]['columns'].append(row)

	for row in PI.Indexes():
		tablekey = '.'.join([row.pop('schema'), row.pop('table')])
		if not 'indexes' in schema[tablekey]:
			schema[tablekey]['indexes'] = []
		schema[tablekey]['indexes'].append(row)

	for row in PI.Constraints():
		tablekey = '.'.join([row.pop('schema'), row.pop('table')])
		if not 'constraints' in schema[tablekey]:
			schema[tablekey]['constraints'] = []
		schema[tablekey]['constraints'].append(row)

	info['schema'] = schema.values()

	return(info)


def post_data_to_web(data):
	to_post = {}
	to_post['data'] = json.dumps(data)
	to_post['api_key'] = api_key
	to_post['collected_at'] = calendar.timegm(time.gmtime())
	to_post['submitter'] = "%s %s" % (MYNAME, VERSION)
	to_post['options'] = {}
	to_post['options']['query_parameters'] = option['queryparameters']
	to_post['options']['system_information'] = option['systeminformation']

	# These will dump the Python dict with Python bools. The posted values will have json semantics (e.g. true/false/null for bools)
	if option['dryrun']:
		logger.info("Dumping data that would get posted")

		to_post['data'] = json.loads(to_post['data'])
		for query in to_post['data']['queries']:
			for plan in query['plans']:
				if 'explain' in plan:
					plan['explain'] = json.loads(plan['explain'])
		pprint(to_post)

		logger.info("Exiting.")
		sys.exit(0)


	try:
		res = urllib.urlopen(API_URL, urllib.urlencode(to_post))
		return res.read(), res.getcode()
	except Exception as e:
		logger.error("Failed to post data to service: %s" % e)
		sys.exit(1)



def write_config():

	sample_config =  '''[pganalyze]
api_key: fill_me_in
db_name: fill_me_in
#db_username:
#db_password:
#db_host: localhost
#db_port: 5432
#psql_binary: /autodetected/from/$PATH
'''

	cf = option['configfile'][0]

	try:
		f = os.open(cf, os.O_WRONLY|os.O_CREAT|os.O_EXCL, 0600)
		os.write(f, sample_config)
		os.close(f)
	except Exception as e:
		logger.error("Failed to write configfile: %s" % e)
		sys.exit(1)
	logger.info("Wrote standard configuration to %s, please edit it and then run the script again" % cf)


def main():
	global option, logger

	option = parse_options()
	logger = configure_logger()

	if option['generate_config']:
		write_config()
		sys.exit(0)

	read_config()

	check_database()

	data = {}
	data['queries'] = fetch_queries()

	if option['systeminformation']:
		data['system'] = fetch_system_information()

	data['postgres'] = fetch_postgres_information()

	num_tries = 0
	while True:
		(output, code) = post_data_to_web(data)
		num_tries = num_tries + 1
		if code == 200 or num_tries >= 3:
			break
		logger.debug("Got code %s while posting data, sleeping 60 seconds then trying again" % code)
		time.sleep(60)

	if code == 200:
		if not option['quiet']:
			logger.info("Submitted successfully")

		if not option['noreset']:
			logger.debug("Resetting stats!")
			db.run_query("SELECT pg_stat_plans_reset()")
	else:
		logger.error("Rejected by server: %s" % output)


if __name__ == '__main__': main()
