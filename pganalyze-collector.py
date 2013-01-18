#!/usr/bin/env python

from optparse import OptionParser
from pprint import pprint
import logging
import os
import re
import configparser


API_URL = 'http://pganalyze.com/queries'
RESET_STATS = True

MYNAME = 'pganalyze-collector'
VERSION = '0.0.1-dev'


def parse_options(print_help=False):
	parser = OptionParser(usage="%s [options]" % MYNAME, version="%s %s" % (MYNAME, VERSION))

	parser.add_option('-v', '--verbose', action='store_true', dest='verbose',
			help='Print verbose debug information')
	parser.add_option('--config', action='store', type='string', dest='configfile',
			default='$HOME/.pganalyze_collector.conf, /etc/pganalyze/collector.conf',
			help='Specifiy alternative path for config file. Defaults: %default')
	parser.add_option('--generate-config', action='callback', callback=write_config,
			help='Writes a default configuration file to $HOME/.pganalyze_collector.conf unless specified otherwise with --config')
	parser.add_option('--cron', '--quiet', action='store_true', dest='quiet',
			help='Suppress all non-warning output during normal operation')

	if print_help:
		parser.print_help()
		return

	(options, args) = parser.parse_args()

	options = options.__dict__

	options['configfile'] = re.split(',\s+', options['configfile'].replace('$HOME', os.environ['HOME']))
	pprint(options)
	return options


def configure_logger():
	logtemp = logging.getLogger(MYNAME)

	if config['verbose']:
		level = logging.DEBUG
	else:
		level = logging.INFO


	lh = logging.StreamHandler()
	format = '%(asctime)s %(message)s'
	lf = logging.Formatter(format)
	lh.setFormatter(lf)
	logtemp.addHandler(lh)

	return logtemp

def read_config
	# Return the first readable config file

	configfile = None
	for file in config['configfile']:
		stat = None

		mode = os.stat(file).st_mode

		if not mode:
			logger.debug "#{f} doesn't exist"
			next

		if not S_ISREG(mode):
			logger.debug "#{f} isn't a regular file"
			next

		configfile = f
		break
	}

	if not configfile:
		logger.error "Couldn't find a config file, perhaps create one with --generate-config?"
		exit 1

	configdump = {}

		File.open(configfile) { |yf| configdump = YAML::load(yf) }
	rescue ArgumentError
		$logger.error "Failure while parsing #{configfile}, please fix or create a new one with --generate-config" 
		exit 1
	end

	logger.debug "read config from %s" % configfile
	configdump.each { |k, v|
		$logger.debug "#{k} => #{v}"
	}

	global db_host, db_port, db_username, db_password, db_name, api_key, psql_binary
	db_host = configdump['db_host']
	db_port = configdump['db_port']
	db_username = configdump['db_username']
	db_password = configdump['db_password']
	db_name = configdump['db_name']
	api_key = configdump['api_key']
	psql_binary = configdump['psql_binary']

	if not db_name and api_key:
		logger.error "Missing database name and/or api key in configfile #{configfile}, perhaps create one with --generate-config?"
		exit 1


def fetch_queries():
	print "Fetching queries"

def post_data_to_web(queries):
	print "Posting data to web"

def check_database():
	print "Checking database"

def write_config():
	print "I've written the config!"

def main():
	global config, logger

	config = parse_options()
	logger = configure_logger()

	print "OMGHI2U2" + VERSION
	exit

	if not db_name and api_key: 
		read_config

	check_database

	queries = fetch_queries

	# FIXME: Verbose error reporting for wrong API key/broken data/etc?
	if post_data_to_web(queries):
		if not quiet:
			print "Submitted"
			#logger.info "Submitted successfully"

		if RESET_STATS:
			print "Resetting stats"
			#logger.debug "Resetting stats!"
			#db.exec("SELECT pg_stat_plans_reset()")
		else:
			print "Rejected"
			#logger.error "Rejected by server"


if __name__ == '__main__': main()

#require 'rubygems'
#require 'json'
#
#require 'net/http'
#require 'getoptlong'
#require 'yaml'
#require 'logger'
#
#require 'pp'
#

#
#class PSQL
#	def initialize(*args)
#		(@host, @port, @username, @password, @dbname, @psql) =  args[0].values_at(:host, :port, :username, :password, :dbname, :psql)
#		@host ||= 'localhost'
#		@port ||= 5432
#		@psql ||= find_psql
#		raise "Please specify a database" unless @dbname
#		raise "Please specify path to psql binary" unless @psql
#		
#		# Setting up ENV for psql
#		ENV['PGUSER'] = @username
#		ENV['PGPASSWORD'] = @password
#		ENV['PGHOST'] = @host
#		ENV['PGPORT'] = @port.to_s
#		ENV['PGDATABASE'] = @dbname
#	end
#
#	def exec(query, should_raise = false, ignore_noncrit = false)
#		# FIXME: ruby1.8 popen only supports command strings, not arrays
#
#		$logger.debug "Running query: #{query}"
#
#		err_rd, err_wr = IO.pipe
#		cmd = [@psql, "-F\u2764", '--no-align', '--no-password', '--no-psqlrc', "-c", query, :err => err_wr]
#		lines = []
#
#		IO.popen(cmd, :err => err_wr) { |f| lines = f.readlines }
#
#		begin
#			stderr = err_rd.read_nonblock(16384).split(/\n/)
#		rescue Errno::EAGAIN
#			# Fall through if stderr is empty
#		end
#
#		# Fail on all invocations where exitstatus is non-null
#		# When exitstatus is null, we might have only encountered notices or warnings which might be expected.
#		if $?.exitstatus != 0 or (stderr and ignore_noncrit == false)
#			if should_raise
#				raise RuntimeError, stderr
#			end
#			$logger.error "Got an error during query execution, exitstatus: #{$?.exitstatus}:"
#			stderr.each { |l| $logger.error l }
#			exit 1
#		end
#
#		# If we've got anything left in stderr it's probably warning/notices. Dump them to debug
#                if stderr
#                        $logger.debug "Encountered warnings/notices:"
#                        stderr.each { |l| $logger.debug l }
#		end
#
#		# Drop number of rows
#		lines.pop
#		# Fetch column headers
#		columns = lines.shift.strip.split(/\u2764/)
#
#		resultset = []
#		lines.each { |line|
#		  values = line.strip.split(/\u2764/)
#			resultset.push(Hash[columns.zip(values)])
#		}
#
#		return resultset
#	end
#
#	def ping
#		$logger.debug "Pinging database"
#		exec('SELECT 1')
#	end
#
#	private
#	def find_psql
#		cmd = 'psql'
#		ENV['PATH'].split(File::PATH_SEPARATOR).each do |path|
#			test = "#{path}/#{cmd}"
#			return test if File.executable? test
#		end
#		return nil
#	end
#end
#
#
#def post_data_to_web(queries)
#	to_post = {}
#	to_post['data'] = {'queries' => queries}.to_json
#	to_post['api_key'] = $api_key
#	to_post['collected_at'] = Time.now.to_i
#
#	begin
#		res = Net::HTTP.post_form(URI.parse(API_URL), to_post)
#		return res.code == '200'
#	rescue => e
#		$logger.error "Failed to post data to service: #{e.message}"
#	end
#end
#
#def fetch_queries
#	both_fields = ["userid", "dbid",
#		"calls", "rows", "total_time",
#		"shared_blks_hit", "shared_blks_read", "shared_blks_written",
#		"local_blks_hit", "local_blks_written",
#		"temp_blks_read", "temp_blks_written"]
#
#	query_fields = ["plan_ids", "calls_per_plan", "avg_time_per_plan",
#		"time_variance", "time_stddev"] + both_fields
#
#	plan_fields = ["planid", "had_our_search_path", "from_our_database",
#		"query_explainable", "last_startup_cost", "last_total_cost"] + both_fields
#
#	query = "SET pg_stat_plans.explain_format TO JSON;"
#	query += "SELECT replace(pg_stat_plans_explain(p.planid, p.userid, p.dbid), chr(10), ' ') AS p_explain"
#	query += ", replace(pq.normalized_query, chr(10), ' ') AS pq_normalized_query"
#	query += ", replace(p.query, chr(10), ' ') AS p_query"
#	query += ", " + query_fields.map {|f| "pq.#{f} AS pq_#{f}"}.join(", ")
#	query += ", " + plan_fields.map {|f| "p.#{f} AS p_#{f}"}.join(", ")
#	query += " FROM pg_stat_plans p"
#	query += " LEFT JOIN pg_stat_plans_queries pq ON p.planid = ANY (pq.plan_ids)"
#	# EXPLAIN, COPY and SET commands cannot be explained
#	query += " WHERE p.query !~* '^\\s*(EXPLAIN|COPY|SET)'"
#	# Plans in pg_catalog cannot be explained
#	query += " AND p.query !~* '\\spg_catalog\\.'"
#	# We don't want our stuff in the statistics
#	query += " AND p.query !~* '\\spg_stat_plans\\s'"
#	# Remove all plans which we can't explain
#	query += " AND p.from_our_database = TRUE AND p.query_explainable = TRUE"
#	query += " AND p.planid = ANY (pq.plan_ids);"
#
#	queries = {}
#	$db.exec(query, false, true).each do |row|
#		query = {}; row.select {|k,| k[/^pq_/] }.each {|k,v| query[k.gsub("pq_", "")] = v }
#		key = query['normalized_query']
#		queries[key] ||= query
#
#		plan = {}; row.select {|k,| k[/^p_/] }.each {|k,v| plan[k.gsub("p_", "")] = v }
#		queries[key]['plans'] ||= []
#		queries[key]['plans'] << plan
#	end
#	queries.values
#end
#
#def check_database
#	$db = PSQL.new(:host => $db_host,
#		       :port => $db_port,
#		       :username => $db_username,
#		       :password => $db_password,
#		       :dbname => $db_name)
#
#	unless $db.ping
#		$logger.error "Can't run query against the database"
#		exit 1
#	end
#	
#	unless $db.exec('SHOW is_superuser')[0]['is_superuser'] == 'on'
#		$logger.error "User #{$db_username} isn't a superuser"
#		exit 1
#	end
#
#	unless $db.exec('SHOW server_version_num')[0]['server_version_num'].to_i >= 90100
#		$logger.error "You must be running PostgreSQL 9.1 or newer"
#		exit 1
#	end
#
#	begin
#		unless $db.exec("SELECT COUNT(*) as foo FROM pg_extension WHERE extname='pg_stat_plans'", true)[0]['foo'] == '1'
#			$logger.error "Extension pg_stat_plans isn't installed"
#			exit 1
#		end
#	rescue
#		$logger.error "Table pg_extension doesn't exist - this shouldn't happen!"
#		exit 1
#	end
#end
#
#
#def write_config
#
#	config = <<'EOF'
#api_key: fill_me_in
#db_name: fill_me_in
##db_username:
##db_password:
##db_host: localhost
##db_port: 5432
##psql_binary: /autodetected/from/$PATH
#EOF
#
#
#	cf = $configfile[0]
#
#	begin
#		File.open(cf, File::WRONLY|File::CREAT|File::EXCL) { |f|
#			f.write(config)
#		}
#	rescue => e
#		$logger.error "Failed to write configfile: #{e.message}"
#		exit 1
#	end
#	$logger.info "Wrote standard configuration to #{cf}, please edit it and then run the script again"
#end
#

