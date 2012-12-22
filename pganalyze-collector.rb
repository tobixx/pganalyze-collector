#!/usr/bin/env ruby1.9.1

require 'rubygems'
require 'json'

require 'net/http'
require 'getoptlong'
require 'yaml'
require 'logger'

require 'pp'

API_URL = 'http://pganalyze.com/queries'
RESET_STATS = true

MYNAME = 'pganalyze-collector'
VERSION = '0.0.1'

opts = GetoptLong.new(
	[ '--help', '-h', GetoptLong::NO_ARGUMENT ],
	[ '--verbose', '-v', GetoptLong::NO_ARGUMENT ],
	[ '--version', '-V', GetoptLong::NO_ARGUMENT ],
	[ '--config', GetoptLong::REQUIRED_ARGUMENT ],
	[ '--generate-config', GetoptLong::NO_ARGUMENT ],
	[ '--cron', '--quiet', GetoptLong::NO_ARGUMENT ]
)

$help = <<"BE_DRAGONS"

#{MYNAME} #{VERSION}

-h, --help
	This help

-v, --verbose
	Print verbose debug information

-V, --version
	Print version information

--config CONFIGFILE
	Specifiy alternative path for config file.
	Defaults: $HOME/.pganalyze_collector.conf, /etc/pganalyze/collector.conf

--generate-config
	Writes a default configuration file to $HOME/.pganalyze_collector.conf
	unless specified otherwise with --config

--cron, --quiet
	Suppress all non-warning output during normal operation

BE_DRAGONS

$configfile = [ ENV['HOME'] + '/.pganalyze_collector.conf', '/etc/pganalyze/collector.conf' ] 

$logger = Logger.new(STDOUT)
$logger.level = Logger::INFO

opts.each do |opt, arg|
	case opt
	when '--help'
		puts $help
		exit
	when '--verbose'
		$logger.level = Logger::DEBUG
	when '--version'
		puts "#{MYNAME} #{VERSION}"
		exit
	when '--config'
		$configfile = [arg]
	when '--cron'
		$quiet = TRUE
	when '--generate-config'
		$generate_config = TRUE
	end
end

class PSQL
	def initialize(*args)
		(@host, @port, @username, @password, @dbname, @psql) =  args[0].values_at(:host, :port, :username, :password, :dbname, :psql)
		@host ||= 'localhost'
		@port ||= 5432
		@psql ||= find_psql
		raise "Please specify a database" unless @dbname
		raise "Please specify path to psql binary" unless @psql
		
		# Setting up ENV for psql
		ENV['PGUSER'] = @username
		ENV['PGPASSWORD'] = @password
		ENV['PGHOST'] = @host
		ENV['PGPORT'] = @port.to_s
		ENV['PGDATABASE'] = @dbname
	end

	def exec(query, should_raise = false)
		# FIXME: ruby1.8 popen only supports command strings, not arrays

		$logger.debug "Running query: #{query}"

		err_rd, err_wr = IO.pipe
		cmd = [@psql, "-F\u2764", '--no-align', '--no-password', '--no-psqlrc', "-c", query, :err => err_wr]
		lines = []

		IO.popen(cmd, :err => err_wr) { |f| lines = f.readlines }

		begin
			stderr = err_rd.read_nonblock(16384).split(/\n/)
		rescue Errno::EAGAIN
			# Fall through if stderr is empty
		end

		if stderr or $?.exitstatus != 0
			if should_raise
				raise RuntimeError, stderr
			end
			$logger.error "Got an error during query execution, exitstatus: #{$?.exitstatus}:"
			stderr.each { |l| $logger.error l }
			exit
		end

		# Drop number of rows
		lines.pop
		# Fetch column headers
		columns = lines.shift.strip.split(/\u2764/)

		resultset = []
		lines.each { |line|
		  values = line.strip.split(/\u2764/)
			resultset.push(Hash[columns.zip(values)])
		}

		return resultset
	end

	def ping
		$logger.debug "Pinging database"
		exec('SELECT 1')
	end

	private
	def find_psql
		cmd = 'psql'
		ENV['PATH'].split(File::PATH_SEPARATOR).each do |path|
			test = "#{path}/#{cmd}"
			return test if File.executable? test
		end
		return nil
	end
end


def post_data_to_web(queries)
	to_post = {}
	to_post['data'] = {'queries' => queries}.to_json
	to_post['api_key'] = $api_key
	to_post['collected_at'] = Time.now.to_i

	begin
		res = Net::HTTP.post_form(URI.parse(API_URL), to_post)
		return res.code == '200'
	rescue => e
		$logger.error "Failed to post data to service: #{e.message}"
	end
end

def fetch_queries
	both_fields = ["userid", "dbid",
		"calls", "rows", "total_time",
		"shared_blks_hit", "shared_blks_read", "shared_blks_written",
		"local_blks_hit", "local_blks_written",
		"temp_blks_read", "temp_blks_written"]

	query_fields = ["plan_ids", "calls_per_plan", "avg_time_per_plan",
		"time_variance", "time_stddev"] + both_fields

	plan_fields = ["planid", "had_our_search_path", "from_our_database",
		"query_valid", "last_startup_cost", "last_total_cost"] + both_fields

	query = "SET pg_stat_plans.explain_format TO JSON;"
	query += "SELECT replace(pg_stat_plans_explain(p.planid, p.userid, p.dbid), chr(10), ' ') AS p_explain"
	query += ", replace(pq.normalized_query, chr(10), ' ') AS pq_normalized_query"
	query += ", replace(p.query, chr(10), ' ') AS p_query"
	query += ", " + query_fields.map {|f| "pq.#{f} AS pq_#{f}"}.join(", ")
	query += ", " + plan_fields.map {|f| "p.#{f} AS p_#{f}"}.join(", ")
	query += " FROM pg_stat_plans p"
	query += " LEFT JOIN pg_stat_plans_queries pq ON p.planid = ANY (pq.plan_ids)"
	# EXPLAIN, COPY and SET commands cannot be explained
	query += " WHERE p.query !~* '^\\s*(EXPLAIN|COPY|SET)'"
	# Plans in pg_catalog cannot be explained
	query += " AND p.query !~* '\\spg_catalog\\.'"
	# We don't want our stuff in the statistics
	query += " AND p.query !~* '\\spg_stat_plans\\s'"
	# Remove all plans which we can't explain
	query += " AND p.from_our_database = TRUE AND p.query_valid = TRUE"
	query += " AND p.planid = ANY (pq.plan_ids);"

	queries = {}
	$db.exec(query).each do |row|
		query = {}; row.select {|k,| k[/^pq_/] }.each {|k,v| query[k.gsub("pq_", "")] = v }
		key = query['normalized_query']
		queries[key] ||= query

		plan = {}; row.select {|k,| k[/^p_/] }.each {|k,v| plan[k.gsub("p_", "")] = v }
		queries[key]['plans'] ||= []
		queries[key]['plans'] << plan
	end
	queries.values
end

def check_database
	$db = PSQL.new(:host => $db_host,
		       :port => $db_port,
		       :username => $db_username,
		       :password => $db_password,
		       :dbname => $db_name)

	unless $db.ping
		$logger.error "DB is not alive"
		exit
	end
	
	unless $db.exec('SHOW is_superuser')[0]['is_superuser'] == 'on'
		$logger.error "User #{$db_username} isn't a superuser"
		exit
	end

	unless $db.exec('SHOW server_version_num')[0]['server_version_num'].to_i >= 90100
		$logger.error "You must be running PostgreSQL 9.1 or newer"
		exit
	end

	begin
		unless $db.exec("SELECT COUNT(*) as foo FROM pg_extension WHERE extname='pg_stat_plans'", true)[0]['foo'] == '1'
			$logger.error "Extension pg_stat_plans isn't installed"
			exit
		end
	rescue
		$logger.error "Table pg_extension doesn't exist - this shouldn't happen!"
		exit
	end
end

def read_config
	# Return the first readable config file

	configfile = nil
	$configfile.each { |f|
		stat = nil
		begin 
			stat = File.stat(f)
		rescue Errno::ENOENT
			$logger.debug "#{f} doesn't exist"
			next
		end

		unless stat.file?
			$logger.debug "#{f} isn't a regular file"
			next
		end

		unless stat.readable?
			$logger.debug "#{f} isn't readable"
			next
		end
		configfile = f
		break
	}

	unless configfile
		$logger.error "Couldn't find a config file, perhaps create one with --generate-config?"
		exit
	end

	configdump = {}

	begin
		File.open(configfile) { |yf| configdump = YAML::load(yf) }
	rescue ArgumentError
		$logger.error "Failure while parsing #{configfile}, please fix or create a new one with --generate-config" 
		exit
	end

	$logger.debug "read config from #{$configfile[0]}"
	configdump.each { |k, v|
		$logger.debug "#{k} => #{v}"
	}

	$db_host = configdump['db_host']
	$db_port = configdump['db_port']
	$db_username = configdump['db_username']
	$db_password = configdump['db_password']
	$db_name = configdump['db_name']
	$api_key = configdump['api_key']
	$psql_binary = configdump['psql_binary']

	unless $db_name && $api_key
		$logger.error "Missing database name and/or api key in configfile #{configfile}, perhaps create one with --generate-config?"
		exit
	end
end

def write_config

	config = <<'EOF'
api_key: fill_me_in
db_name: fill_me_in
#db_username:
#db_password:
#db_host: localhost
#db_port: 5432
#psql_binary: /autodetected/from/$PATH
EOF


	cf = $configfile[0]

	begin
		File.open(cf, File::WRONLY|File::CREAT|File::EXCL) { |f|
			f.write(config)
		}
	rescue => e
		$logger.error "Failed to write configfile: #{e.message}"
		exit
	end
	$logger.info "Wrote standard configuration to #{cf}, please edit it and then run the script again"
end

# Start of main

if $print_help
	print_help
	exit
end

if $generate_config
	write_config
	exit
end

unless $db_name && $api_key
	read_config
end

check_database

queries = fetch_queries

# FIXME: Verbose error reporting for wrong API key/broken data/etc?
if post_data_to_web(queries)
  unless $quiet
	  $logger.info "Submitted successfully"
  end
  if RESET_STATS
    $logger.debug "Resetting stats!"
    $db.exec("SELECT pg_stat_plans_reset()")
  end
else
  $logger.error "Rejected by server"
end
