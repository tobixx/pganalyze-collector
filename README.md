pganalyze-collector
===================

This is a CLI tool which collects various information about Postgres databases
as well as queries run on it. All data is converted to a JSON data structure
which can then be used as data source for monitoring & graphing systems. Or
just as reference on how to pull information out of PostgreSQL.

It currently collections information about

 * Schema
   * Tables (including column, constraint and trigger definitions)
   * Indexes
 * Statistics
   * Tables
   * Indexes
   * Database
   * Queries
 * OS
   * CPU
   * Memory
   * Storage

Installation
------------

The collector is available in multiple convenient options:

* APT/YUM/zip packages: https://pganalyze.com/docs/install/02_installing_the_collector (recommended)
* `git clone -b production https://github.com/pganalyze/pganalyze-collector.git`
* Docker sidekick service, see details further down in this file

Usage
-----

To first generate a configuration, run:

```
./pganalyze-collector --generate-config
```

Then fill out the `.pganalyze_collector.conf` with the appropriate values.

The collector is primarily intended to post data to pganalyze compatible servers,
you can view the JSON data being posted by running:

```
./pganalyze-collector --dry-run
```

See https://pganalyze.com/docs for details.


Setting up a Restricted Monitoring User
---------------------------------------

By default pg_stat_statements does not allow viewing queries run by other users,
unless you are a database superuser. Since you probably don't want monitoring
to run as a superuser, you can setup a separate monitoring user like this:

```
CREATE SCHEMA pganalyze;

CREATE OR REPLACE FUNCTION pganalyze.get_stat_statements() RETURNS SETOF pg_stat_statements AS
$$
  SELECT * FROM public.pg_stat_statements
  WHERE dbid IN (SELECT oid FROM pg_database WHERE datname = current_database());
$$ LANGUAGE sql VOLATILE SECURITY DEFINER;

CREATE OR REPLACE FUNCTION pganalyze.get_stat_activity() RETURNS SETOF pg_stat_activity AS
$$
  SELECT * FROM pg_catalog.pg_stat_activity
  WHERE datname = current_database();
$$ LANGUAGE sql VOLATILE SECURITY DEFINER;

CREATE USER pganalyze PASSWORD 'mypassword';
REVOKE ALL ON SCHEMA public FROM pganalyze;
GRANT USAGE ON SCHEMA pganalyze TO pganalyze;
```

Note that these statements must be run as a superuser (to create the `SECURITY DEFINER` function),
but from here onwards you can use the `pganalyze` user instead.

The collector will automatically use the helper methods
if they exist in the `pganalyze` schema - otherwise data will be fetched directly.


Example output
--------------

To get a feel for the data that is collected you can have a look at the following examples:

 * [Python prettyprinted](https://gist.github.com/terrorobe/7103268)
 * [JSON w/ prettyprinting](https://gist.github.com/terrorobe/7103234)


Docker sidekick service
-----------------------

If the database you want to monitor is running inside a Docker environment you can use the Docker image:

```
docker pull lfittl/pganalyze-collector:stable
docker run --name my-app-pga-collector --link my-app-db:db --env-file collector_config.env  lfittl/pganalyze-collector:stable
```

collector_config.env needs to look like this:

```
PGA_API_KEY=$YOUR_API_KEY
DB_NAME=your_database_name
DB_USERNAME=your_database_user
DB_PASSWORD=your_database_password
```

The only required arguments are PGA_API_KEY (found in the [pganalyze](https://pganalyze.com/) dashboard) and DB_NAME.

Note: You can add ```-v /path/to/database/volume/on/host:/var/lib/postgresql/data``` in order to collect I/O statistics from your database, this requires that it runs on the same machine.

Authors
-------

 * [Michael Renner](https://github.com/terrorobe)
 * [Lukas Fittl](https://github.com/lfittl)


License
-------

pganalyze-collector is licensed under the 3-clause BSD license, see LICENSE file for details.
