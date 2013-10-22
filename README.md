pganalyze-collector
===================

pganalyze data collector

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

etc. and will be extended as needed.


Installation & Usage
--------------------

See http://pganalyze.com/docs for installation and usage instructions.


Example output
--------------

To get a feel for the data that is collected you can have a look at the following examples:

 * [Python prettyprinted](https://gist.github.com/terrorobe/7103268)
 * [JSON w/ prettyprinting](https://gist.github.com/terrorobe/7103234)


License
-------

pganalyze-collector is licensed under the 3-clause BSD license, see LICENSE file for details.
