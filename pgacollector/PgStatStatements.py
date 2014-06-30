# Copyright (c) 2014, pganalyze Team <team@pganalyze.com>
#  All rights reserved.

import logging
import re

logger = logging.getLogger(__name__)


class PgStatStatements():

    def __init__(self, db):
        self.db = db

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
        query += " WHERE query !~* '^%s'" % re.sub(r'([*/])', r'\\\1', self.db.querymarker)
        # Filter out queries we shouldn't see in the first place
        query += " AND query <> '<insufficient privilege>'"
        # Only get queries from current database
        query += " AND dbid IN (SELECT oid FROM pg_database WHERE datname = current_database())"

        queries = []

        for row in self.db.run_query(query, False):
            row['plans'] = []
            queries.append(row)

        return queries

