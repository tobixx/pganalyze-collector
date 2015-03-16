import logging
import re
import sys

logger = logging.getLogger(__name__)


class PgStatStatements():

    def __init__(self, db):
        self.db = db

    def have_stats_helper(self):
        query = """
        SELECT 1 AS enabled
          FROM pg_proc
          JOIN pg_namespace ON (pronamespace = pg_namespace.oid)
         WHERE nspname = 'pganalyze' AND proname = 'get_stat_statements'
        """
        return self.db.run_query(query) == [{"enabled": 1}]

    def fetch_queries(self):
        query = "SELECT * FROM "

        if self.have_stats_helper():
            query += "pganalyze.get_stat_statements()"
        else:
            query += "pg_stat_statements"

        # We don't want our stuff in the statistics
        query += " WHERE query !~* '^%s'" % re.sub(r'([*/])', r'\\\1', self.db.querymarker)
        # Filter out queries we shouldn't see in the first place
        query += " AND query <> '<insufficient privilege>'"
        # Filter out DEALLOCATE statements - they are not useful and consume space
        query += " AND query NOT LIKE 'DEALLOCATE %'"
        # Only get queries from current database
        query += " AND dbid IN (SELECT oid FROM pg_database WHERE datname = current_database())"

        queries = []

        for row in self.db.run_query(query, False):
            del row['dbid']
            del row['userid']

            queries.append(row)

        return queries
